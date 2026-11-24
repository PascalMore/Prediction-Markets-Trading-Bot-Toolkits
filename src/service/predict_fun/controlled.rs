//! The controlled live path of the Predict.fun adapter.
//!
//! The controlled path is the adapter's own path: the same client, the same
//! signer, the same chain read, the same single POST. What it adds is the
//! proof that the order it is about to send is the order the core decided on,
//! and that its evidence is durable before the transport sees it.
//!
//! The fee domain is the part that needed proving. A BUY's fee comes out of
//! the received shares, so the cash the order can take is the signed cash
//! itself. The ordinary reservation bound adds a cash fee on top, which
//! would reserve roughly twice the cash and refuse every legal near-two-USDT
//! order. The proven bound below uses the share-fee direction instead, and
//! every other venue behaviour stays exactly as it was.

use super::fills::{ceil_div, cash_with_fee, checked_product_atoms};
use super::{
    controlled, f64_to_wei, market_amounts, order_body, ApiCreatedOrder, ApiMarket, Order, WEI,
};
use crate::bot::runtime::live_accounting::{controlled_buy_max_cash, minimum_net_shares};
use crate::models::{PlannedOrder, Side};
use crate::service::venue::{
    AccountIdentity, ControlledOrderMetadata, ControlledSendGuard, SignedExecutionBound, VenueError,
    VenueErrorCode,
};
use alloy_primitives::{U256, U512};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;

/// The largest fee rate the venue's own contract allows.
///
/// A signed rate above this is not a domain this adapter has proven, so the
/// order is not signed: an unproven fee direction is not a zero fee.
pub const MAX_SIGNED_FEE_BPS: u32 = 1000;

/// What the adapter recorded about the guarded orders.
#[derive(Debug, Default)]
pub struct ControlledMetadataStore {
    entries: RwLock<HashMap<String, ControlledOrderMetadata>>,
}

impl ControlledMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record what the venue said about one order.
    pub fn record(&self, signed_hash: &str, metadata: ControlledOrderMetadata) {
        self.entries
            .write()
            .unwrap()
            .insert(signed_hash.to_string(), metadata);
    }

    /// What was recorded about one order.
    pub fn get(&self, signed_hash: &str) -> Option<ControlledOrderMetadata> {
        self.entries.read().unwrap().get(signed_hash).cloned()
    }
}

/// Immutable broker facts captured before the adapter's first await.
/// No comparison may use an identity or quantity overwritten during signing.
pub(super) struct FrozenAttempt {
    identity: crate::service::venue::OrderIdentity,
    quantity: U256,
    reservation: U256,
    limit_price: U256,
}

impl FrozenAttempt {
    pub(super) fn capture(
        handle: &crate::service::venue::AttemptHandle,
        planned: &PlannedOrder,
        account: &AccountIdentity,
    ) -> Result<Self> {
        use crate::service::venue::{AttemptPhase, Submission};
        let record = handle.lock();
        let quantity = gross_quantity(planned)?;
        if planned.side != Side::Buy || planned.venue != account.venue
            || record.identity.account != *account
            || record.identity.token_id != planned.token_id
            || record.identity.side != Side::Buy
            || record.request_gross_atoms != quantity || quantity.is_zero()
            || record.phase != AttemptPhase::Prepared
            || record.submission != Submission::NotSubmitted
            || record.identity.signed_hash.is_some()
            || record.identity.venue_order_id.is_some()
        {
            bail!("INPUT_CHANGED:FROZEN_ATTEMPT");
        }
        Ok(Self { identity: record.identity.clone(), quantity,
            reservation: record.r0, limit_price: f64_to_wei(planned.limit_price)? })
    }

    pub(super) fn check_order(
        &self,
        handle: &crate::service::venue::AttemptHandle,
        order: &Order,
        market: &ApiMarket,
        maker: alloy_primitives::Address,
    ) -> Result<()> {
        let record = handle.lock();
        if record.identity != self.identity || record.request_gross_atoms != self.quantity
            || record.r0 != self.reservation || market.id != self.identity.market_id
            || order.tokenId.to_string() != self.identity.token_id
            || order.side != Side::Buy.as_u8() || order.takerAmount != self.quantity
            || order.maker != maker || order.signer != maker || !order.taker.is_zero()
            || order.signatureType != 0 || !order.nonce.is_zero()
        {
            bail!("INPUT_CHANGED:FROZEN_ORDER");
        }
        // Cross-multiply the exact signed ratio. The ceil reservation does
        // not grant even one atom of extra execution price.
        let cash_scaled = U512::from(order.makerAmount) * U512::from(WEI);
        let limit_scaled = U512::from(self.quantity) * U512::from(self.limit_price);
        if cash_scaled > limit_scaled || order.makerAmount > self.reservation {
            bail!("INPUT_CHANGED:CASH_ABOVE_LIMIT");
        }
        Ok(())
    }
}

/// The proven bound of a guarded BUY.
///
/// The order must be a BUY on the market the candidate froze, at the fee rate
/// the candidate froze, and its cash must not exceed the candidate's limit.
/// Anything else is a different input, and a different input ends the
/// candidate rather than silently signing a new quantity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenBuyBound {
    pub signed_hash: String,
    pub request_gross_atoms: U256,
    pub max_cash_atoms: U256,
    pub min_net_shares_atoms: U256,
    pub fee_rate_bps: u32,
}

/// Prove the bound of a guarded BUY order.
///
/// The proof is in atoms and it uses the wider type for the multiplication, so
/// nothing is truncated before the comparison.
pub fn prove_buy_bound(
    order: &Order,
    signed_hash: &str,
    market: &ApiMarket,
    candidate_limit_atoms: U256,
) -> Result<ProvenBuyBound> {
    if order.side != Side::Buy.as_u8() {
        bail!("SIGNED_BOUND_UNPROVEN:NOT_A_BUY");
    }
    let fee_rate_bps: u32 = order
        .feeRateBps
        .try_into()
        .map_err(|_| anyhow::anyhow!("SIGNED_BOUND_UNPROVEN:FEE_RATE"))?;
    if fee_rate_bps > MAX_SIGNED_FEE_BPS {
        bail!("SIGNED_BOUND_UNPROVEN:FEE_RATE_ABOVE_CAP");
    }
    if fee_rate_bps != market.fee_rate_bps {
        bail!("INPUT_CHANGED:FEE_RATE");
    }
    let gross_cash = order.makerAmount;
    let gross_q = order.takerAmount;
    // The cash the order pays must not exceed the candidate's limit: a
    // better price may not silently replace the candidate's input.
    if gross_cash > candidate_limit_atoms {
        bail!("INPUT_CHANGED:CASH_ABOVE_LIMIT");
    }
    let min_net_shares = minimum_net_shares(gross_q, fee_rate_bps)
        .ok_or_else(|| anyhow::anyhow!("SIGNED_BOUND_UNPROVEN:NET_SHARES"))?;
    if min_net_shares.is_zero() {
        bail!("SIGNED_BOUND_UNPROVEN:NET_SHARES");
    }
    Ok(ProvenBuyBound {
        signed_hash: signed_hash.to_string(),
        request_gross_atoms: gross_q,
        max_cash_atoms: controlled_buy_max_cash(gross_cash),
        min_net_shares_atoms: min_net_shares,
        fee_rate_bps,
    })
}

/// The worst-case cash a guarded BUY must reserve.
///
/// The fee is taken from the received shares, so the worst cash is the signed
/// cash itself. The ordinary cash-with-fee bound is not used here: it would
/// reserve about twice the cash and would refuse legal orders.
pub fn controlled_buy_reservation(
    market: &ApiMarket,
    gross_q_atoms: U256,
    limit_price: f64,
) -> Result<U256> {
    let _ = market;
    cash_at_limit_atoms(gross_q_atoms, limit_price)
}

/// Conservative fixed-point price*quantity, with no float atom arithmetic.
pub(crate) fn cash_at_limit_atoms(gross_q_atoms: U256, limit_price: f64) -> Result<U256> {
    let limit_atoms = U512::from(f64_to_wei(limit_price)?);
    let product = U512::from(gross_q_atoms)
        .checked_mul(limit_atoms)
        .ok_or_else(|| anyhow::anyhow!("RESERVATION_OVERFLOW"))?;
    let cash = ceil_div(product, U512::from(WEI))?;
    U256::from_str_radix(&cash.to_string(), 10).map_err(|_| anyhow::anyhow!("RESERVATION_OVERFLOW"))
}

/// The ordinary reservation bound, for the paths that keep it.
///
/// It stays exactly as it was: a cash fee on top of the gross.
pub fn ordinary_buy_reservation(
    market: &ApiMarket,
    gross_q_atoms: U256,
    limit_price: f64,
) -> Result<U256> {
    let limit_atoms = U512::from(f64_to_wei(limit_price)?);
    let product = U512::from(gross_q_atoms)
        .checked_mul(limit_atoms)
        .ok_or_else(|| anyhow::anyhow!("RESERVATION_OVERFLOW"))?;
    let cash = ceil_div(product, U512::from(WEI))?;
    cash_with_fee(cash, market.fee_rate_bps)
}

/// The bound as the venue seam names it.
pub fn to_venue_bound(
    bound: &ProvenBuyBound,
    identity: &AccountIdentity,
    market_id: u64,
    token_id: &str,
    route: &str,
    expiration: i64,
) -> SignedExecutionBound {
    SignedExecutionBound {
        account: identity.clone(),
        market_id,
        token_id: token_id.to_string(),
        route: route.to_string(),
        fee_rate_bps: bound.fee_rate_bps,
        request_gross_atoms: bound.request_gross_atoms,
        gross_cash_atoms: bound.max_cash_atoms,
        max_cash_atoms: bound.max_cash_atoms,
        min_net_shares_atoms: bound.min_net_shares_atoms,
        expiration,
        signed_hash: bound.signed_hash.clone(),
    }
}

/// The order the adapter is about to send, with its proven bound.
pub struct GuardedOrder {
    pub order: Order,
    pub bound: SignedExecutionBound,
    pub body: serde_json::Value,
}

/// The metadata of a guarded order, before the venue answered.
pub fn initial_metadata(removal_locked_until: Option<DateTime<Utc>>) -> ControlledOrderMetadata {
    ControlledOrderMetadata {
        removal_locked_until,
        execution_generation: 0,
        chain_settled: false,
        settlement_checked_at: None,
    }
}

/// The venue's answer to a guarded order.
pub fn created_order_metadata(
    created: &ApiCreatedOrder,
    generation: u64,
) -> ControlledOrderMetadata {
    ControlledOrderMetadata {
        removal_locked_until: created.removal_locked_until,
        execution_generation: generation,
        chain_settled: false,
        settlement_checked_at: None,
    }
}

/// The reason a guarded order may not be sent.
pub fn refuse_reason(error: &VenueError) -> &'static str {
    match error.code {
        VenueErrorCode::InvalidInput => "INPUT_CHANGED",
        VenueErrorCode::ExecutionNotEnabled => "RUN_STOPPED",
        VenueErrorCode::AccountMismatch => "ACCOUNT_MISMATCH",
        VenueErrorCode::ReadUnavailable => "READ_UNAVAILABLE",
        VenueErrorCode::FactIncomplete => "FACT_INCOMPLETE",
        VenueErrorCode::SubmissionUnknown => "SUBMISSION_UNKNOWN",
        VenueErrorCode::ReconciliationRequired => "RECONCILIATION_REQUIRED",
        VenueErrorCode::TxUnconfirmed => "FACT_INCOMPLETE",
        VenueErrorCode::SdkIncompatible => "FACT_INCOMPLETE",
        VenueErrorCode::BudgetUnconfirmed => "RECONCILIATION_REQUIRED",
    }
}

/// The guard's view of the facts of a guarded order.
pub fn check_guarded_facts(
    guard: &dyn ControlledSendGuard,
    bound: &SignedExecutionBound,
) -> Result<()> {
    guard.check_facts(bound)
}

/// The guard's view of the durability of a guarded order.
pub fn await_guarded_ack(
    guard: &dyn ControlledSendGuard,
    bound: &SignedExecutionBound,
) -> Result<()> {
    guard.await_durable_ack(bound)
}

/// The guard's last permission.
pub fn permit_guarded_send(guard: &dyn ControlledSendGuard) -> Result<()> {
    guard.permit_send()
}

/// The route an order is signed against.
pub fn route_of(market: &ApiMarket) -> String {
    if market.is_neg_risk {
        if market.is_yield_bearing {
            "yield-neg-risk".to_string()
        } else {
            "neg-risk".to_string()
        }
    } else if market.is_yield_bearing {
        "yield".to_string()
    } else {
        "ctf".to_string()
    }
}

/// The prepared request body of a guarded order.
pub fn guarded_body(
    order: &Order,
    signed_hash: &str,
    signature: &str,
    amounts: &super::MarketAmounts,
    expiration: i64,
) -> serde_json::Value {
    order_body(order, signed_hash, signature, amounts, expiration)
}

/// The gross quantity of a guarded order, in atoms.
pub(crate) fn quantity_atoms(shares: f64) -> Result<U256> {
    f64_to_wei(shares)
}

pub fn gross_quantity(planned: &PlannedOrder) -> Result<U256> {
    quantity_atoms(planned.shares)
}

/// The market amounts of a guarded order.
pub fn guarded_amounts(
    book: &super::WeiBook,
    side: Side,
    quantity_wei: U256,
) -> Result<super::MarketAmounts> {
    market_amounts(book, side, quantity_wei)
}

/// The product of two atom amounts, in the wider type.
pub fn wide_product(a: U256, b: U256) -> Result<U256> {
    checked_product_atoms(a, b)
}

/// One official round the synthetic transport can serve.
///
/// The fixture is offline: the round's public metadata is fixed, and every
/// signing, RPC and POST answer still comes from the existing synthetic
/// transport. It can never fall through to a network.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct ControlledRound {
    /// The market id the venue reports for this round.
    pub market_id: u64,
    /// The yes outcome token id.
    pub yes_token: u64,
    /// The no outcome token id.
    pub no_token: u64,
    /// The condition id byte the venue reports.
    pub condition: u8,
}

/// One posted order, with the round it belongs to.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ControlledOrder {
    /// The signed order hash the venue knows the order by.
    pub hash: String,
    /// The market the order was placed in.
    pub market_id: u64,
    /// The outcome token the order traded.
    pub token: u64,
    /// The settlement transaction the venue reports.
    pub tx_hash: String,
    /// The signed order body the venue received.
    pub order: serde_json::Value,
}

/// Raw offline transport fixture for the original Broker/adapter consumer.
/// Only public market metadata/book are changed; auth/RPC/sign/POST still use
/// the existing synthetic transport and can never fall through to a network.
/// Captured fake signer/transport inputs, never real signatures or credentials.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct ControlledSyntheticTrace {
    pub signed: std::sync::Mutex<Vec<serde_json::Value>>,
    pub posted: std::sync::Mutex<Vec<serde_json::Value>>,
    pub extra_position_atoms: std::sync::atomic::AtomicU64,
    /// One-shot observation inside the actual request's first transport poll.
    pub first_post: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// The official rounds this transport serves.
    pub rounds: Vec<ControlledRound>,
    /// The fee rate every round reports.
    pub fee_rate_bps: u32,
    /// Every order this transport received, in the order it received them.
    pub orders: std::sync::Mutex<Vec<ControlledOrder>>,
    /// How many times the venue served a fresh book to a preparation.
    pub book_reads: std::sync::atomic::AtomicU64,
    /// Test-only: fills stay invisible to the execution read until this
    /// instant, modelling a venue whose acceptance is ahead of its fills.
    pub hidden_until: std::sync::Mutex<Option<std::time::Instant>>,
    pub read_script: super::fills::order_wait_tests::ReadScript,
}

#[cfg(test)]
impl ControlledSyntheticTrace {
    /// The round a token belongs to.
    fn round_of_token(&self, token: u64) -> Option<ControlledRound> {
        self.rounds.iter().copied()
            .find(|round| round.yes_token == token || round.no_token == token)
    }

    /// The round a market id belongs to.
    fn round_by_id(&self, market_id: u64) -> Option<ControlledRound> {
        self.rounds.iter().copied().find(|round| round.market_id == market_id)
    }

    /// The order the venue knows by this hash.
    fn order_by_hash(&self, hash: &str) -> Option<ControlledOrder> {
        self.orders.lock().unwrap().iter()
            .find(|order| order.hash.eq_ignore_ascii_case(hash)).cloned()
    }

    /// The order the venue settled in this transaction.
    fn order_by_tx(&self, tx: &str) -> Option<ControlledOrder> {
        self.orders.lock().unwrap().iter()
            .find(|order| order.tx_hash.eq_ignore_ascii_case(tx)).cloned()
    }
}

#[cfg(test)]
pub(crate) mod r16_fixture;

/// The single-round fixture of one market.
#[cfg(test)]
pub(crate) fn synthetic_controlled_trader(
    fee_rate_bps: u32, fault: &'static str,
) -> (super::PredictFunTrader, std::sync::Arc<super::account::tests::SyntheticRpc>) {
    let (trader, transport, _) = synthetic_controlled_trader_with_trace(fee_rate_bps, fault);
    (trader, transport)
}

/// The single-round fixture with the captured fake signer and POST inputs.
#[cfg(test)]
pub(crate) fn synthetic_controlled_trader_with_trace(
    fee_rate_bps: u32, fault: &'static str,
) -> (super::PredictFunTrader, std::sync::Arc<super::account::tests::SyntheticRpc>,
    std::sync::Arc<ControlledSyntheticTrace>) {
    controlled_transport(fault, fee_rate_bps, vec![ControlledRound {
        market_id: if fault == "live003" { 4668196 } else { 1 },
        yes_token: 77, no_token: 88, condition: 0xcc }])
}

/// The two-round fixture: one adapter, one client and one account gate serve
/// two official rounds of the same account.
#[cfg(test)]
pub(crate) fn synthetic_controlled_two_round_trader(
    fee_rate_bps: u32,
) -> (super::PredictFunTrader, std::sync::Arc<super::account::tests::SyntheticRpc>,
    std::sync::Arc<ControlledSyntheticTrace>) {
    controlled_transport("controlled_fill", fee_rate_bps, vec![
        ControlledRound { market_id: 1, yes_token: 77, no_token: 88, condition: 0xcc },
        ControlledRound { market_id: 2, yes_token: 177, no_token: 188, condition: 0xdd },
    ])
}

/// Build one adapter that serves the given official rounds.
#[cfg(test)]
fn controlled_transport(
    fault: &'static str, fee_rate_bps: u32, rounds: Vec<ControlledRound>,
) -> (super::PredictFunTrader, std::sync::Arc<super::account::tests::SyntheticRpc>,
    std::sync::Arc<ControlledSyntheticTrace>) {
    let (mut trader, transport) = super::account::tests::synthetic_trader(fault);
    if fault == "fixed_account" {
        // Public deployment address only; the original synthetic signer/transport remain fake.
        trader.account = Some("0xfc9eb66e6f22f96608058c0a711ed048eed8c2e2".parse().unwrap());
    }
    let base = trader.client.market_for_token("77").unwrap();
    let mut markets = Vec::new();
    for round in &rounds {
        let mut market = base.clone();
        market.fee_rate_bps = fee_rate_bps;
        if round.market_id != 1 {
            market.id = round.market_id;
            market.outcomes[0].on_chain_id = round.yes_token.to_string();
            market.outcomes[1].on_chain_id = round.no_token.to_string();
            market.condition_id = Some(alloy_primitives::B256::repeat_byte(round.condition));
        }
        markets.push(market);
    }
    for market in &markets {
        trader.client.remember_market(market);
    }
    let trace = std::sync::Arc::new(ControlledSyntheticTrace {
        rounds, fee_rate_bps, ..Default::default() });
    std::sync::Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(
        std::sync::Arc::new(BookTransport(transport.clone(), fault, trace.clone())));
    (trader, transport, trace)
}

/// The venue's own transport over the offline public round facts.
#[cfg(test)]
struct BookTransport(
    std::sync::Arc<super::account::tests::SyntheticRpc>,
    &'static str,
    std::sync::Arc<ControlledSyntheticTrace>,
);

#[cfg(test)]
impl BookTransport {
    /// Whether the venue reports a fill for the orders it received.
    fn filled(&self) -> bool {
        matches!(self.1, "controlled_fill" | "controlled_partial" | "controlled_missing_fee" | "live003"
            | "book_bid_moved" | "book_moved_then_frozen" | "book_inside_limit_thin_then_frozen"
            | "controlled_missing_facts")
    }

    /// Whether the venue's fill read still lags behind its acceptance.
    fn fill_hidden(&self) -> bool {
        self.2
            .hidden_until
            .lock()
            .unwrap()
            .is_some_and(|until| std::time::Instant::now() < until)
    }

    /// The live002/live004 shape: a fresh book that differs from the frozen one.
    ///
    /// - `book_bid_moved`: the bid moved inside the candidate's limit.
    /// - `book_moved_then_frozen`: the first read is past the limit, later ones
    ///   are the frozen book, so a window that survives one refusal can still
    ///   fill its next candidate.
    /// - `book_past_limit` / `book_past_limit_thin`: the ask moved past the
    ///   candidate's limit, with and without enough depth for the size.
    /// - `book_inside_limit_thin`: the ask is inside the candidate's limit but
    ///   the level cannot cover the frozen quantity.
    /// - `book_inside_limit_thin_then_frozen`: the first read is inside the
    ///   limit and too thin, later reads are the frozen book, so a window that
    ///   survives one refusal can still fill its next candidate.
    /// - `empty_book`: the venue returns no side at all.
    fn book_levels(&self) -> (f64, f64, f64) {
        match self.1 {
            "live003" => (0.37, 0.36, 100.0),
            "better_book" => (0.39, 0.3, 100.0),
            "changed_bid" => (0.4, 0.31, 100.0),
            "changed_depth" => (0.4, 0.3, 99.0),
            "book_bid_moved" => (0.4, 0.29, 100.0),
            "book_past_limit" => (0.41, 0.3, 100.0),
            "book_past_limit_thin" => (0.41, 0.3, 1.0),
            "book_inside_limit_thin" => (0.39, 0.3, 1.0),
            _ => (0.4, 0.3, 100.0),
        }
    }

    /// Whether the venue reports only part of the order as filled.
    fn partial(&self) -> bool {
        self.1 == "controlled_partial"
            && self.0.stage.load(std::sync::atomic::Ordering::SeqCst) == 0
    }

    /// The raw venue facts of one posted order: gross shares, gross cash, fee.
    ///
    /// The fee is a raw venue fact, deliberately not derived from the signed
    /// fee rate.
    fn amounts(&self, order: &serde_json::Value) -> (U256, U256, U256) {
        let requested = U256::from_str_radix(order["takerAmount"].as_str().unwrap(), 10).unwrap();
        let signed_cash = U256::from_str_radix(order["makerAmount"].as_str().unwrap(), 10).unwrap();
        let divisor = U256::from(if self.partial() { 2 } else { 1 });
        let gross = requested / divisor;
        let cash = signed_cash / divisor;
        (gross, cash, gross / U256::from(100))
    }

    /// The venue's own market document for one round.
    fn raw_market(&self, round: ControlledRound) -> serde_json::Value {
        serde_json::json!({"id":round.market_id,"tradingStatus":"OPEN",
            "conditionId":format!("0x{}", hex::encode([round.condition; 32])),
            "isNegRisk":false,"isYieldBearing":false,"feeRateBps":self.2.fee_rate_bps,
            "outcomes":[{"onChainId":round.yes_token.to_string()},
                {"onChainId":round.no_token.to_string()}]})
    }

    /// The cash the account actually spent on every order it received.
    fn spent_cash(&self) -> U256 {
        let orders = self.2.orders.lock().unwrap().clone();
        orders.iter().fold(U256::ZERO, |total, record| {
            total + self.amounts(&record.order).1
        })
    }

    /// The shares the account actually holds of one token.
    fn held(&self, token: u64) -> U256 {
        let orders = self.2.orders.lock().unwrap().clone();
        let mut total = U256::ZERO;
        let mut last = None;
        for record in &orders {
            let (gross, _, fee) = self.amounts(&record.order);
            if record.token == token {
                total = total + (gross - fee);
            }
            last = Some(record.token);
        }
        if last == Some(token) {
            total = total
                + U256::from(self.2.extra_position_atoms.load(std::sync::atomic::Ordering::SeqCst));
        }
        total
    }

    /// The positions the venue reports for the account.
    fn positions(&self) -> Vec<serde_json::Value> {
        use serde_json::json;
        let orders = self.2.orders.lock().unwrap().clone();
        let mut totals: std::collections::HashMap<u64, (u64, U256)> = std::collections::HashMap::new();
        for record in &orders {
            let (gross, _, fee) = self.amounts(&record.order);
            let entry = totals.entry(record.token).or_insert((record.market_id, U256::ZERO));
            entry.1 = entry.1 + (gross - fee);
        }
        let last = orders.last().map(|record| record.token);
        let mut out = Vec::new();
        for (token, (market_id, amount)) in totals {
            let mut amount = amount;
            if Some(token) == last {
                amount = amount + U256::from(
                    self.2.extra_position_atoms.load(std::sync::atomic::Ordering::SeqCst));
            }
            if amount.is_zero() {
                continue;
            }
            let round = self.2.round_by_id(market_id).unwrap();
            out.push(json!({"market":self.raw_market(round),
                "outcome":{"onChainId":token.to_string()},"amount":amount.to_string()}));
        }
        out
    }

    /// The match events the venue reports for this account.
    fn matches(&self, path: &str) -> Vec<serde_json::Value> {
        let wanted = path.split('&').find_map(|part| part.strip_prefix("orderHashes="));
        let orders = self.2.orders.lock().unwrap().clone();
        orders.iter()
            .filter(|record| match wanted {
                Some(hash) => record.hash.eq_ignore_ascii_case(hash),
                None => true,
            })
            .map(|record| self.leg(record))
            .collect()
    }

    /// One match event of one order.
    fn leg(&self, record: &ControlledOrder) -> serde_json::Value {
        use serde_json::json;
        let round = self.2.round_by_id(record.market_id).unwrap();
        let (gross, _, fee) = self.amounts(&record.order);
        let mut leg = json!({"hash":record.order["hash"],"signer":record.order["signer"],
            "quoteType":"Bid","amount":gross.to_string(),
            "price":if self.1 == "live003" { "640000000000000000" } else { "400000000000000000" },
            "outcome":{"onChainId":record.token.to_string()},
            "fee":{"amount":fee.to_string(),"type":"SHARES"}});
        if self.1 == "controlled_missing_fee"
            && self.0.stage.load(std::sync::atomic::Ordering::SeqCst) == 0
        {
            leg.as_object_mut().unwrap().remove("fee");
        }
        json!({"market":self.raw_market(round),"taker":leg,"makers":[],
            "executedAt":"2026-11-19T00:00:00Z","transactionHash":record.tx_hash})
    }

    /// The receipt of one order's settlement.
    fn receipt(&self, record: &ControlledOrder) -> serde_json::Value {
        use alloy_primitives::{Address, keccak256};
        use serde_json::json;
        let owner: Address = record.order["maker"].as_str().unwrap().parse().unwrap();
        let exchange = super::MAINNET.exchange(false, false);
        let (usdt, ctf, _) = super::account::assets(56, false, false);
        let topic = |address: Address| format!("0x{:064x}", U256::from_be_slice(address.as_slice()));
        let words = |values: &[U256]| format!("0x{}", values.iter()
            .map(|value| format!("{value:064x}")).collect::<String>());
        let (gross, cash, fee) = self.amounts(&record.order);
        let block_hash = format!("0x{}", "b".repeat(64));
        let log = |index: u64, address: Address, topics: serde_json::Value, data: String|
            json!({"removed":false,"transactionHash":record.tx_hash,"blockHash":block_hash,
                "blockNumber":"0x123","logIndex":format!("{index:#x}"),
                "address":address,"topics":topics,"data":data});
        json!({"transactionHash":record.tx_hash,"status":"0x1","blockNumber":"0x123",
            "blockHash":block_hash,"logs":[
                log(0, exchange, json!([keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"),
                    record.order["hash"],topic(owner),topic(exchange)]),
                    words(&[U256::ZERO,U256::from(record.token),cash,gross,fee])),
                log(1, usdt, json!([keccak256("Transfer(address,address,uint256)"),topic(owner),topic(exchange)]),
                    words(&[cash])),
                log(2, ctf, json!([keccak256("TransferSingle(address,address,address,uint256,uint256)"),
                    topic(exchange),topic(exchange),topic(owner)]),
                    words(&[U256::from(record.token),gross-fee]))
            ]})
    }
}

#[cfg(test)]
impl super::account::SyntheticTransport for BookTransport {
        fn pending_post(&self) -> bool { self.0.pending_post() }
        fn auth_token(&self) -> String { self.0.auth_token() }
        fn rpc(&self, method: &str, params: &serde_json::Value) -> std::result::Result<serde_json::Value, VenueError> {
            if method == "eth_getBlockByNumber" {
                if let Some(timestamp) = *self.2.read_script.block_time.lock().unwrap() {
                    let mut value = self.0.rpc(method, params)?;
                    value["timestamp"] = serde_json::json!(format!("{timestamp:#x}"));
                    return Ok(value);
                }
            }
            if let Some(value) = self.2.read_script.rpc(method, params) { return Ok(value); }
            if self.filled() && method == "eth_getTransactionReceipt" {
                let tx = params[0].as_str().unwrap();
                let record = self.2.order_by_tx(tx)
                    .ok_or_else(|| VenueError::new(
                        VenueErrorCode::ReadUnavailable, "controlled_receipt", "RECEIPT_UNKNOWN"))?;
                return Ok(self.receipt(&record));
            }
            if self.filled() && method == "eth_call" {
                let data = params[0]["data"].as_str().unwrap();
                let selector = &data[2..10];
                let balance = alloy_primitives::keccak256("balanceOf(address)");
                let inventory = alloy_primitives::keccak256("balanceOf(address,uint256)");
                let registry = alloy_primitives::keccak256("registry(uint256)");
                if selector == hex::encode(&registry.as_slice()[..4]) {
                    let token = U256::from_str_radix(&data[data.len()-64..], 16).unwrap();
                    if let Some(round) = self.2.round_of_token(token.to::<u64>()) {
                        let complement = if token == U256::from(round.yes_token) {
                            round.no_token
                        } else {
                            round.yes_token
                        };
                        return Ok(serde_json::json!(format!("0x{:064x}{}",
                            complement, hex::encode([round.condition; 32]))));
                    }
                }
                if selector == hex::encode(&balance.as_slice()[..4]) {
                    let cash = if self.fill_hidden() { U256::ZERO } else { self.spent_cash() };
                    return Ok(serde_json::json!(format!("0x{:064x}",
                        U256::from(100) * U256::from(WEI) - cash)));
                }
                if selector == hex::encode(&inventory.as_slice()[..4]) {
                    let token = U256::from_str_radix(&data[data.len()-64..], 16).unwrap();
                    let net = self.held(token.to::<u64>());
                    return Ok(serde_json::json!(format!("0x{net:064x}")));
                }
            }
            self.0.rpc(method, params)
        }
        fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&serde_json::Value>) -> Result<String> {
            let raw = (|| {
            if path == "/orders" && *method == reqwest::Method::POST {
                if let Some(first_post) = self.2.first_post.lock().unwrap().take() { first_post(); }
                let body = body.unwrap();
                self.2.posted.lock().unwrap().push(body.clone());
                let order = body["data"]["order"].clone();
                let hash = order["hash"].as_str().unwrap().to_string();
                let token = U256::from_str_radix(order["tokenId"].as_str().unwrap(), 10).unwrap();
                let token = token.to::<u64>();
                let round = self.2.round_of_token(token)
                    .ok_or_else(|| anyhow::anyhow!("CONTROLLED_UNKNOWN_TOKEN"))?;
                let tx_hash = {
                    let mut orders = self.2.orders.lock().unwrap();
                    let tx_hash = format!("0x{:064x}", orders.len() + 1);
                    orders.push(ControlledOrder {
                        hash, market_id: round.market_id, token, tx_hash: tx_hash.clone(), order });
                    tx_hash
                };
                debug_assert!(tx_hash.starts_with("0x"));
            }
            if self.filled() {
                if self.fill_hidden() {
                    if path.starts_with("/positions?") {
                        return Ok(serde_json::json!({"data":[]}).to_string());
                    }
                    if path.starts_with("/orders/matches?") {
                        return Ok(serde_json::json!({"data":[]}).to_string());
                    }
                    if path.starts_with("/orders/") && !path.contains('?') {
                        let hash = path.trim_start_matches("/orders/");
                        let record = self.2.order_by_hash(hash)
                            .ok_or_else(|| anyhow::anyhow!("CONTROLLED_UNKNOWN_ORDER"))?;
                        return Ok(serde_json::json!({"data":{"id":"synthetic-order",
                            "marketId":record.market_id,"amountFilled":"0","status":"OPEN",
                            "order":record.order}}).to_string());
                    }
                }
                if self.1 == "controlled_missing_facts" {
                    // The venue lists the order, but it states no status this
                    // runtime knows: the fact is genuinely missing, not a
                    // complete reading of zero.
                    if path.starts_with("/positions?") {
                        return Ok(serde_json::json!({"data":[]}).to_string());
                    }
                    if path.starts_with("/orders/matches?") {
                        return Ok(serde_json::json!({"data":[]}).to_string());
                    }
                    if path.starts_with("/orders/") && !path.contains('?') {
                        let hash = path.trim_start_matches("/orders/");
                        let record = self.2.order_by_hash(hash)
                            .ok_or_else(|| anyhow::anyhow!("CONTROLLED_UNKNOWN_ORDER"))?;
                        return Ok(serde_json::json!({"data":{"id":"synthetic-order",
                            "marketId":record.market_id,"amountFilled":"0","status":"UNRECOGNIZED",
                            "order":record.order}}).to_string());
                    }
                }
                if path.starts_with("/positions?") {
                    return Ok(serde_json::json!({"data":self.positions()}).to_string());
                }
                if path.starts_with("/orders/matches?") {
                    return Ok(serde_json::json!({"data":self.matches(path)}).to_string());
                }
                if path.starts_with("/orders/") && !path.contains('?') {
                    let hash = path.trim_start_matches("/orders/");
                    let record = self.2.order_by_hash(hash)
                        .ok_or_else(|| anyhow::anyhow!("CONTROLLED_UNKNOWN_ORDER"))?;
                    let (gross, _, _) = self.amounts(&record.order);
                    let requested = U256::from_str_radix(
                        record.order["takerAmount"].as_str().unwrap(), 10).unwrap();
                    let full = gross == requested;
                    return Ok(serde_json::json!({"data":{"id":"synthetic-order","marketId":record.market_id,
                        "amountFilled":gross.to_string(),"status":if full { "FILLED" } else { "OPEN" },
                        "order":record.order}}).to_string());
                }
            }
            if path.strip_prefix("/markets/")
                .and_then(|rest| rest.strip_suffix("/orderbook"))
                .and_then(|id| id.parse::<u64>().ok())
                .is_some()
            {
                let read = self.2.book_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if self.1 == "empty_book" {
                    return Ok(serde_json::json!({"data":{"asks":[],"bids":[]}}).to_string());
                }
                if self.1 == "book_moved_then_frozen" && read == 0 {
                    return Ok(serde_json::json!({"data":{"asks":[[0.41,100.0]],"bids":[[0.3,100.0]]}}).to_string());
                }
                if self.1 == "book_inside_limit_thin_then_frozen" && read == 0 {
                    return Ok(serde_json::json!({"data":{"asks":[[0.39,1.0]],"bids":[[0.3,100.0]]}}).to_string());
                }
                let (ask, bid, depth) = self.book_levels();
                return Ok(serde_json::json!({"data":{"asks":[[ask,depth]],"bids":[[bid,100.0]]}}).to_string());
            }
            self.0.rest(method, path, bearer, body)
            })()?;
            self.2.read_script.rest(path, raw)
        }
        fn sign(&self, order: &Order) -> Result<(alloy_primitives::B256, String)> {
            let result = self.0.sign(order)?;
            self.2.signed.lock().unwrap().push(serde_json::json!({
                "hash":result.0.to_string(),"maker":order.maker.to_checksum(None),
                "signer":order.signer.to_checksum(None),"tokenId":order.tokenId.to_string(),
                "makerAmount":order.makerAmount.to_string(),"takerAmount":order.takerAmount.to_string(),
                "feeRateBps":order.feeRateBps.to_string(),"salt":order.salt.to_string(),
                "expiration":order.expiration.to_string(),"side":order.side,
                "route":"ctf"
            }));
            Ok(result)
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VenueId;
    use crate::service::venue::AccountMode;

    fn identity() -> AccountIdentity {
        AccountIdentity {
            chain_id: 56,
            trading_address: "0x1111111111111111111111111111111111111111".into(),
            mode: AccountMode::PredictAccount,
            signer_address: "0x1111111111111111111111111111111111111111".into(),
            venue: VenueId::PredictFun,
        }
    }

    fn market(fee_rate_bps: u32) -> ApiMarket {
        ApiMarket {
            id: 1,
            question: String::new(),
            trading_status: "OPEN".into(),
            is_neg_risk: false,
            is_yield_bearing: false,
            fee_rate_bps,
            outcomes: Vec::new(),
            category_slug: None,
            status: "OPEN".into(),
            market_variant: String::new(),
            decimal_precision: None,
            polymarket_condition_ids: Vec::new(),
            condition_id: None,
        }
    }

    fn order(fee_rate_bps: u32, cash: u128, shares: u128) -> Order {
        Order {
            salt: U256::from(1u64),
            maker: alloy_primitives::address!("0x1111111111111111111111111111111111111111"),
            signer: alloy_primitives::address!("0x1111111111111111111111111111111111111111"),
            taker: alloy_primitives::address!("0x1111111111111111111111111111111111111111"),
            tokenId: U256::from(2u64),
            makerAmount: U256::from(cash),
            takerAmount: U256::from(shares),
            expiration: U256::from(0u64),
            nonce: U256::from(0u64),
            feeRateBps: U256::from(fee_rate_bps),
            side: Side::Buy.as_u8(),
            signatureType: 0,
        }
    }

    #[test]
    fn a_proven_buy_bound_uses_the_share_fee_direction() {
        // Five shares, two USDT, two hundred basis points.
        let bound = prove_buy_bound(
            &order(200, 2 * 10u128.pow(18), 5 * 10u128.pow(18)),
            &format!("0x{}", "a".repeat(64)),
            &market(200),
            U256::from(2 * 10u128.pow(18)),
        )
        .unwrap();
        // The cash is the signed cash: the fee comes out of the shares.
        assert_eq!(bound.max_cash_atoms, U256::from(2 * 10u128.pow(18)));
        // The fewest shares that can arrive is 4.9.
        assert_eq!(
            bound.min_net_shares_atoms,
            U256::from(4_900_000_000_000_000_000u128)
        );
    }

    #[test]
    fn a_fee_rate_the_contract_does_not_allow_is_not_proven() {
        let error = prove_buy_bound(
            &order(2000, 2 * 10u128.pow(18), 5 * 10u128.pow(18)),
            &format!("0x{}", "a".repeat(64)),
            &market(2000),
            U256::from(2 * 10u128.pow(18)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("SIGNED_BOUND_UNPROVEN"));
    }

    #[test]
    fn a_fee_rate_that_does_not_match_the_market_is_an_input_change() {
        let error = prove_buy_bound(
            &order(200, 2 * 10u128.pow(18), 5 * 10u128.pow(18)),
            &format!("0x{}", "a".repeat(64)),
            &market(100),
            U256::from(2 * 10u128.pow(18)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("INPUT_CHANGED"));
    }

    #[test]
    fn a_cash_above_the_candidate_limit_is_an_input_change() {
        let error = prove_buy_bound(
            &order(200, 3 * 10u128.pow(18), 5 * 10u128.pow(18)),
            &format!("0x{}", "a".repeat(64)),
            &market(200),
            U256::from(2 * 10u128.pow(18)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("INPUT_CHANGED"));
    }

    #[test]
    fn a_sell_is_never_a_proven_buy_bound() {
        let mut order = order(200, 2 * 10u128.pow(18), 5 * 10u128.pow(18));
        order.side = Side::Sell.as_u8();
        let error = prove_buy_bound(
            &order,
            &format!("0x{}", "a".repeat(64)),
            &market(200),
            U256::from(2 * 10u128.pow(18)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("SIGNED_BOUND_UNPROVEN"));
    }

    #[test]
    fn the_guarded_reservation_is_the_signed_cash_not_twice_it() {
        let guarded = controlled_buy_reservation(
            &market(200),
            U256::from(5 * 10u128.pow(18)),
            0.4,
        )
        .unwrap();
        assert_eq!(guarded, U256::from(2 * 10u128.pow(18)));
        // The ordinary bound keeps the extra cash fee.
        let ordinary = ordinary_buy_reservation(
            &market(200),
            U256::from(5 * 10u128.pow(18)),
            0.4,
        )
        .unwrap();
        assert!(ordinary > guarded);
    }

    #[test]
    fn a_legal_two_usdt_order_survives_the_guarded_bound() {
        // The designed vector must not be refused by the tighter bound.
        let guarded = controlled_buy_reservation(
            &market(200),
            U256::from(5 * 10u128.pow(18)),
            0.4,
        )
        .unwrap();
        assert!(guarded <= U256::from(2 * 10u128.pow(18)));
    }

    struct InspectPrepared {
        seen: parking_lot::Mutex<Option<SignedExecutionBound>>,
        snapshot: crate::service::venue::AccountSnapshot,
    }

    impl ControlledSendGuard for InspectPrepared {
        fn check_preparation(&self) -> Result<()> { Ok(()) }
        fn account_snapshot(&self) -> Result<crate::service::venue::AccountSnapshot> {
            Ok(self.snapshot.clone())
        }
        fn check_book(&self, _book: &crate::models::BookSnapshot) -> Result<()> { Ok(()) }
        fn check_facts(&self, bound: &SignedExecutionBound) -> Result<()> {
            *self.seen.lock() = Some(bound.clone());
            bail!("INSPECTED_PREPARED_BOUND");
        }
    }

    fn tracked(
        trader: &super::super::PredictFunTrader, planned: &PlannedOrder,
    ) -> crate::service::venue::AttemptHandle {
        use crate::service::venue::{AttemptRecord, AttemptPhase, Submission, OrderIdentity};
        std::sync::Arc::new(parking_lot::Mutex::new(AttemptRecord {
            identity: OrderIdentity { local_attempt_id: 1, signed_hash: None, venue_order_id: None,
                market_id: 1, token_id: planned.token_id.clone(), side: planned.side, account: trader.identity() },
            request_gross_atoms: gross_quantity(planned).unwrap(),
            r0: U256::from(2u64) * U256::from(WEI), reserved_sell: U256::ZERO,
            phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
        }))
    }

    fn planned(shares: f64) -> PlannedOrder {
        PlannedOrder { venue: VenueId::PredictFun, token_id: "77".into(), side: Side::Buy,
            shares, limit_price: 0.5, usd_notional: shares * 0.5,
            order_type: crate::models::OrderType::Fak, source_trade_hash: None }
    }

    #[tokio::test]
    async fn original_controlled_adapter_proves_two_usdt_without_wei_squared_slack() {
        use crate::service::venue::OrderVenue;
        use std::sync::atomic::Ordering;
        let (trader, transport) = super::super::account::tests::synthetic_trader("terminal");
        let mut market = trader.client.market_for_token("77").unwrap();
        market.fee_rate_bps = 200;
        trader.client.remember_market(&market);
        let planned = planned(4.0);
        let handle = tracked(&trader, &planned);
        let snapshot = trader.account_snapshot().await.unwrap();
        let guard = InspectPrepared { seen: Default::default(), snapshot };
        let error = trader.place_controlled_tracked(&planned, 0, true, handle.clone(), &guard).await.unwrap_err();
        assert!(error.to_string().contains("INSPECTED_PREPARED_BOUND"), "{error}");
        let bound = guard.seen.lock().clone().unwrap();
        assert_eq!(bound.request_gross_atoms, U256::from(4u64) * U256::from(WEI));
        assert_eq!(bound.max_cash_atoms, U256::from(2u64) * U256::from(WEI));
        assert_eq!(bound.fee_rate_bps, 200);
        assert_eq!(bound.min_net_shares_atoms, U256::from(392u64) * U256::from(WEI) / U256::from(100));
        assert_eq!(handle.lock().request_gross_atoms, bound.request_gross_atoms);
        assert!(handle.lock().identity.signed_hash.is_some());
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn original_controlled_adapter_never_overwrites_a_mismatched_frozen_attempt() {
        use crate::service::venue::OrderVenue;
        use std::sync::atomic::Ordering;
        for fault in ["quantity", "market", "account", "token", "side", "truncated_quantity", "cash"] {
            let (trader, transport) = super::super::account::tests::synthetic_trader("terminal");
            let planned = planned(if fault == "truncated_quantity" { 3.123456 } else { 4.0 });
            let handle = tracked(&trader, &planned);
            {
                let mut record = handle.lock();
                match fault {
                    "quantity" => record.request_gross_atoms += U256::from(1),
                    "market" => record.identity.market_id = 99,
                    "account" => record.identity.account.chain_id = 97,
                    "token" => record.identity.token_id = "88".into(),
                    "side" => record.identity.side = Side::Sell,
                    "cash" => record.r0 -= U256::from(1),
                    _ => {}
                }
            }
            let before = handle.lock().clone();
            let snapshot = trader.account_snapshot().await.unwrap();
            let guard = InspectPrepared { seen: Default::default(), snapshot };
            assert!(trader.place_controlled_tracked(&planned, 0, true, handle.clone(), &guard).await.is_err(), "{fault}");
            assert_eq!(*handle.lock(), before, "{fault}");
            assert!(guard.seen.lock().is_none(), "{fault}");
            assert_eq!(transport.signs.load(Ordering::SeqCst), 0, "{fault}");
            assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "{fault}");
        }
    }

    #[test]
    fn normalized_candidate_bound_rounds_up_without_overflow_or_truncation() {
        let market = market(200);
        assert_eq!(controlled_buy_reservation(&market, U256::from(1), 0.5).unwrap(), U256::from(1));
        assert_eq!(controlled_buy_reservation(&market, U256::MAX, 1.0).unwrap(), U256::MAX);
        assert!(controlled_buy_reservation(&market, U256::MAX, 2.0).is_err());
    }

    #[test]
    fn the_route_follows_the_market_flags() {
        assert_eq!(route_of(&market(200)), "ctf");
        let mut neg = market(200);
        neg.is_neg_risk = true;
        assert_eq!(route_of(&neg), "neg-risk");
        let mut yield_bearing = market(200);
        yield_bearing.is_yield_bearing = true;
        assert_eq!(route_of(&yield_bearing), "yield");
    }

    #[test]
    fn the_venue_bound_carries_the_identity_and_the_proof() {
        let proven = ProvenBuyBound {
            signed_hash: format!("0x{}", "a".repeat(64)),
            request_gross_atoms: U256::from(5 * 10u128.pow(18)),
            max_cash_atoms: U256::from(2 * 10u128.pow(18)),
            min_net_shares_atoms: U256::from(4_900_000_000_000_000_000u128),
            fee_rate_bps: 200,
        };
        let bound = to_venue_bound(&proven, &identity(), 7, "9", "ctf", 123);
        assert_eq!(bound.market_id, 7);
        assert_eq!(bound.token_id, "9");
        assert_eq!(bound.max_cash_atoms, proven.max_cash_atoms);
        assert_eq!(bound.min_net_shares_atoms, proven.min_net_shares_atoms);
        assert_eq!(bound.fee_rate_bps, 200);
    }
}
