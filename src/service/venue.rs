//! The seam between the shared pipeline and a venue.
//!
//! Sizing, eligibility, exposure, risk and TP/SL are venue-neutral. A venue
//! supplies market metadata ([`MarketSource`]), order placement
//! ([`OrderVenue`]) and quotes ([`crate::service::midprice::MidpriceSource`]).

use crate::models::{BookSnapshot, PlannedOrder, Side, VenueId};
use crate::service::market_cache::MarketInfo;
use alloy_primitives::U256;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait MarketSource: Send + Sync {
    /// Resolve an outcome token id to its market metadata.
    async fn by_token_id(&self, token_id: &str) -> Result<MarketInfo>;
}

// --- Live execution facts ----------------------------------------------------
//
// A live venue reports what actually happened: how much was filled, at what
// price, what it cost, and whether the order can still fill. Those are separate
// facts, and each one is either known or explicitly unknown. Nothing here
// defaults to zero: a missing fee is not a zero fee, and an unconfirmed fill is
// not a confirmed zero.

/// How the account trades: a plain EOA, or a Predict Account (Kernel) wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountMode {
    Eoa,
    PredictAccount,
}

/// The account an order trades from, and who signs for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIdentity {
    pub chain_id: u64,
    /// The address the venue holds positions and pays from.
    pub trading_address: String,
    pub mode: AccountMode,
    /// The key that signs. Equal to `trading_address` for an EOA.
    pub signer_address: String,
    pub venue: VenueId,
}

/// The stable identity of one order attempt. `local_attempt_id` is always
/// present; a hash and a venue id are added as they become known and never
/// replace each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIdentity {
    pub local_attempt_id: u64,
    pub signed_hash: Option<String>,
    pub venue_order_id: Option<String>,
    pub market_id: u64,
    pub token_id: String,
    pub side: Side,
    pub account: AccountIdentity,
}

/// Whether an authenticated request may be sent again after the venue refused
/// the token.
///
/// A read may be sent once more with a fresh token: a token isn't part of the
/// order. A write is never sent again, not even with the same hash: the venue
/// may already have taken it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPolicy {
    /// A read may be sent once more with a fresh token.
    ReadOnceRefresh,
    /// A write is never sent again.
    NoReplay,
}

/// What the venue did with an order attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The request provably never left.
    NotSubmitted,
    /// The venue took it; nothing about a fill follows from that.
    Accepted,
    /// The request may or may not have reached the venue.
    Unknown,
}

/// How far an attempt got. `MayHaveSubmitted` is the boundary a signed hash
/// crosses: past it, the order may have reached the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPhase {
    Prepared,
    MayHaveSubmitted,
    Submitted,
    Closed,
}

/// What the venue said when the order was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submission {
    /// The request never left, provably.
    NotSubmitted,
    /// The venue refused, and the order provably can't fill.
    RejectedFinal,
    /// The venue took the order. Nothing about a fill follows from that.
    Accepted,
    /// The request may or may not have reached the venue.
    Unknown,
}

/// How much of the order filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantity {
    Unknown,
    Known {
        gross_q_atoms: U256,
        class: QuantityClass,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityClass {
    Zero,
    Partial,
    Full,
}

/// One accounting field: known with a value, or unknown with a reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field<T> {
    Known(T),
    Unknown(String),
}

impl<T> Field<T> {
    pub fn is_known(&self) -> bool {
        matches!(self, Field::Known(_))
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Field::Known(v) => Some(v),
            Field::Unknown(_) => None,
        }
    }
}

/// What the fill actually cost, in collateral atoms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accounting {
    /// Collateral paid for the filled shares, before fees.
    pub gross_cash: Field<U256>,
    /// Fee taken in collateral.
    pub collateral_fee: Field<U256>,
    /// Fee taken in shares.
    pub share_fee: Field<U256>,
    /// Cash after fees: `gross_cash + collateral_fee` for a buy,
    /// `gross_cash - collateral_fee` for a sell.
    pub net_cash: Field<U256>,
    /// Inventory after fees: `gross_q - share_fee` for a buy,
    /// `gross_q + share_fee` for a sell.
    pub net_inventory: Field<U256>,
    /// `gross_cash / gross_q`, or unknown when the quantity is zero.
    pub vwap: Field<U256>,
}

impl Accounting {
    /// Accounting is complete only when every field is known.
    pub fn is_complete(&self) -> bool {
        self.gross_cash.is_known()
            && self.collateral_fee.is_known()
            && self.share_fee.is_known()
            && self.net_cash.is_known()
            && self.net_inventory.is_known()
            && self.vwap.is_known()
    }
}

/// Whether the order can still fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Closure {
    Open,
    /// Removal was asked for; the venue may still fill it.
    Removing,
    /// No proof yet that it can't fill.
    Unproven,
    /// Provably can't fill, with the evidence that says so.
    Final {
        evidence: String,
        final_gross_q: U256,
    },
}

/// One complete reading of an order's execution facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSnapshot {
    pub identity: OrderIdentity,
    pub submission: Submission,
    pub quantity: Quantity,
    pub accounting: Accounting,
    /// Actual matched gross quantity covered by the known accounting fields.
    /// Independent of the newer authoritative order quantity during a scan race.
    pub accounting_gross_q: Option<U256>,
    pub closure: Closure,
    /// Monotonic per order hash. An older reading never overwrites a newer one.
    pub generation: u64,
    /// Every page of the venue's history was read, and the scan ended cleanly.
    pub pages_complete: bool,
    /// The venue's own cumulative filled amount, before and after the scan.
    pub gross_q_before: U256,
    pub gross_q_after: U256,
    /// Where the facts came from, for diagnostics.
    pub source: String,
    pub observed_at: DateTime<Utc>,
    /// The settlement transaction, when the venue named one.
    pub tx_hash: Option<String>,
}

impl ExecutionSnapshot {
    /// A verified zero has no VWAP, but still requires all cash/fee/net fields.
    pub fn accounting_complete(&self) -> bool {
        self.accounting.is_complete()
            || (self.gross_q() == Some(U256::ZERO)
                && self.accounting.gross_cash.value() == Some(&U256::ZERO)
                && self.accounting.collateral_fee.value() == Some(&U256::ZERO)
                && self.accounting.share_fee.value() == Some(&U256::ZERO)
                && self.accounting.net_cash.value() == Some(&U256::ZERO)
                && self.accounting.net_inventory.value() == Some(&U256::ZERO))
    }

    /// The gross quantity this reading adds, or unknown.
    pub fn gross_q(&self) -> Option<U256> {
        match self.quantity {
            Quantity::Known { gross_q_atoms, .. } => Some(gross_q_atoms),
            Quantity::Unknown => None,
        }
    }
}

/// What the account looks like right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub identity: AccountIdentity,
    /// Collateral balance, when it was read.
    pub collateral_atoms: Option<U256>,
    /// Native gas balance of the paying account, when it was read.
    pub gas_atoms: Option<U256>,
    /// Positions held, as token id → shares.
    pub positions: Vec<(String, U256)>,
    /// Closed, settled historical net inventory exactly explains startup positions.
    pub positions_explained: bool,
    /// Orders the venue still lists as open.
    pub open_orders: Vec<String>,
    /// Every required read succeeded and every page was read to the end.
    pub complete: bool,
}

/// What one attempt is worth while it is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub identity: OrderIdentity,
    /// The largest gross quantity this order could fill.
    pub request_gross_atoms: U256,
    /// The worst-case cash this order was reserved against.
    pub r0: U256,
    /// Shares of an existing position this attempt is selling.
    pub reserved_sell: U256,
    pub phase: AttemptPhase,
    pub submission: Submission,
    /// The last failure, as a stable reason token.
    pub last_reason: Option<String>,
}

/// The handle a caller holds across an attempt. The venue writes the hash and
/// the phase into it before the request goes out, so a failure after that point
/// can't lose the order's identity.
pub type AttemptHandle = Arc<Mutex<AttemptRecord>>;

/// A structured failure. `code` is stable; `reason` is a stable token, never a
/// raw error, so nothing sensitive can leak through it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueError {
    pub code: VenueErrorCode,
    pub step: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueErrorCode {
    InvalidInput,
    ExecutionNotEnabled,
    AccountMismatch,
    ReadUnavailable,
    FactIncomplete,
    SubmissionUnknown,
    ReconciliationRequired,
    TxUnconfirmed,
    SdkIncompatible,
    BudgetUnconfirmed,
}

impl VenueError {
    pub fn new(code: VenueErrorCode, step: &str, reason: &str) -> Self {
        Self {
            code,
            step: step.to_string(),
            reason: reason.to_string(),
        }
    }
}

impl std::fmt::Display for VenueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} at {}: {}", self.code, self.step, self.reason)
    }
}

impl std::error::Error for VenueError {}

/// What a venue did with a [`PlannedOrder`].
#[derive(Debug, Clone)]
pub struct OrderTicket {
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    /// `None` until the venue has accepted the order.
    pub order_id: Option<String>,
    /// Shares the venue confirmed filled. `None` when the venue doesn't report
    /// fills synchronously, in which case callers assume the planned size.
    pub filled_shares: Option<f64>,
    /// The hash of the signed order, once it exists.
    pub signed_hash: Option<String>,
    /// The attempt this ticket belongs to.
    pub local_attempt_id: Option<u64>,
    /// What the venue actually reports about the fill. A live venue always
    /// fills this in, even when the facts are unknown.
    pub execution: Option<ExecutionSnapshot>,
}

#[async_trait]
pub trait OrderVenue: Send + Sync {
    fn id(&self) -> VenueId;

    /// Worst-case collateral atoms required before reserving a Predict attempt.
    /// Other venues keep their existing sizing and do not call this method.
    fn reservation_atoms(&self, _planned: &PlannedOrder) -> Result<U256> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "reservation",
            "UNSUPPORTED_VENUE",
        )
        .into())
    }

    /// Build and sign `planned`. Transmits it only when `live` is true, so a
    /// dry run exercises everything up to the wire.
    async fn place(
        &self,
        planned: &PlannedOrder,
        expiration_secs: u64,
        live: bool,
    ) -> Result<OrderTicket>;

    /// Place `planned` while keeping the attempt's identity alive across the
    /// call. Venues that don't track attempts delegate to [`OrderVenue::place`].
    async fn place_tracked(
        &self,
        planned: &PlannedOrder,
        expiration_secs: u64,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<OrderTicket> {
        let _ = handle;
        self.place(planned, expiration_secs, live).await
    }

    /// Stop all new account risk when a consumer detects conflicting ledger facts.
    fn record_account_reconciliation(&self, _reason: &str) {}

    /// Read the execution facts of an order the venue already has.
    async fn execution_snapshot(&self, _signed_hash: &str) -> Result<ExecutionSnapshot> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "execution_snapshot",
            "READ_UNAVAILABLE",
        )
        .into())
    }

    /// Read the account's balances, positions and open orders.
    async fn account_snapshot(&self) -> Result<AccountSnapshot> {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "account_snapshot",
            "READ_UNAVAILABLE",
        )
        .into())
    }
}

/// A market as the strategy bots see it.
#[derive(Debug, Clone, Default)]
pub struct TradableMarket {
    pub id: u64,
    pub question: String,
    pub category_slug: String,
    pub variant: String,
    pub yes_token: String,
    pub no_token: String,
    pub yes_name: String,
    pub no_name: String,
    /// Accepting orders now: open for matching and not past its end.
    pub open: bool,
    /// Under an oracle dispute or paused, whatever the book says.
    pub contested: bool,
    pub tick: f64,
    pub fee_rate_bps: u32,
    pub starts_at: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    pub start_price: Option<f64>,
    pub end_price: Option<f64>,
    pub feed_symbol: Option<String>,
    pub polymarket_condition_ids: Vec<String>,
    /// What one Yes share pays, once that's known.
    pub payout_yes: Option<f64>,
    /// Category and tags, when they came along with the market.
    pub labels: Option<(Option<String>, Vec<String>)>,
}

impl TradableMarket {
    /// Taker fee per share at `price`. Makers pay none.
    pub fn taker_fee(&self, price: f64) -> f64 {
        f64::from(self.fee_rate_bps) / 10_000.0 * price.min(1.0 - price).max(0.0)
    }

    pub fn secs_left(&self, now: DateTime<Utc>) -> Option<i64> {
        self.ends_at.map(|end| (end - now).num_seconds())
    }

    pub fn is_yes(&self, token: &str) -> bool {
        self.yes_token == token
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketQuery {
    /// Those variants, with their schedules. Empty means the most traded markets
    /// of any variant, without schedules: see [`MarketData::with_schedule`].
    pub variants: Vec<String>,
    pub max_pages: u32,
}

/// Read side the strategy bots share. Books are quoted for the Yes outcome.
#[async_trait]
pub trait MarketData: Send + Sync {
    async fn markets(&self, query: &MarketQuery) -> Result<Vec<TradableMarket>>;
    async fn market(&self, id: u64) -> Result<TradableMarket>;
    /// `market` with its end time, category and tags filled in. A scan leaves these
    /// out so that it doesn't need a request per market.
    async fn with_schedule(&self, market: &TradableMarket) -> Result<TradableMarket>;
    async fn books(&self, ids: &[u64]) -> Result<HashMap<u64, BookSnapshot>>;
}

#[derive(Debug, Clone)]
pub struct LimitOrder {
    pub market_id: u64,
    pub token_id: String,
    pub side: Side,
    pub shares: f64,
    pub price: f64,
    pub ttl_secs: u64,
    pub post_only: bool,
}

#[derive(Debug, Clone)]
pub struct PostedOrder {
    pub order_hash: String,
    /// `None` on a dry run, where nothing reached the venue.
    pub order_id: Option<String>,
    /// The attempt this order belongs to.
    pub local_attempt_id: Option<u64>,
    /// What the venue reports about the fill, once it knows.
    pub execution: Option<ExecutionSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoveResult {
    pub removed: Vec<String>,
    /// Already filled, removed or cancelled.
    pub noop: Vec<String>,
    /// Inside the market's removal lock; retry later.
    pub rejected: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestingState {
    pub open: bool,
    pub filled_shares: f64,
    /// The venue's own execution facts, when it has any.
    pub execution: Option<ExecutionSnapshot>,
}

#[derive(Debug, Clone)]
pub struct HeldPosition {
    pub market: TradableMarket,
    pub token_id: String,
    pub shares: f64,
    pub avg_price: f64,
}

/// Resting orders, on top of the taker path in [`OrderVenue`].
#[async_trait]
pub trait LimitVenue: Send + Sync {
    /// Build and sign `order`. Transmits it only when `live` is true.
    async fn post_limit(&self, order: &LimitOrder, live: bool) -> Result<PostedOrder>;

    /// Post `order` while keeping the attempt's identity alive across the call.
    /// Venues that don't track attempts delegate to [`LimitVenue::post_limit`].
    async fn post_limit_tracked(
        &self,
        order: &LimitOrder,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<PostedOrder> {
        let _ = handle;
        self.post_limit(order, live).await
    }

    /// Take orders off the book. The venue says this doesn't cancel them on-chain.
    async fn remove(&self, order_ids: &[String]) -> Result<RemoveResult>;
    async fn order_state(&self, order_hash: &str) -> Result<RestingState>;
    async fn positions(&self) -> Result<Vec<HeldPosition>>;
}
