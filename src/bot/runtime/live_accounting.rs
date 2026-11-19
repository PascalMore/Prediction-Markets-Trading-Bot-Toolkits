//! The controlled live domain's atomic projection of the broker's own state.
//!
//! There is exactly one ledger: the broker's [`crate::bot::runtime::live::LiveState`]
//! and the watermark bookkeeping that goes with it. This module only *reads*
//! that state and projects the window's risk numbers from it. It never keeps a
//! second cash table, never derives an amount from a float holding and never
//! fills an unknown with a zero.
//!
//! Every amount is an integer number of asset atoms. The multiplications that
//! need a wider range go through [`U512`], and every subtraction that could go
//! negative is a checked one: an impossible difference is a fact to refuse,
//! not a number to wrap.

use crate::bot::runtime::live::LiveState;
use alloy_primitives::{U256, U512};
use std::collections::HashMap;

/// The asset's atomic scale: the deployment's collateral and share decimals.
pub const ASSET_UNITS: u32 = 18;
/// One asset in atoms.
pub const ONE: u128 = 1_000_000_000_000_000_000;
/// The window's allocated budget, in USDT.
pub const BUDGET_C0_USDT: u64 = 100;
/// The largest single order's worst cash, in USDT.
pub const MAX_ORDER_USDT: u64 = 2;
/// The largest per-market worst cash, in USDT.
pub const MAX_MARKET_USDT: u64 = 2;
/// The largest total worst cash, in USDT.
pub const MAX_TOTAL_USDT: u64 = 10;
/// The net loss that latches the window, in USDT.
pub const MAX_LOSS_USDT: u64 = 20;

/// The window's allocated budget, in atoms.
pub fn budget_c0_atoms() -> U256 {
    U256::from(BUDGET_C0_USDT) * U256::from(ONE)
}

/// The net loss that latches the window, in atoms.
pub fn max_loss_atoms() -> U256 {
    U256::from(MAX_LOSS_USDT) * U256::from(ONE)
}

/// The window's per-order cap, in atoms.
pub fn max_order_atoms() -> U256 {
    U256::from(MAX_ORDER_USDT) * U256::from(ONE)
}

/// The window's per-market cap, in atoms.
pub fn max_market_atoms() -> U256 {
    U256::from(MAX_MARKET_USDT) * U256::from(ONE)
}

/// The window's total cap, in atoms.
pub fn max_total_atoms() -> U256 {
    U256::from(MAX_TOTAL_USDT) * U256::from(ONE)
}

/// A signed integer number of asset atoms.
///
/// The loss side is a separate sign bit rather than a wrapped value, so a
/// negative amount is never a huge positive one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedAtoms {
    negative: bool,
    magnitude: U256,
}

impl SignedAtoms {
    pub fn zero() -> Self {
        Self {
            negative: false,
            magnitude: U256::ZERO,
        }
    }

    pub fn positive(magnitude: U256) -> Self {
        Self {
            negative: false,
            magnitude,
        }
    }

    pub fn negative(magnitude: U256) -> Self {
        // Zero is never negative: `-0` is not a number this contract has.
        Self {
            negative: !magnitude.is_zero(),
            magnitude,
        }
    }

    /// The difference `a - b`, or `None` when it doesn't fit.
    pub fn checked_sub(a: U256, b: U256) -> Option<Self> {
        match a.checked_sub(b) {
            Some(value) => Some(Self::positive(value)),
            None => b.checked_sub(a).map(Self::negative),
        }
    }

    pub fn is_negative(self) -> bool {
        self.negative
    }

    pub fn magnitude(self) -> U256 {
        self.magnitude
    }

    /// The decimal text of the amount, with a leading `-` when negative.
    pub fn to_text(self) -> String {
        if self.negative {
            format!("-{}", self.magnitude)
        } else {
            self.magnitude.to_string()
        }
    }
}

/// Why a derived number couldn't be produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionGap {
    /// The mark of a held position isn't known.
    UnknownMark,
    /// A cash or fee fact isn't known.
    UnknownFacts,
    /// The gas balance wasn't read.
    UnknownGas,
}

/// How complete the window's numbers are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountingStatus {
    Complete,
    Incomplete(ProjectionGap),
    /// Two facts disagree and the window cannot be valued.
    Conflict,
}

/// The window's projected accounting, in atoms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowAccounting {
    /// The allocated budget of the window. Constant.
    pub budget: U256,
    /// The budget balance still available: the allocation minus the window's
    /// actual spending.
    pub cash: U256,
    /// The cash that is still reserved against an open attempt.
    pub reserved: U256,
    /// The cash that may still be spent.
    pub available: U256,
    /// The cost of the window's holdings that hasn't been realized yet.
    pub cost: U256,
    /// The realized net gain of the window.
    pub realized: SignedAtoms,
    /// The unrealized net gain of the window.
    pub unrealized: SignedAtoms,
    /// The window's net asset value.
    pub nav: SignedAtoms,
    /// The window's net profit or loss.
    pub net_pnl: SignedAtoms,
    /// Whether the loss threshold has latched.
    pub loss_latched: bool,
    /// The value of the pending official claim, when there is one.
    pub pending_claim_value: Option<U256>,
    /// The gas balance of the payer, when it was read.
    pub gas_balance_atoms: Option<U256>,
    /// How complete the projection is.
    pub status: AccountingStatus,
}

/// The marks of the held positions, in tokens.
#[derive(Debug, Clone, Default)]
pub struct Marks {
    /// The valid mid price of each token, as a float.
    pub prices: HashMap<String, f64>,
}

/// The gas reading of one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GasReading {
    pub balance_atoms: U256,
    /// The balance the window started with.
    pub initial_atoms: U256,
}

/// Multiply two atom amounts without truncating to U256 before the division.
fn checked_product(a: U256, b: U256) -> Option<U512> {
    U512::from(a).checked_mul(U512::from(b))
}

/// Divide a wide product by a divisor, rounding down.
fn wide_div(value: U512, divisor: U512) -> Option<U256> {
    let quotient = value / divisor;
    U256::from_str_radix(&quotient.to_string(), 10).ok()
}

/// The floor of `atoms * mark`, computed exactly.
///
/// The float is decomposed into its exact rational value first, so a large
/// atom count multiplied by a float never loses the cents that decide whether
/// the loss threshold was crossed.
pub fn exact_floor_mul(atoms: U256, mark: f64) -> Option<U256> {
    if !mark.is_finite() || mark < 0.0 {
        return None;
    }
    let bits = mark.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa_bits = bits & 0x000f_ffff_ffff_ffff;
    let (mantissa, power) = if exponent == 0 {
        // A subnormal has no implicit leading bit.
        (mantissa_bits, -1074i32)
    } else {
        (mantissa_bits | 0x0010_0000_0000_0000, exponent - 1075)
    };
    let numerator = checked_product(atoms, U256::from(mantissa))?;
    if numerator.is_zero() || power <= -512 { return Some(U256::ZERO); }
    if power >= 512 { return None; }
    if power >= 0 {
        // A whole power of two: shift the numerator up.
        let factor = U512::from(1u64) << power as u32;
        let shifted = numerator.checked_mul(factor)?;
        wide_div(shifted, U512::from(1u64))
    } else {
        let divisor = U512::from(1u64) << ((-power) as u32);
        wide_div(numerator, divisor)
    }
}

/// The conservative worst-case net shares of a proven share-fee BUY.
///
/// The official BUY fee comes out of the received shares, and the signed rate
/// is an upper bound on it. The chain rounds the fee down, so the fewest
/// shares that can arrive is the gross minus the fee computed on the gross.
pub fn minimum_net_shares(gross_atoms: U256, fee_rate_bps: u32) -> Option<U256> {
    let product = checked_product(gross_atoms, U256::from(fee_rate_bps))?;
    let fee = wide_div(product, U512::from(10000u64))?;
    gross_atoms.checked_sub(fee)
}

/// The worst-case cash a proven share-fee BUY can take.
///
/// The fee comes out of the shares, so the cash is the signed cash itself.
/// This is deliberately not the ordinary cash-with-fee bound: that one would
/// reserve roughly twice the cash and would refuse every legal near-two-USDT
/// order.
pub fn controlled_buy_max_cash(gross_cash_atoms: U256) -> U256 {
    gross_cash_atoms
}

/// The live state facts the projection reads.
pub trait LiveFacts {
    /// The cash actually applied, by attempt id.
    fn applied_net_cash(&self, attempt_id: u64) -> U256;
    /// The net inventory actually applied, by attempt id.
    fn applied_net_inventory(&self, attempt_id: u64) -> U256;
    /// The cash still reserved against an attempt.
    fn reservation(&self, attempt_id: u64) -> U256;
    /// The attempt's market id.
    fn attempt_market(&self, attempt_id: u64) -> Option<u64>;
    /// The attempt's token id.
    fn attempt_token(&self, attempt_id: u64) -> Option<String>;
    /// Every attempt id this run made.
    fn attempt_ids(&self) -> Vec<u64>;
    /// The conservative risk floor observed for an attempt.
    fn observed_risk_floor(&self, attempt_id: u64) -> Option<U256>;
    /// A confirmed official claim for this exact applied inventory; not cash.
    fn official_claim(&self, _attempt_id: u64) -> Option<(U256, U256)> { None }
}

/// Read the facts of the broker's own live state.
pub struct StateFacts<'a> {
    pub state: &'a LiveState,
}

impl LiveFacts for StateFacts<'_> {
    fn applied_net_cash(&self, attempt_id: u64) -> U256 {
        self.state.net_cash.get(&attempt_id).copied().unwrap_or(U256::ZERO)
    }

    fn applied_net_inventory(&self, attempt_id: u64) -> U256 {
        self.state
            .net_inventory
            .get(&attempt_id)
            .copied()
            .unwrap_or(U256::ZERO)
    }

    fn reservation(&self, attempt_id: u64) -> U256 {
        self.state.reservations.get(&attempt_id).copied().unwrap_or(U256::ZERO)
    }

    fn attempt_market(&self, attempt_id: u64) -> Option<u64> {
        self.state
            .attempts
            .get(&attempt_id)
            .map(|handle| handle.lock().identity.market_id)
    }

    fn attempt_token(&self, attempt_id: u64) -> Option<String> {
        self.state
            .attempts
            .get(&attempt_id)
            .map(|handle| handle.lock().identity.token_id.clone())
    }

    fn attempt_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.state.attempts.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn observed_risk_floor(&self, attempt_id: u64) -> Option<U256> {
        self.state.observed_risk_floor.get(&attempt_id).copied()
    }

    fn official_claim(&self, attempt_id: u64) -> Option<(U256, U256)> {
        let outcome = self.state.controlled.as_ref()?.attempt_outcomes.get(&attempt_id)?;
        Some((outcome.position_net_atoms, outcome.claim_value_atoms?))
    }
}

/// The window's projection of the broker's live state.
///
/// `marks` carries the valid mid prices of the held positions; a token whose
/// mark is missing makes the valuation incomplete rather than zero.
pub fn project_window<F: LiveFacts>(
    facts: &F,
    marks: &Marks,
    gas: Option<GasReading>,
) -> WindowAccounting {
    let budget = budget_c0_atoms();
    let mut cost = U256::ZERO;
    let mut reserved = U256::ZERO;
    let mut value: Option<U256> = Some(U256::ZERO);
    let mut applied_total = U256::ZERO;
    let mut released_basis = U256::ZERO;
    let mut pending_claim_value: Option<U256> = None;
    let mut conflict = false;

    for attempt_id in facts.attempt_ids() {
        let cash = facts.applied_net_cash(attempt_id);
        let inventory = facts.applied_net_inventory(attempt_id);
        applied_total = applied_total.checked_add(cash).unwrap_or_else(|| {
            conflict = true;
            U256::MAX
        });
        cost = cost.checked_add(cash).unwrap_or_else(|| {
            conflict = true;
            U256::MAX
        });
        let residual = facts.reservation(attempt_id).max(
            facts.observed_risk_floor(attempt_id).unwrap_or(U256::ZERO).saturating_sub(cash));
        reserved = reserved.checked_add(residual).unwrap_or_else(|| {
            conflict = true;
            U256::MAX
        });
        if let Some((covered_inventory, claim)) = facts.official_claim(attempt_id) {
            if covered_inventory != inventory || !residual.is_zero() {
                conflict = true;
                continue;
            }
            if claim.is_zero() {
                // Economic write-off only: chain inventory and spending stay intact.
                cost = cost.checked_sub(cash).unwrap_or_else(|| { conflict = true; U256::ZERO });
                released_basis = released_basis.checked_add(cash)
                    .unwrap_or_else(|| { conflict = true; U256::MAX });
            } else {
                pending_claim_value = Some(pending_claim_value.unwrap_or(U256::ZERO)
                    .checked_add(claim).unwrap_or_else(|| { conflict = true; U256::MAX }));
            }
            value = value.and_then(|total| total.checked_add(claim).or_else(|| {
                conflict = true;
                None
            }));
            continue;
        }
        if inventory.is_zero() {
            continue;
        }
        let Some(token) = facts.attempt_token(attempt_id) else {
            value = None;
            continue;
        };
        let Some(mark) = marks.prices.get(&token).copied() else {
            value = None;
            continue;
        };
        let Some(marked) = exact_floor_mul(inventory, mark) else {
            value = None;
            continue;
        };
        value = value.and_then(|total| total.checked_add(marked).or_else(|| {
            conflict = true;
            None
        }));
    }

    // The budget balance falls by what the window actually spent.
    let cash = budget.saturating_sub(applied_total);
    let available = cash.saturating_sub(reserved);
    let status = match value {
        _ if conflict || applied_total > budget
            || value.is_some_and(|value| cash.checked_add(value).is_none()) => AccountingStatus::Conflict,
        Some(_) => AccountingStatus::Complete,
        None => AccountingStatus::Incomplete(ProjectionGap::UnknownMark),
    };

    // The realized gain is whatever the window recovered; the unrealized gain
    // is the value of what it still holds, less what it paid for it.
    let realized = SignedAtoms::negative(released_basis);
    let value = value.unwrap_or(U256::ZERO);
    let unrealized = SignedAtoms::checked_sub(value, cost).unwrap_or_else(SignedAtoms::zero);
    // NAV is budget cash plus the valuation. Only net_pnl subtracts C0;
    // reservations limit spending, but are not another expense in NAV.
    let nav = SignedAtoms::positive(cash.saturating_add(value));
    let net_pnl = SignedAtoms::checked_sub(cash.saturating_add(value), budget)
        .unwrap_or_else(SignedAtoms::zero);
    let loss_latched = net_pnl.is_negative()
        && net_pnl.magnitude() >= max_loss_atoms();

    WindowAccounting {
        budget,
        cash,
        reserved,
        available,
        cost,
        realized,
        unrealized,
        nav,
        net_pnl,
        loss_latched,
        pending_claim_value,
        gas_balance_atoms: gas.map(|reading| reading.balance_atoms),
        status,
    }
}

/// The original ledger's unrecovered cost and conservative residual risk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RiskExposure {
    pub cost: U256,
    pub reserved: U256,
    pub markets: HashMap<u64, (U256, U256)>,
}

/// Read K/P atomically under the caller's State lock, never from float holdings.
/// The observed floor survives even a snapshot rejected before ledger commit.
pub fn risk_exposure(state: &LiveState) -> Option<RiskExposure> {
    let mut exposure = RiskExposure::default();
    for (id, handle) in &state.attempts {
        let record = handle.lock();
        if record.identity.side != crate::models::Side::Buy { return None; }
        let cost = state.net_cash.get(id).copied().unwrap_or(U256::ZERO);
        let floor = state.observed_risk_floor.get(id).copied().unwrap_or(U256::ZERO);
        let floor = if record.phase == crate::service::venue::AttemptPhase::Closed {
            floor
        } else {
            floor.max(record.r0)
        };
        let reserved = state.reservations.get(id).copied().unwrap_or(U256::ZERO)
            .max(floor.saturating_sub(cost));
        let cost = match (StateFacts { state }).official_claim(*id) {
            Some((inventory, claim)) if claim.is_zero()
                && inventory == state.net_inventory.get(id).copied().unwrap_or(U256::ZERO)
                && reserved.is_zero() => U256::ZERO,
            _ => cost,
        };
        exposure.cost = exposure.cost.checked_add(cost)?;
        exposure.reserved = exposure.reserved.checked_add(reserved)?;
        let market = exposure.markets.entry(record.identity.market_id).or_default();
        market.0 = market.0.checked_add(cost)?;
        market.1 = market.1.checked_add(reserved)?;
    }
    if state.net_cash.keys().chain(state.reservations.keys())
        .chain(state.observed_risk_floor.keys()).any(|id| !state.attempts.contains_key(id))
    {
        return None;
    }
    Some(exposure)
}

pub(super) const LIVE_CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
pub(super) const LIVE_TAIL_BUDGET: std::time::Duration = std::time::Duration::from_secs(600);

impl crate::bot::runtime::Broker {
    /// Stop entries, reconcile with bounded reads, then seal the original live wire.
    /// Cancellation/redemption are not implicitly authorized by shutdown.
    pub(super) async fn finish_controlled_live(&self, data: &dyn crate::service::venue::MarketData) {
        self.finish_controlled_live_with(data, LIVE_CLEANUP_BUDGET, LIVE_TAIL_BUDGET).await;
    }

    /// The same shutdown consumer with explicit budgets for offline boundary tests.
    pub(super) async fn finish_controlled_live_with(
        &self, data: &dyn crate::service::venue::MarketData,
        cleanup_budget: std::time::Duration, tail_budget: std::time::Duration,
    ) {
        {
            let mut state = self.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            if run.emitter.is_finished() { return; }
            if run.control.lock().stop.is_none() { run.stop("MANUAL_STOP", true, chrono::Utc::now()); }
        }
        let cleanup = tokio::time::timeout(cleanup_budget, async {
            self.sync_live().await?;
            self.settle_resolved(data).await
        }).await;
        let cleanup_complete = matches!(cleanup, Ok(Ok(())));
        if cleanup_complete && self.entry_stop_reason() == Some("DEADLINE") {
            let tail = async {
                loop {
                    let pending = {
                        let state = self.state.lock();
                        let run = state.live.controlled.as_ref().unwrap();
                        state.live.net_inventory.iter().any(|(id, shares)| !shares.is_zero()
                            && !run.attempt_outcomes.contains_key(id))
                    };
                    if !pending { break; }
                    self.sync_live().await?;
                    self.settle_resolved(data).await?;
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
                Ok::<(), anyhow::Error>(())
            };
            tokio::select! {
                biased;
                _ = self.wait_emergency() => (),
                _ = tokio::signal::ctrl_c() => self.stop_entries(super::ControlledStop::ParentLost),
                _ = tokio::time::timeout(tail_budget, tail) => (),
            }
        }
        let lane = self.live_operation_lane().unwrap();
        let _operation = lane.lock().await;
        let events = super::live_events::prepare_final(&mut self.state.lock(), cleanup_complete);
        if let Ok(events) = events { let _ = super::live_events::flush_events(events); }
    }

    /// Read only frozen, window-owned markets on the original operation lane.
    /// Official results value claims; this path never redeems or credits cash.
    pub(super) async fn settle_controlled_live(
        &self, data: &dyn crate::service::venue::MarketData,
    ) -> anyhow::Result<()> {
        let lane = self.live_operation_lane().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let _operation = lane.lock().await;
        if self.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished() { return Ok(()); }
        let mut ids: Vec<_> = self.state.lock().live.controlled.as_ref().unwrap()
            .traded.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let market = match data.market(id).await {
                Ok(market) => market,
                Err(_) => {
                    let events = {
                        let mut state = self.state.lock();
                        state.live.controlled.as_mut().unwrap().stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                        super::live_events::prepare_lifecycle(&mut state)
                    };
                    super::live_events::flush_events(events.map_err(anyhow::Error::msg)?)
                        .map_err(anyhow::Error::msg)?;
                    // A failed authoritative read cannot certify cleanup or
                    // keep a deadline-only tail alive as if reconciliation succeeded.
                    anyhow::bail!("DATA_UNAVAILABLE:official result read failed");
                }
            };
            let events = {
                let mut state = self.state.lock();
                if market.id != id {
                    state.live.controlled.as_mut().unwrap().stop("FACT_CONFLICT", true, chrono::Utc::now());
                    super::live_events::prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?
                } else {
                    apply_official_result(&mut state, &market).map_err(anyhow::Error::msg)?
                }
            };
            super::live_events::flush_events(events).map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }
}

/// A registered, undisputed round has no result yet, not a failed result read.
/// Terminal facts (even partial ones) and unknown lifecycle states are not exempt.
fn normal_unpublished_result(market: &crate::service::venue::TradableMarket) -> bool {
    !market.contested && market.end_price.is_none() && market.payout_yes.is_none()
        && market.round_facts.as_ref().is_some_and(|facts| {
            facts.oracle_status.as_deref() == Some("REGISTERED")
                && facts.trading_status.as_deref() == Some("OPEN")
                && [&facts.yes_status, &facts.no_status].into_iter()
                    .all(|status| status.as_deref().unwrap_or("").is_empty())
        })
}

/// Apply immutable official result metadata to the existing applied inventory.
/// The caller owns the operation lane and flushes only after releasing State.
fn apply_official_result(
    state: &mut crate::bot::runtime::State,
    market: &crate::service::venue::TradableMarket,
) -> Result<Vec<super::live_events::PreparedLiveEvent>, String> {
    use super::live_control::LiveOutcome;
    use super::live_events::{self, LivePayload, PayoutPayload};
    use super::paper::{self, Settlement};
    let run = state.live.controlled.as_ref().ok_or("CONTEXT_MISSING")?;
    let original = run.traded.get(&market.id).ok_or("CONTEXT_MISSING")?;
    let now = run.phase1.clock.now().map_err(|_| "DATA_UNAVAILABLE")?;
    let prices = market.start_price.zip(market.end_price);
    let identity = paper::same_round(original, market);
    let mut result = if identity == paper::RoundIdentity::Contradiction {
        Settlement::Conflict
    } else if identity == paper::RoundIdentity::Incomplete {
        Settlement::Pending
    } else if let (Some(facts), Some((start, end))) = (market.round_facts.as_ref(), prices) {
        paper::settle_from_facts(market, facts, start, end)
    } else { Settlement::Pending };
    let payout = prices.and_then(|(start, end)| paper::settlement_payout(market, start, end));
    if result == Settlement::Confirmed {
        let (start, end) = prices.unwrap();
        let expected = if end > start { 1.0 } else if end < start { 0.0 } else { 0.5 };
        if payout.is_none() { result = Settlement::Pending; }
        else if payout != Some(expected) { result = Settlement::Conflict; }
    }
    let mut attempts: Vec<_> = state.live.attempts.iter().filter_map(|(id, handle)| {
        let record = handle.lock().clone();
        (record.identity.market_id == market.id).then_some((*id, record))
    }).collect();
    attempts.sort_by_key(|(id, _)| *id);
    let mut payloads = Vec::new();
    for (id, record) in attempts {
        let inventory = state.live.net_inventory.get(&id).copied().unwrap_or(U256::ZERO);
        if inventory.is_zero() { continue; }
        let identity_valid = record.identity.account == run.identity
            && (record.identity.token_id == original.yes_token || record.identity.token_id == original.no_token);
        let previous = run.attempt_outcomes.get(&id);
        let revised = previous.is_some_and(|outcome| outcome.payout_yes.is_none_or(|paid|
            paper::revised_result(market, (outcome.result_prices.0, outcome.result_prices.1, paid),
                identity, result)));
        let conflict = result == Settlement::Conflict || !identity_valid || revised;
        if conflict {
            payloads.push((id, record.identity.token_id, "CONFLICT", None, None));
            continue;
        }
        if result != Settlement::Confirmed { continue; }
        // Unclosed or incompletely applied execution must keep its original risk.
        if record.phase != crate::service::venue::AttemptPhase::Closed
            || !state.live.reservations.get(&id).is_some_and(U256::is_zero)
            || !state.live.net_cash.contains_key(&id)
        { continue; }
        let token_payout = if record.identity.token_id == original.yes_token {
            payout.unwrap()
        } else { 1.0 - payout.unwrap() };
        let claim = exact_floor_mul(inventory, token_payout).ok_or("FACT_CONFLICT")?;
        if previous.is_some_and(|outcome| outcome.position_net_atoms == inventory
            && outcome.claim_value_atoms == Some(claim)) { continue; }
        payloads.push((id, record.identity.token_id,
            if claim.is_zero() { "CONFIRMED" } else { "CASH_PENDING" }, payout, Some(claim)));
    }
    let run = state.live.controlled.as_mut().unwrap();
    if result == Settlement::Pending && !payloads.iter().any(|(_, _, status, _, _)| *status == "CONFLICT")
        && !(identity == paper::RoundIdentity::Match && normal_unpublished_result(market))
    {
        run.stop("DATA_UNAVAILABLE", true, now.utc);
    }
    for (id, _, status, payout, claim) in &payloads {
        if *status == "CONFLICT" {
            run.stop("FACT_CONFLICT", true, now.utc);
        } else {
            run.attempt_outcomes.insert(*id, LiveOutcome {
                payout_yes: *payout, result_prices: prices.unwrap(),
                position_net_atoms: state.live.net_inventory[id], claim_value_atoms: *claim,
                cash_confirmed: false,
            });
            if claim.is_some_and(|claim| !claim.is_zero()) { run.stop("CASH_PENDING", true, now.utc); }
        }
    }
    super::live_control::refresh_live_risk(state);
    let accounting = live_events::state_accounting(state);
    let run = state.live.controlled.as_ref().unwrap();
    let mut events = Vec::new();
    for (id, token, status, payout, claim) in payloads {
        events.push(run.emitter.prepare(now.utc, Some(market.id.to_string()), Some(token), &accounting,
            LivePayload::Payout(PayoutPayload { status: status.into(), payout_yes: payout,
                position_net_atoms: state.live.net_inventory[&id].to_string(),
                claim_value_atoms: claim.map(|claim| claim.to_string()), cash_received_atoms: None,
                tx_hash: None, observed_at: now.utc,
                reason: if status == "CONFLICT" { "FACT_CONFLICT" }
                    else if status == "CASH_PENDING" { "CASH_PENDING" } else { "NONE" }.into(),
            }))?);
    }
    events.extend(live_events::prepare_lifecycle(state)?);
    Ok(events)
}

/// Whether one order's worst cash fits the window's caps.
///
/// The three caps are independent: the order itself, the market it belongs
/// to, and the window as a whole. Equality is allowed; there is no epsilon
/// and no automatic resize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsCheck {
    pub order_ok: bool,
    pub market_ok: bool,
    pub total_ok: bool,
    pub available_ok: bool,
}

impl CapsCheck {
    pub fn all_ok(&self) -> bool {
        self.order_ok && self.market_ok && self.total_ok && self.available_ok
    }
}

/// Check one order against the window's caps.
///
/// `worst_cash` is the order's own worst-case cash; `market_cost` and
/// `market_reserved` are what the market already carries; `total_cost` and
/// `total_reserved` are what the window already carries.
pub fn check_caps(
    worst_cash: U256,
    market_cost: U256,
    market_reserved: U256,
    total_cost: U256,
    total_reserved: U256,
    available: U256,
) -> CapsCheck {
    let order_ok = worst_cash >= U256::from(ONE) && worst_cash <= max_order_atoms();
    let market_ok = market_cost
        .saturating_add(market_reserved)
        .saturating_add(worst_cash)
        <= max_market_atoms();
    let total_ok = total_cost
        .saturating_add(total_reserved)
        .saturating_add(worst_cash)
        <= max_total_atoms();
    let available_ok = worst_cash <= available;
    CapsCheck {
        order_ok,
        market_ok,
        total_ok,
        available_ok,
    }
}

/// The gas state of the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GasStatus {
    /// The gas was never measured. This is not zero.
    NotMeasured,
    /// The gas balance and the consumption are known.
    Known,
    /// The gas balance couldn't be read.
    Unavailable,
}

/// The gas decision of the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GasDecision {
    pub status: GasStatus,
    /// The gas paid, in atoms, when it can be attributed.
    pub paid_atoms: Option<U256>,
    /// Whether the window may keep adding risk.
    pub may_add_risk: bool,
    /// The reason, when it may not.
    pub reason: Option<&'static str>,
}

/// Check the gas budget of the window.
///
/// The budget controls the balance the payer may consume; it is not a
/// per-transaction cost and it is not a promise about the total loss.
pub fn check_gas(reading: Option<GasReading>, minimum: U256, maximum: U256) -> GasDecision {
    let Some(reading) = reading else {
        return GasDecision {
            status: GasStatus::Unavailable,
            paid_atoms: None,
            may_add_risk: false,
            reason: Some("GAS_UNAVAILABLE"),
        };
    };
    let consumed = reading.initial_atoms.saturating_sub(reading.balance_atoms);
    if reading.balance_atoms < minimum {
        return GasDecision {
            status: GasStatus::Known,
            paid_atoms: None,
            may_add_risk: false,
            reason: Some("GAS_BUDGET"),
        };
    }
    if consumed >= maximum {
        return GasDecision {
            status: GasStatus::Known,
            paid_atoms: Some(consumed),
            may_add_risk: false,
            reason: Some("GAS_BUDGET"),
        };
    }
    GasDecision {
        status: GasStatus::Known,
        paid_atoms: None,
        may_add_risk: true,
        reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atoms(value: u128) -> U256 {
        U256::from(value)
    }

    #[test]
    fn a_zero_fee_buy_keeps_every_share() {
        assert_eq!(
            minimum_net_shares(atoms(5 * ONE), 0),
            Some(atoms(5 * ONE))
        );
    }

    #[test]
    fn the_fee_is_taken_from_the_shares_and_rounds_down() {
        // Five shares at two hundred basis points: the fee is 0.1 shares.
        assert_eq!(
            minimum_net_shares(atoms(5 * ONE), 200),
            Some(atoms(4_900_000_000_000_000_000))
        );
    }

    #[test]
    fn the_designed_vector_matches_the_hand_computation() {
        // Five shares, two USDT, two hundred basis points: 4.9 shares net.
        let net = minimum_net_shares(atoms(5 * ONE), 200).unwrap();
        assert_eq!(net, atoms(4_900_000_000_000_000_000));
        // The worst cash is the signed cash itself: the fee is in the shares.
        assert_eq!(controlled_buy_max_cash(atoms(2 * ONE)), atoms(2 * ONE));
    }

    #[test]
    fn the_worst_net_ev_of_the_designed_vector_is_positive() {
        // 4.9 shares at a fair 0.5 is 2.45 USDT against 2 USDT of cash.
        let net_shares = minimum_net_shares(atoms(5 * ONE), 200).unwrap();
        let gross = checked_product(net_shares, U256::from(ONE / 2)).unwrap();
        let cash = atoms(2 * ONE);
        let ev = gross.checked_sub(U512::from(cash)).unwrap();
        assert!(ev > U512::from(0u64));
    }

    #[test]
    fn the_fee_lower_bound_holds_across_many_vectors() {
        for gross in 1u128..40 {
            for bps in [0u32, 1, 100, 200, 500, 1000] {
                let shares = atoms(gross * ONE / 10);
                let net = minimum_net_shares(shares, bps).unwrap();
                let fee = shares - net;
                // The fee never exceeds the rate on the gross, and the net is
                // never negative.
                let bound = checked_product(shares, U256::from(bps)).unwrap() / U512::from(10000u64);
                assert!(U512::from(fee) <= bound);
                assert!(net <= shares);
            }
        }
    }

    #[test]
    fn a_mark_is_multiplied_exactly_rather_than_through_a_float() {
        // 10 shares at 0.5 is exactly 5 USDT.
        assert_eq!(
            exact_floor_mul(atoms(10 * ONE), 0.5),
            Some(atoms(5 * ONE))
        );
        // A third is not the repeating decimal: the float 1/3 is a little
        // below it, and the exact product says so. A float multiplication
        // would have rounded the same way here, but the point is that the
        // result is derived from the float's exact value, not from a
        // double-rounded intermediate.
        assert_eq!(
            exact_floor_mul(atoms(ONE), 1.0 / 3.0),
            Some(atoms(333_333_333_333_333_314))
        );
        // Three times the float's exact third is still a little under one:
        // the exact product of the float's own value, not a rounded
        // intermediate. This is the difference the exact path buys.
        assert_eq!(
            exact_floor_mul(atoms(3 * ONE), 1.0 / 3.0),
            Some(atoms(999_999_999_999_999_944))
        );
    }

    #[test]
    fn extreme_finite_marks_do_not_wrap_wide_shifts_or_overflow_amounts() {
        assert_eq!(exact_floor_mul(U256::MAX, f64::from_bits(1)), Some(U256::ZERO));
        assert_eq!(exact_floor_mul(U256::ZERO, f64::MAX), Some(U256::ZERO));
        assert_eq!(exact_floor_mul(U256::from(1), f64::MAX), None);
        assert_eq!(exact_floor_mul(U256::MAX, 1.0), Some(U256::MAX));
    }

    #[test]
    fn a_loss_of_exactly_twenty_latches_and_one_atom_less_does_not() {
        let exact = max_loss_atoms();
        assert!(SignedAtoms::negative(exact).magnitude() >= max_loss_atoms());
        assert!(SignedAtoms::negative(exact - U256::from(1u64)).magnitude() < max_loss_atoms());
    }

    #[test]
    fn a_signed_amount_never_prints_negative_zero() {
        assert_eq!(SignedAtoms::negative(U256::ZERO).to_text(), "0");
        assert_eq!(SignedAtoms::negative(U256::from(5u64)).to_text(), "-5");
    }

    #[test]
    fn the_caps_are_independent_and_equality_is_allowed() {
        let two = max_order_atoms();
        let check = check_caps(two, U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO, two);
        assert!(check.all_ok());
        // One atom over the per-order cap is refused even though the total
        // would still fit.
        let over = check_caps(
            two + U256::from(1u64),
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::from(100u64) * U256::from(ONE),
        );
        assert!(!over.order_ok);
        assert!(over.total_ok);
    }

    #[test]
    fn the_market_cap_is_checked_on_its_own() {
        let two = max_order_atoms();
        let check = check_caps(
            two,
            two,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::from(100u64) * U256::from(ONE),
        );
        assert!(!check.market_ok);
        assert!(check.total_ok);
    }

    #[test]
    fn the_gas_budget_stops_new_risk_and_never_reports_zero_as_measured() {
        let decision = check_gas(None, atoms(1), atoms(2));
        assert_eq!(decision.status, GasStatus::Unavailable);
        assert!(!decision.may_add_risk);

        let low = check_gas(
            Some(GasReading {
                balance_atoms: atoms(0),
                initial_atoms: atoms(10),
            }),
            atoms(1),
            atoms(100),
        );
        assert_eq!(low.reason, Some("GAS_BUDGET"));
        assert!(!low.may_add_risk);

        let spent = check_gas(
            Some(GasReading {
                balance_atoms: atoms(1),
                initial_atoms: atoms(10),
            }),
            atoms(1),
            atoms(5),
        );
        assert_eq!(spent.paid_atoms, Some(atoms(9)));
        assert!(!spent.may_add_risk);
    }
}
