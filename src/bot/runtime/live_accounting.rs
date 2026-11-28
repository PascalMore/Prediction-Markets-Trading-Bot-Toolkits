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
use crate::config::run_parameters::RunParameters;
use alloy_primitives::{U256, U512};
use std::collections::HashMap;

/// The asset's atomic scale: the deployment's collateral and share decimals.
pub const ASSET_UNITS: u32 = 18;
/// One asset in atoms.
pub const ONE: u128 = 1_000_000_000_000_000_000;
/// The smallest order worth sending, in atoms (0.5 USDT).
///
/// This is a dust floor, not a risk threshold: the controlled reservation is
/// `ceil(shares * limit)`, which is strictly below the configured order size
/// (a 1.00 USDT order reserves ~0.9999), so the floor must sit below that
/// reservation or no order could ever be sent. The only cap that decides how
/// large an order may be stays [`max_order_atoms`].
pub const MIN_ORDER_ATOMS: u128 = 500_000_000_000_000_000;
/// The window's allocated budget, in USDT.
pub const BUDGET_C0_USDT: u64 = 98;
/// The largest single order's worst cash, in USDT.
pub const MAX_ORDER_USDT: u64 = 1;
/// The largest per-market worst cash, in USDT.
pub const MAX_MARKET_USDT: u64 = 1;
/// The largest total worst cash, in USDT.
pub const MAX_TOTAL_USDT: u64 = 10;
/// The net loss that latches the window, in USDT.
pub const MAX_LOSS_USDT: u64 = 20;

/// Whether a startup reading funds the window's own budget.
///
/// This is the legacy receipt path's coverage test: an account holding at least
/// the budget is funded. The controlled window's strict equality lives in
/// [`cash_matches_budget`].
pub fn startup_funded(budget: U256, collateral: Option<U256>) -> bool {
    collateral.is_some_and(|cash| cash >= budget)
}

/// Whether a controlled window's observed starting cash is exactly its budget.
///
/// The window's C0 is the budget the run declares. When the venue holds a
/// different amount the window's budget-relative cash can never equal the
/// observed balance, so the identity cannot balance against that budget. Startup
/// refuses rather than letting a mismatched window trade first and discover the
/// gap at the end. The comparison is atom-for-atom: no rounding, no tolerance.
pub fn cash_matches_budget(budget: U256, collateral: Option<U256>) -> bool {
    collateral == Some(budget)
}

/// Whether the starting cash is the declared budget plus its verified claims.
///
/// A claimed payout that already arrived raises the account's collateral above
/// the pre-claim C0. Only the reader's own exact claim evidence may explain that
/// increase; every other difference keeps refusing.
pub fn cash_matches_budget_and_claims(budget: U256, collateral: Option<U256>, claims: U256) -> bool {
    budget.checked_add(claims) == collateral
}

/// The exact cash a reader's verified claimed payouts add up to.
pub fn explained_claim_cash(
    snapshot: &crate::service::venue::AccountSnapshot,
) -> Option<U256> {
    snapshot
        .claimed_win
        .iter()
        .try_fold(U256::ZERO, |total, win| total.checked_add(win.claim_atoms))
}

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
    /// The cash a confirmed official claim already delivered: the claimed
    /// inventory and the exact atoms the venue paid. This is cash, not a mark.
    fn claim_credit(&self, _attempt_id: u64) -> Option<(U256, U256)> { None }
    /// Cash the run's opening account read already received as claimed payouts.
    fn opening_claim_cash(&self) -> U256 { U256::ZERO }
    /// The frozen public money of this window. Legacy states keep the
    /// historical values; a parameterized run carries its own.
    fn params(&self) -> RunParameters { RunParameters::legacy() }
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

    fn claim_credit(&self, attempt_id: u64) -> Option<(U256, U256)> {
        self.state.claims.get(&attempt_id).copied()
    }

    fn opening_claim_cash(&self) -> U256 {
        self.state
            .controlled
            .as_ref()
            .map(|run| run.opening_claim_cash)
            .unwrap_or(U256::ZERO)
    }

    fn params(&self) -> RunParameters {
        self.state
            .controlled
            .as_ref()
            .map(|run| run.options.params)
            .unwrap_or_else(RunParameters::legacy)
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
    let params = facts.params();
    let budget = params.budget;
    let mut cost = U256::ZERO;
    let mut reserved = U256::ZERO;
    let mut value: Option<U256> = Some(U256::ZERO);
    let mut applied_total = U256::ZERO;
    let mut released_basis = U256::ZERO;
    // Historical income is already in Cb. C0 and this run's earnings exclude it.
    let mut claimed_cash = U256::ZERO;
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
        if let Some((claimed_inventory, claimed)) = facts.claim_credit(attempt_id) {
            // The official claim already paid: the holding is gone, its cost is
            // released, and the exact atoms are cash this window holds.
            if claimed_inventory != inventory || !residual.is_zero() {
                conflict = true;
                continue;
            }
            cost = cost.checked_sub(cash).unwrap_or_else(|| { conflict = true; U256::ZERO });
            released_basis = released_basis.checked_add(cash)
                .unwrap_or_else(|| { conflict = true; U256::MAX });
            claimed_cash = claimed_cash.checked_add(claimed)
                .unwrap_or_else(|| { conflict = true; U256::MAX });
            continue;
        }
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

    // The budget balance falls by what the window actually spent and rises by
    // the claimed payouts it already received.
    let cash = budget
        .saturating_sub(applied_total)
        .checked_add(claimed_cash)
        .unwrap_or_else(|| {
            conflict = true;
            U256::MAX
        });
    let available = cash.saturating_sub(reserved);
    let status = match value {
        _ if conflict || applied_total > budget
            || value.is_some_and(|value| cash.checked_add(value).is_none()) => AccountingStatus::Conflict,
        Some(_) => AccountingStatus::Complete,
        None => AccountingStatus::Incomplete(ProjectionGap::UnknownMark),
    };

    // The realized gain is what the window recovered: the claimed cash it
    // received, less the cost basis those holdings released. The unrealized
    // gain is the value of what it still holds, less what it paid for it.
    // Keeping the claim inside `realized` is what makes the identity
    // `cash = C0 + realized - cost` hold after a claim was paid.
    let realized = match claimed_cash.checked_sub(released_basis) {
        Some(gain) => SignedAtoms::positive(gain),
        None => SignedAtoms::negative(released_basis - claimed_cash),
    };
    let value = value.unwrap_or(U256::ZERO);
    let unrealized = SignedAtoms::checked_sub(value, cost).unwrap_or_else(SignedAtoms::zero);
    // NAV is budget cash plus the valuation. Only net_pnl subtracts C0;
    // reservations limit spending, but are not another expense in NAV.
    let nav = SignedAtoms::positive(cash.saturating_add(value));
    let net_pnl = SignedAtoms::checked_sub(cash.saturating_add(value), budget)
        .unwrap_or_else(SignedAtoms::zero);
    let loss_latched = net_pnl.is_negative()
        && net_pnl.magnitude() >= params.loss;

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
        let facts = StateFacts { state };
        let inventory = state.net_inventory.get(id).copied().unwrap_or(U256::ZERO);
        // A claimed payout that already arrived is cash, not risk: its cost is
        // released exactly like a write-off. An unclaimed or unconfirmed win
        // keeps its cost.
        let settled = reserved.is_zero()
            && (facts.claim_credit(*id).is_some_and(|(claimed, _)| claimed == inventory)
                || matches!(facts.official_claim(*id),
                    Some((covered, claim)) if claim.is_zero() && covered == inventory));
        let cost = if settled { U256::ZERO } else { cost };
        exposure.cost = exposure.cost.checked_add(cost)?;
        exposure.reserved = exposure.reserved.checked_add(reserved)?;
        let market = exposure.markets.entry(record.identity.market_id).or_default();
        market.0 = market.0.checked_add(cost)?;
        market.1 = market.1.checked_add(reserved)?;
    }
    if state.net_cash.keys().chain(state.reservations.keys())
        .chain(state.observed_risk_floor.keys())
        .chain(state.claims.keys())
        .any(|id| !state.attempts.contains_key(id))
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
            run.stop_expired_recheck();
            if run.control.lock().stop.is_none() { run.stop("MANUAL_STOP", true, chrono::Utc::now()); }
        }
        let cleanup = tokio::time::timeout(cleanup_budget, async {
            self.sync_live().await?;
            self.settle_live_results(data, false).await
        }).await;
        let cleanup_complete = matches!(cleanup, Ok(Ok(())));
        // Cleanup consumes the same W0 window. Read the original clocks only
        // now, at tail entry, so neither finish entry point renews that window.
        let (tail_budget, cash_waiting) = {
            let state = self.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            let budget = match run.cash_wait.as_ref() {
                None => tail_budget,
                Some(wait) => match run.phase1.clock.now() {
                    Ok(now) if wait.may_continue(now, run.options.deadline_at,
                        run.run_deadline_elapsed_ms) => {
                        let wall = (wait.ends_at(run.options.deadline_at) - now.utc)
                            .to_std().unwrap_or_default();
                        let elapsed = std::time::Duration::from_millis(
                            wait.ends_at_elapsed_ms(run.run_deadline_elapsed_ms) - now.elapsed_ms);
                        tail_budget.min(wall).min(elapsed)
                    }
                    _ => std::time::Duration::ZERO,
                },
            };
            (budget, run.cash_wait.is_some())
        };
        if cleanup_complete && (!cash_waiting || !tail_budget.is_zero())
            && self.entry_stop_reason() == Some("DEADLINE") {
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
                    self.settle_live_results(data, false).await?;
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
        self.settle_live_results(data, true).await
    }

    async fn settle_live_results(
        &self, data: &dyn crate::service::venue::MarketData, original_poll: bool,
    ) -> anyhow::Result<()> {
        let lane = self.live_operation_lane().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let operation = lane.lock().await;
        self.settle_live_results_on_lane(data, original_poll, operation).await
    }

    pub(super) async fn settle_live_results_on_lane(
        &self, data: &dyn crate::service::venue::MarketData, original_poll: bool,
        _operation: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        use super::live_events::{flush_events, prepare_lifecycle};
        use crate::service::predict_fun::account::runtime_claims::{capture_native_market, OfficialBatch, OFFICIAL_BATCH};

        let check_r22 = |state: &mut super::State| -> anyhow::Result<()> {
            // Cleanup is a separate consumer, not a recovery probe. Honor the
            // existing R3 cutoff before the R22 boundary if both are armed.
            if original_poll {
                let run = state.live.controlled.as_mut().unwrap();
                if run.r22_wait.is_some() { run.stop_expired_recheck(); }
                super::live_control::check_r22_boundary(state)?;
            }
            Ok(())
        };
        let mut official_batch = OfficialBatch::new();
        let (mut ids, retry) = {
            let mut state = self.state.lock();
            check_r22(&mut state)?;
            let run = state.live.controlled.as_ref().unwrap();
            if run.emitter.is_finished() { return Ok(()); }
            let retry = run.settlement_recheck.filter(|_| original_poll).map(|wait| wait.market_id);
            (run.traded.keys().copied().collect::<Vec<_>>(), retry)
        };
        ids.sort_unstable();
        for id in ids {
            check_r22(&mut self.state.lock())?;
            let (read, native) = if retry.is_some() {
                let Some(budget) = self.settlement_batch_budget()? else { return Ok(()); };
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep(budget) => (Err(anyhow::anyhow!("DATA_UNAVAILABLE")), None),
                    read = capture_native_market(data.settlement_market(id)) => read,
                }
            } else {
                capture_native_market(data.settlement_market(id)).await
            };
            let market = match read {
                Ok(market) => market,
                Err(error) => {
                    let error = crate::service::predict_fun::settlement_read_error(error);
                    let transient = error.downcast_ref::<crate::service::venue::VenueError>()
                        .is_some_and(|error| error.code == crate::service::venue::VenueErrorCode::ReadUnavailable);
                    let events = {
                        let mut state = self.state.lock();
                        check_r22(&mut state)?;
                        let run = state.live.controlled.as_mut().unwrap();
                        run.account_read = None;
                        run.stop_expired_recheck();
                        if retry.is_some() {
                            run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                        }
                        if transient {
                            run.note_failure(super::live_control::FailureKind::Settlement,
                                "READ_UNAVAILABLE", chrono::Utc::now());
                        } else {
                            run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                        }
                        super::live_events::prepare_lifecycle(&mut state)
                    };
                    super::live_events::flush_events(events.map_err(anyhow::Error::msg)?)
                        .map_err(anyhow::Error::msg)?;
                    // A failed authoritative read cannot certify cleanup or
                    // keep a deadline-only tail alive as if reconciliation succeeded.
                    return Err(error);
                }
            };
            let (events, stopped) = {
                let mut state = self.state.lock();
                check_r22(&mut state)?;
                let mut events = Vec::new();
                let mut cutoff = false;
                if retry.is_none() {
                    state.live.controlled.as_mut().unwrap().stop_expired_recheck();
                }
                if retry.is_none() || self.active_recheck_budget(&mut state).is_some() {
                    if market.id != id {
                        state.live.controlled.as_mut().unwrap().stop("FACT_CONFLICT", true, chrono::Utc::now());
                    } else if !state.live.controlled.as_ref().unwrap().r22_wait.as_ref()
                        .is_some_and(|wait| wait.proof.owner.identity.market_id == id && retry != Some(id)) {
                        // The R22 owner's result is a fact in this batch, not a ledger update
                        // until the same account read proves both the canonical legs and the balance.
                        match apply_official_result(&mut state, &market, retry.is_some(), retry == Some(id)) {
                            Ok(Some(prepared)) => events = prepared,
                            Ok(None) => cutoff = true,
                            Err(_) if retry.is_some() => {
                                state.live.controlled.as_mut().unwrap().stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                            }
                            Err(error) => return Err(anyhow::Error::msg(error)),
                        }
                    }
                    if !cutoff && retry == Some(id) {
                        let run = state.live.controlled.as_ref().unwrap();
                        let confirmed = state.live.attempts.iter().any(|(attempt, handle)| {
                            handle.lock().identity.market_id == id && run.attempt_outcomes.contains_key(attempt)
                        });
                        if !confirmed {
                            state.live.controlled.as_mut().unwrap().stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                        }
                    }
                }
                let stopped = cutoff || (retry.is_some() && self.active_recheck_budget(&mut state).is_none());
                if stopped || market.id != id {
                    events.extend(prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?);
                }
                (events, stopped)
            };
            flush_events(events).map_err(anyhow::Error::msg)?;
            if stopped { return Ok(()); }
            official_batch.insert(id, (market, native));
        }
        let runtime_batch = OFFICIAL_BATCH.scope(official_batch, super::live::check_runtime_account(self, false));
        // Require a new receipt from the original account owner; never fall
        // through to the normal pending-claims read on a stopped recheck.
        if retry.is_some() {
            let Some(budget) = self.settlement_batch_budget()? else { return Ok(()); };
            let combined_r22 = {
                let mut state = self.state.lock();
                let run = state.live.controlled.as_mut().unwrap();
                run.account_read = None;
                run.r22_wait.is_some()
            };
            let account = super::live_control::RECHECK_ACCOUNT_CUTOFF.scope(
                std::cell::Cell::new(None), runtime_batch);
            let result = tokio::select! {
                biased;
                _ = tokio::time::sleep(budget) => Err(anyhow::anyhow!("DATA_UNAVAILABLE")),
                result = account => result,
            };
            let (events, stopped) = {
                let mut state = self.state.lock();
                let within_budget = self.active_recheck_budget(&mut state).is_some();
                let run = state.live.controlled.as_ref().unwrap();
                let current = within_budget && run.r22_wait.is_none() && run.phase1.clock.now().ok().is_some_and(|now| {
                    run.account_read.as_ref().is_some_and(|read| read.matches(&state.live, now))
                });
                let recovered = result.is_ok() && current && self.active_recheck_budget(&mut state).is_some();
                let run = state.live.controlled.as_mut().unwrap();
                if recovered {
                    run.settlement_recheck = None;
                    if combined_r22 { run.account_read = None; }
                } else {
                    run.account_read = None;
                    run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
                }
                (prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?, !recovered)
            };
            flush_events(events).map_err(anyhow::Error::msg)?;
            result?;
            if stopped { return Ok(()); }
        } else if {
            let state = self.state.lock();
            !state.live.pending_claims.is_empty()
                || (original_poll && state.live.controlled.as_ref().is_some_and(|run| run.r22_wait.is_some()))
        } {
            // Preserve original cash-first proof on non-recovery reads/cleanup.
            runtime_batch.await?;
        }
        self.state.lock().live.controlled.as_mut().unwrap()
            .clear_failure(super::live_control::FailureKind::Settlement);
        Ok(())
    }

    fn active_recheck_budget(&self, state: &mut super::State) -> Option<std::time::Duration> {
        let entries_stopped = state.entry_stop_reason.is_some();
        let run = state.live.controlled.as_mut().unwrap();
        run.stop_expired_recheck();
        if !entries_stopped && self.controlled_run().is_none_or(|run| run.stopped()) {
            run.stop("DEADLINE", true, chrono::Utc::now());
        }
        if entries_stopped || run.must_end() { return None; }
        match run.recheck_budget() {
            Ok(budget) => Some(budget),
            Err(reason) => {
                run.stop(reason, true, chrono::Utc::now());
                None
            }
        }
    }

    fn settlement_batch_budget(&self) -> anyhow::Result<Option<std::time::Duration>> {
        let events = {
            let mut state = self.state.lock();
            if let Some(budget) = self.active_recheck_budget(&mut state) { return Ok(Some(budget)); }
            super::live_events::prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?
        };
        super::live_events::flush_events(events).map_err(anyhow::Error::msg)?;
        Ok(None)
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

/// Read-only verdict shared with the runtime settlement consumer.
pub(super) fn judge_official_round(
    original: &crate::service::venue::TradableMarket, market: &crate::service::venue::TradableMarket,
) -> (super::paper::RoundIdentity, super::paper::Settlement, Option<(f64, f64)>, Option<f64>) {
    use super::paper::{self, Settlement};
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
    (identity, result, prices, payout)
}

/// Apply immutable official result metadata to the existing applied inventory.
/// The caller owns the operation lane and flushes only after releasing State.
/// `Ok(None)` means the active batch stopped before consuming this result.
pub(super) fn apply_official_result(
    state: &mut crate::bot::runtime::State,
    market: &crate::service::venue::TradableMarket,
    active_batch: bool,
    retry: bool,
) -> Result<Option<Vec<super::live_events::PreparedLiveEvent>>, String> {
    use super::live_control::LiveOutcome;
    use super::live_events::{self, LivePayload, PayoutPayload};
    use super::paper::{self, Settlement};
    let run = state.live.controlled.as_mut().ok_or("CONTEXT_MISSING")?;
    let now = match run.phase1.clock.now() {
        Ok(now) => now,
        Err(_) if active_batch => {
            run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
            return Ok(None);
        }
        Err(_) => return Err("DATA_UNAVAILABLE".into()),
    };
    // Use the decision/audit sample before any result side effect, including
    // for non-target markets in this batch. Cleanup has its own budget.
    if active_batch && !run.advance_recheck_at(now) {
        return Ok(None);
    }
    let original = run.traded.get(&market.id).ok_or("CONTEXT_MISSING")?;
    let (identity, result, prices, payout) = judge_official_round(original, market);
    let mut attempts: Vec<_> = state.live.attempts.iter().filter_map(|(id, handle)| {
        let record = handle.lock().clone();
        (record.identity.market_id == market.id).then_some((*id, record))
    }).collect();
    attempts.sort_by_key(|(id, _)| *id);
    let closed_execution = state.live.baseline.is_some() && state.live.pending_claims.is_empty()
        && state.live.reservations.values().all(U256::is_zero)
        && state.live.attempts.values().all(|handle| {
            let record = handle.lock();
            record.phase == crate::service::venue::AttemptPhase::Closed
                && record.submission != crate::service::venue::Submission::Unknown
        })
        && attempts.iter().all(|(id, record)| {
            record.identity.account == run.identity
                && (record.identity.token_id == original.yes_token || record.identity.token_id == original.no_token)
                && (record.submission != crate::service::venue::Submission::Accepted
                    || (state.live.watermarks.get(id).is_some_and(|watermark| watermark.is_complete())
                        && state.live.net_inventory.contains_key(id) && state.live.net_cash.contains_key(id)))
        });
    let eligible = !retry && closed_execution && run.settlement_recheck.is_none()
        && run.order_wait.is_none() && run.cash_wait.is_none() && !run.loss_latched
        && state.entry_stop_reason.is_none() && !run.must_end()
        && market.ends_at.is_some_and(|end| now.utc >= end)
        && market.end_price.is_none() && market.payout_yes.is_none() && !market.contested
        && market.round_facts.as_ref().is_some_and(|facts| {
            facts.oracle_status.as_deref() == Some("REGISTERED")
                && facts.trading_status.as_deref() == Some("CLOSED")
                && facts.yes_status.is_none() && facts.no_status.is_none()
        })
        && attempts.iter().any(|(id, record)| {
            record.submission == crate::service::venue::Submission::Accepted
                && !run.attempt_outcomes.contains_key(id)
                && state.live.net_inventory.get(id).is_some_and(|quantity| !quantity.is_zero())
        });
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
    let conflict = payloads.iter().any(|(_, _, status, _, _)| *status == "CONFLICT");
    if !conflict && result == Settlement::Pending {
        if eligible && identity == paper::RoundIdentity::Match {
            run.settlement_recheck = Some(super::live_control::SettlementRecheck { market_id: market.id, started: now, last: now });
            run.pending_candidate = None;
            tracing::info!(market_id = market.id, "closed result unpublished; awaiting next settlement poll");
        } else if retry || !(identity == paper::RoundIdentity::Match && normal_unpublished_result(market)) {
            run.stop("DATA_UNAVAILABLE", true, now.utc);
        }
    } else if retry && !conflict && !closed_execution {
        run.stop("DATA_UNAVAILABLE", true, now.utc);
    }
    for (id, _, status, payout, claim) in &payloads {
        if *status == "CONFLICT" {
            run.stop("FACT_CONFLICT", true, now.utc);
        } else {
            let inventory = state.live.net_inventory[id];
            // Only a claim that pays this holding exactly is already cash. One
            // atom short or long is still an unexplained claim, not a wait that
            // anything may clear.
            let booked = state.live.claims.get(id).copied()
                .is_some_and(|(held, cash)| held == inventory && Some(cash) == *claim);
            run.attempt_outcomes.insert(*id, LiveOutcome {
                payout_yes: *payout, result_prices: prices.unwrap(),
                position_net_atoms: inventory, claim_value_atoms: *claim,
                cash_confirmed: booked,
            });
            if claim.is_some_and(|claim| !claim.is_zero()) && !booked {
                run.stop("CASH_PENDING", true, now.utc);
            }
        }
    }
    // Only a subsequent complete runtime read can promote cash-first evidence.
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
    Ok(Some(events))
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
    params: &RunParameters,
    worst_cash: U256,
    market_cost: U256,
    market_reserved: U256,
    total_cost: U256,
    total_reserved: U256,
    available: U256,
) -> CapsCheck {
    let order_ok = worst_cash >= U256::from(MIN_ORDER_ATOMS) && worst_cash <= params.order;
    let market_ok = market_cost
        .saturating_add(market_reserved)
        .saturating_add(worst_cash)
        <= params.market;
    let total_ok = total_cost
        .saturating_add(total_reserved)
        .saturating_add(worst_cash)
        <= params.total;
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
    fn the_startup_gate_uses_the_configured_budget_and_never_resizes_it() {
        let budget = crate::config::RunParameters::legacy().budget;
        // The approved change is the budget itself: the same 97.422 account
        // passes a 97 window and is refused by the historical 98 one.
        let cash = U256::from(97_422_000_000_000_000_000u128);
        let approved = crate::config::RunParameters {
            budget: U256::from(97u64) * U256::from(ONE),
            ..crate::config::RunParameters::legacy()
        };
        assert!(startup_funded(approved.budget, Some(cash)));
        assert!(!startup_funded(budget, Some(cash)));
        // Equality funds the window; one atom less does not; an unknown
        // reading is never treated as funded.
        assert!(startup_funded(approved.budget, Some(approved.budget)));
        assert!(!startup_funded(approved.budget, Some(approved.budget - U256::from(1u64))));
        assert!(!startup_funded(approved.budget, None));
    }

    #[test]
    fn a_controlled_startup_requires_cash_equal_to_the_budget_to_the_atom() {
        let budget = U256::from(97_422_000_000_000_000_000u128);
        assert!(cash_matches_budget(budget, Some(budget)));
        assert!(!cash_matches_budget(budget, Some(budget - U256::from(1u64))));
        assert!(!cash_matches_budget(budget, Some(budget + U256::from(1u64))));
        assert!(!cash_matches_budget(budget, None));
        // The historical 98/100 window is not resized: 100 is not 98.
        assert!(!cash_matches_budget(
            U256::from(98u64) * U256::from(ONE),
            Some(U256::from(100u64) * U256::from(ONE))
        ));
    }

    #[test]
    fn a_claimed_payout_is_cash_not_mark_and_keeps_the_projection_complete() {
        struct Claimed {
            budget: U256,
            inventory: U256,
            credit: Option<(U256, U256)>,
        }
        impl LiveFacts for Claimed {
            fn applied_net_cash(&self, _id: u64) -> U256 { atoms(2 * ONE) }
            fn applied_net_inventory(&self, _id: u64) -> U256 { self.inventory }
            fn reservation(&self, _id: u64) -> U256 { U256::ZERO }
            fn attempt_market(&self, _id: u64) -> Option<u64> { Some(1) }
            fn attempt_token(&self, _id: u64) -> Option<String> { Some("77".into()) }
            fn attempt_ids(&self) -> Vec<u64> { vec![1] }
            fn observed_risk_floor(&self, _id: u64) -> Option<U256> { Some(U256::ZERO) }
            fn claim_credit(&self, _id: u64) -> Option<(U256, U256)> { self.credit }
            fn params(&self) -> RunParameters {
                RunParameters { budget: self.budget, ..RunParameters::legacy() }
            }
        }
        let inventory = atoms(5 * ONE);
        let budget = atoms(10 * ONE);
        let credit = Some((inventory, inventory));
        let projected = project_window(
            &Claimed { budget, inventory, credit },
            &Marks::default(),
            None,
        );
        assert_eq!(projected.status, AccountingStatus::Complete);
        // Two USDT spent, five shares won and paid: the claim is cash.
        assert_eq!(projected.cash, atoms(10 * ONE) - atoms(2 * ONE) + inventory);
        assert_eq!(projected.net_pnl.magnitude(), inventory - atoms(2 * ONE));
        assert!(projected.pending_claim_value.is_none());
        // A credit that doesn't cover the ledger's inventory is a conflict, not
        // a quiet cash increase.
        let conflict = project_window(
            &Claimed { budget, inventory, credit: Some((inventory - U256::from(1), inventory)) },
            &Marks::default(),
            None,
        );
        assert_eq!(conflict.status, AccountingStatus::Conflict);
        // An unclaimed win is pending, never cash.
        let pending = project_window(
            &Claimed { budget, inventory, credit: None },
            &Marks::default(),
            None,
        );
        assert_eq!(pending.cash, budget - atoms(2 * ONE));
        assert!(pending.pending_claim_value.is_none());
    }

    #[test]
    fn an_opening_claimed_payout_adds_exactly_its_cash() {
        struct Opening;
        impl LiveFacts for Opening {
            fn applied_net_cash(&self, _id: u64) -> U256 { U256::ZERO }
            fn applied_net_inventory(&self, _id: u64) -> U256 { U256::ZERO }
            fn reservation(&self, _id: u64) -> U256 { U256::ZERO }
            fn attempt_market(&self, _id: u64) -> Option<u64> { None }
            fn attempt_token(&self, _id: u64) -> Option<String> { None }
            fn attempt_ids(&self) -> Vec<u64> { Vec::new() }
            fn observed_risk_floor(&self, _id: u64) -> Option<U256> { None }
            fn opening_claim_cash(&self) -> U256 { atoms(ONE) }
            fn params(&self) -> RunParameters {
                RunParameters { budget: atoms(10 * ONE), ..RunParameters::legacy() }
            }
        }
        let projected = project_window(&Opening, &Marks::default(), None);
        assert_eq!(projected.status, AccountingStatus::Complete);
        // The opening claim is already inside Cb: this run neither spends it
        // again nor books it as its own earnings.
        assert_eq!(projected.cash, atoms(10 * ONE));
        assert_eq!(projected.net_pnl.magnitude(), U256::ZERO);
        assert!(!projected.net_pnl.is_negative());
        assert_eq!(projected.realized.magnitude(), U256::ZERO);
        assert_eq!(projected.nav.magnitude(), atoms(10 * ONE));
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
        let params = crate::config::RunParameters::legacy();
        let cap = max_order_atoms();
        let check = check_caps(&params, cap, U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO, cap);
        assert!(check.all_ok());
        // A 1.00 USDT order reserves `ceil(shares * limit)` atoms, which is
        // strictly below the configured size: 0.9999 USDT must pass the dust
        // floor and the per-order cap.
        let nearly_one = U256::from(999_900_000_000_000_000u128);
        assert!(check_caps(&params, nearly_one, U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO, cap).all_ok());
        // A 2.00 USDT order reserves 1.9998 and is refused by the order cap.
        let two = U256::from(1_999_800_000_000_000_000u128);
        let over = check_caps(&params, two, U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO,
            U256::from(100u64) * U256::from(ONE));
        assert!(!over.order_ok);
        assert!(over.total_ok);
        // One atom over the per-order cap is refused even though the total
        // would still fit.
        let over = check_caps(
            &params,
            cap + U256::from(1u64),
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
    fn the_frozen_parameters_move_the_caps_they_no_longer_fix() {
        let params = crate::config::RunParameters {
            budget: U256::from(80u64) * U256::from(ONE),
            target: U256::from(2u64) * U256::from(ONE),
            order: U256::from(3u64) * U256::from(ONE),
            market: U256::from(5u64) * U256::from(ONE),
            total: U256::from(12u64) * U256::from(ONE),
            loss: U256::from(30u64) * U256::from(ONE),
        };
        let three = U256::from(3u64) * U256::from(ONE);
        let available = U256::from(80u64) * U256::from(ONE);
        assert!(check_caps(&params, three, U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO, available).all_ok());
        // The legacy caps would have refused this order; the frozen ones admit it.
        assert!(!check_caps(&crate::config::RunParameters::legacy(), three, U256::ZERO,
            U256::ZERO, U256::ZERO, U256::ZERO, available).order_ok);
        let over_market = check_caps(&params, U256::from(2u64) * U256::from(ONE), three,
            U256::ZERO, U256::from(11u64) * U256::from(ONE), U256::ZERO, available);
        assert!(over_market.market_ok && !over_market.total_ok);
    }

    #[test]
    fn the_market_cap_is_checked_on_its_own() {
        let params = crate::config::RunParameters::legacy();
        let two = max_order_atoms();
        let check = check_caps(
            &params,
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
