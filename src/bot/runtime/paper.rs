//! The controlled paper cash model: one cash balance, one reservation, one loss latch.
//!
//! The broker owns the only state that counts. This module adds no second
//! ledger: it reads and writes the same holdings, marks, realized and fees the
//! ordinary path uses, and keeps the controlled extras in the same lock.
//!
//! The money model is deliberately explicit:
//!
//! - `B` is the cash balance, `K` the cost of open net inventory, `R` the
//!   realized result and `P` the worst-case cash still reserved for a BUY
//!   that hasn't filled. `B = C0 + R - K` and `A = B - P` is what's free.
//! - A BUY reserves the worst case `w = shares × limit` before it walks the
//!   book. The walk may fill cheaper: the difference goes back to `A`.
//! - The fee is taken from the shares received, not from the cash a second
//!   time. `f = r × min(p, 1 - p) × q`, `f_q = f / p`, `q_net = q - f_q`.

use crate::config::ControlledRun;
use crate::models::{BookSnapshot, Side};
use crate::service::observation::{CheckTime, Clock};
use crate::service::venue::{RoundFacts, TradableMarket};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info};

use super::{
    token_book, walk, Blocked, Broker, ControlledStop, Holding, State, TakeResult, MIN_ORDER_USD,
};

use crate::bot::arbitrage::phase1::{
    self, CandidateContext, DecisionDiagnostic, Phase, Reason, StrategyContract,
};
use crate::bot::runtime::phase1_control::Phase1Run;

const EPS: f64 = 1e-9;

/// The fee model this module implements. A controlled run states it in the
/// public configuration, and no other value is accepted.
pub const FEE_MODEL_NET_SHARES: &str = "paper_net_shares";

/// How long the controlled run's cleanup may take.
///
/// It is the same budget the launcher gives the child: the run doesn't wait
/// longer than the parent does.
const CONTROLLED_CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

// The test clock is compiled only for tests; production uses the run's clock.
#[cfg(test)]
thread_local! {
    static CLOCK_EXPIRED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// How many more checks the clock may answer before it expires.
    static CLOCK_EXPIRE_AFTER: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
    /// The checks since the clock was armed.
    static CLOCK_CHECKS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Make the controlled run look past its deadline, or stop looking so.
#[cfg(test)]
pub(super) fn set_controlled_clock_expired(expired: bool) {
    CLOCK_EXPIRED.with(|flag| flag.set(expired));
}

/// Let the deadline arrive on the `checks`-th check after this call.
///
/// A run that is already stopped is refused before it reserves anything, so a
/// test that wants to watch a reservation being released has to let the
/// deadline arrive while the order is in flight.
#[cfg(test)]
pub(super) fn set_controlled_clock_expire_after(checks: u32) {
    CLOCK_EXPIRE_AFTER.with(|cell| cell.set(Some(checks)));
    CLOCK_CHECKS.with(|cell| cell.set(0));
}

/// Whether the test clock says the run is past its deadline.
#[cfg(test)]
fn controlled_clock_expired() -> bool {
    CLOCK_CHECKS.with(|checks| checks.set(checks.get() + 1));
    let due = CLOCK_EXPIRE_AFTER.with(|cell| cell.get());
    if let Some(due) = due {
        let reached = CLOCK_CHECKS.with(|checks| checks.get() >= due);
        if reached {
            CLOCK_EXPIRE_AFTER.with(|cell| cell.set(None));
            CLOCK_EXPIRED.with(|flag| flag.set(true));
        }
    }
    CLOCK_EXPIRED.with(|flag| flag.get())
}

/// One BUY's worst-case reservation.
///
/// A controlled run trades one market at a time, so a single slot is enough
/// to express validate → reserve → fill or release without a second ledger.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Reservation {
    pub market_id: u64,
    pub token: String,
    /// The worst-case cash this order can cost.
    pub worst_case: f64,
}

/// A market's mark and whether it is usable right now.
///
/// A mark is only usable when this tick saw both sides of the book. The last
/// good value is kept for disclosure, never to let a new entry through.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarkState {
    pub price: Option<f64>,
    pub observed_at: Option<DateTime<Utc>>,
    /// Whether this tick produced a usable two-sided mid.
    pub valid: bool,
}

/// What the venue officially said about one market's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    Pending,
    Confirmed,
    Conflict,
}

/// The controlled paper state. It lives inside the broker's own lock.
///
/// The realized result and the inventory cost are not here: they are the
/// broker's own `state.realized` and the holdings' cost, so there is only one
/// of each.
#[derive(Clone)]
pub struct PaperCash {
    /// The virtual cash the run started with.
    pub initial_cash: f64,
    /// The cash balance right now.
    pub cash: f64,
    /// The current BUY's worst-case reservation, if any.
    pub reservation: Option<Reservation>,
    /// Whether the loss limit has latched this run.
    pub loss_latched: bool,
    /// Marks by token.
    pub marks: HashMap<String, MarkState>,
    /// The official result of each market this run traded.
    pub settlements: HashMap<u64, Settlement>,
    /// The price source each token's last decision came from.
    pub sources: HashMap<String, (String, DateTime<Utc>)>,
    /// Frozen first-fill identity, not a second inventory or cash balance.
    pub traded: HashMap<u64, TradableMarket>,
    /// Applied official prices/payout; retained after inventory is removed.
    pub applied_results: HashMap<u64, (f64, f64, f64)>,
    /// The phase1 run state, when this run speaks the v2 protocol.
    pub phase1: Option<Phase1Run>,
    /// The candidate that is waiting for its fill, if any.
    pub pending_candidate: Option<PendingCandidate>,
    /// The candidate that filled each token, for the settlement's reference.
    pub filled_candidates: HashMap<String, String>,
    /// Why the last phase1 check refused, for the event that reports it.
    pub last_phase1_reason: Option<Reason>,
    /// Last consumed serial candidate id; terminal candidates cannot be reused.
    pub last_candidate_id: u64,
}

impl std::fmt::Debug for PaperCash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperCash")
            .field("initial_cash", &self.initial_cash)
            .field("cash", &self.cash)
            .field("reservation", &self.reservation)
            .field("loss_latched", &self.loss_latched)
            .field("holdings", &self.traded.len())
            .finish()
    }
}

/// The candidate that has not been applied yet.
#[derive(Clone)]
pub struct PendingCandidate {
    pub context: CandidateContext,
    pub phase: Phase,
    pub checked: CheckTime,
}

impl PaperCash {
    /// Start a run with `initial_cash` and nothing else.
    pub fn new(initial_cash: f64) -> Self {
        Self {
            initial_cash,
            cash: initial_cash,
            reservation: None,
            loss_latched: false,
            marks: HashMap::new(),
            settlements: HashMap::new(),
            sources: HashMap::new(),
            traded: HashMap::new(),
            applied_results: HashMap::new(),
            phase1: None,
            pending_candidate: None,
            filled_candidates: HashMap::new(),
            last_phase1_reason: None,
            last_candidate_id: 0,
        }
    }

    /// The cash still reserved for the BUY in flight.
    pub fn reserved(&self) -> f64 {
        self.reservation.as_ref().map_or(0.0, |r| r.worst_case)
    }

    /// The cash that's free to spend.
    pub fn available(&self) -> f64 {
        self.cash - self.reserved()
    }
}

/// A snapshot of the controlled paper state, for the run's evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct PaperSnapshot {
    /// The cash balance.
    pub cash: f64,
    /// The free cash.
    pub available: f64,
    /// The reserved worst case.
    pub reserved: f64,
    /// The cost of open net inventory.
    pub cost: f64,
    /// The realized result.
    pub realized: f64,
    /// The unrealized result, `V - K`, when every holding has a valid mark.
    /// `None` means it can't be valued, not zero.
    pub unrealized: Option<f64>,
    /// The marked value of the open inventory, `V`.
    pub marked: Option<f64>,
    /// The net asset value, `B + V`.
    pub nav: Option<f64>,
    /// The net result against the starting cash, `R + (V - K)`.
    pub net_pnl: Option<f64>,
    /// Whether the loss limit has latched.
    pub loss_latched: bool,
}

/// The fee a BUY pays, taken from the shares received.
///
/// `f = r × min(p, 1 - p) × q` in cash terms, and `f_q = f / p` in shares.
pub fn net_shares(q: f64, price: f64, fee_rate_bps: u32) -> Option<(f64, f64)> {
    if !q.is_finite() || !price.is_finite() || !(0.0..1.0).contains(&price) || q <= 0.0 {
        return None;
    }
    let rate = f64::from(fee_rate_bps) / 10_000.0;
    let fee = rate * price.min(1.0 - price) * q;
    let fee_shares = fee / price;
    let net = q - fee_shares;
    if !net.is_finite() || net < 0.0 {
        return None;
    }
    Some((net, fee))
}

/// Apply a BUY to the controlled state.
///
/// The reservation must already be in place. The walk's actual cost replaces
/// the worst case, and the difference becomes free again.
pub fn apply_buy(
    paper: &mut PaperCash,
    market_id: u64,
    token: &str,
    gross_shares: f64,
    price: f64,
    fee_rate_bps: u32,
) -> Option<(f64, f64)> {
    let (net_shares, fee) = net_shares(gross_shares, price, fee_rate_bps)?;
    let cost = gross_shares * price;
    if !cost.is_finite() || cost < 0.0 {
        return None;
    }
    let reservation = paper.reservation.take()?;
    if reservation.market_id != market_id || reservation.token != token {
        // The reservation belongs to another order: put it back untouched.
        paper.reservation = Some(reservation);
        return None;
    }
    paper.cash -= cost;
    Some((net_shares, fee))
}

/// Apply a SELL to the controlled state.
///
/// The caller has already clamped the size to the net inventory. The basis is
/// the average cost before the sale, and the fee comes out of the cash.
pub fn apply_sell(
    paper: &mut PaperCash,
    sold_shares: f64,
    price: f64,
    fee_rate_bps: u32,
    avg_cost: f64,
) -> Option<(f64, f64, f64, f64)> {
    if !sold_shares.is_finite() || sold_shares <= 0.0 || !avg_cost.is_finite() {
        return None;
    }
    let (_, fee) = net_shares(sold_shares, price, fee_rate_bps)?;
    let gross = sold_shares * price;
    if !gross.is_finite() {
        return None;
    }
    let basis = avg_cost * sold_shares;
    let net_cash = gross - fee;
    paper.cash += net_cash;
    Some((net_cash, basis, net_cash - basis, fee))
}

/// Apply an official payout to the controlled state.
///
/// The payout is the venue's own number: the inventory is worth exactly that
/// per share. No fee is charged again.
pub fn apply_settlement(paper: &mut PaperCash, net_shares: f64, payout: f64) -> Option<f64> {
    if !net_shares.is_finite() || net_shares < 0.0 || !payout.is_finite() {
        return None;
    }
    if !(0.0..=1.0).contains(&payout) {
        return None;
    }
    let income = net_shares * payout;
    if !income.is_finite() {
        return None;
    }
    paper.cash += income;
    Some(income)
}

/// Whether the venue's official facts settle this round.
///
/// The facts must agree with each other. A missing or contradictory fact is
/// not a payout: it is pending, or a conflict.
pub fn settle_from_facts(
    market: &TradableMarket,
    facts: &RoundFacts,
    start_price: f64,
    end_price: f64,
) -> Settlement {
    let oracle = facts.oracle_status.as_deref().unwrap_or("");
    let trading = facts.trading_status.as_deref().unwrap_or("");
    if oracle != "RESOLVED" || trading != "CLOSED" || market.contested {
        return Settlement::Pending;
    }
    if !start_price.is_finite() || !end_price.is_finite() || start_price <= 0.0 || end_price <= 0.0
    {
        return Settlement::Pending;
    }
    let yes = facts.yes_status.as_deref().unwrap_or("");
    let no = facts.no_status.as_deref().unwrap_or("");
    let up_won = end_price > start_price;
    if end_price == start_price {
        return match (yes, no) {
            ("WON", "LOST") | ("LOST", "WON") => Settlement::Confirmed,
            ("WON", "WON") | ("LOST", "LOST") => Settlement::Conflict,
            _ => Settlement::Pending,
        };
    }
    let (winner, loser) = if up_won { (yes, no) } else { (no, yes) };
    match (winner, loser) {
        ("WON", "LOST") => Settlement::Confirmed,
        ("LOST", "WON") => {
            // The winner is the loser: the facts contradict each other.
            Settlement::Conflict
        }
        _ => Settlement::Pending,
    }
}

/// The payout a confirmed settlement pays, per share.
///
/// An exact tie pays half. Otherwise the outcome that won pays one.
pub fn settlement_payout(market: &TradableMarket, start_price: f64, end_price: f64) -> Option<f64> {
    if !start_price.is_finite() || !end_price.is_finite() || start_price <= 0.0 || end_price <= 0.0
    {
        return None;
    }
    if end_price == start_price {
        return Some(0.5);
    }
    market.payout_yes
}

/// Whether every holding has a usable mark from this tick.
pub fn marks_complete(
    paper: &PaperCash,
    holdings: &HashMap<String, crate::bot::runtime::Holding>,
) -> bool {
    holdings
        .keys()
        .all(|token| paper.marks.get(token).is_some_and(|m| m.valid))
}

/// The marked value of the open net inventory, when it can be valued.
///
/// This is `V`: the inventory at its mark. The unrealized result is `V - K`.
pub fn marked_value(
    paper: &PaperCash,
    holdings: &HashMap<String, crate::bot::runtime::Holding>,
) -> Option<f64> {
    if !marks_complete(paper, holdings) {
        return None;
    }
    let mut value = 0.0;
    for (token, holding) in holdings {
        let mark = paper.marks.get(token)?.price?;
        value += holding.shares * mark;
    }
    Some(value)
}

/// The cost of the open net inventory.
pub fn paper_cost(holdings: &HashMap<String, crate::bot::runtime::Holding>) -> f64 {
    holdings.values().map(|h| h.cost).sum()
}

/// A snapshot of the controlled state.
pub fn snapshot(
    paper: &PaperCash,
    holdings: &HashMap<String, crate::bot::runtime::Holding>,
    realized: f64,
) -> PaperSnapshot {
    let marked = marked_value(paper, holdings);
    let cost = paper_cost(holdings);
    let unrealized = marked.map(|value| value - cost);
    let nav = marked.map(|value| paper.cash + value);
    PaperSnapshot {
        cash: paper.cash,
        available: paper.available(),
        reserved: paper.reserved(),
        cost,
        realized,
        unrealized,
        marked,
        nav,
        net_pnl: nav.map(|value| value - paper.initial_cash),
        loss_latched: paper.loss_latched,
    }
}

/// The net result against the starting cash, when it can be valued.
///
/// This is `R + (V - K)`: the realized result plus the unrealized result. It
/// never subtracts the fee again: the fee is already inside the net shares and
/// the net cash.
pub fn net_pnl(
    paper: &PaperCash,
    holdings: &HashMap<String, crate::bot::runtime::Holding>,
    realized: f64,
) -> Option<f64> {
    marked_value(paper, holdings).map(|value| realized + (value - paper_cost(holdings)))
}

/// Whether the loss limit has been reached.
///
/// The limit is on the net result against the starting cash, and it latches:
/// a recovery never unlocks it.
pub fn loss_limit_reached(
    paper: &PaperCash,
    holdings: &HashMap<String, crate::bot::runtime::Holding>,
    realized: f64,
    max_loss_usd: f64,
) -> bool {
    match net_pnl(paper, holdings, realized) {
        Some(value) => value <= -max_loss_usd,
        // Without a usable valuation there's no proof of health.
        None => false,
    }
}

/// Record a fresh mark for one token.
pub fn mark(paper: &mut PaperCash, token: &str, price: f64, at: DateTime<Utc>, valid: bool) {
    let entry = paper.marks.entry(token.to_string()).or_default();
    entry.valid = valid;
    if valid && price.is_finite() && price > 0.0 {
        entry.price = Some(price);
        entry.observed_at = Some(at);
    }
}

/// Drop every mark's validity, keeping the last known values for disclosure.
///
/// A new tick starts with nothing usable: only the books seen in this tick can
/// make a mark valid again.
pub fn invalidate_marks(paper: &mut PaperCash) {
    for state in paper.marks.values_mut() {
        state.valid = false;
    }
}

/// Whether the book has a usable two-sided mid.
pub fn mid_of(yes_book: &BookSnapshot) -> Option<f64> {
    if yes_book.bids.is_empty()
        || yes_book.asks.is_empty()
        || yes_book.bids.iter().chain(&yes_book.asks).any(|level| {
            !level.price.is_finite()
                || level.price <= 0.0
                || level.price >= 1.0
                || !level.size.is_finite()
                || level.size <= 0.0
        })
        || yes_book
            .bids
            .windows(2)
            .any(|levels| levels[0].price < levels[1].price)
        || yes_book
            .asks
            .windows(2)
            .any(|levels| levels[0].price > levels[1].price)
        || yes_book.bids[0].price > yes_book.asks[0].price
    {
        return None;
    }
    let mid = yes_book.mid()?;
    (mid.is_finite() && mid > 0.0 && mid < 1.0).then_some(mid)
}

fn valid_round(market: &TradableMarket) -> bool {
    market.feed_symbol.as_deref() == Some("BTCUSDT")
        && market
            .starts_at
            .zip(market.ends_at)
            .is_some_and(|(start, end)| end - start == chrono::Duration::seconds(300))
        && market
            .start_price
            .is_some_and(|price| price.is_finite() && price > 0.0)
        && market.yes_token != market.no_token
        && !market.yes_token.is_empty()
        && !market.no_token.is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoundIdentity {
    Match,
    Incomplete,
    Contradiction,
}

/// Compare known identity fields before considering missing metadata.
pub(super) fn same_round(original: &TradableMarket, result: &TradableMarket) -> RoundIdentity {
    let known_tokens = !result.yes_token.is_empty() && !result.no_token.is_empty();
    if original.id != result.id
        || (!result.yes_token.is_empty() && original.yes_token != result.yes_token)
        || (!result.no_token.is_empty() && original.no_token != result.no_token)
        || (known_tokens && result.yes_token == result.no_token)
        || result.starts_at.is_some_and(|value| Some(value) != original.starts_at)
        || result.ends_at.is_some_and(|value| Some(value) != original.ends_at)
        || result.start_price.is_some_and(|value| Some(value) != original.start_price)
        || result.feed_symbol.as_deref().is_some_and(|value| !value.is_empty() && value != "BTCUSDT")
        || result.starts_at.zip(result.ends_at).is_some_and(|(start, end)|
            end - start != chrono::Duration::seconds(300))
        || [result.start_price, result.end_price].into_iter().flatten()
            .any(|value| !value.is_finite() || value <= 0.0)
    { return RoundIdentity::Contradiction; }
    if !known_tokens || result.starts_at.is_none() || result.ends_at.is_none()
        || result.start_price.is_none() || result.feed_symbol.as_deref().unwrap_or("").is_empty()
    { RoundIdentity::Incomplete } else { RoundIdentity::Match }
}

/// Whether a result read lacks facts needed to confirm or compare settlement.
pub(super) fn incomplete_result(market: &TradableMarket) -> bool {
    market.start_price.is_none() || market.end_price.is_none() || market.payout_yes.is_none()
        || market.round_facts.as_ref().is_none_or(|facts| {
            [&facts.oracle_status, &facts.trading_status, &facts.yes_status, &facts.no_status]
                .into_iter().any(|value| value.as_deref().unwrap_or("").is_empty())
        })
}

/// Missing rereads preserve applied facts; known revisions always win.
pub(super) fn revised_result(
    market: &TradableMarket,
    applied: (f64, f64, f64),
    identity: RoundIdentity,
    outcome: Settlement,
) -> bool {
    let (start, end, payout) = applied;
    let expected_yes = if end > start { "WON" } else { "LOST" };
    let expected_no = if end > start { "LOST" } else { "WON" };
    let facts = market.round_facts.as_ref();
    let yes = facts.and_then(|facts| facts.yes_status.as_deref()).filter(|value| !value.is_empty());
    let no = facts.and_then(|facts| facts.no_status.as_deref()).filter(|value| !value.is_empty());
    if identity == RoundIdentity::Contradiction
        || market.start_price.is_some_and(|value| value != start)
        || market.end_price.is_some_and(|value| value != end)
        || market.payout_yes.is_some_and(|value| value != payout)
        || (end != start && (yes.is_some_and(|value| value != expected_yes)
            || no.is_some_and(|value| value != expected_no)))
        || (end == start && yes.zip(no).is_some_and(|(yes, no)| yes == no))
    { return true; }
    let missing = identity == RoundIdentity::Incomplete || incomplete_result(market);
    !missing && outcome != Settlement::Confirmed
}

impl super::State {
    fn apply_controlled_buy(&mut self, market_id: u64, token: &str, net_shares: f64, cost: f64) {
        let holding = self
            .holdings
            .entry(token.to_string())
            .or_insert_with(|| Holding {
                market_id,
                token: token.to_string(),
                shares: 0.0,
                cost: 0.0,
                opened_at: super::observer::paper_now(),
            });
        holding.shares += net_shares;
        holding.cost += cost;
    }

    fn apply_controlled_sell(&mut self, token: &str, sold: f64, basis: f64, pnl: f64) {
        self.realized += pnl;
        let Some(holding) = self.holdings.get_mut(token) else {
            return;
        };
        holding.shares -= sold;
        holding.cost -= basis;
        if holding.shares <= EPS {
            self.holdings.remove(token);
        }
    }
}

impl super::Broker {
    /// Turn this broker into a controlled paper run.
    ///
    /// The controlled branch has no wallet and no venue: a live broker, or one
    /// with a venue attached, is refused rather than half configured.
    pub fn with_controlled_paper(mut self, run: ControlledRun, initial_cash: f64) -> Result<Self> {
        if self.live || self.taker.is_some() || self.limit.is_some() {
            bail!("CONTROLLED_MODE: a controlled paper run has no wallet and no venue");
        }
        if !initial_cash.is_finite() || initial_cash <= 0.0 {
            bail!("CONTROLLED_MODE: the controlled cash must be a positive finite amount");
        }
        self.controlled = Some(run);
        self.state.lock().paper = Some(PaperCash::new(initial_cash));
        Ok(self)
    }

    /// Record the price source a decision came from.
    ///
    /// The controlled run's evidence has to say where each price came from: a
    /// Binance fill and a Coinbase fallback are not the same evidence.
    pub fn record_source(&self, token: &str, source: &str, at: DateTime<Utc>) {
        let mut state = self.state.lock();
        if let Some(paper) = state.paper.as_mut() {
            paper
                .sources
                .insert(token.to_string(), (source.to_string(), at));
        }
    }

    /// The price source a token's last decision came from.
    pub fn source_of(&self, token: &str) -> Option<(String, DateTime<Utc>)> {
        let state = self.state.lock();
        state
            .paper
            .as_ref()
            .and_then(|paper| paper.sources.get(token).cloned())
    }

    /// Stop adding risk, and say why.
    ///
    /// This is the same lock the entries take, so a stop can't race past an
    /// order that is already inside.
    pub fn stop_entries(&self, reason: ControlledStop) {
        let mut state = self.state.lock();
        if state.entry_stop_reason.is_none()
            || matches!(
                reason,
                ControlledStop::ParentLost | ControlledStop::ControlChannel
            )
        {
            state.entry_stop_reason = Some(reason);
        }
        if let Some(run) = state.live.controlled.as_mut() {
            let code = match reason {
                ControlledStop::Deadline => "DEADLINE",
                ControlledStop::ParentLost => "PARENT_LOST",
                ControlledStop::ControlChannel => "CONTROL_CHANNEL",
                ControlledStop::SettlementConflict => "FACT_CONFLICT",
            };
            run.stop(code, true, Utc::now());
        }
        drop(state);
        self.control_changed.notify_one();
    }

    /// Wait for a stop without blocking on market or strategy I/O.
    pub(super) async fn wait_controlled_stop(&self) {
        loop {
            let notified = self.control_changed.notified();
            if self.entry_stop_reason().is_some()
                || self.state.lock().live.controlled.as_ref().is_some_and(|run| !run.may_add_risk())
            {
                return;
            }
            if self.controlled_stopped() {
                self.stop_entries(ControlledStop::Deadline);
                return;
            }
            tokio::select! {
                _ = notified => (),
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => (),
            }
        }
    }

    pub(super) async fn wait_emergency(&self) {
        loop {
            let notified = self.control_changed.notified();
            if matches!(
                self.entry_stop_reason(),
                Some("PARENT_LOST" | "CONTROL_CHANNEL")
            ) {
                return;
            }
            notified.await;
        }
    }

    /// Poll only already-traded unresolved markets, with bounded whole reads.
    pub(super) async fn tail_settle(
        &self,
        data: &dyn crate::service::venue::MarketData,
        budget: std::time::Duration,
        cadence: std::time::Duration,
    ) {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let ids: Vec<u64> = {
                let state = self.state.lock();
                let Some(paper) = state.paper.as_ref() else {
                    return;
                };
                paper
                    .traded
                    .keys()
                    .filter(|id| !paper.applied_results.contains_key(id))
                    .copied()
                    .collect()
            };
            if ids.is_empty() {
                return;
            }
            for id in ids {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return;
                }
                tokio::select! {
                    biased;
                    _ = self.wait_emergency() => return,
                    result = tokio::time::timeout(remaining.min(std::time::Duration::from_secs(15)), data.market(id)) => {
                        if let Ok(Ok(market)) = result { self.controlled_settle(&market); }
                    }
                }
            }
            tokio::select! {
                biased;
                _ = self.wait_emergency() => return,
                _ = tokio::time::sleep_until(deadline) => return,
                _ = tokio::time::sleep(cadence) => (),
            }
        }
    }

    /// Close entries before cleanup; never use the tail to execute strategy code.
    pub(super) async fn finish_controlled(&self, data: &dyn crate::service::venue::MarketData) {
        if self.live_operation_lane().is_some() {
            self.finish_controlled_live(data).await;
            return;
        }
        self.finish_controlled_with(data, CONTROLLED_CLEANUP_BUDGET, self.cancel_all())
            .await;
    }

    /// The bounded cleanup step of a controlled run.
    ///
    /// The cleanup gets one budget: when it doesn't fit, the run says so in its
    /// final event instead of waiting longer.
    async fn finish_controlled_with(
        &self,
        data: &dyn crate::service::venue::MarketData,
        budget: std::time::Duration,
        cleanup: impl std::future::Future<Output = Result<()>>,
    ) -> bool {
        let cleanup_timed_out = tokio::time::timeout(budget, cleanup).await.is_err();
        if self.entry_stop_reason() == Some("DEADLINE") && self.paper_snapshot().is_some() {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => self.stop_entries(ControlledStop::ParentLost),
                _ = self.tail_settle(data, std::time::Duration::from_secs(600), std::time::Duration::from_secs(60)) => (),
            }
        }
        emit_final(self, cleanup_timed_out);
        cleanup_timed_out
    }

    /// The controlled run's identity, when this is a controlled run.
    pub fn controlled_run(&self) -> Option<&ControlledRun> {
        self.controlled.as_ref()
    }

    /// The controlled cash snapshot, when this is a controlled run.
    pub fn paper_snapshot(&self) -> Option<PaperSnapshot> {
        let state = self.state.lock();
        let paper = state.paper.as_ref()?;
        Some(state_snapshot(paper, &state))
    }

    /// Why new entries stopped, when they did.
    pub fn entry_stop_reason(&self) -> Option<&'static str> {
        self.state
            .lock()
            .entry_stop_reason
            .map(|reason| reason.as_str())
    }
    /// The controlled paper path: one wallet-free order flow with real cash.
    ///
    /// Everything happens under the broker's own lock: the checks, the
    /// reservation, the book walk and the fill. There is no await and no venue
    /// call, so nothing can slip between the check and the fill.
    pub(super) fn controlled_take(
        &self,
        market: &TradableMarket,
        token: &str,
        side: Side,
        shares: f64,
        limit: f64,
        yes_book: &BookSnapshot,
    ) -> Result<TakeResult> {
        let mut state = self.state.lock();
        let result = self.controlled_take_locked(
            &mut state, market, token, side, shares, limit, yes_book, None,
        )?;
        if let TakeResult::Blocked(reason) = &result {
            self.emit(
                &state,
                "blocked",
                Some(market.id),
                Some(token),
                Some(serde_json::json!({"reason": format!("{reason:?}")})),
                None,
            );
        }
        Ok(result)
    }

    /// Buy with a phase1 candidate.
    ///
    /// The candidate is the only context this path accepts: it carries the
    /// frozen inputs, the selected side and the identity of the decision. The
    /// broker checks the candidate twice under its own lock — before the
    /// reservation and again before the accounting — so a delay between the
    /// decision and the fill cannot let a stale candidate through.
    pub fn take_candidate(
        &self,
        market: &TradableMarket,
        yes_book: &BookSnapshot,
        candidate: &CandidateContext,
    ) -> Result<TakeResult> {
        let token = candidate.token().to_string();
        let mut state = self.state.lock();
        let paper = state
            .paper
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE"))?;
        if paper.phase1.is_none() {
            bail!("CONTEXT_MISSING");
        }
        if !paper
            .pending_candidate
            .as_ref()
            .is_some_and(|pending| pending.context.same_candidate(candidate))
        {
            bail!("INPUT_CHANGED");
        }
        let serial: u64 = candidate
            .candidate_id()
            .parse()
            .map_err(|_| anyhow::anyhow!("INPUT_CHANGED"))?;
        if serial <= paper.last_candidate_id {
            bail!("INPUT_CHANGED");
        }
        paper.last_candidate_id = serial;
        paper.last_phase1_reason = None;
        let result = self.controlled_take_locked(
            &mut state,
            market,
            &token,
            Side::Buy,
            candidate.gross_shares(),
            candidate.limit_price(),
            yes_book,
            Some(candidate),
        )?;
        if let TakeResult::Blocked(reason) = &result {
            // The refusal carries the exact diagnostic reason when the phase1
            // check produced one, and the broker's own reason otherwise.
            let diagnostic_reason = state
                .paper
                .as_ref()
                .and_then(|paper| paper.last_phase1_reason)
                .unwrap_or(Reason::BrokerBlocked);
            let link = CandidateLink {
                decision_id: candidate.decision_id().to_string(),
                candidate_id: candidate.candidate_id().to_string(),
                diagnostic_reason,
            };
            let mut details = serde_json::json!({"reason": format!("{reason:?}")});
            if let serde_json::Value::Object(fields) = &mut details {
                for (name, value) in link.fields() {
                    fields.insert(name, value);
                }
            }
            // A blocked event carries the relation, not a second copy of the
            // snapshot: the snapshot already travelled as a summary.
            self.emit(
                &state,
                "blocked",
                Some(market.id),
                Some(&token),
                Some(details),
                None,
            );
        }
        state.paper.as_mut().unwrap().pending_candidate = None;
        Ok(result)
    }

    /// Write one controlled event in this run's own wire version.
    fn emit(
        &self,
        state: &State,
        kind: &str,
        market_id: Option<u64>,
        token: Option<&str>,
        source: Option<serde_json::Value>,
        decision: Option<&DecisionDiagnostic>,
    ) {
        let phase1 = state.paper.as_ref().and_then(|paper| paper.phase1.as_ref());
        let _ = emit_event_v2(
            self.controlled.as_ref(),
            kind,
            market_id,
            token,
            state
                .paper
                .as_ref()
                .map(|paper| paper_accounting(paper, state)),
            source,
            phase1,
            decision,
            None,
        );
    }

    /// Record the evaluation of one attempt.
    ///
    /// The snapshot is the evidence of the attempt: it says what the run saw,
    /// what it concluded and why. It travels in a summary, because a
    /// diagnostic is not a trade.
    pub fn emit_phase1_evaluation(
        &self,
        market: &TradableMarket,
        diagnostic: &DecisionDiagnostic,
        candidate: Option<&CandidateContext>,
    ) -> bool {
        if self.state.lock().live.controlled.is_some() {
            return self.emit_live_evaluation(market, diagnostic, candidate);
        }
        let mut state = self.state.lock();
        let Some(run) = state.paper.as_ref().and_then(|paper| paper.phase1.clone()) else {
            return false;
        };
        // An unevaluated replacement cannot silently abandon an older candidate.
        if let Some(old) = state.paper.as_mut().unwrap().pending_candidate.take() {
            let link = CandidateLink {
                decision_id: old.context.decision_id().into(),
                candidate_id: old.context.candidate_id().into(),
                diagnostic_reason: Reason::InputChanged,
            };
            if emit_event_v2(
                self.controlled.as_ref(),
                "blocked",
                Some(old.context.market_id()),
                Some(old.context.token()),
                Some(paper_accounting(state.paper.as_ref().unwrap(), &state)),
                Some(serde_json::json!({"reason": "DataIncomplete"})),
                Some(&run),
                None,
                Some(&link),
            )
            .is_err()
            {
                return false;
            }
        }
        let prepare = |decision: &DecisionDiagnostic| {
            prepare_event(
                self.controlled.as_ref(),
                "summary",
                Some(market.id),
                None,
                Some(paper_accounting(state.paper.as_ref().unwrap(), &state)),
                None,
                Some(&run),
                Some(decision),
                None,
            )
        };
        let (event, accepted) = match prepare(diagnostic) {
            Ok(event) => (event, true),
            Err(_) => {
                let mut limited = scalar_refusal(diagnostic.clone());
                limited.candidate_id = None;
                limited.selected_token = None;
                let Ok(event) = prepare(&limited) else {
                    run.output_failed.store(true, Ordering::SeqCst);
                    return false;
                };
                (event, false)
            }
        };
        if event.flush(Some(&run)).is_err() {
            run.output_failed.store(true, Ordering::SeqCst);
            return false;
        }
        if accepted {
            if let Some(context) = candidate {
                state.paper.as_mut().unwrap().pending_candidate = Some(PendingCandidate {
                    context: context.clone(),
                    phase: Phase::Evaluate,
                    checked: CheckTime {
                        utc: diagnostic.checked_at,
                        elapsed_ms: diagnostic.checked_elapsed_ms,
                    },
                });
            }
        }
        accepted
    }

    /// The first check of a candidate, before anything is reserved.
    ///
    /// It reads the run's own clock, recomputes the candidate's value and
    /// writes the PRE_SUBMIT snapshot. A candidate that no longer holds is
    /// refused here, before a single dollar is spoken for.
    ///
    /// A clock that cannot be read is a refusal, never an excuse: the check
    /// either happens with a real reading or it does not happen at all.
    fn phase1_pre_submit(
        &self,
        state: &mut State,
        candidate: &CandidateContext,
        market: &TradableMarket,
    ) -> Option<(Blocked, Reason)> {
        let phase1 = state.paper.as_ref()?.phase1.as_ref()?.clone();
        // A clock that cannot be read is a refusal, not an excuse: the check
        // either happens with a real reading or the candidate is refused.
        let now = match phase1.clock.now() {
            Ok(now) => now,
            Err(_) => return Some((Blocked::DataIncomplete, Reason::ClockInvalid)),
        };
        let (diagnostic, recheck) = phase1::recheck_diagnostic(
            candidate,
            now,
            Phase::PreSubmit,
            phase1.min_edge,
            phase1.min_secs_left,
        );
        let accounting = state
            .paper
            .as_ref()
            .map(|paper| paper_accounting(paper, state));
        let paper = state.paper.as_mut()?;
        paper.pending_candidate = Some(PendingCandidate {
            context: candidate.clone(),
            phase: Phase::PreSubmit,
            checked: now,
        });
        // A diagnostic summary carries the market and no token: the snapshot
        // itself names the token it belongs to.
        let _ = emit_event_v2(
            self.controlled.as_ref(),
            "summary",
            Some(market.id),
            None,
            accounting,
            None,
            Some(&phase1),
            Some(&diagnostic),
            None,
        );
        recheck
            .is_none()
            .then_some((Blocked::DataIncomplete, diagnostic.reason))
    }

    /// The second check of a candidate, before the accounting happens.
    ///
    /// It runs under the same lock as the fill, after the reservation and
    /// before any share is booked, so a candidate that went stale while the
    /// reservation was being made is released instead of filled.
    fn phase1_pre_apply(
        &self,
        state: &State,
        candidate: &CandidateContext,
        market: &TradableMarket,
    ) -> std::result::Result<(PreparedEvent, Reason), Reason> {
        let run = state
            .paper
            .as_ref()
            .and_then(|paper| paper.phase1.as_ref())
            .ok_or(Reason::ContextMissing)?;
        let prepare = |now| {
            let (mut diagnostic, _) = phase1::recheck_diagnostic(
                candidate,
                now,
                Phase::PreApply,
                run.min_edge,
                run.min_secs_left,
            );
            if self.controlled_stopped()
                || state.entry_stop_reason.is_some()
                || state.halted
                || run.output_failed.load(Ordering::SeqCst)
                || state.paper.as_ref().unwrap().loss_latched
            {
                diagnostic.reason = Reason::RunStopped;
            }
            let encode = |diagnostic: &DecisionDiagnostic| {
                prepare_event(
                    self.controlled.as_ref(),
                    "summary",
                    Some(market.id),
                    None,
                    Some(paper_accounting(state.paper.as_ref().unwrap(), state)),
                    None,
                    Some(run),
                    Some(diagnostic),
                    None,
                )
            };
            let event = encode(&diagnostic)
                .or_else(|_| {
                    diagnostic = scalar_refusal(diagnostic.clone());
                    encode(&diagnostic)
                })
                .map_err(|_| Reason::DiagnosticLimit)?;
            Ok((event, diagnostic.reason))
        };
        // Preflight bounded local encoding; it must not write stdout. The second
        // reading replaces this provisional snapshot, including its reason.
        let first = run.clock.now().map_err(|_| Reason::ClockInvalid)?;
        if state
            .paper
            .as_ref()
            .and_then(|paper| paper.pending_candidate.as_ref())
            .is_some_and(|pending| {
                first.utc < pending.checked.utc || first.elapsed_ms < pending.checked.elapsed_ms
            })
        {
            return Err(Reason::ClockInvalid);
        }
        let _ = prepare(first)?;
        let final_now = run.clock.now().map_err(|_| Reason::ClockInvalid)?;
        if final_now.utc < first.utc || final_now.elapsed_ms < first.elapsed_ms {
            return Err(Reason::ClockInvalid);
        }
        prepare(final_now)
    }

    fn controlled_take_locked(
        &self,
        mut state: &mut State,
        market: &TradableMarket,
        token: &str,
        side: Side,
        shares: f64,
        limit: f64,
        yes_book: &BookSnapshot,
        candidate: Option<&CandidateContext>,
    ) -> Result<TakeResult> {
        if !shares.is_finite()
            || shares <= 0.0
            || !limit.is_finite()
            || !(0.0..1.0).contains(&limit)
            || limit == 0.0
            || !valid_round(market)
            || market.fee_rate_bps > 10_000
            || (token != market.yes_token && token != market.no_token)
        {
            return Ok(TakeResult::Blocked(Blocked::MarketRulesUnknown));
        }

        let precision = market
            .round_facts
            .as_ref()
            .and_then(|facts| facts.decimal_precision);
        if !market.tick.is_finite()
            || market.tick <= 0.0
            || !precision.is_some_and(|places| {
                places <= 18 && (market.tick - 10f64.powi(-(places as i32))).abs() < 1e-12
            })
            || (limit / market.tick - (limit / market.tick).round()).abs() > 1e-9
        {
            return Ok(TakeResult::Blocked(Blocked::MarketRulesUnknown));
        }
        if state.paper.is_none() {
            bail!("CONTROLLED_MODE: the controlled cash is missing");
        }
        if state.entry_stop_reason.is_some()
            || self.controlled_stopped()
            || state
                .paper
                .as_ref()
                .and_then(|paper| paper.phase1.as_ref())
                .is_some_and(|run| run.output_failed.load(Ordering::SeqCst))
        {
            state.paper.as_mut().unwrap().last_phase1_reason = Some(Reason::RunStopped);
            return Ok(TakeResult::Blocked(Blocked::Deadline));
        }
        self.check_controlled_loss_limit(&mut state);
        if state.halted || state.paper.as_ref().unwrap().loss_latched {
            state.paper.as_mut().unwrap().last_phase1_reason = Some(Reason::RunStopped);
            return Ok(TakeResult::Blocked(Blocked::LossLatch));
        }
        // The first check of the candidate: the inputs are still the current
        // ones, and the round still has enough time left.
        if let Some(candidate) = candidate {
            if !candidate.matches_inputs(market, yes_book) {
                state.paper.as_mut().unwrap().last_phase1_reason = Some(Reason::InputChanged);
                return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
            }
            if let Some((blocked, reason)) = self.phase1_pre_submit(&mut state, candidate, market) {
                if let Some(paper) = state.paper.as_mut() {
                    paper.last_phase1_reason = Some(reason);
                }
                return Ok(TakeResult::Blocked(blocked));
            }
        }
        if mid_of(yes_book).is_none()
            || !marks_complete(state.paper.as_ref().unwrap(), &state.holdings)
        {
            return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
        }
        let shares = if side == Side::Sell {
            shares.min(
                state
                    .holdings
                    .get(token)
                    .map_or(0.0, |holding| holding.shares),
            )
        } else {
            shares
        };
        if shares <= 0.0 {
            return Ok(TakeResult::Blocked(Blocked::NothingHeld));
        }
        let book = token_book(market, token, yes_book);
        let levels = match side {
            Side::Buy => &book.asks,
            Side::Sell => &book.bids,
        };
        let Some(avg_price) = walk(levels, side, shares, limit) else {
            return Ok(TakeResult::Blocked(Blocked::ThinBook));
        };
        // The run's own protocol version travels with every event it writes.
        let phase1 = state.paper.as_ref().and_then(|paper| paper.phase1.clone());

        match side {
            Side::Buy => {
                if !market.open
                    || market.contested
                    || state.paper.as_ref().unwrap().reservation.is_some()
                {
                    return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                }
                let worst_case = shares * limit;
                let market_exposure = state.market_exposure(market.id);
                let total_exposure = state.total_exposure();
                let available = state.paper.as_ref().unwrap().available()
                    - (total_exposure - paper_cost(&state.holdings));
                // The candidate's own book must be usable this tick: an entry on
                // a stale or one-sided book isn't a decision, it's a guess.
                let candidate_marked = state
                    .paper
                    .as_ref()
                    .expect("the paper state is present")
                    .marks
                    .get(token)
                    .is_some_and(|mark| mark.valid);
                if !candidate_marked {
                    return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                }
                if worst_case < MIN_ORDER_USD {
                    return Ok(TakeResult::Blocked(Blocked::BelowMinimum));
                }
                if worst_case > self.limits.max_order_usd {
                    return Ok(TakeResult::Blocked(Blocked::OrderCap));
                }
                if market_exposure + worst_case > self.limits.max_market_usd {
                    return Ok(TakeResult::Blocked(Blocked::MarketCap));
                }
                if total_exposure + worst_case > self.limits.max_total_usd {
                    return Ok(TakeResult::Blocked(Blocked::TotalCap));
                }
                if worst_case > available {
                    return Ok(TakeResult::Blocked(Blocked::InsufficientCash));
                }
                let cost = shares * avg_price;
                if cost > worst_case {
                    // The walk promised a price no worse than the limit.
                    return Ok(TakeResult::Blocked(Blocked::ThinBook));
                }
                // Preflight immutable fields and worst-case scalar widths for
                // both outcomes; final accounting still comes from real state.
                if let Some(candidate) = candidate {
                    // Leave room for reservation, PRE_APPLY, release and blocked.
                    if phase1
                        .as_ref()
                        .unwrap()
                        .event_seq
                        .load(Ordering::SeqCst)
                        .checked_add(4)
                        .is_none()
                    {
                        state.paper.as_mut().unwrap().last_phase1_reason =
                            Some(Reason::DiagnosticLimit);
                        return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                    }
                    let Some((net, fee)) = net_shares(shares, avg_price, market.fee_rate_bps)
                    else {
                        state.paper.as_mut().unwrap().last_phase1_reason =
                            Some(Reason::NetSharesInvalid);
                        return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                    };
                    let source = buy_source(
                        shares,
                        net,
                        avg_price,
                        fee,
                        market.fee_rate_bps,
                        Some(
                            serde_json::json!({"name": candidate.source().source, "clock": candidate.spot_clock()}),
                        ),
                    );
                    let link = CandidateLink {
                        decision_id: candidate.decision_id().into(),
                        candidate_id: candidate.candidate_id().into(),
                        diagnostic_reason: Reason::Eligible,
                    };
                    if prepare_event(
                        self.controlled.as_ref(),
                        "fill",
                        Some(market.id),
                        Some(token),
                        Some(paper_accounting(state.paper.as_ref().unwrap(), state)),
                        Some(source),
                        phase1.as_ref(),
                        None,
                        Some(&link),
                    )
                    .and_then(|_| {
                        prepare_event(
                            self.controlled.as_ref(),
                            "release",
                            Some(market.id),
                            Some(token),
                            Some(paper_accounting(state.paper.as_ref().unwrap(), state)),
                            None,
                            phase1.as_ref(),
                            None,
                            Some(&link),
                        )
                    })
                    .is_err()
                    {
                        state.paper.as_mut().unwrap().last_phase1_reason =
                            Some(Reason::DiagnosticLimit);
                        return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                    }
                }
                // Reserve the worst case before the fill: the money is spoken
                // for until the fill replaces it with the real cost.
                state.paper.as_mut().unwrap().reservation = Some(Reservation {
                    market_id: market.id,
                    token: token.to_string(),
                    worst_case,
                });
                let link = candidate.map(|candidate| CandidateLink {
                    decision_id: candidate.decision_id().to_string(),
                    candidate_id: candidate.candidate_id().to_string(),
                    diagnostic_reason: Reason::Eligible,
                });
                let _ = emit_event_v2(
                    self.controlled.as_ref(),
                    "reservation",
                    Some(market.id),
                    Some(token),
                    Some(paper_accounting(state.paper.as_ref().unwrap(), &state)),
                    None,
                    phase1.as_ref(),
                    None,
                    link.as_ref(),
                );
                self.check_controlled_loss_limit(&mut state);
                if self.controlled_stopped()
                    || state.entry_stop_reason.is_some()
                    || state.paper.as_ref().unwrap().loss_latched
                {
                    let paper = state.paper.as_mut().unwrap();
                    paper.reservation = None;
                    // The run stopped after the reservation was made: the
                    // candidate never got to be applied, and the terminal
                    // refusal says so instead of the broker's own reason.
                    paper.last_phase1_reason = Some(Reason::RunStopped);
                    let link = candidate.map(|candidate| CandidateLink {
                        decision_id: candidate.decision_id().to_string(),
                        candidate_id: candidate.candidate_id().to_string(),
                        // A deadline or a loss latch stops the run: the
                        // candidate never got to be applied.
                        diagnostic_reason: Reason::RunStopped,
                    });
                    let _ = emit_event_v2(
                        self.controlled.as_ref(),
                        "release",
                        Some(market.id),
                        Some(token),
                        Some(paper_accounting(state.paper.as_ref().unwrap(), &state)),
                        None,
                        phase1.as_ref(),
                        None,
                        link.as_ref(),
                    );
                    return Ok(TakeResult::Blocked(if self.controlled_stopped() {
                        Blocked::Deadline
                    } else {
                        Blocked::LossLatch
                    }));
                }
                let mut apply_event = None;
                if let Some(candidate) = candidate {
                    let prepared = self.phase1_pre_apply(state, candidate, market);
                    let reason = match prepared {
                        Ok((event, reason)) => {
                            apply_event = Some(event);
                            reason
                        }
                        Err(reason) => reason,
                    };
                    if reason != Reason::Eligible {
                        let paper = state.paper.as_mut().unwrap();
                        paper.reservation = None;
                        paper.last_phase1_reason = Some(reason);
                        if let Some(event) = apply_event.take() {
                            event.flush(phase1.as_ref())?;
                        }
                        let link = CandidateLink {
                            decision_id: candidate.decision_id().to_string(),
                            candidate_id: candidate.candidate_id().to_string(),
                            diagnostic_reason: reason,
                        };
                        emit_event_v2(
                            self.controlled.as_ref(),
                            "release",
                            Some(market.id),
                            Some(token),
                            Some(paper_accounting(state.paper.as_ref().unwrap(), state)),
                            None,
                            phase1.as_ref(),
                            None,
                            Some(&link),
                        )?;
                        return Ok(TakeResult::Blocked(Blocked::DataIncomplete));
                    }
                }
                // No blocking output between the final snapshot above and this
                // mutation. The local PRE_APPLY is flushed before the fill below.
                let (net, fee) = {
                    let paper = state.paper.as_mut().expect("the paper state is present");
                    let Some((net, fee)) = apply_buy(
                        paper,
                        market.id,
                        token,
                        shares,
                        avg_price,
                        market.fee_rate_bps,
                    ) else {
                        paper.reservation = None;
                        return Ok(TakeResult::Blocked(Blocked::MarketRulesUnknown));
                    };
                    (net, fee)
                };
                state
                    .paper
                    .as_mut()
                    .unwrap()
                    .traded
                    .entry(market.id)
                    .or_insert_with(|| market.clone());
                state.apply_controlled_buy(market.id, token, net, cost);
                state.fees += fee;
                let cash = state
                    .paper
                    .as_ref()
                    .expect("the paper state is present")
                    .cash;
                let price_source = state
                    .paper
                    .as_ref()
                    .expect("the paper state is present")
                    .sources
                    .get(token)
                    .map(|(name, at)| {
                        serde_json::json!({
                            "name": name,
                            "observed_at": at.to_rfc3339(),
                        })
                    });
                // A phase1 fill reports the clock of the price it was decided
                // on: the old observed_at was the decision time, not the
                // source's receipt.
                let price_source = match candidate {
                    Some(candidate) => Some(serde_json::json!({
                        "name": candidate.source().source,
                        "clock": candidate.spot_clock(),
                    })),
                    None => price_source,
                };
                if let (Some(candidate), Some(paper)) = (candidate, state.paper.as_mut()) {
                    paper
                        .filled_candidates
                        .insert(token.to_string(), candidate.candidate_id().to_string());
                    paper.pending_candidate = None;
                }
                let details = buy_source(
                    shares,
                    net,
                    avg_price,
                    fee,
                    market.fee_rate_bps,
                    price_source,
                );
                let link = candidate.map(|candidate| CandidateLink {
                    decision_id: candidate.decision_id().to_string(),
                    candidate_id: candidate.candidate_id().to_string(),
                    diagnostic_reason: Reason::Eligible,
                });
                if let Some(event) = apply_event {
                    if event.flush(phase1.as_ref()).is_err() {
                        state.entry_stop_reason = Some(ControlledStop::ControlChannel);
                        bail!("OUTPUT_INVALID");
                    }
                }
                let _ = emit_event_v2(
                    self.controlled.as_ref(),
                    "fill",
                    Some(market.id),
                    Some(token),
                    Some(paper_accounting(
                        state.paper.as_ref().expect("the paper state is present"),
                        &state,
                    )),
                    Some(details),
                    phase1.as_ref(),
                    None,
                    link.as_ref(),
                );
                info!(
                    market = market.id,
                    token,
                    gross_shares = shares,
                    net_shares = net,
                    price = avg_price,
                    cost,
                    fee,
                    cash,
                    "controlled paper fill"
                );
                self.check_controlled_loss_limit(&mut state);
                Ok(TakeResult::Filled {
                    shares: net,
                    price: avg_price,
                })
            }
            Side::Sell => {
                let held = state.holdings.get(token).map_or(0.0, |h| h.shares);
                if held <= EPS {
                    return Ok(TakeResult::Blocked(Blocked::NothingHeld));
                }
                let sold = shares.min(held);
                if sold * limit < MIN_ORDER_USD {
                    return Ok(TakeResult::Blocked(Blocked::BelowMinimum));
                }
                let avg_cost = state
                    .holdings
                    .get(token)
                    .map(|h| h.avg_price())
                    .unwrap_or(0.0);
                let (net_cash, basis, pnl, fee) = {
                    let paper = state.paper.as_mut().expect("the paper state is present");
                    let Some(result) =
                        apply_sell(paper, sold, avg_price, market.fee_rate_bps, avg_cost)
                    else {
                        return Ok(TakeResult::Blocked(Blocked::MarketRulesUnknown));
                    };
                    result
                };
                state.apply_controlled_sell(token, sold, basis, pnl);
                state.fees += fee;
                let cash = state
                    .paper
                    .as_ref()
                    .expect("the paper state is present")
                    .cash;
                emit_event(
                    self.controlled.as_ref(),
                    "fill",
                    Some(market.id),
                    Some(token),
                    Some(paper_accounting(
                        state.paper.as_ref().expect("the paper state is present"),
                        &state,
                    )),
                    Some(serde_json::json!({
                        "side": "sell",
                        "shares": sold,
                        "price": avg_price,
                        "net_cash": net_cash,
                        "basis": basis,
                        "pnl": pnl,
                        "fee": fee,
                        "fee_rate_bps": market.fee_rate_bps,
                        "fee_direction": "cash",
                    })),
                    phase1.as_ref(),
                );
                info!(
                    market = market.id,
                    token,
                    shares = sold,
                    price = avg_price,
                    net_cash,
                    basis,
                    pnl,
                    cash,
                    "controlled paper sell"
                );
                self.check_controlled_loss_limit(&mut state);
                Ok(TakeResult::Filled {
                    shares: sold,
                    price: avg_price,
                })
            }
        }
    }

    /// Whether the controlled run's deadline has passed.
    pub(super) fn controlled_stopped(&self) -> bool {
        #[cfg(test)]
        if controlled_clock_expired() {
            return true;
        }
        self.controlled.as_ref().is_some_and(|run| run.stopped())
    }

    /// Latch the loss limit when the controlled run's net result reaches it.
    fn check_controlled_loss_limit(&self, state: &mut State) {
        let Some(paper) = state.paper.as_ref() else {
            return;
        };
        if paper.loss_latched {
            return;
        }
        if loss_limit_reached(
            paper,
            &state.holdings,
            state.realized,
            self.limits.max_loss_usd,
        ) {
            state.halted = true;
            state
                .paper
                .as_mut()
                .expect("the paper state is present")
                .loss_latched = true;
            emit_event(
                self.controlled.as_ref(),
                "loss_latch",
                None,
                None,
                Some(paper_accounting(
                    state.paper.as_ref().expect("the paper state is present"),
                    &state,
                )),
                None,
                state.paper.as_ref().and_then(|paper| paper.phase1.as_ref()),
            );
            error!(
                limit = self.limits.max_loss_usd,
                "loss limit reached: this controlled run adds no new entries"
            );
        }
    }
    /// The controlled observe path: only a two-sided mid seen this tick makes a
    /// mark usable, and the loss limit is checked right after.
    pub(super) fn controlled_observe(
        &self,
        state: &mut State,
        market: &TradableMarket,
        yes_book: &BookSnapshot,
    ) {
        let now = super::observer::paper_now();
        let mid = mid_of(yes_book);
        let Some(paper) = state.paper.as_mut() else {
            return;
        };
        match mid {
            Some(mid) => {
                mark(paper, &market.yes_token, mid, now, true);
                mark(paper, &market.no_token, 1.0 - mid, now, true);
            }
            None => {
                // A one-sided book proves nothing: the old mark stays for
                // disclosure, but it can't let a new entry through.
                mark(paper, &market.yes_token, 0.0, now, false);
                mark(paper, &market.no_token, 0.0, now, false);
            }
        }
        self.check_controlled_loss_limit(state);
    }
    /// Drop every mark's validity, so only this tick's books can restore it.
    ///
    /// A controlled run calls this at the start of each tick: a mark from an
    /// older tick proves nothing about the current price.
    pub fn invalidate_paper_marks(&self) {
        let mut state = self.state.lock();
        if let Some(paper) = state.paper.as_mut() {
            invalidate_marks(paper);
        }
    }
    /// Settle a controlled market from the venue's own facts.
    ///
    /// Only a confirmed official result pays out, and only once: a repeated
    /// result adds no cash, and a contradictory one is recorded as a conflict
    /// without touching the books.
    pub(crate) fn controlled_settle(&self, market: &TradableMarket) {
        let mut state = self.state.lock();
        let Some(paper) = state.paper.as_ref() else {
            return;
        };
        let Some(original) = paper.traded.get(&market.id) else {
            return;
        };
        let identity = same_round(original, market);
        let prices = market.start_price.zip(market.end_price);
        let mut outcome = if identity == RoundIdentity::Contradiction {
            Settlement::Conflict
        } else if identity == RoundIdentity::Incomplete {
            Settlement::Pending
        } else if let (Some(facts), Some((start, end))) = (market.round_facts.as_ref(), prices) {
            settle_from_facts(market, facts, start, end)
        } else {
            Settlement::Pending
        };
        let payout = prices.and_then(|(start, end)| settlement_payout(market, start, end));
        if outcome == Settlement::Confirmed {
            let (start, end) = prices.unwrap();
            let expected = if end > start {
                1.0
            } else if end < start {
                0.0
            } else {
                0.5
            };
            if payout.is_none() {
                outcome = Settlement::Pending;
            } else if payout != Some(expected) {
                outcome = Settlement::Conflict;
            }
        }
        if let Some(applied) = paper.applied_results.get(&market.id) {
            if revised_result(market, *applied, identity, outcome) {
                state
                    .paper
                    .as_mut()
                    .unwrap()
                    .settlements
                    .insert(market.id, Settlement::Conflict);
                state.entry_stop_reason = Some(ControlledStop::SettlementConflict);
            }
            return;
        }
        if outcome != Settlement::Confirmed {
            state
                .paper
                .as_mut()
                .unwrap()
                .settlements
                .insert(market.id, outcome);
            if outcome == Settlement::Conflict {
                state.entry_stop_reason = Some(ControlledStop::SettlementConflict);
            }
            return;
        }
        let payout_yes = payout.unwrap();
        let (start_price, end_price) = prices.unwrap();
        state
            .paper
            .as_mut()
            .unwrap()
            .applied_results
            .insert(market.id, (start_price, end_price, payout_yes));
        // Pay out every outcome this run still holds in that market.
        let tokens: Vec<String> = state
            .holdings
            .values()
            .filter(|h| h.market_id == market.id)
            .map(|h| h.token.clone())
            .collect();
        for token in tokens {
            let Some((shares, basis)) = state.holdings.get(&token).map(|h| (h.shares, h.cost))
            else {
                continue;
            };
            let payout = if market.is_yes(&token) {
                payout_yes
            } else {
                1.0 - payout_yes
            };
            let income = {
                let paper = state.paper.as_mut().expect("the paper state is present");
                let Some(income) = apply_settlement(paper, shares, payout) else {
                    continue;
                };
                income
            };
            state.realized += income - basis;
            state.holdings.remove(&token);
            let cash = state
                .paper
                .as_ref()
                .expect("the paper state is present")
                .cash;
            let candidate_id = state
                .paper
                .as_ref()
                .and_then(|paper| paper.filled_candidates.get(&token).cloned());
            let mut details = serde_json::json!({
                "shares": shares,
                "payout": payout,
                "income": income,
                "basis": basis,
                "provider": market.round_facts.as_ref()
                    .and_then(|facts| facts.price_feed_provider.as_deref())
                    .filter(|provider| *provider == "CHAINLINK").unwrap_or("UNKNOWN"),
            });
            if let (Some(candidate_id), Some(serde_json::Value::Object(fields))) =
                (candidate_id, Some(&mut details))
            {
                fields.insert("candidate_id".into(), serde_json::json!(candidate_id));
            }
            emit_event(
                self.controlled.as_ref(),
                "settlement",
                Some(market.id),
                Some(&token),
                Some(paper_accounting(
                    state.paper.as_ref().expect("the paper state is present"),
                    &state,
                )),
                Some(details),
                state.paper.as_ref().and_then(|paper| paper.phase1.as_ref()),
            );
            info!(
                market = market.id,
                token, shares, payout, income, cash, "controlled paper settled"
            );
        }
        state
            .paper
            .as_mut()
            .expect("the paper state is present")
            .settlements
            .insert(market.id, Settlement::Confirmed);
        self.check_controlled_loss_limit(&mut state);
    }
}

/// The prefix every controlled event carries on stdout.
///
/// The events are the only account output: they let the run be recomputed
/// line by line, without a second ledger.
pub const EVENT_PREFIX: &str = "BTC5M_EVENT ";

/// The wire version of the controlled protocol.
pub const SCHEMA_VERSION_V1: &str = "btc5m-controlled-paper.v1";
/// The wire version of the phase1 protocol.
pub const SCHEMA_VERSION_V2: &str = "btc5m-controlled-paper.v2";

/// The phase1 economics the protocol names.
pub const STRATEGY_CONTRACT: &str = "phase1_net_shares";

/// Write one controlled event as a single line of JSON.
///
/// A phase1 run speaks v2 for every one of its events, not only the ones
/// about a candidate: the sequence number is what makes the whole run
/// readable in order.
fn emit_event(
    run: Option<&ControlledRun>,
    kind: &str,
    market_id: Option<u64>,
    token: Option<&str>,
    accounting: Option<serde_json::Value>,
    mut source: Option<serde_json::Value>,
    phase1: Option<&Phase1Run>,
) {
    let _ = emit_event_v2(
        run, kind, market_id, token, accounting, source, phase1, None, None,
    );
}

/// The linkage a candidate's execution events carry.
///
/// It is the relation the protocol needs to follow one decision from its
/// evaluation to its terminal event: which attempt it was, which candidate it
/// produced, and why it ended the way it did. Nothing else is added.
#[derive(Debug, Clone)]
pub struct CandidateLink {
    pub decision_id: String,
    pub candidate_id: String,
    pub diagnostic_reason: Reason,
}

impl CandidateLink {
    /// The fields this linkage adds to an event's source.
    fn fields(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut fields = serde_json::Map::new();
        fields.insert("decision_id".into(), serde_json::json!(self.decision_id));
        fields.insert("candidate_id".into(), serde_json::json!(self.candidate_id));
        fields.insert(
            "diagnostic_reason".into(),
            serde_json::json!(self.diagnostic_reason),
        );
        fields
    }
}

/// Write one controlled event, with the phase1 envelope when there is one.
///
/// The v2 envelope adds exactly two fields: the wire version and the event
/// sequence. Everything else keeps the v1 shape, so a reader that knows the
/// old protocol still recognises the old fields.
///
/// The diagnostic object may only travel in a summary: a decision snapshot is
/// an evaluation, not a trade. The execution events carry the linkage
/// instead, so the relation can be followed without a second copy of the
/// snapshot.
#[allow(clippy::too_many_arguments)]
fn emit_event_v2(
    run: Option<&ControlledRun>,
    kind: &str,
    market_id: Option<u64>,
    token: Option<&str>,
    accounting: Option<serde_json::Value>,
    source: Option<serde_json::Value>,
    phase1: Option<&Phase1Run>,
    decision: Option<&DecisionDiagnostic>,
    linkage: Option<&CandidateLink>,
) -> Result<()> {
    if phase1.is_some_and(|run| run.output_failed.load(Ordering::SeqCst)) {
        bail!("OUTPUT_INVALID");
    }
    let result = prepare_event(
        run, kind, market_id, token, accounting, source, phase1, decision, linkage,
    )
    .and_then(|event| event.flush(phase1));
    if result.is_err() {
        if let Some(run) = phase1 {
            run.output_failed.store(true, Ordering::SeqCst);
        }
    }
    result
}

fn scalar_refusal(mut diagnostic: DecisionDiagnostic) -> DecisionDiagnostic {
    diagnostic.reason = Reason::DiagnosticLimit;
    for side in [&mut diagnostic.up, &mut diagnostic.down]
        .into_iter()
        .flatten()
    {
        side.depth_levels = None;
        side.depth_complete = None;
        side.walk_vwap = None;
        side.execution_cost = None;
        side.execution_net_shares = None;
        side.execution_ev = None;
        side.execution_edge = None;
    }
    diagnostic
}

/// A single locally encoded record, owned by this same-lock call only.
struct PreparedEvent {
    event: serde_json::Value,
    line: String,
}

impl PreparedEvent {
    fn flush(self, run: Option<&Phase1Run>) -> Result<()> {
        let result = (|| {
            if let Some(run) = run {
                if run.output_failed.load(Ordering::SeqCst) {
                    bail!("OUTPUT_INVALID");
                }
                let seq = self.event["event_seq"]
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("OUTPUT_INVALID"))?;
                run.event_seq
                    .compare_exchange(seq - 1, seq, Ordering::SeqCst, Ordering::SeqCst)
                    .map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
            }
            #[cfg(test)]
            {
                if OUTPUT_FAILURE.with(|fault| fault.replace(false)) {
                    bail!("OUTPUT_INVALID");
                }
                RECORDED.with(|events| events.borrow_mut().push(self.event));
                println!("{EVENT_PREFIX}{}", self.line);
            }
            #[cfg(not(test))]
            {
                use std::io::Write;
                let mut stdout = std::io::stdout().lock();
                stdout.write_all(format!("{EVENT_PREFIX}{}\n", self.line).as_bytes())?;
                stdout.flush()?;
            }
            Ok(())
        })();
        if result.is_err() {
            if let Some(run) = run {
                run.output_failed.store(true, Ordering::SeqCst);
            }
        }
        result
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_event(
    run: Option<&ControlledRun>,
    kind: &str,
    market_id: Option<u64>,
    token: Option<&str>,
    accounting: Option<serde_json::Value>,
    mut source: Option<serde_json::Value>,
    phase1: Option<&Phase1Run>,
    decision: Option<&DecisionDiagnostic>,
    linkage: Option<&CandidateLink>,
) -> Result<PreparedEvent> {
    #[cfg(test)]
    if ENCODING_LIMIT.with(|fault| fault.replace(false)) {
        bail!("DIAGNOSTIC_LIMIT");
    }
    if matches!(kind, "fill" | "settlement") {
        if let Some(serde_json::Value::Object(fields)) = source.as_mut() {
            fields.insert("cash_unit".into(), serde_json::json!("USDT"));
            fields.insert("share_unit".into(), serde_json::json!("shares"));
        }
    }
    let mut event = serde_json::Map::new();
    event.insert(
        "run_id".into(),
        serde_json::json!(run.map(|run| run.run_id.as_str())),
    );
    event.insert("event_kind".into(), serde_json::json!(kind));
    event.insert("market_id".into(), serde_json::json!(market_id));
    event.insert("token".into(), serde_json::json!(token));
    event.insert("accounting".into(), serde_json::json!(accounting));
    let mut fields = match source {
        Some(serde_json::Value::Object(fields)) => fields,
        Some(other) => {
            event.insert("source".into(), other);
            serde_json::Map::new()
        }
        None => serde_json::Map::new(),
    };
    if let Some(decision) = decision {
        // A snapshot that cannot be represented is a limit, not a shorter
        // snapshot: the values that are missing stay missing.
        if !phase1::diagnostic_is_finite(decision)
            || [&decision.up, &decision.down]
                .into_iter()
                .flatten()
                .any(|side| {
                    side.depth_levels
                        .as_ref()
                        .is_some_and(|levels| levels.len() > 1000)
                })
        {
            return Err(anyhow::anyhow!("DIAGNOSTIC_LIMIT"));
        }
        let value = serde_json::to_value(decision)?;
        fields.insert("decision".into(), value);
    }
    if let Some(linkage) = linkage {
        for (name, value) in linkage.fields() {
            fields.insert(name, value);
        }
    }
    if !fields.is_empty() {
        event.insert("source".into(), serde_json::Value::Object(fields));
    } else if !event.contains_key("source") {
        event.insert("source".into(), serde_json::Value::Null);
    }
    event.insert(
        "event_time".into(),
        serde_json::json!(super::observer::paper_now().to_rfc3339()),
    );
    if let Some(phase1) = phase1 {
        event.insert(
            "schema_version".into(),
            serde_json::json!(SCHEMA_VERSION_V2),
        );
        event.insert(
            "event_seq".into(),
            serde_json::json!(phase1
                .event_seq
                .load(Ordering::SeqCst)
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("DIAGNOSTIC_LIMIT"))?),
        );
    }
    let event = serde_json::Value::Object(event);
    let line = serde_json::to_string(&event)?;
    if phase1.is_some() && linkage.is_some() && matches!(kind, "fill" | "release") {
        let Some(accounting) = event["accounting"].as_object() else {
            bail!("DIAGNOSTIC_LIMIT");
        };
        // 64 bytes covers JSON f64/u64/bool/null, Chrono RFC3339 and Reason.
        // Replacing their actual widths by this bound makes the capacity proof
        // independent of post-state digits, nullable values and event sequence.
        let mut bound = EVENT_PREFIX.len() + line.len() + 1;
        for value in accounting.values().chain([
            &event["event_seq"],
            &event["event_time"],
            &event["source"]["diagnostic_reason"],
        ]) {
            let width = serde_json::to_string(value)?.len();
            if width > 64 {
                bail!("DIAGNOSTIC_LIMIT");
            }
            bound += 64 - width;
        }
        if bound > 64 * 1024 {
            bail!("DIAGNOSTIC_LIMIT");
        }
    }
    // Only the aggregate final grows with the run; all ordinary events retain 64 KiB.
    let limit = if kind == "final" && run.is_some_and(ControlledRun::is_paper72) {
        1024 * 1024
    } else {
        64 * 1024
    };
    if EVENT_PREFIX.len() + line.len() + 1 > limit {
        bail!("DIAGNOSTIC_LIMIT");
    }
    Ok(PreparedEvent { event, line })
}

/// The events this test emitted, in order.
///
/// The recorder is test-only: the production path prints the events and keeps
/// no second copy of them.
#[cfg(test)]
pub(crate) fn recorded_events() -> Vec<serde_json::Value> {
    RECORDED.with(|events| events.borrow().clone())
}

/// Forget the events recorded so far.
#[cfg(test)]
pub(crate) fn clear_recorded_events() {
    RECORDED.with(|events| events.borrow_mut().clear());
}

#[cfg(test)]
thread_local! {
    static RECORDED: std::cell::RefCell<Vec<serde_json::Value>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static ENCODING_LIMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static OUTPUT_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The controlled cash state, as the events report it.
fn state_snapshot(paper: &PaperCash, state: &State) -> PaperSnapshot {
    let mut result = snapshot(paper, &state.holdings, state.realized);
    let unclosed = state.total_exposure() - result.cost;
    result.reserved += unclosed;
    result.available -= unclosed;
    result
}

fn buy_source(
    gross: f64,
    net: f64,
    price: f64,
    fee: f64,
    bps: u32,
    price_source: Option<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({"side": "buy", "gross_shares": gross, "net_shares": net,
        "price": price, "cost": gross * price, "fee": fee, "fee_rate_bps": bps,
        "fee_direction": "shares", "fee_shares": fee / price, "price_source": price_source})
}

fn paper_accounting(paper: &PaperCash, state: &State) -> serde_json::Value {
    let snapshot = state_snapshot(paper, state);
    serde_json::json!({
        "cash": snapshot.cash,
        "available": snapshot.available,
        "reserved": snapshot.reserved,
        "cost": snapshot.cost,
        "realized": snapshot.realized,
        "unrealized": snapshot.unrealized,
        "nav": snapshot.nav,
        "net_pnl": snapshot.net_pnl,
        "loss_latched": snapshot.loss_latched,
        "fees_in_cash": state.fees,
    })
}
/// Watch the parent's control pipe and stop new risk when it says so.
///
/// The parent writes one byte, `D`, when the deadline closes the window, and
/// closes the pipe when it loses the run. Either way the broker stops adding
/// risk through its own lock. The thread only sets that flag: it never runs
/// the strategy, never touches the network and never holds a secret.
pub(super) fn watch_control_channel(broker: Arc<Broker>) {
    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buffer = [0u8; 1];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) => {
                    // The pipe closed: the parent is gone.
                    broker.stop_entries(ControlledStop::ParentLost);
                    return;
                }
                Ok(_) => {
                    if buffer[0] == b'D' {
                        broker.stop_entries(ControlledStop::Deadline);
                    } else {
                        broker.stop_entries(ControlledStop::ControlChannel);
                        return;
                    }
                    // Keep watching: a close after the deadline still matters.
                }
                Err(_) => {
                    broker.stop_entries(ControlledStop::ParentLost);
                    return;
                }
            }
        }
    });
}
/// The final accounting event of a controlled run.
///
/// Everything a reader needs to recompute the run line by line is in here.
pub(super) fn emit_summary(broker: &super::Broker) {
    let state = broker.state.lock();
    let Some(paper) = state.paper.as_ref() else {
        return;
    };
    let snapshot = state_snapshot(paper, &state);
    let s = state.summary();
    emit_event(
        broker.controlled.as_ref(),
        "summary",
        None,
        None,
        Some(serde_json::json!({
            "cash": snapshot.cash,
            "available": snapshot.available,
            "reserved": snapshot.reserved,
            "cost": snapshot.cost,
            "realized": snapshot.realized,
            "unrealized": snapshot.unrealized,
            "nav": snapshot.nav,
            "net_pnl": snapshot.net_pnl,
            "loss_latched": snapshot.loss_latched,
            "fees_in_cash": s.fees,
            "holdings": s.holdings,
        })),
        None,
        state.paper.as_ref().and_then(|paper| paper.phase1.as_ref()),
    );
}

pub(crate) fn emit_final(broker: &Broker, cleanup_timed_out: bool) {
    let state = broker.state.lock();
    let stop_reason = state.entry_stop_reason.map(|reason| reason.as_str());
    let Some(paper) = state.paper.as_ref() else {
        if broker.controlled.is_some() {
            emit_event(
                broker.controlled.as_ref(),
                "final",
                None,
                None,
                Some(
                    serde_json::json!({"cash": null, "available": null, "reserved": null,
                    "cost": null, "realized": null, "unrealized": null, "nav": null,
                    "net_pnl": null, "fees_in_cash": null, "loss_latched": null}),
                ),
                Some(serde_json::json!({"markets": null, "finalized": false,
                    "accounting_reason": "LIVE_ACCOUNTING_UNAVAILABLE",
                    "stop_reason": stop_reason, "cleanup_timed_out": cleanup_timed_out})),
                None,
            );
        }
        return;
    };
    let mut markets: Vec<serde_json::Value> = paper.traded.iter().map(|(id, original)| {
        let status = match paper.settlements.get(id).unwrap_or(&Settlement::Pending) {
            Settlement::Pending => "PENDING", Settlement::Confirmed => "CONFIRMED", Settlement::Conflict => "CONFLICT",
        };
        let holdings: Vec<serde_json::Value> = state.holdings.values().filter(|holding| holding.market_id == *id)
            .map(|holding| serde_json::json!({"token": holding.token, "shares": holding.shares, "cost": holding.cost,
                "mark": paper.marks.get(&holding.token).and_then(|mark| mark.price),
                "mark_valid": paper.marks.get(&holding.token).is_some_and(|mark| mark.valid),
                "mark_time": paper.marks.get(&holding.token).and_then(|mark| mark.observed_at).map(|at| at.to_rfc3339())})).collect();
        serde_json::json!({"market_id": id.to_string(), "status": status,
            "yes_token": original.yes_token, "no_token": original.no_token, "holdings": holdings,
            "payout_yes": paper.applied_results.get(id).map(|result| result.2)})
    }).collect();
    markets.sort_by_key(|market| market["market_id"].as_str().unwrap_or_default().to_string());
    emit_event(
        broker.controlled.as_ref(),
        "final",
        None,
        None,
        Some(paper_accounting(paper, &state)),
        Some(
            serde_json::json!({"markets": markets, "stop_reason": stop_reason,
                "cleanup_timed_out": cleanup_timed_out, "finalized": paper.reservation.is_none()
                    && paper.pending_candidate.is_none()
                    && paper.phase1.as_ref().is_none_or(|run| !run.output_failed.load(Ordering::SeqCst))
                    && paper.traded.keys().all(|id| paper.settlements.get(id) == Some(&Settlement::Confirmed))}),
        ),
        paper.phase1.as_ref(),
    );
}

#[cfg(test)]
mod tests;
