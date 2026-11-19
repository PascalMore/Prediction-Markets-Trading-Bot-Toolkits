//! Phase 1: explicit net-share economics, isolated source epochs and real clocks.
//!
//! The legacy core valued a round against a cash fee and remembered one
//! volatility estimate per symbol. Phase 1 keeps the same model — the same
//! lognormal, the same EWMA, the same `digital_up` — and changes three things
//! around it:
//!
//! - the entry edge is computed on the shares that actually arrive, so the fee
//!   is paid once, in shares, and never twice;
//! - the volatility state belongs to one source at a time, and a source change
//!   or a long gap starts a new epoch that warms up again;
//! - every input carries the clock that received it, so a stale price cannot be
//!   mistaken for a fresh one.
//!
//! Nothing here duplicates the model: [`EwmaVol`] and [`digital_up`] are the
//! originals, and the net-share fee is the broker's own helper.

use super::{MIN_VOL_SAMPLES, VOL_HALF_LIFE_SECS};
use crate::bot::runtime::paper::net_shares;
use crate::bot::runtime::{shares_for, token_book, walk};
use crate::models::{BookLevel, BookSnapshot, Side};
use crate::service::observation::{CheckTime, ObservationClock};
use crate::service::spot::{digital_up, EwmaVol, SpotSource};
use crate::service::venue::TradableMarket;
use chrono::{DateTime, Utc};
use serde::Serialize;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The economics a decision is made under.
///
/// The two are kept apart on purpose: the legacy cash fee stays the default,
/// and only the controlled net-share run opts into the other one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyContract {
    LegacyCashFee,
    Phase1NetShares,
}

/// The phase that produced a diagnostic snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    Evaluate,
    PreSubmit,
    PreApply,
    /// Live-only recheck after all preparation and durable evidence waits.
    FinalSend,
}

/// Why a decision attempt did not pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Reason {
    RunStopped,
    AlreadyEntered,
    MarketInvalid,
    MissingInput,
    InputInvalid,
    ClockInvalid,
    RequestTooSlow,
    SpotStale,
    BookStale,
    RoundTooLate,
    InputChanged,
    Warmup,
    NetSharesInvalid,
    EdgeBelowMin,
    DiagnosticLimit,
    BrokerBlocked,
    ContextMissing,
    Eligible,
}

/// How the prediction source is serving this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceState {
    Primary,
    Degraded,
    Unavailable,
}

/// Why the current epoch started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResetReason {
    None,
    FirstSample,
    SourceSwitch,
    LongGap,
    ClockInvalid,
}

/// The official settlement provider, as the venue's own facts state it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementProvider {
    Chainlink,
    Unknown,
}

/// The identity of the feed a price came from.
///
/// The pair is part of the identity: a USDT pair and a USD pair are not the
/// same price, and their basis is not a rounding error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceKey {
    pub symbol: String,
    pub source: SpotSource,
    pub actual_pair: String,
}

/// The longest gap between two valid receipts that still counts as continuous.
pub const GAP_RESET_MS: u64 = 300_000;
/// The longest a request may take.
pub const MAX_REQUEST_SPAN_MS: i64 = 5_000;
/// The oldest an input may be.
pub const MAX_ACQUISITION_AGE_MS: i64 = 10_000;

/// One book level, as the diagnostic keeps it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// What one side of a round is worth.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SideDiagnostic {
    pub token: String,
    pub fair: Option<f64>,
    pub limit_price: Option<f64>,
    pub gross_shares: Option<f64>,
    pub fee_per_gross_share: Option<f64>,
    pub net_shares: Option<f64>,
    pub cost: Option<f64>,
    pub ev: Option<f64>,
    pub edge: Option<f64>,
    pub roi: Option<f64>,
    pub breakeven_fair: Option<f64>,
    pub qualifies: Option<bool>,
    pub depth_levels: Option<Vec<Level>>,
    pub depth_complete: Option<bool>,
    pub walk_vwap: Option<f64>,
    pub execution_net_shares: Option<f64>,
    pub execution_cost: Option<f64>,
    pub execution_ev: Option<f64>,
    pub execution_edge: Option<f64>,
}

impl SideDiagnostic {
    fn empty(token: String) -> Self {
        Self {
            token,
            fair: None,
            limit_price: None,
            gross_shares: None,
            fee_per_gross_share: None,
            net_shares: None,
            cost: None,
            ev: None,
            edge: None,
            roi: None,
            breakeven_fair: None,
            qualifies: None,
            depth_levels: None,
            depth_complete: None,
            walk_vwap: None,
            execution_net_shares: None,
            execution_cost: None,
            execution_ev: None,
            execution_edge: None,
        }
    }
}

/// The snapshot of one decision attempt.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecisionDiagnostic {
    pub decision_id: String,
    pub candidate_id: Option<String>,
    pub phase: Phase,
    pub strategy_contract: StrategyContract,
    pub reason: Reason,
    pub symbol: String,
    pub prediction_source: Option<SpotSource>,
    pub actual_pair: Option<String>,
    pub source_state: SourceState,
    pub source_epoch: Option<u64>,
    pub reset_reason: ResetReason,
    pub settlement_provider: SettlementProvider,
    #[serde(serialize_with = "crate::service::observation::serialize_millis_option")]
    pub starts_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "crate::service::observation::serialize_millis_option")]
    pub ends_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "crate::service::observation::serialize_millis_option")]
    pub identity_available_at: Option<DateTime<Utc>>,
    #[serde(serialize_with = "crate::service::observation::serialize_millis_option")]
    pub strike_available_at: Option<DateTime<Utc>>,
    pub strike: Option<f64>,
    pub spot: Option<f64>,
    pub spot_clock: Option<ObservationClock>,
    pub book_clock: Option<ObservationClock>,
    #[serde(serialize_with = "crate::service::observation::serialize_millis")]
    pub decision_at: DateTime<Utc>,
    pub decision_elapsed_ms: u64,
    #[serde(serialize_with = "crate::service::observation::serialize_millis")]
    pub checked_at: DateTime<Utc>,
    pub checked_elapsed_ms: u64,
    pub secs_left: Option<f64>,
    pub samples: u32,
    pub sigma: Option<f64>,
    pub p_up: Option<f64>,
    pub best_yes_bid: Option<f64>,
    pub best_yes_ask: Option<f64>,
    pub fee_rate_bps: Option<u32>,
    pub selected_token: Option<String>,
    pub up: Option<SideDiagnostic>,
    pub down: Option<SideDiagnostic>,
}

/// The frozen inputs of one candidate.
///
/// A candidate is built by the core and never changes afterwards: a later
/// check recomputes from these values and the new clock, it never swaps them.
#[derive(Debug, Clone)]
pub struct CandidateContext {
    decision_id: String,
    candidate_id: String,
    market_id: u64,
    token: String,
    side: Side,
    limit_price: f64,
    gross_shares: f64,
    fee_rate_bps: u32,
    strike: f64,
    spot: f64,
    sigma: f64,
    samples: u32,
    fair_up: f64,
    ends_at: DateTime<Utc>,
    decision_at: DateTime<Utc>,
    decision_elapsed_ms: u64,
    spot_clock: ObservationClock,
    book_clock: ObservationClock,
    source: SourceKey,
    epoch: u64,
    settlement_provider: SettlementProvider,
    identity_available_at: Option<DateTime<Utc>>,
    strike_available_at: Option<DateTime<Utc>>,
    starts_at: Option<DateTime<Utc>>,
    /// The revision of the inputs this candidate froze.
    revision: u64,
    /// The shared counter the inputs' revision lives in.
    revision_counter: Arc<AtomicU64>,
    market_revision: u64,
    market_revision_counter: Arc<AtomicU64>,
    frozen_market: TradableMarket,
    frozen_book: BookSnapshot,
    /// The book levels the candidate was built on: the asks of the side it
    /// selected, in the order the walk consumes them.
    book_levels: Vec<(f64, f64)>,
    /// The evaluation that produced this candidate.
    diagnostic: Option<DecisionDiagnostic>,
}

impl CandidateContext {
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }

    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    pub fn market_id(&self) -> u64 {
        self.market_id
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn limit_price(&self) -> f64 {
        self.limit_price
    }

    pub fn gross_shares(&self) -> f64 {
        self.gross_shares
    }

    pub fn fee_rate_bps(&self) -> u32 {
        self.fee_rate_bps
    }

    pub fn ends_at(&self) -> DateTime<Utc> {
        self.ends_at
    }

    /// The official fixed strike this candidate was built on.
    pub fn strike(&self) -> f64 {
        self.strike
    }

    pub fn source(&self) -> &SourceKey {
        &self.source
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn settlement_provider(&self) -> SettlementProvider {
        self.settlement_provider
    }

    pub fn identity_available_at(&self) -> Option<DateTime<Utc>> {
        self.identity_available_at
    }

    pub fn strike_available_at(&self) -> Option<DateTime<Utc>> {
        self.strike_available_at
    }

    pub fn starts_at(&self) -> Option<DateTime<Utc>> {
        self.starts_at
    }

    pub fn spot_clock(&self) -> &ObservationClock {
        &self.spot_clock
    }

    /// The evaluation that produced this candidate.
    pub fn diagnostic(&self) -> Option<&DecisionDiagnostic> {
        self.diagnostic.as_ref()
    }

    pub fn book_clock(&self) -> &ObservationClock {
        &self.book_clock
    }

    pub fn decision_at(&self) -> DateTime<Utc> {
        self.decision_at
    }

    pub fn decision_elapsed_ms(&self) -> u64 {
        self.decision_elapsed_ms
    }

    /// Whether the inputs this candidate froze are still the current ones.
    pub fn inputs_unchanged(&self) -> bool {
        self.revision_counter.load(Ordering::SeqCst) == self.revision
            && self.revision < u64::MAX
            && self.market_revision_counter.load(Ordering::SeqCst) == self.market_revision
            && self.market_revision < u64::MAX
    }

    /// Test-only: move the inputs' revision on, as a refreshed market would.
    #[cfg(test)]
    pub(crate) fn invalidate_revision(&self) {
        self.revision_counter.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn same_candidate(&self, other: &Self) -> bool {
        self.candidate_id == other.candidate_id
            && Arc::ptr_eq(&self.revision_counter, &other.revision_counter)
            && Arc::ptr_eq(
                &self.market_revision_counter,
                &other.market_revision_counter,
            )
    }

    /// Compare the actual Broker inputs, not merely the selected touch.
    pub fn matches_inputs(&self, market: &TradableMarket, book: &BookSnapshot) -> bool {
        let frozen = &self.frozen_market;
        frozen.id == market.id
            && frozen.yes_token == market.yes_token
            && frozen.no_token == market.no_token
            && frozen.feed_symbol == market.feed_symbol
            && frozen.starts_at == market.starts_at
            && frozen.ends_at == market.ends_at
            && frozen.start_price == market.start_price
            && frozen.fee_rate_bps == market.fee_rate_bps
            && frozen.tick == market.tick
            && frozen.open == market.open
            && frozen.contested == market.contested
            && frozen.round_facts == market.round_facts
            && self
                .frozen_book
                .bids
                .iter()
                .map(|level| (level.price, level.size))
                .eq(book.bids.iter().map(|level| (level.price, level.size)))
            && self
                .frozen_book
                .asks
                .iter()
                .map(|level| (level.price, level.size))
                .eq(book.asks.iter().map(|level| (level.price, level.size)))
    }

    /// The immutable Yes-quoted book, for the adapter's exact preparation check.
    pub(crate) fn frozen_book(&self) -> &BookSnapshot {
        &self.frozen_book
    }

    /// The value of the side this candidate selected.
    pub fn fair(&self) -> f64 {
        match self.side {
            Side::Buy => self.fair_up,
            Side::Sell => 1.0 - self.fair_up,
        }
    }
}

/// The result of one evaluation.
#[derive(Debug, Clone)]
pub struct Phase1Evaluation {
    pub diagnostic: DecisionDiagnostic,
    pub candidate: Option<CandidateContext>,
}

/// The recomputed values of a candidate at a later moment.
#[derive(Debug, Clone, Copy)]
pub struct Recheck {
    pub fair: f64,
    pub secs_left: f64,
    pub net_shares: f64,
    pub cost: f64,
    pub ev: f64,
    pub edge: f64,
    /// The price the frozen book would actually fill at.
    pub walk_vwap: Option<f64>,
    /// The net shares that price leaves, after the fee.
    pub execution_net_shares: Option<f64>,
    /// The cost at that price.
    pub execution_cost: Option<f64>,
    /// The expected value at that price.
    pub execution_ev: Option<f64>,
    /// The edge per gross share at that price.
    pub execution_edge: Option<f64>,
}

/// The inputs one evaluation works from.
pub struct EvaluationInputs<'a> {
    pub market: &'a TradableMarket,
    pub book: &'a BookSnapshot,
    pub spot_price: f64,
    /// The feed that answered, when there was one.
    pub spot_source: Option<SpotSource>,
    /// The pair that was actually requested, when there was one.
    pub actual_pair: Option<String>,
    pub spot_clock: Option<ObservationClock>,
    pub book_clock: Option<ObservationClock>,
    pub now: CheckTime,
    pub order_usd: f64,
    pub min_edge: f64,
    pub min_secs_left: f64,
    pub entered: bool,
    pub identity_available_at: Option<DateTime<Utc>>,
    pub strike_available_at: Option<DateTime<Utc>>,
    pub settlement_provider: SettlementProvider,
}

/// One symbol's source state.
pub(super) struct SymbolState {
    key: Option<SourceKey>,
    epoch: u64,
    reset_reason: ResetReason,
    vol: EwmaVol,
    last_receive: Option<(DateTime<Utc>, u64)>,
    last_source: Option<DateTime<Utc>>,
    revision: Arc<AtomicU64>,
}

/// Pure model output; no book, fees, candidate or order state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Phase1ModelValue {
    pub samples: u32,
    pub sigma_raw: f64,
    pub sigma: f64,
    pub p_up: f64,
}

/// The read-only state of one symbol's current epoch.
///
/// The observer needs to know whether the quote it is looking at is the one
/// the core accepted, and whether that epoch is warm. This is a copy of facts
/// the core already holds: it observes nothing, refreshes nothing and
/// changes nothing.
#[derive(Debug, Clone)]
pub struct Phase1ObservationState {
    /// The source the current epoch belongs to.
    pub key: Option<SourceKey>,
    /// The receipt of the last accepted observation.
    pub last_receive: Option<(DateTime<Utc>, u64)>,
    pub last_source: Option<DateTime<Utc>>,
    pub last_check: Option<CheckTime>,
    pub floor: f64,
    /// The current epoch, when the symbol has one.
    pub epoch: Option<u64>,
    /// Why the current epoch started.
    pub reset_reason: ResetReason,
    /// The returns the current epoch holds.
    pub samples: u32,
    /// The volatility before the floor.
    pub sigma_raw: f64,
    /// The volatility after the floor.
    pub sigma: f64,
    /// The revision of the current inputs.
    pub revision: u64,
}

/// The phase1 state of the shared decision core.
///
/// It lives beside the legacy volatility state: the core keeps one contract,
/// one set of bought markets and one revision identity. This state adds the
/// source epochs and the input revisions, and nothing else.
impl super::DecisionCore {
    /// Explicitly opt the original decision core into net-share semantics.
    pub(crate) fn phase1(cfg: crate::config::BtcArbConfig) -> Self {
        let mut core = Self::new(cfg);
        core.contract = StrategyContract::Phase1NetShares;
        core
    }

    pub fn contract(&self) -> StrategyContract {
        self.contract
    }

    /// The current revision of the inputs.
    /// Invalidate this market before delivering refreshed facts or a book.
    pub fn refresh_phase1_market(&mut self, market_id: u64) -> Result<(), Reason> {
        bump(
            self.market_revisions
                .entry(market_id)
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        )
    }

    /// The samples of a symbol's current epoch.
    pub fn samples(&self, symbol: &str) -> u32 {
        self.symbols
            .get(symbol)
            .map_or(0, |state| state.vol.samples())
    }

    /// The volatility of a symbol's current epoch.
    pub fn phase1_sigma(&self, symbol: &str, floor: f64) -> Option<f64> {
        self.symbols.get(symbol).map(|state| state.vol.vol(floor))
    }

    /// Value the active phase1 estimate without touching trading state.
    pub(crate) fn phase1_valuation(
        &self, symbol: &str, spot: f64, strike: f64, secs_left: f64, floor: f64,
    ) -> Result<Phase1ModelValue, Reason> {
        let samples = self.samples(symbol);
        if samples < MIN_VOL_SAMPLES {
            return Err(Reason::Warmup);
        }
        let sigma = self.phase1_sigma(symbol, floor).unwrap_or(0.0);
        if !sigma.is_finite() || sigma <= 0.0 {
            return Err(Reason::Warmup);
        }
        let p_up = digital_up(spot, strike, sigma, secs_left);
        if !p_up.is_finite() || !(0.0..=1.0).contains(&p_up) {
            return Err(Reason::InputInvalid);
        }
        Ok(Phase1ModelValue { samples,
            sigma_raw: self.phase1_sigma(symbol, 0.0).unwrap_or(0.0), sigma, p_up })
    }

    /// The current epoch's state of `symbol`, read without touching it.
    pub fn phase1_observation_state(&self, symbol: &str) -> Phase1ObservationState {
        let state = self.symbols.get(symbol);
        Phase1ObservationState {
            key: state.and_then(|state| state.key.clone()),
            last_receive: state.and_then(|state| state.last_receive),
            last_source: state.and_then(|state| state.last_source),
            last_check: self.last_phase1_check,
            floor: self.floor(0.0),
            epoch: state.map(|state| state.epoch),
            reset_reason: state.map_or(ResetReason::None, |state| state.reset_reason),
            samples: state.map_or(0, |state| state.vol.samples()),
            sigma_raw: state.map_or(0.0, |state| state.vol.vol(0.0)),
            sigma: state.map_or(0.0, |state| state.vol.vol(self.floor(0.0))),
            revision: state.map_or(0, |state| state.revision.load(Ordering::SeqCst)),
        }
    }

    /// The epoch of a symbol.
    pub fn epoch(&self, symbol: &str) -> Option<u64> {
        self.symbols.get(symbol).map(|state| state.epoch)
    }

    /// The reason the current epoch started.
    pub fn reset_reason(&self, symbol: &str) -> ResetReason {
        self.symbols
            .get(symbol)
            .map_or(ResetReason::None, |state| state.reset_reason)
    }

    /// Record one spot observation.
    ///
    /// A price that is not a price changes nothing. A clock that is broken
    /// starts a new epoch and drops the observation: a broken clock cannot
    /// extend an estimate.
    pub fn observe_phase1(
        &mut self,
        symbol: &str,
        price: f64,
        source: SpotSource,
        actual_pair: &str,
        clock: &ObservationClock,
        now: CheckTime,
    ) -> Result<(), Reason> {
        if !price.is_finite()
            || price <= 0.0
            || symbol != "BTCUSDT"
            || !valid_source(Some(source), Some(actual_pair))
        {
            return Err(Reason::InputInvalid);
        }
        let key = SourceKey {
            symbol: symbol.to_string(),
            source,
            actual_pair: actual_pair.to_string(),
        };
        let state = self
            .symbols
            .entry(symbol.to_string())
            .or_insert_with(|| SymbolState {
                key: None,
                epoch: 0,
                reset_reason: ResetReason::None,
                vol: EwmaVol::new(VOL_HALF_LIFE_SECS),
                last_receive: None,
                last_source: None,
                revision: Arc::new(AtomicU64::new(0)),
            });
        bump(&state.revision)?;
        let switched = state.key.as_ref().is_some_and(|current| current != &key);
        let broken_clock = !clock.is_consistent()
            || now.elapsed_ms > i64::MAX as u64
            || now.utc.timestamp_millis() < 0
            || clock.received_at > now.utc
            || clock.received_elapsed_ms > now.elapsed_ms
            || state.last_receive.is_some_and(|(at, elapsed)| {
                clock.received_at <= at || clock.received_elapsed_ms <= elapsed
            })
            || (!switched
                && clock
                    .source_at
                    .zip(state.last_source)
                    .is_some_and(|(new, old)| new <= old));
        if broken_clock {
            Self::reset(state, ResetReason::ClockInvalid);
            return Err(Reason::ClockInvalid);
        }
        if clock.request_span_ms() > MAX_REQUEST_SPAN_MS {
            return Err(Reason::RequestTooSlow);
        }
        if clock.acquisition_age_ms(now) > MAX_ACQUISITION_AGE_MS
            || clock
                .source_at
                .is_some_and(|at| (now.utc - at).num_milliseconds() > MAX_ACQUISITION_AGE_MS)
        {
            return Err(Reason::SpotStale);
        }
        if switched {
            Self::reset(state, ResetReason::SourceSwitch);
        } else if state.key.is_none() {
            state.epoch = 1;
            state.reset_reason = ResetReason::FirstSample;
        } else if let Some((at, elapsed)) = state.last_receive {
            // The gap is the slower of the two clocks: a wall clock that froze
            // while the process kept running must not look continuous.
            let wall = clock.received_at.timestamp_millis() - at.timestamp_millis();
            let elapsed_gap = clock.received_elapsed_ms as i64 - elapsed as i64;
            if wall.max(elapsed_gap) > GAP_RESET_MS as i64 {
                Self::reset(state, ResetReason::LongGap);
            }
        }
        state.key = Some(key);
        state
            .vol
            .update(price, clock.received_elapsed_ms as f64 / 1000.0);
        state.last_receive = Some((clock.received_at, clock.received_elapsed_ms));
        state.last_source = clock.source_at;
        Ok(())
    }

    /// Move the inputs' revision on, and fail closed if it cannot.
    ///
    /// The revision is the identity of the current inputs: a counter that
    /// wrapped would make a stale candidate look current again.

    /// Start a new epoch and drop the estimate that belonged to the old one.
    fn reset(state: &mut SymbolState, reason: ResetReason) {
        state.epoch = state.epoch.saturating_add(1).max(1);
        state.reset_reason = reason;
        state.vol = EwmaVol::new(VOL_HALF_LIFE_SECS);
        state.last_receive = None;
        state.last_source = None;
    }

    /// Value one market and decide whether it is worth buying.
    ///
    /// The checks run in the frozen order: the first failure is the reason,
    /// and every value that could be computed safely is kept.
    pub fn evaluate_phase1(&mut self, inputs: &EvaluationInputs<'_>) -> Phase1Evaluation {
        let rollback = self.last_phase1_check.is_some_and(|previous| {
            inputs.now.utc < previous.utc || inputs.now.elapsed_ms < previous.elapsed_ms
        });
        if rollback {
            for state in self.symbols.values_mut() {
                Self::reset(state, ResetReason::ClockInvalid);
                let _ = bump(&state.revision); // An exhausted revision remains poisoned.
            }
        }
        self.last_phase1_check = Some(inputs.now);
        self.next_decision_id += 1;
        let decision_id = self.next_decision_id.to_string();
        let symbol = inputs
            .market
            .feed_symbol
            .clone()
            .unwrap_or_else(|| "BTCUSDT".to_string());
        let mut diagnostic = DecisionDiagnostic {
            decision_id: decision_id.clone(),
            candidate_id: None,
            phase: Phase::Evaluate,
            strategy_contract: self.contract,
            reason: Reason::Eligible,
            symbol: symbol.clone(),
            prediction_source: inputs.spot_source,
            actual_pair: inputs.actual_pair.clone(),
            source_state: match inputs.spot_source {
                Some(source) => source_state(source),
                None => SourceState::Unavailable,
            },
            source_epoch: self.epoch(&symbol),
            reset_reason: self.reset_reason(&symbol),
            settlement_provider: inputs.settlement_provider,
            starts_at: inputs.market.starts_at,
            ends_at: inputs.market.ends_at,
            identity_available_at: inputs.identity_available_at,
            strike_available_at: inputs.strike_available_at,
            strike: inputs
                .market
                .start_price
                .filter(|price| price.is_finite() && *price > 0.0),
            spot: (inputs.spot_price.is_finite() && inputs.spot_price > 0.0)
                .then_some(inputs.spot_price),
            spot_clock: inputs.spot_clock.clone(),
            book_clock: inputs.book_clock.clone(),
            decision_at: inputs.now.utc,
            decision_elapsed_ms: inputs.now.elapsed_ms,
            checked_at: inputs.now.utc,
            checked_elapsed_ms: inputs.now.elapsed_ms,
            secs_left: inputs
                .market
                .ends_at
                .map(|end| (end - inputs.now.utc).num_milliseconds() as f64 / 1000.0),
            samples: 0,
            sigma: None,
            p_up: None,
            best_yes_bid: inputs
                .book
                .best_bid()
                .map(|level| level.price)
                .filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0),
            best_yes_ask: inputs
                .book
                .best_ask()
                .map(|level| level.price)
                .filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0),
            fee_rate_bps: (inputs.market.fee_rate_bps <= 10_000)
                .then_some(inputs.market.fee_rate_bps),
            selected_token: None,
            up: None,
            down: None,
        };
        diagnostic.samples = self.samples(&symbol);
        diagnostic.source_epoch = self.epoch(&symbol);
        diagnostic.reset_reason = self.reset_reason(&symbol);
        for (side, slot, token) in [
            (Side::Buy, &mut diagnostic.up, &inputs.market.yes_token),
            (Side::Sell, &mut diagnostic.down, &inputs.market.no_token),
        ] {
            if !token.is_empty() {
                *slot = Some(SideDiagnostic::empty(token.clone()));
                if inputs.book_clock.is_some()
                    && crate::bot::runtime::paper::mid_of(inputs.book).is_some()
                    && inputs.market.fee_rate_bps <= 10_000
                {
                    *slot = self.side(inputs, side, None).diagnostic;
                }
            }
        }
        // The old single-entry protection comes before the market's own
        // facts: a round this run already bought is not a new opportunity,
        // whatever its identity says.
        if inputs.entered {
            diagnostic.reason = Reason::AlreadyEntered;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }

        let market = inputs.market;
        // The round's own facts come first: without an identity, a period and
        // a strike there is nothing to value, and no clock can repair that.
        let Some(strike) = market
            .start_price
            .filter(|price| price.is_finite() && *price > 0.0)
        else {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        let Some(ends_at) = market.ends_at else {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        let Some(starts_at) = market.starts_at else {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        if !market.open
            || market.contested
            || market.feed_symbol.as_deref() != Some("BTCUSDT")
            || ends_at - starts_at != chrono::Duration::seconds(300)
            || market.yes_token == market.no_token
            || market.yes_token.is_empty()
            || market.no_token.is_empty()
        {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if starts_at > inputs.now.utc {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        // The official identity and the fixed strike must have been available
        // locally before the decision: a fact that arrived afterwards cannot
        // be the basis of this decision.
        if inputs
            .identity_available_at
            .is_some_and(|at| at > inputs.now.utc)
            || inputs
                .strike_available_at
                .is_some_and(|at| at > inputs.now.utc)
        {
            diagnostic.reason = Reason::MarketInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        // A missing input comes before an invalid one: a price that was never
        // received cannot also be a price that is out of range.
        if inputs.spot_clock.is_none() || inputs.book_clock.is_none() {
            diagnostic.reason = Reason::MissingInput;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        let Some(spot_clock) = inputs.spot_clock.as_ref() else {
            diagnostic.reason = Reason::MissingInput;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        let Some(book_clock) = inputs.book_clock.as_ref() else {
            diagnostic.reason = Reason::MissingInput;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        // A value that is not a value is an invalid input, and it is checked
        // before the clocks: a bad price is not a clock fault.
        if !inputs.spot_price.is_finite() || inputs.spot_price <= 0.0 {
            diagnostic.reason = Reason::InputInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if market.fee_rate_bps > 10_000
            || !valid_source(inputs.spot_source, inputs.actual_pair.as_deref())
            || crate::bot::runtime::paper::mid_of(inputs.book).is_none()
        {
            diagnostic.reason = Reason::InputInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if rollback
            || !spot_clock.is_consistent()
            || !book_clock.is_consistent()
            || self.symbols.get(&symbol).is_some_and(|state| {
                state.reset_reason == ResetReason::ClockInvalid && state.last_receive.is_none()
            })
        {
            diagnostic.reason = Reason::ClockInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if spot_clock.received_at > inputs.now.utc
            || book_clock.received_at > inputs.now.utc
            || spot_clock.received_elapsed_ms > inputs.now.elapsed_ms
            || book_clock.received_elapsed_ms > inputs.now.elapsed_ms
        {
            diagnostic.reason = Reason::ClockInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if spot_clock.request_span_ms() > MAX_REQUEST_SPAN_MS
            || book_clock.request_span_ms() > MAX_REQUEST_SPAN_MS
        {
            diagnostic.reason = Reason::RequestTooSlow;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if spot_clock.acquisition_age_ms(inputs.now) > MAX_ACQUISITION_AGE_MS
            || spot_clock.source_at.is_some_and(|source| {
                inputs.now.utc.timestamp_millis() - source.timestamp_millis()
                    > MAX_ACQUISITION_AGE_MS
            })
        {
            diagnostic.reason = Reason::SpotStale;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if book_clock.acquisition_age_ms(inputs.now) > MAX_ACQUISITION_AGE_MS
            || book_clock.source_at.is_some_and(|source| {
                inputs.now.utc.timestamp_millis() - source.timestamp_millis()
                    > MAX_ACQUISITION_AGE_MS
            })
        {
            diagnostic.reason = Reason::BookStale;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }

        let secs_left = (ends_at - inputs.now.utc).num_milliseconds() as f64 / 1000.0;
        diagnostic.secs_left = Some(secs_left);
        if secs_left < inputs.min_secs_left {
            diagnostic.reason = Reason::RoundTooLate;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        diagnostic.spot = Some(inputs.spot_price);
        if self.symbols.get(&symbol).is_some_and(|state| {
            state.key.as_ref().is_some_and(|key| {
                Some(key.source) != inputs.spot_source
                    || Some(&key.actual_pair) != inputs.actual_pair.as_ref()
            }) || state.last_receive
                != Some((spot_clock.received_at, spot_clock.received_elapsed_ms))
        }) {
            diagnostic.reason = Reason::InputChanged;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        let model = match self.phase1_valuation(
            &symbol, inputs.spot_price, strike, secs_left, self.floor(inputs.min_edge),
        ) {
            Ok(value) => value,
            Err(reason) => {
                // Preserve the old diagnostic, including sigma on invalid p.
                if reason == Reason::InputInvalid {
                    diagnostic.sigma = self.phase1_sigma(&symbol, self.floor(inputs.min_edge));
                }
                diagnostic.reason = reason;
                return Phase1Evaluation { diagnostic, candidate: None };
            }
        };
        let p_up = model.p_up;
        let sigma = model.sigma;
        diagnostic.sigma = Some(sigma);
        diagnostic.p_up = Some(p_up);

        let up = self.side(inputs, Side::Buy, Some(p_up));
        let down = self.side(inputs, Side::Sell, Some(1.0 - p_up));
        diagnostic.up = up.diagnostic.clone();
        diagnostic.down = down.diagnostic.clone();
        // A side that cannot be valued at all is not a low edge: it is an
        // invalid input, and it outranks the edge comparison.
        if let Some(reason) = up.reason.or(down.reason) {
            diagnostic.reason = reason;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }

        let chosen = match (up.diagnostic, down.diagnostic) {
            (Some(up), Some(down)) => Some(choose_side(&up, &down).clone()),
            (Some(up), None) => Some(up),
            (None, Some(down)) => Some(down),
            (None, None) => None,
        };
        let Some(chosen) = chosen else {
            diagnostic.reason = Reason::EdgeBelowMin;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        };
        if chosen.qualifies != Some(true) {
            diagnostic.reason = Reason::EdgeBelowMin;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        diagnostic.reason = Reason::Eligible;
        diagnostic.selected_token = Some(chosen.token.clone());
        let candidate = CandidateContext {
            decision_id: decision_id.clone(),
            candidate_id: decision_id.clone(),
            market_id: market.id,
            token: chosen.token.clone(),
            side: if chosen.token == market.no_token {
                Side::Sell
            } else {
                Side::Buy
            },
            limit_price: chosen.limit_price.unwrap_or_default(),
            gross_shares: chosen.gross_shares.unwrap_or_default(),
            fee_rate_bps: market.fee_rate_bps,
            strike,
            spot: inputs.spot_price,
            sigma,
            samples: diagnostic.samples,
            fair_up: p_up,
            ends_at,
            decision_at: inputs.now.utc,
            decision_elapsed_ms: inputs.now.elapsed_ms,
            spot_clock: spot_clock.clone(),
            book_clock: book_clock.clone(),
            source: SourceKey {
                symbol: symbol.clone(),
                source: inputs.spot_source.unwrap_or(SpotSource::Binance),
                actual_pair: inputs.actual_pair.clone().unwrap_or_else(|| symbol.clone()),
            },
            epoch: diagnostic.source_epoch.unwrap_or(0),
            settlement_provider: inputs.settlement_provider,
            identity_available_at: inputs.identity_available_at,
            strike_available_at: inputs.strike_available_at,
            starts_at: Some(starts_at),
            revision: self.symbols[&symbol].revision.load(Ordering::SeqCst),
            revision_counter: Arc::clone(&self.symbols[&symbol].revision),
            market_revision: self
                .market_revisions
                .entry(market.id)
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .load(Ordering::SeqCst),
            market_revision_counter: Arc::clone(&self.market_revisions[&market.id]),
            frozen_market: market.clone(),
            frozen_book: inputs.book.clone(),
            book_levels: token_book(market, &chosen.token, inputs.book)
                .asks
                .iter()
                .map(|level| (level.price, level.size))
                .collect(),
            diagnostic: None,
        };
        diagnostic.candidate_id = Some(candidate.candidate_id.clone());
        let mut candidate = candidate;
        candidate.diagnostic = Some(diagnostic.clone());
        Phase1Evaluation {
            diagnostic,
            candidate: Some(candidate),
        }
    }

    /// The floor of the volatility estimate. It is fixed by the contract.
    fn floor(&self, _min_edge: f64) -> f64 {
        VOL_FLOOR
    }

    /// Value one side of the round on the net-share economics.
    ///
    /// The result says two things: what the side is worth, and — when it
    /// cannot be valued at all — why. A side that has no usable price is an
    /// invalid input, not a side with a zero edge.
    fn side(&self, inputs: &EvaluationInputs<'_>, side: Side, fair: Option<f64>) -> SideValue {
        let market = inputs.market;
        let token = match side {
            Side::Buy => market.yes_token.clone(),
            Side::Sell => market.no_token.clone(),
        };
        let mut out = SideDiagnostic::empty(token.clone());
        let book = token_book(market, &token, inputs.book);
        let Some(limit) = book
            .best_ask()
            .map(|level| level.price)
            .filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0)
        else {
            return SideValue {
                diagnostic: Some(out),
                reason: Some(Reason::InputInvalid),
            };
        };
        out.limit_price = Some(limit);
        out.fair = fair;
        let shares = shares_for(inputs.order_usd, limit);
        if shares <= 0.0 {
            return SideValue {
                diagnostic: Some(out),
                reason: Some(Reason::InputInvalid),
            };
        }
        out.gross_shares = Some(shares);
        let fee_per_gross = fee_per_gross_share(limit, market.fee_rate_bps);
        out.fee_per_gross_share = Some(fee_per_gross);
        let Some((net, _fee)) = net_shares(shares, limit, market.fee_rate_bps) else {
            return SideValue {
                diagnostic: Some(out),
                reason: Some(Reason::InputInvalid),
            };
        };
        // A price that leaves nothing to pay out is not a trade: the fee would
        // eat the whole share.
        if net <= 0.0 {
            return SideValue {
                diagnostic: Some(out),
                reason: Some(Reason::NetSharesInvalid),
            };
        }
        out.net_shares = Some(net);
        let cost = shares * limit;
        out.cost = Some(cost);
        out.ev = fair.map(|fair| net * fair - cost);
        out.edge = out.ev.map(|ev| ev / shares);
        out.roi = out.ev.map(|ev| ev / cost);
        out.breakeven_fair = {
            let ratio = net / shares;
            if ratio > 0.0 {
                Some(limit / ratio)
            } else {
                None
            }
        };
        out.qualifies = out.edge.map(|edge| edge >= inputs.min_edge);
        let levels: Vec<BookLevel> = book.asks.clone();
        let (prefix, complete) = depth_prefix(&levels, shares, limit);
        out.depth_levels = Some(
            prefix
                .iter()
                .map(|level| Level {
                    price: level.price,
                    size: level.size,
                })
                .collect(),
        );
        out.depth_complete = Some(complete);
        if complete {
            if let Some(vwap) = walk(&levels, Side::Buy, shares, limit) {
                out.walk_vwap = Some(vwap);
                if let Some((exec_net, _)) = net_shares(shares, vwap, market.fee_rate_bps) {
                    let exec_cost = shares * vwap;
                    let exec_ev = fair.map(|fair| exec_net * fair - exec_cost);
                    out.execution_net_shares = Some(exec_net);
                    out.execution_cost = Some(exec_cost);
                    out.execution_ev = exec_ev;
                    out.execution_edge = exec_ev.map(|ev| ev / shares);
                }
            }
        }
        SideValue {
            diagnostic: Some(out),
            reason: None,
        }
    }
}

/// What one side of a round is worth, and why it may be worth nothing.
struct SideValue {
    diagnostic: Option<SideDiagnostic>,
    /// The reason the side cannot be valued, if it cannot.
    reason: Option<Reason>,
}

/// The side the round is worth buying.
///
/// The larger edge wins. An exact tie keeps the old iteration order, which is
/// the Down side: the choice is the production one, not a second rule.
fn choose_side<'a>(up: &'a SideDiagnostic, down: &'a SideDiagnostic) -> &'a SideDiagnostic {
    if down.edge >= up.edge {
        down
    } else {
        up
    }
}

fn bump(counter: &AtomicU64) -> Result<(), Reason> {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_add(1)
        })
        .map(|_| ())
        .map_err(|_| Reason::InputChanged)
}

/// The volatility floor, fixed by the contract.
pub const VOL_FLOOR: f64 = 0.00005;

/// The fee a BUY pays per gross share, in cash terms.
pub fn fee_per_gross_share(price: f64, fee_rate_bps: u32) -> f64 {
    f64::from(fee_rate_bps) / 10_000.0 * price.min(1.0 - price).max(0.0)
}

/// The shortest prefix of `levels` that covers `shares` within `limit`.
fn depth_prefix(levels: &[BookLevel], shares: f64, limit: f64) -> (Vec<BookLevel>, bool) {
    let mut remaining = shares;
    let mut out = Vec::new();
    for level in levels {
        if level.price > limit + 1e-9 || remaining <= 1e-9 {
            break;
        }
        out.push(*level);
        remaining -= level.size.min(remaining);
        if remaining <= 1e-9 {
            return (out, true);
        }
    }
    (out, remaining <= 1e-9)
}

/// Whether the source is the primary one or a degraded substitute.
fn valid_source(source: Option<SpotSource>, pair: Option<&str>) -> bool {
    matches!(
        (source, pair),
        (Some(SpotSource::Binance), Some("BTCUSDT"))
            | (Some(SpotSource::Coinbase), Some("BTC-USD"))
    )
}

fn source_state(source: SpotSource) -> SourceState {
    match source {
        SpotSource::Binance => SourceState::Primary,
        SpotSource::Coinbase => SourceState::Degraded,
    }
}

/// Recompute a candidate's value at a later moment.
///
/// The inputs stay frozen: only the clock moves, so the remaining time and the
/// model's value are recomputed, never remembered.
///
/// The effective now is the later of the wall clock and the decision moment
/// plus the elapsed time since it. A clock that jumped backwards cannot
/// extend the round: the elapsed time still says how much of it is left.
pub fn recheck(
    candidate: &CandidateContext,
    now: CheckTime,
    min_edge: f64,
    min_secs_left: f64,
) -> Result<Recheck, Reason> {
    let result = compute_recheck(candidate, now, min_secs_left)?;
    if result.edge < min_edge || result.execution_edge.is_some_and(|edge| edge < min_edge) {
        return Err(Reason::EdgeBelowMin);
    }
    Ok(result)
}

fn compute_recheck(
    candidate: &CandidateContext,
    now: CheckTime,
    min_secs_left: f64,
) -> Result<Recheck, Reason> {
    if now.elapsed_ms < candidate.decision_elapsed_ms || now.utc < candidate.decision_at {
        return Err(Reason::ClockInvalid);
    }
    // The elapsed time decides how much of the round is left: a wall clock
    // that went backwards must not extend it.
    let elapsed = now.elapsed_ms.saturating_sub(candidate.decision_elapsed_ms) as i64;
    let by_elapsed = candidate.decision_at + chrono::Duration::milliseconds(elapsed);
    let effective = if now.utc > by_elapsed {
        now.utc
    } else {
        by_elapsed
    };
    let secs_left = (candidate.ends_at - effective).num_milliseconds() as f64 / 1000.0;

    // The frozen inputs must still be fresh enough to act on. A price that
    // was already old when the decision was made is refused here too: the
    // check is about the input, not about when it was first seen.
    if candidate.spot_clock.request_span_ms() > MAX_REQUEST_SPAN_MS
        || candidate.book_clock.request_span_ms() > MAX_REQUEST_SPAN_MS
    {
        return Err(Reason::RequestTooSlow);
    }
    if candidate.spot_clock.acquisition_age_ms(now) > MAX_ACQUISITION_AGE_MS
        || candidate.spot_clock.source_at.is_some_and(|source| {
            now.utc.timestamp_millis() - source.timestamp_millis() > MAX_ACQUISITION_AGE_MS
        })
    {
        return Err(Reason::SpotStale);
    }
    if candidate.book_clock.acquisition_age_ms(now) > MAX_ACQUISITION_AGE_MS
        || candidate.book_clock.source_at.is_some_and(|source| {
            now.utc.timestamp_millis() - source.timestamp_millis() > MAX_ACQUISITION_AGE_MS
        })
    {
        return Err(Reason::BookStale);
    }
    if secs_left < min_secs_left {
        return Err(Reason::RoundTooLate);
    }
    if !candidate.inputs_unchanged() {
        return Err(Reason::InputChanged);
    }
    let fair_up = digital_up(candidate.spot, candidate.strike, candidate.sigma, secs_left);
    let fair = match candidate.side {
        Side::Buy => fair_up,
        Side::Sell => 1.0 - fair_up,
    };
    let limit = candidate.limit_price;
    let shares = candidate.gross_shares;
    let Some((net, _)) = net_shares(shares, limit, candidate.fee_rate_bps) else {
        return Err(Reason::NetSharesInvalid);
    };
    if net <= 0.0 {
        return Err(Reason::NetSharesInvalid);
    }
    let cost = shares * limit;
    let ev = net * fair - cost;
    let edge = ev / shares;

    // The price the frozen book would really fill at is the price that has to
    // clear the threshold: a touch that looks good is not a trade.
    let levels: Vec<BookLevel> = candidate
        .book_levels
        .iter()
        .map(|(price, size)| BookLevel {
            price: *price,
            size: *size,
        })
        .collect();
    let walk_vwap = walk(&levels, Side::Buy, shares, limit);
    let mut execution_net_shares = None;
    let mut execution_cost = None;
    let mut execution_ev = None;
    let mut execution_edge = None;
    if let Some(vwap) = walk_vwap {
        if let Some((exec_net, _)) = net_shares(shares, vwap, candidate.fee_rate_bps) {
            let exec_cost = shares * vwap;
            let exec_ev = exec_net * fair - exec_cost;
            let exec_edge = exec_ev / shares;
            execution_net_shares = Some(exec_net);
            execution_cost = Some(exec_cost);
            execution_ev = Some(exec_ev);
            execution_edge = Some(exec_edge);
        }
    }
    Ok(Recheck {
        fair,
        secs_left,
        net_shares: net,
        cost,
        ev,
        edge,
        walk_vwap,
        execution_net_shares,
        execution_cost,
        execution_ev,
        execution_edge,
    })
}

/// The snapshot of a recheck, with the recomputed values of the selected side.
///
/// A failed recheck keeps the values that could be computed safely and carries
/// the reason: it never fills a missing number with zero.
pub fn recheck_diagnostic(
    candidate: &CandidateContext,
    now: CheckTime,
    phase: Phase,
    min_edge: f64,
    min_secs_left: f64,
) -> (DecisionDiagnostic, Option<Recheck>) {
    let mut diagnostic = candidate
        .diagnostic
        .clone()
        .unwrap_or_else(|| DecisionDiagnostic {
            decision_id: candidate.decision_id.clone(),
            candidate_id: Some(candidate.candidate_id.clone()),
            phase,
            strategy_contract: StrategyContract::Phase1NetShares,
            reason: Reason::Eligible,
            symbol: candidate.source.symbol.clone(),
            prediction_source: Some(candidate.source.source),
            actual_pair: Some(candidate.source.actual_pair.clone()),
            source_state: SourceState::Primary,
            source_epoch: Some(candidate.epoch),
            reset_reason: ResetReason::None,
            settlement_provider: candidate.settlement_provider,
            starts_at: candidate.starts_at,
            ends_at: Some(candidate.ends_at),
            identity_available_at: candidate.identity_available_at,
            strike_available_at: candidate.strike_available_at,
            strike: Some(candidate.strike),
            spot: Some(candidate.spot),
            spot_clock: Some(candidate.spot_clock.clone()),
            book_clock: Some(candidate.book_clock.clone()),
            decision_at: candidate.decision_at,
            decision_elapsed_ms: candidate.decision_elapsed_ms,
            checked_at: now.utc,
            checked_elapsed_ms: now.elapsed_ms,
            secs_left: None,
            samples: candidate.samples,
            sigma: Some(candidate.sigma),
            p_up: Some(candidate.fair_up),
            best_yes_bid: None,
            best_yes_ask: None,
            fee_rate_bps: Some(candidate.fee_rate_bps),
            selected_token: Some(candidate.token.clone()),
            up: None,
            down: None,
        });
    diagnostic.phase = phase;
    diagnostic.checked_at = now.utc;
    diagnostic.checked_elapsed_ms = now.elapsed_ms;
    diagnostic.selected_token = Some(candidate.token.clone());
    match compute_recheck(candidate, now, min_secs_left) {
        Ok(result) => {
            let qualifies = result.edge >= min_edge
                && result.execution_edge.is_none_or(|edge| edge >= min_edge);
            diagnostic.reason = if qualifies {
                Reason::Eligible
            } else {
                Reason::EdgeBelowMin
            };
            diagnostic.secs_left = Some(result.secs_left);
            let p_up = if candidate.side == Side::Buy {
                result.fair
            } else {
                1.0 - result.fair
            };
            diagnostic.p_up = Some(p_up);
            for (side, fair) in [
                (&mut diagnostic.up, p_up),
                (&mut diagnostic.down, 1.0 - p_up),
            ] {
                if let Some(side) = side {
                    side.fair = Some(fair);
                    side.ev = side
                        .net_shares
                        .zip(side.cost)
                        .map(|(net, cost)| net * fair - cost);
                    side.edge = side.ev.zip(side.gross_shares).map(|(ev, q)| ev / q);
                    side.roi = side.ev.zip(side.cost).map(|(ev, cost)| ev / cost);
                    side.qualifies = side.edge.map(|edge| edge >= min_edge);
                    side.execution_ev = side
                        .execution_net_shares
                        .zip(side.execution_cost)
                        .map(|(net, cost)| net * fair - cost);
                    side.execution_edge = side
                        .execution_ev
                        .zip(side.gross_shares)
                        .map(|(ev, q)| ev / q);
                }
            }
            let side = if candidate.side == Side::Buy {
                &mut diagnostic.up
            } else {
                &mut diagnostic.down
            };
            if let Some(side) = side {
                side.fair = Some(result.fair);
                side.net_shares = Some(result.net_shares);
                side.cost = Some(result.cost);
                side.ev = Some(result.ev);
                side.edge = Some(result.edge);
                side.walk_vwap = result.walk_vwap;
                side.execution_net_shares = result.execution_net_shares;
                side.execution_cost = result.execution_cost;
                side.execution_ev = result.execution_ev;
                side.execution_edge = result.execution_edge;
                side.qualifies = Some(result.edge >= min_edge);
            }
            (diagnostic, qualifies.then_some(result))
        }
        Err(reason) => {
            diagnostic.reason = reason;
            (diagnostic, None)
        }
    }
}

/// Whether the clock of an input is too old for a decision at `now`.
pub fn input_age_ms(clock: &ObservationClock, now: CheckTime) -> i64 {
    clock.acquisition_age_ms(now)
}

/// Whether every number in a diagnostic can be represented on the wire.
///
/// A value that is not finite has no place in the protocol: it would be
/// written as a missing value or as a lie, and both are worse than a refusal.
pub fn diagnostic_is_finite(diagnostic: &DecisionDiagnostic) -> bool {
    let finite = |value: &Option<f64>| value.is_none_or(|number| number.is_finite());
    finite(&diagnostic.strike)
        && finite(&diagnostic.spot)
        && finite(&diagnostic.sigma)
        && finite(&diagnostic.p_up)
        && finite(&diagnostic.best_yes_bid)
        && finite(&diagnostic.best_yes_ask)
        && finite(&diagnostic.secs_left)
        && diagnostic.up.as_ref().is_none_or(side_is_finite)
        && diagnostic.down.as_ref().is_none_or(side_is_finite)
}

/// Whether every number of one side can be represented on the wire.
fn side_is_finite(side: &SideDiagnostic) -> bool {
    let finite = |value: &Option<f64>| value.is_none_or(|number| number.is_finite());
    finite(&side.fair)
        && finite(&side.limit_price)
        && finite(&side.gross_shares)
        && finite(&side.fee_per_gross_share)
        && finite(&side.net_shares)
        && finite(&side.cost)
        && finite(&side.ev)
        && finite(&side.edge)
        && finite(&side.roi)
        && finite(&side.breakeven_fair)
        && finite(&side.walk_vwap)
        && finite(&side.execution_net_shares)
        && finite(&side.execution_cost)
        && finite(&side.execution_ev)
        && finite(&side.execution_edge)
        && side.depth_levels.as_ref().is_none_or(|levels| {
            levels
                .iter()
                .all(|level| level.price.is_finite() && level.size.is_finite())
        })
}

#[cfg(test)]
mod tests;
