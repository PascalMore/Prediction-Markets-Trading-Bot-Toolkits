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
use std::collections::HashMap;
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
    pub starts_at: Option<DateTime<Utc>>,
    pub ends_at: Option<DateTime<Utc>>,
    pub identity_available_at: Option<DateTime<Utc>>,
    pub strike_available_at: Option<DateTime<Utc>>,
    pub strike: Option<f64>,
    pub spot: Option<f64>,
    pub spot_clock: Option<ObservationClock>,
    pub book_clock: Option<ObservationClock>,
    pub decision_at: DateTime<Utc>,
    pub decision_elapsed_ms: u64,
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
    /// The book levels the candidate was built on.
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
}

/// The inputs one evaluation works from.
pub struct EvaluationInputs<'a> {
    pub market: &'a TradableMarket,
    pub book: &'a BookSnapshot,
    pub spot_price: f64,
    pub spot_source: SpotSource,
    pub actual_pair: String,
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
struct SymbolState {
    key: Option<SourceKey>,
    epoch: u64,
    reset_reason: ResetReason,
    vol: EwmaVol,
    last_receive: Option<(DateTime<Utc>, u64)>,
}

/// The phase1 state of the shared decision core.
pub struct Phase1Core {
    contract: StrategyContract,
    symbols: HashMap<String, SymbolState>,
    /// The identity of the current inputs. It changes whenever an input is
    /// refreshed, so a candidate can tell that its frozen inputs are stale.
    revision: Arc<AtomicU64>,
    next_decision_id: u64,
}

impl Phase1Core {
    pub fn new(contract: StrategyContract) -> Self {
        Self {
            contract,
            symbols: HashMap::new(),
            revision: Arc::new(AtomicU64::new(0)),
            next_decision_id: 0,
        }
    }

    pub fn contract(&self) -> StrategyContract {
        self.contract
    }

    /// The current revision of the inputs.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::SeqCst)
    }

    /// The samples of a symbol's current epoch.
    pub fn samples(&self, symbol: &str) -> u32 {
        self.symbols
            .get(symbol)
            .map_or(0, |state| state.vol.samples())
    }

    /// The volatility of a symbol's current epoch.
    pub fn sigma(&self, symbol: &str, floor: f64) -> Option<f64> {
        self.symbols.get(symbol).map(|state| state.vol.vol(floor))
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
    pub fn observe(
        &mut self,
        symbol: &str,
        price: f64,
        source: SpotSource,
        actual_pair: &str,
        clock: &ObservationClock,
        now: CheckTime,
    ) -> Result<(), Reason> {
        if !price.is_finite() || price <= 0.0 {
            return Err(Reason::InputInvalid);
        }
        let key = SourceKey {
            symbol: symbol.to_string(),
            source,
            actual_pair: actual_pair.to_string(),
        };
        let state = self.symbols.entry(symbol.to_string()).or_insert_with(|| SymbolState {
            key: None,
            epoch: 0,
            reset_reason: ResetReason::None,
            vol: EwmaVol::new(VOL_HALF_LIFE_SECS),
            last_receive: None,
        });
        let broken_clock = !clock.is_consistent()
            || clock.received_at > now.utc
            || clock.received_elapsed_ms > now.elapsed_ms
            || state
                .last_receive
                .is_some_and(|(at, elapsed)| {
                    clock.received_at < at || clock.received_elapsed_ms < elapsed
                });
        if broken_clock {
            Self::reset(state, ResetReason::ClockInvalid);
            self.revision.fetch_add(1, Ordering::SeqCst);
            return Err(Reason::ClockInvalid);
        }
        let switched = state.key.as_ref().is_some_and(|current| current != &key);
        if switched {
            Self::reset(state, ResetReason::SourceSwitch);
        } else if state.key.is_none() {
            state.epoch = 1;
            state.reset_reason = ResetReason::FirstSample;
        } else if let Some((_, elapsed)) = state.last_receive {
            if clock.received_elapsed_ms.saturating_sub(elapsed) > GAP_RESET_MS {
                Self::reset(state, ResetReason::LongGap);
            }
        }
        state.key = Some(key);
        state
            .vol
            .update(price, clock.received_elapsed_ms as f64 / 1000.0);
        state.last_receive = Some((clock.received_at, clock.received_elapsed_ms));
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Start a new epoch and drop the estimate that belonged to the old one.
    fn reset(state: &mut SymbolState, reason: ResetReason) {
        state.epoch = state.epoch.saturating_add(1).max(1);
        state.reset_reason = reason;
        state.vol = EwmaVol::new(VOL_HALF_LIFE_SECS);
        state.last_receive = None;
    }

    /// Value one market and decide whether it is worth buying.
    ///
    /// The checks run in the frozen order: the first failure is the reason,
    /// and every value that could be computed safely is kept.
    pub fn evaluate(&mut self, inputs: &EvaluationInputs<'_>) -> Phase1Evaluation {
        self.next_decision_id += 1;
        let decision_id = self.next_decision_id.to_string();
        let mut diagnostic = DecisionDiagnostic {
            decision_id: decision_id.clone(),
            candidate_id: None,
            phase: Phase::Evaluate,
            strategy_contract: self.contract,
            reason: Reason::Eligible,
            symbol: inputs
                .market
                .feed_symbol
                .clone()
                .unwrap_or_else(|| "BTCUSDT".to_string()),
            prediction_source: Some(inputs.spot_source),
            actual_pair: Some(inputs.actual_pair.clone()),
            source_state: source_state(inputs.spot_source),
            source_epoch: self.epoch(&diagnostic_symbol(inputs)),
            reset_reason: self.reset_reason(&diagnostic_symbol(inputs)),
            settlement_provider: inputs.settlement_provider,
            starts_at: inputs.market.starts_at,
            ends_at: inputs.market.ends_at,
            identity_available_at: inputs.identity_available_at,
            strike_available_at: inputs.strike_available_at,
            strike: inputs.market.start_price,
            spot: None,
            spot_clock: inputs.spot_clock.clone(),
            book_clock: inputs.book_clock.clone(),
            decision_at: inputs.now.utc,
            decision_elapsed_ms: inputs.now.elapsed_ms,
            checked_at: inputs.now.utc,
            checked_elapsed_ms: inputs.now.elapsed_ms,
            secs_left: None,
            samples: 0,
            sigma: None,
            p_up: None,
            best_yes_bid: inputs.book.best_bid().map(|level| level.price),
            best_yes_ask: inputs.book.best_ask().map(|level| level.price),
            fee_rate_bps: Some(inputs.market.fee_rate_bps),
            selected_token: None,
            up: None,
            down: None,
        };
        let symbol = diagnostic.symbol.clone();
        diagnostic.samples = self.samples(&symbol);
        diagnostic.source_epoch = self.epoch(&symbol);
        diagnostic.reset_reason = self.reset_reason(&symbol);

        let market = inputs.market;
        let Some(strike) = market.start_price.filter(|price| price.is_finite() && *price > 0.0)
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
        if !market.open || market.contested {
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
        if inputs.entered {
            diagnostic.reason = Reason::AlreadyEntered;
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
        if !spot_clock.is_consistent() || !book_clock.is_consistent() {
            diagnostic.reason = Reason::ClockInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if spot_clock.received_at > inputs.now.utc || book_clock.received_at > inputs.now.utc {
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
        if spot_clock.acquisition_age_ms(inputs.now) > MAX_ACQUISITION_AGE_MS {
            diagnostic.reason = Reason::SpotStale;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if book_clock.acquisition_age_ms(inputs.now) > MAX_ACQUISITION_AGE_MS {
            diagnostic.reason = Reason::BookStale;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if !inputs.spot_price.is_finite() || inputs.spot_price <= 0.0 {
            diagnostic.reason = Reason::InputInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        if market.fee_rate_bps > 10_000 {
            diagnostic.reason = Reason::InputInvalid;
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
        if diagnostic.samples < MIN_VOL_SAMPLES {
            diagnostic.reason = Reason::Warmup;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        let sigma = self.sigma(&symbol, self.floor(inputs.min_edge)).unwrap_or(0.0);
        if !sigma.is_finite() || sigma <= 0.0 {
            diagnostic.reason = Reason::Warmup;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        diagnostic.sigma = Some(sigma);
        let p_up = digital_up(inputs.spot_price, strike, sigma, secs_left);
        if !p_up.is_finite() || !(0.0..=1.0).contains(&p_up) {
            diagnostic.reason = Reason::InputInvalid;
            return Phase1Evaluation {
                diagnostic,
                candidate: None,
            };
        }
        diagnostic.p_up = Some(p_up);

        let up = self.side(
            inputs,
            &diagnostic,
            Side::Buy,
            p_up,
            strike,
            sigma,
            secs_left,
        );
        let down = self.side(
            inputs,
            &diagnostic,
            Side::Sell,
            1.0 - p_up,
            strike,
            sigma,
            secs_left,
        );
        diagnostic.up = up.clone();
        diagnostic.down = down.clone();

        let chosen = match (up, down) {
            (Some(up), Some(down)) => {
                if down.edge >= up.edge {
                    Some(down)
                } else {
                    Some(up)
                }
            }
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
                source: inputs.spot_source,
                actual_pair: inputs.actual_pair.clone(),
            },
            epoch: diagnostic.source_epoch.unwrap_or(0),
            settlement_provider: inputs.settlement_provider,
            identity_available_at: inputs.identity_available_at,
            strike_available_at: inputs.strike_available_at,
            starts_at: Some(starts_at),
            revision: self.revision(),
            revision_counter: Arc::clone(&self.revision),
            book_levels: inputs
                .book
                .asks
                .iter()
                .chain(inputs.book.bids.iter())
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
    fn side(
        &self,
        inputs: &EvaluationInputs<'_>,
        diagnostic: &DecisionDiagnostic,
        side: Side,
        fair: f64,
        strike: f64,
        sigma: f64,
        secs_left: f64,
    ) -> Option<SideDiagnostic> {
        let market = inputs.market;
        let token = match side {
            Side::Buy => market.yes_token.clone(),
            Side::Sell => market.no_token.clone(),
        };
        let mut out = SideDiagnostic::empty(token.clone());
        let book = token_book(market, &token, inputs.book);
        let limit = match side {
            Side::Buy => book.best_ask().map(|level| level.price),
            Side::Sell => book.best_ask().map(|level| level.price),
        };
        let Some(limit) = limit.filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0)
        else {
            return Some(out);
        };
        out.limit_price = Some(limit);
        out.fair = Some(fair);
        let shares = shares_for(inputs.order_usd, limit);
        if shares <= 0.0 {
            return Some(out);
        }
        out.gross_shares = Some(shares);
        let fee_per_gross = fee_per_gross_share(limit, market.fee_rate_bps);
        out.fee_per_gross_share = Some(fee_per_gross);
        let Some((net, _fee)) = net_shares(shares, limit, market.fee_rate_bps) else {
            return Some(out);
        };
        out.net_shares = Some(net);
        let cost = shares * limit;
        out.cost = Some(cost);
        let ev = net * fair - cost;
        out.ev = Some(ev);
        let edge = ev / shares;
        out.edge = Some(edge);
        out.roi = if cost > 0.0 { Some(ev / cost) } else { None };
        out.breakeven_fair = {
            let ratio = net / shares;
            if ratio > 0.0 {
                Some(limit / ratio)
            } else {
                None
            }
        };
        out.qualifies = Some(edge >= inputs.min_edge);
        let levels: Vec<BookLevel> = match side {
            Side::Buy => book.asks.clone(),
            Side::Sell => book.asks.clone(),
        };
        let (prefix, complete) = depth_prefix(&levels, shares, limit);
        out.depth_levels = Some(prefix.iter().map(|level| Level { price: level.price, size: level.size }).collect());
        out.depth_complete = Some(complete);
        if complete {
            if let Some(vwap) = walk(&levels, Side::Buy, shares, limit) {
                out.walk_vwap = Some(vwap);
                if let Some((exec_net, _)) = net_shares(shares, vwap, market.fee_rate_bps) {
                    let exec_cost = shares * vwap;
                    let exec_ev = exec_net * fair - exec_cost;
                    out.execution_net_shares = Some(exec_net);
                    out.execution_cost = Some(exec_cost);
                    out.execution_ev = Some(exec_ev);
                    out.execution_edge = Some(exec_ev / shares);
                }
            }
        }
        let _ = (diagnostic, strike, sigma, secs_left);
        Some(out)
    }
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
fn source_state(source: SpotSource) -> SourceState {
    match source {
        SpotSource::Binance => SourceState::Primary,
        SpotSource::Coinbase => SourceState::Degraded,
    }
}

/// The symbol of a diagnostic, before the market is known.
fn diagnostic_symbol(inputs: &EvaluationInputs<'_>) -> String {
    inputs
        .market
        .feed_symbol
        .clone()
        .unwrap_or_else(|| "BTCUSDT".to_string())
}

/// Recompute a candidate's value at a later moment.
///
/// The inputs stay frozen: only the clock moves, so the remaining time and the
/// model's value are recomputed, never remembered.
pub fn recheck(
    candidate: &CandidateContext,
    now: CheckTime,
    min_edge: f64,
    min_secs_left: f64,
) -> Result<Recheck, Reason> {
    if !candidate.inputs_unchanged() {
        return Err(Reason::InputChanged);
    }
    if now.elapsed_ms < candidate.decision_elapsed_ms || now.utc < candidate.decision_at {
        return Err(Reason::ClockInvalid);
    }
    // A clock that went backwards must not extend the round: the elapsed time
    // decides how much of the round is left.
    let effective = if now.utc > candidate.decision_at {
        now.utc
    } else {
        candidate.decision_at
    };
    let secs_left = (candidate.ends_at - effective).num_milliseconds() as f64 / 1000.0;
    if secs_left < min_secs_left {
        return Err(Reason::RoundTooLate);
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
    if edge < min_edge {
        return Err(Reason::EdgeBelowMin);
    }
    Ok(Recheck {
        fair,
        secs_left,
        net_shares: net,
        cost,
        ev,
        edge,
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
    let mut diagnostic = candidate.diagnostic.clone().unwrap_or_else(|| {
        DecisionDiagnostic {
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
        }
    });
    diagnostic.phase = phase;
    diagnostic.checked_at = now.utc;
    diagnostic.checked_elapsed_ms = now.elapsed_ms;
    diagnostic.selected_token = Some(candidate.token.clone());
    match recheck(candidate, now, min_edge, min_secs_left) {
        Ok(result) => {
            diagnostic.reason = Reason::Eligible;
            diagnostic.secs_left = Some(result.secs_left);
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
                side.qualifies = Some(true);
            }
            (diagnostic, Some(result))
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

#[cfg(test)]
mod tests;

