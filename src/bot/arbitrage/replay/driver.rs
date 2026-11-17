//! The state machine that walks a replay or proxy event stream.
//!
//! The driver owns the behaviour: it validates each envelope, keeps the
//! market and spot state, calls the shared decision core and produces one
//! output row per event. It has no file, process or network I/O: the facade
//! owns the stream.

use super::super::phase1::{DecisionDiagnostic, EvaluationInputs, Reason, SettlementProvider};
use super::super::{DecisionCore, MIN_VOL_SAMPLES};
use super::types::*;
use crate::models::{BookLevel, BookSnapshot};
use crate::service::observation::{CheckTime, ObservationClock};
use crate::service::spot::SpotSource;
use crate::service::venue::TradableMarket;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap, HashSet};

const MISSING_EVIDENCE_REASONS: [&str; 8] = [
    "state_unknown",
    "start_price_unknown",
    "fee_unknown",
    "spot_unknown",
    "warmup",
    "book_unknown",
    "result_unknown",
    "candidate_unconfirmed",
];
// ------------------------------------------------------------------ state ---

/// The identity and state of one market, as known at the current time.
#[derive(Debug, Clone)]
struct MarketState {
    market_id: u64,
    symbol: String,
    starts_at_ms: i64,
    ends_at_ms: i64,
    yes_token: String,
    no_token: String,
    yes_name: String,
    no_name: String,
    rules_source: String,
    open: Option<bool>,
    contested: Option<bool>,
    paused: Option<bool>,
    start_price: Option<f64>,
    fee_rate_bps: Option<u32>,
    book: Option<BookSnapshot>,
    last_book: Option<(i64, BookPayload)>,
    /// The clock of the book currently held, when the version carries one.
    book_clock: Option<ObservationClock>,
    /// The candidate waiting for a result, if any.
    pending_candidate: Option<String>,
    /// The candidates that were replaced before they were answered.
    terminated_candidates: HashSet<String>,
    /// Set once a result was unknown: the market stops offering entries.
    unknown_result: bool,
    /// When the market's identity first became locally available.
    identity_available_at: Option<i64>,
    /// When the market's fixed strike first became locally available.
    strike_available_at: Option<i64>,
    fixed_strike_conflict: bool,
}

/// The last spot seen for a symbol.
#[derive(Debug, Clone)]
struct SpotState {
    price: f64,
    at_ms: i64,
    /// The source and the clock of the observation, when the version carries
    /// them.
    observed: Option<ObservedSpotState>,
}

/// The source and clock of one spot observation.
#[derive(Debug, Clone)]
struct ObservedSpotState {
    source: SpotSource,
    actual_pair: String,
    clock: ObservationClock,
}
/// A failure inside the driver, before it becomes a [`ReplayError`].
#[derive(Debug)]
pub(crate) struct DriverFailure {
    pub(super) exit_code: u8,
    pub(super) code: String,
    pub(super) message: String,
}

impl DriverFailure {
    fn new(exit_code: u8, code: &str, message: String) -> Self {
        Self {
            exit_code,
            code: code.to_string(),
            message,
        }
    }
}

/// One row of output.
///
/// The original version writes the row alone. The net-share version writes
/// the same row and the decision snapshot that produced it.
pub(crate) struct DriverRow {
    pub base: OutputRow,
    pub decision: Option<DecisionDiagnostic>,
}

/// The state machine that walks the stream.
pub(crate) struct Driver {
    mode: ReplayMode,
    version: ReplayVersion,
    core: DecisionCore,
    config: ReplayConfig,
    markets: BTreeMap<u64, MarketState>,
    spots: HashMap<String, SpotState>,
    seen: HashMap<String, StreamEvent>,
    last_key: Option<(i64, u64)>,
    /// The last elapsed clock seen, so it can never go backwards.
    last_elapsed: Option<u64>,
    saw_official_label: bool,
    saw_real_market: bool,
    stream_evidence: Option<EvidenceType>,
    // Tests pass the actual evaluated candidate to the existing Broker.
    #[cfg(test)]
    pub(crate) fixture_candidate: Option<super::super::phase1::CandidateContext>,
    // The frozen wire has no venue tick precision. Execution tests explicitly
    // inject only official rules; never replace wire prices or market state.
    #[cfg(test)]
    pub(crate) fixture_round_facts: Option<crate::service::venue::RoundFacts>,
}

impl Driver {
    pub(crate) fn new(config: ReplayConfig, mode: ReplayMode) -> Self {
        let version = config.version().unwrap_or(ReplayVersion::V1);
        // The net-share version runs the same core under the other economics.
        let core = match version {
            ReplayVersion::V1 => DecisionCore::with_warmup(
                config.strategy_config(),
                config.half_life_secs,
                config.min_vol_samples,
            ),
            ReplayVersion::V2 => DecisionCore::phase1(config.strategy_config()),
        };
        Self {
            mode,
            version,
            core,
            config,
            markets: BTreeMap::new(),
            spots: HashMap::new(),
            seen: HashMap::new(),
            last_key: None,
            last_elapsed: None,
            saw_official_label: false,
            saw_real_market: false,
            stream_evidence: None,
            #[cfg(test)]
            fixture_candidate: None,
            #[cfg(test)]
            fixture_round_facts: None,
        }
    }

    /// Process one event.
    pub(crate) fn step(
        &mut self,
        event: StreamEvent,
        _line: u64,
        summary: &mut ReplaySummary,
    ) -> Result<Option<DriverRow>, DriverFailure> {
        if self.version == ReplayVersion::V1 {
            self.validate_envelope(&event)?;
        }
        // A repeat of an event already seen is ignored; a repeat that says
        // something different is a conflict.
        if let Some(previous) = self.seen.get(&event.event_id) {
            if *previous == event {
                summary.duplicate_count += 1;
                return Ok(None);
            }
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "event {}: repeated id with a different payload",
                    event.event_id
                ),
            ));
        }

        // A repeated old observation must not trip the current elapsed clock
        // or reset an epoch. Conflicting identities above still fail closed.
        if self.version == ReplayVersion::V2 {
            self.validate_envelope(&event)?;
        }
        let key = (event.available_at_ms, event.seq);
        if let Some(last) = self.last_key {
            if key <= last {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "time_order",
                    format!(
                        "event {}: available_at_ms/seq must strictly increase",
                        event.event_id
                    ),
                ));
            }
        }
        self.last_key = Some(key);
        self.last_elapsed = Some(event.elapsed_ms);
        self.stream_evidence = Some(event.evidence_type);
        self.seen.insert(event.event_id.clone(), event.clone());

        self.dispatch(event, summary)
    }

    /// Check the envelope's shape, before anything is remembered about it.
    fn validate_envelope(&self, event: &StreamEvent) -> Result<(), DriverFailure> {
        let expected = match self.version {
            ReplayVersion::V1 => SCHEMA_VERSION,
            ReplayVersion::V2 => SCHEMA_VERSION_V2,
        };
        if event.schema_version != expected {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: schema_version must be {expected}",
                    event.event_id
                ),
            ));
        }
        if event.event_id.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                "event_id must not be empty".to_string(),
            ));
        }
        if event.source.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!("event {}: source must not be empty", event.event_id),
            ));
        }
        if DateTime::<Utc>::from_timestamp_millis(event.available_at_ms).is_none() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                format!(
                    "event {}: available_at_ms is not a UTC instant",
                    event.event_id
                ),
            ));
        }
        if let Some(event_at) = event.event_at_ms {
            if DateTime::<Utc>::from_timestamp_millis(event_at).is_none() {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!("event {}: event_at_ms is not a UTC instant", event.event_id),
                ));
            }
            if event_at > event.available_at_ms {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "time_order",
                    format!(
                        "event {}: event_at_ms is after available_at_ms",
                        event.event_id
                    ),
                ));
            }
        }
        // The elapsed clock is the process's own: it never goes backwards.
        if let Some(last) = self.last_elapsed {
            if event.elapsed_ms < last {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "time_order",
                    format!("event {}: elapsed_ms went backwards", event.event_id),
                ));
            }
        }
        // Evidence is a stream identity, not a per-row permission to share state.
        if matches!(
            (self.mode, event.evidence_type),
            (ReplayMode::Proxy, EvidenceType::RealMarket)
                | (ReplayMode::Replay, EvidenceType::RealProxy)
        ) {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "event {}: evidence is not allowed in this mode",
                    event.event_id
                ),
            ));
        }
        if self
            .stream_evidence
            .is_some_and(|kind| kind != event.evidence_type)
        {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "event {}: evidence cannot change within a stream",
                    event.event_id
                ),
            ));
        }
        match (self.mode, event.payload.event_type()) {
            (ReplayMode::Proxy, EventType::Spot) | (ReplayMode::Proxy, EventType::Value) => Ok(()),
            (ReplayMode::Proxy, other) => Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: {} is not available in proxy mode",
                    event.event_id,
                    other.as_str()
                ),
            )),
            (ReplayMode::Replay, EventType::Value) => Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: value is not available in replay mode",
                    event.event_id
                ),
            )),
            (ReplayMode::Replay, _) => Ok(()),
        }
    }

    /// Apply one event and produce its row.
    fn dispatch(
        &mut self,
        event: StreamEvent,
        summary: &mut ReplaySummary,
    ) -> Result<Option<DriverRow>, DriverFailure> {
        let event_type = event.payload.event_type();
        let schema_version = match self.version {
            ReplayVersion::V1 => SCHEMA_VERSION,
            ReplayVersion::V2 => SCHEMA_VERSION_V2,
        };
        let mut row = OutputRow {
            schema_version: schema_version.to_string(),
            event_id: event.event_id.clone(),
            event_at_ms: event.event_at_ms,
            available_at_ms: event.available_at_ms,
            seq: event.seq,
            evidence_type: event.evidence_type,
            source: event.source.clone(),
            event_type,
            market_id: None,
            yes_token: None,
            no_token: None,
            symbol: None,
            samples: None,
            sigma: None,
            p_up: None,
            candidate: None,
            token: None,
            price: None,
            fair: None,
            edge: None,
            result_status: None,
            filled_shares: None,
            entered_before: None,
            entered_after: None,
            reason: "observed".to_string(),
        };
        let mut decision = None;
        if event.evidence_type == EvidenceType::RealMarket {
            self.saw_real_market = true;
        }
        match event.payload {
            StreamPayload::Market(payload) => self.apply_market(&payload, &mut row)?,
            StreamPayload::State(payload) => self.apply_state(&payload, &mut row)?,
            StreamPayload::Spot(payload) => self.apply_spot(
                &payload,
                event.available_at_ms,
                event.elapsed_ms,
                &mut row,
                summary,
            )?,
            StreamPayload::Book(payload) => {
                if self.apply_book(&payload, event.available_at_ms, event.elapsed_ms, &mut row)? {
                    summary.duplicate_count += 1;
                    return Ok(None);
                }
            }
            StreamPayload::Tick(payload) => {
                decision = self.apply_tick(
                    &payload,
                    event.available_at_ms,
                    event.elapsed_ms,
                    &mut row,
                    summary,
                )?;
            }
            StreamPayload::Value(payload) => {
                self.apply_value(&payload, event.available_at_ms, &mut row, summary)?
            }
            StreamPayload::Result(payload) => {
                self.apply_result(&payload, &mut row, summary)?;
            }
            StreamPayload::Label(payload) => {
                self.apply_label(&payload, event.available_at_ms, &mut row)?
            }
        }
        Ok(Some(DriverRow {
            base: row,
            decision,
        }))
    }

    /// Establish or re-state a market's identity.
    fn apply_market(
        &mut self,
        payload: &MarketPayload,
        row: &mut OutputRow,
    ) -> Result<(), DriverFailure> {
        if payload.symbol != REPLAY_SYMBOL {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "market {}: symbol must be {REPLAY_SYMBOL}",
                    payload.market_id
                ),
            ));
        }
        if payload.ends_at_ms - payload.starts_at_ms != ROUND_MS {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("market {}: a round lasts {ROUND_MS} ms", payload.market_id),
            ));
        }
        if payload.yes_token.trim().is_empty()
            || payload.no_token.trim().is_empty()
            || payload.yes_token == payload.no_token
        {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "market {}: tokens must be distinct and non-empty",
                    payload.market_id
                ),
            ));
        }
        if payload.yes_name != "Up" || payload.no_name != "Down" {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "market {}: the Yes token must be Up and the No token Down",
                    payload.market_id
                ),
            ));
        }
        if payload.rules_source.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "market {}: rules_source must not be empty",
                    payload.market_id
                ),
            ));
        }
        let state = MarketState {
            market_id: payload.market_id,
            symbol: payload.symbol.clone(),
            starts_at_ms: payload.starts_at_ms,
            ends_at_ms: payload.ends_at_ms,
            yes_token: payload.yes_token.clone(),
            no_token: payload.no_token.clone(),
            yes_name: payload.yes_name.clone(),
            no_name: payload.no_name.clone(),
            rules_source: payload.rules_source.clone(),
            open: None,
            contested: None,
            paused: None,
            start_price: None,
            fee_rate_bps: None,
            book: None,
            last_book: None,
            book_clock: None,
            pending_candidate: None,
            terminated_candidates: HashSet::new(),
            unknown_result: false,
            identity_available_at: None,
            strike_available_at: None,
            fixed_strike_conflict: false,
        };
        match self.markets.get(&payload.market_id) {
            Some(existing) => {
                if !same_identity(existing, &state) {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "identity_invalid",
                        format!(
                            "market {}: identity cannot change once established",
                            payload.market_id
                        ),
                    ));
                }
            }
            None => {
                // A token belongs to one market only: the same token in two
                // markets would let one market's book be read as another's.
                if let Some(other) = self.markets.values().find(|existing| {
                    existing.yes_token == state.yes_token
                        || existing.no_token == state.no_token
                        || existing.yes_token == state.no_token
                        || existing.no_token == state.yes_token
                }) {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "identity_invalid",
                        format!(
                            "market {}: a token of market {} cannot be reused",
                            payload.market_id,
                            other_id(other)
                        ),
                    ));
                }
                self.markets.insert(payload.market_id, state);
            }
        }
        // The identity was first available when this envelope became
        // available: the first valid reading freezes that moment.
        let market = self
            .markets
            .get_mut(&payload.market_id)
            .expect("the market exists");
        if market.identity_available_at.is_none() {
            market.identity_available_at = Some(row.available_at_ms);
        }
        row.market_id = Some(payload.market_id);
        row.yes_token = Some(payload.yes_token.clone());
        row.no_token = Some(payload.no_token.clone());
        row.symbol = Some(payload.symbol.clone());
        Ok(())
    }

    /// Apply a market's state, effective from the time it was available.
    fn apply_state(
        &mut self,
        payload: &StatePayload,
        row: &mut OutputRow,
    ) -> Result<(), DriverFailure> {
        if let Some(price) = payload.start_price {
            if !price.is_finite() || price <= 0.0 {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!("market {}: start_price must be positive", payload.market_id),
                ));
            }
        }
        let phase1 = self.version == ReplayVersion::V2;
        let market = self.market_mut(payload.market_id)?;
        if phase1
            && market.strike_available_at.is_some()
            && market.start_price != payload.start_price
        {
            market.fixed_strike_conflict = true;
        }
        market.open = Some(payload.open);
        market.contested = Some(payload.contested);
        market.paused = Some(payload.paused);
        market.start_price = payload.start_price;
        market.fee_rate_bps = payload.fee_rate_bps;
        // The fixed strike was first available when this envelope became
        // available: the first valid reading freezes that moment.
        if payload.start_price.is_some() && market.strike_available_at.is_none() {
            market.strike_available_at = Some(row.available_at_ms);
        }
        row.market_id = Some(payload.market_id);
        row.symbol = Some(market.symbol.clone());
        Ok(())
    }

    /// Record a spot observation on the event's clock.
    fn apply_spot(
        &mut self,
        payload: &SpotEvent,
        available_at_ms: i64,
        elapsed_ms: u64,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<(), DriverFailure> {
        if payload.symbol != REPLAY_SYMBOL {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("spot: symbol must be {REPLAY_SYMBOL}"),
            ));
        }
        if !payload.price.is_finite() || payload.price <= 0.0 {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "spot: price must be positive and finite".to_string(),
            ));
        }
        if payload.model_source.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                "spot: model_source must not be empty".to_string(),
            ));
        }
        if self.mode == ReplayMode::Proxy && available_at_ms % ROUND_MS != 0 {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "spot: proxy bars must be aligned to the round length".to_string(),
            ));
        }
        // The net-share version carries the source, the pair and the clock;
        // the source and the pair must agree with each other, and the clock
        // must be the moment the row was consumed. The identity of the feed
        // comes first: a price of the wrong pair is not a stale price, it is
        // a different price.
        if let Some(source) = payload.source {
            let Some(actual_pair) = payload.actual_pair.as_deref() else {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "schema_invalid",
                    "spot: actual_pair must not be empty".to_string(),
                ));
            };
            let expected = match source {
                SpotSource::Binance => "BTCUSDT",
                SpotSource::Coinbase => "BTC-USD",
            };
            if actual_pair != expected {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "identity_invalid",
                    format!("spot: the {source:?} feed serves {expected}, not {actual_pair}"),
                ));
            }
        }
        let clock = payload.clock.clone();
        if let Some(clock) = &clock {
            self.check_clock(clock, available_at_ms, elapsed_ms, row, "spot")?;
        }
        if let Some(last) = self.spots.get(&payload.symbol) {
            if last.at_ms == available_at_ms {
                if last.price != payload.price {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        format!("spot: two different prices at {available_at_ms} ms"),
                    ));
                }
                summary.spot_duplicate_count += 1;
            }
        }
        // A gap in the observed series breaks the volatility history: the
        // estimate warms up again instead of carrying across the hole.
        if self.mode == ReplayMode::Proxy {
            if let Some(last) = self.spots.get(&payload.symbol) {
                if available_at_ms - last.at_ms > ROUND_MS {
                    self.core.forget_vol(&payload.symbol);
                }
            }
        }
        match self.version {
            ReplayVersion::V1 => {
                self.core.observe_spot(
                    &payload.symbol,
                    payload.price,
                    available_at_ms as f64 / 1000.0,
                );
            }
            ReplayVersion::V2 => {
                let (Some(source), Some(actual_pair), Some(clock)) =
                    (payload.source, payload.actual_pair.clone(), clock.clone())
                else {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "schema_invalid",
                        "spot: the net-share version needs a source, a pair and a clock"
                            .to_string(),
                    ));
                };
                let now = CheckTime {
                    utc: DateTime::<Utc>::from_timestamp_millis(available_at_ms).ok_or_else(
                        || {
                            DriverFailure::new(
                                EXIT_CONTRACT,
                                "value_invalid",
                                "spot: available_at_ms is not a UTC instant".to_string(),
                            )
                        },
                    )?,
                    elapsed_ms,
                };
                if let Err(reason) = self.core.observe_phase1(
                    &payload.symbol,
                    payload.price,
                    source,
                    &actual_pair,
                    &clock,
                    now,
                ) {
                    // Keep the received input for the next diagnostic, just
                    // like RoundTaker. It was not admitted to EWMA; silently
                    // substituting the older cached quote would lose its clock.
                    row.reason = reason_text(reason).to_lowercase();
                    summary.missing_reasons.push(row.reason.clone());
                }
            }
        }
        self.spots.insert(
            payload.symbol.clone(),
            SpotState {
                price: payload.price,
                at_ms: available_at_ms,
                observed: match (payload.source, payload.actual_pair.clone(), clock) {
                    (Some(source), Some(actual_pair), Some(clock)) => Some(ObservedSpotState {
                        source,
                        actual_pair,
                        clock,
                    }),
                    _ => None,
                },
            },
        );
        row.symbol = Some(payload.symbol.clone());
        row.samples = match self.version {
            ReplayVersion::V1 => self.core.vol_samples(&payload.symbol),
            ReplayVersion::V2 => Some(self.core.samples(&payload.symbol)),
        };
        row.sigma = match self.version {
            ReplayVersion::V1 => self.core.sigma(&payload.symbol),
            ReplayVersion::V2 => self
                .core
                .phase1_sigma(&payload.symbol, self.config.vol_floor),
        };
        Ok(())
    }

    /// Record a book snapshot, returning true for a same-time duplicate.
    fn apply_book(
        &mut self,
        payload: &BookEvent,
        available_at_ms: i64,
        elapsed_ms: u64,
        row: &mut OutputRow,
    ) -> Result<bool, DriverFailure> {
        if let Some(clock) = &payload.clock {
            self.check_clock(clock, available_at_ms, elapsed_ms, row, "book")?;
        }
        let market = self.market_mut(payload.market_id)?;
        if payload.yes_token != market.yes_token {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "market {}: the book's token does not belong to this market",
                    payload.market_id
                ),
            ));
        }
        let bids = levels(&payload.bids, "bids")?;
        let asks = levels(&payload.asks, "asks")?;
        for window in bids.windows(2) {
            if window[0].price <= window[1].price {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!("market {}: bids must decrease", payload.market_id),
                ));
            }
        }
        for window in asks.windows(2) {
            if window[0].price >= window[1].price {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!("market {}: asks must increase", payload.market_id),
                ));
            }
        }
        if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            if best_bid.price > best_ask.price {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!(
                        "market {}: the best bid is above the best ask",
                        payload.market_id
                    ),
                ));
            }
        }
        let wire = BookPayload {
            market_id: payload.market_id,
            yes_token: payload.yes_token.clone(),
            bids: payload.bids.clone(),
            asks: payload.asks.clone(),
        };
        if let Some((previous_at, previous)) = &market.last_book {
            if *previous_at == available_at_ms {
                if previous != &wire {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        format!(
                            "market {}: conflicting books at {available_at_ms} ms",
                            payload.market_id
                        ),
                    ));
                }
                return Ok(true);
            }
        }
        market.last_book = Some((available_at_ms, wire));
        market.book = Some(BookSnapshot { bids, asks });
        market.book_clock = payload.clock.clone();
        row.market_id = Some(payload.market_id);
        row.yes_token = Some(payload.yes_token.clone());
        row.symbol = Some(market.symbol.clone());
        Ok(false)
    }

    /// Decide on a market, the way the live tick would.
    fn apply_tick(
        &mut self,
        payload: &TickPayload,
        available_at_ms: i64,
        elapsed_ms: u64,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<Option<DecisionDiagnostic>, DriverFailure> {
        match self.version {
            ReplayVersion::V1 => {
                self.apply_tick_v1(payload, available_at_ms, row, summary)?;
                Ok(None)
            }
            ReplayVersion::V2 => {
                self.apply_tick_v2(payload, available_at_ms, elapsed_ms, row, summary)
            }
        }
    }

    /// Decide on a market under the original cash-fee economics.
    fn apply_tick_v1(
        &mut self,
        payload: &TickPayload,
        available_at_ms: i64,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<(), DriverFailure> {
        let now = DateTime::<Utc>::from_timestamp_millis(available_at_ms).ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "tick: available_at_ms is not a UTC instant".to_string(),
            )
        })?;
        let market = self.market_mut(payload.market_id)?.clone();
        row.market_id = Some(payload.market_id);
        row.yes_token = Some(market.yes_token.clone());
        row.no_token = Some(market.no_token.clone());
        row.symbol = Some(market.symbol.clone());
        // The volatility state is reported on every tick, so a reader can see
        // how far the warm-up had got even when no decision was possible.
        row.samples = self.core.vol_samples(&market.symbol);
        row.sigma = self.core.sigma(&market.symbol);
        let entered_before = self.core.is_entered(payload.market_id);
        row.entered_before = Some(entered_before);
        row.entered_after = Some(entered_before);
        row.candidate = Some(false);

        if let Some(reason) = self.tick_reason(&market, payload.market_id, available_at_ms) {
            row.reason = reason.to_string();
            if MISSING_EVIDENCE_REASONS.contains(&reason) {
                summary.missing_reasons.push(reason.to_string());
            }
            return Ok(());
        }
        // Everything needed is present, so value the round and look for an entry.
        let symbol = market.symbol.clone();
        let spot = self
            .spots
            .get(&symbol)
            .map(|state| state.price)
            .ok_or_else(|| {
                DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    "tick: spot must be known".to_string(),
                )
            })?;
        let book = market.book.clone().ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "tick: a book must be known".to_string(),
            )
        })?;
        let start_price = market.start_price.ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "tick: a start price must be known".to_string(),
            )
        })?;
        let secs_left = (market.ends_at_ms - available_at_ms) / 1000;
        if let Some(valuation) = self
            .core
            .valuation(&symbol, spot, start_price, secs_left as f64)
        {
            row.samples = Some(valuation.samples);
            row.sigma = Some(valuation.sigma);
            row.p_up = Some(valuation.p_up);
        }
        let tradable = tradable_market(&market);
        match self.core.entry(&tradable, &book, spot, now) {
            Some(entry) => {
                row.candidate = Some(true);
                row.token = Some(entry.token.clone());
                row.price = Some(entry.price);
                row.fair = Some(entry.fair);
                row.edge = Some(entry.edge);
                row.reason = "candidate".to_string();
                summary.candidate_count += 1;
                if let Some(state) = self.markets.get_mut(&payload.market_id) {
                    state.pending_candidate = Some(row.event_id.clone());
                }
            }
            None => {
                row.reason = "edge_below_threshold".to_string();
            }
        }
        Ok(())
    }

    /// Decide on a market under the net-share economics.
    ///
    /// The shared core does the deciding: this path only builds the inputs the
    /// live path would build and reports what the core concluded. The
    /// candidate the core produces is the one the result refers to.
    fn apply_tick_v2(
        &mut self,
        payload: &TickPayload,
        available_at_ms: i64,
        elapsed_ms: u64,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<Option<DecisionDiagnostic>, DriverFailure> {
        let now = DateTime::<Utc>::from_timestamp_millis(available_at_ms).ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "tick: available_at_ms is not a UTC instant".to_string(),
            )
        })?;
        let market = self.market_mut(payload.market_id)?.clone();
        row.market_id = Some(payload.market_id);
        row.yes_token = Some(market.yes_token.clone());
        row.no_token = Some(market.no_token.clone());
        row.symbol = Some(market.symbol.clone());
        let entered_before = self.core.is_entered(payload.market_id);
        row.entered_before = Some(entered_before);
        row.entered_after = Some(entered_before);
        row.candidate = Some(false);
        row.samples = Some(self.core.samples(&market.symbol));
        row.sigma = self
            .core
            .phase1_sigma(&market.symbol, self.config.vol_floor);

        // The round keeps one open entry: while a candidate waits for its
        // result, the round offers no second one.
        let open_candidate = market.pending_candidate.is_some();
        let spot = self.spots.get(&market.symbol).cloned();
        let observed = spot.as_ref().and_then(|state| state.observed.as_ref());
        let spot_clock = observed.map(|observed| observed.clock.clone());
        let actual_pair = observed.map(|observed| observed.actual_pair.clone());
        let source = observed.map(|observed| observed.source);
        let book = market.book.clone().unwrap_or_default();
        let tradable = tradable_market_v2(&market);
        #[cfg(test)]
        let tradable = TradableMarket {
            round_facts: self.fixture_round_facts.clone(),
            ..tradable
        };
        let inputs = EvaluationInputs {
            market: &tradable,
            book: &book,
            spot_price: spot.as_ref().map(|state| state.price).unwrap_or(f64::NAN),
            spot_source: source,
            actual_pair,
            spot_clock,
            book_clock: market.book_clock.clone(),
            now: CheckTime {
                utc: now,
                elapsed_ms,
            },
            order_usd: self.config.order_usd,
            min_edge: self.config.min_edge,
            min_secs_left: self.config.min_secs_left as f64,
            entered: entered_before,
            identity_available_at: market
                .identity_available_at
                .and_then(|at| DateTime::<Utc>::from_timestamp_millis(at)),
            strike_available_at: market
                .strike_available_at
                .and_then(|at| DateTime::<Utc>::from_timestamp_millis(at)),
            // The net-share wire carries no official provider: it stays
            // unknown rather than being read out of the prediction source.
            settlement_provider: SettlementProvider::Unknown,
        };
        let evaluation = self.core.evaluate_phase1(&inputs);
        let mut diagnostic = evaluation.diagnostic;
        let mut candidate = evaluation.candidate;
        // The official fee is a rule of the market: without it the fee cannot
        // be zero. The core sees no fee, so the driver refuses here.
        diagnostic.fee_rate_bps = market.fee_rate_bps;
        if market.fee_rate_bps.is_none() && !entered_before {
            diagnostic.reason = Reason::MarketInvalid;
            diagnostic.candidate_id = None;
            diagnostic.selected_token = None;
            candidate = None;
        }
        if open_candidate || market.unknown_result {
            // The round already has an open entry: the single-entry
            // protection of the round applies, and no new candidate is made.
            candidate = None;
            diagnostic.candidate_id = None;
            diagnostic.reason = Reason::AlreadyEntered;
            diagnostic.selected_token = None;
        }
        row.samples = Some(diagnostic.samples);
        row.sigma = diagnostic.sigma;
        row.p_up = diagnostic.p_up;
        row.reason = reason_text(diagnostic.reason).to_lowercase();
        if market.unknown_result {
            row.reason = "result_unknown".to_string();
        }
        if let Some(selected) = &diagnostic.selected_token {
            row.token = Some(selected.clone());
            let side = if selected == &market.yes_token {
                diagnostic.up.as_ref()
            } else {
                diagnostic.down.as_ref()
            };
            if let Some(side) = side {
                row.price = side.limit_price;
                row.fair = side.fair;
                row.edge = side.edge;
            }
        }
        if let Some(candidate) = &candidate {
            row.candidate = Some(true);
            row.reason = "candidate".to_string();
            summary.candidate_count += 1;
            if let Some(state) = self.markets.get_mut(&payload.market_id) {
                // The result refers to the candidate the core produced, not to
                // the event that asked for it. An older candidate that is
                // replaced is terminated, never confirmed.
                if let Some(previous) = state
                    .pending_candidate
                    .replace(candidate.candidate_id().to_string())
                {
                    state.terminated_candidates.insert(previous);
                }
            }
        } else if !MISSING_EVIDENCE_REASONS.contains(&row.reason.as_str())
            && row.reason != "already_entered"
        {
            summary.missing_reasons.push(row.reason.clone());
        }
        #[cfg(test)]
        {
            self.fixture_candidate = candidate;
        }
        Ok(Some(diagnostic))
    }

    /// The first reason this tick cannot decide, in the frozen priority order.
    fn tick_reason(
        &self,
        market: &MarketState,
        market_id: u64,
        available_at_ms: i64,
    ) -> Option<&'static str> {
        if market.open.is_none() {
            return Some("state_unknown");
        }
        if available_at_ms < market.starts_at_ms {
            return Some("not_started");
        }
        if available_at_ms >= market.ends_at_ms {
            return Some("ended");
        }
        if !market.open.unwrap_or(false) {
            return Some("closed");
        }
        if market.contested.unwrap_or(false) {
            return Some("contested");
        }
        if market.paused.unwrap_or(false) {
            return Some("paused");
        }
        if market.start_price.is_none() {
            return Some("start_price_unknown");
        }
        if market.fee_rate_bps.is_none() {
            return Some("fee_unknown");
        }
        if !self.spots.contains_key(&market.symbol) {
            return Some("spot_unknown");
        }
        if self.core.vol_samples(&market.symbol).unwrap_or(0) < MIN_VOL_SAMPLES {
            return Some("warmup");
        }
        if market.book.is_none() {
            return Some("book_unknown");
        }
        if self.core.is_entered(market_id) {
            return Some("already_entered");
        }
        if market.unknown_result {
            return Some("result_unknown");
        }
        if market.pending_candidate.is_some() {
            return Some("candidate_unconfirmed");
        }
        let secs_left = (market.ends_at_ms - available_at_ms) / 1000;
        if secs_left < self.config.min_secs_left {
            return Some("near_expiry");
        }
        None
    }

    /// Apply a local execution result to a pending candidate.
    fn apply_result(
        &mut self,
        payload: &ResultPayload,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<(), DriverFailure> {
        if let Some(shares) = payload.filled_shares {
            if !shares.is_finite() || shares < 0.0 {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!(
                        "result: filled_shares must be non-negative for market {}",
                        payload.market_id
                    ),
                ));
            }
        }
        match payload.status {
            ResultStatus::Blocked => {
                if payload.filled_shares.is_some() {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "result_invalid",
                        "result: a blocked result carries no fill".to_string(),
                    ));
                }
                match payload.detail.as_deref() {
                    Some(detail) if !detail.trim().is_empty() => {}
                    _ => {
                        return Err(DriverFailure::new(
                            EXIT_CONTRACT,
                            "result_invalid",
                            "result: a blocked result needs a reason".to_string(),
                        ))
                    }
                }
            }
            ResultStatus::Filled => {
                if payload.filled_shares.is_none() {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "result_invalid",
                        "result: a filled result needs a share count".to_string(),
                    ));
                }
            }
            ResultStatus::Unknown => {
                if payload.filled_shares.is_some() {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "result_invalid",
                        "result: an unknown result carries no fill".to_string(),
                    ));
                }
            }
        }
        let pending = self
            .markets
            .get(&payload.market_id)
            .and_then(|market| market.pending_candidate.clone());
        let terminated = self.markets.get(&payload.market_id).is_some_and(|market| {
            market
                .terminated_candidates
                .contains(&payload.candidate_event_id)
        });
        match pending {
            Some(candidate) if candidate == payload.candidate_event_id => {}
            _ => {
                // A candidate that was replaced may still be answered with a
                // refusal; it can never be answered with a fill.
                if terminated && payload.status == ResultStatus::Blocked {
                    let market = self
                        .markets
                        .get_mut(&payload.market_id)
                        .expect("the market exists");
                    market
                        .terminated_candidates
                        .remove(&payload.candidate_event_id);
                    row.market_id = Some(payload.market_id);
                    row.result_status = Some(payload.status);
                    row.reason = "blocked".to_string();
                    return Ok(());
                }
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "result_invalid",
                    format!(
                        "result: market {} has no pending candidate {}",
                        payload.market_id, payload.candidate_event_id
                    ),
                ));
            }
        }
        let entered_before = self.core.is_entered(payload.market_id);
        row.entered_before = Some(entered_before);
        row.market_id = Some(payload.market_id);
        row.symbol = Some(
            self.markets
                .get(&payload.market_id)
                .map(|market| market.symbol.clone())
                .unwrap_or_default(),
        );
        row.result_status = Some(payload.status);
        row.filled_shares = payload.filled_shares;
        if let Some(state) = self.markets.get_mut(&payload.market_id) {
            state.pending_candidate = None;
        }
        match payload.status {
            ResultStatus::Blocked => {
                row.reason = "blocked".to_string();
            }
            ResultStatus::Filled => {
                let shares = payload.filled_shares.unwrap_or(0.0);
                if shares > 0.0 {
                    self.core.confirm_fill(payload.market_id, shares);
                    summary.positive_fill_count += 1;
                    row.reason = "filled_positive".to_string();
                } else {
                    row.reason = "filled_zero".to_string();
                }
            }
            ResultStatus::Unknown => {
                if let Some(state) = self.markets.get_mut(&payload.market_id) {
                    state.unknown_result = true;
                }
                summary.unknown_result_count += 1;
                row.reason = "result_unknown".to_string();
                summary.missing_reasons.push("result_unknown".to_string());
            }
        }
        row.entered_after = Some(self.core.is_entered(payload.market_id));
        Ok(())
    }

    /// Record a settlement label. It never reaches a decision.
    fn apply_label(
        &mut self,
        payload: &LabelPayload,
        available_at_ms: i64,
        row: &mut OutputRow,
    ) -> Result<(), DriverFailure> {
        let market = self.market_mut(payload.market_id)?;
        if payload.yes_token != market.yes_token {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("label: market {} has no such token", payload.market_id),
            ));
        }
        if payload.official_source.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                "label: official_source must not be empty".to_string(),
            ));
        }
        let ends_at_ms = market.ends_at_ms;
        let symbol = market.symbol.clone();
        match payload.status {
            LabelStatus::Settled | LabelStatus::Tie => {
                let payout = payload.payout_yes.ok_or_else(|| {
                    DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        "label: a settled label needs a payout".to_string(),
                    )
                })?;
                if !payout.is_finite() || !(0.0..=1.0).contains(&payout) {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        "label: payout_yes must be within [0, 1]".to_string(),
                    ));
                }
                if payload.status == LabelStatus::Tie && payout != 0.5 {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        "label: a tie pays 0.5".to_string(),
                    ));
                }
                if payload.authority_report_ref.trim().is_empty() {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "schema_invalid",
                        "label: a settled label needs an authority reference".to_string(),
                    ));
                }
                let published = payload.published_at_ms.ok_or_else(|| {
                    DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        "label: a settled label needs a publication time".to_string(),
                    )
                })?;
                if published < ends_at_ms || published > available_at_ms {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "time_order",
                        "label: the publication time is outside the round".to_string(),
                    ));
                }
                self.saw_official_label = true;
            }
            LabelStatus::Pending | LabelStatus::Disputed => {
                if payload.payout_yes.is_some() {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        "label: an unsettled label carries no payout".to_string(),
                    ));
                }
            }
        }
        row.market_id = Some(payload.market_id);
        row.yes_token = Some(payload.yes_token.clone());
        row.symbol = Some(symbol);
        row.reason = "label_evaluation_only".to_string();
        Ok(())
    }

    /// Value the underlying against a strike, in proxy mode.
    fn apply_value(
        &mut self,
        payload: &ValuePayload,
        available_at_ms: i64,
        row: &mut OutputRow,
        summary: &mut ReplaySummary,
    ) -> Result<(), DriverFailure> {
        if payload.symbol != REPLAY_SYMBOL {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("value: symbol must be {REPLAY_SYMBOL}"),
            ));
        }
        if !payload.strike.is_finite() || payload.strike <= 0.0 {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "value: strike must be positive and finite".to_string(),
            ));
        }
        if payload.horizon_secs != PROXY_HORIZON_SECS {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                format!("value: horizon_secs must be {PROXY_HORIZON_SECS}"),
            ));
        }
        let spot = self.spots.get(&payload.symbol).cloned().ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "value: a spot observation is required first".to_string(),
            )
        })?;
        if spot.at_ms != available_at_ms || spot.price != payload.strike {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                "value: the strike must equal the spot closed at this time".to_string(),
            ));
        }
        row.symbol = Some(payload.symbol.clone());
        row.samples = self.core.vol_samples(&payload.symbol);
        match self.core.valuation(
            &payload.symbol,
            payload.strike,
            payload.strike,
            payload.horizon_secs as f64,
        ) {
            Some(valuation) => {
                row.sigma = Some(valuation.sigma);
                row.p_up = Some(valuation.p_up);
                row.reason = "valued".to_string();
            }
            None => {
                row.reason = "warmup".to_string();
                summary.missing_reasons.push("warmup".to_string());
            }
        }
        Ok(())
    }

    /// Check that the clock of an input agrees with the envelope that
    /// carries it.
    ///
    /// The envelope's available time is the moment the row became available
    /// and the elapsed clock is the process's own reading at that moment: the
    /// two cannot disagree with the clock the input carries.
    fn check_clock(
        &self,
        clock: &ObservationClock,
        available_at_ms: i64,
        elapsed_ms: u64,
        row: &mut OutputRow,
        kind: &str,
    ) -> Result<(), DriverFailure> {
        let received_at_ms = clock.received_at.timestamp_millis();
        if received_at_ms != available_at_ms {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "time_order",
                format!("{kind}: available_at_ms must be the moment the input was received"),
            ));
        }
        if clock.received_elapsed_ms != elapsed_ms {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "time_order",
                format!("{kind}: elapsed_ms must be the received elapsed clock"),
            ));
        }
        // The source's own time and the event's own time are the same fact,
        // or both are absent. Neither one is allowed to cover the other.
        match (row.event_at_ms, clock.source_at) {
            (None, None) => Ok(()),
            (Some(event_at), Some(source_at)) if event_at == source_at.timestamp_millis() => Ok(()),
            _ => Err(DriverFailure::new(
                EXIT_CONTRACT,
                "time_order",
                format!(
                    "{kind}: event_at_ms and the source's own time must agree, or both be absent"
                ),
            )),
        }
    }

    /// The market state, or a failure if the market was never established.
    fn market_mut(&mut self, market_id: u64) -> Result<&mut MarketState, DriverFailure> {
        self.markets.get_mut(&market_id).ok_or_else(|| {
            DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("market {market_id} was never established"),
            )
        })
    }

    /// Close the run: count what is still open and settle the exit code.
    pub(super) fn finish(&mut self, summary: &mut ReplaySummary) {
        summary.unconfirmed_count = self
            .markets
            .values()
            .filter(|market| market.pending_candidate.is_some())
            .count() as u64;
        for market in self.markets.values() {
            if market.pending_candidate.is_some() {
                summary
                    .missing_reasons
                    .push("candidate_unconfirmed".to_string());
            }
        }
        if self.mode == ReplayMode::Proxy {
            summary.missing_reasons.push("proxy_only".to_string());
        } else if self.saw_real_market && !self.saw_official_label {
            summary
                .missing_reasons
                .push("official_labels_missing".to_string());
        }
        let mut reasons = summary.missing_reasons.clone();
        reasons.sort();
        reasons.dedup();
        summary.missing_reasons = reasons;
        summary.exit_code = if summary.missing_reasons.is_empty() {
            0
        } else {
            EXIT_EVIDENCE
        };
    }
}

/// The market as the live path sees it.
fn tradable_market(market: &MarketState) -> TradableMarket {
    TradableMarket {
        id: 0,
        question: String::new(),
        category_slug: String::new(),
        variant: String::new(),
        yes_token: market.yes_token.clone(),
        no_token: market.no_token.clone(),
        yes_name: market.yes_name.clone(),
        no_name: market.no_name.clone(),
        open: market.open.unwrap_or(false),
        contested: market.contested.unwrap_or(false),
        tick: 0.01,
        fee_rate_bps: market.fee_rate_bps.unwrap_or(0),
        starts_at: DateTime::<Utc>::from_timestamp_millis(market.starts_at_ms),
        ends_at: DateTime::<Utc>::from_timestamp_millis(market.ends_at_ms),
        start_price: market.start_price,
        end_price: None,
        feed_symbol: Some(market.symbol.clone()),
        polymarket_condition_ids: Vec::new(),
        payout_yes: None,
        labels: None,
        round_facts: None,
    }
}

/// The market as the net-share path sees it.
///
/// The market keeps its own identity: the core checks the frozen inputs
/// against it, so the round the driver offers is the round it holds.
fn tradable_market_v2(market: &MarketState) -> TradableMarket {
    TradableMarket {
        id: market.market_id,
        open: market.open == Some(true)
            && market.paused == Some(false)
            && !market.fixed_strike_conflict,
        ..tradable_market(market)
    }
}

/// The identity of a market, for an error message.
fn other_id(market: &MarketState) -> u64 {
    market.market_id
}

/// Whether two market identities agree.
fn same_identity(left: &MarketState, right: &MarketState) -> bool {
    left.symbol == right.symbol
        && left.starts_at_ms == right.starts_at_ms
        && left.ends_at_ms == right.ends_at_ms
        && left.yes_token == right.yes_token
        && left.no_token == right.no_token
        && left.yes_name == right.yes_name
        && left.no_name == right.no_name
        && left.rules_source == right.rules_source
}

/// Check a book's levels and turn them into the model's own type.
fn levels(levels: &[BookLevelPayload], side: &str) -> Result<Vec<BookLevel>, DriverFailure> {
    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        if !level.price.is_finite() || !(0.0..=1.0).contains(&level.price) {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                format!("book: {side} prices must be within [0, 1]"),
            ));
        }
        if !level.size.is_finite() || level.size < 0.0 {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                format!("book: {side} sizes must be non-negative"),
            ));
        }
        out.push(BookLevel {
            price: level.price,
            size: level.size,
        });
    }
    Ok(out)
}

/// The stable text of a diagnostic reason.
///
/// The reason is an enum with one spelling; the text comes from the enum
/// itself, so it cannot drift away from the contract.
fn reason_text(reason: Reason) -> &'static str {
    match reason {
        Reason::RunStopped => "RUN_STOPPED",
        Reason::AlreadyEntered => "ALREADY_ENTERED",
        Reason::MarketInvalid => "MARKET_INVALID",
        Reason::MissingInput => "MISSING_INPUT",
        Reason::InputInvalid => "INPUT_INVALID",
        Reason::ClockInvalid => "CLOCK_INVALID",
        Reason::RequestTooSlow => "REQUEST_TOO_SLOW",
        Reason::SpotStale => "SPOT_STALE",
        Reason::BookStale => "BOOK_STALE",
        Reason::RoundTooLate => "ROUND_TOO_LATE",
        Reason::InputChanged => "INPUT_CHANGED",
        Reason::Warmup => "WARMUP",
        Reason::NetSharesInvalid => "NET_SHARES_INVALID",
        Reason::EdgeBelowMin => "EDGE_BELOW_MIN",
        Reason::DiagnosticLimit => "DIAGNOSTIC_LIMIT",
        Reason::BrokerBlocked => "BROKER_BLOCKED",
        Reason::ContextMissing => "CONTEXT_MISSING",
        Reason::Eligible => "ELIGIBLE",
    }
}
