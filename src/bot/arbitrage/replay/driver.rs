//! The state machine that walks a replay or proxy event stream.
//!
//! The driver owns the behaviour: it validates each envelope, keeps the
//! market and spot state, calls the shared decision core and produces one
//! output row per event. It has no file, process or network I/O: the facade
//! owns the stream.

use super::types::*;
use super::super::{DecisionCore, MIN_VOL_SAMPLES};
use crate::models::{BookLevel, BookSnapshot};
use crate::service::venue::TradableMarket;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};

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
    /// The candidate waiting for a result, if any.
    pending_candidate: Option<String>,
    /// Set once a result was unknown: the market stops offering entries.
    unknown_result: bool,
}

/// The last spot seen for a symbol.
#[derive(Debug, Clone, Copy)]
struct SpotState {
    price: f64,
    at_ms: i64,
}
/// A failure inside the driver, before it becomes a [`ReplayError`].
pub(super) struct DriverFailure {
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

/// The state machine that walks the stream.
pub(super) struct Driver {
    mode: ReplayMode,
    core: DecisionCore,
    config: ReplayConfig,
    markets: BTreeMap<u64, MarketState>,
    spots: HashMap<String, SpotState>,
    seen: HashMap<String, EventEnvelope>,
    last_key: Option<(i64, u64)>,
    saw_official_label: bool,
    saw_real_market: bool,
    stream_evidence: Option<EvidenceType>,
}

impl Driver {
    pub(super) fn new(config: ReplayConfig, mode: ReplayMode) -> Self {
        let core = DecisionCore::with_warmup(
            config.strategy_config(),
            config.half_life_secs,
            config.min_vol_samples,
        );
        Self {
            mode,
            core,
            config,
            markets: BTreeMap::new(),
            spots: HashMap::new(),
            seen: HashMap::new(),
            last_key: None,
            saw_official_label: false,
            saw_real_market: false,
            stream_evidence: None,
        }
    }

    /// Process one event.
    pub(super) fn step(
        &mut self,
        envelope: EventEnvelope,
        _line: u64,
        summary: &mut ReplaySummary,
    ) -> Result<Option<OutputRow>, DriverFailure> {
        self.validate_envelope(&envelope)?;
        // A repeat of an event already seen is ignored; a repeat that says
        // something different is a conflict.
        if let Some(previous) = self.seen.get(&envelope.event_id) {
            if *previous == envelope {
                summary.duplicate_count += 1;
                return Ok(None);
            }
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!(
                    "event {}: repeated id with a different payload",
                    envelope.event_id
                ),
            ));
        }

        let key = (envelope.available_at_ms, envelope.seq);
        if let Some(last) = self.last_key {
            if key <= last {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "time_order",
                    format!(
                        "event {}: available_at_ms/seq must strictly increase",
                        envelope.event_id
                    ),
                ));
            }
        }
        self.last_key = Some(key);
        self.stream_evidence = Some(envelope.evidence_type);
        self.seen.insert(envelope.event_id.clone(), envelope.clone());

        self.dispatch(envelope, summary)
    }

    /// Check the envelope's shape, before anything is remembered about it.
    fn validate_envelope(&self, envelope: &EventEnvelope) -> Result<(), DriverFailure> {
        if envelope.schema_version != SCHEMA_VERSION {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: schema_version must be {SCHEMA_VERSION}",
                    envelope.event_id
                ),
            ));
        }
        if envelope.event_id.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                "event_id must not be empty".to_string(),
            ));
        }
        if envelope.source.trim().is_empty() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!("event {}: source must not be empty", envelope.event_id),
            ));
        }
        if DateTime::<Utc>::from_timestamp_millis(envelope.available_at_ms).is_none() {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "value_invalid",
                format!(
                    "event {}: available_at_ms is not a UTC instant",
                    envelope.event_id
                ),
            ));
        }
        if let Some(event_at) = envelope.event_at_ms {
            if DateTime::<Utc>::from_timestamp_millis(event_at).is_none() {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "value_invalid",
                    format!("event {}: event_at_ms is not a UTC instant", envelope.event_id),
                ));
            }
            if event_at > envelope.available_at_ms {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "time_order",
                    format!(
                        "event {}: event_at_ms is after available_at_ms",
                        envelope.event_id
                    ),
                ));
            }
        }
        // Evidence is a stream identity, not a per-row permission to share state.
        if matches!((self.mode, envelope.evidence_type),
            (ReplayMode::Proxy, EvidenceType::RealMarket)
                | (ReplayMode::Replay, EvidenceType::RealProxy))
        {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("event {}: evidence is not allowed in this mode", envelope.event_id),
            ));
        }
        if self.stream_evidence.is_some_and(|kind| kind != envelope.evidence_type) {
            return Err(DriverFailure::new(
                EXIT_CONTRACT,
                "identity_invalid",
                format!("event {}: evidence cannot change within a stream", envelope.event_id),
            ));
        }
        match (self.mode, envelope.payload.event_type()) {
            (ReplayMode::Proxy, EventType::Spot) | (ReplayMode::Proxy, EventType::Value) => Ok(()),
            (ReplayMode::Proxy, other) => Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: {} is not available in proxy mode",
                    envelope.event_id,
                    other.as_str()
                ),
            )),
            (ReplayMode::Replay, EventType::Value) => Err(DriverFailure::new(
                EXIT_CONTRACT,
                "schema_invalid",
                format!(
                    "event {}: value is not available in replay mode",
                    envelope.event_id
                ),
            )),
            (ReplayMode::Replay, _) => Ok(()),
        }
    }

    /// Apply one event and produce its row.
    fn dispatch(
        &mut self,
        envelope: EventEnvelope,
        summary: &mut ReplaySummary,
    ) -> Result<Option<OutputRow>, DriverFailure> {
        let event_type = envelope.payload.event_type();
        let mut row = OutputRow {
            schema_version: SCHEMA_VERSION.to_string(),
            event_id: envelope.event_id.clone(),
            event_at_ms: envelope.event_at_ms,
            available_at_ms: envelope.available_at_ms,
            seq: envelope.seq,
            evidence_type: envelope.evidence_type,
            source: envelope.source.clone(),
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
        if envelope.evidence_type == EvidenceType::RealMarket {
            self.saw_real_market = true;
        }
        match envelope.payload {
            Payload::Market(payload) => self.apply_market(&payload, &mut row)?,
            Payload::State(payload) => self.apply_state(&payload, &mut row)?,
            Payload::Spot(payload) => {
                self.apply_spot(&payload, envelope.available_at_ms, &mut row, summary)?
            }
            Payload::Book(payload) => {
                if self.apply_book(&payload, envelope.available_at_ms, &mut row)? {
                    summary.duplicate_count += 1;
                    return Ok(None);
                }
            }
            Payload::Tick(payload) => {
                self.apply_tick(&payload, envelope.available_at_ms, &mut row, summary)?
            }
            Payload::Value(payload) => {
                self.apply_value(&payload, envelope.available_at_ms, &mut row, summary)?
            }
            Payload::Result(payload) => {
                self.apply_result(&payload, &mut row, summary)?;
            }
            Payload::Label(payload) => self.apply_label(&payload, envelope.available_at_ms, &mut row)?,
        }
        Ok(Some(row))
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
                format!("market {}: symbol must be {REPLAY_SYMBOL}", payload.market_id),
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
                format!("market {}: rules_source must not be empty", payload.market_id),
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
            pending_candidate: None,
            unknown_result: false,
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
                            payload.market_id, other_id(other)
                        ),
                    ));
                }
                self.markets.insert(payload.market_id, state);
            }
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
                    format!(
                        "market {}: start_price must be positive",
                        payload.market_id
                    ),
                ));
            }
        }
        let market = self.market_mut(payload.market_id)?;
        market.open = Some(payload.open);
        market.contested = Some(payload.contested);
        market.paused = Some(payload.paused);
        market.start_price = payload.start_price;
        market.fee_rate_bps = payload.fee_rate_bps;
        row.market_id = Some(payload.market_id);
        row.symbol = Some(market.symbol.clone());
        Ok(())
    }

    /// Record a spot observation on the event's clock.
    fn apply_spot(
        &mut self,
        payload: &SpotPayload,
        available_at_ms: i64,
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
        self.core
            .observe_spot(&payload.symbol, payload.price, available_at_ms as f64 / 1000.0);
        self.spots.insert(
            payload.symbol.clone(),
            SpotState {
                price: payload.price,
                at_ms: available_at_ms,
            },
        );
        row.symbol = Some(payload.symbol.clone());
        row.samples = self.core.vol_samples(&payload.symbol);
        row.sigma = self.core.sigma(&payload.symbol);
        Ok(())
    }

    /// Record a book snapshot, returning true for a same-time duplicate.
    fn apply_book(
        &mut self,
        payload: &BookPayload,
        available_at_ms: i64,
        row: &mut OutputRow,
    ) -> Result<bool, DriverFailure> {
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
        if let Some((previous_at, previous)) = &market.last_book {
            if *previous_at == available_at_ms {
                if previous != payload {
                    return Err(DriverFailure::new(
                        EXIT_CONTRACT,
                        "value_invalid",
                        format!("market {}: conflicting books at {available_at_ms} ms", payload.market_id),
                    ));
                }
                return Ok(true);
            }
        }
        market.last_book = Some((available_at_ms, payload.clone()));
        market.book = Some(BookSnapshot { bids, asks });
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
        let spot = self.spots.get(&symbol).map(|state| state.price).ok_or_else(|| {
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
        match pending {
            Some(candidate) if candidate == payload.candidate_event_id => {}
            _ => {
                return Err(DriverFailure::new(
                    EXIT_CONTRACT,
                    "result_invalid",
                    format!(
                        "result: market {} has no pending candidate {}",
                        payload.market_id, payload.candidate_event_id
                    ),
                ))
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
        let spot = self.spots.get(&payload.symbol).copied().ok_or_else(|| {
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
                summary.missing_reasons.push("candidate_unconfirmed".to_string());
            }
        }
        if self.mode == ReplayMode::Proxy {
            summary.missing_reasons.push("proxy_only".to_string());
        } else if self.saw_real_market && !self.saw_official_label {
            summary.missing_reasons.push("official_labels_missing".to_string());
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
