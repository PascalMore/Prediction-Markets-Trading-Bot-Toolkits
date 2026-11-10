//! The wire types of the local replay protocol.
//!
//! This module owns the configuration, the event envelope, every payload, the
//! output row, the run summary and the error type. It has no I/O and no
//! decision logic: the driver owns the behaviour, the facade owns the stream.

use crate::config::BtcArbConfig;
use serde::{Deserialize, Serialize};

/// The wire format version of the replay protocol.
pub const SCHEMA_VERSION: &str = "btc-arb-replay/1";
/// The only symbol this protocol covers.
pub const REPLAY_SYMBOL: &str = "BTCUSDT";
/// The round length this protocol covers, in milliseconds.
pub const ROUND_MS: i64 = 300_000;
/// The proxy horizon, in seconds.
pub const PROXY_HORIZON_SECS: u32 = 300;

/// The frozen original half-life and warm-up the configuration must carry.
const FROZEN_HALF_LIFE_SECS: f64 = super::super::VOL_HALF_LIFE_SECS;
const FROZEN_MIN_VOL_SAMPLES: u32 = super::super::MIN_VOL_SAMPLES;

// ---------------------------------------------------------------- config ---

/// The offline parameters the driver runs with.
///
/// These are the original strategy's parameters, written out explicitly so a
/// run records what it used. They are not knobs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    pub schema_version: String,
    pub min_edge: f64,
    pub order_usd: f64,
    pub min_secs_left: i64,
    pub vol_floor: f64,
    pub half_life_secs: f64,
    pub min_vol_samples: u32,
}

impl ReplayConfig {
    /// Check the frozen values and the shapes a number must have.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "config.schemaVersion: expected {SCHEMA_VERSION}, got {}",
                self.schema_version
            ));
        }
        for (name, value) in [
            ("min_edge", self.min_edge),
            ("order_usd", self.order_usd),
            ("vol_floor", self.vol_floor),
            ("half_life_secs", self.half_life_secs),
        ] {
            if !value.is_finite() {
                return Err(format!("config.{name}: finite number required"));
            }
        }
        if self.min_edge < 0.0 || self.min_edge > 1.0 {
            return Err("config.min_edge: outside [0, 1]".to_string());
        }
        if self.order_usd <= 0.0 {
            return Err("config.order_usd: positive number required".to_string());
        }
        if self.min_secs_left < 0 {
            return Err("config.min_secs_left: non-negative integer required".to_string());
        }
        if self.vol_floor < 0.0 {
            return Err("config.vol_floor: non-negative number required".to_string());
        }
        if self.min_edge != 0.03 || self.order_usd != 5.0
            || self.min_secs_left != 30 || self.vol_floor != 0.00005
        {
            return Err("config: only the original strategy values are accepted".to_string());
        }
        if self.half_life_secs != FROZEN_HALF_LIFE_SECS {
            return Err(format!(
                "config.half_life_secs: only the original value {FROZEN_HALF_LIFE_SECS} is accepted"
            ));
        }
        if self.min_vol_samples != FROZEN_MIN_VOL_SAMPLES {
            return Err(format!(
                "config.min_vol_samples: only the original value {FROZEN_MIN_VOL_SAMPLES} is accepted"
            ));
        }
        Ok(())
    }

    /// The strategy configuration this maps onto.
    pub(super) fn strategy_config(&self) -> BtcArbConfig {
        BtcArbConfig {
            symbols: vec![REPLAY_SYMBOL.to_string()],
            round_interval_secs: None,
            min_edge: self.min_edge,
            order_usd: self.order_usd,
            min_secs_left: self.min_secs_left,
            vol_floor: self.vol_floor,
        }
    }
}

/// Which stream the driver is walking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    Replay,
    Proxy,
}

impl ReplayMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplayMode::Replay => "replay",
            ReplayMode::Proxy => "proxy",
        }
    }
}

// --------------------------------------------------------------- protocol ---

/// The evidence class of a stream. One stream carries one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceType {
    Fixture,
    RealProxy,
    RealMarket,
}

impl EvidenceType {
    pub fn as_str(self) -> &'static str {
        match self {
            EvidenceType::Fixture => "fixture",
            EvidenceType::RealProxy => "real-proxy",
            EvidenceType::RealMarket => "real-market",
        }
    }
}

/// The kind of event a row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Market,
    State,
    Spot,
    Book,
    Tick,
    Value,
    Result,
    Label,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Market => "market",
            EventType::State => "state",
            EventType::Spot => "spot",
            EventType::Book => "book",
            EventType::Tick => "tick",
            EventType::Value => "value",
            EventType::Result => "result",
            EventType::Label => "label",
        }
    }
}

/// The status of a local execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    Blocked,
    Filled,
    Unknown,
}

/// The status of a settlement label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelStatus {
    Settled,
    Pending,
    Disputed,
    Tie,
}

/// Deserialize a field that must be present but may be null.
///
/// Plain `Option<T>` fields are silently defaulted to `None` when the key is
/// absent, which would let a missing field pass as a known null.
fn present_option<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// One event envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope {
    pub schema_version: String,
    pub event_id: String,
    #[serde(deserialize_with = "present_option")]
    pub event_at_ms: Option<i64>,
    pub available_at_ms: i64,
    pub seq: u64,
    pub source: String,
    pub evidence_type: EvidenceType,
    pub payload: Payload,
}

/// The payload of an event, tagged by its type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Market(MarketPayload),
    State(StatePayload),
    Spot(SpotPayload),
    Book(BookPayload),
    Tick(TickPayload),
    Value(ValuePayload),
    Result(ResultPayload),
    Label(LabelPayload),
}

impl Payload {
    pub fn event_type(&self) -> EventType {
        match self {
            Payload::Market(_) => EventType::Market,
            Payload::State(_) => EventType::State,
            Payload::Spot(_) => EventType::Spot,
            Payload::Book(_) => EventType::Book,
            Payload::Tick(_) => EventType::Tick,
            Payload::Value(_) => EventType::Value,
            Payload::Result(_) => EventType::Result,
            Payload::Label(_) => EventType::Label,
        }
    }
}

/// A market's immutable identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketPayload {
    pub market_id: u64,
    pub symbol: String,
    pub starts_at_ms: i64,
    pub ends_at_ms: i64,
    pub yes_token: String,
    pub no_token: String,
    pub yes_name: String,
    pub no_name: String,
    pub rules_source: String,
}

/// A market's state at the time it was available.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatePayload {
    pub market_id: u64,
    pub open: bool,
    pub contested: bool,
    pub paused: bool,
    #[serde(deserialize_with = "present_option")]
    pub start_price: Option<f64>,
    #[serde(deserialize_with = "present_option")]
    pub fee_rate_bps: Option<u32>,
}

/// One spot observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotPayload {
    pub symbol: String,
    pub price: f64,
    pub model_source: String,
}

/// One book snapshot for the Yes token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookPayload {
    pub market_id: u64,
    pub yes_token: String,
    pub bids: Vec<BookLevelPayload>,
    pub asks: Vec<BookLevelPayload>,
}

/// One price level of a book.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookLevelPayload {
    pub price: f64,
    pub size: f64,
}

/// A request to decide on a market.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickPayload {
    pub market_id: u64,
}

/// A request to value the underlying.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuePayload {
    pub symbol: String,
    pub strike: f64,
    pub horizon_secs: u32,
}

/// The local result of a candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultPayload {
    pub market_id: u64,
    pub candidate_event_id: String,
    pub status: ResultStatus,
    #[serde(deserialize_with = "present_option")]
    pub filled_shares: Option<f64>,
    #[serde(deserialize_with = "present_option")]
    pub detail: Option<String>,
}

/// A settlement label, for after-the-fact evaluation only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelPayload {
    pub market_id: u64,
    pub yes_token: String,
    pub status: LabelStatus,
    #[serde(deserialize_with = "present_option")]
    pub payout_yes: Option<f64>,
    pub official_source: String,
    pub authority_report_ref: String,
    #[serde(deserialize_with = "present_option")]
    pub published_at_ms: Option<i64>,
}

// ----------------------------------------------------------------- output ---

/// One decision row, in the frozen field order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutputRow {
    pub schema_version: String,
    pub event_id: String,
    pub event_at_ms: Option<i64>,
    pub available_at_ms: i64,
    pub seq: u64,
    pub evidence_type: EvidenceType,
    pub source: String,
    pub event_type: EventType,
    pub market_id: Option<u64>,
    pub yes_token: Option<String>,
    pub no_token: Option<String>,
    pub symbol: Option<String>,
    pub samples: Option<u32>,
    pub sigma: Option<f64>,
    pub p_up: Option<f64>,
    pub candidate: Option<bool>,
    pub token: Option<String>,
    pub price: Option<f64>,
    pub fair: Option<f64>,
    pub edge: Option<f64>,
    pub result_status: Option<ResultStatus>,
    pub filled_shares: Option<f64>,
    pub entered_before: Option<bool>,
    pub entered_after: Option<bool>,
    pub reason: String,
}

/// What a run produced.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplaySummary {
    pub schema_version: String,
    pub mode: String,
    pub completed: bool,
    pub event_count: u64,
    pub output_count: u64,
    pub duplicate_count: u64,
    pub spot_duplicate_count: u64,
    pub candidate_count: u64,
    pub positive_fill_count: u64,
    pub unconfirmed_count: u64,
    pub unknown_result_count: u64,
    pub missing_reasons: Vec<String>,
    pub exit_code: u8,
}

impl ReplaySummary {
    pub(super) fn new(mode: ReplayMode) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            mode: mode.as_str().to_string(),
            completed: false,
            event_count: 0,
            output_count: 0,
            duplicate_count: 0,
            spot_duplicate_count: 0,
            candidate_count: 0,
            positive_fill_count: 0,
            unconfirmed_count: 0,
            unknown_result_count: 0,
            missing_reasons: Vec::new(),
            exit_code: 0,
        }
    }
}

/// Why a run failed, and what it had produced when it did.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayError {
    pub exit_code: u8,
    pub code: String,
    /// The one-based input line the failure was found on; 0 before the input.
    pub line: u64,
    pub partial: ReplaySummary,
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (line {}): {}", self.code, self.line, self.exit_code)
    }
}

impl std::error::Error for ReplayError {}

/// Exit code for a contract failure in the input stream.
pub const EXIT_CONTRACT: u8 = 2;
/// Exit code for a legal input that lacks the evidence to decide.
pub const EXIT_EVIDENCE: u8 = 3;
/// Exit code for an output or input I/O failure.
pub const EXIT_IO: u8 = 5;
