//! The wire types of the local replay protocol.
//!
//! This module owns the configuration, the event envelope, every payload, the
//! output row, the run summary and the error type. It has no I/O and no
//! decision logic: the driver owns the behaviour, the facade owns the stream.

use crate::config::BtcArbConfig;
use crate::service::observation::{FreshnessBasis, ObservationClock, SourceTimeStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The decision snapshot and the strategy contract of the net-share version.
pub use super::super::phase1::{DecisionDiagnostic, StrategyContract};

/// The wire format version of the replay protocol.
pub const SCHEMA_VERSION: &str = "btc-arb-replay/1";
/// The net-share wire format version of the replay protocol.
pub const SCHEMA_VERSION_V2: &str = "btc-arb-replay/2";
/// The order size the original version trades.
pub const REPLAY_ORDER_USD_V1: f64 = 5.0;
/// The order size the net-share version trades.
pub const REPLAY_ORDER_USD_V2: f64 = 2.0;
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
    /// The wire version this configuration selects.
    ///
    /// An unknown version is not a version: it is a refusal. The version is
    /// read before any input is read, so a stream is never probed for the
    /// economics it happens to look like.
    pub fn version(&self) -> Option<ReplayVersion> {
        match self.schema_version.as_str() {
            SCHEMA_VERSION => Some(ReplayVersion::V1),
            SCHEMA_VERSION_V2 => Some(ReplayVersion::V2),
            _ => None,
        }
    }

    /// Check the frozen values of one version.
    ///
    /// Each version has its own order size: the original trades five dollars
    /// a round, the net-share version trades two.
    fn validate_version(&self, version: ReplayVersion) -> Result<(), String> {
        let order_usd = match version {
            ReplayVersion::V1 => REPLAY_ORDER_USD_V1,
            ReplayVersion::V2 => REPLAY_ORDER_USD_V2,
        };
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
        if self.min_edge != 0.03
            || self.order_usd != order_usd
            || self.min_secs_left != 30
            || self.vol_floor != 0.00005
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

    /// Check the frozen values of the original version.
    ///
    /// The original version is the only one the history adapter accepts: it
    /// has no market and no execution, so it cannot speak the net-share
    /// version's economics.
    pub fn validate(&self) -> Result<(), String> {
        if self.version() != Some(ReplayVersion::V1) {
            return Err(format!(
                "config.schemaVersion: expected {SCHEMA_VERSION}, got {}",
                self.schema_version
            ));
        }
        self.validate_version(ReplayVersion::V1)
    }

    /// Check the configuration against the mode it will run in.
    ///
    /// The net-share version trades, so it cannot run in proxy mode: a proxy
    /// run has no market and no execution, and its whole point is that it
    /// cannot make a trade. An unknown version is refused before anything is
    /// read.
    pub fn validate_for(&self, mode: ReplayMode) -> Result<(), String> {
        match self.version() {
            Some(ReplayVersion::V1) => self.validate_version(ReplayVersion::V1),
            Some(ReplayVersion::V2) => {
                if mode == ReplayMode::Proxy {
                    return Err(
                        "config: the net-share version trades and cannot run in proxy mode"
                            .to_string(),
                    );
                }
                self.validate_version(ReplayVersion::V2)
            }
            None => Err(format!(
                "config.schemaVersion: expected {SCHEMA_VERSION} or {SCHEMA_VERSION_V2}, got {}",
                self.schema_version
            )),
        }
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

/// The wire version of the replay protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVersion {
    /// The original cash-fee version.
    V1,
    /// The net-share version.
    V2,
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

// --------------------------------------------------------------- version 2 ---

/// Whether the upstream source's own event time is known.
///
/// The wire carries the same two facts the shared clock does, in its own
/// spelling: the source's own time is either present or missing, and the
/// freshness is either receipt-bound or backed by the source's own time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceTimeStatusWire {
    Missing,
    Present,
}

/// What the freshness of an input can be proven from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FreshnessBasisWire {
    /// Only the local receipt can be proven; the upstream freshness is unknown.
    ReceiptBound,
    /// The source's own event time is known and bounded as well.
    SourceAndReceipt,
}

/// The clock of one acquired input, as the net-share version carries it.
///
/// The clock is the same shape the live path uses: the request that left,
/// the response that came back, and both elapsed readings. The source's own
/// event time is not invented: it is `null` with the matching status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockPayload {
    #[serde(deserialize_with = "present_option")]
    pub source_at: Option<String>,
    pub source_time_status: SourceTimeStatusWire,
    pub freshness_basis: FreshnessBasisWire,
    pub request_at: String,
    pub received_at: String,
    pub request_elapsed_ms: u64,
    pub received_elapsed_ms: u64,
}

impl ClockPayload {
    /// The clock in the shape the shared core reads.
    ///
    /// A clock whose two statuses disagree with each other, or with the
    /// source's own time, cannot be read: it is a refusal, not a guess.
    pub(super) fn to_observation(&self) -> Option<ObservationClock> {
        let source_at = match self.source_at.as_deref() {
            Some(at) => Some(clock_timestamp(at)?),
            None => None,
        };
        let (status, basis, wire_basis) = match (source_at, self.source_time_status) {
            (None, SourceTimeStatusWire::Missing) => (
                SourceTimeStatus::Missing,
                FreshnessBasis::ReceiptBound,
                FreshnessBasisWire::ReceiptBound,
            ),
            (Some(_), SourceTimeStatusWire::Present) => (
                SourceTimeStatus::Present,
                FreshnessBasis::SourceAndReceipt,
                FreshnessBasisWire::SourceAndReceipt,
            ),
            _ => return None,
        };
        if self.freshness_basis != wire_basis {
            return None;
        }
        Some(ObservationClock {
            source_at,
            source_time_status: status,
            freshness_basis: basis,
            request_at: clock_timestamp(&self.request_at)?,
            received_at: clock_timestamp(&self.received_at)?,
            request_elapsed_ms: self.request_elapsed_ms,
            received_elapsed_ms: self.received_elapsed_ms,
        })
    }
}

/// Read the SPEC §4.3 timestamp, including its required millisecond precision.
fn clock_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let (_, fractional) = value.split_once('.')?;
    if fractional.bytes().take_while(u8::is_ascii_digit).count() < 3 {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

/// The prediction source of one spot observation.
///
/// The pair is part of the identity: a USDT pair and a USD pair are not the
/// same price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PredictionSource {
    Binance,
    Coinbase,
}

impl PredictionSource {
    /// The pair this source actually serves.
    pub fn actual_pair(self) -> &'static str {
        match self {
            PredictionSource::Binance => "BTCUSDT",
            PredictionSource::Coinbase => "BTC-USD",
        }
    }
}

/// One spot observation of the net-share version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotPayloadV2 {
    pub symbol: String,
    pub price: f64,
    pub model_source: PredictionSource,
    /// The pair that was actually requested.
    pub actual_pair: String,
    pub clock: ClockPayload,
}

/// One book snapshot of the net-share version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookPayloadV2 {
    pub market_id: u64,
    pub yes_token: String,
    pub bids: Vec<BookLevelPayload>,
    pub asks: Vec<BookLevelPayload>,
    pub clock: ClockPayload,
}

/// The payload of a net-share event, tagged by its type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayloadV2 {
    Market(MarketPayload),
    State(StatePayload),
    Spot(SpotPayloadV2),
    Book(BookPayloadV2),
    Tick(TickPayload),
    Value(ValuePayload),
    Result(ResultPayload),
    Label(LabelPayload),
}

impl PayloadV2 {
    pub fn event_type(&self) -> EventType {
        match self {
            PayloadV2::Market(_) => EventType::Market,
            PayloadV2::State(_) => EventType::State,
            PayloadV2::Spot(_) => EventType::Spot,
            PayloadV2::Book(_) => EventType::Book,
            PayloadV2::Tick(_) => EventType::Tick,
            PayloadV2::Value(_) => EventType::Value,
            PayloadV2::Result(_) => EventType::Result,
            PayloadV2::Label(_) => EventType::Label,
        }
    }
}

/// One event envelope of the net-share version.
///
/// The original fields travel unchanged and the elapsed clock is added: it
/// is the process's own elapsed time when the row was consumed, and it never
/// goes backwards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelopeV2 {
    pub schema_version: String,
    pub event_id: String,
    #[serde(deserialize_with = "present_option")]
    pub event_at_ms: Option<i64>,
    pub available_at_ms: i64,
    pub seq: u64,
    pub source: String,
    pub evidence_type: EvidenceType,
    /// The process's elapsed clock when this row was consumed.
    pub elapsed_ms: u64,
    pub payload: PayloadV2,
}

/// One spot observation, whatever version it arrived in.
///
/// The original version carries no clock and no source pair; the net-share
/// version carries both. The driver reads the fields the version has and
/// refuses the decisions that need the ones it does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotEvent {
    pub symbol: String,
    pub price: f64,
    /// The source as the wire named it.
    pub model_source: String,
    /// The feed that answered, when the version carries one.
    pub source: Option<crate::service::spot::SpotSource>,
    /// The pair that was actually requested, when the version carries one.
    pub actual_pair: Option<String>,
    /// The clock that received it, when the version carries one.
    pub clock: Option<ObservationClock>,
}

/// One book snapshot, whatever version it arrived in.
#[derive(Debug, Clone, PartialEq)]
pub struct BookEvent {
    pub market_id: u64,
    pub yes_token: String,
    pub bids: Vec<BookLevelPayload>,
    pub asks: Vec<BookLevelPayload>,
    /// The clock that received it, when the version carries one.
    pub clock: Option<ObservationClock>,
}

/// The payload of one normalized event.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamPayload {
    Market(MarketPayload),
    State(StatePayload),
    Spot(SpotEvent),
    Book(BookEvent),
    Tick(TickPayload),
    Value(ValuePayload),
    Result(ResultPayload),
    Label(LabelPayload),
}

impl StreamPayload {
    /// The kind of event this payload carries.
    pub fn event_type(&self) -> EventType {
        match self {
            StreamPayload::Market(_) => EventType::Market,
            StreamPayload::State(_) => EventType::State,
            StreamPayload::Spot(_) => EventType::Spot,
            StreamPayload::Book(_) => EventType::Book,
            StreamPayload::Tick(_) => EventType::Tick,
            StreamPayload::Value(_) => EventType::Value,
            StreamPayload::Result(_) => EventType::Result,
            StreamPayload::Label(_) => EventType::Label,
        }
    }
}

/// One normalized event, whatever version it arrived in.
///
/// The two versions differ in what they carry, not in what they mean: the
/// driver sees one event shape and one state machine.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEvent {
    pub schema_version: String,
    pub event_id: String,
    pub event_at_ms: Option<i64>,
    pub available_at_ms: i64,
    pub seq: u64,
    pub source: String,
    pub evidence_type: EvidenceType,
    /// The process's elapsed clock when the row was consumed.
    pub elapsed_ms: u64,
    pub payload: StreamPayload,
}

impl EventEnvelope {
    /// The original envelope as a normalized event.
    ///
    /// The original version carries no clock: what it does not carry, the
    /// driver cannot use.
    pub fn normalize(self) -> StreamEvent {
        StreamEvent {
            schema_version: self.schema_version,
            event_id: self.event_id,
            event_at_ms: self.event_at_ms,
            available_at_ms: self.available_at_ms,
            seq: self.seq,
            source: self.source,
            evidence_type: self.evidence_type,
            elapsed_ms: 0,
            payload: match self.payload {
                Payload::Market(payload) => StreamPayload::Market(payload),
                Payload::State(payload) => StreamPayload::State(payload),
                Payload::Spot(payload) => StreamPayload::Spot(SpotEvent {
                    symbol: payload.symbol,
                    price: payload.price,
                    model_source: payload.model_source,
                    source: None,
                    actual_pair: None,
                    clock: None,
                }),
                Payload::Book(payload) => StreamPayload::Book(BookEvent {
                    market_id: payload.market_id,
                    yes_token: payload.yes_token,
                    bids: payload.bids,
                    asks: payload.asks,
                    clock: None,
                }),
                Payload::Tick(payload) => StreamPayload::Tick(payload),
                Payload::Value(payload) => StreamPayload::Value(payload),
                Payload::Result(payload) => StreamPayload::Result(payload),
                Payload::Label(payload) => StreamPayload::Label(payload),
            },
        }
    }
}

impl EventEnvelopeV2 {
    /// The net-share envelope as a normalized event.
    ///
    /// The clock of a spot or a book becomes the observation's own clock; a
    /// clock that cannot be read is a refusal, not a missing value.
    pub fn normalize(self) -> Result<StreamEvent, String> {
        let payload = match self.payload {
            PayloadV2::Market(payload) => StreamPayload::Market(payload),
            PayloadV2::State(payload) => StreamPayload::State(payload),
            PayloadV2::Spot(payload) => {
                let source = match payload.model_source {
                    PredictionSource::Binance => crate::service::spot::SpotSource::Binance,
                    PredictionSource::Coinbase => crate::service::spot::SpotSource::Coinbase,
                };
                StreamPayload::Spot(SpotEvent {
                    symbol: payload.symbol,
                    price: payload.price,
                    model_source: match payload.model_source {
                        PredictionSource::Binance => "Binance".to_string(),
                        PredictionSource::Coinbase => "Coinbase".to_string(),
                    },
                    source: Some(source),
                    actual_pair: Some(payload.actual_pair),
                    clock: Some(
                        payload
                            .clock
                            .to_observation()
                            .ok_or_else(|| "spot: the clock is not a UTC instant".to_string())?,
                    ),
                })
            }
            PayloadV2::Book(payload) => StreamPayload::Book(BookEvent {
                market_id: payload.market_id,
                yes_token: payload.yes_token,
                bids: payload.bids,
                asks: payload.asks,
                clock: Some(
                    payload
                        .clock
                        .to_observation()
                        .ok_or_else(|| "book: the clock is not a UTC instant".to_string())?,
                ),
            }),
            PayloadV2::Tick(payload) => StreamPayload::Tick(payload),
            PayloadV2::Value(payload) => StreamPayload::Value(payload),
            PayloadV2::Result(payload) => StreamPayload::Result(payload),
            PayloadV2::Label(payload) => StreamPayload::Label(payload),
        };
        Ok(StreamEvent {
            schema_version: self.schema_version,
            event_id: self.event_id,
            event_at_ms: self.event_at_ms,
            available_at_ms: self.available_at_ms,
            seq: self.seq,
            source: self.source,
            evidence_type: self.evidence_type,
            elapsed_ms: self.elapsed_ms,
            payload,
        })
    }
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

/// One decision row of the net-share version.
///
/// The original fields travel unchanged; the decision snapshot and the
/// strategy contract are added, and they are present on every row — `null`
/// where the row is not a decision.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutputRowV2 {
    #[serde(flatten)]
    pub base: OutputRow,
    pub decision: Option<DecisionDiagnostic>,
    pub strategy_contract: Option<StrategyContract>,
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
    /// The summary of a run, in the version the run used.
    pub(super) fn new(mode: ReplayMode, version: ReplayVersion) -> Self {
        let schema_version = match version {
            ReplayVersion::V1 => SCHEMA_VERSION,
            ReplayVersion::V2 => SCHEMA_VERSION_V2,
        };
        Self {
            schema_version: schema_version.to_string(),
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
