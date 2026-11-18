//! Label-free phase1 research adapter. No broker, book or application configuration.

use super::{DecisionCore, phase1::{Reason, ResetReason}};
use crate::config::BtcArbConfig;
use crate::service::observation::{CheckTime, ObservationClock, SourceTimeStatus, FreshnessBasis};
use crate::service::spot::SpotSource;
use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};

pub const SCHEMA: &str = "btc-arb-phase1-history/1";
const SECOND: i64 = 1_000_000;
const ROUND: i64 = 300 * SECOND;
const OFFSETS: [u32; 6] = [30, 60, 120, 180, 240, 270];

// deserialize_with makes a nullable field required: absent and null differ.
fn nullable<'de, D, T>(decoder: D) -> Result<Option<T>, D::Error>
where D: Deserializer<'de>, T: Deserialize<'de> { Option::<T>::deserialize(decoder) }

/// Isolated research parameters. Production configuration is never mutated.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: String,
    pub symbol: String,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub as_of_us: i64,
    pub sample_seconds: u32,
    pub half_life_secs: u32,
    pub min_vol_samples: u32,
    pub vol_floor: f64,
    pub clock_basis: String,
}

impl Config {
    /// Reject all parameter freedom except the three frozen research floors.
    pub fn validate(&self) -> Result<(), String> {
        let cutoff_valid = match self.schema_version.as_str() {
            "btc-arb-phase1-research-config/1" => self.as_of_us == 1_794_927_600_000_000,
            "btc-arb-phase1-research-config/2" => self.as_of_us >= 0,
            _ => false,
        };
        if !cutoff_valid
            || self.symbol != "BTCUSDT" || self.window_start_us < 0
            || self.window_start_us % ROUND != 0 || self.window_end_us % ROUND != 0
            || self.window_end_us <= self.window_start_us
            || self.window_end_us > self.as_of_us
            || self.sample_seconds != 5 || self.half_life_secs != 300
            || self.min_vol_samples != 20 || self.clock_basis != "event_time_proxy"
            || ![0.000025, 0.00005, 0.0001].contains(&self.vol_floor) {
            return Err("research config invalid".into());
        }
        Ok(())
    }
}

/// Strict label-free event. All UTC clocks are integer microseconds.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema_version: String,
    pub at_us: i64,
    pub seq: u64,
    pub payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    Spot { symbol: String, price: f64, bar_open_us: i64, close_exclusive_us: i64,
        available_at_us: i64, prediction_source: String, actual_pair: String,
        source_dataset: String, price_kind: String, clock_basis: String },
    Value { symbol: String, planned_start_us: i64, offset: u32,
        #[serde(deserialize_with = "nullable")] round_id: Option<String>,
        #[serde(deserialize_with = "nullable")] starts_at_us: Option<i64>,
        #[serde(deserialize_with = "nullable")] ends_at_us: Option<i64>,
        #[serde(deserialize_with = "nullable")] identity_available_at_us: Option<i64>,
        #[serde(deserialize_with = "nullable")] strike: Option<f64>,
        #[serde(deserialize_with = "nullable")] strike_available_at_us: Option<i64> },
}

/// Raw valuation; nullable fields remain explicit, without labels or orders.
#[derive(Debug, Clone, Serialize)]
pub struct Value {
    pub schema_version: &'static str,
    pub seq: u64,
    pub planned_start_us: i64,
    pub offset: u32,
    pub at_us: i64,
    pub round_id: Option<String>,
    pub starts_at_us: Option<i64>,
    pub ends_at_us: Option<i64>,
    pub identity_available_at_us: Option<i64>,
    pub strike_available_at_us: Option<i64>,
    pub strike: Option<f64>,
    pub spot: Option<f64>,
    pub bar_open_us: Option<i64>,
    pub close_exclusive_us: Option<i64>,
    pub spot_available_at_us: Option<i64>,
    pub proxy_observation_at_us: Option<i64>,
    pub logical_elapsed_ms: Option<u64>,
    pub prediction_source: Option<String>,
    pub actual_pair: Option<String>,
    pub source_dataset: Option<String>,
    pub price_kind: Option<String>,
    pub clock_basis: &'static str,
    pub source_epoch: Option<u64>,
    pub reset_reason: ResetReason,
    pub samples: u32,
    pub sigma_raw: Option<f64>,
    pub sigma: Option<f64>,
    pub floor_hit: Option<bool>,
    pub secs_left: Option<f64>,
    pub p_up: Option<f64>,
    pub valuation_status: &'static str,
}

#[derive(Clone)]
struct Spot {
    at: i64, price: f64, source: String, pair: String, dataset: String,
}

#[derive(Clone)]
struct RoundFacts {
    planned: i64, id: Option<String>, starts: Option<i64>, ends: Option<i64>,
    identity_at: Option<i64>, strike: Option<u64>, strike_at: Option<i64>, offset: u32,
}

/// Continuous single-symbol state, at most one active round, across all days.
pub struct History {
    config: Config,
    core: DecisionCore,
    last: Option<(i64, u64, bool)>,
    spot: Option<Spot>,
    round: Option<RoundFacts>,
}

impl History {
    /// Construct isolated phase1 state after validating all research parameters.
    pub fn new(config: Config) -> Result<Self, String> {
        config.validate()?;
        Ok(Self { config, core: DecisionCore::phase1(BtcArbConfig::default()),
            last: None, spot: None, round: None })
    }

    /// Apply one ordered event. A rejected stream must not be resumed.
    pub fn apply(&mut self, event: Event) -> Result<Option<Value>, String> {
        let is_value = matches!(event.payload, Payload::Value { .. });
        if event.schema_version != SCHEMA || event.at_us <= self.config.window_start_us
            || event.at_us > self.config.window_end_us || event.at_us % (5 * SECOND) != 0
            || event.seq != self.last.map_or(0, |(_, seq, _)| seq + 1)
            || self.last.is_some_and(|(at, _, valued)| event.at_us < at
                || (at == event.at_us && (valued || !is_value))) {
            return Err("event version/order/window invalid".into());
        }
        let at = event.at_us;
        let result = match event.payload {
            Payload::Spot { symbol, price, bar_open_us, close_exclusive_us, available_at_us,
                prediction_source, actual_pair, source_dataset, price_kind, clock_basis } => {
                if symbol != "BTCUSDT" || !positive(price)
                    || bar_open_us.checked_add(SECOND) != Some(at)
                    || close_exclusive_us != at || available_at_us != at
                    || prediction_source != "Binance" || actual_pair != "BTCUSDT"
                    || source_dataset != "binance-vision" || price_kind != "trade_close"
                    || clock_basis != "event_time_proxy" {
                    return Err("spot facts invalid".into());
                }
                let (clock, now) = self.proxy_clock(at)?;
                self.core.observe_phase1("BTCUSDT", price, SpotSource::Binance,
                    &actual_pair, &clock, now).map_err(|_| "observation rejected".to_string())?;
                self.spot = Some(Spot { at, price, source: prediction_source,
                    pair: actual_pair, dataset: source_dataset });
                None
            }
            Payload::Value { symbol, planned_start_us, offset, round_id, starts_at_us,
                ends_at_us, identity_available_at_us, strike, strike_available_at_us } => {
                if symbol != "BTCUSDT" || planned_start_us < self.config.window_start_us
                    || planned_start_us >= self.config.window_end_us || planned_start_us % ROUND != 0
                    || !OFFSETS.contains(&offset)
                    || planned_start_us.checked_add(offset as i64 * SECOND) != Some(at)
                    || round_id.as_ref().is_some_and(|id| id.is_empty())
                    || strike.is_some_and(|price| !positive(price))
                    || [starts_at_us, ends_at_us, identity_available_at_us, strike_available_at_us]
                        .iter().flatten().any(|time| *time < 0) {
                    return Err("value facts invalid".into());
                }
                let facts = RoundFacts { planned: planned_start_us, id: round_id.clone(),
                    starts: starts_at_us, ends: ends_at_us, identity_at: identity_available_at_us,
                    strike: strike.map(f64::to_bits), strike_at: strike_available_at_us, offset };
                if let Some(old) = &self.round {
                    if planned_start_us < old.planned || (planned_start_us == old.planned
                        && (offset <= old.offset || conflicts(old, &facts))) {
                        return Err("round order/identity/strike conflict".into());
                    }
                }
                self.round = Some(merge_facts(self.round.as_ref(), facts));
                let spot = self.spot.as_ref().filter(|spot| spot.at == at);
                let secs_left = ends_at_us.map(|ends| (ends - at) as f64 / SECOND as f64);
                let mut value = Value { schema_version: "btc-arb-phase1-value/1", seq: event.seq,
                    planned_start_us, offset, at_us: at, round_id, starts_at_us, ends_at_us,
                    identity_available_at_us, strike_available_at_us, strike,
                    spot: spot.map(|s| s.price), bar_open_us: spot.map(|s| s.at - SECOND),
                    close_exclusive_us: spot.map(|s| s.at), spot_available_at_us: spot.map(|s| s.at),
                    proxy_observation_at_us: spot.map(|s| s.at),
                    logical_elapsed_ms: spot.map(|s| ((s.at - self.config.window_start_us) / 1000) as u64),
                    prediction_source: spot.map(|s| s.source.clone()),
                    actual_pair: spot.map(|s| s.pair.clone()), source_dataset: spot.map(|s| s.dataset.clone()),
                    price_kind: spot.map(|_| "trade_close".into()), clock_basis: "event_time_proxy",
                    source_epoch: self.core.epoch("BTCUSDT"), reset_reason: self.core.reset_reason("BTCUSDT"),
                    samples: self.core.samples("BTCUSDT"), sigma_raw: None, sigma: None,
                    floor_hit: None, secs_left, p_up: None, valuation_status: "INPUT_INVALID" };
                value.valuation_status = if value.round_id.is_none() || identity_available_at_us.is_none()
                    || starts_at_us.is_none() || ends_at_us.is_none() { "MISSING_IDENTITY" }
                    else if identity_available_at_us.unwrap() > at { "IDENTITY_NOT_AVAILABLE" }
                    else if ends_at_us.unwrap().checked_sub(starts_at_us.unwrap()) != Some(ROUND)
                        || starts_at_us.unwrap() != planned_start_us
                        || starts_at_us.unwrap() > at || secs_left.unwrap() < 30.0 { "ROUND_INVALID" }
                    else if strike.is_none() || strike_available_at_us.is_none() { "MISSING_STRIKE" }
                    else if strike_available_at_us.unwrap() > at { "STRIKE_NOT_AVAILABLE" }
                    else if strike_available_at_us.unwrap() < starts_at_us.unwrap() { "ROUND_INVALID" }
                    else if spot.is_none() { "MISSING_SPOT" }
                    else {
                        match self.core.phase1_valuation("BTCUSDT", spot.unwrap().price,
                            strike.unwrap(), secs_left.unwrap(), self.config.vol_floor) {
                            Ok(model) => {
                                value.sigma_raw = Some(model.sigma_raw); value.sigma = Some(model.sigma);
                                value.floor_hit = Some(model.sigma_raw <= self.config.vol_floor);
                                value.p_up = Some(model.p_up); "VALUED"
                            }
                            Err(Reason::Warmup) => "WARMUP",
                            Err(_) => "INPUT_INVALID",
                        }
                    };
                Some(value)
            }
        };
        self.last = Some((at, event.seq, is_value));
        Ok(result)
    }

    /// Peak active-round count: a started round is retained until its successor.
    pub fn max_active_rounds(&self) -> usize { usize::from(self.round.is_some()) }

    fn proxy_clock(&self, at: i64) -> Result<(ObservationClock, CheckTime), String> {
        let utc = DateTime::from_timestamp_micros(at).ok_or("UTC out of range")?;
        let elapsed_ms = ((at - self.config.window_start_us) / 1000) as u64;
        Ok((ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound, request_at: utc, received_at: utc,
            request_elapsed_ms: elapsed_ms, received_elapsed_ms: elapsed_ms }, CheckTime { utc, elapsed_ms }))
    }
}

fn positive(value: f64) -> bool { value.is_finite() && value > 0.0 }
fn changed<T: PartialEq>(old: &Option<T>, new: &Option<T>) -> bool {
    old.as_ref().zip(new.as_ref()).is_some_and(|(left, right)| left != right)
}
fn conflicts(old: &RoundFacts, new: &RoundFacts) -> bool {
    changed(&old.id, &new.id) || changed(&old.starts, &new.starts) || changed(&old.ends, &new.ends)
        || changed(&old.identity_at, &new.identity_at) || changed(&old.strike, &new.strike)
        || changed(&old.strike_at, &new.strike_at)
}
fn merge_facts(old: Option<&RoundFacts>, mut new: RoundFacts) -> RoundFacts {
    if let Some(old) = old.filter(|old| old.planned == new.planned) {
        new.id = new.id.or_else(|| old.id.clone()); new.starts = new.starts.or(old.starts);
        new.ends = new.ends.or(old.ends); new.identity_at = new.identity_at.or(old.identity_at);
        new.strike = new.strike.or(old.strike); new.strike_at = new.strike_at.or(old.strike_at);
    }
    new
}

#[cfg(test)]
mod tests {
    use super::{Config, Event, History, Payload, SCHEMA, SECOND, ROUND};
    use super::super::{DecisionCore, phase1::StrategyContract};
    use crate::config::BtcArbConfig;
    use crate::service::observation::{CheckTime, ObservationClock, SourceTimeStatus, FreshnessBasis};
    use crate::service::spot::{SpotSource, digital_up};
    use chrono::{TimeZone, Utc};

    fn config() -> Config {
        Config { schema_version: "btc-arb-phase1-research-config/1".into(), symbol: "BTCUSDT".into(),
            window_start_us: 1_780_000_200_000_000, window_end_us: 1_780_086_600_000_000,
            as_of_us: 1_794_927_600_000_000, sample_seconds: 5, half_life_secs: 300,
            min_vol_samples: 20, vol_floor: 0.00005, clock_basis: "event_time_proxy".into() }
    }
    fn spot(start: i64, second: i64, seq: u64) -> Event {
        let at = start + second * SECOND;
        Event { schema_version: SCHEMA.into(), at_us: at, seq,
            payload: Payload::Spot { symbol: "BTCUSDT".into(), price: 60_000.0,
                bar_open_us: at - SECOND, close_exclusive_us: at, available_at_us: at,
                prediction_source: "Binance".into(), actual_pair: "BTCUSDT".into(),
                source_dataset: "binance-vision".into(), price_kind: "trade_close".into(),
                clock_basis: "event_time_proxy".into() } }
    }
    fn value(start: i64, offset: u32, seq: u64) -> Event {
        Event { schema_version: SCHEMA.into(), at_us: start + offset as i64 * SECOND, seq,
            payload: Payload::Value { symbol: "BTCUSDT".into(), planned_start_us: start, offset,
                round_id: Some(format!("official-{start}")), starts_at_us: Some(start),
                ends_at_us: Some(start + ROUND), identity_available_at_us: Some(start),
                strike: Some(60_000.0), strike_available_at_us: Some(start) } }
    }

    #[test]
    fn strict_wire_rejects_labels_missing_nullable_duplicates_and_bool_seq() {
        let good = serde_json::to_string(&value(config().window_start_us, 30, 0)).unwrap();
        assert!(serde_json::from_str::<Event>(&good).is_ok());
        for invalid in [good.replace("\"seq\":0", "\"seq\":true"),
            good.replace("\"seq\":0", "\"seq\":0,\"seq\":0"),
            good.replace("\"strike\":60000.0,", ""),
            good.replace("\"strike\":60000.0", "\"strike\":60000.0,\"strike\":60000.0"),
            good.replace("\"strike\":60000.0", "\"strike\":60000.0,\"label\":\"UP\"")] {
            assert!(serde_json::from_str::<Event>(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn microsecond_availability_and_missing_spot_outrank_warmup() {
        let cfg = config(); let start = cfg.window_start_us;
        let mut history = History::new(cfg).unwrap();
        let mut event = value(start, 30, 0);
        if let Payload::Value { identity_available_at_us, .. } = &mut event.payload {
            *identity_available_at_us = Some(event.at_us + 1);
        }
        assert_eq!(history.apply(event).unwrap().unwrap().valuation_status, "IDENTITY_NOT_AVAILABLE");
        // The same round's availability is immutable; a changed timestamp fails.
        assert!(history.apply(value(start, 60, 1)).is_err());
        let mut history = History::new(config()).unwrap();
        assert_eq!(history.apply(value(start, 30, 0)).unwrap().unwrap().valuation_status, "MISSING_SPOT");
        history.apply(spot(start, 60, 1)).unwrap();
        assert_eq!(history.apply(value(start, 60, 2)).unwrap().unwrap().valuation_status, "WARMUP");
    }

    #[test]
    fn continuous_warmup_round_eviction_and_gap_boundary() {
        let mut history = History::new(config()).unwrap();
        let start = config().window_start_us;
        for index in 1..=24 { history.apply(spot(start, index * 5, index as u64 - 1)).unwrap(); }
        let result = history.apply(value(start, 120, 24)).unwrap().unwrap();
        assert_eq!(result.samples, 23); assert_eq!(result.valuation_status, "VALUED");
        history.apply(spot(start, 420, 25)).unwrap(); // Exactly 300s does not reset.
        let result = history.apply(value(start + ROUND, 120, 26)).unwrap().unwrap();
        assert_eq!(result.samples, 24); assert_eq!(history.max_active_rounds(), 1);
        history.apply(spot(start, 725, 27)).unwrap();
        assert_eq!(history.core.samples("BTCUSDT"), 0);
        assert_eq!(history.core.epoch("BTCUSDT"), Some(2));
    }

    #[test]
    fn explicit_cutoff_v2_accepts_new_dates_without_loosening_v1() {
        let mut cfg = config();
        cfg.window_start_us = 1_795_046_400_000_000; // 2026-11-19 UTC
        cfg.window_end_us = cfg.window_start_us + 14 * 86_400 * SECOND;
        cfg.as_of_us = cfg.window_end_us;
        assert!(History::new(cfg.clone()).is_err()); // v1 remains fixed.
        cfg.schema_version = "btc-arb-phase1-research-config/2".into();
        assert!(History::new(cfg.clone()).is_ok());
        cfg.as_of_us -= 1;
        assert!(History::new(cfg.clone()).is_err());
        cfg.as_of_us = cfg.window_end_us;
        cfg.vol_floor = 0.00003;
        assert!(History::new(cfg.clone()).is_err());
        cfg.vol_floor = 0.00005;
        cfg.schema_version = "btc-arb-phase1-research-config/3".into();
        assert!(History::new(cfg).is_err());
    }

    #[test]
    fn stream_order_and_frozen_config_are_fail_closed() {
        let mut history = History::new(config()).unwrap(); let start = config().window_start_us;
        assert!(history.apply(spot(start, 5, 1)).is_err());
        history.apply(spot(start, 5, 0)).unwrap();
        assert!(history.apply(spot(start, 5, 1)).is_err());
        let mut cfg = config(); cfg.vol_floor = 0.00003;
        assert!(History::new(cfg).is_err());
        let mut cfg = config(); cfg.sample_seconds = 1;
        assert!(History::new(cfg).is_err());
    }

    fn observe(core: &mut DecisionCore, second: i64, price: f64, source: SpotSource) {
        let utc = Utc.timestamp_opt(1_700_000_000 + second, 0).unwrap();
        let elapsed_ms = second as u64 * 1000;
        let clock = ObservationClock { source_at: None,
            source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: utc, received_at: utc,
            request_elapsed_ms: elapsed_ms, received_elapsed_ms: elapsed_ms };
        let pair = if source == SpotSource::Binance { "BTCUSDT" } else { "BTC-USD" };
        core.observe_phase1("BTCUSDT", price, source, pair, &clock,
            CheckTime { utc, elapsed_ms }).unwrap();
    }

    #[test]
    fn pure_valuation_matches_phase1_and_keeps_legacy() {
        let legacy = DecisionCore::new(BtcArbConfig::default());
        let mut core = DecisionCore::phase1(BtcArbConfig::default());
        assert_eq!(legacy.contract(), StrategyContract::LegacyCashFee);
        for index in 0..21 {
            observe(&mut core, index * 5, 60_000.0 + index as f64, SpotSource::Binance);
            let value = core.phase1_valuation("BTCUSDT", 60_020.0, 60_000.0, 30.0, 0.00005);
            if index < 20 { assert!(value.is_err()); }
        }
        let value = core.phase1_valuation("BTCUSDT", 60_020.0, 60_000.0, 30.0, 0.00005).unwrap();
        assert_eq!(value.samples, 20);
        assert_eq!(value.sigma, core.phase1_sigma("BTCUSDT", 0.00005).unwrap());
        assert_eq!(value.sigma_raw, core.phase1_sigma("BTCUSDT", 0.0).unwrap());
        assert_eq!(value.p_up, digital_up(60_020.0, 60_000.0, value.sigma, 30.0));
        assert!(core.entered.is_empty());
        assert!(core.market_revisions.is_empty());
        assert_eq!(legacy.samples("BTCUSDT"), 0);

        // Only this production-path fixture supplies a book. Research never does.
        use super::super::phase1::{EvaluationInputs, SettlementProvider, Reason};
        use crate::models::{BookSnapshot, BookLevel};
        use crate::service::venue::TradableMarket;
        let now = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
        let market = TradableMarket { id: 1, open: true, contested: false, tick: 0.01,
            fee_rate_bps: 200, starts_at: Some(now - chrono::Duration::seconds(270)),
            ends_at: Some(now + chrono::Duration::seconds(30)), start_price: Some(60_000.0),
            feed_symbol: Some("BTCUSDT".into()), yes_token: "yes-1".into(),
            no_token: "no-1".into(), ..Default::default() };
        let book = BookSnapshot { bids: vec![BookLevel { price: 0.40, size: 100.0 }],
            asks: vec![BookLevel { price: 0.45, size: 100.0 }] };
        let clock = ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound, request_at: now, received_at: now,
            request_elapsed_ms: 100_000, received_elapsed_ms: 100_000 };
        let mut inputs = EvaluationInputs { market: &market, book: &book, spot_price: 60_020.0,
            spot_source: Some(SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
            spot_clock: Some(clock.clone()), book_clock: Some(clock),
            now: CheckTime { utc: now, elapsed_ms: 100_000 }, order_usd: 2.0,
            min_edge: 0.03, min_secs_left: 30.0, entered: false,
            identity_available_at: None, strike_available_at: None,
            settlement_provider: SettlementProvider::Unknown };
        let evaluated = core.evaluate_phase1(&inputs);
        assert_eq!(evaluated.diagnostic.sigma, Some(value.sigma));
        assert_eq!(evaluated.diagnostic.p_up, Some(value.p_up));
        assert_eq!(evaluated.diagnostic.samples, value.samples);
        inputs.book_clock = None;
        let refused = core.evaluate_phase1(&inputs);
        assert_eq!(refused.diagnostic.reason, Reason::MissingInput);
        assert!(refused.diagnostic.p_up.is_none());
    }

    #[test]
    fn research_floor_does_not_mutate_production_or_source_state() {
        let mut core = DecisionCore::phase1(BtcArbConfig::default());
        for index in 0..21 { observe(&mut core, index * 5, 60_000.0, SpotSource::Binance); }
        let low = core.phase1_valuation("BTCUSDT", 60_000.0, 60_000.0, 270.0, 0.000025).unwrap();
        let high = core.phase1_valuation("BTCUSDT", 60_000.0, 60_000.0, 270.0, 0.0001).unwrap();
        assert_eq!(low.sigma_raw, 0.0);
        assert_eq!(low.sigma, 0.000025);
        assert_eq!(high.sigma, 0.0001);
        assert_eq!(core.cfg.vol_floor, 0.00005);
        assert_eq!(core.samples("BTCUSDT"), 20);
        observe(&mut core, 105, 60_000.0, SpotSource::Coinbase);
        assert_eq!(core.samples("BTCUSDT"), 0);
        assert_eq!(core.epoch("BTCUSDT"), Some(2));
        assert!(core.phase1_valuation("BTCUSDT", 60_000.0, 60_000.0, 270.0, 0.00005).is_err());
    }
}
