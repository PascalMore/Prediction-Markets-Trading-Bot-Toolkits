//! Isolated, I/O-free official-history adapter to the unchanged decision core.
use super::{DecisionCore, replay::ReplayConfig};
use crate::config::BtcArbConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SCHEMA: &str = "btc-arb-history/1";
const SYMBOL: &str = "BTCUSDT";

/// Label-free history event. Millisecond clocks are UTC Unix clocks.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema_version: String,
    pub at_ms: i64,
    pub seq: u64,
    pub payload: Payload,
}

/// Only closed prices and official fixed-strike valuation requests are accepted.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    Spot { symbol: String, price: f64, available_at_ms: i64 },
    Value { symbol: String, round_id: String, starts_at_ms: i64, ends_at_ms: i64,
            strike: f64, identity_available_at_ms: i64, strike_available_at_ms: i64 },
}

/// One valuation, never a candidate, order or fill.
#[derive(Debug, Clone, Serialize)]
pub struct Value {
    pub round_id: String,
    pub at_ms: i64,
    pub starts_at_ms: i64,
    pub ends_at_ms: i64,
    pub strike: f64,
    pub spot: Option<f64>,
    pub spot_at_ms: Option<i64>,
    pub secs_left: f64,
    pub samples: u32,
    pub sigma: Option<f64>,
    pub p_up: Option<f64>,
    pub reason: String,
}

/// Continuous state for the entire stream; there is no day reset.
pub struct History {
    core: DecisionCore,
    clock: Option<(i64, u64, bool)>,
    spot: Option<(i64, f64)>,
    rounds: HashMap<String, (i64, i64, u64)>,
}

impl History {
    /// Validate frozen parameters without loading any application configuration.
    pub fn new(config: &ReplayConfig) -> Result<Self, String> {
        config.validate()?;
        let cfg = BtcArbConfig { symbols: vec![SYMBOL.into()], round_interval_secs: Some(300),
            min_edge: config.min_edge, order_usd: config.order_usd,
            min_secs_left: config.min_secs_left, vol_floor: config.vol_floor };
        Ok(Self { core: DecisionCore::new(cfg), clock: None, spot: None, rounds: HashMap::new() })
    }

    /// Apply one ordered event, returning rows only for value requests.
    pub fn apply(&mut self, event: Event) -> Result<Option<Value>, String> {
        if event.schema_version != SCHEMA || event.at_ms < 0 {
            return Err("schema or UTC clock invalid".into());
        }
        let is_value = matches!(event.payload, Payload::Value { .. });
        if let Some((at, seq, value_seen)) = self.clock {
            if event.at_ms < at || event.seq <= seq ||
                (event.at_ms == at && value_seen && !is_value) {
                return Err("clock/seq or spot-before-value order invalid".into());
            }
        }
        let now = event.at_ms;
        let result = match event.payload {
            Payload::Spot { symbol, price, available_at_ms } => {
                if symbol != SYMBOL || !positive(price) || available_at_ms != now ||
                    self.spot.is_some_and(|(at, _)| at >= now) {
                    return Err("spot symbol/price/availability invalid".into());
                }
                self.core.observe_spot(SYMBOL, price, now as f64 / 1000.0);
                self.spot = Some((now, price));
                None
            }
            Payload::Value { symbol, round_id, starts_at_ms, ends_at_ms, strike,
                             identity_available_at_ms, strike_available_at_ms } => {
                if symbol != SYMBOL || round_id.is_empty() || !positive(strike) ||
                    starts_at_ms < 0 || ends_at_ms.checked_sub(starts_at_ms) != Some(300_000) ||
                    now < starts_at_ms || now > ends_at_ms - 30_000 ||
                    identity_available_at_ms < 0 || strike_available_at_ms < starts_at_ms ||
                    identity_available_at_ms > now || strike_available_at_ms > now {
                    return Err("round identity/time/strike invalid".into());
                }
                let identity = (starts_at_ms, ends_at_ms, strike.to_bits());
                if self.rounds.get(&round_id).is_some_and(|old| old != &identity) {
                    return Err("round identity/strike changed".into());
                }
                self.rounds.insert(round_id.clone(), identity);
                let secs_left = (ends_at_ms - now) as f64 / 1000.0;
                let valuation = self.spot.and_then(|(_, price)| self.core.valuation(SYMBOL, price, strike, secs_left));
                let reason = if self.spot.is_none() { "missing_spot" }
                    else if valuation.is_none() { "warmup" } else { "valued" };
                Some(Value { round_id, at_ms: now, starts_at_ms, ends_at_ms, strike,
                    spot: self.spot.map(|(_, price)| price), spot_at_ms: self.spot.map(|(at, _)| at),
                    secs_left, samples: self.core.vol_samples(SYMBOL).unwrap_or(0),
                    sigma: self.core.sigma(SYMBOL), p_up: valuation.map(|v| v.p_up), reason: reason.into() })
            }
        };
        self.clock = Some((now, event.seq, is_value));
        Ok(result)
    }
}

fn positive(value: f64) -> bool { value.is_finite() && value > 0.0 }

#[cfg(test)]
mod tests;
