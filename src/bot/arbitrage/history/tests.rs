use super::*;
use serde_json::json;

fn config() -> ReplayConfig {
    serde_json::from_value(json!({"schema_version":"btc-arb-replay/1", "min_edge":0.03,
        "order_usd":5.0,"min_secs_left":30,"vol_floor":0.00005,
        "half_life_secs":300.0,"min_vol_samples":20})).unwrap()
}
fn spot(at: i64, seq: u64) -> Event {
    Event { schema_version: SCHEMA.into(), at_ms: at, seq,
        payload: Payload::Spot {symbol: SYMBOL.into(), price: 100.0 + seq as f64 * 0.001,
            available_at_ms: at} }
}
fn value(at: i64, seq: u64) -> Event {
    Event { schema_version: SCHEMA.into(), at_ms: at, seq,
        payload: Payload::Value {symbol: SYMBOL.into(), round_id:"official".into(),
            starts_at_ms:0, ends_at_ms:300_000, strike:99.9,
            identity_available_at_ms:0, strike_available_at_ms:0} }
}
#[test]
fn wire_roundtrip_and_injection() {
    for event in [spot(1000, 0), value(30_000, 1)] {
        let text = serde_json::to_value(&event).unwrap();
        assert!(serde_json::from_value::<Event>(text.clone()).is_ok());
        for field in ["book", "fee", "outcome", "end_price", "resolved_at", "rpc"] {
            let mut bad = text.clone(); bad[field] = json!(0);
            assert!(serde_json::from_value::<Event>(bad).is_err(), "{field}");
        }
    }
}
#[test]
fn twenty_returns_fixed_strike_and_real_horizons() {
    let mut history = History::new(&config()).unwrap();
    for index in 0..20 { history.apply(spot(1000 + index * 1000, index as u64)).unwrap(); }
    assert_eq!(history.apply(value(20_000, 20)).unwrap().unwrap().reason, "warmup");
    history.apply(spot(21_000, 21)).unwrap();
    for (at, seq) in [(30_000, 22), (180_000, 23), (270_000, 24)] {
        let row = history.apply(value(at, seq)).unwrap().unwrap();
        assert_eq!(row.samples, 20);
        assert_eq!(row.secs_left, (300_000-at) as f64/1000.0);
        let expected = history.core.valuation(SYMBOL, row.spot.unwrap(), 99.9, row.secs_left).unwrap();
        assert_eq!(row.p_up, Some(expected.p_up));
    }
}
#[test]
fn boundary_identity_clock_and_future_guards() {
    assert!(History::new(&config()).unwrap().apply(value(270_000,0)).is_ok());
    for at in [270_001, 300_000, -1] {
        assert!(History::new(&config()).unwrap().apply(value(at,0)).is_err());
    }
    let mut history = History::new(&config()).unwrap();
    history.apply(spot(30_000, 0)).unwrap();
    history.apply(value(30_000, 1)).unwrap();
    assert!(history.apply(spot(30_000, 2)).is_err());
    assert!(history.apply(value(29_000, 2)).is_err());
    assert!(history.apply(value(40_000, 1)).is_err());
    let mut changed = value(40_000, 2);
    if let Payload::Value {ref mut strike, ..} = changed.payload { *strike = 101.0; }
    assert!(history.apply(changed).is_err());
    let mut future = spot(1000, 0);
    if let Payload::Spot {ref mut available_at_ms, ..} = future.payload { *available_at_ms = 2000; }
    assert!(History::new(&config()).unwrap().apply(future).is_err());
    let mut future_identity = value(30_000,0);
    if let Payload::Value {ref mut identity_available_at_ms, ..} = future_identity.payload {
        *identity_available_at_ms = 31_000;
    }
    assert!(History::new(&config()).unwrap().apply(future_identity).is_err());
}
#[test]
fn continuous_across_utc_day_boundary() {
    let mut history = History::new(&config()).unwrap();
    for index in 0..61 { history.apply(spot(86_370_000 + index * 1000, index as u64)).unwrap(); }
    let mut request = value(86_430_000, 61);
    if let Payload::Value {ref mut starts_at_ms, ref mut ends_at_ms, ref mut strike_available_at_ms, ..} = request.payload {
        *starts_at_ms = 86_400_000; *ends_at_ms = 86_700_000; *strike_available_at_ms = 86_400_000;
    }
    let row = history.apply(request).unwrap().unwrap();
    assert_eq!(row.samples, 60);
    assert!(row.p_up.is_some());
}
