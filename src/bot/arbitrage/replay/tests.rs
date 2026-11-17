//! The test helpers and the first acceptance groups of the replay driver.
//!
//! The groups match the acceptance vectors the design freezes: warm-up
//! boundaries, identity and availability, duplicates and conflicts. The
//! state-machine groups live in the `state` submodule and reuse these helpers.

use super::*;

mod state;

// Keep phase1 coverage in the existing test file (Design §3.1/§3.6).
mod v2 {
    use super::*;
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};

    fn config_v2() -> ReplayConfig {
        ReplayConfig {
            schema_version: SCHEMA_VERSION_V2.into(),
            order_usd: 2.0,
            ..config()
        }
    }

    fn clock(at: i64, elapsed: u64) -> Value {
        let timestamp = crate::service::observation::format_millis(
            &DateTime::<Utc>::from_timestamp_millis(at).unwrap(),
        );
        json!({"source_at": null, "source_time_status": "MISSING",
            "freshness_basis": "RECEIPT_BOUND", "request_at": timestamp,
            "received_at": timestamp, "request_elapsed_ms": elapsed,
            "received_elapsed_ms": elapsed})
    }

    fn envelope(id: &str, at: i64, seq: u64, payload: Value) -> Value {
        json!({"schema_version": SCHEMA_VERSION_V2, "event_id": id,
            "event_at_ms": null, "available_at_ms": at, "seq": seq,
            "source": "test-fixture", "evidence_type": "fixture",
            "elapsed_ms": (at - T0) as u64, "payload": payload})
    }

    // The final warm-up price is current, not ten rounds older than the tick.
    fn prefix() -> Vec<Value> {
        let mut lines: Vec<_> = (0..21)
            .map(|index| {
                let elapsed = index * 5_000;
                let at = T0 + elapsed as i64;
                envelope(
                    &format!("spot-{index}"),
                    at,
                    index,
                    json!({"type": "spot", "symbol": REPLAY_SYMBOL,
                    "price": 100.0 + index as f64 * 0.01,
                    "model_source": "Binance", "actual_pair": "BTCUSDT",
                    "clock": clock(at, elapsed)}),
                )
            })
            .collect();
        lines.push(envelope(
            "market-1",
            T0 + 100_001,
            21,
            json!({"type": "market", "market_id": 1, "symbol": REPLAY_SYMBOL,
                "starts_at_ms": T0, "ends_at_ms": T0 + ROUND,
                "yes_token": "yes-1", "no_token": "no-1", "yes_name": "Up",
                "no_name": "Down", "rules_source": "test-rules"}),
        ));
        lines.push(envelope(
            "state-1",
            T0 + 100_002,
            22,
            json!({"type": "state", "market_id": 1, "open": true,
                "contested": false, "paused": false, "start_price": 100.0,
                "fee_rate_bps": 200}),
        ));
        lines.push(envelope(
            "book-1",
            T0 + 100_003,
            23,
            json!({"type": "book", "market_id": 1, "yes_token": "yes-1",
                "bids": [{"price": 0.4, "size": 50.0}],
                "asks": [{"price": 0.45, "size": 50.0}],
                "clock": clock(T0 + 100_003, 100_003)}),
        ));
        lines.push(envelope(
            "tick-1",
            T0 + 101_000,
            24,
            json!({"type": "tick", "market_id": 1}),
        ));
        lines
    }

    fn drive_with(
        config: ReplayConfig,
        mode: ReplayMode,
        lines: &[Value],
    ) -> (Result<ReplaySummary, ReplayError>, Vec<Value>) {
        let input: String = lines.iter().map(|line| format!("{line}\n")).collect();
        let mut output = Vec::new();
        let result = run(
            config,
            mode,
            T0,
            T0 + 100 * ROUND,
            std::io::Cursor::new(input),
            &mut output,
        );
        let rows = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        (result, rows)
    }

    fn drive(lines: &[Value]) -> (Vec<Value>, ReplaySummary) {
        let (result, rows) = drive_with(config_v2(), ReplayMode::Replay, lines);
        (rows, result.unwrap())
    }

    fn refused(lines: &[Value], code: &str, line: u64) {
        let (result, output) = drive_with(config_v2(), ReplayMode::Replay, lines);
        let error = result.unwrap_err();
        assert_eq!(
            (error.exit_code, error.code.as_str(), error.line),
            (EXIT_CONTRACT, code, line)
        );
        assert!(output.is_empty(), "invalid streams must not be written");
    }

    #[test]
    fn the_version_chooses_the_economics() {
        assert!(config().validate().is_ok());
        assert!(config_v2().validate_for(ReplayMode::Replay).is_ok());
        assert!(config_v2().validate().is_err());
        assert!(config().validate_for(ReplayMode::Proxy).is_ok());
        for (version, dollars, mode) in [
            (SCHEMA_VERSION_V2, 5.0, ReplayMode::Replay),
            (SCHEMA_VERSION, 2.0, ReplayMode::Replay),
            ("btc-arb-replay/3", 2.0, ReplayMode::Replay),
            (SCHEMA_VERSION_V2, 2.0, ReplayMode::Proxy),
        ] {
            let cfg = ReplayConfig {
                schema_version: version.into(),
                order_usd: dollars,
                ..config()
            };
            assert!(cfg.validate_for(mode).is_err());
            // Test the actual facade, not only the configuration helper.
            let (result, output) = drive_with(cfg, mode, &[]);
            assert_eq!(result.unwrap_err().line, 0);
            assert!(output.is_empty());
        }
    }

    #[test]
    fn a_mixed_stream_is_refused_before_anything_is_decided() {
        let original: Value = serde_json::from_str(&market_event("market-1", T0, 0, 1)).unwrap();
        refused(&[original], "json_invalid", 1);
        let stream = prefix();
        let mut wrong_version = stream[0].clone();
        wrong_version["schema_version"] = json!(SCHEMA_VERSION);
        let mut output = Vec::new();
        let error = run(
            config_v2(),
            ReplayMode::Replay,
            T0 + ROUND,
            T0 + 2 * ROUND,
            std::io::Cursor::new(wrong_version.to_string()),
            &mut output,
        )
        .unwrap_err();
        assert_eq!(error.code, "schema_invalid");
        assert!(
            output.is_empty(),
            "a window filter must not hide a mixed wire version"
        );
        let (result, output) = drive_with(config(), ReplayMode::Replay, &stream[..1]);
        assert!(result.is_err());
        assert!(output.is_empty());
        for mutation in [
            "elapsed_ms",
            "extra",
            "bool",
            "null",
            "clock-extra",
            "level-extra",
        ] {
            let mut lines = stream.clone();
            match mutation {
                "elapsed_ms" => {
                    lines[0].as_object_mut().unwrap().remove("elapsed_ms");
                }
                "extra" => lines[0]["extra"] = json!(1),
                "bool" => lines[0]["elapsed_ms"] = json!(true),
                "null" => lines[0]["payload"]["clock"] = Value::Null,
                "clock-extra" => lines[0]["payload"]["clock"]["extra"] = json!(1),
                "level-extra" => lines[23]["payload"]["asks"][0]["extra"] = json!(1),
                _ => unreachable!(),
            }
            let line = if mutation == "level-extra" { 24 } else { 1 };
            refused(&lines, "json_invalid", line);
        }
    }

    #[test]
    fn the_elapsed_clock_never_goes_backwards() {
        let mut lines = prefix();
        lines.push(envelope(
            "tick-2",
            T0 + 102_000,
            25,
            json!({"type": "tick", "market_id": 1}),
        ));
        lines[25]["elapsed_ms"] = json!(100_000);
        refused(&lines, "time_order", 26);
        let mut lines = prefix();
        lines[23]["payload"]["clock"]["received_at"] =
            clock(T0 + 100_004, 100_004)["received_at"].clone();
        refused(&lines, "time_order", 24);
        let mut lines = prefix();
        lines[23]["payload"]["clock"]["received_elapsed_ms"] = json!(100_004);
        refused(&lines, "time_order", 24);
        let mut lines = prefix();
        lines[23]["event_at_ms"] = json!(T0 + 100_003);
        refused(&lines, "time_order", 24);
    }

    #[test]
    fn the_source_and_the_pair_must_agree() {
        let mut lines = prefix();
        lines[0]["payload"]["actual_pair"] = json!("BTC-USD");
        refused(&lines, "identity_invalid", 1);
        let mut lines = prefix();
        for row in &mut lines[..21] {
            row["payload"]["model_source"] = json!("Coinbase");
            row["payload"]["actual_pair"] = json!("BTC-USD");
        }
        let (rows, _) = drive(&lines);
        assert_eq!(rows[24]["decision"]["source_state"], "DEGRADED");
        assert_eq!(rows[24]["decision"]["settlement_provider"], "UNKNOWN");
    }

    #[test]
    fn the_net_share_version_decides_with_the_shared_core() {
        let (rows, summary) = drive(&prefix());
        assert_eq!(rows.len(), 25);
        assert!(rows[21]["decision"].is_null());
        for row in &rows {
            assert_eq!(row["strategy_contract"], "phase1_net_shares");
            assert_eq!(row["schema_version"], SCHEMA_VERSION_V2);
        }
        let tick = &rows[24];
        let decision = &tick["decision"];
        assert_eq!(decision["reason"], "ELIGIBLE");
        assert_eq!(decision["samples"], 20);
        assert!(decision["sigma"].as_f64().unwrap().is_finite());
        assert!(decision["p_up"].as_f64().unwrap().is_finite());
        assert_eq!(decision["candidate_id"], decision["decision_id"]);
        assert!(decision["candidate_id"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .is_ok());
        assert_eq!(tick["candidate"], true);
        assert_eq!(summary.candidate_count, 1);
        assert_eq!(decision["up"]["gross_shares"], 4.44);
    }

    #[test]
    fn the_result_refers_to_the_core_candidate() {
        let lines = prefix();
        let (rows, _) = drive(&lines);
        let id = rows[24]["decision"]["candidate_id"].clone();
        for (named, shares, expected) in [
            (json!("tick-1"), 4.0, false),
            (id.clone(), 4.0, true),
            (id, 0.0, false),
        ] {
            let mut stream = lines.clone();
            stream.push(envelope(
                "result-1",
                T0 + 102_000,
                25,
                json!({"type": "result", "market_id": 1, "candidate_event_id": named,
                    "status": "filled", "filled_shares": shares, "detail": null}),
            ));
            if named == json!("tick-1") {
                refused(&stream, "result_invalid", 26);
            } else {
                let (rows, summary) = drive(&stream);
                assert_eq!(rows[25]["entered_before"], false);
                assert_eq!(rows[25]["entered_after"], expected);
                assert_eq!(summary.positive_fill_count, u64::from(expected));
                // A second terminal under a new event id is not idempotent.
                let mut repeated = stream.last().unwrap().clone();
                repeated["event_id"] = json!("result-2");
                repeated["available_at_ms"] = json!(T0 + 102_001);
                repeated["elapsed_ms"] = json!(102_001);
                repeated["seq"] = json!(26);
                stream.push(repeated);
                refused(&stream, "result_invalid", 27);
            }
        }
    }

    #[test]
    fn a_round_without_a_fee_is_refused() {
        let mut lines = prefix();
        lines[22]["payload"]["fee_rate_bps"] = Value::Null;
        let (rows, _) = drive(&lines);
        assert_eq!(rows[24]["decision"]["reason"], "MARKET_INVALID");
        assert!(rows[24]["decision"]["candidate_id"].is_null());
        assert_eq!(rows[24]["candidate"], false);
    }

    #[test]
    fn the_identity_and_the_strike_freeze_their_first_availability() {
        let mut lines = prefix();
        let mut market = lines[21].clone();
        market["event_id"] = json!("market-again");
        market["available_at_ms"] = json!(T0 + 100_004);
        market["elapsed_ms"] = json!(100_004);
        market["seq"] = json!(24);
        let mut state = lines[22].clone();
        state["event_id"] = json!("state-again");
        state["available_at_ms"] = json!(T0 + 100_005);
        state["elapsed_ms"] = json!(100_005);
        state["seq"] = json!(25);
        lines.insert(24, market);
        lines.insert(25, state);
        lines[26]["seq"] = json!(26);
        let (rows, _) = drive(&lines);
        let decision = &rows.last().unwrap()["decision"];
        for (field, ms) in [
            ("identity_available_at", T0 + 100_001),
            ("strike_available_at", T0 + 100_002),
        ] {
            let parsed = DateTime::parse_from_rfc3339(decision[field].as_str().unwrap()).unwrap();
            assert_eq!(parsed.with_timezone(&Utc).timestamp_millis(), ms);
        }
    }

    #[test]
    fn a_conflicting_fixed_strike_stays_invalid() {
        let mut lines = prefix();
        let mut changed = lines[22].clone();
        changed["event_id"] = json!("changed-strike");
        changed["available_at_ms"] = json!(T0 + 100_004);
        changed["elapsed_ms"] = json!(100_004);
        changed["seq"] = json!(24);
        changed["payload"]["start_price"] = json!(102.0);
        lines.insert(24, changed);
        lines[25]["seq"] = json!(25);
        let (rows, _) = drive(&lines);
        assert_eq!(rows[25]["decision"]["reason"], "MARKET_INVALID");
        assert!(rows[25]["decision"]["candidate_id"].is_null());
    }

    #[test]
    fn paused_and_unknown_results_cannot_offer_another_entry() {
        let mut paused = prefix();
        paused[22]["payload"]["paused"] = json!(true);
        let (rows, _) = drive(&paused);
        assert_eq!(rows[24]["candidate"], false);
        assert_eq!(rows[24]["decision"]["reason"], "MARKET_INVALID");
        let mut lines = prefix();
        let (rows, _) = drive(&lines);
        lines.push(envelope(
            "unknown-result",
            T0 + 102_000,
            25,
            json!({"type": "result", "market_id": 1,
                "candidate_event_id": rows[24]["decision"]["candidate_id"],
                "status": "unknown", "filled_shares": null, "detail": null}),
        ));
        lines.push(envelope(
            "tick-again",
            T0 + 103_000,
            26,
            json!({"type": "tick", "market_id": 1}),
        ));
        let (rows, summary) = drive(&lines);
        assert_eq!(rows[26]["candidate"], false);
        assert_eq!(rows[26]["entered_after"], false);
        assert_eq!(rows[26]["reason"], "result_unknown");
        assert_eq!(summary.candidate_count, 1);
    }

    #[test]
    fn malformed_clock_and_duplicate_recursive_keys_are_refused() {
        let value = prefix()[0].clone();
        let first = value.to_string();
        for field in [
            "schema_version",
            "price",
            "received_at",
            "source_at",
            "model_source",
            "type",
        ] {
            let needle = format!("\"{field}\":");
            // Insert a duplicate before the existing value, without parsing it
            // through Value (which would silently erase duplicate keys).
            let original = value
                .get(field)
                .or_else(|| value["payload"].get(field))
                .or_else(|| value["payload"]["clock"].get(field))
                .unwrap();
            let duplicate = first.replacen(&needle, &format!("{needle}{original},{needle}"), 1);
            let mut output = Vec::new();
            let error = run(
                config_v2(),
                ReplayMode::Replay,
                T0,
                T0 + ROUND,
                std::io::Cursor::new(duplicate),
                &mut output,
            )
            .unwrap_err();
            assert_eq!(error.exit_code, EXIT_CONTRACT, "{field}");
            assert!(output.is_empty());
        }
        let mut lines = prefix();
        lines[0]["payload"]["clock"]["source_at"] = json!(i64::MAX);
        refused(&lines, "json_invalid", 1);
        let mut lines = prefix();
        lines[0]["payload"]["clock"]["received_at"] = json!("2025-11-08T21:20:00Z");
        refused(&lines, "value_invalid", 1);
    }

    /// Export the very same successful unit-test inputs for the real CLI.
    /// Only synthetic fixtures are written, exclusively under this profile's TMPDIR.
    #[test]
    fn phase1_replay_cli_fixture() {
        use std::io::Write;
        let root = std::path::PathBuf::from(std::env::var_os("TMPDIR").expect("profile scratch"));
        let directory = root.join(format!("r2-replay-v2-cli-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        let input: String = prefix().iter().map(|line| format!("{line}\n")).collect();
        let files = [
            ("config.json", serde_json::to_string(&config_v2()).unwrap()),
            ("input.jsonl", input),
        ];
        for (name, content) in files {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join(name))
                .unwrap();
            file.write_all(content.as_bytes()).unwrap();
        }
        println!("REPLAY_V2_CLI_FIXTURE {}", directory.display());
    }

    #[test]
    fn old_event_duplicates_are_ignored_before_elapsed_validation() {
        let mut lines = prefix();
        lines.push(lines[0].clone());
        let (rows, summary) = drive(&lines);
        assert_eq!(rows.len(), 25);
        assert_eq!(summary.duplicate_count, 1);
        assert_eq!(rows[24]["decision"]["samples"], 20);
        lines[25]["payload"]["price"] = json!(123.0);
        refused(&lines, "identity_invalid", 26);
    }
}

const T0: i64 = 1_762_636_800_000; // 2025-11-08T21:20:00Z
const ROUND: i64 = 300_000;

/// The frozen configuration the tests run with.
fn config() -> ReplayConfig {
    ReplayConfig {
        schema_version: SCHEMA_VERSION.to_string(),
        min_edge: 0.03,
        order_usd: 5.0,
        min_secs_left: 30,
        vol_floor: 0.00005,
        half_life_secs: 300.0,
        min_vol_samples: 20,
    }
}

/// Build an event envelope.
fn event(id: &str, at_ms: i64, seq: u64, payload: serde_json::Value) -> String {
    serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "event_id": id,
        "event_at_ms": at_ms,
        "available_at_ms": at_ms,
        "seq": seq,
        "source": "test-fixture",
        "evidence_type": "fixture",
        "payload": payload,
    })
    .to_string()
}

/// The market identity event, with an explicit round window.
fn market_window_event(
    id: &str,
    at_ms: i64,
    seq: u64,
    market_id: u64,
    starts_at_ms: i64,
    ends_at_ms: i64,
) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({
            "type": "market",
            "market_id": market_id,
            "symbol": REPLAY_SYMBOL,
            "starts_at_ms": starts_at_ms,
            "ends_at_ms": ends_at_ms,
            "yes_token": format!("yes-{market_id}"),
            "no_token": format!("no-{market_id}"),
            "yes_name": "Up",
            "no_name": "Down",
            "rules_source": "test-rules",
        }),
    )
}

/// The market identity event, for a round that starts now.
fn market_event(id: &str, at_ms: i64, seq: u64, market_id: u64) -> String {
    market_window_event(id, at_ms, seq, market_id, at_ms, at_ms + ROUND)
}

/// The market state event.
fn state_event(
    id: &str,
    at_ms: i64,
    seq: u64,
    market_id: u64,
    open: bool,
    contested: bool,
    paused: bool,
    start_price: Option<f64>,
    fee_rate_bps: Option<u32>,
) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({
            "type": "state",
            "market_id": market_id,
            "open": open,
            "contested": contested,
            "paused": paused,
            "start_price": start_price,
            "fee_rate_bps": fee_rate_bps,
        }),
    )
}

/// A spot observation.
fn spot_event(id: &str, at_ms: i64, seq: u64, price: f64) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({
            "type": "spot",
            "symbol": REPLAY_SYMBOL,
            "price": price,
            "model_source": "test-feed",
        }),
    )
}

/// A book snapshot, wide enough that the model finds an edge.
fn book_event(id: &str, at_ms: i64, seq: u64, market_id: u64) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({
            "type": "book",
            "market_id": market_id,
            "yes_token": format!("yes-{market_id}"),
            "bids": [{"price": 0.40, "size": 50.0}],
            "asks": [{"price": 0.45, "size": 50.0}],
        }),
    )
}

/// A tick.
fn tick_event(id: &str, at_ms: i64, seq: u64, market_id: u64) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({"type": "tick", "market_id": market_id}),
    )
}

/// A local execution result.
fn result_event(
    id: &str,
    at_ms: i64,
    seq: u64,
    market_id: u64,
    candidate_event_id: &str,
    status: &str,
    filled_shares: Option<f64>,
    detail: Option<&str>,
) -> String {
    event(
        id,
        at_ms,
        seq,
        serde_json::json!({
            "type": "result",
            "market_id": market_id,
            "candidate_event_id": candidate_event_id,
            "status": status,
            "filled_shares": filled_shares,
            "detail": detail,
        }),
    )
}

/// Run a stream and return the rows and the summary.
fn drive(mode: ReplayMode, lines: &[String]) -> (Vec<OutputRow>, ReplaySummary) {
    let input = if lines.is_empty() {
        String::new()
    } else {
        lines.join("\n") + "\n"
    };
    let mut output: Vec<u8> = Vec::new();
    let result = run(
        config(),
        mode,
        i64::MIN / 2,
        i64::MAX / 2,
        std::io::BufReader::new(input.as_bytes()),
        &mut output,
    );
    let rows: Vec<OutputRow> = output
        .split(|byte| *byte == b'\n')
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| serde_json::from_slice(chunk).expect("each row is valid JSON"))
        .collect();
    match result {
        Ok(summary) => (rows, summary),
        Err(error) => panic!("the run failed: {error:?}"),
    }
}

/// Run a stream that is expected to fail, and return the failure.
fn drive_failure(mode: ReplayMode, lines: &[String]) -> ReplayError {
    let input = lines.join("\n") + "\n";
    let mut output: Vec<u8> = Vec::new();
    run(
        config(),
        mode,
        i64::MIN / 2,
        i64::MAX / 2,
        std::io::BufReader::new(input.as_bytes()),
        &mut output,
    )
    .expect_err("the run should have failed")
}

/// A series of strictly increasing prices. `count` prices give `count - 1`
/// return samples, so twenty returns need twenty-one prices.
fn price_series(start_at: i64, first_seq: u64, count: u64) -> Vec<String> {
    (0..count)
        .map(|index| {
            spot_event(
                &format!("spot-{index}"),
                start_at + index as i64 * ROUND,
                first_seq + index,
                100.0 + 0.0001 * index as f64,
            )
        })
        .collect()
}

/// The market, state and book that let a tick decide.
fn market_setup(at_ms: i64, first_seq: u64) -> Vec<String> {
    vec![
        market_event("market-1", at_ms, first_seq, 1),
        state_event(
            "state-1",
            at_ms + 1,
            first_seq + 1,
            1,
            true,
            false,
            false,
            Some(100.0),
            Some(200),
        ),
        book_event("book-1", at_ms + 2, first_seq + 2, 1),
    ]
}

/// Cross-market ownership includes both token sides; restating an identity is legal.
#[test]
fn fr02_token_sets_must_be_disjoint() {
    for (yes, no) in [
        ("no-1", "no-2"),
        ("yes-2", "yes-1"),
        ("yes-1", "no-2"),
        ("yes-2", "no-1"),
        ("same", "same"),
    ] {
        let mut second: serde_json::Value =
            serde_json::from_str(&market_event("market-2", T0 + 1, 1, 2)).unwrap();
        second["payload"]["yes_token"] = yes.into();
        second["payload"]["no_token"] = no.into();
        let failure = drive_failure(
            ReplayMode::Replay,
            &[market_event("market-1", T0, 0, 1), second.to_string()],
        );
        assert_eq!(failure.exit_code, EXIT_CONTRACT, "{yes}/{no}");
        assert_eq!(failure.line, 2);
        assert_eq!(failure.code, "identity_invalid");
    }
    let mut restated: serde_json::Value =
        serde_json::from_str(&market_event("market-1", T0, 0, 1)).unwrap();
    restated["event_id"] = "market-1-restated".into();
    restated["seq"] = 2.into();
    let (rows, summary) = drive(
        ReplayMode::Replay,
        &[
            market_event("market-1", T0, 0, 1),
            market_event("market-2", T0 + 1, 1, 2),
            {
                restated["available_at_ms"] = (T0 + 2).into();
                restated.to_string()
            },
        ],
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(summary.exit_code, 0);
    assert_eq!(rows[2].yes_token.as_deref(), Some("yes-1"));
}

/// The warm-up boundary: the twentieth return is the first that counts.
#[test]
fn warmup_19_20_returns() {
    // Twenty prices: nineteen returns, one short of the threshold.
    let base = T0 + 30 * ROUND;
    let mut lines = price_series(T0, 0, 20);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    let (rows, summary) = drive(ReplayMode::Replay, &lines);
    let tick = rows
        .iter()
        .find(|row| row.event_id == "tick-1")
        .expect("the tick produced a row");
    assert_eq!(tick.reason, "warmup");
    assert_eq!(tick.samples, Some(19));
    assert!(summary.missing_reasons.contains(&"warmup".to_string()));
    // Twenty-one prices: twenty returns, which is enough.
    let base = T0 + 30 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let tick = rows
        .iter()
        .find(|row| row.event_id == "tick-1")
        .expect("the tick produced a row");
    assert_eq!(tick.samples, Some(20));
    assert_eq!(tick.reason, "candidate");
}

/// The identity and availability rules.
#[test]
fn identity_and_available_state() {
    // A market for another symbol is refused.
    let lines = vec![event(
        "market-1",
        T0,
        0,
        serde_json::json!({
            "type": "market",
            "market_id": 1,
            "symbol": "ETHUSDT",
            "starts_at_ms": T0,
            "ends_at_ms": T0 + ROUND,
            "yes_token": "yes-1",
            "no_token": "no-1",
            "yes_name": "Up",
            "no_name": "Down",
            "rules_source": "test-rules",
        }),
    )];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A round that is not 300 seconds long is refused.
    let lines = vec![market_window_event("market-1", T0, 0, 1, T0, T0 + 299_000)];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // The same token in two markets is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "market-2",
            T0 + 1,
            1,
            serde_json::json!({
                "type": "market",
                "market_id": 2,
                "symbol": REPLAY_SYMBOL,
                "starts_at_ms": T0 + ROUND,
                "ends_at_ms": T0 + 2 * ROUND,
                "yes_token": "yes-1",
                "no_token": "no-2",
                "yes_name": "Up",
                "no_name": "Down",
                "rules_source": "test-rules",
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A market whose identity changes is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        market_window_event("market-1b", T0 + 1, 1, 1, T0 + ROUND, T0 + 2 * ROUND),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A state for a market that was never established is refused.
    let lines = vec![state_event(
        "state-1",
        T0,
        0,
        9,
        true,
        false,
        false,
        Some(100.0),
        Some(200),
    )];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A book whose token belongs to another market is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "book-1",
            T0 + 1,
            1,
            serde_json::json!({
                "type": "book",
                "market_id": 1,
                "yes_token": "yes-999",
                "bids": [{"price": 0.40, "size": 50.0}],
                "asks": [{"price": 0.45, "size": 50.0}],
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );
}

/// The reasons a tick gives for not deciding.
#[test]
fn tick_reasons_follow_the_frozen_priority() {
    let cases: Vec<(&str, bool, bool, bool, Option<f64>, Option<u32>, &str)> = vec![
        (
            "closed",
            false,
            false,
            false,
            Some(100.0),
            Some(200),
            "closed",
        ),
        (
            "contested",
            true,
            true,
            false,
            Some(100.0),
            Some(200),
            "contested",
        ),
        (
            "paused",
            true,
            false,
            true,
            Some(100.0),
            Some(200),
            "paused",
        ),
        (
            "no-start-price",
            true,
            false,
            false,
            None,
            Some(200),
            "start_price_unknown",
        ),
        (
            "no-fee",
            true,
            false,
            false,
            Some(100.0),
            None,
            "fee_unknown",
        ),
    ];
    for (name, open, contested, paused, start_price, fee, reason) in cases {
        let base = T0 + 30 * ROUND;
        let mut lines = price_series(T0, 0, 21);
        lines.push(market_event("market-1", base, 100, 1));
        lines.push(state_event(
            "state-1",
            base + 1,
            101,
            1,
            open,
            contested,
            paused,
            start_price,
            fee,
        ));
        lines.push(book_event("book-1", base + 2, 102, 1));
        lines.push(tick_event("tick-1", base + 1_000, 103, 1));
        let (rows, _) = drive(ReplayMode::Replay, &lines);
        let tick = rows
            .iter()
            .find(|row| row.event_id == "tick-1")
            .expect("the tick produced a row");
        assert_eq!(tick.reason, reason, "case {name}");
    }

    // Without any state the round is unknown, whatever else is known.
    let base = T0 + 30 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.push(market_event("market-1", base, 100, 1));
    lines.push(book_event("book-1", base + 2, 102, 1));
    lines.push(tick_event("tick-1", base + 1_000, 103, 1));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let tick = rows
        .iter()
        .find(|row| row.event_id == "tick-1")
        .expect("the tick produced a row");
    assert_eq!(tick.reason, "state_unknown");

    // Without a spot there is nothing to value.
    let base = T0 + 30 * ROUND;
    let mut lines = vec![
        market_event("market-1", base, 100, 1),
        state_event(
            "state-1",
            base + 1,
            101,
            1,
            true,
            false,
            false,
            Some(100.0),
            Some(200),
        ),
        book_event("book-1", base + 2, 102, 1),
        tick_event("tick-1", base + 1_000, 103, 1),
    ];
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let tick = rows
        .iter()
        .find(|row| row.event_id == "tick-1")
        .expect("the tick produced a row");
    assert_eq!(tick.reason, "spot_unknown");

    // Without a book there is nothing to buy.
    let base = T0 + 30 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.push(market_event("market-1", base, 100, 1));
    lines.push(state_event(
        "state-1",
        base + 1,
        101,
        1,
        true,
        false,
        false,
        Some(100.0),
        Some(200),
    ));
    lines.push(tick_event("tick-1", base + 1_000, 103, 1));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let tick = rows
        .iter()
        .find(|row| row.event_id == "tick-1")
        .expect("the tick produced a row");
    assert_eq!(tick.reason, "book_unknown");
}

/// The time boundaries: before the start, at the end, and near the expiry.
///
/// Each boundary gets its own stream: a candidate that is still pending
/// stops the next tick, so the cases cannot share one round.
#[test]
fn the_time_boundaries_are_respected() {
    let base = T0 + 40 * ROUND;
    let start = base + 1_000;
    let end = start + ROUND;

    // One stream, one tick, at the given offset from the end of the round.
    let reason_at = |offset_ms: i64| -> String {
        let mut lines = price_series(T0, 0, 21);
        lines.push(market_window_event("market-1", base, 100, 1, start, end));
        lines.push(state_event(
            "state-1",
            base + 100,
            101,
            1,
            true,
            false,
            false,
            Some(100.0),
            Some(200),
        ));
        lines.push(book_event("book-1", base + 200, 102, 1));
        lines.push(tick_event("tick-1", end - offset_ms, 103, 1));
        let (rows, _) = drive(ReplayMode::Replay, &lines);
        rows.iter()
            .find(|row| row.event_id == "tick-1")
            .expect("the tick produced a row")
            .reason
            .clone()
    };

    // Before the round starts.
    let mut lines = price_series(T0, 0, 21);
    lines.push(market_window_event("market-1", base, 100, 1, start, end));
    lines.push(state_event(
        "state-1",
        base + 100,
        101,
        1,
        true,
        false,
        false,
        Some(100.0),
        Some(200),
    ));
    lines.push(book_event("book-1", base + 200, 102, 1));
    lines.push(tick_event("tick-early", base + 500, 103, 1));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    assert_eq!(
        rows.iter()
            .find(|row| row.event_id == "tick-early")
            .expect("the tick produced a row")
            .reason,
        "not_started"
    );

    // Thirty seconds left is still tradable.
    assert_eq!(reason_at(30_000), "candidate");
    // Twenty-nine seconds left is too late.
    assert_eq!(reason_at(29_000), "near_expiry");
    // The round has ended.
    assert_eq!(reason_at(-1), "ended");
}

/// Duplicates and conflicts.
#[test]
fn order_duplicates_and_conflicts() {
    // The same id with the same payload is ignored.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        market_event("market-1", T0, 0, 1),
    ];
    let (rows, summary) = drive(ReplayMode::Replay, &lines);
    assert_eq!(rows.len(), 1);
    assert_eq!(summary.duplicate_count, 1);

    // The same id with a different payload is a conflict.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        market_event("market-1", T0 + 1, 1, 2),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // Time going backwards is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        market_event("market-2", T0 - 1, 1, 2),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // The same time with the same sequence number is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        market_event("market-2", T0, 0, 2),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // Two different prices at the same time are refused.
    let lines = vec![
        spot_event("spot-1", T0, 0, 100.0),
        spot_event("spot-2", T0, 1, 100.5),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // The same price at the same time is a duplicate, not a conflict.
    let lines = vec![
        spot_event("spot-1", T0, 0, 100.0),
        spot_event("spot-2", T0, 1, 100.0),
    ];
    let (rows, summary) = drive(ReplayMode::Replay, &lines);
    assert_eq!(rows.len(), 2);
    assert_eq!(summary.spot_duplicate_count, 1);

    // A book price outside [0, 1] is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "book-1",
            T0 + 1,
            1,
            serde_json::json!({
                "type": "book",
                "market_id": 1,
                "yes_token": "yes-1",
                "bids": [{"price": 0.48, "size": 50.0}],
                "asks": [{"price": 1.5, "size": 50.0}],
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // Bids that do not decrease are refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "book-1",
            T0 + 1,
            1,
            serde_json::json!({
                "type": "book",
                "market_id": 1,
                "yes_token": "yes-1",
                "bids": [{"price": 0.40, "size": 50.0}, {"price": 0.48, "size": 50.0}],
                "asks": [{"price": 0.52, "size": 50.0}],
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A negative depth is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "book-1",
            T0 + 1,
            1,
            serde_json::json!({
                "type": "book",
                "market_id": 1,
                "yes_token": "yes-1",
                "bids": [{"price": 0.48, "size": -1.0}],
                "asks": [{"price": 0.52, "size": 50.0}],
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A missing field is not silently accepted as a known null.
    let lines = vec![serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "event_id": "market-1",
        "event_at_ms": T0,
        "available_at_ms": T0,
        "seq": 0,
        "source": "test-fixture",
        "evidence_type": "fixture",
        "payload": {"type": "state", "market_id": 1, "open": true},
    })
    .to_string()];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // An unknown field is refused.
    let lines = vec![serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "event_id": "market-1",
        "event_at_ms": T0,
        "available_at_ms": T0,
        "seq": 0,
        "source": "test-fixture",
        "evidence_type": "fixture",
        "extra": 1,
        "payload": {"type": "tick", "market_id": 1},
    })
    .to_string()];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A repeated JSON key is refused.
    let lines = vec![format!(
        "{{\"schema_version\":\"{SCHEMA_VERSION}\",\"event_id\":\"market-1\",\
         \"event_id\":\"market-2\",\"event_at_ms\":{T0},\"available_at_ms\":{T0},\
         \"seq\":0,\"source\":\"test-fixture\",\"evidence_type\":\"fixture\",\
         \"payload\":{{\"type\":\"tick\",\"market_id\":1}}}}"
    )];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A boolean where a number belongs is refused.
    let lines = vec![serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "event_id": "spot-1",
        "event_at_ms": T0,
        "available_at_ms": T0,
        "seq": 0,
        "source": "test-fixture",
        "evidence_type": "fixture",
        "payload": {"type": "spot", "symbol": REPLAY_SYMBOL, "price": true, "model_source": "test-feed"},
    })
    .to_string()];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // An event whose source time is after its available time is refused.
    let lines = vec![serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "event_id": "spot-1",
        "event_at_ms": T0 + 1,
        "available_at_ms": T0,
        "seq": 0,
        "source": "test-fixture",
        "evidence_type": "fixture",
        "payload": {"type": "spot", "symbol": REPLAY_SYMBOL, "price": 100.0, "model_source": "test-feed"},
    })
    .to_string()];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );
}
