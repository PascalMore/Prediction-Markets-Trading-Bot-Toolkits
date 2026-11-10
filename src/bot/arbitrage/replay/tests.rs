//! The test helpers and the first acceptance groups of the replay driver.
//!
//! The groups match the acceptance vectors the design freezes: warm-up
//! boundaries, identity and availability, duplicates and conflicts. The
//! state-machine groups live in the `state` submodule and reuse these helpers.

use super::*;

mod state;


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
    let input = if lines.is_empty() { String::new() } else { lines.join("\n") + "\n" };
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
    for (yes, no) in [("no-1", "no-2"), ("yes-2", "yes-1"),
        ("yes-1", "no-2"), ("yes-2", "no-1"), ("same", "same")] {
        let mut second: serde_json::Value =
            serde_json::from_str(&market_event("market-2", T0 + 1, 1, 2)).unwrap();
        second["payload"]["yes_token"] = yes.into();
        second["payload"]["no_token"] = no.into();
        let failure = drive_failure(ReplayMode::Replay, &[
            market_event("market-1", T0, 0, 1), second.to_string(),
        ]);
        assert_eq!(failure.exit_code, EXIT_CONTRACT, "{yes}/{no}");
        assert_eq!(failure.line, 2);
        assert_eq!(failure.code, "identity_invalid");
    }
    let mut restated: serde_json::Value =
        serde_json::from_str(&market_event("market-1", T0, 0, 1)).unwrap();
    restated["event_id"] = "market-1-restated".into();
    restated["seq"] = 2.into();
    let (rows, summary) = drive(ReplayMode::Replay, &[
        market_event("market-1", T0, 0, 1),
        market_event("market-2", T0 + 1, 1, 2),
        { restated["available_at_ms"] = (T0 + 2).into(); restated.to_string() },
    ]);
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
        ("closed", false, false, false, Some(100.0), Some(200), "closed"),
        ("contested", true, true, false, Some(100.0), Some(200), "contested"),
        ("paused", true, false, true, Some(100.0), Some(200), "paused"),
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
