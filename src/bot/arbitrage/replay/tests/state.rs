use super::*;

fn evidence(line: &str, kind: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
    value["evidence_type"] = kind.into();
    value.to_string()
}

#[test]
fn fr01_mode_rejects_wrong_evidence() {
    for (mode, kind) in [(ReplayMode::Proxy, "real-market"),
        (ReplayMode::Replay, "real-proxy")] {
        let failure = drive_failure(mode, &[evidence(&spot_event("spot", T0, 0, 100.0), kind)]);
        assert_eq!(failure.exit_code, EXIT_CONTRACT);
        assert_eq!(failure.line, 1);
        assert_eq!(failure.partial.output_count, 0);
    }
}

#[test]
fn fr01_evidence_switch_stops_before_shared_state() {
    let base = T0 + 30 * ROUND;
    for (first, second) in [("fixture", "real-market"), ("real-market", "fixture")] {
        let mut lines: Vec<_> = price_series(T0, 0, 21).iter()
            .map(|line| evidence(line, first)).collect();
        lines.extend(market_setup(base, 100).iter().map(|line| evidence(line, second)));
        lines.push(evidence(&tick_event("tick", base + 1_000, 200, 1), second));
        let failure = drive_failure(ReplayMode::Replay, &lines);
        assert_eq!(failure.exit_code, EXIT_CONTRACT);
        assert_eq!(failure.line, 22);
        assert_eq!(failure.partial.output_count, 21);
        assert_eq!(failure.partial.candidate_count, 0);
    }
}

#[test]
fn fr01_homogeneous_evidence_keeps_evidence_ceiling() {
    for kind in ["fixture", "real-proxy"] {
        let lines: Vec<_> = price_series(T0, 0, 21).iter()
            .map(|line| evidence(line, kind)).collect();
        let (rows, summary) = drive(ReplayMode::Proxy, &lines);
        assert_eq!(rows.len(), 21);
        assert_eq!(rows[20].samples, Some(20));
        assert_eq!(summary.exit_code, 3);
    }
    let base = T0 + 30 * ROUND;
    for kind in ["fixture", "real-market"] {
        let mut lines = price_series(T0, 0, 21);
        lines.extend(market_setup(base, 100));
        lines.push(tick_event("tick", base + 1_000, 200, 1));
        let lines: Vec<_> = lines.iter().map(|line| evidence(line, kind)).collect();
        let (rows, summary) = drive(ReplayMode::Replay, &lines);
        assert_eq!(rows.last().unwrap().reason, "candidate");
        assert_eq!(summary.exit_code, 3); // Unconfirmed, not a fabricated fill.
        assert_eq!(summary.candidate_count, 1);
    }
}

#[test]
fn fr03_same_time_different_book_is_contract_failure() {
    let mut changed: serde_json::Value =
        serde_json::from_str(&book_event("book-2", T0 + 1, 2, 1)).unwrap();
    changed["payload"]["asks"][0]["price"] = 0.46.into();
    let failure = drive_failure(ReplayMode::Replay, &[
        market_event("market", T0, 0, 1),
        book_event("book-1", T0 + 1, 1, 1), changed.to_string(),
    ]);
    assert_eq!(failure.exit_code, EXIT_CONTRACT);
    assert_eq!(failure.code, "value_invalid");
    assert_eq!(failure.line, 3);
    assert_eq!(failure.partial.output_count, 2);
}

#[test]
fn fr03_same_payload_duplicate_and_later_update() {
    let base = T0 + 30 * ROUND;
    for (delay, ask, duplicates) in [(0, 0.45, 1), (1, 0.46, 0)] {
        let mut lines = price_series(T0, 0, 21);
        lines.extend(market_setup(base, 100));
        let mut next: serde_json::Value =
            serde_json::from_str(&book_event("book-2", base + 2 + delay, 103, 1)).unwrap();
        next["payload"]["asks"][0]["price"] = ask.into();
        lines.push(next.to_string());
        lines.push(tick_event("tick", base + 1_000, 200, 1));
        let (rows, summary) = drive(ReplayMode::Replay, &lines);
        assert_eq!(summary.exit_code, 3);
        assert_eq!(summary.duplicate_count, duplicates);
        assert_eq!(rows.len(), 26 - duplicates as usize);
        assert_eq!(rows.last().unwrap().reason, "candidate");
        assert_eq!(rows.last().unwrap().price, Some(ask));
    }
}

/// The confirmation state machine.
#[test]
fn confirmation_state_machine() {
    let base = T0 + 50 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    // A blocked result clears the candidate, so the round may be tried again.
    lines.push(result_event(
        "result-1",
        base + 2_000,
        201,
        1,
        "tick-1",
        "blocked",
        None,
        Some("no liquidity"),
    ));
    lines.push(tick_event("tick-2", base + 3_000, 202, 1));
    // A zero fill also clears the candidate.
    lines.push(result_event(
        "result-2",
        base + 4_000,
        203,
        1,
        "tick-2",
        "filled",
        Some(0.0),
        None,
    ));
    lines.push(tick_event("tick-3", base + 5_000, 204, 1));
    // A positive fill confirms the entry: no further candidate this round.
    lines.push(result_event(
        "result-3",
        base + 6_000,
        205,
        1,
        "tick-3",
        "filled",
        Some(9.6),
        None,
    ));
    lines.push(tick_event("tick-4", base + 7_000, 206, 1));
    let (rows, summary) = drive(ReplayMode::Replay, &lines);
    let row = |id: &str| {
        rows.iter()
            .find(|row| row.event_id == id)
            .expect("the row exists")
            .clone()
    };
    assert_eq!(row("tick-1").candidate, Some(true));
    assert_eq!(row("result-1").reason, "blocked");
    assert_eq!(row("tick-2").candidate, Some(true));
    assert_eq!(row("result-2").reason, "filled_zero");
    assert_eq!(row("tick-3").candidate, Some(true));
    assert_eq!(row("result-3").reason, "filled_positive");
    assert_eq!(row("result-3").entered_before, Some(false));
    assert_eq!(row("result-3").entered_after, Some(true));
    // The round already has an entry, so no new candidate is offered.
    assert_eq!(row("tick-4").candidate, Some(false));
    assert_eq!(row("tick-4").reason, "already_entered");
    assert_eq!(summary.positive_fill_count, 1);
    assert_eq!(summary.candidate_count, 3);

    // A candidate with no result yet stops the round from offering another.
    let base = T0 + 55 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    lines.push(tick_event("tick-2", base + 2_000, 201, 1));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let tick2 = rows
        .iter()
        .find(|row| row.event_id == "tick-2")
        .expect("the row exists");
    assert_eq!(tick2.reason, "candidate_unconfirmed");
    assert_eq!(tick2.candidate, Some(false));

    // A result for a market with no candidate is refused.
    let lines = vec![result_event(
        "result-1",
        T0,
        0,
        1,
        "tick-9",
        "filled",
        Some(1.0),
        None,
    )];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A second result for the same candidate is refused.
    let base = T0 + 60 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    lines.push(result_event(
        "result-1",
        base + 2_000,
        201,
        1,
        "tick-1",
        "filled",
        Some(1.0),
        None,
    ));
    lines.push(result_event(
        "result-2",
        base + 3_000,
        202,
        1,
        "tick-1",
        "filled",
        Some(1.0),
        None,
    ));
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // An unknown result stops the round from offering new entries.
    let base = T0 + 70 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    lines.push(result_event(
        "result-1",
        base + 2_000,
        201,
        1,
        "tick-1",
        "unknown",
        None,
        None,
    ));
    lines.push(tick_event("tick-2", base + 3_000, 202, 1));
    let (rows, summary) = drive(ReplayMode::Replay, &lines);
    let tick2 = rows
        .iter()
        .find(|row| row.event_id == "tick-2")
        .expect("the row exists");
    assert_eq!(tick2.reason, "result_unknown");
    assert_eq!(tick2.candidate, Some(false));
    assert_eq!(summary.unknown_result_count, 1);

    // A candidate with no result at all is left unconfirmed.
    let base = T0 + 80 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    let (_, summary) = drive(ReplayMode::Replay, &lines);
    assert_eq!(summary.unconfirmed_count, 1);
    assert!(summary
        .missing_reasons
        .contains(&"candidate_unconfirmed".to_string()));
}

/// A future event cannot change an earlier decision.
#[test]
fn prefix_causality_and_determinism() {
    let base = T0 + 90 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    lines.push(result_event(
        "result-1",
        base + 2_000,
        201,
        1,
        "tick-1",
        "filled",
        Some(9.6),
        None,
    ));
    lines.push(tick_event("tick-2", base + 3_000, 202, 1));

    let (full, _) = drive(ReplayMode::Replay, &lines);
    let (prefix, _) = drive(ReplayMode::Replay, &lines[..lines.len() - 1]);
    // Every prefix row is the same as the corresponding full row.
    for (index, row) in prefix.iter().enumerate() {
        assert_eq!(row, &full[index], "row {index} changed");
    }

    // Additional regression: check every prefix, including empty and full input.
    for count in 0..=lines.len() {
        let (partial, _) = drive(ReplayMode::Replay, &lines[..count]);
        assert_eq!(&partial[..], &full[..partial.len()], "prefix {count} changed");
    }

    // Appending a label does not change the earlier rows.
    let mut with_label = lines.clone();
    with_label.push(event(
        "label-1",
        base + ROUND + 1_000,
        300,
        serde_json::json!({
            "type": "label",
            "market_id": 1,
            "yes_token": "yes-1",
            "status": "settled",
            "payout_yes": 1.0,
            "official_source": "test-authority",
            "authority_report_ref": "test-report-1",
            "published_at_ms": base + ROUND,
        }),
    ));
    let (with_label_rows, _) = drive(ReplayMode::Replay, &with_label);
    for (index, row) in full.iter().enumerate() {
        assert_eq!(row, &with_label_rows[index], "row {index} changed");
    }

    // The same input twice gives byte-identical output.
    let mut first: Vec<u8> = Vec::new();
    let mut second: Vec<u8> = Vec::new();
    for output in [&mut first, &mut second] {
        run(
            config(),
            ReplayMode::Replay,
            i64::MIN / 2,
            i64::MAX / 2,
            std::io::BufReader::new(lines.join("\n").as_bytes()),
            output,
        )
        .expect("the run succeeded");
    }
    assert_eq!(first, second);
}

/// The proxy mode has no markets, no books and no execution.
#[test]
fn proxy_has_no_market_or_execution() {
    // A market in proxy mode is refused.
    let lines = vec![market_event("market-1", T0, 0, 1)];
    assert_eq!(
        drive_failure(ReplayMode::Proxy, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A value in replay mode is refused.
    let lines = vec![event(
        "value-1",
        T0,
        0,
        serde_json::json!({
            "type": "value",
            "symbol": REPLAY_SYMBOL,
            "strike": 100.0,
            "horizon_secs": 300,
        }),
    )];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // The proxy values the underlying against the spot closed at that time.
    let mut lines = price_series(T0, 0, 21);
    let value_at = T0 + 20 * ROUND;
    lines.push(event(
        "value-1",
        value_at,
        100,
        serde_json::json!({
            "type": "value",
            "symbol": REPLAY_SYMBOL,
            "strike": 100.0 + 0.0001 * 20.0,
            "horizon_secs": 300,
        }),
    ));
    let (rows, summary) = drive(ReplayMode::Proxy, &lines);
    let value = rows
        .iter()
        .find(|row| row.event_id == "value-1")
        .expect("the value produced a row");
    assert_eq!(value.reason, "valued");
    assert_eq!(value.samples, Some(20));
    assert!(value.p_up.expect("a probability").is_finite());
    // The proxy run is always short of evidence: it can never trade.
    assert_eq!(summary.exit_code, EXIT_EVIDENCE);
    assert!(summary.missing_reasons.contains(&"proxy_only".to_string()));

    // A strike that is not the spot closed at that time is refused.
    let lines = vec![
        spot_event("spot-1", T0, 0, 100.0),
        event(
            "value-1",
            T0,
            1,
            serde_json::json!({
                "type": "value",
                "symbol": REPLAY_SYMBOL,
                "strike": 100.5,
                "horizon_secs": 300,
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Proxy, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A gap in the proxy series restarts the volatility warm-up.
    let mut lines = price_series(T0, 0, 21);
    // A hole of two rounds: the history does not carry across it.
    let after_gap = T0 + 40 * ROUND;
    lines.push(spot_event("spot-gap", after_gap, 100, 101.0));
    let (rows, _) = drive(ReplayMode::Proxy, &lines);
    let gap = rows
        .iter()
        .find(|row| row.event_id == "spot-gap")
        .expect("the spot produced a row");
    assert_eq!(gap.samples, Some(0));

    // A horizon other than 300 seconds is refused.
    let lines = vec![
        spot_event("spot-1", T0, 0, 100.0),
        event(
            "value-1",
            T0,
            1,
            serde_json::json!({
                "type": "value",
                "symbol": REPLAY_SYMBOL,
                "strike": 100.0,
                "horizon_secs": 60,
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Proxy, &lines).exit_code,
        EXIT_CONTRACT
    );
}

/// The configuration only accepts the original frozen values.
#[test]
fn the_configuration_only_accepts_the_original_values() {
    assert!(config().validate().is_ok());
    let mut wrong = config();
    wrong.half_life_secs = 600.0;
    assert!(wrong.validate().is_err());
    let mut wrong = config();
    wrong.min_vol_samples = 19;
    assert!(wrong.validate().is_err());
    let mut wrong = config();
    wrong.schema_version = "other/1".to_string();
    assert!(wrong.validate().is_err());
    let mut wrong = config();
    wrong.min_edge = f64::NAN;
    assert!(wrong.validate().is_err());
    // Additional frozen-value regression assertions; the original assertions above remain.
    for field in ["min_edge", "order_usd", "min_secs_left", "vol_floor"] {
        let mut value = serde_json::to_value(config()).unwrap();
        value[field] = serde_json::json!(1);
        let wrong: ReplayConfig = serde_json::from_value(value).unwrap();
        assert!(wrong.validate().is_err(), "{field} must stay frozen");
    }
}

/// A label never reaches a decision.
#[test]
fn a_label_is_only_evaluated_after_the_fact() {
    let base = T0 + 100 * ROUND;
    let mut lines = price_series(T0, 0, 21);
    lines.extend(market_setup(base, 100));
    lines.push(tick_event("tick-1", base + 1_000, 200, 1));
    lines.push(event(
        "label-1",
        base + ROUND + 1_000,
        300,
        serde_json::json!({
            "type": "label",
            "market_id": 1,
            "yes_token": "yes-1",
            "status": "settled",
            "payout_yes": 1.0,
            "official_source": "test-authority",
            "authority_report_ref": "test-report-1",
            "published_at_ms": base + ROUND,
        }),
    ));
    let (rows, _) = drive(ReplayMode::Replay, &lines);
    let label = rows
        .iter()
        .find(|row| row.event_id == "label-1")
        .expect("the label produced a row");
    assert_eq!(label.reason, "label_evaluation_only");
    // The label carries no decision fields.
    assert_eq!(label.candidate, None);
    assert_eq!(label.token, None);
    assert_eq!(label.fair, None);
    assert_eq!(label.edge, None);

    // A settled label without an authority reference is refused.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "label-1",
            T0 + ROUND + 1_000,
            1,
            serde_json::json!({
                "type": "label",
                "market_id": 1,
                "yes_token": "yes-1",
                "status": "settled",
                "payout_yes": 1.0,
                "official_source": "test-authority",
                "authority_report_ref": "",
                "published_at_ms": T0 + ROUND,
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );

    // A tie pays one half.
    let lines = vec![
        market_event("market-1", T0, 0, 1),
        event(
            "label-1",
            T0 + ROUND + 1_000,
            1,
            serde_json::json!({
                "type": "label",
                "market_id": 1,
                "yes_token": "yes-1",
                "status": "tie",
                "payout_yes": 1.0,
                "official_source": "test-authority",
                "authority_report_ref": "test-report-1",
                "published_at_ms": T0 + ROUND,
            }),
        ),
    ];
    assert_eq!(
        drive_failure(ReplayMode::Replay, &lines).exit_code,
        EXIT_CONTRACT
    );
}
