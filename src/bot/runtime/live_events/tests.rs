use super::*;
use crate::bot::runtime::live_accounting::SignedAtoms;
use alloy_primitives::U256;
use chrono::Utc;
use serde_json::{json, Value};

fn identity() -> LiveRunIdentity {
    LiveRunIdentity {
        run_id: "run-1".into(),
        risk_window_id: "window-1".into(),
    }
}

fn accounting() -> LiveAccounting {
    LiveAccounting {
        cash: Some(U256::from(100_000_000_000_000_000u128)),
        available: Some(U256::from(100_000_000_000_000_000u128)),
        reserved: Some(U256::ZERO),
        cost: Some(U256::ZERO),
        realized: Some(SignedAtoms::zero()),
        unrealized: Some(SignedAtoms::zero()),
        nav: Some(SignedAtoms::zero()),
        net_pnl: Some(SignedAtoms::zero()),
        loss_latched: false,
        status: "COMPLETE".into(),
        reason: reason::NONE.into(),
        mark_time: None,
        pending_claim_value: None,
        gas_status: "NOT_MEASURED".into(),
        gas_paid_atoms: None,
    }
}

fn stop_envelope() -> Value {
    let emitter = LiveEventEmitter::new(identity());
    emitter.next_seq();
    emitter.envelope(
        Utc::now(), None, None, &accounting(),
        LivePayload::Stop(StopPayload {
            stop_new_risk: true,
            reason: reason::DEADLINE.into(),
            takeover_required: true,
            alert_status: "LOCAL_RECORDED".into(),
            stop_detail: None,
        }),
    ).unwrap()
}

#[test]
fn a_stop_event_round_trips_the_schema() {
    let envelope = stop_envelope();
    assert_eq!(LiveEventEmitter::validate(&envelope), Ok(()));
}

#[test]
fn an_unknown_key_is_refused() {
    let mut envelope = stop_envelope();
    envelope["payload"]["extra"] = json!(1);
    assert_eq!(
        LiveEventEmitter::validate(&envelope),
        Err("UNKNOWN_KEY:extra".to_string())
    );
}

#[test]
fn a_missing_key_is_refused() {
    let mut envelope = stop_envelope();
    envelope["payload"].as_object_mut().unwrap().remove("takeover_required");
    assert_eq!(
        LiveEventEmitter::validate(&envelope),
        Err("MISSING_KEY:takeover_required".to_string())
    );
}

#[test]
fn an_unknown_reason_is_refused() {
    let mut envelope = stop_envelope();
    envelope["payload"]["reason"] = json!("SOMETHING_ELSE");
    assert_eq!(
        LiveEventEmitter::validate(&envelope),
        Err("REASON_INVALID".to_string())
    );
}

#[test]
fn a_latch_may_not_be_reported_as_unlatched() {
    let emitter = LiveEventEmitter::new(identity());
    emitter.next_seq();
    let mut envelope = emitter.envelope(
        Utc::now(), None, None, &accounting(),
        LivePayload::LossLatch(LossLatchPayload {
            latched: true,
            trigger_net_pnl_atoms: None,
            reason: reason::LOSS_LATCH.into(),
        }),
    ).unwrap();
    envelope["payload"]["latched"] = json!(false);
    assert_eq!(
        LiveEventEmitter::validate(&envelope),
        Err("LATCHED_INVALID".to_string())
    );
}

#[test]
fn an_atom_amount_is_canonical() {
    assert!(is_atoms("0"));
    assert!(is_atoms("1000000000000000000"));
    assert!(!is_atoms("01"));
    assert!(!is_atoms("-1"));
    assert!(!is_atoms(""));
    assert!(is_signed_atoms("-20"));
    assert!(!is_signed_atoms("-0"));
    assert!(!is_signed_atoms("1.5"));
}

#[test]
fn a_hash_is_strict() {
    assert!(is_hash(&format!("0x{}", "a".repeat(64))));
    assert!(!is_hash(&format!("0x{}", "a".repeat(63))));
    assert!(!is_hash(&format!("0x{}", "z".repeat(64))));
}

#[test]
fn unsigned_and_signed_atom_magnitudes_cannot_overflow_u256() {
    assert!(is_atoms(&U256::MAX.to_string()));
    let overflow = "115792089237316195423570985008687907853269984665640564039457584007913129639936";
    assert!(!is_atoms(overflow));
    assert!(!is_signed_atoms(&format!("-{overflow}")));
}

#[test]
fn market_ids_times_nullable_keys_and_general_reasons_are_strict() {
    let base = stop_envelope();
    for invalid in [json!(12), json!(true), json!(""), json!("12x")] {
        let mut event = base.clone();
        event["market_id"] = invalid;
        assert!(LiveEventEmitter::validate(&event).is_err());
    }
    let mut event = base.clone();
    event["market_id"] = json!("0012");
    assert_eq!(LiveEventEmitter::validate(&event), Ok(()));
    for invalid in [json!(true), json!("yesterday"), json!("2026-11-19T12:00:00"),
        json!("2026-11-19T12:00:00.000001Z")] {
        event = base.clone();
        event["event_time"] = invalid;
        assert!(LiveEventEmitter::validate(&event).is_err());
    }
    for reason in [reason::BROKER_BLOCKED, reason::RUN_STOPPED] {
        event = base.clone();
        event["payload"]["reason"] = json!(reason);
        assert_eq!(LiveEventEmitter::validate(&event), Err("REASON_INVALID".into()));
    }
    event = base;
    event.as_object_mut().unwrap().remove("token");
    assert_eq!(LiveEventEmitter::validate(&event), Err("MISSING_KEY:token".into()));
}

fn reading() -> ExecutionSnapshot {
    use crate::models::{Side, VenueId};
    use crate::service::venue::{AccountIdentity, AccountMode, Accounting, OrderIdentity};
    ExecutionSnapshot {
        identity: OrderIdentity {
            local_attempt_id: 1,
            signed_hash: Some(format!("0x{}", "a".repeat(64))),
            venue_order_id: Some("order_1-A".into()),
            market_id: 1,
            token_id: "77".into(),
            side: Side::Buy,
            account: AccountIdentity {
                chain_id: 56,
                trading_address: "0x1111111111111111111111111111111111111111".into(),
                signer_address: "0x2222222222222222222222222222222222222222".into(),
                mode: AccountMode::PredictAccount,
                venue: VenueId::PredictFun,
            },
        },
        submission: Submission::Accepted,
        quantity: Quantity::Known { gross_q_atoms: U256::from(5_000), class: QuantityClass::Partial },
        accounting: Accounting {
            gross_cash: Field::Known(U256::from(2_000)),
            collateral_fee: Field::Known(U256::ZERO),
            share_fee: Field::Known(U256::from(100)),
            net_cash: Field::Known(U256::from(2_000)),
            net_inventory: Field::Known(U256::from(4_900)),
            vwap: Field::Known(U256::from(400_000_000_000_000_000u128)),
        },
        accounting_gross_q: Some(U256::from(5_000)),
        closure: Closure::Unproven,
        generation: 1,
        pages_complete: true,
        gross_q_before: U256::ZERO,
        gross_q_after: U256::from(5_000),
        source: "offline-events-projection".into(),
        observed_at: Utc::now(),
        tx_hash: None,
    }
}

fn bound_state(snapshot: &ExecutionSnapshot) -> crate::bot::runtime::State {
    use crate::bot::runtime::{live, State};
    use crate::service::venue::AttemptRecord;
    let mut state = State::default();
    live::reserve_attempt(&mut state.live, AttemptRecord {
        identity: snapshot.identity.clone(),
        request_gross_atoms: U256::from(6_000),
        r0: U256::from(3_000),
        reserved_sell: U256::ZERO,
        phase: AttemptPhase::MayHaveSubmitted,
        submission: Submission::Accepted,
        last_reason: None,
    });
    state
}

fn execution_event(snapshot: &ExecutionSnapshot, applied: &AppliedExecution) -> Value {
    let emitter = LiveEventEmitter::new(identity());
    emitter.next_seq();
    let (quantity, gross) = quantity_class(snapshot);
    let (closure, final_gross) = closure_kind(snapshot);
    emitter.envelope(Utc::now(), Some(snapshot.identity.market_id.to_string()),
        Some(snapshot.identity.token_id.clone()), &accounting(),
        LivePayload::Execution(ExecutionPayload {
            attempt_id: snapshot.identity.local_attempt_id.to_string(),
            signed_hash: snapshot.identity.signed_hash.clone().unwrap(),
            venue_order_id: snapshot.identity.venue_order_id.clone(),
            generation: snapshot.generation,
            observed_at: snapshot.observed_at,
            pages_complete: snapshot.pages_complete,
            quantity_class: quantity.into(),
            gross_q_atoms: gross.map(|value| value.to_string()),
            accounting_gross_q_atoms: snapshot.accounting_gross_q.map(|value| value.to_string()),
            closure: closure.into(),
            final_gross_q_atoms: final_gross.map(|value| value.to_string()),
            facts: execution_facts(snapshot),
            delta: execution_delta(applied),
            tx_hash: snapshot.tx_hash.clone(),
            settlement_status: "UNVERIFIED".into(),
            settlement_checked_at: None,
            reason: reason::NONE.into(),
        }),
    ).unwrap()
}

#[test]
fn real_snapshot_application_projects_actual_net_fee_and_duplicate_zero() {
    use crate::bot::runtime::live::apply_snapshot_detailed;
    let snapshot = reading();
    let mut state = bound_state(&snapshot);
    let applied = apply_snapshot_detailed(&mut state, &snapshot).unwrap();
    let event = execution_event(&snapshot, &applied);
    assert_eq!(LiveEventEmitter::validate(&event), Ok(()));
    assert_eq!(event["payload"]["facts"]["net_inventory"]["atoms"], "4900");
    assert_eq!(event["payload"]["facts"]["share_fee"]["atoms"], "100");
    assert_eq!(event["payload"]["delta"]["net_inventory_atoms"], "4900");
    assert_eq!(event["payload"]["delta"]["net_cash_atoms"], "2000");
    let duplicate = apply_snapshot_detailed(&mut state, &snapshot).unwrap();
    let duplicate = execution_event(&snapshot, &duplicate);
    assert_eq!(LiveEventEmitter::validate(&duplicate), Ok(()));
    for key in DELTA_KEYS {
        assert_eq!(duplicate["payload"]["delta"][*key], "0");
    }
    assert_eq!(state.live.net_inventory[&1], U256::from(4_900));
}

#[test]
fn incomplete_facts_preserve_unknown_and_unapplied_null() {
    use crate::bot::runtime::live::apply_snapshot_detailed;
    let mut snapshot = reading();
    snapshot.accounting.net_inventory = Field::Unknown("unavailable".into());
    let mut state = bound_state(&snapshot);
    let applied = apply_snapshot_detailed(&mut state, &snapshot).unwrap();
    let event = execution_event(&snapshot, &applied);
    assert_eq!(LiveEventEmitter::validate(&event), Ok(()));
    assert_eq!(event["payload"]["facts"]["net_inventory"]["status"], "UNKNOWN");
    assert!(event["payload"]["facts"]["net_inventory"]["atoms"].is_null());
    for key in DELTA_KEYS {
        assert!(event["payload"]["delta"][*key].is_null());
    }
    assert_eq!(state.live.reservations[&1], U256::from(3_000));
    assert!(state.live.net_inventory.is_empty());
}

#[test]
fn only_zero_trade_vwap_can_be_not_applicable() {
    let mut snapshot = reading();
    snapshot.quantity = Quantity::Known { gross_q_atoms: U256::ZERO, class: QuantityClass::Zero };
    snapshot.accounting_gross_q = Some(U256::ZERO);
    snapshot.accounting.gross_cash = Field::Known(U256::ZERO);
    snapshot.accounting.collateral_fee = Field::Known(U256::ZERO);
    snapshot.accounting.share_fee = Field::Known(U256::ZERO);
    snapshot.accounting.net_cash = Field::Known(U256::ZERO);
    snapshot.accounting.vwap = Field::Unknown("no trades".into());
    snapshot.accounting.net_inventory = Field::Unknown("no net reading".into());
    let event = execution_event(&snapshot, &AppliedExecution::default());
    assert_eq!(LiveEventEmitter::validate(&event), Ok(()));
    assert_eq!(event["payload"]["facts"]["vwap"]["status"], "NOT_APPLICABLE");
    assert_eq!(event["payload"]["facts"]["net_inventory"]["status"], "UNKNOWN");
    let mut invalid = event.clone();
    invalid["payload"]["facts"]["net_inventory"]["status"] = json!("NOT_APPLICABLE");
    assert!(LiveEventEmitter::validate(&invalid).is_err());
    invalid = event;
    invalid["payload"]["gross_q_atoms"] = json!("1");
    assert!(LiveEventEmitter::validate(&invalid).is_err());
}

#[test]
fn nested_fact_delta_and_required_nullable_shapes_are_exact() {
    let event = execution_event(&reading(), &AppliedExecution::default());
    for key in ["facts", "delta"] {
        let mut invalid = event.clone();
        invalid["payload"][key]["raw"] = json!("forbidden");
        assert!(LiveEventEmitter::validate(&invalid).is_err());
    }
    let mut invalid = event.clone();
    invalid["payload"]["facts"]["gross_cash"].as_object_mut().unwrap().remove("coverage_gross_atoms");
    assert!(LiveEventEmitter::validate(&invalid).is_err());
    invalid = event.clone();
    invalid["payload"].as_object_mut().unwrap().remove("settlement_checked_at");
    assert!(LiveEventEmitter::validate(&invalid).is_err());
    invalid = event.clone();
    invalid["payload"]["generation"] = json!(true);
    assert!(LiveEventEmitter::validate(&invalid).is_err());
    invalid = event;
    invalid["payload"]["facts"]["gross_cash"]["atoms"] = json!(true);
    assert!(LiveEventEmitter::validate(&invalid).is_err());
}

#[test]
fn raw_decode_rejects_duplicates_invalid_utf8_nonfinite_and_oversize() {
    let event = stop_envelope();
    let text = serde_json::to_string(&event).unwrap();
    assert_eq!(LiveEventEmitter::validate_bytes(text.as_bytes()), Ok(event));
    for invalid in [
        text.replacen("\"event_seq\":1", "\"event_seq\":1,\"event_seq\":1", 1),
        text.replacen("\"stop_new_risk\":true", "\"stop_new_risk\":true,\"stop_new_risk\":true", 1),
        text.replacen("\"event_seq\":1", "\"event_seq\":NaN", 1),
        text.replacen("\"event_seq\":1", "\"event_seq\":Infinity", 1),
        text.replacen("\"event_seq\":1", "\"event_seq\":true", 1),
    ] {
        assert_ne!(invalid, text);
        assert!(LiveEventEmitter::validate_bytes(invalid.as_bytes()).is_err());
    }
    assert!(LiveEventEmitter::validate_bytes(&[0xff]).is_err());
    assert!(LiveEventEmitter::validate_bytes(&vec![b' '; MAX_EVENT_LINE_BYTES]).is_err());
}

#[test]
fn nonfinite_typed_payout_is_rejected_before_json_can_turn_it_into_null() {
    let emitter = LiveEventEmitter::new(identity());
    for payout_yes in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let payload = LivePayload::Payout(PayoutPayload {
            status: "CONFIRMED".into(),
            payout_yes: Some(payout_yes),
            position_net_atoms: "1".into(),
            claim_value_atoms: None,
            cash_received_atoms: None,
            tx_hash: None,
            observed_at: Utc::now(),
            reason: reason::NONE.into(),
        });
        assert_eq!(emitter.envelope(Utc::now(), None, None, &accounting(), payload),
            Err("PAYOUT_YES_INVALID".into()));
    }
}

#[test]
fn framing_counts_the_newline_and_a_rejected_write_is_sticky() {
    let mut bytes = serde_json::to_vec(&stop_envelope()).unwrap();
    bytes.resize(MAX_EVENT_LINE_BYTES - LIVE_EVENT_PREFIX.len() - 1, b' ');
    assert!(LiveEventEmitter::validate_bytes(&bytes).is_ok());
    bytes.push(b' ');
    assert!(LiveEventEmitter::validate_bytes(&bytes).is_err());
    let emitter = LiveEventEmitter::new(identity());
    let bad = LivePayload::Stop(StopPayload {
        stop_new_risk: false,
        reason: reason::DEADLINE.into(),
        takeover_required: true,
        alert_status: "LOCAL_RECORDED".into(),
        stop_detail: None,
    });
    assert_eq!(emitter.write(Utc::now(), None, None, &accounting(), bad.clone()),
        Err("STOP_NEW_RISK_INVALID".into()));
    assert!(emitter.output_failed());
    assert_eq!(emitter.write(Utc::now(), None, None, &accounting(), bad),
        Err("OUTPUT_INVALID".into()));
}

#[test]
fn the_reason_set_is_closed() {
    assert!(is_known_reason(reason::OUTPUT_INVALID));
    assert!(!is_known_reason("WHATEVER"));
}

fn core_diagnostic() -> DecisionDiagnostic {
    use crate::bot::arbitrage::{DecisionCore, phase1::{EvaluationInputs, SettlementProvider}};
    use crate::service::observation::{CheckTime, ObservationClock, SourceTimeStatus, FreshnessBasis};
    let mut market = crate::bot::runtime::test_support::market(1);
    market.yes_token = "77".into();
    market.no_token = "88".into();
    let book = crate::bot::runtime::test_support::book(&[(0.4, 10.0)], &[(0.5, 10.0)]);
    let now = Utc::now();
    let clock = ObservationClock {
        source_at: None, source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound, request_at: now, received_at: now,
        request_elapsed_ms: 0, received_elapsed_ms: 0,
    };
    DecisionCore::phase1(crate::config::BtcArbConfig::default()).evaluate_phase1(&EvaluationInputs {
        market: &market, book: &book, spot_price: 100.0,
        spot_source: Some(crate::service::spot::SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(clock.clone()), book_clock: Some(clock), now: CheckTime { utc: now, elapsed_ms: 0 },
        order_usd: 2.0, min_edge: 0.03, min_secs_left: 30.0, entered: false,
        identity_available_at: Some(now), strike_available_at: Some(now),
        settlement_provider: SettlementProvider::Chainlink,
    }).diagnostic
}

fn decision_event(diagnostic: DecisionDiagnostic) -> Result<Value, String> {
    let emitter = LiveEventEmitter::new(identity());
    emitter.next_seq();
    emitter.envelope(Utc::now(), Some("1".into()), None, &accounting(),
        LivePayload::Decision(DecisionPayload { decision: diagnostic }))
}

#[test]
fn actual_core_diagnostic_requires_every_recursive_key_and_live_phase() {
    use crate::bot::arbitrage::phase1::Phase;
    let diagnostic = core_diagnostic();
    let base = decision_event(diagnostic.clone()).unwrap();
    assert_eq!(LiveEventEmitter::validate(&base), Ok(()));
    for path in [vec!["payload", "decision"], vec!["payload", "decision", "spot_clock"],
        vec!["payload", "decision", "up"], vec!["payload", "decision", "down"]] {
        let object = path.iter().fold(&base, |value, key| &value[*key]).as_object().unwrap();
        for key in object.keys() {
            let mut invalid = base.clone();
            let target = path.iter().fold(&mut invalid, |value, key| &mut value[*key]);
            target.as_object_mut().unwrap().remove(key);
            assert!(LiveEventEmitter::validate(&invalid).is_err(), "missing {path:?}.{key}");
        }
        let mut invalid = base.clone();
        let target = path.iter().fold(&mut invalid, |value, key| &mut value[*key]);
        target["raw"] = json!("forbidden");
        assert!(LiveEventEmitter::validate(&invalid).is_err());
    }
    for (key, invalid) in [("checked_elapsed_ms", json!(true)), ("reason", json!("NONE")),
        ("strategy_contract", json!("legacy_cash_fee")), ("source_epoch", json!(-1))] {
        let mut event = base.clone();
        event["payload"]["decision"][key] = invalid;
        assert!(LiveEventEmitter::validate(&event).is_err(), "{key}");
    }
    for phase in [Phase::Evaluate, Phase::PreSubmit, Phase::FinalSend] {
        let mut diagnostic = diagnostic.clone();
        diagnostic.phase = phase;
        assert!(LiveEventEmitter::validate(&decision_event(diagnostic).unwrap()).is_ok());
    }
    let mut diagnostic = diagnostic;
    diagnostic.phase = Phase::PreApply;
    assert!(LiveEventEmitter::validate(&decision_event(diagnostic).unwrap()).is_err());
}

#[test]
fn core_nonfinite_scalars_are_rejected_before_serde_can_turn_them_into_null() {
    use crate::bot::arbitrage::phase1::Level;
    for nonfinite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let original = core_diagnostic();
        let mut invalid = original.clone();
        invalid.spot = Some(nonfinite);
        assert_eq!(decision_event(invalid), Err("DECISION_NUMBER_INVALID".into()));
        let mut invalid = original.clone();
        invalid.up.as_mut().unwrap().execution_edge = Some(nonfinite);
        assert_eq!(decision_event(invalid), Err("DECISION_NUMBER_INVALID".into()));
        let mut invalid = original;
        invalid.down.as_mut().unwrap().depth_levels = Some(vec![Level { price: 0.5, size: nonfinite }]);
        assert_eq!(decision_event(invalid), Err("DECISION_NUMBER_INVALID".into()));
    }
}

#[test]
fn all_ten_payload_shapes_reject_missing_extra_and_invalid_nested_fields() {
    let time = "2026-11-19T00:00:00.000Z";
    let hash = format!("0x{}", "a".repeat(64));
    let mut fixtures = vec![
        decision_event(core_diagnostic()).unwrap(),
        execution_event(&reading(), &AppliedExecution::default()),
        stop_envelope(),
    ];
    for (kind, payload) in [
        ("reservation", json!({"candidate_id":"1","attempt_id":"1","action":"RESERVE",
            "worst_cash_atoms":"2","remaining_atoms":"2","reason":"NONE"})),
        ("order", json!({"candidate_id":"1","attempt_id":"1","signed_hash":hash,
            "venue_order_id":null,"attempt_phase":"PREPARED","submission":"NOT_SUBMITTED",
            "request_gross_atoms":"5","max_cash_atoms":"2","min_net_shares_atoms":"4",
            "expires_at":null,"removal_locked_until":null,"reason":"NONE"})),
        ("reconciliation", json!({"scope":"STARTUP","checked_at":time,"complete":false,
            "collateral_atoms":null,"gas_balance_atoms":null,"positions":[],"open_hashes":[],
            "foreign_risk":false,"reason":"FACT_INCOMPLETE"})),
        ("loss_latch", json!({"latched":true,"trigger_net_pnl_atoms":"-20","reason":"LOSS_LATCH"})),
        ("recovery", json!({"policy":"COLD_START_REFUSE","state":"ARMED","reason":"NONE"})),
        ("payout", json!({"status":"UNRESOLVED","payout_yes":null,"position_net_atoms":"5",
            "claim_value_atoms":null,"cash_received_atoms":null,"tx_hash":null,"observed_at":time,
            "reason":"FACT_INCOMPLETE"})),
        ("final", json!({"run_status":"INCOMPLETE","stop_reason":"FACT_INCOMPLETE",
            "execution_complete":false,"cash_complete":false,"audit_complete":false,
            "takeover_required":true,"unresolved":[],"positions":[],"stop_detail":null})),
    ] {
        let mut event = stop_envelope();
        event["event_kind"] = json!(kind);
        event["payload"] = payload;
        if matches!(kind, "reservation" | "order" | "payout") {
            event["market_id"] = json!("1");
            event["token"] = json!("77");
        }
        fixtures.push(event);
    }
    assert_eq!(fixtures.len(), EventKind::all().len());
    for event in fixtures {
        assert_eq!(LiveEventEmitter::validate(&event), Ok(()), "{}", event["event_kind"]);
        for key in event["payload"].as_object().unwrap().keys() {
            let mut invalid = event.clone();
            invalid["payload"].as_object_mut().unwrap().remove(key);
            assert!(LiveEventEmitter::validate(&invalid).is_err(), "{} missing {key}", event["event_kind"]);
        }
        let mut invalid = event.clone();
        let (key, bad) = match event["event_kind"].as_str().unwrap() {
            "decision" => ("decision", json!({})),
            "reservation" => ("action", json!("RESET")),
            "order" => ("attempt_phase", json!("READY")),
            "execution" => ("quantity_class", json!(true)),
            "reconciliation" => ("complete", json!(1)),
            "loss_latch" => ("latched", json!(false)),
            "stop" => ("takeover_required", json!(0)),
            "recovery" => ("policy", json!("AUTO_RESUME")),
            "payout" => ("payout_yes", json!(1.01)),
            "final" => ("audit_complete", json!(null)),
            _ => unreachable!(),
        };
        invalid["payload"][key] = bad;
        assert!(LiveEventEmitter::validate(&invalid).is_err(), "{} invalid {key}", event["event_kind"]);
        if matches!(event["event_kind"].as_str(), Some("reconciliation" | "final")) {
            invalid = event.clone();
            invalid["payload"]["positions"] = json!([{"market_id":"1","token":"77",
                "net_shares_atoms":null,"cost_atoms":null}]);
            assert!(LiveEventEmitter::validate(&invalid).is_err());
        }
        let mut invalid = event;
        invalid["payload"]["secret"] = json!("forbidden");
        assert!(LiveEventEmitter::validate(&invalid).is_err());
    }
}

#[test]
fn a_holding_is_settled_only_by_a_zero_write_off_or_exact_claimed_cash() {
    use crate::bot::runtime::live_control::LiveOutcome;
    let inventory = U256::from(5_000_000_000_000_000_000u128);
    let claimed = |inventory: U256, cash: U256| LiveOutcome {
        payout_yes: Some(1.0),
        result_prices: (100.0, 110.0),
        position_net_atoms: inventory,
        claim_value_atoms: Some(cash),
        cash_confirmed: true,
    };
    // Claimed cash covering the whole holding is settled, but only when the
    // confirmed official result owes exactly that amount.
    let win = claimed(inventory, inventory);
    assert!(settled_holding(inventory, Some((inventory, inventory)), Some(&win)));
    // A credit that doesn't cover the holding is still open risk.
    assert!(!settled_holding(inventory, Some((inventory - U256::from(1), inventory)), Some(&win)));
    // A claim the confirmed result does not owe exactly is not settled: one
    // atom short is an unexplained payout, not a closed holding.
    assert!(!settled_holding(inventory, Some((inventory, inventory - U256::from(1))), Some(&win)));
    // An unclaimed confirmed win is not cash yet.
    assert!(!settled_holding(inventory, None, Some(&win)));
    // A zero write-off is settled; a positive claim is not, without the cash.
    let zero = claimed(inventory, U256::ZERO);
    assert!(settled_holding(inventory, None, Some(&zero)));
    assert!(!settled_holding(inventory, None, None));
}
