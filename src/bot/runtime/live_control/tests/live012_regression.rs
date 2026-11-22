//! Native R10 (run-012) regression: a signed-but-unsent input change.
//!
//! Pascal's 2026-11-22 run-012 evidence: the second attempt was signed and then
//! refused at the last check because the frozen book had aged past
//! MAX_ACQUISITION_AGE_MS while the account receipt was still fresh. The send
//! permission was never taken, so no byte could have left: the refusal ends that
//! candidate — cash released, signed hash kept as evidence, window open — and
//! only a change under the permission (or a run-level fact) may end the window.
//! Everything here uses the offline synthetic venue only — no archived run,
//! network, signer or real order.

use super::*;

#[tokio::test]
async fn signed_but_unsent_frozen_input_change_ends_the_candidate_not_the_window() {
    // R10 (run-012): the attempt was signed, and then the frozen book aged
    // past MAX_ACQUISITION_AGE_MS before the first transport poll. The send
    // permission was never taken, so no byte could have left: the refusal is
    // this candidate's, the cash is released, the signed hash stays as
    // evidence, and the window still trades the next round.
    use crate::service::venue::OrderVenue;
    let (broker, clock) = risk_fixture();
    // Keep the fixture clock behind the adapter's system-clock expiration while
    // elapsed freshness still moves: the refused attempt and the next round
    // span more than one round's worth of elapsed time.
    clock.set_utc(Utc::now() - chrono::Duration::minutes(10));
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(
        crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
    let (trader, transport, _trace) =
        crate::service::predict_fun::controlled::synthetic_controlled_two_round_trader(200);
    let identity = trader.identity();
    let baseline = trader.account_snapshot().await.unwrap();
    let mut broker = Arc::new(broker);
    {
        let original = Arc::get_mut(&mut broker).unwrap();
        original.identity = Some(identity.clone());
        original.gate = Some(trader.gate.clone());
        original.taker = Some(Arc::new(trader) as Arc<dyn OrderVenue>);
        let mut state = original.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address.clone();
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    let (market, book, evaluation) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
    let candidate = evaluation.candidate.unwrap();
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    // The book was read 6 s before the attempt started; the account receipt is
    // read now. Signing then takes 4.001 s more, so at the last check the book
    // is 10.001 s old — past its window — while the receipt is 4.001 s old and
    // still fresh. That is run-012's shape (book 10.1 s, receipt 5.7 s).
    clock.advance(6_000);
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let hooks = Arc::clone(&state.live.controlled.as_ref().unwrap().preparation);
        *hooks.fault.lock() = Some((6, PreparationFault::BookAge(4_001)));
        let moved = clock.clone();
        *hooks.clock.lock() = Some(Arc::new(move |ms| moved.advance(ms)));
    }
    let seq_before = broker.state.lock().live.controlled.as_ref().unwrap().emitter.last_seq();
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a frozen input that aged out after signing refuses this candidate, not the window");
    assert!(matches!(outcome.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", outcome.result);
    let id = outcome.attempt_id.expect("the refused attempt keeps its id as evidence");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "the order was signed");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "no byte reached the transport");
    let seq_after_refusal;
    let signed_hash;
    {
        let state = broker.state.lock();
        let record = state.live.attempts[&id].lock();
        assert_eq!(record.phase, AttemptPhase::Closed);
        assert_eq!(record.submission, Submission::NotSubmitted);
        signed_hash = record.identity.signed_hash.clone()
            .expect("the signed hash survives the refusal as evidence");
        assert!(signed_hash.starts_with("0x"));
        drop(record);
        assert!(!state.live.reservations.contains_key(&id), "the unsent cash is released");
        assert_eq!(state.live.tombstones[&live::taker_key(market.id, candidate.token(), Side::Buy)], id,
            "one candidate, one attempt: the tombstone is unchanged");
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.pending_candidate.is_none());
        assert!(run.may_add_risk(), "the window stays open");
        assert!(run.control.lock().stop.is_none());
        assert!(state.entry_stop_reason.is_none());
        assert!(!state.halted);
        seq_after_refusal = run.emitter.last_seq();
        assert_eq!(state.live.attempts.len(), 1);
    }
    assert!(seq_after_refusal > seq_before, "the refusal is recorded");
    // The same decision cannot be submitted twice: the candidate is gone and
    // no attempt, cash or event follows it.
    let replay = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    assert!(replay.err().unwrap().to_string().contains("CONTEXT_MISSING"));
    {
        let state = broker.state.lock();
        assert_eq!(state.live.attempts.len(), 1);
        assert!(state.live.reservations.is_empty());
        assert_eq!(state.live.attempts[&id].lock().identity.signed_hash.as_deref(), Some(&*signed_hash));
        assert_eq!(state.live.controlled.as_ref().unwrap().emitter.last_seq(), seq_after_refusal);
    }
    // And the next decision still trades: a fresh round is signed and posted
    // once, with the window's own budget.
    {
        let state = broker.state.lock();
        let hooks = Arc::clone(&state.live.controlled.as_ref().unwrap().preparation);
        *hooks.fault.lock() = None;
    }
    clock.advance(300_000);
    live::check_runtime_account(&broker, false).await.unwrap();
    let (market2, book2, evaluation2) = evaluate_live_round_with_core(&clock, true, 2, &mut core);
    let candidate2 = evaluation2.candidate.unwrap();
    assert!(broker.emit_phase1_evaluation(&market2, &evaluation2.diagnostic, Some(&candidate2)));
    let ack = ack_next(broker.clone(), false);
    let second = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market2, yes_book: &book2, candidate: &candidate2 }).await
        .unwrap_or_else(|error| panic!("the next candidate after a provably unsent refusal still trades: {error:?}"));
    ack.join().unwrap();
    assert!(matches!(second.result, TakeResult::Filled { .. }), "{:?}", second.result);
    assert_eq!(second.attempt_id, Some(2));
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

#[test]
fn a_signed_but_unsent_refusal_records_its_own_order_event() {
    // R10: the events the refusal writes are the existing order/release pair —
    // NOT_SUBMITTED with the signed hash, reason INPUT_CHANGED — and no
    // posted-order event is ever produced.
    use crate::bot::runtime::live_events::{LiveEventEmitter, LIVE_EVENT_PREFIX};
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bot::runtime::live_control::candidate_tests::live012_regression::signed_but_unsent_frozen_input_change_ends_the_candidate_not_the_window", "--nocapture"])
        .output().unwrap();
    assert!(output.status.success());
    let text = std::str::from_utf8(&output.stdout).unwrap();
    let events: Vec<_> = text.lines().filter_map(|line| line.split_once(LIVE_EVENT_PREFIX))
        .map(|(_, json)| LiveEventEmitter::validate_bytes(json.as_bytes()).unwrap()).collect();
    let orders: Vec<_> = events.iter().filter(|event| event["event_kind"] == "order").collect();
    // One refused order, then the accepted round's PREPARED/MAY_HAVE_SUBMITTED/
    // SUBMITTED sequence. The refused attempt's event is the only one that
    // carries a signed hash and never a venue order id.
    assert_eq!(orders.len(), 4, "one refused order and one accepted order");
    assert_eq!(orders[0]["payload"]["attempt_id"], "1");
    assert_eq!(orders[0]["payload"]["submission"], "NOT_SUBMITTED");
    assert_eq!(orders[0]["payload"]["reason"], "INPUT_CHANGED");
    assert_eq!(orders[0]["payload"]["attempt_phase"], "CLOSED");
    assert!(orders[0]["payload"]["signed_hash"].as_str().is_some_and(|hash| hash.starts_with("0x")),
        "the refused attempt's signed hash is evidence");
    assert_eq!(orders[0]["payload"]["venue_order_id"], serde_json::Value::Null);
    assert_eq!(orders[3]["payload"]["attempt_id"], "2");
    assert_eq!(orders[3]["payload"]["submission"], "ACCEPTED");
    assert!(orders[3]["payload"]["signed_hash"].as_str().is_some_and(|hash| hash.starts_with("0x")));
    assert_ne!(orders[3]["payload"]["signed_hash"], orders[0]["payload"]["signed_hash"],
        "the next round signs its own order");
    let releases: Vec<_> = events.iter().filter(|event| event["event_kind"] == "reservation").collect();
    assert_eq!(releases.len(), 4, "RESERVE/RELEASE for both attempts");
    assert_eq!(releases[1]["payload"]["action"], "RELEASE");
    assert_eq!(releases[1]["payload"]["reason"], "INPUT_CHANGED");
    assert_eq!(releases[1]["payload"]["attempt_id"], "1");
}
