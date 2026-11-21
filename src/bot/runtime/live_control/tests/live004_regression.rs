//! Native live004 regression: the venue's fresh book moved after the cash was
//! reserved.
//!
//! Pascal's 2026-11-21 decision: one candidate refused because the live book
//! (or its limit) moved — or, since the R1 correction, because its depth no
//! longer covers the frozen quantity — must release its own reservation, record
//! the refusal and leave the window open; only account, identity, bound,
//! acknowledgement and risk failures may end the run. Everything here uses the
//! offline synthetic venue only — no archived run, network, signer or real order.

use super::*;

/// A refusal is one candidate's event, one release and no stop.
#[test]
fn refused_candidate_events_show_one_unsent_order_and_one_release_without_a_stop() {
    // Both candidate-level refusals: the book moved past the limit, and (R1)
    // the book is inside the limit but too thin for the frozen quantity.
    for test_name in [
        "preparation_refusal_closes_only_the_unsent_cash_not_the_candidate_evidence",
        "thin_book_refusal_closes_only_the_unsent_cash_not_the_candidate_evidence",
    ] {
        let events = capture_production_test(test_name);
        let reserve = events.iter().find(|event| event["payload"]["action"] == "RESERVE").unwrap();
        let orders: Vec<_> = events.iter().filter(|event| event["event_kind"] == "order").collect();
        let last = orders.last().unwrap();
        assert_eq!(last["payload"]["candidate_id"], reserve["payload"]["candidate_id"]);
        assert_eq!(last["payload"]["attempt_id"], reserve["payload"]["attempt_id"]);
        assert_eq!(last["payload"]["submission"], "NOT_SUBMITTED");
        assert_eq!(last["payload"]["attempt_phase"], "CLOSED");
        assert!(last["payload"]["signed_hash"].is_null());
        assert_eq!(last["payload"]["reason"], "INPUT_CHANGED");
        let release = events.iter().find(|event| event["payload"]["action"] == "RELEASE").unwrap();
        assert_eq!(release["payload"]["remaining_atoms"], "0");
        assert_eq!(release["payload"]["attempt_id"], reserve["payload"]["attempt_id"]);
        // A refused candidate is not a stopped run: no stop, no post, no execution.
        assert!(!events.iter().any(|event| event["event_kind"] == "stop"), "{test_name}");
        assert!(!events.iter().any(|event| event["event_kind"] == "execution"), "{test_name}");
    }
}

/// The refused candidate cannot be retried, and the window still fills the
/// next decision that a normal observation tick produces.
#[tokio::test]
async fn a_refused_candidate_does_not_block_the_next_candidate_from_sending() {
    // The venue's first fresh book is past the limit; every later read is the
    // frozen book, so only the first decision is refused.
    let (broker, clock, market, book, candidate, transport) =
        sending_fixture("book_moved_then_frozen").await;
    let refused = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a moved book refuses the candidate, not the window");
    assert!(matches!(refused.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", refused.result);
    assert_eq!(refused.attempt_id, Some(1));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
    // The next decision comes from a new evaluation of the same round, not
    // from a retry of the refused one.
    let (next_market, next_book, next_evaluation) = evaluate_live_round(&clock, true, market.id);
    let next_candidate = next_evaluation.candidate.expect("the round is still eligible");
    live::check_runtime_account(&broker, false).await.unwrap();
    broker.observe(&next_market, &next_book);
    assert!(
        broker.emit_phase1_evaluation(
            &next_market,
            &next_evaluation.diagnostic,
            Some(&next_candidate),
        ),
        "the window must accept the next decision after a refusal"
    );
    let ack = ack_next(broker.clone(), false);
    let sent = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &next_market, yes_book: &next_book, candidate: &next_candidate }).await
        .expect("the next candidate is submitted normally");
    ack.join().unwrap();
    assert!(matches!(sent.result, TakeResult::Filled { shares, .. } if shares > 0.0),
        "{:?}", sent.result);
    assert_eq!(sent.attempt_id, Some(2));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

/// A book missing a whole side is missing data, not a refused decision.
#[tokio::test]
async fn an_empty_refreshed_book_stops_the_window_as_read_unavailable() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("empty_book").await;
    let error = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .err()
        .expect("missing book data must not be treated as a candidate refusal");
    assert_eq!(crate::bot::runtime::taker::preparation_reason(&error), "READ_UNAVAILABLE", "{error}");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let (reason, takeover_required) = {
        let control = run.control.lock();
        let stop = control.stop.as_ref().expect("missing data stops the window");
        (stop.reason.clone(), stop.takeover_required)
    };
    assert_eq!(reason, "READ_UNAVAILABLE");
    assert!(takeover_required);
    assert!(!run.may_add_risk());
    assert!(state.live.reservations.is_empty());
}

/// A book past the candidate's limit that also lacks the depth for the size is
/// the same candidate-level refusal, not missing data: both mean the fresh book
/// cannot support the frozen decision, and nothing has been signed or sent.
#[tokio::test]
async fn a_book_past_the_limit_without_depth_refuses_only_the_candidate() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("book_past_limit_thin").await;
    let refused = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a thin book past the limit refuses the candidate, not the window");
    assert!(matches!(refused.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", refused.result);
    assert_eq!(refused.attempt_id, Some(1));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.control.lock().stop.is_none(), "a thin book is not a stopped window");
    assert!(run.may_add_risk());
    assert!(state.entry_stop_reason.is_none());
    assert!(state.live.reservations.is_empty());
}

/// The R1 decision: a fresh book inside the candidate's limit whose depth
/// cannot cover the frozen quantity refuses that candidate alone — cash
/// released, NOT_SUBMITTED evidence, no stop — and the next decision from a
/// new observation still sends.
#[tokio::test]
async fn a_thin_book_inside_the_limit_refuses_the_candidate_and_keeps_the_window_open() {
    let (broker, clock, market, book, candidate, transport) =
        sending_fixture("book_inside_limit_thin_then_frozen").await;
    let refused = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a thin book inside the limit refuses the candidate, not the window");
    assert!(matches!(refused.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", refused.result);
    assert_eq!(refused.attempt_id, Some(1));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    {
        let state = broker.state.lock();
        let record = state.live.attempts[&1].lock();
        assert_eq!(record.phase, AttemptPhase::Closed);
        assert_eq!(record.submission, Submission::NotSubmitted);
        assert!(record.identity.signed_hash.is_none());
        drop(record);
        assert!(!state.live.reservations.contains_key(&1));
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.pending_candidate.is_none());
        assert!(run.control.lock().stop.is_none());
        assert!(run.may_add_risk());
        assert!(state.entry_stop_reason.is_none());
    }
    // The next decision comes from a new evaluation of the same round, not from
    // a retry of the refused one, and it is submitted normally.
    let (next_market, next_book, next_evaluation) = evaluate_live_round(&clock, true, market.id);
    let next_candidate = next_evaluation.candidate.expect("the round is still eligible");
    live::check_runtime_account(&broker, false).await.unwrap();
    broker.observe(&next_market, &next_book);
    assert!(
        broker.emit_phase1_evaluation(
            &next_market,
            &next_evaluation.diagnostic,
            Some(&next_candidate),
        ),
        "the window must accept the next decision after a refusal"
    );
    let ack = ack_next(broker.clone(), false);
    let sent = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &next_market, yes_book: &next_book, candidate: &next_candidate }).await
        .expect("the next candidate is submitted normally");
    ack.join().unwrap();
    assert!(matches!(sent.result, TakeResult::Filled { shares, .. } if shares > 0.0),
        "{:?}", sent.result);
    assert_eq!(sent.attempt_id, Some(2));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}
