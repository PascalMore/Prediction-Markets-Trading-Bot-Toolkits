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

fn economic_evaluation(
    core: &mut crate::bot::arbitrage::DecisionCore,
    clock: &crate::service::observation::FixtureClock,
    market: &crate::service::venue::TradableMarket,
    book: &crate::models::BookSnapshot,
) -> crate::bot::arbitrage::phase1::Phase1Evaluation {
    use crate::service::observation::{Clock, ObservationClock, SourceTimeStatus, FreshnessBasis};
    use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
    clock.advance(1);
    let now = clock.now().unwrap();
    let observation = ObservationClock {
        source_at: None, source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound, request_at: now.utc, received_at: now.utc,
        request_elapsed_ms: now.elapsed_ms, received_elapsed_ms: now.elapsed_ms,
    };
    core.observe_phase1("BTCUSDT", 100.2, crate::service::spot::SpotSource::Binance,
        "BTCUSDT", &observation, now).unwrap();
    core.evaluate_phase1(&EvaluationInputs {
        market, book, spot_price: 100.2,
        spot_source: Some(crate::service::spot::SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(observation.clone()), book_clock: Some(observation), now,
        order_usd: 1.0, min_edge: 0.03, min_secs_left: 30.0, entered: false,
        identity_available_at: market.starts_at, strike_available_at: market.starts_at,
        settlement_provider: SettlementProvider::Chainlink,
    })
}

async fn take_with_optional_ack(
    broker: Arc<Broker>, market: &crate::service::venue::TradableMarket,
    book: &crate::models::BookSnapshot, candidate: &CandidateContext,
) -> anyhow::Result<crate::bot::runtime::taker::LiveTakeOutcome> {
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (completion, channel) = (finished.clone(),
        broker.state.lock().live.controlled.as_ref().unwrap().channel.clone());
    let ack = std::thread::spawn(move || {
        if let Some(pending) = channel.wait_pending_until(&completion) {
            channel.accept(&channel::ControlFrame::Ack {
                seq: pending.seq, signed_hash: pending.signed_hash }).unwrap();
        }
    });
    let result = broker.take_live_candidate(taker::LiveCandidateRequest { market, yes_book: book, candidate }).await;
    finished.store(true, Ordering::SeqCst);
    ack.join().unwrap();
    result
}

#[tokio::test]
async fn conservative_edge_preview_skips_before_reserve_sign_and_keeps_next_core_candidate() {
    use crate::service::venue::OrderVenue;
    let (mut broker, clock) = risk_fixture();
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(
        crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
    let (market, _, _) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
    let expensive = crate::bot::runtime::test_support::book(&[(0.94, 100.0)], &[(0.95, 100.0)]);
    let evaluation = economic_evaluation(&mut core, &clock, &market, &expensive);
    let candidate = evaluation.candidate.expect("model edge qualifies, unlike conservative signed floor");
    assert_eq!(candidate.fee_rate_bps(), 200);
    assert_eq!(candidate.limit_price(), 0.95);
    let (trader, transport) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "controlled_fill");
    let identity = trader.identity();
    let baseline = trader.account_snapshot().await.unwrap();
    broker.identity = Some(identity.clone());
    broker.gate = Some(trader.gate.clone());
    broker.taker = Some(Arc::new(trader));
    {
        let mut state = broker.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address;
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    let broker = Arc::new(broker);
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    let outcome = take_with_optional_ack(broker.clone(), &market, &expensive, &candidate).await.unwrap();
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0, "precheck must precede signing");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert!(outcome.attempt_id.is_none());
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(state.live.attempts.is_empty() && state.live.reservations.is_empty());
        assert!(run.pending_candidate.is_none() && run.control.lock().stop.is_none());
        assert!(run.may_add_risk() && state.entry_stop_reason.is_none());
    }
    assert!(broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &expensive, candidate: &candidate }).await.is_err(), "old decision is not retried");
    clock.advance(5_000);
    live::check_runtime_account(&broker, false).await.unwrap();
    let cheap = crate::bot::runtime::test_support::book(&[(0.3, 100.0)], &[(0.4, 100.0)]);
    let next = economic_evaluation(&mut core, &clock, &market, &cheap);
    let next_candidate = next.candidate.unwrap();
    assert_ne!(candidate.candidate_id(), next_candidate.candidate_id());
    broker.observe(&market, &cheap);
    assert!(broker.emit_phase1_evaluation(&market, &next.diagnostic, Some(&next_candidate)));
    let sent = take_with_optional_ack(broker.clone(), &market, &cheap, &next_candidate).await.unwrap();
    assert!(matches!(sent.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

/// Construct the exact Prepared/NotSubmitted guard boundary, without bypassing its checks.
fn prepared_economic_guard() -> (Broker, LiveCandidateGuard, SignedExecutionBound) {
    use crate::service::observation::Clock;
    let (broker, clock) = risk_fixture();
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(
        crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
    let (market, _, _) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
    let book = crate::bot::runtime::test_support::book(&[(0.94, 100.0)], &[(0.95, 100.0)]);
    let evaluation = economic_evaluation(&mut core, &clock, &market, &book);
    let candidate = evaluation.candidate.unwrap();
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    let gross = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(gross, candidate.limit_price()).unwrap();
    let hash = format!("0x{}", "e".repeat(64));
    let mut state = broker.state.lock();
    let baseline = state.live.baseline.clone().unwrap();
    record_account_read(&mut state, &baseline, Some(clock.now().unwrap())).unwrap();
    let id = live::next_attempt_id(&mut state.live);
    let identity = state.live.controlled.as_ref().unwrap().identity.clone();
    let handle = live::reserve_attempt(&mut state.live, AttemptRecord {
        identity: OrderIdentity { local_attempt_id: id, signed_hash: Some(hash.clone()),
            venue_order_id: None, market_id: market.id, token_id: candidate.token().into(),
            side: Side::Buy, account: identity.clone() },
        request_gross_atoms: gross, r0: cash, reserved_sell: U256::ZERO,
        phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
    });
    let run = state.live.controlled.as_mut().unwrap();
    run.pending_candidate.as_mut().unwrap().worst_cash_atoms = cash;
    run.attempt_candidates.insert(id, candidate.candidate_id().into());
    let (control, emitter) = (run.control.clone(), run.emitter.clone());
    drop(state);
    let guard = LiveCandidateGuard::new(control, id, candidate.candidate_id().into(), emitter)
        .bind(broker.state.clone(), candidate.clone(), handle, "ctf".into());
    let bound = SignedExecutionBound {
        account: identity, market_id: market.id, token_id: candidate.token().into(), route: "ctf".into(),
        fee_rate_bps: candidate.fee_rate_bps(), request_gross_atoms: gross, gross_cash_atoms: cash,
        max_cash_atoms: cash,
        min_net_shares_atoms: crate::bot::runtime::live_accounting::minimum_net_shares(gross, candidate.fee_rate_bps()).unwrap(),
        expiration: clock.now().unwrap().utc.timestamp() + 60, signed_hash: hash,
    };
    (broker, guard, bound)
}

#[test]
fn conservative_edge_final_facts_refuses_only_the_unsent_candidate() {
    use crate::service::venue::{CandidateRefused, ControlledSendGuard};
    let (broker, guard, bound) = prepared_economic_guard();
    let error = guard.check_facts(&bound).err().expect("conservative edge is insufficient");
    assert!(error.downcast_ref::<CandidateRefused>().is_some(), "{error}");
    assert_eq!(taker::preparation_reason(&error), "INPUT_CHANGED");
    let state = broker.state.lock();
    assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none());
    let observation = guard.diagnostic().unwrap();
    assert_eq!(observation.stage, "FACTS");
    assert_eq!(observation.detail, "EDGE_BELOW_MIN");
}

#[test]
fn conservative_edge_atom_boundaries_and_invalid_proofs() {
    use crate::service::predict_fun::controlled::{conservative_buy_edge, cash_at_limit_atoms};
    let gross = U256::from(SCALE);
    let net = crate::bot::runtime::live_accounting::minimum_net_shares(gross, 200).unwrap();
    let value = crate::bot::runtime::live_accounting::exact_floor_mul(net, 0.5).unwrap();
    let required = cash_at_limit_atoms(gross, 0.03).unwrap();
    let boundary = value - required;
    for (cash, passes) in [(boundary - U256::from(1), true), (boundary, true), (boundary + U256::from(1), false)] {
        assert_eq!(conservative_buy_edge(gross, cash, 200, 0.5, 0.03).unwrap(), passes);
    }
    assert!(!conservative_buy_edge(gross, value + U256::from(1), 200, 0.5, 0.03).unwrap());
    assert!(conservative_buy_edge(U256::ZERO, U256::ZERO, 200, 0.5, 0.03).is_err());
    assert!(conservative_buy_edge(gross, boundary, 1001, 0.5, 0.03).is_err());
    assert!(conservative_buy_edge(gross, boundary, 200, f64::NAN, 0.03).is_err());
    assert!(conservative_buy_edge(gross, boundary, 200, 0.5, f64::NAN).is_err());
}

#[test]
fn conservative_edge_window_and_unproved_bounds_are_never_candidate_refusals() {
    use crate::service::venue::{CandidateRefused, ControlledSendGuard, SendCheckStage};
    let (broker, guard, bound) = prepared_economic_guard();
    let error = guard.check_locked(&mut broker.state.lock(), &bound,
        FrozenInput::Window, SendCheckStage::SendPermit).err().unwrap();
    assert!(error.downcast_ref::<CandidateRefused>().is_none());
    assert_eq!(error.to_string(), "SIGNED_BOUND_UNPROVEN");
    for fault in ["identity", "hash", "quantity", "fee", "net_floor", "unknown", "already_stopped"] {
        let (broker, guard, mut bound) = prepared_economic_guard();
        match fault {
            "identity" => bound.market_id += 1,
            "hash" => bound.signed_hash = format!("0x{}", "a".repeat(64)),
            "quantity" => bound.request_gross_atoms += U256::from(1),
            "fee" => bound.fee_rate_bps += 1,
            "net_floor" => bound.min_net_shares_atoms -= U256::from(1),
            "unknown" => guard.binding.as_ref().unwrap().handle.lock().submission = Submission::Unknown,
            _ => broker.state.lock().live.controlled.as_mut().unwrap().stop("LOSS_LATCH", true, Utc::now()),
        }
        let error = guard.check_facts(&bound).err().unwrap();
        assert!(error.downcast_ref::<CandidateRefused>().is_none(), "{fault}");
    }
}

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
