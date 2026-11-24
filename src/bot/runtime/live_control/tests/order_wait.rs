//! The bounded wait for an accepted order's complete execution facts.
//!
//! These regressions use the real adapter, the real broker and the real
//! attempt bookkeeping: the venue is only a transport, and its answers are
//! the venue's own words. Nothing here reaches a network, a credential or an
//! account.

use super::candidate_tests::*;
use crate::bot::arbitrage::phase1::CandidateContext;
use crate::bot::runtime::live::order_wait;
use crate::bot::runtime::{live, taker, Broker};
use crate::models::BookSnapshot;
use crate::service::venue::{
    AttemptPhase, OrderVenue, Submission, TradableMarket,
};
use alloy_primitives::U256;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// The stop the run already holds, if any.
fn stopped(broker: &Broker) -> Option<(String, Option<String>, bool)> {
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let stop = { run.control.lock().stop.clone() };
    stop.map(|stop| (stop.reason, stop.detail, stop.takeover_required))
}

/// Let the wait observe the run's own clock, exactly as the drive loop does.
fn observe_wait(broker: &Broker) {
    let mut state = broker.state.lock();
    let run = state.live.controlled.as_mut().unwrap();
    order_wait::observe_budget(run);
}

/// Read the order's facts through the original sync until its fill arrives.
///
/// The original sync is the only reader here: the wait never reads on its
/// own, it only says which facts are still missing.
pub(super) async fn read_until_the_fill_arrives(broker: &Arc<Broker>, attempt_id: &u64) {
    for _ in 0..200 {
        if broker.state.lock().live.reservations.get(attempt_id).copied().unwrap_or(U256::ZERO)
            .is_zero()
        {
            return;
        }
        let _ = broker.sync_live().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Place the first candidate while the venue's fill read still lags.
///
/// Returns the attempt id and the gross quantity the order asked for.
async fn place_first_order(
    broker: &Arc<Broker>,
    market: &TradableMarket,
    book: &BookSnapshot,
    candidate: &CandidateContext,
) -> (u64, U256) {
    let ack = ack_next(broker.clone(), false);
    let outcome = broker
        .take_live_candidate(taker::LiveCandidateRequest {
            market,
            yes_book: book,
            candidate,
        })
        .await
        .unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.expect("the venue accepted the order");
    let requested =
        crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    (id, requested)
}

#[tokio::test]
async fn a_complete_zero_reading_waits_for_the_signed_expiry_and_does_not_stop_at_the_missing_budget() {
    let (broker, clock, market, book, candidate, transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    // The venue accepted the order and reports a complete reading of zero
    // fills: the fact is complete, and the order is still the venue's.
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _requested) = place_first_order(&broker, &market, &book, &candidate).await;
    assert!(stopped(&broker).is_none(), "a complete reading is not a failure");
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.order_wait.as_ref().expect("the order has its own wait");
        assert!(wait.pending, "a complete accepted reading is a fact in progress");
        assert!(wait.has_lifetime(), "the order's own signed expiry bounds it");
        assert!(wait.missing_since.is_none(), "nothing is missing");
        assert!(state.live.reservations[&id] > U256::ZERO, "the risk is not released");
        assert!(!run.may_add_risk(), "a pending order adds no risk");
    }
    // The missing-facts budget used to end this window. It must not: the
    // order's own signed expiry is what bounds it now.
    clock.advance(121_000);
    observe_wait(&broker);
    assert!(
        stopped(&broker).is_none(),
        "a complete zero fill past the missing budget is still observed: {:?}",
        stopped(&broker)
    );
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.order_wait.as_ref().expect("the wait survives").pending);
        assert!(state.live.reservations[&id] > U256::ZERO);
    }
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "no new order was sent");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "no new order was signed");
}

#[tokio::test]
async fn a_genuinely_missing_reading_still_stops_at_the_original_budget() {
    let (broker, clock, market, book, candidate, _transport, _trace) =
        sending_fixture_with_trace("controlled_missing_facts").await;
    // The venue states no status this runtime knows: the fact is missing,
    // not a complete reading of zero.
    let (id, _requested) = place_first_order(&broker, &market, &book, &candidate).await;
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.order_wait.as_ref().expect("the order is watched");
        assert!(!wait.pending, "an unknown status is not an accepted pending order");
        assert!(wait.missing_since.is_some(), "the missing streak starts at once");
        assert!(!wait.has_lifetime(), "a missing fact gets no speculative lifetime");
    }
    assert!(stopped(&broker).is_none(), "the missing budget has not run out yet");
    clock.advance(121_000);
    observe_wait(&broker);
    let stop = stopped(&broker).expect("a missing fact still stops the run at the budget");
    assert_eq!(stop.0, "FACT_INCOMPLETE", "the reason is the one it already used");
    let state = broker.state.lock();
    assert_ne!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
    assert!(state.live.reservations[&id] > U256::ZERO, "the risk is not released");
}

#[tokio::test]
async fn the_wait_survives_a_repeated_read_without_being_re_anchored() {
    let (broker, clock, market, book, candidate, transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _requested) = place_first_order(&broker, &market, &book, &candidate).await;
    let started = {
        let state = broker.state.lock();
        state.live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().started
    };
    // Repeated complete readings of the same order never move its origin.
    for _ in 0..3 {
        clock.advance(1_000);
        let hash = broker.state.lock().live.attempts[&id].lock().identity.signed_hash.clone();
        let hash = hash.expect("the order has a signed hash");
        let snapshot = broker
            .taker
            .as_ref()
            .expect("the venue")
            .execution_snapshot(&hash)
            .await
            .expect("the venue answers");
        live::consume_snapshot(&broker, &snapshot).expect("the reading applies");
    }
    let state = broker.state.lock();
    let wait = state.live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap();
    assert_eq!(wait.started, started, "the origin of the wait is frozen");
    assert!(wait.pending);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "no new order was sent");
}

#[tokio::test]
async fn raw_unknown_or_wrong_venue_identity_never_becomes_healthy_pending() {
    let _logs = tracing::subscriber::set_default(tracing_subscriber::fmt()
        .with_ansi(false).with_writer(std::io::stderr).finish());
    for status in ["UNRECOGNIZED", "private-free-text-must-not-leak"] {
        let (broker, clock, market, book, candidate, _, trace) =
            sending_fixture_with_trace("controlled_fill").await;
        *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        *trace.read_script.status.lock().unwrap() = Some(status.into());
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        assert!(!broker.state.lock().live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().has_lifetime());
        clock.advance(120_000);
        observe_wait(&broker);
        assert_eq!(stopped(&broker).unwrap().0, "FACT_INCOMPLETE");
        assert!(broker.state.lock().live.reservations[&id] > U256::ZERO);
    }
    let (broker, _, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    *trace.read_script.venue_id.lock().unwrap() = Some("foreign-order".into());
    let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
    assert!(broker.taker.as_ref().unwrap().execution_snapshot(identity.signed_hash.as_ref().unwrap()).await.is_err());
    assert!(broker.state.lock().live.reservations[&id] > U256::ZERO);
}

#[tokio::test]
async fn native_zero_order_survives_both_observed_stop_intervals_and_all_reads() {
    // Synthetic OPEN is an explicit scenario, not a recovered historical status.
    for (old_stop_ms, reads) in [(121_792u64, 107u64), (123_679, 105)] {
        let (broker, clock, market, book, candidate, transport, trace) = sending_fixture_with_trace("controlled_fill").await;
        *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        let hash = broker.state.lock().live.attempts[&id].lock().identity.signed_hash.clone().unwrap();
        let origin = broker.state.lock().live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().started;
        let mut elapsed = 0;
        for read in 1..=reads {
            let next = old_stop_ms * read / reads;
            clock.advance(next - elapsed); elapsed = next;
            let snapshot = broker.taker.as_ref().unwrap().execution_snapshot(&hash).await.unwrap();
            live::consume_snapshot(&broker, &snapshot).unwrap();
            assert!(stopped(&broker).is_none());
        }
        let state = broker.state.lock();
        assert_eq!(state.live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().started, origin);
        assert!(state.live.reservations[&id] > U256::ZERO);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        assert!(trace.read_script.order_reads.load(Ordering::SeqCst) >= (reads * 2) as usize);
    }
}

#[tokio::test]
async fn only_authoritative_terminal_facts_release_zero_order_reservation() {
    use crate::service::observation::Clock;
    use crate::service::venue::Closure;
    for status in ["OPEN", "UNRECOGNIZED", "EXPIRED", "CANCELLED", "FILLED"] {
        let (broker, clock, market, book, candidate, transport, trace) = sending_fixture_with_trace("controlled_fill").await;
        *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
        let venue = broker.taker.as_ref().unwrap();
        let expiry = venue.controlled_order_expiration(&identity).unwrap().unwrap();
        *trace.read_script.status.lock().unwrap() = Some(status.into());
        *trace.read_script.block_time.lock().unwrap() = Some(expiry as u64 + 1);
        let remaining = expiry * 1000 - clock.now().unwrap().utc.timestamp_millis();
        clock.advance(remaining as u64 + 1);
        let snapshot = venue.execution_snapshot(identity.signed_hash.as_ref().unwrap()).await.unwrap();
        let terminal = matches!(status, "EXPIRED" | "CANCELLED" | "FILLED");
        assert_eq!(matches!(snapshot.closure, Closure::Final { .. }), terminal);
        live::consume_snapshot(&broker, &snapshot).unwrap();
        assert_eq!(broker.state.lock().live.reservations[&id].is_zero(), terminal);
        if terminal {
            assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_some(),
                "Final releases the original reservation but is not account recovery");
            live::check_runtime_account(&broker, false).await.unwrap();
            assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_none());
            live::consume_snapshot(&broker, &snapshot).unwrap();
            assert_eq!(broker.state.lock().live.net_cash[&id], U256::ZERO);
        } else {
            assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_some());
        }
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_late_final_cannot_erase_the_exhausted_lifecycle_or_reopen_risk() {
    use crate::service::observation::Clock;
    let (broker, clock, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
    let expiry = broker.taker.as_ref().unwrap().controlled_order_expiration(&identity).unwrap().unwrap();
    clock.advance((expiry * 1000 - clock.now().unwrap().utc.timestamp_millis()) as u64 + 120_000);
    *trace.read_script.status.lock().unwrap() = Some("EXPIRED".into());
    *trace.read_script.block_time.lock().unwrap() = Some(expiry as u64 + 1);
    let snapshot = broker.taker.as_ref().unwrap().execution_snapshot(identity.signed_hash.as_ref().unwrap()).await.unwrap();
    live::consume_snapshot(&broker, &snapshot).unwrap();
    assert_eq!(stopped(&broker).unwrap().0, "FACT_INCOMPLETE");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.order_wait.as_ref().unwrap().exhausted);
    assert!(!run.may_add_risk());
}

#[tokio::test]
async fn fixed_block_expiry_equality_needs_an_independent_chain_or_nonce_proof() {
    use crate::service::venue::Closure;
    let (broker, _, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
    let venue = broker.taker.as_ref().unwrap();
    let expiry = venue.controlled_order_expiration(&identity).unwrap().unwrap();
    *trace.read_script.status.lock().unwrap() = Some("EXPIRED".into());
    *trace.read_script.block_time.lock().unwrap() = Some(expiry as u64);
    for (flags, final_expected) in [((false, true), false), ((true, true), true), ((false, false), true)] {
        *trace.read_script.close_flags.lock().unwrap() = Some(flags);
        let snapshot = venue.execution_snapshot(identity.signed_hash.as_ref().unwrap()).await.unwrap();
        assert_eq!(matches!(snapshot.closure, Closure::Final { .. }), final_expected);
    }
    // None of these reads were applied: querying a proof does not release risk.
    assert!(broker.state.lock().live.reservations[&id] > U256::ZERO);
}

#[tokio::test]
async fn unknown_pages_and_duplicate_generations_cannot_clear_an_expired_missing_streak() {
    let (broker, clock, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    let hash = broker.state.lock().live.attempts[&id].lock().identity.signed_hash.clone().unwrap();
    let mut snapshot = broker.taker.as_ref().unwrap().execution_snapshot(&hash).await.unwrap();
    snapshot.pages_complete = false;
    live::consume_snapshot(&broker, &snapshot).unwrap();
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().missing_since.is_some());
    clock.advance(120_000);
    snapshot.pages_complete = true;
    snapshot.generation += 1;
    live::consume_snapshot(&broker, &snapshot).unwrap();
    assert_eq!(stopped(&broker).unwrap().0, "FACT_INCOMPLETE");
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
}

#[tokio::test]
async fn hard_parent_stop_survives_a_later_valid_final_without_clearing_the_wait() {
    let (broker, _, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
    let expiry = broker.taker.as_ref().unwrap().controlled_order_expiration(&identity).unwrap().unwrap();
    broker.stop_entries(crate::bot::runtime::ControlledStop::ParentLost);
    *trace.read_script.status.lock().unwrap() = Some("EXPIRED".into());
    *trace.read_script.block_time.lock().unwrap() = Some(expiry as u64 + 1);
    let snapshot = broker.taker.as_ref().unwrap().execution_snapshot(identity.signed_hash.as_ref().unwrap()).await.unwrap();
    live::consume_snapshot(&broker, &snapshot).unwrap();
    assert_eq!(stopped(&broker).unwrap().0, "PARENT_LOST");
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_some());
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
}

struct WaitBooks { market: TradableMarket, book: BookSnapshot }
#[async_trait::async_trait]
impl crate::service::venue::MarketData for WaitBooks {
    async fn markets(&self, _: &crate::service::venue::MarketQuery) -> anyhow::Result<Vec<TradableMarket>> {
        Ok(vec![self.market.clone()])
    }
    async fn market(&self, _: u64) -> anyhow::Result<TradableMarket> { Ok(self.market.clone()) }
    async fn with_schedule(&self, market: &TradableMarket) -> anyhow::Result<TradableMarket> { Ok(market.clone()) }
    async fn books(&self, ids: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, BookSnapshot>> {
        Ok(ids.iter().map(|id| (*id, self.book.clone())).collect())
    }
}

struct ResumeSecondRound {
    clock: Arc<crate::service::observation::FixtureClock>,
    calls: usize,
    first_id: u64,
    first_market: TradableMarket,
    first_book: BookSnapshot,
    first_candidate: CandidateContext,
    core: crate::bot::arbitrage::DecisionCore,
}
#[async_trait::async_trait]
impl crate::bot::runtime::Strategy for ResumeSecondRound {
    fn name(&self) -> &'static str { "r17-order-wait-drive" }
    async fn tick(&mut self, ctx: &crate::bot::runtime::Ctx) -> anyhow::Result<()> {
        self.calls += 1;
        if self.calls == 1 {
            self.first_id = place_first_order(&ctx.broker, &self.first_market,
                &self.first_book, &self.first_candidate).await.0;
            return Ok(());
        }
        assert_eq!(self.calls, 2, "no strategy runs while the first order is pending");
        assert!(ctx.broker.state.lock().live.reservations[&self.first_id].is_zero());
        use crate::service::observation::{Clock, CheckTime, ObservationClock, FreshnessBasis, SourceTimeStatus};
        let now = self.clock.now()?;
        // Warm the shared core with distinct past observations, not by moving the
        // run 21s past the native signer's still-real clock for its next order.
        for seconds in (0..=20).rev() {
            let checked = CheckTime { utc: now.utc - chrono::Duration::seconds(seconds),
                elapsed_ms: now.elapsed_ms - seconds as u64 * 1000 };
            let receipt = ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
                freshness_basis: FreshnessBasis::ReceiptBound,
                request_at: checked.utc, received_at: checked.utc,
                request_elapsed_ms: checked.elapsed_ms, received_elapsed_ms: checked.elapsed_ms };
            self.core.observe_phase1("BTCUSDT", 100.2, crate::service::spot::SpotSource::Binance,
                "BTCUSDT", &receipt, checked).unwrap();
        }
        let (market, book, evaluation) = evaluate_live_round_with_core(&self.clock, false, 2, &mut self.core);
        live::check_runtime_account(&ctx.broker, false).await?;
        ctx.broker.observe(&market, &book);
        let candidate = evaluation.candidate.unwrap();
        assert!(ctx.broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
        let ack = ack_next(ctx.broker.clone(), false);
        let outcome = ctx.broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate,
        }).await?;
        ack.join().unwrap();
        assert!(outcome.attempt_id.is_some_and(|id| id != self.first_id));
        ctx.broker.stop_entries(crate::bot::runtime::ControlledStop::ParentLost);
        Ok(())
    }
}

#[tokio::test]
async fn original_drive_observes_zero_then_resumes_a_second_post_on_the_same_adapter() {
    drive_zero_order_through_expiry(None).await;
}

#[tokio::test]
async fn frozen_first_run_actual_drive_crosses_stop_round_end_and_native_expiry() {
    drive_zero_order_through_expiry(Some(("2026-11-24T01:12:51.776Z",
        "2026-11-24T01:14:53.568Z", "2026-11-24T01:15:00Z",
        "2026-11-24T01:17:50Z"))).await;
}

#[tokio::test]
async fn frozen_second_run_actual_drive_crosses_round_end_stop_and_native_expiry() {
    drive_zero_order_through_expiry(Some(("2026-11-24T12:39:02.170Z",
        "2026-11-24T12:41:05.849Z", "2026-11-24T12:40:00Z",
        "2026-11-24T12:44:00Z"))).await;
}

async fn drive_zero_order_through_expiry(history: Option<(&str, &str, &str, &str)>) {
    use crate::service::observation::{Clock, ObservationClock, FreshnessBasis, SourceTimeStatus};
    let _logs = tracing::subscriber::set_default(tracing_subscriber::fmt()
        .with_ansi(false).with_writer(std::io::stderr).finish());
    use crate::service::predict_fun::controlled::synthetic_controlled_two_round_trader;
    let (mut broker, clock) = risk_fixture();
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let (mut market, book, mut evaluation) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
    let historical = history.map(|(accepted, stop, end, expiry)| {
        let parse = |text: &str| chrono::DateTime::parse_from_rfc3339(text).unwrap()
            .with_timezone(&chrono::Utc);
        (parse(accepted), parse(stop), parse(end), parse(expiry))
    });
    if let Some((accepted, _, end, _)) = historical {
        market.ends_at = Some(clock.now().unwrap().utc + (end - accepted));
        market.starts_at = Some(market.ends_at.unwrap() - chrono::Duration::seconds(300));
        let now = clock.now().unwrap();
        let receipt = ObservationClock { source_at: None,
            source_time_status: SourceTimeStatus::Missing, freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: now.utc, received_at: now.utc,
            request_elapsed_ms: now.elapsed_ms, received_elapsed_ms: now.elapsed_ms };
        evaluation = core.evaluate_phase1(&crate::bot::arbitrage::phase1::EvaluationInputs {
            market: &market, book: &book, spot_price: 100.2,
            spot_source: Some(crate::service::spot::SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
            spot_clock: Some(receipt.clone()), book_clock: Some(receipt), now,
            order_usd: 1.0, min_edge: 0.03, min_secs_left: 30.0, entered: false,
            identity_available_at: market.starts_at, strike_available_at: market.starts_at,
            settlement_provider: crate::bot::arbitrage::phase1::SettlementProvider::Chainlink,
        });
    }
    let candidate = evaluation.candidate.unwrap();
    let (trader, transport, trace) = synthetic_controlled_two_round_trader(candidate.fee_rate_bps());
    let baseline = trader.account_snapshot().await.unwrap();
    broker.identity = Some(trader.identity());
    broker.gate = Some(trader.gate.clone());
    let trader = Arc::new(trader);
    broker.taker = Some(trader.clone());
    broker.limit = Some(trader.clone());
    {
        let mut state = broker.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = trader.identity();
        run.options.gas_policy.payer_address = trader.identity().signer_address;
        let receipt = super::account_receipt(&state).unwrap();
        super::record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    let broker = Arc::new(broker);
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let mut cfg = ctx_fixture(broker.identity.as_ref().unwrap()); cfg.strategies.poll_secs = 1;
    let client = crate::service::predict_fun::PredictFunClient::new(&cfg).unwrap();
    let ctx = crate::bot::runtime::Ctx { cfg, client, broker: broker.clone(),
        data: Arc::new(WaitBooks { market: market.clone(), book: book.clone() }) };
    let mut strategy = ResumeSecondRound { clock: clock.clone(), calls: 0, first_id: 0,
        first_market: market, first_book: book, first_candidate: candidate, core };
    let drive = crate::bot::runtime::drive(&mut strategy, &ctx);
    tokio::pin!(drive);
    tokio::select! {
        result = &mut drive => panic!("pending drive returned early: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(1100)) => (),
    }
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert!(trace.read_script.order_reads.load(Ordering::SeqCst) >= 4);
    let identity = broker.state.lock().live.attempts[&1].lock().identity.clone();
    let expiry = broker.taker.as_ref().unwrap().controlled_order_expiration(&identity).unwrap().unwrap();
    let accepted_clock = clock.now().unwrap();
    let checkpoints = historical.map(|(accepted, stop, end, _)| {
        let mut points = vec![(stop - accepted).num_milliseconds() as u64,
            (end - accepted).num_milliseconds() as u64 + 1];
        points.sort_unstable();
        points
    }).unwrap_or_else(|| vec![123_679]);
    for offset in checkpoints {
        let elapsed = clock.now().unwrap().elapsed_ms - accepted_clock.elapsed_ms;
        clock.advance(offset - elapsed);
        tokio::select! {
            result = &mut drive => panic!("drive stopped inside signed lifetime: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(1100)) => (),
        }
        assert!(stopped(&broker).is_none());
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        assert!(broker.state.lock().live.reservations[&1] > U256::ZERO);
    }
    // Native signing still uses its real clock. Replay historical stop/end offsets,
    // then age the injected runtime clock to THIS signed expiry, not an invented
    // early chain Final. The historical expiry is provenance, never a guessed status.
    let remaining = expiry * 1000 - clock.now().unwrap().utc.timestamp_millis();
    assert!(remaining > 0);
    clock.advance(remaining as u64 + 1);
    if let Some((accepted, stop, end, historical_expiry)) = historical {
        eprintln!("R17_FROZEN_DRIVE accepted={accepted} stop={stop} market_end={end}             historical_expiry={historical_expiry} native_expiry={expiry}             runtime_at_final={} elapsed_ms={}", clock.now().unwrap().utc,
            clock.now().unwrap().elapsed_ms);
    }
    trace.read_script.statuses.lock().unwrap().insert(identity.signed_hash.unwrap(), "EXPIRED".into());
    *trace.read_script.block_time.lock().unwrap() = Some(expiry as u64 + 1);
    tokio::time::timeout(std::time::Duration::from_secs(6), &mut drive).await.unwrap().unwrap();
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2, "stop={:?}", stopped(&broker));
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(broker.state.lock().live.attempts.len(), 2);
}

#[tokio::test]
async fn read_failure_uses_its_own_120_seconds_and_recovery_cannot_reanchor_the_lifetime() {
    for recover in [false, true] {
        let (broker, clock, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
        *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        let origin = broker.state.lock().live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap().started;
        trace.read_script.fail_reads.store(true, Ordering::SeqCst);
        assert!(broker.sync_live().await.is_err());
        clock.advance(if recover { 119_999 } else { 120_000 });
        if recover { trace.read_script.fail_reads.store(false, Ordering::SeqCst); }
        let _ = broker.sync_live().await;
        if recover {
            assert!(stopped(&broker).is_none());
            let state = broker.state.lock();
            let wait = state.live.controlled.as_ref().unwrap().order_wait.as_ref().unwrap();
            assert_eq!(wait.started, origin);
            assert!(wait.missing_since.is_none());
        } else {
            assert_eq!(stopped(&broker).unwrap().0, "READ_UNAVAILABLE");
            assert!(broker.state.lock().live.reservations[&id] > U256::ZERO);
        }
    }
}

#[tokio::test]
async fn after_read_wrong_id_is_a_hard_conflict_not_a_missing_fact_grace() {
    let (broker, _, market, book, candidate, _, trace) = sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    let reads = trace.read_script.order_reads.load(Ordering::SeqCst);
    trace.read_script.id_after.store(reads + 1, Ordering::SeqCst);
    *trace.read_script.venue_id.lock().unwrap() = Some("wrong-after-id".into());
    assert!(broker.sync_live().await.is_err());
    assert_eq!(stopped(&broker).unwrap().0, "FACT_CONFLICT");
    assert!(broker.state.lock().live.reservations[&id] > U256::ZERO);
}

#[tokio::test]
async fn partial_and_late_fill_keep_the_original_ledger_and_reservation_rules() {
    let (broker, clock, market, book, candidate, transport, _) = sending_fixture_with_trace("controlled_partial").await;
    let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
    {
        let state = broker.state.lock();
        assert!(state.live.net_inventory[&id] > U256::ZERO);
        assert!(state.live.reservations[&id] > U256::ZERO);
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    }
    clock.advance(124_000);
    transport.stage.store(1, Ordering::SeqCst);
    broker.observe(&market, &book);
    broker.sync_live().await.unwrap();
    let state = broker.state.lock();
    assert_eq!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
    assert!(state.live.reservations[&id].is_zero());
    assert!(state.live.controlled.as_ref().unwrap().order_wait.is_none());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

mod r17_reviewer_regressions {
    use super::*;
    use crate::service::observation::{
        Clock, FreshnessBasis, ObservationClock, ObservedBook, SourceTimeStatus,
    };
    use std::collections::HashMap;

    struct ReceiptBooks { inner: WaitBooks, case: &'static str }
    #[async_trait::async_trait]
    impl crate::service::venue::MarketData for ReceiptBooks {
        async fn markets(&self, _: &crate::service::venue::MarketQuery)
            -> anyhow::Result<Vec<TradableMarket>> { Ok(vec![self.inner.market.clone()]) }
        async fn market(&self, _: u64) -> anyhow::Result<TradableMarket> {
            Ok(self.inner.market.clone())
        }
        async fn with_schedule(&self, market: &TradableMarket) -> anyhow::Result<TradableMarket> {
            Ok(market.clone())
        }
        async fn books(&self, _: &[u64]) -> anyhow::Result<HashMap<u64, BookSnapshot>> {
            Ok(HashMap::new())
        }
        async fn observed_books(&self, _: &[u64], clock: &dyn Clock)
            -> anyhow::Result<HashMap<u64, ObservedBook>> {
            let now = clock.now()?;
            if self.case == "missing" { return Ok(HashMap::new()); }
            if self.case == "failed" { anyhow::bail!("SYNTHETIC_READ_FAILED"); }
            let delay = if self.case == "slow" { 60_000 } else { 5_000 };
            let mut observation = ObservationClock {
                    source_at: None, source_time_status: SourceTimeStatus::Missing,
                    freshness_basis: FreshnessBasis::ReceiptBound,
                    request_at: now.utc - chrono::Duration::milliseconds(delay),
                    received_at: now.utc - chrono::Duration::milliseconds(4_000),
                    request_elapsed_ms: now.elapsed_ms.saturating_sub(delay as u64),
                    received_elapsed_ms: now.elapsed_ms - 4_000,
            };
            match self.case {
                "slow" => { observation.received_at = now.utc;
                    observation.received_elapsed_ms = now.elapsed_ms; }
                "stale" => { observation.request_at = now.utc - chrono::Duration::milliseconds(11_000);
                    observation.request_elapsed_ms = now.elapsed_ms - 11_000;
                    observation.received_at = observation.request_at;
                    observation.received_elapsed_ms = observation.request_elapsed_ms; }
                "future_wall" => observation.received_at = now.utc + chrono::Duration::milliseconds(1),
                "future_elapsed" => observation.received_elapsed_ms = now.elapsed_ms + 1,
                "inconsistent" => observation.request_at = now.utc + chrono::Duration::seconds(1),
                _ => (),
            }
            Ok(HashMap::from([(self.inner.market.id, ObservedBook {
                book: self.inner.book.clone(), clock: observation,
            })]))
        }
    }

    #[tokio::test]
    async fn reviewer_stale_acquisition_must_not_be_restamped_as_fresh_mark() {
        let (broker, clock, market, book, candidate, _, _) =
            sending_fixture_with_trace("controlled_partial").await;
        place_first_order(&broker, &market, &book, &candidate).await;
        clock.advance(61_000);
        let old_receipt = broker.state.lock().live.controlled.as_ref().unwrap()
            .mark_receipts.get(&market.yes_token).copied();
        let data = ReceiptBooks { inner: WaitBooks { market: market.clone(), book }, case: "slow" };
        order_wait::observe_wait_tick(&broker, &data).await.unwrap();
        assert_eq!(broker.state.lock().live.controlled.as_ref().unwrap()
            .mark_receipts.get(&market.yes_token).copied(), old_receipt,
            "a 60s acquisition must not be stamped now and clear valuation degradation");
    }

    #[tokio::test]
    async fn actual_wait_observations_preserve_receipts_and_reject_unusable_books() {
        for case in ["fresh", "slow", "stale", "future_wall", "future_elapsed",
                     "inconsistent", "missing", "failed"] {
            let (broker, clock, market, book, candidate, transport, _) =
                sending_fixture_with_trace("controlled_partial").await;
            place_first_order(&broker, &market, &book, &candidate).await;
            clock.advance(61_000);
            let now = clock.now().unwrap();
            let data = ReceiptBooks { inner: WaitBooks { market: market.clone(), book }, case };
            order_wait::observe_wait_tick(&broker, &data).await.unwrap();
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            if case == "fresh" {
                let receipt = run.mark_receipts[&market.yes_token];
                assert_eq!(receipt.utc, now.utc - chrono::Duration::milliseconds(4_000));
                assert_eq!(receipt.elapsed_ms, now.elapsed_ms - 4_000);
                assert!(!run.degraded());
            } else {
                assert!(!run.mark_receipts.contains_key(&market.yes_token), "{case}");
                assert!(run.degraded(), "{case}");
            }
            assert!(!run.may_add_risk());
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn final_recovery_needs_current_account_and_valid_marks_inside_both_limits() {
        for boundary in ["within", "wall_equal", "elapsed_equal", "stale_account"] {
            let (broker, clock, market, book, candidate, transport, _) =
                sending_fixture_with_trace("controlled_partial").await;
            let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
            broker.observe(&market, &book);
            clock.advance(11_000);
            transport.stage.store(1, Ordering::SeqCst);
            broker.sync_live().await.unwrap();
            let wait = broker.state.lock().live.controlled.as_ref().unwrap()
                .order_wait.as_ref().unwrap().clone();
            let now = clock.now().unwrap();
            match boundary {
                "wall_equal" => clock.set_utc(chrono::DateTime::from_timestamp_millis(
                    wait.wall_limit_ms.unwrap()).unwrap()),
                "elapsed_equal" => {
                    clock.advance(wait.elapsed_limit_ms.unwrap() - now.elapsed_ms);
                    clock.set_utc(now.utc);
                }
                "stale_account" => clock.advance(11_000),
                _ => (),
            }
            broker.observe(&market, &book);
            if boundary.ends_with("equal") {
                assert_eq!(stopped(&broker).unwrap().0, "FACT_INCOMPLETE");
                assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_some());
            } else if boundary == "stale_account" {
                assert!(broker.state.lock().live.controlled.as_ref().unwrap().order_wait.is_some());
                assert!(!broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
                broker.sync_live().await.unwrap();
            }
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.may_add_risk(), !boundary.ends_with("equal"), "{boundary}");
            assert!(state.live.reservations[&id].is_zero());
            assert_eq!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn reviewer_final_without_valid_mark_must_keep_observation_wait() {
        let (broker, clock, market, book, candidate, transport, _) =
            sending_fixture_with_trace("controlled_partial").await;
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        broker.observe(&market, &book);
        clock.advance(11_000);
        transport.stage.store(1, Ordering::SeqCst);
        broker.sync_live().await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
        assert!(run.degraded());
        assert!(!run.may_add_risk());
        assert!(run.order_wait.is_some(), "Final alone must not clear the wait before recovery");
    }

    #[tokio::test]
    async fn reviewer_final_with_valuation_gap_cannot_recover_beyond_frozen_lifecycle() {
        let (broker, clock, market, book, candidate, transport, _) =
            sending_fixture_with_trace("controlled_partial").await;
        let (id, _) = place_first_order(&broker, &market, &book, &candidate).await;
        let identity = broker.state.lock().live.attempts[&id].lock().identity.clone();
        let expiry = broker.taker.as_ref().unwrap()
            .controlled_order_expiration(&identity).unwrap().unwrap();
        broker.observe(&market, &book);
        clock.advance(11_000);
        transport.stage.store(1, Ordering::SeqCst);
        broker.sync_live().await.unwrap();
        assert!(broker.state.lock().live.controlled.as_ref().unwrap().degraded());
        clock.set_utc(chrono::DateTime::from_timestamp(expiry + 121, 0).unwrap());
        broker.observe(&market, &book);
        assert!(!broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk(),
            "deferred mark recovery after frozen expiry+120 must not reopen risk");
    }
}
