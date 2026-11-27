use super::*;
use crate::service::observation::Clock;

fn expired_fixture() -> (Broker, Arc<crate::service::observation::FixtureClock>, OfficialResultData) {
    let (broker, clock) = risk_fixture();
    let (mut market, _, _) = evaluate_live_fixture(&clock, false);
    market.yes_token = "1".into();
    apply_risk_fill(&broker, market.id, 2 * SCALE, true);
    broker.state.lock().live.controlled.as_mut().unwrap().traded.insert(market.id, market.clone());
    let delay = (market.ends_at.unwrap() - clock.now().unwrap().utc).num_milliseconds();
    clock.advance(delay as u64 + 1);
    (broker, clock, OfficialResultData { result: Mutex::new(unpublished(market)), reads: Default::default() })
}

#[tokio::test]
async fn retry_does_not_rearm_on_pending_open_or_a_failed_read() {
    for outcome in ["pending", "open", "read", "conflict", "sticky", "budget", "deadline", "rollback"] {
        let (broker, clock, data) = expired_fixture();
        broker.settle_resolved(&data).await.unwrap();
        let first = broker.state.lock().live.controlled.as_ref().unwrap().settlement_recheck.unwrap().started;
        clock.advance(61_000);
        {
            let mut state = broker.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            run.stop_expired_recheck();
            run.clear_failure(FailureKind::Settlement);
            assert_eq!(run.settlement_recheck.unwrap().started, first);
            match outcome {
                "sticky" => run.stop("LOSS_LATCH", true, clock.now().unwrap().utc),
                "deadline" => run.run_deadline_elapsed_ms = clock.now().unwrap().elapsed_ms,
                "budget" => clock.advance(539_000),
                "rollback" => clock.set_utc(first.utc + chrono::Duration::seconds(30)),
                "open" => data.result.lock().round_facts.as_mut().unwrap().trading_status = Some("OPEN".into()),
                "conflict" => data.result.lock().yes_token = "bad-token".into(),
                _ => (),
            }
        }
        if outcome == "read" {
            assert!(broker.settle_resolved(&ScriptedRead { data: &data, fail: true }).await.is_err());
        } else {
            broker.settle_resolved(&data).await.unwrap();
        }
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let reason = match outcome { "conflict" => "FACT_CONFLICT", "sticky" => "LOSS_LATCH",
            "deadline" => "DEADLINE", _ => "DATA_UNAVAILABLE" };
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, reason, "{outcome}");
        assert!(run.must_end() && !run.may_add_risk(), "{outcome}");
        assert_eq!(run.settlement_recheck.unwrap().started, first, "{outcome}");
    }
}

#[tokio::test]
async fn incomplete_execution_never_arms_and_other_market_does_not_replace_wait() {
    for fault in ["unknown", "reservation", "other-market"] {
        let (broker, _, data) = expired_fixture();
        {
            let mut state = broker.state.lock();
            match fault {
                "unknown" => state.live.attempts[&1].lock().submission = Submission::Unknown,
                "reservation" => { state.live.reservations.insert(1, U256::from(1)); },
                _ => {
                    let run = state.live.controlled.as_mut().unwrap();
                    let mut other = run.traded[&1].clone();
                    other.id = 2;
                    run.traded.insert(2, other);
                }
            }
        }
        broker.settle_resolved(&ScriptedRead { data: &data, fail: false }).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        if fault == "other-market" {
            assert_eq!(run.settlement_recheck.unwrap().market_id, 1);
            assert!(!run.must_end() && !run.may_add_risk());
        } else {
            assert!(run.settlement_recheck.is_none());
            assert!(run.must_end());
        }
    }
}

struct ScriptedRead<'a> { data: &'a OfficialResultData, fail: bool }

#[async_trait::async_trait]
impl crate::service::venue::MarketData for ScriptedRead<'_> {
    async fn markets(&self, _: &crate::service::venue::MarketQuery)
        -> anyhow::Result<Vec<crate::service::venue::TradableMarket>> { panic!("unexpected scan") }
    async fn market(&self, id: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
        use crate::service::venue::{VenueError, VenueErrorCode};
        if self.fail { return Err(VenueError::new(VenueErrorCode::ReadUnavailable, "settlement", "READ_UNAVAILABLE").into()); }
        let mut market = self.data.market(id).await?;
        market.id = id;
        if id == 2 { market.round_facts.as_mut().unwrap().trading_status = Some("OPEN".into()); }
        Ok(market)
    }
    async fn with_schedule(&self, _: &crate::service::venue::TradableMarket)
        -> anyhow::Result<crate::service::venue::TradableMarket> { panic!("unexpected schedule") }
    async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> {
        panic!("unexpected books")
    }
}

#[tokio::test]
async fn expired_matched_closed_keeps_original_run_observing_without_entry() {
    let (broker, clock) = risk_fixture();
    let (mut market, _, _) = evaluate_live_fixture(&clock, false);
    market.yes_token = "1".into();
    apply_risk_fill(&broker, market.id, 2 * SCALE, true);
    broker.state.lock().live.controlled.as_mut().unwrap().traded.insert(market.id, market.clone());
    let now = clock.now().unwrap();
    clock.advance((market.ends_at.unwrap() - now.utc).num_milliseconds().max(0) as u64 + 1);
    market.open = false;
    market.end_price = None;
    market.payout_yes = None;
    market.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("REGISTERED".into()), trading_status: Some("CLOSED".into()),
        yes_status: None, no_status: None, decimal_precision: None, price_feed_provider: None,
    });
    let data = OfficialResultData { result: Mutex::new(market), reads: Default::default() };
    let ledger = |state: &State| (state.live.net_cash.clone(), state.live.net_inventory.clone(),
        state.live.reservations.clone(), state.live.controlled.as_ref().unwrap().attempt_outcomes.clone());
    let before = ledger(&broker.state.lock());
    broker.settle_resolved(&data).await.unwrap();
    let mut state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(data.reads.load(Ordering::SeqCst), 1);
    assert_eq!(ledger(&state), before);
    assert!(!run.must_end(), "verified unpublished CLOSED terminated the original run: {:?}",
        run.control.lock().stop);
    assert!(!run.may_add_risk());
    assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)), Some(Blocked::Halted));
}

mod batch_cutoff {
    use super::*;
    use crate::service::{observation::FixtureClock, venue::{MarketData, MarketQuery, TradableMarket}};
    use std::time::Duration;

    struct Batch<'a> {
        clock: &'a FixtureClock,
        target: TradableMarket,
        stall: u64,
        calls: Mutex<Vec<u64>>,
    }

    #[async_trait::async_trait]
    impl MarketData for Batch<'_> {
        async fn markets(&self, _: &MarketQuery) -> anyhow::Result<Vec<TradableMarket>> { panic!("unexpected scan") }
        async fn market(&self, id: u64) -> anyhow::Result<TradableMarket> {
            self.calls.lock().push(id);
            if id == self.stall {
                if id < self.target.id { self.clock.advance(50); }
                return std::future::pending().await;
            }
            assert_eq!(id, self.target.id);
            Ok(self.target.clone())
        }
        async fn with_schedule(&self, _: &TradableMarket) -> anyhow::Result<TradableMarket> { panic!("unexpected schedule") }
        async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> { panic!("unexpected books") }
    }

    fn waiting_market(id: u64) -> (Broker, Arc<FixtureClock>, TradableMarket) {
        let (broker, clock) = risk_fixture();
        let (mut market, _, _) = evaluate_live_fixture(&clock, false);
        market.id = id;
        market.yes_token = id.to_string();
        apply_risk_fill(&broker, market.id, 2 * SCALE, true);
        broker.state.lock().live.controlled.as_mut().unwrap().traded.insert(id, market.clone());
        let delay = (market.ends_at.unwrap() - clock.now().unwrap().utc).num_milliseconds();
        clock.advance(delay as u64 + 1);
        (broker, clock, unpublished(market))
    }

    #[tokio::test]
    async fn deadline_bounds_preceding_market_in_next_original_poll() {
        let (broker, clock, target) = waiting_market(2);
        let first = OfficialResultData { result: Mutex::new(target.clone()), reads: Default::default() };
        broker.settle_resolved(&first).await.unwrap();
        clock.advance(61_000);
        {
            let mut state = broker.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            assert_eq!(run.settlement_recheck.unwrap().market_id, 2);
            run.run_deadline_elapsed_ms = clock.now().unwrap().elapsed_ms + 50;
            let mut other = target.clone();
            other.id = 1;
            run.traded.insert(1, other);
        }
        let batch = Batch { clock: &clock, target, stall: 1, calls: Default::default() };
        let complete = tokio::time::timeout(Duration::from_millis(250), broker.settle_resolved(&batch)).await;
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let stop = run.control.lock().stop.clone();
        assert!(run.must_end() && !run.may_add_risk());
        assert!(complete.is_ok(), "original poll still awaiting unrelated market past the run deadline: calls={:?}, stop={stop:?}", batch.calls.lock());
        assert_eq!(stop.unwrap().reason, "DEADLINE");
        assert_eq!(&*batch.calls.lock(), &[1]);
    }

    #[tokio::test]
    async fn failed_target_does_not_await_another_market_before_returning_to_drive() {
        let (broker, clock, target) = waiting_market(1);
        let first = OfficialResultData { result: Mutex::new(target.clone()), reads: Default::default() };
        broker.settle_resolved(&first).await.unwrap();
        clock.advance(61_000);
        {
            let mut state = broker.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            assert_eq!(run.settlement_recheck.unwrap().market_id, 1);
            let mut other = target.clone();
            other.id = 2;
            run.traded.insert(2, other);
        }
        let batch = Batch { clock: &clock, target, stall: 2, calls: Default::default() };
        let complete = tokio::time::timeout(Duration::from_millis(250), broker.settle_resolved(&batch)).await;
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let stop = run.control.lock().stop.clone().unwrap();
        assert_eq!(stop.reason, "DATA_UNAVAILABLE");
        assert!(run.must_end() && !run.may_add_risk());
        assert!(complete.is_ok(), "original poll opened/awaited an unrelated read after stopping: calls={:?}", batch.calls.lock());
        assert_eq!(&*batch.calls.lock(), &[1]);
    }

    #[tokio::test]
    async fn review_4191_market_owner_at_deadline_does_not_consume_result() {
        use crate::service::observation::CheckTime;
        use std::sync::atomic::AtomicUsize;
        struct BoundaryClock { base: CheckTime, reads: AtomicUsize }
        impl Clock for BoundaryClock {
            fn now(&self) -> anyhow::Result<CheckTime> {
                // The first eight samples stay within budget. The ninth is
                // apply_official_result's decision and audit timestamp.
                let crossed = self.reads.fetch_add(1, Ordering::SeqCst) >= 8;
                let delta = if crossed { 50 } else { 0 };
                Ok(CheckTime { elapsed_ms: self.base.elapsed_ms + delta,
                    utc: self.base.utc + chrono::Duration::milliseconds(delta as i64) })
            }
        }
        let (broker, clock, target) = waiting_market(1);
        broker.settle_resolved(&OfficialResultData { result: Mutex::new(target.clone()), reads: Default::default() }).await.unwrap();
        clock.advance(61_000);
        let now = clock.now().unwrap();
        let boundary = Arc::new(BoundaryClock { base: now, reads: AtomicUsize::new(0) });
        let ledger = |state: &State| (state.live.net_cash.clone(), state.live.net_inventory.clone(),
            state.live.reservations.clone(), state.live.claims.clone(), state.live.pending_claims.clone());
        let (before, receipt_checked) = {
            let mut state = broker.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            run.run_deadline_elapsed_ms = now.elapsed_ms + 50;
            run.phase1.clock = boundary.clone();
            assert!(run.attempt_outcomes.is_empty());
            let checked = run.account_read.as_ref().unwrap().checked;
            (ledger(&state), checked)
        };
        let mut confirmed = target;
        confirmed.start_price = Some(100.0);
        confirmed.end_price = Some(99.0);
        confirmed.payout_yes = Some(0.0);
        let facts = confirmed.round_facts.as_mut().unwrap();
        facts.oracle_status = Some("RESOLVED".into());
        facts.yes_status = Some("LOST".into());
        facts.no_status = Some("WON".into());
        let data = OfficialResultData { result: Mutex::new(confirmed), reads: Default::default() };
        broker.settle_resolved(&data).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(data.reads.load(Ordering::SeqCst), 1);
        let stop = run.control.lock().stop.clone().unwrap();
        assert_eq!(stop.reason, "DEADLINE");
        assert_eq!(stop.at, now.utc + chrono::Duration::milliseconds(50));
        assert!(run.must_end() && !run.may_add_risk());
        assert!(run.attempt_outcomes.is_empty(), "market result consumed at hard deadline; clock reads={}", boundary.reads.load(Ordering::SeqCst));
        assert!(ledger(&state) == before, "original holding or account changed at cutoff");
        assert_eq!(run.account_read.as_ref().unwrap().checked, receipt_checked);
    }
}

fn unpublished(mut market: crate::service::venue::TradableMarket) -> crate::service::venue::TradableMarket {
    market.open = false;
    market.end_price = None;
    market.payout_yes = None;
    let facts = market.round_facts.as_mut().unwrap();
    facts.oracle_status = Some("REGISTERED".into());
    facts.trading_status = Some("CLOSED".into());
    facts.yes_status = None;
    facts.no_status = None;
    market
}

#[tokio::test]
async fn next_poll_reconciles_real_adapter_before_the_original_gate_reopens() {
    use crate::service::predict_fun::controlled::r16_fixture;
    use crate::service::venue::OrderVenue;
    for payout in [0.0, 1.0] {
        let (mut broker, clock) = risk_fixture();
        clock.set_utc(Utc::now() - chrono::Duration::minutes(10));
        let (trader, transport, _, facts) = r16_fixture::trader();
        let opening = trader.account_snapshot().await.unwrap();
        broker.identity = Some(trader.identity());
        broker.gate = Some(trader.gate.clone());
        let trader = Arc::new(trader);
        broker.taker = Some(trader.clone());
        broker.limit = Some(trader.clone());
        let broker = Arc::new(broker);
        {
            let mut state = broker.state.lock();
            state.live.baseline = Some(opening.clone());
            let run = state.live.controlled.as_mut().unwrap();
            run.identity = trader.identity();
            run.options.gas_policy.payer_address = trader.identity().signer_address;
            let receipt = account_receipt(&state).unwrap();
            record_account_read(&mut state, &opening, receipt).unwrap();
        }
        let (market, book, evaluation) = evaluate_live_fixture(&clock, true);
        facts.round(market.clone());
        live::check_runtime_account(&broker, false).await.unwrap();
        broker.observe(&market, &book);
        let candidate = evaluation.candidate.unwrap();
        assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
        let ack = ack_next(broker.clone(), false);
        let outcome = broker.take_live_candidate(crate::bot::runtime::taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate,
        }).await.unwrap();
        ack.join().unwrap();
        assert!(matches!(outcome.result, crate::bot::runtime::TakeResult::Filled { .. }));
        live::sync_predict(&broker).await.unwrap();
        let mut result = unpublished(market);
        clock.advance((result.ends_at.unwrap() - clock.now().unwrap().utc).num_milliseconds() as u64 + 1);
        let data = OfficialResultData { result: Mutex::new(result.clone()), reads: Default::default() };
        broker.settle_resolved(&data).await.unwrap();
        assert_eq!(broker.check_live_entry(&mut broker.state.lock(), 2, U256::from(SCALE)), Some(Blocked::Halted));
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        live::check_runtime_account(&broker, false).await.unwrap();
        assert!(broker.state.lock().live.controlled.as_ref().unwrap().settlement_recheck.is_some());
        clock.advance(61_000);
        result.end_price = Some(if payout == 0.0 { 99.0 } else { 101.0 });
        result.payout_yes = Some(payout);
        let facts_result = result.round_facts.as_mut().unwrap();
        facts_result.oracle_status = Some("RESOLVED".into());
        facts_result.yes_status = Some(if payout == 0.0 { "LOST" } else { "WON" }.into());
        facts_result.no_status = Some(if payout == 0.0 { "WON" } else { "LOST" }.into());
        facts.round(result.clone());
        if payout == 0.0 { facts.hidden.lock().unwrap().insert(77); }
        *data.result.lock() = result;
        broker.settle_resolved(&data).await.unwrap();
        let mut state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.settlement_recheck.is_none());
        assert_eq!(run.account_read.as_ref().unwrap().checked, clock.now().unwrap());
        assert_eq!(data.reads.load(Ordering::SeqCst), 2);
        assert!(state.live.claims.is_empty(), "a result alone never credits cash");
        assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)),
            if payout == 0.0 { None } else { Some(Blocked::Halted) });
    }
}
