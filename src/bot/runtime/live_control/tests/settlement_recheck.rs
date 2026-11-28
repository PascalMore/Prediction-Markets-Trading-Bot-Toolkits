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
    original_poll_real_adapter(false).await;
}

#[tokio::test]
async fn r22_does_not_consume_r3_retry() {
    original_poll_real_adapter(true).await;
}

#[tokio::test]
async fn r22_first_preserves_original_r3_retry_and_current_receipt_requirement() {
    use super::super::r16_cash_drive::{adapter_fixture, resolved, send_round};
    use crate::service::predict_fun::controlled::r16_fixture::{self, RedemptionFixture, RedemptionStage};
    use std::time::Instant;
    for (published, fresh) in [(false, false), (true, false), (true, true)] {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let first = send_round(&broker, &clock, &facts, &mut core, 1).await;
        clock.advance(300_000);
        let second = send_round(&broker, &clock, &facts, &mut core, 2).await;
        clock.advance((second.ends_at.unwrap() - clock.now().unwrap().utc).num_milliseconds() as u64 + 1);
        facts.round(resolved(first, 1.0));
        facts.round(unpublished(second.clone()));
        let quantity = broker.state.lock().live.net_inventory[&1];
        let stage = |stage| Some(RedemptionFixture::for_target(stage, 0xcc, quantity));
        *facts.redemption.lock().unwrap() = stage(RedemptionStage::Before);
        live::check_runtime_account(&broker, false).await.unwrap();
        *facts.redemption.lock().unwrap() = stage(RedemptionStage::Complete);
        facts.stale_positions.lock().unwrap().insert(77);
        facts.pay(77);
        live::check_runtime_account(&broker, false).await.unwrap();
        let r22_start = {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert!(run.settlement_recheck.is_none() && !run.may_add_risk());
            run.r22_wait.as_ref().unwrap().proof.sampled
        };
        let data = r16_fixture::data(&trader);
        let mut last_poll = Instant::now() - crate::bot::runtime::SETTLE_CHECK_EVERY;
        clock.advance(1_000);
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        let r3_start = {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert!(!run.must_end() && run.account_read.is_none());
            assert_eq!(run.r22_wait.as_ref().unwrap().proof.sampled, r22_start);
            let r3 = run.settlement_recheck.unwrap();
            assert_eq!((r3.market_id, r3.started), (2, clock.now().unwrap()));
            r3.started
        };
        clock.advance(60_000);
        if published {
            facts.round(resolved(second, 0.0));
            facts.pay(177);
        }
        if fresh {
            *facts.redemption.lock().unwrap() = stage(RedemptionStage::After);
            facts.stale_positions.lock().unwrap().remove(&77);
        }
        facts.rest_calls.lock().unwrap().clear();
        facts.rpc_calls.lock().unwrap().clear();
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        assert!(facts.rest_calls.lock().unwrap().is_empty() && facts.rpc_calls.lock().unwrap().is_empty());
        {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            let r3 = run.settlement_recheck.unwrap();
            assert_eq!(r3.started, r3_start); // The watchdog may advance `last`, never the retry origin.
            let wait = run.r22_wait.as_ref().unwrap();
            assert_eq!((wait.proof.sampled, wait.slots), (r22_start, 1));
            assert!(run.account_read.is_none() && !run.may_add_risk());
        }
        broker.settle_resolved(data.as_ref()).await.unwrap();
        let routes = facts.rest_calls.lock().unwrap();
        // The account adapter also reads each newly absent position's
        // authoritative result; that is not another scheduled poll.
        assert_eq!(routes.iter().filter(|path| path.as_str() == "/markets/1").count(),
            1 + usize::from(fresh), "{routes:?}");
        assert_eq!(routes.iter().filter(|path| path.as_str() == "/markets/2").count(),
            1 + usize::from(published), "{routes:?}");
        assert_eq!(routes.iter().filter(|path| path.starts_with("/positions?")).count(), usize::from(published));
        assert_eq!((transport.posts.load(Ordering::SeqCst), transport.signs.load(Ordering::SeqCst)), (2, 2));
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        if fresh {
            assert!(run.r22_wait.is_none() && run.settlement_recheck.is_none() && run.may_add_risk());
            assert!(run.account_read.is_none(), "the combined closure is not a pre-send read");
            assert_eq!(run.cash_anchor.as_ref().unwrap().1, clock.now().unwrap());
            assert_eq!(state.live.claims[&1], (quantity, quantity));
        } else {
            assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DATA_UNAVAILABLE");
            assert_eq!(run.settlement_recheck.unwrap().started, r3_start);
            assert_eq!(run.r22_wait.as_ref().unwrap().proof.sampled, r22_start);
            assert!(run.must_end() && !run.may_add_risk() && run.account_read.is_none());
            assert!(!state.live.claims.contains_key(&1));
        }
    }
}

struct PollClockEdge<'a> {
    data: &'a dyn crate::service::venue::MarketData,
    clock: &'a crate::service::observation::FixtureClock,
    advance_ms: u64,
    in_flight: Option<&'a tokio::sync::Notify>,
    cancelled: Option<&'a std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::service::venue::MarketData for PollClockEdge<'_> {
    async fn markets(&self, _: &crate::service::venue::MarketQuery)
        -> anyhow::Result<Vec<crate::service::venue::TradableMarket>> { panic!("unexpected scan") }
    async fn market(&self, _: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
        panic!("expected the authoritative consumer")
    }
    async fn settlement_market(&self, id: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
        let result = self.data.settlement_market(id).await;
        if id == 1 {
            if let Some(started) = self.in_flight {
                struct Cancellation<'a>(Option<&'a std::sync::atomic::AtomicUsize>);
                impl Drop for Cancellation<'_> {
                    fn drop(&mut self) {
                        if let Some(count) = self.0 { count.fetch_add(1, Ordering::SeqCst); }
                    }
                }
                let _cancelled = Cancellation(self.cancelled);
                started.notify_one();
                std::future::pending::<()>().await;
            }
            self.clock.advance(self.advance_ms);
        }
        result
    }
    async fn with_schedule(&self, _: &crate::service::venue::TradableMarket)
        -> anyhow::Result<crate::service::venue::TradableMarket> { panic!("unexpected schedule") }
    async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> {
        panic!("unexpected books")
    }
}

#[tokio::test]
async fn r22_coalesced_original_poll_rechecks_cutoff_between_legs() {
    r22_coalesced_original_poll(PollScenario::Deadline).await;
}

#[tokio::test]
async fn r22_coalesced_original_poll_reads_one_batch_and_never_a_third_slot() {
    r22_coalesced_original_poll(PollScenario::Healthy).await;
}

#[tokio::test]
async fn r22_original_poll_typed_429_preserves_failure_origin_and_earlier_deadline() {
    r22_coalesced_original_poll(PollScenario::RateLimited).await;
}

#[tokio::test]
async fn r22_coalesced_poll_cancels_lane_wait_and_in_flight_at_earlier_deadline() {
    for scenario in [PollScenario::Queued, PollScenario::InFlight] {
        r22_coalesced_original_poll(scenario).await;
    }
}

#[derive(Clone, Copy)]
enum PollScenario {
    Deadline, Healthy, RateLimited, Queued, InFlight, BatchBudget(bool), Targeted429,
    ProofRead(ProofFault),
    RawProof(ProofFault, &'static str, RawOutcome),
}

#[derive(Clone, Copy, Debug)]
enum RawOutcome { Read, Conflict, Missing, Admission }

#[derive(Clone, Copy, Debug)]
enum ProofFault { Official, Census, Mixed { reverse: bool, conflict: bool } }

#[tokio::test]
async fn r22_active_proof_read_failure_preserves_original_streak() {
    use futures_util::FutureExt;
    let mut failed = Vec::new();
    for fault in [ProofFault::Official, ProofFault::Census,
        ProofFault::Mixed { reverse: false, conflict: false },
        ProofFault::Mixed { reverse: true, conflict: false },
        ProofFault::Mixed { reverse: false, conflict: true },
        ProofFault::Mixed { reverse: true, conflict: true },
    ] {
        if std::panic::AssertUnwindSafe(r22_coalesced_original_poll(PollScenario::ProofRead(fault)))
            .catch_unwind().await.is_err() {
            failed.push(fault);
        }
    }
    assert!(failed.is_empty(), "failed proof scenarios: {failed:?}");
}

const RPC_LIMIT_REPLY: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"Limit exceeded"}}"#;

#[tokio::test]
async fn r22_raw_rpc_proof_read_preserves_original_streak() {
    use futures_util::FutureExt;
    use RawOutcome::*;
    let mut cases = vec![(ProofFault::Census, RPC_LIMIT_REPLY, Read)];
    for reverse in [false, true] {
        for conflict in [false, true] {
            cases.push((ProofFault::Mixed { reverse, conflict }, RPC_LIMIT_REPLY,
                if conflict { Conflict } else { Read }));
        }
    }
    for body in [
        r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32005,"message":"Limit exceeded"}}"#,
        r#"{"jsonrpc":"1.0","id":1,"error":{"code":-32005,"message":"Limit exceeded"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":"-32005","message":"Limit exceeded"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"Limit exceeded"},"result":null}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"Limit exceeded"}}"#,
    ] { cases.push((ProofFault::Census, body, Conflict)); }
    for body in [r#"{"jsonrpc":"2.0","id":1}"#, r#"{"jsonrpc":"2.0","id":1,"result":null}"#] {
        cases.push((ProofFault::Census, body, Missing));
    }
    cases.push((ProofFault::Census, RPC_LIMIT_REPLY, Admission));
    let mut failed = Vec::new();
    for (fault, body, expected) in cases {
        if std::panic::AssertUnwindSafe(r22_coalesced_original_poll(PollScenario::RawProof(fault, body, expected)))
            .catch_unwind().await.is_err() {
            failed.push((fault, body, expected));
        }
    }
    assert!(failed.is_empty(), "raw RPC proof cases failed: {failed:?}");
}

#[tokio::test]
async fn r22_targeted_positions_429_preserves_read_streak_and_cutoff() {
    r22_coalesced_original_poll(PollScenario::Targeted429).await;
}

#[tokio::test]
async fn r22_second_batch_budget_includes_lane_wait() {
    for released_in_time in [false, true] {
        r22_coalesced_original_poll(PollScenario::BatchBudget(released_in_time)).await;
    }
}

#[tokio::test]
async fn r22_a0_advances_only_after_audited_runtime_commit() {
    use super::super::r16_cash_drive::adapter_fixture;
    use crate::service::predict_fun::controlled::r16_fixture::{RedemptionFixture, RedemptionStage::*};
    use std::sync::atomic::Ordering;
    for fault in [None, Some("ledger"), Some("output")] {
        let (broker, clock, _, transport, facts) = adapter_fixture().await;
        let lane = broker.state.lock().live.controlled.as_ref().unwrap().operation_lane.clone();
        let _lane = lane.lock().await;
        *facts.redemption.lock().unwrap() = Some(RedemptionFixture::new(Before));
        assert!(broker.state.lock().live.controlled.as_ref().unwrap().cash_anchor.is_none());
        live::check_runtime_account(&broker, false).await.unwrap();
        let (before, checked, seq) = {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            let (anchor, checked) = run.cash_anchor.clone().unwrap();
            assert_eq!(anchor.number, 0x124);
            assert_eq!(anchor.identity, run.identity);
            assert_eq!(run.account_read.as_ref().unwrap().snapshot.collateral_atoms, Some(anchor.collateral));
            assert!(run.emitter.last_seq() > 0);
            (anchor, checked, run.emitter.last_seq())
        };
        clock.advance(3_000);
        *facts.redemption.lock().unwrap() = Some(RedemptionFixture::new(Complete));
        match fault {
            Some("ledger") => *facts.cash_offset.lock().unwrap() = 1,
            Some("output") => broker.state.lock().live.controlled.as_ref().unwrap()
                .output_failed.store(true, Ordering::SeqCst),
            _ => {},
        }
        let result = live::check_runtime_account(&broker, false).await;
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let (anchor, at) = run.cash_anchor.as_ref().unwrap();
        if let Some(fault) = fault {
            let error = result.unwrap_err();
            assert_eq!(anchor, &before, "{fault}");
            assert_eq!(at, &checked);
            assert!(run.account_read.is_none() && !run.may_add_risk(), "{error}");
            if fault == "output" { assert!(run.emitter.output_failed()); }
        } else {
            result.unwrap();
            assert_eq!(anchor.number, 0x125);
            assert_ne!(anchor.hash, before.hash);
            assert!(at.elapsed_ms > checked.elapsed_ms && at.utc > checked.utc);
            assert!(run.emitter.last_seq() > seq);
        }
        assert!(state.live.claims.is_empty());
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    }
}

async fn r22_coalesced_original_poll(scenario: PollScenario) {
    use super::super::r16_cash_drive::{adapter_fixture, resolved, send_round};
    use crate::service::predict_fun::controlled::r16_fixture::{self, RedemptionFixture, RedemptionStage};
    use std::time::Instant;
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let first = send_round(&broker, &clock, &facts, &mut core, 1).await;
    clock.advance(300_000);
    send_round(&broker, &clock, &facts, &mut core, 2).await;
    facts.round(resolved(first, 1.0));
    let quantity = broker.state.lock().live.net_inventory[&1];
    let script = |stage| RedemptionFixture::for_target(stage, 0xcc, quantity);
    *facts.redemption.lock().unwrap() = Some(script(RedemptionStage::Before));
    live::check_runtime_account(&broker, false).await.unwrap();
    *facts.redemption.lock().unwrap() = Some(script(RedemptionStage::Complete));
    if let PollScenario::RawProof(_, body, RawOutcome::Admission) = scenario {
        use crate::service::venue::{VenueError, VenueErrorCode};
        *facts.proof_rpc_body.lock().unwrap() = Some(body);
        *facts.fail_position_balance.lock().unwrap() = Some(177);
        let error = trader.read_runtime_account_observation(None).await.err().expect("ordinary read must fail");
        let error = error.downcast_ref::<VenueError>().expect("ordinary read must keep the original type");
        assert_eq!(error.code, VenueErrorCode::ReadUnavailable);
        assert_eq!(error.reason, "RPC_RESPONSE");
        assert_eq!(*facts.raw_rpc_reasons.lock().unwrap(), ["RPC_RESPONSE"]);
        *facts.fail_position_balance.lock().unwrap() = None;
        *facts.proof_rpc_failures.lock().unwrap() = 0;
        facts.raw_rpc_reasons.lock().unwrap().clear();
    }
    facts.stale_positions.lock().unwrap().insert(77);
    facts.pay(77);
    if let PollScenario::RawProof(_, _, RawOutcome::Admission) = scenario {
        *facts.fail_census_header.lock().unwrap() = true;
        let error = live::check_runtime_account(&broker, false).await.unwrap_err();
        assert_eq!(*facts.proof_rpc_failures.lock().unwrap(), 1);
        assert_eq!(*facts.raw_rpc_reasons.lock().unwrap(), ["RPC_RESPONSE"]);
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.r22_wait.is_none() && run.read_failures.is_none(), "{error}");
        assert!(run.account_read.is_none() && state.live.claims.is_empty() && !run.may_add_risk());
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "RECONCILIATION_REQUIRED");
        return;
    }
    if matches!(scenario, PollScenario::Targeted429 | PollScenario::ProofRead(_) | PollScenario::RawProof(..)) {
        // Failure since uses the original wall clock; place that origin before T0
        // so the original read-failure budget, not the R22 hard limit, wins.
        clock.set_utc(Utc::now() + chrono::Duration::seconds(30));
    }
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let mut state = broker.state.lock();
        let run = state.live.controlled.as_mut().unwrap();
        assert_eq!(run.r22_wait.as_ref().unwrap().slots, 0);
        assert!(!run.may_add_risk());
        if matches!(scenario, PollScenario::Deadline | PollScenario::Queued | PollScenario::InFlight) {
            run.run_deadline_elapsed_ms = clock.now().unwrap().elapsed_ms + 1;
        }
    }
    facts.rest_calls.lock().unwrap().clear();
    facts.rpc_calls.lock().unwrap().clear();
    let data = r16_fixture::data(&trader);
    let mut last_poll = Instant::now() - crate::bot::runtime::SETTLE_CHECK_EVERY;
    if matches!(scenario, PollScenario::RateLimited) {
        let t0 = clock.now().unwrap();
        broker.state.lock().live.controlled.as_mut().unwrap().run_deadline_elapsed_ms = t0.elapsed_ms + 90_000;
        *facts.market_fault.lock().unwrap() = Some("category_http429");
        let mut since = None;
        for slot in 1..=2 {
            live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
            {
                let state = broker.state.lock();
                let run = state.live.controlled.as_ref().unwrap();
                let failure = run.settlement_failures.unwrap();
                assert_eq!(failure.count, u32::from(slot));
                assert_eq!(*since.get_or_insert(failure.since), failure.since);
                assert!(run.read_failures.is_none() && !run.must_end() && !run.may_add_risk());
                let wait = run.r22_wait.as_ref().unwrap();
                assert_eq!((wait.proof.sampled, wait.slots), (t0, slot));
                assert!(run.account_read.is_none() && state.live.claims.is_empty());
            }
            let expected = ["/markets/1", "/categories/r16-1"].repeat(slot as usize);
            assert_eq!(*facts.rest_calls.lock().unwrap(), expected);
            assert!(facts.rpc_calls.lock().unwrap().is_empty());
            live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
            assert_eq!(*facts.rest_calls.lock().unwrap(), expected);
            if slot == 1 {
                clock.advance(60_000);
                last_poll -= crate::bot::runtime::SETTLE_CHECK_EVERY;
            }
        }
        clock.advance(30_000);
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        assert_eq!(facts.rest_calls.lock().unwrap().len(), 4);
        assert!(facts.rpc_calls.lock().unwrap().is_empty());
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DEADLINE");
        assert!(run.account_read.is_none() && state.live.claims.is_empty() && !run.may_add_risk());
        assert_eq!((transport.posts.load(Ordering::SeqCst), transport.signs.load(Ordering::SeqCst)), (2, 2));
        return;
    }
    if let PollScenario::ProofRead(fault) = scenario {
        r22_proof_fault(&broker, &clock, data.as_ref(), &transport, &facts, fault, None).await;
        return;
    }
    if let PollScenario::RawProof(fault, body, expected) = scenario {
        *facts.proof_rpc_body.lock().unwrap() = Some(body);
        r22_proof_fault(&broker, &clock, data.as_ref(), &transport, &facts, fault, Some(expected)).await;
        return;
    }
    if matches!(scenario, PollScenario::Targeted429) {
        last_poll = Instant::now();
        *facts.market_fault.lock().unwrap() = Some("positions_http429");
        let t0 = clock.now().unwrap();
        let mut since = None;
        let mut counts = (0, 0);
        for slot in 1..=2 {
            live::order_wait::r22_wakeup(&broker).await;
            live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
            {
                let state = broker.state.lock();
                let run = state.live.controlled.as_ref().unwrap();
                let failure = run.read_failures.unwrap();
                assert_eq!(failure.count, u32::from(slot));
                assert_eq!(*since.get_or_insert(failure.since), failure.since);
                assert!(run.settlement_failures.is_none() && !run.must_end());
                let wait = run.r22_wait.as_ref().unwrap();
                assert_eq!((wait.proof.sampled, wait.slots), (t0, slot));
                assert!(run.account_read.is_none() && state.live.claims.is_empty() && !run.may_add_risk());
            }
            {
                let routes = facts.rest_calls.lock().unwrap();
                assert_eq!(routes.iter().filter(|path| path.starts_with("/positions?")).count(), usize::from(slot));
                assert!(routes.iter().all(|path| !path.starts_with("/categories/")), "not an original official poll");
                counts = (routes.len(), facts.rpc_calls.lock().unwrap().len());
            }
            live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
            assert_eq!((facts.rest_calls.lock().unwrap().len(), facts.rpc_calls.lock().unwrap().len()), counts);
            if slot == 1 { clock.advance(30_000); }
        }
        let bound = since.unwrap() + chrono::Duration::from_std(READ_FAILURE_BUDGET).unwrap();
        assert!(bound < t0.utc + chrono::Duration::seconds(120));
        clock.set_utc(bound - chrono::Duration::milliseconds(1));
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        assert!(!broker.state.lock().live.controlled.as_ref().unwrap().must_end());
        clock.advance(1);
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        assert_eq!((facts.rest_calls.lock().unwrap().len(), facts.rpc_calls.lock().unwrap().len()), counts);
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "READ_UNAVAILABLE");
        assert!(run.account_read.is_none() && state.live.claims.is_empty() && !run.may_add_risk());
        assert_eq!((transport.posts.load(Ordering::SeqCst), transport.signs.load(Ordering::SeqCst)), (2, 2));
        return;
    }
    if matches!(scenario, PollScenario::Healthy) {
        live::order_wait::r22_wakeup(&broker).await;
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        {
            let routes = facts.rest_calls.lock().unwrap();
            for route in ["/markets/1", "/markets/2"] {
                assert_eq!(routes.iter().filter(|path| path.as_str() == route).count(), 1, "{routes:?}");
            }
            assert_eq!(routes.iter().filter(|path| path.starts_with("/positions?")).count(), 1, "{routes:?}");
        }
        let first = {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            let wait = run.r22_wait.as_ref().unwrap();
            assert_eq!(wait.slots, 1);
            assert!(!run.may_add_risk() && run.account_read.is_none());
            wait.proof.sampled
        };
        assert!(last_poll.elapsed() < crate::bot::runtime::SETTLE_CHECK_EVERY);
        clock.advance(30_000);
        live::order_wait::r22_wakeup(&broker).await;
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        let counts = || (facts.rest_calls.lock().unwrap().len(), facts.rpc_calls.lock().unwrap().len());
        let completed = counts();
        assert_eq!(facts.rest_calls.lock().unwrap().iter().filter(|path| path.starts_with("/positions?")).count(), 2);
        clock.advance(59_999);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(10),
            live::order_wait::r22_wakeup(&broker)).await.is_err());
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        assert_eq!(counts(), completed);
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.r22_wait.as_ref().unwrap();
        assert_eq!((wait.slots, wait.proof.sampled), (2, first));
        assert!(!run.must_end() && !run.may_add_risk() && run.account_read.is_none());
        assert!(run.attempt_outcomes.is_empty() && state.live.claims.is_empty());
        assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
        return;
    }
    if let PollScenario::BatchBudget(released_in_time) = scenario {
        use std::time::Duration;
        last_poll = Instant::now();
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        let t0 = {
            let state = broker.state.lock();
            let wait = state.live.controlled.as_ref().unwrap().r22_wait.as_ref().unwrap();
            assert_eq!(wait.slots, 1);
            wait.proof.sampled
        };
        facts.rest_calls.lock().unwrap().clear();
        facts.rpc_calls.lock().unwrap().clear();
        clock.advance(30_000);
        live::order_wait::r22_wakeup(&broker).await;
        last_poll -= crate::bot::runtime::SETTLE_CHECK_EVERY;
        let due_poll = last_poll;
        let pending = tokio::sync::Notify::new();
        let cancelled = std::sync::atomic::AtomicUsize::new(0);
        let edge = PollClockEdge { data: data.as_ref(), clock: &clock, advance_ms: 0,
            in_flight: released_in_time.then_some(&pending), cancelled: Some(&cancelled) };
        let lane = broker.live_operation_lane().unwrap();
        {
            let held = lane.lock().await;
            let observing = live::order_wait::observe_r22(&broker, &edge, &mut last_poll);
            tokio::pin!(observing);
            tokio::select! {
                biased;
                _ = &mut observing => panic!("slot 2 did not queue on the operation lane"),
                _ = tokio::task::yield_now() => (),
            }
            assert!(facts.rest_calls.lock().unwrap().is_empty());
            clock.advance(if released_in_time { 29_000 } else { 30_000 });
            if released_in_time {
                drop(held);
                tokio::select! {
                    biased;
                    _ = &mut observing => panic!("no read with one second left"),
                    result = tokio::time::timeout(Duration::from_secs(2), pending.notified()) => result.unwrap(),
                }
                clock.advance(999);
                tokio::select! {
                    biased;
                    _ = &mut observing => panic!("cancelled before equality"),
                    _ = tokio::task::yield_now() => (),
                }
                assert_eq!(cancelled.load(Ordering::SeqCst), 0);
                clock.advance(1);
                tokio::time::timeout(Duration::from_secs(2), &mut observing).await
                    .expect("in-flight read must not receive a fresh 30s after the lane wait");
            } else {
                tokio::time::timeout(Duration::from_secs(2), &mut observing).await
                    .expect("slot 2 must expire at equality while the lane is still held");
                drop(held);
            }
        }
        assert!(lane.try_lock().is_ok());
        assert_eq!(cancelled.load(Ordering::SeqCst), usize::from(released_in_time));
        if !released_in_time { assert_eq!(last_poll, due_poll, "no poll started"); }
        // A late release must not dispatch the abandoned batch or a third one.
        assert!(tokio::time::timeout(Duration::from_millis(10),
            live::order_wait::r22_wakeup(&broker)).await.is_err());
        live::order_wait::observe_r22(&broker, data.as_ref(), &mut last_poll).await;
        let routes = facts.rest_calls.lock().unwrap();
        let expected: &[&str] = if released_in_time { &["/markets/1", "/categories/r16-1"] } else { &[] };
        assert_eq!(routes.as_slice(), expected);
        assert!(facts.rpc_calls.lock().unwrap().is_empty());
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.r22_wait.as_ref().unwrap();
        assert_eq!((wait.proof.sampled, wait.slots), (t0, 2));
        assert!(run.control.lock().stop.is_none(), "a batch timeout is not a window hard stop");
        assert!(!run.may_add_risk() && run.account_read.is_none());
        assert!(run.attempt_outcomes.is_empty() && state.live.claims.is_empty());
        assert_eq!((transport.posts.load(Ordering::SeqCst), transport.signs.load(Ordering::SeqCst)), (2, 2));
        return;
    }
    let pending = tokio::sync::Notify::new();
    let in_flight = matches!(scenario, PollScenario::InFlight);
    let queued = matches!(scenario, PollScenario::Queued);
    let edge = PollClockEdge { data: data.as_ref(), clock: &clock, advance_ms: 1,
        in_flight: in_flight.then_some(&pending), cancelled: None };
    if queued || in_flight {
        use std::time::Duration;
        let lane = broker.live_operation_lane().unwrap();
        let held = if queued { Some(lane.lock().await) } else { None };
        let observing = live::order_wait::observe_r22(&broker, &edge, &mut last_poll);
        tokio::pin!(observing);
        let reached_edge = async {
            if queued { tokio::task::yield_now().await; } else { pending.notified().await; }
        };
        tokio::select! {
            biased;
            _ = &mut observing => panic!("original lane returned before the cutoff"),
            result = tokio::time::timeout(Duration::from_secs(2), reached_edge) => result.unwrap(),
        }
        clock.advance(1);
        // Keep the lane occupied until the queued future has actually returned.
        tokio::time::timeout(Duration::from_secs(2), &mut observing).await.unwrap();
        drop(held);
        assert!(lane.try_lock().is_ok(), "cancellation must release the original lane");
    } else {
        live::order_wait::observe_r22(&broker, &edge, &mut last_poll).await;
    }
    let routes = facts.rest_calls.lock().unwrap();
    assert_eq!(routes.iter().filter(|path| path.as_str() == "/markets/1").count(), usize::from(!queued));
    assert!(routes.iter().all(|path| path != "/markets/2" && !path.starts_with("/positions?")),
        "late reply must not start another official or account leg: {routes:?}");
    assert!(facts.rpc_calls.lock().unwrap().is_empty());
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DEADLINE");
    assert_eq!(run.r22_wait.as_ref().unwrap().slots, usize::from(!queued) as u8);
    assert!(!run.may_add_risk() && run.account_read.is_none());
    assert!(run.attempt_outcomes.is_empty() && state.live.claims.is_empty());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
}

async fn r22_proof_fault(
    broker: &Broker, clock: &crate::service::observation::FixtureClock,
    data: &dyn crate::service::venue::MarketData,
    transport: &crate::service::predict_fun::account::tests::SyntheticRpc,
    facts: &crate::service::predict_fun::controlled::r16_fixture::Facts,
    fault: ProofFault,
    expected: Option<RawOutcome>,
) {
    let conflict = matches!(expected, Some(RawOutcome::Conflict))
        || matches!(fault, ProofFault::Mixed { conflict: true, .. });
    let missing = matches!(expected, Some(RawOutcome::Missing));
    let mut last_poll = std::time::Instant::now();
    let (t0, anchor, cash_anchor, net_cash, net_inventory) = {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.r22_wait.as_ref().unwrap();
        (wait.proof.sampled, wait.a.clone(), run.cash_anchor.clone(),
            state.live.net_cash.clone(), state.live.net_inventory.clone())
    };
    match fault {
        ProofFault::Official => *facts.market_fault.lock().unwrap() = Some("category_http429"),
        ProofFault::Census => *facts.fail_census_header.lock().unwrap() = true,
        ProofFault::Mixed { reverse, conflict } => {
            *facts.position_order_reversed.lock().unwrap() = Some(reverse);
            *facts.fail_position_balance.lock().unwrap() = Some(177);
            if conflict {
                // Legal ERC-1155 response, but neither the REST amount nor zero.
                facts.position_overrides.lock().unwrap().insert(77, net_inventory[&1] + U256::from(1));
            }
        }
    }
    let counters = || (facts.rest_calls.lock().unwrap().len(), facts.rpc_calls.lock().unwrap().len());
    let assert_no_commit = |slots| {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let wait = run.r22_wait.as_ref().unwrap();
        assert_eq!((wait.proof.sampled, &wait.a, wait.slots), (t0, &anchor, slots));
        assert_eq!(run.cash_anchor, cash_anchor);
        assert_eq!((&state.live.net_cash, &state.live.net_inventory), (&net_cash, &net_inventory));
        assert!(run.account_read.is_none() && state.live.claims.is_empty() && !run.may_add_risk());
        assert!(run.settlement_recheck.is_none() && run.settlement_failures.is_none());
        assert_eq!((transport.posts.load(Ordering::SeqCst), transport.signs.load(Ordering::SeqCst)), (2, 2));
    };
    let mut since = None;
    for slot in 1..=2 {
        facts.position_reads.lock().unwrap().clear();
        live::order_wait::r22_wakeup(broker).await;
        live::order_wait::observe_r22(broker, data, &mut last_poll).await;
        let reads = counters();
        {
            let routes = facts.rest_calls.lock().unwrap();
            let count = |prefix: &str| routes.iter().filter(|path| path.starts_with(prefix)).count();
            assert_eq!(count("/positions?"), usize::from(slot));
            let proof_get = usize::from(slot) * usize::from(matches!(fault, ProofFault::Official | ProofFault::Census));
            assert_eq!(count("/categories/r16-1"), proof_get);
            assert_eq!(count("/categories/r16-2"), 0);
        }
        assert_eq!(*facts.proof_rpc_failures.lock().unwrap(),
            usize::from(slot) * usize::from(!matches!(fault, ProofFault::Official)));
        if let ProofFault::Mixed { reverse, .. } = fault {
            assert_eq!(*facts.position_reads.lock().unwrap(), if reverse { [177, 77] } else { [77, 177] });
        }
        assert_no_commit(slot);
        {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            let control = run.control.lock();
            if conflict {
                let stop = control.stop.as_ref().expect("observed conflict must outrank a read failure");
                assert_eq!(stop.reason, "RECONCILIATION_REQUIRED");
                assert!(run.read_failures.is_none());
            } else if missing {
                assert!(control.stop.is_none() && run.read_failures.is_none(), "missing proof is not a read failure or conflict");
            } else {
                let failure = run.read_failures.unwrap_or_else(|| panic!("{fault:?}: no original read streak; stop={:?}",
                    control.stop.as_ref().map(|stop| &stop.reason)));
                assert_eq!(failure.count, u32::from(slot));
                assert_eq!(*since.get_or_insert(failure.since), failure.since);
                assert!(control.stop.is_none());
            }
        }
        if let Some(expected) = expected {
            let reason = match (fault, expected) {
                (ProofFault::Mixed { .. }, _) | (_, RawOutcome::Read) => "RPC_LIMIT_EXCEEDED",
                (_, RawOutcome::Missing) => "RPC_RESULT_MISSING",
                _ => "RPC_RESPONSE",
            };
            assert_eq!(*facts.raw_rpc_reasons.lock().unwrap(), vec![reason; usize::from(slot)]);
            eprintln!("D22-05 {fault:?} {expected:?} slot={slot} cumulative REST/RPC={reads:?} decoder={reason}");
        }
        live::order_wait::observe_r22(broker, data, &mut last_poll).await;
        assert_eq!(counters(), reads, "{fault:?}: no immediate retry");
        if conflict { return; }
        if slot == 1 { clock.advance(30_000); }
    }
    if missing { return; }
    let reads = counters();
    let bound = since.unwrap() + chrono::Duration::from_std(READ_FAILURE_BUDGET).unwrap();
    assert!(bound < t0.utc + chrono::Duration::seconds(120));
    clock.set_utc(bound - chrono::Duration::milliseconds(1));
    live::order_wait::observe_r22(broker, data, &mut last_poll).await;
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().must_end());
    clock.advance(1);
    live::order_wait::observe_r22(broker, data, &mut last_poll).await;
    assert_eq!(counters(), reads, "no third batch or read at equality");
    assert_no_commit(2);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "READ_UNAVAILABLE");
}

async fn original_poll_real_adapter(stale_positive: bool) {
    use crate::service::predict_fun::controlled::r16_fixture;
    use crate::service::venue::OrderVenue;
    for payout in if stale_positive { &[1.0][..] } else { &[0.0, 1.0][..] }.iter().copied() {
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
        if stale_positive {
            facts.stale_positions.lock().unwrap().insert(77);
            facts.pay(77);
            let mut last_poll = std::time::Instant::now();
            live::order_wait::observe_r22(&broker, &data, &mut last_poll).await;
            let state = broker.state.lock();
            assert!(state.live.controlled.as_ref().unwrap().settlement_recheck.is_some());
            assert!(state.live.claims.is_empty());
            assert_eq!(data.reads.load(Ordering::SeqCst), 1);
        }
        *data.result.lock() = result;
        broker.settle_resolved(&data).await.unwrap();
        let mut state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        if stale_positive {
            assert_eq!(data.reads.load(Ordering::SeqCst), 2);
            assert!(run.settlement_recheck.is_some() && run.r22_wait.is_none());
            assert!(run.account_read.is_none() && run.must_end());
            assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DATA_UNAVAILABLE");
            assert!(state.live.claims.is_empty());
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            continue;
        }
        assert!(run.settlement_recheck.is_none());
        assert_eq!(run.account_read.as_ref().unwrap().checked, clock.now().unwrap());
        assert_eq!(data.reads.load(Ordering::SeqCst), 2);
        assert!(state.live.claims.is_empty(), "a result alone never credits cash");
        assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)),
            if payout == 0.0 { None } else { Some(Blocked::Halted) });
    }
}
