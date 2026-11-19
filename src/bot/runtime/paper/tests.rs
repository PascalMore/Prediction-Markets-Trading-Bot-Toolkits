use super::*;
use crate::bot::runtime::test_support::book;
use crate::bot::runtime::tests::{limits, FakeVenue};
use crate::config::{ControlledMode, StrategyLimits};
use crate::service::venue::{LimitVenue, OrderVenue};

/// Output-only capacity fixture: a seeded terminal inventory, not a backtest.
#[test]
fn paper72_terminal_sender_fixture() {
    let Ok(start) = std::env::var("BTC5M_TEST_STARTED_AT") else {
        return;
    };
    println!();
    let end = std::env::var("BTC5M_TEST_DEADLINE_AT").unwrap();
    let run = ControlledRun::from_arguments(
        Some("unsigned-paper"),
        Some("run-1"),
        Some(&start),
        Some(&end),
    )
    .unwrap()
    .unwrap();
    assert!(run.is_paper72());
    let broker = Broker::new(controlled_limits(), false, None, None)
        .unwrap()
        .with_controlled_paper(run, 100.0)
        .unwrap()
        .with_phase1(
            Arc::new(crate::service::observation::SystemClock::new().unwrap()),
            phase1::StrategyContract::Phase1NetShares,
            0.03,
            30.0,
        )
        .unwrap();
    {
        let mut state = broker.state.lock();
        let paper = state.paper.as_mut().unwrap();
        for index in 1..=865 {
            let mut original = market(index);
            original.yes_token = format!("{}{:04}", "1".repeat(74), 2 * index);
            original.no_token = format!("{}0{:04}", "1".repeat(73), 2 * index + 1);
            paper.traded.insert(index, original);
            paper.settlements.insert(index, Settlement::Confirmed);
            paper.applied_results.insert(index, (100.0, 101.0, 1.0));
        }
        // Keep one genuine terminal holding shape with unknown mark; do not drop it.
        let token = paper.traded[&865].yes_token.clone();
        paper.settlements.insert(865, Settlement::Pending);
        paper.applied_results.remove(&865);
        paper.cash = 98.0;
        state.holdings.insert(
            token.clone(),
            Holding {
                market_id: 865,
                token,
                shares: 3.0,
                cost: 2.0,
                opened_at: Utc::now(),
            },
        );
    }
    let before = broker.paper_snapshot().unwrap();
    emit_final(&broker, false);
    assert_eq!(before, broker.paper_snapshot().unwrap());
    let event = recorded_events().last().unwrap().clone();
    assert_eq!(event["source"]["markets"].as_array().unwrap().len(), 865);
    assert!(serde_json::to_vec(&event).unwrap().len() > 64 * 1024);
}

#[test]
fn paper72_terminal_capacity_is_bounded_and_kind_scoped() {
    let start = Utc::now();
    super::super::observer::freeze_paper_now(Some(start));
    for seconds in [21_600, 259_200] {
        let end = start + chrono::Duration::seconds(seconds);
        let run = ControlledRun::from_arguments(
            Some("unsigned-paper"),
            Some("run-1"),
            Some(&start.to_rfc3339()),
            Some(&end.to_rfc3339()),
        )
        .unwrap()
        .unwrap();
        let limit = if seconds == 259_200 {
            1024 * 1024
        } else {
            64 * 1024
        };
        let empty = prepare_event(
            Some(&run),
            "final",
            None,
            None,
            None,
            Some(serde_json::json!({"padding": ""})),
            None,
            None,
            None,
        )
        .unwrap();
        let width = EVENT_PREFIX.len() + empty.line.len() + 1;
        let source = serde_json::json!({"padding": "x".repeat(limit - width)});
        let before = run.elapsed();
        assert!(prepare_event(
            Some(&run),
            "final",
            None,
            None,
            None,
            Some(source.clone()),
            None,
            None,
            None
        )
        .is_ok());
        assert!(prepare_event(
            Some(&run),
            "final",
            None,
            None,
            None,
            Some(serde_json::json!({"padding": "x".repeat(limit - width + 1)})),
            None,
            None,
            None
        )
        .is_err());
        if seconds == 259_200 {
            assert!(prepare_event(
                Some(&run),
                "summary",
                None,
                None,
                None,
                Some(source),
                None,
                None,
                None
            )
            .is_err());
        }
        assert!(run.elapsed() >= before && !run.stopped());
    }
    super::super::observer::freeze_paper_now(None);
}

// Each async test uses Tokio's current-thread runtime. Reset even after panic,
// since the test clock and recorder are deliberately thread-local, not global.
struct EventScope;
impl EventScope {
    fn new() -> Self {
        clear_recorded_events();
        Self
    }
}
impl Drop for EventScope {
    fn drop(&mut self) {
        set_controlled_clock_expired(false);
        CLOCK_EXPIRE_AFTER.with(|clock| clock.set(None));
        CLOCK_CHECKS.with(|checks| checks.set(0));
        ENCODING_LIMIT.with(|fault| fault.set(false));
        OUTPUT_FAILURE.with(|fault| fault.set(false));
        clear_recorded_events();
    }
}

#[tokio::test]
async fn deadline_after_reservation_releases_cash_before_fill() {
    let _events = EventScope::new();
    let broker = controlled(100.0);
    let market = market(1);
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    set_controlled_clock_expire_after(2);
    assert_eq!(
        broker
            .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
        TakeResult::Blocked(Blocked::Deadline)
    );
    let events = recorded_events();
    assert_eq!(
        events
            .iter()
            .map(|event| event["event_kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["reservation", "release", "blocked"]
    );
    assert_eq!(events[0]["accounting"]["reserved"], 2.0);
    for event in &events[1..] {
        assert_eq!(event["accounting"]["cash"], 100.0);
        assert_eq!(event["accounting"]["reserved"], 0.0);
        assert_eq!(event["accounting"]["available"], 100.0);
        assert_eq!(event["accounting"]["cost"], 0.0);
    }
    assert_eq!(events[2]["source"]["reason"], "Deadline");
    assert!(broker.holdings().is_empty());
    assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
}

/// A failure at the final application sample must release, never fill.
#[test]
fn phase1_final_clock_failure_releases_once() {
    use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
    use crate::service::observation::{FreshnessBasis, ObservationClock, SourceTimeStatus};
    use crate::service::spot::SpotSource;
    struct FinalClock {
        now: CheckTime,
        calls: std::sync::atomic::AtomicUsize,
        final_delay_ms: Option<i64>,
        encoding_limit: bool,
        output_failure: bool,
    }
    impl Clock for FinalClock {
        fn now(&self) -> Result<CheckTime> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call >= 2 {
                if call == 2 && self.encoding_limit {
                    ENCODING_LIMIT.with(|fault| fault.set(true));
                }
                if call == 2 && self.output_failure {
                    OUTPUT_FAILURE.with(|fault| fault.set(true));
                }
                let Some(delay) = self.final_delay_ms else {
                    bail!("CLOCK_UNAVAILABLE");
                };
                return Ok(CheckTime {
                    utc: self.now.utc + chrono::Duration::milliseconds(delay),
                    elapsed_ms: self.now.elapsed_ms + delay as u64,
                });
            }
            Ok(self.now)
        }
    }
    let _events = EventScope::new();
    for (final_delay_ms, encoding_limit, output_failure, sequence_exhausted) in [
        (None, false, false, false),
        (Some(1), false, false, false),
        (Some(0), true, false, false),
        (Some(0), false, true, false),
        (Some(0), false, false, true),
        (Some(0), false, false, false),
    ] {
        clear_recorded_events();
        let now = Utc::now();
        let mut market = market(1);
        market.start_price = Some(100.0);
        market.starts_at = Some(now - chrono::Duration::seconds(270));
        market.ends_at = Some(now + chrono::Duration::seconds(30));
        let book = book(&[(0.39, 100.0)], &[(0.4, 100.0)]);
        let observation = ObservationClock {
            source_at: None,
            source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: now,
            received_at: now,
            request_elapsed_ms: 100_000,
            received_elapsed_ms: 100_000,
        };
        let checked = CheckTime {
            utc: now,
            elapsed_ms: 100_000,
        };
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(Default::default());
        for index in 0..=20 {
            let mut sample = observation.clone();
            sample.request_at -= chrono::Duration::seconds(100 - index * 5);
            sample.received_at = sample.request_at;
            sample.request_elapsed_ms = index as u64 * 5_000;
            sample.received_elapsed_ms = sample.request_elapsed_ms;
            core.observe_phase1(
                "BTCUSDT",
                101.0,
                SpotSource::Binance,
                "BTCUSDT",
                &sample,
                CheckTime {
                    utc: sample.received_at,
                    elapsed_ms: sample.received_elapsed_ms,
                },
            )
            .unwrap();
        }
        let evaluation = core.evaluate_phase1(&EvaluationInputs {
            market: &market,
            book: &book,
            spot_price: 101.0,
            spot_source: Some(SpotSource::Binance),
            actual_pair: Some("BTCUSDT".into()),
            spot_clock: Some(observation.clone()),
            book_clock: Some(observation),
            now: checked,
            order_usd: 2.0,
            min_edge: 0.03,
            min_secs_left: 30.0,
            entered: false,
            identity_available_at: Some(now),
            strike_available_at: Some(now),
            settlement_provider: SettlementProvider::Unknown,
        });
        assert_eq!(evaluation.diagnostic.reason, Reason::Eligible);
        let candidate = evaluation.candidate.unwrap();
        let broker = controlled(100.0)
            .with_phase1(
                Arc::new(FinalClock {
                    now: checked,
                    calls: std::sync::atomic::AtomicUsize::new(0),
                    final_delay_ms,
                    encoding_limit,
                    output_failure,
                }),
                StrategyContract::Phase1NetShares,
                0.03,
                30.0,
            )
            .unwrap();
        broker.observe(&market, &book);
        broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate));
        if sequence_exhausted {
            broker
                .state
                .lock()
                .paper
                .as_ref()
                .unwrap()
                .phase1
                .as_ref()
                .unwrap()
                .event_seq
                .store(u64::MAX - 3, Ordering::SeqCst);
        }
        let result = broker.take_candidate(&market, &book, &candidate);
        if sequence_exhausted {
            assert!(
                matches!(result.unwrap(), TakeResult::Blocked(_)),
                "exhausted output sequence must refuse before applying cash"
            );
            assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
            assert_eq!(broker.paper_snapshot().unwrap().reserved, 0.0);
            let events = recorded_events();
            assert!(
                !events
                    .iter()
                    .any(|event| event["event_kind"] == "reservation"
                        || event["event_kind"] == "fill")
            );
            assert_eq!(
                events.last().unwrap()["source"]["diagnostic_reason"],
                "DIAGNOSTIC_LIMIT"
            );
            continue;
        }
        if output_failure {
            assert!(result.is_err());
            let snapshot = broker.paper_snapshot().unwrap();
            assert_eq!(
                snapshot.cash, 98.0,
                "output failure must not undo a real fill"
            );
            assert_eq!(snapshot.reserved, 0.0);
            assert!(broker.phase1_entries_stopped());
            let state = broker.state.lock();
            assert!(
                state
                    .paper
                    .as_ref()
                    .unwrap()
                    .phase1
                    .as_ref()
                    .unwrap()
                    .output_failed
                    .load(Ordering::SeqCst),
                "a failed direct PRE_APPLY flush must mark run evidence incomplete"
            );
            assert!(!recorded_events()
                .iter()
                .any(|event| event["event_kind"] == "fill"));
            continue;
        }
        let result = result.unwrap();
        if final_delay_ms == Some(0) && !encoding_limit {
            assert!(matches!(result, TakeResult::Filled { shares, .. } if shares > 0.0));
            assert_eq!(broker.paper_snapshot().unwrap().cash, 98.0);
            let events = recorded_events();
            let apply = events
                .iter()
                .find(|event| event["source"]["decision"]["phase"] == "PRE_APPLY")
                .unwrap();
            let fill = events
                .iter()
                .find(|event| event["event_kind"] == "fill")
                .unwrap();
            assert_eq!(apply["accounting"]["cash"], 100.0);
            assert_eq!(apply["accounting"]["reserved"], 2.0);
            assert_eq!(fill["accounting"]["cash"], 98.0);
            assert_eq!(fill["accounting"]["reserved"], 0.0);
            assert_eq!(apply["event_seq"].as_u64().unwrap() + 1, fill["event_seq"]);
            for (index, event) in events.iter().enumerate() {
                assert_eq!(event["event_seq"].as_u64().unwrap(), index as u64 + 1);
            }
            continue;
        }
        assert_eq!(result, TakeResult::Blocked(Blocked::DataIncomplete));
        assert!(broker.holdings().is_empty());
        assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
        assert_eq!(broker.paper_snapshot().unwrap().reserved, 0.0);
        let events = recorded_events();
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_kind"] == "release")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_kind"] == "fill")
                .count(),
            0
        );
        let expected = if encoding_limit {
            "DIAGNOSTIC_LIMIT"
        } else if final_delay_ms.is_none() {
            "CLOCK_INVALID"
        } else {
            "ROUND_TOO_LATE"
        };
        assert_eq!(
            events.last().unwrap()["source"]["diagnostic_reason"],
            expected
        );
        if encoding_limit {
            let apply = events
                .iter()
                .find(|event| event["source"]["decision"]["phase"] == "PRE_APPLY")
                .expect("encoding refusal must keep the final recheck snapshot");
            let decision = &apply["source"]["decision"];
            assert_eq!(decision["reason"], "DIAGNOSTIC_LIMIT");
            assert_eq!(decision["candidate_id"], candidate.candidate_id());
            assert_eq!(decision["decision_id"], candidate.decision_id());
            assert_eq!(decision["selected_token"], candidate.token());
            assert!(decision["up"]["depth_levels"].is_null());
            assert!(decision["up"]["depth_complete"].is_null());
            assert_eq!(apply["accounting"]["reserved"], 2.0);
        }
    }
}

/// A deadline that arrives after the reservation releases the cash once, and
/// both the release and the terminal refusal carry RUN_STOPPED.
///
/// The money is released either way; what the diagnostic says is the point.
#[test]
fn a_deadline_after_the_reservation_is_recorded_as_run_stopped() {
    use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
    use crate::service::observation::{FreshnessBasis, ObservationClock, SourceTimeStatus};
    use crate::service::spot::SpotSource;
    /// A clock that always answers with the same reading.
    struct FixedClock {
        now: CheckTime,
    }
    impl Clock for FixedClock {
        fn now(&self) -> Result<CheckTime> {
            Ok(self.now)
        }
    }
    let _events = EventScope::new();
    clear_recorded_events();
    let now = Utc::now();
    let mut market = market(1);
    market.start_price = Some(100.0);
    market.starts_at = Some(now - chrono::Duration::seconds(270));
    market.ends_at = Some(now + chrono::Duration::seconds(30));
    let book = book(&[(0.39, 100.0)], &[(0.4, 100.0)]);
    let observation = ObservationClock {
        source_at: None,
        source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound,
        request_at: now,
        received_at: now,
        request_elapsed_ms: 100_000,
        received_elapsed_ms: 100_000,
    };
    let checked = CheckTime {
        utc: now,
        elapsed_ms: 100_000,
    };
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(Default::default());
    for index in 0..=20 {
        let mut sample = observation.clone();
        sample.request_at -= chrono::Duration::seconds(100 - index * 5);
        sample.received_at = sample.request_at;
        sample.request_elapsed_ms = index as u64 * 5_000;
        sample.received_elapsed_ms = sample.request_elapsed_ms;
        core.observe_phase1(
            "BTCUSDT",
            101.0,
            SpotSource::Binance,
            "BTCUSDT",
            &sample,
            CheckTime {
                utc: sample.received_at,
                elapsed_ms: sample.received_elapsed_ms,
            },
        )
        .unwrap();
    }
    let evaluation = core.evaluate_phase1(&EvaluationInputs {
        market: &market,
        book: &book,
        spot_price: 101.0,
        spot_source: Some(SpotSource::Binance),
        actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(observation.clone()),
        book_clock: Some(observation),
        now: checked,
        order_usd: 2.0,
        min_edge: 0.03,
        min_secs_left: 30.0,
        entered: false,
        identity_available_at: Some(now),
        strike_available_at: Some(now),
        settlement_provider: SettlementProvider::Unknown,
    });
    assert_eq!(evaluation.diagnostic.reason, Reason::Eligible);
    let candidate = evaluation.candidate.unwrap();
    let broker = controlled(100.0)
        .with_phase1(
            Arc::new(FixedClock { now: checked }),
            StrategyContract::Phase1NetShares,
            0.03,
            30.0,
        )
        .unwrap();
    broker.observe(&market, &book);
    broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate));
    // The deadline arrives after the run passed its own pre-submit check and
    // reserved the worst case, so the reservation has to be released.
    set_controlled_clock_expire_after(2);
    let result = broker.take_candidate(&market, &book, &candidate).unwrap();
    assert!(matches!(result, TakeResult::Blocked(_)));
    let events = recorded_events();
    let execution: Vec<_> = events
        .iter()
        .filter(|event| event["event_kind"] != "summary")
        .collect();
    let kinds: Vec<_> = execution
        .iter()
        .map(|event| event["event_kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["reservation", "release", "blocked"], "{kinds:?}");
    for event in &execution[1..] {
        assert_eq!(
            event["source"]["diagnostic_reason"], "RUN_STOPPED",
            "{event}"
        );
    }
    assert_eq!(execution[1]["accounting"]["reserved"], 0.0);
    assert_eq!(execution[1]["accounting"]["cash"], 100.0);
    assert_eq!(execution[2]["accounting"]["reserved"], 0.0);
    assert_eq!(execution[2]["accounting"]["cash"], 100.0);
    assert_eq!(
        execution
            .iter()
            .filter(|event| event["event_kind"] == "fill")
            .count(),
        0
    );
    let snapshot = broker.paper_snapshot().unwrap();
    assert_eq!(snapshot.cash, 100.0);
    assert_eq!(snapshot.reserved, 0.0);
    assert_eq!(snapshot.cost, 0.0);
    assert!(broker.holdings().is_empty());
}

struct BlockedMarketData(tokio::sync::Notify);
#[async_trait::async_trait]
impl crate::service::venue::MarketData for BlockedMarketData {
    async fn market(&self, _: u64) -> Result<TradableMarket> {
        if std::env::var_os("BTC5M_TEST_PIPE_MARKET").is_some() {
            use std::io::Write;
            println!("PIPE_READY");
            std::io::stdout().flush().unwrap();
        }
        self.0.notify_one();
        std::future::pending().await
    }
    async fn markets(&self, _: &crate::service::venue::MarketQuery) -> Result<Vec<TradableMarket>> {
        panic!("SCAN_FORBIDDEN")
    }
    async fn with_schedule(&self, _: &TradableMarket) -> Result<TradableMarket> {
        panic!("SCHEDULE_FORBIDDEN")
    }
    async fn books(&self, _: &[u64]) -> Result<HashMap<u64, BookSnapshot>> {
        panic!("BOOK_FORBIDDEN")
    }
}

/// Encoding-bound probe, not a market-data or account-state fixture.
#[test]
fn phase1_output_capacity_reserves_dynamic_scalar_widths() {
    use crate::service::observation::FixtureClock;
    let run = Phase1Run {
        clock: Arc::new(FixtureClock::new(Utc::now())),
        contract: StrategyContract::Phase1NetShares,
        min_edge: 0.03,
        min_secs_left: 30.0,
        event_seq: Arc::new(AtomicU64::new(9)),
        output_failed: Arc::new(AtomicBool::new(false)),
    };
    let mut link = CandidateLink {
        decision_id: "1".into(),
        candidate_id: "1".into(),
        diagnostic_reason: Reason::Eligible,
    };
    let before = serde_json::json!({"cash":9.0, "available":9.0, "cost":0.0, "reserved":0.0,
        "fees_in_cash":0.0, "loss_latched":false, "nav":null, "net_pnl":null, "realized":0.0, "unrealized":null});
    for kind in ["fill", "release"] {
        let encode = |padding: usize, accounting: &serde_json::Value, link: &CandidateLink| {
            prepare_event(
                None,
                kind,
                Some(1),
                Some("yes-1"),
                Some(accounting.clone()),
                Some(serde_json::json!({"padding": "x".repeat(padding)})),
                Some(&run),
                None,
                Some(link),
            )
        };
        let base = encode(0, &before, &link).unwrap().line.len() + EVENT_PREFIX.len() + 1;
        assert!(
            encode(64 * 1024 - base - 4, &before, &link).is_err(),
            "a nearly full pre-state record must not promise room for wider post-state numbers"
        );
        let padding = 64 * 1024 - base - 1024;
        assert!(encode(padding, &before, &link).is_ok());
        let mut after = before.clone();
        for value in after.as_object_mut().unwrap().values_mut() {
            *value = if value.is_boolean() {
                serde_json::json!(true)
            } else {
                serde_json::json!(-f64::MAX)
            };
        }
        run.event_seq.store(u64::MAX - 1, Ordering::SeqCst);
        link.diagnostic_reason = Reason::DiagnosticLimit;
        let final_record = encode(padding, &after, &link).unwrap();
        assert!(final_record.line.len() + EVENT_PREFIX.len() + 1 <= 64 * 1024);
        run.event_seq.store(9, Ordering::SeqCst);
        link.diagnostic_reason = Reason::Eligible;
    }
}

struct CleanupVenue {
    pending: bool,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl LimitVenue for CleanupVenue {
    async fn post_limit(
        &self,
        _: &crate::service::venue::LimitOrder,
        _: bool,
    ) -> Result<crate::service::venue::PostedOrder> {
        panic!("POST_FORBIDDEN")
    }
    async fn remove(&self, ids: &[String]) -> Result<crate::service::venue::RemoveResult> {
        assert_eq!(ids, ["fixture"]);
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.pending {
            std::future::pending::<()>().await;
        }
        Ok(crate::service::venue::RemoveResult {
            removed: ids.to_vec(),
            noop: vec![],
            rejected: vec![],
        })
    }
    async fn order_state(&self, _: &str) -> Result<crate::service::venue::RestingState> {
        panic!("STATE_FORBIDDEN")
    }
    async fn positions(&self) -> Result<Vec<crate::service::venue::HeldPosition>> {
        panic!("POSITIONS_FORBIDDEN")
    }
}

#[tokio::test]
async fn actual_cancel_all_timeout_is_reported_in_final() {
    let _events = EventScope::new();
    for pending in [false, true] {
        clear_recorded_events();
        let venue = Arc::new(CleanupVenue {
            pending,
            calls: Default::default(),
        });
        // A synthetic signed broker only; no signer, keyring or network is constructed.
        let mut broker = Broker::new(controlled_limits(), false, None, None).unwrap();
        broker.live = true;
        broker.limit = Some(venue.clone());
        broker.controlled = Some(
            ControlledRun::new(
                ControlledMode::SignedFutureLive,
                "cancel-test".into(),
                Utc::now(),
            )
            .unwrap(),
        );
        broker.state.lock().resting.insert(
            "fixture".into(),
            super::super::Resting {
                id: "fixture".into(),
                hash: "hash".into(),
                market_id: 1,
                token: "77".into(),
                side: Side::Buy,
                price: 0.4,
                shares: 5.0,
                filled: 0.0,
                expires_at: Utc::now(),
                removing: false,
            },
        );
        broker.stop_entries(ControlledStop::ParentLost);
        let data = BlockedMarketData(tokio::sync::Notify::new());
        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            broker.finish_controlled_with(
                &data,
                std::time::Duration::from_millis(5),
                broker.cancel_all(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(timed_out, pending);
        assert_eq!(venue.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(broker.state.lock().resting["fixture"].removing, !pending);
        let events = recorded_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_kind"], "final");
        assert_eq!(events[0]["source"]["cleanup_timed_out"], pending);
        assert_eq!(events[0]["source"]["stop_reason"], "PARENT_LOST");
    }
}

#[tokio::test]
async fn drive_interrupts_blocked_market_and_boundary_emergency() {
    struct MarketTick;
    #[async_trait::async_trait]
    impl super::super::Strategy for MarketTick {
        fn name(&self) -> &'static str {
            "blocked-market-test"
        }
        async fn tick(&mut self, ctx: &super::super::Ctx) -> Result<()> {
            ctx.data.market(1).await?;
            panic!("STOP_DID_NOT_CANCEL_MARKET")
        }
    }
    let _events = EventScope::new();
    for boundary in [false, true] {
        clear_recorded_events();
        let mut cfg = crate::config::AppConfig::load_controlled_unsigned_paper(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tools/predictfun/btc5m-controlled-paper.json"),
        )
        .unwrap();
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        cfg.controlled = Some(
            ControlledRun::new(
                ControlledMode::UnsignedPaper,
                "blocked-market".into(),
                Utc::now(),
            )
            .unwrap(),
        );
        let mut ctx =
            super::super::Ctx::connect_with_trader(cfg, "test", |_, _| panic!("NO_SIGNER"))
                .unwrap();
        // This in-process test owns its stop source; the separately tested real
        // stdin watcher must not stop it before MarketData has been polled.
        ctx.broker = Arc::new(controlled(100.0));
        let data = Arc::new(BlockedMarketData(tokio::sync::Notify::new()));
        ctx.data = data.clone();
        let stop = async {
            data.0.notified().await;
            if boundary {
                ctx.broker.stop_entries(ControlledStop::Deadline);
            }
            ctx.broker.stop_entries(ControlledStop::ParentLost);
        };
        tokio::time::timeout(std::time::Duration::from_millis(200), async {
            let mut strategy = MarketTick;
            tokio::join!(super::super::drive(&mut strategy, &ctx), stop)
                .0
                .unwrap();
        })
        .await
        .unwrap();
        assert_eq!(ctx.broker.entry_stop_reason(), Some("PARENT_LOST"));
        assert_eq!(ctx.broker.paper_snapshot().unwrap().cash, 100.0);
        assert_eq!(
            recorded_events().last().unwrap()["source"]["stop_reason"],
            "PARENT_LOST"
        );
    }
}

fn holdings(entries: &[(&str, f64, f64)]) -> HashMap<String, Holding> {
    entries
        .iter()
        .map(|(token, shares, cost)| {
            (
                (*token).to_string(),
                Holding {
                    market_id: 1,
                    token: (*token).to_string(),
                    shares: *shares,
                    cost: *cost,
                    opened_at: Utc::now(),
                },
            )
        })
        .collect()
}

fn market(id: u64) -> TradableMarket {
    let start = Utc::now();
    let mut market = crate::bot::runtime::test_support::market(id);
    market.starts_at = Some(start);
    market.ends_at = Some(start + chrono::Duration::seconds(300));
    market.start_price = Some(100.0);
    market.feed_symbol = Some("BTCUSDT".into());
    market.round_facts = Some(RoundFacts {
        decimal_precision: Some(2),
        ..Default::default()
    });
    market
}

#[test]
fn unsigned_ctx_bypasses_the_actual_trader_factory() {
    let mut cfg = crate::config::AppConfig::load_controlled_unsigned_paper(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tools/predictfun/btc5m-controlled-paper.json"),
    )
    .unwrap();
    cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
    cfg.controlled = Some(
        ControlledRun::new(ControlledMode::UnsignedPaper, "ctx-test".into(), Utc::now()).unwrap(),
    );
    let ctx = super::super::Ctx::connect_with_trader(cfg, "test", |_, _| {
        panic!("TRADER_CONSTRUCTION_FORBIDDEN")
    })
    .unwrap();
    assert!(!ctx.broker.is_live());
    assert!(ctx.broker.taker.is_none() && ctx.broker.limit.is_none() && ctx.broker.gate.is_none());
    assert_eq!(ctx.broker.paper_snapshot().unwrap().cash, 100.0);
    assert_eq!(
        ctx.cfg.credentials.predict_api_key.as_deref(),
        Some("synthetic-market-only")
    );
}

#[test]
fn signed_ctx_uses_original_factory_and_identity_bound_budget_gate() {
    use crate::service::predict_fun::PredictFunTrader;
    let base = crate::config::AppConfig::load_controlled_unsigned_paper(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tools/predictfun/btc5m-controlled-paper.json"),
    )
    .unwrap();
    for confirmed in [false, true] {
        let mut cfg = base.clone();
        cfg.bot.enable_trading = true;
        cfg.bot.mock_trading = false;
        cfg.controlled_paper = None;
        cfg.credentials.private_key = "11".repeat(32); // Public synthetic vector only.
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        cfg.credentials.predict_account = Some("0x1234567890123456789012345678901234567890".into());
        cfg.controlled = Some(
            ControlledRun::new(
                ControlledMode::SignedFutureLive,
                "signed-test".into(),
                Utc::now(),
            )
            .unwrap(),
        );
        let calls = std::cell::Cell::new(0);
        let ctx = super::super::Ctx::connect_with_trader(cfg.clone(), "test", |client, config| {
            calls.set(calls.get() + 1);
            let trader = PredictFunTrader::new(client, config)?;
            let identity = trader.identity();
            let mut wrong = identity.clone();
            wrong.trading_address = "0x0000000000000000000000000000000000000001".into();
            trader.gate.confirm_budget(&wrong);
            assert!(!trader.claim_run_confirmation());
            if confirmed {
                trader.gate.confirm_budget(&identity);
            }
            assert_eq!(trader.claim_run_confirmation(), confirmed);
            Ok(trader)
        })
        .unwrap();
        assert_eq!(calls.get(), 1);
        assert!(ctx.broker.is_live());
        assert!(ctx.broker.paper_snapshot().is_none());
        assert!(ctx.broker.gate.is_some());
        emit_final(&ctx.broker, false);
        cfg.credentials.private_key = "invalid-synthetic".into();
        assert!(
            super::super::Ctx::connect_with_trader(cfg, "test", PredictFunTrader::new).is_err()
        );
    }
}

/// Fixture of the same core/Broker context used by unsigned protocol consumers.
struct ProtocolHarness {
    broker: Broker,
    core: crate::bot::arbitrage::DecisionCore,
    clock: Arc<crate::service::observation::FixtureClock>,
    observation: crate::service::observation::ObservationClock,
}

impl ProtocolHarness {
    fn new() -> Self {
        use crate::service::observation::{
            FixtureClock, FreshnessBasis, ObservationClock, SourceTimeStatus,
        };
        let origin = Utc::now() - chrono::Duration::seconds(100);
        let clock = Arc::new(FixtureClock::new(origin));
        clock.advance(100_000);
        let broker = controlled(100.0)
            .with_phase1(clock.clone(), StrategyContract::Phase1NetShares, 0.03, 30.0)
            .unwrap();
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(Default::default());
        let mut observation = ObservationClock {
            source_at: None,
            source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: origin,
            received_at: origin,
            request_elapsed_ms: 0,
            received_elapsed_ms: 0,
        };
        for index in 0..=20 {
            observation.request_at = origin + chrono::Duration::seconds(index * 5);
            observation.received_at = observation.request_at;
            observation.request_elapsed_ms = index as u64 * 5000;
            observation.received_elapsed_ms = observation.request_elapsed_ms;
            core.observe_phase1(
                "BTCUSDT",
                101.0,
                crate::service::spot::SpotSource::Binance,
                "BTCUSDT",
                &observation,
                CheckTime {
                    utc: observation.received_at,
                    elapsed_ms: observation.received_elapsed_ms,
                },
            )
            .unwrap();
        }
        Self {
            broker,
            core,
            clock,
            observation,
        }
    }

    fn take(
        broker: &Broker,
        core: &mut crate::bot::arbitrage::DecisionCore,
        clock: &crate::service::observation::FixtureClock,
        observation: &crate::service::observation::ObservationClock,
        market: &TradableMarket,
        book: &BookSnapshot,
    ) -> TakeResult {
        use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
        clock.set_utc(Utc::now());
        let now = clock.now().unwrap();
        let evaluation = core.evaluate_phase1(&EvaluationInputs {
            market,
            book,
            spot_price: 101.0,
            spot_source: Some(crate::service::spot::SpotSource::Binance),
            actual_pair: Some("BTCUSDT".into()),
            spot_clock: Some(observation.clone()),
            book_clock: Some(observation.clone()),
            now,
            order_usd: 2.0,
            min_edge: 0.03,
            min_secs_left: 30.0,
            entered: false,
            identity_available_at: Some(now.utc),
            strike_available_at: Some(now.utc),
            settlement_provider: SettlementProvider::Unknown,
        });
        let candidate = evaluation.candidate.unwrap();
        assert!(broker.emit_phase1_evaluation(market, &evaluation.diagnostic, Some(&candidate)));
        broker.take_candidate(market, book, &candidate).unwrap()
    }
}

#[tokio::test]
async fn protocol_consumer_fixture() {
    let ProtocolHarness {
        broker,
        mut core,
        clock,
        observation,
    } = ProtocolHarness::new();
    let mut market = market(1);
    market.yes_token = "77".into();
    market.no_token = "88".into();
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    broker.record_source("77", "Binance", Utc::now());
    emit_summary(&broker);
    ProtocolHarness::take(
        &broker,
        &mut core,
        clock.as_ref(),
        &observation,
        &market,
        &book,
    );
    assert_eq!(
        ProtocolHarness::take(
            &broker,
            &mut core,
            clock.as_ref(),
            &observation,
            &market,
            &book
        ),
        TakeResult::Blocked(Blocked::MarketCap)
    );
    market.open = false;
    market.end_price = Some(101.0);
    market.payout_yes = Some(1.0);
    let facts = market.round_facts.as_mut().unwrap();
    facts.oracle_status = Some("RESOLVED".into());
    facts.trading_status = Some("CLOSED".into());
    facts.yes_status = Some("WON".into());
    facts.no_status = Some("LOST".into());
    broker.controlled_settle(&market);
    broker.controlled_settle(&market);
    assert!((broker.paper_snapshot().unwrap().cash - 102.9).abs() < 1e-9);
    broker.stop_entries(ControlledStop::Deadline);
    emit_final(&broker, false);
}

#[tokio::test]
async fn extended_protocol_consumer_fixture() {
    let _events = EventScope::new();
    for scenario in [
        "release",
        "sell",
        "loss",
        "pending",
        "conflict",
        "paid-conflict",
        "draw",
    ] {
        let ProtocolHarness {
            mut broker,
            mut core,
            clock,
            observation,
        } = ProtocolHarness::new();
        broker.controlled.as_mut().unwrap().run_id = format!("test-{scenario}");
        emit_summary(&broker);
        let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
        for id in 1..=if scenario == "loss" { 10 } else { 1 } {
            let mut market = market(id);
            market.yes_token = (id * 2).to_string();
            market.no_token = (id * 2 + 1).to_string();
            broker.observe(&market, &book);
            if scenario == "release" {
                set_controlled_clock_expire_after(2);
            }
            let result = ProtocolHarness::take(
                &broker,
                &mut core,
                clock.as_ref(),
                &observation,
                &market,
                &book,
            );
            if scenario == "release" {
                assert_eq!(result, TakeResult::Blocked(Blocked::Deadline));
                set_controlled_clock_expired(false);
                assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
                continue;
            }
            assert!(matches!(result, TakeResult::Filled { .. }));
            if scenario == "sell" {
                let exit =
                    crate::bot::runtime::test_support::book(&[(0.6, 100.0)], &[(0.6, 100.0)]);
                broker.observe(&market, &exit);
                assert!(matches!(
                    broker
                        .take(&market, &market.yes_token, Side::Sell, 5.0, 0.6, &exit)
                        .await
                        .unwrap(),
                    TakeResult::Filled { .. }
                ));
                assert!((broker.paper_snapshot().unwrap().cash - 100.9008).abs() < EPS);
            } else if scenario == "pending" {
                broker.controlled_settle(&market);
                assert_eq!(broker.paper_snapshot().unwrap().cash, 98.0);
            } else {
                market.open = false;
                market.end_price = Some(if scenario == "loss" { 99.0 } else { 101.0 });
                market.payout_yes = Some(if scenario == "loss" { 0.0 } else { 1.0 });
                if scenario == "draw" {
                    market.end_price = market.start_price;
                    market.payout_yes = Some(0.5);
                }
                market.round_facts = Some(if scenario == "loss" {
                    facts("LOST", "WON")
                } else {
                    facts("WON", "LOST")
                });
                if scenario == "conflict" {
                    market.no_token = "999".into();
                }
                broker.controlled_settle(&market);
                if scenario == "paid-conflict" {
                    let paid = broker.paper_snapshot().unwrap().cash;
                    market.end_price = Some(99.0);
                    market.payout_yes = Some(0.0);
                    market.round_facts = Some(facts("LOST", "WON"));
                    broker.controlled_settle(&market);
                    assert_eq!(broker.paper_snapshot().unwrap().cash, paid);
                }
            }
        }
        if scenario == "loss" {
            assert_eq!(broker.paper_snapshot().unwrap().cash, 80.0);
            assert!(broker.paper_snapshot().unwrap().loss_latched);
        }
        broker.stop_entries(ControlledStop::ParentLost);
        emit_final(&broker, false);
    }
}

#[tokio::test]
async fn concurrent_buys_share_one_cap_and_fee_threshold_matrix() {
    for bps in [0, 100, 200, 1000] {
        let broker = Arc::new(controlled(100.0));
        let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
        let mut tasks = Vec::new();
        for id in 1..=8 {
            let broker = broker.clone();
            let book = book.clone();
            tasks.push(tokio::spawn(async move {
                let mut market = market(id);
                market.fee_rate_bps = bps;
                broker.observe(&market, &book);
                broker
                    .take(&market, &market.yes_token, Side::Buy, 5.0, 0.4, &book)
                    .await
                    .unwrap()
            }));
        }
        let mut filled = 0;
        for task in tasks {
            if matches!(task.await.unwrap(), TakeResult::Filled { .. }) {
                filled += 1;
            }
        }
        assert_eq!(filled, 5);
        let snapshot = broker.paper_snapshot().unwrap();
        assert_eq!(snapshot.cost, 10.0);
        assert_eq!(snapshot.cash, 90.0);
        assert_eq!(snapshot.reserved, 0.0);
        assert!((snapshot.net_pnl.unwrap() + 10.0 * bps as f64 / 10000.0).abs() < 1e-9);
    }
    for (realized, expected) in [
        (-19.999, false),
        (-20.0, true),
        (-20.001, true),
        (-11.0, false),
    ] {
        let broker = controlled(100.0);
        {
            let mut state = broker.state.lock();
            state.realized = realized;
            state.paper.as_mut().unwrap().cash = 100.0 + realized;
            broker.check_controlled_loss_limit(&mut state);
        }
        assert_eq!(broker.paper_snapshot().unwrap().loss_latched, expected);
    }
}

#[tokio::test]
async fn control_pipe_drive_probe() {
    if std::env::var_os("BTC5M_TEST_PIPE").is_none() {
        return;
    }
    struct BlockedTick;
    #[async_trait::async_trait]
    impl super::super::Strategy for BlockedTick {
        fn name(&self) -> &'static str {
            "blocked-test"
        }
        async fn tick(&mut self, ctx: &super::super::Ctx) -> Result<()> {
            if std::env::var_os("BTC5M_TEST_PIPE_MARKET").is_some() {
                ctx.data.market(1).await?;
                panic!("MARKET_MUST_BE_CANCELLED")
            }
            use std::io::Write;
            println!("PIPE_READY");
            std::io::stdout().flush().unwrap();
            std::future::pending().await
        }
    }
    let mut cfg = crate::config::AppConfig::load_controlled_unsigned_paper(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tools/predictfun/btc5m-controlled-paper.json"),
    )
    .unwrap();
    cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
    cfg.controlled = Some(
        ControlledRun::new(
            ControlledMode::UnsignedPaper,
            "pipe-test".into(),
            Utc::now(),
        )
        .unwrap(),
    );
    let mut ctx =
        super::super::Ctx::connect_with_trader(cfg, "test", |_, _| panic!("NO_SIGNER")).unwrap();
    if std::env::var_os("BTC5M_TEST_PIPE_MARKET").is_some() {
        ctx.data = Arc::new(BlockedMarketData(tokio::sync::Notify::new()));
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        super::super::drive(&mut BlockedTick, &ctx),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ctx.broker.entry_stop_reason(), Some("PARENT_LOST"));
    assert!(!ctx.broker.paper_snapshot().unwrap().loss_latched);
}

#[tokio::test]
async fn preexisting_resting_and_unknown_live_risk_are_not_reused() {
    use crate::service::venue::{AttemptPhase, AttemptRecord, OrderIdentity, Submission};
    use alloy_primitives::U256;
    let broker = controlled(100.0);
    let market = market(1);
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    let (trader, _) = crate::service::predict_fun::account::tests::synthetic_trader("");
    let identity = OrderIdentity {
        local_attempt_id: 1,
        signed_hash: Some("fixture-hash".into()),
        venue_order_id: None,
        market_id: 1,
        token_id: market.yes_token.clone(),
        side: Side::Buy,
        account: trader.identity(),
    };
    {
        let mut state = broker.state.lock();
        super::super::live::reserve_attempt(
            &mut state.live,
            AttemptRecord {
                identity,
                request_gross_atoms: U256::from(5),
                r0: U256::from(2_000_000_000_000_000_000u64),
                reserved_sell: U256::ZERO,
                phase: AttemptPhase::MayHaveSubmitted,
                submission: Submission::Unknown,
                last_reason: None,
            },
        );
        state.resting.insert(
            "fixture".into(),
            super::super::Resting {
                id: "fixture".into(),
                hash: "fixture-hash".into(),
                market_id: 1,
                token: market.yes_token.clone(),
                side: Side::Buy,
                price: 0.4,
                shares: 5.0,
                filled: 0.0,
                expires_at: Utc::now(),
                removing: true,
            },
        );
    }
    let before = broker.paper_snapshot().unwrap();
    assert_eq!(before.reserved, 2.0); // Same risk is not summed twice.
    assert_eq!(before.available, 98.0);
    assert_eq!(
        broker
            .take(&market, &market.yes_token, Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
        TakeResult::Blocked(Blocked::MarketCap)
    );
    assert_eq!(broker.paper_snapshot().unwrap(), before);
    broker.state.lock().live.attempts.clear();
    broker.state.lock().live.reservations.clear();
    assert_eq!(broker.paper_snapshot().unwrap().reserved, 2.0); // Removing != terminal.
}

#[tokio::test]
async fn stop_notification_upgrades_deadline_to_emergency() {
    let broker = controlled(100.0);
    broker.stop_entries(ControlledStop::Deadline);
    broker.wait_controlled_stop().await;
    broker.stop_entries(ControlledStop::ParentLost);
    tokio::time::timeout(
        std::time::Duration::from_millis(50),
        broker.wait_emergency(),
    )
    .await
    .unwrap();
    assert_eq!(broker.entry_stop_reason(), Some("PARENT_LOST"));
    assert!(!broker.paper_snapshot().unwrap().loss_latched);
}

#[tokio::test]
async fn official_tail_is_bounded_and_only_reads_traded_markets() {
    use crate::service::venue::{MarketData, MarketQuery};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Data {
        market: TradableMarket,
        reads: AtomicUsize,
        pending: bool,
    }
    #[async_trait::async_trait]
    impl MarketData for Data {
        async fn market(&self, id: u64) -> Result<TradableMarket> {
            assert_eq!(id, self.market.id);
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.pending {
                std::future::pending::<()>().await;
            }
            Ok(self.market.clone())
        }
        async fn markets(&self, _: &MarketQuery) -> Result<Vec<TradableMarket>> {
            panic!("SCAN_FORBIDDEN")
        }
        async fn with_schedule(&self, _: &TradableMarket) -> Result<TradableMarket> {
            panic!("SCHEDULE_FORBIDDEN")
        }
        async fn books(&self, _: &[u64]) -> Result<HashMap<u64, BookSnapshot>> {
            panic!("BOOK_FORBIDDEN")
        }
    }
    for pending in [false, true] {
        let broker = controlled(100.0);
        let mut market = market(1);
        let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
        broker.observe(&market, &book);
        broker
            .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap();
        market.end_price = Some(101.0);
        market.payout_yes = Some(1.0);
        market.round_facts = Some(facts("WON", "LOST"));
        let data = Data {
            market,
            reads: AtomicUsize::new(0),
            pending,
        };
        broker.stop_entries(ControlledStop::Deadline);
        tokio::time::timeout(
            std::time::Duration::from_millis(150),
            broker.tail_settle(
                &data,
                std::time::Duration::from_millis(30),
                std::time::Duration::from_millis(10),
            ),
        )
        .await
        .unwrap();
        assert_eq!(data.reads.load(Ordering::SeqCst), 1);
        assert!(
            (broker.paper_snapshot().unwrap().cash - if pending { 98.0 } else { 102.9 }).abs()
                < EPS
        );
    }
}

#[tokio::test]
async fn loss_is_checked_before_the_next_take_and_never_unlatches() {
    let broker = controlled(100.0);
    let market = market(1);
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    {
        let mut state = broker.state.lock();
        state.realized = -20.0;
        state.paper.as_mut().unwrap().cash = 80.0;
    }
    assert_eq!(
        broker
            .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
        TakeResult::Blocked(Blocked::LossLatch)
    );
    {
        let mut state = broker.state.lock();
        state.realized = -10.0;
        state.paper.as_mut().unwrap().cash = 90.0;
    }
    broker.observe(&market, &book);
    assert!(broker.is_halted());
    assert!(broker.paper_snapshot().unwrap().loss_latched);
}

fn paper_with_cash() -> PaperCash {
    PaperCash::new(100.0)
}

fn facts(yes: &str, no: &str) -> RoundFacts {
    RoundFacts {
        oracle_status: Some("RESOLVED".into()),
        trading_status: Some("CLOSED".into()),
        yes_status: Some(yes.into()),
        no_status: Some(no.into()),
        decimal_precision: Some(3),
        price_feed_provider: Some("CHAINLINK".into()),
    }
}

fn market_with_prices(start: f64, end: f64) -> TradableMarket {
    TradableMarket {
        id: 1,
        yes_token: "yes-1".into(),
        no_token: "no-1".into(),
        start_price: Some(start),
        end_price: Some(end),
        payout_yes: Some(if end > start {
            1.0
        } else if end < start {
            0.0
        } else {
            0.5
        }),
        ..Default::default()
    }
}

/// A009: the fee comes out of the shares, not out of the cash twice.
#[test]
fn a_buy_takes_the_fee_from_the_shares() {
    let (net, fee) = net_shares(5.0, 0.4, 200).unwrap();
    assert!((fee - 0.04).abs() < 1e-9, "{fee}");
    assert!((net - 4.9).abs() < 1e-9, "{net}");
}

/// A009: the cash drops by the gross cost only, and the fee is not taken
/// from the cash a second time.
#[test]
fn a_buy_costs_the_gross_amount() {
    let mut paper = paper_with_cash();
    paper.reservation = Some(Reservation {
        market_id: 1,
        token: "yes-1".into(),
        worst_case: 2.0,
    });
    let (net, fee) = apply_buy(&mut paper, 1, "yes-1", 5.0, 0.4, 200).unwrap();
    assert!((net - 4.9).abs() < 1e-9);
    assert!((fee - 0.04).abs() < 1e-9);
    assert!((paper.cash - 98.0).abs() < 1e-9, "{}", paper.cash);
    assert_eq!(paper.reserved(), 0.0, "the reservation became the cost");
}

/// A008: the price improvement goes back to the free cash.
#[test]
fn a_cheaper_fill_frees_the_difference() {
    let mut paper = paper_with_cash();
    paper.reservation = Some(Reservation {
        market_id: 1,
        token: "yes-1".into(),
        worst_case: 2.0,
    });
    apply_buy(&mut paper, 1, "yes-1", 5.0, 0.4, 200).unwrap();
    // The worst case was 2.00 and the real cost was 2.00 here; a cheaper
    // walk leaves the difference free.
    assert!((paper.available() - 98.0).abs() < 1e-9);
}

/// A007: a reservation that belongs to another order is not consumed.
#[test]
fn a_fill_cannot_use_another_orders_reservation() {
    let mut paper = paper_with_cash();
    paper.reservation = Some(Reservation {
        market_id: 2,
        token: "no-2".into(),
        worst_case: 2.0,
    });
    assert!(apply_buy(&mut paper, 1, "yes-1", 5.0, 0.4, 200).is_none());
    assert!(
        (paper.reserved() - 2.0).abs() < 1e-9,
        "the reservation survives"
    );
    assert!((paper.cash - 100.0).abs() < 1e-9, "the cash is untouched");
}

/// A010: a SELL pays the fee out of the cash and never exceeds the net
/// inventory.
#[test]
fn a_sell_pays_the_fee_out_of_the_cash() {
    let mut paper = paper_with_cash();
    let (net_cash, basis, pnl, fee) = apply_sell(&mut paper, 4.9, 0.6, 200, 2.0 / 4.9).unwrap();
    assert!((fee - 0.0392).abs() < 1e-9, "{fee}");
    assert!((net_cash - 2.9008).abs() < 1e-9, "{net_cash}");
    assert!((basis - 2.0).abs() < 1e-9, "{basis}");
    assert!((pnl - 0.9008).abs() < 1e-9, "{pnl}");
    assert!((paper.cash - 102.9008).abs() < 1e-9, "{}", paper.cash);
}

/// A011: an official payout adds the income once.
#[test]
fn a_settlement_pays_the_net_inventory_once() {
    let mut paper = paper_with_cash();
    let income = apply_settlement(&mut paper, 4.9, 1.0).unwrap();
    assert!((income - 4.9).abs() < 1e-9);
    assert!((paper.cash - 104.9).abs() < 1e-9);
}

/// A011: the venue's own facts decide, and a contradiction is a conflict.
#[test]
fn the_official_facts_decide_the_outcome() {
    let market = market_with_prices(100.0, 101.0);
    assert_eq!(
        settle_from_facts(&market, &facts("WON", "LOST"), 100.0, 101.0),
        Settlement::Confirmed
    );
    assert_eq!(
        settle_from_facts(&market, &facts("LOST", "WON"), 100.0, 101.0),
        Settlement::Conflict
    );
    assert_eq!(
        settle_from_facts(&market, &facts("WON", "LOST"), 100.0, 100.0),
        Settlement::Confirmed,
        "an exact tie is settled"
    );
    // Not resolved yet.
    let mut pending = facts("WON", "LOST");
    pending.oracle_status = Some("PENDING".into());
    assert_eq!(
        settle_from_facts(&market, &pending, 100.0, 101.0),
        Settlement::Pending
    );
    // The market is contested.
    let mut contested = market.clone();
    contested.contested = true;
    assert_eq!(
        settle_from_facts(&contested, &facts("WON", "LOST"), 100.0, 101.0),
        Settlement::Pending
    );
}

/// A011: a tie pays half a share, whatever the venue marked.
#[test]
fn a_tie_pays_half() {
    let market = market_with_prices(100.0, 100.0);
    assert_eq!(settlement_payout(&market, 100.0, 100.0), Some(0.5));
}

/// A012: the loss limit latches at exactly -20 and not at -19.999.
#[test]
fn the_loss_limit_latches_at_the_threshold() {
    let mut paper = paper_with_cash();
    // One share bought at 19.0 and marked at 18.0: the unrealized result is
    // -1.0, so with a realized -19 the net result is exactly -20.
    let book = holdings(&[("yes-1", 1.0, 19.0)]);
    mark(&mut paper, "yes-1", 18.0, Utc::now(), true);
    assert!(loss_limit_reached(&paper, &book, -19.0, 20.0));
    // Realized -19 with no inventory: -19, below the limit.
    assert!(!loss_limit_reached(&paper, &holdings(&[]), -19.0, 20.0));
    // Realized -19 with an unrealized -0.999: -19.999, still below.
    let mut paper = paper_with_cash();
    let book = holdings(&[("yes-1", 1.0, 19.0)]);
    mark(&mut paper, "yes-1", 18.001, Utc::now(), true);
    assert!(!loss_limit_reached(&paper, &book, -19.0, 20.0));
}

/// A012: a peak does not latch, and a recovery does not unlock.
#[test]
fn a_peak_drawdown_does_not_latch() {
    let mut paper = paper_with_cash();
    // Realized +10 with an unrealized -11: the net result is -1.
    let book = holdings(&[("yes-1", 1.0, 21.0)]);
    mark(&mut paper, "yes-1", 10.0, Utc::now(), true);
    assert!(!loss_limit_reached(&paper, &book, 10.0, 20.0));
}

/// A013: without a usable mark there is no valuation, so no proof of
/// health.
#[test]
fn an_unvaluable_position_is_not_a_healthy_one() {
    let mut paper = paper_with_cash();
    let book = holdings(&[("yes-1", 1.0, 2.0)]);
    assert!(!marks_complete(&paper, &book), "no mark yet");
    assert_eq!(marked_value(&paper, &book), None);
    // A mark from an older tick is not usable either.
    mark(&mut paper, "yes-1", 0.5, Utc::now(), true);
    assert!(marks_complete(&paper, &book));
    invalidate_marks(&mut paper);
    assert!(!marks_complete(&paper, &book), "an old mark proves nothing");
    assert_eq!(marked_value(&paper, &book), None);
}

/// A009: the snapshot agrees with the cash identity.
#[test]
fn the_snapshot_agrees_with_the_cash_identity() {
    let mut paper = paper_with_cash();
    paper.reservation = Some(Reservation {
        market_id: 1,
        token: "yes-1".into(),
        worst_case: 2.0,
    });
    apply_buy(&mut paper, 1, "yes-1", 5.0, 0.4, 200).unwrap();
    let book = holdings(&[("yes-1", 4.9, 2.0)]);
    mark(&mut paper, "yes-1", 0.4, Utc::now(), true);
    let snapshot = snapshot(&paper, &book, 0.0);
    assert!((snapshot.cash - 98.0).abs() < 1e-9);
    assert!((snapshot.available - 98.0).abs() < 1e-9);
    assert!((snapshot.cost - 2.0).abs() < 1e-9);
    // U = 4.9 × 0.4 - 2.0 = -0.04, so NAV = 99.96.
    assert!((snapshot.unrealized.unwrap() + 0.04).abs() < 1e-9);
    assert!((snapshot.marked.unwrap() - 1.96).abs() < 1e-9);
    assert!((snapshot.nav.unwrap() - 99.96).abs() < 1e-9);
    assert!((snapshot.net_pnl.unwrap() + 0.04).abs() < 1e-9);
}

/// A008: the fee is a display fact, never a second cash deduction.
#[test]
fn the_fee_is_never_deducted_twice() {
    let mut paper = paper_with_cash();
    paper.reservation = Some(Reservation {
        market_id: 1,
        token: "yes-1".into(),
        worst_case: 2.0,
    });
    let (net, fee) = apply_buy(&mut paper, 1, "yes-1", 5.0, 0.4, 200).unwrap();
    let book = holdings(&[("yes-1", net, 2.0)]);
    mark(&mut paper, "yes-1", 0.4, Utc::now(), true);
    let snapshot = snapshot(&paper, &book, 0.0);
    // The net result is the unrealized loss of the fee only: -0.04, not
    // -0.08.
    assert!(
        (snapshot.net_pnl.unwrap() + 0.04).abs() < 1e-9,
        "{snapshot:?}"
    );
    assert!((fee - 0.04).abs() < 1e-9);
}

/// A001: an invalid price or size is refused, not rounded up.
#[test]
fn invalid_numbers_are_refused() {
    assert!(net_shares(5.0, 0.0, 200).is_none());
    assert!(net_shares(5.0, 1.0, 200).is_none());
    assert!(net_shares(f64::NAN, 0.4, 200).is_none());
    assert!(net_shares(0.0, 0.4, 200).is_none());
}

/// A controlled broker with the fixed 100 USDT of virtual cash.
fn controlled(cash: f64) -> Broker {
    controlled_with(controlled_limits(), cash)
}

/// The limits a controlled run uses: 2 / 2 / 10 / 20.
fn controlled_limits() -> StrategyLimits {
    StrategyLimits {
        max_order_usd: 2.0,
        max_market_usd: 2.0,
        max_total_usd: 10.0,
        max_loss_usd: 20.0,
    }
}

fn controlled_with(limits: StrategyLimits, cash: f64) -> Broker {
    let run =
        ControlledRun::new(ControlledMode::UnsignedPaper, "test-run".into(), Utc::now()).unwrap();
    Broker::new(limits, false, None, None)
        .unwrap()
        .with_controlled_paper(run, cash)
        .unwrap()
}
/// A controlled broker needs a book before it may enter.
fn marked_market() -> TradableMarket {
    market(1)
}

/// A009: the controlled broker books a BUY with the fee taken from the
/// shares, and the cash identity holds.
#[tokio::test]
async fn a_controlled_buy_takes_the_fee_from_the_shares() {
    let b = controlled(100.0);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    // The market must have a usable mark from this tick.
    b.observe(&m, &bk);
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    match r {
        TakeResult::Filled { shares, price } => {
            assert!((shares - 4.9).abs() < 1e-9, "{shares}");
            assert!((price - 0.4).abs() < 1e-9, "{price}");
        }
        other => panic!("expected a fill, got {other:?}"),
    }
    let snapshot = b.paper_snapshot().expect("a controlled snapshot");
    assert!((snapshot.cash - 98.0).abs() < 1e-9, "{snapshot:?}");
    assert!((snapshot.available - 98.0).abs() < 1e-9);
    assert!(
        (snapshot.reserved - 0.0).abs() < 1e-9,
        "no reservation is left"
    );
    assert!((snapshot.cost - 2.0).abs() < 1e-9);
    assert!((snapshot.unrealized.unwrap() + 0.04).abs() < 1e-9);
    assert!((snapshot.nav.unwrap() - 99.96).abs() < 1e-9);
    assert!((snapshot.net_pnl.unwrap() + 0.04).abs() < 1e-9);
}

/// A007: a controlled broker with too little cash refuses the order and
/// changes nothing.
#[tokio::test]
async fn a_controlled_buy_without_cash_is_refused() {
    let b = controlled(1.5);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    b.observe(&m, &bk);
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::InsufficientCash));
    let snapshot = b.paper_snapshot().expect("a controlled snapshot");
    assert!((snapshot.cash - 1.5).abs() < 1e-9);
    assert!(b.holdings().is_empty());
}

/// A008: the market cap and the total cap are separate limits: one market
/// may hold 2 USDT, and the whole run may hold 10 USDT.
#[tokio::test]
async fn the_controlled_caps_are_separate_limits() {
    let b = controlled(100.0);
    let bk = book(&[(0.4, 1000.0)], &[(0.4, 1000.0)]);
    // One BUY of 2 USDT in the first market.
    let first = market(1);
    b.observe(&first, &bk);
    let r = b
        .take(&first, "yes-1", Side::Buy, 5.0, 0.4, &bk)
        .await
        .unwrap();
    assert!(matches!(r, TakeResult::Filled { .. }), "{r:?}");
    // A second BUY in the same market would exceed the 2 USDT market cap.
    let r = b
        .take(&first, "yes-1", Side::Buy, 5.0, 0.4, &bk)
        .await
        .unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::MarketCap), "{r:?}");
    // Four more markets fit the 10 USDT total exactly.
    for index in 2..=5 {
        let m = market(index);
        b.observe(&m, &bk);
        let r = b
            .take(&m, &format!("yes-{index}"), Side::Buy, 5.0, 0.4, &bk)
            .await
            .unwrap();
        assert!(matches!(r, TakeResult::Filled { .. }), "{r:?}");
    }
    // A sixth market would exceed the total.
    let sixth = market(6);
    b.observe(&sixth, &bk);
    let r = b
        .take(&sixth, "yes-6", Side::Buy, 5.0, 0.4, &bk)
        .await
        .unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::TotalCap), "{r:?}");
    // A single order above 2 USDT is refused by the order cap.
    let big = market(9);
    b.observe(&big, &bk);
    let r = b
        .take(&big, "yes-9", Side::Buy, 10.0, 0.4, &bk)
        .await
        .unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::OrderCap));
}

/// A013: without a usable mark there is no entry, and the old mark doesn't
/// stand in for it.
#[tokio::test]
async fn a_controlled_entry_needs_a_fresh_mark() {
    let b = controlled(100.0);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    // No observe yet: no mark, so no entry.
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::DataIncomplete));
    // A one-sided book proves nothing either.
    b.observe(&m, &book(&[(0.4, 100.0)], &[]));
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::DataIncomplete));
    // A two-sided book does.
    b.observe(&m, &bk);
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    assert!(matches!(r, TakeResult::Filled { .. }), "{r:?}");
}

/// A007: a controlled broker has no venue, and a live one can't be turned
/// into a controlled one.
#[tokio::test]
async fn a_controlled_broker_refuses_a_wallet() {
    let venue = Arc::new(FakeVenue::default());
    let live = Broker::new(
        limits(),
        true,
        Some(venue.clone() as Arc<dyn OrderVenue>),
        Some(venue.clone() as Arc<dyn LimitVenue>),
    )
    .unwrap();
    let run =
        ControlledRun::new(ControlledMode::UnsignedPaper, "test-run".into(), Utc::now()).unwrap();
    assert!(live.with_controlled_paper(run, 100.0).is_err());
}

/// A016: a stopped controlled run adds no risk.
#[tokio::test]
async fn a_stopped_controlled_run_adds_no_risk() {
    let b = controlled(100.0);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    b.observe(&m, &bk);
    b.stop_entries(ControlledStop::ParentLost);
    let r = b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    assert_eq!(r, TakeResult::Blocked(Blocked::Deadline));
    assert_eq!(b.entry_stop_reason(), Some("PARENT_LOST"));
    assert!(b.holdings().is_empty());
}

/// A009: a controlled run doesn't rest orders.
#[tokio::test]
async fn a_controlled_run_does_not_rest_orders() {
    let b = controlled(100.0);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    let r = b
        .rest(&m, "yes-1", Side::Buy, 5.0, 0.4, 60, &bk)
        .await
        .unwrap();
    assert_eq!(r, Err(Blocked::ControlledRestUnsupported));
}

/// A010: a controlled SELL never exceeds the net inventory, and the fee
/// comes out of the cash.
#[tokio::test]
async fn a_controlled_sell_is_clamped_to_the_net_inventory() {
    let b = controlled(100.0);
    let m = marked_market();
    let bk = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    b.observe(&m, &bk);
    b.take(&m, "yes-1", Side::Buy, 5.0, 0.4, &bk).await.unwrap();
    let exit = book(&[(0.6, 100.0)], &[(0.6, 100.0)]);
    b.observe(&m, &exit);
    let r = b
        .take(&m, "yes-1", Side::Sell, 5.0, 0.6, &exit)
        .await
        .unwrap();
    match r {
        TakeResult::Filled { shares, .. } => {
            assert!(
                (shares - 4.9).abs() < 1e-9,
                "the gross 5 never filled: {shares}"
            );
        }
        other => panic!("expected a fill, got {other:?}"),
    }
    let snapshot = b.paper_snapshot().expect("a controlled snapshot");
    // The cash is 98 + 2.9008 and the realized result is 0.9008.
    assert!((snapshot.cash - 100.9008).abs() < 1e-9, "{snapshot:?}");
    assert!((snapshot.realized - 0.9008).abs() < 1e-9, "{snapshot:?}");
}

#[tokio::test]
async fn better_vwap_releases_difference_and_summary_never_double_charges() {
    let broker = controlled(100.0);
    let market = marked_market();
    let book = book(&[(0.35, 100.0)], &[(0.35, 100.0)]);
    broker.observe(&market, &book);
    assert!(matches!(
        broker
            .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
        TakeResult::Filled { .. }
    ));
    let snap = broker.paper_snapshot().unwrap();
    assert!((snap.cash - 98.25).abs() < EPS);
    assert_eq!(snap.reserved, 0.0);
    assert!((broker.summary().equity() - snap.net_pnl.unwrap()).abs() < EPS);
}

#[tokio::test]
async fn an_existing_unclosed_reservation_cannot_be_overwritten() {
    let broker = controlled(100.0);
    let market = marked_market();
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    let reservation = Reservation {
        market_id: 99,
        token: "yes-99".into(),
        worst_case: 2.0,
    };
    broker.state.lock().paper.as_mut().unwrap().reservation = Some(reservation.clone());
    assert!(matches!(
        broker
            .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
        TakeResult::Blocked(_)
    ));
    assert_eq!(
        broker.state.lock().paper.as_ref().unwrap().reservation,
        Some(reservation)
    );
    assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
}

#[tokio::test]
async fn invalid_token_precision_and_book_leave_funds_unchanged() {
    for fault in ["token", "precision", "size", "price", "crossed", "fee"] {
        let broker = controlled(100.0);
        let mut market = marked_market();
        let mut book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
        let token = if fault == "token" { "alien" } else { "yes-1" };
        match fault {
            "precision" => market.round_facts = None,
            "size" => book.asks[0].size = f64::NAN,
            "price" => book.asks[0].price = f64::NAN,
            "crossed" => book.bids[0].price = 0.6,
            "fee" => market.fee_rate_bps = 10_001,
            _ => (),
        }
        broker.observe(&market, &book);
        assert!(
            matches!(
                broker
                    .take(&market, token, Side::Buy, 5.0, 0.4, &book)
                    .await
                    .unwrap(),
                TakeResult::Blocked(_)
            ),
            "{fault}"
        );
        assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0, "{fault}");
    }
}

#[test]
fn a_draw_requires_opposite_official_outcomes() {
    let market = market_with_prices(100.0, 100.0);
    assert_eq!(
        settle_from_facts(&market, &facts("", ""), 100.0, 100.0),
        Settlement::Pending
    );
    assert_eq!(
        settle_from_facts(&market, &facts("WON", "WON"), 100.0, 100.0),
        Settlement::Conflict
    );
}

struct SettlementData(TradableMarket);
#[async_trait::async_trait]
impl crate::service::venue::MarketData for SettlementData {
    async fn market(&self, id: u64) -> Result<TradableMarket> {
        assert_eq!(id, self.0.id);
        Ok(self.0.clone())
    }
    async fn markets(&self, _: &crate::service::venue::MarketQuery) -> Result<Vec<TradableMarket>> {
        panic!("SCAN_FORBIDDEN")
    }
    async fn with_schedule(&self, _: &TradableMarket) -> Result<TradableMarket> {
        panic!("SCHEDULE_FORBIDDEN")
    }
    async fn books(&self, _: &[u64]) -> Result<HashMap<u64, BookSnapshot>> {
        panic!("BOOK_FORBIDDEN")
    }
}

#[tokio::test]
async fn missing_official_identity_is_pending_then_pays_once() {
    let broker = controlled(100.0);
    let mut market = marked_market();
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    broker.take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book).await.unwrap();
    market.open = false;
    market.end_price = Some(101.0);
    market.payout_yes = Some(1.0);
    market.round_facts = Some(facts("WON", "LOST"));
    let mut missing = market.clone();
    missing.feed_symbol = None;
    broker.settle_resolved(&SettlementData(missing)).await.unwrap();
    assert_eq!(broker.state.lock().paper.as_ref().unwrap().settlements[&1], Settlement::Pending);
    assert_eq!(broker.paper_snapshot().unwrap().cash, 98.0);
    assert_eq!(broker.paper_snapshot().unwrap().cost, 2.0);
    assert!(broker.entry_stop_reason().is_none());
    broker.settle_resolved(&SettlementData(market.clone())).await.unwrap();
    broker.settle_resolved(&SettlementData(market)).await.unwrap();
    assert!((broker.paper_snapshot().unwrap().cash - 102.9).abs() < EPS);
}

fn missing_result(market: &mut TradableMarket, field: &str) {
    match field {
        "yes" => market.yes_token.clear(),
        "no" => market.no_token.clear(),
        "start" => market.starts_at = None,
        "end" => market.ends_at = None,
        "strike" => market.start_price = None,
        "feed" => market.feed_symbol = None,
        "empty_feed" => market.feed_symbol = Some(String::new()),
        "end_price" => market.end_price = None,
        "payout" => market.payout_yes = None,
        "facts" => market.round_facts = None,
        "yes_status" => market.round_facts.as_mut().unwrap().yes_status = None,
        "no_status" => market.round_facts.as_mut().unwrap().no_status = None,
        "oracle" => market.round_facts.as_mut().unwrap().oracle_status = None,
        "trading" => market.round_facts.as_mut().unwrap().trading_status = None,
        "all" => {
            market.yes_token.clear(); market.no_token.clear();
            market.starts_at = None; market.ends_at = None; market.start_price = None;
            market.feed_symbol = None; market.end_price = None; market.payout_yes = None;
            market.round_facts = None;
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn official_missing_matrix_preserves_pending_and_applied_books() {
    for payout in [0.0, 0.5, 1.0] {
        for field in ["yes", "no", "start", "end", "strike", "feed", "empty_feed", "end_price",
            "payout", "facts", "yes_status", "no_status", "oracle", "trading", "all"] {
            let broker = controlled(100.0);
            let mut market = marked_market();
            let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
            broker.observe(&market, &book);
            broker.take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book).await.unwrap();
            market.open = false;
            market.end_price = Some(if payout == 1.0 { 101.0 } else if payout == 0.0 { 99.0 } else { 100.0 });
            market.payout_yes = Some(payout);
            market.round_facts = Some(if payout == 0.0 { facts("LOST", "WON") } else { facts("WON", "LOST") });
            let mut missing = market.clone();
            missing_result(&mut missing, field);
            let before = broker.paper_snapshot().unwrap();
            broker.settle_resolved(&SettlementData(missing.clone())).await.unwrap();
            // A DRAW's explicit payout may be absent: the exact tie supplies 0.5.
            let draw_payout = payout == 0.5 && field == "payout";
            if !draw_payout {
                assert_eq!(broker.paper_snapshot().unwrap(), before, "{field}/{payout}");
                assert_eq!(broker.state.lock().paper.as_ref().unwrap().settlements[&1], Settlement::Pending);
                assert!(!broker.holdings().is_empty());
            }
            assert!(broker.entry_stop_reason().is_none(), "{field}/{payout}");
            broker.settle_resolved(&SettlementData(market.clone())).await.unwrap();
            let paid = broker.paper_snapshot().unwrap();
            assert!((paid.cash - (98.0 + 4.9 * payout)).abs() < EPS);
            assert!(broker.holdings().is_empty());
            let reread = crate::service::venue::MarketData::market(&SettlementData(missing), 1).await.unwrap();
            broker.controlled_settle(&reread);
            broker.controlled_settle(&market);
            assert_eq!(broker.paper_snapshot().unwrap(), paid, "{field}/{payout}");
            assert_eq!(broker.state.lock().paper.as_ref().unwrap().settlements[&1], Settlement::Confirmed);
            assert!(broker.entry_stop_reason().is_none(), "post-paid {field}/{payout}");
        }
    }
}

#[tokio::test]
async fn official_known_contradictions_win_over_missing_fields() {
    for applied in [false, true] {
        for fault in ["yes", "no", "same_tokens", "start", "end", "strike", "nan", "zero", "negative", "infinity", "invalid_end", "feed",
            "end_price", "payout", "outcome", "dispute", "nonterminal"] {
            let broker = controlled(100.0);
            let mut market = marked_market();
            let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
            broker.observe(&market, &book);
            broker.take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book).await.unwrap();
            market.open = false; market.end_price = Some(101.0); market.payout_yes = Some(1.0);
            market.round_facts = Some(facts("WON", "LOST"));
            if applied { broker.settle_resolved(&SettlementData(market.clone())).await.unwrap(); }
            let before = broker.paper_snapshot().unwrap();
            match fault {
                "yes" => { market.yes_token = "alien".into(); market.feed_symbol = None; }
                "no" => { market.no_token = "alien".into(); market.starts_at = None; }
                "same_tokens" => { market.no_token = market.yes_token.clone(); market.start_price = None; }
                "start" => { market.starts_at = market.starts_at.map(|value| value + chrono::Duration::seconds(1)); market.feed_symbol = None; }
                "end" => { market.ends_at = market.ends_at.map(|value| value + chrono::Duration::seconds(1)); market.yes_token.clear(); }
                "strike" => { market.start_price = Some(102.0); market.feed_symbol = None; }
                "nan" => { market.start_price = Some(f64::NAN); market.ends_at = None; }
                "zero" => { market.start_price = Some(0.0); market.starts_at = None; }
                "negative" => { market.start_price = Some(-1.0); market.feed_symbol = None; }
                "infinity" => { market.start_price = Some(f64::INFINITY); market.no_token.clear(); }
                "invalid_end" => { market.end_price = Some(f64::NAN); market.round_facts = None; }
                "feed" => { market.feed_symbol = Some("ETHUSDT".into()); market.start_price = None; }
                "end_price" => { market.end_price = Some(102.0); market.round_facts = None; }
                "payout" => { market.payout_yes = Some(0.0); market.end_price = None; }
                "outcome" => { market.round_facts.as_mut().unwrap().yes_status = Some("LOST".into()); market.round_facts.as_mut().unwrap().no_status = None; }
                "dispute" => market.contested = true,
                "nonterminal" => market.round_facts.as_mut().unwrap().oracle_status = Some("PENDING".into()),
                _ => unreachable!(),
            }
            // settle_resolved scans holdings only; explicitly exercise the same
            // application consumer after payment has removed those holdings.
            let reread = crate::service::venue::MarketData::market(&SettlementData(market), 1).await.unwrap();
            broker.controlled_settle(&reread);
            assert_eq!(broker.paper_snapshot().unwrap(), before, "{fault}/{applied}");
            let conflict = applied || !["end_price", "payout", "outcome", "dispute", "nonterminal"].contains(&fault);
            assert_eq!(broker.state.lock().paper.as_ref().unwrap().settlements[&1],
                if conflict { Settlement::Conflict } else { Settlement::Pending }, "{fault}/{applied}");
            assert_eq!(broker.entry_stop_reason().is_some(), conflict, "{fault}/{applied}");
        }
    }
}

#[tokio::test]
async fn official_result_identity_and_post_payment_conflict_preserve_cash() {
    let broker = controlled(100.0);
    let mut market = marked_market();
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    broker
        .take(&market, "yes-1", Side::Buy, 5.0, 0.4, &book)
        .await
        .unwrap();
    market.open = false;
    market.end_price = Some(101.0);
    market.payout_yes = Some(1.0);
    market.round_facts = Some(facts("WON", "LOST"));
    let mut wrong = market.clone();
    wrong.no_token = "alien".into();
    broker.controlled_settle(&wrong);
    assert_eq!(broker.paper_snapshot().unwrap().cash, 98.0);
    broker.controlled_settle(&market);
    assert!((broker.paper_snapshot().unwrap().cash - 102.9).abs() < EPS);
    broker.controlled_settle(&market);
    assert!((broker.paper_snapshot().unwrap().cash - 102.9).abs() < EPS);
    market.end_price = Some(99.0);
    market.payout_yes = Some(0.0);
    market.round_facts = Some(facts("LOST", "WON"));
    broker.controlled_settle(&market);
    assert_eq!(
        broker.state.lock().paper.as_ref().unwrap().settlements[&1],
        Settlement::Conflict
    );
    assert!(broker.entry_stop_reason().is_some());
    assert!((broker.paper_snapshot().unwrap().cash - 102.9).abs() < EPS);
}
