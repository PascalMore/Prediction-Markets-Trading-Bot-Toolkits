use super::*;
use crate::bot::runtime::test_support::book;
use crate::bot::runtime::tests::{limits, FakeVenue};
use crate::config::{ControlledMode, StrategyLimits};
use crate::service::venue::{LimitVenue, OrderVenue};

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

#[tokio::test]
async fn protocol_consumer_fixture() {
    let broker = controlled(100.0);
    let mut market = market(1);
    market.yes_token = "77".into();
    market.no_token = "88".into();
    let book = book(&[(0.4, 100.0)], &[(0.4, 100.0)]);
    broker.observe(&market, &book);
    broker.record_source("77", "Binance", Utc::now());
    emit_summary(&broker);
    broker
        .take(&market, "77", Side::Buy, 5.0, 0.4, &book)
        .await
        .unwrap();
    assert_eq!(
        broker
            .take(&market, "77", Side::Buy, 5.0, 0.4, &book)
            .await
            .unwrap(),
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
    ] {
        let mut broker = controlled(100.0);
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
            let result = broker
                .take(&market, &market.yes_token, Side::Buy, 5.0, 0.4, &book)
                .await
                .unwrap();
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
