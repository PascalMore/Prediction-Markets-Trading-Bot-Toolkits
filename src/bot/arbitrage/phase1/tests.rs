use super::super::{Availability, DecisionCore, RoundTaker};
use super::*;
use crate::bot::runtime::{paper, Broker, TakeResult};
use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
use crate::service::observation::{Clock, FixtureClock, ObservedBook, ObservedSpot};
use crate::service::spot::SpotFeed;
use crate::service::venue::RoundFacts;
use std::collections::HashMap;

fn forward_fixture() -> (
    RoundTaker,
    Broker,
    Arc<FixtureClock>,
    ObservedSpot,
    ObservedBook,
) {
    let mut market = market(200);
    market.yes_token = "1001".into();
    market.no_token = "1002".into();
    market.round_facts = Some(RoundFacts {
        decimal_precision: Some(2),
        price_feed_provider: Some("CHAINLINK".into()),
        ..Default::default()
    });
    let cfg = crate::config::BtcArbConfig {
        order_usd: 2.0,
        ..Default::default()
    };
    let clock = Arc::new(FixtureClock::new(start()));
    clock.advance(101_000);
    let broker = Broker::new(
        StrategyLimits {
            max_order_usd: 2.0,
            max_market_usd: 2.0,
            max_total_usd: 10.0,
            max_loss_usd: 20.0,
        },
        false,
        None,
        None,
    )
    .unwrap()
    .with_controlled_paper(
        ControlledRun::new(
            ControlledMode::UnsignedPaper,
            "phase1-fixture".into(),
            Utc::now(),
        )
        .unwrap(),
        100.0,
    )
    .unwrap()
    .with_phase1(clock.clone(), StrategyContract::Phase1NetShares, 0.03, 30.0)
    .unwrap();
    let mut available = Availability::default();
    available.record(&market, start());
    let bot = RoundTaker {
        cfg: cfg.clone(),
        core: DecisionCore::phase1(cfg),
        spot: SpotFeed::new().unwrap(),
        known: Default::default(),
        rounds: vec![market.clone()],
        last_scan: None,
        observer: None,
        observed_quote: None,
        observed_symbols: Vec::new(),
        availability: [(market.id, available)].into_iter().collect(),
        inputs: None,
        rounds_error: false,
    };
    let quote = ObservedSpot {
        price: 100.2,
        source: SpotSource::Binance,
        actual_pair: "BTCUSDT".into(),
        clock: clock_at_latest(),
    };
    let observed = ObservedBook {
        book: book(),
        clock: clock_at_latest(),
    };
    (bot, broker, clock, quote, observed)
}

fn clock_at_latest() -> ObservationClock {
    clock(start(), 100_000, 100_100)
}

#[tokio::test]
async fn round_scan_failure_preserves_selected_attempts() {
    use crate::bot::runtime::{Ctx, Strategy};
    use crate::service::predict_fun::PredictFunClient;
    for (selected, entered, stopped, legacy) in [
        (2, false, false, false),
        (2, true, false, false),
        (2, true, true, false),
        (0, false, false, false),
        (2, false, false, true),
    ] {
        paper::clear_recorded_events();
        let (mut bot, broker, _, _, observed) = forward_fixture();
        if selected == 0 {
            bot.rounds.clear();
        } else {
            let mut second = bot.rounds[0].clone();
            second.id = 2;
            second.yes_token = "1003".into();
            second.no_token = "1004".into();
            bot.rounds.push(second);
        }
        if legacy {
            bot.core = DecisionCore::new(bot.cfg.clone());
        }
        if entered {
            bot.core.confirm_fill(1, 1.0);
        }
        if stopped {
            broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
        }
        bot.known.remember(&bot.rounds);
        for market in &bot.rounds {
            broker.observe(market, &observed.book);
        }
        bot.rounds_error = true;
        let before_ids: Vec<_> = bot.rounds.iter().map(|market| market.id).collect();
        let mut cfg: crate::config::AppConfig = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tools/predictfun/btc5m-controlled-paper.json"
        )))
        .unwrap();
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        let client = PredictFunClient::new(&cfg).unwrap();
        let ctx = Ctx {
            cfg,
            data: client.clone(),
            client,
            broker: Arc::new(broker),
        };
        let error = bot.tick(&ctx).await.unwrap_err();
        assert_eq!(error.to_string(), "SYNTHETIC_ROUND_SCAN");
        let events = paper::recorded_events();
        assert_eq!(events.len(), if legacy { 0 } else { selected });
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["event_kind"], "summary");
            assert_eq!(event["market_id"], before_ids[index]);
            let decision = &event["source"]["decision"];
            let expected = if stopped {
                "RUN_STOPPED"
            } else if entered && index == 0 {
                "ALREADY_ENTERED"
            } else {
                "MISSING_INPUT"
            };
            assert_eq!(decision["reason"], expected);
            assert!(decision["candidate_id"].is_null());
            assert!(decision["spot_clock"].is_null());
            assert!(decision["book_clock"].is_null());
            assert_eq!(event["accounting"]["cash"], 100.0);
            assert_eq!(event["accounting"]["reserved"], 0.0);
        }
        assert!(bot.last_scan.is_none());
        assert_eq!(
            bot.rounds
                .iter()
                .map(|market| market.id)
                .collect::<Vec<_>>(),
            before_ids
        );
        assert!(ctx.broker.holdings().is_empty());
        assert_eq!(ctx.broker.paper_snapshot().unwrap().cash, 100.0);
    }
}

/// Real acquisition consumers fail before HTTP, then still emit selected attempts.
#[tokio::test]
async fn acquisition_failures_keep_selected_attempts_without_network() {
    use crate::bot::runtime::{Ctx, Strategy};
    use crate::service::predict_fun::PredictFunClient;
    use crate::service::venue::{MarketData, MarketQuery};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct AcquisitionClock {
        now: CheckTime,
        calls: AtomicUsize,
    }
    impl Clock for AcquisitionClock {
        fn now(&self) -> anyhow::Result<CheckTime> {
            // scan_at succeeds; both source request clocks fail before send().
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 1 || call == 2 {
                anyhow::bail!("SYNTHETIC_ACQUISITION_CLOCK");
            }
            Ok(self.now)
        }
    }
    struct Books {
        observed: ObservedBook,
        failed: bool,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl MarketData for Books {
        async fn markets(&self, _: &MarketQuery) -> anyhow::Result<Vec<TradableMarket>> {
            panic!("RESCAN_FORBIDDEN")
        }
        async fn market(&self, _: u64) -> anyhow::Result<TradableMarket> {
            panic!("UNTRACKED_MARKET_FORBIDDEN")
        }
        async fn with_schedule(&self, _: &TradableMarket) -> anyhow::Result<TradableMarket> {
            panic!("SCHEDULE_FORBIDDEN")
        }
        async fn books(&self, _: &[u64]) -> anyhow::Result<HashMap<u64, BookSnapshot>> {
            panic!("UNCLOCKED_BOOK_FORBIDDEN")
        }
        async fn observed_books(
            &self,
            ids: &[u64],
            _: &dyn Clock,
        ) -> anyhow::Result<HashMap<u64, ObservedBook>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(ids, &[1, 2]);
            if self.failed {
                anyhow::bail!("SYNTHETIC_BOOK_FAILURE");
            }
            Ok(ids.iter().map(|id| (*id, self.observed.clone())).collect())
        }
    }
    for fault in ["spot", "book", "entered", "stopped", "conflict"] {
        paper::clear_recorded_events();
        let (mut bot, broker, _, _, observed) = forward_fixture();
        let mut second = bot.rounds[0].clone();
        second.id = 2;
        second.yes_token = "1003".into();
        second.no_token = "1004".into();
        bot.rounds.push(second);
        bot.cfg.symbols = vec!["BTCUSDT".into()];
        bot.last_scan = Some(std::time::Instant::now());
        bot.known.remember(&bot.rounds);
        if fault == "entered" || fault == "stopped" {
            bot.core.confirm_fill(1, 1.0);
        }
        if fault == "stopped" {
            broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
        }
        if fault == "conflict" {
            let mut changed = bot.rounds[0].clone();
            changed.start_price = Some(102.0);
            bot.availability
                .get_mut(&1)
                .unwrap()
                .record(&changed, start());
        }
        let clock = Arc::new(AcquisitionClock {
            now: CheckTime {
                utc: start() + chrono::Duration::seconds(101),
                elapsed_ms: 101_000,
            },
            calls: AtomicUsize::new(0),
        });
        let broker = broker
            .with_phase1(clock.clone(), StrategyContract::Phase1NetShares, 0.03, 30.0)
            .unwrap();
        let data = Arc::new(Books {
            observed: observed.clone(),
            failed: fault == "book",
            calls: AtomicUsize::new(0),
        });
        let mut cfg: crate::config::AppConfig = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tools/predictfun/btc5m-controlled-paper.json"
        )))
        .unwrap();
        cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
        let client = PredictFunClient::new(&cfg).unwrap();
        let ctx = Ctx {
            cfg,
            client,
            data: data.clone(),
            broker: Arc::new(broker),
        };
        assert_eq!(
            bot.tick(&ctx).await.unwrap_err().to_string(),
            "DATA_INCOMPLETE"
        );
        assert_eq!(clock.calls.load(Ordering::SeqCst), 5);
        assert_eq!(data.calls.load(Ordering::SeqCst), 1);
        let events = paper::recorded_events();
        assert_eq!(events.len(), 2);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["event_kind"], "summary");
            assert_eq!(event["event_seq"], index as u64 + 1);
            assert_eq!(event["market_id"], index as u64 + 1);
            let decision = &event["source"]["decision"];
            let reason = match fault {
                "stopped" => "RUN_STOPPED",
                "entered" if index == 0 => "ALREADY_ENTERED",
                "conflict" if index == 0 => "MARKET_INVALID",
                _ => "MISSING_INPUT",
            };
            assert_eq!(decision["reason"], reason);
            assert_eq!(decision["source_state"], "UNAVAILABLE");
            for field in [
                "candidate_id",
                "selected_token",
                "prediction_source",
                "actual_pair",
                "spot",
                "spot_clock",
                "p_up",
                "sigma",
            ] {
                assert!(decision[field].is_null(), "{fault}/{field}");
            }
            if fault == "book" {
                assert!(decision["book_clock"].is_null());
                assert!(decision["best_yes_bid"].is_null());
            } else {
                assert_eq!(
                    decision["book_clock"],
                    serde_json::to_value(&observed.clock).unwrap()
                );
                assert_eq!(decision["best_yes_bid"], observed.book.bids[0].price);
            }
            assert!(decision["up"]["fair"].is_null());
            assert!(decision["down"]["fair"].is_null());
            assert_eq!(event["accounting"]["cash"], 100.0);
            assert_eq!(event["accounting"]["reserved"], 0.0);
        }
        assert!(ctx.broker.holdings().is_empty());
        assert!(!bot.core.is_entered(2));
    }
    paper::clear_recorded_events();
}

#[tokio::test]
async fn phase1_protocol_consumer_fixture() {
    paper::clear_recorded_events();
    let (mut bot, broker, clock, quote, observed) = forward_fixture();
    let market = bot.rounds[0].clone();
    let quotes = [("BTCUSDT".into(), quote)].into_iter().collect();
    let books = [(market.id, observed.clone())].into_iter().collect();
    broker.observe(&market, &observed.book);
    bot.trade_phase1(&broker, &quotes, &books, clock.as_ref())
        .await
        .unwrap();
    assert_eq!(
        paper::recorded_events()[0]["source"]["decision"]["reason"],
        "WARMUP"
    );
    warm(&mut bot.core, "BTCUSDT", 21);
    bot.trade_phase1(&broker, &quotes, &books, clock.as_ref())
        .await
        .unwrap();
    assert!(bot.core.is_entered(market.id));
    assert!((broker.paper_snapshot().unwrap().cash - 98.002).abs() < 1e-9);
    bot.trade_phase1(&broker, &quotes, &books, clock.as_ref())
        .await
        .unwrap();
    assert_eq!(
        paper::recorded_events().last().unwrap()["source"]["decision"]["reason"],
        "ALREADY_ENTERED"
    );
    let mut result = market.clone();
    result.open = false;
    result.end_price = Some(101.0);
    result.payout_yes = Some(1.0);
    let facts = result.round_facts.as_mut().unwrap();
    facts.oracle_status = Some("RESOLVED".into());
    facts.trading_status = Some("CLOSED".into());
    facts.yes_status = Some("WON".into());
    facts.no_status = Some("LOST".into());
    broker.controlled_settle(&result);
    broker.controlled_settle(&result);
    paper::emit_final(&broker, false);
    let events = paper::recorded_events();
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["schema_version"], paper::SCHEMA_VERSION_V2);
        assert_eq!(event["event_seq"], (index + 1) as u64);
    }
    let kinds: Vec<_> = events
        .iter()
        .map(|event| event["event_kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "summary",
            "summary",
            "summary",
            "reservation",
            "summary",
            "fill",
            "summary",
            "settlement",
            "final"
        ]
    );
    let apply = &events[4];
    assert_eq!(apply["source"]["decision"]["phase"], "PRE_APPLY");
    assert_eq!(apply["accounting"]["cash"], 100.0);
    assert_eq!(
        events[7]["source"]["candidate_id"],
        events[5]["source"]["candidate_id"]
    );
    assert_eq!(events[8]["source"]["finalized"], true);
    assert!(broker.holdings().is_empty());
    assert!(bot.core.is_entered(market.id));
}

#[test]
fn broker_checks_full_inputs_revision_and_terminal_reuse() {
    for fault in ["book", "fee", "strike", "source", "refresh", "reuse"] {
        paper::clear_recorded_events();
        let (mut bot, broker, clock, quote, observed) = forward_fixture();
        warm(&mut bot.core, "BTCUSDT", 21);
        let mut market = bot.rounds[0].clone();
        let mut observed = observed;
        let mut input = inputs(&market, &observed.book);
        input.spot_price = quote.price;
        input.spot_clock = Some(quote.clock.clone());
        input.book_clock = Some(observed.clock.clone());
        input.now = clock.now().unwrap();
        let evaluation = bot.core.evaluate_phase1(&input);
        let candidate = evaluation.candidate.expect("a warm candidate");
        broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate));
        match fault {
            "book" => observed.book.bids[0].size += 1.0,
            "fee" => market.fee_rate_bps += 1,
            "strike" => market.start_price = Some(101.0),
            "source" => {
                let at = self::clock(start(), 101_000, 101_100);
                bot.core
                    .observe_phase1(
                        "BTCUSDT",
                        100.0,
                        SpotSource::Coinbase,
                        "BTC-USD",
                        &at,
                        CheckTime {
                            utc: at.received_at,
                            elapsed_ms: at.received_elapsed_ms,
                        },
                    )
                    .unwrap();
            }
            "refresh" => bot.core.refresh_phase1_market(market.id).unwrap(),
            _ => (),
        }
        broker.observe(&market, &observed.book);
        let result = broker
            .take_candidate(&market, &observed.book, &candidate)
            .unwrap();
        if fault == "reuse" {
            assert!(matches!(result, TakeResult::Filled { .. }));
            let before = broker.paper_snapshot().unwrap();
            let count = paper::recorded_events().len();
            assert!(broker
                .take_candidate(&market, &observed.book, &candidate)
                .is_err());
            assert_eq!(before, broker.paper_snapshot().unwrap());
            assert_eq!(count, paper::recorded_events().len());
        } else {
            assert!(matches!(result, TakeResult::Blocked(_)), "{fault}");
            assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
            assert_eq!(broker.paper_snapshot().unwrap().reserved, 0.0);
            assert_eq!(
                paper::recorded_events().last().unwrap()["source"]["diagnostic_reason"],
                "INPUT_CHANGED",
                "{fault}"
            );
        }
    }
}
#[test]
fn fee_counterexample_and_exact_threshold_use_the_real_core_on_both_sides() {
    for down in [false, true] {
        let mut market = market(200);
        let mut depth = book();
        depth.bids[0].price = if down { 0.6 } else { 0.39 };
        depth.asks[0].price = if down { 0.61 } else { 0.4 };
        let evaluate = |price| {
            let mut core = DecisionCore::phase1(Default::default());
            warm(&mut core, "BTCUSDT", 21);
            let received = clock(start(), 105_000, 105_100);
            let now = CheckTime {
                utc: start() + chrono::Duration::seconds(106),
                elapsed_ms: 106_000,
            };
            core.observe_phase1(
                "BTCUSDT",
                price,
                SpotSource::Binance,
                "BTCUSDT",
                &received,
                now,
            )
            .unwrap();
            let mut input = inputs(&market, &depth);
            input.spot_price = price;
            input.spot_clock = Some(received.clone());
            input.book_clock = Some(received);
            input.now = now;
            let result = core.evaluate_phase1(&input);
            (core, result)
        };
        let target = if down { 1.0 - 0.4381 } else { 0.4381 };
        let (mut low, mut high) = (99.0, 101.0);
        for _ in 0..60 {
            let mid = (low + high) / 2.0;
            if evaluate(mid).1.diagnostic.p_up.unwrap() < target {
                low = mid;
            } else {
                high = mid;
            }
        }
        let price = (low + high) / 2.0;
        let (core, evaluated) = evaluate(price);
        let side = if down {
            evaluated.diagnostic.down.as_ref().unwrap()
        } else {
            evaluated.diagnostic.up.as_ref().unwrap()
        };
        assert!((side.fair.unwrap() - 0.4381).abs() < 1e-9);
        assert!((side.edge.unwrap() - 0.029338).abs() < 1e-9);
        assert_eq!(evaluated.diagnostic.reason, Reason::EdgeBelowMin);
        assert!(evaluated.candidate.is_none());
        let legacy = super::super::round_entry(
            &market,
            &depth,
            price,
            core.phase1_sigma("BTCUSDT", VOL_FLOOR).unwrap(),
            &crate::config::BtcArbConfig {
                order_usd: 2.0,
                ..Default::default()
            },
            start() + chrono::Duration::seconds(106),
        );
        assert!(
            legacy.is_some(),
            "old cash-fee gate passes this same valuation"
        );
        // Compare an exactly representable computed edge and its next f64,
        // without an epsilon or display rounding in the production comparison.
        market.fee_rate_bps = 0;
        let mut exact_core = DecisionCore::phase1(Default::default());
        warm(&mut exact_core, "BTCUSDT", 21);
        let mut input = inputs(&market, &depth);
        input.spot_price = 100.2;
        input.spot_clock = Some(clock_at_latest());
        input.book_clock = Some(clock_at_latest());
        input.now = CheckTime {
            utc: start() + chrono::Duration::seconds(101),
            elapsed_ms: 101_000,
        };
        input.min_edge = 0.0;
        let candidate = exact_core.evaluate_phase1(&input).candidate.unwrap();
        let edge = recheck(&candidate, input.now, 0.0, 30.0).unwrap().edge;
        assert!(recheck(&candidate, input.now, edge, 30.0).is_ok());
        assert_eq!(
            recheck(
                &candidate,
                input.now,
                f64::from_bits(edge.to_bits() + 1),
                30.0
            )
            .unwrap_err(),
            Reason::EdgeBelowMin
        );
    }
}

/// A book whose selected side can be bought at 0.4, on either side.
fn counterexample_book(down: bool) -> BookSnapshot {
    let mut depth = book();
    depth.bids[0].price = if down { 0.6 } else { 0.39 };
    depth.asks[0].price = if down { 0.61 } else { 0.4 };
    depth
}

/// The spot price at which the model values the selected side at `target`.
///
/// The search runs the real core on the real book, so the price it finds is
/// the price the consumer will see. `target` is the model's own probability
/// of the Up side.
fn price_for_fair(market: &TradableMarket, depth: &BookSnapshot, target: f64) -> f64 {
    let evaluate = |price: f64| {
        let mut core = DecisionCore::phase1(Default::default());
        warm(&mut core, "BTCUSDT", 21);
        let received = clock(start(), 105_000, 105_100);
        let now = CheckTime {
            utc: start() + chrono::Duration::seconds(106),
            elapsed_ms: 106_000,
        };
        core.observe_phase1(
            "BTCUSDT",
            price,
            SpotSource::Binance,
            "BTCUSDT",
            &received,
            now,
        )
        .unwrap();
        let mut input = inputs(market, depth);
        input.spot_price = price;
        input.spot_clock = Some(received.clone());
        input.book_clock = Some(received);
        input.now = now;
        let result = core.evaluate_phase1(&input);
        (core, result)
    };
    let mut low = 99.0;
    let mut high = 101.0;
    for _ in 0..60 {
        let mid = (low + high) / 2.0;
        if evaluate(mid).1.diagnostic.p_up.unwrap() < target {
            low = mid;
        } else {
            high = mid;
        }
    }
    (low + high) / 2.0
}

/// The forward fixture at the moment the search used, with the chosen book
/// and price: the consumer sees exactly the valuation the search found.
fn consumer_fixture(
    depth: BookSnapshot,
    price: f64,
) -> (RoundTaker, Broker, Arc<FixtureClock>, ObservedSpot, ObservedBook) {
    let (mut bot, broker, run_clock, mut quote, mut observed) = forward_fixture();
    run_clock.advance(5_000);
    quote.price = price;
    quote.clock = clock(start(), 105_000, 105_100);
    observed.book = depth;
    observed.clock = clock(start(), 105_000, 105_100);
    warm(&mut bot.core, "BTCUSDT", 21);
    bot.core
        .observe_phase1(
            "BTCUSDT",
            price,
            SpotSource::Binance,
            "BTCUSDT",
            &quote.clock,
            CheckTime {
                utc: start() + chrono::Duration::seconds(106),
                elapsed_ms: 106_000,
            },
        )
        .unwrap();
    (bot, broker, run_clock, quote, observed)
}

/// The fee counterexample must reach the real consumer.
///
/// The same valuation passes the old cash-fee gate and is refused by the
/// phase1 net-share path: the refusal happens in the broker round trip, with
/// no fill, no entry and no money moved.
#[tokio::test]
async fn the_fee_counterexample_is_refused_by_the_real_consumer() {
    for down in [false, true] {
        paper::clear_recorded_events();
        let depth = counterexample_book(down);
        let market = market(200);
        let target = if down { 1.0 - 0.4381 } else { 0.4381 };
        let price = price_for_fair(&market, &depth, target);
        let (mut bot, broker, run_clock, quote, observed) = consumer_fixture(depth.clone(), price);
        // The old cash-fee gate passes this same valuation.
        assert!(
            super::super::round_entry(
                &market,
                &depth,
                price,
                bot.core.phase1_sigma("BTCUSDT", VOL_FLOOR).unwrap(),
                &bot.cfg,
                start() + chrono::Duration::seconds(106),
            )
            .is_some(),
            "down={down}: the old cash-fee gate passes this valuation"
        );
        broker.observe(&bot.rounds[0], &observed.book);
        bot.trade_phase1(
            &broker,
            &[("BTCUSDT".into(), quote)].into_iter().collect(),
            &[(bot.rounds[0].id, observed)].into_iter().collect(),
            run_clock.as_ref(),
        )
        .await
        .unwrap();
        let events = paper::recorded_events();
        assert_eq!(events.len(), 1, "down={down}");
        assert_eq!(events[0]["event_kind"], "summary");
        let decision = &events[0]["source"]["decision"];
        assert_eq!(decision["reason"], "EDGE_BELOW_MIN", "down={down}");
        assert!(decision["candidate_id"].is_null(), "down={down}");
        let side = if down {
            &decision["down"]
        } else {
            &decision["up"]
        };
        // The frozen numbers: the side is worth 0.4381 and its net edge is
        // 0.029338, which is below the 0.03 the contract fixes.
        assert!(
            (side["fair"].as_f64().unwrap() - 0.4381).abs() < 1e-9,
            "down={down}: {:?}",
            side["fair"]
        );
        assert!(
            (side["edge"].as_f64().unwrap() - 0.029338).abs() < 1e-9,
            "down={down}: {:?}",
            side["edge"]
        );
        assert_eq!(side["qualifies"], false, "down={down}");
        assert!(
            !events.iter().any(|event| event["event_kind"] == "fill"),
            "down={down}: the counterexample never fills"
        );
        let snapshot = broker.paper_snapshot().unwrap();
        assert_eq!(snapshot.cash, 100.0, "down={down}");
        assert_eq!(snapshot.reserved, 0.0, "down={down}");
        assert!(broker.holdings().is_empty(), "down={down}");
        assert!(!bot.core.is_entered(bot.rounds[0].id), "down={down}");
    }
    paper::clear_recorded_events();
}

/// The fixed threshold, on the real f64 values the core computes.
///
/// The spot price is searched until the core's own verdict changes. The two
/// prices that straddle it are neighbouring floats, so the pair shows the
/// threshold exactly: no epsilon and no display rounding is involved.
#[test]
fn the_fixed_threshold_accepts_exactly_its_boundary() {
    // A zero fee is an explicit fact, not a missing one: the edge is then the
    // model's fair value minus the touch.
    let market = market(0);
    let depth = book();
    let evaluate = |price: f64| {
        let mut core = DecisionCore::phase1(Default::default());
        warm(&mut core, "BTCUSDT", 21);
        let received = clock_at_latest();
        let now = CheckTime {
            utc: start() + chrono::Duration::milliseconds(101_000),
            elapsed_ms: 101_000,
        };
        let mut input = inputs(&market, &depth);
        input.spot_price = price;
        input.spot_clock = Some(received.clone());
        input.book_clock = Some(received);
        input.now = now;
        let evaluation = core.evaluate_phase1(&input);
        (evaluation.diagnostic, evaluation.candidate)
    };
    // The Up side is worth buying once the model's fair value clears the
    // touch by the threshold: 0.45 + 0.03.
    let edge_of = |price: f64| {
        let (diagnostic, candidate) = evaluate(price);
        let up = diagnostic.up.as_ref().expect("the up side is valued");
        (up.edge.unwrap(), up.roi.unwrap(), diagnostic, candidate)
    };
    let mut low = 99.0;
    let mut high = 100.0;
    for _ in 0..100 {
        let mid = (low + high) / 2.0;
        if edge_of(mid).0 >= 0.03 {
            high = mid;
        } else {
            low = mid;
        }
    }
    let rejected = low;
    let accepted = high;
    assert!(
        f64::from_bits(rejected.to_bits() + 1) == accepted,
        "the two prices are neighbouring floats"
    );
    let (rejected_edge, rejected_roi, rejected_diagnostic, rejected_candidate) =
        edge_of(rejected);
    let (accepted_edge, _accepted_roi, accepted_diagnostic, accepted_candidate) = edge_of(accepted);
    // The threshold is exact: the lower neighbour is refused, the upper one
    // is accepted, and nothing in between exists.
    assert!(rejected_edge < 0.03, "{rejected_edge}");
    assert!(accepted_edge >= 0.03, "{accepted_edge}");
    assert!(0.03 - rejected_edge < 1e-9, "{rejected_edge}");
    assert!(accepted_edge - 0.03 < 1e-9, "{accepted_edge}");
    assert_eq!(rejected_diagnostic.reason, Reason::EdgeBelowMin);
    assert!(rejected_candidate.is_none());
    assert_eq!(accepted_diagnostic.reason, Reason::Eligible);
    assert!(accepted_candidate.is_some());
    // The return on the cost is not the gate: the refused price has the
    // better ratio and is still refused.
    assert!(
        rejected_roi > 0.03,
        "the roi does not replace the edge: {rejected_roi}"
    );
    // The side with the larger edge is the one that is bought.
    let up = accepted_diagnostic.up.as_ref().unwrap();
    let down = accepted_diagnostic.down.as_ref().unwrap();
    assert!(up.edge.unwrap() > down.edge.unwrap());
    assert_eq!(accepted_diagnostic.selected_token.as_deref(), Some("yes-1"));
}

/// The side the round is worth buying, on the production choice itself.
///
/// An exact tie keeps the old iteration order, which is the Down side.
#[test]
fn the_larger_edge_wins_and_an_exact_tie_is_the_down_side() {
    let side = |token: &str, edge: f64| SideDiagnostic {
        token: token.into(),
        fair: Some(0.5),
        limit_price: Some(0.45),
        gross_shares: Some(4.0),
        fee_per_gross_share: Some(0.0),
        net_shares: Some(4.0),
        cost: Some(1.8),
        ev: Some(edge * 4.0),
        edge: Some(edge),
        roi: Some(edge),
        breakeven_fair: Some(0.45),
        qualifies: Some(edge >= 0.03),
        depth_levels: None,
        depth_complete: None,
        walk_vwap: None,
        execution_net_shares: None,
        execution_cost: None,
        execution_ev: None,
        execution_edge: None,
    };
    let up = side("up", 0.05);
    let down = side("down", 0.04);
    // The first argument is the Up side, the second the Down side.
    assert_eq!(choose_side(&up, &down).edge, Some(0.05));
    assert_eq!(choose_side(&down, &up).edge, Some(0.05));
    assert_eq!(choose_side(&up, &down).token, "up");
    // An exact tie keeps the down side, which is the second argument.
    let tie = side("down", 0.05);
    assert_eq!(choose_side(&up, &tie).token, "down");
    assert_eq!(
        choose_side(&up, &tie),
        &tie,
        "an exact tie keeps the down side"
    );
}

/// The thirty second boundary, in the broker's own two checks.
///
/// The same candidate is checked twice: before the reservation and again
/// before the accounting. Exactly thirty seconds passes in both; one
/// millisecond less is refused by the check that runs.
#[tokio::test]
async fn the_thirty_second_boundary_is_checked_at_submit_and_at_apply() {
    for stage in ["submit-equal", "submit-late", "apply-equal", "apply-late"] {
        paper::clear_recorded_events();
        let (mut bot, broker, _run_clock, mut quote, mut observed) = forward_fixture();
        let mut market = bot.rounds[0].clone();
        // The round ends 131 seconds after the fixture starts, so the thirty
        // second boundary falls exactly on the decision at 101 seconds.
        market.ends_at = Some(start() + chrono::Duration::milliseconds(131_000));
        market.starts_at = Some(market.ends_at.unwrap() - chrono::Duration::seconds(300));
        bot.rounds[0] = market.clone();
        warm(&mut bot.core, "BTCUSDT", 21);
        quote.clock = clock_at_latest();
        observed.clock = quote.clock.clone();
        let decision = CheckTime {
            utc: start() + chrono::Duration::milliseconds(101_000),
            elapsed_ms: 101_000,
        };
        let mut input = inputs(&market, &observed.book);
        input.spot_price = quote.price;
        input.spot_clock = Some(quote.clock.clone());
        input.book_clock = Some(observed.clock.clone());
        input.now = decision;
        let evaluation = bot.core.evaluate_phase1(&input);
        assert_eq!(evaluation.diagnostic.reason, Reason::Eligible, "{stage}");
        let candidate = evaluation.candidate.expect("a warm candidate");
        let reading = |millis: u64| CheckTime {
            utc: start() + chrono::Duration::milliseconds(millis as i64),
            elapsed_ms: millis,
        };
        // 131000 - 30000 = 101000: exactly thirty seconds left.
        let equal = reading(101_000);
        let late = reading(101_001);
        let readings = match stage {
            // The submit check takes the first reading, the apply check the
            // second and the third.
            "submit-equal" => vec![equal, equal, equal],
            "submit-late" => vec![late, late, late],
            "apply-equal" => vec![equal, equal, equal],
            _ => vec![equal, late, late],
        };
        let broker = broker
            .with_phase1(
                Arc::new(ScriptedClock::new(readings)),
                StrategyContract::Phase1NetShares,
                0.03,
                30.0,
            )
            .unwrap();
        broker.observe(&market, &observed.book);
        assert!(broker.emit_phase1_evaluation(
            &market,
            &evaluation.diagnostic,
            Some(&candidate)
        ));
        let result = broker
            .take_candidate(&market, &observed.book, &candidate)
            .unwrap();
        let events = paper::recorded_events();
        let checks: Vec<_> = events
            .iter()
            .filter_map(|event| event["source"].get("decision"))
            .collect();
        let refused = stage.ends_with("late");
        if refused {
            assert!(matches!(result, TakeResult::Blocked(_)), "{stage}");
            let last = checks.last().unwrap();
            assert_eq!(last["reason"], "ROUND_TOO_LATE", "{stage}");
            assert_eq!(
                last["phase"],
                if stage == "submit-late" {
                    "PRE_SUBMIT"
                } else {
                    "PRE_APPLY"
                },
                "{stage}"
            );
            // The reservation only exists when the refusal came after it.
            let reserved = usize::from(stage == "apply-late");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["event_kind"] == "reservation")
                    .count(),
                reserved,
                "{stage}"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["event_kind"] == "release")
                    .count(),
                reserved,
                "{stage}"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["event_kind"] == "fill")
                    .count(),
                0,
                "{stage}"
            );
            assert!(broker.holdings().is_empty(), "{stage}");
            assert!(!bot.core.is_entered(market.id), "{stage}");
            let snapshot = broker.paper_snapshot().unwrap();
            assert_eq!(snapshot.cash, 100.0, "{stage}");
            assert_eq!(snapshot.reserved, 0.0, "{stage}");
        } else {
            assert!(
                matches!(result, TakeResult::Filled { .. }),
                "{stage}: {result:?} {events:?}"
            );
            // The check that passed saw exactly thirty seconds left.
            let submit = checks
                .iter()
                .find(|check| check["phase"] == "PRE_SUBMIT")
                .expect("the submit check");
            assert_eq!(submit["reason"], "ELIGIBLE", "{stage}");
            assert_eq!(submit["secs_left"], 30.0, "{stage}");
            let apply = checks
                .iter()
                .find(|check| check["phase"] == "PRE_APPLY")
                .expect("the apply check");
            assert_eq!(apply["reason"], "ELIGIBLE", "{stage}");
            assert_eq!(apply["secs_left"], 30.0, "{stage}");
        }
    }
    paper::clear_recorded_events();
}

/// The old six hundred second probe also starts a new epoch.
///
/// Six hundred seconds is the old probe, not a new configuration: the
/// threshold that is in force is the three hundred second one.
#[test]
fn a_six_hundred_second_gap_starts_a_new_epoch() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    let last = 20 * 5_000 + 100;
    let quote = clock(start(), last + 600_000, last + 600_100);
    let now = CheckTime {
        utc: quote.received_at + chrono::Duration::milliseconds(50),
        elapsed_ms: quote.received_elapsed_ms + 50,
    };
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Binance,
        "BTCUSDT",
        &quote,
        now,
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(2));
    assert_eq!(core.reset_reason("BTCUSDT"), ResetReason::LongGap);
    assert_eq!(core.samples("BTCUSDT"), 0);
}

/// A reset epoch warms up again: nineteen returns are not enough.
///
/// The first price after a reset is only a price; the estimate needs twenty
/// returns before it may be used.
#[test]
fn a_reset_epoch_needs_twenty_returns_before_it_may_be_used() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    // A source switch starts a new epoch with no warm state.
    let switched = clock(start(), 120_000, 120_100);
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Coinbase,
        "BTC-USD",
        &switched,
        CheckTime {
            utc: switched.received_at,
            elapsed_ms: switched.received_elapsed_ms,
        },
    )
    .unwrap();
    assert_eq!(core.samples("BTCUSDT"), 0);
    let market = market(200);
    let depth = book();
    let mut last_clock = switched.clone();
    let mut reason = Reason::Warmup;
    for index in 0..20 {
        let quote = clock(start(), 130_000 + index * 5_000, 130_100 + index * 5_000);
        let now = CheckTime {
            utc: quote.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: quote.received_elapsed_ms + 50,
        };
        core.observe_phase1(
            "BTCUSDT",
            100.0 + index as f64 * 0.01,
            SpotSource::Coinbase,
            "BTC-USD",
            &quote,
            now,
        )
        .unwrap();
        last_clock = quote;
        let mut input = inputs(&market, &depth);
        input.spot_price = 100.0 + index as f64 * 0.01;
        input.spot_source = Some(SpotSource::Coinbase);
        input.actual_pair = Some("BTC-USD".into());
        input.spot_clock = Some(last_clock.clone());
        input.book_clock = Some(last_clock.clone());
        input.now = CheckTime {
            utc: last_clock.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: last_clock.received_elapsed_ms + 50,
        };
        let evaluation = core.evaluate_phase1(&input);
        reason = evaluation.diagnostic.reason;
        // The switch's own price is the first price of the epoch: the price
        // after it is the first return.
        assert_eq!(evaluation.diagnostic.samples, index as u32 + 1);
        if index < 19 {
            assert_eq!(reason, Reason::Warmup, "nineteen returns: {index}");
        }
    }
    assert_ne!(reason, Reason::Warmup, "twenty returns may be used");
    assert_eq!(core.samples("BTCUSDT"), 20);
}

/// Switching back to a source does not restore its old warm state.
///
/// The epoch is new even though the source is one the run has used before,
/// and the estimate has to warm up again.
#[test]
fn switching_back_to_a_used_source_does_not_restore_the_warm_state() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    assert_eq!(core.samples("BTCUSDT"), 20);
    let coinbase = clock(start(), 120_000, 120_100);
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Coinbase,
        "BTC-USD",
        &coinbase,
        CheckTime {
            utc: coinbase.received_at,
            elapsed_ms: coinbase.received_elapsed_ms,
        },
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(2));
    assert_eq!(core.samples("BTCUSDT"), 0);
    // Back to the primary source, a short gap later: a new epoch again.
    let back = clock(start(), 125_000, 125_100);
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Binance,
        "BTCUSDT",
        &back,
        CheckTime {
            utc: back.received_at,
            elapsed_ms: back.received_elapsed_ms,
        },
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(3));
    assert_eq!(core.reset_reason("BTCUSDT"), ResetReason::SourceSwitch);
    assert_eq!(core.samples("BTCUSDT"), 0);
}

/// A new round does not cost the run its estimate.
///
/// The volatility belongs to the symbol, not to one round: another round is
/// valued with the estimate the run already has.
#[test]
fn a_new_round_keeps_the_volatility_estimate() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    let first = market(200);
    let depth = book();
    let mut input = inputs(&first, &depth);
    input.spot_price = 100.2;
    input.spot_clock = Some(clock_at_latest());
    input.book_clock = Some(clock_at_latest());
    input.now = CheckTime {
        utc: start() + chrono::Duration::milliseconds(101_000),
        elapsed_ms: 101_000,
    };
    let evaluation = core.evaluate_phase1(&input);
    assert_eq!(evaluation.diagnostic.samples, 20);
    // Another round, another strike: the same estimate is used.
    let mut second = market(200);
    second.id = 2;
    second.yes_token = "2001".into();
    second.no_token = "2002".into();
    second.start_price = Some(100.5);
    let mut input = inputs(&second, &depth);
    input.spot_price = 100.2;
    input.spot_clock = Some(clock_at_latest());
    input.book_clock = Some(clock_at_latest());
    input.now = CheckTime {
        utc: start() + chrono::Duration::milliseconds(101_000),
        elapsed_ms: 101_000,
    };
    let evaluation = core.evaluate_phase1(&input);
    assert_eq!(evaluation.diagnostic.samples, 20);
    assert_eq!(evaluation.diagnostic.source_epoch, Some(1));
    assert_eq!(core.samples("BTCUSDT"), 20);
    assert_eq!(core.epoch("BTCUSDT"), Some(1));
}

#[tokio::test]
async fn selected_missing_stopped_and_conflicting_facts_still_leave_attempts() {
    for fault in ["missing", "stopped", "conflict"] {
        paper::clear_recorded_events();
        let (mut bot, broker, clock, quote, observed) = forward_fixture();
        if fault == "stopped" {
            broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
        }
        if fault == "conflict" {
            let mut changed = bot.rounds[0].clone();
            changed.start_price = Some(102.0);
            bot.availability
                .get_mut(&changed.id)
                .unwrap()
                .record(&changed, start() + chrono::Duration::seconds(1));
        }
        let quotes = if fault == "missing" {
            HashMap::new()
        } else {
            [("BTCUSDT".into(), quote)].into_iter().collect()
        };
        let books = [(1, observed)].into_iter().collect();
        bot.trade_phase1(&broker, &quotes, &books, clock.as_ref())
            .await
            .unwrap();
        let events = paper::recorded_events();
        assert_eq!(events.len(), 1);
        let expected = match fault {
            "missing" => "MISSING_INPUT",
            "stopped" => "RUN_STOPPED",
            _ => "MARKET_INVALID",
        };
        assert_eq!(events[0]["source"]["decision"]["reason"], expected);
        assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
    }
}

#[tokio::test]
async fn required_depth_limits_refuse_without_truncating_or_filling() {
    for count in [1000, 1001, 1800] {
        paper::clear_recorded_events();
        let (mut bot, broker, clock, quote, mut observed) = forward_fixture();
        warm(&mut bot.core, "BTCUSDT", 21);
        let q = shares_for(2.0, observed.book.asks[0].price);
        observed.book.asks = (0..count)
            .map(|_| BookLevel {
                price: 0.45,
                size: q / (count as f64 - 0.5),
            })
            .collect();
        broker.observe(&bot.rounds[0], &observed.book);
        bot.trade_phase1(
            &broker,
            &[("BTCUSDT".into(), quote)].into_iter().collect(),
            &[(1, observed)].into_iter().collect(),
            clock.as_ref(),
        )
        .await
        .unwrap();
        let events = paper::recorded_events();
        if count == 1000 {
            assert!(bot.core.is_entered(1));
            assert_eq!(
                events[0]["source"]["decision"]["up"]["depth_levels"]
                    .as_array()
                    .unwrap()
                    .len(),
                1000
            );
        } else {
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0]["source"]["decision"]["reason"],
                "DIAGNOSTIC_LIMIT"
            );
            assert!(events[0]["source"]["decision"]["up"]["depth_levels"].is_null());
            assert!(!bot.core.is_entered(1));
            assert_eq!(broker.paper_snapshot().unwrap().cash, 100.0);
        }
    }
}

/// Compare the real forward consumer and original Driver, then send the
/// Driver's actual candidate through the same Broker implementation.
#[tokio::test]
async fn phase1_forward_replay_broker_parity() {
    use super::super::replay::{self, EventEnvelopeV2, FixtureDriver, ReplayConfig, ReplayMode};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DelayedClock {
        now: CheckTime,
        jump_on: usize,
        calls: AtomicUsize,
    }
    impl Clock for DelayedClock {
        fn now(&self) -> anyhow::Result<CheckTime> {
            let delayed = self.calls.fetch_add(1, Ordering::SeqCst) >= self.jump_on;
            let delay = if delayed { 2_000 } else { 0 };
            Ok(CheckTime {
                utc: self.now.utc + chrono::Duration::milliseconds(delay),
                elapsed_ms: self.now.elapsed_ms + delay as u64,
            })
        }
    }
    fn wire_clock(clock: &ObservationClock) -> Value {
        serde_json::to_value(clock).unwrap()
    }
    fn step(
        driver: &mut FixtureDriver,
        summary: &mut replay::ReplaySummary,
        seq: &mut u64,
        at: CheckTime,
        payload: Value,
    ) -> (Option<DecisionDiagnostic>, Value) {
        let source_at = payload
            .get("clock")
            .and_then(|value| value.get("source_at"))
            .and_then(Value::as_str)
            .map(|value| {
                DateTime::parse_from_rfc3339(value)
                    .unwrap()
                    .timestamp_millis()
            });
        let wire = json!({"schema_version": replay::SCHEMA_VERSION_V2,
            "event_id": format!("event-{}", *seq), "event_at_ms": source_at,
            "available_at_ms": at.utc.timestamp_millis(), "elapsed_ms": at.elapsed_ms,
            "seq": *seq, "source": "offline-fixture", "evidence_type": "fixture",
            "payload": payload});
        let event: EventEnvelopeV2 = serde_json::from_value(wire).unwrap();
        *seq += 1;
        let row = driver
            .step(event.normalize().unwrap(), *seq, summary)
            .unwrap()
            .unwrap();
        (row.decision, serde_json::to_value(row.base).unwrap())
    }

    for scenario in [
        "fill",
        "fill-down",
        "thin",
        "submit-expiry",
        "apply-expiry",
        "source-switch",
        "gap",
        "slow-quote",
        "stale-book-source",
    ] {
        paper::clear_recorded_events();
        let (mut bot, forward_broker, _, mut quote, mut observed) = forward_fixture();
        let down = scenario == "fill-down";
        if down {
            quote.price = 99.8;
        }
        bot.rounds[0].yes_name = "Up".into();
        bot.rounds[0].no_name = "Down".into();
        bot.rounds[0]
            .round_facts
            .as_mut()
            .unwrap()
            .price_feed_provider = None;
        let mut market = bot.rounds[0].clone();
        let late = scenario.ends_with("expiry");
        let last_ms = if late {
            268_000
        } else if scenario == "gap" {
            700_000
        } else if scenario == "slow-quote" {
            106_100
        } else {
            100_100
        };
        if scenario == "gap" {
            market.starts_at = Some(start() + chrono::Duration::milliseconds(650_000));
            market.ends_at = Some(start() + chrono::Duration::milliseconds(950_000));
            bot.rounds[0] = market.clone();
        }
        let checked = CheckTime {
            utc: start() + chrono::Duration::milliseconds(last_ms + 900),
            elapsed_ms: last_ms as u64 + 900,
        };
        let jump_on = match scenario {
            "submit-expiry" => 1,
            "apply-expiry" => 3,
            _ => usize::MAX,
        };
        let make_clock = || {
            Arc::new(DelayedClock {
                now: checked,
                jump_on,
                calls: AtomicUsize::new(0),
            })
        };
        let forward_clock = make_clock();
        let replay_clock = make_clock();
        let forward_broker = forward_broker
            .with_phase1(
                forward_clock.clone(),
                StrategyContract::Phase1NetShares,
                0.03,
                30.0,
            )
            .unwrap();
        let (_, replay_broker, _, _, _) = forward_fixture();
        let replay_broker = replay_broker
            .with_phase1(
                replay_clock.clone(),
                StrategyContract::Phase1NetShares,
                0.03,
                30.0,
            )
            .unwrap();
        let config = ReplayConfig {
            schema_version: replay::SCHEMA_VERSION_V2.into(),
            min_edge: 0.03,
            order_usd: 2.0,
            min_secs_left: 30,
            vol_floor: VOL_FLOOR,
            half_life_secs: 300.0,
            min_vol_samples: 20,
        };
        // Obtain the summary through the public facade; no duplicate summary logic.
        let mut summary = replay::run(
            config.clone(),
            ReplayMode::Replay,
            0,
            i64::MAX,
            std::io::Cursor::new(""),
            Vec::new(),
        )
        .unwrap();
        let mut driver = FixtureDriver::new(config, ReplayMode::Replay);
        driver.fixture_round_facts = market.round_facts.clone();
        let mut seq = 0;
        for index in 0..21 {
            let sample = clock(start(), index * 5_000, index * 5_000 + 100);
            let at = CheckTime {
                utc: sample.received_at,
                elapsed_ms: sample.received_elapsed_ms,
            };
            let price = 100.0 + index as f64 * if down { -0.01 } else { 0.01 };
            bot.core
                .observe_phase1(
                    "BTCUSDT",
                    price,
                    SpotSource::Binance,
                    "BTCUSDT",
                    &sample,
                    at,
                )
                .unwrap();
            step(
                &mut driver,
                &mut summary,
                &mut seq,
                at,
                json!({"type": "spot", "symbol": "BTCUSDT", "price": price,
                    "model_source": "Binance", "actual_pair": "BTCUSDT", "clock": wire_clock(&sample)}),
            );
        }
        if late || scenario == "gap" || scenario == "source-switch" || scenario == "slow-quote" {
            quote.clock = clock(start(), last_ms as u64, last_ms as u64 + 100);
            if scenario == "source-switch" {
                quote.clock = clock(start(), 100_200, 100_300);
                quote.source = SpotSource::Coinbase;
                quote.actual_pair = "BTC-USD".into();
            }
            if scenario == "slow-quote" {
                quote.clock = clock(start(), 100_000, 106_100);
            }
            let at = CheckTime {
                utc: quote.clock.received_at,
                elapsed_ms: quote.clock.received_elapsed_ms,
            };
            let result = bot.core.observe_phase1(
                "BTCUSDT",
                quote.price,
                quote.source,
                &quote.actual_pair,
                &quote.clock,
                at,
            );
            if scenario == "slow-quote" {
                assert_eq!(result, Err(Reason::RequestTooSlow));
            } else {
                result.unwrap();
            }
            step(
                &mut driver,
                &mut summary,
                &mut seq,
                at,
                json!({"type": "spot", "symbol": "BTCUSDT", "price": quote.price,
                    "model_source": quote.source, "actual_pair": quote.actual_pair,
                    "clock": wire_clock(&quote.clock)}),
            );
        }
        observed.clock = quote.clock.clone();
        if scenario == "thin" {
            observed.book.asks[0].size = 0.01;
        }
        if scenario == "stale-book-source" {
            // The book carries its own source time, and that time is too old:
            // the local receipt is fresh, the upstream event is not.
            observed.clock = source_clock(observed.clock.clone(), 90_000);
        }
        let available_at = quote.clock.received_at;
        let at = CheckTime {
            utc: available_at,
            elapsed_ms: quote.clock.received_elapsed_ms,
        };
        let mut availability = Availability::default();
        availability.record(&market, available_at);
        bot.availability.insert(market.id, availability);
        step(
            &mut driver,
            &mut summary,
            &mut seq,
            at,
            json!({"type": "market", "market_id": market.id, "symbol": "BTCUSDT",
                "starts_at_ms": market.starts_at.unwrap().timestamp_millis(),
                "ends_at_ms": market.ends_at.unwrap().timestamp_millis(),
                "yes_token": market.yes_token, "no_token": market.no_token,
                "yes_name": "Up", "no_name": "Down", "rules_source": "fixture"}),
        );
        step(
            &mut driver,
            &mut summary,
            &mut seq,
            at,
            json!({"type": "state", "market_id": market.id, "open": true,
                "contested": false, "paused": false, "start_price": market.start_price,
                "fee_rate_bps": market.fee_rate_bps}),
        );
        step(
            &mut driver,
            &mut summary,
            &mut seq,
            at,
            json!({"type": "book", "market_id": market.id, "yes_token": market.yes_token,
                "bids": observed.book.bids.iter().map(|level| json!({"price": level.price, "size": level.size})).collect::<Vec<_>>(),
                "asks": observed.book.asks.iter().map(|level| json!({"price": level.price, "size": level.size})).collect::<Vec<_>>(),
                "clock": wire_clock(&observed.clock)}),
        );
        forward_broker.observe(&market, &observed.book);
        replay_broker.observe(&market, &observed.book);
        bot.trade_phase1(
            &forward_broker,
            &[("BTCUSDT".into(), quote.clone())].into_iter().collect(),
            &[(market.id, observed.clone())].into_iter().collect(),
            forward_clock.as_ref(),
        )
        .await
        .unwrap();
        let forward_events = paper::recorded_events();
        let forward_decision = &forward_events[0]["source"]["decision"];
        let replay_now = replay_clock.now().unwrap();
        let (decision, _) = step(
            &mut driver,
            &mut summary,
            &mut seq,
            replay_now,
            json!({"type": "tick", "market_id": market.id}),
        );
        let decision = decision.unwrap();
        assert_eq!(
            serde_json::to_value(&decision).unwrap(),
            *forward_decision,
            "{scenario}: full decision"
        );
        paper::clear_recorded_events();
        let candidate = driver.fixture_candidate.take();
        assert!(replay_broker.emit_phase1_evaluation(&market, &decision, candidate.as_ref()));
        if let Some(candidate) = candidate {
            replay_broker.record_source(
                candidate.token(),
                &format!("{:?}", quote.source),
                checked.utc,
            );
            let result = replay_broker
                .take_candidate(&market, &observed.book, &candidate)
                .unwrap();
            let (status, shares, detail) = match result {
                TakeResult::Filled { shares, .. } => ("filled", Some(shares), None),
                TakeResult::Blocked(_) => ("blocked", None, Some("fixture-blocked")),
            };
            let (_, row) = step(
                &mut driver,
                &mut summary,
                &mut seq,
                replay_clock.now().unwrap(),
                json!({"type": "result", "market_id": market.id,
                    "candidate_event_id": candidate.candidate_id(), "status": status,
                    "filled_shares": shares, "detail": detail}),
            );
            assert_eq!(
                row["entered_after"],
                bot.core.is_entered(market.id),
                "{scenario}"
            );
        }
        let replay_events = paper::recorded_events();
        assert_eq!(
            forward_broker.paper_snapshot(),
            replay_broker.paper_snapshot(),
            "{scenario}: account"
        );
        assert_eq!(
            forward_events.len(),
            replay_events.len(),
            "{scenario}: event count"
        );
        for (forward, replay) in forward_events.iter().zip(&replay_events) {
            // Event wall time and run id are assembly identity. All economics,
            // ordering, diagnostics, candidate ids and checked clocks are exact.
            for field in [
                "event_kind",
                "event_seq",
                "market_id",
                "token",
                "accounting",
                "source",
            ] {
                assert_eq!(forward[field], replay[field], "{scenario}/{field}");
            }
        }
        let filled = scenario.starts_with("fill");
        match scenario {
            "source-switch" | "gap" => {
                assert_eq!(decision.reason, Reason::Warmup);
                assert_eq!(decision.samples, 0);
                assert_eq!(decision.source_epoch, Some(2));
                assert_eq!(
                    decision.reset_reason,
                    if scenario == "gap" {
                        ResetReason::LongGap
                    } else {
                        ResetReason::SourceSwitch
                    }
                );
            }
            "submit-expiry" | "apply-expiry" => {
                let checks: Vec<_> = forward_events
                    .iter()
                    .filter_map(|event| event["source"].get("decision"))
                    .collect();
                let last = checks.last().unwrap();
                assert_eq!(last["reason"], "ROUND_TOO_LATE");
                assert_eq!(
                    last["phase"],
                    if scenario == "submit-expiry" {
                        "PRE_SUBMIT"
                    } else {
                        "PRE_APPLY"
                    }
                );
                for kind in ["reservation", "release"] {
                    let count = forward_events
                        .iter()
                        .filter(|event| event["event_kind"] == kind)
                        .count();
                    assert_eq!(count, usize::from(scenario == "apply-expiry"));
                }
            }
            "thin" => assert_eq!(
                forward_events.last().unwrap()["source"]["reason"],
                "ThinBook"
            ),
            "slow-quote" => assert_eq!(decision.reason, Reason::RequestTooSlow),
            "stale-book-source" => {
                assert_eq!(decision.reason, Reason::BookStale);
                assert!(
                    !forward_events
                        .iter()
                        .any(|event| event["event_kind"] == "fill"),
                    "{scenario}: a stale book source never fills"
                );
                assert_eq!(
                    forward_broker.paper_snapshot().unwrap().cash,
                    100.0,
                    "{scenario}: cash"
                );
            }
            _ => (),
        }
        assert_eq!(
            bot.core.is_entered(market.id),
            filled,
            "{scenario}: entered"
        );
        if filled {
            let mut settled = market.clone();
            settled.open = false;
            settled.end_price = Some(if down { 99.0 } else { 101.0 });
            settled.payout_yes = Some(if down { 0.0 } else { 1.0 });
            let facts = settled.round_facts.as_mut().unwrap();
            facts.oracle_status = Some("RESOLVED".into());
            facts.trading_status = Some("CLOSED".into());
            facts.yes_status = Some(if down { "LOST" } else { "WON" }.into());
            facts.no_status = Some(if down { "WON" } else { "LOST" }.into());
            paper::clear_recorded_events();
            forward_broker.controlled_settle(&settled);
            forward_broker.controlled_settle(&settled);
            let forward = paper::recorded_events();
            paper::clear_recorded_events();
            replay_broker.controlled_settle(&settled);
            replay_broker.controlled_settle(&settled);
            let replay = paper::recorded_events();
            assert_eq!(forward.len(), 1);
            assert_eq!(replay.len(), 1);
            assert_eq!(forward[0]["accounting"], replay[0]["accounting"]);
            assert_eq!(forward[0]["source"], replay[0]["source"]);
            assert_eq!(
                forward_broker.paper_snapshot(),
                replay_broker.paper_snapshot()
            );
            assert!(forward_broker.holdings().is_empty());
            assert!(replay_broker.holdings().is_empty());
        } else {
            assert_eq!(forward_broker.paper_snapshot().unwrap().cash, 100.0);
            assert_eq!(forward_broker.paper_snapshot().unwrap().reserved, 0.0);
        }
    }
    paper::clear_recorded_events();
}

use crate::service::observation::{FreshnessBasis, SourceTimeStatus};

fn clock(start: DateTime<Utc>, request_ms: u64, receive_ms: u64) -> ObservationClock {
    ObservationClock {
        source_at: None,
        source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound,
        request_at: start + chrono::Duration::milliseconds(request_ms as i64),
        received_at: start + chrono::Duration::milliseconds(receive_ms as i64),
        request_elapsed_ms: request_ms,
        received_elapsed_ms: receive_ms,
    }
}

fn start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap()
}

fn market(fee_rate_bps: u32) -> TradableMarket {
    let now = start();
    TradableMarket {
        id: 1,
        open: true,
        contested: false,
        tick: 0.01,
        fee_rate_bps,
        starts_at: Some(now - chrono::Duration::seconds(1)),
        ends_at: Some(now + chrono::Duration::seconds(299)),
        start_price: Some(100.0),
        feed_symbol: Some("BTCUSDT".into()),
        yes_token: "yes-1".into(),
        no_token: "no-1".into(),
        round_facts: None,
        ..Default::default()
    }
}

fn book() -> BookSnapshot {
    BookSnapshot {
        bids: vec![BookLevel {
            price: 0.40,
            size: 100.0,
        }],
        asks: vec![BookLevel {
            price: 0.45,
            size: 100.0,
        }],
    }
}

fn warm(core: &mut super::super::DecisionCore, symbol: &str, count: u32) {
    for index in 0..count {
        let quote = clock(start(), index as u64 * 5_000, index as u64 * 5_000 + 100);
        let now = CheckTime {
            utc: quote.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: quote.received_elapsed_ms + 50,
        };
        core.observe_phase1(
            symbol,
            100.0 + index as f64 * 0.01,
            SpotSource::Binance,
            "BTCUSDT",
            &quote,
            now,
        )
        .unwrap();
    }
}

fn inputs<'a>(market: &'a TradableMarket, book: &'a BookSnapshot) -> EvaluationInputs<'a> {
    EvaluationInputs {
        market,
        book,
        spot_price: 100.5,
        spot_source: Some(SpotSource::Binance),
        actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(clock(start(), 0, 100)),
        book_clock: Some(clock(start(), 0, 100)),
        now: CheckTime {
            utc: start() + chrono::Duration::seconds(1),
            elapsed_ms: 1_000,
        },
        order_usd: 2.0,
        min_edge: 0.03,
        min_secs_left: 30.0,
        entered: false,
        identity_available_at: None,
        strike_available_at: None,
        settlement_provider: SettlementProvider::Unknown,
    }
}

#[test]
fn the_net_share_edge_is_smaller_than_the_cash_fee_edge() {
    // p = 0.45, bps = 200: the cash fee subtracts the whole fee, the
    // net-share edge only loses the fee's share of the payout.
    let cash_fee_edge = 0.55 - 0.45 - fee_per_gross_share(0.45, 200);
    let net_ratio = 1.0 - fee_per_gross_share(0.45, 200) / 0.45;
    let net_edge = 0.55 * net_ratio - 0.45;
    assert!(net_edge < cash_fee_edge);
    assert!((net_edge - 0.089).abs() < 1e-9, "{net_edge}");
}

#[test]
fn freshness_equalities_and_process_rollback_do_not_bypass_the_core() {
    for (fault, expected) in [
        ("equal", Reason::Eligible),
        ("span", Reason::RequestTooSlow),
        ("spot", Reason::SpotStale),
        ("book", Reason::BookStale),
    ] {
        let mut core = DecisionCore::phase1(Default::default());
        warm(&mut core, "BTCUSDT", 21);
        let market = market(200);
        let depth = book();
        let mut input = inputs(&market, &depth);
        input.spot_price = 100.2;
        input.spot_clock = Some(clock_at_latest());
        input.book_clock = Some(clock_at_latest());
        input.now = CheckTime {
            utc: start() + chrono::Duration::milliseconds(110_000),
            elapsed_ms: 110_000,
        };
        match fault {
            "span" => input.book_clock = Some(clock(start(), 104_000, 109_001)),
            "spot" => {
                input.now.utc += chrono::Duration::milliseconds(1);
                input.now.elapsed_ms += 1;
                input.book_clock = Some(clock(start(), 109_000, 109_100));
            }
            "book" => input.book_clock = Some(clock(start(), 99_999, 100_100)),
            _ => (),
        }
        assert_eq!(
            core.evaluate_phase1(&input).diagnostic.reason,
            expected,
            "{fault}"
        );
        if fault == "equal" {
            input.now.utc -= chrono::Duration::milliseconds(1);
            input.now.elapsed_ms -= 1;
            let rolled = core.evaluate_phase1(&input);
            assert_eq!(rolled.diagnostic.reason, Reason::ClockInvalid);
            assert_eq!(rolled.diagnostic.samples, 0);
            assert_eq!(rolled.diagnostic.source_epoch, Some(2));
        }
    }
}

/// A clock whose own source time is known.
///
/// The source time is a fact about the upstream event, not about the local
/// receipt: it may be much older than the request that fetched it.
fn source_clock(mut clock: ObservationClock, source_ms: u64) -> ObservationClock {
    clock.source_at = Some(start() + chrono::Duration::milliseconds(source_ms as i64));
    clock.source_time_status = SourceTimeStatus::Present;
    clock.freshness_basis = FreshnessBasis::SourceAndReceipt;
    clock
}

/// A book whose own source time is too old is refused, even when the local
/// receipt is still fresh. Ten seconds exactly is still fresh.
#[test]
fn a_book_with_an_old_source_is_refused_before_the_edge_is_computed() {
    for (source_ms, expected) in [
        (99_999u64, Reason::BookStale),
        (100_000, Reason::Eligible),
    ] {
        let mut core = DecisionCore::phase1(Default::default());
        warm(&mut core, "BTCUSDT", 21);
        let market = market(200);
        let depth = book();
        let mut input = inputs(&market, &depth);
        input.spot_price = 100.2;
        input.spot_clock = Some(clock_at_latest());
        input.book_clock = Some(source_clock(clock_at_latest(), source_ms));
        input.now = CheckTime {
            utc: start() + chrono::Duration::milliseconds(110_000),
            elapsed_ms: 110_000,
        };
        assert_eq!(
            core.evaluate_phase1(&input).diagnostic.reason,
            expected,
            "{source_ms}"
        );
    }
}

/// A clock that answers with a fixed list of readings.
///
/// The list is walked once per call; the last reading repeats.
struct ScriptedClock {
    readings: Vec<CheckTime>,
    calls: std::sync::atomic::AtomicUsize,
}

impl ScriptedClock {
    fn new(readings: Vec<CheckTime>) -> Self {
        Self {
            readings,
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Clock for ScriptedClock {
    fn now(&self) -> anyhow::Result<CheckTime> {
        let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(*self
            .readings
            .get(index)
            .unwrap_or_else(|| self.readings.last().unwrap()))
    }
}

/// The broker's own recheck refuses a book whose source time went stale
/// while the run was in flight: before the reservation and after it.
///
/// The source time is exactly ten seconds old in the accepted case, and one
/// millisecond older in the refused ones.
#[test]
fn a_stale_book_source_is_refused_by_the_broker_recheck() {
    for stage in ["submit", "apply", "equal"] {
        paper::clear_recorded_events();
        let (mut bot, broker, clock, quote, observed) = forward_fixture();
        warm(&mut bot.core, "BTCUSDT", 21);
        let market = bot.rounds[0].clone();
        let mut input = inputs(&market, &observed.book);
        input.spot_price = quote.price;
        input.spot_clock = Some(quote.clock.clone());
        input.book_clock = Some(source_clock(quote.clock.clone(), 92_000));
        input.now = clock.now().unwrap();
        let evaluation = bot.core.evaluate_phase1(&input);
        assert_eq!(evaluation.diagnostic.reason, Reason::Eligible, "{stage}");
        let candidate = evaluation.candidate.expect("a warm candidate");
        let fresh = CheckTime {
            utc: start() + chrono::Duration::milliseconds(101_000),
            elapsed_ms: 101_000,
        };
        let equal = CheckTime {
            utc: start() + chrono::Duration::milliseconds(102_000),
            elapsed_ms: 102_000,
        };
        let stale = CheckTime {
            utc: start() + chrono::Duration::milliseconds(102_001),
            elapsed_ms: 102_001,
        };
        let readings = match stage {
            "submit" => vec![stale],
            "apply" => vec![fresh, fresh, stale],
            _ => vec![equal, equal, equal],
        };
        let broker = broker
            .with_phase1(
                Arc::new(ScriptedClock::new(readings)),
                StrategyContract::Phase1NetShares,
                0.03,
                30.0,
            )
            .unwrap();
        broker.observe(&market, &observed.book);
        assert!(broker.emit_phase1_evaluation(
            &market,
            &evaluation.diagnostic,
            Some(&candidate)
        ));
        let result = broker
            .take_candidate(&market, &observed.book, &candidate)
            .unwrap();
        let events = paper::recorded_events();
        if stage == "equal" {
            assert!(matches!(result, TakeResult::Filled { .. }), "{stage}");
            assert!(
                !events
                    .iter()
                    .any(|event| event["source"]["diagnostic_reason"] == "BOOK_STALE"),
                "{stage}"
            );
            continue;
        }
        assert!(matches!(result, TakeResult::Blocked(_)), "{stage}");
        let blocked = events.last().unwrap();
        assert_eq!(blocked["event_kind"], "blocked", "{stage}");
        assert_eq!(
            blocked["source"]["diagnostic_reason"], "BOOK_STALE",
            "{stage}"
        );
        let reserved = usize::from(stage == "apply");
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_kind"] == "reservation")
                .count(),
            reserved,
            "{stage}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_kind"] == "release")
                .count(),
            reserved,
            "{stage}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_kind"] == "fill")
                .count(),
            0,
            "{stage}"
        );
        let snapshot = broker.paper_snapshot().unwrap();
        assert_eq!(snapshot.cash, 100.0, "{stage}");
        assert_eq!(snapshot.reserved, 0.0, "{stage}");
        assert!(broker.holdings().is_empty(), "{stage}");
    }
}

#[test]
fn a_source_switch_starts_a_new_epoch_and_warms_up_again() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 25);
    assert_eq!(core.samples("BTCUSDT"), 24);
    let quote = clock(start(), 200_000, 200_100);
    let now = CheckTime {
        utc: quote.received_at + chrono::Duration::milliseconds(50),
        elapsed_ms: quote.received_elapsed_ms + 50,
    };
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Coinbase,
        "BTC-USD",
        &quote,
        now,
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(2));
    assert_eq!(core.reset_reason("BTCUSDT"), ResetReason::SourceSwitch);
    assert_eq!(core.samples("BTCUSDT"), 0);
}

#[test]
fn a_gap_of_exactly_three_hundred_seconds_keeps_the_epoch() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    let last = 20 * 5_000 + 100;
    let quote = clock(start(), last + GAP_RESET_MS - 100, last + GAP_RESET_MS);
    let now = CheckTime {
        utc: quote.received_at + chrono::Duration::milliseconds(50),
        elapsed_ms: quote.received_elapsed_ms + 50,
    };
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Binance,
        "BTCUSDT",
        &quote,
        now,
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(1));
    assert_eq!(core.reset_reason("BTCUSDT"), ResetReason::FirstSample);
}

#[test]
fn a_longer_gap_starts_a_new_epoch() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    let last = 20 * 5_000 + 100;
    let quote = clock(start(), last + GAP_RESET_MS + 1, last + GAP_RESET_MS + 101);
    let now = CheckTime {
        utc: quote.received_at + chrono::Duration::milliseconds(50),
        elapsed_ms: quote.received_elapsed_ms + 50,
    };
    core.observe_phase1(
        "BTCUSDT",
        100.0,
        SpotSource::Binance,
        "BTCUSDT",
        &quote,
        now,
    )
    .unwrap();
    assert_eq!(core.epoch("BTCUSDT"), Some(2));
    assert_eq!(core.reset_reason("BTCUSDT"), ResetReason::LongGap);
}

#[test]
fn a_broken_clock_drops_the_observation_and_starts_a_new_epoch() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 21);
    let now = CheckTime {
        utc: start() + chrono::Duration::seconds(60),
        elapsed_ms: 60_000,
    };
    let broken = ObservationClock {
        source_at: None,
        source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound,
        request_at: start() + chrono::Duration::seconds(60),
        received_at: start() + chrono::Duration::seconds(60),
        request_elapsed_ms: 60_000,
        received_elapsed_ms: 59_000,
    };
    assert_eq!(
        core.observe_phase1(
            "BTCUSDT",
            100.0,
            SpotSource::Binance,
            "BTCUSDT",
            &broken,
            now,
        ),
        Err(Reason::ClockInvalid)
    );
    assert_eq!(core.epoch("BTCUSDT"), Some(2));
    assert_eq!(core.samples("BTCUSDT"), 0);
}

#[test]
fn duplicate_receive_and_source_times_reset_instead_of_extending_vol() {
    for fault in ["receive", "source"] {
        let mut core = super::super::DecisionCore::phase1(Default::default());
        let mut observed = clock(start(), 1_000, 1_100);
        if fault == "source" {
            observed.source_at = Some(start());
            observed.source_time_status = SourceTimeStatus::Present;
            observed.freshness_basis = FreshnessBasis::SourceAndReceipt;
        }
        let now = CheckTime {
            utc: start() + chrono::Duration::seconds(10),
            elapsed_ms: 10_000,
        };
        core.observe_phase1(
            "BTCUSDT",
            100.0,
            SpotSource::Binance,
            "BTCUSDT",
            &observed,
            now,
        )
        .unwrap();
        if fault == "source" {
            observed.request_at += chrono::Duration::seconds(1);
            observed.received_at += chrono::Duration::seconds(1);
            observed.request_elapsed_ms += 1_000;
            observed.received_elapsed_ms += 1_000;
        }
        assert_eq!(
            core.observe_phase1(
                "BTCUSDT",
                101.0,
                SpotSource::Binance,
                "BTCUSDT",
                &observed,
                now
            ),
            Err(Reason::ClockInvalid),
            "{fault}"
        );
        assert_eq!(core.samples("BTCUSDT"), 0);
        assert_eq!(core.epoch("BTCUSDT"), Some(2));
    }
}

#[test]
fn invalid_market_and_book_and_future_elapsed_fail_before_warmup() {
    for (fault, reason) in [
        ("duration", Reason::MarketInvalid),
        ("symbol", Reason::MarketInvalid),
        ("crossed", Reason::InputInvalid),
        ("size", Reason::InputInvalid),
        ("pair", Reason::InputInvalid),
        ("elapsed", Reason::ClockInvalid),
        ("status", Reason::ClockInvalid),
    ] {
        let mut core = super::super::DecisionCore::phase1(Default::default());
        let mut market = market(200);
        let mut book = book();
        match fault {
            "duration" => market.ends_at = Some(start() + chrono::Duration::seconds(300)),
            "symbol" => market.feed_symbol = None,
            "crossed" => book.bids[0].price = 0.5,
            "size" => book.asks[0].size = -1.0,
            _ => (),
        }
        let mut input = inputs(&market, &book);
        match fault {
            "pair" => input.actual_pair = Some("BTC-USD".into()),
            "elapsed" => input.spot_clock.as_mut().unwrap().received_elapsed_ms = 2_000,
            "status" => {
                input.spot_clock.as_mut().unwrap().source_time_status = SourceTimeStatus::Present
            }
            _ => (),
        }
        assert_eq!(
            core.evaluate_phase1(&input).diagnostic.reason,
            reason,
            "{fault}"
        );
    }
}

#[test]
fn a_cold_start_is_a_warmup_and_not_an_edge() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    let market = market(200);
    let book = book();
    let evaluation = core.evaluate_phase1(&inputs(&market, &book));
    assert_eq!(evaluation.diagnostic.reason, Reason::Warmup);
    assert!(evaluation.candidate.is_none());
}

#[test]
fn a_stale_spot_is_refused_before_the_edge_is_computed() {
    let mut core = super::super::DecisionCore::phase1(Default::default());
    warm(&mut core, "BTCUSDT", 25);
    let market = market(200);
    let book = book();
    let mut inputs = inputs(&market, &book);
    inputs.now = CheckTime {
        utc: start() + chrono::Duration::seconds(30),
        elapsed_ms: 30_000,
    };
    let evaluation = core.evaluate_phase1(&inputs);
    assert_eq!(evaluation.diagnostic.reason, Reason::SpotStale);
}
