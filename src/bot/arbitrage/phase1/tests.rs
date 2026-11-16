    use super::*;
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

    fn warm(core: &mut Phase1Core, symbol: &str, count: u32) {
        for index in 0..count {
            let quote = clock(start(), index as u64 * 5_000, index as u64 * 5_000 + 100);
            let now = CheckTime {
                utc: quote.received_at + chrono::Duration::milliseconds(50),
                elapsed_ms: quote.received_elapsed_ms + 50,
            };
            core.observe(
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
            spot_source: SpotSource::Binance,
            actual_pair: "BTCUSDT".into(),
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
    fn a_source_switch_starts_a_new_epoch_and_warms_up_again() {
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
        warm(&mut core, "BTCUSDT", 25);
        assert_eq!(core.samples("BTCUSDT"), 24);
        let quote = clock(start(), 200_000, 200_100);
        let now = CheckTime {
            utc: quote.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: quote.received_elapsed_ms + 50,
        };
        core.observe(
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
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
        warm(&mut core, "BTCUSDT", 21);
        let last = 20 * 5_000 + 100;
        let quote = clock(start(), last + GAP_RESET_MS - 100, last + GAP_RESET_MS);
        let now = CheckTime {
            utc: quote.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: quote.received_elapsed_ms + 50,
        };
        core.observe(
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
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
        warm(&mut core, "BTCUSDT", 21);
        let last = 20 * 5_000 + 100;
        let quote = clock(start(), last + GAP_RESET_MS + 1, last + GAP_RESET_MS + 101);
        let now = CheckTime {
            utc: quote.received_at + chrono::Duration::milliseconds(50),
            elapsed_ms: quote.received_elapsed_ms + 50,
        };
        core.observe(
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
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
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
            core.observe(
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
    fn a_cold_start_is_a_warmup_and_not_an_edge() {
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
        let market = market(200);
        let book = book();
        let evaluation = core.evaluate(&inputs(&market, &book));
        assert_eq!(evaluation.diagnostic.reason, Reason::Warmup);
        assert!(evaluation.candidate.is_none());
    }

    #[test]
    fn a_stale_spot_is_refused_before_the_edge_is_computed() {
        let mut core = Phase1Core::new(StrategyContract::Phase1NetShares);
        warm(&mut core, "BTCUSDT", 25);
        let market = market(200);
        let book = book();
        let mut inputs = inputs(&market, &book);
        inputs.now = CheckTime {
            utc: start() + chrono::Duration::seconds(30),
            elapsed_ms: 30_000,
        };
        let evaluation = core.evaluate(&inputs);
        assert_eq!(evaluation.diagnostic.reason, Reason::SpotStale);
    }
