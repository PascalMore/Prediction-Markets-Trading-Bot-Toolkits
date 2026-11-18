//! The observer's own tests.
//!
//! They check two things: that the observation says what it saw, and that
//! saying it changed nothing. The record is built from real inputs — a real
//! core, a real broker and real books — and nothing in the trading path is
//! replaced by a stand-in.

use super::*;
use crate::bot::arbitrage::phase1::{ResetReason, SettlementProvider};
use crate::bot::runtime::observer::BrokerObservationSnapshot;
use crate::bot::runtime::test_support::{book, market};
use crate::bot::runtime::Broker;
use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
use crate::service::observation::{
    Clock, FixtureClock, FreshnessBasis, ObservationClock, SourceTimeStatus,
};
use crate::service::spot::SpotSource;
use crate::service::venue::RoundFacts;
use std::collections::BTreeMap;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixDatagram;

/// The moment the fixture round starts.
fn start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap()
}

/// A clock of one acquired input.
fn input_clock(request_ms: u64, receive_ms: u64) -> ObservationClock {
    ObservationClock {
        source_at: None,
        source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound,
        request_at: start() + chrono::Duration::milliseconds(request_ms as i64),
        received_at: start() + chrono::Duration::milliseconds(receive_ms as i64),
        request_elapsed_ms: request_ms,
        received_elapsed_ms: receive_ms,
    }
}

/// The reading the fixture tick proved.
fn now_at(receive_ms: u64) -> CheckTime {
    CheckTime {
        utc: start() + chrono::Duration::milliseconds(receive_ms as i64),
        elapsed_ms: receive_ms,
    }
}

/// A round with the official facts the observer needs.
fn fixture_market() -> TradableMarket {
    TradableMarket {
        id: 200,
        open: true,
        contested: false,
        tick: 0.01,
        fee_rate_bps: 200,
        starts_at: Some(start()),
        ends_at: Some(start() + chrono::Duration::seconds(300)),
        start_price: Some(100.0),
        feed_symbol: Some("BTCUSDT".into()),
        yes_token: "1001".into(),
        no_token: "1002".into(),
        round_facts: Some(RoundFacts {
            decimal_precision: Some(2),
            price_feed_provider: Some("CHAINLINK".into()),
            ..Default::default()
        }),
        ..market(200)
    }
}

/// A core that has accepted twenty-one quotes from the primary feed.
///
/// The last one is the fixture's own quote: the observer may only value a
/// quote the core actually accepted.
fn warm_core() -> DecisionCore {
    let mut core = DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 2.0,
        ..Default::default()
    });
    for index in 0..21u64 {
        let receive_ms = 100_000 + index * 5_000;
        let clock = input_clock(receive_ms, receive_ms + 100);
        let reading = now_at(receive_ms + 100);
        core.observe_phase1(
            "BTCUSDT",
            100.0 + index as f64 * 0.01,
            SpotSource::Binance,
            "BTCUSDT",
            &clock,
            reading,
        )
        .unwrap();
    }
    core
}

/// The facts one observation is built from.
struct Fixture {
    core: DecisionCore,
    market: TradableMarket,
    quote: ObservedSpot,
    book: ObservedBook,
    now: CheckTime,
    state: Phase1ObservationState,
}

fn fixture() -> Fixture {
    let market = fixture_market();
    let receive_ms = 200_000;
    let clock = input_clock(receive_ms, receive_ms + 100);
    let core = warm_core();
    let state = core.phase1_observation_state("BTCUSDT");
    Fixture {
        core,
        market,
        quote: ObservedSpot {
            price: 100.2,
            source: SpotSource::Binance,
            actual_pair: "BTCUSDT".into(),
            clock: clock.clone(),
        },
        book: ObservedBook {
            book: book(&[(0.40, 100.0)], &[(0.45, 100.0)]),
            clock,
        },
        now: now_at(receive_ms + 100),
        state,
    }
}

/// One observation built from the fixture.
fn observe(fixture: &Fixture, token: &str, position: Option<(f64, f64)>) -> PostEntryObservation {
    let input = ObservationInput {
        run_id: "observer-test",
        seq: 1,
        market: &fixture.market,
        token,
        position,
        sample_state: SampleState::Active,
        now: Some(fixture.now),
        quote: Some(&fixture.quote),
        book: Some(&fixture.book),
        state: &fixture.state,
        identity_available_at: Some(start()),
        strike_available_at: Some(start()),
        paper_fee_confirmed: true,
    };
    build_observation(&fixture.core, &input).expect("the fixture is a valid observation")
}

/// The model's own value, for comparison.
fn core_p_up(core: &DecisionCore, spot: f64, strike: f64, secs_left: f64) -> Option<f64> {
    core.phase1_valuation("BTCUSDT", spot, strike, secs_left, phase1::VOL_FLOOR)
        .ok()
        .map(|value| value.p_up)
}

/// The observation says what the core's own model says.
#[test]
fn the_observation_reports_the_core_own_model() {
    let fixture = fixture();
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.model_reason, ModelReason::Ok);
    assert_eq!(record.p_down, record.p_up.map(|value| 1.0 - value));
    let secs_left =
        (fixture.market.ends_at.unwrap() - fixture.now.utc).num_milliseconds() as f64 / 1000.0;
    let expected = core_p_up(&fixture.core, 100.2, 100.0, secs_left).unwrap();
    assert!((record.p_up.unwrap() - expected).abs() < 1e-12);
    assert_eq!(record.source_epoch, Some(1));
    assert_eq!(record.reset_reason, ResetReason::FirstSample);
    assert_eq!(record.settlement_provider, SettlementProvider::Chainlink);
    assert_eq!(record.samples, 20);
}

/// The model value and the exit value are separate statements.
#[test]
fn the_model_value_and_the_exit_value_are_separate() {
    let fixture = fixture();
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    // The held token is the Yes side, so the hold value uses p_up.
    assert!((record.model_hold_value.unwrap() - 3.0 * record.p_up.unwrap()).abs() < 1e-12);
    assert!(
        (record.model_hold_surplus.unwrap() - (3.0 * record.p_up.unwrap() - 1.5)).abs() < 1e-12
    );
    // The exit is the visible bid, not the model's value.
    assert_eq!(record.exit_vwap, Some(0.40));
    assert!((record.exit_gross_income.unwrap() - 1.2).abs() < 1e-12);
}

/// The No side is read from the mirrored book, not from another depth.
#[test]
fn the_no_side_comes_from_the_mirrored_book() {
    let fixture = fixture();
    let record = observe(&fixture, "1002", Some((3.0, 1.5)));
    assert_eq!(record.down_quote_basis, DownQuoteBasis::MirroredYesBook);
    // The Yes bid at 0.40 is a No ask at 0.60; the Yes ask at 0.45 is a No bid
    // at 0.55.
    assert_eq!(record.down_ask, Some(0.60));
    assert_eq!(record.down_bid, Some(0.55));
    assert_eq!(record.exit_bid_levels.as_ref().unwrap()[0].price, 0.55);
}

/// The held token's own side is walked, never the other one.
#[test]
fn the_exit_walks_the_held_tokens_own_bids() {
    let fixture = fixture();
    let record = observe(&fixture, "1002", Some((3.0, 1.5)));
    // The No token's bid is the mirrored Yes ask at 0.55, not the Yes bid.
    assert_eq!(record.exit_vwap, Some(0.55));
    assert!((record.exit_gross_income.unwrap() - 1.65).abs() < 1e-12);
}

/// The fee is the confirmed paper rule, and it is named as such.
#[test]
fn the_fee_is_the_confirmed_paper_rule() {
    let fixture = fixture();
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.fee_basis, FeeBasis::PaperCashFee);
    assert_eq!(record.fee_rate_bps, Some(200));
    // r * min(p, 1 - p) * q, on the walk's average price.
    let expected = 0.02 * 0.4 * 3.0;
    assert!((record.exit_fee.unwrap() - expected).abs() < 1e-12);
    assert!((record.exit_net_income.unwrap() - (1.2 - expected)).abs() < 1e-12);
    assert!((record.exit_pnl_vs_cost.unwrap() - (1.2 - expected - 1.5)).abs() < 1e-12);
}

/// An unconfirmed fee rule leaves the net income unknown.
#[test]
fn an_unconfirmed_fee_rule_leaves_the_net_income_unknown() {
    let fixture = fixture();
    let input = ObservationInput {
        run_id: "observer-test",
        seq: 1,
        market: &fixture.market,
        token: "1001",
        position: Some((3.0, 1.5)),
        sample_state: SampleState::Active,
        now: Some(fixture.now),
        quote: Some(&fixture.quote),
        book: Some(&fixture.book),
        state: &fixture.state,
        identity_available_at: Some(start()),
        strike_available_at: Some(start()),
        paper_fee_confirmed: false,
    };
    let record = build_observation(&fixture.core, &input).unwrap();
    assert_eq!(record.fee_basis, FeeBasis::Unknown);
    assert_eq!(record.fee_rate_bps, None);
    assert_eq!(record.exit_fee, None);
    assert_eq!(record.exit_net_income, None);
    assert_eq!(record.exit_pnl_vs_cost, None);
    // The gross value still stands: it does not need a fee.
    assert!((record.exit_gross_income.unwrap() - 1.2).abs() < 1e-12);
}

/// A position that is not held has no exit scale at all.
#[test]
fn a_position_that_is_not_held_has_no_exit_scale() {
    let fixture = fixture();
    let record = observe(&fixture, "1001", None);
    assert_eq!(record.exit_reason, ExitReason::NoPosition);
    assert_eq!(record.position_net_shares, None);
    assert_eq!(record.position_cost, None);
    assert_eq!(record.exit_covered_shares, None);
    assert_eq!(record.exit_vwap, None);
    assert_eq!(record.exit_gross_income, None);
    assert_eq!(record.model_hold_value, None);
}

/// The depth that is there is disclosed even when it is not enough.
#[test]
fn an_insufficient_depth_keeps_the_prefix_and_nulls_the_money() {
    let mut fixture = fixture();
    // One share is visible; ten are held.
    fixture.book.book = book(&[(0.40, 1.0)], &[(0.45, 100.0)]);
    let record = observe(&fixture, "1001", Some((10.0, 5.0)));
    assert_eq!(record.exit_reason, ExitReason::InsufficientDepth);
    assert_eq!(record.exit_depth_complete, Some(false));
    assert_eq!(record.exit_covered_shares, Some(1.0));
    assert_eq!(record.exit_bid_levels.as_ref().unwrap().len(), 1);
    assert_eq!(record.exit_vwap, None);
    assert_eq!(record.exit_gross_income, None);
    assert_eq!(record.exit_net_income, None);
    assert_eq!(record.exit_pnl_vs_cost, None);
}

/// The last level keeps its own size, not the size that was consumed.
#[test]
fn the_last_level_keeps_its_own_size() {
    let mut fixture = fixture();
    fixture.book.book = book(&[(0.40, 1.0)], &[(0.45, 100.0)]);
    let record = observe(&fixture, "1001", Some((10.0, 5.0)));
    let levels = record.exit_bid_levels.as_ref().unwrap();
    assert_eq!(levels[0].size, 1.0);
}

/// A book with no bids is an empty book, not a missing one.
#[test]
fn an_empty_book_is_a_no_bid() {
    let mut fixture = fixture();
    fixture.book.book = book(&[], &[(0.45, 100.0)]);
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.exit_reason, ExitReason::NoBid);
    assert_eq!(record.exit_covered_shares, Some(0.0));
    assert_eq!(record.exit_depth_complete, Some(false));
    assert_eq!(record.exit_vwap, None);
}

/// A book that is not a book cannot be walked.
#[test]
fn an_invalid_book_is_refused() {
    let mut fixture = fixture();
    // The bid crosses the ask: no exit price exists in it.
    fixture.book.book = book(&[(0.50, 100.0)], &[(0.45, 100.0)]);
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.exit_reason, ExitReason::InputInvalid);
    assert_eq!(record.exit_vwap, None);
}

/// A missing book leaves the model alone.
#[test]
fn a_missing_book_leaves_the_model_alone() {
    let fixture = fixture();
    let input = ObservationInput {
        run_id: "observer-test",
        seq: 1,
        market: &fixture.market,
        token: "1001",
        position: Some((3.0, 1.5)),
        sample_state: SampleState::Active,
        now: Some(fixture.now),
        quote: Some(&fixture.quote),
        book: None,
        state: &fixture.state,
        identity_available_at: Some(start()),
        strike_available_at: Some(start()),
        paper_fee_confirmed: true,
    };
    let record = build_observation(&fixture.core, &input).unwrap();
    assert_eq!(record.exit_reason, ExitReason::MissingBook);
    assert_eq!(record.model_reason, ModelReason::Ok);
    assert!(record.p_up.is_some());
}

/// A missing spot leaves the exit alone.
#[test]
fn a_missing_spot_leaves_the_exit_alone() {
    let fixture = fixture();
    let input = ObservationInput {
        run_id: "observer-test",
        seq: 1,
        market: &fixture.market,
        token: "1001",
        position: Some((3.0, 1.5)),
        sample_state: SampleState::Active,
        now: Some(fixture.now),
        quote: None,
        book: Some(&fixture.book),
        state: &fixture.state,
        identity_available_at: Some(start()),
        strike_available_at: Some(start()),
        paper_fee_confirmed: true,
    };
    let record = build_observation(&fixture.core, &input).unwrap();
    assert_eq!(record.model_reason, ModelReason::MissingInput);
    assert_eq!(record.exit_reason, ExitReason::Ok);
    assert!((record.exit_gross_income.unwrap() - 1.2).abs() < 1e-12);
}

/// A quote the core did not accept cannot borrow the old epoch's warmth.
#[test]
fn a_quote_the_core_refused_cannot_be_valued() {
    let mut fixture = fixture();
    // A different source than the epoch's: the core's own guard refuses it.
    fixture.quote.source = SpotSource::Coinbase;
    fixture.quote.actual_pair = "BTC-USD".into();
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.model_reason, ModelReason::InputChanged);
    assert_eq!(record.p_up, None);
}

/// A stale quote is refused with its own reason.
#[test]
fn a_stale_quote_is_refused() {
    let mut fixture = fixture();
    fixture.quote.clock = input_clock(0, 100);
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.model_reason, ModelReason::SpotStale);
}

/// A clock that cannot be read leaves no normal sample.
#[test]
fn a_clock_that_cannot_be_read_leaves_no_sample() {
    let fixture = fixture();
    let input = ObservationInput {
        run_id: "observer-test",
        seq: 1,
        market: &fixture.market,
        token: "1001",
        position: Some((3.0, 1.5)),
        sample_state: SampleState::Active,
        now: None,
        quote: Some(&fixture.quote),
        book: Some(&fixture.book),
        state: &fixture.state,
        identity_available_at: Some(start()),
        strike_available_at: Some(start()),
        paper_fee_confirmed: true,
    };
    let record = build_observation(&fixture.core, &input).unwrap();
    assert_eq!(record.observed_at, None);
    assert_eq!(record.observed_elapsed_ms, None);
    assert_eq!(record.model_reason, ModelReason::MissingInput);
    assert_eq!(record.exit_reason, ExitReason::MissingBook);
}

/// A cold epoch is not a model.
#[test]
fn a_cold_epoch_is_not_a_model() {
    let mut fixture = fixture();
    // A core that has accepted nothing: its epoch does not exist.
    let cold = DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 2.0,
        ..Default::default()
    });
    fixture.core = cold;
    fixture.state = fixture.core.phase1_observation_state("BTCUSDT");
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.model_reason, ModelReason::Warmup);
    assert_eq!(record.p_up, None);
    assert_eq!(record.sigma, None);
}

/// A terminal state carries no new value.
#[test]
fn a_terminal_state_carries_no_new_value() {
    let fixture = fixture();
    for (state, model, exit) in [
        (
            SampleState::RoundEnded,
            ModelReason::RoundEnded,
            ExitReason::RoundEnded,
        ),
        (
            SampleState::Settled,
            ModelReason::Settled,
            ExitReason::Settled,
        ),
        (
            SampleState::RunStopped,
            ModelReason::RunStopped,
            ExitReason::RunStopped,
        ),
    ] {
        let input = ObservationInput {
            run_id: "observer-test",
            seq: 1,
            market: &fixture.market,
            token: "1001",
            position: Some((3.0, 1.5)),
            sample_state: state,
            now: Some(fixture.now),
            quote: Some(&fixture.quote),
            book: Some(&fixture.book),
            state: &fixture.state,
            identity_available_at: Some(start()),
            strike_available_at: Some(start()),
            paper_fee_confirmed: true,
        };
        let record = build_observation(&fixture.core, &input).unwrap();
        assert_eq!(record.model_reason, model);
        assert_eq!(record.exit_reason, exit);
        assert_eq!(record.p_up, None);
        assert_eq!(record.exit_vwap, None);
        assert_eq!(record.exit_gross_income, None);
        assert_eq!(record.model_hold_value, None);
    }
}

/// A token that is not this market's is refused.
#[test]
fn an_unknown_token_is_refused() {
    let fixture = fixture();
    let record = observe(&fixture, "9999", Some((3.0, 1.5)));
    assert_eq!(record.exit_reason, ExitReason::MarketInvalid);
    assert_eq!(record.model_reason, ModelReason::MarketInvalid);
    assert_eq!(record.model_hold_value, None);
}

/// The identity of the observation is the market's own decimal id.
#[test]
fn the_identity_is_the_decimal_id() {
    let fixture = fixture();
    let record = observe(&fixture, "1001", Some((3.0, 1.5)));
    assert_eq!(record.market_id, "200");
    assert_eq!(record.token, "1001");
    assert_eq!(record.schema_version, RECORD_SCHEMA_VERSION);
}

/// The clock proxy forwards every reading and remembers the last one.
#[test]
fn the_clock_proxy_forwards_every_reading() {
    let clock = std::sync::Arc::new(FixtureClock::new(start()));
    let proxy = TickClock::new(clock.clone());
    assert_eq!(proxy.now().unwrap(), clock.now().unwrap());
    clock.advance(1_000);
    let second = proxy.now().unwrap();
    assert_eq!(second.elapsed_ms, 1_000);
    assert_eq!(proxy.last(), Some(second));
}

/// The broker's snapshot reports the inventory without changing it.
#[test]
fn the_broker_snapshot_reports_the_inventory_and_changes_nothing() {
    let now = Utc::now();
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
        ControlledRun::new(ControlledMode::UnsignedPaper, "observer-test".into(), now).unwrap(),
        100.0,
    )
    .unwrap();
    let market = fixture_market();
    let book_snapshot = book(&[(0.40, 100.0)], &[(0.45, 100.0)]);
    broker.observe(&market, &book_snapshot);
    let before = broker.paper_snapshot().unwrap();
    let snapshot = broker.post_entry_snapshot();
    assert!(snapshot.positions.is_empty());
    assert!(!snapshot.entries_stopped);
    let after = broker.paper_snapshot().unwrap();
    assert_eq!(before, after);
}

/// The channel reports what it saw and closes with its own report.
#[test]
fn the_channel_reports_and_closes() {
    let (parent, child) = UnixDatagram::pair().unwrap();
    let fd = child.as_raw_fd();
    // SAFETY: the descriptor is duplicated for the observer and closed here.
    let duplicate = unsafe { libc_dup(fd) };
    std::env::set_var(FD_ENV, duplicate.to_string());
    let mut observer = PostEntryObserver::attach("observer-test", true).unwrap();
    let mut buffer = [0u8; 64 * 1024];
    let read = parent.recv(&mut buffer).unwrap();
    let ready: serde_json::Value = serde_json::from_slice(&buffer[..read]).unwrap();
    assert_eq!(ready["kind"], "READY");
    assert_eq!(ready["wire_version"], WIRE_VERSION);
    assert_eq!(ready["run_id"], "observer-test");
    observer.finish();
    let read = parent.recv(&mut buffer).unwrap();
    let end: serde_json::Value = serde_json::from_slice(&buffer[..read]).unwrap();
    assert_eq!(end["kind"], "END");
    assert!(end["producer"]["attempted_samples"].as_u64().is_some());
    // SAFETY: the duplicate was created above and is not used again.
    unsafe { libc_close(duplicate) };
    std::env::remove_var(FD_ENV);
    drop(parent);
    drop(child);
}

/// A missing channel is not an error: the run simply does not observe.
#[test]
fn a_missing_channel_is_not_an_error() {
    std::env::remove_var(FD_ENV);
    assert!(PostEntryObserver::attach("observer-test", true).is_none());
}

/// A descriptor that is not a datagram is refused.
#[test]
fn a_descriptor_that_is_not_a_datagram_is_refused() {
    // A pipe's read end is a valid descriptor, but not a datagram.
    let mut fds = [0i32; 2];
    // SAFETY: the pair is created here and closed at the end of the test.
    let ok = unsafe { libc_pipe(fds.as_mut_ptr()) };
    assert_eq!(ok, 0);
    std::env::set_var(FD_ENV, fds[0].to_string());
    assert!(PostEntryObserver::attach("observer-test", true).is_none());
    std::env::remove_var(FD_ENV);
    // SAFETY: the pair was created above and is not used again.
    unsafe {
        libc_close(fds[0]);
        libc_close(fds[1]);
    }
}

/// The observation sequence has no gaps when every attempt succeeds.
#[test]
fn the_observation_sequence_has_no_gaps() {
    let fixture = fixture();
    let snapshot: BrokerObservationSnapshot = BrokerObservationSnapshot::default();
    let markets: BTreeMap<u64, TradableMarket> =
        BTreeMap::from([(fixture.market.id, fixture.market.clone())]);
    let availability: BTreeMap<u64, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)> =
        BTreeMap::from([(fixture.market.id, (Some(start()), Some(start())))]);
    let mut observer = PostEntryObserver {
        socket: None,
        run_id: "observer-test".into(),
        seq: 0,
        paper_fee_confirmed: true,
        series: BTreeMap::new(),
        counters: ProducerCounters::default(),
        disabled: None,
    };
    let quotes = HashMap::from([("BTCUSDT".to_string(), fixture.quote.clone())]);
    let books = HashMap::from([(fixture.market.id, fixture.book.clone())]);
    for _ in 0..3 {
        observer.after_tick(
            &fixture.core,
            &snapshot,
            &["1001".to_string()],
            Some(fixture.now),
            &quotes,
            &books,
            &markets,
            &availability,
            &["BTCUSDT".into()].into_iter().collect(),
        );
    }
    // The snapshot holds nothing, so there is nothing to observe: no sample
    // is invented out of an empty position.
    assert_eq!(observer.counters.attempted_samples, 0);
    assert_eq!(observer.counters.last_observation_seq, 0);
}

/// Exercise the producer with a real local socket and a positive net holding.
fn producer_fixture() -> (PostEntryObserver, UnixDatagram, BrokerObservationSnapshot) {
    let (parent, child) = UnixDatagram::pair().unwrap();
    child.set_nonblocking(true).unwrap();
    parent
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let producer = PostEntryObserver {
        socket: Some(child),
        run_id: "observer-test".into(),
        seq: 0,
        paper_fee_confirmed: true,
        series: BTreeMap::new(),
        counters: ProducerCounters::default(),
        disabled: None,
    };
    let market = fixture_market();
    let position = crate::bot::runtime::observer::ObservedPosition {
        market_id: market.id,
        token: market.yes_token.clone(),
        shares: 3.0,
        cost: 1.5,
    };
    let snapshot = BrokerObservationSnapshot {
        positions: HashMap::from([(position.token.clone(), position)]),
        traded: HashMap::from([(market.id, market)]),
        ..Default::default()
    };
    (producer, parent, snapshot)
}

fn produce(
    producer: &mut PostEntryObserver,
    snapshot: &BrokerObservationSnapshot,
    fixture: &Fixture,
    now: Option<CheckTime>,
) {
    producer.after_tick(
        &fixture.core,
        snapshot,
        &["1001".into()],
        now,
        &HashMap::from([("BTCUSDT".into(), fixture.quote.clone())]),
        &HashMap::from([(fixture.market.id, fixture.book.clone())]),
        &BTreeMap::from([(fixture.market.id, fixture.market.clone())]),
        &BTreeMap::from([(fixture.market.id, (Some(start()), Some(start())))]),
        &["BTCUSDT".into()].into_iter().collect(),
    );
}

#[test]
fn terminal_is_sent_only_once_and_is_never_reopened() {
    let fixture = fixture();
    let (mut producer, parent, mut snapshot) = producer_fixture();
    produce(&mut producer, &snapshot, &fixture, Some(fixture.now));
    snapshot.entries_stopped = true;
    produce(&mut producer, &snapshot, &fixture, Some(fixture.now));
    snapshot.entries_stopped = false;
    produce(&mut producer, &snapshot, &fixture, Some(fixture.now));
    assert_eq!(producer.counters.sent_samples, 2);
    let mut bytes = [0u8; MAX_FRAME_BYTES];
    parent.recv(&mut bytes).unwrap();
    let size = parent.recv(&mut bytes).unwrap();
    let frame: serde_json::Value = serde_json::from_slice(&bytes[..size]).unwrap();
    assert_eq!(frame["record"]["sample_state"], "RUN_STOPPED");
    assert!(frame["record"]["p_up"].is_null());
}

#[test]
fn missing_clock_is_a_counted_drop_not_a_normal_null_sample() {
    let fixture = fixture();
    let (mut producer, _parent, snapshot) = producer_fixture();
    produce(&mut producer, &snapshot, &fixture, None);
    assert_eq!(producer.counters.attempted_samples, 1);
    assert_eq!(producer.counters.last_observation_seq, 1);
    assert_eq!(producer.counters.clock_unavailable_samples, 1);
    assert_eq!(producer.counters.dropped_samples, 1);
    assert_eq!(producer.counters.encoded_samples, 0);
    assert_eq!(producer.counters.sent_samples, 0);
}

#[test]
fn terminal_series_are_subject_to_the_same_capacity_limit() {
    let (mut producer, _parent, _) = producer_fixture();
    for id in 1..=MAX_SERIES as u64 + 1 {
        let mut market = fixture_market();
        market.id = id;
        producer.remember(&market, "1001", SampleState::Settled, id);
    }
    assert_eq!(producer.series.len(), MAX_SERIES);
    assert_eq!(producer.disabled, Some(DisabledReason::ObservationLimit));
}

#[test]
fn a_failed_terminal_send_has_no_successful_terminal_sequence() {
    let fixture = fixture();
    let (mut producer, parent, mut snapshot) = producer_fixture();
    snapshot.entries_stopped = true;
    drop(parent);
    produce(&mut producer, &snapshot, &fixture, Some(fixture.now));
    assert_eq!(producer.counters.sent_samples, 0);
    assert_eq!(producer.counters.send_failed_samples, 1);
    assert_eq!(producer.counters.dropped_samples, 1);
    let series = producer
        .series
        .get(&(fixture.market.id, "1001".into()))
        .unwrap();
    assert_eq!(series.terminal, Some(SampleState::RunStopped));
    assert_eq!(series.terminal_seq, None);
}

// --- The narrow libc calls the channel needs -------------------------------

extern "C" {
    fn dup(oldfd: i32) -> i32;
    fn close(fd: i32) -> i32;
    fn pipe(fds: *mut i32) -> i32;
}

unsafe fn libc_dup(fd: i32) -> i32 {
    dup(fd)
}

unsafe fn libc_pipe(fds: *mut i32) -> i32 {
    pipe(fds)
}

unsafe fn libc_close(fd: i32) {
    close(fd);
}

/// Only the external transports are replaced; acquisition still consumes Clock.
#[test]
fn clock_bounds_use_original_request_age_and_exact_equalities() {
    for (span, age, expected) in [
        (5_000, 10_000, None),
        (5_001, 10_000, Some("slow")),
        (5_000, 10_001, Some("stale")),
        (0, 10_000, None),
    ] {
        let clock = input_clock(0, span);
        assert_eq!(
            clock_error(&clock, now_at(age), "invalid", "slow", "stale"),
            expected
        );
    }
    let invalid = input_clock(101, 100);
    assert_eq!(
        clock_error(&invalid, now_at(200), "invalid", "slow", "stale"),
        Some("invalid")
    );
    let clock = Arc::new(FixtureClock::new(start()));
    clock.advance(20);
    let proxy = TickClock::new(clock.clone());
    assert_eq!(proxy.now().unwrap(), clock.now().unwrap());
    // A failing underlying clock must not leave a prior successful reading usable.
    struct Fails;
    impl Clock for Fails {
        fn now(&self) -> Result<CheckTime> {
            anyhow::bail!("FIXTURE_CLOCK")
        }
    }
    let failed = TickClock::new(Arc::new(Fails));
    assert!(failed.now().is_err());
    assert_eq!(failed.last(), None);
}

#[test]
fn original_epoch_warmup_gap_and_source_boundaries_are_read_only() {
    for gap in [300_000, 300_001] {
        let mut fixture = fixture();
        let at = 200_100 + gap;
        let clock = input_clock(at, at);
        fixture
            .core
            .observe_phase1(
                "BTCUSDT",
                100.2,
                SpotSource::Binance,
                "BTCUSDT",
                &clock,
                now_at(at),
            )
            .unwrap();
        assert_eq!(
            fixture.core.epoch("BTCUSDT"),
            Some(if gap == 300_000 { 1 } else { 2 })
        );
        assert_eq!(
            fixture.core.samples("BTCUSDT"),
            if gap == 300_000 { 21 } else { 0 }
        );
    }
    let mut fixture = fixture();
    for (offset, source, pair, epoch) in [
        (1, SpotSource::Coinbase, "BTC-USD", 2),
        (2, SpotSource::Binance, "BTCUSDT", 3),
    ] {
        let at = 200_100 + offset;
        fixture
            .core
            .observe_phase1(
                "BTCUSDT",
                100.2,
                source,
                pair,
                &input_clock(at, at),
                now_at(at),
            )
            .unwrap();
        fixture.state = fixture.core.phase1_observation_state("BTCUSDT");
        fixture.quote = ObservedSpot {
            price: 100.2,
            source,
            actual_pair: pair.into(),
            clock: input_clock(at, at),
        };
        fixture.now = now_at(at);
        assert_eq!(
            observe(&fixture, "1001", Some((3.0, 1.5))).model_reason,
            ModelReason::Warmup
        );
        assert_eq!(fixture.core.epoch("BTCUSDT"), Some(epoch));
        assert_eq!(fixture.core.samples("BTCUSDT"), 0);
    }
}

#[test]
fn epsilon_and_oversized_depth_never_fabricate_full_income() {
    let mut fixture = fixture();
    for (shares, price) in [(EPS / 2.0, 0.4), (EPS, 0.4), (3.0, 1e-9)] {
        fixture.book.book.bids[0].price = price;
        let record = observe(&fixture, "1001", Some((shares, 0.0)));
        assert_eq!(record.exit_reason, ExitReason::InputInvalid);
        assert_eq!(record.exit_gross_income, None);
    }
    let size = 0.00000012345678901234567;
    fixture.book.book = book(&vec![(0.12345678901234567, size); 1000], &[(0.3, 1.0)]);
    let mut record = observe(&fixture, "1001", Some((size * 1000.0, 0.00001)));
    assert_eq!(record.exit_reason, ExitReason::Ok);
    assert_eq!(record.exit_bid_levels.as_ref().unwrap().len(), 1000);
    fixture
        .book
        .book
        .bids
        .push(fixture.book.book.bids[0].clone());
    assert_eq!(
        observe(&fixture, "1001", Some((1.0, 0.1))).exit_reason,
        ExitReason::ObservationLimit
    );
    // Fault-inject an oversized encoded payload at the physical writer seam.
    let levels = record.exit_bid_levels.as_ref().unwrap().clone();
    record.exit_bid_levels.as_mut().unwrap().extend(levels);
    let (mut producer, parent, _) = producer_fixture();
    assert!(producer.send_sample(10, &mut record));
    assert_eq!(record.exit_reason, ExitReason::ObservationLimit);
    assert!(record.exit_bid_levels.is_none());
    assert_eq!(record.exit_gross_income, None);
    assert_eq!(record.model_reason, ModelReason::Ok);
    let mut bytes = [0; MAX_FRAME_BYTES + 1];
    assert!(parent.recv(&mut bytes).unwrap() <= MAX_FRAME_BYTES);
}

#[test]
fn tick_external_stop_reports_missing_terminal_without_a_new_clock() {
    let fixture = fixture();
    let (mut producer, parent, mut snapshot) = producer_fixture();
    produce(&mut producer, &snapshot, &fixture, Some(fixture.now));
    snapshot.entries_stopped = true;
    producer.finish_with_snapshot(&snapshot);
    let mut bytes = [0; MAX_FRAME_BYTES];
    parent.recv(&mut bytes).unwrap();
    let size = parent.recv(&mut bytes).unwrap();
    let end: serde_json::Value = serde_json::from_slice(&bytes[..size]).unwrap();
    assert_eq!(end["producer"]["attempted_samples"], 1);
    assert_eq!(
        end["producer"]["series"][0]["terminal_state"],
        "RUN_STOPPED"
    );
    assert!(end["producer"]["series"][0]["terminal_observation_seq"].is_null());
}

/// Only the external transports are replaced; acquisition still consumes Clock.
struct TickFixtureData {
    requests: std::sync::Mutex<Vec<Vec<u64>>>,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::service::venue::MarketData for TickFixtureData {
    async fn markets(&self, _: &crate::service::venue::MarketQuery) -> Result<Vec<TradableMarket>> {
        panic!("UNDECLARED_MARKET_REQUEST")
    }
    async fn market(&self, _: u64) -> Result<TradableMarket> {
        panic!("UNDECLARED_MARKET_REQUEST")
    }
    async fn with_schedule(&self, _: &TradableMarket) -> Result<TradableMarket> {
        panic!("UNDECLARED_SCHEDULE_REQUEST")
    }
    async fn books(&self, _: &[u64]) -> Result<HashMap<u64, BookSnapshot>> {
        panic!("UNCLOCKED_BOOK_REQUEST")
    }
    async fn observed_books(
        &self,
        ids: &[u64],
        clock: &dyn Clock,
    ) -> Result<HashMap<u64, ObservedBook>> {
        self.requests.lock().unwrap().push(ids.to_vec());
        assert_eq!(ids, &[200]);
        let request = clock.now()?;
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("FIXTURE_BOOK_FAILURE")
        }
        let receive = clock.now()?;
        let clock = ObservationClock {
            source_at: None,
            source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: request.utc,
            received_at: receive.utc,
            request_elapsed_ms: request.elapsed_ms,
            received_elapsed_ms: receive.elapsed_ms,
        };
        Ok(HashMap::from([(
            200,
            ObservedBook {
                book: book(&[(0.40, 2.0), (0.39, 100.0)], &[(0.45, 100.0)]),
                clock,
            },
        )]))
    }
}

struct TickFixtureClock {
    at: std::sync::Mutex<CheckTime>,
    calls: std::sync::Mutex<Vec<CheckTime>>,
}

impl Clock for TickFixtureClock {
    fn now(&self) -> Result<CheckTime> {
        let at = *self.at.lock().unwrap();
        self.calls.lock().unwrap().push(at);
        Ok(at)
    }
}

/// Snapshot after every original tick, not just the final zero-inventory state.
async fn tick_scenario(observer: Option<PostEntryObserver>, failures: bool) -> serde_json::Value {
    use crate::bot::arbitrage::{Availability, RoundTaker};
    use crate::bot::runtime::{paper, Ctx, Strategy};
    use crate::service::predict_fun::PredictFunClient;
    use crate::service::spot::{SpotFeed, SpotQuote};
    use std::sync::Arc;
    paper::clear_recorded_events();
    let market = fixture_market();
    let clock = Arc::new(TickFixtureClock {
        at: std::sync::Mutex::new(now_at(0)),
        calls: Default::default(),
    });
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
            "observer-test".into(),
            Utc::now(),
        )
        .unwrap(),
        100.0,
    )
    .unwrap()
    .with_phase1(
        clock.clone(),
        phase1::StrategyContract::Phase1NetShares,
        0.03,
        30.0,
    )
    .unwrap();
    let mut cfg: crate::config::AppConfig = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tools/predictfun/btc5m-controlled-paper.json"
    )))
    .unwrap();
    cfg.credentials.predict_api_key = Some("synthetic-market-only".into());
    let data = Arc::new(TickFixtureData {
        requests: Default::default(),
        fail: false.into(),
    });
    let ctx = Ctx {
        client: PredictFunClient::new(&cfg).unwrap(),
        cfg,
        data: data.clone(),
        broker: Arc::new(broker),
    };
    let cfg = crate::config::BtcArbConfig {
        order_usd: 2.0,
        symbols: vec!["BTCUSDT".into()],
        ..Default::default()
    };
    let mut available = Availability::default();
    available.record(&market, start());
    let quote = ObservedSpot {
        price: 100.2,
        source: SpotSource::Binance,
        actual_pair: "BTCUSDT".into(),
        clock: input_clock(0, 0),
    };
    let mut bot = RoundTaker {
        cfg: cfg.clone(),
        core: DecisionCore::phase1(cfg),
        spot: SpotFeed::new().unwrap(),
        known: Default::default(),
        rounds: vec![market.clone()],
        last_scan: Some(std::time::Instant::now()),
        observer,
        observed_quote: Some(Ok(quote.clone())),
        observed_symbols: Vec::new(),
        availability: HashMap::from([(market.id, available)]),
        inputs: Some((
            vec![market.clone()],
            SpotQuote {
                price: quote.price,
                source: quote.source,
            },
            start(),
        )),
        rounds_error: false,
    };
    bot.known.remember(&[market.clone()]);
    let mut states = Vec::new();
    for step in 0..29u64 {
        let ms = match step {
            23 => 110_000,
            27 => 275_000,
            28 => 300_000,
            _ => step * 5_000,
        };
        *clock.at.lock().unwrap() = now_at(ms);
        crate::bot::runtime::observer::freeze_paper_now(Some(now_at(ms).utc));
        if step == 24 && failures {
            bot.observed_quote = Some(Err("FIXTURE_SPOT_FAILURE"));
        } else {
            bot.observed_quote = Some(Ok(quote.clone()));
        }
        data.fail
            .store(step == 25 && failures, std::sync::atomic::Ordering::SeqCst);
        bot.rounds_error = step == 26 && failures;
        if bot.rounds_error {
            bot.last_scan = None;
        } else {
            bot.last_scan = Some(std::time::Instant::now());
        }
        // Repeat the accepted receipt at step23, then leave the selected set.
        if step == 27 {
            bot.rounds.clear();
        }
        let result = bot.tick(&ctx).await;
        if failures && (24..=26).contains(&step) {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        let state = bot.core.phase1_observation_state("BTCUSDT");
        states.push(serde_json::json!({
            "paper": format!("{:?}", ctx.broker.paper_snapshot()),
            "holdings": format!("{:?}", ctx.broker.holdings()),
            "halted": ctx.broker.is_halted(), "entered": bot.core.is_entered(200),
            "model": format!("{:?}", state), "next_decision": bot.core.next_decision_id,
            "last_check": format!("{:?}", bot.core.last_phase1_check),
            "market_revision": bot.core.market_revisions.get(&200).map(|r| r.load(std::sync::atomic::Ordering::SeqCst)),
        }));
    }
    assert!(bot.core.is_entered(200));
    let mut resolved = market.clone();
    resolved.open = false;
    resolved.end_price = Some(101.0);
    resolved.payout_yes = Some(1.0);
    let facts = resolved.round_facts.as_mut().unwrap();
    facts.oracle_status = Some("RESOLVED".into());
    facts.trading_status = Some("CLOSED".into());
    facts.yes_status = Some("WON".into());
    facts.no_status = Some("LOST".into());
    ctx.broker.controlled_settle(&resolved);
    ctx.broker.controlled_settle(&resolved);
    paper::emit_final(&ctx.broker, false);
    if let Some(observer) = bot.observer.as_mut() {
        observer.finish();
    }
    crate::bot::runtime::observer::freeze_paper_now(None);
    let events = paper::recorded_events();
    assert_eq!(
        events.iter().filter(|e| e["event_kind"] == "fill").count(),
        1
    );
    assert!(events
        .iter()
        .any(|e| e["source"]["decision"]["reason"] == "ALREADY_ENTERED"));
    assert!(ctx.broker.holdings().is_empty());
    serde_json::json!({"events": events, "states": states,
        "clock": format!("{:?}", clock.calls.lock().unwrap()),
        "spot_requests": bot.observed_symbols, "requests": *data.requests.lock().unwrap()})
}

#[tokio::test]
async fn real_tick_off_on_preserves_full_events_state_clock_and_requests() {
    for failures in [false, true] {
        let off = tick_scenario(None, failures).await;
        let (mut observer, parent, _) = producer_fixture();
        observer.send_ready();
        let on = tick_scenario(Some(observer), failures).await;
        assert_eq!(
            off, on,
            "full unfiltered trace/state including times, IDs and reasons"
        );
        parent.set_nonblocking(true).unwrap();
        let mut bytes = [0; MAX_FRAME_BYTES];
        let mut frames = Vec::new();
        while let Ok(size) = parent.recv(&mut bytes) {
            frames.push(serde_json::from_slice::<serde_json::Value>(&bytes[..size]).unwrap());
        }
        let records: Vec<_> = frames
            .iter()
            .filter_map(|frame| frame.get("record"))
            .collect();
        assert!(!records.is_empty());
        assert!(records.iter().any(|r| r["model_reason"] == "INPUT_CHANGED"));
        assert_eq!(records.last().unwrap()["sample_state"], "ROUND_ENDED");
        assert_eq!(
            records
                .iter()
                .filter(|r| r["sample_state"] == "ROUND_ENDED")
                .count(),
            1
        );
        if failures {
            assert!(records.iter().any(|r| r["model_reason"] == "MISSING_INPUT"));
            assert!(records.iter().any(|r| r["exit_reason"] == "MISSING_BOOK"));
        }
    }
}

#[tokio::test]
async fn post_entry_protocol_consumer_fixture() {
    // The exact fixture is a real child; only its external inputs are synthetic.
    // Terminate libtest's "test <name> ... " progress prefix, not an event.
    println!();
    let observer = PostEntryObserver::attach("observer-test", true);
    tick_scenario(observer, false).await;
}

/// Real END sender with 865 maximum-width legal token identities.
#[tokio::test]
async fn paper72_end_sender_fixture() {
    if std::env::var_os("BTC5M_TEST_STARTED_AT").is_none() {
        return;
    }
    println!();
    tick_scenario(None, false).await;
    let mut observer = PostEntryObserver::attach("observer-test", true).unwrap();
    for index in 1..=865 {
        let mut market = fixture_market();
        market.id = u64::MAX - index;
        let token = format!("{}{:04}", "1".repeat(74), index);
        observer.remember(&market, &token, SampleState::RoundEnded, index);
    }
    observer.counters = ProducerCounters {
        attempted_samples: 865,
        encoded_samples: 865,
        sent_samples: 865,
        last_observation_seq: 865,
        ..Default::default()
    };
    observer.finish();
    assert!(observer.disabled.is_none(), "{:?}", observer.disabled);
}

#[test]
fn paper72_end_and_sample_sender_limits() {
    let (mut producer, parent, _) = producer_fixture();
    // A 6h END retains the old byte bound before touching the socket.
    let frame = serde_json::json!({"kind": "END", "padding": "x".repeat(MAX_FRAME_BYTES)});
    assert!(!producer.send_frame(&frame));
    assert_eq!(producer.disabled, Some(DisabledReason::ObservationLimit));
    // SAMPLE never acquires the extended terminal capability.
    let frame = serde_json::json!({"kind": "SAMPLE", "padding": "x".repeat(MAX_FRAME_BYTES)});
    assert!(!producer.send_frame(&frame));
    parent.set_nonblocking(true).unwrap();
    assert!(parent.recv(&mut [0; 1]).is_err());
}

#[test]
fn paper72_native_sender_exact_boundary_fixture() {
    if std::env::var_os("BTC5M_TEST_STARTED_AT").is_none() {
        return;
    }
    let mut producer = PostEntryObserver::attach("run-1", true).unwrap();
    let empty = serde_json::json!({"kind": "END", "padding": ""});
    let width = serde_json::to_vec(&empty).unwrap().len() + 1;
    assert!(producer.send_frame(&serde_json::json!({"kind": "END",
        "padding": "x".repeat(PAPER72_END_BYTES - width)})));
    assert!(!producer.send_frame(&serde_json::json!({"kind": "END",
        "padding": "x".repeat(PAPER72_END_BYTES - width + 1)})));
    assert!(!producer.send_frame(&serde_json::json!({"kind": "SAMPLE",
        "padding": "x".repeat(MAX_FRAME_BYTES)})));
    assert_eq!(producer.disabled, Some(DisabledReason::ObservationLimit));
}
