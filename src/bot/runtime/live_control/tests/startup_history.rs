//! R18 exercises the original seed/account lane, never an injected eligibility flag.
use super::*;
use crate::bot::runtime::live;
use crate::service::observation::{Clock, FixtureClock};
use crate::service::predict_fun::account::startup_history_tests::{group_fixture, GroupTransport};
use crate::service::predict_fun::account::startup_history_tests::{
    history_fixture, HistoryTransport,
};
use crate::service::predict_fun::controlled::r16_fixture;
use crate::service::predict_fun::PredictFunTrader;
use crate::service::venue::{OrderVenue, TradableMarket};

async fn fixture(
    mode: &'static str,
) -> (
    Broker,
    Arc<FixtureClock>,
    Arc<PredictFunTrader>,
    Arc<HistoryTransport>,
) {
    let (trader, transport, _) = history_fixture(mode).await;
    let (mut broker, clock) = candidate_tests::risk_fixture();
    let budget = match mode {
        "history_claimed_win" => 85_480_000_000_000_000_000u128,
        "history_claimed_tie" => 90_430_000_000_000_000_000u128,
        _ => 95_380_000_000_000_000_000u128,
    };
    broker.identity = Some(trader.identity());
    broker.gate = Some(trader.gate.clone());
    let trader = Arc::new(trader);
    broker.taker = Some(trader.clone());
    broker.limit = Some(trader.clone());
    {
        let mut state = broker.state.lock();
        state.live.baseline = None;
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = trader.identity();
        run.options.params.budget = U256::from(budget);
        run.options.gas_policy.payer_address = trader.identity().signer_address;
        run.account_read = None;
    }
    (broker, clock, trader, transport)
}

#[tokio::test]
async fn startup_history_survives_ticks_markets_and_generations() {
    let (broker, clock, _, transport) = fixture("history_settled_zero").await;
    live::seed_predict(&broker).await.unwrap();
    let before = transport.counts();
    let first = broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .account_read
        .clone()
        .unwrap();
    for id in [2, 3] {
        clock.advance(65_001);
        let (market, book, _) = candidate_tests::evaluate_live_round_with_core(
            &clock,
            true,
            id,
            &mut crate::bot::arbitrage::DecisionCore::phase1(Default::default()),
        );
        broker.observe(&market, &book);
        // Current execution generations are deliberately not startup scope keys.
        broker.state.lock().live.generation.insert(900, id);
        live::check_runtime_account(&broker, false).await.unwrap();
        let state = broker.state.lock();
        let receipt = state
            .live
            .controlled
            .as_ref()
            .unwrap()
            .account_read
            .as_ref()
            .unwrap();
        assert!(receipt.checked.elapsed_ms > first.checked.elapsed_ms);
        assert!(state.live.claims.is_empty());
    }
    let after = transport.counts();
    assert_eq!((after.0, after.1), (before.0, before.1));
    assert_eq!(after.2 - before.2, 4);
    println!(
        "R18_BROKER seed={before:?} hot2={after:?} elapsed_ms={}",
        clock.now().unwrap().elapsed_ms
    );
}

#[tokio::test]
async fn failed_seed_never_activates_history() {
    for fault in ["second_position", "account_failure"] {
        let (broker, _, trader, transport) = fixture("history_settled_zero").await;
        // An independently successful read is only pending material.
        assert!(trader.account_snapshot().await.unwrap().positions_explained);
        *transport.fault.lock() = fault;
        assert!(live::seed_predict(&broker).await.is_err(), "{fault}");
        *transport.fault.lock() = "";
        let before = transport.counts();
        assert!(trader.account_snapshot().await.unwrap().positions_explained);
        assert!(
            transport.counts().0 > before.0,
            "failed seed must stay cold"
        );
        assert!(broker
            .state
            .lock()
            .live
            .controlled
            .as_ref()
            .unwrap()
            .control
            .lock()
            .stop
            .is_some());
    }
}

#[tokio::test]
async fn new_scope_starts_cold() {
    let (broker, _, trader, transport) = fixture("history_settled_zero").await;
    live::seed_predict(&broker).await.unwrap();
    for (run, window) in [("other-run", "risk-window"), ("other-run", "other-window")] {
        trader.begin_startup_history(run, window).unwrap();
        let before = transport.counts();
        assert!(trader.account_snapshot().await.unwrap().positions_explained);
        let after = transport.counts();
        assert!(after.0 > before.0 && after.1 > before.1);
    }
    let (_, _, second, second_transport) = fixture("history_settled_zero").await;
    assert!(second.account_snapshot().await.unwrap().positions_explained);
    assert!(second_transport.counts().0 > 0 && second_transport.counts().1 > 0);
}

#[tokio::test]
async fn opening_claims_are_not_credited_again() {
    for mode in [
        "history_claimed_win",
        "history_claimed_tie",
        "history_settled_zero",
    ] {
        let (broker, clock, _, transport) = fixture(mode).await;
        live::seed_predict(&broker).await.unwrap();
        let baseline = broker.state.lock().live.baseline.clone().unwrap();
        let opening = broker
            .state
            .lock()
            .live
            .controlled
            .as_ref()
            .unwrap()
            .opening_claim_cash;
        let before = transport.counts();
        for _ in 0..2 {
            clock.advance(65_001);
            live::check_runtime_account(&broker, false).await.unwrap();
            let state = broker.state.lock();
            assert_eq!(state.live.baseline.as_ref().unwrap(), &baseline);
            assert!(state.live.claims.is_empty() && state.live.pending_claims.is_empty());
            assert_eq!(
                state.live.controlled.as_ref().unwrap().opening_claim_cash,
                opening
            );
        }
        assert_eq!(transport.counts().0, before.0);
    }
}

async fn seeded_group() -> (
    Arc<Broker>,
    Arc<FixtureClock>,
    Arc<PredictFunTrader>,
    Arc<GroupTransport>,
    Arc<r16_fixture::Facts>,
) {
    let (trader, transport, facts) = group_fixture(true).await;
    let (mut broker, clock) = candidate_tests::risk_fixture();
    clock.set_utc(Utc::now() - chrono::Duration::minutes(10));
    broker.identity = Some(trader.identity());
    broker.gate = Some(trader.gate.clone());
    let trader = Arc::new(trader);
    broker.taker = Some(trader.clone());
    broker.limit = Some(trader.clone());
    {
        let mut state = broker.state.lock();
        state.live.baseline = None;
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = trader.identity();
        run.options.gas_policy.payer_address = trader.identity().signer_address;
        run.account_read = None;
    }
    live::seed_predict(&broker).await.unwrap();
    assert_eq!(
        broker
            .state
            .lock()
            .live
            .controlled
            .as_ref()
            .unwrap()
            .opening_claim_cash,
        transport.opening_cash()
    );
    (Arc::new(broker), clock, trader, transport, facts.unwrap())
}

async fn send(
    broker: &Arc<Broker>,
    clock: &Arc<FixtureClock>,
    facts: &r16_fixture::Facts,
    core: &mut crate::bot::arbitrage::DecisionCore,
    id: u64,
) -> TradableMarket {
    let (market, book, evaluation) =
        candidate_tests::evaluate_live_round_with_core(clock, true, id, core);
    facts.round(market.clone());
    let prior: Vec<_> = broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .traded
        .values()
        .cloned()
        .collect();
    for held in prior {
        broker.observe(&held, &book);
    }
    live::check_runtime_account(broker, false).await.unwrap();
    broker.observe(&market, &book);
    let candidate = evaluation.candidate.unwrap();
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    let ack = candidate_tests::ack_next(broker.clone(), false);
    let outcome = broker
        .take_live_candidate(crate::bot::runtime::taker::LiveCandidateRequest {
            market: &market,
            yes_book: &book,
            candidate: &candidate,
        })
        .await
        .unwrap();
    ack.join().unwrap();
    assert!(matches!(
        outcome.result,
        crate::bot::runtime::TakeResult::Filled { .. }
    ));
    live::sync_predict(broker).await.unwrap();
    market
}

fn resolved(mut market: TradableMarket) -> TradableMarket {
    market.open = false;
    market.end_price = Some(101.0);
    market.payout_yes = Some(1.0);
    market.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()),
        trading_status: Some("CLOSED".into()),
        yes_status: Some("WON".into()),
        no_status: Some("LOST".into()),
        decimal_precision: Some(2),
        price_feed_provider: Some("CHAINLINK".into()),
    });
    market
}

async fn current_claims(cash_first: bool) {
    let (broker, clock, trader, transport, facts) = seeded_group().await;
    let old = transport.counts();
    let opening = transport.opening_cash();
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0,
        ..Default::default()
    });
    for id in [1, 2] {
        clock.advance(65_001);
        let market = send(&broker, &clock, &facts, &mut core, id).await;
        let token = if id == 1 { 77 } else { 177 };
        facts.round(resolved(market));
        if cash_first {
            facts.pay(token);
            live::check_runtime_account(&broker, false).await.unwrap();
        } else {
            broker
                .settle_resolved(r16_fixture::data(&trader).as_ref())
                .await
                .unwrap();
        }
        {
            let state = broker.state.lock();
            assert_eq!(state.live.claims.len(), (id - 1) as usize);
            assert_eq!(
                state.live.controlled.as_ref().unwrap().control.lock().state,
                LiveRunState::WaitCash
            );
        }
        let reads = facts.rest_calls.lock().unwrap().len();
        if cash_first {
            broker
                .settle_resolved(r16_fixture::data(&trader).as_ref())
                .await
                .unwrap();
        } else {
            facts.pay(token);
            live::check_runtime_account(&broker, false).await.unwrap();
        }
        assert!(
            facts.rest_calls.lock().unwrap().len() > reads,
            "fresh account read after result publication"
        );
        {
            let state = broker.state.lock();
            assert_eq!(state.live.claims.len(), id as usize);
            assert!(state.live.pending_claims.is_empty());
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.control.lock().state, LiveRunState::Ready);
            assert_eq!(run.opening_claim_cash, opening);
        }
        clock.advance(300_000);
    }
    let after = transport.counts();
    assert_eq!(
        (after.0, after.1),
        (old.0, old.1),
        "current claims cannot expand startup members"
    );
    assert!(after.2 > old.2);
    broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
    broker
        .finish_controlled_live_with(
            r16_fixture::data(&trader).as_ref(),
            std::time::Duration::from_secs(1),
            std::time::Duration::ZERO,
        )
        .await;
    assert!(broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .emitter
        .is_finished());
    println!("R18_CURRENT_CLAIMS cash_first={cash_first} startup={old:?} final={after:?}");
}

#[tokio::test]
async fn startup_history_keeps_current_claim_cash_checks() {
    current_claims(false).await;
}

#[tokio::test]
async fn native_startup_history_cash_first() {
    current_claims(true).await;
}

#[tokio::test]
async fn native_startup_history_sticky_stop() {
    let (broker, clock, trader, _, facts) = seeded_group().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0,
        ..Default::default()
    });
    let market = send(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market));
    facts.pay(77);
    *facts.cash_offset.lock().unwrap() = 1;
    broker
        .settle_resolved(r16_fixture::data(&trader).as_ref())
        .await
        .unwrap();
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    let first = broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .control
        .lock()
        .stop
        .clone()
        .unwrap();
    *facts.cash_offset.lock().unwrap() = 0;
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(
            run.control.lock().stop.as_ref().unwrap().reason,
            first.reason
        );
        assert!(!run.may_add_risk());
        assert_eq!(state.live.claims.len(), 1);
    }
    broker
        .finish_controlled_live_with(
            r16_fixture::data(&trader).as_ref(),
            std::time::Duration::from_secs(1),
            std::time::Duration::ZERO,
        )
        .await;
    assert!(broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .emitter
        .is_finished());
}

#[tokio::test]
async fn failed_startup_flush_fixture() {
    if std::env::var("BTC5M_R18_FLUSH_PROBE").as_deref() != Ok("1") {
        return;
    }
    let (broker, _, trader, transport) = fixture("history_settled_zero").await;
    // The parent closes only this child's stdout after the harness has started.
    eprintln!("R18_FLUSH_READY");
    let mut ready = [0];
    std::io::Read::read_exact(&mut std::io::stdin(), &mut ready).unwrap();
    assert!(live::seed_predict(&broker).await.is_err());
    assert!(
        broker.state.lock().live.baseline.is_some(),
        "the original baseline precedes flush"
    );
    assert!(broker
        .state
        .lock()
        .live
        .controlled
        .as_ref()
        .unwrap()
        .output_failed
        .load(Ordering::SeqCst));
    let before = transport.counts();
    assert!(trader.account_snapshot().await.unwrap().positions_explained);
    assert!(
        transport.counts().0 > before.0,
        "failed flush cannot commit pending history"
    );
    eprintln!("R18_FLUSH_ASSERTIONS_PASSED");
}

#[test]
fn failed_startup_flush_never_commits() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bot::runtime::live_control::startup_history::failed_startup_flush_fixture",
            "--nocapture",
        ])
        .env("BTC5M_R18_FLUSH_PROBE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = std::io::BufReader::new(child.stderr.take().unwrap());
    let mut prefix = String::new();
    loop {
        let mut line = String::new();
        assert!(
            stderr.read_line(&mut line).unwrap() > 0,
            "child did not reach original seed"
        );
        prefix.push_str(&line);
        if line.contains("R18_FLUSH_READY") {
            break;
        }
    }
    drop(child.stdout.take());
    child.stdin.take().unwrap().write_all(b"1").unwrap();
    let mut remainder = String::new();
    std::io::Read::read_to_string(&mut stderr, &mut remainder).unwrap();
    let status = child.wait().unwrap();
    assert!(
        remainder.contains("R18_FLUSH_ASSERTIONS_PASSED"),
        "{prefix}{remainder}"
    );
    println!("R18_EXPECTED_BROKEN_PIPE child_exit={status} assertions=passed");
}

#[tokio::test]
async fn current_claim_failures_are_not_hidden_by_eleven_history_hits() {
    for fault in [
        "unburned",
        "unpaid",
        "plus_atom",
        "minus_atom",
        "wrong_inventory",
    ] {
        let (broker, clock, trader, transport, facts) = seeded_group().await;
        let before = transport.counts();
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0,
            ..Default::default()
        });
        let market = send(&broker, &clock, &facts, &mut core, 1).await;
        facts.round(resolved(market));
        if fault != "unpaid" {
            facts.pay(77);
        }
        if fault == "unburned" {
            let held = broker.state.lock().live.net_inventory[&1];
            facts.position_overrides.lock().unwrap().insert(77, held);
        }
        if fault == "wrong_inventory" {
            facts
                .position_overrides
                .lock()
                .unwrap()
                .insert(77, U256::from(1));
        }
        if matches!(fault, "plus_atom" | "minus_atom") {
            *facts.cash_offset.lock().unwrap() = if fault == "plus_atom" { 1 } else { -1 };
        }
        broker
            .settle_resolved(r16_fixture::data(&trader).as_ref())
            .await
            .unwrap();
        if fault == "unpaid" {
            assert_eq!(
                broker
                    .state
                    .lock()
                    .live
                    .controlled
                    .as_ref()
                    .unwrap()
                    .control
                    .lock()
                    .state,
                LiveRunState::WaitCash
            );
        } else {
            // Settlement records its stop; the original account consumer owns
            // the typed failure rather than settle_resolved's completion value.
            assert!(
                live::check_runtime_account(&broker, false).await.is_err(),
                "{fault}"
            );
            assert!(
                broker
                    .state
                    .lock()
                    .live
                    .controlled
                    .as_ref()
                    .unwrap()
                    .control
                    .lock()
                    .stop
                    .is_some(),
                "{fault}"
            );
            *facts.cash_offset.lock().unwrap() = 0;
            facts.position_overrides.lock().unwrap().clear();
            let _ = live::check_runtime_account(&broker, false).await;
            assert!(
                broker
                    .state
                    .lock()
                    .live
                    .controlled
                    .as_ref()
                    .unwrap()
                    .control
                    .lock()
                    .stop
                    .is_some(),
                "hard stop cannot be cleared"
            );
        }
        if fault == "unpaid" {
            assert!(broker.state.lock().live.claims.is_empty());
        }
        assert_eq!(
            (transport.counts().0, transport.counts().1),
            (before.0, before.1)
        );
    }
}
