//! R16 regressions on the existing ledger; transport/drive cases extend this module.
use super::*;
use crate::bot::runtime::live;
use crate::models::VenueId;
use crate::service::venue::{AccountSnapshot, AttemptPhase, BoundClaimEvidence, Closure, OrderTicket,
    OrderVenue, RuntimeAccountObservation, RuntimeClaimInventory};

struct ExactAccount {
    snapshot: AccountSnapshot,
    proof: BoundClaimEvidence,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl OrderVenue for ExactAccount {
    fn id(&self) -> VenueId { VenueId::PredictFun }
    async fn place(&self, _: &crate::models::PlannedOrder, _: u64, _: bool)
        -> anyhow::Result<OrderTicket> { panic!("read-only fixture cannot send") }
    async fn account_snapshot(&self) -> anyhow::Result<AccountSnapshot> {
        Ok(self.snapshot.clone())
    }
    async fn runtime_account_observation(&self, _: &[RuntimeClaimInventory])
        -> anyhow::Result<RuntimeAccountObservation> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(RuntimeAccountObservation {
            snapshot: self.snapshot.clone(), bound_claims: vec![self.proof.clone()],
        })
    }
}

fn exact_account(broker: &Broker) -> ExactAccount {
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let record = state.live.attempts[&1].lock();
    let inventory = state.live.net_inventory[&1];
    let market = &run.traded[&record.identity.market_id];
    let mut snapshot = state.live.baseline.clone().unwrap();
    snapshot.collateral_atoms = Some(snapshot.collateral_atoms.unwrap()
        - state.live.net_cash[&1] + inventory);
    snapshot.positions.clear();
    ExactAccount { snapshot, reads: Default::default(), proof: BoundClaimEvidence {
        identity: record.identity.clone(), execution_generation: state.live.generation[&1],
        yes_token: market.yes_token.clone(), no_token: market.no_token.clone(),
        inventory_atoms: inventory, claim_atoms: inventory, payout_millionths: 1_000_000,
    }}
}

#[tokio::test]
async fn cash_first_requires_a_fresh_read_after_the_result_before_credit_or_resume() {
    let (mut broker, result) = candidate_tests::official_result_fixture(1.0);
    let account = exact_account(&broker);
    let reads = account.reads.clone();
    broker.taker = Some(Arc::new(account));
    live::check_runtime_account(&broker, false).await.unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(state.live.claims.is_empty(), "unconsumed official result cannot credit cash");
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
    }
    // The settlement lane itself re-reads after publishing the result. Count
    // actual reads: simply promoting the saved tuple would leave this at one.
    broker.settle_resolved(&result).await.unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.attempt_outcomes[&1].cash_confirmed);
    assert_eq!(run.control.lock().state, LiveRunState::Ready);
    assert!(state.live.pending_claims.is_empty());
    assert_eq!(state.live.claims.len(), 1);
}

use crate::service::predict_fun::controlled::r16_fixture;
use crate::service::venue::{MarketData, MarketQuery, TradableMarket};
use crate::service::observation::{Clock, FixtureClock};

async fn adapter_fixture() -> (Arc<Broker>, Arc<FixtureClock>,
    Arc<crate::service::predict_fun::PredictFunTrader>,
    Arc<crate::service::predict_fun::account::tests::SyntheticRpc>, Arc<r16_fixture::Facts>) {
    let (mut broker, clock) = candidate_tests::risk_fixture();
    clock.set_utc(Utc::now() - chrono::Duration::minutes(10));
    let (trader, transport, _, facts) = r16_fixture::trader();
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
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    (Arc::new(broker), clock, trader, transport, facts)
}

fn resolved(mut market: TradableMarket, payout: f64) -> TradableMarket {
    market.open = false;
    market.end_price = Some(if payout == 1.0 { 101.0 } else if payout == 0.0 { 99.0 } else { 100.0 });
    market.payout_yes = Some(payout);
    market.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some(if payout == 0.5 { "DRAW" } else if payout == 1.0 { "WON" } else { "LOST" }.into()),
        no_status: Some(if payout == 0.5 { "DRAW" } else if payout == 0.0 { "WON" } else { "LOST" }.into()),
        decimal_precision: Some(2), price_feed_provider: None,
    });
    market
}

async fn send_round(broker: &Arc<Broker>, clock: &Arc<FixtureClock>, facts: &r16_fixture::Facts,
    core: &mut crate::bot::arbitrage::DecisionCore, id: u64) -> TradableMarket {
    let (market, book, evaluation) = candidate_tests::evaluate_live_round_with_core(clock, true, id, core);
    facts.round(market.clone());
    let prior: Vec<_> = broker.state.lock().live.controlled.as_ref().unwrap().traded.values().cloned().collect();
    for held in prior { broker.observe(&held, &book); }
    live::check_runtime_account(broker, false).await.unwrap();
    broker.observe(&market, &book);
    let candidate = evaluation.candidate.unwrap();
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    let ack = candidate_tests::ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(crate::bot::runtime::taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate,
    }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, crate::bot::runtime::TakeResult::Filled { .. }));
    live::sync_predict(broker).await.unwrap();
    market
}

#[tokio::test]
async fn closed_reread_preserves_final_without_reapplying_or_stopping() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    let (hash, generation, quantity, cash, inventory) = {
        let state = broker.state.lock();
        let record = state.live.attempts[&1].lock();
        assert_eq!(record.phase, AttemptPhase::Closed);
        (record.identity.signed_hash.clone().unwrap(), state.live.generation[&1],
            state.live.applied_q[&1], state.live.net_cash[&1], state.live.net_inventory[&1])
    };
    *facts.miss_receipt.lock().unwrap() = true;
    let reading = trader.execution_snapshot(&hash).await.unwrap();
    assert!(matches!(reading.closure, Closure::Unproven));
    assert_eq!(reading.gross_q(), Some(quantity));
    assert!(!reading.pages_complete && !reading.accounting_complete());
    let lane = broker.live_operation_lane().unwrap();
    let operation = lane.lock().await;
    let applied = live::consume_snapshot(&broker, &reading).unwrap();
    assert!(!applied.applied && !applied.accounting_complete);
    assert_eq!((applied.net_cash_delta, applied.net_inventory_delta), (None, None));
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(!run.must_end(), "a degraded re-read must not stop a proven Final");
        assert!(!state.halted);
        assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Closed);
        assert_eq!(state.live.reservations[&1], U256::ZERO);
        assert_eq!((state.live.generation[&1], state.live.net_cash[&1], state.live.net_inventory[&1]),
            (generation, cash, inventory));
    }
    // Same-generation exact replay is idempotent, including on the wire.
    assert!(live::consume_snapshot(&broker, &reading).unwrap().duplicate);
    live::check_runtime_account(&broker, false).await.unwrap();
    drop(operation);
    broker.sync_live().await.unwrap();
    assert_eq!(broker.state.lock().live.generation[&1], generation);
    // Prove and book a real, exact claim while the last validated read generation
    // is newer than the execution generation used for payout ownership.
    facts.round(resolved(market, 1.0));
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    facts.pay(77);
    live::check_runtime_account(&broker, false).await.unwrap();
    assert_eq!(broker.state.lock().live.claims[&1], (inventory, inventory));
    *facts.miss_receipt.lock().unwrap() = false;
    broker.sync_live().await.unwrap();
    {
        let state = broker.state.lock();
        assert!(!state.live.controlled.as_ref().unwrap().must_end());
        assert_eq!((state.live.net_cash[&1], state.live.net_inventory[&1]), (cash, inventory));
        assert_eq!(state.live.claims.len(), 1);
    }
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
    broker.finish_controlled_live_with(r16_fixture::data(&trader).as_ref(),
        std::time::Duration::from_secs(1), std::time::Duration::ZERO).await;
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished());
}

#[tokio::test]
async fn closed_reread_still_refuses_conflicting_or_unread_facts() {
    use crate::service::venue::{Field, Quantity, Submission};
    for case in ["reopen", "new_final", "fee", "uncovered", "scan_race", "regression",
        "same_generation", "unknown_quantity", "identity", "submission", "quantity_increase"]
    {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        send_round(&broker, &clock, &facts, &mut core, 1).await;
        let (hash, generation, quantity, cash, inventory) = {
            let state = broker.state.lock();
            let record = state.live.attempts[&1].lock();
            (record.identity.signed_hash.clone().unwrap(), state.live.generation[&1],
                state.live.applied_q[&1], state.live.net_cash[&1], state.live.net_inventory[&1])
        };
        let lane = broker.live_operation_lane().unwrap();
        let _operation = lane.lock().await;
        *facts.miss_receipt.lock().unwrap() = true;
        let mut reading = trader.execution_snapshot(&hash).await.unwrap();
        live::consume_snapshot(&broker, &reading).unwrap();
        if case != "same_generation" { reading.generation += 1; }
        let one = U256::from(1);
        match case {
            "reopen" => reading.closure = Closure::Open,
            "new_final" => reading.closure = Closure::Final { evidence: "RPC_FIXED_BLOCK:0x42".into(),
                final_gross_q: quantity + one },
            "fee" | "same_generation" => reading.accounting.collateral_fee =
                Field::Known(*reading.accounting.collateral_fee.value().unwrap() + one),
            "uncovered" => reading.accounting_gross_q = None,
            "scan_race" => reading.gross_q_before -= one,
            "regression" => reading.generation = generation,
            "unknown_quantity" => reading.quantity = Quantity::Unknown,
            "identity" => reading.identity.token_id = "999".into(),
            "submission" => reading.submission = Submission::Unknown,
            "quantity_increase" => if let Quantity::Known { gross_q_atoms, .. } = &mut reading.quantity {
                *gross_q_atoms += one;
            },
            _ => unreachable!(),
        }
        assert!(live::consume_snapshot(&broker, &reading).is_err(), "{case}");
        let state = broker.state.lock();
        assert!(state.live.controlled.as_ref().unwrap().must_end(), "{case}");
        assert_eq!((state.live.generation[&1], state.live.net_cash[&1], state.live.net_inventory[&1]),
            (generation, cash, inventory), "{case}");
        assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Closed);
        assert_eq!(state.live.reservations[&1], U256::ZERO);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn closed_reread_unconsumed_generation_refuses_observation_and_commit() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    send_round(&broker, &clock, &facts, &mut core, 1).await;
    let frozen = {
        let state = broker.state.lock();
        let identity = state.live.attempts[&1].lock().identity.clone();
        let generation = state.live.generation[&1];
        RuntimeClaimInventory { identity, execution_generation: generation,
            validated_read_generation: generation, inventory_atoms: state.live.net_inventory[&1] }
    };
    let batch = &[frozen];
    assert!(trader.runtime_account_observation(batch).await.unwrap().snapshot.complete);
    *facts.miss_receipt.lock().unwrap() = true;
    let _unconsumed = trader.execution_snapshot(batch[0].identity.signed_hash.as_deref().unwrap())
        .await.unwrap();
    let mut committed = false;
    let reject = trader.with_runtime_claim_read_fence(batch, &mut || { committed = true; Ok(()) });
    assert!(reject.unwrap_err().to_string().contains("CLAIM_GENERATION"));
    assert!(!committed);
    assert_eq!(live::check_runtime_account(&broker, false).await.unwrap_err().to_string(),
        "CLAIM_GENERATION");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.must_end() && run.account_read.is_none() && state.live.claims.is_empty());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

async fn actual_adapter_two_rounds(cash_first: bool) {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 1.0));
    if cash_first { facts.pay(77); }
    if cash_first {
        live::check_runtime_account(&broker, false).await.unwrap();
        let state = broker.state.lock();
        assert!(state.live.claims.is_empty());
        assert_eq!(state.live.pending_claims.len(), 1);
    } else {
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    }
    let started = broker.state.lock().live.controlled.as_ref().unwrap().cash_wait.as_ref().unwrap().started;
    for _ in 0..2 {
        live::check_runtime_account(&broker, false).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
        assert!(!run.must_end() && !run.may_add_risk());
        assert_eq!(run.cash_wait.as_ref().unwrap().started, started);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    }
    clock.advance(5_407);
    if cash_first { broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap(); }
    else { facts.pay(77); live::check_runtime_account(&broker, false).await.unwrap(); }
    assert_eq!(broker.state.lock().live.controlled.as_ref().unwrap().control.lock().state, LiveRunState::Ready);
    clock.advance(300_000);
    let second = send_round(&broker, &clock, &facts, &mut core, 2).await;
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    facts.round(resolved(second, 1.0));
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    facts.pay(177);
    live::check_runtime_account(&broker, false).await.unwrap();
    let state = broker.state.lock();
    assert_eq!(state.live.claims.len(), 2);
    assert!(state.live.pending_claims.is_empty());
    assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
    drop(state);
    broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
    broker.finish_controlled_live_with(r16_fixture::data(&trader).as_ref(),
        std::time::Duration::from_secs(1), std::time::Duration::ZERO).await;
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished());
}

#[tokio::test]
async fn actual_adapter_result_first_cash_then_second_real_post() {
    actual_adapter_two_rounds(false).await;
}

// Keep the same Broker, native trader, gate and DecisionCore across settlement,
// the failed official GET and the next real (synthetic-transport) submission.
async fn settlement_read_recovers(fault: &'static str, payout: f64) {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, payout));
    if payout == 0.0 { facts.hidden.lock().unwrap().insert(77); }
    else { facts.pay(77); }
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    live::check_runtime_account(&broker, false).await.unwrap();
    let claims = broker.state.lock().live.claims.clone();
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
    *facts.market_fault.lock().unwrap() = Some(fault);
    let error = live::check_runtime_account(&broker, false).await.unwrap_err();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(!run.must_end(), "one official GET failure must degrade, not stop: {error}");
        assert!(run.degraded() && !run.may_add_risk());
        assert!(run.control.lock().stop.is_none());
        assert!(run.account_read.is_none());
        assert_eq!(state.live.claims, claims, "failed read cannot credit the claim twice");
    }
    let venue = error.downcast_ref::<crate::service::venue::VenueError>().unwrap();
    assert_eq!(venue.code, crate::service::venue::VenueErrorCode::ReadUnavailable);
    assert!(venue.reason.starts_with(if fault.starts_with("category_") {
        "SETTLEMENT_CATEGORY_READ_UNAVAILABLE"
    } else { "RUNTIME_ACCOUNT_READ_UNAVAILABLE" }));
    assert!(!format!("{error:?}").contains("SECRET_SENTINEL"));
    let since = broker.state.lock().live.controlled.as_ref().unwrap().read_failures.as_ref().unwrap().since;
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.read_failures.as_ref().unwrap().since, since);
        assert_eq!(run.read_failures.as_ref().unwrap().count, 2);
        assert!(!run.must_end() && !run.may_add_risk());
    }
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    *facts.market_fault.lock().unwrap() = None;
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.may_add_risk() && !run.degraded() && run.read_failures.is_none());
        assert_eq!(state.live.claims, claims);
    }
    clock.advance(300_000);
    let second = send_round(&broker, &clock, &facts, &mut core, 2).await;
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    facts.round(resolved(second, 1.0));
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    facts.pay(177);
    live::check_runtime_account(&broker, false).await.unwrap();
    broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
    broker.finish_controlled_live_with(r16_fixture::data(&trader).as_ref(),
        std::time::Duration::from_secs(1), std::time::Duration::ZERO).await;
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished());
}

#[tokio::test]
async fn settlement_http503_recovers_and_trades_again() {
    settlement_read_recovers("http503", 1.0).await;
}

#[tokio::test]
async fn category_http429_account_recovers_and_trades_again() {
    settlement_read_recovers("category_http429", 0.0).await;
}

#[tokio::test]
async fn settlement_consumer_typed_faults_degrade_instead_of_stopping() {
    for fault in ["http429", "http503", "transport", "category_http429",
                  "category_http503", "category_transport"] {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        facts.round(resolved(market, 0.0));
        facts.hidden.lock().unwrap().insert(77);
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
        live::check_runtime_account(&broker, false).await.unwrap();
        *facts.market_fault.lock().unwrap() = Some(fault);
        let error = broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.err().unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(!run.must_end(), "official settlement read must degrade: {fault}: {error}");
        assert!(run.degraded() && !run.may_add_risk());
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn settlement_other_typed_read_faults_recover_after_paid_or_zero_outcome() {
    for fault in ["http401", "http429", "transport", "category_http429", "category_http503", "category_transport"] {
        for payout in [0.0, 1.0] { settlement_read_recovers(fault, payout).await; }
    }
}

#[tokio::test]
async fn settlement_invalid_data_and_unknown_errors_still_stop() {
    for (fault, detail) in [("schema", "RUNTIME_ACCOUNT_FACT_INCOMPLETE"), ("raw", "UNKNOWN")] {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        facts.round(resolved(market, 1.0));
        facts.pay(77);
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
        *facts.market_fault.lock().unwrap() = Some(fault);
        let error = live::check_runtime_account(&broker, false).await.unwrap_err();
        let reason = error.downcast_ref::<crate::service::venue::VenueError>()
            .map(|venue| venue.reason.clone()).unwrap_or_else(|| error.to_string());
        assert_eq!(reason, detail, "safe error projection");
        assert!(!format!("{error:?}").contains("SECRET_SENTINEL"));
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.must_end() && !run.may_add_risk());
        let stop = run.control.lock().stop.clone().unwrap();
        assert_eq!(stop.reason, "RECONCILIATION_REQUIRED");
        assert_eq!(stop.detail.as_deref(), Some(detail));
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn settlement_read_exhaustion_remains_sticky_after_a_valid_read() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 1.0));
    facts.pay(77);
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    *facts.market_fault.lock().unwrap() = Some("http503");
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    {
        let mut state = broker.state.lock();
        let run = state.live.controlled.as_mut().unwrap();
        assert!(!run.must_end());
        // Only the existing streak's input clock is injected; no real wait or
        // production budget change. Equal/exceeded existing budget must stop.
        run.read_failures.as_mut().unwrap().since = Utc::now()
            - chrono::Duration::from_std(READ_FAILURE_BUDGET).unwrap();
    }
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().must_end());
    *facts.market_fault.lock().unwrap() = None;
    let _ = live::check_runtime_account(&broker, false).await;
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.must_end() && !run.may_add_risk());
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "READ_UNAVAILABLE");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn actual_adapter_cash_first_result_then_second_real_post() {
    actual_adapter_two_rounds(true).await;
}

#[tokio::test]
async fn unburned_loss_native_account_requires_exact_or_zero_balance_and_official_zero_payout() {
    for (payout, balance_kind, accepted) in [
        (Some(0.0), "exact", true), (Some(0.0), "zero", true),
        (Some(0.0), "short", false), (Some(0.0), "excess", false),
        (Some(1.0), "exact", false), (Some(0.5), "exact", false),
        (None, "exact", false), (None, "zero", false),
    ] {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        let inventory = {
            let state = broker.state.lock();
            let identity = state.live.attempts[&1].lock().identity.clone();
            RuntimeClaimInventory { identity,
                execution_generation: state.live.generation[&1], validated_read_generation: state.live.generation[&1],
                inventory_atoms: state.live.net_inventory[&1] }
        };
        assert!(!inventory.inventory_atoms.is_zero());
        let balance = match balance_kind {
            "zero" => U256::ZERO,
            "short" => inventory.inventory_atoms - U256::from(1),
            "excess" => inventory.inventory_atoms + U256::from(1),
            _ => inventory.inventory_atoms,
        };
        facts.hidden.lock().unwrap().insert(77);
        facts.position_overrides.lock().unwrap().insert(77, balance);
        if let Some(payout) = payout { facts.round(resolved(market, payout)); }
        let observation = trader.runtime_account_observation(&[inventory.clone()]).await;
        let zero_proven = observation.as_ref().is_ok_and(|value| !value.snapshot.settled_zero.is_empty());
        assert_eq!(zero_proven, accepted, "{payout:?}/{balance_kind}: {observation:?}");
        if !accepted && !(payout.is_none() && balance_kind == "zero") {
            assert!(observation.is_err(), "{payout:?}/{balance_kind} must not explain a claim");
        }
        if accepted {
            let observation = observation.unwrap();
            assert!(observation.snapshot.complete && observation.snapshot.positions_explained);
            assert!(observation.snapshot.positions.is_empty());
            assert_eq!(observation.snapshot.settled_zero.len(), 1);
            assert!(observation.bound_claims.is_empty(), "zero payout is not a cash claim");
            for field in ["generation", "identity"] {
                let mut wrong = inventory.clone();
                if field == "generation" { wrong.validated_read_generation += 1; }
                else { wrong.identity.market_id += 1; }
                assert!(trader.runtime_account_observation(&[wrong]).await.is_err());
            }
        }
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn unburned_loss_original_broker_closes_once_and_allows_second_round() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 0.0));
    facts.hidden.lock().unwrap().insert(77);
    // REST removes a losing outcome without redeeming/burning its ERC-1155 balance.
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        assert!(state.live.claims.is_empty(), "adapter evidence cannot credit before official consumption");
        assert!(state.live.pending_claims.is_empty(), "zero payout is not a pending cash claim");
        assert!(state.live.controlled.as_ref().unwrap().attempt_outcomes.is_empty());
    }
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    for _ in 0..2 {
        live::check_runtime_account(&broker, false).await.unwrap();
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    }
    {
        let state = broker.state.lock();
        assert!(state.live.claims.is_empty(), "loss must never credit cash");
        assert!(state.live.pending_claims.is_empty());
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.attempt_outcomes.len(), 1);
        assert_eq!(run.attempt_outcomes[&1].claim_value_atoms, Some(U256::ZERO));
        assert!(run.may_add_risk());
    }
    assert!(!facts.paid.lock().unwrap().contains(&77), "fixture did not burn or redeem");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    clock.advance(300_000);
    send_round(&broker, &clock, &facts, &mut core, 2).await;
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn settlement_and_account_streaks_do_not_clear_each_other_or_renew() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 0.0));
    facts.hidden.lock().unwrap().insert(77);
    let data = r16_fixture::data(&trader);
    broker.settle_resolved(data.as_ref()).await.unwrap();
    *facts.market_fault.lock().unwrap() = Some("category_http429");
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    assert!(broker.settle_resolved(data.as_ref()).await.is_err());
    let since = broker.state.lock().live.controlled.as_ref().unwrap()
        .settlement_failures.unwrap().since;
    *facts.market_fault.lock().unwrap() = None;
    live::check_runtime_account(&broker, false).await.unwrap();
    assert_eq!(broker.state.lock().live.controlled.as_ref().unwrap()
        .settlement_failures.unwrap().since, since);
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
    *facts.market_fault.lock().unwrap() = Some("category_http503");
    assert!(broker.settle_resolved(data.as_ref()).await.is_err());
    assert_eq!(broker.state.lock().live.controlled.as_ref().unwrap()
        .settlement_failures.unwrap().since, since);
    *facts.market_fault.lock().unwrap() = None;
    broker.settle_resolved(data.as_ref()).await.unwrap();
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
    *facts.market_fault.lock().unwrap() = Some("category_http429");
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    *facts.market_fault.lock().unwrap() = None;
    broker.settle_resolved(data.as_ref()).await.unwrap();
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().read_failures.is_some());
    live::check_runtime_account(&broker, false).await.unwrap();
    *facts.market_fault.lock().unwrap() = Some("category_http429");
    assert!(broker.settle_resolved(data.as_ref()).await.is_err());
    broker.state.lock().live.controlled.as_mut().unwrap().settlement_failures.as_mut().unwrap()
        .since = Utc::now() - chrono::Duration::from_std(READ_FAILURE_BUDGET).unwrap();
    assert!(broker.settle_resolved(data.as_ref()).await.is_err());
    *facts.market_fault.lock().unwrap() = None;
    broker.settle_resolved(data.as_ref()).await.unwrap();
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.must_end() && !run.may_add_risk());
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "READ_UNAVAILABLE");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn category_invalid_success_stays_fatal_and_optional_market_is_unchanged() {
    for fault in ["schema", "raw", "category_schema", "category_missing"] {
        let (broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        facts.round(resolved(market, 0.0));
        facts.hidden.lock().unwrap().insert(77);
        let data = r16_fixture::data(&trader);
        broker.settle_resolved(data.as_ref()).await.unwrap();
        *facts.market_fault.lock().unwrap() = Some("category_http429");
        assert!(data.market(1).await.is_ok(), "ordinary optional category fallback is preserved");
        *facts.market_fault.lock().unwrap() = Some(fault);
        if let Err(error) = broker.settle_resolved(data.as_ref()).await {
            assert!(!format!("{error:?}").contains("SECRET_SENTINEL"));
        }
        assert!(broker.state.lock().live.controlled.as_ref().unwrap().must_end());
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    }
}

struct SettlementRecoveryDrive {
    core: crate::bot::arbitrage::DecisionCore,
    clock: Arc<FixtureClock>,
    facts: Arc<r16_fixture::Facts>,
    submitted: usize,
}

#[async_trait::async_trait]
impl crate::bot::runtime::Strategy for SettlementRecoveryDrive {
    fn name(&self) -> &'static str { "settlement-category-drive" }
    async fn tick(&mut self, ctx: &crate::bot::runtime::Ctx) -> anyhow::Result<()> {
        if self.submitted == 0 {
            let market = send_round(&ctx.broker, &self.clock, &self.facts, &mut self.core, 1).await;
            self.facts.round(resolved(market, 0.0));
            self.facts.hidden.lock().unwrap().insert(77);
            ctx.broker.settle_resolved(ctx.data.as_ref()).await?;
            live::check_runtime_account(&ctx.broker, false).await?;
            self.submitted = 1;
            *self.facts.market_fault.lock().unwrap() = Some("category_http429");
        } else if self.facts.market_fault.lock().unwrap().is_none() {
            self.clock.advance(300_000);
            let market = send_round(&ctx.broker, &self.clock, &self.facts, &mut self.core, 2).await;
            self.facts.round(resolved(market, 1.0));
            ctx.broker.settle_resolved(ctx.data.as_ref()).await?;
            self.facts.pay(177);
            live::check_runtime_account(&ctx.broker, false).await?;
            self.submitted = 2;
            self.clock.advance(3_600_000);
        }
        Ok(())
    }
}

#[tokio::test]
async fn original_drive_category_fault_recovers_without_restart_or_duplicate_post() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut strategy = SettlementRecoveryDrive {
        core: crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        }), clock: clock.clone(), facts: facts.clone(), submitted: 0,
    };
    let mut cfg = candidate_tests::ctx_fixture(broker.identity.as_ref().unwrap());
    cfg.strategies.poll_secs = 1;
    let client = r16_fixture::data(&trader);
    let ctx = crate::bot::runtime::Ctx { cfg, client: client.clone(), data: client, broker: broker.clone() };
    let driving = crate::bot::runtime::drive(&mut strategy, &ctx);
    tokio::pin!(driving);
    tokio::select! {
        result = &mut driving => panic!("category failure terminated original drive: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(61_100)) => (),
    }
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.settlement_failures.is_some() && run.degraded());
        assert!(!run.must_end() && !run.may_add_risk());
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }
    *facts.market_fault.lock().unwrap() = None;
    // Exercise recovery through the original consumer, without resetting the drive/core.
    broker.settle_resolved(ctx.data.as_ref()).await.unwrap();
    live::check_runtime_account(&broker, false).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut driving)
        .await.expect("same drive recovers and closes at original deadline").unwrap();
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DEADLINE");
}

struct NextRound {
    core: crate::bot::arbitrage::DecisionCore,
    clock: Arc<FixtureClock>,
    facts: Arc<r16_fixture::Facts>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::bot::runtime::Strategy for NextRound {
    fn name(&self) -> &'static str { "r16-original-drive" }
    async fn tick(&mut self, ctx: &crate::bot::runtime::Ctx) -> anyhow::Result<()> {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0,
            "waiting drive must not run a strategy, and the original deadline ends after round two");
        self.clock.advance(300_000);
        let market = send_round(&ctx.broker, &self.clock, &self.facts, &mut self.core, 2).await;
        self.facts.round(resolved(market, 1.0));
        ctx.broker.settle_resolved(ctx.data.as_ref()).await?;
        self.facts.pay(177);
        live::check_runtime_account(&ctx.broker, false).await?;
        // Advance the existing controlled clock to its original deadline. The
        // drive, not this strategy, performs cleanup/final and returns.
        self.clock.advance(3_600_000);
        Ok(())
    }
}

async fn original_drive_cash_recovery(cash_first: bool) {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default()
    });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 1.0));
    if cash_first {
        facts.pay(77);
        live::check_runtime_account(&broker, false).await.unwrap();
    } else {
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut strategy = NextRound { core, clock: clock.clone(), facts: facts.clone(), calls: calls.clone() };
    let mut cfg = candidate_tests::ctx_fixture(broker.identity.as_ref().unwrap());
    cfg.strategies.poll_secs = 1;
    let client = r16_fixture::data(&trader);
    let ctx = crate::bot::runtime::Ctx { cfg, client: client.clone(), data: client, broker: broker.clone() };
    let driving = crate::bot::runtime::drive(&mut strategy, &ctx);
    tokio::pin!(driving);
    tokio::select! {
        result = &mut driving => panic!("cash wait prematurely returned: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(2_100)) => (),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished());
    facts.pay(77);
    clock.advance(5_407);
    // Cash-first preserves the original 60s settlement cadence. This offline
    // test waits for that real cadence, not a shortened production constant.
    tokio::time::timeout(std::time::Duration::from_secs(75), &mut driving)
        .await.expect("original drive must recover, post the next round, and finish").unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(state.live.claims.len(), 2);
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DEADLINE");
}

#[tokio::test]
async fn original_drive_result_first_recovers_and_posts_without_restart() {
    original_drive_cash_recovery(false).await;
}

#[tokio::test]
async fn original_drive_cash_first_recovers_and_posts_without_restart() {
    original_drive_cash_recovery(true).await;
}

struct NeverTick;
#[async_trait::async_trait]
impl crate::bot::runtime::Strategy for NeverTick {
    fn name(&self) -> &'static str { "r16-deadline-close" }
    async fn tick(&mut self, _: &crate::bot::runtime::Ctx) -> anyhow::Result<()> {
        panic!("the expired original drive must finish before a strategy call")
    }
}

async fn close_at_original_deadline(broker: Arc<Broker>, clock: Arc<FixtureClock>,
    trader: &crate::service::predict_fun::PredictFunTrader) {
    clock.advance(3_600_000);
    let cfg = candidate_tests::ctx_fixture(broker.identity.as_ref().unwrap());
    let client = r16_fixture::data(trader);
    let ctx = crate::bot::runtime::Ctx { cfg, client:client.clone(), data:client, broker:broker.clone() };
    tokio::time::timeout(std::time::Duration::from_secs(5),
        crate::bot::runtime::drive(&mut NeverTick, &ctx)).await.unwrap().unwrap();
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().emitter.is_finished());
}

#[tokio::test]
async fn actual_adapter_two_claims_paid_separately_keep_wait_until_every_owner_closes() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd:1.0, ..Default::default() });
    let first = send_round(&broker, &clock, &facts, &mut core, 1).await;
    clock.advance(300_000);
    let second = send_round(&broker, &clock, &facts, &mut core, 2).await;
    facts.round(resolved(first, 1.0)); facts.round(resolved(second, 1.0));
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    let started = broker.state.lock().live.controlled.as_ref().unwrap().cash_wait.as_ref().unwrap().started;
    facts.pay(77);
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(state.live.claims.len(), 1);
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
        assert!(!run.may_add_risk());
        assert_eq!(run.cash_wait.as_ref().unwrap().started, started);
    }
    facts.pay(177);
    live::check_runtime_account(&broker, false).await.unwrap();
    let cash = crate::bot::runtime::live_events::state_accounting(&broker.state.lock()).cash;
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        assert_eq!(state.live.claims.len(), 2);
        assert_eq!(crate::bot::runtime::live_events::state_accounting(&state).cash, cash);
        assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
    }
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    close_at_original_deadline(broker, clock, &trader).await;
}

#[tokio::test]
async fn actual_adapter_zero_payout_removes_inventory_without_inventing_a_cash_claim() {
    let (broker, clock, trader, transport, facts) = adapter_fixture().await;
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd:1.0, ..Default::default() });
    let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
    facts.round(resolved(market, 0.0));
    broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
    facts.pay(77);
    live::check_runtime_account(&broker, false).await.unwrap();
    assert!(broker.state.lock().live.claims.is_empty());
    assert!(broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    close_at_original_deadline(broker, clock, &trader).await;
}

#[tokio::test]
async fn actual_adapter_cash_confirmation_cannot_clear_any_first_hard_stop() {
    for reason in ["DATA_UNAVAILABLE", "FACT_CONFLICT", "DEADLINE", "PARENT_LOST",
        "AUDIT_UNAVAILABLE", "OUTPUT_INVALID", "LOSS_LATCH"] {
        for stop_first in [false, true] {
            let (broker, clock, trader, transport, facts) = adapter_fixture().await;
            let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
                order_usd:1.0, ..Default::default() });
            let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
            facts.round(resolved(market, 1.0));
            let stop = || broker.state.lock().live.controlled.as_mut().unwrap()
                .stop(reason, true, clock.now().unwrap().utc);
            if stop_first { stop(); }
            broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
            if !stop_first { stop(); }
            facts.pay(77);
            live::check_runtime_account(&broker, false).await.unwrap();
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.control.lock().state, LiveRunState::StopNewRisk, "{reason}/{stop_first}");
            assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, reason);
            assert!(!run.may_add_risk() && run.must_end());
            assert_eq!(state.live.claims.len(), 1, "cash facts remain truthful after a hard stop");
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn cash_wait_599999_600000_600001_on_either_clock_never_renews_its_origin() {
    for delta in [599_999, 600_000, 600_001] {
        for mode in ["both", "utc", "elapsed"] {
            let (mut broker, result) = candidate_tests::official_result_fixture(1.0);
            broker.settle_resolved(&result).await.unwrap();
            let start = broker.state.lock().live.controlled.as_ref().unwrap().cash_wait.as_ref().unwrap().started;
            let clock = Arc::new(FixtureClock::new(start.utc));
            clock.advance(start.elapsed_ms);
            clock.set_utc(start.utc);
            if mode != "utc" { clock.advance(delta); }
            if mode == "elapsed" { clock.set_utc(start.utc); }
            if mode == "utc" { clock.set_utc(start.utc + chrono::Duration::milliseconds(delta as i64)); }
            broker.state.lock().live.controlled.as_mut().unwrap().phase1.clock = clock;
            broker.taker = Some(Arc::new(exact_account(&broker)));
            live::check_runtime_account(&broker, false).await.unwrap();
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            if delta < 600_000 { assert!(run.may_add_risk(), "{delta}/{mode}"); }
            else {
                assert!(!run.may_add_risk() && run.must_end(), "{delta}/{mode}");
                assert_eq!(run.cash_wait.as_ref().unwrap().started, start);
            }
        }
    }
}

// Keep the production finish consumer pending after cleanup, without waiting
// for its real 60s poll cadence or enabling Tokio test-util/dependency changes.
struct FinishClock {
    inner: Arc<FixtureClock>,
    fail_after_reads: std::sync::atomic::AtomicUsize,
}
impl Clock for FinishClock {
    fn now(&self) -> anyhow::Result<crate::service::observation::CheckTime> {
        if self.fail_after_reads.try_update(Ordering::SeqCst, Ordering::SeqCst,
            |left| left.checked_sub(1)).ok() == Some(1) {
            anyhow::bail!("fixture clock unavailable");
        }
        self.inner.now()
    }
}

struct FinishPendingData {
    market: TradableMarket,
    clock: Arc<FinishClock>,
    cleanup_advance_ms: u64,
    fail_tail_clock: bool,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl MarketData for FinishPendingData {
    async fn markets(&self, _: &MarketQuery) -> anyhow::Result<Vec<TradableMarket>> {
        panic!("finish must not scan")
    }
    async fn market(&self, _: u64) -> anyhow::Result<TradableMarket> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            self.clock.inner.advance(self.cleanup_advance_ms);
            // Eight original settlement/risk/lifecycle clock reads precede
            // the shared finish budget. Fail that next read only; final can
            // still record truthful facts with the original clock restored.
            if self.fail_tail_clock { self.clock.fail_after_reads.store(9, Ordering::SeqCst); }
            Ok(self.market.clone())
        } else {
            std::future::pending().await
        }
    }
    async fn with_schedule(&self, _: &TradableMarket) -> anyhow::Result<TradableMarket> {
        panic!("finish must not reschedule")
    }
    async fn books(&self, _: &[u64])
        -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> {
        panic!("finish must not invent a mark")
    }
}

#[tokio::test]
async fn finish_tail_uses_only_the_original_cash_wait_remainder_after_cleanup() {
    use crate::bot::runtime::live_control::cash_wait::CashWait;
    use std::time::Duration;
    // Each positive remainder is tiny on purpose: fake UTC/elapsed advance
    // independently, while the real consumer's timeout is exercised unchanged.
    for (case, wall_ms, elapsed_ms, deadline_ms, cleanup_ms, exhausted, explicit, expected_ms) in [
        ("partial", 599_940, 599_940, 3_600_000, 0, false, false, 60),
        ("utc_first", 599_960, 599_900, 3_600_000, 0, false, false, 40),
        ("elapsed_first", 599_900, 599_960, 3_600_000, 0, false, false, 40),
        ("utc_equal", 600_000, 599_900, 3_600_000, 0, false, false, 0),
        ("elapsed_equal", 599_900, 600_000, 3_600_000, 0, false, false, 0),
        ("over", 600_001, 600_001, 3_600_000, 0, false, false, 0),
        ("deadline_utc", 100, 100, 140, 0, false, false, 40),
        ("deadline_elapsed", 100, 130, 140, 0, false, false, 10),
        ("cleanup_partial", 599_900, 599_900, 3_600_000, 60, false, false, 40),
        ("cleanup_exhausts", 599_980, 599_980, 3_600_000, 30, false, false, 0),
        ("latched", 0, 0, 3_600_000, 0, true, false, 0),
        ("utc_rollback", -1, 0, 3_600_000, 0, false, false, 0),
        ("elapsed_rollback", 0, 0, 3_600_000, 0, false, false, 0),
        ("clock_error", 599_900, 599_900, 3_600_000, 0, false, false, 0),
        ("deadline_equal", 100, 100, 100, 0, false, false, 0),
        ("explicit_cap", 599_900, 599_900, 3_600_000, 0, false, true, 25),
        ("explicit_exhausted", 600_000, 600_000, 3_600_000, 0, false, true, 0),
        ("no_wait", 0, 0, 3_600_000, 0, false, true, 25),
    ] {
        let (broker, clock, _trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd: 1.0, ..Default::default()
        });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        let started = clock.now().unwrap();
        {
            let mut state = broker.state.lock();
            let run = state.live.controlled.as_mut().unwrap();
            if case != "no_wait" {
                let mut wait = CashWait::begin(started, vec![1]);
                if exhausted { wait.exhaust(); }
                run.cash_wait = Some(wait);
            }
            run.options.deadline_at = started.utc + chrono::Duration::milliseconds(deadline_ms);
            run.run_deadline_elapsed_ms = started.elapsed_ms + deadline_ms as u64;
        }
        clock.advance(elapsed_ms);
        clock.set_utc(started.utc + chrono::Duration::milliseconds(wall_ms));
        broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
        // A later hard stop must not rename the first cause or authorize risk.
        broker.state.lock().live.controlled.as_mut().unwrap()
            .stop("FACT_CONFLICT", true, started.utc);
        let tail_clock = Arc::new(FinishClock { inner: if case == "elapsed_rollback" {
            Arc::new(FixtureClock::new(started.utc))
        } else { clock }, fail_after_reads: Default::default() });
        broker.state.lock().live.controlled.as_mut().unwrap().phase1.clock = tail_clock.clone();
        let data = FinishPendingData { market, clock: tail_clock, cleanup_advance_ms: cleanup_ms,
            fail_tail_clock: case == "clock_error", reads: Default::default() };
        let before = tokio::time::Instant::now();
        let finish = async {
            if explicit {
                broker.finish_controlled_live_with(&data, Duration::from_secs(1),
                    Duration::from_millis(25)).await;
            } else {
                broker.finish_controlled_live(&data).await;
            }
        };
        let watchdog_ms = if expected_ms == 0 { 300 } else { expected_ms + 30 };
        tokio::time::timeout(Duration::from_millis(watchdog_ms), finish).await
            .unwrap_or_else(|_| panic!("{case}: finish exceeded the original remainder"));
        assert!(before.elapsed() >= Duration::from_millis(expected_ms), "{case}: tail was lost");
        assert_eq!(data.reads.load(Ordering::SeqCst), if expected_ms == 0 { 1 } else { 2 },
            "{case}: exhausted waits must not even poll a tail read");
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.emitter.is_finished(), "{case}");
        assert!(!run.may_add_risk(), "{case}");
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DEADLINE", "{case}");
        if case != "no_wait" {
            assert_eq!(run.cash_wait.as_ref().unwrap().started, started, "{case}: W0 changed");
        }
        assert!(run.attempt_outcomes.is_empty() && state.live.claims.is_empty(), "{case}");
        assert_eq!(run.control.lock().state, LiveRunState::Handoff, "{case}: cash is not closed");
        assert!(!state.live.net_inventory[&1].is_zero(), "{case}: uncredited holding lost");
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "{case}: new sign");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "{case}: new POST");
    }
}

#[test]
fn captured_finish_tail_remainders_keep_every_final_cash_incomplete() {
    use crate::bot::runtime::live_events::{LiveEventEmitter, LIVE_EVENT_PREFIX};
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bot::runtime::live_control::r16_cash_drive::finish_tail_uses_only_the_original_cash_wait_remainder_after_cleanup",
            "--nocapture"]).current_dir(env!("CARGO_MANIFEST_DIR")).output().unwrap();
    assert!(output.status.success(), "{}\n{}", String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr));
    let text = std::str::from_utf8(&output.stdout).unwrap();
    let finals: Vec<_> = text.lines().filter_map(|line| line.split_once(LIVE_EVENT_PREFIX))
        .map(|(_, json)| LiveEventEmitter::validate_bytes(json.as_bytes()).unwrap())
        .filter(|event| event["event_kind"] == "final").collect();
    assert_eq!(finals.len(), 18, "one actual producer final per parameterized boundary");
    for event in finals {
        assert_eq!(event["payload"]["cash_complete"], false);
        assert_eq!(event["payload"]["takeover_required"], true);
        assert_eq!(event["payload"]["stop_reason"], "DEADLINE");
        assert_eq!(event["payload"]["positions"].as_array().unwrap().len(), 1);
    }
}

struct TamperedObservation {
    trader: Arc<crate::service::predict_fun::PredictFunTrader>,
    fault: &'static str,
}
#[async_trait::async_trait]
impl OrderVenue for TamperedObservation {
    fn id(&self) -> VenueId { VenueId::PredictFun }
    async fn place(&self, _: &crate::models::PlannedOrder, _: u64, _: bool)
        -> anyhow::Result<OrderTicket> { panic!("fault injection cannot send") }
    async fn runtime_account_observation(&self, closed: &[RuntimeClaimInventory])
        -> anyhow::Result<RuntimeAccountObservation> {
        let mut read = self.trader.runtime_account_observation(closed).await?;
        assert_eq!(read.bound_claims.len(), 1);
        let claim = &mut read.bound_claims[0];
        match self.fault {
            "inventory_minus" => claim.inventory_atoms -= U256::from(1),
            "inventory_plus" => claim.inventory_atoms += U256::from(1),
            "cash_minus" => claim.claim_atoms -= U256::from(1),
            "cash_plus" => claim.claim_atoms += U256::from(1),
            "generation" => claim.execution_generation -= 1,
            "market" => claim.identity.market_id = 999,
            "token" => claim.identity.token_id = "999".into(),
            "side" => claim.identity.side = crate::models::Side::Sell,
            "pair" => claim.no_token = "999".into(),
            "owner" => claim.identity.local_attempt_id = 999,
            "hash" => claim.identity.signed_hash = Some(format!("0x{}", "f".repeat(64))),
            "extra_cash" => *read.snapshot.collateral_atoms.as_mut().unwrap() += U256::from(1),
            "inventory_still_present" => read.snapshot.positions.push((claim.identity.token_id.clone(), claim.inventory_atoms)),
            "open_order" => read.snapshot.open_orders.push("999".into()),
            "incomplete" => read.snapshot.complete = false,
            "duplicate_owner" => { let duplicate = claim.clone(); read.bound_claims.push(duplicate); }
            _ => panic!("unknown test fault"),
        }
        Ok(read)
    }
}

#[tokio::test]
async fn actual_adapter_proof_mutations_fail_before_any_partial_credit_or_resume() {
    for fault in ["inventory_minus", "inventory_plus", "cash_minus", "cash_plus", "generation",
        "market", "token", "side", "pair", "owner", "hash", "extra_cash", "inventory_still_present",
        "open_order", "incomplete", "duplicate_owner"] {
        let (mut broker, clock, trader, transport, facts) = adapter_fixture().await;
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
            order_usd:1.0, ..Default::default() });
        let market = send_round(&broker, &clock, &facts, &mut core, 1).await;
        facts.round(resolved(market, 1.0));
        broker.settle_resolved(r16_fixture::data(&trader).as_ref()).await.unwrap();
        facts.pay(77);
        Arc::get_mut(&mut broker).unwrap().taker = Some(Arc::new(TamperedObservation { trader, fault }));
        assert!(live::check_runtime_account(&broker, false).await.is_err(), "{fault}");
        let state = broker.state.lock();
        assert!(state.live.claims.is_empty(), "{fault}");
        assert!(state.live.pending_claims.is_empty(), "{fault}");
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk(), "{fault}");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "{fault}");
    }
}
