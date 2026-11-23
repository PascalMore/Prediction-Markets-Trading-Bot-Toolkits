//! Incremental regressions for the real live ledger -> core feedback seam.
//! These do not stand in for the guarded adapter's end-to-end acceptance matrix.

use super::*;
use crate::bot::runtime::{live, taker, Blocked, TakeResult};
use crate::models::{Side, VenueId};
use crate::service::venue::{
    AccountMode, Accounting, AttemptPhase, AttemptRecord, Closure, ExecutionSnapshot, Field,
    OrderIdentity, Quantity, QuantityClass, Submission,
};

const SCALE: u128 = 1_000_000_000_000_000_000;

#[path = "tests/live003_regression.rs"]
mod live003_regression;

#[path = "tests/live004_regression.rs"]
mod live004_regression;

#[path = "tests/live012_regression.rs"]
mod live012_regression;

#[path = "tests/send_timing_bench.rs"]
mod send_timing_bench;

#[path = "tests/claim_wait.rs"]
mod claim_wait;

#[path = "tests/degrade_recovery.rs"]
mod degrade_recovery;

/// Test-only: the preparation checkpoints and the faults injected into them.
///
/// The hooks live with the tests that use them: they are not part of the
/// run's own state contract, only a way to inject a change at a known
/// point of the real preparation sequence.
#[derive(Default)]
pub(crate) struct PreparationHooks {
    /// How many preparation checkpoints the guard has passed.
    pub checkpoints: std::sync::atomic::AtomicUsize,
    /// The fault to inject, and the checkpoint to inject it at.
    pub fault: Mutex<Option<(usize, PreparationFault)>>,
    /// The hook that moves the run's clock.
    pub clock: Mutex<Option<Arc<dyn Fn(u64) + Send + Sync>>>,
}

impl std::fmt::Debug for PreparationHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparationHooks")
            .field("checkpoints", &self.checkpoints.load(Ordering::SeqCst))
            .field("fault", &self.fault.lock().as_ref().map(|(at, fault)| (*at, *fault)))
            .field("clock", &self.clock.lock().is_some())
            .finish()
    }
}

/// Test-only: the change injected at one preparation checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparationFault {
    /// The clock moves past the receipt's freshness window.
    Clock,
    /// The frozen book ages out of its own window while the account receipt,
    /// read later, stays fresh. The value is the clock advance in milliseconds.
    BookAge(u64),
    /// The run's loss latch turns on.
    Loss,
    /// The run's deadline passes.
    Deadline,
    /// The run stops adding risk.
    Stop,
    /// The candidate's frozen inputs are invalidated.
    Revision,
}

/// Test-only: count one preparation checkpoint and inject its fault.
///
/// The checkpoint runs after the guard's own checks, so an injected change
/// is seen by the *next* preparation check exactly as a real change
/// between two awaits would be.
pub(crate) fn preparation_checkpoint(guard: &LiveCandidateGuard) {
    let Some((state, candidate)) = guard.test_binding() else { return };
    let hooks = {
        let state = state.lock();
        let Some(run) = state.live.controlled.as_ref() else { return };
        Arc::clone(&run.preparation)
    };
    let index = hooks.checkpoints.fetch_add(1, Ordering::SeqCst);
    let Some((at, fault)) = hooks.fault.lock().clone() else { return };
    if at != index {
        return;
    }
    let clock = hooks.clock.lock().clone();
    let mut state = state.lock();
    let run = state.live.controlled.as_mut().unwrap();
    match fault {
        PreparationFault::Clock => {
            if let Some(clock) = clock {
                clock(10_001);
            }
        }
        PreparationFault::BookAge(advance) => {
            // R10: age the frozen book past its own window. The account
            // receipt was read later, so it stays inside its window and the
            // refusal is the book's, not the read's.
            if let Some(clock) = clock {
                clock(advance);
            }
        }
        PreparationFault::Loss => run.loss_latched = true,
        PreparationFault::Deadline => {
            run.options.deadline_at = run.phase1.clock.now().unwrap().utc;
        }
        PreparationFault::Stop => run.stop("AUDIT_UNAVAILABLE", true, Utc::now()),
        PreparationFault::Revision => {
            // The candidate's own input revision counter moves on.
            candidate.invalidate_revision();
        }
    }
}

pub(super) fn ctx_fixture(identity: &AccountIdentity) -> crate::config::AppConfig {
    use crate::config::controlled_live::ActivationAccountIdentity;
    let mut cfg: crate::config::AppConfig = serde_json::from_str(
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/config.json"))).unwrap();
    cfg.venue = VenueId::PredictFun;
    cfg.predict_fun.network = crate::config::PredictNetwork::Mainnet;
    cfg.credentials.predict_api_key = Some("offline-synthetic-not-a-credential".into());
    cfg.bot.enable_trading = true;
    cfg.bot.mock_trading = false;
    cfg.controlled_paper = None;
    cfg.strategies.poll_secs = 5;
    cfg.strategies.rescan_secs = 300;
    cfg.strategies.quote_ttl_secs = 300;
    cfg.strategies.limits = serde_json::from_value(serde_json::json!({
        "max_order_usd": 1.0, "max_market_usd": 1.0, "max_total_usd": 10.0, "max_loss_usd": 20.0,
    })).unwrap();
    cfg.strategies.btc_arb = serde_json::from_value(serde_json::json!({
        "symbols": ["BTCUSDT"], "round_interval_secs": 300, "min_edge": 0.03,
        "order_usd": 1.0, "min_secs_left": 30, "vol_floor": 0.00005,
    })).unwrap();
    let start = Utc::now();
    let deadline = start + chrono::Duration::hours(1);
    let mut options = ControlledLiveOptions::from_arguments(
        Some(crate::config::controlled_live::LIVE_CONTRACT), Some("supervised"), Some("ctx-window"),
        Some("ctx-auth"), Some(&deadline.to_rfc3339()), &start.to_rfc3339()).unwrap().unwrap();
    options.account_identity = Some(ActivationAccountIdentity {
        venue: "PREDICT_FUN".into(), chain_id: 56, account_mode: "predict-account".into(),
        trading_address: identity.trading_address.clone(), signer_address: identity.signer_address.clone(),
    });
    options.gas_policy = crate::config::controlled_live::GasPolicy {
        payer_address: identity.signer_address.clone(), minimum_balance_atoms: U256::from(1),
        maximum_consumption_atoms: U256::from(10),
    };
    cfg.controlled = Some(crate::config::ControlledRun::from_live_arguments(Some("signed-future-live"), Some("ctx-test"),
        Some(&start.to_rfc3339()), Some(&deadline.to_rfc3339()), options.clone()).unwrap());
    cfg.controlled_live = Some(options);
    cfg
}

#[test]
fn original_ctx_installs_one_live_phase1_channel_and_emitter_without_paper_cash() {
    let (trader, transport) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "fixed_account");
    let identity = trader.identity();
    let cfg = ctx_fixture(&identity);
    let mut factory_calls = 0;
    let ctx = super::super::Ctx::connect_with_trader(cfg, "BTC arbitrage", |_, _| {
        factory_calls += 1;
        Ok(trader)
    }).unwrap();
    assert_eq!(factory_calls, 1);
    assert!(ctx.broker.is_live());
    assert!(ctx.broker.is_phase1());
    let state = ctx.broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.identity, identity);
    assert!(Arc::ptr_eq(&run.event_seq, &run.phase1.event_seq));
    assert!(Arc::ptr_eq(&run.output_failed, &run.phase1.output_failed));
    assert_eq!(run.emitter.last_seq(), run.event_seq.load(Ordering::SeqCst));
    assert!(state.paper.is_none());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
}

#[test]
fn original_ctx_refuses_invalid_public_context_before_original_trader_factory() {
    let (trader, _) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "fixed_account");
    let identity = trader.identity();
    for fault in ["venue", "paper", "limits", "model", "context", "account", "strategy",
        "payer-trading", "payer-foreign", "payer-missing", "payer-malformed", "payer-mismatch"] {
        let mut cfg = ctx_fixture(&identity);
        match fault {
            "venue" => cfg.venue = VenueId::Polymarket,
            "paper" => cfg.controlled_paper = Some(crate::config::ControlledPaperConfig {
                initial_cash: 100.0, fee_model: "paper_net_shares".into() }),
            "limits" => cfg.strategies.limits.max_total_usd = 11.0,
            "model" => cfg.strategies.btc_arb.min_edge = 0.04,
            "context" => cfg.controlled_live = None,
            "account" => {
                let mut options = cfg.controlled_live.take().unwrap();
                options.account_identity.as_mut().unwrap().trading_address = "0x1111111111111111111111111111111111111111".into();
                cfg.controlled.as_mut().unwrap().live = Some(options.clone());
                cfg.controlled_live = Some(options);
            }
            payer if payer.starts_with("payer-") => {
                let options = cfg.controlled_live.as_mut().unwrap();
                options.gas_policy.payer_address = match payer { "payer-trading" => identity.trading_address.clone(), "payer-foreign" | "payer-mismatch" => format!("0x{}", "3".repeat(40)), "payer-malformed" => "bad".into(), _ => String::new() };
                if payer != "payer-mismatch" { cfg.controlled.as_mut().unwrap().live = Some(options.clone()); }
            }
            _ => (),
        }
        let mut calls = 0;
        let result = super::super::Ctx::connect_with_trader(cfg,
            if fault == "strategy" { "sports" } else { "BTC arbitrage" }, |_, _| {
                calls += 1; anyhow::bail!("must not construct trader")
            });
        assert!(result.is_err(), "{fault}");
        assert_eq!(calls, 0, "{fault}");
    }
}

fn snapshot(generation: u64, gross: u128, cash: u128, fee: u128) -> ExecutionSnapshot {
    ExecutionSnapshot {
        identity: OrderIdentity {
            local_attempt_id: 1,
            signed_hash: Some(format!("0x{}", "a".repeat(64))),
            venue_order_id: None,
            market_id: 1,
            token_id: "77".into(),
            side: Side::Buy,
            account: AccountIdentity {
                chain_id: 56,
                trading_address: "0x1111111111111111111111111111111111111111".into(),
                signer_address: "0x2222222222222222222222222222222222222222".into(),
                mode: AccountMode::PredictAccount,
                venue: VenueId::PredictFun,
            },
        },
        submission: Submission::Accepted,
        quantity: Quantity::Known {
            gross_q_atoms: U256::from(gross),
            class: QuantityClass::Partial,
        },
        accounting: Accounting {
            gross_cash: Field::Known(U256::from(cash)),
            collateral_fee: Field::Known(U256::ZERO),
            share_fee: Field::Known(U256::from(fee)),
            net_cash: Field::Known(U256::from(cash)),
            net_inventory: Field::Known(U256::from(gross - fee)),
            vwap: Field::Known(U256::from(4 * SCALE / 10)),
        },
        accounting_gross_q: Some(U256::from(gross)),
        closure: Closure::Unproven,
        generation,
        pages_complete: true,
        gross_q_before: U256::ZERO,
        gross_q_after: U256::from(gross),
        source: "offline-live-feedback-regression".into(),
        observed_at: Utc::now(),
        tx_hash: None,
    }
}

fn reserve(snapshot: &ExecutionSnapshot) -> State {
    let mut state = State::default();
    live::reserve_attempt(
        &mut state.live,
        AttemptRecord {
            identity: snapshot.identity.clone(),
            request_gross_atoms: U256::from(5 * SCALE),
            r0: U256::from(2 * SCALE),
            reserved_sell: U256::ZERO,
            phase: AttemptPhase::MayHaveSubmitted,
            submission: Submission::Accepted,
            last_reason: None,
        },
    );
    state
}

#[test]
fn actual_share_fee_feedback_is_net_not_requested_gross() {
    let reading = snapshot(1, 5 * SCALE, 2 * SCALE, SCALE / 10);
    let mut state = reserve(&reading);
    let applied = live::apply_snapshot_detailed(&mut state, &reading).unwrap();
    assert!(applied.applied);
    assert_eq!(applied.delta.gross_q, U256::from(5 * SCALE));
    assert_eq!(applied.net_inventory_delta, Some(U256::from(49 * SCALE / 10)));
    assert_eq!(
        taker::net_take_result(&applied, 0.4),
        TakeResult::Filled { shares: 4.9, price: 0.4 }
    );
    assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
    assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
}

#[test]
fn partial_then_late_fill_feedback_only_committed_net_increment_and_duplicate_zero() {
    let first = snapshot(1, 2 * SCALE, 8 * SCALE / 10, SCALE / 25);
    let mut state = reserve(&first);
    let first_applied = live::apply_snapshot_detailed(&mut state, &first).unwrap();
    assert_eq!(first_applied.net_inventory_delta, Some(U256::from(196 * SCALE / 100)));
    let mut late = snapshot(2, 5 * SCALE, 2 * SCALE, SCALE / 10);
    late.observed_at = first.observed_at + chrono::Duration::seconds(60);
    let applied = live::apply_snapshot_detailed(&mut state, &late).unwrap();
    assert_eq!(applied.net_inventory_delta, Some(U256::from(294 * SCALE / 100)));
    assert!(matches!(taker::net_take_result(&applied, 0.4),
        TakeResult::Filled { shares, .. } if shares == 2.94));
    let duplicate = live::apply_snapshot_detailed(&mut state, &late).unwrap();
    assert!(duplicate.duplicate);
    assert!(!duplicate.applied);
    assert_eq!(taker::net_take_result(&duplicate, 0.4),
        TakeResult::Filled { shares: 0.0, price: 0.4 });
    assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
    assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
}

#[test]
fn known_gross_with_unknown_net_does_not_confirm_a_fill_or_release_risk() {
    let mut reading = snapshot(1, 5 * SCALE, 2 * SCALE, SCALE / 10);
    reading.accounting.net_inventory = Field::Unknown("MISSING_NET".into());
    let mut state = reserve(&reading);
    let applied = live::apply_snapshot_detailed(&mut state, &reading).unwrap();
    assert!(!applied.accounting_complete);
    assert!(!applied.applied);
    assert_eq!(applied.net_inventory_delta, None);
    assert_eq!(taker::net_take_result(&applied, 0.4), TakeResult::Blocked(Blocked::Halted));
    assert_eq!(state.live.reservations[&1], U256::from(2 * SCALE));
    assert!(state.live.net_inventory.is_empty());
    assert_eq!(state.live.attempts[&1].lock().identity.signed_hash, reading.identity.signed_hash);
}

#[tokio::test]
async fn bare_buy_in_controlled_phase1_live_never_reaches_original_adapter() {
    use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
    use crate::service::predict_fun::account::tests::synthetic_trader;
    let (trader, transport) = synthetic_trader("terminal");
    let identity = trader.identity();
    let gate = trader.gate.clone();
    let trader = Arc::new(trader);
    let mut broker = Broker::new(StrategyLimits::default(), true,
        Some(trader.clone()), Some(trader)).unwrap().with_account_gate(gate, identity.clone());
    broker.controlled = Some(ControlledRun::new(
        ControlledMode::SignedFutureLive, "feedback-test".into(), Utc::now()).unwrap());
    let phase1 = Phase1Run::new(
        Arc::new(crate::service::observation::SystemClock::new().unwrap()),
        crate::bot::arbitrage::phase1::StrategyContract::Phase1NetShares, 0.03, 30.0);
    let options = ControlledLiveOptions {
        account_identity: None,
        contract: crate::config::controlled_live::LIVE_CONTRACT.into(),
        operations_mode: OperationsMode::Supervised,
        risk_window_id: "feedback-window".into(),
        authorization_ref: "offline-only".into(),
        deadline_at: Utc::now() + chrono::Duration::hours(1),
        budget_secs: 3600,
        params: crate::config::RunParameters::legacy(),
        gas_policy: crate::config::controlled_live::GasPolicy {
            payer_address: identity.signer_address.clone(),
            minimum_balance_atoms: U256::from(1),
            maximum_consumption_atoms: U256::from(1),
        },
    };
    broker.state.lock().live.controlled = Some(ControlledLiveState::new(
        "feedback-test".into(), phase1, options, identity));
    let mut market = crate::bot::runtime::test_support::market(1);
    market.yes_token = "77".into();
    market.no_token = "88".into();
    let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
    let result = broker.take(&market, "77", Side::Buy, 4.0, 0.5, &book).await.unwrap();
    assert_eq!(result, TakeResult::Blocked(Blocked::DataIncomplete));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    assert!(broker.state.lock().live.attempts.is_empty());
    assert!(broker.state.lock().paper.is_none());
}

/// Local risk-seam fixture only: no client, signer, transport or second ledger.
pub(super) fn risk_fixture() -> (Broker, Arc<crate::service::observation::FixtureClock>) {
    use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
    use crate::service::observation::FixtureClock;
    let now = Utc::now();
    let clock = Arc::new(FixtureClock::new(now));
    let identity = snapshot(1, 5 * SCALE, 2 * SCALE, SCALE / 10).identity.account;
    let gate = crate::service::predict_fun::account::AccountGate::new();
    gate.confirm_budget(&identity);
    let mut broker = Broker::new(StrategyLimits::default(), false, None, None).unwrap()
        .with_account_gate(gate, identity.clone());
    broker.live = true;
    broker.controlled = Some(ControlledRun::new(
        ControlledMode::SignedFutureLive, "risk-test".into(), now).unwrap());
    let options = ControlledLiveOptions {
        account_identity: None,
        contract: crate::config::controlled_live::LIVE_CONTRACT.into(),
        operations_mode: OperationsMode::Supervised, risk_window_id: "risk-window".into(),
        authorization_ref: "offline-only".into(), deadline_at: now + chrono::Duration::hours(1),
        budget_secs: 3600, params: crate::config::RunParameters::legacy(),
        gas_policy: crate::config::controlled_live::GasPolicy {
            payer_address: identity.signer_address.clone(), minimum_balance_atoms: U256::from(1),
            maximum_consumption_atoms: U256::from(100),
        },
    };
    let baseline = crate::service::venue::AccountSnapshot {
        identity: identity.clone(), collateral_atoms: Some(U256::from(100 * SCALE)),
        gas_atoms: Some(U256::from(1000)), positions: vec![], positions_explained: true,
        settled_zero: vec![],
        claimed_win: vec![],
        open_orders: vec![], complete: true,
    };
    {
        let mut state = broker.state.lock();
        state.live.controlled = Some(ControlledLiveState::new("risk-test".into(),
            Phase1Run::new(clock.clone(), crate::bot::arbitrage::phase1::StrategyContract::Phase1NetShares,
                0.03, 30.0), options, identity));
        state.live.baseline = Some(baseline.clone());
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    (broker, clock)
}

fn apply_risk_fill(broker: &Broker, market_id: u64, cash: u128, final_fill: bool) {
    let mut reading = snapshot(1, 5 * SCALE, cash, SCALE / 10);
    let mut state = broker.state.lock();
    reading.identity.local_attempt_id = live::next_attempt_id(&mut state.live);
    reading.identity.market_id = market_id;
    reading.identity.token_id = market_id.to_string();
    reading.identity.signed_hash = Some(format!("0x{:064x}", reading.identity.local_attempt_id));
    reading.closure = if final_fill {
        Closure::Final { final_gross_q: U256::from(5 * SCALE), evidence: "synthetic-authoritative".into() }
    } else { Closure::Unproven };
    let now = state.live.controlled.as_ref().unwrap().phase1.clock.now().unwrap();
    reading.observed_at = now.utc;
    state.marks.insert(reading.identity.token_id.clone(), 0.5);
    state.live.controlled.as_mut().unwrap().mark_receipts.insert(reading.identity.token_id.clone(), now);
    live::reserve_attempt(&mut state.live, AttemptRecord {
        identity: reading.identity.clone(), request_gross_atoms: U256::from(5 * SCALE),
        r0: U256::from(2 * SCALE), reserved_sell: U256::ZERO,
        phase: AttemptPhase::MayHaveSubmitted, submission: Submission::Accepted, last_reason: None,
    });
    live::apply_snapshot_detailed(&mut state, &reading).unwrap();
    let total = state.live.net_cash.values().copied().sum::<U256>();
    let mut account = state.live.baseline.clone().unwrap();
    account.collateral_atoms = Some(U256::from(100 * SCALE) - total);
    let receipt = account_receipt(&state).unwrap();
    record_account_read(&mut state, &account, receipt).unwrap();
}

#[test]
fn atomic_live_entry_accepts_legal_cash_and_checks_one_atom_cap_neighbors() {
    let (broker, _) = risk_fixture();
    let mut state = broker.state.lock();
    let legal = U256::from(999_900_000_000_000_000u128); // a 1.00 USDT order reserves ~0.9999
    let cap = U256::from(SCALE);
    assert_eq!(broker.check_live_entry(&mut state, 1, legal), None);
    assert_eq!(broker.check_live_entry(&mut state, 1, cap), None);
    // Below the 0.5 USDT dust floor.
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(499_900_000_000_000_000u128)),
        Some(Blocked::BelowMinimum));
    // A 2.00 USDT order reserves 1.9998 and is refused by the 1 USDT order cap.
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(1_999_800_000_000_000_000u128)),
        Some(Blocked::OrderCap));
    assert_eq!(broker.check_live_entry(&mut state, 1, cap + U256::from(1)), Some(Blocked::OrderCap));
    assert!(state.paper.is_none());
    assert!(state.live.attempts.is_empty());
    // Original entry gate: only the signer may spend the unchanged native gas budget.
    let signer = state.live.controlled.as_ref().unwrap().identity.signer_address.clone();
    let trading = state.live.controlled.as_ref().unwrap().identity.trading_address.clone();
    assert_ne!(signer, trading);
    for (payer, gas, minimum, maximum, accepted) in [
        (signer.as_str(), Some(1000), 1000, 100, true),
        (signer.as_str(), Some(999), 1000, 100, false),
        (signer.as_str(), Some(901), 1, 100, true),
        (signer.as_str(), Some(900), 1, 100, false),
        (signer.as_str(), None, 1, 100, false),
        (signer.as_str(), Some(1001), 1, 100, false),
        (trading.as_str(), Some(1000), 1, 100, false),
        ("0x3333333333333333333333333333333333333333", Some(1000), 1, 100, false),
        ("", Some(1000), 1, 100, false), ("bad", Some(1000), 1, 100, false),
    ] {
        let run = state.live.controlled.as_mut().unwrap();
        run.options.gas_policy.payer_address = payer.into();
        run.options.gas_policy.minimum_balance_atoms = U256::from(minimum as u64);
        run.options.gas_policy.maximum_consumption_atoms = U256::from(maximum as u64);
        run.account_read.as_mut().unwrap().snapshot.gas_atoms = gas.map(|value| U256::from(value as u64));
        assert_eq!(broker.check_live_entry(&mut state, 1, legal).is_none(), accepted, "{payer} {gas:?}");
    }
}

#[test]
fn actual_cost_is_not_lost_when_a_final_fill_releases_the_reservation() {
    use crate::bot::runtime::live_accounting::risk_exposure;
    let (broker, _) = risk_fixture();
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    let mut state = broker.state.lock();
    assert!(state.live.reservations.values().all(U256::is_zero));
    let risk = risk_exposure(&state.live).unwrap();
    assert_eq!(risk.cost, U256::from(2 * SCALE));
    assert_eq!(risk.reserved, U256::ZERO);
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), Some(Blocked::MarketCap));
    assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)), None);
    let accounting = super::super::live_events::state_accounting(&state);
    // The window's cash is budget-relative: 98 - 2 spent.
    assert_eq!(accounting.cash, Some(U256::from(96 * SCALE)));
    assert_eq!(accounting.nav, Some(super::super::live_accounting::SignedAtoms::positive(
        U256::from(9845 * SCALE / 100))));
}

#[test]
fn partial_cost_plus_residual_counts_once_and_rejected_overrisk_survives() {
    use crate::bot::runtime::live_accounting::risk_exposure;
    let (broker, _) = risk_fixture();
    apply_risk_fill(&broker, 1, SCALE, false);
    let mut state = broker.state.lock();
    let risk = risk_exposure(&state.live).unwrap();
    assert_eq!((risk.cost, risk.reserved), (U256::from(SCALE), U256::from(SCALE)));
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), Some(Blocked::MarketCap));
    // An overfill is refused by the real application, but the observed risk
    // floor must still be visible rather than clipped back to R0.
    let mut overfill = snapshot(2, 6 * SCALE, 3 * SCALE, SCALE / 10);
    overfill.identity = state.live.attempts[&1].lock().identity.clone();
    assert_eq!(live::apply_snapshot_detailed(&mut state, &overfill).unwrap_err(), "OVERFILL");
    let risk = risk_exposure(&state.live).unwrap();
    assert_eq!((risk.cost, risk.reserved), (U256::from(SCALE), U256::from(2 * SCALE)));
    assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)), Some(Blocked::Halted));
}

#[test]
fn total_cap_includes_actual_cost_and_allows_exactly_ten_not_one_atom_more() {
    let (broker, _) = risk_fixture();
    // Ten 1-USDT orders fill ten distinct markets; the per-market cap forces
    // one market per order, and the total cap is exactly ten.
    for market in 1..=9 { apply_risk_fill(&broker, market, SCALE, true); }
    {
        let mut state = broker.state.lock();
        assert_eq!(broker.check_live_entry(&mut state, 10, U256::from(SCALE)), None);
    }
    apply_risk_fill(&broker, 10, SCALE, true);
    let mut state = broker.state.lock();
    assert_eq!(broker.check_live_entry(&mut state, 11, U256::from(SCALE)), Some(Blocked::Halted));
    assert_eq!(state.live.controlled.as_ref().unwrap().control.lock().stop.as_ref()
        .unwrap().detail.as_deref(), Some("TOTAL_CAP"));
}

#[test]
fn actual_exposure_atom_boundaries_and_candidate_overflow_are_distinct() {
    use super::super::live_accounting::risk_exposure;
    for (last_cash, stop) in [(SCALE - 1, false), (SCALE, true), (SCALE + 1, true)] {
        let (broker, _) = risk_fixture();
        for market in 1..=9 { apply_risk_fill(&broker, market, SCALE, true); }
        apply_risk_fill(&broker, 10, last_cash, true);
        let mut state = broker.state.lock();
        let risk = risk_exposure(&state.live).unwrap();
        assert_eq!(risk.cost, U256::from(9 * SCALE + last_cash));
        assert_eq!(risk.reserved, U256::ZERO);
        assert_eq!(!state.live.controlled.as_ref().unwrap().may_add_risk(), stop);
        if stop {
            let control = state.live.controlled.as_ref().unwrap().control.lock();
            assert_eq!(control.stop.as_ref().unwrap().reason, "MANUAL_STOP");
            assert_eq!(control.stop.as_ref().unwrap().detail.as_deref(), Some("TOTAL_CAP"));
        } else {
            // The proposed order exceeds 10; it is not existing exposure.
            assert_eq!(broker.check_live_entry(&mut state, 11, U256::from(SCALE)), Some(Blocked::TotalCap));
            assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
        }
    }
    let (broker, _) = risk_fixture();
    for market in 1..=8 { apply_risk_fill(&broker, market, SCALE, true); }
    apply_risk_fill(&broker, 9, SCALE, false);
    let mut state = broker.state.lock();
    let risk = risk_exposure(&state.live).unwrap();
    assert_eq!((risk.cost, risk.reserved), (U256::from(9 * SCALE), U256::from(SCALE)));
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    // Releasing risk later cannot clear the first stop.
    state.live.reservations.clear();
    state.live.attempts.clear();
    state.live.net_cash.clear();
    state.live.observed_risk_floor.clear();
    refresh_live_risk(&mut state);
    assert_eq!(state.live.controlled.as_ref().unwrap().control.lock().stop.as_ref()
        .unwrap().detail.as_deref(), Some("TOTAL_CAP"));
}

#[tokio::test]
async fn exposure_waiter_preserves_in_flight_policy_until_actual_reservations_reach_cap() {
    let (broker, _) = risk_fixture();
    {
        let mut state = broker.state.lock();
        let mut reading = incomplete_reading(1, Closure::Open);
        reserve_reading(&mut state, &mut reading, Submission::Accepted, Some("in-flight"));
    }
    assert!(tokio::time::timeout(std::time::Duration::from_millis(120),
        broker.wait_controlled_stop()).await.is_err(), "partial in-flight read alone is not a new stop");
    assert!(stopped(&broker).is_none());
    {
        let mut state = broker.state.lock();
        for id in 2..=5 {
            let mut reading = incomplete_reading(1, Closure::Open);
            reading.identity.local_attempt_id = id;
            reading.identity.market_id = id;
            reading.identity.token_id = id.to_string();
            reserve_reading(&mut state, &mut reading, Submission::Accepted, Some("in-flight"));
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), broker.wait_controlled_stop())
        .await.expect("no new candidate/mark is required to stop existing reservation risk");
    assert_eq!(stopped(&broker), Some(("MANUAL_STOP".into(), Some("TOTAL_CAP".into()), true)));
    assert_eq!(broker.state.lock().live.reservations.values().copied().sum::<U256>(), U256::from(10 * SCALE));
}

#[tokio::test]
async fn exposure_and_loss_stops_return_from_original_drive_without_sign_or_post() {
    struct ReachRisk(&'static str, usize);
    #[async_trait::async_trait]
    impl super::super::Strategy for ReachRisk {
        fn name(&self) -> &'static str { "actual-risk-exit-regression" }
        async fn tick(&mut self, ctx: &super::super::Ctx) -> anyhow::Result<()> {
            self.1 += 1;
            assert_eq!(self.1, 1, "a stopped run must not spin another strategy tick");
            if self.0 == "loss" {
                apply_risk_fill(&ctx.broker, 1, 2245 * SCALE / 100, true);
            } else {
                let filled = if self.0 == "mixed" { 8 } else { 9 };
                for market in 1..=filled { apply_risk_fill(&ctx.broker, market, SCALE, true); }
                apply_risk_fill(&ctx.broker, filled + 1,
                    if self.0 == "above" { SCALE + 1 } else { SCALE }, self.0 != "mixed");
            }
            Ok(())
        }
    }
    for fault in ["exact", "above", "mixed", "loss"] {
        let (broker, _, market, book, candidate, transport) = sending_fixture("terminal").await;
        let mut broker = Arc::try_unwrap(broker).ok().unwrap();
        let (limit, _) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "fixed_account");
        broker.limit = Some(Arc::new(limit));
        let broker = Arc::new(broker);
        let mut cfg = ctx_fixture(broker.identity.as_ref().unwrap());
        cfg.strategies.poll_secs = 1;
        let client = crate::service::predict_fun::PredictFunClient::new(&cfg).unwrap();
        let data = ShutdownData { broker: broker.clone(), market: market.clone(), fault: "unused",
            reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
        let ctx = super::super::Ctx { cfg, client, data: Arc::new(data), broker: broker.clone() };
        let mut strategy = ReachRisk(fault, 0);
        tokio::time::timeout(std::time::Duration::from_secs(5), super::super::drive(&mut strategy, &ctx))
            .await.expect("original drive must finish, not idle at the cap").unwrap();
        assert_eq!(strategy.1, 1);
        {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert!(run.emitter.is_finished());
            assert!(run.stop_event_emitted);
            let control = run.control.lock();
            assert_eq!(control.state, LiveRunState::Handoff);
            let stop = control.stop.as_ref().unwrap();
            assert_eq!(stop.reason, if fault == "loss" { "LOSS_LATCH" } else { "MANUAL_STOP" });
            assert!(stop.takeover_required);
            if fault != "loss" { assert_eq!(stop.detail.as_deref(), Some("TOTAL_CAP")); }
            assert!(!state.live.net_inventory.is_empty(), "exit is not a fabricated close");
            if fault == "mixed" { assert!(!state.live.reservations[&9].is_zero()); }
        }
        let error = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await.err().unwrap();
        assert_eq!(error.to_string(), "CONTEXT_MISSING", "sealed run refuses any new candidate");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn exposure_exit_final_retains_positions_unknown_reservations_and_takeover() {
    use super::super::live_events::{LiveEventEmitter, LIVE_EVENT_PREFIX};
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bot::runtime::live_control::candidate_tests::exposure_and_loss_stops_return_from_original_drive_without_sign_or_post", "--nocapture"])
        .output().unwrap();
    assert!(output.status.success());
    let text = std::str::from_utf8(&output.stdout).unwrap();
    let events: Vec<_> = text.lines().filter_map(|line| line.split_once(LIVE_EVENT_PREFIX))
        .map(|(_, json)| LiveEventEmitter::validate_bytes(json.as_bytes()).unwrap()).collect();
    let finals: Vec<_> = events.iter().filter(|event| event["event_kind"] == "final").collect();
    assert_eq!(finals.len(), 4);
    for (index, terminal) in finals.iter().enumerate() {
        assert_eq!(terminal["payload"]["stop_reason"], if index == 3 { "LOSS_LATCH" } else { "MANUAL_STOP" });
        assert_eq!(terminal["payload"]["takeover_required"], true);
        assert_eq!(terminal["payload"]["cash_complete"], false);
        assert!(!terminal["payload"]["positions"].as_array().unwrap().is_empty());
        if index < 3 { assert_eq!(terminal["payload"]["stop_detail"], "TOTAL_CAP"); }
    }
    let unresolved = finals[2]["payload"]["unresolved"].as_array().unwrap();
    assert!(unresolved.iter().any(|attempt| attempt["reserved_atoms"] == SCALE.to_string()
        && attempt["closure"] == "UNPROVEN"));
}

#[test]
fn official_cash_and_receipt_version_are_required_for_each_new_reservation() {
    let (broker, clock) = risk_fixture();
    let mut state = broker.state.lock();
    state.live.controlled.as_mut().unwrap().account_read.as_mut().unwrap()
        .snapshot.collateral_atoms = Some(U256::from(SCALE - 1));
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), Some(Blocked::InsufficientCash));
    state.live.controlled.as_mut().unwrap().account_read.as_mut().unwrap()
        .snapshot.collateral_atoms = Some(U256::from(100 * SCALE));
    clock.advance(10_000);
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), None);
    clock.advance(1);
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), Some(Blocked::DataIncomplete));
    let checked = account_receipt(&state).unwrap();
    let baseline = state.live.baseline.clone().unwrap();
    record_account_read(&mut state, &baseline, checked).unwrap();
    state.live.next_attempt_id += 1;
    assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), Some(Blocked::DataIncomplete));
}

fn evaluate_live_fixture(
    clock: &crate::service::observation::FixtureClock,
    warm: bool,
) -> (crate::service::venue::TradableMarket, crate::models::BookSnapshot,
    crate::bot::arbitrage::phase1::Phase1Evaluation) {
    evaluate_live_round(clock, warm, 1)
}

fn evaluate_live_round(
    clock: &crate::service::observation::FixtureClock, warm: bool, market_id: u64,
) -> (crate::service::venue::TradableMarket, crate::models::BookSnapshot,
    crate::bot::arbitrage::phase1::Phase1Evaluation) {
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(
        crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
    evaluate_live_round_with_core(clock, warm, market_id, &mut core)
}

pub(super) fn evaluate_live_round_with_core(
    clock: &crate::service::observation::FixtureClock, warm: bool, market_id: u64,
    core: &mut crate::bot::arbitrage::DecisionCore,
) -> (crate::service::venue::TradableMarket, crate::models::BookSnapshot,
    crate::bot::arbitrage::phase1::Phase1Evaluation) {
    use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
    use crate::service::observation::{Clock, ObservationClock, SourceTimeStatus, FreshnessBasis};
    let mut market = crate::bot::runtime::test_support::market(market_id);
    market.yes_token = if market_id == 1 { "77" } else { "177" }.into();
    market.no_token = if market_id == 1 { "88" } else { "188" }.into();
    market.feed_symbol = Some("BTCUSDT".into());
    market.start_price = Some(100.0);
    market.round_facts = Some(crate::service::venue::RoundFacts {
        decimal_precision: Some(2), ..Default::default()
    });
    market.starts_at = Some(clock.now().unwrap().utc - chrono::Duration::seconds(30));
    market.ends_at = Some(market.starts_at.unwrap() + chrono::Duration::seconds(300));
    let book = crate::bot::runtime::test_support::book(&[(0.3, 100.0)], &[(0.4, 100.0)]);
    let observe = |clock: &crate::service::observation::FixtureClock| {
        let now = clock.now().unwrap();
        ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound, request_at: now.utc, received_at: now.utc,
            request_elapsed_ms: now.elapsed_ms, received_elapsed_ms: now.elapsed_ms }
    };
    if warm {
        for _ in 0..=20 {
            clock.advance(1000);
            let observation = observe(clock);
            core.observe_phase1("BTCUSDT", 100.2, crate::service::spot::SpotSource::Binance,
                "BTCUSDT", &observation, clock.now().unwrap()).unwrap();
        }
    }
    let observation = observe(clock);
    let evaluation = core.evaluate_phase1(&EvaluationInputs {
        market: &market, book: &book, spot_price: 100.2,
        spot_source: Some(crate::service::spot::SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(observation.clone()), book_clock: Some(observation), now: clock.now().unwrap(),
        order_usd: 1.0, min_edge: 0.03, min_secs_left: 30.0, entered: false,
        identity_available_at: market.starts_at, strike_available_at: market.starts_at,
        settlement_provider: SettlementProvider::Chainlink,
    });
    (market, book, evaluation)
}

#[test]
fn live_evaluate_is_reachable_without_paper_and_warmup_is_a_legal_wait() {
    use crate::bot::arbitrage::phase1::Reason;
    let (broker, clock) = risk_fixture();
    let (market, _, evaluation) = evaluate_live_fixture(&clock, false);
    assert_eq!(evaluation.diagnostic.reason, Reason::Warmup);
    assert!(evaluation.candidate.is_none());
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, None));
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.may_add_risk());
    assert!(run.pending_candidate.is_none());
    assert_eq!(run.emitter.last_seq(), 1);
    assert_eq!(run.event_seq.load(Ordering::SeqCst), 1);
    assert_eq!(run.phase1.event_seq.load(Ordering::SeqCst), 1);
    assert!(state.paper.is_none());
}

#[test]
fn live_candidate_and_pre_submit_share_the_core_context_emitter_and_sequence() {
    let (broker, clock) = risk_fixture();
    let (market, _, evaluation) = evaluate_live_fixture(&clock, true);
    let candidate = evaluation.candidate.unwrap();
    assert_ne!(candidate.fee_rate_bps(), 0);
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    assert!(broker.live_pre_submit(&candidate).unwrap());
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.pending_candidate.as_ref().unwrap().context.same_candidate(&candidate));
    assert_eq!(run.emitter.last_seq(), 2);
    assert_eq!(run.phase1.event_seq.load(Ordering::SeqCst), 2);
    assert!(Arc::ptr_eq(&run.output_failed, &run.phase1.output_failed));
    assert!(state.paper.is_none());
}

#[test]
fn paired_original_paper_live_pre_submit_fixture() {
    use crate::config::{ControlledMode, ControlledRun, StrategyLimits};
    use crate::service::observation::Clock;
    use crate::bot::arbitrage::phase1::StrategyContract;
    for (delay_ms, invalidate, expected) in [(0, false, true), (1, false, true),
        (10_001, false, false), (0, true, false)] {
        let (live, clock) = risk_fixture();
        let (market, book, evaluation) = evaluate_live_fixture(&clock, true);
        let candidate = evaluation.candidate.unwrap();
        // The exact same candidate carries market, book, time, fee and quantity
        // to both original mode entrances, not two separately rebuilt inputs.
        let paper = Broker::new(StrategyLimits {
            max_order_usd: 1.0, max_market_usd: 1.0, max_total_usd: 10.0, max_loss_usd: 20.0,
        }, false, None, None).unwrap().with_controlled_paper(
            ControlledRun::new(ControlledMode::UnsignedPaper, "paired-paper".into(),
                clock.now().unwrap().utc).unwrap(), 100.0).unwrap()
            .with_phase1(clock.clone(), StrategyContract::Phase1NetShares, 0.03, 30.0).unwrap();
        paper.observe(&market, &book);
        assert!(paper.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
        assert!(live.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
        clock.advance(delay_ms);
        if invalidate { candidate.invalidate_revision(); }
        let paper_result = paper.take_candidate(&market, &book, &candidate).unwrap();
        let live_accepted = live.live_pre_submit(&candidate).unwrap();
        assert_eq!(matches!(paper_result, TakeResult::Filled { .. }), expected);
        assert_eq!(live_accepted, expected);
        // Only paper simulates execution; the live lane has no adapter/signer,
        // no fake fill and no local attempt just because the core accepts.
        assert!(paper.state.lock().live.controlled.is_none());
        let state = live.state.lock();
        assert!(state.paper.is_none());
        assert!(state.live.attempts.is_empty());
        assert!(state.holdings.is_empty());
    }
}

#[test]
fn paired_original_mode_diagnostics_match_for_frozen_inputs() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bot::runtime::live_control::candidate_tests::paired_original_paper_live_pre_submit_fixture", "--nocapture"])
        .output().unwrap();
    assert!(output.status.success(), "{}{}", String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr));
    let stdout = std::str::from_utf8(&output.stdout).unwrap();
    let mut paper = Vec::new();
    let mut live = Vec::new();
    for line in stdout.lines() {
        if let Some(frame) = line.strip_prefix(super::super::paper::EVENT_PREFIX) {
            let event: serde_json::Value = serde_json::from_str(frame).unwrap();
            if event["source"]["decision"]["phase"] == "PRE_SUBMIT" {
                paper.push(event["source"]["decision"].clone());
            }
        }
        if let Some(frame) = line.strip_prefix(super::super::live_events::LIVE_EVENT_PREFIX) {
            let event: serde_json::Value = serde_json::from_str(frame).unwrap();
            if event["payload"]["decision"]["phase"] == "PRE_SUBMIT" {
                live.push(event["payload"]["decision"].clone());
            }
        }
    }
    assert_eq!(paper.len(), 4);
    assert_eq!(live.len(), 4);
    assert_eq!(paper, live, "all comparable model and candidate fields must match");
}

#[test]
fn a_busy_live_lane_stops_evaluation_without_interleaving_another_event() {
    let (broker, clock) = risk_fixture();
    let (market, _, evaluation) = evaluate_live_fixture(&clock, false);
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.try_lock().unwrap();
    assert!(!broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, None));
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(!run.may_add_risk());
    assert_eq!(run.emitter.last_seq(), 0);
}

#[test]
fn actual_loss_at_twenty_and_atom_neighbors_latches_without_clipping_overspend() {
    for (cash, expected) in [(2245 * SCALE / 100 - 1, false),
        (2245 * SCALE / 100, true), (2245 * SCALE / 100 + 1, true)] {
        let (broker, _) = risk_fixture();
        // An unexpected authoritative cash cost is applied, not clipped to R0.
        apply_risk_fill(&broker, 1, cash, true);
        let mut state = broker.state.lock();
        assert_eq!(state.live.net_cash[&1], U256::from(cash));
        assert_eq!(state.live.controlled.as_ref().unwrap().loss_latched, expected);
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
        state.marks.insert("1".into(), 0.9);
        refresh_live_risk(&mut state);
        assert_eq!(state.live.controlled.as_ref().unwrap().loss_latched, expected);
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    }
}

async fn sending_fixture(fault: &'static str) -> (
    Arc<Broker>, Arc<crate::service::observation::FixtureClock>,
    crate::service::venue::TradableMarket, crate::models::BookSnapshot, CandidateContext,
    Arc<crate::service::predict_fun::account::tests::SyntheticRpc>,
) {
    let (broker, clock, market, book, candidate, transport, _) = sending_fixture_with_trace(fault).await;
    (broker, clock, market, book, candidate, transport)
}

pub(super) async fn sending_fixture_with_trace(fault: &'static str) -> (
    Arc<Broker>, Arc<crate::service::observation::FixtureClock>,
    crate::service::venue::TradableMarket, crate::models::BookSnapshot, CandidateContext,
    Arc<crate::service::predict_fun::account::tests::SyntheticRpc>,
    Arc<crate::service::predict_fun::controlled::ControlledSyntheticTrace>,
) {
    use crate::service::venue::OrderVenue;
    let (mut broker, clock) = risk_fixture();
    let (market, book, evaluation) = evaluate_live_fixture(&clock, true);
    let candidate = evaluation.candidate.unwrap();
    let (trader, transport, trace) = crate::service::predict_fun::controlled::synthetic_controlled_trader_with_trace(
        candidate.fee_rate_bps(), fault);
    let identity = trader.identity();
    let baseline = trader.account_snapshot().await.unwrap();
    broker.identity = Some(identity.clone());
    broker.gate = Some(trader.gate.clone());
    broker.taker = Some(Arc::new(trader));
    {
        let mut state = broker.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address;
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    (Arc::new(broker), clock, market, book, candidate, transport, trace)
}

pub(super) fn ack_next(broker: Arc<Broker>, stop_before_ack: bool) -> std::thread::JoinHandle<()> {
    ack_next_with(broker, move |broker| {
        if stop_before_ack { channel::ControlWatcher::parent_lost(broker); }
    })
}

fn ack_next_with(
    broker: Arc<Broker>, before_ack: impl FnOnce(&Broker) + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let channel = broker.state.lock().live.controlled.as_ref().unwrap().channel.clone();
        let pending = channel.wait_pending();
        before_ack(&broker);
        channel.accept(&channel::ControlFrame::Ack {
            seq: pending.seq, signed_hash: pending.signed_hash }).unwrap();
    })
}

#[tokio::test]
async fn final_gate_callback_rechecks_the_current_clock_before_first_poll() {
    use crate::service::venue::ControlledSendGuard;
    use crate::bot::runtime::live_accounting::minimum_net_shares;
    for (advance_ms, should_poll) in [(100, true), (10_001, false)] {
        let (broker, clock, market, _, candidate, transport) = sending_fixture("terminal").await;
        let q = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
        let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(q, candidate.limit_price()).unwrap();
        let route = broker.taker.as_ref().unwrap().controlled_route(candidate.token()).unwrap();
        let hash = format!("0x{}", "d".repeat(64));
        let (handle, id, control, emitter) = {
            let mut state = broker.state.lock();
            let id = live::next_attempt_id(&mut state.live);
            let identity = state.live.controlled.as_ref().unwrap().identity.clone();
            let handle = live::reserve_attempt(&mut state.live, AttemptRecord {
                identity: OrderIdentity { local_attempt_id: id, signed_hash: Some(hash.clone()),
                    venue_order_id: None, market_id: market.id, token_id: candidate.token().into(),
                    side: Side::Buy, account: identity },
                request_gross_atoms: q, r0: cash, reserved_sell: U256::ZERO,
                phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
            });
            let run = state.live.controlled.as_mut().unwrap();
            run.pending_candidate.as_mut().unwrap().worst_cash_atoms = cash;
            run.attempt_candidates.insert(id, candidate.candidate_id().into());
            (handle, id, run.control.clone(), run.emitter.clone())
        };
        let gate = broker.gate.as_ref().unwrap();
        gate.reserve(&handle.lock().identity.account, id).unwrap();
        let guard = LiveCandidateGuard::new(control, id, candidate.candidate_id().into(), emitter)
            .bind(broker.state.clone(), candidate.clone(), handle.clone(), route.clone());
        let bound = SignedExecutionBound {
            account: handle.lock().identity.account.clone(), market_id: market.id,
            token_id: candidate.token().into(), route, fee_rate_bps: candidate.fee_rate_bps(),
            request_gross_atoms: q, gross_cash_atoms: cash, max_cash_atoms: cash,
            min_net_shares_atoms: minimum_net_shares(q, candidate.fee_rate_bps()).unwrap(),
            expiration: Utc::now().timestamp() + 60, signed_hash: hash,
        };
        guard.check_facts(&bound).unwrap();
        let ack = ack_next(broker.clone(), false);
        guard.await_durable_ack(&bound).unwrap();
        ack.join().unwrap();
        let mut polls = 0;
        let result = guard.with_send_permit(&handle, &mut |record, check_clock| {
            // Fake only the elapsed wait. The original gate and original guard
            // still decide whether their real first-poll callback may execute.
            clock.advance(advance_ms);
            gate.with_checked_prepared_attempt(record, &mut || check_clock().map_err(|_| {
                crate::service::venue::VenueError::new(
                    crate::service::venue::VenueErrorCode::InvalidInput, "controlled_send", "INPUT_CHANGED")
            }), &mut || { polls += 1; })?;
            Ok(())
        });
        assert_eq!(result.is_ok(), should_poll);
        assert_eq!(polls, usize::from(should_poll));
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "this unit seam uses no HTTP");
        assert_eq!(handle.lock().submission,
            if should_poll { Submission::Unknown } else { Submission::NotSubmitted });
        assert!(handle.lock().identity.signed_hash.is_some());
        assert_eq!(guard.emitter().last_seq(), 4, "EVALUATE, PREPARED, FINAL_SEND, order");
    }
}

#[tokio::test]
async fn original_core_broker_predict_transport_reaches_a_nonzero_fee_post_after_ack() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("terminal").await;
    // The drive loop reads and reconciles the account (sync_live) before the
    // tick that submits; the send is bound to that read.
    live::check_runtime_account(&broker, false).await.unwrap();
    let ack = ack_next(broker.clone(), false);
    let result = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    ack.join().unwrap();
    let outcome = result.unwrap();
    assert!(outcome.guard.as_ref().unwrap().is_acked());
    assert!(outcome.guard.as_ref().unwrap().was_consumed());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let state = broker.state.lock();
    let handle = &state.live.attempts[&outcome.attempt_id.unwrap()];
    assert_eq!(handle.lock().request_gross_atoms,
        crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap());
    assert!(handle.lock().identity.signed_hash.is_some());
    assert!(state.paper.is_none());
    drop(state);
    live::sync_predict(&broker).await.unwrap();
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "synchronization must never replay");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn original_core_broker_commits_actual_nonzero_share_fee_from_raw_venue_facts() {
    let (broker, _, market, book, candidate, transport, trace) = sending_fixture_with_trace("controlled_fill").await;
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    let gross = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    let fee = gross / U256::from(100);
    let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(gross, candidate.limit_price()).unwrap();
    assert!(fee > U256::ZERO);
    assert!(matches!(outcome.result, TakeResult::Filled { shares, price }
        if shares > 0.0 && price == candidate.limit_price()));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let signed = trace.signed.lock().unwrap();
    let posted = trace.posted.lock().unwrap();
    assert_eq!(signed.len(), 1);
    assert_eq!(posted.len(), 1);
    let bound = outcome.guard.as_ref().unwrap().bound.lock().clone().unwrap();
    for key in ["hash", "salt", "maker", "signer", "tokenId", "makerAmount", "takerAmount", "feeRateBps", "side"] {
        assert_eq!(signed[0][key], posted[0]["data"]["order"][key], "{key}");
    }
    assert_eq!(signed[0]["hash"], bound.signed_hash);
    assert_eq!(signed[0]["tokenId"], candidate.token());
    assert_eq!(signed[0]["takerAmount"], gross.to_string());
    assert_eq!(signed[0]["makerAmount"], cash.to_string());
    assert_eq!(bound.route, "ctf");
    assert_eq!(bound.market_id, market.id);
    assert_eq!(bound.account, broker.identity.clone().unwrap());
    assert_eq!(bound.request_gross_atoms, gross);
    assert_eq!(bound.max_cash_atoms, cash);
    assert_eq!(bound.min_net_shares_atoms,
        super::super::live_accounting::minimum_net_shares(gross, candidate.fee_rate_bps()).unwrap());
    assert!(gross - fee >= bound.min_net_shares_atoms);
    drop(posted);
    drop(signed);
    let state = broker.state.lock();
    assert_eq!(state.live.net_inventory[&id], gross - fee);
    assert_eq!(state.live.net_cash[&id], cash);
    assert_eq!(state.live.reservations[&id], U256::ZERO);
    assert_eq!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.attempt_candidates[&id], candidate.candidate_id());
    assert_eq!(run.traded[&market.id].starts_at, market.starts_at);
    assert_eq!(run.traded[&market.id].ends_at, market.ends_at);
    assert_eq!(run.emitter.last_seq(), run.phase1.event_seq.load(Ordering::SeqCst));
    assert!(state.paper.is_none());
}

#[tokio::test]
async fn two_official_rounds_use_one_core_ledger_and_emitter_with_actual_fee_and_payouts() {
    use crate::service::venue::OrderVenue;
    let (broker, clock) = risk_fixture();
    // Two historical official rounds fit before the adapter's system-clock
    // expiration; elapsed freshness is still advanced by the real fixture clock.
    clock.set_utc(Utc::now() - chrono::Duration::minutes(10));
    let mut broker = Arc::new(broker);
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(
        crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
    let emitter = broker.state.lock().live.controlled.as_ref().unwrap().emitter.clone();
    // One adapter, one client and one account gate serve both rounds. The
    // fixture is built exactly once, and the broker keeps that instance.
    let (trader, transport, trace) =
        crate::service::predict_fun::controlled::synthetic_controlled_two_round_trader(200);
    let identity = trader.identity();
    let gate = trader.gate.clone();
    let trader = Arc::new(trader);
    {
        let baseline = trader.account_snapshot().await.unwrap();
        let original = Arc::get_mut(&mut broker).unwrap();
        original.identity = Some(identity.clone());
        original.gate = Some(gate.clone());
        original.taker = Some(trader.clone() as Arc<dyn OrderVenue>);
        let mut state = original.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address.clone();
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    let mut rounds = Vec::new();
    let mut last_seq = 0;
    for market_id in [1, 2] {
        if market_id == 2 { clock.advance(300_000); }
        let (market, book, evaluation) = evaluate_live_round_with_core(&clock, true, market_id, &mut core);
        let candidate = evaluation.candidate.unwrap();
        // The same adapter instance serves the second round as well: the
        // broker's only venue slot holds this exact instance.
        assert_eq!(Arc::strong_count(&trader), 2,
            "the broker's single venue slot holds the fixture's own adapter");
        live::check_runtime_account(&broker, false).await.unwrap();
        broker.observe(&market, &book);
        assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
        {
            let mut state = broker.state.lock();
            assert_eq!(broker.check_live_entry(&mut state, market_id, U256::from(SCALE)), None,
                "round {market_id} authoritative entry must be healthy");
        }
        let ack = ack_next(broker.clone(), false);
        let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
        ack.join().unwrap();
        assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
        let id = outcome.attempt_id.unwrap();
        let q = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
        let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(q, candidate.limit_price()).unwrap();
        let net = q-q/U256::from(100);
        let bound = outcome.guard.as_ref().unwrap().bound.lock().clone().unwrap();
        assert_eq!(bound.market_id, market_id);
        assert_eq!(bound.token_id, market.yes_token);
        assert_eq!(bound.request_gross_atoms, q);
        assert_eq!(bound.max_cash_atoms, cash);
        assert!(net >= bound.min_net_shares_atoms);
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(Arc::ptr_eq(&emitter, &run.emitter));
        assert!(run.emitter.last_seq() > last_seq);
        last_seq = run.emitter.last_seq();
        assert_eq!(last_seq, run.phase1.event_seq.load(Ordering::SeqCst));
        assert_eq!(state.live.net_inventory[&id], net);
        assert_eq!(state.live.net_cash[&id], cash);
        assert_eq!(state.live.reservations[&id], U256::ZERO);
        assert_eq!(state.live.attempts[&id].lock().identity.account, identity);
        assert_eq!(run.attempt_candidates[&id], candidate.candidate_id());
        assert_eq!(market.ends_at.unwrap()-market.starts_at.unwrap(), chrono::Duration::seconds(300));
        assert!(state.paper.is_none());
        drop(state);
        if market_id == 1 {
            assert_normal_result_wait(&broker, market.clone()).await;
            live::sync_predict(&broker).await.unwrap();
            let mut resolved = market.clone();
            resolved.open = false;
            resolved.end_price = Some(99.0);
            resolved.payout_yes = Some(0.0);
            resolved.round_facts = Some(crate::service::venue::RoundFacts {
                oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
                yes_status: Some("LOST".into()), no_status: Some("WON".into()),
                decimal_precision: None, price_feed_provider: None,
            });
            broker.settle_resolved(&OfficialResultData {
                result: Mutex::new(resolved), reads: Default::default(),
            }).await.unwrap();
            assert!(broker.state.lock().live.controlled.as_ref().unwrap().may_add_risk());
        }
        rounds.push(market);
    }
    // The one adapter signed and posted exactly once per round.
    assert_eq!(transport.signs.load(Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
    let signed = trace.signed.lock().unwrap();
    let posted = trace.posted.lock().unwrap();
    assert_eq!(signed.len(), 2);
    assert_eq!(posted.len(), 2);
    for index in 0..2 {
        for key in ["hash", "salt", "maker", "signer", "tokenId", "makerAmount", "takerAmount", "feeRateBps"] {
            assert_eq!(signed[index][key], posted[index]["data"]["order"][key], "{key}");
        }
        assert_eq!(signed[index]["tokenId"], if index == 0 { "77" } else { "177" });
        assert_eq!(signed[index]["takerAmount"], (25*SCALE/10).to_string());
        assert_eq!(signed[index]["makerAmount"], SCALE.to_string());
        assert_ne!(signed[0]["hash"], signed[1]["hash"], "two rounds, two distinct orders");
    }
    drop(posted);
    drop(signed);
    assert!(rounds[0].ends_at <= rounds[1].starts_at);
    for (market, payout) in rounds.iter_mut().zip([0.0, 1.0]) {
        market.open = false;
        market.end_price = Some(if payout == 0.0 { 99.0 } else { 101.0 });
        market.payout_yes = Some(payout);
        market.round_facts = Some(crate::service::venue::RoundFacts {
            oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
            yes_status: Some(if payout == 0.0 { "LOST" } else { "WON" }.into()),
            no_status: Some(if payout == 0.0 { "WON" } else { "LOST" }.into()),
            decimal_precision: None, price_feed_provider: None,
        });
    }
    struct Results(Vec<crate::service::venue::TradableMarket>);
    #[async_trait::async_trait]
    impl crate::service::venue::MarketData for Results {
        async fn markets(&self, _: &crate::service::venue::MarketQuery) -> anyhow::Result<Vec<crate::service::venue::TradableMarket>> { panic!("unexpected scan") }
        async fn market(&self, id: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
            Ok(self.0.iter().find(|market| market.id == id).unwrap().clone())
        }
        async fn with_schedule(&self, _: &crate::service::venue::TradableMarket) -> anyhow::Result<crate::service::venue::TradableMarket> { panic!("unexpected schedule") }
        async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> { panic!("unexpected book") }
    }
    broker.settle_resolved(&Results(rounds)).await.unwrap();
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
    assert_eq!(state.live.net_inventory[&1], U256::from(2475*SCALE/1000));
    assert_eq!(state.live.net_inventory[&2], U256::from(2475*SCALE/1000));
    assert_eq!(state.live.net_cash.values().copied().sum::<U256>(), U256::from(2*SCALE));
    assert!(Arc::ptr_eq(&emitter, &run.emitter));
    assert!(run.emitter.last_seq() > last_seq);
}

#[tokio::test]
async fn raw_partial_late_and_duplicate_facts_commit_only_the_original_net_increment() {
    let (broker, clock, market, book, candidate, transport) = sending_fixture("controlled_partial").await;
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    let requested = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(requested, candidate.limit_price()).unwrap();
    let half = requested / U256::from(2);
    let half_fee = half / U256::from(100);
    let hash = {
        let state = broker.state.lock();
        assert_eq!(state.live.net_inventory[&id], half-half_fee);
        assert_eq!(state.live.net_cash[&id], cash / U256::from(2));
        assert_eq!(state.live.reservations[&id], cash-cash / U256::from(2));
        let hash = state.live.attempts[&id].lock().identity.signed_hash.clone().unwrap();
        hash
    };
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    clock.advance(3_600_001);
    transport.stage.store(1, Ordering::SeqCst);
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.lock().await;
    let late = broker.taker.as_ref().unwrap().execution_snapshot(&hash).await.unwrap();
    assert!(late.pages_complete && late.accounting_complete());
    assert_eq!(late.gross_q(), Some(requested));
    assert_eq!(late.accounting_gross_q, Some(requested));
    assert_eq!(late.identity.local_attempt_id, id);
    assert_eq!(late.accounting.share_fee, Field::Known(requested / U256::from(100)));
    let applied = live::consume_snapshot(&broker, &late).unwrap();
    assert_eq!(applied.net_inventory_delta, Some(requested-requested/U256::from(100)-half+half_fee));
    let repeated = live::consume_snapshot(&broker, &late).unwrap();
    assert!(repeated.duplicate);
    assert_eq!(repeated.net_inventory_delta, Some(U256::ZERO));
    let refreshed = broker.taker.as_ref().unwrap().execution_snapshot(&hash).await.unwrap();
    let no_new_facts = live::consume_snapshot(&broker, &refreshed).unwrap();
    assert_eq!(no_new_facts.net_inventory_delta, Some(U256::ZERO));
    let state = broker.state.lock();
    assert_eq!(state.live.net_inventory[&id], requested-requested/U256::from(100));
    assert_eq!(state.live.net_cash[&id], cash);
    assert_eq!(state.live.reservations[&id], U256::ZERO);
    assert_eq!(state.live.attempts[&id].lock().identity.signed_hash.as_deref(), Some(hash.as_str()));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn raw_missing_fee_never_confirms_net_or_releases_risk_and_later_facts_do_not_unstop() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("controlled_missing_fee").await;
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Blocked(Blocked::Halted)));
    let id = outcome.attempt_id.unwrap();
    let requested = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(requested, candidate.limit_price()).unwrap();
    let hash = {
        let state = broker.state.lock();
        assert!(state.live.net_inventory.get(&id).is_none_or(U256::is_zero));
        assert_eq!(state.live.reservations[&id], cash);
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
        let hash = state.live.attempts[&id].lock().identity.signed_hash.clone().unwrap();
        hash
    };
    transport.stage.store(1, Ordering::SeqCst);
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.lock().await;
    let filled = broker.taker.as_ref().unwrap().execution_snapshot(&hash).await.unwrap();
    assert!(filled.accounting_complete());
    live::consume_snapshot(&broker, &filled).unwrap();
    let state = broker.state.lock();
    assert_eq!(state.live.net_inventory[&id], requested-requested/U256::from(100));
    assert_eq!(state.live.net_cash[&id], cash);
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_adapter_rechecks_clock_stop_loss_deadline_and_attempt_identity_when_ack_returns() {
    for fault in ["clock", "stop", "loss", "deadline", "attempt_identity", "generation"] {
        let (broker, clock, market, book, candidate, transport) = sending_fixture("terminal").await;
        let ack = ack_next_with(broker.clone(), move |broker| {
            // expect() -> this mutation -> ACK is a condition-variable ordered
            // barrier, not a sleep-dependent race with the transport.
            if fault == "clock" { clock.advance(10_001); return; }
            if fault == "stop" { channel::ControlWatcher::parent_lost(broker); return; }
            let mut state = broker.state.lock();
            if fault == "generation" {
                // R10: a generation mismatch is a run-level fact, not this
                // candidate's frozen input: it still stops the window.
                *state.live.generation.entry(1).or_insert(0) += 1;
                return;
            }
            let run = state.live.controlled.as_mut().unwrap();
            match fault {
                "loss" => run.loss_latched = true,
                "deadline" => run.options.deadline_at = run.phase1.clock.now().unwrap().utc,
                "attempt_identity" => {
                    for candidate_id in run.attempt_candidates.values_mut() { *candidate_id = "999".into(); }
                }
                _ => unreachable!(),
            }
        });
        let result = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await;
        ack.join().unwrap();
        assert!(result.is_err(), "{fault}");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "{fault}");
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "{fault}");
        let state = broker.state.lock();
        assert_eq!(state.live.attempts.len(), 1, "{fault}");
        let record = state.live.attempts.values().next().unwrap().lock();
        assert_eq!(record.submission, Submission::NotSubmitted, "{fault}");
        assert!(record.identity.signed_hash.is_some(), "{fault}");
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk(), "{fault}");
        assert_eq!(state.live.controlled.as_ref().unwrap().channel.confirmed().len(), 1, "{fault}");
        assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_some(),
            "{fault}: a run-level change still ends the window");
    }
}

#[tokio::test]
async fn every_preparation_checkpoint_rechecks_the_run_before_the_order_is_sent() {
    use self::PreparationFault;
    // The adapter's real preparation has several checkpoints: the account
    // read, the book, the authorization, the signing, the token and the
    // ERC-1271 signature. Each of them is followed by the guard's own check.
    // A change injected at any of them must stop the order before the POST.
    // The number of checkpoints is measured on an unfaulted run first: a run
    // that is refused early never reaches its own later checkpoints.
    let (broker, _, market, book, candidate, transport) = sending_fixture("terminal").await;
    let ack = ack_next(broker.clone(), false);
    let result = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    ack.join().unwrap();
    assert!(result.is_ok());
    let total = broker.state.lock().live.controlled.as_ref().unwrap()
        .preparation.checkpoints.load(Ordering::SeqCst);
    assert_eq!(total, 10, "account/book/chain/balance/authorize/sign/token/chain/ERC1271 checkpoints");
    for fault in [PreparationFault::Clock, PreparationFault::Loss,
        PreparationFault::Deadline, PreparationFault::Stop, PreparationFault::Revision] {
        for checkpoint in 0..total {
            let (broker, clock, market, book, candidate, transport) = sending_fixture("terminal").await;
            let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let state = broker.state.lock();
                let hooks = Arc::clone(&state.live.controlled.as_ref().unwrap().preparation);
                *hooks.fault.lock() = Some((checkpoint, fault));
                let moved = clock.clone();
                *hooks.clock.lock() = Some(Arc::new(move |ms| moved.advance(ms)));
            }
            // The acknowledgement only exists once the adapter produced the
            // critical order. A preparation refused before that point never
            // reaches `expect`, so the waiter watches the run's own outcome.
            let ack = {
                let broker = broker.clone();
                let finished = Arc::clone(&finished);
                std::thread::spawn(move || {
                    let channel = broker.state.lock().live.controlled.as_ref().unwrap().channel.clone();
                    let Some(pending) = channel.wait_pending_until(&finished) else { return };
                    channel.accept(&channel::ControlFrame::Ack {
                        seq: pending.seq, signed_hash: pending.signed_hash }).unwrap();
                })
            };
            let result = broker.take_live_candidate(taker::LiveCandidateRequest {
                market: &market, yes_book: &book, candidate: &candidate }).await;
            finished.store(true, Ordering::SeqCst);
            ack.join().unwrap();
            // R10: the input revision that moves after the last preparation
            // check is seen by the last check itself, where the frozen decision
            // is refused as a candidate — nothing was sent, so the window stays
            // open. Every other checkpoint keeps the hard stop.
            if fault == PreparationFault::Revision && checkpoint == total - 1 {
                let outcome = result.unwrap_or_else(|error| panic!(
                    "{fault:?}@{checkpoint}: a late input revision must refuse only the candidate: {error}"));
                assert!(matches!(outcome.result, TakeResult::Blocked(Blocked::ThinBook)),
                    "{fault:?}@{checkpoint}");
                assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "{fault:?}@{checkpoint}");
                let state = broker.state.lock();
                let record = state.live.attempts.values().next().unwrap().lock();
                assert_eq!(record.submission, Submission::NotSubmitted, "{fault:?}@{checkpoint}");
                assert_eq!(record.phase, AttemptPhase::Closed, "{fault:?}@{checkpoint}");
                drop(record);
                assert!(state.live.controlled.as_ref().unwrap().may_add_risk(), "{fault:?}@{checkpoint}");
                assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none(),
                    "{fault:?}@{checkpoint}");
                continue;
            }
            assert!(result.is_err(), "{fault:?}@{checkpoint}: the order must be refused");
            assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "{fault:?}@{checkpoint}");
            let state = broker.state.lock();
            let record = state.live.attempts.values().next().unwrap().lock();
            assert_eq!(record.submission, Submission::NotSubmitted, "{fault:?}@{checkpoint}");
            assert!(!state.live.controlled.as_ref().unwrap().may_add_risk(), "{fault:?}@{checkpoint}");
        }
    }
}

#[tokio::test]
async fn eof_observed_before_ack_closes_the_same_state_before_original_post() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("terminal").await;
    live::check_runtime_account(&broker, false).await.unwrap();
    let ack = ack_next(broker.clone(), true);
    let result = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    ack.join().unwrap();
    assert!(result.is_err());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let state = broker.state.lock();
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    assert!(state.live.attempts.values().all(|handle|
        handle.lock().submission == Submission::NotSubmitted));
    assert!(state.live.attempts.values().any(|handle| handle.lock().identity.signed_hash.is_some()));
    assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Closed);
    assert!(!state.live.reservations.contains_key(&1));
    assert_eq!(state.live.tombstones[&live::taker_key(market.id, candidate.token(), Side::Buy)], 1);
}

#[tokio::test]
async fn pending_first_transport_poll_preserves_hash_reservation_and_never_reposts_after_eof() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let (broker, _, market, book, candidate, transport) = sending_fixture("abort").await;
    let ack = ack_next(broker.clone(), false);
    let mut operation = Box::pin(broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(operation.as_mut().poll(&mut context), Poll::Pending));
    ack.join().unwrap();
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    let before = {
        let state = broker.state.lock();
        let record = state.live.attempts[&1].lock().clone();
        assert_eq!(record.phase, AttemptPhase::MayHaveSubmitted);
        assert_eq!(record.submission, Submission::Unknown);
        assert!(record.identity.signed_hash.is_some());
        assert_eq!(state.live.reservations[&1], record.r0);
        record
    };
    channel::ControlWatcher::parent_lost(broker.as_ref());
    assert!(matches!(operation.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    drop(operation);
    let state = broker.state.lock();
    assert_eq!(*state.live.attempts[&1].lock(), before);
    assert_eq!(state.live.reservations[&1], before.r0);
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    assert!(state.live.controlled.as_ref().unwrap().pending_candidate.is_none());
}

#[tokio::test]
async fn original_controlled_post_errors_keep_one_hash_one_sign_and_full_risk() {
    for fault in ["timeout", "post401", "malformed"] {
        let (broker, _, market, book, candidate, transport) = sending_fixture(fault).await;
        let ack = ack_next(broker.clone(), false);
        let result = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await;
        ack.join().unwrap();
        assert!(result.is_err(), "{fault}");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "{fault}");
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "{fault}");
        let state = broker.state.lock();
        let record = state.live.attempts[&1].lock();
        assert_eq!(record.phase, AttemptPhase::MayHaveSubmitted, "{fault}");
        assert_eq!(record.submission, Submission::Unknown, "{fault}");
        assert_eq!(state.live.reservations[&1], record.r0, "{fault}");
        assert!(record.identity.signed_hash.is_some());
        assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
        drop(record);
        // R10: once the transport owns the order nothing can be a
        // candidate-level refusal — the window still stops with its cash held.
        assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_some(),
            "{fault}: a post-first-poll failure still ends the window");
    }
}

#[tokio::test]
async fn original_channel_bad_ack_partial_eof_and_parent_eof_stop_the_waiting_broker() {
    for fault in ["hash", "sequence", "partial", "eof", "embedded_deadline"] {
        let (broker, _, market, book, candidate, transport) = sending_fixture("terminal").await;
        let parent = broker.clone();
        let reader = std::thread::spawn(move || {
            let channel = parent.state.lock().live.controlled.as_ref().unwrap().channel.clone();
            let start = std::time::Instant::now();
            loop {
                if let Some(pending) = channel.pending() {
                    let input = match fault {
                        "hash" => format!("A {} 0x{}\n", pending.seq, "f".repeat(64)),
                        "sequence" => format!("A {} {}\n", pending.seq + 1, pending.signed_hash),
                        "partial" => format!("A {} {}", pending.seq, pending.signed_hash),
                        "embedded_deadline" => "A D".into(),
                        _ => String::new(),
                    };
                    channel::read_control_channel(&mut std::io::Cursor::new(input.as_bytes()), channel, parent);
                    return;
                }
                assert!(start.elapsed() < std::time::Duration::from_secs(3));
                std::thread::park_timeout(std::time::Duration::from_millis(1));
            }
        });
        let result = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await;
        reader.join().unwrap();
        assert!(result.is_err(), "{fault}");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0, "{fault}");
        let state = broker.state.lock();
        assert_eq!(state.live.controlled.as_ref().unwrap().control.lock().stop.as_ref().unwrap().reason,
            if fault == "eof" { "PARENT_LOST" } else { "CONTROL_CHANNEL" });
        assert!(state.live.attempts.values().any(|handle| handle.lock().identity.signed_hash.is_some()));
    }
}

#[tokio::test]
async fn a_changed_book_inside_the_limit_never_replaces_the_frozen_decision() {
    // live004: a better price, a moved bid and a thinner level are market
    // movement, not a new decision. The order may proceed, but its identity,
    // quantity and cash bound stay the candidate's.
    for fault in ["better_book", "changed_bid", "changed_depth", "book_bid_moved"] {
        let (broker, _, market, book, candidate, transport) = sending_fixture(fault).await;
        let ack = ack_next(broker.clone(), false);
        let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await;
        ack.join().unwrap();
        let outcome = outcome.unwrap_or_else(|error| panic!("{fault}: {error}"));
        let id = outcome.attempt_id.unwrap_or_else(|| panic!("{fault}: no attempt"));
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "{fault}");
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "{fault}");
        let quantity = crate::service::predict_fun::controlled::quantity_atoms(
            candidate.gross_shares()).unwrap();
        let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(
            quantity, candidate.limit_price()).unwrap();
        let state = broker.state.lock();
        let record = state.live.attempts[&id].lock().clone();
        assert_eq!(record.identity.market_id, market.id, "{fault}");
        assert_eq!(record.identity.token_id, candidate.token(), "{fault}");
        assert_eq!(record.request_gross_atoms, quantity, "{fault}");
        // The reservation is the candidate's own worst case, never the fresh
        // book's price.
        assert!(record.r0 <= cash, "{fault}: {} > {cash}", record.r0);
        assert!(record.identity.signed_hash.is_some(), "{fault}");
        drop(state);
    }
}

#[tokio::test]
async fn preparation_refusal_closes_only_the_unsent_cash_not_the_candidate_evidence() {
    // live004: the fresh book moved past the candidate's limit after its cash
    // was reserved. Nothing was signed or sent, so the attempt's cash and the
    // candidate go away while the window and the attempt's evidence stay.
    let (broker, _, market, book, candidate, transport) = sending_fixture("book_past_limit").await;
    live::check_runtime_account(&broker, false).await.unwrap();
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a moved book refuses the candidate, not the window");
    assert!(matches!(outcome.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", outcome.result);
    assert_eq!(outcome.attempt_id, Some(1));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    let seq_after_refusal = broker.state.lock().live.controlled.as_ref().unwrap().emitter.last_seq();
    {
        let state = broker.state.lock();
        let record = state.live.attempts[&1].lock();
        assert_eq!(record.phase, AttemptPhase::Closed);
        assert_eq!(record.submission, Submission::NotSubmitted);
        assert!(record.identity.signed_hash.is_none());
        drop(record);
        assert!(!state.live.reservations.contains_key(&1));
        assert_eq!(state.live.tombstones[&live::taker_key(market.id, candidate.token(), Side::Buy)], 1);
        assert!(state.live.controlled.as_ref().unwrap().pending_candidate.is_none());
        assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
        assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none());
        assert!(state.entry_stop_reason.is_none());
    }
    // The same decision cannot be taken twice without a new evaluation: the
    // refused candidate is gone, and the run adds no attempt, cash or event.
    let replay = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    assert!(replay.err().unwrap().to_string().contains("CONTEXT_MISSING"));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    let state = broker.state.lock();
    assert_eq!(state.live.attempts.len(), 1);
    assert!(state.live.reservations.is_empty());
    assert_eq!(state.live.controlled.as_ref().unwrap().emitter.last_seq(), seq_after_refusal);
}

#[tokio::test]
async fn thin_book_refusal_closes_only_the_unsent_cash_not_the_candidate_evidence() {
    // R1: the fresh book is inside the candidate's limit but too thin for the
    // frozen quantity. Nothing was signed or sent, so the attempt's cash is
    // released and the window stays open — the same candidate-level refusal as
    // a book that moved past the limit.
    let (broker, _, market, book, candidate, transport) = sending_fixture("book_inside_limit_thin").await;
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await
        .expect("a thin book inside the limit refuses the candidate, not the window");
    assert!(matches!(outcome.result, TakeResult::Blocked(Blocked::ThinBook)), "{:?}", outcome.result);
    assert_eq!(outcome.attempt_id, Some(1));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    let state = broker.state.lock();
    let record = state.live.attempts[&1].lock();
    assert_eq!(record.phase, AttemptPhase::Closed);
    assert_eq!(record.submission, Submission::NotSubmitted);
    assert!(record.identity.signed_hash.is_none());
    drop(record);
    assert!(state.live.reservations.is_empty());
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.pending_candidate.is_none());
    assert!(run.control.lock().stop.is_none());
    assert!(run.may_add_risk());
    assert!(state.entry_stop_reason.is_none());
}

#[test]
fn loss_producer_retains_the_first_trigger_after_marks_recover_and_emits_once() {
    use super::super::live_events::{flush_events, prepare_lifecycle};
    let (broker, _) = risk_fixture();
    apply_risk_fill(&broker, 1, 2245 * SCALE / 100 + 1, true);
    let events = prepare_lifecycle(&mut broker.state.lock()).unwrap();
    assert_eq!(events.len(), 2, "loss latch and sticky stop");
    flush_events(events).unwrap();
    let mut state = broker.state.lock();
    state.marks.insert("1".into(), 0.9);
    refresh_live_risk(&mut state);
    assert!(prepare_lifecycle(&mut state).unwrap().is_empty());
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.loss_trigger.unwrap().to_text(), format!("-{}", 20 * SCALE + 1));
    assert_eq!(run.emitter.last_seq(), 2);
    assert!(run.loss_latched);
    assert!(!run.may_add_risk());
}

#[tokio::test]
async fn contextual_account_failures_stop_safely_without_disclosing_raw_errors() {
    // The subprocess below observes the real drive's Debug-formatted WARNs,
    // not merely the safe protocol detail or Display of an adapter error.
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::WARN)
        .with_ansi(false).without_time().try_init();
    struct FaultAfterHealthyTick(Arc<crate::service::predict_fun::account::tests::SyntheticRpc>);
    #[async_trait::async_trait]
    impl super::super::Strategy for FaultAfterHealthyTick {
        fn name(&self) -> &'static str { "account-output-regression" }
        async fn tick(&mut self, _: &super::super::Ctx) -> anyhow::Result<()> {
            self.0.stage.store(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for (fault, expected) in [
        ("account_parse", "POSITIONS_FACT_INCOMPLETE"),
        ("account_short", "ACCOUNT_LEDGER_MISMATCH"),
        // An untyped failure is not provably a read that could not be made:
        // it keeps the fail-closed stop, with its existing UNKNOWN detail.
        ("account_unknown", "UNKNOWN"),
    ] {
        let (broker, _, market, _, _, transport) = sending_fixture(fault).await;
        let mut broker = Arc::try_unwrap(broker).ok().unwrap();
        let (limit, _) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "fixed_account");
        broker.limit = Some(Arc::new(limit));
        let broker = Arc::new(broker);
        let mut cfg = ctx_fixture(broker.identity.as_ref().unwrap());
        cfg.strategies.poll_secs = 1;
        let client = crate::service::predict_fun::PredictFunClient::new(&cfg).unwrap();
        let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
            reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
        let ctx = super::super::Ctx { cfg, client, data: Arc::new(data), broker: broker.clone() };
        let mut strategy = FaultAfterHealthyTick(transport.clone());
        tokio::time::timeout(std::time::Duration::from_secs(5), super::super::drive(&mut strategy, &ctx))
            .await.expect("bounded original drive").unwrap();
        {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.control.lock().stop.as_ref().and_then(|stop| stop.detail.as_deref()), Some(expected), "{fault}");
            assert!(run.account_read.is_none());
            assert!(!run.may_add_risk());
            assert!(run.stop_event_emitted);
        }
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.emitter.is_finished());
        assert_eq!(run.control.lock().stop.as_ref().and_then(|stop| stop.detail.as_deref()), Some(expected), "first stop survives cleanup");
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    }
    // A read that could not be made is not a fact gap: the window degrades,
    // it adds no risk, and it keeps observing instead of ending at once.
    for fault in ["account_http", "account_transport", "account_venue"] {
        let (broker, _, market, _, _, transport) = sending_fixture(fault).await;
        let mut broker = Arc::try_unwrap(broker).ok().unwrap();
        let (limit, _) = crate::service::predict_fun::controlled::synthetic_controlled_trader(200, "fixed_account");
        broker.limit = Some(Arc::new(limit));
        let broker = Arc::new(broker);
        let mut cfg = ctx_fixture(broker.identity.as_ref().unwrap());
        cfg.strategies.poll_secs = 1;
        let client = crate::service::predict_fun::PredictFunClient::new(&cfg).unwrap();
        let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
            reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
        let ctx = super::super::Ctx { cfg, client, data: Arc::new(data), broker: broker.clone() };
        let mut strategy = FaultAfterHealthyTick(transport.clone());
        // The window keeps observing while it is degraded. If drive returned
        // early on the unreadable account (the run-016 failure shape) this
        // bound would not expire, and the window's own deadline is far away.
        let observing = tokio::time::timeout(std::time::Duration::from_secs(3),
            super::super::drive(&mut strategy, &ctx)).await;
        assert!(observing.is_err(), "{fault}: a degraded window keeps observing");
        {
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert!(run.degraded(), "{fault}");
            assert!(run.control.lock().stop.is_none(), "{fault}: a degraded window is not stopped");
            assert!(!run.may_add_risk(), "{fault}: a degraded window adds no risk");
            assert!(!run.must_end(), "{fault}: a degraded window is not finished");
            assert!(run.account_read.is_none());
        }
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        // The facts come back: one successful read restores the window by
        // itself, without any restart and without its own stop.
        transport.stage.store(0, Ordering::SeqCst);
        live::check_runtime_account(&broker, false).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(!run.degraded(), "{fault}: a successful read restores the window");
        assert!(run.control.lock().stop.is_none(), "{fault}: recovery is not a stop");
        assert!(!run.must_end(), "{fault}: recovery does not end the window");
        assert!(run.may_add_risk(), "{fault}: a restored window may add risk");
        assert!(run.account_read.is_some(), "{fault}: the restored read is bound");
    }
}

#[test]
fn contextual_account_diagnostics_are_public_on_the_original_output_lane() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bot::runtime::live_control::candidate_tests::contextual_account_failures_stop_safely_without_disclosing_raw_errors", "--nocapture"])
        .output().unwrap();
    assert!(output.status.success());
    let stdout = std::str::from_utf8(&output.stdout).unwrap();
    let stderr = std::str::from_utf8(&output.stderr).unwrap();
    assert!(stdout.contains("order state check failed"), "real drive WARN must be observed: {stdout}{stderr}");
    assert!(!stdout.contains("SECRET_SENTINEL") && !stderr.contains("SECRET_SENTINEL"),
        "raw account cause escaped: {stdout}{stderr}");
    let events: Vec<serde_json::Value> = stdout.lines()
        .filter_map(|line| line.strip_prefix(super::super::live_events::LIVE_EVENT_PREFIX))
        .map(|line| serde_json::from_str(line).unwrap()).collect();
    let stops: Vec<_> = events.iter().filter(|event| event["event_kind"] == "stop").collect();
    assert_eq!(stops.len(), 3);
    let finals: Vec<_> = events.iter().filter(|event| event["event_kind"] == "final").collect();
    assert_eq!(finals.len(), 3);
    for (stop, terminal) in stops.iter().zip(finals) {
        assert_eq!(stop["payload"]["reason"], "RECONCILIATION_REQUIRED");
        assert_eq!(stop["payload"]["stop_detail"], terminal["payload"]["stop_detail"]);
    }
}

#[tokio::test]
async fn startup_account_errors_return_only_public_diagnostics() {
    for (fault, expected) in [
        ("account_http", "POSITIONS_READ_UNAVAILABLE_HTTP_503"),
        ("account_transport", "POSITIONS_READ_UNAVAILABLE"),
        ("account_parse", "POSITIONS_FACT_INCOMPLETE"),
        ("account_short", "ACCOUNT_LEDGER_MISMATCH"),
        ("account_venue", "RPC_JSON"),
        ("account_unknown", "UNKNOWN"),
    ] {
        let (broker, _, _, _, _, transport) = sending_fixture(fault).await;
        broker.state.lock().live.baseline = None;
        transport.stage.store(1, Ordering::SeqCst);
        let error = live::seed_predict(&broker).await.unwrap_err();
        assert!(!format!("{error:?}").contains("SECRET_SENTINEL"), "{fault}");
        assert_eq!(error.chain().count(), 1, "{fault}");
        assert!(error.to_string().contains(expected), "{fault}: {error}");
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let stop = run.control.lock().stop.clone().expect("the failed startup stops the run");
        assert_eq!(stop.reason, "RECONCILIATION_REQUIRED", "{fault}");
        assert_eq!(stop.detail.as_deref(), Some(expected),
            "{fault}: the startup stop names the code the read held");
        assert!(run.stop_event_emitted, "{fault}");
        assert!(!run.may_add_risk());
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    }
}

/// The window's C0 is the cash it starts with. A startup whose observed
/// collateral differs from the declared budget is refused before any order, so a
/// mismatched window can never fill first and stop incomplete at the end.
#[tokio::test]
async fn startup_refuses_a_budget_that_is_not_the_observed_cash_before_any_order() {
    let (broker, _clock, _market, _book, _candidate, transport) = sending_fixture("terminal").await;
    let cash = broker.taker.as_ref().unwrap().account_snapshot().await.unwrap()
        .collateral_atoms.unwrap();
    {
        let mut state = broker.state.lock();
        state.live.baseline = None;
        let run = state.live.controlled.as_mut().unwrap();
        run.account_read = None;
        // One atom short is still not the observed cash: equality is exact.
        run.options.params.budget = cash - U256::from(1u64);
    }
    let error = live::seed_predict(&broker).await.unwrap_err();
    assert!(error.to_string().contains("CASH_BUDGET_MISMATCH"), "{error}");
    // Refused before any signature or POST was ever attempted.
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    let state = broker.state.lock();
    assert!(state.live.baseline.is_none(), "a mismatch seeds no baseline");
    let stop = state.live.controlled.as_ref().unwrap().control.lock().stop.clone().unwrap();
    assert_eq!(stop.reason, "RECONCILIATION_REQUIRED");
    assert_eq!(stop.detail.as_deref(), Some("CASH_BUDGET_MISMATCH"));
}

/// The mirror case: a budget declared at the exact observed cash keeps the
/// window's budget-relative cash equal to the real cash, so the identity is
/// COMPLETE and it never reports FACT_INCOMPLETE.
#[test]
fn a_budget_equal_to_the_observed_cash_keeps_the_accounting_complete() {
    let (broker, _) = risk_fixture();
    // The window declares exactly the cash it starts with (100), not the
    // historical 98 allocation.
    broker.state.lock().live.controlled.as_mut().unwrap().options.params.budget =
        U256::from(100 * SCALE);
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    let state = broker.state.lock();
    let accounting = super::super::live_events::state_accounting(&state);
    // C0 equals the observed cash, so budget-relative cash is the actual cash.
    assert_eq!(accounting.cash, Some(U256::from(98 * SCALE)));
    assert_eq!(accounting.status, "COMPLETE");
    assert_eq!(accounting.reason, "NONE");
}

/// The venue seam of a startup account read: `seed_predict` consumes exactly
/// this call, so the code a real read would hold is injected here unchanged.
struct StartupReadFailure {
    account_error: &'static str,
}

#[async_trait::async_trait]
impl crate::service::venue::OrderVenue for StartupReadFailure {
    fn id(&self) -> VenueId {
        VenueId::PredictFun
    }

    async fn place(
        &self,
        _planned: &crate::models::PlannedOrder,
        _expiration_secs: u64,
        _live: bool,
    ) -> anyhow::Result<crate::service::venue::OrderTicket> {
        unreachable!("a failing startup read never places an order")
    }

    async fn account_snapshot(&self) -> anyhow::Result<crate::service::venue::AccountSnapshot> {
        // Exactly the shape the native reader returns: one bounded code, no
        // transport text, no cause chain.
        Err(anyhow::Error::msg(self.account_error))
    }
}

fn startup_failure_broker(account_error: &'static str) -> (Broker, Arc<crate::service::observation::FixtureClock>) {
    let (mut broker, clock) = risk_fixture();
    {
        let mut state = broker.state.lock();
        state.live.baseline = None;
        // Startup must not be servable from a prior receipt.
        state.live.controlled.as_mut().unwrap().account_read = None;
    }
    broker.taker = Some(Arc::new(StartupReadFailure { account_error }));
    (broker, clock)
}

/// The stop the startup read failure takes names the code it actually held,
/// and nothing else about the stop moves: same reason, same takeover flag.
#[tokio::test]
async fn startup_read_failure_names_the_code_it_held_in_the_stop() {
    for (account_error, expected) in [
        ("RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED", "HISTORY_NOT_CLOSED"),
        ("RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS", "UNEXPLAINED_POSITIONS"),
        // The venue/route failures name a class and a cause; the stop keeps the
        // cause it already knows instead of degrading to UNKNOWN.
        ("FACT_INCOMPLETE:MISSING_TOKEN", "MISSING_TOKEN"),
        ("READ_UNAVAILABLE:PAGE_CAP", "PAGE_CAP"),
        // A forged token, then a bare 64-hex key: neither may reach the stop,
        // and neither may leave the code claiming a cause it cannot name.
        ("RECONCILIATION_REQUIRED:https://secret.invalid/?jwt=SECRET_SENTINEL", "UNKNOWN"),
        ("RECONCILIATION_REQUIRED:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "UNKNOWN"),
    ] {
        let (broker, _clock) = startup_failure_broker(account_error);
        let error = live::seed_predict(&broker).await.unwrap_err();
        assert!(!format!("{error:?}").contains("SECRET_SENTINEL"), "{account_error}");
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let stop = run.control.lock().stop.clone().expect("the failed read stops the run");
        assert_eq!(stop.reason, "RECONCILIATION_REQUIRED", "{account_error}");
        assert_eq!(stop.detail.as_deref(), Some(expected), "{account_error}");
        assert!(stop.takeover_required, "{account_error}");
        assert!(run.stop_event_emitted, "{account_error}");
        let detail = stop.detail.clone().unwrap();
        assert!(detail.len() <= 64
            && detail.bytes().all(|byte| byte.is_ascii_uppercase()
                || byte.is_ascii_digit() || byte == b'_'),
            "{account_error}: one bounded public code, got {detail}");
        assert!(!detail.contains("SECRET_SENTINEL") && !detail.contains("://"),
            "{account_error}: nothing secret or URI-shaped leaves the stop");
    }
}

#[tokio::test]
async fn official_runtime_read_mismatch_emits_failed_reconciliation_and_sticky_stop() {
    let (broker, _, _, _, _, transport) = sending_fixture("terminal").await;
    {
        // Simulate a changed official cash baseline, not a fabricated wire event.
        let mut state = broker.state.lock();
        state.live.baseline.as_mut().unwrap().collateral_atoms = Some(U256::from(99 * SCALE));
    }
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.lock().await;
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.account_read.is_none());
    assert!(!run.may_add_risk());
    assert!(run.stop_event_emitted);
    assert_eq!(run.emitter.last_seq(), 3, "EVALUATE, failed official read, stop");
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
}

/// run005's silhouette: the runtime read stops the run, then the bounded
/// shutdown writes the terminal record. Both records keep the same code.
#[tokio::test]
async fn live_shutdown_after_a_runtime_read_mismatch_keeps_the_failure_code() {
    let (broker, _, market, _, _, transport) = sending_fixture("terminal").await;
    {
        // The same changed official baseline the mismatch test uses.
        let mut state = broker.state.lock();
        state.live.baseline.as_mut().unwrap().collateral_atoms = Some(U256::from(99 * SCALE));
    }
    {
        let lane = broker.live_operation_lane().unwrap();
        let _operation = lane.lock().await;
        assert!(live::check_runtime_account(&broker, false).await.is_err());
    }
    let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    tokio::time::timeout(std::time::Duration::from_secs(5), broker.finish_controlled(&data))
        .await.expect("the bounded shutdown ends");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().state, LiveRunState::Handoff);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
}

/// The stop names the account path that stopped the run, not only the reason:
/// the code the check already held reaches the stop and the terminal record.
#[test]
fn the_runtime_read_stop_and_final_name_the_failure_code() {
    let events = capture_production_test(
        "live_shutdown_after_a_runtime_read_mismatch_keeps_the_failure_code");
    let stop = events.iter().find(|event| event["event_kind"] == "stop").unwrap();
    assert_eq!(stop["payload"]["reason"], "RECONCILIATION_REQUIRED");
    assert_eq!(stop["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["stop_reason"], "RECONCILIATION_REQUIRED");
    assert_eq!(terminal["payload"]["stop_detail"], "ACCOUNT_LEDGER_MISMATCH");
}

/// The startup path's own wire: the read failure's code reaches both the stop
/// and the terminal record, in the fields that already existed.
#[tokio::test]
async fn startup_read_failure_stop_and_final_name_the_failure_code() {
    let (broker, clock) = startup_failure_broker("RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED");
    let (market, _, _) = evaluate_live_fixture(&clock, true);
    let broker = Arc::new(broker);
    assert!(live::seed_predict(&broker).await.is_err(), "the startup read fails");
    let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    tokio::time::timeout(std::time::Duration::from_secs(5), broker.finish_controlled(&data))
        .await.expect("the bounded shutdown ends");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().state, LiveRunState::Handoff);
    assert_eq!(run.control.lock().stop.as_ref().and_then(|stop| stop.detail.as_deref()),
        Some("HISTORY_NOT_CLOSED"), "the first stop keeps the code it held");
}

#[test]
fn the_startup_stop_and_final_name_the_failure_code() {
    let events = capture_production_test("startup_read_failure_stop_and_final_name_the_failure_code");
    let stop = events.iter().find(|event| event["event_kind"] == "stop").unwrap();
    assert_eq!(stop["payload"]["reason"], "RECONCILIATION_REQUIRED");
    assert_eq!(stop["payload"]["stop_detail"], "HISTORY_NOT_CLOSED");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["stop_reason"], "RECONCILIATION_REQUIRED");
    assert_eq!(terminal["payload"]["stop_detail"], "HISTORY_NOT_CLOSED");
}

fn capture_production_test(test_name: &str) -> Vec<serde_json::Value> {
    use super::super::live_events::{LiveEventEmitter, LIVE_EVENT_PREFIX};
    // Original core/Broker/Predict synthetic consumer, not invented payloads.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &format!("bot::runtime::live_control::candidate_tests::{test_name}"), "--nocapture"])
        .current_dir(env!("CARGO_MANIFEST_DIR")).output().unwrap();
    assert!(output.status.success(), "{test_name}: {}\n{}",
        String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    let text = std::str::from_utf8(&output.stdout).unwrap();
    let events: Vec<_> = text.lines().filter_map(|line| line.split_once(LIVE_EVENT_PREFIX))
        .map(|(_, json)| LiveEventEmitter::validate_bytes(json.as_bytes()).unwrap()).collect();
    assert!(!events.is_empty(), "{test_name}");
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["event_seq"], (index + 1) as u64, "{test_name}");
        assert_eq!(event["run_id"], events[0]["run_id"]);
        assert_eq!(event["risk_window_id"], events[0]["risk_window_id"]);
    }
    use std::io::Write;
    std::io::stdout().write_all(&output.stdout).unwrap();
    events
}

#[tokio::test]
async fn one_atom_unexplained_raw_inventory_still_refuses_reconciliation_after_a_real_fill() {
    let (broker, _, market, book, candidate, transport, trace) = sending_fixture_with_trace("controlled_fill").await;
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    let id = outcome.attempt_id.unwrap();
    let net = broker.state.lock().live.net_inventory[&id];
    trace.extra_position_atoms.store(1, Ordering::SeqCst);
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.lock().await;
    assert!(live::check_runtime_account(&broker, false).await.is_err());
    let state = broker.state.lock();
    assert_eq!(state.live.net_inventory[&id], net, "foreign atoms never enter the ledger");
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.account_read.is_none());
    assert!(!run.may_add_risk());
    assert!(run.stop_event_emitted);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[test]
fn two_round_producer_associates_actual_fees_payouts_and_reconciled_owned_positions() {
    let events = capture_production_test("two_official_rounds_use_one_core_ledger_and_emitter_with_actual_fee_and_payouts");
    for (market, token, payout) in [("1", "77", 0.0), ("2", "177", 1.0)] {
        let reserve = events.iter().find(|event| event["market_id"] == market
            && event["payload"]["action"] == "RESERVE").unwrap();
        let order = events.iter().find(|event| event["market_id"] == market
            && event["payload"]["submission"] == "ACCEPTED").unwrap();
        let execution = events.iter().find(|event| event["market_id"] == market
            && event["event_kind"] == "execution").unwrap();
        assert_eq!(reserve["token"], token);
        assert_eq!(order["token"], token);
        assert_eq!(execution["token"], token);
        assert_eq!(reserve["payload"]["attempt_id"], order["payload"]["attempt_id"]);
        assert_eq!(reserve["payload"]["candidate_id"], order["payload"]["candidate_id"]);
        assert_eq!(execution["payload"]["attempt_id"], order["payload"]["attempt_id"]);
        assert_eq!(execution["payload"]["signed_hash"], order["payload"]["signed_hash"]);
        assert_eq!(order["payload"]["request_gross_atoms"], (25*SCALE/10).to_string());
        assert_eq!(order["payload"]["max_cash_atoms"], SCALE.to_string());
        assert_eq!(order["payload"]["min_net_shares_atoms"], (245*SCALE/100).to_string());
        for (field, amount) in [("gross_q_atoms",25*SCALE/10),("gross_cash_atoms",SCALE),
            ("net_cash_atoms",SCALE),("share_fee_atoms",SCALE/40),("net_inventory_atoms",2475*SCALE/1000)] {
            assert_eq!(execution["payload"]["delta"][field], amount.to_string());
        }
        for fact in ["gross_cash", "collateral_fee", "share_fee", "net_cash", "net_inventory", "vwap"] {
            assert_eq!(execution["payload"]["facts"][fact]["coverage_gross_atoms"], (25*SCALE/10).to_string());
        }
        let paid = events.iter().find(|event| event["market_id"] == market && event["event_kind"] == "payout").unwrap();
        assert_eq!(paid["payload"]["position_net_atoms"], (2475*SCALE/1000).to_string());
        assert_eq!(paid["payload"]["payout_yes"], payout);
        assert_eq!(paid["payload"]["cash_received_atoms"], serde_json::Value::Null);
    }
    let stops: Vec<_> = events.iter().filter(|event| event["event_kind"] == "stop").collect();
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0]["payload"]["reason"], "CASH_PENDING");
    for event in events.iter().filter(|event| event["event_kind"] == "reconciliation") {
        assert_eq!(event["payload"]["complete"], true, "exactly reconciled owned positions are not foreign");
        assert_eq!(event["payload"]["foreign_risk"], false);
    }
}

#[test]
fn real_producer_streams_include_acceptance_actual_delta_and_unsent_release() {
    for (test_name, sent, signed, stopped) in [
        ("original_core_broker_predict_transport_reaches_a_nonzero_fee_post_after_ack", true, true, false),
        ("eof_observed_before_ack_closes_the_same_state_before_original_post", false, true, true),
        // live004: a candidate refused for a moved book leaves no stop event;
        // the window stays open for the next decision.
        ("preparation_refusal_closes_only_the_unsent_cash_not_the_candidate_evidence", false, false, false),
    ] {
        let events = capture_production_test(test_name);
        let reservation = events.iter().find(|event| event["payload"]["action"] == "RESERVE").unwrap();
        let orders: Vec<_> = events.iter().filter(|event| event["event_kind"] == "order").collect();
        let last = orders.last().unwrap();
        assert_eq!(last["payload"]["candidate_id"], reservation["payload"]["candidate_id"]);
        assert_eq!(last["payload"]["attempt_id"], reservation["payload"]["attempt_id"]);
        assert_eq!(!last["payload"]["signed_hash"].is_null(), signed);
        for order in &orders {
            assert_eq!(order["market_id"], reservation["market_id"]);
            assert_eq!(order["token"], reservation["token"]);
            if !order["payload"]["signed_hash"].is_null() {
                assert_eq!(order["payload"]["signed_hash"], last["payload"]["signed_hash"]);
            }
        }
        let release = events.iter().find(|event| event["payload"]["action"] == "RELEASE").unwrap();
        assert_eq!(release["payload"]["remaining_atoms"], "0");
        assert_eq!(release["payload"]["attempt_id"], reservation["payload"]["attempt_id"]);
        if sent {
            assert!(orders.iter().any(|event| event["payload"]["submission"] == "ACCEPTED"));
            let execution = events.iter().find(|event| event["event_kind"] == "execution").unwrap();
            assert_eq!(execution["payload"]["signed_hash"], last["payload"]["signed_hash"]);
            assert_eq!(execution["payload"]["venue_order_id"], last["payload"]["venue_order_id"]);
            assert_eq!(execution["payload"]["delta"]["gross_q_atoms"], "0");
            assert_eq!(execution["payload"]["delta"]["net_inventory_atoms"], "0");
            assert_eq!(execution["payload"]["facts"]["vwap"]["status"], "NOT_APPLICABLE");
            let executions: Vec<_> = events.iter().filter(|event| event["event_kind"] == "execution").collect();
            assert_eq!(executions.len(), 2, "initial response and original sync consumer");
            for event in executions {
                assert_eq!(event["payload"]["signed_hash"], last["payload"]["signed_hash"]);
                for field in ["gross_q_atoms", "gross_cash_atoms", "collateral_fee_atoms",
                    "share_fee_atoms", "net_cash_atoms", "net_inventory_atoms"]
                { assert_eq!(event["payload"]["delta"][field], "0"); }
            }
        } else {
            assert_eq!(last["payload"]["submission"], "NOT_SUBMITTED");
            assert_eq!(last["payload"]["attempt_phase"], "CLOSED");
            assert!(!events.iter().any(|event| event["event_kind"] == "execution"));
            assert_eq!(events.iter().filter(|event| event["event_kind"] == "stop").count(),
                if stopped { 1 } else { 0 }, "{test_name}");
        }
        assert!(events.iter().any(|event| event["event_kind"] == "reconciliation"));
    }
    let events = capture_production_test("loss_producer_retains_the_first_trigger_after_marks_recover_and_emits_once");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event_kind"], "loss_latch");
    assert_eq!(events[0]["payload"]["trigger_net_pnl_atoms"], format!("-{}", 20 * SCALE + 1));
    assert_eq!(events[1]["event_kind"], "stop");
    assert_eq!(events[1]["payload"]["reason"], "LOSS_LATCH");
    let events = capture_production_test("official_runtime_read_mismatch_emits_failed_reconciliation_and_sticky_stop");
    assert_eq!(events.len(), 3);
    assert_eq!(events[1]["event_kind"], "reconciliation");
    assert_eq!(events[1]["payload"]["complete"], false);
    assert_eq!(events[1]["payload"]["collateral_atoms"], (100 * SCALE).to_string());
    assert_eq!(events[2]["payload"]["reason"], "RECONCILIATION_REQUIRED");
}

/// Only the official data boundary is fake; Broker settlement/final are real.
pub(super) struct OfficialResultData {
    result: Mutex<crate::service::venue::TradableMarket>,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::service::venue::MarketData for OfficialResultData {
    async fn markets(&self, _: &crate::service::venue::MarketQuery)
        -> anyhow::Result<Vec<crate::service::venue::TradableMarket>> { panic!("unexpected scan") }
    async fn market(&self, _: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.result.lock().clone())
    }
    async fn with_schedule(&self, _: &crate::service::venue::TradableMarket)
        -> anyhow::Result<crate::service::venue::TradableMarket> { panic!("unexpected schedule") }
    async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> {
        panic!("settlement must not invent a mark")
    }
}

pub(super) fn official_result_fixture(payout: f64) -> (Broker, OfficialResultData) {
    let (broker, clock) = risk_fixture();
    let (mut market, _, _) = evaluate_live_fixture(&clock, false);
    market.yes_token = "1".into();
    apply_risk_fill(&broker, market.id, 2 * SCALE, true);
    broker.state.lock().live.controlled.as_mut().unwrap().traded.insert(market.id, market.clone());
    market.open = false;
    market.end_price = Some(if payout == 1.0 { 101.0 } else if payout == 0.0 { 99.0 } else { 100.0 });
    market.payout_yes = Some(payout);
    market.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some(if payout == 0.0 { "LOST" } else { "WON" }.into()),
        no_status: Some(if payout == 0.0 { "WON" } else { "LOST" }.into()),
        decimal_precision: None, price_feed_provider: None,
    });
    (broker, OfficialResultData { result: Mutex::new(market), reads: Default::default() })
}

async fn assert_normal_result_wait(broker: &Broker, mut market: crate::service::venue::TradableMarket) {
    use crate::bot::runtime::live_accounting::{project_window, StateFacts};
    market.open = true;
    market.end_price = None;
    market.payout_yes = None;
    market.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("REGISTERED".into()), trading_status: Some("OPEN".into()),
        yes_status: None, no_status: None, decimal_precision: None, price_feed_provider: None,
    });
    let snapshot = || {
        let state = broker.state.lock();
        (state.live.net_cash.clone(), state.live.net_inventory.clone(), state.live.reservations.clone(),
            state.live.controlled.as_ref().unwrap().attempt_outcomes.clone(),
            (project_window(&StateFacts { state: &state.live }, &Default::default(), None).cash,
             project_window(&StateFacts { state: &state.live }, &Default::default(), None).cost))
    };
    let before = snapshot();
    let data = OfficialResultData { result: Mutex::new(market), reads: Default::default() };
    for _ in 0..3 {
        broker.settle_resolved(&data).await.unwrap();
        assert_eq!(snapshot(), before, "normal waiting must preserve cost, inventory, reserve, outcomes and cash");
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::Ready, "normal OPEN is not a read failure");
        assert!(run.may_add_risk());
    }
    assert_eq!(data.reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn official_claims_replace_marks_never_credit_cash_and_zero_payout_releases_only_basis() {
    use crate::bot::runtime::live_accounting::{self, SignedAtoms, StateFacts};
    for payout in [0.0, 0.5, 1.0] {
        let (broker, data) = official_result_fixture(payout);
        let pending = data.result.lock().clone();
        assert_normal_result_wait(&broker, pending).await;
        broker.settle_resolved(&data).await.unwrap();
        let seq = broker.state.lock().live.controlled.as_ref().unwrap().emitter.last_seq();
        broker.settle_resolved(&data).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.emitter.last_seq(), seq, "repeated official reading must not repay or emit again");
        assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
        assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
        let projected = live_accounting::project_window(&StateFacts { state: &state.live }, &Default::default(), None);
        assert_eq!(projected.status, live_accounting::AccountingStatus::Complete, "official claims need no book");
        assert_eq!(projected.cash, U256::from(96 * SCALE));
        let claim = live_accounting::exact_floor_mul(U256::from(49 * SCALE / 10), payout).unwrap();
        assert_eq!(projected.nav, SignedAtoms::positive(U256::from(96 * SCALE) + claim));
        assert_eq!(projected.pending_claim_value, (!claim.is_zero()).then_some(claim));
        let risk = live_accounting::risk_exposure(&state.live).unwrap();
        if payout == 0.0 {
            assert_eq!(projected.cost, U256::ZERO);
            assert_eq!(projected.realized, SignedAtoms::negative(U256::from(2 * SCALE)));
            assert_eq!(projected.unrealized, SignedAtoms::zero());
            assert_eq!(risk.cost, U256::ZERO);
        } else {
            assert_eq!(projected.cost, U256::from(2 * SCALE));
            assert_eq!(risk.cost, U256::from(2 * SCALE));
            assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
            assert!(!run.may_add_risk());
        }
        assert!(state.paper.is_none());
    }
}

#[tokio::test]
async fn official_result_identity_dispute_and_revision_do_not_rewrite_ledger() {
    for fault in ["token", "start", "duration", "feed", "dispute", "contradiction", "revision", "market"] {
        let (broker, data) = official_result_fixture(1.0);
        if fault == "revision" { broker.settle_resolved(&data).await.unwrap(); }
        {
            let mut market = data.result.lock();
            match fault {
                "token" => market.yes_token = "999".into(),
                "start" => market.start_price = Some(102.0),
                "duration" => market.ends_at = market.ends_at.map(|end| end + chrono::Duration::seconds(1)),
                "feed" => market.feed_symbol = Some("ETHUSDT".into()),
                "dispute" => market.contested = true,
                "contradiction" => market.payout_yes = Some(0.0),
                "revision" => market.end_price = Some(102.0),
                "market" => market.id = 2,
                _ => unreachable!(),
            }
        }
        broker.settle_resolved(&data).await.unwrap();
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
        assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
        if fault == "dispute" {
            assert!(run.attempt_outcomes.is_empty());
        } else {
            assert!(!run.may_add_risk(), "{fault}");
            if fault == "revision" { assert_eq!(run.attempt_outcomes[&1].result_prices, (100.0, 101.0)); }
            else { assert!(run.attempt_outcomes.is_empty()); }
        }
    }
}

#[tokio::test]
async fn incomplete_official_results_preserve_live_risk_and_sticky_reason() {
    use crate::bot::runtime::live_accounting::{project_window, StateFacts};
    for prior in ["READY", "DEADLINE", "CASH_PENDING", "OPEN_ATTEMPT"] {
        let expected_reason = if ["READY", "OPEN_ATTEMPT", "CASH_PENDING"].contains(&prior) { "DATA_UNAVAILABLE" } else { prior };
        for field in ["yes", "no", "start", "end", "strike", "feed", "empty_feed", "end_price",
            "payout", "facts", "yes_status", "no_status", "all", "open_feed", "open_dispute",
            "open_unknown", "open_terminal", "open_end", "open_payout", "open_won"] {
            let (broker, data) = official_result_fixture(1.0);
            let complete = data.result.lock().clone();
            if prior == "CASH_PENDING" { broker.settle_resolved(&data).await.unwrap(); }
            if prior == "OPEN_ATTEMPT" { apply_risk_fill(&broker, complete.id, SCALE, false); }
            if prior == "DEADLINE" {
                broker.state.lock().live.controlled.as_mut().unwrap().stop("DEADLINE", true, Utc::now());
            }
            let (cash, inventory, reservations, outcomes, cash_balance) = {
                let state = broker.state.lock();
                (state.live.net_cash.clone(), state.live.net_inventory.clone(), state.live.reservations.clone(),
                    state.live.controlled.as_ref().unwrap().attempt_outcomes.clone(),
                    project_window(&StateFacts { state: &state.live }, &Default::default(), None).cash)
            };
            {
                let mut market = data.result.lock();
                if field.starts_with("open_") {
                    market.open = true; market.end_price = None; market.payout_yes = None;
                    let facts = market.round_facts.as_mut().unwrap();
                    facts.oracle_status = Some("REGISTERED".into()); facts.trading_status = Some("OPEN".into());
                    facts.yes_status = None; facts.no_status = None;
                }
                match field {
                    "open_feed" => market.feed_symbol = None,
                    "open_dispute" => market.contested = true,
                    "open_unknown" => market.round_facts.as_mut().unwrap().oracle_status = Some("UNKNOWN".into()),
                    "open_terminal" => market.round_facts.as_mut().unwrap().oracle_status = Some("RESOLVED".into()),
                    "open_end" => market.end_price = Some(101.0),
                    "open_payout" => market.payout_yes = Some(1.0),
                    "open_won" => market.round_facts.as_mut().unwrap().yes_status = Some("WON".into()),
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
                    "all" => {
                        market.yes_token.clear(); market.no_token.clear();
                        market.starts_at = None; market.ends_at = None; market.start_price = None;
                        market.feed_symbol = None; market.end_price = None; market.payout_yes = None;
                        market.round_facts = None;
                    }
                    _ => unreachable!(),
                }
            }
            broker.settle_resolved(&data).await.unwrap();
            {
                let state = broker.state.lock();
                let run = state.live.controlled.as_ref().unwrap();
                assert_eq!(state.live.net_cash, cash, "{prior}/{field}");
                assert_eq!(state.live.net_inventory, inventory);
                assert_eq!(state.live.reservations, reservations);
                assert_eq!(run.attempt_outcomes, outcomes, "{prior}/{field}");
                assert_eq!(project_window(&StateFacts { state: &state.live }, &Default::default(), None).cash, cash_balance);
                if prior == "OPEN_ATTEMPT" { assert!(reservations.values().any(|value| !value.is_zero())); }
                assert!(!run.may_add_risk());
                let control = run.control.lock();
                let stop = control.stop.as_ref().unwrap();
                assert_eq!(stop.reason, expected_reason, "{field}");
                assert!(stop.takeover_required);
                assert_eq!(control.state, LiveRunState::StopNewRisk);
            }
            *data.result.lock() = complete;
            broker.settle_resolved(&data).await.unwrap();
            let seq = broker.state.lock().live.controlled.as_ref().unwrap().emitter.last_seq();
            broker.settle_resolved(&data).await.unwrap();
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.emitter.last_seq(), seq);
            assert_eq!(run.attempt_outcomes.len(), 1);
            assert_eq!(run.attempt_outcomes[&1].result_prices, (100.0, 101.0));
            assert_eq!(state.live.net_cash, cash);
            assert_eq!(state.live.net_inventory, inventory);
            assert_eq!(state.live.reservations, reservations);
            assert!(!run.may_add_risk());
            assert_eq!(run.control.lock().stop.as_ref().unwrap().reason,
                expected_reason);
        }
    }
}

#[tokio::test]
async fn live_known_conflicts_override_missing_without_rewriting_outcomes() {
    for applied in [false, true] {
        for fault in ["token", "time", "strike", "feed", "same_tokens", "nan", "zero", "negative", "infinity", "invalid_end", "end_price", "payout", "outcome", "dispute", "nonterminal"] {
            let (broker, data) = official_result_fixture(1.0);
            if applied { broker.settle_resolved(&data).await.unwrap(); }
            let outcomes = broker.state.lock().live.controlled.as_ref().unwrap().attempt_outcomes.clone();
            let seq_before = broker.state.lock().live.controlled.as_ref().unwrap().emitter.last_seq();
            {
                let mut market = data.result.lock();
                match fault {
                    "token" => { market.yes_token = "999".into(); market.feed_symbol = None; }
                    "time" => { market.ends_at = market.ends_at.map(|value| value + chrono::Duration::seconds(1)); market.starts_at = None; }
                    "strike" => { market.start_price = Some(102.0); market.feed_symbol = None; }
                    "feed" => { market.feed_symbol = Some("ETHUSDT".into()); market.start_price = None; }
                    "same_tokens" => { market.no_token = market.yes_token.clone(); market.feed_symbol = None; }
                    "nan" => { market.start_price = Some(f64::NAN); market.ends_at = None; }
                    "zero" => { market.start_price = Some(0.0); market.starts_at = None; }
                    "negative" => { market.start_price = Some(-1.0); market.feed_symbol = None; }
                    "infinity" => { market.start_price = Some(f64::INFINITY); market.no_token.clear(); }
                    "invalid_end" => { market.end_price = Some(f64::NAN); market.round_facts = None; }
                    "end_price" => { market.end_price = Some(102.0); market.round_facts = None; }
                    "payout" => { market.payout_yes = Some(0.0); market.end_price = None; }
                    "outcome" => { market.round_facts.as_mut().unwrap().yes_status = Some("LOST".into()); market.round_facts.as_mut().unwrap().no_status = None; }
                    "dispute" => market.contested = true,
                    "nonterminal" => market.round_facts.as_mut().unwrap().oracle_status = Some("PENDING".into()),
                    _ => unreachable!(),
                }
            }
            broker.settle_resolved(&data).await.unwrap();
            let state = broker.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            assert_eq!(run.attempt_outcomes, outcomes, "{fault}/{applied}");
            assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
            assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
            let conflict = applied || !["end_price", "payout", "outcome", "dispute", "nonterminal"].contains(&fault);
            let control = run.control.lock();
            if conflict {
                assert!(!control.may_add_risk());
                assert_eq!(control.stop.as_ref().unwrap().reason, "FACT_CONFLICT");
                assert_eq!(control.state, LiveRunState::StopNewRisk);
                let events = run.emitter.last_seq();
                assert!(events > seq_before, "{fault}/{applied}: conflict must emit even with a sticky stop");
            }
        }
    }
}

#[tokio::test]
async fn original_live_finish_emits_once_after_watcher_stop_and_seals_all_producers() {
    let (broker, data) = official_result_fixture(1.0);
    channel::ControlWatcher::parent_lost(&broker);
    broker.finish_controlled(&data).await;
    let (seq, emitter) = {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.emitter.is_finished());
        assert_eq!(run.control.lock().state, LiveRunState::Handoff);
        assert!(!run.may_add_risk());
        assert!(state.holdings.contains_key("1"), "no ordinary-live settle removal");
        (run.emitter.last_seq(), run.emitter.clone())
    };
    let reads = data.reads.load(Ordering::SeqCst);
    broker.finish_controlled(&data).await;
    broker.settle_resolved(&data).await.unwrap();
    assert_eq!(data.reads.load(Ordering::SeqCst), reads);
    assert_eq!(emitter.last_seq(), seq);
    assert!(!emitter.output_failed());
}

#[test]
fn captured_original_live_final_has_independent_cash_and_execution_status_and_no_later_event() {
    let events = capture_production_test("original_live_finish_emits_once_after_watcher_stop_and_seals_all_producers");
    let kinds: Vec<_> = events.iter().map(|event| event["event_kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["payout", "stop", "recovery", "final"]);
    assert_eq!(events[0]["payload"]["status"], "CASH_PENDING");
    assert!(events[0]["payload"]["cash_received_atoms"].is_null());
    assert_eq!(events[2]["payload"]["policy"], "COLD_START_REFUSE");
    assert_eq!(events[3]["payload"]["execution_complete"], true);
    assert_eq!(events[3]["payload"]["cash_complete"], false);
    assert_eq!(events[3]["payload"]["stop_reason"], "PARENT_LOST");
    assert_eq!(events[3]["payload"]["takeover_required"], true);
    assert_eq!(events[3]["payload"]["positions"][0]["net_shares_atoms"], (49 * SCALE / 10).to_string());
    assert!(events[3]["payload"]["unresolved"].as_array().unwrap().is_empty());
}

async fn assert_early_candidate_failure(fault: &str) {
        let (broker, _, mut market, book, candidate, transport) = sending_fixture("terminal").await;
        let mut broker = Arc::try_unwrap(broker).ok().unwrap();
        if fault == "missing_route" { broker.taker = None; }
        else { market.start_price = Some(999.0); }
        assert!(broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await.is_err());
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.pending_candidate.is_none());
        assert!(run.stop_event_emitted);
        assert!(!run.may_add_risk());
        assert!(state.live.attempts.is_empty());
        assert!(state.live.reservations.is_empty());
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(run.emitter.last_seq(), 2, "EVALUATE and stop, not a fictional execution");
}

#[tokio::test]
async fn early_candidate_failures_clear_only_the_candidate_without_inventing_attempts() {
    assert_early_candidate_failure("missing_route").await;
}

#[tokio::test]
async fn changed_candidate_inputs_close_without_inventing_attempts() {
    assert_early_candidate_failure("changed_input").await;
}

#[test]
fn mismatched_snapshot_projects_stop_without_execution_or_ledger_rollback() {
    let (broker, _) = risk_fixture();
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    let mut reading = snapshot(2, 5 * SCALE, 2 * SCALE, SCALE / 10);
    reading.identity = broker.state.lock().live.attempts[&1].lock().identity.clone();
    reading.identity.token_id = "999".into();
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.try_lock().unwrap();
    assert!(live::consume_snapshot(&broker, &reading).is_err());
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.emitter.last_seq(), 1);
    assert!(run.stop_event_emitted);
    assert!(!run.emitter.output_failed());
    assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
    assert_eq!(state.live.net_inventory[&1], U256::from(49 * SCALE / 10));
    assert_eq!(state.live.attempts[&1].lock().identity.token_id, "1");
    assert!(state.live.attempts[&1].lock().identity.signed_hash.is_some());
}

#[test]
fn original_observe_projects_loss_and_stop_without_waiting_for_another_snapshot() {
    let (broker, clock) = risk_fixture();
    let (mut market, _, _) = evaluate_live_fixture(&clock, false);
    market.yes_token = "1".into();
    apply_risk_fill(&broker, 1, 22 * SCALE, true);
    assert!(!broker.state.lock().live.controlled.as_ref().unwrap().loss_latched);
    let book = crate::bot::runtime::test_support::book(&[(0.005, 100.0)], &[(0.015, 100.0)]);
    broker.observe(&market, &book);
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.loss_latched);
    assert!(run.loss_event_emitted);
    assert!(run.stop_event_emitted);
    assert_eq!(run.emitter.last_seq(), 2);
}

#[test]
fn captured_failure_producers_never_invent_an_execution_for_missing_identity() {
    let events = capture_production_test("mismatched_snapshot_projects_stop_without_execution_or_ledger_rollback");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_kind"], "stop");
    assert_eq!(events[0]["payload"]["reason"], "FACT_CONFLICT");
    let events = capture_production_test("original_observe_projects_loss_and_stop_without_waiting_for_another_snapshot");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event_kind"], "loss_latch");
    assert_eq!(events[1]["event_kind"], "stop");
    let events = capture_production_test("early_candidate_failures_clear_only_the_candidate_without_inventing_attempts");
    let kinds: Vec<_> = events.iter().map(|event| event["event_kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["decision", "stop"]);
    let events = capture_production_test("changed_candidate_inputs_close_without_inventing_attempts");
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["event_kind"], "stop");
}

/// Only the official read is faulted; the adapter, sync, shutdown and wire are real.
struct ShutdownData {
    broker: Arc<Broker>,
    market: crate::service::venue::TradableMarket,
    fault: &'static str,
    reads: std::sync::atomic::AtomicUsize,
    dropped_reads: std::sync::atomic::AtomicUsize,
    release: tokio::sync::Notify,
}

struct PendingOfficialRead<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for PendingOfficialRead<'_> {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

#[async_trait::async_trait]
impl crate::service::venue::MarketData for ShutdownData {
    async fn markets(&self, _: &crate::service::venue::MarketQuery)
        -> anyhow::Result<Vec<crate::service::venue::TradableMarket>> { panic!("unexpected scan") }
    async fn market(&self, id: u64) -> anyhow::Result<crate::service::venue::TradableMarket> {
        assert_eq!(id, self.market.id, "only the owned official round may be read");
        let read = self.reads.fetch_add(1, Ordering::SeqCst);
        if read == 0 && self.fault == "settle_race" { self.release.notified().await; }
        if self.fault == "read_failure" { anyhow::bail!("offline official read failure"); }
        if self.fault == "cleanup_pending" || (read > 0 && self.fault == "tail_pending") {
            let _pending = PendingOfficialRead(&self.dropped_reads);
            return std::future::pending().await;
        }
        if read > 0 && self.fault == "tail_stop" {
            self.broker.stop_entries(super::super::ControlledStop::ParentLost);
            let _pending = PendingOfficialRead(&self.dropped_reads);
            return std::future::pending().await;
        }
        Ok(self.market.clone())
    }
    async fn with_schedule(&self, _: &crate::service::venue::TradableMarket)
        -> anyhow::Result<crate::service::venue::TradableMarket> { panic!("unexpected schedule") }
    async fn books(&self, _: &[u64]) -> anyhow::Result<std::collections::HashMap<u64, crate::models::BookSnapshot>> {
        panic!("shutdown must not invent a book")
    }
}

async fn assert_bounded_live_shutdown(fault: &'static str) {
    use std::time::Duration;
    use super::super::ControlledStop;
    use super::super::live_accounting::{LIVE_CLEANUP_BUDGET, LIVE_TAIL_BUDGET};
    assert_eq!(LIVE_CLEANUP_BUDGET, Duration::from_secs(30));
    assert_eq!(LIVE_TAIL_BUDGET, Duration::from_secs(600));
    let (broker, _, market, book, candidate, transport) = sending_fixture("controlled_fill").await;
    broker.observe(&market, &book);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    let before = {
        let state = broker.state.lock();
        let hash = state.live.attempts[&id].lock().identity.signed_hash.clone();
        (state.live.net_cash[&id], state.live.net_inventory[&id], hash)
    };
    broker.stop_entries(if fault == "manual_stop" { ControlledStop::ControlChannel }
        else { ControlledStop::Deadline });
    let data = ShutdownData { broker: broker.clone(), market: market.clone(), fault,
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    // Zero budgets deterministically expire a genuinely Pending original read;
    // no sleep or scheduler race selects the fault boundary. Production constants
    // are checked above and the production wrapper supplies those exact values.
    let cleanup = if fault == "cleanup_pending" { Duration::ZERO } else { Duration::from_secs(5) };
    let tail = if fault == "tail_pending" { Duration::ZERO } else { Duration::from_secs(600) };
    tokio::time::timeout(Duration::from_secs(10),
        broker.finish_controlled_live_with(&data, cleanup, tail)).await.unwrap();
    assert_eq!(data.reads.load(Ordering::SeqCst),
        if matches!(fault, "tail_pending" | "tail_stop") { 2 } else { 1 });
    assert_eq!(data.dropped_reads.load(Ordering::SeqCst),
        usize::from(matches!(fault, "cleanup_pending" | "tail_pending" | "tail_stop")));
    let emitter = {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::Handoff);
        assert!(run.attempt_outcomes.is_empty(), "no fabricated official resolution");
        assert!(!run.may_add_risk());
        assert_eq!(state.live.net_cash[&id], before.0);
        assert_eq!(state.live.net_inventory[&id], before.1);
        assert_eq!(state.live.attempts[&id].lock().identity.signed_hash, before.2);
        assert_eq!(state.live.reservations[&id], U256::ZERO);
        run.emitter.clone()
    };
    assert!(emitter.is_finished());
    let final_seq = emitter.last_seq();
    broker.finish_controlled_live_with(&data, Duration::ZERO, Duration::ZERO).await;
    broker.settle_resolved(&data).await.unwrap();
    broker.sync_live().await.unwrap();
    broker.observe(&market, &book);
    assert_eq!(emitter.last_seq(), final_seq, "all producers sealed after final");
    assert!(!emitter.output_failed());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "readonly cleanup/tail never re-POST");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1, "readonly cleanup/tail never re-sign");
}

#[tokio::test]
async fn live_cleanup_budget_cancels_pending_official_read_without_releasing_risk() {
    assert_bounded_live_shutdown("cleanup_pending").await;
}

#[tokio::test]
async fn live_deadline_tail_budget_cancels_pending_read_without_faking_resolution() {
    assert_bounded_live_shutdown("tail_pending").await;
}

#[tokio::test]
async fn live_deadline_tail_emergency_cancels_the_same_pending_read() {
    assert_bounded_live_shutdown("tail_stop").await;
}

#[tokio::test]
async fn live_non_deadline_stop_does_not_enter_tail() {
    assert_bounded_live_shutdown("manual_stop").await;
}

#[tokio::test]
async fn live_shutdown_official_read_failure_keeps_cash_and_inventory() {
    assert_bounded_live_shutdown("read_failure").await;
}

#[test]
fn captured_bounded_shutdown_distinguishes_cash_execution_and_child_exit() {
    for (name, execution_complete, stop_reason) in [
        ("live_cleanup_budget_cancels_pending_official_read_without_releasing_risk", false, "DEADLINE"),
        ("live_deadline_tail_budget_cancels_pending_read_without_faking_resolution", true, "DEADLINE"),
        ("live_deadline_tail_emergency_cancels_the_same_pending_read", true, "DEADLINE"),
        ("live_non_deadline_stop_does_not_enter_tail", true, "CONTROL_CHANNEL"),
        ("live_shutdown_official_read_failure_keeps_cash_and_inventory", false, "DEADLINE"),
    ] {
        let events = capture_production_test(name);
        let finals: Vec<_> = events.iter().filter(|event| event["event_kind"] == "final").collect();
        assert_eq!(finals.len(), 1, "{name}");
        let terminal = finals[0];
        assert_eq!(events.last(), Some(terminal), "{name}: final is the last stdout event");
        assert_eq!(terminal["payload"]["execution_complete"], execution_complete, "{name}");
        assert_eq!(terminal["payload"]["cash_complete"], false, "{name}");
        assert_eq!(terminal["payload"]["audit_complete"], true, "{name}");
        assert_eq!(terminal["payload"]["takeover_required"], true, "{name}");
        assert_eq!(terminal["payload"]["stop_reason"], stop_reason, "{name}");
        assert_eq!(terminal["payload"]["positions"].as_array().unwrap().len(), 1);
        assert!(!events.iter().any(|event| event["event_kind"] == "payout"), "{name}");
    }
}

#[tokio::test]
async fn concurrent_official_settle_late_raw_fill_and_two_finishers_share_the_original_lane() {
    use std::time::Duration;
    let (broker, _, market, book, candidate, transport) = sending_fixture("controlled_partial").await;
    broker.observe(&market, &book);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    let q = crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    let cash = crate::service::predict_fun::controlled::cash_at_limit_atoms(q, candidate.limit_price()).unwrap();
    assert_eq!(broker.state.lock().live.net_inventory[&id], q/U256::from(2)-q/U256::from(200));
    transport.stage.store(1, Ordering::SeqCst);
    let mut resolved = market.clone();
    resolved.open = false;
    resolved.end_price = Some(101.0);
    resolved.payout_yes = Some(1.0);
    resolved.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some("WON".into()), no_status: Some("LOST".into()),
        decimal_precision: None, price_feed_provider: None,
    });
    let data = ShutdownData { broker: broker.clone(), market: resolved, fault: "settle_race",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    let settle = broker.settle_resolved(&data);
    tokio::pin!(settle);
    assert!(futures_util::poll!(&mut settle).is_pending());
    assert_eq!(data.reads.load(Ordering::SeqCst), 1, "real official await owns the lane");
    let late = broker.sync_live();
    tokio::pin!(late);
    assert!(futures_util::poll!(&mut late).is_pending());
    let first = broker.finish_controlled_live_with(&data, Duration::from_secs(5), Duration::ZERO);
    let second = broker.finish_controlled_live_with(&data, Duration::from_secs(5), Duration::ZERO);
    tokio::pin!(first, second);
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(data.reads.load(Ordering::SeqCst), 1, "no settlement bypasses the owned lane");
    data.release.notify_one();
    let (settled, synced, (), ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(settle, late, first, second)
    }).await.unwrap();
    settled.unwrap();
    synced.unwrap();
    let (emitter, hash) = {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(state.live.net_inventory[&id], q-q/U256::from(100));
        assert_eq!(state.live.net_cash[&id], cash);
        assert_eq!(state.live.reservations[&id], U256::ZERO);
        assert_eq!(run.attempt_outcomes[&id].position_net_atoms, q-q/U256::from(100));
        assert_eq!(run.attempt_outcomes[&id].claim_value_atoms, Some(q-q/U256::from(100)));
        assert!(!run.attempt_outcomes[&id].cash_confirmed);
        assert_eq!(run.control.lock().state, LiveRunState::Handoff);
        let hash = state.live.attempts[&id].lock().identity.signed_hash.clone().unwrap();
        (run.emitter.clone(), hash)
    };
    assert!(emitter.is_finished());
    let seq = emitter.last_seq();
    broker.sync_live().await.unwrap();
    broker.settle_resolved(&data).await.unwrap();
    broker.finish_controlled(&data).await;
    assert_eq!(emitter.last_seq(), seq);
    assert!(!emitter.output_failed());
    assert_eq!(broker.state.lock().live.attempts[&id].lock().identity.signed_hash.as_ref(), Some(&hash));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[test]
fn captured_concurrent_shutdown_has_one_final_after_late_fill_and_cash_pending() {
    let events = capture_production_test(
        "concurrent_official_settle_late_raw_fill_and_two_finishers_share_the_original_lane");
    assert_eq!(events.iter().filter(|event| event["event_kind"] == "final").count(), 1);
    let payouts: Vec<_> = events.iter().filter(|event| event["event_kind"] == "payout").collect();
    assert_eq!(payouts.len(), 1, "partial result did not prematurely value a claim");
    assert_eq!(payouts[0]["payload"]["status"], "CASH_PENDING");
    assert_eq!(payouts[0]["payload"]["position_net_atoms"], (2475*SCALE/1000).to_string());
    let final_event = events.last().unwrap();
    assert_eq!(final_event["event_kind"], "final");
    assert_eq!(final_event["payload"]["execution_complete"], true);
    assert_eq!(final_event["payload"]["cash_complete"], false);
    assert_eq!(final_event["payload"]["audit_complete"], true);
}

#[tokio::test]
async fn live_shutdown_account_reconciliation_failure_is_incomplete_without_clearing_applied_fills() {
    let (broker, _, market, book, candidate, transport, trace) = sending_fixture_with_trace("controlled_fill").await;
    broker.observe(&market, &book);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    let before = {
        let state = broker.state.lock();
        (state.live.net_cash[&id], state.live.net_inventory[&id])
    };
    trace.extra_position_atoms.store(1, Ordering::SeqCst);
    broker.stop_entries(super::super::ControlledStop::ControlChannel);
    let data = ShutdownData { broker: broker.clone(), market: market.clone(), fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    broker.finish_controlled(&data).await;
    assert_eq!(data.reads.load(Ordering::SeqCst), 0, "failed sync cannot certify cleanup or settle");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().state, LiveRunState::Handoff);
    assert_eq!(state.live.net_cash[&id], before.0);
    assert_eq!(state.live.net_inventory[&id], before.1);
    assert!(run.account_read.is_none());
    assert!(!run.may_add_risk());
    assert!(run.attempt_outcomes.is_empty());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn live_shutdown_after_pending_post_cancellation_keeps_unknown_risk_and_one_hash() {
    let (broker, _, market, book, candidate, transport) = sending_fixture("abort").await;
    let ack = ack_next(broker.clone(), false);
    let mut operation = Box::pin(broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }));
    assert!(futures_util::poll!(&mut operation).is_pending());
    ack.join().unwrap();
    let before = broker.state.lock().live.attempts[&1].lock().clone();
    channel::ControlWatcher::parent_lost(broker.as_ref());
    assert!(futures_util::poll!(&mut operation).is_pending());
    // Dropping the owning future is the actual cancellation boundary: no
    // second adapter or copied State can release its unknown reservation.
    drop(operation);
    assert_eq!(before.phase, AttemptPhase::MayHaveSubmitted);
    assert_eq!(before.submission, Submission::Unknown);
    let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    tokio::time::timeout(std::time::Duration::from_secs(5), broker.finish_controlled(&data)).await.unwrap();
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let record = state.live.attempts[&1].lock();
    assert_eq!(record.identity.signed_hash, before.identity.signed_hash);
    assert_eq!(record.request_gross_atoms, before.request_gross_atoms);
    assert_eq!(record.r0, before.r0);
    assert_eq!(state.live.reservations[&1], before.r0);
    assert_ne!(record.phase, AttemptPhase::Closed);
    assert_eq!(record.submission, Submission::Unknown);
    assert!(run.emitter.is_finished());
    assert!(!run.may_add_risk());
    assert_eq!(run.control.lock().state, LiveRunState::Handoff);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[test]
fn captured_sync_failure_final_does_not_certify_a_closed_but_unreconciled_ledger() {
    let events = capture_production_test(
        "live_shutdown_account_reconciliation_failure_is_incomplete_without_clearing_applied_fills");
    let final_event = events.last().unwrap();
    assert_eq!(final_event["event_kind"], "final");
    assert_eq!(final_event["payload"]["execution_complete"], false);
    assert_eq!(final_event["payload"]["cash_complete"], false);
    assert_eq!(final_event["payload"]["audit_complete"], true);
    assert_eq!(final_event["payload"]["run_status"], "INCOMPLETE");
    assert!(final_event["payload"]["unresolved"].as_array().unwrap().is_empty(),
        "closed executions alone do not certify account reconciliation");
    assert_eq!(final_event["payload"]["positions"][0]["net_shares_atoms"], (2475*SCALE/1000).to_string());
}

#[test]
fn captured_cancelled_pending_post_final_retains_unknown_attempt_reservation_and_hash() {
    let events = capture_production_test(
        "live_shutdown_after_pending_post_cancellation_keeps_unknown_risk_and_one_hash");
    let prepared = events.iter().find(|event| event["event_kind"] == "order"
        && event["payload"]["attempt_phase"] == "PREPARED").unwrap();
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["execution_complete"], false);
    assert_eq!(terminal["payload"]["cash_complete"], false);
    assert_eq!(terminal["payload"]["audit_complete"], true);
    assert_eq!(terminal["payload"]["stop_reason"], "PARENT_LOST");
    let stop = events.iter().find(|event| event["event_kind"] == "stop").unwrap();
    assert_eq!(stop["payload"]["stop_detail"], serde_json::Value::Null,
        "a legal stop gains no detail from the failure-code fix");
    assert_eq!(terminal["payload"]["stop_detail"], serde_json::Value::Null,
        "the terminal record keeps the legal stop's empty detail");
    let unresolved = terminal["payload"]["unresolved"].as_array().unwrap();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0]["signed_hash"], prepared["payload"]["signed_hash"]);
    assert_eq!(unresolved[0]["reserved_atoms"], SCALE.to_string());
    assert_eq!(unresolved[0]["closure"], "UNPROVEN");
}

#[tokio::test]
async fn live_startup_missing_authoritative_page_stops_and_finishes_without_an_attempt() {
    use crate::service::venue::OrderVenue;
    let (mut broker, clock) = risk_fixture();
    let (market, _, _) = evaluate_live_fixture(&clock, true);
    let (trader, transport) = crate::service::predict_fun::controlled::synthetic_controlled_trader(
        200, "position_missing");
    let identity = trader.identity();
    broker.identity = Some(identity.clone());
    broker.gate = Some(trader.gate.clone());
    let trader = Arc::new(trader);
    broker.limit = Some(trader.clone());
    broker.taker = Some(trader);
    {
        let mut state = broker.state.lock();
        state.live.baseline = None;
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address;
        run.account_read = None;
    }
    let broker = Arc::new(broker);
    assert!(live::seed_predict(&broker).await.is_err(), "missing REST pagination is not an empty account");
    let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    broker.finish_controlled(&data).await;
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert!(state.live.baseline.is_none());
    assert!(state.live.attempts.is_empty());
    assert!(state.live.reservations.is_empty());
    assert!(state.live.net_cash.is_empty());
    assert!(state.live.net_inventory.is_empty());
    assert!(!run.may_add_risk());
    assert!(run.emitter.is_finished());
    assert_eq!(run.control.lock().state, LiveRunState::Handoff);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
    assert_eq!(data.reads.load(Ordering::SeqCst), 0);
}

#[test]
fn captured_startup_failure_never_certifies_empty_account_or_invents_execution() {
    let events = capture_production_test(
        "live_startup_missing_authoritative_page_stops_and_finishes_without_an_attempt");
    assert!(!events.iter().any(|event| matches!(event["event_kind"].as_str(), Some("order" | "execution"))));
    assert_eq!(events.iter().filter(|event| event["event_kind"] == "final").count(), 1);
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["execution_complete"], false);
    assert_eq!(terminal["payload"]["cash_complete"], false);
    assert_eq!(terminal["payload"]["audit_complete"], true);
    assert!(terminal["payload"]["unresolved"].as_array().unwrap().is_empty());
    assert!(terminal["payload"]["positions"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn original_guard_and_request_recheck_after_proven_account_gate_contention() {
    use crate::service::predict_fun::account::AccountGate;
    use std::sync::mpsc;
    use std::time::Duration;
    let (broker, clock, market, book, candidate, transport) = sending_fixture("terminal").await;
    let (contended_tx, contended_rx) = mpsc::channel();
    let state = broker.state.clone();
    AccountGate::on_contended_send_for_test(Some(Box::new(move || {
        assert!(state.try_lock().is_none(), "the original send guard already owns State");
        contended_tx.send(()).unwrap();
    })));
    let channel = broker.state.lock().live.controlled.as_ref().unwrap().channel.clone();
    let gate = broker.gate.as_ref().unwrap().clone();
    let ack = std::thread::spawn(move || {
        let pending = channel.wait_pending();
        gate.hold_send_gate_for_test(|| {
            channel.accept(&channel::ControlFrame::Ack {
                seq: pending.seq, signed_hash: pending.signed_hash }).unwrap();
            // The signal requires a failed try-lock in the actual request's
            // AccountGate call. It is not a sleep or a substitute callback.
            contended_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            clock.advance(10_001);
        });
    });
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await;
    AccountGate::on_contended_send_for_test(None);
    ack.join().unwrap();
    assert!(outcome.is_err());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let state = broker.state.lock();
    let record = state.live.attempts[&1].lock();
    assert!(record.identity.signed_hash.is_some());
    assert_eq!(record.submission, Submission::NotSubmitted);
    assert_eq!(record.phase, AttemptPhase::Closed);
    assert!(!state.live.reservations.contains_key(&1));
    assert_eq!(state.live.tombstones[&live::taker_key(market.id, candidate.token(), Side::Buy)], 1);
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
}

#[test]
fn captured_contended_original_request_final_diagnostic_reports_the_actual_stale_clock() {
    let events = capture_production_test(
        "original_guard_and_request_recheck_after_proven_account_gate_contention");
    let diagnostic = events.iter().find(|event| event["event_kind"] == "decision"
        && event["payload"]["decision"]["phase"] == "FINAL_SEND").unwrap();
    assert_eq!(diagnostic["payload"]["decision"]["reason"], "SPOT_STALE");
    let decision = &diagnostic["payload"]["decision"];
    assert_eq!(decision["checked_elapsed_ms"].as_u64().unwrap()
        - decision["decision_elapsed_ms"].as_u64().unwrap(), 10_001);
    let prepared = events.iter().find(|event| event["event_kind"] == "order"
        && event["payload"]["attempt_phase"] == "PREPARED").unwrap();
    let last_order = events.iter().rev().find(|event| event["event_kind"] == "order").unwrap();
    assert_eq!(last_order["payload"]["submission"], "NOT_SUBMITTED");
    assert_eq!(last_order["payload"]["signed_hash"], prepared["payload"]["signed_hash"]);
    assert_eq!(last_order["payload"]["candidate_id"], prepared["payload"]["candidate_id"]);
    assert_eq!(last_order["payload"]["attempt_id"], prepared["payload"]["attempt_id"]);
}

#[test]
fn missing_mark_stops_without_faking_loss_and_valuation_recovery_cannot_unlock() {
    let (broker, clock) = risk_fixture();
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    clock.advance(10_001);
    let mut state = broker.state.lock();
    refresh_live_risk(&mut state);
    assert!(!state.live.controlled.as_ref().unwrap().loss_latched);
    // A missing mark is a valuation gap, not a fact gap: the window degrades,
    // it adds no risk, and it keeps observing instead of stopping at once.
    assert!(state.live.controlled.as_ref().unwrap().degraded());
    assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none());
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    // Only a fresh mark restores the window, so a bare price is not a recovery
    // and it never unlocks anything: the window stays degraded, never stopped.
    state.marks.insert("1".into(), 0.9);
    assert!(state.live.controlled.as_ref().unwrap().degraded());
    assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none());
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
}

#[test]
fn a_valuation_gap_stops_with_its_existing_code_only_after_the_read_budget() {
    let (broker, clock) = risk_fixture();
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    clock.advance(10_001);
    let mut state = broker.state.lock();
    refresh_live_risk(&mut state);
    // Before the budget the window observes without risk: no stop, no end.
    {
        let run = state.live.controlled.as_ref().unwrap();
        assert!(run.degraded());
        assert!(run.control.lock().stop.is_none());
        assert!(!run.may_add_risk());
        assert!(!run.must_end());
    }
    // The gap outlasts the budget: the window is blind and stops with the code
    // it already carried, exactly as an unread fact would.
    clock.advance(READ_FAILURE_BUDGET.as_millis() as u64 + 1);
    refresh_live_risk(&mut state);
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DATA_UNAVAILABLE");
    assert!(run.must_end());
}

#[tokio::test]
async fn original_first_poll_holds_state_and_handle_against_a_concurrent_parent_stop() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (broker, _, market, book, candidate, transport, trace) = sending_fixture_with_trace("abort").await;
    let (polled_tx, polled_rx) = mpsc::channel();
    let (contended_tx, contended_rx) = mpsc::channel();
    let state = broker.state.clone();
    let competitor = broker.clone();
    let stopper = std::thread::spawn(move || {
        polled_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(competitor.state.try_lock().is_none(), "first poll owns the real State");
        contended_tx.send(()).unwrap();
        channel::ControlWatcher::parent_lost(competitor.as_ref());
    });
    let prepared_handle = Arc::new(Mutex::new(None::<crate::service::venue::AttemptHandle>));
    let observed_handle = prepared_handle.clone();
    *trace.first_post.lock().unwrap() = Some(Box::new(move || {
        assert!(state.try_lock().is_none());
        let handle = observed_handle.lock().as_ref().unwrap().clone();
        assert!(handle.try_lock().is_none(), "same attempt handle is held through first poll");
        polled_tx.send(()).unwrap();
        contended_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let captured_handle = prepared_handle.clone();
    let ack = ack_next_with(broker.clone(), move |broker| {
        *captured_handle.lock() = Some(broker.state.lock().live.attempts[&1].clone());
    });
    let mut operation = Box::pin(broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }));
    assert!(futures_util::poll!(&mut operation).is_pending());
    ack.join().unwrap();
    stopper.join().unwrap();
    assert!(futures_util::poll!(&mut operation).is_pending(), "only the same POST future advances");
    drop(operation);
    let before = broker.state.lock().live.attempts[&1].lock().clone();
    assert_eq!(before.phase, AttemptPhase::MayHaveSubmitted);
    assert_eq!(before.submission, Submission::Unknown);
    assert!(before.identity.signed_hash.is_some());
    let data = ShutdownData { broker: broker.clone(), market, fault: "unused",
        reads: Default::default(), dropped_reads: Default::default(), release: Default::default() };
    broker.finish_controlled(&data).await;
    let state = broker.state.lock();
    assert_eq!(state.live.attempts[&1].lock().identity.signed_hash, before.identity.signed_hash);
    assert_eq!(state.live.reservations[&1], before.r0);
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[test]
fn captured_first_poll_winner_keeps_unknown_risk_after_the_competing_stop() {
    let events = capture_production_test(
        "original_first_poll_holds_state_and_handle_against_a_concurrent_parent_stop");
    let final_event = events.last().unwrap();
    assert_eq!(final_event["event_kind"], "final");
    assert_eq!(final_event["payload"]["stop_reason"], "PARENT_LOST");
    assert_eq!(final_event["payload"]["execution_complete"], false);
    let unresolved = final_event["payload"]["unresolved"].as_array().unwrap();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0]["reserved_atoms"], SCALE.to_string());
    let prepared = events.iter().find(|event| event["event_kind"] == "order"
        && event["payload"]["attempt_phase"] == "PREPARED").unwrap();
    assert_eq!(unresolved[0]["signed_hash"], prepared["payload"]["signed_hash"]);
}

/// One reading of an attempt whose facts are not all known yet.
fn incomplete_reading(generation: u64, closure: Closure) -> ExecutionSnapshot {
    let mut reading = snapshot(generation, 5 * SCALE, 2 * SCALE, SCALE / 10);
    reading.closure = closure;
    reading.accounting.share_fee = Field::Unknown("MISSING_SHARE_FEE".into());
    reading
}

/// Reserve the attempt one reading belongs to, with the ACK facts it names.
fn reserve_reading(
    state: &mut State,
    reading: &mut ExecutionSnapshot,
    submission: Submission,
    venue_order_id: Option<&str>,
) {
    reading.identity.venue_order_id = venue_order_id.map(str::to_string);
    live::reserve_attempt(&mut state.live, AttemptRecord {
        identity: reading.identity.clone(),
        request_gross_atoms: U256::from(5 * SCALE),
        r0: U256::from(2 * SCALE),
        reserved_sell: U256::ZERO,
        phase: AttemptPhase::Submitted,
        submission,
        last_reason: None,
    });
}

/// The stop the run already holds, if any, without keeping the control lock.
fn stopped(broker: &Broker) -> Option<(String, Option<String>, bool)> {
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let stop = { run.control.lock().stop.clone() };
    stop.map(|stop| (stop.reason, stop.detail, stop.takeover_required))
}

#[test]
fn an_in_flight_order_stays_pending_and_never_stops_the_window() {
    let (broker, _) = risk_fixture();
    let mut open = incomplete_reading(1, Closure::Open);
    {
        let mut state = broker.state.lock();
        let now = state.live.controlled.as_ref().unwrap().phase1.clock.now().unwrap();
        state.marks.insert("77".into(), 0.5);
        state.live.controlled.as_mut().unwrap().mark_receipts.insert("77".into(), now);
        reserve_reading(&mut state, &mut open, Submission::Accepted, Some("synthetic-order"));
    }
    let mut state = broker.state.lock();
    let applied = live::apply_snapshot_detailed(&mut state, &open).unwrap();
    assert!(!applied.applied);
    assert!(!applied.accounting_complete);
    assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none(),
        "an accepted order that may still fill must not stop the window");
    assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Submitted);
    assert_eq!(state.live.reservations[&1], U256::from(2 * SCALE));
    assert!(state.live.net_cash.is_empty());
    drop(state);

    // The order closes and proves its facts: the attempt closes, the run never
    // stopped in between.
    let mut closed = snapshot(2, 5 * SCALE, 2 * SCALE, SCALE / 10);
    closed.identity.venue_order_id = Some("synthetic-order".into());
    closed.closure = Closure::Final {
        final_gross_q: U256::from(5 * SCALE),
        evidence: "synthetic-final".into(),
    };
    closed.observed_at = open.observed_at + chrono::Duration::seconds(1);
    let mut state = broker.state.lock();
    let applied = live::apply_snapshot_detailed(&mut state, &closed).unwrap();
    assert!(applied.applied);
    assert!(applied.accounting_complete);
    assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Closed);
    let stop = { state.live.controlled.as_ref().unwrap().control.lock().stop.clone() };
    assert!(stop.is_none(), "the closed order never stopped the run: {stop:?}");
    assert_eq!(state.live.net_cash[&1], U256::from(2 * SCALE));
    drop(state);
    assert_eq!(stopped(&broker), None);
}

#[test]
fn an_unknown_quantity_read_of_an_in_flight_order_is_not_a_run_failure() {
    let (broker, _) = risk_fixture();
    let mut known = snapshot(1, 5 * SCALE, 2 * SCALE, SCALE / 10);
    {
        let mut state = broker.state.lock();
        let now = state.live.controlled.as_ref().unwrap().phase1.clock.now().unwrap();
        state.marks.insert("77".into(), 0.5);
        state.live.controlled.as_mut().unwrap().mark_receipts.insert("77".into(), now);
        reserve_reading(&mut state, &mut known, Submission::Accepted, Some("synthetic-order"));
        live::apply_snapshot_detailed(&mut state, &known).unwrap();
    }
    let mut unread = snapshot(2, 5 * SCALE, 2 * SCALE, SCALE / 10);
    unread.quantity = Quantity::Unknown;
    unread.closure = Closure::Open;
    unread.identity.venue_order_id = Some("synthetic-order".into());
    let mut state = broker.state.lock();
    let applied = live::apply_snapshot_detailed(&mut state, &unread).unwrap();
    assert!(!applied.applied, "an unread quantity applies nothing");
    assert!(!applied.accounting_complete);
    assert!(state.live.controlled.as_ref().unwrap().control.lock().stop.is_none());
    assert_eq!(state.live.attempts[&1].lock().phase, AttemptPhase::Submitted);
    drop(state);
    assert_eq!(stopped(&broker), None);
}

#[test]
fn unproven_incomplete_readings_still_stop_with_the_code_already_held() {
    let final_closure = || Closure::Final {
        final_gross_q: U256::from(5 * SCALE),
        evidence: "synthetic-final".into(),
    };
    for (submission, venue_order_id, closure, expected) in [
        (Submission::Unknown, None, Closure::Open, "SUBMISSION_UNKNOWN"),
        (Submission::NotSubmitted, None, Closure::Open, "SUBMISSION_UNKNOWN"),
        (Submission::Accepted, Some("synthetic-order"), final_closure(), "FACT_INCOMPLETE"),
    ] {
        let (broker, _) = risk_fixture();
        let mut reading = incomplete_reading(1, closure);
        {
            let mut state = broker.state.lock();
            reserve_reading(&mut state, &mut reading, submission, venue_order_id);
        }
        let mut state = broker.state.lock();
        let applied = live::apply_snapshot_detailed(&mut state, &reading).unwrap();
        assert!(!applied.accounting_complete);
        drop(state);
        let stop = stopped(&broker).expect("an unproven reading still stops the run");
        assert_eq!(stop.0, "FACT_INCOMPLETE", "{submission:?}");
        assert_eq!(stop.1.as_deref(), Some(expected), "{submission:?}");
        assert!(stop.2, "an unproven reading still needs takeover");
    }
}

#[test]
fn an_unknown_quantity_read_without_a_proven_ack_still_stops_with_its_code() {
    let (broker, _) = risk_fixture();
    let mut unread = snapshot(1, 5 * SCALE, 2 * SCALE, SCALE / 10);
    unread.quantity = Quantity::Unknown;
    unread.closure = Closure::Open;
    {
        let mut state = broker.state.lock();
        reserve_reading(&mut state, &mut unread, Submission::Unknown, None);
    }
    let mut state = broker.state.lock();
    let applied = live::apply_snapshot_detailed(&mut state, &unread).unwrap();
    assert!(!applied.accounting_complete);
    drop(state);
    let stop = stopped(&broker).expect("no proven acceptance is still a stop");
    assert_eq!(stop.0, "FACT_INCOMPLETE");
    assert_eq!(stop.1.as_deref(), Some("SUBMISSION_UNKNOWN"));
}

#[test]
fn a_rejected_reading_names_its_existing_code_on_the_stop() {
    let (broker, _) = risk_fixture();
    apply_risk_fill(&broker, 1, 2 * SCALE, true);
    let mut reading = snapshot(2, 5 * SCALE, 2 * SCALE, SCALE / 10);
    reading.identity = broker.state.lock().live.attempts[&1].lock().identity.clone();
    reading.identity.token_id = "999".into();
    let lane = broker.live_operation_lane().unwrap();
    let _operation = lane.try_lock().unwrap();
    assert!(live::consume_snapshot(&broker, &reading).is_err());
    let stop = stopped(&broker).expect("a contradiction still stops the run");
    assert_eq!(stop.0, "FACT_CONFLICT");
    assert_eq!(stop.1.as_deref(), Some("ORDER_IDENTITY_MISMATCH"));
}

/// Place the first candidate while the venue's fill read still lags.
///
/// Returns the attempt id and the gross quantity the order asked for.
async fn first_order_with_visible_ack_but_invisible_fill(
    broker: &Arc<Broker>,
    market: &crate::service::venue::TradableMarket,
    book: &crate::models::BookSnapshot,
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
    let id = outcome.attempt_id.unwrap();
    let requested =
        crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares()).unwrap();
    (id, requested)
}

#[tokio::test]
async fn an_accepted_order_whose_facts_lag_is_re_read_before_a_blocked_candidate_stops_the_run() {
    let (broker, _clock, market, book, candidate, transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    // The venue accepted the order but its fill read still lags.
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_millis(150));
    let (id, requested) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    assert!(stopped(&broker).is_none(), "the first acceptance is not a failure");
    {
        let state = broker.state.lock();
        assert!(state.live.net_inventory.get(&id).is_none_or(U256::is_zero));
        assert!(state.live.reservations[&id] > U256::ZERO);
    }
    // The next candidate for the same market is blocked while the first
    // order's facts have not arrived.
    // A fresh mark keeps the filled inventory valued the way the live loop
    // keeps it; the window observes the book every tick.
    broker.observe(&market, &book);
    taker::set_pending_candidate(
        &mut broker.state.lock(),
        candidate.clone(),
        U256::from(SCALE),
    );
    let blocked = broker
        .take_live_candidate(taker::LiveCandidateRequest {
            market: &market,
            yes_book: &book,
            candidate: &candidate,
        })
        .await
        .unwrap();
    assert!(blocked.attempt_id.is_none(), "the block creates no new attempt");
    assert!(
        stopped(&broker).is_none(),
        "arriving facts must not be turned into a run failure: {:?}",
        stopped(&broker)
    );
    let state = broker.state.lock();
    assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
    assert_eq!(state.live.net_inventory[&id], requested - requested / U256::from(100));
    assert_eq!(state.live.reservations[&id], U256::ZERO);
    // Every re-read went through the existing execution read.
    assert!(state.live.generation[&id] >= 2, "the unread attempt was re-read");
    drop(state);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "no new order was sent");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_order_still_unread_at_the_bound_keeps_the_existing_stop() {
    let (broker, _clock, market, book, candidate, _transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(30));
    let (id, _) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    assert!(stopped(&broker).is_none());
    broker
        .resolve_blocked_candidate(
            std::time::Duration::from_millis(80),
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap();
    let stop = stopped(&broker).expect("an exhausted bound still stops the run");
    assert_eq!(stop.0, "FACT_INCOMPLETE");
    assert_eq!(stop.1, None, "the reason is unchanged, no new detail");
    let state = broker.state.lock();
    assert_ne!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
    assert!(state.live.reservations[&id] > U256::ZERO, "the risk is not released");
}

#[tokio::test]
async fn a_conflict_observed_while_waiting_keeps_the_existing_conflict_stop() {
    let (broker, _clock, market, book, candidate, _transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(30));
    let (id, _) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    // The next reading proves a higher risk than the order ever reserved: the
    // existing conflict path, not the wait, owns the stop.
    let reserved = broker.state.lock().live.reservations[&id];
    broker
        .state
        .lock()
        .live
        .observed_risk_floor
        .insert(id, reserved + U256::from(1));
    broker
        .resolve_blocked_candidate(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap();
    let stop = stopped(&broker).expect("a contradiction still stops the run");
    assert_eq!(stop.0, "FACT_CONFLICT");
    assert!(
        stop.1.is_none(),
        "the conflict path's own reasoning is kept, the wait adds nothing"
    );
}

#[tokio::test]
async fn a_re_read_that_reaches_the_run_deadline_stops_with_the_deadline_reason() {
    let (broker, _clock, market, book, candidate, _transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(30));
    let (_id, _) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    assert!(stopped(&broker).is_none());
    crate::bot::runtime::paper::set_controlled_clock_expired(true);
    broker
        .resolve_blocked_candidate(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap();
    crate::bot::runtime::paper::set_controlled_clock_expired(false);
    let stop = stopped(&broker).expect("the deadline still stops the run");
    assert_eq!(stop.0, "DEADLINE");
}

/// A clock the test makes transiently unreadable.
///
/// The first `healthy` readings succeed, then every reading fails: the run's
/// own clock source goes away after the facts were applied, not before.
struct ScriptedClock {
    healthy: usize,
    calls: std::sync::atomic::AtomicUsize,
}

impl crate::service::observation::Clock for ScriptedClock {
    fn now(&self) -> anyhow::Result<crate::service::observation::CheckTime> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call > self.healthy {
            anyhow::bail!("CLOCK_UNAVAILABLE")
        }
        Ok(crate::service::observation::CheckTime { utc: Utc::now(), elapsed_ms: 0 })
    }
}

/// Make the run's clock succeed `healthy` more readings, then fail.
fn fail_run_clock_after(broker: &Broker, healthy: usize) {
    let mut state = broker.state.lock();
    let run = state.live.controlled.as_mut().unwrap();
    run.phase1.clock = Arc::new(ScriptedClock { healthy, calls: Default::default() });
}

#[tokio::test]
async fn a_read_whose_clock_fails_without_a_recorded_stop_still_stops_the_run() {
    let (broker, _clock, market, book, candidate, transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    assert!(stopped(&broker).is_none());
    // The facts are applied, then the run's own clock read fails before any
    // stop is written: that reading is a failure, not a reason to keep going.
    fail_run_clock_after(&broker, 2);
    let started = std::time::Instant::now();
    let result = broker
        .resolve_blocked_candidate(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(20),
        )
        .await;
    let elapsed = started.elapsed();
    assert!(result.is_ok(), "the latched failure is not a preparation error: {result:?}");
    let stop = stopped(&broker).expect("a failed read without any stop still stops the run");
    assert_eq!(stop.0, "FACT_INCOMPLETE");
    assert_eq!(stop.1, None, "the latch adds no new detail");
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "the run must stop at the failed read instead of re-reading: {elapsed:?}"
    );
    let state = broker.state.lock();
    assert_ne!(state.live.attempts[&id].lock().phase, AttemptPhase::Closed);
    assert!(state.live.reservations[&id] > U256::ZERO, "the risk is not released");
    drop(state);
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1, "no new order was sent");
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_read_that_already_recorded_its_reason_keeps_that_reason() {
    let (broker, _clock, market, book, candidate, _transport, trace) =
        sending_fixture_with_trace("controlled_fill").await;
    *trace.hidden_until.lock().unwrap() =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
    let (id, _) =
        first_order_with_visible_ack_but_invisible_fill(&broker, &market, &book, &candidate).await;
    // The failing reading stops the run itself: its projection cannot name the
    // candidate this attempt was bound to.
    broker
        .state
        .lock()
        .live
        .controlled
        .as_mut()
        .unwrap()
        .attempt_candidates
        .remove(&id);
    broker
        .resolve_blocked_candidate(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(20),
        )
        .await
        .unwrap();
    let stop = stopped(&broker).expect("the projection failure still stops the run");
    assert_eq!(stop.0, "FACT_CONFLICT");
    assert_eq!(
        stop.1.as_deref(),
        Some("UNBOUND_CANDIDATE"),
        "the first reason is kept, the latch adds nothing"
    );
}

