//! R13b: a `CASH_PENDING` wait that only the missing claim caused.
//!
//! An official result that pays a positive claim stops the window in
//! `WAIT_CASH` until the cash is read back. When Pascal has the venue's own
//! auto-claim enabled, "result first, claim second" is the routine order, so a
//! wait that nothing can clear would glue the window shut. These tests pin the
//! two orders and the three ways the wait must survive: a claim one atom off,
//! a result that is not confirmed, and a claim for a token the window never
//! held. Everything here is the offline synthetic venue — no archived run,
//! network, signer or real order.

use super::*;

/// The venue seam of a runtime account read that now explains a claimed payout.
/// It answers exactly the one call `check_runtime_account` makes.
struct ClaimRead {
    snapshot: crate::service::venue::AccountSnapshot,
}

#[async_trait::async_trait]
impl crate::service::venue::OrderVenue for ClaimRead {
    fn id(&self) -> VenueId {
        VenueId::PredictFun
    }

    async fn place(
        &self,
        _planned: &crate::models::PlannedOrder,
        _expiration_secs: u64,
        _live: bool,
    ) -> anyhow::Result<crate::service::venue::OrderTicket> {
        unreachable!("a claim read never places an order")
    }

    async fn account_snapshot(&self) -> anyhow::Result<crate::service::venue::AccountSnapshot> {
        Ok(self.snapshot.clone())
    }
}

/// The venue's own claimed payout for `inventory` at `payout`.
fn claimed_win(
    inventory: U256,
    market_id: u64,
    token: &str,
    payout: f64,
) -> crate::service::venue::ClaimedWin {
    let claim_atoms =
        crate::bot::runtime::live_accounting::exact_floor_mul(inventory, payout).unwrap();
    crate::service::venue::ClaimedWin {
        token: token.into(),
        market_id,
        oracle_status: "RESOLVED".into(),
        trading_status: "CLOSED".into(),
        outcome_status: if payout == 0.5 { "DRAW".into() } else { "WON".into() },
        payout_millionths: (payout * 1_000_000.0) as u32,
        inventory_atoms: inventory,
        claim_atoms,
    }
}

/// The account after the venue paid `win`: the window's spend is gone from the
/// balance, the claimed shares are gone from the positions and the claim's cash
/// is in. Nothing else about the account moves.
fn claim_snapshot(broker: &Broker, win: crate::service::venue::ClaimedWin) -> crate::service::venue::AccountSnapshot {
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    let spent = state.live.net_cash.values().copied().sum::<U256>();
    let mut snapshot = state.live.baseline.clone().unwrap();
    snapshot.identity = run.identity.clone();
    snapshot.collateral_atoms = snapshot
        .collateral_atoms
        .map(|start| start - spent + win.claim_atoms);
    snapshot.positions = vec![];
    snapshot.claimed_win = vec![win];
    snapshot.open_orders = vec![];
    snapshot.positions_explained = true;
    snapshot.complete = true;
    snapshot
}

/// The same account, wrapped in the venue seam.
fn claim_read(broker: &Broker, win: crate::service::venue::ClaimedWin) -> ClaimRead {
    ClaimRead { snapshot: claim_snapshot(broker, win) }
}

/// The legal cleared flow on the real producer: the wire carries the order's
/// own lifecycle, so the canonical reader can associate the paid claim with
/// the attempt that owns it.
#[tokio::test]
async fn the_real_producer_pays_the_claim_and_the_reader_can_follow_it() {
    use crate::service::venue::OrderVenue;
    let (broker, _clock, market, book, candidate, _transport, _trace) =
        sending_fixture_with_trace("controlled_fill").await;
    let mut broker = broker;
    broker.observe(&market, &book);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    let id = outcome.attempt_id.unwrap();
    // The official result settles the round as a win: the window waits for its cash.
    let mut resolved = market.clone();
    resolved.open = false;
    resolved.end_price = Some(101.0);
    resolved.payout_yes = Some(1.0);
    resolved.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some("WON".into()), no_status: Some("LOST".into()),
        decimal_precision: None, price_feed_provider: None,
    });
    broker.settle_resolved(&OfficialResultData {
        result: Mutex::new(resolved), reads: Default::default() }).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let control = run.control.lock();
        let reason = control.stop.as_ref().map(|stop| stop.reason.clone());
        assert_eq!(control.state, LiveRunState::WaitCash,
                   "the official result waits for its cash: {reason:?}");
        assert_eq!(reason.as_deref(), Some("CASH_PENDING"));
    }
    let (inventory, token) = {
        let state = broker.state.lock();
        let token = state.live.attempts[&id].lock().identity.token_id.clone();
        let inventory = state.live.net_inventory[&id];
        (inventory, token)
    };
    // The account the venue reports once it paid the claim.
    let snapshot = claim_snapshot(&broker, claimed_win(inventory, market.id, &token, 1.0));
    let original = Arc::get_mut(&mut broker).unwrap();
    original.taker = Some(Arc::new(ClaimRead { snapshot }));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::Ready);
        assert_eq!(state.live.claims[&id], (inventory, inventory));
    }
    let events = crate::bot::runtime::live_events::prepare_final(&mut broker.state.lock(), true)
        .unwrap();
    for event in events { event.flush().unwrap(); }
}

/// The continuation on the real producer: once the exact claim is paid the
/// window is running again, so its next candidate must reach the wire through
/// RESERVE and FINAL_SEND. The canonical reader has to accept that stream: a
/// stop the paid claim revoked cannot be used to judge the later orders
/// OUTPUT_INVALID.
#[tokio::test]
async fn the_real_producer_trades_again_after_the_exact_claim_and_the_reader_follows_it() {
    use crate::service::venue::OrderVenue;
    let (broker, clock) = risk_fixture();
    // Two historical official rounds fit before the adapter's system-clock
    // expiration; elapsed freshness is still advanced by the real fixture clock.
    clock.set_utc(chrono::Utc::now() - chrono::Duration::minutes(10));
    let mut broker = Arc::new(broker);
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(crate::config::BtcArbConfig {
        order_usd: 1.0, ..Default::default() });
    let (trader, transport, trace) =
        crate::service::predict_fun::controlled::synthetic_controlled_two_round_trader(200);
    let identity = trader.identity();
    let gate = trader.gate.clone();
    let trader = Arc::new(trader);
    {
        let baseline = trader.account_snapshot().await.unwrap();
        let original = Arc::get_mut(&mut broker).unwrap();
        original.identity = Some(identity.clone());
        original.gate = Some(gate);
        original.taker = Some(trader.clone() as Arc<dyn OrderVenue>);
        let mut state = original.state.lock();
        state.live.baseline = Some(baseline.clone());
        let run = state.live.controlled.as_mut().unwrap();
        run.identity = identity.clone();
        run.options.gas_policy.payer_address = identity.signer_address.clone();
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    // Round 1: a win whose claim is what the venue pays.
    let (market, book, evaluation) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
    let candidate = evaluation.candidate.unwrap();
    crate::bot::runtime::live::check_runtime_account(&broker, false).await.unwrap();
    broker.observe(&market, &book);
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    {
        let mut state = broker.state.lock();
        assert_eq!(broker.check_live_entry(&mut state, 1, U256::from(SCALE)), None);
    }
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    let id = outcome.attempt_id.unwrap();
    let mut resolved = market.clone();
    resolved.open = false;
    resolved.end_price = Some(101.0);
    resolved.payout_yes = Some(1.0);
    resolved.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some("WON".into()), no_status: Some("LOST".into()),
        decimal_precision: None, price_feed_provider: None,
    });
    broker.settle_resolved(&OfficialResultData {
        result: Mutex::new(resolved), reads: Default::default() }).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let control = run.control.lock();
        assert_eq!(control.state, LiveRunState::WaitCash);
        assert_eq!(control.stop.as_ref().map(|stop| stop.reason.as_str()), Some("CASH_PENDING"));
    }
    // The next round's own time arrives first. The venue now reports the paid
    // claim, so the same adapter serves the next order and the account reads.
    clock.advance(300_000);
    let (market2, book2, evaluation2) = evaluate_live_round_with_core(&clock, true, 2, &mut core);
    let candidate2 = evaluation2.candidate.unwrap();
    let (inventory, token) = {
        let state = broker.state.lock();
        let token = state.live.attempts[&id].lock().identity.token_id.clone();
        let inventory = state.live.net_inventory[&id];
        (inventory, token)
    };
    let snapshot = claim_snapshot(&broker, claimed_win(inventory, market.id, &token, 1.0));
    let original = Arc::get_mut(&mut broker).unwrap();
    original.taker = Some(Arc::new(ResumedVenue { trader: trader.clone(), snapshot }));
    crate::bot::runtime::live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::Ready, "the exact claim ends the wait");
        assert!(run.may_add_risk());
    }
    // The window trades again through the same reserve/send path, after the
    // closed round's own venue reconciliation.
    crate::bot::runtime::live::sync_predict(&broker).await.unwrap();
    broker.observe(&market2, &book2);
    assert!(broker.emit_phase1_evaluation(&market2, &evaluation2.diagnostic, Some(&candidate2)));
    {
        let mut state = broker.state.lock();
        assert_eq!(broker.check_live_entry(&mut state, 2, U256::from(SCALE)), None,
            "the resumed window may enter its next round");
    }
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market2, yes_book: &book2, candidate: &candidate2 }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0),
        "the resumed window's second order reaches the venue");
    assert_eq!(transport.signs.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(transport.posts.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(trace.signed.lock().unwrap().len(), 2);
    // The window is still open on this wire: the reader judges a stream whose
    // last record is the resumed order's own release, not a terminal one.
}

/// The same account after the venue paid the claim, wrapped around the real
/// adapter: every order path is the adapter's own, only the account read now
/// shows the cash the claim paid and the shares it retired.
struct ResumedVenue {
    trader: Arc<crate::service::predict_fun::PredictFunTrader>,
    snapshot: crate::service::venue::AccountSnapshot,
}

#[async_trait::async_trait]
impl crate::service::venue::OrderVenue for ResumedVenue {
    fn id(&self) -> VenueId { self.trader.id() }

    fn reservation_atoms(&self, planned: &crate::models::PlannedOrder) -> anyhow::Result<U256> {
        self.trader.reservation_atoms(planned)
    }

    fn controlled_reservation_atoms(&self, planned: &crate::models::PlannedOrder) -> anyhow::Result<U256> {
        self.trader.controlled_reservation_atoms(planned)
    }

    fn controlled_route(&self, token_id: &str) -> anyhow::Result<String> {
        self.trader.controlled_route(token_id)
    }

    fn controlled_order_metadata(&self, signed_hash: &str) -> Option<crate::service::venue::ControlledOrderMetadata> {
        self.trader.controlled_order_metadata(signed_hash)
    }

    async fn place(&self, planned: &crate::models::PlannedOrder, expiration_secs: u64,
                   live: bool) -> anyhow::Result<crate::service::venue::OrderTicket> {
        self.trader.place(planned, expiration_secs, live).await
    }

    async fn place_controlled_tracked(
        &self, planned: &crate::models::PlannedOrder, expiration_secs: u64, live: bool,
        handle: crate::service::venue::AttemptHandle,
        guard: &dyn crate::service::venue::ControlledSendGuard,
    ) -> anyhow::Result<crate::service::venue::OrderTicket> {
        self.trader.place_controlled_tracked(planned, expiration_secs, live, handle, guard).await
    }

    async fn place_tracked(&self, planned: &crate::models::PlannedOrder, expiration_secs: u64,
                           live: bool, handle: crate::service::venue::AttemptHandle)
        -> anyhow::Result<crate::service::venue::OrderTicket> {
        self.trader.place_tracked(planned, expiration_secs, live, handle).await
    }

    fn record_account_reconciliation(&self, reason: &str) {
        self.trader.record_account_reconciliation(reason)
    }

    async fn execution_snapshot(&self, signed_hash: &str)
        -> anyhow::Result<crate::service::venue::ExecutionSnapshot> {
        self.trader.execution_snapshot(signed_hash).await
    }

    async fn account_snapshot(&self) -> anyhow::Result<crate::service::venue::AccountSnapshot> {
        Ok(self.snapshot.clone())
    }
}

/// The fail-closed counter-example on the same real producer: a claim one
/// atom short of what the confirmed result owes is never booked as cash, and
/// the terminal record keeps the run incomplete.
#[tokio::test]
async fn the_real_producer_refuses_a_claim_one_atom_short() {
    use crate::service::venue::OrderVenue;
    let (broker, _clock, market, book, candidate, _transport, _trace) =
        sending_fixture_with_trace("controlled_fill").await;
    let mut broker = broker;
    broker.observe(&market, &book);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    ack.join().unwrap();
    assert!(matches!(outcome.result, TakeResult::Filled { shares, .. } if shares > 0.0));
    let id = outcome.attempt_id.unwrap();
    let mut resolved = market.clone();
    resolved.open = false;
    resolved.end_price = Some(101.0);
    resolved.payout_yes = Some(1.0);
    resolved.round_facts = Some(crate::service::venue::RoundFacts {
        oracle_status: Some("RESOLVED".into()), trading_status: Some("CLOSED".into()),
        yes_status: Some("WON".into()), no_status: Some("LOST".into()),
        decimal_precision: None, price_feed_provider: None,
    });
    broker.settle_resolved(&OfficialResultData {
        result: Mutex::new(resolved), reads: Default::default() }).await.unwrap();
    let (inventory, token) = {
        let state = broker.state.lock();
        let token = state.live.attempts[&id].lock().identity.token_id.clone();
        let inventory = state.live.net_inventory[&id];
        (inventory, token)
    };
    // The venue paid one atom less than the confirmed result owes.
    let mut short = claimed_win(inventory, market.id, &token, 1.0);
    short.claim_atoms -= U256::from(1u64);
    let snapshot = claim_snapshot(&broker, short);
    let original = Arc::get_mut(&mut broker).unwrap();
    original.taker = Some(Arc::new(ClaimRead { snapshot }));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let control = run.control.lock();
        assert_eq!(control.state, LiveRunState::WaitCash,
                   "one atom short never closes the cash wait");
        assert_eq!(control.stop.as_ref().map(|stop| stop.reason.as_str()),
                   Some("CASH_PENDING"));
        assert!(!state.live.claims.contains_key(&id),
            "a claim one atom short is not ledger cash");
    }
    let events = crate::bot::runtime::live_events::prepare_final(&mut broker.state.lock(), true)
        .unwrap();
    for event in events { event.flush().unwrap(); }
}

/// Ordering a): the claim is credited before the official result is applied.
/// The claim is proven but not yet cash: it is booked only once the
/// confirmed official result owes exactly that amount.
#[tokio::test]
async fn a_claim_credited_before_the_official_result_never_stops_the_window() {
    let (mut broker, data) = official_result_fixture(1.0);
    let inventory = broker.state.lock().live.net_inventory[&1];
    broker.taker = Some(Arc::new(claim_read(&broker, claimed_win(inventory, 1, "1", 1.0))));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::Ready);
        assert!(run.control.lock().stop.is_none(), "the claim is already proven");
        assert!(!state.live.claims.contains_key(&1),
            "an unconfirmed claim is not ledger cash yet");
        assert_eq!(state.live.pending_claims[&1], (inventory, inventory));
    }
    broker.settle_resolved(&data).await.unwrap();
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().state, LiveRunState::Ready,
        "a credited claim must not open a cash wait");
    assert!(run.may_add_risk());
    let outcome = &run.attempt_outcomes[&1];
    assert!(outcome.cash_confirmed);
    assert_eq!(outcome.claim_value_atoms, Some(inventory));
    assert_eq!(state.live.claims[&1], (inventory, inventory),
        "the confirmed result books the proven claim as cash");
    assert!(!state.live.pending_claims.contains_key(&1));
    assert_eq!(crate::bot::runtime::live_events::state_accounting(&state).status, "COMPLETE");
}

/// Ordering b): the official result lands first and stops the window in
/// `WAIT_CASH`; the exact claim arrives afterwards. The wait must end and the
/// window's terminal record must report complete cash.
#[tokio::test]
async fn a_later_exact_claim_clears_the_cash_wait_and_the_final_cash_is_complete() {
    let (mut broker, data) = official_result_fixture(1.0);
    broker.settle_resolved(&data).await.unwrap();
    let inventory = broker.state.lock().live.net_inventory[&1];
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "CASH_PENDING");
        assert!(!run.may_add_risk());
    }
    broker.taker = Some(Arc::new(claim_read(&broker, claimed_win(inventory, 1, "1", 1.0))));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        let control = run.control.lock();
        assert_eq!(control.state, LiveRunState::Ready, "the exact claim ends the wait");
        assert!(control.stop.is_none(), "nothing else needed a stop");
        drop(control);
        assert_eq!(state.live.claims[&1], (inventory, inventory));
        assert!(run.attempt_outcomes[&1].cash_confirmed);
        assert!(run.may_add_risk());
    }
    // The window's own terminal record then reports its cash as complete.
    let events = crate::bot::runtime::live_events::prepare_final(&mut broker.state.lock(), true)
        .unwrap();
    for event in events {
        event.flush().unwrap();
    }
}

#[test]
fn captured_late_exact_claim_final_reports_complete_cash() {
    let events = capture_production_test(
        "claim_wait::a_later_exact_claim_clears_the_cash_wait_and_the_final_cash_is_complete");
    let payout = events.iter().find(|event| event["event_kind"] == "payout").unwrap();
    assert_eq!(payout["payload"]["status"], "CASH_PENDING", "the result did wait for its cash");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["cash_complete"], true,
        "the exact claim's cash is received, not pending");
    assert_eq!(terminal["payload"]["takeover_required"], false);
    assert_eq!(terminal["payload"]["run_status"], "STOPPED");
}

/// The fail-closed counter-example: a claim one atom short of what the
/// confirmed result owes is never booked as cash, and the terminal record
/// keeps the run incomplete.
#[tokio::test]
async fn a_claim_one_atom_short_is_never_booked_as_cash() {
    let (mut broker, data) = official_result_fixture(1.0);
    broker.settle_resolved(&data).await.unwrap();
    let inventory = broker.state.lock().live.net_inventory[&1];
    let mut short = claimed_win(inventory, 1, "1", 1.0);
    short.claim_atoms -= U256::from(1u64);
    broker.taker = Some(Arc::new(claim_read(&broker, short)));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "CASH_PENDING");
        assert!(!state.live.claims.contains_key(&1),
            "a claim one atom short is not ledger cash");
        assert!(state.live.pending_claims.contains_key(&1),
            "the proven claim waits for the official result");
    }
    let events = crate::bot::runtime::live_events::prepare_final(&mut broker.state.lock(), true)
        .unwrap();
    for event in events { event.flush().unwrap(); }
}

#[test]
fn captured_one_atom_short_claim_keeps_the_cash_incomplete() {
    let events = capture_production_test(
        "claim_wait::a_claim_one_atom_short_is_never_booked_as_cash");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["event_kind"], "final");
    assert_eq!(terminal["payload"]["cash_complete"], false,
        "a claim one atom short never closes the cash");
    assert_eq!(terminal["payload"]["takeover_required"], true);
}

/// Ordering c): a claim that is not exactly what the confirmed result owes, a
/// result that is not confirmed and a claim for a token this window never held
/// all keep the stop they already had.
#[tokio::test]
async fn a_late_claim_that_is_not_exact_leaves_the_cash_wait_in_place() {
    // One atom short of the owed payout.
    let (mut broker, data) = official_result_fixture(1.0);
    broker.settle_resolved(&data).await.unwrap();
    let inventory = broker.state.lock().live.net_inventory[&1];
    let mut short = claimed_win(inventory, 1, "1", 1.0);
    short.claim_atoms -= U256::from(1u64);
    broker.taker = Some(Arc::new(claim_read(&broker, short)));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::WaitCash,
            "one atom short is not the cash the result owes");
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "CASH_PENDING");
        assert!(!run.may_add_risk());
    }
}

#[tokio::test]
async fn a_claim_never_clears_a_stop_that_is_not_the_cash_wait() {
    // The official result is not confirmed: the window holds DATA_UNAVAILABLE,
    // not a cash wait, and a claim cannot turn that into a running window.
    let (mut broker, data) = official_result_fixture(1.0);
    {
        let mut market = data.result.lock();
        market.end_price = None;
        market.payout_yes = None;
    }
    broker.settle_resolved(&data).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DATA_UNAVAILABLE");
    }
    let inventory = broker.state.lock().live.net_inventory[&1];
    broker.taker = Some(Arc::new(claim_read(&broker, claimed_win(inventory, 1, "1", 1.0))));
    live::check_runtime_account(&broker, false).await.unwrap();
    {
        let state = broker.state.lock();
        let run = state.live.controlled.as_ref().unwrap();
        assert_eq!(run.control.lock().state, LiveRunState::StopNewRisk);
        assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "DATA_UNAVAILABLE");
        assert!(!run.may_add_risk());
    }
    // A claim for a token this window never held is not this ledger's claim:
    // the read stays fail-closed and the cash wait stands.
    let (mut broker, data) = official_result_fixture(1.0);
    broker.settle_resolved(&data).await.unwrap();
    let inventory = broker.state.lock().live.net_inventory[&1];
    broker.taker = Some(Arc::new(claim_read(&broker, claimed_win(inventory, 1, "999", 1.0))));
    assert!(live::check_runtime_account(&broker, false).await.is_err(),
        "a foreign claim is not this window's cash");
    let state = broker.state.lock();
    let run = state.live.controlled.as_ref().unwrap();
    assert_eq!(run.control.lock().state, LiveRunState::WaitCash);
    assert_eq!(run.control.lock().stop.as_ref().unwrap().reason, "CASH_PENDING");
    assert!(!run.may_add_risk());
}
