//! Native live003 regression; no archived data or real transport is used.
use super::*;
use crate::bot::arbitrage::phase1::{EvaluationInputs, SettlementProvider};
use crate::service::observation::{Clock, FixtureClock, FreshnessBasis, ObservationClock, SourceTimeStatus};
use crate::service::venue::OrderVenue;

fn down_evaluation(clock: &FixtureClock) -> (
    crate::service::venue::TradableMarket,
    crate::models::BookSnapshot,
    crate::bot::arbitrage::phase1::Phase1Evaluation,
) {
    let mut market = crate::bot::runtime::test_support::market(4668196);
    market.yes_token = "77".into();
    market.no_token = "88".into();
    market.feed_symbol = Some("BTCUSDT".into());
    market.start_price = Some(100.0);
    market.starts_at = Some(clock.now().unwrap().utc - chrono::Duration::seconds(30));
    market.ends_at = Some(market.starts_at.unwrap() + chrono::Duration::seconds(300));
    let book = crate::bot::runtime::test_support::book(&[(0.36, 100.0)], &[(0.37, 100.0)]);
    let observe = || {
        let now = clock.now().unwrap();
        ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound, request_at: now.utc, received_at: now.utc,
            request_elapsed_ms: now.elapsed_ms, received_elapsed_ms: now.elapsed_ms }
    };
    let mut core = crate::bot::arbitrage::DecisionCore::phase1(Default::default());
    for _ in 0..=20 {
        clock.advance(1000);
        core.observe_phase1("BTCUSDT", 99.9, crate::service::spot::SpotSource::Binance,
            "BTCUSDT", &observe(), clock.now().unwrap()).unwrap();
    }
    let observation = observe();
    let evaluation = core.evaluate_phase1(&EvaluationInputs {
        market: &market, book: &book, spot_price: 99.9,
        spot_source: Some(crate::service::spot::SpotSource::Binance), actual_pair: Some("BTCUSDT".into()),
        spot_clock: Some(observation.clone()), book_clock: Some(observation), now: clock.now().unwrap(),
        order_usd: 1.0, min_edge: 0.03, min_secs_left: 30.0, entered: false,
        identity_available_at: market.starts_at, strike_available_at: market.starts_at,
        settlement_provider: SettlementProvider::Chainlink,
    });
    (market, book, evaluation)
}

#[tokio::test]
async fn lowercase_payer_reaches_native_down_send_after_multiple_account_reads() {
    // Public live003 inputs: Down 1.56 @ 0.64 under the current 1-USDT order
    // contract (the original 2-USDT run saw 3.12 shares), 200bps, PRIMARY,
    // 1144ms recheck.
    // Source events.jsonl SHA256: 435fa46b907afb54add360260bcd319b572ca1e17713aeb5fc118eb7ad19f5bd.
    // Synthetic spot history and short token ids are not the original incident replay.
    let (mut broker, clock) = risk_fixture();
    let (market, book, evaluation) = down_evaluation(&clock);
    let candidate = evaluation.candidate.unwrap();
    assert_eq!(candidate.token(), "88");
    assert_eq!(candidate.gross_shares(), 1.56);
    assert_eq!(candidate.limit_price(), 0.64);
    assert_eq!(candidate.fee_rate_bps(), 200);
    assert_eq!(evaluation.diagnostic.source_state, crate::bot::arbitrage::phase1::SourceState::Primary);
    let (trader, transport, trace) =
        crate::service::predict_fun::controlled::synthetic_controlled_trader_with_trace(200, "live003");
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
        run.options.gas_policy.payer_address = identity.signer_address.to_ascii_lowercase();
        assert_ne!(run.options.gas_policy.payer_address, identity.signer_address);
        let receipt = account_receipt(&state).unwrap();
        record_account_read(&mut state, &baseline, receipt).unwrap();
    }
    let broker = Arc::new(broker);
    for _ in 0..3 {
        live::check_runtime_account(&broker, false).await.unwrap();
    }
    broker.observe(&market, &book);
    assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
    clock.advance(1144);
    let ack = ack_next(broker.clone(), false);
    let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate }).await.unwrap();
    assert!(outcome.attempt_id.is_some(), "first failure: {:?}; lowercase payer must identify the same signer", outcome.result);
    ack.join().unwrap();
    let id = outcome.attempt_id.unwrap();
    let guard = outcome.guard.unwrap();
    assert!(guard.is_acked() && guard.was_consumed());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let gross = U256::from(1_560_000_000_000_000_000u128);
    let cash = U256::from(998_400_000_000_000_000u128);
    let fee = gross / U256::from(100); // Existing fixture reports an actual 1% share fee.
    let bound = guard.bound.lock().clone().unwrap();
    assert_eq!(bound.request_gross_atoms, gross);
    assert_eq!(bound.max_cash_atoms, cash);
    assert_eq!(bound.fee_rate_bps, 200);
    assert_eq!(trace.posted.lock().unwrap()[0]["data"]["order"]["makerAmount"], cash.to_string());
    {
        let state = broker.state.lock();
        assert_eq!(state.live.net_cash[&id], cash);
        assert_eq!(state.live.net_inventory[&id], gross - fee);
        assert_eq!(state.live.reservations[&id], U256::ZERO);
        assert!(state.live.controlled.as_ref().unwrap().may_add_risk());
    }
    // Same account/broker: an authoritative subsequent read must still reconcile.
    live::sync_predict(&broker).await.unwrap();
    live::check_runtime_account(&broker, false).await.unwrap();
    assert!(broker.gate.as_ref().unwrap().allows_new_orders(&identity));
    // A later candidate uses the same broker, account gate and committed ledger.
    // The existing 1-USDT per-market cap must still refuse another full order.
    let (next_market, next_book, next_evaluation) = down_evaluation(&clock);
    let next_candidate = next_evaluation.candidate.unwrap();
    live::check_runtime_account(&broker, false).await.unwrap();
    broker.observe(&next_market, &next_book);
    assert!(broker.emit_phase1_evaluation(&next_market, &next_evaluation.diagnostic, Some(&next_candidate)));
    let next = broker.take_live_candidate(taker::LiveCandidateRequest {
        market: &next_market, yes_book: &next_book, candidate: &next_candidate }).await.unwrap();
    assert!(matches!(next.result, TakeResult::Blocked(Blocked::MarketCap)));
    assert!(next.attempt_id.is_none());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    let state = broker.state.lock();
    assert_eq!(state.live.attempts.len(), 1);
    assert_eq!(state.live.net_cash[&id], cash);
    assert!(!state.live.controlled.as_ref().unwrap().may_add_risk());
}

#[test]
fn gas_payer_case_equivalence_preserves_address_gas_receipt_and_budget_refusals() {
    let (broker, clock) = risk_fixture();
    let mut state = broker.state.lock();
    // Public signer from original live-context.json; no signing key is loaded.
    let raw = "0x5129330159f4e6a99ef873f86901e3cafa7f9a7d";
    let checksum = raw.parse::<alloy_primitives::Address>().unwrap().to_checksum(None);
    assert_ne!(raw, checksum);
    let mut identity = state.live.controlled.as_ref().unwrap().identity.clone();
    identity.signer_address = checksum.clone();
    broker.gate.as_ref().unwrap().confirm_budget(&identity);
    let mut baseline = state.live.baseline.clone().unwrap();
    baseline.identity = identity.clone();
    state.live.baseline = Some(baseline.clone());
    state.live.controlled.as_mut().unwrap().identity = identity;
    let receipt = account_receipt(&state).unwrap();
    record_account_read(&mut state, &baseline, receipt).unwrap();
    let cash = U256::from(998_400_000_000_000_000u128);
    for (payer, expected) in [
        (raw, None), (checksum.as_str(), None),
        ("0x5129330159F4E6A99EF873F86901E3CAFA7F9A7D", None),
        ("0x5129330159f4e6a99ef873f86901e3cafa7f9a7e", Some(Blocked::DataIncomplete)),
        ("0x1111111111111111111111111111111111111111", Some(Blocked::DataIncomplete)),
        ("", Some(Blocked::DataIncomplete)), ("bad", Some(Blocked::DataIncomplete)),
    ] {
        state.live.controlled.as_mut().unwrap().options.gas_policy.payer_address = payer.into();
        assert_eq!(broker.check_live_entry(&mut state, 4668196, cash), expected, "{payer}");
    }
    state.live.controlled.as_mut().unwrap().options.gas_policy.payer_address = raw.into();
    assert_eq!(broker.check_live_entry(&mut state, 4668196, U256::from(SCALE+1)), Some(Blocked::OrderCap));
    for (gas, expected) in [(Some(1000u64), None), (Some(900), Some(Blocked::DataIncomplete)),
        (Some(0), Some(Blocked::DataIncomplete)), (Some(1001), Some(Blocked::DataIncomplete)),
        (None, Some(Blocked::DataIncomplete))] {
        state.live.controlled.as_mut().unwrap().account_read.as_mut().unwrap().snapshot.gas_atoms = gas.map(U256::from);
        assert_eq!(broker.check_live_entry(&mut state, 4668196, cash), expected);
    }
    state.live.controlled.as_mut().unwrap().account_read.as_mut().unwrap().snapshot.gas_atoms = Some(U256::from(1000));
    clock.advance(10_001);
    assert_eq!(broker.check_live_entry(&mut state, 4668196, cash), Some(Blocked::DataIncomplete));
}
