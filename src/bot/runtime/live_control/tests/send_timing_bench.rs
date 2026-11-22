//! Send-path timing benchmark (excluded from the default suite).
//!
//! It runs the real Broker submission path against the offline synthetic venue,
//! once per iteration, and reports the p50/p95 of every measured segment plus
//! the network round trips each submission needs. `YQUANT_SYNTH_RTT_MS` prices
//! one round trip, so the same basis can show the code cost (0 ms) and a
//! network-modelled cost without ever leaving the machine.
//!
//! Run explicitly:
//!   YQUANT_TIMING=1 YQUANT_TIMING_RUNS=35 YQUANT_SYNTH_RTT_MS=0 \
//!     cargo test --lib send_path_timing_benchmark -- --ignored --exact --nocapture

use super::*;

fn percentile(sorted: &[u64], percent: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let rank = ((percent / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "timing benchmark: run explicitly with YQUANT_TIMING=1"]
async fn send_path_timing_benchmark() {
    use crate::bot::runtime::timing;
    use crate::service::venue::OrderVenue;
    let runs: usize = std::env::var("YQUANT_TIMING_RUNS").ok()
        .and_then(|value| value.parse().ok()).unwrap_or(35);
    let mut stages: std::collections::BTreeMap<String, Vec<u64>> =
        Default::default();
    let mut decision_us = Vec::new();
    let mut end_to_end_us = Vec::new();
    let mut net_calls = Vec::new();
    for _ in 0..runs {
        // The same fixture as sending_fixture_with_trace, with the decision
        // computation measured in front of the submission it produces.
        let (mut broker, clock) = risk_fixture();
        let mut core = crate::bot::arbitrage::DecisionCore::phase1(
            crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
        let started = std::time::Instant::now();
        let (market, book, evaluation) = evaluate_live_round_with_core(&clock, true, 1, &mut core);
        let decision = started.elapsed().as_micros() as u64;
        let candidate = evaluation.candidate.expect("the fixture round yields a candidate");
        let (trader, transport, _trace) =
            crate::service::predict_fun::controlled::synthetic_controlled_trader_with_trace(
                candidate.fee_rate_bps(), "controlled_fill");
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
        let broker = Arc::new(broker);
        let ack = ack_next(broker.clone(), false);
        let outcome = broker.take_live_candidate(taker::LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate }).await
            .expect("the offline venue fills the measured submission");
        ack.join().unwrap();
        assert!(matches!(outcome.result, TakeResult::Filled { .. }), "{:?}", outcome.result);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        let segments = timing::last_segments();
        for (stage, us) in &segments {
            stages.entry(stage.clone()).or_default().push(*us);
        }
        let total = timing::last_total_us();
        decision_us.push(decision);
        end_to_end_us.push(decision + total);
        net_calls.push(timing::last_net_calls());
    }
    let line = |name: &str, values: &[u64]| -> String {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        format!("\"{name}\":{{\"p50_us\":{},\"p95_us\":{},\"n\":{}}}",
            percentile(&sorted, 50.0), percentile(&sorted, 95.0), sorted.len())
    };
    let mut parts: Vec<String> = vec![line("decision", &decision_us), line("end_to_end", &end_to_end_us)];
    for (stage, values) in &stages {
        parts.push(line(stage, values));
    }
    let mut calls = net_calls.clone();
    calls.sort_unstable();
    let report = format!(
        "{{\"event\":\"send_path_timing\",\"runs\":{},\"rtt_ms\":{},\"net_calls_p50\":{},\"net_calls_max\":{},\"segments\":{{{}}}}}",
        runs,
        std::env::var("YQUANT_SYNTH_RTT_MS").unwrap_or_else(|_| "0".into()),
        percentile(&calls, 50.0),
        calls.last().copied().unwrap_or(0),
        parts.join(",")
    );
    println!("YQUANT_TIMING_REPORT {report}");
    if let Ok(path) = std::env::var("YQUANT_TIMING_OUT") {
        std::fs::write(path, format!("{report}\n")).unwrap();
    }
}
