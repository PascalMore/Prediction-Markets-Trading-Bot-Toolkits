//! R16 T0→Tp probe, against fake transports only. Legacy before: NON_COMPARABLE.
use super::*;
use crate::service::observation::{CheckTime, Clock};
use crate::service::predict_fun::controlled::r16_fixture;
use crate::service::venue::OrderVenue;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

struct MeasuredClock { origin: CheckTime, started: Instant }
impl Clock for MeasuredClock {
    fn now(&self) -> anyhow::Result<CheckTime> {
        let elapsed = self.started.elapsed().as_millis() as u64;
        Ok(CheckTime { utc: self.origin.utc + chrono::Duration::milliseconds(elapsed as i64),
            elapsed_ms: self.origin.elapsed_ms + elapsed })
    }
}

fn statistics(samples: &[Value], key: &str) -> Value {
    let mut values: Vec<u64> = samples.iter().filter_map(|row| row[key].as_u64()).collect();
    values.sort_unstable();
    let rank = |percent: usize| values.get((values.len() * percent).div_ceil(100).saturating_sub(1)).copied();
    json!({"n":values.len(), "missing":samples.len()-values.len(),
        "p50_ms":rank(50), "p95_ms":rank(95), "max_ms":values.last()})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "offline timing probe, run explicitly with YQUANT_TIMING=1"]
async fn send_path_timing_benchmark() {
    let runs: usize = std::env::var("YQUANT_TIMING_RUNS").ok()
        .and_then(|value| value.parse().ok()).unwrap_or(9);
    assert!(runs > 0);
    let mut groups = Vec::new();
    for temperature in ["cold", "hot"] {
        let mut samples = Vec::new();
        for index in 0..runs {
            let (mut broker, clock) = risk_fixture();
            let mut core = crate::bot::arbitrage::DecisionCore::phase1(
                crate::config::BtcArbConfig { order_usd: 1.0, ..Default::default() });
            // Warmup and startup seeding are deliberately outside the measured tick.
            for _ in 0..=20 {
                clock.advance(1_000);
                let now = clock.now().unwrap();
                let observation = crate::service::observation::ObservationClock {
                    source_at: None, source_time_status: crate::service::observation::SourceTimeStatus::Missing,
                    freshness_basis: crate::service::observation::FreshnessBasis::ReceiptBound,
                    request_at: now.utc, received_at: now.utc,
                    request_elapsed_ms: now.elapsed_ms, received_elapsed_ms: now.elapsed_ms,
                };
                core.observe_phase1("BTCUSDT", 100.2, crate::service::spot::SpotSource::Binance,
                    "BTCUSDT", &observation, now).unwrap();
            }
            let (trader, transport) = r16_fixture::timing_trader().await;
            let baseline = trader.account_snapshot().await.unwrap();
            assert_eq!(r16_fixture::history_memo_count(&trader), 2);
            if temperature == "cold" { r16_fixture::cold_history(&trader); }
            let memo_before = r16_fixture::history_memo_count(&trader);
            let measured = Arc::new(MeasuredClock { origin: clock.now().unwrap(), started: Instant::now() });
            broker.identity = Some(trader.identity());
            broker.gate = Some(trader.gate.clone());
            broker.taker = Some(Arc::new(trader));
            {
                let mut state = broker.state.lock();
                state.live.baseline = Some(baseline);
                let run = state.live.controlled.as_mut().unwrap();
                run.identity = broker.identity.clone().unwrap();
                run.options.gas_policy.payer_address = run.identity.signer_address.clone();
                run.phase1.clock = measured.clone();
            }
            let broker = Arc::new(broker);
            // The actual production reader captures T0 before this complete request.
            live::check_runtime_account(&broker, false).await.unwrap();
            let (t0, receipt) = {
                let state = broker.state.lock();
                let run = state.live.controlled.as_ref().unwrap();
                (run.tick_started_elapsed_ms.unwrap(), run.account_read.as_ref().unwrap().checked)
            };
            let acquired = measured.now().unwrap();
            clock.advance(acquired.elapsed_ms - clock.now().unwrap().elapsed_ms);
            clock.set_utc(acquired.utc);
            core.observe_phase1("BTCUSDT", 100.2, crate::service::spot::SpotSource::Binance,
                "BTCUSDT", &crate::service::observation::ObservationClock {
                    source_at: None, source_time_status: crate::service::observation::SourceTimeStatus::Missing,
                    freshness_basis: crate::service::observation::FreshnessBasis::ReceiptBound,
                    request_at: acquired.utc, received_at: acquired.utc,
                    request_elapsed_ms: acquired.elapsed_ms, received_elapsed_ms: acquired.elapsed_ms,
                }, acquired).unwrap();
            let mut market = crate::bot::runtime::test_support::market(1);
            market.yes_token = "77".into(); market.no_token = "88".into();
            market.fee_rate_bps = 0; // Original declared-history fixture's actual venue fee.
            market.feed_symbol = Some("BTCUSDT".into()); market.start_price = Some(100.0);
            market.round_facts = Some(crate::service::venue::RoundFacts {
                decimal_precision: Some(2), ..Default::default() });
            market.starts_at = Some(acquired.utc - chrono::Duration::seconds(30));
            market.ends_at = Some(acquired.utc + chrono::Duration::seconds(270));
            let book = crate::bot::runtime::test_support::book(&[(0.3,100.0)], &[(0.4,100.0)]);
            let observation = crate::service::observation::ObservationClock {
                source_at: None, source_time_status: crate::service::observation::SourceTimeStatus::Missing,
                freshness_basis: crate::service::observation::FreshnessBasis::ReceiptBound,
                request_at: acquired.utc, received_at: acquired.utc,
                request_elapsed_ms: acquired.elapsed_ms, received_elapsed_ms: acquired.elapsed_ms,
            };
            let evaluation = core.evaluate_phase1(&crate::bot::arbitrage::phase1::EvaluationInputs {
                market:&market, book:&book, spot_price:100.2,
                spot_source:Some(crate::service::spot::SpotSource::Binance), actual_pair:Some("BTCUSDT".into()),
                spot_clock:Some(observation.clone()), book_clock:Some(observation), now:acquired,
                order_usd:1.0, min_edge:0.03, min_secs_left:30.0, entered:false,
                identity_available_at:market.starts_at, strike_available_at:market.starts_at,
                settlement_provider:crate::bot::arbitrage::phase1::SettlementProvider::Chainlink,
            });
            let candidate = evaluation.candidate.clone().unwrap_or_else(|| panic!("{:?}", evaluation.diagnostic));
            let tb = candidate.book_clock().received_elapsed_ms;
            assert!(t0 <= receipt.elapsed_ms && receipt.elapsed_ms <= tb);
            assert!(broker.emit_phase1_evaluation(&market, &evaluation.diagnostic, Some(&candidate)));
            let channel = broker.state.lock().live.controlled.as_ref().unwrap().channel.clone();
            let ack_clock = measured.clone();
            let ack = std::thread::spawn(move || {
                let pending = channel.wait_pending();
                std::thread::sleep(Duration::from_millis(25));
                let at = ack_clock.now().unwrap().elapsed_ms;
                channel.accept(&channel::ControlFrame::Ack { seq:pending.seq, signed_hash:pending.signed_hash }).unwrap();
                at
            });
            let result = broker.take_live_candidate(taker::LiveCandidateRequest {
                market:&market, yes_book:&book, candidate:&candidate }).await.unwrap();
            let ack_at = ack.join().unwrap();
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            let guard = result.guard.unwrap();
            let diagnostic = guard.diagnostic().unwrap();
            assert_eq!(diagnostic.stage, "SEND_PERMIT");
            assert_eq!(diagnostic.detail, "NONE");
            assert!(guard.was_consumed() && guard.is_acked());
            let tp = diagnostic.checked_elapsed_ms.unwrap();
            assert!(tp >= ack_at && tp >= tb && tb >= t0);
            samples.push(json!({"index":index, "classification":"SUCCESS",
                "declared_history_hashes":2, "memo_before":memo_before,
                "t0_elapsed_ms":t0, "account_receipt_elapsed_ms":receipt.elapsed_ms,
                "tb_elapsed_ms":tb, "ack_elapsed_ms":ack_at, "tp_elapsed_ms":tp,
                "tick_to_send_permit_ms":tp-t0, "book_receipt_to_send_permit_ms":tp-tb,
                "ack_to_send_permit_ms":tp-ack_at, "account_read_ms":receipt.elapsed_ms-t0,
                "account_receipt_age_ms":diagnostic.account_receipt_age_ms,
                "book_request_age_ms":diagnostic.book_request_age_ms,
                "book_receipt_age_ms":diagnostic.book_receipt_age_ms,
                "spot_request_age_ms":diagnostic.spot_request_age_ms,
                "spot_receipt_age_ms":diagnostic.spot_receipt_age_ms, "diagnostic":diagnostic,
                "send_only_net_calls":crate::bot::runtime::timing::last_net_calls(),
                "send_only_total_us":crate::bot::runtime::timing::last_total_us(),
                "segments_us":crate::bot::runtime::timing::last_segments()}));
        }
        let keys = ["tick_to_send_permit_ms", "book_receipt_to_send_permit_ms", "ack_to_send_permit_ms",
            "account_read_ms", "account_receipt_age_ms", "book_request_age_ms", "book_receipt_age_ms",
            "spot_request_age_ms", "spot_receipt_age_ms"];
        let metrics: serde_json::Map<String, Value> = keys.iter()
            .map(|key| ((*key).into(), statistics(&samples, key))).collect();
        groups.push(json!({"temperature":temperature, "success":samples.len(),
            "local_refusal":0, "unknown":0, "missing":0, "metrics":metrics, "samples":samples}));
    }
    let report = json!({"event":"send_path_timing",
        "measurement_revision":crate::bot::runtime::timing::MEASUREMENT_REVISION,
        "unit":"ms", "before":"NON_COMPARABLE", "production":"NOT_RUN",
        "input":"fixed synthetic book/spot; original DecisionCore/adapter/account/gate",
        "rtt_ms":std::env::var("YQUANT_SYNTH_RTT_MS").unwrap_or_else(|_|"0".into()),
        "ack_delay_ms":25, "runs_per_temperature":runs, "groups":groups});
    println!("YQUANT_TIMING_REPORT {report}");
}
