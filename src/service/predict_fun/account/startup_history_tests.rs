//! Startup history regressions through the original account consumer.
use super::*;
use crate::service::predict_fun::fills::tests::{
    historical_trader, history_record, record_history, Scenario,
};
use crate::service::venue::OrderVenue;
use parking_lot::Mutex as Lock;
use reqwest::Method;
use crate::service::predict_fun::fills::{self, BoundOrder};
use crate::service::venue::{AttemptRecord, Closure, Field, OrderIdentity, Submission};

/// Historical economic inputs with synthetic account/hash/receipt identities.
async fn r20_fixture(mode: &'static str) -> (PredictFunTrader, Arc<Scenario>, String, BoundOrder) {
    let (trader, transport, [hash, _]) = historical_trader(mode).await;
    let mut record = history_record(&transport, &hash);
    record["order"]["status"] = json!(if mode == "history_r20_open" { "OPEN" } else { "CANCELLED" });
    record["order"]["amountFilled"] = json!("2629999999999999999");
    record["order"]["order"]["makerAmount"] = json!("999400000000000000");
    record["order"]["order"]["takerAmount"] = json!("2630000000000000000");
    for (index, (quantity, fee)) in [
        ("349533875338742820", "6990677506774856"),
        ("2280466124661257179", "45609322493225143"),
    ].into_iter().enumerate() {
        let leg = &mut record["matches"][index]["taker"];
        leg["amount"] = json!(quantity);
        leg["price"] = json!("380000000000000000");
        leg["fee"] = json!({"amount":fee,"type":"SHARES"});
    }
    let requested = U256::from(2630000000000000000u128);
    let maker = U256::from(999400000000000000u128);
    let bound = BoundOrder {
        handle: Arc::new(Lock::new(AttemptRecord {
            identity: OrderIdentity {
                local_attempt_id: 0,
                signed_hash: Some(hash.clone()),
                venue_order_id: Some(record["order"]["id"].as_str().unwrap().into()),
                market_id: 1,
                token_id: "77".into(),
                side: crate::models::Side::Buy,
                account: trader.identity(),
            },
            request_gross_atoms: requested,
            r0: maker,
            reserved_sell: U256::ZERO,
            phase: AttemptPhase::Submitted,
            submission: Submission::Unknown,
            last_reason: None,
        })),
        market: trader.client.market_for_token("77").unwrap(),
        requested,
        maker_amount: maker,
        taker_amount: requested,
        expiration: record["order"]["order"]["expiration"].as_i64().unwrap(),
    };
    record_history(&transport, &hash, record);
    // This case declares only the economic order, not the unrelated zero-fill fixture.
    trader.run_context.write().as_mut().unwrap().reconciled_orders = vec![hash.clone()];
    assert!(trader.bound_orders.read().is_empty());
    assert!(trader.history_proven.read().is_empty());
    transport.paths.lock().clear();
    (trader, transport, hash, bound)
}

#[tokio::test]
async fn r20_non_atomic_cash_original_consumers() {
    for mode in ["history_r20", "history_r20_open"] {
        let (trader, transport, hash, bound) = r20_fixture(mode).await;
        let record = history_record(&transport, &hash);
        let matches = serde_json::from_value::<Vec<super::super::ApiMatch>>(record["matches"].clone()).unwrap();
        let legs = fills::own_legs(&matches, &hash, &trader.identity().trading_address).unwrap();
        let facts = fills::cumulative(&legs, U256::from(2629999999999999999u128));
        let execution = trader.read_order_execution(&hash, &bound).await.unwrap();
        let reason = bound.handle.lock().last_reason.clone();
        // No successful execution is inserted into history_proven before this cold read.
        let history = trader.reconcile_account_history().await;
        println!("D20-01 mode={mode} cumulative={facts:?} pages={} accounting={} closure={:?} reason={reason:?} history_error={:?}",
            execution.pages_complete, execution.accounting_complete(), execution.closure,
            history.as_ref().err().map(ToString::to_string));
        let facts = facts.unwrap();
        assert_eq!(facts.gross_cash_atoms, U256::from(999399999999999999u128));
        assert!(execution.pages_complete && execution.accounting_complete());
        assert_eq!(execution.accounting.net_cash.value(), Some(&U256::from(999399999999999999u128)));
        assert_eq!(execution.accounting.share_fee.value(), Some(&U256::from(52599999999999999u128)));
        assert_eq!(execution.accounting.net_inventory.value(), Some(&U256::from(2577400000000000000u128)));
        for tx in 1..=2 {
            assert_eq!(transport.paths.lock().iter().filter(|p| **p == format!("receipt:0x{tx:064x}")).count(), 2);
        }
        if mode == "history_r20_open" {
            assert_eq!(execution.closure, Closure::Open);
            assert_eq!(history.err().unwrap().to_string(), "RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED");
        } else {
            assert!(matches!(execution.closure, Closure::Final { final_gross_q, .. } if final_gross_q == bound.requested - U256::from(1)));
            assert_eq!(history.unwrap().totals["77"], U256::from(2577400000000000000u128));
        }
        let mut atomic = legs[..1].to_vec();
        atomic[0].q_atoms = U256::from(super::super::WEI);
        assert_eq!(fills::cumulative(&atomic, atomic[0].q_atoms).unwrap().gross_cash_atoms,
            U256::from(380000000000000000u128));
        // Prove per-leg floor, not floor after summing the products.
        let mut tiny = legs.clone();
        for leg in &mut tiny { leg.q_atoms = U256::from(2); }
        assert_eq!(fills::cumulative(&tiny, U256::from(4)).unwrap().gross_cash_atoms, U256::ZERO);
    }
}

#[tokio::test]
async fn r20_non_atomic_cash_mismatch_stays_unproven() {
    for mode in [
        "history_r20_event_cash_minus", "history_r20_event_cash_plus",
        "history_r20_transfer_cash_minus", "history_r20_transfer_cash_plus",
        "history_r20_offset", "history_r20_event_fee_minus", "history_r20_event_fee_plus",
        "history_r20_inventory_minus", "history_r20_inventory_plus",
    ] {
        let (trader, transport, hash, bound) = r20_fixture(mode).await;
        let execution = trader.read_order_execution(&hash, &bound).await.unwrap();
        assert!(execution.pages_complete, "{mode}");
        assert!(matches!(execution.closure, Closure::Final { .. }), "{mode}");
        assert!(!execution.accounting_complete(), "{mode}");
        assert_eq!(execution.accounting.net_cash, Field::Unknown("SETTLEMENT_UNPROVEN".into()));
        assert_eq!(execution.accounting.net_inventory, Field::Unknown("SETTLEMENT_UNPROVEN".into()));
        let before = transport.paths.lock().iter().filter(|p| p.starts_with("receipt:")).count();
        assert!(before > 0, "{mode} must read actual receipt");
        let error = trader.reconcile_account_history().await.err().unwrap();
        assert_eq!(error.to_string(), "RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED", "{mode}");
        let after = transport.paths.lock().iter().filter(|p| p.starts_with("receipt:")).count();
        assert!(after > before, "{mode} cold history must reread receipt");
        assert!(trader.history_proven.read().is_empty());
        println!("D20-02 mode={mode} receipt_reads={before}->{after} error={error}");
    }
}

/// Raw-DTO overlay and route counter; never a second account/settlement model.
pub(crate) struct HistoryTransport {
    inner: Arc<Scenario>,
    pub(crate) calls: Lock<Vec<String>>,
    pub(crate) fault: Lock<&'static str>,
    cash: Lock<Option<U256>>,
    positions: std::sync::atomic::AtomicUsize,
    hashes: Vec<String>,
}
impl HistoryTransport {
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let calls = self.calls.lock();
        (
            calls
                .iter()
                .filter(|p| p.starts_with("/markets/") || p.starts_with("/categories/"))
                .count(),
            calls
                .iter()
                .filter(|p| p.starts_with("/orders/") && !p.starts_with("/orders/matches?"))
                .count(),
            calls
                .iter()
                .filter(|p| p.starts_with("/orders/matches?") && !p.contains("orderHashes="))
                .count(),
        )
    }
}
impl SyntheticTransport for HistoryTransport {
    fn auth_token(&self) -> String {
        self.inner.auth_token()
    }
    fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
        if method == "eth_call"
            && params[0]["data"]
                .as_str()
                .unwrap()
                .starts_with("0x70a08231")
        {
            if let Some(cash) = *self.cash.lock() {
                return Ok(json!(format!("0x{cash:064x}")));
            }
        }
        self.inner.rpc(method, params)
    }
    fn rest(
        &self,
        method: &Method,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> anyhow::Result<String> {
        assert_eq!(*method, Method::GET, "history-only reads cannot POST");
        self.calls.lock().push(path.into());
        let fault = *self.fault.lock();
        if (fault == "page_failure" && path.contains("signerAddress=") && path.contains("after="))
            || (fault == "account_failure" && path == "/account")
        {
            return Err(VenueError::new(
                VenueErrorCode::ReadUnavailable,
                "synthetic",
                "READ_FAILED",
            )
            .into());
        }
        if path.starts_with("/markets/") && fault == "http503" {
            return Err(crate::service::predict_fun::ApiError {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                body: "synthetic".into(),
            }
            .into());
        }
        if path == "/markets/1" && fault == "schema" {
            return Ok(json!({"data":{"id":"invalid"}}).to_string());
        }
        if path == "/markets/1" && fault == "transport" {
            return Err(reqwest::Client::new()
                .get("http://[synthetic")
                .build()
                .unwrap_err()
                .into());
        }
        if path == "/categories/sports" && fault == "category_http503" {
            return Err(crate::service::predict_fun::ApiError {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                body: "synthetic".into(),
            }
            .into());
        }
        if path == "/categories/sports" && fault == "category_schema" {
            return Ok(json!({"data":{"variantData":{"endPrice":"invalid"}}}).to_string());
        }
        let mut raw: Value = serde_json::from_str(&self.inner.rest(method, path, bearer, body)?)?;
        // The old Scenario only supports two pages. This overlay has a real
        // numeric cursor so additional economic legs reach the original reader.
        if let Some(query) = path.strip_prefix("/orders/matches?") {
            if !matches!(
                *self.inner.mode.lock(),
                "history_missing_page" | "history_loop" | "history_missing_fee"
            ) {
                let params: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
                let filtered = params.get("orderHashes").map(|v| v.as_ref());
                let page: usize = params.get("after").map(|v| v.parse().unwrap()).unwrap_or(0);
                let events: Vec<_> = self
                    .hashes
                    .iter()
                    .filter(|hash| filtered.is_none_or(|wanted| wanted == hash.as_str()))
                    .flat_map(|hash| {
                        history_record(&self.inner, hash)["matches"]
                            .as_array()
                            .unwrap()
                            .clone()
                    })
                    .collect();
                raw = json!({"data": events.get(page).cloned().into_iter().collect::<Vec<_>>(),
                    "cursor": (page + 1 < events.len()).then(|| (page + 1).to_string())});
            }
        }
        if path == "/markets/1" {
            raw["data"]["decimalPrecision"] = json!(2);
            match fault {
                "variant" => raw["data"]["marketVariant"] = json!("SPORTS"),
                "unpublished" => raw["data"]["status"] = json!("REGISTERED"),
                "contested" => raw["data"]["status"] = json!("PRICE_DISPUTED"),
                "pair" => raw["data"]["outcomes"][1]["onChainId"] = json!("99"),
                _ => {}
            }
        }
        if path == "/categories/sports" {
            raw["data"]["startsAt"] = json!("2026-01-01T00:00:00Z");
            raw["data"]["endsAt"] = json!("2026-01-01T00:05:00Z");
            match fault {
                "legacy" => {
                    raw["data"].as_object_mut().unwrap().remove("startsAt");
                    raw["data"].as_object_mut().unwrap().remove("endsAt");
                }
                "start" => {
                    raw["data"].as_object_mut().unwrap().remove("startsAt");
                }
                "end" => {
                    raw["data"].as_object_mut().unwrap().remove("endsAt");
                }
                "duration" => raw["data"]["endsAt"] = json!("2026-01-01T00:06:00Z"),
                "feed" => raw["data"]["variantData"]["priceFeedSymbol"] = json!("ETHUSDT"),
                "provider" => raw["data"]["variantData"]["priceFeedProvider"] = json!("OTHER"),
                "round" => raw["data"]["variantData"] = Value::Null,
                "remote_revision" => raw["data"]["variantData"]["endPrice"] = json!(110),
                _ => {}
            }
        }
        if path.starts_with("/positions?") {
            let n = self
                .positions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if fault == "position" || (fault == "second_position" && n > 0) {
                raw = serde_json::from_str(&self.inner.base.rest(method, path, bearer, body)?)?;
                raw["data"][0]["amount"] = json!("1");
            }
        }
        if path.contains("signerAddress=") {
            match fault {
                "route" => {
                    for event in raw["data"].as_array_mut().unwrap() {
                        event["market"]["isYieldBearing"] = json!(true);
                    }
                }
                "observed_result" => {
                    for event in raw["data"].as_array_mut().unwrap() {
                        event["market"]["outcomes"][0]["status"] = json!("WON");
                    }
                }
                "description" => {
                    for event in raw["data"].as_array_mut().unwrap() {
                        event["market"]["question"] = json!("unrelated description changed");
                    }
                }
                _ => {}
            }
        }
        Ok(raw.to_string())
    }
}

pub(crate) async fn history_fixture(
    mode: &'static str,
) -> (PredictFunTrader, Arc<HistoryTransport>, [String; 2]) {
    let (mut trader, inner, hashes) = historical_trader(mode).await;
    let transport = Arc::new(HistoryTransport {
        inner,
        calls: Lock::new(vec![]),
        fault: Lock::new(""),
        cash: Lock::new(None),
        positions: Default::default(),
        hashes: hashes.to_vec(),
    });
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(transport.clone());
    (trader, transport, hashes)
}

#[tokio::test]
async fn cold_reads_require_commit_and_hot_reads_skip_cleared_history() {
    for mode in [
        "history_settled_zero",
        "history_claimed_win",
        "history_claimed_tie",
    ] {
        let (trader, transport, _) = history_fixture(mode).await;
        trader.begin_startup_history("run", "window").unwrap();
        let first = trader.account_snapshot().await.unwrap();
        assert!(first.positions_explained);
        let cold = transport.counts();
        let uncommitted = trader.account_snapshot().await.unwrap();
        assert_eq!(first, uncommitted);
        let before = transport.counts();
        assert!(before.0 > cold.0, "pending is not active");
        trader
            .commit_startup_history("run", "window", &uncommitted)
            .unwrap();
        for _ in 0..2 {
            let next = trader.account_snapshot().await.unwrap();
            assert_eq!(next, first);
        }
        let after = transport.counts();
        assert_eq!((after.0, after.1), (before.0, before.1));
        assert!(after.2 > before.2, "full account matches still read");
        assert!(
            transport
                .calls
                .lock()
                .iter()
                .filter(|p| p.as_str() == "/account")
                .count()
                >= 4
        );
        println!("R18_COUNTS mode={mode} cold={cold:?} pending={before:?} hot2={after:?}");
    }
}

#[tokio::test]
async fn only_complete_final_official_and_exact_cash_can_commit() {
    for fault in [
        "legacy", "provider", "feed", "variant", "start", "end", "duration",
    ] {
        let (trader, transport, _) = history_fixture("history_claimed_win").await;
        trader.begin_history("run", "window").unwrap();
        *transport.fault.lock() = fault;
        let snapshot = trader.account_snapshot().await.unwrap();
        assert!(
            snapshot.positions_explained,
            "metadata cannot strengthen admission: {fault}"
        );
        trader.commit_history("run", "window", &snapshot).unwrap();
        let before = transport.counts();
        assert_eq!(trader.account_snapshot().await.unwrap(), snapshot);
        assert!(
            transport.counts().0 > before.0,
            "noneligible target must still GET: {fault}"
        );
    }
    for fault in [
        "unpublished",
        "contested",
        "pair",
        "round",
        "http503",
        "page_failure",
    ] {
        let (trader, transport, _) = history_fixture("history_claimed_win").await;
        trader.begin_history("run", "window").unwrap();
        *transport.fault.lock() = fault;
        let result = trader.account_snapshot().await;
        match result {
            Ok(snapshot) => {
                assert!(!snapshot.positions_explained, "{fault}");
                assert!(
                    trader.commit_history("run", "window", &snapshot).is_err(),
                    "{fault}"
                );
            }
            Err(_) => {}
        }
        *transport.fault.lock() = "";
        let valid = trader.account_snapshot().await.unwrap();
        assert!(valid.positions_explained, "{fault}");
        let mut altered = valid.clone();
        altered.collateral_atoms = altered.collateral_atoms.map(|v| v + U256::from(1));
        assert!(trader.commit_history("run", "window", &altered).is_err());
        assert!(trader
            .commit_history("other-run", "window", &valid)
            .is_err());
        trader.commit_history("run", "window", &valid).unwrap();
        trader.commit_history("run", "window", &valid).unwrap();
    }
    for drift in [-1i128, 1] {
        let (trader, transport, _) = history_fixture("history_claimed_tie").await;
        trader.begin_history("run", "window").unwrap();
        let cash = U256::from(95_380_000_000_000_000_000u128);
        *transport.cash.lock() = Some(if drift < 0 {
            cash - U256::from(1)
        } else {
            cash + U256::from(1)
        });
        let snapshot = trader.account_snapshot().await.unwrap();
        assert!(!snapshot.positions_explained);
        assert!(trader.commit_history("run", "window", &snapshot).is_err());
    }
    for mode in [
        "history_open",
        "history_missing_fee",
        "history_missing_page",
        "history_loop",
    ] {
        let (trader, _, _) = history_fixture(mode).await;
        trader.begin_history("run", "window").unwrap();
        assert!(trader.account_snapshot().await.is_err(), "{mode}");
    }
}

#[tokio::test]
async fn unpolled_revision_is_not_claimed_detected_but_observed_conflicts_stop() {
    for fault in [
        "remote_revision",
        "route",
        "observed_result",
        "description",
        "position",
    ] {
        let (trader, transport, _) = history_fixture("history_settled_zero").await;
        trader.begin_history("run", "window").unwrap();
        let snapshot = trader.account_snapshot().await.unwrap();
        trader.commit_history("run", "window", &snapshot).unwrap();
        let before = transport.counts();
        *transport.fault.lock() = fault;
        let result = trader.account_snapshot().await;
        if matches!(fault, "remote_revision" | "description") {
            assert_eq!(result.unwrap(), snapshot);
            assert_eq!(transport.counts().0, before.0);
        } else {
            assert_eq!(
                result
                    .unwrap_err()
                    .downcast_ref::<VenueError>()
                    .unwrap()
                    .code,
                VenueErrorCode::ReconciliationRequired,
                "{fault}"
            );
            *transport.fault.lock() = "";
            assert!(
                trader.account_snapshot().await.is_err(),
                "conflict remains sticky"
            );
            assert!(trader.commit_history("run", "window", &snapshot).is_err());
        }
    }
}

#[tokio::test]
async fn complete_matches_detect_new_or_changed_history() {
    for fault in [
        "new_hash",
        "missing",
        "amount",
        "fee",
        "unknown_fee",
        "more",
        "reverse",
    ] {
        let (trader, transport, [hash, _]) = history_fixture("history_settled_zero").await;
        trader.begin_history("run", "window").unwrap();
        let snapshot = trader.account_snapshot().await.unwrap();
        trader.commit_history("run", "window", &snapshot).unwrap();
        let mut record = history_record(&transport.inner, &hash);
        let events = record["matches"].as_array_mut().unwrap();
        match fault {
            "new_hash" => events[1]["taker"]["hash"] = json!(format!("0x{}", "e".repeat(64))),
            "missing" => {
                events.pop();
            }
            "amount" => events[0]["taker"]["amount"] = json!("1"),
            "fee" => events[0]["taker"]["fee"]["amount"] = json!("1"),
            "unknown_fee" => {
                events[0]["taker"].as_object_mut().unwrap().remove("fee");
            }
            "more" => {
                let mut event = events[0].clone();
                event["executedAt"] = json!("2030-01-01T00:00:00Z");
                events.push(event);
            }
            "reverse" => events.reverse(),
            _ => unreachable!(),
        }
        record_history(&transport.inner, &hash, record);
        let result = trader.account_snapshot().await;
        if fault == "reverse" {
            assert_eq!(result.unwrap(), snapshot);
        } else if fault == "new_hash" {
            assert_eq!(
                result.unwrap_err().to_string(),
                "RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY"
            );
        } else {
            assert_eq!(
                result
                    .unwrap_err()
                    .downcast_ref::<VenueError>()
                    .unwrap()
                    .code,
                VenueErrorCode::ReconciliationRequired,
                "{fault}"
            );
        }
    }
    let (trader, transport, _) = history_fixture("history_settled_zero").await;
    trader.begin_history("run", "window").unwrap();
    let snapshot = trader.account_snapshot().await.unwrap();
    trader.commit_history("run", "window", &snapshot).unwrap();
    *transport.fault.lock() = "page_failure";
    assert!(trader.account_snapshot().await.is_err());
    *transport.fault.lock() = "";
    let reads = transport.counts();
    assert_eq!(trader.account_snapshot().await.unwrap(), snapshot);
    assert_eq!(
        transport.counts().0,
        reads.0,
        "transient failure must not discard committed members"
    );
}

/// Eleven independent declared markets, raw order/match DTOs and corresponding
/// receipt/registry ABI facts. Optional current-run transport is the original R16 one.
pub(crate) struct GroupTransport {
    source: Arc<HistoryTransport>,
    original: [String; 2],
    live: Option<Arc<dyn SyntheticTransport>>,
    pub(crate) calls: Lock<Vec<String>>,
    pub(crate) fault: Lock<&'static str>,
}
impl GroupTransport {
    fn hash(index: usize, zero: bool) -> String {
        format!("0x{:064x}", 10_000 + index * 2 + usize::from(zero))
    }
    fn token(index: usize, no: bool) -> u64 {
        10_000 + index as u64 * 100 + if no { 88 } else { 77 }
    }
    pub(crate) fn opening_cash(&self) -> U256 {
        U256::from(49_500_000_000_000_000_000u128)
    }
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let calls = self.calls.lock();
        (
            calls
                .iter()
                .filter(|p| {
                    p.strip_prefix("/markets/")
                        .and_then(|id| id.parse::<u64>().ok())
                        .is_some_and(|id| (1000..1011).contains(&id))
                        || p.starts_with("/categories/old-")
                })
                .count(),
            calls
                .iter()
                .filter(|p| {
                    (0..11).any(|i| {
                        p.as_str() == format!("/orders/{}", Self::hash(i, false))
                            || p.as_str() == format!("/orders/{}", Self::hash(i, true))
                    })
                })
                .count(),
            calls
                .iter()
                .filter(|p| p.starts_with("/orders/matches?") && !p.contains("orderHashes="))
                .count(),
        )
    }
    fn raw(&self, value: &mut Value, index: usize) {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields.iter_mut() {
                    match key.as_str() {
                        "marketId" => *value = json!(1000 + index),
                        "id" if value == &json!(1) => *value = json!(1000 + index),
                        "onChainId" | "tokenId" if value == "77" => {
                            *value = json!(Self::token(index, false).to_string())
                        }
                        "onChainId" | "tokenId" if value == "88" => {
                            *value = json!(Self::token(index, true).to_string())
                        }
                        "hash" if value == &json!(self.original[0]) => {
                            *value = json!(Self::hash(index, false))
                        }
                        "hash" if value == &json!(self.original[1]) => {
                            *value = json!(Self::hash(index, true))
                        }
                        "transactionHash" => {
                            let original = U256::from_str_radix(
                                value.as_str().unwrap().trim_start_matches("0x"),
                                16,
                            )
                            .unwrap()
                            .to::<usize>();
                            *value = json!(format!("0x{:064x}", 100_000 + index * 10 + original));
                        }
                        "categorySlug" => *value = json!(format!("old-{index}")),
                        _ => self.raw(value, index),
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.raw(value, index);
                }
            }
            _ => {}
        }
    }
    fn receipt(&self, value: &mut Value, index: usize, tx: &str) {
        // Replace complete ABI words only; quantities and unrelated topics stay exact.
        for log in value["logs"].as_array_mut().unwrap() {
            for field in ["data", "topics"] {
                let encoded = serde_json::to_string(&log[field])
                    .unwrap()
                    .replace(
                        self.original[0].trim_start_matches("0x"),
                        Self::hash(index, false).trim_start_matches("0x"),
                    )
                    .replace(
                        &format!("{:064x}", 77),
                        &format!("{:064x}", Self::token(index, false)),
                    )
                    .replace(
                        &format!("{:064x}", 88),
                        &format!("{:064x}", Self::token(index, true)),
                    );
                log[field] = serde_json::from_str(&encoded).unwrap();
            }
            log["transactionHash"] = json!(tx);
        }
        value["transactionHash"] = json!(tx);
    }
}
impl SyntheticTransport for GroupTransport {
    fn auth_token(&self) -> String {
        self.source.auth_token()
    }
    fn sign(&self, order: &crate::service::predict_fun::Order) -> anyhow::Result<(B256, String)> {
        self.live
            .as_ref()
            .expect("history-only fixture cannot sign")
            .sign(order)
    }
    fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
        use alloy_sol_types::SolCall;
        if method == "eth_getTransactionReceipt" {
            let tx = params[0].as_str().unwrap();
            let number = U256::from_str_radix(tx.trim_start_matches("0x"), 16).unwrap();
            if number >= U256::from(100_000) && number < U256::from(100_110) {
                let number = number.to::<usize>() - 100_000;
                let mut receipt = self
                    .source
                    .rpc(method, &json!([format!("0x{:064x}", number % 10)]))?;
                self.receipt(&mut receipt, number / 10, tx);
                return Ok(receipt);
            }
        }
        if method == "eth_call" {
            let data = params[0]["data"].as_str().unwrap();
            if data.starts_with(&format!(
                "0x{}",
                hex::encode(ExchangeRead::registryCall::SELECTOR)
            )) {
                let token = U256::from_str_radix(&data[data.len() - 64..], 16)
                    .unwrap()
                    .to::<u64>();
                for index in 0..11 {
                    for no in [false, true] {
                        if token == Self::token(index, no) {
                            return Ok(json!(format!(
                                "0x{:064x}{}",
                                Self::token(index, !no),
                                hex::encode(B256::repeat_byte(0xcc))
                            )));
                        }
                    }
                }
            }
            if data.starts_with("0x70a08231") {
                let cash = if let Some(live) = &self.live {
                    let value = live.rpc(method, params)?;
                    U256::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16)
                        .unwrap()
                } else {
                    U256::from(98) * U256::from(super::super::WEI)
                };
                return Ok(json!(format!("0x{:064x}", cash + self.opening_cash())));
            }
        }
        match &self.live {
            Some(live) => live.rpc(method, params),
            None => self.source.rpc(method, params),
        }
    }
    fn rest(
        &self,
        method: &Method,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> anyhow::Result<String> {
        self.calls.lock().push(path.into());
        let fault = *self.fault.lock();
        if let Some(index) = path
            .strip_prefix("/markets/")
            .and_then(|id| id.parse::<usize>().ok())
            .and_then(|id| id.checked_sub(1000))
            .filter(|i| *i < 11)
        {
            let mut raw: Value =
                serde_json::from_str(&self.source.rest(method, "/markets/1", bearer, body)?)?;
            if index >= 5 {
                raw["data"]["outcomes"][0]["status"] = json!("LOST");
                raw["data"]["outcomes"][1]["status"] = json!("WON");
            }
            self.raw(&mut raw, index);
            return Ok(raw.to_string());
        }
        if let Some(index) = path
            .strip_prefix("/categories/old-")
            .and_then(|id| id.parse::<usize>().ok())
        {
            let mut raw: Value = serde_json::from_str(&self.source.rest(
                method,
                "/categories/sports",
                bearer,
                body,
            )?)?;
            if index >= 5 {
                raw["data"]["variantData"]["endPrice"] = json!(90);
            }
            if fault == "partial" && index == 10 {
                raw["data"]["variantData"]["priceFeedProvider"] = json!("OTHER");
            }
            return Ok(raw.to_string());
        }
        for index in 0..11 {
            for zero in [false, true] {
                if path == format!("/orders/{}", Self::hash(index, zero)) {
                    let mut raw: Value = serde_json::from_str(&self.source.rest(
                        method,
                        &format!("/orders/{}", self.original[usize::from(zero)]),
                        bearer,
                        body,
                    )?)?;
                    self.raw(&mut raw, index);
                    return Ok(raw.to_string());
                }
            }
        }
        if let Some(query) = path.strip_prefix("/orders/matches?") {
            let params: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
            let wanted = params.get("orderHashes").map(|v| v.as_ref());
            let mut events = Vec::new();
            for index in 0..11 {
                if wanted.is_some_and(|hash| hash != Self::hash(index, false)) {
                    continue;
                }
                for mut event in history_record(&self.source.inner, &self.original[0])["matches"]
                    .as_array()
                    .unwrap()
                    .clone()
                {
                    self.raw(&mut event, index);
                    events.push(event);
                }
            }
            if let Some(live) = &self.live {
                let raw: Value = serde_json::from_str(&live.rest(method, path, bearer, body)?)?;
                events.extend(raw["data"].as_array().unwrap().iter().cloned());
            }
            let page: usize = params.get("after").map(|v| v.parse().unwrap()).unwrap_or(0);
            let start = page * 2;
            return Ok(
                json!({"data": events.iter().skip(start).take(2).cloned().collect::<Vec<_>>(),
                "cursor": (start + 2 < events.len()).then(|| (page + 1).to_string())})
                .to_string(),
            );
        }
        match &self.live {
            Some(live) => live.rest(method, path, bearer, body),
            None => self.source.rest(method, path, bearer, body),
        }
    }
}

pub(crate) async fn group_fixture(
    current: bool,
) -> (
    PredictFunTrader,
    Arc<GroupTransport>,
    Option<Arc<crate::service::predict_fun::controlled::r16_fixture::Facts>>,
) {
    let (mut trader, source, original) = history_fixture("history_claimed_win").await;
    let (live, facts) = if current {
        let (new_trader, _, _, facts) =
            crate::service::predict_fun::controlled::r16_fixture::trader();
        trader = new_trader;
        (trader.client.synthetic.clone(), Some(facts))
    } else {
        (None, None)
    };
    let transport = Arc::new(GroupTransport {
        source,
        original,
        live,
        calls: Lock::new(vec![]),
        fault: Lock::new(""),
    });
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(transport.clone());
    {
        let mut context = trader.run_context.write();
        let context = context.as_mut().unwrap();
        context.budget_atoms = Some(U256::from(98) * U256::from(super::super::WEI));
        context.reconciled_orders = (0..11)
            .flat_map(|index| {
                [
                    GroupTransport::hash(index, false),
                    GroupTransport::hash(index, true),
                ]
            })
            .collect();
    }
    (trader, transport, facts)
}

fn canonical(mut snapshot: AccountSnapshot) -> AccountSnapshot {
    snapshot
        .settled_zero
        .sort_by(|left, right| left.token.cmp(&right.token));
    snapshot
        .claimed_win
        .sort_by(|left, right| left.token.cmp(&right.token));
    snapshot
}

#[tokio::test]
async fn eleven_targets_and_partial_eligibility_keep_strict_fallback() {
    for partial in [false, true] {
        let (trader, transport, _) = group_fixture(false).await;
        if partial {
            *transport.fault.lock() = "partial";
        }
        trader.begin_startup_history("run", "window").unwrap();
        let first = trader.account_snapshot().await.unwrap();
        assert!(first.positions_explained);
        assert_eq!((first.claimed_win.len(), first.settled_zero.len()), (5, 6));
        let cold = transport.counts();
        let pending = trader.account_snapshot().await.unwrap();
        assert_eq!(canonical(pending.clone()), canonical(first.clone()));
        let strict = transport.counts();
        assert_eq!(strict.0 - cold.0, 22);
        assert_eq!(
            strict.1, cold.1,
            "existing history_proven already saved details"
        );
        trader
            .commit_startup_history("run", "window", &pending)
            .unwrap();
        let mut intervals = Vec::new();
        for _ in 0..2 {
            let before = transport.counts();
            let start = transport.calls.lock().len();
            assert_eq!(
                canonical(trader.account_snapshot().await.unwrap()),
                canonical(first.clone())
            );
            let after = transport.counts();
            assert_eq!(after.0 - before.0, if partial { 2 } else { 0 });
            assert_eq!(after.1, before.1);
            assert_eq!(after.2 - before.2, 11);
            let routes = transport.calls.lock()[start..].iter().fold(
                std::collections::BTreeMap::<String, usize>::new(),
                |mut counts, path| {
                    *counts.entry(format!("GET {path}")).or_default() += 1;
                    counts
                },
            );
            intervals.push(routes);
        }
        for index in 0..11 {
            println!(
                "R18_TARGET market={} token={} payout={} eligible={}",
                1000 + index,
                GroupTransport::token(index, false),
                if index < 5 { 1 } else { 0 },
                !partial || index != 10
            );
        }
        println!(
            "R18_SCALE partial={partial} cold={cold:?} strict2={strict:?} hot2={:?} intervals={}",
            transport.counts(),
            serde_json::to_string(&intervals).unwrap()
        );
    }
}

#[tokio::test]
async fn observed_known_bindings_and_deployment_cannot_be_overwritten_by_hot_rows() {
    for fault in ["known_pair", "known_result", "deployment"] {
        let (mut trader, _, _) = history_fixture("history_settled_zero").await;
        trader.begin_startup_history("run", "window").unwrap();
        let snapshot = trader.account_snapshot().await.unwrap();
        trader
            .commit_startup_history("run", "window", &snapshot)
            .unwrap();
        if fault == "deployment" {
            Arc::get_mut(&mut trader.client).unwrap().deployment = &super::super::TESTNET;
        } else {
            let mut market = trader.client.market_for_token("77").unwrap();
            if fault == "known_pair" {
                market.outcomes[1].on_chain_id = "99".into();
            } else {
                market.outcomes[0].status = Some("WON".into());
            }
            trader.client.remember_market(&market);
        }
        let error = trader.account_snapshot().await.unwrap_err();
        let code = error.downcast_ref::<VenueError>().unwrap().code;
        assert_eq!(
            code,
            if fault == "deployment" {
                VenueErrorCode::AccountMismatch
            } else {
                VenueErrorCode::ReconciliationRequired
            },
            "{fault}"
        );
        assert!(trader
            .commit_startup_history("run", "window", &snapshot)
            .is_err());
    }
}

#[tokio::test]
async fn ineligible_history_keeps_original_typed_official_failures() {
    for fault in [
        "http503",
        "transport",
        "schema",
        "category_http503",
        "category_schema",
    ] {
        let (trader, transport, _) = history_fixture("history_settled_zero").await;
        trader.begin_startup_history("run", "window").unwrap();
        *transport.fault.lock() = "legacy";
        let snapshot = trader.account_snapshot().await.unwrap();
        trader
            .commit_startup_history("run", "window", &snapshot)
            .unwrap();
        *transport.fault.lock() = fault;
        let before = transport.counts();
        let error = trader.account_snapshot().await.unwrap_err();
        assert!(transport.counts().0 > before.0);
        assert_eq!(
            error.downcast_ref::<VenueError>().unwrap().code,
            if fault.contains("schema") {
                VenueErrorCode::FactIncomplete
            } else {
                VenueErrorCode::ReadUnavailable
            },
            "{fault}"
        );
    }
}
