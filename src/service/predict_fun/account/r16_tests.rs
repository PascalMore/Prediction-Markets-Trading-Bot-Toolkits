//! R15/R16 account dependency and error-priority tests at the existing fake transport.
use super::*;
use crate::models::{OrderType, PlannedOrder, Side, VenueId};
use crate::service::venue::{AttemptRecord, ControlledSendGuard, OrderIdentity, OrderVenue, Submission};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct ColdHistory {
    inner: Arc<super::super::fills::tests::Scenario>,
    position_pages: AtomicUsize,
    positions_done: AtomicBool,
    hide_matches: AtomicBool,
}
impl SyntheticTransport for ColdHistory {
    fn auth_token(&self) -> String { self.inner.auth_token() }
    fn sign(&self, _: &super::super::Order) -> anyhow::Result<(B256, String)> {
        panic!("an account read cannot sign")
    }
    fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
        self.inner.rpc(method, params)
    }
    fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&Value>)
        -> anyhow::Result<String> {
        if path.starts_with("/positions?") {
            self.position_pages.fetch_add(1, Ordering::SeqCst);
            if !path.contains("after=") {
                self.positions_done.store(false, Ordering::SeqCst);
                return Ok(serde_json::json!({"data":[],"cursor":"positions-still-pending"}).to_string());
            }
            self.positions_done.store(true, Ordering::SeqCst);
        }
        if path.starts_with("/orders/matches?") {
            assert!(self.positions_done.load(Ordering::SeqCst), "history consumed before all position pages");
            if !path.contains("orderHashes=") && self.hide_matches.load(Ordering::SeqCst) {
                return Ok(serde_json::json!({"data":[]}).to_string());
            }
        }
        self.inner.rest(method, path, bearer, body)
    }
}

#[tokio::test]
async fn cold_position_only_market_precedes_declared_history_and_hot_scan_still_rejects_foreign_hash() {
    use super::super::fills::tests::{historical_trader, history_record, record_history, queried_order_count};
    let (mut trader, original, [hash, zero]) = historical_trader("history_duplicate").await;
    trader.client.markets.write().clear();
    let transport = Arc::new(ColdHistory { inner: original.clone(), position_pages: AtomicUsize::new(0),
        positions_done: AtomicBool::new(false), hide_matches: AtomicBool::new(true) });
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(transport.clone());
    let cold = trader.read_account_snapshot().await.unwrap();
    assert!(cold.complete && cold.positions_explained);
    assert_eq!(transport.position_pages.load(Ordering::SeqCst), 2);
    let reads = queried_order_count(&original, &hash) + queried_order_count(&original, &zero);
    assert!(reads > 0);
    let hot = trader.read_account_snapshot().await.unwrap();
    assert_eq!(cold.positions, hot.positions);
    assert!(hot.positions_explained);
    assert_eq!(queried_order_count(&original, &hash) + queried_order_count(&original, &zero), reads);
    transport.hide_matches.store(false, Ordering::SeqCst);
    let mut record = history_record(&original, &hash);
    record["matches"][1]["taker"]["hash"] = serde_json::json!(format!("0x{}", "e".repeat(64)));
    record_history(&original, &hash, record);
    assert_eq!(trader.read_account_snapshot().await.unwrap_err().to_string(),
        "RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY");
}

/// A declared order with no fills cannot seed its market from positions or matches.
struct ZeroFillHistory {
    inner: Arc<super::super::fills::tests::Scenario>,
    fault: &'static str,
    market_reads: AtomicUsize,
}
impl SyntheticTransport for ZeroFillHistory {
    fn auth_token(&self) -> String { self.inner.auth_token() }
    fn sign(&self, _: &super::super::Order) -> anyhow::Result<(B256, String)> {
        panic!("history reconciliation must not sign")
    }
    fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
        if self.fault == "executable" && method == "eth_call" {
            use alloy_sol_types::SolCall;
            let selector = hex::encode(ExchangeRead::getOrderStatusCall::SELECTOR);
            if params[0]["data"].as_str().unwrap().starts_with(&format!("0x{selector}")) {
                return Ok(serde_json::json!(format!("0x{:064x}{:064x}", 0, 0)));
            }
        }
        self.inner.rpc(method, params)
    }
    fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&Value>)
        -> anyhow::Result<String> {
        assert_eq!(*method, reqwest::Method::GET, "history reconciliation cannot write");
        if path.starts_with("/positions?") || path.starts_with("/orders/matches?") {
            return Ok(serde_json::json!({"data":[]}).to_string());
        }
        if path == "/markets/1" {
            self.market_reads.fetch_add(1, Ordering::SeqCst);
            if self.fault == "read" { return Err(read_error("READ_FAILED").into()); }
            let mut market = serde_json::json!({"id":1,"tradingStatus":"CLOSED",
                "conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,
                "feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]});
            match self.fault {
                "wrong_id" => market["id"] = serde_json::json!(2),
                "missing_token" => market["outcomes"][0]["onChainId"] = serde_json::json!("99"),
                "duplicate" => market["outcomes"][1]["onChainId"] = serde_json::json!("77"),
                "extra" => market["outcomes"].as_array_mut().unwrap().push(
                    serde_json::json!({"onChainId":"99"})),
                _ => {}
            }
            return Ok(serde_json::json!({"data":market}).to_string());
        }
        self.inner.rest(method, path, bearer, body)
    }
}

async fn zero_fill_history(fault: &'static str) -> (PredictFunTrader, Arc<ZeroFillHistory>, String) {
    use super::super::fills::tests::{historical_trader, history_record, record_history};
    let (mut trader, inner, [_, zero]) = historical_trader("history_duplicate").await;
    let mut record = history_record(&inner, &zero);
    if fault == "open" || fault == "unknown" {
        record["order"]["status"] = serde_json::json!(if fault == "open" { "OPEN" } else { "NEW_STATUS" });
        record_history(&inner, &zero, record);
    }
    trader.run_context.write().as_mut().unwrap().reconciled_orders = vec![zero.clone()];
    trader.client.markets.write().clear();
    let transport = Arc::new(ZeroFillHistory { inner, fault, market_reads: AtomicUsize::new(0) });
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(transport.clone());
    (trader, transport, zero)
}

#[tokio::test]
async fn zero_fill_cold_history_reconciles_without_positions_or_matches_and_memoizes_final() {
    use super::super::fills::tests::queried_order_count;
    let (trader, transport, zero) = zero_fill_history("").await;
    assert!(trader.client.markets.read().is_empty());
    let signs = transport.inner.base.signs.load(Ordering::SeqCst);
    let posts = transport.inner.base.posts.load(Ordering::SeqCst);
    let snapshot = trader.read_account_snapshot().await.unwrap();
    assert!(snapshot.complete && snapshot.positions_explained);
    assert!(snapshot.positions.is_empty() && snapshot.open_orders.is_empty());
    assert_eq!(transport.market_reads.load(Ordering::SeqCst), 1);
    let reads = queried_order_count(&transport.inner, &zero);
    assert!(reads >= 3, "must read and verify the actual declared order");
    assert!(trader.read_account_snapshot().await.unwrap().positions_explained);
    assert_eq!(queried_order_count(&transport.inner, &zero), reads);
    assert_eq!(transport.market_reads.load(Ordering::SeqCst), 1);
    assert_eq!(transport.inner.base.signs.load(Ordering::SeqCst), signs);
    assert_eq!(transport.inner.base.posts.load(Ordering::SeqCst), posts);
}

#[tokio::test]
async fn zero_fill_cold_history_keeps_unclosed_orders_unproven() {
    for fault in ["open", "unknown", "executable"] {
        let (trader, _, _) = zero_fill_history(fault).await;
        let error = trader.read_account_snapshot().await.unwrap_err();
        assert_eq!(error.to_string(), "RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED", "{fault}");
        assert!(trader.history_proven.read().is_empty());
    }
}

#[tokio::test]
async fn zero_fill_cold_history_rejects_wrong_market_binding_and_failed_reads() {
    for fault in ["wrong_id", "missing_token", "duplicate", "extra", "read"] {
        let (trader, transport, _) = zero_fill_history(fault).await;
        assert!(trader.read_account_snapshot().await.is_err(), "{fault}");
        assert!(trader.history_proven.read().is_empty());
        assert_eq!(transport.market_reads.load(Ordering::SeqCst), 1, "{fault}");
    }
}

#[tokio::test]
async fn zero_fill_cold_history_rejects_conflicting_preexisting_cache() {
    for fault in ["cached_wrong_id", "partial_replacement", "complement_first", "complement_second"] {
        let (trader, _, _) = zero_fill_history("").await;
        let (first, second) = match fault {
            "cached_wrong_id" => ("77", "88"),
            "complement_first" => ("88", "99"),
            _ => ("99", "88"),
        };
        let cached: ApiMarket = serde_json::from_value(serde_json::json!({
            "id":if fault == "partial_replacement" { 1 } else { 2 },
            "tradingStatus":"CLOSED", "conditionId":B256::repeat_byte(0xcc),
            "isNegRisk":false, "isYieldBearing":false, "feeRateBps":0,
            "outcomes":[{"onChainId":first}, {"onChainId":second}]
        })).unwrap();
        trader.client.remember_market(&cached);
        let error = trader.read_account_snapshot().await.unwrap_err();
        assert_eq!(error.downcast_ref::<VenueError>().unwrap().reason, "TOKEN_MISMATCH", "{fault}");
        assert!(trader.history_proven.read().is_empty());
        assert_eq!(trader.client.market_for_token("88").unwrap().outcomes[0].on_chain_id,
            cached.outcomes[0].on_chain_id, "failed hydration must not poison the cache");
    }
}

struct FundsFault {
    inner: Arc<dyn SyntheticTransport>,
    balance: &'static str,
}
impl SyntheticTransport for FundsFault {
    fn auth_token(&self) -> String { self.inner.auth_token() }
    fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
        if method == "eth_call" {
            let data = params[0]["data"].as_str().unwrap();
            let selector = hex::decode(&data[2..10]).unwrap();
            if selector == TokenRead::balanceOfCall::SELECTOR {
                if self.balance == "error" { return Err(read_error("BALANCE_FAULT")); }
                if self.balance == "zero" { return Ok(serde_json::json!(format!("0x{:064x}",0))); }
            }
            if selector == ExchangeRead::registryCall::SELECTOR { return Err(read_error("AUTHORIZE_FAULT")); }
        }
        if method == "eth_getBalance" && self.balance == "zero_gas" {
            return Ok(serde_json::json!("0x0"));
        }
        self.inner.rpc(method, params)
    }
    fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&Value>)
        -> anyhow::Result<String> { self.inner.rest(method, path, bearer, body) }
    fn sign(&self, _: &super::super::Order) -> anyhow::Result<(B256, String)> {
        panic!("funds/authorize fault must fail before signing")
    }
}
struct Preparation {
    snapshot: AccountSnapshot,
    calls: AtomicUsize,
    stop_after_balance: bool,
}
impl ControlledSendGuard for Preparation {
    fn check_preparation(&self) -> anyhow::Result<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 4 && self.stop_after_balance {
            anyhow::bail!("RUN_STOPPED");
        }
        Ok(())
    }
    fn account_snapshot(&self) -> anyhow::Result<AccountSnapshot> { Ok(self.snapshot.clone()) }
    fn check_book(&self, _: &crate::models::BookSnapshot) -> anyhow::Result<()> { Ok(()) }
}

#[tokio::test]
async fn balances_preparation_funds_and_authorize_keep_the_original_failure_order() {
    for (balance, stop, expected) in [("error", false, "BALANCE_FAULT"), ("error", true, "BALANCE_FAULT"),
        ("zero", false, "INSUFFICIENT_FUNDS"), ("zero_gas", false, "INSUFFICIENT_FUNDS"),
        ("enough", false, "AUTHORIZE_FAULT"), ("enough", true, "RUN_STOPPED"),
        ("zero", true, "RUN_STOPPED")] {
        let (mut trader, base) = super::super::controlled::synthetic_controlled_trader(200, "controlled_fill");
        let snapshot = trader.account_snapshot().await.unwrap();
        let identity = trader.identity();
        let client = Arc::get_mut(&mut trader.client).unwrap();
        let original = client.synthetic.clone().unwrap();
        client.synthetic = Some(Arc::new(FundsFault { inner: original, balance }));
        let order = PlannedOrder { venue: VenueId::PredictFun, token_id: "77".into(), side: Side::Buy,
            shares: 2.5, limit_price: 0.4, usd_notional: 1.0, order_type: OrderType::Fak, source_trade_hash: None };
        let handle = Arc::new(parking_lot::Mutex::new(AttemptRecord {
            identity: OrderIdentity { local_attempt_id: 1, signed_hash: None, venue_order_id: None,
                account: identity, market_id: 1, token_id: "77".into(), side: Side::Buy },
            request_gross_atoms: U256::from(25) * U256::from(super::super::WEI) / U256::from(10),
            r0: U256::from(super::super::WEI), reserved_sell: U256::ZERO,
            phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
        }));
        let guard = Preparation { snapshot, calls: AtomicUsize::new(0), stop_after_balance: stop };
        let error = trader.place_controlled_tracked(&order, 0, true, handle, &guard).await.unwrap_err();
        let reason = error.downcast_ref::<VenueError>().map(|error| error.reason.as_str())
            .unwrap_or_else(|| if error.to_string() == "RUN_STOPPED" { "RUN_STOPPED" } else { "UNEXPECTED" });
        assert_eq!(reason, expected, "{balance}/{stop}: {error:#}");
        assert_eq!(base.signs.load(Ordering::SeqCst), 0);
        assert_eq!(base.posts.load(Ordering::SeqCst), 0);
    }
}
