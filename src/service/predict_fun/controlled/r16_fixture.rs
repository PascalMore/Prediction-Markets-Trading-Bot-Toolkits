//! Transport facts for R16: the real trader/account/parser/gate remain in use.
use super::*;
use super::super::account::SyntheticTransport;
use crate::service::venue::TradableMarket;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use serde_json::{json, Value};

#[derive(Default)]
pub(crate) struct Facts {
    pub rounds: Mutex<HashMap<u64, TradableMarket>>,
    pub paid: Mutex<HashSet<u64>>,
    pub hidden: Mutex<HashSet<u64>>,
    /// Cash and fixed-block balance have moved, but REST still reports the shares.
    pub stale_positions: Mutex<HashSet<u64>>,
    pub position_overrides: Mutex<HashMap<u64, U256>>,
    pub cash_offset: Mutex<i128>,
    pub rpc_calls: Mutex<Vec<String>>,
    pub rest_calls: Mutex<Vec<String>>,
    /// Fault only the public official-settlement GET, never signing or POST.
    pub market_fault: Mutex<Option<&'static str>>,
    pub fail_census_header: Mutex<bool>,
    pub fail_position_balance: Mutex<Option<u64>>,
    pub position_order_reversed: Mutex<Option<bool>>,
    pub position_reads: Mutex<Vec<u64>>,
    pub proof_rpc_failures: Mutex<usize>,
    pub proof_rpc_body: Mutex<Option<&'static str>>,
    pub raw_rpc_reasons: Mutex<Vec<String>>,
    pub miss_receipt: Mutex<bool>,
    pub mixed_fill_receipt: Mutex<bool>,
    pub redemption: Mutex<Option<RedemptionFixture>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RedemptionStage { Before, Complete, After, ShortBatch, ExtraLegs, ShortWithExtraLegs, WrongParent, Foreign }

/// Shape of the scripted `eth_getBlockReceipts` batch array.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum BatchNull { #[default] None, Trailing, Only }

pub(crate) const REDEMPTION_ATOMS: u64 = 2_332_400_000_000_000_000;

pub(crate) struct RedemptionFixture {
    pub stage: RedemptionStage,
    pub condition: alloy_primitives::B256,
    pub token: U256,
    pub atoms: U256,
    pub next: Option<(U256, bool)>,
    pub batch_null: BatchNull,
    first_paid: bool,
}

impl RedemptionFixture {
    pub fn new(stage: RedemptionStage) -> Self {
        Self { stage, condition: alloy_primitives::B256::repeat_byte(0x41), token: U256::from(77),
            atoms: U256::from(REDEMPTION_ATOMS), next: None, batch_null: BatchNull::None, first_paid: true }
    }

    // Both scripted receipts are after the left anchor in this adapter-only census.
    pub(crate) fn c_census(first: Option<U256>, second: Option<U256>) -> Self {
        let mut script = Self::for_target(RedemptionStage::After, 0x41, first.unwrap_or_default());
        script.first_paid = first.is_some();
        script.next = second.map(|atoms| (atoms, false));
        script
    }

    pub fn for_target(stage: RedemptionStage, condition: u8, atoms: U256) -> Self {
        Self { condition: alloy_primitives::B256::repeat_byte(condition), atoms, ..Self::new(stage) }
    }

    fn rpc(&self, owner: alloy_primitives::Address, method: &str, params: &Value) -> Option<Value> {
        use alloy_primitives::{Address, Bloom, BloomInput, B256, keccak256};
        use RedemptionStage::*;
        let later = self.next.filter(|_| match method {
            "eth_getBlockByNumber" => matches!(params[0].as_str(), Some("latest" | "0x126")),
            "eth_getBlockReceipts" => params[0] == "0x126",
            "eth_getTransactionReceipt" => params[0] == json!(B256::repeat_byte(0xa5)),
            "eth_call" => params[1] == "0x126" && params[0]["data"].as_str().is_some_and(|data| {
                data.contains(&hex::encode([0xdd; 32])) || data.contains(&hex::encode([0xa6; 32]))
            }),
            _ => false,
        });
        let height = if later.is_some() { "0x126" } else { "0x125" };
        let (token, condition, q) = later.map_or((self.token, self.condition, self.atoms), |(atoms, _)| {
            (U256::from(177), B256::repeat_byte(0xdd), atoms)
        });
        let (usdt, ctf, _) = super::super::account::assets(56, false, false);
        let route = Address::repeat_byte(0xb1);
        let external = Address::repeat_byte(0xb2);
        let tx = B256::repeat_byte(if later.is_some() { 0xa5 } else { 0xa0 });
        let anchor = B256::repeat_byte(if later.is_some() { 0xa2 } else { 0xa1 });
        let block = B256::repeat_byte(if later.is_some() { 0xa4 } else { 0xa2 });
        let collection = B256::repeat_byte(if later.is_some() { 0xa6 } else { 0xa3 });
        let words = |words: &[U256]| format!("0x{}", words.iter().map(|word| format!("{word:064x}")).collect::<String>());
        let digest = |signature| keccak256(signature);
        let cash_sig = digest("Transfer(address,address,uint256)");
        let share_sig = digest("TransferSingle(address,address,address,uint256,uint256)");
        let payout_sig = digest("PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)");
        let log = |index: u64, address, topics: Vec<B256>, data: String| json!({"removed":false,
            "address":address,"topics":topics,"data":data,"transactionHash":tx,"transactionIndex":"0x0",
            "blockHash":block,"blockNumber":height,"logIndex":format!("{index:#x}")});
        let payment = |index, from: Address, to: Address| {
            log(index, usdt, vec![cash_sig, from.into_word(), to.into_word()], words(&[q]))
        };
        let mut logs = vec![
            log(0, ctf, vec![share_sig, owner.into_word(), owner.into_word(), route.into_word()], words(&[token, q])),
            log(1, ctf, vec![share_sig, route.into_word(), route.into_word(), B256::ZERO], words(&[token, q])),
            payment(2, ctf, route),
            log(3, ctf, vec![payout_sig, route.into_word(), usdt.into_word(), B256::ZERO],
                words(&[U256::from_be_bytes(condition.0), U256::from(96), q, U256::from(1), U256::from(1)])),
            payment(4, route, owner),
        ];
        if !self.first_paid && later.is_none() && matches!(self.stage, After) { logs.clear(); }
        if matches!(self.stage, Foreign) || later.is_some_and(|(_, foreign)| foreign) { logs = vec![payment(0, external, owner)]; }
        if matches!(self.stage, ExtraLegs | ShortWithExtraLegs) {
            logs.extend([payment(5, external, owner), payment(6, owner, external)]);
        }
        match method {
            "eth_getBlockByNumber" => {
                let requested = params[0].as_str().unwrap();
                if requested == "0x123" { return None; }
                if later.is_none() && (requested == "0x126" || (requested == "latest" && matches!(self.stage, After))) {
                    return Some(json!({"number":"0x126", "hash":B256::repeat_byte(0xa4),
                        "timestamp":"0x1006", "parentHash":block, "logsBloom":Bloom::ZERO, "transactions":[]}));
                }
                let empty = requested == "0x124" || (requested == "latest" && matches!(self.stage, Before));
                assert!(empty || requested == "latest" || requested == height);
                let mut bloom = Bloom::ZERO;
                if !empty {
                    for entry in &logs {
                        let address: Address = serde_json::from_value(entry["address"].clone()).unwrap();
                        bloom.accrue(BloomInput::Raw(address.as_slice()));
                        let topics: Vec<B256> = serde_json::from_value(entry["topics"].clone()).unwrap();
                        for topic in topics { bloom.accrue(BloomInput::Raw(topic.as_slice())); }
                    }
                }
                Some(json!({"number":if empty { "0x124" } else { height }, "hash":if empty { anchor } else { block },
                    "timestamp":if empty { "0x1000" } else if later.is_some() { "0x1006" } else { "0x1003" },
                    "parentHash":if empty || matches!(self.stage, WrongParent) { B256::ZERO } else { anchor },
                    "logsBloom":bloom,"transactions":if empty { vec![] } else { vec![tx] }}))
            }
            "eth_getBlockReceipts" | "eth_getTransactionReceipt" => {
                let batch = method == "eth_getBlockReceipts";
                if !batch && params[0] != json!(tx) { return None; }
                if batch {
                    assert_eq!(params, &json!([height]));
                    if matches!(self.stage, ShortBatch | ShortWithExtraLegs) { logs.pop(); }
                }
                let receipt = json!({"status":"0x1","blockNumber":height,"blockHash":block,
                    "transactionHash":tx,"transactionIndex":"0x0","logs":logs});
                Some(if !batch { receipt } else {
                    match self.batch_null {
                        BatchNull::Trailing => json!([receipt, Value::Null]),
                        BatchNull::Only => json!([Value::Null]),
                        BatchNull::None => json!([receipt]),
                    }
                })
            }
            "eth_call" => {
                let data = params[0]["data"].as_str().unwrap();
                let matches_call = |signature| data[2..10] == hex::encode(&digest(signature)[..4]);
                if matches_call("getCollectionId(bytes32,bytes32,uint256)") {
                    assert_eq!(params[0]["to"], json!(ctf));
                    assert_eq!(params[1], if matches!(self.stage, After) { "0x126" } else { height });
                    assert_eq!(&data[10..], &words(&[U256::ZERO, U256::from_be_bytes(condition.0), U256::from(1)])[2..]);
                    Some(json!(collection))
                } else if matches_call("getPositionId(address,bytes32)") {
                    assert_eq!(params[0]["to"], json!(ctf));
                    assert_eq!(params[1], if matches!(self.stage, After) { "0x126" } else { height });
                    assert_eq!(&data[10..], &words(&[U256::from_be_bytes(usdt.into_word().0), U256::from_be_bytes(collection.0)])[2..]);
                    Some(json!(words(&[token])))
                } else { None }
            }
            _ => None,
        }
    }
}

impl Facts {
    pub fn round(&self, market: TradableMarket) {
        self.rounds.lock().unwrap().insert(market.id, market);
    }
    pub fn pay(&self, token: u64) { self.paid.lock().unwrap().insert(token); }

    pub(crate) fn script_redemption(&self, script: RedemptionFixture) {
        *self.redemption.lock().unwrap() = Some(script);
    }

    pub(crate) fn script_lag_fault(&self, atoms: U256, fault: Option<&str>) {
        use RedemptionStage::*;
        let stage = match fault { Some("block") => WrongParent, Some("foreign") => Foreign, _ => Complete };
        let condition = if fault == Some("condition") { 0xcd } else { 0xcc };
        let mut script = RedemptionFixture::for_target(stage, condition, atoms);
        if fault == Some("token") { script.token = U256::from(88); }
        if fault == Some("route") { *self.market_fault.lock().unwrap() = Some("proof_route"); }
        self.lag_target(script, true);
    }

    pub(crate) fn lag_target(&self, script: RedemptionFixture, paid: bool) {
        self.stale_positions.lock().unwrap().insert(77);
        if paid {
            self.script_redemption(script);
            self.pay(77);
        } else {
            self.position_overrides.lock().unwrap().insert(77, U256::ZERO);
        }
    }

    pub(crate) fn clear_target_lag(&self, closing: RedemptionFixture, target_unpaid: bool) {
        if target_unpaid { self.pay(77); }
        self.script_redemption(closing);
        self.stale_positions.lock().unwrap().remove(&77);
    }
}

struct R16Transport { inner: BookTransport, facts: Arc<Facts>, owner: alloy_primitives::Address }

impl R16Transport {
    fn proof_rpc_failure(&self, method: &str) -> std::result::Result<Value, VenueError> {
        *self.facts.proof_rpc_failures.lock().unwrap() += 1;
        if let Some(raw) = *self.facts.proof_rpc_body.lock().unwrap() {
            let result = super::super::account::runtime_claims::decode_rpc_reply(
                serde_json::from_str(raw).expect("fixture JSON"),
            );
            if let Err(error) = &result {
                self.facts.raw_rpc_reasons.lock().unwrap().push(error.reason.clone());
            }
            return result;
        }
        Err(VenueError::new(VenueErrorCode::ReadUnavailable, method, "RPC_HTTP_429"))
    }

    fn paid_cash(&self) -> U256 {
        self.facts.paid.lock().unwrap().iter().fold(U256::ZERO, |cash, token| {
            let market = self.inner.2.round_of_token(*token).unwrap();
            let results = self.facts.rounds.lock().unwrap();
            let result = &results[&market.market_id];
            let payout = result.payout_yes.expect("cash requires official venue result");
            let rate = if *token == market.yes_token { payout } else { 1.0 - payout };
            cash + self.inner.held(*token) * U256::from((rate * 1_000_000.0) as u64)
                / U256::from(1_000_000)
        })
    }
}

impl SyntheticTransport for R16Transport {
    fn auth_token(&self) -> String { self.inner.auth_token() }
    fn pending_post(&self) -> bool { false }
    fn sign(&self, order: &Order) -> Result<(alloy_primitives::B256, String)> {
        self.inner.sign(order)
    }
    fn rpc(&self, method: &str, params: &Value) -> std::result::Result<Value, VenueError> {
        self.facts.rpc_calls.lock().unwrap().push(method.into());
        if method == "eth_getBlockByNumber" && params[0] == "0x125"
            && *self.facts.fail_census_header.lock().unwrap()
        {
            return self.proof_rpc_failure(method);
        }
        if method == "eth_getTransactionReceipt" && *self.facts.miss_receipt.lock().unwrap() {
            return Err(VenueError::new(VenueErrorCode::ReadUnavailable, "controlled_receipt", "RECEIPT_UNKNOWN"));
        }
        if let Some(script) = self.facts.redemption.lock().unwrap().as_ref() {
            if let Some(value) = script.rpc(self.owner, method, params) {
                return Ok(value);
            }
        }
        if method == "eth_call" {
            let data = params[0]["data"].as_str().unwrap();
            let selector = &data[2..10];
            if selector == hex::encode(&alloy_primitives::keccak256("balanceOf(address)")[..4]) {
                // The public fixture budget is 98, not the base transport's 100.
                let mut cash = U256::from(98) * U256::from(WEI)
                    - self.inner.spent_cash() + self.paid_cash();
                let offset = *self.facts.cash_offset.lock().unwrap();
                if offset >= 0 { cash += U256::from(offset as u128); }
                else { cash -= U256::from(offset.unsigned_abs()); }
                return Ok(json!(format!("0x{cash:064x}")));
            }
            if selector == hex::encode(&alloy_primitives::keccak256("balanceOf(address,uint256)")[..4]) {
                let token = U256::from_str_radix(&data[data.len()-64..], 16).unwrap().to::<u64>();
                self.facts.position_reads.lock().unwrap().push(token);
                if *self.facts.fail_position_balance.lock().unwrap() == Some(token) {
                    return self.proof_rpc_failure(method);
                }
                if let Some(balance) = self.facts.position_overrides.lock().unwrap().get(&token) {
                    return Ok(json!(format!("0x{balance:064x}")));
                }
                if self.facts.paid.lock().unwrap().contains(&token) {
                    return Ok(json!(format!("0x{:064x}", 0)));
                }
            }
        }
        // The legacy in-process stub asserts its fixed test block. Translate
        // only the delegation to that stub, after checking the actual pin here.
        let mut legacy_params = params.clone();
        if let Some(script) = self.facts.redemption.lock().unwrap().as_ref() {
            if matches!(method, "eth_call" | "eth_getBalance") && params[1] != "0x123" {
                let expected = match script.stage { RedemptionStage::Before => "0x124",
                    RedemptionStage::After => "0x126", _ => "0x125" };
                assert_eq!(params[1], expected);
                legacy_params[1] = json!("0x123");
            }
        }
        let mut response = self.inner.rpc(method, &legacy_params)?;
        if method == "eth_getTransactionReceipt" && *self.facts.mixed_fill_receipt.lock().unwrap() {
            let logs = response["logs"].as_array_mut().unwrap();
            let mut transfer = logs[1].clone();
            let owner = transfer["topics"][1].clone();
            let external = json!(format!("0x{:064x}", 123456u64));
            transfer["logIndex"] = json!("0x3");
            transfer["topics"][2] = external.clone();
            logs.push(transfer.clone());
            transfer["logIndex"] = json!("0x4");
            transfer["topics"][1] = external;
            transfer["topics"][2] = owner;
            logs.push(transfer);
        }
        Ok(response)
    }
    fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&Value>)
        -> Result<String> {
        self.facts.rest_calls.lock().unwrap().push(path.into());
        if let Some(id) = path.strip_prefix("/markets/").and_then(|text| text.parse::<u64>().ok()) {
            match *self.facts.market_fault.lock().unwrap() {
                Some(fault @ ("http401" | "http429" | "http503")) => {
                    let status = fault.strip_prefix("http").unwrap().parse::<u16>().unwrap();
                    return Err(super::super::ApiError {
                        status: reqwest::StatusCode::from_u16(status).unwrap(),
                        body: "SECRET_SENTINEL bearer=do-not-log".into(),
                    }.into());
                }
                Some("transport") => {
                    // Construct a real reqwest error locally; no socket is opened.
                    return Err(reqwest::Client::new().get("http://[SECRET_SENTINEL")
                        .build().unwrap_err().into());
                }
                Some("schema") => return Ok(r#"{"data":{"id":"SECRET_SENTINEL"}}"#.into()),
                Some("raw") => anyhow::bail!("SECRET_SENTINEL unclassified response"),
                None => {}
                Some(fault) if fault.starts_with("category_") || matches!(fault, "proof_route" | "positions_http429") => {}
                Some(_) => panic!("unknown settlement fixture fault"),
            }
            let results = self.facts.rounds.lock().unwrap();
            let result = results.get(&id).expect("configure real market facts before account reads");
            let round = self.inner.2.round_by_id(id).unwrap();
            let mut raw = self.inner.raw_market(round);
            raw["question"] = json!(result.question);
            if *self.facts.market_fault.lock().unwrap() == Some("proof_route") { raw["isNegRisk"] = json!(true); }
            raw["categorySlug"] = json!(format!("r16-{id}"));
            raw["marketVariant"] = json!("CRYPTO_UP_DOWN");
            raw["decimalPrecision"] = json!(2);
            raw["status"] = json!(if result.payout_yes.is_some() { "RESOLVED" } else { "REGISTERED" });
            raw["tradingStatus"] = json!(if result.payout_yes.is_some() || result.round_facts.as_ref()
                .and_then(|facts| facts.trading_status.as_deref()) == Some("CLOSED") { "CLOSED" } else { "OPEN" });
            for (index, name) in [(0, "Up"), (1, "Down")] {
                raw["outcomes"][index]["name"] = json!(name);
                if let Some(payout) = result.payout_yes {
                    raw["outcomes"][index]["status"] = json!(if payout == 0.5 { "DRAW" }
                        else if (index == 0) == (payout == 1.0) { "WON" } else { "LOST" });
                }
            }
            return Ok(json!({"data":raw}).to_string());
        }
        if let Some(id) = path.strip_prefix("/categories/r16-").and_then(|text| text.parse::<u64>().ok()) {
            match *self.facts.market_fault.lock().unwrap() {
                Some(fault @ ("category_http429" | "category_http503")) => {
                    return Err(super::super::ApiError {
                        status: reqwest::StatusCode::from_u16(if fault.ends_with("429") { 429 } else { 503 }).unwrap(),
                        body: "SECRET_SENTINEL category-only failure".into(),
                    }.into());
                }
                Some("category_transport") => return Err(reqwest::Client::new()
                    .get("http://[SECRET_SENTINEL").build().unwrap_err().into()),
                Some("category_schema") => return Ok(r#"{"data":{"variantData":{"startPrice":"SECRET_SENTINEL"}}}"#.into()),
                _ => {}
            }
            let results = self.facts.rounds.lock().unwrap();
            let result = &results[&id];
            return Ok(json!({"data":{"startsAt":result.starts_at,"endsAt":result.ends_at,
                "variantData":{"startPrice":result.start_price,"endPrice":if *self.facts.market_fault.lock().unwrap() == Some("category_missing") { None } else { result.end_price },
                    "priceFeedSymbol":"BTCUSDT","priceFeedProvider":"CHAINLINK"},"markets":[]}}).to_string());
        }
        if path.starts_with("/positions?") && *self.facts.market_fault.lock().unwrap() == Some("positions_http429") {
            return Err(super::super::ApiError {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS, body: "synthetic positions rate limit".into(),
            }.into());
        }
        let raw = self.inner.rest(method, path, bearer, body)?;
        if path.starts_with("/positions?") {
            let paid = self.facts.paid.lock().unwrap();
            let hidden = self.facts.hidden.lock().unwrap();
            let stale = self.facts.stale_positions.lock().unwrap();
            let mut response: Value = serde_json::from_str(&raw)?;
            let rows = response["data"].as_array_mut().unwrap();
            rows.retain(|position| {
                let token = position["outcome"]["onChainId"].as_str().unwrap().parse::<u64>().unwrap();
                (!paid.contains(&token) || stale.contains(&token)) && !hidden.contains(&token)
            });
            if let Some(reverse) = *self.facts.position_order_reversed.lock().unwrap() {
                rows.sort_by(|a, b| a["outcome"]["onChainId"].as_str().unwrap().parse::<u64>().unwrap()
                    .cmp(&b["outcome"]["onChainId"].as_str().unwrap().parse::<u64>().unwrap()));
                if reverse { rows.reverse(); }
            }
            return Ok(response.to_string());
        }
        Ok(raw)
    }
}

// Account history stays on the original history parser; only new controlled
// order/book/signing facts use the existing controlled transport.
struct TimingHistory {
    history: Arc<dyn SyntheticTransport>,
    send: BookTransport,
    declared: [String; 2],
}
impl SyntheticTransport for TimingHistory {
    fn auth_token(&self) -> String { self.history.auth_token() }
    fn sign(&self, order: &Order) -> Result<(alloy_primitives::B256, String)> {
        self.send.sign(order)
    }
    fn rpc(&self, method: &str, params: &Value) -> std::result::Result<Value, VenueError> {
        self.history.rpc(method, params)
    }
    fn rest(&self, method: &reqwest::Method, path: &str, bearer: Option<&str>, body: Option<&Value>)
        -> Result<String> {
        let new_order_read = (path.starts_with("/orders/") && !path.starts_with("/orders/matches?"))
            || path.starts_with("/orders/matches?orderHashes=");
        if path.ends_with("/orderbook") || *method == reqwest::Method::POST
            || (new_order_read && !self.declared.iter().any(|hash| path.contains(hash))) {
            self.send.rest(method, path, bearer, body)
        } else { self.history.rest(method, path, bearer, body) }
    }
}

pub(crate) async fn timing_trader() -> (super::super::PredictFunTrader,
    Arc<super::super::account::tests::SyntheticRpc>) {
    let (mut trader, history, declared) = super::super::fills::tests::historical_trader("history_duplicate").await;
    let (_, base, trace) = synthetic_controlled_trader_with_trace(0, "controlled_fill");
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(Arc::new(TimingHistory {
        history, send: BookTransport(base.clone(), "controlled_fill", trace), declared,
    }));
    (trader, base)
}

pub(crate) fn cold_history(trader: &super::super::PredictFunTrader) {
    trader.history_proven.write().clear();
    trader.client.markets.write().clear();
}

pub(crate) fn history_memo_count(trader: &super::super::PredictFunTrader) -> usize {
    trader.history_proven.read().len()
}

pub(crate) fn data(trader: &super::super::PredictFunTrader) -> Arc<super::super::PredictFunClient> {
    trader.client.clone()
}

pub(crate) fn trader() -> (super::super::PredictFunTrader,
    Arc<super::super::account::tests::SyntheticRpc>, Arc<ControlledSyntheticTrace>, Arc<Facts>) {
    trader_with_r22_incident(false)
}

pub(crate) fn trader_with_r22_incident(incident: bool) -> (super::super::PredictFunTrader,
    Arc<super::super::account::tests::SyntheticRpc>, Arc<ControlledSyntheticTrace>, Arc<Facts>) {
    let (mut trader, base, trace) = synthetic_controlled_two_round_trader(200);
    let fill = if incident { "r22_incident_fill" } else { "controlled_fill" };
    let facts = Arc::new(Facts::default());
    let owner = trader.trading_address();
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(Arc::new(R16Transport {
        inner: BookTransport(base.clone(), fill, trace.clone()), facts: facts.clone(), owner,
    }));
    (trader, base, trace, facts)
}
