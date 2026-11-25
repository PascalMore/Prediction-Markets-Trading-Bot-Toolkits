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
    pub position_overrides: Mutex<HashMap<u64, U256>>,
    pub cash_offset: Mutex<i128>,
    pub rpc_calls: Mutex<Vec<String>>,
    pub rest_calls: Mutex<Vec<String>>,
    /// Fault only the public official-settlement GET, never signing or POST.
    pub market_fault: Mutex<Option<&'static str>>,
}

impl Facts {
    pub fn round(&self, market: TradableMarket) {
        self.rounds.lock().unwrap().insert(market.id, market);
    }
    pub fn pay(&self, token: u64) { self.paid.lock().unwrap().insert(token); }
}

struct R16Transport { inner: BookTransport, facts: Arc<Facts> }

impl R16Transport {
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
                if let Some(balance) = self.facts.position_overrides.lock().unwrap().get(&token) {
                    return Ok(json!(format!("0x{balance:064x}")));
                }
                if self.facts.paid.lock().unwrap().contains(&token) {
                    return Ok(json!(format!("0x{:064x}", 0)));
                }
            }
        }
        self.inner.rpc(method, params)
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
                Some(fault) if fault.starts_with("category_") => {}
                Some(_) => panic!("unknown settlement fixture fault"),
            }
            let results = self.facts.rounds.lock().unwrap();
            let result = results.get(&id).expect("configure real market facts before account reads");
            let round = self.inner.2.round_by_id(id).unwrap();
            let mut raw = self.inner.raw_market(round);
            raw["question"] = json!(result.question);
            raw["categorySlug"] = json!(format!("r16-{id}"));
            raw["marketVariant"] = json!("CRYPTO_UP_DOWN");
            raw["decimalPrecision"] = json!(2);
            raw["status"] = json!(if result.payout_yes.is_some() { "RESOLVED" } else { "REGISTERED" });
            raw["tradingStatus"] = json!(if result.payout_yes.is_some() { "CLOSED" } else { "OPEN" });
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
        let raw = self.inner.rest(method, path, bearer, body)?;
        if path.starts_with("/positions?") {
            let paid = self.facts.paid.lock().unwrap();
            let hidden = self.facts.hidden.lock().unwrap();
            let mut response: Value = serde_json::from_str(&raw)?;
            response["data"].as_array_mut().unwrap().retain(|position| {
                let token = position["outcome"]["onChainId"].as_str().unwrap().parse::<u64>().unwrap();
                !paid.contains(&token) && !hidden.contains(&token)
            });
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
    let (mut trader, base, trace) = synthetic_controlled_two_round_trader(200);
    let facts = Arc::new(Facts::default());
    Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(Arc::new(R16Transport {
        inner: BookTransport(base.clone(), "controlled_fill", trace.clone()), facts: facts.clone(),
    }));
    (trader, base, trace, facts)
}
