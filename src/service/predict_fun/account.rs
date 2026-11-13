//! The account gate: what may and may not happen on one account.
//!
//! One account, one venue, one set of limits. When an order's fate is unknown,
//! or a required read failed, or the account's identity doesn't match what the
//! operator declared, every new order on that account stops. Queries, lookups
//! and removals keep working: they can't add risk.
//!
//! The gate holds no second set of balances. It only knows which orders are
//! open, which reads failed, and whether a human confirmed this run's budget.

use super::{ApiMarket, PredictFunTrader, ECDSA_VALIDATOR};
use crate::service::venue::{
    AccountIdentity, AccountMode, AccountSnapshot, AttemptPhase, VenueError, VenueErrorCode,
};
use alloy_primitives::{address, Address, Bytes, B256, U256};
use alloy_sol_types::{sol, SolCall, SolEvent};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// In-memory synthetic test boundary; no socket, real signing or credentials.
#[cfg(test)]
pub(crate) trait SyntheticTransport: Send + Sync {
    fn pending_post(&self) -> bool {
        false
    }
    fn auth_token(&self) -> String {
        panic!("unexpected synthetic auth");
    }
    fn rpc(&self, _method: &str, _params: &Value) -> Result<Value, VenueError> {
        panic!("unexpected synthetic RPC");
    }
    fn rest(
        &self,
        _method: &reqwest::Method,
        _path: &str,
        _bearer: Option<&str>,
        _body: Option<&Value>,
    ) -> anyhow::Result<String> {
        panic!("unexpected synthetic REST");
    }
    fn sign(&self, _order: &super::Order) -> anyhow::Result<(B256, String)> {
        panic!("unexpected synthetic signing");
    }
}

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
    event OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee);
    interface TokenRead {
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function decimals() external view returns (uint8);
    }
    interface CtfRead {
        function balanceOf(address owner, uint256 id) external view returns (uint256);
        function isApprovedForAll(address owner, address operator) external view returns (bool);
    }
    interface ValidatorRead {
        function ecdsaValidatorStorage(address smartAccount) external view returns (address);
        function isInitialized(address smartAccount) external view returns (bool);
    }
    interface AccountRead {
        function isValidSignature(bytes32 hash, bytes signature) external view returns (bytes4);
    }
    interface ExchangeRead {
        function getCollateral() external view returns (address);
        function getCtf() external view returns (address);
        function registry(uint256 token) external view returns (uint256 complement, bytes32 conditionId);
        function getOrderStatus(bytes32 hash) external view returns (bool isFilledOrCancelled, uint256 remaining);
        function isValidNonce(address maker, uint256 nonce) external view returns (bool);
    }
    interface AdapterRead {
        function col() external view returns (address);
        function wcol() external view returns (address);
        function ctf() external view returns (address);
    }
}

/// The public deployment table frozen by DESIGN-07-002 §3.3.5.
pub(super) fn assets(
    chain: u64,
    neg: bool,
    yield_bearing: bool,
) -> (Address, Address, Option<Address>) {
    match (chain, neg, yield_bearing) {
        (56, false, false) => (
            address!("55d398326f99059fF775485246999027B3197955"),
            address!("22DA1810B194ca018378464a58f6Ac2B10C9d244"),
            None,
        ),
        (56, true, false) => (
            address!("55d398326f99059fF775485246999027B3197955"),
            address!("22DA1810B194ca018378464a58f6Ac2B10C9d244"),
            Some(address!("c3Cf7c252f65E0d8D88537dF96569AE94a7F1A6E")),
        ),
        (56, false, true) => (
            address!("55d398326f99059fF775485246999027B3197955"),
            address!("9400F8Ad57e9e0F352345935d6D3175975eb1d9F"),
            None,
        ),
        (56, true, true) => (
            address!("55d398326f99059fF775485246999027B3197955"),
            address!("F64b0b318AAf83BD9071110af24D24445719A07F"),
            Some(address!("41dCe1A4B8FB5e6327701750aF6231B7CD0B2A40")),
        ),
        (97, false, false) => (
            address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
            address!("2827AAef52D71910E8FBad2FfeBC1B6C2DA37743"),
            None,
        ),
        (97, true, false) => (
            address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
            address!("2827AAef52D71910E8FBad2FfeBC1B6C2DA37743"),
            Some(address!("285c1B939380B130D7EBd09467b93faD4BA623Ed")),
        ),
        (97, false, true) => (
            address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
            address!("38BF1cbD66d174bb5F3037d7068E708861D68D7f"),
            None,
        ),
        (97, true, true) => (
            address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
            address!("26e865CbaAe99b62fbF9D18B55c25B5E079A93D5"),
            Some(address!("b74aea04bdeBE912Aa425bC9173F9668e6f11F99")),
        ),
        _ => unreachable!("deployment only supports chain 56/97"),
    }
}

fn read_error(reason: &str) -> VenueError {
    VenueError::new(VenueErrorCode::ReadUnavailable, "rpc", reason)
}

fn mismatch(reason: &str) -> VenueError {
    VenueError::new(VenueErrorCode::AccountMismatch, "rpc", reason)
}

fn rpc_quantity(value: &Value) -> Result<U256, VenueError> {
    let raw = value.as_str().ok_or_else(|| read_error("INVALID_HEX"))?;
    let digits = raw
        .strip_prefix("0x")
        .filter(|v| !v.is_empty() && v.len() <= 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| read_error("INVALID_HEX"))?;
    U256::from_str_radix(digits, 16).map_err(|_| read_error("INVALID_HEX"))
}

/// Fixed-block, read-only access using the original client's timeout and pool.
pub(super) struct ChainRead<'a> {
    trader: &'a PredictFunTrader,
    pub block: String,
    pub timestamp: U256,
}

impl PredictFunTrader {
    /// No REST authentication headers or raw errors cross this RPC boundary.
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, VenueError> {
        if !matches!(
            method,
            "eth_chainId"
                | "eth_getBalance"
                | "eth_call"
                | "eth_getTransactionReceipt"
                | "eth_getBlockByNumber"
        ) {
            return Err(read_error("RPC_METHOD_NOT_ALLOWED"));
        }
        let url = {
            self.run_context
                .read()
                .as_ref()
                .and_then(|c| c.rpc_url.clone())
                .ok_or_else(|| read_error("MISSING_READ_CONTEXT"))?
        };
        #[cfg(test)]
        if let Some(transport) = &self.client.synthetic {
            return transport.rpc(method, &params);
        }
        let response = self
            .client
            .http
            .post(url.0)
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
            .send()
            .await
            .map_err(|_| read_error("RPC_TRANSPORT"))?;
        if !response.status().is_success() {
            return Err(read_error("RPC_HTTP"));
        }
        let body: Value = response.json().await.map_err(|_| read_error("RPC_JSON"))?;
        if body.get("error").is_some()
            || body.get("id") != Some(&json!(1))
            || body.get("jsonrpc") != Some(&json!("2.0"))
        {
            return Err(read_error("RPC_RESPONSE"));
        }
        body.get("result")
            .filter(|value| !value.is_null())
            .cloned()
            .ok_or_else(|| read_error("RPC_RESULT_MISSING"))
    }

    pub(super) async fn chain_read(&self) -> Result<ChainRead<'_>, VenueError> {
        let chain = rpc_quantity(&self.rpc("eth_chainId", json!([])).await?)?;
        if chain != U256::from(self.identity().chain_id) {
            return Err(mismatch("CHAIN_MISMATCH"));
        }
        let block = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let number = rpc_quantity(&block["number"])?;
        let timestamp = rpc_quantity(&block["timestamp"])?;
        let _: B256 = block["hash"]
            .as_str()
            .ok_or_else(|| read_error("BLOCK_HASH"))?
            .parse()
            .map_err(|_| read_error("BLOCK_HASH"))?;
        Ok(ChainRead {
            trader: self,
            block: format!("{number:#x}"),
            timestamp,
        })
    }
}

impl ChainRead<'_> {
    async fn call<C: SolCall>(&self, to: Address, call: C) -> Result<C::Return, VenueError> {
        let result = self.trader.rpc("eth_call", json!([{"to":to, "data":format!("0x{}", hex::encode(call.abi_encode()))}, self.block])).await?;
        let raw = result
            .as_str()
            .and_then(|s| s.strip_prefix("0x"))
            .ok_or_else(|| read_error("ABI_RESULT"))?;
        let bytes = hex::decode(raw).map_err(|_| read_error("ABI_RESULT"))?;
        C::abi_decode_returns(&bytes, true).map_err(|_| read_error("ABI_RESULT"))
    }

    pub(super) async fn balances(&self) -> Result<(U256, U256), VenueError> {
        let identity = self.trader.identity();
        let owner: Address = identity
            .trading_address
            .parse()
            .map_err(|_| mismatch("ADDRESS_MISMATCH"))?;
        let signer: Address = identity
            .signer_address
            .parse()
            .map_err(|_| mismatch("SIGNER_MISMATCH"))?;
        if identity.mode == AccountMode::Eoa && owner != signer {
            return Err(mismatch("SIGNER_MISMATCH"));
        }
        if identity.mode == AccountMode::PredictAccount {
            if !self
                .call(
                    ECDSA_VALIDATOR,
                    ValidatorRead::isInitializedCall {
                        smartAccount: owner,
                    },
                )
                .await?
                ._0
            {
                return Err(mismatch("ACCOUNT_NOT_INITIALIZED"));
            }
            let observed = self
                .call(
                    ECDSA_VALIDATOR,
                    ValidatorRead::ecdsaValidatorStorageCall {
                        smartAccount: owner,
                    },
                )
                .await?
                ._0;
            if observed == Address::ZERO || observed != signer {
                return Err(mismatch("OWNER_MISMATCH"));
            }
        }
        let (usdt, _, _) = assets(identity.chain_id, false, false);
        if self.call(usdt, TokenRead::decimalsCall {}).await?._0 != 18 {
            return Err(read_error("COLLATERAL_DECIMALS"));
        }
        let collateral = self
            .call(usdt, TokenRead::balanceOfCall { owner })
            .await?
            ._0;
        // Supported direct SDK route: Privy pays its account-operation gas.
        let gas = rpc_quantity(
            &self
                .trader
                .rpc("eth_getBalance", json!([signer, self.block]))
                .await?,
        )?;
        Ok((collateral, gas))
    }
}

impl PredictFunTrader {
    /// Complete account read; any failed prerequisite blocks subsequent orders.
    pub(super) async fn read_account_snapshot(&self) -> anyhow::Result<AccountSnapshot> {
        use super::{ApiAccount, ApiOrderData};
        use reqwest::Method;
        let identity = self.identity();
        let result: anyhow::Result<AccountSnapshot> = async {
            let positions = self.read_positions().await?;
            let mut open_orders = Vec::new();
            let mut cursor: Option<String> = None;
            let mut seen = std::collections::HashSet::new();
            let mut complete = false;
            for _ in 0..super::fills::MAX_PAGES {
                let mut url = url::Url::parse("https://synthetic.invalid/orders").unwrap();
                {
                    let mut query = url.query_pairs_mut();
                    query
                        .append_pair("status", "OPEN")
                        .append_pair("first", "100");
                    if let Some(cursor) = &cursor {
                        query.append_pair("after", cursor);
                    }
                }
                let page = self
                    .authed::<Vec<ApiOrderData>>(
                        Method::GET,
                        &format!("{}?{}", url.path(), url.query().unwrap()),
                        None,
                    )
                    .await?;
                for order in page.data {
                    if open_orders.contains(&order.id) {
                        anyhow::bail!("FACT_INCOMPLETE:DUPLICATE_ORDER");
                    }
                    open_orders.push(order.id);
                }
                match page.cursor.filter(|cursor| !cursor.is_empty()) {
                    None => {
                        complete = true;
                        break;
                    }
                    Some(next) => {
                        if !seen.insert(next.clone()) {
                            anyhow::bail!("READ_UNAVAILABLE:PAGINATION_LOOP");
                        }
                        cursor = Some(next);
                    }
                }
            }
            if !complete {
                anyhow::bail!("READ_UNAVAILABLE:PAGE_CAP");
            }
            let observed = self
                .authed::<ApiAccount>(Method::GET, "/account", None)
                .await?
                .data
                .address;
            check_identity(&observed, &identity)?;
            let chain = self.chain_read().await?;
            let history = self.reconcile_account_history().await?;
            let observed: HashMap<_, _> = positions
                .iter()
                .filter(|(_, amount)| !amount.is_zero())
                .cloned()
                .collect();
            let positions_explained = history == observed;
            let (collateral_atoms, gas_atoms) = chain.balances().await?;
            for (token, amount) in &positions {
                let market = self.client.market_for_token(token)?;
                chain.position(&market, token, *amount).await?;
            }
            Ok(AccountSnapshot {
                identity: identity.clone(),
                collateral_atoms: Some(collateral_atoms),
                gas_atoms: Some(gas_atoms),
                positions,
                positions_explained,
                open_orders,
                complete: true,
            })
        }
        .await;
        match &result {
            Ok(_) => self.gate.record_read_success(&identity, "snapshot"),
            Err(error) => {
                if let Some(error) = error
                    .downcast_ref::<VenueError>()
                    .filter(|error| error.code == VenueErrorCode::AccountMismatch)
                {
                    self.gate.record_mismatch(&identity, &error.reason);
                } else {
                    self.gate
                        .record_read_failure(&identity, "snapshot", "READ_FAILED");
                }
            }
        }
        result
    }

    /// Read all position pages; duplicate tokens and cursor loops are ambiguous.
    pub(super) async fn read_positions(&self) -> anyhow::Result<Vec<(String, U256)>> {
        let mut cursor: Option<String> = None;
        let mut seen = std::collections::HashSet::new();
        let mut out: Vec<(String, U256)> = Vec::new();
        for _ in 0..super::fills::MAX_PAGES {
            let mut url = url::Url::parse("https://synthetic.invalid/positions").unwrap();
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("first", "100");
                if let Some(cursor) = &cursor {
                    query.append_pair("after", cursor);
                }
            }
            let page = self
                .authed::<Vec<super::ApiHeldPosition>>(
                    reqwest::Method::GET,
                    &format!("{}?{}", url.path(), url.query().unwrap()),
                    None,
                )
                .await?;
            for position in page.data {
                self.client.remember_market(&position.market);
                if out
                    .iter()
                    .any(|(token, _)| token == &position.outcome.on_chain_id)
                {
                    anyhow::bail!("FACT_INCOMPLETE:DUPLICATE_POSITION");
                }
                out.push((
                    position.outcome.on_chain_id,
                    super::decimal_to_wei_exact(&position.amount)?,
                ));
            }
            match page.cursor.filter(|cursor| !cursor.is_empty()) {
                None => return Ok(out),
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        anyhow::bail!("READ_UNAVAILABLE:PAGINATION_LOOP");
                    }
                    cursor = Some(next);
                }
            }
        }
        anyhow::bail!("READ_UNAVAILABLE:PAGE_CAP")
    }

    /// Require complete account history and actually re-read every declared hash.
    /// A declaration never makes an executable or incompletely accounted order safe.
    pub(super) async fn reconcile_account_history(&self) -> anyhow::Result<HashMap<String, U256>> {
        use super::{decimal_to_wei_exact, ApiMatch, ApiOrderData};
        use crate::models::Side;
        use crate::service::venue::{AttemptRecord, OrderIdentity, Submission};
        let identity = self.identity();
        let declared = self
            .run_context
            .read()
            .as_ref()
            .map(|context| context.reconciled_orders.clone())
            .unwrap_or_default();
        let mut hashes = std::collections::HashSet::new();
        let mut cursor: Option<String> = None;
        let mut cursors = std::collections::HashSet::new();
        let mut fingerprints = std::collections::HashSet::new();
        let mut complete = false;
        for _ in 0..super::fills::MAX_PAGES {
            let mut url = url::Url::parse("https://synthetic.invalid/orders/matches").unwrap();
            {
                let mut query = url.query_pairs_mut();
                query
                    .append_pair("first", "100")
                    .append_pair("signerAddress", &identity.trading_address);
                if let Some(cursor) = &cursor {
                    query.append_pair("after", cursor);
                }
            }
            let path = format!("{}?{}", url.path(), url.query().unwrap());
            let page = self
                .authed::<Vec<ApiMatch>>(reqwest::Method::GET, &path, None)
                .await?;
            for event in page.data {
                if !fingerprints.insert(format!("{event:?}")) {
                    anyhow::bail!("FACT_INCOMPLETE:AMBIGUOUS_MATCH_DUPLICATE");
                }
                self.client.remember_market(&event.market);
                for leg in std::iter::once(&event.taker).chain(event.makers.iter()) {
                    if leg.signer.eq_ignore_ascii_case(&identity.trading_address) {
                        validate_order_hash(&leg.hash)?;
                        hashes.insert(leg.hash.clone());
                    }
                }
            }
            match page.cursor.filter(|c| !c.is_empty()) {
                None => {
                    complete = true;
                    break;
                }
                Some(next) => {
                    if !cursors.insert(next.clone()) {
                        anyhow::bail!("READ_UNAVAILABLE:PAGINATION_LOOP");
                    }
                    cursor = Some(next);
                }
            }
        }
        if !complete {
            anyhow::bail!("READ_UNAVAILABLE:PAGE_CAP");
        }
        hashes.extend(declared.iter().cloned());
        let mut totals: HashMap<String, (U256, U256)> = HashMap::new();
        for hash in hashes {
            // This run's bound orders are reconciled by its owning consumer.
            if self.bound_orders.read().contains_key(&hash) {
                continue;
            }
            if !declared
                .iter()
                .any(|declared| declared.eq_ignore_ascii_case(&hash))
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY");
            }
            let data = self
                .authed::<ApiOrderData>(reqwest::Method::GET, &format!("/orders/{hash}"), None)
                .await?
                .data;
            let contract = data
                .order
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_CONTRACT_ORDER"))?;
            let token = contract
                .token_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_TOKEN"))?;
            let market = self.client.market_for_token(&token)?;
            let side = match contract.side {
                Some(0) => Side::Buy,
                Some(1) => Side::Sell,
                _ => anyhow::bail!("FACT_INCOMPLETE:SIDE"),
            };
            let maker = decimal_to_wei_exact(
                contract
                    .maker_amount
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:AMOUNT"))?,
            )?;
            let taker = decimal_to_wei_exact(
                contract
                    .taker_amount
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:AMOUNT"))?,
            )?;
            let requested = if side == Side::Buy { taker } else { maker };
            let bound = super::fills::BoundOrder {
                handle: Arc::new(parking_lot::Mutex::new(AttemptRecord {
                    identity: OrderIdentity {
                        local_attempt_id: 0,
                        signed_hash: Some(hash.clone()),
                        venue_order_id: Some(data.id.clone()),
                        market_id: data.market_id,
                        token_id: token,
                        side,
                        account: identity.clone(),
                    },
                    request_gross_atoms: requested,
                    r0: maker,
                    reserved_sell: U256::ZERO,
                    phase: AttemptPhase::Submitted,
                    submission: Submission::Unknown,
                    last_reason: None,
                })),
                market,
                requested,
                maker_amount: maker,
                taker_amount: taker,
                expiration: contract
                    .expiration
                    .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:EXPIRATION"))?,
            };
            let snapshot = self.read_order_execution(&hash, &bound).await?;
            if !snapshot.pages_complete
                || !snapshot.accounting_complete()
                || !matches!(
                    snapshot.closure,
                    crate::service::venue::Closure::Final { .. }
                )
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED");
            }
            let (bought, sold) = totals.entry(snapshot.identity.token_id).or_default();
            let total = if side == Side::Buy { bought } else { sold };
            *total = total
                .checked_add(*snapshot.accounting.net_inventory.value().unwrap())
                .ok_or_else(|| anyhow::anyhow!("HISTORICAL_INVENTORY_OVERFLOW"))?;
        }
        totals
            .into_iter()
            .filter(|(_, (bought, sold))| bought != sold)
            .map(|(token, (bought, sold))| {
                Ok((
                    token,
                    bought
                        .checked_sub(sold)
                        .ok_or_else(|| anyhow::anyhow!("HISTORICAL_INVENTORY_CONFLICT"))?,
                ))
            })
            .collect()
    }
}

impl ChainRead<'_> {
    async fn token_identity(&self, market: &ApiMarket, token: U256) -> Result<(), VenueError> {
        let condition = market
            .condition_id
            .filter(|condition| *condition != B256::ZERO)
            .ok_or_else(|| read_error("CONDITION_MISSING"))?;
        let exchange = self
            .trader
            .client
            .deployment
            .exchange(market.is_neg_risk, market.is_yield_bearing);
        let registered = self
            .call(exchange, ExchangeRead::registryCall { token })
            .await?;
        if registered.conditionId != condition
            || registered.complement == token
            || !market
                .outcomes
                .iter()
                .any(|outcome| outcome.on_chain_id == registered.complement.to_string())
        {
            return Err(mismatch("CONDITION_TOKEN_MISMATCH"));
        }
        Ok(())
    }

    pub(super) async fn authorize(
        &self,
        market: &ApiMarket,
        token: U256,
        side: crate::models::Side,
        required: U256,
    ) -> Result<(), VenueError> {
        self.token_identity(market, token).await?;
        let identity = self.trader.identity();
        let owner = self.trader.trading_address();
        let exchange = self
            .trader
            .client
            .deployment
            .exchange(market.is_neg_risk, market.is_yield_bearing);
        let (usdt, ctf, adapter) = assets(
            identity.chain_id,
            market.is_neg_risk,
            market.is_yield_bearing,
        );
        let collateral = self
            .call(exchange, ExchangeRead::getCollateralCall {})
            .await?
            ._0;
        let observed_ctf = self.call(exchange, ExchangeRead::getCtfCall {}).await?._0;
        if let Some(adapter) = adapter {
            if self.call(adapter, AdapterRead::colCall {}).await?._0 != usdt
                || self.call(adapter, AdapterRead::ctfCall {}).await?._0 != ctf
                || (observed_ctf != ctf && observed_ctf != adapter)
            {
                return Err(mismatch("ASSET_ROUTE_MISMATCH"));
            }
            let wrapped = self.call(adapter, AdapterRead::wcolCall {}).await?._0;
            if collateral != usdt && collateral != wrapped {
                return Err(mismatch("ASSET_ROUTE_MISMATCH"));
            }
            // No implicit 1:1 wrapped-collateral accounting.
            if collateral != usdt {
                return Err(read_error("WRAPPED_COLLATERAL_UNPROVEN"));
            }
        } else if collateral != usdt || observed_ctf != ctf {
            return Err(mismatch("ASSET_ROUTE_MISMATCH"));
        }
        match side {
            crate::models::Side::Buy => {
                if self
                    .call(
                        usdt,
                        TokenRead::allowanceCall {
                            owner,
                            spender: exchange,
                        },
                    )
                    .await?
                    ._0
                    < required
                {
                    return Err(read_error("INSUFFICIENT_ALLOWANCE"));
                }
            }
            crate::models::Side::Sell => {
                if self
                    .call(ctf, CtfRead::balanceOfCall { owner, id: token })
                    .await?
                    ._0
                    < required
                {
                    return Err(read_error("INSUFFICIENT_INVENTORY"));
                }
                if !self
                    .call(
                        ctf,
                        CtfRead::isApprovedForAllCall {
                            owner,
                            operator: exchange,
                        },
                    )
                    .await?
                    ._0
                {
                    return Err(read_error("OPERATOR_NOT_APPROVED"));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn closed(
        &self,
        bound: &super::fills::BoundOrder,
        hash: &str,
        status: &str,
        q: U256,
    ) -> Result<bool, VenueError> {
        if status == "FILLED" && q == bound.requested {
            return Ok(true);
        }
        if !matches!(status, "FILLED" | "CANCELLED" | "EXPIRED" | "INVALIDATED") {
            return Ok(false);
        }
        if bound.expiration > 0 && self.timestamp > U256::from(bound.expiration as u64) {
            return Ok(true);
        }
        let hash: B256 = hash.parse().map_err(|_| mismatch("HASH_MISMATCH"))?;
        let exchange = self
            .trader
            .client
            .deployment
            .exchange(bound.market.is_neg_risk, bound.market.is_yield_bearing);
        Ok(self
            .call(exchange, ExchangeRead::getOrderStatusCall { hash })
            .await?
            .isFilledOrCancelled
            || !self
                .call(
                    exchange,
                    ExchangeRead::isValidNonceCall {
                        maker: self.trader.trading_address(),
                        nonce: U256::ZERO,
                    },
                )
                .await?
                ._0)
    }

    /// Associate the exact original order with successful, same-chain receipts.
    /// Ambiguous net transfers never become a complete execution.
    pub(super) async fn settled(
        &self,
        bound: &super::fills::BoundOrder,
        identity: &crate::service::venue::OrderIdentity,
        legs: &[super::fills::OwnLeg],
        accounting: &crate::service::venue::Accounting,
    ) -> Result<bool, VenueError> {
        use super::fills;
        if legs.is_empty() {
            return Ok(true);
        }
        if !accounting.is_complete() {
            return Ok(false);
        }
        let hash: B256 = identity
            .signed_hash
            .as_deref()
            .ok_or_else(|| mismatch("HASH_MISSING"))?
            .parse()
            .map_err(|_| mismatch("HASH_MISMATCH"))?;
        let token =
            U256::from_str_radix(&identity.token_id, 10).map_err(|_| mismatch("TOKEN_MISMATCH"))?;
        self.token_identity(&bound.market, token).await?;
        let owner = self.trader.trading_address();
        let exchange = self
            .trader
            .client
            .deployment
            .exchange(bound.market.is_neg_risk, bound.market.is_yield_bearing);
        let (usdt, ctf, _) = assets(
            identity.account.chain_id,
            bound.market.is_neg_risk,
            bound.market.is_yield_bearing,
        );
        let mut txs = std::collections::HashSet::new();
        for leg in legs {
            txs.insert(
                leg.tx_hash
                    .as_deref()
                    .ok_or_else(|| read_error("SETTLEMENT_TX_MISSING"))?,
            );
        }
        for tx in txs {
            validate_order_hash(tx).map_err(|_| read_error("SETTLEMENT_TX_INVALID"))?;
            let receipt = self
                .trader
                .rpc("eth_getTransactionReceipt", json!([tx]))
                .await?;
            if receipt["transactionHash"].as_str() != Some(tx)
                || rpc_quantity(&receipt["status"])? != U256::from(1)
            {
                return Ok(false);
            }
            let number = rpc_quantity(&receipt["blockNumber"])?;
            if number > rpc_quantity(&json!(self.block))? {
                return Ok(false);
            }
            let block = self
                .trader
                .rpc(
                    "eth_getBlockByNumber",
                    json!([format!("{number:#x}"), false]),
                )
                .await?;
            if block["hash"] != receipt["blockHash"]
                || block["number"] != receipt["blockNumber"]
                || !block["hash"].is_string()
            {
                return Err(read_error("RECEIPT_BLOCK_MISMATCH"));
            }
            let subset: Vec<_> = legs
                .iter()
                .filter(|leg| leg.tx_hash.as_deref() == Some(tx))
                .cloned()
                .collect();
            let facts = fills::cumulative(&subset, U256::ZERO)
                .map_err(|_| read_error("SETTLEMENT_AMOUNT"))?;
            let expected = fills::accounting(
                identity.side,
                facts.gross_q_atoms,
                facts.gross_cash_atoms,
                facts.collateral_fee_atoms,
                facts.share_fee_atoms,
            );
            let mut q = U256::ZERO;
            let mut cash = U256::ZERO;
            let mut event_fee = U256::ZERO;
            let mut cash_in = U256::ZERO;
            let mut cash_out = U256::ZERO;
            let mut inventory_in = U256::ZERO;
            let mut inventory_out = U256::ZERO;
            let mut indices = std::collections::HashSet::new();
            let logs = receipt["logs"]
                .as_array()
                .ok_or_else(|| read_error("RECEIPT_LOGS"))?;
            for log in logs {
                if log["removed"] != json!(false)
                    || log["transactionHash"] != receipt["transactionHash"]
                    || log["blockHash"] != receipt["blockHash"]
                    || log["blockNumber"] != receipt["blockNumber"]
                    || !indices.insert(rpc_quantity(&log["logIndex"])?)
                {
                    return Err(read_error("RECEIPT_LOG_IDENTITY"));
                }
                let emitter: Address = log["address"]
                    .as_str()
                    .ok_or_else(|| read_error("LOG_ADDRESS"))?
                    .parse()
                    .map_err(|_| read_error("LOG_ADDRESS"))?;
                if emitter != exchange && emitter != usdt && emitter != ctf {
                    continue;
                }
                let topics: Vec<B256> = serde_json::from_value(log["topics"].clone())
                    .map_err(|_| read_error("LOG_TOPICS"))?;
                let data = log["data"]
                    .as_str()
                    .and_then(|s| s.strip_prefix("0x"))
                    .and_then(|s| hex::decode(s).ok())
                    .ok_or_else(|| read_error("LOG_DATA"))?;
                if emitter == exchange && topics.first() == Some(&OrderFilled::SIGNATURE_HASH) {
                    let event = OrderFilled::decode_raw_log(topics.iter().copied(), &data, true)
                        .map_err(|_| read_error("ORDER_FILLED_ABI"))?;
                    if event.maker != owner {
                        continue;
                    }
                    if event.orderHash != hash {
                        return Ok(false);
                    }
                    let (maker_asset, taker_asset, gross_q, gross_cash) = match identity.side {
                        crate::models::Side::Buy => (
                            U256::ZERO,
                            token,
                            event.takerAmountFilled,
                            event.makerAmountFilled,
                        ),
                        crate::models::Side::Sell => (
                            token,
                            U256::ZERO,
                            event.makerAmountFilled,
                            event.takerAmountFilled,
                        ),
                    };
                    if event.makerAssetId != maker_asset || event.takerAssetId != taker_asset {
                        return Err(mismatch("EVENT_ASSET_MISMATCH"));
                    }
                    event_fee = event_fee
                        .checked_add(event.fee)
                        .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                    q = q
                        .checked_add(gross_q)
                        .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                    cash = cash
                        .checked_add(gross_cash)
                        .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                } else if emitter == usdt && topics.first() == Some(&Transfer::SIGNATURE_HASH) {
                    let event = Transfer::decode_raw_log(topics.iter().copied(), &data, true)
                        .map_err(|_| read_error("TRANSFER_ABI"))?;
                    if event.from == owner {
                        cash_out = cash_out
                            .checked_add(event.value)
                            .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                    }
                    if event.to == owner {
                        cash_in = cash_in
                            .checked_add(event.value)
                            .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                    }
                } else if emitter == ctf {
                    let (from, to, ids, values) =
                        if topics.first() == Some(&TransferSingle::SIGNATURE_HASH) {
                            let event =
                                TransferSingle::decode_raw_log(topics.iter().copied(), &data, true)
                                    .map_err(|_| read_error("CTF_TRANSFER_ABI"))?;
                            (event.from, event.to, vec![event.id], vec![event.value])
                        } else if topics.first() == Some(&TransferBatch::SIGNATURE_HASH) {
                            let event =
                                TransferBatch::decode_raw_log(topics.iter().copied(), &data, true)
                                    .map_err(|_| read_error("CTF_TRANSFER_ABI"))?;
                            (event.from, event.to, event.ids, event.values)
                        } else {
                            continue;
                        };
                    if ids.len() != values.len() {
                        return Err(read_error("CTF_ARRAY_LENGTH"));
                    }
                    for (id, value) in ids.into_iter().zip(values) {
                        if id != token {
                            continue;
                        }
                        if from == owner {
                            inventory_out = inventory_out
                                .checked_add(value)
                                .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                        }
                        if to == owner {
                            inventory_in = inventory_in
                                .checked_add(value)
                                .ok_or_else(|| read_error("SETTLEMENT_OVERFLOW"))?;
                        }
                    }
                }
            }
            let (net_cash, net_inventory) = match identity.side {
                crate::models::Side::Buy => (
                    cash_out.checked_sub(cash_in),
                    inventory_in.checked_sub(inventory_out),
                ),
                crate::models::Side::Sell => (
                    cash_in.checked_sub(cash_out),
                    inventory_out.checked_sub(inventory_in),
                ),
            };
            let expected_fee = match (facts.collateral_fee_atoms, facts.share_fee_atoms) {
                (Some(collateral), Some(shares)) if collateral.is_zero() || shares.is_zero() => {
                    collateral.checked_add(shares)
                }
                _ => None,
            };
            if Some(event_fee) != expected_fee
                || q != facts.gross_q_atoms
                || cash != facts.gross_cash_atoms
                || net_cash.as_ref() != expected.net_cash.value()
                || net_inventory.as_ref() != expected.net_inventory.value()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) async fn position(
        &self,
        market: &ApiMarket,
        token: &str,
        expected: U256,
    ) -> Result<(), VenueError> {
        let (_, ctf, _) = assets(
            self.trader.identity().chain_id,
            market.is_neg_risk,
            market.is_yield_bearing,
        );
        let id = U256::from_str_radix(token, 10).map_err(|_| mismatch("TOKEN_MISMATCH"))?;
        let actual = self
            .call(
                ctf,
                CtfRead::balanceOfCall {
                    owner: self.trader.trading_address(),
                    id,
                },
            )
            .await?
            ._0;
        if actual != expected {
            return Err(VenueError::new(
                VenueErrorCode::ReconciliationRequired,
                "positions",
                "INVENTORY_MISMATCH",
            ));
        }
        Ok(())
    }

    pub(super) async fn signature(&self, hash: B256, signature: &str) -> Result<(), VenueError> {
        if self.trader.account.is_none() {
            return Ok(());
        }
        let bytes = signature
            .strip_prefix("0x")
            .and_then(|raw| hex::decode(raw).ok())
            .ok_or_else(|| mismatch("SIGNATURE_MISMATCH"))?;
        if bytes.len() != 86 || bytes[0] != 1 || bytes[1..21] != ECDSA_VALIDATOR.as_slice()[..] {
            return Err(mismatch("SIGNATURE_MISMATCH"));
        }
        let magic = self
            .call(
                self.trader.trading_address(),
                AccountRead::isValidSignatureCall {
                    hash,
                    signature: Bytes::from(bytes),
                },
            )
            .await?
            ._0;
        if magic.as_slice() != [0x16, 0x26, 0xba, 0x7e] {
            return Err(mismatch("SIGNATURE_MISMATCH"));
        }
        Ok(())
    }
}

/// Why the account can't take new orders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateReason {
    /// An order may have been submitted and its fate isn't known.
    UnconfirmedSubmission { local_attempt_id: u64 },
    /// A required read failed. The reason is a stable token.
    ReadFailed { step: String, reason: String },
    /// The account isn't the one the operator declared.
    AccountMismatch { reason: String },
    /// The operator hasn't confirmed this run's budget.
    BudgetUnconfirmed,
    /// The account's facts disagree with the local ledger.
    ReconciliationRequired { reason: String },
}

impl GateReason {
    pub fn code(&self) -> VenueErrorCode {
        match self {
            GateReason::UnconfirmedSubmission { .. } => VenueErrorCode::SubmissionUnknown,
            GateReason::ReadFailed { .. } => VenueErrorCode::ReadUnavailable,
            GateReason::AccountMismatch { .. } => VenueErrorCode::AccountMismatch,
            GateReason::BudgetUnconfirmed => VenueErrorCode::BudgetUnconfirmed,
            GateReason::ReconciliationRequired { .. } => VenueErrorCode::ReconciliationRequired,
        }
    }

    /// A stable reason token, safe to log.
    pub fn token(&self) -> String {
        match self {
            GateReason::UnconfirmedSubmission { local_attempt_id } => {
                format!("UNCONFIRMED_SUBMISSION:{local_attempt_id}")
            }
            GateReason::ReadFailed { step, reason } => format!("READ_FAILED:{step}:{reason}"),
            GateReason::AccountMismatch { reason } => format!("ACCOUNT_MISMATCH:{reason}"),
            GateReason::BudgetUnconfirmed => "BUDGET_UNCONFIRMED".into(),
            GateReason::ReconciliationRequired { reason } => {
                format!("RECONCILIATION_REQUIRED:{reason}")
            }
        }
    }
}

/// The account's key: one chain, one address, one mode, one venue.
pub fn account_key(identity: &AccountIdentity) -> String {
    format!(
        "{}:{}:{:?}:{:?}",
        identity.chain_id, identity.trading_address, identity.mode, identity.venue
    )
}

/// One account's state.
#[derive(Debug, Default)]
struct AccountState {
    /// Attempts that may have been submitted and aren't resolved yet.
    unconfirmed: HashMap<u64, String>,
    /// Reads that failed, by step. A successful read clears its own step only.
    failed_reads: HashMap<String, String>,
    /// A mismatch between the declared and the observed account.
    mismatch: Option<String>,
    /// A conflict between the venue's facts and the local ledger.
    reconciliation: Option<String>,
    /// The operator confirmed this run's budget for this account.
    budget_confirmed: bool,
}

/// The gate every order on an account passes through.
#[derive(Debug, Default)]
pub struct AccountGate {
    accounts: Mutex<HashMap<String, AccountState>>,
}

impl AccountGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reserve an attempt. A second attempt on the same account is refused
    /// while an earlier one is unconfirmed.
    ///
    /// `handle` is registered by its local attempt id: re-registering the same
    /// id only moves it forward, it doesn't create a second reservation.
    pub fn reserve(
        &self,
        identity: &AccountIdentity,
        local_attempt_id: u64,
    ) -> Result<(), VenueError> {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.entry(key).or_default();
        if let Some(reason) = blocking_reason(state) {
            // Re-registering our own in-flight handle is idempotent, but may
            // not override a mismatch, failed read or reconciliation conflict.
            if !matches!(reason, GateReason::UnconfirmedSubmission { local_attempt_id: id } if id == local_attempt_id)
                || !state.failed_reads.is_empty()
            {
                return Err(VenueError::new(
                    reason.code(),
                    "account_gate",
                    &reason.token(),
                ));
            }
        }
        if !state.unconfirmed.contains_key(&local_attempt_id) {
            state
                .unconfirmed
                .insert(local_attempt_id, "PREPARED".to_string());
        }
        Ok(())
    }

    /// Record that an attempt may have reached the venue.
    pub fn mark_submitted(&self, identity: &AccountIdentity, local_attempt_id: u64) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.entry(key).or_default();
        state
            .unconfirmed
            .insert(local_attempt_id, "MAY_HAVE_SUBMITTED".into());
    }

    /// Resolve an attempt. Only a proven outcome closes it.
    pub fn resolve(&self, identity: &AccountIdentity, local_attempt_id: u64, phase: AttemptPhase) {
        if phase == AttemptPhase::Closed {
            self.clear_attempt(identity, local_attempt_id);
        }
    }

    /// Drop an attempt that provably never left.
    pub fn release(&self, identity: &AccountIdentity, local_attempt_id: u64) {
        self.clear_attempt(identity, local_attempt_id);
    }

    fn clear_attempt(&self, identity: &AccountIdentity, local_attempt_id: u64) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        if let Some(state) = accounts.get_mut(&key) {
            state.unconfirmed.remove(&local_attempt_id);
        }
    }

    /// Record that a required read failed.
    pub fn record_read_failure(&self, identity: &AccountIdentity, step: &str, reason: &str) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.entry(key).or_default();
        state
            .failed_reads
            .insert(step.to_string(), reason.to_string());
    }

    /// Record that a required read succeeded. It clears only that step.
    pub fn record_read_success(&self, identity: &AccountIdentity, step: &str) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        if let Some(state) = accounts.get_mut(&key) {
            state.failed_reads.remove(step);
        }
    }

    /// Record that the account isn't the one the operator declared.
    pub fn record_mismatch(&self, identity: &AccountIdentity, reason: &str) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.entry(key).or_default();
        state.mismatch = Some(reason.to_string());
    }

    /// Record a conflict between the venue's facts and the local ledger.
    pub fn record_reconciliation(&self, identity: &AccountIdentity, reason: &str) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.entry(key).or_default();
        state.reconciliation = Some(reason.to_string());
    }

    /// Whether new orders on this account are allowed right now.
    pub fn allows_new_orders(&self, identity: &AccountIdentity) -> bool {
        let key = account_key(identity);
        let accounts = self.accounts.lock().unwrap();
        accounts
            .get(&key)
            .map(|state| blocking_reason(state).is_none())
            .unwrap_or(true)
    }

    /// Why new orders are refused, if they are.
    pub fn block_reason(&self, identity: &AccountIdentity) -> Option<GateReason> {
        let key = account_key(identity);
        let accounts = self.accounts.lock().unwrap();
        accounts.get(&key).and_then(blocking_reason)
    }

    /// The attempts that may have been submitted and aren't resolved.
    pub fn unconfirmed_attempts(&self, identity: &AccountIdentity) -> Vec<u64> {
        let key = account_key(identity);
        let accounts = self.accounts.lock().unwrap();
        let mut ids: Vec<u64> = accounts
            .get(&key)
            .map(|state| state.unconfirmed.keys().copied().collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }

    /// Record the operator's confirmation of this run's budget.
    pub fn confirm_budget(&self, identity: &AccountIdentity) {
        let key = account_key(identity);
        let mut accounts = self.accounts.lock().unwrap();
        accounts.entry(key).or_default().budget_confirmed = true;
    }

    /// Whether the operator confirmed this run's budget.
    pub fn budget_confirmed(&self, identity: &AccountIdentity) -> bool {
        let key = account_key(identity);
        let accounts = self.accounts.lock().unwrap();
        accounts
            .get(&key)
            .map(|state| state.budget_confirmed)
            .unwrap_or(false)
    }
}

/// The first reason that blocks the account, in the order the contract fixes:
/// a mismatch outranks a reconciliation conflict, which outranks an unknown
/// submission, which outranks a failed read.
fn blocking_reason(state: &AccountState) -> Option<GateReason> {
    if let Some(reason) = state.mismatch.as_ref() {
        return Some(GateReason::AccountMismatch {
            reason: reason.clone(),
        });
    }
    if let Some(reason) = state.reconciliation.as_ref() {
        return Some(GateReason::ReconciliationRequired {
            reason: reason.clone(),
        });
    }
    if let Some((&attempt_id, _)) = state.unconfirmed.iter().next() {
        return Some(GateReason::UnconfirmedSubmission {
            local_attempt_id: attempt_id,
        });
    }
    if let Some((step, reason)) = state.failed_reads.iter().next() {
        return Some(GateReason::ReadFailed {
            step: step.clone(),
            reason: reason.clone(),
        });
    }
    None
}

/// One page of a paginated read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

/// What went wrong while reading pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    /// The same cursor came back twice.
    CursorLoop,
    /// The page count limit was reached with more to read.
    PageCap,
    /// A page couldn't be read.
    ReadFailed(String),
}

impl PageError {
    pub fn token(&self) -> String {
        match self {
            PageError::CursorLoop => "PAGINATION_LOOP".into(),
            PageError::PageCap => "PAGE_CAP".into(),
            PageError::ReadFailed(reason) => format!("READ_FAILED:{reason}"),
        }
    }
}

/// Read every page of a paginated endpoint.
///
/// The scan ends when the endpoint stops naming a cursor. A repeated cursor, a
/// repeated page, or the page cap with more to read are all failures: a
/// truncated read is not a complete one, and a partial read must not look like
/// a zero balance or an empty position list.
pub async fn collect_pages<T, F, Fut>(mut fetch: F, max_pages: u32) -> Result<Vec<T>, PageError>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<Page<T>, PageError>>,
{
    let mut all: Vec<T> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors: Vec<String> = Vec::new();

    for _ in 0..max_pages {
        let page = fetch(cursor.clone()).await?;
        if let Some(next) = page.cursor.as_ref() {
            if seen_cursors.iter().any(|c| c == next) {
                return Err(PageError::CursorLoop);
            }
            seen_cursors.push(next.clone());
        }
        all.extend(page.items);
        match page.cursor {
            None => return Ok(all),
            Some(next) => cursor = Some(next),
        }
    }
    Err(PageError::PageCap)
}

/// A human's confirmation of this run's budget for one account.
///
/// It is not an authorisation to override a health check, and it carries no
/// amount: it says the operator looked at this account's risk and accepts the
/// budget already configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunConfirmation {
    pub chain_id: u64,
    pub address: String,
    pub mode: AccountMode,
    /// Order hashes the operator says they explained, one by one.
    pub reconciled_orders: Vec<String>,
}

/// Parse a `--predict-budget-confirmation` value: `chain:address:mode`.
pub fn parse_confirmation(raw: &str) -> Result<RunConfirmation, VenueError> {
    let mut parts = raw.split(':');
    let chain_id = parts
        .next()
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| VenueError::new(VenueErrorCode::InvalidInput, "budget", "BAD_CHAIN_ID"))?;
    let address = parts
        .next()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| VenueError::new(VenueErrorCode::InvalidInput, "budget", "BAD_ADDRESS"))?
        .to_string();
    let mode = match parts.next() {
        Some("eoa") => AccountMode::Eoa,
        Some("predict-account") => AccountMode::PredictAccount,
        _ => {
            return Err(VenueError::new(
                VenueErrorCode::InvalidInput,
                "budget",
                "BAD_ACCOUNT_MODE",
            ))
        }
    };
    if parts.next().is_some() {
        return Err(VenueError::new(
            VenueErrorCode::InvalidInput,
            "budget",
            "TOO_MANY_FIELDS",
        ));
    }
    Ok(RunConfirmation {
        chain_id,
        address,
        mode,
        reconciled_orders: Vec::new(),
    })
}

/// An endpoint may contain credentials in any URL component. Never format it.
#[derive(Clone)]
pub struct RedactedRpcUrl(url::Url);

impl std::fmt::Debug for RedactedRpcUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED_RPC_URL]")
    }
}

impl std::fmt::Display for RedactedRpcUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED_RPC_URL]")
    }
}

/// Parse before credentials are loaded, without returning a URL-bearing cause.
pub fn parse_rpc_url(raw: &str) -> Result<RedactedRpcUrl, VenueError> {
    let invalid = || {
        VenueError::new(
            VenueErrorCode::InvalidInput,
            "rpc_input",
            "PREDICT_RPC_URL_INVALID",
        )
    };
    if raw.is_empty() || raw.chars().any(char::is_whitespace) {
        return Err(invalid());
    }
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(RedactedRpcUrl(url))
}

/// Validate each declaration independently, even without a budget declaration.
pub fn validate_order_hash(raw: &str) -> Result<(), VenueError> {
    if raw.len() != 66 || !raw.starts_with("0x") || !raw[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(VenueError::new(
            VenueErrorCode::InvalidInput,
            "reconciled_order",
            "BAD_ORDER_HASH",
        ));
    }
    Ok(())
}

/// One process-local handoff, not a second global configuration slot.
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    pub confirmation: Option<RunConfirmation>,
    pub reconciled_orders: Vec<String>,
    pub rpc_url: Option<RedactedRpcUrl>,
}

/// The one-shot hand-off between the command line and the running bot.
///
/// The confirmation is claimed by exactly one trader, so a second trader in
/// the same process can't inherit someone else's confirmation.
#[derive(Debug, Default)]
pub struct ConfirmationSlot {
    slot: Mutex<Option<RunContext>>,
}

/// The process-wide slot the command line writes into.
static RUN_CONFIRMATION: OnceLock<Arc<ConfirmationSlot>> = OnceLock::new();

/// The process-wide slot, created on first use.
pub fn run_confirmation_slot() -> Arc<ConfirmationSlot> {
    RUN_CONFIRMATION.get_or_init(ConfirmationSlot::new).clone()
}

/// Install a declaration for this process. It isn't applied until a trader
/// claims it.
pub fn install_run_confirmation(confirmation: RunConfirmation) {
    run_confirmation_slot().install(confirmation);
}

impl ConfirmationSlot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Store a declaration. It isn't applied until a trader claims it.
    pub fn install(&self, confirmation: RunConfirmation) {
        self.install_context(RunContext {
            reconciled_orders: confirmation.reconciled_orders.clone(),
            confirmation: Some(confirmation),
            rpc_url: None,
        });
    }

    /// Install all explicit inputs together; parsing performs no I/O.
    pub fn install_context(&self, context: RunContext) {
        *self.slot.lock().unwrap() = Some(context);
    }

    /// Consume once, including mismatched declarations, without losing inputs.
    pub fn claim_context(
        &self,
        identity: &AccountIdentity,
    ) -> Result<Option<RunContext>, VenueError> {
        let Some(context) = self.slot.lock().unwrap().take() else {
            return Ok(None);
        };
        if let Some(confirmation) = &context.confirmation {
            if confirmation.chain_id != identity.chain_id
                || !confirmation
                    .address
                    .eq_ignore_ascii_case(&identity.trading_address)
                || confirmation.mode != identity.mode
            {
                return Err(VenueError::new(
                    VenueErrorCode::AccountMismatch,
                    "run_context",
                    "DECLARATION_MISMATCH",
                ));
            }
        }
        Ok(Some(context))
    }

    /// Compatibility entry for callers that only need the declaration.
    pub fn claim(&self, identity: &AccountIdentity) -> Option<RunConfirmation> {
        self.claim_context(identity)
            .ok()
            .flatten()
            .and_then(|context| context.confirmation)
    }

    /// Whether a declaration is waiting.
    pub fn is_pending(&self) -> bool {
        self.slot.lock().unwrap().is_some()
    }
}

/// Check that the account the venue reports is the account we meant to trade.
pub fn check_identity(
    observed_address: &str,
    expected: &AccountIdentity,
) -> Result<(), VenueError> {
    if observed_address.eq_ignore_ascii_case(&expected.trading_address) {
        return Ok(());
    }
    Err(VenueError::new(
        VenueErrorCode::AccountMismatch,
        "account_identity",
        "ADDRESS_MISMATCH",
    ))
}

/// Check that a snapshot was read in full. A partial read is not a healthy
/// account, however empty the part that was read looked.
pub fn check_snapshot_complete(snapshot: &AccountSnapshot) -> Result<(), VenueError> {
    if snapshot.complete && snapshot.collateral_atoms.is_some() && snapshot.gas_atoms.is_some() {
        Ok(())
    } else {
        Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "account_snapshot",
            "INCOMPLETE_PAGES",
        ))
    }
}

/// The risk an unknown-source position carries: its shares at the most a share
/// can pay. It is a risk figure, never a cost.
pub fn inherited_risk(shares: U256) -> U256 {
    shares
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::models::VenueId;

    fn identity() -> AccountIdentity {
        AccountIdentity {
            chain_id: 56,
            trading_address: "0xabc".into(),
            mode: AccountMode::Eoa,
            signer_address: "0xabc".into(),
            venue: VenueId::PredictFun,
        }
    }

    pub(crate) struct SyntheticRpc {
        signer: Address,
        fault: &'static str,
        calls: Mutex<Vec<(String, Value)>>,
        order: Mutex<Option<Value>>,
        pub posts: std::sync::atomic::AtomicUsize,
        pub signs: std::sync::atomic::AtomicUsize,
        pub stage: std::sync::atomic::AtomicUsize,
    }

    impl SyntheticRpc {
        fn legs(&self) -> Value {
            let order = self.order.lock().unwrap().clone().unwrap();
            let stage = self.stage.load(std::sync::atomic::Ordering::SeqCst);
            if self.fault == "sell" {
                return json!([{"market":{"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]},"taker":{"hash":order["hash"],"signer":order["signer"],"quoteType":"Ask","amount":"10000000000000000000","price":"500000000000000000","outcome":{"onChainId":"77"},"fee":{"amount":"20000000000000000","type":"COLLATERAL"}},"makers":[],"executedAt":"2026-11-13T00:00:00Z","transactionHash":format!("0x{:064x}",1)}]);
            }
            let count = if stage == 0 || self.fault == "late" {
                1
            } else {
                2
            };
            let legs: Vec<_> = (0..count).map(|index| {
                let (q, p, fee, kind) = if index == 0 {
                    (4u64, 400_000_000_000_000_000u64, 20_000_000_000_000_000u64, "COLLATERAL")
                } else {
                    (6, 500_000_000_000_000_000, 100_000_000_000_000_000, "SHARES")
                };
                let mut leg = json!({"hash":order["hash"],"signer":order["signer"],"quoteType":"Bid","amount":(U256::from(q)*U256::from(super::super::WEI)).to_string(),"price":p.to_string(),"outcome":{"onChainId":"77"},"fee":{"amount":fee.to_string(),"type":kind}});
                if self.fault == "missing_fee" && stage == 0 { leg.as_object_mut().unwrap().remove("fee"); }
                if self.fault == "invalid_units" { leg["amount"] = json!("-1"); }
                json!({"market":{"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]},"taker":leg,"makers":[],"executedAt":"2026-11-13T00:00:00Z","transactionHash":format!("0x{:064x}",index+1)})
            }).collect();
            json!(legs)
        }

        fn receipt(&self, tx: &str) -> Value {
            let index = if tx.ends_with('1') { 0 } else { 1 };
            let order = self.order.lock().unwrap().clone().unwrap();
            let atom = U256::from(super::super::WEI);
            let (q, cash, fee, share_fee) = if self.fault == "sell" {
                (
                    U256::from(10) * atom,
                    U256::from(5) * atom,
                    U256::from(2) * atom / U256::from(100),
                    U256::ZERO,
                )
            } else if index == 0 {
                (
                    U256::from(4) * atom,
                    U256::from(16) * atom / U256::from(10),
                    U256::from(2) * atom / U256::from(100),
                    U256::ZERO,
                )
            } else {
                (
                    U256::from(6) * atom,
                    U256::from(3) * atom,
                    U256::ZERO,
                    atom / U256::from(10),
                )
            };
            let owner: Address = order["maker"].as_str().unwrap().parse().unwrap();
            let exchange = super::super::MAINNET.exchange(false, false);
            let (usdt, ctf, _) = assets(56, false, false);
            let topic =
                |address: Address| format!("0x{:064x}", U256::from_be_slice(address.as_slice()));
            let words = |values: &[U256]| {
                format!(
                    "0x{}",
                    values
                        .iter()
                        .map(|v| format!("{v:064x}"))
                        .collect::<String>()
                )
            };
            let block_hash = format!("0x{}", "b".repeat(64));
            let log = |index: u64, emitter: Address, topics: Value, data: String| json!({"removed":false,"transactionHash":tx,"blockHash":block_hash,"blockNumber":"0x123","logIndex":format!("{index:#x}"),"address":emitter,"topics":topics,"data":data});
            let logs = if self.fault == "sell" {
                vec![
                    log(
                        0,
                        exchange,
                        json!([
                            OrderFilled::SIGNATURE_HASH,
                            order["hash"],
                            topic(owner),
                            topic(exchange)
                        ]),
                        words(&[U256::from(77), U256::ZERO, q, cash, fee]),
                    ),
                    log(
                        1,
                        usdt,
                        json!([Transfer::SIGNATURE_HASH, topic(exchange), topic(owner)]),
                        words(&[cash - fee]),
                    ),
                    log(
                        2,
                        ctf,
                        json!([
                            TransferSingle::SIGNATURE_HASH,
                            topic(exchange),
                            topic(owner),
                            topic(exchange)
                        ]),
                        words(&[U256::from(77), q]),
                    ),
                ]
            } else {
                vec![
                    log(
                        0,
                        exchange,
                        json!([
                            OrderFilled::SIGNATURE_HASH,
                            order["hash"],
                            topic(owner),
                            topic(exchange)
                        ]),
                        words(&[U256::ZERO, U256::from(77), cash, q, fee + share_fee]),
                    ),
                    log(
                        1,
                        usdt,
                        json!([Transfer::SIGNATURE_HASH, topic(owner), topic(exchange)]),
                        words(&[cash + fee]),
                    ),
                    log(
                        2,
                        ctf,
                        json!([
                            TransferSingle::SIGNATURE_HASH,
                            topic(exchange),
                            topic(exchange),
                            topic(owner)
                        ]),
                        words(&[U256::from(77), q - share_fee]),
                    ),
                ]
            };
            let mut receipt = json!({"transactionHash":tx,"status":"0x1","blockNumber":"0x123","blockHash":block_hash,"logs":logs});
            if self.fault == "receipt_fee" {
                receipt["logs"][0]["data"] = json!(words(&[
                    U256::ZERO,
                    U256::from(77),
                    cash,
                    q,
                    fee + share_fee + U256::from(1)
                ]));
            }
            if self.fault == "log_block" {
                receipt["logs"][0]["blockNumber"] = json!("0x122");
            }
            receipt
        }
    }

    impl SyntheticTransport for SyntheticRpc {
        fn pending_post(&self) -> bool {
            self.fault == "abort"
        }
        fn auth_token(&self) -> String {
            "synthetic-refreshed-jwt".into()
        }
        fn rest(
            &self,
            method: &reqwest::Method,
            path: &str,
            _bearer: Option<&str>,
            body: Option<&Value>,
        ) -> anyhow::Result<String> {
            if path == "/orders/remove" {
                return Ok(
                    json!({"removed":["synthetic-order"],"noop":[],"rejected":[]}).to_string(),
                );
            }
            if path == "/orders" && *method == reqwest::Method::POST {
                self.posts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                *self.order.lock().unwrap() = Some(body.unwrap()["data"]["order"].clone());
                if self.fault == "timeout" {
                    anyhow::bail!("SYNTHETIC_TIMEOUT");
                }
                if self.fault == "post401" {
                    return Err(super::super::ApiError {
                        status: reqwest::StatusCode::UNAUTHORIZED,
                        body: "synthetic".into(),
                    }
                    .into());
                }
                if self.fault == "malformed" {
                    return Ok("{".into());
                }
                return Ok(json!({"data":{"orderId":"synthetic-order"}}).to_string());
            }
            let atom = U256::from(super::super::WEI);
            let settled_copy = self.order.lock().unwrap().is_some()
                && matches!(self.fault, "fills" | "missing_fee")
                && self.stage.load(std::sync::atomic::Ordering::SeqCst) > 0;
            if settled_copy && path.starts_with("/positions?") {
                return Ok(json!({"data":[{"market":{"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]},"outcome":{"onChainId":"77"},"amount":(atom * U256::from(99) / U256::from(10)).to_string()}]}).to_string());
            }
            if path.starts_with("/positions?") && self.fault == "sell" {
                return Ok(json!({"data":[{"market":{"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]},"outcome":{"onChainId":"77"},"amount":"10000000000000000000"}]}).to_string());
            }
            if path.starts_with("/positions?")
                && matches!(
                    self.fault,
                    "positions" | "position_loop" | "position_missing" | "position_cap"
                )
            {
                let second = path.contains("after=");
                if second && self.fault == "position_missing" {
                    anyhow::bail!("SYNTHETIC_MISSING_PAGE");
                }
                let token = if second { "88" } else { "77" };
                let amount = if second {
                    "3000000000000000000"
                } else {
                    "2000000000000000000"
                };
                let cursor = match self.fault {
                    "position_loop" => Some("next".to_string()),
                    "position_cap" => Some(format!("next{}", path.len())),
                    _ if !second => Some("next".to_string()),
                    _ => None,
                };
                return Ok(json!({"data":[{"market":{"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]},"outcome":{"onChainId":token},"amount":amount}],"cursor":cursor}).to_string());
            }
            if path.starts_with("/orders/matches?") && self.fault == "history_loop" {
                return Ok(json!({"data":[],"cursor":"repeat"}).to_string());
            }
            if path.starts_with("/orders/matches?")
                && matches!(
                    self.fault,
                    "fills"
                        | "missing_fee"
                        | "invalid_units"
                        | "receipt_fee"
                        | "log_block"
                        | "late"
                        | "sell"
                )
                && self.order.lock().unwrap().is_some()
            {
                return Ok(json!({"data":self.legs()}).to_string());
            }
            let data = if path.starts_with("/positions?")
                || path.starts_with("/orders?status=OPEN")
                || path.starts_with("/orders/matches?")
            {
                json!([])
            } else if path == "/account" {
                json!({"address":"0x1234567890123456789012345678901234567890"})
            } else if path == "/markets/1/orderbook" {
                json!({"asks":[[0.5,100.0]], "bids":[[0.4,100.0]]})
            } else if path.starts_with("/orders/") {
                if self.fault == "404"
                    || (self.fault == "late"
                        && self.stage.load(std::sync::atomic::Ordering::SeqCst) == 1)
                {
                    anyhow::bail!("SYNTHETIC_404");
                }
                let nonzero = matches!(
                    self.fault,
                    "fills"
                        | "missing_fee"
                        | "invalid_units"
                        | "receipt_fee"
                        | "log_block"
                        | "late"
                        | "sell"
                );
                let stage = self.stage.load(std::sync::atomic::Ordering::SeqCst);
                let q = if self.fault == "sell" {
                    10
                } else if nonzero {
                    if stage == 0 || self.fault == "late" {
                        4u64
                    } else {
                        10
                    }
                } else {
                    0
                };
                json!({"id":"synthetic-order", "marketId":1, "amountFilled":(U256::from(q)*U256::from(super::super::WEI)).to_string(), "status": if self.fault == "sell" { "FILLED" } else if self.fault == "late" { if stage >= 3 { "CANCELLED" } else { "OPEN" } } else if self.fault == "terminal" || (self.stage.load(std::sync::atomic::Ordering::SeqCst)>0 && matches!(self.fault,"timeout"|"post401"|"malformed"|"abort")) { "CANCELLED" } else if nonzero && stage > 0 { "FILLED" } else { "OPEN" }, "order":self.order.lock().unwrap().clone().unwrap()})
            } else {
                panic!("unexpected synthetic REST {method} {path}");
            };
            Ok(json!({"data":data}).to_string())
        }

        fn sign(&self, order: &super::super::Order) -> anyhow::Result<(B256, String)> {
            self.signs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok((
                super::super::order_digest(order, &super::super::MAINNET, false, false),
                format!("0x01{}{}", hex::encode(ECDSA_VALIDATOR), "00".repeat(65)),
            ))
        }

        fn rpc(&self, method: &str, params: &Value) -> Result<Value, VenueError> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            if self.fault == "read" {
                return Err(read_error("SYNTHETIC_READ_FAILURE"));
            }
            match method {
                "eth_getTransactionReceipt" => Ok(self.receipt(params[0].as_str().unwrap())),
                "eth_chainId" => Ok(json!(if self.fault == "chain" {
                    "0x61"
                } else {
                    "0x38"
                })),
                "eth_getBlockByNumber" => Ok(
                    json!({"number":"0x123", "timestamp":"0x100", "hash": format!("0x{}", "b".repeat(64))}),
                ),
                "eth_getBalance" => {
                    assert_eq!(params[1], "0x123");
                    Ok(json!("0x100"))
                }
                "eth_call" => {
                    assert_eq!(params[1], "0x123");
                    let data = params[0]["data"].as_str().unwrap();
                    let selector = hex::decode(&data[2..10]).unwrap();
                    if selector == ExchangeRead::registryCall::SELECTOR {
                        let token = U256::from_str_radix(&data[data.len() - 64..], 16).unwrap();
                        return Ok(json!(format!(
                            "0x{:064x}{}",
                            if token == U256::from(77) { 88 } else { 77 },
                            hex::encode(B256::repeat_byte(0xcc))
                        )));
                    }
                    if selector == ExchangeRead::getOrderStatusCall::SELECTOR {
                        return Ok(json!(format!(
                            "0x{:064x}{:064x}",
                            if self.fault == "late"
                                && self.stage.load(std::sync::atomic::Ordering::SeqCst) < 3
                            {
                                0
                            } else {
                                1
                            },
                            0
                        )));
                    }
                    let word = if selector == ValidatorRead::isInitializedCall::SELECTOR {
                        U256::from((self.fault != "initialized") as u8)
                    } else if selector == ValidatorRead::ecdsaValidatorStorageCall::SELECTOR {
                        if self.fault == "owner" {
                            U256::ZERO
                        } else {
                            U256::from_be_slice(self.signer.as_slice())
                        }
                    } else if selector == TokenRead::decimalsCall::SELECTOR {
                        U256::from(18)
                    } else if selector == TokenRead::balanceOfCall::SELECTOR
                        || selector == TokenRead::allowanceCall::SELECTOR
                    {
                        let atom = U256::from(super::super::WEI);
                        if selector == TokenRead::balanceOfCall::SELECTOR
                            && self.order.lock().unwrap().is_some()
                            && matches!(self.fault, "fills" | "missing_fee")
                            && self.stage.load(std::sync::atomic::Ordering::SeqCst) > 0
                        {
                            U256::from(100u64) * atom - U256::from(462) * atom / U256::from(100)
                        } else {
                            U256::from(100u64) * atom
                        }
                    } else if selector == ExchangeRead::isValidNonceCall::SELECTOR {
                        U256::from(1)
                    } else if selector == CtfRead::balanceOfCall::SELECTOR {
                        let token = U256::from_str_radix(&data[data.len() - 64..], 16).unwrap();
                        if token == U256::from(77)
                            && self.order.lock().unwrap().is_some()
                            && matches!(self.fault, "fills" | "missing_fee")
                            && self.stage.load(std::sync::atomic::Ordering::SeqCst) > 0
                        {
                            U256::from(99) * U256::from(super::super::WEI) / U256::from(10)
                        } else if self.fault == "sell" {
                            U256::from(10) * U256::from(super::super::WEI)
                        } else if token == U256::from(77) {
                            U256::from(2) * U256::from(super::super::WEI)
                        } else {
                            U256::from(3) * U256::from(super::super::WEI)
                        }
                    } else if selector == CtfRead::isApprovedForAllCall::SELECTOR {
                        U256::from(1)
                    } else if selector == ExchangeRead::getCollateralCall::SELECTOR {
                        U256::from_be_slice(assets(56, false, false).0.as_slice())
                    } else if selector == ExchangeRead::getCtfCall::SELECTOR {
                        U256::from_be_slice(assets(56, false, false).1.as_slice())
                    } else if selector == AccountRead::isValidSignatureCall::SELECTOR {
                        return Ok(json!(format!(
                            "0x{}{}",
                            if self.fault == "signature" {
                                "ffffffff"
                            } else {
                                "1626ba7e"
                            },
                            "0".repeat(56)
                        )));
                    } else {
                        panic!("unexpected ABI selector");
                    };
                    Ok(json!(format!("{word:#066x}")))
                }
                _ => panic!("unexpected RPC method"),
            }
        }
    }

    /// Only derives the address from the pre-existing public 0x11…11 vector;
    /// every signing/transport method is synthetic and never delegates.
    pub(crate) fn synthetic_trader(fault: &'static str) -> (PredictFunTrader, Arc<SyntheticRpc>) {
        use super::super::{PredictFunClient, MAINNET};
        use alloy_signer::Signer;
        let signer = "1111111111111111111111111111111111111111111111111111111111111111"
            .parse::<alloy_signer_local::PrivateKeySigner>()
            .unwrap();
        let transport = Arc::new(SyntheticRpc {
            signer: signer.address(),
            fault,
            calls: Mutex::new(vec![]),
            order: Mutex::new(None),
            posts: Default::default(),
            signs: Default::default(),
            stage: Default::default(),
        });
        let client = Arc::new(PredictFunClient {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            deployment: &MAINNET,
            network: crate::config::PredictNetwork::Mainnet,
            api_key: None,
            markets: parking_lot::RwLock::new(HashMap::new()),
            categories: parking_lot::RwLock::new(HashMap::new()),
            events: parking_lot::RwLock::new(HashMap::new()),
            synthetic: Some(transport.clone()),
        });
        let trader = PredictFunTrader {
            client,
            signer,
            account: Some(address!("1234567890123456789012345678901234567890")),
            jwt: tokio::sync::RwLock::new(Some("synthetic-jwt".into())),
            gate: AccountGate::new(),
            generations: parking_lot::RwLock::new(HashMap::new()),
            bound_orders: parking_lot::RwLock::new(HashMap::new()),
            run_context: parking_lot::RwLock::new(Some(RunContext {
                rpc_url: Some(parse_rpc_url("https://synthetic.invalid").unwrap()),
                ..RunContext::default()
            })),
        };
        let market: ApiMarket = serde_json::from_value(json!({"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]})).unwrap();
        trader.client.remember_market(&market);
        trader.gate.confirm_budget(&trader.identity());
        (trader, transport)
    }

    #[tokio::test]
    async fn a009_direct_privy_reader_uses_distinct_gas_and_asset_addresses() {
        let (trader, transport) = synthetic_trader("");
        let read = trader.chain_read().await.unwrap();
        let balances = read.balances().await.unwrap();
        assert_eq!(
            balances.0,
            U256::from(100u64) * U256::from(super::super::WEI)
        );
        assert_eq!(balances.1, U256::from(256));
        let market: ApiMarket = serde_json::from_value(json!({"id":1,"tradingStatus":"OPEN","conditionId":B256::repeat_byte(0xcc),"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"outcomes":[{"onChainId":"77"},{"onChainId":"88"}]})).unwrap();
        read.authorize(
            &market,
            U256::from(77),
            crate::models::Side::Buy,
            U256::from(10),
        )
        .await
        .unwrap();
        let signature = format!("0x01{}{}", hex::encode(ECDSA_VALIDATOR), "00".repeat(65));
        read.signature(B256::ZERO, &signature).await.unwrap();
        let calls = transport.calls.lock().unwrap();
        let gas = calls
            .iter()
            .find(|(method, _)| method == "eth_getBalance")
            .unwrap();
        assert_eq!(gas.1[0], json!(transport.signer));
        assert_ne!(gas.1[0], json!(trader.trading_address()));
        let balance = calls
            .iter()
            .find(|(method, params)| {
                method == "eth_call"
                    && params[0]["data"].as_str().unwrap().starts_with(&format!(
                        "0x{}",
                        hex::encode(TokenRead::balanceOfCall::SELECTOR)
                    ))
            })
            .unwrap();
        assert!(balance.1[0]["data"]
            .as_str()
            .unwrap()
            .ends_with(&hex::encode(trader.trading_address())));
        assert_eq!(balance.1[0]["to"], json!(assets(56, false, false).0));
    }

    #[tokio::test]
    async fn a010_direct_privy_reader_rejects_missing_chain_owner_and_signature() {
        for fault in ["chain", "owner", "initialized", "read", "signature"] {
            let (trader, _) = synthetic_trader(fault);
            trader.gate.reserve(&trader.identity(), 7).unwrap();
            let result = async {
                let read = trader.chain_read().await?;
                read.balances().await?;
                read.signature(
                    B256::ZERO,
                    &format!("0x01{}{}", hex::encode(ECDSA_VALIDATOR), "00".repeat(65)),
                )
                .await
            }
            .await;
            let error = result.unwrap_err();
            assert_eq!(
                error.code,
                if fault == "read" {
                    VenueErrorCode::ReadUnavailable
                } else {
                    VenueErrorCode::AccountMismatch
                }
            );
            assert_eq!(
                trader.gate.unconfirmed_attempts(&trader.identity()),
                vec![7]
            );
        }
        let (trader, transport) = synthetic_trader("");
        *trader.run_context.write() = None;
        assert_eq!(
            trader.chain_read().await.err().unwrap().reason,
            "MISSING_READ_CONTEXT"
        );
        assert!(transport.calls.lock().unwrap().is_empty());
        assert_eq!(
            trader
                .rpc("eth_sendRawTransaction", json!([]))
                .await
                .unwrap_err()
                .reason,
            "RPC_METHOD_NOT_ALLOWED"
        );
    }

    #[test]
    fn a010_restart_confirmation_cannot_override_risk() {
        let gate = AccountGate::new();
        let id = identity();
        // An unconfirmed submission blocks new orders.
        gate.reserve(&id, 1).unwrap();
        gate.mark_submitted(&id, 1);
        assert!(!gate.allows_new_orders(&id));
        assert!(gate.reserve(&id, 2).is_err());

        // A confirmation doesn't clear it.
        gate.confirm_budget(&id);
        assert!(!gate.allows_new_orders(&id));
        assert!(gate.reserve(&id, 3).is_err());

        // Only resolving the attempt opens the account.
        gate.resolve(&id, 1, AttemptPhase::Closed);
        assert!(gate.allows_new_orders(&id));
        assert!(gate.reserve(&id, 4).is_ok());
    }

    #[test]
    fn the_same_attempt_is_reserved_once() {
        let gate = AccountGate::new();
        let id = identity();
        gate.reserve(&id, 7).unwrap();
        // Re-registering the same id doesn't block it.
        assert!(gate.reserve(&id, 7).is_ok());
        assert_eq!(gate.unconfirmed_attempts(&id), vec![7]);
    }

    #[test]
    fn a_failed_read_blocks_until_that_read_succeeds() {
        let gate = AccountGate::new();
        let id = identity();
        gate.record_read_failure(&id, "positions", "PAGE_CAP");
        assert!(!gate.allows_new_orders(&id));
        gate.record_read_success(&id, "positions");
        assert!(gate.allows_new_orders(&id));
    }

    #[test]
    fn a_mismatch_outranks_everything_else() {
        let gate = AccountGate::new();
        let id = identity();
        gate.record_read_failure(&id, "positions", "PAGE_CAP");
        gate.mark_submitted(&id, 1);
        gate.record_mismatch(&id, "ADDRESS_MISMATCH");
        let reason = gate.block_reason(&id).unwrap();
        assert_eq!(reason.code(), VenueErrorCode::AccountMismatch);
    }

    #[test]
    fn a_confirmation_belongs_to_one_account() {
        let slot = ConfirmationSlot::new();
        let mut id = identity();
        slot.install(parse_confirmation("56:0xABC:eoa").unwrap());
        assert!(slot.claim(&id).is_some());
        // It was claimed, so nobody else can use it.
        assert!(slot.claim(&id).is_none());

        let mut other = identity();
        other.trading_address = "0xdef".into();
        slot.install(parse_confirmation("56:0xABC:eoa").unwrap());
        assert!(slot.claim(&other).is_none());
    }

    #[test]
    fn a010_rpc_input_is_explicit_redacted_and_claimed_once() {
        let raw = "https://synthetic-user:synthetic-pass@rpc.invalid/private?key=synthetic-key";
        let url = parse_rpc_url(raw).unwrap();
        assert_eq!(format!("{url:?}"), "[REDACTED_RPC_URL]");
        assert_eq!(format!("{url}"), "[REDACTED_RPC_URL]");
        for invalid in [
            "",
            " ",
            "relative",
            "ftp://rpc.invalid",
            "https://rpc.invalid/#key",
            "https://rpc.invalid/ bad",
        ] {
            let error = parse_rpc_url(invalid).unwrap_err();
            assert_eq!(error.reason, "PREDICT_RPC_URL_INVALID");
            assert!(!format!("{error:?}").contains("rpc.invalid"));
        }
        let hash = format!("0x{}", "a".repeat(64));
        assert!(validate_order_hash(&hash).is_ok());
        for invalid in ["0xhash", "abc", "0x", ""] {
            assert!(validate_order_hash(invalid).is_err());
        }
        let slot = ConfirmationSlot::new();
        let mut confirmation = parse_confirmation("56:0xABC:eoa").unwrap();
        confirmation.reconciled_orders.push(hash.clone());
        slot.install_context(RunContext {
            confirmation: Some(confirmation),
            rpc_url: Some(url),
            reconciled_orders: vec![hash.clone()],
        });
        let context = slot.claim_context(&identity()).unwrap().unwrap();
        assert_eq!(context.reconciled_orders, vec![hash]);
        assert!(context.rpc_url.is_some());
        assert!(slot.claim_context(&identity()).unwrap().is_none());
        slot.install_context(RunContext {
            confirmation: Some(parse_confirmation("97:0xABC:eoa").unwrap()),
            rpc_url: None,
            reconciled_orders: vec![],
        });
        assert_eq!(
            slot.claim_context(&identity()).unwrap_err().code,
            VenueErrorCode::AccountMismatch
        );
        assert!(!slot.is_pending());
    }

    #[test]
    fn a_malformed_confirmation_is_rejected() {
        assert!(parse_confirmation("56:0xabc").is_err());
        assert!(parse_confirmation("nope:0xabc:eoa").is_err());
        assert!(parse_confirmation("56:0xabc:wallet").is_err());
        assert!(parse_confirmation("56:0xabc:eoa:extra").is_err());
    }

    #[tokio::test]
    async fn a009_account_pagination_identity_and_read_failures() {
        // A cursor that repeats is a loop, not a complete read.
        let pages = vec![
            Page {
                items: vec![1],
                cursor: Some("c1".into()),
            },
            Page {
                items: vec![2],
                cursor: Some("c1".into()),
            },
        ];
        let mut index = 0;
        let result = collect_pages(
            |_cursor| {
                let page = pages[index].clone();
                index += 1;
                async move { Ok(page) }
            },
            20,
        )
        .await;
        assert_eq!(result.unwrap_err(), PageError::CursorLoop);

        // A page cap with more to read isn't complete.
        let result = collect_pages(
            |cursor| async move {
                Ok(Page {
                    items: vec![1],
                    cursor: Some(format!("{}n", cursor.unwrap_or_default())),
                })
            },
            3,
        )
        .await;
        assert_eq!(result.unwrap_err(), PageError::PageCap);

        // A read that ends without a cursor is complete.
        let result = collect_pages(
            |_cursor| async move {
                Ok(Page {
                    items: vec![1, 2],
                    cursor: None,
                })
            },
            20,
        )
        .await;
        assert_eq!(result.unwrap(), vec![1, 2]);
    }

    #[test]
    fn an_empty_but_complete_account_is_known_zero() {
        let snapshot = AccountSnapshot {
            identity: identity(),
            collateral_atoms: Some(U256::ZERO),
            gas_atoms: Some(U256::ZERO),
            positions: vec![],
            positions_explained: true,
            open_orders: vec![],
            complete: true,
        };
        assert!(check_snapshot_complete(&snapshot).is_ok());
    }

    #[test]
    fn an_incomplete_account_is_not_a_healthy_account() {
        let snapshot = AccountSnapshot {
            identity: identity(),
            collateral_atoms: None,
            gas_atoms: None,
            positions: vec![],
            positions_explained: true,
            open_orders: vec![],
            complete: false,
        };
        assert_eq!(
            check_snapshot_complete(&snapshot).unwrap_err().code,
            VenueErrorCode::ReadUnavailable
        );
    }

    #[test]
    fn a009_missing_required_balances_cannot_be_complete() {
        for (collateral_atoms, gas_atoms) in [
            (None, None),
            (Some(U256::ZERO), None),
            (None, Some(U256::ZERO)),
        ] {
            let snapshot = AccountSnapshot {
                identity: identity(),
                collateral_atoms,
                gas_atoms,
                positions: vec![],
                positions_explained: true,
                open_orders: vec![],
                complete: true,
            };
            assert_eq!(
                check_snapshot_complete(&snapshot).unwrap_err().code,
                VenueErrorCode::ReadUnavailable
            );
        }
    }

    #[test]
    fn the_identity_must_match() {
        assert!(check_identity("0xABC", &identity()).is_ok());
        assert_eq!(
            check_identity("0xdef", &identity()).unwrap_err().code,
            VenueErrorCode::AccountMismatch
        );
    }
}
