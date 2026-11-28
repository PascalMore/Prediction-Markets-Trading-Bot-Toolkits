//! One account observation, with evidence referring only to sync-consumed generations.

use super::*;
use crate::service::venue::{Accounting, BoundClaimEvidence, ExecutionSnapshot, OrderIdentity,
    RuntimeAccountObservation, RuntimeClaimInventory};
use crate::models::Side;
use std::{cell::RefCell, future::Future};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillCashLeg {
    pub tx: B256,
    pub log_index: U256,
    pub block_number: U256,
    pub block_hash: B256,
    pub incoming: bool,
    pub atoms: U256,
}

#[derive(Debug, Clone)]
pub(crate) struct FillCashEvidence {
    pub identity: OrderIdentity,
    pub generation: u64,
    pub legs: Vec<FillCashLeg>,
}

#[derive(Default)]
struct FillRead {
    receipts: Option<Vec<FillCashLeg>>,
    evidence: Option<FillCashEvidence>,
}

tokio::task_local! { static FILL_READ: RefCell<FillRead>; }

pub(crate) async fn capture_fill_evidence<T>(read: impl Future<Output = T>) -> (T, Option<FillCashEvidence>) {
    FILL_READ.scope(RefCell::new(FillRead { receipts: Some(Vec::new()), evidence: None }), async {
        let result = read.await;
        let evidence = FILL_READ.with(|slot| slot.borrow_mut().evidence.take());
        (result, evidence)
    }).await
}

pub(in crate::service::predict_fun) fn finish_fill_read(snapshot: &ExecutionSnapshot) {
    let _ = FILL_READ.try_with(|slot| {
        let mut read = slot.borrow_mut();
        let legs = read.receipts.take();
        read.evidence = legs.filter(|_| snapshot.pages_complete && snapshot.accounting_complete()
            && snapshot.submission == crate::service::venue::Submission::Accepted
            && matches!(snapshot.closure, crate::service::venue::Closure::Final { .. }))
            .map(|legs| FillCashEvidence { identity: snapshot.identity.clone(), generation: snapshot.generation, legs });
    });
}

pub(super) fn observe_fill_receipt(receipt: &Value, identity: &OrderIdentity,
    route: [Address; 4], token: U256, expected: &Accounting) {
    let _ = FILL_READ.try_with(|slot| {
        let mut read = slot.borrow_mut();
        if let Some(legs) = &mut read.receipts {
            match project_fill_receipt(receipt, identity, route, token, expected) {
                Some(leg) => legs.push(leg),
                None => read.receipts = None,
            }
        }
    });
}

// Only the unambiguous direct exchange route is projected. Legacy settlement
// still accepts its original aggregation; its net is not attribution evidence.
fn project_fill_receipt(receipt: &Value, identity: &OrderIdentity,
    [owner, exchange, usdt, ctf]: [Address; 4], token: U256, expected: &Accounting) -> Option<FillCashLeg> {
    let incoming = identity.side == Side::Sell;
    let (cash_from, cash_to) = if incoming { (exchange, owner) } else { (owner, exchange) };
    let signed: B256 = identity.signed_hash.as_deref()?.parse().ok()?;
    let mut fill_seen = false;
    let mut inventory_seen = false;
    let mut cash_index = None;
    for log in receipt["logs"].as_array()? {
        let address: Address = log["address"].as_str()?.parse().ok()?;
        if ![exchange, usdt, ctf].contains(&address) { continue; }
        let topics: Vec<B256> = serde_json::from_value(log["topics"].clone()).ok()?;
        let data = hex::decode(log["data"].as_str()?.strip_prefix("0x")?).ok()?;
        match (address, topics.first()) {
            (emitter, Some(sig)) if emitter == exchange && *sig == OrderFilled::SIGNATURE_HASH => {
                let fill = OrderFilled::decode_raw_log(topics.iter().copied(), &data, true).ok()?;
                if fill.maker != owner { continue; }
                if fill_seen || fill.orderHash != signed || fill.taker != exchange { return None; }
                fill_seen = true;
            }
            (emitter, Some(sig)) if emitter == usdt && *sig == Transfer::SIGNATURE_HASH => {
                let transfer = Transfer::decode_raw_log(topics.iter().copied(), &data, true).ok()?;
                if transfer.from != owner && transfer.to != owner { continue; }
                if cash_index.is_some() || (transfer.from, transfer.to) != (cash_from, cash_to)
                    || Some(&transfer.value) != expected.net_cash.value() { return None; }
                cash_index = Some(rpc_quantity(&log["logIndex"]).ok()?);
            }
            (emitter, Some(sig)) if emitter == ctf && *sig == TransferSingle::SIGNATURE_HASH => {
                let transfer = TransferSingle::decode_raw_log(topics.iter().copied(), &data, true).ok()?;
                if transfer.id != token || (transfer.from != owner && transfer.to != owner) { continue; }
                if inventory_seen || transfer.operator != exchange || (transfer.from, transfer.to) != (cash_to, cash_from)
                    || Some(&transfer.value) != expected.net_inventory.value() { return None; }
                inventory_seen = true;
            }
            (emitter, Some(sig)) if emitter == ctf && *sig == TransferBatch::SIGNATURE_HASH => {
                let transfer = TransferBatch::decode_raw_log(topics.iter().copied(), &data, true).ok()?;
                if transfer.ids.contains(&token) && (transfer.from == owner || transfer.to == owner) { return None; }
            }
            _ => {}
        }
    }
    if !fill_seen || !inventory_seen { return None; }
    Some(FillCashLeg {
        tx: receipt["transactionHash"].as_str()?.parse().ok()?,
        log_index: cash_index?,
        block_number: rpc_quantity(&receipt["blockNumber"]).ok()?,
        block_hash: receipt["blockHash"].as_str()?.parse().ok()?,
        incoming,
        atoms: *expected.net_cash.value()?,
    })
}

use futures_util::{stream, StreamExt};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CashAnchor {
    pub number: u64,
    pub hash: B256,
    pub timestamp: U256,
    pub collateral: U256,
    pub identity: AccountIdentity,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CashProofError {
    Unavailable(&'static str),
    Read(VenueError),
    Conflict(&'static str),
}

use CashProofError::{Conflict, Unavailable};

impl From<VenueError> for CashProofError {
    fn from(error: VenueError) -> Self {
        match (error.code, error.reason.as_str()) {
            // The RPC boundary also uses ReadUnavailable for invalid or absent replies.
            (VenueErrorCode::ReadUnavailable, "RPC_RESULT_MISSING") => Unavailable("RPC_RESULT_MISSING"),
            (VenueErrorCode::ReadUnavailable, "RPC_JSON" | "RPC_RESPONSE" | "ABI_RESULT"
                | "INVALID_HEX" | "RPC_METHOD_NOT_ALLOWED" | "MISSING_READ_CONTEXT") => Conflict("PROOF_RPC_INVALID"),
            (VenueErrorCode::ReadUnavailable, _) => Self::Read(error),
            _ => Conflict("PROOF_READ_INVALID"),
        }
    }
}

// Restrict the new reply classification to the existing bound R22 subreads.
tokio::task_local! { static ACTIVE_R22_RPC: (); }

async fn r22_rpc_reply<T>(active: bool, read: impl Future<Output = T>) -> T {
    if active { ACTIVE_R22_RPC.scope((), read).await } else { read.await }
}

// Shared by the HTTP reply and the offline raw-response fixture.
pub(in crate::service::predict_fun) fn decode_rpc_reply(body: Value) -> Result<Value, VenueError> {
    if body.get("id") != Some(&json!(1)) || body.get("jsonrpc") != Some(&json!("2.0")) {
        return Err(read_error("RPC_RESPONSE"));
    }
    if let Some(error) = body.get("error") {
        // Ethereum RPC -32005 (Limit exceeded). Never classify by provider text.
        if ACTIVE_R22_RPC.try_with(|_| ()).is_ok() && body.get("result").is_none()
            && error.is_object() && error["code"].as_i64() == Some(-32005)
            && error["message"].is_string()
        {
            return Err(read_error("RPC_LIMIT_EXCEEDED"));
        }
        return Err(read_error("RPC_RESPONSE"));
    }
    body.get("result")
        .filter(|value| !value.is_null())
        .cloned()
        .ok_or_else(|| read_error("RPC_RESULT_MISSING"))
}

type ProofResult<T> = Result<T, CashProofError>;

fn field<T: serde::de::DeserializeOwned>(value: &Value, reason: &'static str) -> ProofResult<T> {
    serde_json::from_value(value.clone()).map_err(|_| Unavailable(reason))
}

fn number(value: &Value) -> ProofResult<u64> {
    u64::try_from(rpc_quantity(value).map_err(|_| Unavailable("QUANTITY"))?)
        .map_err(|_| Conflict("QUANTITY_OVERFLOW"))
}

struct CashHeader {
    number: u64,
    hash: B256,
    parent: B256,
    bloom: alloy_primitives::Bloom,
    txs: Vec<B256>,
}

#[derive(Debug, PartialEq, Eq)]
struct CashLog {
    index: u64,
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

#[derive(Debug, PartialEq, Eq)]
struct CashReceipt {
    status: u64,
    logs: Vec<CashLog>,
}

#[derive(Debug)]
struct CashTransaction {
    block_number: u64,
    block_hash: B256,
    tx: B256,
    receipt: CashReceipt,
}

fn parse_receipt(raw: &Value, header: &CashHeader, index: usize) -> ProofResult<CashReceipt> {
    check_receipt_identity(raw, header, index)?;
    let status = number(&raw["status"])?;
    let raw_logs = raw["logs"].as_array().ok_or(Unavailable("RECEIPT_LOGS"))?;
    if status > 1 || (status == 0 && !raw_logs.is_empty()) {
        return Err(Conflict("RECEIPT_STATUS"));
    }
    let mut logs = Vec::with_capacity(raw_logs.len());
    for log in raw_logs {
        if field::<bool>(&log["removed"], "LOG_REMOVED")? {
            return Err(Conflict("LOG_REMOVED"));
        }
        check_receipt_identity(log, header, index)?;
        logs.push(CashLog {
            index: number(&log["logIndex"])?,
            address: field(&log["address"], "LOG_ADDRESS")?,
            topics: field(&log["topics"], "LOG_TOPICS")?,
            data: field(&log["data"], "LOG_DATA")?,
        });
    }
    Ok(CashReceipt { status, logs })
}

fn check_receipt_identity(raw: &Value, header: &CashHeader, index: usize) -> ProofResult<()> {
    if number(&raw["blockNumber"])? != header.number
        || field::<B256>(&raw["blockHash"], "RECEIPT_BLOCK")? != header.hash
        || number(&raw["transactionIndex"])? != index as u64
        || field::<B256>(&raw["transactionHash"], "RECEIPT_TX")? != header.txs[index]
    {
        return Err(Conflict("RECEIPT_IDENTITY"));
    }
    Ok(())
}

impl ChainRead<'_> {
    fn cash_anchor(&self, collateral: U256) -> ProofResult<CashAnchor> {
        Ok(CashAnchor { number: number(&json!(self.block))?, hash: self.hash,
            timestamp: self.timestamp, collateral, identity: self.trader.identity() })
    }

    async fn cash_header(&self, height: u64, endpoints: [&CashAnchor; 2]) -> ProofResult<CashHeader> {
        let raw = self.trader.rpc("eth_getBlockByNumber", json!([format!("{height:#x}"), false])).await?;
        let number = number(&raw["number"])?;
        let hash = field(&raw["hash"], "HEADER_HASH")?;
        if number != height || endpoints.iter().any(|end| end.number == height && end.hash != hash) {
            return Err(Conflict("CANONICAL_ENDPOINT"));
        }
        Ok(CashHeader { number, hash, parent: field(&raw["parentHash"], "HEADER_PARENT")?,
            bloom: field(&raw["logsBloom"], "HEADER_BLOOM")?,
            txs: field(&raw["transactions"], "HEADER_TRANSACTIONS")? })
    }

    async fn cash_census(&self, left: &CashAnchor, right: &CashAnchor) -> ProofResult<(Vec<CashTransaction>, bool)> {
        let span = right.number.checked_sub(left.number).ok_or(Conflict("BLOCK_REGRESSION"))?;
        if left.identity != right.identity || right.timestamp < left.timestamp || (span == 0 && (left.hash != right.hash
            || left.collateral != right.collateral || left.timestamp != right.timestamp)) {
            return Err(Conflict("ANCHOR_CONFLICT"));
        }
        if span > 96 { return Err(Unavailable("HEADER_BUDGET")); }
        let owner = self.trader.trading_address();
        let (usdt, _, _) = assets(right.identity.chain_id, false, false);
        let topic = owner.into_word();
        let ends = [left, right];
        let mut headers = stream::iter(left.number..=right.number)
            .map(|height| self.cash_header(height, ends)).buffered(8);
        let mut previous: Option<CashHeader> = None;
        let mut candidates = Vec::new();
        let mut tx_count = 0usize;
        let mut seen = HashSet::new();
        while let Some(header) = headers.next().await {
            let header = header?;
            if let Some(parent) = previous.replace(header) {
                let child = previous.as_ref().unwrap();
                if child.parent != parent.hash { return Err(Conflict("HEADER_PARENT")); }
                if parent.number != left.number && cash_candidate(&parent, usdt, topic) {
                    push_candidate(parent, &mut candidates, &mut tx_count, &mut seen)?;
                }
            }
        }
        if let Some(last) = previous.filter(|last| last.number != left.number && cash_candidate(last, usdt, topic)) {
            push_candidate(last, &mut candidates, &mut tx_count, &mut seen)?;
        }
        let mut log_count = 0usize;
        let mut census = Vec::with_capacity(tx_count);
        let mut short_logs = false;
        for header in &candidates {
            let batch = self.trader.rpc("eth_getBlockReceipts", json!([format!("{:#x}", header.number)])).await?;
            let batch = batch.as_array().ok_or(Unavailable("BLOCK_RECEIPTS"))?;
            // §R22.2(3) requires an exact no-null batch; a null entry stays
            // missing evidence even when every single receipt read agrees.
            short_logs |= batch.iter().any(Value::is_null);
            let mut indexed = BTreeMap::new();
            for raw in batch.iter().filter(|raw| !raw.is_null()) {
                let index = usize::try_from(number(&raw["transactionIndex"])?)
                    .map_err(|_| Conflict("RECEIPT_INDEX"))?;
                if index >= header.txs.len() || indexed.contains_key(&index) {
                    return Err(Conflict("EXTRA_OR_DUPLICATE_RECEIPT"));
                }
                charge_logs(raw, &mut log_count)?;
                indexed.insert(index, parse_receipt(raw, header, index)?);
            }
            let mut singles = stream::iter(header.txs.iter().copied()).map(|tx: B256| async move {
                self.trader.rpc("eth_getTransactionReceipt", json!([tx])).await
            }).buffered(4).enumerate();
            let mut next_log = 0u64;
            while let Some((index, single)) = singles.next().await {
                let single = single?;
                charge_logs(&single, &mut log_count)?;
                let full = parse_receipt(&single, header, index)?;
                if let Some(batch) = indexed.remove(&index) {
                    if batch.status != full.status || batch.logs.iter().zip(&full.logs).any(|(a, b)| a != b) {
                        return Err(Conflict("RECEIPT_READ_DISAGREEMENT"));
                    }
                    short_logs |= batch.logs.len() != full.logs.len();
                } else {
                    short_logs = true;
                }
                for log in &full.logs {
                    if log.index != next_log { return Err(Conflict("BLOCK_LOG_INDEX")); }
                    next_log += 1;
                }
                census.push(CashTransaction { block_number: header.number, block_hash: header.hash,
                    tx: header.txs[index], receipt: full });
            }
        }
        // Check canonicality again after the last child read, not only at entry.
        for end in ends { self.cash_header(end.number, ends).await?; }
        // Keep independently checked logs even on a short batch response so
        // already-proven conflicts cannot be downgraded to missing evidence.
        Ok((census, short_logs))
    }
}

fn cash_candidate(header: &CashHeader, usdt: Address, owner: B256) -> bool {
    header.bloom.contains_input(alloy_primitives::BloomInput::Raw(usdt.as_slice()))
        && header.bloom.contains_input(alloy_primitives::BloomInput::Raw(Transfer::SIGNATURE_HASH.as_slice()))
        && header.bloom.contains_input(alloy_primitives::BloomInput::Raw(owner.as_slice()))
}

fn push_candidate(header: CashHeader, candidates: &mut Vec<CashHeader>, count: &mut usize,
    seen: &mut HashSet<B256>) -> ProofResult<()> {
    if header.txs.iter().any(|tx| !seen.insert(*tx)) { return Err(Conflict("DUPLICATE_HEADER_TX")); }
    *count = count.checked_add(header.txs.len()).ok_or(Unavailable("RECEIPT_BUDGET"))?;
    if candidates.len() >= 32 || *count > 4096 / 2 { return Err(Unavailable("RECEIPT_BUDGET")); }
    candidates.push(header);
    Ok(())
}

fn charge_logs(raw: &Value, count: &mut usize) -> ProofResult<()> {
    let logs = raw["logs"].as_array().ok_or(Unavailable("RECEIPT_LOGS"))?;
    *count = count.checked_add(logs.len()).filter(|count| *count <= 32768)
        .ok_or(Unavailable("LOG_BUDGET"))?;
    Ok(())
}

sol! {
    event PayoutRedemption(address indexed redeemer, address indexed collateralToken,
        bytes32 indexed parentCollectionId, bytes32 conditionId, uint256[] indexSets, uint256 payout);
    interface RedemptionRead {
        function getCollectionId(bytes32 parentCollectionId, bytes32 conditionId, uint256 indexSet) external view returns (bytes32);
        function getPositionId(address collateralToken, bytes32 collectionId) external view returns (uint256);
    }
}

/// Input for the proof only. The Broker must bind this to the current official
/// result and fully applied execution before it can consume any evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CashTarget {
    pub condition: B256,
    pub token: U256,
    pub quantity: U256,
    pub payout: U256,
}

#[derive(Debug, Clone)]
pub(crate) struct CashProof {
    pub payout: Option<FillCashLeg>,
    pub other_in: U256,
    pub other_out: U256,
    pub claims: HashMap<u64, FillCashLeg>,
}

/// Private adapter-to-Broker observation, never a successful account receipt.
#[derive(Debug)]
pub(crate) struct PositionLag {
    pub token: String,
    pub quantity: U256,
    pub anchor: ProofResult<CashAnchor>,
    pub proof: Option<ProofResult<(LagCashProof, Vec<crate::service::venue::TradableMarket>)>>,
    pub double_zero: bool,
    pub observation: RuntimeAccountObservation,
}

#[derive(Debug, Clone)]
pub(crate) struct LagCashProof {
    pub owner: RuntimeClaimInventory,
    pub left: CashAnchor,
    pub target: CashTarget,
    pub cash: CashProof,
    pub sampled: crate::service::observation::CheckTime,
}

pub(crate) type LagProofRequest = (CashAnchor, CashTarget, Vec<FillCashLeg>, crate::service::observation::CheckTime,
    Option<(CashAnchor, Option<FillCashLeg>)>);
pub(crate) type LagTarget = Box<dyn Fn(&PositionLag, &RuntimeClaimInventory,
    &crate::service::venue::TradableMarket, &NativeSettlementMarket, bool) -> ProofResult<LagProofRequest> + Send + Sync>;
tokio::task_local! { pub(crate) static LAG_BOOKED: HashSet<u64>; }

pub(crate) type OfficialBatch = HashMap<u64, (crate::service::venue::TradableMarket, Option<NativeSettlementMarket>)>;
tokio::task_local! {
    pub(crate) static LAG_TARGET: LagTarget;
    pub(crate) static LAG_OWNER: Option<RuntimeClaimInventory>;
    pub(crate) static OFFICIAL_BATCH: OfficialBatch;
}

impl std::fmt::Display for PositionLag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("R22_POSITION_LAG")
    }
}
impl std::error::Error for PositionLag {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeSettlementMarket {
    pub id: u64,
    pub condition: Option<B256>,
    pub tokens: [String; 2],
    pub neg_risk: bool,
    pub yield_bearing: bool,
}

tokio::task_local! {
    static POSITION_LAG_READ: ();
    static NATIVE_SETTLEMENT: RefCell<Option<NativeSettlementMarket>>;
}

pub(crate) async fn capture_native_market<T>(read: impl Future<Output = T>) -> (T, Option<NativeSettlementMarket>) {
    NATIVE_SETTLEMENT.scope(RefCell::new(None), async {
        let result = read.await;
        (result, NATIVE_SETTLEMENT.with(|slot| slot.borrow_mut().take()))
    }).await
}

pub(in crate::service::predict_fun) fn project_native_market(id: u64, market: &ApiMarket) {
    let _ = NATIVE_SETTLEMENT.try_with(|slot| {
        let [yes, no] = market.outcomes.as_slice() else { return; };
        if market.id != id || yes.on_chain_id == no.on_chain_id { return; }
        *slot.borrow_mut() = Some(NativeSettlementMarket { id, condition: market.condition_id,
            tokens: [yes.on_chain_id.clone(), no.on_chain_id.clone()],
            neg_risk: market.is_neg_risk, yield_bearing: market.is_yield_bearing });
    });
}

pub(crate) async fn capture_position_lag<T>(read: impl Future<Output = T>) -> T {
    POSITION_LAG_READ.scope((), read).await
}

pub(crate) fn inventory_mismatch() -> VenueError {
    VenueError::new(VenueErrorCode::ReconciliationRequired, "positions", "INVENTORY_MISMATCH")
}

#[derive(Debug)]
pub(crate) struct CashObservationPending;

impl std::fmt::Display for CashObservationPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("R22_CASH_OBSERVATION_PENDING")
    }
}
impl std::error::Error for CashObservationPending {}

#[derive(Default)]
struct CashRead {
    request: Option<(CashAnchor, CashTarget, Vec<FillCashLeg>)>,
    evidence: Option<ProofResult<(CashAnchor, Option<CashProof>)>>,
}

tokio::task_local! { static CASH_READ: RefCell<CashRead>; }

pub(crate) async fn capture_cash_evidence<T>(
    request: Option<(CashAnchor, CashTarget, Vec<FillCashLeg>)>, read: impl Future<Output = T>,
) -> (T, Option<ProofResult<(CashAnchor, Option<CashProof>)>>) {
    CASH_READ.scope(RefCell::new(CashRead { request, evidence: None }), async {
        let result = read.await;
        (result, CASH_READ.with(|slot| slot.borrow_mut().evidence.take()))
    }).await
}

impl ChainRead<'_> {
    async fn observe_cash(&self, collateral: U256) -> ProofResult<(CashAnchor, Option<CashProof>)> {
        let anchor = self.cash_anchor(collateral)?;
        let request = CASH_READ.with(|slot| slot.borrow_mut().request.take());
        let proof = if let Some((left, target, other)) = request {
            Some(tokio::time::timeout(Duration::from_secs(20), self.prove_cash(&left, &anchor, &target, &other, &[], 0))
                .await.map_err(|_| Unavailable("CENSUS_TIMEOUT"))??)
        } else { None };
        Ok((anchor, proof))
    }

    async fn prove_cash(&self, left: &CashAnchor, right: &CashAnchor, target: &CashTarget,
        other: &[FillCashLeg], claims: &[(u64, CashTarget)], max_extra: usize) -> ProofResult<CashProof> {
        if right.identity.chain_id != 56 { return Err(Unavailable("REDEMPTION_ROUTE")); }
        let mut conditions = HashSet::from([target.condition]);
        let mut tokens = HashSet::from([target.token]);
        if claims.iter().any(|(_, claim)| !conditions.insert(claim.condition) || !tokens.insert(claim.token)) {
            return Err(Conflict("DUPLICATE_CLAIM_BINDING"));
        }
        let (usdt, ctf, _) = assets(right.identity.chain_id, false, false);
        let owner = self.trader.trading_address();
        let (txs, incomplete) = self.cash_census(left, right).await?;
        let mut proof = CashProof { payout: None, other_in: U256::ZERO, other_out: U256::ZERO, claims: HashMap::new() };
        let mut derivations = Vec::new();
        for tx in txs {
            let mut redemptions = HashMap::new();
            for (id, binding) in std::iter::once((None, target)).chain(claims.iter().map(|(id, claim)| (Some(*id), claim))) {
                if let Some((index, index_set)) = redemption_leg(&tx, binding, owner, usdt, ctf)? {
                    if redemptions.insert(index, (id, binding, index_set)).is_some() {
                        return Err(Conflict("DUPLICATE_PAYOUT_LEG"));
                    }
                }
            }
            for log in &tx.receipt.logs {
                let Some(transfer) = cash_transfer(log, usdt)? else { continue; };
                if transfer.from != owner && transfer.to != owner { continue; }
                if transfer.from == owner && transfer.to == owner { return Err(Conflict("SELF_TRANSFER")); }
                let leg = FillCashLeg { tx: tx.tx, log_index: U256::from(log.index),
                    block_number: U256::from(tx.block_number), block_hash: tx.block_hash,
                    incoming: transfer.to == owner, atoms: transfer.value };
                let mut attributed = other.iter().filter(|item| item.tx == leg.tx && item.log_index == leg.log_index);
                let known = attributed.next();
                if attributed.next().is_some() || known.is_some_and(|known| known != &leg) {
                    return Err(Conflict("CASH_LEG_ATTRIBUTION"));
                }
                if let Some((id, binding, index_set)) = redemptions.remove(&log.index) {
                    if known.is_some() { return Err(Conflict("DUPLICATE_PAYOUT")); }
                    derivations.push((binding, index_set, tx.block_number, tx.block_hash));
                    if let Some(id) = id {
                        proof.other_in = proof.other_in.checked_add(leg.atoms).ok_or(Conflict("CASH_OVERFLOW"))?;
                        if proof.claims.insert(id, leg).is_some() { return Err(Conflict("DUPLICATE_PAYOUT")); }
                    } else if proof.payout.replace(leg).is_some() { return Err(Conflict("DUPLICATE_PAYOUT")); }
                } else {
                    if known.is_none() { return Err(Conflict("UNATTRIBUTED_CASH")); }
                    let total = if leg.incoming { &mut proof.other_in } else { &mut proof.other_out };
                    *total = total.checked_add(leg.atoms).ok_or(Conflict("CASH_OVERFLOW"))?;
                }
            }
        }
        // Compare every identified owner transfer before a missing derivation can
        // mask an already established cash conflict elsewhere in this census.
        for (binding, index_set, height, hash) in derivations {
            let collection = self.call(ctf, RedemptionRead::getCollectionIdCall {
                parentCollectionId: B256::ZERO, conditionId: binding.condition, indexSet: index_set,
            }).await?._0;
            let position = self.call(ctf, RedemptionRead::getPositionIdCall {
                collateralToken: usdt, collectionId: collection,
            }).await?._0;
            if position != binding.token { return Err(Conflict("REDEMPTION_POSITION_ID")); }
            if self.cash_header(height, [left, right]).await?.hash != hash {
                return Err(Conflict("PAYOUT_REORG"));
            }
        }
        // Count only attributed payouts, after their token and block are proved.
        if proof.claims.len() > max_extra { return Err(Conflict("R22_CLAIM_SCOPE")); }
        if incomplete { return Err(Unavailable("RECEIPT_CENSUS_SHORT")); }
        let payout = proof.payout.as_ref().map_or(U256::ZERO, |leg| leg.atoms);
        if left.collateral.checked_add(proof.other_in).and_then(|cash| cash.checked_add(payout))
            .zip(right.collateral.checked_add(proof.other_out)).is_none_or(|(expected, actual)| expected != actual)
        {
            return Err(Conflict("CASH_DELTA"));
        }
        for end in [left, right] { self.cash_header(end.number, [left, right]).await?; }
        Ok(proof)
    }
}

fn cash_transfer(log: &CashLog, usdt: Address) -> ProofResult<Option<Transfer>> {
    if log.address != usdt || log.topics.first() != Some(&Transfer::SIGNATURE_HASH) { return Ok(None); }
    Transfer::decode_raw_log(log.topics.iter().copied(), &log.data, true).map(Some)
        .map_err(|_| Unavailable("CASH_TRANSFER_ABI"))
}

fn redemption_leg(tx: &CashTransaction, target: &CashTarget, owner: Address, usdt: Address, ctf: Address)
    -> ProofResult<Option<(u64, U256)>> {
    let mut redemptions = Vec::new();
    let mut inventory = Vec::new();
    let mut cash = Vec::new();
    for log in &tx.receipt.logs {
        if let Some(transfer) = cash_transfer(log, usdt)? { cash.push((log.index, transfer)); }
        if log.address != ctf { continue; }
        match log.topics.first() {
            Some(sig) if *sig == PayoutRedemption::SIGNATURE_HASH => {
                let event = PayoutRedemption::decode_raw_log(log.topics.iter().copied(), &log.data, true)
                    .map_err(|_| Unavailable("REDEMPTION_ABI"))?;
                if event.conditionId == target.condition { redemptions.push(event); }
            }
            Some(sig) if *sig == TransferSingle::SIGNATURE_HASH => {
                let event = TransferSingle::decode_raw_log(log.topics.iter().copied(), &log.data, true)
                    .map_err(|_| Unavailable("REDEMPTION_1155_ABI"))?;
                if event.id == target.token { inventory.push(event); }
            }
            Some(sig) if *sig == TransferBatch::SIGNATURE_HASH => {
                let event = TransferBatch::decode_raw_log(log.topics.iter().copied(), &log.data, true)
                    .map_err(|_| Unavailable("REDEMPTION_1155_ABI"))?;
                if event.ids.contains(&target.token) && (event.from == owner || event.to == owner) {
                    return Err(Conflict("REDEMPTION_1155_ROUTE"));
                }
            }
            _ => {}
        }
    }
    let mut credited = None;
    for event in redemptions {
        let route = event.redeemer;
        let funded: Vec<_> = cash.iter().filter(|(_, leg)| leg.from == route && leg.to == owner).collect();
        let sent: Vec<_> = inventory.iter().filter(|leg| leg.from == owner && leg.to == route).collect();
        if funded.is_empty() && sent.is_empty() { continue; } // Other users can redeem this condition in the same block.
        let burned: Vec<_> = inventory.iter().filter(|leg| leg.from == route && leg.to == Address::ZERO).collect();
        let paid: Vec<_> = cash.iter().filter(|(_, leg)| leg.from == ctf && leg.to == route).collect();
        let ([index_set], [out], [burn], [(_, paid)], [(log_index, incoming)]) =
            (event.indexSets.as_slice(), sent.as_slice(), burned.as_slice(), paid.as_slice(), funded.as_slice())
        else { return Err(Conflict("REDEMPTION_LEGS")); };
        if route == owner || route == Address::ZERO || route == ctf || event.parentCollectionId != B256::ZERO
            || event.collateralToken != usdt || ![U256::from(1), U256::from(2)].contains(index_set)
            || target.quantity.is_zero() || target.payout.is_zero() || event.payout != target.payout
            || out.value != target.quantity || burn.value != target.quantity || burn.operator != route
            || paid.value != target.payout || incoming.value != target.payout
            || credited.replace((*log_index, *index_set)).is_some()
        {
            return Err(Conflict("REDEMPTION_BINDING"));
        }
    }
    Ok(credited)
}

impl PredictFunTrader {
    async fn native_claim_market(&self, owner: &RuntimeClaimInventory, route: &ApiMarket)
        -> ProofResult<(crate::service::venue::TradableMarket, NativeSettlementMarket)> {
        use crate::service::venue::MarketData;
        let (market, native) = match OFFICIAL_BATCH.try_with(|batch| batch.get(&owner.identity.market_id).cloned()) {
            Ok(Some(facts)) => facts,
            Ok(None) => return Err(Unavailable("OFFICIAL_BATCH")),
            Err(_) => {
                let (read, native) = capture_native_market(self.client.settlement_market(owner.identity.market_id)).await;
                let market = read.map_err(|error| {
                    let error = super::super::settlement_read_error(error);
                    match error.downcast::<VenueError>() {
                        Ok(error) => CashProofError::from(error),
                        Err(_) => Conflict("OFFICIAL_READ_INVALID"),
                    }
                })?;
                (market, native)
            }
        };
        let native = native.ok_or(Unavailable("OFFICIAL_NATIVE"))?;
        let [yes, no] = route.outcomes.as_slice() else { return Err(Conflict("LAG_ROUTE")); };
        if native.id != route.id || native.condition != route.condition_id
            || native.tokens != [yes.on_chain_id.clone(), no.on_chain_id.clone()]
            || native.neg_risk || native.yield_bearing
        { return Err(Conflict("OFFICIAL_ROUTE")); }
        Ok((market, native))
    }

    async fn prove_position_lag(&self, chain: &ChainRead<'_>, lag: &PositionLag,
        owner: &RuntimeClaimInventory, route: &ApiMarket, closed: &[RuntimeClaimInventory])
        -> ProofResult<(LagCashProof, Vec<crate::service::venue::TradableMarket>)> {
        let right = lag.anchor.as_ref().map_err(|_| Unavailable("LAG_ANCHOR"))?;
        let (market, native) = self.native_claim_market(owner, route).await?;
        let (left, target, fills, sampled, frozen) = LAG_TARGET.with(|make| make(lag, owner, &market, &native, false))?;
        let mut markets = vec![market];
        let mut claims = Vec::new();
        if lag.double_zero {
            for claim in &lag.observation.bound_claims {
                let id = claim.identity.local_attempt_id;
                if claim.identity == owner.identity || LAG_BOOKED.with(|booked| booked.contains(&id)) { continue; }
                let inventory = closed.iter().find(|item| item.identity == claim.identity)
                    .ok_or(Conflict("CLAIM_OWNER"))?;
                let hash = inventory.identity.signed_hash.as_deref().ok_or(Conflict("CLAIM_ORDER"))?;
                let route = self.bound_orders.read().get(hash).ok_or(Conflict("CLAIM_ORDER"))?.market.clone();
                let (market, native) = self.native_claim_market(inventory, &route).await?;
                let (_, binding, _, _, _) = LAG_TARGET.with(|make| make(lag, inventory, &market, &native, true))?;
                claims.push((id, binding));
                markets.push(market);
            }
        }
        // Only a target already proved in A may share C with one independent payout.
        let max_extra = usize::from(frozen.as_ref().is_some_and(|(_, paid)| paid.is_some()));
        let active = LAG_OWNER.try_with(|frozen| frozen.as_ref()
            .is_some_and(|frozen| frozen.identity == owner.identity)).unwrap_or(false);
        let cash = tokio::time::timeout(Duration::from_secs(20), r22_rpc_reply(active, async {
            let cash = chain.prove_cash(&left, right, &target, &fills, &claims, max_extra).await?;
            if let Some((a0, paid)) = &frozen {
                chain.cash_header(a0.number, [a0, &left]).await?;
                if let Some(leg) = paid {
                    let height = u64::try_from(leg.block_number).map_err(|_| Conflict("PAYOUT_HEIGHT"))?;
                    if chain.cash_header(height, [a0, &left]).await?.hash != leg.block_hash {
                        return Err(Conflict("PAYOUT_REORG"));
                    }
                }
                // Check A and the response block again after the prior segment.
                for end in [&left, right] { chain.cash_header(end.number, [&left, right]).await?; }
            }
            Ok(cash)
        })).await.map_err(|_| Unavailable("CENSUS_TIMEOUT"))??;
        if claims.iter().any(|(id, _)| !cash.claims.contains_key(id)) { return Err(Unavailable("CLAIM_PAYOUT")); }
        Ok((LagCashProof { owner: owner.clone(), left, target, cash, sampled }, markets))
    }

    /// Hold the read counter stable through synchronous local bookkeeping.
    pub(crate) fn commit_at_claim_read_generation(
        &self, closed: &[RuntimeClaimInventory], commit: &mut dyn FnMut() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let generations = self.generations.read();
        for inventory in closed {
            let hash = inventory.identity.signed_hash.as_deref()
                .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:UNBOUND_ORDER"))?;
            if generations.get(hash).copied() != Some(inventory.validated_read_generation) {
                anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_GENERATION");
            }
        }
        let committed = commit();
        drop(generations);
        committed
    }
    /// Read the original account legs once; runtime evidence never polls execution.
    pub(crate) async fn read_runtime_account_observation(
        &self,
        closed: Option<&[RuntimeClaimInventory]>,
    ) -> anyhow::Result<RuntimeAccountObservation> {
        use super::super::ApiAccount;
        use reqwest::Method;
        let identity = self.identity();
        let result: anyhow::Result<RuntimeAccountObservation> = async {
            // Positions populate the market cache used by declared-order history.
            // Keep other legs concurrent, and keep the original error priority.
            let position_history = async {
                let positions = self.read_positions().await;
                let history = if positions.is_ok() {
                    self.reconcile_account_history().await
                } else {
                    Ok(startup_history::HistoryRead::default())
                };
                (positions, history)
            };
            let ((positions, history), open_orders, account, chain) = tokio::join!(
                position_history,
                self.read_open_orders(),
                self.authed::<ApiAccount>(Method::GET, "/account", None),
                self.chain_read(),
            );
            let positions = positions?;
            let open_orders = open_orders?;
            let observed = account
                .map_err(|error| super::super::request::account_read_error(error, "ACCOUNT"))?
                .data.address;
            check_identity(&observed, &identity)?;
            let chain = chain?;
            let history = history?;
            let observed: HashMap<_, _> = positions.iter()
                .filter(|(_, amount)| !amount.is_zero()).cloned().collect();
            let (collateral_atoms, gas_atoms) = chain.balances().await?;
            let mut explanation = self.explain_positions_mode(
                &history.totals, &observed, collateral_atoms, closed.is_some(),
            ).await?;
            let inspect_lag = closed.is_some() && POSITION_LAG_READ.try_with(|_| ()).is_ok();
            let frozen_owner = LAG_OWNER.try_with(Clone::clone).ok().flatten();
            let active = frozen_owner.is_some();
            let proofs = positions.iter().map(|(token, amount)| {
                let chain = &chain;
                async move {
                    let market = self.client.market_for_token(token)?;
                    if inspect_lag {
                        let actual = r22_rpc_reply(active, chain.position_balance(&market, token)).await.map_err(|error| {
                            if error.code != VenueErrorCode::ReadUnavailable { return error; }
                            match CashProofError::from(error.clone()) {
                                CashProofError::Read(_) => error,
                                _ => VenueError::new(VenueErrorCode::FactIncomplete, &error.step, &error.reason),
                            }
                        })?;
                        if actual != *amount {
                            tracing::warn!(branch = "REST_CHAIN_QUANTITY", token, expected_atoms = %amount,
                                actual_atoms = %actual, block = %chain.block, "inventory reconciliation pending proof");
                            if !amount.is_zero() && actual.is_zero() && !market.is_neg_risk && !market.is_yield_bearing {
                                return Ok(Some((token.clone(), *amount, market)));
                            }
                            return Err(inventory_mismatch().into());
                        }
                    } else {
                        chain.position(&market, token, *amount).await?;
                    }
                    Ok::<_, anyhow::Error>(None)
                }
            });
            let mut lag = None;
            let mut unavailable = None;
            // All of these proofs have already returned. A confirmed conflict or
            // a second discrepancy wins over an incomplete read in either order.
            for proof in futures_util::future::join_all(proofs).await {
                match proof {
                    Ok(Some(found)) if lag.is_none() => lag = Some(found),
                    Ok(None) => (),
                    Ok(Some(_)) => return Err(inventory_mismatch().into()),
                    Err(error) if inspect_lag && error.downcast_ref::<VenueError>()
                        .is_some_and(|error| error.code == VenueErrorCode::ReadUnavailable) => {
                        unavailable.get_or_insert(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            if let Some(error) = unavailable {
                if let Some((token, amount, market)) = &lag {
                    if !open_orders.is_empty() || !frozen_owner.as_ref().is_some_and(|owner| {
                        &owner.identity.token_id == token && owner.inventory_atoms == *amount
                            && owner.identity.market_id == market.id
                    }) {
                        return Err(inventory_mismatch().into());
                    }
                }
                return Err(error);
            }
            let mut bound_claims = Vec::new();
            let mut double_zero = None;
            for inventory in closed.unwrap_or_default() {
                if inventory.identity.account != identity {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_IDENTITY");
                }
                if observed.contains_key(&inventory.identity.token_id) { continue; }
                let hash = inventory.identity.signed_hash.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:UNBOUND_ORDER"))?;
                let bound = self.bound_orders.read().get(hash).cloned()
                    .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:UNBOUND_ORDER"))?;
                let record = bound.handle.lock().clone();
                if record.identity != inventory.identity || record.phase != AttemptPhase::Closed
                    || self.generations.read().get(hash).copied() != Some(inventory.validated_read_generation)
                {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_GENERATION");
                }
                // REST can hide a losing outcome without burning its ERC-1155 token.
                // Read once at the original pinned block; only exact or zero can be explained.
                let chain_inventory = chain.position_balance(&bound.market, &inventory.identity.token_id).await?;
                if frozen_owner.as_ref().is_some_and(|owner| owner.identity.token_id == inventory.identity.token_id) {
                    if !chain_inventory.is_zero() || lag.is_some() || double_zero.is_some() {
                        return Err(inventory_mismatch().into());
                    }
                    double_zero = Some((inventory.identity.token_id.clone(), inventory.inventory_atoms, bound.market.clone()));
                }
                if !chain_inventory.is_zero() && chain_inventory != inventory.inventory_atoms {
                    tracing::warn!(branch = "CLOSED_CHAIN_QUANTITY", token = %inventory.identity.token_id,
                        expected_atoms = %inventory.inventory_atoms, actual_atoms = %chain_inventory,
                        block = %chain.block, "inventory reconciliation refused");
                    return Err(VenueError::new(VenueErrorCode::ReconciliationRequired, "positions", "INVENTORY_MISMATCH").into());
                }
                let Some(settlement) = self.official_settlement(&inventory.identity.token_id).await? else {
                    if !chain_inventory.is_zero() {
                        tracing::warn!(branch = "SETTLEMENT_UNAVAILABLE", token = %inventory.identity.token_id,
                            expected_atoms = %inventory.inventory_atoms, actual_atoms = %chain_inventory,
                            block = %chain.block, "inventory reconciliation refused");
                        return Err(VenueError::new(VenueErrorCode::ReconciliationRequired, "positions", "INVENTORY_MISMATCH").into());
                    }
                    continue;
                };
                let yes = bound.market.outcomes.first()
                    .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_TOKEN"))?;
                let no = bound.market.outcomes.get(1)
                    .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_TOKEN"))?;
                if settlement.market_id != inventory.identity.market_id
                    || bound.market.outcomes.len() != 2 || yes.on_chain_id == no.on_chain_id
                    || ![&yes.on_chain_id, &no.on_chain_id].contains(&&inventory.identity.token_id)
                    || self.generations.read().get(hash).copied() != Some(inventory.validated_read_generation)
                    || bound.handle.lock().identity != inventory.identity
                    || bound.handle.lock().phase != AttemptPhase::Closed
                {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_GENERATION");
                }
                if settlement.payout == 0.0 {
                    explanation.settled_zero.push(settlement.settled_zero(&inventory.identity.token_id));
                    continue;
                }
                if !chain_inventory.is_zero() {
                    tracing::warn!(branch = "POSITIVE_PAYOUT_UNBURNED", token = %inventory.identity.token_id,
                        expected_atoms = %inventory.inventory_atoms, actual_atoms = %chain_inventory,
                        block = %chain.block, "inventory reconciliation refused");
                    return Err(VenueError::new(VenueErrorCode::ReconciliationRequired, "positions", "INVENTORY_MISMATCH").into());
                }
                let Some(claim) = settlement.claimed_win(
                    &inventory.identity.token_id, inventory.inventory_atoms,
                ) else { continue; };
                bound_claims.push(BoundClaimEvidence {
                    identity: inventory.identity.clone(),
                    execution_generation: inventory.execution_generation,
                    yes_token: yes.on_chain_id.clone(), no_token: no.on_chain_id.clone(),
                    inventory_atoms: inventory.inventory_atoms,
                    claim_atoms: claim.claim_atoms, payout_millionths: claim.payout_millionths,
                });
            }
            // Recheck the whole batch: an earlier owner must not race a later await.
            self.commit_at_claim_read_generation(closed.unwrap_or_default(), &mut || Ok(()))?;
            let observation = RuntimeAccountObservation {
                snapshot: AccountSnapshot {
                    identity: identity.clone(), collateral_atoms: Some(collateral_atoms),
                    gas_atoms: Some(gas_atoms), positions,
                    positions_explained: explanation.explained,
                    settled_zero: explanation.settled_zero, claimed_win: explanation.claimed_win,
                    open_orders, complete: true,
                },
                bound_claims,
            };
            let is_double_zero = double_zero.is_some();
            if let Some((token, quantity, route)) = lag.or(double_zero) {
                if !observation.snapshot.open_orders.is_empty() { return Err(inventory_mismatch().into()); }
                let mut lag = PositionLag { token, quantity, anchor: chain.cash_anchor(collateral_atoms),
                    proof: None, double_zero: is_double_zero, observation };
                let mut owners = closed.unwrap_or_default().iter().filter(|item| item.identity.token_id == lag.token);
                if LAG_TARGET.try_with(|_| ()).is_ok() {
                    if let Some(owner) = owners.next().filter(|owner| owners.next().is_none()
                        && owner.identity.market_id == route.id && owner.inventory_atoms == lag.quantity) {
                        lag.proof = Some(self.prove_position_lag(&chain, &lag, owner, &route, closed.unwrap_or_default()).await);
                        self.commit_at_claim_read_generation(closed.unwrap_or_default(), &mut || Ok(()))?;
                    }
                }
                return Err(lag.into());
            }
            if closed.is_none() {
                self.stage_history(&observation.snapshot, &history, explanation.official)?;
            } else if let Ok(probing) = CASH_READ.try_with(|slot| slot.borrow().request.is_some()) {
                if probing {
                    let evidence = chain.observe_cash(collateral_atoms).await;
                    self.commit_at_claim_read_generation(closed.unwrap_or_default(), &mut || Ok(()))?;
                    CASH_READ.with(|slot| slot.borrow_mut().evidence = Some(evidence));
                    // An observation carrying an R22 proof is not an account success.
                    return Err(CashObservationPending.into());
                }
                // The normal read already fenced the full observation. No proof read or adapter state.
                CASH_READ.with(|slot| {
                    slot.borrow_mut().evidence = Some(chain.cash_anchor(collateral_atoms).map(|anchor| (anchor, None)));
                });
            }
            Ok(observation)
        }.await.map_err(|error| {
            if error.is::<CashObservationPending>() || error.is::<PositionLag>() { return error; }
            // Auxiliary official-settlement reads belong to this account
            // observation too; preserve typed causes before the public boundary.
            super::super::request::account_read_error(error, "RUNTIME_ACCOUNT")
        });
        match &result {
            Ok(_) => self.gate.record_read_success(&identity, "snapshot"),
            Err(error) if error.is::<CashObservationPending>() || error.is::<PositionLag>() => {},
            Err(error) => {
                self.discard_pending_history();
                if let Some(error) = error.downcast_ref::<VenueError>()
                    .filter(|error| error.code == VenueErrorCode::AccountMismatch)
                {
                    self.gate.record_mismatch(&identity, &error.reason);
                } else {
                    self.gate.record_read_failure(&identity, "snapshot", "READ_FAILED");
                }
            }
        }
        result
    }
}

#[cfg(test)]
pub(crate) mod cash_census_tests {
    use super::*;
    use crate::service::predict_fun::controlled::r16_fixture::{self, BatchNull, RedemptionFixture,
        RedemptionStage::*, REDEMPTION_ATOMS};
    use crate::service::venue::OrderVenue;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn r22_adapter_cash_census_requires_both_complete_log_reads() {
        let (trader, transport, _, facts) = r16_fixture::trader();
        *facts.redemption.lock().unwrap() = Some(RedemptionFixture::new(Before));
        let (read, proof) = capture_cash_evidence(None, trader.runtime_account_observation(&[])).await;
        let (left, before) = proof.unwrap().unwrap();
        assert_eq!(read.unwrap().snapshot.collateral_atoms, Some(left.collateral));
        assert!(before.is_none());
        let atoms = U256::from(REDEMPTION_ATOMS);
        *facts.cash_offset.lock().unwrap() = i128::from(REDEMPTION_ATOMS);
        for stage in [Complete, ShortBatch, ExtraLegs, ShortWithExtraLegs, Foreign] {
            let script = RedemptionFixture::new(stage);
            let target = CashTarget { condition: script.condition, token: script.token, quantity: atoms, payout: atoms };
            *facts.redemption.lock().unwrap() = Some(script);
            facts.rpc_calls.lock().unwrap().clear();
            let (read, proof) = capture_cash_evidence(Some((left.clone(), target, Vec::new())),
                trader.runtime_account_observation(&[])).await;
            assert!(read.unwrap_err().is::<CashObservationPending>());
            let result = proof.unwrap();
            match stage {
                Complete => {
                    let (right, proof) = result.unwrap();
                    assert_eq!(right.collateral - left.collateral, atoms);
                    let proof = proof.unwrap();
                    assert_eq!((proof.other_in, proof.other_out), (U256::ZERO, U256::ZERO));
                    let leg = proof.payout.unwrap();
                    assert_eq!((leg.log_index, leg.atoms, leg.block_hash, leg.incoming), (U256::from(4), atoms, right.hash, true));
                }
                ShortBatch => assert_eq!(result.unwrap_err(), CashProofError::Unavailable("RECEIPT_CENSUS_SHORT")),
                ExtraLegs | ShortWithExtraLegs | Foreign => assert_eq!(result.unwrap_err(), CashProofError::Conflict("UNATTRIBUTED_CASH")),
                Before | After | WrongParent => unreachable!(),
            }
            assert_cash_receipt_reads(&facts, 1);
        }
        // A null slot prevents a proof, but not a conflict independently
        // established by a valid receipt in that same batch.
        for (stage, batch_null, expected) in [
            (Complete, BatchNull::Trailing, CashProofError::Unavailable("RECEIPT_CENSUS_SHORT")),
            (Complete, BatchNull::Only, CashProofError::Unavailable("RECEIPT_CENSUS_SHORT")),
            (ExtraLegs, BatchNull::Trailing, CashProofError::Conflict("UNATTRIBUTED_CASH")),
        ] {
            let mut script = RedemptionFixture::new(stage);
            script.batch_null = batch_null;
            let target = CashTarget { condition: script.condition, token: script.token, quantity: atoms, payout: atoms };
            *facts.redemption.lock().unwrap() = Some(script);
            facts.rpc_calls.lock().unwrap().clear();
            let (read, proof) = capture_cash_evidence(Some((left.clone(), target, Vec::new())),
                trader.runtime_account_observation(&[])).await;
            assert!(read.unwrap_err().is::<CashObservationPending>());
            assert_eq!(proof.unwrap().unwrap_err(), expected, "{batch_null:?}");
            assert_cash_receipt_reads(&facts, 1);
        }
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    }

    // Assert the adapter evidence before the drive test checks the Broker disposition.
    pub(crate) fn assert_lag_cash_evidence(error: anyhow::Error, fault: Option<&str>,
        left: &CashAnchor, closed: &RuntimeClaimInventory) {
        if fault == Some("generation") {
            assert!(!error.is::<PositionLag>());
            return;
        }
        let lag = error.downcast::<PositionLag>().unwrap();
        let proof = lag.proof.unwrap();
        match fault {
            None => {
                let (proof, _) = proof.unwrap();
                assert_eq!(&proof.left, left);
                assert_eq!(&proof.owner, closed);
                let paid = proof.cash.payout.unwrap();
                assert_eq!(paid.atoms, closed.inventory_atoms);
                assert_eq!(paid.block_hash, lag.anchor.unwrap().hash);
            }
            Some("condition" | "token") => assert!(matches!(proof, Err(Conflict(_)))),
            Some("route") => assert_eq!(proof.unwrap_err(), Conflict("OFFICIAL_ROUTE")),
            Some("block") => assert_eq!(proof.unwrap_err(), Conflict("HEADER_PARENT")),
            Some("foreign") => assert_eq!(proof.unwrap_err(), Conflict("UNATTRIBUTED_CASH")),
            _ => unreachable!(),
        }
    }

    pub(crate) async fn assert_native_market_binding(client: &crate::service::predict_fun::PredictFunClient) {
        use crate::service::venue::MarketData;
        let (official, native) = capture_native_market(client.settlement_market(1)).await;
        let official = official.unwrap();
        let native = native.unwrap();
        assert_eq!(native.id, official.id);
        assert_eq!(native.tokens, [official.yes_token, official.no_token]);
        assert!(!native.neg_risk && !native.yield_bearing);
    }

    fn assert_cash_receipt_reads(facts: &r16_fixture::Facts, count: usize) {
        let calls = facts.rpc_calls.lock().unwrap();
        for method in ["eth_getBlockReceipts", "eth_getTransactionReceipt"] {
            assert_eq!(calls.iter().filter(|call| call.as_str() == method).count(), count, "{method}");
        }
    }

    #[tokio::test]
    async fn r22_c_census_rejects_two_independently_proven_extra_payouts() {
        let (trader, transport, _, facts) = r16_fixture::trader();
        let payout = |condition, token: u64, atoms: u64| CashTarget {
            condition: B256::repeat_byte(condition), token: U256::from(token),
            quantity: U256::from(atoms), payout: U256::from(atoms),
        };
        // The target was paid before C. Only the two independent read-side
        // bindings below can appear in (A,C]; no market or order setup is needed.
        let target = payout(0xcc, 99, REDEMPTION_ATOMS);
        let extras = [(11, payout(0x41, 77, 700)), (12, payout(0xdd, 177, 900))];
        let canonical = [(0x125u64, 0xa2, 0xa0), (0x126, 0xa4, 0xa5)];
        *facts.redemption.lock().unwrap() = Some(RedemptionFixture::new(Before));
        let chain = trader.chain_read().await.unwrap();
        let left = chain.cash_anchor(chain.balances().await.unwrap().0).unwrap();
        assert_eq!(left.number, 0x124);

        for active in [&extras[..1], &extras[1..], &extras[..]] {
            let payment = |id| active.iter().find(|(owner, _)| *owner == id).map(|(_, proof)| proof.payout);
            let delta: u64 = active.iter().map(|(_, proof)| proof.payout.to::<u64>()).sum();
            *facts.cash_offset.lock().unwrap() = i128::from(delta);
            *facts.redemption.lock().unwrap() = Some(RedemptionFixture::c_census(payment(11), payment(12)));
            let chain = trader.chain_read().await.unwrap();
            let right = chain.cash_anchor(chain.balances().await.unwrap().0).unwrap();
            assert_eq!(right.number, 0x126);
            assert_eq!(right.collateral - left.collateral, U256::from(delta));
            facts.rpc_calls.lock().unwrap().clear();
            let proof = chain.prove_cash(&left, &right, &target, &[], active, 1).await;
            assert_cash_receipt_reads(&facts, active.len());
            if let [(id, extra)] = active {
                let proof = proof.unwrap();
                assert!(proof.payout.is_none());
                assert_eq!((proof.other_in, proof.other_out), (extra.payout, U256::ZERO));
                let (height, hash, tx) = canonical[usize::from(*id == 12)];
                assert_eq!(proof.claims.len(), 1);
                assert_eq!(proof.claims[id], FillCashLeg {
                    tx: B256::repeat_byte(tx), log_index: U256::from(4), block_number: U256::from(height),
                    block_hash: B256::repeat_byte(hash), incoming: true, atoms: extra.payout,
                });
            } else {
                assert_eq!(proof.unwrap_err(), Conflict("R22_CLAIM_SCOPE"));
            }
        }
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
    }
}
