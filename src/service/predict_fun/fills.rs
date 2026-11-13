//! Reading what a Predict.fun order actually did.
//!
//! The venue reports fills as a list of match events, each of which may hold
//! several legs. This module turns those into the account's own legs, adds up
//! the gross quantity and the cash, and keeps the accounting honest: a missing
//! fee is unknown, not zero, and a quantity is only "full" when the venue's own
//! cumulative amount agrees with the legs.

use super::{decimal_to_wei_exact, wei_to_f64, ApiMatch, ApiOrderData, WEI};
use crate::models::Side;
use crate::service::venue::{Accounting, Closure, Field, QuantityClass, Submission};
use alloy_primitives::{U256, U512};
use anyhow::{anyhow, bail, Result};

use super::{ApiContractOrder, ApiMarket, Envelope, PredictFunTrader};
use crate::service::venue::{
    AttemptHandle, ExecutionSnapshot, OrderIdentity, Quantity, VenueError, VenueErrorCode,
};
use alloy_primitives::B256;
use reqwest::Method;
use serde_json::Value;
use std::collections::HashSet;

use super::{
    account, check_limit, f64_to_wei, market_amounts, order_body, ApiCreatedOrder,
    PredictFunClient, WeiBook, MARKET_ORDER_TTL_SECS,
};
use crate::models::{PlannedOrder, VenueId};
use crate::service::venue::{AccountSnapshot, AttemptPhase, OrderTicket, OrderVenue};
use async_trait::async_trait;
use tracing::{info, warn};

/// Multiply atom amounts without truncating to U256 before division.
pub(super) fn checked_product_atoms(price: U256, quantity: U256) -> Result<U256> {
    let product = U512::from(price)
        .checked_mul(U512::from(quantity))
        .ok_or_else(|| anyhow!("AMOUNT_OVERFLOW"))?;
    U256::from_str_radix(&(product / U512::from(WEI)).to_string(), 10)
        .map_err(|_| anyhow!("AMOUNT_OVERFLOW"))
}

fn ceil_div(value: U512, divisor: U512) -> Result<U512> {
    let (whole, remainder) = value.div_rem(divisor);
    whole
        .checked_add(U512::from(!remainder.is_zero() as u8))
        .ok_or_else(|| anyhow!("RESERVATION_OVERFLOW"))
}

fn cash_with_fee(cash: U512, bps: u32) -> Result<U256> {
    let fee = ceil_div(
        cash.checked_mul(U512::from(bps))
            .ok_or_else(|| anyhow!("RESERVATION_OVERFLOW"))?,
        U512::from(10000),
    )?;
    let total = cash
        .checked_add(fee)
        .ok_or_else(|| anyhow!("RESERVATION_OVERFLOW"))?;
    U256::from_str_radix(&total.to_string(), 10).map_err(|_| anyhow!("RESERVATION_OVERFLOW"))
}

#[async_trait]
impl OrderVenue for PredictFunTrader {
    fn id(&self) -> VenueId {
        VenueId::PredictFun
    }

    fn reservation_atoms(&self, planned: &PlannedOrder) -> Result<U256> {
        if planned.side == Side::Sell {
            return Ok(U256::ZERO);
        }
        let market = self.client.market_for_token(&planned.token_id)?;
        let product = U512::from(f64_to_wei(planned.shares)?)
            .checked_mul(U512::from(f64_to_wei(planned.limit_price)?))
            .ok_or_else(|| anyhow!("RESERVATION_OVERFLOW"))?;
        let cash = ceil_div(product, U512::from(WEI))?;
        cash_with_fee(cash, market.fee_rate_bps)
    }

    async fn place(&self, planned: &PlannedOrder, _: u64, live: bool) -> Result<OrderTicket> {
        if live {
            bail!("EXECUTION_NOT_ENABLED:TRACKED_ATTEMPT_REQUIRED");
        }
        self.place_tracked(planned, 0, false, new_untracked_handle())
            .await
    }

    async fn place_tracked(
        &self,
        planned: &PlannedOrder,
        _: u64,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<OrderTicket> {
        let result = async {
            if live {
                self.check_allows_new_order()?;
                self.begin_attempt(&handle)?;
                let snapshot = self.read_account_snapshot().await?;
                account::check_snapshot_complete(&snapshot)?;
            }
            let market = self.client.market_for_token(&planned.token_id)?;
            let mut quantity = f64_to_wei(planned.shares)?;
            if live && planned.side == Side::Sell {
                let held = self.held_wei(market.id, &planned.token_id).await?;
                if held.is_zero() {
                    info!(token = %planned.token_id,"nothing held to sell");
                    return Ok(OrderTicket {
                        token_id: planned.token_id.clone(),
                        maker_amount: "0".into(),
                        taker_amount: "0".into(),
                        order_id: None,
                        filled_shares: Some(0.0),
                        signed_hash: None,
                        local_attempt_id: None,
                        execution: None,
                    });
                }
                quantity = quantity.min(held);
            }
            let mirrored = PredictFunClient::is_mirrored(&market, &planned.token_id);
            let book = WeiBook::from_api(&self.client.book(market.id).await?, mirrored)?;
            let amounts = market_amounts(&book, planned.side, quantity)?;
            check_limit(&amounts, planned.side, planned.limit_price)?;
            let expiration = chrono::Utc::now().timestamp() + MARKET_ORDER_TTL_SECS;
            let order = self.build_order(
                &market,
                &planned.token_id,
                planned.side,
                &amounts,
                expiration,
            )?;
            if live {
                let read = self.chain_read().await?;
                let (collateral, gas) = read.balances().await?;
                // A buy's cash bound is the signed cash plus the venue's cash
                // fee, so the balance and the allowance must cover that bound.
                let required = match planned.side {
                    Side::Buy => {
                        cash_with_fee(U512::from(amounts.maker_amount), market.fee_rate_bps)?
                    }
                    Side::Sell => amounts.quantity,
                };
                if gas.is_zero() || (planned.side == Side::Buy && collateral < required) {
                    return Err(VenueError::new(
                        VenueErrorCode::ReadUnavailable,
                        "funds",
                        "INSUFFICIENT_FUNDS",
                    )
                    .into());
                }
                read.authorize(&market, order.tokenId, planned.side, required)
                    .await?;
            }
            if live {
                self.check_signed_bound(&handle, &order)?;
            }
            let (digest, signature) = self.sign_order(&order, &market).await?;
            let order_hash = format!("0x{}", hex::encode(digest));
            // The consumer's shared handle retains identity before any possible send.
            {
                let mut record = handle.lock();
                record.identity.signed_hash = Some(order_hash.clone());
                record.identity.market_id = market.id;
                record.identity.token_id = planned.token_id.clone();
                record.identity.side = planned.side;
                record.identity.account = self.identity();
                record.request_gross_atoms = amounts.quantity;
                record.phase = AttemptPhase::Prepared;
            }
            self.bound_orders.write().insert(
                order_hash.clone(),
                BoundOrder {
                    handle: handle.clone(),
                    market: market.clone(),
                    requested: amounts.quantity,
                    maker_amount: amounts.maker_amount,
                    taker_amount: amounts.taker_amount,
                    expiration,
                },
            );
            let mut ticket = OrderTicket {
                token_id: planned.token_id.clone(),
                maker_amount: amounts.maker_amount.to_string(),
                taker_amount: amounts.taker_amount.to_string(),
                order_id: None,
                filled_shares: None,
                signed_hash: Some(order_hash.clone()),
                local_attempt_id: Some(handle.lock().identity.local_attempt_id),
                execution: None,
            };
            if !live {
                return Ok(ticket);
            }
            let attempt_id = handle.lock().identity.local_attempt_id;
            if attempt_id != 0 {
                self.gate.reserve(&self.identity(), attempt_id)?;
            }
            self.token().await?;
            self.chain_read()
                .await?
                .signature(digest, &signature)
                .await?;
            handle.lock().phase = AttemptPhase::MayHaveSubmitted;
            self.gate.mark_submitted(&self.identity(), attempt_id);
            let body = order_body(&order, &order_hash, &signature, &amounts, expiration);
            // POST is never retried, including timeout, malformed body and 401.
            let created = match self
                .authed_write::<ApiCreatedOrder>(Method::POST, "/orders", Some(&body))
                .await
            {
                Ok(envelope) => envelope.data,
                Err(error) => {
                    {
                        let mut record = handle.lock();
                        record.submission = Submission::Unknown;
                        record.last_reason = Some("POST_NOT_REPLAYED".into());
                    }
                    self.gate.mark_submitted(&self.identity(), attempt_id);
                    return Err(error);
                }
            };
            ticket.order_id = Some(created.order_id.clone());
            {
                let mut record = handle.lock();
                record.submission = Submission::Accepted;
                record.phase = AttemptPhase::Submitted;
                record.identity.venue_order_id = Some(created.order_id);
            }
            match self
                .reconcile_order(&order_hash, planned.side, amounts.quantity)
                .await
            {
                Ok(snapshot) => {
                    ticket.filled_shares = snapshot.gross_q().map(wei_to_f64);
                    ticket.execution = Some(snapshot);
                }
                Err(error) => {
                    warn!(error = ?error,order_hash,"couldn't read the order's execution facts")
                }
            }
            Ok(ticket)
        }
        .await;
        if live && result.is_err() {
            self.release_prepared(&handle);
        }
        result
    }

    async fn execution_snapshot(&self, signed_hash: &str) -> Result<ExecutionSnapshot> {
        let snapshot = self.read_bound_execution(signed_hash).await?;
        if snapshot.pages_complete && snapshot.accounting_complete() {
            self.gate
                .record_read_success(&snapshot.identity.account, "execution");
        }
        if snapshot.pages_complete
            && snapshot.accounting_complete()
            && matches!(snapshot.closure,Closure::Final { final_gross_q,.. } if Some(final_gross_q) == snapshot.gross_q())
        {
            self.gate.resolve(
                &snapshot.identity.account,
                snapshot.identity.local_attempt_id,
                AttemptPhase::Closed,
            );
        }
        Ok(snapshot)
    }

    fn record_account_reconciliation(&self, reason: &str) {
        self.gate.record_reconciliation(&self.identity(), reason);
    }

    async fn account_snapshot(&self) -> Result<AccountSnapshot> {
        self.read_account_snapshot().await
    }
}

use super::{limit_amounts, limit_order_body, tradable, ApiRemoved, ReplayPolicy};
use crate::service::venue::{
    HeldPosition, LimitOrder, LimitVenue, PostedOrder, RemoveResult, RestingState,
};
use anyhow::Context;
use serde_json::json;

#[async_trait]
impl LimitVenue for PredictFunTrader {
    async fn post_limit(&self, order: &LimitOrder, live: bool) -> Result<PostedOrder> {
        if live {
            bail!("EXECUTION_NOT_ENABLED:TRACKED_ATTEMPT_REQUIRED");
        }
        self.post_limit_tracked(order, false, new_untracked_handle())
            .await
    }

    async fn post_limit_tracked(
        &self,
        order: &LimitOrder,
        live: bool,
        handle: AttemptHandle,
    ) -> Result<PostedOrder> {
        let result = async {
            if live {
                self.check_allows_new_order()?;
                self.begin_attempt(&handle)?;
                let snapshot = self.read_account_snapshot().await?;
                account::check_snapshot_complete(&snapshot)?;
            }
            let market = self.client.market_for_token(&order.token_id)?;
            let amounts = limit_amounts(
                order.side,
                f64_to_wei(order.price)?,
                f64_to_wei(order.shares)?,
            )?;
            let expiration = chrono::Utc::now().timestamp() + order.ttl_secs as i64;
            let signed =
                self.build_order(&market, &order.token_id, order.side, &amounts, expiration)?;
            if live {
                let read = self.chain_read().await?;
                let (collateral, gas) = read.balances().await?;
                // Same bound as the market path: the signed cash plus the fee.
                let required = match order.side {
                    Side::Buy => {
                        cash_with_fee(U512::from(amounts.maker_amount), market.fee_rate_bps)?
                    }
                    Side::Sell => amounts.quantity,
                };
                if gas.is_zero() || (order.side == Side::Buy && collateral < required) {
                    return Err(VenueError::new(
                        VenueErrorCode::ReadUnavailable,
                        "funds",
                        "INSUFFICIENT_FUNDS",
                    )
                    .into());
                }
                read.authorize(&market, signed.tokenId, order.side, required)
                    .await?;
            }
            if live {
                self.check_signed_bound(&handle, &signed)?;
            }
            let (digest, signature) = self.sign_order(&signed, &market).await?;
            let order_hash = format!("0x{}", hex::encode(digest));
            {
                let mut record = handle.lock();
                record.identity.signed_hash = Some(order_hash.clone());
                record.identity.market_id = market.id;
                record.identity.token_id = order.token_id.clone();
                record.identity.side = order.side;
                record.identity.account = self.identity();
                record.request_gross_atoms = amounts.quantity;
                record.phase = AttemptPhase::Prepared;
            }
            self.bound_orders.write().insert(
                order_hash.clone(),
                BoundOrder {
                    handle: handle.clone(),
                    market: market.clone(),
                    requested: amounts.quantity,
                    maker_amount: amounts.maker_amount,
                    taker_amount: amounts.taker_amount,
                    expiration,
                },
            );
            if !live {
                return Ok(PostedOrder {
                    order_hash,
                    order_id: None,
                    local_attempt_id: Some(handle.lock().identity.local_attempt_id),
                    execution: None,
                });
            }
            let attempt_id = handle.lock().identity.local_attempt_id;
            self.gate.reserve(&self.identity(), attempt_id)?;
            self.token().await?;
            self.chain_read()
                .await?
                .signature(digest, &signature)
                .await?;
            handle.lock().phase = AttemptPhase::MayHaveSubmitted;
            self.gate.mark_submitted(&self.identity(), attempt_id);
            let body = limit_order_body(
                &signed,
                &order_hash,
                &signature,
                &amounts,
                expiration,
                order.post_only,
            );
            let created = match self
                .authed_write::<ApiCreatedOrder>(Method::POST, "/orders", Some(&body))
                .await
            {
                Ok(envelope) => envelope.data,
                Err(error) => {
                    {
                        let mut record = handle.lock();
                        record.submission = Submission::Unknown;
                        record.last_reason = Some("POST_NOT_REPLAYED".into());
                    }
                    self.gate.mark_submitted(&self.identity(), attempt_id);
                    return Err(error);
                }
            };
            {
                let mut record = handle.lock();
                record.submission = Submission::Accepted;
                record.phase = AttemptPhase::Submitted;
                record.identity.venue_order_id = Some(created.order_id.clone());
            }
            Ok(PostedOrder {
                order_hash,
                order_id: Some(created.order_id),
                local_attempt_id: Some(handle.lock().identity.local_attempt_id),
                execution: None,
            })
        }
        .await;
        if live && result.is_err() {
            self.release_prepared(&handle);
        }
        result
    }

    async fn remove(&self, order_ids: &[String]) -> Result<RemoveResult> {
        let body = json!({"data":{"ids":order_ids}});
        let text = self
            .authed_text(
                Method::POST,
                "/orders/remove",
                Some(&body),
                ReplayPolicy::NoReplay,
            )
            .await?;
        let removed: ApiRemoved =
            serde_json::from_str(&text).context("parsing POST /orders/remove response")?;
        Ok(RemoveResult {
            removed: removed.removed,
            noop: removed.noop,
            rejected: removed.rejected,
        })
    }

    async fn order_state(&self, hash: &str) -> Result<RestingState> {
        let snapshot = self.read_bound_execution(hash).await?;
        Ok(RestingState {
            open: !matches!(snapshot.closure, Closure::Final { .. }),
            filled_shares: snapshot.gross_q().map(wei_to_f64).unwrap_or(0.0),
            execution: Some(snapshot),
        })
    }

    async fn positions(&self) -> Result<Vec<HeldPosition>> {
        self.read_positions()
            .await?
            .into_iter()
            .map(|(token, amount)| {
                let market = self.client.market_for_token(&token)?;
                Ok(HeldPosition {
                    market: tradable(&market, None, chrono::Utc::now()),
                    token_id: token,
                    shares: wei_to_f64(amount),
                    avg_price: 1.0,
                })
            })
            .collect()
    }
}

use super::{order_digest, sign_for_account, MarketAmounts, Order, MAX_SALT};
use alloy_primitives::Address;
use alloy_signer::Signer;

impl PredictFunTrader {
    fn release_prepared(&self, handle: &AttemptHandle) {
        let record = handle.lock();
        if record.phase == AttemptPhase::Prepared && record.submission == Submission::NotSubmitted {
            self.gate
                .release(&record.identity.account, record.identity.local_attempt_id);
        }
    }

    fn check_signed_bound(&self, handle: &AttemptHandle, order: &Order) -> Result<()> {
        if order.side == Side::Buy.as_u8() {
            let bps: u32 = order
                .feeRateBps
                .try_into()
                .map_err(|_| anyhow!("RESERVATION_OVERFLOW"))?;
            let worst = cash_with_fee(U512::from(order.makerAmount), bps)?;
            if worst > handle.lock().r0 {
                bail!("SIGNED_RISK_EXCEEDS_RESERVATION");
            }
        }
        Ok(())
    }

    fn begin_attempt(&self, handle: &AttemptHandle) -> Result<()> {
        let mut record = handle.lock();
        if record.identity.local_attempt_id == 0
            || record.identity.signed_hash.is_some()
            || record.phase != AttemptPhase::Prepared
            || record.submission != Submission::NotSubmitted
        {
            bail!("RECONCILIATION_REQUIRED:ATTEMPT_ALREADY_USED_OR_UNTRACKED");
        }
        record.identity.account = self.identity();
        self.gate
            .reserve(&record.identity.account, record.identity.local_attempt_id)?;
        Ok(())
    }

    fn build_order(
        &self,
        market: &ApiMarket,
        token_id: &str,
        side: Side,
        amounts: &MarketAmounts,
        expiration: i64,
    ) -> Result<Order> {
        let maker = self.trading_address();
        Ok(Order {
            salt: U256::from(rand::random::<u32>() % MAX_SALT),
            maker,
            signer: maker,
            taker: Address::ZERO,
            tokenId: U256::from_str_radix(token_id, 10)
                .map_err(|_| anyhow!("token id must be a decimal U256"))?,
            makerAmount: amounts.maker_amount,
            takerAmount: amounts.taker_amount,
            expiration: U256::from(expiration),
            nonce: U256::ZERO,
            feeRateBps: U256::from(market.fee_rate_bps),
            side: side.as_u8(),
            signatureType: 0,
        })
    }

    async fn sign_order(&self, order: &Order, market: &ApiMarket) -> Result<(B256, String)> {
        #[cfg(test)]
        if let Some(transport) = &self.client.synthetic {
            return transport.sign(order);
        }
        let digest = order_digest(
            order,
            self.client.deployment,
            market.is_neg_risk,
            market.is_yield_bearing,
        );
        let signature = match self.account {
            Some(account) => {
                sign_for_account(
                    &self.signer,
                    digest,
                    account,
                    self.client.deployment.chain_id,
                )
                .await?
            }
            None => {
                let signature = self
                    .signer
                    .sign_hash(&digest)
                    .await
                    .context("signing order digest")?;
                format!("0x{}", hex::encode(signature.as_bytes()))
            }
        };
        Ok((digest, signature))
    }
}

use crate::service::venue::{AccountIdentity, AccountMode, AttemptRecord};
use std::sync::Arc;

/// Identity-free handles exist only on the non-live construction path.
fn new_untracked_handle() -> AttemptHandle {
    Arc::new(parking_lot::Mutex::new(AttemptRecord {
        identity: OrderIdentity {
            local_attempt_id: 0,
            signed_hash: None,
            venue_order_id: None,
            market_id: 0,
            token_id: String::new(),
            side: Side::Buy,
            account: AccountIdentity {
                chain_id: 0,
                trading_address: String::new(),
                mode: AccountMode::Eoa,
                signer_address: String::new(),
                venue: VenueId::PredictFun,
            },
        },
        request_gross_atoms: U256::ZERO,
        r0: U256::ZERO,
        reserved_sell: U256::ZERO,
        phase: AttemptPhase::Prepared,
        submission: Submission::NotSubmitted,
        last_reason: None,
    }))
}

impl PredictFunTrader {
    /// Net held inventory is read through every position page.
    async fn held_wei(&self, _: u64, token_id: &str) -> Result<U256> {
        self.read_positions()
            .await?
            .into_iter()
            .filter(|(token, _)| token == token_id)
            .try_fold(U256::ZERO, |sum, (_, amount)| {
                sum.checked_add(amount)
                    .ok_or_else(|| anyhow!("POSITION_OVERFLOW"))
            })
    }

    async fn reconcile_order(
        &self,
        hash: &str,
        side: Side,
        requested: U256,
    ) -> Result<ExecutionSnapshot> {
        let bound = self
            .bound_orders
            .read()
            .get(hash)
            .cloned()
            .ok_or_else(|| anyhow!("RECONCILIATION_REQUIRED:UNBOUND_ORDER"))?;
        if bound.requested != requested || bound.handle.lock().identity.side != side {
            bail!("ACCOUNT_MISMATCH:ORDER_CONSTRAINT_MISMATCH");
        }
        self.read_bound_execution(hash).await
    }
}

/// Constraints retained from the original attempt, never inferred from a poll.
#[derive(Clone)]
pub(super) struct BoundOrder {
    pub handle: AttemptHandle,
    pub market: ApiMarket,
    pub requested: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub expiration: i64,
}

impl BoundOrder {
    fn validate(&self, data: &ApiOrderData, hash: &str) -> Result<()> {
        let identity = self.handle.lock().identity.clone();
        let contract = data
            .order
            .as_ref()
            .ok_or_else(|| anyhow!("FACT_INCOMPLETE:MISSING_CONTRACT_ORDER"))?;
        let equal = |actual: &Option<String>, expected: &str| {
            actual
                .as_deref()
                .is_some_and(|v| v.eq_ignore_ascii_case(expected))
        };
        if data.market_id != identity.market_id
            || !equal(&contract.hash, hash)
            || !equal(&contract.maker, &identity.account.trading_address)
            || !equal(&contract.signer, &identity.account.trading_address)
            || contract.token_id.as_deref() != Some(&identity.token_id)
            || contract.side != Some(i32::from(identity.side.as_u8()))
            || contract.expiration != Some(self.expiration)
            || contract
                .maker_amount
                .as_deref()
                .map(decimal_to_wei_exact)
                .transpose()?
                != Some(self.maker_amount)
            || contract
                .taker_amount
                .as_deref()
                .map(decimal_to_wei_exact)
                .transpose()?
                != Some(self.taker_amount)
        {
            return Err(VenueError::new(
                VenueErrorCode::AccountMismatch,
                "reconcile_order",
                "ORDER_IDENTITY_MISMATCH",
            )
            .into());
        }
        if venue_gross_q(data)? > self.requested {
            bail!("RECONCILIATION_REQUIRED:OVERFILL");
        }
        Ok(())
    }
}

impl PredictFunTrader {
    pub(super) async fn read_bound_execution(&self, hash: &str) -> Result<ExecutionSnapshot> {
        let bound = self.bound_orders.read().get(hash).cloned().ok_or_else(|| {
            VenueError::new(
                VenueErrorCode::ReconciliationRequired,
                "reconcile_order",
                "UNBOUND_ORDER",
            )
        })?;
        let result = self.read_order_execution(hash, &bound).await;
        let account = bound.handle.lock().identity.account.clone();
        if let Ok(snapshot) = &result {
            let cash = snapshot
                .accounting
                .gross_cash
                .value()
                .copied()
                .unwrap_or(U256::ZERO);
            let fee = snapshot
                .accounting
                .collateral_fee
                .value()
                .copied()
                .unwrap_or(U256::ZERO);
            if snapshot.identity.side == Side::Buy
                && cash.saturating_add(fee) > bound.handle.lock().r0
            {
                self.gate
                    .record_reconciliation(&account, "KNOWN_RISK_EXCEEDS_RESERVATION");
            }
        }
        match &result {
            Ok(snapshot) if snapshot.pages_complete && snapshot.accounting_complete() => {
                self.gate.record_read_success(&account, "execution");
            }
            _ => self
                .gate
                .record_read_failure(&account, "execution", "FACT_INCOMPLETE"),
        }
        result
    }

    /// Read a single immutable order binding; historical reads never replace live handles.
    pub(super) async fn read_order_execution(
        &self,
        hash: &str,
        bound: &BoundOrder,
    ) -> Result<ExecutionSnapshot> {
        let generation = {
            let mut generations = self.generations.write();
            let generation = generations.entry(hash.into()).or_default();
            *generation += 1;
            *generation
        };
        let before = self
            .authed::<ApiOrderData>(Method::GET, &format!("/orders/{hash}"), None)
            .await?
            .data;
        bound.validate(&before, hash)?;
        let identity = bound.handle.lock().identity.clone();
        let q_before = venue_gross_q(&before)?;
        let unknown = || Field::Unknown("FACT_INCOMPLETE".into());
        let mut snapshot = ExecutionSnapshot {
            identity: identity.clone(),
            submission: submission_from_status(&before.status),
            quantity: Quantity::Known {
                gross_q_atoms: q_before,
                class: quantity_class(q_before, bound.requested),
            },
            accounting: Accounting {
                gross_cash: unknown(),
                collateral_fee: unknown(),
                share_fee: unknown(),
                net_cash: unknown(),
                net_inventory: unknown(),
                vwap: unknown(),
            },
            accounting_gross_q: None,
            closure: Closure::Unproven,
            generation,
            pages_complete: false,
            gross_q_before: q_before,
            gross_q_after: q_before,
            source: "Predict REST cumulative + same-chain receipts".into(),
            observed_at: chrono::Utc::now(),
            tx_hash: None,
        };
        let result: Result<()> = async {
            let mut matches = Vec::new();
            let mut cursor: Option<String> = None;
            let mut cursors = HashSet::new();
            let mut fingerprints = HashSet::new();
            let mut complete = false;
            for _ in 0..MAX_PAGES {
                let mut url = url::Url::parse("https://synthetic.invalid/orders/matches").unwrap();
                {
                    let mut query = url.query_pairs_mut();
                    query
                        .append_pair("first", "100")
                        .append_pair("orderHashes", hash)
                        .append_pair("signerAddress", &identity.account.trading_address);
                    if let Some(cursor) = &cursor {
                        query.append_pair("after", cursor);
                    }
                }
                let path = format!("{}?{}", url.path(), url.query().unwrap());
                let page = self
                    .authed::<Vec<ApiMatch>>(Method::GET, &path, None)
                    .await?;
                for item in page.data {
                    // Fingerprint is an ambiguity detector, not an invented fill id.
                    let fingerprint = format!("{:?}", item);
                    if !fingerprints.insert(fingerprint) {
                        bail!("FACT_INCOMPLETE:AMBIGUOUS_MATCH_DUPLICATE");
                    }
                    if item.market.id != identity.market_id
                        || item.market.condition_id != bound.market.condition_id
                        || item.market.is_neg_risk != bound.market.is_neg_risk
                        || item.market.is_yield_bearing != bound.market.is_yield_bearing
                    {
                        bail!("ACCOUNT_MISMATCH:MARKET_MISMATCH");
                    }
                    chrono::DateTime::parse_from_rfc3339(&item.executed_at)
                        .map_err(|_| anyhow!("FACT_INCOMPLETE:EXECUTED_AT"))?;
                    matches.push(item);
                }
                match page.cursor.filter(|s| !s.is_empty()) {
                    None => {
                        complete = true;
                        break;
                    }
                    Some(next) => {
                        if !cursors.insert(next.clone()) {
                            bail!("READ_UNAVAILABLE:PAGINATION_LOOP");
                        }
                        cursor = Some(next);
                    }
                }
            }
            if !complete {
                bail!("READ_UNAVAILABLE:PAGE_CAP");
            }
            let legs = own_legs(&matches, hash, &identity.account.trading_address)?;
            for leg in &legs {
                if leg.token_id != identity.token_id
                    || leg.quote_type
                        != if identity.side == Side::Buy {
                            "Bid"
                        } else {
                            "Ask"
                        }
                {
                    bail!("ACCOUNT_MISMATCH:LEG_IDENTITY_MISMATCH");
                }
            }
            let facts = cumulative(&legs, q_before)?;
            snapshot.accounting_gross_q = Some(facts.gross_q_atoms);
            snapshot.accounting = accounting(
                identity.side,
                facts.gross_q_atoms,
                facts.gross_cash_atoms,
                facts.collateral_fee_atoms,
                facts.share_fee_atoms,
            );
            snapshot.tx_hash = facts.tx_hashes.first().cloned();
            let after = self
                .authed::<ApiOrderData>(Method::GET, &format!("/orders/{hash}"), None)
                .await?
                .data;
            bound.validate(&after, hash)?;
            let q_after = venue_gross_q(&after)?;
            snapshot.gross_q_after = q_after;
            snapshot.quantity = Quantity::Known {
                gross_q_atoms: q_after,
                class: quantity_class(q_after, bound.requested),
            };
            let mut accounting = snapshot.accounting.clone();
            let matched = q_before == q_after && q_after == facts.gross_q_atoms;
            let chain = self.chain_read().await?;
            if !matched || !chain.settled(&bound, &identity, &legs, &accounting).await? {
                accounting.net_cash = Field::Unknown("SETTLEMENT_UNPROVEN".into());
                accounting.net_inventory = Field::Unknown("SETTLEMENT_UNPROVEN".into());
            }
            let closure = if matched && chain.closed(&bound, hash, &after.status, q_after).await? {
                Closure::Final {
                    evidence: format!("RPC_FIXED_BLOCK:{}", chain.block),
                    final_gross_q: q_after,
                }
            } else if after.status == "OPEN" {
                Closure::Open
            } else {
                Closure::Unproven
            };
            snapshot.submission = submission_from_status(&after.status);
            snapshot.accounting = accounting;
            snapshot.closure = closure;
            snapshot.pages_complete = complete && matched;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            // Preserve REST facts already obtained, but never turn an RPC or
            // parsing failure into settled inventory or authoritative closure.
            snapshot.pages_complete = false;
            snapshot.closure = Closure::Unproven;
            snapshot.accounting.net_cash = Field::Unknown("SETTLEMENT_UNPROVEN".into());
            snapshot.accounting.net_inventory = Field::Unknown("SETTLEMENT_UNPROVEN".into());
            bound.handle.lock().last_reason = Some(error.to_string());
        }
        Ok(snapshot)
    }
}

/// The API's page size for matches and orders.
pub const PAGE_SIZE: u32 = 100;
/// The most pages one scan may read. Past this, the facts aren't complete.
pub const MAX_PAGES: u32 = 20;

/// One of the account's own legs of a match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnLeg {
    /// The order hash this leg belongs to.
    pub order_hash: String,
    /// The outcome token that changed hands.
    pub token_id: String,
    /// `Bid` for a buy, `Ask` for a sell.
    pub quote_type: String,
    /// Filled shares, in atoms.
    pub q_atoms: U256,
    /// Price per share, in collateral atoms.
    pub p_atoms: U256,
    /// The fee, and which side it was charged on.
    pub fee: FeeFact,
    /// The settlement transaction.
    pub tx_hash: Option<String>,
    /// The match's own timestamp.
    pub executed_at: String,
}

/// What the venue says about a fee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeFact {
    /// The venue states the fee, in collateral atoms.
    Known { amount: U256, kind: FeeKind },
    /// The venue gave no fee, and that is not the same as a zero fee.
    Missing,
}

/// Which side a fee was charged on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeKind {
    Collateral,
    Shares,
}

impl FeeFact {
    pub fn kind(&self) -> Option<FeeKind> {
        match self {
            FeeFact::Known { kind, .. } => Some(*kind),
            FeeFact::Missing => None,
        }
    }
}

/// Everything one scan of an order's history added up to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CumulativeFacts {
    /// The account's own filled shares, in atoms.
    pub gross_q_atoms: U256,
    /// The gross cash, in collateral atoms: `sum(q * p)`.
    pub gross_cash_atoms: U256,
    /// The collateral fee, in collateral atoms.
    pub collateral_fee_atoms: Option<U256>,
    /// The share fee, in atoms.
    pub share_fee_atoms: Option<U256>,
    /// Every page was read, and the scan ended cleanly.
    pub pages_complete: bool,
    /// The venue's own cumulative filled amount after the scan.
    pub venue_gross_q: U256,
    /// The settlement transactions the legs named.
    pub tx_hashes: Vec<String>,
}

/// A stable fingerprint of one leg. It identifies page overlap, and exposes
/// contradictions; it is not a claim about an official fill id.
pub fn leg_fingerprint(leg: &OwnLeg) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        leg.tx_hash.as_deref().unwrap_or(""),
        leg.executed_at,
        leg.order_hash,
        leg.token_id,
        leg.quote_type,
        leg.q_atoms,
        leg.p_atoms,
    )
}

/// Keep the legs of `matches` that belong to `order_hash` and to `account`.
///
/// A match event describes a whole transaction, so the list may hold other
/// legs: another of the account's orders, or the counterparty's. Only the
/// account's own leg of `order_hash` counts.
pub fn own_legs(matches: &[ApiMatch], order_hash: &str, account: &str) -> Result<Vec<OwnLeg>> {
    let account = account.to_lowercase();
    let mut out = Vec::new();
    for m in matches {
        let own_count = std::iter::once(&m.taker)
            .chain(m.makers.iter())
            .filter(|leg| {
                leg.hash.eq_ignore_ascii_case(order_hash)
                    && leg.signer.eq_ignore_ascii_case(&account)
            })
            .count();
        if own_count > 1 {
            bail!("FACT_INCOMPLETE:AMBIGUOUS_OWN_LEGS");
        }
        for leg in std::iter::once(&m.taker).chain(m.makers.iter()) {
            if !leg.hash.eq_ignore_ascii_case(order_hash) {
                continue;
            }
            if !leg.signer.to_lowercase().eq_ignore_ascii_case(&account) {
                continue;
            }
            out.push(OwnLeg {
                order_hash: leg.hash.clone(),
                token_id: leg.outcome.on_chain_id.clone(),
                quote_type: leg.quote_type.clone(),
                q_atoms: decimal_to_wei_exact(&leg.amount)?,
                p_atoms: decimal_to_wei_exact(&leg.price)?,
                fee: fee_fact(leg)?,
                tx_hash: m.transaction_hash.clone(),
                executed_at: m.executed_at.clone(),
            });
        }
    }
    Ok(out)
}

/// Read the venue's fee for a leg. A missing fee stays missing.
fn fee_fact(leg: &super::ApiLeg) -> Result<FeeFact> {
    let Some(fee) = leg.fee.as_ref() else {
        return Ok(FeeFact::Missing);
    };
    let kind = match fee.fee_type.as_str() {
        "COLLATERAL" => FeeKind::Collateral,
        "SHARES" => FeeKind::Shares,
        other => bail!("unknown fee type {other:?}"),
    };
    Ok(FeeFact::Known {
        amount: decimal_to_wei_exact(&fee.amount)?,
        kind,
    })
}

/// Add up the account's own legs.
///
/// The gross cash is an exact rational: `sum(q_i * p_i) / 10^36`. The division
/// keeps its remainder, so a price that isn't representable in collateral
/// atoms isn't silently rounded away.
pub fn cumulative(legs: &[OwnLeg], venue_gross_q: U256) -> Result<CumulativeFacts> {
    let mut gross_q_atoms = U256::ZERO;
    let mut gross_cash_atoms = U256::ZERO;
    let mut collateral_fee_atoms: Option<U256> = Some(U256::ZERO);
    let mut share_fee_atoms: Option<U256> = Some(U256::ZERO);
    let mut tx_hashes: Vec<String> = Vec::new();

    for leg in legs {
        gross_q_atoms = gross_q_atoms
            .checked_add(leg.q_atoms)
            .ok_or_else(|| anyhow!("CUMULATIVE_OVERFLOW"))?;
        // q and p are both 18-decimal, so their product is 36-decimal. Keep the
        // product in collateral atoms: divide by 10^18 and keep the remainder.
        let product = U512::from(leg.q_atoms) * U512::from(leg.p_atoms);
        let (whole, remainder) = product.div_rem(U512::from(WEI));
        if !remainder.is_zero() {
            bail!("NON_ATOMIC_CASH");
        }
        let whole = U256::from_str_radix(&whole.to_string(), 10)
            .map_err(|_| anyhow!("CUMULATIVE_OVERFLOW"))?;
        gross_cash_atoms = gross_cash_atoms
            .checked_add(whole)
            .ok_or_else(|| anyhow!("CUMULATIVE_OVERFLOW"))?;

        match leg.fee {
            FeeFact::Known { amount, kind } => {
                let slot = match kind {
                    FeeKind::Collateral => &mut collateral_fee_atoms,
                    FeeKind::Shares => &mut share_fee_atoms,
                };
                if let Some(total) = *slot {
                    *slot = Some(
                        total
                            .checked_add(amount)
                            .ok_or_else(|| anyhow!("CUMULATIVE_OVERFLOW"))?,
                    );
                }
            }
            FeeFact::Missing => {
                collateral_fee_atoms = None;
                share_fee_atoms = None;
            }
        }
        if let Some(tx) = leg.tx_hash.as_ref() {
            if !tx_hashes.iter().any(|t| t == tx) {
                tx_hashes.push(tx.clone());
            }
        }
    }

    Ok(CumulativeFacts {
        gross_q_atoms,
        gross_cash_atoms,
        collateral_fee_atoms,
        share_fee_atoms,
        pages_complete: true,
        venue_gross_q,
        tx_hashes,
    })
}

/// The accounting of one fill, given the side the order traded.
///
/// A buy pays `gross + collateral fee` and receives `gross - share fee`; a sell
/// receives `gross - collateral fee` and gives up `gross + share fee`. A fee the
/// venue didn't state stays unknown, so the accounting isn't complete.
pub fn accounting(
    side: Side,
    gross_q_atoms: U256,
    gross_cash_atoms: U256,
    collateral_fee_atoms: Option<U256>,
    share_fee_atoms: Option<U256>,
) -> Accounting {
    let (net_cash, net_inventory) = match side {
        Side::Buy => {
            let cash = collateral_fee_atoms.and_then(|fee| gross_cash_atoms.checked_add(fee));
            let inventory = share_fee_atoms.and_then(|fee| gross_q_atoms.checked_sub(fee));
            (cash, inventory)
        }
        Side::Sell => {
            let cash = collateral_fee_atoms.and_then(|fee| gross_cash_atoms.checked_sub(fee));
            // This route does not support SELL share fees. Never guess a net
            // inventory deduction if the venue reports a nonzero share fee.
            let inventory = share_fee_atoms.filter(U256::is_zero).map(|_| gross_q_atoms);
            (cash, inventory)
        }
    };

    let vwap = if gross_q_atoms.is_zero() {
        Field::Unknown("MISSING_VWAP".into())
    } else {
        let scaled = U512::from(gross_cash_atoms) * U512::from(WEI);
        match U256::from_str_radix(&(scaled / U512::from(gross_q_atoms)).to_string(), 10) {
            Ok(value) => Field::Known(value),
            Err(_) => Field::Unknown("VWAP_OVERFLOW".into()),
        }
    };

    Accounting {
        gross_cash: Field::Known(gross_cash_atoms),
        collateral_fee: match collateral_fee_atoms {
            Some(v) => Field::Known(v),
            None => Field::Unknown("MISSING_FEE".into()),
        },
        share_fee: match share_fee_atoms {
            Some(v) => Field::Known(v),
            None => Field::Unknown("MISSING_FEE".into()),
        },
        net_cash: match net_cash {
            Some(v) => Field::Known(v),
            None => Field::Unknown("MISSING_FEE".into()),
        },
        net_inventory: match net_inventory {
            Some(v) => Field::Known(v),
            None => Field::Unknown("MISSING_FEE".into()),
        },
        vwap,
    }
}

/// The quantity class of a fill, given how much was asked for.
pub fn quantity_class(filled: U256, requested: U256) -> QuantityClass {
    if filled.is_zero() {
        QuantityClass::Zero
    } else if filled >= requested {
        QuantityClass::Full
    } else {
        QuantityClass::Partial
    }
}

/// The closure of an order, given what the venue says.
///
/// Only `FILLED` at the signed amount is provably closed. The other terminal
/// statuses are the venue's words, not a proof that the order can't fill, so
/// they stay unproven.
pub fn closure_from_status(status: &str, filled: U256, requested: U256) -> Closure {
    match status {
        "OPEN" => Closure::Open,
        "FILLED" if filled >= requested => Closure::Final {
            evidence: "STATUS_FILLED".into(),
            final_gross_q: filled,
        },
        "FILLED" => Closure::Unproven,
        "CANCELLED" | "INVALIDATED" | "EXPIRED" => Closure::Unproven,
        _ => Closure::Unproven,
    }
}

/// The submission outcome of an order the venue still lists.
pub fn submission_from_status(status: &str) -> Submission {
    match status {
        "OPEN" => Submission::Accepted,
        "FILLED" | "EXPIRED" | "CANCELLED" | "INVALIDATED" => Submission::Accepted,
        _ => Submission::Unknown,
    }
}

/// The venue's own cumulative filled amount for an order.
pub fn venue_gross_q(order: &ApiOrderData) -> Result<U256> {
    decimal_to_wei_exact(&order.amount_filled)
}

/// The shares an order state reports, for callers that only want a number.
pub fn filled_shares(facts: &CumulativeFacts) -> f64 {
    wei_to_f64(facts.gross_q_atoms)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::service::predict_fun::{ApiFee, ApiLeg, ApiMatch, ApiOutcome};
    use serde_json::json;

    use super::super::account::tests::{synthetic_trader, SyntheticRpc};
    use super::super::account::SyntheticTransport;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mutate raw DTOs at the existing transport seam, not final snapshots.
    pub(crate) struct Scenario {
        pub(crate) base: Arc<SyntheticRpc>,
        pub(crate) mode: Mutex<&'static str>,
        paths: Mutex<Vec<String>>,
        refreshes: AtomicUsize,
        get401: AtomicUsize,
        position_pages: AtomicUsize,
        history: Mutex<std::collections::HashMap<String, Value>>,
    }

    impl SyntheticTransport for Scenario {
        fn auth_token(&self) -> String {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            self.base.auth_token()
        }
        fn sign(&self, order: &Order) -> Result<(B256, String)> {
            self.base.sign(order)
        }
        fn rpc(&self, method: &str, params: &Value) -> std::result::Result<Value, VenueError> {
            let mode = *self.mode.lock();
            if method == "eth_call"
                && ((mode == "fee_funds"
                    && params[0]["data"]
                        .as_str()
                        .is_some_and(|s| s.starts_with("0x70a08231")))
                    || (mode == "fee_allowance"
                        && params[0]["data"]
                            .as_str()
                            .is_some_and(|s| s.starts_with("0xdd62ed3e"))))
            {
                return Ok(json!(format!("0x{:064x}", U256::from(5) * U256::from(WEI))));
            }
            if (matches!(mode, "receipt_error" | "history_receipt_error")
                && method == "eth_getTransactionReceipt")
                || (mode == "signature_error"
                    && method == "eth_call"
                    && params[0]["data"]
                        .as_str()
                        .is_some_and(|s| s.starts_with("0x1626ba7e")))
            {
                return Err(VenueError::new(
                    VenueErrorCode::ReadUnavailable,
                    "synthetic",
                    "SYNTHETIC_READ_FAILURE",
                ));
            }
            if mode == "cash_drift"
                && method == "eth_call"
                && params[0]["data"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("0x70a08231"))
            {
                return Ok(json!(format!(
                    "0x{:064x}",
                    U256::from(99) * U256::from(WEI)
                )));
            }
            if matches!(mode, "inventory_drift" | "history_extra_inventory") && method == "eth_call"
            {
                use alloy_sol_types::SolCall;
                let selector = hex::encode(account::CtfRead::balanceOfCall::SELECTOR);
                if params[0]["data"]
                    .as_str()
                    .is_some_and(|data| data.starts_with(&format!("0x{selector}")))
                {
                    return Ok(json!(format!(
                        "0x{:064x}",
                        U256::from(if mode == "history_extra_inventory" {
                            109
                        } else {
                            98
                        }) * U256::from(WEI)
                            / U256::from(10)
                    )));
                }
            }
            self.base.rpc(method, params)
        }
        fn rest(
            &self,
            method: &Method,
            path: &str,
            bearer: Option<&str>,
            body: Option<&Value>,
        ) -> Result<String> {
            self.paths.lock().push(path.into());
            let mode = *self.mode.lock();
            if mode == "get401"
                && *method == Method::GET
                && path.starts_with("/orders/")
                && !path.starts_with("/orders/matches?")
                && self.get401.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(super::super::ApiError {
                    status: reqwest::StatusCode::UNAUTHORIZED,
                    body: "synthetic".into(),
                }
                .into());
            }
            if matches!(mode, "external_open" | "tracked_open")
                && path.starts_with("/orders?status=OPEN")
            {
                return Ok(json!({"data":[{"id":if mode == "tracked_open" {
                    "synthetic-order"
                } else { "external-order" }, "marketId":1,"amountFilled":"0","status":"OPEN"}]})
                .to_string());
            }
            if mode == "position_cap" && path.starts_with("/positions?") {
                let page = self.position_pages.fetch_add(1, Ordering::SeqCst);
                return Ok(json!({"data":[],"cursor":format!("next{page}")}).to_string());
            }
            if mode.starts_with("history_") {
                let history = self.history.lock();
                if let Some(record) = path
                    .strip_prefix("/orders/")
                    .and_then(|hash| history.get(hash))
                {
                    let mut order = record["order"].clone();
                    if record["matches"].as_array().unwrap().is_empty() && mode == "history_open" {
                        order["status"] = json!("OPEN");
                    }
                    if mode == "history_wrong_account" {
                        order["order"]["maker"] = json!(Address::ZERO);
                    }
                    return Ok(json!({"data":order}).to_string());
                }
                if let Some(query) = path.strip_prefix("/orders/matches?") {
                    let params: std::collections::HashMap<_, _> =
                        url::form_urlencoded::parse(query.as_bytes()).collect();
                    let filtered = params.get("orderHashes").map(|hash| hash.as_ref());
                    let page = usize::from(params.contains_key("after"));
                    if page > 0 && mode == "history_missing_page" {
                        bail!("SYNTHETIC_MISSING_HISTORY_PAGE");
                    }
                    if mode == "history_loop" {
                        return Ok(json!({"data":[],"cursor":"repeat"}).to_string());
                    }
                    let events: Vec<_> = history
                        .iter()
                        .filter(|(hash, _)| filtered.is_none_or(|wanted| wanted == hash.as_str()))
                        .flat_map(|(_, record)| {
                            record["matches"].as_array().unwrap().iter().cloned()
                        })
                        .collect();
                    let mut selected = events.get(page).cloned().into_iter().collect::<Vec<_>>();
                    if filtered.is_some() && mode == "history_missing_fee" {
                        for event in &mut selected {
                            event["taker"].as_object_mut().unwrap().remove("fee");
                        }
                    }
                    return Ok(json!({"data":selected,"cursor":if page + 1 < events.len() { Some("next") } else { None }}).to_string());
                }
            }
            let mut value: Value =
                serde_json::from_str(&self.base.rest(method, path, bearer, body)?)?;
            if mode == "history_extra_inventory" && path.starts_with("/positions?") {
                value["data"][0]["amount"] =
                    json!((U256::from(109) * U256::from(WEI) / U256::from(10)).to_string());
            }
            if mode == "closed_rest"
                && path.starts_with("/orders/")
                && !path.starts_with("/orders/matches?")
            {
                value["data"]["status"] = json!("CANCELLED");
            }
            if path == "/orders" && *method == Method::POST && mode == "noid" {
                value["data"] = json!({});
            }
            if mode == "inventory_drift" && path.starts_with("/positions?") {
                value["data"][0]["amount"] = json!("9800000000000000000");
            }
            if path.starts_with("/orders/matches?") {
                if let Some(events) = value["data"].as_array_mut() {
                    for event in events {
                        match mode {
                            "wrong_condition" => event["market"]["conditionId"] = json!(B256::ZERO),
                            "wrong_token" => event["taker"]["outcome"]["onChainId"] = json!("88"),
                            "nan" => event["taker"]["amount"] = json!("NaN"),
                            "fraction" => event["taker"]["amount"] = json!("1.5"),
                            "bad_fee" => event["taker"]["fee"]["type"] = json!("UNKNOWN"),
                            "huge_fee" => {
                                event["taker"]["fee"]["amount"] = json!("6000000000000000000")
                            }
                            "revised_fee" => {
                                event["taker"]["fee"]["amount"] = json!("30000000000000000")
                            }
                            "ambiguous" => event["makers"] = json!([event["taker"].clone()]),
                            _ => {}
                        }
                    }
                }
            }
            if mode == "scan_growth"
                && path.starts_with("/orders/matches?")
                && path.contains("orderHashes=")
            {
                self.base.stage.store(1, Ordering::SeqCst);
            }
            if mode == "external_open" && path.starts_with("/orders?status=OPEN") {
                value["data"] = json!([{"id":"external-order","marketId":1,"amountFilled":"0","status":"OPEN"}]);
            }
            Ok(value.to_string())
        }
    }

    pub(crate) fn scenario(
        base_fault: &'static str,
        mode: &'static str,
    ) -> (PredictFunTrader, Arc<Scenario>) {
        let (mut trader, base) = synthetic_trader(base_fault);
        let transport = Arc::new(Scenario {
            base,
            mode: Mutex::new(mode),
            paths: Mutex::new(vec![]),
            refreshes: AtomicUsize::new(0),
            get401: AtomicUsize::new(0),
            position_pages: AtomicUsize::new(0),
            history: Mutex::new(Default::default()),
        });
        Arc::get_mut(&mut trader.client).unwrap().synthetic = Some(transport.clone());
        (trader, transport)
    }

    /// Reuse raw DTOs and receipts from a fake submitted order, then simulate a
    /// new consumer with no in-process order bindings and two declared hashes.
    pub(crate) async fn historical_trader(
        mode: &'static str,
    ) -> (PredictFunTrader, Arc<Scenario>, [String; 2]) {
        let (trader, transport) = scenario("fills", "");
        transport.base.stage.store(1, Ordering::SeqCst);
        let attempt = handle(&trader, 1, trader.reservation_atoms(&planned()).unwrap());
        let ticket = trader
            .place_tracked(&planned(), 0, true, attempt)
            .await
            .unwrap();
        assert!(ticket.execution.unwrap().accounting_complete());
        let hash = trader.bound_orders.read().keys().next().unwrap().clone();
        let response: Value = serde_json::from_str(
            &transport
                .base
                .rest(&Method::GET, &format!("/orders/{hash}"), None, None)
                .unwrap(),
        )
        .unwrap();
        let matches: Value = serde_json::from_str(
            &transport
                .base
                .rest(
                    &Method::GET,
                    &format!("/orders/matches?orderHashes={hash}"),
                    None,
                    None,
                )
                .unwrap(),
        )
        .unwrap();
        let zero_hash = format!("0x{}", "d".repeat(64));
        let mut zero = response["data"].clone();
        zero["id"] = json!("historical-zero");
        zero["order"]["hash"] = json!(zero_hash);
        zero["amountFilled"] = json!("0");
        zero["status"] = json!("CANCELLED");
        transport.history.lock().insert(
            hash.clone(),
            json!({"order":response["data"],"matches":matches["data"]}),
        );
        transport
            .history
            .lock()
            .insert(zero_hash.clone(), json!({"order":zero,"matches":[]}));
        trader.bound_orders.write().clear();
        trader
            .gate
            .resolve(&trader.identity(), 1, AttemptPhase::Closed);
        trader
            .run_context
            .write()
            .as_mut()
            .unwrap()
            .reconciled_orders = vec![hash.clone(), zero_hash.clone()];
        if mode == "history_undeclared" {
            trader
                .run_context
                .write()
                .as_mut()
                .unwrap()
                .reconciled_orders
                .clear();
        }
        *transport.mode.lock() = mode;
        (trader, transport, [hash, zero_hash])
    }

    pub(crate) fn history_bindings_clear(trader: &PredictFunTrader) -> bool {
        trader.bound_orders.read().is_empty()
    }

    pub(crate) fn queried_order_count(transport: &Scenario, hash: &str) -> usize {
        transport
            .paths
            .lock()
            .iter()
            .filter(|path| **path == format!("/orders/{hash}"))
            .count()
    }

    fn planned() -> PlannedOrder {
        PlannedOrder {
            venue: VenueId::PredictFun,
            token_id: "77".into(),
            side: Side::Buy,
            shares: 10.0,
            limit_price: 0.5,
            usd_notional: 5.0,
            order_type: crate::models::OrderType::Fak,
            source_trade_hash: None,
        }
    }

    fn handle(trader: &PredictFunTrader, id: u64, r0: U256) -> AttemptHandle {
        let handle = new_untracked_handle();
        {
            let mut record = handle.lock();
            record.identity.local_attempt_id = id;
            record.identity.account = trader.identity();
            record.r0 = r0;
        }
        handle
    }

    #[test]
    fn a006_checked_reservation_extremes_and_rounding() {
        assert_eq!(cash_with_fee(U512::from(1), 1).unwrap(), U256::from(2));
        assert!(cash_with_fee(U512::MAX, u32::MAX).is_err());
        assert!(cash_with_fee(U512::from(U256::MAX), 1).is_err());
        assert_eq!(cash_with_fee(U512::from(U256::MAX), 0).unwrap(), U256::MAX);
        assert_eq!(ceil_div(U512::MAX, U512::MAX).unwrap(), U512::from(1));
    }

    #[tokio::test]
    async fn fix_r1_buy_funds_and_allowance_include_fee_before_signing() {
        for limit in [false, true] {
            for mode in ["fee_funds", "fee_allowance", ""] {
                let (trader, transport) = scenario("terminal", mode);
                let mut market = trader.client.market_for_token("77").unwrap();
                market.fee_rate_bps = 100;
                trader.client.remember_market(&market);
                let order = planned();
                let attempt = handle(&trader, 71, trader.reservation_atoms(&order).unwrap());
                let result = if limit {
                    trader
                        .post_limit_tracked(
                            &LimitOrder {
                                market_id: 1,
                                token_id: "77".into(),
                                side: Side::Buy,
                                shares: 10.0,
                                price: 0.5,
                                ttl_secs: 60,
                                post_only: true,
                            },
                            true,
                            attempt,
                        )
                        .await
                        .map(|_| ())
                } else {
                    trader
                        .place_tracked(&order, 60, true, attempt)
                        .await
                        .map(|_| ())
                };
                assert_eq!(result.is_ok(), mode.is_empty(), "limit={limit}, {mode}");
                assert_eq!(
                    transport.base.signs.load(Ordering::SeqCst),
                    usize::from(mode.is_empty())
                );
                assert_eq!(
                    transport.base.posts.load(Ordering::SeqCst),
                    usize::from(mode.is_empty())
                );
            }
        }
    }

    #[tokio::test]
    async fn a007_signed_bound_and_prepared_failure_release() {
        for mode in ["", "signature_error"] {
            let (trader, transport) = scenario("terminal", mode);
            let insufficient = handle(&trader, 41, U256::from(1));
            assert!(trader
                .place_tracked(&planned(), 0, true, insufficient.clone())
                .await
                .is_err());
            assert!(trader.gate.allows_new_orders(&trader.identity()));
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 0);
            let attempt = handle(&trader, 42, trader.reservation_atoms(&planned()).unwrap());
            if mode == "signature_error" {
                assert!(trader
                    .place_tracked(&planned(), 0, true, attempt)
                    .await
                    .is_err());
                assert!(trader.gate.allows_new_orders(&trader.identity()));
                assert_eq!(transport.base.posts.load(Ordering::SeqCst), 0);
            }
            *transport.mode.lock() = "";
            let attempt = handle(&trader, 43, trader.reservation_atoms(&planned()).unwrap());
            trader
                .place_tracked(&planned(), 0, true, attempt)
                .await
                .unwrap();
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a007_post_401_not_replayed_get_refresh_preserved() {
        for (base, mode) in [
            ("post401", ""),
            ("terminal", "get401"),
            ("terminal", "noid"),
        ] {
            let (trader, transport) = scenario(base, mode);
            let attempt = handle(&trader, 1, trader.reservation_atoms(&planned()).unwrap());
            let result = trader
                .place_tracked(&planned(), 0, true, attempt.clone())
                .await;
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
            let hash = attempt.lock().identity.signed_hash.clone().unwrap();
            if mode == "get401" {
                result.unwrap();
                assert_eq!(transport.refreshes.load(Ordering::SeqCst), 1);
                assert_eq!(transport.get401.load(Ordering::SeqCst), 3);
                assert!(
                    transport
                        .paths
                        .lock()
                        .iter()
                        .filter(|p| **p == format!("/orders/{hash}"))
                        .count()
                        == 3
                );
            } else {
                assert!(result.is_err());
                assert_eq!(transport.refreshes.load(Ordering::SeqCst), 0);
                assert!(!trader.gate.allows_new_orders(&trader.identity()));
                trader.release_prepared(&attempt);
                assert!(!trader.gate.allows_new_orders(&trader.identity()));
                transport.base.stage.store(1, Ordering::SeqCst);
                *transport.mode.lock() = "";
                let snapshot = trader.execution_snapshot(&hash).await.unwrap();
                assert!(matches!(snapshot.closure, Closure::Final { .. }));
            }
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a006_raw_faults_preserve_known_quantity_without_inventory() {
        for mode in [
            "wrong_condition",
            "wrong_token",
            "nan",
            "fraction",
            "bad_fee",
            "ambiguous",
            "receipt_error",
        ] {
            let (trader, transport) = scenario("fills", mode);
            let attempt = handle(&trader, 1, trader.reservation_atoms(&planned()).unwrap());
            let ticket = trader
                .place_tracked(&planned(), 0, true, attempt)
                .await
                .unwrap();
            let snapshot = ticket.execution.unwrap();
            assert_eq!(
                snapshot.gross_q(),
                Some(U256::from(4) * U256::from(WEI)),
                "{mode}"
            );
            assert!(!snapshot.accounting_complete(), "{mode}");
            assert!(!snapshot.pages_complete, "{mode}");
            assert!(!trader.gate.allows_new_orders(&trader.identity()), "{mode}");
            if mode == "receipt_error" {
                assert_eq!(
                    snapshot.accounting.gross_cash.value(),
                    Some(&U256::from(1_600_000_000_000_000_000u128))
                );
                assert_eq!(
                    snapshot.accounting.collateral_fee.value(),
                    Some(&U256::from(20_000_000_000_000_000u128))
                );
            }
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a009_real_page_cap_and_held_second_page() {
        let (trader, transport) = scenario("", "position_cap");
        assert!(trader
            .read_positions()
            .await
            .unwrap_err()
            .to_string()
            .contains("PAGE_CAP"));
        assert_eq!(
            transport.position_pages.load(Ordering::SeqCst),
            MAX_PAGES as usize
        );
        let (trader, _) = scenario("positions", "");
        assert_eq!(
            trader.held_wei(1, "88").await.unwrap(),
            U256::from(3) * U256::from(WEI)
        );
    }

    fn outcome() -> ApiOutcome {
        serde_json::from_value(json!({"onChainId": "77", "name": "Yes", "indexSet": 1})).unwrap()
    }

    fn leg(quote_type: &str, amount: &str, price: &str, fee: Option<ApiFee>) -> ApiLeg {
        ApiLeg {
            quote_type: quote_type.into(),
            amount: amount.into(),
            price: price.into(),
            outcome: outcome(),
            signer: "0xAAA".into(),
            hash: "0xhash".into(),
            fee,
        }
    }

    fn match_event(taker: ApiLeg, makers: Vec<ApiLeg>) -> ApiMatch {
        ApiMatch {
            market: crate::service::predict_fun::ApiMarket {
                id: 1,
                question: "q".into(),
                trading_status: "OPEN".into(),
                is_neg_risk: false,
                is_yield_bearing: false,
                fee_rate_bps: 0,
                outcomes: vec![outcome()],
                category_slug: None,
                status: "OPEN".into(),
                market_variant: "".into(),
                decimal_precision: None,
                polymarket_condition_ids: vec![],
                condition_id: None,
            },
            taker,
            makers,
            transaction_hash: Some("0xtx".into()),
            executed_at: "2026-11-13T00:00:00Z".into(),
        }
    }

    fn collateral_fee(amount: &str) -> ApiFee {
        ApiFee {
            amount: amount.into(),
            fee_type: "COLLATERAL".into(),
        }
    }

    fn share_fee(amount: &str) -> ApiFee {
        ApiFee {
            amount: amount.into(),
            fee_type: "SHARES".into(),
        }
    }

    #[test]
    fn a004_own_leg_mapping_and_sell() {
        // 4 @ 0.40 then 6 @ 0.50, with a 0.02 collateral fee and a 0.1 share fee.
        let buy_leg = leg(
            "Bid",
            "4000000000000000000",
            "400000000000000000",
            Some(collateral_fee("20000000000000000")),
        );
        let second = leg(
            "Bid",
            "6000000000000000000",
            "500000000000000000",
            Some(collateral_fee("0")),
        );
        let events = vec![match_event(buy_leg, vec![]), match_event(second, vec![])];
        let legs = own_legs(&events, "0xhash", "0xaaa").unwrap();
        let facts = cumulative(&legs, U256::from(10_000_000_000_000_000_000u128)).unwrap();
        assert_eq!(
            facts.gross_q_atoms,
            U256::from(10_000_000_000_000_000_000u128)
        );
        // 4 * 0.40 + 6 * 0.50 = 4.60
        assert_eq!(
            facts.gross_cash_atoms,
            U256::from(4_600_000_000_000_000_000u128)
        );
        assert_eq!(
            facts.collateral_fee_atoms,
            Some(U256::from(20_000_000_000_000_000u128))
        );

        let acct = accounting(
            Side::Buy,
            facts.gross_q_atoms,
            facts.gross_cash_atoms,
            facts.collateral_fee_atoms,
            Some(U256::from(100_000_000_000_000_000u128)),
        );
        // Cash out 4.60 + 0.02 = 4.62
        assert_eq!(
            acct.net_cash.value().copied(),
            Some(U256::from(4_620_000_000_000_000_000u128))
        );
        // Inventory 10 - 0.1 = 9.9
        assert_eq!(
            acct.net_inventory.value().copied(),
            Some(U256::from(9_900_000_000_000_000_000u128))
        );
        // VWAP 4.60 / 10 = 0.46
        assert_eq!(
            acct.vwap.value().copied(),
            Some(U256::from(460_000_000_000_000_000u128))
        );
        assert!(acct.is_complete());
    }

    #[test]
    fn a004_sell_net_cash_and_inventory() {
        // 10 @ 0.50 with a 0.02 collateral fee.
        let sell_leg = leg(
            "Ask",
            "10000000000000000000",
            "500000000000000000",
            Some(collateral_fee("20000000000000000")),
        );
        let events = vec![match_event(sell_leg, vec![])];
        let legs = own_legs(&events, "0xhash", "0xaaa").unwrap();
        let facts = cumulative(&legs, U256::from(10_000_000_000_000_000_000u128)).unwrap();
        let acct = accounting(
            Side::Sell,
            facts.gross_q_atoms,
            facts.gross_cash_atoms,
            facts.collateral_fee_atoms,
            None,
        );
        // Cash in 5.00 - 0.02 = 4.98
        assert_eq!(
            acct.net_cash.value().copied(),
            Some(U256::from(4_980_000_000_000_000_000u128))
        );
        // The share fee is unknown, so the inventory isn't.
        assert!(!acct.net_inventory.is_known());
    }

    #[test]
    fn another_accounts_leg_is_not_ours() {
        let other = ApiLeg {
            signer: "0xbbb".into(),
            ..leg("Bid", "1000000000000000000", "400000000000000000", None)
        };
        let events = vec![match_event(other, vec![])];
        assert!(own_legs(&events, "0xhash", "0xaaa").unwrap().is_empty());
    }

    #[test]
    fn another_orders_leg_in_the_same_match_is_not_ours() {
        let mine = leg("Bid", "1000000000000000000", "400000000000000000", None);
        let theirs = ApiLeg {
            hash: "0xother".into(),
            signer: "0xbbb".into(),
            ..leg("Ask", "1000000000000000000", "400000000000000000", None)
        };
        let events = vec![match_event(theirs, vec![mine])];
        assert!(own_legs(&events, "0xother", "0xaaa").unwrap().is_empty());
    }

    #[test]
    fn a006_invalid_units_and_fee() {
        // A negative amount isn't a number.
        let bad = leg("Bid", "-1", "400000000000000000", None);
        let events = vec![match_event(bad, vec![])];
        assert!(own_legs(&events, "0xhash", "0xaaa").is_err());

        // An unknown fee kind is a conflict, not a zero fee.
        let unknown_fee = ApiFee {
            amount: "1".into(),
            fee_type: "GAS".into(),
        };
        let events = vec![match_event(leg("Bid", "1", "1", Some(unknown_fee)), vec![])];
        assert!(own_legs(&events, "0xhash", "0xaaa").is_err());
    }

    #[test]
    fn a_missing_fee_leaves_the_accounting_incomplete() {
        let events = vec![match_event(
            leg("Bid", "1000000000000000000", "400000000000000000", None),
            vec![],
        )];
        let legs = own_legs(&events, "0xhash", "0xaaa").unwrap();
        let facts = cumulative(&legs, U256::from(1_000_000_000_000_000_000u128)).unwrap();
        let acct = accounting(
            Side::Buy,
            facts.gross_q_atoms,
            facts.gross_cash_atoms,
            facts.collateral_fee_atoms,
            facts.share_fee_atoms,
        );
        assert!(!acct.is_complete());
        assert!(!acct.collateral_fee.is_known());
    }

    #[test]
    fn a_zero_quantity_has_no_vwap() {
        let acct = accounting(
            Side::Buy,
            U256::ZERO,
            U256::ZERO,
            Some(U256::ZERO),
            Some(U256::ZERO),
        );
        assert!(!acct.vwap.is_known());
    }

    #[test]
    fn terminal_statuses_other_than_filled_stay_unproven() {
        assert_eq!(
            closure_from_status("CANCELLED", U256::ZERO, U256::from(10)),
            Closure::Unproven
        );
        assert_eq!(
            closure_from_status("EXPIRED", U256::ZERO, U256::from(10)),
            Closure::Unproven
        );
        assert!(matches!(
            closure_from_status("FILLED", U256::from(10), U256::from(10)),
            Closure::Final { .. }
        ));
    }
}
