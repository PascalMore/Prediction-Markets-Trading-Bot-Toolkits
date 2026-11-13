//! Open-position tracking + per-category / per-tag exposure accounting.
//!
//! Single source of truth for "what are we currently long/short?". The order
//! executor inserts on successful entries; the position monitor reads
//! snapshots and removes on exit. Exposure totals are derived on demand —
//! cheap because the working set is bounded by `max_open_positions`-ish.

use crate::bot::runtime::live::ApplyOutcome;
use crate::models::Side;
use crate::service::venue::{AttemptPhase, AttemptRecord, ExecutionSnapshot};
use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OpenPosition {
    pub token_id: String,
    pub slug: String,
    pub category: Option<String>,
    pub tags: Vec<String>,
    pub side: Side,
    pub entry_price: f64,
    pub shares: f64,
    pub usd_notional: f64,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub opened_at: DateTime<Utc>,
}

impl OpenPosition {
    /// Current unrealised P&L percentage given a midprice quote.
    ///
    /// For BUY entries (long the outcome), gain = +(midprice - entry) / entry.
    /// For SELL entries (rare on Polymarket, short the outcome), gain is
    /// inverted.
    pub fn pnl_pct(&self, midprice: f64) -> f64 {
        if self.entry_price <= 0.0 {
            return 0.0;
        }
        let raw = (midprice - self.entry_price) / self.entry_price * 100.0;
        match self.side {
            Side::Buy => raw,
            Side::Sell => -raw,
        }
    }
}

#[derive(Debug, Default)]
pub struct PositionStore {
    inner: RwLock<HashMap<String, OpenPosition>>,
    /// The copy attempts that are open, by their local attempt id.
    attempts: RwLock<HashMap<u64, AttemptRecord>>,
    /// The cumulative gross quantity already applied, by attempt id.
    applied_q: RwLock<HashMap<u64, U256>>,
    /// The cumulative gross cash already applied, by attempt id.
    applied_cash: RwLock<HashMap<u64, U256>>,
    /// Pending exit intents survive errors and cancelled tasks in this store.
    exits: RwLock<
        HashMap<
            String,
            (
                crate::service::venue::AttemptHandle,
                Option<ExecutionSnapshot>,
            ),
        >,
    >,
    /// Serialize cumulative validation and watermark commit.
    execution_lock: parking_lot::Mutex<()>,
    collateral_fees: RwLock<HashMap<u64, U256>>,
    share_fees: RwLock<HashMap<u64, U256>>,
    /// The newest generation accepted, by attempt id.
    generations: RwLock<HashMap<u64, u64>>,
    /// Entry metadata and cumulative net accounting share the exit transaction lock.
    entries: RwLock<HashMap<String, OpenPosition>>,
    net_cash: RwLock<HashMap<u64, U256>>,
    net_inventory: RwLock<HashMap<u64, U256>>,
    coverage: RwLock<HashMap<u64, crate::bot::runtime::live::CumulativeWatermark>>,
    baseline: RwLock<Option<crate::service::venue::AccountSnapshot>>,
    /// Startup inventory is risk-only, not strategy-owned sellable inventory.
    inherited_usd: RwLock<f64>,
}

impl PositionStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Keep an exit handle before awaiting a venue. A later tick only queries it.
    pub fn reserve_exit(
        &self,
        key: &str,
        order: &crate::models::PlannedOrder,
    ) -> (bool, crate::service::venue::AttemptHandle) {
        use crate::service::venue::{AccountIdentity, AccountMode, OrderIdentity, Submission};
        static NEXT_EXIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let mut exits = self.exits.write();
        if let Some((handle, _)) = exits.get(key) {
            return (false, handle.clone());
        }
        let record = AttemptRecord {
            identity: OrderIdentity {
                local_attempt_id: NEXT_EXIT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                signed_hash: None,
                venue_order_id: None,
                market_id: 0,
                token_id: order.token_id.clone(),
                side: order.side,
                account: AccountIdentity {
                    chain_id: 0,
                    trading_address: String::new(),
                    signer_address: String::new(),
                    mode: AccountMode::Eoa,
                    venue: order.venue,
                },
            },
            request_gross_atoms: U256::from((order.shares * 1e18) as u128),
            r0: U256::ZERO,
            reserved_sell: U256::from((order.shares * 1e18) as u128),
            phase: AttemptPhase::Prepared,
            submission: Submission::NotSubmitted,
            last_reason: None,
        };
        let handle = Arc::new(parking_lot::Mutex::new(record));
        exits.insert(key.to_string(), (handle.clone(), None));
        (true, handle)
    }

    /// Return a retained monitor intent before evaluating another price trigger.
    pub fn exit_handle(&self, key: &str) -> Option<crate::service::venue::AttemptHandle> {
        self.exits.read().get(key).map(|(handle, _)| handle.clone())
    }

    /// Retain copy intent and its reservation before the first await.
    pub fn reserve_copy(
        &self,
        key: &str,
        order: &crate::models::PlannedOrder,
        template: OpenPosition,
        r0: U256,
        cfg: &crate::config::AppConfig,
    ) -> anyhow::Result<(bool, crate::service::venue::AttemptHandle)> {
        let _transaction = self.execution_lock.lock();
        if let Some((handle, _)) = self.exits.read().get(key) {
            return Ok((false, handle.clone()));
        }
        let want = crate::bot::runtime::live::wei_to_shares(r0);
        if want > cfg.strategy.max_order_size_usd || !want.is_finite() {
            anyhow::bail!("COPY_ORDER_CAP");
        }
        if let Some(category) = &template.category {
            for (name, cap) in &cfg.filters.per_category_max_open_usd {
                if ci_eq(name, category) && self.open_usd_by_category(category) + want > *cap {
                    anyhow::bail!("COPY_CATEGORY_CAP");
                }
            }
        }
        for tag in &template.tags {
            for (name, cap) in &cfg.filters.per_tag_max_open_usd {
                if ci_eq(name, tag) && self.open_usd_by_tag(tag) + want > *cap {
                    anyhow::bail!("COPY_TAG_CAP");
                }
            }
        }
        let (fresh, handle) = self.reserve_exit(key, order);
        if fresh {
            let mut record = handle.lock();
            record.r0 = r0;
            record.reserved_sell = U256::ZERO;
            self.entries.write().insert(key.to_string(), template);
        }
        Ok((fresh, handle))
    }

    /// A returned preparation error is releasable; cancellation never reaches this path.
    pub fn release_copy_not_submitted(&self, key: &str) {
        let _transaction = self.execution_lock.lock();
        let mut exits = self.exits.write();
        let releasable = exits.get(key).is_some_and(|(handle, _)| {
            let record = handle.lock();
            record.phase == AttemptPhase::Prepared
                && record.submission == crate::service::venue::Submission::NotSubmitted
        });
        if releasable {
            exits.remove(key);
            self.entries.write().remove(key);
        }
    }

    /// Reconcile every pending copy intent without constructing another order.
    pub async fn sync_copy(
        &self,
        venue: &dyn crate::service::venue::OrderVenue,
    ) -> anyhow::Result<()> {
        if self.baseline.read().is_none() {
            let snapshot = venue.account_snapshot().await?;
            crate::service::predict_fun::account::check_snapshot_complete(&snapshot)?;
            let _transaction = self.execution_lock.lock();
            let mut baseline = self.baseline.write();
            if baseline.is_none() {
                let inherited = snapshot
                    .positions
                    .iter()
                    .map(|(_, amount)| crate::bot::runtime::live::wei_to_shares(*amount))
                    .sum::<f64>();
                let mut risk = self.inherited_usd.write();
                *risk = risk.max(inherited);
                if !snapshot.positions_explained {
                    venue.record_account_reconciliation("UNEXPLAINED_POSITIONS");
                    anyhow::bail!("RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS");
                }
                if !self.exits.read().is_empty() || !snapshot.open_orders.is_empty() {
                    anyhow::bail!("RECONCILIATION_REQUIRED:COPY_STARTUP_RISK");
                }
                *baseline = Some(snapshot);
            }
        }
        let keys: Vec<_> = self.entries.read().keys().cloned().collect();
        for key in keys {
            let hash = self
                .exits
                .read()
                .get(&key)
                .and_then(|(handle, _)| handle.lock().identity.signed_hash.clone());
            if let Some(hash) = hash {
                let snapshot = venue.execution_snapshot(&hash).await?;
                if let Err(reason) = self.apply_exit(&key, &snapshot) {
                    venue.record_account_reconciliation(&reason);
                    return Err(anyhow::anyhow!(reason));
                }
            }
        }
        if self
            .exits
            .read()
            .values()
            .any(|(handle, _)| handle.lock().phase != AttemptPhase::Closed)
        {
            return Ok(());
        }
        let current = venue.account_snapshot().await?;
        let _transaction = self.execution_lock.lock();
        let exits = self.exits.read();
        if exits
            .values()
            .any(|(handle, _)| handle.lock().phase != AttemptPhase::Closed)
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_READ_RACE");
        }
        let mut changes = Vec::new();
        for (handle, _) in exits.values() {
            let record = handle.lock();
            let id = record.identity.local_attempt_id;
            if let (Some(cash), Some(inventory)) = (
                self.net_cash.read().get(&id),
                self.net_inventory.read().get(&id),
            ) {
                changes.push((
                    record.identity.token_id.clone(),
                    record.identity.side,
                    *cash,
                    *inventory,
                ));
            }
        }
        let result = crate::bot::runtime::live::check_account_delta(
            self.baseline.read().as_ref().unwrap(),
            &current,
            changes,
        );
        if result.is_err() {
            venue.record_account_reconciliation("COPY_ACCOUNT_LEDGER_MISMATCH");
        }
        result
    }

    /// Apply a complete cumulative exit once, holding the exit lock through
    /// validation, inventory mutation and watermark commit.
    pub fn apply_exit(&self, key: &str, snapshot: &ExecutionSnapshot) -> Result<(), String> {
        use crate::bot::runtime::live::wei_to_shares;
        let _transaction = self.execution_lock.lock();
        let mut exits = self.exits.write();
        let (handle, previous) = exits.get_mut(key).ok_or("UNKNOWN_ATTEMPT")?;
        let mut record = handle.lock();
        if record.identity.signed_hash.is_none()
            || snapshot.identity.signed_hash != record.identity.signed_hash
            || snapshot.identity.account != record.identity.account
            || snapshot.identity.token_id != record.identity.token_id
            || snapshot.identity.side != record.identity.side
        {
            return Err("ACCOUNT_MISMATCH".into());
        }
        let id = record.identity.local_attempt_id;
        let old = self.coverage.read().get(&id).cloned().unwrap_or_default();
        let Some(watermark) = crate::bot::runtime::live::advance_watermark(&old, snapshot)? else {
            return Ok(());
        };
        if snapshot
            .gross_q()
            .is_none_or(|q| q > record.request_gross_atoms)
        {
            return Err("QUANTITY_CONFLICT".into());
        }
        if !snapshot.pages_complete
            || !snapshot.accounting_complete()
            || snapshot.accounting_gross_q != snapshot.gross_q()
        {
            self.coverage.write().insert(id, watermark);
            return Ok(());
        }
        if snapshot
            .gross_q()
            .is_none_or(|q| q > record.request_gross_atoms)
        {
            return Err("QUANTITY_CONFLICT".into());
        }
        let net = *snapshot
            .accounting
            .net_inventory
            .value()
            .ok_or("MISSING_NET")?;
        let old_net = previous
            .as_ref()
            .and_then(|s| s.accounting.net_inventory.value())
            .copied()
            .unwrap_or(U256::ZERO);
        let delta = net.checked_sub(old_net).ok_or("CUMULATIVE_DECREASE")?;
        let mut positions = self.inner.write();
        if record.identity.side == Side::Buy {
            let template = self
                .entries
                .read()
                .get(key)
                .cloned()
                .ok_or("MISSING_COPY_METADATA")?;
            let cash = *snapshot
                .accounting
                .net_cash
                .value()
                .ok_or("MISSING_NET_CASH")?;
            let old_cash = previous
                .as_ref()
                .and_then(|snapshot| snapshot.accounting.net_cash.value())
                .copied()
                .unwrap_or(U256::ZERO);
            let cash_delta = cash.checked_sub(old_cash).ok_or("NET_CASH_DECREASE")?;
            if !delta.is_zero() || !cash_delta.is_zero() {
                let position =
                    positions
                        .entry(record.identity.token_id.clone())
                        .or_insert(OpenPosition {
                            shares: 0.0,
                            usd_notional: 0.0,
                            ..template
                        });
                position.shares += wei_to_shares(delta);
                position.usd_notional += wei_to_shares(cash_delta);
                position.entry_price = position.usd_notional / position.shares;
            }
        } else if let Some(position) = positions.get_mut(&record.identity.token_id) {
            let shares = wei_to_shares(delta);
            if shares > position.shares + 1e-9 {
                return Err("INVENTORY_CONFLICT".into());
            }
            let left = (position.shares - shares).max(0.0);
            position.usd_notional *= left / position.shares;
            position.shares = left;
            if left <= 1e-9 {
                positions.remove(&record.identity.token_id);
            }
        } else if !delta.is_zero() {
            return Err("INVENTORY_CONFLICT".into());
        }
        let id = record.identity.local_attempt_id;
        self.net_cash.write().insert(
            id,
            *snapshot
                .accounting
                .net_cash
                .value()
                .ok_or("MISSING_NET_CASH")?,
        );
        self.net_inventory.write().insert(id, net);
        self.collateral_fees.write().insert(
            id,
            *snapshot
                .accounting
                .collateral_fee
                .value()
                .ok_or("MISSING_FEE")?,
        );
        self.share_fees.write().insert(
            id,
            *snapshot.accounting.share_fee.value().ok_or("MISSING_FEE")?,
        );
        self.generations.write().insert(id, snapshot.generation);
        self.applied_q
            .write()
            .insert(id, snapshot.gross_q().ok_or("MISSING_Q")?);
        self.applied_cash.write().insert(
            id,
            *snapshot
                .accounting
                .gross_cash
                .value()
                .ok_or("MISSING_CASH")?,
        );
        if matches!(snapshot.closure, crate::service::venue::Closure::Final { final_gross_q, .. } if Some(final_gross_q) == snapshot.gross_q())
        {
            record.phase = AttemptPhase::Closed;
        }
        self.coverage.write().insert(id, watermark);
        *previous = Some(snapshot.clone());
        Ok(())
    }

    pub fn open(&self, pos: OpenPosition) {
        self.inner.write().insert(pos.token_id.clone(), pos);
    }

    pub fn close(&self, token_id: &str) -> Option<OpenPosition> {
        self.inner.write().remove(token_id)
    }

    /// Take `shares` off a position after a partial exit.
    pub fn reduce(&self, token_id: &str, shares: f64) {
        if let Some(pos) = self.inner.write().get_mut(token_id) {
            let left = (pos.shares - shares).max(0.0);
            if pos.shares > 0.0 {
                pos.usd_notional *= left / pos.shares;
            }
            pos.shares = left;
        }
    }

    /// Record a copy attempt. It stays until the attempt is closed.
    pub fn record_attempt(&self, record: AttemptRecord) {
        let id = record.identity.local_attempt_id;
        self.attempts.write().insert(id, record);
    }

    /// Read a copy attempt.
    pub fn attempt(&self, local_attempt_id: u64) -> Option<AttemptRecord> {
        self.attempts.read().get(&local_attempt_id).cloned()
    }

    /// The attempts that may have reached the venue.
    pub fn open_attempts(&self) -> Vec<AttemptRecord> {
        self.attempts
            .read()
            .values()
            .filter(|record| record.phase != AttemptPhase::Closed)
            .cloned()
            .collect()
    }

    /// Close an attempt and drop it.
    pub fn close_attempt(&self, local_attempt_id: u64) {
        self.attempts.write().remove(&local_attempt_id);
    }

    /// What a reading adds to the ledger, given what's already applied.
    ///
    /// The same rules as the broker's live path: an older generation is
    /// dropped, the same generation adds nothing, and a cumulative amount that
    /// went down is a conflict.
    pub fn apply_execution(
        &self,
        local_attempt_id: u64,
        snapshot: &ExecutionSnapshot,
    ) -> ApplyOutcome {
        let _transaction = self.execution_lock.lock();
        let applied_q = self
            .applied_q
            .read()
            .get(&local_attempt_id)
            .copied()
            .unwrap_or(U256::ZERO);
        let applied_cash = self
            .applied_cash
            .read()
            .get(&local_attempt_id)
            .copied()
            .unwrap_or(U256::ZERO);
        let generation = self.generations.read().get(&local_attempt_id).copied();

        let outcome = crate::bot::runtime::live::apply_cumulative(
            applied_q,
            applied_cash,
            self.collateral_fees.read().get(&local_attempt_id).copied(),
            self.share_fees.read().get(&local_attempt_id).copied(),
            generation,
            snapshot,
        );
        if let ApplyOutcome::Delta(delta) = &outcome {
            *self.applied_q.write().entry(local_attempt_id).or_default() += delta.gross_q;
            *self
                .applied_cash
                .write()
                .entry(local_attempt_id)
                .or_default() += delta.gross_cash;
            if let Some(value) = snapshot.accounting.collateral_fee.value() {
                self.collateral_fees
                    .write()
                    .insert(local_attempt_id, *value);
            }
            if let Some(value) = snapshot.accounting.share_fee.value() {
                self.share_fees.write().insert(local_attempt_id, *value);
            }
            self.generations
                .write()
                .insert(local_attempt_id, snapshot.generation);
        }
        outcome
    }

    /// The cumulative quantity applied for an attempt.
    pub fn applied_quantity(&self, local_attempt_id: u64) -> U256 {
        self.applied_q
            .read()
            .get(&local_attempt_id)
            .copied()
            .unwrap_or(U256::ZERO)
    }

    pub fn get(&self, token_id: &str) -> Option<OpenPosition> {
        self.inner.read().get(token_id).cloned()
    }

    pub fn snapshot(&self) -> Vec<OpenPosition> {
        self.inner.read().values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Residual entry reservation, never R0 plus already recorded cost.
    fn pending_entry_usd(&self, matches: impl Fn(&OpenPosition) -> bool) -> f64 {
        let exits = self.exits.read();
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(_, template)| matches(template))
            .map(|(key, _)| {
                let Some((handle, snapshot)) = exits.get(key) else {
                    return 0.0;
                };
                let record = handle.lock();
                if record.phase == AttemptPhase::Closed {
                    return 0.0;
                }
                let confirmed = snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.accounting.net_cash.value())
                    .copied()
                    .unwrap_or(U256::ZERO);
                let risk = self
                    .coverage
                    .read()
                    .get(&record.identity.local_attempt_id)
                    .map(|coverage| coverage.risk_lower_bound())
                    .unwrap_or(U256::ZERO);
                crate::bot::runtime::live::wei_to_shares(
                    record.r0.max(risk).saturating_sub(confirmed),
                )
            })
            .sum()
    }

    pub fn open_usd_by_category(&self, category: &str) -> f64 {
        let held: f64 = self
            .inner
            .read()
            .values()
            .filter(|p| {
                p.category
                    .as_ref()
                    .map(|c| ci_eq(c, category))
                    .unwrap_or(false)
            })
            .map(|p| p.usd_notional)
            .sum();
        held + *self.inherited_usd.read()
            + self.pending_entry_usd(|position| {
                position
                    .category
                    .as_deref()
                    .is_some_and(|value| ci_eq(value, category))
            })
    }

    pub fn open_usd_by_tag(&self, tag: &str) -> f64 {
        let held: f64 = self
            .inner
            .read()
            .values()
            .filter(|p| p.tags.iter().any(|t| ci_eq(t, tag)))
            .map(|p| p.usd_notional)
            .sum();
        held + *self.inherited_usd.read()
            + self.pending_entry_usd(|position| position.tags.iter().any(|value| ci_eq(value, tag)))
    }
}

fn ci_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.chars()
            .zip(b.chars())
            .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a007_copy_complete_budget_is_atomic_and_only_unsent_releases() {
        let mut cfg: crate::config::AppConfig =
            serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.filters = Default::default();
        cfg.strategy.max_order_size_usd = 20.0;
        cfg.filters
            .per_category_max_open_usd
            .insert("sports".into(), 10.0);
        let store = PositionStore::new();
        let order = crate::models::PlannedOrder {
            venue: crate::models::VenueId::PredictFun,
            token_id: "77".into(),
            side: Side::Buy,
            shares: 10.0,
            limit_price: 0.5,
            usd_notional: 5.0,
            order_type: crate::models::OrderType::Fak,
            source_trade_hash: None,
        };
        let r0 = U256::from(5_250_000_000_000_000_000u128);
        let workers: Vec<_> = (0..8)
            .map(|index| {
                let (store, cfg, order) = (store.clone(), cfg.clone(), order.clone());
                std::thread::spawn(move || {
                    store
                        .reserve_copy(
                            &format!("copy-{index}"),
                            &order,
                            pos("copy", "sports", &[], 0.0),
                            r0,
                            &cfg,
                        )
                        .ok()
                        .map(|(_, handle)| (index, handle))
                })
            })
            .collect();
        let reservations: Vec<_> = workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(reservations.len(), 1);
        assert_eq!(store.open_usd_by_category("sports"), 5.25);
        let (index, handle) = &reservations[0];
        assert_eq!(handle.lock().r0, r0);
        let key = format!("copy-{index}");
        handle.lock().phase = AttemptPhase::MayHaveSubmitted;
        store.release_copy_not_submitted(&key);
        assert_eq!(store.open_usd_by_category("sports"), 5.25);
        assert!(
            !store
                .reserve_copy(&key, &order, pos("copy", "sports", &[], 0.0), r0, &cfg)
                .unwrap()
                .0
        );
        // Separate never-submitted attempt: release and reserve again normally.
        cfg.filters.per_category_max_open_usd.clear();
        let (_, unsent) = store
            .reserve_copy("unsent", &order, pos("copy", "sports", &[], 0.0), r0, &cfg)
            .unwrap();
        assert_eq!(
            unsent.lock().submission,
            crate::service::venue::Submission::NotSubmitted
        );
        store.release_copy_not_submitted("unsent");
        assert!(
            store
                .reserve_copy("unsent", &order, pos("copy", "sports", &[], 0.0), r0, &cfg)
                .unwrap()
                .0
        );
        cfg.filters.per_tag_max_open_usd.insert("test".into(), 5.0);
        assert!(store
            .reserve_copy("tag", &order, pos("tag", "other", &["test"], 0.0), r0, &cfg)
            .is_err());
    }

    fn pos(slug: &str, cat: &str, tags: &[&str], usd: f64) -> OpenPosition {
        OpenPosition {
            token_id: format!("tok-{slug}"),
            slug: slug.into(),
            category: Some(cat.into()),
            tags: tags.iter().map(|s| s.to_string()).collect(),
            side: Side::Buy,
            entry_price: 0.50,
            shares: usd * 2.0,
            usd_notional: usd,
            take_profit_pct: 50.0,
            stop_loss_pct: 30.0,
            opened_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a005_store_real_adapter_coverage_cash_fees_and_watermark() {
        use crate::models::{OrderType, PlannedOrder, VenueId};
        use crate::service::venue::OrderVenue;
        use std::sync::atomic::Ordering;
        for fault in ["missing_fee", "revised_fee", "decrease", "receipt_error"] {
            let (trader, transport) = crate::service::predict_fun::fills::tests::scenario(
                if fault == "missing_fee" {
                    "missing_fee"
                } else {
                    "fills"
                },
                "",
            );
            let store = PositionStore::new();
            let mut template = pos("a", "Crypto", &[], 0.0);
            template.token_id = "77".into();
            let order = PlannedOrder {
                venue: VenueId::PredictFun,
                token_id: "77".into(),
                side: Side::Buy,
                shares: 10.0,
                limit_price: 0.5,
                usd_notional: 5.0,
                order_type: OrderType::Fak,
                source_trade_hash: None,
            };
            let cfg = serde_json::from_str(include_str!("../../config.json")).unwrap();
            let (_, handle) = store
                .reserve_copy(
                    "copy",
                    &order,
                    template,
                    trader.reservation_atoms(&order).unwrap(),
                    &cfg,
                )
                .unwrap();
            let ticket = trader
                .place_tracked(&order, 60, true, handle.clone())
                .await
                .unwrap();
            let first = ticket.execution.unwrap();
            store.apply_exit("copy", &first).unwrap();
            let id = handle.lock().identity.local_attempt_id;
            let hash = handle.lock().identity.signed_hash.clone().unwrap();
            if fault == "missing_fee" {
                assert!(store.get("77").is_none());
                assert!(!store.applied_q.read().contains_key(&id));
                assert!(store.coverage.read().contains_key(&id));
                transport.base.stage.store(1, Ordering::SeqCst);
                let full = trader.execution_snapshot(&hash).await.unwrap();
                store.apply_exit("copy", &full).unwrap();
                store.apply_exit("copy", &full).unwrap();
                store.apply_exit("copy", &first).unwrap();
                assert!((store.get("77").unwrap().shares - 9.9).abs() < 1e-12);
                assert!((store.get("77").unwrap().usd_notional - 4.62).abs() < 1e-12);
                assert_eq!(
                    store.net_cash.read()[&id],
                    *full.accounting.net_cash.value().unwrap()
                );
                assert_eq!(
                    store.net_inventory.read()[&id],
                    *full.accounting.net_inventory.value().unwrap()
                );
                assert_eq!(
                    store.collateral_fees.read()[&id],
                    *full.accounting.collateral_fee.value().unwrap()
                );
                assert_eq!(
                    store.share_fees.read()[&id],
                    *full.accounting.share_fee.value().unwrap()
                );
                assert_eq!(store.generations.read()[&id], full.generation);
            } else {
                if fault == "decrease" {
                    transport.base.stage.store(1, Ordering::SeqCst);
                    store
                        .apply_exit("copy", &trader.execution_snapshot(&hash).await.unwrap())
                        .unwrap();
                    transport.base.stage.store(0, Ordering::SeqCst);
                } else {
                    *transport.mode.lock() = fault;
                }
                let before = store.get("77").unwrap();
                let cash = store.net_cash.read()[&id];
                let fee = store.collateral_fees.read()[&id];
                let inventory = store.net_inventory.read()[&id];
                let generation = store.generations.read()[&id];
                let snapshot = trader.execution_snapshot(&hash).await.unwrap();
                let result = store.apply_exit("copy", &snapshot);
                assert_eq!(result.is_err(), fault != "receipt_error", "{fault}");
                assert_eq!(store.get("77").unwrap().shares, before.shares);
                assert_eq!(store.get("77").unwrap().usd_notional, before.usd_notional);
                assert_eq!(store.net_cash.read()[&id], cash);
                assert_eq!(store.net_inventory.read()[&id], inventory);
                assert_eq!(store.collateral_fees.read()[&id], fee);
                assert_eq!(store.generations.read()[&id], generation);
            }
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn fix_r1_store_scan_growth() {
        store_scan_or_zero(false).await;
    }

    #[tokio::test]
    async fn fix_r1_store_terminal_zero() {
        store_scan_or_zero(true).await;
    }

    async fn store_scan_or_zero(zero: bool) {
        use crate::models::{OrderType, PlannedOrder, VenueId};
        use crate::service::venue::OrderVenue;
        use std::sync::atomic::Ordering;
        {
            let (trader, transport) = crate::service::predict_fun::fills::tests::scenario(
                if zero { "terminal" } else { "fills" },
                if zero { "" } else { "scan_growth" },
            );
            let store = PositionStore::new();
            store.sync_copy(&trader).await.unwrap();
            let mut template = pos("a", "Crypto", &[], 0.0);
            template.token_id = "77".into();
            let order = PlannedOrder {
                venue: VenueId::PredictFun,
                token_id: "77".into(),
                side: Side::Buy,
                shares: 10.0,
                limit_price: 0.5,
                usd_notional: 5.0,
                order_type: OrderType::Fak,
                source_trade_hash: None,
            };
            let cfg = serde_json::from_str(include_str!("../../config.json")).unwrap();
            let r0 = trader.reservation_atoms(&order).unwrap();
            let (_, handle) = store
                .reserve_copy("copy", &order, template.clone(), r0, &cfg)
                .unwrap();
            let ticket = trader
                .place_tracked(&order, 60, true, handle.clone())
                .await
                .unwrap();
            let first = ticket.execution.unwrap();
            if !zero {
                assert_eq!(
                    first.gross_q_before,
                    U256::from(4) * U256::from(10u64.pow(18))
                );
                assert_eq!(
                    first.gross_q_after,
                    U256::from(10) * U256::from(10u64.pow(18))
                );
            }
            store.apply_exit("copy", &first).unwrap();
            if !zero {
                assert!(store.get("77").is_none());
                assert_eq!(store.open_usd_by_category("Crypto"), 5.0);
            }
            *transport.mode.lock() = "";
            store.sync_copy(&trader).await.unwrap();
            store.sync_copy(&trader).await.unwrap();
            assert_eq!(handle.lock().phase, AttemptPhase::Closed);
            assert!(
                !store
                    .reserve_copy("copy", &order, template, r0, &cfg)
                    .unwrap()
                    .0
            );
            if zero {
                assert!(store.is_empty());
                assert_eq!(store.open_usd_by_category("Crypto"), 0.0);
                *transport.mode.lock() = "cash_drift";
                assert!(
                    store.sync_copy(&trader).await.is_err(),
                    "closed zero resumes account comparison"
                );
            } else {
                assert!((store.get("77").unwrap().shares - 9.9).abs() < 1e-12);
                assert!((store.get("77").unwrap().usd_notional - 4.62).abs() < 1e-12);
            }
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn exit_cumulative_net_inventory_is_applied_once() {
        use crate::models::{OrderType, PlannedOrder, VenueId};
        use crate::service::venue::{Closure, Quantity, QuantityClass, Submission};
        let store = PositionStore::new();
        let initial = pos("a", "Politics", &[], 50.0);
        store.open(initial.clone());
        let order = PlannedOrder {
            venue: VenueId::PredictFun,
            token_id: initial.token_id.clone(),
            side: Side::Sell,
            shares: initial.shares,
            limit_price: 0.5,
            usd_notional: 50.0,
            order_type: OrderType::Fak,
            source_trade_hash: None,
        };
        let (_, handle) = store.reserve_exit("exit", &order);
        handle.lock().identity.signed_hash = Some("original-hash".into());
        let gross = U256::from(40_000_000_000_000_000_000u128);
        let snapshot = ExecutionSnapshot {
            identity: handle.lock().identity.clone(),
            submission: Submission::Accepted,
            quantity: Quantity::Known {
                gross_q_atoms: gross,
                class: QuantityClass::Partial,
            },
            accounting: crate::service::predict_fun::fills::accounting(
                Side::Sell,
                gross,
                U256::from(20_000_000_000_000_000_000u128),
                Some(U256::from(20_000_000_000_000_000u128)),
                Some(U256::ZERO),
            ),
            accounting_gross_q: Some(gross),
            closure: Closure::Unproven,
            generation: 1,
            pages_complete: true,
            gross_q_before: gross,
            gross_q_after: gross,
            source: "UNIT_FAKE".into(),
            observed_at: Utc::now(),
            tx_hash: None,
        };
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let snapshot = snapshot.clone();
                std::thread::spawn(move || store.apply_exit("exit", &snapshot).unwrap())
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let remaining = store.get(&initial.token_id).unwrap();
        assert_eq!(remaining.shares, 60.0);
        assert_eq!(remaining.usd_notional, 30.0);
        let mut revised = snapshot.clone();
        revised.generation = 2;
        revised.accounting.collateral_fee = crate::service::venue::Field::Known(U256::ZERO);
        assert!(store.apply_exit("exit", &revised).is_err());
        assert_eq!(store.get(&initial.token_id).unwrap().shares, 60.0);
    }

    #[test]
    fn category_exposure_sums_only_same_category() {
        let s = PositionStore::default();
        s.open(pos("a", "Politics", &[], 100.0));
        s.open(pos("b", "Politics", &[], 50.0));
        s.open(pos("c", "Crypto", &[], 200.0));
        assert_eq!(s.open_usd_by_category("Politics"), 150.0);
        assert_eq!(s.open_usd_by_category("crypto"), 200.0);
    }

    #[test]
    fn tag_exposure_sums_across_multiple_tags() {
        let s = PositionStore::default();
        s.open(pos("a", "Politics", &["election2024", "us"], 100.0));
        s.open(pos("b", "Politics", &["us"], 50.0));
        assert_eq!(s.open_usd_by_tag("us"), 150.0);
        assert_eq!(s.open_usd_by_tag("election2024"), 100.0);
    }

    #[test]
    fn pnl_pct_buy_long_direction() {
        let mut p = pos("a", "X", &[], 100.0);
        p.entry_price = 0.50;
        assert!((p.pnl_pct(0.75) - 50.0).abs() < 1e-9);
        assert!((p.pnl_pct(0.25) + 50.0).abs() < 1e-9);
    }

    #[test]
    fn pnl_pct_sell_short_direction() {
        let mut p = pos("a", "X", &[], 100.0);
        p.side = Side::Sell;
        p.entry_price = 0.50;
        assert!((p.pnl_pct(0.25) - 50.0).abs() < 1e-9);
    }
}
