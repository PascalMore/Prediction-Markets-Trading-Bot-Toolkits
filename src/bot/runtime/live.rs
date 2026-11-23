//! Live state for the broker: reservations, cumulative facts and tombstones.
//!
//! Everything here happens under the broker's own state lock. A reservation is
//! taken before anything can be sent, a cumulative reading is applied exactly
//! once, and an intent that may have reached the venue is never re-signed.

use crate::bot::runtime::{Blocked, Broker, State, TakeResult};
use crate::models::Side;
use crate::service::venue::{
    AttemptPhase, AttemptRecord, Closure, ExecutionSnapshot, Field, Quantity, QuantityClass,
    Submission,
};
use alloy_primitives::U256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod claims;

/// The state the live path keeps, in the broker's lock.
#[derive(Debug, Default)]
pub struct LiveState {
    /// Every attempt this run made, by its local id.
    pub attempts: HashMap<u64, crate::service::venue::AttemptHandle>,
    /// The next local attempt id.
    pub next_attempt_id: u64,
    /// The cumulative gross quantity already applied, by attempt id.
    pub applied_q: HashMap<u64, U256>,
    /// The cumulative gross cash already applied, by attempt id.
    pub applied_cash: HashMap<u64, U256>,
    /// The cumulative collateral fee already applied, by attempt id.
    pub applied_collateral_fee: HashMap<u64, U256>,
    /// The cumulative share fee already applied, by attempt id.
    pub applied_share_fee: HashMap<u64, U256>,
    /// The newest generation accepted, by attempt id.
    pub generation: HashMap<u64, u64>,
    /// Intents that may have been submitted. Their key stops a new order for
    /// the same thing, however healthy the account looks later.
    pub tombstones: HashMap<String, u64>,
    pub watermarks: HashMap<u64, CumulativeWatermark>,
    pub net_cash: HashMap<u64, U256>,
    pub net_inventory: HashMap<u64, U256>,
    pub reservations: HashMap<u64, U256>,
    /// Claimed payouts already received, by attempt id: the claimed inventory
    /// and the exact cash atoms the venue paid. Written only after a complete
    /// account read reconciles the increase atom-for-atom and the claim
    /// matches, atom for atom, what the confirmed official result owes.
    pub claims: HashMap<u64, (U256, U256)>,
    /// Claims the account read proved but the official result has not
    /// confirmed yet, by attempt id: the claimed inventory and the cash atoms
    /// the venue paid.
    ///
    /// A claim here is not ledger cash. It becomes cash only when the
    /// confirmed official result owes exactly this amount; a claim one atom
    /// off stays here and keeps the run waiting.
    pub pending_claims: HashMap<u64, (U256, U256)>,
    /// The conservative cash risk observed for an attempt, by attempt id.
    ///
    /// It only ever rises: once the venue showed a gross cash or a fee above
    /// what was reserved, that higher risk is kept even when the snapshot
    /// itself couldn't be applied. It is a floor on the risk, not a second
    /// record of what was traded.
    pub observed_risk_floor: HashMap<u64, U256>,
    pub fees_in_cash: f64,
    /// Fixed starting observation for in-process account/ledger reconciliation.
    pub baseline: Option<crate::service::venue::AccountSnapshot>,
    /// The controlled live run's own metadata.
    ///
    /// It is `None` in every run that isn't an explicit controlled live run:
    /// an ordinary live run has no candidate, no stop flag and no controlled
    /// output of its own.
    pub controlled: Option<crate::bot::runtime::live_control::ControlledLiveState>,
}

#[derive(Debug, Default, Clone)]
pub struct CumulativeWatermark {
    generation: u64,
    gross_q: U256,
    fields: HashMap<&'static str, (U256, U256)>,
}

impl CumulativeWatermark {
    /// Known net risk at current coverage, otherwise the confirmed gross risk.
    pub(crate) fn risk_lower_bound(&self) -> U256 {
        let cash = self
            .fields
            .get("cash")
            .map(|(_, value)| *value)
            .unwrap_or(U256::ZERO);
        let fee = self
            .fields
            .get("collateral_fee")
            .map(|(_, value)| *value)
            .unwrap_or(U256::ZERO);
        self.fields
            .get("net_cash")
            .map(|(_, value)| *value)
            .unwrap_or(U256::ZERO)
            .max(cash.saturating_add(fee))
    }
}

/// Validate per-field coverage without mutating any consumer's ledger.
pub(crate) fn advance_watermark(
    old: &CumulativeWatermark,
    snapshot: &ExecutionSnapshot,
) -> Result<Option<CumulativeWatermark>, String> {
    if snapshot.generation <= old.generation {
        return Ok(None);
    }
    let q = snapshot.gross_q().ok_or("QUANTITY_UNKNOWN")?;
    if q < old.gross_q {
        return Err("CUMULATIVE_DECREASE".into());
    }
    let mut fields = old.fields.clone();
    for (name, field) in [
        ("cash", &snapshot.accounting.gross_cash),
        ("collateral_fee", &snapshot.accounting.collateral_fee),
        ("share_fee", &snapshot.accounting.share_fee),
        ("net_cash", &snapshot.accounting.net_cash),
        ("net_inventory", &snapshot.accounting.net_inventory),
    ] {
        if let (Some(value), Some(coverage)) = (field.value(), snapshot.accounting_gross_q) {
            if let Some((covered_q, previous)) = fields.get(name) {
                if coverage < *covered_q
                    || value < previous
                    || (*covered_q == coverage && value != previous)
                {
                    return Err("CUMULATIVE_FIELD_REVISED".into());
                }
            }
            fields.insert(name, (coverage, *value));
        }
    }
    Ok(Some(CumulativeWatermark {
        generation: snapshot.generation,
        gross_q: q,
        fields,
    }))
}

/// The result of applying one cumulative reading.
///
/// It carries the original [`Delta`] plus the two net amounts the original
/// delta could not express, and whether the reading was applied at all. The
/// original [`Delta`] and its consumers keep their contract: this is a
/// wrapper around the same commit, not a second accounting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppliedExecution {
    /// The original gross delta.
    pub delta: Delta,
    /// The net cash actually applied, when the reading was complete.
    pub net_cash_delta: Option<U256>,
    /// The net inventory actually applied, when the reading was complete.
    pub net_inventory_delta: Option<U256>,
    /// Whether this reading changed the ledger.
    pub applied: bool,
    /// Whether this reading repeated facts that were already applied.
    pub duplicate: bool,
    /// Whether the accounting was complete enough to trust.
    pub accounting_complete: bool,
}

/// Validate all cumulative fields first, then commit holdings and watermarks
/// under the caller's existing State lock. Unknown fields retain old coverage.
pub(super) fn apply_snapshot(
    state: &mut State,
    snapshot: &ExecutionSnapshot,
) -> Result<Delta, String> {
    apply_snapshot_detailed(state, snapshot).map(|applied| applied.delta)
}

/// Apply one cumulative reading and report what it actually applied.
///
/// The risk floor is raised here, before any refusal: a snapshot that showed
/// a higher gross cash or fee keeps that higher risk even when the commit
/// itself was rejected, so an observed risk is never lost because the
/// accounting couldn't be completed.
pub(super) fn apply_snapshot_detailed(
    state: &mut State,
    snapshot: &ExecutionSnapshot,
) -> Result<AppliedExecution, String> {
    let id = snapshot.identity.local_attempt_id;
    let handle = state
        .live
        .attempts
        .get(&id)
        .cloned()
        .ok_or("UNBOUND_ATTEMPT")?;
    let mut record = handle.lock();
    if snapshot.identity != record.identity {
        return Err("ORDER_IDENTITY_MISMATCH".into());
    }
    // The conservative risk this reading shows, whether or not it can be
    // applied. It only ever rises.
    let observed = observed_risk_of(snapshot);
    let floor = state.live.observed_risk_floor.entry(id).or_insert(U256::ZERO);
    if observed > *floor {
        *floor = observed;
    }
    let old = {
        state.live.watermarks.entry(id).or_default().clone()
    };
    // A newer read that cannot state its quantity contradicts nothing: for an
    // order the venue already accepted, its fills may still be arriving. That
    // is an incomplete fact, so no watermark advances and nothing is applied.
    let quantity_unread = snapshot.generation > old.generation && snapshot.gross_q().is_none();
    let advanced = if quantity_unread {
        None
    } else {
        advance_watermark(&old, snapshot)?
    };
    if advanced.is_none() && !quantity_unread {
        // Nothing new: the facts are already in the ledger.
        return Ok(AppliedExecution {
            delta: Delta::default(),
            net_cash_delta: Some(U256::ZERO),
            net_inventory_delta: Some(U256::ZERO),
            applied: false,
            duplicate: true,
            accounting_complete: state
                .live
                .watermarks
                .get(&id)
                .is_some_and(|watermark| watermark.fields.len() == 5),
        });
    }
    let old_risk = old.risk_lower_bound();
    let Some(watermark) = advanced else {
        let applied = incomplete_reading(state, &record, snapshot, old_risk);
        drop(record);
        super::live_control::refresh_live_risk(state);
        return Ok(applied);
    };
    let q = watermark.gross_q;
    if q > record.request_gross_atoms {
        return Err("OVERFILL".into());
    }
    let known_risk = watermark.risk_lower_bound();
    let next = watermark.fields;
    let complete = snapshot.pages_complete
        && snapshot.accounting_complete()
        && next.values().all(|(coverage, _)| *coverage == q)
        && next.len() == 5;
    let previous_cash = state.live.net_cash.get(&id).copied().unwrap_or(U256::ZERO);
    let previous_inventory = state
        .live
        .net_inventory
        .get(&id)
        .copied()
        .unwrap_or(U256::ZERO);
    let mut delta = Delta::default();
    let mut net_cash_delta = None;
    let mut net_inventory_delta = None;
    let mut applied = false;
    if complete {
        let cash = next["net_cash"].1;
        let inventory = next["net_inventory"].1;
        let cash_delta = cash.checked_sub(previous_cash).ok_or("NET_CASH_DECREASE")?;
        let inventory_delta = inventory
            .checked_sub(previous_inventory)
            .ok_or("NET_INVENTORY_DECREASE")?;
        let fee = next["collateral_fee"].1;
        let old_fee = state
            .live
            .applied_collateral_fee
            .get(&id)
            .copied()
            .unwrap_or(U256::ZERO);
        delta = Delta {
            gross_q: q - state.live.applied_q.get(&id).copied().unwrap_or(U256::ZERO),
            gross_cash: next["cash"].1
                - state
                    .live
                    .applied_cash
                    .get(&id)
                    .copied()
                    .unwrap_or(U256::ZERO),
            collateral_fee: fee.checked_sub(old_fee).ok_or("FEE_DECREASE")?,
            share_fee: next["share_fee"].1
                - state
                    .live
                    .applied_share_fee
                    .get(&id)
                    .copied()
                    .unwrap_or(U256::ZERO),
        };
        let shares = wei_to_shares(inventory_delta);
        let cash = wei_to_shares(cash_delta);
        match record.identity.side {
            Side::Buy => {
                let holding = state
                    .holdings
                    .entry(record.identity.token_id.clone())
                    .or_insert_with(|| crate::bot::runtime::Holding {
                        market_id: record.identity.market_id,
                        token: record.identity.token_id.clone(),
                        shares: 0.0,
                        cost: 0.0,
                        opened_at: chrono::Utc::now(),
                    });
                holding.shares += shares;
                holding.cost += cash;
                if holding.shares <= super::EPS {
                    state.holdings.remove(&record.identity.token_id);
                }
            }
            Side::Sell => {
                if shares > 0.0 {
                    let holding = state
                        .holdings
                        .get_mut(&record.identity.token_id)
                        .ok_or("SELL_NOT_HELD")?;
                    if shares > holding.shares + super::EPS {
                        return Err("SELL_INVENTORY_MISMATCH".into());
                    }
                    let basis = holding.avg_price() * shares;
                    state.realized += cash - basis;
                    holding.shares -= shares;
                    holding.cost -= basis;
                    if holding.shares <= super::EPS {
                        state.holdings.remove(&record.identity.token_id);
                    }
                }
            }
        }
        let fee = wei_to_shares(delta.collateral_fee);
        state.fees += fee;
        state.live.fees_in_cash += fee;
        state.live.net_cash.insert(id, next["net_cash"].1);
        state.live.net_inventory.insert(id, next["net_inventory"].1);
        commit_delta(&mut state.live, id, &delta, snapshot.generation);
        net_cash_delta = Some(cash_delta);
        net_inventory_delta = Some(inventory_delta);
        applied = true;
    }
    let closed = complete
        && matches!(snapshot.closure, Closure::Final { final_gross_q, .. } if final_gross_q == q);
    let residual = if record.identity.side == Side::Sell || closed {
        U256::ZERO
    } else {
        record
            .r0
            .max(known_risk)
            .max(
                state
                    .live
                    .observed_risk_floor
                    .get(&id)
                    .copied()
                    .unwrap_or(U256::ZERO),
            )
            .saturating_sub(state.live.net_cash.get(&id).copied().unwrap_or(U256::ZERO))
    };
    state.live.reservations.insert(id, residual);
    if closed {
        record.phase = AttemptPhase::Closed;
    }
    state.live.watermarks.insert(
        id,
        CumulativeWatermark {
            generation: snapshot.generation,
            gross_q: q,
            fields: next,
        },
    );
    if !complete {
        incomplete_decision(state, &record, snapshot);
    }
    drop(record);
    super::live_control::refresh_live_risk(state);
    Ok(AppliedExecution {
        delta,
        net_cash_delta,
        net_inventory_delta,
        applied,
        duplicate: false,
        accounting_complete: complete,
    })
}

/// The venue's own order id for an attempt the venue has accepted and the
/// window has not closed yet. Nothing else is in flight: without a proven ACK
/// and the venue's id there is no order the venue could list.
fn in_flight_order(record: &AttemptRecord) -> Option<&str> {
    (record.submission == Submission::Accepted && record.phase != AttemptPhase::Closed)
        .then(|| record.identity.venue_order_id.as_deref())
        .flatten()
}

/// Whether this incomplete reading belongs to an order the venue already
/// accepted and that may still fill.
///
/// A proven ACK (`ACCEPTED` on the attempt, with the venue's own order id) and
/// a snapshot that is `ACCEPTED` with a closure that is not `FINAL` mean the
/// order is in flight: its facts are still arriving, so an incomplete reading
/// is a fact in progress — not an unknown submission and not a run failure.
/// Anything else (no proven ACK, a refused or unknown submission, or a closure
/// that proves the order can't fill) stays a real unproven fact.
fn in_flight(record: &AttemptRecord, snapshot: &ExecutionSnapshot) -> bool {
    in_flight_order(record).is_some()
        && snapshot.submission == Submission::Accepted
        && !matches!(snapshot.closure, Closure::Final { .. })
}

/// The one order this window is holding in flight, when exactly one attempt is.
///
/// More than one in-flight attempt explains nothing: an order list matching one
/// of them still names an order the other did not place, so the caller keeps
/// treating the whole list as unexplained.
fn only_in_flight_order<'a>(
    records: impl IntoIterator<Item = &'a AttemptRecord>,
) -> Option<&'a str> {
    let mut orders = records.into_iter().filter_map(in_flight_order);
    let only = orders.next()?;
    orders.next().is_none().then_some(only)
}

/// How long one order's still-invisible execution facts may be re-read.
pub(super) const UNREAD_FACTS_BUDGET: Duration = Duration::from_secs(120);

/// The pause between two reads of the same order's facts.
pub(super) const UNREAD_FACTS_INTERVAL: Duration = Duration::from_millis(500);

/// What one bounded re-read of the window's unread attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UnreadFacts {
    /// No attempt of this window waits for facts that have not arrived.
    NothingToRead,
    /// The facts arrived, or the read path itself stopped the run.
    Resolved,
    /// The bound expired with the facts still invisible.
    Expired,
}

/// The one attempt whose accepted facts are still invisible.
///
/// Only an order that may already be at the venue qualifies: a proven or
/// possible submission, not closed yet, with a hash to read back and no
/// positive cumulative quantity. More than one such attempt explains nothing,
/// so the caller reads no facts at all rather than guess.
fn unread_attempt(state: &State) -> Option<(u64, String)> {
    let mut found = None;
    for (id, handle) in &state.live.attempts {
        let record = handle.lock();
        let waiting = matches!(record.submission, Submission::Accepted | Submission::Unknown)
            && record.phase != AttemptPhase::Closed;
        let visible = record.phase == AttemptPhase::Closed
            || state
                .live
                .watermarks
                .get(id)
                .is_some_and(|watermark| !watermark.gross_q.is_zero());
        if !waiting || visible {
            continue;
        }
        let Some(hash) = record.identity.signed_hash.clone() else {
            continue;
        };
        if found.is_some() {
            return None;
        }
        found = Some((*id, hash));
    }
    found
}

/// Re-read the one order whose facts have not arrived, bounded in time.
///
/// Only this attempt's existing execution read is repeated: no other order is
/// read, no new request kind is used and nothing is sent. The caller owns the
/// operation lane. Every stop the read path itself raises keeps its first
/// reason, so a conflict is never swallowed by the wait.
pub(super) async fn re_read_unread_facts(
    broker: &Broker,
    budget: Duration,
    interval: Duration,
) -> UnreadFacts {
    let Some((attempt_id, _)) = ({ let state = broker.state.lock(); unread_attempt(&state) })
    else {
        return UnreadFacts::NothingToRead;
    };
    let started = Instant::now();
    loop {
        if broker.controlled_stopped() {
            broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
            return UnreadFacts::Resolved;
        }
        let hash = {
            let state = broker.state.lock();
            if let Some(run) = state.live.controlled.as_ref() {
                if run.control.lock().stop.is_some() {
                    return UnreadFacts::Resolved;
                }
            }
            unread_attempt(&state).and_then(|(id, hash)| (id == attempt_id).then_some(hash))
        };
        let Some(hash) = hash else {
            return UnreadFacts::Resolved;
        };
        if started.elapsed() >= budget {
            return UnreadFacts::Expired;
        }
        let Some(venue) = broker.taker.as_ref() else {
            return UnreadFacts::Resolved;
        };
        let snapshot = match venue.execution_snapshot(&hash).await {
            Ok(snapshot) => snapshot,
            Err(_) => {
                // A transient read failure degrades the window: it keeps
                // observing and adds no risk, and a later read restores it.
                // Only a streak that outlasts the budget stops the run.
                let stopped = {
                    let mut state = broker.state.lock();
                    match state.live.controlled.as_mut() {
                        Some(run) => run.note_failure(super::live_control::FailureKind::Execution,
                            "READ_UNAVAILABLE", chrono::Utc::now()),
                        None => true,
                    }
                };
                if stopped {
                    let _ = super::live_events::stop_on_lane(broker, "READ_UNAVAILABLE");
                }
                return UnreadFacts::Resolved;
            }
        };
        if consume_snapshot(broker, &snapshot).is_err() {
            // The read path stops the run with its own reason, unless the
            // failure arrives after the facts were applied and before any stop
            // was written (a lost clock read). That reading is still a failure:
            // no later observation may continue without a stop.
            latch_unlatched_read_failure(broker);
            return UnreadFacts::Resolved;
        }
        if {
            let state = broker.state.lock();
            unread_attempt(&state).is_none()
        } {
            return UnreadFacts::Resolved;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Stop the run when a failed reading recorded no stop of its own.
///
/// A reading can fail after it already applied its facts — a lost clock read
/// is the existing example — without stopping the run. The window may not keep
/// observing then. The first stop the run already holds keeps its reason, its
/// detail and its takeover flag; otherwise this failure stops the run with the
/// same code an unread fact would use.
fn latch_unlatched_read_failure(broker: &Broker) {
    let unlatched = {
        let state = broker.state.lock();
        state.entry_stop_reason.is_none()
            && state
                .live
                .controlled
                .as_ref()
                .is_some_and(|run| run.control.lock().stop.is_none())
    };
    if unlatched {
        // A lost output is already sticky on the emitter, so a failed flush of
        // this stop needs no second record here.
        let _ = super::live_events::stop_on_lane(
            broker,
            super::live_events::reason::FACT_INCOMPLETE,
        );
    }
}

/// The failure code this run already holds for an incomplete attempt.
///
/// An attempt without a proven acceptance may hold an order at the venue: that
/// is the existing `SUBMISSION_UNKNOWN` code. A proven one is the existing
/// `FACT_INCOMPLETE` code. No new vocabulary is introduced.
fn incomplete_code(record: &AttemptRecord) -> &'static str {
    if record.submission == Submission::Accepted {
        super::live_events::reason::FACT_INCOMPLETE
    } else {
        super::live_events::reason::SUBMISSION_UNKNOWN
    }
}

/// Record what an incomplete reading still proves and act on it.
///
/// The residual cash risk is kept as before. An order in flight only stops the
/// run from adding risk: the window keeps observing, and the attempt stays
/// pending until its facts settle. Any other incomplete reading still stops the
/// run, now with the failure code this attempt already carries.
fn incomplete_reading(
    state: &mut State,
    record: &AttemptRecord,
    snapshot: &ExecutionSnapshot,
    known_risk: U256,
) -> AppliedExecution {
    let id = snapshot.identity.local_attempt_id;
    let residual = if record.identity.side == Side::Sell {
        U256::ZERO
    } else {
        record
            .r0
            .max(known_risk)
            .max(
                state
                    .live
                    .observed_risk_floor
                    .get(&id)
                    .copied()
                    .unwrap_or(U256::ZERO),
            )
            .saturating_sub(state.live.net_cash.get(&id).copied().unwrap_or(U256::ZERO))
    };
    state.live.reservations.insert(id, residual);
    incomplete_decision(state, record, snapshot);
    AppliedExecution::default()
}

/// Decide what an incomplete reading does to the run.
///
/// An order in flight is not a run failure: `consume_snapshot` already stopped
/// the window from adding risk through `state.halted`, the attempt stays
/// pending and its accounting stays incomplete until the facts settle, so the
/// window keeps observing. Any other incomplete reading still stops the run,
/// with the code it already carries.
fn incomplete_decision(state: &mut State, record: &AttemptRecord, snapshot: &ExecutionSnapshot) {
    if in_flight(record, snapshot) {
        return;
    }
    if let Some(run) = state.live.controlled.as_mut() {
        run.stop_with_detail(
            super::live_events::reason::FACT_INCOMPLETE,
            incomplete_code(record),
            true,
            snapshot.observed_at,
        );
    }
}

/// Apply and project one venue reading in the same State critical section.
/// The operation lane must be held by the caller; output is flushed after State.
pub(super) fn consume_snapshot(
    broker: &Broker,
    snapshot: &ExecutionSnapshot,
) -> anyhow::Result<AppliedExecution> {
    let metadata = snapshot.identity.signed_hash.as_deref()
        .and_then(|hash| broker.taker.as_ref()?.controlled_order_metadata(hash));
    let (applied, events) = {
        let mut state = broker.state.lock();
        let id = snapshot.identity.local_attempt_id;
        let previous = state.live.reservations.get(&id).copied().unwrap_or(U256::ZERO);
        let mut result = apply_snapshot_detailed(&mut state, snapshot);
        if state.live.controlled.is_some()
            && result.as_ref().is_ok_and(|applied| !applied.accounting_complete)
        { state.halted = true; }
        if let Err(error) = &result {
            state.halted = true;
            if let Some(run) = state.live.controlled.as_mut() {
                run.stop_with_detail(
                    if snapshot.gross_q().is_none() { "FACT_INCOMPLETE" } else { "FACT_CONFLICT" },
                    &existing_code(error),
                    true,
                    snapshot.observed_at,
                );
            }
            if let Some(handle) = state.live.attempts.get(&id) {
                handle.lock().last_reason = Some(error.clone());
            }
            if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
                gate.record_reconciliation(identity, error);
            }
        }
        // A rejected reading still discloses its original facts, never a
        // fabricated applied delta. Identity mismatches are refused by projection.
        let unapplied = AppliedExecution::default();
        let projected = super::live_events::prepare_execution(&state, snapshot,
            result.as_ref().unwrap_or(&unapplied), previous, metadata.as_ref());
        let mut events = match projected {
            Ok(events) => events,
            Err(error) => {
                if let Some(run) = state.live.controlled.as_mut() {
                    run.stop_with_detail("FACT_CONFLICT", &existing_code(&error),
                        true, snapshot.observed_at);
                }
                result = Err(error);
                Vec::new()
            }
        };
        let events = super::live_events::prepare_lifecycle(&mut state).map(|lifecycle| {
            events.extend(lifecycle);
            events
        });
        (result, events)
    };
    super::live_events::flush_events(events.map_err(anyhow::Error::msg)?)
        .map_err(anyhow::Error::msg)?;
    applied.map_err(anyhow::Error::msg)
}

/// The conservative cash risk one reading shows.
///
/// It is the greater of the net cash the venue reported and the gross cash
/// plus the collateral fee: a fee the venue charges on top of the gross is
/// real risk even when the net is unknown.
pub(crate) fn observed_risk_of(snapshot: &ExecutionSnapshot) -> U256 {
    let gross = snapshot.accounting.gross_cash.value().copied().unwrap_or(U256::ZERO);
    let fee = snapshot
        .accounting
        .collateral_fee
        .value()
        .copied()
        .unwrap_or(U256::ZERO);
    let net = snapshot.accounting.net_cash.value().copied().unwrap_or(U256::ZERO);
    gross.saturating_add(fee).max(net)
}

/// Release only on a returned error proving the request never left. Dropped
/// futures never reach here and keep their reservation, even without a hash.
pub(super) fn release_not_submitted(
    broker: &Broker,
    handle: &crate::service::venue::AttemptHandle,
    key: &str,
) {
    let mut state = broker.state.lock();
    let mut record = handle.lock();
    if record.phase == AttemptPhase::Prepared && record.submission == Submission::NotSubmitted {
        record.phase = AttemptPhase::Closed;
        state
            .live
            .reservations
            .remove(&record.identity.local_attempt_id);
        // A controlled candidate is single-use even when definitely unsent;
        // retaining its tombstone/hash is evidence, not outstanding cash risk.
        if state.live.controlled.is_none() { state.live.tombstones.remove(key); }
        if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
            gate.release(identity, record.identity.local_attempt_id);
        }
    } else {
        tombstone(&mut state.live, key, record.identity.local_attempt_id);
    }
}

/// Seed only from a complete account and reconciled history. Never overwrite
/// an in-process order's unknown risk with an operator's budget declaration.
pub(super) async fn seed_predict(broker: &Broker) -> anyhow::Result<()> {
    let lane = broker.live_operation_lane();
    let _operation = match lane.as_ref() { Some(lane) => Some(lane.lock().await), None => None };
    let venue = broker
        .taker
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("MISSING_VENUE"))?;
    let gate = broker
        .gate
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("MISSING_ACCOUNT_GATE"))?;
    let identity = broker
        .identity
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ACCOUNT_MISMATCH"))?;
    let result = async {
        if broker.state.lock().live.baseline.is_some() {
            if broker
                .state
                .lock()
                .live
                .attempts
                .values()
                .any(|handle| handle.lock().phase != AttemptPhase::Closed)
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_RISK_REQUIRES_SYNC");
            }
            return check_runtime_account(broker, false).await;
        }
        let snapshot = venue.account_snapshot().await?;
        let checked = super::live_control::account_receipt(&broker.state.lock())?;
        let controlled_budget = broker
            .state
            .lock()
            .live
            .controlled
            .as_ref()
            .map(|run| run.options.params.budget);
        let startup_budget = controlled_budget.unwrap_or_else(super::live_accounting::budget_c0_atoms);
        crate::service::predict_fun::account::check_snapshot_complete(&snapshot)?;
        // A claimed payout the opening read explained is exact cash above the
        // pre-claim C0; the window may account for it, never for any other gap.
        let claim_cash = if snapshot.positions_explained {
            super::live_accounting::explained_claim_cash(&snapshot)
                .ok_or_else(|| anyhow::anyhow!("CASH_CONFLICT"))?
        } else {
            U256::ZERO
        };
        // A controlled window's C0 is exactly the cash it accounts for. A
        // different observed balance is refused before any order, so the window
        // never trades first and then discovers an unclosable identity.
        if controlled_budget.is_some_and(|budget| {
            !super::live_accounting::cash_matches_budget_and_claims(
                budget,
                snapshot.collateral_atoms,
                claim_cash,
            )
        }) {
            anyhow::bail!("CASH_BUDGET_MISMATCH");
        }
        if checked.is_some() && (!snapshot.positions.is_empty()
            || !super::live_accounting::startup_funded(startup_budget, snapshot.collateral_atoms))
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:CONTROLLED_STARTUP_BUDGET_OR_POSITIONS");
        }
        if &snapshot.identity != identity {
            anyhow::bail!("ACCOUNT_MISMATCH");
        }
        if !snapshot.open_orders.is_empty() {
            anyhow::bail!("RECONCILIATION_REQUIRED:OPEN_ORDERS");
        }
        if !gate.budget_confirmed(identity) {
            anyhow::bail!("BUDGET_UNCONFIRMED");
        }
        let positions = broker
            .limit
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MISSING_VENUE"))?
            .positions()
            .await?;
        if positions.len() != snapshot.positions.len()
            || positions.iter().any(|position| {
                !snapshot.positions.iter().any(|(token, amount)| {
                    token == &position.token_id
                        && (wei_to_shares(*amount) - position.shares).abs() <= super::EPS
                })
            })
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:POSITION_READ_CHANGED");
        }
        let mut state = broker.state.lock();
        if !state.live.attempts.is_empty()
            || !state.holdings.is_empty()
            || !state.resting.is_empty()
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_RISK_REQUIRES_SYNC");
        }
        let mut inherited = HashMap::new();
        for position in positions {
            *inherited.entry(position.market.id).or_default() += position.shares;
        }
        if !state.inherited.is_empty() && state.inherited != inherited {
            anyhow::bail!("RECONCILIATION_REQUIRED:INHERITED_CHANGED");
        }
        state.inherited = inherited;
        if !snapshot.positions_explained {
            anyhow::bail!("RECONCILIATION_REQUIRED:UNEXPLAINED_POSITIONS");
        }
        super::live_control::record_account_read(&mut state, &snapshot, checked)?;
        state.live.baseline = Some(snapshot.clone());
        if let Some(run) = state.live.controlled.as_mut() {
            run.opening_claim_cash = claim_cash;
        }
        let event = checked.map(|checked| super::live_events::prepare_reconciliation(
            &state, &snapshot, checked.utc, "STARTUP", true, "NONE")).transpose()
            .map_err(anyhow::Error::msg)?.flatten();
        drop(state);
        if let Some(event) = event { event.flush().map_err(anyhow::Error::msg)?; }
        Ok(())
    }
    .await;
    match &result {
        Ok(()) => gate.record_read_success(identity, "startup"),
        Err(_) => gate.record_read_failure(identity, "startup", "STARTUP_UNPROVEN"),
    }
    if let Err(error) = &result {
        // The stop this failure takes names the code it actually held, before
        // it is recorded: the first stop keeps its reason and its code, so this
        // cannot rewrite a finding an earlier stop already carries.
        if let Some(run) = broker.state.lock().live.controlled.as_mut() {
            run.stop_with_detail("RECONCILIATION_REQUIRED", &account_failure_code(error), true,
                chrono::Utc::now());
        }
        super::live_events::stop_on_lane(broker, "RECONCILIATION_REQUIRED")
            .map_err(anyhow::Error::msg)?;
    }
    result.map_err(public_account_error)
}

/// Compare a fresh authoritative account read with the original observation
/// plus this consumer's already committed net deltas, using only atom arithmetic.
pub(crate) fn check_account_delta(
    initial: &crate::service::venue::AccountSnapshot,
    current: &crate::service::venue::AccountSnapshot,
    changes: impl IntoIterator<Item = (String, Side, U256, U256)>,
) -> anyhow::Result<()> {
    check_account_delta_with_own_order(initial, current, None, changes)
}

/// The same comparison, naming the one venue order id this window accepted and
/// has not closed yet.
///
/// Only that order may appear in the venue's open-order list: it is the window's
/// own in-flight fact, not foreign risk. Any other id, a mixed list, or a list
/// without such an attempt stays `RECONCILIATION_REQUIRED:OPEN_ORDERS`.
pub(crate) fn check_account_delta_with_own_order(
    initial: &crate::service::venue::AccountSnapshot,
    current: &crate::service::venue::AccountSnapshot,
    own_order: Option<&str>,
    changes: impl IntoIterator<Item = (String, Side, U256, U256)>,
) -> anyhow::Result<()> {
    check_account_delta_with_claims(initial, current, own_order, changes, std::iter::empty())
}

/// The same comparison plus claimed payouts, as `(token, shares, cash)`.
///
/// A claim is the venue's own confirmed official result paying for inventory
/// this ledger already holds: its shares are gone from the account and its
/// exact cash atoms arrived. A claim for a token the ledger doesn't hold at
/// this point is not applied — its cash is part of the baseline, and any other
/// difference still fails the cash comparison.
pub(crate) fn check_account_delta_with_claims(
    initial: &crate::service::venue::AccountSnapshot,
    current: &crate::service::venue::AccountSnapshot,
    own_order: Option<&str>,
    changes: impl IntoIterator<Item = (String, Side, U256, U256)>,
    claims: impl IntoIterator<Item = (String, U256, U256)>,
) -> anyhow::Result<()> {
    claims::check_account_delta_with_claims(initial, current, own_order, changes, claims)
}

/// Keep typed venue reasons ahead of display contexts. The request/account
/// boundary projects transport, HTTP and parse failures from types, not text.
/// Legacy internal short codes are still accepted through context wrappers;
/// arbitrary messages never become more specific than UNKNOWN.
fn account_failure_code(error: &anyhow::Error) -> String {
    if let Some(venue) = error.downcast_ref::<crate::service::venue::VenueError>() {
        return existing_code(&venue.reason);
    }
    // Textual context is not an error code. Only the original legacy cause
    // participates in compatibility, after typed request errors were projected.
    existing_code(&error.root_cause().to_string())
}

/// Only an existing uppercase token may leave this module; anything else is
/// `UNKNOWN`, never display text.
///
/// A venue/route failure may name its class and its cause as one code
/// (`FACT_INCOMPLETE:MISSING_TOKEN`). The detail carries the single bounded
/// cause; exactly one known class prefix is dropped, so no new vocabulary is
/// introduced and no display text can pass.
fn existing_code(token: &str) -> String {
    let token = token.trim_start_matches("RECONCILIATION_REQUIRED:");
    let token = match token.split_once(':') {
        Some((class, cause)) if super::live_events::is_known_reason(class) => cause,
        _ => token,
    };
    (!token.is_empty() && token.len() <= 64
        && token.bytes().all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'))
        .then(|| token.to_string())
        .unwrap_or_else(|| "UNKNOWN".into())
}

/// Detach raw causes before startup, drive or shutdown can log an account
/// failure. Keep the typed code and safe tokens, never arbitrary Debug context.
fn public_account_error(error: anyhow::Error) -> anyhow::Error {
    let reason = account_failure_code(&error);
    if let Some(venue) = error.downcast_ref::<crate::service::venue::VenueError>() {
        let step = if !venue.step.is_empty() && venue.step.len() <= 64
            && venue.step.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        { &venue.step } else { "ACCOUNT" };
        return crate::service::venue::VenueError::new(venue.code, step, &reason).into();
    }
    anyhow::Error::msg(reason)
}

/// The claimed payouts this ledger can apply, and the ones it records now.
///
/// A fresh claim is attributable only to exactly one local attempt whose closed
/// inventory it pays in full. A claim whose token the ledger doesn't own is not
/// this window's delta: its cash is already part of the baseline.
fn claimed_payouts(
    state: &crate::bot::runtime::State,
    snapshot: &crate::service::venue::AccountSnapshot,
) -> anyhow::Result<(Vec<(String, U256, U256)>, Vec<(u64, U256, U256)>)> {
    let mut claims = Vec::new();
    let mut by_token: HashMap<String, (U256, U256)> = HashMap::new();
    for (id, (inventory, cash)) in &state.live.claims {
        let token = state
            .live
            .attempts
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:CASH_CONFLICT"))?
            .lock()
            .identity
            .token_id
            .clone();
        by_token.insert(token.clone(), (*inventory, *cash));
        claims.push((token, *inventory, *cash));
    }
    let mut fresh = Vec::new();
    for win in &snapshot.claimed_win {
        let owners: Vec<u64> = state
            .live
            .attempts
            .iter()
            .filter(|(_, handle)| handle.lock().identity.token_id == win.token)
            .map(|(id, _)| *id)
            .collect();
        let [id] = owners.as_slice() else { continue };
        let id = *id;
        let record = state.live.attempts[&id].lock().clone();
        let Some(inventory) = state.live.net_inventory.get(&id).copied() else {
            anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_ACCOUNTING_UNPROVEN");
        };
        let reserved = state.live.reservations.get(&id).copied().unwrap_or(U256::ZERO);
        // The claim must pay this holding in full, and nothing may still be
        // reserved against the attempt.
        if inventory != win.inventory_atoms
            || record.phase != AttemptPhase::Closed
            || !reserved.is_zero()
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_ACCOUNTING_UNPROVEN");
        }
        match by_token.get(&win.token) {
            Some((held, cash)) => {
                if *held != win.inventory_atoms || *cash != win.claim_atoms {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CASH_CONFLICT");
                }
            }
            None => {
                by_token.insert(win.token.clone(), (win.inventory_atoms, win.claim_atoms));
                claims.push((win.token.clone(), win.inventory_atoms, win.claim_atoms));
                fresh.push((id, win.inventory_atoms, win.claim_atoms));
            }
        }
    }
    Ok((claims, fresh))
}

pub(super) async fn check_runtime_account(
    broker: &Broker,
    include_pending: bool,
) -> anyhow::Result<()> {
    let (version, closed, controlled) = {
        let mut state = broker.state.lock();
        if state.live.baseline.is_none()
            || (!include_pending
                && state
                    .live
                    .attempts
                    .values()
                    .any(|handle| handle.lock().phase != AttemptPhase::Closed))
        {
            return Ok(());
        }
        if let Some(run) = state.live.controlled.as_mut() {
            run.tick_started_elapsed_ms = Some(run.phase1.clock.now()?.elapsed_ms);
        }
        ((state.live.next_attempt_id, state.live.generation.clone()),
            claims::closed_inventory(&state), state.live.controlled.is_some())
    };
    let result = async {
        let venue = broker.taker.as_ref().ok_or_else(|| anyhow::anyhow!("MISSING_VENUE"))?;
        let observation = if controlled {
            venue.runtime_account_observation(&closed).await?
        } else {
            crate::service::venue::RuntimeAccountObservation {
                snapshot: venue.account_snapshot().await?, bound_claims: Vec::new(),
            }
        };
        let mut snapshot = observation.snapshot;
        let mut state = broker.state.lock();
        let checked = super::live_control::account_receipt(&state)?;
        let mut observed = snapshot.clone();
        // Another task may have reserved while the read was in flight. Never
        // compare a pre-submit observation against post-submit bookkeeping.
        if version != (state.live.next_attempt_id, state.live.generation.clone()) {
            anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_READ_RACE");
        }
        // The window's own accepted, still-open order may legitimately be
        // listed by the venue. Exactly one attempt can explain the list; every
        // other id stays unexplained and stops the run.
        let records: Vec<AttemptRecord> =
            state.live.attempts.values().map(|handle| handle.lock().clone()).collect();
        let own_order = only_in_flight_order(records.iter()).map(str::to_string);
        let mut changes = Vec::new();
        for (id, handle) in &state.live.attempts {
            let record = handle.lock();
            if let (Some(cash), Some(inventory)) = (
                state.live.net_cash.get(id),
                state.live.net_inventory.get(id),
            ) {
                changes.push((
                    record.identity.token_id.clone(),
                    record.identity.side,
                    *cash,
                    *inventory,
                ));
            } else if record.phase != AttemptPhase::Closed {
                anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_ACCOUNTING_UNPROVEN");
            }
        }
        let (claims, fresh) = if controlled {
            claims::runtime_payouts(&state, &observation.bound_claims, &snapshot.settled_zero)?
        } else { claimed_payouts(&state, &snapshot)? };
        let mut booked_claims: Vec<(u64, U256, U256)> = Vec::new();
        // This flag is an input to the full equality check below, not a
        // bypass: only the Broker's atom equations may explain bound positions.
        snapshot.positions_explained = true;
        let reconciled = check_account_delta_with_claims(
            state.live.baseline.as_ref().unwrap(), &snapshot, own_order.as_deref(), changes, claims)
            .and_then(|()| {
                super::live_control::record_account_read(&mut state, &snapshot, checked)?;
                // The account read proves the exact claimed atoms arrived; this
                // is the one place a claimed payout becomes ledger cash, and the
                // one place a wait only the missing claim caused can end.
                for (id, inventory, cash) in fresh {
                    let (booked, _) = if state.live.pending_claims.contains_key(&id) {
                        super::live_control::promote_pending_claim(&mut state, id, inventory, cash)
                    } else {
                        super::live_control::credit_claim(&mut state, id, inventory, cash)
                    };
                    if let Some((inventory, cash)) = booked {
                        booked_claims.push((id, inventory, cash));
                    }
                }
                // Adapter history excludes this run's bound orders. Only the
                // exact baseline + committed atom deltas can explain their
                // positions here; do not project them as foreign after proof.
                snapshot.positions_explained = true;
                observed.positions_explained = true;
                Ok(())
            });
        if let Err(error) = &reconciled {
            if let Some(run) = state.live.controlled.as_mut() {
                run.account_read = None;
                run.stop_with_detail("RECONCILIATION_REQUIRED", &account_failure_code(error),
                    true, checked.map_or_else(chrono::Utc::now, |time| time.utc));
            }
        }
        let mut events = Vec::new();
        if let Some(checked) = checked {
            if let Some(event) = super::live_events::prepare_reconciliation(&state, &observed,
                checked.utc, "RUNTIME", reconciled.is_ok(),
                if reconciled.is_ok() { "NONE" } else { "RECONCILIATION_REQUIRED" })
                .map_err(anyhow::Error::msg)?
            { events.push(event); }
            // A claim that became ledger cash is a wire fact: the reader has to
            // be able to see that the wait it latched is over.
            for (id, inventory, cash) in &booked_claims {
                if let Some(event) = super::live_events::prepare_claim_paid(
                    &state, *id, *inventory, *cash, checked.utc)
                    .map_err(anyhow::Error::msg)?
                { events.push(event); }
            }
            events.extend(super::live_events::prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?);
        }
        drop(state);
        super::live_events::flush_events(events).map_err(anyhow::Error::msg)?;
        reconciled
    }
    .await;
    // A read that failed before its own stop still names the code it held: the
    // first stop of this run carries it.
    if let Err(error) = &result {
        let transient = account_failure_reason(error) == "READ_UNAVAILABLE";
        let stopped = {
            let mut state = broker.state.lock();
            match state.live.controlled.as_mut() {
                Some(run) => {
                    run.account_read = None;
                    if transient {
                        // A transient read failure degrades the window: it
                        // keeps observing and adds no risk, and a later read
                        // restores it. Only a streak that outlasts the budget
                        // stops the run.
                        run.note_failure(super::live_control::FailureKind::Read,
                            "READ_UNAVAILABLE", chrono::Utc::now())
                    } else {
                        run.stop_with_detail("RECONCILIATION_REQUIRED",
                            &account_failure_code(error), true, chrono::Utc::now());
                        true
                    }
                }
                None => false,
            }
        };
        if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
            gate.record_read_failure(identity, "runtime_account", "ACCOUNT_LEDGER_UNPROVEN");
        }
        if stopped {
            super::live_events::stop_on_lane(broker, account_failure_reason(error))
                .map_err(anyhow::Error::msg)?
        }
    } else {
        // The account read came back: the window's read streak is over.
        let mut state = broker.state.lock();
        if let Some(run) = state.live.controlled.as_mut() {
            run.clear_failure(super::live_control::FailureKind::Read);
        }
    }
    if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
        if result.is_ok() {
            gate.record_read_success(identity, "runtime_account");
        }
    }
    result.map_err(public_account_error)
}

/// The reason a failed account read stops the window with.
///
/// A read that could not be made is `READ_UNAVAILABLE`; a read that was made
/// and disagrees with the ledger is a reconciliation conflict.
fn account_failure_reason(error: &anyhow::Error) -> &'static str {
    use crate::service::venue::{VenueError, VenueErrorCode};
    match error.downcast_ref::<VenueError>().map(|venue| venue.code) {
        Some(VenueErrorCode::ReadUnavailable) => "READ_UNAVAILABLE",
        _ => "RECONCILIATION_REQUIRED",
    }
}

pub(super) async fn sync_predict(broker: &Broker) -> anyhow::Result<()> {
    let lane = broker.live_operation_lane();
    let _operation = match lane.as_ref() { Some(lane) => Some(lane.lock().await), None => None };
    if broker.state.lock().live.controlled.as_ref().is_some_and(|run| run.emitter.is_finished()) { return Ok(()); }
    let venue = broker
        .taker
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("MISSING_VENUE"))?;
    let handles: Vec<_> = broker
        .state
        .lock()
        .live
        .attempts
        .values()
        .cloned()
        .collect();
    for handle in handles {
        let record = handle.lock().clone();
        if broker.state.lock().live.controlled.is_some() && record.phase == AttemptPhase::Closed
            && record.submission == Submission::NotSubmitted
        { continue; }
        let Some(hash) = record.identity.signed_hash else {
            continue;
        };
        let snapshot = match venue.execution_snapshot(&hash).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
                    gate.record_read_failure(identity, "execution", "READ_FAILED");
                }
                // A transient execution read failure degrades the window: it
                // keeps observing and adds no risk, and a later read restores
                // it. Only a streak that outlasts the budget stops the run.
                let stopped = {
                    let mut state = broker.state.lock();
                    match state.live.controlled.as_mut() {
                        Some(run) => run.note_failure(super::live_control::FailureKind::Execution,
                            "READ_UNAVAILABLE", chrono::Utc::now()),
                        None => true,
                    }
                };
                if stopped {
                    super::live_events::stop_on_lane(broker, "READ_UNAVAILABLE")
                        .map_err(anyhow::Error::msg)?
                }
                return Err(error);
            }
        };
        {
            // The execution read came back: its failure streak is over.
            let mut state = broker.state.lock();
            if let Some(run) = state.live.controlled.as_mut() {
                run.clear_failure(super::live_control::FailureKind::Execution);
            }
        }
        let controlled = broker.state.lock().live.controlled.is_some();
        let applied = if controlled {
            consume_snapshot(broker, &snapshot).map(|_| ()).map_err(|error| error.to_string())
        } else {
            apply_snapshot(&mut broker.state.lock(), &snapshot).map(|_| ())
        };
        if let Err(reason) = applied {
            handle.lock().last_reason = Some(reason.clone());
            if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
                gate.record_reconciliation(identity, &reason);
            }
            return Err(anyhow::anyhow!("RECONCILIATION_REQUIRED:{reason}"));
        }
        let mut state = broker.state.lock();
        let closed = handle.lock().phase == AttemptPhase::Closed;
        for resting in state
            .resting
            .values_mut()
            .filter(|resting| resting.hash == hash)
        {
            resting.filled = snapshot
                .gross_q()
                .map(wei_to_shares)
                .unwrap_or(resting.filled);
        }
        if closed {
            state.resting.retain(|_, resting| resting.hash != hash);
        }
        if let (Some(gate), Some(identity)) = (&broker.gate, &broker.identity) {
            if snapshot.pages_complete && snapshot.accounting_complete() {
                gate.record_read_success(identity, "execution");
            }
            if closed {
                gate.resolve(
                    identity,
                    record.identity.local_attempt_id,
                    AttemptPhase::Closed,
                );
            }
        }
    }
    check_runtime_account(broker, false).await
}

/// The key of a taker intent: the market, the token and the side.
pub fn taker_key(market_id: u64, token: &str, side: Side) -> String {
    format!("taker:{market_id}:{token}:{side:?}")
}

/// The key of a copy intent: the source trade, when there is one.
pub fn copy_key(token: &str, source_trade_hash: Option<&str>) -> String {
    match source_trade_hash {
        Some(hash) => format!("copy:{hash}:{token}"),
        None => format!("copy:{token}"),
    }
}

/// The key of a monitor intent: the token and when the position opened.
pub fn monitor_key(token: &str, opened_at: &str) -> String {
    format!("monitor:{token}:{opened_at}")
}

/// Allocate the next local attempt id.
pub fn next_attempt_id(state: &mut LiveState) -> u64 {
    state.next_attempt_id += 1;
    state.next_attempt_id
}

/// Register an attempt, and reserve its worst-case risk.
///
/// The reservation is the request's gross cash plus the collateral fee the
/// signed order could still cost. It is taken before anything is sent, so a
/// failure after the request went out can't leave the budget unreserved.
pub fn reserve_attempt(
    state: &mut LiveState,
    record: AttemptRecord,
) -> Arc<parking_lot::Mutex<AttemptRecord>> {
    let id = record.identity.local_attempt_id;
    state.reservations.insert(
        id,
        if record.identity.side == Side::Buy {
            record.r0
        } else {
            U256::ZERO
        },
    );
    let handle = Arc::new(parking_lot::Mutex::new(record));
    state.attempts.insert(id, handle.clone());
    handle
}

/// The risk an attempt still holds: the worse of what was reserved and what
/// the fills actually show. It is never the sum of the two.
pub fn protected_risk(record: &AttemptRecord, applied_q: U256, applied_cash: U256) -> U256 {
    let confirmed = applied_cash.max(U256::ZERO);
    record.r0.max(confirmed)
}

/// The delta one cumulative reading adds to what has been applied.
///
/// A reading older than the last accepted one is dropped. A reading of the
/// same generation adds nothing. A cumulative amount that went down, or a
/// known amount that changed value, is a conflict: the caller must reconcile,
/// not silently overwrite the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Nothing new.
    NoChange,
    /// New facts to apply.
    Delta(Delta),
    /// The facts disagree with the ledger.
    Conflict(String),
}

/// The new facts one reading adds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Delta {
    pub gross_q: U256,
    pub gross_cash: U256,
    pub collateral_fee: U256,
    pub share_fee: U256,
}

/// Work out what a cumulative reading adds to the ledger.
///
/// The applied quantities are cumulative totals, not differences: the venue's
/// cumulative snapshot is the source of truth, and the delta is the difference
/// between it and what was already applied.
pub fn apply_cumulative(
    applied_q: U256,
    applied_cash: U256,
    applied_collateral_fee: Option<U256>,
    applied_share_fee: Option<U256>,
    last_generation: Option<u64>,
    snapshot: &ExecutionSnapshot,
) -> ApplyOutcome {
    if let Some(last) = last_generation {
        if snapshot.generation < last {
            return ApplyOutcome::NoChange;
        }
        if snapshot.generation == last {
            return ApplyOutcome::NoChange;
        }
    }

    let Some(q) = snapshot.gross_q() else {
        return ApplyOutcome::NoChange;
    };
    if q < applied_q {
        return ApplyOutcome::Conflict("CUMULATIVE_DECREASE".into());
    }

    let cash = match snapshot.accounting.gross_cash.value() {
        Some(v) => *v,
        None => return ApplyOutcome::NoChange,
    };
    if cash < applied_cash || (q == applied_q && cash != applied_cash) {
        return ApplyOutcome::Conflict("CUMULATIVE_CASH_REVISED".into());
    }

    let fee_delta = |field: &Field<U256>, old: Option<U256>| -> Result<U256, String> {
        let Some(value) = field.value().copied() else {
            return Ok(U256::ZERO);
        };
        if let Some(previous) = old {
            if value < previous || (q == applied_q && value != previous) {
                return Err("FEE_REVISED".into());
            }
        }
        Ok(value - old.unwrap_or(U256::ZERO))
    };
    let collateral_fee =
        match fee_delta(&snapshot.accounting.collateral_fee, applied_collateral_fee) {
            Ok(delta) => delta,
            Err(reason) => return ApplyOutcome::Conflict(reason),
        };
    let share_fee = match fee_delta(&snapshot.accounting.share_fee, applied_share_fee) {
        Ok(delta) => delta,
        Err(reason) => return ApplyOutcome::Conflict(reason),
    };

    ApplyOutcome::Delta(Delta {
        gross_q: q - applied_q,
        gross_cash: cash - applied_cash,
        collateral_fee,
        share_fee,
    })
}

/// Apply a delta to the ledger, in one step.
pub fn commit_delta(state: &mut LiveState, attempt_id: u64, delta: &Delta, generation: u64) {
    *state.applied_q.entry(attempt_id).or_default() += delta.gross_q;
    *state.applied_cash.entry(attempt_id).or_default() += delta.gross_cash;
    *state.applied_collateral_fee.entry(attempt_id).or_default() += delta.collateral_fee;
    *state.applied_share_fee.entry(attempt_id).or_default() += delta.share_fee;
    state.generation.insert(attempt_id, generation);
}

/// Whether the attempt may still fill, and whether its facts are complete.
pub fn attempt_open(record: &AttemptRecord) -> bool {
    !matches!(record.phase, AttemptPhase::Closed)
}

/// Whether the account has an intent it already made that it must not repeat.
pub fn tombstoned(state: &LiveState, key: &str) -> Option<u64> {
    state.tombstones.get(key).copied()
}

/// Mark an intent as made, so it isn't made again.
pub fn tombstone(state: &mut LiveState, key: &str, attempt_id: u64) {
    state.tombstones.insert(key.to_string(), attempt_id);
}

/// The shares a snapshot confirms, in the broker's share units.
///
/// A zero fill is zero only when the venue proved it: an accepted order with
/// no fills, an empty match list or a failed query are all unknown.
pub fn confirmed_shares(snapshot: &ExecutionSnapshot) -> Option<f64> {
    match snapshot.quantity {
        Quantity::Known { gross_q_atoms, .. } => Some(wei_to_shares(gross_q_atoms)),
        Quantity::Unknown => None,
    }
}

/// The price a snapshot confirms, per share.
pub fn confirmed_price(snapshot: &ExecutionSnapshot) -> Option<f64> {
    match &snapshot.accounting.vwap {
        Field::Known(v) => Some(wei_to_shares(*v)),
        Field::Unknown(_) => None,
    }
}

/// Convert atoms to shares.
pub fn wei_to_shares(atoms: U256) -> f64 {
    atoms.to_string().parse::<f64>().unwrap_or(0.0) / 1e18
}

/// The outcome of a live take, given what the venue reports.
///
/// A complete fill returns the confirmed shares and their actual price. An
/// unknown or partial result keeps the position and the budget, and says so.
pub fn take_result(snapshot: &ExecutionSnapshot, fallback_price: f64) -> TakeResult {
    if !snapshot.pages_complete || !snapshot.accounting_complete() {
        return TakeResult::Blocked(Blocked::Halted);
    }
    if snapshot.gross_q() == Some(U256::ZERO)
        && !matches!(snapshot.closure, Closure::Final { final_gross_q, .. } if final_gross_q.is_zero())
    {
        return TakeResult::Blocked(Blocked::Halted);
    }
    match snapshot.quantity {
        Quantity::Known {
            gross_q_atoms,
            class,
        } => {
            let shares = wei_to_shares(gross_q_atoms);
            match class {
                QuantityClass::Zero => TakeResult::Filled {
                    shares: 0.0,
                    // The venue's limit, as a placeholder. It is not a VWAP and
                    // it doesn't enter the performance numbers.
                    price: fallback_price,
                },
                _ => match confirmed_price(snapshot) {
                    Some(price) => TakeResult::Filled { shares, price },
                    None => TakeResult::Blocked(Blocked::Halted),
                },
            }
        }
        Quantity::Unknown => TakeResult::Blocked(Blocked::Halted),
    }
}

/// Whether a submission may be retried. It may not: an order that may have
/// reached the venue is only ever looked up again.
pub fn may_retry(submission: Submission) -> bool {
    matches!(submission, Submission::NotSubmitted)
}

/// Whether the order is provably closed.
pub fn is_closed(closure: &Closure) -> bool {
    matches!(closure, Closure::Final { .. })
}

/// The broker's live state, if it has one.
pub fn live_state(state: &State) -> &LiveState {
    &state.live
}

/// The broker's live state, mutably.
pub fn live_state_mut(state: &mut State) -> &mut LiveState {
    &mut state.live
}

/// The broker this module extends.
pub type LiveBroker = Broker;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VenueId;
    use crate::service::venue::{AccountIdentity, AccountMode, Accounting, OrderIdentity};

    fn identity(attempt_id: u64) -> OrderIdentity {
        OrderIdentity {
            local_attempt_id: attempt_id,
            signed_hash: Some("0xhash".into()),
            venue_order_id: None,
            market_id: 1,
            token_id: "77".into(),
            side: Side::Buy,
            account: AccountIdentity {
                chain_id: 56,
                trading_address: "0xabc".into(),
                mode: AccountMode::Eoa,
                signer_address: "0xabc".into(),
                venue: VenueId::PredictFun,
            },
        }
    }

    fn accounting(cash: U256, collateral_fee: U256, share_fee: U256) -> Accounting {
        Accounting {
            gross_cash: Field::Known(cash),
            collateral_fee: Field::Known(collateral_fee),
            share_fee: Field::Known(share_fee),
            net_cash: Field::Known(cash + collateral_fee),
            net_inventory: Field::Known(U256::ZERO),
            vwap: Field::Known(U256::ZERO),
        }
    }

    fn snapshot(generation: u64, q: u128, cash: u128, collateral_fee: u128) -> ExecutionSnapshot {
        ExecutionSnapshot {
            identity: identity(1),
            submission: Submission::Accepted,
            quantity: Quantity::Known {
                gross_q_atoms: U256::from(q),
                class: QuantityClass::Partial,
            },
            accounting: accounting(U256::from(cash), U256::from(collateral_fee), U256::ZERO),
            accounting_gross_q: Some(U256::from(q)),
            closure: Closure::Unproven,
            generation,
            pages_complete: true,
            gross_q_before: U256::ZERO,
            gross_q_after: U256::from(q),
            source: "test".into(),
            observed_at: chrono::Utc::now(),
            tx_hash: None,
        }
    }

    #[test]
    fn a005_cumulative_field_completion_and_conflict() {
        // The first reading adds everything.
        let first = snapshot(1, 10, 100, 5);
        let outcome = apply_cumulative(U256::ZERO, U256::ZERO, None, None, None, &first);
        let ApplyOutcome::Delta(delta) = outcome else {
            panic!("expected a delta, got {outcome:?}");
        };
        assert_eq!(delta.gross_q, U256::from(10u128));
        assert_eq!(delta.gross_cash, U256::from(100u128));
        assert_eq!(delta.collateral_fee, U256::from(5u128));

        // The same generation adds nothing.
        assert_eq!(
            apply_cumulative(
                U256::from(10u128),
                U256::from(100u128),
                Some(U256::from(5u128)),
                Some(U256::ZERO),
                Some(1),
                &first
            ),
            ApplyOutcome::NoChange
        );

        // An older generation doesn't roll the ledger back.
        let older = snapshot(0, 5, 50, 0);
        assert_eq!(
            apply_cumulative(
                U256::from(10u128),
                U256::from(100u128),
                Some(U256::from(5u128)),
                Some(U256::ZERO),
                Some(1),
                &older
            ),
            ApplyOutcome::NoChange
        );

        // A cumulative decrease is a conflict.
        let smaller = snapshot(2, 5, 50, 0);
        assert!(matches!(
            apply_cumulative(
                U256::from(10u128),
                U256::from(100u128),
                Some(U256::from(5u128)),
                Some(U256::ZERO),
                Some(1),
                &smaller
            ),
            ApplyOutcome::Conflict(_)
        ));

        // A fee that was known and changed value is a conflict too.
        let revised = snapshot(2, 10, 100, 9);
        assert!(matches!(
            apply_cumulative(
                U256::from(10u128),
                U256::from(100u128),
                Some(U256::from(5u128)),
                Some(U256::ZERO),
                Some(1),
                &revised
            ),
            ApplyOutcome::Conflict(_)
        ));
    }

    #[test]
    fn a_fee_that_arrives_late_only_adds_the_fee() {
        // The quantity is known, the fee isn't yet.
        let mut first = snapshot(1, 10, 100, 0);
        first.accounting.collateral_fee = Field::Unknown("MISSING_FEE".into());
        let outcome = apply_cumulative(U256::ZERO, U256::ZERO, None, None, None, &first);
        let ApplyOutcome::Delta(delta) = outcome else {
            panic!("expected a delta, got {outcome:?}");
        };
        assert_eq!(delta.gross_q, U256::from(10u128));
        assert_eq!(delta.collateral_fee, U256::ZERO);

        // The fee arrives later: the quantity isn't counted twice.
        let second = snapshot(2, 10, 100, 5);
        let outcome = apply_cumulative(
            U256::from(10u128),
            U256::from(100u128),
            None,
            Some(U256::ZERO),
            Some(1),
            &second,
        );
        let ApplyOutcome::Delta(delta) = outcome else {
            panic!("expected a delta, got {outcome:?}");
        };
        assert_eq!(delta.gross_q, U256::ZERO);
        assert_eq!(delta.collateral_fee, U256::from(5u128));
    }

    #[test]
    fn the_protected_risk_is_the_worse_of_the_two() {
        let record = AttemptRecord {
            identity: identity(1),
            request_gross_atoms: U256::from(10u128),
            r0: U256::from(100u128),
            reserved_sell: U256::ZERO,
            phase: AttemptPhase::MayHaveSubmitted,
            submission: Submission::Unknown,
            last_reason: None,
        };
        // The confirmed risk is below the reservation: the reservation holds.
        assert_eq!(
            protected_risk(&record, U256::from(10u128), U256::from(40u128)),
            U256::from(100u128)
        );
        // The confirmed risk is above the reservation: the truth holds.
        assert_eq!(
            protected_risk(&record, U256::from(10u128), U256::from(400u128)),
            U256::from(400u128)
        );
    }

    #[test]
    fn a_tombstone_stops_the_same_intent_being_made_twice() {
        let mut state = LiveState::default();
        let key = taker_key(7, "77", Side::Buy);
        assert!(tombstoned(&state, &key).is_none());
        tombstone(&mut state, &key, 3);
        assert_eq!(tombstoned(&state, &key), Some(3));
    }

    #[tokio::test]
    async fn a001_paper_without_wallet_or_sdk() {
        let broker = Broker::new(
            crate::config::StrategyLimits {
                max_order_usd: 10.0,
                max_market_usd: 20.0,
                max_total_usd: 30.0,
                max_loss_usd: 5.0,
            },
            false,
            None,
            None,
        )
        .unwrap();
        let market = crate::bot::runtime::test_support::market(1);
        let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
        let result = broker
            .take(&market, &market.yes_token, Side::Buy, 10.0, 0.5, &book)
            .await
            .unwrap();
        assert_eq!(
            result,
            TakeResult::Filled {
                shares: 10.0,
                price: 0.5
            }
        );
        assert_eq!(broker.holding(&market.yes_token).unwrap().shares, 10.0);
        assert_eq!(broker.summary().open_cost, 5.0);
        assert!(broker.state.lock().live.attempts.is_empty());
    }

    #[tokio::test]
    async fn a003_broker_terminal_zero_vs_open_unknown() {
        use crate::service::predict_fun::account::tests::synthetic_trader;
        use std::sync::atomic::Ordering;
        for scenario in ["terminal", "open", "404", "timeout"] {
            let (trader, transport) = synthetic_trader(scenario);
            let identity = trader.identity();
            let gate = trader.gate.clone();
            let trader = Arc::new(trader);
            let broker = Broker::new(
                crate::config::StrategyLimits {
                    max_order_usd: 10.0,
                    max_market_usd: 20.0,
                    max_total_usd: 30.0,
                    max_loss_usd: 5.0,
                },
                true,
                Some(trader.clone()),
                Some(trader.clone()),
            )
            .unwrap()
            .with_account_gate(gate, identity);
            let mut market = crate::bot::runtime::test_support::market(1);
            market.yes_token = "77".into();
            market.no_token = "88".into();
            let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
            let result = broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await;
            if scenario == "terminal" {
                assert_eq!(
                    result.unwrap(),
                    TakeResult::Filled {
                        shares: 0.0,
                        price: 0.5
                    }
                );
                assert_eq!(broker.state.lock().total_exposure(), 0.0);
            } else {
                assert!(
                    result.is_err()
                        || matches!(result.unwrap(), TakeResult::Blocked(Blocked::Halted))
                );
                assert_eq!(broker.state.lock().total_exposure(), 5.0);
                assert_eq!(
                    broker
                        .take(&market, "88", Side::Buy, 10.0, 0.5, &book)
                        .await
                        .unwrap(),
                    TakeResult::Blocked(Blocked::Halted)
                );
            }
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
            let state = broker.state.lock();
            let record = state.live.attempts[&1].lock();
            assert!(record
                .identity
                .signed_hash
                .as_ref()
                .unwrap()
                .starts_with("0x"));
            assert_eq!(record.r0, U256::from(5_000_000_000_000_000_000u128));
            assert!(state.holdings.is_empty());
            assert_eq!(state.fees, 0.0);
        }
    }

    fn synthetic_broker(
        fault: &'static str,
    ) -> (
        Arc<Broker>,
        Arc<crate::service::predict_fun::account::tests::SyntheticRpc>,
        crate::service::venue::TradableMarket,
        crate::models::BookSnapshot,
    ) {
        let (trader, transport) =
            crate::service::predict_fun::account::tests::synthetic_trader(fault);
        let identity = trader.identity();
        let gate = trader.gate.clone();
        let trader = Arc::new(trader);
        let broker = Broker::new(
            crate::config::StrategyLimits {
                max_order_usd: 10.0,
                max_market_usd: 20.0,
                max_total_usd: 30.0,
                max_loss_usd: 5.0,
            },
            true,
            Some(trader.clone()),
            Some(trader),
        )
        .unwrap()
        .with_account_gate(gate, identity);
        let mut market = crate::bot::runtime::test_support::market(1);
        market.yes_token = "77".into();
        market.no_token = "88".into();
        let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
        (Arc::new(broker), transport, market, book)
    }

    #[tokio::test]
    async fn a009_runtime_account_cash_drift_and_closed_attempt_reseed() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        let (trader, transport) = scenario("terminal", "");
        let identity = trader.identity();
        let gate = trader.gate.clone();
        let trader = Arc::new(trader);
        let broker = Broker::new(
            crate::config::StrategyLimits {
                max_order_usd: 10.0,
                max_market_usd: 20.0,
                max_total_usd: 30.0,
                max_loss_usd: 5.0,
            },
            true,
            Some(trader.clone()),
            Some(trader),
        )
        .unwrap()
        .with_account_gate(gate.clone(), identity.clone());
        let mut market = crate::bot::runtime::test_support::market(1);
        market.yes_token = "77".into();
        market.no_token = "88".into();
        let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
        broker.seed_from_venue().await.unwrap();
        broker
            .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
            .await
            .unwrap();
        broker.sync_live().await.unwrap();
        broker.seed_from_venue().await.unwrap();
        assert!(gate.allows_new_orders(&identity));
        *transport.mode.lock() = "cash_drift";
        assert!(broker
            .take(&market, "88", Side::Buy, 10.0, 0.7, &book)
            .await
            .is_err());
        assert!(!gate.allows_new_orders(&identity));
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        assert_eq!(broker.summary().open_cost, 0.0);
        *transport.mode.lock() = "";
        broker.seed_from_venue().await.unwrap();
        assert!(gate.allows_new_orders(&identity));
        broker
            .take(&market, "88", Side::Buy, 10.0, 0.7, &book)
            .await
            .unwrap();
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 2);
        assert_eq!(transport.base.signs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a009_filled_runtime_ledger_matches_cash_and_net_inventory() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        for drift in ["cash_drift", "inventory_drift"] {
            let (trader, transport) = scenario("fills", "");
            let identity = trader.identity();
            let gate = trader.gate.clone();
            let trader = Arc::new(trader);
            let broker = Broker::new(
                crate::config::StrategyLimits {
                    max_order_usd: 10.0,
                    max_market_usd: 20.0,
                    max_total_usd: 30.0,
                    max_loss_usd: 5.0,
                },
                true,
                Some(trader.clone()),
                Some(trader),
            )
            .unwrap()
            .with_account_gate(gate.clone(), identity.clone());
            let mut market = crate::bot::runtime::test_support::market(1);
            market.yes_token = "77".into();
            market.no_token = "88".into();
            let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
            broker.seed_from_venue().await.unwrap();
            broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await
                .unwrap();
            transport.base.stage.store(1, Ordering::SeqCst);
            broker.sync_live().await.unwrap();
            assert!(gate.allows_new_orders(&identity));
            let holding = broker.holding("77").unwrap();
            let summary = broker.summary();
            *transport.mode.lock() = drift;
            assert!(broker.sync_live().await.is_err(), "{drift}");
            assert!(!gate.allows_new_orders(&identity));
            assert_eq!(broker.summary(), summary);
            assert_eq!(broker.holding("77").unwrap().shares, holding.shares);
            *transport.mode.lock() = "";
            broker.sync_live().await.unwrap();
            assert!(gate.allows_new_orders(&identity));
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a010_historical_hashes_seed_through_real_adapter() {
        use crate::service::predict_fun::fills::tests::{
            historical_trader, history_bindings_clear, queried_order_count,
        };
        use std::sync::atomic::Ordering;
        for mode in [
            "history_healthy",
            "history_open",
            "history_missing_fee",
            "history_missing_page",
            "history_loop",
            "history_wrong_account",
            "history_undeclared",
            "history_receipt_error",
            "history_extra_inventory",
        ] {
            let (trader, transport, hashes) = historical_trader(mode).await;

            let identity = trader.identity();
            let gate = trader.gate.clone();
            let trader = Arc::new(trader);
            let broker = Broker::new(
                crate::config::StrategyLimits {
                    max_order_usd: 10.0,
                    max_market_usd: 20.0,
                    max_total_usd: 30.0,
                    max_loss_usd: 5.0,
                },
                true,
                Some(trader.clone()),
                Some(trader.clone()),
            )
            .unwrap()
            .with_account_gate(gate.clone(), identity.clone());
            let result = broker.seed_from_venue().await;
            if mode == "history_healthy" {
                result.unwrap();
                assert!((broker.state.lock().total_exposure() - 9.9).abs() < 1e-9);
                assert!(
                    broker.holding("77").is_none(),
                    "inherited shares are not sellable strategy holdings"
                );
                for hash in &hashes {
                    assert!(
                        queried_order_count(&transport, hash) >= 3,
                        "each declared hash must be read before/after complete matches: {hash}"
                    );
                }
                broker.seed_from_venue().await.unwrap();
                assert!(gate.allows_new_orders(&identity));
                gate.reserve(&identity, 99).unwrap();
                gate.release(&identity, 99);
            } else {
                assert!(result.is_err(), "{mode}");
                assert!(broker.state.lock().live.baseline.is_none());
                let risk = if mode == "history_extra_inventory" {
                    10.9
                } else {
                    0.0
                };
                assert!((broker.state.lock().total_exposure() - risk).abs() < 1e-9);
                if mode == "history_extra_inventory" {
                    let copy = crate::service::position_store::PositionStore::new();
                    assert!(copy.sync_copy(trader.as_ref()).await.is_err());
                    assert!((copy.open_usd_by_tag("unknown") - 10.9).abs() < 1e-9);
                    assert!(copy.is_empty());
                }
                gate.confirm_budget(&identity);
                assert!(
                    !gate.allows_new_orders(&identity),
                    "confirmation must not override {mode}"
                );
                assert!(broker.seed_from_venue().await.is_err(), "{mode}");
                if mode == "history_receipt_error" {
                    *transport.mode.lock() = "history_healthy";
                    broker.seed_from_venue().await.unwrap();
                    assert!(
                        gate.allows_new_orders(&identity),
                        "a successfully re-read history must heal its own read failure"
                    );
                }
            }
            assert!(
                history_bindings_clear(&trader),
                "history lookups must not leak live bindings"
            );
            assert_eq!(
                transport.base.posts.load(Ordering::SeqCst),
                1,
                "history never submits"
            );
            assert_eq!(
                transport.base.signs.load(Ordering::SeqCst),
                1,
                "history never signs"
            );
        }
    }

    #[tokio::test]
    async fn a005_raw_consumer_conflicts_and_known_risk_are_atomic() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        for mode in ["revised_fee", "decrease", "huge_fee", "receipt_error"] {
            let (trader, transport) = scenario("fills", if mode == "huge_fee" { mode } else { "" });
            let gate = trader.gate.clone();
            let identity = trader.identity();
            let trader = Arc::new(trader);
            let broker = Broker::new(
                crate::config::StrategyLimits {
                    max_order_usd: 10.0,
                    max_market_usd: 20.0,
                    max_total_usd: 30.0,
                    max_loss_usd: 5.0,
                },
                true,
                Some(trader.clone()),
                Some(trader),
            )
            .unwrap()
            .with_account_gate(gate.clone(), identity.clone());
            let mut market = crate::bot::runtime::test_support::market(1);
            market.yes_token = "77".into();
            market.no_token = "88".into();
            let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
            broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await
                .unwrap();
            if mode == "huge_fee" {
                assert!(broker.holdings().is_empty());
                assert!((broker.state.lock().total_exposure() - 7.6).abs() < 1e-12);
                assert!(!gate.allows_new_orders(&identity));
            } else {
                if mode == "decrease" {
                    transport.base.stage.store(1, Ordering::SeqCst);
                    broker.sync_live().await.unwrap();
                    transport.base.stage.store(0, Ordering::SeqCst);
                } else {
                    *transport.mode.lock() = mode;
                }
                let before = broker.summary();
                let holding = broker.holding("77").unwrap();
                let result = broker.sync_live().await;
                if mode != "receipt_error" {
                    assert!(result.is_err(), "{mode}");
                }
                assert_eq!(broker.summary(), before, "{mode}");
                assert_eq!(broker.holding("77").unwrap().shares, holding.shares);
                assert_eq!(broker.holding("77").unwrap().cost, holding.cost);
            }
            assert_eq!(
                broker
                    .take(&market, "88", Side::Buy, 10.0, 0.5, &book)
                    .await
                    .unwrap(),
                TakeResult::Blocked(Blocked::Halted)
            );
            assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn fix_r1_broker_scan_growth_completion_is_not_revision() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        let (trader, transport) = scenario("fills", "scan_growth");
        let gate = trader.gate.clone();
        let identity = trader.identity();
        let trader = Arc::new(trader);
        let broker = Broker::new(
            crate::config::StrategyLimits {
                max_order_usd: 10.0,
                max_market_usd: 20.0,
                max_total_usd: 30.0,
                max_loss_usd: 5.0,
            },
            true,
            Some(trader.clone()),
            Some(trader),
        )
        .unwrap()
        .with_account_gate(gate, identity);
        let mut market = crate::bot::runtime::test_support::market(1);
        market.yes_token = "77".into();
        let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.5, 100.0)]);
        broker
            .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
            .await
            .unwrap();
        assert!(broker.holdings().is_empty());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        *transport.mode.lock() = "";
        broker.sync_live().await.unwrap();
        broker.sync_live().await.unwrap();
        assert!((broker.holding("77").unwrap().shares - 9.9).abs() < 1e-12);
        assert!((broker.holding("77").unwrap().cost - 4.62).abs() < 1e-12);
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.base.signs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a004_broker_actual_cash_fee_net_inventory() {
        use std::sync::atomic::Ordering;
        let (broker, transport, market, book) = synthetic_broker("fills");
        assert_eq!(
            broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await
                .unwrap(),
            TakeResult::Filled {
                shares: 4.0,
                price: 0.4
            }
        );
        assert!((broker.holding("77").unwrap().cost - 1.62).abs() < 1e-12);
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        transport.stage.store(1, Ordering::SeqCst);
        broker.sync_live().await.unwrap();
        let state = broker.state.lock();
        assert!((state.holdings["77"].shares - 9.9).abs() < 1e-12);
        assert!((state.holdings["77"].cost - 4.62).abs() < 1e-12);
        assert!((state.fees - 0.02).abs() < 1e-12);
        assert!((state.total_exposure() - 4.62).abs() < 1e-12);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a004_broker_sell_actual_cash_and_fee() {
        let (broker, transport, market, book) = synthetic_broker("sell");
        // Initial inventory is an explicit synthetic precondition, not a final
        // execution snapshot injected in place of the adapter under test.
        broker.state.lock().holdings.insert(
            "77".into(),
            crate::bot::runtime::Holding {
                market_id: 1,
                token: "77".into(),
                shares: 10.0,
                cost: 4.0,
                opened_at: chrono::Utc::now(),
            },
        );
        assert_eq!(
            broker
                .take(&market, "77", Side::Sell, 10.0, 0.4, &book)
                .await
                .unwrap(),
            TakeResult::Filled {
                shares: 10.0,
                price: 0.5
            }
        );
        assert!(broker.holding("77").is_none());
        let state = broker.state.lock();
        assert!((state.realized - 0.98).abs() < 1e-12);
        assert!((state.fees - 0.02).abs() < 1e-12);
        assert_eq!(
            state.live.net_cash[&1],
            U256::from(4_980_000_000_000_000_000u128)
        );
        assert_eq!(transport.posts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a005_broker_cumulative_once_under_concurrency() {
        use std::sync::atomic::Ordering;
        let (broker, transport, market, book) = synthetic_broker("missing_fee");
        let _ = broker
            .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
            .await
            .unwrap();
        assert!(broker.holding("77").is_none());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        transport.stage.store(1, Ordering::SeqCst);
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let broker = broker.clone();
                tokio::spawn(async move {
                    broker.sync_live().await.unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.await.unwrap();
        }
        assert!((broker.holding("77").unwrap().shares - 9.9).abs() < 1e-12);
        assert!((broker.summary().fees - 0.02).abs() < 1e-12);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a006_broker_incomplete_and_invalid_facts() {
        use std::sync::atomic::Ordering;
        for fault in ["missing_fee", "invalid_units", "receipt_fee", "log_block"] {
            let (broker, transport, market, book) = synthetic_broker(fault);
            let result = broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await;
            assert!(
                result.is_err() || matches!(result.unwrap(), TakeResult::Blocked(Blocked::Halted)),
                "{fault}"
            );
            assert!(broker.holding("77").is_none(), "{fault}");
            assert_eq!(broker.state.lock().total_exposure(), 5.0, "{fault}");
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a007_broker_submission_identity_and_no_replay() {
        use std::sync::atomic::Ordering;
        for fault in ["timeout", "post401", "malformed", "abort"] {
            let (broker, transport, market, book) = synthetic_broker(fault);
            if fault == "abort" {
                let task = {
                    let broker = broker.clone();
                    let market = market.clone();
                    let book = book.clone();
                    tokio::spawn(async move {
                        broker
                            .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                            .await
                    })
                };
                for _ in 0..100 {
                    if transport.posts.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(broker
                    .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                    .await
                    .is_err());
            }
            let original = broker.state.lock().live.attempts[&1]
                .lock()
                .identity
                .signed_hash
                .clone()
                .unwrap();
            assert_eq!(broker.state.lock().total_exposure(), 5.0);
            assert_eq!(
                broker
                    .take(&market, "88", Side::Buy, 10.0, 0.5, &book)
                    .await
                    .unwrap(),
                TakeResult::Blocked(Blocked::Halted)
            );
            transport.stage.store(1, Ordering::SeqCst);
            broker.sync_live().await.unwrap();
            assert_eq!(broker.state.lock().total_exposure(), 0.0);
            assert_eq!(
                broker.state.lock().live.attempts[&1]
                    .lock()
                    .identity
                    .signed_hash
                    .as_deref(),
                Some(original.as_str())
            );
            assert_eq!(
                broker
                    .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                    .await
                    .unwrap(),
                TakeResult::Blocked(Blocked::Halted)
            );
            assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a008_broker_remove_late_fill_closure() {
        use std::sync::atomic::Ordering;
        let (broker, transport, market, book) = synthetic_broker("late");
        let id = broker
            .rest(&market, "77", Side::Buy, 10.0, 0.45, 60, &book)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(broker.state.lock().total_exposure(), 4.5);
        broker.cancel(&[id.clone()]).await.unwrap();
        broker.sync_live().await.unwrap();
        assert_eq!(broker.holding("77").unwrap().shares, 4.0);
        assert!((broker.holding("77").unwrap().cost - 1.62).abs() < 1e-12);
        assert_eq!(broker.state.lock().total_exposure(), 4.5);
        transport.stage.store(1, Ordering::SeqCst);
        assert!(broker.sync_live().await.is_err());
        assert!(broker.state.lock().resting[&id].removing);
        transport.stage.store(2, Ordering::SeqCst);
        broker.sync_live().await.unwrap();
        assert_eq!(broker.state.lock().total_exposure(), 4.5);
        assert!(broker.state.lock().resting[&id].removing);
        transport.stage.store(3, Ordering::SeqCst);
        broker.sync_live().await.unwrap();
        assert!(broker.state.lock().resting.is_empty());
        assert!((broker.state.lock().total_exposure() - 1.62).abs() < 1e-12);
        assert_eq!(broker.holding("77").unwrap().shares, 4.0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fix_r1_rest_reconciles_account_and_tracked_open() {
        use crate::service::predict_fun::fills::tests::scenario;
        use std::sync::atomic::Ordering;
        for mode in ["cash_drift", "external_open", ""] {
            let (trader, transport) = scenario("open", "");
            let identity = trader.identity();
            let gate = trader.gate.clone();
            let trader = Arc::new(trader);
            let broker = Broker::new(
                crate::config::StrategyLimits {
                    max_order_usd: 10.0,
                    max_market_usd: 20.0,
                    max_total_usd: 30.0,
                    max_loss_usd: 5.0,
                },
                true,
                Some(trader.clone()),
                Some(trader),
            )
            .unwrap()
            .with_account_gate(gate, identity);
            broker.seed_from_venue().await.unwrap();
            *transport.mode.lock() = mode;
            let mut market = crate::bot::runtime::test_support::market(1);
            market.yes_token = "77".into();
            market.no_token = "88".into();
            let book = crate::bot::runtime::test_support::book(&[(0.4, 100.0)], &[(0.6, 100.0)]);
            let result = broker
                .rest(&market, "77", Side::Buy, 10.0, 0.5, 60, &book)
                .await;
            assert_eq!(result.is_ok(), mode.is_empty(), "{mode}");
            assert_eq!(
                transport.base.signs.load(Ordering::SeqCst),
                usize::from(mode.is_empty())
            );
            assert_eq!(
                transport.base.posts.load(Ordering::SeqCst),
                usize::from(mode.is_empty())
            );
            if mode.is_empty() {
                broker.sync_live().await.unwrap();
                *transport.mode.lock() = "tracked_open";
                check_runtime_account(&broker, true).await.unwrap();
                assert_eq!(
                    broker
                        .rest(&market, "88", Side::Buy, 10.0, 0.5, 60, &book)
                        .await
                        .unwrap(),
                    Err(Blocked::Halted)
                );
                assert_eq!(transport.base.posts.load(Ordering::SeqCst), 1);
                *transport.mode.lock() = "closed_rest";
                broker.sync_live().await.unwrap();
                *transport.mode.lock() = "";
                assert!(broker
                    .rest(&market, "88", Side::Buy, 10.0, 0.5, 60, &book)
                    .await
                    .unwrap()
                    .is_ok());
                assert_eq!(transport.base.posts.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[tokio::test]
    async fn fix_r1_startup_unexplained_positions_keep_risk_and_block() {
        use crate::service::position_store::PositionStore;
        use crate::service::predict_fun::fills::tests::{historical_trader, scenario};
        use std::sync::atomic::Ordering;
        let (trader, transport) = scenario("positions", "");
        let identity = trader.identity();
        let gate = trader.gate.clone();
        let trader = Arc::new(trader);
        let broker = Broker::new(
            crate::config::StrategyLimits {
                max_order_usd: 10.0,
                max_market_usd: 20.0,
                max_total_usd: 30.0,
                max_loss_usd: 5.0,
            },
            true,
            Some(trader.clone()),
            Some(trader.clone()),
        )
        .unwrap()
        .with_account_gate(gate.clone(), identity.clone());
        assert!(broker.seed_from_venue().await.is_err());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        assert!(broker.holdings().is_empty());
        gate.confirm_budget(&identity);
        assert!(!gate.allows_new_orders(&identity));
        let copy = PositionStore::new();
        assert!(copy.sync_copy(trader.as_ref()).await.is_err());
        assert_eq!(copy.open_usd_by_category("sports"), 5.0);
        assert!(copy.is_empty());
        assert_eq!(transport.base.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.base.posts.load(Ordering::SeqCst), 0);
        let (healthy, _, _) = historical_trader("history_healthy").await;
        let copy = PositionStore::new();
        copy.sync_copy(&healthy).await.unwrap();
        assert!((copy.open_usd_by_category("sports") - 9.9).abs() < 1e-9);
        assert!(
            copy.is_empty(),
            "historical risk never creates sellable copy inventory"
        );
    }

    #[tokio::test]
    async fn a009_broker_startup_and_runtime_account_gate() {
        use std::sync::atomic::Ordering;
        let (broker, transport, market, book) = synthetic_broker("terminal");
        broker.seed_from_venue().await.unwrap();
        broker.seed_from_venue().await.unwrap();
        assert_eq!(broker.state.lock().total_exposure(), 0.0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert!(matches!(
            broker
                .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
                .await
                .unwrap(),
            TakeResult::Filled { shares: 0.0, .. }
        ));
        let (broker, _, _, _) = synthetic_broker("positions");
        // Pagination and chain balances are complete, but no history explains ownership.
        assert!(broker.seed_from_venue().await.is_err());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        assert!(broker.seed_from_venue().await.is_err());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        for fault in [
            "chain",
            "owner",
            "initialized",
            "read",
            "position_loop",
            "position_missing",
            "position_cap",
            "history_loop",
        ] {
            let (broker, transport, market, book) = synthetic_broker(fault);
            assert!(broker.seed_from_venue().await.is_err(), "{fault}");
            assert!(matches!(
                broker
                    .take(&market, "88", Side::Buy, 10.0, 0.5, &book)
                    .await
                    .unwrap(),
                TakeResult::Blocked(Blocked::Halted)
            ));
            assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
            assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn a010_broker_restart_gate() {
        let (broker, transport, market, book) = synthetic_broker("timeout");
        broker.seed_from_venue().await.unwrap();
        assert!(broker
            .take(&market, "77", Side::Buy, 10.0, 0.5, &book)
            .await
            .is_err());
        broker
            .gate
            .as_ref()
            .unwrap()
            .confirm_budget(broker.identity.as_ref().unwrap());
        assert!(broker.seed_from_venue().await.is_err());
        assert_eq!(broker.state.lock().total_exposure(), 5.0);
        assert_eq!(transport.posts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn cumulative_net_state_is_atomic_and_reservation_is_not_double_counted() {
        let atoms = |value: &str| U256::from_str_radix(value, 10).unwrap();
        let q = atoms("10000000000000000000");
        let cash = atoms("4600000000000000000");
        let fee = atoms("20000000000000000");
        let share_fee = atoms("100000000000000000");
        let mut state = State::default();
        reserve_attempt(
            &mut state.live,
            AttemptRecord {
                identity: identity(1),
                request_gross_atoms: q,
                r0: atoms("5000000000000000000"),
                reserved_sell: U256::ZERO,
                phase: AttemptPhase::MayHaveSubmitted,
                submission: Submission::Accepted,
                last_reason: None,
            },
        );
        let mut facts = snapshot(1, 0, 0, 0);
        facts.quantity = Quantity::Known {
            gross_q_atoms: q,
            class: QuantityClass::Full,
        };
        facts.accounting = crate::service::predict_fun::fills::accounting(
            Side::Buy,
            q,
            cash,
            Some(fee),
            Some(share_fee),
        );
        facts.accounting_gross_q = Some(q);
        facts.gross_q_before = q;
        facts.gross_q_after = q;
        let state = Arc::new(parking_lot::Mutex::new(state));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let state = state.clone();
                let facts = facts.clone();
                std::thread::spawn(move || apply_snapshot(&mut state.lock(), &facts).unwrap())
            })
            .collect();
        let total = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().gross_q)
            .fold(U256::ZERO, |sum, value| sum + value);
        assert_eq!(total, q);
        let mut state = state.lock();
        assert!((state.holdings["77"].shares - 9.9).abs() < 1e-12);
        assert!((state.holdings["77"].cost - 4.62).abs() < 1e-12);
        assert!((state.fees - 0.02).abs() < 1e-12);
        assert!((state.total_exposure() - 5.0).abs() < 1e-12);
        facts.generation = 2;
        facts.closure = Closure::Final {
            evidence: "SYNTHETIC_PROOF".into(),
            final_gross_q: q,
        };
        assert_eq!(
            apply_snapshot(&mut state, &facts).unwrap().gross_q,
            U256::ZERO
        );
        assert!((state.total_exposure() - 4.62).abs() < 1e-12);
        facts.generation = 3;
        facts.accounting.collateral_fee = Field::Known(fee + U256::from(1));
        assert!(apply_snapshot(&mut state, &facts).is_err());
        assert!((state.holdings["77"].cost - 4.62).abs() < 1e-12);
        facts.generation = 0;
        assert_eq!(
            apply_snapshot(&mut state, &facts).unwrap().gross_q,
            U256::ZERO
        );
    }

    #[test]
    fn zero_needs_complete_accounting_and_closure() {
        let mut zero = snapshot(1, 0, 0, 0);
        zero.quantity = Quantity::Known {
            gross_q_atoms: U256::ZERO,
            class: QuantityClass::Zero,
        };
        zero.accounting.vwap = Field::Unknown("ZERO_HAS_NO_VWAP".into());
        assert_eq!(
            take_result(&zero, 0.5),
            TakeResult::Blocked(Blocked::Halted)
        );
        zero.closure = Closure::Final {
            evidence: "FAKE_CHAIN_EXPIRY".into(),
            final_gross_q: U256::ZERO,
        };
        assert_eq!(
            take_result(&zero, 0.5),
            TakeResult::Filled {
                shares: 0.0,
                price: 0.5
            }
        );
        assert!(!zero.accounting.vwap.is_known());
        zero.pages_complete = false;
        assert_eq!(
            take_result(&zero, 0.5),
            TakeResult::Blocked(Blocked::Halted)
        );
    }

    #[test]
    fn a007_reservation_retains_the_consumers_shared_handle() {
        let mut state = LiveState::default();
        let record = AttemptRecord {
            identity: identity(1),
            request_gross_atoms: U256::from(10),
            r0: U256::from(100),
            reserved_sell: U256::ZERO,
            phase: AttemptPhase::Prepared,
            submission: Submission::NotSubmitted,
            last_reason: None,
        };
        let handle = reserve_attempt(&mut state, record);
        handle.lock().identity.signed_hash = Some("synthetic-original-hash".into());
        handle.lock().phase = AttemptPhase::MayHaveSubmitted;
        let retained = state.attempts.get(&1).unwrap();
        assert!(Arc::ptr_eq(&handle, retained));
        assert_eq!(
            retained.lock().identity.signed_hash.as_deref(),
            Some("synthetic-original-hash")
        );
        assert_eq!(retained.lock().r0, U256::from(100));
    }

    #[test]
    fn an_unknown_fill_is_never_a_zero_fill() {
        let mut snapshot = snapshot(1, 0, 0, 0);
        snapshot.quantity = Quantity::Unknown;
        assert_eq!(
            take_result(&snapshot, 0.5),
            TakeResult::Blocked(Blocked::Halted)
        );
    }

    fn account(open_orders: &[&str]) -> crate::service::venue::AccountSnapshot {
        crate::service::venue::AccountSnapshot {
            identity: identity(1).account,
            collateral_atoms: Some(U256::from(100u128)),
            gas_atoms: Some(U256::ZERO),
            positions: Vec::new(),
            positions_explained: true,
            settled_zero: vec![],
            claimed_win: vec![],
            open_orders: open_orders.iter().map(|order| order.to_string()).collect(),
            complete: true,
        }
    }

    fn attempt(
        phase: AttemptPhase,
        submission: Submission,
        venue_order_id: Option<&str>,
    ) -> AttemptRecord {
        AttemptRecord {
            identity: OrderIdentity {
                venue_order_id: venue_order_id.map(str::to_string),
                ..identity(1)
            },
            request_gross_atoms: U256::from(10),
            r0: U256::from(100),
            reserved_sell: U256::ZERO,
            phase,
            submission,
            last_reason: None,
        }
    }

    type Changes = Vec<(String, Side, U256, U256)>;

    /// The window's own accepted order may be listed as outstanding; every other
    /// id, and any list without such an attempt, stays unexplained.
    #[test]
    fn only_the_own_accepted_open_order_is_explained_in_the_delta_check() {
        let initial = account(&[]);
        let cases: [(&[&str], Option<&str>, bool); 6] = [
            (&["venue-1"], Some("venue-1"), true),
            (&[], Some("venue-1"), true),
            (&["venue-1"], None, false),
            (&["venue-2"], Some("venue-1"), false),
            (&["venue-1", "venue-2"], Some("venue-1"), false),
            (&["venue-1", "venue-1"], Some("venue-2"), false),
        ];
        for (open_orders, own_order, reconciled) in cases {
            let result = check_account_delta_with_own_order(
                &initial,
                &account(open_orders),
                own_order,
                Changes::new(),
            );
            assert_eq!(result.is_ok(), reconciled, "{open_orders:?} / {own_order:?}");
            if let Err(error) = result {
                assert_eq!(error.to_string(), "RECONCILIATION_REQUIRED:OPEN_ORDERS");
            }
        }
    }

    #[test]
    fn a_claimed_payout_moves_exactly_its_cash_and_shares() {
        let initial = account(&[]);
        // The window bought ten shares for forty; the official result paid the
        // claim twenty, so the account holds eighty instead of forty.
        let changes: Changes = vec![("77".into(), Side::Buy, U256::from(40), U256::from(10))];
        let mut current = account(&[]);
        current.collateral_atoms = Some(U256::from(80));
        let claims = vec![("77".to_string(), U256::from(10), U256::from(20))];
        assert!(check_account_delta_with_claims(
            &initial, &current, None, changes.clone(), claims.clone()).is_ok());
        // Without the claim the same read is an unexplained cash and position
        // difference.
        assert!(check_account_delta_with_claims(
            &initial, &current, None, changes.clone(), std::iter::empty()).is_err());
        // One atom of cash off, or one atom too many shares claimed, still fails.
        for (shares, cash) in [("10", "19"), ("10", "21"), ("11", "20"), ("9", "20")] {
            let claims = vec![("77".to_string(), U256::from_str_radix(shares, 10).unwrap(),
                U256::from_str_radix(cash, 10).unwrap())];
            assert!(check_account_delta_with_claims(
                &initial, &current, None, changes.clone(), claims).is_err(),
                "{shares}/{cash}");
        }
        // A claim for a token the ledger doesn't hold is part of the baseline,
        // not a second credit: the cash comparison still decides.
        let mut baseline = account(&[]);
        baseline.collateral_atoms = Some(U256::from(100));
        let foreign = vec![("99".to_string(), U256::from(10), U256::from(20))];
        assert!(check_account_delta_with_claims(
            &baseline, &baseline, None, Changes::new(), foreign).is_ok());
    }

    /// Only a proven ACK carrying the venue's own id, with the attempt still
    /// open, names the in-flight order; and then only if it is the sole one.
    #[test]
    fn the_in_flight_order_needs_a_proven_ack_an_id_and_an_open_attempt() {
        let accepted = attempt(AttemptPhase::Submitted, Submission::Accepted, Some("venue-1"));
        assert_eq!(only_in_flight_order([&accepted]), Some("venue-1"));
        for record in [
            attempt(AttemptPhase::Prepared, Submission::NotSubmitted, None),
            attempt(AttemptPhase::Submitted, Submission::Accepted, None),
            attempt(AttemptPhase::MayHaveSubmitted, Submission::Unknown, Some("venue-1")),
            attempt(AttemptPhase::Closed, Submission::Accepted, Some("venue-1")),
        ] {
            assert_eq!(only_in_flight_order([&record]), None, "{record:?}");
        }
        let second = attempt(AttemptPhase::Submitted, Submission::Accepted, Some("venue-2"));
        assert_eq!(only_in_flight_order([&accepted, &second]), None);
        assert_eq!(only_in_flight_order(std::iter::empty::<&AttemptRecord>()), None);
    }
}
