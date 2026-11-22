//! The typed wire of the controlled live domain.
//!
//! One schema version, one envelope shape, one strict key set per payload.
//! There is no free-form detail bag: every field a reader may rely on is
//! named here, and anything else is refused before it reaches the disk.
//!
//! The emitter builds the envelope from the broker's own state and from the
//! venue's own facts. It never computes an amount that the ledger doesn't
//! already hold, and it never fills an unknown with a zero.

use crate::bot::arbitrage::phase1::DecisionDiagnostic;
use crate::bot::runtime::live_accounting::SignedAtoms;
use crate::bot::runtime::live::AppliedExecution;
use crate::service::venue::{
    AttemptPhase, Closure, ExecutionSnapshot, Field, Quantity, QuantityClass, Submission,
};
use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod types;
pub use self::types::{
    LIVE_SCHEMA_VERSION, LIVE_EVENT_PREFIX, MAX_EVENT_LINE_BYTES, MAX_EVENT_ARRAY_ITEMS,
    reason, REASONS, is_known_reason, EventKind, LiveAccounting, LiveRunIdentity, LiveEvent,
    ENVELOPE_KEYS, ACCOUNTING_KEYS, FACT_KEYS, DELTA_KEYS, payload_keys, is_atoms,
    is_signed_atoms, is_hash, is_order_ref, is_decimal_id, check_exact_keys,
    LivePayload, DecisionPayload, ReservationPayload, OrderPayload, ExecutionPayload,
    ReconciliationPayload, LossLatchPayload, StopPayload, RecoveryPayload, PayoutPayload,
    FinalPayload, ExecutionFacts, LiveFact, ExecutionDelta, LivePosition, UnresolvedAttempt,
};

/// The quantity class of a snapshot, as the wire names it.
pub fn quantity_class(snapshot: &ExecutionSnapshot) -> (&'static str, Option<U256>) {
    match snapshot.quantity {
        Quantity::Unknown => ("UNKNOWN", None),
        Quantity::Known {
            gross_q_atoms,
            class,
        } => (
            match class {
                QuantityClass::Zero => "ZERO",
                QuantityClass::Partial => "PARTIAL",
                QuantityClass::Full => "FULL",
            },
            Some(gross_q_atoms),
        ),
    }
}

/// The closure of a snapshot, as the wire names it.
pub fn closure_kind(snapshot: &ExecutionSnapshot) -> (&'static str, Option<U256>) {
    match &snapshot.closure {
        Closure::Open => ("OPEN", None),
        Closure::Removing => ("REMOVING", None),
        Closure::Unproven => ("UNPROVEN", None),
        Closure::Final { final_gross_q, .. } => ("FINAL", Some(*final_gross_q)),
    }
}

/// The submission of a snapshot, as the wire names it.
pub fn submission_kind(submission: Submission) -> &'static str {
    match submission {
        Submission::NotSubmitted => "NOT_SUBMITTED",
        Submission::RejectedFinal => "REJECTED_FINAL",
        Submission::Accepted => "ACCEPTED",
        Submission::Unknown => "UNKNOWN",
    }
}

/// The attempt phase of a handle, as the wire names it.
pub fn phase_kind(phase: AttemptPhase) -> &'static str {
    match phase {
        AttemptPhase::Prepared => "PREPARED",
        AttemptPhase::MayHaveSubmitted => "MAY_HAVE_SUBMITTED",
        AttemptPhase::Submitted => "SUBMITTED",
        AttemptPhase::Closed => "CLOSED",
    }
}

/// One accounting fact, as the wire names it.
fn project_fact(field: &Field<U256>, coverage: Option<U256>, zero_vwap: bool) -> LiveFact {
    let (status, atoms, reason) = match field {
        _ if zero_vwap => ("NOT_APPLICABLE", None, reason::NOT_APPLICABLE),
        Field::Known(value) => ("KNOWN", Some(value.to_string()), reason::NONE),
        Field::Unknown(_) => ("UNKNOWN", None, reason::FACT_INCOMPLETE),
    };
    LiveFact {
        status: status.into(),
        atoms,
        coverage_gross_atoms: coverage.map(|value| value.to_string()),
        reason: reason.into(),
    }
}

/// Project the six facts from the original snapshot, without inferring coverage.
pub fn execution_facts(snapshot: &ExecutionSnapshot) -> ExecutionFacts {
    let coverage = snapshot.accounting_gross_q;
    ExecutionFacts {
        gross_cash: project_fact(&snapshot.accounting.gross_cash, coverage, false),
        collateral_fee: project_fact(&snapshot.accounting.collateral_fee, coverage, false),
        share_fee: project_fact(&snapshot.accounting.share_fee, coverage, false),
        net_cash: project_fact(&snapshot.accounting.net_cash, coverage, false),
        net_inventory: project_fact(&snapshot.accounting.net_inventory, coverage, false),
        vwap: project_fact(&snapshot.accounting.vwap, coverage,
            snapshot.gross_q() == Some(U256::ZERO) && coverage == Some(U256::ZERO) && snapshot.pages_complete),
    }
}

/// The execution facts of a snapshot.
pub fn facts_json(snapshot: &ExecutionSnapshot) -> Value {
    json!(execution_facts(snapshot))
}

/// Project only the increments actually committed in the original application.
pub fn execution_delta(applied: &AppliedExecution) -> ExecutionDelta {
    let known = applied.applied || (applied.duplicate && applied.accounting_complete);
    ExecutionDelta {
        gross_q_atoms: known.then(|| applied.delta.gross_q.to_string()),
        gross_cash_atoms: known.then(|| applied.delta.gross_cash.to_string()),
        collateral_fee_atoms: known.then(|| applied.delta.collateral_fee.to_string()),
        share_fee_atoms: known.then(|| applied.delta.share_fee.to_string()),
        net_cash_atoms: known.then_some(applied.net_cash_delta).flatten().map(|v| v.to_string()),
        net_inventory_atoms: known.then_some(applied.net_inventory_delta).flatten().map(|v| v.to_string()),
    }
}

/// The execution delta of an applied reading.
///
/// A field that wasn't applied is null, not zero: only a reading that was
/// confirmed to add nothing may report zero.
pub fn delta_json(applied: &AppliedExecution) -> Value {
    json!(execution_delta(applied))
}

/// Project the current locked live ledger without creating another cash table.
pub(super) fn state_accounting(state: &crate::bot::runtime::State) -> LiveAccounting {
    use super::live_accounting::{self, AccountingStatus, StateFacts};
    let run = state.live.controlled.as_ref();
    let exposure = live_accounting::risk_exposure(&state.live);
    let projected = live_accounting::project_window(&StateFacts { state: &state.live },
        &super::live_control::fresh_marks(state), None);
    let read = run.and_then(|run| run.account_read.as_ref().filter(|read| {
        run.phase1.clock.now().is_ok_and(|now| read.matches(&state.live, now))
            && read.snapshot.identity == run.identity
    }));
    let cash = exposure.as_ref().map(|_| projected.cash);
    let available = read.and_then(|read| read.snapshot.collateral_atoms).zip(cash)
        .zip(exposure.as_ref()).and_then(|((official, budget), risk)| {
            official.checked_sub(risk.reserved).zip(budget.checked_sub(risk.reserved))
                .map(|(official, budget)| official.min(budget))
        });
    let fact_reason = run.and_then(|run| run.control.lock().stop.as_ref().map(|stop| stop.reason.clone()));
    let conflict = projected.status == AccountingStatus::Conflict
        || fact_reason.as_deref() == Some(reason::FACT_CONFLICT);
    let complete = projected.status == AccountingStatus::Complete && available.is_some()
        && read.is_some() && !conflict && fact_reason.as_deref() != Some(reason::FACT_INCOMPLETE);
    LiveAccounting {
        cash: read.and(cash), available,
        reserved: exposure.as_ref().map(|risk| risk.reserved),
        cost: exposure.as_ref().map(|risk| risk.cost),
        realized: complete.then_some(projected.realized),
        unrealized: complete.then_some(projected.unrealized),
        nav: complete.then_some(projected.nav), net_pnl: complete.then_some(projected.net_pnl),
        loss_latched: run.is_some_and(|run| run.loss_latched),
        status: if conflict { "CONFLICT" } else if complete { "COMPLETE" } else { "INCOMPLETE" }.into(),
        reason: if conflict { reason::FACT_CONFLICT } else if complete { reason::NONE } else { reason::FACT_INCOMPLETE }.into(),
        mark_time: run.and_then(|run| run.mark_receipts.values().map(|time| time.utc).min()),
        pending_claim_value: projected.pending_claim_value,
        gas_status: "NOT_MEASURED".into(), gas_paid_atoms: None,
    }
}

/// Project one original ledger commit while its State lock is still held.
/// The caller owns the operation lane and flushes these records after unlocking.
pub(super) fn prepare_execution(
    state: &crate::bot::runtime::State,
    snapshot: &ExecutionSnapshot,
    applied: &AppliedExecution,
    previous_reservation: U256,
    metadata: Option<&crate::service::venue::ControlledOrderMetadata>,
) -> Result<Vec<PreparedLiveEvent>, String> {
    let Some(run) = state.live.controlled.as_ref() else { return Ok(Vec::new()); };
    let id = snapshot.identity.local_attempt_id;
    let handle = state.live.attempts.get(&id).ok_or("UNBOUND_ATTEMPT")?;
    let record = handle.lock().clone();
    if record.identity != snapshot.identity { return Err("ORDER_IDENTITY_MISMATCH".into()); }
    let candidate = run.attempt_candidates.get(&id).ok_or("UNBOUND_CANDIDATE")?;
    let hash = snapshot.identity.signed_hash.clone().ok_or("SIGNED_BOUND_UNPROVEN")?;
    let accounting = state_accounting(state);
    let metadata = metadata.filter(|data| data.execution_generation == snapshot.generation);
    let chain_checked = metadata.and_then(|data| data.settlement_checked_at);
    let settled = snapshot.tx_hash.is_some() && chain_checked.is_some()
        && metadata.is_some_and(|data| data.chain_settled);
    let (quantity, gross) = quantity_class(snapshot);
    let (closure, final_gross) = closure_kind(snapshot);
    let mut events = vec![run.emitter.prepare(snapshot.observed_at,
        Some(snapshot.identity.market_id.to_string()), Some(snapshot.identity.token_id.clone()),
        &accounting, LivePayload::Execution(ExecutionPayload {
            attempt_id: id.to_string(), signed_hash: hash,
            venue_order_id: snapshot.identity.venue_order_id.clone(), generation: snapshot.generation,
            observed_at: snapshot.observed_at, pages_complete: snapshot.pages_complete,
            quantity_class: quantity.into(), gross_q_atoms: gross.map(|value| value.to_string()),
            accounting_gross_q_atoms: snapshot.accounting_gross_q.map(|value| value.to_string()),
            closure: closure.into(), final_gross_q_atoms: final_gross.map(|value| value.to_string()),
            facts: execution_facts(snapshot), delta: execution_delta(applied),
            tx_hash: snapshot.tx_hash.clone(),
            settlement_status: if settled { "CHAIN_SETTLED" } else if snapshot.tx_hash.is_some() {
                "MATCHED"
            } else { "UNVERIFIED" }.into(),
            settlement_checked_at: chain_checked,
            reason: if accounting.status == "CONFLICT" { reason::FACT_CONFLICT }
                else if applied.accounting_complete { reason::NONE } else { reason::FACT_INCOMPLETE }.into(),
        }))?];
    let remaining = state.live.reservations.get(&id).copied().unwrap_or(U256::ZERO);
    if remaining < previous_reservation {
        events.push(run.emitter.prepare(snapshot.observed_at,
            Some(snapshot.identity.market_id.to_string()), Some(snapshot.identity.token_id.clone()),
            &accounting, LivePayload::Reservation(ReservationPayload {
                candidate_id: candidate.clone(), attempt_id: id.to_string(), action: "RELEASE".into(),
                worst_cash_atoms: record.r0.to_string(), remaining_atoms: remaining.to_string(),
                reason: reason::NONE.into(),
            }))?);
    }
    Ok(events)
}

/// Project the actual handle after preparation, acceptance or a proven refusal.
/// No signed quantity is reconstructed from the planned floating-point order.
pub(super) fn prepare_order(
    state: &crate::bot::runtime::State,
    attempt_id: u64,
    bound: Option<&crate::service::venue::SignedExecutionBound>,
    metadata: Option<&crate::service::venue::ControlledOrderMetadata>,
    event_reason: &str,
) -> Result<Option<PreparedLiveEvent>, String> {
    let Some(run) = state.live.controlled.as_ref() else { return Ok(None); };
    let record = state.live.attempts.get(&attempt_id).ok_or("UNBOUND_ATTEMPT")?.lock().clone();
    if bound.is_some_and(|bound| record.identity.signed_hash.as_deref() != Some(&bound.signed_hash)
        || record.identity.account != bound.account || record.identity.market_id != bound.market_id
        || record.identity.token_id != bound.token_id || record.request_gross_atoms != bound.request_gross_atoms)
    { return Err("ORDER_IDENTITY_MISMATCH".into()); }
    let candidate = run.attempt_candidates.get(&attempt_id).ok_or("UNBOUND_CANDIDATE")?;
    run.emitter.prepare(run.phase1.clock.now().map_err(|_| "DATA_UNAVAILABLE")?.utc,
        Some(record.identity.market_id.to_string()), Some(record.identity.token_id.clone()),
        &state_accounting(state), LivePayload::Order(OrderPayload {
            candidate_id: candidate.clone(), attempt_id: attempt_id.to_string(),
            signed_hash: record.identity.signed_hash, venue_order_id: record.identity.venue_order_id,
            attempt_phase: phase_kind(record.phase).into(), submission: submission_kind(record.submission).into(),
            request_gross_atoms: record.request_gross_atoms.to_string(),
            max_cash_atoms: bound.map_or(record.r0, |bound| bound.max_cash_atoms).to_string(),
            min_net_shares_atoms: bound.map(|bound| bound.min_net_shares_atoms.to_string()),
            expires_at: bound.and_then(|bound| DateTime::from_timestamp(bound.expiration, 0)),
            removal_locked_until: metadata.and_then(|metadata| metadata.removal_locked_until),
            reason: event_reason.into(),
        })).map(Some)
}

/// Emit only a reservation reduction observed in the original ledger.
pub(super) fn prepare_release(
    state: &crate::bot::runtime::State,
    attempt_id: u64,
    previous: U256,
    event_reason: &str,
) -> Result<Option<PreparedLiveEvent>, String> {
    let Some(run) = state.live.controlled.as_ref() else { return Ok(None); };
    let remaining = state.live.reservations.get(&attempt_id).copied().unwrap_or(U256::ZERO);
    if remaining >= previous { return Ok(None); }
    let record = state.live.attempts.get(&attempt_id).ok_or("UNBOUND_ATTEMPT")?.lock().clone();
    let candidate = run.attempt_candidates.get(&attempt_id).ok_or("UNBOUND_CANDIDATE")?;
    run.emitter.prepare(run.phase1.clock.now().map_err(|_| "DATA_UNAVAILABLE")?.utc,
        Some(record.identity.market_id.to_string()), Some(record.identity.token_id),
        &state_accounting(state), LivePayload::Reservation(ReservationPayload {
            candidate_id: candidate.clone(), attempt_id: attempt_id.to_string(), action: "RELEASE".into(),
            worst_cash_atoms: record.r0.to_string(), remaining_atoms: remaining.to_string(),
            reason: event_reason.into(),
        })).map(Some)
}

/// Project sticky lifecycle facts once; the lane owner must flush this batch.
/// A failed output poisons the shared emitter rather than retrying a partial line.
pub(super) fn prepare_lifecycle(
    state: &mut crate::bot::runtime::State,
) -> Result<Vec<PreparedLiveEvent>, String> {
    let accounting = state_accounting(state);
    let Some(run) = state.live.controlled.as_mut() else { return Ok(Vec::new()); };
    let now = run.phase1.clock.now().map_err(|_| "DATA_UNAVAILABLE")?;
    let mut events = Vec::new();
    if run.loss_latched && !run.loss_event_emitted {
        events.push(run.emitter.prepare(now.utc, None, None, &accounting,
            LivePayload::LossLatch(LossLatchPayload {
                latched: true, trigger_net_pnl_atoms: run.loss_trigger.map(|value| value.to_text()),
                reason: reason::LOSS_LATCH.into(),
            }))?);
        run.loss_event_emitted = true;
    }
    let stop = run.control.lock().stop.clone();
    if let Some(stop) = stop.filter(|_| !run.stop_event_emitted) {
        events.push(run.emitter.prepare(now.utc, None, None, &accounting,
            LivePayload::Stop(StopPayload { stop_new_risk: true, reason: stop.reason,
                takeover_required: stop.takeover_required, alert_status: "LOCAL_RECORDED".into(),
                stop_detail: stop.detail,
            }))?);
        run.stop_event_emitted = true;
    }
    Ok(events)
}

/// Project the official account response, disclosing only window-owned tokens.
/// Foreign positions/orders set the flag, never expand the disclosed account.
pub(super) fn prepare_reconciliation(
    state: &crate::bot::runtime::State,
    snapshot: &crate::service::venue::AccountSnapshot,
    checked_at: DateTime<Utc>,
    scope: &str,
    complete: bool,
    event_reason: &str,
) -> Result<Option<PreparedLiveEvent>, String> {
    let Some(run) = state.live.controlled.as_ref() else { return Ok(None); };
    let mut positions = Vec::new();
    let mut foreign = snapshot.identity != run.identity || !snapshot.positions_explained;
    for (token, shares) in &snapshot.positions {
        let records: Vec<_> = state.live.attempts.iter().filter_map(|(id, handle)| {
            let record = handle.lock();
            (record.identity.token_id == *token).then_some((*id, record.identity.market_id))
        }).collect();
        if let Some((_, market)) = records.first() {
            let cost = records.iter().try_fold(U256::ZERO, |total, (id, _)|
                total.checked_add(state.live.net_cash.get(id).copied().unwrap_or(U256::ZERO)));
            positions.push(LivePosition { market_id: market.to_string(), token: token.clone(),
                net_shares_atoms: shares.to_string(), cost_atoms: cost.map(|cost| cost.to_string()) });
        } else { foreign = true; }
    }
    positions.sort_by(|left, right| (&left.market_id, &left.token).cmp(&(&right.market_id, &right.token)));
    let mut open_hashes = Vec::new();
    for order in &snapshot.open_orders {
        let hash = state.live.attempts.values().find_map(|handle| {
            let record = handle.lock();
            (record.identity.venue_order_id.as_deref() == Some(order.as_str())
                || record.identity.signed_hash.as_deref() == Some(order.as_str()))
                .then(|| record.identity.signed_hash.clone()).flatten()
        });
        if let Some(hash) = hash { open_hashes.push(hash); } else { foreign = true; }
    }
    open_hashes.sort();
    open_hashes.dedup();
    run.emitter.prepare(checked_at, None, None, &state_accounting(state),
        LivePayload::Reconciliation(ReconciliationPayload { scope: scope.into(), checked_at,
            complete: complete && snapshot.complete && !foreign,
            collateral_atoms: snapshot.collateral_atoms.map(|amount| amount.to_string()),
            gas_balance_atoms: snapshot.gas_atoms.map(|amount| amount.to_string()),
            positions, open_hashes, foreign_risk: foreign, reason: event_reason.into(),
        })).map(Some)
}

/// `true` when the holding is settled: a zero write-off, or a claimed payout
/// already received as exact cash. Anything else is still open risk.
///
/// A claim settles the holding only when it pays exactly what the confirmed
/// official result owes: the same net inventory and the same claim value. A
/// claim one atom off is an unexplained payout, not a settled holding.
fn settled_holding(
    inventory: U256,
    credit: Option<(U256, U256)>,
    outcome: Option<&super::live_control::LiveOutcome>,
) -> bool {
    credit.is_some_and(|(claimed, paid)| {
        claimed == inventory
            && outcome.is_some_and(|outcome| {
                outcome.position_net_atoms == inventory
                    && outcome.claim_value_atoms == Some(paid)
            })
    })
        || outcome.is_some_and(|outcome| {
            outcome.position_net_atoms == inventory
                && outcome.claim_value_atoms == Some(U256::ZERO)
        })
}

/// Prepare the payout record of a claim that became ledger cash.
///
/// The claim's cash is the venue's own read-back of the exact atoms it paid,
/// so the record carries the received atoms and no transaction hash of its
/// own: no redeem transaction is performed by this runtime.
pub(super) fn prepare_claim_paid(
    state: &crate::bot::runtime::State,
    id: u64,
    inventory: U256,
    cash: U256,
    observed_at: DateTime<Utc>,
) -> Result<Option<PreparedLiveEvent>, String> {
    let run = state.live.controlled.as_ref().ok_or("CONTEXT_MISSING")?;
    let record = state.live.attempts.get(&id).ok_or("UNBOUND_ATTEMPT")?.lock().clone();
    let outcome = run.attempt_outcomes.get(&id).ok_or("CONTEXT_MISSING")?;
    run.emitter.prepare(observed_at, Some(record.identity.market_id.to_string()),
        Some(record.identity.token_id.clone()), &state_accounting(state),
        LivePayload::Payout(PayoutPayload { status: "CASH_CONFIRMED".into(),
            payout_yes: outcome.payout_yes, position_net_atoms: inventory.to_string(),
            claim_value_atoms: Some(cash.to_string()),
            cash_received_atoms: Some(cash.to_string()), tx_hash: None,
            observed_at, reason: reason::NONE.into(),
        })).map(Some)
}

/// Produce the one terminal live record from the original locked ledger.
/// The operation-lane owner has already finished all bounded read-only work.
pub(super) fn prepare_final(
    state: &mut crate::bot::runtime::State, cleanup_complete: bool,
) -> Result<Vec<PreparedLiveEvent>, String> {
    use super::live_control::LiveRunState;
    let run = state.live.controlled.as_ref().ok_or("CONTEXT_MISSING")?;
    if run.emitter.is_finished() { return Ok(Vec::new()); }
    let now = run.phase1.clock.now().map_err(|_| "DATA_UNAVAILABLE")?;
    let mut unresolved = Vec::new();
    let mut positions = Vec::new();
    let mut cash_complete = true;
    let risk = super::live_accounting::risk_exposure(&state.live);
    let mut ids: Vec<_> = state.live.attempts.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        let record = state.live.attempts[&id].lock();
        let reserved = state.live.reservations.get(&id).copied().unwrap_or(U256::ZERO)
            .max(state.live.observed_risk_floor.get(&id).copied().unwrap_or(U256::ZERO)
                .saturating_sub(state.live.net_cash.get(&id).copied().unwrap_or(U256::ZERO)));
        if record.phase != AttemptPhase::Closed || !reserved.is_zero() {
            unresolved.push(UnresolvedAttempt { attempt_id: id.to_string(),
                signed_hash: record.identity.signed_hash.clone(), closure: "UNPROVEN".into(),
                reserved_atoms: reserved.to_string(), reason: reason::FACT_INCOMPLETE.into() });
        }
        let inventory = state.live.net_inventory.get(&id).copied().unwrap_or(U256::ZERO);
        if !inventory.is_zero() {
            // A settled holding is not open risk: a zero write-off, or a
            // claimed payout the confirmed result owes exactly.
            let credit = state.live.claims.get(&id).copied();
            let outcome = run.attempt_outcomes.get(&id);
            let settled = settled_holding(inventory, credit, outcome);
            if !settled {
                cash_complete = false;
                // An unsettled holding is open risk and stays in the terminal
                // set unless the venue's own account read explains it as a
                // claimed win: the same token with the same inventory. That
                // claim is the only venue fact that removes a holding while
                // leaving a residual (a claim that did not pay exactly), and
                // the reader can reconcile the remainder only when the
                // position is gone. Every other unsettled holding is retained.
                let claimed = run.account_read.as_ref().is_some_and(|read| read
                    .snapshot
                    .claimed_win
                    .iter()
                    .any(|win| win.token == record.identity.token_id
                        && win.inventory_atoms == inventory));
                if !claimed {
                    positions.push(LivePosition { market_id: record.identity.market_id.to_string(),
                        token: record.identity.token_id.clone(), net_shares_atoms: inventory.to_string(),
                        cost_atoms: state.live.net_cash.get(&id).map(|cash| cash.to_string()) });
                }
            }
        }
    }
    // An empty local ledger is not evidence of an empty authoritative account:
    // startup may have failed before any baseline/receipt could be established.
    let execution_complete = unresolved.is_empty() && cleanup_complete && risk.is_some()
        && state.live.baseline.is_some() && run.account_read.is_some();
    let accounting = state_accounting(state);
    cash_complete &= execution_complete && accounting.status == "COMPLETE";
    let audit_complete = !run.emitter.output_failed();
    let stop = run.control.lock().stop.clone();
    let stop_reason = stop.as_ref().map(|stop| stop.reason.clone()).unwrap_or_else(|| reason::MANUAL_STOP.into());
    let stop_detail = stop.as_ref().and_then(|stop| stop.detail.clone());
    let takeover = !execution_complete || !cash_complete || !audit_complete
        || stop.as_ref().is_some_and(|stop| stop.takeover_required);
    let mut events = prepare_lifecycle(state)?;
    let run = state.live.controlled.as_mut().unwrap();
    events.push(run.emitter.prepare(now.utc, None, None, &accounting,
        LivePayload::Recovery(RecoveryPayload { policy: "COLD_START_REFUSE".into(),
            state: "HANDOFF".into(), reason: stop_reason.clone() }))?);
    events.push(run.emitter.prepare(now.utc, None, None, &accounting,
        LivePayload::Final(FinalPayload { run_status: if execution_complete && audit_complete {
            "STOPPED" } else { "INCOMPLETE" }.into(), stop_reason,
            execution_complete, cash_complete, audit_complete, takeover_required: takeover,
            unresolved, positions, stop_detail }))?);
    run.pending_candidate = None;
    run.control.lock().state = if takeover { LiveRunState::Handoff } else { LiveRunState::Ended };
    Ok(events)
}

/// Record a failure on an already-owned operation lane, without fabricating an attempt.
pub(super) fn stop_on_lane(broker: &crate::bot::runtime::Broker, reason: &str) -> Result<(), String> {
    let events = {
        let mut state = broker.state.lock();
        let Some(run) = state.live.controlled.as_mut() else { return Ok(()); };
        if run.emitter.is_finished() { return Ok(()); }
        run.stop(reason, true, chrono::Utc::now());
        prepare_lifecycle(&mut state)?
    };
    flush_events(events)
}

/// Flush a lane-owned batch after releasing State, without retrying a failed line.
pub(super) fn flush_events(events: Vec<PreparedLiveEvent>) -> Result<(), String> {
    for event in events { event.flush()?; }
    Ok(())
}

/// The decision payload of an evaluation.
///
/// The diagnostic is the core's own object, unchanged: the live domain does
/// not add a field to it and does not rename its reasons.
pub fn decision_json(diagnostic: &DecisionDiagnostic) -> Value {
    serde_json::to_value(diagnostic).unwrap_or(Value::Null)
}

/// Refuse non-finite core scalars before JSON serialization can erase them.
fn check_decision_finite(decision: &DecisionDiagnostic) -> Result<(), String> {
    if !crate::bot::arbitrage::phase1::diagnostic_is_finite(decision) {
        return Err("DECISION_NUMBER_INVALID".into());
    }
    Ok(())
}

/// The emitter of one run's live events.
///
/// It owns the sequence number and the output failure flag. A failed write is
/// sticky: the run may not present itself as complete afterwards.
#[derive(Debug, Clone)]
pub struct LiveEventEmitter {
    run: LiveRunIdentity,
    seq: Arc<AtomicU64>,
    output_failed: Arc<std::sync::atomic::AtomicBool>,
    finished: Arc<std::sync::atomic::AtomicBool>,
}

impl LiveEventEmitter {
    pub fn new(run: LiveRunIdentity) -> Self {
        Self {
            run,
            seq: Arc::new(AtomicU64::new(0)),
            output_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            finished: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Share the single run sequence and sticky failure bit with Broker state.
    pub(super) fn with_shared_state(
        run: LiveRunIdentity,
        seq: Arc<AtomicU64>,
        output_failed: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self { run, seq, output_failed,
            finished: Arc::new(std::sync::atomic::AtomicBool::new(false)) }
    }

    /// Whether this run already allocated its one terminal record.
    pub(super) fn is_finished(&self) -> bool { self.finished.load(Ordering::SeqCst) }

    /// Whether an output already failed.
    pub fn output_failed(&self) -> bool {
        self.output_failed.load(Ordering::SeqCst)
    }

    /// The next sequence number, without writing anything.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The last sequence number that was written.
    pub fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Build the envelope of one event.
    pub fn envelope(
        &self,
        event_time: DateTime<Utc>,
        market_id: Option<String>,
        token: Option<String>,
        accounting: &LiveAccounting,
        payload: LivePayload,
    ) -> Result<Value, String> {
        // serde_json maps non-finite floats to null. Reject on the core object
        // first, while an invalid Some(NaN) is still distinguishable from None.
        match &payload {
            LivePayload::Decision(payload) => check_decision_finite(&payload.decision)?,
            LivePayload::Payout(payout) if payout.payout_yes.is_some_and(|value| !value.is_finite()) =>
                return Err("PAYOUT_YES_INVALID".into()),
            _ => {}
        }
        let kind = payload.kind();
        let payload = serde_json::to_value(payload).map_err(|_| reason::OUTPUT_INVALID.to_string())?;
        let mut envelope = Map::new();
        envelope.insert("schema_version".into(), json!(LIVE_SCHEMA_VERSION));
        envelope.insert("run_id".into(), json!(self.run.run_id));
        envelope.insert("risk_window_id".into(), json!(self.run.risk_window_id));
        envelope.insert("event_seq".into(), json!(self.seq.load(Ordering::SeqCst)));
        envelope.insert("event_kind".into(), json!(kind.as_str()));
        envelope.insert(
            "event_time".into(),
            json!(event_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        envelope.insert("market_id".into(), json!(market_id));
        envelope.insert("token".into(), json!(token));
        envelope.insert("accounting".into(), accounting.to_json());
        envelope.insert("payload".into(), payload);
        Ok(Value::Object(envelope))
    }

    /// Check one envelope against the strict schema.
    ///
    /// The check runs before anything is written: a bad event is refused at
    /// the producer, not cleaned up at the reader.
    pub fn validate(envelope: &Value) -> Result<(), String> {
        types::validate_envelope(envelope)
    }

    /// Decode raw UTF-8 JSON without losing duplicate object keys before validation.
    pub fn validate_bytes(bytes: &[u8]) -> Result<Value, String> {
        types::decode_envelope(bytes)
    }

    /// Write one event line.
    ///
    /// The line is validated first, then written and flushed. A failed write
    /// latches the output failure: the run stops adding risk and may not
    /// present itself as complete.
    pub fn write(
        &self,
        event_time: DateTime<Utc>,
        market_id: Option<String>,
        token: Option<String>,
        accounting: &LiveAccounting,
        payload: LivePayload,
    ) -> Result<u64, String> {
        self.prepare(event_time, market_id, token, accounting, payload)?.flush()
    }

    /// Allocate and validate under State; flush only after releasing State.
    pub(super) fn prepare(
        &self,
        event_time: DateTime<Utc>,
        market_id: Option<String>,
        token: Option<String>,
        accounting: &LiveAccounting,
        payload: LivePayload,
    ) -> Result<PreparedLiveEvent, String> {
        if self.is_finished() { return Err(reason::RUN_STOPPED.into()); }
        if self.output_failed() { return Err(reason::OUTPUT_INVALID.into()); }
        let is_final = matches!(payload, LivePayload::Final(_));
        let result = (|| {
            let seq = self.next_seq();
            let mut envelope = self.envelope(event_time, market_id, token, accounting, payload)?;
            envelope["event_seq"] = json!(seq);
            Self::validate(&envelope)?;
            let line = serde_json::to_string(&envelope).map_err(|_| reason::OUTPUT_INVALID.to_string())?;
            if line.len() + LIVE_EVENT_PREFIX.len() + 1 > MAX_EVENT_LINE_BYTES {
                return Err(reason::OUTPUT_INVALID.into());
            }
            Ok(PreparedLiveEvent { seq, line: format!("{LIVE_EVENT_PREFIX}{line}\n"),
                output_failed: Arc::clone(&self.output_failed) })
        })();
        if result.is_err() { self.output_failed.store(true, Ordering::SeqCst); }
        if is_final { self.finished.store(true, Ordering::SeqCst); }
        result
    }
}

/// One immutable, prevalidated record owned by the operation holding the lane.
pub(super) struct PreparedLiveEvent {
    pub seq: u64,
    line: String,
    output_failed: Arc<std::sync::atomic::AtomicBool>,
}

impl PreparedLiveEvent {
    /// Write once outside the ledger lock. Any partial I/O poisons the run.
    pub fn flush(self) -> Result<u64, String> {
        use std::io::Write;
        let result = (|| -> std::io::Result<()> {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(self.line.as_bytes())?;
            stdout.flush()
        })();
        if result.is_err() {
            self.output_failed.store(true, Ordering::SeqCst);
            return Err(reason::OUTPUT_INVALID.into());
        }
        Ok(self.seq)
    }
}

#[cfg(test)]
mod tests;
