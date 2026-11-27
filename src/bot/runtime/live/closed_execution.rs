//! A degraded read may add no facts after authoritative Final was applied.
use super::*;

#[derive(Debug)]
pub(super) struct ClosedExecution {
    final_read: ExecutionSnapshot,
    degraded: Option<ExecutionSnapshot>,
}

impl ClosedExecution {
    pub(super) fn new(snapshot: &ExecutionSnapshot) -> Self {
        Self { final_read: snapshot.clone(), degraded: None }
    }

    pub(super) fn read_generation(&self) -> u64 {
        self.degraded.as_ref().unwrap_or(&self.final_read).generation
    }
}

fn unchanged_if_known<T: PartialEq>(current: &Field<T>, applied: &Field<T>) -> bool {
    current.value().is_none_or(|value| applied.value() == Some(value))
}

/// `None` leaves a full, unchanged Final on the original application path.
/// The exception never fabricates a complete observation or mutates the ledger.
pub(super) fn observe(
    state: &mut State,
    record: &AttemptRecord,
    snapshot: &ExecutionSnapshot,
    old: &CumulativeWatermark,
) -> Result<Option<AppliedExecution>, String> {
    let id = record.identity.local_attempt_id;
    let Some(proof) = state.live.closed_execution.get_mut(&id) else { return Ok(None) };
    let applied = &proof.final_read;
    let latest = proof.degraded.as_ref().unwrap_or(applied);
    if snapshot.generation < latest.generation
        || (snapshot.generation == latest.generation && snapshot != latest)
    {
        return Err("CUMULATIVE_FIELD_REVISED".into());
    }
    if snapshot == latest {
        let complete = proof.degraded.is_none();
        return Ok(Some(AppliedExecution {
            duplicate: true,
            closed_reread: true,
            accounting_complete: complete,
            net_cash_delta: complete.then_some(U256::ZERO),
            net_inventory_delta: complete.then_some(U256::ZERO),
            ..Default::default()
        }));
    }
    let q = snapshot.gross_q().ok_or("FACT_INCOMPLETE")?;
    if q > record.request_gross_atoms { return Err("OVERFILL".into()); }
    let next = advance_watermark(old, snapshot)?.ok_or("CUMULATIVE_FIELD_REVISED")?;
    if applied.identity != snapshot.identity
        || record.phase != AttemptPhase::Closed
        || record.submission != Submission::Accepted || snapshot.submission != Submission::Accepted
        || state.live.generation.get(&id) != Some(&applied.generation)
        || state.live.applied_q.get(&id) != Some(&q)
        || state.live.reservations.get(&id) != Some(&U256::ZERO)
        || next.fields != old.fields || next.fields.len() != 5
        || next.fields.values().any(|(coverage, _)| *coverage != q)
        || state.live.observed_risk_floor.get(&id).copied().unwrap_or_default() > old.risk_lower_bound()
        || !unchanged_if_known(&snapshot.accounting.vwap, &applied.accounting.vwap)
    {
        return Err("CUMULATIVE_FIELD_REVISED".into());
    }
    if snapshot.pages_complete && snapshot.accounting_complete()
        && snapshot.accounting_gross_q == Some(q)
        && snapshot.gross_q_before == q && snapshot.gross_q_after == q
        && matches!(snapshot.closure, Closure::Final { final_gross_q, .. } if final_gross_q == q)
    {
        return Ok(None);
    }
    // For this narrow case only the proof became unavailable; none of the
    // actual facts moved. A disagreement in quantity, even within one scan,
    // or an explicit reopening is not a degraded re-read of the same facts.
    if !matches!(snapshot.closure, Closure::Unproven)
        || (snapshot.pages_complete && snapshot.accounting_complete())
        || snapshot.quantity != applied.quantity
        || snapshot.gross_q_before != q || snapshot.gross_q_after != q
        || snapshot.accounting_gross_q.is_some_and(|coverage| coverage != q)
        || (snapshot.accounting_gross_q.is_none()
            && [&snapshot.accounting.gross_cash, &snapshot.accounting.collateral_fee,
                &snapshot.accounting.share_fee, &snapshot.accounting.net_cash,
                &snapshot.accounting.net_inventory, &snapshot.accounting.vwap]
                .iter().any(|field| field.is_known()))
        || snapshot.tx_hash.as_ref().is_some_and(|hash| applied.tx_hash.as_ref() != Some(hash))
    {
        return Err("CUMULATIVE_FIELD_REVISED".into());
    }
    proof.degraded = Some(snapshot.clone());
    state.live.watermarks.insert(id, next);
    Ok(Some(AppliedExecution { closed_reread: true, ..Default::default() }))
}
