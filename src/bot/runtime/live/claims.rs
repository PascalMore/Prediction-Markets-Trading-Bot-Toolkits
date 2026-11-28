//! Pure claim attribution. No balances are committed in this module.

use super::*;
use crate::service::venue::{BoundClaimEvidence, RuntimeClaimInventory};
use crate::service::predict_fun::account::runtime_claims::{capture_fill_evidence,
    capture_cash_evidence, capture_position_lag, inventory_mismatch, CashAnchor, CashProofError,
    CashTarget, FillCashEvidence, LagProofRequest, NativeSettlementMarket, PositionLag, LAG_BOOKED, LAG_OWNER, LAG_TARGET};

pub(in crate::bot::runtime) async fn read_runtime_batch(
    broker: &Broker, venue: &dyn crate::service::venue::OrderVenue, closed: &[RuntimeClaimInventory],
) -> (anyhow::Result<crate::service::venue::RuntimeAccountObservation>, Option<CashAnchor>) {
    let state = broker.state.clone();
    let trusted = {
        let state = state.lock();
        state.live.controlled.as_ref().and_then(|run| {
            // An original poll may carry both obligations. It may prove an
            // existing R22 episode, but cannot open one to replace an R3 retry.
            ((run.settlement_recheck.is_none() || run.r22_wait.is_some()) && !run.must_end()).then(|| {
                run.cash_anchor.clone().map(|anchor| (anchor, run.r22_wait.clone(), state.live.claims.keys().copied().collect()))
            }).flatten()
        })
    };
    let read = capture_cash_evidence(None, capture_position_lag(venue.runtime_account_observation(closed)));
    let (read, evidence) = if let Some((trusted, waiting, booked)) = trusted {
        let closed = closed.to_vec();
        let frozen_owner = waiting.as_ref().map(|wait| wait.proof.owner.clone());
        LAG_BOOKED.scope(booked, LAG_OWNER.scope(frozen_owner, LAG_TARGET.scope(Box::new(move |lag, owner, official, native, extra| {
            let state = state.lock();
            let run = state.live.controlled.as_ref().ok_or(CashProofError::Conflict("RUN_BINDING"))?;
            if run.cash_anchor.as_ref() != Some(&trusted)
                || (run.settlement_recheck.is_some() && (waiting.is_none() || run.r22_wait.is_none()))
                || closed_inventory(&state) != closed
            { return Err(CashProofError::Conflict("LAG_OWNER_OR_GENERATION")); }
            if extra {
                let id = owner.identity.local_attempt_id;
                if run.r22_wait.is_none() || !lag.double_zero || state.live.claims.contains_key(&id)
                    || owner.execution_generation != owner.validated_read_generation
                    || !state.live.watermarks.get(&id).is_some_and(|mark| mark.is_complete())
                    || !state.live.attempts.get(&id).is_some_and(|handle| handle.lock().submission == Submission::Accepted)
                    || !closed.contains(owner)
                { return Err(CashProofError::Conflict("INDEPENDENT_CLAIM_OWNER")); }
            } else {
                if bind_position_lag(&state, &closed, lag).as_ref() != Some(owner) {
                    return Err(CashProofError::Conflict("LAG_OWNER_OR_GENERATION"));
                }
                if waiting.as_ref().is_some_and(|wait| wait.proof.owner.identity != owner.identity) {
                    return Err(CashProofError::Conflict("EPISODE_OWNER"));
                }
            }
            let left = waiting.as_ref().map_or_else(|| trusted.0.clone(), |wait| wait.a.clone());
            lag_cash_target(&state, owner, official, native, left)
        }), read))).await
    } else { read.await };
    let anchor = match evidence { Some(Ok((anchor, None))) if read.is_ok() => Some(anchor), _ => None };
    (read, anchor)
}

fn lag_cash_target(state: &State, owner: &RuntimeClaimInventory,
    official: &crate::service::venue::TradableMarket, native: &NativeSettlementMarket,
    left: CashAnchor) -> Result<LagProofRequest, CashProofError> {
    use crate::bot::runtime::{live_accounting, paper};
    use CashProofError::{Conflict, Unavailable};
    let run = state.live.controlled.as_ref().ok_or(Conflict("RUN_BINDING"))?;
    let original = run.traded.get(&owner.identity.market_id).ok_or(Conflict("OFFICIAL_ROUND"))?;
    if native.id != original.id || native.id != official.id || native.neg_risk || native.yield_bearing
        || native.tokens != [original.yes_token.clone(), original.no_token.clone()]
        || native.tokens != [official.yes_token.clone(), official.no_token.clone()]
    { return Err(Conflict("OFFICIAL_ROUTE")); }
    let (identity, result, _, payout) = live_accounting::judge_official_round(original, official);
    let id = owner.identity.local_attempt_id;
    if result == paper::Settlement::Conflict || run.attempt_outcomes.get(&id).is_some_and(|outcome| {
        outcome.payout_yes.is_none_or(|paid| paper::revised_result(official,
            (outcome.result_prices.0, outcome.result_prices.1, paid), identity, result))
    }) { return Err(Conflict("OFFICIAL_REVISION")); }
    let payout = payout.filter(|_| result == paper::Settlement::Confirmed).ok_or(Unavailable("OFFICIAL_RESULT"))?;
    let condition = native.condition.filter(|condition| !condition.is_zero()).ok_or(Unavailable("OFFICIAL_CONDITION"))?;
    let token = owner.identity.token_id.parse::<U256>().map_err(|_| Conflict("OFFICIAL_TOKEN"))?;
    let rate = if owner.identity.token_id == official.yes_token { payout } else { 1.0 - payout };
    let quantity = owner.inventory_atoms;
    let payout = live_accounting::exact_floor_mul(quantity, rate).ok_or(Conflict("OFFICIAL_ATOMS"))?;
    let fills = run.applied_fill_cash.iter().filter(|(id, proof)| {
        state.live.generation.get(id) == Some(&proof.generation)
            && state.live.closed_execution.get(id).is_some_and(|read| read.read_generation() == proof.generation)
            && state.live.attempts.get(id).is_some_and(|attempt| attempt.lock().identity == proof.identity)
            && proof.identity.account == run.identity
    }).flat_map(|(_, proof)| proof.legs.iter().cloned()).collect();
    let sampled = run.phase1.clock.now().map_err(|_| Conflict("CLOCK"))?;
    let frozen = run.r22_wait.as_ref().map(|wait| (wait.proof.left.clone(), wait.proof.cash.payout.clone()));
    Ok((left, CashTarget { condition, token, quantity, payout }, fills, sampled, frozen))
}

pub(super) fn applied_deltas(state: &State) -> anyhow::Result<Vec<(String, Side, U256, U256)>> {
    let mut changes = Vec::new();
    for (id, handle) in &state.live.attempts {
        let record = handle.lock();
        if let (Some(cash), Some(inventory)) = (state.live.net_cash.get(id), state.live.net_inventory.get(id)) {
            changes.push((record.identity.token_id.clone(), record.identity.side, *cash, *inventory));
        } else if record.phase != AttemptPhase::Closed {
            anyhow::bail!("RECONCILIATION_REQUIRED:LOCAL_ACCOUNTING_UNPROVEN");
        }
    }
    Ok(changes)
}

/// Return only a frozen read identity, not an episode, receipt or permit.
pub(in crate::bot::runtime) fn bind_position_lag(
    state: &State, closed: &[RuntimeClaimInventory], lag: &PositionLag,
) -> Option<RuntimeClaimInventory> {
    let run = state.live.controlled.as_ref()?;
    let anchor = lag.anchor.as_ref().ok()?;
    let mut owners = closed.iter().filter(|item| item.identity.token_id == lag.token);
    let owner = owners.next()?;
    let id = owner.identity.local_attempt_id;
    let evidence = run.applied_fill_cash.get(&id)?;
    let record = state.live.attempts.get(&id)?.lock();
    let round = run.traded.get(&owner.identity.market_id)?;
    let now = run.phase1.clock.now().ok()?;
    (owners.next().is_none() && owner.inventory_atoms == lag.quantity
        && owner.identity == record.identity && owner.identity.account == run.identity
        && anchor.identity == run.identity && evidence.identity == owner.identity
        && evidence.generation == owner.execution_generation
        && evidence.generation == owner.validated_read_generation
        && owner.identity.signed_hash.as_ref().is_some_and(|hash| !hash.is_empty())
        && owner.identity.venue_order_id.as_ref().is_some_and(|order| !order.is_empty())
        && record.submission == Submission::Accepted && record.phase == AttemptPhase::Closed
        && state.live.watermarks.get(&id).is_some_and(|watermark| watermark.is_complete())
        && state.live.reservations.values().all(U256::is_zero)
        && !state.live.claims.contains_key(&id) && !run.must_end() && !run.loss_latched
        && (round.yes_token == lag.token || round.no_token == lag.token)
        && round.ends_at.is_some_and(|end| end <= now.utc)).then(|| owner.clone())
}

use crate::service::observation::CheckTime;
use crate::service::predict_fun::account::runtime_claims::LagCashProof;

#[derive(Debug, Clone)]
pub(in crate::bot::runtime) struct R22Wait {
    pub a: CashAnchor,
    pub proof: LagCashProof,
    pub last: CheckTime,
    pub slots: u8,
}

impl R22Wait {
    pub fn remaining(&self, now: CheckTime) -> Option<std::time::Duration> {
        if now.utc < self.last.utc || now.elapsed_ms < self.last.elapsed_ms { return None; }
        let start = self.proof.sampled;
        let utc = start.utc.checked_add_signed(chrono::Duration::seconds(120))?;
        let elapsed = start.elapsed_ms.checked_add(120_000)?;
        let wall = (utc - now.utc).to_std().ok()?;
        let mono = std::time::Duration::from_millis(elapsed.checked_sub(now.elapsed_ms)?);
        let budget = wall.min(mono);
        (!budget.is_zero()).then_some(budget)
    }
}

type ResolvedBatch = (crate::service::venue::RuntimeAccountObservation, Option<CashAnchor>, Option<Vec<crate::service::venue::TradableMarket>>);

pub(super) fn resolve_batch(
    broker: &Broker, venue: &dyn crate::service::venue::OrderVenue, closed: &[RuntimeClaimInventory],
    version: &(u64, std::collections::HashMap<u64, u64>), read: anyhow::Result<crate::service::venue::RuntimeAccountObservation>,
) -> anyhow::Result<Option<ResolvedBatch>> {
    let lag = match read {
        Err(error) => match error.downcast::<PositionLag>() {
            // R3 cannot open a new R22 episode in lieu of its own receipt.
            // An existing episode still distinguishes conflict from missing proof.
            Ok(_) if super::super::live_control::RECHECK_ACCOUNT_CUTOFF.try_with(|_| ()).is_ok()
                && !broker.state.lock().live.controlled.as_ref()
                    .is_some_and(|run| run.r22_wait.is_some()) => return Ok(None),
            Ok(lag) => lag,
            Err(error) => return Err(error),
        },
        ok => return if broker.state.lock().live.controlled.as_ref().is_some_and(|run| run.r22_wait.is_some()) {
            Err(inventory_mismatch().into())
        } else { ok.map(|observation| Some((observation, None, None))) },
    };
    let mut state = broker.state.lock();
    if state.live.next_attempt_id != version.0 || state.live.generation != version.1
        || closed_inventory(&state) != closed {
        anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_READ_RACE");
    }
    super::super::live_control::check_r22_boundary(&mut state)?;
    if state.halted || state.entry_stop_reason.is_some() { return Err(inventory_mismatch().into()); }
    let mut closure = None;
    venue.with_runtime_claim_read_fence(closed, &mut || {
        let owner = bind_position_lag(&state, closed, &lag).ok_or_else(inventory_mismatch)?;
        let run = state.live.controlled.as_ref().unwrap();
        let anchor = lag.anchor.as_ref().map_err(|_| inventory_mismatch())?;
        let waiting = run.r22_wait.as_ref();
        // A pre-existing cash wait for another attempt cannot be reconciled by
        // this R22 batch, including when the batch has not finished its proof.
        let other = |id: u64| id != owner.identity.local_attempt_id;
        if run.cash_wait.as_ref().is_some_and(|wait| {
            wait.pending.iter().copied().any(other) || run.attempt_outcomes.iter().any(|(&id, outcome)| {
                other(id) && !outcome.cash_confirmed
                    && outcome.claim_value_atoms.is_some_and(|atoms| !atoms.is_zero())
            })
        }) { return Err(inventory_mismatch().into()); }
        if waiting.is_some_and(|wait| wait.proof.owner.identity != owner.identity) {
            return Err(inventory_mismatch().into());
        }
        let (proof, official) = match lag.proof.as_ref() {
            Some(Ok(proven)) => (&proven.0, &proven.1),
            // Admission still requires a full proof. For an existing episode the
            // account consumer owns the original read streak; this is not a receipt.
            Some(Err(CashProofError::Read(error))) if waiting.is_some() => return Err(error.clone().into()),
            Some(Err(CashProofError::Unavailable(_))) if waiting.is_some() && run.settlement_recheck.is_none() => return Ok(()),
            _ => return Err(inventory_mismatch().into()),
        };
        let paid = proof.cash.payout.as_ref().map_or(U256::ZERO, |leg| leg.atoms);
        if proof.owner != owner || proof.target.quantity != lag.quantity
            || (paid != U256::ZERO && paid != proof.target.payout) {
            return Err(inventory_mismatch().into());
        }
        if let Some(wait) = waiting {
            if wait.proof.target != proof.target || wait.a != proof.left {
                return Err(inventory_mismatch().into());
            }
            let early = wait.proof.cash.payout.as_ref().map_or(U256::ZERO, |leg| leg.atoms);
            let total = early.checked_add(paid).filter(|total| *total <= proof.target.payout).ok_or_else(inventory_mismatch)?;
            if !lag.double_zero {
                if !paid.is_zero() { return Err(inventory_mismatch().into()); }
                verify_lag_residual(&state, &lag, early)?;
                return Ok(());
            }
            let (_, fresh) = runtime_payouts(&state, &lag.observation.bound_claims, &lag.observation.snapshot.settled_zero)?;
            let independent = fresh.iter().filter(|(id, _, _)| *id != owner.identity.local_attempt_id);
            if independent.clone().count() != proof.cash.claims.len() || independent.into_iter().any(|(id, _, cash)| {
                !proof.cash.claims.get(id).is_some_and(|leg| leg.incoming && leg.atoms == *cash)
            }) { return Err(inventory_mismatch().into()); }
            if total != proof.target.payout { return Ok(()); }
            let id = owner.identity.local_attempt_id;
            if run.attempt_outcomes.get(&id).is_some_and(|outcome| outcome.position_net_atoms != lag.quantity
                || outcome.claim_value_atoms != Some(proof.target.payout)) { return Err(inventory_mismatch().into()); }
            let matched = if total.is_zero() {
                lag.observation.snapshot.settled_zero.iter().any(|item| item.token == lag.token)
            } else {
                lag.observation.bound_claims.iter().any(|claim| claim.identity == owner.identity && claim.claim_atoms == total)
            };
            if !matched { return Err(inventory_mismatch().into()); }
            closure = Some((anchor.clone(), official.clone()));
        } else {
            if lag.double_zero || run.cash_anchor.as_ref().map(|(anchor, _)| anchor) != Some(&proof.left) {
                return Err(inventory_mismatch().into());
            }
            verify_lag_residual(&state, &lag, paid)?;
            let run = state.live.controlled.as_mut().unwrap();
            let now = run.phase1.clock.now()?;
            let wait = R22Wait { a: anchor.clone(), proof: proof.clone(), last: proof.sampled, slots: 0 };
            if wait.remaining(now).is_none() { return Err(inventory_mismatch().into()); }
            run.r22_wait = Some(R22Wait { last: now, ..wait });
            run.account_read = None;
            run.pending_candidate = None;
        }
        Ok(())
    })?;
    Ok(closure.map(|(anchor, official)| (lag.observation, Some(anchor), Some(official))))
}

pub(super) fn commit_r22_official(state: &mut State, official: &crate::service::venue::TradableMarket)
    -> anyhow::Result<Vec<super::super::live_events::PreparedLiveEvent>> {
    let events = super::super::live_accounting::apply_official_result(state, official, false, false)
        .map_err(anyhow::Error::msg)?.ok_or_else(inventory_mismatch)?;
    super::super::live_control::check_r22_boundary(state)?;
    Ok(events)
}

/// The observed REST position is still present; back out only the separately proved
/// target payment and require the unchanged baseline equation for the whole batch.
fn verify_lag_residual(state: &State, lag: &PositionLag, paid: U256) -> anyhow::Result<()> {
    let observed = &lag.observation;
    let mut snapshot = observed.snapshot.clone();
    if !observed.bound_claims.is_empty() {
        // A new non-target claim needs its own native cash-leg attribution.
        return Err(inventory_mismatch().into());
    }
    snapshot.collateral_atoms = snapshot.collateral_atoms.and_then(|cash| cash.checked_sub(paid));
    if snapshot.collateral_atoms.is_none() { return Err(inventory_mismatch().into()); }
    let baseline = state.live.baseline.as_ref().ok_or_else(inventory_mismatch)?;
    let (claims, _) = runtime_payouts(state, &observed.bound_claims, &snapshot.settled_zero)?;
    check_account_delta_with_claims(baseline, &snapshot, None, applied_deltas(state)?, claims)
}

pub(super) async fn read_sync_execution(
    broker: &Broker, read: impl std::future::Future<Output = anyhow::Result<ExecutionSnapshot>>,
) -> (anyhow::Result<ExecutionSnapshot>, Option<FillCashEvidence>) {
    let controlled = broker.state.lock().live.controlled.is_some();
    if controlled { capture_fill_evidence(read).await } else { (read.await, None) }
}

pub(super) fn remember_applied_fill(state: &mut State, snapshot: &ExecutionSnapshot,
    evidence: Option<FillCashEvidence>) {
    let id = snapshot.identity.local_attempt_id;
    let Some(run) = state.live.controlled.as_mut() else { return; };
    run.applied_fill_cash.remove(&id);
    if let Some(evidence) = evidence.filter(|proof| proof.identity == snapshot.identity
        && proof.generation == snapshot.generation
        && state.live.generation.get(&id) == Some(&proof.generation)
        && state.live.closed_execution.get(&id).map(|closed| closed.read_generation()) == Some(proof.generation)
        && state.live.net_cash.get(&id) == snapshot.accounting.net_cash.value()
        && state.live.net_inventory.get(&id) == snapshot.accounting.net_inventory.value()
        && state.live.reservations.get(&id) == Some(&U256::ZERO)) {
        run.applied_fill_cash.insert(id, evidence);
    }
}

/// Inventory named only by the current closed, completely applied generation.
pub(in crate::bot::runtime) fn closed_inventory(state: &State) -> Vec<RuntimeClaimInventory> {
    state.live.attempts.iter().filter_map(|(id, handle)| {
        let record = handle.lock();
        let inventory = *state.live.net_inventory.get(id)?;
        let generation = *state.live.generation.get(id)?;
        (record.phase == AttemptPhase::Closed && !inventory.is_zero()
            && state.live.net_cash.contains_key(id)
            && state.live.reservations.get(id).copied().unwrap_or_default().is_zero())
            .then(|| RuntimeClaimInventory {
                identity: record.identity.clone(), execution_generation: generation,
                validated_read_generation: state.live.closed_execution.get(id)
                    .map_or(generation, |execution| execution.read_generation()),
                inventory_atoms: inventory,
            })
    }).collect()
}

/// Validate all owners before the caller commits any cash, including duplicates.
pub(super) fn runtime_payouts(
    state: &State, evidence: &[BoundClaimEvidence], zero: &[crate::service::venue::SettledZero],
) -> anyhow::Result<(Vec<(String, U256, U256)>, Vec<(u64, U256, U256)>)> {
    let closed = closed_inventory(state);
    let mut claims = Vec::new();
    let mut fresh = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (id, (inventory, cash)) in &state.live.claims {
        let record = state.live.attempts.get(id)
            .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:CASH_CONFLICT"))?.lock();
        claims.push((record.identity.token_id.clone(), *inventory, *cash));
    }
    for claim in evidence {
        let id = claim.identity.local_attempt_id;
        let owners: Vec<_> = closed.iter().filter(|owner|
            owner.identity.token_id == claim.identity.token_id).collect();
        let [owner] = owners.as_slice() else {
            anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_OWNER");
        };
        let expected = alloy_primitives::U512::from(claim.inventory_atoms)
            .checked_mul(alloy_primitives::U512::from(claim.payout_millionths))
            .map(|value| value / alloy_primitives::U512::from(1_000_000u64));
        if !seen.insert(id) || owner.identity != claim.identity
            || owner.execution_generation != claim.execution_generation
            || owner.inventory_atoms != claim.inventory_atoms
            || ![500_000, 1_000_000].contains(&claim.payout_millionths)
            || expected != Some(alloy_primitives::U512::from(claim.claim_atoms))
            || claim.claim_atoms.is_zero() || claim.yes_token == claim.no_token
            || ![&claim.yes_token, &claim.no_token].contains(&&claim.identity.token_id)
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:CASH_CONFLICT");
        }
        let run = state.live.controlled.as_ref()
            .ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let market = run.traded.get(&claim.identity.market_id)
            .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:CLAIM_IDENTITY"))?;
        if market.id != claim.identity.market_id || market.yes_token != claim.yes_token
            || market.no_token != claim.no_token || run.identity != claim.identity.account
        {
            anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_IDENTITY");
        }
        if let Some(outcome) = run.attempt_outcomes.get(&id) {
            if outcome.position_net_atoms != claim.inventory_atoms
                || outcome.claim_value_atoms != Some(claim.claim_atoms)
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:CASH_CONFLICT");
            }
        }
        if let Some((inventory, cash)) = state.live.claims.get(&id) {
            if (*inventory, *cash) != (claim.inventory_atoms, claim.claim_atoms) {
                anyhow::bail!("RECONCILIATION_REQUIRED:CASH_CONFLICT");
            }
        } else {
            claims.push((claim.identity.token_id.clone(), claim.inventory_atoms, claim.claim_atoms));
            fresh.push((id, claim.inventory_atoms, claim.claim_atoms));
        }
    }
    for settled in zero {
        let owners: Vec<_> = closed.iter().filter(|owner| owner.identity.token_id == settled.token).collect();
        if owners.is_empty() && state.live.baseline.as_ref()
            .is_some_and(|baseline| baseline.settled_zero.contains(settled)) { continue; }
        let [owner] = owners.as_slice() else {
            anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_OWNER");
        };
        let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let market = run.traded.get(&owner.identity.market_id)
            .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:CLAIM_IDENTITY"))?;
        if !seen.insert(owner.identity.local_attempt_id) || settled.market_id != market.id
            || ![&market.yes_token, &market.no_token].contains(&&settled.token)
            || settled.payout_millionths != 0 || settled.oracle_status != "RESOLVED"
            || settled.trading_status != "CLOSED" || settled.outcome_status != "LOST"
            || run.attempt_outcomes.get(&owner.identity.local_attempt_id)
                .is_some_and(|outcome| outcome.claim_value_atoms != Some(U256::ZERO))
        { anyhow::bail!("RECONCILIATION_REQUIRED:CASH_CONFLICT"); }
        // Z consumes only the proven closed inventory. It adds neither a cash
        // claim nor a fresh credit: the existing result owns the realized loss.
        claims.push((settled.token.clone(), owner.inventory_atoms, U256::ZERO));
    }
    Ok((claims, fresh))
}

/// Exact account equality over trade-only deltas and the full claim set.
pub(crate) fn check_account_delta_with_claims(
    initial: &crate::service::venue::AccountSnapshot,
    current: &crate::service::venue::AccountSnapshot,
    own_order: Option<&str>,
    changes: impl IntoIterator<Item = (String, Side, U256, U256)>,
    claims: impl IntoIterator<Item = (String, U256, U256)>,
) -> anyhow::Result<()> {
    crate::service::predict_fun::account::check_snapshot_complete(current)?;
    if initial.identity != current.identity { anyhow::bail!("ACCOUNT_MISMATCH"); }
    if current.open_orders.iter().any(|order| Some(order.as_str()) != own_order) {
        anyhow::bail!("RECONCILIATION_REQUIRED:OPEN_ORDERS");
    }
    use alloy_primitives::U512;
    let mut incoming = U512::ZERO;
    let mut outgoing = U512::ZERO;
    let mut inventory: HashMap<String, (U512, U512)> = HashMap::new();
    for (token, side, cash, shares) in changes {
        let (added, removed) = inventory.entry(token).or_default();
        let (cash_total, share_total) = match side {
            Side::Buy => (&mut outgoing, added), Side::Sell => (&mut incoming, removed),
        };
        *cash_total = cash_total.checked_add(U512::from(cash))
            .ok_or_else(|| anyhow::anyhow!("CASH_OVERFLOW"))?;
        *share_total = share_total.checked_add(U512::from(shares))
            .ok_or_else(|| anyhow::anyhow!("INVENTORY_OVERFLOW"))?;
    }
    let start = U512::from(initial.collateral_atoms
        .ok_or_else(|| anyhow::anyhow!("MISSING_COLLATERAL"))?);
    let mut positions: HashMap<_, _> = initial.positions.iter().cloned().collect();
    if positions.len() != initial.positions.len() {
        anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_LEDGER_MISMATCH");
    }
    for (token, (added, removed)) in inventory {
        let held = positions.entry(token).or_default();
        let amount = U512::from(*held).checked_add(added)
            .and_then(|value| value.checked_sub(removed))
            .ok_or_else(|| anyhow::anyhow!("INVENTORY_CONFLICT"))?;
        *held = U256::from_str_radix(&amount.to_string(), 10)
            .map_err(|_| anyhow::anyhow!("INVENTORY_OVERFLOW"))?;
    }
    let mut claim_cash = U512::ZERO;
    for (token, shares, cash) in claims {
        let Some(held) = positions.get_mut(&token) else { continue; };
        if *held < shares { anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_LEDGER_MISMATCH"); }
        *held -= shares;
        claim_cash = claim_cash.checked_add(U512::from(cash))
            .ok_or_else(|| anyhow::anyhow!("CASH_OVERFLOW"))?;
    }
    let cash = start.checked_add(incoming).and_then(|value| value.checked_sub(outgoing))
        .and_then(|value| value.checked_add(claim_cash))
        .ok_or_else(|| anyhow::anyhow!("CASH_CONFLICT"))?;
    positions.retain(|_, value| !value.is_zero());
    let observed: HashMap<_, _> = current.positions.iter()
        .filter(|(_, value)| !value.is_zero()).cloned().collect();
    if current.positions.iter().map(|(token, _)| token).collect::<std::collections::HashSet<_>>().len()
        != current.positions.len()
        || current.collateral_atoms.map(U512::from) != Some(cash) || observed != positions
    {
        anyhow::bail!("RECONCILIATION_REQUIRED:ACCOUNT_LEDGER_MISMATCH");
    }
    Ok(())
}
