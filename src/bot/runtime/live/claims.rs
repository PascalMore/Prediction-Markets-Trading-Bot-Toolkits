//! Pure claim attribution. No balances are committed in this module.

use super::*;
use crate::service::venue::{BoundClaimEvidence, RuntimeClaimInventory};

/// Inventory named only by the current closed, completely applied generation.
pub(super) fn closed_inventory(state: &State) -> Vec<RuntimeClaimInventory> {
    state.live.attempts.iter().filter_map(|(id, handle)| {
        let record = handle.lock();
        let inventory = *state.live.net_inventory.get(id)?;
        let generation = *state.live.generation.get(id)?;
        (record.phase == AttemptPhase::Closed && !inventory.is_zero()
            && state.live.net_cash.contains_key(id)
            && state.live.reservations.get(id).copied().unwrap_or_default().is_zero())
            .then(|| RuntimeClaimInventory {
                identity: record.identity.clone(), execution_generation: generation,
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
