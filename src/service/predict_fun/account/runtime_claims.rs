//! One account observation, with evidence referring only to sync-consumed generations.

use super::*;
use crate::service::venue::{BoundClaimEvidence, RuntimeAccountObservation, RuntimeClaimInventory};

impl PredictFunTrader {
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
            let proofs: Vec<_> = positions.iter().map(|(token, amount)| async {
                let market = self.client.market_for_token(token)?;
                chain.position(&market, token, *amount).await?;
                Ok::<(), anyhow::Error>(())
            }).collect();
            for proof in futures_util::future::join_all(proofs).await { proof?; }
            let mut bound_claims = Vec::new();
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
                    || self.generations.read().get(hash).copied() != Some(inventory.execution_generation)
                {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_GENERATION");
                }
                // REST can hide a losing outcome without burning its ERC-1155 token.
                // Read once at the original pinned block; only exact or zero can be explained.
                let chain_inventory = chain.position_balance(&bound.market, &inventory.identity.token_id).await?;
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
                    || self.generations.read().get(hash).copied() != Some(inventory.execution_generation)
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
            for inventory in closed.unwrap_or_default() {
                let hash = inventory.identity.signed_hash.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("RECONCILIATION_REQUIRED:UNBOUND_ORDER"))?;
                if self.generations.read().get(hash).copied() != Some(inventory.execution_generation) {
                    anyhow::bail!("RECONCILIATION_REQUIRED:CLAIM_GENERATION");
                }
            }
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
            if closed.is_none() {
                self.stage_history(&observation.snapshot, &history, explanation.official)?;
            }
            Ok(observation)
        }.await.map_err(|error| {
            // Auxiliary official-settlement reads belong to this account
            // observation too; preserve typed causes before the public boundary.
            super::super::request::account_read_error(error, "RUNTIME_ACCOUNT")
        });
        match &result {
            Ok(_) => self.gate.record_read_success(&identity, "snapshot"),
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
