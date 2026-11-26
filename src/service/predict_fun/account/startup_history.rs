//! Historical order contributions and the startup-only cleared explanation.
use super::*;
use crate::models::Side;
use crate::service::predict_fun::{decimal_to_wei_exact, ApiMatch, ApiOrderData};
use crate::service::venue::{AttemptRecord, OrderIdentity, Submission};

/// Instance-local only. Nothing here is serialized or transferred to another trader.
#[derive(Default)]
pub(in crate::service::predict_fun) struct StartupHistory {
    scope: Option<HistoryScope>,
    pending: Option<ClearedHistory>,
    committed: Option<ClearedHistory>,
    conflict: bool,
}
#[derive(Clone, PartialEq, Eq)]
struct HistoryScope {
    run: String,
    window: String,
    account: AccountIdentity,
    deployment: (u64, &'static str, [Address; 4]),
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MarketBinding {
    id: u64,
    pair: Vec<String>,
    condition: Option<B256>,
    neg: bool,
    yield_bearing: bool,
    exchange: Address,
    assets: (Address, Address, Option<Address>),
}
impl MarketBinding {
    // Original official market responses can omit the condition already proved
    // by matches/receipts. An absent optional value is not a revised identity.
    fn matches_known(&self, proved: &Self) -> bool {
        self.id == proved.id
            && self.pair == proved.pair
            && self
                .condition
                .is_none_or(|condition| Some(condition) == proved.condition)
            && self.neg == proved.neg
            && self.yield_bearing == proved.yield_bearing
            && self.exchange == proved.exchange
            && self.assets == proved.assets
    }
    fn new(trader: &PredictFunTrader, market: &ApiMarket) -> Self {
        Self {
            id: market.id,
            pair: market
                .outcomes
                .iter()
                .map(|o| o.on_chain_id.clone())
                .collect(),
            condition: market.condition_id,
            neg: market.is_neg_risk,
            yield_bearing: market.is_yield_bearing,
            exchange: trader
                .client
                .deployment
                .exchange(market.is_neg_risk, market.is_yield_bearing),
            assets: assets(
                trader.identity().chain_id,
                market.is_neg_risk,
                market.is_yield_bearing,
            ),
        }
    }
}
/// Account-owned leg only; exact numbers and absent fees stay distinct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EconomicLeg {
    market: MarketBinding,
    hash: String,
    signer: String,
    token: String,
    quote: String,
    amount: U256,
    price: U256,
    fee: Option<(String, U256)>,
    transaction: Option<String>,
    executed: String,
    maker: bool,
}
#[derive(Default)]
pub(super) struct HistoryRead {
    pub(super) totals: HashMap<String, U256>,
    legs: HashMap<String, Vec<EconomicLeg>>,
}
#[derive(Clone)]
struct ClearedToken {
    amount: U256,
    binding: MarketBinding,
    official: OfficialSettlement,
}
#[derive(Clone)]
struct ClearedHistory {
    snapshot: AccountSnapshot,
    tokens: HashMap<String, ClearedToken>,
    orders: HashMap<String, (ProvenHistoryOrder, Vec<EconomicLeg>)>,
}
fn history_conflict() -> anyhow::Error {
    VenueError::new(
        VenueErrorCode::ReconciliationRequired,
        "startup_history",
        "STARTUP_HISTORY_CONFLICT",
    )
    .into()
}
impl PredictFunTrader {
    fn history_scope(&self, run: &str, window: &str) -> HistoryScope {
        let d = self.client.deployment;
        HistoryScope {
            run: run.into(),
            window: window.into(),
            account: self.identity(),
            deployment: (
                d.chain_id,
                d.api_base,
                [
                    d.ctf_exchange,
                    d.neg_risk_exchange,
                    d.yield_ctf_exchange,
                    d.yield_neg_risk_exchange,
                ],
            ),
        }
    }
    pub(in crate::service::predict_fun) fn begin_history(
        &self,
        run: &str,
        window: &str,
    ) -> anyhow::Result<()> {
        if run.is_empty() || window.is_empty() {
            return Err(VenueError::new(
                VenueErrorCode::FactIncomplete,
                "startup_history",
                "STARTUP_SCOPE",
            )
            .into());
        }
        *self.startup_history.write() = StartupHistory {
            scope: Some(self.history_scope(run, window)),
            ..Default::default()
        };
        self.history_proven.write().clear();
        Ok(())
    }
    pub(in crate::service::predict_fun) fn commit_history(
        &self,
        run: &str,
        window: &str,
        snapshot: &AccountSnapshot,
    ) -> anyhow::Result<()> {
        let scope = self.history_scope(run, window);
        let mut state = self.startup_history.write();
        if state.conflict {
            return Err(history_conflict());
        }
        if state.scope.as_ref() != Some(&scope) {
            return Err(mismatch("STARTUP_SCOPE").into());
        }
        if let Some(committed) = &state.committed {
            return if committed.snapshot == *snapshot {
                Ok(())
            } else {
                Err(history_conflict())
            };
        }
        let pending = state.pending.as_ref().ok_or_else(|| {
            VenueError::new(
                VenueErrorCode::FactIncomplete,
                "startup_history",
                "STARTUP_AUDIT",
            )
        })?;
        if pending.snapshot != *snapshot || !snapshot.complete || !snapshot.positions_explained {
            return Err(VenueError::new(
                VenueErrorCode::ReconciliationRequired,
                "startup_history",
                "STARTUP_INPUT",
            )
            .into());
        }
        state.committed = state.pending.take();
        Ok(())
    }
    pub(super) fn check_history_scope(&self) -> anyhow::Result<()> {
        let mut state = self.startup_history.write();
        if state.conflict {
            return Err(history_conflict());
        }
        if let Some(scope) = &state.scope {
            if *scope != self.history_scope(&scope.run, &scope.window) {
                state.conflict = true;
                return Err(mismatch("STARTUP_SCOPE").into());
            }
        }
        Ok(())
    }
    /// Check before remember_market can overwrite the binding we proved.
    pub(super) fn check_history_market(&self, market: &ApiMarket) -> anyhow::Result<()> {
        self.check_history_scope()?;
        let binding = MarketBinding::new(self, market);
        let mut state = self.startup_history.write();
        let Some(committed) = &state.committed else {
            return Ok(());
        };
        for (id, token) in &committed.tokens {
            let known = self.client.market_for_token(id)?;
            let incoming = (binding.id == token.binding.id
                || binding.pair.iter().any(|t| token.binding.pair.contains(t)))
            .then_some(market);
            // A slim fresh row cannot erase an already observed contradictory
            // binding/result installed by another original market consumer.
            for (index, market) in std::iter::once(&known).chain(incoming).enumerate() {
                let binding = MarketBinding::new(self, market);
                // Slim API matches do not contain round prices; missing optional
                // official fields aren't revisions. Explicit contradictory statuses are.
                let result_conflict = (!market.status.is_empty()
                    && market.status != "RESOLVED"
                    && market.status != "REGISTERED")
                    || market.outcomes.iter().any(|o| {
                        token.binding.pair.first() == Some(&o.on_chain_id)
                            && o.status.as_ref().is_some_and(|s| {
                                token.official.facts.yes_status.as_ref() != Some(s)
                            })
                    })
                    || market.outcomes.iter().any(|o| {
                        token.binding.pair.get(1) == Some(&o.on_chain_id)
                            && o.status
                                .as_ref()
                                .is_some_and(|s| token.official.facts.no_status.as_ref() != Some(s))
                    });
                let same_binding = if index == 0 {
                    binding.matches_known(&token.binding)
                } else {
                    binding == token.binding
                };
                if !same_binding || result_conflict {
                    state.conflict = true;
                    return Err(history_conflict());
                }
            }
        }
        Ok(())
    }
    fn check_history_legs(&self, legs: &HashMap<String, Vec<EconomicLeg>>) -> anyhow::Result<()> {
        let mut state = self.startup_history.write();
        if state.conflict {
            return Err(history_conflict());
        }
        if let Some(committed) = &state.committed {
            if committed
                .orders
                .iter()
                .any(|(hash, (_, old))| legs.get(hash).cloned().unwrap_or_default() != *old)
            {
                state.conflict = true;
                return Err(history_conflict());
            }
        }
        Ok(())
    }
    pub(super) fn stage_history(
        &self,
        snapshot: &AccountSnapshot,
        history: &HistoryRead,
        official: HashMap<String, OfficialSettlement>,
    ) -> anyhow::Result<()> {
        self.check_history_scope()?;
        let mut state = self.startup_history.write();
        if state.scope.is_none() || state.committed.is_some() {
            return Ok(());
        }
        state.pending = None;
        if !snapshot.complete || !snapshot.positions_explained {
            return Ok(());
        }
        let proven = self.history_proven.read();
        let mut tokens = HashMap::new();
        let mut orders = HashMap::new();
        for (token, official) in official {
            if !official.baseline_eligible() {
                continue;
            }
            let amount = history.totals[&token];
            let contributions: Vec<_> = proven.iter().filter(|(_, p)| p.token == token).collect();
            // The binding comes from the proved account legs, not a potentially
            // slimmer official market response that just refreshed the cache.
            let Some(binding) = contributions.iter().find_map(|(hash, _)| {
                history
                    .legs
                    .get(*hash)
                    .and_then(|l| l.first())
                    .map(|l| l.market.clone())
            }) else {
                continue;
            };
            for (hash, order) in contributions {
                orders.insert(
                    hash.clone(),
                    (
                        order.clone(),
                        history.legs.get(hash).cloned().unwrap_or_default(),
                    ),
                );
            }
            tokens.insert(
                token,
                ClearedToken {
                    amount,
                    binding,
                    official,
                },
            );
        }
        state.pending = Some(ClearedHistory {
            snapshot: snapshot.clone(),
            tokens,
            orders,
        });
        Ok(())
    }
    pub(super) fn discard_pending_history(&self) {
        self.startup_history.write().pending = None;
    }
    fn cleared_history(
        &self,
        history: &HashMap<String, U256>,
        observed: &HashMap<String, U256>,
    ) -> anyhow::Result<HashMap<String, ClearedToken>> {
        self.check_history_scope()?;
        let tokens = self
            .startup_history
            .read()
            .committed
            .as_ref()
            .map(|c| c.tokens.clone())
            .unwrap_or_default();
        for (token, basis) in &tokens {
            // Verify current bindings without accepting a new overwritten cache entry.
            self.check_history_market(&self.client.market_for_token(token)?)?;
            if history.get(token) != Some(&basis.amount) || observed.contains_key(token) {
                self.startup_history.write().conflict = true;
                return Err(history_conflict());
            }
        }
        Ok(tokens)
    }
}

/// Evidence is retained even when exact cash has not yet explained a claim.
#[derive(Debug, Default)]
pub(super) struct ExplainedPositions {
    pub(super) settled_zero: Vec<SettledZero>,
    pub(super) claimed_win: Vec<ClaimedWin>,
    pub(super) explained: bool,
    pub(super) official: HashMap<String, OfficialSettlement>,
}

/// One confirmed official result, bound by the original strict reader.
#[derive(Debug, Clone)]
pub(super) struct OfficialSettlement {
    pub(super) market_id: u64,
    oracle_status: String,
    trading_status: String,
    outcome_status: String,
    pub(super) payout: f64,
    facts: crate::service::venue::RoundFacts,
    prices: (f64, f64),
    variant: String,
    feed_symbol: Option<String>,
    window: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
}
impl OfficialSettlement {
    // Additional reuse eligibility, not an admission rule for the original reader.
    fn baseline_eligible(&self) -> bool {
        self.variant == "CRYPTO_UP_DOWN"
            && self.feed_symbol.as_deref() == Some("BTCUSDT")
            && self.facts.price_feed_provider.as_deref() == Some("CHAINLINK")
            && self
                .window
                .is_some_and(|(start, end)| end - start == chrono::Duration::seconds(300))
    }
    pub(super) fn settled_zero(&self, token: &str) -> SettledZero {
        SettledZero {
            token: token.to_string(),
            market_id: self.market_id,
            oracle_status: self.oracle_status.clone(),
            trading_status: self.trading_status.clone(),
            outcome_status: self.outcome_status.clone(),
            payout_millionths: 0,
        }
    }
    pub(super) fn claimed_win(&self, token: &str, inventory: U256) -> Option<ClaimedWin> {
        if inventory.is_zero() || !(self.payout > 0.0) {
            return None;
        }
        let millionths = (self.payout * 1_000_000.0).round();
        if millionths != 500_000.0 && millionths != 1_000_000.0 {
            return None;
        }
        let claim_atoms =
            crate::bot::runtime::live_accounting::exact_floor_mul(inventory, self.payout)?;
        if claim_atoms.is_zero() {
            return None;
        }
        Some(ClaimedWin {
            token: token.to_string(),
            market_id: self.market_id,
            oracle_status: self.oracle_status.clone(),
            trading_status: self.trading_status.clone(),
            outcome_status: self.outcome_status.clone(),
            payout_millionths: millionths as u32,
            inventory_atoms: inventory,
            claim_atoms,
        })
    }
}

/// The original final, completely accounted order's net contribution.
#[derive(Debug, Clone)]
pub(crate) struct ProvenHistoryOrder {
    pub token: String,
    pub side: Side,
    pub net_inventory: U256,
}
fn add_history_total(
    totals: &mut HashMap<String, (U256, U256)>,
    token: &str,
    side: Side,
    net_inventory: U256,
) -> anyhow::Result<()> {
    let (bought, sold) = totals.entry(token.to_string()).or_default();
    let total = if side == Side::Buy { bought } else { sold };
    *total = total
        .checked_add(net_inventory)
        .ok_or_else(|| anyhow::anyhow!("HISTORICAL_INVENTORY_OVERFLOW"))?;
    Ok(())
}

impl PredictFunTrader {
    pub(super) async fn explain_positions_mode(
        &self,
        history: &HashMap<String, U256>,
        observed: &HashMap<String, U256>,
        collateral_atoms: U256,
        runtime: bool,
    ) -> anyhow::Result<ExplainedPositions> {
        let cleared = self.cleared_history(history, observed)?;
        if observed
            .iter()
            .any(|(token, amount)| history.get(token) != Some(amount))
        {
            return Ok(ExplainedPositions::default());
        }
        let mut settled_zero = Vec::new();
        let mut claimed_win = Vec::new();
        let mut official = HashMap::new();
        for (token, amount) in history {
            match observed.get(token) {
                Some(held) if held == amount => {}
                Some(_) => return Ok(ExplainedPositions::default()),
                None => {
                    let settlement = match cleared.get(token) {
                        Some(basis) => Some(basis.official.clone()),
                        None => self.official_settlement(token).await?,
                    };
                    match settlement {
                        Some(settlement) if settlement.payout == 0.0 => {
                            settled_zero.push(settlement.settled_zero(token));
                            official.insert(token.clone(), settlement);
                        }
                        Some(settlement) => match settlement.claimed_win(token, *amount) {
                            Some(basis) => {
                                claimed_win.push(basis);
                                official.insert(token.clone(), settlement);
                            }
                            None => return Ok(ExplainedPositions::default()),
                        },
                        None => return Ok(ExplainedPositions::default()),
                    }
                }
            }
        }
        // Old cleared claims are already in the broker's opening cash. Current
        // cash may legitimately change; only unqualified claims still need C0.
        let explained = claimed_win
            .iter()
            .all(|claim| cleared.contains_key(&claim.token))
            || (!runtime && self.claimed_cash_matches(collateral_atoms, &claimed_win));
        Ok(ExplainedPositions {
            settled_zero,
            claimed_win,
            explained,
            official,
        })
    }

    fn claimed_cash_matches(&self, collateral_atoms: U256, claims: &[ClaimedWin]) -> bool {
        let Some(budget) = self
            .run_context
            .read()
            .as_ref()
            .and_then(|context| context.budget_atoms)
        else {
            return false;
        };
        let Some(claimed) = claims.iter().try_fold(U256::ZERO, |total, claim| {
            total.checked_add(claim.claim_atoms)
        }) else {
            return false;
        };
        budget.checked_add(claimed) == Some(collateral_atoms)
    }

    pub(super) async fn official_settlement(
        &self,
        token: &str,
    ) -> anyhow::Result<Option<OfficialSettlement>> {
        use crate::bot::runtime::paper::{settle_from_facts, settlement_payout, Settlement};
        use crate::service::venue::MarketData;
        let known = self.client.market_for_token(token)?;
        let market = self.client.settlement_market(known.id).await?;
        let known_yes = known
            .outcomes
            .first()
            .map(|outcome| outcome.on_chain_id.as_str());
        let known_no = known
            .outcomes
            .get(1)
            .map(|outcome| outcome.on_chain_id.as_str());
        if market.id != known.id
            || known.outcomes.len() != 2
            || market.yes_token.is_empty()
            || market.no_token.is_empty()
            || market.yes_token == market.no_token
            || known_yes != Some(market.yes_token.as_str())
            || known_no != Some(market.no_token.as_str())
            || (token != market.yes_token && token != market.no_token)
        {
            return Ok(None);
        }
        let (Some(facts), Some((start, end))) = (
            market.round_facts.as_ref(),
            market.start_price.zip(market.end_price),
        ) else {
            return Ok(None);
        };
        if settle_from_facts(&market, facts, start, end) != Settlement::Confirmed {
            return Ok(None);
        }
        let Some(up_payout) = settlement_payout(&market, start, end) else {
            return Ok(None);
        };
        let yes = market.is_yes(token);
        let payout = if yes { up_payout } else { 1.0 - up_payout };
        let status = if yes {
            &facts.yes_status
        } else {
            &facts.no_status
        };
        Ok(Some(OfficialSettlement {
            market_id: market.id,
            oracle_status: facts.oracle_status.clone().unwrap_or_default(),
            trading_status: facts.trading_status.clone().unwrap_or_default(),
            outcome_status: status.clone().unwrap_or_default(),
            payout,
            facts: facts.clone(),
            prices: (start, end),
            variant: market.variant.clone(),
            feed_symbol: market.feed_symbol.clone(),
            window: market.starts_at.zip(market.ends_at),
        }))
    }

    /// Read the complete account history; only Final, accounted orders may be remembered.
    pub(super) async fn reconcile_account_history(&self) -> anyhow::Result<HistoryRead> {
        self.check_history_scope()?;
        let project = self.startup_history.read().scope.is_some();
        let mut legs: HashMap<String, Vec<EconomicLeg>> = HashMap::new();
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
        for _ in 0..crate::service::predict_fun::fills::MAX_PAGES {
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
                .await
                .map_err(|error| {
                    crate::service::predict_fun::request::account_read_error(error, "MATCHES")
                })?;
            for event in page.data {
                if !fingerprints.insert(format!("{event:?}")) {
                    anyhow::bail!("FACT_INCOMPLETE:AMBIGUOUS_MATCH_DUPLICATE");
                }
                self.check_history_market(&event.market)?;
                self.client.remember_market(&event.market);
                for (index, leg) in std::iter::once(&event.taker)
                    .chain(event.makers.iter())
                    .enumerate()
                {
                    if leg.signer.eq_ignore_ascii_case(&identity.trading_address) {
                        validate_order_hash(&leg.hash)?;
                        hashes.insert(leg.hash.clone());
                        if project && declared.iter().any(|d| d.eq_ignore_ascii_case(&leg.hash)) {
                            legs.entry(leg.hash.clone()).or_default().push(EconomicLeg {
                                market: MarketBinding::new(self, &event.market),
                                hash: leg.hash.clone(),
                                signer: leg.signer.to_ascii_lowercase(),
                                token: leg.outcome.on_chain_id.clone(),
                                quote: leg.quote_type.clone(),
                                amount: decimal_to_wei_exact(&leg.amount)?,
                                price: decimal_to_wei_exact(&leg.price)?,
                                fee: leg
                                    .fee
                                    .as_ref()
                                    .map(|f| {
                                        Ok::<_, anyhow::Error>((
                                            f.fee_type.clone(),
                                            decimal_to_wei_exact(&f.amount)?,
                                        ))
                                    })
                                    .transpose()?,
                                transaction: event.transaction_hash.clone(),
                                executed: event.executed_at.clone(),
                                maker: index != 0,
                            });
                        }
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
        if hashes.iter().any(|hash| {
            !self.bound_orders.read().contains_key(hash)
                && !declared.iter().any(|d| d.eq_ignore_ascii_case(hash))
        }) {
            anyhow::bail!("RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY");
        }
        for values in legs.values_mut() {
            values.sort();
        }
        self.check_history_legs(&legs)?;
        let mut totals: HashMap<String, (U256, U256)> = HashMap::new();
        for hash in hashes {
            if self.bound_orders.read().contains_key(&hash) {
                continue;
            }
            if !declared
                .iter()
                .any(|declared| declared.eq_ignore_ascii_case(&hash))
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:UNEXPLAINED_HISTORY");
            }
            if let Some(proven) = self.history_proven.read().get(&hash).cloned() {
                add_history_total(
                    &mut totals,
                    &proven.token,
                    proven.side,
                    proven.net_inventory,
                )?;
                continue;
            }
            let data = self
                .authed::<ApiOrderData>(reqwest::Method::GET, &format!("/orders/{hash}"), None)
                .await
                .map_err(|error| {
                    crate::service::predict_fun::request::account_read_error(error, "HISTORY_ORDER")
                })?
                .data;
            let contract = data
                .order
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_CONTRACT_ORDER"))?;
            let token = contract
                .token_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE:MISSING_TOKEN"))?;
            // Zero-fill declared orders have no positions or matches to seed the cache.
            let market = match self.client.market_for_token(&token) {
                Ok(market) => market,
                Err(_) => {
                    self.client
                        .get::<ApiMarket>(&format!("/markets/{}", data.market_id))
                        .await
                        .map_err(|error| {
                            crate::service::predict_fun::request::account_read_error(
                                error,
                                "HISTORY_ORDER",
                            )
                        })?
                        .data
                }
            };
            if market.id != data.market_id
                || market.outcomes.len() != 2
                || market.outcomes[0].on_chain_id == market.outcomes[1].on_chain_id
                || !market
                    .outcomes
                    .iter()
                    .any(|outcome| outcome.on_chain_id == token)
                || self.client.markets.read().values().any(|known| {
                    (known.id == market.id
                        || known.outcomes.iter().any(|old| {
                            market
                                .outcomes
                                .iter()
                                .any(|fresh| old.on_chain_id == fresh.on_chain_id)
                        }))
                        && (known.id != market.id
                            || known.outcomes.len() != 2
                            || known
                                .outcomes
                                .iter()
                                .zip(&market.outcomes)
                                .any(|(old, fresh)| old.on_chain_id != fresh.on_chain_id))
                })
            {
                return Err(mismatch("TOKEN_MISMATCH").into());
            }
            self.client.remember_market(&market);
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
            let bound = crate::service::predict_fun::fills::BoundOrder {
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
            let snapshot = self
                .read_order_execution(&hash, &bound)
                .await
                .map_err(|error| {
                    crate::service::predict_fun::request::account_read_error(error, "HISTORY_ORDER")
                })?;
            if !snapshot.pages_complete
                || !snapshot.accounting_complete()
                || !matches!(
                    snapshot.closure,
                    crate::service::venue::Closure::Final { .. }
                )
            {
                anyhow::bail!("RECONCILIATION_REQUIRED:HISTORY_NOT_CLOSED");
            }
            let (bought, sold) = totals
                .entry(snapshot.identity.token_id.clone())
                .or_default();
            let total = if side == Side::Buy { bought } else { sold };
            let net_inventory = *snapshot.accounting.net_inventory.value().unwrap();
            *total = total
                .checked_add(net_inventory)
                .ok_or_else(|| anyhow::anyhow!("HISTORICAL_INVENTORY_OVERFLOW"))?;
            self.history_proven.write().insert(
                hash,
                ProvenHistoryOrder {
                    token: snapshot.identity.token_id.clone(),
                    side,
                    net_inventory,
                },
            );
        }
        let totals = totals
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
            .collect::<anyhow::Result<_>>()?;
        Ok(HistoryRead { totals, legs })
    }
}
