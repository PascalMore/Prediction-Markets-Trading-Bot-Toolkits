//! The controlled live run's candidate submission.
//!
//! The paper run and the live run share one core and one candidate. What
//! differs is what happens to the candidate: the paper run simulates the
//! fill, and the live run reserves the cash, signs the order, proves the
//! bound, waits for the durable evidence and takes the last permission.
//!
//! This module is the live half of that difference. It is deliberately thin:
//! the broker's own state lock, the broker's own attempt bookkeeping and the
//! broker's own tombstone rules all still apply. What this adds is the
//! candidate's identity and the guard that the venue must pass.

use crate::bot::arbitrage::phase1::CandidateContext;
use crate::bot::runtime::live::{self, LiveState};
use crate::bot::runtime::live_control::{
    LiveCandidateGuard, LiveRunState, PendingLiveCandidate, RunControl,
};
use crate::bot::runtime::{Blocked, Broker, State, TakeResult};
use crate::models::{PlannedOrder, Side};
use crate::service::venue::{
    AccountIdentity, AttemptRecord, AttemptPhase, OrderVenue, Submission, TradableMarket,
};
use alloy_primitives::U256;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// What the live path needs of the market and the candidate.
///
/// It is the same information the paper path reads; only the destination
/// differs.
pub struct LiveCandidateRequest<'a> {
    pub market: &'a TradableMarket,
    pub yes_book: &'a crate::models::BookSnapshot,
    pub candidate: &'a CandidateContext,
}

/// The outcome of one live candidate submission.
pub struct LiveTakeOutcome {
    /// What the broker decided.
    pub result: TakeResult,
    /// The guard of the attempt, when one was made.
    pub guard: Option<Arc<LiveCandidateGuard>>,
    /// The attempt's local id, when one was made.
    pub attempt_id: Option<u64>,
}

/// Clear only the candidate owned by this operation, including early refusals.
/// Attempt/hash/tombstone and reservations deliberately survive cancellation.
struct PendingCandidateCompletion<'a> {
    broker: &'a Broker,
    candidate: &'a CandidateContext,
}

impl Drop for PendingCandidateCompletion<'_> {
    fn drop(&mut self) {
        let mut state = self.broker.state.lock();
        if let Some(run) = state.live.controlled.as_mut() {
            if run.pending_candidate.as_ref().is_some_and(|pending| pending.context.same_candidate(self.candidate)) {
                run.pending_candidate = None;
            }
        }
    }
}

impl Broker {
    /// Buy with a phase1 candidate, for a controlled live run.
    ///
    /// The candidate is the only context this path accepts: it carries the
    /// frozen inputs, the selected side and the identity of the decision. A
    /// bare BUY without a candidate is refused before anything is reserved.
    ///
    /// The sequence is the design's: the broker's own lock checks the
    /// candidate, reserves the worst-case cash and creates the attempt; the
    /// lock is released before the venue is called; and the venue's guarded
    /// path proves the bound, waits for the durable evidence and takes the
    /// last permission before the order reaches the transport.
    pub async fn take_live_candidate(
        &self,
        request: LiveCandidateRequest<'_>,
    ) -> anyhow::Result<LiveTakeOutcome> {
        let LiveCandidateRequest {
            market,
            yes_book: _yes_book,
            candidate,
        } = request;
        let lane = self.state.lock().live.controlled.as_ref()
            .map(|run| Arc::clone(&run.operation_lane))
            .ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let _operation = lane.lock().await;
        if !self.state.lock().live.controlled.as_ref().and_then(|run| run.pending_candidate.as_ref())
            .is_some_and(|pending| pending.context.same_candidate(candidate))
        {
            anyhow::bail!("CONTEXT_MISSING");
        }
        let _completion = PendingCandidateCompletion { broker: self, candidate };
        let result = self.submit_live_candidate(market, _yes_book, candidate).await;
        match &result {
            Err(error) => {
                super::live_events::stop_on_lane(self, preparation_reason(error))
                    .map_err(anyhow::Error::msg)?;
            }
            Ok(outcome) if outcome.attempt_id.is_none() => {
                // Nothing was created, so nothing new may be at the venue: the
                // block alone proves no unproven submission. Only an earlier
                // accepted order whose facts have not arrived yet can still be
                // unknown, and it is re-read here, bounded.
                self.resolve_blocked_candidate(
                    live::UNREAD_FACTS_BUDGET,
                    live::UNREAD_FACTS_INTERVAL,
                )
                .await
                .map_err(anyhow::Error::msg)?;
            }
            _ => {}
        }
        result
    }

    /// Resolve a candidate that was blocked before any attempt existed.
    ///
    /// The one fact that can still be unproven is an order this window already
    /// got accepted whose execution facts have not become visible. That order
    /// is re-read through its existing execution read, bounded in time.
    ///
    /// A refused candidate is not a missing fact: nothing was submitted, so
    /// nothing can be unproven, and the window keeps its existing stop. Only
    /// the bound expiring with the facts still invisible stops the window.
    pub(super) async fn resolve_blocked_candidate(
        &self,
        budget: std::time::Duration,
        interval: std::time::Duration,
    ) -> Result<(), String> {
        match live::re_read_unread_facts(self, budget, interval).await {
            // No order of this window waits for facts: a refused candidate
            // proves that nothing was submitted, so the window goes on.
            live::UnreadFacts::NothingToRead | live::UnreadFacts::Resolved => Ok(()),
            // The facts stayed invisible for the whole bound: the window
            // cannot prove what it owns, and it stops.
            live::UnreadFacts::Expired => {
                super::live_events::stop_on_lane(self, "FACT_INCOMPLETE")
            }
        }
    }

    /// The submission body runs only with this run's lane and candidate completion held.
    async fn submit_live_candidate(
        &self, market: &TradableMarket, _yes_book: &crate::models::BookSnapshot,
        candidate: &CandidateContext,
    ) -> anyhow::Result<LiveTakeOutcome> {
        // One measured window per submission: inert unless YQUANT_TIMING=1.
        super::timing::begin();
        let result = self.submit_live_candidate_inner(market, _yes_book, candidate).await;
        super::timing::finish();
        result
    }

    /// The measured submission: reserve, prove, sign, acknowledge, send.
    async fn submit_live_candidate_inner(
        &self, market: &TradableMarket, _yes_book: &crate::models::BookSnapshot,
        candidate: &CandidateContext,
    ) -> anyhow::Result<LiveTakeOutcome> {
        let token = candidate.token().to_string();
        let route = self.taker.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?
            .controlled_route(&token)?;
        if !candidate.matches_inputs(market, _yes_book) {
            anyhow::bail!("INPUT_CHANGED");
        }
        // The tick's own account read (sync_live) is the one this send is
        // bound to: it ran before the inputs this decision was built from, and
        // the guard re-proves its receipt, identity, generation, attempt
        // binding and freshness at every later check. A second full read here
        // would only age the book the decision is frozen on.
        if !self.live_pre_submit(candidate)? {
            return Ok(LiveTakeOutcome { result: TakeResult::Blocked(Blocked::DataIncomplete),
                guard: None, attempt_id: None });
        }
        super::timing::mark("pre_submit");

        // The reservation and the attempt are made under the broker's own
        // lock, exactly as the ordinary path does. No lock is held across the
        // venue call.
        let (handle, attempt_id, reservation, intent_key) = {
            let mut state = self.state.lock();
            let Some(controlled) = state.live.controlled.as_mut() else {
                anyhow::bail!("CONTEXT_MISSING");
            };
            if !controlled.may_add_risk() {
                return Ok(LiveTakeOutcome {
                    result: TakeResult::Blocked(Blocked::Halted),
                    guard: None,
                    attempt_id: None,
                });
            }
            // The pending candidate must be the one this call carries: a
            // different candidate is a different decision.
            let matches = controlled
                .pending_candidate
                .as_ref()
                .is_some_and(|pending| pending.context.same_candidate(candidate));
            if !matches {
                anyhow::bail!("INPUT_CHANGED");
            }
            let serial: u64 = candidate
                .candidate_id()
                .parse()
                .map_err(|_| anyhow::anyhow!("INPUT_CHANGED"))?;
            let Some(venue) = self.taker.as_ref() else {
                anyhow::bail!("CONTROLLED_MODE: the live run has no venue");
            };
            let planned = PlannedOrder {
                venue: venue.id(),
                token_id: token.clone(),
                side: Side::Buy,
                shares: candidate.gross_shares(),
                limit_price: candidate.limit_price(),
                usd_notional: candidate.gross_shares() * candidate.limit_price(),
                order_type: crate::models::OrderType::Fak,
                source_trade_hash: None,
            };
            // The guarded domain reserves the signed cash: a BUY's fee comes
            // out of the received shares, not out of the cash.
            let reservation = venue.controlled_reservation_atoms(&planned)?;
            if let Some(blocked) = self.check_live_entry(&mut state, market.id, reservation) {
                return Ok(LiveTakeOutcome {
                    result: TakeResult::Blocked(blocked),
                    guard: None,
                    attempt_id: None,
                });
            }
            if state.live.attempts.values().any(|handle| {
                let record = handle.lock();
                record.identity.market_id == market.id
                    && (record.submission != Submission::NotSubmitted
                        || matches!(record.phase, AttemptPhase::MayHaveSubmitted | AttemptPhase::Submitted))
            }) {
                return Ok(LiveTakeOutcome { result: TakeResult::Blocked(Blocked::Halted),
                    guard: None, attempt_id: None });
            }
            // The model's price-dependent fee estimate is not the signed worst-case
            // share floor. Reject an uneconomic candidate before reserving or signing.
            let gross = crate::service::predict_fun::controlled::gross_quantity(&planned)?;
            if !crate::service::predict_fun::controlled::conservative_buy_edge(
                gross, reservation, candidate.fee_rate_bps(), candidate.fair(),
                state.live.controlled.as_ref().unwrap().phase1.min_edge,
            )? {
                return Ok(LiveTakeOutcome { result: TakeResult::Blocked(Blocked::ThinBook),
                    guard: None, attempt_id: None });
            }
            let attempt_id = crate::bot::runtime::live::next_attempt_id(&mut state.live);
            let record = AttemptRecord {
                identity: crate::service::venue::OrderIdentity {
                    local_attempt_id: attempt_id,
                    signed_hash: None,
                    venue_order_id: None,
                    market_id: market.id,
                    token_id: token.clone(),
                    side: Side::Buy,
                    account: self
                        .identity
                        .clone()
                        .ok_or_else(|| anyhow::anyhow!("ACCOUNT_MISMATCH:MISSING_BROKER_IDENTITY"))?,
                },
                request_gross_atoms: crate::service::predict_fun::controlled::gross_quantity(&planned)?,
                r0: reservation,
                reserved_sell: U256::ZERO,
                phase: AttemptPhase::Prepared,
                submission: Submission::NotSubmitted,
                last_reason: None,
            };
            let handle = crate::bot::runtime::live::reserve_attempt(&mut state.live, record);
            let intent_key = crate::bot::runtime::live::taker_key(market.id, &token, Side::Buy);
            crate::bot::runtime::live::tombstone(&mut state.live, &intent_key, attempt_id);
            let run = state.live.controlled.as_mut().unwrap();
            run.pending_candidate.as_mut().unwrap().worst_cash_atoms = reservation;
            run.attempt_candidates.insert(attempt_id, candidate.candidate_id().to_string());
            run.traded.entry(market.id).or_insert_with(|| market.clone());
            (handle, attempt_id, reservation, intent_key)
        };
        super::timing::mark("reserved");
        let reserve_event = {
            use super::live_events::{self, LivePayload, ReservationPayload};
            let state = self.state.lock();
            let run = state.live.controlled.as_ref().unwrap();
            run.emitter.prepare(run.phase1.clock.now()?.utc, Some(market.id.to_string()), Some(token.clone()),
                &live_events::state_accounting(&state), LivePayload::Reservation(ReservationPayload {
                    candidate_id: candidate.candidate_id().into(), attempt_id: attempt_id.to_string(),
                    action: "RESERVE".into(), worst_cash_atoms: reservation.to_string(),
                    remaining_atoms: state.live.reservations[&attempt_id].to_string(), reason: "NONE".into(),
                }))
        };
        reserve_event.and_then(|event| event.flush()).map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
        super::timing::mark("reserve_event");

        // The guard is built from the run's own control block, so the stop
        // flag and the permission are the same state the broker reads.
        let (control, emitter) = {
            let state = self.state.lock();
            let controlled = state
                .live
                .controlled
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
            (
                Arc::clone(&controlled.control),
                Arc::clone(&controlled.emitter),
            )
        };
        let guard = Arc::new(LiveCandidateGuard::new(
            control,
            attempt_id,
            candidate.candidate_id().to_string(),
            emitter,
        ).bind(Arc::clone(&self.state), candidate.clone(), handle.clone(), route));

        let Some(venue) = self.taker.as_ref() else {
            anyhow::bail!("CONTROLLED_MODE: the live run has no venue");
        };
        let planned = PlannedOrder {
            venue: venue.id(),
            token_id: token.clone(),
            side: Side::Buy,
            shares: candidate.gross_shares(),
            limit_price: candidate.limit_price(),
            usd_notional: candidate.gross_shares() * candidate.limit_price(),
            order_type: crate::models::OrderType::Fak,
            source_trade_hash: None,
        };

        let ticket = match venue
            .place_controlled_tracked(&planned, 0, true, handle.clone(), guard.as_ref())
            .await
        {
            Ok(ticket) => ticket,
            Err(error) => {
                let event_reason = preparation_reason(&error);
                // A candidate the venue refused while the request provably
                // never left — the fresh book no longer supports the frozen
                // decision, or those frozen inputs aged out at the last check —
                // ends this candidate, not the run. Its cash is released, the
                // refusal is recorded and the window stays open; the pending
                // candidate is cleared by the completion guard, so the same
                // decision can never be taken twice.
                if error.downcast_ref::<crate::service::venue::CandidateRefused>().is_some() {
                    // Only a request that provably never left releases cash.
                    // The original hash/tombstone and any unknown risk survive.
                    crate::bot::runtime::live::release_not_submitted(self, &handle, &intent_key);
                    guard.emit_order_result(reservation, event_reason, None)?;
                    return Ok(LiveTakeOutcome {
                        // The fresh book no longer supports the frozen decision
                        // (moved past the limit, or too thin for the size):
                        // the entry is blocked, the run is not.
                        result: TakeResult::Blocked(Blocked::ThinBook),
                        guard: Some(guard),
                        attempt_id: Some(attempt_id),
                    });
                }
                self.state.lock().live.controlled.as_mut().unwrap()
                    .stop(event_reason, true, chrono::Utc::now());
                // Only a request that provably never left releases cash.
                // The original hash/tombstone and any unknown risk survive.
                crate::bot::runtime::live::release_not_submitted(self, &handle, &intent_key);
                guard.emit_order_result(reservation, event_reason, None)?;
                return Err(error);
            }
        };

        let metadata = handle.lock().identity.signed_hash.as_deref()
            .and_then(|hash| venue.controlled_order_metadata(hash));
        guard.emit_order_result(reservation, "NONE", metadata.as_ref())?;

        // The venue's own facts and the very same commit drive feedback and wire.
        let result = match ticket.execution.as_ref() {
            Some(snapshot) => match crate::bot::runtime::live::consume_snapshot(self, snapshot) {
                Ok(applied) => net_take_result(&applied, candidate.limit_price()),
                Err(_) => TakeResult::Blocked(Blocked::Halted),
            },
            None => {
                // A live venue without facts is an unknown fill, not a
                // planned one.
                let mut state = self.state.lock();
                state.halted = true;
                if let Some(controlled) = state.live.controlled.as_mut() {
                    controlled.stop("FACT_INCOMPLETE", true, chrono::Utc::now());
                }
                crate::bot::runtime::live::tombstone(&mut state.live, &intent_key, attempt_id);
                let events = super::live_events::prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?;
                drop(state);
                super::live_events::flush_events(events).map_err(anyhow::Error::msg)?;
                TakeResult::Blocked(Blocked::Halted)
            }
        };

        // The candidate has been dealt with: it may not be reused.
        {
            let mut state = self.state.lock();
            if let Some(controlled) = state.live.controlled.as_mut() {
                controlled.pending_candidate = None;
            }
        }

        Ok(LiveTakeOutcome {
            result,
            guard: Some(guard),
            attempt_id: Some(attempt_id),
        })
    }

    /// The live path's own entry checks.
    ///
    /// They are the ordinary ones plus the window's caps, and they run under
    /// the broker's own lock, before anything is reserved.
    pub(super) fn check_live_entry(
        &self,
        state: &mut State,
        market_id: u64,
        reservation: U256,
    ) -> Option<Blocked> {
        use crate::bot::runtime::live_accounting::{check_caps, risk_exposure};
        crate::bot::runtime::live_control::refresh_live_risk(state);
        // Only the lifecycle part of ordinary check_entry applies here. Its
        // float minimum/caps cannot validate an atom-denominated live budget.
        if self.controlled.as_ref().is_none_or(|run| run.stopped())
            || state.entry_stop_reason.is_some()
        {
            return Some(Blocked::Deadline);
        }
        let Some(controlled) = state.live.controlled.as_ref() else {
            return Some(Blocked::DataIncomplete);
        };
        if state.halted || !controlled.may_add_risk() { return Some(Blocked::Halted); }
        let Ok(now) = controlled.phase1.clock.now() else { return Some(Blocked::DataIncomplete); };
        if !controlled.options.in_window(now.utc) { return Some(Blocked::Deadline); }
        let Some(read) = controlled.account_read.as_ref() else { return Some(Blocked::DataIncomplete); };
        if !read.matches(&state.live, now) || read.snapshot.identity != controlled.identity {
            return Some(Blocked::DataIncomplete);
        }
        let Some(gate) = self.gate.as_ref() else { return Some(Blocked::DataIncomplete); };
        if !gate.budget_confirmed(&controlled.identity) || !gate.allows_new_orders(&controlled.identity) {
            return Some(Blocked::Halted);
        }
        let gas = read.snapshot.gas_atoms.zip(state.live.baseline.as_ref().and_then(|read| read.gas_atoms))
            .map(|(balance_atoms, initial_atoms)| crate::bot::runtime::live_accounting::GasReading {
                balance_atoms, initial_atoms,
            });
        let policy = &controlled.options.gas_policy;
        // Context addresses may be lowercase; the adapter emits EIP-55 checksum case.
        // Address identity must not depend on that representation.
        if !policy.payer_address.eq_ignore_ascii_case(&controlled.identity.signer_address)
            || gas.is_some_and(|read| read.balance_atoms > read.initial_atoms)
            || !crate::bot::runtime::live_accounting::check_gas(gas,
                policy.minimum_balance_atoms, policy.maximum_consumption_atoms).may_add_risk
        {
            return Some(Blocked::DataIncomplete);
        }
        let Some(exposure) = risk_exposure(&state.live) else { return Some(Blocked::DataIncomplete); };
        let Some(official_cash) = read.snapshot.collateral_atoms else { return Some(Blocked::DataIncomplete); };
        let params = controlled.options.params;
        let applied_cash = state.live.net_cash.values().try_fold(U256::ZERO, |total, cash| total.checked_add(*cash));
        let available = applied_cash.and_then(|spent| params.budget.checked_sub(spent))
            .and_then(|cash| cash.checked_sub(exposure.reserved))
            .zip(official_cash.checked_sub(exposure.reserved))
            .map(|(budget, official)| budget.min(official));
        let (cost, reserved) = exposure.markets.get(&market_id).copied().unwrap_or_default();
        let caps = check_caps(&params, reservation, cost, reserved, exposure.cost, exposure.reserved,
            available.unwrap_or(U256::ZERO));
        if reservation < U256::from(crate::bot::runtime::live_accounting::MIN_ORDER_ATOMS) {
            Some(Blocked::BelowMinimum)
        } else if !caps.order_ok { Some(Blocked::OrderCap)
        } else if !caps.market_ok { Some(Blocked::MarketCap)
        } else if !caps.total_ok { Some(Blocked::TotalCap)
        } else if !caps.available_ok { Some(Blocked::InsufficientCash)
        } else { None }
    }
}

/// Map typed venue failures to the closed public reason set; never emit raw text.
pub(crate) fn preparation_reason(error: &anyhow::Error) -> &'static str {
    use crate::service::venue::{VenueError, VenueErrorCode};
    let Some(error) = error.downcast_ref::<VenueError>() else {
        return if error.to_string() == "INPUT_CHANGED" { "INPUT_CHANGED" } else { "FACT_INCOMPLETE" };
    };
    match error.code {
        VenueErrorCode::InvalidInput if error.reason == "INPUT_CHANGED" => "INPUT_CHANGED",
        VenueErrorCode::InvalidInput => "SIGNED_BOUND_UNPROVEN",
        VenueErrorCode::ExecutionNotEnabled | VenueErrorCode::BudgetUnconfirmed
            | VenueErrorCode::ReconciliationRequired => "RECONCILIATION_REQUIRED",
        VenueErrorCode::AccountMismatch => "ACCOUNT_MISMATCH",
        VenueErrorCode::ReadUnavailable => "READ_UNAVAILABLE",
        VenueErrorCode::SubmissionUnknown => "SUBMISSION_UNKNOWN",
        VenueErrorCode::FactIncomplete | VenueErrorCode::TxUnconfirmed
            | VenueErrorCode::SdkIncompatible => "FACT_INCOMPLETE",
    }
}

/// Feed the core only the net inventory increment committed by the live ledger.
/// Unknown or unapplied accounting is not evidence that the requested gross filled.
pub(super) fn net_take_result(
    applied: &crate::bot::runtime::live::AppliedExecution,
    fallback_price: f64,
) -> TakeResult {
    if !applied.accounting_complete {
        return TakeResult::Blocked(Blocked::Halted);
    }
    let Some(net) = applied.net_inventory_delta else {
        return TakeResult::Blocked(Blocked::Halted);
    };
    let price = if applied.delta.gross_q.is_zero() {
        fallback_price
    } else {
        crate::bot::runtime::live::wei_to_shares(applied.delta.gross_cash)
            / crate::bot::runtime::live::wei_to_shares(applied.delta.gross_q)
    };
    TakeResult::Filled {
        shares: crate::bot::runtime::live::wei_to_shares(net),
        price,
    }
}

/// The pending candidate of a controlled live run.
pub fn set_pending_candidate(
    state: &mut State,
    candidate: CandidateContext,
    worst_cash_atoms: U256,
) {
    let Some(controlled) = state.live.controlled.as_mut() else {
        return;
    };
    controlled.pending_candidate = Some(PendingLiveCandidate {
        candidate_id: candidate.candidate_id().to_string(),
        context: candidate,
        worst_cash_atoms,
    });
}

/// The run's control block, when it has one.
pub fn control_of(state: &State) -> Option<Arc<parking_lot::Mutex<RunControl>>> {
    state
        .live
        .controlled
        .as_ref()
        .map(|controlled| Arc::clone(&controlled.control))
}

/// Whether the run's control block says it may still add risk.
pub fn may_add_risk(state: &State) -> bool {
    control_of(state)
        .map(|control| control.lock().may_add_risk())
        .unwrap_or(false)
}

/// The account the run trades.
pub fn identity_of(state: &State) -> Option<&AccountIdentity> {
    state
        .live
        .controlled
        .as_ref()
        .map(|controlled| &controlled.identity)
}

/// The state machine's current state.
pub fn run_state(state: &State) -> Option<LiveRunState> {
    control_of(state).map(|control| control.lock().state)
}

/// Whether the run's output has already failed.
pub fn output_failed(state: &State) -> bool {
    state
        .live
        .controlled
        .as_ref()
        .is_some_and(|controlled| controlled.output_failed.load(Ordering::SeqCst))
}

/// The live state facts of a broker, for the accounting projection.
pub fn facts_of(state: &LiveState) -> crate::bot::runtime::live_accounting::StateFacts<'_> {
    crate::bot::runtime::live_accounting::StateFacts { state }
}
