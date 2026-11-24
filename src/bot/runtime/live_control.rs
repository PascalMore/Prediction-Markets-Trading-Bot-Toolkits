//! The controlled live run's own execution path.
//!
//! Everything here sits beside the broker's ordinary state: it holds no cash
//! table of its own, it owns no client and it holds no signing material. Its
//! job is to make the live path behave like the paper path — one core, one
//! candidate, one order — and to make every step of the way auditable.
//!
//! The pieces are:
//!
//! - [`ControlledLiveState`], the run metadata that lives inside the
//!   broker's own state lock.
//! - [`LiveCandidateGuard`], the handle a venue uses to check the prepared
//!   facts, wait for the durable evidence and take the last permission.
//! - [`channel`], the new control frames and the EOF watcher that replaces
//!   the paper run's single-byte watcher.
//!
//! The state machine is one-way: `Ready` may become `StopNewRisk` or
//! `WaitCash`, and neither ever becomes `Ready` again.

pub mod channel;
pub mod diagnostics;
pub mod cash_wait;

#[cfg(test)]
#[path = "live_control/tests/r16_cash_drive.rs"]
mod r16_cash_drive;
#[cfg(test)]
#[path = "live_control/tests/r16_diagnostics.rs"]
mod r16_diagnostics;

#[cfg(test)]
#[path = "live_control/tests/order_wait.rs"]
mod order_wait_tests;

#[cfg(test)]
#[path = "live_control/tests.rs"]
pub(crate) mod candidate_tests;

use crate::bot::arbitrage::phase1::CandidateContext;
use crate::bot::runtime::live::LiveState;
use crate::bot::runtime::live_events::LiveEventEmitter;
use crate::bot::runtime::phase1_control::Phase1Run;
use crate::bot::runtime::{Broker, State};
use crate::config::controlled_live::{ControlledLiveOptions, OperationsMode};
use crate::service::venue::{
    AccountIdentity, ControlledSendGuard, SignedExecutionBound, VenueError, VenueErrorCode,
};
use alloy_primitives::U256;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Where the run is in its one-way state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRunState {
    /// The run may still add risk.
    Ready,
    /// The run stopped adding risk.
    StopNewRisk,
    /// The run stopped because an official claim waits for a human.
    WaitCash,
    /// The run handed its remaining risk over.
    Handoff,
    /// The run is over.
    Ended,
}

impl LiveRunState {
    pub fn as_str(self) -> &'static str {
        match self {
            LiveRunState::Ready => "READY",
            LiveRunState::StopNewRisk => "STOP_NEW_RISK",
            LiveRunState::WaitCash => "WAIT_CASH",
            LiveRunState::Handoff => "HANDOFF",
            LiveRunState::Ended => "ENDED",
        }
    }

    /// Whether the run may still add risk from this state.
    pub fn may_add_risk(self) -> bool {
        self == LiveRunState::Ready
    }
}

/// The reason a run stopped adding risk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStop {
    /// The stable reason token.
    pub reason: String,
    /// The failure code the stop was already holding, when one was known.
    pub detail: Option<String>,
    /// Whether a human has to take the remaining risk over.
    pub takeover_required: bool,
    /// The moment the stop was set.
    pub at: DateTime<Utc>,
}

/// How long consecutive read failures or valuation gaps may degrade the
/// window before it stops with its existing reason.
///
/// The budget is the one the unread-facts wait already uses: a window that
/// cannot read for two minutes is not observing any more, it is blind.
pub(super) const READ_FAILURE_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// Which failure a streak counts.
///
/// The kinds are kept apart so that a success in one does not clear the other:
/// a working account read says nothing about an execution read that keeps
/// failing, and a fresh mark says nothing about an unread venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FailureKind {
    /// A read of the venue's own account or market facts failed.
    Read,
    /// A read of one order's execution facts failed.
    Execution,
    /// The valuation of a held position could not be made.
    Valuation,
}

/// The consecutive failures that keep a window observing without adding risk.
#[derive(Debug, Clone, Copy)]
pub(super) struct FailureStreak {
    /// How many failures the streak holds.
    pub count: u32,
    /// When the streak started.
    pub since: DateTime<Utc>,
}

/// The controlled live run's own metadata.
///
/// It lives inside the broker's state lock, next to the ledger: the pending
/// candidate, the attempt relations, the stop flag and the output sequence
/// are part of the same state, not a second record of it.
#[derive(Debug, Clone)]
pub struct ControlledLiveState {
    /// The phase1 clock and economics of this run.
    pub phase1: Phase1Run,
    /// The public options this run was started with.
    pub options: ControlledLiveOptions,
    /// The account this run trades.
    pub identity: AccountIdentity,
    /// The one candidate that has not been submitted yet.
    pub pending_candidate: Option<PendingLiveCandidate>,
    /// The candidate of each attempt, by attempt id.
    pub attempt_candidates: HashMap<u64, String>,
    /// The official result of each settled attempt, by attempt id.
    pub attempt_outcomes: HashMap<u64, LiveOutcome>,
    /// Frozen official round identity from the original candidate submission.
    pub traded: HashMap<u64, crate::service::venue::TradableMarket>,
    /// The run's one-way state machine, shared with the guard.
    pub control: Arc<Mutex<RunControl>>,
    /// The output sequence of this run.
    pub event_seq: Arc<AtomicU64>,
    /// A failed output cannot be retried or presented as complete evidence.
    pub output_failed: Arc<AtomicBool>,
    /// Latest complete official read tied to the ledger version it reconciled.
    pub account_read: Option<LiveAccountRead>,
    /// Cash wait metadata, never a second balance ledger.
    pub cash_wait: Option<cash_wait::CashWait>,
    /// The bounded wait for one accepted order's complete execution facts.
    ///
    /// It holds no quantity, no cash and no release: the ledger's own atomic
    /// facts remain the only truth about the order, and only the original
    /// Final path still releases the risk.
    pub order_wait: Option<crate::bot::runtime::live::order_wait::OrderWait>,
    /// The original deadline on the run clock; it never restarts with a wait.
    pub run_deadline_elapsed_ms: u64,
    /// When this tick's first account read was issued.
    ///
    /// It is the tick's own origin for the send path's measurement: the
    /// startup seed is not one, and a later read's receipt never overwrites
    /// it.
    pub tick_started_elapsed_ms: Option<u64>,
    /// Cash the opening account read already held as claimed payouts. The
    /// declared budget stays the pre-claim C0, so the opening projection adds
    /// exactly this verified credit instead of hiding it.
    pub opening_claim_cash: U256,
    /// Receipt times of valid original-mid marks, invalidated on a bad book.
    pub mark_receipts: HashMap<String, crate::service::observation::CheckTime>,
    /// Monotone within this risk window; a recovered valuation cannot clear it.
    pub loss_latched: bool,
    /// The consecutive failures that keep this window observing without risk.
    ///
    /// A failed read or an unvaluable holding degrades the window instead of
    /// stopping it: the window keeps watching, adds no risk, and recovers by
    /// itself when the facts come back.
    pub(super) read_failures: Option<FailureStreak>,
    /// The consecutive execution-read failures of this window.
    pub(super) execution_failures: Option<FailureStreak>,
    /// The consecutive valuation gaps of this window.
    pub(super) valuation_failures: Option<FailureStreak>,
    /// Whether the window is degraded: it observes, but it adds no risk.
    ///
    /// The flag is derived from the streaks and kept beside them so that the
    /// entry checks can read it without taking three locks.
    pub(super) degraded: bool,
    /// Preserve the first complete triggering value even if later marks recover.
    pub(super) loss_trigger: Option<super::live_accounting::SignedAtoms>,
    pub(super) loss_event_emitted: bool,
    pub(super) stop_event_emitted: bool,
    /// All live producers and executions serialize through this original run lane.
    pub operation_lane: Arc<tokio::sync::Mutex<()>>,
    /// One emitter, sequence and output failure for the whole run.
    pub emitter: Arc<LiveEventEmitter>,
    /// This run's sole ACK channel, never shared across run identities.
    pub channel: Arc<channel::ControlChannel>,
    /// Test-only: the preparation checkpoints and their injected faults.
    #[cfg(test)]
    pub(crate) preparation: Arc<candidate_tests::PreparationHooks>,
}

/// A receipt-bound account observation, not a second cash balance.
#[derive(Debug, Clone)]
pub struct LiveAccountRead {
    pub snapshot: crate::service::venue::AccountSnapshot,
    pub checked: crate::service::observation::CheckTime,
    pub next_attempt_id: u64,
    pub generations: HashMap<u64, u64>,
}

/// The run's one-way state machine, shared between the broker and the guard.
///
/// It is a separate small block rather than a field of the broker's state:
/// the guard has to read it while the venue is sending, and taking the
/// broker's whole state lock there would hold it across the network.
#[derive(Debug, Clone)]
pub struct RunControl {
    /// Where the run is in its state machine.
    pub state: LiveRunState,
    /// Why the run stopped, when it did.
    pub stop: Option<LiveStop>,
    /// The run's own output failure flag.
    pub output_failed: Arc<AtomicBool>,
}

impl RunControl {
    /// Build the control block of a ready run.
    pub fn new(output_failed: Arc<AtomicBool>) -> Self {
        Self {
            state: LiveRunState::Ready,
            stop: None,
            output_failed,
        }
    }

    /// Whether the run may still add risk.
    pub fn may_add_risk(&self) -> bool {
        self.state.may_add_risk() && !self.output_failed.load(Ordering::SeqCst)
    }

    /// Whether the run must be finished at once.
    ///
    /// Only a stop or a broken output lane ends a window. A window that is
    /// merely degraded still observes: it may not add risk, but it waits for
    /// its own facts to come back, so it is not finished.
    pub fn must_end(&self) -> bool {
        !matches!(self.state, LiveRunState::Ready | LiveRunState::WaitCash)
            || self.output_failed.load(Ordering::SeqCst)
    }

    /// Stop adding risk, and say why.
    ///
    /// The stop is sticky: once the run has stopped, it never becomes ready
    /// again, whatever the market does.
    pub fn stop(&mut self, reason: &str, takeover_required: bool, at: DateTime<Utc>) {
        let replace_wait = self.state == LiveRunState::WaitCash
            && reason != crate::bot::runtime::live_events::reason::CASH_PENDING;
        if self.state == LiveRunState::Ready || replace_wait {
            self.state = if reason == crate::bot::runtime::live_events::reason::CASH_PENDING {
                LiveRunState::WaitCash
            } else {
                LiveRunState::StopNewRisk
            };
        }
        if self.stop.is_none() || replace_wait {
            self.stop = Some(LiveStop {
                reason: reason.to_string(),
                detail: None,
                takeover_required,
                at,
            });
        }
    }

    /// Stop adding risk with the failure code that was already in hand.
    ///
    /// The first stop wins: a later stop cannot rewrite the reason, the
    /// takeover flag or the code an earlier one recorded.
    pub fn stop_with_detail(&mut self, reason: &str, detail: &str,
                            takeover_required: bool, at: DateTime<Utc>) {
        let first = self.stop.is_none() || (self.state == LiveRunState::WaitCash
            && reason != crate::bot::runtime::live_events::reason::CASH_PENDING);
        self.stop(reason, takeover_required, at);
        if first {
            if let Some(stop) = self.stop.as_mut() {
                stop.detail = Some(detail.to_string());
            }
        }
    }
}

/// The candidate a live attempt is built from.
#[derive(Clone)]
pub struct PendingLiveCandidate {
    pub context: CandidateContext,
    pub candidate_id: String,
    pub worst_cash_atoms: U256,
}

impl std::fmt::Debug for PendingLiveCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingLiveCandidate")
            .field("candidate_id", &self.candidate_id)
            .field("worst_cash_atoms", &self.worst_cash_atoms)
            .finish()
    }
}

/// The official outcome of a settled attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveOutcome {
    /// The official payout of the yes outcome.
    pub payout_yes: Option<f64>,
    /// Immutable official fixing values used to detect later result revisions.
    pub result_prices: (f64, f64),
    /// The net shares the attempt holds.
    pub position_net_atoms: U256,
    /// The value of the claim, when it is known.
    pub claim_value_atoms: Option<U256>,
    /// Whether the claim has been confirmed by a cash read-back.
    pub cash_confirmed: bool,
}

impl ControlledLiveState {
    /// Whether the run is in a state where it may add risk.
    ///
    /// A degraded window may not add risk: it observes until its facts come
    /// back. Degradation is not a stop — the run's own state machine still
    /// says `Ready`, and a success restores trading.
    ///
    /// An order that is waiting for its own facts may not add risk either: the
    /// window knows an order it may still own, so it only observes.
    pub fn may_add_risk(&self) -> bool {
        self.control.lock().may_add_risk()
            && !self.degraded
            && self.order_wait.is_none()
    }

    /// Whether the window must be finished at once.
    ///
    /// A degraded window is not finished: it observes until its facts come
    /// back, and only a stop or a broken output lane ends it.
    pub fn must_end(&self) -> bool {
        self.control.lock().must_end() || self.phase1.clock.now().map_or(true, |now| {
            now.utc >= self.options.deadline_at || now.elapsed_ms >= self.run_deadline_elapsed_ms
                || self.cash_wait.as_ref().is_some_and(|wait| !wait.may_continue(
                    now, self.options.deadline_at, self.run_deadline_elapsed_ms))
        })
    }

    /// Record one failure of a kind and say whether the window must stop.
    ///
    /// A failure inside the budget degrades the window: it keeps observing,
    /// it adds no risk, and it recovers by itself when the facts come back.
    /// A streak that outlasts the budget is a blind window, and it stops with
    /// the reason that failure already carries.
    pub(super) fn note_failure(
        &mut self, kind: FailureKind, reason: &str, now: DateTime<Utc>,
    ) -> bool {
        let streak = match kind {
            FailureKind::Read => &mut self.read_failures,
            FailureKind::Execution => &mut self.execution_failures,
            FailureKind::Valuation => &mut self.valuation_failures,
        };
        let streak = streak.get_or_insert(FailureStreak { count: 0, since: now });
        streak.count += 1;
        let since = streak.since;
        self.degraded = true;
        if now.signed_duration_since(since) >= chrono::Duration::from_std(READ_FAILURE_BUDGET)
            .unwrap_or_else(|_| chrono::Duration::zero())
        {
            self.stop(reason, true, now);
            return true;
        }
        false
    }

    /// Clear the streak of a kind: its facts came back.
    pub(super) fn clear_failure(&mut self, kind: FailureKind) {
        match kind {
            FailureKind::Read => self.read_failures = None,
            FailureKind::Execution => self.execution_failures = None,
            FailureKind::Valuation => self.valuation_failures = None,
        }
        self.degraded = self.read_failures.is_some()
            || self.execution_failures.is_some()
            || self.valuation_failures.is_some();
    }

    /// Whether the window is degraded: it observes, but it adds no risk.
    pub fn degraded(&self) -> bool {
        self.degraded
    }
}

/// The guard a controlled live order passes through.
///
/// It is the only thing that may hand a signed order to the transport, and it
/// does so exactly once. The order's facts are checked against the frozen
/// candidate, the critical evidence is waited for, and the last permission is
/// consumed. After that the order may be sent; the guard never sends it
/// twice.
pub struct LiveCandidateGuard {
    control: Arc<Mutex<RunControl>>,
    /// The identity of the attempt this guard belongs to.
    attempt_id: u64,
    /// The candidate the attempt was built from.
    candidate_id: String,
    /// The expected signed hash, once the order is signed.
    expected_hash: Mutex<Option<String>>,
    /// The one permission to send.
    permit: Mutex<Option<PermitState>>,
    /// Whether the durable evidence was confirmed.
    acked: AtomicBool,
    /// The emitter of the run's events.
    emitter: Arc<LiveEventEmitter>,
    binding: Option<GuardBinding>,
    bound: Mutex<Option<SignedExecutionBound>>,
    /// The measurements this attempt froze: the tick's account receipt and
    /// the candidate's own book and spot clocks.
    measurement: Mutex<Option<diagnostics::FrozenMeasurement>>,
    /// The first send check that failed for this attempt.
    diagnostic: Mutex<Option<crate::service::venue::SendCheckObservation>>,
}

struct GuardBinding {
    state: Arc<Mutex<State>>,
    candidate: CandidateContext,
    handle: crate::service::venue::AttemptHandle,
    route: String,
}

/// The state of the one send permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermitState {
    /// Nothing was checked yet.
    Unprepared,
    /// The facts were checked and the evidence is durable.
    Ready,
    /// The permission was consumed: the order may have been sent.
    Consumed,
}

/// What a frozen input that no longer stands means at one check.
///
/// The same refusal is produced where the attempt is still `Prepared` and
/// `NotSubmitted` and the send permission is still held — nothing can have
/// left the process, so it ends this candidate only — and under the send
/// permission, where the transport seam may already own the order: there the
/// same change must stop the window. The recorded reason token is the same
/// either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrozenInput {
    /// This candidate is refused; its cash is released and the run goes on.
    Candidate,
    /// The window stops; the transport seam may already own the order.
    Window,
}

impl LiveCandidateGuard {
    /// Build the guard of one attempt.
    pub fn new(
        control: Arc<Mutex<RunControl>>,
        attempt_id: u64,
        candidate_id: String,
        emitter: Arc<LiveEventEmitter>,
    ) -> Self {
        Self {
            control,
            attempt_id,
            candidate_id,
            expected_hash: Mutex::new(None),
            permit: Mutex::new(Some(PermitState::Unprepared)),
            acked: AtomicBool::new(false),
            emitter,
            binding: None,
            bound: Mutex::new(None),
            measurement: Mutex::new(None),
            diagnostic: Mutex::new(None),
        }
    }

    pub(super) fn bind(
        mut self, state: Arc<Mutex<State>>, candidate: CandidateContext,
        handle: crate::service::venue::AttemptHandle, route: String,
    ) -> Self {
        self.binding = Some(GuardBinding { state, candidate, handle, route });
        if let Some(binding) = &self.binding {
            let state = binding.state.lock();
            if let Some(run) = &state.live.controlled {
                self.freeze_measurement(run.account_read.as_ref().map(|read| read.checked),
                    run.tick_started_elapsed_ms);
            }
        }
        self
    }

    /// Project the actual returned handle and immutable signed proof on the same lane.
    pub(super) fn emit_order_result(
        &self, previous_reservation: U256, event_reason: &str,
        metadata: Option<&crate::service::venue::ControlledOrderMetadata>,
    ) -> Result<()> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let bound = self.bound.lock().clone();
        let diagnostic = self.diagnostic();
        let events = {
            let mut state = binding.state.lock();
            let mut events = Vec::new();
            if let Some(event) = super::live_events::prepare_order(&state, self.attempt_id,
                bound.as_ref(), metadata, event_reason, diagnostic).map_err(anyhow::Error::msg)?
            { events.push(event); }
            if let Some(event) = super::live_events::prepare_release(&state, self.attempt_id,
                previous_reservation, event_reason).map_err(anyhow::Error::msg)?
            { events.push(event); }
            events.extend(super::live_events::prepare_lifecycle(&mut state).map_err(anyhow::Error::msg)?);
            events
        };
        super::live_events::flush_events(events).map_err(anyhow::Error::msg)
    }

    /// Recheck the actual core and original ledger without holding a lock over IO.
    ///
    /// The stage names the boundary this check belongs to, so the diagnostic
    /// says where the refusal happened. The original order of the checks is
    /// unchanged: the attempt binding is proved first, then the identity,
    /// then the frozen input.
    fn check_locked(&self, state: &mut State, bound: &SignedExecutionBound,
        frozen: FrozenInput, stage: crate::service::venue::SendCheckStage,
    ) -> Result<crate::bot::arbitrage::phase1::DecisionDiagnostic> {
        use super::live_accounting as accounting;
        use crate::service::venue::{AttemptPhase, Submission};
        use crate::bot::arbitrage::phase1::{recheck_diagnostic, Phase};
        use crate::service::venue::SendCheckDetail;
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        refresh_live_risk(state);
        let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let now = run.phase1.clock.now()?;
        let candidate = &binding.candidate;
        let record = binding.handle.lock().clone();
        let read = run.account_read.as_ref().ok_or_else(|| {
            self.record_check_at(now, stage, SendCheckDetail::ReadUnavailable);
            anyhow::anyhow!("READ_UNAVAILABLE")
        })?;
        if state.halted || state.entry_stop_reason.is_some() || !run.may_add_risk()
            || !run.options.in_window(now.utc) || run.loss_latched
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::RunStopped);
            bail!("RUN_STOPPED");
        }
        // The attempt this guard belongs to: the emitter, the handle, the
        // candidate and the pending reservation must all be the same ones.
        if !Arc::ptr_eq(&self.emitter, &run.emitter)
            || !state.live.attempts.get(&self.attempt_id).is_some_and(|handle| Arc::ptr_eq(handle, &binding.handle))
            || run.attempt_candidates.get(&self.attempt_id) != Some(&self.candidate_id)
            || !run.pending_candidate.as_ref().is_some_and(|pending| pending.context.same_candidate(candidate)
                && pending.worst_cash_atoms == record.r0)
            || record.identity.local_attempt_id != self.attempt_id
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::AttemptMismatch);
            bail!("INPUT_CHANGED");
        }
        // The identity of the order: hash, account, market, token and side.
        if record.identity.signed_hash.as_deref() != Some(&bound.signed_hash)
            || record.identity.account != run.identity || bound.account != run.identity
            || record.identity.market_id != candidate.market_id() || bound.market_id != candidate.market_id()
            || record.identity.token_id != candidate.token() || bound.token_id != candidate.token()
            || record.identity.side != crate::models::Side::Buy
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::IdentityMismatch);
            bail!("INPUT_CHANGED");
        }
        // The frozen input itself: route, fee, quantity, bound and phase.
        if bound.route != binding.route
            || bound.fee_rate_bps != candidate.fee_rate_bps()
            || record.request_gross_atoms != bound.request_gross_atoms
            || bound.request_gross_atoms != crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares())?
            || accounting::minimum_net_shares(bound.request_gross_atoms, bound.fee_rate_bps) != Some(bound.min_net_shares_atoms)
            || bound.fee_rate_bps > crate::service::predict_fun::controlled::MAX_SIGNED_FEE_BPS
            || record.phase != AttemptPhase::Prepared || record.submission != Submission::NotSubmitted
            || bound.max_cash_atoms != bound.gross_cash_atoms || bound.max_cash_atoms > record.r0
            || bound.expiration <= now.utc.timestamp()
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::FrozenInput);
            bail!("INPUT_CHANGED");
        }
        if !fresh_receipt(read.checked, now) || !read.snapshot.complete
            || read.snapshot.identity != run.identity || read.generations != state.live.generation
            || read.next_attempt_id.checked_add(1) != Some(state.live.next_attempt_id)
        {
            let detail = if !fresh_receipt(read.checked, now) { SendCheckDetail::AccountStale }
                else if !read.snapshot.complete { SendCheckDetail::AccountIncomplete }
                else if read.snapshot.identity != run.identity { SendCheckDetail::IdentityMismatch }
                else if read.generations != state.live.generation { SendCheckDetail::GenerationMismatch }
                else { SendCheckDetail::AttemptMismatch };
            self.record_check_at(now, stage, detail);
            bail!("READ_UNAVAILABLE");
        }
        let risk = accounting::risk_exposure(&state.live).ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE"))?;
        let (cost, reserved) = risk.markets.get(&bound.market_id).copied().unwrap_or_default();
        let total = risk.cost.checked_add(risk.reserved).ok_or_else(|| anyhow::anyhow!("FACT_CONFLICT"))?;
        let market = cost.checked_add(reserved).ok_or_else(|| anyhow::anyhow!("FACT_CONFLICT"))?;
        let params = run.options.params;
        if record.r0 < U256::from(accounting::MIN_ORDER_ATOMS) || record.r0 > params.order
            || market > params.market || total > params.total
            || risk.reserved > params.budget.checked_sub(risk.cost).unwrap_or(U256::ZERO)
            || read.snapshot.collateral_atoms.is_none_or(|cash| risk.reserved > cash)
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::SignedBoundUnproven);
            bail!("SIGNED_BOUND_UNPROVEN");
        }
        let gas = read.snapshot.gas_atoms.zip(state.live.baseline.as_ref().and_then(|snapshot| snapshot.gas_atoms))
            .map(|(balance_atoms, initial_atoms)| accounting::GasReading { balance_atoms, initial_atoms });
        if gas.is_none_or(|gas| gas.balance_atoms > gas.initial_atoms)
            || !accounting::check_gas(gas, run.options.gas_policy.minimum_balance_atoms,
                run.options.gas_policy.maximum_consumption_atoms).may_add_risk
        { bail!("GAS_UNAVAILABLE"); }
        let (diagnostic, checked) = recheck_diagnostic(candidate, now, Phase::FinalSend,
            run.phase1.min_edge, run.phase1.min_secs_left);
        if checked.is_none() {
            // The frozen decision's own inputs (the book's and the spot's age,
            // the edge, the round's remaining time) no longer stand. Before the
            // send permission this is a refusal of this candidate only: the
            // attempt is still `Prepared`/`NotSubmitted`, so the venue proved
            // nothing was sent. Under the permission it stops the window.
            let detail = diagnostics::detail_of_core_reason(&diagnostic.reason);
            drop(state);
            self.record_check_at(now, stage, detail);
            return match frozen {
                FrozenInput::Candidate => Err(crate::service::venue::CandidateRefused.into()),
                FrozenInput::Window => bail!("INPUT_CHANGED"),
            };
        }
        let value = accounting::exact_floor_mul(bound.min_net_shares_atoms, candidate.fair())
            .ok_or_else(|| anyhow::anyhow!("SIGNED_BOUND_UNPROVEN"))?;
        let edge = crate::service::predict_fun::controlled::cash_at_limit_atoms(
            bound.request_gross_atoms, run.phase1.min_edge)?;
        if bound.min_net_shares_atoms.is_zero()
            || value.checked_sub(bound.max_cash_atoms).is_none_or(|net| net < edge)
        {
            drop(state);
            self.record_check_at(now, stage, SendCheckDetail::EdgeBelowMin);
            bail!("SIGNED_BOUND_UNPROVEN");
        }
        Ok(diagnostic)
    }

    /// The attempt this guard belongs to.
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    /// Freeze the measurements this attempt's checks report against.
    ///
    /// The tick's account receipt and the candidate's own book and spot
    /// clocks are read once, here, so every later check of the same attempt
    /// reports the same ages.
    pub(super) fn freeze_measurement(&self, account_receipt: Option<crate::service::observation::CheckTime>,
                                    tick_started_elapsed_ms: Option<u64>) {
        let Some(binding) = self.binding.as_ref() else { return; };
        let mut measurement = self.measurement.lock();
        if measurement.is_some() {
            return;
        }
        *measurement = Some(diagnostics::FrozenMeasurement::freeze(
            &binding.candidate, account_receipt, tick_started_elapsed_ms,
        ));
    }

    /// The diagnostic of the first check that failed for this attempt.
    fn diagnostic(&self) -> Option<super::live_events::OrderDiagnostic> {
        let observation = self.diagnostic.lock().clone()?;
        Some(super::live_events::OrderDiagnostic::from_observation(&observation))
    }

    /// Record a send check's outcome.
    ///
    /// The first failure wins: a later success never overwrites the reason
    /// the order did not go out. The clock is passed in because the caller
    /// already holds the State lock this guard is bound to.
    fn record_check_at(&self, now: crate::service::observation::CheckTime,
                       stage: crate::service::venue::SendCheckStage,
                       detail: crate::service::venue::SendCheckDetail) {
        let ages = self.measurement.lock().as_ref()
            .map(|measurement| measurement.ages(now))
            .unwrap_or_default();
        let observation = diagnostics::project(stage, detail, now, ages);
        let mut diagnostic = self.diagnostic.lock();
        if diagnostic.as_ref().is_none_or(|prior| prior.detail == crate::service::venue::SendCheckDetail::None) {
            *diagnostic = Some(observation);
        }
    }

    /// The check's own clock, taken from the run the guard belongs to.
    ///
    /// Only for the callers that do not hold the State lock: the bound
    /// state's lock is not re-entrant.
    fn check_time(&self) -> Option<crate::service::observation::CheckTime> {
        let binding = self.binding.as_ref()?;
        let state = binding.state.lock();
        state.live.controlled.as_ref()
            .and_then(|run| run.phase1.clock.now().ok())
    }

    /// The candidate this guard belongs to.
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    /// Test-only: the state and candidate this guard is bound to.
    #[cfg(test)]
    pub(crate) fn test_binding(&self) -> Option<(Arc<Mutex<State>>, CandidateContext)> {
        self.binding.as_ref()
            .map(|binding| (Arc::clone(&binding.state), binding.candidate.clone()))
    }

    /// The emitter of the run's events.
    pub fn emitter(&self) -> &Arc<LiveEventEmitter> {
        &self.emitter
    }

    /// Record the hash the order is expected to carry.
    pub fn expect_hash(&self, signed_hash: &str) {
        *self.expected_hash.lock() = Some(signed_hash.to_string());
    }

    /// Whether the durable evidence was confirmed.
    pub fn is_acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    /// Whether the order may still be sent.
    pub fn may_send(&self) -> bool {
        matches!(*self.permit.lock(), Some(PermitState::Ready))
    }

    /// Whether the order has already been handed over.
    pub fn was_consumed(&self) -> bool {
        matches!(*self.permit.lock(), Some(PermitState::Consumed))
    }
}

impl ControlledSendGuard for LiveCandidateGuard {
    fn note_send_check(&self, stage: crate::service::venue::SendCheckStage,
        detail: crate::service::venue::SendCheckDetail) {
        if let Some(now) = self.check_time() { self.record_check_at(now, stage, detail); }
    }
    fn check_preparation(&self) -> Result<()> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let mut state = binding.state.lock();
        refresh_live_risk(&mut state);
        let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let now = run.phase1.clock.now()?;
        let stopped = state.halted || state.entry_stop_reason.is_some() || !run.may_add_risk()
            || !run.options.in_window(now.utc);
        if stopped || !binding.candidate.inputs_unchanged()
        {
            drop(state);
            self.record_check_at(now,
                crate::service::venue::SendCheckStage::Prepare,
                if stopped { crate::service::venue::SendCheckDetail::RunStopped }
                else { crate::service::venue::SendCheckDetail::FrozenInput },
            );
            bail!("RUN_STOPPED");
        }
        drop(state);
        #[cfg(test)]
        candidate_tests::preparation_checkpoint(self);
        Ok(())
    }

    /// The receipt-bound account read this attempt prepares from.
    ///
    /// It is the tick's own read: `check_locked` proves its completeness,
    /// identity, ledger generation, attempt binding and ten-second freshness at
    /// every later check, so the adapter does not age the frozen book with a
    /// second full account read.
    fn account_snapshot(&self) -> Result<crate::service::venue::AccountSnapshot> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let state = binding.state.lock();
        state
            .live
            .controlled
            .as_ref()
            .and_then(|run| run.account_read.as_ref())
            .map(|read| read.snapshot.clone())
            .ok_or_else(|| {
                VenueError::new(
                    VenueErrorCode::ReadUnavailable,
                    "controlled_send",
                    "READ_UNAVAILABLE",
                )
                .into()
            })
    }

    fn check_book(&self, book: &crate::models::BookSnapshot) -> Result<()> {
        // The adapter reads this book from the venue inside the guarded
        // preparation, after `check_preparation` proved the run is still in
        // window with unchanged inputs, so it is this run's own fresh read.
        // A book that differs from the evaluation's snapshot is normal market
        // movement, not a different decision: the frozen decision is carried
        // by the candidate's limit, `market_amounts`, `FrozenAttempt::check_order`
        // and `candidate_limit_atoms`, which all still refuse a book that no
        // longer supports it.
        self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        // A book missing a whole side is missing data, not a refused decision:
        // retrying the same decision against it cannot succeed.
        if book.asks.is_empty() || book.bids.is_empty() {
            self.note_send_check(
                crate::service::venue::SendCheckStage::Prepare,
                crate::service::venue::SendCheckDetail::EmptyBook,
            );
            return Err(VenueError::new(
                VenueErrorCode::ReadUnavailable,
                "controlled_send",
                "EMPTY_BOOK",
            )
            .into());
        }
        Ok(())
    }

    /// Check the prepared facts against the frozen candidate.
    ///
    /// The hash the order carries must be the one the attempt prepared, and
    /// the run must still be allowed to add risk. Both are checked here, at
    /// the last moment before the transport sees the order.
    fn check_facts(&self, bound: &SignedExecutionBound) -> anyhow::Result<()> {
        if let Some(binding) = &self.binding {
            let mut state = binding.state.lock();
            // Nothing has been emitted for this order and no acknowledgement
            // slot is waiting yet: a frozen input that no longer stands here is
            // this candidate's refusal, and the transport never saw a byte.
            self.check_locked(&mut state, bound, FrozenInput::Candidate,
                crate::service::venue::SendCheckStage::Facts)?;
            let mut prepared = self.bound.lock();
            if prepared.is_some() { bail!("INPUT_CHANGED"); }
            self.expect_hash(&bound.signed_hash);
            *prepared = Some(bound.clone());
        }
        let expected = self.expected_hash.lock().clone();
        match expected {
            Some(expected) if expected == bound.signed_hash => {}
            Some(_) => {
                return Err(VenueError::new(
                    VenueErrorCode::InvalidInput,
                    "controlled_send",
                    "INPUT_CHANGED",
                )
                .into())
            }
            None => {
                return Err(VenueError::new(
                    VenueErrorCode::InvalidInput,
                    "controlled_send",
                    "SIGNED_BOUND_UNPROVEN",
                )
                .into())
            }
        }
        let state = self.control.lock();
        if !state.may_add_risk() {
            return Err(VenueError::new(
                VenueErrorCode::ExecutionNotEnabled,
                "controlled_send",
                "RUN_STOPPED",
            )
            .into());
        }
        if bound.min_net_shares_atoms.is_zero() {
            return Err(VenueError::new(
                VenueErrorCode::InvalidInput,
                "controlled_send",
                "SIGNED_BOUND_UNPROVEN",
            )
            .into());
        }
        Ok(())
    }

    /// Wait until the critical order's evidence is durable.
    ///
    /// The evidence is written by the run's supervisor and confirmed before
    /// the order may be sent. Without that confirmation the order is not
    /// sent: a signed hash that exists only in memory proves nothing.
    fn await_durable_ack(&self, bound: &SignedExecutionBound) -> anyhow::Result<()> {
        if let Some(binding) = &self.binding {
            use super::live_events::{self, LivePayload, OrderPayload};
            let (event, channel) = {
                let mut state = binding.state.lock();
                // Still before the acknowledgement slot: a frozen input that
                // no longer stands ends this candidate, not the window.
                self.check_locked(&mut state, bound, FrozenInput::Candidate,
                    crate::service::venue::SendCheckStage::Ack)?;
                if self.bound.lock().as_ref() != Some(bound) { bail!("INPUT_CHANGED"); }
                let run = state.live.controlled.as_ref().unwrap();
                let now = run.phase1.clock.now()?;
                let event = self.emitter.prepare(now.utc, Some(bound.market_id.to_string()), Some(bound.token_id.clone()),
                    &live_events::state_accounting(&state), LivePayload::Order(OrderPayload {
                        candidate_id: self.candidate_id.clone(), attempt_id: self.attempt_id.to_string(),
                        signed_hash: Some(bound.signed_hash.clone()), venue_order_id: None,
                        attempt_phase: "PREPARED".into(), submission: "NOT_SUBMITTED".into(),
                        request_gross_atoms: bound.request_gross_atoms.to_string(),
                        max_cash_atoms: bound.max_cash_atoms.to_string(),
                        min_net_shares_atoms: Some(bound.min_net_shares_atoms.to_string()),
                        expires_at: DateTime::from_timestamp(bound.expiration, 0), removal_locked_until: None,
                        reason: "NONE".into(),
                        diagnostic: None,
                    })).map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
                let channel = Arc::clone(&run.channel);
                channel.expect(event.seq, &bound.signed_hash).map_err(|_| anyhow::anyhow!("CONTROL_CHANNEL"))?;
                (event, channel)
            };
            let seq = event.flush().map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
            let started = std::time::Instant::now();
            loop {
                let mut state = binding.state.lock();
                // The acknowledgement slot is already waiting: nothing may be
                // left dangling, so this change stops the window.
                self.check_locked(&mut state, bound, FrozenInput::Window,
                    crate::service::venue::SendCheckStage::Ack)?;
                if channel.is_confirmed(seq, &bound.signed_hash) {
                    self.acked.store(true, Ordering::SeqCst);
                    break;
                }
                if started.elapsed() >= std::time::Duration::from_secs(15) {
                    state.live.controlled.as_mut().unwrap().stop("AUDIT_UNAVAILABLE", true, Utc::now());
                    let now = state.live.controlled.as_ref().unwrap().phase1.clock.now()?;
                    drop(state);
                    self.record_check_at(now,
                        crate::service::venue::SendCheckStage::Ack,
                        crate::service::venue::SendCheckDetail::AckUnavailable,
                    );
                    bail!("AUDIT_UNAVAILABLE");
                }
                drop(state);
                std::thread::park_timeout(std::time::Duration::from_millis(2));
            }
        }
        if !self.acked.load(Ordering::SeqCst) {
            return Err(VenueError::new(
                VenueErrorCode::FactIncomplete,
                "controlled_send",
                "AUDIT_UNAVAILABLE",
            )
            .into());
        }
        let mut permit = self.permit.lock();
        match *permit {
            Some(PermitState::Unprepared) => {
                *permit = Some(PermitState::Ready);
                Ok(())
            }
            Some(PermitState::Ready) => Ok(()),
            Some(PermitState::Consumed) => Err(VenueError::new(
                VenueErrorCode::SubmissionUnknown,
                "controlled_send",
                "SUBMISSION_UNKNOWN",
            )
            .into()),
            None => Err(VenueError::new(
                VenueErrorCode::ExecutionNotEnabled,
                "controlled_send",
                "EXECUTION_NOT_ENABLED",
            )
            .into()),
        }
    }

    fn with_send_permit(
        &self, handle: &crate::service::venue::AttemptHandle,
        first_poll: &mut dyn FnMut(&mut crate::service::venue::AttemptRecord, &mut dyn FnMut() -> Result<()>) -> Result<()>,
    ) -> Result<()> {
        use super::live_events::{self, DecisionPayload, LivePayload};
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let result = (|| {
            if !Arc::ptr_eq(handle, &binding.handle) || !self.is_acked() { bail!("AUDIT_UNAVAILABLE"); }
            let bound = self.bound.lock().clone().ok_or_else(|| anyhow::anyhow!("SIGNED_BOUND_UNPROVEN"))?;
            let mut state = binding.state.lock();
            self.check_locked(&mut state, &bound, FrozenInput::Window,
                crate::service::venue::SendCheckStage::SendPermit)?;
            // State remains locked while waiting for the account gate. Capture
            // accounting here (it reads attempt handles), but defer the core
            // clock/diagnostic to the callback inside the gate, before first poll.
            let accounting = live_events::state_accounting(&state);
            let mut final_event = None;
            let mut record = binding.handle.lock();
            if record.identity.signed_hash.as_deref() != Some(&bound.signed_hash)
                || record.request_gross_atoms != bound.request_gross_atoms
                || record.identity.account != bound.account || record.identity.market_id != bound.market_id
                || record.identity.token_id != bound.token_id || record.r0 < bound.max_cash_atoms
            {
                drop(record);
                let now = state.live.controlled.as_ref().unwrap().phase1.clock.now()?;
                drop(state);
                self.record_check_at(now,
                    crate::service::venue::SendCheckStage::SendPermit,
                    crate::service::venue::SendCheckDetail::IdentityMismatch,
                );
                bail!("INPUT_CHANGED");
            }
            let mut permit = self.permit.lock();
            if *permit != Some(PermitState::Ready) { bail!("SUBMISSION_UNKNOWN"); }
            *permit = Some(PermitState::Consumed);
            // State -> handle -> AccountGate -> first transport poll. No IO,
            // await or post-poll state reconstruction is inserted here.
            let run = state.live.controlled.as_ref().unwrap();
            let min_edge = run.phase1.min_edge;
            let min_secs_left = run.phase1.min_secs_left;
            // The final check's own outcome, kept until the handoff returns:
            // the callback cannot take the State lock again, so the record
            // happens once the closure is done with it.
            let mut permit_detail: Option<crate::service::venue::SendCheckDetail> = None;
            let mut permit_checked = None;
            let mut last_clock_check = || {
                use crate::bot::arbitrage::phase1::{recheck_diagnostic, Phase};
                let now = run.phase1.clock.now()?;
                let (diagnostic, checked) = recheck_diagnostic(&binding.candidate, now, Phase::FinalSend,
                    min_edge, min_secs_left);
                permit_checked = Some(now);
                let reason = diagnostic.reason.clone();
                final_event = Some(self.emitter.prepare(now.utc, Some(bound.market_id.to_string()), None,
                    &accounting, LivePayload::Decision(DecisionPayload { decision: diagnostic }))
                    .map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?);
                if checked.is_none() || !run.options.in_window(now.utc)
                    || !run.may_add_risk() || bound.expiration <= now.utc.timestamp()
                    || !fresh_receipt(run.account_read.as_ref().unwrap().checked, now)
                {
                    // The final check refused: the order was not handed over.
                    // The core's own reason names the frozen input that aged
                    // out; it is not replaced by a more precise guess.
                    let detail = diagnostics::detail_of_core_reason(&reason);
                    permit_detail = Some(if detail == crate::service::venue::SendCheckDetail::Unknown {
                        crate::service::venue::SendCheckDetail::FrozenInput
                    } else { detail });
                    bail!("INPUT_CHANGED");
                }
                Ok(())
            };
            let handoff = first_poll(&mut record, &mut last_clock_check);
            let actual = record.clone();
            drop(permit);
            drop(record);
            // The final check refused, or it completed: the diagnostic says
            // which, and it is taken after the State lock is released.
            let permit_detail = permit_detail.unwrap_or(if handoff.is_ok() {
                crate::service::venue::SendCheckDetail::None
            } else {
                crate::service::venue::SendCheckDetail::SubmissionUnknown
            });
            drop(state);
            if let Some(permit_now) = permit_checked {
                self.record_check_at(permit_now,
                    crate::service::venue::SendCheckStage::SendPermit, permit_detail);
            }
            let state = binding.state.lock();
            if final_event.is_none() {
                if handoff.is_ok() { bail!("AUDIT_UNAVAILABLE"); }
                // A gate refusal can happen without invoking the callback. It
                // never sent; record a current core diagnostic, not an old pass.
                let now = state.live.controlled.as_ref().unwrap().phase1.clock.now()?;
                let (diagnostic, _) = crate::bot::arbitrage::phase1::recheck_diagnostic(
                    &binding.candidate, now, crate::bot::arbitrage::phase1::Phase::FinalSend,
                    min_edge, min_secs_left);
                final_event = Some(self.emitter.prepare(now.utc, Some(bound.market_id.to_string()), None,
                    &accounting, LivePayload::Decision(DecisionPayload { decision: diagnostic }))
                    .map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?);
            }
            let order_event = self.emitter.prepare(
                state.live.controlled.as_ref().unwrap().phase1.clock.now()?.utc,
                Some(bound.market_id.to_string()), Some(bound.token_id.clone()),
                &live_events::state_accounting(&state), LivePayload::Order(live_events::OrderPayload {
                    candidate_id: self.candidate_id.clone(), attempt_id: self.attempt_id.to_string(),
                    signed_hash: actual.identity.signed_hash, venue_order_id: actual.identity.venue_order_id,
                    attempt_phase: live_events::phase_kind(actual.phase).into(),
                    submission: live_events::submission_kind(actual.submission).into(),
                    request_gross_atoms: actual.request_gross_atoms.to_string(), max_cash_atoms: bound.max_cash_atoms.to_string(),
                    min_net_shares_atoms: Some(bound.min_net_shares_atoms.to_string()),
                    expires_at: DateTime::from_timestamp(bound.expiration, 0), removal_locked_until: None,
                    reason: if handoff.is_ok() { "NONE" } else { "INPUT_CHANGED" }.into(),
                    diagnostic: self.diagnostic(),
                })).map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"));
            drop(state);
            final_event.expect("the final callback or gate refusal supplied the diagnostic")
                .flush().map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
            order_event?.flush().map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
            handoff
        })();
        if result.is_err() {
            let mut state = binding.state.lock();
            state.live.controlled.as_mut().unwrap().stop("RECONCILIATION_REQUIRED", true, Utc::now());
        }
        result
    }

    /// The last synchronous permission to hand the order to the transport.
    ///
    /// The permission is consumed here. After this the order may have been
    /// sent, and nothing may pretend it wasn't.
    fn permit_send(&self) -> anyhow::Result<()> {
        if self.binding.is_some() { bail!("EXECUTION_NOT_ENABLED:ATOMIC_HANDOFF_REQUIRED"); }
        let state = self.control.lock();
        if !state.may_add_risk() {
            return Err(VenueError::new(
                VenueErrorCode::ExecutionNotEnabled,
                "controlled_send",
                "RUN_STOPPED",
            )
            .into());
        }
        drop(state);
        let mut permit = self.permit.lock();
        match *permit {
            Some(PermitState::Ready) => {
                *permit = Some(PermitState::Consumed);
                Ok(())
            }
            Some(PermitState::Unprepared) => Err(VenueError::new(
                VenueErrorCode::FactIncomplete,
                "controlled_send",
                "AUDIT_UNAVAILABLE",
            )
            .into()),
            Some(PermitState::Consumed) => Err(VenueError::new(
                VenueErrorCode::SubmissionUnknown,
                "controlled_send",
                "SUBMISSION_UNKNOWN",
            )
            .into()),
            None => Err(VenueError::new(
                VenueErrorCode::ExecutionNotEnabled,
                "controlled_send",
                "EXECUTION_NOT_ENABLED",
            )
            .into()),
        }
    }
}

/// The controlled live state, built for one run.
impl ControlledLiveState {
    /// Build the run's state.
    pub fn new(
        run_id: String,
        phase1: Phase1Run,
        options: ControlledLiveOptions,
        identity: AccountIdentity,
    ) -> Self {
        let output_failed = Arc::clone(&phase1.output_failed);
        let event_seq = Arc::clone(&phase1.event_seq);
        let emitter = Arc::new(LiveEventEmitter::with_shared_state(
            super::live_events::LiveRunIdentity { run_id, risk_window_id: options.risk_window_id.clone() },
            Arc::clone(&event_seq), Arc::clone(&output_failed),
        ));
        let control = Arc::new(Mutex::new(RunControl::new(Arc::clone(&output_failed))));
        let run_deadline_elapsed_ms = phase1.clock.now().ok().and_then(|now| {
            let remaining = (options.deadline_at - now.utc).num_milliseconds();
            (remaining >= 0).then(|| now.elapsed_ms.saturating_add(remaining as u64))
        }).unwrap_or(0);
        Self {
            phase1,
            options,
            identity,
            pending_candidate: None,
            attempt_candidates: HashMap::new(),
            attempt_outcomes: HashMap::new(),
            traded: HashMap::new(),
            control,
            event_seq,
            output_failed,
            account_read: None,
            cash_wait: None,
            order_wait: None,
            run_deadline_elapsed_ms,
            tick_started_elapsed_ms: None,
            opening_claim_cash: U256::ZERO,
            mark_receipts: HashMap::new(),
            loss_latched: false,
            read_failures: None,
            execution_failures: None,
            valuation_failures: None,
            degraded: false,
            loss_trigger: None,
            loss_event_emitted: false,
            stop_event_emitted: false,
            operation_lane: Arc::new(tokio::sync::Mutex::new(())),
            emitter,
            channel: Arc::new(channel::ControlChannel::new()),
            #[cfg(test)]
            preparation: Arc::new(candidate_tests::PreparationHooks::default()),
        }
    }

    /// The operations mode of this run.
    pub fn operations_mode(&self) -> OperationsMode {
        self.options.operations_mode
    }

    /// Stop adding risk, and say why.
    ///
    /// The stop is sticky: once the run has stopped, it never becomes ready
    /// again, whatever the market does.
    pub fn stop(&mut self, reason: &str, takeover_required: bool, at: DateTime<Utc>) {
        if reason == super::live_events::reason::CASH_PENDING && self.cash_wait.is_none() {
            match self.phase1.clock.now() {
                Ok(now) => self.cash_wait = Some(cash_wait::CashWait::begin(now, Vec::new())),
                Err(_) => { self.stop("DATA_UNAVAILABLE", true, at); return; }
            }
        }
        let mut control = self.control.lock();
        let waiting = control.state == LiveRunState::WaitCash;
        control.stop(reason, takeover_required, at);
        if waiting && control.state != LiveRunState::WaitCash { self.stop_event_emitted = false; }
    }

    /// Stop adding risk with the failure code that was already in hand.
    pub fn stop_with_detail(&mut self, reason: &str, detail: &str,
                            takeover_required: bool, at: DateTime<Utc>) {
        let before = self.control.lock().stop.clone();
        self.stop(reason, takeover_required, at);
        let mut control = self.control.lock();
        if before != control.stop {
            if let Some(stop) = control.stop.as_mut() { stop.detail = Some(detail.to_string()); }
        }
    }
}

/// Capture the controlled clock at the official account response, before later awaits.
pub(super) fn account_receipt(state: &State) -> Result<Option<crate::service::observation::CheckTime>> {
    state.live.controlled.as_ref().map(|run| run.phase1.clock.now()).transpose()
}

/// Credit one authoritative claimed payout and release the wait it caused.
///
/// The account read proves the claimed atoms arrived; this is the one place a
/// claimed payout becomes ledger cash. But a claim is only cash when it pays
/// exactly what the confirmed official result owes this ledger: the attempt's
/// own net inventory and the payout the settlement recorded, atom for atom.
/// The judgment comes first: a claim that is one atom off, an attempt whose
/// result is not confirmed, or any other mismatch is not booked as cash at
/// all — it waits for the official result, and the run stays stopped.
///
/// Returns the claim that became ledger cash, and whether this credit
/// cleared a `CASH_PENDING` wait.
pub(super) fn credit_claim(
    state: &mut State, id: u64, inventory: U256, cash: U256,
) -> (Option<(U256, U256)>, bool) {
    use crate::bot::runtime::live_events::reason;
    if state.live.claims.get(&id) == Some(&(inventory, cash)) { return (None, false); }
    // The judgment gate: only a confirmed official result that owes exactly
    // this claim may turn the claim into ledger cash.
    let exact = state.live.controlled.as_ref().is_some_and(|run| {
        run.attempt_outcomes.get(&id).is_some_and(|outcome| {
            outcome.position_net_atoms == inventory && outcome.claim_value_atoms == Some(cash)
        })
    });
    if !exact {
        // Not cash yet: keep the proven claim apart from the ledger's cash.
        state.live.pending_claims.insert(id, (inventory, cash));
        if let Some(run) = state.live.controlled.as_mut() {
            let at = run.phase1.clock.now().map_or_else(|_| chrono::Utc::now(), |now| now.utc);
            run.stop(reason::CASH_PENDING, true, at);
            if let Some(wait) = run.cash_wait.as_mut() { wait.add_pending(id); }
        }
        return (None, false);
    }
    state.live.pending_claims.remove(&id);
    state.live.claims.insert(id, (inventory, cash));
    let Some(run) = state.live.controlled.as_mut() else { return (None, false); };
    if let Some(outcome) = run.attempt_outcomes.get_mut(&id) {
        outcome.cash_confirmed = true;
    }
    // Every holding with inventory must be settled by its own official result:
    // a zero write-off, or this exact claimed cash. A holding without a
    // confirmed result, or with a claim that doesn't pay it exactly, keeps the
    // wait open whatever else landed.
    let settled = {
        let run = state.live.controlled.as_ref().unwrap();
        state.live.net_inventory.iter().all(|(attempt, shares)| {
            shares.is_zero() || run.attempt_outcomes.get(attempt).is_some_and(|outcome| {
                outcome.position_net_atoms == *shares && match outcome.claim_value_atoms {
                    Some(claim) if claim.is_zero() => true,
                    Some(claim) => state.live.claims.get(attempt)
                        .is_some_and(|(held, paid)| *held == *shares && *paid == claim),
                    None => false,
                }
            })
        })
    };
    if !settled || state.live.attempts.values().any(|handle| handle.lock().phase != crate::service::venue::AttemptPhase::Closed)
        || state.live.reservations.values().any(|reserved| !reserved.is_zero()) {
        return (Some((inventory, cash)), false);
    }
    let run = state.live.controlled.as_mut().unwrap();
    cash_wait::observe_budget(run);
    if run.must_end() || run.loss_latched { return (Some((inventory, cash)), false); }
    let mut control = run.control.lock();
    let open_wait = control.state == LiveRunState::WaitCash
        && control.stop.as_ref().is_some_and(|stop| stop.reason == reason::CASH_PENDING);
    if !open_wait { return (Some((inventory, cash)), false); }
    control.state = LiveRunState::Ready;
    control.stop = None;
    drop(control);
    // The wait is over, so a later stop of the same run is its own fact.
    run.stop_event_emitted = false;
    run.cash_wait = None;
    run.pending_candidate = None;
    (Some((inventory, cash)), true)
}

/// Book a proven claim as ledger cash once the official result confirms it.
///
/// The account read proved the venue paid this claim. It becomes cash only
/// when the confirmed official result owes exactly this amount; a claim one
/// atom off stays unbooked and keeps the run waiting for the cash it is owed.
///
/// Returns `true` when this claim was booked.
pub(super) fn promote_pending_claim(state: &mut State, id: u64, inventory: U256, cash: U256)
    -> (Option<(U256, U256)>, bool) {
    if !state
        .live
        .pending_claims
        .get(&id)
        .is_some_and(|(held, paid)| *held == inventory && *paid == cash)
    {
        return (None, false);
    }
    // A tuple alone is not a current-generation account proof. Runtime reads
    // revalidate the full batch before invoking the single credit path.
    let current = state.live.controlled.as_ref().and_then(|run| run.account_read.as_ref())
        .is_some_and(|read| read.next_attempt_id == state.live.next_attempt_id
            && read.generations == state.live.generation);
    if current { credit_claim(state, id, inventory, cash) } else { (None, false) }
}

/// Bind a complete reconciled account response to the same locked ledger version.
pub(super) fn record_account_read(
    state: &mut State,
    snapshot: &crate::service::venue::AccountSnapshot,
    checked: Option<crate::service::observation::CheckTime>,
) -> Result<()> {
    if let Some(checked) = checked {
        crate::service::predict_fun::account::check_snapshot_complete(snapshot)?;
        let read = LiveAccountRead {
            snapshot: snapshot.clone(), checked,
            next_attempt_id: state.live.next_attempt_id,
            generations: state.live.generation.clone(),
        };
        let run = state.live.controlled.as_mut().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        if snapshot.identity != run.identity { bail!("ACCOUNT_MISMATCH"); }
        run.account_read = Some(read);
    }
    Ok(())
}

/// Receipt freshness uses both clocks and includes the original ten-second boundary.
pub(super) fn fresh_receipt(
    receipt: crate::service::observation::CheckTime,
    now: crate::service::observation::CheckTime,
) -> bool {
    (0..=10_000).contains(&now.utc.signed_duration_since(receipt.utc).num_milliseconds())
        && now.elapsed_ms.checked_sub(receipt.elapsed_ms).is_some_and(|age| age <= 10_000)
}

impl LiveAccountRead {
    pub(super) fn matches(&self, state: &LiveState, now: crate::service::observation::CheckTime) -> bool {
        self.next_attempt_id == state.next_attempt_id && self.generations == state.generation
            && fresh_receipt(self.checked, now) && self.snapshot.complete
    }
}

/// Only still-valid original-mid marks participate in a live valuation.
pub(super) fn fresh_marks(state: &State) -> super::live_accounting::Marks {
    let mut marks = super::live_accounting::Marks::default();
    if let Some(run) = state.live.controlled.as_ref() {
        if let Ok(now) = run.phase1.clock.now() {
            marks.prices = state.marks.iter().filter(|(token, _)| {
                run.mark_receipts.get(*token).is_some_and(|receipt| fresh_receipt(*receipt, now))
            }).map(|(token, price)| (token.clone(), *price)).collect();
        }
    }
    marks
}

/// Revalue actual net inventory with fresh original-mid marks and latch loss.
/// Unknown valuation stops risk, but never manufactures a threshold crossing.
///
/// A valuation gap and a fact gap are different failures. A missing fresh
/// mark means the window cannot price what it holds: it stops adding risk and
/// keeps observing, and a fresh mark restores it. A missing fact means the
/// window cannot know what it owns: that keeps its existing stop semantics.
pub(super) fn refresh_live_risk(state: &mut State) {
    use super::live_accounting::{self, AccountingStatus, ProjectionGap, StateFacts};
    if let Some(run) = state.live.controlled.as_mut() {
        super::live::order_wait::observe_budget(run);
    }
    let Some(run) = state.live.controlled.as_ref() else { return; };
    let Ok(now) = run.phase1.clock.now() else {
        state.live.controlled.as_mut().unwrap().stop("DATA_UNAVAILABLE", true, Utc::now());
        return;
    };
    let marks = fresh_marks(state);
    let accounting = live_accounting::project_window(&StateFacts { state: &state.live }, &marks, None);
    let overrun = state.live.attempts.iter().any(|(id, handle)| {
        state.live.observed_risk_floor.get(id).is_some_and(|floor| *floor > handle.lock().r0)
    });
    // A valuation gap is recoverable: the window keeps observing without
    // adding risk, and a fresh mark lets it trade again. A fact gap is not.
    let valuation_gap = accounting.status == AccountingStatus::Incomplete(ProjectionGap::UnknownMark);
    let run = state.live.controlled.as_mut().unwrap();
    if accounting.status == AccountingStatus::Complete && accounting.loss_latched {
        if !run.loss_latched { run.loss_trigger = Some(accounting.net_pnl); }
        run.loss_latched = true;
        run.stop("LOSS_LATCH", true, now.utc);
    }
    if overrun || accounting.status == AccountingStatus::Conflict {
        run.stop("FACT_CONFLICT", true, now.utc);
    } else if valuation_gap {
        // The mark is missing, not the fact: this window keeps watching.
        // The stop itself, when the budget is out, is recorded on the run;
        // the stop event travels with the next lifecycle flush.
        run.note_failure(FailureKind::Valuation, "DATA_UNAVAILABLE", now.utc);
    } else if accounting.status != AccountingStatus::Complete {
        // A missing cash or gas fact is not a missing mark: the window cannot
        // know what it owns, and that keeps its existing stop.
        run.clear_failure(FailureKind::Valuation);
        run.stop("DATA_UNAVAILABLE", true, now.utc);
    } else {
        run.clear_failure(FailureKind::Valuation);
    }
    refresh_live_exposure(state, now.utc);
    super::live::order_wait::recover_after_checks(state);
}

/// Check existing exposure only, without reclassifying an in-flight partial read.
pub(super) fn refresh_live_exposure(state: &mut State, at: DateTime<Utc>) {
    let Some(params) = state.live.controlled.as_ref().map(|run| run.options.params) else { return; };
    // Proposed next orders are not part of the original ledger's risk.
    if super::live_accounting::risk_exposure(&state.live).is_some_and(|risk|
        risk.cost.saturating_add(risk.reserved) >= params.total)
    {
        state.live.controlled.as_mut().unwrap()
            .stop_with_detail("MANUAL_STOP", "TOTAL_CAP", true, at);
    }
}

impl Broker {
    /// The existing controlled run's operation lane; ordinary modes have none.
    pub(super) fn live_operation_lane(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
        self.state.lock().live.controlled.as_ref().map(|run| run.operation_lane.clone())
    }

    /// Update only the new live domain's marks; ordinary live/paper stay unchanged.
    pub(super) fn observe_controlled_live(
        &self, market: &crate::service::venue::TradableMarket,
        book: &crate::models::BookSnapshot,
    ) {
        let receipt = self.state.lock().live.controlled.as_ref()
            .and_then(|run| run.phase1.clock.now().ok());
        self.observe_controlled_live_receipt(market, book, receipt);
    }

    /// Apply an already validated observation without manufacturing a new receipt.
    pub(super) fn observe_controlled_live_at(
        &self, market: &crate::service::venue::TradableMarket,
        book: &crate::models::BookSnapshot, receipt: crate::service::observation::CheckTime,
    ) {
        self.observe_controlled_live_receipt(market, book, Some(receipt));
    }

    fn observe_controlled_live_receipt(
        &self, market: &crate::service::venue::TradableMarket,
        book: &crate::models::BookSnapshot, receipt: Option<crate::service::observation::CheckTime>,
    ) {
        let lane = self.live_operation_lane().unwrap();
        let Ok(_operation) = lane.try_lock() else {
            self.state.lock().live.controlled.as_mut().unwrap().stop("AUDIT_UNAVAILABLE", true, Utc::now());
            return;
        };
        let mut locked = self.state.lock();
        let state = &mut *locked;
        let run = state.live.controlled.as_mut().unwrap();
        if run.emitter.is_finished() { return; }
        let mark = super::paper::mid_of(book).zip(receipt.filter(|receipt| {
            run.phase1.clock.now().is_ok_and(|now| fresh_receipt(*receipt, now))
        }));
        for (token, yes) in [(&market.yes_token, true), (&market.no_token, false)] {
            if let Some((mid, now)) = mark {
                state.marks.insert(token.clone(), if yes { mid } else { 1.0 - mid });
                run.mark_receipts.insert(token.clone(), now);
            } else {
                state.marks.remove(token);
                run.mark_receipts.remove(token);
            }
        }
        refresh_live_risk(state);
        let events = super::live_events::prepare_lifecycle(state);
        drop(locked);
        if let Ok(events) = events { let _ = super::live_events::flush_events(events); }
    }

    /// Record the real core evaluation on the live wire, without paper cash.
    pub(super) fn emit_live_evaluation(
        &self, market: &crate::service::venue::TradableMarket,
        diagnostic: &crate::bot::arbitrage::phase1::DecisionDiagnostic,
        candidate: Option<&CandidateContext>,
    ) -> bool {
        use super::live_events::{self, DecisionPayload, LivePayload};
        let lane = self.state.lock().live.controlled.as_ref().unwrap().operation_lane.clone();
        let Ok(_operation) = lane.try_lock() else {
            self.state.lock().live.controlled.as_mut().unwrap().stop("AUDIT_UNAVAILABLE", true, Utc::now());
            return false;
        };
        let (event, accepted) = {
            let mut state = self.state.lock();
            refresh_live_risk(&mut state);
            let accounting = live_events::state_accounting(&state);
            let run = state.live.controlled.as_mut().unwrap();
            if run.pending_candidate.is_some() {
                run.stop("INPUT_CHANGED", true, diagnostic.checked_at);
                return false;
            }
            let accepted = run.may_add_risk() && candidate.is_none_or(|context| {
                context.diagnostic() == Some(diagnostic) && context.market_id() == market.id
                    && context.source().source == crate::service::spot::SpotSource::Binance
            });
            if accepted {
                if let Some(context) = candidate {
                    run.pending_candidate = Some(PendingLiveCandidate {
                        context: context.clone(), candidate_id: context.candidate_id().into(),
                        worst_cash_atoms: U256::ZERO,
                    });
                }
            } else if candidate.is_some() {
                run.stop("SOURCE_NOT_ALLOWED", true, diagnostic.checked_at);
            }
            (run.emitter.prepare(diagnostic.checked_at, Some(market.id.to_string()), None,
                &accounting, LivePayload::Decision(DecisionPayload { decision: diagnostic.clone() })), accepted)
        };
        let emitted = event.and_then(|event| event.flush()).is_ok();
        accepted && emitted
    }

    /// Same-core PRE_SUBMIT check, with a new clock after the account wait.
    pub(super) fn live_pre_submit(&self, candidate: &CandidateContext) -> anyhow::Result<bool> {
        use super::live_events::{self, DecisionPayload, LivePayload};
        use crate::bot::arbitrage::phase1::{self, Phase};
        let (events, accepted) = {
            let mut state = self.state.lock();
            refresh_live_risk(&mut state);
            let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
            let now = run.phase1.clock.now()?;
            // Reuse the tick's authoritative read without dropping its audit
            // projection or inventing a new receipt for the cached response.
            let mut events = Vec::new();
            if let Some(read) = run.account_read.as_ref().filter(|read| read.matches(&state.live, now)) {
                if let Some(event) = live_events::prepare_reconciliation(&state,
                    &read.snapshot, read.checked.utc, "RUNTIME", true, "NONE")
                    .map_err(anyhow::Error::msg)? {
                    events.push(event);
                }
            }
            let (diagnostic, checked) = phase1::recheck_diagnostic(candidate, now, Phase::PreSubmit,
                run.phase1.min_edge, run.phase1.min_secs_left);
            let accepted = checked.is_some() && run.may_add_risk();
            events.push(run.emitter.prepare(now.utc, Some(candidate.market_id().to_string()), None,
                &live_events::state_accounting(&state), LivePayload::Decision(DecisionPayload { decision: diagnostic }))
                .map_err(anyhow::Error::msg)?);
            (events, accepted)
        };
        live_events::flush_events(events).map_err(anyhow::Error::msg)?;
        Ok(accepted)
    }

    /// Install the controlled live run's own state.
    ///
    /// The run must be a live run with a controlled identity, and it must not
    /// have a paper cash: there is one ledger, and it is the live one.
    pub fn with_controlled_live(
        mut self,
        options: crate::config::controlled_live::ControlledLiveOptions,
        identity: crate::service::venue::AccountIdentity,
        min_edge: f64,
        min_secs_left: f64,
    ) -> Result<Self> {
        if !self.live {
            bail!("CONTROLLED_MODE: a controlled live run must be a live run");
        }
        if self.controlled.is_none() {
            bail!("CONTROLLED_MODE: a controlled live run needs its controlled identity");
        }
        if self.state.lock().paper.is_some() {
            bail!("CONTROLLED_MODE: a controlled live run has no paper cash");
        }
        let phase1 = crate::bot::runtime::phase1_control::Phase1Run::new(
            Arc::new(crate::service::observation::SystemClock::new()?),
            crate::bot::arbitrage::phase1::StrategyContract::Phase1NetShares,
            min_edge,
            min_secs_left,
        );
        let controlled = crate::bot::runtime::live_control::ControlledLiveState::new(
            self.controlled.as_ref().unwrap().run_id.clone(), phase1, options, identity);
        self.state.lock().live.controlled = Some(controlled);
        Ok(self)
    }
}

/// Attach the explicit context to the original Ctx broker and original account gate.
pub(super) fn install_from_config(broker: Broker, cfg: &crate::config::AppConfig) -> Result<Broker> {
    let Some(options) = cfg.controlled.as_ref().and_then(|run| run.live.clone()) else {
        return Ok(broker);
    };
    let expected = options.account_identity.as_ref()
        .ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: ACCOUNT"))?;
    let identity = broker.identity.clone().ok_or_else(|| anyhow::anyhow!("CONTROLLED_MODE: ACCOUNT"))?;
    if broker.gate.is_none() || identity.venue != crate::models::VenueId::PredictFun
        || identity.chain_id != 56 || identity.mode != crate::service::venue::AccountMode::PredictAccount
        || identity.trading_address.to_ascii_lowercase() != expected.trading_address.to_ascii_lowercase()
        || identity.signer_address.to_ascii_lowercase() != expected.signer_address.to_ascii_lowercase()
    {
        bail!("CONTROLLED_MODE: ACCOUNT");
    }
    broker.with_controlled_live(options, identity,
        cfg.strategies.btc_arb.min_edge, cfg.strategies.btc_arb.min_secs_left as f64)
}

/// Register exactly one stdin reader, using the channel owned by this same run.
pub(super) fn watch_broker(broker: Arc<Broker>) {
    let channel = broker.state.lock().live.controlled.as_ref().map(|run| Arc::clone(&run.channel));
    if let Some(channel) = channel {
        channel::watch_control_channel(channel, broker);
    } else {
        super::paper::watch_control_channel(broker);
    }
}

impl State {
    /// The controlled live state of this run, when it has one.
    pub fn controlled_live(&self) -> Option<&crate::bot::runtime::live_control::ControlledLiveState> {
        self.live.controlled.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bot::arbitrage::phase1::StrategyContract;
    use crate::bot::runtime::live_events::LiveRunIdentity;
    use crate::config::controlled_live::{GasPolicy, OperationsMode};
    use crate::service::observation::SystemClock;
    use crate::service::venue::AccountMode;
    use chrono::Utc;
    use std::sync::Arc;

    fn identity() -> AccountIdentity {
        AccountIdentity {
            chain_id: 56,
            trading_address: "0x1111111111111111111111111111111111111111".into(),
            mode: AccountMode::PredictAccount,
            signer_address: "0x1111111111111111111111111111111111111111".into(),
            venue: crate::models::VenueId::PredictFun,
        }
    }

    fn options() -> ControlledLiveOptions {
        ControlledLiveOptions {
            account_identity: None,
            contract: crate::config::controlled_live::LIVE_CONTRACT.into(),
            operations_mode: OperationsMode::Supervised,
            risk_window_id: "window-1".into(),
            authorization_ref: "auth-1".into(),
            deadline_at: Utc::now() + chrono::Duration::seconds(3600),
            budget_secs: 3600,
            params: crate::config::RunParameters::legacy(),
            gas_policy: GasPolicy {
                payer_address: "0x1111111111111111111111111111111111111111".into(),
                minimum_balance_atoms: U256::ZERO,
                maximum_consumption_atoms: U256::ZERO,
            },
        }
    }

    fn phase1() -> Phase1Run {
        Phase1Run::new(
            Arc::new(SystemClock::new().unwrap()),
            StrategyContract::Phase1NetShares,
            0.03,
            5.0,
        )
    }

    fn bound() -> SignedExecutionBound {
        SignedExecutionBound {
            account: identity(),
            market_id: 1,
            token_id: "2".into(),
            route: "ctf".into(),
            fee_rate_bps: 200,
            request_gross_atoms: U256::from(5_000_000_000_000_000_000u128),
            gross_cash_atoms: U256::from(2_000_000_000_000_000_000u128),
            max_cash_atoms: U256::from(2_000_000_000_000_000_000u128),
            min_net_shares_atoms: U256::from(4_900_000_000_000_000_000u128),
            expiration: 0,
            signed_hash: format!("0x{}", "a".repeat(64)),
        }
    }

    fn guard(control: Arc<Mutex<RunControl>>) -> Arc<LiveCandidateGuard> {
        Arc::new(LiveCandidateGuard::new(
            control,
            1,
            "1".into(),
            Arc::new(LiveEventEmitter::new(LiveRunIdentity {
                run_id: "run-1".into(),
                risk_window_id: "window-1".into(),
            })),
        ))
    }

    fn control() -> Arc<Mutex<RunControl>> {
        let state = ControlledLiveState::new("run-1".into(), phase1(), options(), identity());
        Arc::clone(&state.control)
    }

    #[test]
    fn a_stop_is_sticky_and_never_returns_to_ready() {
        let mut state = ControlledLiveState::new("run-1".into(), phase1(), options(), identity());
        assert!(state.control.lock().may_add_risk());
        state.stop("DEADLINE", true, Utc::now());
        assert!(!state.control.lock().may_add_risk());
        assert_eq!(
            state.control.lock().state,
            LiveRunState::StopNewRisk
        );
        // A later observation can't undo it.
        assert!(!state.control.lock().may_add_risk());
    }

    #[test]
    fn a_cash_pending_stop_is_its_own_state() {
        let mut state = ControlledLiveState::new("run-1".into(), phase1(), options(), identity());
        state.stop(
            crate::bot::runtime::live_events::reason::CASH_PENDING,
            true,
            Utc::now(),
        );
        assert_eq!(state.control.lock().state, LiveRunState::WaitCash);
    }

    #[test]
    fn a_guard_without_a_hash_refuses_the_facts() {
        let guard = guard(control());
        assert!(guard.check_facts(&bound()).is_err());
    }

    #[test]
    fn a_guard_with_the_wrong_hash_refuses_the_facts() {
        let guard = guard(control());
        guard.expect_hash(&format!("0x{}", "b".repeat(64)));
        assert!(guard.check_facts(&bound()).is_err());
    }

    #[test]
    fn the_order_needs_the_durable_evidence_before_it_may_be_sent() {
        let guard = guard(control());
        guard.expect_hash(&bound().signed_hash);
        assert!(guard.check_facts(&bound()).is_ok());
        // Without the acknowledgement there is no permission.
        assert!(guard.await_durable_ack(&bound()).is_err());
        assert!(!guard.may_send());
    }

    #[test]
    fn a_consumed_permission_is_never_given_twice() {
        let guard = guard(control());
        guard.expect_hash(&bound().signed_hash);
        guard.acked.store(true, Ordering::SeqCst);
        assert!(guard.await_durable_ack(&bound()).is_ok());
        assert!(guard.permit_send().is_ok());
        assert!(guard.was_consumed());
        // A second attempt is unknown, not a fresh send.
        assert!(guard.permit_send().is_err());
    }

    #[test]
    fn a_stopped_run_cannot_take_a_permission() {
        let control = control();
        let guard = guard(Arc::clone(&control));
        guard.expect_hash(&bound().signed_hash);
        guard.acked.store(true, Ordering::SeqCst);
        assert!(guard.await_durable_ack(&bound()).is_ok());
        control.lock().stop("DEADLINE", true, Utc::now());
        assert!(guard.permit_send().is_err());
    }
}
