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
    /// Whether a human has to take the remaining risk over.
    pub takeover_required: bool,
    /// The moment the stop was set.
    pub at: DateTime<Utc>,
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
    /// Receipt times of valid original-mid marks, invalidated on a bad book.
    pub mark_receipts: HashMap<String, crate::service::observation::CheckTime>,
    /// Monotone within this risk window; a recovered valuation cannot clear it.
    pub loss_latched: bool,
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

    /// Stop adding risk, and say why.
    ///
    /// The stop is sticky: once the run has stopped, it never becomes ready
    /// again, whatever the market does.
    pub fn stop(&mut self, reason: &str, takeover_required: bool, at: DateTime<Utc>) {
        if self.state == LiveRunState::Ready {
            self.state = if reason == crate::bot::runtime::live_events::reason::CASH_PENDING {
                LiveRunState::WaitCash
            } else {
                LiveRunState::StopNewRisk
            };
        }
        if self.stop.is_none() {
            self.stop = Some(LiveStop {
                reason: reason.to_string(),
                takeover_required,
                at,
            });
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
    pub fn may_add_risk(&self) -> bool {
        self.control.lock().may_add_risk()
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
        }
    }

    pub(super) fn bind(
        mut self, state: Arc<Mutex<State>>, candidate: CandidateContext,
        handle: crate::service::venue::AttemptHandle, route: String,
    ) -> Self {
        self.binding = Some(GuardBinding { state, candidate, handle, route });
        self
    }

    /// Project the actual returned handle and immutable signed proof on the same lane.
    pub(super) fn emit_order_result(
        &self, previous_reservation: U256, event_reason: &str,
        metadata: Option<&crate::service::venue::ControlledOrderMetadata>,
    ) -> Result<()> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let bound = self.bound.lock().clone();
        let events = {
            let mut state = binding.state.lock();
            let mut events = Vec::new();
            if let Some(event) = super::live_events::prepare_order(&state, self.attempt_id,
                bound.as_ref(), metadata, event_reason).map_err(anyhow::Error::msg)?
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
    fn check_locked(&self, state: &mut State, bound: &SignedExecutionBound) -> Result<crate::bot::arbitrage::phase1::DecisionDiagnostic> {
        use super::live_accounting as accounting;
        use crate::service::venue::{AttemptPhase, Submission};
        use crate::bot::arbitrage::phase1::{recheck_diagnostic, Phase};
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        refresh_live_risk(state);
        let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let now = run.phase1.clock.now()?;
        let candidate = &binding.candidate;
        let record = binding.handle.lock().clone();
        let read = run.account_read.as_ref().ok_or_else(|| anyhow::anyhow!("READ_UNAVAILABLE"))?;
        if state.halted || state.entry_stop_reason.is_some() || !run.may_add_risk()
            || !run.options.in_window(now.utc) || run.loss_latched
        { bail!("RUN_STOPPED"); }
        if !Arc::ptr_eq(&self.emitter, &run.emitter)
            || !state.live.attempts.get(&self.attempt_id).is_some_and(|handle| Arc::ptr_eq(handle, &binding.handle))
            || run.attempt_candidates.get(&self.attempt_id) != Some(&self.candidate_id)
            || !run.pending_candidate.as_ref().is_some_and(|pending| pending.context.same_candidate(candidate)
                && pending.worst_cash_atoms == record.r0)
            || record.identity.local_attempt_id != self.attempt_id
            || record.identity.signed_hash.as_deref() != Some(&bound.signed_hash)
            || record.identity.account != run.identity || bound.account != run.identity
            || record.identity.market_id != candidate.market_id() || bound.market_id != candidate.market_id()
            || record.identity.token_id != candidate.token() || bound.token_id != candidate.token()
            || record.identity.side != crate::models::Side::Buy || bound.route != binding.route
            || bound.fee_rate_bps != candidate.fee_rate_bps()
            || record.request_gross_atoms != bound.request_gross_atoms
            || bound.request_gross_atoms != crate::service::predict_fun::controlled::quantity_atoms(candidate.gross_shares())?
            || accounting::minimum_net_shares(bound.request_gross_atoms, bound.fee_rate_bps) != Some(bound.min_net_shares_atoms)
            || bound.fee_rate_bps > crate::service::predict_fun::controlled::MAX_SIGNED_FEE_BPS
            || record.phase != AttemptPhase::Prepared || record.submission != Submission::NotSubmitted
            || bound.max_cash_atoms != bound.gross_cash_atoms || bound.max_cash_atoms > record.r0
            || bound.expiration <= now.utc.timestamp()
        { bail!("INPUT_CHANGED"); }
        if !fresh_receipt(read.checked, now) || !read.snapshot.complete
            || read.snapshot.identity != run.identity || read.generations != state.live.generation
            || read.next_attempt_id.checked_add(1) != Some(state.live.next_attempt_id)
        { bail!("READ_UNAVAILABLE"); }
        let risk = accounting::risk_exposure(&state.live).ok_or_else(|| anyhow::anyhow!("FACT_INCOMPLETE"))?;
        let (cost, reserved) = risk.markets.get(&bound.market_id).copied().unwrap_or_default();
        let total = risk.cost.checked_add(risk.reserved).ok_or_else(|| anyhow::anyhow!("FACT_CONFLICT"))?;
        let market = cost.checked_add(reserved).ok_or_else(|| anyhow::anyhow!("FACT_CONFLICT"))?;
        if record.r0 < U256::from(accounting::ONE) || record.r0 > accounting::max_order_atoms()
            || market > accounting::max_market_atoms() || total > accounting::max_total_atoms()
            || risk.reserved > accounting::budget_c0_atoms().checked_sub(risk.cost).unwrap_or(U256::ZERO)
            || read.snapshot.collateral_atoms.is_none_or(|cash| risk.reserved > cash)
        { bail!("SIGNED_BOUND_UNPROVEN"); }
        let gas = read.snapshot.gas_atoms.zip(state.live.baseline.as_ref().and_then(|snapshot| snapshot.gas_atoms))
            .map(|(balance_atoms, initial_atoms)| accounting::GasReading { balance_atoms, initial_atoms });
        if gas.is_none_or(|gas| gas.balance_atoms > gas.initial_atoms)
            || !accounting::check_gas(gas, run.options.gas_policy.minimum_balance_atoms,
                run.options.gas_policy.maximum_consumption_atoms).may_add_risk
        { bail!("GAS_UNAVAILABLE"); }
        let (diagnostic, checked) = recheck_diagnostic(candidate, now, Phase::FinalSend,
            run.phase1.min_edge, run.phase1.min_secs_left);
        if checked.is_none() { bail!("INPUT_CHANGED"); }
        let value = accounting::exact_floor_mul(bound.min_net_shares_atoms, candidate.fair())
            .ok_or_else(|| anyhow::anyhow!("SIGNED_BOUND_UNPROVEN"))?;
        let edge = crate::service::predict_fun::controlled::cash_at_limit_atoms(
            bound.request_gross_atoms, run.phase1.min_edge)?;
        if bound.min_net_shares_atoms.is_zero()
            || value.checked_sub(bound.max_cash_atoms).is_none_or(|net| net < edge)
        { bail!("SIGNED_BOUND_UNPROVEN"); }
        Ok(diagnostic)
    }

    /// The attempt this guard belongs to.
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
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
    fn check_preparation(&self) -> Result<()> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let mut state = binding.state.lock();
        refresh_live_risk(&mut state);
        let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let now = run.phase1.clock.now()?;
        if state.halted || state.entry_stop_reason.is_some() || !run.may_add_risk()
            || !run.options.in_window(now.utc) || !binding.candidate.inputs_unchanged()
        { bail!("RUN_STOPPED"); }
        drop(state);
        #[cfg(test)]
        candidate_tests::preparation_checkpoint(self);
        Ok(())
    }

    fn check_book(&self, book: &crate::models::BookSnapshot) -> Result<()> {
        let binding = self.binding.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
        let frozen = binding.candidate.frozen_book();
        let same = |left: &[crate::models::BookLevel], right: &[crate::models::BookLevel]| {
            left.iter().map(|level| (level.price, level.size))
                .eq(right.iter().map(|level| (level.price, level.size)))
        };
        if !same(&frozen.asks, &book.asks) || !same(&frozen.bids, &book.bids) { bail!("INPUT_CHANGED"); }
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
            self.check_locked(&mut state, bound)?;
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
                self.check_locked(&mut state, bound)?;
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
                    })).map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
                let channel = Arc::clone(&run.channel);
                channel.expect(event.seq, &bound.signed_hash).map_err(|_| anyhow::anyhow!("CONTROL_CHANNEL"))?;
                (event, channel)
            };
            let seq = event.flush().map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
            let started = std::time::Instant::now();
            loop {
                let mut state = binding.state.lock();
                self.check_locked(&mut state, bound)?;
                if channel.is_confirmed(seq, &bound.signed_hash) {
                    self.acked.store(true, Ordering::SeqCst);
                    break;
                }
                if started.elapsed() >= std::time::Duration::from_secs(15) {
                    state.live.controlled.as_mut().unwrap().stop("AUDIT_UNAVAILABLE", true, Utc::now());
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
            self.check_locked(&mut state, &bound)?;
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
            { bail!("INPUT_CHANGED"); }
            let mut permit = self.permit.lock();
            if *permit != Some(PermitState::Ready) { bail!("SUBMISSION_UNKNOWN"); }
            *permit = Some(PermitState::Consumed);
            // State -> handle -> AccountGate -> first transport poll. No IO,
            // await or post-poll state reconstruction is inserted here.
            let run = state.live.controlled.as_ref().unwrap();
            let mut last_clock_check = || {
                use crate::bot::arbitrage::phase1::{recheck_diagnostic, Phase};
                let now = run.phase1.clock.now()?;
                let (diagnostic, checked) = recheck_diagnostic(&binding.candidate, now, Phase::FinalSend,
                    run.phase1.min_edge, run.phase1.min_secs_left);
                final_event = Some(self.emitter.prepare(now.utc, Some(bound.market_id.to_string()), None,
                    &accounting, LivePayload::Decision(DecisionPayload { decision: diagnostic }))
                    .map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?);
                if checked.is_none() || !run.options.in_window(now.utc)
                    || !run.may_add_risk() || bound.expiration <= now.utc.timestamp()
                    || !fresh_receipt(run.account_read.as_ref().unwrap().checked, now)
                { bail!("INPUT_CHANGED"); }
                Ok(())
            };
            let handoff = first_poll(&mut record, &mut last_clock_check);
            let actual = record.clone();
            drop(permit);
            drop(record);
            if final_event.is_none() {
                if handoff.is_ok() { bail!("AUDIT_UNAVAILABLE"); }
                // A gate refusal can happen without invoking the callback. It
                // never sent; record a current core diagnostic, not an old pass.
                let now = run.phase1.clock.now()?;
                let (diagnostic, _) = crate::bot::arbitrage::phase1::recheck_diagnostic(
                    &binding.candidate, now, crate::bot::arbitrage::phase1::Phase::FinalSend,
                    run.phase1.min_edge, run.phase1.min_secs_left);
                final_event = Some(self.emitter.prepare(now.utc, Some(bound.market_id.to_string()), None,
                    &accounting, LivePayload::Decision(DecisionPayload { decision: diagnostic }))
                    .map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?);
            }
            let order_event = self.emitter.prepare(run.phase1.clock.now()?.utc,
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
            mark_receipts: HashMap::new(),
            loss_latched: false,
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
        self.control
            .lock()
            .stop(reason, takeover_required, at);
    }
}

/// Capture the controlled clock at the official account response, before later awaits.
pub(super) fn account_receipt(state: &State) -> Result<Option<crate::service::observation::CheckTime>> {
    state.live.controlled.as_ref().map(|run| run.phase1.clock.now()).transpose()
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
pub(super) fn refresh_live_risk(state: &mut State) {
    use super::live_accounting::{self, AccountingStatus, StateFacts};
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
    let run = state.live.controlled.as_mut().unwrap();
    if accounting.status == AccountingStatus::Complete && accounting.loss_latched {
        if !run.loss_latched { run.loss_trigger = Some(accounting.net_pnl); }
        run.loss_latched = true;
        run.stop("LOSS_LATCH", true, now.utc);
    }
    if overrun || accounting.status == AccountingStatus::Conflict {
        run.stop("FACT_CONFLICT", true, now.utc);
    } else if accounting.status != AccountingStatus::Complete {
        run.stop("DATA_UNAVAILABLE", true, now.utc);
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
        let lane = self.live_operation_lane().unwrap();
        let Ok(_operation) = lane.try_lock() else {
            self.state.lock().live.controlled.as_mut().unwrap().stop("AUDIT_UNAVAILABLE", true, Utc::now());
            return;
        };
        let mut locked = self.state.lock();
        let state = &mut *locked;
        let run = state.live.controlled.as_mut().unwrap();
        if run.emitter.is_finished() { return; }
        let mark = super::paper::mid_of(book).zip(run.phase1.clock.now().ok());
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
        let (event, accepted) = {
            let mut state = self.state.lock();
            refresh_live_risk(&mut state);
            let run = state.live.controlled.as_ref().ok_or_else(|| anyhow::anyhow!("CONTEXT_MISSING"))?;
            let now = run.phase1.clock.now()?;
            let (diagnostic, checked) = phase1::recheck_diagnostic(candidate, now, Phase::PreSubmit,
                run.phase1.min_edge, run.phase1.min_secs_left);
            let accepted = checked.is_some() && run.may_add_risk();
            let event = run.emitter.prepare(now.utc, Some(candidate.market_id().to_string()), None,
                &live_events::state_accounting(&state), LivePayload::Decision(DecisionPayload { decision: diagnostic }));
            (event, accepted)
        };
        event.and_then(|event| event.flush()).map_err(|_| anyhow::anyhow!("OUTPUT_INVALID"))?;
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
