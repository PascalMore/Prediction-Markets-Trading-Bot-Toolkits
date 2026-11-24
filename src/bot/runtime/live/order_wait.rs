//! The bounded wait for an accepted order's complete execution facts.
//!
//! A venue that has accepted an order and then reports a complete reading of
//! zero fills has not lost the order: its facts are still arriving. That
//! state has its own lifetime, derived from the order's own signed expiry,
//! and it is bounded on both clocks exactly like every other wait in this
//! runtime.
//!
//! Nothing here owns a ledger, a balance or a release. The order's facts are
//! still applied by the original reading path, and only the original Final
//! path still releases the risk. A truly missing fact keeps the runtime's
//! existing unread-facts budget, and an unknown submission keeps its own.

use crate::bot::runtime::live_control::ControlledLiveState;
use crate::bot::runtime::{Broker, State};
use crate::service::observation::CheckTime;
use crate::service::venue::{AttemptPhase, Closure, ExecutionSnapshot, Submission};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// The missing-facts budget this wait reuses, in milliseconds.
///
/// It is the runtime's existing unread-facts budget, referenced and not
/// reinvented: a wait may not be longer than the cleanup it lives inside.
pub(crate) const MISSING_FACTS_BUDGET_MS: u64 = 120_000;

/// The metadata of one accepted order's bounded wait.
///
/// It holds no quantity, no cash and no release: the ledger's own atomic
/// facts remain the only truth about the order.
#[derive(Debug, Clone)]
pub struct OrderWait {
    /// The attempt this wait is about.
    pub attempt_id: u64,
    /// Frozen order identity; the enclosing ControlledLiveState owns run/window scope.
    pub identity: Option<crate::service::venue::OrderIdentity>,
    /// Last successfully consumed execution generation, not a second ledger.
    pub generation: u64,
    /// Authoritative Final consumed; recovery still needs current account and marks.
    pub final_generation: Option<u64>,
    /// The moment this wait was created, on both clocks.
    ///
    /// It is set once, at the first reading of the order, and never
    /// re-anchored by a later reading.
    pub created: Option<CheckTime>,
    /// The moment the pending phase began, on both clocks (`W0`).
    ///
    /// It is set once, at the first complete reading of an accepted order,
    /// and never re-anchored by a later reading.
    pub started: Option<CheckTime>,
    /// The signed expiry, in UTC milliseconds (`E`).
    pub expires_at_ms: Option<i64>,
    /// The end of the pending phase on the elapsed clock (`P_m`).
    pub pending_until_elapsed_ms: Option<u64>,
    /// The end of the bounded observation on the wall clock (`T_w`).
    pub wall_limit_ms: Option<i64>,
    /// The end of the bounded observation on the elapsed clock (`T_m`).
    pub elapsed_limit_ms: Option<u64>,
    /// The start of the current run of truly missing facts (`U0`).
    pub missing_since: Option<CheckTime>,
    /// A true transport/read failure keeps its original READ_UNAVAILABLE stop.
    pub missing_read_error: bool,
    /// The last clock reading this wait compared itself against.
    pub last_check: Option<CheckTime>,
    /// Whether the run still owns an order whose facts are not final.
    pub pending: bool,
    /// Whether the bounded observation is over.
    pub exhausted: bool,
}

impl OrderWait {
    /// Begin tracking one accepted order, before its lifetime is known.
    ///
    /// The lifetime itself is only granted at the first complete reading, and
    /// only when the order's own signed expiry proves it. Until then the order
    /// keeps the runtime's existing missing-facts budget.
    pub fn begin(attempt_id: u64) -> Self {
        Self {
            attempt_id,
            identity: None,
            generation: 0,
            final_generation: None,
            created: None,
            started: None,
            expires_at_ms: None,
            pending_until_elapsed_ms: None,
            wall_limit_ms: None,
            elapsed_limit_ms: None,
            missing_since: None,
            missing_read_error: false,
            last_check: None,
            pending: false,
            exhausted: false,
        }
    }

    /// Grant the order its lifetime, from its own signed expiry.
    ///
    /// A missing, bad or already past expiry grants nothing: such an order
    /// keeps the budget it already had and is never given a speculative one.
    fn grant_lifetime(&mut self, now: CheckTime, expiration_seconds: i64) {
        if self.has_lifetime() || expiration_seconds <= 0
            || DateTime::<Utc>::from_timestamp(expiration_seconds, 0).is_none()
        {
            return;
        }
        let Some(expires_at_ms) = expiration_seconds.checked_mul(1000) else { return };
        let Some(remaining) = expires_at_ms.checked_sub(now.utc.timestamp_millis()) else {
            return;
        };
        if remaining <= 0 {
            return;
        }
        let Some(pending_until_elapsed_ms) = now.elapsed_ms.checked_add(remaining as u64) else {
            return;
        };
        let Some(wall_limit_ms) = expires_at_ms.checked_add(MISSING_FACTS_BUDGET_MS as i64) else {
            return;
        };
        let Some(elapsed_limit_ms) = pending_until_elapsed_ms.checked_add(MISSING_FACTS_BUDGET_MS)
        else {
            return;
        };
        self.started = Some(now);
        self.expires_at_ms = Some(expires_at_ms);
        self.pending_until_elapsed_ms = Some(pending_until_elapsed_ms);
        self.wall_limit_ms = Some(wall_limit_ms);
        self.elapsed_limit_ms = Some(elapsed_limit_ms);
    }

    /// Whether this order has a lifetime of its own.
    pub fn has_lifetime(&self) -> bool {
        self.started.is_some()
    }

    /// Whether the order is still in its pending phase.
    ///
    /// Either clock reaching its bound ends the phase: an equality is not a
    /// recovery point.
    pub fn in_pending(&self, now: CheckTime) -> bool {
        match (self.expires_at_ms, self.pending_until_elapsed_ms) {
            (Some(expires_at_ms), Some(pending_until_elapsed_ms)) => {
                now.utc.timestamp_millis() < expires_at_ms
                    && now.elapsed_ms < pending_until_elapsed_ms
            }
            _ => false,
        }
    }

    /// Whether the bounded observation of this order may still continue.
    ///
    /// The earlier of the wait's own bound and the run's deadline wins, on
    /// both clocks. An order without a lifetime has no bound of its own: its
    /// missing facts keep the runtime's existing budget.
    pub fn may_observe(
        &self,
        now: CheckTime,
        deadline_at: DateTime<Utc>,
        run_deadline_elapsed_ms: u64,
    ) -> bool {
        if self.exhausted || self.regressed(now) || self.missing_expired(now, MISSING_FACTS_BUDGET_MS) {
            return false;
        }
        if now.utc >= deadline_at || now.elapsed_ms >= run_deadline_elapsed_ms {
            return false;
        }
        match (self.wall_limit_ms, self.elapsed_limit_ms) {
            (Some(wall_limit_ms), Some(elapsed_limit_ms)) => {
                now.utc.timestamp_millis() < wall_limit_ms
                    && now.elapsed_ms < elapsed_limit_ms
            }
            _ => true,
        }
    }

    /// Whether a clock reading went backwards against this wait.
    ///
    /// A wall clock that moved back or an elapsed clock that lost time is not
    /// a new anchor: the wait fails closed instead of re-anchoring itself.
    pub fn regressed(&self, now: CheckTime) -> bool {
        match self.last_check.or(self.created) {
            Some(last) => now.utc < last.utc || now.elapsed_ms < last.elapsed_ms,
            None => false,
        }
    }

    fn missing_expired(&self, now: CheckTime, budget_ms: u64) -> bool {
        self.missing_since.is_some_and(|since| {
            let wall = since.utc.timestamp_millis().checked_add_unsigned(budget_ms);
            let elapsed = since.elapsed_ms.checked_add(budget_ms);
            wall.is_none_or(|end| now.utc.timestamp_millis() >= end)
                || elapsed.is_none_or(|end| now.elapsed_ms >= end)
        })
    }
}

/// Whether this run observes an accepted order whose facts are complete.
///
/// A pending order is not a cash wait: the two are told apart, and both of
/// them only observe.
pub(crate) fn waiting(broker: &Broker) -> bool {
    broker.state.lock().live.controlled.as_ref().is_some_and(|run| run.order_wait.is_some())
}

/// Note what one reading proves about the wait of the order it belongs to.
///
/// A complete reading of an accepted order is a fact in progress: it opens
/// the wait once and never re-anchors it. A reading that is not complete, or
/// that does not prove an acceptance, is a missing fact: it starts the
/// missing streak and leaves the wait's own bound untouched.
pub(crate) fn note_snapshot(
    state: &mut State,
    snapshot: &ExecutionSnapshot,
    applied_complete: bool,
    expiration: Option<i64>,
) {
    if state.live.controlled.is_none() {
        return;
    }
    let id = snapshot.identity.local_attempt_id;
    let owners: Vec<_> = state.live.attempts.values().filter_map(|handle| {
        let record = handle.lock();
        (record.phase != AttemptPhase::Closed
            && matches!(record.submission, Submission::Accepted | Submission::Unknown))
            .then(|| record.identity.clone())
    }).collect();
    let Some(run) = state.live.controlled.as_mut() else { return; };
    observe_budget(run);
    // A real Final may still update the original ledger after a hard stop,
    // but it never erases an exhausted wait or reopens the run.
    if run.must_end() { return; }
    if matches!(snapshot.closure, Closure::Final { .. }) {
        if let Some(wait) = run.order_wait.as_mut().filter(|wait| {
            applied_complete && wait.identity.as_ref() == Some(&snapshot.identity)
                && snapshot.generation > wait.generation
        }) {
            wait.generation = snapshot.generation;
            wait.final_generation = Some(snapshot.generation);
            wait.pending = false;
            if wait.has_lifetime() { wait.missing_since = None; }
            wait.missing_read_error = false;
        }
        return;
    }
    if owners.len() != 1 || owners[0] != snapshot.identity {
        run.stop_with_detail("FACT_CONFLICT", "ORDER_IDENTITY_MISMATCH", true, snapshot.observed_at);
        return;
    }
    let Ok(now) = run.phase1.clock.now() else {
        run.stop("DATA_UNAVAILABLE", true, snapshot.observed_at);
        return;
    };
    let wait = run.order_wait.get_or_insert_with(|| OrderWait::begin(id));
    if wait.identity.as_ref().is_some_and(|identity| identity != &snapshot.identity) {
        run.stop_with_detail("FACT_CONFLICT", "ORDER_IDENTITY_MISMATCH", true, snapshot.observed_at);
        return;
    }
    wait.identity.get_or_insert_with(|| snapshot.identity.clone());
    wait.created.get_or_insert(now);
    if wait.regressed(now) {
        run.stop("DATA_UNAVAILABLE", true, snapshot.observed_at);
        return;
    }
    wait.last_check = Some(now);
    // An old generation is not a successful current read and cannot clear U0.
    if snapshot.generation <= wait.generation { return; }
    wait.generation = snapshot.generation;
    let complete = applied_complete && snapshot.pages_complete
        && snapshot.accounting_complete() && snapshot.gross_q().is_some()
        && snapshot.gross_q() == snapshot.accounting_gross_q
        && snapshot.gross_q() == Some(snapshot.gross_q_before)
        && snapshot.gross_q_before == snapshot.gross_q_after
        && snapshot.submission == Submission::Accepted;
    if complete {
        if !wait.has_lifetime() {
            if let Some(expiration) = expiration { wait.grant_lifetime(now, expiration); }
        }
        if wait.has_lifetime() {
            wait.pending = true;
            wait.missing_since = None;
            wait.missing_read_error = false;
            return;
        }
    }
    wait.pending = false;
    wait.missing_since.get_or_insert(now);
}

/// Latch the wait's own exhaustion, using the original run clock.
///
/// The wait never restores risk by itself: it only says that its bounded
/// observation is over, and the run stops with the reason it already uses
/// for facts that never arrived.
pub(crate) fn observe_budget(run: &mut ControlledLiveState) {
    if run.order_wait.is_none() { return; }
    let Ok(now) = run.phase1.clock.now() else {
        return run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now());
    };
    let wait = run.order_wait.as_ref().unwrap();
    let reason = if wait.regressed(now) {
        Some("DATA_UNAVAILABLE")
    } else if now.utc >= run.options.deadline_at || now.elapsed_ms >= run.run_deadline_elapsed_ms {
        Some("DEADLINE")
    } else if wait.missing_read_error && wait.missing_expired(now, MISSING_FACTS_BUDGET_MS) {
        Some("READ_UNAVAILABLE")
    } else if !wait.may_observe(now, run.options.deadline_at, run.run_deadline_elapsed_ms) {
        Some("FACT_INCOMPLETE")
    } else { None };
    let wait = run.order_wait.as_mut().unwrap();
    if let Some(reason) = reason {
        wait.exhausted = true;
        run.stop(reason, true, now.utc);
    } else {
        wait.last_check = Some(now);
    }
}

/// Clear metadata only after original Final, account and risk consumers recovered.
/// The ledger and reservation remain exclusively owned by the original consumers.
pub(crate) fn recover_after_checks(state: &mut State) {
    if let Some(run) = state.live.controlled.as_mut() { observe_budget(run); }
    let Some(run) = state.live.controlled.as_ref() else { return; };
    if run.must_end() || run.degraded() || run.loss_latched { return; }
    let Some(wait) = run.order_wait.as_ref() else { return; };
    let Some(generation) = wait.final_generation else { return; };
    let Ok(now) = run.phase1.clock.now() else { return; };
    let exact = state.live.attempts.get(&wait.attempt_id).is_some_and(|handle| {
        let record = handle.lock();
        record.phase == AttemptPhase::Closed && wait.identity.as_ref() == Some(&record.identity)
    }) && state.live.generation.get(&wait.attempt_id) == Some(&generation)
        && state.live.attempts.values().all(|handle| handle.lock().phase == AttemptPhase::Closed)
        && state.live.reservations.values().all(|reserved| reserved.is_zero());
    if exact && run.account_read.as_ref().is_some_and(|read| read.matches(&state.live, now)) {
        state.live.controlled.as_mut().unwrap().order_wait = None;
    }
}

/// Check the original owner set and bounds before applying a new reading.
pub(super) fn before_read(state: &mut State) {
    let owners = state.live.attempts.values().filter(|handle| {
        let record = handle.lock();
        record.phase != AttemptPhase::Closed
            && matches!(record.submission, Submission::Accepted | Submission::Unknown)
    }).count();
    if let Some(run) = state.live.controlled.as_mut() {
        observe_budget(run);
        if owners > 1 {
            run.stop_with_detail("FACT_CONFLICT", "ORDER_IDENTITY_MISMATCH", true, Utc::now());
        }
    }
}

/// Keep the fixed lifetime and the independent missing-fact clock on read failure.
pub(super) fn note_read_error(broker: &Broker, error: &anyhow::Error) {
    use crate::service::venue::{VenueError, VenueErrorCode};
    let mut state = broker.state.lock();
    let Some(run) = state.live.controlled.as_mut() else { return; };
    if error.downcast_ref::<VenueError>().is_some_and(|e| e.code == VenueErrorCode::AccountMismatch) {
        run.stop_with_detail("FACT_CONFLICT", "ORDER_IDENTITY_MISMATCH", true, Utc::now());
        return;
    }
    let Ok(now) = run.phase1.clock.now() else {
        run.stop("DATA_UNAVAILABLE", true, Utc::now()); return;
    };
    observe_budget(run);
    if let Some(wait) = run.order_wait.as_mut() {
        wait.pending = false;
        wait.missing_since.get_or_insert(now);
        wait.missing_read_error = true;
    }
}

/// The one attempt whose accepted facts are still invisible.
///
/// Only an order that may already be at the venue qualifies, and only when
/// its facts are not yet visible: an order that has filled or that is closed
/// waits for nothing.
pub(super) fn unread_attempt(state: &State) -> Option<(u64, String)> {
    let mut found = None;
    for (id, handle) in &state.live.attempts {
        let record = handle.lock();
        let waiting = matches!(record.submission, Submission::Accepted | Submission::Unknown)
            && record.phase != AttemptPhase::Closed;
        if !waiting {
            continue;
        }
        // A complete, accepted reading is a fact in progress: the wait owns
        // it, and it is never re-read as a missing fact.
        if wait_of(state, *id).is_some_and(|wait| wait.pending) {
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

/// The wait of one attempt, when this run has one.
fn wait_of(state: &State, attempt_id: u64) -> Option<&OrderWait> {
    state
        .live
        .controlled
        .as_ref()?
        .order_wait
        .as_ref()
        .filter(|wait| wait.attempt_id == attempt_id)
}

/// Whether the bounded observation of one attempt is over.
///
/// An order with a proven lifetime is bounded by that lifetime; an order
/// without one keeps the runtime's existing budget, counted from the first
/// truly missing reading of the run of missing readings.
fn observation_over(state: &State, attempt_id: u64, budget: Duration) -> bool {
    let Some(run) = state.live.controlled.as_ref() else { return true; };
    let Ok(now) = run.phase1.clock.now() else { return true; };
    let Some(wait) = wait_of(state, attempt_id) else { return false; };
    !wait.may_observe(now, run.options.deadline_at, run.run_deadline_elapsed_ms)
        || wait.missing_expired(now, budget.as_millis().try_into().unwrap_or(u64::MAX))
}

/// Re-read the one order whose facts have not arrived, bounded in time.
///
/// Only this attempt's existing execution read is repeated: no other order is
/// read, no new request kind is used and nothing is sent. The caller owns
/// the operation lane. Every stop the read path itself raises keeps its first
/// reason, so a conflict is never swallowed by the wait.
pub(crate) async fn re_read_unread_facts(
    broker: &Broker,
    budget: Duration,
    interval: Duration,
) -> super::UnreadFacts {
    let Some((attempt_id, _)) = ({ let state = broker.state.lock(); unread_attempt(&state) })
    else {
        return super::UnreadFacts::NothingToRead;
    };
    let started = std::time::Instant::now();
    loop {
        if broker.controlled_stopped() {
            broker.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
            return super::UnreadFacts::Resolved;
        }
        let hash = {
            let state = broker.state.lock();
            if let Some(run) = state.live.controlled.as_ref() {
                if run.control.lock().stop.is_some() {
                    return super::UnreadFacts::Resolved;
                }
            }
            unread_attempt(&state).and_then(|(id, hash)| (id == attempt_id).then_some(hash))
        };
        let Some(hash) = hash else {
            return super::UnreadFacts::Resolved;
        };
        let (over, without_lifetime) = {
            let state = broker.state.lock();
            (
                observation_over(&state, attempt_id, budget),
                wait_of(&state, attempt_id).is_none_or(|wait| !wait.has_lifetime()),
            )
        };
        if over {
            return super::UnreadFacts::Expired;
        }
        if without_lifetime && started.elapsed() >= budget {
            return super::UnreadFacts::Expired;
        }
        let Some(venue) = broker.taker.as_ref() else {
            return super::UnreadFacts::Resolved;
        };
        let reading = tokio::select! {
            biased;
            result = venue.execution_snapshot(&hash) => result,
            _ = wait_budget_stop(broker) => return super::UnreadFacts::Resolved,
        };
        let snapshot = match reading {
            Ok(snapshot) => snapshot,
            Err(error) => {
                note_read_error(broker, &error);
                // A transient read failure degrades the window: it keeps
                // observing and adds no risk, and a later read restores it.
                // Only a streak that outlasts the budget stops the run.
                let stopped = {
                    let mut state = broker.state.lock();
                    match state.live.controlled.as_mut() {
                        Some(run) => run.note_failure(
                            crate::bot::runtime::live_control::FailureKind::Execution,
                            "READ_UNAVAILABLE",
                            chrono::Utc::now(),
                        ),
                        None => true,
                    }
                };
                if stopped {
                    let _ = crate::bot::runtime::live_events::stop_on_lane(
                        broker,
                        "READ_UNAVAILABLE",
                    );
                }
                return super::UnreadFacts::Resolved;
            }
        };
        if super::consume_snapshot(broker, &snapshot).is_err() {
            // The read path stops the run with its own reason, unless the
            // failure arrives after the facts were applied and before any stop
            // was written (a lost clock read). That reading is still a failure:
            // no later observation may continue without a stop.
            latch_unlatched_read_failure(broker);
            return super::UnreadFacts::Resolved;
        }
        if {
            let state = broker.state.lock();
            unread_attempt(&state).is_none()
        } {
            return super::UnreadFacts::Resolved;
        }
        // A complete accepted reading is a fact in progress: the wait owns it
        // from here, and the original drive keeps reading it. This helper only
        // has to stop blocking the candidate; it does not end the observation.
        if {
            let state = broker.state.lock();
            wait_of(&state, attempt_id).is_some_and(|wait| wait.pending)
        } {
            return super::UnreadFacts::Resolved;
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
        let _ = crate::bot::runtime::live_events::stop_on_lane(
            broker,
            crate::bot::runtime::live_events::reason::FACT_INCOMPLETE,
        );
    }
}

/// Bound an in-flight read without creating a new wait origin.
async fn wait_budget_stop(broker: &Broker) {
    loop {
        {
            let mut state = broker.state.lock();
            let Some(run) = state.live.controlled.as_mut() else { return; };
            observe_budget(run);
            if run.must_end() { return; }
        }
        tokio::select! {
            biased;
            _ = broker.wait_controlled_stop() => return,
            _ = tokio::time::sleep(super::UNREAD_FACTS_INTERVAL) => (),
        }
    }
}

/// Dispatch one original drive observation without expanding the parent runtime.
pub(crate) async fn observe_tick(
    broker: &Broker, data: &dyn crate::service::venue::MarketData,
) -> (bool, bool) {
    let cash = crate::bot::runtime::live_control::cash_wait::waiting(broker);
    let order = waiting(broker);
    let result = if order { observe_wait_tick(broker, data).await }
        else { crate::bot::runtime::live_control::cash_wait::observe_wait_tick(broker).await };
    if let Err(error) = result { tracing::warn!(error = ?error, "order state check failed"); }
    (cash, order)
}

/// Observe the run's waiting state on the original drive tick.
///
/// A cash wait and an order wait are two different facts: each is judged on
/// its own, and both of them only observe. The observation itself is the
/// runtime's original sync, so the order's facts keep being read, and the
/// market of the waiting order keeps being marked through the original book
/// read.
pub(crate) async fn observe_wait_tick(
    broker: &Broker,
    data: &dyn crate::service::venue::MarketData,
) -> anyhow::Result<()> {
    let market = {
        let mut state = broker.state.lock();
        let Some(run) = state.live.controlled.as_mut() else { return Ok(()); };
        observe_budget(run);
        let Some(wait) = run.order_wait.as_ref() else { return Ok(()); };
        let attempt_id = wait.attempt_id;
        let Some(attempt) = state.live.attempts.get(&attempt_id) else { return Ok(()); };
        let market_id = attempt.lock().identity.market_id;
        state.live.controlled.as_ref().and_then(|run| run.traded.get(&market_id)).cloned()
    };
    if !waiting(broker) {
        return broker.sync_live().await;
    }
    let observation = tokio::select! {
        biased;
        _ = wait_budget_stop(broker) => Ok(()),
        result = broker.sync_live() => result,
    };
    // Skipping the strategy must not stop the valuation of what the run owns:
    // the market of the waiting order keeps its original mark.
    let clock = broker.state.lock().live.controlled.as_ref().map(|run| run.phase1.clock.clone());
    if let (Some(market), Some(clock)) = (market, clock) {
        let market_ids = [market.id];
        let books = tokio::select! {
            biased;
            _ = wait_budget_stop(broker) => return observation,
            books = data.observed_books(&market_ids, clock.as_ref()) => books,
        };
        if let Ok(books) = books {
            if let Some(observed) = books.get(&market.id) {
                use crate::bot::arbitrage::phase1::{MAX_ACQUISITION_AGE_MS, MAX_REQUEST_SPAN_MS};
                let source = &observed.clock;
                if let Ok(now) = clock.now() {
                    let usable = source.is_consistent()
                        && source.received_at <= now.utc && source.received_elapsed_ms <= now.elapsed_ms
                        && source.request_span_ms() <= MAX_REQUEST_SPAN_MS
                        && source.acquisition_age_ms(now) <= MAX_ACQUISITION_AGE_MS
                        && source.source_at.is_none_or(|at| now.utc.signed_duration_since(at)
                            .num_milliseconds() <= MAX_ACQUISITION_AGE_MS);
                    if usable {
                        broker.observe_controlled_live_at(&market, &observed.book, CheckTime {
                            utc: source.received_at, elapsed_ms: source.received_elapsed_ms,
                        });
                    }
                }
            }
        }
    }
    super::super::live_control::refresh_live_risk(&mut broker.state.lock());
    observation
}

#[cfg(test)]
mod clock_tests {
    use super::*;

    fn time(seconds: i64, elapsed_ms: u64) -> CheckTime {
        CheckTime {
            utc: DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(seconds),
            elapsed_ms,
        }
    }

    #[test]
    fn rollback_is_relative_to_the_latest_observation_not_only_the_origin() {
        let mut wait = OrderWait::begin(1);
        wait.created = Some(time(100, 0));
        wait.last_check = Some(time(200, 100_000));
        assert!(wait.regressed(time(199, 101_000)));
        assert!(wait.regressed(time(201, 99_000)));
    }

    #[test]
    fn missing_facts_expire_on_either_clock_even_with_a_later_signed_expiry() {
        let mut wait = OrderWait::begin(1);
        wait.grant_lifetime(time(100, 0), 400);
        wait.missing_since = Some(time(110, 10_000));
        let deadline = time(1000, 0).utc;
        assert!(!wait.may_observe(time(230, 11_000), deadline, 1_000_000));
        assert!(!wait.may_observe(time(111, 130_000), deadline, 1_000_000));
    }

    #[test]
    fn lifetime_tail_is_frozen_and_its_equality_is_not_recoverable() {
        let mut wait = OrderWait::begin(1);
        wait.grant_lifetime(time(100, 0), 400);
        let deadline = time(1000, 0).utc;
        assert!(wait.may_observe(time(519, 419_000), deadline, 1_000_000));
        assert!(!wait.may_observe(time(520, 419_000), deadline, 1_000_000));
        assert!(!wait.may_observe(time(519, 420_000), deadline, 1_000_000));
    }

    #[test]
    fn invalid_expiry_never_grants_a_lifetime() {
        for expiration in [0, -1, 100, i64::MAX] {
            let mut wait = OrderWait::begin(1);
            wait.grant_lifetime(time(100, 0), expiration);
            assert!(!wait.has_lifetime());
        }
    }
}
