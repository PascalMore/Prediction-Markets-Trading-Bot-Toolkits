//! The bounded wait for a claim's cash.
//!
//! A paid claim waits for a human or for the venue to deliver. That wait is
//! revocable: when the exact cash arrives and every owner is closed, the run
//! may continue. What it never is is a second balance table or a way to skip
//! the original checks.
//!
//! Three facts define the wait:
//!
//! - `W0` is the moment the wait began. It is set once and never reset by a
//!   later read, a partial recovery or a repeated result.
//! - The budget is the run's own read-only tail budget, reused rather than
//!   invented: 600 seconds on both clocks.
//! - The original deadline still wins. The wait ends at
//!   `min(W0 + budget, deadline)`, whichever clock arrives first.

use crate::service::observation::CheckTime;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// The budget of one cash wait.
///
/// This is the run's existing read-only tail budget, reused by reference and
/// not by a new constant of its own: a wait must not be longer than the
/// cleanup it lives inside.
pub const CASH_WAIT_BUDGET: Duration = super::super::live_accounting::LIVE_TAIL_BUDGET;

/// The metadata of one cash wait.
///
/// It holds no balance: the ledger's own atomic facts remain the only cash
/// the run has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CashWait {
    /// The moment the wait began, on both clocks.
    pub started: CheckTime,
    /// The identities this wait is waiting for, by attempt id.
    pub pending: Vec<u64>,
    /// Whether the wait's budget has run out.
    pub exhausted: bool,
}

impl CashWait {
    /// Begin a wait for a batch of attempts.
    pub fn begin(started: CheckTime, pending: Vec<u64>) -> Self {
        Self { started, pending, exhausted: false }
    }

    /// The moment this wait may no longer continue, on the wall clock.
    ///
    /// The earlier of the budget and the run's own deadline ends the wait.
    pub fn ends_at(&self, deadline_at: DateTime<Utc>) -> DateTime<Utc> {
        let budget = self.started.utc + chrono::Duration::from_std(CASH_WAIT_BUDGET)
            .unwrap_or_else(|_| chrono::Duration::zero());
        budget.min(deadline_at)
    }

    /// The elapsed reading this wait may no longer continue at.
    pub fn ends_at_elapsed_ms(&self, run_deadline_elapsed_ms: u64) -> u64 {
        self.started.elapsed_ms
            .saturating_add(CASH_WAIT_BUDGET.as_millis() as u64)
            .min(run_deadline_elapsed_ms)
    }

    /// Whether the wait may still continue at `now`.
    ///
    /// Both clocks must still be inside their bound. An equality ends the
    /// wait: the boundary is not a recovery point.
    pub fn may_continue(&self, now: CheckTime, deadline_at: DateTime<Utc>,
                        run_deadline_elapsed_ms: u64) -> bool {
        !self.exhausted
            && now.utc >= self.started.utc && now.elapsed_ms >= self.started.elapsed_ms
            && now.utc < self.ends_at(deadline_at)
            && now.elapsed_ms < self.ends_at_elapsed_ms(run_deadline_elapsed_ms)
    }

    /// Note that the wait's budget has run out.
    ///
    /// The wait is then over: it cannot be extended by a later read.
    pub fn exhaust(&mut self) {
        self.exhausted = true;
    }

    /// Add an attempt to the identities this wait covers.
    ///
    /// A repeated attempt is added once: the wait names identities, not
    /// readings.
    pub fn add_pending(&mut self, attempt_id: u64) {
        if !self.pending.contains(&attempt_id) {
            self.pending.push(attempt_id);
        }
    }

    /// Whether every attempt this wait covers has been closed.
    pub fn all_closed(&self, closed: impl Fn(u64) -> bool) -> bool {
        self.pending.iter().all(|id| closed(*id))
    }
}

/// The run's own deadline on the elapsed clock.
///
/// The deadline is a wall-clock moment; the elapsed clock is the run's own.
/// The run's elapsed deadline is the moment its own clock passes the same
/// budget it was started with.
pub fn run_deadline_elapsed_ms(started: CheckTime, budget_secs: i64) -> u64 {
    started.elapsed_ms.saturating_add((budget_secs.max(0) as u64).saturating_mul(1000))
}

/// Whether the wait may continue, given the run's own bound.
pub fn may_continue(wait: &CashWait, now: CheckTime, deadline_at: DateTime<Utc>,
                    run_deadline_elapsed_ms: u64) -> bool {
    wait.may_continue(now, deadline_at, run_deadline_elapsed_ms)
}

/// Latch exhaustion before any recovery, using the original run clock.
pub(crate) fn observe_budget(run: &mut super::ControlledLiveState) {
    let now = match run.phase1.clock.now() {
        Ok(now) => now,
        Err(_) => { run.stop("DATA_UNAVAILABLE", true, chrono::Utc::now()); return; }
    };
    if now.utc >= run.options.deadline_at || now.elapsed_ms >= run.run_deadline_elapsed_ms {
        run.stop("DEADLINE", true, now.utc);
    }
    if let Some(wait) = run.cash_wait.as_mut() {
        if !wait.may_continue(now, run.options.deadline_at, run.run_deadline_elapsed_ms) {
            wait.exhaust();
        }
    }
}

/// Whether the original drive should observe instead of running the strategy.
pub(crate) fn waiting(broker: &super::Broker) -> bool {
    broker.state.lock().live.controlled.as_ref().is_some_and(|run| run.cash_wait.is_some())
}

/// Sync on the original lane; only a read-only wait future may be interrupted.
pub(crate) async fn observe_wait_tick(broker: &super::Broker) -> anyhow::Result<()> {
    if !waiting(broker) { return broker.sync_live().await; }
    if let Some(run) = broker.state.lock().live.controlled.as_mut() { observe_budget(run); }
    tokio::select! {
        biased;
        _ = broker.wait_controlled_stop() => Ok(()),
        result = broker.sync_live() => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now(utc_seconds: i64, elapsed_ms: u64) -> CheckTime {
        CheckTime {
            utc: DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(utc_seconds),
            elapsed_ms,
        }
    }

    #[test]
    fn a_wait_ends_at_the_earlier_of_the_budget_and_the_deadline() {
        let started = now(0, 0);
        let wait = CashWait::begin(started, vec![1]);
        let deadline = DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(100);
        // Before the deadline the wait continues.
        assert!(wait.may_continue(now(50, 50_000), deadline, 1_000_000));
        // The deadline is earlier than the budget, so it ends the wait.
        assert!(!wait.may_continue(now(100, 100_000), deadline, 1_000_000));
        assert!(wait.may_continue(now(50, 50_000), DateTime::<Utc>::UNIX_EPOCH
            + chrono::Duration::seconds(100_000), 1_000_000));
    }

    #[test]
    fn the_boundary_is_not_a_recovery_point() {
        let wait = CashWait::begin(now(0, 0), vec![1]);
        let deadline = DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(100_000);
        assert!(!wait.may_continue(now(600, 600_000), deadline, 10_000_000));
        assert!(wait.may_continue(now(599, 599_999), deadline, 10_000_000));
    }

    #[test]
    fn an_exhausted_wait_never_continues() {
        let mut wait = CashWait::begin(now(0, 0), vec![1]);
        wait.exhaust();
        assert!(!wait.may_continue(now(1, 1_000), DateTime::<Utc>::UNIX_EPOCH
            + chrono::Duration::seconds(100_000), 10_000_000));
    }
}
