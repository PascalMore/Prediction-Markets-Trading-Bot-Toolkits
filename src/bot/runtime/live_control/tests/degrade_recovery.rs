//! R14b: the per-kind failure streaks that degrade a window without stopping it.
//!
//! A read that could not be made degrades the window inside `READ_FAILURE_BUDGET`
//! and a successful read clears the streak and restores it. The streaks are per
//! kind: Read, Execution and Valuation each clear only on their own success, so
//! one recovered path never unlocks a window that is still blind on another.
//! Offline fixtures only — no venue, network, signer or real order.

use super::*;
use crate::bot::runtime::live_control::FailureKind;

#[test]
fn read_and_execution_streaks_degrade_and_clear_only_on_their_own_success() {
    let (broker, _clock) = risk_fixture();
    let mut state = broker.state.lock();
    let now = chrono::Utc::now();
    let run = state.live.controlled.as_mut().unwrap();
    assert!(!run.degraded(), "a healthy window is not degraded");
    // A read that could not be made degrades the window inside its budget.
    assert!(!run.note_failure(FailureKind::Read, "READ_UNAVAILABLE", now));
    assert!(run.degraded());
    assert!(!run.may_add_risk());
    assert!(run.control.lock().stop.is_none());
    // A success of another kind is not this streak's success.
    run.clear_failure(FailureKind::Execution);
    run.clear_failure(FailureKind::Valuation);
    assert!(run.degraded(), "a read streak is not cleared by another kind");
    // Its own success restores the window: no stop, no restart.
    run.clear_failure(FailureKind::Read);
    assert!(!run.degraded());
    assert!(run.may_add_risk());
    assert!(run.control.lock().stop.is_none());
    // The execution streak behaves the same way, in the other direction.
    assert!(!run.note_failure(FailureKind::Execution, "DATA_UNAVAILABLE", now));
    assert!(run.degraded());
    assert!(!run.may_add_risk());
    run.clear_failure(FailureKind::Read);
    assert!(run.degraded(), "an execution streak is not cleared by a read");
    run.clear_failure(FailureKind::Execution);
    assert!(!run.degraded());
    assert!(run.may_add_risk());
}
