//! The account gate's controlled live check.
//!
//! The ordinary gate refuses new orders while an earlier attempt is
//! unconfirmed. The controlled path needs one more thing: it must be able to
//! prove that the *only* unconfirmed attempt on the account is the one that
//! is about to be sent, and that this attempt is still in its prepared
//! state.
//!
//! That check happens under the gate's own lock, so it can't race with
//! another attempt being reserved. It never clears the gate's reasons: a
//! failed read, a mismatch or a reconciliation conflict still blocks the
//! account, exactly as before.

use crate::service::venue::{AccountIdentity, AttemptPhase, VenueError, VenueErrorCode};
use std::collections::HashMap;
use std::sync::Mutex;

/// The account state the controlled check reads.
///
/// The parent module owns the real state; this view is what the check needs.
pub type AccountStates = Mutex<HashMap<String, super::AccountState>>;

/// Check that the account may send exactly this one attempt.
///
/// The unconfirmed set must hold this attempt and no other, and this
/// attempt's recorded state must be the prepared one. Anything else — a
/// second attempt, an attempt that may already have been submitted, a failed
/// read, a mismatch or a reconciliation conflict — refuses the send.
pub fn check_prepared_attempt(
    states: &AccountStates,
    identity: &AccountIdentity,
    local_attempt_id: u64,
    phase: AttemptPhase,
) -> Result<(), VenueError> {
    let key = super::account_key(identity);
    let accounts = states.lock().unwrap();
    let state = accounts.get(&key).ok_or_else(|| VenueError::new(
        VenueErrorCode::ReconciliationRequired, "controlled_gate", "ATTEMPT_NOT_PREPARED"))?;
    check_state(state, local_attempt_id, phase)
}

fn check_state(state: &super::AccountState, local_attempt_id: u64, phase: AttemptPhase) -> Result<(), VenueError> {
    if let Some(reason) = state.mismatch.as_ref() {
        return Err(VenueError::new(
            VenueErrorCode::AccountMismatch,
            "controlled_gate",
            &reason.clone(),
        ));
    }
    if let Some(reason) = state.reconciliation.as_ref() {
        return Err(VenueError::new(
            VenueErrorCode::ReconciliationRequired,
            "controlled_gate",
            &reason.clone(),
        ));
    }
    if let Some((step, reason)) = state.failed_reads.iter().next() {
        let _ = step;
        return Err(VenueError::new(
            VenueErrorCode::ReadUnavailable,
            "controlled_gate",
            &reason.clone(),
        ));
    }
    if state.unconfirmed.len() > 1 {
        return Err(VenueError::new(
            VenueErrorCode::ReconciliationRequired,
            "controlled_gate",
            "ANOTHER_ATTEMPT_UNCONFIRMED",
        ));
    }
    if !state.budget_confirmed {
        return Err(VenueError::new(VenueErrorCode::BudgetUnconfirmed,
            "controlled_gate", "BUDGET_UNCONFIRMED"));
    }
    match state.unconfirmed.get(&local_attempt_id) {
        None => Err(VenueError::new(VenueErrorCode::ReconciliationRequired,
            "controlled_gate", "ATTEMPT_NOT_PREPARED")),
        Some(state_text) if state_text == "PREPARED" && phase == AttemptPhase::Prepared => Ok(()),
        Some(_) => Err(VenueError::new(
            VenueErrorCode::ReconciliationRequired,
            "controlled_gate",
            "ATTEMPT_NOT_PREPARED",
        )),
    }
}

#[cfg(test)]
thread_local! {
    static ON_CONTENDED_SEND: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        std::cell::RefCell::new(None);
}

impl super::AccountGate {
    /// Hold the actual account mutex while an offline test drives its peer.
    #[cfg(test)]
    pub(crate) fn hold_send_gate_for_test(&self, body: impl FnOnce()) {
        let _accounts = self.accounts.lock().unwrap();
        body();
    }

    /// Observe a failed try-lock on this thread, not an assumed scheduling delay.
    #[cfg(test)]
    pub(crate) fn on_contended_send_for_test(callback: Option<Box<dyn Fn()>>) {
        ON_CONTENDED_SEND.with(|hook| *hook.borrow_mut() = callback);
    }

    /// Consume this attempt under the existing gate lock and immediately hand
    /// it to a nonblocking first-poll callback. Caller holds State then handle.
    pub(crate) fn with_prepared_attempt(
        &self,
        record: &mut crate::service::venue::AttemptRecord,
        handoff: &mut dyn FnMut(),
    ) -> Result<(), VenueError> {
        self.with_checked_prepared_attempt(record, &mut || Ok(()), handoff)
    }

    /// Recheck the receipt-bound clock after acquiring the last lock, before
    /// marking or polling anything. A contended gate cannot extend input age.
    pub(crate) fn with_checked_prepared_attempt(
        &self,
        record: &mut crate::service::venue::AttemptRecord,
        validate: &mut dyn FnMut() -> Result<(), VenueError>,
        handoff: &mut dyn FnMut(),
    ) -> Result<(), VenueError> {
        let key = super::account_key(&record.identity.account);
        #[cfg(test)]
        if self.accounts.try_lock().is_err() {
            ON_CONTENDED_SEND.with(|hook| {
                if let Some(callback) = hook.borrow().as_ref() { callback(); }
            });
        }
        let mut accounts = self.accounts.lock().unwrap();
        let state = accounts.get_mut(&key).ok_or_else(|| VenueError::new(
            VenueErrorCode::ReconciliationRequired, "controlled_gate", "ATTEMPT_NOT_PREPARED"))?;
        let id = record.identity.local_attempt_id;
        check_state(state, id, record.phase)?;
        validate()?;
        state.unconfirmed.insert(id, "MAY_HAVE_SUBMITTED".into());
        record.phase = AttemptPhase::MayHaveSubmitted;
        record.submission = crate::service::venue::Submission::Unknown;
        handoff();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VenueId;
    use crate::service::venue::AccountMode;

    fn identity() -> AccountIdentity {
        AccountIdentity {
            chain_id: 56,
            trading_address: "0x1111111111111111111111111111111111111111".into(),
            mode: AccountMode::PredictAccount,
            signer_address: "0x1111111111111111111111111111111111111111".into(),
            venue: VenueId::PredictFun,
        }
    }

    fn states_with(entries: Vec<(u64, String)>) -> AccountStates {
        let key = super::super::account_key(&identity());
        let mut state = super::super::AccountState::default();
        state.budget_confirmed = true;
        for (id, text) in entries {
            state.unconfirmed.insert(id, text);
        }
        Mutex::new(HashMap::from([(key, state)]))
    }

    #[test]
    fn the_only_prepared_attempt_may_send() {
        let states = states_with(vec![(7, "PREPARED".into())]);
        assert!(check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared).is_ok());
    }

    #[test]
    fn another_unconfirmed_attempt_refuses_the_send() {
        let states = states_with(vec![
            (7, "PREPARED".into()),
            (8, "PREPARED".into()),
        ]);
        let error = check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared)
            .unwrap_err();
        assert_eq!(error.reason, "ANOTHER_ATTEMPT_UNCONFIRMED");
    }

    #[test]
    fn an_attempt_that_may_have_been_sent_refuses_a_new_send() {
        let states = states_with(vec![(7, "MAY_HAVE_SUBMITTED".into())]);
        let error = check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared)
            .unwrap_err();
        assert_eq!(error.reason, "ATTEMPT_NOT_PREPARED");
    }

    #[test]
    fn an_account_without_its_own_prepared_attempt_cannot_send() {
        let states: AccountStates = Mutex::new(HashMap::new());
        assert!(check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared).is_err());
        for entries in [vec![], vec![(8, "PREPARED".into())]] {
            let states = states_with(entries);
            assert!(check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared).is_err());
        }
    }

    #[test]
    fn prepared_handle_and_confirmed_budget_are_independently_required() {
        let states = states_with(vec![(7, "PREPARED".into())]);
        assert!(check_prepared_attempt(&states, &identity(), 7, AttemptPhase::MayHaveSubmitted).is_err());
        states.lock().unwrap().get_mut(&super::super::account_key(&identity())).unwrap()
            .budget_confirmed = false;
        let error = check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared).unwrap_err();
        assert_eq!(error.code, VenueErrorCode::BudgetUnconfirmed);
    }

    #[test]
    fn prepared_gate_handoff_counts_first_pending_poll_as_possible_submission() {
        use crate::service::venue::{AttemptRecord, OrderIdentity, Submission};
        use crate::models::Side;
        use alloy_primitives::U256;
        use std::future::Future;
        let gate = super::super::AccountGate::new();
        gate.confirm_budget(&identity());
        gate.reserve(&identity(), 7).unwrap();
        let mut record = AttemptRecord {
            identity: OrderIdentity { local_attempt_id: 7, signed_hash: Some(format!("0x{}", "a".repeat(64))),
                venue_order_id: None, market_id: 1, token_id: "77".into(), side: Side::Buy, account: identity() },
            request_gross_atoms: U256::from(5), r0: U256::from(2), reserved_sell: U256::ZERO,
            phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
        };
        let mut polls = 0;
        let mut future = std::pin::pin!(std::future::pending::<()>());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        gate.with_prepared_attempt(&mut record, &mut || {
            assert!(gate.accounts.try_lock().is_err(), "gate stays locked through first poll");
            polls += 1;
            assert!(future.as_mut().poll(&mut context).is_pending());
        }).unwrap();
        assert_eq!(polls, 1);
        assert_eq!(record.phase, AttemptPhase::MayHaveSubmitted);
        assert_eq!(record.submission, Submission::Unknown);
        assert!(gate.with_prepared_attempt(&mut record, &mut || { polls += 1; }).is_err());
        assert_eq!(polls, 1);
        assert!(!gate.allows_new_orders(&identity()));
    }

    #[test]
    fn contended_original_gate_revalidates_clock_before_marking_or_first_poll() {
        use crate::service::observation::{Clock, FixtureClock};
        use crate::service::venue::{AttemptRecord, OrderIdentity, Submission};
        use crate::models::Side;
        use alloy_primitives::U256;
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let gate = Arc::new(super::super::AccountGate::new());
        gate.confirm_budget(&identity());
        gate.reserve(&identity(), 7).unwrap();
        let clock = Arc::new(FixtureClock::new(chrono::Utc::now()));
        let received = clock.now().unwrap();
        let mut locked = gate.accounts.lock().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (callback_tx, callback_rx) = mpsc::channel();
        let worker_gate = gate.clone();
        let worker_clock = clock.clone();
        let worker = std::thread::spawn(move || {
            let mut record = AttemptRecord {
                identity: OrderIdentity { local_attempt_id: 7, signed_hash: Some(format!("0x{}", "a".repeat(64))),
                    venue_order_id: None, market_id: 1, token_id: "77".into(), side: Side::Buy, account: identity() },
                request_gross_atoms: U256::from(5), r0: U256::from(2), reserved_sell: U256::ZERO,
                phase: AttemptPhase::Prepared, submission: Submission::NotSubmitted, last_reason: None,
            };
            let mut polls = 0;
            entered_tx.send(()).unwrap();
            let result = worker_gate.with_checked_prepared_attempt(&mut record, &mut || {
                let checked = worker_clock.now().unwrap();
                callback_tx.send(checked).unwrap();
                if checked.elapsed_ms - received.elapsed_ms > 10_000 {
                    return Err(VenueError::new(VenueErrorCode::InvalidInput, "controlled_send", "INPUT_CHANGED"));
                }
                Ok(())
            }, &mut || { polls += 1; });
            (result, record, polls)
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(callback_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "the real gate mutex must prevent validation/handoff while occupied");
        clock.advance(10_001);
        assert_eq!(locked.get_mut(&super::super::account_key(&identity())).unwrap()
            .unconfirmed.get(&7).map(String::as_str), Some("PREPARED"));
        drop(locked);
        let checked = callback_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (result, record, polls) = worker.join().unwrap();
        assert_eq!(checked.elapsed_ms - received.elapsed_ms, 10_001);
        assert_eq!(result.unwrap_err().reason, "INPUT_CHANGED");
        assert_eq!(polls, 0);
        assert_eq!(record.phase, AttemptPhase::Prepared);
        assert_eq!(record.submission, Submission::NotSubmitted);
        assert!(record.identity.signed_hash.is_some());
        assert!(check_prepared_attempt(&gate.accounts, &identity(), 7, AttemptPhase::Prepared).is_ok());
    }

    #[test]
    fn a_failed_read_still_refuses_the_send() {
        let key = super::super::account_key(&identity());
        let mut state = super::super::AccountState::default();
        state
            .failed_reads
            .insert("account".into(), "READ_FAILED".into());
        let states: AccountStates = Mutex::new(HashMap::from([(key, state)]));
        let error = check_prepared_attempt(&states, &identity(), 7, AttemptPhase::Prepared)
            .unwrap_err();
        assert_eq!(error.code, VenueErrorCode::ReadUnavailable);
    }
}
