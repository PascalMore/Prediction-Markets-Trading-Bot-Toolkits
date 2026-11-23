//! Diagnostic projection tests use the same guards and original adapter branches.
use super::*;
use crate::bot::runtime::taker::LiveCandidateRequest;
use crate::service::observation::{CheckTime, ObservationClock, FreshnessBasis, SourceTimeStatus};

#[test]
fn any_negative_clock_age_is_missing_not_zero_or_the_other_clock() {
    let receipt = CheckTime { utc: chrono::DateTime::UNIX_EPOCH, elapsed_ms: 10 };
    let backwards = CheckTime { utc: receipt.utc - chrono::Duration::milliseconds(1), elapsed_ms: 11 };
    assert_eq!(diagnostics::age_ms(backwards, Some(receipt)), None);
    let clock = ObservationClock { source_at: None, source_time_status: SourceTimeStatus::Missing,
        freshness_basis: FreshnessBasis::ReceiptBound, request_at: receipt.utc,
        received_at: receipt.utc, request_elapsed_ms: receipt.elapsed_ms, received_elapsed_ms: receipt.elapsed_ms };
    assert_eq!(diagnostics::request_age_ms(backwards, Some(&clock)), None);
    assert_eq!(diagnostics::receipt_age_ms(backwards, Some(&clock)), None);
    let elapsed_back = CheckTime { utc: receipt.utc + chrono::Duration::seconds(1), elapsed_ms: 9 };
    assert_eq!(diagnostics::age_ms(elapsed_back, Some(receipt)), None);
    assert_eq!(diagnostics::request_age_ms(elapsed_back, Some(&clock)), None);
    assert_eq!(diagnostics::receipt_age_ms(elapsed_back, Some(&clock)), None);
    let extreme = CheckTime { utc: receipt.utc, elapsed_ms: u64::MAX };
    assert_eq!(diagnostics::age_ms(extreme, Some(receipt)), Some(u64::MAX - 10));
}

#[tokio::test]
async fn original_limit_depth_and_empty_book_branches_have_typed_preparation_details() {
    for (fault, detail) in [("book_past_limit", "LIMIT_PRICE"),
        ("book_inside_limit_thin", "DEPTH"), ("empty_book", "EMPTY_BOOK")] {
        let (broker, _, market, book, candidate, transport, _) =
            candidate_tests::sending_fixture_with_trace(fault).await;
        let result = broker.take_live_candidate(LiveCandidateRequest {
            market: &market, yes_book: &book, candidate: &candidate,
        }).await;
        assert_eq!(transport.signs.load(Ordering::SeqCst), 0);
        assert_eq!(transport.posts.load(Ordering::SeqCst), 0);
        if fault == "empty_book" {
            assert!(result.err().unwrap().to_string().contains("EMPTY_BOOK"));
            continue; // The real hard-error event is checked by the Python native-log test.
        }
        let outcome = result.unwrap();
        let observed = outcome.guard.unwrap().diagnostic().expect("original branch must reach the guard");
        assert_eq!(observed.stage, "PREPARE");
        assert_eq!(observed.detail, detail, "{fault}");
    }
}

#[tokio::test]
async fn a_post_failure_is_unknown_not_a_retryable_local_candidate_refusal() {
    let (broker, _, market, book, candidate, transport, _) =
        candidate_tests::sending_fixture_with_trace("timeout").await;
    let ack = candidate_tests::ack_next(broker.clone(), false);
    let result = broker.take_live_candidate(LiveCandidateRequest {
        market: &market, yes_book: &book, candidate: &candidate,
    }).await;
    ack.join().unwrap();
    assert!(result.is_err());
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(transport.signs.load(Ordering::SeqCst), 1);
    let state = broker.state.lock();
    assert!(state.live.attempts.values().any(|handle| handle.lock().submission
        == crate::service::venue::Submission::Unknown));
    assert!(state.live.reservations.values().any(|value| !value.is_zero()));
}
