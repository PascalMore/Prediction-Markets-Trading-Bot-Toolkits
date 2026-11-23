//! The one source of the send path's own diagnostics.
//!
//! A rejection that says only "INPUT_CHANGED" is not actionable: it does not
//! say whether the book moved, the account went stale, or the request never
//! left. This module turns the original branches' own outcome into a closed,
//! typed projection and keeps the first failure, so the wire carries the
//! reason the original guard actually produced.
//!
//! Three rules hold here:
//!
//! - The original decision is never changed. This module observes; it does
//!   not classify, retry or release anything.
//! - A measurement that was not taken stays `None`. No age is ever clamped
//!   to zero and no clock is invented.
//! - Only fixed-size values are captured. Nothing here allocates a
//!   collection, logs, waits or does IO inside a critical section.

use crate::bot::arbitrage::phase1::CandidateContext;
use crate::service::observation::CheckTime;
use crate::service::venue::{
    SendCheckDetail, SendCheckObservation, SendCheckStage,
};


/// The ages one check measured, all on the check's own clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendCheckAges {
    /// When this tick's first account read was issued.
    pub tick_started_elapsed_ms: Option<u64>,
    /// The age of the book receipt.
    pub book_receipt_age_ms: Option<u64>,
    /// The age of the spot receipt.
    pub spot_receipt_age_ms: Option<u64>,
    /// The age of the account receipt.
    pub account_receipt_age_ms: Option<u64>,
    /// The age of the book's own request.
    pub book_request_age_ms: Option<u64>,
    /// The age of the spot's own request.
    pub spot_request_age_ms: Option<u64>,
}

/// The age of one reading, on both clocks.
///
/// The age is the slower of the two clocks, and it exists only when both
/// readings are there and neither ran backwards. A clock that went backwards
/// or a reading that never happened is no age at all.
pub fn age_ms(now: CheckTime, receipt: Option<CheckTime>) -> Option<u64> {
    let receipt = receipt?;
    let wall = now.utc.signed_duration_since(receipt.utc).num_milliseconds();
    let wall = u64::try_from(wall).ok()?;
    Some(wall.max(now.elapsed_ms.checked_sub(receipt.elapsed_ms)?))
}

/// The age of one observation clock's own request, on both clocks.
///
/// The request age is what the venue's freshness gate really measures; the
/// receipt age is a different number and is reported beside it.
pub fn request_age_ms(now: CheckTime, clock: Option<&crate::service::observation::ObservationClock>) -> Option<u64> {
    let clock = clock?;
    age_ms(now, Some(CheckTime { utc: clock.request_at, elapsed_ms: clock.request_elapsed_ms }))
}

/// The receipt age of one observation clock's own receipt.
///
/// A receipt is the moment the response was decoded; its age is measured
/// from that moment, not from the request.
pub fn receipt_age_ms(now: CheckTime, clock: Option<&crate::service::observation::ObservationClock>) -> Option<u64> {
    let clock = clock?;
    age_ms(now, Some(CheckTime { utc: clock.received_at, elapsed_ms: clock.received_elapsed_ms }))
}


/// The measurement a guard freezes for the attempt it guards.
///
/// The guard owns these readings: the candidate's own book and spot clocks
/// and the tick's account receipt. They are frozen once, so every later
/// check of the same attempt reports the same ages.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FrozenMeasurement {
    /// When this tick's first account read was issued.
    pub tick_started_elapsed_ms: Option<u64>,
    /// The account read this attempt prepares from.
    pub account_receipt: Option<CheckTime>,
    /// The candidate's own book clock.
    pub book_clock: Option<crate::service::observation::ObservationClock>,
    /// The candidate's own spot clock.
    pub spot_clock: Option<crate::service::observation::ObservationClock>,
}

impl FrozenMeasurement {
    /// Freeze the measurement of one attempt from the run's own facts.
    pub fn freeze(candidate: &CandidateContext, account_receipt: Option<CheckTime>,
                  tick_started_elapsed_ms: Option<u64>) -> Self {
        Self {
            tick_started_elapsed_ms,
            account_receipt,
            book_clock: Some(candidate.book_clock().clone()),
            spot_clock: Some(candidate.spot_clock().clone()),
        }
    }

    /// The ages this measurement reports at one check's clock.
    pub fn ages(&self, now: CheckTime) -> SendCheckAges {
        SendCheckAges {
            tick_started_elapsed_ms: self.tick_started_elapsed_ms,
            book_receipt_age_ms: receipt_age_ms(now, self.book_clock.as_ref()),
            spot_receipt_age_ms: receipt_age_ms(now, self.spot_clock.as_ref()),
            account_receipt_age_ms: age_ms(now, self.account_receipt),
            book_request_age_ms: request_age_ms(now, self.book_clock.as_ref()),
            spot_request_age_ms: request_age_ms(now, self.spot_clock.as_ref()),
        }
    }
}

/// The projection of one check, ready to be written.
pub fn project(stage: SendCheckStage, detail: SendCheckDetail, now: CheckTime,
               ages: SendCheckAges) -> SendCheckObservation {
    SendCheckObservation {
        stage,
        detail,
        checked_at: now.utc,
        checked_elapsed_ms: now.elapsed_ms,
        tick_started_elapsed_ms: ages.tick_started_elapsed_ms,
        book_receipt_age_ms: ages.book_receipt_age_ms,
        spot_receipt_age_ms: ages.spot_receipt_age_ms,
        account_receipt_age_ms: ages.account_receipt_age_ms,
        book_request_age_ms: ages.book_request_age_ms,
        spot_request_age_ms: ages.spot_request_age_ms,
    }
}

/// The detail the original core's own refusal names.
///
/// The core's reasons are precise already; this maps them onto the closed
/// detail set without inventing a more specific cause. A reason the set does
/// not cover stays `UNKNOWN`.
pub fn detail_of_core_reason(reason: &crate::bot::arbitrage::phase1::Reason) -> SendCheckDetail {
    use crate::bot::arbitrage::phase1::Reason;
    match reason {
        Reason::SpotStale => SendCheckDetail::SpotStale,
        Reason::BookStale => SendCheckDetail::BookStale,
        Reason::RoundTooLate => SendCheckDetail::RoundTooLate,
        Reason::EdgeBelowMin => SendCheckDetail::EdgeBelowMin,
        Reason::InputChanged => SendCheckDetail::FrozenInput,
        _ => SendCheckDetail::Unknown,
    }
}
