//! Real observation clocks: when a request left, when a response came back,
//! and how long this process has been running.
//!
//! A price without a receipt time is not a fresh price, it is a remembered one.
//! This module owns the two readings a decision needs — the wall clock and the
//! process's monotonic elapsed time — and never derives one from the other.
//! The elapsed clock is Linux `CLOCK_BOOTTIME`, so a suspended process still
//! ages: a pause can never look like a fresh input.

use crate::models::BookSnapshot;
use crate::service::spot::SpotSource;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

/// One reading of both clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckTime {
    /// The wall clock at the moment of the reading.
    pub utc: DateTime<Utc>,
    /// Milliseconds since the clock's own origin. It never goes backwards.
    pub elapsed_ms: u64,
}

/// A source of clock readings.
///
/// The implementation is chosen by the caller, never by the configuration: the
/// production path uses [`SystemClock`], and only a test may inject its own.
pub trait Clock: Send + Sync {
    fn now(&self) -> Result<CheckTime>;
}

/// Whether the upstream source's own event time is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceTimeStatus {
    Missing,
    Present,
}

/// What the freshness of an input can be proven from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FreshnessBasis {
    /// Only the local receipt can be proven; the upstream freshness is unknown.
    ReceiptBound,
    /// The source's own event time is known and bounded as well.
    SourceAndReceipt,
}

/// The clock of one acquired input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObservationClock {
    /// The source's own event time, when it is known. The current REST feeds
    /// carry no such time, so this is `None` and never guessed.
    #[serde(serialize_with = "serialize_millis_option")]
    pub source_at: Option<DateTime<Utc>>,
    pub source_time_status: SourceTimeStatus,
    pub freshness_basis: FreshnessBasis,
    /// When this request actually left, before the first byte came back.
    #[serde(serialize_with = "serialize_millis")]
    pub request_at: DateTime<Utc>,
    /// When the response was decoded and checked.
    #[serde(serialize_with = "serialize_millis")]
    pub received_at: DateTime<Utc>,
    pub request_elapsed_ms: u64,
    pub received_elapsed_ms: u64,
}

/// Write a timestamp with exactly millisecond precision.
///
/// The protocol's timestamps are read by a strict consumer, so a fixed width
/// keeps them comparable and free of a nanosecond tail nobody asked for.
pub fn format_millis(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Write a required timestamp.
pub fn serialize_millis<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format_millis(value))
}

/// Write a timestamp that may be absent.
pub fn serialize_millis_option<S>(
    value: &Option<DateTime<Utc>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_str(&format_millis(value)),
        None => serializer.serialize_none(),
    }
}

impl ObservationClock {
    /// The conservative span of the request: the slower of the two clocks.
    pub fn request_span_ms(&self) -> i64 {
        let wall = self.received_at.timestamp_millis() - self.request_at.timestamp_millis();
        let elapsed = self.received_elapsed_ms as i64 - self.request_elapsed_ms as i64;
        wall.max(elapsed)
    }

    /// The conservative age of the input at `now`, measured from the request.
    pub fn acquisition_age_ms(&self, now: CheckTime) -> i64 {
        let wall = now.utc.timestamp_millis() - self.request_at.timestamp_millis();
        let elapsed = now.elapsed_ms as i64 - self.request_elapsed_ms as i64;
        wall.max(elapsed)
    }

    /// Whether the clock itself is self-consistent.
    ///
    /// A future receipt, a receipt before its request or an elapsed clock that
    /// ran backwards are all clock faults: the observation is not usable.
    pub fn is_consistent(&self) -> bool {
        self.request_at.timestamp_millis() >= 0
            && self.request_elapsed_ms <= i64::MAX as u64
            && self.received_elapsed_ms <= i64::MAX as u64
            && matches!(
                (
                    self.source_at,
                    self.source_time_status,
                    self.freshness_basis
                ),
                (
                    None,
                    SourceTimeStatus::Missing,
                    FreshnessBasis::ReceiptBound
                ) | (
                    Some(_),
                    SourceTimeStatus::Present,
                    FreshnessBasis::SourceAndReceipt
                )
            )
            && self.received_at >= self.request_at
            && self.received_elapsed_ms >= self.request_elapsed_ms
            && self
                .source_at
                .is_none_or(|source| source.timestamp_millis() >= 0 && source <= self.received_at)
    }
}

/// A spot quote with the clock that received it.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedSpot {
    pub price: f64,
    pub source: SpotSource,
    /// The pair that was actually requested, e.g. `BTC-USD` for Coinbase.
    pub actual_pair: String,
    pub clock: ObservationClock,
}

/// A book snapshot with the clock that received it.
#[derive(Debug, Clone)]
pub struct ObservedBook {
    pub book: BookSnapshot,
    pub clock: ObservationClock,
}

/// The production clock: UTC plus Linux `CLOCK_BOOTTIME`.
///
/// `CLOCK_BOOTTIME` includes the time a suspended process spent asleep, so a
/// pause can never make an input look fresh.
pub struct SystemClock {
    origin: Duration,
}

impl SystemClock {
    pub fn new() -> Result<Self> {
        let origin = boottime().ok_or_else(|| anyhow!("CLOCK_UNAVAILABLE"))?;
        Ok(Self { origin })
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Result<CheckTime> {
        let now = boottime().ok_or_else(|| anyhow!("CLOCK_UNAVAILABLE"))?;
        Ok(CheckTime {
            utc: Utc::now(),
            elapsed_ms: u64::try_from(
                now.checked_sub(self.origin)
                    .ok_or_else(|| anyhow!("CLOCK_UNAVAILABLE"))?
                    .as_millis(),
            )
            .map_err(|_| anyhow!("CLOCK_UNAVAILABLE"))?,
        })
    }
}

/// A clock a test drives by hand.
///
/// It is not a configuration knob: only the code that owns it can move it.
pub struct FixtureClock {
    reading: Mutex<CheckTime>,
}

impl FixtureClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            reading: Mutex::new(CheckTime {
                utc: start,
                elapsed_ms: 0,
            }),
        }
    }

    /// Move the clock forward by `millis` on both readings.
    pub fn advance(&self, millis: u64) {
        let mut reading = self.reading.lock();
        reading.utc += chrono::Duration::milliseconds(millis as i64);
        reading.elapsed_ms += millis;
    }

    /// Set the wall clock without moving the elapsed clock.
    pub fn set_utc(&self, utc: DateTime<Utc>) {
        self.reading.lock().utc = utc;
    }
}

impl Clock for FixtureClock {
    fn now(&self) -> Result<CheckTime> {
        Ok(*self.reading.lock())
    }
}

/// A clock handle a strategy and the broker can share.
pub type ClockHandle = Arc<dyn Clock>;

/// The Linux monotonic clock that includes suspend time.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn boottime() -> Option<Duration> {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    extern "C" {
        fn clock_gettime(clock_id: i32, tp: *mut Timespec) -> i32;
    }
    const CLOCK_BOOTTIME: i32 = 7;
    let mut now = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable `timespec`, and the ABI of
    // `clock_gettime` on Linux x86_64/aarch64 is the C one declared above.
    let code = unsafe { clock_gettime(CLOCK_BOOTTIME, &mut now as *mut Timespec) };
    if code != 0 || now.tv_sec < 0 || !(0..1_000_000_000).contains(&now.tv_nsec) {
        return None;
    }
    Some(Duration::new(now.tv_sec as u64, now.tv_nsec as u32))
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
fn boottime() -> Option<Duration> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_never_goes_backwards() {
        let clock = SystemClock::new().unwrap();
        let first = clock.now().unwrap();
        let second = clock.now().unwrap();
        assert!(second.utc >= first.utc);
        assert!(second.elapsed_ms >= first.elapsed_ms);
    }

    #[test]
    fn the_fixture_clock_moves_both_readings_together() {
        let start = DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap();
        let clock = FixtureClock::new(start);
        clock.advance(1_500);
        let reading = clock.now().unwrap();
        assert_eq!(reading.elapsed_ms, 1_500);
        assert_eq!(reading.utc, start + chrono::Duration::milliseconds(1_500));
    }

    #[test]
    fn a_clock_that_ran_backwards_is_inconsistent() {
        let start = DateTime::<Utc>::from_timestamp_millis(1_762_636_800_000).unwrap();
        let clock = ObservationClock {
            source_at: None,
            source_time_status: SourceTimeStatus::Missing,
            freshness_basis: FreshnessBasis::ReceiptBound,
            request_at: start,
            received_at: start,
            request_elapsed_ms: 100,
            received_elapsed_ms: 50,
        };
        assert!(!clock.is_consistent());
        assert_eq!(clock.request_span_ms(), 0);
    }
}
