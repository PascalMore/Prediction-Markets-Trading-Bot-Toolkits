//! Gated send-path timing.
//!
//! The path that reserves cash and sends an order is measured, not guessed: with
//! `YQUANT_TIMING=1` one line per submission carries the milliseconds of every
//! segment from the reservation to the first transport response. Off by default
//! and inert without the variable, so production behaviour, event schema and the
//! ledger are untouched. This is evidence, not a control.
//!
//! The segments are an ordered array, never an object: the same stage may
//! occur twice in one path, and an object would silently drop one of them.
//! Each entry names its own unit, so a segment is never compared to a
//! measurement in another unit.

use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// The revision of this measurement's shape and units.
///
/// A report that names its revision can be compared with another report of
/// the same revision, and only with that one.
pub const MEASUREMENT_REVISION: &str = "btc5m-send-path.r16.1";

fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| matches!(std::env::var("YQUANT_TIMING").as_deref(), Ok("1")))
}

/// One measured segment of the path, in path order.
#[derive(Debug, Clone, Serialize)]
pub struct Segment {
    /// The stage this segment ends at.
    pub stage: String,
    /// How long the segment took, in microseconds.
    pub duration_us: u64,
}

/// One measurement window: the monotonic marks of a single submission.
struct Window {
    started: Instant,
    marks: Vec<(&'static str, u64)>,
    net_calls: u64,
}

static WINDOW: Mutex<Option<Window>> = Mutex::new(None);
static LAST: Mutex<Vec<(String, u64)>> = Mutex::new(Vec::new());
static LAST_NET_CALLS: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_US: AtomicU64 = AtomicU64::new(0);

/// Start (or restart) the window of this submission. No-op unless enabled.
pub(crate) fn begin() {
    if !enabled() { return; }
    if let Some(window) = WINDOW.lock().as_mut() {
        window.started = Instant::now();
        window.marks.clear();
        window.net_calls = 0;
        return;
    }
    *WINDOW.lock() = Some(Window { started: Instant::now(), marks: Vec::new(), net_calls: 0 });
}

/// Record one segment boundary. No-op outside a window.
pub(crate) fn mark(stage: &'static str) {
    if !enabled() { return; }
    let mut guard = WINDOW.lock();
    let Some(window) = guard.as_mut() else { return; };
    let at = window.started.elapsed().as_micros() as u64;
    window.marks.push((stage, at));
}

/// Count one network round trip of this window (RPC or REST). No-op outside it.
pub(crate) fn net_call() {
    if !enabled() { return; }
    let mut guard = WINDOW.lock();
    if let Some(window) = guard.as_mut() { window.net_calls += 1; }
}

/// Close the window, print it once to stderr and keep it for the caller.
pub(crate) fn finish() {
    let window = WINDOW.lock().take();
    let Some(window) = window else { return; };
    let total_us = window.started.elapsed().as_micros() as u64;
    let mut segments: Vec<(String, u64)> = Vec::with_capacity(window.marks.len());
    let mut previous = 0u64;
    for (stage, at) in &window.marks {
        segments.push(((*stage).to_string(), at.saturating_sub(previous)));
        previous = *at;
    }
    // The segments are an ordered array: a stage may occur twice in one
    // path, and an object would drop one of them.
    let body = serde_json::to_string(&segments.iter().map(|(stage, duration_us)| Segment {
        stage: stage.clone(), duration_us: *duration_us,
    }).collect::<Vec<_>>()).expect("integer timing segments serialize");
    eprintln!(
        "YQUANT_TIMING {{\"event\":\"send_path\",\"measurement_revision\":\"{MEASUREMENT_REVISION}\",\"total_us\":{total_us},\"net_calls\":{},\"segments\":{body}}}",
        window.net_calls
    );
    *LAST.lock() = segments;
    LAST_NET_CALLS.store(window.net_calls, Ordering::SeqCst);
    LAST_TOTAL_US.store(total_us, Ordering::SeqCst);
}

/// The segments of the last closed window, in microseconds, in path order.
#[cfg(test)]
pub(crate) fn last_segments() -> Vec<(String, u64)> {
    LAST.lock().clone()
}

/// The network round trips counted in the last closed window.
#[cfg(test)]
pub(crate) fn last_net_calls() -> u64 {
    LAST_NET_CALLS.load(Ordering::SeqCst)
}

/// The whole measured path of the last closed window, in microseconds.
#[cfg(test)]
pub(crate) fn last_total_us() -> u64 {
    LAST_TOTAL_US.load(Ordering::SeqCst)
}

/// Test-only: model one network round trip of the offline synthetic venue.
///
/// Zero unless `YQUANT_SYNTH_RTT_MS` is set, so every existing test keeps its
/// instant transport; a benchmark can price the same path at a realistic RTT
/// without reaching a network.
#[cfg(test)]
pub(crate) async fn synth_round_trip() {
    static MILLIS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let millis = *MILLIS.get_or_init(|| {
        std::env::var("YQUANT_SYNTH_RTT_MS").ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    });
    if millis > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    }
}
