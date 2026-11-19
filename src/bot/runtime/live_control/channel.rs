//! The controlled live run's control channel.
//!
//! The parent owns the other end of the standard input pipe. It may send
//! exactly two things:
//!
//! - the single byte `D`, which closes the window, and
//! - an acknowledgement frame `A <seq> <signed_hash>\n`, which confirms that
//!   one critical order's evidence is durable on disk.
//!
//! There is no command channel. The parent cannot reset the run, change an
//! amount, ask for a signature or do anything else: a frame that isn't one of
//! those two is a control-channel failure and stops new risk.
//!
//! The watcher replaces the paper run's byte reader. It still owns the
//! standard input alone — there is never a second reader — and it only ever
//! sets a sticky flag on the broker's own state.

use crate::bot::runtime::live_control::LiveRunState;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The largest a control frame may be.
pub const MAX_FRAME_BYTES: usize = 160;
/// The deadline frame.
pub const DEADLINE_FRAME: u8 = b'D';
/// The acknowledgement frame's first byte.
pub const ACK_FRAME: u8 = b'A';

/// What one frame of the control channel says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlFrame {
    /// The window closed.
    Deadline,
    /// One critical order's evidence is durable.
    Ack {
        /// The sequence number of the order event.
        seq: u64,
        /// The signed hash of the order.
        signed_hash: String,
    },
}

/// Why the control channel failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelFailure {
    /// The frame was not one of the two the protocol allows.
    BadFrame,
    /// An acknowledgement arrived that no order was waiting for.
    UnexpectedAck,
    /// An acknowledgement repeated an order that was already confirmed.
    DuplicateAck,
    /// An acknowledgement arrived out of order.
    OutOfOrderAck,
    /// An acknowledgement named an order that isn't the one being waited for.
    MismatchedAck,
    /// The parent closed the pipe.
    ParentLost,
    /// The pipe could not be read.
    ReadFailed,
}

impl ChannelFailure {
    /// The stable reason token of the failure.
    pub fn reason(self) -> &'static str {
        match self {
            ChannelFailure::BadFrame => "CONTROL_CHANNEL",
            ChannelFailure::UnexpectedAck
            | ChannelFailure::DuplicateAck
            | ChannelFailure::OutOfOrderAck
            | ChannelFailure::MismatchedAck => "CONTROL_CHANNEL",
            ChannelFailure::ParentLost => "PARENT_LOST",
            ChannelFailure::ReadFailed => "PARENT_LOST",
        }
    }
}

/// Whether a signed hash is a strict `0x` plus sixty-four hex characters.
fn is_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse one control frame.
///
/// The frame is exactly `A <seq> <signed_hash>`: one space between the
/// fields, a newline at the end, and nothing else.
pub fn parse_frame(raw: &str) -> Result<ControlFrame, ChannelFailure> {
    if raw == "D" {
        return Ok(ControlFrame::Deadline);
    }
    let body = raw.strip_prefix("A ").ok_or(ChannelFailure::BadFrame)?;
    let body = body.strip_suffix('\n').ok_or(ChannelFailure::BadFrame)?;
    let mut parts = body.split(' ');
    let seq_text = parts.next().ok_or(ChannelFailure::BadFrame)?;
    let hash = parts.next().ok_or(ChannelFailure::BadFrame)?;
    if parts.next().is_some() {
        return Err(ChannelFailure::BadFrame);
    }
    if seq_text.is_empty() || !seq_text.chars().all(|c| c.is_ascii_digit()) {
        return Err(ChannelFailure::BadFrame);
    }
    if seq_text.starts_with('0') && seq_text.len() > 1 {
        return Err(ChannelFailure::BadFrame);
    }
    let seq: u64 = seq_text.parse().map_err(|_| ChannelFailure::BadFrame)?;
    if seq == 0 {
        return Err(ChannelFailure::BadFrame);
    }
    if !is_hash(hash) {
        return Err(ChannelFailure::BadFrame);
    }
    Ok(ControlFrame::Ack {
        seq,
        signed_hash: hash.to_string(),
    })
}

/// The one acknowledgement the run is waiting for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAck {
    pub seq: u64,
    pub signed_hash: String,
}

/// The control channel's own state.
///
/// It holds at most one pending acknowledgement: the run waits for one
/// critical order at a time, and a second one is a protocol failure.
#[derive(Debug, Default)]
pub struct ControlChannel {
    pending: Mutex<Option<PendingAck>>,
    confirmed: Mutex<Vec<PendingAck>>,
    #[cfg(test)]
    pending_ready: std::sync::Condvar,
}

impl ControlChannel {
    pub fn new() -> Self {
        Self::default()
    }

    /// The acknowledgement the run is waiting for.
    pub fn pending(&self) -> Option<PendingAck> {
        self.pending.lock().unwrap().clone()
    }

    /// The confirmations this run received, in order.
    pub fn confirmed(&self) -> Vec<PendingAck> {
        self.confirmed.lock().unwrap().clone()
    }

    /// Start waiting for one order's evidence.
    ///
    /// Only one order may be waiting at a time.
    pub fn expect(&self, seq: u64, signed_hash: &str) -> Result<(), ChannelFailure> {
        let mut pending = self.pending.lock().unwrap();
        if pending.is_some() {
            return Err(ChannelFailure::UnexpectedAck);
        }
        *pending = Some(PendingAck {
            seq,
            signed_hash: signed_hash.to_string(),
        });
        #[cfg(test)]
        self.pending_ready.notify_all();
        Ok(())
    }

    /// Wait for the actual production expect boundary; timeout is only a test watchdog.
    #[cfg(test)]
    pub(crate) fn wait_pending(&self) -> PendingAck {
        let (pending, _) = self.pending_ready.wait_timeout_while(
            self.pending.lock().unwrap(), std::time::Duration::from_secs(3),
            |pending| pending.is_none()).unwrap();
        pending.clone().expect("adapter never produced critical order")
    }

    /// Wait for the production expect boundary, or for the run to finish
    /// without ever producing a critical order.
    ///
    /// A preparation that is refused before the order exists never reaches
    /// `expect`: the acknowledgement has nothing to confirm. The waiter
    /// therefore watches both the real condition variable and the run's own
    /// completion flag, and reports which of the two happened.
    #[cfg(test)]
    pub(crate) fn wait_pending_until(
        &self, finished: &std::sync::atomic::AtomicBool,
    ) -> Option<PendingAck> {
        let mut pending = self.pending.lock().unwrap();
        loop {
            if let Some(waiting) = pending.as_ref() { return Some(waiting.clone()); }
            if finished.load(Ordering::SeqCst) { return None; }
            let (guard, _) = self.pending_ready
                .wait_timeout(pending, std::time::Duration::from_millis(20)).unwrap();
            pending = guard;
        }
    }

    /// Take one acknowledgement.
    ///
    /// The acknowledgement must name the order that is actually waiting: a
    /// different one, a repeated one or one that arrives out of order is a
    /// control-channel failure, not a confirmation.
    pub fn accept(&self, frame: &ControlFrame) -> Result<(), ChannelFailure> {
        let ControlFrame::Ack { seq, signed_hash } = frame else {
            return Err(ChannelFailure::BadFrame);
        };
        let mut pending = self.pending.lock().unwrap();
        let Some(waiting) = pending.take() else {
            return Err(ChannelFailure::UnexpectedAck);
        };
        if *seq != waiting.seq || *signed_hash != waiting.signed_hash {
            // The failed attempt keeps its slot: the run is waiting for the
            // same order still, and the channel is broken.
            *pending = Some(waiting);
            return Err(ChannelFailure::MismatchedAck);
        }
        self.confirmed.lock().unwrap().push(waiting);
        Ok(())
    }

    /// Whether the given order's evidence was confirmed.
    pub fn is_confirmed(&self, seq: u64, signed_hash: &str) -> bool {
        self.confirmed
            .lock()
            .unwrap()
            .iter()
            .any(|ack| ack.seq == seq && ack.signed_hash == signed_hash)
    }
}

/// The watcher's view of the run.
///
/// The watcher only sets flags: it never runs the strategy, never touches
/// the network and never holds a secret.
pub trait ControlWatcher: Send + Sync {
    /// The parent closed the pipe.
    fn parent_lost(&self);
    /// The window closed.
    fn deadline(&self);
    /// The control channel is broken.
    fn channel_failed(&self);
    /// One order's evidence was confirmed.
    fn acknowledged(&self, seq: u64, signed_hash: &str);
}

/// The state a live run's watcher updates.
#[derive(Debug)]
pub struct ChannelState {
    /// The run's state, updated by the watcher.
    pub state: Mutex<LiveRunState>,
    /// The last confirmed acknowledgement.
    pub last_ack: Mutex<Option<PendingAck>>,
    /// Whether the channel failed.
    pub failed: AtomicBool,
    /// The number of frames the watcher read.
    pub frames: AtomicU64,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            state: Mutex::new(LiveRunState::Ready),
            last_ack: Mutex::new(None),
            failed: AtomicBool::new(false),
            frames: AtomicU64::new(0),
        }
    }
}

impl ChannelState {
    /// Whether the channel is still usable.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }
}

impl ControlWatcher for ChannelState {
    fn parent_lost(&self) {
        *self.state.lock().unwrap() = LiveRunState::StopNewRisk;
        self.failed.store(true, Ordering::SeqCst);
    }

    fn deadline(&self) {
        *self.state.lock().unwrap() = LiveRunState::StopNewRisk;
    }

    fn channel_failed(&self) {
        *self.state.lock().unwrap() = LiveRunState::StopNewRisk;
        self.failed.store(true, Ordering::SeqCst);
    }

    fn acknowledged(&self, seq: u64, signed_hash: &str) {
        *self.last_ack.lock().unwrap() = Some(PendingAck {
            seq,
            signed_hash: signed_hash.to_string(),
        });
    }
}

/// The real watcher stops the same Broker whose State linearizes a POST.
/// ACKs live only in that run's unique ControlChannel, not a detached flag.
impl ControlWatcher for crate::bot::runtime::Broker {
    fn parent_lost(&self) {
        self.stop_entries(crate::bot::runtime::ControlledStop::ParentLost);
    }
    fn deadline(&self) {
        self.stop_entries(crate::bot::runtime::ControlledStop::Deadline);
    }
    fn channel_failed(&self) {
        self.stop_entries(crate::bot::runtime::ControlledStop::ControlChannel);
    }
    fn acknowledged(&self, _seq: u64, _signed_hash: &str) {}
}

/// Watch the parent's control pipe.
///
/// The reader owns the standard input alone. It reads frames, never bytes:
/// the deadline frame is one byte, and an acknowledgement frame is a line.
/// A frame that isn't one of the two is a channel failure.
pub fn watch_control_channel(channel: Arc<ControlChannel>, watcher: Arc<dyn ControlWatcher>) {
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin();
        read_control_channel(&mut stdin, channel, watcher);
    });
}

/// Read the production protocol from one owned stream (pipe in production).
/// The same loop is exercised with a finite reader by offline fixtures.
pub(crate) fn read_control_channel(
    reader: &mut dyn Read, channel: Arc<ControlChannel>, watcher: Arc<dyn ControlWatcher>,
) {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 1];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => {
                    // The pipe closed: the parent is gone.
                    if buffer.is_empty() { watcher.parent_lost(); }
                    else { watcher.channel_failed(); }
                    return;
                }
                Ok(_) => {
                    if chunk[0] == DEADLINE_FRAME {
                        if !buffer.is_empty() { watcher.channel_failed(); return; }
                        watcher.deadline();
                        // Keep watching: a close after the deadline still matters.
                        continue;
                    }
                    buffer.push(chunk[0]);
                    if buffer.len() > MAX_FRAME_BYTES {
                        watcher.channel_failed();
                        return;
                    }
                    if chunk[0] != b'\n' {
                        continue;
                    }
                    let raw = match std::str::from_utf8(&buffer) {
                        Ok(raw) => raw.to_string(),
                        Err(_) => {
                            watcher.channel_failed();
                            return;
                        }
                    };
                    buffer.clear();
                    match parse_frame(&raw) {
                        Ok(ControlFrame::Deadline) => watcher.deadline(),
                        Ok(ControlFrame::Ack { seq, signed_hash }) => {
                            match channel.accept(&ControlFrame::Ack {
                                seq,
                                signed_hash: signed_hash.clone(),
                            }) {
                                Ok(()) => watcher.acknowledged(seq, &signed_hash),
                                Err(_) => {
                                    watcher.channel_failed();
                                    return;
                                }
                            }
                        }
                        Err(_) => {
                            watcher.channel_failed();
                            return;
                        }
                    }
                }
                Err(_) => {
                    watcher.parent_lost();
                    return;
                }
            }
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_deadline_frame_is_read() {
        assert_eq!(parse_frame("D"), Ok(ControlFrame::Deadline));
    }

    #[test]
    fn an_acknowledgement_frame_is_read() {
        let hash = format!("0x{}", "a".repeat(64));
        assert_eq!(
            parse_frame(&format!("A 7 {hash}\n")),
            Ok(ControlFrame::Ack {
                seq: 7,
                signed_hash: hash.clone()
            })
        );
    }

    #[test]
    fn a_bad_frame_is_refused() {
        assert_eq!(parse_frame("X"), Err(ChannelFailure::BadFrame));
        assert_eq!(parse_frame("A 7"), Err(ChannelFailure::BadFrame));
        assert_eq!(parse_frame("A 7 notahash\n"), Err(ChannelFailure::BadFrame));
        assert_eq!(
            parse_frame(&format!("A 07 {}\n", "a".repeat(64))),
            Err(ChannelFailure::BadFrame)
        );
        assert_eq!(
            parse_frame(&format!("A 0 {}\n", "a".repeat(64))),
            Err(ChannelFailure::BadFrame)
        );
    }

    #[test]
    fn an_acknowledgement_without_a_waiting_order_is_refused() {
        let channel = ControlChannel::new();
        let hash = format!("0x{}", "a".repeat(64));
        assert_eq!(
            channel.accept(&ControlFrame::Ack {
                seq: 1,
                signed_hash: hash.clone()
            }),
            Err(ChannelFailure::UnexpectedAck)
        );
    }

    #[test]
    fn an_acknowledgement_for_another_order_is_refused() {
        let channel = ControlChannel::new();
        let hash = format!("0x{}", "a".repeat(64));
        let other = format!("0x{}", "b".repeat(64));
        channel.expect(1, &hash).unwrap();
        assert_eq!(
            channel.accept(&ControlFrame::Ack {
                seq: 2,
                signed_hash: hash.clone()
            }),
            Err(ChannelFailure::MismatchedAck)
        );
        // The failed acknowledgement leaves the order still waiting.
        assert_eq!(channel.pending().unwrap().seq, 1);
    }

    #[test]
    fn a_matching_acknowledgement_is_confirmed_once() {
        let channel = ControlChannel::new();
        let hash = format!("0x{}", "a".repeat(64));
        channel.expect(1, &hash).unwrap();
        assert!(channel
            .accept(&ControlFrame::Ack {
                seq: 1,
                signed_hash: hash.clone()
            })
            .is_ok());
        assert!(channel.is_confirmed(1, &hash));
        // A second one has nothing to confirm.
        assert_eq!(
            channel.accept(&ControlFrame::Ack {
                seq: 1,
                signed_hash: hash.clone()
            }),
            Err(ChannelFailure::UnexpectedAck)
        );
    }

    #[test]
    fn two_orders_cannot_wait_at_once() {
        let channel = ControlChannel::new();
        let hash = format!("0x{}", "a".repeat(64));
        channel.expect(1, &hash).unwrap();
        assert_eq!(channel.expect(2, &hash), Err(ChannelFailure::UnexpectedAck));
    }

    #[test]
    fn the_watcher_stops_the_run_when_the_parent_is_gone() {
        let state = Arc::new(ChannelState::default());
        let watcher: Arc<dyn ControlWatcher> = state.clone();
        watcher.parent_lost();
        assert_eq!(*state.state.lock().unwrap(), LiveRunState::StopNewRisk);
        assert!(state.is_failed());
    }
}
