//! The read-only seam the post-entry observer uses.
//!
//! Two things live here: the broker's snapshot of its own inventory, taken
//! under the broker's own lock, and the paper model's wall-clock reading.
//!
//! The snapshot is a copy. It never returns a reference into the broker's
//! state, it never mutates anything, and it never decides anything: it does
//! not check whether entries may continue, it does not refresh a mark and it
//! does not read the clock. It reads the flags that are already there.

use super::{Broker, Settlement};
use crate::service::venue::TradableMarket;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

/// One position, as the broker holds it right now.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedPosition {
    pub market_id: u64,
    pub token: String,
    /// The net shares the position is worth.
    pub shares: f64,
    /// The cash cost of those shares.
    pub cost: f64,
}

/// The broker's own facts, read under its own lock.
///
/// It is a snapshot of one instant. Everything in it was already true before
/// the snapshot was taken.
#[derive(Debug, Clone, Default)]
pub struct BrokerObservationSnapshot {
    /// The open positions, by token.
    pub positions: HashMap<String, ObservedPosition>,
    /// The first-fill identity of every market this run traded.
    pub traded: HashMap<u64, TradableMarket>,
    /// The markets the venue has officially settled.
    pub settled_markets: HashSet<u64>,
    /// Whether new entries have stopped, by the flags that already say so.
    pub entries_stopped: bool,
    /// The trading event sequence as it stands. It is read, never advanced.
    pub after_event_seq: u64,
}

impl BrokerObservationSnapshot {
    /// The position of one token.
    pub fn position_of(&self, token: &str) -> Option<(f64, f64)> {
        self.positions
            .get(token)
            .map(|position| (position.shares, position.cost))
    }

    /// The market a token belongs to.
    pub fn market_of(&self, token: &str) -> Option<u64> {
        self.positions.get(token).map(|position| position.market_id)
    }

    /// The first-fill identity of the market a token belongs to.
    pub fn market_identity(&self, token: &str) -> Option<&TradableMarket> {
        self.market_of(token)
            .and_then(|market_id| self.traded.get(&market_id))
    }

    /// The tokens this snapshot holds.
    pub fn tokens(&self) -> Vec<String> {
        let mut tokens: Vec<String> = self.positions.keys().cloned().collect();
        tokens.sort();
        tokens
    }
}
impl Broker {
    /// Read the broker's own facts for an observation.
    ///
    /// The lock is taken once, and only facts are copied out of it. Nothing is
    /// refreshed, nothing is decided and the clock is not read: a snapshot
    /// must not become a second decision point.
    pub fn post_entry_snapshot(&self) -> BrokerObservationSnapshot {
        let state = self.state.lock();
        let mut positions = HashMap::new();
        for (token, holding) in &state.holdings {
            if holding.shares > 0.0 {
                positions.insert(
                    token.clone(),
                    ObservedPosition {
                        market_id: holding.market_id,
                        token: token.clone(),
                        shares: holding.shares,
                        cost: holding.cost,
                    },
                );
            }
        }
        let mut traded = HashMap::new();
        let mut settled_markets = HashSet::new();
        let mut after_event_seq = 0;
        let mut entries_stopped = state.entry_stop_reason.is_some()
            || state.halted
            || state.paper.as_ref().is_some_and(|paper| paper.loss_latched);
        if let Some(paper) = state.paper.as_ref() {
            traded = paper.traded.clone();
            settled_markets = paper
                .settlements
                .iter()
                .filter(|(_, settlement)| **settlement == Settlement::Confirmed)
                .map(|(market_id, _)| *market_id)
                .collect();
            if let Some(run) = paper.phase1.as_ref() {
                after_event_seq = run.event_seq.load(Ordering::SeqCst);
                entries_stopped |= run.output_failed.load(Ordering::SeqCst);
            }
        }
        BrokerObservationSnapshot {
            positions,
            traded,
            settled_markets,
            entries_stopped,
            after_event_seq,
        }
    }
}

/// The paper model's wall-clock reading.
///
/// Production reads the clock exactly where it always did. A test may freeze
/// it, so that the same run can be compared twice: only the code that owns
/// this seam can move it, and it cannot be moved from the environment or from
/// a public argument.
pub fn paper_now() -> DateTime<Utc> {
    #[cfg(test)]
    if let Some(frozen) = FROZEN_NOW.with(|cell| cell.get()) {
        return frozen;
    }
    Utc::now()
}

// The frozen reading, when a test set one.
#[cfg(test)]
thread_local! {
    static FROZEN_NOW: std::cell::Cell<Option<DateTime<Utc>>> = const { std::cell::Cell::new(None) };
}

/// Freeze the paper model's wall clock, or release it again.
#[cfg(test)]
pub(crate) fn freeze_paper_now(at: Option<DateTime<Utc>>) {
    FROZEN_NOW.with(|cell| cell.set(at));
}
