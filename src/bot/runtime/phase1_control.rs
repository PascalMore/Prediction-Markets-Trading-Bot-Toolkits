//! The phase1 protocol's own controls, shared by paper and live.
//!
//! The four controls of the phase1 protocol are: whether the run speaks it,
//! which clock it reads, whether it may still add risk, and the run's output
//! sequence. They used to live inside the paper cash, which made them
//! unreachable to a live run that has no paper cash at all.
//!
//! They live here now. The paper run keeps its own cash and its own
//! behaviour; the live run keeps its own atomic ledger. What they share is
//! this: the protocol's controls, not the money.

use crate::bot::arbitrage::phase1::StrategyContract;
use crate::bot::runtime::live_control::ControlledLiveState;
use crate::bot::runtime::paper::PaperCash;
use crate::bot::runtime::{Broker, State};
use crate::service::observation::Clock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// The phase1 state of a controlled run.
///
/// It lives inside the broker's own lock, next to the cash or next to the
/// live ledger: the event sequence and the output failure belong to the same
/// state, not to a second ledger.
#[derive(Clone)]
pub struct Phase1Run {
    /// The one clock the whole run reads.
    pub clock: Arc<dyn Clock>,
    /// The economics this run decides under.
    pub contract: StrategyContract,
    /// The minimum edge per gross share.
    pub min_edge: f64,
    /// The least time a round may have left.
    pub min_secs_left: f64,
    /// The next event sequence number.
    pub event_seq: Arc<AtomicU64>,
    /// A failed output cannot be retried or presented as complete evidence.
    pub output_failed: Arc<AtomicBool>,
}

impl std::fmt::Debug for Phase1Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Phase1Run")
            .field("contract", &self.contract)
            .field("min_edge", &self.min_edge)
            .field("min_secs_left", &self.min_secs_left)
            .finish()
    }
}

impl Phase1Run {
    /// Build the run's protocol state.
    pub fn new(
        clock: Arc<dyn Clock>,
        contract: StrategyContract,
        min_edge: f64,
        min_secs_left: f64,
    ) -> Self {
        Self {
            clock,
            contract,
            min_edge,
            min_secs_left,
            event_seq: Arc::new(AtomicU64::new(0)),
            output_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether an output already failed.
    pub fn output_failed(&self) -> bool {
        self.output_failed.load(Ordering::SeqCst)
    }
}

impl Broker {
    /// Install the phase1 state of this controlled run.
    ///
    /// Only a controlled run gets it: the clock, the economics and the event
    /// sequence belong to that run and to no other.
    pub fn with_phase1(
        self,
        clock: Arc<dyn Clock>,
        contract: StrategyContract,
        min_edge: f64,
        min_secs_left: f64,
    ) -> anyhow::Result<Self> {
        if self.controlled.is_none() {
            anyhow::bail!("CONTROLLED_MODE: only a controlled run speaks the phase1 protocol");
        }
        let run = Phase1Run::new(clock, contract, min_edge, min_secs_left);
        {
            let mut state = self.state.lock();
            let Some(paper) = state.paper.as_mut() else {
                anyhow::bail!("CONTROLLED_MODE: the controlled cash is missing");
            };
            paper.phase1 = Some(run);
        }
        Ok(self)
    }

    /// Whether this run speaks the phase1 protocol.
    ///
    /// A paper run speaks it when its paper cash carries the protocol; a live
    /// run speaks it when its controlled state does. An ordinary run speaks
    /// neither, exactly as before.
    pub fn is_phase1(&self) -> bool {
        let state = self.state.lock();
        if state
            .paper
            .as_ref()
            .is_some_and(|paper| paper.phase1.is_some())
        {
            return true;
        }
        state
            .live
            .controlled
            .as_ref()
            .is_some_and(|live| live.phase1.contract == StrategyContract::Phase1NetShares)
    }

    /// Same-lock lifecycle checks used before emitting a selected attempt.
    pub fn phase1_entries_stopped(&self) -> bool {
        let state = self.state.lock();
        self.controlled_stopped()
            || state.entry_stop_reason.is_some()
            || state.halted
            || state
                .paper
                .as_ref()
                .and_then(|paper| paper.phase1.as_ref())
                .is_some_and(|run| run.output_failed.load(Ordering::SeqCst))
            || state.paper.as_ref().is_some_and(|paper| paper.loss_latched)
            || state
                .live
                .controlled
                .as_ref()
                .is_some_and(|live| !live.may_add_risk())
    }

    /// The clock of this run, when it speaks the phase1 protocol.
    pub fn phase1_clock(&self) -> Option<Arc<dyn Clock>> {
        let state = self.state.lock();
        if let Some(paper) = state.paper.as_ref().and_then(|paper| paper.phase1.as_ref()) {
            return Some(Arc::clone(&paper.clock));
        }
        state
            .live
            .controlled
            .as_ref()
            .map(|live| Arc::clone(&live.phase1.clock))
    }
}

/// The paper run's phase1 state, if it has one.
pub fn paper_phase1(state: &State) -> Option<&Phase1Run> {
    state.paper.as_ref().and_then(|paper| paper.phase1.as_ref())
}

/// The live run's phase1 state, if it has one.
pub fn live_phase1(state: &State) -> Option<&ControlledLiveState> {
    state.live.controlled.as_ref()
}

/// Whether the paper cash of this state carries the phase1 protocol.
pub fn paper_speaks_phase1(paper: &PaperCash) -> bool {
    paper.phase1.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ControlledMode, ControlledRun};
    use crate::service::observation::SystemClock;
    use chrono::Utc;

    fn run() -> ControlledRun {
        ControlledRun::new(ControlledMode::UnsignedPaper, "run-1".into(), Utc::now()).unwrap()
    }

    #[test]
    fn an_ordinary_broker_does_not_speak_phase1() {
        let broker = Broker::new(
            crate::bot::runtime::StrategyLimits::default(),
            false,
            None,
            None,
        )
        .unwrap();
        assert!(!broker.is_phase1());
        assert!(broker.phase1_clock().is_none());
    }

    #[test]
    fn the_phase1_state_needs_a_controlled_run() {
        let broker = Broker::new(
            crate::bot::runtime::StrategyLimits::default(),
            false,
            None,
            None,
        )
        .unwrap();
        let error = broker
            .with_phase1(
                Arc::new(SystemClock::new().unwrap()),
                StrategyContract::Phase1NetShares,
                0.03,
                5.0,
            )
            .err()
            .expect("a broker without a controlled run refuses the phase1 protocol");
        assert!(error.to_string().contains("CONTROLLED_MODE"));
    }

    #[test]
    fn a_paper_run_speaks_phase1_once_it_is_installed() {
        let broker = Broker::new(
            crate::bot::runtime::StrategyLimits::default(),
            false,
            None,
            None,
        )
        .unwrap()
        .with_controlled_paper(run(), 100.0)
        .unwrap()
        .with_phase1(
            Arc::new(SystemClock::new().unwrap()),
            StrategyContract::Phase1NetShares,
            0.03,
            5.0,
        )
        .unwrap();
        assert!(broker.is_phase1());
        assert!(broker.phase1_clock().is_some());
        assert!(!broker.phase1_entries_stopped());
    }
}
