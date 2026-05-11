//! Phase 8 — DrainQueue (W4.9).
//!
//! Pre-condition: Promote completed.
//! Post-condition: the DDL queue receiver is empty AND every queued DDL
//! has been applied through the cluster path.
//!
//! Today the imperative bootstrap calls `drain_ddl_queue` (defined in
//! `controller/cluster.rs`) to wait for the channel to settle.  This
//! module restates the post-condition: `(channel_empty &&
//! applied == enqueued)`.

use super::phase::{BootstrapError, BootstrapPhase};

/// Snapshot of the drain progress.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainQueueState {
    /// `true` once the unbounded receiver returned `Empty`
    /// `REQUIRED_CONSECUTIVE_EMPTY` times in a row.
    pub channel_empty: bool,
    /// Total ops observed in the queue (across the entire Forming
    /// window).
    pub enqueued: u64,
    /// Total ops actually applied through the cluster path.
    pub applied: u64,
}

pub fn precondition(promoted: bool) -> Result<(), BootstrapError> {
    if promoted {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            "Promote post-condition not satisfied",
        ))
    }
}

pub fn postcondition(state: DrainQueueState) -> Result<(), BootstrapError> {
    if !state.channel_empty {
        return Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            "DDL queue receiver is not yet empty",
        ));
    }
    if state.applied != state.enqueued {
        return Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            format!(
                "applied {} of {} queued DDL ops",
                state.applied, state.enqueued
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_queue_postcondition_holds_when_empty_and_applied_matches() {
        let state = DrainQueueState {
            channel_empty: true,
            enqueued: 3,
            applied: 3,
        };
        precondition(true).expect("promote ok");
        postcondition(state).expect("drain complete");
    }

    #[test]
    fn drain_queue_flags_unapplied_op() {
        let state = DrainQueueState {
            channel_empty: true,
            enqueued: 3,
            applied: 2,
        };
        let err = postcondition(state).expect_err("missing apply → err");
        assert_eq!(err.name(), BootstrapPhase::DrainQueue);
    }

    #[test]
    fn drain_queue_flags_non_empty_channel() {
        let state = DrainQueueState {
            channel_empty: false,
            enqueued: 3,
            applied: 3,
        };
        assert!(postcondition(state).is_err());
    }

    #[test]
    fn drain_queue_precondition_requires_promoted() {
        assert!(precondition(false).is_err());
    }
}
