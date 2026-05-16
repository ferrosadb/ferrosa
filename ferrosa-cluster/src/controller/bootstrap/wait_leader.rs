//! Phase 4 — WaitLeader.
//!
//! Pre-condition: the local Raft instance is published.
//! Post-condition: `current_leader().await.is_some()` within the
//! formation deadline (default 30 s, overridable via
//! `formation_timeout_secs`).
//!
//! Failure to satisfy the post-condition surfaces as
//! `BootstrapError::Phase { name: WaitLeader, .. }` — the caller
//! reverts to `Pair` mode (see existing logic at cluster.rs:~1340).

use std::time::Duration;

use super::phase::{BootstrapError, BootstrapPhase};

/// Resolution of the wait-loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderObservation {
    /// A leader was observed before the deadline.
    Elected { node_id: u64, waited: Duration },
    /// The deadline elapsed with no leader.
    Timeout { waited: Duration },
}

/// Pre-condition: the Raft instance must have been created
/// (CreateRaft post-condition `true`).
pub fn precondition(raft_created: bool) -> Result<(), BootstrapError> {
    if raft_created {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::WaitLeader,
            "Raft instance not created",
        ))
    }
}

/// Post-condition: an `Elected` observation is required.
pub fn postcondition(obs: LeaderObservation) -> Result<(), BootstrapError> {
    match obs {
        LeaderObservation::Elected { .. } => Ok(()),
        LeaderObservation::Timeout { waited } => Err(BootstrapError::phase(
            BootstrapPhase::WaitLeader,
            format!("no leader after {} ms", waited.as_millis()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elected_observation_passes_postcondition() {
        let obs = LeaderObservation::Elected {
            node_id: 1,
            waited: Duration::from_millis(120),
        };
        postcondition(obs).expect("elected → ok");
    }

    #[test]
    fn timeout_observation_fails_postcondition() {
        let obs = LeaderObservation::Timeout {
            waited: Duration::from_secs(30),
        };
        let err = postcondition(obs).expect_err("timeout → err");
        assert_eq!(err.name(), BootstrapPhase::WaitLeader);
        let msg = format!("{err}");
        assert!(msg.contains("no leader"), "{msg}");
    }

    #[test]
    fn precondition_requires_raft_created() {
        precondition(true).expect("raft ok");
        assert!(precondition(false).is_err());
    }
}
