//! Phase 6 — BootstrapStream (W4.7).
//!
//! Pre-condition: schema replay complete on every node.
//! Post-condition: every owning replica has streamed its share of the
//! token-redistribution payload.  Operationally, the leader iterates
//! the [`crate::ring::TokenRing`] to determine which replicas owe
//! data to which joiners and tracks completion via
//! `BootstrapComplete` RPC acks.

use std::collections::{BTreeMap, BTreeSet};

use super::phase::{BootstrapError, BootstrapPhase};

/// Per-replica streaming progress.  `expected_owners` is every
/// replica that owes data to the joining set; `completed_owners` is
/// every replica that has sent `BootstrapComplete`.
#[derive(Clone, Debug)]
pub struct BootstrapStreamState {
    pub expected_owners: BTreeSet<u64>,
    pub completed_owners: BTreeSet<u64>,
    /// For diagnostics: per-replica byte counter (zero for empty
    /// keyspaces — still counts as "completed" once the ack lands).
    pub bytes_streamed: BTreeMap<u64, u64>,
}

pub fn precondition(schema_replayed: bool) -> Result<(), BootstrapError> {
    if schema_replayed {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::BootstrapStream,
            "ReplaySchema post-condition not satisfied",
        ))
    }
}

pub fn postcondition(state: &BootstrapStreamState) -> Result<(), BootstrapError> {
    let missing: Vec<u64> = state
        .expected_owners
        .difference(&state.completed_owners)
        .copied()
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::BootstrapStream,
            format!(
                "{n} replica(s) did not finish streaming: {missing:?}",
                n = missing.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_stream_postcondition_holds_when_all_owners_complete() {
        let state = BootstrapStreamState {
            expected_owners: [1, 2, 3].into_iter().collect(),
            completed_owners: [1, 2, 3].into_iter().collect(),
            bytes_streamed: BTreeMap::new(),
        };
        precondition(true).expect("replay ok");
        postcondition(&state).expect("all owners completed");
    }

    #[test]
    fn bootstrap_stream_flags_uncompleted_replica() {
        let state = BootstrapStreamState {
            expected_owners: [1, 2, 3].into_iter().collect(),
            completed_owners: [1, 2].into_iter().collect(),
            bytes_streamed: BTreeMap::new(),
        };
        let err = postcondition(&state).expect_err("missing replica → fail");
        assert_eq!(err.name(), BootstrapPhase::BootstrapStream);
    }

    #[test]
    fn bootstrap_stream_precondition_requires_replay() {
        assert!(precondition(false).is_err());
    }
}
