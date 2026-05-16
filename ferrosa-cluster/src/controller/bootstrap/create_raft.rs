//! Phase 3 — CreateRaft.
//!
//! Constructs the `FerrosRaft` instance and publishes the resulting
//! `Arc<FerrosRaft>` to the three watch / swap sinks the rest of the
//! controller observes:
//!
//! 1. `raft_tx` — the `LazyRaft` watch consumed by Raft RPC handlers.
//! 2. `raft_instance_swap` — the controller-level `ArcSwap` so
//!    `controller.raft()` returns `Some` to external callers.
//! 3. The `DdlPath` rebuild path consumes the same Arc when the
//!    DDL path is swapped from `Direct` → `Cluster` after leader
//!    election.
//!
//! Pre-condition: previous phase (EstablishPools) succeeded — captured
//! here as a boolean that the caller passes through.
//! Post-condition: the three sinks all observe a non-null Raft Arc.

use super::phase::{BootstrapError, BootstrapPhase};

/// Tracks publication of the Raft instance to its three sinks.
///
/// Booleans are sufficient — the actual `Arc` is shared by reference
/// in the live system, and tests want to assert "did publication
/// happen for sink X" without depending on the concrete Raft type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CreateRaftSinks {
    pub raft_tx_published: bool,
    pub raft_instance_swap_published: bool,
    pub ddl_path_published: bool,
}

impl CreateRaftSinks {
    pub fn all_published(self) -> bool {
        self.raft_tx_published && self.raft_instance_swap_published && self.ddl_path_published
    }
}

/// Pre-condition: the upstream phases must have completed cleanly.  We
/// don't reconstruct their state here — the caller passes the
/// EstablishPools post-condition outcome as a boolean.
pub fn precondition(pools_established: bool) -> Result<(), BootstrapError> {
    if pools_established {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::CreateRaft,
            "EstablishPools post-condition not satisfied",
        ))
    }
}

/// Post-condition: the Raft Arc has been published to all three sinks.
pub fn postcondition(sinks: CreateRaftSinks) -> Result<(), BootstrapError> {
    if sinks.all_published() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::CreateRaft,
            format!("not all Raft sinks published: {sinks:?}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precondition_requires_pools_established() {
        precondition(true).expect("pools ok");
        let err = precondition(false).expect_err("pools missing → fail");
        assert_eq!(err.name(), BootstrapPhase::CreateRaft);
    }

    #[test]
    fn postcondition_requires_all_three_sinks() {
        let all = CreateRaftSinks {
            raft_tx_published: true,
            raft_instance_swap_published: true,
            ddl_path_published: true,
        };
        postcondition(all).expect("all sinks → ok");

        let missing_ddl = CreateRaftSinks {
            ddl_path_published: false,
            ..all
        };
        assert!(postcondition(missing_ddl).is_err());

        let missing_swap = CreateRaftSinks {
            raft_instance_swap_published: false,
            ..all
        };
        assert!(postcondition(missing_swap).is_err());

        let missing_tx = CreateRaftSinks {
            raft_tx_published: false,
            ..all
        };
        assert!(postcondition(missing_tx).is_err());
    }
}
