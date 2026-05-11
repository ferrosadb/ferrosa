//! Phase 1 — DeliverInvites (W4.2).
//!
//! On entry we are in [`crate::mode::DeploymentMode::Forming`] with a
//! committed peer list.  The phase multicasts a `ClusterInvite` to
//! every peer and acks them off as they reply.  When all peers have
//! acked, the phase post-condition is satisfied and the bootstrap
//! pipeline advances to [`super::BootstrapPhase::EstablishPools`].
//!
//! This module exposes the *pure* pre/post-condition logic so it can be
//! unit-tested in isolation. The networking side-effect lives in the
//! existing imperative bootstrap task in `controller/cluster.rs`; the
//! follow-on rewire (after Sprint 6's multi-Raft scaffolding) will
//! replace that imperative section with a call into this module.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::phase::{BootstrapError, BootstrapPhase};
use crate::mode::DeploymentMode;

/// View of the runtime state DeliverInvites needs.
#[derive(Clone, Debug)]
pub struct DeliverInvitesState {
    /// Current deployment mode.  Pre-condition is `Forming`.
    pub mode: DeploymentMode,
    /// Peers we expect to invite (excluding self).
    pub expected_peers: BTreeSet<Uuid>,
    /// Peers that have acked the `ClusterInviteAck`.
    pub acked_peers: BTreeSet<Uuid>,
}

/// Pre-condition: we must be in Forming mode.  Without that, the
/// invite multicast is a no-op (pair / standalone routes through other
/// transitions) and we abort the phase rather than emit confusing
/// invites.
pub fn precondition(state: &DeliverInvitesState) -> Result<(), BootstrapError> {
    match state.mode {
        DeploymentMode::Forming => Ok(()),
        other => Err(BootstrapError::phase(
            BootstrapPhase::DeliverInvites,
            format!("expected mode=Forming, got {other:?}"),
        )),
    }
}

/// Post-condition: every expected peer must appear in `acked_peers`.
///
/// Returns the missing peers in sorted order so callers can log
/// deterministically.
pub fn postcondition(state: &DeliverInvitesState) -> Result<(), BootstrapError> {
    let missing: Vec<Uuid> = state
        .expected_peers
        .difference(&state.acked_peers)
        .copied()
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::DeliverInvites,
            format!(
                "{n} peer(s) did not ack ClusterInvite: {missing:?}",
                n = missing.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(byte: u8) -> Uuid {
        Uuid::from_bytes([byte; 16])
    }

    /// W4.2 RED → GREEN: 3-node setup, every peer acks → post-condition
    /// is satisfied; the phase reports `Ok(())`.
    #[test]
    fn deliver_invites_succeeds_to_all_peers() {
        let mut state = DeliverInvitesState {
            mode: DeploymentMode::Forming,
            expected_peers: [uuid(2), uuid(3)].into_iter().collect(),
            acked_peers: BTreeSet::new(),
        };
        precondition(&state).expect("Forming → pre passes");
        // Before anyone acks, post fails — gives the failure-mode test
        // a deterministic signal to wait on.
        assert!(postcondition(&state).is_err());

        // Each peer acks; the phase post-condition flips to OK.
        state.acked_peers.insert(uuid(2));
        state.acked_peers.insert(uuid(3));
        postcondition(&state).expect("post-condition holds with all acks");
    }

    #[test]
    fn precondition_rejects_non_forming_mode() {
        let state = DeliverInvitesState {
            mode: DeploymentMode::Cluster,
            expected_peers: BTreeSet::new(),
            acked_peers: BTreeSet::new(),
        };
        let err = precondition(&state).expect_err("Cluster mode → pre fails");
        assert_eq!(err.name(), BootstrapPhase::DeliverInvites);
    }

    #[test]
    fn postcondition_lists_missing_peers() {
        let state = DeliverInvitesState {
            mode: DeploymentMode::Forming,
            expected_peers: [uuid(2), uuid(3), uuid(4)].into_iter().collect(),
            acked_peers: [uuid(2)].into_iter().collect(),
        };
        let err = postcondition(&state).expect_err("missing acks → post fails");
        let msg = format!("{err}");
        assert!(msg.contains("did not ack"), "{msg}");
    }
}
