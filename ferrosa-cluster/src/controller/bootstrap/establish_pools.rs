//! Phase 2 — EstablishPools.
//!
//! Pre-condition: every expected peer has a registered identity (UUID
//! known to the network factory).
//! Post-condition: every peer has an outbound pool live on **both**
//! `Lane::Raft` and `Lane::Data`.  Raft RPCs travel on `Lane::Raft`;
//! ClusterCoordinator and streaming use `Lane::Data`.
//!
//! Today the imperative `transition_to_cluster` calls
//! [`ferrosa_net::pool::PriorityPool::connect`] in a `spawn_tracked`
//! per peer (~ll. 347-367) and then waits up to 10 s for live-peer
//! status before creating Raft. This module captures the invariant:
//! a phase post-condition is satisfied only when **all** Raft+Data
//! lanes are observable to the [`ferrosa_net::peer::PeerManager`].

use std::collections::{BTreeMap, BTreeSet};

use uuid::Uuid;

use super::phase::{BootstrapError, BootstrapPhase};

/// Bitmask of the lanes that must be live for a peer.  Stored per peer
/// because an established pool may carry one but not the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerLaneStatus {
    pub raft: bool,
    pub data: bool,
}

impl PeerLaneStatus {
    pub fn both_live(self) -> bool {
        self.raft && self.data
    }
}

/// View the EstablishPools phase needs.  Tests can populate this
/// directly; the live bootstrap task derives it from
/// `peer_manager.has_live_peer` checks per lane.
#[derive(Clone, Debug)]
pub struct EstablishPoolsState {
    pub expected_peers: BTreeSet<Uuid>,
    pub lane_status: BTreeMap<Uuid, PeerLaneStatus>,
}

/// Pre-condition: the expected peer set is non-empty.  A single-node
/// cluster never enters EstablishPools (it skips straight to
/// CreateRaft).
pub fn precondition(state: &EstablishPoolsState) -> Result<(), BootstrapError> {
    if state.expected_peers.is_empty() {
        Err(BootstrapError::phase(
            BootstrapPhase::EstablishPools,
            "no peers to establish pools for",
        ))
    } else {
        Ok(())
    }
}

/// Post-condition: every expected peer has *both* `Lane::Raft` and
/// `Lane::Data` live.
pub fn postcondition(state: &EstablishPoolsState) -> Result<(), BootstrapError> {
    let mut missing: Vec<(Uuid, PeerLaneStatus)> = Vec::new();
    for peer in &state.expected_peers {
        let status = state.lane_status.get(peer).copied().unwrap_or_default();
        if !status.both_live() {
            missing.push((*peer, status));
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::EstablishPools,
            format!(
                "{n} peer(s) missing lane(s): {missing:?}",
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

    #[test]
    fn pools_postcondition_holds_when_both_lanes_live_for_every_peer() {
        let mut lane_status = BTreeMap::new();
        lane_status.insert(
            uuid(2),
            PeerLaneStatus {
                raft: true,
                data: true,
            },
        );
        lane_status.insert(
            uuid(3),
            PeerLaneStatus {
                raft: true,
                data: true,
            },
        );
        let state = EstablishPoolsState {
            expected_peers: [uuid(2), uuid(3)].into_iter().collect(),
            lane_status,
        };
        precondition(&state).expect("non-empty peer set passes pre");
        postcondition(&state).expect("both lanes live for all peers");
    }

    #[test]
    fn postcondition_flags_peer_missing_data_lane() {
        let mut lane_status = BTreeMap::new();
        lane_status.insert(
            uuid(2),
            PeerLaneStatus {
                raft: true,
                data: false,
            },
        );
        let state = EstablishPoolsState {
            expected_peers: [uuid(2)].into_iter().collect(),
            lane_status,
        };
        let err = postcondition(&state).expect_err("data lane missing → fail");
        assert_eq!(err.name(), BootstrapPhase::EstablishPools);
    }

    #[test]
    fn postcondition_flags_unknown_peer() {
        let state = EstablishPoolsState {
            expected_peers: [uuid(2)].into_iter().collect(),
            lane_status: BTreeMap::new(),
        };
        assert!(postcondition(&state).is_err());
    }

    #[test]
    fn precondition_rejects_empty_peer_set() {
        let state = EstablishPoolsState {
            expected_peers: BTreeSet::new(),
            lane_status: BTreeMap::new(),
        };
        assert!(precondition(&state).is_err());
    }
}
