//! `SimulatedNode` — one Raft participant inside the deterministic
//! simulator.
//!
//! Sprint 5 W5.2.  A `SimulatedNode` carries the **protocol-level**
//! state every TLA+ action reads or writes: term, vote, log,
//! `RoleState`, and a derived `DeploymentMode` mirroring
//! `ferrosa-cluster::mode::DeploymentMode`.
//!
//! Only the fields the spec at `specs/tla/raft.tla` cares about are
//! modelled.  The full `FerrosRaft` (sled, networking, schema replay)
//! is *not* run under sim — that job belongs to the in-process test
//! harness in `ferrosa-cluster/tests/`.

use crate::deployment::DeploymentMode;

/// Identifier for a node inside the simulator.
///
/// Mirrors openraft's `NodeId = u64` so trace events from the real
/// engine can be replayed against `SimulatedNode`s 1:1.
pub type NodeId = u64;

/// Raft role for a [`SimulatedNode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Role {
    /// Has not begun participating; produced only at startup.
    PreVoter,
    /// Voting follower (the steady state for non-leaders).
    Follower,
    /// Running PreVote (Sprint 3 introduced this; Sprint 5 W5.8 models it).
    PreCandidate,
    /// Running a full vote round.
    Candidate,
    /// Holds leadership for the current term.
    Leader,
}

/// One Raft participant in the simulator.
///
/// W5.2 RED → GREEN: a brand-new `SimulatedNode` is in role
/// [`Role::PreVoter`] with `term = 0`, no vote, an empty log, and
/// reports [`DeploymentMode::Standalone`].
///
/// `id` is the only required parameter — clusters in W5.3 call
/// [`SimulatedNode::new`] once per voter.
#[derive(Clone, Debug)]
pub struct SimulatedNode {
    /// Stable identifier for this node within the simulated cluster.
    pub id: NodeId,
    /// Current Raft role.
    pub role: Role,
    /// `currentTerm` from the Raft paper.
    pub term: u64,
    /// `votedFor` for the current term, if any.
    pub voted_for: Option<NodeId>,
    /// Length of the local log.  W5.7+ extends this to a full
    /// `Vec<LogEntry>`; the W5.2 minimum is a counter.
    pub log_len: u64,
    /// Highest log index known to be committed.
    pub commit_index: u64,
}

impl SimulatedNode {
    /// Construct a fresh node at term 0 with an empty log.
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            role: Role::PreVoter,
            term: 0,
            voted_for: None,
            log_len: 0,
            commit_index: 0,
        }
    }

    /// Project the protocol state onto a [`DeploymentMode`].
    ///
    /// The mapping is intentionally narrow — only the modes the W5.2
    /// test asserts on are required:
    ///
    /// - A solitary node (no peers seen) is always `Standalone`.
    /// - Cluster transitions are derived inside the cluster object
    ///   in W5.3, not here.
    pub fn deployment_mode(&self, peer_count: usize) -> DeploymentMode {
        DeploymentMode::from_peer_count(peer_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W5.2 RED → GREEN: a single `SimulatedNode` with no peers
    /// reports [`DeploymentMode::Standalone`].  Pre-Sprint-5 this
    /// type did not exist; the test pins the contract.
    #[test]
    fn madsim_runs_single_node() {
        let node = SimulatedNode::new(1);
        assert_eq!(node.id, 1);
        assert_eq!(node.role, Role::PreVoter);
        assert_eq!(node.term, 0);
        assert_eq!(node.voted_for, None);
        assert_eq!(node.log_len, 0);
        assert_eq!(node.deployment_mode(0), DeploymentMode::Standalone);
    }
}
