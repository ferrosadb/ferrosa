//! Electorate membership management for Accord consensus.
//!
//! The electorate is the set of replicas that participate in Accord protocol
//! rounds (PreAccept, Accept, Commit). When a new node joins or an existing
//! node leaves, the electorate must be reconfigured safely.
//!
//! ## JoinElectorate Protocol (A7.3)
//!
//! A new member must pass through four gates before participating:
//! 1. **Discovery**: Node announces itself and receives the current electorate.
//! 2. **HistorySync**: Node receives protocol history (committed txns) for
//!    all token ranges it will own.
//! 3. **CatchUp**: Node replays history to build local state.
//! 4. **Active**: Node is added to the electorate and may participate.
//!
//! ## Electorate Shrink (A7.4)
//!
//! When a node leaves (graceful decommission or detected failure):
//! 1. The quorum size is recalculated for the new electorate size.
//! 2. Votes from the departed node are invalidated.
//! 3. Stale-epoch responses from the departed node are rejected.

use std::collections::{HashMap, HashSet};

use super::epoch::Epoch;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Unique identifier for a cluster node.
pub type NodeId = u64;

/// Gate that a joining node must pass through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JoinGate {
    /// Node has announced itself.
    Discovery = 0,
    /// Node has received protocol history.
    HistorySync = 1,
    /// Node has replayed history and built local state.
    CatchUp = 2,
    /// Node is fully active in the electorate.
    Active = 3,
}

/// Status of a node in the electorate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    /// Node is a full member and may participate in protocol rounds.
    Active,
    /// Node is joining — gate indicates progress.
    Joining(JoinGate),
    /// Node has been removed from the electorate.
    Removed,
}

/// A record of the protocol history sent to a joining node.
#[derive(Debug, Clone)]
pub struct ProtocolHistory {
    /// Epoch at the time of history capture.
    pub epoch: Epoch,
    /// Number of committed transactions included.
    pub committed_count: usize,
    /// Token ranges covered.
    pub token_ranges: Vec<(i64, i64)>,
}

/// Error during electorate operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectorateError {
    /// Node attempted to participate before completing all gates.
    PrematureParticipation { node: NodeId, gate: JoinGate },
    /// Node is not a member of the electorate.
    NotMember(NodeId),
    /// Vote from a node not in the current electorate.
    InvalidVote {
        node: NodeId,
        electorate_epoch: Epoch,
    },
    /// Response from a stale epoch.
    StaleEpoch {
        node: NodeId,
        msg_epoch: Epoch,
        current_epoch: Epoch,
    },
    /// Node is already a member.
    AlreadyMember(NodeId),
}

impl std::fmt::Display for ElectorateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElectorateError::PrematureParticipation { node, gate } => {
                write!(
                    f,
                    "node {} attempted participation at gate {:?}",
                    node, gate
                )
            }
            ElectorateError::NotMember(n) => write!(f, "node {} is not a member", n),
            ElectorateError::InvalidVote {
                node,
                electorate_epoch,
            } => {
                write!(
                    f,
                    "vote from non-member {} (epoch {})",
                    node, electorate_epoch
                )
            }
            ElectorateError::StaleEpoch {
                node,
                msg_epoch,
                current_epoch,
            } => {
                write!(
                    f,
                    "stale epoch from node {}: msg epoch {} < current {}",
                    node, msg_epoch, current_epoch
                )
            }
            ElectorateError::AlreadyMember(n) => write!(f, "node {} is already a member", n),
        }
    }
}

impl std::error::Error for ElectorateError {}

// ---------------------------------------------------------------------------
// Electorate
// ---------------------------------------------------------------------------

/// Manages the set of nodes participating in Accord consensus.
pub struct Electorate {
    /// Current epoch.
    epoch: Epoch,
    /// Active members that may participate in protocol rounds.
    members: HashSet<NodeId>,
    /// Status of all known nodes (including joining/removed).
    statuses: HashMap<NodeId, MemberStatus>,
    /// Protocol history records sent to joining nodes.
    join_history: HashMap<NodeId, ProtocolHistory>,
}

impl Electorate {
    /// Create a new electorate with the given initial members.
    ///
    /// # Panics
    /// Panics if `initial_members` is empty.
    pub fn new(epoch: Epoch, initial_members: Vec<NodeId>) -> Self {
        assert!(
            !initial_members.is_empty(),
            "electorate must have at least one member"
        );
        let members: HashSet<NodeId> = initial_members.iter().copied().collect();
        let statuses: HashMap<NodeId, MemberStatus> = initial_members
            .iter()
            .map(|&n| (n, MemberStatus::Active))
            .collect();
        Self {
            epoch,
            members,
            statuses,
            join_history: HashMap::new(),
        }
    }

    /// Current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Number of active members.
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Whether a node is an active member.
    pub fn is_active(&self, node: &NodeId) -> bool {
        self.members.contains(node)
    }

    /// Get the status of a node.
    pub fn status(&self, node: &NodeId) -> Option<&MemberStatus> {
        self.statuses.get(node)
    }

    /// Calculate the quorum size for the current electorate.
    ///
    /// Uses simple majority: `(n / 2) + 1`.
    pub fn quorum_size(&self) -> usize {
        (self.members.len() / 2) + 1
    }

    // -----------------------------------------------------------------------
    // JoinElectorate Protocol (A7.3)
    // -----------------------------------------------------------------------

    /// Begin the join process for a new node (Gate 1: Discovery).
    pub fn begin_join(&mut self, node: NodeId) -> Result<(), ElectorateError> {
        if self.members.contains(&node) {
            return Err(ElectorateError::AlreadyMember(node));
        }
        self.statuses
            .insert(node, MemberStatus::Joining(JoinGate::Discovery));
        Ok(())
    }

    /// Advance a joining node to HistorySync (Gate 2).
    ///
    /// Provides the protocol history the node needs to catch up.
    pub fn send_history(
        &mut self,
        node: NodeId,
        history: ProtocolHistory,
    ) -> Result<(), ElectorateError> {
        match self.statuses.get(&node) {
            Some(MemberStatus::Joining(JoinGate::Discovery)) => {}
            _ => {
                return Err(ElectorateError::PrematureParticipation {
                    node,
                    gate: JoinGate::HistorySync,
                });
            }
        }
        self.join_history.insert(node, history);
        self.statuses
            .insert(node, MemberStatus::Joining(JoinGate::HistorySync));
        Ok(())
    }

    /// Get the history record sent to a joining node.
    pub fn get_join_history(&self, node: &NodeId) -> Option<&ProtocolHistory> {
        self.join_history.get(node)
    }

    /// Advance a joining node to CatchUp (Gate 3).
    pub fn mark_caught_up(&mut self, node: NodeId) -> Result<(), ElectorateError> {
        match self.statuses.get(&node) {
            Some(MemberStatus::Joining(JoinGate::HistorySync)) => {}
            _ => {
                return Err(ElectorateError::PrematureParticipation {
                    node,
                    gate: JoinGate::CatchUp,
                });
            }
        }
        self.statuses
            .insert(node, MemberStatus::Joining(JoinGate::CatchUp));
        Ok(())
    }

    /// Activate a joining node (Gate 4: Active).
    ///
    /// The node is added to the electorate and may participate in rounds.
    /// Epoch is bumped.
    pub fn activate(&mut self, node: NodeId) -> Result<Epoch, ElectorateError> {
        match self.statuses.get(&node) {
            Some(MemberStatus::Joining(JoinGate::CatchUp)) => {}
            _ => {
                return Err(ElectorateError::PrematureParticipation {
                    node,
                    gate: JoinGate::Active,
                });
            }
        }
        self.epoch += 1;
        self.members.insert(node);
        self.statuses.insert(node, MemberStatus::Active);
        self.join_history.remove(&node);
        Ok(self.epoch)
    }

    /// Check whether a joining node may participate in protocol rounds.
    ///
    /// Returns `Err(PrematureParticipation)` if the node is still joining.
    pub fn check_participation(&self, node: &NodeId) -> Result<(), ElectorateError> {
        match self.statuses.get(node) {
            Some(MemberStatus::Active) => Ok(()),
            Some(MemberStatus::Joining(gate)) => Err(ElectorateError::PrematureParticipation {
                node: *node,
                gate: *gate,
            }),
            Some(MemberStatus::Removed) | None => Err(ElectorateError::NotMember(*node)),
        }
    }

    // -----------------------------------------------------------------------
    // Electorate Shrink (A7.4)
    // -----------------------------------------------------------------------

    /// Remove a node from the electorate.
    ///
    /// Bumps the epoch. Returns the new quorum size.
    pub fn remove_member(&mut self, node: NodeId) -> Result<usize, ElectorateError> {
        if !self.members.remove(&node) {
            return Err(ElectorateError::NotMember(node));
        }
        self.statuses.insert(node, MemberStatus::Removed);
        self.epoch += 1;
        Ok(self.quorum_size())
    }

    /// Validate a vote from a node.
    ///
    /// Returns `Err(InvalidVote)` if the node is not an active member.
    pub fn validate_vote(&self, node: &NodeId) -> Result<(), ElectorateError> {
        if self.members.contains(node) {
            Ok(())
        } else {
            Err(ElectorateError::InvalidVote {
                node: *node,
                electorate_epoch: self.epoch,
            })
        }
    }

    /// Validate a response epoch against the current epoch.
    ///
    /// Returns `Err(StaleEpoch)` if the message epoch is less than current.
    pub fn validate_epoch(&self, node: &NodeId, msg_epoch: Epoch) -> Result<(), ElectorateError> {
        if msg_epoch < self.epoch {
            Err(ElectorateError::StaleEpoch {
                node: *node,
                msg_epoch,
                current_epoch: self.epoch,
            })
        } else {
            Ok(())
        }
    }

    /// Return the set of active member node IDs.
    pub fn active_members(&self) -> &HashSet<NodeId> {
        &self.members
    }
}

// ===========================================================================
// Tests — 6 tests for A7.3 (3) + A7.4 (3)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // A7.3 Test 1: join_electorate_four_gates
    //   New member must wait for all 4 gates before participating.
    // -----------------------------------------------------------------------

    #[test]
    fn join_electorate_four_gates() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3]);
        assert_eq!(electorate.size(), 3);
        assert_eq!(electorate.quorum_size(), 2);

        let new_node: NodeId = 4;

        // Gate 1: Discovery.
        electorate.begin_join(new_node).expect("begin_join");
        assert_eq!(
            electorate.status(&new_node),
            Some(&MemberStatus::Joining(JoinGate::Discovery))
        );
        assert!(!electorate.is_active(&new_node), "not yet active");

        // Gate 2: HistorySync.
        let history = ProtocolHistory {
            epoch: 1,
            committed_count: 42,
            token_ranges: vec![(0, 1000)],
        };
        electorate
            .send_history(new_node, history)
            .expect("send_history");
        assert_eq!(
            electorate.status(&new_node),
            Some(&MemberStatus::Joining(JoinGate::HistorySync))
        );

        // Gate 3: CatchUp.
        electorate.mark_caught_up(new_node).expect("mark_caught_up");
        assert_eq!(
            electorate.status(&new_node),
            Some(&MemberStatus::Joining(JoinGate::CatchUp))
        );

        // Gate 4: Active.
        let new_epoch = electorate.activate(new_node).expect("activate");
        assert_eq!(new_epoch, 2, "epoch must bump on activation");
        assert!(electorate.is_active(&new_node), "node must be active");
        assert_eq!(electorate.size(), 4);
        assert_eq!(electorate.quorum_size(), 3, "quorum for 4 nodes = 3");

        // Participation check passes for active node.
        electorate
            .check_participation(&new_node)
            .expect("participation ok");
    }

    // -----------------------------------------------------------------------
    // A7.3 Test 2: join_electorate_receives_history
    //   Joining node receives protocol history during HistorySync gate.
    // -----------------------------------------------------------------------

    #[test]
    fn join_electorate_receives_history() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3]);
        let new_node: NodeId = 4;

        electorate.begin_join(new_node).expect("begin_join");

        let history = ProtocolHistory {
            epoch: 1,
            committed_count: 100,
            token_ranges: vec![(0, 500), (500, 1000)],
        };
        electorate
            .send_history(new_node, history.clone())
            .expect("send_history");

        // Verify the history was recorded.
        let received = electorate
            .get_join_history(&new_node)
            .expect("history must exist");
        assert_eq!(received.epoch, 1);
        assert_eq!(received.committed_count, 100);
        assert_eq!(received.token_ranges.len(), 2);
        assert_eq!(received.token_ranges[0], (0, 500));
        assert_eq!(received.token_ranges[1], (500, 1000));

        // After activation, history is cleaned up.
        electorate.mark_caught_up(new_node).unwrap();
        electorate.activate(new_node).unwrap();
        assert!(
            electorate.get_join_history(&new_node).is_none(),
            "history must be cleaned up after activation"
        );
    }

    // -----------------------------------------------------------------------
    // A7.3 Test 3: join_electorate_premature_rejected
    //   Attempting to participate before completing gates is rejected.
    // -----------------------------------------------------------------------

    #[test]
    fn join_electorate_premature_rejected() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3]);
        let new_node: NodeId = 4;

        // Before any join — not a member at all.
        let err = electorate.check_participation(&new_node).unwrap_err();
        assert!(
            matches!(err, ElectorateError::NotMember(4)),
            "non-member must be rejected"
        );

        // Gate 1: Discovery — participation still rejected.
        electorate.begin_join(new_node).unwrap();
        let err = electorate.check_participation(&new_node).unwrap_err();
        assert!(
            matches!(
                err,
                ElectorateError::PrematureParticipation {
                    node: 4,
                    gate: JoinGate::Discovery
                }
            ),
            "at Discovery gate, participation must be rejected"
        );

        // Skip Gate 2 and try to go directly to CatchUp — error.
        let err = electorate.mark_caught_up(new_node).unwrap_err();
        assert!(
            matches!(
                err,
                ElectorateError::PrematureParticipation {
                    node: 4,
                    gate: JoinGate::CatchUp
                }
            ),
            "skipping HistorySync must fail"
        );

        // Skip to activation — error.
        let err = electorate.activate(new_node).unwrap_err();
        assert!(
            matches!(
                err,
                ElectorateError::PrematureParticipation {
                    node: 4,
                    gate: JoinGate::Active
                }
            ),
            "skipping to Active must fail"
        );

        // Cannot duplicate begin_join for an existing member.
        electorate
            .send_history(
                new_node,
                ProtocolHistory {
                    epoch: 1,
                    committed_count: 0,
                    token_ranges: vec![],
                },
            )
            .unwrap();
        electorate.mark_caught_up(new_node).unwrap();
        electorate.activate(new_node).unwrap();

        let err = electorate.begin_join(new_node).unwrap_err();
        assert!(
            matches!(err, ElectorateError::AlreadyMember(4)),
            "already-active member cannot re-join"
        );
    }

    // -----------------------------------------------------------------------
    // A7.4 Test 1: electorate_shrink_quorum_resize
    //   After shrink, quorum size adjusts dynamically.
    // -----------------------------------------------------------------------

    #[test]
    fn electorate_shrink_quorum_resize() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3, 4, 5]);
        assert_eq!(electorate.size(), 5);
        assert_eq!(electorate.quorum_size(), 3, "quorum for 5 = 3");

        // Remove node 5.
        let new_quorum = electorate.remove_member(5).expect("remove_member");
        assert_eq!(electorate.size(), 4);
        assert_eq!(new_quorum, 3, "quorum for 4 = 3");
        assert_eq!(electorate.epoch(), 2, "epoch bumped after removal");

        // Remove node 4.
        let new_quorum = electorate.remove_member(4).expect("remove_member");
        assert_eq!(electorate.size(), 3);
        assert_eq!(new_quorum, 2, "quorum for 3 = 2");
        assert_eq!(electorate.epoch(), 3);

        // Remove node 3.
        let new_quorum = electorate.remove_member(3).expect("remove_member");
        assert_eq!(electorate.size(), 2);
        assert_eq!(new_quorum, 2, "quorum for 2 = 2");

        // Verify removed nodes are not active.
        assert!(!electorate.is_active(&5));
        assert!(!electorate.is_active(&4));
        assert!(!electorate.is_active(&3));

        // Verify removed node status.
        assert_eq!(electorate.status(&5), Some(&MemberStatus::Removed));
    }

    // -----------------------------------------------------------------------
    // A7.4 Test 2: electorate_vote_validation
    //   Votes are validated against the current electorate membership.
    // -----------------------------------------------------------------------

    #[test]
    fn electorate_vote_validation() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3]);

        // Active member votes are valid.
        electorate.validate_vote(&1).expect("node 1 vote valid");
        electorate.validate_vote(&2).expect("node 2 vote valid");
        electorate.validate_vote(&3).expect("node 3 vote valid");

        // Non-member vote is invalid.
        let err = electorate.validate_vote(&4).unwrap_err();
        assert!(
            matches!(err, ElectorateError::InvalidVote { node: 4, .. }),
            "non-member vote must be rejected"
        );

        // Remove node 3 — its votes are now invalid.
        electorate.remove_member(3).unwrap();
        let err = electorate.validate_vote(&3).unwrap_err();
        assert!(
            matches!(err, ElectorateError::InvalidVote { node: 3, .. }),
            "removed node's vote must be rejected"
        );

        // Remaining members still valid.
        electorate.validate_vote(&1).expect("node 1 still valid");
        electorate.validate_vote(&2).expect("node 2 still valid");
    }

    // -----------------------------------------------------------------------
    // A7.4 Test 3: electorate_stale_epoch_response
    //   Responses with stale epochs are rejected.
    // -----------------------------------------------------------------------

    #[test]
    fn electorate_stale_epoch_response() {
        let mut electorate = Electorate::new(1, vec![1, 2, 3]);

        // Current epoch message is accepted.
        electorate.validate_epoch(&2, 1).expect("current epoch ok");

        // Future epoch message is accepted (sender knows about a newer config).
        electorate.validate_epoch(&2, 5).expect("future epoch ok");

        // Advance epoch via member removal.
        electorate.remove_member(3).unwrap();
        assert_eq!(electorate.epoch(), 2);

        // Now epoch 1 is stale.
        let err = electorate.validate_epoch(&2, 1).unwrap_err();
        match err {
            ElectorateError::StaleEpoch {
                node,
                msg_epoch,
                current_epoch,
            } => {
                assert_eq!(node, 2);
                assert_eq!(msg_epoch, 1, "message epoch must be the stale value");
                assert_eq!(current_epoch, 2, "current epoch must be reported");
            }
            other => panic!("expected StaleEpoch, got {:?}", other),
        }

        // Epoch 2 (current) is accepted.
        electorate.validate_epoch(&2, 2).expect("current epoch ok");

        // Epoch 3 (future) is accepted.
        electorate.validate_epoch(&2, 3).expect("future epoch ok");
    }
}
