use serde::{Deserialize, Serialize};

/// Deployment mode tracking the cluster formation lifecycle.
///
/// Progressive formation: Standalone → Pair → Forming → Cluster.
/// Degraded states preserve context for automatic recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    /// Intermediate state: 2nd peer seen, mesh forming, awaiting Raft init.
    Forming,
    Cluster,
    /// Pair peer lost. Preserves peer context for recovery.
    DegradedPair,
    /// Cluster quorum lost. Raft running but cannot commit.
    DegradedCluster,
}

impl DeploymentMode {
    /// Infer deployment mode from the number of peers (excluding self).
    pub fn from_peer_count(count: usize) -> Self {
        match count {
            0 => Self::Standalone,
            1 => Self::Pair,
            _ => Self::Cluster,
        }
    }

    /// The mode a node in `self` becomes when it can see `peers` other nodes.
    ///
    /// Unlike [`Self::from_peer_count`], this knows where the node has BEEN,
    /// which is the only way to honour the rule that a multi-node Raft cluster
    /// never becomes a pair again. A count alone cannot express it: one peer
    /// means "pair" to a node that has only ever been standalone, and "degraded
    /// cluster" to a node that has held a quorum.
    ///
    /// The three valid shapes are a single node, a pair (leader plus read
    /// replica), and a multi-node Raft cluster. Degradation moves within a
    /// shape; it never moves back to a smaller one. A pair that loses its peer
    /// is a degraded pair, a cluster that loses a node is a degraded cluster --
    /// because the shapes commit differently, and silently swapping a quorum
    /// for a point-to-point primary accepts writes a quorum would have refused.
    ///
    /// `Forming` may still fall back to `Pair`: it is the pre-Raft state, so no
    /// multi-node cluster was ever established and there is nothing to preserve.
    #[must_use]
    pub fn next_mode(self, peers: usize) -> Self {
        match self {
            // Once a cluster, always a cluster. Peer count decides only whether
            // it is healthy or degraded, never what shape it is.
            Self::Cluster | Self::DegradedCluster => {
                if peers >= 2 {
                    Self::Cluster
                } else {
                    Self::DegradedCluster
                }
            }
            Self::Standalone => match peers {
                0 => Self::Standalone,
                1 => Self::Pair,
                _ => Self::Cluster,
            },
            Self::Pair | Self::DegradedPair => match peers {
                0 => Self::DegradedPair,
                1 => Self::Pair,
                _ => Self::Cluster,
            },
            Self::Forming => match peers {
                0 => Self::DegradedPair,
                1 => Self::Pair,
                _ => Self::Cluster,
            },
        }
    }

    /// The mode a node must start in, given whether it was already a member of
    /// a Raft cluster.
    ///
    /// Deployment mode lives only in memory: every controller starts at
    /// `Standalone` and walks up from peer connections. So a node that WAS a
    /// committed cluster member forgets that on restart and re-derives its
    /// shape from whoever it reconnects to first -- which, for a node whose
    /// only seed is one peer, is `Pair`.
    ///
    /// Observed on node1, 2026-08-20. The cluster still had it in the
    /// membership: the leader was sending it AppendEntries every 3.5 seconds
    /// and timing out. Locally it believed it was half of a pair. The cluster's
    /// view and the node's view of the same node disagreed, and the node's view
    /// was the one deciding how it replicated.
    ///
    /// Raft membership on disk is the evidence and it outranks a peer count. A
    /// returning member starts `DegradedCluster` -- a cluster member with no
    /// quorum yet -- from which the only exit is `Cluster`. That is what makes
    /// the no-going-back rule survive a process restart rather than only a mode
    /// change within one.
    #[must_use]
    pub fn initial_for_restart(was_cluster_member: bool) -> Self {
        if was_cluster_member {
            Self::DegradedCluster
        } else {
            Self::Standalone
        }
    }

    /// Check if transitioning from `self` to `target` is allowed.
    ///
    /// Valid transitions:
    /// ```text
    /// Standalone → Pair
    /// Pair → Forming | DegradedPair
    /// Forming → Cluster | Pair (fallback on timeout)
    /// Cluster → DegradedCluster
    /// DegradedPair → Pair (recovery) | Standalone (operator demote)
    /// DegradedCluster → Cluster (quorum restored)
    /// ```
    /// Legacy: Standalone → Cluster and Pair → Cluster are kept for
    /// backward compatibility but new code should go through Forming.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            // Forward progression
            (Self::Standalone, Self::Pair)
                | (Self::Pair, Self::Forming)
                | (Self::Forming, Self::Cluster)
                // Forming fallback (3rd node disappeared)
                | (Self::Forming, Self::Pair)
                // Forming fallback with NO peers left. Found by the
                // next_mode/can_transition_to cross-check property test, which
                // is the reachable case the hand-written list missed: a node
                // that saw a second peer, began forming, then lost both.
                // DegradedPair rather than Standalone because peer context is
                // still worth keeping for recovery -- the same reason
                // Pair -> DegradedPair exists.
                | (Self::Forming, Self::DegradedPair)
                // Degraded transitions
                | (Self::Pair, Self::DegradedPair)
                | (Self::Cluster, Self::DegradedCluster)
                // Recovery
                | (Self::DegradedPair, Self::Pair)
                | (Self::DegradedCluster, Self::Cluster)
                // Operator demote from degraded pair
                | (Self::DegradedPair, Self::Standalone)
                // A degraded pair that regains enough peers forms a cluster
                // directly. Second gap found by the cross-check property test.
                // Consistent with the Pair -> Cluster allowance below: a
                // degraded pair IS a pair that lost its peer, so if peers
                // return in cluster numbers there is no reason to route it
                // through Pair first -- and doing so would briefly assert
                // "pair" about a node that can see two peers.
                | (Self::DegradedPair, Self::Cluster)
                // Legacy direct transitions (backward compat)
                | (Self::Standalone, Self::Cluster)
                | (Self::Pair, Self::Cluster)
        )
    }
}

impl std::fmt::Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone => write!(f, "standalone"),
            Self::Pair => write!(f, "pair"),
            Self::Forming => write!(f, "forming"),
            Self::Cluster => write!(f, "cluster"),
            Self::DegradedPair => write!(f, "degraded-pair"),
            Self::DegradedCluster => write!(f, "degraded-cluster"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_peer_count() {
        assert_eq!(
            DeploymentMode::from_peer_count(0),
            DeploymentMode::Standalone
        );
        assert_eq!(DeploymentMode::from_peer_count(1), DeploymentMode::Pair);
        assert_eq!(DeploymentMode::from_peer_count(2), DeploymentMode::Cluster);
        assert_eq!(DeploymentMode::from_peer_count(10), DeploymentMode::Cluster);
    }

    /// A cluster that has been a multi-node Raft cluster can never become a
    /// pair again. Losing a node makes it a DEGRADED cluster.
    ///
    /// Stated by Ben, 2026-08-20: there are three valid shapes -- a single
    /// node, a pair with a leader and a read replica, and a full multi-node
    /// Raft cluster. Once the third is reached the first two are behind you. A
    /// departure is a degradation of what you are, not a reversion to what you
    /// were.
    ///
    /// It matters because the shapes have different correctness. A pair
    /// replicates DDL point-to-point with one side authoritative; a Raft
    /// cluster commits through a quorum. Falling back means writes a quorum
    /// would have refused get accepted by whichever node still believes it is
    /// primary -- and on 2026-08-20 node1 did exactly that, sitting in pair
    /// mode with an empty schema while answering CQL.
    /// A node that was already a cluster member must come back as one.
    ///
    /// "A cluster never becomes a pair" is worthless if a restart resets the
    /// node to Standalone and lets it walk back up to Pair -- which is what
    /// node1 did while the leader was still replicating to it.
    #[test]
    fn a_returning_cluster_member_never_restarts_as_a_pair() {
        let mode = DeploymentMode::initial_for_restart(true);
        assert_eq!(
            mode,
            DeploymentMode::DegradedCluster,
            "a committed member with no quorum yet is a degraded cluster"
        );
        for forbidden in [
            DeploymentMode::Pair,
            DeploymentMode::Standalone,
            DeploymentMode::DegradedPair,
        ] {
            assert!(
                !mode.can_transition_to(forbidden),
                "a returning member must not be able to reach {forbidden}"
            );
        }
        assert!(
            mode.can_transition_to(DeploymentMode::Cluster),
            "rejoining the quorum is the only way out"
        );
    }

    /// A genuinely new node still starts standalone. The fix must not turn
    /// every first boot into a phantom cluster member.
    #[test]
    fn a_node_with_no_history_still_starts_standalone() {
        assert_eq!(
            DeploymentMode::initial_for_restart(false),
            DeploymentMode::Standalone
        );
    }

    /// The two mechanisms must agree: a returning member cannot be walked back
    /// to a pair by peer count either.
    #[test]
    fn a_returning_member_with_one_peer_stays_a_cluster() {
        let mode = DeploymentMode::initial_for_restart(true);
        assert_eq!(mode.next_mode(1), DeploymentMode::DegradedCluster);
        assert_eq!(mode.next_mode(0), DeploymentMode::DegradedCluster);
        assert_eq!(mode.next_mode(2), DeploymentMode::Cluster);
    }

    #[test]
    fn a_cluster_never_degrades_into_a_pair() {
        for target in [
            DeploymentMode::Pair,
            DeploymentMode::Standalone,
            DeploymentMode::DegradedPair,
            DeploymentMode::Forming,
        ] {
            assert!(
                !DeploymentMode::Cluster.can_transition_to(target),
                "a Raft cluster must never become {target}; losing a node means DegradedCluster"
            );
        }
        assert!(
            DeploymentMode::Cluster.can_transition_to(DeploymentMode::DegradedCluster),
            "a departure degrades the cluster, and that is the only way down"
        );
    }

    /// A degraded cluster is still a cluster. Further loss does not demote it
    /// to a pair or a standalone node.
    #[test]
    fn a_degraded_cluster_never_becomes_a_pair_or_standalone() {
        for target in [
            DeploymentMode::Pair,
            DeploymentMode::Standalone,
            DeploymentMode::DegradedPair,
            DeploymentMode::Forming,
        ] {
            assert!(
                !DeploymentMode::DegradedCluster.can_transition_to(target),
                "a degraded cluster must never become {target}"
            );
        }
        assert!(
            DeploymentMode::DegradedCluster.can_transition_to(DeploymentMode::Cluster),
            "restoring quorum is the only exit"
        );
    }

    /// `from_peer_count` is a back door around the guard above.
    ///
    /// It maps a count to a mode with NO reference to the current mode: one
    /// peer means Pair, always. So a three-node cluster that loses a node is
    /// described as a Pair by this function -- precisely the transition
    /// can_transition_to forbids. Any caller storing its result has bypassed
    /// the state machine.
    #[test]
    fn peer_count_alone_cannot_decide_the_mode() {
        let from_count = DeploymentMode::from_peer_count(1);
        assert_eq!(from_count, DeploymentMode::Pair);
        assert!(
            !DeploymentMode::Cluster.can_transition_to(from_count),
            "from_peer_count(1) produces a mode a Cluster may not enter, so it \
must never be stored directly; use next_mode, which knows the current state"
        );
    }

    /// The history-aware successor: what a node in `self` becomes when it can
    /// see `peers` other nodes.
    #[test]
    fn next_mode_degrades_a_cluster_instead_of_pairing_it() {
        assert_eq!(
            DeploymentMode::Cluster.next_mode(1),
            DeploymentMode::DegradedCluster,
            "one surviving peer is a degraded cluster, not a pair"
        );
        assert_eq!(
            DeploymentMode::Cluster.next_mode(0),
            DeploymentMode::DegradedCluster,
            "an isolated cluster member is still a cluster member"
        );
        assert_eq!(
            DeploymentMode::DegradedCluster.next_mode(2),
            DeploymentMode::Cluster
        );
        assert_eq!(
            DeploymentMode::DegradedCluster.next_mode(1),
            DeploymentMode::DegradedCluster
        );
    }

    /// The pre-cluster shapes still behave as before.
    #[test]
    fn next_mode_leaves_the_pre_cluster_shapes_alone() {
        assert_eq!(
            DeploymentMode::Standalone.next_mode(1),
            DeploymentMode::Pair
        );
        assert_eq!(
            DeploymentMode::Pair.next_mode(0),
            DeploymentMode::DegradedPair,
            "a pair that loses its peer keeps its peer context"
        );
        assert_eq!(
            DeploymentMode::DegradedPair.next_mode(1),
            DeploymentMode::Pair
        );
        // Forming is pre-Raft, so falling back to Pair is legitimate: no
        // multi-node cluster was ever established.
        assert_eq!(DeploymentMode::Forming.next_mode(1), DeploymentMode::Pair);
        assert_eq!(
            DeploymentMode::Forming.next_mode(2),
            DeploymentMode::Cluster
        );
    }

    /// Every mode next_mode can return must be a legal transition from the
    /// mode it was called on. Without this the two functions can drift -- which
    /// is how the guard came to be dead code while a bypass shipped.
    #[test]
    fn next_mode_never_proposes_an_illegal_transition() {
        let all = [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ];
        for current in all {
            for peers in 0..5usize {
                let next = current.next_mode(peers);
                assert!(
                    next == current || current.can_transition_to(next),
                    "{current} with {peers} peers proposed {next}, not a legal transition"
                );
            }
        }
    }

    #[test]
    fn transitions_are_one_way() {
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Pair));
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Cluster));
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::Cluster));
        assert!(!DeploymentMode::Pair.can_transition_to(DeploymentMode::Standalone));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Pair));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Standalone));
    }

    // --- Formation state machine tests (S1.1) ---

    #[test]
    fn forming_state_exists() {
        // Forming is an intermediate state between Pair and Cluster
        let mode = DeploymentMode::Forming;
        assert_eq!(format!("{mode}"), "forming");
    }

    #[test]
    fn degraded_pair_state_exists() {
        let mode = DeploymentMode::DegradedPair;
        assert_eq!(format!("{mode}"), "degraded-pair");
    }

    #[test]
    fn degraded_cluster_state_exists() {
        let mode = DeploymentMode::DegradedCluster;
        assert_eq!(format!("{mode}"), "degraded-cluster");
    }

    #[test]
    fn pair_to_forming_transition() {
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::Forming));
    }

    #[test]
    fn forming_to_cluster_transition() {
        assert!(DeploymentMode::Forming.can_transition_to(DeploymentMode::Cluster));
    }

    #[test]
    fn forming_falls_back_to_pair() {
        // If 3rd node disappears, Forming should fall back to Pair
        assert!(DeploymentMode::Forming.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn pair_to_degraded_pair() {
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::DegradedPair));
    }

    #[test]
    fn degraded_pair_recovers_to_pair() {
        assert!(DeploymentMode::DegradedPair.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn degraded_pair_demotes_to_standalone() {
        assert!(DeploymentMode::DegradedPair.can_transition_to(DeploymentMode::Standalone));
    }

    #[test]
    fn cluster_to_degraded_cluster() {
        assert!(DeploymentMode::Cluster.can_transition_to(DeploymentMode::DegradedCluster));
    }

    #[test]
    fn degraded_cluster_recovers_to_cluster() {
        assert!(DeploymentMode::DegradedCluster.can_transition_to(DeploymentMode::Cluster));
    }

    #[test]
    fn forming_cannot_go_to_standalone() {
        assert!(!DeploymentMode::Forming.can_transition_to(DeploymentMode::Standalone));
    }

    #[test]
    fn cluster_cannot_go_to_pair() {
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn degraded_cluster_cannot_go_to_pair() {
        assert!(!DeploymentMode::DegradedCluster.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn all_modes_display_correctly() {
        assert_eq!(format!("{}", DeploymentMode::Standalone), "standalone");
        assert_eq!(format!("{}", DeploymentMode::Pair), "pair");
        assert_eq!(format!("{}", DeploymentMode::Forming), "forming");
        assert_eq!(format!("{}", DeploymentMode::Cluster), "cluster");
        assert_eq!(format!("{}", DeploymentMode::DegradedPair), "degraded-pair");
        assert_eq!(
            format!("{}", DeploymentMode::DegradedCluster),
            "degraded-cluster"
        );
    }

    #[test]
    fn all_modes_serialize_deserialize() {
        for mode in [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: DeploymentMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }
}
