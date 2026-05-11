//! Sprint 6 W6.5 — Consistency-level routing across multi-DC topologies.
//!
//! The coordinator consults [`route_for_cl`] before fanning out a
//! mutation to decide whether the operation can be served from the
//! local DC's Raft group, must fan out across DCs, or has to fail
//! with [`crate::error::ClusterError::NotImplemented`] until Sprint 7
//! wires Accord cross-DC consensus.
//!
//! This module is purposefully a small *pure* helper: full coordinator
//! integration (write/read fan-out, hint storage, etc.) ripples
//! through hundreds of call sites and conflicts with Sprint 4's
//! bootstrap decomposition. The routing decision lives here so the
//! integration can land additively in Sprint 7.

use crate::consistency::ConsistencyLevel;
use crate::error::ClusterError;
use crate::raft::NodeState;
use crate::ring::TokenRing;

// ---------------------------------------------------------------------------
// W8.4 — CL → eligible-roles table for learner-aware read routing.
// ---------------------------------------------------------------------------

/// Set of roles eligible to serve a given consistency level.
///
/// W8.4 / ADR-014: voter-quorum CLs (`QUORUM`, `LOCAL_QUORUM`,
/// `LOCAL_SERIAL`, `SERIAL`) must exclude learners; `ALL` and the
/// single-replica CLs (`ONE`, `LOCAL_ONE`) include learners that
/// own tokens.
///
/// `SERIAL` / `LOCAL_SERIAL` further mark the read as leader-only
/// (LWT round-trip).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CLReplicaPolicy {
    /// Whether `NodeState::Normal` voters are eligible.
    pub include_voters: bool,
    /// Whether `NodeState::Learner { owns_tokens: true }` replicas
    /// are eligible. (Learners with `owns_tokens=false` are always
    /// excluded.)
    pub include_learners: bool,
    /// Whether the operation must round-trip through the leader
    /// (`SERIAL` / `LOCAL_SERIAL` LWT). Coordinators that observe
    /// this flag must skip learners regardless of `include_learners`
    /// (a learner is never the leader by definition).
    pub leader_only: bool,
}

impl CLReplicaPolicy {
    /// Voters only — used for `QUORUM`, `LOCAL_QUORUM`, `Two`, `Three`.
    pub const VOTERS_ONLY: Self = Self {
        include_voters: true,
        include_learners: false,
        leader_only: false,
    };

    /// Voters and token-owning learners — used for `ONE` / `LOCAL_ONE`.
    pub const ANY_REPLICA: Self = Self {
        include_voters: true,
        include_learners: true,
        leader_only: false,
    };

    /// Voters and learners — used for `ALL` (every replica that owns
    /// the token, learner or voter).
    pub const ALL_REPLICAS: Self = Self {
        include_voters: true,
        include_learners: true,
        leader_only: false,
    };

    /// Leader-only round-trip — used for `SERIAL` / `LOCAL_SERIAL`.
    pub const LEADER_ONLY: Self = Self {
        include_voters: true,
        include_learners: false,
        leader_only: true,
    };
}

/// Map a consistency level to the role-eligibility policy from
/// ADR-014's CL routing table.
pub fn replica_policy_for_cl(cl: ConsistencyLevel) -> CLReplicaPolicy {
    match cl {
        // Single-replica reads — any replica that owns the token.
        ConsistencyLevel::One | ConsistencyLevel::LocalOne => CLReplicaPolicy::ANY_REPLICA,
        // Voter-quorum reads — never count learners.
        ConsistencyLevel::Two
        | ConsistencyLevel::Three
        | ConsistencyLevel::Quorum
        | ConsistencyLevel::LocalQuorum
        | ConsistencyLevel::EachQuorum => CLReplicaPolicy::VOTERS_ONLY,
        // ALL — every voter and every learner that owns the token.
        ConsistencyLevel::All => CLReplicaPolicy::ALL_REPLICAS,
        // Strict-consistency CLs — leader round-trip; learners excluded
        // (they are never the leader).
        ConsistencyLevel::Serial | ConsistencyLevel::LocalSerial => CLReplicaPolicy::LEADER_ONLY,
    }
}

/// Filter `replicas` (as produced by `TokenRing::replicas()`) to only
/// those eligible under `cl`.
///
/// Note: `TokenRing::replicas()` already excludes
/// `NodeState::Learner { owns_tokens: false }`. This filter applies
/// the additional CL-specific role policy:
///
/// - For voter-quorum CLs: drop any remaining `Learner { .. }` nodes.
/// - For `LEADER_ONLY` CLs: drop learners (the actual leader test
///   happens at the coordinator's read path, which sends to the
///   leader directly).
/// - For `ANY_REPLICA` and `ALL_REPLICAS`: pass through.
pub fn eligible_replicas_for_cl(
    cl: ConsistencyLevel,
    replicas: &[u64],
    ring: &TokenRing,
) -> Vec<u64> {
    let policy = replica_policy_for_cl(cl);
    replicas
        .iter()
        .copied()
        .filter(|&n| {
            let info = match ring.get_node(n) {
                Some(i) => i,
                None => return false,
            };
            match info.state {
                NodeState::Normal => policy.include_voters,
                NodeState::Learner { owns_tokens: true } => {
                    policy.include_learners && !policy.leader_only
                }
                // owns_tokens=false learners are already filtered upstream;
                // leaving the explicit case for safety.
                NodeState::Learner { owns_tokens: false } => false,
                _ => false,
            }
        })
        .collect()
}

/// What the coordinator should do for a `(topology, CL)` pair.
#[derive(Debug)]
pub enum CLRoute {
    /// Stay within the local-DC Raft group / replica set. Used for
    /// `ConsistencyLevel::One`, `Two`, `Three`, `LocalOne`,
    /// `LocalQuorum`, `LocalSerial`, and (in single-DC topologies)
    /// `Quorum` / `EachQuorum` / `All` / `Serial`.
    LocalDcOnly,
    /// All DCs participate; the coordinator fans out to every DC's
    /// replica set. Reachable in single-DC mode for `All`,
    /// `EachQuorum`, etc., where there's only one DC and it's local.
    AllDcs,
    /// Multi-DC: route the operation through Accord (CEP-15) for
    /// strict-serializable cross-DC consensus (Sprint 7 / ADR-015).
    /// The coordinator drives Accord pre-accept across both DCs'
    /// Raft groups; on apply each DC commits its share via
    /// [`crate::membership::MembershipChanger::accord_vote_commit`].
    CrossDcAccord,
    /// Multi-DC consensus required but not yet implemented. Reserved
    /// for cross-DC LWT (`Serial` / `LocalSerial` ↔ `EachQuorum`
    /// composition) which Sprint 7 does not deliver.
    NotImplementedCrossDc(ClusterError),
}

impl CLRoute {
    /// True iff this route is `LocalDcOnly`.
    pub fn is_local_dc_only(&self) -> bool {
        matches!(self, Self::LocalDcOnly)
    }

    /// True iff this route fans out to every DC.
    pub fn is_all_dcs(&self) -> bool {
        matches!(self, Self::AllDcs)
    }

    /// True iff this route is `CrossDcAccord` (Sprint 7).
    pub fn is_cross_dc_accord(&self) -> bool {
        matches!(self, Self::CrossDcAccord)
    }

    /// True iff this route is `NotImplementedCrossDc`.
    pub fn is_not_implemented(&self) -> bool {
        matches!(self, Self::NotImplementedCrossDc(_))
    }

    /// Convert into a `Result`: `Ok(self)` for the implemented routes,
    /// `Err(_)` for `NotImplementedCrossDc(_)`. Coordinator code uses
    /// `route_for_cl(...).into_result()?` to short-circuit cross-DC
    /// CLs into the standard error path.
    pub fn into_result(self) -> Result<Self, ClusterError> {
        match self {
            Self::NotImplementedCrossDc(e) => Err(e),
            other => Ok(other),
        }
    }
}

/// Decide how a consistency level routes given the cluster topology.
///
/// `dc_count` is the number of distinct DCs the cluster spans.
/// Single-DC clusters (`dc_count <= 1`) fall through to the legacy
/// single-Raft path for every CL — this is the backward-compat
/// guarantee called out in the sprint plan.
pub fn route_for_cl(cl: ConsistencyLevel, dc_count: usize) -> CLRoute {
    let multi_dc = dc_count >= 2;
    match cl {
        ConsistencyLevel::One
        | ConsistencyLevel::Two
        | ConsistencyLevel::Three
        | ConsistencyLevel::LocalOne
        | ConsistencyLevel::LocalQuorum
        | ConsistencyLevel::LocalSerial => CLRoute::LocalDcOnly,
        ConsistencyLevel::Quorum => {
            if multi_dc {
                // Sprint 7 W7.7: cross-DC QUORUM goes through Accord.
                CLRoute::CrossDcAccord
            } else {
                CLRoute::LocalDcOnly
            }
        }
        ConsistencyLevel::EachQuorum => {
            if multi_dc {
                // Sprint 7 W7.7: cross-DC EACH_QUORUM goes through Accord.
                CLRoute::CrossDcAccord
            } else {
                CLRoute::LocalDcOnly
            }
        }
        ConsistencyLevel::All => {
            if multi_dc {
                // Sprint 7 W7.7: cross-DC ALL goes through Accord.
                CLRoute::CrossDcAccord
            } else {
                CLRoute::AllDcs
            }
        }
        ConsistencyLevel::Serial => {
            if multi_dc {
                // Sprint 7 does not deliver cross-DC LWT (SERIAL +
                // CAS). Sprint 8 layers Accord LWT on top of the W7.7
                // adapter; until then, surface the original error.
                CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented {
                    feature: "SERIAL cross-DC LWT (Sprint 8 / Accord LWT)".to_string(),
                })
            } else {
                CLRoute::LocalDcOnly
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::NodeInfo;
    use uuid::Uuid;

    // ----- W8.4 helper builders -----

    fn voter_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn learner_node(addr: &str, owns_tokens: bool) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Learner { owns_tokens },
            cql_broadcast: None,
        }
    }

    fn build_ring_with(nodes: &[(u64, NodeInfo)]) -> TokenRing {
        let mut ring = TokenRing::new();
        for (id, info) in nodes {
            ring.add_node(*id, info.clone());
        }
        ring
    }

    /// W8.4 RED. `LOCAL_ONE` accepts any local replica — voter or
    /// token-owning learner. The eligibility filter must NOT drop
    /// learners.
    #[test]
    fn local_one_routes_to_any_local_replica() {
        let ring = build_ring_with(&[
            (1, voter_node("10.0.0.1:7000")),
            (2, learner_node("10.0.0.2:7000", true)),
        ]);
        let eligible = eligible_replicas_for_cl(ConsistencyLevel::LocalOne, &[1, 2], &ring);
        assert_eq!(eligible.len(), 2, "LOCAL_ONE should accept both replicas");
        assert!(eligible.contains(&1));
        assert!(eligible.contains(&2));

        // ONE behaves identically.
        let eligible_one = eligible_replicas_for_cl(ConsistencyLevel::One, &[1, 2], &ring);
        assert_eq!(eligible_one.len(), 2);
    }

    /// W8.4 RED. `LOCAL_QUORUM` excludes learners from the eligibility
    /// set so the quorum is drawn purely from voters.
    #[test]
    fn local_quorum_excludes_learners_from_quorum() {
        let ring = build_ring_with(&[
            (1, voter_node("10.0.0.1:7000")),
            (2, voter_node("10.0.0.2:7000")),
            (3, learner_node("10.0.0.3:7000", true)),
        ]);
        let eligible = eligible_replicas_for_cl(ConsistencyLevel::LocalQuorum, &[1, 2, 3], &ring);
        assert_eq!(eligible.len(), 2, "learner must not count toward quorum");
        assert!(!eligible.contains(&3));

        // Sanity: with the learner gone the quorum size is the same.
        assert!(eligible.contains(&1));
        assert!(eligible.contains(&2));
    }

    /// W8.4 RED. `QUORUM` excludes learners, identically to
    /// `LOCAL_QUORUM` (voter-quorum semantics).
    #[test]
    fn quorum_excludes_learners_from_quorum() {
        let ring = build_ring_with(&[
            (1, voter_node("10.0.0.1:7000")),
            (2, voter_node("10.0.0.2:7000")),
            (3, voter_node("10.0.0.3:7000")),
            (4, learner_node("10.0.0.4:7000", true)),
        ]);
        let eligible = eligible_replicas_for_cl(ConsistencyLevel::Quorum, &[1, 2, 3, 4], &ring);
        assert_eq!(eligible.len(), 3);
        assert!(!eligible.contains(&4));
    }

    /// W8.4 RED. `SERIAL` is leader-only — the eligibility filter
    /// excludes learners. (The coordinator's read path then routes to
    /// the leader; learners can never be leader, so this filter is the
    /// last line of defense against a learner accidentally serving an
    /// LWT read.)
    #[test]
    fn serial_forces_leader_round_trip_skips_learner() {
        let ring = build_ring_with(&[
            (1, voter_node("10.0.0.1:7000")),
            (2, learner_node("10.0.0.2:7000", true)),
        ]);
        let policy = replica_policy_for_cl(ConsistencyLevel::Serial);
        assert!(
            policy.leader_only,
            "SERIAL must mark the read as leader-only",
        );
        let eligible = eligible_replicas_for_cl(ConsistencyLevel::Serial, &[1, 2], &ring);
        assert_eq!(eligible, vec![1], "learner must be skipped under SERIAL");

        // LOCAL_SERIAL behaves identically.
        let policy = replica_policy_for_cl(ConsistencyLevel::LocalSerial);
        assert!(policy.leader_only);
        let eligible_local =
            eligible_replicas_for_cl(ConsistencyLevel::LocalSerial, &[1, 2], &ring);
        assert_eq!(eligible_local, vec![1]);
    }

    /// W8.4 sanity: `ALL` includes learners that own tokens.
    #[test]
    fn all_includes_token_owning_learner() {
        let ring = build_ring_with(&[
            (1, voter_node("10.0.0.1:7000")),
            (2, learner_node("10.0.0.2:7000", true)),
            (3, learner_node("10.0.0.3:7000", false)),
        ]);
        // Note: replicas() already excludes (3) at the ring layer; the
        // filter must not re-include it. Pass [1, 2] as `replicas` to
        // mirror what the ring would have returned.
        let eligible = eligible_replicas_for_cl(ConsistencyLevel::All, &[1, 2], &ring);
        assert_eq!(eligible.len(), 2, "ALL should keep voter and token learner");
    }

    /// W6.5 RED: `LOCAL_QUORUM` always routes within the local DC,
    /// regardless of topology. Single-DC and multi-DC clusters must
    /// behave identically for this CL.
    #[test]
    fn local_quorum_routes_within_dc() {
        assert!(
            route_for_cl(ConsistencyLevel::LocalQuorum, 1).is_local_dc_only(),
            "single-DC: LOCAL_QUORUM stays local"
        );
        assert!(
            route_for_cl(ConsistencyLevel::LocalQuorum, 2).is_local_dc_only(),
            "dual-DC: LOCAL_QUORUM stays in local DC; cross-DC voters are excluded"
        );
        assert!(route_for_cl(ConsistencyLevel::LocalQuorum, 3).is_local_dc_only());
    }

    /// W7.7 GREEN: `QUORUM` in a multi-DC topology routes through
    /// Accord. (Sprint 6 returned `NotImplemented`; Sprint 7 wires
    /// the cross-DC adapter.)
    #[test]
    fn quorum_routes_through_accord_in_multi_dc() {
        let route = route_for_cl(ConsistencyLevel::Quorum, 2);
        assert!(
            route.is_cross_dc_accord(),
            "multi-DC QUORUM must take the CrossDcAccord route, got {route:?}"
        );

        // Single-DC backward-compat: QUORUM keeps the local-Raft path.
        assert!(
            route_for_cl(ConsistencyLevel::Quorum, 1).is_local_dc_only(),
            "single-DC QUORUM keeps the existing single-Raft path"
        );
    }

    /// W7.7 GREEN: same for `EACH_QUORUM`.
    #[test]
    fn each_quorum_routes_through_accord_in_multi_dc() {
        let route = route_for_cl(ConsistencyLevel::EachQuorum, 2);
        assert!(route.is_cross_dc_accord());
        assert!(route_for_cl(ConsistencyLevel::EachQuorum, 1).is_local_dc_only());
    }

    /// W7.7 GREEN: `ALL` cross-DC also goes through Accord. `SERIAL`
    /// (cross-DC LWT) is deferred to Sprint 8.
    #[test]
    fn all_routes_through_accord_serial_still_deferred() {
        assert!(route_for_cl(ConsistencyLevel::All, 2).is_cross_dc_accord());
        assert!(route_for_cl(ConsistencyLevel::Serial, 2).is_not_implemented());

        // Single-DC: ALL fans out to every replica (one DC == "all
        // DCs"); SERIAL uses the local Raft.
        assert!(route_for_cl(ConsistencyLevel::All, 1).is_all_dcs());
        assert!(route_for_cl(ConsistencyLevel::Serial, 1).is_local_dc_only());
    }

    /// `into_result()` short-circuits the cross-DC error to the
    /// standard `ClusterError` path used by coordinators. Only
    /// `NotImplementedCrossDc` produces `Err`; `CrossDcAccord` is now
    /// a happy-path route and passes through `Ok`.
    #[test]
    fn into_result_short_circuits_only_not_implemented() {
        // SERIAL multi-DC remains NotImplemented in Sprint 7.
        let r = route_for_cl(ConsistencyLevel::Serial, 2).into_result();
        match r {
            Err(ClusterError::NotImplemented { feature }) => {
                assert!(feature.contains("SERIAL"));
            }
            other => panic!("expected Err(NotImplemented), got {other:?}"),
        }

        // Cross-DC Accord routes pass through Ok.
        let r = route_for_cl(ConsistencyLevel::Quorum, 2).into_result();
        assert!(r.is_ok(), "QUORUM cross-DC must now succeed at routing");

        // Local routes pass through.
        let r = route_for_cl(ConsistencyLevel::LocalQuorum, 2).into_result();
        assert!(r.is_ok());
    }
}
