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
