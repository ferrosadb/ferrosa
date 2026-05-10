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
    /// Multi-DC consensus required but not yet implemented (Sprint 7).
    /// Coordinator MUST fail the request with the contained error.
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
                CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented {
                    feature: "QUORUM cross-DC consensus (Sprint 7 / ADR-015 Accord)".to_string(),
                })
            } else {
                CLRoute::LocalDcOnly
            }
        }
        ConsistencyLevel::EachQuorum => {
            if multi_dc {
                CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented {
                    feature: "EACH_QUORUM cross-DC fan-out (Sprint 7 / ADR-015 Accord)".to_string(),
                })
            } else {
                CLRoute::LocalDcOnly
            }
        }
        ConsistencyLevel::All => {
            if multi_dc {
                CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented {
                    feature: "ALL cross-DC fan-out (Sprint 7 / ADR-015 Accord)".to_string(),
                })
            } else {
                CLRoute::AllDcs
            }
        }
        ConsistencyLevel::Serial => {
            if multi_dc {
                CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented {
                    feature: "SERIAL cross-DC LWT (Sprint 7 / ADR-015 Accord)".to_string(),
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

    /// W6.5 RED: `QUORUM` in a multi-DC topology returns
    /// `NotImplemented` until Sprint 7 wires Accord.
    #[test]
    fn quorum_returns_not_implemented_cross_dc() {
        let route = route_for_cl(ConsistencyLevel::Quorum, 2);
        match route {
            CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented { feature }) => {
                assert!(feature.contains("QUORUM"));
                assert!(feature.contains("Sprint 7"));
            }
            other => panic!("expected NotImplementedCrossDc, got {other:?}"),
        }

        // Single-DC backward-compat: QUORUM still works.
        assert!(
            route_for_cl(ConsistencyLevel::Quorum, 1).is_local_dc_only(),
            "single-DC QUORUM keeps the existing single-Raft path"
        );
    }

    /// W6.5 RED: same for `EACH_QUORUM`.
    #[test]
    fn each_quorum_returns_not_implemented() {
        let route = route_for_cl(ConsistencyLevel::EachQuorum, 2);
        match route {
            CLRoute::NotImplementedCrossDc(ClusterError::NotImplemented { feature }) => {
                assert!(feature.contains("EACH_QUORUM"));
            }
            other => panic!("expected NotImplementedCrossDc, got {other:?}"),
        }

        // Single-DC: EACH_QUORUM degenerates to local-DC quorum.
        assert!(route_for_cl(ConsistencyLevel::EachQuorum, 1).is_local_dc_only());
    }

    /// `ALL` and `SERIAL` cross-DC also need Accord. Sanity-check so
    /// the scaffolding doesn't silently route them into a half-built
    /// path.
    #[test]
    fn all_and_serial_cross_dc_are_not_implemented() {
        assert!(route_for_cl(ConsistencyLevel::All, 2).is_not_implemented());
        assert!(route_for_cl(ConsistencyLevel::Serial, 2).is_not_implemented());

        // Single-DC: ALL fans out to every replica (one DC == "all
        // DCs"); SERIAL uses the local Raft.
        assert!(route_for_cl(ConsistencyLevel::All, 1).is_all_dcs());
        assert!(route_for_cl(ConsistencyLevel::Serial, 1).is_local_dc_only());
    }

    /// `into_result()` short-circuits the cross-DC error to the
    /// standard `ClusterError` path used by coordinators.
    #[test]
    fn into_result_short_circuits_cross_dc() {
        let r = route_for_cl(ConsistencyLevel::Quorum, 2).into_result();
        match r {
            Err(ClusterError::NotImplemented { feature }) => {
                assert!(feature.contains("QUORUM"));
            }
            other => panic!("expected Err(NotImplemented), got {other:?}"),
        }

        // Local routes pass through.
        let r = route_for_cl(ConsistencyLevel::LocalQuorum, 2).into_result();
        assert!(r.is_ok());
    }
}
