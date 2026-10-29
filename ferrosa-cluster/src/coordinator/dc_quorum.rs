//! Quorum obligations over an immutable configured replica/DC scope.
//! Correctness: origin and DC majorities are independent; lost replies never shrink denominators.
//! Last revised: 2026-10-29
//! Last changed: Share hierarchical and local consistency accounting across coordinators.

use crate::consistency::ConsistencyLevel;
use crate::ring::strategy::ReplicationStrategy;

/// Counts describe the first unmet obligation: origin replicas, then DC votes.
#[derive(Clone, Copy, Debug)]
pub(super) struct Progress {
    pub required: usize,
    pub received: usize,
}

impl Progress {
    pub fn satisfied(self) -> bool {
        self.required > 0 && self.received >= self.required
    }
}

/// `count_in_dc` counts distinct eligible successful responders, not contacts.
/// The requirement for a datacenter-scoped **read**: the origin datacenter's quorum alone.
///
/// A read never pays a cross-DC hop, which means it is blind to what replicated elsewhere: a client
/// that acknowledged a write in one datacenter and then fails over to another can read backwards
/// past its own write until replication catches up. That is the accepted cost of `DC_QUORUM`.
pub fn local_progress(
    strategy: &ReplicationStrategy,
    origin: &str,
    count_in_dc: impl Fn(&str) -> usize,
) -> Progress {
    let configured = strategy.dc_replication_factors();
    let Some(&rf) = configured.get(origin).filter(|rf| **rf > 0) else {
        return Progress {
            required: 1,
            received: 0,
        };
    };
    Progress {
        required: rf / 2 + 1,
        received: count_in_dc(origin),
    }
}

/// The configured strategy must be held fixed for the operation's lifetime.
/// This evaluates counters without allocating or copying the configured scope.
pub(super) fn progress(
    cl: ConsistencyLevel,
    strategy: &ReplicationStrategy,
    origin: &str,
    total: usize,
    count_in_dc: impl Fn(&str) -> usize,
) -> Progress {
    let configured = strategy.dc_replication_factors();
    match cl {
        // DC_QUORUM: origin-datacenter quorum plus a quorum of configured datacenters. Cassandra's
        // QUORUM is the total-count arm below (`_ =>`, rf / 2 + 1 anywhere), which is deliberately
        // not this shape: the two are incomparable above two datacenters.
        ConsistencyLevel::DcQuorum if !configured.is_empty() => {
            let Some(&origin_rf) = configured.get(origin).filter(|rf| **rf > 0) else {
                return Progress {
                    required: 1,
                    received: 0,
                };
            };
            let origin = Progress {
                required: origin_rf / 2 + 1,
                received: count_in_dc(origin),
            };
            if !origin.satisfied() {
                return origin;
            }
            // Second condition: a quorum of configured datacenters, each reaching its own quorum.
            // Two disjoint partitions cannot both hold a majority of the configured DCs, so this is
            // what prevents a split brain. The origin gate above is the first condition, and it is
            // what lets a reader satisfy this level without a cross-DC hop.
            let configured_dcs = configured.values().filter(|rf| **rf > 0).count();
            let dc_quorum = Progress {
                required: configured_dcs / 2 + 1,
                received: configured
                    .iter()
                    .filter(|(dc, rf)| **rf > 0 && count_in_dc(dc) > **rf / 2)
                    .count(),
            };
            if !dc_quorum.satisfied() {
                return dc_quorum;
            }
            // Third condition: a quorum of ALL replicas. The datacenter shape alone still admits a
            // node-level split inside the participating datacenters.
            let rf_total: usize = configured.values().filter(|rf| **rf > 0).sum();
            Progress {
                required: rf_total / 2 + 1,
                received: total,
            }
        }
        ConsistencyLevel::LocalQuorum
        | ConsistencyLevel::LocalOne
        | ConsistencyLevel::LocalSerial => {
            let Some(&rf) = configured.get(origin).filter(|rf| **rf > 0) else {
                return Progress {
                    required: 1,
                    received: 0,
                };
            };
            Progress {
                required: cl.block_for_dc(rf),
                received: count_in_dc(origin),
            }
        }
        ConsistencyLevel::EachQuorum => {
            let required = configured
                .values()
                .filter(|rf| **rf > 0)
                .map(|rf| rf / 2 + 1)
                .sum();
            if required == 0 {
                return Progress {
                    required: 1,
                    received: 0,
                };
            }
            Progress {
                required,
                received: configured
                    .iter()
                    .filter(|(_, rf)| **rf > 0)
                    .map(|(dc, rf)| count_in_dc(dc).min(rf / 2 + 1))
                    .sum(),
            }
        }
        _ => {
            let rf = strategy.replication_factor();
            if rf == 0 {
                return Progress {
                    required: 1,
                    received: 0,
                };
            }
            let required = match cl {
                ConsistencyLevel::Two => 2,
                ConsistencyLevel::Three => 3,
                _ => cl.block_for(rf),
            };
            Progress {
                required,
                received: total,
            }
        }
    }
}
