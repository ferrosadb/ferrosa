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
        ConsistencyLevel::Quorum if !configured.is_empty() => {
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
            let configured_dcs = configured.values().filter(|rf| **rf > 0).count();
            Progress {
                required: configured_dcs / 2 + 1,
                received: configured
                    .iter()
                    .filter(|(dc, rf)| **rf > 0 && count_in_dc(dc) > **rf / 2)
                    .count(),
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
