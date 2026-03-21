//! Replication strategies for replica selection.
//!
//! Phase 2: SimpleStrategy (single DC).
//! Phase 3: NetworkTopologyStrategy (multi-DC).

use std::collections::HashMap;

/// Replication strategy determines how replicas are selected.
#[derive(Debug, Clone)]
pub enum ReplicationStrategy {
    Simple { replication_factor: usize },
    NetworkTopology { dc_rf: HashMap<String, usize> },
}

impl ReplicationStrategy {
    /// Total replication factor across all data centers.
    ///
    /// For `Simple`, returns the single RF. For `NetworkTopology`, returns
    /// the sum of all per-DC replication factors.
    pub fn replication_factor(&self) -> usize {
        match self {
            Self::Simple { replication_factor } => *replication_factor,
            Self::NetworkTopology { dc_rf } => dc_rf.values().sum(),
        }
    }

    /// Per-DC replication factors. Returns an empty map for `Simple`.
    pub fn dc_replication_factors(&self) -> &HashMap<String, usize> {
        // Use a static empty map for the Simple case to avoid allocation.
        static EMPTY: std::sync::LazyLock<HashMap<String, usize>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            Self::Simple { .. } => &EMPTY,
            Self::NetworkTopology { dc_rf } => dc_rf,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_strategy_rf() {
        let s = ReplicationStrategy::Simple {
            replication_factor: 3,
        };
        assert_eq!(s.replication_factor(), 3);
    }

    #[test]
    fn network_topology_total_rf() {
        let dc_rf: HashMap<String, usize> =
            HashMap::from([("dc1".to_string(), 3), ("dc2".to_string(), 2)]);
        let s = ReplicationStrategy::NetworkTopology {
            dc_rf: dc_rf.clone(),
        };
        assert_eq!(s.replication_factor(), 5);
        assert_eq!(s.dc_replication_factors(), &dc_rf);
    }

    #[test]
    fn network_topology_single_dc() {
        let dc_rf = HashMap::from([("us-east-1".to_string(), 3)]);
        let s = ReplicationStrategy::NetworkTopology { dc_rf };
        assert_eq!(s.replication_factor(), 3);
    }

    #[test]
    fn network_topology_empty_dc_rf() {
        let s = ReplicationStrategy::NetworkTopology {
            dc_rf: HashMap::new(),
        };
        assert_eq!(s.replication_factor(), 0);
    }

    #[test]
    fn simple_dc_replication_factors_is_empty() {
        let s = ReplicationStrategy::Simple {
            replication_factor: 3,
        };
        assert!(s.dc_replication_factors().is_empty());
    }
}
