//! Replication strategies for replica selection.
//!
//! Phase 2: SimpleStrategy (single DC).
//! Phase 3: NetworkTopologyStrategy (multi-DC).

/// Replication strategy determines how replicas are selected.
#[derive(Debug, Clone)]
pub enum ReplicationStrategy {
    Simple { replication_factor: usize },
    // NetworkTopology deferred to Phase 3
}

impl ReplicationStrategy {
    pub fn replication_factor(&self) -> usize {
        match self {
            Self::Simple { replication_factor } => *replication_factor,
        }
    }
}
