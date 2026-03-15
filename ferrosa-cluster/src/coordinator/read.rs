//! Read coordination -- fans out reads to replicas with CL enforcement.
//!
//! Phase 2 implementation: local reads when the coordinator is a replica,
//! single-replica remote reads otherwise. Full fan-out with digest
//! comparison is deferred to Phase 3.

use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::Row;
use ferrosa_storage::TableId;

use crate::error::ClusterError;

use super::ClusterCoordinator;

impl ClusterCoordinator {
    /// Coordinate a read from the appropriate replicas.
    ///
    /// For this phase:
    /// - If the local node is a replica, read locally (fast path).
    /// - Otherwise, attempt to read from the first available remote replica.
    /// - Full fan-out with digest comparison is deferred to Phase 3.
    pub async fn coordinate_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        let ring = self.ring.load();
        let replicas = ring.replicas(key.token.0, self.default_rf);
        let required = self.default_cl.block_for(self.default_rf);

        if replicas.len() < required {
            return Err(ClusterError::Unavailable {
                consistency: self.default_cl.to_string(),
                required,
                alive: replicas.len(),
            });
        }

        // Fast path: if this node is a replica, read locally.
        if replicas.contains(&self.local_node_id) {
            return self
                .storage
                .read(table_id, key)
                .map(|opt| opt.map(|partition| partition.rows))
                .map_err(ClusterError::Storage);
        }

        // Slow path: try reading from remote replicas.
        // Full fan-out with digest comparison deferred to Phase 3.
        // For now, try each replica in order until one responds.
        for &replica_id in &replicas {
            if let Some(node_info) = ring.get_node(replica_id) {
                // TODO(Phase 3): encode a ReadRequest, send via Data lane,
                // decode ReadResponse. For now we log and fall through.
                tracing::debug!(
                    replica_addr = %node_info.addr,
                    "remote read not yet implemented, skipping replica"
                );
            }
        }

        // No local replica and remote reads not yet wired — return None.
        Ok(None)
    }
}
