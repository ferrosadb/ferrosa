//! Write coordination -- fans out mutations to replicas with CL enforcement.

use ferrosa_common::key::DecoratedKey;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::Row;
use ferrosa_storage::{Mutation, TableId};

use crate::error::ClusterError;
use crate::pair::coordinator::encode_mutation;

use super::ClusterCoordinator;

impl ClusterCoordinator {
    /// Coordinate a write to the appropriate replicas.
    ///
    /// 1. Compute replicas from token ring.
    /// 2. Verify enough replicas available for the consistency level.
    /// 3. Fan out: local write if self is replica, `MutationForward` for remote.
    /// 4. Collect ACKs until `block_for(CL)` reached.
    /// 5. Return success or `WriteTimeout`.
    pub async fn coordinate_write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> crate::error::Result<()> {
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

        let mutation = Mutation {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            key: key.clone(),
            rows: vec![row.clone()],
            timestamp,
        };
        let body = encode_mutation(&mutation);

        let mut acks = 0usize;
        for &replica_id in &replicas {
            if replica_id == self.local_node_id {
                // Write locally
                self.storage
                    .write(table_id, key, row.clone(), timestamp)
                    .map_err(ClusterError::Storage)?;
                acks += 1;
            } else {
                // Forward to remote replica via Data lane
                if let Some(node_info) = ring.get_node(replica_id) {
                    match self
                        .peer_manager
                        .send(
                            node_info.host_id,
                            Message::MutationForward(body.clone()),
                            Lane::Data,
                        )
                        .await
                    {
                        Ok(Message::MutationAck(_)) => acks += 1,
                        Ok(_) => {}  // unexpected response, treat as failure
                        Err(_) => {} // replica failed, continue
                    }
                }
                // Node not in ring metadata — skip
            }
            if acks >= required {
                break;
            }
        }

        if acks >= required {
            Ok(())
        } else {
            Err(ClusterError::WriteTimeout {
                consistency: self.default_cl.to_string(),
                received: acks,
                required,
            })
        }
    }
}
