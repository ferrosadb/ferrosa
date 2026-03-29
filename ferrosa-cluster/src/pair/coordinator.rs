use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{Mutation, TableId};

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Coordinates writes in pair mode.
///
/// Primary: writes locally, then replicates to secondary.
/// Secondary: forwards to primary (which writes + replicates back).
pub struct PairCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    storage: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
}

impl PairCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        storage: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            storage,
            peer_manager,
        }
    }

    /// Return the local storage engine (used by `WritePath::range_read` for
    /// pair mode full-table scans — both pair nodes hold a full copy).
    pub(crate) fn local_storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Route a write based on current role.
    ///
    /// On the primary: writes locally first (always succeeds), then
    /// best-effort replicates to the secondary. If replication fails
    /// (peer down, lanes reconnecting), the write still succeeds —
    /// the secondary will catch up when it reconnects.
    pub async fn coordinate_write(&self, mutation: &Mutation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                self.apply_locally(mutation)?;
                if let Err(e) = self.replicate_to_peer(mutation).await {
                    tracing::warn!("pair replication failed (write succeeded locally): {e}");
                }
                Ok(())
            }
            PairRole::Secondary => self.forward_to_primary(mutation).await,
        }
    }

    /// Apply a mutation to local storage.
    pub(crate) fn apply_locally(&self, mutation: &Mutation) -> Result<()> {
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        for row in &mutation.rows {
            self.storage
                .write(&table_id, &mutation.key, row.clone(), mutation.timestamp)
                .map_err(ClusterError::Storage)?;
        }
        Ok(())
    }

    /// Send a mutation to the peer and wait for ACK.
    pub(crate) async fn replicate_to_peer(&self, mutation: &Mutation) -> Result<()> {
        let body = encode_mutation(mutation);
        let resp = self
            .peer_manager
            .send_with_timeout(
                self.peer_host_id,
                Message::PairWriteForward(body),
                Lane::Data,
                Duration::from_secs(5),
            )
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairWriteAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairWriteAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward a write to the primary and wait for ACK.
    async fn forward_to_primary(&self, mutation: &Mutation) -> Result<()> {
        let body = encode_mutation(mutation);
        let resp = self
            .peer_manager
            .send_with_timeout(
                self.peer_host_id,
                Message::PairWriteForward(body),
                Lane::Data,
                Duration::from_secs(5),
            )
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairWriteAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairWriteAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }

    /// Get peer host_id.
    pub fn peer_host_id(&self) -> Uuid {
        self.peer_host_id
    }
}

/// Encode a Mutation into Bytes for the wire.
pub fn encode_mutation(mutation: &Mutation) -> Bytes {
    let size = mutation.serialized_size();
    let mut buf = vec![0u8; size];
    mutation.serialize_into(&mut buf);
    Bytes::from(buf)
}

/// Decode a Mutation from wire bytes.
pub fn decode_mutation(body: &[u8]) -> Result<Mutation> {
    Mutation::deserialize_from(body)
        .map_err(|e| ClusterError::Internal(format!("mutation decode: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_mutation() -> Mutation {
        let key = DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![10, 20],
            cells: vec![(0, CellValue::live(vec![100], 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        Mutation {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key,
            rows: vec![row],
            timestamp: 1000,
        }
    }

    #[test]
    fn encode_decode_mutation_roundtrip() {
        let mutation = test_mutation();
        let encoded = encode_mutation(&mutation);
        let decoded = decode_mutation(&encoded).unwrap();

        assert_eq!(decoded.keyspace, mutation.keyspace);
        assert_eq!(decoded.table, mutation.table);
        assert_eq!(decoded.timestamp, mutation.timestamp);
        assert_eq!(decoded.rows.len(), mutation.rows.len());
    }
}
