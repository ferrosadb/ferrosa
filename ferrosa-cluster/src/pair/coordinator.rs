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

    /// Forward a batch of mutations atomically.
    ///
    /// On the primary: writes all locally first via `write_atomic_batch`, then
    /// best-effort replicates the whole batch to the peer as a single message.
    /// On the secondary: forwards the whole batch to the primary as a single RPC.
    ///
    /// The `batch_id` is used by the primary to enable idempotent application
    /// if the secondary retries after an ACK is lost (future: track applied
    /// batch_ids in a small LRU).
    pub async fn coordinate_batch(
        &self,
        mutations: Vec<Mutation>,
        batch_id: Uuid,
    ) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                // Apply locally as an atomic batch.
                self.storage
                    .write_atomic_batch(mutations.clone())
                    .map_err(ClusterError::Storage)?;
                // Best-effort replicate batch to peer.
                if let Err(e) = self.replicate_batch_to_peer(&mutations, batch_id).await {
                    tracing::warn!(
                        "pair batch replication failed (write succeeded locally): {e}"
                    );
                }
                Ok(())
            }
            PairRole::Secondary => {
                self.forward_batch_to_primary(&mutations, batch_id).await
            }
        }
    }

    /// Replicate an atomic batch to the peer (primary → secondary).
    async fn replicate_batch_to_peer(
        &self,
        mutations: &[Mutation],
        batch_id: Uuid,
    ) -> Result<()> {
        let body = encode_batch(batch_id, mutations)?;
        let resp = self
            .peer_manager
            .send_with_timeout(
                self.peer_host_id,
                Message::PairBatchForward(body),
                Lane::Data,
                Duration::from_secs(5),
            )
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairBatchAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairBatchAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward a batch to the primary (secondary → primary).
    async fn forward_batch_to_primary(
        &self,
        mutations: &[Mutation],
        batch_id: Uuid,
    ) -> Result<()> {
        let body = encode_batch(batch_id, mutations)?;
        let resp = self
            .peer_manager
            .send_with_timeout(
                self.peer_host_id,
                Message::PairBatchForward(body),
                Lane::Data,
                Duration::from_secs(5),
            )
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairBatchAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairBatchAck, got {:?}",
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

/// Encode a batch of mutations with a `batch_id` prefix for atomic pair replication.
///
/// Wire layout: `batch_id:[u8;16] | mutation_count:u32 | (len:u32 | mutation)*`
pub fn encode_batch(batch_id: Uuid, mutations: &[Mutation]) -> Result<Bytes> {
    let count = u32::try_from(mutations.len())
        .map_err(|_| ClusterError::Internal("batch too large".into()))?;

    // Compute total size: 16 (batch_id) + 4 (count) + sum(4 + serialized_size)
    let mutations_bytes: usize = mutations.iter().map(|m| 4 + m.serialized_size()).sum();
    let total = 16 + 4 + mutations_bytes;

    let mut buf = vec![0u8; total];
    let mut pos = 0;

    // batch_id
    buf[pos..pos + 16].copy_from_slice(batch_id.as_bytes());
    pos += 16;

    // mutation_count
    buf[pos..pos + 4].copy_from_slice(&count.to_be_bytes());
    pos += 4;

    // mutations: each prefixed with 4-byte length
    for m in mutations {
        let size = m.serialized_size();
        let len = u32::try_from(size)
            .map_err(|_| ClusterError::Internal("mutation too large to encode".into()))?;
        buf[pos..pos + 4].copy_from_slice(&len.to_be_bytes());
        pos += 4;
        m.serialize_into(&mut buf[pos..pos + size]);
        pos += size;
    }

    Ok(Bytes::from(buf))
}

/// Decode a batch payload encoded by [`encode_batch`].
///
/// Returns `(batch_id, mutations)`.
pub fn decode_batch(body: &[u8]) -> Result<(Uuid, Vec<Mutation>)> {
    if body.len() < 20 {
        return Err(ClusterError::Internal("batch payload too short".into()));
    }

    // batch_id
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&body[0..16]);
    let batch_id = Uuid::from_bytes(id_bytes);

    // mutation_count
    let count = u32::from_be_bytes([body[16], body[17], body[18], body[19]]) as usize;
    let mut pos = 20;

    let mut mutations = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > body.len() {
            return Err(ClusterError::Internal("batch truncated at length prefix".into()));
        }
        let len = u32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
            as usize;
        pos += 4;
        if pos + len > body.len() {
            return Err(ClusterError::Internal("batch truncated at mutation body".into()));
        }
        let m = Mutation::deserialize_from(&body[pos..pos + len])
            .map_err(|e| ClusterError::Internal(format!("batch mutation decode: {e}")))?;
        mutations.push(m);
        pos += len;
    }

    Ok((batch_id, mutations))
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
            mutation_id: [0x82u8; 16],
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
