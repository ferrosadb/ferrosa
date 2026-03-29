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
    /// On the primary: writes locally, then waits for the secondary to ACK
    /// before returning success to the caller. If replication fails (peer
    /// down, timeout), the error is propagated — the write is NOT confirmed
    /// until the secondary has durably received it (C2.1).
    pub async fn coordinate_write(&self, mutation: &Mutation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                self.apply_locally(mutation)?;
                self.replicate_to_peer(mutation).await?;
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
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::PeerId;
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

    /// No-op listener used in unit tests where peer lifecycle events
    /// are irrelevant.
    struct NoopListener;

    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: Uuid) {}
        fn on_peer_failed(&self, _peer_id: Uuid) {}
    }

    /// Build a PairCoordinator in Primary role whose peer has no connection
    /// pool (simulates a permanently unreachable secondary).
    ///
    /// The table referenced by `test_mutation()` (test_ks.test_tbl) is
    /// registered so that `apply_locally` succeeds. This isolates the
    /// replication failure as the only error path under test.
    async fn primary_coordinator_with_unreachable_peer(
        dir: &std::path::Path,
    ) -> (PairCoordinator, Uuid) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
            SyncStrategyConfig,
        };

        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
        };
        let config = StorageEngineConfig {
            commit_log,
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.to_path_buf(),
        };
        let storage = Arc::new(StorageEngine::new(config, None).expect("storage engine"));

        // Register test_ks.test_tbl so apply_locally succeeds; we want to
        // isolate the replication error path, not storage validation errors.
        storage
            .register_table(TableSchema {
                keyspace: "test_ks".to_string(),
                table: "test_tbl".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![ColumnDefinition {
                    name: "ck".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                }],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "val".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
            })
            .expect("register table");

        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let config = Arc::new(NetConfig::default());
        let peer_manager = Arc::new(PeerManager::new(
            config,
            local_id,
            Arc::new(NoopListener),
        ));

        // add_peer_entry inserts a pool-less entry — send_with_timeout on it
        // returns Err(NetError::Protocol("no connection pool")), which is the
        // same error class produced by a crashed or unreachable secondary.
        let peer_addr: std::net::SocketAddr = "127.0.0.1:7001".parse().unwrap();
        peer_manager.add_peer_entry((peer_id, peer_addr)).await;

        let role = Arc::new(ArcSwap::new(Arc::new(PairRole::Primary)));
        let coordinator = PairCoordinator::new(role, peer_id, storage, peer_manager);

        (coordinator, peer_id)
    }

    /// C2.1 — write must NOT be confirmed to the client when the secondary
    /// cannot be reached. Before the fix, coordinate_write returned Ok even
    /// when replication failed, risking a write-loss on subsequent primary
    /// crash.
    #[tokio::test]
    async fn pair_write_confirmed_after_secondary_ack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _peer_id) =
            primary_coordinator_with_unreachable_peer(dir.path()).await;

        let mutation = test_mutation();
        let result = coordinator.coordinate_write(&mutation).await;

        assert!(
            result.is_err(),
            "coordinate_write should return Err when secondary is unreachable, \
             got Ok — primary must not confirm before secondary ACKs (C2.1)"
        );
    }

    /// C2.1 — replication timeout must propagate as an error to the caller.
    /// The no-pool peer entry returns an immediate error (equivalent to a
    /// timeout or connection refusal from a crashed secondary).
    #[tokio::test]
    async fn pair_replication_timeout_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _peer_id) =
            primary_coordinator_with_unreachable_peer(dir.path()).await;

        let mutation = test_mutation();
        let result = coordinator.coordinate_write(&mutation).await;

        assert!(
            result.is_err(),
            "coordinate_write must return Err on replication failure (timeout/unreachable)"
        );

        // Verify it is specifically a Net error wrapped in ReplicationFailed or Net,
        // not some unrelated error type.
        let err = result.unwrap_err();
        let is_replication_or_net = matches!(
            err,
            ClusterError::ReplicationFailed(_) | ClusterError::Net(_)
        );
        assert!(
            is_replication_or_net,
            "error should be ReplicationFailed or Net, got: {err}"
        );
    }
}
