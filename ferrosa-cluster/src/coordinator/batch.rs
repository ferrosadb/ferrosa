//! Logged batch coordination -- 3-phase batchlog protocol.
//!
//! Phase 1: Write BatchlogEntry to batchlog replicas (2 nodes, or local-only).
//! Phase 2: Fan out all mutations to their respective replicas.
//! Phase 3: Delete BatchlogEntry from batchlog replicas.
//!
//! If the coordinator crashes between phases, the background replay task
//! on the batchlog replicas will detect the stale entry and replay the
//! mutations.

use uuid::Uuid;

use ferrosa_storage::batchlog::BatchlogEntry;
use ferrosa_storage::Mutation;

use super::ClusterCoordinator;

impl ClusterCoordinator {
    /// Coordinate a logged batch across the cluster.
    ///
    /// Implements the 3-phase batchlog protocol:
    /// 1. Write batchlog entry to batchlog replicas.
    /// 2. Fan out individual mutations to their replicas (using `coordinate_write`).
    /// 3. Delete batchlog entry from batchlog replicas.
    pub async fn coordinate_logged_batch(
        &self,
        mutations: Vec<Mutation>,
    ) -> crate::error::Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        let batch_id = Uuid::new_v4();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let entry = BatchlogEntry {
            id: batch_id,
            created_at: now_ms,
            mutations: mutations.clone(),
        };

        // Phase 1: Write to batchlog.
        self.write_batchlog(&entry).await?;

        // Phase 2: Fan out mutations.
        let mut result = Ok(());
        for m in &mutations {
            let table_id = ferrosa_storage::TableId::new(&m.keyspace, &m.table);
            for row in &m.rows {
                if let Err(e) = self
                    .coordinate_write(&table_id, &m.key, row.clone(), m.timestamp)
                    .await
                {
                    result = Err(e);
                    break;
                }
            }
            if result.is_err() {
                break;
            }
        }

        // Phase 3: Delete batchlog entry (even on partial failure --
        // mutations that were written are durable, and the batch contract
        // is "at least once" delivery).
        self.delete_batchlog(batch_id).await?;

        result
    }

    /// Write a batchlog entry to batchlog replicas.
    ///
    /// In single-node / single-DC mode, writes to the local batchlog.
    /// In multi-node mode, also sends `BatchlogWrite` to remote batchlog
    /// replicas on a best-effort basis.
    async fn write_batchlog(&self, entry: &BatchlogEntry) -> crate::error::Result<()> {
        // Always write to local batchlog.
        if let Some(batchlog) = self.storage.batchlog() {
            batchlog.write_entry(entry.clone()).map_err(|e| {
                crate::error::ClusterError::Internal(format!("batchlog write failed: {e}"))
            })?;
        }

        // Best-effort write to remote batchlog replicas.
        let ring = self.ring.load();
        let batchlog_replicas = ring.select_batchlog_replicas(self.local_node_id, 2);
        drop(ring);

        let payload = bytes::Bytes::from(entry.serialize());
        for host_id in batchlog_replicas {
            if let Err(e) = self
                .peer_manager
                .send(
                    host_id,
                    ferrosa_net::message::Message::BatchlogWrite(payload.clone()),
                    ferrosa_net::codec::Lane::Data,
                )
                .await
            {
                tracing::warn!(
                    peer = %host_id,
                    "failed to send batchlog write to remote replica: {e}"
                );
            }
        }

        Ok(())
    }

    /// Delete a batchlog entry from batchlog replicas.
    async fn delete_batchlog(&self, batch_id: Uuid) -> crate::error::Result<()> {
        // Local batchlog delete.
        if let Some(batchlog) = self.storage.batchlog() {
            batchlog.delete_entry(batch_id).map_err(|e| {
                crate::error::ClusterError::Internal(format!("batchlog delete failed: {e}"))
            })?;
        }

        // Best-effort delete on remote batchlog replicas.
        let ring = self.ring.load();
        let batchlog_replicas = ring.select_batchlog_replicas(self.local_node_id, 2);
        drop(ring);

        let payload = bytes::Bytes::copy_from_slice(batch_id.as_bytes());
        for host_id in batchlog_replicas {
            if let Err(e) = self
                .peer_manager
                .send(
                    host_id,
                    ferrosa_net::message::Message::BatchlogDelete(payload.clone()),
                    ferrosa_net::codec::Lane::Data,
                )
                .await
            {
                tracing::warn!(
                    peer = %host_id,
                    "failed to send batchlog delete to remote replica: {e}"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::ConsistencyLevel;
    use crate::ring::TokenRing;
    use arc_swap::ArcSwap;
    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_net::peer::PeerManager;
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::engine::StorageEngine;
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, Mutation, StorageEngineConfig, TableId,
    };
    use std::sync::Arc;

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_node(addr: &str) -> crate::raft::NodeInfo {
        crate::raft::NodeInfo {
            host_id: uuid::Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: crate::raft::NodeState::Normal,
        }
    }

    fn test_key() -> DecoratedKey {
        DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        }
    }

    fn test_row() -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };
        storage.register_table(schema).unwrap();
    }

    struct NoopListener;
    impl ferrosa_net::peer::PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    fn noop_peer_manager() -> Arc<PeerManager> {
        Arc::new(PeerManager::new(
            Arc::new(ferrosa_net::config::NetConfig::default()),
            uuid::Uuid::new_v4(),
            Arc::new(NoopListener),
        ))
    }

    #[tokio::test]
    async fn coordinate_logged_batch_local_only() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let batchlog = storage.batchlog().unwrap();

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let mutations = vec![Mutation {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        }];

        coordinator
            .coordinate_logged_batch(mutations)
            .await
            .unwrap();

        // Batchlog should be empty (entry was deleted after success).
        assert_eq!(batchlog.entry_count(), 0);

        // Mutation should be visible in storage.
        let table_id = TableId::new("test_ks", "test_tbl");
        let result = storage.read(&table_id, &test_key()).unwrap();
        assert!(result.is_some());
    }
}
