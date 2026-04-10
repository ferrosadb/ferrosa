//! Write path abstraction for runtime mode transitions.
//!
//! The CQL router calls `WritePath::write()` for all DML mutations. The
//! active implementation is swapped atomically via `ArcSwap` when the
//! deployment mode changes (standalone → pair → cluster).
//!
//! - `WritePath::Direct` — standalone mode, writes directly to `StorageEngine`.
//! - `WritePath::Pair` — pair mode, delegates to `PairCoordinator::coordinate_write()`.

use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{Mutation, TableId};

use crate::consistency::ConsistencyLevel;
use crate::coordinator::ClusterCoordinator;
use crate::pair::coordinator::PairCoordinator;
use crate::ring::strategy::ReplicationStrategy;

/// The active write path. Swapped atomically via `ArcSwap` when the
/// deployment mode changes (standalone → pair → cluster).
///
/// Uses enum dispatch instead of trait objects so that `ArcSwap` works
/// (trait objects are `!Sized` and `ArcSwap` requires `Sized`).
pub enum WritePath {
    /// Standalone mode: writes directly to StorageEngine.
    Direct(Arc<StorageEngine>),
    /// Pair mode: delegates to PairCoordinator.
    Pair(Arc<PairCoordinator>),
    /// Cluster mode: delegates to ClusterCoordinator with CL enforcement.
    Cluster(Arc<ClusterCoordinator>),
    /// Degraded: peer lost, writes rejected until operator promotes.
    Unavailable,
}

impl WritePath {
    /// Create a standalone write path.
    pub fn direct(engine: Arc<StorageEngine>) -> Self {
        Self::Direct(engine)
    }

    /// Create a pair mode write path.
    pub fn pair(coordinator: Arc<PairCoordinator>) -> Self {
        Self::Pair(coordinator)
    }

    /// Create a cluster mode write path.
    pub fn cluster(coordinator: Arc<ClusterCoordinator>) -> Self {
        Self::Cluster(coordinator)
    }

    /// Create an unavailable write path (degraded pair mode).
    pub fn unavailable() -> Self {
        Self::Unavailable
    }

    /// Write a logged batch atomically. In standalone mode this goes
    /// through `StorageEngine::write_atomic_batch()`. In pair mode each
    /// mutation is forwarded individually (atomic guarantee comes from
    /// the batchlog). In cluster mode the `ClusterCoordinator` handles
    /// the 3-phase batchlog protocol.
    pub async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        _cl: ConsistencyLevel,
        _rf: usize,
    ) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.write_atomic_batch(mutations),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                // Pair mode: forward each mutation individually.
                for m in mutations {
                    coordinator
                        .coordinate_write(&m)
                        .await
                        .map_err(|e| ferrosa_common::Error::InvalidData(format!("pair: {e}")))?;
                }
                Ok(())
            }
            Self::Cluster(coordinator) => coordinator
                .coordinate_logged_batch(mutations)
                .await
                .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
        }
    }

    /// Read a single partition by key with CL enforcement.
    ///
    /// - `Direct` / `Pair`: reads from local storage (single-node case).
    /// - `Cluster`: routes through ClusterCoordinator with digest protocol.
    /// - `Unavailable`: returns an error.
    pub async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        match self {
            Self::Direct(engine) => engine.read(table_id, key),
            Self::Pair(coordinator) => coordinator.local_storage().read(table_id, key),
            Self::Cluster(coordinator) => {
                let rows_opt = match strategy {
                    ReplicationStrategy::Simple { replication_factor } => {
                        coordinator
                            .coordinate_read_with(table_id, key, cl, *replication_factor)
                            .await
                    }
                    ReplicationStrategy::NetworkTopology { .. } => {
                        coordinator
                            .coordinate_read_nts(table_id, key, cl, strategy)
                            .await
                    }
                };
                match rows_opt {
                    Ok(Some(rows)) if !rows.is_empty() => Ok(Some(Partition {
                        key: key.clone(),
                        deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                        static_row: None,
                        rows,
                    })),
                    Ok(_) => Ok(None),
                    Err(e) => Err(ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
                }
            }
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, reads rejected until operator promotes".into(),
            )),
        }
    }

    /// Read a single partition by key, routing to the correct replica.
    ///
    /// - `Direct` / `Pair`: reads from local storage (single-node).
    /// - `Cluster`: routes through ClusterCoordinator to the correct replica.
    /// - `Unavailable`: returns None.
    pub async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        match self {
            Self::Direct(engine) => engine.read(table_id, key),
            Self::Pair(coordinator) => coordinator.local_storage().read(table_id, key),
            Self::Cluster(coordinator) => match coordinator.coordinate_read(table_id, key).await {
                Ok(Some(rows)) => Ok(Some(Partition {
                    key: key.clone(),
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    static_row: None,
                    rows,
                })),
                Ok(None) => Ok(None),
                Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                    "coordinate_read: {e}"
                ))),
            },
            Self::Unavailable => Ok(None),
        }
    }

    /// Scatter a full-table range read to all nodes that hold data for
    /// `table_id` and return the deduplicated union of all partitions.
    ///
    /// - `Direct` / `Pair`: reads from local storage only (single-node case).
    /// - `Cluster`: fans out to every ring node and merges results.
    /// - `Unavailable`: returns error (degraded mode).
    ///
    /// Errors are propagated — callers MUST handle them. Silently returning
    /// empty results on failure causes data loss (see BUG: large-write-causes-
    /// data-loss-in-partition).
    pub async fn range_read(&self, table_id: &TableId) -> crate::error::Result<Vec<Partition>> {
        match self {
            Self::Direct(engine) => engine
                .read_range(table_id, None, None, 1_000_000)
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) => coordinator
                .local_storage()
                .read_range(table_id, None, None, 1_000_000)
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => coordinator.coordinate_range_read(table_id).await,
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "range read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Truncate a table. In standalone/pair mode this truncates local storage.
    /// In cluster mode the coordinator fans out to all nodes.
    pub async fn truncate(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.truncate(table_id),
            Self::Pair(coordinator) => coordinator.local_storage().truncate(table_id),
            Self::Cluster(coordinator) => coordinator
                .coordinate_truncate(table_id)
                .await
                .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster truncate: {e}"))),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, truncate rejected until operator promotes".into(),
            )),
        }
    }

    /// Write a row. In standalone mode this goes directly to storage.
    /// In pair mode this goes through the PairCoordinator which handles
    /// replication (primary) or forwarding (secondary).
    /// In cluster mode the replication strategy determines whether to use
    /// SimpleStrategy or NetworkTopologyStrategy DC-aware coordination.
    pub async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.write(table_id, key, row, timestamp),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                let mutation = Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key.clone(),
                    vec![row],
                    timestamp,
                );
                coordinator
                    .coordinate_write(&mutation)
                    .await
                    .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster: {e}")))
            }
            Self::Cluster(coordinator) => match strategy {
                ReplicationStrategy::Simple { replication_factor } => coordinator
                    .coordinate_write_with(table_id, key, row, timestamp, cl, *replication_factor)
                    .await
                    .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
                ReplicationStrategy::NetworkTopology { .. } => coordinator
                    .coordinate_write_nts(table_id, key, row, timestamp, cl, strategy)
                    .await
                    .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::strategy::ReplicationStrategy;
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
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
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    #[tokio::test]
    async fn direct_write_path_delegates_to_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        // Register a table
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();

        let wp = WritePath::direct(storage.clone());
        let table_id = TableId::new("ks", "tbl");
        let key = DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        let strategy = ReplicationStrategy::Simple {
            replication_factor: 1,
        };
        WritePath::write(
            &wp,
            &table_id,
            &key,
            row,
            1000,
            ConsistencyLevel::One,
            &strategy,
        )
        .await
        .unwrap();

        // Verify data was written
        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "DirectWritePath should write to storage");
    }
}
