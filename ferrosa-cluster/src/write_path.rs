//! Write path abstraction for runtime mode transitions.
//!
//! The CQL router calls `WritePath::write()` for all DML mutations. The
//! active implementation is swapped atomically via `ArcSwap` when the
//! deployment mode changes (standalone → pair → cluster).
//!
//! - [`DirectWritePath`] — standalone mode, writes directly to `StorageEngine`.
//! - [`PairWritePath`] — pair mode, delegates to `PairCoordinator::coordinate_write()`.

use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::Row;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{Mutation, TableId};

use crate::pair::coordinator::PairCoordinator;

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

    /// Create an unavailable write path (degraded pair mode).
    pub fn unavailable() -> Self {
        Self::Unavailable
    }

    /// Write a row. In standalone mode this goes directly to storage.
    /// In pair mode this goes through the PairCoordinator which handles
    /// replication (primary) or forwarding (secondary).
    pub async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.write(table_id, key, row, timestamp),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                let mutation = Mutation {
                    keyspace: table_id.keyspace.clone(),
                    table: table_id.table.clone(),
                    key: key.clone(),
                    rows: vec![row],
                    timestamp,
                };
                coordinator
                    .coordinate_write(&mutation)
                    .await
                    .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster: {e}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
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

        WritePath::write(&wp, &table_id, &key, row, 1000)
            .await
            .unwrap();

        // Verify data was written
        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "DirectWritePath should write to storage");
    }
}
