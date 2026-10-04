//! Shared fixture for the partition-state transfer tests.
//!
//! Every path that moves a partition between nodes (row streaming, Merkle
//! repair apply, read repair) must carry the WHOLE partition: its clustered
//! rows, its static row and its partition-level deletion. This fixture builds
//! one partition that has all three, seeds a receiver with an older row the
//! deletion must shadow, and checks what the receiver reads back.

use std::sync::Arc;

use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

pub(crate) const KEYSPACE: &str = "pst_ks";
pub(crate) const TABLE: &str = "pst_tbl";

/// Partition-level deletion on the source: newer than the receiver's stale
/// row (100), older than the source's live data (600, 700).
pub(crate) const PARTITION_DELETED_AT: i64 = 500;

pub(crate) fn table_id() -> TableId {
    TableId::new(KEYSPACE, TABLE)
}

pub(crate) fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"pst-key".to_vec()))
}

fn utf8(name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
    }
}

/// `(pk text, ck int, s text STATIC, v text, PRIMARY KEY (pk, ck))`.
/// Cell ordinals: statics first, so `s` = 0 and `v` = 1.
pub(crate) fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![utf8("s")],
        regular_columns: vec![utf8("v")],
        extensions: Default::default(),
    }
}

pub(crate) fn storage(dir: &std::path::Path) -> Arc<StorageEngine> {
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
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 4,
        cache_hot_window_secs: 900,
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    storage.register_table(schema()).unwrap();
    storage
}

fn clustered(ck: i32, value: &str, ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(1, CellValue::live(value.as_bytes().to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

pub(crate) fn static_row() -> Row {
    Row {
        clustering: Vec::new(),
        cells: vec![(0, CellValue::live(b"static-value".to_vec(), 600))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::NONE,
    }
}

/// The source replica's partition: a static row, one live clustered row and
/// a partition deletion at [`PARTITION_DELETED_AT`].
pub(crate) fn source_partition() -> Partition {
    Partition {
        key: key(),
        deletion: DeletionTime::new(PARTITION_DELETED_AT, 1_700_000_000),
        static_row: Some(static_row()),
        rows: vec![clustered(1, "after-delete", 700)],
    }
}

/// Seed the receiver with a row written BEFORE the source's partition
/// deletion. Once the deletion arrives it must never be visible again.
pub(crate) fn seed_stale_row(storage: &StorageEngine) {
    storage
        .write(
            &table_id(),
            &key(),
            clustered(2, "deleted-on-source", 100),
            100,
        )
        .unwrap();
}

/// Rows a reader may see: those not shadowed by the partition deletion.
fn visible_rows(p: &Partition) -> Vec<Row> {
    let deleted_at = p.deletion.marked_for_delete_at;
    p.rows
        .iter()
        .filter(|r| {
            r.primary_key_liveness.timestamp > deleted_at
                || r.cells.iter().any(|(_, c)| c.timestamp > deleted_at)
        })
        .cloned()
        .collect()
}

/// The receiver must read back the source's partition: same deletion, same
/// static row, same visible rows, and the stale row must stay deleted.
pub(crate) fn assert_receiver_matches_source(storage: &StorageEngine, path: &str) {
    let source = source_partition();
    let got = storage
        .read(&table_id(), &key())
        .unwrap()
        .unwrap_or_else(|| panic!("{path}: partition missing on the receiver"));
    assert_eq!(
        got.deletion, source.deletion,
        "{path}: partition deletion did not reach the receiver, so the stale row \
         written at ts=100 resurrects"
    );
    assert_eq!(
        got.static_row, source.static_row,
        "{path}: static row did not reach the receiver"
    );
    assert_eq!(
        visible_rows(&got),
        source.rows,
        "{path}: visible clustered rows differ from the source"
    );
}
