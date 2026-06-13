//! Integration tests for the shared atomic multi-write batch / transaction
//! primitive (spec URS-QEC-X02,
//! `specs/storage-multiwrite-batch-primitive.md`).
//!
//! These tests pin the four guarantees a batch must provide:
//!
//! * **Atomicity** — a batch of mixed writes + tombstones either ALL apply or,
//!   on an injected mid-batch failure, NONE apply (no partial state readable).
//! * **Durability** — an applied batch survives a simulated process restart
//!   (it is in the commit log and replays).
//! * **Visibility** — after `apply_batch` returns `Ok`, every op is immediately
//!   readable / tombstoned.
//! * **Fail-loud** — an unsupported / unregistered batch returns a clear `Err`,
//!   never a silent partial apply (spec X01).

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use std::path::Path;
use std::time::Duration;

use ferrosa_storage::{
    BatchOp, CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
    SyncStrategyConfig, TableId,
};

fn test_schema(keyspace: &str, table: &str) -> TableSchema {
    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
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
        extensions: Default::default(),
    }
}

fn test_engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

/// A clustering value encodes a single Int32 clustering column.
fn ck(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn live_row(clustering: Vec<u8>, value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering,
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

/// Helper: pull the live value for a (key, clustering) out of a table, or None
/// if the partition / row is absent or fully tombstoned.
fn read_cell(
    engine: &StorageEngine,
    tid: &TableId,
    key: &DecoratedKey,
    clustering: &[u8],
) -> Option<Vec<u8>> {
    let partition = engine.read(tid, key).ok()??;
    let row = partition.rows.iter().find(|r| r.clustering == clustering)?;
    if !row.deletion.is_live() {
        return None;
    }
    row.cells
        .iter()
        .find(|(idx, _)| *idx == 0)
        .and_then(|(_, c)| c.value.clone())
}

/// VISIBILITY + (single-batch) ATOMICITY: a batch of two writes and one
/// tombstone applies fully; after `apply_batch` returns `Ok`, every op is
/// immediately readable / tombstoned.
#[test]
fn apply_batch_writes_and_tombstone_all_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    // Seed a row we will tombstone in the batch.
    engine
        .apply_batch(vec![BatchOp::Write {
            keyspace: "ks".into(),
            table: "t".into(),
            key: key.clone(),
            row: live_row(ck(3), b"to_delete", 100),
            timestamp: 100,
        }])
        .unwrap();
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(3)).as_deref(),
        Some(&b"to_delete"[..])
    );

    // One mixed batch: two new writes + one row tombstone.
    engine
        .apply_batch(vec![
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(1), b"alpha", 200),
                timestamp: 200,
            },
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(2), b"beta", 200),
                timestamp: 200,
            },
            BatchOp::Tombstone {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                clustering: Some(ck(3)),
                timestamp: 300,
            },
        ])
        .unwrap();

    // Immediately visible: both writes readable, tombstoned row gone.
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"alpha"[..])
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"beta"[..])
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(3)),
        None,
        "tombstoned row must be gone"
    );
}

/// ATOMICITY (all-or-nothing on injected mid-batch failure): a batch where one
/// op targets an *unregistered* table must apply NONE of its ops — the already
/// registered table must show no partial state.
#[test]
fn apply_batch_all_or_nothing_on_injected_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    engine.register_table(test_schema("ks", "good")).unwrap();
    // "ks.bad" is intentionally NOT registered → mid-batch failure injection.
    let good = TableId::new("ks", "good");
    let key = make_key("p1");

    let result = engine.apply_batch(vec![
        BatchOp::Write {
            keyspace: "ks".into(),
            table: "good".into(),
            key: key.clone(),
            row: live_row(ck(1), b"should_not_persist", 100),
            timestamp: 100,
        },
        BatchOp::Write {
            keyspace: "ks".into(),
            table: "bad".into(), // unregistered → forces the batch to fail
            key: key.clone(),
            row: live_row(ck(2), b"x", 100),
            timestamp: 100,
        },
    ]);

    assert!(
        result.is_err(),
        "batch touching an unregistered table must fail loud"
    );

    // NONE applied: the write to the *good* table must not be visible.
    assert_eq!(
        read_cell(&engine, &good, &key, &ck(1)),
        None,
        "no op may be visible after an all-or-nothing batch failure"
    );
}

/// DURABILITY: an applied batch survives a simulated process restart — it is in
/// the commit log and replays into the reopened engine.
#[test]
fn apply_batch_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let key = make_key("p1");

    {
        let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
        engine.register_table(test_schema("ks", "t")).unwrap();
        engine
            .apply_batch(vec![
                BatchOp::Write {
                    keyspace: "ks".into(),
                    table: "t".into(),
                    key: key.clone(),
                    row: live_row(ck(1), b"durable_a", 100),
                    timestamp: 100,
                },
                BatchOp::Write {
                    keyspace: "ks".into(),
                    table: "t".into(),
                    key: key.clone(),
                    row: live_row(ck(2), b"durable_b", 100),
                    timestamp: 100,
                },
            ])
            .unwrap();
        engine.shutdown().unwrap();
    }

    // Simulated restart: reopen the same directory, re-register schema, replay.
    let (engine, pending) = StorageEngine::open(test_engine_config(dir.path()), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    engine.replay_mutations(pending).unwrap();

    let tid = TableId::new("ks", "t");
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"durable_a"[..]),
        "batched write must survive restart via commit-log replay"
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"durable_b"[..]),
        "batched write must survive restart via commit-log replay"
    );
}

/// FAIL-LOUD: an empty batch is a no-op `Ok(())`.
#[test]
fn apply_batch_empty_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    engine.apply_batch(vec![]).unwrap();
}

/// FAIL-LOUD: a batch op targeting an unregistered table returns a clear error
/// (not a silent partial). The error message names the missing table.
#[test]
fn apply_batch_unregistered_table_errors_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    let err = engine
        .apply_batch(vec![BatchOp::Write {
            keyspace: "ks".into(),
            table: "missing".into(),
            key: make_key("p1"),
            row: live_row(ck(1), b"x", 100),
            timestamp: 100,
        }])
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing") || msg.contains("not registered"),
        "error should name the unregistered table, got: {msg}"
    );
}

/// BatchTxn (Bolt explicit tx): `commit` applies all staged ops atomically;
/// `abort` leaves nothing durable / visible.
#[test]
fn batch_txn_commit_applies_abort_discards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    // Abort path: staged ops never become durable / visible.
    {
        let mut txn = engine.begin_batch();
        txn.stage(BatchOp::Write {
            keyspace: "ks".into(),
            table: "t".into(),
            key: key.clone(),
            row: live_row(ck(9), b"rolled_back", 50),
            timestamp: 50,
        });
        assert_eq!(txn.len(), 1);
        txn.abort();
    }
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(9)),
        None,
        "aborted op must not be visible"
    );

    // Commit path: all staged ops apply atomically.
    {
        let mut txn = engine.begin_batch();
        txn.stage(BatchOp::Write {
            keyspace: "ks".into(),
            table: "t".into(),
            key: key.clone(),
            row: live_row(ck(1), b"committed_a", 100),
            timestamp: 100,
        });
        txn.stage(BatchOp::Write {
            keyspace: "ks".into(),
            table: "t".into(),
            key: key.clone(),
            row: live_row(ck(2), b"committed_b", 100),
            timestamp: 100,
        });
        txn.commit().unwrap();
    }
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"committed_a"[..])
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"committed_b"[..])
    );
}
