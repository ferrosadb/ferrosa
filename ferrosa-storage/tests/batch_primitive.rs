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
    engine_config_with(dir, SyncStrategyConfig::Batch, 256 * 1024)
}

/// Engine config with an explicit commit-log sync strategy + segment size.
///
/// The durability and oversized-entry tests pin behavior under the PRODUCTION
/// default sync strategy (`Periodic`), not the zero-loss `Batch` strategy the
/// other tests use, so the synchronous-durability guarantee is proven where it
/// actually has to hold.
fn engine_config_with(
    dir: &Path,
    sync_strategy: SyncStrategyConfig,
    segment_size: usize,
) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size,
            max_segment_age: Duration::from_secs(60),
            sync_strategy,
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

/// DURABILITY (synchronous, under the PRODUCTION default strategy + crash):
/// after `apply_batch` returns `Ok` under `Periodic` sync, the batch is on disk
/// **before** the call returns — a crash that loses all in-memory state and the
/// background sync timer must NOT lose an acked batch.
///
/// Crash is simulated faithfully: the `Periodic` background thread is set to a
/// 1-hour interval so it cannot fsync during the test, and the engine is
/// *dropped without `shutdown()`* (no clean flush — `PeriodicSync::drop` does
/// not flush). The ONLY way the bytes can be on disk is an explicit
/// force-sync inside `apply_batch`. We then reopen the directory and replay:
/// the rows must be present.
#[test]
fn apply_batch_is_synchronously_durable_under_periodic_sync() {
    let dir = tempfile::tempdir().unwrap();
    let key = make_key("p1");
    // 1-hour interval: the periodic background thread will not fsync during the
    // test, so durability can only come from a synchronous force-sync.
    let cfg = || engine_config_with(dir.path(), periodic_one_hour(), 256 * 1024);

    {
        let engine = StorageEngine::new(cfg(), None).unwrap();
        engine.register_table(test_schema("ks", "t")).unwrap();
        engine
            .apply_batch(vec![
                BatchOp::Write {
                    keyspace: "ks".into(),
                    table: "t".into(),
                    key: key.clone(),
                    row: live_row(ck(1), b"acked_a", 100),
                    timestamp: 100,
                },
                BatchOp::Write {
                    keyspace: "ks".into(),
                    table: "t".into(),
                    key: key.clone(),
                    row: live_row(ck(2), b"acked_b", 100),
                    timestamp: 100,
                },
            ])
            .unwrap();
        // SIMULATED CRASH: drop the engine WITHOUT shutdown(). Under Periodic
        // the Drop path does not flush, so anything on disk got there only via
        // a synchronous fsync inside apply_batch.
        drop(engine);
    }

    let (engine, pending) = StorageEngine::open(cfg(), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    engine.replay_mutations(pending).unwrap();

    let tid = TableId::new("ks", "t");
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"acked_a"[..]),
        "acked batch must be durable before apply_batch returns (no background sync, no clean shutdown)"
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"acked_b"[..]),
        "acked batch must be durable before apply_batch returns (no background sync, no clean shutdown)"
    );
}

/// ATOMICITY (Phase-1 mid-batch append failure): a batch whose SECOND op is too
/// large to fit a commit-log segment must apply NONE of its ops — neither
/// durably (no replay of op #1) nor in-memory. The pre-existing implementation
/// appended op #1 before op #2's append failed, leaving op #1 durable: a
/// partial apply surviving a crash. The whole-batch size pre-flight must reject
/// before any append.
#[test]
fn apply_batch_oversized_entry_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    // Small segment so a modest row overflows a single commit-log entry. Batch
    // sync keeps the test deterministic (the failure is the append, not sync).
    let cfg = || engine_config_with(dir.path(), SyncStrategyConfig::Batch, 4 * 1024);

    let result = {
        let engine = StorageEngine::new(cfg(), None).unwrap();
        engine.register_table(test_schema("ks", "t")).unwrap();
        let key = make_key("p1");
        // Op #1 is small and would append fine; op #2's value exceeds the
        // segment capacity so its append fails mid-batch.
        let huge = vec![b'x'; 8 * 1024];
        let r = engine.apply_batch(vec![
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(1), b"small_first", 100),
                timestamp: 100,
            },
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(2), &huge, 100),
                timestamp: 100,
            },
        ]);
        assert!(
            r.is_err(),
            "a batch with an entry larger than a segment must fail loud"
        );
        // In-memory: op #1 must NOT be visible (all-or-nothing).
        let tid = TableId::new("ks", "t");
        assert_eq!(
            read_cell(&engine, &tid, &key, &ck(1)),
            None,
            "op #1 must not be visible after an all-or-nothing batch rejection"
        );
        drop(engine);
        r
    };
    assert!(result.is_err());

    // Durable: op #1 must NOT replay after a restart — nothing was appended.
    let (engine, pending) = StorageEngine::open(cfg(), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    engine.replay_mutations(pending).unwrap();
    let tid = TableId::new("ks", "t");
    assert_eq!(
        read_cell(&engine, &tid, &make_key("p1"), &ck(1)),
        None,
        "no op of a rejected batch may survive a crash (no partial durable apply)"
    );
}

/// TOMBSTONE LOWERING (whole-partition delete): a `Tombstone { clustering:
/// None }` in a batch must delete every clustered row of the partition, not
/// just an empty-clustering row. Exercises the `clustering.unwrap_or_default()`
/// partition-tombstone lowering path that the existing tests (which only delete
/// a single `Some(ck)` row) never covered.
#[test]
fn apply_batch_partition_tombstone_deletes_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(test_engine_config(dir.path()), None).unwrap();
    engine.register_table(test_schema("ks", "t")).unwrap();
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    // Seed two clustered rows in the partition.
    engine
        .apply_batch(vec![
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(1), b"r1", 100),
                timestamp: 100,
            },
            BatchOp::Write {
                keyspace: "ks".into(),
                table: "t".into(),
                key: key.clone(),
                row: live_row(ck(2), b"r2", 100),
                timestamp: 100,
            },
        ])
        .unwrap();
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"r1"[..])
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"r2"[..])
    );

    // Whole-partition tombstone (clustering: None) at a higher timestamp.
    engine
        .apply_batch(vec![BatchOp::Tombstone {
            keyspace: "ks".into(),
            table: "t".into(),
            key: key.clone(),
            clustering: None,
            timestamp: 200,
        }])
        .unwrap();

    // Both rows must now be gone.
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)),
        None,
        "partition tombstone must delete row ck(1)"
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)),
        None,
        "partition tombstone must delete row ck(2)"
    );
}

/// A `Periodic` strategy whose background fsync interval is one hour — long
/// enough that the timer never fires during a unit test, isolating synchronous
/// durability from background flushing.
fn periodic_one_hour() -> SyncStrategyConfig {
    SyncStrategyConfig::Periodic {
        sync_interval: Duration::from_secs(3600),
    }
}
