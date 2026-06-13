//! Connection transaction state machine (URS-QEC-B02).
//!
//! A `ConnTxn` is the per-connection explicit-transaction state machine that
//! backs Bolt `BEGIN` / `RUN` / `COMMIT` / `ROLLBACK`. It:
//!
//! * `BEGIN` opens a tx (`tx_id`, queues statements, defers execution),
//! * `RUN`/`PULL` inside a tx STAGE writes onto the primitive's
//!   `begin_batch()` -> `BatchTxn` (here: an owned op buffer materialized into a
//!   `BatchTxn` only at commit),
//! * `COMMIT` atomically commits the staged batch via `BatchTxn::commit`
//!   (durable, all-or-nothing),
//! * `ROLLBACK` aborts (`BatchTxn::abort`, nothing persisted),
//! * reads inside the tx see the connection's own staged writes,
//! * protocol misuse (nested BEGIN, COMMIT/ROLLBACK with no open tx) FAILS LOUD.
//!
//! These tests pin the three headline guarantees (FAIL-LOUD, NEVER FAKE,
//! URS-QEC-X01): a `COMMIT` durably persists ALL staged ops or FAILS — it never
//! acks a transaction it didn't persist; a `ROLLBACK` persists NOTHING; a failed
//! `COMMIT` leaves NO partial state.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use ferrosa_cql::session::ConnTxn;
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

fn engine_config(dir: &Path) -> StorageEngineConfig {
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

fn write_op(table: &str, key: &DecoratedKey, ck: Vec<u8>, val: &[u8], ts: i64) -> BatchOp {
    BatchOp::Write {
        keyspace: "ks".into(),
        table: table.into(),
        key: key.clone(),
        row: live_row(ck, val, ts),
        timestamp: ts,
    }
}

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

fn engine_with_table(dir: &Path, table: &str) -> Arc<StorageEngine> {
    let engine = Arc::new(StorageEngine::new(engine_config(dir), None).unwrap());
    engine.register_table(test_schema("ks", table)).unwrap();
    engine
}

/// BEGIN + writes + COMMIT persists ALL staged writes atomically and durably.
#[test]
fn begin_writes_commit_persists_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin(42).expect("begin opens tx");
    assert!(tx.is_open());
    assert_eq!(tx.tx_id(), Some(42));

    // Nothing is durable before COMMIT (deferred execution).
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();
    tx.stage(write_op("t", &key, ck(2), b"beta", 100)).unwrap();
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)),
        None,
        "staged write must NOT be durable before COMMIT"
    );

    tx.commit(&engine).expect("commit must persist");
    assert!(!tx.is_open(), "tx closed after commit");

    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"alpha"[..])
    );
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(2)).as_deref(),
        Some(&b"beta"[..])
    );
}

/// Reads inside the tx see the connection's own staged (uncommitted) writes.
#[test]
fn reads_see_own_staged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let _engine = engine_with_table(dir.path(), "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin(7).unwrap();
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();
    tx.stage(write_op("t", &key, ck(2), b"beta", 100)).unwrap();

    let staged = tx.staged_ops();
    assert_eq!(staged.len(), 2, "own staged writes visible inside the tx");
}

/// BEGIN + writes + ROLLBACK persists NOTHING.
#[test]
fn begin_writes_rollback_persists_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin(1).unwrap();
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();
    tx.stage(write_op("t", &key, ck(2), b"beta", 100)).unwrap();

    tx.rollback().expect("rollback aborts cleanly");
    assert!(!tx.is_open(), "tx closed after rollback");

    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)),
        None,
        "ROLLBACK must persist nothing"
    );
    assert_eq!(read_cell(&engine, &tid, &key, &ck(2)), None);
}

/// A failed COMMIT FAILS LOUD and leaves NO partial state — one op targets an
/// unregistered table, so the whole batch must fail and persist none.
#[test]
fn failed_commit_fails_loud_no_partial() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "good");
    let tid = TableId::new("ks", "good");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin(9).unwrap();
    tx.stage(write_op("good", &key, ck(1), b"should_not_persist", 100))
        .unwrap();
    // "ks.bad" is intentionally unregistered → forces the batch to fail.
    tx.stage(write_op("bad", &key, ck(2), b"x", 100)).unwrap();

    let err = tx.commit(&engine).expect_err("commit must FAIL LOUD");
    let _ = err; // a real Err, not a fake Ok

    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)),
        None,
        "a failed COMMIT must leave NO partial state"
    );
}

/// Protocol misuse FAILS LOUD: nested BEGIN, and COMMIT/ROLLBACK with no open tx.
#[test]
fn protocol_misuse_fails_loud() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");

    let mut tx = ConnTxn::new();
    // COMMIT with no open tx.
    assert!(tx.commit(&engine).is_err(), "COMMIT with no tx must fail");
    // ROLLBACK with no open tx.
    assert!(tx.rollback().is_err(), "ROLLBACK with no tx must fail");
    // STAGE with no open tx.
    let key = make_key("p1");
    assert!(
        tx.stage(write_op("t", &key, ck(1), b"x", 100)).is_err(),
        "STAGE outside a tx must fail"
    );

    tx.begin(1).unwrap();
    // Nested BEGIN.
    assert!(tx.begin(2).is_err(), "nested BEGIN must fail");
}

// ── URS-QEC-B03: per-transaction timeout enforcement ────────────────────────
//
// A transaction opened with a deadline must, once that deadline is exceeded,
// ABORT (discard all staged writes, close the tx) and FAIL LOUD on the next
// `stage`/`commit` — never silently commit a timed-out transaction. The error
// is a distinct timeout (so the caller emits a Bolt FAILURE the driver can
// classify), not a generic Invalid.

/// A COMMIT after the per-tx deadline has passed FAILS LOUD and persists
/// NOTHING (URS-QEC-B03, fail-loud).
#[test]
fn commit_after_timeout_fails_loud_and_persists_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin_with_timeout(7, Duration::from_millis(20))
        .expect("begin opens tx with a deadline");
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();

    // Let the transaction deadline pass.
    std::thread::sleep(Duration::from_millis(40));

    let err = tx
        .commit(&engine)
        .expect_err("commit after the deadline must FAIL, never silently persist");
    assert!(
        err.is_transaction_timeout(),
        "expected a transaction-timeout error, got: {err:?}"
    );
    // FAIL-LOUD, NEVER FAKE: nothing was persisted.
    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)),
        None,
        "a timed-out COMMIT must persist NOTHING"
    );
    // The transaction is over.
    assert!(!tx.is_open(), "a timed-out tx is closed");
}

/// Staging a write after the per-tx deadline has passed FAILS LOUD and aborts
/// the transaction (URS-QEC-B03).
#[test]
fn stage_after_timeout_fails_loud_and_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin_with_timeout(9, Duration::from_millis(20)).unwrap();
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();

    std::thread::sleep(Duration::from_millis(40));

    let err = tx
        .stage(write_op("t", &key, ck(2), b"beta", 100))
        .expect_err("staging after the deadline must FAIL");
    assert!(
        err.is_transaction_timeout(),
        "expected a transaction-timeout error, got: {err:?}"
    );
    assert!(
        !tx.is_open(),
        "the timed-out tx is aborted on the late stage"
    );

    // Even an attempt to commit now is a no-op failure — nothing persisted.
    assert!(tx.commit(&engine).is_err());
    assert_eq!(read_cell(&engine, &tid, &key, &ck(1)), None);
}

/// A transaction that COMMITs within its timeout still persists normally:
/// the deadline must not abort a healthy tx (URS-QEC-B03).
#[test]
fn commit_within_timeout_persists() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin_with_timeout(11, Duration::from_secs(30)).unwrap();
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();
    tx.commit(&engine)
        .expect("commit within the timeout persists");

    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"alpha"[..])
    );
}

/// A transaction opened without a timeout (`begin`) never times out: it is the
/// unbounded default and a late COMMIT still succeeds (URS-QEC-B03).
#[test]
fn begin_without_timeout_never_expires() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_table(dir.path(), "t");
    let tid = TableId::new("ks", "t");
    let key = make_key("p1");

    let mut tx = ConnTxn::new();
    tx.begin(13).unwrap();
    tx.stage(write_op("t", &key, ck(1), b"alpha", 100)).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    tx.commit(&engine)
        .expect("a no-timeout tx must never expire");

    assert_eq!(
        read_cell(&engine, &tid, &key, &ck(1)).as_deref(),
        Some(&b"alpha"[..])
    );
}
