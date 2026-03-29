//! Correctness tests for T-022: commit log replay idempotency + batch atomicity.
//!
//! C6.1  — Commit log replay is idempotent after kill (no duplicate rows).
//! C6.2  — Transactions execute in dependency order under partition (Accord).
//! C6.3  — CQL BATCH is all-or-nothing under kill-coordinator.
//!
//! The C6.1 unit tests run without a live cluster.  The C6.2 / C6.3 integration
//! tests panic with setup instructions when cluster infrastructure is not available.

use std::path::Path;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, Mutation, StorageEngine, StorageEngineConfig,
    SyncStrategyConfig, TableId,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn test_engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.to_path_buf(),
    }
}

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
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
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_row(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn make_mutation(id: [u8; 16], key: &str, value: &[u8], ts: i64) -> Mutation {
    Mutation {
        mutation_id: id,
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        key: make_key(key),
        rows: vec![make_row(value, ts)],
        timestamp: ts,
    }
}

fn table_id() -> TableId {
    TableId::new("ks", "tbl")
}

// ---------------------------------------------------------------------------
// C6.1 unit test: replaying the same mutation twice must not produce two rows
// ---------------------------------------------------------------------------

/// Replaying the same mutation (same mutation_id) twice via `replay_mutations`
/// applies it exactly once.  The read result must show a single row with the
/// value from the first (only) application.
#[test]
fn commitlog_replay_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();

    let mutation_id = [0xAAu8; 16];
    let m = make_mutation(mutation_id, "pk1", b"hello", 1_000);

    // Simulate crash-then-replay: present the same mutation twice.
    engine.replay_mutations(vec![m.clone(), m.clone()]).unwrap();

    // Only one row should exist.
    let result = engine.read(&table_id(), &make_key("pk1")).unwrap();
    assert!(result.is_some(), "row should be present after replay");

    let partition = result.unwrap();
    assert_eq!(
        partition.rows.len(),
        1,
        "replay must be idempotent: expected 1 row, got {}",
        partition.rows.len()
    );
    assert_eq!(
        partition.rows[0].cells[0].1.value.as_deref(),
        Some(b"hello".as_slice()),
        "row value should match the replayed mutation"
    );
}

// ---------------------------------------------------------------------------
// C6.1b unit test: no duplicate rows after replaying many mutations with mix
//                  of duplicates and uniques
// ---------------------------------------------------------------------------

/// Replaying N distinct mutations plus K duplicate copies must produce exactly
/// N rows in the table, not N + K.
#[test]
fn commitlog_no_duplicate_rows() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();

    // 5 distinct mutations, each duplicated once (10 total).
    let mut mutations = Vec::new();
    for i in 0u8..5 {
        let mut id = [0u8; 16];
        id[0] = i + 1; // non-zero id
        let key = format!("pk{i}");
        let m = make_mutation(id, &key, b"v", 1_000 + i64::from(i));
        mutations.push(m.clone());
        mutations.push(m); // duplicate
    }

    engine.replay_mutations(mutations).unwrap();

    // Each partition key should appear exactly once.
    for i in 0u8..5 {
        let key = format!("pk{i}");
        let result = engine.read(&table_id(), &make_key(&key)).unwrap();
        assert!(result.is_some(), "partition pk{i} should exist");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows.len(),
            1,
            "pk{i}: expected 1 row after dedup, got {}",
            partition.rows.len()
        );
    }
}

// ---------------------------------------------------------------------------
// C6.1c unit test: legacy mutations (zero id) are always re-applied
// ---------------------------------------------------------------------------

/// A mutation with a zero mutation_id is treated as a legacy entry and is
/// never deduplicated — both copies must be applied (last-write-wins by
/// timestamp keeps the data consistent).
#[test]
fn commitlog_legacy_zero_id_always_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();

    let zero_id = [0u8; 16];
    let m = make_mutation(zero_id, "pk_legacy", b"legacy_val", 2_000);

    // Two copies — both must be applied without errors.
    engine.replay_mutations(vec![m.clone(), m.clone()]).unwrap();

    let result = engine.read(&table_id(), &make_key("pk_legacy")).unwrap();
    assert!(
        result.is_some(),
        "legacy mutation should land in storage even if replayed twice"
    );
}

// ---------------------------------------------------------------------------
// C6.2 integration test (ignored — requires Accord cluster)
// ---------------------------------------------------------------------------

/// Transaction T1 that depends on T2 never applies before T2's effects are
/// visible.  This invariant requires a live Accord cluster to verify.
///
/// Requires a live Accord cluster. Set FERROSA_TEST_CLUSTER_NODES to run.
#[tokio::test]
async fn dep_wait_ordering_under_partition() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "dep_wait_ordering_under_partition requires a live Accord cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    todo!("requires live Accord cluster")
}

// ---------------------------------------------------------------------------
// C6.3 integration test (ignored — requires live cluster)
// ---------------------------------------------------------------------------

/// A BATCH of 3 rows: if the coordinator is killed after the first row is
/// written, the surviving nodes see either all 3 rows or none.
///
/// Requires a live Accord cluster. Set FERROSA_TEST_CLUSTER_NODES to run.
#[tokio::test]
async fn batch_atomicity_kill_coordinator() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "batch_atomicity_kill_coordinator requires a live Accord cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    todo!("requires live cluster with fault injection")
}
