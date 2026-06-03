//! End-to-end pipeline integration tests.
//!
//! These tests exercise the full write -> commit log -> memtable -> flush ->
//! SSTable -> read path with realistic workloads.

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use std::time::Duration;

use ferrosa_storage::{
    CommitLog, CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
    SyncStrategyConfig, TableId,
};

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
        extensions: Default::default(),
    }
}

fn make_engine(dir: &std::path::Path) -> StorageEngine {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
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
    };
    StorageEngine::new(config, None).unwrap()
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

/// Write N partitions, flush, read all back, verify correctness.
#[test]
fn write_flush_read_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    engine.register_table(test_schema()).unwrap();
    let tid = TableId::new("ks", "tbl");

    let n = 50;
    for i in 0..n {
        let key = make_key(&format!("partition_{i:04}"));
        let row = make_row(format!("value_{i}").as_bytes(), 1000 + i);
        engine.write(&tid, &key, row, 1000 + i).unwrap();
    }

    engine.flush(&tid).unwrap();
    assert_eq!(engine.sstable_count(&tid), 1);
    assert_eq!(engine.memtable_size(&tid), 0);

    // Read every partition back.
    for i in 0..n {
        let key = make_key(&format!("partition_{i:04}"));
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "missing partition_{i:04}");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(format!("value_{i}").as_bytes())
        );
    }

    engine.shutdown().unwrap();
}

/// Write, flush, write more, read — verifies merge across memtable + SSTable.
#[test]
fn write_flush_write_merge() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    engine.register_table(test_schema()).unwrap();
    let tid = TableId::new("ks", "tbl");

    // Write and flush first batch.
    for i in 0..10 {
        let key = make_key(&format!("k{i:04}"));
        engine
            .write(&tid, &key, make_row(b"old", 1000), 1000)
            .unwrap();
    }
    engine.flush(&tid).unwrap();

    // Write newer values for same keys.
    for i in 0..10 {
        let key = make_key(&format!("k{i:04}"));
        engine
            .write(&tid, &key, make_row(b"new", 2000), 2000)
            .unwrap();
    }

    // Read should return the newer values (LWW merge).
    for i in 0..10 {
        let key = make_key(&format!("k{i:04}"));
        let partition = engine.read(&tid, &key).unwrap().unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice()),
            "partition k{i:04} should have newer value"
        );
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
    }

    engine.shutdown().unwrap();
}

/// Multiple flushes, read all partitions across SSTables.
#[test]
fn multiple_flushes_read_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    engine.register_table(test_schema()).unwrap();
    let tid = TableId::new("ks", "tbl");

    // 3 rounds of write+flush.
    for round in 0..3 {
        for i in 0..5 {
            let key = make_key(&format!("r{round}_k{i}"));
            engine
                .write(&tid, &key, make_row(b"v", 1000), 1000)
                .unwrap();
        }
        engine.flush(&tid).unwrap();
    }

    assert_eq!(engine.sstable_count(&tid), 3);

    // All 15 partitions should be readable.
    for round in 0..3 {
        for i in 0..5 {
            let key = make_key(&format!("r{round}_k{i}"));
            assert!(
                engine.read(&tid, &key).unwrap().is_some(),
                "missing r{round}_k{i}"
            );
        }
    }

    engine.shutdown().unwrap();
}

/// Simulates a crash by dropping the engine without shutdown, then
/// verifies that open_and_replay recovers the mutations.
#[test]
fn wal_replay_after_crash() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write some data, then "crash" (drop without shutdown).
    {
        let engine = make_engine(dir.path());
        engine.register_table(test_schema()).unwrap();
        let tid = TableId::new("ks", "tbl");

        for i in 0..5 {
            let key = make_key(&format!("crash_k{i}"));
            engine
                .write(&tid, &key, make_row(b"recoverable", 1000), 1000)
                .unwrap();
        }
        // Drop engine without calling shutdown — simulates crash.
        // The commit log has BatchSync, so data is fsynced on each write.
    }

    // Phase 2: Replay the commit log.
    let replay_config = CommitLogConfig {
        segment_size: 4096,
        max_segment_age: Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        batch: Default::default(),
        log_dir: dir.path().join("commitlog"),
        checkpoint_dir: dir.path().join("commitlog"),
        archive: None,
    };
    let (commit_log, mutations) = CommitLog::open_and_replay(replay_config).unwrap();

    assert_eq!(mutations.len(), 5, "should recover 5 mutations from WAL");
    for mutation in &mutations {
        assert!(mutation.key.key.as_bytes().starts_with(b"crash_k"));
    }

    commit_log.shutdown().unwrap();
}

/// Concurrent writers and a reader during flush — verifies no data loss
/// or panics under contention.
#[test]
fn concurrent_writers_during_flush() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(make_engine(dir.path()));
    engine.register_table(test_schema()).unwrap();
    let tid = TableId::new("ks", "tbl");

    let writers: Vec<_> = (0..4)
        .map(|t| {
            let engine = Arc::clone(&engine);
            let tid = tid.clone();
            thread::spawn(move || {
                for i in 0..20 {
                    let key = make_key(&format!("t{t}_k{i}"));
                    engine
                        .write(&tid, &key, make_row(b"v", 1000 + t), 1000 + t)
                        .unwrap();
                }
            })
        })
        .collect();

    // Flush while writers are active.  Wait long enough that at least some
    // writes land first — 1 ms was too short on resource-constrained CI runners.
    let flusher = {
        let engine = Arc::clone(&engine);
        let tid = tid.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            engine.flush(&tid).unwrap();
        })
    };

    for w in writers {
        w.join().unwrap();
    }
    flusher.join().unwrap();

    // All 80 partitions should be readable (some from memtable, some from SSTable).
    for t in 0..4i64 {
        for i in 0..20 {
            let key = make_key(&format!("t{t}_k{i}"));
            assert!(
                engine.read(&tid, &key).unwrap().is_some(),
                "missing t{t}_k{i}"
            );
        }
    }

    engine.shutdown().unwrap();
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    // Write arbitrary partitions, flush, verify all are readable.
    #[test]
    fn arbitrary_writes_recoverable(
        count in 1usize..30,
        base_ts in 1000i64..100_000,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(dir.path());
        engine.register_table(test_schema()).unwrap();
        let tid = TableId::new("ks", "tbl");

        let mut keys = Vec::new();
        for i in 0..count {
            let key = make_key(&format!("prop_k{i:06}"));
            let row = make_row(format!("val_{i}").as_bytes(), base_ts + i as i64);
            engine.write(&tid, &key, row, base_ts + i as i64).unwrap();
            keys.push(key);
        }

        engine.flush(&tid).unwrap();

        for (i, key) in keys.iter().enumerate() {
            let result = engine.read(&tid, key).unwrap();
            prop_assert!(result.is_some(), "missing partition {i}");
        }

        engine.shutdown().unwrap();
    }
}
