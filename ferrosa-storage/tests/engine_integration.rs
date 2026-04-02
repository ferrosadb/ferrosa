//! Integration tests for StorageEngine end-to-end workflows.

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use std::path::Path;
use std::time::Duration;

use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
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
            segment_size: 32 * 1024 * 1024, // 32 MB — must fit largest mutation
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
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
    }
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

/// Write → read round trip through the engine.
#[test]
fn write_read_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let schema = test_schema("ks", "users");
    engine.register_table(schema).unwrap();

    let tid = TableId::new("ks", "users");
    let keys: Vec<_> = (0..10).map(|i| make_key(&format!("user_{i}"))).collect();

    // Write 10 partitions.
    for (i, key) in keys.iter().enumerate() {
        let ts = (i as i64 + 1) * 1000;
        engine
            .write(&tid, key, make_row(format!("value_{i}").as_bytes(), ts), ts)
            .unwrap();
    }

    // Read all back.
    for (i, key) in keys.iter().enumerate() {
        let result = engine.read(&tid, key).unwrap();
        assert!(result.is_some(), "key user_{i} should exist");
        let p = result.unwrap();
        assert_eq!(
            p.rows[0].cells[0].1.value.as_deref(),
            Some(format!("value_{i}").as_bytes())
        );
    }

    engine.shutdown().unwrap();
}

/// Write → flush → read: data survives flush to SSTable.
#[test]
fn write_flush_read() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let schema = test_schema("ks", "orders");
    engine.register_table(schema).unwrap();

    let tid = TableId::new("ks", "orders");

    // Write data.
    for i in 0..5 {
        let key = make_key(&format!("order_{i}"));
        engine
            .write(&tid, &key, make_row(b"pending", 1000 + i), 1000 + i)
            .unwrap();
    }

    // Flush.
    engine.flush(&tid).unwrap();
    assert_eq!(engine.sstable_count(&tid), 1);
    assert_eq!(engine.memtable_size(&tid), 0);

    // All data still readable from SSTable.
    for i in 0..5 {
        let key = make_key(&format!("order_{i}"));
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "order_{i} should be readable after flush");
    }

    engine.shutdown().unwrap();
}

/// Write → flush → write more → read: memtable + SSTable merge.
#[test]
fn write_flush_write_read_merges() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let schema = test_schema("ks", "events");
    engine.register_table(schema).unwrap();

    let tid = TableId::new("ks", "events");
    let key = make_key("evt_1");

    // Write old value and flush.
    engine
        .write(&tid, &key, make_row(b"old_data", 1000), 1000)
        .unwrap();
    engine.flush(&tid).unwrap();

    // Write new value (stays in memtable).
    engine
        .write(&tid, &key, make_row(b"new_data", 2000), 2000)
        .unwrap();

    // Read should merge and return the newer value.
    let result = engine.read(&tid, &key).unwrap().unwrap();
    assert_eq!(
        result.rows[0].cells[0].1.value.as_deref(),
        Some(b"new_data".as_slice())
    );
    assert_eq!(result.rows[0].cells[0].1.timestamp, 2000);

    engine.shutdown().unwrap();
}

/// Multiple flushes accumulate SSTables; all data remains readable.
#[test]
fn multiple_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let schema = test_schema("ks", "logs");
    engine.register_table(schema).unwrap();

    let tid = TableId::new("ks", "logs");

    for batch in 0..3 {
        for i in 0..5 {
            let key = make_key(&format!("batch{batch}_entry{i}"));
            let ts = (batch * 1000 + i) as i64;
            engine
                .write(&tid, &key, make_row(b"log_data", ts), ts)
                .unwrap();
        }
        engine.flush(&tid).unwrap();
    }

    assert_eq!(engine.sstable_count(&tid), 3);

    // All 15 entries readable.
    for batch in 0..3 {
        for i in 0..5 {
            let key = make_key(&format!("batch{batch}_entry{i}"));
            assert!(engine.read(&tid, &key).unwrap().is_some());
        }
    }

    engine.shutdown().unwrap();
}

/// Multi-table isolation: writes to one table don't affect another.
#[test]
fn multi_table_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    engine.register_table(test_schema("ks", "table_a")).unwrap();
    engine.register_table(test_schema("ks", "table_b")).unwrap();

    let tid_a = TableId::new("ks", "table_a");
    let tid_b = TableId::new("ks", "table_b");
    let key = make_key("shared_key");

    engine
        .write(&tid_a, &key, make_row(b"from_a", 1000), 1000)
        .unwrap();
    engine
        .write(&tid_b, &key, make_row(b"from_b", 2000), 2000)
        .unwrap();

    // Each table sees its own data.
    let a = engine.read(&tid_a, &key).unwrap().unwrap();
    assert_eq!(
        a.rows[0].cells[0].1.value.as_deref(),
        Some(b"from_a".as_slice())
    );

    let b = engine.read(&tid_b, &key).unwrap().unwrap();
    assert_eq!(
        b.rows[0].cells[0].1.value.as_deref(),
        Some(b"from_b".as_slice())
    );

    // Flush one table, other is unaffected.
    engine.flush(&tid_a).unwrap();
    assert_eq!(engine.sstable_count(&tid_a), 1);
    assert_eq!(engine.sstable_count(&tid_b), 0);

    engine.shutdown().unwrap();
}

/// Concurrent writers to the same table produce no data loss.
#[test]
fn concurrent_writers() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let schema = test_schema("ks", "concurrent");
    engine.register_table(schema).unwrap();

    let tid = TableId::new("ks", "concurrent");
    let engine = std::sync::Arc::new(engine);

    let threads: Vec<_> = (0..4)
        .map(|t| {
            let engine = std::sync::Arc::clone(&engine);
            let tid = tid.clone();
            std::thread::spawn(move || {
                for i in 0..25 {
                    let key = make_key(&format!("t{t}_k{i}"));
                    let ts = (t * 1000 + i) as i64;
                    engine.write(&tid, &key, make_row(b"val", ts), ts).unwrap();
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    // All 100 writes should be readable.
    let mut found = 0;
    for t in 0..4 {
        for i in 0..25 {
            let key = make_key(&format!("t{t}_k{i}"));
            if engine.read(&tid, &key).unwrap().is_some() {
                found += 1;
            }
        }
    }
    assert_eq!(found, 100, "all 100 writes should be readable");

    engine.shutdown().unwrap();
}

/// Write 2000 unique keys, flush once, read all back.
/// Tests the SSTable writer/reader pipeline for large partition counts.
#[test]
fn flush_2000_keys_all_readable() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema("load", "data")).unwrap();
    let tid = TableId::new("load", "data");

    for i in 0..2000u64 {
        let key = make_key(&format!("k{i:06}"));
        let ts = i as i64;
        engine
            .write(&tid, &key, make_row(format!("v{i}").as_bytes(), ts), ts)
            .unwrap();
    }

    engine.flush(&tid).unwrap();

    let mut missing = 0u64;
    for i in 0..2000u64 {
        let key = make_key(&format!("k{i:06}"));
        if engine.read(&tid, &key).unwrap().is_none() {
            missing += 1;
            if missing <= 3 {
                eprintln!("MISSING after single flush: k{i:06}");
            }
        }
    }

    assert_eq!(
        missing, 0,
        "data loss: {missing}/2000 keys missing after single flush"
    );
    engine.shutdown().unwrap();
}

/// 1000 unique keys across 20 flush cycles — no data loss.
/// Mimics the loadgen pattern without concurrency to isolate
/// whether data loss is a flush/SSTable bug or a concurrency bug.
#[test]
fn many_flushes_no_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema("load", "data")).unwrap();
    let tid = TableId::new("load", "data");

    // Write 1000 unique keys across 20 flush cycles.
    for batch in 0..20u64 {
        for i in 0..50u64 {
            let key_idx = batch * 50 + i;
            let key = make_key(&format!("k{key_idx:06}"));
            let ts = key_idx as i64 * 1000;
            engine
                .write(
                    &tid,
                    &key,
                    make_row(format!("v{key_idx}").as_bytes(), ts),
                    ts,
                )
                .unwrap();
        }
        engine.flush(&tid).unwrap();
    }

    // Read back all 1000 keys.
    let mut missing = Vec::new();
    for i in 0..1000u64 {
        let key = make_key(&format!("k{i:06}"));
        if engine.read(&tid, &key).unwrap().is_none() {
            missing.push(format!("k{i:06}"));
        }
    }

    assert!(
        missing.is_empty(),
        "data loss: {}/1000 keys missing after 20 flushes: {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );

    engine.shutdown().unwrap();
}

/// Single writer + concurrent flusher — isolates flush race from write contention.
#[test]
fn single_writer_concurrent_flush_no_data_loss() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine.register_table(test_schema("load", "data")).unwrap();
    let tid = TableId::new("load", "data");

    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicU64::new(0));

    // Single writer thread
    let eng = engine.clone();
    let tid_w = tid.clone();
    let stop_w = stop.clone();
    let counter_w = counter.clone();
    let writer = std::thread::spawn(move || {
        let mut ts = 0i64;
        while !stop_w.load(Ordering::Relaxed) {
            let idx = counter_w.fetch_add(1, Ordering::SeqCst);
            let key = make_key(&format!("k{idx:08}"));
            ts += 1;
            let _ = eng.write(&tid_w, &key, make_row(format!("v{idx}").as_bytes(), ts), ts);
        }
    });

    // Main thread flushes rapidly for 3 seconds.
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(50));
        let _ = engine.flush(&tid);
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    engine.flush(&tid).unwrap();

    let total = counter.load(Ordering::SeqCst);
    let mut missing = 0u64;
    for i in 0..total {
        let key = make_key(&format!("k{i:08}"));
        if engine.read(&tid, &key).unwrap().is_none() {
            missing += 1;
        }
    }

    assert_eq!(
        missing, 0,
        "data loss: {missing}/{total} keys missing with single writer + concurrent flush"
    );
    engine.shutdown().unwrap();
}

/// Concurrent writes + periodic flushes — reproduces the loadgen data loss.
/// 8 writers, main thread flushes every 50ms for 10 seconds, 5000 key space.
#[test]
fn concurrent_writes_with_flushes_no_data_loss() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path());
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine.register_table(test_schema("load", "data")).unwrap();
    let tid = TableId::new("load", "data");

    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicU64::new(0));
    let written_keys = Arc::new(parking_lot::Mutex::new(
        std::collections::HashSet::<u64>::new(),
    ));

    let mut handles = vec![];
    for worker in 0..8u64 {
        let eng = engine.clone();
        let tid = tid.clone();
        let stop = stop.clone();
        let counter = counter.clone();
        let wk = written_keys.clone();
        handles.push(std::thread::spawn(move || {
            let mut ts = worker * 10_000_000;
            let mut local_counter = worker;
            while !stop.load(Ordering::Relaxed) {
                // Use a 5000 key space to create overwrites (like the loadgen).
                let key_idx = local_counter % 5000;
                local_counter += 8; // stride by worker count
                let key = make_key(&format!("k{key_idx:08}"));
                ts += 1;
                match eng.write(
                    &tid,
                    &key,
                    make_row(format!("v{ts}").as_bytes(), ts as i64),
                    ts as i64,
                ) {
                    Ok(()) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        wk.lock().insert(key_idx);
                    }
                    Err(_) => {
                        // Write failed — don't count it.
                    }
                }
            }
        }));
    }

    // Flush for 10 seconds.
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(50));
        let _ = engine.flush(&tid);
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    // Final flush.
    engine.flush(&tid).unwrap();

    // Read back only keys that were successfully written.
    let total_ops = counter.load(Ordering::SeqCst);
    let keys = written_keys.lock().clone();
    let mut missing = 0u64;
    for &key_idx in &keys {
        let key = make_key(&format!("k{key_idx:08}"));
        if engine.read(&tid, &key).unwrap().is_none() {
            missing += 1;
        }
    }
    let read_errors = engine.sstable_read_errors(&tid);
    eprintln!(
        "[TEST] total_ops={total_ops}, unique_keys={}, missing={missing}, read_errors={read_errors}, sstables={}",
        keys.len(),
        engine.sstable_count(&tid)
    );

    assert_eq!(
        missing,
        0,
        "data loss: {missing}/{} written keys missing after concurrent write+flush",
        keys.len()
    );

    engine.shutdown().unwrap();
}
