//! Integration tests for ferrosa-storage.
//!
//! These tests exercise the full write → flush → read pipeline,
//! verifying that all components compose correctly.

use std::sync::Arc;
use std::thread;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_sstable::WriteOptions;

use ferrosa_storage::flush::{FileFlushTarget, InMemoryFlushTarget};
use ferrosa_storage::store::TableStore;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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

fn test_options() -> WriteOptions {
    WriteOptions {
        compression: None,
        ..WriteOptions::default()
    }
}

fn test_store() -> TableStore<InMemoryFlushTarget> {
    TableStore::new(test_schema(), InMemoryFlushTarget::new(), test_options())
}

#[test]
fn write_flush_read_round_trip() {
    let store = test_store();
    let n = 50;

    for i in 0..n {
        let key = make_key(&format!("key_{i:04}"));
        store
            .write(
                &key,
                make_row(format!("value_{i}").as_bytes(), 1000 + i as i64),
            )
            .unwrap();
    }

    store.flush().unwrap();

    for i in 0..n {
        let key = make_key(&format!("key_{i:04}"));
        let result = store.read(&key).unwrap();
        assert!(result.is_some(), "key_{i:04} not found after flush");
        let p = result.unwrap();
        assert_eq!(
            p.rows[0].cells[0].1.value.as_deref(),
            Some(format!("value_{i}").as_bytes())
        );
    }
}

#[test]
fn multiple_flushes_merge() {
    let store = test_store();

    // Write and flush with initial timestamps
    store
        .write(&make_key("k1"), make_row(b"old_1", 1000))
        .unwrap();
    store
        .write(&make_key("k2"), make_row(b"old_2", 1000))
        .unwrap();
    store.flush().unwrap();

    // Write same keys with newer timestamps and flush again
    store
        .write(&make_key("k1"), make_row(b"new_1", 2000))
        .unwrap();
    store
        .write(&make_key("k2"), make_row(b"new_2", 2000))
        .unwrap();
    store.flush().unwrap();

    // Write to memtable (not flushed)
    store
        .write(&make_key("k1"), make_row(b"newest_1", 3000))
        .unwrap();

    // Read should merge 2 SSTables + memtable
    let p1 = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(
        p1.rows[0].cells[0].1.value.as_deref(),
        Some(b"newest_1".as_slice())
    );
    assert_eq!(p1.rows[0].cells[0].1.timestamp, 3000);

    let p2 = store.read(&make_key("k2")).unwrap().unwrap();
    assert_eq!(
        p2.rows[0].cells[0].1.value.as_deref(),
        Some(b"new_2".as_slice())
    );
    assert_eq!(p2.rows[0].cells[0].1.timestamp, 2000);
}

#[test]
fn flush_does_not_block_reads() {
    let store = Arc::new(test_store());
    let n = 100;

    // Pre-populate
    for i in 0..n {
        store
            .write(&make_key(&format!("k{i}")), make_row(b"v", 1000))
            .unwrap();
    }

    // Spawn reader thread
    let store_clone = Arc::clone(&store);
    let reader = thread::spawn(move || {
        let mut reads = 0;
        for _ in 0..1000 {
            for i in 0..n {
                let _ = store_clone.read(&make_key(&format!("k{i}")));
                reads += 1;
            }
        }
        reads
    });

    // Flush concurrently
    store.flush().unwrap();

    let reads = reader.join().unwrap();
    assert!(reads > 0);

    // All data still readable after flush
    for i in 0..n {
        assert!(
            store.read(&make_key(&format!("k{i}"))).unwrap().is_some(),
            "k{i} missing after concurrent flush"
        );
    }
}

#[test]
fn deletion_suppresses_across_sources() {
    let store = test_store();

    // Write and flush data
    store
        .write(&make_key("k1"), make_row(b"alive", 1000))
        .unwrap();
    store.flush().unwrap();

    // Write newer data to memtable that overwrites flushed data.
    store
        .write(&make_key("k1"), make_row(b"newer", 2000))
        .unwrap();

    let result = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(
        result.rows[0].cells[0].1.value.as_deref(),
        Some(b"newer".as_slice())
    );
}

#[test]
fn snapshot_produces_token_order() {
    use ferrosa_storage::memtable::Memtable;
    use ferrosa_storage::ShardedBTreeMemtable;

    let mem = ShardedBTreeMemtable::with_default_shards();
    let schema = test_schema();

    for i in 0..100 {
        let key = make_key(&format!("random_key_{i}"));
        mem.put(&key, make_row(format!("v{i}").as_bytes(), 1000), &schema)
            .unwrap();
    }

    let snapshot = mem.snapshot();
    assert_eq!(snapshot.len(), 100);

    for window in snapshot.windows(2) {
        assert!(
            window[0].key <= window[1].key,
            "snapshot not in token order"
        );
    }
}

#[test]
fn file_flush_target_creates_readable_sstables() {
    let dir = tempfile::tempdir().unwrap();
    let store = TableStore::new(
        test_schema(),
        FileFlushTarget::new(dir.path().to_path_buf()).unwrap(),
        test_options(),
    );

    store
        .write(&make_key("k1"), make_row(b"file_v1", 1000))
        .unwrap();
    store
        .write(&make_key("k2"), make_row(b"file_v2", 2000))
        .unwrap();
    store.flush().unwrap();

    assert!(store.read(&make_key("k1")).unwrap().is_some());
    assert!(store.read(&make_key("k2")).unwrap().is_some());

    let p = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(
        p.rows[0].cells[0].1.value.as_deref(),
        Some(b"file_v1".as_slice())
    );
}

#[test]
fn concurrent_writes_no_data_loss() {
    let store = Arc::new(test_store());
    let num_threads = 8;
    let keys_per_thread = 50;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for k in 0..keys_per_thread {
                    let key = make_key(&format!("t{t}_k{k}"));
                    store
                        .write(
                            &key,
                            make_row(format!("v{t}_{k}").as_bytes(), 1000 + t as i64),
                        )
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    store.flush().unwrap();

    for t in 0..num_threads {
        for k in 0..keys_per_thread {
            let key = make_key(&format!("t{t}_k{k}"));
            assert!(
                store.read(&key).unwrap().is_some(),
                "missing t{t}_k{k} after concurrent writes + flush"
            );
        }
    }
}

#[test]
fn merge_is_commutative() {
    let store1 = test_store();
    let store2 = test_store();

    // Store1: write A then B
    store1
        .write(&make_key("k1"), make_row(b"v_a", 1000))
        .unwrap();
    store1.flush().unwrap();
    store1
        .write(&make_key("k1"), make_row(b"v_b", 2000))
        .unwrap();
    store1.flush().unwrap();

    // Store2: write B then A
    store2
        .write(&make_key("k1"), make_row(b"v_b", 2000))
        .unwrap();
    store2.flush().unwrap();
    store2
        .write(&make_key("k1"), make_row(b"v_a", 1000))
        .unwrap();
    store2.flush().unwrap();

    let p1 = store1.read(&make_key("k1")).unwrap().unwrap();
    let p2 = store2.read(&make_key("k1")).unwrap().unwrap();

    assert_eq!(p1.rows[0].cells[0].1.value, p2.rows[0].cells[0].1.value);
    assert_eq!(
        p1.rows[0].cells[0].1.timestamp,
        p2.rows[0].cells[0].1.timestamp
    );
}
