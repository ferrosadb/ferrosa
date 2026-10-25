//! Measure the real storage write entry point and async-observer fan-out.
//! Correctness: queued values and persisted rows agree; observers share payloads.
//! Last revised: 2026-10-24
//! Last changed: Added a repeatable allocation probe and a sharing regression.
//!
//! Counts are allocation requests and requested bytes on the calling thread,
//! including reallocations. They exclude fixture construction, queue draining,
//! reporting and other threads. They are not RSS, peak live memory or throughput.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    Mutation, ObserverMode, StorageEngine, StorageEngineConfig, TableId, WriteObserver,
};

struct CountingAllocator;

thread_local! {
    static COUNTERS: Cell<(bool, usize, usize)> = const { Cell::new((false, 0, 0)) };
}

fn count(bytes: usize) {
    let _ = COUNTERS.try_with(|counter| {
        let (armed, calls, total) = counter.get();
        if armed {
            counter.set((true, calls + 1, total + bytes));
        }
    });
}

// SAFETY: this wrapper preserves System's allocation contract and forwards the
// original pointer/layout unchanged. The const-initialized TLS counters allocate
// nothing, and try_with also tolerates allocations during thread teardown.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measured<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    COUNTERS.with(|counter| counter.set((true, 0, 0)));
    let result = f();
    let (_, calls, bytes) = COUNTERS.with(|counter| counter.replace((false, 0, 0)));
    (result, calls, bytes)
}

struct Observer(TableId);

impl WriteObserver for Observer {
    fn mode(&self) -> ObserverMode {
        ObserverMode::Async
    }

    fn tables(&self) -> Vec<TableId> {
        vec![self.0.clone()]
    }

    fn watches_table(&self, table: &TableId) -> bool {
        table == &self.0
    }

    fn on_write(&self, _: &TableId, _: &Mutation) -> Vec<Mutation> {
        Vec::new()
    }
}

fn schema(cells: usize) -> TableSchema {
    TableSchema {
        keyspace: "payload_probe".into(),
        table: "rows".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".into(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
        }],
        static_columns: vec![],
        regular_columns: (0..cells)
            .map(|i| ColumnDefinition {
                name: format!("c{i}"),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            })
            .collect(),
        extensions: Default::default(),
    }
}

fn row(cells: usize, timestamp: i64) -> Row {
    Row {
        clustering: 1i32.to_be_bytes().to_vec(),
        cells: (0..cells)
            .map(|i| (i as u16, CellValue::live(vec![b'x'; 64], timestamp)))
            .collect(),
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn engine(dir: &std::path::Path, cells: usize) -> StorageEngine {
    let mut config = StorageEngineConfig::test_config(dir);
    // This probe measures writes and delivery, not background flush scheduling.
    config.flush_threshold_bytes = 64 * 1024 * 1024;
    // The largest fixture encodes to about 12 KiB; the unit-test default
    // segment is only 8 KiB. Keep a full sample series inside a 1 MiB segment.
    config.commit_log.segment_size = 1024 * 1024;
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(schema(cells)).unwrap();
    engine
}

#[test]
fn measure_observer_write_allocations() {
    // Prove that the allocator window actually counts and closes.
    let (sentinel, calls, bytes) = measured(|| vec![7u8; 513]);
    assert!(calls > 0 && bytes >= 513);
    std::hint::black_box(sentinel);

    for cells in [1, 32, 128] {
        for observers in [0, 1, 4] {
            let dir = tempfile::tempdir().unwrap();
            let engine = engine(dir.path(), cells);
            let table = TableId::new("payload_probe", "rows");
            let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
            let mut receivers: Vec<_> = (0..observers)
                .map(|_| engine.register_async_observer(Arc::new(Observer(table.clone())), 16))
                .collect();
            let mut samples = Vec::with_capacity(8);
            for iteration in 0..12 {
                let timestamp = 1_800_000_000_000_000 + iteration;
                let (input, input_calls, input_bytes) = measured(|| row(cells, timestamp));
                // Build the oracle independently: cloning input here could
                // promote shared backing before the write's measured window.
                let expected = row(cells, timestamp);
                let (result, calls, bytes) =
                    measured(|| engine.write(&table, &key, input, timestamp));
                result.unwrap();
                for receiver in &mut receivers {
                    let (received_table, mutation) = receiver.try_recv().unwrap();
                    assert_eq!(received_table, table);
                    assert_eq!(mutation.key, key);
                    assert_eq!(mutation.rows.as_slice(), std::slice::from_ref(&expected));
                    assert!(receiver.try_recv().is_err());
                }
                let stored = engine.read(&table, &key).unwrap().unwrap();
                assert_eq!(stored.rows, [expected]);
                if iteration >= 4 {
                    samples.push((calls, bytes, input_calls, input_bytes));
                }
            }
            println!(
                "OBSERVER_ALLOC {}",
                serde_json::json!({
                    "cells": cells, "observers": observers,
                    "samples": samples.iter().map(|(calls, bytes, input_calls, input_bytes)| serde_json::json!({
                        "allocation_calls": calls, "requested_bytes": bytes,
                        "input_allocation_calls": input_calls, "input_requested_bytes": input_bytes
                    })).collect::<Vec<_>>()
                })
            );
        }
    }
}

#[test]
fn observers_share_the_same_immutable_payload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), 4);
    let table = TableId::new("payload_probe", "rows");
    let observer = Arc::new(Observer(table.clone()));
    let mut left = engine.register_async_observer(observer.clone(), 1);
    let mut right = engine.register_async_observer(observer, 1);
    let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
    let expected = row(4, 1_800_000_000_000_000);
    engine
        .write(&table, &key, expected.clone(), 1_800_000_000_000_000)
        .unwrap();
    // Keep both payloads alive throughout the identity comparison. Equal
    // addresses cannot then be allocator reuse after dropping the first value.
    let (_, a) = left.try_recv().unwrap();
    let (_, b) = right.try_recv().unwrap();
    assert_eq!(a.rows.as_slice(), std::slice::from_ref(&expected));
    assert_eq!(b.rows, [expected]);
    assert!(
        std::ptr::eq(a.rows.as_ptr(), b.rows.as_ptr()),
        "fan-out deep-copied rows"
    );
    assert!(
        std::ptr::eq(a.keyspace.as_ptr(), b.keyspace.as_ptr()),
        "fan-out deep-copied identity"
    );
}

#[test]
fn owned_and_borrowed_batches_share_observer_payloads() {
    for atomic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 4);
        let table = TableId::new("payload_probe", "rows");
        let observer = Arc::new(Observer(table.clone()));
        let mut left = engine.register_async_observer(observer.clone(), 1);
        let mut right = engine.register_async_observer(observer, 1);
        let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
        let timestamp = 1_800_000_000_000_000;
        let expected = row(4, timestamp);
        if atomic {
            engine
                .write_atomic_batch(vec![Mutation::new(
                    table.keyspace.clone(),
                    table.table.clone(),
                    key.clone(),
                    vec![expected.clone()],
                    timestamp,
                )])
                .unwrap();
        } else {
            engine
                .batch_write(&table, vec![(key.clone(), expected.clone(), timestamp)])
                .unwrap();
        }
        let (_, a) = left.try_recv().unwrap();
        let (_, b) = right.try_recv().unwrap();
        assert_eq!(a.rows.as_slice(), std::slice::from_ref(&expected));
        assert_eq!(b.rows.as_slice(), std::slice::from_ref(&expected));
        assert_eq!(a.key, key);
        assert_eq!(a.mutation_id, b.mutation_id);
        assert!(std::ptr::eq(a.rows.as_ptr(), b.rows.as_ptr()));
        assert!(std::ptr::eq(a.keyspace.as_ptr(), b.keyspace.as_ptr()));
        assert_eq!(engine.read(&table, &key).unwrap().unwrap().rows, [expected]);
    }
}

#[test]
fn input_memtable_and_observers_share_bytes_and_keep_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), 4);
    let table = TableId::new("payload_probe", "rows");
    let observer = Arc::new(Observer(table.clone()));
    let mut left = engine.register_async_observer(observer.clone(), 1);
    let mut right = engine.register_async_observer(observer, 1);
    let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
    let timestamp = 1_800_000_000_000_000;
    // Hold the original input AND all consumers alive. A deep copy cannot pass
    // by freeing the original allocation and accidentally reusing its address.
    let input = row(4, timestamp);
    engine
        .write(&table, &key, input.clone(), timestamp)
        .unwrap();
    let (_, a) = left.try_recv().unwrap();
    let (_, b) = right.try_recv().unwrap();
    let old_read = engine.read(&table, &key).unwrap().unwrap();
    for (index, (_, original)) in input.cells.iter().enumerate() {
        let original_bytes = original.value.as_ref().unwrap();
        for view in [&a.rows[0], &b.rows[0], &old_read.rows[0]] {
            let actual = view.cells[index].1.value.as_ref().unwrap();
            assert_eq!(actual.as_slice(), original_bytes.as_slice());
            assert!(
                std::ptr::eq(actual.as_ptr(), original_bytes.as_ptr()),
                "cell payload copied between input, observers and memtable read"
            );
        }
    }

    let mut newer = row(4, timestamp + 1);
    for (_, cell) in &mut newer.cells {
        *cell = CellValue::live(vec![b'y'; 64], timestamp + 1);
    }
    engine
        .write(&table, &key, newer.clone(), timestamp + 1)
        .unwrap();
    let latest = engine.read(&table, &key).unwrap().unwrap();
    assert_eq!(latest.rows.as_slice(), std::slice::from_ref(&newer));
    for old in [&a.rows[0], &b.rows[0], &old_read.rows[0]] {
        assert_eq!(old, &input, "a later merge mutated an earlier view");
    }
}
