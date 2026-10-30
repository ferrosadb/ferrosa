//! Count live rows without copying resident cell payloads.
//! Correctness: payload size cannot change count allocation volume; keys and counts agree.
//! Last revised: 2026-10-27
//! Last changed: Add payload-size controls for ordinary and predicate counts.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};

struct CountingAllocator;
thread_local! {
    static BYTES: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
}

fn allocated(bytes: usize) {
    let _ = BYTES.try_with(|counter| {
        let (enabled, total) = counter.get();
        if enabled {
            counter.set((true, total + bytes));
        }
    });
}

// SAFETY: forward System's pointer/layout contract unchanged. Const TLS neither
// allocates nor borrows heap state; try_with tolerates thread teardown.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn sample(payload_bytes: usize, predicate: bool, columns: usize) -> usize {
    let directory = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(directory.path());
    config.flush_threshold_bytes = 64 * 1024 * 1024;
    config.commit_log.segment_size = 4 * 1024 * 1024;
    let engine = StorageEngine::new(config, None).unwrap();
    engine
        .register_table(TableSchema {
            keyspace: "count_payload".into(),
            table: "rows".into(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: (0..columns)
                .map(|i| ColumnDefinition {
                    name: format!("payload{i}"),
                    type_name: "org.apache.cassandra.db.marshal.BytesType".into(),
                })
                .collect(),
            extensions: Default::default(),
        })
        .unwrap();
    let table = TableId::new("count_payload", "rows");
    let timestamp = 1_800_000_000_000_000;
    for index in 0..32 {
        let key = DecoratedKey::new(PartitionKey::new(vec![index]));
        engine
            .write(
                &table,
                &key,
                Row {
                    clustering: vec![],
                    cells: (0..columns)
                        .map(|i| {
                            (
                                i as u16,
                                CellValue::live(vec![b'x'; payload_bytes], timestamp),
                            )
                        })
                        .collect(),
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                },
                timestamp,
            )
            .unwrap();
    }
    let count = || {
        if predicate {
            engine.count_range_matching(&table, None, None, &|key| key.key.as_bytes()[0] % 2 == 0)
        } else {
            engine.count_range(&table, None, None)
        }
        .unwrap()
    };
    let expected = if predicate { 16 } else { 32 };
    assert_eq!(count(), expected);
    BYTES.with(|counter| counter.set((true, 0)));
    let actual = count();
    let (_, bytes) = BYTES.with(|counter| counter.replace((false, 0)));
    assert_eq!(actual, expected);
    // Stored values must survive the count unchanged.
    let key = DecoratedKey::new(PartitionKey::new(vec![0]));
    let stored = engine.read(&table, &key).unwrap().unwrap();
    assert_eq!(
        stored.rows[0].cells[0].1.value.as_ref().unwrap().len(),
        payload_bytes
    );
    bytes
}

fn assert_payload_independent(predicate: bool) {
    let small = sample(32, predicate, 1);
    let large = sample(256 * 1024, predicate, 1);
    println!("COUNT_ALLOC predicate={predicate} small={small} large={large}");
    assert!(
        large <= small + 64 * 1024,
        "count copied cell payloads: small={small}, large={large}"
    );
}

#[test]
fn count_all_does_not_copy_active_cell_payloads() {
    assert_payload_independent(false);
}

#[test]
fn count_matching_does_not_copy_active_cell_payloads() {
    assert_payload_independent(true);
}

#[test]
fn count_all_does_not_clone_irrelevant_cell_metadata() {
    assert_columns_independent(false);
}

#[test]
fn count_matching_does_not_clone_irrelevant_cell_metadata() {
    assert_columns_independent(true);
}

fn assert_columns_independent(predicate: bool) {
    let narrow = sample(32, predicate, 1);
    let wide = sample(32, predicate, 256);
    println!("COUNT_COLUMNS predicate={predicate} narrow={narrow} wide={wide}");
    assert!(
        wide <= narrow + 64 * 1024,
        "count cloned cell metadata: narrow={narrow}, wide={wide}"
    );
}
