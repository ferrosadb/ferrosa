//! T-037 RE3: row-body encoding must not allocate in steady state.
//!
//! Uses a counting `#[global_allocator]` (the only one in this test binary;
//! all three shapes run inside ONE `#[test]` function so a thread-unaware,
//! process-wide counter isn't corrupted by other tests running concurrently)
//! to measure whether serializing MORE rows into an already-warm
//! `SSTableWriter` costs any additional heap allocation.
//!
//! This drives `SSTableWriter::serialize_rows_for_test` (a `#[doc(hidden)]`
//! test seam over `serialize_partition`), not `add_partition`. `add_partition`
//! also inserts into the Partitions.db key trie and the bloom filter —
//! pre-existing code this packet does not touch, whose own cost is real
//! (tens of allocations per call) but not constant per call (confirmed with a
//! matched warm-up trajectory so two `add_partition` calls insert into
//! byte-identical trie states before diverging only in row count: costs were
//! still unequal, e.g. 64 vs 176 allocations for 2 vs 30 rows — that noise
//! floor is bigger than any row-level signal this test needs, so comparing
//! two `add_partition` calls cannot prove or disprove row-body encoding's
//! allocation behavior). `serialize_rows_for_test` isolates exactly the code
//! this packet changed, so the assertion below is a real, exact zero.
//!
//! This test is also what caught a real bug: `crate::marshal::collection_value_type`
//! (used once per complex-column run, per row) built a throwaway `Vec<&str>`
//! via `top_level_args` on every call. `encode_row_body` runs twice per row
//! (size pass, then write pass), so this doubled an existing allocation into
//! a two-per-row one. Fixed in `marshal.rs` by making `top_level_args` an
//! allocation-free iterator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

struct CountingAllocator;

static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn alloc_events() -> usize {
    ALLOC_EVENTS.load(Ordering::Relaxed)
}

const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";
const LONG: &str = "org.apache.cassandra.db.marshal.LongType";
const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const SET_INT: &str =
    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)";

/// `ROW_INDEX_MIN_ROWS` in `writer.rs` — every partition in this test stays
/// below it so the wide-partition Rows.db trie (out of this packet's scope)
/// never activates and cannot confound the measurement.
const ROW_INDEX_MIN_ROWS: usize = 32;

fn header(
    clustering_types: Vec<&str>,
    regular_types: Vec<&str>,
    complex_collections: bool,
) -> SerializationHeader {
    SerializationHeader {
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: UTF8.to_string(),
        clustering_types: clustering_types.into_iter().map(String::from).collect(),
        static_columns: Vec::new(),
        regular_columns: regular_types
            .into_iter()
            .enumerate()
            .map(|(i, t)| (format!("c{i}").into_bytes(), t.to_string()))
            .collect(),
        complex_collections,
    }
}

fn key(i: u32) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::from(format!("key-{i:08}").into_bytes()))
}

fn u16_prefixed(components: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in components {
        out.extend_from_slice(&(c.len() as u16).to_be_bytes());
        out.extend_from_slice(c);
    }
    out
}

/// Single-column (fixed-width Int32) clustering, two simple regular columns.
fn simple_row_single_ck(i: i32) -> Row {
    Row {
        clustering: i.to_be_bytes().to_vec(),
        cells: vec![
            (0, CellValue::live(b"v0".to_vec(), 1000)),
            (1, CellValue::live(b"v1".to_vec(), 1000)),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}

/// Multi-column (3-component, u16-prefixed) clustering, two simple regular
/// columns — exercises `split_u16_prefixed`.
fn simple_row_multi_ck(i: i32) -> Row {
    let a = i.to_be_bytes();
    let b = *b"mid";
    let c = (i as i64).to_be_bytes();
    Row {
        clustering: u16_prefixed(&[&a, &b, &c]),
        cells: vec![
            (0, CellValue::live(b"v0".to_vec(), 1000)),
            (1, CellValue::live(b"v1".to_vec(), 1000)),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}

/// Single-column clustering, one complex (`set<int>`) column whose 4 element
/// cells already arrive in cell-path order.
fn complex_row(i: i32) -> Row {
    let cells = (0..4i32)
        .map(|e| {
            (
                0u16,
                CellValue::live(Vec::new(), 1000).with_path(e.to_be_bytes().to_vec()),
            )
        })
        .collect();
    Row {
        clustering: i.to_be_bytes().to_vec(),
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}

fn partition(k: u32, rows: Vec<Row>) -> Partition {
    assert!(
        (rows.len()) < ROW_INDEX_MIN_ROWS,
        "keep this test off the Rows.db trie path"
    );
    Partition {
        key: key(k),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows,
    }
}

/// Warms up a writer with one partition, then asserts that serializing MORE
/// rows costs exactly zero further allocations.
fn assert_row_loop_is_alloc_free(hdr: SerializationHeader, make_row: impl Fn(i32) -> Row) {
    let dir = tempfile::tempdir().expect("tempdir");
    let options = WriteOptions {
        compression: None,
        ..WriteOptions::default()
    };
    let mut writer = SSTableWriter::new_file_backed(options, hdr, dir.path().join("raw-data.tmp"))
        .expect("new_file_backed");

    // Warm-up through the real `add_partition` (not the test-only seam): lets
    // `present_columns_scratch` / `complex_order_scratch` reach their
    // steady-state capacity and trips any one-time lazy statics under
    // exactly the code path production uses.
    let warmup = partition(0, (0..8i32).map(&make_row).collect());
    writer.add_partition(&warmup).expect("warmup add_partition");

    let more = partition(1, (0..30i32).map(&make_row).collect());
    let before = alloc_events();
    writer
        .serialize_rows_for_test(&more)
        .expect("serialize_rows_for_test");
    let delta = alloc_events() - before;

    assert_eq!(
        delta, 0,
        "serializing 30 more rows cost {delta} allocations — row-body encoding \
         is not allocation-free in steady state"
    );
}

// A single test function, not three — see the module doc for why.
#[test]
fn row_encode_alloc_row_body_is_allocation_free() {
    assert_row_loop_is_alloc_free(
        header(vec![INT32], vec![UTF8, UTF8], false),
        simple_row_single_ck,
    );
    assert_row_loop_is_alloc_free(
        header(vec![INT32, UTF8, LONG], vec![UTF8, UTF8], false),
        simple_row_multi_ck,
    );
    assert_row_loop_is_alloc_free(header(vec![INT32], vec![SET_INT], true), complex_row);
}
