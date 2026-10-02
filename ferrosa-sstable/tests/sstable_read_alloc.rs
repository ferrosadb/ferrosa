//! Allocation-count guard for the SSTable READ path.
//!
//! Uses a counting `#[global_allocator]` (the only one in this test binary; ALL
//! measured shapes run inside ONE `#[test]` so a thread-unaware, process-wide
//! counter isn't corrupted by other tests running concurrently — see the
//! `row_encode_alloc.rs` docstring, which is the model for this file).
//!
//! Measured shape: decode `P` partitions × `R` rows through the real streaming
//! reader (`partitions_iter` + `next_partition`). The warm-up primes every
//! one-time lazy static and the reader's own bounded caches; the steady-state
//! delta is what the read loop itself allocates.
//!
//! This is the read-path counterpart to `row_encode_alloc.rs`: the target is
//! ZERO steady-state allocations per decoded row. `read_row` built a
//! `Vec<(usize, bool, String)>` (`col_meta`) per row — one `String` per column,
//! per row — which is O(columns) allocations per row on the hot scan path.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
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
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, old, new_size) }
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

const KS: &str = "org.apache.cassandra.db.marshal.BytesType";
const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";

/// Two simple regular columns, single Int32 clustering column.
fn header() -> SerializationHeader {
    SerializationHeader {
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: KS.to_string(),
        clustering_types: vec![INT32.to_string()],
        static_columns: Vec::new(),
        regular_columns: vec![
            (b"c0".to_vec(), UTF8.to_string()),
            (b"c1".to_vec(), UTF8.to_string()),
        ],
        complex_collections: false,
    }
}

fn row(i: i32) -> Row {
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

fn partition(idx: usize, rows: usize) -> Partition {
    Partition {
        key: DecoratedKey::new(PartitionKey::new(format!("pk-{idx:016}").into_bytes())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: (0..rows as i32).map(row).collect(),
    }
}

fn build_and_open(n: usize, rows: usize) -> (tempfile::TempDir, SSTableReader<FileReadAt>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let staging = dir.path().join("staging");
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 16 * 1024,
        verify_output: false,
    };
    let mut writer = SSTableWriter::new_file_backed(options, header(), staging.join("Data.raw"))
        .expect("writer");
    let mut parts: Vec<Partition> = (0..n).map(|i| partition(i, rows)).collect();
    parts.sort_by_key(|p| p.key.token);
    for p in &parts {
        writer.add_partition(p).expect("add_partition");
    }
    let files = writer.finish_to_directory(&staging).expect("finish");
    let components = SSTableComponents {
        data: FileReadAt::open(&files.data).expect("data"),
        partitions: FileReadAt::open(&files.partitions).expect("partitions"),
        rows: FileReadAt::open(&files.rows).expect("rows"),
        filter: std::fs::read(&files.filter).expect("filter"),
        compression_info: None,
        statistics: std::fs::read(&files.statistics).expect("stats"),
    };
    let reader = SSTableReader::open(components).expect("open");
    (dir, reader)
}

/// Scan every partition through the streaming reader and count allocations.
fn scan_allocs(reader: &SSTableReader<FileReadAt>) -> usize {
    let before = alloc_events();
    let mut iter = reader.partitions_iter().expect("iter");
    while iter.next_partition().expect("next").is_some() {}
    alloc_events() - before
}

/// ONE test fn — see module doc. The steady-state read loop must not allocate
/// per row: only the caller-visible `Vec<Row>`/`Vec<CellValue>` payloads (the
/// rows themselves) may allocate, and those are the return value, not waste.
#[test]
fn sstable_scan_steady_state_allocation_count() {
    const PARTITIONS: usize = 24;
    const ROWS: usize = 16;

    let (dir, reader) = build_and_open(PARTITIONS, ROWS);

    // Warm-up: primes the chunk cache and any lazily-built reader caches.
    let warm = scan_allocs(&reader);
    let steady = scan_allocs(&reader);

    let rows_scanned = PARTITIONS * ROWS;
    eprintln!(
        "SSTABLE_SCAN_ALLOC partitions={PARTITIONS} rows_per_partition={ROWS} \
         rows_scanned={rows_scanned} warmup_allocs={warm} steady_allocs={steady} \
         allocs_per_row={:.3} dir={}",
        steady as f64 / rows_scanned as f64,
        dir.path().display()
    );

    // Measured (this fixture: 2 simple columns, Int32 clustering):
    //   3552 allocs / 384 rows = 9.250 per row   (original)
    //   2400 allocs / 384 rows = 6.250 per row   (per-row `col_meta` table removed)
    //   2016 allocs / 384 rows = 5.250 per row   (per-row `present_columns` Vec removed)
    // — an exact 4-alloc/row drop across the two fixes.
    //
    // The remainder is the honest return payload: each decoded row owns a
    // clustering `Vec<u8>`, a `Vec<(u16, CellValue)>` of 2 cells, and two cell
    // value `Vec<u8>`s. Those are *constructed* by the decoder, so moving does
    // not remove them; only a caller-owned/pooled decode could, which is a
    // separate API change.
    //
    // Guard budget: 6/row, tightened so it catches EITHER regression — the
    // `col_meta` table (+3/row -> 8.25) and the `present_columns` Vec (+1/row ->
    // 6.25) each trip it, while the current 5.25 clears it.
    const BUDGET_PER_ROW: usize = 6;
    let budget = rows_scanned * BUDGET_PER_ROW;
    assert!(
        steady <= budget,
        "REGRESSION: scan allocated {steady} times for {rows_scanned} rows ({:.3}/row), \
         over the {BUDGET_PER_ROW}/row budget ({budget}). `read_row`/`read_partition_projected` \
         must not build a per-row `col_meta` `Vec<(usize, bool, String)>` (one `String` per \
         column per row).",
        steady as f64 / rows_scanned as f64
    );

    let _ = dir;
}
