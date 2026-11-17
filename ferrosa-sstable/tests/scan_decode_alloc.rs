//! Storage.Streaming S3: a decoded chunk must be written into a caller-supplied
//! buffer rather than returned as a fresh Vec.
//! Last revised: 2026-11-17
//! Last changed: Pin the sequential scan path's reusable decode buffers.
//!
//! Lean row: specs/lean4/Ferrosa/Storage/Streaming.lean —
//!   "a decoded chunk is written into a caller-supplied buffer rather than
//!    returned as a new Vec" (.failing; witness
//!   Streaming.a_per_chunk_decoder_copies_the_whole_input).
//!
//! What this pins. The sequential scan path decodes every chunk into ONE pair of
//! buffers owned by the cursor (ScanBuffers { input, output } in reader.rs,
//! handed to read_compressed_chunk_into, which does
//! chunk.resize(ci.chunk_length, 0) + decompress_into on the caller's Vec).
//! So a full scan's allocation count must NOT follow the chunk count. Here the
//! same bytes are written twice — identical rows, 64 KiB chunks and 4 KiB
//! chunks — so the fixtures differ only in how many chunks that byte stream is
//! cut into. If the decode target were per-chunk (Compression::decompress's
//! Vec return shape), the 4 KiB scan would allocate once more for every extra
//! chunk and the steady-state delta would be ~chunk_count instead of ~0.
//!
//! Why scan_iter() and not partitions_iter(). PartitionIter::new calls
//! data_source(), which for a compressed table builds
//! ChunkedCompressedData — the point-read chunk CACHE, which must own an
//! Arc<Vec<u8>> per cold chunk by design (a hit returns an Arc clone). That
//! is correct behaviour, and measuring it would attribute a cache contract to
//! this row. scan_iter() is the constructor that builds
//! ReadSource::Scan(ScanData { buffers: ScanBuffers }) — the task-owned,
//! reusable buffers this row is about.
//!
//! Harness: tests/sstable_read_alloc.rs (same counting allocator, same
//! warm-up-then-steady discipline, ONE measured #[test] per binary).
//! Fixture builder: tests/scan_cursor.rs (SSTableWriter::new →
//! add_partition → finish → in-memory SSTableComponents).
//!
//! No absolute budget is pinned here. The assertion compares the two fixtures
//! and derives its slack from the measured chunk-count gap, so it can only fail
//! if per-chunk allocation actually exists. If you later want an absolute
//! per-scan budget, take it from two consecutive measured runs of this test —
//! do not guess one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::compression::Compression;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

// --- counting allocator ---
//
// Process-wide, like tests/sstable_read_alloc.rs: this integration test is its
// own binary and holds ONE measured #[test], so no other test's allocations
// can be attributed to it.

static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

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

// --- fixture: two regular columns, ~1 MiB of row data ---

const KS: &str = "org.apache.cassandra.db.marshal.BytesType";
const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";

/// Bytes of payload in the narrow column (c0).
const NARROW_VALUE: usize = 16;
/// Bytes of payload in the wide column (c1). With 8 rows per partition this
/// lands the Data.db at roughly 1 MiB of uncompressed row data.
const WIDE_VALUE: usize = 1000;

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

/// Deterministic, mildly varied payload — enough to keep LZ4 doing real work
/// without making the two fixtures differ in length. Built before any
/// measurement, so its allocations are never counted.
fn value(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|j| ((seed.wrapping_mul(31) + j * 7) % 251) as u8)
        .collect()
}

fn row(partition_idx: usize, i: i32) -> Row {
    let seed = partition_idx.wrapping_mul(97) + i as usize;
    Row {
        clustering: i.to_be_bytes().to_vec(),
        cells: vec![
            (0, CellValue::live(value(seed, NARROW_VALUE), 1000)),
            (1, CellValue::live(value(seed + 1, WIDE_VALUE), 1000)),
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
        rows: (0..rows as i32).map(|i| row(idx, i)).collect(),
    }
}

struct Fixture {
    reader: SSTableReader<Vec<u8>>,
    chunk_length: usize,
    data_length: u64,
    /// ceil(data_length / chunk_length), taken from the table's own
    /// CompressionInfo — never assumed from the requested chunk_size.
    chunks: u64,
}

fn build(chunk_size: usize, partitions: usize, rows_per_partition: usize) -> Fixture {
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        bloom_fp_chance: 0.01,
        chunk_size,
        verify_output: false,
    };

    let mut writer = SSTableWriter::new(options, header());
    let mut parts: Vec<Partition> = (0..partitions)
        .map(|i| partition(i, rows_per_partition))
        .collect();
    parts.sort_by_key(|p| p.key.token);
    for p in &parts {
        writer.add_partition(p).expect("add_partition");
    }
    let output = writer.finish().expect("finish");

    let components = SSTableComponents {
        data: output.data,
        partitions: output.partitions,
        rows: output.rows,
        filter: output.filter,
        compression_info: output.compression_info,
        statistics: output.statistics,
    };
    let reader = SSTableReader::open(components).expect("open");

    let ci = reader
        .compression_info()
        .expect("a compressed table must carry CompressionInfo");
    let (chunk_length, data_length) = (ci.chunk_length, ci.data_length);

    // Guard: the writer's buffer-fit rule (compression_buffers_fit) silently
    // falls back to Compression::DEFAULT_CHUNK_SIZE when the working set does
    // not fit, which would quietly collapse this test into comparing a fixture
    // against itself.
    assert_eq!(
        chunk_length, chunk_size,
        "writer did not honour chunk_size={chunk_size}; it fell back to {chunk_length}"
    );
    assert!(
        chunk_length > 0,
        "CompressionInfo reports a zero chunk length"
    );

    let chunks = data_length.div_ceil(chunk_length as u64);
    Fixture {
        reader,
        chunk_length,
        data_length,
        chunks,
    }
}

/// One full sequential scan through the reused-buffer path, returning
/// (allocations observed, partitions decoded).
fn scan_once(fixture: &Fixture) -> (usize, usize) {
    let before = alloc_events();
    let mut iter = fixture.reader.scan_iter().expect("scan_iter");
    let mut partitions = 0usize;
    while iter.next_partition().expect("next_partition").is_some() {
        partitions += 1;
    }
    (alloc_events() - before, partitions)
}

/// The Storage.Streaming S3 row, in the name the closing test must take
/// (red-ledger/src/lib.rs: the red's name is the name its closing test takes).
#[test]
fn a_decoded_chunk_is_written_into_a_caller_supplied_buffer_rather_than_returned_as_a_new_vec() {
    const LARGE_CHUNK: usize = 64 * 1024;
    const SMALL_CHUNK: usize = 4 * 1024;
    const PARTITIONS: usize = 128;
    const ROWS_PER_PARTITION: usize = 8;

    let large = build(LARGE_CHUNK, PARTITIONS, ROWS_PER_PARTITION);
    let small = build(SMALL_CHUNK, PARTITIONS, ROWS_PER_PARTITION);

    assert_eq!(
        large.data_length, small.data_length,
        "the two fixtures must hold byte-identical Data.db; only the chunking may differ"
    );
    assert!(
        small.chunks >= large.chunks * 4,
        "not enough chunk-count gap to measure against: large={} chunks, small={} chunks",
        large.chunks,
        small.chunks
    );

    // Warm-up is per reader, not global: each fixture has its own reader, so
    // each needs its own pass to prime the LZ4 thread-local table and the
    // reader's lazily-built caches. Only the second pass of each is compared.
    // A fresh scan_iter() allocates the two cursor buffers on first read, so
    // that constant cost is present in every measurement and cancels out.
    let (large_warm, large_seen) = scan_once(&large);
    let (large_steady, _) = scan_once(&large);
    let (small_warm, small_seen) = scan_once(&small);
    let (small_steady, _) = scan_once(&small);

    let rows = PARTITIONS * ROWS_PER_PARTITION;
    let chunk_delta = small.chunks - large.chunks;
    let steady_delta = small_steady as i64 - large_steady as i64;

    eprintln!(
        "STREAMING_S3_SCAN_DECODE_ALLOC data_length={} partitions={} rows={} \
         | large chunk={} chunks={} warm={} steady={} steady_per_row={:.3} \
         | small chunk={} chunks={} warm={} steady={} steady_per_row={:.3} \
         | steady_delta={} extra_chunks={}",
        large.data_length,
        PARTITIONS,
        rows,
        large.chunk_length,
        large.chunks,
        large_warm,
        large_steady,
        large_steady as f64 / rows as f64,
        small.chunk_length,
        small.chunks,
        small_warm,
        small_steady,
        small_steady as f64 / rows as f64,
        steady_delta,
        chunk_delta,
    );

    assert_eq!(
        large_seen, PARTITIONS,
        "large-chunk fixture did not decode every partition"
    );
    assert_eq!(
        small_seen, PARTITIONS,
        "small-chunk fixture did not decode every partition"
    );

    // Derived bound, not a guessed budget. S3 says a chunk decode allocates
    // nothing of its own, so the chunk_delta extra chunks may not cost one
    // allocation each. A quarter of them is slack for constant per-scan
    // overhead and still four times tighter than the smallest regression this
    // test exists to catch (one fresh Vec per chunk).
    let slack = (chunk_delta / 4).max(4) as usize;
    assert!(
        small_steady <= large_steady + slack,
        "REGRESSION (Storage.Streaming S3): scanning identical bytes in 4 KiB chunks took \
         {small_steady} allocations against {large_steady} in 64 KiB chunks — delta {steady_delta} \
         across {chunk_delta} extra chunks (slack {slack}). A decoded chunk must be written into \
         the cursor's own buffer (ScanBuffers::output, ScanData::read_at), not into a fresh \
         Vec per chunk."
    );
}
