//! T-038: the file-backed writer's Data.db path (`DataSink::Stream` /
//! `ChunkCompressor`) must not allocate on the write hot path after open —
//! RE5 (a row far larger than the pump's segment, streamed without an
//! allocation sized to the row) and the packet's "zero allocations after
//! open, compressed and uncompressed, at 10x the estimated chunk count" bar.
//!
//! Same counting `#[global_allocator]` pattern as `pump_sync_alloc.rs`/
//! `pump_async_alloc.rs`, extended the same way `pump_async_alloc.rs`
//! extends it: the async (`depth >= 1`, the default) pump runs a real
//! background flusher OS thread, and the compressed path additionally runs
//! chunk compression on `compression_pool()`'s rayon worker threads — a
//! thread-local counter (the pattern `compress_into_alloc.rs` uses for a
//! single-threaded measurement) would silently miss allocations on either of
//! those threads. So this file uses one **process-wide** atomic counter, and
//! serializes its tests behind `MEASURE_LOCK` so they don't attribute one
//! another's background-thread activity to the wrong measurement window —
//! `pump_async_alloc.rs` accepts that risk for two tests in one file; this
//! file has four times two compression codecs, so it removes the risk
//! instead of hoping for lucky scheduling.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::compression::Compression;
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

/// Serializes every test's measured window against every other's — see the
/// module doc comment. Held for the whole test, not just the measured
/// region, so one test's warm-up can't overlap another's measurement either.
static MEASURE_LOCK: Mutex<()> = Mutex::new(());

fn single_value_header() -> SerializationHeader {
    SerializationHeader {
        min_timestamp: 0,
        min_local_deletion_time: i32::MAX,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_types: vec![],
        static_columns: vec![],
        regular_columns: vec![(
            b"v".to_vec(),
            "org.apache.cassandra.db.marshal.BytesType".into(),
        )],
        complex_collections: false,
    }
}

fn one_value_partition(key: &[u8], value: Vec<u8>) -> Partition {
    Partition {
        key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(value, 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }],
    }
}

fn ordered_partitions(prefix: &str, count: usize, value: &[u8]) -> Vec<Partition> {
    let mut partitions = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("{prefix}{i:05}");
        partitions.push(one_value_partition(key.as_bytes(), value.to_vec()));
    }
    partitions.sort_by_key(|partition| partition.key.token);
    assert!(
        partitions
            .windows(2)
            .all(|pair| pair[0].key.token < pair[1].key.token),
        "generated test keys must have distinct tokens"
    );
    partitions
}

/// RE5: a single 64 MiB row streams through the pump (default segment =
/// 1 MiB — `PumpConfig::DEFAULT_SEGMENT_BYTES`) without any allocation
/// scaled to the row's size. Uncompressed (`compression: None`): no
/// `ChunkCompressor`, no `compression_pool()` involvement, so this is the
/// pump/`DataSink` write path alone.
#[test]
fn data_sink_alloc_re5_64mib_row_through_1mib_pump_no_row_sized_allocation() {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let staging_dir = dir.path().join("staging");
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 65536,
        verify_output: false, // the readback verify itself allocates; RE5 is about the write path
    };
    let mut writer = SSTableWriter::new_file_backed(
        options,
        single_value_header(),
        staging_dir.join("Data.raw"),
    )
    .expect("new_file_backed");

    // Warm-up: primes the flusher thread, its channel plumbing, and any
    // lazily-initialized statics, with a small row first.
    writer
        .add_partition(&one_value_partition(b"warmup", vec![0u8; 64]))
        .expect("warmup add_partition");

    let big_value = vec![0xEFu8; 64 * 1024 * 1024];
    let big_partition = one_value_partition(b"zzz-big", big_value);

    let before = alloc_events();
    writer
        .add_partition(&big_partition)
        .expect("add_partition for the 64 MiB row");
    let delta = alloc_events() - before;

    assert_eq!(
        delta, 0,
        "writing a single 64 MiB row through a 1 MiB pump cost {delta} allocations — \
         row encoding (T-037) and the pump/DataSink write path (T-038) must not allocate \
         anything sized to the row"
    );

    let _ = writer
        .finish_to_directory(&staging_dir)
        .expect("finish_to_directory");
}

/// Zero allocations after open, uncompressed, at 10x an "estimated" chunk
/// count: many separate `add_partition` calls (not one wide row — RE5 covers
/// that shape) totalling far more than a small table's worth of data.
#[test]
fn data_sink_alloc_zero_after_open_uncompressed_10x_chunk_estimate() {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let staging_dir = dir.path().join("staging");
    let value = vec![0x11u8; 4096];
    let partitions = ordered_partitions("p", 1008, &value);
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 4096,
        verify_output: false,
    };
    let mut writer = SSTableWriter::new_file_backed(
        options,
        single_value_header(),
        staging_dir.join("Data.raw"),
    )
    .expect("new_file_backed");

    for partition in &partitions[..8] {
        writer
            .add_partition(partition)
            .expect("warmup add_partition");
    }
    std::thread::yield_now();

    // A "chunk estimate" of 100 (4096-byte chunks -> ~400 KiB table), so 10x
    // is ~1000 partitions of one 4096-byte value each.
    let before = alloc_events();
    for partition in &partitions[8..] {
        writer.add_partition(partition).expect("add_partition");
    }
    let delta = alloc_events() - before;
    assert_eq!(
        delta, 0,
        "1000 more uncompressed partitions after warm-up cost {delta} allocations — \
         the pump/DataSink write path must not allocate in steady state"
    );

    let _ = writer
        .finish_to_directory(&staging_dir)
        .expect("finish_to_directory");
}

/// Same claim, compressed: `ChunkCompressor`'s `inputs`/`outputs` are
/// allocated once at open and `compress_into` (T-036, plus T-038's LZ4
/// `CompressTable` reuse) is zero-alloc after its own per-thread warm-up, so
/// this should reach the same zero bar — not merely "near zero" — once every
/// `compression_pool()` worker thread has been touched once.
fn assert_zero_after_open_compressed(compression: Compression) {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let staging_dir = dir.path().join("staging");
    let value = vec![0x22u8; 4096];
    let partitions = ordered_partitions("p", 1064, &value);
    let options = WriteOptions {
        compression: Some(compression),
        bloom_fp_chance: 0.01,
        chunk_size: 4096,
        verify_output: false,
    };
    let mut writer = SSTableWriter::new_file_backed(
        options,
        single_value_header(),
        staging_dir.join("Data.raw"),
    )
    .expect("new_file_backed");

    // Enough warm-up batches (default batch_chunks = 16) to touch every
    // compression_pool() worker thread's own lazily-created table/context at
    // least once, and to cycle the flusher's segments once.
    for partition in &partitions[..64] {
        writer
            .add_partition(partition)
            .expect("warmup add_partition");
    }
    std::thread::yield_now();

    let before = alloc_events();
    for partition in &partitions[64..] {
        writer.add_partition(partition).expect("add_partition");
    }
    let delta = alloc_events() - before;
    assert_eq!(
        delta, 0,
        "1000 more compressed partitions after warm-up cost {delta} allocations — \
         ChunkCompressor's reused buffers and compress_into (T-036/T-038) must not \
         allocate in steady state"
    );

    let _ = writer
        .finish_to_directory(&staging_dir)
        .expect("finish_to_directory");
}

#[test]
fn data_sink_alloc_zero_after_open_compressed_lz4_10x_chunk_estimate() {
    assert_zero_after_open_compressed(Compression::Lz4);
}

#[test]
fn data_sink_alloc_zero_after_open_compressed_zstd_10x_chunk_estimate() {
    assert_zero_after_open_compressed(Compression::Zstd { level: 3 });
}
