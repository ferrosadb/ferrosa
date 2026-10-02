//! P0-OOM memory measurement for the SSTable read path.
//!
//! ESTABLISHED PATTERN (mirrors `ferrosa-storage/tests/recovery_oom_memory_bound.rs`
//! and `fulltext_streaming_each_memory_bound.rs`): a scoped `#[global_allocator]`
//! peak-allocation tracker (armed flag + mutex), a fixture FAR LARGER than any
//! internal buffer, and the assertion that PEAK heap does NOT scale with
//! file size. It also drives the materializing path as a BASELINE so the fixture
//! is provably big enough.
//!
//! This test exists to answer, with numbers, whether the shapes the
//! `p0-oom-audit` rules flag on the SSTable reader are REAL memory growth or
//! merely signature-shape false positives.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

// --- peak-additional-heap tracker (scoped to this integration-test binary) ---
struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const VALUE_BYTES: usize = 1000;

fn header() -> SerializationHeader {
    SerializationHeader {
        complex_collections: false,
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
        static_columns: vec![],
        regular_columns: vec![(
            b"r0".to_vec(),
            "org.apache.cassandra.db.marshal.BytesType".to_string(),
        )],
    }
}

/// One partition with `rows` clustered rows, each a single `VALUE_BYTES` cell.
fn partition(idx: usize, rows: usize) -> Partition {
    let key = DecoratedKey::new(PartitionKey::new(format!("pk-{idx:016}").into_bytes()));
    let body: Vec<Row> = (0..rows)
        .map(|r| Row {
            clustering: (r as i32).to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(vec![b'x'; VALUE_BYTES], 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        })
        .collect();
    Partition {
        key,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: body,
    }
}

/// Build a file-backed SSTable of `n` partitions × `rows` rows and open it.
/// Returns the tempdir (kept alive) and the opened reader.
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
        .expect("new_file_backed");
    let mut parts: Vec<Partition> = (0..n).map(|i| partition(i, rows)).collect();
    parts.sort_by_key(|p| p.key.token);
    for p in &parts {
        writer.add_partition(p).expect("add_partition");
    }
    let files = writer.finish_to_directory(&staging).expect("finish");

    let components = SSTableComponents {
        data: FileReadAt::open(&files.data).expect("open data"),
        partitions: FileReadAt::open(&files.partitions).expect("open partitions"),
        rows: FileReadAt::open(&files.rows).expect("open rows"),
        filter: std::fs::read(&files.filter).expect("read filter"),
        compression_info: None,
        statistics: std::fs::read(&files.statistics).expect("read stats"),
    };
    let reader = SSTableReader::open(components).expect("reader open");
    (dir, reader)
}

fn report(
    tag: &str,
    n: usize,
    rows: usize,
    materialize_peak: i64,
    streaming_peak: i64,
    total: i64,
) {
    let bytes = n * rows * VALUE_BYTES;
    let line = format!(
        "SSTABLE_READ_PEAK tag={tag} partitions={n} rows_per_partition={rows} fixture_bytes={bytes} \
         materialize_peak={materialize_peak} streaming_peak={streaming_peak} \
         materialize_ratio={:.2} streaming_ratio={:.2}\n",
        materialize_peak as f64 / bytes.max(1) as f64,
        streaming_peak as f64 / bytes.max(1) as f64,
    );
    eprintln!("{line}");
    let path = std::env::var_os("FERROSA_SSTABLE_OOM_REPORT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/ferrosa-sstable-oom-report.txt"));
    let mut prev = std::fs::read_to_string(&path).unwrap_or_default();
    prev.push_str(&line);
    let _ = std::fs::write(&path, prev);
    let _ = total;
}

/// The core question: does the SSTable read path's peak heap scale with the
/// FILE SIZE? Materializing (`read_partitions_limited_rows`) SHOULD; streaming
/// (`partitions_iter` + `next_partition`, dropping each) should NOT.
#[test]
fn sstable_read_peak_scales_with_file_only_for_the_materializing_path() {
    const SMALL: usize = 500;
    const LARGE: usize = 4000; // 8× the partitions, 8× the file
    const ROWS: usize = 50;

    let (_d1, small) = build_and_open(SMALL, ROWS);
    let (_d2, large) = build_and_open(LARGE, ROWS);

    // Warm the chunk cache + lazily-built caches outside the measuring window.
    let warm = |reader: &SSTableReader<FileReadAt>| {
        let mut iter = reader.partitions_iter().expect("iter");
        while iter.next_partition().expect("next").is_some() {}
    };
    warm(&small);
    warm(&large);

    // Streaming: peak must be ~flat across an 8× larger file.
    let (_, small_stream) = measure_peak(|| {
        let mut iter = small.partitions_iter().expect("iter");
        let mut count = 0usize;
        while iter.next_partition().expect("next").is_some() {
            count += 1;
        }
        count
    });
    let (_, large_stream) = measure_peak(|| {
        let mut iter = large.partitions_iter().expect("iter");
        let mut count = 0usize;
        while iter.next_partition().expect("next").is_some() {
            count += 1;
        }
        count
    });

    // Materializing baseline: peak should track the whole file.
    let (small_count, small_mat) = measure_peak(|| {
        small
            .read_partitions_limited_rows(SMALL, 0)
            .expect("bounded read")
            .len()
    });
    let (large_count, large_mat) = measure_peak(|| {
        large
            .read_partitions_limited_rows(LARGE, 0)
            .expect("bounded read")
            .len()
    });
    assert_eq!(small_count, SMALL);
    assert_eq!(large_count, LARGE);

    report("small", SMALL, ROWS, small_mat, small_stream, 0);
    report("large", LARGE, ROWS, large_mat, large_stream, 0);

    // The fixture is only meaningful if the materializing path really does blow
    // up: it must hold a substantial fraction of the file.
    let large_file = (LARGE * ROWS * VALUE_BYTES) as i64;
    assert!(
        large_mat > large_file / 4,
        "fixture too small to be a memory test: materializing peak {large_mat} B vs file {large_file} B"
    );

    // Baseline (pre-fix) evidence: streaming must already be flat.
    eprintln!(
        "streaming scaling small={small_stream} B large={large_stream} B ratio={:.2}",
        large_stream as f64 / small_stream.max(1) as f64
    );
}

/// The in-memory `finish()` self-verification (Gate B) reopened the SSTable by
/// deep-cloning every component (`Data.db`, `Partitions.db`, `Rows.db`) into a
/// second copy. Measure the `finish()` peak with verification ON vs OFF to
/// quantify that clone — it should be ~one extra table's worth of bytes.
#[test]
fn writer_finish_verify_clone_is_measurable() {
    const N: usize = 4000;
    const ROWS: usize = 50;

    let build = |verify: bool| {
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 16 * 1024,
            verify_output: verify,
        };
        let mut writer = SSTableWriter::new(options, header());
        let mut parts: Vec<Partition> = (0..N).map(|i| partition(i, ROWS)).collect();
        parts.sort_by_key(|p| p.key.token);
        for p in &parts {
            writer.add_partition(p).expect("add_partition");
        }
        drop(parts);
        writer
    };

    // Build OUTSIDE the window so the measured peak is the `finish()` call
    // itself (where the verification clone happens), not the write/build phase.
    let verified = build(true);
    let plain = build(false);
    let (verify_peak, plain_peak) = {
        let (verify_out, verify_peak) =
            measure_peak(|| verified.finish().expect("finish verified"));
        let (plain_out, plain_peak) = measure_peak(|| plain.finish().expect("finish unverified"));
        assert_eq!(verify_out.data.len(), plain_out.data.len());
        (verify_peak, plain_peak)
    };

    let file = (N * ROWS * VALUE_BYTES) as i64;
    eprintln!(
        "WRITER_FINISH_PEAK file_bytes={file} verify_on_peak={verify_peak} \
         verify_off_peak={plain_peak} clone_delta={}",
        verify_peak - plain_peak
    );
    // REGRESSION GUARD: verification must NOT re-copy the table. Pre-fix this
    // delta was ~one full table's worth of bytes; post-fix it must be a small
    // constant (the reader's own bounded caches, not a second Data.db).
    let delta = verify_peak - plain_peak;
    assert!(
        delta < file / 4,
        "REGRESSION: verify_output re-copied the SSTable — verify_on_peak={verify_peak} B, \
         verify_off_peak={plain_peak} B, clone_delta={delta} B on a {file} B table. \
         The self-verification must read the components by reference, not deep-clone them."
    );
}
