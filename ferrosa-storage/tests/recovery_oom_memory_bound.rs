//! P0 regression guard: startup/self-heal SSTable validation must stream.
//!
//! The startup smoke test (`StorageEngine::smoke_test_generation`, also run by
//! the periodic self-heal corruption scan) validates every unverified SSTable
//! generation. It once materialized each whole SSTable into a `Vec<Partition>`
//! (`read_all_partitions`). On a large or corrupt SSTable that materialization
//! could exhaust the node's 2 GiB cgroup cap, get OOM-killed, restart, re-read
//! the SAME SSTable, and OOM again — an unrecoverable crash loop. This was
//! observed in the dev cluster: `fmem-dev-node1` `OOMKilled=true`,
//! `RestartCount=5`, which took the cluster's CQL endpoint (and the forge task
//! board on :19042) down until the restart budget was exhausted.
//!
//! The fix streams validation one partition at a time. This test locks that in:
//! peak heap during `smoke_test_generation` must stay far below the cost of
//! materializing the whole SSTable. If recovery ever materializes again, peak
//! memory scales with partition count and this test fails loudly — long before
//! it can OOM a real node.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_sstable::{SSTableComponents, SSTableReader};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
//
// Each integration test file is its own binary, so this `#[global_allocator]`
// affects nothing else in the workspace. `alloc`/`dealloc` touch only atomics
// and `System`, never the heap, so there is no reentrancy. Tracking is gated by
// `ARMED` and this file deliberately contains a single test, so no other thread
// flips the flag concurrently. `LIVE` is `i64` and may dip below zero when
// allocations made before arming are freed during the window — that is fine,
// `PEAK` only ever grows on allocation and captures the peak *additional* bytes
// held at once during the measured call.

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

/// Run `f` with peak-allocation tracking armed; return its result and the peak
/// number of additional live bytes observed during the call.
fn measure_peak<T>(f: impl FnOnce() -> T) -> (T, i64) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (out, PEAK.load(Ordering::Relaxed))
}

const VALUE_BYTES: usize = 16 * 1024; // payload per partition
const SMALL_PARTITIONS: usize = 64; // ~1 MiB SSTable
const LARGE_PARTITIONS: usize = 1024; // 16x larger SSTable, same per-partition size

fn mem_test_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 8 * 1024 * 1024,
            max_segment_age: Duration::from_secs(3600),
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
        // No auto-flush: keep all partitions in the memtable so a single manual
        // flush produces exactly one generation holding all N partitions.
        flush_threshold_bytes: 256 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    }
}

fn single_column_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "big".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// Read SSTable component `{gen}-{comp}` for the single generation, searching
/// the table dir recursively (the engine may nest the generation in a subdir).
fn read_component(table_dir: &Path, gen_str: &str, comp: &str) -> Option<Vec<u8>> {
    let target = format!("{gen_str}-{comp}");
    let mut stack = vec![table_dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if entry.file_name().to_string_lossy() == target {
                return Some(std::fs::read(&path).expect("read SSTable component"));
            }
        }
    }
    None
}

/// Open a `Vec`-backed reader for the generation. Component bytes are read into
/// memory *before* the caller arms the tracker, so the backing buffers count as
/// baseline rather than as per-call allocation.
fn open_vec_reader(table_dir: &Path, gen_str: &str) -> SSTableReader<Vec<u8>> {
    let components = SSTableComponents {
        data: read_component(table_dir, gen_str, "Data.db").expect("Data.db"),
        partitions: read_component(table_dir, gen_str, "Partitions.db").expect("Partitions.db"),
        rows: read_component(table_dir, gen_str, "Rows.db").unwrap_or_default(),
        filter: read_component(table_dir, gen_str, "Filter.db").expect("Filter.db"),
        compression_info: read_component(table_dir, gen_str, "CompressionInfo.db"),
        statistics: read_component(table_dir, gen_str, "Statistics.db").expect("Statistics.db"),
    };
    SSTableReader::open(components).expect("open reconstructed SSTable reader")
}

fn big_row(seed: usize, ts: i64) -> Row {
    // Vary the bytes per partition so the payload is not trivially deduplicated.
    let value: Vec<u8> = (0..VALUE_BYTES).map(|j| (seed.wrapping_add(j)) as u8).collect();
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value, ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Peaks observed for one SSTable size: materializing every partition vs.
/// running the real recovery smoke test.
struct Peaks {
    materialize: i64,
    smoke: i64,
}

/// Build a single-generation SSTable with `n` partitions, then measure the peak
/// heap of (a) materializing all partitions and (b) the recovery smoke test.
fn measure_recovery_peaks(n: usize) -> Peaks {
    let dir = tempfile::tempdir().unwrap();
    let tid = TableId::new("ks", "big");

    {
        let engine = StorageEngine::new(mem_test_config(dir.path()), None).unwrap();
        engine.register_table(single_column_schema()).unwrap();
        for i in 0..n {
            let key = DecoratedKey::new(PartitionKey::new(format!("pk{i:06}").into_bytes()));
            let ts = 1_000 + i as i64;
            engine.write(&tid, &key, big_row(i, ts), ts).unwrap();
        }
        engine.flush(&tid).unwrap();
    }

    let table_dir = dir.path().join("sstables").join(tid.to_string());
    let gens = StorageEngine::list_generations_in_dir(&table_dir);
    assert_eq!(
        gens.len(),
        1,
        "a single manual flush must produce exactly one generation, got {gens:?}"
    );
    let gen = gens[0];
    let gen_str = gen.to_string();

    // Open once, before arming, so the reader's fixed structures (and the
    // backing Data buffer) are baseline and excluded from the per-call peak.
    let reader = open_vec_reader(&table_dir, &gen_str);

    // Baseline: materialize every partition — the OLD recovery behavior
    // (`read_all_partitions`) whose peak scales with partition count.
    let (materialized, materialize) = measure_peak(|| {
        reader
            .read_partitions_limited(usize::MAX)
            .expect("materialize all partitions")
    });
    assert_eq!(
        materialized.len(),
        n,
        "the fixture must hold every partition in one generation"
    );
    drop(materialized);

    // The actual recovery entry point (also run by the periodic self-heal scan).
    let (smoke_result, smoke) = measure_peak(|| StorageEngine::smoke_test_generation(&table_dir, gen));
    smoke_result.expect("a healthy SSTable must pass the startup smoke test");

    Peaks { materialize, smoke }
}

/// Recovery validation must cost roughly the same memory whether the SSTable
/// holds 64 partitions or 512. Materialization cost grows with partition count;
/// streaming cost is bounded by the decompression chunk cache (independent of
/// partition count). If recovery ever materializes again, its peak will scale
/// with SSTable size like the materialization baseline — and OOM a real node.
#[test]
fn recovery_smoke_test_memory_is_independent_of_sstable_size() {
    let small = measure_recovery_peaks(SMALL_PARTITIONS);
    let large = measure_recovery_peaks(LARGE_PARTITIONS);

    // Sanity: the materialization baseline must actually scale with the 16x
    // larger partition count, otherwise the comparison proves nothing.
    assert!(
        large.materialize > small.materialize * 4,
        "sanity: materializing {LARGE_PARTITIONS} partitions ({} B) should cost far more than \
         {SMALL_PARTITIONS} ({} B); fixture sizes are too close to be meaningful",
        large.materialize,
        small.materialize,
    );

    // Primary guard: on the large SSTable, recovery must hold only a small
    // fraction of what materializing the whole table would. If recovery
    // materializes, its peak ~= the materialization peak and this fails.
    assert!(
        large.smoke * 4 < large.materialize,
        "recovery smoke-test peak {} B is not far below materialization peak {} B on a \
         {LARGE_PARTITIONS}-partition SSTable — REGRESSION: startup validation is materializing \
         whole SSTables again, the OOM-crash-loop vector that took node1 down (OOMKilled, \
         RestartCount=5)",
        large.smoke,
        large.materialize,
    );

    // Shape guard: recovery cost stays ~flat as the SSTable grows 16x, while
    // materialization grew with it. A materializing recovery would scale like
    // the baseline (well past 3x), not stay bounded by the chunk cache.
    assert!(
        large.smoke < small.smoke * 3,
        "recovery smoke-test peak scaled with SSTable size ({} B for {SMALL_PARTITIONS} parts -> \
         {} B for {LARGE_PARTITIONS} parts) while materialization went {} B -> {} B — REGRESSION: \
         recovery memory now tracks SSTable size instead of staying bounded",
        small.smoke,
        large.smoke,
        small.materialize,
        large.materialize,
    );
}
