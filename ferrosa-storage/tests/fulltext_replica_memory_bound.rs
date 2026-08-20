//! TDD guard (t_ee98faa0 layer 2 — the REPLICA-side fulltext-search OOM):
//! the REAL engine search (`StorageEngine::fulltext_search`) over a REAL
//! on-disk FTI sidecar with a query-derived `LIMIT k` must hold O(k)
//! additional memory, INDEPENDENT of how many documents match the term.
//!
//! Live failure this pins: a broad
//! `… context_snippet = fts_match('memory') LIMIT 10 ALLOW FILTERING` over a
//! large `agent_memory.entity_store` OOM-killed ALL THREE 2 GiB-capped
//! replicas at once. Each replica's `fulltext_search`:
//!   1. `std::fs::read` the whole sidecar,
//!   2. deserialized the ENTIRE index (every term, every posting) via
//!      `FullTextIndexReader::open` — O(index size),
//!   3. scored EVERY matching posting into an owned score map — O(matches),
//!   4. cloned the union into another engine-level map + sorted Vec —
//!      O(matches) again.
//!
//! The fix streams single-term queries straight off the sidecar file
//! (`ferrosa_index::fulltext::stream`) into a bounded top-k, so none of the
//! above is proportional to the index or match-set size.
//!
//! The 2 GiB node cap is a deliberate forcing function and is NEVER raised;
//! the bound is the query's own LIMIT — never a server-side cap.
//!
//! Modeled on `ferrosa-cluster/tests/replica_scan_serialization_memory_bound.rs`:
//! seeding, flush, and sidecar build all run OUTSIDE the measurement window;
//! only the search is measured. Hard budgets make a blow-up FAIL loudly
//! instead of OOM-ing the test process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

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
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so a free
            // of memory allocated BEFORE the window would drive the counter
            // negative and, because PEAK is a running maximum of LIVE, suppress
            // every later allocation. Seeding runs outside the window by design,
            // so how much of it is released inside the window is pure timing.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
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

const KS: &str = "agent_memory";
const TBL: &str = "entity_store";
const IDX: &str = "idx_snippet";

fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        // Large threshold: we flush explicitly, once, so exactly one SSTable +
        // one FTI sidecar exist and the memtable is empty during measurement.
        flush_threshold_bytes: u64::MAX,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    let schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "context_snippet".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    engine.register_table(schema).unwrap();
    engine
        .add_fulltext_index(&TableId::new(KS, TBL), IDX, 0)
        .unwrap();
    engine
}

/// Seed `n` rows whose snippet ALL contain the broad term "memory" (realistic
/// snippet sizes, ~15 tokens), then flush ONCE so a single FTI sidecar covers
/// them and the memtable is empty.
fn seed_and_flush(engine: &StorageEngine, n: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in 0..n {
        let key_bytes = format!("entity-{i:016}").into_bytes();
        let dk = DecoratedKey {
            token: Token(i as i64),
            key: PartitionKey::new(key_bytes),
        };
        let text = format!(
            "memory snippet {i} about durable typed agent knowledge graph \
             entity number {i} stored context"
        );
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(text.into_bytes(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &dk, row, 1000).unwrap();
    }
    engine.flush(&table_id).unwrap();
}

const K: usize = 10;
/// Hard per-search budget for a LIMIT-k replica search: covers the streaming
/// walker's buffers + O(k) hits + allocator jitter, and is tiny next to both
/// the O(index) deserialize and the O(matches) score map of the pre-fix shape.
const REPLICA_SEARCH_BUDGET_BYTES: i64 = 1024 * 1024; // 1 MiB, << 2 GiB cap

/// RED→GREEN for t_ee98faa0 layer 2: the replica-side engine search with the
/// query-derived LIMIT pushed down must hold a bounded working set,
/// INDEPENDENT of the matching-doc count. Pre-fix (whole-sidecar read +
/// full-index deserialize + score-everything) this FAILS: peak is O(index +
/// matches) and scales ~linearly with N.
#[test]
fn replica_limit_k_search_peak_is_bounded_independent_of_matching_docs() {
    const SMALL_N: usize = 2_000;
    const LARGE_N: usize = 32_000; // 16× more matching docs

    fn search_peak(n: usize) -> i64 {
        let dir = tempfile::tempdir().unwrap();
        let storage = engine(dir.path());
        seed_and_flush(&storage, n);
        let table_id = TableId::new(KS, TBL);
        // Sanity outside the window: the search is real and hits the sidecar.
        let warm = storage
            .fulltext_search(&table_id, IDX, "memory", Some(K))
            .unwrap();
        assert_eq!(warm.len(), K, "LIMIT-{K} search must return {K} doc keys");

        let (hits, peak) = measure_peak(|| {
            storage
                .fulltext_search(&table_id, IDX, "memory", Some(K))
                .unwrap()
        });
        assert_eq!(hits.len(), K);
        peak
    }

    let small = search_peak(SMALL_N);
    let large = search_peak(LARGE_N);
    eprintln!(
        "replica fulltext_search LIMIT-{K} peak: small(N={SMALL_N})={small} B, \
         large(N={LARGE_N})={large} B, ratio={:.2}; budget={REPLICA_SEARCH_BUDGET_BYTES} B",
        large as f64 / small.max(1) as f64,
    );

    // BOUNDED: the replica must not read/deserialize the whole index nor
    // materialize the match set for a LIMIT-k query.
    assert!(
        large < REPLICA_SEARCH_BUDGET_BYTES,
        "REGRESSION (t_ee98faa0 layer 2): replica LIMIT-{K} fts search peak {large} B exceeds \
         the {REPLICA_SEARCH_BUDGET_BYTES} B budget at N={LARGE_N} matching docs. At the \
         intentional 2 GiB node cap this OOM-kills every replica at once on a broad fts_match."
    );
    // INDEPENDENT OF N: 16× more matching docs must NOT cost ~16× more memory.
    assert!(
        large < small.max(65536) * 3,
        "REGRESSION (t_ee98faa0 layer 2): replica LIMIT-{K} fts search peak scales with the \
         match set — small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B ({}× more \
         matching docs). Peak must be O(k), not O(index + matches).",
        LARGE_N / SMALL_N,
    );
}

/// Completeness guard: with NO LIMIT the complete match set is returned —
/// the memory fix must never truncate a no-LIMIT result server-side
/// (no-server-side-limits principle).
#[test]
fn no_limit_search_still_returns_complete_match_set() {
    const N: usize = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, N);
    let table_id = TableId::new(KS, TBL);

    let all = storage
        .fulltext_search(&table_id, IDX, "memory", None)
        .unwrap();
    assert_eq!(
        all.len(),
        N,
        "no-LIMIT fts search must return EVERY matching doc key (complete result)"
    );

    // And the LIMIT path returns exactly k of them (a subset).
    let some = storage
        .fulltext_search(&table_id, IDX, "memory", Some(7))
        .unwrap();
    assert_eq!(some.len(), 7);
    let full: std::collections::HashSet<_> = all.into_iter().collect();
    for key in &some {
        assert!(
            full.contains(key),
            "top-k keys must come from the match set"
        );
    }
}
