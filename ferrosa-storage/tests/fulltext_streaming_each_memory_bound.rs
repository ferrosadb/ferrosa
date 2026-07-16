//! TDD guard (t_8fc24ce2 / t_4ae47a9f layer 2b — the NO-LIMIT replica-side
//! fulltext OOM): `StorageEngine::fulltext_search_each` must hand matching doc
//! keys to a callback one at a time with a bounded working set, INDEPENDENT of
//! the matching-doc count — even when the query carries NO LIMIT.
//!
//! Live failure this pins: `fulltext_search(.., None)` (the broad `fts_match`
//! shape, reached directly or via the router's geometric LIMIT escalation)
//! scored EVERY matching posting into an owned `score_map: HashMap<Vec<u8>,
//! f64>` on every replica at once — O(matches) per replica — which OOM-killed
//! all three 2 GiB-capped nodes (t_8fc24ce2). The LIMIT-k path was already
//! bounded (t_ee98faa0 layer 2); this file closes the `limit=None` hole by
//! pinning the streaming callback primitive the cluster-level windowed
//! producer will drive.
//!
//! Contract pinned here:
//!   * every matching key is delivered exactly as `fulltext_search(.., None)`
//!     would return it (parity, modulo cross-source duplicates — the caller
//!     dedups);
//!   * `ControlFlow::Break` from the callback halts the walk immediately
//!     (consumer-paced backpressure);
//!   * peak additional heap during the walk is bounded and independent of N.
//!
//! Modeled on `fulltext_replica_memory_bound.rs` (same engine harness and
//! peak-additional-heap tracker; seeding/flush happen OUTSIDE the window).

use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::ControlFlow;
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
            LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
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

/// Hard per-walk budget: covers the streaming walker's buffers + one key in
/// flight + allocator jitter. Deliberately tiny next to the O(matches)
/// score-map of the pre-fix `limit=None` shape.
const STREAMING_WALK_BUDGET_BYTES: i64 = 1024 * 1024; // 1 MiB, << 2 GiB cap

/// RED→GREEN for layer 2b: a NO-LIMIT single-term walk over the real engine
/// (real FTI sidecar on disk) must hold a bounded working set independent of
/// the matching-doc count. The consumer here counts and discards — the shape
/// of a caller forwarding into a bounded channel whose receiver keeps up.
#[test]
fn no_limit_streaming_walk_peak_is_bounded_independent_of_matches() {
    const SMALL_N: usize = 2_000;
    const LARGE_N: usize = 32_000; // 16× more matching docs

    fn walk_peak(n: usize) -> i64 {
        let dir = tempfile::tempdir().unwrap();
        let storage = engine(dir.path());
        seed_and_flush(&storage, n);
        let table_id = TableId::new(KS, TBL);

        // Sanity outside the window: the walk is real and complete.
        let mut warm = 0usize;
        storage
            .fulltext_search_each(&table_id, IDX, "memory", &mut |_key: Vec<u8>| {
                warm += 1;
                ControlFlow::Continue(())
            })
            .unwrap();
        assert_eq!(warm, n, "no-LIMIT walk must visit EVERY matching doc");

        let (count, peak) = measure_peak(|| {
            let mut count = 0usize;
            storage
                .fulltext_search_each(&table_id, IDX, "memory", &mut |_key: Vec<u8>| {
                    count += 1;
                    ControlFlow::Continue(())
                })
                .unwrap();
            count
        });
        assert_eq!(count, n);
        peak
    }

    let small = walk_peak(SMALL_N);
    let large = walk_peak(LARGE_N);
    eprintln!(
        "no-LIMIT fulltext_search_each peak: small(N={SMALL_N})={small} B, \
         large(N={LARGE_N})={large} B, ratio={:.2}; budget={STREAMING_WALK_BUDGET_BYTES} B",
        large as f64 / small.max(1) as f64,
    );

    assert!(
        large < STREAMING_WALK_BUDGET_BYTES,
        "REGRESSION (t_4ae47a9f layer 2b): no-LIMIT streaming walk peak {large} B exceeds the \
         {STREAMING_WALK_BUDGET_BYTES} B budget at N={LARGE_N} matching docs. This is the exact \
         shape that OOM-killed every replica at the 2 GiB cap on a broad fts_match."
    );
    assert!(
        large < small.max(65536) * 3,
        "REGRESSION (t_4ae47a9f layer 2b): streaming walk peak scales with the match set — \
         small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B ({}× more matching docs). \
         Peak must be O(1), not O(matches).",
        LARGE_N / SMALL_N,
    );
}

/// Parity: the callback walk delivers exactly the key set of
/// `fulltext_search(.., None)` (deduped — cross-source duplicates are the
/// documented caller's concern).
#[test]
fn streaming_walk_matches_materializing_search_key_set() {
    const N: usize = 1_500;
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, N);
    let table_id = TableId::new(KS, TBL);

    let expected: std::collections::HashSet<Vec<u8>> = storage
        .fulltext_search(&table_id, IDX, "memory", None)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(expected.len(), N);

    let mut streamed: std::collections::HashSet<Vec<u8>> = Default::default();
    storage
        .fulltext_search_each(&table_id, IDX, "memory", &mut |key: Vec<u8>| {
            streamed.insert(key);
            ControlFlow::Continue(())
        })
        .unwrap();

    assert_eq!(
        streamed, expected,
        "same key set as the materializing search"
    );
}

/// Compound (multi-term) queries fall back to the existing bounded evaluation
/// internally but must still deliver the complete parity key set through the
/// callback — callers get ONE code path regardless of query shape.
#[test]
fn streaming_walk_compound_query_parity() {
    const N: usize = 800;
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, N);
    let table_id = TableId::new(KS, TBL);

    let q = "memory AND snippet";
    let expected: std::collections::HashSet<Vec<u8>> = storage
        .fulltext_search(&table_id, IDX, q, None)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(expected.len(), N, "every doc contains both terms");

    let mut streamed: std::collections::HashSet<Vec<u8>> = Default::default();
    storage
        .fulltext_search_each(&table_id, IDX, q, &mut |key: Vec<u8>| {
            streamed.insert(key);
            ControlFlow::Continue(())
        })
        .unwrap();
    assert_eq!(streamed, expected);
}

/// `ControlFlow::Break` from the callback halts the walk immediately — the
/// consumer-paced backpressure hook (dropped downstream receiver, satisfied
/// page/LIMIT). No further callbacks may arrive after a Break.
#[test]
fn streaming_walk_break_stops_immediately() {
    const N: usize = 500;
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, N);
    let table_id = TableId::new(KS, TBL);

    let mut calls = 0usize;
    storage
        .fulltext_search_each(&table_id, IDX, "memory", &mut |_key: Vec<u8>| {
            calls += 1;
            ControlFlow::Break(())
        })
        .unwrap();
    assert_eq!(calls, 1, "walk must stop at the first Break");
}

/// Invalid query syntax fails loudly BEFORE any walk work — parity with
/// `fulltext_search` (fail loud, never fake an empty result).
#[test]
fn streaming_walk_invalid_query_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, 10);
    let table_id = TableId::new(KS, TBL);

    let mut calls = 0usize;
    let res = storage.fulltext_search_each(&table_id, IDX, "AND AND", &mut |_key: Vec<u8>| {
        calls += 1;
        ControlFlow::Continue(())
    });
    assert!(res.is_err(), "malformed fts query must be an error");
    assert_eq!(calls, 0, "callback must never fire for an invalid query");
}
