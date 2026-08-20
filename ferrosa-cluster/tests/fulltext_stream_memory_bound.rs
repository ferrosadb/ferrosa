//! TDD guard (t_4ae47a9f, e2e): a broad no-`LIMIT` `fts_match` through the
//! CLUSTER write path must stream its matching keys with a working set that
//! is (a) complete, (b) independent of DOCUMENT size, and (c) inside a hard
//! per-key budget.
//!
//! Live failure this pins (t_8fc24ce2): the legacy union path held the match
//! set several times over — every replica's `score_map` (key + f64 score),
//! the up-to-256 MiB bincode response frame, the coordinator's `seen` +
//! `all_keys`, and the router's `matched` — and the compound-query arm
//! `fs::read` the WHOLE FTI sidecar, so coordinator memory scaled with both
//! the match count AND the indexed document bytes. Nodes at the intentional
//! 2 GiB cap OOM-killed in cascade.
//!
//! The streaming path's ONLY O(distinct matches) allocation is the deduped
//! key set itself (keys only — no scores, no extra copies, no row/doc
//! bytes), so:
//!   * doubling document size must NOT move the peak (assertion 2), and
//!   * peak/key must fit a budget far below the multi-copy legacy shape
//!     (assertion 3).
//!
//! Scope note (measured 2026-07-16): in THIS RF=1 single-node harness the
//! legacy gate (`FERROSA_BULK_STREAMING_FULLTEXT=0`) measures ~187 B/key vs
//! the stream's ~155 B/key — the earlier t_ee98faa0 layers already bound the
//! node-LOCAL single-term path, and the legacy blowup's remaining copies
//! (response frames, cross-replica unions) only exist with real remote
//! replicas. So this test guards completeness/wiring/dedup, doc-size
//! independence, the per-key backstop, and early-drop teardown; the sharp
//! walk-level bound lives in
//! `ferrosa-storage/tests/fulltext_streaming_each_memory_bound.rs`, and the
//! multi-node validation is the live fly.io reproduction (t_4ae47a9f
//! verification gate).
//!
//! Modeled on `range_scan_streaming_memory_bound.rs` (same single-node
//! cluster WritePath harness — RF=1 exercises the full
//! `coordinate_fulltext_search_stream` feeder/merge/dedup machinery with no
//! remote fan-out) and `ferrosa-storage/tests/fulltext_replica_memory_bound.rs`
//! (same peak-additional-heap tracker; seeding and flush run OUTSIDE the
//! measurement window).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};

// --- peak-allocation tracker (scoped to this integration-test binary) ---
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
            // Clamp at zero. `measure_peak` zeroes LIVE at arm time, so a free
            // of memory allocated BEFORE the window would otherwise push the
            // counter negative — and because PEAK is a running maximum of LIVE,
            // every later allocation would be measured against that negative
            // baseline and the peak would collapse toward zero. Seeding runs
            // outside the window by design, so how much of it is released
            // inside the window is pure timing: that is what made these peaks
            // flaky across machines.
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

/// The tracker must report the working set of the measured region even when the
/// region frees memory that was allocated *before* the window opened.
///
/// `measure_peak` zeroes `LIVE` at arm time, but `dealloc` decrements it for
/// every free while armed — including frees of pre-window allocations. Those
/// frees drive `LIVE` negative, and since the peak is a running maximum of
/// `LIVE`, a later genuine allocation is measured against that negative
/// baseline and all but disappears.
///
/// This is what made `cluster_fulltext_stream_peak_bounded_and_doc_size_independent`
/// flaky. The seeding phase runs outside the window by design, so how much of
/// its memory happens to be released *inside* the window is pure timing. On a
/// GitHub runner the 64 B arm measured 85 587 B against the 4096 B arm's stable
/// 1 240 828 B — a 14.5x "doc size leak" that was really just a suppressed
/// baseline, failing a test whose subject had not regressed at all.
#[test]
fn peak_survives_frees_of_pre_window_allocations() {
    let pre_window = vec![0u8; 4 * 1024 * 1024];

    let (_, peak) = measure_peak(|| {
        // Released inside the window: must not lower the floor.
        drop(pre_window);
        let working_set = vec![0u8; 1024 * 1024];
        std::hint::black_box(working_set.len())
    });

    assert!(
        peak >= 1_000_000,
        "peak {peak} B lost the 1 MiB working set — a pre-window free drove the \
         live counter negative and suppressed the measurement"
    );
}

const KS: &str = "agent_memory";
const TBL: &str = "entity_store";
const IDX: &str = "idx_snippet";

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_disconnected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_suspected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
    fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
}

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
        // Flush explicitly, once: one SSTable + one FTI sidecar, empty
        // memtable inside the measurement window.
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

/// Seed `n` docs that ALL contain the broad term "memory", padding each
/// snippet to ~`doc_bytes` so the FTI sidecar's size scales with the
/// document payload while the doc KEYS stay identical across runs.
fn seed_and_flush(engine: &StorageEngine, n: usize, doc_bytes: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in 0..n {
        let key_bytes = format!("entity-{i:016}").into_bytes();
        let dk = DecoratedKey {
            token: Token(i as i64),
            key: PartitionKey::new(key_bytes),
        };
        // Distinct filler tokens per doc keep the index honest (real term
        // dictionary growth), padded to the requested payload size.
        let mut text = format!("memory snippet {i} durable typed agent entity {i}");
        while text.len() < doc_bytes {
            text.push_str(" filler");
            text.push_str(&(text.len() % 97).to_string());
        }
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

/// Single-node cluster `WritePath` (RF=1): exercises the full
/// `coordinate_fulltext_search_stream` feeder → merge → dedup machinery with
/// no remote fan-out.
fn cluster_write_path(storage: Arc<StorageEngine>) -> WritePath {
    let node_id = 1u64;
    let mut ring = TokenRing::new();
    ring.add_node(
        node_id,
        NodeInfo {
            host_id: uuid::Uuid::new_v4(),
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        },
    );
    ring.assign_tokens(node_id, &[i64::MIN, 0, i64::MAX]);
    let peers = Arc::new(PeerManager::new(
        Arc::new(NetConfig::default()),
        uuid::Uuid::new_v4(),
        Arc::new(NoopListener),
    ));
    let coordinator = ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        node_id,
        storage,
        1,
        ConsistencyLevel::One,
    );
    WritePath::cluster(Arc::new(coordinator))
}

/// Drain a cluster streaming fulltext search, counting distinct keys without
/// holding them; returns (distinct_keys, peak_additional_heap).
fn cluster_fts_stream_peak(n: usize, doc_bytes: usize) -> (usize, i64) {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, n, doc_bytes);
    let wp = cluster_write_path(storage);
    let table_id = TableId::new(KS, TBL);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Warm sanity pass OUTSIDE the window: the stream is real and complete.
    let warm: usize = rt.block_on(async {
        let mut rx = wp
            .fulltext_search_stream(&table_id, IDX, "memory")
            .await
            .expect("open fulltext stream");
        let mut count = 0usize;
        while let Some(item) = rx.recv().await {
            count += item.expect("stream batch").len();
        }
        count
    });
    assert_eq!(warm, n, "warm pass must deliver every matching doc key");

    measure_peak(|| {
        rt.block_on(async {
            let mut rx = wp
                .fulltext_search_stream(&table_id, IDX, "memory")
                .await
                .expect("open fulltext stream");
            let mut count = 0usize;
            while let Some(item) = rx.recv().await {
                count += item.expect("stream batch").len();
            }
            count
        })
    })
}

/// Per-key budget for the coordinator-side stream: the deduped key set
/// (~22-byte keys in `HashSet<Vec<u8>>`, measured ~155 B/key with allocator
/// overhead) plus bounded channels and one in-flight batch. A multi-copy
/// regression (scores + duplicate unions, ~3× and up) trips this backstop.
const PER_KEY_BUDGET_BYTES: i64 = 256;

/// (a) completeness at scale through the full cluster write path,
/// (b) peak independent of DOCUMENT bytes, (c) hard per-key budget.
#[test]
fn cluster_fulltext_stream_peak_bounded_and_doc_size_independent() {
    const N: usize = 8_000;
    const SMALL_DOC: usize = 64;
    const LARGE_DOC: usize = 4_096; // 64× the payload, identical keys

    let (count_small, peak_small) = cluster_fts_stream_peak(N, SMALL_DOC);
    let (count_large, peak_large) = cluster_fts_stream_peak(N, LARGE_DOC);
    eprintln!(
        "cluster fts stream peak: doc={SMALL_DOC}B -> {peak_small} B, \
         doc={LARGE_DOC}B -> {peak_large} B, ratio={:.2}; \
         budget={} B (N={N} keys x {PER_KEY_BUDGET_BYTES} B)",
        peak_large as f64 / peak_small.max(1) as f64,
        N as i64 * PER_KEY_BUDGET_BYTES,
    );

    assert_eq!(count_small, N, "every matching key, exactly once (dedup)");
    assert_eq!(count_large, N, "every matching key, exactly once (dedup)");

    // (b) Document payload must not reach the key stream: 64× bigger docs
    // may not move the peak beyond allocator noise. The legacy compound arm
    // read the ENTIRE sidecar (O(index bytes)) and every arm carried scores;
    // both scale with doc payload and fail this.
    assert!(
        peak_large < peak_small.max(256 * 1024) * 2,
        "REGRESSION (t_4ae47a9f e2e): fulltext stream peak scales with document \
         size — {SMALL_DOC}B docs: {peak_small} B vs {LARGE_DOC}B docs: {peak_large} B. \
         Document/index bytes are leaking into the key-streaming path."
    );

    // (c) Hard budget: keys-only, one deduped copy, bounded plumbing.
    let budget = N as i64 * PER_KEY_BUDGET_BYTES;
    assert!(
        peak_large < budget,
        "REGRESSION (t_4ae47a9f e2e): fulltext stream peak {peak_large} B exceeds \
         the {budget} B keys-only budget (N={N} × {PER_KEY_BUDGET_BYTES} B). The \
         multi-copy legacy union (scores + response frames + duplicate sets) is back."
    );
}

/// Dropping the receiver early must tear the stream down promptly (feeders
/// observe closed channels and stop) — the consumer-paced cancel contract.
/// Guards against a regression where an abandoned no-LIMIT search keeps
/// walking and buffering in the background.
#[test]
fn cluster_fulltext_stream_early_drop_stops_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed_and_flush(&storage, 20_000, 64);
    let wp = cluster_write_path(storage);
    let table_id = TableId::new(KS, TBL);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let mut rx = wp
            .fulltext_search_stream(&table_id, IDX, "memory")
            .await
            .expect("open fulltext stream");
        // Take one batch, then abandon the stream.
        let first = rx.recv().await.expect("at least one batch");
        assert!(!first.expect("stream batch").is_empty());
        drop(rx);
        // The runtime must quiesce: give spawned feeders a beat to observe
        // the closed channels. A hang here (walk still running unbounded)
        // fails via the test harness timeout.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });
}
