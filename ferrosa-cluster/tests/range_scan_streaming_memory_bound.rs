//! TDD red guard: cluster-mode full-range scans (`SELECT … ALLOW FILTERING`,
//! `COUNT(*)`) MUST stream, not materialize the whole local range into a `Vec`.
//!
//! Regression (this weekend, #229/#228): in cluster mode the coordinated range
//! read `coordinate_range_read_stream_all_with` calls
//! `read_local_range_stream_limited_rows`, which collects up to `limit`
//! partitions into `Vec::with_capacity(limit)` (`range_read_stream.rs:232`) and
//! returns the whole `Vec` before the "stream" yields anything. For a large
//! table (e.g. `entity_store`, 269k rows) that `Vec` is gigabytes — the 2 GiB
//! node OOM-kills (`exit 137`), restarts, re-scans, OOMs again: an unrecoverable
//! crash loop observed on the dev cluster.
//!
//! The single-node path (`local_range_stream`) wraps `range_iter` lazily and is
//! bounded; the cluster path must do the same (move-based, one partition in
//! flight — never a `Vec`). This test seeds N partitions and asserts the
//! streaming scan's peak heap is ~independent of N — locking in the COUNT-path
//! fix (PR #230). The SELECT degraded arm + the data-loss regression test land
//! in the follow-up (forge task t_a243e406).
//!
//! Modeled on `ferrosa-storage/tests/recovery_oom_memory_bound.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::StreamExt;

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
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
// This file has a SINGLE test, so nothing else flips `ARMED` concurrently and
// `PEAK` captures the peak *additional* bytes held at once during the window.
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

/// Measure peak *additional* heap bytes held at once during `f`.
fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const KS: &str = "test_ks";
const TBL: &str = "test_tbl";
const ROW_BYTES: usize = 4096;

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
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
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
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    engine.register_table(schema).unwrap();
    engine
}

/// Seed `n` distinct single-row partitions, each carrying a `ROW_BYTES` value.
fn seed(engine: &StorageEngine, n: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in 0..n {
        let key_bytes = format!("pk-{i:08}").into_bytes();
        let dk = DecoratedKey {
            token: Token(i as i64),
            key: PartitionKey::new(key_bytes),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![b'x'; ROW_BYTES], 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &dk, row, 1000).unwrap();
    }
}

/// Single-node cluster `WritePath` (RF=1, CL=ONE → no remote fan-out, so the
/// scan returns straight after the materializing local read).
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

/// Peak heap while draining a cluster full-range scan, counting rows (never
/// holding the rows ourselves). If the cluster path streams, this is ~O(1)
/// partitions regardless of N; if it materializes, it is ~O(N).
fn cluster_scan_peak(n: usize) -> i64 {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed(&storage, n);
    let wp = cluster_write_path(storage);
    let table_id = TableId::new(KS, TBL);
    let strategy = ReplicationStrategy::Simple {
        replication_factor: 1,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (count, peak) = measure_peak(|| {
        rt.block_on(async {
            let mut stream = wp
                .range_read_stream_all_with(&table_id, 0, ConsistencyLevel::One, &strategy)
                .await
                .expect("range read stream");
            let mut rows = 0usize;
            while let Some(p) = stream.next().await {
                rows += p.expect("partition").rows.len();
            }
            rows
        })
    });
    assert_eq!(count, n, "scan must return every seeded row (N={n})");
    peak
}

/// A cluster `ALLOW FILTERING` / full-range scan must hold a bounded working set
/// — peak heap independent of partition count. The materializing coordinator
/// (`read_local_range_stream_limited_rows` → `Vec::with_capacity(limit)`) makes
/// peak scale with N, which OOM-kills real nodes. This locks the streaming
/// contract in: scanning 16× more partitions must NOT cost ~16× more memory.
#[test]
fn cluster_full_scan_peak_is_independent_of_partition_count() {
    const SMALL_N: usize = 750;
    const LARGE_N: usize = 12_000; // 16× more, and PAST the 10_000 magic cap

    let small = cluster_scan_peak(SMALL_N);
    let large = cluster_scan_peak(LARGE_N);
    eprintln!(
        "cluster_scan_peak: small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B, \
         ratio={:.2} (row_bytes={ROW_BYTES})",
        large as f64 / small.max(1) as f64,
    );

    // Streaming: peak is a bounded working set, so 16× the partitions costs only
    // a small constant factor more (slack covers buffers / merge bookkeeping).
    // Materializing: peak ≈ N × row size, so `large` ≈ 16× `small` and this
    // fails — exactly the OOM regression.
    assert!(
        large < small * 3,
        "REGRESSION: cluster full-scan peak heap scales with partition count — \
         {SMALL_N} parts: {small} B, {LARGE_N} parts ({}× more): {large} B. \
         The coordinated range read is materializing the whole local range into a \
         Vec instead of streaming partition-at-a-time.",
        LARGE_N / SMALL_N,
    );
}

// NOTE: the data-loss half of this bug — `range_read_limited_rows` silently
// truncating at DEFAULT_RANGE_READ_LIMIT (10_000) on the SELECT degraded scan
// arm — is intentionally NOT tested here. Its regression test ships with the
// follow-up PR that removes the cap from scan paths (forge task t_a243e406),
// where it goes green. Adding it now would be a known-failing test (this repo's
// CI runs #[ignore] tests in the cluster-gated job).
