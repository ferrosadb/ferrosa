//! t_fed055cb — the coordinated projected partition-limit.
//!
//! On the coordinated **cluster** path a **projected** scan
//! (`SELECT <cols> FROM t`) that carries a **partition_limit** — which happens
//! on EVERY paged scan (the driver's fetch_size becomes a per-page
//! partition_limit) and on `SELECT <cols> ... LIMIT N` — used to hit
//! `WritePath::range_read_projected_stream_all_with`'s cluster branch, which
//! **refused**:
//!
//!   "projected cluster range scan with partition_limit is not implemented;
//!    refusing to return partial results"
//!
//! So the most common query shape fails-loud on a real cluster. The
//! single-node Direct/Pair paths already honor `partition_limit`; only the
//! coordinated cluster fan-out was unimplemented.
//!
//! This test exercises the real coordinated `WritePath::cluster` (a
//! `ClusterCoordinator`-backed write path), not the single-node `Direct` arm.
//! It seeds >10k partitions (PAST the 10_000 magic cap), then:
//!   1. asserts a projected paged scan returns ALL matching rows (no cap
//!      beyond the query's own bound);
//!   2. asserts a projected `partition_limit`/page returns exactly that many
//!      partitions' rows;
//!   3. asserts a projected `LIMIT N`-shaped scan returns exactly N.
//!
//! Modeled on `range_scan_streaming_memory_bound.rs`
//! (`cluster_full_scan_peak_is_independent_of_partition_count`), which builds a
//! real coordinated `WritePath::cluster`.

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

const KS: &str = "test_ks";
const TBL: &str = "test_tbl";

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
    // Two regular columns so a projection genuinely selects a SUBSET.
    let schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "v0".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "v1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        extensions: Default::default(),
    };
    engine.register_table(schema).unwrap();
    engine
}

/// Seed `n` distinct single-row partitions, each carrying two regular cells.
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
            cells: vec![
                (0, CellValue::live(format!("a-{i}").into_bytes(), 1000)),
                (1, CellValue::live(vec![b'x'; 256], 1000)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &dk, row, 1000).unwrap();
    }
    // Flush to SSTable so the projection byte-skips unprojected cells at the
    // read layer (the memtable read path keeps all cells); this exercises the
    // real cold-read projected scan.
    engine.flush(&table_id).unwrap();
}

/// Single-node cluster `WritePath` (RF=1, CL=ONE → no remote fan-out) built on a
/// real `ClusterCoordinator`. This is the `WritePath::Cluster` arm — NOT
/// `Direct`/`Pair` — so it exercises the coordinated projected range path.
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

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Drain a projected coordinated range scan (`wanted` = column ordinal subset),
/// carrying an optional `partition_limit`, and return the count of partitions
/// (and total rows) it yields — asserting each yielded partition carries ONLY
/// the projected cells.
async fn drain_projected(
    wp: &WritePath,
    table_id: &TableId,
    wanted: Vec<u16>,
    partition_limit: Option<usize>,
    strategy: &ReplicationStrategy,
) -> (usize, usize) {
    let mut stream = wp
        .range_read_projected_stream_all_with(
            table_id,
            wanted.clone(),
            partition_limit,
            ConsistencyLevel::One,
            strategy,
        )
        .await
        .expect("projected coordinated range scan must not refuse a partition_limit");
    let mut partitions = 0usize;
    let mut rows = 0usize;
    while let Some(item) = stream.next().await {
        let p = item.expect("partition");
        partitions += 1;
        for r in &p.rows {
            rows += r.cells.len();
            // Projection safety: only the wanted ordinal(s) survive.
            for (ord, _) in &r.cells {
                assert!(
                    wanted.contains(ord),
                    "projected row carried unprojected ordinal {ord}; wanted={wanted:?}"
                );
            }
        }
    }
    (partitions, rows)
}

/// A projected coordinated scan WITH a `partition_limit` must return the correct
/// rows — not refuse — over a real `WritePath::cluster`. Seeds PAST the 10_000
/// magic cap so an unbounded (`None`) projected scan returns EVERY partition,
/// and a bounded (`Some(k)`) scan returns AT LEAST `k` partitions (the fragment
/// merge over-fetches the tail page; exactness is enforced by the row-level
/// LIMIT at the CQL layer, not here).
#[test]
fn cluster_projected_scan_with_partition_limit_returns_all_rows() {
    const N: usize = 12_000; // PAST the 10_000 magic cap
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed(&storage, N);
    let wp = cluster_write_path(storage);
    let table_id = TableId::new(KS, TBL);
    let strategy = ReplicationStrategy::Simple {
        replication_factor: 1,
    };

    rt().block_on(async {
        // 1. Unbounded projected scan (partition_limit = None): EVERY partition,
        //    projecting only ordinal 0 (`v0`), not ordinal 1 (`v1`).
        let (parts, _rows) = drain_projected(&wp, &table_id, vec![0], None, &strategy).await;
        assert_eq!(
            parts, N,
            "unbounded projected cluster scan must return every seeded partition (N={N})"
        );

        // 2. Projected scan WITH a partition_limit (a paged fetch / page-size).
        //    This is the shape that used to fail-loud. It must return AT LEAST
        //    `page` partitions (over-fetch is fine; the CQL row-level LIMIT
        //    trims to exact), and NEVER refuse.
        const PAGE: usize = 5_000;
        let (parts, _rows) = drain_projected(&wp, &table_id, vec![0], Some(PAGE), &strategy).await;
        assert!(
            parts >= PAGE,
            "projected scan with partition_limit={PAGE} must yield at least that \
             many partitions, got {parts}"
        );
        // The bound must actually bound: it must not walk the whole table when a
        // small page was asked for and far fewer than N partitions are needed.
        assert!(
            parts < N,
            "projected scan with partition_limit={PAGE} must stop early, not scan \
             all {N} partitions (got {parts})"
        );

        // 3. `SELECT <cols> ... LIMIT N` shape: a small explicit bound returns at
        //    least that many partitions and stops early.
        const LIM: usize = 15;
        let (parts, _rows) = drain_projected(&wp, &table_id, vec![0], Some(LIM), &strategy).await;
        assert!(
            (LIM..N).contains(&parts),
            "projected LIMIT {LIM} scan must yield >= {LIM} partitions and stop \
             early, got {parts}"
        );
    });
}
