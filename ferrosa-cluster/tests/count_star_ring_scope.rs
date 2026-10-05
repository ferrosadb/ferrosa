//! P0 regression: `SELECT count(*)` must count the WHOLE ring, not the local
//! replica's owned subset.
//!
//! ## The defect
//!
//! `ClusterCoordinator::coordinate_range_count_matching` takes a local-only
//! shortcut when it believes the local node owns every token range:
//!
//! ```text
//! if self.range_read_remotes(cl, self.default_rf).needed == 0 {
//!     return self.storage.count_range_matching(table_id, None, None, matches);  // LOCAL ONLY
//! }
//! ```
//!
//! `self.default_rf` is NOT the table's replication factor. It is the MAX RF
//! across every user keyspace, frozen at cluster formation
//! (`resolve_formation_rf`, `controller/cluster.rs`). A cluster that contains
//! ANY keyspace with `RF == node_count` sets `default_rf == node_count`, and the
//! gate then concludes "the local node owns everything" for EVERY table —
//! including tables in a lower-RF keyspace, where the local node owns only its
//! share of the ring. `count(*)` then silently tallies that subset while a full
//! `SELECT` (which fans out and dedups by token) returns every row.
//!
//! This is forge `t_8c4e44e8` (2026-06, "observed {12,15,23} of 50 rows"). The
//! original fix (`74e42c3f`) reused the CL/RF fan-out decision but keyed it on
//! the cluster-wide `default_rf`, so it only holds when `RF >= node_count`; it
//! re-breaks whenever one keyspace sits at `RF == node_count` and another below
//! it. This test pins the lower-RF case that the cluster-max gate gets wrong.
//!
//! ## What this test proves
//!
//! Three REAL `RpcServer`s over 127.0.0.1 (coordinator + 2 replicas), each with
//! a real `StorageEngine` holding a DISJOINT third of the keyset — the
//! production topology, where a replica's local storage holds only the
//! partitions in its owned token ranges, never the whole table. The coordinator
//! is constructed exactly as production does for a 3-node cluster that has an
//! `RF=3` keyspace AND an `RF=1` keyspace: `default_rf = 3 == node_count`, with
//! `CL=ONE`. The count is issued for a table whose keyspace is `RF=1`
//! (SimpleStrategy) — the shape the gate mis-decides.
//!
//! RED (before the fix): the local-only shortcut runs, so `count(*)` returns
//! only the coordinator's own third — strictly fewer than the ring's total.
//! GREEN (after): the strategy-aware gate fans out across the replicas, dedups
//! by token, and returns every partition.
//!
//! `COUNT(*)` returning fewer rows than are present is silent truncation of a
//! result — the query answers a smaller table than it was asked for, with no
//! error. That is a data-correctness bug, not a performance one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::stream_frame_router::StreamFrameRouter;
use ferrosa_cluster::coordinator::stream_request_handler::{
    PeerManagerSinkFactory, RangeReadStreamRequestHandler,
};
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;

const KS: &str = "count_ring_ks";
const TBL: &str = "t";

/// This binary starts several real loopback servers; keep the tests in it from
/// contending on ports/timing the way the workspace serial-range-scan group does.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_disconnected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_suspected(&self, _peer: (uuid::Uuid, std::net::SocketAddr)) {}
    fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
    fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
}

fn net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

fn ring_node(host_id: uuid::Uuid, addr: &str) -> NodeInfo {
    NodeInfo {
        host_id,
        addr: addr.to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
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
        cache_hot_window_secs: 900,
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    let schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    engine.register_table(schema).unwrap();
    engine
}

fn partition(i: usize) -> Partition {
    let key_bytes = format!("pk-{i:08}").into_bytes();
    let dk = DecoratedKey::new(PartitionKey::new(key_bytes));
    Partition {
        key: dk,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(format!("v-{i}").into_bytes(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
    }
}

/// Seed `storage` with partitions `lo..hi` — a DISJOINT slice of the keyset, so
/// each node holds only its share and the union across the ring is the whole
/// table. Flushed to SSTables so the metadata-only count path is exercised.
fn seed_slice(storage: &StorageEngine, lo: usize, hi: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in lo..hi {
        let p = partition(i);
        storage
            .write(&table_id, &p.key, p.rows[0].clone(), 1000)
            .unwrap();
    }
    storage.flush(&table_id).unwrap();
}

async fn spawn_storage_replica(
    host_id: uuid::Uuid,
    dir: &std::path::Path,
    lo: usize,
    hi: usize,
) -> (Arc<RpcServer>, std::net::SocketAddr, Arc<PeerManager>) {
    let storage = engine(dir);
    seed_slice(&storage, lo, hi);
    let back = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        host_id,
        Arc::new(NoopListener),
    ));
    let sink_factory = Arc::new(PeerManagerSinkFactory::new(back.clone()));
    let handler = Arc::new(RangeReadStreamRequestHandler::new(
        Arc::new(storage),
        sink_factory,
        4,
    ));
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::RangeReadStreamRequest, handler.clone());
    registry.register(MsgType::RangeReadStreamCancel, handler);
    let server = Arc::new(RpcServer::new(net_config(), host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr, back)
}

/// `count(*)` over an `RF=1` keyspace must return every partition in the ring,
/// even though the coordinator's cluster-max `default_rf` equals `node_count`.
#[test]
fn count_star_counts_the_whole_ring_not_the_local_replica_subset() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        const N: usize = 120;
        let third = N / 3;

        let dir_local = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let dir3 = tempfile::tempdir().unwrap();

        // The coordinator's OWN storage also holds only its third.
        let storage = engine(dir_local.path());
        seed_slice(&storage, 0, third);

        let coord_id = uuid::Uuid::new_v4();
        let r2_id = uuid::Uuid::new_v4();
        let r3_id = uuid::Uuid::new_v4();

        let (srv2, addr2, back2) =
            spawn_storage_replica(r2_id, dir2.path(), third, 2 * third).await;
        let (srv3, addr3, back3) = spawn_storage_replica(r3_id, dir3.path(), 2 * third, N).await;

        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node(coord_id, "127.0.0.1:1"));
        ring.add_node(2, ring_node(r2_id, &addr2.to_string()));
        ring.add_node(3, ring_node(r3_id, &addr3.to_string()));
        ring.assign_tokens(1, &[i64::MIN]);
        ring.assign_tokens(2, &[0]);
        ring.assign_tokens(3, &[i64::MAX]);

        let peers = Arc::new(PeerManager::new(
            Arc::new(net_config()),
            coord_id,
            Arc::new(NoopListener),
        ));
        peers.ensure_peer(r2_id, &addr2.to_string()).await.unwrap();
        peers.ensure_peer(r3_id, &addr3.to_string()).await.unwrap();

        // Exactly the production shape that mis-decides: a 3-node cluster with
        // a default_rf of 3 (set by an RF=3 keyspace elsewhere), CL=ONE, asked
        // to count a table whose keyspace is RF=1.
        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            storage,
            3,
            ConsistencyLevel::One,
        ));

        let frame_router = Arc::new(StreamFrameRouter::new(coordinator.stream_router()));
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::RangeReadStreamChunk, frame_router.clone());
        registry.register(MsgType::RangeReadStreamHeartbeat, frame_router.clone());
        registry.register(MsgType::RangeReadStreamDone, frame_router.clone());
        let coord_srv = Arc::new(RpcServer::new(net_config(), coord_id, registry));
        let coord_addr = coord_srv.start_and_get_addr().await.unwrap();
        back2
            .ensure_peer(coord_id, &coord_addr.to_string())
            .await
            .unwrap();
        back3
            .ensure_peer(coord_id, &coord_addr.to_string())
            .await
            .unwrap();

        let wp = WritePath::cluster(coordinator);
        let table_id = TableId::new(KS, TBL);

        // The table's keyspace is RF=1 (SimpleStrategy) — the shape the
        // cluster-max gate mis-decides. This is the strategy the router derives
        // from the keyspace and now threads into the count path.
        let strategy = ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
            replication_factor: 1,
        };

        let count = wp
            .count_range_with(&table_id, ConsistencyLevel::One, &strategy)
            .await
            .expect("count_range_with must succeed");

        assert_eq!(
            count,
            N as u64,
            "SELECT count(*) returned {count} of {N} rows. The coordinator took \
             the local-only shortcut because its cluster-max default_rf (3) equals \
             the node count, so it counted only its own {} partitions and silently \
             dropped the other {}. A count(*) that understates the table is \
             truncation of a result (forge t_8c4e44e8): it must fan out across the \
             ring and dedup by token unless the TABLE's RF spans every node.",
            third,
            N - third
        );

        let _ = coord_srv.shutdown(Duration::from_millis(50)).await;
        srv2.shutdown(Duration::from_millis(50)).await;
        srv3.shutdown(Duration::from_millis(50)).await;
        drop((dir_local, dir2, dir3));
        let _ = Ordering::Relaxed;
    });
}
