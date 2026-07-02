//! t_dc729b1d + t_3fc6be3c — the multi-replica paged-scan stream lifecycle.
//!
//! Live fmem-dev signature (t_dc729b1d): EVERY multi-chunk internode stream
//! closed with `stream chunk sequence gap/reorder; closing stream route
//! expected_seq=0 observed_seq=5` on FRESH consecutive request_ids over a
//! single stable connection; paged reads stalled while COUNT/aggregates over
//! the same data (tight full drain, no page boundary) completed fine.
//!
//! Root cause (confirmed by `abandoned_route_stragglers_drop_silently_without_
//! phantom_close` in `stream_frame_router`): when the paging consumer abandons
//! a page's merged stream mid-flight, the forwarder unregisters the route but
//! the remote producer keeps streaming (no cancel was ever SENT — the frame
//! type existed but had no sender and no registered receiver-side MsgType).
//! The first in-order straggler was accepted against live seq-state and then
//! `route_frame`'s `NoRoute`/`ChannelClosed` handling CLEARED that state; the
//! next straggler hit `entry().or_default()` fabricating fresh `expected=0`
//! state → phantom gap-close, once per page, on every page. `pages × replicas`
//! uncancelled full-table producers then pile onto `Lane::Bulk`, starving live
//! streams (heartbeat-defunct connections, replica OOM on the live cluster).
//!
//! Repro harness: 3 real `RpcServer`s over 127.0.0.1 loopback (loopback is a
//! test convenience; production stays address-agnostic). node1 = coordinator,
//! node2 + node3 = replicas, RF=3, CL=ALL → 2 remotes → `expected_done == 2`.
//!
//! RED (before the fix):
//!  * `..._cancels_abandoned_producers` — abandoning a page leaves the parked
//!    replica producers alive; the live-producer counter never returns to 0.
//!  * `..._returns_all_rows_across_pages` — abandoned pages' stragglers
//!    phantom-close routes: the coordinator's `route_closures()` counter is
//!    non-zero (the exact live `expected=0 observed=5` storm, counted — not
//!    log-scraped).
//!
//! GREEN (after the fix): stragglers for torn-down routes drop silently
//! (`route_closures() == 0`), the forwarder fires `RangeReadStreamCancel` when
//! the consumer abandons a stream, every page arrives, and the loop terminates
//! under hard timeouts (a regression FAILS instead of wedging CI).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::StreamExt;
use tokio::sync::Notify;

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
    PartitionStream, PeerManagerSinkFactory, RangeReadStreamRequestHandler, StreamRangeReader,
};
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;

const KS: &str = "test_ks";
const TBL: &str = "test_tbl";
/// Clustered table for the wide-partition paging tests. Registered with a
/// real clustering column — the SSTable serialization header takes its
/// clustering component list from the schema, so rows written with clustering
/// bytes against a clustering-less schema would silently lose them on flush.
const TBL_WIDE: &str = "test_wide";

/// File-level serialization guard. Two tests in this binary mutate the process-
/// global `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT` env var to force a small
/// fragment (and thus many stream windows). Cargo runs integration-test fns
/// concurrently on threads in ONE process, so an env mutation in one test would
/// corrupt the fragment size another test reads mid-scan. Every test in this
/// file acquires this lock first, so they run serially and no test observes
/// another's env mutation. (These are heavy loopback-cluster tests; serial
/// execution is acceptable.) Poisoning is recovered — a panicking test still
/// releases a usable lock for the next.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_guard() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

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
    let wide_schema = TableSchema {
        keyspace: KS.to_string(),
        table: TBL_WIDE.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v0".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    engine.register_table(wide_schema).unwrap();
    engine
}

fn partition(i: usize) -> Partition {
    let key_bytes = format!("pk-{i:08}").into_bytes();
    // Real murmur3 token (via DecoratedKey::new) so token ordering is consistent
    // with a resume `start_key` the coordinator rebuilds from the raw key bytes.
    let dk = DecoratedKey::new(PartitionKey::new(key_bytes));
    Partition {
        key: dk,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(format!("a-{i}").into_bytes(), 1000)),
                (1, CellValue::live(vec![b'x'; 64], 1000)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
    }
}

fn seed_local(engine: &StorageEngine, n: usize) {
    let table_id = TableId::new(KS, TBL);
    for i in 0..n {
        let p = partition(i);
        engine
            .write(&table_id, &p.key, p.rows[0].clone(), 1000)
            .unwrap();
    }
    engine.flush(&table_id).unwrap();
}

/// Reader that emits a bounded PREFIX of partitions and then PARKS on a `Notify`
/// that is never fired — modelling a replica whose scan is in flight but not yet
/// exhausted when the coordinator abandons the page. It tracks how many reader
/// tasks are currently live via `live` (increment on poll-start, decrement on
/// drop). A correctly-cancelled producer drops this stream, so `live` returns to
/// 0; an uncancelled producer stays parked and `live` stays > 0.
struct ParkingReader {
    prefix: usize,
    live: Arc<AtomicUsize>,
    park: Arc<Notify>,
}

struct LiveGuard(Arc<AtomicUsize>);
impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StreamRangeReader for ParkingReader {
    fn range_iter<'a>(
        &'a self,
        _table_id: &TableId,
        _projected_regular_ordinals: Option<&'a [u16]>,
        start: Option<&'a DecoratedKey>,
    ) -> ferrosa_common::Result<PartitionStream<'a>> {
        let _ = start;
        let prefix = self.prefix;
        let live = self.live.clone();
        let park = self.park.clone();
        live.fetch_add(1, Ordering::SeqCst);
        let guard = LiveGuard(live);
        // Emit a `prefix`-length token-ORDERED run of partitions (the N-way merge
        // requires each source to be token-ordered), then PARK on the Notify
        // (never fired) so an UNCANCELLED producer stays alive forever. A
        // CANCELLED producer has this whole stream dropped, running `guard`'s Drop
        // (decrementing `live`). `guard` + `park` ride the unfold state so they
        // live exactly as long as the stream.
        let mut frags: Vec<Partition> = (0..prefix.max(1) * 4).map(partition).collect();
        frags.sort_by_key(|p| p.key.token.0);
        frags.truncate(prefix);
        let stream = futures::stream::unfold(
            (0usize, frags, guard, park),
            move |(i, frags, guard, park)| async move {
                if i < frags.len() {
                    let p = frags[i].clone();
                    Some((Ok(p), (i + 1, frags, guard, park)))
                } else {
                    // Scan not yet exhausted: park until the stream is dropped
                    // (cancel) — the Notify is never fired in this test.
                    park.notified().await;
                    drop(guard);
                    None
                }
            },
        );
        Ok(Box::pin(stream))
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

fn net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

/// A replica node: an `RpcServer` serving the streaming range-read handler backed
/// by a `ParkingReader`, plus a `PeerManager` used by its `PeerFireSink` to fire
/// chunk frames back to the coordinator.
async fn spawn_replica(
    host_id: uuid::Uuid,
    prefix: usize,
    live: Arc<AtomicUsize>,
    park: Arc<Notify>,
) -> (Arc<RpcServer>, std::net::SocketAddr, Arc<PeerManager>) {
    // Back-channel PeerManager: replica → coordinator (for chunk fires). It is
    // connected to the coordinator explicitly AFTER the coordinator binds, so
    // the first chunk fire never races an unconnected pool (which would drop the
    // chunk and stall the merge).
    let back = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        host_id,
        Arc::new(NoopListener),
    ));
    let sink_factory = Arc::new(PeerManagerSinkFactory::new(back.clone()));
    let reader = Arc::new(ParkingReader { prefix, live, park });
    // chunk_size = 1 so each emitted prefix partition flushes as its own chunk
    // immediately — the coordinator's merge sees remote data before the reader
    // parks (the handler otherwise buffers up to chunk_size partitions).
    let handler = Arc::new(RangeReadStreamRequestHandler::new(reader, sink_factory, 1));

    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::RangeReadStreamRequest, handler.clone());
    // Same handler serves the cancel so an abandoned page aborts the producer.
    registry.register(MsgType::RangeReadStreamCancel, handler);

    let server = Arc::new(RpcServer::new(net_config(), host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();

    (server, addr, back)
}

/// Wait until `live` observes `expected` live producer tasks (they start once the
/// coordinator's fan-out reaches the replicas).
async fn wait_live_at_least(live: &AtomicUsize, expected: usize) -> bool {
    for _ in 0..400 {
        if live.load(Ordering::SeqCst) >= expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// Wait until `live` drops back to 0 (all producers cancelled/finished).
async fn wait_live_zero(live: &AtomicUsize) -> bool {
    for _ in 0..400 {
        if live.load(Ordering::SeqCst) == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

#[test]
fn multi_replica_paged_projected_scan_cancels_abandoned_producers() {
    let _serial = serial_guard();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let storage = engine(dir.path());
        // Local replica holds the full dataset too (RF spans the ring).
        seed_local(&storage, 3);

        let coord_id = uuid::Uuid::new_v4();
        let r2_id = uuid::Uuid::new_v4();
        let r3_id = uuid::Uuid::new_v4();

        // Two live-producer counters + parks (one per replica). The two replicas
        // each emit a small prefix then park — models an in-flight scan.
        let live2 = Arc::new(AtomicUsize::new(0));
        let live3 = Arc::new(AtomicUsize::new(0));
        let park2 = Arc::new(Notify::new());
        let park3 = Arc::new(Notify::new());

        let (srv2, addr2, back2) = spawn_replica(r2_id, 4, live2.clone(), park2.clone()).await;
        let (srv3, addr3, back3) = spawn_replica(r3_id, 4, live3.clone(), park3.clone()).await;

        // Ring: 3 nodes, all own the whole ring so RF=3 spans it; CL=ALL → the
        // coordinator contacts BOTH remotes (`expected_done == 2`).
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node(coord_id, "127.0.0.1:1"));
        ring.add_node(2, ring_node(r2_id, &addr2.to_string()));
        ring.add_node(3, ring_node(r3_id, &addr3.to_string()));
        ring.assign_tokens(1, &[i64::MIN]);
        ring.assign_tokens(2, &[0]);
        ring.assign_tokens(3, &[i64::MAX]);

        // Coordinator PeerManager → replicas.
        let peers = Arc::new(PeerManager::new(
            Arc::new(net_config()),
            coord_id,
            Arc::new(NoopListener),
        ));
        peers.ensure_peer(r2_id, &addr2.to_string()).await.unwrap();
        peers.ensure_peer(r3_id, &addr3.to_string()).await.unwrap();

        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            storage,
            3,
            ConsistencyLevel::All,
        ));

        // Coordinator RpcServer: routes replica chunk/heartbeat/done frames back
        // into the coordinator's StreamRouter so its per-request receivers fill.
        let frame_router = Arc::new(StreamFrameRouter::new(coordinator.stream_router()));
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::RangeReadStreamChunk, frame_router.clone());
        registry.register(MsgType::RangeReadStreamHeartbeat, frame_router.clone());
        registry.register(MsgType::RangeReadStreamDone, frame_router.clone());
        let coord_srv = Arc::new(RpcServer::new(net_config(), coord_id, registry));
        let coord_addr = coord_srv.start_and_get_addr().await.unwrap();

        // Connect each replica's back-channel to the coordinator BEFORE the scan
        // so chunk fires never race an unconnected pool.
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
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        // Open page 1 of a projected paged scan over the REAL multi-replica
        // fan-out. `range_read_projected_stream_all_from` is the exact call
        // `SELECT <col>` (no WHERE/LIMIT/ORDER BY) makes for every page.
        let mut stream = tokio::time::timeout(
            Duration::from_secs(10),
            wp.range_read_projected_stream_all_from(
                &table_id,
                vec![0],
                None,
                ConsistencyLevel::All,
                &strategy,
            ),
        )
        .await
        .expect("opening page-1 multi-replica stream must not hang")
        .expect("multi-replica projected paged scan must not refuse");

        // Pull ONE partition (proves the coordinated fan-out is live and both
        // remotes started their producers), then ABANDON the page by dropping
        // the stream — exactly what the CQL paging collector does between pages.
        let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("pulling the first partition must not hang");
        assert!(first.is_some(), "page 1 must yield at least one partition");

        assert!(
            wait_live_at_least(&live2, 1).await && wait_live_at_least(&live3, 1).await,
            "both remote replica producers must start (expected_done == 2 fan-out)"
        );

        // Abandon the page.
        drop(stream);

        // The coordinator MUST cancel the abandoned remote producers. Without the
        // fix they stay parked forever and this wait times out (RED). With the
        // fix, RangeReadStreamCancel fires, each parked producer returns, and the
        // live-producer counters drop to 0 (GREEN).
        assert!(
            wait_live_zero(&live2).await,
            "replica 2 producer was not cancelled after the page was abandoned — \
             the coordinator leaked an uncancelled multi-replica scan (t_3fc6be3c HANG)"
        );
        assert!(
            wait_live_zero(&live3).await,
            "replica 3 producer was not cancelled after the page was abandoned — \
             the coordinator leaked an uncancelled multi-replica scan (t_3fc6be3c HANG)"
        );

        // The parked producers never streamed a gap; abandoning a page must not
        // register as a sequence-error route close (t_dc729b1d phantom close).
        assert_eq!(
            frame_router.route_closures(),
            0,
            "abandoning a page must not phantom-close stream routes"
        );

        srv2.shutdown(Duration::from_millis(50)).await;
        srv3.shutdown(Duration::from_millis(50)).await;
        coord_srv.shutdown(Duration::from_millis(50)).await;
    });
}

/// One clustered row for the wide-partition tests: clustering bytes are
/// byte-ordered (`ck-%06d`), matching the storage layer's raw-byte clustering
/// order, with one projected cell.
fn wide_row(j: usize) -> Row {
    Row {
        clustering: format!("ck-{j:06}").into_bytes(),
        cells: vec![(0, CellValue::live(format!("v-{j}").into_bytes(), 1000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}

/// Seed `pks.len()` WIDE partitions of `rows_per` clustered rows each, then
/// flush so the scan is SSTable-backed (the live `typed_edges` shape: a
/// handful of huge partitions).
fn seed_wide(engine: &StorageEngine, pks: &[&str], rows_per: usize) {
    let table_id = TableId::new(KS, TBL_WIDE);
    for pk in pks {
        let dk = DecoratedKey::new(PartitionKey::new(pk.as_bytes().to_vec()));
        for j in 0..rows_per {
            engine.write(&table_id, &dk, wide_row(j), 1000).unwrap();
        }
    }
    engine.flush(&table_id).unwrap();
}

/// Fully-wired 3-node loopback cluster (coordinator + 2 storage replicas) for
/// paged-scan tests. Every node is seeded identically via `seed` (RF=3 —
/// every replica owns the whole ring).
struct LoopbackCluster {
    wp: WritePath,
    frame_router: Arc<StreamFrameRouter>,
    srv2: Arc<RpcServer>,
    srv3: Arc<RpcServer>,
    coord_srv: Arc<RpcServer>,
    _dirs: Vec<tempfile::TempDir>,
}

impl LoopbackCluster {
    async fn shutdown(self) {
        self.srv2.shutdown(Duration::from_millis(50)).await;
        self.srv3.shutdown(Duration::from_millis(50)).await;
        self.coord_srv.shutdown(Duration::from_millis(50)).await;
    }
}

async fn build_loopback_cluster(seed: &dyn Fn(&StorageEngine)) -> LoopbackCluster {
    let dir_local = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let dir3 = tempfile::tempdir().unwrap();

    let storage = engine(dir_local.path());
    seed(&storage);

    let coord_id = uuid::Uuid::new_v4();
    let r2_id = uuid::Uuid::new_v4();
    let r3_id = uuid::Uuid::new_v4();

    let (srv2, addr2, back2) = spawn_storage_replica_seeded(r2_id, dir2.path(), seed).await;
    let (srv3, addr3, back3) = spawn_storage_replica_seeded(r3_id, dir3.path(), seed).await;

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

    let coordinator = Arc::new(ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        storage,
        3,
        ConsistencyLevel::All,
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

    LoopbackCluster {
        wp: WritePath::cluster(coordinator),
        frame_router,
        srv2,
        srv3,
        coord_srv,
        _dirs: vec![dir_local, dir2, dir3],
    }
}

/// Like [`build_loopback_cluster`] but seeds each of the three replicas
/// (coordinator-local, node2, node3) with a DIFFERENT partition subset — the
/// production topology where a replica's local storage holds only its owned
/// token ranges, NOT the whole keyset. The N-way merge must genuinely UNION
/// across all three sources for the scan to be complete; a source dropping out
/// mid-scan (a window-continuation that closes without a terminating Done) then
/// deletes that source's remaining rows from the result — the silent partial.
async fn build_loopback_cluster_split(
    seed_local_fn: &dyn Fn(&StorageEngine),
    seed_n2: &dyn Fn(&StorageEngine),
    seed_n3: &dyn Fn(&StorageEngine),
) -> LoopbackCluster {
    let dir_local = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let dir3 = tempfile::tempdir().unwrap();

    let storage = engine(dir_local.path());
    seed_local_fn(&storage);

    let coord_id = uuid::Uuid::new_v4();
    let r2_id = uuid::Uuid::new_v4();
    let r3_id = uuid::Uuid::new_v4();

    let (srv2, addr2, back2) = spawn_storage_replica_seeded(r2_id, dir2.path(), seed_n2).await;
    let (srv3, addr3, back3) = spawn_storage_replica_seeded(r3_id, dir3.path(), seed_n3).await;

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

    let coordinator = Arc::new(ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        storage,
        3,
        ConsistencyLevel::All,
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

    LoopbackCluster {
        wp: WritePath::cluster(coordinator),
        frame_router,
        srv2,
        srv3,
        coord_srv,
        _dirs: vec![dir_local, dir2, dir3],
    }
}

/// Page a coordinated projected scan to completion using EXACTLY the CQL
/// collector's cursor semantics (`collect_page_from_partition_stream`):
///
/// * a page collects up to `page_rows` clustered ROWS (not partitions);
/// * the continuation cursor is `(partition key bytes, clustering bytes)` of
///   the last accepted row, taken only when one MORE row proves a
///   continuation is needed;
/// * resume re-opens the scan at the cursor's partition key (INCLUSIVE lower
///   bound) and skips rows in that partition whose clustering is `<=` the
///   cursor clustering.
///
/// Returns every accepted `(pk, clustering)` in delivery order. Hard
/// timeouts: a stalled pull or a non-terminating cursor FAILS the test.
async fn page_rows_like_cql_collector(
    wp: &WritePath,
    table_id: &TableId,
    strategy: &ReplicationStrategy,
    page_rows: usize,
    max_pages: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut cursor: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(
            pages <= max_pages,
            "paged scan did not terminate within {max_pages} pages — the \
             paging cursor is cycling instead of advancing (t_a0f922a3 mode 1); \
             delivered {} rows so far",
            out.len()
        );

        let start = cursor
            .as_ref()
            .map(|(pk, ck)| ferrosa_cluster::write_path::ScanResume {
                key: DecoratedKey::new(PartitionKey::new(pk.clone())),
                clustering: Some(ck.clone()),
            });
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            wp.range_read_projected_stream_all_from(
                table_id,
                vec![0],
                start.as_ref(),
                ConsistencyLevel::All,
                strategy,
            ),
        )
        .await
        .expect("opening a page stream must not hang (t_a0f922a3 mode 2)")
        .expect("multi-replica projected paged scan must not refuse");

        let mut accepted = 0usize;
        let mut more = false;
        let mut page_cursor: Option<(Vec<u8>, Vec<u8>)> = None;
        'page: loop {
            let Some(item) = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .expect(
                    "a page pull must not hang — a stall is never acceptable (t_a0f922a3 mode 2)",
                )
            else {
                break; // stream exhausted — final page
            };
            let p = item.expect("partition");
            let pk = p.key.key.as_bytes().to_vec();
            for row in p.rows {
                // Skip rows already returned on a previous page (the cursor
                // partition is re-entered at partition start on resume).
                if let Some((cpk, cck)) = cursor.as_ref() {
                    if &pk == cpk && row.clustering.as_slice() <= cck.as_slice() {
                        continue;
                    }
                }
                if accepted == page_rows {
                    more = true;
                    break 'page;
                }
                out.push((pk.clone(), row.clustering.clone()));
                page_cursor = Some((pk.clone(), row.clustering));
                accepted += 1;
            }
        }
        drop(stream); // abandon the fan-out exactly like the CQL collector

        if !more {
            return out;
        }
        cursor = page_cursor;
    }
}

/// t_a0f922a3 mode 1 (CYCLE) on the REAL multi-replica path: wide partitions
/// spanning multiple pages must page to completion with an exact union. The
/// live 3-node cluster looped forever over ~10% of `typed_edges` (page starts
/// repeating with period ~3, `has_more_pages` never false) — a handful of
/// huge partitions, RF=3, paged projected scan.
#[test]
fn multi_replica_wide_partition_paged_scan_terminates_exactly() {
    let _serial = serial_guard();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        const ROWS_PER: usize = 4_000;
        const PAGE_ROWS: usize = 1_500;
        let pks: [&str; 3] = ["wpk-a", "wpk-b", "wpk-c"];

        let cluster = build_loopback_cluster(&|storage| seed_wide(storage, &pks, ROWS_PER)).await;
        let table_id = TableId::new(KS, TBL_WIDE);
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        let total = pks.len() * ROWS_PER;
        let max_pages = total.div_ceil(PAGE_ROWS) + 2;
        let delivered =
            page_rows_like_cql_collector(&cluster.wp, &table_id, &strategy, PAGE_ROWS, max_pages)
                .await;

        let expected: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> = pks
            .iter()
            .flat_map(|pk| {
                (0..ROWS_PER).map(|j| (pk.as_bytes().to_vec(), format!("ck-{j:06}").into_bytes()))
            })
            .collect();
        let got: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> =
            delivered.iter().cloned().collect();
        assert_eq!(
            got.len(),
            delivered.len(),
            "paged multi-replica wide-partition scan emitted duplicate rows"
        );
        assert_eq!(
            got,
            expected,
            "paged multi-replica wide-partition scan must deliver every row \
             exactly once (no gaps, no dupes); got {} of {}",
            got.len(),
            total
        );

        assert_eq!(
            cluster.frame_router.route_closures(),
            0,
            "paging must not phantom-close stream routes (t_dc729b1d)"
        );

        cluster.shutdown().await;
    });
}

/// t_a0f922a3 mode 2 (STALL) on the REAL multi-replica path: MANY SMALL
/// partitions (the live `vfix.nums` shape — 15k single-row partitions), a
/// no-LIMIT paged projected scan. Live, page 1 never returned (client
/// blocked over 14 min, zero route closes). Every pull runs under a hard
/// timeout so a stall FAILS; the union must be exact.
#[test]
fn multi_replica_many_small_partitions_paged_scan_completes() {
    let _serial = serial_guard();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        const N: usize = 15_000;
        const PAGE_ROWS: usize = 5_000;

        let cluster = build_loopback_cluster(&|storage| seed_local(storage, N)).await;
        let table_id = TableId::new(KS, TBL);
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        let max_pages = N.div_ceil(PAGE_ROWS) + 2;
        let delivered =
            page_rows_like_cql_collector(&cluster.wp, &table_id, &strategy, PAGE_ROWS, max_pages)
                .await;

        let expected: std::collections::BTreeSet<Vec<u8>> =
            (0..N).map(|i| format!("pk-{i:08}").into_bytes()).collect();
        let got: std::collections::BTreeSet<Vec<u8>> =
            delivered.iter().map(|(pk, _)| pk.clone()).collect();
        assert_eq!(
            got.len(),
            delivered.len(),
            "paged multi-replica small-partition scan emitted duplicate rows"
        );
        assert_eq!(
            got,
            expected,
            "paged multi-replica small-partition scan must deliver every \
             partition exactly once; got {} of {N}",
            got.len()
        );

        assert_eq!(
            cluster.frame_router.route_closures(),
            0,
            "paging must not phantom-close stream routes (t_dc729b1d)"
        );

        cluster.shutdown().await;
    });
}

/// t_a0f922a3 bug #2 (SILENT PARTIAL / data loss) on the REAL multi-replica
/// path, reproduced by forcing MANY windows PER PAGE.
///
/// The live `typed_edges` failure (COUNT=50807 intact) returned only 21160
/// rows with `has_more_pages=FALSE` — the scan finished ~one big partition /
/// token range then STOPPED, reporting complete. The prior harness tests all
/// use the DEFAULT 4096-row fragment cap, so `STREAM_WINDOW_CHUNKS=16` windows
/// each cover ~64k rows — a whole page fits in ONE window and the
/// `WindowedReplicaForwarder` continuation loop barely runs. The live cluster
/// had wide partitions and small effective windows, so the producer's
/// per-window stream closed at partition/window boundaries MANY times per page.
///
/// This test pins `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT=1` so every row is its
/// own chunk: a 16-chunk window covers only ~16 rows, forcing ~250 window
/// continuations across a 4000-row wide partition — the exact regime where a
/// producer stream closing without a terminating Done silently truncates the
/// scan. The union MUST be exact and `has_more` MUST stay true until true
/// exhaustion; a premature short page is silent data loss.
#[test]
fn multi_replica_many_windows_per_page_scan_is_not_silently_truncated() {
    // Serialize against every other test in this binary before mutating the
    // process-global fragment-size env var (cargo runs test fns concurrently
    // in one process).
    let _serial = serial_guard();
    // Force tiny windows: 1 row per fragment ⇒ 1 row per chunk ⇒ a 16-chunk
    // window is ~16 rows, so a wide partition spans hundreds of windows and the
    // continuation loop is exercised heavily.
    std::env::set_var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT", "1");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        const ROWS_PER: usize = 4_000;
        const PAGE_ROWS: usize = 1_500;
        // Distinct partition keys land on DIFFERENT tokens, so the paged scan
        // crosses partition boundaries mid-page and mid-window — the live shape.
        let pks: [&str; 3] = ["wpk-a", "wpk-b", "wpk-c"];

        let cluster = build_loopback_cluster(&|storage| seed_wide(storage, &pks, ROWS_PER)).await;
        let table_id = TableId::new(KS, TBL_WIDE);
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        let total = pks.len() * ROWS_PER;
        let max_pages = total.div_ceil(PAGE_ROWS) + 2;
        let delivered =
            page_rows_like_cql_collector(&cluster.wp, &table_id, &strategy, PAGE_ROWS, max_pages)
                .await;

        let expected: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> = pks
            .iter()
            .flat_map(|pk| {
                (0..ROWS_PER).map(|j| (pk.as_bytes().to_vec(), format!("ck-{j:06}").into_bytes()))
            })
            .collect();
        let got: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> =
            delivered.iter().cloned().collect();
        assert_eq!(
            got.len(),
            delivered.len(),
            "paged scan emitted duplicate rows across many-window pages"
        );
        assert_eq!(
            got,
            expected,
            "MANY-WINDOWS-PER-PAGE multi-replica scan silently truncated: \
             delivered {} of {} rows. A window-boundary producer close that is \
             read as scan-complete is silent data loss (t_a0f922a3 bug #2).",
            got.len(),
            total
        );

        assert_eq!(
            cluster.frame_router.route_closures(),
            0,
            "many-window paging must not phantom-close stream routes (t_dc729b1d)"
        );

        cluster.shutdown().await;
    });

    std::env::remove_var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT");
}

/// Seed a chosen subset of wide partitions on one replica. Models the live
/// topology where a replica's local storage holds only some partitions.
fn seed_wide_pks(engine: &StorageEngine, pks: &[&str], rows_per: usize) {
    seed_wide(engine, pks, rows_per);
}

/// t_a0f922a3 bug #2 (SILENT PARTIAL / data loss) with DISJOINT per-replica
/// data AND many windows per page — the true live topology.
///
/// Each replica's local storage holds a DIFFERENT subset of the wide
/// partitions (not the whole keyset), so the N-way merge must UNION across all
/// three sources. With `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT=1` a 16-chunk
/// window spans ~16 rows, so every wide partition crosses hundreds of window
/// boundaries per source. If ANY source's windowed continuation closes its
/// stream without a terminating Done (or the coordinator reads such a close as
/// "this replica is exhausted"), that source's remaining partitions vanish from
/// the result and the scan reports has_more=false — exactly the live
/// `COUNT=50807 but scan returns 21160, has_more=FALSE` signature.
///
/// The union MUST equal every seeded row across all replicas.
#[test]
fn multi_replica_disjoint_data_many_windows_scan_unions_completely() {
    let _serial = serial_guard();
    std::env::set_var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT", "1");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    ferrosa_cluster::coordinator::range_read_stream::forwarder_diag::reset();

    rt.block_on(async move {
        const ROWS_PER: usize = 3_000;
        const PAGE_ROWS: usize = 900;
        // Disjoint partition subsets per replica: local holds one BIG partition
        // (the live "one big partition" that the scan reached before stopping),
        // node2 + node3 each hold two further wide partitions. No replica holds
        // the whole keyset, so completeness depends on every source streaming to
        // true exhaustion across all its window boundaries.
        let local_pks: [&str; 1] = ["wpk-local-big"];
        let n2_pks: [&str; 2] = ["wpk-n2-a", "wpk-n2-b"];
        let n3_pks: [&str; 2] = ["wpk-n3-a", "wpk-n3-b"];

        let cluster = build_loopback_cluster_split(
            &|s| seed_wide_pks(s, &local_pks, ROWS_PER),
            &|s| seed_wide_pks(s, &n2_pks, ROWS_PER),
            &|s| seed_wide_pks(s, &n3_pks, ROWS_PER),
        )
        .await;
        let table_id = TableId::new(KS, TBL_WIDE);
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        let all_pks: Vec<&str> = local_pks
            .iter()
            .chain(n2_pks.iter())
            .chain(n3_pks.iter())
            .copied()
            .collect();
        let total = all_pks.len() * ROWS_PER;
        let max_pages = total.div_ceil(PAGE_ROWS) + 4;
        let delivered =
            page_rows_like_cql_collector(&cluster.wp, &table_id, &strategy, PAGE_ROWS, max_pages)
                .await;

        let expected: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> = all_pks
            .iter()
            .flat_map(|pk| {
                (0..ROWS_PER).map(|j| (pk.as_bytes().to_vec(), format!("ck-{j:06}").into_bytes()))
            })
            .collect();
        let got: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> =
            delivered.iter().cloned().collect();
        assert_eq!(
            got.len(),
            delivered.len(),
            "disjoint-data many-window scan emitted duplicate rows"
        );
        assert_eq!(
            got,
            expected,
            "DISJOINT-DATA multi-replica scan SILENTLY TRUNCATED: delivered {} \
             of {} rows with has_more=false. A source dropping out at a window \
             boundary (stream closed without a terminating Done, read as \
             replica-exhausted) is silent data loss (t_a0f922a3 bug #2).",
            got.len(),
            total
        );

        assert_eq!(
            cluster.frame_router.route_closures(),
            0,
            "disjoint-data paging must not phantom-close stream routes"
        );

        // The regime must actually exercise many window continuations (tiny
        // fragments ⇒ 16-chunk windows span ~16 rows over 3000-row partitions),
        // otherwise this test proves nothing about the continuation path.
        assert!(
            ferrosa_cluster::coordinator::range_read_stream::forwarder_diag::continuations_fired()
                > 0,
            "the many-window regime must fire window continuations, else the \
             silent-partial path is not under test"
        );

        cluster.shutdown().await;
    });

    // A loud replica error that could not be delivered to the consumer on a
    // full-drain scan is the silent-partial signature — it must never happen
    // when the scan pages to true completion.
    assert_eq!(
        ferrosa_cluster::coordinator::range_read_stream::forwarder_diag::error_send_dropped(),
        0,
        "a per-replica forwarder produced a loud error that was DISCARDED because \
         the merged output was gone — a replica's failure vanished and the scan \
         could look complete while rows remained (t_a0f922a3 bug #2)"
    );

    std::env::remove_var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT");
}

/// A replica node whose streaming range-read handler is backed by a REAL
/// `StorageEngine` seeded with the full dataset (the production
/// `StreamRangeReader for Arc<StorageEngine>` impl). Returns the server, its
/// bound addr, and the replica→coordinator back-channel `PeerManager`.
async fn spawn_storage_replica(
    host_id: uuid::Uuid,
    dir: &std::path::Path,
    n: usize,
) -> (Arc<RpcServer>, std::net::SocketAddr, Arc<PeerManager>) {
    spawn_storage_replica_seeded(host_id, dir, &|storage| seed_local(storage, n)).await
}

/// [`spawn_storage_replica`] with an arbitrary seeding function, so tests can
/// seed wide (multi-row) partitions and not just the single-row keyset.
async fn spawn_storage_replica_seeded(
    host_id: uuid::Uuid,
    dir: &std::path::Path,
    seed: &dyn Fn(&StorageEngine),
) -> (Arc<RpcServer>, std::net::SocketAddr, Arc<PeerManager>) {
    let storage = engine(dir);
    seed(&storage);
    let back = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        host_id,
        Arc::new(NoopListener),
    ));
    let sink_factory = Arc::new(PeerManagerSinkFactory::new(back.clone()));
    // Small chunk_size so multiple chunks flow per page — the replicas are
    // ACTIVELY streaming many chunks when the coordinator abandons a page
    // (t_3fc6be3c: parked-only replicas do not model the live failure).
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

/// End-to-end GREEN: page a REAL multi-replica projected scan
/// (`expected_done == 2`) to completion, resuming each page from the previous
/// page's last key — exactly what the CQL paging collector does with
/// `range_read_projected_stream_all_from`. The union of all pages must equal the
/// full keyset with NO gaps and NO duplicates, the loop must TERMINATE (a hard
/// timeout guards every page and the whole loop so a regression FAILS instead
/// of hanging CI), and — t_dc729b1d — NO stream route may close on a phantom
/// sequence gap: `route_closures()` must stay 0 even though every abandoned
/// page leaves in-flight straggler chunks behind.
#[test]
fn multi_replica_paged_projected_scan_returns_all_rows_across_pages() {
    let _serial = serial_guard();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        const N: usize = 120;
        const PAGE: usize = 7;

        let dir_local = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let dir3 = tempfile::tempdir().unwrap();

        let storage = engine(dir_local.path());
        seed_local(&storage, N);

        let coord_id = uuid::Uuid::new_v4();
        let r2_id = uuid::Uuid::new_v4();
        let r3_id = uuid::Uuid::new_v4();

        let (srv2, addr2, back2) = spawn_storage_replica(r2_id, dir2.path(), N).await;
        let (srv3, addr3, back3) = spawn_storage_replica(r3_id, dir3.path(), N).await;

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

        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            storage,
            3,
            ConsistencyLevel::All,
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
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };

        // Page through the whole scan, resuming each page at the previous page's
        // last key (inclusive lower bound; the coordinator + CQL collector apply
        // skip-≤-last, so here we skip the boundary key on resume to avoid a
        // duplicate, exactly mirroring the collector's cursor semantics).
        let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut start_key: Option<DecoratedKey> = None;
        let mut pages = 0usize;

        let whole = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                pages += 1;
                assert!(pages <= N + 5, "paging did not terminate (runaway pages)");

                let resume = start_key
                    .as_ref()
                    .map(|k| ferrosa_cluster::write_path::ScanResume {
                        key: k.clone(),
                        clustering: None,
                    });
                let mut stream = wp
                    .range_read_projected_stream_all_from(
                        &table_id,
                        vec![0],
                        resume.as_ref(),
                        ConsistencyLevel::All,
                        &strategy,
                    )
                    .await
                    .expect("multi-replica projected paged scan must not refuse");

                let mut page_keys: Vec<Vec<u8>> = Vec::new();
                let mut last_in_page: Option<DecoratedKey> = None;
                while page_keys.len() < PAGE {
                    let Some(item) = tokio::time::timeout(Duration::from_secs(15), stream.next())
                        .await
                        .expect("a page pull must not hang (stall is never acceptable)")
                    else {
                        break; // stream exhausted
                    };
                    let p = item.expect("partition");
                    // Skip the resume boundary key (inclusive lower bound) so a
                    // resumed page does not re-emit the page's last partition.
                    if let Some(prev) = start_key.as_ref() {
                        if p.key.key.as_bytes() == prev.key.as_bytes() {
                            continue;
                        }
                    }
                    let key_bytes = p.key.key.as_bytes().to_vec();
                    assert!(
                        seen.insert(key_bytes.clone()),
                        "duplicate partition across pages: {:?}",
                        String::from_utf8_lossy(&key_bytes)
                    );
                    page_keys.push(key_bytes);
                    last_in_page = Some(p.key.clone());
                }
                // Drop the page stream — abandons the coordinated fan-out exactly
                // like the CQL collector between pages, while the replicas are
                // still actively streaming chunks for it.
                drop(stream);

                match last_in_page {
                    // A full page: resume from its last key.
                    Some(k) if page_keys.len() == PAGE => start_key = Some(k),
                    // Short/empty final page: scan complete.
                    _ => break,
                }
            }
        })
        .await;
        assert!(
            whole.is_ok(),
            "the whole paged scan must terminate, not hang"
        );

        let expected: std::collections::BTreeSet<Vec<u8>> =
            (0..N).map(|i| format!("pk-{i:08}").into_bytes()).collect();
        assert_eq!(
            seen,
            expected,
            "paged multi-replica scan must return EVERY partition exactly once \
             across all pages (no gaps, no dupes); got {} of {}",
            seen.len(),
            N
        );
        assert!(
            pages >= 2,
            "PAGE={PAGE} over N={N} must span multiple pages"
        );

        // t_dc729b1d RED assertion: every abandoned page left the replicas
        // streaming straggler chunks at the coordinator. Pre-fix, each page's
        // stragglers fabricated fresh seq-state and phantom-closed that page's
        // route (`expected_seq=0 observed_seq=5` — one WARN per page, exactly
        // the 834-closes-per-viz-run live signature). Give in-flight stragglers
        // a moment to land, then assert on the COUNTER, not log text.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            frame_router.route_closures(),
            0,
            "in-order straggler chunks from abandoned pages must never close \
             stream routes as sequence gaps (t_dc729b1d)"
        );

        srv2.shutdown(Duration::from_millis(50)).await;
        srv3.shutdown(Duration::from_millis(50)).await;
        coord_srv.shutdown(Duration::from_millis(50)).await;
    });
}
