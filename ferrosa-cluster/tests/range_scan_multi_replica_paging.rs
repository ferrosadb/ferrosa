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
use std::sync::Arc;
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

/// A replica node whose streaming range-read handler is backed by a REAL
/// `StorageEngine` seeded with the full dataset (the production
/// `StreamRangeReader for Arc<StorageEngine>` impl). Returns the server, its
/// bound addr, and the replica→coordinator back-channel `PeerManager`.
async fn spawn_storage_replica(
    host_id: uuid::Uuid,
    dir: &std::path::Path,
    n: usize,
) -> (Arc<RpcServer>, std::net::SocketAddr, Arc<PeerManager>) {
    let storage = engine(dir);
    seed_local(&storage, n);
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

                let mut stream = wp
                    .range_read_projected_stream_all_from(
                        &table_id,
                        vec![0],
                        start_key.as_ref(),
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
