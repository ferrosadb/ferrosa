//! Keyed (in-partition) secondary-index reads stream end to end over
//! internode, with no result cap.
//!
//! Harness: real `RpcServer`s over 127.0.0.1 loopback. Node 1 is the
//! coordinator and a replica (its engine is read locally); the other nodes are
//! remote replicas whose real engines serve the streaming request through the
//! production `RangeReadStreamRequestHandler`, firing frames back over a
//! back-channel `PeerManager` into the coordinator's `StreamFrameRouter`.
//!
//! RED before the change: `WritePath::index_read_in_partition_stream` did not
//! exist; the keyed read returned a `Vec` and every replica refused more than
//! 10,000 matches in one partition ("secondary index query exceeded 10000 row
//! limit").

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::StreamExt;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_index::IndexKey;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::stream_frame_router::StreamFrameRouter;
use ferrosa_cluster::coordinator::stream_request_handler::{
    PeerManagerSinkFactory, RangeReadStreamRequestHandler,
};
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
use ferrosa_net::rpc::server::RpcServer;

const KS: &str = "keyed_ks";
const TBL: &str = "rows";
const INDEX: &str = "value_idx";

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _peer: PeerId) {}
    fn on_peer_disconnected(&self, _peer: PeerId) {}
    fn on_peer_suspected(&self, _peer: PeerId) {}
    fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
    fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
}

fn table() -> TableId {
    TableId::new(KS, TBL)
}

fn key(name: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
}

fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
    // One explicit flush per test instead of test_config's 4 KB threshold:
    // hundreds of tiny SSTables would make every point read probe them all.
    let mut config = StorageEngineConfig::test_config(dir);
    config.flush_threshold_bytes = 256 * 1024 * 1024;
    let engine = StorageEngine::new(config, None).unwrap();
    engine
        .register_table_with_indexes(
            TableSchema {
                keyspace: KS.into(),
                table: TBL.into(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                clustering_columns: vec![ColumnDefinition {
                    name: "ck".into(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
                }],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "value".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                }],
                extensions: Default::default(),
            },
            vec![(INDEX.into(), 0)],
        )
        .unwrap();
    Arc::new(engine)
}

fn write_rows(engine: &StorageEngine, partition: &str, value: &[u8], rows: std::ops::Range<i32>) {
    let key = key(partition);
    for ck in rows {
        engine
            .write(
                &table(),
                &key,
                Arc::new(Row {
                    clustering: ck.to_be_bytes().to_vec(),
                    cells: vec![(0, CellValue::live(value.to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                }),
                1000,
            )
            .unwrap();
    }
}

fn net_config(capabilities: u32) -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised_capabilities: capabilities,
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

/// Counts the stream messages a replica receives, then hands them to the
/// production stream handler.
struct Counting {
    inner: Arc<dyn RpcHandler>,
    keyed_requests: Arc<AtomicUsize>,
    global_requests: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
}

#[async_trait]
impl RpcHandler for Counting {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let counter = match &msg {
            Message::IndexReadInPartitionStreamRequest(_) => &self.keyed_requests,
            Message::RangeReadStreamRequest(_) => &self.global_requests,
            Message::RangeReadStreamCancel(_) => &self.cancels,
            _ => return self.inner.handle(from, msg).await,
        };
        counter.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(from, msg).await
    }
}

struct Replica {
    server: Arc<RpcServer>,
    addr: String,
    host: uuid::Uuid,
    back: Arc<PeerManager>,
    keyed_requests: Arc<AtomicUsize>,
    global_requests: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
}

/// A remote replica serving `engine` with the production stream handler and
/// chunk size, advertising `capabilities` (a value without `CAP_INDEX_PARTITION_STREAM`
/// stands in for a peer on an older build, which is only sent the requests
/// it already understands).
async fn spawn_replica(engine: Arc<StorageEngine>, capabilities: u32) -> Replica {
    let host = uuid::Uuid::new_v4();
    let back = Arc::new(PeerManager::new(
        Arc::new(net_config(ferrosa_net::handshake::LOCAL_CAPABILITIES)),
        host,
        Arc::new(NoopListener),
    ));
    let stream_handler = Arc::new(RangeReadStreamRequestHandler::new(
        Arc::new(engine.clone()),
        Arc::new(PeerManagerSinkFactory::new(back.clone())),
        ferrosa_cluster::coordinator::range_read_stream::STREAMING_CHUNK_PARTITIONS,
    ));
    let keyed_requests = Arc::new(AtomicUsize::new(0));
    let global_requests = Arc::new(AtomicUsize::new(0));
    let cancels = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(Counting {
        inner: stream_handler,
        keyed_requests: keyed_requests.clone(),
        global_requests: global_requests.clone(),
        cancels: cancels.clone(),
    });
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::RangeReadStreamRequest, handler.clone());
    registry.register(MsgType::RangeReadStreamCancel, handler.clone());
    if capabilities & ferrosa_net::handshake::CAP_INDEX_PARTITION_STREAM != 0 {
        registry.register(MsgType::IndexReadInPartitionStreamRequest, handler);
    }
    registry.register(
        MsgType::ReadRequest,
        Arc::new(ferrosa_cluster::raft::handlers::ReadRequestHandler::new(
            engine,
        )),
    );
    let server = Arc::new(RpcServer::new(net_config(capabilities), host, registry));
    let addr = server.start_and_get_addr().await.unwrap().to_string();
    Replica {
        server,
        addr,
        host,
        back,
        keyed_requests,
        global_requests,
        cancels,
    }
}

struct Cluster {
    write_path: WritePath,
    coordinator_server: Arc<RpcServer>,
    replicas: Vec<Replica>,
    strategy: ReplicationStrategy,
}

impl Cluster {
    /// Node 1 (local `storage`) coordinates; every node replicates every key.
    async fn new(storage: Arc<StorageEngine>, replicas: Vec<Replica>) -> Self {
        let coordinator_host = uuid::Uuid::new_v4();
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node(coordinator_host, "127.0.0.1:1"));
        ring.assign_tokens(1, &[i64::MIN]);
        for (index, replica) in replicas.iter().enumerate() {
            let id = index as u64 + 2;
            ring.add_node(id, ring_node(replica.host, &replica.addr));
            ring.assign_tokens(id, &[(index as i64 + 1) * (i64::MAX / 4)]);
        }
        let peers = Arc::new(PeerManager::new(
            Arc::new(net_config(ferrosa_net::handshake::LOCAL_CAPABILITIES)),
            coordinator_host,
            Arc::new(NoopListener),
        ));
        let node_count = replicas.len() + 1;
        let coordinator = Arc::new(ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            storage,
            node_count,
            ConsistencyLevel::All,
        ));
        let frames = Arc::new(StreamFrameRouter::new(coordinator.stream_router()));
        let registry = Arc::new(HandlerRegistry::new());
        for kind in [
            MsgType::RangeReadStreamChunk,
            MsgType::RangeReadStreamHeartbeat,
            MsgType::RangeReadStreamDone,
        ] {
            registry.register(kind, frames.clone());
        }
        let coordinator_server = Arc::new(RpcServer::new(
            net_config(ferrosa_net::handshake::LOCAL_CAPABILITIES),
            coordinator_host,
            registry,
        ));
        let coordinator_addr = coordinator_server
            .start_and_get_addr()
            .await
            .unwrap()
            .to_string();
        for replica in &replicas {
            replica
                .back
                .ensure_peer(coordinator_host, &coordinator_addr)
                .await
                .unwrap();
        }
        Self {
            write_path: WritePath::cluster(coordinator),
            coordinator_server,
            replicas,
            strategy: ReplicationStrategy::Simple {
                replication_factor: node_count,
            },
        }
    }

    async fn open(
        &self,
        partition: &str,
        value: &[u8],
    ) -> ferrosa_cluster::write_path::PartitionResultStream {
        tokio::time::timeout(
            Duration::from_secs(20),
            self.write_path.index_read_in_partition_stream(
                &table(),
                &key(partition),
                INDEX,
                &IndexKey(value.to_vec()),
                ConsistencyLevel::All,
                &self.strategy,
            ),
        )
        .await
        .expect("opening a keyed index stream must not hang")
        .expect("a keyed index stream must open")
    }

    async fn shutdown(self) {
        for replica in self.replicas {
            replica.server.shutdown(Duration::from_millis(50)).await;
        }
        self.coordinator_server
            .shutdown(Duration::from_millis(50))
            .await;
    }
}

/// Every clustering a stream yields, in order, checking each item is one row
/// of `partition`.
async fn drain(
    mut stream: ferrosa_cluster::write_path::PartitionResultStream,
    partition: &str,
) -> Vec<Vec<u8>> {
    let mut clusterings = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("the keyed index stream stalled");
        let Some(item) = next else {
            return clusterings;
        };
        let item = item.expect("keyed index stream item");
        assert_eq!(item.key.key.as_bytes(), partition.as_bytes());
        assert_eq!(item.rows.len(), 1, "one row per item");
        clusterings.extend(item.rows.iter().map(|row| row.clustering.clone()));
    }
}

async fn wait_for(counter: &AtomicUsize, at_least: usize) -> bool {
    for _ in 0..400 {
        if counter.load(Ordering::SeqCst) >= at_least {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[test]
fn a_keyed_index_read_streams_more_than_ten_thousand_rows_over_internode() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let engines: Vec<_> = dirs.iter().map(|dir| engine(dir.path())).collect();
        const MATCHES: i32 = 10_001;
        for engine in &engines {
            // Half the postings come from a flushed sidecar, half from the
            // memtable.
            write_rows(engine, "hot", b"X", 0..MATCHES / 2);
            engine.flush(&table()).unwrap();
            write_rows(engine, "hot", b"X", MATCHES / 2..MATCHES);
            write_rows(engine, "hot", b"Y", MATCHES..MATCHES + 50);
            write_rows(engine, "other", b"X", 0..50);
        }
        let mut replicas = Vec::new();
        for engine in &engines[1..] {
            replicas.push(
                spawn_replica(engine.clone(), ferrosa_net::handshake::LOCAL_CAPABILITIES).await,
            );
        }
        let cluster = Cluster::new(engines[0].clone(), replicas).await;

        let rows = drain(cluster.open("hot", b"X").await, "hot").await;
        assert_eq!(
            rows.len(),
            MATCHES as usize,
            "every match arrives, none twice"
        );
        assert!(
            rows.windows(2).all(|pair| pair[0] < pair[1]),
            "in clustering order"
        );
        for replica in &cluster.replicas {
            assert!(
                replica.keyed_requests.load(Ordering::SeqCst) > 1,
                "a peer with the capability streams in windows of the new request"
            );
            assert_eq!(replica.global_requests.load(Ordering::SeqCst), 0);
        }

        // An early-stopping consumer: one row, then the stream is dropped.
        // Each remote producer is cancelled rather than driven to the end.
        let before: Vec<usize> = cluster
            .replicas
            .iter()
            .map(|replica| replica.keyed_requests.load(Ordering::SeqCst))
            .collect();
        let mut stream = cluster.open("hot", b"X").await;
        let first = tokio::time::timeout(Duration::from_secs(20), stream.next())
            .await
            .expect("first row must arrive")
            .expect("stream has rows")
            .expect("first row");
        assert_eq!(first.rows[0].clustering, 0_i32.to_be_bytes().to_vec());
        drop(stream);
        for (replica, before) in cluster.replicas.iter().zip(before) {
            assert!(
                wait_for(&replica.cancels, 1).await,
                "the abandoned keyed stream must cancel its remote producer"
            );
            assert_eq!(
                replica.keyed_requests.load(Ordering::SeqCst),
                before + 1,
                "one window, never a continuation, for an abandoned read"
            );
        }
        cluster.shutdown().await;
    });
}

#[test]
fn a_replica_without_the_capability_is_served_the_global_walk_cut_at_the_partition() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let engines: Vec<_> = dirs.iter().map(|dir| engine(dir.path())).collect();
        for engine in &engines {
            // `b` is the keyed partition; `c` sorts after it in posting order
            // and holds far more than one window (16 chunks of 64) of matches.
            write_rows(engine, "a", b"X", 0..20);
            write_rows(engine, "b", b"X", 0..5);
            write_rows(engine, "c", b"X", 0..3_000);
        }
        let old_build = ferrosa_net::handshake::LOCAL_CAPABILITIES
            & !ferrosa_net::handshake::CAP_INDEX_PARTITION_STREAM;
        let replica = spawn_replica(engines[1].clone(), old_build).await;
        let cluster = Cluster::new(engines[0].clone(), vec![replica]).await;

        let rows = drain(cluster.open("b", b"X").await, "b").await;
        let expected: Vec<Vec<u8>> = (0..5_i32).map(|ck| ck.to_be_bytes().to_vec()).collect();
        assert_eq!(rows, expected, "exactly the keyed partition's rows");
        let old = &cluster.replicas[0];
        assert_eq!(
            old.keyed_requests.load(Ordering::SeqCst),
            0,
            "a peer without the capability is never sent the new type byte"
        );
        assert!(old.global_requests.load(Ordering::SeqCst) >= 1);
        assert!(
            wait_for(&old.cancels, 1).await,
            "the global walk, cut at the first row of another partition, must be cancelled"
        );
        cluster.shutdown().await;
    });
}
