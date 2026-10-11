//! F11 (outage 2026-10-10): a LOCAL_QUORUM read with one of three replicas
//! down, and with one replica unable to read its own storage.
//!
//! ## What happened
//!
//! The control listener's ledger read (`SELECT version FROM
//! agent_memory.schema_version WHERE version = ?`, LOCAL_QUORUM, RF 3) failed
//! 89 times between 23:55Z and 00:11Z with
//! `read timeout: CL=LOCAL_QUORUM, received=1, required=2`. node2 was down.
//! node1 was up but could not read the partition: at 22:21Z it had evicted
//! `schema_version` generation 1790884385644859 to R2, and at 23:55Z a read of
//! it logged `outcome=NotOwned` ("no object store owns it"), because the node
//! had started through `StorageEngine::open`, which does not register the
//! evicted-read store (F6). From then on every node1 read of that partition
//! failed. Only node3 could answer, so `received=1` was the truth.
//!
//! The 37 `data_present=true` failures were reads coordinated by node3 and
//! each followed, 10 s later, one node1 `ReadRequestHandler: storage read
//! failed` line (37 of 37). The 52 `data_present=false` failures were
//! coordinated by node1, whose own full read failed.
//!
//! ## What these tests pin
//!
//! Three real nodes over loopback, each with its own `StorageEngine` serving
//! the production `ReadRequestHandler`; the coordinator is node 1.
//!
//! 1. `local_quorum_read_succeeds_with_one_of_three_replicas_down` (T-F11-1):
//!    the read path is correct with a down replica.
//! 2. `local_quorum_read_is_served_by_the_live_replicas_when_the_coordinator_cannot_read`:
//!    node1's storage read fails, node2 and node3 are healthy. Two live
//!    replicas can answer, so the read must succeed. Before the fix the
//!    coordinator launched only DIGEST reads to replace its failed full read,
//!    and failed with `received=2, required=2, data_present=false`; had node2
//!    been up during the outage, every node1-coordinated read would still have
//!    failed this way.
//! 3. `local_quorum_read_fails_loud_when_only_one_replica_can_answer`: the
//!    outage's exact shape (node1 cannot read, node2 down, node3 healthy). One
//!    replica cannot make a quorum, so the read must fail with
//!    `received=1, required=2` rather than return node3's data unchecked.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::error::ClusterError;
use ferrosa_cluster::raft::handlers::ReadRequestHandler;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;

const KS: &str = "quorum_down_ks";
const TBL: &str = "schema_version";
const WRITE_TS: i64 = 1_790_884_385;
/// RF 3, as the outage keyspace.
const RF: usize = 3;
/// Bound on any single read in this file. A read that waits on a dead replica
/// for longer than this is itself the defect.
const READ_BOUND: Duration = Duration::from_secs(30);

/// These tests start real loopback servers; keep them from contending.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        flush_threshold_bytes: 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
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
    engine
        .register_table(TableSchema {
            keyspace: KS.to_string(),
            table: TBL.to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "applied_at".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        })
        .unwrap();
    engine
}

fn table_id() -> TableId {
    TableId::new(KS, TBL)
}

/// The ledger row the listener looks for: migration 66.
fn ledger_key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(66i32.to_be_bytes().to_vec()))
}

fn ledger_row() -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(b"applied".to_vec(), WRITE_TS))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(WRITE_TS),
    }
}

/// Write the ledger row and flush it, so the partition lives only in an
/// SSTable (as the evicted generation did on node1).
fn seed(storage: &StorageEngine) {
    storage
        .write(&table_id(), &ledger_key(), ledger_row(), WRITE_TS)
        .unwrap();
    storage.flush(&table_id()).unwrap();
}

/// Make every read of the ledger partition on `storage` fail, the way node1's
/// reads failed once its generation was evicted with no store to serve it:
/// the only copy of the row is in an SSTable whose `Data.db` is unreadable.
fn make_unreadable(dir: &std::path::Path) {
    let sstable_dir = dir.join("sstables").join(table_id().to_string());
    let mut damaged = 0usize;
    for entry in walk(&sstable_dir) {
        if entry.to_string_lossy().ends_with("-Data.db") {
            std::fs::write(&entry, [0u8]).unwrap();
            damaged += 1;
        }
    }
    assert!(damaged > 0, "test setup: no flushed Data.db to damage");
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    // Bounded: an SSTable directory is at most two levels deep.
    for _ in 0..64 {
        let Some(next) = pending.pop() else { break };
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                out.push(path);
            }
        }
    }
    assert!(
        pending.is_empty(),
        "test setup: SSTable tree deeper than expected"
    );
    out
}

/// A remote replica running on its OWN Tokio runtime, so it can be killed the
/// way a process dies: dropping the runtime aborts its accept loop and every
/// connection task, and the OS closes their sockets. (`RpcServer::shutdown`
/// only stops accepting; an established connection keeps answering, which
/// would make a "down" replica that still serves reads.)
struct Replica {
    host_id: uuid::Uuid,
    addr: std::net::SocketAddr,
    runtime: Option<tokio::runtime::Runtime>,
    _dir: tempfile::TempDir,
}

impl Replica {
    /// Kill the replica: every task on its runtime stops, every socket closes.
    fn kill(&mut self) {
        let runtime = self.runtime.take().expect("test bug: replica killed twice");
        runtime.shutdown_background();
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        // A runtime may not be dropped from async context; shut it down
        // without blocking instead.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// A remote replica: its own storage holding the ledger row, served by the
/// production `ReadRequestHandler`.
fn spawn_replica() -> Replica {
    let dir = tempfile::tempdir().unwrap();
    let storage = engine(dir.path());
    seed(&storage);
    let host_id = uuid::Uuid::new_v4();
    let handler = Arc::new(ReadRequestHandler::new(storage));
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::ReadRequest, handler.clone());
    registry.register(MsgType::PartitionSuffixReadRequest, handler);
    let server = Arc::new(RpcServer::new(net_config(), host_id, registry));
    // Build and start the replica's runtime on a plain thread: a runtime
    // cannot be blocked on from inside the test's runtime.
    let (runtime, addr) = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("replica runtime");
        let addr = runtime
            .block_on(server.start_and_get_addr())
            .expect("replica server start");
        (runtime, addr)
    })
    .join()
    .expect("replica start thread panicked");
    Replica {
        host_id,
        addr,
        runtime: Some(runtime),
        _dir: dir,
    }
}

struct Cluster {
    coordinator: ClusterCoordinator,
    node2: Replica,
    /// Held so node3 stays up for the whole test; dropped (killed) at the end.
    _node3: Replica,
    _dir1: tempfile::TempDir,
}

/// Three nodes, every one a replica of every token (RF 3 on 3 nodes), node 1
/// coordinating. `node1_unreadable` damages node1's local copy.
async fn three_node_cluster(node1_unreadable: bool) -> Cluster {
    let dir1 = tempfile::tempdir().unwrap();
    let storage1 = engine(dir1.path());
    seed(&storage1);
    if node1_unreadable {
        make_unreadable(dir1.path());
    }
    let node2 = spawn_replica();
    let node3 = spawn_replica();

    let node1_id = uuid::Uuid::new_v4();
    let mut ring = TokenRing::new();
    ring.add_node(1, ring_node(node1_id, "127.0.0.1:1"));
    ring.add_node(2, ring_node(node2.host_id, &node2.addr.to_string()));
    ring.add_node(3, ring_node(node3.host_id, &node3.addr.to_string()));
    ring.assign_tokens(1, &[i64::MIN / 2]);
    ring.assign_tokens(2, &[0]);
    ring.assign_tokens(3, &[i64::MAX / 2]);
    assert_eq!(
        ring.replicas(ledger_key().token.0, RF).len(),
        RF,
        "test setup: every node must be a replica of the ledger key"
    );

    let peers = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        node1_id,
        Arc::new(NoopListener),
    ));
    peers
        .ensure_peer(node2.host_id, &node2.addr.to_string())
        .await
        .unwrap();
    peers
        .ensure_peer(node3.host_id, &node3.addr.to_string())
        .await
        .unwrap();

    let coordinator = ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        storage1,
        RF,
        ConsistencyLevel::LocalQuorum,
    );
    Cluster {
        coordinator,
        node2,
        _node3: node3,
        _dir1: dir1,
    }
}

/// Take node2 down the way the outage did: it was a connected peer, then its
/// process went away. Waits until a fresh connection to it is refused, so the
/// read under test runs against a replica that is really gone.
async fn kill(replica: &mut Replica) {
    replica.kill();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::net::TcpStream::connect(replica.addr).await {
            Err(_) => return,
            Ok(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(_) => panic!("killed replica {} still accepts connections", replica.addr),
        }
    }
}

async fn ledger_read(
    coordinator: &ClusterCoordinator,
) -> (Result<Option<Vec<Row>>, ClusterError>, Duration) {
    let started = Instant::now();
    let result = tokio::time::timeout(
        READ_BOUND,
        coordinator.coordinate_read_with(
            &table_id(),
            &ledger_key(),
            ConsistencyLevel::LocalQuorum,
            RF,
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("LOCAL_QUORUM read did not finish within {READ_BOUND:?}"));
    (result, started.elapsed())
}

fn assert_is_ledger_row(rows: Option<Vec<Row>>, context: &str) {
    let rows = rows.unwrap_or_else(|| panic!("{context}: the ledger row is absent"));
    assert_eq!(rows.len(), 1, "{context}: expected exactly the ledger row");
    let (column, cell) = &rows[0].cells[0];
    assert_eq!(*column, 0, "{context}: wrong column");
    assert_eq!(cell.timestamp, WRITE_TS, "{context}: wrong write timestamp");
    assert_eq!(
        cell.value.as_deref(),
        Some(&b"applied"[..]),
        "{context}: wrong cell value"
    );
}

/// T-F11-1: RF 3, one replica down, LOCAL_QUORUM returns the written row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_quorum_read_succeeds_with_one_of_three_replicas_down() {
    let _serial = SERIAL.lock().await;
    let mut cluster = three_node_cluster(false).await;
    kill(&mut cluster.node2).await;

    let (result, elapsed) = ledger_read(&cluster.coordinator).await;
    let rows = result.unwrap_or_else(|e| {
        panic!("LOCAL_QUORUM with 2 of 3 replicas up must succeed, got {e} after {elapsed:?}")
    });
    assert_is_ledger_row(rows, "one of three replicas down");
}

/// The coordinator's own replica cannot read; the two remote replicas can.
/// Two live replicas make a LOCAL_QUORUM, so the read must be served by them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_quorum_read_is_served_by_the_live_replicas_when_the_coordinator_cannot_read() {
    let _serial = SERIAL.lock().await;
    let cluster = three_node_cluster(true).await;

    let (result, elapsed) = ledger_read(&cluster.coordinator).await;
    let rows = result.unwrap_or_else(|e| {
        panic!(
            "node2 and node3 both hold the row and both answer, so LOCAL_QUORUM \
             must succeed even though node1 cannot read its own copy; got {e} \
             after {elapsed:?}"
        )
    });
    assert_is_ledger_row(rows, "coordinator replica unreadable");
}

/// The outage's shape: node1 cannot read, node2 is down, node3 is healthy.
/// One replica is not a quorum; the read must fail and say so precisely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_quorum_read_fails_loud_when_only_one_replica_can_answer() {
    let _serial = SERIAL.lock().await;
    let mut cluster = three_node_cluster(true).await;
    kill(&mut cluster.node2).await;

    let (result, elapsed) = ledger_read(&cluster.coordinator).await;
    match result {
        Err(ClusterError::ReadTimeout {
            consistency,
            received,
            required,
            data_present: _,
        }) => {
            // `data_present` is deliberately not asserted: whether node3 is
            // asked for the full read or for a digest depends on whether
            // node1's failed full read or node2's failed digest is seen first,
            // and the outage logged both (52 false, 37 true). Either way one
            // replica cannot make the quorum.
            assert_eq!(consistency, "LOCAL_QUORUM");
            assert_eq!(required, 2, "LOCAL_QUORUM of RF 3 needs 2");
            assert_eq!(
                received, 1,
                "only node3 could answer; the outage reported received=1"
            );
        }
        other => panic!(
            "one readable replica of three is not a LOCAL_QUORUM; expected \
             ReadTimeout received=1 required=2, got {other:?} after {elapsed:?}"
        ),
    }
}
