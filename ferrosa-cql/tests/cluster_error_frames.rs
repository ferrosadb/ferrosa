//! A coordinator's read timeout and unavailability reach CQL clients as the
//! protocol's ReadTimeout (0x1200) and Unavailable (0x1000) errors, with their
//! fields, not as a ServerError (t_43f70b3a, outage 2026-10-10).
//!
//! In the outage the control listener saw
//! `Internal server error ... server error: storage error: invalid data:
//! cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, ...`.
//! `WritePath` flattened the coordinator's typed `ClusterError` into
//! `ferrosa_common::Error::InvalidData(String)`, and the CQL layer could only
//! send that as ServerError (0x0000). A driver's retry policy, and
//! ferrosa-memory's startup retry classifier (which treats
//! `DbError::ReadTimeout` / `DbError::Unavailable` as transient), never saw a
//! timeout.
//!
//! These tests run a real `ClusterCoordinator` read through `WritePath` (the
//! path a CQL SELECT takes), convert the error the way the router does
//! (`CqlError::from`), encode the ERROR body, and parse it with the scylla
//! driver's own parser.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_cql::error::CqlError;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};
use scylla_cql::frame::protocol_features::ProtocolFeatures;
use scylla_cql::frame::response::error::{DbError, Error as DriverError};
use scylla_cql::Consistency;

const KS: &str = "frames_ks";
const TBL: &str = "schema_version";

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

fn node(addr: &str) -> NodeInfo {
    NodeInfo {
        host_id: uuid::Uuid::new_v4(),
        addr: addr.to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
}

/// An address nothing listens on: a peer there is down.
fn dead_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(66i32.to_be_bytes().to_vec()))
}

/// A coordinator (node 1, holding the row) in a ring of `remotes` + 1 nodes
/// whose remote replicas are all down.
fn write_path(dir: &std::path::Path, remotes: usize) -> WritePath {
    let storage = engine(dir);
    let row = Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(b"applied".to_vec(), 1000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    };
    storage
        .write(&TableId::new(KS, TBL), &key(), row, 1000)
        .unwrap();

    let mut ring = TokenRing::new();
    ring.add_node(1, node("127.0.0.1:1"));
    ring.assign_tokens(1, &[i64::MIN / 2]);
    for i in 0..remotes {
        let id = 2 + i as u64;
        ring.add_node(id, node(&dead_addr()));
        ring.assign_tokens(id, &[i as i64 * 1000]);
    }
    let peers = Arc::new(PeerManager::new(
        Arc::new(NetConfig::default()),
        uuid::Uuid::new_v4(),
        Arc::new(NoopListener),
    ));
    WritePath::cluster(Arc::new(ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        storage,
        3,
        ConsistencyLevel::LocalQuorum,
    )))
}

/// Read at LOCAL_QUORUM through `WritePath`, convert the failure the way the
/// router does, and parse the encoded ERROR body with the driver's parser.
async fn driver_view_of_failed_read(wp: &WritePath) -> (u32, DriverError) {
    let strategy = ReplicationStrategy::Simple {
        replication_factor: 3,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        wp.pk_read(
            &TableId::new(KS, TBL),
            &key(),
            ConsistencyLevel::LocalQuorum,
            &strategy,
        ),
    )
    .await
    .expect("the read must finish");
    let err = match result {
        Err(e) => CqlError::from(e),
        Ok(rows) => panic!("one live replica of three cannot serve LOCAL_QUORUM; got {rows:?}"),
    };
    let body = err.encode_body();
    let code = u32::from_be_bytes(body[..4].try_into().unwrap());
    let mut buf: &[u8] = &body;
    let parsed = DriverError::deserialize(&ProtocolFeatures::default(), &mut buf)
        .unwrap_or_else(|e| panic!("the driver cannot parse our ERROR frame ({err}): {e}"));
    assert!(
        buf.is_empty(),
        "the ERROR body has {} trailing bytes the driver did not read",
        buf.len()
    );
    (code, parsed)
}

/// The outage shape: the coordinator's own replica answers, the other two are
/// down, so LOCAL_QUORUM gets one response of two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_read_timeout_reaches_the_client_as_cql_read_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let wp = write_path(dir.path(), 2);

    let (code, parsed) = driver_view_of_failed_read(&wp).await;

    assert_eq!(
        code, 0x1200,
        "a coordinator read timeout must be sent as CQL Read_timeout (0x1200), \
         not ServerError; the client saw: {}",
        parsed.reason
    );
    match parsed.error {
        DbError::ReadTimeout {
            consistency,
            received,
            required,
            data_present,
        } => {
            assert_eq!(consistency, Consistency::LocalQuorum);
            assert_eq!(received, 1, "only the coordinator's own replica answered");
            assert_eq!(required, 2, "LOCAL_QUORUM of RF 3 blocks for 2");
            assert!(
                data_present,
                "the coordinator's own replica returned the data"
            );
        }
        other => panic!("the driver must classify this as DbError::ReadTimeout; got {other:?}"),
    }
}

/// Fewer live-eligible replicas than the CL needs: CQL Unavailable (0x1000).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_unavailable_reaches_the_client_as_cql_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    // A one-node ring: RF 3 resolves to a single replica.
    let wp = write_path(dir.path(), 0);

    let (code, parsed) = driver_view_of_failed_read(&wp).await;

    assert_eq!(
        code, 0x1000,
        "a coordinator Unavailable must be sent as CQL Unavailable (0x1000), \
         not ServerError; the client saw: {}",
        parsed.reason
    );
    match parsed.error {
        DbError::Unavailable {
            consistency,
            required,
            alive,
        } => {
            assert_eq!(consistency, Consistency::LocalQuorum);
            assert_eq!(required, 2);
            assert_eq!(alive, 1);
        }
        other => panic!("the driver must classify this as DbError::Unavailable; got {other:?}"),
    }
}
