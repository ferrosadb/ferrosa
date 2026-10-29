use super::*;
use crate::consistency::ConsistencyLevel;
use crate::coordinator::stream_frame_router::StreamFrameRouter;
use crate::coordinator::stream_request_handler::{
    PeerManagerSinkFactory, RangeReadStreamRequestHandler,
};
use crate::raft::{NodeInfo, NodeState};
use crate::ring::{strategy::ReplicationStrategy, TokenRing};
use crate::write_path::WritePath;
use arc_swap::ArcSwap;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::{handler::HandlerRegistry, server::RpcServer};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};
const KS: &str = "test_ks";
const TBL: &str = "test_tbl";
const TBL_WIDE: &str = "test_wide";
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
        cache_hot_window_secs: 900,
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
async fn range_case(live: &[u64], cl: ConsistencyLevel) -> crate::error::Result<Vec<Partition>> {
    let mut dirs = vec![tempfile::tempdir().unwrap()];
    let storage = engine(dirs[0].path());
    seed_local(&storage, 4);
    let local = uuid::Uuid::new_v4();
    let peers = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        local,
        Arc::new(NoopListener),
    ));
    let mut ring = TokenRing::new();
    let mut servers = Vec::new();
    let mut backs = Vec::new();
    for id in 1..=9u64 {
        let host = if id == 1 { local } else { uuid::Uuid::new_v4() };
        let mut node = ring_node(host, "127.0.0.1:1");
        node.data_center = format!("dc{}", (id - 1) / 3 + 1);
        if id != 1 && live.contains(&id) {
            dirs.push(tempfile::tempdir().unwrap());
            let (srv, addr, back) =
                spawn_storage_replica_seeded(host, dirs.last().unwrap().path(), &|s| {
                    seed_local(s, 4);
                    if id == 4 {
                        let mut p = partition(0);
                        p.rows[0].cells[0].1 = CellValue::live(b"newest".to_vec(), 2000);
                        s.write(&TableId::new(KS, TBL), &p.key, p.rows[0].clone(), 2000)
                            .unwrap();
                    }
                })
                .await;
            node.addr = addr.to_string();
            peers.ensure_peer(host, &node.addr).await.unwrap();
            servers.push(srv);
            backs.push(back);
        }
        ring.add_node(id, node);
        ring.assign_tokens(id, &[i64::MIN / 2 + (id as i64) * 1000]);
    }
    let coordinator = Arc::new(ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers,
        1,
        storage,
        9,
        cl,
    ));
    let registry = Arc::new(HandlerRegistry::new());
    let router = Arc::new(StreamFrameRouter::new(coordinator.stream_router()));
    registry.register(MsgType::RangeReadStreamChunk, router.clone());
    registry.register(MsgType::RangeReadStreamHeartbeat, router.clone());
    registry.register(MsgType::RangeReadStreamDone, router);
    let srv = Arc::new(RpcServer::new(net_config(), local, registry));
    let addr = srv.start_and_get_addr().await.unwrap();
    for back in backs {
        back.ensure_peer(local, &addr.to_string()).await.unwrap();
    }
    let strategy = ReplicationStrategy::NetworkTopology {
        dc_rf: [("dc1".into(), 3), ("dc2".into(), 3), ("dc3".into(), 3)].into(),
    };
    let result = async {
        let wp = WritePath::cluster(coordinator);
        let table = TableId::new(KS, TBL);
        let mut last_rows = Vec::new();
        for mode in 0..4 {
            let mut stream = match mode {
                0 => {
                    wp.range_read_stream_all_with(&table, 0, cl, &strategy)
                        .await?
                }
                1 => {
                    wp.range_read_stream_all_from(&table, None, cl, &strategy)
                        .await?
                }
                2 => {
                    wp.range_read_projected_stream_all_with(&table, vec![0], None, cl, &strategy)
                        .await?
                }
                _ => {
                    wp.range_read_projected_stream_all_from(&table, vec![0], None, cl, &strategy)
                        .await?
                }
            };
            let mut rows = Vec::new();
            while let Some(p) = stream.next().await {
                rows.push(p?);
            }
            assert_eq!(rows.len(), 4, "range entry point {mode} must be complete");
            let mut actual: Vec<_> = rows.iter().map(|p| p.key.clone()).collect();
            let mut expected: Vec<_> = (0..4).map(|i| partition(i).key).collect();
            actual.sort();
            expected.sort();
            assert_eq!(
                actual, expected,
                "range entry point {mode} lost, duplicated or invented a partition"
            );
            last_rows = rows;
        }
        assert_eq!(wp.count_range_with(&table, cl, &strategy).await?, 4);
        let rows = last_rows;
        Ok(rows)
    }
    .await;
    for server in servers {
        server.shutdown(Duration::from_millis(10)).await;
    }
    srv.shutdown(Duration::from_millis(10)).await;
    result
}
#[tokio::test]
async fn verification_nts_range_two_dcs_complete() {
    let rows = range_case(&[1, 2, 4, 5], ConsistencyLevel::Quorum)
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows.iter().map(|p| p.rows.len()).sum::<usize>(), 4);
    let newest = rows.iter().find(|p| p.key == partition(0).key).unwrap();
    assert_eq!(
        newest.rows[0].cells[0].1.value.as_deref(),
        Some(b"newest".as_slice())
    );
}
#[tokio::test]
async fn verification_nts_range_rejects_pooled() {
    assert!(range_case(&[1, 2, 3, 4, 7], ConsistencyLevel::Quorum)
        .await
        .is_err());
}
#[tokio::test]
async fn verification_nts_range_requires_origin() {
    assert!(range_case(&[1, 4, 5, 6, 7, 8, 9], ConsistencyLevel::Quorum)
        .await
        .is_err());
}
#[tokio::test]
async fn verification_nts_range_local() {
    assert_eq!(
        range_case(&[1, 2], ConsistencyLevel::LocalQuorum)
            .await
            .unwrap()
            .len(),
        4
    );
}
#[tokio::test]
async fn verification_nts_range_configured_scope() {
    assert!(range_case(&[1, 2, 3], ConsistencyLevel::Quorum)
        .await
        .is_err());
}

#[tokio::test]
async fn verification_nts_range_local_rejects_remote_substitution() {
    assert!(
        range_case(&[1, 4, 5, 6, 7, 8, 9], ConsistencyLevel::LocalQuorum)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn verification_nts_range_each_requires_all_configured_dcs() {
    assert!(range_case(&[1, 2, 4, 5], ConsistencyLevel::EachQuorum)
        .await
        .is_err());
    assert_eq!(
        range_case(&[1, 2, 4, 5, 7, 8], ConsistencyLevel::EachQuorum)
            .await
            .unwrap()
            .len(),
        4
    );
}
#[test]
fn verification_nts_range_certificate_requires_every_token_scope() {
    let hosts: Vec<_> = (0..6).map(|_| uuid::Uuid::new_v4()).collect();
    let cert = RangeCertificate {
        strategy: ReplicationStrategy::NetworkTopology {
            dc_rf: [("dc1".into(), 1), ("dc2".into(), 1), ("dc3".into(), 1)].into(),
        },
        cl: ConsistencyLevel::Quorum,
        origin: "dc1".into(),
        local_host: hosts[0],
        scopes: vec![
            vec![
                (hosts[0], "dc1".into()),
                (hosts[1], "dc2".into()),
                (hosts[2], "dc3".into()),
            ],
            vec![
                (hosts[3], "dc1".into()),
                (hosts[4], "dc2".into()),
                (hosts[5], "dc3".into()),
            ],
        ],
    };
    assert!(
        !cert.satisfied(&[hosts[0], hosts[1], hosts[2]].into()),
        "a certificate over one token range cannot cover the other"
    );
    assert!(cert.satisfied(&[hosts[0], hosts[1], hosts[3], hosts[4]].into()));
}

#[test]
fn verification_nts_range_scalar_cannot_shrink_configured_rf() {
    assert_eq!(remote_count_for_cl(ConsistencyLevel::Quorum, 5, 2, 1), 2);
    assert_eq!(remote_count_for_cl(ConsistencyLevel::All, 5, 2, 1), 4);
    assert!(remote_count_for_cl(ConsistencyLevel::Quorum, 0, 0, 0) > 0);
    assert!(remote_count_for_cl(ConsistencyLevel::EachQuorum, 3, 3, 2) > 2);
}
