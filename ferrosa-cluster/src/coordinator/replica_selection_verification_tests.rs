use super::*;
use crate::consistency::ConsistencyLevel;
use crate::coordinator::stream_frame_router::StreamFrameRouter;
use crate::coordinator::stream_request_handler::{
    PeerManagerSinkFactory, RangeReadStreamRequestHandler,
};
use crate::coordinator::{ClusterCoordinator, MutationForwardHandler};
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
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::TableId;
use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
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
    engine
        .register_table_with_indexes(schema, vec![("v0_idx".into(), 0)])
        .unwrap();
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
#[derive(Clone, Copy)]
enum Topology {
    Aliases,
    Learners,
    Normal,
    LocalAlias,
}
#[derive(Clone, Copy)]
enum Operation {
    Write,
    Read,
    Range,
    ReadOne,
    Index,
}
async fn exercise(topology: Topology, operation: Operation) -> crate::error::Result<()> {
    let mut dirs = vec![tempfile::tempdir().unwrap()];
    let storage = engine(dirs[0].path());
    if matches!(topology, Topology::LocalAlias) {
        seed_local(&storage, 4);
        let mut p = partition(0);
        p.rows[0].cells[0].1 = CellValue::live(b"local-only".to_vec(), 2000);
        storage
            .write(&TableId::new(KS, TBL), &p.key, p.rows[0].clone(), 2000)
            .unwrap();
    }
    let local = uuid::Uuid::new_v4();
    let peers = Arc::new(PeerManager::new(
        Arc::new(net_config()),
        local,
        Arc::new(NoopListener),
    ));
    let mut servers = Vec::new();
    let mut backs = Vec::new();
    let mut physical = Vec::new();
    let up = if matches!(topology, Topology::Aliases | Topology::LocalAlias) {
        1
    } else {
        2
    };
    for _ in 0..up {
        dirs.push(tempfile::tempdir().unwrap());
        let remote = engine(dirs.last().unwrap().path());
        seed_local(&remote, 4);
        let host = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(
            MsgType::MutationForward,
            Arc::new(MutationForwardHandler::new(remote.clone())),
        );
        registry.register(
            MsgType::IndexReadInPartitionRequest,
            Arc::new(crate::raft::handlers::IndexReadInPartitionHandler::new(
                remote.clone(),
            )),
        );
        registry.register(
            MsgType::ReadRequest,
            Arc::new(crate::raft::handlers::ReadRequestHandler::new(
                remote.clone(),
            )),
        );
        let back = Arc::new(PeerManager::new(
            Arc::new(net_config()),
            host,
            Arc::new(NoopListener),
        ));
        let handler = Arc::new(RangeReadStreamRequestHandler::new(
            Arc::new(remote),
            Arc::new(PeerManagerSinkFactory::new(back.clone())),
            4,
        ));
        registry.register(MsgType::RangeReadStreamRequest, handler.clone());
        registry.register(MsgType::RangeReadStreamCancel, handler);
        let server = Arc::new(RpcServer::new(net_config(), host, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        peers.ensure_peer(host, &addr.to_string()).await.unwrap();
        physical.push((host, addr.to_string()));
        servers.push(server);
        backs.push(back);
    }
    physical.push((uuid::Uuid::new_v4(), "127.0.0.1:1".into()));
    let mut ring = TokenRing::new();
    let mut origin = ring_node(local, "127.0.0.1:2");
    origin.state = NodeState::Joining;
    ring.add_node(1, origin);
    for id in 2..=4u64 {
        let index = if matches!(topology, Topology::LocalAlias) {
            0
        } else if matches!(topology, Topology::Aliases) {
            if id < 4 {
                0
            } else {
                1
            }
        } else {
            (id - 2) as usize
        };
        let (host, addr) = &physical[index];
        let mut node = if matches!(topology, Topology::LocalAlias) && id < 4 {
            ring_node(local, "127.0.0.1:2")
        } else {
            ring_node(*host, addr)
        };
        if matches!(topology, Topology::Learners) && id < 4 {
            node.state = NodeState::Learner { owns_tokens: true };
        }
        ring.add_node(id, node);
        ring.assign_tokens(id, &[partition(0).key.token.0.saturating_add(id as i64)]);
    }
    let coordinator = Arc::new(
        ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            if matches!(topology, Topology::LocalAlias) {
                3
            } else {
                1
            },
            storage,
            3,
            ConsistencyLevel::Quorum,
        )
        .with_hint_store(Arc::new(
            crate::hints::HintStore::new(crate::hints::HintConfig {
                dir: dirs[0].path().join("hints"),
                ..crate::hints::HintConfig::default()
            })
            .unwrap(),
        )),
    );
    let registry = Arc::new(HandlerRegistry::new());
    let router = Arc::new(StreamFrameRouter::new(coordinator.stream_router()));
    registry.register(MsgType::RangeReadStreamChunk, router.clone());
    registry.register(MsgType::RangeReadStreamHeartbeat, router.clone());
    registry.register(MsgType::RangeReadStreamDone, router);
    let server = Arc::new(RpcServer::new(net_config(), local, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    for back in backs {
        back.ensure_peer(local, &addr.to_string()).await.unwrap();
    }
    let table = TableId::new(KS, TBL);
    let key = partition(0).key;
    let strategy = ReplicationStrategy::NetworkTopology {
        dc_rf: [("dc1".into(), 3)].into(),
    };
    let permits = coordinator.write_semaphore.available_permits();
    let result = async {
        match operation {
            Operation::Write => {
                coordinator
                    .coordinate_write_nts(
                        &table,
                        &key,
                        partition(0).rows.remove(0),
                        3000,
                        ConsistencyLevel::Quorum,
                        &strategy,
                    )
                    .await?
            }
            Operation::Read => assert_eq!(
                coordinator
                    .coordinate_read_nts(&table, &key, ConsistencyLevel::Quorum, &strategy)
                    .await?
                    .unwrap()
                    .len(),
                1
            ),
            Operation::ReadOne => {
                let rows = coordinator
                    .coordinate_read_nts(&table, &key, ConsistencyLevel::One, &strategy)
                    .await?
                    .unwrap();
                let expected = if matches!(topology, Topology::LocalAlias) {
                    b"local-only".as_slice()
                } else {
                    b"a-0".as_slice()
                };
                assert_eq!(rows[0].cells[0].1.value.as_deref(), Some(expected));
            }
            Operation::Index => {
                let value = if matches!(topology, Topology::LocalAlias) {
                    b"local-only".as_slice()
                } else {
                    b"a-0".as_slice()
                };
                let rows = coordinator
                    .coordinate_index_read_in_partition(
                        &table,
                        &key,
                        "v0_idx",
                        &ferrosa_index::IndexKey(value.to_vec()),
                        ConsistencyLevel::Quorum,
                        &strategy,
                    )
                    .await?;
                assert_eq!(rows.len(), 1);
            }
            Operation::Range => {
                let mut stream = WritePath::cluster(coordinator.clone())
                    .range_read_stream_all_with(&table, 0, ConsistencyLevel::Quorum, &strategy)
                    .await?;
                let mut count = 0;
                while let Some(partition) = stream.next().await {
                    count += partition?.rows.len();
                }
                assert_eq!(count, 4);
            }
        }
        Ok(())
    }
    .await;
    let _drained = tokio::time::timeout(
        Duration::from_secs(5),
        coordinator
            .write_semaphore
            .clone()
            .acquire_many_owned(permits as u32),
    )
    .await
    .expect("post-quorum writes must finish before cleanup")
    .unwrap();
    for remote in servers {
        remote.shutdown(Duration::from_millis(10)).await;
    }
    server.shutdown(Duration::from_millis(10)).await;
    result
}
#[tokio::test]
async fn verification_replica_identity_write_alias_cannot_supply_two_votes() {
    assert!(exercise(Topology::Aliases, Operation::Write).await.is_err());
}
#[tokio::test]
async fn verification_replica_identity_read_alias_cannot_supply_two_votes() {
    assert!(exercise(Topology::Aliases, Operation::Read).await.is_err());
}
#[tokio::test]
async fn verification_replica_identity_range_alias_control() {
    assert!(exercise(Topology::Aliases, Operation::Range).await.is_err());
}
#[tokio::test]
async fn verification_replica_identity_write_learners_cannot_supply_quorum() {
    assert!(exercise(Topology::Learners, Operation::Write)
        .await
        .is_err());
}
#[tokio::test]
async fn verification_replica_identity_range_learners_cannot_supply_quorum() {
    assert!(exercise(Topology::Learners, Operation::Range)
        .await
        .is_err());
}
#[tokio::test]
async fn verification_replica_identity_read_learners_control() {
    assert!(exercise(Topology::Learners, Operation::Read).await.is_err());
}
#[tokio::test]
async fn verification_replica_identity_two_distinct_voters_control() {
    for operation in [
        Operation::Write,
        Operation::Read,
        Operation::Range,
        Operation::ReadOne,
        Operation::Index,
    ] {
        exercise(Topology::Normal, operation).await.unwrap();
    }
}
#[test]
fn verification_replica_identity_conflicting_dc_labels_fail_closed() {
    let host = uuid::Uuid::new_v4();
    let mut ring = TokenRing::new();
    let first = ring_node(host, "127.0.0.1:1");
    let mut alias = first.clone();
    alias.data_center = "dc2".into();
    ring.add_node(1, first);
    ring.add_node(2, alias);
    assert!(eligible_replicas_for_cl(ConsistencyLevel::Quorum, &[1, 2], &ring).is_empty());
}

#[tokio::test]
async fn verification_replica_identity_local_alias_write_uses_local_storage() {
    exercise(Topology::LocalAlias, Operation::Write)
        .await
        .unwrap();
}
#[tokio::test]
async fn verification_replica_identity_local_alias_read_uses_local_storage() {
    exercise(Topology::LocalAlias, Operation::Read)
        .await
        .unwrap();
}
#[tokio::test]
async fn verification_replica_identity_local_alias_range_control() {
    exercise(Topology::LocalAlias, Operation::Range)
        .await
        .unwrap();
}

#[tokio::test]
async fn verification_replica_identity_local_alias_one_reads_local_value() {
    exercise(Topology::LocalAlias, Operation::ReadOne)
        .await
        .unwrap();
}
#[tokio::test]
async fn verification_replica_identity_local_alias_index_uses_local_storage() {
    exercise(Topology::LocalAlias, Operation::Index)
        .await
        .unwrap();
}
