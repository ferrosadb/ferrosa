//! Real Raft/TCP regression for late initial membership topology publication.
use crate::{
    config::ClusterConfig,
    controller::{ModeController, ModeControllerHandles},
    raft::uuid_to_node_id,
};
use ferrosa_net::{
    config::NetConfig,
    peer::{PeerEventListener, PeerManager},
    pool::PriorityPool,
    rpc::{server::RpcServer, HandlerRegistry},
};
use ferrosa_storage::{
    engine::{StorageEngine, StorageEngineConfig},
    CommitLogConfig, CompactionConfig,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use uuid::Uuid;
fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
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
    Arc::new(StorageEngine::new(config, None).unwrap())
}

/// Create a test Schema instance.
fn test_schema() -> Arc<ferrosa_schema::Schema> {
    use ferrosa_schema::{
        AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
        PasswordPolicy, RateLimitConfig, SchemaConfig,
    };
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: SchemaDeploymentMode::Development,
    };
    Arc::new(ferrosa_schema::Schema::new(config).unwrap())
}

/// Create a NetConfig that binds to a random port on localhost.
fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        broadcast_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _: (Uuid, SocketAddr)) {}
    fn on_peer_disconnected(&self, _: (Uuid, SocketAddr)) {}
    fn on_peer_suspected(&self, _: (Uuid, SocketAddr)) {}
    fn on_peer_recovered(&self, _: Uuid) {}
    fn on_peer_failed(&self, _: Uuid) {}
}
struct Node {
    controller: Arc<ModeController>,
    _handles: ModeControllerHandles,
    server: Arc<RpcServer>,
    pm: Arc<PeerManager>,
    id: Uuid,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}
impl Node {
    fn transition(&self, peers: Vec<(Uuid, SocketAddr)>) {
        let _guard = self.controller.transition_guard.lock();
        self.controller.transition_to_cluster(peers);
    }
    async fn start(id: Uuid) -> Self {
        Self::start_with(id, 5, false).await
    }
    async fn start_with(id: Uuid, expected: usize, auto_join: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ClusterConfig {
            expected_cluster_size: expected,
            auto_join,
            num_tokens: 2,
            raft_data_dir: Some(dir.path().join("raft")),
            raft_heartbeat_ms: 100,
            raft_election_timeout_min_ms: 400,
            raft_election_timeout_max_ms: 800,
            ..ClusterConfig::default()
        });
        let net = Arc::new(test_net_config());
        let registry = Arc::new(HandlerRegistry::new());
        let storage = test_storage(dir.path());
        use ferrosa_net::codec::MsgType;
        registry.register(
            MsgType::MutationForward,
            Arc::new(crate::MutationForwardHandler::new(storage.clone())),
        );
        registry.register(
            MsgType::RepairMerkleRequest,
            Arc::new(crate::repair::rpc::RepairMerkleHandler::new(
                storage.clone(),
            )),
        );
        registry.register(
            MsgType::RepairFetchRequest,
            Arc::new(crate::repair::rpc::RepairFetchHandler::new(storage.clone())),
        );
        registry.register(
            MsgType::RepairApplyRequest,
            Arc::new(crate::repair::rpc::RepairApplyHandler::new(storage.clone())),
        );
        let (controller, handles) = ModeController::new(
            config,
            net.clone(),
            id,
            storage,
            test_schema(),
            registry.clone(),
        );
        let pm = Arc::new(PeerManager::new(net.clone(), id, Arc::new(NoopListener)));
        controller.set_peer_manager(pm.clone());
        let server = Arc::new(RpcServer::new((*net).clone(), id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        Self {
            controller,
            _handles: handles,
            server,
            pm,
            id,
            addr,
            _dir: dir,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn verification_membership_late_initial_five_publish_complete_routing() {
    let mut nodes = Vec::new();
    for id in 1..=5u128 {
        nodes.push(Node::start(Uuid::from_u128(id)).await);
    }
    // Dial all peers before explicitly invoking the production transition.
    // The seed's captured transition view is deliberately the original three;
    // connected_peers is the complete view the late-initialization loop sees.
    // auto_join=false prevents reconnect-driven admissions from hiding a
    // missing authoritative seed topology (formation is explicitly configured).
    for source in &nodes {
        let mut connected = Vec::new();
        for target in &nodes {
            if source.id == target.id {
                continue;
            }
            let pool = PriorityPool::connect(
                Arc::new(test_net_config()),
                source.id,
                &target.addr.to_string(),
                None,
                None,
            )
            .await
            .unwrap();
            source.pm.add_peer((target.id, target.addr), pool).await;
            source.controller.register_peer_in_raft_node_map(target.id);
            connected.push((target.id, target.addr));
        }
        source.controller.connected_peers.store(Arc::new(connected));
    }
    for (i, node) in nodes.iter().enumerate() {
        let mut peers = (**node.controller.connected_peers.load()).clone();
        if i == 4 {
            peers.truncate(2);
        }
        node.transition(peers);
    }
    let expected: std::collections::BTreeSet<_> =
        nodes.iter().map(|n| uuid_to_node_id(n.id)).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut ready_with_incomplete_routing;
    let complete = loop {
        ready_with_incomplete_routing = nodes.iter().any(|node| {
            node.controller.accepts_cql_connections()
                && node
                    .controller
                    .token_ring()
                    .is_none_or(|ring| ring.replicas(42, 5).len() != 5)
        });
        if ready_with_incomplete_routing {
            break false;
        }
        let complete = nodes.iter().all(|node| {
            node.controller.accepts_cql_connections()
                && node.controller.token_ring().is_some_and(|ring| {
                    ring.node_ids()
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        == expected
                        && ring.replicas(42, 5).len() == 5
                })
        });
        if complete || tokio::time::Instant::now() >= deadline {
            break complete;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let observed: Vec<_> = nodes
        .iter()
        .map(|node| {
            let voters = node.controller.raft().map(|raft| {
                raft.metrics()
                    .borrow()
                    .membership_config
                    .membership()
                    .voter_ids()
                    .count()
            });
            let ring = node.controller.token_ring();
            (
                node.id,
                voters,
                node.controller.accepts_cql_connections(),
                ring.as_ref().map(|r| r.node_ids().len()),
                ring.as_ref().map(|r| r.replicas(42, 5).len()),
            )
        })
        .collect();
    for node in &nodes {
        node.controller.shutdown().await;
        node.server.shutdown(Duration::from_millis(50)).await;
    }
    assert!(
        complete,
        "five ready voters must have five routable replicas (unsafe admission={ready_with_incomplete_routing}): {observed:?}"
    );
}

async fn mesh(nodes: &[Node]) {
    for source in nodes {
        let mut peers = Vec::new();
        for target in nodes {
            if source.id == target.id {
                continue;
            }
            if !source.pm.has_live_peer(target.id) {
                let pool = PriorityPool::connect(
                    Arc::new(test_net_config()),
                    source.id,
                    &target.addr.to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();
                source.pm.add_peer((target.id, target.addr), pool).await;
            }
            source.controller.register_peer_in_raft_node_map(target.id);
            peers.push((target.id, target.addr));
        }
        source.controller.connected_peers.store(Arc::new(peers));
    }
}

async fn wait_ready(nodes: &[Node], voters: usize) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if nodes.iter().all(|n| {
            n.controller.accepts_cql_connections()
                && n.controller.raft().is_some_and(|r| {
                    r.metrics()
                        .borrow()
                        .membership_config
                        .membership()
                        .voter_ids()
                        .count()
                        == voters
                })
                && n.controller
                    .token_ring()
                    .is_some_and(|r| r.replicas(0, voters).len() == voters)
        }) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn verification_membership_sequential_three_four_five_preserves_rf5_canary() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("ferrosa_cluster::controller=info,ferrosa_cluster::repair=info,ferrosa_cluster::membership=info,ferrosa_cluster::raft=warn")
        .with_test_writer()
        .try_init();
    use crate::{
        consistency::ConsistencyLevel as CL,
        raft::{NodeState, RaftCommand, RaftOp},
        ring::strategy::ReplicationStrategy,
    };
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_schema::metadata::{
        column::{ClusteringOrder, ColumnKind, ColumnMetadata},
        keyspace::{KeyspaceMetadata, ReplicationParams},
        table::TableParams,
    };
    use ferrosa_schema::TableMetadata;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    let mut nodes = Vec::new();
    for id in 3..=5 {
        nodes.push(Node::start_with(Uuid::from_u128(id), 3, true).await);
    }
    mesh(&nodes).await;
    for n in &nodes {
        n.transition((**n.controller.connected_peers.load()).clone());
    }
    assert!(wait_ready(&nodes, 3).await, "initial three must be ready");
    let leader_id = nodes[0]
        .controller
        .raft()
        .unwrap()
        .current_leader()
        .await
        .unwrap();
    let leader_index = nodes
        .iter()
        .position(|n| uuid_to_node_id(n.id) == leader_id)
        .unwrap();
    let raft = nodes[leader_index].controller.raft().unwrap();
    // Real legacy RPC negative control plus benign metadata-forward mirror.
    let legacy_sender = (leader_index + 1) % nodes.len();
    let unknown = crate::raft::NodeInfo {
        host_id: Uuid::from_u128(99),
        addr: "127.0.0.1:9999".into(),
        data_center: nodes[leader_index].controller.config.data_center.clone(),
        rack: "rack1".into(),
        state: NodeState::Normal,
        cql_broadcast: None,
    };
    let denied = crate::raft_forward::forward_raft_command_to_leader(
        &nodes[legacy_sender].pm,
        nodes[leader_index].id,
        RaftCommand {
            op: RaftOp::JoinNode(unknown.clone()),
            schema_version: nodes[leader_index].controller.schema.snapshot().version,
        },
    )
    .await;
    assert!(
        denied.is_err(),
        "legacy RPC cannot bypass verified admission with Normal"
    );
    assert!(nodes[leader_index]
        .controller
        .token_ring()
        .unwrap()
        .get_node(uuid_to_node_id(unknown.host_id))
        .is_none());
    let ddl = crate::pair::ddl::DdlOperation::JoinNode(unknown.clone())
        .to_bytes()
        .unwrap();
    let denied = nodes[legacy_sender]
        .pm
        .send(
            nodes[leader_index].id,
            ferrosa_net::message::Message::PairDdlForward(ddl),
            ferrosa_net::codec::Lane::Data,
        )
        .await;
    assert!(
        denied.is_err(),
        "legacy DDL envelope cannot bypass verified admission"
    );
    let existing = nodes[leader_index]
        .controller
        .token_ring()
        .unwrap()
        .get_node(leader_id)
        .unwrap()
        .clone();
    crate::raft_forward::forward_raft_command_to_leader(
        &nodes[legacy_sender].pm,
        nodes[leader_index].id,
        RaftCommand {
            op: RaftOp::UpdateNodeInfo(existing),
            schema_version: nodes[leader_index].controller.schema.snapshot().version,
        },
    )
    .await
    .expect("benign legacy metadata refresh remains compatible");
    let ks = KeyspaceMetadata {
        name: "grow_ks".into(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".into(),
            options: std::collections::HashMap::from([("replication_factor".into(), "5".into())]),
        },
    };
    let columns = indexmap::IndexMap::from([
        (
            "id".into(),
            ColumnMetadata {
                name: "id".into(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "blob".into(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        ),
        (
            "value".into(),
            ColumnMetadata {
                name: "value".into(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".into(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        ),
    ]);
    let table = TableMetadata {
        keyspace: "grow_ks".into(),
        name: "canary".into(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["id".into()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: Default::default(),
        extensions: Default::default(),
        is_system: false,
    };
    for op in [
        RaftOp::CreateKeyspace(ks),
        RaftOp::CreateTable(Box::new(table)),
    ] {
        raft.client_write(RaftCommand {
            op,
            schema_version: Uuid::new_v4(),
        })
        .await
        .unwrap();
    }
    let table = ferrosa_storage::TableId::new("grow_ks", "canary");
    let key = DecoratedKey::new(PartitionKey::new(b"canonical-growth-key".to_vec()));
    let row = Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(b"before-join".to_vec(), 1000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    };
    let strategy = ReplicationStrategy::Simple {
        replication_factor: 5,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !nodes.iter().all(|n| {
        n.controller
            .schema
            .snapshot()
            .tables
            .contains_key(&("grow_ks".into(), "canary".into()))
    }) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "schema must replicate before data write"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    nodes[leader_index]
        .controller
        .write_path
        .load_full()
        .write(&table, &key, row.clone(), 1000, CL::Quorum, &strategy)
        .await
        .unwrap();
    let mut failure = None;
    for id in [2, 1] {
        nodes.push(Node::start_with(Uuid::from_u128(id), 3, true).await);
        mesh(&nodes).await;
        let newcomer = nodes.last().unwrap();
        newcomer.transition((**newcomer.controller.connected_peers.load()).clone());
        let requester = if id == 1 {
            (leader_index + 1) % 3
        } else {
            leader_index
        };
        nodes[requester]
            .controller
            .trigger_cluster_join(newcomer.id, newcomer.addr, None, None);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let new_data = newcomer
                .controller
                .storage
                .read(&table, &key)
                .ok()
                .flatten();
            let new_state = nodes[leader_index]
                .controller
                .token_ring()
                .and_then(|r| r.get_node(uuid_to_node_id(newcomer.id)).map(|n| n.state));
            if new_state == Some(NodeState::Normal)
                && new_data
                    .as_ref()
                    .is_none_or(|p| p.rows != vec![row.clone()])
            {
                failure = Some(format!(
                    "node{id} marked Normal before verified canary copy"
                ));
                break;
            }
            let voters = nodes[leader_index]
                .controller
                .raft()
                .unwrap()
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .count();
            if voters == nodes.len() && wait_ready(&nodes, nodes.len()).await {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                failure = Some(format!(
                    "growth to{} stalled with{voters} voters, newcomer state{new_state:?}, metrics={:?}, leader_progress={:?}, verified={:?}",
                    nodes.len(), newcomer.controller.raft().map(|r| { let m=r.metrics().borrow().clone(); (m.state,m.current_leader,m.last_applied,m.last_log_index,m.membership_config.membership().voter_ids().count()) }),
                    nodes[leader_index].controller.raft().map(|r| { let m=r.metrics().borrow().clone(); (m.current_leader,m.last_log_index,m.last_applied,m.replication) }),
                    nodes[leader_index].controller.token_ring().map(|r|r.bootstrap_complete(uuid_to_node_id(newcomer.id)))
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if failure.is_some() {
            break;
        }
        for n in &nodes {
            let value = n
                .controller
                .write_path
                .load_full()
                .pk_read(&table, &key, CL::Quorum, &strategy)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                value.rows,
                vec![row.clone()],
                "RF5 QUORUM canary survives admission on{}",
                n.id
            );
        }
    }
    for n in &nodes {
        n.controller.shutdown().await;
        n.server.shutdown(Duration::from_millis(50)).await;
    }
    assert!(
        failure.is_none(),
        "safe sequential growth failed: {failure:?}"
    );
}
