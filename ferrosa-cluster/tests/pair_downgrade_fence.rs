//! t_47bbeb66: a cluster dissolved into a pair must stay dissolved.
//!
//! Ben's flow (t_ad872ac7): the operator takes a node down, then downgrades
//! naming the peer. Stopping Raft on the downgrading node alone left the named
//! peer running Raft, so the member that was taken down could come back and
//! form a Raft majority with the peer, committing beside the pair. These tests
//! drive that scenario through a real Raft group.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::raft_harness::TestCluster;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::controller::dissolution::{
    DissolutionState, PairDissolveRequest, PairDissolveTransport,
};
use ferrosa_cluster::controller::ModeController;
use ferrosa_cluster::error::{ClusterError, Result};
use ferrosa_cluster::raft::{uuid_to_node_id, NodeInfo, NodeState, RaftCommand, RaftOp};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_common::deployment_mode::DeploymentMode;
use ferrosa_net::config::NetConfig;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_schema::{
    AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
    PasswordPolicy, RateLimitConfig, Schema, SchemaConfig,
};
use ferrosa_storage::engine::StorageEngine;
use uuid::Uuid;

fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
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
    Arc::new(StorageEngine::new(config, None).unwrap())
}

fn test_schema() -> Arc<Schema> {
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: SchemaDeploymentMode::Development,
    };
    Arc::new(Schema::new(config).unwrap())
}

/// The host id whose openraft node id is `node_id` (the harness numbers its
/// voters 1..=N; `uuid_to_node_id` reads bytes 8..16 little-endian).
fn host_for(node_id: u64) -> Uuid {
    let mut bytes = [0xA5u8; 16];
    bytes[8..].copy_from_slice(&node_id.to_le_bytes());
    let host = Uuid::from_bytes(bytes);
    assert_eq!(uuid_to_node_id(host), node_id);
    host
}

fn member(host_id: Uuid) -> NodeInfo {
    NodeInfo {
        host_id,
        addr: "127.0.0.1:7000".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
}

/// A cluster-mode controller for harness voter `node_id`, with its Raft group,
/// a peer manager, the committed ring of all `voters`, and `up` as the
/// connected peers. Its Raft directory is `dir/raft`, where the dissolution
/// marker lives.
fn controller_for(
    cluster: &TestCluster,
    node_id: u64,
    voters: u64,
    up: &[u64],
    dir: &std::path::Path,
) -> Arc<ModeController> {
    let config = Arc::new(ClusterConfig {
        data_center: "dc1".to_string(),
        raft_data_dir: Some(dir.join("raft")),
        ..ClusterConfig::default()
    });
    let (controller, _handles) = ModeController::new(
        config,
        Arc::new(NetConfig::default()),
        host_for(node_id),
        test_storage(dir),
        test_schema(),
        Arc::new(HandlerRegistry::new()),
    );
    controller.set_mode_for_test(DeploymentMode::Cluster);
    controller.set_raft_for_dc(
        "dc1",
        cluster.raft_for_node_id(node_id).expect("harness voter"),
    );
    controller.set_peer_manager(Arc::new(ferrosa_net::peer::PeerManager::new(
        Arc::new(NetConfig::default()),
        controller.host_id(),
        controller.clone(),
    )));
    let mut ring = TokenRing::new();
    for id in 1..=voters {
        ring.add_node(id, member(host_for(id)));
        ring.assign_tokens(id, &[id as i64 * 100]);
    }
    controller.set_token_ring(Arc::new(ring));
    let addr: std::net::SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.set_connected_peers_for_test(up.iter().map(|&id| (host_for(id), addr)).collect());
    controller
}

/// A harness cluster of `n` voters in which EVERY voter has applied the
/// membership. A follower that has not yet seen it reports no voters, and the
/// dissolution check (correctly) refuses on that view.
async fn formed(n: usize) -> TestCluster {
    let cluster = TestCluster::with_voters(n).await;
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let all_see_membership = (1..=n as u64).all(|id| {
            let raft = cluster.raft_for_node_id(id).expect("harness voter");
            let seen = raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .count();
            seen == n
        });
        if all_see_membership {
            return cluster;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "not every voter applied the {n}-voter membership within 10s"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Phase 1 delivered in-process: the requester's call lands on the named
/// peer's controller exactly as the internode handler would deliver it.
struct InProcess {
    peer: Arc<ModeController>,
}

#[async_trait]
impl PairDissolveTransport for InProcess {
    async fn request(&self, req: PairDissolveRequest) -> Result<()> {
        self.peer.accept_pair_dissolution(req).await
    }
}

struct Unreachable;

#[async_trait]
impl PairDissolveTransport for Unreachable {
    async fn request(&self, req: PairDissolveRequest) -> Result<()> {
        Err(ClusterError::ModeTransitionRejected(format!(
            "peer {} unreachable",
            req.peer
        )))
    }
}

/// Watch voters `ids` for `window`: if any of them becomes leader and can
/// COMMIT, the old cluster is alive beside the pair. Returns the committing
/// node, if any. Bounded, as every negative liveness check must be: ten-plus
/// election timeouts of the harness (200-400 ms).
async fn first_commit_among(cluster: &TestCluster, ids: &[u64], window: Duration) -> Option<u64> {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        for &id in ids {
            let raft = cluster.raft_for_node_id(id).expect("harness voter");
            if raft.metrics().borrow().current_leader != Some(id) {
                continue;
            }
            let write = raft.client_write(RaftCommand {
                op: RaftOp::ApproveNode {
                    host_id: Uuid::new_v4(),
                },
                schema_version: Uuid::new_v4(),
            });
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(1), write).await {
                return Some(id);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// The returning-member scenario. Three voters A=1, B=2, C=3. The operator
/// takes C down and downgrades A naming B. Then C comes back. Nothing from the
/// old Raft group may commit again: before the fence, B (still running Raft)
/// and C formed a majority and committed beside the pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_member_cannot_form_a_majority_beside_the_pair() {
    let cluster = formed(3).await;
    cluster.isolate_by_node_id(3);
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = controller_for(&cluster, 1, 3, &[2], dir_a.path());
    let b = controller_for(&cluster, 2, 3, &[1], dir_b.path());
    a.set_pair_dissolve_transport_for_test(Arc::new(InProcess { peer: b.clone() }));

    a.downgrade_to_pair(Some(host_for(2)))
        .await
        .expect("node down + explicit named downgrade must be accepted");

    // C comes back.
    cluster.heal();
    let committed = first_commit_among(&cluster, &[1, 2, 3], Duration::from_secs(5)).await;
    assert_eq!(
        committed, None,
        "the old Raft group committed on node {committed:?} beside the pair: split brain"
    );

    // The pair nodes' Raft groups are shut down, not merely forgotten.
    for id in [1, 2] {
        let write = cluster
            .raft_for_node_id(id)
            .expect("harness voter")
            .client_write(RaftCommand {
                op: RaftOp::ApproveNode {
                    host_id: Uuid::new_v4(),
                },
                schema_version: Uuid::new_v4(),
            })
            .await;
        assert!(write.is_err(), "node {id}'s Raft group must be shut down");
    }

    // Both pair nodes hold the durable marker and run no Raft.
    for (name, node) in [("A", &a), ("B", &b)] {
        assert!(
            matches!(node.dissolution_state(), DissolutionState::Dissolved(_)),
            "{name} must record the dissolution durably"
        );
        assert!(node.raft().is_none(), "{name} must hold no Raft group");
        assert_eq!(
            node.mode(),
            DeploymentMode::Pair,
            "{name} is half of the pair"
        );
        // The pair machinery is installed, not just the mode label.
        assert!(node.role().is_some(), "{name} must have a pair role");
        assert_eq!(
            node.ddl_path_kind(),
            "pair",
            "{name} must use the pair DDL path"
        );
    }
    assert_ne!(a.role(), b.role(), "one primary and one secondary");
    cluster.shutdown().await;
}

/// A restarted pair node must not rebuild Raft from its persisted log: the
/// marker makes it start Standalone (not as a returning cluster member) and
/// admit only its partner. A former member that reconnects is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_pair_node_stays_out_of_raft_and_refuses_former_members() {
    let cluster = formed(3).await;
    cluster.isolate_by_node_id(3);
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = controller_for(&cluster, 1, 3, &[2], dir_a.path());
    let b = controller_for(&cluster, 2, 3, &[1], dir_b.path());
    a.set_pair_dissolve_transport_for_test(Arc::new(InProcess { peer: b.clone() }));
    a.downgrade_to_pair(Some(host_for(2)))
        .await
        .expect("downgrade");

    // "Restart" B: a fresh controller over the same Raft directory. The
    // cluster-member marker would make it a returning member; the dissolution
    // marker must win.
    std::fs::create_dir_all(dir_b.path().join("raft").join("dc1")).unwrap();
    ferrosa_common::deployment_mode::DeploymentMode::record_cluster_membership(
        &dir_b.path().join("raft").join("dc1"),
    )
    .unwrap();
    let config = Arc::new(ClusterConfig {
        data_center: "dc1".to_string(),
        raft_data_dir: Some(dir_b.path().join("raft")),
        ..ClusterConfig::default()
    });
    let (restarted, _handles) = ModeController::new(
        config,
        Arc::new(NetConfig::default()),
        host_for(2),
        test_storage(&dir_b.path().join("restart")),
        test_schema(),
        Arc::new(HandlerRegistry::new()),
    );
    assert!(
        matches!(
            restarted.dissolution_state(),
            DissolutionState::Dissolved(_)
        ),
        "the marker must survive a restart"
    );
    assert_eq!(
        restarted.mode(),
        DeploymentMode::Standalone,
        "a dissolved node must not come back as a returning cluster member"
    );
    assert!(
        restarted.raft_start_refusal().is_some(),
        "a dissolved node must refuse to start Raft"
    );
    use ferrosa_net::peer::PeerEventListener;
    let addr: std::net::SocketAddr = "127.0.0.1:7000".parse().unwrap();
    restarted.on_peer_connected((host_for(3), addr));
    assert!(
        restarted.connected_peers_for_test().is_empty(),
        "a former member must not be admitted as a peer"
    );
    cluster.shutdown().await;
}

/// The members taken down must not be a Raft majority on their own: with five
/// voters and three down, those three could re-form the cluster whatever the
/// pair does. Refused, with Raft still running on the requester.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downgrade_whose_downed_members_hold_a_majority_is_refused() {
    let cluster = formed(5).await;
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = controller_for(&cluster, 1, 5, &[2], dir_a.path());
    let b = controller_for(&cluster, 2, 5, &[1], dir_b.path());
    a.set_pair_dissolve_transport_for_test(Arc::new(InProcess { peer: b.clone() }));

    let err = a
        .downgrade_to_pair(Some(host_for(2)))
        .await
        .expect_err("three downed voters of five are a majority");

    assert!(err.to_string().contains("majority"), "{err}");
    assert!(a.raft().is_some() && b.raft().is_some(), "nothing may stop");
    assert_eq!(a.dissolution_state(), DissolutionState::None);
    assert_eq!(b.dissolution_state(), DissolutionState::None);
    cluster.shutdown().await;
}

/// Two-phase: if the named peer does not confirm it stopped Raft, the
/// requester changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downgrade_the_peer_does_not_confirm_changes_nothing() {
    let cluster = formed(3).await;
    let dir_a = tempfile::tempdir().unwrap();
    let a = controller_for(&cluster, 1, 3, &[2], dir_a.path());
    a.set_pair_dissolve_transport_for_test(Arc::new(Unreachable));

    let err = a
        .downgrade_to_pair(Some(host_for(2)))
        .await
        .expect_err("an unconfirmed phase 1 must refuse");

    assert!(err.to_string().contains("unreachable"), "{err}");
    assert_eq!(a.mode(), DeploymentMode::Cluster);
    assert!(a.raft().is_some(), "Raft must still run on the requester");
    assert_eq!(a.dissolution_state(), DissolutionState::None);
    cluster.shutdown().await;
}
