//! P0-2 / P0-4 / P0-5: no replica-ownership change without verified data
//! movement.
//!
//! Each test here pins one membership transition that used to commit without
//! checking that the data actually moved:
//!
//! - `downgrade_to_pair` left Raft running, so the node was still a voter and a
//!   replica while it committed point-to-point (split brain).
//! - decommission committed `LeaveNode` even when streaming failed, and
//!   streamed only the partitions the leaving node was PRIMARY for.
//! - the restart promote pass moved every `Joining` member to `Normal`,
//!   including one whose bootstrap never completed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::controller::ModeController;
use ferrosa_net::config::NetConfig;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_schema::{
    AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
    PasswordPolicy, RateLimitConfig, Schema, SchemaConfig,
};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use async_trait::async_trait;
use ferrosa_cluster::controller::data_movement::{decommission_verified, PartitionStreamer};
use ferrosa_cluster::raft::{uuid_to_node_id, NodeInfo, NodeState, RaftCommand, RaftOp};
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use futures::StreamExt;

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

fn build_controller(dir: &std::path::Path, local_id: uuid::Uuid) -> Arc<ModeController> {
    let config = Arc::new(ClusterConfig {
        data_center: "dc1".to_string(),
        ..ClusterConfig::default()
    });
    let (controller, _handles) = ModeController::new(
        config,
        Arc::new(NetConfig::default()),
        local_id,
        test_storage(dir),
        test_schema(),
        Arc::new(HandlerRegistry::new()),
    );
    controller
}

/// P0-5: an operator downgrade is refused while this node still runs Raft.
///
/// While Raft runs the node is a voter and a replica. Installing the pair
/// write path on top lets it commit point-to-point writes a quorum never saw,
/// while the rest of the cluster still counts it: split brain. There is no
/// supported way to stop Raft and shrink membership to the named peer yet, so
/// the action must refuse rather than pretend.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgrade_to_pair_is_refused_while_raft_is_running() {
    let cluster = TestCluster::with_voters(1).await;
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("single-voter leader");
    let raft = cluster.leader_node().raft.clone();

    let dir = tempfile::tempdir().unwrap();
    let controller = build_controller(dir.path(), uuid::Uuid::new_v4());
    controller.set_mode_for_test(ferrosa_common::deployment_mode::DeploymentMode::Cluster);
    controller.set_raft_for_dc("dc1", raft);

    let err = controller
        .downgrade_to_pair(Some(uuid::Uuid::new_v4()))
        .expect_err("a downgrade while Raft runs must be refused");

    assert!(
        err.to_string().contains("Raft is running"),
        "the refusal must say Raft is still running, got: {err}"
    );
    assert_eq!(
        controller.mode(),
        ferrosa_common::deployment_mode::DeploymentMode::Cluster,
        "a refused downgrade must leave the node untouched"
    );
    cluster.shutdown().await;
}

fn member(host_id: uuid::Uuid) -> NodeInfo {
    NodeInfo {
        host_id,
        addr: "127.0.0.1:7000".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
}

fn partition(token: i64, key: &[u8]) -> Partition {
    Partition {
        key: DecoratedKey {
            token: Token(token),
            key: PartitionKey::new(key.to_vec()),
        },
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"v".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }],
    }
}

struct Streamer {
    fail: bool,
}

#[async_trait]
impl PartitionStreamer for Streamer {
    async fn stream(
        &self,
        _target: u64,
        _table: &TableId,
        partitions: Vec<Partition>,
    ) -> Result<u64, String> {
        if self.fail {
            Err("receiver rejected the stream: checksum mismatch".into())
        } else {
            Ok(partitions.len() as u64)
        }
    }
}

/// Three committed members with one token each, plus the ring the
/// decommission plans from. Returns `(cluster, leaving_node_id, ring)`.
async fn three_member_cluster() -> (TestCluster, u64, TokenRing) {
    let cluster = TestCluster::with_voters(1).await;
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("single-voter leader");
    let raft = cluster.leader_node().raft.clone();
    let mut ring = TokenRing::new();
    let mut ids = Vec::new();
    for i in 1..=3u128 {
        let host = uuid::Uuid::from_u128(i);
        let id = uuid_to_node_id(host);
        raft.client_write(RaftCommand {
            op: RaftOp::JoinNode(member(host)),
            schema_version: uuid::Uuid::new_v4(),
        })
        .await
        .expect("JoinNode commits");
        ring.add_node(id, member(host));
        ids.push(id);
    }
    ids.sort_unstable();
    // Explicit tokens so the replica sets are known: ids[0] owns 100.
    for (i, id) in ids.iter().enumerate() {
        ring.assign_tokens(*id, &[(i as i64 + 1) * 100]);
    }
    (cluster, ids[0], ring)
}

async fn member_state(cluster: &TestCluster, node_id: u64) -> Option<NodeState> {
    cluster
        .leader_node()
        .state_snapshot()
        .await
        .members
        .get(&node_id)
        .map(|m| m.state)
}

/// P0-2, end to end through a real Raft group: a stream that fails leaves the
/// node COMMITTED as `Leaving` (still a member, LeaveNode never applied), and
/// the same decommission with a verified stream removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_decommission_stream_leaves_the_node_committed_as_leaving() {
    let (cluster, leaving, ring) = three_member_cluster().await;
    let raft = cluster.leader_node().raft.clone();
    let tables = [(
        TableId::new("ks", "t"),
        ReplicationStrategy::Simple {
            replication_factor: 2,
        },
    )];
    let scan = |_: &TableId| {
        futures::stream::iter(vec![Ok(partition(50, b"a")), Ok(partition(60, b"b"))]).boxed()
    };

    let err = decommission_verified(
        raft.as_ref(),
        &ring,
        leaving,
        &tables,
        scan,
        &Streamer { fail: true },
    )
    .await
    .expect_err("a failed stream must abort the decommission");
    assert!(err.to_string().contains("checksum mismatch"), "{err}");
    assert_eq!(
        member_state(&cluster, leaving).await,
        Some(NodeState::Leaving),
        "LeaveNode must not have committed; the node stays a Leaving member"
    );

    let evidence = decommission_verified(
        raft.as_ref(),
        &ring,
        leaving,
        &tables,
        scan,
        &Streamer { fail: false },
    )
    .await
    .expect("a verified transfer decommissions");
    assert_eq!(evidence.partitions, 2);
    assert_eq!(
        member_state(&cluster, leaving).await,
        None,
        "after a verified transfer LeaveNode commits and the member is gone"
    );
    cluster.shutdown().await;
}

/// P0-2: decommission streams the LOCAL node's data, so it refuses to run for
/// another host. The old code streamed this node's copies and then removed
/// the other node, calling its data moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decommission_of_a_remote_host_is_refused() {
    let cluster = TestCluster::with_voters(1).await;
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("single-voter leader");
    let dir = tempfile::tempdir().unwrap();
    let controller = build_controller(dir.path(), uuid::Uuid::new_v4());
    controller.set_raft_for_dc("dc1", cluster.leader_node().raft.clone());

    let remote = uuid::Uuid::new_v4();
    let err = controller
        .initiate_decommission(remote)
        .await
        .expect_err("a remote decommission must be refused");

    assert!(
        err.to_string().contains("must run on that node"),
        "the refusal must say why, got: {err}"
    );
    assert!(
        !cluster
            .leader_node()
            .state_snapshot()
            .await
            .members
            .contains_key(&uuid_to_node_id(remote)),
        "nothing may be proposed for the remote node"
    );
    cluster.shutdown().await;
}
