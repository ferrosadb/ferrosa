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
