//! Sprint 6 — multi-DC Raft group plumbing tests.
//!
//! W6.2: `ModeController` carries a `Map<RaftGroupId, Arc<FerrosRaft>>`
//! so multi-DC deployments can run one Raft group per DC. The single-DC
//! shim still works (one entry; `controller.raft()` returns it).
//!
//! These tests exercise the controller's group-map machinery directly
//! using `Arc<FerrosRaft>` instances built by the in-process harness.
//! They do not exercise the full per-DC bootstrap path — that lands in
//! W6.3.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::controller::ModeController;
use ferrosa_cluster::raft::RaftGroupId;
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
        flush_threshold_bytes: 4096,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
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

fn build_controller() -> Arc<ModeController> {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        data_center: "dc1".to_string(),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = uuid::Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);
    // Leak the tempdir for the lifetime of the test by forgetting it —
    // the controller does not need to outlive its disk state in these
    // map-machinery checks.
    std::mem::forget(dir);
    controller
}

/// W6.2 RED: when one DC1 cluster + one DC2 cluster both publish
/// their leader's `Arc<FerrosRaft>` to the controller, the controller
/// holds two entries — one per `RaftGroupId`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_holds_one_raft_per_dc() {
    let dc1_cluster = TestCluster::with_voters(3).await;
    let dc2_cluster = TestCluster::with_voters(3).await;

    let _ = dc1_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc1 leader");
    let _ = dc2_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc2 leader");

    let dc1_raft: Arc<_> = dc1_cluster
        .nodes()
        .iter()
        .next()
        .expect("dc1 has at least one node")
        .raft
        .clone();
    let dc2_raft: Arc<_> = dc2_cluster
        .nodes()
        .iter()
        .next()
        .expect("dc2 has at least one node")
        .raft
        .clone();

    let controller = build_controller();
    // Initially the map is empty.
    assert_eq!(controller.raft_groups().len(), 0);
    assert!(controller.raft().is_none());

    // Install one Raft per DC.
    controller.set_raft_for_dc("dc1", dc1_raft.clone());
    controller.set_raft_for_dc("dc2", dc2_raft.clone());

    let groups = controller.raft_groups();
    assert_eq!(groups.len(), 2, "two per-DC Raft groups installed");
    assert!(groups.contains_key(&RaftGroupId::for_dc("dc1")));
    assert!(groups.contains_key(&RaftGroupId::for_dc("dc2")));

    // Per-DC accessor returns the right instance.
    let dc1_back = controller.raft_for_dc("dc1").expect("dc1 group present");
    assert!(Arc::ptr_eq(&dc1_back, &dc1_raft));

    let dc2_back = controller.raft_for_dc("dc2").expect("dc2 group present");
    assert!(Arc::ptr_eq(&dc2_back, &dc2_raft));

    // controller.raft() is the backward-compat shim. With two groups
    // it must prefer the local DC (configured as "dc1").
    let local = controller.raft().expect("local DC group via shim");
    assert!(Arc::ptr_eq(&local, &dc1_raft));

    dc1_cluster.shutdown().await;
    dc2_cluster.shutdown().await;
}

/// W6.2 RED: single-DC backward-compat — when exactly one Raft group is
/// installed, `controller.raft()` returns it regardless of the
/// configured DC name. Lets existing single-DC tests run unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_raft_shim_works_for_single_dc() {
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");
    let raft = cluster
        .nodes()
        .iter()
        .next()
        .expect("at least one node")
        .raft
        .clone();

    let controller = build_controller();
    // Install under a DC name that is *not* the configured local DC.
    controller.set_raft_for_dc("some-other-dc", raft.clone());

    // Backward-compat: with exactly one group, `.raft()` returns it.
    let back = controller.raft().expect("shim returns the only group");
    assert!(Arc::ptr_eq(&back, &raft));

    cluster.shutdown().await;
}
