//! Test-only helpers for constructing a standalone [`SharedState`].
//!
//! Gated behind the `test-util` feature so external crates (e.g.
//! `ferrosa-flight`) can build a real, single-node execution context in their
//! integration tests without duplicating the ~80-line fixture. Not compiled
//! into normal builds.

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use ferrosa_schema::{
    AuthMethod, DeploymentMode, LogAuditSink, NodeConfig, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig,
};
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig};

use crate::observability::CqlMetrics;
use crate::prepared::PreparedCache;
use crate::router::SharedState;
use crate::topology::ClientTopologyPolicy;
use crate::virtual_tables::active_queries::QueryTracker;
use crate::virtual_tables::connections::ConnectionTracker;
use crate::virtual_tables::{FullScanTracker, IndexUsageTracker};

/// Build a standalone (single-node, `Direct` write/DDL path) `SharedState`
/// rooted at `data_dir`. Auth is disabled; DML/DDL execute against a real local
/// `StorageEngine`. The caller owns `data_dir` (e.g. a `tempfile::TempDir`).
pub fn standalone_for_test(data_dir: &Path) -> Arc<SharedState> {
    let commit_log = CommitLogConfig {
        log_dir: data_dir.join("commitlog"),
        checkpoint_dir: data_dir.join("commitlog"),
        archive: None,
        ..CommitLogConfig::default()
    };
    let engine_config = StorageEngineConfig {
        commit_log,
        compaction: CompactionConfig::from_env(data_dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: data_dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    let node_config = Arc::new(NodeConfig {
        cluster_name: "test".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        rpc_port: 9042,
        host_id: uuid::Uuid::new_v4(),
        listen_address: "127.0.0.1".parse().unwrap(),
        listen_port: 7000,
        broadcast_address: "127.0.0.1".parse().unwrap(),
        broadcast_port: 7000,
        rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_port: 9042,
        tokens: vec![],
    });
    let udf_executor =
        Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());
    let mode_controller =
        ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());
    Arc::new(SharedState {
        core: Arc::new(ferrosa_session::SessionCore {
            engine: engine.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(
                ferrosa_cluster::ClusterStateHolder::Standalone,
            )),
            write_path: Arc::new(ArcSwap::from_pointee(ferrosa_cluster::WritePath::direct(
                engine.clone(),
            ))),
            ddl_path: Arc::new(ArcSwap::from_pointee(ferrosa_cluster::DdlPath::Direct {
                schema,
                engine,
            })),
            udf_executor,
            mode_controller,
            auth_warn: false,
            peer_manager: None,
            accord_clock: None,
            accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
        }),
        prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
        connection_tracker: Arc::new(ConnectionTracker::new()),
        query_tracker: Arc::new(QueryTracker::new()),
        full_scan_tracker: Arc::new(FullScanTracker::new()),
        index_usage_tracker: Arc::new(IndexUsageTracker::new()),
        event_sender: tokio::sync::broadcast::channel(64).0,
        last_schema_event: tokio::sync::watch::channel(None).0,
        cql_metrics: Arc::new(CqlMetrics::new()),
        topology_policy: ClientTopologyPolicy::default(),
        txn_registry: std::sync::Arc::new(parking_lot::Mutex::new(
            crate::txn_registry::TransactionRegistry::default(),
        )),
    })
}
