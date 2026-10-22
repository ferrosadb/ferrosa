//! A standalone single-node context, built the way `ferrosa`'s `main` builds
//! one, minus every service a lite node does not run.
//!
//! This mirrors `ferrosa_cql::test_util::standalone_for_test` (the in-tree
//! fixture) with one difference that matters for the swap story: the engine is
//! opened with [`ferrosa_storage::StorageEngine::open`], which collects pending
//! commit-log mutations, and those mutations are replayed after the schema
//! snapshot is restored — boot steps 4a/4b of the full node. The fixture uses
//! `StorageEngine::new`, which does not replay, so acknowledged writes that
//! were still in the commit log vanish on restart.

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use ferrosa_schema::{
    AuthMethod, DeploymentMode, LogAuditSink, NodeConfig, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig,
};
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig};

use ferrosa_cql::observability::CqlMetrics;
use ferrosa_cql::prepared::PreparedCache;
use ferrosa_cql::router::SharedState;
use ferrosa_cql::topology::ClientTopologyPolicy;
use ferrosa_cql::virtual_tables::active_queries::QueryTracker;
use ferrosa_cql::virtual_tables::connections::ConnectionTracker;
use ferrosa_cql::virtual_tables::{FullScanTracker, IndexUsageTracker};

/// What one boot did, so a restart can be described rather than asserted.
pub struct BootReport {
    pub restored_tables: usize,
    pub registered_tables: usize,
    pub replayed_mutations: usize,
    pub commitlog_replay: bool,
}

pub fn build(
    data_dir: &Path,
) -> Result<(Arc<SharedState>, BootReport), Box<dyn std::error::Error>> {
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
        cache_hot_window_secs: 900,
    };

    // Boot step 3: an existing commit log is replayed, a fresh volume is not.
    let has_commitlog_segments = std::fs::read_dir(data_dir.join("commitlog"))
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("commitlog-")
            })
        })
        .unwrap_or(false);
    let (engine, pending) = if has_commitlog_segments {
        let (engine, mutations) = StorageEngine::open(engine_config, None)?;
        (Arc::new(engine), mutations)
    } else {
        (Arc::new(StorageEngine::new(engine_config, None)?), Vec::new())
    };
    let replayed = pending.len();

    let host_id = load_or_generate_host_id(data_dir);
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })?,
    );
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema)?;

    let node_config = Arc::new(NodeConfig {
        cluster_name: "ferrosa-lite".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        rpc_port: 9042,
        host_id,
        listen_address: "127.0.0.1".parse().unwrap(),
        listen_port: 7000,
        broadcast_address: "127.0.0.1".parse().unwrap(),
        broadcast_port: 7000,
        rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_port: 9042,
        tokens: vec![],
    });
    let udf_executor = Arc::new(
        ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default())?,
    );
    let mode_controller =
        ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());

    let state = Arc::new(SharedState {
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
                schema: schema.clone(),
                engine: engine.clone(),
            })),
            udf_executor,
            mode_controller,
            auth_warn: false,
            peer_manager: None,
            accord_clock: None,
            accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
        }),
        prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
        param_cache: None,
        connection_tracker: Arc::new(ConnectionTracker::new()),
        query_tracker: Arc::new(QueryTracker::new()),
        full_scan_tracker: Arc::new(FullScanTracker::new()),
        index_usage_tracker: Arc::new(IndexUsageTracker::new()),
        event_sender: tokio::sync::broadcast::channel(64).0,
        last_schema_event: tokio::sync::watch::channel(None).0,
        cql_metrics: Arc::new(CqlMetrics::new()),
        topology_policy: ClientTopologyPolicy::default(),
        txn_registry: Arc::new(parking_lot::Mutex::new(
            ferrosa_cql::txn_registry::TransactionRegistry::default(),
        )),
        result_cursors: Arc::new(ferrosa_cql::result_cursor::ResultCursorRegistry::new(
            ferrosa_cql::result_cursor::ResultCursorConfig::default(),
            host_id,
        )),
    });

    // Boot step 4a/4b: restore the persisted registry snapshot, register its
    // user tables with storage.
    let mut restored_tables = 0usize;
    let mut registered_tables = 0usize;
    if let Some(snapshot) =
        ferrosa_storage::schema_snapshot::SchemaSnapshotStore::new(data_dir).load()?
    {
        restored_tables = snapshot.tables.len();
        for ((_ks, _tbl), meta) in &snapshot.tables {
            if ferrosa_schema::is_system_keyspace(&meta.keyspace) {
                continue;
            }
            engine.register_table(meta.to_storage_schema())?;
            registered_tables += 1;
        }
        schema.schema_ref().store(Arc::new(snapshot));
    }

    // Then replay, so mutations for tables that exist in the restored schema
    // land in memtables instead of being deferred.
    if !pending.is_empty() {
        engine.replay_mutations(pending)?;
    }

    Ok((
        state,
        BootReport {
            restored_tables,
            registered_tables,
            replayed_mutations: replayed,
            commitlog_replay: has_commitlog_segments,
        },
    ))
}

/// `{data_dir}/host_id`, as `ferrosa`'s main writes it. A lite node that
/// regenerated its identity on every start would change `system.local` and any
/// token assignment on each boot, which is not a drop-in swap.
fn load_or_generate_host_id(data_dir: &Path) -> uuid::Uuid {
    let path = data_dir.join("host_id");
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Ok(id) = uuid::Uuid::parse_str(text.trim()) {
            return id;
        }
    }
    let id = uuid::Uuid::new_v4();
    if let Err(err) = std::fs::write(&path, id.to_string()) {
        eprintln!("ferrosa-lite: could not persist host_id at {}: {err}", path.display());
    }
    id
}
