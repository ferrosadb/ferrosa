//! TDD regression test for the cluster-replication bug in adjacency keyspace
//! creation.
//!
//! See `specs/in-process/bug-system-graph-ks-not-replicated-on-write-path.md`.
//!
//! Pre-fix behaviour: `GraphEngine::new` calls
//! `Schema::create_keyspace_internal` / `create_table_internal` directly for
//! the auto-generated `system_graph_<ks>` keyspace. Those methods only
//! mutate the local schema registry, so on a multi-node cluster only the
//! coordinator node ever registers the adjacency table. Replicas reject
//! adjacency `MutationForward` writes with "table not registered" and
//! every graph-edge mutation hangs until the coordinator's per-replica
//! timeout, then surfaces as `operation timed out` to the client.
//!
//! Post-fix behaviour: graph-engine-driven DDL goes through a
//! `GraphSchemaCoordinator` injected at construction time. A
//! cluster-aware coordinator routes the DDL through the same replication
//! path as regular CQL DDL, so every replica registers the adjacency
//! table and the MutationForward writes land.
//!
//! This test pins the contract that `GraphSchemaCoordinator` is the only
//! path through which the engine creates adjacency schema. A recording
//! coordinator captures the engine's DDL calls; if the engine bypasses it
//! (the pre-fix behaviour), the recording is empty and the test fails.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use tempfile::tempdir;

use async_trait::async_trait;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_graph::engine::{GraphEngine, GraphSchemaCoordinator, LocalGraphSchemaCoordinator};
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode, EnvSecretsProvider,
    PasswordHasher, PasswordPolicy, RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum DdlEvent {
    CreateKeyspace(String),
    CreateTable(String, String),
}

/// Test double that records every DDL operation the graph engine issues
/// through its `GraphSchemaCoordinator`. Inner coordinator still applies
/// the DDL to the local schema (so the engine's downstream paths see the
/// adjacency table) — the recording is just an observation hook on top.
struct RecordingCoordinator {
    inner: LocalGraphSchemaCoordinator,
    events: Mutex<Vec<DdlEvent>>,
}

impl RecordingCoordinator {
    fn new(schema: Arc<Schema>) -> Self {
        Self {
            inner: LocalGraphSchemaCoordinator::new(schema),
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<DdlEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl GraphSchemaCoordinator for RecordingCoordinator {
    async fn apply_create_keyspace(
        &self,
        ks: KeyspaceMetadata,
    ) -> ferrosa_graph::error::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(DdlEvent::CreateKeyspace(ks.name.clone()));
        self.inner.apply_create_keyspace(ks).await
    }

    async fn apply_create_table(&self, table: TableMetadata) -> ferrosa_graph::error::Result<()> {
        self.events.lock().unwrap().push(DdlEvent::CreateTable(
            table.keyspace.clone(),
            table.name.clone(),
        ));
        self.inner.apply_create_table(table).await
    }
}

fn simple_strategy_rf1() -> ReplicationParams {
    ReplicationParams {
        strategy: "SimpleStrategy".to_string(),
        options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
    }
}

fn build_edge_table_metadata(keyspace: &str, table: &str, label: &str) -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "tenant_id".to_string(),
        ColumnMetadata {
            name: "tenant_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    columns.insert(
        "session_id".to_string(),
        ColumnMetadata {
            name: "session_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 1,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    columns.insert(
        "src_id".to_string(),
        ColumnMetadata {
            name: "src_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "edge_type".to_string(),
        ColumnMetadata {
            name: "edge_type".to_string(),
            kind: ColumnKind::Clustering,
            position: 1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "dst_id".to_string(),
        ColumnMetadata {
            name: "dst_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 2,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "weight".to_string(),
        ColumnMetadata {
            name: "weight".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "float".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), "edge".to_string());
    extensions.insert("graph.label".to_string(), label.to_string());
    TableMetadata {
        keyspace: keyspace.to_string(),
        name: table.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
        clustering_key: vec![
            ("src_id".to_string(), ClusteringOrder::Asc),
            ("edge_type".to_string(), ClusteringOrder::Asc),
            ("dst_id".to_string(), ClusteringOrder::Asc),
        ],
        params: TableParams::default(),
        flags: HashSet::from([TableFlag::Compound]),
        extensions,
        is_system: false,
    }
}

fn test_schema() -> Schema {
    Schema::new(SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap()
}

fn test_storage_engine(dir: &std::path::Path) -> StorageEngine {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    StorageEngine::new(config, None).unwrap()
}

/// At engine startup, the auto-created `system_graph_<ks>.adjacency` schema
/// must flow through the injected `GraphSchemaCoordinator`. The recording
/// coordinator captures the operation; pre-fix the engine called
/// `Schema::create_*_internal` directly and bypassed the coordinator
/// entirely, leaving cluster replicas with no idea the table exists.
/// The graph engine's lazy adjacency-registration path (invoked from
/// every `execute_with_params`) must route DDL through the injected
/// `GraphSchemaCoordinator`. On a multi-node cluster the coordinator
/// goes through Raft so every replica's state machine applies the DDL;
/// if the engine bypasses it (calling `Schema::create_*_internal`
/// directly, as it used to), replicas reject `MutationForward` writes
/// against the unregistered adjacency table and edge mutations hang.
///
/// A recording coordinator captures every DDL op the engine issues
/// through the trait. Pre-fix the engine made direct schema calls and
/// recorded events is empty; post-fix both `apply_create_keyspace` and
/// `apply_create_table` are observed.
#[tokio::test]
async fn graph_engine_lazy_adjacency_registration_routes_through_coordinator() {
    let schema = Arc::new(test_schema());
    schema
        .create_keyspace_internal(KeyspaceMetadata {
            name: "agent_memory".to_string(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .unwrap();
    schema
        .create_table_internal(build_edge_table_metadata(
            "agent_memory",
            "typed_edges",
            "TYPED_EDGE",
        ))
        .unwrap();

    let dir = tempdir().unwrap();
    let storage = Arc::new(test_storage_engine(dir.path()));
    storage
        .register_table(
            schema
                .snapshot()
                .tables
                .get(&("agent_memory".to_string(), "typed_edges".to_string()))
                .unwrap()
                .to_storage_schema(),
        )
        .unwrap();

    let recorder = Arc::new(RecordingCoordinator::new(Arc::clone(&schema)));

    let engine = GraphEngine::new_with_coordinator(
        Arc::clone(&schema),
        Arc::clone(&storage),
        Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        ))),
        GraphEngineConfig::default(),
        Duration::from_secs(60),
        recorder.clone(),
    );

    // Construction alone must not register adjacency — the lazy path
    // owns that responsibility now.
    assert!(
        recorder.events().is_empty(),
        "constructor must not emit DDL; recorded: {:?}",
        recorder.events()
    );

    // Trigger the lazy path the way a graph query does.
    engine
        .ensure_adjacency_storage_for_keyspace_for_test("agent_memory")
        .await
        .expect("lazy adjacency registration should succeed against the local coordinator");

    let events = recorder.events();
    assert!(
        events.iter().any(
            |e| matches!(e, DdlEvent::CreateKeyspace(name) if name == "system_graph_agent_memory")
        ),
        "lazy adjacency registration must route the keyspace DDL through the \
         GraphSchemaCoordinator; recorded events: {events:?}",
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            DdlEvent::CreateTable(ks, tbl)
                if ks == "system_graph_agent_memory" && tbl == "adjacency"
        )),
        "lazy adjacency registration must route the table DDL through the \
         GraphSchemaCoordinator; recorded events: {events:?}",
    );
}

/// Cluster mode transitions: the graph engine's adjacency DDL must
/// follow whatever `DdlPath` variant is currently active. The cluster
/// orchestrator swaps the variant via `ArcSwap::store` when the
/// deployment mode changes (standalone → pair → cluster, and back);
/// `ClusterGraphSchemaCoordinator` holds the same `Arc<ArcSwap<DdlPath>>`
/// regular CQL uses, so each `.load()` picks up the current variant.
///
/// This test covers the round-trip path through the variants we can
/// construct without standing up real Raft or a PeerManager:
/// `Direct → Forming → Unavailable → Direct`. The full
/// `Direct → Pair → Cluster → Pair → Direct` path is exercised by the
/// existing pair / cluster integration suites in `ferrosa-cluster/tests/`
/// (where Raft and the pair `DdlCoordinator` are stood up properly).
/// The contract pinned here is the engine-side property: whichever
/// variant is loaded at call time, the engine surfaces the result of
/// `DdlPath::execute` without inventing its own path. Pre-fix the
/// engine bypassed `DdlPath` entirely and applied DDL directly to the
/// local schema, so transitions were invisible to the client but other
/// replicas saw nothing.
#[tokio::test]
async fn cluster_graph_schema_coordinator_follows_ddl_path_transitions() {
    use ferrosa_cluster::ddl_path::DdlPath;
    use ferrosa_graph::engine::{ClusterGraphSchemaCoordinator, GraphSchemaCoordinator};

    let schema = Arc::new(test_schema());
    schema
        .create_keyspace_internal(KeyspaceMetadata {
            name: "agent_memory".to_string(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .unwrap();

    let dir = tempdir().unwrap();
    let storage = Arc::new(test_storage_engine(dir.path()));

    // Standalone (Direct).
    let ddl_swap = Arc::new(arc_swap::ArcSwap::from_pointee(DdlPath::Direct {
        schema: Arc::clone(&schema),
        engine: Arc::clone(&storage),
    }));
    let coordinator = ClusterGraphSchemaCoordinator::new(Arc::clone(&ddl_swap));

    let adj_ks = "system_graph_standalone_test".to_string();
    coordinator
        .apply_create_keyspace(KeyspaceMetadata {
            name: adj_ks.clone(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .await
        .expect("Direct variant: apply_create_keyspace should succeed");
    assert!(
        schema.snapshot().keyspaces.contains_key(&adj_ks),
        "Direct variant must apply DDL to the local schema"
    );

    // Standalone → Forming. The orchestrator swaps the path during
    // cluster formation; in-flight graph DDL gets a retriable error
    // (the operation is queued for replay after election).
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    ddl_swap.store(Arc::new(DdlPath::Forming { queue: tx }));
    let res = coordinator
        .apply_create_keyspace(KeyspaceMetadata {
            name: "system_graph_forming_test".into(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .await;
    assert!(
        res.is_err(),
        "Forming variant must surface a retriable error to the engine, got: {res:?}"
    );

    // Forming → Unavailable. Peer-lost / degraded; DDL must error out
    // until the operator promotes.
    ddl_swap.store(Arc::new(DdlPath::Unavailable));
    let res = coordinator
        .apply_create_keyspace(KeyspaceMetadata {
            name: "system_graph_unavailable_test".into(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .await;
    assert!(
        res.is_err(),
        "Unavailable variant must surface an error, got: {res:?}"
    );

    // Unavailable → Direct (downgrade back to standalone after the
    // operator resolves the degraded mode). DDL must succeed again via
    // the new variant — no rewiring of the coordinator needed because
    // the ArcSwap load on each call picks up the new variant.
    ddl_swap.store(Arc::new(DdlPath::Direct {
        schema: Arc::clone(&schema),
        engine: Arc::clone(&storage),
    }));
    let adj_ks_2 = "system_graph_standalone_after_unavailable".to_string();
    coordinator
        .apply_create_keyspace(KeyspaceMetadata {
            name: adj_ks_2.clone(),
            durable_writes: true,
            replication: simple_strategy_rf1(),
        })
        .await
        .expect("Direct variant after Unavailable: apply_create_keyspace should succeed");
    assert!(
        schema.snapshot().keyspaces.contains_key(&adj_ks_2),
        "Direct variant after Unavailable must apply DDL to the local schema"
    );
}
