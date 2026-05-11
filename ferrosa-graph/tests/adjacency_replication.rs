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

impl GraphSchemaCoordinator for RecordingCoordinator {
    fn apply_create_keyspace(&self, ks: KeyspaceMetadata) -> ferrosa_graph::error::Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(DdlEvent::CreateKeyspace(ks.name.clone()));
        self.inner.apply_create_keyspace(ks)
    }

    fn apply_create_table(&self, table: TableMetadata) -> ferrosa_graph::error::Result<()> {
        self.events.lock().unwrap().push(DdlEvent::CreateTable(
            table.keyspace.clone(),
            table.name.clone(),
        ));
        self.inner.apply_create_table(table)
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
    };
    StorageEngine::new(config, None).unwrap()
}

/// At engine startup, the auto-created `system_graph_<ks>.adjacency` schema
/// must flow through the injected `GraphSchemaCoordinator`. The recording
/// coordinator captures the operation; pre-fix the engine called
/// `Schema::create_*_internal` directly and bypassed the coordinator
/// entirely, leaving cluster replicas with no idea the table exists.
#[test]
fn graph_engine_startup_routes_adjacency_ddl_through_coordinator() {
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

    let _engine = GraphEngine::new_with_coordinator(
        Arc::clone(&schema),
        Arc::clone(&storage),
        Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        ))),
        GraphEngineConfig::default(),
        Duration::from_secs(60),
        recorder.clone(),
    );

    let events = recorder.events();
    assert!(
        events.iter().any(
            |e| matches!(e, DdlEvent::CreateKeyspace(name) if name == "system_graph_agent_memory")
        ),
        "graph engine startup must route adjacency-keyspace DDL through the \
         GraphSchemaCoordinator so cluster replicas can register it; \
         recorded events: {events:?}",
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            DdlEvent::CreateTable(ks, tbl)
                if ks == "system_graph_agent_memory" && tbl == "adjacency"
        )),
        "graph engine startup must route adjacency-table DDL through the \
         GraphSchemaCoordinator; recorded events: {events:?}",
    );
}

/// Each cluster node applies Raft-replicated DDL to its local schema
/// independently. If an edge-typed table is added to a node's schema
/// *after* `GraphEngine::new_with_coordinator` ran (the common case in
/// CI: nodes start before the migrate step), the engine's startup scan
/// missed it. Pre-fix, no further trigger registered adjacency on that
/// node until a graph query happened to hit it as coordinator. Post-fix,
/// the engine reconciles its adjacency registrations against the
/// current schema in the background, so every node ends up with the
/// adjacency table registered shortly after the edge table appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_engine_registers_adjacency_for_edge_keyspaces_added_after_startup() {
    let schema = Arc::new(test_schema());
    // Engine starts with an empty schema — no edge tables visible yet.
    let dir = tempdir().unwrap();
    let storage = Arc::new(test_storage_engine(dir.path()));
    let recorder = Arc::new(RecordingCoordinator::new(Arc::clone(&schema)));

    let _engine = GraphEngine::new_with_coordinator(
        Arc::clone(&schema),
        Arc::clone(&storage),
        Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        ))),
        GraphEngineConfig::default(),
        Duration::from_secs(60),
        recorder.clone(),
    );

    // Startup scan recorded nothing — no edge tables existed yet.
    assert!(
        recorder.events().is_empty(),
        "engine startup should not register adjacency before any edge table \
         exists; recorded: {:?}",
        recorder.events()
    );

    // Simulate Raft-replicated DDL: an edge-typed table appears in the
    // local schema after the engine has already started.
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

    // The engine should pick it up within a bounded interval. Poll up to
    // 5 seconds — generous so flaky CI runners don't false-fail.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let events = recorder.events();
        let saw_keyspace = events.iter().any(
            |e| matches!(e, DdlEvent::CreateKeyspace(name) if name == "system_graph_agent_memory"),
        );
        let saw_table = events.iter().any(|e| matches!(
            e,
            DdlEvent::CreateTable(ks, tbl) if ks == "system_graph_agent_memory" && tbl == "adjacency"
        ));
        if saw_keyspace && saw_table {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "engine should have registered adjacency for agent_memory after \
                 the edge table appeared in schema, but didn't within 5s; \
                 recorded events: {events:?}",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
