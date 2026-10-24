//! Key derivation invariants: a key is a function of the label, the declared
//! key columns and their values — never of the Cypher variable that names the
//! pattern element.
//!
//! I1 (the write path, the read path, the inferred/planner shape and storage all
//! deriving the *same bytes*) lives as a unit test in
//! `src/executor/expand.rs::tests::i1_write_read_and_inferred_shapes_derive_the_same_key`,
//! because those entry points are private to the executor and an integration
//! test can only observe them through the engine. The invariants below are
//! observable end-to-end, so they are pinned end-to-end, on the bytes storage
//! actually holds — not on "the write returned Ok".

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::{json, Value};
use uuid::Uuid;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_graph::executor::result::GraphResult;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode, EnvSecretsProvider,
    PasswordHasher, PasswordPolicy, RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

const KEYSPACE: &str = "kiks";

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn superuser_auth() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn build_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
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
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

fn column(name: &str, kind: ColumnKind, position: i32, column_type: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position,
        column_type: column_type.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

struct Harness {
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    engine: GraphEngine,
    _dir: tempfile::TempDir,
}

/// One stored row, as the bytes storage holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredKey {
    partition: Vec<u8>,
    clustering: Vec<u8>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_storage(dir.path());
        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::default(),
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );
        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: KEYSPACE.to_string(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: HashMap::from([(
                            "replication_factor".to_string(),
                            "1".to_string(),
                        )]),
                    },
                },
                &superuser_auth(),
            )
            .unwrap();
        let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        )));
        let engine = GraphEngine::new(
            Arc::clone(&schema),
            Arc::clone(&storage),
            write_path,
            GraphEngineConfig::default(),
            std::time::Duration::ZERO,
        );
        Harness {
            schema,
            storage,
            engine,
            _dir: dir,
        }
    }

    fn declare(&self, table: TableMetadata) {
        self.schema
            .create_table(table.clone(), &superuser_auth())
            .unwrap_or_else(|e| panic!("explicit DDL for {}: {e}", table.name));
        self.storage
            .register_table(table.to_storage_schema())
            .unwrap_or_else(|e| panic!("storage registration for {}: {e}", table.name));
    }

    async fn ok(&self, query: &str) -> GraphResult {
        self.engine
            .execute(query, KEYSPACE, &superuser_auth())
            .await
            .unwrap_or_else(|e| panic!("query {query:?} failed: {e:?}"))
    }

    async fn count(&self, label: &str) -> i64 {
        let result = self
            .ok(&format!("MATCH (n:{label}) RETURN count(n) AS c"))
            .await;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_i64)
            .unwrap_or(-1)
    }

    /// Every live row of a table, as the exact key bytes storage holds.
    async fn stored_keys(&self, table: &str) -> Vec<StoredKey> {
        let write_path = WritePath::direct(Arc::clone(&self.storage));
        let partitions = write_path
            .range_read(&TableId::new(KEYSPACE, table))
            .await
            .unwrap_or_else(|e| panic!("range read of {table}: {e}"));
        let mut keys = Vec::new();
        for partition in partitions {
            for row in &partition.rows {
                keys.push(StoredKey {
                    partition: partition.key.key.as_bytes().to_vec(),
                    clustering: row.clustering.clone(),
                });
            }
        }
        keys.sort_by(|a, b| (&a.partition, &a.clustering).cmp(&(&b.partition, &b.clustering)));
        keys
    }
}

/// A `Person` vertex: uuid key column, text name — the shape a Bolt client's
/// `MERGE (n:Person {id: ...})` targets.
fn person_table() -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, 0, "uuid"),
    );
    columns.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, 0, "text"),
    );
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), "vertex".to_string());
    extensions.insert("graph.label".to_string(), "Person".to_string());
    TableMetadata {
        keyspace: KEYSPACE.to_string(),
        name: "person_v".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions,
        is_system: false,
    }
}

// ---------------------------------------------------------------------------
// I2 — names do not affect bytes
// ---------------------------------------------------------------------------

/// The same logical entity written three ways — `MERGE (n:Person {...})`,
/// `MERGE (person:Person {...})`, `MERGE (:Person {...})` — must land on
/// identical key bytes. The variable is query scope; the label and its key
/// columns are the entity. A synthesized name for an anonymous element, or a
/// variable threaded into the derivation, would show up here as three different
/// partitions.
#[tokio::test]
async fn i2_the_variable_name_does_not_affect_the_key_bytes() {
    let id = Uuid::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0001);
    let queries = [
        format!("MERGE (n:Person {{id: '{id}', name: 'Alice'}}) RETURN 1 AS v"),
        format!("MERGE (person:Person {{id: '{id}', name: 'Alice'}}) RETURN 1 AS v"),
        format!("MERGE (:Person {{id: '{id}', name: 'Alice'}}) RETURN 1 AS v"),
    ];

    let mut keys = Vec::new();
    for query in &queries {
        let h = Harness::new();
        h.declare(person_table());
        h.ok(query).await;

        let stored = h.stored_keys("person_v").await;
        assert_eq!(
            stored.len(),
            1,
            "each spelling writes exactly one row, got {stored:?} for {query:?}"
        );
        keys.push(stored[0].clone());

        // The read path must find that row through the same spelling's pattern,
        // so the equality below is about persisted bytes both directions agree on.
        let found = h
            .ok("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
            .await;
        assert_eq!(found.rows, vec![vec![json!("Alice")]], "for {query:?}");
    }

    assert_eq!(
        keys[0], keys[1],
        "`n` and `person` name the same entity, so must the bytes"
    );
    assert_eq!(
        keys[0], keys[2],
        "an anonymous pattern element names nothing, so its bytes are the entity's"
    );
    assert_eq!(
        keys[0].partition,
        id.as_bytes().to_vec(),
        "the declared uuid key column decides the partition, and only it"
    );
    assert!(
        keys[0].clustering.is_empty(),
        "Person has no clustering key"
    );
}

// ---------------------------------------------------------------------------
// I3 — an anonymous MERGE is idempotent
// ---------------------------------------------------------------------------

/// `MERGE (:Label {id: 1})` twice is one node. The engine has to bind the
/// anonymous element to something internally; if that internal name reached the
/// key derivation the second MERGE would miss the first and create a second
/// node.
#[tokio::test]
async fn i3_an_anonymous_merge_is_idempotent() {
    let h = Harness::new();
    let query = "MERGE (:Widget {pk: 1}) RETURN 1 AS v";

    h.ok(query).await;
    let after_first = h.stored_keys("widget_v").await;
    h.ok(query).await;
    let after_second = h.stored_keys("widget_v").await;

    assert_eq!(
        after_first.len(),
        1,
        "the first anonymous MERGE creates one row: {after_first:?}"
    );
    assert_eq!(
        after_second, after_first,
        "the second anonymous MERGE must land on the first one's key, byte for byte"
    );
    assert_eq!(
        h.count("Widget").await,
        1,
        "two anonymous MERGEs of one entity leave exactly one node"
    );
}

// ---------------------------------------------------------------------------
// I4 — a relationship MERGE with bound endpoints is idempotent, and the edge
//      it writes is traversable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn i4_a_relationship_merge_with_bound_endpoints_is_idempotent_and_traversable() {
    let h = Harness::new();
    h.declare(person_table());

    let alice = Uuid::from_u128(0xa11ce);
    let bob = Uuid::from_u128(0xb0b);
    h.ok(&format!(
        "MERGE (a:Person {{id: '{alice}', name: 'Alice'}}) RETURN 1 AS v"
    ))
    .await;
    h.ok(&format!(
        "MERGE (b:Person {{id: '{bob}', name: 'Bob'}}) RETURN 1 AS v"
    ))
    .await;

    let merge_edge = format!(
        "MATCH (a:Person {{id: '{alice}'}}), (b:Person {{id: '{bob}'}}) \
         MERGE (a)-[r:KNOWS {{since: 2024}}]->(b) RETURN 1 AS v"
    );
    h.ok(&merge_edge).await;
    let after_first = h.stored_keys("knows_e").await;
    h.ok(&merge_edge).await;
    let after_second = h.stored_keys("knows_e").await;

    assert_eq!(
        after_first.len(),
        1,
        "one bound-endpoint relationship MERGE writes one edge row: {after_first:?}"
    );
    assert_eq!(
        after_second, after_first,
        "re-merging the same endpoints and properties must reuse the same edge key"
    );

    // Traversable afterwards: the read path reconstructs the edge key from the
    // endpoints, so a found edge proves the write path put it where the read
    // path looks.
    let traversed = h
        .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS since")
        .await;
    assert_eq!(
        traversed.rows,
        vec![vec![json!(2024)]],
        "the merged edge must be traversable exactly once"
    );

    // And the endpoints' own rows are untouched by the edge write.
    assert_eq!(h.count("Person").await, 2);
}
