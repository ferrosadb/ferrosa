//! Write-path gaps that stop a real Bolt workload from loading data (TDD).
//!
//! Three forms failed against a live cluster while their read-side twins worked.
//! Each test below is written to fail on the pre-fix tree and documents the
//! failure it was written against:
//!
//! 1. `MATCH ... SET n.prop = <expr> RETURN ...` — the `SET` clause had no
//!    trailing `RETURN`, so the parser rejected the query with
//!    `unexpected token after statement: Keyword(Return)`, and an assignment
//!    right-hand side that referenced the matched row (`SET n.age = n.age + 1`)
//!    reached the encoder unevaluated:
//!    `cannot convert expression to bytes: Arithmetic { .. }`.
//! 2. `UNWIND $rows AS row ...` — a parameter holding a list of maps was bound as
//!    a scalar, so the parameterised form failed with
//!    `parameter $rows must be a scalar value` while the inline list worked.
//! 3. `MATCH (a:L {..}), (b:L {..}) MERGE (a)-[:R {..}]->(b)` — a relationship
//!    pattern inside a `MERGE` that follows a `MATCH` did not parse:
//!    `expected RETURN, OPTIONAL MATCH, WITH, SET, REMOVE, DELETE, DETACH DELETE,
//!    or CALL after MATCH, got Keyword(Merge)`.
//!
//! Invariants pinned here (one test each, named after the invariant):
//! - `merge_is_idempotent_*` — a repeated `MERGE` yields one entity.
//! - `set_scope_*` — `SET` touches only the rows the `MATCH` selected.
//! - `parameter_and_inline_forms_are_equivalent_*` — same rows, same mutation,
//!   same stats.
//! - `*_applies_to_all_n_*` — N rows are all written (no silent truncation).
//!
//! Note on filters: this keyspace's graph tables use a `uuid` partition key, and
//! a UUID property read back out is hex-encoded storage bytes, so the tests
//! select rows by their `text`/`int` properties (`name`, `age`) rather than by a
//! UUID string literal.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::{json, Value};
use uuid::Uuid;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::error::GraphError;
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
};

const KEYSPACE: &str = "gapks";

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

fn person_vertex_metadata() -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, 0, "uuid"),
    );
    columns.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, -1, "text"),
    );
    columns.insert(
        "age".to_string(),
        column("age", ColumnKind::Regular, -1, "int"),
    );
    columns.insert(
        "value".to_string(),
        column("value", ColumnKind::Regular, -1, "text"),
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

fn knows_edge_metadata() -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "src_id".to_string(),
        column("src_id", ColumnKind::PartitionKey, 0, "uuid"),
    );
    columns.insert(
        "dst_id".to_string(),
        column("dst_id", ColumnKind::Clustering, 0, "uuid"),
    );
    columns.insert(
        "since".to_string(),
        column("since", ColumnKind::Regular, -1, "int"),
    );
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), "edge".to_string());
    extensions.insert("graph.label".to_string(), "KNOWS".to_string());
    extensions.insert("graph.source".to_string(), "src_id".to_string());
    extensions.insert("graph.target".to_string(), "dst_id".to_string());
    extensions.insert("graph.source_label".to_string(), "Person".to_string());
    extensions.insert("graph.target_label".to_string(), "Person".to_string());
    TableMetadata {
        keyspace: KEYSPACE.to_string(),
        name: "knows_e".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["src_id".to_string()],
        clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions,
        is_system: false,
    }
}

struct Harness {
    engine: GraphEngine,
    _dir: tempfile::TempDir,
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
        let auth = superuser_auth();
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
                &auth,
            )
            .unwrap();
        let person = person_vertex_metadata();
        let knows = knows_edge_metadata();
        schema.create_table(person.clone(), &auth).unwrap();
        schema.create_table(knows.clone(), &auth).unwrap();
        storage.register_table(person.to_storage_schema()).unwrap();
        storage.register_table(knows.to_storage_schema()).unwrap();
        let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
            Arc::clone(&storage),
        )));
        let engine = GraphEngine::new(
            schema,
            storage,
            write_path,
            GraphEngineConfig::default(),
            std::time::Duration::ZERO,
        );
        Harness { engine, _dir: dir }
    }

    async fn run(&self, query: &str) -> ferrosa_graph::error::Result<GraphResult> {
        self.engine
            .execute(query, KEYSPACE, &superuser_auth())
            .await
    }

    async fn run_with_params(
        &self,
        query: &str,
        params: &HashMap<String, Value>,
    ) -> ferrosa_graph::error::Result<GraphResult> {
        self.engine
            .execute_with_params(query, KEYSPACE, &superuser_auth(), params)
            .await
    }

    async fn ok(&self, query: &str) -> GraphResult {
        self.run(query)
            .await
            .unwrap_or_else(|e| panic!("query {query:?} failed: {e:?}"))
    }

    async fn ok_with_params(&self, query: &str, params: &HashMap<String, Value>) -> GraphResult {
        self.run_with_params(query, params)
            .await
            .unwrap_or_else(|e| panic!("query {query:?} failed: {e:?}"))
    }

    /// Create one `Person` with an explicit uuid key so the storage key is
    /// deterministic.
    async fn create_person(&self, id: &str, name: &str, age: i64) {
        self.ok(&format!(
            "CREATE (n:Person {{id: '{id}', name: '{name}', age: {age}}}) RETURN 1 AS v"
        ))
        .await;
    }

    /// The value of `n.value` for the named person, or `None` when the property
    /// is absent.
    async fn value_of(&self, name: &str) -> Option<Value> {
        let result = self
            .ok(&format!(
                "MATCH (n:Person {{name: '{name}'}}) RETURN n.value AS v"
            ))
            .await;
        match result.rows.first().and_then(|row| row.first()) {
            Some(Value::Null) | None => None,
            Some(value) => Some(value.clone()),
        }
    }

    async fn age_of(&self, name: &str) -> Value {
        let result = self
            .ok(&format!(
                "MATCH (n:Person {{name: '{name}'}}) RETURN n.age AS v"
            ))
            .await;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Value::Null)
    }

    async fn know_count(&self) -> i64 {
        let result = self
            .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS c")
            .await;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_i64)
            .unwrap_or(-1)
    }

    async fn name_count(&self, name: &str) -> i64 {
        let result = self
            .ok(&format!(
                "MATCH (n:Person {{name: '{name}'}}) RETURN count(n) AS c"
            ))
            .await;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_i64)
            .unwrap_or(-1)
    }
}

/// The single column `name` of a result, as a sorted list of JSON values.
fn sorted_column(result: &GraphResult, name: &str) -> Vec<Value> {
    let index = result
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("result has no column {name:?}: {:?}", result.columns));
    let mut values: Vec<Value> = result.rows.iter().map(|row| row[index].clone()).collect();
    values.sort_by_key(|value| value.to_string());
    values
}

fn uuid_for(tag: u64) -> String {
    format!("00000000-0000-0000-0000-{tag:012}")
}

// ---------------------------------------------------------------------------
// gap 1 — SET with a trailing RETURN, and expression right-hand sides
// ---------------------------------------------------------------------------

/// Pre-fix: `unexpected token after statement: Keyword(Return)`.
#[tokio::test]
async fn set_clause_projects_its_trailing_return() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000a1", "Alice", 30)
        .await;

    let result = h
        .ok("MATCH (n:Person {name: 'Alice'}) SET n.value = 'x' RETURN n.value AS v")
        .await;
    assert_eq!(
        result.rows,
        vec![vec!(json!("x"))],
        "SET ... RETURN must project the value it just wrote"
    );

    // ... and the write must be visible to a later query.
    assert_eq!(h.value_of("Alice").await, Some(json!("x")));
}

/// Pre-fix: `cannot convert expression to bytes: Arithmetic { .. }` — the
/// right-hand side reached the encoder unevaluated.
#[tokio::test]
async fn set_expression_rhs_reads_the_matched_row() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000a2", "Alice", 30)
        .await;

    let result = h
        .ok("MATCH (n:Person {name: 'Alice'}) SET n.age = n.age + 1 RETURN n.age AS v")
        .await;
    assert_eq!(result.rows, vec![vec!(json!(31))]);
    assert_eq!(
        h.age_of("Alice").await,
        json!(31),
        "the increment persisted"
    );

    // A second run increments again: the RHS reads the stored value, not a
    // stale snapshot.
    h.ok("MATCH (n:Person {name: 'Alice'}) SET n.age = n.age + 1 RETURN n.age AS v")
        .await;
    assert_eq!(h.age_of("Alice").await, json!(32));
}

#[tokio::test]
async fn set_parameter_rhs_binds() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000a3", "Alice", 30)
        .await;

    let result = h
        .ok_with_params(
            "MATCH (n:Person {name: 'Alice'}) SET n.value = $v RETURN n.value AS v",
            &HashMap::from([("v".to_string(), json!("from-param"))]),
        )
        .await;
    assert_eq!(result.rows, vec![vec!(json!("from-param"))]);
    assert_eq!(h.value_of("Alice").await, Some(json!("from-param")));
}

/// Invariant (scope): a SET that matches nothing changes nothing.
#[tokio::test]
async fn set_scope_is_the_matched_rows_only() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000b1", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-0000000000b2", "Bob", 40)
        .await;

    let result = h
        .ok("MATCH (n:Person {name: 'Nobody'}) SET n.age = 99 RETURN n.age AS v")
        .await;
    assert!(
        result.rows.is_empty(),
        "a SET over an empty match produces no rows, got {:?}",
        result.rows
    );
    assert_eq!(
        result.stats.vertices_written, 0,
        "a SET over an empty match writes nothing"
    );

    // Neither selected nor unselected rows moved: the predicate stayed a filter.
    assert_eq!(h.age_of("Alice").await, json!(30));
    assert_eq!(h.age_of("Bob").await, json!(40));
    assert_eq!(h.name_count("Nobody").await, 0);
}

/// Invariant (completeness): a SET over N rows writes all N.
#[tokio::test]
async fn set_applies_to_all_n_matched_rows() {
    const N: usize = 5;
    let h = Harness::new();
    for tag in 0..N {
        h.create_person(&uuid_for(100 + tag as u64), "Bulk", 20)
            .await;
    }
    assert_eq!(h.name_count("Bulk").await, N as i64);

    let result = h
        .ok("MATCH (n:Person {name: 'Bulk'}) SET n.age = 7 RETURN n.age AS v")
        .await;
    assert_eq!(
        result.rows.len(),
        N,
        "SET ... RETURN projects one row per matched row"
    );
    assert!(
        result.rows.iter().all(|row| row[0] == json!(7)),
        "every projected row shows the value just written"
    );
    assert_eq!(
        result.stats.vertices_written, N,
        "all N rows were written, none truncated"
    );

    // Persisted for every row, not just the last one.
    let persisted = h
        .ok("MATCH (n:Person {name: 'Bulk', age: 7}) RETURN count(n) AS c")
        .await;
    assert_eq!(persisted.rows[0][0], json!(N as i64));
}

#[tokio::test]
async fn set_without_return_keeps_the_status_row() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000c1", "Alice", 30)
        .await;

    let result = h
        .ok("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
        .await;
    assert_eq!(result.columns, vec!["status".to_string()]);
    assert_eq!(result.rows, vec![vec!(json!("updated 1 vertices"))]);
    assert_eq!(h.age_of("Alice").await, json!(31));
}

// ---------------------------------------------------------------------------
// gap 2 — parameterised list of maps in UNWIND
// ---------------------------------------------------------------------------

/// Pre-fix: `parameter $rows must be a scalar value`.
#[tokio::test]
async fn unwind_parameter_list_of_maps_reads_like_the_inline_list() {
    let h = Harness::new();
    let rows = json!([
        {"id": uuid_for(1), "name": "P1", "age": 10},
        {"id": uuid_for(2), "name": "P2", "age": 20},
        {"id": uuid_for(3), "name": "P3", "age": 30},
    ]);

    let parameterised = h
        .ok_with_params(
            "UNWIND $rows AS row RETURN row.name AS v",
            &HashMap::from([("rows".to_string(), rows.clone())]),
        )
        .await;
    let inline = h
        .ok("UNWIND [{name: 'P1'}, {name: 'P2'}, {name: 'P3'}] AS row RETURN row.name AS v")
        .await;

    assert_eq!(parameterised.columns, inline.columns);
    assert_eq!(
        sorted_column(&parameterised, "v"),
        sorted_column(&inline, "v"),
        "a list-of-maps parameter must produce exactly the inline list's rows"
    );

    // Property access on the element works, and every element is projected.
    let ages = h
        .ok_with_params(
            "UNWIND $rows AS row RETURN row.age AS v",
            &HashMap::from([("rows".to_string(), rows)]),
        )
        .await;
    assert_eq!(
        sorted_column(&ages, "v"),
        vec![json!(10), json!(20), json!(30)]
    );
}

/// Invariant (parameter equivalence): the same logical input produces the same
/// rows, the same mutation, and the same stats, parameterised or inline.
#[tokio::test]
async fn parameter_and_inline_forms_are_equivalent_for_unwind_writes() {
    let h = Harness::new();
    let rows = json!([
        {"id": uuid_for(11), "name": "Param1", "age": 1},
        {"id": uuid_for(12), "name": "Param2", "age": 2},
        {"id": uuid_for(13), "name": "Param3", "age": 3},
        {"id": uuid_for(14), "name": "Param4", "age": 4},
    ]);
    let inline_rows = "[{id: '00000000-0000-0000-0000-000000000021', name: 'Inline1', age: 1}, \
                       {id: '00000000-0000-0000-0000-000000000022', name: 'Inline2', age: 2}, \
                       {id: '00000000-0000-0000-0000-000000000023', name: 'Inline3', age: 3}, \
                       {id: '00000000-0000-0000-0000-000000000024', name: 'Inline4', age: 4}]";

    let parameterised = h
        .ok_with_params(
            "UNWIND $rows AS row CREATE (n:Person {id: row.id, name: row.name, age: row.age}) \
             RETURN 1 AS v",
            &HashMap::from([("rows".to_string(), rows)]),
        )
        .await;
    let inline = h
        .ok(&format!(
            "UNWIND {inline_rows} AS row CREATE (n:Person {{id: row.id, name: row.name, \
             age: row.age}}) RETURN 1 AS v"
        ))
        .await;

    assert_eq!(
        parameterised.rows, inline.rows,
        "the parameterised and inline forms must project the same rows"
    );
    assert_eq!(
        parameterised.stats.vertices_written, inline.stats.vertices_written,
        "the parameterised and inline forms must report the same write stat"
    );
    assert_eq!(parameterised.stats.vertices_written, 4);

    // Same mutation: every element of each list landed.
    for name in ["Param1", "Param2", "Param3", "Param4"] {
        assert_eq!(h.name_count(name).await, 1, "{name} was not created");
    }
    for name in ["Inline1", "Inline2", "Inline3", "Inline4"] {
        assert_eq!(h.name_count(name).await, 1, "{name} was not created");
    }
}

/// Invariant (completeness): a writing UNWIND applies to all N elements.
#[tokio::test]
async fn unwind_write_applies_to_all_n_elements() {
    const N: usize = 6;
    let h = Harness::new();
    let rows: Vec<Value> = (0..N)
        .map(|tag| json!({"id": uuid_for(200 + tag as u64), "name": format!("Bulk{tag}"), "age": 42}))
        .collect();

    let result = h
        .ok_with_params(
            "UNWIND $rows AS row CREATE (n:Person {id: row.id, name: row.name, age: row.age}) \
             RETURN 1 AS v",
            &HashMap::from([("rows".to_string(), Value::Array(rows))]),
        )
        .await;
    assert_eq!(
        result.rows.len(),
        N,
        "a writing UNWIND must project one row per element"
    );
    assert_eq!(
        result.stats.vertices_written, N,
        "and write every element, with no silent truncation"
    );
    for tag in 0..N {
        assert_eq!(
            h.name_count(&format!("Bulk{tag}")).await,
            1,
            "element {tag} was not created"
        );
    }
}

// ---------------------------------------------------------------------------
// gap 3 — a relationship pattern inside MERGE, after MATCH
// ---------------------------------------------------------------------------

/// Pre-fix: `expected RETURN, OPTIONAL MATCH, WITH, SET, REMOVE, DELETE, DETACH
/// DELETE, or CALL after MATCH, got Keyword(Merge)`.
#[tokio::test]
async fn merge_relationship_after_match_upserts_the_edge() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000d1", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-0000000000d2", "Bob", 40)
        .await;

    h.ok(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         MERGE (a)-[:KNOWS {since: 2024}]->(b) RETURN 1 AS v",
    )
    .await;

    let stored = h
        .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS v")
        .await;
    assert_eq!(stored.rows, vec![vec!(json!(2024))]);
    assert_eq!(h.know_count().await, 1);
}

/// Invariant (idempotence): running the same MERGE twice yields ONE entity.
#[tokio::test]
async fn merge_after_match_is_idempotent() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000e1", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-0000000000e2", "Bob", 40)
        .await;

    let query = "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
                 MERGE (a)-[:KNOWS {since: 2024}]->(b) RETURN 1 AS v";
    h.ok(query).await;
    let after_first = h.know_count().await;
    h.ok(query).await;
    let after_second = h.know_count().await;

    assert_eq!(after_first, 1, "first run creates exactly one edge");
    assert_eq!(
        after_second, 1,
        "a second identical MERGE must not create a second edge"
    );
    assert_eq!(
        h.name_count("Alice").await,
        1,
        "and must not create a second endpoint vertex"
    );
    assert_eq!(h.name_count("Bob").await, 1);
}

#[tokio::test]
async fn merge_after_match_binds_parameters_in_the_relationship_properties() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-0000000000f1", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-0000000000f2", "Bob", 40)
        .await;

    let params = HashMap::from([
        ("a".to_string(), json!("Alice")),
        ("b".to_string(), json!("Bob")),
        ("since".to_string(), json!(2030)),
    ]);
    let query = "MATCH (a:Person {name: $a}), (b:Person {name: $b}) \
                 MERGE (a)-[:KNOWS {since: $since}]->(b) RETURN 1 AS v";
    h.ok_with_params(query, &params).await;
    h.ok_with_params(query, &params).await;

    let stored = h
        .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS v")
        .await;
    assert_eq!(
        stored.rows,
        vec![vec!(json!(2030))],
        "the relationship property map must take the parameter's value"
    );
    assert_eq!(
        h.know_count().await,
        1,
        "and stay idempotent with parameters"
    );
}

/// The boundary of the fix, pinned loudly: a MERGE after MATCH whose endpoints
/// carry no properties cannot be bound to the rows the MATCH selected, and says
/// so instead of silently upserting against an arbitrary row.
#[tokio::test]
async fn merge_after_match_with_bare_endpoints_fails_loud() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-000000000101", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-000000000102", "Bob", 40)
        .await;

    let err = h
        .run("MATCH (a:Person), (b:Person) MERGE (a)-[:KNOWS {since: 1}]->(b) RETURN 1 AS v")
        .await
        .expect_err("a bare endpoint has no key to derive, so this must be rejected");
    match err {
        GraphError::Parse(parse_error) => assert!(
            parse_error
                .message
                .contains("has no properties to identify it"),
            "the error must explain the boundary, got: {}",
            parse_error.message
        ),
        other => panic!("expected a parse error, got {other:?}"),
    }
    assert_eq!(h.know_count().await, 0, "and nothing was written");
}

/// A relationship in the folded MATCH would be re-upserted by the MERGE, so it is
/// rejected rather than reinterpreted.
#[tokio::test]
async fn merge_after_match_rejects_a_relationship_in_the_match() {
    let h = Harness::new();
    h.create_person("00000000-0000-0000-0000-000000000111", "Alice", 30)
        .await;
    h.create_person("00000000-0000-0000-0000-000000000112", "Bob", 40)
        .await;

    let err = h
        .run(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) \
             MERGE (a)-[:LIKES]->(b) RETURN 1 AS v",
        )
        .await
        .expect_err("folding a MATCH relationship into the MERGE must be rejected");
    assert!(
        matches!(err, GraphError::Parse(_)),
        "expected a parse error, got {err:?}"
    );
}

/// Invariant (idempotence), vertex form: a repeated MERGE yields one vertex.
#[tokio::test]
async fn merge_vertex_is_idempotent() {
    let h = Harness::new();
    let query = "MERGE (n:Person {name: 'Zed'}) RETURN 1 AS v";
    h.ok(query).await;
    let first = h.name_count("Zed").await;
    h.ok(query).await;

    assert_eq!(first, 1);
    assert_eq!(h.name_count("Zed").await, 1);
}
