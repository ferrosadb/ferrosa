//! Schema-on-write for graph labels.
//!
//! Neo4j (the comparator ferrosa's graph front end is benchmarked against) is
//! schema-on-write: the first `CREATE` naming a label creates it. Ferrosa's graph
//! front end is catalog-first — a Cypher label must map to a table declared with
//! `graph.type` / `graph.label` extensions — so a Bolt client that does
//! `CREATE (n:BenchRow {pk: 1})` against a fresh keyspace was rejected with
//! `no table with graph.label 'BenchRow' found in keyspace ...`.
//!
//! These tests pin the implicit-creation behaviour that closes that gap. Each is
//! named after the invariant it proves, and every one of them was watched red on
//! the pre-change tree (see the PR body for the captured output).
//!
//! Invariants pinned here (one test each):
//! - I1 `i1_create_into_a_declared_table_still_works` — a CREATE that already
//!   resolved to a declared table is untouched by this change.
//! - I2 `i2_uninferable_property_fails_the_write_loudly` and
//!   `i2_table_name_collision_fails_the_write_loudly` — a write is never silently
//!   dropped; when the backing table cannot be created the statement fails.
//! - I3 `i3_label_resolves_to_one_table_name` — the label → table mapping is
//!   deterministic (same label, same table, every time).
//! - I4 `i4_concurrent_first_writes_of_one_label_all_succeed` — 8 real threads
//!   racing on a brand-new label: all writes land, exactly one table is created.
//! - I5 `i5_unknown_label_reads_are_unchanged` — MATCH on an unknown label is still
//!   an empty result, and `count(n)` over it is still one row of 0.
//! - I6 `i6_implicit_table_equals_explicit_ddl` — the implicitly created table is
//!   field-for-field the metadata the equivalent explicit `CREATE TABLE ... WITH
//!   extensions = {...}` produces.

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
};

const KEYSPACE: &str = "swks";

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

    /// Declare a table exactly as `CREATE TABLE ... WITH extensions = {...}` does.
    fn declare(&self, table: TableMetadata) {
        self.schema
            .create_table(table.clone(), &superuser_auth())
            .unwrap_or_else(|e| panic!("explicit DDL for {}: {e}", table.name));
        self.storage
            .register_table(table.to_storage_schema())
            .unwrap_or_else(|e| panic!("storage registration for {}: {e}", table.name));
    }

    async fn run(&self, query: &str) -> ferrosa_graph::error::Result<GraphResult> {
        self.engine
            .execute(query, KEYSPACE, &superuser_auth())
            .await
    }

    async fn ok(&self, query: &str) -> GraphResult {
        self.run(query)
            .await
            .unwrap_or_else(|e| panic!("query {query:?} failed: {e:?}"))
    }

    /// `count(<var>)` for the label, as an i64 (-1 when the result is not a count).
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

    /// Every table in the keyspace carrying a `graph.label`, as
    /// `(table name, label, graph.type)`. The count of entries for one label is
    /// the number of backing tables the schema holds for it.
    fn graph_tables(&self) -> Vec<(String, String, String)> {
        let snap = self.schema.snapshot();
        let mut tables: Vec<_> = snap
            .tables
            .values()
            .filter(|meta| meta.keyspace == KEYSPACE)
            .filter_map(|meta| {
                let label = meta.extensions.get("graph.label")?.clone();
                let graph_type = meta.extensions.get("graph.type")?.clone();
                Some((meta.name.clone(), label, graph_type))
            })
            .collect();
        tables.sort();
        tables
    }

    /// The registered metadata for a table in the keyspace.
    fn table_meta(&self, name: &str) -> Option<TableMetadata> {
        self.schema
            .snapshot()
            .tables
            .get(&(KEYSPACE.to_string(), name.to_string()))
            .cloned()
    }
}

/// A `Person` vertex table, declared as a client would: uuid key, text name,
/// int age. (Shape mirrors `bolt_write_gaps.rs`'s fixture.)
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
    columns.insert(
        "age".to_string(),
        column("age", ColumnKind::Regular, 0, "int"),
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
// I1 — a CREATE that already resolved to a declared table is unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn i1_create_into_a_declared_table_still_works() {
    let h = Harness::new();
    h.declare(person_table());
    let before = h.table_meta("person_v").unwrap();

    h.ok(&format!(
        "CREATE (n:Person {{id: '{}', name: 'Alice', age: 30}}) RETURN 1 AS v",
        Uuid::from_u128(0xa1).hyphenated()
    ))
    .await;

    let rows = h
        .ok("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age")
        .await;
    assert_eq!(
        rows.rows,
        vec![vec![json!(30)]],
        "the write must land exactly where a declared table puts it"
    );
    assert_eq!(
        h.table_meta("person_v").unwrap().columns.len(),
        before.columns.len(),
        "a write into a declared table must not alter its schema"
    );
    assert_eq!(
        h.graph_tables(),
        vec![(
            "person_v".to_string(),
            "Person".to_string(),
            "vertex".to_string()
        )],
        "and must not create a second table for the label"
    );
}

// ---------------------------------------------------------------------------
// I2 — a write is never silently dropped
// ---------------------------------------------------------------------------

/// A property whose column type cannot be inferred (`null`) cannot become a
/// column. The CREATE must fail and leave nothing behind — not create a table
/// with a guessed type, and not report success for a write it did not perform.
#[tokio::test]
async fn i2_uninferable_property_fails_the_write_loudly() {
    let h = Harness::new();

    let err = h
        .run("CREATE (n:Nullable {id: 'a1', note: null}) RETURN 1 AS v")
        .await
        .expect_err("a null-only property has no inferable column type");

    let message = err.to_string();
    assert!(
        message.contains("note"),
        "the error must name the property it could not type, got: {message}"
    );
    assert!(
        !message.contains("no table with graph.label"),
        "the failure must be about typing, not a missing label: {message}"
    );
    assert!(
        h.graph_tables().is_empty(),
        "a failed first write must not leave a half-built table behind: {:?}",
        h.graph_tables()
    );
}

/// The derived table name can already be taken by a table that is not a
/// `graph.type` table for this label. Creating nothing and then silently writing
/// nowhere is the failure mode this forbids: the write must fail and name the
/// collision.
#[tokio::test]
async fn i2_table_name_collision_fails_the_write_loudly() {
    let h = Harness::new();
    // `CREATE TABLE swks.benchrow_v (id uuid PRIMARY KEY, v text)` with no graph
    // extensions occupies the name the `BenchRow` label maps to.
    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, 0, "uuid"),
    );
    columns.insert("v".to_string(), column("v", ColumnKind::Regular, 0, "text"));
    h.declare(TableMetadata {
        keyspace: KEYSPACE.to_string(),
        name: "benchrow_v".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    });

    let err = h
        .run("CREATE (n:BenchRow {pk: 1, v: 'x'}) RETURN 1 AS v")
        .await
        .expect_err("the label's table name is taken by a non-graph table");

    assert!(
        err.to_string().contains("benchrow_v"),
        "the error must name the table it could not claim, got: {err}"
    );
    assert!(
        h.graph_tables().is_empty(),
        "no graph table was created: {:?}",
        h.graph_tables()
    );
}

// ---------------------------------------------------------------------------
// I3 — the label -> table mapping is deterministic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn i3_label_resolves_to_one_table_name() {
    let h = Harness::new();

    h.ok("CREATE (n:BenchRow {pk: 1, v: 'a'}) RETURN 1 AS v")
        .await;
    h.ok("CREATE (n:BenchRow {pk: 2, v: 'b'}) RETURN 1 AS v")
        .await;

    assert_eq!(
        h.graph_tables(),
        vec![(
            "benchrow_v".to_string(),
            "BenchRow".to_string(),
            "vertex".to_string()
        )],
        "two writes of one label must reach one table"
    );
    assert_eq!(h.count("BenchRow").await, 2);
}

// ---------------------------------------------------------------------------
// I4 — concurrent first writes of the same new label
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn i4_concurrent_first_writes_of_one_label_all_succeed() {
    const WRITERS: u64 = 8;
    let h = Arc::new(Harness::new());

    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let h = Arc::clone(&h);
        tasks.push(tokio::spawn(async move {
            h.run(&format!(
                "CREATE (n:Fresh {{pk: {writer}, v: 'row{writer}'}}) RETURN 1 AS v"
            ))
            .await
        }));
    }

    let mut failures = Vec::new();
    for (writer, task) in tasks.into_iter().enumerate() {
        match task.await.expect("writer task panicked") {
            Ok(_) => {}
            Err(e) => failures.push(format!("writer {writer}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "every racing first-write must succeed, but {} failed: {failures:#?}",
        failures.len()
    );

    let tables: Vec<_> = h
        .graph_tables()
        .into_iter()
        .filter(|(_, label, _)| label == "Fresh")
        .collect();
    assert_eq!(
        tables.len(),
        1,
        "exactly one backing table for the label, got {tables:?}"
    );
    assert_eq!(
        h.count("Fresh").await,
        WRITERS as i64,
        "no racing write may be lost"
    );
}

// ---------------------------------------------------------------------------
// I5 — read behaviour is unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn i5_unknown_label_reads_are_unchanged() {
    let h = Harness::new();

    let rows = h.ok("MATCH (n:NoSuchLabel) RETURN n AS v").await;
    assert!(
        rows.rows.is_empty(),
        "MATCH on an unknown label is an empty result, got {:?}",
        rows.rows
    );

    // `count(n)` over the empty match must report 0 and stay one column. Its row
    // *count* is deliberately not pinned here: the "one row of 0 instead of no
    // rows" change lives on the unmerged `fix/pgwire-nodelay` branch (commit
    // 490407194, `empty_match_result`), not on this branch's base — and this
    // change must not move either shape.
    let counted = h.ok("MATCH (n:NoSuchLabel) RETURN count(n) AS c").await;
    assert_eq!(counted.columns, vec!["c".to_string()]);
    assert!(counted.rows.len() <= 1, "{:?}", counted.rows);
    for row in &counted.rows {
        assert_eq!(row, &vec![json!(0)], "the count of nothing is 0");
    }

    assert!(
        h.graph_tables().is_empty(),
        "and a read must not create a table: {:?}",
        h.graph_tables()
    );
}

// ---------------------------------------------------------------------------
// I6 — an implicit table is identical to an explicit one
// ---------------------------------------------------------------------------

#[tokio::test]
async fn i6_implicit_table_equals_explicit_ddl() {
    let h = Harness::new();

    h.ok("CREATE (n:BenchRow {pk: 1, v: 'x'}) RETURN 1 AS v")
        .await;

    // What this DDL produces through `ferrosa_cql::router::route_create_table`:
    //
    //   CREATE TABLE swks.benchrow_v (id uuid PRIMARY KEY, pk bigint, v text)
    //   WITH extensions = {'graph.type': 'vertex', 'graph.label': 'BenchRow'};
    //
    // — key column first with ColumnKind::PartitionKey/position 0, regular
    // columns at position 0 (the router's `else` arm), TableParams::default(),
    // no flags, and the extensions map exactly as written.
    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, 0, "uuid"),
    );
    columns.insert(
        "pk".to_string(),
        column("pk", ColumnKind::Regular, 0, "bigint"),
    );
    columns.insert("v".to_string(), column("v", ColumnKind::Regular, 0, "text"));
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), "vertex".to_string());
    extensions.insert("graph.label".to_string(), "BenchRow".to_string());
    let expected = TableMetadata {
        keyspace: KEYSPACE.to_string(),
        name: "benchrow_v".to_string(),
        // `id` is a fresh uuid per creation; every other field is compared.
        id: Uuid::nil(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions,
        is_system: false,
    };

    let actual = h
        .table_meta("benchrow_v")
        .expect("the implicit table exists under the label's table name");

    assert_eq!(actual.keyspace, expected.keyspace);
    assert_eq!(actual.name, expected.name);
    let names: Vec<&String> = actual.columns.keys().collect();
    let expected_names: Vec<&String> = expected.columns.keys().collect();
    assert_eq!(
        names, expected_names,
        "column order must match the DDL's declaration order"
    );
    for (name, want) in &expected.columns {
        let got = &actual.columns[name];
        assert_eq!(got.kind, want.kind, "column {name}: kind");
        assert_eq!(got.position, want.position, "column {name}: position");
        assert_eq!(got.column_type, want.column_type, "column {name}: type");
        assert_eq!(
            got.clustering_order, want.clustering_order,
            "column {name}: clustering order"
        );
    }
    assert_eq!(actual.partition_key, expected.partition_key);
    assert_eq!(actual.clustering_key, expected.clustering_key);
    assert_eq!(actual.flags, expected.flags);
    assert_eq!(actual.extensions, expected.extensions);
    assert_eq!(actual.is_system, expected.is_system);
    assert_eq!(
        actual.params.gc_grace_seconds,
        expected.params.gc_grace_seconds
    );
    assert_eq!(
        actual.params.default_time_to_live,
        expected.params.default_time_to_live
    );
    assert_eq!(
        actual.to_storage_schema().regular_columns,
        expected.to_storage_schema().regular_columns,
        "the storage schema the write path consumes must be identical too"
    );
}

// ---------------------------------------------------------------------------
// the write shapes the Bolt benchmark and a MERGE workload actually use
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_on_an_unknown_label_writes_the_row() {
    let h = Harness::new();

    h.ok("CREATE (n:BenchRow {pk: 7, v: 'seven'}) RETURN 1 AS v")
        .await;

    let rows = h.ok("MATCH (n:BenchRow {pk: 7}) RETURN n.v AS v").await;
    assert_eq!(rows.rows, vec![vec![json!("seven")]]);
    assert_eq!(h.count("BenchRow").await, 1);
}

/// A FOREACH body may only contain updating clauses (Neo4j: SET, REMOVE, CREATE,
/// MERGE, DELETE, FOREACH), so a CREATE inside it is a write and creates the
/// label implicitly. Reads never create — see `i5_unknown_label_reads_are_unchanged`.
#[tokio::test]
async fn foreach_with_a_create_creates_the_label() {
    let h = Harness::new();

    h.ok("FOREACH (x IN ['a', 'b'] | CREATE (n:ForeachRow {name: x}))")
        .await;

    assert_eq!(h.count("ForeachRow").await, 2);
}

#[tokio::test]
async fn merge_on_an_unknown_label_creates_then_upserts() {
    let h = Harness::new();
    let query = "MERGE (n:MergeRow {pk: 1}) SET n.v = 'merged' RETURN 1 AS v";

    h.ok(query).await;
    h.ok(query).await;

    assert_eq!(h.count("MergeRow").await, 1, "MERGE stays idempotent");
    let rows = h.ok("MATCH (n:MergeRow) RETURN n.v AS v").await;
    assert_eq!(
        rows.rows,
        vec![vec![json!("merged")]],
        "and the SET property exists as a column of the implicitly created table"
    );
}

// Schema-on-write relationships must reuse both endpoints' storage keys. UUID
// fallback keys are 16 bytes; inline and MATCH-bound endpoints must agree so
// the new edge is also visible to traversal and deletion.
#[tokio::test]
async fn merge_of_an_unknown_relationship_type_creates_the_edge_table() {
    let h = Harness::new();

    h.ok("MERGE (a:Person {name: 'Alice'}) RETURN 1 AS v").await;
    h.ok("MERGE (b:Person {name: 'Bob'}) RETURN 1 AS v").await;
    let query = "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
                 MERGE (a)-[r:KNOWS {since: 2024}]->(b) RETURN 1 AS v";
    h.ok(query).await;
    h.ok(query).await;

    let edges: Vec<_> = h
        .graph_tables()
        .into_iter()
        .filter(|(_, label, _)| label == "KNOWS")
        .collect();
    assert_eq!(
        edges,
        vec![(
            "knows_e".to_string(),
            "KNOWS".to_string(),
            "edge".to_string()
        )]
    );
    let edge = h.table_meta("knows_e").unwrap();
    assert_eq!(
        edge.extensions.get("graph.source_label"),
        Some(&"Person".to_string()),
        "an implicit edge table carries the endpoint labels the DDL contract requires"
    );
    assert_eq!(
        edge.extensions.get("graph.target_label"),
        Some(&"Person".to_string())
    );
    let read = h
        .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS v")
        .await;
    assert_eq!(read.rows, vec![vec![json!(2024)]]);
}
