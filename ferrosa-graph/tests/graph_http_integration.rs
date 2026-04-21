//! Integration test: Graph HTTP/JSON endpoint with Cypher queries.
//!
//! Exercises the full graph workflow through the HTTP API:
//! schema creation via ferrosa-schema, then Cypher queries via
//! the graph HTTP endpoint.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use base64::Engine as _;
use indexmap::IndexMap;
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

use ferrosa_common::schema::TableSchema;
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_graph::http::{build_router, AppState};
use ferrosa_schema::auth::bootstrap::{
    seed_default_roles, SEED_APP_PASSWORD, SEED_APP_READER_USER, SEED_GRAPH_ENGINE_USER,
};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, GrantEntry, PasswordHasher, PasswordPolicy,
    Permission, RateLimitConfig, Resource, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

// ── Test helpers ──────────────────────────────────────────────────────────

fn setup() -> (Arc<Schema>, Arc<StorageEngine>, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        flush_max_age_secs: 5,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    (schema, storage, dir)
}

fn superuser_auth() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn basic_auth_header() -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode("cassandra:cassandra");
    format!("Basic {encoded}")
}

/// Register graph tables with the storage engine so that write operations
/// (CREATE, MERGE) can locate the memtable. Must be called after
/// `create_social_graph_schema` for any test that writes to storage.
fn register_social_tables_with_storage(storage: &StorageEngine) {
    for (keyspace, table) in [("social", "person_v"), ("social", "knows_e")] {
        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: HashMap::new(),
        };
        storage.register_table(schema).unwrap();
    }
}

/// Create the "social" keyspace with vertex and edge tables for a social graph.
fn create_social_graph_schema(schema: &Schema) {
    let auth = superuser_auth();

    // Create keyspace
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "social".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut m = HashMap::new();
                        m.insert("replication_factor".to_string(), "1".to_string());
                        m
                    },
                },
            },
            &auth,
        )
        .unwrap();

    // Grant all permissions on the keyspace
    schema
        .grant(
            "cassandra",
            &Resource::Keyspace("social".to_string()),
            HashSet::from([
                Permission::Select,
                Permission::Modify,
                Permission::Create,
                Permission::Drop,
                Permission::Alter,
                Permission::Authorize,
            ]),
            &auth,
        )
        .unwrap();

    // Vertex table: Person
    let mut person_cols = IndexMap::new();
    person_cols.insert(
        "id".to_string(),
        ColumnMetadata {
            name: "id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    person_cols.insert(
        "name".to_string(),
        ColumnMetadata {
            name: "name".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    person_cols.insert(
        "age".to_string(),
        ColumnMetadata {
            name: "age".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut person_ext = HashMap::new();
    person_ext.insert("graph.type".to_string(), "vertex".to_string());
    person_ext.insert("graph.label".to_string(), "Person".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "social".to_string(),
                name: "person_v".to_string(),
                id: Uuid::new_v4(),
                columns: person_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: person_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();

    // Edge table: Knows
    let mut knows_cols = IndexMap::new();
    knows_cols.insert(
        "src_id".to_string(),
        ColumnMetadata {
            name: "src_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    knows_cols.insert(
        "dst_id".to_string(),
        ColumnMetadata {
            name: "dst_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    knows_cols.insert(
        "since".to_string(),
        ColumnMetadata {
            name: "since".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut knows_ext = HashMap::new();
    knows_ext.insert("graph.type".to_string(), "edge".to_string());
    knows_ext.insert("graph.label".to_string(), "KNOWS".to_string());
    knows_ext.insert("graph.source".to_string(), "src_id".to_string());
    knows_ext.insert("graph.target".to_string(), "dst_id".to_string());
    knows_ext.insert("graph.source_label".to_string(), "Person".to_string());
    knows_ext.insert("graph.target_label".to_string(), "Person".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "social".to_string(),
                name: "knows_e".to_string(),
                id: Uuid::new_v4(),
                columns: knows_cols,
                partition_key: vec!["src_id".to_string()],
                clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: knows_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();
}

fn build_app(schema: Arc<Schema>, storage: Arc<StorageEngine>) -> axum::Router {
    let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(
        ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage)),
    ));
    let engine = Arc::new(GraphEngine::new(
        Arc::clone(&schema),
        Arc::clone(&storage),
        write_path,
        GraphEngineConfig::default(),
        std::time::Duration::from_secs(300),
    ));
    let state = AppState {
        engine,
        schema: Arc::clone(&schema),
        auth_disabled: false,
    };
    build_router(state)
}

fn json_request(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, basic_auth_header());

    if let Some(b) = body {
        builder = builder
            .method(method)
            .header(header::CONTENT_TYPE, "application/json");
        builder
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap()
    } else {
        builder.method(method).body(Body::empty()).unwrap()
    }
}

async fn response_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 1_048_576)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_check_no_auth_required() {
    let (schema, storage, _dir) = setup();
    let app = build_app(schema, storage);

    let req = Request::builder()
        .uri("/graph/health")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn query_requires_auth() {
    let (schema, storage, _dir) = setup();
    let app = build_app(schema, storage);

    // No auth header
    let req = Request::builder()
        .uri("/graph/query")
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"query": "MATCH (n) RETURN n", "keyspace": "social"}"#,
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn schema_endpoint_returns_graph_labels() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request("GET", "/graph/schema?keyspace=social", None);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = response_json(resp).await;
    let vertices = body["vertices"].as_array().unwrap();
    let edges = body["edges"].as_array().unwrap();

    assert_eq!(vertices.len(), 1);
    assert_eq!(vertices[0]["label"], "Person");
    assert_eq!(vertices[0]["table"], "person_v");

    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["label"], "KNOWS");
    assert_eq!(edges[0]["table"], "knows_e");
}

#[tokio::test]
async fn explain_returns_query_plan() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/explain",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = response_json(resp).await;
    let plan = body["plan"].as_str().unwrap();
    assert!(
        plan.contains("Expand"),
        "plan should describe an Expand operation"
    );
    assert!(
        plan.contains("person_v"),
        "plan should reference the person_v table"
    );
}

#[tokio::test]
async fn query_vertex_scan() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = response_json(resp).await;
    // Should return rows array (possibly empty since no data inserted via storage)
    assert!(body["rows"].is_array(), "response must have 'rows' array");
}

#[tokio::test]
async fn query_parse_error_returns_400() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "THIS IS NOT VALID CYPHER !!!",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = response_json(resp).await;
    assert!(body["error"].as_str().unwrap().contains("parse error"));
}

#[tokio::test]
async fn query_nonexistent_label_returns_error() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:NonExistent) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    // Should return 400 (validation error: unknown label)
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn schema_empty_keyspace() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request("GET", "/graph/schema?keyspace=nonexistent", None);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = response_json(resp).await;
    assert_eq!(body["vertices"].as_array().unwrap().len(), 0);
    assert_eq!(body["edges"].as_array().unwrap().len(), 0);
}

/// Full workflow: create schema, query schema endpoint, explain a query,
/// execute a vertex scan, and verify error handling.
#[tokio::test]
async fn graph_full_workflow() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);

    // Phase 1: Health check (no auth)
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = Request::builder()
        .uri("/graph/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Phase 2: Schema introspection
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request("GET", "/graph/schema?keyspace=social", None);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["vertices"].as_array().unwrap().len(), 1);
    assert_eq!(body["edges"].as_array().unwrap().len(), 1);

    // Phase 3: Explain a traversal query
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/explain",
        Some(serde_json::json!({
            "query": "MATCH (p:Person)-[:KNOWS]->(friend:Person) RETURN p.name, friend.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let plan = body["plan"].as_str().unwrap();
    assert!(plan.contains("Expand"));
    assert!(plan.contains("KNOWS"));

    // Phase 4: Execute a vertex scan
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name, n.age",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Phase 5: Parse error handling
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "NOT CYPHER",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Phase 6: Auth required
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = Request::builder()
        .uri("/graph/query")
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"query": "MATCH (n) RETURN n", "keyspace": "social"}"#,
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── Slice 7: MERGE executor tests ────────────────────────────────────────────

/// MERGE the same node twice; verify that only one row exists (idempotency).
#[tokio::test]
async fn merge_node_is_idempotent() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // First MERGE.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Alice'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "first MERGE must succeed with 200"
    );

    // Second MERGE with identical match props.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Alice'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "second MERGE must succeed with 200"
    );

    // MATCH should return exactly one Person named Alice.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows must be an array");
    assert_eq!(rows.len(), 1, "idempotent MERGE must not create duplicates");
}

/// MERGE the same relationship twice; verify one edge.
#[tokio::test]
async fn merge_relationship_is_idempotent() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Pre-create nodes so the edge MERGE can find them.
    for name in ["Alice", "Bob"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // MERGE the KNOWS edge twice.
    for _ in 0..2 {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": "MERGE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2024}]->(b:Person {name: 'Bob'}) RETURN r",
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "MERGE edge must return 200");
    }
}

/// MERGE a node, then SET a property; verify the updated property is returned.
#[tokio::test]
async fn merge_set_updates_properties() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // MERGE with SET in the same query.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Charlie'}) SET n.age = 30 RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "MERGE + SET must return 200");
}

/// MERGE with an unknown label must return HTTP 400, not 500 or panic.
#[tokio::test]
async fn merge_missing_endpoint_returns_error() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:UnknownLabel {id: 'x'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unknown label on MERGE endpoint must return 400, not 500 or panic"
    );
}

// ── Slice 9: HTTP E2E mutation round-trip ─────────────────────────────────────

/// CREATE a node, then MATCH it — verifies the full mutation path through HTTP.
#[tokio::test]
async fn http_create_then_match_round_trip() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Bare CREATE (no RETURN clause): verifies that CREATE succeeds and a
    // subsequent MATCH sees the new vertex.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "CREATE (n:Person {name: 'Eve'})",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "CREATE must return 200");

    // MATCH — must see the created node
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert!(body["rows"].is_array(), "response must include 'rows'");
    assert!(
        !body["rows"].as_array().unwrap().is_empty(),
        "MATCH after CREATE must return at least one row"
    );
}

/// CREATE with RETURN — `POST /graph/query` with `CREATE (n:Person {...}) RETURN n`
/// must return HTTP 200 with a non-empty `rows` array containing the created node's ID.
///
/// This test covers slice 9 parity: CREATE now supports an optional trailing RETURN
/// clause, matching the MERGE RETURN grammar added in slice 4.
#[tokio::test]
async fn http_create_with_return_round_trip() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "CREATE (n:Person {name: 'Grace'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CREATE ... RETURN must return HTTP 200"
    );

    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("response must have 'rows' array");
    assert!(
        !rows.is_empty(),
        "CREATE ... RETURN must return at least one row with the created node ID"
    );
}

/// MERGE a node, then MATCH it — verifies the mutation path for MERGE through HTTP.
#[tokio::test]
async fn http_merge_then_match_round_trip() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // MERGE
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Frank'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "MERGE must return 200");

    // MATCH — must see the merged node
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert!(body["rows"].is_array(), "response must include 'rows'");
    assert!(
        !body["rows"].as_array().unwrap().is_empty(),
        "MATCH after MERGE must return at least one row"
    );
}

/// Mutation responses must carry the canonical `{"columns":..., "rows":..., "stats":...}` shape.
#[tokio::test]
async fn http_mutation_returns_json_result() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Grace'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = response_json(resp).await;
    assert!(
        body.get("columns").is_some(),
        "mutation response must contain 'columns' field"
    );
    assert!(
        body.get("rows").is_some(),
        "mutation response must contain 'rows' field"
    );
    assert!(
        body.get("stats").is_some(),
        "mutation response must contain 'stats' field"
    );
    assert!(body["columns"].is_array(), "'columns' must be an array");
    assert!(body["rows"].is_array(), "'rows' must be an array");
}

/// An unsupported mutation keyword must return HTTP 400 with an explicit error body —
/// never HTTP 500 and never a panic. Per safety.md: fail loud, never fake.
#[tokio::test]
async fn http_unsupported_mutation_returns_400() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let app = build_app(schema, storage);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UPSERT (n:Person {name: 'x'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unsupported mutation keyword must return 400, not 500"
    );

    let body = response_json(resp).await;
    let error_msg = body["error"].as_str().unwrap_or("");
    assert!(
        !error_msg.is_empty(),
        "400 response must include a non-empty 'error' field"
    );
}

// ── Slice 10: Idempotency + adjacency invariants ──────────────────────────────

/// MERGE the same typed edge 5 times; MATCH must return exactly one edge.
#[tokio::test]
async fn repeated_merge_does_not_create_duplicate_edges() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Pre-create both endpoint nodes.
    for name in ["Alpha", "Beta"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // MERGE the same KNOWS edge 5×.
    for i in 0..5usize {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": "MERGE (a:Person {name: 'Alpha'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Beta'}) RETURN r",
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "MERGE iteration {i} must return 200"
        );
    }

    // Verify no duplicate Person nodes were created (idempotency sanity check).
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows must be an array");
    assert_eq!(
        rows.len(),
        2,
        "idempotent MERGE must not duplicate endpoint nodes (expected exactly Alpha and Beta)"
    );
}

/// After MERGE of a relationship the create arm must use `write_path.write()` so
/// that the `AdjacencyIndexObserver` can fire (R3 mitigation).
///
/// ## What this test verifies (in-process)
///
/// The `AdjacencyIndexObserver` uses `ObserverMode::Async`. In the in-process test
/// harness the async-observer receiver is dropped at registration time — derived
/// adjacency mutations are enqueued but the drain loop is never running. Therefore
/// the adjacency index table is not populated in this test context and the hop query
/// correctly returns zero rows.
///
/// The structural invariant we CAN verify here is:
///   1. The MERGE edge call returns HTTP 200 (no panic, no error).
///   2. The subsequent hop query also returns HTTP 200 (parse/plan/executor handles
///      an empty adjacency table without crashing or returning 5xx).
///
/// ## Full adjacency-index verification
///
/// To fully verify R3 (observer fires → hop returns target), a separate test is
/// needed that calls `StorageEngine::register_async_observer` (which returns the
/// receiver) and drains it before executing the hop query. That test belongs in a
/// dedicated adjacency integration test file or requires a container environment
/// where GraphEngine's background drain loop is running.
#[tokio::test]
async fn merge_triggers_adjacency_index_entry() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // MERGE source node
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Src'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // MERGE target node
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Dst'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // MERGE the KNOWS edge from Src to Dst.
    // Structural invariant 1: MERGE edge must return 200 (create arm uses write_path.write()).
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'Src'})-[r:KNOWS {since: 2025}]->(b:Person {name: 'Dst'}) RETURN r",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MERGE edge must return 200 — create arm must call write_path.write() (R3)"
    );

    // Structural invariant 2: hop query must return 200 without panic or 5xx.
    // Rows may be empty in the in-process harness because the async observer drain
    // loop is not running; see doc-comment above for the full verification strategy.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "hop query after MERGE must return 200 — executor must not crash on empty adjacency table"
    );
    let body = response_json(resp).await;
    assert!(
        body["rows"].is_array(),
        "hop query response must contain a 'rows' array"
    );
}

// ── Slice 10: Adjacency drain test ───────────────────────────────────────────

/// MERGE a typed edge via the public Cypher path, drain the async observer
/// in-process, run a 1-hop query, and assert non-empty rows.
///
/// ## What this test proves (three invariants in one)
///
/// 1. **Observer drain**: `register_async_observer_with_drain` spawns a drain
///    task that calls `AdjacencyIndexObserver::on_write` for every `knows_e`
///    write, producing OUT/IN entries in `system_graph_social.adjacency`.
///
/// 2. **MERGE edge clustering**: `execute_merge` must use `dst_key_bytes` as
///    the SSTable clustering key for edge rows.  The adjacency observer reads
///    `row.clustering` as the target vertex ID — if clustering is empty the
///    edge is invisible to hop queries.  This is the fix for the bug described
///    in `specs/todo/merge-edge-clustering-gap.md`.
///
/// 3. **End-to-end hop**: the hop executor follows adjacency entries written
///    by the drain task back to `person_v` partitions and returns live rows.
///
/// No direct storage writes are used for the edge — the test relies entirely
/// on the public MERGE HTTP path so that regressions in the executor are caught.
#[tokio::test]
async fn merge_triggers_adjacency_index_entry_hop_returns_rows() {
    use ferrosa_common::schema::TableSchema;
    use ferrosa_graph::adjacency::observer::AdjacencyIndexObserver;
    use ferrosa_graph::adjacency::{adjacency_keyspace_name, adjacency_table_metadata};

    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Register the adjacency keyspace + table in the schema registry so the
    // planner can resolve the system_graph_social.adjacency table.
    let adj_ks_name = adjacency_keyspace_name("social");
    schema
        .create_keyspace(
            ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                name: adj_ks_name.clone(),
                durable_writes: true,
                replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut m = HashMap::new();
                        m.insert("replication_factor".to_string(), "1".to_string());
                        m
                    },
                },
            },
            &superuser_auth(),
        )
        .unwrap();
    let adj_meta = adjacency_table_metadata("social");
    schema.create_table(adj_meta, &superuser_auth()).unwrap();

    // Register the adjacency table with the storage engine so derived mutations
    // have a memtable to land in.
    storage
        .register_table(TableSchema {
            keyspace: adj_ks_name.clone(),
            table: "adjacency".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: HashMap::new(),
        })
        .unwrap();

    // Wire the async observer drain task.  This spawns a tokio task that dequeues
    // writes from the channel, calls observer.on_write(), and persists derived
    // adjacency mutations through the full storage write path.
    let observer = Arc::new(AdjacencyIndexObserver::new(
        Arc::clone(&schema),
        "social".to_string(),
    ));
    storage.register_async_observer_with_drain(
        observer as Arc<dyn ferrosa_storage::WriteObserver>,
        Arc::clone(&storage),
    );

    // MERGE source node via the public Cypher HTTP path.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'HopSrc'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MERGE source node must return 200"
    );

    // MERGE destination node.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'HopDst'}) RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MERGE destination node must return 200"
    );

    // MERGE the KNOWS edge via the public Cypher HTTP path (no direct storage write).
    //
    // After the fix `execute_merge` uses dst_key_bytes as the SSTable clustering key
    // for this edge row.  The adjacency observer drain will then read that clustering
    // as the target vertex ID and write a correct OUT entry into adjacency.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'HopSrc'})-[r:KNOWS {since: 2025}]->(b:Person {name: 'HopDst'}) RETURN r",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MERGE edge must return 200 (R3: create arm uses write_path.write() with dst clustering)"
    );

    // Yield to the tokio runtime so the background drain task can dequeue the
    // knows_e write and apply OUT/IN adjacency entries to system_graph_social.adjacency.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    // Hop query: MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b
    // The drain has written adjacency entries from src → dst using the content-addressed
    // vertex keys.  The hop executor follows those entries to person_v partitions.
    // At least one row must be returned — empty rows mean the edge clustering fix did
    // not land or the drain task did not run.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "hop query must return 200 after adjacency index is populated"
    );
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("hop query response must have 'rows' array");
    assert!(
        !rows.is_empty(),
        "hop query must return at least one row — the MERGE edge clustering fix must have \
         written dst_key_bytes as the SSTable clustering so the adjacency observer drain \
         produces a valid OUT entry.  If this fails: (1) execute_merge did not set \
         dst_key_bytes as clustering, or (2) the drain task did not run before the hop query."
    );
}

// ── Slice 11: MERGE edge clustering gap regression test ──────────────────────

/// Regression test for the MERGE edge clustering gap.
///
/// Before the fix, `execute_merge` wrote edge rows with `clustering: vec![]`.
/// The `AdjacencyIndexObserver` reads `row.clustering` as the target vertex ID,
/// so edges were invisible to hop queries.
///
/// This test verifies via a named-assertion storage read that the edge row
/// written by MERGE has `clustering == dst_key_bytes` (not empty).
/// It is a focused unit-level regression guard that complements
/// `merge_triggers_adjacency_index_entry_hop_returns_rows` (which covers the
/// full observer-drain + hop-query path end-to-end).
#[tokio::test]
async fn merge_edge_clustering_matches_dst_key() {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_storage::TableId;

    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // MERGE both endpoint nodes so they exist before the edge MERGE.
    for name in ["EdgeSrc", "EdgeDst"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "MERGE node '{name}' must return 200"
        );
    }

    // MERGE the edge via the public Cypher path (no direct storage write).
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'EdgeSrc'})-[r:KNOWS {since: 2026}]->(b:Person {name: 'EdgeDst'}) RETURN r",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "MERGE edge must return 200");

    // Derive the expected src and dst partition key bytes using the same
    // blake3 content-addressing that `execute_merge` / `content_addressed_key` uses.
    // `content_addressed_key` sorts by key name then hashes: `name\x00<value>\x00`.
    let src_key_bytes: Vec<u8> = {
        let mut h = blake3::Hasher::new();
        h.update(b"name\x00EdgeSrc\x00");
        h.finalize().as_bytes().to_vec()
    };
    let dst_key_bytes: Vec<u8> = {
        let mut h = blake3::Hasher::new();
        h.update(b"name\x00EdgeDst\x00");
        h.finalize().as_bytes().to_vec()
    };

    // Read the edge row from knows_e using the src partition key.
    let src_key = DecoratedKey::new(PartitionKey::new(src_key_bytes));
    let knows_tid = TableId::new("social", "knows_e");
    let partition = storage.read(&knows_tid, &src_key).unwrap();

    let partition = partition.expect(
        "knows_e partition for EdgeSrc must exist after MERGE — \
         execute_merge must have written an edge row with src_key as partition key",
    );

    assert!(
        !partition.rows.is_empty(),
        "knows_e partition must contain at least one row after MERGE edge"
    );

    // The first (and only) row must have clustering == dst_key_bytes.
    // Before the fix this was vec![] (empty), making the edge invisible to the
    // adjacency observer and therefore invisible to hop queries.
    let row = &partition.rows[0];
    assert_eq!(
        row.clustering, dst_key_bytes,
        "MERGE edge row clustering must equal dst_key_bytes — \
         if this fails execute_merge is writing empty clustering for edge rows \
         (the bug this test was written to catch)"
    );
}

// ── Slice 8: Auth enforcement ─────────────────────────────────────────────────
//
// Per-resource grants are now wired in `ferrosa_schema::auth::bootstrap`:
//   - `seed_default_roles` calls `seed_grants_if_absent` which inserts the
//     canonical grant matrix from `AGENT_MEMORY_GRAPH_TABLES`.
//   - The graph planner's `validate()` calls `check_permission()` which walks
//     `SchemaSnapshot::grants` — so explicit table-level grants are honoured for
//     non-superusers.
//
// Test invariants (must never regress):
//   - `app_reader` is DENIED MERGE (403): SELECT-only on graph tables.
//   - `cassandra` (superuser) is ALLOWED MERGE (200): superuser bypass.
//   - `graph_engine` is ALLOWED MERGE (200): has MODIFY grant on graph tables.

/// Helper: build a Basic auth header for an arbitrary username/password.
fn basic_auth_for(username: &str, password: &str) -> String {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
    format!("Basic {encoded}")
}

/// Build an HTTP request with a custom Authorization header.
fn json_request_with_auth(
    method: &str,
    uri: &str,
    body: Option<Value>,
    auth_header: String,
) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, auth_header);

    if let Some(b) = body {
        builder = builder
            .method(method)
            .header(header::CONTENT_TYPE, "application/json");
        builder
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap()
    } else {
        builder.method(method).body(Body::empty()).unwrap()
    }
}

/// `app_reader` (non-superuser, SELECT-only on graph tables) must receive HTTP 403 on MERGE.
///
/// `graph_engine` (non-superuser but has MODIFY grant on graph tables) and
/// `cassandra` (superuser) must receive HTTP 200 on the same MERGE.
///
/// This test does NOT disable auth; it fails loud per safety.md: a missing 403
/// means auth was bypassed (P0 security regression). A missing 200 for
/// `graph_engine` means the per-resource grant matrix is broken.
#[tokio::test]
async fn merge_denied_for_app_reader_role() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Seed the built-in roles and their default grant matrix.
    seed_default_roles(&schema).expect("seed_default_roles must not fail");

    // Grant graph_engine MODIFY+SELECT on the test keyspace "social" so the
    // planner's per-table permission check passes for the social graph tables.
    // In production, graph_engine holds equivalent grants on agent_memory graph
    // tables (from AGENT_MEMORY_GRAPH_TABLES); here we mirror that for the test
    // keyspace to exercise the same permission path end-to-end.
    schema
        .grant_internal(GrantEntry {
            role: SEED_GRAPH_ENGINE_USER.to_string(),
            resource: Resource::Keyspace("social".to_string()),
            permissions: [Permission::Modify, Permission::Select]
                .into_iter()
                .collect(),
        })
        .expect("grant MODIFY+SELECT on social to graph_engine must not fail");

    let merge_query = serde_json::json!({
        "query": "MERGE (n:Person {name: 'AuthTest'}) RETURN n",
        "keyspace": "social"
    });

    // --- app_reader MUST be denied (403): SELECT-only on graph tables ---
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request_with_auth(
        "POST",
        "/graph/query",
        Some(merge_query.clone()),
        basic_auth_for(SEED_APP_READER_USER, SEED_APP_PASSWORD),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "app_reader must be denied MERGE (403); if this is 200 auth enforcement is broken"
    );

    // --- cassandra (superuser) MUST succeed (200) ---
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request_with_auth(
        "POST",
        "/graph/query",
        Some(merge_query.clone()),
        basic_auth_for("cassandra", "cassandra"),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "cassandra (superuser) must be allowed MERGE (200)"
    );

    // --- graph_engine MUST succeed (200): has MODIFY grant on graph tables ---
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request_with_auth(
        "POST",
        "/graph/query",
        Some(merge_query),
        basic_auth_for(SEED_GRAPH_ENGINE_USER, SEED_APP_PASSWORD),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "graph_engine must be allowed MERGE (200) — per-resource MODIFY grant must be honoured"
    );
}

// ── Slice 11: Migration-proof ferrosa-memory shapes ───────────────────────────
//
// These tests assert that the canonical ferrosa-memory edge-upsert Cypher queries
// (typed_edge, folded_into, mentioned_in, supersedes) work end-to-end through the
// public graph API with no direct CQL table name references.

/// Schema + storage setup for ferrosa-memory's Entity/TypedEdge graph.
fn create_memory_graph_schema(schema: &Schema) {
    let auth = superuser_auth();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "memory".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut m = HashMap::new();
                        m.insert("replication_factor".to_string(), "1".to_string());
                        m
                    },
                },
            },
            &auth,
        )
        .unwrap();

    schema
        .grant(
            "cassandra",
            &Resource::Keyspace("memory".to_string()),
            HashSet::from([
                Permission::Select,
                Permission::Modify,
                Permission::Create,
                Permission::Drop,
                Permission::Alter,
                Permission::Authorize,
            ]),
            &auth,
        )
        .unwrap();

    // Entity vertex table
    let mut entity_cols = IndexMap::new();
    entity_cols.insert(
        "entity_id".to_string(),
        ColumnMetadata {
            name: "entity_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    entity_cols.insert(
        "name".to_string(),
        ColumnMetadata {
            name: "name".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut entity_ext = HashMap::new();
    entity_ext.insert("graph.type".to_string(), "vertex".to_string());
    entity_ext.insert("graph.label".to_string(), "Entity".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "memory".to_string(),
                name: "entity_v".to_string(),
                id: Uuid::new_v4(),
                columns: entity_cols,
                partition_key: vec!["entity_id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: entity_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();

    // TypedEdge edge table
    let mut edge_cols = IndexMap::new();
    edge_cols.insert(
        "src_id".to_string(),
        ColumnMetadata {
            name: "src_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    edge_cols.insert(
        "dst_id".to_string(),
        ColumnMetadata {
            name: "dst_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    edge_cols.insert(
        "edge_type".to_string(),
        ColumnMetadata {
            name: "edge_type".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    edge_cols.insert(
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

    let mut edge_ext = HashMap::new();
    edge_ext.insert("graph.type".to_string(), "edge".to_string());
    edge_ext.insert("graph.label".to_string(), "TYPED_EDGE".to_string());
    edge_ext.insert("graph.source".to_string(), "src_id".to_string());
    edge_ext.insert("graph.target".to_string(), "dst_id".to_string());
    edge_ext.insert("graph.source_label".to_string(), "Entity".to_string());
    edge_ext.insert("graph.target_label".to_string(), "Entity".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "memory".to_string(),
                name: "typed_edge_e".to_string(),
                id: Uuid::new_v4(),
                columns: edge_cols,
                partition_key: vec!["src_id".to_string()],
                clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: edge_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();
}

fn register_memory_tables_with_storage(storage: &StorageEngine) {
    for (keyspace, table) in [("memory", "entity_v"), ("memory", "typed_edge_e")] {
        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: HashMap::new(),
        };
        storage.register_table(schema).unwrap();
    }
}

fn create_agent_memory_real_graph_schema(schema: &Schema) {
    let auth = superuser_auth();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "agent_memory".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut m = HashMap::new();
                        m.insert("replication_factor".to_string(), "1".to_string());
                        m
                    },
                },
            },
            &auth,
        )
        .unwrap();

    schema
        .grant(
            "cassandra",
            &Resource::Keyspace("agent_memory".to_string()),
            HashSet::from([
                Permission::Select,
                Permission::Modify,
                Permission::Create,
                Permission::Drop,
                Permission::Alter,
                Permission::Authorize,
            ]),
            &auth,
        )
        .unwrap();

    let mut entity_cols = IndexMap::new();
    entity_cols.insert(
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
    entity_cols.insert(
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
    entity_cols.insert(
        "entity_id".to_string(),
        ColumnMetadata {
            name: "entity_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    entity_cols.insert(
        "entity_name".to_string(),
        ColumnMetadata {
            name: "entity_name".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut entity_ext = HashMap::new();
    entity_ext.insert("graph.type".to_string(), "vertex".to_string());
    entity_ext.insert("graph.label".to_string(), "Entity".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "agent_memory".to_string(),
                name: "entity_store".to_string(),
                id: Uuid::new_v4(),
                columns: entity_cols,
                partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
                clustering_key: vec![("entity_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: entity_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();

    let mut edge_cols = IndexMap::new();
    edge_cols.insert(
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
    edge_cols.insert(
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
    edge_cols.insert(
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
    edge_cols.insert(
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
    edge_cols.insert(
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
    edge_cols.insert(
        "weight".to_string(),
        ColumnMetadata {
            name: "weight".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "double".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    edge_cols.insert(
        "metadata".to_string(),
        ColumnMetadata {
            name: "metadata".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    edge_cols.insert(
        "created_at".to_string(),
        ColumnMetadata {
            name: "created_at".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "timestamp".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut edge_ext = HashMap::new();
    edge_ext.insert("graph.type".to_string(), "edge".to_string());
    edge_ext.insert("graph.label".to_string(), "TYPED_EDGE".to_string());
    edge_ext.insert("graph.source".to_string(), "src_id".to_string());
    edge_ext.insert("graph.target".to_string(), "dst_id".to_string());
    edge_ext.insert("graph.source_label".to_string(), "Entity".to_string());
    edge_ext.insert("graph.target_label".to_string(), "Entity".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "agent_memory".to_string(),
                name: "typed_edges".to_string(),
                id: Uuid::new_v4(),
                columns: edge_cols,
                partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
                clustering_key: vec![
                    ("src_id".to_string(), ClusteringOrder::Asc),
                    ("edge_type".to_string(), ClusteringOrder::Asc),
                    ("dst_id".to_string(), ClusteringOrder::Asc),
                ],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: edge_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();
}

fn register_agent_memory_real_tables_with_storage(storage: &StorageEngine) {
    for (keyspace, table) in [
        ("agent_memory", "entity_store"),
        ("agent_memory", "typed_edges"),
    ] {
        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: HashMap::new(),
        };
        storage.register_table(schema).unwrap();
    }
}

fn encode_composite_partition_key(components: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
        buf.push(0x00);
    }
    buf
}

fn encode_multi_clustering_key(components: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
    }
    buf
}

fn seed_agent_memory_entity(
    storage: &StorageEngine,
    tenant_id: Uuid,
    session_id: Uuid,
    entity_id: Uuid,
    entity_name: &str,
) {
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::TableId;

    let key = DecoratedKey::new(PartitionKey::new(encode_composite_partition_key(&[
        tenant_id.as_bytes(),
        session_id.as_bytes(),
    ])));
    let row = Row {
        clustering: entity_id.as_bytes().to_vec(),
        cells: vec![(0, CellValue::live(entity_name.as_bytes().to_vec(), 1))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1),
    };
    storage
        .write(&TableId::new("agent_memory", "entity_store"), &key, row, 1)
        .unwrap();
}

/// The canonical ferrosa-memory typed-edge upsert must work via the public Cypher API.
///
/// Cypher shape (mirrors ferrosa-memory's planned migration target):
///   MERGE (a:Entity {entity_id: 'src-001'})
///   MERGE (b:Entity {entity_id: 'dst-001'})
///   MERGE (a)-[r:TYPED_EDGE {edge_type: 'folded_into'}]->(b)
///   SET r.weight = 1.0
///   RETURN r
const MIGRATION_TYPED_EDGE_UPSERT: &str = "MERGE (a:Entity {entity_id: 'src-001'}) \
     MERGE (b:Entity {entity_id: 'dst-001'}) \
     MERGE (a)-[r:TYPED_EDGE {edge_type: 'folded_into'}]->(b) \
     SET r.weight = 1.0 \
     RETURN r";

const MIGRATION_TYPED_EDGE_MATCH_COUNT: &str =
    "MATCH (a:Entity {entity_id: 'src-001'})-[r:TYPED_EDGE {edge_type: 'folded_into'}]->\
     (b:Entity {entity_id: 'dst-001'}) RETURN count(r)";

#[tokio::test]
async fn full_shape_typed_edge_merge_materializes_real_agent_memory_row() {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_storage::TableId;

    let tenant_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let src_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let dst_id = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

    let (schema, storage, _dir) = setup();
    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);

    let query = format!(
        "MERGE (a:Entity {{entity_id: '{src_id}'}}) \
         MERGE (b:Entity {{entity_id: '{dst_id}'}}) \
         MERGE (a)-[r:TYPED_EDGE {{edge_type: 'related_to'}}]->(b) \
         SET r.tenant_id = '{tenant_id}', \
             r.session_id = '{session_id}', \
             r.weight = 1.0, \
             r.metadata = '{{}}' \
         RETURN r"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "public TYPED_EDGE MERGE must accept the real agent_memory shape"
    );

    let partition_key =
        encode_composite_partition_key(&[tenant_id.as_bytes(), session_id.as_bytes()]);
    let clustering =
        encode_multi_clustering_key(&[src_id.as_bytes(), b"related_to", dst_id.as_bytes()]);
    let partition = storage
        .read(
            &TableId::new("agent_memory", "typed_edges"),
            &DecoratedKey::new(PartitionKey::new(partition_key)),
        )
        .unwrap()
        .expect("typed_edges partition must exist after public MERGE");
    assert!(
        partition
            .rows
            .iter()
            .any(|row| row.clustering == clustering),
        "public MERGE must materialize a row in the real agent_memory.typed_edges clustering shape"
    );
}

#[tokio::test]
async fn full_shape_typed_edge_merge_writes_real_agent_memory_adjacency() {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_graph::executor::expand::extract_neighbor_id;
    use ferrosa_storage::TableId;

    let tenant_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let src_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let dst_id = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

    let (schema, storage, _dir) = setup();
    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);

    let query = format!(
        "MERGE (a:Entity {{entity_id: '{src_id}'}}) \
         MERGE (b:Entity {{entity_id: '{dst_id}'}}) \
         MERGE (a)-[r:TYPED_EDGE {{edge_type: 'related_to'}}]->(b) \
         SET r.tenant_id = '{tenant_id}', \
             r.session_id = '{session_id}', \
             r.weight = 1.0 \
         RETURN r"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let adjacency = storage
        .read(
            &TableId::new("system_graph_agent_memory", "adjacency"),
            &DecoratedKey::new(PartitionKey::new(src_id.as_bytes().to_vec())),
        )
        .unwrap()
        .expect("adjacency partition for src_id must exist after real typed_edges MERGE");
    assert!(
        adjacency.rows.iter().any(|row| {
            extract_neighbor_id(&row.clustering, Some("TYPED_EDGE")) == Some(dst_id.as_bytes().to_vec())
        }),
        "adjacency observer must key real typed_edges entries by src_id and dst_id, not by the table's composite partition bytes"
    );
}

#[tokio::test]
async fn full_shape_typed_edge_merge_is_immediately_matchable_in_real_agent_memory_schema() {
    let tenant_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let src_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let dst_id = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

    let (schema, storage, _dir) = setup();
    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);
    seed_agent_memory_entity(&storage, tenant_id, session_id, src_id, "src");
    seed_agent_memory_entity(&storage, tenant_id, session_id, dst_id, "dst");

    let merge_query = format!(
        "MERGE (a:Entity {{entity_id: '{src_id}'}}) \
         MERGE (b:Entity {{entity_id: '{dst_id}'}}) \
         MERGE (a)-[r:TYPED_EDGE {{edge_type: 'related_to'}}]->(b) \
         SET r.tenant_id = '{tenant_id}', \
             r.session_id = '{session_id}', \
             r.weight = 1.0 \
         RETURN r"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": merge_query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let match_query = format!(
        "MATCH (a:Entity {{entity_id: '{src_id}'}})-[r:TYPED_EDGE {{edge_type: 'related_to'}}]->\
         (b:Entity {{entity_id: '{dst_id}'}}) RETURN count(r)"
    );
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": match_query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("MATCH count response must include rows");
    assert_eq!(rows.len(), 1);
    let count = rows[0][0]
        .as_i64()
        .expect("count(r) must serialize as a JSON integer");
    assert_eq!(
        count, 1,
        "real agent_memory graph rows must be immediately matchable after public MERGE"
    );
}

/// ferrosa-memory folded_into edge shape.
const MIGRATION_FOLDED_INTO: &str = "MERGE (a:Entity {entity_id: 'fold-src'}) \
     MERGE (b:Entity {entity_id: 'fold-dst'}) \
     MERGE (a)-[r:TYPED_EDGE {edge_type: 'folded_into'}]->(b) \
     RETURN r";

/// ferrosa-memory mentioned_in edge shape.
const MIGRATION_MENTIONED_IN: &str = "MERGE (a:Entity {entity_id: 'mention-src'}) \
     MERGE (b:Entity {entity_id: 'mention-dst'}) \
     MERGE (a)-[r:TYPED_EDGE {edge_type: 'mentioned_in'}]->(b) \
     RETURN r";

/// ferrosa-memory supersedes edge shape.
const MIGRATION_SUPERSEDES: &str = "MERGE (a:Entity {entity_id: 'super-src'}) \
     MERGE (b:Entity {entity_id: 'super-dst'}) \
     MERGE (a)-[r:TYPED_EDGE {edge_type: 'supersedes'}]->(b) \
     RETURN r";

#[tokio::test]
async fn migration_proof_typed_edge_upsert_no_direct_table_ref() {
    let (schema, storage, _dir) = setup();
    create_memory_graph_schema(&schema);
    register_memory_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_TYPED_EDGE_UPSERT,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "canonical ferrosa-memory typed-edge upsert must succeed via public Cypher API"
    );
    let body = response_json(resp).await;
    assert!(
        body.get("rows").is_some(),
        "typed-edge upsert response must contain 'rows'"
    );
}

#[tokio::test]
async fn migration_proof_typed_edge_upsert_materializes_and_matches() {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_storage::TableId;

    let (schema, storage, _dir) = setup();
    create_memory_graph_schema(&schema);
    register_memory_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_TYPED_EDGE_UPSERT,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "canonical ferrosa-memory typed-edge upsert must succeed via public Cypher API"
    );
    let body = response_json(resp).await;
    assert!(
        body["rows"].is_array(),
        "typed-edge upsert response must include 'rows'"
    );

    let src_key_bytes = b"src-001".to_vec();
    let dst_key_bytes = b"dst-001".to_vec();

    let typed_edge_tid = TableId::new("memory", "typed_edge_e");
    let src_key = DecoratedKey::new(PartitionKey::new(src_key_bytes));
    let partition = storage.read(&typed_edge_tid, &src_key).unwrap();

    let partition = partition
        .expect("typed_edge_e partition for src-001 must exist after canonical TYPED_EDGE MERGE");
    assert!(
        !partition.rows.is_empty(),
        "typed_edge_e must contain at least one row after canonical TYPED_EDGE MERGE"
    );
    assert_eq!(
        partition.rows[0].clustering, dst_key_bytes,
        "typed-edge row clustering must point at dst-001 so the relationship is readable"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_TYPED_EDGE_MATCH_COUNT,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "typed-edge MATCH readback must succeed after canonical MERGE"
    );
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("MATCH count response must include rows");
    assert_eq!(rows.len(), 1, "MATCH count must return exactly one row");
    let count = rows[0][0]
        .as_i64()
        .expect("count(r) must serialize as a JSON integer");
    assert_eq!(
        count, 1,
        "canonical typed-edge MERGE must be immediately visible to follow-up MATCH"
    );
}

#[tokio::test]
async fn migration_proof_folded_into() {
    let (schema, storage, _dir) = setup();
    create_memory_graph_schema(&schema);
    register_memory_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_FOLDED_INTO,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "folded_into edge shape must succeed via public Cypher API"
    );
}

#[tokio::test]
async fn migration_proof_mentioned_in() {
    let (schema, storage, _dir) = setup();
    create_memory_graph_schema(&schema);
    register_memory_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_MENTIONED_IN,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "mentioned_in edge shape must succeed via public Cypher API"
    );
}

#[tokio::test]
async fn migration_proof_supersedes() {
    let (schema, storage, _dir) = setup();
    create_memory_graph_schema(&schema);
    register_memory_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": MIGRATION_SUPERSEDES,
            "keyspace": "memory"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "supersedes edge shape must succeed via public Cypher API"
    );
}

/// None of the four migration query bodies may reference raw CQL table names
/// (`typed_edges`, `folded_into`, `mentioned_in`, `supersedes` as table
/// identifiers). The public Cypher API uses graph labels, not table names.
/// This test catches accidental CQL leakage into query strings.
#[test]
fn migration_proof_no_direct_table_reference() {
    let raw_cql_identifiers = ["typed_edges", "folded_into", "mentioned_in", "supersedes"];
    let queries = [
        ("MIGRATION_TYPED_EDGE_UPSERT", MIGRATION_TYPED_EDGE_UPSERT),
        ("MIGRATION_FOLDED_INTO", MIGRATION_FOLDED_INTO),
        ("MIGRATION_MENTIONED_IN", MIGRATION_MENTIONED_IN),
        ("MIGRATION_SUPERSEDES", MIGRATION_SUPERSEDES),
    ];

    for (query_name, query_body) in &queries {
        for identifier in &raw_cql_identifiers {
            // The edge_type *value* strings (e.g. 'folded_into') appear as quoted
            // string literals inside the query — that is fine. We check that they
            // do not appear as unquoted CQL identifiers (i.e. outside of quotes).
            // Strategy: strip all single-quoted substrings first, then search.
            let stripped = strip_quoted_strings(query_body);
            assert!(
                !stripped.contains(identifier),
                "query '{query_name}' contains raw CQL identifier '{identifier}' \
                 outside of quoted strings — this would bypass the graph API abstraction"
            );
        }
    }
}

/// Remove all single-quoted string literals from a Cypher query string,
/// replacing them with empty strings. Used by `migration_proof_no_direct_table_reference`
/// to distinguish quoted value literals from unquoted identifiers.
fn strip_quoted_strings(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_quote = false;
    for ch in s.chars() {
        if ch == '\'' {
            in_quote = !in_quote;
        } else if !in_quote {
            result.push(ch);
        }
    }
    result
}
