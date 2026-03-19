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

use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_graph::http::{build_router, AppState};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy, Permission,
    RateLimitConfig, Resource, Schema, SchemaConfig, TestAuditSink,
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
        data_dir: dir.path().to_path_buf(),
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
    let engine = Arc::new(GraphEngine::new(
        Arc::clone(&schema),
        Arc::clone(&storage),
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
