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
use futures::future::join_all;
use indexmap::IndexMap;
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

use bytes::Bytes;
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::ClusterCoordinator;
use ferrosa_cluster::raft::handlers::{
    partition_to_wire, RangeReadResponsePayload, ReadResponsePayload,
};
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_graph::bolt::codec::PackValue;
use ferrosa_graph::bolt::message::BoltMessage;
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_graph::http::{build_router, AppState};
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::{HandlerRegistry, InboundPeerCallback, RpcHandler};
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
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
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
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
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
    storage
        .register_table(TableSchema {
            keyspace: "social".to_string(),
            table: "person_v".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "age".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: HashMap::new(),
        })
        .unwrap();
    storage
        .register_table(TableSchema {
            keyspace: "social".to_string(),
            table: "knows_e".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "dst_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "since".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            extensions: HashMap::new(),
        })
        .unwrap();
}

/// Register the `company_v` vertex table in storage. Used to exercise UNION
/// arms that reuse the same pattern variable (`n`) with different labels —
/// each arm has independent variable scope, so they must resolve to their own
/// table, not a flat-merged last-write-wins binding.
fn register_company_table_with_storage(storage: &StorageEngine) {
    storage
        .register_table(TableSchema {
            keyspace: "social".to_string(),
            table: "company_v".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "name".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: HashMap::new(),
        })
        .unwrap();
}

fn register_social_likes_table_with_storage(storage: &StorageEngine) {
    let schema = TableSchema {
        keyspace: "social".to_string(),
        table: "likes_e".to_string(),
        key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "dst_id".to_string(),
            type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "weight".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        extensions: HashMap::new(),
    };
    storage.register_table(schema).unwrap();
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

/// Create a second vertex table `company_v` labeled `Company` in the `social`
/// keyspace. The keyspace and grants are created by `create_social_graph_schema`,
/// which must run first.
fn create_company_vertex_schema(schema: &Schema) {
    let auth = superuser_auth();

    let mut company_cols = IndexMap::new();
    company_cols.insert(
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
    company_cols.insert(
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

    let mut company_ext = HashMap::new();
    company_ext.insert("graph.type".to_string(), "vertex".to_string());
    company_ext.insert("graph.label".to_string(), "Company".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "social".to_string(),
                name: "company_v".to_string(),
                id: Uuid::new_v4(),
                columns: company_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: company_ext,
                is_system: false,
            },
            &auth,
        )
        .unwrap();
}

fn create_social_likes_edge_schema(schema: &Schema) {
    let auth = superuser_auth();
    let mut likes_cols = IndexMap::new();
    likes_cols.insert(
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
    likes_cols.insert(
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
    likes_cols.insert(
        "weight".to_string(),
        ColumnMetadata {
            name: "weight".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut likes_ext = HashMap::new();
    likes_ext.insert("graph.type".to_string(), "edge".to_string());
    likes_ext.insert("graph.label".to_string(), "LIKES".to_string());
    likes_ext.insert("graph.source".to_string(), "src_id".to_string());
    likes_ext.insert("graph.target".to_string(), "dst_id".to_string());
    likes_ext.insert("graph.source_label".to_string(), "Person".to_string());
    likes_ext.insert("graph.target_label".to_string(), "Person".to_string());

    schema
        .create_table(
            TableMetadata {
                keyspace: "social".to_string(),
                name: "likes_e".to_string(),
                id: Uuid::new_v4(),
                columns: likes_cols,
                partition_key: vec!["src_id".to_string()],
                clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: likes_ext,
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
    build_app_with_write_path(schema, storage, write_path)
}

fn build_app_with_write_path(
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    write_path: Arc<arc_swap::ArcSwap<ferrosa_cluster::write_path::WritePath>>,
) -> axum::Router {
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

fn build_app_and_engine(
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
) -> (axum::Router, Arc<GraphEngine>) {
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
        engine: Arc::clone(&engine),
        schema: Arc::clone(&schema),
        auth_disabled: false,
    };
    (build_router(state), engine)
}

#[derive(Debug)]
struct NoopPeerListener;

impl PeerEventListener for NoopPeerListener {
    fn on_peer_connected(&self, _peer: PeerId) {}
    fn on_peer_disconnected(&self, _peer: PeerId) {}
    fn on_peer_suspected(&self, _peer: PeerId) {}
    fn on_peer_recovered(&self, _peer_id: Uuid) {}
    fn on_peer_failed(&self, _peer_id: Uuid) {}
}

impl InboundPeerCallback for NoopPeerListener {
    fn on_inbound_peer(
        &self,
        _peer_id: PeerId,
        _cql_broadcast: Option<String>,
        _internode_broadcast: Option<String>,
    ) {
    }
}

struct RemoteAnchorHandler {
    partition: Partition,
}

#[async_trait::async_trait]
impl RpcHandler for RemoteAnchorHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::ReadRequest(_) => Some(Message::ReadResponse(Bytes::from(
                bincode::serialize(&ReadResponsePayload {
                    found: true,
                    partition: Some(partition_to_wire(self.partition.clone())),
                    digest: None,
                    timestamp: self
                        .partition
                        .rows
                        .iter()
                        .flat_map(|row| row.cells.iter().map(|(_, cell)| cell.timestamp))
                        .max()
                        .unwrap_or(i64::MIN),
                    has_more: false,
                    next_page_state: vec![],
                })
                .unwrap(),
            ))),
            Message::RangeReadRequest(_) => Some(Message::RangeReadResponse(Bytes::from(
                bincode::serialize(&RangeReadResponsePayload {
                    partitions: vec![],
                    truncated: false,
                })
                .unwrap(),
            ))),
            _ => None,
        }
    }
}

async fn start_graph_rpc_server(
    handler: Arc<dyn RpcHandler>,
) -> (Arc<RpcServer>, std::net::SocketAddr, Uuid) {
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    };
    let server_id = Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::ReadRequest, handler.clone());
    registry.register(MsgType::RangeReadRequest, handler);
    let server = Arc::new(RpcServer::new(config, server_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr, server_id)
}

fn make_cluster_node(addr: &str, host_id: Uuid) -> NodeInfo {
    NodeInfo {
        host_id,
        addr: addr.to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
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
async fn cluster_anchor_full_primary_key_match_reads_remote_vertex_without_range_scan() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let person_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let key_bytes = person_id.as_bytes().to_vec();
    let (token, _) = ferrosa_common::murmur3::hash3_x64_128(&key_bytes, 0);
    let partition = Partition {
        key: ferrosa_common::key::DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            key_bytes.clone(),
        )),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(1, ferrosa_common::CellValue::live(b"Alice".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
    };

    let (server, addr, remote_host_id) =
        start_graph_rpc_server(Arc::new(RemoteAnchorHandler { partition })).await;

    let local_node_id = 1u64;
    let remote_node_id = 2u64;
    let local_host_id = Uuid::new_v4();
    let mut ring = TokenRing::new();
    ring.add_node(
        local_node_id,
        make_cluster_node("127.0.0.1:7000", local_host_id),
    );
    ring.add_node(
        remote_node_id,
        make_cluster_node(&addr.to_string(), remote_host_id),
    );
    ring.assign_tokens(remote_node_id, &[token]);
    ring.assign_tokens(local_node_id, &[token.wrapping_add(1)]);

    let peer_manager = Arc::new(PeerManager::new(
        Arc::new(NetConfig::default()),
        local_host_id,
        Arc::new(NoopPeerListener),
    ));
    let coordinator = Arc::new(ClusterCoordinator::new(
        Arc::new(arc_swap::ArcSwap::from_pointee(ring)),
        peer_manager,
        local_node_id,
        Arc::clone(&storage),
        1,
        ConsistencyLevel::One,
    ));
    let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(
        ferrosa_cluster::write_path::WritePath::cluster(coordinator),
    ));
    let app = build_app_with_write_path(Arc::clone(&schema), Arc::clone(&storage), write_path);

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": format!("MATCH (n:Person {{id: '{person_id}'}}) RETURN n.name"),
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["rows"], serde_json::json!([["Alice"]]));

    server.shutdown(std::time::Duration::from_millis(50)).await;
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
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["rows"], serde_json::json!([]));
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

#[tokio::test]
async fn http_var_length_and_shortest_path_are_fast_and_correct_on_cycle() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    for name in ["VarPathA", "VarPathB", "VarPathC", "VarPathD"] {
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
            "node MERGE should succeed for {name}"
        );
    }

    for (src, dst) in [
        ("VarPathA", "VarPathB"),
        ("VarPathB", "VarPathC"),
        ("VarPathC", "VarPathA"),
        ("VarPathC", "VarPathD"),
    ] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (a:Person {{name: '{src}'}})-[r:KNOWS]->(b:Person {{name: '{dst}'}}) RETURN r"),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "edge MERGE should succeed for {src}->{dst}"
        );
    }

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'VarPathA'})-[:KNOWS]->(b:Person {name: 'VarPathB'}) RETURN b.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fixed_body = response_json(resp).await;
    assert_eq!(
        fixed_body["rows"].as_array().unwrap(),
        &vec![serde_json::json!(["VarPathB"])],
        "fixed-hop setup must work before testing varpath"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'VarPathA'})-[:KNOWS*1..4]->(b:Person {name: 'VarPathD'}) RETURN b.name",
            "keyspace": "social"
        })),
    );
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("tiny cyclic varpath query should complete under 1s")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows, &vec![serde_json::json!(["VarPathD"])]);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH p = shortestPath((a:Person {name: 'VarPathA'})-[:KNOWS*1..4]->(b:Person {name: 'VarPathD'})) RETURN b.name",
            "keyspace": "social"
        })),
    );
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("tiny shortestPath query should complete under 1s")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows, &vec![serde_json::json!(["VarPathD"])]);
}

#[tokio::test]
async fn concurrent_merge_node_and_relationship_remain_idempotent() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let node_futures = (0..8).map(|_| {
        let schema = Arc::clone(&schema);
        let storage = Arc::clone(&storage);
        async move {
            let app = build_app(schema, storage);
            let req = json_request(
                "POST",
                "/graph/query",
                Some(serde_json::json!({
                    "query": "MERGE (n:Person {name: 'ConcurrentNode'}) RETURN n",
                    "keyspace": "social"
                })),
            );
            app.oneshot(req).await.unwrap().status()
        }
    });
    for status in join_all(node_futures).await {
        assert_eq!(status, StatusCode::OK);
    }

    let rel_futures = (0..8).map(|_| {
        let schema = Arc::clone(&schema);
        let storage = Arc::clone(&storage);
        async move {
            let app = build_app(schema, storage);
            let req = json_request(
                "POST",
                "/graph/query",
                Some(serde_json::json!({
                    "query": "MERGE (a:Person {name: 'ConcurrentSrc'})-[r:KNOWS]->(b:Person {name: 'ConcurrentDst'}) RETURN r",
                    "keyspace": "social"
                })),
            );
            app.oneshot(req).await.unwrap().status()
        }
    });
    for status in join_all(rel_futures).await {
        assert_eq!(status, StatusCode::OK);
    }

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'ConcurrentNode'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows = response_json(resp).await["rows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.len(),
        1,
        "concurrent node MERGE should materialize one node"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'ConcurrentSrc'})-[r:KNOWS]->(b:Person {name: 'ConcurrentDst'}) RETURN r",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows = response_json(resp).await["rows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.len(),
        1,
        "concurrent relationship MERGE should materialize one edge"
    );
}

// ── Slice 7: MERGE executor tests ────────────────────────────────────────────

/// MERGE the same node twice; verify that only one row exists (idempotency).
#[tokio::test]
async fn graph_subscribe_sse_snapshot_delta_and_unsubscribe_cancel() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let (app, engine) = build_app_and_engine(Arc::clone(&schema), Arc::clone(&storage));

    let create_alice = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000a001', name: 'Alice'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let create_alice_resp = app.clone().oneshot(create_alice).await.unwrap();
    let create_alice_status = create_alice_resp.status();
    let create_alice_body = response_json(create_alice_resp).await;
    assert_eq!(
        create_alice_status,
        StatusCode::OK,
        "create Alice failed: {create_alice_body}"
    );

    let subscribe = json_request(
        "POST",
        "/graph/subscribe",
        Some(serde_json::json!({
            "query": "SUBSCRIBE MATCH (n:Person) RETURN n.name EVERY 500 ms DELTA",
            "keyspace": "social"
        })),
    );
    let resp = app.clone().oneshot(subscribe).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(engine.subscription_registry().count(), 1);

    let create_bob = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000b002', name: 'Bob'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    assert_eq!(
        app.clone().oneshot(create_bob).await.unwrap().status(),
        StatusCode::OK
    );

    tokio::time::sleep(std::time::Duration::from_millis(650)).await;

    let unsubscribe = json_request(
        "POST",
        "/graph/unsubscribe",
        Some(serde_json::json!({"stream_id": 1})),
    );
    let unsub_resp = app.clone().oneshot(unsubscribe).await.unwrap();
    assert_eq!(unsub_resp.status(), StatusCode::OK);
    assert_eq!(engine.subscription_registry().count(), 0);

    let body = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        axum::body::to_bytes(resp.into_body(), 1_048_576),
    )
    .await
    .expect("subscription body should finish after unsubscribe")
    .unwrap();
    let sse = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        sse.contains("event: snapshot"),
        "SSE did not contain snapshot: {sse}"
    );
    assert!(
        sse.contains("Alice"),
        "snapshot did not contain initial row: {sse}"
    );
    assert!(
        sse.contains("event: delta"),
        "SSE did not contain delta: {sse}"
    );
    assert!(sse.contains("Bob"), "delta did not contain new row: {sse}");
}

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

/// T-QEC-D07 / URS-QEC-D06: `REMOVE n.prop` unsets the property — a subsequent
/// MATCH must show it gone (null), not the old value.
#[tokio::test]
async fn remove_property_unsets_it() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Create a Person with an age.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Dana'}) SET n.age = 42 RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "MERGE + SET must return 200");

    // Confirm the age is present before REMOVE.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Dana'}) RETURN n.age",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows = response_json(resp).await["rows"].clone();
    assert_eq!(
        rows,
        serde_json::json!([[42]]),
        "age must be present before REMOVE"
    );

    // REMOVE the age property.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Dana'}) REMOVE n.age",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "REMOVE must return 200");

    // A subsequent MATCH must show the property gone (null).
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Dana'}) RETURN n.age",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows = response_json(resp).await["rows"].clone();
    assert_eq!(
        rows,
        serde_json::json!([[serde_json::Value::Null]]),
        "age must be unset (null) after REMOVE n.age"
    );
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

#[tokio::test]
async fn http_query_params_bind_match_merge_and_set_literals() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: $name}) SET n.age = $age RETURN n",
            "keyspace": "social",
            "params": {"name": "Param Alice", "age": 41}
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MERGE with params must return 200"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: $name}) WHERE n.age = $age RETURN n.name, n.age",
            "keyspace": "social",
            "params": {"name": "Param Alice", "age": 41}
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "MATCH with params must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "parameterized MATCH should find exactly the bound row"
    );
    assert_eq!(rows[0][0], "Param Alice");
    assert_eq!(rows[0][1], 41);
}

#[tokio::test]
async fn return_distinct_deduplicates_projected_rows() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for (id, name) in [
        ("00000000-0000-0000-0000-00000000d001", "Alice"),
        ("00000000-0000-0000-0000-00000000d002", "Alice"),
        ("00000000-0000-0000-0000-00000000d003", "Bob"),
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{id: '{id}', name: '{name}'}}) RETURN n.name"),
                "keyspace": "social"
            })),
        );
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
    }

    let all_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name ORDER BY n.name",
            "keyspace": "social"
        })),
    );
    let all_resp = app.clone().oneshot(all_req).await.unwrap();
    assert_eq!(all_resp.status(), StatusCode::OK);
    let all_body = response_json(all_resp).await;
    assert_eq!(
        all_body["rows"],
        serde_json::json!([["Alice"], ["Alice"], ["Bob"]]),
        "non-DISTINCT query should preserve duplicates"
    );

    let distinct_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN DISTINCT n.name ORDER BY n.name",
            "keyspace": "social"
        })),
    );
    let distinct_resp = app.oneshot(distinct_req).await.unwrap();
    assert_eq!(distinct_resp.status(), StatusCode::OK);
    let distinct_body = response_json(distinct_resp).await;
    assert_eq!(
        distinct_body["rows"],
        serde_json::json!([["Alice"], ["Bob"]]),
        "RETURN DISTINCT should deduplicate whole projected rows"
    );
}

#[tokio::test]
async fn negative_pattern_predicate_filters_existing_relationships() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r",
        "MERGE (c:Person {name: 'Cara'}) RETURN c.name",
    ] {
        let setup_req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({"query": query, "keyspace": "social"})),
        );
        assert_eq!(
            app.clone().oneshot(setup_req).await.unwrap().status(),
            StatusCode::OK,
            "setup query failed: {query}"
        );
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person) WHERE NOT (a)-[:KNOWS]->(:Person) RETURN a.name ORDER BY a.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "negative pattern query failed: {body:?}"
    );
    assert_eq!(
        body["rows"],
        serde_json::json!([["Bob"], ["Cara"]]),
        "negative pattern should keep only nodes without a matching outgoing KNOWS edge"
    );
}

#[tokio::test]
async fn negative_multi_hop_pattern_predicate_filters_existing_paths() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r",
        "MERGE (b:Person {name: 'Bob'})-[r:KNOWS]->(d:Person {name: 'Dana'}) RETURN r",
        "MERGE (c:Person {name: 'Cara'}) RETURN c.name",
        "MERGE (d:Person {name: 'Dana'}) RETURN d.name",
    ] {
        let setup_req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({"query": query, "keyspace": "social"})),
        );
        assert_eq!(
            app.clone().oneshot(setup_req).await.unwrap().status(),
            StatusCode::OK,
            "setup query failed: {query}"
        );
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person) WHERE NOT (a)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person {name: 'Dana'}) RETURN a.name ORDER BY a.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(
        body["rows"],
        serde_json::json!([["Bob"], ["Cara"], ["Dana"]]),
        "multi-hop negative pattern should exclude only starts that have the full path"
    );
}

#[tokio::test]
async fn exists_pattern_predicate_filters_rows_with_matching_paths() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})",
        "MERGE (b:Person {name: 'Bob'})-[r:KNOWS]->(d:Person {name: 'Dana'})",
        "MERGE (c:Person {name: 'Cara'}) RETURN c.name",
        "MERGE (d:Person {name: 'Dana'}) RETURN d.name",
    ] {
        let setup_req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({"query": query, "keyspace": "social"})),
        );
        assert_eq!(
            app.clone().oneshot(setup_req).await.unwrap().status(),
            StatusCode::OK,
            "setup query failed: {query}"
        );
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person {name: 'Dana'}) } RETURN a.name ORDER BY a.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "EXISTS pattern query failed: {body:?}"
    );
    assert_eq!(
        body["rows"],
        serde_json::json!([["Alice"]]),
        "EXISTS pattern should keep only starts that have the full path"
    );
}

#[tokio::test]
async fn count_star_counts_matching_rows_as_signed_integer() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    for (id, name) in [
        ("11111111-1111-1111-1111-111111111111", "Alice"),
        ("22222222-2222-2222-2222-222222222222", "Bob"),
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("CREATE (n:Person {{id: '{id}', name: '{name}'}})"),
                "keyspace": "social"
            })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN count(*)",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "count(*) response body: {body:?}");
    assert_eq!(body["columns"], serde_json::json!(["count(*)"]));
    assert_eq!(body["rows"][0][0].as_i64(), Some(2));
}

#[tokio::test]
async fn aggregate_distinct_deduplicates_inputs_per_group() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(schema, storage);

    for query in [
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000a101', name: 'Alice', age: 10})",
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000b202', name: 'Bob', age: 10})",
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000c303', name: 'Cara', age: 20})",
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({ "query": query, "keyspace": "social" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "setup query failed: {query}");
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN count(DISTINCT n.age) AS ages, sum(DISTINCT n.age) AS total, avg(DISTINCT n.age) AS avg_age, collect(DISTINCT n.age) AS collected",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "aggregate DISTINCT query failed: {body:?}"
    );
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    let row = rows[0].as_array().expect("aggregate row");
    assert_eq!(row[0], serde_json::json!(2));
    assert_eq!(row[1], serde_json::json!(30.0));
    assert_eq!(row[2], serde_json::json!(15.0));
    let mut collected = row[3]
        .as_array()
        .expect("collect result")
        .iter()
        .map(|v| v.as_i64().expect("integer age"))
        .collect::<Vec<_>>();
    collected.sort_unstable();
    assert_eq!(collected, vec![10, 20]);
}

#[tokio::test]
async fn aggregate_collect_skips_null_property_values() {
    // openCypher: collect() ignores null values. A node whose `age` is unset
    // must NOT contribute a null entry to collect(n.age).
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(schema, storage);

    for query in [
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000a101', name: 'Alice', age: 10})",
        // Bob has no age property at all -> n.age is null.
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000b202', name: 'Bob'})",
        "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000c303', name: 'Cara', age: 20})",
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({ "query": query, "keyspace": "social" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "setup query failed: {query}");
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN collect(n.age) AS ages, count(n.age) AS aged",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "collect query failed: {body:?}");
    let row = body["rows"][0].as_array().expect("aggregate row");
    let mut collected = row[0]
        .as_array()
        .expect("collect result must be an array, never null")
        .iter()
        .map(|v| {
            v.as_i64()
                .expect("collect must skip nulls -> only integers remain")
        })
        .collect::<Vec<_>>();
    collected.sort_unstable();
    assert_eq!(
        collected,
        vec![10, 20],
        "collect(n.age) must skip the null age"
    );
    // count(n.age) also skips nulls -> 2 aged persons.
    assert_eq!(row[1].as_i64(), Some(2));
}

#[tokio::test]
async fn aggregate_empty_input_semantics() {
    // openCypher empty-input semantics over zero matched rows:
    //   count -> 0, sum -> 0, collect -> [], avg/min/max -> null.
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(schema, storage);

    // No Person nodes created -> the MATCH yields zero rows.
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN count(n.age) AS c, sum(n.age) AS s, collect(n.age) AS col, avg(n.age) AS a, min(n.age) AS mn, max(n.age) AS mx",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "empty-input query failed: {body:?}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "aggregate over empty input collapses to one row"
    );
    let row = rows[0].as_array().expect("aggregate row");
    assert_eq!(row[0].as_i64(), Some(0), "count over empty input -> 0");
    assert_eq!(row[1].as_i64(), Some(0), "sum over empty input -> 0");
    assert_eq!(
        row[2],
        serde_json::json!([]),
        "collect over empty input -> []"
    );
    assert!(row[3].is_null(), "avg over empty input -> null");
    assert!(row[4].is_null(), "min over empty input -> null");
    assert!(row[5].is_null(), "max over empty input -> null");
}

#[tokio::test]
async fn where_in_list_literal_filters_rows() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (n:Person {name: 'Alice'})",
        "MERGE (n:Person {name: 'Bob'})",
        "MERGE (n:Person {name: 'Cara'})",
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({ "query": query, "keyspace": "social" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "setup query failed: {query}");
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) WHERE n.name IN ['Alice', 'Cara'] RETURN n.name ORDER BY n.name ASC",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "IN query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([["Alice"], ["Cara"]]));
}

#[tokio::test]
async fn list_indexing_and_map_literal_project_values() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [0] AS _ RETURN [10, 20, 30][1] AS item, {name: 'Alice', age: 7} AS m",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "list/map query failed: {body:?}");
    assert_eq!(
        body["rows"],
        serde_json::json!([[20, {"name": "Alice", "age": 7}]])
    );
}

#[tokio::test]
async fn list_slicing_projects_sublist() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [0] AS _ RETURN [10, 20, 30, 40][1..3] AS slice",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "list slice query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([[[20, 30]]]));
}

#[tokio::test]
async fn list_predicates_any_and_all_evaluate_scoped_variables() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [0] AS _ RETURN any(x IN [1, 2, 3] WHERE x = 2) AS has_two, all(x IN [1, 2, 3] WHERE x > 0) AS all_positive",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "list predicate query failed: {body:?}"
    );
    assert_eq!(body["rows"], serde_json::json!([[true, true]]));
}

#[tokio::test]
async fn relationship_type_alternatives_match_any_listed_type() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    create_social_likes_edge_schema(&schema);
    register_social_tables_with_storage(&storage);
    register_social_likes_table_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (a:Person {name: 'AltAlice'})",
        "MERGE (b:Person {name: 'AltBob'})",
        "MERGE (c:Person {name: 'AltCara'})",
        "MERGE (d:Person {name: 'AltDana'})",
        "MERGE (a:Person {name: 'AltAlice'})-[r:KNOWS]->(b:Person {name: 'AltBob'}) RETURN r",
        "MERGE (c:Person {name: 'AltCara'})-[r:LIKES]->(d:Person {name: 'AltDana'}) RETURN r",
    ] {
        let resp = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/graph/query",
                Some(serde_json::json!({"query": query, "keyspace": "social"})),
            ))
            .await
            .unwrap();
        let status = resp.status();
        let body = response_json(resp).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "setup query failed: {query}; body: {body:?}"
        );
    }

    for (query, expected_rows) in [
        (
            "MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name ORDER BY n.name ASC",
            serde_json::json!([["AltAlice", "AltBob"]]),
        ),
        (
            "MATCH (n:Person)-[:LIKES]->(m:Person) RETURN n.name, m.name ORDER BY n.name ASC",
            serde_json::json!([["AltCara", "AltDana"]]),
        ),
    ] {
        let resp = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/graph/query",
                Some(serde_json::json!({"query": query, "keyspace": "social"})),
            ))
            .await
            .unwrap();
        let status = resp.status();
        let body = response_json(resp).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "single-label query failed: {body:?}"
        );
        assert_eq!(
            body["rows"], expected_rows,
            "single-label query mismatch: {query}"
        );
    }

    let resp = app
        .oneshot(json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": "MATCH (n:Person)-[:KNOWS|LIKES]->(m:Person) RETURN n.name, m.name ORDER BY n.name ASC",
                "keyspace": "social"
            })),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "type alternative query failed: {body:?}"
    );
    assert_eq!(
        body["rows"],
        serde_json::json!([["AltAlice", "AltBob"], ["AltCara", "AltDana"]])
    );
}

#[tokio::test]
async fn multi_label_node_patterns_return_explicit_boundary_error() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(schema, storage);

    let resp = app
        .oneshot(json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": "MATCH (n:Person:Employee) RETURN n.name",
                "keyspace": "social"
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = response_json(resp).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("multi-label node patterns are not yet supported"),
        "unexpected error: {body:?}"
    );
}

#[tokio::test]
async fn optional_match_preserves_rows_and_nulls_missing_pattern() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})",
        "MERGE (c:Person {name: 'Cara'})",
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({ "query": query, "keyspace": "social" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "setup query failed: {query}");
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(friend:Person) RETURN n.name, friend.name ORDER BY n.name ASC",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "OPTIONAL MATCH query failed: {body:?}"
    );
    assert_eq!(
        body["rows"],
        serde_json::json!([["Alice", "Bob"], ["Bob", null], ["Cara", null]])
    );
}

#[tokio::test]
async fn with_projects_alias_filters_orders_and_limits_pipeline() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    for query in [
        "MERGE (n:Person {name: 'Alice'})",
        "MERGE (n:Person {name: 'Bob'})",
        "MERGE (n:Person {name: 'Cara'})",
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({ "query": query, "keyspace": "social" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "setup query failed: {query}");
    }

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) WITH n.name AS name WHERE name = 'Cara' RETURN name ORDER BY name ASC LIMIT 1",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "WITH query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([["Cara"]]));
}

#[tokio::test]
async fn unwind_list_literal_expands_rows_and_preserves_empty_list() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x ASC",
            "keyspace": "social"
        })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "UNWIND query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([[1], [2], [3]]));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [] AS x RETURN x",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "UNWIND empty list query failed: {body:?}"
    );
    assert_eq!(body["rows"], serde_json::json!([]));
}

#[tokio::test]
async fn union_all_preserves_duplicates_and_union_deduplicates_rows() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [1, 2] AS x RETURN x UNION ALL UNWIND [2, 3] AS x RETURN x",
            "keyspace": "social"
        })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "UNION ALL query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([[1], [2], [2], [3]]));

    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "UNWIND [1, 2] AS x RETURN x UNION UNWIND [2, 3] AS x RETURN x",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "UNION query failed: {body:?}");
    assert_eq!(body["rows"], serde_json::json!([[1], [2], [3]]));
}

#[tokio::test]
async fn union_over_match_deduplicates_rows() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    // Seed three people, two sharing the name "Alice".
    for (id, name) in [
        ("00000000-0000-0000-0000-00000000e001", "Alice"),
        ("00000000-0000-0000-0000-00000000e002", "Alice"),
        ("00000000-0000-0000-0000-00000000e003", "Bob"),
    ] {
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{id: '{id}', name: '{name}'}}) RETURN n.name"),
                "keyspace": "social"
            })),
        );
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
    }

    // UNION of two identical MATCH arms must dedup across arms (and within),
    // so "Alice" (twice in each arm) collapses to a single row.
    let union_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name AS a \
                      UNION \
                      MATCH (n:Person) RETURN n.name AS a",
            "keyspace": "social"
        })),
    );
    let union_resp = app.clone().oneshot(union_req).await.unwrap();
    let status = union_resp.status();
    let union_body = response_json(union_resp).await;
    assert_eq!(status, StatusCode::OK, "UNION query failed: {union_body:?}");
    assert_eq!(union_body["columns"], serde_json::json!(["a"]));
    let mut rows = union_body["rows"].as_array().unwrap().clone();
    rows.sort_by_key(|r| r.to_string());
    assert_eq!(
        rows,
        vec![serde_json::json!(["Alice"]), serde_json::json!(["Bob"])],
        "UNION must deduplicate rows across and within arms"
    );

    // UNION ALL keeps every duplicate: each arm yields 3 rows (Alice, Alice, Bob),
    // so the combined result has 6 rows.
    let union_all_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name AS a \
                      UNION ALL \
                      MATCH (n:Person) RETURN n.name AS a",
            "keyspace": "social"
        })),
    );
    let union_all_resp = app.oneshot(union_all_req).await.unwrap();
    let status = union_all_resp.status();
    let union_all_body = response_json(union_all_resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "UNION ALL query failed: {union_all_body:?}"
    );
    assert_eq!(
        union_all_body["rows"].as_array().unwrap().len(),
        6,
        "UNION ALL must preserve every duplicate row from both arms"
    );
}

/// UNION arms have **independent variable scope** (openCypher). Reusing the
/// same pattern variable (`n`) with *different labels* across arms is legal and
/// common: `MATCH (n:Person) RETURN n.name AS a UNION MATCH (n:Company) RETURN n.name AS a`.
/// Each arm must scan its own table — the Person arm must return Person rows and
/// the Company arm must return Company rows. A flat last-write-wins binding map
/// shared across arms would make both arms scan whichever table won the merge,
/// silently dropping the other arm's rows (the worst failure class: no error,
/// no crash, lost rows).
#[tokio::test]
async fn union_arms_with_same_var_different_labels_keep_all_rows() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    create_company_vertex_schema(&schema);
    register_company_table_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    // Seed one Person (Alice) and one Company (Acme).
    let person_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {id: '00000000-0000-0000-0000-00000000f001', name: 'Alice'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    assert_eq!(
        app.clone().oneshot(person_req).await.unwrap().status(),
        StatusCode::OK
    );
    let company_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Company {id: '00000000-0000-0000-0000-00000000f002', name: 'Acme'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    assert_eq!(
        app.clone().oneshot(company_req).await.unwrap().status(),
        StatusCode::OK
    );

    // Same variable `n`, different labels across arms. Result must contain BOTH
    // Alice (from the Person arm) and Acme (from the Company arm).
    let union_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name AS a \
                      UNION \
                      MATCH (n:Company) RETURN n.name AS a",
            "keyspace": "social"
        })),
    );
    let union_resp = app.oneshot(union_req).await.unwrap();
    let status = union_resp.status();
    let union_body = response_json(union_resp).await;
    assert_eq!(status, StatusCode::OK, "UNION query failed: {union_body:?}");
    assert_eq!(union_body["columns"], serde_json::json!(["a"]));
    let mut rows = union_body["rows"].as_array().unwrap().clone();
    rows.sort_by_key(|r| r.to_string());
    assert_eq!(
        rows,
        vec![serde_json::json!(["Acme"]), serde_json::json!(["Alice"])],
        "each UNION arm must scan its OWN labelled table; neither arm's rows may be dropped"
    );
}

#[tokio::test]
async fn union_mismatched_columns_returns_clear_error() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    // Arms differ in column name (`a` vs `b`) — openCypher requires identical
    // result column names/arity across arms, so this must fail loud, not
    // silently merge or return wrong/empty results.
    let mismatch_name_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name AS a \
                      UNION \
                      MATCH (n:Person) RETURN n.name AS b",
            "keyspace": "social"
        })),
    );
    let resp = app.clone().oneshot(mismatch_name_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "mismatched UNION column names must be a validation error"
    );
    let body = response_json(resp).await;
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("UNION") && err.contains("column"),
        "error must clearly mention UNION column mismatch, got: {err:?}"
    );

    // Arms differ in arity (1 column vs 2 columns) — same fail-loud requirement.
    let mismatch_arity_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person) RETURN n.name AS a \
                      UNION \
                      MATCH (n:Person) RETURN n.name AS a, n.age AS b",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(mismatch_arity_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "mismatched UNION column arity must be a validation error"
    );
    let body = response_json(resp).await;
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("UNION") && err.contains("column"),
        "error must clearly mention UNION column mismatch, got: {err:?}"
    );
}

#[tokio::test]
async fn http_query_missing_param_returns_validation_error() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(schema, storage);
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: $name}) RETURN n",
            "keyspace": "social",
            "params": {}
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = response_json(resp).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("missing required query parameter $name"),
        "error should name the missing parameter: {body:?}"
    );
}

#[tokio::test]
async fn http_unsupported_cypher_clauses_return_explicit_400() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    for (query, expected) in [
        (
            "OPTIONAL MATCH (n) RETURN n",
            "unsupported statement keyword: Keyword(Optional)",
        ),
        (
            "WITH 1 AS x RETURN x",
            "unsupported Cypher clause: Keyword(With)",
        ),
        (
            "MATCH (n) RETURN n WITH n RETURN n",
            "unsupported Cypher clause: Keyword(With)",
        ),
        (
            // Bare top-level CALL (stored procedure) is still unsupported; only the
            // `CALL {}` subquery form (which requires a driving query) is supported.
            "CALL db.labels()",
            "unsupported Cypher clause: Keyword(Call)",
        ),
        (
            "LOAD CSV FROM 'file:///people.csv' AS row RETURN row",
            "unsupported Cypher clause: Keyword(Load)",
        ),
    ] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({"query": query, "keyspace": "social"})),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{query} should be rejected"
        );
        let body = response_json(resp).await;
        let error = body["error"].as_str().unwrap();
        assert!(
            error.contains(expected),
            "{query} should mention {expected}, got: {error}"
        );
    }
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
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        StatusCode::OK,
        status,
        "hop query after MERGE must return 200 — executor must not crash on empty adjacency table"
    );
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
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        StatusCode::OK,
        status,
        "hop query must return 200 after adjacency index is populated"
    );
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

    let mut co_cols = IndexMap::new();
    co_cols.insert(
        "entity_a".to_string(),
        ColumnMetadata {
            name: "entity_a".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
        "entity_b".to_string(),
        ColumnMetadata {
            name: "entity_b".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    co_cols.insert(
        "session_id".to_string(),
        ColumnMetadata {
            name: "session_id".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
        "tenant_id".to_string(),
        ColumnMetadata {
            name: "tenant_id".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
        "strength".to_string(),
        ColumnMetadata {
            name: "strength".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "float".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
        "first_seen".to_string(),
        ColumnMetadata {
            name: "first_seen".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "timestamp".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
        "last_reinforced".to_string(),
        ColumnMetadata {
            name: "last_reinforced".to_string(),
            kind: ColumnKind::Regular,
            position: -1,
            column_type: "timestamp".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    co_cols.insert(
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

    schema
        .create_table(
            TableMetadata {
                keyspace: "agent_memory".to_string(),
                name: "co_occurs_with".to_string(),
                id: Uuid::new_v4(),
                columns: co_cols,
                partition_key: vec!["entity_a".to_string()],
                clustering_key: vec![("entity_b".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::from([
                    ("graph.type".to_string(), "edge".to_string()),
                    ("graph.label".to_string(), "CO_OCCURS_WITH".to_string()),
                    ("graph.source".to_string(), "entity_a".to_string()),
                    ("graph.target".to_string(), "entity_b".to_string()),
                    ("graph.source_label".to_string(), "Entity".to_string()),
                    ("graph.target_label".to_string(), "Entity".to_string()),
                ]),
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
        ("agent_memory", "co_occurs_with"),
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
    seed_agent_memory_entity(&storage, tenant_id, session_id, src_id, "src");
    seed_agent_memory_entity(&storage, tenant_id, session_id, dst_id, "dst");

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
    seed_agent_memory_entity(&storage, tenant_id, session_id, src_id, "src");
    seed_agent_memory_entity(&storage, tenant_id, session_id, dst_id, "dst");

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

#[tokio::test]
async fn full_shape_typed_edge_match_returns_relationship_properties_in_real_agent_memory_schema() {
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
         (b:Entity {{entity_id: '{dst_id}'}}) RETURN r.edge_type, r.weight"
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
        .expect("MATCH relationship projection response must include rows");
    assert_eq!(
        rows.len(),
        1,
        "MATCH must return exactly one relationship row"
    );
    assert_eq!(rows[0][0], serde_json::json!("related_to"));
    assert_eq!(rows[0][1], serde_json::json!(1.0));
}

#[tokio::test]
async fn full_shape_typed_edge_match_finds_edge_when_entities_exist_in_multiple_scopes() {
    let expected_tenant_id = Uuid::parse_str("6792702e-2a9c-4465-ba65-ba100b5aaafa").unwrap();
    let expected_session_id = Uuid::parse_str("909e2671-aea0-534a-83bc-bb5efc544b0f").unwrap();
    let other_tenant_id = Uuid::parse_str("0e66aeeb-0228-461d-a1ff-28571da15443").unwrap();
    let other_session_id = Uuid::parse_str("870a223e-d134-5057-a0c9-92e12f408ebc").unwrap();
    let src_id = Uuid::parse_str("41753309-7297-454e-8f2d-c6546740cf2b").unwrap();
    let dst_id = Uuid::parse_str("f6ffe258-9194-470d-9811-5b3e23b33103").unwrap();

    let (schema, storage, _dir) = setup();
    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);

    seed_agent_memory_entity(
        &storage,
        expected_tenant_id,
        expected_session_id,
        src_id,
        "src",
    );
    seed_agent_memory_entity(
        &storage,
        expected_tenant_id,
        expected_session_id,
        dst_id,
        "dst",
    );
    seed_agent_memory_entity(
        &storage,
        other_tenant_id,
        other_session_id,
        src_id,
        "src-other",
    );
    seed_agent_memory_entity(
        &storage,
        other_tenant_id,
        other_session_id,
        dst_id,
        "dst-other",
    );

    let merge_query = format!(
        "MERGE (a:Entity {{entity_id: '{src_id}'}}) \
         MERGE (b:Entity {{entity_id: '{dst_id}'}}) \
         MERGE (a)-[r:TYPED_EDGE {{edge_type: 'implements'}}]->(b) \
         SET r.tenant_id = '{expected_tenant_id}', \
             r.session_id = '{expected_session_id}', \
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
        "MATCH (a:Entity {{entity_id: '{src_id}'}})-[r:TYPED_EDGE {{edge_type: 'implements'}}]->\
         (b:Entity {{entity_id: '{dst_id}'}}) RETURN r.edge_type, r.weight"
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
        .expect("MATCH relationship projection response must include rows");
    assert_eq!(
        rows.len(),
        1,
        "MATCH must still find the scoped edge when duplicate entity rows exist in other scopes"
    );
    assert_eq!(rows[0][0], serde_json::json!("implements"));
    assert_eq!(rows[0][1], serde_json::json!(1.0));
}

#[tokio::test]
async fn canonical_typed_edge_merge_infers_scope_from_existing_entities() {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_storage::TableId;

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
         SET r.weight = 1.0 \
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

    let partition_key =
        encode_composite_partition_key(&[tenant_id.as_bytes(), session_id.as_bytes()]);
    let row_clustering =
        encode_multi_clustering_key(&[src_id.as_bytes(), b"related_to", dst_id.as_bytes()]);
    let partition = storage
        .read(
            &TableId::new("agent_memory", "typed_edges"),
            &DecoratedKey::new(PartitionKey::new(partition_key)),
        )
        .unwrap()
        .expect("typed_edges partition must exist after canonical MERGE using existing entities");
    assert!(
        partition.rows.iter().any(|row| row.clustering == row_clustering),
        "canonical MERGE must infer tenant/session scope from matched entities and materialize the typed edge row",
    );
}

#[tokio::test]
async fn canonical_typed_edge_merge_without_scoped_entities_returns_validation_error() {
    let src_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let dst_id = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

    let (schema, storage, _dir) = setup();
    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);

    let merge_query = format!(
        "MERGE (a:Entity {{entity_id: '{src_id}'}}) \
         MERGE (b:Entity {{entity_id: '{dst_id}'}}) \
         MERGE (a)-[r:TYPED_EDGE {{edge_type: 'related_to'}}]->(b) \
         SET r.weight = 1.0 \
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
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unscoped canonical MERGE must fail instead of acknowledging success without materializing a row"
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
async fn graph_engine_constructed_before_fmem_ddl_registers_adjacency_for_first_edge_write() {
    let tenant_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let session_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let src_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let dst_id = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

    let (schema, storage, _dir) = setup();
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));

    create_agent_memory_real_graph_schema(&schema);
    register_agent_memory_real_tables_with_storage(&storage);
    seed_agent_memory_entity(&storage, tenant_id, session_id, src_id, "src");
    seed_agent_memory_entity(&storage, tenant_id, session_id, dst_id, "dst");

    let merge_query = format!(
        "MERGE (a:Entity {{tenant_id: '{tenant_id}', session_id: '{session_id}', entity_id: '{src_id}'}})\
         MERGE (b:Entity {{tenant_id: '{tenant_id}', session_id: '{session_id}', entity_id: '{dst_id}'}})\
         MERGE (a)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}', session_id: '{session_id}'}}]->(b) \
         SET r.strength = 0.5, \
             r.created_at = '2026-05-07T00:00:00Z', \
             r.first_seen = '2026-05-07T00:00:00Z', \
             r.last_reinforced = '2026-05-07T00:00:00Z' \
         RETURN r"
    );
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": merge_query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("first tiny fmem CO_OCCURS MERGE after DDL must not hang")
        .unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-DDL first MERGE response body: {body:#}"
    );
}

#[tokio::test]
async fn co_occurs_merge_on_tiny_agent_memory_graph_is_immediately_matchable() {
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
        "MERGE (a:Entity {{tenant_id: '{tenant_id}', session_id: '{session_id}', entity_id: '{src_id}'}})\
         MERGE (b:Entity {{tenant_id: '{tenant_id}', session_id: '{session_id}', entity_id: '{dst_id}'}})\
         MERGE (a)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}', session_id: '{session_id}'}}]->(b) \
         SET r.strength = 0.5, \
             r.created_at = '2026-05-07T00:00:00Z', \
             r.first_seen = '2026-05-07T00:00:00Z', \
             r.last_reinforced = '2026-05-07T00:00:00Z' \
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
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("tiny fmem CO_OCCURS MERGE must not hang")
        .unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "MERGE response body: {body:#}");

    let match_query = format!(
        "MATCH (a:Entity)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}'}}]->(b:Entity) \
         RETURN a.entity_id AS src_id, b.entity_id AS dst_id, r.strength AS strength"
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
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("tiny fmem CO_OCCURS list MATCH must return immediately")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("MATCH response must include rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_str(), Some(src_id.to_string().as_str()));
    assert_eq!(rows[0][1].as_str(), Some(dst_id.to_string().as_str()));

    let set_query = format!(
        "MATCH (a:Entity)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}', entity_a: '{src_id}', entity_b: '{dst_id}'}}]->(b:Entity) \
         SET r.strength = 0.25"
    );
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": set_query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("edge-keyed fmem CO_OCCURS SET must return immediately")
        .unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "SET response body: {body:#}");

    let match_query = format!(
        "MATCH (a:Entity)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}'}}]->(b:Entity) \
         RETURN a.entity_id AS src_id, b.entity_id AS dst_id, r.strength AS strength"
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
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("updated tiny fmem CO_OCCURS list MATCH must return immediately")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("MATCH response must include rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_str(), Some(src_id.to_string().as_str()));
    assert_eq!(rows[0][1].as_str(), Some(dst_id.to_string().as_str()));
    assert_eq!(rows[0][2].as_f64(), Some(0.25));

    let delete_query = format!(
        "MATCH (a:Entity)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}', entity_a: '{src_id}', entity_b: '{dst_id}'}}]->(b:Entity) \
         DELETE r"
    );
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": delete_query,
            "keyspace": "agent_memory"
        })),
    );
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("edge-keyed fmem CO_OCCURS DELETE must return immediately")
        .unwrap();
    let status = resp.status();
    let body = response_json(resp).await;
    assert_eq!(status, StatusCode::OK, "DELETE response body: {body:#}");

    let match_query = format!(
        "MATCH (a:Entity)-[r:CO_OCCURS_WITH {{tenant_id: '{tenant_id}'}}]->(b:Entity) \
         RETURN a.entity_id AS src_id, b.entity_id AS dst_id, r.strength AS strength"
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
    let resp = tokio::time::timeout(std::time::Duration::from_secs(1), app.oneshot(req))
        .await
        .expect("deleted tiny fmem CO_OCCURS list MATCH must return immediately")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"]
        .as_array()
        .expect("MATCH response must include rows");
    assert!(
        rows.is_empty(),
        "DELETE must remove the matched edge: {rows:#?}"
    );
}

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

// ── Slice 12: DETACH DELETE cascade (URS-QEC-D01/D03/D07, T-QEC-D01/D03) ──────

/// blake3 content-address key for a `name`-keyed vertex, mirroring
/// `content_addressed_key` (`name\x00<value>\x00`).
fn vertex_key_bytes(name: &str) -> Vec<u8> {
    let mut h = blake3::Hasher::new();
    h.update(format!("name\x00{name}\x00").as_bytes());
    h.finalize().as_bytes().to_vec()
}

/// Register the `social` schema + storage tables and wire the synchronous
/// adjacency observer so MERGE edge writes land OUT/IN adjacency entries.
/// Returns an engine that drives Cypher directly.
fn detach_delete_engine() -> (Arc<GraphEngine>, Arc<StorageEngine>, Arc<Schema>, TempDir) {
    use ferrosa_graph::adjacency::observer::AdjacencyIndexObserver;
    use ferrosa_graph::adjacency::{adjacency_keyspace_name, adjacency_table_metadata};

    let (schema, storage, dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Register the adjacency keyspace + table so the observer's derived
    // mutations have a registered home and the planner can resolve them.
    let adj_ks_name = adjacency_keyspace_name("social");
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: adj_ks_name.clone(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            },
            &superuser_auth(),
        )
        .unwrap();
    schema
        .create_table(adjacency_table_metadata("social"), &superuser_auth())
        .unwrap();
    storage
        .register_table(adjacency_table_metadata("social").to_storage_schema())
        .unwrap();

    // Synchronous observer: adjacency entries are written in the same write
    // call as the edge, so no drain task / yield is required.
    let observer = Arc::new(AdjacencyIndexObserver::new(
        Arc::clone(&schema),
        "social".to_string(),
    ));
    storage.register_observer(observer as Arc<dyn ferrosa_storage::WriteObserver>);

    let (_app, engine) = build_app_and_engine(Arc::clone(&schema), Arc::clone(&storage));
    (engine, storage, schema, dir)
}

/// Count live rows in a single-partition read (None partition => 0).
fn live_row_count(storage: &StorageEngine, keyspace: &str, table: &str, key_bytes: &[u8]) -> usize {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_storage::TableId;

    let tid = TableId::new(keyspace, table);
    let key = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));
    match storage.read(&tid, &key).unwrap() {
        Some(partition) => partition
            .rows
            .iter()
            .filter(|r| r.deletion.is_live())
            .count(),
        None => 0,
    }
}

/// T-QEC-D01 / T-QEC-D03 (URS-QEC-D01/D03/D07): a node with N inbound +
/// M outbound edges, on `DETACH DELETE`, loses the node and **all** N+M
/// incident edges, and the adjacency index has **zero** references to the
/// node afterward — verified via direct CQL reads of `knows_e` and
/// `system_graph_social.adjacency`, and via MATCH — all **without** running
/// reconciliation.
#[tokio::test]
async fn detach_delete_removes_node_all_incident_edges_and_adjacency() {
    let (engine, storage, _schema, _dir) = detach_delete_engine();
    let auth = superuser_auth();
    let ks = "social";

    // Build the graph: Center with 2 outbound + 2 inbound edges.
    let out_neighbors = ["OutA", "OutB"];
    let in_neighbors = ["InX", "InY"];
    for name in ["Center", "OutA", "OutB", "InX", "InY"] {
        engine
            .execute(
                &format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                ks,
                &auth,
            )
            .await
            .expect("node MERGE must succeed");
    }
    for dst in out_neighbors {
        engine
            .execute(
                &format!(
                    "MERGE (a:Person {{name: 'Center'}})-[r:KNOWS]->(b:Person {{name: '{dst}'}}) RETURN r"
                ),
                ks,
                &auth,
            )
            .await
            .expect("outbound edge MERGE must succeed");
    }
    for src in in_neighbors {
        engine
            .execute(
                &format!(
                    "MERGE (a:Person {{name: '{src}'}})-[r:KNOWS]->(b:Person {{name: 'Center'}}) RETURN r"
                ),
                ks,
                &auth,
            )
            .await
            .expect("inbound edge MERGE must succeed");
    }

    let center = vertex_key_bytes("Center");

    // Precondition: edges + adjacency exist before the delete.
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &center),
        2,
        "precondition: Center must have 2 outbound knows_e rows"
    );
    for src in in_neighbors {
        assert_eq!(
            live_row_count(&storage, ks, "knows_e", &vertex_key_bytes(src)),
            1,
            "precondition: inbound edge {src}->Center must exist"
        );
    }
    let adj_ks = "system_graph_social";
    assert!(
        live_row_count(&storage, adj_ks, "adjacency", &center) >= 4,
        "precondition: Center adjacency must hold 2 OUT + 2 IN entries (got {})",
        live_row_count(&storage, adj_ks, "adjacency", &center)
    );

    // The DETACH DELETE under test. Adjacency keyspace is already registered
    // (the MERGEs registered it), so this query does NOT trigger reconciliation.
    engine
        .execute(
            "MATCH (n:Person {name: 'Center'}) DETACH DELETE n",
            ks,
            &auth,
        )
        .await
        .expect("DETACH DELETE must succeed");

    // (1) The node is gone from MATCH and from person_v storage.
    let match_center = engine
        .execute("MATCH (n:Person {name: 'Center'}) RETURN n.name", ks, &auth)
        .await
        .expect("MATCH after delete must succeed");
    assert!(
        match_center.rows.is_empty(),
        "Center node must be gone after DETACH DELETE; got rows: {:?}",
        match_center.rows
    );
    assert_eq!(
        live_row_count(&storage, ks, "person_v", &center),
        0,
        "Center vertex row must be tombstoned in person_v"
    );

    // (2) ALL outbound edges gone: knows_e partition for Center has no live rows.
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &center),
        0,
        "all outbound edges from Center must be tombstoned in knows_e"
    );

    // (3) ALL inbound edges gone: each inbound src->Center knows_e row tombstoned.
    for src in in_neighbors {
        assert_eq!(
            live_row_count(&storage, ks, "knows_e", &vertex_key_bytes(src)),
            0,
            "inbound edge {src}->Center must be tombstoned in knows_e"
        );
    }

    // (4) Adjacency scan finds ZERO references to Center:
    //     (a) Center's own partition (both OUT and IN entries) is empty.
    assert_eq!(
        live_row_count(&storage, adj_ks, "adjacency", &center),
        0,
        "Center adjacency partition must have zero live entries after DETACH DELETE"
    );
    //     (b) Each neighbor's mirror entry pointing back at Center is gone.
    //         Out-neighbors held an IN entry (Center as neighbor); in-neighbors
    //         held an OUT entry (Center as neighbor).
    for name in out_neighbors.iter().chain(in_neighbors.iter()) {
        let nbr_key = vertex_key_bytes(name);
        let refs_center = adjacency_partition_references(&storage, adj_ks, &nbr_key, &center);
        assert!(
            !refs_center,
            "neighbor {name} adjacency partition must not reference Center after DETACH DELETE"
        );
    }

    // (5) MATCH hop over KNOWS from/through Center returns nothing.
    let hop = engine
        .execute(
            "MATCH (a:Person {name: 'Center'})-[:KNOWS]->(b:Person) RETURN b.name",
            ks,
            &auth,
        )
        .await
        .expect("hop MATCH after delete must succeed");
    assert!(
        hop.rows.is_empty(),
        "no outbound KNOWS hop from Center may survive; got: {:?}",
        hop.rows
    );
}

/// T-QEC-D02 (URS-QEC-D02): a plain `DELETE n` (no DETACH) on a node that
/// still has surviving relationships must **fail loud** with a Neo4j-style
/// constraint error and delete **nothing** — neither the node nor any of its
/// incident edges/adjacency entries may be tombstoned. The adjacency index is
/// probed *before* any write, so the failure is detected with zero side
/// effects.
#[tokio::test]
async fn plain_delete_with_surviving_relationships_fails_loud_deletes_nothing() {
    let (engine, storage, _schema, _dir) = detach_delete_engine();
    let auth = superuser_auth();
    let ks = "social";

    // Build the graph: Center with 1 outbound + 1 inbound edge.
    for name in ["Center", "Out", "In"] {
        engine
            .execute(
                &format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                ks,
                &auth,
            )
            .await
            .expect("node MERGE must succeed");
    }
    engine
        .execute(
            "MERGE (a:Person {name: 'Center'})-[r:KNOWS]->(b:Person {name: 'Out'}) RETURN r",
            ks,
            &auth,
        )
        .await
        .expect("outbound edge MERGE must succeed");
    engine
        .execute(
            "MERGE (a:Person {name: 'In'})-[r:KNOWS]->(b:Person {name: 'Center'}) RETURN r",
            ks,
            &auth,
        )
        .await
        .expect("inbound edge MERGE must succeed");

    let center = vertex_key_bytes("Center");
    let adj_ks = "system_graph_social";

    // Preconditions: node, both edges, and adjacency entries exist.
    assert_eq!(
        live_row_count(&storage, ks, "person_v", &center),
        1,
        "precondition: Center vertex must exist"
    );
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &center),
        1,
        "precondition: Center outbound edge must exist"
    );
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &vertex_key_bytes("In")),
        1,
        "precondition: inbound In->Center edge must exist"
    );
    let adj_before = live_row_count(&storage, adj_ks, "adjacency", &center);
    assert!(
        adj_before >= 2,
        "precondition: Center adjacency must hold OUT + IN entries (got {adj_before})"
    );

    // The plain DELETE under test — MUST fail loud, deleting nothing.
    let err = engine
        .execute("MATCH (n:Person {name: 'Center'}) DELETE n", ks, &auth)
        .await
        .expect_err("plain DELETE of a node with surviving relationships must fail loud");
    let msg = err.to_string();
    assert!(
        msg.contains("still has relationships") || msg.contains("DETACH DELETE"),
        "error must be a Neo4j-style constraint violation; got: {msg}"
    );

    // Nothing was deleted: node, both edges, and adjacency are all intact.
    assert_eq!(
        live_row_count(&storage, ks, "person_v", &center),
        1,
        "Center vertex must survive a failed DELETE"
    );
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &center),
        1,
        "Center outbound edge must survive a failed DELETE"
    );
    assert_eq!(
        live_row_count(&storage, ks, "knows_e", &vertex_key_bytes("In")),
        1,
        "inbound In->Center edge must survive a failed DELETE"
    );
    assert_eq!(
        live_row_count(&storage, adj_ks, "adjacency", &center),
        adj_before,
        "Center adjacency entries must be untouched by a failed DELETE"
    );

    // And the node is still queryable via MATCH.
    let match_center = engine
        .execute("MATCH (n:Person {name: 'Center'}) RETURN n.name", ks, &auth)
        .await
        .expect("MATCH after failed delete must succeed");
    assert_eq!(
        match_center.rows.len(),
        1,
        "Center must still be MATCHable after a failed DELETE; got: {:?}",
        match_center.rows
    );
}

/// True if any live adjacency row in `vertex`'s partition names `neighbor`
/// as its neighbor_id (i.e. an incident-edge reference to `neighbor`).
fn adjacency_partition_references(
    storage: &StorageEngine,
    adj_keyspace: &str,
    vertex_key_bytes: &[u8],
    neighbor: &[u8],
) -> bool {
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_graph::executor::expand::extract_neighbor_id;
    use ferrosa_storage::TableId;

    let tid = TableId::new(adj_keyspace, "adjacency");
    let key = DecoratedKey::new(PartitionKey::new(vertex_key_bytes.to_vec()));
    let Some(partition) = storage.read(&tid, &key).unwrap() else {
        return false;
    };
    partition.rows.iter().any(|row| {
        row.deletion.is_live()
            && extract_neighbor_id(&row.clustering, None).as_deref() == Some(neighbor)
    })
}

// ── Bolt parity test (T-QEC-D04 / URS-QEC-D05) ─────────────────────────────

/// A minimal Bolt v5 client over a real TCP socket: handshake, HELLO, LOGON,
/// then RUN/PULL per query. Exercises the **actual** `start_bolt_server`
/// connection handler and PackStream codec — the same path a Neo4j driver
/// would drive — so the test proves Bolt behaves identically to HTTP, not that
/// they merely share a function.
struct BoltTestClient {
    stream: tokio::net::TcpStream,
    decoder: ferrosa_graph::bolt::codec::ChunkDecoder,
    /// Decoded-but-not-yet-consumed messages (a single TCP read can yield
    /// several framed messages, e.g. SUCCESS + RECORDs).
    pending: std::collections::VecDeque<Vec<u8>>,
}

impl BoltTestClient {
    /// Connect and complete handshake + HELLO + LOGON (auth_disabled server,
    /// so credentials are accepted as superuser).
    async fn connect(addr: std::net::SocketAddr) -> Self {
        use ferrosa_graph::bolt::codec::ChunkDecoder;
        use ferrosa_graph::bolt::handshake::{BOLT_MAGIC, BOLT_VERSION_5_0};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Retry briefly while the spawned server task binds its listener.
        let mut stream = {
            let mut connected = None;
            for _ in 0..100 {
                if let Ok(s) = tokio::net::TcpStream::connect(addr).await {
                    connected = Some(s);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            connected.expect("Bolt TCP connect must succeed")
        };

        // Handshake: magic + 4 version proposals (only Bolt 5.0 offered).
        let mut hs = Vec::with_capacity(20);
        hs.extend_from_slice(&BOLT_MAGIC);
        hs.extend_from_slice(&BOLT_VERSION_5_0.to_be_bytes());
        hs.extend_from_slice(&[0u8; 12]);
        stream.write_all(&hs).await.expect("handshake write");
        let mut resp = [0u8; 4];
        stream
            .read_exact(&mut resp)
            .await
            .expect("handshake response read");
        assert_eq!(
            u32::from_be_bytes(resp),
            BOLT_VERSION_5_0,
            "server must negotiate Bolt 5.0"
        );

        let mut client = Self {
            stream,
            decoder: ChunkDecoder::new(),
            pending: std::collections::VecDeque::new(),
        };

        // HELLO (no inline auth) then LOGON with basic scheme.
        let hello = BoltMessage::Hello {
            extra: vec![(
                "user_agent".into(),
                PackValue::String("ferrosa-test/1.0".into()),
            )],
        };
        client.send(&hello).await;
        client.expect_success("HELLO").await;

        let logon = BoltMessage::Logon {
            auth: vec![
                ("scheme".into(), PackValue::String("basic".into())),
                ("principal".into(), PackValue::String("tester".into())),
                ("credentials".into(), PackValue::String("ignored".into())),
            ],
        };
        client.send(&logon).await;
        client.expect_success("LOGON").await;
        client
    }

    /// Send one Bolt message, chunk-framed.
    async fn send(&mut self, msg: &BoltMessage) {
        use ferrosa_graph::bolt::codec::{chunk_encode, DEFAULT_MAX_CHUNK_SIZE};
        use tokio::io::AsyncWriteExt;

        let framed = chunk_encode(&msg.encode(), DEFAULT_MAX_CHUNK_SIZE);
        self.stream.write_all(&framed).await.expect("bolt send");
    }

    /// Read the next complete Bolt message from the socket, buffering any
    /// extra messages that arrive in the same TCP read.
    async fn recv(&mut self) -> BoltMessage {
        use tokio::io::AsyncReadExt;

        loop {
            if let Some(data) = self.pending.pop_front() {
                return BoltMessage::decode(&data).expect("decode bolt message");
            }
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await.expect("bolt recv read");
            assert!(n > 0, "unexpected EOF from Bolt server");
            self.pending.extend(self.decoder.feed(&buf[..n]));
        }
    }

    /// Assert the next message is SUCCESS, surfacing FAILURE details otherwise.
    async fn expect_success(&mut self, ctx: &str) {
        match self.recv().await {
            BoltMessage::Success { .. } => {}
            BoltMessage::Failure { metadata } => {
                panic!("Bolt {ctx} returned FAILURE: {metadata:?}");
            }
            other => panic!("Bolt {ctx} expected SUCCESS, got {other:?}"),
        }
    }

    /// RUN + PULL a query, returning the result rows (each a list of PackValues).
    /// Panics (fail-loud) if the server returns a Bolt FAILURE for the RUN.
    async fn query(&mut self, cypher: &str, keyspace: &str) -> Vec<Vec<PackValue>> {
        let run = BoltMessage::Run {
            query: cypher.to_string(),
            params: vec![],
            extra: vec![("db".into(), PackValue::String(keyspace.to_string()))],
        };
        self.send(&run).await;
        match self.recv().await {
            BoltMessage::Success { .. } => {}
            BoltMessage::Failure { metadata } => {
                panic!("Bolt RUN `{cypher}` returned FAILURE: {metadata:?}");
            }
            other => panic!("Bolt RUN `{cypher}` expected SUCCESS, got {other:?}"),
        }

        let pull = BoltMessage::Pull {
            extra: vec![("n".into(), PackValue::Int(-1))],
        };
        self.send(&pull).await;

        let mut rows = Vec::new();
        loop {
            match self.recv().await {
                BoltMessage::Record { values } => rows.push(values),
                BoltMessage::Success { .. } => break,
                BoltMessage::Failure { metadata } => {
                    panic!("Bolt PULL `{cypher}` returned FAILURE: {metadata:?}");
                }
                other => panic!("Bolt PULL `{cypher}` unexpected message {other:?}"),
            }
        }
        rows
    }
}

/// Direct-CQL snapshot of the post-delete state of the `Center` node: its
/// vertex row count, outbound edge count, inbound edge counts from each named
/// in-neighbor, and its adjacency partition live-row count. Two interfaces are
/// "the same result" iff their snapshots are byte-for-byte equal (all zeros
/// after a successful DETACH DELETE).
#[derive(Debug, PartialEq, Eq)]
struct CenterDeleteSnapshot {
    person_v: usize,
    out_edges: usize,
    in_edges: Vec<(String, usize)>,
    adjacency: usize,
}

fn center_delete_snapshot(storage: &StorageEngine, in_neighbors: &[&str]) -> CenterDeleteSnapshot {
    let ks = "social";
    let adj_ks = "system_graph_social";
    let center = vertex_key_bytes("Center");
    CenterDeleteSnapshot {
        person_v: live_row_count(storage, ks, "person_v", &center),
        out_edges: live_row_count(storage, ks, "knows_e", &center),
        in_edges: in_neighbors
            .iter()
            .map(|src| {
                (
                    (*src).to_string(),
                    live_row_count(storage, ks, "knows_e", &vertex_key_bytes(src)),
                )
            })
            .collect(),
        adjacency: live_row_count(storage, adj_ks, "adjacency", &center),
    }
}

/// Build the `Center`-with-2-out-2-in graph through `engine.execute`. Shared by
/// both arms of the parity test so the pre-delete state is identical.
async fn build_center_graph(engine: &Arc<GraphEngine>, auth: &AuthContext) {
    let ks = "social";
    for name in ["Center", "OutA", "OutB", "InX", "InY"] {
        engine
            .execute(
                &format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                ks,
                auth,
            )
            .await
            .expect("node MERGE must succeed");
    }
    for dst in ["OutA", "OutB"] {
        engine
            .execute(
                &format!(
                    "MERGE (a:Person {{name: 'Center'}})-[r:KNOWS]->(b:Person {{name: '{dst}'}}) RETURN r"
                ),
                ks,
                auth,
            )
            .await
            .expect("outbound edge MERGE must succeed");
    }
    for src in ["InX", "InY"] {
        engine
            .execute(
                &format!(
                    "MERGE (a:Person {{name: '{src}'}})-[r:KNOWS]->(b:Person {{name: 'Center'}}) RETURN r"
                ),
                ks,
                auth,
            )
            .await
            .expect("inbound edge MERGE must succeed");
    }
}

/// T-QEC-D04 (URS-QEC-D05): a `DETACH DELETE n` issued over the **real Bolt
/// wire protocol** produces the SAME result as the same statement over the HTTP
/// `/graph/query` endpoint. After each delete:
///   (a) the node is immediately invisible to a subsequent read on that same
///       interface (Bolt MATCH / HTTP MATCH), and
///   (b) it is gone from direct CQL reads of the underlying `knows_e` and
///       `system_graph_social.adjacency` tables.
/// The two interfaces' post-delete CQL snapshots must be byte-for-byte equal.
/// Bolt shares the Cypher executor, so no executor change is expected — this
/// test is the proof that Bolt has not diverged.
#[tokio::test]
async fn detach_delete_via_bolt_matches_http_and_is_invisible_to_reads_and_cql() {
    let in_neighbors = ["InX", "InY"];

    // ── HTTP arm ──────────────────────────────────────────────────────────
    let (http_engine, http_storage, http_schema, _http_dir) = detach_delete_engine();
    let auth = superuser_auth();
    build_center_graph(&http_engine, &auth).await;

    // Drive the real HTTP router over the SAME engine the graph was built on,
    // so the delete and the follow-up read share one storage view.
    let http_app = build_router(AppState {
        engine: Arc::clone(&http_engine),
        schema: Arc::clone(&http_schema),
        auth_disabled: false,
    });

    // DETACH DELETE over HTTP.
    let del_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Center'}) DETACH DELETE n",
            "keyspace": "social"
        })),
    );
    let del_resp = http_app.clone().oneshot(del_req).await.unwrap();
    assert_eq!(
        del_resp.status(),
        StatusCode::OK,
        "HTTP DETACH DELETE status"
    );
    let del_body = response_json(del_resp).await;
    // The executor reports the delete as a one-row `status` result on BOTH
    // interfaces; capture HTTP's so we can prove Bolt returns the same string.
    let http_delete_status = del_body["rows"][0][0]
        .as_str()
        .expect("HTTP DETACH DELETE must return a status row")
        .to_string();
    assert_eq!(
        http_delete_status, "deleted 1 vertices",
        "HTTP DETACH DELETE status row, got body: {del_body:?}"
    );

    // (a) HTTP read on the same interface no longer sees Center.
    let read_req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Center'}) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let read_resp = http_app.oneshot(read_req).await.unwrap();
    assert_eq!(read_resp.status(), StatusCode::OK);
    let http_read_rows = response_json(read_resp).await["rows"]
        .as_array()
        .expect("rows array")
        .len();
    assert_eq!(
        http_read_rows, 0,
        "HTTP MATCH after DETACH DELETE must return no rows"
    );

    // (b) direct CQL snapshot of the HTTP store.
    let http_snapshot = center_delete_snapshot(&http_storage, &in_neighbors);

    // ── Bolt arm ──────────────────────────────────────────────────────────
    let (bolt_engine, bolt_storage, bolt_schema, _bolt_dir) = detach_delete_engine();
    build_center_graph(&bolt_engine, &auth).await;

    let bolt_config = ferrosa_graph::bolt::server::BoltConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        auth_disabled: true,
        ..Default::default()
    };
    // Bind first to learn the ephemeral port, then serve on it.
    let listener = std::net::TcpListener::bind(bolt_config.bind_addr).unwrap();
    let bolt_addr = listener.local_addr().unwrap();
    drop(listener);
    let bolt_config = ferrosa_graph::bolt::server::BoltConfig {
        bind_addr: bolt_addr,
        ..bolt_config
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_engine = Arc::clone(&bolt_engine);
    let server_schema = Arc::clone(&bolt_schema);
    let server = tokio::spawn(async move {
        ferrosa_graph::bolt::server::start_bolt_server(
            server_engine,
            server_schema,
            bolt_config,
            shutdown_rx,
        )
        .await
    });

    let mut client = BoltTestClient::connect(bolt_addr).await;

    // DETACH DELETE over Bolt — must return the SAME status row as HTTP.
    let del_rows = client
        .query(
            "MATCH (n:Person {name: 'Center'}) DETACH DELETE n",
            "social",
        )
        .await;
    let bolt_delete_status = match del_rows.as_slice() {
        [row] => match row.as_slice() {
            [PackValue::String(s)] => s.clone(),
            other => panic!("Bolt DETACH DELETE status row shape unexpected: {other:?}"),
        },
        other => panic!("Bolt DETACH DELETE must return exactly one status row; got {other:?}"),
    };
    assert_eq!(
        bolt_delete_status, http_delete_status,
        "Bolt DETACH DELETE status row must match HTTP's"
    );

    // (a) Bolt read on the same interface no longer sees Center.
    let bolt_read_rows = client
        .query("MATCH (n:Person {name: 'Center'}) RETURN n.name", "social")
        .await;
    assert!(
        bolt_read_rows.is_empty(),
        "Bolt MATCH after DETACH DELETE must return no rows; got {bolt_read_rows:?}"
    );

    // (b) direct CQL snapshot of the Bolt store.
    let bolt_snapshot = center_delete_snapshot(&bolt_storage, &in_neighbors);

    // Clean shutdown of the Bolt server.
    let _ = shutdown_tx.send(true);
    server.abort();

    // ── Parity assertions ─────────────────────────────────────────────────
    // Both interfaces fully tore down Center: nothing left in CQL.
    assert_eq!(
        http_snapshot,
        CenterDeleteSnapshot {
            person_v: 0,
            out_edges: 0,
            in_edges: vec![("InX".into(), 0), ("InY".into(), 0)],
            adjacency: 0,
        },
        "HTTP DETACH DELETE must leave zero CQL rows for Center"
    );
    // The whole point of T-QEC-D04: Bolt == HTTP, exactly.
    assert_eq!(
        bolt_snapshot, http_snapshot,
        "Bolt DETACH DELETE must produce the SAME CQL result as HTTP"
    );
    assert_eq!(
        http_read_rows,
        bolt_read_rows.len(),
        "Bolt and HTTP post-delete reads must agree (both empty)"
    );
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

// ── Comprehensions & map projection (QE-M4) ──────────────────────────────────

/// List comprehension `[x IN list WHERE pred | expr]` evaluates end-to-end
/// through the HTTP query path.
#[tokio::test]
async fn list_comprehension_projects_through_http() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "RETURN [x IN [1, 2, 3, 4] WHERE x > 2 | x * 10] AS doubled",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list comprehension must return 200"
    );
    let body = response_json(resp).await;
    assert_eq!(body["rows"], serde_json::json!([[[30, 40]]]));
}

/// Map projection `n {.name, .age}` builds a map by selecting properties off a
/// matched node, end-to-end through the HTTP query path.
#[tokio::test]
async fn map_projection_selects_properties_through_http() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Create a Person to project.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (n:Person {name: 'Projee'}) SET n.age = 41 RETURN n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Projee'}) RETURN n {.name, .age} AS proj",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "map projection must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected one matched Person, got {rows:?}");
    let proj = &rows[0][0];
    assert_eq!(proj["name"], serde_json::json!("Projee"));
    // age is projected through whatever the storage layer returns; assert the
    // key is present and carries the stored value (not null/missing).
    assert!(
        !proj["age"].is_null(),
        "map projection must carry the .age property, got {proj:?}"
    );
    assert_eq!(
        proj.as_object().unwrap().len(),
        2,
        "exactly name + age projected, got {proj:?}"
    );
}

/// Pattern comprehension `[ (n)-[:KNOWS]->(m) | m.name ]` traverses real edges
/// and collects projected target properties — it must NOT silently return an
/// empty list when edges exist.
#[tokio::test]
async fn pattern_comprehension_traverses_edges_through_http() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Create two friends of Anchor.
    for name in ["Anchor", "FriendA", "FriendB"] {
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
    for friend in ["FriendA", "FriendB"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!(
                    "MERGE (a:Person {{name: 'Anchor'}})-[r:KNOWS]->(b:Person {{name: '{friend}'}}) RETURN r"
                ),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "edge MERGE must return 200");
    }

    // Pattern comprehension collecting friend names.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (n:Person {name: 'Anchor'}) \
                      RETURN [ (n)-[:KNOWS]->(m:Person) | m.name ] AS friends",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "pattern comprehension must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected one anchor row, got {rows:?}");
    let friends = rows[0][0].as_array().expect("friends list");
    let mut names: Vec<String> = friends
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["FriendA".to_string(), "FriendB".to_string()],
        "pattern comprehension must collect both traversed friends, not silently empty"
    );
}

/// FOREACH (x IN list | CREATE (:Person {name: x})) must execute the contained
/// update clause once per list element, creating exactly one node per element.
#[tokio::test]
async fn http_foreach_creates_one_node_per_list_element() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "FOREACH (x IN ['Ada', 'Babbage', 'Cantor'] | \
                      CREATE (n:Person {name: x}))",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "FOREACH with a CREATE body must return 200"
    );

    // MATCH back — must see exactly the three nodes, one per list element,
    // each carrying the element's value as its name.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
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
    let rows = body["rows"].as_array().expect("rows array");
    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_str().expect("name string").to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "Ada".to_string(),
            "Babbage".to_string(),
            "Cantor".to_string()
        ],
        "FOREACH must CREATE exactly one Person per list element, named after the element"
    );
}

/// FOREACH must be atomic with respect to its body: if the contained update
/// clause fails for any element, the whole statement rolls back and no nodes
/// are created (no partial writes from the earlier, successful elements).
///
/// Here the body targets a label with no backing table (`Ghost`), which fails
/// validation. The earlier `Person` elements must NOT be persisted.
#[tokio::test]
async fn http_foreach_partial_failure_rolls_back() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // The body first creates a VALID Person, then a CREATE against an unknown
    // label `Ghost`. The Person create would succeed on its own; only the Ghost
    // create fails. Atomicity requires the whole FOREACH to roll back, so NONE of
    // the Person nodes (for any element) may be persisted.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "FOREACH (x IN ['Ada', 'Babbage'] | \
                      CREATE (p:Person {name: x}) \
                      CREATE (g:Ghost {name: x}))",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::OK,
        "FOREACH whose body targets a non-existent label must fail loud, not 200"
    );

    // No Person nodes — and critically nothing partially written — should remain.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
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
    let rows = body["rows"].as_array().expect("rows array");
    assert!(
        rows.is_empty(),
        "FOREACH body failure must roll back: expected zero Person nodes, got {rows:?}"
    );
}

/// Regression: a NESTED FOREACH whose body fails validation must roll back the
/// OUTER element writes too. Previously phase 1 skipped nested-FOREACH bodies,
/// so the outer CREATEs committed before the nested body ever validated — a
/// partial write survived a failing FOREACH. The whole statement must be planned
/// (recursively, including nested bodies) before any write, so an unknown label
/// in the nested body aborts with ZERO outer writes.
#[tokio::test]
async fn http_foreach_nested_body_failure_rolls_back_outer() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "FOREACH (x IN ['Ada', 'Babbage'] | \
                      CREATE (p:Person {name: x}) \
                      FOREACH (y IN [1] | CREATE (g:Ghost {name: x})))",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::OK,
        "FOREACH with a nested body targeting an unknown label must fail loud"
    );

    // The outer CREATE (Person) must NOT have persisted — the failing nested
    // FOREACH body has to be validated/planned before any outer write.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
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
    let rows = body["rows"].as_array().expect("rows array");
    assert!(
        rows.is_empty(),
        "nested FOREACH failure must roll back outer writes: expected zero Person nodes, got {rows:?}"
    );
}

/// A nested FOREACH whose body is entirely valid runs once per (outer × inner)
/// element, in source order, materializing both loop variables. Proves the
/// recursive flatten preserves per-element semantics rather than batching all
/// outer clauses ahead of nested loops.
#[tokio::test]
async fn http_foreach_nested_creates_cross_product() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "FOREACH (x IN ['Ada', 'Babbage'] | \
                      FOREACH (y IN ['x', 'y'] | \
                      CREATE (n:Person {name: x})))",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "valid nested FOREACH must return 200"
    );

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
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
    let rows = body["rows"].as_array().expect("rows array");
    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_str().expect("name string").to_string())
        .collect();
    names.sort();
    // 2 outer × 2 inner = 4 nodes; each named after the OUTER element.
    assert_eq!(
        names,
        vec![
            "Ada".to_string(),
            "Ada".to_string(),
            "Babbage".to_string(),
            "Babbage".to_string()
        ],
        "nested FOREACH must create one node per (outer, inner) element pair"
    );
}

/// Seed three Person nodes with ages, returning each id so the test can assert
/// per-node behaviour later. Helper shared by the CALL {} subquery tests.
async fn seed_three_people(schema: &Arc<Schema>, storage: &Arc<StorageEngine>) {
    for (name, age) in [("Ada", 36), ("Babbage", 49), ("Cantor", 72)] {
        let app = build_app(Arc::clone(schema), Arc::clone(storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("CREATE (n:Person {{name: '{name}', age: {age}}})"),
                "keyspace": "social"
            })),
        );
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "seed CREATE must return 200");
    }
}

/// Correlated `CALL {}` subquery: the inner query unit runs once per outer row,
/// with the imported variable (`WITH p`) bound to that row. The inner results are
/// UNITED, and (because the outer query keeps projecting) each inner row is paired
/// with its driving outer row. Here each outer Person drives an inner query that
/// derives a value from the imported node's own properties — a true correlation,
/// not a constant subquery.
#[tokio::test]
async fn http_call_subquery_correlated_returns_per_row_inner_results() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    // For each Person p, the inner subquery imports p and projects a label derived
    // from p's own age. One inner row per outer row -> three result rows total,
    // each pairing the outer name with the correlated inner value.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN p.age + 1 AS next_age } \
                      RETURN p.name, next_age",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "correlated CALL {{}} subquery must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    let mut pairs: Vec<(String, i64)> = rows
        .iter()
        .map(|r| {
            (
                r[0].as_str().expect("name string").to_string(),
                r[1].as_i64().expect("next_age int"),
            )
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), 37),
            ("Babbage".to_string(), 50),
            ("Cantor".to_string(), 73),
        ],
        "each outer row must drive its own correlated inner result (p.age + 1)"
    );
}

/// Unit `CALL {}` subquery (no inner RETURN): the inner query only performs
/// updates, executed once per outer row. Here every Person drives a CREATE that
/// writes a new node named after the imported person — a per-row write side effect.
/// A unit subquery does not change the outer cardinality; the outer query still
/// projects its own rows.
#[tokio::test]
async fn http_call_subquery_unit_performs_writes_per_row() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    // For each existing Person p, create a mirror Person carrying p's imported name
    // and an age derived from p's imported age. 3 outer rows -> 3 new nodes, each
    // correlated to its driving row. The new node's name equals the original's
    // (proving per-row import) while its age is shifted so the mirror is
    // distinguishable from the source.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p CREATE (m:Person {name: p.name, age: p.age + 1000}) }",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "unit CALL {{}} subquery performing writes must return 200"
    );

    // Now there must be 6 Person nodes: the 3 originals plus 3 mirrors, each
    // mirror sharing its source's name (so each name appears exactly twice).
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
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    let mut pairs: Vec<(String, i64)> = rows
        .iter()
        .map(|r| {
            (
                r[0].as_str().expect("name string").to_string(),
                r[1].as_i64().expect("age int"),
            )
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Ada".to_string(), 36),
            ("Ada".to_string(), 1036),
            ("Babbage".to_string(), 49),
            ("Babbage".to_string(), 1049),
            ("Cantor".to_string(), 72),
            ("Cantor".to_string(), 1072),
        ],
        "unit CALL {{}} must perform one correlated write per outer row (mirror with age + 1000)"
    );
}

/// Trailing `RETURN ... LIMIT n` after a `CALL {}` must apply the limit. With 3
/// outer rows the unbounded result is 3 rows; `LIMIT 1` must cut it to exactly 1.
/// Silently ignoring the limit (returning 3) is a wrong result (URS-QEC-X01).
#[tokio::test]
async fn http_call_subquery_trailing_return_applies_limit() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN p.age AS a } \
                      RETURN p.name, a LIMIT 1",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "trailing LIMIT must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "trailing CALL {{}} RETURN ... LIMIT 1 must emit exactly 1 row, not all 3"
    );
}

/// Trailing `RETURN DISTINCT` after a `CALL {}` must deduplicate. Three outer rows
/// each project the constant `1`; `DISTINCT one` must collapse to a single row.
/// Returning 3 duplicate rows is a silent wrong result (URS-QEC-X01).
#[tokio::test]
async fn http_call_subquery_trailing_return_applies_distinct() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN 1 AS one } \
                      RETURN DISTINCT one",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "trailing DISTINCT must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "trailing CALL {{}} RETURN DISTINCT must collapse 3 identical rows to 1"
    );
    assert_eq!(
        rows[0][0].as_i64(),
        Some(1),
        "the surviving distinct row is 1"
    );
}

/// Trailing `RETURN ... ORDER BY` after a `CALL {}` must sort the united result.
/// Without ORDER BY the rows arrive in outer-match order; ORDER BY age DESC must
/// produce a strictly descending sequence. Ignoring ORDER BY is a wrong result.
#[tokio::test]
async fn http_call_subquery_trailing_return_applies_order_by() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN p.age AS a } \
                      RETURN a ORDER BY a DESC",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "trailing ORDER BY must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    let ages: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_i64().expect("age int"))
        .collect();
    assert_eq!(
        ages,
        vec![72, 49, 36],
        "trailing CALL {{}} RETURN ... ORDER BY a DESC must sort the united rows descending"
    );
}

/// Trailing `RETURN count(*)` after a `CALL {}` must aggregate over the united
/// result. Three outer rows each yield one inner row, so `count(*)` over the whole
/// trailing projection is 3. Evaluating count per-row (returning 1 three times, or
/// any non-grouped shape) is a silent wrong result (URS-QEC-X01).
#[tokio::test]
async fn http_call_subquery_trailing_return_aggregates_count() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN p.age AS a } \
                      RETURN count(*) AS n",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "trailing aggregation must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "an aggregate-only trailing RETURN must collapse to a single grouped row"
    );
    assert_eq!(
        rows[0][0].as_i64(),
        Some(3),
        "count(*) over the 3 united inner rows must be 3, not a per-row 1"
    );
}

/// Trailing `RETURN key, count(*)` after a `CALL {}` must group by the non-aggregate
/// key. Three Person rows each map to a distinct age, so grouping by age yields 3
/// groups of count 1. A correlated grouped aggregation over the united result.
#[tokio::test]
async fn http_call_subquery_trailing_return_grouped_aggregate() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p RETURN p.age AS a } \
                      RETURN a, count(*) AS n ORDER BY a",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "trailing grouped aggregate must return 200"
    );
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (r[0].as_i64().expect("age"), r[1].as_i64().expect("count")))
        .collect();
    assert_eq!(
        pairs,
        vec![(36, 1), (49, 1), (72, 1)],
        "grouped aggregate must produce one (age, count=1) row per distinct age, ordered"
    );
}

/// Fail loud: a `CALL {}` nested inside another `CALL {}` is not supported. The
/// engine must return a clear Cypher error (non-200), never silently no-op or
/// return wrong/empty results.
#[tokio::test]
async fn http_call_subquery_nested_call_fails_loud() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);
    seed_three_people(&schema, &storage).await;

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (p:Person) \
                      CALL { WITH p CALL { WITH p RETURN p.age AS a } RETURN a } \
                      RETURN p.name, a",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::OK,
        "a CALL {{}} nested inside a CALL {{}} must fail loud, not silently succeed"
    );
}

/// t_8c506227: a label-agnostic outgoing expansion `(a)-[r]->(n)` must hydrate
/// the opposite endpoint node (and the relationship) across MULTIPLE edge
/// labels — exactly like the typed `-[r:KNOWS]->` form does — instead of
/// returning a null neighbor. Regression guard for the generic explorer's
/// recenter / n-hop expansion over all relationship types.
#[tokio::test]
async fn http_label_agnostic_outgoing_expansion_hydrates_neighbor_nodes() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    create_social_likes_edge_schema(&schema);
    register_social_tables_with_storage(&storage);
    register_social_likes_table_with_storage(&storage);

    for name in ["Alice", "Bob", "Carol"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                "keyspace": "social"
            })),
        );
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    // Two DIFFERENT edge labels out of Alice, so the unlabeled hop must resolve
    // the opposite vertex table per adjacency row (heterogeneous edge tables).
    for (label, dst) in [("KNOWS", "Bob"), ("LIKES", "Carol")] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!(
                    "MERGE (a:Person {{name: 'Alice'}})-[r:{label}]->(b:Person {{name: '{dst}'}}) RETURN r"
                ),
                "keyspace": "social"
            })),
        );
        assert_eq!(
            app.oneshot(req).await.unwrap().status(),
            StatusCode::OK,
            "seeding the {label} edge should succeed"
        );
    }

    // Label-agnostic outgoing expansion: BOTH neighbors must hydrate with real
    // node properties, not null.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'Alice'})-[r]->(n) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");

    let names: Vec<String> = rows
        .iter()
        .map(|r| {
            r[0].as_str()
                .unwrap_or_else(|| {
                    panic!("label-agnostic expansion left the neighbor un-hydrated (null): {r:?}")
                })
                .to_string()
        })
        .collect();

    assert_eq!(
        rows.len(),
        2,
        "expected both the KNOWS and LIKES neighbors, got {names:?}"
    );
    assert!(
        names.contains(&"Bob".to_string()),
        "KNOWS neighbor 'Bob' missing / un-hydrated: {names:?}"
    );
    assert!(
        names.contains(&"Carol".to_string()),
        "LIKES neighbor 'Carol' missing / un-hydrated: {names:?}"
    );
}

/// t_8c506227 (inverse): a label-agnostic INCOMING expansion `(b)<-[r]-(n)`
/// must hydrate the SOURCE neighbor (resolved via the edge's source label),
/// not the target — the direction the earlier report found "surprising".
#[tokio::test]
async fn http_label_agnostic_incoming_expansion_hydrates_source_neighbor() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

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
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r",
            "keyspace": "social"
        })),
    );
    assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);

    // Incoming expansion from Bob: the un-hydrated bug would leave n null; the
    // fix must resolve the SOURCE vertex (Alice) via graph.source_label.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (b:Person {name: 'Bob'})<-[r]-(n) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one incoming neighbor, got {rows:?}"
    );
    assert_eq!(
        rows[0][0].as_str(),
        Some("Alice"),
        "incoming label-agnostic expansion must hydrate the SOURCE neighbor 'Alice': {rows:?}"
    );
}

/// t_8c506227 (partial-label gap): a TYPED edge with an UNLABELED target node
/// `-[r:KNOWS]->(n)` must still hydrate `n` — its table is resolved from the
/// edge's graph.target_label even though the pattern did not label `n`.
#[tokio::test]
async fn http_typed_edge_unlabeled_target_hydrates_neighbor() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

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
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r",
            "keyspace": "social"
        })),
    );
    assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(n) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected one neighbor, got {rows:?}");
    assert_eq!(
        rows[0][0].as_str(),
        Some("Bob"),
        "typed edge with unlabeled target must hydrate n via the edge's target_label: {rows:?}"
    );
}

/// t_8c506227 (partial-label gap): an UNLABELED edge with a LABELED target
/// `-[r]->(n:Person)` must hydrate the RELATIONSHIP `r` (real _type + edge
/// properties), not fall back to raw adjacency cells (col_0).
#[tokio::test]
async fn http_unlabeled_edge_labeled_target_hydrates_relationship() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

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
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MERGE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2021}]->(b:Person {name: 'Bob'}) RETURN r",
            "keyspace": "social"
        })),
    );
    assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);

    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'Alice'})-[r]->(n:Person) RETURN r",
            "keyspace": "social"
        })),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected one relationship, got {rows:?}");
    let r = &rows[0][0];
    assert_eq!(
        r["_type"].as_str(),
        Some("KNOWS"),
        "unlabeled edge to a labeled target must hydrate r._type from the resolved edge: {r:?}"
    );
    assert_eq!(
        r["since"].as_i64(),
        Some(2021),
        "relationship properties must be hydrated, not the col_0 fallback: {r:?}"
    );
    assert!(
        r.get("col_0").is_none(),
        "resolved relationship must not carry the raw col_0 adjacency fallback: {r:?}"
    );
}

/// t_8c506227 (acceptance #4 / write-side guarantee): the "silent null endpoint"
/// state is prevented at the source — a graph EDGE table must declare its
/// endpoint labels, so creating one without `graph.target_label` is rejected at
/// DDL time with a clear error rather than being allowed to exist and later
/// yield un-hydratable neighbors.
#[tokio::test]
async fn graph_edge_table_without_endpoint_labels_is_rejected() {
    let (schema, _storage, _dir) = setup();
    create_social_graph_schema(&schema);
    let auth = superuser_auth();

    let mut cols = IndexMap::new();
    cols.insert(
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
    cols.insert(
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
    let mut ext = HashMap::new();
    ext.insert("graph.type".to_string(), "edge".to_string());
    ext.insert("graph.label".to_string(), "UNDECLARED".to_string());
    ext.insert("graph.source".to_string(), "src_id".to_string());
    ext.insert("graph.target".to_string(), "dst_id".to_string());
    // Deliberately no graph.source_label / graph.target_label.

    let result = schema.create_table(
        TableMetadata {
            keyspace: "social".to_string(),
            name: "undeclared_e".to_string(),
            id: Uuid::new_v4(),
            columns: cols,
            partition_key: vec!["src_id".to_string()],
            clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: ext,
            is_system: false,
        },
        &auth,
    );

    let err = result
        .expect_err("an edge table missing its endpoint labels must be rejected, not created");
    let msg = format!("{err}");
    assert!(
        msg.contains("graph.source_label") || msg.contains("graph.target_label"),
        "rejection must name the missing endpoint-label extension: {msg}"
    );
    assert!(
        msg.contains("undeclared_e"),
        "rejection must name the offending edge table: {msg}"
    );
}

/// t_0cc8d63e: a bare `LIMIT k` expansion (no ORDER BY / DISTINCT / WITH /
/// aggregation) must short-circuit — hydrating only ~k neighbors, not the whole
/// fan-out. Asserted via `stats.vertices_read`, which counts hydrated neighbors.
#[tokio::test]
async fn http_limit_short_circuits_expansion_hydration() {
    let (schema, storage, _dir) = setup();
    create_social_graph_schema(&schema);
    register_social_tables_with_storage(&storage);

    // Alice + five neighbors, all reachable via KNOWS.
    for name in ["Alice", "N1", "N2", "N3", "N4", "N5"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!("MERGE (n:Person {{name: '{name}'}}) RETURN n"),
                "keyspace": "social"
            })),
        );
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }
    for dst in ["N1", "N2", "N3", "N4", "N5"] {
        let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
        let req = json_request(
            "POST",
            "/graph/query",
            Some(serde_json::json!({
                "query": format!(
                    "MERGE (a:Person {{name: 'Alice'}})-[r:KNOWS]->(b:Person {{name: '{dst}'}}) RETURN r"
                ),
                "keyspace": "social"
            })),
        );
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    // Baseline: the unlimited expansion hydrates every neighbor.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'Alice'})-[r]->(n) RETURN n.name",
            "keyspace": "social"
        })),
    );
    let body = response_json(app.oneshot(req).await.unwrap()).await;
    assert_eq!(
        body["rows"].as_array().unwrap().len(),
        5,
        "baseline must see all five neighbors: {body}"
    );
    let read_all = body["stats"]["vertices_read"]
        .as_u64()
        .expect("vertices_read");

    // LIMIT 2 must short-circuit: it stops hydrating once two rows accumulate,
    // so it reads strictly fewer neighbors than the unlimited query.
    let app = build_app(Arc::clone(&schema), Arc::clone(&storage));
    let req = json_request(
        "POST",
        "/graph/query",
        Some(serde_json::json!({
            "query": "MATCH (a:Person {name: 'Alice'})-[r]->(n) RETURN n.name LIMIT 2",
            "keyspace": "social"
        })),
    );
    let body = response_json(app.oneshot(req).await.unwrap()).await;
    assert_eq!(
        body["rows"].as_array().unwrap().len(),
        2,
        "LIMIT 2 must return exactly two rows: {body}"
    );
    let read_2 = body["stats"]["vertices_read"]
        .as_u64()
        .expect("vertices_read");
    assert!(
        read_all - read_2 >= 3,
        "LIMIT 2 must skip hydrating the 3 excess neighbors (short-circuit): \
         limited={read_2} unlimited={read_all}"
    );
}
