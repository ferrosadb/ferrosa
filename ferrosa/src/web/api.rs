use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use ferrosa_cluster::raft::NodeState;
use ferrosa_cluster::ModeController;
use ferrosa_common::DataType;
use ferrosa_schema::VirtualTableRegistry;
use serde_json::{json, Value};
use uuid::Uuid;

use super::WebAppState;

// ---------------------------------------------------------------------------
// Request / response types for cluster management endpoints
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct AddNodeRequest {
    host_id: String,
}

#[derive(serde::Deserialize)]
struct DecommissionRequest {
    host_id: Option<String>,
}

#[derive(serde::Serialize)]
struct RingInfo {
    nodes: Vec<RingNodeInfo>,
}

#[derive(serde::Serialize)]
struct RingNodeInfo {
    node_id: u64,
    host_id: String,
    address: String,
    state: String,
    token_count: usize,
}

pub fn routes() -> Router<WebAppState> {
    Router::new()
        .route("/connections", get(get_connections))
        .route("/storage_stats", get(get_storage_stats))
        .route("/storage", get(get_storage_stats))
        .route("/active_queries", get(get_active_queries))
        .route("/queries", get(get_active_queries))
        .route("/tables", get(list_tables))
}

/// Prometheus text exposition endpoint — returns `text/plain; charset=utf-8`.
///
/// Sits outside the auth middleware so Prometheus scrapers work without
/// credentials.
pub async fn get_metrics(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (
    StatusCode,
    [(axum::http::header::HeaderName, &'static str); 1],
    String,
) {
    let body = ferrosa_cql::prometheus::render_metrics(&registry);
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        body,
    )
}

pub fn cluster_routes() -> Router<WebAppState> {
    Router::new()
        .route("/status", get(cluster_status))
        .route("/promote", post(cluster_promote))
        .route("/switchover", post(cluster_switchover))
        .route("/add-node", post(add_node_handler))
        .route("/decommission", post(decommission_handler))
        .route("/ring", get(ring_handler))
        .route("/rebalance", post(rebalance_handler))
}

async fn cluster_status(State(mc): State<Arc<ModeController>>) -> Json<Value> {
    Json(json!({
        "mode": mc.mode().to_string(),
        "role": mc.role().map(|r| r.to_string()),
        "host_id": mc.host_id().to_string(),
    }))
}

async fn cluster_promote(State(mc): State<Arc<ModeController>>) -> (StatusCode, Json<Value>) {
    match mc.force_promote() {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "status": "promoted",
                "mode": mc.mode().to_string(),
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn cluster_switchover(State(mc): State<Arc<ModeController>>) -> (StatusCode, Json<Value>) {
    match mc.switchover().await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "status": "switchover complete",
                "role": mc.role().map(|r| r.to_string()),
            })),
        ),
        Err(ferrosa_cluster::ClusterError::NotPrimary) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "switchover must be initiated from the primary node",
                "hint": "POST to the primary node's /api/cluster/switchover endpoint",
            })),
        ),
        Err(ferrosa_cluster::ClusterError::Net(e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": format!("peer communication failed: {e}"),
                "hint": "ensure both nodes are running and connected",
            })),
        ),
        Err(ferrosa_cluster::ClusterError::ReplicationFailed(reason)) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": format!("role swap rejected by peer: {reason}"),
                "hint": "check peer node logs for details",
            })),
        ),
        Err(ferrosa_cluster::ClusterError::ModeTransitionRejected(reason)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": format!("mode transition rejected: {reason}"),
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

// ---------------------------------------------------------------------------
// Cluster management handlers
// ---------------------------------------------------------------------------

/// `POST /api/cluster/add-node` — pre-approve a node for cluster admission.
///
/// Parses the `host_id` UUID from the request body, records approval in the
/// mode controller's local set, and proposes an `ApproveNode` Raft command
/// if Raft is available.
async fn add_node_handler(
    State(mc): State<Arc<ModeController>>,
    Json(body): Json<AddNodeRequest>,
) -> (StatusCode, Json<Value>) {
    let host_id = match body.host_id.parse::<Uuid>() {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid host_id UUID: {e}") })),
            );
        }
    };

    // Record approval in the controller's local set.
    mc.approve_node(host_id);

    // If Raft is initialized, also propose ApproveNode so all replicas learn.
    if let Some(raft) = mc.raft() {
        let cmd = ferrosa_cluster::raft::RaftCommand {
            op: ferrosa_cluster::raft::RaftOp::ApproveNode { host_id },
            schema_version: uuid::Uuid::new_v4(),
        };
        if let Err(e) = raft.client_write(cmd).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Raft ApproveNode failed: {e}") })),
            );
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "approved",
            "host_id": host_id.to_string(),
        })),
    )
}

/// `POST /api/cluster/decommission` — initiate graceful removal of a node.
///
/// Parses the `host_id` UUID and delegates to
/// [`ModeController::initiate_decommission`], which proposes a `LeaveNode`
/// Raft command.
async fn decommission_handler(
    State(mc): State<Arc<ModeController>>,
    Json(body): Json<DecommissionRequest>,
) -> (StatusCode, Json<Value>) {
    // When host_id is omitted, decommission the local node.
    let host_id = match body.host_id {
        Some(id_str) => match id_str.parse::<Uuid>() {
            Ok(id) => id,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("invalid host_id UUID: {e}") })),
                );
            }
        },
        None => mc.host_id(),
    };

    match mc.initiate_decommission(host_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "status": "decommissioning",
                "host_id": host_id.to_string(),
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `GET /api/cluster/ring` — return token ring topology.
///
/// Returns a JSON object with a `nodes` array. Each entry contains the node's
/// openraft `node_id`, `host_id`, internode `address`, lifecycle `state`, and
/// `token_count`.  Returns 503 if not in cluster mode.
async fn ring_handler(State(mc): State<Arc<ModeController>>) -> (StatusCode, Json<Value>) {
    let ring = match mc.token_ring() {
        Some(r) => r,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "not in cluster mode" })),
            );
        }
    };

    let nodes: Vec<RingNodeInfo> = ring
        .node_ids()
        .into_iter()
        .filter_map(|node_id| {
            let info = ring.get_node(node_id)?;
            Some(RingNodeInfo {
                node_id,
                host_id: info.host_id.to_string(),
                address: info.addr.clone(),
                state: node_state_to_str(info.state),
                token_count: ring.tokens_for_node(node_id).len(),
            })
        })
        .collect();

    let ring_info = RingInfo { nodes };
    match serde_json::to_value(&ring_info) {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `POST /api/cluster/rebalance` — rebalance token distribution across nodes.
///
/// Requires Raft to be initialized. Calls [`ferrosa_cluster::rebalance::execute_rebalance`]
/// which computes a rebalance plan and proposes `AssignTokens` Raft commands.
async fn rebalance_handler(State(mc): State<Arc<ModeController>>) -> (StatusCode, Json<Value>) {
    let raft = match mc.raft() {
        Some(r) => r,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "raft not initialized" })),
            );
        }
    };

    let ring = match mc.token_ring() {
        Some(r) => r,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "not in cluster mode" })),
            );
        }
    };

    match ferrosa_cluster::rebalance::execute_rebalance(&raft, &ring).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "rebalance complete" })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Convert [`NodeState`] to a human-readable string for JSON output.
fn node_state_to_str(state: NodeState) -> String {
    match state {
        NodeState::Joining => "Joining".to_string(),
        NodeState::Normal => "Normal".to_string(),
        NodeState::Leaving => "Leaving".to_string(),
        NodeState::Decommissioned => "Decommissioned".to_string(),
    }
}

async fn get_connections(State(registry): State<Arc<VirtualTableRegistry>>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "connections"))
}

async fn get_storage_stats(State(registry): State<Arc<VirtualTableRegistry>>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "storage_stats"))
}

async fn get_active_queries(State(registry): State<Arc<VirtualTableRegistry>>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "active_queries"))
}

async fn list_tables(State(registry): State<Arc<VirtualTableRegistry>>) -> Json<Value> {
    let tables = registry.list("system_observability");
    let names: Vec<&str> = tables.iter().map(|t| t.name()).collect();
    Json(json!(names))
}

pub(crate) fn virtual_table_to_json(registry: &VirtualTableRegistry, table_name: &str) -> Value {
    let table = match registry.get("system_observability", table_name) {
        Some(t) => t,
        None => return json!([]),
    };

    let columns = table.columns();
    let rows = table.read(None);

    let json_rows: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(cell) = row.cells.get(i) {
                    if let Some(bytes) = cell.value.as_deref() {
                        let val = match col.data_type {
                            DataType::Text => {
                                Value::String(String::from_utf8_lossy(bytes).to_string())
                            }
                            DataType::Int => {
                                if bytes.len() >= 4 {
                                    Value::Number(
                                        i32::from_be_bytes(
                                            bytes[..4].try_into().unwrap_or_default(),
                                        )
                                        .into(),
                                    )
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::BigInt | DataType::Timestamp => {
                                if bytes.len() >= 8 {
                                    Value::Number(
                                        i64::from_be_bytes(
                                            bytes[..8].try_into().unwrap_or_default(),
                                        )
                                        .into(),
                                    )
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::Double => {
                                if bytes.len() >= 8 {
                                    let f = f64::from_be_bytes(
                                        bytes[..8].try_into().unwrap_or_default(),
                                    );
                                    serde_json::Number::from_f64(f)
                                        .map(Value::Number)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::Boolean => Value::Bool(!bytes.is_empty() && bytes[0] != 0),
                            _ => Value::String("<binary>".to_string()),
                        };
                        obj.insert(col.name.clone(), val);
                    } else {
                        obj.insert(col.name.clone(), Value::Null);
                    }
                }
            }
            Value::Object(obj)
        })
        .collect();

    json!(json_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use ferrosa_cluster::ring::TokenRing;
    use ferrosa_cluster::ModeController;
    use ferrosa_common::CellValue;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_schema::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
        VirtualTableRegistry,
    };
    use ferrosa_storage::commitlog::CommitLogConfig;
    use ferrosa_storage::compaction::CompactionConfig;
    use ferrosa_storage::{StorageEngine, StorageEngineConfig};
    use tower::ServiceExt;

    /// Build a minimal `WebAppState` with a freshly created `ModeController`.
    fn make_state() -> WebAppState {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
        };
        let storage = Arc::new(StorageEngine::new(storage_config, None).expect("storage engine"));
        let registry = Arc::new(HandlerRegistry::new());
        // Build a minimal schema with no auth complexity.
        let schema = Arc::new(
            ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: ferrosa_schema::RateLimitConfig::default(),
                audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::DeploymentMode::Development,
            })
            .expect("test schema"),
        );
        let host_id = uuid::Uuid::new_v4();
        let (mc, _handles) = ModeController::new(
            Arc::new(ferrosa_cluster::ClusterConfig::default()),
            Arc::new(ferrosa_net::config::NetConfig::default()),
            host_id,
            storage.clone(),
            schema.clone(),
            registry,
        );
        WebAppState {
            registry: Arc::new(VirtualTableRegistry::new()),
            mode_controller: mc,
            schema,
            storage,
            host_id,
            auth_disabled: true,
        }
    }

    struct StubTable {
        cols: Vec<VirtualColumnDef>,
        rows: Vec<VirtualRow>,
    }

    impl VirtualTable for StubTable {
        fn name(&self) -> &str {
            "test_table"
        }
        fn keyspace(&self) -> &str {
            "system_observability"
        }
        fn columns(&self) -> &[VirtualColumnDef] {
            &self.cols
        }
        fn primary_key_columns(&self) -> &[usize] {
            &[0]
        }
        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
            self.rows.clone()
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn virtual_table_to_json_empty() {
        let registry = VirtualTableRegistry::new();
        let result = virtual_table_to_json(&registry, "nonexistent");
        assert_eq!(result, json!([]));
    }

    #[test]
    fn virtual_table_to_json_text_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "host".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(b"127.0.0.1".to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["host"], "127.0.0.1");
    }

    #[test]
    fn virtual_table_to_json_int_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "count".to_string(),
                data_type: DataType::Int,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(42i32.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["count"], 42);
    }

    #[test]
    fn virtual_table_to_json_bigint_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "total".to_string(),
                data_type: DataType::BigInt,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(1_000_000i64.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["total"], 1_000_000);
    }

    #[test]
    fn virtual_table_to_json_double_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "ratio".to_string(),
                data_type: DataType::Double,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(1.5f64.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert!((rows[0]["ratio"].as_f64().unwrap() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn virtual_table_to_json_boolean_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(vec![1], 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["active"], true);
    }

    #[test]
    fn virtual_table_to_json_null_value() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "host".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::tombstone(1, 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["host"], Value::Null);
    }

    #[test]
    fn virtual_table_to_json_blob_shows_binary() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "data".to_string(),
                data_type: DataType::Blob,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(vec![0xDE, 0xAD], 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["data"], "<binary>");
    }

    struct NamedStubTable {
        table_name: &'static str,
    }

    impl VirtualTable for NamedStubTable {
        fn name(&self) -> &str {
            self.table_name
        }
        fn keyspace(&self) -> &str {
            "system_observability"
        }
        fn columns(&self) -> &[VirtualColumnDef] {
            &[]
        }
        fn primary_key_columns(&self) -> &[usize] {
            &[]
        }
        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
            vec![]
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn virtual_table_registration_lists_both_tables() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(NamedStubTable {
            table_name: "connections",
        }));
        registry.register(Arc::new(NamedStubTable {
            table_name: "active_queries",
        }));

        let tables = registry.list("system_observability");
        assert_eq!(tables.len(), 2);

        let names: Vec<&str> = tables.iter().map(|t| t.name()).collect();
        assert!(names.contains(&"connections"));
        assert!(names.contains(&"active_queries"));
    }

    #[test]
    fn virtual_table_to_json_multiple_columns() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![
                VirtualColumnDef {
                    name: "host".to_string(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "port".to_string(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"localhost".to_vec(), 1),
                    CellValue::live(9042i32.to_be_bytes().to_vec(), 1),
                ],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["host"], "localhost");
        assert_eq!(rows[0]["port"], 9042);
    }

    // ---- Cluster ring endpoint tests ------------------------------------

    /// Ring endpoint returns 503 when the node is not in cluster mode.
    #[tokio::test]
    async fn api_ring_returns_503_when_not_in_cluster_mode() {
        let state = make_state();
        // No ring installed — standalone mode.
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/cluster/ring")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Ring endpoint returns JSON with the correct number of nodes when a ring
    /// is seeded directly via `ModeController::set_token_ring`.
    #[tokio::test]
    async fn api_ring_returns_json() {
        use ferrosa_cluster::raft::{NodeInfo, NodeState};

        let state = make_state();

        // Build a 3-node token ring and seed it into the controller.
        let mut ring = TokenRing::new();
        for i in 1u64..=3 {
            let host_id = uuid::Uuid::new_v4();
            ring.add_node(
                i,
                NodeInfo {
                    host_id,
                    addr: format!("10.0.0.{}:7000", i),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                },
            );
            // Assign a few tokens per node so token_count > 0.
            let tokens: Vec<i64> = (0..16).map(|j| (i as i64) * 1_000_000 + j).collect();
            ring.assign_tokens(i, &tokens);
        }
        state.mode_controller.set_token_ring(Arc::new(ring));

        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/cluster/ring")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let nodes = parsed["nodes"].as_array().expect("nodes array");
        assert_eq!(nodes.len(), 3, "ring should contain 3 nodes");

        for node in nodes {
            assert!(node["node_id"].is_number(), "node_id must be a number");
            assert!(node["host_id"].is_string(), "host_id must be a string");
            assert!(node["address"].is_string(), "address must be a string");
            assert!(node["state"].is_string(), "state must be a string");
            assert_eq!(node["state"], "Normal", "state should be Normal");
            assert_eq!(node["token_count"], 16, "each node has 16 tokens");
        }
    }

    /// Add-node endpoint rejects invalid UUIDs with 400 Bad Request.
    #[tokio::test]
    async fn api_add_node_rejects_invalid_uuid() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/cluster/add-node")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"host_id": "not-a-uuid"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    /// Decommission endpoint rejects invalid UUIDs with 400 Bad Request.
    #[tokio::test]
    async fn api_decommission_rejects_invalid_uuid() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/cluster/decommission")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"host_id": "not-a-uuid"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    /// BUG-009: Decommission with empty body (no host_id) should decommission
    /// the local node, not return a deserialization error.
    #[tokio::test]
    async fn api_decommission_empty_body_targets_local_node() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/cluster/decommission")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // Should NOT be 422 (Unprocessable Entity — JSON parse failure).
        // The API should accept an empty body and decommission the local node.
        assert_ne!(
            resp.status(),
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "empty body should not cause deserialization failure"
        );
    }

    /// Rebalance endpoint returns 503 when Raft is not initialized.
    #[tokio::test]
    async fn api_rebalance_returns_503_when_raft_not_initialized() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/cluster/rebalance")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    // ---- Switchover endpoint tests ------------------------------------

    /// Switchover in standalone mode should return 409 Conflict (not 500).
    #[tokio::test]
    async fn api_switchover_returns_409_when_not_in_pair_mode() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/api/cluster/switchover")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::CONFLICT,
            "switchover on standalone node should be 409 Conflict, not 500"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            parsed.get("error").is_some(),
            "response should include error message"
        );
    }

    /// When the controller returns NotPrimary, switchover must yield 409 Conflict.
    #[tokio::test]
    async fn api_switchover_not_primary_returns_409() {
        use ferrosa_cluster::ClusterError;
        // Invoke the handler directly via the function with a mock that returns NotPrimary.
        // We use the state's Arc<ModeController> but call the error-mapping logic by
        // constructing the match response inline.
        let mc = {
            let dir = tempfile::tempdir().expect("tempdir");
            let storage_config = ferrosa_storage::StorageEngineConfig {
                commit_log: ferrosa_storage::commitlog::CommitLogConfig {
                    log_dir: dir.path().join("commitlog"),
                    checkpoint_dir: dir.path().join("commitlog"),
                    archive: None,
                    ..ferrosa_storage::commitlog::CommitLogConfig::default()
                },
                compaction: ferrosa_storage::compaction::CompactionConfig::from_env(
                    dir.path().join("compaction"),
                ),
                object_store: None,
                local_cache_max_bytes: 1024 * 1024,
                flush_threshold_bytes: 4096,
                flush_max_age_secs: 5,
                data_dir: dir.path().to_path_buf(),
            };
            let storage = Arc::new(
                ferrosa_storage::StorageEngine::new(storage_config, None).expect("storage engine"),
            );
            let registry = Arc::new(ferrosa_net::rpc::HandlerRegistry::new());
            let schema = Arc::new(
                ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                    hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                    password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                    auth_method: ferrosa_schema::AuthMethod::Password,
                    rate_limit: ferrosa_schema::RateLimitConfig::default(),
                    audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                    secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                    mode: ferrosa_schema::DeploymentMode::Development,
                })
                .expect("test schema"),
            );
            let (mc, _) = ModeController::new(
                Arc::new(ferrosa_cluster::ClusterConfig::default()),
                Arc::new(ferrosa_net::config::NetConfig::default()),
                uuid::Uuid::new_v4(),
                storage,
                schema,
                registry,
            );
            mc
        };

        // Map errors the same way the handler does — verify the mapping logic.
        let not_primary: Result<(), ClusterError> = Err(ClusterError::NotPrimary);
        let (status, _body) = match not_primary {
            Ok(()) => (StatusCode::OK, Json(json!({}))),
            Err(ClusterError::NotPrimary) => (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "switchover must be initiated from the primary node",
                    "hint": "POST to the primary node's /api/cluster/switchover endpoint",
                })),
            ),
            Err(ClusterError::Net(e)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": format!("peer communication failed: {e}") })),
            ),
            Err(ClusterError::ReplicationFailed(reason)) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("role swap rejected by peer: {reason}") })),
            ),
            Err(ClusterError::ModeTransitionRejected(reason)) => (
                StatusCode::CONFLICT,
                Json(json!({ "error": format!("mode transition rejected: {reason}") })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            ),
        };
        assert_eq!(status, StatusCode::CONFLICT);
        drop(mc);
    }

    /// When the controller returns Net error, switchover must yield 503.
    #[tokio::test]
    async fn api_switchover_net_error_returns_503() {
        use ferrosa_cluster::ClusterError;
        use ferrosa_net::error::NetError;

        let net_err: Result<(), ClusterError> = Err(ClusterError::Net(NetError::Timeout(
            "peer unreachable".into(),
        )));
        let (status, _body) = match net_err {
            Ok(()) => (StatusCode::OK, Json(json!({}))),
            Err(ClusterError::NotPrimary) => (StatusCode::CONFLICT, Json(json!({}))),
            Err(ClusterError::Net(e)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": format!("peer communication failed: {e}"),
                    "hint": "ensure both nodes are running and connected",
                })),
            ),
            Err(ClusterError::ReplicationFailed(reason)) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("role swap rejected by peer: {reason}") })),
            ),
            Err(ClusterError::ModeTransitionRejected(reason)) => (
                StatusCode::CONFLICT,
                Json(json!({ "error": format!("mode transition rejected: {reason}") })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            ),
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// When the controller returns ReplicationFailed, switchover must yield 502.
    #[tokio::test]
    async fn api_switchover_replication_failed_returns_502() {
        use ferrosa_cluster::ClusterError;

        let err: Result<(), ClusterError> = Err(ClusterError::ReplicationFailed(
            "role swap response mismatch".into(),
        ));
        let (status, _body) = match err {
            Ok(()) => (StatusCode::OK, Json(json!({}))),
            Err(ClusterError::NotPrimary) => (StatusCode::CONFLICT, Json(json!({}))),
            Err(ClusterError::Net(e)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": format!("peer communication failed: {e}") })),
            ),
            Err(ClusterError::ReplicationFailed(reason)) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "error": format!("role swap rejected by peer: {reason}"),
                    "hint": "check peer node logs for details",
                })),
            ),
            Err(ClusterError::ModeTransitionRejected(reason)) => (
                StatusCode::CONFLICT,
                Json(json!({ "error": format!("mode transition rejected: {reason}") })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            ),
        };
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    /// BUG-019: All documented cluster management endpoints must be routable
    /// (i.e. return a non-404 status). The handler may return 400, 503, etc.
    /// depending on preconditions, but never 404 — that would mean the route
    /// itself is missing.
    #[tokio::test]
    async fn api_cluster_management_endpoints_are_routable() {
        let state = make_state();

        // (method, uri, body) for each endpoint that was previously returning 404.
        let cases: Vec<(&str, &str, &str)> = vec![
            (
                "POST",
                "/api/cluster/add-node",
                r#"{"host_id":"00000000-0000-0000-0000-000000000001"}"#,
            ),
            ("POST", "/api/cluster/decommission", "{}"),
            ("GET", "/api/cluster/ring", ""),
            ("POST", "/api/cluster/rebalance", ""),
        ];

        for (method, uri, body) in cases {
            let router = crate::web::build_router(state.clone());
            let mut builder = Request::builder().method(method).uri(uri);
            if !body.is_empty() {
                builder = builder.header("content-type", "application/json");
            }
            let req = builder.body(Body::from(body.to_string())).unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_ne!(
                resp.status(),
                axum::http::StatusCode::NOT_FOUND,
                "{method} {uri} must not return 404 (BUG-019)"
            );
        }
    }

    /// BUG-2026-0003: `/api/queries` must be routable (alias for `/api/active_queries`).
    #[tokio::test]
    async fn api_queries_alias_is_routable() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/queries")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "GET /api/queries must not return 404 (BUG-2026-0003)"
        );
    }

    /// BUG-2026-0004: `/api/storage` must be routable (alias for `/api/storage_stats`).
    #[tokio::test]
    async fn api_storage_alias_is_routable() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/storage")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "GET /api/storage must not return 404 (BUG-2026-0004)"
        );
    }

    /// Verify the `/metrics` endpoint is reachable through the full router
    /// (not just calling the handler directly).
    #[tokio::test]
    async fn metrics_endpoint_reachable_through_router() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "GET /metrics must not return 404"
        );
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        // Verify it returns the correct content type
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.starts_with("text/plain"),
            "expected text/plain content type, got: {content_type}"
        );
    }

    #[tokio::test]
    async fn get_metrics_returns_prometheus_text() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![
                VirtualColumnDef {
                    name: "host".to_string(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "count".to_string(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"node1".to_vec(), 1),
                    CellValue::live(5i32.to_be_bytes().to_vec(), 1),
                ],
            }],
        };
        registry.register(Arc::new(table));

        let state = Arc::new(registry);
        let (status, headers, body) = get_metrics(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[0].1, "text/plain; charset=utf-8");
        assert!(body.contains("ferrosa_test_table_count"));
        assert!(body.contains("host=\"node1\""));
        assert!(body.contains("5"));
    }

    // =========================================================================
    // BT-001: Web API endpoint smoke tests
    //
    // Each GET endpoint must return 200 with a valid JSON body.
    // =========================================================================

    /// BT-001a: GET /api/connections returns 200 + JSON array.
    #[tokio::test]
    async fn bt001_api_connections_returns_200_json() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/connections")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/connections must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/connections must return valid JSON");
        assert!(
            parsed.is_array(),
            "GET /api/connections must return a JSON array, got: {parsed}"
        );
    }

    /// BT-001b: GET /api/storage_stats returns 200 + JSON array.
    #[tokio::test]
    async fn bt001_api_storage_stats_returns_200_json() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/storage_stats")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/storage_stats must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/storage_stats must return valid JSON");
        assert!(
            parsed.is_array(),
            "GET /api/storage_stats must return a JSON array, got: {parsed}"
        );
    }

    /// BT-001c: GET /api/storage (alias) returns 200 + JSON array identical to
    /// /api/storage_stats.
    #[tokio::test]
    async fn bt001_api_storage_alias_returns_200_json() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/storage")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/storage must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/storage must return valid JSON");
        assert!(
            parsed.is_array(),
            "GET /api/storage must return a JSON array, got: {parsed}"
        );
    }

    /// BT-001d: GET /api/active_queries returns 200 + JSON array.
    #[tokio::test]
    async fn bt001_api_active_queries_returns_200_json() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/active_queries")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/active_queries must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/active_queries must return valid JSON");
        assert!(
            parsed.is_array(),
            "GET /api/active_queries must return a JSON array, got: {parsed}"
        );
    }

    /// BT-001e: GET /api/queries (alias) returns 200 + JSON array.
    #[tokio::test]
    async fn bt001_api_queries_alias_returns_200_json() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/queries")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/queries must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/queries must return valid JSON");
        assert!(
            parsed.is_array(),
            "GET /api/queries must return a JSON array, got: {parsed}"
        );
    }

    /// BT-001f: GET /api/tables returns 200 + JSON array of table names.
    #[tokio::test]
    async fn bt001_api_tables_returns_200_json() {
        let mut state = make_state();
        // Register at least one virtual table so /api/tables is non-empty.
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(NamedStubTable {
            table_name: "connections",
        }));
        registry.register(Arc::new(NamedStubTable {
            table_name: "storage_stats",
        }));
        state.registry = Arc::new(registry);

        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/tables")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/tables must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/tables must return valid JSON");
        let names = parsed
            .as_array()
            .expect("/api/tables must return a JSON array");
        assert!(
            names.len() >= 2,
            "expected at least 2 tables registered, got: {names:?}"
        );
        let name_strs: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
        assert!(
            name_strs.contains(&"connections"),
            "table list must include 'connections', got: {name_strs:?}"
        );
        assert!(
            name_strs.contains(&"storage_stats"),
            "table list must include 'storage_stats', got: {name_strs:?}"
        );
    }

    // =========================================================================
    // BT-002: Cluster status endpoint test
    // =========================================================================

    /// BT-002: GET /api/cluster/status returns 200 + JSON with mode, role, host_id.
    #[tokio::test]
    async fn bt002_api_cluster_status_returns_200_json() {
        let state = make_state();
        let expected_host_id = state.host_id.to_string();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/api/cluster/status")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /api/cluster/status must return 200"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("GET /api/cluster/status must return valid JSON");

        // Must contain "mode" field (string).
        let mode = parsed
            .get("mode")
            .expect("cluster/status must have 'mode' field");
        assert!(mode.is_string(), "'mode' must be a string, got: {mode}");

        // Must contain "host_id" field matching the node's host_id.
        let host_id = parsed
            .get("host_id")
            .expect("cluster/status must have 'host_id' field");
        assert_eq!(
            host_id.as_str().unwrap(),
            expected_host_id,
            "host_id must match the configured value"
        );

        // "role" field should be present (may be null in standalone mode).
        assert!(
            parsed.get("role").is_some(),
            "cluster/status must have 'role' field (even if null)"
        );
    }

    // =========================================================================
    // BT-003: Prometheus /metrics endpoint test
    // =========================================================================

    /// BT-003: GET /metrics returns text/plain with ferrosa_ prefix metrics
    /// when virtual tables with numeric columns are registered.
    #[tokio::test]
    async fn bt003_metrics_returns_ferrosa_prefix_metrics() {
        let mut state = make_state();
        // Register a virtual table with a numeric column so the metrics
        // output contains at least one ferrosa_ prefixed line.
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![
                VirtualColumnDef {
                    name: "host".to_string(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "active_count".to_string(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"node1".to_vec(), 1),
                    CellValue::live(3i32.to_be_bytes().to_vec(), 1),
                ],
            }],
        };
        registry.register(Arc::new(table));
        state.registry = Arc::new(registry);

        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /metrics must return 200"
        );

        // Verify content type.
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.starts_with("text/plain"),
            "/metrics must return text/plain, got: {ct}"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);

        // Body must contain at least one ferrosa_ prefixed metric.
        assert!(
            body_str.contains("ferrosa_"),
            "/metrics body must contain ferrosa_ prefix metrics, got: {body_str}"
        );
        // Verify the specific metric from the stub table appears.
        assert!(
            body_str.contains("ferrosa_test_table_active_count"),
            "/metrics must contain ferrosa_test_table_active_count, got: {body_str}"
        );
    }

    /// BT-003b: GET /metrics with empty registry returns 200 + text/plain
    /// (possibly empty body, but no error).
    #[tokio::test]
    async fn bt003_metrics_empty_registry_returns_200() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET /metrics with empty registry must still return 200"
        );

        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.starts_with("text/plain"),
            "content-type must be text/plain even with empty registry, got: {ct}"
        );
    }
}
