//! Web observability JSON API endpoints (Batch 5).
//!
//! These endpoints return JSON data from virtual tables for the web console.
//! - `GET /api/observability/cql`   — CQL stats (T-19)
//! - `GET /api/observability/alerts` — Alert data (T-27)

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use ferrosa_schema::VirtualTableRegistry;
use serde_json::{json, Value};

use super::WebAppState;

pub fn routes() -> Router<WebAppState> {
    Router::new()
        .route("/observability/cql", get(get_cql_stats))
        .route("/observability/alerts", get(get_alerts))
        .route(
            "/observability/query_fingerprints",
            get(get_query_fingerprints),
        )
        .route("/observability/table_access", get(get_table_access))
        .route(
            "/observability/full_scan_reasons",
            get(get_full_scan_reasons),
        )
        .route("/observability/billing", get(get_billing))
        .route(
            "/observability/auth_warn_denials",
            get(get_auth_warn_denials),
        )
}

/// `GET /api/observability/cql` — returns CQL statistics from virtual tables.
async fn get_cql_stats(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let mut result = json!({});

    // Active queries
    if let Some(table) = registry.get("system_observability", "active_queries") {
        let rows = table.read(None);
        let queries: Vec<Value> = rows
            .iter()
            .map(|row| {
                json!({
                    "query_id": row.cells.first().and_then(|c| c.value.as_deref())
                        .map(|b| if b.len() == 8 { i64::from_be_bytes(b.try_into().unwrap_or([0;8])) } else { 0 }),
                    "client_address": row.cells.get(1).and_then(|c| c.value.as_deref())
                        .map(|b| String::from_utf8_lossy(b).to_string()),
                    "username": row.cells.get(2).and_then(|c| c.value.as_deref())
                        .map(|b| String::from_utf8_lossy(b).to_string()),
                    "query_text": row.cells.get(3).and_then(|c| c.value.as_deref())
                        .map(|b| String::from_utf8_lossy(b).to_string()),
                    "keyspace": row.cells.get(4).and_then(|c| c.value.as_deref())
                        .map(|b| String::from_utf8_lossy(b).to_string()),
                })
            })
            .collect();
        result["active_queries"] = json!(queries);
    }

    // Connections
    if let Some(table) = registry.get("system_observability", "connections") {
        result["connection_count"] = json!(table.read(None).len());
    }

    (StatusCode::OK, Json(result))
}

/// `GET /api/observability/alerts` — returns alert data.
async fn get_alerts(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let rows = registry
        .get("system_observability", "alerts")
        .map(|t| t.read(None))
        .unwrap_or_default();

    let alerts: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "name": row.cells.first().and_then(|c| c.value.as_deref())
                    .map(|b| String::from_utf8_lossy(b).to_string()),
                "severity": row.cells.get(1).and_then(|c| c.value.as_deref())
                    .map(|b| String::from_utf8_lossy(b).to_string()),
                "message": row.cells.get(2).and_then(|c| c.value.as_deref())
                    .map(|b| String::from_utf8_lossy(b).to_string()),
                "triggered_at": row.cells.get(3).and_then(|c| c.value.as_deref())
                    .map(|b| String::from_utf8_lossy(b).to_string()),
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({ "alerts": alerts })))
}

/// `GET /api/observability/query_fingerprints` — returns query fingerprint data.
async fn get_query_fingerprints(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let rows = registry
        .get("system_observability", "query_fingerprints")
        .map(|t| t.read(None))
        .unwrap_or_default();

    (StatusCode::OK, Json(json!({ "fingerprints": rows.len() })))
}

/// `GET /api/observability/table_access` — returns table access summary.
async fn get_table_access(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let rows = registry
        .get("system_observability", "table_access_summary")
        .map(|t| t.read(None))
        .unwrap_or_default();

    (StatusCode::OK, Json(json!({ "tables": rows.len() })))
}

/// `GET /api/observability/full_scan_reasons` — returns full scan reasons.
async fn get_full_scan_reasons(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let rows = registry
        .get("system_observability", "full_scan_reasons")
        .map(|t| t.read(None))
        .unwrap_or_default();

    (StatusCode::OK, Json(json!({ "full_scans": rows.len() })))
}

/// `GET /api/observability/billing` — returns billing meter data.
async fn get_billing(
    State(registry): State<Arc<VirtualTableRegistry>>,
) -> (StatusCode, Json<Value>) {
    let rows = registry
        .get("system_observability", "billing_meters")
        .map(|t| t.read(None))
        .unwrap_or_default();

    (StatusCode::OK, Json(json!({ "meters": rows.len() })))
}

/// `GET /api/observability/auth_warn_denials` — tally of would-be CQL
/// permission denials collected while auth is running in warn (soak) mode.
///
/// Shape:
/// ```json
/// {
///   "total": 123,
///   "by_role": {"ferrosa_user": 100, "app_reader": 23},
///   "by_resource": {"table agent_memory.typed_edges": 123}
/// }
/// ```
///
/// When `FERROSA_AUTH_WARN` is off (the steady state) or auth is
/// entirely disabled, this endpoint returns an all-zero payload — it
/// is always present so dashboards never 404; the counters only tick
/// while warn mode is active. See
/// `specs/decisions/design-cql-role-auth-rollout.md` Sprint D.
async fn get_auth_warn_denials() -> (StatusCode, Json<Value>) {
    let snap = ferrosa_cql::auth::warn_denial_stats();
    (
        StatusCode::OK,
        Json(json!({
            "total": snap.total,
            "by_role": snap.by_role,
            "by_resource": snap.by_resource,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use ferrosa_cluster::ModeController;
    use ferrosa_common::CellValue;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_schema::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
    };
    use ferrosa_storage::commitlog::CommitLogConfig;
    use ferrosa_storage::compaction::CompactionConfig;
    use ferrosa_storage::{StorageEngine, StorageEngineConfig};
    use tower::ServiceExt;

    use crate::web::{build_router, WebAppState};

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Named stub virtual table for registering with a specific table name.
    struct NamedStubTable {
        table_name: &'static str,
        rows: Vec<VirtualRow>,
    }

    impl NamedStubTable {
        fn empty(table_name: &'static str) -> Self {
            Self {
                table_name,
                rows: vec![],
            }
        }

        fn with_rows(table_name: &'static str, rows: Vec<VirtualRow>) -> Self {
            Self { table_name, rows }
        }
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
            self.rows.clone()
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    /// Build a minimal `WebAppState` with a given virtual table registry.
    fn make_state_with_registry(registry: Arc<VirtualTableRegistry>) -> WebAppState {
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
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        let storage = Arc::new(StorageEngine::new(storage_config, None).expect("storage engine"));
        let rpc_registry = Arc::new(HandlerRegistry::new());
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
            rpc_registry,
        );
        WebAppState {
            registry,
            mode_controller: mc,
            schema,
            storage,
            host_id,
            auth_disabled: true,
            debug: None,
        }
    }

    fn make_state() -> WebAppState {
        make_state_with_registry(Arc::new(VirtualTableRegistry::new()))
    }

    // -----------------------------------------------------------------------
    // Registry-level tests
    // -----------------------------------------------------------------------

    #[test]
    fn cql_stats_returns_empty_when_no_tables_registered() {
        let registry = Arc::new(VirtualTableRegistry::new());
        // Verify basic get behavior with empty registry (no tables).
        assert!(registry
            .get("system_observability", "active_queries")
            .is_none());
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/cql
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_cql_stats_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/cql")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // With no tables registered, the result should be an empty object.
        assert!(parsed.is_object(), "response should be a JSON object");
    }

    #[tokio::test]
    async fn get_cql_stats_includes_active_queries_when_registered() {
        let registry = Arc::new(VirtualTableRegistry::new());
        // Register active_queries with a row containing 5+ cells.
        let query_id_bytes = 42i64.to_be_bytes().to_vec();
        let row = VirtualRow {
            cells: vec![
                CellValue::live(query_id_bytes, 1),              // query_id
                CellValue::live(b"192.168.1.1".to_vec(), 1),     // client_address
                CellValue::live(b"admin".to_vec(), 1),           // username
                CellValue::live(b"SELECT * FROM t".to_vec(), 1), // query_text
                CellValue::live(b"my_keyspace".to_vec(), 1),     // keyspace
            ],
        };
        registry.register(Arc::new(NamedStubTable::with_rows(
            "active_queries",
            vec![row],
        )));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/cql")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let queries = parsed["active_queries"]
            .as_array()
            .expect("active_queries array");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["query_id"], 42);
        assert_eq!(queries[0]["client_address"], "192.168.1.1");
        assert_eq!(queries[0]["username"], "admin");
        assert_eq!(queries[0]["query_text"], "SELECT * FROM t");
        assert_eq!(queries[0]["keyspace"], "my_keyspace");
    }

    #[tokio::test]
    async fn get_cql_stats_includes_connection_count() {
        let registry = Arc::new(VirtualTableRegistry::new());
        // Register connections table with 3 rows.
        let rows = vec![
            VirtualRow { cells: vec![] },
            VirtualRow { cells: vec![] },
            VirtualRow { cells: vec![] },
        ];
        registry.register(Arc::new(NamedStubTable::with_rows("connections", rows)));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/cql")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["connection_count"], 3);
    }

    #[tokio::test]
    async fn get_cql_stats_endpoint_is_routable() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/cql")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GET /api/observability/cql must not return 404"
        );
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/alerts
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_alerts_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/alerts")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alerts = parsed["alerts"].as_array().expect("alerts array");
        assert!(
            alerts.is_empty(),
            "alerts should be empty with no registry data"
        );
    }

    #[tokio::test]
    async fn get_alerts_includes_alert_data_when_registered() {
        let registry = Arc::new(VirtualTableRegistry::new());
        let row = VirtualRow {
            cells: vec![
                CellValue::live(b"HighLatency".to_vec(), 1), // name
                CellValue::live(b"warning".to_vec(), 1),     // severity
                CellValue::live(b"p99 > 500ms".to_vec(), 1), // message
                CellValue::live(b"2026-04-03T10:00:00Z".to_vec(), 1), // triggered_at
            ],
        };
        registry.register(Arc::new(NamedStubTable::with_rows("alerts", vec![row])));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/alerts")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alerts = parsed["alerts"].as_array().expect("alerts array");
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0]["name"], "HighLatency");
        assert_eq!(alerts[0]["severity"], "warning");
        assert_eq!(alerts[0]["message"], "p99 > 500ms");
        assert_eq!(alerts[0]["triggered_at"], "2026-04-03T10:00:00Z");
    }

    #[tokio::test]
    async fn get_alerts_endpoint_is_routable() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/alerts")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GET /api/observability/alerts must not return 404"
        );
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/query_fingerprints
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_query_fingerprints_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/query_fingerprints")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["fingerprints"], 0);
    }

    #[tokio::test]
    async fn get_query_fingerprints_counts_rows() {
        let registry = Arc::new(VirtualTableRegistry::new());
        let rows = vec![VirtualRow { cells: vec![] }, VirtualRow { cells: vec![] }];
        registry.register(Arc::new(NamedStubTable::with_rows(
            "query_fingerprints",
            rows,
        )));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/query_fingerprints")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["fingerprints"], 2);
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/table_access
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_table_access_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/table_access")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["tables"], 0);
    }

    #[tokio::test]
    async fn get_table_access_counts_rows() {
        let registry = Arc::new(VirtualTableRegistry::new());
        let rows = vec![VirtualRow { cells: vec![] }];
        registry.register(Arc::new(NamedStubTable::with_rows(
            "table_access_summary",
            rows,
        )));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/table_access")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["tables"], 1);
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/full_scan_reasons
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_full_scan_reasons_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/full_scan_reasons")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["full_scans"], 0);
    }

    #[tokio::test]
    async fn get_full_scan_reasons_counts_rows() {
        let registry = Arc::new(VirtualTableRegistry::new());
        let rows = vec![
            VirtualRow { cells: vec![] },
            VirtualRow { cells: vec![] },
            VirtualRow { cells: vec![] },
            VirtualRow { cells: vec![] },
        ];
        registry.register(Arc::new(NamedStubTable::with_rows(
            "full_scan_reasons",
            rows,
        )));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/full_scan_reasons")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["full_scans"], 4);
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/observability/billing
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_billing_returns_200_with_empty_registry() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/billing")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["meters"], 0);
    }

    #[tokio::test]
    async fn get_billing_counts_rows() {
        let registry = Arc::new(VirtualTableRegistry::new());
        let rows = vec![VirtualRow { cells: vec![] }, VirtualRow { cells: vec![] }];
        registry.register(Arc::new(NamedStubTable::with_rows("billing_meters", rows)));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/billing")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["meters"], 2);
    }

    // -----------------------------------------------------------------------
    // All observability endpoints are routable (regression gate)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn all_observability_endpoints_are_routable() {
        let endpoints = vec![
            "/api/observability/cql",
            "/api/observability/alerts",
            "/api/observability/query_fingerprints",
            "/api/observability/table_access",
            "/api/observability/full_scan_reasons",
            "/api/observability/billing",
        ];

        for uri in endpoints {
            let state = make_state();
            let router = build_router(state);
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_ne!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "GET {uri} must not return 404"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Response format validation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn all_observability_endpoints_return_valid_json() {
        let endpoints = vec![
            "/api/observability/cql",
            "/api/observability/alerts",
            "/api/observability/query_fingerprints",
            "/api/observability/table_access",
            "/api/observability/full_scan_reasons",
            "/api/observability/billing",
        ];

        for uri in endpoints {
            let state = make_state();
            let router = build_router(state);
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let resp = router.oneshot(req).await.unwrap();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&body);
            assert!(
                parsed.is_ok(),
                "GET {uri} must return valid JSON, got: {}",
                String::from_utf8_lossy(&body)
            );
        }
    }

    #[tokio::test]
    async fn get_cql_stats_with_active_queries_no_connections_omits_connection_count() {
        let registry = Arc::new(VirtualTableRegistry::new());
        // Register only active_queries, no connections table.
        registry.register(Arc::new(NamedStubTable::empty("active_queries")));

        let state = make_state_with_registry(registry);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/api/observability/cql")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // active_queries should be present (empty array), connection_count absent.
        assert!(parsed["active_queries"].is_array());
        assert!(
            parsed.get("connection_count").is_none(),
            "connection_count should not be present when connections table is unregistered"
        );
    }
}
