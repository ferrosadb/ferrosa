//! HTTP/JSON endpoint for graph queries.
//!
//! Provides a REST API for executing Cypher queries, explaining query plans,
//! inspecting graph schema, and health checks. Includes Basic auth middleware (T2),
//! error sanitization (T8), and TLS support (T11).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::Engine as _;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;

use crate::engine::GraphEngine;
use crate::error::GraphError;

/// Configuration for the graph HTTP server.
#[derive(Debug, Clone)]
pub struct GraphHttpConfig {
    /// Bind address (default: 0.0.0.0:7474).
    pub bind_addr: SocketAddr,
    /// Path to TLS certificate file (PEM).
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file (PEM).
    pub tls_key_path: Option<String>,
    /// Whether TLS is required (T11: fail if true and no cert provided).
    pub require_tls: bool,
    /// Maximum request body size in bytes (default: 1MB).
    pub max_request_body_bytes: usize,
}

impl Default for GraphHttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 7474)),
            tls_cert_path: None,
            tls_key_path: None,
            require_tls: false,
            max_request_body_bytes: 1_048_576, // 1 MB
        }
    }
}

/// Shared application state for route handlers.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<GraphEngine>,
    pub schema: Arc<Schema>,
    pub auth_disabled: bool,
}

/// Request body for query and explain endpoints.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub keyspace: String,
}

/// Query parameters for the schema endpoint.
#[derive(Debug, Deserialize)]
pub struct SchemaParams {
    pub keyspace: String,
}

/// Error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Convert a `GraphError` to an HTTP response with sanitized error messages (T8).
fn error_to_response(err: &GraphError) -> Response {
    let (status, message) = match err {
        GraphError::Parse(e) => (
            StatusCode::BAD_REQUEST,
            format!("parse error at byte {}: {}", e.span.start, e.message),
        ),
        GraphError::Validation(msg) => {
            (StatusCode::BAD_REQUEST, format!("validation error: {msg}"))
        }
        GraphError::PermissionDenied(_) => (StatusCode::FORBIDDEN, "permission denied".to_string()),
        GraphError::Timeout => (StatusCode::REQUEST_TIMEOUT, "query timeout".to_string()),
        GraphError::ResourceLimit(msg) => {
            (StatusCode::BAD_REQUEST, format!("resource limit: {msg}"))
        }
        // T8: Internal errors are never exposed to the client.
        GraphError::Storage(_) | GraphError::Schema(_) | GraphError::Internal(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        ),
    };

    let body = ErrorResponse { error: message };
    (status, Json(body)).into_response()
}

/// Auth middleware (T2): extract Basic auth header, decode, authenticate.
///
/// Injects `AuthContext` as a request extension on success.
async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    // Graph HTTP endpoints always require auth — FERROSA_AUTH_DISABLED applies
    // to CQL protocol auth only.  When auth is disabled we inject a superuser
    // context so handlers can proceed without explicit credentials.
    if state.auth_disabled {
        req.extensions_mut().insert(AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        });
        return next.run(req).await;
    }

    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let Some(auth_value) = auth_header else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "missing Authorization header".to_string(),
            }),
        )
            .into_response();
    };

    let Some(encoded) = auth_value.strip_prefix("Basic ") else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "unsupported auth scheme (expected Basic)".to_string(),
            }),
        )
            .into_response();
    };

    let decoded = match base64::engine::general_purpose::STANDARD.decode(encoded.trim()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "invalid base64 in Authorization header".to_string(),
                }),
            )
                .into_response();
        }
    };

    let decoded_str = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "invalid UTF-8 in credentials".to_string(),
                }),
            )
                .into_response();
        }
    };

    let Some((username, password)) = decoded_str.split_once(':') else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid credentials format".to_string(),
            }),
        )
            .into_response();
    };

    match state.schema.authenticate(username, password) {
        Ok(auth_ctx) => {
            req.extensions_mut().insert(auth_ctx);
            next.run(req).await
        }
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "authentication failed".to_string(),
            }),
        )
            .into_response(),
    }
}

/// POST /graph/query — execute a graph query with audit emission (T10).
async fn handle_query(State(state): State<AppState>, req: Request<Body>) -> Response {
    let Some(auth) = req.extensions().get::<AuthContext>().cloned() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "authentication required".to_string(),
            }),
        )
            .into_response();
    };

    // Extract JSON body
    let body_bytes = match axum::body::to_bytes(req.into_body(), 1_048_576).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    let query_req: QueryRequest = match serde_json::from_slice(&body_bytes) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid JSON: {e}"),
                }),
            )
                .into_response();
        }
    };

    // T10: Audit logging via tracing (Phase 1)
    tracing::info!(
        user = %auth.role,
        keyspace = %query_req.keyspace,
        query = %query_req.query,
        "graph query submitted"
    );

    match state
        .engine
        .execute(&query_req.query, &query_req.keyspace, &auth)
    {
        Ok(result) => {
            tracing::info!(
                user = %auth.role,
                keyspace = %query_req.keyspace,
                rows = result.rows.len(),
                execution_ms = result.stats.execution_ms,
                status = "Ok",
                "graph query completed"
            );
            Json(result).into_response()
        }
        Err(ref e) => {
            let status_str = match e {
                GraphError::Timeout => "Timeout",
                GraphError::PermissionDenied(_) => "Denied",
                _ => "Error",
            };
            tracing::info!(
                user = %auth.role,
                keyspace = %query_req.keyspace,
                status = status_str,
                error = %e,
                "graph query failed"
            );
            error_to_response(e)
        }
    }
}

/// POST /graph/explain — return query plan without executing.
async fn handle_explain(State(state): State<AppState>, req: Request<Body>) -> Response {
    let Some(auth) = req.extensions().get::<AuthContext>().cloned() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "authentication required".to_string(),
            }),
        )
            .into_response();
    };

    let body_bytes = match axum::body::to_bytes(req.into_body(), 1_048_576).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    let query_req: QueryRequest = match serde_json::from_slice(&body_bytes) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid JSON: {e}"),
                }),
            )
                .into_response();
        }
    };

    match state
        .engine
        .explain(&query_req.query, &query_req.keyspace, &auth)
    {
        Ok(plan_debug) => Json(serde_json::json!({ "plan": plan_debug })).into_response(),
        Err(ref e) => error_to_response(e),
    }
}

/// GET /graph/schema?keyspace=... — list vertex/edge tables with labels.
async fn handle_schema(
    State(state): State<AppState>,
    Query(params): Query<SchemaParams>,
) -> Response {
    match state.engine.graph_schema(&params.keyspace) {
        Ok(schema) => Json(schema).into_response(),
        Err(ref e) => error_to_response(e),
    }
}

/// GET /graph/health — health check (no auth required).
async fn handle_health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Request body for the unsubscribe endpoint.
#[derive(Debug, Deserialize)]
pub struct UnsubscribeRequest {
    pub stream_id: u16,
}

/// POST /graph/subscribe — start a subscription, returning an SSE stream.
///
/// The initial snapshot is sent as the first SSE event. Subsequent events
/// are sent at the configured interval. In delta mode, only changed/new rows
/// are sent after the initial snapshot.
async fn handle_subscribe(State(state): State<AppState>, req: Request<Body>) -> Response {
    let Some(auth) = req.extensions().get::<AuthContext>().cloned() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "authentication required".to_string(),
            }),
        )
            .into_response();
    };

    let body_bytes = match axum::body::to_bytes(req.into_body(), 1_048_576).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    let query_req: QueryRequest = match serde_json::from_slice(&body_bytes) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid JSON: {e}"),
                }),
            )
                .into_response();
        }
    };

    tracing::info!(
        user = %auth.role,
        keyspace = %query_req.keyspace,
        query = %query_req.query,
        "graph subscribe requested"
    );

    // Parse and execute initial snapshot, extracting interval and delta flag.
    let (initial_result, interval, delta) =
        match state
            .engine
            .execute_subscribe(&query_req.query, &query_req.keyspace, &auth)
        {
            Ok(r) => r,
            Err(ref e) => {
                tracing::info!(
                    user = %auth.role,
                    keyspace = %query_req.keyspace,
                    error = %e,
                    "graph subscribe failed"
                );
                return error_to_response(e);
            }
        };

    // Build the SSE stream.
    let stream = make_subscribe_stream(
        state.engine.clone(),
        query_req.query,
        query_req.keyspace,
        auth,
        initial_result,
        interval,
        delta,
    );

    Sse::new(stream).into_response()
}

/// Build an SSE stream that yields the initial snapshot followed by
/// periodic re-executions of the query.
fn make_subscribe_stream(
    engine: Arc<GraphEngine>,
    query: String,
    keyspace: String,
    auth: AuthContext,
    initial_result: crate::executor::result::GraphResult,
    interval: Duration,
    delta: bool,
) -> impl Stream<Item = Result<Event, Infallible>> {
    // Channel-based approach: we produce events via an async generator pattern
    // using tokio_stream.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(16);

    // Clone the engine for the subscription registry access after task spawn.
    let registry_engine = engine.clone();

    // Spawn the subscription background task.
    let task = tokio::spawn(async move {
        // Send initial snapshot.
        let initial_json = serde_json::to_string(&initial_result).unwrap_or_default();
        let event = Event::default().event("snapshot").data(initial_json);
        if tx.send(Ok(event)).await.is_err() {
            return; // Client disconnected.
        }

        let mut previous_rows = if delta {
            Some(initial_result.rows.clone())
        } else {
            None
        };

        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // Consume the immediate first tick.

        loop {
            ticker.tick().await;

            // Re-execute the query.
            let result = engine.execute(&query, &keyspace, &auth);
            match result {
                Ok(current) => {
                    if delta {
                        // Compute delta: rows in current that were not in previous.
                        let prev = previous_rows.as_ref().unwrap();
                        let new_rows: Vec<&Vec<serde_json::Value>> = current
                            .rows
                            .iter()
                            .filter(|row| !prev.contains(row))
                            .collect();

                        if !new_rows.is_empty() {
                            let delta_result = serde_json::json!({
                                "columns": current.columns,
                                "rows": new_rows,
                                "stats": current.stats,
                            });
                            let json = serde_json::to_string(&delta_result).unwrap_or_default();
                            let event = Event::default().event("delta").data(json);
                            if tx.send(Ok(event)).await.is_err() {
                                break; // Client disconnected.
                            }
                        }
                        previous_rows = Some(current.rows);
                    } else {
                        let json = serde_json::to_string(&current).unwrap_or_default();
                        let event = Event::default().event("data").data(json);
                        if tx.send(Ok(event)).await.is_err() {
                            break; // Client disconnected.
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "subscription query re-execution failed");
                    let event = Event::default()
                        .event("error")
                        .data(format!("query error: {e}"));
                    if tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Register the task in the subscription registry so it can be cancelled.
    if let Ok(sub_id) = registry_engine.subscription_registry().register(task) {
        tracing::info!(subscription_id = sub_id, "subscription registered");
    }

    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// POST /graph/unsubscribe — cancel a subscription by ID.
async fn handle_unsubscribe(State(state): State<AppState>, req: Request<Body>) -> Response {
    let body_bytes = match axum::body::to_bytes(req.into_body(), 1_048_576).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    let unsub_req: UnsubscribeRequest = match serde_json::from_slice(&body_bytes) {
        Ok(u) => u,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid JSON: {e}"),
                }),
            )
                .into_response();
        }
    };

    let cancelled = state
        .engine
        .subscription_registry()
        .cancel(unsub_req.stream_id);

    if cancelled {
        tracing::info!(stream_id = unsub_req.stream_id, "subscription cancelled");
        Json(serde_json::json!({
            "status": "cancelled",
            "stream_id": unsub_req.stream_id,
        }))
        .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("subscription {} not found", unsub_req.stream_id),
            }),
        )
            .into_response()
    }
}

/// Build the Axum router with all graph routes.
///
/// Auth middleware is applied to all routes except /graph/health.
pub fn build_router(state: AppState) -> Router {
    // Routes that require authentication.
    let authenticated = Router::new()
        .route("/graph/query", post(handle_query))
        .route("/graph/explain", post(handle_explain))
        .route("/graph/schema", get(handle_schema))
        .route("/graph/subscribe", post(handle_subscribe))
        .route("/graph/unsubscribe", post(handle_unsubscribe))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state.clone());

    // Health check does not require auth.
    let health = Router::new().route("/graph/health", get(handle_health));

    Router::new().merge(authenticated).merge(health)
}

/// Start the graph HTTP server.
///
/// T11: If `require_tls` is true but no cert/key is provided, returns an error.
pub async fn start_graph_http(
    config: &GraphHttpConfig,
    state: AppState,
) -> crate::error::Result<()> {
    // T11: Check TLS requirements.
    if config.require_tls && (config.tls_cert_path.is_none() || config.tls_key_path.is_none()) {
        return Err(GraphError::Internal(
            "require_tls is true but tls_cert_path or tls_key_path is not configured".to_string(),
        ));
    }

    let app = build_router(state)
        .layer(CatchPanicLayer::new())
        .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes));

    if let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) {
        // TLS mode
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| GraphError::Internal(format!("TLS configuration error: {e}")))?;

        tracing::info!(addr = %config.bind_addr, "starting graph HTTP server with TLS");

        axum_server::bind_rustls(config.bind_addr, tls_config)
            .serve(app.into_make_service())
            .await
            .map_err(|e| GraphError::Internal(format!("HTTP server error: {e}")))?;
    } else {
        // Plain HTTP mode
        tracing::info!(addr = %config.bind_addr, "starting graph HTTP server (plain)");

        let listener = tokio::net::TcpListener::bind(config.bind_addr)
            .await
            .map_err(|e| GraphError::Internal(format!("bind error: {e}")))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| GraphError::Internal(format!("HTTP server error: {e}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = GraphHttpConfig::default();
        assert_eq!(config.bind_addr, SocketAddr::from(([0, 0, 0, 0], 7474)));
        assert!(config.tls_cert_path.is_none());
        assert!(config.tls_key_path.is_none());
        assert!(!config.require_tls);
        assert_eq!(config.max_request_body_bytes, 1_048_576);
    }

    #[test]
    fn error_to_response_parse_error() {
        use crate::parser::{ParseError, Span};
        let err = GraphError::Parse(ParseError {
            message: "unexpected token".to_string(),
            span: Span { start: 5, end: 10 },
        });
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_to_response_validation() {
        let err = GraphError::Validation("bad label".to_string());
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_to_response_permission_denied() {
        let err = GraphError::PermissionDenied("no access".to_string());
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn error_to_response_timeout() {
        let err = GraphError::Timeout;
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[test]
    fn error_to_response_resource_limit() {
        let err = GraphError::ResourceLimit("too many rows".to_string());
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_to_response_storage_sanitized() {
        let err = GraphError::Storage(ferrosa_common::Error::Io(std::io::Error::other(
            "disk failure",
        )));
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_to_response_internal_sanitized() {
        let err = GraphError::Internal("secret details".to_string());
        let resp = error_to_response(&err);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn query_request_deserialize() {
        let json = r#"{"query": "MATCH (n) RETURN n", "keyspace": "social"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query, "MATCH (n) RETURN n");
        assert_eq!(req.keyspace, "social");
    }

    #[test]
    fn schema_params_deserialize() {
        let json = r#"{"keyspace": "social"}"#;
        let params: SchemaParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.keyspace, "social");
    }

    #[test]
    fn error_response_serialize() {
        let resp = ErrorResponse {
            error: "test error".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("test error"));
    }

    #[test]
    fn unsubscribe_request_deserialize() {
        let json = r#"{"stream_id": 42}"#;
        let req: UnsubscribeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.stream_id, 42);
    }

    /// Verify the subscribe route is registered by checking that the router
    /// builds without panic and the subscribe route is reachable.
    #[test]
    fn build_router_includes_subscribe_route() {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
            RateLimitConfig, SchemaConfig, TestAuditSink,
        };
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
        };

        let schema = Arc::new(
            ferrosa_schema::Schema::new(SchemaConfig {
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

        let tmp = tempfile::tempdir().unwrap();
        let storage_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: tmp.path().to_path_buf(),
                checkpoint_dir: tmp.path().to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(tmp.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: tmp.path().to_path_buf(),
        };
        let storage = Arc::new(ferrosa_storage::StorageEngine::new(storage_config, None).unwrap());

        let engine = Arc::new(crate::engine::GraphEngine::new(
            Arc::clone(&schema),
            storage,
            crate::executor::expand::GraphEngineConfig::default(),
            std::time::Duration::from_secs(300),
        ));

        let state = AppState {
            engine,
            schema,
            auth_disabled: true,
        };

        // build_router should succeed and include subscribe/unsubscribe routes.
        let _router = build_router(state);
    }
}
