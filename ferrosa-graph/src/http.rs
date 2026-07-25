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
use serde_json::Value;
use std::collections::HashMap;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;

use tokio_util::sync::CancellationToken;

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;

use crate::engine::GraphEngine;
use crate::error::GraphError;

/// Configuration for the graph HTTP server.
#[derive(Debug, Clone)]
pub struct GraphHttpConfig {
    /// Bind address (default: 127.0.0.1:7474).
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
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 7474)),
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
    #[serde(default)]
    pub params: HashMap<String, Value>,
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
        // URS-QEC-D02: a constraint violation is a client error; the message is
        // the safe Neo4j-style guidance, not internal detail, so expose it.
        GraphError::ConstraintViolation(msg) => (StatusCode::CONFLICT, msg.clone()),
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
/// Injects `AuthContext` as a request extension on success. When
/// `auth_disabled` is set (e.g. FERROSA_AUTH_DISABLED=true), the
/// middleware injects a default superuser context and skips
/// credential validation.
async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    // When auth is disabled, inject a default superuser context and skip
    // credential validation — same behaviour as the CQL protocol path.
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
        .execute_with_params(
            &query_req.query,
            &query_req.keyspace,
            &auth,
            &query_req.params,
        )
        .await
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
            stream_graph_result(result)
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
    let (initial_result, interval, delta) = match state
        .engine
        .execute_subscribe(&query_req.query, &query_req.keyspace, &auth)
        .await
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
/// Serialize a [`GraphResult`] into a STREAMING chunked response body instead of
/// `Json(result)`.
///
/// `Json` serializes the whole result into one contiguous buffer before writing
/// a byte — for a large result that is a second full copy of the data on top of
/// the rows themselves, at peak. This emits the identical JSON
/// (`{"columns":…,"rows":[…],"stats":…}`, same field order as the derive) one
/// row at a time, so the serialized form is never fully resident and the client
/// starts receiving data immediately (t_4ce82a3e inc 7).
///
/// SCOPE: this removes the serialization copy, not the row buffer — `result`
/// still owns every row. Ending server-side buffering needs `execute()` to
/// return a `RowStream`, which is tracked separately.
///
/// A row that fails to serialize aborts the body with an error rather than
/// silently emitting truncated JSON — a partial body that parses would be worse
/// than a broken connection.
fn stream_graph_result(result: crate::executor::GraphResult) -> Response {
    use futures::stream::StreamExt as _;

    let crate::executor::GraphResult {
        columns,
        rows,
        stats,
    } = result;

    let head = match serde_json::to_string(&columns) {
        Ok(cols) => format!("{{\"columns\":{cols},\"rows\":["),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("encode columns: {e}") })),
            )
                .into_response()
        }
    };
    let tail = match serde_json::to_string(&stats) {
        Ok(st) => format!("],\"stats\":{st}}}"),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("encode stats: {e}") })),
            )
                .into_response()
        }
    };

    let body_stream = futures::stream::once(async move { Ok::<_, std::io::Error>(head) })
        .chain(
            futures::stream::iter(rows.into_iter().enumerate()).map(|(i, row)| {
                let mut chunk = String::new();
                if i > 0 {
                    chunk.push(',');
                }
                match serde_json::to_string(&row) {
                    Ok(encoded) => {
                        chunk.push_str(&encoded);
                        Ok(chunk)
                    }
                    // Fail loud: abort the body rather than emit truncated-but-parsable JSON.
                    Err(e) => Err(std::io::Error::other(format!("encode row {i}: {e}"))),
                }
            }),
        )
        .chain(futures::stream::once(async move {
            Ok::<_, std::io::Error>(tail)
        }));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(body_stream))
        .unwrap_or_else(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("build response: {e}") })),
            )
                .into_response()
        })
}

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

    // Create a cancellation token so the registry can stop the task gracefully.
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();

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
            tokio::select! {
                _ = ticker.tick() => {
                    // Re-execute the query.
                    let result = engine.execute(&query, &keyspace, &auth).await;
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
                _ = task_cancel.cancelled() => {
                    tracing::debug!("subscription task shutting down");
                    break;
                }
            }
        }
    });

    // Register the task in the subscription registry so it can be cancelled.
    if let Ok(sub_id) = registry_engine
        .subscription_registry()
        .register(cancel, task)
    {
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
    // /graph/health is the canonical path; /health is an alias for convenience
    // (e.g. load-balancer probes that hit the root path prefix).
    let health = Router::new()
        .route("/graph/health", get(handle_health))
        .route("/health", get(handle_health));

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

/// The body returned by the disabled-engine stub for every request.
fn graph_disabled_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "graph engine disabled",
            "remediation": "the graph engine is not enabled on this node; set [graph] enabled = true \
                            (or FERROSA_GRAPH_ENABLED=true) and restart to enable graph queries",
        })),
    )
        .into_response()
}

/// Fallback handler: every path on the disabled stub returns the clear error.
async fn graph_disabled_handler() -> Response {
    graph_disabled_response()
}

/// Build the disabled-engine router — a clear `503 graph engine disabled` for
/// every route (t_2dd438d2). Without this the graph ports simply don't listen
/// when the engine is disabled, so clients (e.g. ferrosa-memory) get an opaque
/// connection-refused instead of an actionable error.
pub fn build_disabled_router() -> Router {
    Router::new().fallback(graph_disabled_handler)
}

/// Start a thin HTTP listener that responds to every request with a clear
/// "graph engine disabled" error + remediation, used when the graph engine is
/// not enabled. Mirrors [`start_graph_http`]'s plain-HTTP bind.
pub async fn start_graph_disabled_http(config: &GraphHttpConfig) -> crate::error::Result<()> {
    let app =
        build_disabled_router().layer(RequestBodyLimitLayer::new(config.max_request_body_bytes));
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|e| GraphError::Internal(format!("bind error: {e}")))?;
    tracing::info!(
        addr = %config.bind_addr,
        "graph engine disabled — serving disabled-engine responses on the graph HTTP port"
    );
    axum::serve(listener, app)
        .await
        .map_err(|e| GraphError::Internal(format!("HTTP server error: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The streamed body MUST be byte-identical to what `Json(result)` produced,
    /// or every existing client breaks on a change that was supposed to be pure
    /// plumbing. Field order, separators, and escaping all have to match serde's
    /// own output, so this asserts against serde directly rather than against a
    /// hand-written expected string that could encode the same mistake twice.
    #[tokio::test]
    async fn streamed_body_is_byte_identical_to_json_serialization() {
        let cases = vec![
            // Empty: the `rows:[]` separator logic has no element to lean on.
            vec![],
            vec![vec![serde_json::json!(1)]],
            vec![
                vec![serde_json::json!("a"), serde_json::json!(null)],
                // Quotes, backslashes, newlines, and non-BMP unicode must escape
                // exactly as serde escapes them — hand-rolled framing around
                // serde-encoded rows is where a mismatch would hide.
                vec![
                    serde_json::json!("he said \"hi\"\n\\ \u{1f600}"),
                    serde_json::json!(2.5),
                ],
            ],
        ];

        for rows in cases {
            let result = crate::executor::GraphResult {
                columns: vec!["x".to_string(), "y".to_string()],
                rows,
                stats: crate::executor::QueryStats::default(),
            };
            let expected = serde_json::to_string(&result).expect("serde encodes the result");

            let resp = stream_graph_result(result);
            assert_eq!(resp.status(), StatusCode::OK);
            let got = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .expect("streamed body collects");

            assert_eq!(
                String::from_utf8(got.to_vec()).expect("body is utf8"),
                expected,
                "streamed body diverged from Json(result)"
            );
        }
    }

    #[tokio::test]
    async fn disabled_router_returns_clear_error_for_all_routes() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        // t_2dd438d2: every path on the disabled stub must return a clear,
        // actionable error — not connection-refused, and not a misleading
        // missing-table error.
        let app = build_disabled_router();
        for path in [
            "/graph/query",
            "/graph/schema",
            "/graph/health",
            "/anything",
        ] {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{path} should report the engine disabled"
            );
            let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
                .await
                .unwrap();
            let s = String::from_utf8_lossy(&body);
            assert!(s.contains("graph engine disabled"), "{path}: {s}");
            assert!(
                s.contains("enabled = true"),
                "{path} must include remediation: {s}"
            );
        }
    }

    #[test]
    fn default_config() {
        let config = GraphHttpConfig::default();
        assert_eq!(config.bind_addr, SocketAddr::from(([127, 0, 0, 1], 7474)));
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
                batch: Default::default(),
                log_dir: tmp.path().to_path_buf(),
                checkpoint_dir: tmp.path().to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(tmp.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: tmp.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        let storage = Arc::new(ferrosa_storage::StorageEngine::new(storage_config, None).unwrap());

        let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(
            ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage)),
        ));
        let engine = Arc::new(crate::engine::GraphEngine::new(
            Arc::clone(&schema),
            storage,
            write_path,
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
