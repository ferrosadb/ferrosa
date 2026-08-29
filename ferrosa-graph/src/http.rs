//! Module: Serve authenticated bounded HTTP/JSON graph and durable CDC requests.
//! Correctness: Correct when authentication and authorization precede storage I/O,
//! request/page bounds are enforced, and storage failures return sanitized typed responses.
//! Last revised: 2026-08-29
//! Last changed: Added authenticated durable cursor-page admission and replay.
//!
//! Provides a REST API for executing Cypher queries, explaining query plans,
//! inspecting graph schema, and health checks. Includes Basic auth middleware (T2),
//! error sanitization (T8), and TLS support (T11).
//!
//! `POST /graph/query` consumes a `RowStream` end to end: the handler never
//! holds the result set. See `stream_graph_rows` (private) for the exact scope
//! of that claim — it bounds the response, not the query — and for what a
//! client observes when a query fails after N rows.

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
use ferrosa_schema::auth::{Permission, Resource};
use ferrosa_schema::Schema;
use ferrosa_storage::commitlog::cdc::CdcPageLimit;
use ferrosa_storage::commitlog::cdc::CdcReplayError;
use ferrosa_storage::{CommitLogPosition, TableId};

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

/// Initial durable CDC transport contract. Source-filtered graph projection is
/// layered on this authenticated, authorized, cursor-bounded transport.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableCdcPageRequest {
    keyspace: String,
    table: String,
    #[serde(default)]
    after: Option<String>,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct DurableCdcMutationEvent {
    event_id: String,
    source_cursor: String,
    /// Versioned Ferrosa mutation bytes. Graph source filtering replaces this
    /// transport envelope with projected records before Streamer integration.
    mutation: String,
}

#[derive(Debug, Serialize)]
struct DurableCdcPageResponse {
    events: Vec<DurableCdcMutationEvent>,
    high_water_cursor: String,
}

#[derive(Debug, Serialize)]
struct DurableCdcGapResponse {
    error: &'static str,
    resync_required: bool,
    requested_segment: u64,
    oldest_retained_segment: Option<u64>,
}

const DURABLE_CDC_CURSOR_VERSION: u8 = 1;
const DURABLE_CDC_CURSOR_ENCODED_BYTES: usize = 23;

fn decode_durable_cdc_cursor(value: &str) -> Result<CommitLogPosition, &'static str> {
    if value.len() != DURABLE_CDC_CURSOR_ENCODED_BYTES {
        return Err("invalid durable CDC cursor");
    }
    let mut decoded = [0_u8; 17];
    let written = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode_slice(value, &mut decoded)
        .map_err(|_| "invalid durable CDC cursor")?;
    if written != decoded.len() || decoded[0] != DURABLE_CDC_CURSOR_VERSION {
        return Err("invalid durable CDC cursor");
    }
    Ok(CommitLogPosition {
        segment_id: u64::from_be_bytes(
            decoded[1..9]
                .try_into()
                .map_err(|_| "invalid durable CDC cursor")?,
        ),
        offset: u64::from_be_bytes(
            decoded[9..17]
                .try_into()
                .map_err(|_| "invalid durable CDC cursor")?,
        ),
    })
}

fn encode_durable_cdc_cursor(position: CommitLogPosition) -> String {
    let mut bytes = [0_u8; 17];
    bytes[0] = DURABLE_CDC_CURSOR_VERSION;
    bytes[1..9].copy_from_slice(&position.segment_id.to_be_bytes());
    bytes[9..17].copy_from_slice(&position.offset.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
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
        .execute_stream_with_params(
            &query_req.query,
            &query_req.keyspace,
            &auth,
            &query_req.params,
        )
        .await
    {
        Ok((columns, rows, stats)) => {
            // The completion log moves to the end of the body: with a streamed
            // response the row count and the total duration are not known until
            // the last row is out. A client that disconnects mid-stream
            // therefore produces no completion line — the drain never finishes.
            let user = auth.role.clone();
            let keyspace = query_req.keyspace.clone();
            let drain_start = std::time::Instant::now();
            stream_graph_rows(
                columns,
                rows,
                Box::new(move |emitted| {
                    let mut stats = stats;
                    // The executor's measurement stops at setup; the streamed
                    // projection happens after it. Adding the drain makes
                    // `execution_ms` cover the whole query rather than its
                    // prefix — a MORE accurate number than the buffered path
                    // reported, not a differently-defined one.
                    stats.execution_ms = stats
                        .execution_ms
                        .saturating_add(drain_start.elapsed().as_millis() as u64);
                    tracing::info!(
                        user = %user,
                        keyspace = %keyspace,
                        rows = emitted,
                        execution_ms = stats.execution_ms,
                        status = "Ok",
                        "graph query completed"
                    );
                    stats
                }),
            )
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
/// Produces the response's trailing `"stats"` object once the row stream has
/// drained, given the number of rows actually emitted.
///
/// A closure rather than a value because `execution_ms` is not knowable until
/// the last row is out — the executor's own measurement stops at setup, and for
/// a genuinely streamed plan the projection happens afterwards. Capturing the
/// stats up front would report a duration that excludes most of the query.
type StatsFinalizer = Box<dyn FnOnce(usize) -> crate::executor::QueryStats + Send>;

/// Write a query result to the wire as a STREAMING chunked response body,
/// pulling rows from `rows` as the client consumes them.
///
/// Emits exactly the JSON `Json(GraphResult)` would
/// (`{"columns":…,"rows":[…],"stats":…}`, same field order as the derive), one
/// row at a time. Neither the rows nor their serialized form is ever fully
/// resident on the server, and the client starts receiving data immediately.
///
/// # Scope — this bounds the RESPONSE, not the query
///
/// Server-side buffering of the *result* is gone for every plan the executor
/// streams. It is NOT gone for the query as a whole: phase A of `Expand` still
/// materializes the frontier before the first row is projected, and the plans
/// that legitimately buffer (ORDER BY, aggregation, DELETE, `WcoJoin`,
/// `ExpandVarLength`) still hand back a fully-materialized stream. A
/// high-fan-out query can still exhaust memory in phase A.
///
/// # Failure after the first chunk
///
/// The status line is chosen before any byte is written, so a failure that
/// surfaces mid-stream — an executor error, or a row that fails to serialize —
/// can only ABORT the body. It does that rather than emit the closing
/// `],"stats":{…}}`, because truncated-but-parsable JSON would be read by the
/// client as a complete, shorter result. The client observes a 200 with an
/// incomplete body (a broken chunked transfer), not an error status.
fn stream_graph_rows(
    columns: Vec<String>,
    rows: crate::executor::stream::RowStream<'static>,
    finish: StatsFinalizer,
) -> Response {
    use futures::stream::StreamExt as _;

    // Built eagerly: a failure here still happens before the status is chosen.
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

    let mut rows = rows;
    let body_stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(head);

        let mut emitted = 0usize;
        while let Some(row) = rows.next().await {
            let row = match row {
                Ok(row) => row,
                // Fail loud: abort mid-body rather than close the JSON around a
                // short result the client cannot distinguish from a complete one.
                Err(e) => {
                    yield Err(std::io::Error::other(format!("row {emitted}: {e}")));
                    return;
                }
            };
            let mut chunk = String::new();
            if emitted > 0 {
                chunk.push(',');
            }
            match serde_json::to_string(&row) {
                Ok(encoded) => chunk.push_str(&encoded),
                Err(e) => {
                    yield Err(std::io::Error::other(format!("encode row {emitted}: {e}")));
                    return;
                }
            }
            emitted += 1;
            yield Ok(chunk);
        }

        // Only now are the stats final.
        let stats = finish(emitted);
        match serde_json::to_string(&stats) {
            Ok(st) => yield Ok(format!("],\"stats\":{st}}}")),
            Err(e) => yield Err(std::io::Error::other(format!("encode stats: {e}"))),
        }
    };

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

/// POST /graph/cdc/page — authenticated bounded durable replay.
///
/// The projection/filter implementation is installed by the durable graph CDC
/// source. Keeping the route behind the same middleware as graph queries makes
/// it impossible to open commit-log replay before authentication succeeds.
async fn handle_durable_cdc_page(State(state): State<AppState>, req: Request<Body>) -> Response {
    let Some(auth) = req.extensions().get::<AuthContext>().cloned() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "authentication required".to_string(),
            }),
        )
            .into_response();
    };

    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: "durable CDC request exceeds 65536 bytes".to_string(),
                }),
            )
                .into_response();
        }
    };
    let request: DurableCdcPageRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid durable CDC request".to_string(),
                }),
            )
                .into_response();
        }
    };
    let limit = match CdcPageLimit::new(request.limit) {
        Ok(limit) => limit,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            )
                .into_response();
        }
    };
    let resource = Resource::Table(request.keyspace.clone(), request.table.clone());
    if state
        .schema
        .check_permission(&auth, Permission::Select, &resource)
        .is_err()
    {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "permission denied".to_string(),
            }),
        )
            .into_response();
    }
    let position = match request.after.as_deref() {
        Some(cursor) => match decode_durable_cdc_cursor(cursor) {
            Ok(cursor) => cursor,
            Err(message) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: message.to_string(),
                    }),
                )
                    .into_response();
            }
        },
        None => CommitLogPosition {
            segment_id: 0,
            offset: 0,
        },
    };
    let storage = state.engine.storage();
    let table = TableId::new(&request.keyspace, &request.table);
    let page =
        match tokio::task::spawn_blocking(move || storage.durable_cdc_page(position, table, limit))
            .await
        {
            Ok(Ok(page)) => page,
            Ok(Err(CdcReplayError::CursorExpired {
                requested_segment,
                oldest_retained_segment,
            })) => {
                return (
                    StatusCode::CONFLICT,
                    Json(DurableCdcGapResponse {
                        error: "cursor_expired",
                        resync_required: true,
                        requested_segment,
                        oldest_retained_segment,
                    }),
                )
                    .into_response();
            }
            Ok(Err(CdcReplayError::InvalidCursor { .. })) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid durable CDC cursor".to_string(),
                    }),
                )
                    .into_response();
            }
            Ok(Err(CdcReplayError::InvalidPageLimit { .. })) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid durable CDC page limit".to_string(),
                    }),
                )
                    .into_response();
            }
            Ok(Err(CdcReplayError::EventTooLarge { .. })) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(ErrorResponse {
                        error: "durable CDC event exceeds page byte budget".to_string(),
                    }),
                )
                    .into_response();
            }
            Ok(Err(CdcReplayError::Storage(_))) | Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "durable CDC unavailable".to_string(),
                    }),
                )
                    .into_response();
            }
        };

    let high_water_cursor = encode_durable_cdc_cursor(page.high_water);
    let events = page
        .entries
        .into_iter()
        .map(|(mutation, position)| {
            let mut bytes = vec![0_u8; mutation.serialized_size()];
            mutation.serialize_into(&mut bytes);
            DurableCdcMutationEvent {
                event_id: base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(mutation.mutation_id),
                source_cursor: encode_durable_cdc_cursor(position),
                mutation: base64::engine::general_purpose::STANDARD.encode(bytes),
            }
        })
        .collect();
    Json(DurableCdcPageResponse {
        events,
        high_water_cursor,
    })
    .into_response()
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
        .route("/graph/cdc/page", post(handle_durable_cdc_page))
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

    use crate::executor::stream::{stream_from_rows, RowStream};

    /// A finalizer that ignores the row count and returns fixed stats, so the
    /// byte-identity assertion stays deterministic (the production finalizer
    /// stamps a wall-clock `execution_ms`).
    fn fixed_stats(stats: crate::executor::QueryStats) -> StatsFinalizer {
        Box::new(move |_rows| stats)
    }

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
            // The rows now arrive as a stream, so the expected value is built
            // from the same rows the stream will yield.
            let result = crate::executor::GraphResult {
                columns: vec!["x".to_string(), "y".to_string()],
                rows: rows.clone(),
                stats: crate::executor::QueryStats::default(),
            };
            let expected = serde_json::to_string(&result).expect("serde encodes the result");

            let resp = stream_graph_rows(
                result.columns.clone(),
                stream_from_rows(rows),
                fixed_stats(crate::executor::QueryStats::default()),
            );
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

    /// The trailing `"stats"` object is built AFTER the last row, not captured
    /// up front — `execution_ms` is not knowable until the stream drains.
    ///
    /// Asserted by having the finalizer observe a counter that the row stream
    /// itself increments: if `stats` were serialized eagerly the finalizer would
    /// see 0 rows, and the body would report 0.
    #[tokio::test]
    async fn stats_are_built_after_the_last_row_not_captured_up_front() {
        use futures::stream::StreamExt as _;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&pulled);
        let rows: RowStream<'static> = Box::pin(
            stream_from_rows(vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)],
            ])
            .inspect(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
        );

        let observed = Arc::clone(&pulled);
        let resp = stream_graph_rows(
            vec!["n".to_string()],
            rows,
            Box::new(move |row_count| crate::executor::QueryStats {
                // Two independent witnesses that the finalizer ran last: the
                // row count handed to it, and what the stream had produced.
                vertices_read: row_count,
                edges_read: observed.load(Ordering::SeqCst),
                ..Default::default()
            }),
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("streamed body collects");
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("body is well-formed JSON");
        assert_eq!(parsed["stats"]["vertices_read"], serde_json::json!(3));
        assert_eq!(parsed["stats"]["edges_read"], serde_json::json!(3));
    }

    /// The response is NOT buffered server-side: its first chunk is deliverable
    /// while the rows are still unproduced.
    ///
    /// This is the property the whole change exists for. The row stream here
    /// cannot yield until it is released, so a handler that collected rows
    /// before writing the body would deadlock at the first `next()` instead of
    /// handing back the head.
    #[tokio::test]
    async fn the_response_head_is_deliverable_before_any_row_exists() {
        use futures::stream::StreamExt as _;

        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let rows: RowStream<'static> = Box::pin(futures::stream::once(async move {
            released.await.expect("release signal");
            Ok(vec![serde_json::json!(1)])
        }));

        let resp = stream_graph_rows(
            vec!["n".to_string()],
            rows,
            fixed_stats(crate::executor::QueryStats::default()),
        );
        let mut chunks = resp.into_body().into_data_stream();

        let head = chunks
            .next()
            .await
            .expect("a head chunk before the rows")
            .expect("head chunk is not an error");
        assert_eq!(
            head.as_ref(),
            br#"{"columns":["n"],"rows":["#,
            "the head must be on the wire before the first row is produced"
        );

        // Only now let the row through.
        release.send(()).expect("body stream is still alive");
        let mut rest = Vec::new();
        while let Some(chunk) = chunks.next().await {
            rest.extend_from_slice(&chunk.expect("no mid-stream failure"));
        }
        assert_eq!(
            String::from_utf8(rest).expect("utf8"),
            format!(
                "[1]],\"stats\":{}}}",
                serde_json::to_string(&crate::executor::QueryStats::default()).unwrap()
            )
        );
    }

    /// An executor error that surfaces AFTER the first chunk is on the wire must
    /// ABORT the body. Emitting the closing `],"stats":{…}}` anyway would produce
    /// truncated-but-parsable JSON — a silently short result, which is strictly
    /// worse than a broken connection.
    ///
    /// The client-visible consequence is spelled out here because it is a real
    /// limitation, not an implementation detail: the status line (200) is
    /// already sent, so the failure can only appear as an aborted/incomplete
    /// response body, never as a 4xx/5xx.
    #[tokio::test]
    async fn mid_stream_error_aborts_the_body_instead_of_closing_the_json() {
        let rows: RowStream<'static> = Box::pin(futures::stream::iter(vec![
            Ok(vec![serde_json::json!(1)]),
            Err(crate::error::GraphError::Internal("boom".to_string())),
        ]));

        let resp = stream_graph_rows(
            vec!["n".to_string()],
            rows,
            fixed_stats(crate::executor::QueryStats::default()),
        );
        // The status was already chosen before any byte was written.
        assert_eq!(resp.status(), StatusCode::OK);

        let err = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect_err("a mid-stream failure must abort the body, not complete it");
        assert!(
            err.to_string().contains("boom"),
            "the abort must carry the underlying cause, got: {err}"
        );
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

    #[tokio::test]
    async fn durable_cdc_page_authenticates_before_opening_replay() {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
            RateLimitConfig, SchemaConfig, TestAuditSink,
        };
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
        };
        use tower::ServiceExt;

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
        let storage = Arc::new(
            ferrosa_storage::StorageEngine::new(
                StorageEngineConfig {
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
                    auth_enabled: true,
                    auth_warn: false,
                    max_pending_replay_mutations_without_schema: 1024,
                    memtable_num_shards: 64,
                },
                None,
            )
            .unwrap(),
        );
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
        let app = build_router(AppState {
            engine: Arc::clone(&engine),
            schema: Arc::clone(&schema),
            auth_disabled: false,
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graph/cdc/page")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let authenticated_app = build_router(AppState {
            engine: Arc::clone(&engine),
            schema: Arc::clone(&schema),
            auth_disabled: true,
        });
        let response = authenticated_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graph/cdc/page")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"keyspace":"graph","table":"edges","limit":0}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = authenticated_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graph/cdc/page")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"keyspace":"graph","table":"edges","after":"not-a-cursor","limit":1}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = authenticated_app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graph/cdc/page")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"keyspace":"graph","table":"edges","limit":1}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let mut denied_request = Request::builder()
            .method("POST")
            .uri("/graph/cdc/page")
            .body(Body::from(
                r#"{"keyspace":"graph","table":"edges","limit":1}"#,
            ))
            .unwrap();
        denied_request.extensions_mut().insert(AuthContext {
            role: "cdc_without_select".to_string(),
            is_superuser: false,
            must_change_password: false,
        });
        let response = handle_durable_cdc_page(
            State(AppState {
                engine,
                schema,
                auth_disabled: false,
            }),
            denied_request,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
