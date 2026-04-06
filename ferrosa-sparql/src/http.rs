//! SPARQL HTTP endpoint (W3C SPARQL Protocol).
//!
//! Implements the SPARQL 1.1 Protocol over HTTP:
//! - `POST /sparql` with `application/sparql-query` content type
//! - `GET /sparql?query=...` for URL-encoded queries
//! - `GET /sparql/health` for health checks

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Query as AxumQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use ferrosa_schema::Schema;

use crate::engine::SparqlEngine;

/// HTTP server configuration.
#[derive(Debug, Clone)]
pub struct SparqlHttpConfig {
    pub bind_addr: SocketAddr,
}

impl Default for SparqlHttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        }
    }
}

/// Shared state for the HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<SparqlEngine>,
    pub schema: Arc<Schema>,
    pub auth_disabled: bool,
}

/// Start the SPARQL HTTP server.
pub async fn start_sparql_http(config: &SparqlHttpConfig, state: AppState) -> std::io::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "SPARQL HTTP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Maximum query body size: 1 MiB. Prevents DoS via oversized requests (BUG-S17).
const MAX_QUERY_BODY: usize = 1024 * 1024;

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/sparql", post(handle_sparql_post))
        .route("/sparql", get(handle_sparql_get))
        .route("/sparql/update", post(handle_sparql_update))
        .route("/sparql/health", get(handle_health))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(
            MAX_QUERY_BODY,
        ))
        .with_state(state)
}

/// POST /sparql — execute a SPARQL query.
///
/// Accepts `application/sparql-query` (raw SPARQL text) or
/// `application/x-www-form-urlencoded` (query=... parameter).
async fn handle_sparql_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let query_str = match extract_query_from_post(&headers, &body) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let keyspace = headers
        .get("X-Keyspace")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("rdf");

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    execute_and_respond(&state, &query_str, keyspace, accept)
}

/// GET /sparql?query=... — execute a SPARQL query via URL parameter.
#[derive(Deserialize)]
struct SparqlGetParams {
    query: String,
    #[serde(default = "default_keyspace")]
    keyspace: String,
}

fn default_keyspace() -> String {
    "rdf".into()
}

async fn handle_sparql_get(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    AxumQuery(params): AxumQuery<SparqlGetParams>,
) -> Response {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    execute_and_respond(&state, &params.query, &params.keyspace, accept)
}

/// POST /sparql/update — execute a SPARQL UPDATE (INSERT DATA, DELETE DATA).
async fn handle_sparql_update(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let update_str = match extract_query_from_post(&headers, &body) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let keyspace = headers
        .get("X-Keyspace")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("rdf");

    match state.engine.execute_update(&update_str, keyspace) {
        Ok(result) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "triples_inserted": result.triples_inserted,
                "triples_deleted": result.triples_deleted,
            })),
        )
            .into_response(),
        Err(crate::error::SparqlError::Parse(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// GET /sparql/health — basic health check.
///
/// Returns service status. When auth is enabled, returns a minimal
/// response that does not reveal internal details.
async fn handle_health(State(state): State<AppState>) -> Response {
    if state.auth_disabled {
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ok", "service": "sparql"})),
        )
            .into_response()
    } else {
        // With auth enabled, return just the status without service details.
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
    }
}

/// Execute a SPARQL query and build the HTTP response.
///
/// Parses the `Accept` header to determine the response format. Supports:
/// - `text/turtle` -> Turtle serialization
/// - `application/n-triples` -> N-Triples serialization
/// - Default -> `application/sparql-results+json`
fn execute_and_respond(state: &AppState, query: &str, keyspace: &str, accept: &str) -> Response {
    let format = crate::results::ResultFormat::from_accept(accept);

    match state.engine.execute(query, keyspace) {
        Ok(result) => match result.serialize(format) {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, format.content_type())],
                bytes,
            )
                .into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("serialization error: {e}"),
            ),
        },
        Err(crate::error::SparqlError::Parse(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(crate::error::SparqlError::Plan(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn extract_query_from_post(headers: &axum::http::HeaderMap, body: &[u8]) -> Result<String, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/sparql-query") {
        String::from_utf8(body.to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))
    } else if content_type.contains("application/x-www-form-urlencoded") {
        // Parse query=... from form body.
        let params: Vec<(String, String)> =
            serde_urlencoded::from_bytes(body).map_err(|e| format!("form parse error: {e}"))?;
        params
            .into_iter()
            .find(|(k, _)| k == "query")
            .map(|(_, v)| v)
            .ok_or_else(|| "missing 'query' parameter".into())
    } else if content_type.contains("application/json") {
        // Accept JSON body with {"query": "..."} for convenience.
        let parsed: serde_json::Value =
            serde_json::from_slice(body).map_err(|e| format!("JSON parse error: {e}"))?;
        parsed["query"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "JSON body missing 'query' field".into())
    } else {
        // Default: treat body as raw SPARQL text.
        String::from_utf8(body.to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({"error": message}))).into_response()
}
