//! Node-local operator cancellation of currently registered compactions.
//! Correctness: validate scope before requesting cancellation; never claim completion.
//! Last revised: 2026-09-27
//! Last changed: Add authenticated POST /api/compaction/stop.

use axum::extract::{rejection::JsonRejection, DefaultBodyLimit, State};
use axum::http::{StatusCode, Uri};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use super::WebAppState;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopRequest {
    keyspace: Option<String>,
    table: Option<String>,
}

pub fn routes() -> Router<WebAppState> {
    Router::new()
        .route("/stop", post(stop))
        // Two identifiers plus JSON framing need no bulk request body.
        .layer(DefaultBodyLimit::max(4096))
}

async fn stop(
    State(state): State<WebAppState>,
    uri: Uri,
    body: Result<Json<StopRequest>, JsonRejection>,
) -> (StatusCode, Json<Value>) {
    if uri.query().is_some_and(|query| !query.is_empty()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "scope must be supplied in the JSON body, not query parameters"})),
        );
    }
    let request = match body {
        Ok(Json(request)) => request,
        Err(error) => {
            let status = if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return (status, Json(json!({"error": error.body_text()})));
        }
    };
    let table_id = match (&request.keyspace, &request.table) {
        (None, None) => None,
        (Some(keyspace), Some(table))
            if !keyspace.trim().is_empty() && !table.trim().is_empty() =>
        {
            Some(ferrosa_storage::TableId::new(keyspace, table))
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(
                    json!({"error": "supply both non-empty keyspace and table, or neither for all current tasks on this node"}),
                ),
            )
        }
    };
    match state.storage.request_compaction_stop(table_id.as_ref()) {
        Ok(report) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "status": "cancellation_requested",
                "node_id": state.host_id,
                "keyspace": request.keyspace,
                "table": request.table,
                "matched_tasks": report.matched_tasks,
                "already_cancelled_tasks": report.already_cancelled_tasks,
            })),
        ),
        Err(error) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": error.to_string()})),
        ),
    }
}
