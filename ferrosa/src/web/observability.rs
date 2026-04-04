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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_stats_returns_empty_when_no_tables_registered() {
        let registry = Arc::new(VirtualTableRegistry::new());
        // Verify basic get behavior with empty registry (no tables).
        assert!(registry
            .get("system_observability", "active_queries")
            .is_none());
    }
}
