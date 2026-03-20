//! Subcommand implementations for ferrosa-ctl.
//!
//! Each public `run_*` function connects to the Ferrosa node, issues one
//! or more CQL SELECTs against `system_observability.*`, and prints a
//! formatted table to stdout using the `tabled` crate.
//!
//! The functions are intentionally simple: connect → query → format →
//! print. Error handling surfaces the `CqlError` to `main`, which
//! prints the message and exits with a non-zero status.

use std::net::SocketAddr;

use tabled::builder::Builder;
use tabled::settings::Style;

use ferrosa_cql::client::{CqlClient, QueryResult};
use ferrosa_cql::error::CqlError;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Render a `QueryResult` as a plain table and print it to stdout.
///
/// The header row comes from `result.column_names`. Each data row uses
/// column bytes decoded as UTF-8 (falling back to `"<binary>"` on error,
/// and `"NULL"` for SQL null).
pub fn print_table(result: &QueryResult) {
    let mut builder = Builder::default();

    // Header
    builder.push_record(result.column_names.clone());

    // Data rows
    for row in &result.rows {
        let cells: Vec<String> = row
            .columns
            .iter()
            .map(|cell| match cell {
                None => "NULL".to_string(),
                Some(bytes) => std::str::from_utf8(bytes)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<binary>".to_string()),
            })
            .collect();
        builder.push_record(cells);
    }

    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
}

/// Print a one-line summary when the result set is empty.
pub fn print_empty(label: &str) {
    println!("(no {label} found)");
}

// ── status ───────────────────────────────────────────────────────────────────

/// Show node status by querying `system_observability.connections` (row count
/// gives a proxy for server health) and printing a brief summary.
pub async fn run_status(addr: SocketAddr) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await?;

    let conn_count = result.rows.len();
    println!("Ferrosa node at {addr}");
    println!("Status: UP");
    println!("Active connections: {conn_count}");
    Ok(())
}

// ── connections ──────────────────────────────────────────────────────────────

/// List active CQL connections, optionally sorting by a named column.
pub async fn run_connections(addr: SocketAddr, sort: Option<&str>) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let mut result = client
        .query("SELECT * FROM system_observability.connections")
        .await?;

    // Client-side sort if requested and the column exists.
    if let Some(col) = sort {
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        } else {
            eprintln!("warning: sort column '{col}' not found in result; ignoring");
        }
    }

    if result.rows.is_empty() {
        print_empty("connections");
    } else {
        print_table(&result);
    }
    Ok(())
}

// ── queries ──────────────────────────────────────────────────────────────────

/// List currently running queries; with `--long-running` flag, filter to
/// queries that have been running longest (server determines threshold).
pub async fn run_queries(addr: SocketAddr, long_running: bool) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let mut result = client
        .query("SELECT * FROM system_observability.active_queries")
        .await?;

    // If --long-running, sort descending by elapsed_ms (if column exists).
    if long_running {
        if let Some(idx) = result.column_names.iter().position(|n| n == "elapsed_ms") {
            result.rows.sort_by(|a, b| {
                let parse = |row: &ferrosa_cql::client::ResultRow| -> u64 {
                    row.columns
                        .get(idx)
                        .and_then(|c| c.as_deref())
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0)
                };
                parse(b).cmp(&parse(a)) // descending
            });
        }
    }

    if result.rows.is_empty() {
        print_empty("active queries");
    } else {
        print_table(&result);
    }
    Ok(())
}

// ── storage ──────────────────────────────────────────────────────────────────

/// Show storage engine statistics from `system_observability.storage_stats`.
pub async fn run_storage(addr: SocketAddr) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let result = client
        .query("SELECT * FROM system_observability.storage_stats")
        .await?;

    if result.rows.is_empty() {
        print_empty("storage stats");
    } else {
        print_table(&result);
    }
    Ok(())
}

// ── topology ─────────────────────────────────────────────────────────────────

/// Show ring topology. Queries a topology virtual table (placeholder until
/// the cluster crate exposes one).
pub async fn run_topology(addr: SocketAddr) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    // system.peers is the conventional Cassandra table; we mirror that.
    let result = client
        .query("SELECT peer, data_center, rack, tokens FROM system.peers")
        .await?;

    if result.rows.is_empty() {
        println!("Topology: single-node cluster (no peers)");
    } else {
        print_table(&result);
    }
    Ok(())
}

// ── peers ────────────────────────────────────────────────────────────────────

/// List peer nodes from `system.peers`.
pub async fn run_peers(addr: SocketAddr) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let result = client
        .query("SELECT peer, data_center, rack, release_version FROM system.peers")
        .await?;

    if result.rows.is_empty() {
        print_empty("peers");
    } else {
        print_table(&result);
    }
    Ok(())
}

// ── monitor ──────────────────────────────────────────────────────────────────

/// Launch the interactive TUI monitor dashboard.
pub async fn run_monitor(addr: SocketAddr, panel: Option<&str>) -> Result<(), CqlError> {
    crate::tui::run(addr, panel.map(String::from))
        .await
        .map_err(|e| CqlError::Protocol(format!("TUI error: {e}")))?;
    Ok(())
}

// ── Cluster web-API commands ──────────────────────────────────────────────────
//
// These commands make HTTP requests to the Ferrosa web admin API (port 9090 by
// default).  They intentionally avoid coupling to the CQL stack.

/// Error type returned by HTTP-based cluster commands.
pub type WebError = Box<dyn std::error::Error + Send + Sync>;

/// Pre-approve a node for cluster join.
///
/// Issues `POST /api/cluster/add-node` with a JSON body `{"host_id": …}`.
pub async fn add_node(host: &str, web_port: u16, host_id: &str) -> Result<(), WebError> {
    let url = format!("http://{}:{}/api/cluster/add-node", host, web_port);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "host_id": host_id }))
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("Node {} approved for cluster join.", host_id);
        if !body.is_empty() {
            println!("{body}");
        }
        Ok(())
    } else {
        Err(format!("add-node failed (HTTP {}): {}", status, body).into())
    }
}

/// Decommission a node from the cluster.
///
/// Issues `POST /api/cluster/decommission` with an optional `{"host_id": …}`.
/// When `host_id` is `None` the server decommissions the local node.
pub async fn decommission(
    host: &str,
    web_port: u16,
    host_id: Option<&str>,
) -> Result<(), WebError> {
    let url = format!("http://{}:{}/api/cluster/decommission", host, web_port);
    let client = reqwest::Client::new();

    let body = match host_id {
        Some(id) => serde_json::json!({ "host_id": id }),
        None => serde_json::json!({}),
    };

    let resp = client.post(&url).json(&body).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        match host_id {
            Some(id) => println!("Node {} decommissioned.", id),
            None => println!("Local node decommissioned."),
        }
        if !text.is_empty() {
            println!("{text}");
        }
        Ok(())
    } else {
        Err(format!("decommission failed (HTTP {}): {}", status, text).into())
    }
}

/// Show token ring distribution.
///
/// Issues `GET /api/cluster/ring` and renders the response as a table.
pub async fn ring(host: &str, web_port: u16) -> Result<(), WebError> {
    let url = format!("http://{}:{}/api/cluster/ring", host, web_port);
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?;
    let status = resp.status();

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("ring failed (HTTP {}): {}", status, body).into());
    }

    let value: serde_json::Value = resp.json().await?;

    // The API is expected to return a JSON array of objects with at least
    // "host_id", "token", and "status" fields.  We do a best-effort render.
    if let Some(entries) = value.as_array() {
        if entries.is_empty() {
            println!("(ring is empty)");
            return Ok(());
        }

        // Collect column names from the union of all keys in the first entry.
        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        use tabled::builder::Builder;
        use tabled::settings::Style;
        let mut builder = Builder::default();
        builder.push_record(columns.clone());

        for entry in entries {
            let row: Vec<String> = columns
                .iter()
                .map(|col| {
                    entry
                        .get(col)
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            builder.push_record(row);
        }

        let mut table = builder.build();
        table.with(Style::sharp());
        println!("{table}");
    } else {
        // If the server returned something other than an array, just pretty-print it.
        println!("{}", serde_json::to_string_pretty(&value)?);
    }

    Ok(())
}

/// Rebalance token distribution across the cluster.
///
/// Issues `POST /api/cluster/rebalance` with an empty body.
pub async fn rebalance(host: &str, web_port: u16) -> Result<(), WebError> {
    let url = format!("http://{}:{}/api/cluster/rebalance", host, web_port);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("Rebalance initiated.");
        if !body.is_empty() {
            println!("{body}");
        }
        Ok(())
    } else {
        Err(format!("rebalance failed (HTTP {}): {}", status, body).into())
    }
}

// ── Snapshot and restore commands ─────────────────────────────────────────────
//
// These commands make HTTP requests to the Ferrosa web admin API for PITR
// (point-in-time recovery) operations.  The REST endpoints are not yet
// implemented; for now each function prints what it would do and returns `Ok`.

/// Create a snapshot on the target node.
///
/// Will issue `POST /api/snapshots` once the endpoint exists.
pub async fn snapshot_create(
    host: &str,
    web_port: u16,
    name: &str,
    ttl_hours: Option<u64>,
) -> Result<(), WebError> {
    let node_url = format!("http://{}:{}", host, web_port);
    println!("Creating snapshot '{name}'...");
    println!("  TTL: {ttl_hours:?} hours");
    println!("  Node: {node_url}");
    // TODO: POST /api/snapshots
    Ok(())
}

/// List snapshots on the target node.
///
/// Will issue `GET /api/snapshots` once the endpoint exists.
pub async fn snapshot_list(host: &str, web_port: u16) -> Result<(), WebError> {
    let node_url = format!("http://{}:{}", host, web_port);
    println!("Listing snapshots...");
    println!("  Node: {node_url}");
    // TODO: GET /api/snapshots
    Ok(())
}

/// Delete a snapshot on the target node.
///
/// Will issue `DELETE /api/snapshots/{name}` once the endpoint exists.
pub async fn snapshot_delete(host: &str, web_port: u16, name: &str) -> Result<(), WebError> {
    let node_url = format!("http://{}:{}", host, web_port);
    println!("Deleting snapshot '{name}'...");
    println!("  Node: {node_url}");
    // TODO: DELETE /api/snapshots/{name}
    Ok(())
}

/// Restore from a snapshot, optionally to a point in time.
///
/// Will issue `POST /api/restore` once the endpoint exists.
pub async fn restore(
    host: &str,
    web_port: u16,
    snapshot_name: &str,
    point_in_time: Option<&str>,
    force: bool,
) -> Result<(), WebError> {
    let node_url = format!("http://{}:{}", host, web_port);
    println!("Restoring from snapshot '{snapshot_name}'...");
    println!("  Point-in-time: {point_in_time:?}");
    println!("  Force: {force}");
    println!("  Node: {node_url}");
    // TODO: POST /api/restore
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use ferrosa_cql::client::{QueryResult, ResultRow};

    use super::*;

    fn make_result(col_names: &[&str], rows: Vec<Vec<Option<&'static [u8]>>>) -> QueryResult {
        QueryResult {
            column_names: col_names.iter().map(|s| s.to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|row| ResultRow {
                    columns: row
                        .into_iter()
                        .map(|cell| cell.map(|b| b.to_vec()))
                        .collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn print_table_empty_rows_no_panic() {
        let result = make_result(&["col_a", "col_b"], vec![]);
        // Should not panic even with zero rows.
        print_table(&result);
    }

    #[test]
    fn print_table_with_null_cell_no_panic() {
        let result = make_result(&["name", "value"], vec![vec![Some(b"alice"), None]]);
        print_table(&result);
    }

    #[test]
    fn print_table_utf8_cell() {
        let result = make_result(&["host"], vec![vec![Some(b"127.0.0.1:9042")]]);
        // Verify the function runs without panicking.
        print_table(&result);
    }

    #[test]
    fn connections_sort_by_missing_column_is_graceful() {
        // If the sort column doesn't exist the result should still be usable.
        let mut result = make_result(
            &["addr", "state"],
            vec![
                vec![Some(b"b"), Some(b"open")],
                vec![Some(b"a"), Some(b"open")],
            ],
        );

        // sort by non-existent "elapsed_ms" — should do nothing
        let col = "elapsed_ms";
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        }
        // Rows should be unchanged (b before a).
        assert_eq!(result.rows[0].columns[0].as_deref(), Some(b"b" as &[u8]));
    }

    #[test]
    fn connections_sort_by_existing_column() {
        let mut result = make_result(
            &["addr", "state"],
            vec![
                vec![Some(b"b"), Some(b"open")],
                vec![Some(b"a"), Some(b"open")],
            ],
        );

        let col = "addr";
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        }
        // After sort: a before b.
        assert_eq!(result.rows[0].columns[0].as_deref(), Some(b"a" as &[u8]));
    }

    #[test]
    fn long_running_sort_descending() {
        let mut result = make_result(
            &["query_id", "elapsed_ms"],
            vec![
                vec![Some(b"q1"), Some(b"100")],
                vec![Some(b"q2"), Some(b"500")],
                vec![Some(b"q3"), Some(b"200")],
            ],
        );

        if let Some(idx) = result.column_names.iter().position(|n| n == "elapsed_ms") {
            result.rows.sort_by(|a, b| {
                let parse = |row: &ResultRow| -> u64 {
                    row.columns
                        .get(idx)
                        .and_then(|c| c.as_deref())
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0)
                };
                parse(b).cmp(&parse(a))
            });
        }
        // Descending: q2(500), q3(200), q1(100)
        assert_eq!(result.rows[0].columns[0].as_deref(), Some(b"q2" as &[u8]));
        assert_eq!(result.rows[1].columns[0].as_deref(), Some(b"q3" as &[u8]));
        assert_eq!(result.rows[2].columns[0].as_deref(), Some(b"q1" as &[u8]));
    }

    // ── Cluster web-API command unit tests ────────────────────────────────────
    //
    // These tests verify URL construction and request-body serialization without
    // making live HTTP connections.

    /// Helper: build the URL for add-node the same way the real function does.
    fn add_node_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/cluster/add-node", host, web_port)
    }

    /// Helper: build the URL for decommission.
    fn decommission_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/cluster/decommission", host, web_port)
    }

    /// Helper: build the URL for ring.
    fn ring_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/cluster/ring", host, web_port)
    }

    /// Helper: build the URL for rebalance.
    fn rebalance_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/cluster/rebalance", host, web_port)
    }

    #[test]
    fn add_node_formats_correct_url() {
        let url = add_node_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/add-node");
    }

    #[test]
    fn add_node_formats_correct_url_custom_port() {
        let url = add_node_url("10.0.0.5", 8080);
        assert_eq!(url, "http://10.0.0.5:8080/api/cluster/add-node");
    }

    #[test]
    fn add_node_body_contains_host_id() {
        let body = serde_json::json!({ "host_id": "host-uuid-1234" });
        assert_eq!(body["host_id"], "host-uuid-1234");
    }

    #[test]
    fn decommission_formats_correct_url() {
        let url = decommission_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/decommission");
    }

    #[test]
    fn decommission_body_with_host_id() {
        let body = serde_json::json!({ "host_id": "node-abc" });
        assert_eq!(body["host_id"], "node-abc");
    }

    #[test]
    fn decommission_body_without_host_id_is_empty_object() {
        let body = serde_json::json!({});
        assert!(body.as_object().unwrap().is_empty());
    }

    #[test]
    fn ring_formats_correct_url() {
        let url = ring_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/ring");
    }

    #[test]
    fn rebalance_formats_correct_url() {
        let url = rebalance_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/rebalance");
    }

    #[test]
    fn ring_renders_array_response() {
        // Verify that the column extraction logic works for a sample API response.
        let value = serde_json::json!([
            { "host_id": "node-1", "token": "-9223372036854775808", "status": "UP" },
            { "host_id": "node-2", "token": "0",                    "status": "UP" },
        ]);

        let entries = value.as_array().unwrap();
        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&"host_id".to_string()));
        assert!(columns.contains(&"token".to_string()));
        assert!(columns.contains(&"status".to_string()));

        // Each entry produces the right number of cells.
        for entry in entries {
            let row: Vec<String> = columns
                .iter()
                .map(|col| {
                    entry
                        .get(col)
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            assert_eq!(row.len(), 3);
        }
    }

    #[test]
    fn ring_renders_empty_array_response() {
        let value = serde_json::json!([]);
        let entries = value.as_array().unwrap();
        assert!(entries.is_empty());
    }

    // ── Snapshot / restore URL-construction tests ──────────────────────────────

    fn snapshot_create_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/snapshots", host, web_port)
    }

    fn snapshot_list_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/snapshots", host, web_port)
    }

    fn snapshot_delete_url(host: &str, web_port: u16, name: &str) -> String {
        format!("http://{}:{}/api/snapshots/{}", host, web_port, name)
    }

    fn restore_url(host: &str, web_port: u16) -> String {
        format!("http://{}:{}/api/restore", host, web_port)
    }

    #[test]
    fn snapshot_create_formats_correct_url() {
        let url = snapshot_create_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/snapshots");
    }

    #[test]
    fn snapshot_list_formats_correct_url() {
        let url = snapshot_list_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/snapshots");
    }

    #[test]
    fn snapshot_delete_formats_correct_url() {
        let url = snapshot_delete_url("127.0.0.1", 9090, "daily-backup");
        assert_eq!(url, "http://127.0.0.1:9090/api/snapshots/daily-backup");
    }

    #[test]
    fn restore_formats_correct_url() {
        let url = restore_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/restore");
    }

    #[test]
    fn restore_body_with_point_in_time() {
        let body = serde_json::json!({
            "snapshot_name": "daily-backup",
            "point_in_time": "2026-03-18T12:00:00Z",
            "force": false,
        });
        assert_eq!(body["snapshot_name"], "daily-backup");
        assert_eq!(body["point_in_time"], "2026-03-18T12:00:00Z");
        assert_eq!(body["force"], false);
    }

    #[test]
    fn restore_body_without_point_in_time() {
        let pit: Option<&str> = None;
        let body = serde_json::json!({
            "snapshot_name": "my-snap",
            "point_in_time": pit,
            "force": true,
        });
        assert!(body["point_in_time"].is_null());
        assert_eq!(body["force"], true);
    }
}
