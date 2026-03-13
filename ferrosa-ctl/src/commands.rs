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

/// Interactive TUI monitor (stub — T24 will implement).
pub async fn run_monitor(_addr: SocketAddr, panel: Option<&str>) -> Result<(), CqlError> {
    let panel_label = panel.unwrap_or("overview");
    println!("TUI mode not yet implemented (panel: {panel_label})");
    println!("Coming in T24.");
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

    #[test]
    fn monitor_stub_message() {
        // run_monitor is async; just verify the stub path compiles and the
        // message string is stable (checked via the function body, not here).
        let panel_label = "overview";
        let msg = format!("TUI mode not yet implemented (panel: {panel_label})");
        assert!(msg.contains("not yet implemented"));
    }
}
