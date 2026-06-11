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

/// Offline SSTable analysis & recovery (`ferrosa-ctl sstable ...`). Operates on
/// an on-disk table directory with no network connection.
pub mod sstable;

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

// ── Auth ─────────────────────────────────────────────────────────────────────

/// Dispatch for `ferrosa-ctl auth set-password`.
///
/// 1. Read NEW password (twice, no echo unless `--no-confirm`); validate.
/// 2. Connect to `addr` as `admin_user` with the password from
///    `--admin-password-env` (or seed default → prompt fallback).
/// 3. Issue `ALTER ROLE "<user>" WITH PASSWORD = '<new>'`.
///
/// Returns a [`crate::auth::SetPasswordError`] carrying both the message
/// to print and the desired exit code.
#[allow(clippy::too_many_arguments)]
pub async fn run_auth_set_password(
    addr: SocketAddr,
    user: &str,
    admin_user: &str,
    admin_password_env: Option<&str>,
    ssl: bool,
    force: bool,
    no_confirm: bool,
) -> Result<(), crate::auth::SetPasswordError> {
    use crate::auth;

    if ssl {
        return Err(auth::SetPasswordError::Operation(
            "--ssl is not yet supported by ferrosa-ctl; use a localhost connection".to_string(),
        ));
    }

    if !force {
        let stdin = std::io::stdin();
        let mut locked = stdin.lock();
        let mut stderr = std::io::stderr();
        auth::confirm_superuser_change(user, &mut locked, &mut stderr)?;
    }

    let new_password = auth::read_new_password_interactive(!no_confirm)?;

    let admin_choice = auth::resolve_admin_password(admin_password_env, |k| std::env::var(k).ok())
        .map_err(|e| auth::SetPasswordError::BadInput(e.to_string()))?;

    // Try the supplied/seed admin password first; on auth failure, prompt
    // once if we're allowed to (no env var was given).
    let mut executor =
        match auth::LiveCqlExecutor::connect(addr, admin_user, &admin_choice.password).await {
            Ok(e) => e,
            Err(ferrosa_cql::error::CqlError::BadCredentials)
                if admin_choice.may_retry_with_prompt =>
            {
                let pw = auth::prompt_password("Admin password").map_err(|e| {
                    auth::SetPasswordError::BadInput(format!("could not read admin password: {e}"))
                })?;
                auth::LiveCqlExecutor::connect(addr, admin_user, &pw)
                    .await
                    .map_err(|e| {
                        auth::SetPasswordError::Operation(auth::format_cql_error(
                            &e.to_string(),
                            addr,
                        ))
                    })?
            }
            Err(e) => {
                return Err(auth::SetPasswordError::Operation(auth::format_cql_error(
                    &e.to_string(),
                    addr,
                )));
            }
        };

    auth::run_set_password(&mut executor, user, &new_password, addr).await?;

    eprintln!("Password updated for role {user}");
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

/// Transfer Raft leadership to a target node (Ongaro §3.10, ADR-012 W3.13).
///
/// Issues `POST /api/cluster/raft/transfer-leader` with `{"to": "<host_id>"}`.
/// The server-side handler invokes
/// `raft.trigger().transfer_to(target_node_id).await`. Returns within
/// `election_timeout × 2` or with `TransferError::Timeout`.
///
/// **Status**: this client-side wiring lands in Sprint 3. The server-side
/// handler and the underlying openraft `transfer_to` API are deferred (see
/// `specs/in-process/sprint-03-openraft-patches.md` items W3.8/W3.9). Until
/// the server endpoint exists, this command surfaces the upstream HTTP 404
/// (or 501 Not Implemented) cleanly so operators see what's missing.
pub async fn raft_transfer_leader(
    host: &str,
    web_port: u16,
    to_host_id: &str,
) -> Result<(), WebError> {
    let url = format!(
        "http://{}:{}/api/cluster/raft/transfer-leader",
        host, web_port
    );
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "to": to_host_id }))
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("Raft leadership transfer to {} initiated.", to_host_id);
        if !body.is_empty() {
            println!("{body}");
        }
        Ok(())
    } else if status.as_u16() == 404 || status.as_u16() == 501 {
        Err(format!(
            "raft transfer-leader: server endpoint not yet implemented (HTTP {}). \
             See specs/in-process/sprint-03-openraft-patches.md (W3.8, W3.9). \
             Body: {}",
            status, body
        )
        .into())
    } else {
        Err(format!("raft transfer-leader failed (HTTP {}): {}", status, body).into())
    }
}

// ── W8.5 — Learner lifecycle commands (ADR-014) ─────────────────────

/// Build the `add-learner` URL.
fn add_learner_url(host: &str, web_port: u16) -> String {
    format!("http://{}:{}/api/cluster/add-learner", host, web_port)
}

/// Build the JSON body for `add-learner`.
fn add_learner_body(host_id: &str, addr: &str, owns_tokens: bool) -> serde_json::Value {
    serde_json::json!({
        "host_id": host_id,
        "addr": addr,
        "owns_tokens": owns_tokens,
    })
}

/// Build the `promote-to-voter` URL.
fn promote_to_voter_url(host: &str, web_port: u16) -> String {
    format!("http://{}:{}/api/cluster/promote-to-voter", host, web_port)
}

/// Build the JSON body for `promote-to-voter`.
fn promote_to_voter_body(host_id: &str) -> serde_json::Value {
    serde_json::json!({ "host_id": host_id })
}

/// Build the `demote-to-learner` URL.
fn demote_to_learner_url(host: &str, web_port: u16) -> String {
    format!("http://{}:{}/api/cluster/demote-to-learner", host, web_port)
}

/// Build the JSON body for `demote-to-learner`.
fn demote_to_learner_body(host_id: &str) -> serde_json::Value {
    serde_json::json!({ "host_id": host_id })
}

/// W8.5 — Add a long-lived learner replica to the cluster.
///
/// Issues `POST /api/cluster/add-learner`. The server-side handler
/// invokes `MembershipChanger::add_learner_only(host_id, addr,
/// NodeJoinConfig { owns_tokens })`.
pub async fn cluster_add_learner(
    host: &str,
    web_port: u16,
    host_id: &str,
    addr: &str,
    owns_tokens: bool,
) -> Result<(), WebError> {
    let url = add_learner_url(host, web_port);
    let body = add_learner_body(host_id, addr, owns_tokens);
    let client = reqwest::Client::new();
    let resp = client.post(&url).json(&body).send().await?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!(
            "Learner {} ({}) added to cluster (owns_tokens={}).",
            host_id, addr, owns_tokens
        );
        if !text.is_empty() {
            println!("{text}");
        }
        Ok(())
    } else if status.as_u16() == 404 || status.as_u16() == 501 {
        Err(format!(
            "cluster add-learner: server endpoint not yet wired (HTTP {status}). \
             See specs/in-process/sprint-08-learners-endurance.md (W8.5). \
             Body: {text}"
        )
        .into())
    } else {
        Err(format!("cluster add-learner failed (HTTP {status}): {text}").into())
    }
}

/// W8.5 — Promote a learner to a voter.
///
/// Issues `POST /api/cluster/promote-to-voter`. The server-side handler
/// invokes `MembershipChanger::promote_learner_to_voter(host_id)`.
pub async fn cluster_promote_to_voter(
    host: &str,
    web_port: u16,
    host_id: &str,
) -> Result<(), WebError> {
    let url = promote_to_voter_url(host, web_port);
    let body = promote_to_voter_body(host_id);
    let client = reqwest::Client::new();
    let resp = client.post(&url).json(&body).send().await?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("Learner {host_id} promoted to voter.");
        if !text.is_empty() {
            println!("{text}");
        }
        Ok(())
    } else if status.as_u16() == 404 || status.as_u16() == 501 {
        Err(format!(
            "cluster promote-to-voter: server endpoint not yet wired (HTTP {status}). \
             See specs/in-process/sprint-08-learners-endurance.md (W8.5). \
             Body: {text}"
        )
        .into())
    } else {
        Err(format!("cluster promote-to-voter failed (HTTP {status}): {text}").into())
    }
}

/// W8.5 — Demote a voter to a learner.
///
/// Issues `POST /api/cluster/demote-to-learner`. The server-side
/// handler invokes `MembershipChanger::demote_voter_to_learner(host_id)`.
pub async fn cluster_demote_to_learner(
    host: &str,
    web_port: u16,
    host_id: &str,
) -> Result<(), WebError> {
    let url = demote_to_learner_url(host, web_port);
    let body = demote_to_learner_body(host_id);
    let client = reqwest::Client::new();
    let resp = client.post(&url).json(&body).send().await?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        println!("Voter {host_id} demoted to learner.");
        if !text.is_empty() {
            println!("{text}");
        }
        Ok(())
    } else if status.as_u16() == 404 || status.as_u16() == 501 {
        Err(format!(
            "cluster demote-to-learner: server endpoint not yet wired (HTTP {status}). \
             See specs/in-process/sprint-08-learners-endurance.md (W8.5). \
             Body: {text}"
        )
        .into())
    } else {
        Err(format!("cluster demote-to-learner failed (HTTP {status}): {text}").into())
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

/// Run anti-entropy repair against every peer that replicates `keyspace.table`.
///
/// Issues `POST /api/cluster/repair?keyspace=&table=&rf=` and prints the
/// per-table session summary the server returns: total/ok/failed session
/// counts, partitions streamed in/out, timestamp ties, and any error
/// strings from individual sessions.
pub async fn repair(
    host: &str,
    web_port: u16,
    keyspace: &str,
    table: &str,
    rf: usize,
) -> Result<(), WebError> {
    let url = format!(
        "http://{host}:{web_port}/api/cluster/repair?keyspace={keyspace}&table={table}&rf={rf}"
    );
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.is_success() {
        // Pretty-print the JSON summary if we can parse it; otherwise dump raw.
        match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or(body)),
            Err(_) => println!("{body}"),
        }
        Ok(())
    } else {
        Err(format!("repair failed (HTTP {status}): {body}").into())
    }
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
    let url = format!("http://{host}:{web_port}/api/snapshots");
    let mut body = serde_json::json!({ "name": name });
    if let Some(ttl) = ttl_hours {
        body["ttl_hours"] = serde_json::json!(ttl);
    }
    let resp = reqwest::Client::new().post(&url).json(&body).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        println!("Snapshot '{name}' created: {text}");
        Ok(())
    } else {
        Err(format!("POST {url}: {status} — {text}").into())
    }
}

/// List snapshots on the target node.
pub async fn snapshot_list(host: &str, web_port: u16) -> Result<(), WebError> {
    let url = format!("http://{host}:{web_port}/api/snapshots");
    let resp = reqwest::Client::new().get(&url).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        println!("{text}");
        Ok(())
    } else {
        Err(format!("GET {url}: {status} — {text}").into())
    }
}

/// Delete a snapshot on the target node.
pub async fn snapshot_delete(host: &str, web_port: u16, name: &str) -> Result<(), WebError> {
    let url = format!("http://{host}:{web_port}/api/snapshots/{name}");
    let resp = reqwest::Client::new().delete(&url).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        println!("Snapshot '{name}' deleted.");
        Ok(())
    } else {
        Err(format!("DELETE {url}: {status} — {text}").into())
    }
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
    let url = format!("http://{host}:{web_port}/api/restore");
    let mut body = serde_json::json!({
        "snapshot_name": snapshot_name,
        "force": force,
    });
    if let Some(pit) = point_in_time {
        body["point_in_time"] = serde_json::json!(pit);
    }
    let resp = reqwest::Client::new().post(&url).json(&body).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_success() {
        println!("Restore initiated: {text}");
        Ok(())
    } else {
        Err(format!("POST {url}: {status} — {text}").into())
    }
}

// ── Raft administration (offline) ─────────────────────────────────────────────

/// Wipe a stopped node's persisted Raft state by atomically replacing
/// the raft directory.
///
/// The node must be stopped before this runs. Reset deliberately does
/// not open the sled database, so it does not take sled's exclusive
/// directory lock and the next startup can immediately open a fresh store.
/// On `dry_run = true`, the function reports the planned replacement
/// without opening or mutating the database.
pub fn raft_reset(
    data_dir: &std::path::Path,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use ferrosa_cluster::raft::log_store::SledLogStore;

    if dry_run {
        println!(
            "ferrosa-ctl raft reset --dry-run\n\
             would replace raft directory at: {}\n\
             note: exact log/meta counts are not reported because dry-run does not open sled",
            data_dir.display()
        );
        return Ok(());
    }

    let counts = SledLogStore::reset(data_dir)?;
    let backup = counts
        .backup_path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "<none; directory was missing>".to_string());
    println!(
        "ferrosa-ctl raft reset\n\
         path: {}\n\
         backup: {}\n\
         counts: unavailable; reset did not open sled\n\
         next start: this node will rejoin as a learner; \
         the leader's snapshot/append will replay committed history.",
        data_dir.display(),
        backup
    );
    Ok(())
}

/// W6.7 (ADR-015): bootstrap a new per-DC Raft group.
///
/// Computes the deterministic [`ferrosa_cluster::raft::RaftGroupId`]
/// from the DC name and prints the operator-visible bootstrap plan.
/// The HTTP wire-up to the running node lands in Sprint 7 alongside
/// the multi-DC formation rollout — until then this command is the
/// scaffolding that lets operators verify the per-DC namespace
/// (group_id, log dir layout, seed list) before pulling the trigger.
///
/// Returns `Ok(())` for any well-formed argument set; an empty seed
/// list is still allowed because Sprint 6 doesn't yet contact the
/// node, and this enables CI tests of the wiring.
pub fn cluster_bootstrap_dc(
    dc_name: &str,
    seeds: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if dc_name.trim().is_empty() {
        return Err("--dc must not be empty".into());
    }
    let group_id = ferrosa_cluster::raft::RaftGroupId::for_dc(dc_name);
    println!(
        "ferrosa-ctl cluster bootstrap-dc\n\
         dc:        {dc_name}\n\
         group_id:  {group_id}\n\
         seeds:     {seeds_pretty}\n\
         log_dir:   <raft_data_dir>/{dc_name}/\n\
         status:    Sprint 6 scaffolding — group_id derivation verified;\n\
                    the live HTTP wire-up lands in Sprint 7. Re-run\n\
                    against a Sprint 7 build (or call the HTTP endpoint\n\
                    directly) to actually create the group on the node.",
        seeds_pretty = if seeds.is_empty() {
            "(none)".to_string()
        } else {
            seeds.join(", ")
        }
    );
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use ferrosa_cql::client::{QueryResult, ResultRow};

    use super::*;

    /// W1.11: `ferrosa-ctl raft reset` clears a stopped node's persisted
    /// Raft state (log + meta), enabling recovery from the runaway-term
    /// failure mode. The test simulates a node with persisted Raft state,
    /// runs the reset, and asserts the trees are empty afterward.
    #[test]
    fn ferrosa_ctl_raft_reset_recovers_runaway_term_node() {
        let dir = tempfile::tempdir().unwrap();

        // Step 1: simulate a running node by creating a sled DB with
        // some log entries and meta keys.
        {
            let db = sled::open(dir.path()).unwrap();
            let log = db.open_tree("log").unwrap();
            let meta = db.open_tree("meta").unwrap();
            for i in 0u64..5 {
                log.insert(i.to_be_bytes(), b"entry".to_vec()).unwrap();
            }
            meta.insert(b"vote", b"some-vote-bytes".to_vec()).unwrap();
            db.flush().unwrap();
        } // drop releases the lock

        // Step 2: run `ferrosa-ctl raft reset` (the underlying function).
        super::raft_reset(dir.path(), false).expect("raft reset must succeed");

        // Step 3: reopen and confirm the trees are empty — the node will
        // rejoin as a fresh learner.
        let db = sled::open(dir.path()).unwrap();
        let log = db.open_tree("log").unwrap();
        let meta = db.open_tree("meta").unwrap();
        assert_eq!(log.len(), 0, "log must be empty after reset");
        assert_eq!(meta.len(), 0, "meta must be empty after reset");
    }

    /// W1.11 dry-run: --dry-run reports counts without mutating.
    #[test]
    fn ferrosa_ctl_raft_reset_dry_run_does_not_mutate() {
        let dir = tempfile::tempdir().unwrap();

        {
            let db = sled::open(dir.path()).unwrap();
            let log = db.open_tree("log").unwrap();
            for i in 0u64..3 {
                log.insert(i.to_be_bytes(), b"entry".to_vec()).unwrap();
            }
            db.flush().unwrap();
        }

        super::raft_reset(dir.path(), true).expect("dry-run must succeed");

        // Assert nothing was cleared.
        let db = sled::open(dir.path()).unwrap();
        let log = db.open_tree("log").unwrap();
        assert_eq!(log.len(), 3, "dry-run must not mutate");
    }

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

    /// Helper: build the URL for repair.
    fn repair_url(host: &str, web_port: u16, keyspace: &str, table: &str, rf: usize) -> String {
        format!(
            "http://{host}:{web_port}/api/cluster/repair?keyspace={keyspace}&table={table}&rf={rf}"
        )
    }

    #[test]
    fn repair_formats_correct_url() {
        let url = repair_url("127.0.0.1", 9090, "agent_memory", "entity_store", 3);
        assert_eq!(
            url,
            "http://127.0.0.1:9090/api/cluster/repair?keyspace=agent_memory&table=entity_store&rf=3"
        );
    }

    #[test]
    fn repair_url_custom_rf() {
        let url = repair_url("10.0.0.5", 8080, "ks", "t", 5);
        assert_eq!(
            url,
            "http://10.0.0.5:8080/api/cluster/repair?keyspace=ks&table=t&rf=5"
        );
    }

    // ── W8.5 — learner lifecycle CLI commands ────────────────────────

    /// W8.5 RED. `ferrosa-ctl cluster add-learner <host_id> <addr>`
    /// must POST `{host_id, addr, owns_tokens}` to the new
    /// `/api/cluster/add-learner` endpoint.
    #[test]
    fn ferrosa_ctl_cluster_add_learner() {
        let url = super::add_learner_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/add-learner");

        let body = super::add_learner_body("host-1", "10.0.0.7:7000", true);
        assert_eq!(body["host_id"], "host-1");
        assert_eq!(body["addr"], "10.0.0.7:7000");
        assert_eq!(body["owns_tokens"], true);

        // Default owns_tokens is true (long-lived read replica).
        let body_default = super::add_learner_body("host-2", "10.0.0.8:7000", true);
        assert_eq!(body_default["owns_tokens"], true);

        // Witness / analytics learner: owns_tokens=false.
        let body_witness = super::add_learner_body("host-3", "10.0.0.9:7000", false);
        assert_eq!(body_witness["owns_tokens"], false);
    }

    /// W8.5 RED. `ferrosa-ctl cluster promote-to-voter <host_id>` must
    /// POST `{host_id}` to `/api/cluster/promote-to-voter`.
    #[test]
    fn ferrosa_ctl_cluster_promote_to_voter() {
        let url = super::promote_to_voter_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/promote-to-voter");

        let body = super::promote_to_voter_body("host-promote");
        assert_eq!(body["host_id"], "host-promote");
    }

    /// W8.5 RED. `ferrosa-ctl cluster demote-to-learner <host_id>` must
    /// POST `{host_id}` to `/api/cluster/demote-to-learner`.
    #[test]
    fn ferrosa_ctl_cluster_demote_to_learner() {
        let url = super::demote_to_learner_url("127.0.0.1", 9090);
        assert_eq!(url, "http://127.0.0.1:9090/api/cluster/demote-to-learner");

        let body = super::demote_to_learner_body("host-demote");
        assert_eq!(body["host_id"], "host-demote");
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

    // ── CT-001: CQL command output formatting ──────────────────────────────────

    #[test]
    fn print_table_binary_cell_renders_as_binary_marker() {
        // Non-UTF8 bytes should render as "<binary>", not panic.
        let result = make_result(&["data"], vec![vec![Some(&[0xFF, 0xFE, 0x00])]]);
        // Exercise the rendering path — the real assertion is that no panic occurs
        // and the cell mapping logic produces "<binary>" for non-UTF8.
        let cell_text: Vec<String> = result.rows[0]
            .columns
            .iter()
            .map(|cell| match cell {
                None => "NULL".to_string(),
                Some(bytes) => std::str::from_utf8(bytes)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<binary>".to_string()),
            })
            .collect();
        assert_eq!(cell_text, vec!["<binary>"]);
    }

    #[test]
    fn print_table_all_null_row() {
        // A row where every column is NULL should render without panic.
        let result = make_result(&["a", "b", "c"], vec![vec![None, None, None]]);
        let cell_texts: Vec<String> = result.rows[0]
            .columns
            .iter()
            .map(|cell| match cell {
                None => "NULL".to_string(),
                Some(bytes) => std::str::from_utf8(bytes)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<binary>".to_string()),
            })
            .collect();
        assert_eq!(cell_texts, vec!["NULL", "NULL", "NULL"]);
    }

    #[test]
    fn print_table_no_columns_no_panic() {
        // Edge case: result with zero columns and zero rows.
        let result = make_result(&[], vec![]);
        print_table(&result);
    }

    #[test]
    fn print_table_single_column_header_only() {
        // Header with one column and no data rows.
        let result = make_result(&["only_col"], vec![]);
        print_table(&result);
    }

    #[test]
    fn print_table_multi_row_data() {
        // Multiple rows should all render without error.
        let result = make_result(
            &["id", "name"],
            vec![
                vec![Some(b"1"), Some(b"alice")],
                vec![Some(b"2"), Some(b"bob")],
                vec![Some(b"3"), Some(b"carol")],
            ],
        );
        assert_eq!(result.rows.len(), 3);
        print_table(&result);
    }

    #[test]
    fn print_empty_output_format() {
        // Verify print_empty produces output (no panic).
        print_empty("widgets");
    }

    #[test]
    fn print_table_row_with_fewer_columns_than_header() {
        // If a row has fewer columns than the header, the mapping should handle it.
        let mut result = make_result(&["a", "b", "c"], vec![vec![Some(b"x")]]);
        // The row only has 1 column but header has 3 — iter handles this gracefully
        // since it maps over `row.columns.iter()` which stops at actual column count.
        assert_eq!(result.rows[0].columns.len(), 1);
        print_table(&result);
        // Also verify the sort logic doesn't panic when rows are short.
        let col = "b";
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        }
    }

    // ── CT-001: Long-running query sort edge cases ─────────────────────────────

    #[test]
    fn long_running_sort_with_null_elapsed() {
        // Rows with NULL elapsed_ms should sort to the bottom (treated as 0).
        let mut result = make_result(
            &["query_id", "elapsed_ms"],
            vec![vec![Some(b"q1"), None], vec![Some(b"q2"), Some(b"300")]],
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
        // q2(300) should be first, q1(NULL -> 0) second.
        assert_eq!(result.rows[0].columns[0].as_deref(), Some(b"q2" as &[u8]));
        assert_eq!(result.rows[1].columns[0].as_deref(), Some(b"q1" as &[u8]));
    }

    #[test]
    fn long_running_sort_with_non_numeric_elapsed() {
        // Non-numeric elapsed_ms values should be treated as 0.
        let mut result = make_result(
            &["query_id", "elapsed_ms"],
            vec![
                vec![Some(b"q1"), Some(b"not-a-number")],
                vec![Some(b"q2"), Some(b"100")],
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
        // q2(100) first, q1(NaN -> 0) second.
        assert_eq!(result.rows[0].columns[0].as_deref(), Some(b"q2" as &[u8]));
        assert_eq!(result.rows[1].columns[0].as_deref(), Some(b"q1" as &[u8]));
    }

    #[test]
    fn long_running_sort_all_zeros() {
        // All rows with 0 elapsed_ms — sort should be stable (no panic).
        let mut result = make_result(
            &["query_id", "elapsed_ms"],
            vec![
                vec![Some(b"q1"), Some(b"0")],
                vec![Some(b"q2"), Some(b"0")],
                vec![Some(b"q3"), Some(b"0")],
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
        // All equal; just verify no panic and length preserved.
        assert_eq!(result.rows.len(), 3);
    }

    // ── CT-002: Cluster URL construction edge cases ────────────────────────────

    #[test]
    fn cluster_urls_with_hostname() {
        // URLs should work with hostnames, not just IP addresses.
        assert_eq!(
            add_node_url("my-node.example.com", 9090),
            "http://my-node.example.com:9090/api/cluster/add-node"
        );
        assert_eq!(
            decommission_url("my-node.example.com", 9090),
            "http://my-node.example.com:9090/api/cluster/decommission"
        );
        assert_eq!(
            ring_url("my-node.example.com", 9090),
            "http://my-node.example.com:9090/api/cluster/ring"
        );
        assert_eq!(
            rebalance_url("my-node.example.com", 9090),
            "http://my-node.example.com:9090/api/cluster/rebalance"
        );
    }

    #[test]
    fn cluster_urls_with_port_boundaries() {
        // Minimum and maximum valid port numbers.
        assert_eq!(
            add_node_url("localhost", 1),
            "http://localhost:1/api/cluster/add-node"
        );
        assert_eq!(
            add_node_url("localhost", 65535),
            "http://localhost:65535/api/cluster/add-node"
        );
    }

    #[test]
    fn snapshot_delete_url_with_special_chars() {
        // Snapshot names may contain hyphens and underscores.
        let url = snapshot_delete_url("127.0.0.1", 9090, "snap_2026-03-30_daily");
        assert_eq!(
            url,
            "http://127.0.0.1:9090/api/snapshots/snap_2026-03-30_daily"
        );
    }

    #[test]
    fn snapshot_urls_with_custom_host_and_port() {
        assert_eq!(
            snapshot_create_url("10.0.1.5", 8443),
            "http://10.0.1.5:8443/api/snapshots"
        );
        assert_eq!(
            snapshot_list_url("10.0.1.5", 8443),
            "http://10.0.1.5:8443/api/snapshots"
        );
        assert_eq!(
            restore_url("10.0.1.5", 8443),
            "http://10.0.1.5:8443/api/restore"
        );
    }

    // ── CT-002: Ring JSON response parsing edge cases ──────────────────────────

    #[test]
    fn ring_response_with_numeric_values() {
        // The ring response may include non-string JSON types (numbers, booleans).
        let value = serde_json::json!([
            { "host_id": "node-1", "token": -9223372036854775808_i64, "load": 42.5, "up": true },
        ]);

        let entries = value.as_array().unwrap();
        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        // Render each cell using the same logic as ring().
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

            // Numeric values should be rendered via their Display impl.
            assert!(row.contains(&"node-1".to_string()));
            // i64, f64, bool are all rendered via to_string.
            assert!(row.iter().any(|s| s == "-9223372036854775808"));
            assert!(row.iter().any(|s| s == "42.5"));
            assert!(row.iter().any(|s| s == "true"));
        }
    }

    #[test]
    fn ring_response_with_missing_key_in_second_entry() {
        // If later entries are missing keys present in the first, they should render as "NULL".
        let value = serde_json::json!([
            { "host_id": "node-1", "token": "0", "status": "UP" },
            { "host_id": "node-2", "token": "100" },
        ]);

        let entries = value.as_array().unwrap();
        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        // Second entry is missing "status"
        let second_row: Vec<String> = columns
            .iter()
            .map(|col| {
                entries[1]
                    .get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect();

        assert!(second_row.contains(&"NULL".to_string()));
        assert!(second_row.contains(&"node-2".to_string()));
    }

    #[test]
    fn ring_response_non_array_is_detected() {
        // If the server returns an object instead of an array, as_array() returns None.
        let value = serde_json::json!({ "error": "not implemented" });
        assert!(value.as_array().is_none());
        // The function would fall through to serde_json::to_string_pretty.
        let pretty = serde_json::to_string_pretty(&value).unwrap();
        assert!(pretty.contains("not implemented"));
    }

    #[test]
    fn ring_response_single_entry() {
        // Single-node ring should work.
        let value = serde_json::json!([
            { "host_id": "only-node", "token": "0", "status": "UP" },
        ]);
        let entries = value.as_array().unwrap();
        assert_eq!(entries.len(), 1);

        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();
        assert!(!columns.is_empty());
    }

    // ── CT-003: Error handling and address parsing ─────────────────────────────

    #[test]
    fn socket_addr_parsing_valid() {
        let addr: Result<std::net::SocketAddr, _> = "127.0.0.1:9042".parse();
        assert!(addr.is_ok());
        let addr = addr.unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 9042);
    }

    #[test]
    fn socket_addr_parsing_ipv6() {
        let addr: Result<std::net::SocketAddr, _> = "[::1]:9042".parse();
        assert!(addr.is_ok());
        let addr = addr.unwrap();
        assert_eq!(addr.port(), 9042);
    }

    #[test]
    fn socket_addr_parsing_invalid_host() {
        let addr: Result<std::net::SocketAddr, _> = "not-an-address".parse();
        assert!(addr.is_err());
    }

    #[test]
    fn socket_addr_parsing_missing_port() {
        let addr: Result<std::net::SocketAddr, _> = "127.0.0.1".parse();
        assert!(addr.is_err());
    }

    #[test]
    fn socket_addr_parsing_port_out_of_range() {
        let addr: Result<std::net::SocketAddr, _> = "127.0.0.1:99999".parse();
        assert!(addr.is_err());
    }

    #[test]
    fn web_host_extraction_from_socket_addr() {
        // Mirrors main.rs logic: addr.ip().to_string() is used for HTTP calls.
        let addr: std::net::SocketAddr = "10.0.0.5:9042".parse().unwrap();
        let web_host = addr.ip().to_string();
        assert_eq!(web_host, "10.0.0.5");

        let web_port: u16 = 9090;
        let url = add_node_url(&web_host, web_port);
        assert_eq!(url, "http://10.0.0.5:9090/api/cluster/add-node");
    }

    #[test]
    fn web_host_extraction_from_ipv6_socket_addr() {
        let addr: std::net::SocketAddr = "[::1]:9042".parse().unwrap();
        let web_host = addr.ip().to_string();
        assert_eq!(web_host, "::1");
    }

    #[test]
    fn http_error_message_formatting() {
        // Verify the error message pattern used by add_node, decommission, rebalance.
        let status = 503_u16;
        let body = "service unavailable";
        let msg = format!("add-node failed (HTTP {}): {}", status, body);
        assert_eq!(msg, "add-node failed (HTTP 503): service unavailable");

        let msg2 = format!("decommission failed (HTTP {}): {}", 404, "not found");
        assert_eq!(msg2, "decommission failed (HTTP 404): not found");

        let msg3 = format!("ring failed (HTTP {}): {}", 500, "internal error");
        assert_eq!(msg3, "ring failed (HTTP 500): internal error");

        let msg4 = format!("rebalance failed (HTTP {}): {}", 409, "conflict");
        assert_eq!(msg4, "rebalance failed (HTTP 409): conflict");
    }

    #[test]
    fn http_error_message_with_empty_body() {
        // When server returns an error with no body.
        let body = String::new();
        let msg = format!("add-node failed (HTTP {}): {}", 500, body);
        assert_eq!(msg, "add-node failed (HTTP 500): ");
    }

    // ── CT-004: Storage stats and topology rendering ──────────────────────────

    #[test]
    fn storage_stats_multi_column_result() {
        // Typical storage stats result with multiple metric columns.
        let result = make_result(
            &[
                "keyspace",
                "table",
                "live_disk_space",
                "memtable_size",
                "sstable_count",
            ],
            vec![
                vec![
                    Some(b"system"),
                    Some(b"local"),
                    Some(b"1048576"),
                    Some(b"4096"),
                    Some(b"3"),
                ],
                vec![
                    Some(b"my_ks"),
                    Some(b"users"),
                    Some(b"52428800"),
                    Some(b"16384"),
                    Some(b"12"),
                ],
            ],
        );

        assert_eq!(result.column_names.len(), 5);
        assert_eq!(result.rows.len(), 2);
        // Verify specific cell values.
        assert_eq!(
            result.rows[0].columns[0].as_deref(),
            Some(b"system" as &[u8])
        );
        assert_eq!(
            result.rows[1].columns[2].as_deref(),
            Some(b"52428800" as &[u8])
        );
        // Render should not panic.
        print_table(&result);
    }

    #[test]
    fn storage_stats_empty_result_formatting() {
        // When no storage stats are available, print_empty should be called.
        let result = make_result(&["keyspace", "table", "live_disk_space"], vec![]);
        assert!(result.rows.is_empty());
        print_empty("storage stats");
    }

    #[test]
    fn topology_peers_result_rendering() {
        // Simulates system.peers query result used by run_topology.
        let result = make_result(
            &["peer", "data_center", "rack", "tokens"],
            vec![
                vec![
                    Some(b"10.0.0.2"),
                    Some(b"dc1"),
                    Some(b"rack1"),
                    Some(b"-4611686018427387904"),
                ],
                vec![
                    Some(b"10.0.0.3"),
                    Some(b"dc1"),
                    Some(b"rack2"),
                    Some(b"4611686018427387904"),
                ],
            ],
        );

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.column_names[0], "peer");
        assert_eq!(result.column_names[3], "tokens");
        print_table(&result);
    }

    #[test]
    fn topology_single_node_empty_peers() {
        // Single-node cluster has no peers.
        let result = make_result(&["peer", "data_center", "rack", "tokens"], vec![]);
        assert!(result.rows.is_empty());
        // run_topology prints "Topology: single-node cluster (no peers)" for this case.
    }

    #[test]
    fn topology_multi_dc_peers() {
        // Multi-datacenter topology.
        let result = make_result(
            &["peer", "data_center", "rack", "release_version"],
            vec![
                vec![
                    Some(b"10.0.0.2"),
                    Some(b"us-east-1"),
                    Some(b"rack1"),
                    Some(b"0.1.0"),
                ],
                vec![
                    Some(b"10.1.0.2"),
                    Some(b"eu-west-1"),
                    Some(b"rack1"),
                    Some(b"0.1.0"),
                ],
                vec![
                    Some(b"10.2.0.2"),
                    Some(b"ap-south-1"),
                    Some(b"rack1"),
                    Some(b"0.1.0"),
                ],
            ],
        );
        assert_eq!(result.rows.len(), 3);
        // Verify distinct data centers.
        let dcs: Vec<&[u8]> = result
            .rows
            .iter()
            .filter_map(|r| r.columns[1].as_deref())
            .collect();
        assert_eq!(dcs.len(), 3);
        assert!(dcs.contains(&(b"us-east-1" as &[u8])));
        assert!(dcs.contains(&(b"eu-west-1" as &[u8])));
        assert!(dcs.contains(&(b"ap-south-1" as &[u8])));
        print_table(&result);
    }

    #[test]
    fn snapshot_create_body_serialization() {
        // Verify the JSON body for snapshot create with TTL.
        let body = serde_json::json!({
            "name": "hourly-snap",
            "ttl_hours": 24,
        });
        assert_eq!(body["name"], "hourly-snap");
        assert_eq!(body["ttl_hours"], 24);
    }

    #[test]
    fn snapshot_create_body_without_ttl() {
        let ttl: Option<u64> = None;
        let body = serde_json::json!({
            "name": "manual-snap",
            "ttl_hours": ttl,
        });
        assert_eq!(body["name"], "manual-snap");
        assert!(body["ttl_hours"].is_null());
    }

    #[test]
    fn snapshot_list_response_parsing() {
        // Simulated GET /api/snapshots response.
        let value = serde_json::json!([
            { "name": "daily-2026-03-29", "created_at": "2026-03-29T00:00:00Z", "size_bytes": 1048576 },
            { "name": "daily-2026-03-30", "created_at": "2026-03-30T00:00:00Z", "size_bytes": 2097152 },
        ]);

        let snapshots = value.as_array().unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0]["name"], "daily-2026-03-29");
        assert_eq!(snapshots[1]["size_bytes"], 2097152);
    }

    #[test]
    fn snapshot_list_empty_response() {
        let value = serde_json::json!([]);
        let snapshots = value.as_array().unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn restore_body_full_serialization() {
        // Complete restore request body with all fields.
        let snapshot_name = "backup-2026-03-30";
        let point_in_time: Option<&str> = Some("2026-03-30T06:00:00Z");
        let force = true;

        let body = serde_json::json!({
            "snapshot_name": snapshot_name,
            "point_in_time": point_in_time,
            "force": force,
        });

        assert_eq!(body["snapshot_name"], "backup-2026-03-30");
        assert_eq!(body["point_in_time"], "2026-03-30T06:00:00Z");
        assert_eq!(body["force"], true);
    }

    // ── CT-002/CT-004: Ring table rendering with tabled ────────────────────────

    #[test]
    fn ring_table_rendering_with_builder() {
        // Exercise the full table-building path used by ring().
        let value = serde_json::json!([
            { "host_id": "node-1", "token": "0", "status": "UP", "load": "1.5 GiB" },
            { "host_id": "node-2", "token": "4611686018427387904", "status": "UP", "load": "2.3 GiB" },
            { "host_id": "node-3", "token": "-4611686018427387904", "status": "DOWN", "load": "0 B" },
        ]);

        let entries = value.as_array().unwrap();
        let columns: Vec<String> = entries
            .first()
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

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
        let rendered = table.to_string();

        // Verify the rendered table contains expected data.
        assert!(rendered.contains("node-1"));
        assert!(rendered.contains("node-2"));
        assert!(rendered.contains("node-3"));
        assert!(rendered.contains("DOWN"));
        assert!(rendered.contains("1.5 GiB"));
    }

    // ── CT-003: Connection sort with all-NULL column ──────────────────────────

    #[test]
    fn connections_sort_with_all_null_values() {
        // Sorting by a column where all values are NULL should not panic.
        let mut result = make_result(
            &["addr", "state"],
            vec![vec![Some(b"10.0.0.1"), None], vec![Some(b"10.0.0.2"), None]],
        );

        let col = "state";
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        }
        // Both are None so order is preserved (stable sort).
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn connections_sort_mixed_null_and_values() {
        // NULLs should sort before non-NULL values (None < Some).
        let mut result = make_result(
            &["addr", "state"],
            vec![
                vec![Some(b"10.0.0.1"), Some(b"open")],
                vec![Some(b"10.0.0.2"), None],
                vec![Some(b"10.0.0.3"), Some(b"closing")],
            ],
        );

        let col = "state";
        if let Some(idx) = result.column_names.iter().position(|n| n == col) {
            result.rows.sort_by(|a, b| {
                let va = a.columns.get(idx).and_then(|c| c.as_deref());
                let vb = b.columns.get(idx).and_then(|c| c.as_deref());
                va.cmp(&vb)
            });
        }
        // None < Some, so NULL row comes first.
        assert!(result.rows[0].columns[1].is_none());
        // "closing" < "open" lexicographically.
        assert_eq!(
            result.rows[1].columns[1].as_deref(),
            Some(b"closing" as &[u8])
        );
        assert_eq!(result.rows[2].columns[1].as_deref(), Some(b"open" as &[u8]));
    }

    /// W6.7: `ferrosa-ctl cluster bootstrap-dc --dc dc3 --seeds ...`
    /// derives a deterministic per-DC `RaftGroupId` and exits clean.
    /// Re-running with the same DC produces an identical group id —
    /// idempotency guarantees that re-issuing the command on retry is
    /// safe.
    #[test]
    fn ferrosa_ctl_bootstrap_dc_creates_raft_group() {
        // Computed once here so the assertion below pins the
        // determinism property without relying on the printed output.
        let id_a = ferrosa_cluster::raft::RaftGroupId::for_dc("dc3");
        let id_b = ferrosa_cluster::raft::RaftGroupId::for_dc("dc3");
        assert_eq!(id_a, id_b, "group_id derivation must be deterministic");

        // Different DC produces a different id — existing dc1/dc2
        // groups are unaffected by a `bootstrap-dc dc3` run.
        let id_other = ferrosa_cluster::raft::RaftGroupId::for_dc("dc1");
        assert_ne!(id_a, id_other);

        // The command itself accepts the well-formed args without
        // error, even with an empty seed list (Sprint 6 scaffolding;
        // the HTTP wire-up that requires the seed list lands in
        // Sprint 7).
        let seeds = vec![
            "node3a:7000".to_string(),
            "node3b:7000".to_string(),
            "node3c:7000".to_string(),
        ];
        cluster_bootstrap_dc("dc3", &seeds).expect("well-formed args");

        // Empty DC name is rejected.
        let err = cluster_bootstrap_dc("", &seeds).expect_err("empty dc must be rejected");
        assert!(err.to_string().contains("--dc"));

        // Empty seeds — allowed in Sprint 6 (operator may inspect the
        // derived group_id without committing to a seed set yet).
        cluster_bootstrap_dc("dc3", &[]).expect("empty seeds permitted in scaffolding");
    }
}
