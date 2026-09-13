//! Index build state — `ferrosa-ctl index list`.
//!
//! # Why this exists
//!
//! The engine has always known which indexes are built. `IndexStateTracker`
//! maintains a status per index — `current`, `building`, `stale`, `failed` —
//! and `system_observability.secondary_indexes` has exposed it over CQL for
//! some time. What was missing was a way to ASK from an operator's shell.
//!
//! Repair is deliberately NOT here. Triggering a rebuild needs a server-side
//! entry point that does not exist yet: `reload_indexes_from_system_schema`
//! runs only at startup, and there is no `REBUILD INDEX` statement. A flag
//! that quietly did nothing would be worse than no flag.
//!
//! Until now the only way to learn that an index had not been built was to
//! notice a latency step, or to catch a WARN scrolling past:
//!
//! ```text
//! index NOT used: this node's table does not have it, though the schema lists it
//! engine: partition-key index backfill FAILED; rows in this SSTable are not in
//!   the index and reads through it will be incomplete
//! ```
//!
//! That second line is the one that costs: a failed backfill leaves reads
//! through those SSTables incomplete. The planner is honest — it withholds the
//! index and takes a scan where one is licensed — but nothing surfaces the
//! standing condition, so an index can sit failed indefinitely while every
//! query silently pays for a full scan.
//!
//! # The vocabulary is the engine's, deliberately
//!
//! `current` / `building` / `stale` / `failed` are the tracker's own words,
//! rendered verbatim. An operator who greps a log and an operator who runs
//! this command must be looking at the same thing, not at two vocabularies
//! that happen to describe it.
//!
//! # This speaks for ONE node
//!
//! The tracker records what THIS node's SSTables contain. An index can be
//! `current` here and `failed` on a replica. So every answer names the node it
//! asked; reporting one node's state as the cluster's would be a lie in the
//! most expensive direction — "all healthy" while a replica scans.

use std::net::SocketAddr;

use ferrosa_cql::client::{CqlClient, QueryResult, ResultRow};
use ferrosa_cql::error::CqlError;

/// The observability table the engine already publishes.
const SECONDARY_INDEXES: &str =
    "SELECT keyspace_name, table_name, index_name, index_type, status, \
     indexed_sstable_count, pending_sstable_count, lag_seconds, build_errors \
     FROM system_observability.secondary_indexes";

/// Column order of [`SECONDARY_INDEXES`], so a row read is not a pile of
/// magic subscripts.
mod col {
    pub const KEYSPACE: usize = 0;
    pub const TABLE: usize = 1;
    pub const NAME: usize = 2;
    pub const STATUS: usize = 4;
    pub const PENDING: usize = 6;
}

/// One index as one node reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRow {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    /// The tracker's own word: `current`, `building`, `stale`, or `failed`.
    pub status: String,
    /// SSTables awaiting indexing. Non-zero means reads through them are
    /// incomplete right now.
    pub pending: i64,
}

impl IndexRow {
    /// Whether reads through this index are incomplete right now.
    ///
    /// This is the operator's actual question. `failed` qualifies whatever the
    /// pending count says: the rows in the SSTable whose backfill failed are
    /// not in the index, and nothing is currently planning to put them there.
    #[must_use]
    pub fn reads_are_incomplete(&self) -> bool {
        self.status == "failed" || self.pending > 0
    }
}

/// Decode the query result into rows, tolerating a node that predates the
/// observability table.
///
/// A short or reordered row is a real mismatch between this tool and the node
/// it is talking to, so it is reported rather than skipped — a silently
/// dropped row here would read as "that index is fine".
fn decode(result: &QueryResult) -> Result<Vec<IndexRow>, CqlError> {
    let cell = |row: &ResultRow, i: usize| -> String {
        row.columns
            .get(i)
            .and_then(|c| c.as_ref())
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default()
    };

    // `pending_sstable_count` is a CQL `int`: four big-endian bytes, NOT text.
    //
    // Decoding it as a string and calling `.parse().unwrap_or(0)` is how this
    // first shipped, and it was silently wrong in the worst direction — every
    // index reported zero pending SSTables, which renders as "reads complete"
    // for an index that cannot answer in full. A wrong width is a real
    // disagreement with the node, so it is reported, never defaulted.
    let count = |row: &ResultRow, i: usize| -> Result<i64, CqlError> {
        match row.columns.get(i).and_then(|c| c.as_ref()) {
            // NULL means the node had no count to give, which is a genuine
            // zero rather than a decode failure.
            None => Ok(0),
            Some(b) if b.len() == 4 => Ok(i32::from_be_bytes(
                b[..4].try_into().expect("length checked immediately above"),
            ) as i64),
            Some(b) if b.len() == 8 => Ok(i64::from_be_bytes(
                b[..8].try_into().expect("length checked immediately above"),
            )),
            Some(b) => Err(CqlError::ServerError(format!(
                "secondary_indexes column {i} is {} bytes; expected a 4- or 8-byte integer. \
                 This ferrosa-ctl disagrees with the node about that table's shape.",
                b.len()
            ))),
        }
    };

    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.columns.len() <= col::PENDING {
            return Err(CqlError::ServerError(format!(
                "system_observability.secondary_indexes returned {} columns, expected at least {}. \
                 This ferrosa-ctl is newer than the node it is talking to.",
                row.columns.len(),
                col::PENDING + 1
            )));
        }
        out.push(IndexRow {
            keyspace: cell(row, col::KEYSPACE),
            table: cell(row, col::TABLE),
            name: cell(row, col::NAME),
            status: cell(row, col::STATUS),
            pending: count(row, col::PENDING)?,
        });
    }

    // Stable order: two runs against an unchanged node must produce the same
    // list, so an operator comparing before and after is not diffing a set.
    out.sort_by(|a, b| (&a.keyspace, &a.table, &a.name).cmp(&(&b.keyspace, &b.table, &b.name)));
    Ok(out)
}

/// `ferrosa-ctl index list` — what this node knows about every index it has.
///
/// # Errors
///
/// Propagates connection and query failures. A node without the observability
/// table fails loudly rather than reporting an empty, healthy-looking list.
pub async fn run_index_list(addr: SocketAddr, problems_only: bool) -> Result<(), CqlError> {
    let mut client = CqlClient::connect(addr).await?;
    let result = client.query(SECONDARY_INDEXES).await?;
    let rows = decode(&result)?;

    let shown: Vec<&IndexRow> = if problems_only {
        rows.iter().filter(|r| r.reads_are_incomplete()).collect()
    } else {
        rows.iter().collect()
    };

    println!("Indexes on the node at {addr}");
    if rows.is_empty() {
        println!("  (this node has no registered indexes)");
        return Ok(());
    }

    let mut builder = tabled::builder::Builder::default();
    builder.push_record(vec![
        "keyspace", "table", "index", "status", "pending", "reads",
    ]);
    for r in &shown {
        builder.push_record(vec![
            r.keyspace.clone(),
            r.table.clone(),
            r.name.clone(),
            r.status.clone(),
            r.pending.to_string(),
            if r.reads_are_incomplete() {
                "INCOMPLETE".into()
            } else {
                "complete".to_string()
            },
        ]);
    }
    let mut table = builder.build();
    table.with(tabled::settings::Style::psql());
    println!("{table}");

    let incomplete = rows.iter().filter(|r| r.reads_are_incomplete()).count();
    if incomplete > 0 {
        // Say the consequence, not just the count. "3 indexes are failed" is a
        // status; "reads through them are incomplete" is why you care.
        println!(
            "\n{incomplete} of {} indexes cannot answer in full. Reads through them fall back \
             to a scan where one is licensed, and are refused where it is not.",
            rows.len()
        );
        // Say what is actually true about the remedy. A `stale` index has work
        // queued and the build scheduler drains it; a `failed` one is waiting
        // on a retry backoff and may never clear on its own. Promising a
        // `rebuild` subcommand here would be advertising something unbuilt.
        println!(
            "  stale  — SSTables are queued; the build scheduler is working through them.\n\
             \x20 failed — the last build errored and is on a retry backoff. Check the node log \
             for `index backfill FAILED` and the reason."
        );
    }
    // NOT a cluster-wide verdict: this is one node's view.
    println!(
        "\nThis is the state on {addr} alone. An index can be built here and failed on a replica."
    );
    Ok(())
}

/// Build the rebuild URL for one index on one node.
///
/// Split out so the query-string construction is testable without a server —
/// a misspelled parameter name would otherwise surface as a 400 at 3am.
#[must_use]
pub fn rebuild_url(host: &str, web_port: u16, keyspace: &str, table: &str, index: &str) -> String {
    format!(
        "http://{host}:{web_port}/api/index/rebuild?keyspace={keyspace}&table={table}&index={index}"
    )
}

/// What the node said about a rebuild it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildReport {
    pub sstables_rebuilt: u64,
    pub sstables_total: u64,
    pub complete: bool,
}

/// Read the node's rebuild response.
///
/// `complete` is taken from the node rather than recomputed here: the node
/// knows how many SSTables it holds, and a client that guessed would drift.
/// A response missing the counts is a disagreement about the API, reported
/// rather than defaulted — defaulting to zero would render a failed repair as
/// "rebuilt 0 of 0, complete".
///
/// # Errors
///
/// Returns a message when the body is not the expected shape.
pub fn parse_rebuild_report(body: &serde_json::Value) -> Result<RebuildReport, String> {
    let rebuilt = body
        .get("sstables_rebuilt")
        .and_then(serde_json::Value::as_u64)
        .ok_or("response has no `sstables_rebuilt`; this ferrosa-ctl and the node disagree about /api/index/rebuild")?;
    let total = body
        .get("sstables_total")
        .and_then(serde_json::Value::as_u64)
        .ok_or("response has no `sstables_total`; this ferrosa-ctl and the node disagree about /api/index/rebuild")?;
    let complete = body
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .ok_or("response has no `complete`; refusing to guess whether the repair finished")?;
    Ok(RebuildReport {
        sstables_rebuilt: rebuilt,
        sstables_total: total,
        complete,
    })
}

/// `ferrosa-ctl index rebuild` — repair one index on one node.
///
/// # Errors
///
/// Fails when the node is unreachable, refuses the request, or answers in a
/// shape this tool does not recognise.
pub async fn run_index_rebuild(
    web_host: &str,
    web_port: u16,
    keyspace: &str,
    table: &str,
    index: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = rebuild_url(web_host, web_port, keyspace, table, index);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.as_u16() == 404 {
        return Err(format!(
            "the node at {web_host}:{web_port} has no /api/index/rebuild endpoint (HTTP 404). \
             It is older than this ferrosa-ctl."
        )
        .into());
    }
    let body: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("could not read the node's response ({e}): {text}"))?;

    if !status.is_success() {
        let msg = body
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&text);
        return Err(format!("{keyspace}.{table} index '{index}': {msg}").into());
    }

    let report = parse_rebuild_report(&body)?;
    if report.complete {
        println!(
            "Rebuilt '{index}' on {keyspace}.{table}: {} of {} SSTables. Reads through it are \
             complete on {web_host}.",
            report.sstables_rebuilt, report.sstables_total
        );
        Ok(())
    } else {
        // A partial rebuild is NOT a success. Saying so in the exit code is
        // what keeps a repair script from marching on.
        Err(format!(
            "Rebuilt '{index}' on {keyspace}.{table}: only {} of {} SSTables. Reads through \
             this index are STILL incomplete — check the node log for `index backfill FAILED` \
             and the reason.",
            report.sstables_rebuilt, report.sstables_total
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, status: &str, pending: i64) -> IndexRow {
        IndexRow {
            keyspace: "agent_memory".into(),
            table: "entity_warmth".into(),
            name: name.into(),
            status: status.into(),
            pending,
        }
    }

    #[test]
    fn a_current_index_with_nothing_pending_answers_in_full() {
        let r = row("idx_ok", "current", 0);
        assert!(!r.reads_are_incomplete());
    }

    #[test]
    fn a_failed_backfill_means_incomplete_reads() {
        // The condition on Ben's cluster: 13 SSTables whose partition-key
        // index backfill failed. Reads through them are short, and nothing
        // fixes it on its own.
        let r = row("idx_warm", "failed", 0);
        assert!(
            r.reads_are_incomplete(),
            "a failed backfill is incomplete even when the pending queue has drained"
        );
    }

    #[test]
    fn a_stale_index_with_queued_sstables_is_incomplete() {
        let r = row("idx_lag", "stale", 7);
        assert!(
            r.reads_are_incomplete(),
            "7 SSTables are queued, so this index does not cover the whole table yet"
        );
    }

    /// Build a full-width row with `pending` encoded the way the node encodes
    /// it: a CQL `int`, four big-endian bytes.
    fn wire_row(status: &str, pending: i32) -> ResultRow {
        let mut cols: Vec<Option<Vec<u8>>> = vec![
            Some(b"agent_memory".to_vec()),
            Some(b"typed_edges".to_vec()),
            Some(b"idx_typed_edges_dst".to_vec()),
            Some(b"btree".to_vec()),
            Some(status.as_bytes().to_vec()),
            Some(0i32.to_be_bytes().to_vec()),
            Some(pending.to_be_bytes().to_vec()),
        ];
        cols.push(Some(0i64.to_be_bytes().to_vec()));
        ResultRow { columns: cols }
    }

    #[test]
    fn a_binary_int_pending_count_is_decoded_not_stringified() {
        // Regression. This first shipped decoding `pending_sstable_count` as
        // UTF-8 text and swallowing the parse failure with `unwrap_or(0)`, so
        // every index on a live cluster reported zero pending SSTables — which
        // renders as "reads complete" for an index that cannot answer in full.
        let result = QueryResult {
            column_names: vec![],
            rows: vec![wire_row("stale", 7)],
        };
        let rows = decode(&result).expect("a well-formed wire row decodes");
        assert_eq!(
            rows[0].pending, 7,
            "the count must survive the wire, not default to 0"
        );
        assert!(
            rows[0].reads_are_incomplete(),
            "7 pending SSTables means reads through this index are short"
        );
    }

    #[test]
    fn an_integer_of_unexpected_width_is_reported_not_defaulted() {
        let mut row = wire_row("current", 0);
        row.columns[col::PENDING] = Some(vec![0u8; 3]);
        let result = QueryResult {
            column_names: vec![],
            rows: vec![row],
        };
        let err = decode(&result).expect_err("a 3-byte integer is a real disagreement");
        assert!(
            err.to_string().contains("expected a 4- or 8-byte integer"),
            "{err}"
        );
    }

    #[test]
    fn a_node_returning_too_few_columns_fails_loudly() {
        // A short row means this tool and the node disagree about the table.
        // Skipping it would render as "that index is fine".
        let result = QueryResult {
            column_names: vec!["keyspace_name".into()],
            rows: vec![ResultRow {
                columns: vec![Some(b"agent_memory".to_vec())],
            }],
        };
        let err = decode(&result).expect_err("a short row must not be silently dropped");
        assert!(
            err.to_string().contains("newer than the node"),
            "the message must say what to do about it: {err}"
        );
    }
    #[test]
    fn the_rebuild_url_names_every_parameter_the_node_requires() {
        let u = rebuild_url(
            "10.0.0.4",
            9090,
            "agent_memory",
            "entity_store",
            "idx_entity_by_id",
        );
        assert!(
            u.starts_with("http://10.0.0.4:9090/api/index/rebuild?"),
            "{u}"
        );
        assert!(u.contains("keyspace=agent_memory"), "{u}");
        assert!(u.contains("table=entity_store"), "{u}");
        assert!(u.contains("index=idx_entity_by_id"), "{u}");
    }

    #[test]
    fn a_complete_rebuild_reports_complete() {
        let body = serde_json::json!({
            "sstables_rebuilt": 17, "sstables_total": 17, "complete": true
        });
        let r = parse_rebuild_report(&body).unwrap();
        assert!(r.complete);
        assert_eq!(r.sstables_rebuilt, 17);
    }

    #[test]
    fn a_partial_rebuild_is_not_reported_as_complete() {
        // 3 of 17 leaves reads incomplete. Rendering that as success is the
        // failure this whole command exists to avoid.
        let body = serde_json::json!({
            "sstables_rebuilt": 3, "sstables_total": 17, "complete": false
        });
        let r = parse_rebuild_report(&body).unwrap();
        assert!(!r.complete);
    }

    #[test]
    fn a_response_missing_the_counts_is_refused_not_defaulted() {
        // Defaulting to zero would render a failed repair as
        // "rebuilt 0 of 0, complete".
        let err = parse_rebuild_report(&serde_json::json!({ "ok": true }))
            .expect_err("a shapeless response must not parse");
        assert!(err.contains("sstables_rebuilt"), "{err}");
    }

    #[test]
    fn a_response_without_complete_is_refused_rather_than_guessed() {
        let body = serde_json::json!({ "sstables_rebuilt": 5, "sstables_total": 5 });
        let err = parse_rebuild_report(&body).expect_err("must not infer completeness");
        assert!(err.contains("refusing to guess"), "{err}");
    }
}
