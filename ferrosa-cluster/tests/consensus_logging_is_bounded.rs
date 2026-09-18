//! The consensus hot path may not log once per transaction.
//!
//! Why this exists. On the local three-node cluster, `node1.out.log` reached
//! 1.6 GB with no rotation, and 70% of it was two INFO lines emitted once per
//! Accord transaction — 106,274 "transaction committed" and 105,004 "generic IF
//! condition not met" in a single 300,000-line window.
//!
//! That is not a tidiness problem. The writes saturated the same disk the
//! storage engine uses, which froze the CQL request runtime — 19,115 recorded
//! stalls on one node, mean 1453 ms, worst 86,134 ms. A frozen runtime cannot
//! answer Accord read votes, which is how the cluster reached
//! "read-vote lacked F+1 (2) agreement ... (got 0 reads)" and dropped every
//! control session for ~90 seconds. Logging the hot path caused the outage the
//! logging was there to describe, and then logged that too.
//!
//! The rule: a log carrying `txn_id` fires once per transaction, so it belongs
//! at debug or lower. Per-outage and per-phase logs are unaffected — they do
//! not carry a transaction id.

use std::path::Path;

/// Files whose logging is on the per-transaction consensus path.
const HOT_PATH_SOURCES: &[&str] = &["src/accord/coordinator.rs", "src/accord/handlers.rs"];

/// A `tracing::info!` (or bare `info!`) whose fields mention a transaction id.
///
/// Scans statement-wise rather than line-wise because these macros are written
/// across several lines, with the message last.
fn per_transaction_info_sites(source: &str) -> Vec<usize> {
    let bytes: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    for (index, line) in bytes.iter().enumerate() {
        let trimmed = line.trim_start();
        if !(trimmed.starts_with("tracing::info!") || trimmed.starts_with("info!(")) {
            continue;
        }
        // Take the macro invocation up to its closing `);` at the same indent,
        // bounded so a malformed file cannot run away.
        let mut statement = String::new();
        for probe in bytes.iter().skip(index).take(24) {
            statement.push_str(probe);
            statement.push('\n');
            if probe.trim_end().ends_with(");") {
                break;
            }
        }
        if statement.contains("txn_id") {
            found.push(index + 1);
        }
    }
    found
}

#[test]
fn the_consensus_hot_path_does_not_log_once_per_transaction() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    for relative in HOT_PATH_SOURCES {
        let path = root.join(relative);
        if !path.exists() {
            continue;
        }
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        for line in per_transaction_info_sites(&source) {
            offenders.push(format!("{relative}:{line}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "these log once per Accord transaction and must be debug! or lower — at INFO they \
         saturate the disk that the CQL runtime needs, which is what stops consensus \
         answering read votes: {offenders:?}"
    );
}

#[test]
fn the_scanner_recognises_a_multi_line_per_transaction_info() {
    let source = r#"
        tracing::info!(
            txn_id = ?txn_id,
            t = ?commit_t,
            "accord: transaction committed"
        );
"#;
    assert_eq!(per_transaction_info_sites(source), vec![2]);
}

#[test]
fn a_log_without_a_transaction_id_is_not_flagged() {
    let source = r#"
        tracing::info!(peer = ?peer, "accord: Apply phase complete");
"#;
    assert!(per_transaction_info_sites(source).is_empty());
}

#[test]
fn a_debug_log_carrying_a_transaction_id_is_allowed() {
    let source = r#"
        tracing::debug!(
            txn_id = ?txn_id,
            "accord: transaction committed"
        );
"#;
    assert!(per_transaction_info_sites(source).is_empty());
}
