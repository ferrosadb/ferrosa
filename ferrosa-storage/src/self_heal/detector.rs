//! Corrupt-SSTable detector.
//!
//! Scans a table directory for generations that fail the startup smoke test
//! (via [`StorageEngine::scan_table_dir_for_corrupt`]) and turns them into a
//! [`TableIssue`]. **Detection is loud and unconditional**: every detected
//! issue emits a `WARN` + bumps a metric, independent of whether remediation
//! is enabled (design "Loud" principle; FMEA #6).
//!
//! The detector does NOT decide replica posture — that is a cluster fact the
//! caller supplies. It defaults to [`ReplicaPosture::SingleNode`] (the safe,
//! never-quarantine posture) so a mis-wired caller fails safe, not lossy.

use std::path::Path;

use crate::engine::StorageEngine;

use super::metrics;
use super::snapshot::{CorruptSstable, IssueKind, ReplicaPosture, TableIssue, TableKey};

/// Detect corrupt SSTables for one table directory.
///
/// Returns `Some(TableIssue)` when at least one generation is corrupt, else
/// `None`. Emits a loud `WARN` + metric whenever corruption is present.
///
/// `replica_posture` is the caller-supplied cluster fact used downstream by
/// the quarantine safety rail (FMEA #1).
pub fn detect_corrupt_sstables(
    table: &TableKey,
    table_dir: &Path,
    replica_posture: ReplicaPosture,
) -> Option<TableIssue> {
    let corrupt = StorageEngine::scan_table_dir_for_corrupt(table_dir);
    if corrupt.is_empty() {
        return None;
    }

    // LOUD, ALWAYS — independent of remediation (FMEA #6 / "logs warnings if
    // data has issues").
    let gens: Vec<u64> = corrupt.iter().map(|(g, _)| *g).collect();
    tracing::warn!(
        keyspace = %table.keyspace,
        table = %table.table,
        corrupt_count = corrupt.len(),
        generations = ?gens,
        replica_posture = ?replica_posture,
        "self-heal: detected {} corrupt SSTable(s) excluded for table {}",
        corrupt.len(),
        table
    );
    metrics::inc_corrupt_detected(corrupt.len() as u64);

    let corrupt_sstables = corrupt
        .into_iter()
        .map(|(generation, reason)| CorruptSstable { generation, reason })
        .collect();

    Some(TableIssue {
        table: table.clone(),
        kind: IssueKind::CorruptSstables,
        corrupt_sstables,
        replica_posture,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_heal::metrics;
    use crate::self_heal::test_fixtures::{corrupt_one_generation, table_dir_with_n_generations};
    use serial_test::serial;

    #[test]
    #[serial]
    fn no_corruption_returns_none() {
        metrics::_reset_self_heal_metrics_for_tests();
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let table = TableKey::new("test_ks", "test_table");
        let issue =
            detect_corrupt_sstables(&table, &table_dir, ReplicaPosture::HealthyReplicaAvailable);
        assert!(issue.is_none(), "healthy table → no issue");
        assert_eq!(
            metrics::self_heal_metrics().corrupt_sstable_detected_total,
            0
        );
    }

    #[test]
    #[serial]
    fn corruption_emits_metric_and_issue_even_when_unremediable() {
        metrics::_reset_self_heal_metrics_for_tests();
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let corrupted_gen = corrupt_one_generation(&table_dir);

        let table = TableKey::new("test_ks", "test_table");
        // SingleNode posture → remediation will be refused. Detection still
        // fires (loud-on-issue, FMEA #6).
        let issue = detect_corrupt_sstables(&table, &table_dir, ReplicaPosture::SingleNode)
            .expect("corruption must surface an issue");
        assert_eq!(issue.kind, IssueKind::CorruptSstables);
        assert_eq!(issue.corrupt_sstables.len(), 1);
        assert_eq!(issue.corrupt_sstables[0].generation, corrupted_gen);
        assert_eq!(
            metrics::self_heal_metrics().corrupt_sstable_detected_total,
            1
        );
    }
}
