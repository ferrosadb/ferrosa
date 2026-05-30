//! Format auditor: structural invariants every merged partition must satisfy.
//!
//! The Cassandra tool's `FormatAuditor` checks byte- and semantic-level
//! invariants on compaction output. The universal subset that applies to
//! Ferrosa's row model: rows are strictly ordered by clustering (no duplicate
//! rows survive a merge), cells within a row are strictly ordered by column (no
//! duplicate columns), TTL and local deletion time are non-negative, and a
//! tombstone cell is never simultaneously an expiring write (the IS_DELETED +
//! IS_EXPIRING exclusivity errata).

use ferrosa_common::CellValue;
use ferrosa_sstable::types::{Partition, Row};

/// Audit `partition` and return the list of invariant violations. An empty
/// vector means the partition is well formed.
pub fn audit_partition(partition: &Partition) -> Vec<String> {
    let mut violations = Vec::new();

    for pair in partition.rows.windows(2) {
        if pair[0].clustering >= pair[1].clustering {
            violations.push(format!(
                "rows not strictly ordered by clustering: {:?} >= {:?}",
                pair[0].clustering, pair[1].clustering
            ));
        }
    }

    if let Some(static_row) = &partition.static_row {
        audit_row(static_row, &mut violations);
    }
    for row in &partition.rows {
        audit_row(row, &mut violations);
    }

    violations
}

fn audit_row(row: &Row, violations: &mut Vec<String>) {
    for pair in row.cells.windows(2) {
        if pair[0].0 >= pair[1].0 {
            violations.push(format!(
                "cells not strictly ordered by column: {} >= {}",
                pair[0].0, pair[1].0
            ));
        }
    }
    for (col, cell) in &row.cells {
        audit_cell(*col, cell, violations);
    }
}

fn audit_cell(col: u16, cell: &CellValue, violations: &mut Vec<String>) {
    if cell.ttl < 0 {
        violations.push(format!("column {col}: negative ttl {}", cell.ttl));
    }
    if cell.local_deletion_time < 0 {
        violations.push(format!(
            "column {col}: negative local_deletion_time {}",
            cell.local_deletion_time
        ));
    }
    if cell.value.is_none() && cell.ttl > 0 {
        violations.push(format!(
            "column {col}: tombstone cell must not be expiring (ttl={})",
            cell.ttl
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn row(clustering: &[u8], cells: Vec<(u16, CellValue)>) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }
    }

    fn partition(rows: Vec<Row>) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    #[test]
    fn well_formed_partition_has_no_violations() {
        let p = partition(vec![
            row(b"c0", vec![(0, CellValue::live(b"a".to_vec(), 1))]),
            row(b"c1", vec![(0, CellValue::live(b"b".to_vec(), 1))]),
        ]);
        assert!(audit_partition(&p).is_empty());
    }

    #[test]
    fn detects_unordered_rows_and_duplicate_columns() {
        let unordered = partition(vec![
            row(b"c1", vec![(0, CellValue::live(b"a".to_vec(), 1))]),
            row(b"c0", vec![(0, CellValue::live(b"b".to_vec(), 1))]),
        ]);
        assert!(!audit_partition(&unordered).is_empty());

        let dup_cols = partition(vec![row(
            b"c0",
            vec![
                (0, CellValue::live(b"a".to_vec(), 1)),
                (0, CellValue::live(b"b".to_vec(), 1)),
            ],
        )]);
        assert!(!audit_partition(&dup_cols).is_empty());
    }
}
