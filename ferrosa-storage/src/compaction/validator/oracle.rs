//! Independent reference merge ("oracle") for compaction validation.
//!
//! This reimplements Ferrosa's documented compaction merge semantics from
//! scratch, with one deliberate difference: a fully **deterministic,
//! order-independent** tie-break. Last-write-wins is only well defined if two
//! cells with the *same* timestamp resolve the same way regardless of the order
//! the sources were merged in — otherwise two compactions (or two replicas)
//! that group SSTables differently diverge.
//!
//! Any divergence between this oracle and [`crate::merge::merge_partitions`]
//! flags a real compaction bug. That is the whole point of a second
//! implementation written against the spec rather than copied from the code.

use std::collections::BTreeMap;

use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

/// Merge every source version of one partition into the canonical result.
///
/// All sources must share the same partition key. The result has rows sorted by
/// clustering key and cells sorted by column, matching the read-path merge.
pub fn oracle_merge(sources: &[Partition]) -> Partition {
    assert!(
        !sources.is_empty(),
        "oracle_merge requires at least one source"
    );

    let mut deletion = sources[0].deletion;
    let mut static_row: Option<Row> = None;
    let mut rows: BTreeMap<Vec<u8>, Row> = BTreeMap::new();

    for source in sources {
        if source.deletion.marked_for_delete_at > deletion.marked_for_delete_at {
            deletion = source.deletion;
        }
        if let Some(sr) = &source.static_row {
            static_row = Some(match static_row {
                None => sr.clone(),
                Some(acc) => oracle_merge_row(&acc, sr),
            });
        }
        for row in &source.rows {
            let merged = match rows.remove(&row.clustering) {
                None => row.clone(),
                Some(acc) => oracle_merge_row(&acc, row),
            };
            rows.insert(row.clustering.clone(), merged);
        }
    }

    let mut partition = Partition {
        key: sources[0].key.clone(),
        deletion,
        static_row,
        rows: rows.into_values().collect(),
    };
    suppress_deleted(&mut partition);
    partition
}

/// Merge two versions of the same row (same clustering key).
fn oracle_merge_row(a: &Row, b: &Row) -> Row {
    let deletion = if deletion_wins(b.deletion, a.deletion) {
        b.deletion
    } else {
        a.deletion
    };
    let primary_key_liveness = if liveness_wins(b.primary_key_liveness, a.primary_key_liveness) {
        b.primary_key_liveness
    } else {
        a.primary_key_liveness
    };

    let mut cells: BTreeMap<u16, CellValue> = BTreeMap::new();
    for (col, cell) in a.cells.iter().chain(b.cells.iter()) {
        let winner = match cells.remove(col) {
            None => cell.clone(),
            Some(cur) if cell_wins(cell, &cur) => cell.clone(),
            Some(cur) => cur,
        };
        cells.insert(*col, winner);
    }

    Row {
        clustering: a.clustering.clone(),
        cells: cells.into_iter().collect(),
        deletion,
        primary_key_liveness,
    }
}

/// True when partition/row deletion `x` supersedes `y` (newer wins; ties broken
/// deterministically by local deletion time).
fn deletion_wins(x: DeletionTime, y: DeletionTime) -> bool {
    (x.marked_for_delete_at, x.local_deletion_time)
        > (y.marked_for_delete_at, y.local_deletion_time)
}

/// True when primary-key liveness `x` supersedes `y`. Newer timestamp wins;
/// equal timestamps are broken deterministically so the merge is order
/// independent.
fn liveness_wins(x: LivenessInfo, y: LivenessInfo) -> bool {
    (x.timestamp, x.ttl, x.local_deletion_time) > (y.timestamp, y.ttl, y.local_deletion_time)
}

/// True when cell `cand` supersedes `cur` under deterministic last-write-wins.
///
/// Higher timestamp wins. On an equal timestamp the result must not depend on
/// merge order: a write (a cell with a value) beats a tombstone so equal-
/// timestamp data is preserved; among writes the lexicographically greater value
/// wins; two tombstones are broken by local deletion time.
fn cell_wins(cand: &CellValue, cur: &CellValue) -> bool {
    if cand.timestamp != cur.timestamp {
        return cand.timestamp > cur.timestamp;
    }
    tie_rank(cand) > tie_rank(cur)
}

fn tie_rank(c: &CellValue) -> (bool, &[u8], i32) {
    (
        c.value.is_some(),
        c.value.as_deref().unwrap_or(&[]),
        c.local_deletion_time,
    )
}

/// Apply deletion suppression, matching `crate::merge::apply_deletions`.
fn suppress_deleted(partition: &mut Partition) {
    let partition_delete_at = partition.deletion.marked_for_delete_at;

    if !partition.deletion.is_live() {
        partition
            .rows
            .retain(|row| row.primary_key_liveness.timestamp >= partition_delete_at);
        if let Some(static_row) = &mut partition.static_row {
            static_row
                .cells
                .retain(|(_col, cell)| cell.timestamp >= partition_delete_at);
            if static_row.cells.is_empty() {
                partition.static_row = None;
            }
        }
    }

    for row in &mut partition.rows {
        if !row.deletion.is_live() {
            let row_delete_at = row.deletion.marked_for_delete_at;
            row.cells
                .retain(|(_col, cell)| cell.timestamp >= row_delete_at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::LivenessInfo;

    fn key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn row(clustering: &[u8], cells: Vec<(u16, CellValue)>, ts: i64) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn partition(k: &str, rows: Vec<Row>) -> Partition {
        Partition {
            key: key(k),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    #[test]
    fn oracle_is_order_independent_on_equal_timestamp_conflict() {
        // Same partition, row, and column written at the SAME timestamp with
        // different values in two SSTables. LWW must converge regardless of the
        // order the sources are merged.
        let a = partition(
            "k",
            vec![row(b"c", vec![(0, CellValue::live(b"aaa".to_vec(), 5))], 5)],
        );
        let b = partition(
            "k",
            vec![row(b"c", vec![(0, CellValue::live(b"bbb".to_vec(), 5))], 5)],
        );

        let ab = oracle_merge(&[a.clone(), b.clone()]);
        let ba = oracle_merge(&[b, a]);

        assert_eq!(ab, ba, "oracle merge must be order-independent");
        // Deterministic tie-break: greater value wins.
        assert_eq!(
            ab.rows[0].cells[0].1.value.as_deref(),
            Some(b"bbb".as_slice())
        );
    }
}
