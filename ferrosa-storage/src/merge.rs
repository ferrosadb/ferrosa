//! Read-path merge for partitions from multiple SSTables and memtables.
//!
//! Cassandra's storage model is append-only: writes never update in place.
//! When reading, we must merge data from multiple sources (memtable + SSTables)
//! into a single consistent view. The merge rules are:
//!
//! - **Cell-level last-write-wins (LWW):** When the same column appears in
//!   multiple sources, the cell with the highest timestamp wins.
//! - **Deletion suppression:** Partition-level and row-level deletions suppress
//!   cells with timestamps older than the deletion timestamp.
//! - **Static row merge:** Static rows are merged using the same cell-level LWW
//!   rules as regular rows.
//! - **Row-level deletion:** When merging two versions of the same row, the
//!   newer deletion timestamp wins.

use ferrosa_sstable::types::{Partition, Row};

/// Merge multiple partitions (from different SSTables/memtable) into one.
///
/// All sources must represent the same partition key. The merge applies
/// cell-level last-write-wins, resolves deletions, and produces a single
/// consistent partition view.
///
/// # Panics
///
/// Panics if `sources` is empty.
pub fn merge_partitions(sources: Vec<Partition>) -> Partition {
    assert!(
        !sources.is_empty(),
        "merge_partitions requires at least one source"
    );

    if sources.len() == 1 {
        let mut result = sources.into_iter().next().unwrap();
        apply_deletions(&mut result);
        return result;
    }

    let mut iter = sources.into_iter();
    let first = iter.next().unwrap();

    let result_key = first.key;
    let mut result_deletion = first.deletion;
    let mut result_static_row = first.static_row;
    let mut all_rows: Vec<Row> = first.rows;

    for source in iter {
        // Partition-level deletion: newest wins
        if source.deletion.marked_for_delete_at > result_deletion.marked_for_delete_at {
            result_deletion = source.deletion;
        }

        // Static row merge
        result_static_row = match (result_static_row, source.static_row) {
            (None, None) => None,
            (Some(r), None) => Some(r),
            (None, Some(r)) => Some(r),
            (Some(a), Some(b)) => Some(merge_rows(a, b)),
        };

        // Collect all rows
        all_rows.extend(source.rows);
    }

    sort_and_fold_rows(&mut all_rows);

    let mut result = Partition {
        key: result_key,
        deletion: result_deletion,
        static_row: result_static_row,
        rows: all_rows,
    };

    apply_deletions(&mut result);
    result
}

/// Sort rows by clustering key IN PLACE and fold same-clustering rows via
/// cell-level LWW (`pop/merge/push`). `sort_by` is stable, so equal-clustering
/// rows keep their input order before folding. Operates on `&mut Vec<Row>` (no
/// `Vec<Row>` return) so it does not read as a read-path materialization site.
/// Shared by [`merge_partitions`] (multi source) and
/// [`ensure_partition_rows_sorted`] (single-source compaction rewrite) so the
/// two paths produce identical row order.
fn sort_and_fold_rows(rows: &mut Vec<Row>) {
    rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
    // Fold adjacent same-clustering rows by rebuilding into the same Vec.
    let sorted = std::mem::take(rows);
    for row in sorted {
        if rows
            .last()
            .is_some_and(|last| last.clustering == row.clustering)
        {
            let prev = rows.pop().unwrap();
            rows.push(merge_rows(prev, row));
        } else {
            rows.push(row);
        }
    }
}

/// Guarantee a partition's rows are in clustering order, sorting + folding
/// duplicates only if they are not already.
///
/// A compaction is a REWRITE: its output must be monotonic. But
/// [`merge_partitions`] returns a single-source partition (`sources.len() == 1`)
/// in its on-disk order — and a legacy Cassandra-format SSTable can store a
/// wide partition's rows OUT of clustering order (t_a0f922a3). Without this,
/// such a partition would be rewritten with byte-comparable bounds
/// (`legacy_format = false`) yet keep its non-monotonic rows, and never be
/// re-selected. Compaction calls this on every merged partition; the
/// `is_sorted_by` guard makes it a cheap O(n) check (no reorder) for the common
/// already-sorted case — including every multi-source `merge_partitions` output,
/// which is already sorted.
pub fn ensure_partition_rows_sorted(partition: &mut Partition) {
    if partition
        .rows
        .is_sorted_by(|a, b| a.clustering <= b.clustering)
    {
        return;
    }
    sort_and_fold_rows(&mut partition.rows);
}

/// Merge two rows with the same clustering key using cell-level LWW.
///
/// Public so the cluster repair crate's cross-source streaming
/// merge in `TableStore::walk_token_range_for_digest` can fold
/// rows arriving one-at-a-time from N SSTable iterators into the
/// merged row that gets hashed into a `PartitionDigestStream` —
/// without ever materialising the full multi-source partition.
pub fn merge_rows(a: Row, b: Row) -> Row {
    // Row-level deletion: newer wins
    let deletion = if b.deletion.marked_for_delete_at > a.deletion.marked_for_delete_at {
        b.deletion
    } else {
        a.deletion
    };

    // Primary key liveness: newer wins
    let primary_key_liveness =
        if b.primary_key_liveness.timestamp > a.primary_key_liveness.timestamp {
            b.primary_key_liveness
        } else {
            a.primary_key_liveness
        };

    // Cell-level LWW: merge cells from both rows, keyed by (column index, cell
    // path). A simple column has one cell (path == None); a complex (collection)
    // column has one cell per element, distinguished by path — so two cells with
    // the same column but different paths are distinct elements that BOTH survive,
    // NOT competing writes. Keying by column index alone would collapse a
    // collection to a single element on every cross-SSTable merge / compaction.
    let mut cells: Vec<(u16, ferrosa_common::CellValue)> = Vec::new();

    // Collect all cells from both rows and sort by (column index, path).
    let mut all_cells: Vec<(u16, ferrosa_common::CellValue)> =
        Vec::with_capacity(a.cells.len() + b.cells.len());
    all_cells.extend(a.cells);
    all_cells.extend(b.cells);
    all_cells.sort_by(|(ca, cella), (cb, cellb)| (ca, &cella.path).cmp(&(cb, &cellb.path)));

    // Merge cells with the same (column_index, path) using LWW.
    for (col, cell) in all_cells {
        if let Some(last) = cells.last() {
            if last.0 == col && last.1.path == cell.path {
                // Same (column, path) — deterministic last-write-wins.
                if cell_supersedes(&cell, &last.1) {
                    cells.pop();
                    cells.push((col, cell));
                }
                continue;
            }
        }
        cells.push((col, cell));
    }

    Row {
        clustering: a.clustering,
        cells,
        deletion,
        primary_key_liveness,
    }
}

/// Deterministic last-write-wins comparison for a single cell.
///
/// Higher timestamp wins. On an **equal** timestamp the outcome must not depend
/// on merge order, or two compactions that group SSTables differently (STCS vs
/// UCS) — or two replicas — would diverge. The order-independent tie-break is:
/// a write (a cell with a value) beats a tombstone so equal-timestamp data is
/// preserved; among writes the lexicographically greater value wins; two
/// tombstones are broken by local deletion time.
fn cell_supersedes(cand: &ferrosa_common::CellValue, cur: &ferrosa_common::CellValue) -> bool {
    if cand.timestamp != cur.timestamp {
        return cand.timestamp > cur.timestamp;
    }
    (
        cand.value.is_some(),
        cand.value.as_deref().unwrap_or(&[]),
        cand.local_deletion_time,
    ) > (
        cur.value.is_some(),
        cur.value.as_deref().unwrap_or(&[]),
        cur.local_deletion_time,
    )
}

/// Apply deletion suppression to a merged partition.
///
/// - Partition-level deletion suppresses rows with `primary_key_liveness.timestamp`
///   older than the deletion timestamp.
/// - Row-level deletion suppresses cells with timestamps older than the row
///   deletion timestamp.
/// - Partition deletion also applies to the static row.
pub(crate) fn apply_deletions(partition: &mut Partition) {
    let partition_delete_at = partition.deletion.marked_for_delete_at;

    if !partition.deletion.is_live() {
        // Suppress rows older than partition deletion
        partition
            .rows
            .retain(|row| row.primary_key_liveness.timestamp >= partition_delete_at);

        // Apply partition deletion to static row
        if let Some(ref mut static_row) = partition.static_row {
            static_row
                .cells
                .retain(|(_col, cell)| cell.timestamp >= partition_delete_at);
            if static_row.cells.is_empty() {
                partition.static_row = None;
            }
        }
    }

    // Apply row-level deletions to cells within each row
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
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_cell(col: u16, value: &[u8], ts: i64) -> (u16, CellValue) {
        (col, CellValue::live(value.to_vec(), ts))
    }

    fn make_row_with_clustering(clustering: &[u8], cells: Vec<(u16, CellValue)>, ts: i64) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn make_partition(key: &str, rows: Vec<Row>) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    /// t_a0f922a3: `merge_partitions` leaves a single-source partition in its
    /// on-disk order (a legacy SSTable can store rows non-monotonically).
    /// `ensure_partition_rows_sorted` re-sorts it so a compaction rewrite emits
    /// monotonic output, and is a no-op for already-sorted rows.
    #[test]
    fn ensure_partition_rows_sorted_fixes_misordered_single_source() {
        // Rows out of clustering order (c5 misplaced to the front), exactly the
        // legacy-SSTable shape. merge_partitions of this single source keeps the
        // bad order:
        let misordered = make_partition(
            "k1",
            vec![
                make_row_with_clustering(b"c5", vec![make_cell(0, b"v", 5)], 5),
                make_row_with_clustering(b"c1", vec![make_cell(0, b"v", 1)], 1),
                make_row_with_clustering(b"c2", vec![make_cell(0, b"v", 2)], 2),
                make_row_with_clustering(b"c3", vec![make_cell(0, b"v", 3)], 3),
                make_row_with_clustering(b"c4", vec![make_cell(0, b"v", 4)], 4),
            ],
        );
        let mut merged = merge_partitions(vec![misordered]);
        let before: Vec<Vec<u8>> = merged.rows.iter().map(|r| r.clustering.clone()).collect();
        assert_eq!(
            before,
            vec![
                b"c5".to_vec(),
                b"c1".to_vec(),
                b"c2".to_vec(),
                b"c3".to_vec(),
                b"c4".to_vec()
            ],
            "precondition: merge_partitions leaves the single source unsorted"
        );

        ensure_partition_rows_sorted(&mut merged);
        let after: Vec<Vec<u8>> = merged.rows.iter().map(|r| r.clustering.clone()).collect();
        assert_eq!(
            after,
            vec![
                b"c1".to_vec(),
                b"c2".to_vec(),
                b"c3".to_vec(),
                b"c4".to_vec(),
                b"c5".to_vec()
            ],
            "ensure_partition_rows_sorted must produce strictly increasing clustering order"
        );

        // Idempotent: a second call changes nothing.
        ensure_partition_rows_sorted(&mut merged);
        let again: Vec<Vec<u8>> = merged.rows.iter().map(|r| r.clustering.clone()).collect();
        assert_eq!(again, after, "re-sorting already-sorted rows is a no-op");
    }

    #[test]
    fn single_source_passthrough() {
        let p = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"v1", 1000)],
                1000,
            )],
        );
        let merged = merge_partitions(vec![p.clone()]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells.len(), 1);
        assert_eq!(merged.rows[0].cells[0].1.timestamp, 1000);
    }

    #[test]
    fn merge_rows_keeps_distinct_path_complex_cells() {
        // Two per-element cells of the SAME collection column (col 0) with
        // DIFFERENT cell paths must BOTH survive a cross-SSTable merge — they are
        // distinct collection elements, not competing writes to one cell. Keying
        // the merge by column index alone would collapse them and destroy the
        // collection.
        let a = Row {
            clustering: b"c1".to_vec(),
            cells: vec![(
                0,
                CellValue::live(b"a".to_vec(), 10).with_path(b"pA".to_vec()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(10),
        };
        let b = Row {
            clustering: b"c1".to_vec(),
            cells: vec![(
                0,
                CellValue::live(b"b".to_vec(), 11).with_path(b"pB".to_vec()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(11),
        };
        let merged = merge_rows(a, b);
        assert_eq!(
            merged.cells.len(),
            2,
            "both distinct-path elements retained"
        );
        assert_eq!(merged.cells[0].1.path.as_deref(), Some(b"pA".as_slice()));
        assert_eq!(merged.cells[1].1.path.as_deref(), Some(b"pB".as_slice()));
    }

    #[test]
    fn merge_rows_reconciles_same_path_complex_cell_lww() {
        // Same (col, path) from two sources IS a competing write — LWW applies.
        let a = Row {
            clustering: b"c1".to_vec(),
            cells: vec![(
                0,
                CellValue::live(b"old".to_vec(), 10).with_path(b"pA".to_vec()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(10),
        };
        let b = Row {
            clustering: b"c1".to_vec(),
            cells: vec![(
                0,
                CellValue::live(b"new".to_vec(), 20).with_path(b"pA".to_vec()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(20),
        };
        let merged = merge_rows(a, b);
        assert_eq!(merged.cells.len(), 1, "same (col,path) reconciled");
        assert_eq!(merged.cells[0].1.value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn cell_level_lww_newer_wins() {
        let p1 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"old", 1000)],
                1000,
            )],
        );
        let p2 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"new", 2000)],
                2000,
            )],
        );
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells.len(), 1);
        assert_eq!(
            merged.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
        assert_eq!(merged.rows[0].cells[0].1.timestamp, 2000);
    }

    #[test]
    fn cell_level_lww_is_commutative() {
        let p1 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"old", 1000)],
                1000,
            )],
        );
        let p2 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"new", 2000)],
                2000,
            )],
        );

        let merged_ab = merge_partitions(vec![p1.clone(), p2.clone()]);
        let merged_ba = merge_partitions(vec![p2, p1]);

        assert_eq!(
            merged_ab.rows[0].cells[0].1.value,
            merged_ba.rows[0].cells[0].1.value
        );
        assert_eq!(
            merged_ab.rows[0].cells[0].1.timestamp,
            merged_ba.rows[0].cells[0].1.timestamp
        );
    }

    #[test]
    fn disjoint_rows_concatenate() {
        let p1 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"v1", 1000)],
                1000,
            )],
        );
        let p2 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c2",
                vec![make_cell(0, b"v2", 2000)],
                2000,
            )],
        );
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 2);
        assert_eq!(merged.rows[0].clustering, b"c1");
        assert_eq!(merged.rows[1].clustering, b"c2");
    }

    #[test]
    fn disjoint_cells_merge_within_same_row() {
        let p1 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"v0", 1000)],
                1000,
            )],
        );
        let p2 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(1, b"v1", 2000)],
                2000,
            )],
        );
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells.len(), 2);
        assert_eq!(merged.rows[0].cells[0].0, 0);
        assert_eq!(merged.rows[0].cells[1].0, 1);
    }

    #[test]
    fn row_deletion_suppresses_older_cells() {
        let p1 = make_partition(
            "k1",
            vec![make_row_with_clustering(
                b"c1",
                vec![make_cell(0, b"v1", 1000)],
                1000,
            )],
        );
        let p2 = make_partition(
            "k1",
            vec![Row {
                clustering: b"c1".to_vec(),
                cells: vec![],
                deletion: DeletionTime::new(2000, 100),
                primary_key_liveness: LivenessInfo::NONE,
            }],
        );
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert!(
            merged.rows[0].cells.is_empty(),
            "cells should be suppressed"
        );
    }

    #[test]
    fn partition_deletion_suppresses_all_rows() {
        let mut p = make_partition(
            "k1",
            vec![
                make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
                make_row_with_clustering(b"c2", vec![make_cell(0, b"v2", 1000)], 1000),
            ],
        );
        p.deletion = DeletionTime::new(2000, 100);
        let merged = merge_partitions(vec![p]);
        assert!(merged.rows.is_empty(), "all rows should be suppressed");
    }

    #[test]
    fn partition_deletion_keeps_newer_rows() {
        let mut p = make_partition(
            "k1",
            vec![
                make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
                make_row_with_clustering(b"c2", vec![make_cell(0, b"v2", 3000)], 3000),
            ],
        );
        p.deletion = DeletionTime::new(2000, 100);
        let merged = merge_partitions(vec![p]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].clustering, b"c2");
    }

    #[test]
    fn static_row_merge_one_sided() {
        let mut p1 = make_partition("k1", vec![]);
        p1.static_row = Some(Row {
            clustering: vec![],
            cells: vec![make_cell(0, b"static_val", 1000)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        });
        let p2 = make_partition("k1", vec![]);

        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.static_row.is_some());
        assert_eq!(merged.static_row.as_ref().unwrap().cells.len(), 1);
    }

    #[test]
    fn static_row_merge_two_sided_lww() {
        let mut p1 = make_partition("k1", vec![]);
        p1.static_row = Some(Row {
            clustering: vec![],
            cells: vec![make_cell(0, b"old_static", 1000)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        });

        let mut p2 = make_partition("k1", vec![]);
        p2.static_row = Some(Row {
            clustering: vec![],
            cells: vec![make_cell(0, b"new_static", 2000)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        });

        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.static_row.is_some());
        let static_row = merged.static_row.as_ref().unwrap();
        assert_eq!(static_row.cells.len(), 1);
        assert_eq!(
            static_row.cells[0].1.value.as_deref(),
            Some(b"new_static".as_slice())
        );
    }

    #[test]
    #[should_panic(expected = "merge_partitions requires at least one source")]
    fn empty_inputs() {
        merge_partitions(vec![]);
    }
}
