//! Module: Tombstone purge for compaction output (`gc_grace_seconds`).
//! Correctness: Correct when a deletion marker is dropped only if BOTH (a) its
//!   `local_deletion_time` is older than `gc_before` (the grace period elapsed) AND
//!   (b) its timestamp is below `max_purgeable_timestamp` (no data outside this
//!   compaction can be older than it), no live cell is ever removed, and dropping a
//!   marker never lets a cell this compaction kept shadowed become visible again.
//! Last revised: 2026-09-26
//! Last changed: New module — compaction kept every partition/row/cell tombstone
//!   forever because `gc_grace_seconds` was stored in the schema but never read.

use std::collections::HashSet;

use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use super::metadata::SSTableMetadata;

/// When a deletion marker may be dropped from compaction output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PurgePolicy {
    /// A marker is past its grace period when `local_deletion_time < gc_before`
    /// (seconds since epoch; `now - gc_grace_seconds`).
    pub gc_before: i64,
    /// A marker is safe to drop only when its timestamp is `<` this (microseconds).
    /// The minimum timestamp of any data outside the compaction that could overlap:
    /// dropping a tombstone newer than that data would resurrect it.
    pub max_purgeable_timestamp: i64,
}

impl PurgePolicy {
    /// Whether a marker with this timestamp / local deletion time may be dropped.
    pub fn purgeable(&self, timestamp: i64, local_deletion_time: i64) -> bool {
        local_deletion_time < self.gc_before && timestamp < self.max_purgeable_timestamp
    }

    fn purgeable_deletion(&self, deletion: &DeletionTime) -> bool {
        !deletion.is_live()
            && self.purgeable(
                deletion.marked_for_delete_at,
                i64::from(deletion.local_deletion_time),
            )
    }
}

/// The largest timestamp a tombstone may have and still be dropped: the smallest
/// timestamp of any data outside the compaction that could sit under it.
///
/// `others` are the table's SSTables NOT in `inputs`. Only those whose token range
/// overlaps the inputs' combined range can hold shadowed data. `unflushed_min` is
/// the minimum timestamp in the active and flushing memtables. A legacy-format
/// SSTable's timestamp bounds are not trusted, so an overlapping one blocks
/// purging entirely.
pub fn max_purgeable_timestamp(
    inputs: &[SSTableMetadata],
    others: &[SSTableMetadata],
    unflushed_min: i64,
) -> i64 {
    let Some(low) = inputs.iter().map(|s| s.min_token).min() else {
        // No inputs means no compaction; refuse to purge rather than guess a range.
        return i64::MIN;
    };
    let high = inputs.iter().map(|s| s.max_token).max().unwrap_or(low);
    others
        .iter()
        .filter(|s| s.min_token <= high && s.max_token >= low)
        .map(|s| {
            if s.legacy_format {
                i64::MIN
            } else {
                s.min_timestamp
            }
        })
        .fold(unflushed_min, i64::min)
}

/// Build the policy for a compaction at `now_secs` for a table with
/// `gc_grace_seconds`. `gc_before` may be negative when the grace period is longer
/// than the clock; nothing is then old enough to purge.
pub fn policy_for(
    now_secs: i64,
    gc_grace_seconds: u32,
    max_purgeable_timestamp: i64,
) -> PurgePolicy {
    PurgePolicy {
        gc_before: now_secs - i64::from(gc_grace_seconds),
        max_purgeable_timestamp,
    }
}

/// What a purge pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeStats {
    pub partition_deletions: u64,
    pub row_deletions: u64,
    pub cell_tombstones: u64,
    pub rows_removed: u64,
}

impl PurgeStats {
    /// Total markers dropped.
    pub fn markers(&self) -> u64 {
        self.partition_deletions + self.row_deletions + self.cell_tombstones
    }
}

/// Drop every purgeable deletion marker from `partition` (already merged, with
/// shadowed data already removed by `merge::apply_deletions`).
pub fn purge_partition(partition: &mut Partition, policy: &PurgePolicy) -> PurgeStats {
    let mut stats = PurgeStats::default();
    if policy.purgeable_deletion(&partition.deletion) {
        // Rows and static cells it shadowed were already removed by the merge, and
        // the overlap guard says nothing older survives outside this compaction.
        partition.deletion = DeletionTime::LIVE;
        stats.partition_deletions += 1;
    }
    if let Some(static_row) = partition.static_row.as_mut() {
        if purge_row(static_row, policy, &mut stats) && static_row.cells.is_empty() {
            partition.static_row = None;
        }
    }
    let rows_before = partition.rows.len();
    partition.rows.retain_mut(|row| {
        let changed = purge_row(row, policy, &mut stats);
        !(changed && row_is_vacant(row))
    });
    stats.rows_removed += (rows_before - partition.rows.len()) as u64;
    stats
}

/// A row with no cells, no deletion and no primary-key liveness carries nothing.
fn row_is_vacant(row: &Row) -> bool {
    row.cells.is_empty() && row.deletion.is_live() && !row.primary_key_liveness.has_timestamp()
}

/// Purge one row's markers. Returns whether anything was removed.
fn purge_row(row: &mut Row, policy: &PurgePolicy, stats: &mut PurgeStats) -> bool {
    let before = stats.markers();

    if policy.purgeable_deletion(&row.deletion) {
        let deleted_at = row.deletion.marked_for_delete_at;
        row.deletion = DeletionTime::LIVE;
        stats.row_deletions += 1;
        // The deletion shadowed any liveness at or below its timestamp. Dropping
        // only the marker would leave that liveness and resurrect an empty row.
        if row.primary_key_liveness.has_timestamp()
            && row.primary_key_liveness.timestamp < deleted_at
        {
            row.primary_key_liveness = LivenessInfo::NONE;
        }
    }

    // A pathless tombstone on a column shadows that column's path-keyed element
    // cells only at read time (`merge_rows` keys cells by `(column, path)`), so it
    // is kept while any such element remains.
    let columns_with_elements: HashSet<u16> = row
        .cells
        .iter()
        .filter(|(_, cell)| cell.path.is_some())
        .map(|(column, _)| *column)
        .collect();
    row.cells.retain(|(column, cell)| {
        let droppable = cell.is_tombstone()
            && policy.purgeable(cell.timestamp, i64::from(cell.local_deletion_time))
            && !(cell.path.is_none() && columns_with_elements.contains(column));
        if droppable {
            stats.cell_tombstones += 1;
        }
        !droppable
    });

    stats.markers() != before
}

/// Cheap pre-check: does `partition` hold any marker [`purge_partition`] could
/// drop? Lets the caller skip the purge (and any defensive copy) for the common
/// partition that has none. May over-report (a pathless collection tombstone that
/// the purge would keep still counts); never under-reports.
pub fn has_purgeable_marker(partition: &Partition, policy: &PurgePolicy) -> bool {
    let row_has_marker = |row: &Row| {
        policy.purgeable_deletion(&row.deletion)
            || row.cells.iter().any(|(_, cell)| {
                cell.is_tombstone()
                    && policy.purgeable(cell.timestamp, i64::from(cell.local_deletion_time))
            })
    };
    policy.purgeable_deletion(&partition.deletion)
        || partition.static_row.as_ref().is_some_and(row_has_marker)
        || partition.rows.iter().any(row_has_marker)
}

/// True when nothing is left to write for `partition`: no deletion, no static row,
/// no rows.
pub fn is_empty_partition(partition: &Partition) -> bool {
    partition.deletion.is_live() && partition.static_row.is_none() && partition.rows.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use proptest::prelude::*;

    const NOW: i64 = 2_000_000_000;
    /// Anything deleted before this second is past its grace period.
    const GC_BEFORE: i64 = NOW - 864_000;
    const OLD_LDT: i32 = (GC_BEFORE - 1_000) as i32;
    const FRESH_LDT: i32 = (GC_BEFORE + 1_000) as i32;

    fn policy(max_purgeable_timestamp: i64) -> PurgePolicy {
        PurgePolicy {
            gc_before: GC_BEFORE,
            max_purgeable_timestamp,
        }
    }

    fn key() -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(b"k".to_vec()))
    }

    fn partition(deletion: DeletionTime, rows: Vec<Row>) -> Partition {
        Partition {
            key: key(),
            deletion,
            static_row: None,
            rows,
        }
    }

    fn row(clustering: &[u8], cells: Vec<(u16, CellValue)>, liveness_ts: i64) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: if liveness_ts == i64::MIN {
                LivenessInfo::NONE
            } else {
                LivenessInfo::with_timestamp(liveness_ts)
            },
        }
    }

    #[test]
    fn purgeable_needs_both_the_grace_period_and_the_overlap_guard() {
        let p = policy(1_000);
        assert!(p.purgeable(999, OLD_LDT as i64), "old and below the guard");
        assert!(
            !p.purgeable(1_000, OLD_LDT as i64),
            "ts == guard: not below"
        );
        assert!(!p.purgeable(999, FRESH_LDT as i64), "still in grace");
        assert!(!p.purgeable(999, GC_BEFORE), "ldt == gc_before: not before");
        assert!(!p.purgeable(2_000, FRESH_LDT as i64));
    }

    #[test]
    fn an_old_partition_deletion_is_dropped() {
        let mut p = partition(DeletionTime::new(500, OLD_LDT as u32), vec![]);
        let stats = purge_partition(&mut p, &policy(1_000));
        assert!(p.deletion.is_live());
        assert_eq!(stats.partition_deletions, 1);
        assert!(is_empty_partition(&p));
    }

    #[test]
    fn a_partition_deletion_inside_grace_or_above_the_guard_is_kept() {
        let inside_grace = DeletionTime::new(500, FRESH_LDT as u32);
        let mut a = partition(inside_grace, vec![]);
        assert_eq!(purge_partition(&mut a, &policy(1_000)).markers(), 0);
        assert_eq!(a.deletion, inside_grace);

        let above_guard = DeletionTime::new(1_500, OLD_LDT as u32);
        let mut b = partition(above_guard, vec![]);
        assert_eq!(purge_partition(&mut b, &policy(1_000)).markers(), 0);
        assert_eq!(b.deletion, above_guard);
    }

    #[test]
    fn a_purged_row_deletion_also_clears_the_liveness_it_shadowed() {
        // Row deleted at ts 500; its INSERT liveness (ts 100) is older. Dropping only
        // the deletion would leave the liveness and resurrect an empty live row.
        let mut r = row(b"c1", vec![], 100);
        r.deletion = DeletionTime::new(500, OLD_LDT as u32);
        let mut p = partition(DeletionTime::LIVE, vec![r]);
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(stats.row_deletions, 1);
        assert_eq!(stats.rows_removed, 1, "nothing left of the row");
        assert!(p.rows.is_empty());
    }

    #[test]
    fn a_row_reinserted_after_its_deletion_keeps_its_liveness_and_cells() {
        let mut r = row(b"c1", vec![(0, CellValue::live(b"v".to_vec(), 700))], 700);
        r.deletion = DeletionTime::new(500, OLD_LDT as u32);
        let mut p = partition(DeletionTime::LIVE, vec![r]);
        purge_partition(&mut p, &policy(1_000));
        assert_eq!(p.rows.len(), 1);
        assert!(p.rows[0].deletion.is_live());
        assert_eq!(p.rows[0].primary_key_liveness.timestamp, 700);
        assert_eq!(p.rows[0].cells.len(), 1);
    }

    #[test]
    fn purgeable_cell_tombstones_go_and_live_cells_stay() {
        let cells = vec![
            (0, CellValue::live(b"keep".to_vec(), 900)),
            (1, CellValue::tombstone(500, OLD_LDT)),
            (2, CellValue::tombstone(500, FRESH_LDT)),
            (3, CellValue::tombstone(1_500, OLD_LDT)),
        ];
        let mut p = partition(DeletionTime::LIVE, vec![row(b"c1", cells, 900)]);
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(stats.cell_tombstones, 1);
        let kept: Vec<u16> = p.rows[0].cells.iter().map(|(c, _)| *c).collect();
        assert_eq!(kept, vec![0, 2, 3]);
    }

    #[test]
    fn a_row_emptied_by_the_purge_is_removed_but_a_bare_insert_is_not() {
        let only_tombstone = row(
            b"c1",
            vec![(1, CellValue::tombstone(500, OLD_LDT))],
            i64::MIN,
        );
        let bare_insert = row(b"c2", vec![], 800);
        let mut p = partition(DeletionTime::LIVE, vec![only_tombstone, bare_insert]);
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(stats.rows_removed, 1);
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.rows[0].clustering, b"c2".to_vec());
    }

    #[test]
    fn static_row_tombstones_are_purged_and_an_empty_static_row_is_dropped() {
        let mut p = partition(DeletionTime::LIVE, vec![]);
        p.static_row = Some(row(
            b"",
            vec![(0, CellValue::tombstone(500, OLD_LDT))],
            i64::MIN,
        ));
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(stats.cell_tombstones, 1);
        assert!(p.static_row.is_none());
    }

    #[test]
    fn a_collection_clear_tombstone_is_kept_while_element_cells_it_shadows_remain() {
        // A pathless tombstone on a column shadows that column's path-keyed element
        // cells only at read time. Dropping it would resurrect them.
        let clear = (4, CellValue::tombstone(500, OLD_LDT));
        let element = (
            4,
            CellValue::live(b"e".to_vec(), 400).with_path(b"p".to_vec()),
        );
        let mut p = partition(
            DeletionTime::LIVE,
            vec![row(b"c1", vec![clear, element], 400)],
        );
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(stats.cell_tombstones, 0);
        assert_eq!(p.rows[0].cells.len(), 2);
    }

    fn sst(id: &str, min_token: i64, max_token: i64, min_ts: i64, legacy: bool) -> SSTableMetadata {
        SSTableMetadata {
            id: id.to_string(),
            path: std::path::PathBuf::from(format!("/t/{id}")),
            size_bytes: 1,
            min_token,
            max_token,
            min_timestamp: min_ts,
            max_timestamp: min_ts + 10,
            partition_count: 1,
            legacy_format: legacy,
        }
    }

    #[test]
    fn the_guard_is_the_unflushed_minimum_when_no_other_sstable_overlaps() {
        let inputs = [sst("a", 0, 100, 500, false), sst("b", 50, 200, 600, false)];
        assert_eq!(max_purgeable_timestamp(&inputs, &[], i64::MAX), i64::MAX);
        assert_eq!(max_purgeable_timestamp(&inputs, &[], 777), 777);
        // Disjoint on both sides of the inputs' combined range [0, 200].
        let far = [sst("x", 201, 300, 1, false), sst("y", -50, -1, 2, false)];
        assert_eq!(max_purgeable_timestamp(&inputs, &far, i64::MAX), i64::MAX);
    }

    #[test]
    fn an_overlapping_sstable_outside_the_compaction_lowers_the_guard() {
        let inputs = [sst("a", 0, 100, 500, false), sst("b", 50, 200, 600, false)];
        let others = [
            sst("o1", 150, 400, 300, false),
            sst("o2", 0, 5, 450, false),
            sst("far", 900, 950, 1, false),
        ];
        assert_eq!(max_purgeable_timestamp(&inputs, &others, i64::MAX), 300);
        assert_eq!(max_purgeable_timestamp(&inputs, &others, 100), 100);
    }

    #[test]
    fn range_overlap_is_inclusive_at_the_edges() {
        let inputs = [sst("a", 10, 20, 500, false)];
        assert_eq!(
            max_purgeable_timestamp(&inputs, &[sst("o", 20, 30, 42, false)], i64::MAX),
            42
        );
        assert_eq!(
            max_purgeable_timestamp(&inputs, &[sst("o", 0, 10, 43, false)], i64::MAX),
            43
        );
    }

    #[test]
    fn an_overlapping_legacy_sstable_blocks_purging_outright() {
        let inputs = [sst("a", 0, 100, 500, false)];
        let others = [sst("legacy", 50, 60, 9_999, true)];
        assert_eq!(
            max_purgeable_timestamp(&inputs, &others, i64::MAX),
            i64::MIN
        );
        // A non-overlapping legacy file does not.
        let far = [sst("legacy", 500, 600, 9_999, true)];
        assert_eq!(max_purgeable_timestamp(&inputs, &far, i64::MAX), i64::MAX);
    }

    #[test]
    fn policy_for_puts_gc_before_a_grace_period_in_the_past() {
        let p = policy_for(2_000_000_000, 864_000, 55);
        assert_eq!(p.gc_before, 2_000_000_000 - 864_000);
        assert_eq!(p.max_purgeable_timestamp, 55);
        // A grace period longer than the clock does not wrap: nothing is old enough.
        assert!(policy_for(100, u32::MAX, 55).gc_before < 0);
        assert_eq!(policy_for(100, 0, 55).gc_before, 100);
    }

    #[test]
    fn has_purgeable_marker_finds_each_marker_kind_and_only_those() {
        let pol = policy(1_000);
        let live = partition(
            DeletionTime::LIVE,
            vec![row(
                b"c",
                vec![(0, CellValue::live(b"v".to_vec(), 900))],
                900,
            )],
        );
        assert!(!has_purgeable_marker(&live, &pol));

        assert!(has_purgeable_marker(
            &partition(DeletionTime::new(500, OLD_LDT as u32), vec![]),
            &pol
        ));
        let mut row_del = row(b"c", vec![], 100);
        row_del.deletion = DeletionTime::new(500, OLD_LDT as u32);
        assert!(has_purgeable_marker(
            &partition(DeletionTime::LIVE, vec![row_del]),
            &pol
        ));
        let cell_tomb = row(
            b"c",
            vec![(1, CellValue::tombstone(500, OLD_LDT))],
            i64::MIN,
        );
        assert!(has_purgeable_marker(
            &partition(DeletionTime::LIVE, vec![cell_tomb]),
            &pol
        ));
        let mut static_tomb = partition(DeletionTime::LIVE, vec![]);
        static_tomb.static_row = Some(row(
            b"",
            vec![(0, CellValue::tombstone(500, OLD_LDT))],
            i64::MIN,
        ));
        assert!(has_purgeable_marker(&static_tomb, &pol));

        // Markers inside grace or at/above the guard do not count.
        let fresh = partition(DeletionTime::new(500, FRESH_LDT as u32), vec![]);
        assert!(!has_purgeable_marker(&fresh, &pol));
        let above = partition(DeletionTime::new(1_500, OLD_LDT as u32), vec![]);
        assert!(!has_purgeable_marker(&above, &pol));
    }

    #[test]
    fn purging_twice_changes_nothing_more() {
        let cells = vec![
            (0, CellValue::live(b"v".to_vec(), 900)),
            (1, CellValue::tombstone(500, OLD_LDT)),
        ];
        let mut p = partition(
            DeletionTime::new(300, OLD_LDT as u32),
            vec![row(b"c", cells, 900)],
        );
        purge_partition(&mut p, &policy(1_000));
        let after_once = p.clone();
        let again = purge_partition(&mut p, &policy(1_000));
        assert_eq!(again, PurgeStats::default());
        assert_eq!(p, after_once);
    }

    fn arb_cell() -> impl Strategy<Value = CellValue> {
        (
            any::<bool>(),
            0i64..2_000,
            prop::sample::select(vec![OLD_LDT, FRESH_LDT]),
        )
            .prop_map(|(live, ts, ldt)| {
                if live {
                    CellValue::live(b"v".to_vec(), ts)
                } else {
                    CellValue::tombstone(ts, ldt)
                }
            })
    }

    fn arb_row() -> impl Strategy<Value = Row> {
        (
            0u8..6,
            prop::collection::vec((0u16..4, arb_cell()), 0..5),
            prop::option::of(0i64..2_000),
            prop::option::of((0i64..2_000, prop::sample::select(vec![OLD_LDT, FRESH_LDT]))),
        )
            .prop_map(|(c, cells, liveness, del)| {
                let mut r = row(&[c], cells, liveness.unwrap_or(i64::MIN));
                if let Some((ts, ldt)) = del {
                    r.deletion = DeletionTime::new(ts, ldt as u32);
                }
                r
            })
    }

    proptest! {
        /// Safety: a purge never removes a live cell, never drops a marker that is
        /// inside its grace period or at/above the overlap guard, and is idempotent.
        #[test]
        fn purge_is_safe_and_idempotent(
            rows in prop::collection::vec(arb_row(), 0..6),
            guard in 0i64..2_000,
        ) {
            let pol = policy(guard);
            let mut p = partition(DeletionTime::LIVE, rows);
            p.rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
            p.rows.dedup_by(|a, b| a.clustering == b.clustering);
            let before = p.clone();
            purge_partition(&mut p, &pol);

            let live_before: Vec<_> = before.rows.iter()
                .flat_map(|r| r.cells.iter().filter(|(_, c)| !c.is_tombstone()).map(|(i, c)| (r.clustering.clone(), *i, c.clone())))
                .collect();
            let live_after: Vec<_> = p.rows.iter()
                .flat_map(|r| r.cells.iter().filter(|(_, c)| !c.is_tombstone()).map(|(i, c)| (r.clustering.clone(), *i, c.clone())))
                .collect();
            prop_assert_eq!(live_before, live_after, "live cells must survive");

            for r in &before.rows {
                for (i, c) in r.cells.iter().filter(|(_, c)| c.is_tombstone()) {
                    if !pol.purgeable(c.timestamp, c.local_deletion_time as i64) {
                        let survived = p.rows.iter().any(|x| x.clustering == r.clustering
                            && x.cells.iter().any(|(j, d)| j == i && d == c));
                        prop_assert!(survived, "unpurgeable tombstone dropped");
                    }
                }
                if !r.deletion.is_live()
                    && !pol.purgeable(r.deletion.marked_for_delete_at, r.deletion.local_deletion_time as i64)
                {
                    let survived = p.rows.iter().any(|x| x.clustering == r.clustering && x.deletion == r.deletion);
                    prop_assert!(survived, "unpurgeable row deletion dropped");
                }
            }

            let once = p.clone();
            purge_partition(&mut p, &pol);
            prop_assert_eq!(p, once, "second pass must be a no-op");
        }
    }
}
