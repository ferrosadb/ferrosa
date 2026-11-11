//! Last revised: 2026-11-11
//! Last changed: Expired TTL cells and row liveness purge like tombstones (ST-105);
//!   `PurgedRow` gives the read digest a borrowed, copy-free view (CL-80), and
//!   `repair_guard` judges what a read repair target can drop itself (CL-81).
//! Correctness: Collection sentinel and purge boundary tests.
//!
//! Module: Tombstone purge for compaction output (`gc_grace_seconds`).
//! Correctness: Correct when a deletion marker is dropped only if BOTH (a) its
//!   `local_deletion_time` is older than `gc_before` (the grace period elapsed) AND
//!   (b) its timestamp is below `max_purgeable_timestamp` (no data outside this
//!   compaction can be older than it), no live cell is ever removed, and dropping a
//!   marker never lets a cell this compaction kept shadowed become visible again.
//! Last revised: 2026-09-26
//! Last changed: New module — compaction kept every partition/row/cell tombstone
//!   forever because `gc_grace_seconds` was stored in the schema but never read.

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
    /// The table-level tombstone in force when this compaction was planned
    /// (microseconds), or [`DeletionTime::LIVE`]. Compaction is where a truncated
    /// table's rows are physically reclaimed: every merged partition adopts this
    /// watermark before it is written out, so rows older than the truncate are
    /// dropped. It is carried on the policy so the whole table is reclaimed by the
    /// NEXT compaction of any of its data (the logical effect stays immediate
    /// regardless — reads suppress immediately). The reserved table-tombstone
    /// partition itself is exempt from purge (see `compaction::executor`).
    pub table_delete: DeletionTime,
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
    table_delete: DeletionTime,
) -> PurgePolicy {
    PurgePolicy {
        gc_before: now_secs - i64::from(gc_grace_seconds),
        max_purgeable_timestamp,
        table_delete,
    }
}

/// What a purge pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeStats {
    pub partition_deletions: u64,
    pub row_deletions: u64,
    pub cell_tombstones: u64,
    /// Expired TTL cells dropped.
    pub expired_cells: u64,
    /// Expired TTL row liveness (`INSERT ... USING TTL` row markers) dropped.
    pub expired_liveness: u64,
    pub rows_removed: u64,
    /// The newest timestamp of anything dropped, or `None` when nothing was. A
    /// replica that leaves purged data out of a read response reports it, so
    /// the coordinator can tell whether another replica's older live data sits
    /// under what was left out (CL-82).
    pub newest_dropped: Option<i64>,
}

impl PurgeStats {
    fn dropped(&mut self, timestamp: i64) {
        self.newest_dropped = Some(self.newest_dropped.map_or(timestamp, |n| n.max(timestamp)));
    }

    /// Total markers dropped, expired TTL data included.
    pub fn markers(&self) -> u64 {
        self.partition_deletions
            + self.row_deletions
            + self.cell_tombstones
            + self.expired_cells
            + self.expired_liveness
    }
}

/// Drop every purgeable deletion marker from `partition` (already merged, with
/// shadowed data already removed by `merge::apply_deletions`).
pub fn purge_partition(partition: &mut Partition, policy: &PurgePolicy) -> PurgeStats {
    let mut stats = PurgeStats::default();
    if policy.purgeable_deletion(&partition.deletion) {
        // Rows and static cells it shadowed were already removed by the merge, and
        // the overlap guard says nothing older survives outside this compaction.
        stats.dropped(partition.deletion.marked_for_delete_at);
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
pub(crate) fn row_is_vacant(row: &Row) -> bool {
    row.cells.is_empty() && row.deletion.is_live() && !row.primary_key_liveness.has_timestamp()
}

/// What a purge leaves of one row's deletion and primary-key liveness.
#[derive(Clone, Copy)]
struct RowMarkers {
    deletion: DeletionTime,
    liveness: LivenessInfo,
    deletion_dropped: bool,
    liveness_expired: bool,
}

impl RowMarkers {
    fn for_row(row: &Row, policy: &PurgePolicy) -> Self {
        let mut deletion = row.deletion;
        let mut liveness = row.primary_key_liveness;
        let deletion_dropped = policy.purgeable_deletion(&row.deletion);
        if deletion_dropped {
            deletion = DeletionTime::LIVE;
            // The deletion shadowed any liveness at or below its timestamp. Dropping
            // only the marker would leave that liveness and resurrect an empty row.
            if liveness.has_timestamp() && liveness.timestamp < row.deletion.marked_for_delete_at {
                liveness = LivenessInfo::NONE;
            }
        }
        let liveness_expired = liveness_is_purgeable(&liveness, policy);
        if liveness_expired {
            liveness = LivenessInfo::NONE;
        }
        Self {
            deletion,
            liveness,
            deletion_dropped,
            liveness_expired,
        }
    }
}

/// Which of one row's cells a purge drops.
struct CellRule {
    policy: PurgePolicy,
    /// Columns holding a path-keyed element cell. A pathless tombstone on such a
    /// column shadows those elements only at read time (`merge_rows` keys cells by
    /// `(column, path)`), so it is kept while any element remains. Column ordinals
    /// are u16: this fixed 8 KiB bitmap covers every valid ordinal, including
    /// unsorted callers, without a per-row hash allocation.
    columns_with_elements: [u64; 1024],
}

impl CellRule {
    fn for_row(row: &Row, policy: &PurgePolicy) -> Self {
        let mut columns_with_elements = [0u64; 1024];
        for (column, cell) in &row.cells {
            if cell.path.is_some() {
                columns_with_elements[usize::from(*column) / 64] |= 1 << (*column % 64);
            }
        }
        Self {
            policy: *policy,
            columns_with_elements,
        }
    }

    fn drops(&self, column: u16, cell: &ferrosa_common::CellValue) -> bool {
        let has_elements =
            self.columns_with_elements[usize::from(column) / 64] & (1 << (column % 64)) != 0;
        cell_is_purgeable(cell, &self.policy) && !(cell.path.is_none() && has_elements)
    }
}

/// Purge one row's markers. Returns whether anything was removed.
pub(crate) fn purge_row(row: &mut Row, policy: &PurgePolicy, stats: &mut PurgeStats) -> bool {
    let before = stats.markers();

    let markers = RowMarkers::for_row(row, policy);
    if markers.deletion_dropped {
        stats.dropped(row.deletion.marked_for_delete_at);
    }
    if row.primary_key_liveness.has_timestamp() && !markers.liveness.has_timestamp() {
        stats.dropped(row.primary_key_liveness.timestamp);
    }
    row.deletion = markers.deletion;
    row.primary_key_liveness = markers.liveness;
    stats.row_deletions += u64::from(markers.deletion_dropped);
    stats.expired_liveness += u64::from(markers.liveness_expired);

    let rule = CellRule::for_row(row, policy);
    row.cells.retain(|(column, cell)| {
        let droppable = rule.drops(*column, cell);
        if droppable {
            stats.dropped(cell.timestamp);
            if cell.is_tombstone() {
                stats.cell_tombstones += 1;
            } else {
                stats.expired_cells += 1;
            }
        }
        !droppable
    });

    stats.markers() != before
}

/// A cell compaction may drop: a tombstone, or an expiring cell, whose local
/// deletion time is past the grace window and whose timestamp is below the
/// overlap guard.
///
/// An expired TTL cell is invisible but still carries its value, so a rule that
/// looked only at `is_tombstone()` kept it forever, and a partition written one
/// TTL'd row per event grew with every event (Lean:
/// `Tombstone.expired_history_grows_without_bound`). Dropping one under these two
/// conditions cannot change any read: every other version of the cell that
/// survives outside this compaction is strictly newer and wins the merge anyway
/// (`Tombstone.purging_expired_data_is_invisible_to_reads`). The cell is dropped,
/// not rewritten as a tombstone: the reconcile order is expiry-blind, so a
/// tombstone would beat an equal-timestamp live value the expired cell loses to
/// (`Tombstone.rewriting_expired_data_as_a_tombstone_changes_a_tie`).
fn cell_is_purgeable(cell: &ferrosa_common::CellValue, policy: &PurgePolicy) -> bool {
    (cell.is_tombstone() || cell.is_expiring())
        && policy.purgeable(cell.timestamp, i64::from(cell.local_deletion_time))
}

/// A row's primary-key liveness written with a TTL, expired past the grace window
/// and below the overlap guard: the row marker of an `INSERT ... USING TTL`. Same
/// argument as [`cell_is_purgeable`]: an older liveness for this row outside the
/// compaction would sit at or above the guard, and the merge already kept the
/// newest one inside it.
fn liveness_is_purgeable(liveness: &LivenessInfo, policy: &PurgePolicy) -> bool {
    liveness.has_timestamp()
        && liveness.has_ttl()
        && policy.purgeable(liveness.timestamp, i64::from(liveness.local_deletion_time))
}

/// Cheap pre-check: does `partition` hold any marker [`purge_partition`] could
/// drop? Lets the caller skip the purge (and any defensive copy) for the common
/// partition that has none. May over-report (a pathless collection tombstone that
/// the purge would keep still counts); never under-reports.
pub fn has_purgeable_marker(partition: &Partition, policy: &PurgePolicy) -> bool {
    let row_has_marker = |row: &Row| {
        policy.purgeable_deletion(&row.deletion)
            || liveness_is_purgeable(&row.primary_key_liveness, policy)
            || row
                .cells
                .iter()
                .any(|(_, cell)| cell_is_purgeable(cell, policy))
    };
    policy.purgeable_deletion(&partition.deletion)
        || partition.static_row.as_ref().is_some_and(row_has_marker)
        || partition.rows.iter().any(row_has_marker)
}

/// The policy for viewing an already-merged read result at horizon `gc_before`:
/// what a replica's read digest is computed over, so replicas that purged at
/// different times still agree.
///
/// A read has merged every local source (memtable and every SSTable), so no local
/// data sits outside it and the overlap guard is open. The rows a read returns are
/// never filtered by it: everything it removes is already invisible to a reader,
/// and whether a REPLICA that never saw a delete can bring data back is the grace
/// window's question, the same as for compaction.
pub fn read_view_policy(gc_before: i64) -> PurgePolicy {
    PurgePolicy {
        gc_before,
        max_purgeable_timestamp: i64::MAX,
        table_delete: DeletionTime::LIVE,
    }
}

/// The guard one replica's read response imposes on a read repair written to
/// it: the smallest timestamp of anything it holds that is not past grace at
/// `gc_before`, or `i64::MAX` when it holds nothing else.
///
/// This is the compaction overlap guard with the target replica as the data
/// outside. A repair to this replica may leave out a merged element that is
/// past grace and below the guard (`PurgePolicy { gc_before,
/// max_purgeable_timestamp: repair_guard(..), .. }`): everything the replica
/// holds is newer than that element or itself past grace, so the replica reads
/// the merged answer either way, and its own compaction would drop the element
/// anyway (Lean: `Tombstone.repair_converges_every_replica_to_the_merged_answer`,
/// `Tombstone.repaired_write_matches_the_shipped_write_after_compaction`).
pub fn repair_guard(partition: &Partition, gc_before: i64) -> i64 {
    let open = read_view_policy(gc_before);
    let mut guard = i64::MAX;
    let mut hold = |timestamp: i64| guard = guard.min(timestamp);
    if !partition.deletion.is_live() && !open.purgeable_deletion(&partition.deletion) {
        hold(partition.deletion.marked_for_delete_at);
    }
    for row in partition.static_row.iter().chain(partition.rows.iter()) {
        if !row.deletion.is_live() && !open.purgeable_deletion(&row.deletion) {
            hold(row.deletion.marked_for_delete_at);
        }
        let liveness = &row.primary_key_liveness;
        if liveness.has_timestamp() && !liveness_is_purgeable(liveness, &open) {
            hold(liveness.timestamp);
        }
        for (_, cell) in &row.cells {
            if !cell_is_purgeable(cell, &open) {
                hold(cell.timestamp);
            }
        }
    }
    guard
}

/// The partition deletion [`purge_partition`] would leave.
pub fn purged_partition_deletion(deletion: DeletionTime, policy: &PurgePolicy) -> DeletionTime {
    if policy.purgeable_deletion(&deletion) {
        DeletionTime::LIVE
    } else {
        deletion
    }
}

/// One row as [`purge_partition`] would leave it, borrowed rather than copied, so
/// a digest can hash the purged form of a partition one row at a time without
/// cloning row data.
pub struct PurgedRow<'a> {
    row: &'a Row,
    cells: CellRule,
    markers: RowMarkers,
}

impl<'a> PurgedRow<'a> {
    pub fn new(row: &'a Row, policy: &PurgePolicy) -> Self {
        Self {
            row,
            cells: CellRule::for_row(row, policy),
            markers: RowMarkers::for_row(row, policy),
        }
    }

    /// The clustering bytes (never changed by a purge).
    pub fn clustering(&self) -> &'a [u8] {
        &self.row.clustering
    }

    /// The cells the purge keeps, in their original order.
    pub fn cells(&self) -> impl Iterator<Item = &'a (u16, ferrosa_common::CellValue)> + '_ {
        self.row
            .cells
            .iter()
            .filter(|(column, cell)| !self.cells.drops(*column, cell))
    }

    /// The surviving row deletion.
    pub fn deletion(&self) -> DeletionTime {
        self.markers.deletion
    }

    /// The surviving primary-key liveness.
    pub fn liveness(&self) -> LivenessInfo {
        self.markers.liveness
    }

    /// Whether the purge removed anything from this row.
    pub fn changed(&self) -> bool {
        self.markers.deletion_dropped
            || self.markers.liveness_expired
            || self.cells().count() != self.row.cells.len()
    }

    /// Whether nothing is left: no cells, no deletion, no primary-key liveness.
    /// Writing such a row changes nothing, so a repair leaves it out.
    pub fn is_vacant(&self) -> bool {
        self.cells().next().is_none()
            && self.markers.deletion.is_live()
            && !self.markers.liveness.has_timestamp()
    }

    /// Whether [`purge_partition`] drops this clustered row entirely.
    pub fn removes_row(&self) -> bool {
        self.changed()
            && self.cells().next().is_none()
            && self.markers.deletion.is_live()
            && !self.markers.liveness.has_timestamp()
    }

    /// Whether [`purge_partition`] drops this row when it is the static row.
    pub fn removes_static_row(&self) -> bool {
        self.changed() && self.cells().next().is_none()
    }
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
            table_delete: DeletionTime::LIVE,
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

    /// An expiring cell written at `ts` with a 60 s TTL, expiring at `ldt`.
    fn expiring(ts: i64, ldt: i32) -> CellValue {
        CellValue::expiring(b"ttl".to_vec(), ts, 60, ldt)
    }

    /// Lean: `Tombstone.expired_history_grows_without_bound` is the shipped rule's
    /// witness; `Tombstone.purging_expired_data_is_invisible_to_reads` is why
    /// dropping under the two conditions below is safe.
    #[test]
    fn an_expired_ttl_cell_past_grace_and_below_the_guard_is_purged() {
        let cells = vec![
            (0, CellValue::live(b"keep".to_vec(), 900)),
            (1, expiring(500, OLD_LDT)),
            (2, expiring(500, FRESH_LDT)),
            (3, expiring(1_500, OLD_LDT)),
        ];
        let mut p = partition(DeletionTime::LIVE, vec![row(b"c1", cells, 900)]);
        let stats = purge_partition(&mut p, &policy(1_000));
        let kept: Vec<u16> = p.rows[0].cells.iter().map(|(c, _)| *c).collect();
        assert_eq!(
            kept,
            vec![0, 2, 3],
            "the expired cell goes; one inside grace and one at/above the guard stay"
        );
        assert_eq!(stats.markers(), 1);
    }

    #[test]
    fn a_row_whose_ttl_liveness_and_cells_expired_past_grace_is_removed() {
        let mut expired = row(b"c1", vec![(0, expiring(500, OLD_LDT))], i64::MIN);
        expired.primary_key_liveness = LivenessInfo::with_ttl(500, 60, OLD_LDT);
        let mut fresh = row(b"c2", vec![(0, expiring(500, FRESH_LDT))], i64::MIN);
        fresh.primary_key_liveness = LivenessInfo::with_ttl(500, 60, FRESH_LDT);
        let mut p = partition(DeletionTime::LIVE, vec![expired, fresh.clone()]);
        let stats = purge_partition(&mut p, &policy(1_000));
        assert_eq!(
            p.rows,
            vec![fresh],
            "only the row still inside grace remains"
        );
        assert_eq!(stats.rows_removed, 1);
    }

    #[test]
    fn an_expired_ttl_liveness_goes_but_a_live_cell_keeps_the_row() {
        let mut r = row(
            b"c1",
            vec![(0, CellValue::live(b"v".to_vec(), 400))],
            i64::MIN,
        );
        r.primary_key_liveness = LivenessInfo::with_ttl(500, 60, OLD_LDT);
        let mut p = partition(DeletionTime::LIVE, vec![r]);
        purge_partition(&mut p, &policy(1_000));
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.rows[0].primary_key_liveness, LivenessInfo::NONE);
        assert_eq!(
            p.rows[0].cells,
            vec![(0, CellValue::live(b"v".to_vec(), 400))]
        );
    }

    #[test]
    fn an_expired_ttl_liveness_above_the_guard_is_kept() {
        // Data outside this compaction as old as the guard could be an older INSERT
        // of the same row that the expired liveness shadows.
        let liveness = LivenessInfo::with_ttl(1_500, 60, OLD_LDT);
        let mut r = row(b"c1", vec![], i64::MIN);
        r.primary_key_liveness = liveness;
        let mut p = partition(DeletionTime::LIVE, vec![r]);
        assert_eq!(purge_partition(&mut p, &policy(1_000)).markers(), 0);
        assert_eq!(p.rows[0].primary_key_liveness, liveness);
    }

    #[test]
    fn has_purgeable_marker_finds_expired_ttl_data() {
        let pol = policy(1_000);
        let cell = row(b"c", vec![(0, expiring(500, OLD_LDT))], 900);
        assert!(has_purgeable_marker(
            &partition(DeletionTime::LIVE, vec![cell]),
            &pol
        ));
        let mut liveness = row(b"c", vec![], i64::MIN);
        liveness.primary_key_liveness = LivenessInfo::with_ttl(500, 60, OLD_LDT);
        assert!(has_purgeable_marker(
            &partition(DeletionTime::LIVE, vec![liveness]),
            &pol
        ));
        let fresh = row(b"c", vec![(0, expiring(500, FRESH_LDT))], 900);
        assert!(!has_purgeable_marker(
            &partition(DeletionTime::LIVE, vec![fresh]),
            &pol
        ));
    }

    /// Rebuild what the borrowed views say a purge leaves (test-only copy).
    fn rebuilt_from_views(p: &Partition, policy: &PurgePolicy) -> Partition {
        let owned = |view: &PurgedRow<'_>| Row {
            clustering: view.clustering().to_vec(),
            cells: view.cells().cloned().collect(),
            deletion: view.deletion(),
            primary_key_liveness: view.liveness(),
        };
        let static_row = p
            .static_row
            .as_ref()
            .map(|r| PurgedRow::new(r, policy))
            .filter(|v| !v.removes_static_row())
            .map(|v| owned(&v));
        let rows = p
            .rows
            .iter()
            .map(|r| PurgedRow::new(r, policy))
            .filter(|v| !v.removes_row())
            .map(|v| owned(&v))
            .collect();
        Partition {
            key: p.key.clone(),
            deletion: purged_partition_deletion(p.deletion, policy),
            static_row,
            rows,
        }
    }

    #[test]
    fn a_replica_holding_only_past_grace_data_imposes_no_repair_guard() {
        let mut expired = row(b"a", vec![(0, expiring(1_500, OLD_LDT))], i64::MIN);
        expired.primary_key_liveness = LivenessInfo::with_ttl(1_500, 60, OLD_LDT);
        let tomb = row(
            b"b",
            vec![(0, CellValue::tombstone(1_200, OLD_LDT))],
            i64::MIN,
        );
        let p = partition(
            DeletionTime {
                marked_for_delete_at: 100,
                local_deletion_time: OLD_LDT as u32,
            },
            vec![expired, tomb],
        );
        assert_eq!(repair_guard(&p, GC_BEFORE), i64::MAX);
        assert_eq!(
            repair_guard(&partition(DeletionTime::LIVE, vec![]), GC_BEFORE),
            i64::MAX
        );
    }

    #[test]
    fn the_repair_guard_is_the_oldest_thing_held_that_is_not_past_grace() {
        let live = row(b"a", vec![(0, CellValue::live(b"v".to_vec(), 900))], 950);
        let unexpired = row(b"b", vec![(0, expiring(700, FRESH_LDT))], i64::MIN);
        let fresh_tomb = row(
            b"c",
            vec![(0, CellValue::tombstone(650, FRESH_LDT))],
            i64::MIN,
        );
        let old_tomb = row(b"d", vec![(0, CellValue::tombstone(10, OLD_LDT))], i64::MIN);
        let p = partition(DeletionTime::LIVE, vec![live.clone(), unexpired.clone()]);
        assert_eq!(
            repair_guard(&p, GC_BEFORE),
            700,
            "an unexpired TTL cell counts"
        );
        let p = partition(DeletionTime::LIVE, vec![live.clone(), fresh_tomb, old_tomb]);
        assert_eq!(
            repair_guard(&p, GC_BEFORE),
            650,
            "a tombstone inside grace counts; one past grace does not"
        );
        let mut deleted = live;
        deleted.deletion = DeletionTime {
            marked_for_delete_at: 300,
            local_deletion_time: FRESH_LDT as u32,
        };
        let p = partition(DeletionTime::LIVE, vec![deleted]);
        assert_eq!(
            repair_guard(&p, GC_BEFORE),
            300,
            "a row deletion inside grace counts"
        );
    }

    #[test]
    fn a_purged_row_view_of_an_untouched_row_reports_no_change() {
        let r = row(b"c", vec![(0, expiring(500, FRESH_LDT))], 900);
        let view = PurgedRow::new(&r, &read_view_policy(GC_BEFORE));
        assert!(!view.changed());
        assert!(!view.removes_row());
        assert_eq!(view.cells().count(), 1);
    }

    #[test]
    fn the_read_view_strips_expired_rows_whatever_their_timestamp() {
        // A read merged every local source, so there is no overlap guard: even a
        // timestamp a compaction's guard would have protected is stripped.
        let mut expired = row(b"a", vec![(0, expiring(1_500, OLD_LDT))], i64::MIN);
        expired.primary_key_liveness = LivenessInfo::with_ttl(1_500, 60, OLD_LDT);
        let kept = row(b"b", vec![(0, CellValue::live(b"v".to_vec(), 900))], 900);
        let p = partition(DeletionTime::LIVE, vec![expired, kept.clone()]);
        let viewed = rebuilt_from_views(&p, &read_view_policy(GC_BEFORE));
        assert_eq!(viewed.rows, vec![kept]);
    }

    #[test]
    fn purged_row_views_agree_with_purge_partition() {
        let policy = PurgePolicy {
            gc_before: GC_BEFORE,
            max_purgeable_timestamp: 1_000,
            table_delete: DeletionTime::LIVE,
        };
        let mut deleted = row(b"d", vec![(0, CellValue::live(b"x".to_vec(), 950))], 100);
        deleted.deletion = DeletionTime {
            marked_for_delete_at: 200,
            local_deletion_time: OLD_LDT as u32,
        };
        let mut ttl_row = row(b"t", vec![(0, expiring(500, OLD_LDT))], i64::MIN);
        ttl_row.primary_key_liveness = LivenessInfo::with_ttl(500, 60, OLD_LDT);
        let guarded = row(b"g", vec![(0, expiring(1_500, OLD_LDT))], i64::MIN);
        let mixed = row(
            b"m",
            vec![
                (0, CellValue::tombstone(300, OLD_LDT)),
                (1, CellValue::live(b"v".to_vec(), 900)),
            ],
            900,
        );
        let mut p = partition(
            DeletionTime {
                marked_for_delete_at: 50,
                local_deletion_time: OLD_LDT as u32,
            },
            vec![deleted, ttl_row, guarded, mixed],
        );
        p.static_row = Some(row(&[], vec![(2, expiring(400, OLD_LDT))], i64::MIN));

        let viewed = rebuilt_from_views(&p, &policy);
        let mut purged = p.clone();
        purge_partition(&mut purged, &policy);
        assert_eq!(viewed, purged);
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
        let p = policy_for(2_000_000_000, 864_000, 55, DeletionTime::LIVE);
        assert_eq!(p.gc_before, 2_000_000_000 - 864_000);
        assert_eq!(p.max_purgeable_timestamp, 55);
        assert!(p.table_delete.is_live());
        // A grace period longer than the clock does not wrap: nothing is old enough.
        assert!(policy_for(100, u32::MAX, 55, DeletionTime::LIVE).gc_before < 0);
        assert_eq!(policy_for(100, 0, 55, DeletionTime::LIVE).gc_before, 100);
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
            0u8..3,
            0i64..2_000,
            prop::sample::select(vec![OLD_LDT, FRESH_LDT]),
        )
            .prop_map(|(kind, ts, ldt)| match kind {
                0 => CellValue::live(b"v".to_vec(), ts),
                1 => CellValue::tombstone(ts, ldt),
                _ => expiring(ts, ldt),
            })
    }

    /// Visible to a reader at `NOW`: not a tombstone, not past its expiry.
    fn visible_now(c: &CellValue) -> bool {
        !c.is_tombstone() && !(c.is_expiring() && i64::from(c.local_deletion_time) <= NOW)
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
            prop_assert_eq!(&rebuilt_from_views(&before, &pol), &p, "borrowed views must match the purge");

            let live_before: Vec<_> = before.rows.iter()
                .flat_map(|r| r.cells.iter().filter(|(_, c)| visible_now(c)).map(|(i, c)| (r.clustering.clone(), *i, c.clone())))
                .collect();
            let live_after: Vec<_> = p.rows.iter()
                .flat_map(|r| r.cells.iter().filter(|(_, c)| visible_now(c)).map(|(i, c)| (r.clustering.clone(), *i, c.clone())))
                .collect();
            prop_assert_eq!(live_before, live_after, "visible cells must survive");

            for r in &before.rows {
                for (i, c) in r.cells.iter().filter(|(_, c)| !visible_now(c)) {
                    let survived = p.rows.iter().any(|x| x.clustering == r.clustering
                        && x.cells.iter().any(|(j, d)| j == i && d == c));
                    let purgeable = pol.purgeable(c.timestamp, i64::from(c.local_deletion_time));
                    if !purgeable {
                        prop_assert!(survived, "unpurgeable marker or expired cell dropped");
                    } else if c.is_expiring() {
                        prop_assert!(!survived, "purgeable expired cell kept");
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
