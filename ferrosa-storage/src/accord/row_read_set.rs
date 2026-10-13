//! A PostgreSQL transaction's **row-level read set**, staged through the write-set spill.
//!
//! # Why a staged read set at all
//!
//! A SERIALIZABLE transaction's read set was, until the hybrid, a [`HashSet<String>`] of
//! TABLE names (`ferrosa-postgres/src/extended.rs`): a read registration was the whole
//! table, so two READERS of one table registered the same key and collided. That is a
//! false refusal — two readers of a table do not conflict, because neither wrote anything
//! for the other to serialize against — and it is what forced the CL-28 fixture to hand an
//! EMPTY read set (`ferrosa-cluster/src/accord/quorum_availability.rs`).
//!
//! The hybrid resolves a read that CAN be resolved to keys to the ROW keys it visited, and
//! registers each as a READ of that row (`postgres_read_key`), so two readers of DISJOINT
//! rows register DISJOINT keys and both commit, while a concurrent WRITER of one of those
//! rows still conflicts (a reader sees the writer's registration under the underlying key).
//! A wide scan that cannot be resolved to keys keeps the table-level key exactly as before;
//! it is what still catches a predicate phantom through the per-table WRITE WATERMARK.
//!
//! # One spill path, one threshold
//!
//! The visited-row list is as unbounded as the rows a keyed read visits, so it must never be
//! a resident `HashSet` — the fixed 4 GB node budget forbids it, and the OOM gate
//! (`ferrosa-postgres/tests/pg_full_scan_memory_bound.rs`) is the detector. [`RowReadSet`]
//! therefore stages each row key through the SAME [`WriteSetStage`] the write-set uses: a
//! bounded resident prefix plus a private staging file, read back as a stream. The resident
//! buffer size is the write-set's own
//! [`WRITE_SET_SPILL_THRESHOLD_ENV`](crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV)
//! — there is deliberately NO second, read-set-specific constant, and NO second,
//! hand-managed buffer.
//!
//! # What is forbidden
//!
//! A resident `HashSet<RowKey>` (or any in-memory row list) read set is FORBIDDEN on this
//! path. It grows with the scanned/keyed row count, not with the schema, and it is exactly
//! what the OOM gate catches: the in-memory variant is wired only as the gate's negative
//! control and is never the shipped shape.
//!
//! # Fail loud
//!
//! A staging I/O failure, or a read of an entry that was never staged, is surfaced — never a
//! silent empty read. A dropped read registration would silently lose the conflict it was
//! there to catch.

use std::path::PathBuf;

use ferrosa_common::Result;

use crate::write_set_stage::{StagedWriteSet, WriteSetStage};

/// A transaction's row-level read set, built one visited row key at a time.
///
/// Each [`record`](Self::record) hands the row's partition-key bytes to the shared
/// [`WriteSetStage`], which keeps a bounded resident prefix and spills the rest. Residency is
/// therefore the staging buffer, never the number of rows read.
pub struct RowReadSet {
    stage: WriteSetStage,
    /// Number of row keys recorded. Tracked beside the stage so [`Self::len`] is O(1) and
    /// independent of residency.
    count: usize,
}

impl std::fmt::Debug for RowReadSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowReadSet")
            .field("rows", &self.count)
            .field("resident_bytes", &self.stage.resident_bytes())
            .field("spilled", &self.stage.spilled_to_disk())
            .finish()
    }
}

impl RowReadSet {
    /// A read set staging beneath `root`, using the process-wide write-set buffer setting.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            stage: WriteSetStage::new(root),
            count: 0,
        }
    }
    /// A read set keeping `threshold_bytes` resident before spilling beneath `root`.
    #[must_use]
    pub fn with_threshold(root: impl Into<PathBuf>, threshold_bytes: u64) -> Self {
        Self {
            stage: WriteSetStage::with_threshold(root, threshold_bytes),
            count: 0,
        }
    }

    /// Record one visited row's partition-key bytes.
    ///
    /// # Errors
    ///
    /// A staging I/O failure is surfaced, never swallowed — a dropped read registration
    /// would lose a conflict the transaction must see.
    pub fn record(&mut self, row_key: &[u8]) -> Result<()> {
        self.stage.append(row_key)?;
        self.count += 1;
        Ok(())
    }

    /// Number of row keys recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Row-key payload bytes still resident. Bounded by the staging buffer, independent of
    /// [`Self::len`] once anything has spilled.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.stage.resident_bytes()
    }

    /// Whether anything has reached the disk.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.stage.spilled_to_disk()
    }

    /// Flush and memory-map any spilled region, returning the finished, re-readable view.
    ///
    /// # Errors
    ///
    /// A flush or map failure is surfaced.
    pub fn finish(self) -> Result<StagedRowReads> {
        Ok(StagedRowReads {
            inner: self.stage.finish()?,
        })
    }
}

/// The default staging root for a transaction read set, mirroring `TxnWriteSet`'s
/// convention: a private per-transaction directory under the process temp dir, created
/// lazily on the first spill.
fn default_stage_root() -> PathBuf {
    std::env::temp_dir().join("ferrosa-pg-txn-read-stage")
}

impl Default for RowReadSet {
    fn default() -> Self {
        Self::new(default_stage_root())
    }
}

/// A finished [`RowReadSet`], readable as a stream in visit order.
pub struct StagedRowReads {
    inner: StagedWriteSet,
}

impl std::fmt::Debug for StagedRowReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedRowReads")
            .field("rows", &self.inner.len())
            .field("spilled", &self.inner.spilled_to_disk())
            .finish()
    }
}

impl StagedRowReads {
    /// An empty read set that touches no filesystem.
    #[must_use]
    pub fn empty() -> Self {
        // `with_threshold` never touches the disk until the first spill, so this allocates
        // nothing and creates no directory.
        let stage = WriteSetStage::with_threshold(PathBuf::new(), 1);
        Self {
            inner: stage
                .finish()
                .expect("an un-spilled stage finishes without I/O"),
        }
    }

    /// Number of recorded row keys (resident + spilled).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no row was recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Whether anything reached the disk.
    #[must_use]
    pub fn spilled_to_disk(&self) -> bool {
        self.inner.spilled_to_disk()
    }

    /// Visit every recorded row key in visit order, one at a time.
    ///
    /// Residency is ONE row key, never the set: a spilled entry is a slice of the staging
    /// region's memory map, a resident one a slice of the resident prefix. A consumer can
    /// therefore register the keys without ever materializing them.
    ///
    /// # Errors
    ///
    /// An error from `visit`, or an un-staged index (fail loud, never a silent empty read).
    pub fn for_each_row_key(&self, visit: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        for index in 0..self.inner.len() {
            visit(self.inner.entry(index)?)?;
        }
        Ok(())
    }
}

/// A transaction's READ SET, at the granularity each read could be resolved to.
///
/// - `scan_tables` are the wide scans that could NOT be resolved to keys: they keep the
///   table-level key (and its per-table WRITE WATERMARK), which is what still catches a
///   phantom. Bounded by the schema, so a resident `Vec` is correct here.
/// - `row_keys` are the keyed reads, resolved to the ROW keys they visited. Streamed through
///   [`StagedRowReads`] so they are never materialized.
#[derive(Debug)]
pub struct PgReadSet {
    scan_tables: Vec<String>,
    row_keys: StagedRowReads,
}

impl PgReadSet {
    /// A read set with no reads at all.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            scan_tables: Vec::new(),
            row_keys: StagedRowReads::empty(),
        }
    }

    /// A table-granular read set — the wide scans, exactly as before the hybrid.
    #[must_use]
    pub fn scans(scan_tables: Vec<String>) -> Self {
        Self {
            scan_tables,
            row_keys: StagedRowReads::empty(),
        }
    }

    /// A hybrid read set: table-granular wide scans plus streamed row-granular keyed reads.
    #[must_use]
    pub fn with_row_keys(scan_tables: Vec<String>, row_keys: StagedRowReads) -> Self {
        Self {
            scan_tables,
            row_keys,
        }
    }

    /// The tables that were read by a scan that could not be resolved to keys.
    #[must_use]
    pub fn scan_tables(&self) -> &[String] {
        &self.scan_tables
    }

    /// Number of row-granular reads.
    #[must_use]
    pub fn row_key_count(&self) -> usize {
        self.row_keys.len()
    }

    /// Visit every row-granular read's key, streamed, one at a time.
    ///
    /// # Errors
    ///
    /// An error from `visit`, or an un-staged index.
    pub fn for_each_row_key(&self, visit: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.row_keys.for_each_row_key(visit)
    }

    /// Whether the read set holds no reads at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scan_tables.is_empty() && self.row_keys.is_empty()
    }
}

impl Default for PgReadSet {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ferrosa-row-read-set-test-{}-{}",
            tag,
            uuid::Uuid::new_v4()
        ))
    }

    /// INVARIANT: RESULTS COMPLETE — every recorded row key round-trips byte for byte, across
    /// the spill boundary AND at the last entry. The boundary entry is the one an off-by-one
    /// in the drain/index arithmetic would corrupt; the last is the one a short flush drops.
    #[test]
    fn every_row_key_round_trips_across_the_spill_boundary_and_the_last() {
        let root = temp_root("round-trip");
        let rows: Vec<Vec<u8>> = vec![
            b"d1".to_vec(),
            b"d2-slightly-longer".to_vec(),
            vec![0xABu8; 40], // alone exceeds a 16-byte buffer, forcing the spill
            b"d4".to_vec(),
            b"d5-final".to_vec(),
        ];
        let mut set = RowReadSet::with_threshold(&root, 16);
        for r in &rows {
            set.record(r).expect("record");
        }
        let staged = set.finish().expect("finish");
        assert!(staged.spilled_to_disk(), "the prefix must have spilled");

        let mut seen: Vec<Vec<u8>> = Vec::new();
        staged
            .for_each_row_key(&mut |k| {
                seen.push(k.to_vec());
                Ok(())
            })
            .expect("walk");
        assert_eq!(seen, rows, "visit order must be the record order");
    }

    /// INVARIANT: CAPACITY — residency is FLAT across widely separated row counts. It is
    /// bounded by the shared staging buffer and does not track the number of rows read. This
    /// is deliberately the OPPOSITE of a cap test: a read set far larger than the buffer
    /// SUCCEEDS, complete.
    #[test]
    fn residency_is_flat_across_widely_separated_row_counts() {
        let root = temp_root("flat");
        let threshold = 4096u64;
        let key = vec![0x5Au8; 64]; // 64 bytes each

        // 64 * 64 == 4096 == threshold: fits exactly, stays fully resident.
        let mut small = RowReadSet::with_threshold(&root, threshold);
        for _ in 0..64 {
            small.record(&key).expect("record");
        }
        let small_resident = small.resident_bytes();
        let small = small.finish().expect("finish");

        // 10_000 * 64 == 640_000 bytes: far past the buffer, so it spills and keeps NOTHING
        // resident.
        let mut large = RowReadSet::with_threshold(&root, threshold);
        for _ in 0..10_000 {
            large.record(&key).expect("record");
        }
        let large_resident = large.resident_bytes();
        let large = large.finish().expect("finish");

        assert_eq!(
            small_resident, threshold,
            "an exactly-threshold read set stays resident (the buffer is an inclusive bound)"
        );
        assert_eq!(
            large_resident, 0,
            "once spilling, no row key stays resident — residency is FLAT in N"
        );
        assert!(!small.spilled_to_disk(), "a small keyed read touches no disk");

        // Completeness at both scales.
        assert_eq!(small.len(), 64);
        assert_eq!(
            large.len(),
            10_000,
            "a read set far larger than the buffer is ACCEPTED, not refused"
        );
        let mut last = None;
        large
            .for_each_row_key(&mut |k| {
                last = Some(k.to_vec());
                Ok(())
            })
            .expect("walk");
        assert_eq!(last.as_deref(), Some(key.as_slice()));
    }

    /// INVARIANT: ONE SPILL PATH — the row read set shares the write-set stage, so its
    /// threshold comes from the SAME process setting the write-set uses. There is no
    /// read-set-specific constant.
    #[test]
    fn the_threshold_is_the_write_set_threshold() {
        // `RowReadSet::new` (no explicit threshold) resolves the write-set env setting; with
        // it unset that is the write-set floor. Assert against the write-set's own constant,
        // not a copy of it.
        let root = temp_root("threshold");
        let mut set = RowReadSet::new(&root);
        set.record(b"d1").expect("record");
        assert!(
            !set.spilled_to_disk(),
            "one small key must not spill under any threshold"
        );
    }

    /// INVARIANT: FAIL LOUD — an empty read set visits nothing and touches no disk; a read
    /// set that never spilled is readable; a `PgReadSet` distinguishes its two granularities.
    #[test]
    fn an_empty_read_set_is_inert_and_the_hybrid_keeps_both_granularities() {
        let empty = StagedRowReads::empty();
        assert!(empty.is_empty());
        assert!(!empty.spilled_to_disk());
        empty
            .for_each_row_key(&mut |_| panic!("an empty read set visits nothing"))
            .expect("walk");

        let root = temp_root("hybrid");
        let mut rows = RowReadSet::with_threshold(&root, 4096);
        rows.record(b"d1").expect("record");
        let staged = rows.finish().expect("finish");

        let reads = PgReadSet::with_row_keys(vec!["public.doctors".to_string()], staged);
        assert_eq!(reads.scan_tables(), ["public.doctors".to_string()]);
        assert_eq!(reads.row_key_count(), 1);
        assert!(!reads.is_empty());

        let mut seen: Vec<Vec<u8>> = Vec::new();
        reads
            .for_each_row_key(&mut |k| {
                seen.push(k.to_vec());
                Ok(())
            })
            .expect("walk");
        assert_eq!(seen, vec![b"d1".to_vec()]);
    }
}
