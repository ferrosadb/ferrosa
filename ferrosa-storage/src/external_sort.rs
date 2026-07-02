//! Bounded-memory external merge sort for CQL result rows.
//!
//! An unbounded `ORDER BY` (no `LIMIT`) over an arbitrary column cannot be
//! computed in O(1) streaming state: every row must be seen before the first
//! output row is known. Materializing the whole table for an in-memory
//! `Vec::sort` is an OOM risk, which is why the query router previously
//! fail-loud-refused this shape past a row cap.
//!
//! This module replaces that cap with a classic **external merge sort**:
//!
//! 1. Rows are pushed into an in-memory buffer.
//! 2. When the buffer's estimated size crosses the **spill threshold**
//!    (see [`crate::spill_budget`]), the buffer is sorted and written to a
//!    **run file** on disk, then cleared. Peak in-memory rows are thus bounded
//!    by the threshold, independent of the total result size.
//! 3. [`ExternalSorter::finish`] performs a **bounded k-way merge** of all run
//!    files (plus the final in-memory buffer, spilled if any runs exist) using a
//!    min-heap that holds at most one row per run. The merge streams rows in
//!    fully sorted order.
//!
//! # Correctness
//!
//! Correctness is the whole point. The merge is a stable k-way merge: it emits
//! rows in exactly the order defined by [`RowOrder`], loses no row (every pushed
//! row lands in a run or the final buffer, and every run row is drained), and
//! duplicates none (each row is moved exactly once; a heap entry is popped
//! before its successor from the same run is read). See the property test
//! `order_by_spills_and_stays_correct` in `ferrosa-cql`.
//!
//! # Failure philosophy
//!
//! Every spill/merge I/O error is propagated (`Result`) — a spilled run is never
//! silently dropped, because dropping a run would silently lose rows, which is
//! strictly worse than the old fail-loud cap.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use ferrosa_common::{CqlValue, Error, Result};

/// A single result row: one optional value per output column.
pub type Row = Vec<Option<CqlValue>>;

/// The ORDER BY key: a list of `(column_index, ascending)` pairs, applied
/// left-to-right (the first unequal column decides the order).
///
/// Semantics mirror the CQL router's in-memory comparator exactly:
/// - `NULL` (`None`) sorts **before** any present value in ascending order;
/// - `ascending == false` reverses the per-column comparison.
#[derive(Debug, Clone)]
pub struct RowOrder {
    specs: Vec<(usize, bool)>,
}

impl RowOrder {
    /// Build an order from `(column_index, ascending)` specs.
    pub fn new(specs: Vec<(usize, bool)>) -> Self {
        Self { specs }
    }

    /// Compare two rows under this order.
    pub fn compare(&self, a: &Row, b: &Row) -> Ordering {
        for &(idx, ascending) in &self.specs {
            let cmp = match (
                a.get(idx).and_then(|v| v.as_ref()),
                b.get(idx).and_then(|v| v.as_ref()),
            ) {
                (Some(va), Some(vb)) => va.cmp(vb),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            let cmp = if ascending { cmp } else { cmp.reverse() };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// Estimate the in-memory footprint of a row for threshold accounting.
///
/// This need not be exact — it only has to grow monotonically with real memory
/// use so the accumulator spills before the process is starved. We count the
/// `Option<CqlValue>` slots plus the payload bytes of variable-length values.
fn estimate_row_bytes(row: &Row) -> usize {
    // Per-slot overhead: the Vec slot + Option/enum discriminant.
    let mut bytes = row.len() * std::mem::size_of::<Option<CqlValue>>();
    for cell in row.iter().flatten() {
        bytes += cql_value_payload_bytes(cell);
    }
    bytes
}

/// Payload bytes of a `CqlValue` beyond its fixed enum size (variable-length
/// heap allocations: strings, blobs, collections).
fn cql_value_payload_bytes(v: &CqlValue) -> usize {
    match v {
        CqlValue::Text(s) | CqlValue::Ascii(s) => s.len(),
        CqlValue::Blob(b) => b.len(),
        CqlValue::Vector(items) => items.len() * std::mem::size_of::<u32>(),
        CqlValue::List(items) | CqlValue::Set(items) => {
            items.iter().map(cql_value_payload_bytes).sum::<usize>()
                + items.len() * std::mem::size_of::<CqlValue>()
        }
        CqlValue::Map(entries) => {
            entries
                .iter()
                .map(|(k, val)| cql_value_payload_bytes(k) + cql_value_payload_bytes(val))
                .sum::<usize>()
                + entries.len() * 2 * std::mem::size_of::<CqlValue>()
        }
        CqlValue::Tuple(items) => {
            items
                .iter()
                .flatten()
                .map(cql_value_payload_bytes)
                .sum::<usize>()
                + items.len() * std::mem::size_of::<Option<CqlValue>>()
        }
        _ => 0,
    }
}

/// Wrap a contextual message as an `Error::Io` (the `Io` variant carries a
/// `std::io::Error`, so string context is threaded through one).
fn io_error(msg: String) -> Error {
    Error::Io(std::io::Error::other(msg))
}

/// Serialize one row to `w` as a length-prefixed JSON record: `[u32 LE len][json]`.
fn write_row<W: Write>(w: &mut W, row: &Row) -> Result<()> {
    let bytes = serde_json::to_vec(row)
        .map_err(|e| Error::InvalidFormat(format!("external sort: encode row: {e}")))?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| Error::InvalidFormat("external sort: row exceeds 4 GiB".into()))?;
    w.write_all(&len.to_le_bytes())
        .map_err(|e| io_error(format!("external sort: write run len: {e}")))?;
    w.write_all(&bytes)
        .map_err(|e| io_error(format!("external sort: write run row: {e}")))?;
    Ok(())
}

/// Read the next length-prefixed row from `r`, or `Ok(None)` at clean EOF.
///
/// A truncated record (EOF mid-row) is a corrupt run and fails loud — it is
/// never treated as a clean end, because that would silently drop rows.
fn read_row<R: Read>(r: &mut R) -> Result<Option<Row>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(io_error(format!("external sort: read run len: {e}"))),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).map_err(|e| {
        io_error(format!(
            "external sort: truncated run record (want {len} bytes): {e}"
        ))
    })?;
    let row: Row = serde_json::from_slice(&buf)
        .map_err(|e| Error::InvalidFormat(format!("external sort: decode run row: {e}")))?;
    Ok(Some(row))
}

/// A bounded-memory external sorter for CQL rows.
///
/// Push every row, then call [`ExternalSorter::finish`] to obtain the sorted
/// stream. Run files are written under the caller-provided directory (typically
/// a [`crate::TempSortTableReservation`] whose `Drop` removes them).
pub struct ExternalSorter {
    dir: PathBuf,
    order: RowOrder,
    threshold_bytes: usize,
    buffer: Vec<Row>,
    buffer_bytes: usize,
    runs: Vec<PathBuf>,
    /// Whether at least one spill to disk occurred (observability + tests).
    spilled: bool,
}

impl ExternalSorter {
    /// Create a sorter that spills into `dir` once the in-memory buffer's
    /// estimated size reaches `threshold_bytes`.
    pub fn new(dir: impl Into<PathBuf>, order: RowOrder, threshold_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            order,
            // At least 1 so a zero/absurd threshold still makes progress.
            threshold_bytes: (threshold_bytes as usize).max(1),
            buffer: Vec::new(),
            buffer_bytes: 0,
            runs: Vec::new(),
            spilled: false,
        }
    }

    /// Whether any run has been spilled to disk. Used by tests/metrics to
    /// confirm the spill path actually engaged.
    pub fn spilled(&self) -> bool {
        self.spilled
    }

    /// Number of run files spilled so far.
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Push one row (moved, never cloned). Spills the buffer to a run file if it
    /// crosses the threshold.
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.buffer_bytes += estimate_row_bytes(&row);
        self.buffer.push(row);
        if self.buffer_bytes >= self.threshold_bytes {
            self.spill_buffer()?;
        }
        Ok(())
    }

    /// Sort the current buffer and write it to a new run file, then clear it.
    fn spill_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_by(|a, b| self.order.compare(a, b));
        let path = self.dir.join(format!("run-{:08}.bin", self.runs.len()));
        let file = File::create(&path)
            .map_err(|e| io_error(format!("external sort: create run {}: {e}", path.display())))?;
        let mut w = BufWriter::new(file);
        // Drain the buffer so rows move out (no clone) into the writer.
        for row in self.buffer.drain(..) {
            write_row(&mut w, &row)?;
        }
        w.flush()
            .map_err(|e| io_error(format!("external sort: flush run {}: {e}", path.display())))?;
        self.buffer_bytes = 0;
        self.runs.push(path);
        self.spilled = true;
        Ok(())
    }

    /// Finish sorting and return the fully ordered rows.
    ///
    /// - **No spill occurred**: the buffer is sorted in place and returned — the
    ///   pure in-memory fast path (bounded, since it never crossed threshold).
    /// - **Spills occurred**: the final buffer is spilled too, then the runs are
    ///   **cascade-merged down to at most `MERGE_FANIN` runs** before the final
    ///   k-way merge. This caps peak open readers (and heap size) at `MERGE_FANIN`
    ///   regardless of how many runs (i.e. how large the result) — so the sort's
    ///   working set is bounded independent of the row count.
    pub fn finish(mut self) -> Result<SortedRows> {
        if self.runs.is_empty() {
            self.buffer.sort_by(|a, b| self.order.compare(a, b));
            let rows = std::mem::take(&mut self.buffer);
            return Ok(SortedRows::InMemory(rows.into_iter()));
        }
        // Spill whatever remains so the merge reads uniformly from run files.
        self.spill_buffer()?;
        // Cascade-merge until at most MERGE_FANIN runs remain, bounding the final
        // merge's open readers/heap at MERGE_FANIN.
        self.reduce_runs_to_fanin()?;
        let merger = KWayMerge::open(&self.runs, self.order.clone())?;
        Ok(SortedRows::Merged(merger))
    }

    /// Repeatedly merge groups of at most `MERGE_FANIN` runs into single larger
    /// runs until the total run count is `<= MERGE_FANIN`. Each pass opens at
    /// most `MERGE_FANIN` readers at once, so peak memory during reduction is
    /// `O(MERGE_FANIN)`, not `O(runs)`.
    fn reduce_runs_to_fanin(&mut self) -> Result<()> {
        let mut next_gen = 0usize;
        while self.runs.len() > MERGE_FANIN {
            let mut merged: Vec<PathBuf> = Vec::new();
            // Consume the current runs in fixed-size groups.
            let current = std::mem::take(&mut self.runs);
            for group in current.chunks(MERGE_FANIN) {
                if group.len() == 1 {
                    // A lone leftover run passes through unchanged.
                    merged.push(group[0].clone());
                    continue;
                }
                let out = self
                    .dir
                    .join(format!("merge-{next_gen:04}-{:08}.bin", merged.len()));
                merge_runs_into(group, &self.order, &out)?;
                // The input runs are now subsumed by `out`; remove them to keep
                // disk bounded. Failure to unlink is logged, not fatal (the
                // reservation dir is cleaned up wholesale on drop).
                for r in group {
                    if let Err(e) = std::fs::remove_file(r) {
                        tracing::debug!(path = %r.display(), %e, "external sort: could not remove merged run");
                    }
                }
                merged.push(out);
            }
            self.runs = merged;
            next_gen += 1;
        }
        Ok(())
    }
}

/// Maximum number of runs merged at once (peak open readers / heap size). Chosen
/// so the merge working set is a small constant independent of the result size;
/// larger fan-in means fewer passes but more concurrent readers.
const MERGE_FANIN: usize = 64;

/// Merge the sorted run files in `inputs` into one sorted run at `out` via a
/// bounded k-way merge, then return. Rows move through one at a time — the
/// output run is written streaming, so this holds `<= inputs.len()` rows.
fn merge_runs_into(inputs: &[PathBuf], order: &RowOrder, out: &std::path::Path) -> Result<()> {
    let mut merger = KWayMerge::open(inputs, order.clone())?;
    let file = File::create(out).map_err(|e| {
        io_error(format!(
            "external sort: create merge {}: {e}",
            out.display()
        ))
    })?;
    let mut w = BufWriter::new(file);
    while let Some(row) = merger.next_row()? {
        write_row(&mut w, &row)?;
    }
    w.flush()
        .map_err(|e| io_error(format!("external sort: flush merge {}: {e}", out.display())))?;
    Ok(())
}

/// The sorted output of an [`ExternalSorter`]. Iterating yields rows in order.
///
/// Each item is a `Result<Row>`: the merge does real I/O, so a read error on a
/// spilled run surfaces here rather than being swallowed.
pub enum SortedRows {
    /// Fast path: everything fit in memory; already-sorted rows.
    InMemory(std::vec::IntoIter<Row>),
    /// Spilled path: a bounded k-way merge over run files.
    Merged(KWayMerge),
}

impl Iterator for SortedRows {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SortedRows::InMemory(iter) => iter.next().map(Ok),
            SortedRows::Merged(merger) => merger.next_row().transpose(),
        }
    }
}

/// One run file's reader. Its head row lives in the merge heap; the reader is
/// pulled to refill the heap slot after each pop.
struct RunCursor {
    reader: BufReader<File>,
}

/// A heap entry ordering run heads by the sort key. The `run_idx` breaks ties
/// deterministically so the merge is stable and total.
struct HeapEntry {
    row: Row,
    run_idx: usize,
    order: RowOrder,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap; we want the SMALLEST key on top, so invert
        // the key comparison. Ties fall back to run_idx (also inverted) so the
        // total order is deterministic and stable across runs.
        self.order
            .compare(&self.row, &other.row)
            .then(self.run_idx.cmp(&other.run_idx))
            .reverse()
    }
}

/// Bounded k-way merge of sorted run files.
///
/// The heap holds at most one entry per run, so peak memory is
/// O(number_of_runs), independent of the total number of rows.
pub struct KWayMerge {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<HeapEntry>,
    order: RowOrder,
}

impl KWayMerge {
    fn open(runs: &[PathBuf], order: RowOrder) -> Result<Self> {
        let mut cursors = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (run_idx, path) in runs.iter().enumerate() {
            let file = File::open(path).map_err(|e| {
                io_error(format!("external sort: open run {}: {e}", path.display()))
            })?;
            let mut reader = BufReader::new(file);
            if let Some(row) = read_row(&mut reader)? {
                heap.push(HeapEntry {
                    row,
                    run_idx,
                    order: order.clone(),
                });
            }
            // Keep a cursor slot per run for index alignment. An empty run's
            // slot is simply never refilled (nothing was pushed for it).
            cursors.push(RunCursor { reader });
        }
        Ok(Self {
            cursors,
            heap,
            order,
        })
    }

    /// Emit the next row in sorted order, or `Ok(None)` when all runs drain.
    fn next_row(&mut self) -> Result<Option<Row>> {
        let Some(top) = self.heap.pop() else {
            return Ok(None);
        };
        let run_idx = top.run_idx;
        // Refill this run's slot with its next row before returning `top.row`,
        // so exactly one in-flight row per run is held.
        let cursor = &mut self.cursors[run_idx];
        if let Some(next) = read_row(&mut cursor.reader)? {
            self.heap.push(HeapEntry {
                row: next,
                run_idx,
                order: self.order.clone(),
            });
        }
        Ok(Some(top.row))
    }
}

/// Sort `rows` fully in memory (used by the fast path and as the reference).
#[cfg(test)]
pub(crate) fn sort_in_memory(mut rows: Vec<Row>, order: &RowOrder) -> Vec<Row> {
    rows.sort_by(|a, b| order.compare(a, b));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn irow(v: i64) -> Row {
        vec![Some(CqlValue::Bigint(v))]
    }

    fn ival(r: &Row) -> i64 {
        match r[0] {
            Some(CqlValue::Bigint(n)) => n,
            _ => panic!("expected bigint row"),
        }
    }

    #[test]
    fn in_memory_when_under_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, true)]);
        // Huge threshold → never spills.
        let mut s = ExternalSorter::new(dir.path(), order, u64::MAX);
        for v in [5i64, 1, 3, 2, 4] {
            s.push(irow(v)).unwrap();
        }
        assert!(!s.spilled());
        let out: Vec<i64> = s.finish().unwrap().map(|r| ival(&r.unwrap())).collect();
        assert_eq!(out, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn spills_and_merges_ascending() {
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, true)]);
        // Tiny threshold forces a spill roughly every row.
        let mut s = ExternalSorter::new(dir.path(), order, 1);
        let input = [9i64, 3, 7, 1, 8, 2, 6, 0, 5, 4];
        for v in input {
            s.push(irow(v)).unwrap();
        }
        assert!(s.spilled(), "tiny threshold must spill");
        assert!(s.run_count() >= 2, "expected multiple runs");
        let out: Vec<i64> = s.finish().unwrap().map(|r| ival(&r.unwrap())).collect();
        assert_eq!(out, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn spills_and_merges_descending() {
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, false)]);
        let mut s = ExternalSorter::new(dir.path(), order, 1);
        for v in [3i64, 1, 4, 1, 5, 9, 2, 6] {
            s.push(irow(v)).unwrap();
        }
        assert!(s.spilled());
        let out: Vec<i64> = s.finish().unwrap().map(|r| ival(&r.unwrap())).collect();
        assert_eq!(out, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    /// Tiny deterministic LCG so this unit test needs no `rand` dependency.
    /// (The full property test with `rand`/`proptest` lives in `ferrosa-cql`.)
    struct Lcg(u64);
    impl Lcg {
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn range(&mut self, lo: i64, hi: i64) -> i64 {
            let span = (hi - lo) as u64;
            lo + (self.next_u64() % span) as i64
        }
    }

    #[test]
    fn spilled_result_equals_in_memory_reference_randomized() {
        let mut rng = Lcg(0xF355_0A5A);
        let order = RowOrder::new(vec![(0, true)]);
        for trial in 0..20 {
            let n = rng.range(0, 2_000) as usize;
            let rows: Vec<Row> = (0..n).map(|_| irow(rng.range(-1_000, 1_000))).collect();

            let reference = sort_in_memory(rows.clone(), &order);

            let dir = tempfile::tempdir().unwrap();
            let mut s = ExternalSorter::new(dir.path(), order.clone(), 64);
            for r in rows {
                s.push(r).unwrap();
            }
            let got: Vec<Row> = s.finish().unwrap().map(|r| r.unwrap()).collect();

            assert_eq!(
                got.len(),
                reference.len(),
                "trial {trial}: length differs (n={n})"
            );
            let got_vals: Vec<i64> = got.iter().map(ival).collect();
            let ref_vals: Vec<i64> = reference.iter().map(ival).collect();
            assert_eq!(
                got_vals, ref_vals,
                "trial {trial}: spilled order != in-memory reference (n={n})"
            );
        }
    }

    #[test]
    fn cascade_merge_over_many_runs_stays_correct() {
        // Force FAR more than MERGE_FANIN runs (threshold=1 → ~1 run per row) so
        // finish() must cascade-merge across multiple passes. The result must
        // still be a total, correct order with no loss/dup.
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, true)]);
        let mut s = ExternalSorter::new(dir.path(), order.clone(), 1);
        let mut rng = Lcg(0xABCD_1234);
        let n = 5_000; // >> MERGE_FANIN (64) → guaranteed multi-pass cascade
        let mut expected: Vec<i64> = Vec::with_capacity(n);
        for _ in 0..n {
            let v = rng.range(-50_000, 50_000);
            expected.push(v);
            s.push(irow(v)).unwrap();
        }
        assert!(s.run_count() > MERGE_FANIN, "must exceed the merge fan-in");
        expected.sort_unstable();
        let got: Vec<i64> = s.finish().unwrap().map(|r| ival(&r.unwrap())).collect();
        assert_eq!(got.len(), expected.len(), "no rows lost or duplicated");
        assert_eq!(
            got, expected,
            "cascade merge must be totally correctly ordered"
        );
    }

    #[test]
    fn nulls_sort_first_ascending() {
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, true)]);
        let mut s = ExternalSorter::new(dir.path(), order, 1);
        s.push(vec![Some(CqlValue::Bigint(2))]).unwrap();
        s.push(vec![None]).unwrap();
        s.push(vec![Some(CqlValue::Bigint(1))]).unwrap();
        let out: Vec<Option<i64>> = s
            .finish()
            .unwrap()
            .map(|r| {
                r.unwrap()[0].as_ref().map(|v| match v {
                    CqlValue::Bigint(n) => *n,
                    _ => panic!(),
                })
            })
            .collect();
        assert_eq!(out, vec![None, Some(1), Some(2)]);
    }

    #[test]
    fn multi_column_order() {
        let dir = tempfile::tempdir().unwrap();
        // Sort by col0 asc, then col1 desc.
        let order = RowOrder::new(vec![(0, true), (1, false)]);
        let mk = |a: i64, b: i64| vec![Some(CqlValue::Bigint(a)), Some(CqlValue::Bigint(b))];
        let mut s = ExternalSorter::new(dir.path(), order, 1);
        for (a, b) in [(1, 1), (1, 3), (2, 5), (1, 2), (2, 4)] {
            s.push(mk(a, b)).unwrap();
        }
        let out: Vec<(i64, i64)> = s
            .finish()
            .unwrap()
            .map(|r| {
                let row = r.unwrap();
                let g = |i: usize| match row[i] {
                    Some(CqlValue::Bigint(n)) => n,
                    _ => panic!(),
                };
                (g(0), g(1))
            })
            .collect();
        assert_eq!(out, vec![(1, 3), (1, 2), (1, 1), (2, 5), (2, 4)]);
    }

    #[test]
    fn empty_input_yields_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let order = RowOrder::new(vec![(0, true)]);
        let s = ExternalSorter::new(dir.path(), order, 1);
        let out: Vec<Row> = s.finish().unwrap().map(|r| r.unwrap()).collect();
        assert!(out.is_empty());
    }

    #[test]
    fn truncated_run_fails_loud() {
        // A run file cut off mid-record must error, not silently stop early.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run-00000000.bin");
        // Claim 100 bytes but write only 3 → truncated.
        let mut f = File::create(&path).unwrap();
        f.write_all(&100u32.to_le_bytes()).unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
        drop(f);
        let mut r = BufReader::new(File::open(&path).unwrap());
        let err = read_row(&mut r).unwrap_err();
        assert!(
            format!("{err}").contains("truncated"),
            "expected truncated-run error, got {err}"
        );
    }
}
