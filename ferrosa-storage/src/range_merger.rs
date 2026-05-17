//! Lazy k-way merge across memtable + flushing memtable + SSTable
//! iterators (ADR-020). Yields one merged-and-deletion-suppressed
//! partition at a time in token order — no Vec<Partition> ever held
//! for the whole table.
//!
//! Memory profile: peak is one Partition (the currently-emitting
//! group's merged result) plus one Partition cached per source as a
//! peek. With ~3 sources (memtable + flushing + a few SSTables) the
//! resident set is O(num_sources) partitions, not O(table_size).
//!
//! Used by [`TableStore::range_iter`] to back the
//! `ferrosa_cluster::coordinator::stream_request_handler::StreamRangeReader`
//! Phase 2 path.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::Result;
use ferrosa_sstable::reader::{PartitionIter, SSTableReader};
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::ReadAt;

use crate::merge;

/// One source contributing partitions to the merge. Wraps either a
/// memtable iterator (infallible) or an SSTable streaming iterator
/// (fallible per-partition).
pub enum MergeSource<'a, R: ReadAt> {
    /// Memtable / flushing memtable: infallible per-partition yield.
    Memtable(Box<dyn Iterator<Item = Partition> + Send + 'a>),
    /// SSTable streaming reader. Yields full partitions including
    /// decoded cells.
    SsTable(PartitionIter<'a, R>),
    /// SSTable streaming reader in METADATA-ONLY mode (clustering +
    /// liveness + row-deletion decoded, cells byte-skipped). Used by
    /// the COUNT(*) fast path.
    SsTableMetadata(PartitionIter<'a, R>),
    /// SSTable streaming reader with COLUMN PROJECTION — decodes
    /// only the cells whose ordinals are in `wanted`; the rest are
    /// byte-skipped via `read_cell_skip`. Used by the CQL projection
    /// fast path so `SELECT a, b FROM t` on a wide table doesn't pay
    /// the cell read+decode cost for columns the caller doesn't want.
    SsTableProjected {
        iter: PartitionIter<'a, R>,
        /// Column ordinals to retain. Order matches
        /// `SerializationHeader::regular_columns` (and
        /// `static_columns` for static rows). Empty = no cells.
        wanted: &'a [u16],
    },
    /// LSM-level-style run of token-disjoint SSTables. The run is
    /// read **sequentially** (concatenated) instead of contributing
    /// `len()` separate slots to the k-way merge heap — collapses
    /// O(num_sstables) initial peeks into O(num_runs).
    SsTableRun(SsTableRunIter<'a, R>),
}

impl<'a, R: ReadAt> MergeSource<'a, R> {
    fn next_partition(&mut self) -> Result<Option<Partition>> {
        match self {
            Self::Memtable(it) => Ok(it.next()),
            Self::SsTable(it) => it.next_partition(),
            Self::SsTableMetadata(it) => it.next_partition_metadata(),
            Self::SsTableProjected { iter, wanted } => iter.next_partition_projected(wanted),
            Self::SsTableRun(run) => run.next_partition(),
        }
    }
}

/// Iterator over a sequence of token-disjoint SSTables — emits
/// every partition of the first, then the second, etc. Because the
/// run is token-disjoint by construction, concatenation preserves
/// global token order without any per-emission heap operations.
///
/// `Mode` controls per-cell decoding behavior — the same `Full /
/// Metadata / Projected` choice the merger exposes for single-
/// SSTable sources, lifted into the run.
pub struct SsTableRunIter<'a, R: ReadAt> {
    sstables: &'a [Arc<SSTableReader<R>>],
    mode: RunMode<'a>,
    cursor: usize,
    /// `partitions_iter()` for `sstables[cursor]`; `None` until
    /// first use or after the current table exhausts.
    current: Option<PartitionIter<'a, R>>,
}

#[derive(Clone, Copy)]
pub enum RunMode<'a> {
    Full,
    Metadata,
    Projected(&'a [u16]),
}

impl<'a, R: ReadAt> SsTableRunIter<'a, R> {
    pub fn new(sstables: &'a [Arc<SSTableReader<R>>], mode: RunMode<'a>) -> Self {
        Self {
            sstables,
            mode,
            cursor: 0,
            current: None,
        }
    }

    fn next_partition(&mut self) -> Result<Option<Partition>> {
        loop {
            if self.current.is_none() {
                if self.cursor >= self.sstables.len() {
                    return Ok(None);
                }
                match self.sstables[self.cursor].partitions_iter() {
                    Ok(it) => self.current = Some(it),
                    Err(e) => {
                        tracing::warn!(
                            cursor = self.cursor,
                            "SsTableRunIter: open failed, skipping table: {e}"
                        );
                        self.cursor += 1;
                        continue;
                    }
                }
            }
            let it = self.current.as_mut().expect("just initialised");
            let result = match self.mode {
                RunMode::Full => it.next_partition(),
                RunMode::Metadata => it.next_partition_metadata(),
                RunMode::Projected(wanted) => it.next_partition_projected(wanted),
            };
            match result {
                Ok(Some(p)) => return Ok(Some(p)),
                Ok(None) => {
                    // Exhausted the current table; advance.
                    self.current = None;
                    self.cursor += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        cursor = self.cursor,
                        "SsTableRunIter: read failed, dropping rest of table: {e}"
                    );
                    self.current = None;
                    self.cursor += 1;
                }
            }
        }
    }
}

/// Greedy interval-coloring: group `sstables` into the smallest
/// number of **token-disjoint runs** (per-run sequence of SSTables
/// where each table's `smallest_key` is strictly greater than the
/// previous table's `largest_key`). Each run can then be read by
/// concatenation instead of contributing `n` separate slots to the
/// merger's heap.
///
/// Algorithm (classic interval-scheduling):
///   1. Sort SSTables by smallest_key.
///   2. For each, place it in the first run whose last table's
///      largest_key < this smallest_key. Open a new run otherwise.
///
/// Number of runs = max number of SSTables that overlap at any
/// token point. For an effectively-compacted LSM that's `O(log
/// num_sstables)`; in the worst case it equals `num_sstables`.
///
/// Returns runs as `Vec<Vec<Arc<SSTableReader>>>` — each inner Vec
/// is sorted by token range so concatenation produces sorted
/// output.
pub fn partition_into_disjoint_runs<R: ReadAt + Send + Sync + 'static>(
    sstables: &[Arc<SSTableReader<R>>],
) -> Vec<Vec<Arc<SSTableReader<R>>>> {
    let bounds: Vec<(Vec<u8>, Vec<u8>)> = sstables
        .iter()
        .map(|sst| {
            (
                sst.smallest_key_bytes().to_vec(),
                sst.largest_key_bytes().to_vec(),
            )
        })
        .collect();
    let runs_of_indices = group_disjoint_runs(&bounds);
    runs_of_indices
        .into_iter()
        .map(|indices| {
            indices
                .into_iter()
                .map(|i| Arc::clone(&sstables[i]))
                .collect()
        })
        .collect()
}

/// Pure interval-coloring on byte-comparable `(smallest, largest)`
/// bounds — extracted from `partition_into_disjoint_runs` so the
/// algorithm can be unit-tested without building real SSTable
/// readers.
///
/// Returns a `Vec` of runs; each inner `Vec<usize>` is a sequence
/// of indices into `bounds` whose `(smallest, largest)` ranges are
/// pairwise token-disjoint AND sorted in ascending order of
/// `smallest`. Number of runs equals the maximum number of input
/// intervals that overlap at any single token point.
///
/// Empty input → empty output.
pub fn group_disjoint_runs(bounds: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<usize>> {
    if bounds.is_empty() {
        return Vec::new();
    }
    let mut indexed: Vec<usize> = (0..bounds.len()).collect();
    indexed.sort_by(|&a, &b| bounds[a].0.cmp(&bounds[b].0));

    let mut runs: Vec<Vec<usize>> = Vec::new();
    for idx in indexed {
        let smallest = &bounds[idx].0;
        let mut placed = false;
        for run in &mut runs {
            let last_idx = *run.last().expect("non-empty run");
            let last_largest = &bounds[last_idx].1;
            if smallest.as_slice() > last_largest.as_slice() {
                run.push(idx);
                placed = true;
                break;
            }
        }
        if !placed {
            runs.push(vec![idx]);
        }
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::group_disjoint_runs;

    fn b(s: &[u8]) -> Vec<u8> {
        s.to_vec()
    }

    /// Token-disjoint SSTables in input order collapse into a
    /// single run — the dream LSM-leveled state.
    #[test]
    fn disjoint_in_order_yields_one_run() {
        let bounds = vec![
            (b(b"a"), b(b"c")),
            (b(b"d"), b(b"f")),
            (b(b"g"), b(b"i")),
        ];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 1, "all disjoint → exactly one run");
        assert_eq!(runs[0], vec![0, 1, 2]);
    }

    /// Disjoint SSTables given OUT OF ORDER still collapse into one
    /// run, with the run sorted by smallest key. Proves the sort
    /// step works.
    #[test]
    fn disjoint_out_of_order_sorts_into_one_run() {
        let bounds = vec![
            (b(b"g"), b(b"i")),
            (b(b"a"), b(b"c")),
            (b(b"d"), b(b"f")),
        ];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 1);
        // Run holds indices sorted by smallest_key: 1 (a-c), 2
        // (d-f), 0 (g-i).
        assert_eq!(runs[0], vec![1, 2, 0]);
    }

    /// Two SSTables with identical ranges cannot share a run — they
    /// overlap. Output is two runs with one table each.
    #[test]
    fn fully_overlapping_split_across_runs() {
        let bounds = vec![(b(b"a"), b(b"z")), (b(b"a"), b(b"z"))];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0], vec![0]);
        assert_eq!(runs[1], vec![1]);
    }

    /// Partial overlap: [a,m] and [k,z] share keys k..m. Two runs.
    #[test]
    fn partial_overlap_two_runs() {
        let bounds = vec![(b(b"a"), b(b"m")), (b(b"k"), b(b"z"))];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 2);
    }

    /// Three tables where 0 and 1 overlap, but 2 is disjoint from
    /// both. Greedy places 2 into run-0 (after 0) or run-1 (after
    /// 1), whichever scanner reaches first — but the result must be
    /// at most 2 runs (the count of pairwise overlaps at any
    /// point).
    #[test]
    fn one_overlap_extra_disjoint_fits_into_existing_run() {
        let bounds = vec![
            (b(b"a"), b(b"m")), // run-0
            (b(b"k"), b(b"q")), // overlaps with #0 → run-1
            (b(b"r"), b(b"z")), // disjoint from both → fits into either
        ];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 2, "max overlap is 2 → 2 runs suffice");
        let total_placed: usize = runs.iter().map(|r| r.len()).sum();
        assert_eq!(total_placed, 3, "every input placed exactly once");
    }

    /// Three tables all overlapping at one token point → 3 runs.
    /// Demonstrates worst-case fragmentation = number of intervals
    /// covering the densest point.
    #[test]
    fn fully_overlapping_three_yields_three_runs() {
        let bounds = vec![
            (b(b"a"), b(b"z")),
            (b(b"b"), b(b"y")),
            (b(b"c"), b(b"x")),
        ];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 3);
    }

    /// Edge: empty input → empty output, no panic.
    #[test]
    fn empty_input_no_runs() {
        let runs = group_disjoint_runs(&[]);
        assert!(runs.is_empty());
    }

    /// Edge: single SSTable always yields exactly one run with one
    /// member.
    #[test]
    fn single_table_one_run() {
        let bounds = vec![(b(b"a"), b(b"z"))];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs, vec![vec![0]]);
    }

    /// Within each run, entries must be sorted by smallest key so
    /// concatenation produces sorted output. This invariant is
    /// load-bearing for the merger (SsTableRunIter walks the run
    /// in order and assumes the result is globally token-sorted).
    #[test]
    fn runs_are_sorted_by_smallest_key_internally() {
        let bounds = vec![
            (b(b"d"), b(b"f")),
            (b(b"a"), b(b"c")),
            (b(b"g"), b(b"i")),
            (b(b"j"), b(b"l")),
        ];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 1);
        let run = &runs[0];
        for w in run.windows(2) {
            let prev_smallest = &bounds[w[0]].0;
            let next_smallest = &bounds[w[1]].0;
            assert!(
                prev_smallest <= next_smallest,
                "run not sorted: {prev_smallest:?} > {next_smallest:?}",
            );
        }
    }
}

/// Heap entry: one peeked partition per still-active source. The
/// heap is min-keyed; ties (same DecoratedKey across sources) are
/// broken by source index for total ordering — the merger groups
/// all same-key partitions into a single merge call.
struct HeapEntry {
    key: DecoratedKey,
    src: usize,
    partition: Partition,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.src == other.src
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse — BinaryHeap is a max-heap; we want smallest key
        // popped first.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.src.cmp(&self.src))
    }
}

/// Lazy merger. Construct with the source iterators (any order),
/// then pull `next_merged_partition()` until `Ok(None)`.
pub struct RangeMerger<'a, R: ReadAt> {
    sources: Vec<MergeSource<'a, R>>,
    heap: BinaryHeap<HeapEntry>,
    /// Optional inclusive lower bound (skip partitions with key < start).
    start: Option<DecoratedKey>,
    /// Optional inclusive upper bound (stop yielding once key > end).
    end: Option<DecoratedKey>,
    /// Set once a key > end is observed so we stop pulling sources.
    exhausted: bool,
    /// Leaked allocation backing the per-run `Vec<Arc<SSTableReader>>`
    /// slices that `SsTableRunIter`s borrow from. `Some` only when
    /// the merger was built via `build_merger_with_runs`; freed by
    /// the `Drop` impl below. `*const` rather than `Box` because
    /// `Box::leak`'s output has to be re-wrapped manually.
    runs_arena: Option<*const [Vec<Arc<SSTableReader<R>>>]>,
}

// SAFETY: `runs_arena` is a leaked allocation that contains only
// `Vec<Arc<SSTableReader<R>>>`. `Arc<SSTableReader<R>>` is `Send`
// and `Sync` when `R` is, so the pointer is safe to send across
// thread boundaries. We only ever read from it on the thread that
// owns the merger (the spawn_blocking task), but a `Send` bound is
// required so the merger itself can be moved into the task.
unsafe impl<'a, R: ReadAt + Send + Sync> Send for RangeMerger<'a, R> {}

impl<'a, R: ReadAt> Drop for RangeMerger<'a, R> {
    fn drop(&mut self) {
        if let Some(ptr) = self.runs_arena.take() {
            // SAFETY: ptr came from `Box::leak(Box<[Vec<...>]>)`
            // in `build_merger_with_runs`. Reconstructing the Box
            // here drops the leaked allocation. All `SsTableRunIter`
            // instances that borrowed from the slab live inside
            // `self.sources` and are dropped before this Drop impl
            // runs (struct fields are dropped in declaration order
            // and `sources` is before `runs_arena`).
            unsafe {
                drop(Box::from_raw(ptr as *mut [Vec<Arc<SSTableReader<R>>>]));
            }
        }
    }
}

impl<'a, R: ReadAt> RangeMerger<'a, R> {
    pub fn new(
        sources: Vec<MergeSource<'a, R>>,
        start: Option<DecoratedKey>,
        end: Option<DecoratedKey>,
    ) -> Self {
        let mut merger = Self {
            sources,
            heap: BinaryHeap::new(),
            start,
            end,
            exhausted: false,
            runs_arena: None,
        };
        // Prime the heap with one partition per non-empty source.
        // We need a Vec of indices to avoid double-borrowing `merger.sources`.
        let n = merger.sources.len();
        for src in 0..n {
            merger.refill_source(src);
        }
        merger
    }

    /// Pull the next available partition from `src` (skipping over
    /// entries below `start`) and push it onto the heap.
    fn refill_source(&mut self, src: usize) {
        loop {
            match self.sources[src].next_partition() {
                Ok(Some(partition)) => {
                    if let Some(ref s) = self.start {
                        if partition.key < *s {
                            continue;
                        }
                    }
                    self.heap.push(HeapEntry {
                        key: partition.key.clone(),
                        src,
                        partition,
                    });
                    return;
                }
                Ok(None) => return,
                Err(e) => {
                    tracing::warn!(src, "range_merger: source error, dropping source: {e}");
                    return;
                }
            }
        }
    }

    /// Pull one merged partition. Returns `Ok(None)` when every
    /// source is exhausted (or the upper bound was crossed). Pops
    /// the smallest-key entry; if more entries share that key,
    /// drains them too and merges via `merge::merge_partitions`;
    /// applies deletion suppression before yielding.
    pub fn next_merged_partition(&mut self) -> Result<Option<Partition>> {
        if self.exhausted {
            return Ok(None);
        }
        // Pop the smallest-key entry.
        let first = match self.heap.pop() {
            Some(e) => e,
            None => return Ok(None),
        };
        // Upper-bound check — once the smallest key in the heap is
        // past `end`, all remaining entries are too (the heap is
        // min-key ordered).
        if let Some(ref e) = self.end {
            if first.key > *e {
                self.exhausted = true;
                return Ok(None);
            }
        }
        let key = first.key.clone();
        let first_src = first.src;
        let mut group: Vec<Partition> = vec![first.partition];

        // Drain any other heap entries with the SAME key.
        let mut refill_srcs: Vec<usize> = vec![first_src];
        while let Some(top) = self.heap.peek() {
            if top.key != key {
                break;
            }
            let entry = self.heap.pop().expect("peek succeeded");
            refill_srcs.push(entry.src);
            group.push(entry.partition);
        }

        // Refill every source we drained from so the next call sees
        // a fresh peek.
        for src in refill_srcs {
            self.refill_source(src);
        }

        // Merge same-key group (no-op for group of 1, but still
        // useful to apply deletion suppression uniformly).
        let mut merged = if group.len() == 1 {
            group.into_iter().next().unwrap()
        } else {
            merge::merge_partitions(group)
        };
        merge::apply_deletions(&mut merged);
        Ok(Some(merged))
    }
}

/// Convenience constructor: build a merger from explicit source
/// inputs. `active_iter` and `flushing_iter` are memtable iterators;
/// `sstables` are the per-SSTable Arcs whose `partitions_iter()`
/// will be consumed.
pub fn merger_for_sources<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    Ok(build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Full,
        start,
        end,
    ))
}

/// Projection variant: SSTables decode only the cells whose
/// ordinals are in `wanted`; memtable contributions retain their
/// full cells (already in memory; stripping costs more than it
/// saves). `merge::merge_partitions` correctly merges across the
/// mixed-projection rows because dedup is keyed on the clustering
/// key, not on the cell payload. Used by the CQL projection fast
/// path.
pub fn merger_for_projected_sources<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    wanted: &'a [u16],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    Ok(build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Projected(wanted),
        start,
        end,
    ))
}

/// Metadata-only variant: SSTables use `next_partition_metadata`
/// so cell payloads are byte-skipped. Memtables still contribute
/// full partitions (they're already in memory; stripping cells
/// would cost a clone with no IO savings — `merge::merge_partitions`
/// produces the correct merged shape anyway, and the caller is
/// expected to read only `rows.len()` / `clustering` from the
/// output). Used by the COUNT(*) fast path.
pub fn merger_for_metadata_sources<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    Ok(build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Metadata,
        start,
        end,
    ))
}

/// Shared back-end for the three `merger_for_*_sources` constructors:
/// group SSTables into token-disjoint runs (interval coloring), then
/// build one `MergeSource::SsTableRun` per run rather than one
/// `MergeSource::SsTable*` per SSTable. Cuts the merger heap's
/// initial peek count from `O(num_sstables)` to `O(num_runs)` —
/// dramatic for fragmented LSM states where the leader has 100+
/// SSTables on a single table.
fn build_merger_with_runs<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    mode: RunMode<'a>,
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> RangeMerger<'a, R> {
    // We can't actually keep the runs as `Vec<Vec<Arc<...>>>` because
    // SsTableRunIter borrows from a slice (`&'a [Arc<...>]`), and a
    // freshly allocated Vec<Arc<...>> wouldn't have the same lifetime
    // as the input slice. Instead, we compute the run *indices* via
    // `partition_into_disjoint_runs`, then turn those into
    // `&'a [Arc<...>]` sub-slices into the original `sstables` slice
    // by detecting contiguous-after-sort groupings.
    //
    // The greedy interval-coloring algorithm produces runs that are
    // not necessarily *contiguous* in the input order — so we need
    // to either:
    //   (a) keep the per-run Vec owned by the merger, or
    //   (b) accept that some runs will produce non-sorted output if
    //       not stable-sorted.
    //
    // Option (a) is straightforward: own the per-run Vec, pass a
    // borrow into the SsTableRunIter. We do that here.
    let runs = partition_into_disjoint_runs(sstables);
    // Move the Vec<Vec<Arc>> into a boxed slab the merger owns;
    // SsTableRunIter holds `&'a [Arc<...>]` which we satisfy by
    // leaking the slab to the merger's lifetime. We allocate it
    // into a `Box<[Vec<Arc<...>>]>` and stash on the merger so it
    // lives as long as the iterators borrowing from it.
    let mut sources: Vec<MergeSource<'a, R>> = Vec::with_capacity(2 + runs.len());
    sources.push(MergeSource::Memtable(active_iter));
    if let Some(it) = flushing_iter {
        sources.push(MergeSource::Memtable(it));
    }
    let mut owned_runs: Vec<Vec<Arc<SSTableReader<R>>>> = runs;
    // We need a stable slice address per run. `Box<[Vec<...>]>` gives
    // that — but borrowing from inside via `&self.runs[i]` requires
    // the merger to own it. Stash into RangeMerger's `owned_runs`
    // field (added below) and create SsTableRunIter borrowing from
    // those entries.
    //
    // Lifetime trick: we extend the per-run slices to `'a` via the
    // standard "self-ref with stable allocation" pattern — `Box<[T]>`
    // owned by RangeMerger, slices into it that outlive the iters.
    // This is sound because Vec<Arc<T>> never moves once boxed.
    let runs_arena: Box<[Vec<Arc<SSTableReader<R>>>]> =
        std::mem::take(&mut owned_runs).into_boxed_slice();
    let runs_ptr: *const [Vec<Arc<SSTableReader<R>>>] = Box::leak(runs_arena);
    // SAFETY: we own this allocation for the merger's lifetime and
    // never move/free it until the merger is dropped (handled in
    // RangeMerger::drop). The slices we hand to SsTableRunIter are
    // therefore valid for `'a`.
    let runs_ref: &'a [Vec<Arc<SSTableReader<R>>>] = unsafe { &*runs_ptr };
    for run in runs_ref.iter() {
        sources.push(MergeSource::SsTableRun(SsTableRunIter::new(
            run.as_slice(),
            mode,
        )));
    }
    let mut merger = RangeMerger::new(sources, start, end);
    merger.runs_arena = Some(runs_ptr);
    merger
}
