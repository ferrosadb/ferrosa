//! Lazy k-way merge across memtable + flushing memtable + SSTable
//! iterators (ADR-020). Yields one merged-and-deletion-suppressed
//! partition at a time in token order — no `Vec<Partition>` ever held
//! for the whole table.
//!
//! Memory profile: peak is one Partition (the currently-emitting
//! group's merged result) plus one Partition cached per source as a
//! peek. With ~3 sources (memtable + flushing + a few SSTables) the
//! resident set is O(num_sources) partitions, not O(table_size).
//!
//! Used by `TableStore::range_iter` to back the
//! `ferrosa_cluster::coordinator::stream_request_handler::StreamRangeReader`
//! Phase 2 path.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{Error, Result};
use ferrosa_sstable::reader::{PartitionIter, SSTableReader};
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::ReadAt;

use crate::merge;

/// One source contributing partitions to the merge.
///
/// Each variant supports a **peek/pop split**: `peek_key()` returns
/// the next partition's key without decoding the body (cheap — for
/// SSTables it reads a ~10-byte header), and `pop_partition()`
/// decodes and consumes the full partition. This is the cold-cache
/// fast-path: the merger primes its heap with `(key, source_id)`
/// pairs via `peek_key`, then decodes the body only when the source
/// is popped — turning `O(num_sources × cold_body_decode)` into
/// `O(num_sources × cold_header_read) + O(emitted × cold_body_decode)`.
///
/// For memtable sources the iterator yields owned `Partition`s and
/// there is no cheap "peek key" primitive — so peek reads + caches
/// the whole partition. That's still free in practice (memtables are
/// in-memory) and pop takes the cached partition.
pub enum MergeSource<'a, R: ReadAt> {
    /// Memtable / flushing memtable: infallible per-partition yield.
    /// `peeked` caches the result of the most recent `peek_key()`
    /// (the full partition, since memtables yield owned partitions);
    /// `pop_partition` takes it.
    Memtable {
        iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
        peeked: Option<Partition>,
    },
    /// Single SSTable streaming reader. `mode` controls per-cell
    /// decoding on `pop_partition`. `peek_key` reads only the
    /// partition header (~10 bytes) via `PartitionIter::peek_partition_key`
    /// and caches the key in `peeked_key`; `pop_partition` decodes
    /// the body from the same offset and clears the cache.
    SsTable {
        iter: PartitionIter<'a, R>,
        mode: RunMode<'a>,
        peeked_key: Option<DecoratedKey>,
    },
    /// LSM-level-style run of token-disjoint SSTables. The run is
    /// read **sequentially** (concatenated) instead of contributing
    /// `len()` separate slots to the k-way merge heap — collapses
    /// O(num_sstables) initial peeks into O(num_runs). `peek_key`
    /// reads the current SSTable's header; `pop_partition` decodes
    /// the body.
    SsTableRun {
        run: SsTableRunIter<'a, R>,
        peeked_key: Option<DecoratedKey>,
    },
}

impl<'a, R: ReadAt> MergeSource<'a, R> {
    /// Look at the next partition's key WITHOUT decoding the body.
    /// Repeated calls are idempotent; the cached peek is consumed by
    /// the next `pop_partition()`. Returns `Ok(None)` at EOS.
    fn peek_key(&mut self) -> Result<Option<DecoratedKey>> {
        match self {
            Self::Memtable { iter, peeked } => {
                if peeked.is_none() {
                    *peeked = iter.next();
                }
                Ok(peeked.as_ref().map(|p| p.key.clone()))
            }
            Self::SsTable {
                iter, peeked_key, ..
            } => {
                if peeked_key.is_none() {
                    *peeked_key = iter.peek_partition_key()?;
                }
                Ok(peeked_key.clone())
            }
            Self::SsTableRun { run, peeked_key } => {
                if peeked_key.is_none() {
                    *peeked_key = run.peek_key()?;
                }
                Ok(peeked_key.clone())
            }
        }
    }

    /// Decode and consume the next partition (the one most recently
    /// returned by `peek_key()` — must follow a peek that returned
    /// `Ok(Some(_))`). Clears the peek cache. Returns `Ok(None)` at
    /// EOS only if the source was exhausted between peek and pop
    /// (race-free here — we don't share sources across threads).
    fn pop_partition(&mut self) -> Result<Option<Partition>> {
        match self {
            Self::Memtable { iter, peeked } => {
                if let Some(p) = peeked.take() {
                    Ok(Some(p))
                } else {
                    Ok(iter.next())
                }
            }
            Self::SsTable {
                iter,
                mode,
                peeked_key,
            } => {
                peeked_key.take();
                match mode {
                    RunMode::Full => iter.next_partition(),
                    RunMode::Metadata => iter.next_partition_metadata(),
                    RunMode::Projected(wanted) => iter.next_partition_projected(wanted),
                }
            }
            Self::SsTableRun { run, peeked_key } => {
                peeked_key.take();
                run.next_partition()
            }
        }
    }

    /// Advance past the currently-peeked partition WITHOUT decoding
    /// its body — used by the merger's duplicate-key dedup loop.
    /// For SSTable sources, this uses the lazy partition-offset
    /// cache so the cost is a single binary search + pos
    /// assignment. For memtable sources, we just discard the cached
    /// peek (the memtable iterator already yielded the partition
    /// during peek, so advancing is "drop the cached peek").
    ///
    /// `allow(dead_code)`: wired but not yet applied in the dedup
    /// loop — see the matching note on
    /// `SsTableRunIter::skip_to_next_partition`.
    #[allow(dead_code)]
    fn skip_peeked_partition(&mut self) -> Result<()> {
        match self {
            Self::Memtable { peeked, .. } => {
                peeked.take();
                Ok(())
            }
            Self::SsTable {
                iter, peeked_key, ..
            } => {
                peeked_key.take();
                iter.skip_to_next_partition()
            }
            Self::SsTableRun { run, peeked_key } => {
                peeked_key.take();
                run.skip_to_next_partition()
            }
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

    /// Skip past the partition currently at the front of this run
    /// WITHOUT decoding it. If the current SSTable is exhausted,
    /// advance to the next SSTable in the run. Used by the merger's
    /// duplicate-key dedup loop via
    /// `MergeSource::skip_peeked_partition`.
    ///
    /// `allow(dead_code)`: the primitive is wired but the dedup
    /// loop in `next_merged_partition` does not yet apply it
    /// conditionally — that requires surfacing the table's
    /// clustering shape so we only skip when the body decode is
    /// safe to drop (single-row partitions). See
    /// `bug-streaming-range-read-perf-50x-floor.md`.
    #[allow(dead_code)]
    fn skip_to_next_partition(&mut self) -> Result<()> {
        loop {
            if self.current.is_none() {
                if self.cursor >= self.sstables.len() {
                    return Ok(());
                }
                match self.sstables[self.cursor].partitions_iter() {
                    Ok(it) => self.current = Some(it),
                    Err(e) => return Err(e),
                }
            }
            let it = self.current.as_mut().expect("just initialised");
            it.skip_to_next_partition()?;
            // If the iterator is now at EOF, advance to the next
            // SSTable so the next peek_key sees the run's next
            // partition.
            match it.peek_partition_key()? {
                Some(_) => return Ok(()),
                None => {
                    self.current = None;
                    self.cursor += 1;
                }
            }
        }
    }

    /// Peek the next partition's key, advancing through exhausted
    /// SSTables in the run if necessary. Does NOT decode any
    /// partition body. Used by `MergeSource::peek_key` for cheap
    /// heap priming.
    fn peek_key(&mut self) -> Result<Option<DecoratedKey>> {
        loop {
            if self.current.is_none() {
                if self.cursor >= self.sstables.len() {
                    return Ok(None);
                }
                match self.sstables[self.cursor].partitions_iter() {
                    Ok(it) => self.current = Some(it),
                    Err(e) => return Err(e),
                }
            }
            let it = self.current.as_mut().expect("just initialised");
            match it.peek_partition_key() {
                Ok(Some(k)) => return Ok(Some(k)),
                Ok(None) => {
                    self.current = None;
                    self.cursor += 1;
                }
                Err(e) => return Err(e),
            }
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
                    Err(e) => return Err(e),
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
                Err(e) => return Err(e),
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
    build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Full,
        start,
        end,
    )
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
    build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Projected(wanted),
        start,
        end,
    )
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
    build_merger_with_runs(
        active_iter,
        flushing_iter,
        sstables,
        RunMode::Metadata,
        start,
        end,
    )
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
) -> Result<RangeMerger<'a, R>> {
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
    sources.push(MergeSource::Memtable {
        iter: active_iter,
        peeked: None,
    });
    if let Some(it) = flushing_iter {
        sources.push(MergeSource::Memtable {
            iter: it,
            peeked: None,
        });
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
        sources.push(MergeSource::SsTableRun {
            run: SsTableRunIter::new(run.as_slice(), mode),
            peeked_key: None,
        });
    }
    let mut merger = RangeMerger::new(sources, start, end)?;
    merger.runs_arena = Some(runs_ptr);
    Ok(merger)
}

/// Heap entry: one peeked KEY per still-active source. The heap is
/// min-keyed; ties (same DecoratedKey across sources) are broken by
/// source index for total ordering — the merger groups all same-key
/// partitions into a single merge call.
///
/// Importantly the entry does NOT hold the partition body. The body
/// is decoded lazily when the entry's source is popped, via
/// `MergeSource::pop_partition`. On cold cache this defers the
/// dominant cost (per-partition body decode) from `O(num_sources)`
/// at construction time to `O(emitted)` over the scan lifetime.
struct HeapEntry {
    key: DecoratedKey,
    src: usize,
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
    ) -> Result<Self> {
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
            merger.refill_source(src)?;
        }
        Ok(merger)
    }

    /// Peek the next key from `src`, skipping entries below `start`,
    /// and push it onto the heap. The partition BODY is not decoded
    /// here — only the cheap header read is performed. If `peek_key`
    /// returns a key below `start`, we pop it (consuming the body to
    /// advance the source) and try again.
    fn refill_source(&mut self, src: usize) -> Result<()> {
        loop {
            let peeked = self.sources[src].peek_key()?;
            let key = match peeked {
                Some(k) => k,
                None => return Ok(()),
            };
            if let Some(ref s) = self.start {
                if key < *s {
                    match self.sources[src].pop_partition()? {
                        Some(_) => {}
                        None => {
                            return Err(Error::InvalidFormat(
                                "range_merger: source ended after a successful peek while skipping start-bound partition".into(),
                            ));
                        }
                    }
                    continue;
                }
            }
            self.heap.push(HeapEntry { key, src });
            return Ok(());
        }
    }

    /// Pull one merged partition. Returns `Ok(None)` when every
    /// source is exhausted (or the upper bound was crossed). Pops
    /// the smallest-key heap entry, asks that source for the full
    /// partition (now warm — header was just paged in by peek), and
    /// repeats for every other source whose head matches the same
    /// key. Merges + applies deletion suppression before yielding.
    pub fn next_merged_partition(&mut self) -> Result<Option<Partition>> {
        if self.exhausted {
            return Ok(None);
        }
        let first = match self.heap.pop() {
            Some(e) => e,
            None => return Ok(None),
        };
        if let Some(ref e) = self.end {
            if first.key > *e {
                self.exhausted = true;
                return Ok(None);
            }
        }
        let key = first.key.clone();
        let first_src = first.src;
        let first_partition = match self.sources[first_src].pop_partition() {
            Ok(Some(p)) => p,
            Ok(None) => {
                self.refill_source(first_src)?;
                return self.next_merged_partition();
            }
            Err(e) => return Err(e),
        };
        let mut group: Vec<Partition> = vec![first_partition];
        let mut popped_srcs: Vec<usize> = vec![first_src];

        while let Some(top) = self.heap.peek() {
            if top.key != key {
                break;
            }
            let entry = self.heap.pop().expect("peek succeeded");
            match self.sources[entry.src].pop_partition()? {
                Some(p) => {
                    popped_srcs.push(entry.src);
                    group.push(p);
                }
                None => {
                    popped_srcs.push(entry.src);
                }
            }
        }

        for src in popped_srcs {
            self.refill_source(src)?;
        }

        let mut merged = if group.len() == 1 {
            group.into_iter().next().unwrap()
        } else {
            merge::merge_partitions(group)
        };
        merge::apply_deletions(&mut merged);
        Ok(Some(merged))
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{group_disjoint_runs, merger_for_sources};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
    use ferrosa_sstable::statistics::SerializationHeader;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

    fn b(s: &[u8]) -> Vec<u8> {
        s.to_vec()
    }

    fn test_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    fn test_partition(key: &[u8], clustering: i32, value: &[u8]) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::from(key)),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: clustering.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(value.to_vec(), 1_000_042))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_042),
            }],
        }
    }

    fn reader_from_partitions(partitions: &[Partition]) -> SSTableReader<Vec<u8>> {
        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                verify_output: false,
                ..WriteOptions::default()
            },
            test_header(),
        );
        for partition in partitions {
            writer.add_partition(partition).unwrap();
        }
        let output = writer.finish().unwrap();
        SSTableReader::open(SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
        .unwrap()
    }

    fn reader_with_truncated_tail(partitions: &[Partition]) -> SSTableReader<Vec<u8>> {
        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                verify_output: false,
                ..WriteOptions::default()
            },
            test_header(),
        );
        for partition in partitions {
            writer.add_partition(partition).unwrap();
        }
        let mut output = writer.finish().unwrap();
        output.data.truncate(output.data.len() - 1);
        SSTableReader::open(SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
        .unwrap()
    }

    /// Token-disjoint SSTables in input order collapse into a
    /// single run — the dream LSM-leveled state.
    #[test]
    fn disjoint_in_order_yields_one_run() {
        let bounds = vec![(b(b"a"), b(b"c")), (b(b"d"), b(b"f")), (b(b"g"), b(b"i"))];
        let runs = group_disjoint_runs(&bounds);
        assert_eq!(runs.len(), 1, "all disjoint → exactly one run");
        assert_eq!(runs[0], vec![0, 1, 2]);
    }

    /// Disjoint SSTables given OUT OF ORDER still collapse into one
    /// run, with the run sorted by smallest key. Proves the sort
    /// step works.
    #[test]
    fn disjoint_out_of_order_sorts_into_one_run() {
        let bounds = vec![(b(b"g"), b(b"i")), (b(b"a"), b(b"c")), (b(b"d"), b(b"f"))];
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
        let bounds = vec![(b(b"a"), b(b"z")), (b(b"b"), b(b"y")), (b(b"c"), b(b"x"))];
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

    #[test]
    fn range_merger_propagates_truncated_sstable_tail_error() {
        let first = test_partition(b"pk1", 1, b"first");
        let second = test_partition(b"pk2", 1, b"second");
        let good_reader = reader_from_partitions(std::slice::from_ref(&first));
        let corrupt_reader = reader_with_truncated_tail(&[first, second]);
        let sstables = vec![Arc::new(good_reader), Arc::new(corrupt_reader)];
        let mut merger =
            merger_for_sources(Box::new(std::iter::empty()), None, &sstables, None, None).unwrap();

        assert!(
            merger.next_merged_partition().unwrap().is_some(),
            "first partition should still be readable"
        );
        let err = merger
            .next_merged_partition()
            .expect_err("corrupt SSTable tail must fail the range scan");
        assert!(
            err.to_string().contains("read_exact_at")
                || err.to_string().contains("unexpected EOF")
                || err.to_string().contains("UnexpectedEof"),
            "error should identify the SSTable read failure, got: {err}"
        );
    }
}
