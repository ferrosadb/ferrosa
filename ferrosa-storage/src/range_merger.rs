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
    /// SSTable streaming reader.
    SsTable(PartitionIter<'a, R>),
}

impl<'a, R: ReadAt> MergeSource<'a, R> {
    fn next_partition(&mut self) -> Result<Option<Partition>> {
        match self {
            Self::Memtable(it) => Ok(it.next()),
            Self::SsTable(it) => it.next_partition(),
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
pub fn merger_for_sources<'a, R: ReadAt>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    let mut sources: Vec<MergeSource<'a, R>> = Vec::with_capacity(2 + sstables.len());
    sources.push(MergeSource::Memtable(active_iter));
    if let Some(it) = flushing_iter {
        sources.push(MergeSource::Memtable(it));
    }
    for sst in sstables {
        // partitions_iter borrows from the SSTableReader; the caller
        // keeps the Arcs alive for the merger's lifetime.
        match sst.partitions_iter() {
            Ok(it) => sources.push(MergeSource::SsTable(it)),
            Err(e) => {
                tracing::warn!("range_merger: SSTable open failed, skipping: {e}");
            }
        }
    }
    Ok(RangeMerger::new(sources, start, end))
}
