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
use ferrosa_common::schema::TableSchema;
use ferrosa_common::{Error, Result};
use ferrosa_sstable::reader::{PartitionIter, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_sstable::ReadAt;

use crate::merge;

/// Default number of clustered rows carried in one [`PartitionFragment`].
/// Bounds the merger's resident working set to `O(num_sources + K)`
/// rows regardless of how wide a single partition is — an inverted-index
/// partition (one hot term ⇒ millions of rows in one `Vec<Row>`) is the
/// shape that OOM-killed replicas on a full-table `SELECT *`.
///
/// Tunable via `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT`. Picked at a few
/// thousand so the per-fragment frame amortises message overhead while
/// keeping resident memory comfortably under any sane cgroup cap.
pub const DEFAULT_ROWS_PER_FRAGMENT: usize = 4_096;

/// Resolve the fragment row cap `K` from the environment, falling back to
/// [`DEFAULT_ROWS_PER_FRAGMENT`]. A value of `0` or an unparseable value is
/// treated as the default (never zero — a zero cap would loop forever).
pub fn rows_per_fragment() -> usize {
    match std::env::var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT") {
        Ok(v) => match v.trim().parse::<usize>() {
            Ok(n) if n >= 1 => n,
            _ => DEFAULT_ROWS_PER_FRAGMENT,
        },
        Err(_) => DEFAULT_ROWS_PER_FRAGMENT,
    }
}

/// One bounded slice of a merged partition emitted by [`FragmentMerger`].
///
/// A wide partition is delivered as a SEQUENCE of fragments that all share
/// the same `key`, in clustering order, with non-overlapping row ranges.
/// Reassembly contract (relied on by the cluster stream handler, the
/// coordinator merge, and the CQL row bridge):
///
/// - The FIRST fragment of a key (`first == true`) carries the partition's
///   real `deletion` and `static_row`. Every later fragment carries
///   `deletion = LIVE` and `static_row = None`, so a consumer that flattens
///   fragments into rows never double-counts the static row nor re-applies
///   the partition tombstone.
/// - `rows` are already cell-merged across sources (LWW), deletion-suppressed,
///   and sorted by clustering key, with `rows.len() <= K`.
/// - `last == true` marks the final fragment for the key.
///
/// A consumer can therefore treat each fragment exactly like a small
/// `Partition` and append its rows; the concatenation is byte-identical to
/// the single whole-partition merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFragment {
    pub key: DecoratedKey,
    pub deletion: ferrosa_sstable::types::DeletionTime,
    pub static_row: Option<Row>,
    pub rows: Vec<Row>,
    pub first: bool,
    pub last: bool,
}

impl PartitionFragment {
    /// View this fragment as a standalone `Partition`. Because non-first
    /// fragments carry `deletion = LIVE` / `static_row = None`, flattening
    /// the fragment sequence of one key reproduces the merged partition.
    pub fn into_partition(self) -> Partition {
        Partition {
            key: self.key,
            deletion: self.deletion,
            static_row: self.static_row,
            rows: self.rows,
        }
    }
}

/// Per-SSTable translation from the physical column ordinals in an
/// SSTable's SerializationHeader to the current table schema ordinals.
#[derive(Clone, Debug)]
pub struct ColumnOrdinalMapping {
    static_columns: Vec<Option<u16>>,
    regular_columns: Vec<Option<u16>>,
    identity: bool,
}

impl ColumnOrdinalMapping {
    pub fn for_header(schema: &TableSchema, header: &SerializationHeader) -> Self {
        fn build_map(
            source: &[(Vec<u8>, String)],
            target: &[ferrosa_common::schema::ColumnDefinition],
        ) -> Vec<Option<u16>> {
            source
                .iter()
                .map(|(source_name, source_type)| {
                    target
                        .iter()
                        .position(|target_col| {
                            target_col.name.as_bytes() == source_name.as_slice()
                                && target_col.type_name == *source_type
                        })
                        .map(|idx| idx as u16)
                })
                .collect()
        }

        let static_columns = build_map(&header.static_columns, &schema.static_columns);
        let regular_columns = build_map(&header.regular_columns, &schema.regular_columns);
        let identity = is_identity(&static_columns) && is_identity(&regular_columns);

        Self {
            static_columns,
            regular_columns,
            identity,
        }
    }

    pub fn is_identity(&self) -> bool {
        self.identity
    }

    pub fn remap_partition(&self, partition: &mut Partition) {
        if self.identity {
            return;
        }
        if let Some(static_row) = partition.static_row.as_mut() {
            self.remap_static_row(static_row);
        }
        for row in &mut partition.rows {
            self.remap_regular_row(row);
        }
    }

    pub fn remap_static_row(&self, row: &mut Row) {
        if self.identity {
            return;
        }
        remap_cells(&mut row.cells, &self.static_columns);
    }

    pub fn remap_regular_row(&self, row: &mut Row) {
        if self.identity {
            return;
        }
        remap_cells(&mut row.cells, &self.regular_columns);
    }

    pub fn source_regular_ordinals_for_projection(&self, wanted_current: &[u16]) -> Vec<u16> {
        if self.identity {
            return wanted_current.to_vec();
        }
        self.regular_columns
            .iter()
            .enumerate()
            .filter_map(|(source_idx, target_idx)| {
                let target_idx = target_idx.as_ref()?;
                wanted_current
                    .contains(target_idx)
                    .then_some(source_idx as u16)
            })
            .collect()
    }
}

fn is_identity(mapping: &[Option<u16>]) -> bool {
    mapping
        .iter()
        .enumerate()
        .all(|(idx, target)| *target == Some(idx as u16))
}

fn remap_cells(cells: &mut Vec<(u16, ferrosa_common::cell::CellValue)>, mapping: &[Option<u16>]) {
    if cells.is_empty() {
        return;
    }

    let mut remapped = Vec::with_capacity(cells.len());
    for (source_idx, cell) in cells.drain(..) {
        if let Some(Some(target_idx)) = mapping.get(source_idx as usize) {
            remapped.push((*target_idx, cell));
        }
    }
    remapped.sort_by_key(|(idx, _)| *idx);
    *cells = remapped;
}

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
/// Header (and, for in-memory sources, the body) of a partition popped
/// for fragment streaming. See [`MergeSource::pop_partition_header`].
struct PoppedHeader {
    key: DecoratedKey,
    deletion: ferrosa_sstable::types::DeletionTime,
    static_row: Option<Row>,
    /// `Some` for `Memtable` sources: the partition's rows, already sorted
    /// by clustering key, to be drained by the `FragmentMerger`. `None` for
    /// SSTable-backed sources, whose rows stream via
    /// [`MergeSource::next_fragment_row`].
    mem_body: Option<Vec<Row>>,
}

pub enum MergeSource<'a, R: ReadAt> {
    /// Memtable / flushing memtable: infallible per-partition yield.
    /// `peeked` caches the result of the most recent `peek_key()`
    /// (the full partition, since memtables yield owned partitions);
    /// `pop_partition` takes it.
    Memtable {
        iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
        peeked: Option<Partition>,
    },
    // NOTE: fragment-row streaming state for the Memtable variant lives
    // in `FragmentMerger` (see `MemRowSource`) rather than here, so the
    // whole-partition `RangeMerger` path is untouched.
    /// Single SSTable streaming reader. `mode` controls per-cell
    /// decoding on `pop_partition`. `peek_key` reads only the
    /// partition header (~10 bytes) via `PartitionIter::peek_partition_key`
    /// and caches the key in `peeked_key`; `pop_partition` decodes
    /// the body from the same offset and clears the cache.
    SsTable {
        iter: PartitionIter<'a, R>,
        mode: RunMode<'a>,
        mapping: Option<&'a ColumnOrdinalMapping>,
        peeked_key: Option<DecoratedKey>,
        failed: bool,
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
                iter,
                mode,
                peeked_key,
                failed,
                ..
            } => {
                if *failed {
                    return Ok(None);
                }
                if peeked_key.is_none() {
                    match iter.peek_partition_key() {
                        Ok(key) => *peeked_key = key,
                        Err(e) if mode.is_fail_soft() => {
                            tracing::warn!(
                                %e,
                                mode = mode.label(),
                                "range scan: skipping SSTable whose next key failed to decode"
                            );
                            *failed = true;
                            return Ok(None);
                        }
                        Err(e) => return Err(e),
                    }
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
                mapping,
                peeked_key,
                failed,
            } => {
                if *failed {
                    return Ok(None);
                }
                peeked_key.take();
                let result = match mode {
                    RunMode::Full => iter.next_partition(),
                    RunMode::Metadata => iter.next_partition_metadata(),
                    RunMode::Projected(wanted) => {
                        let source_wanted = mapping
                            .map(|m| m.source_regular_ordinals_for_projection(wanted))
                            .unwrap_or_else(|| wanted.to_vec());
                        iter.next_partition_projected(&source_wanted)
                    }
                };
                let mut partition = match result {
                    Ok(partition) => partition,
                    Err(e) if mode.is_fail_soft() => {
                        tracing::warn!(
                            %e,
                            mode = mode.label(),
                            "range scan: skipping SSTable whose partition failed to decode"
                        );
                        *failed = true;
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                };
                if let (Some(mapping), Some(partition)) = (mapping, partition.as_mut()) {
                    mapping.remap_partition(partition);
                }
                Ok(partition)
            }
            Self::SsTableRun { run, peeked_key } => {
                peeked_key.take();
                run.next_partition()
            }
        }
    }

    /// Fragment-streaming counterpart of [`Self::pop_partition`].
    /// Pops the next partition's HEADER (key + partition deletion +
    /// optional static row) for the most-recently-peeked key,
    /// **parking the source at the first clustered row** so the body
    /// can be streamed via [`Self::next_fragment_row`] without ever
    /// materialising the whole partition.
    ///
    /// For `Memtable` sources the partition body is already in memory,
    /// so the header carries the whole partition out as
    /// `MemPartitionBody` (the `FragmentMerger` drains its rows in
    /// clustering order). For `SsTable` / `SsTableRun` sources only
    /// the ~10-byte header is decoded here; rows arrive lazily.
    ///
    /// Returns `Ok(None)` if the source was exhausted between peek and
    /// pop (race-free here — sources are not shared across threads).
    fn pop_partition_header(&mut self) -> Result<Option<PoppedHeader>> {
        match self {
            Self::Memtable { iter, peeked } => {
                let partition = match peeked.take() {
                    Some(p) => p,
                    None => match iter.next() {
                        Some(p) => p,
                        None => return Ok(None),
                    },
                };
                let key = partition.key.clone();
                let deletion = partition.deletion;
                let static_row = partition.static_row.clone();
                let mut rows = partition.rows;
                rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
                Ok(Some(PoppedHeader {
                    key,
                    deletion,
                    static_row,
                    mem_body: Some(rows),
                }))
            }
            Self::SsTable {
                iter,
                mode,
                mapping,
                peeked_key,
                failed,
            } => {
                if *failed {
                    return Ok(None);
                }
                peeked_key.take();
                let header = match iter.next_partition_header_only() {
                    Ok(Some(h)) => h,
                    Ok(None) => return Ok(None),
                    Err(e) if mode.is_fail_soft() => {
                        tracing::warn!(
                            %e,
                            mode = mode.label(),
                            "range scan: skipping SSTable whose partition header failed to decode"
                        );
                        *failed = true;
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                };
                let (key, deletion, mut static_row) = header;
                if let (Some(mapping), Some(sr)) = (*mapping, static_row.as_mut()) {
                    mapping.remap_static_row(sr);
                }
                Ok(Some(PoppedHeader {
                    key,
                    deletion,
                    static_row,
                    mem_body: None,
                }))
            }
            Self::SsTableRun { run, peeked_key } => {
                peeked_key.take();
                match run.next_partition_header_only()? {
                    Some((key, deletion, static_row)) => Ok(Some(PoppedHeader {
                        key,
                        deletion,
                        static_row,
                        mem_body: None,
                    })),
                    None => Ok(None),
                }
            }
        }
    }

    /// Pull one clustered row from the partition opened by the most
    /// recent [`Self::pop_partition_header`] on this source. Returns
    /// `Ok(None)` at end-of-partition. Only valid for SSTable-backed
    /// sources; `Memtable` rows are drained from the `MemPartitionBody`
    /// the header carried out, so this is `Ok(None)` for them.
    fn next_fragment_row(&mut self) -> Result<Option<Row>> {
        match self {
            Self::Memtable { .. } => Ok(None),
            Self::SsTable {
                iter,
                mode,
                mapping,
                failed,
                ..
            } => {
                if *failed {
                    return Ok(None);
                }
                let mode = *mode;
                let mapping = *mapping;
                let row = match mode {
                    RunMode::Projected(wanted) => {
                        let source_wanted = mapping
                            .map(|m| m.source_regular_ordinals_for_projection(wanted))
                            .unwrap_or_else(|| wanted.to_vec());
                        iter.next_clustered_row_projected(&source_wanted)
                    }
                    RunMode::Full | RunMode::Metadata => iter.next_clustered_row(),
                };
                let mut row = match row {
                    Ok(r) => r,
                    Err(e) if mode.is_fail_soft() => {
                        tracing::warn!(
                            %e,
                            mode = mode.label(),
                            "range scan: skipping SSTable whose row failed to decode"
                        );
                        *failed = true;
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                };
                if let (Some(mapping), Some(r)) = (mapping, row.as_mut()) {
                    mapping.remap_regular_row(r);
                }
                Ok(row)
            }
            Self::SsTableRun { run, .. } => run.next_fragment_row(),
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
                iter,
                peeked_key,
                failed,
                ..
            } => {
                if *failed {
                    return Ok(());
                }
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

impl<'a> RunMode<'a> {
    fn is_fail_soft(self) -> bool {
        false
    }

    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Metadata => "metadata",
            Self::Projected(_) => "projected",
        }
    }
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
                    Err(e) if self.mode.is_fail_soft() => {
                        tracing::warn!(
                            %e,
                            sstable_index = self.cursor,
                            mode = self.mode.label(),
                            "range scan: skipping SSTable whose iterator failed to open"
                        );
                        self.cursor += 1;
                        continue;
                    }
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
                Err(e) if self.mode.is_fail_soft() => {
                    tracing::warn!(
                        %e,
                        sstable_index = self.cursor,
                        mode = self.mode.label(),
                        "range scan: skipping SSTable whose next key failed to decode"
                    );
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
                    Err(e) if self.mode.is_fail_soft() => {
                        tracing::warn!(
                            %e,
                            sstable_index = self.cursor,
                            mode = self.mode.label(),
                            "range scan: skipping SSTable whose iterator failed to open"
                        );
                        self.cursor += 1;
                        return Ok(None);
                    }
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
                Err(e) if self.mode.is_fail_soft() => {
                    tracing::warn!(
                        %e,
                        sstable_index = self.cursor,
                        mode = self.mode.label(),
                        "range scan: skipping SSTable whose partition failed to decode"
                    );
                    self.current = None;
                    self.cursor += 1;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fragment-row streaming: read the current partition's HEADER
    /// (key + deletion + optional static row) from the run's current
    /// SSTable, parking that SSTable's `PartitionIter` at the first
    /// clustered row. The follow-up is [`Self::next_fragment_row`].
    /// Because runs are token-disjoint, every key appears in exactly
    /// one SSTable of the run, so the header always comes from the
    /// `current` iterator. Returns `Ok(None)` at end-of-run.
    fn next_partition_header_only(
        &mut self,
    ) -> Result<
        Option<(
            DecoratedKey,
            ferrosa_sstable::types::DeletionTime,
            Option<Row>,
        )>,
    > {
        loop {
            if self.current.is_none() {
                if self.cursor >= self.sstables.len() {
                    return Ok(None);
                }
                self.current = Some(self.sstables[self.cursor].partitions_iter()?);
            }
            let it = self.current.as_mut().expect("just initialised");
            match it.next_partition_header_only()? {
                Some(header) => return Ok(Some(header)),
                None => {
                    self.current = None;
                    self.cursor += 1;
                }
            }
        }
    }

    /// Pull the next clustered row of the partition most recently
    /// opened by [`Self::next_partition_header_only`], honoring the
    /// run's projection `mode`. Returns `Ok(None)` at
    /// end-of-partition. Does NOT advance to the next SSTable — that
    /// happens on the following `next_partition_header_only`.
    fn next_fragment_row(&mut self) -> Result<Option<Row>> {
        let Some(it) = self.current.as_mut() else {
            return Ok(None);
        };
        match self.mode {
            RunMode::Full | RunMode::Metadata => it.next_clustered_row(),
            RunMode::Projected(wanted) => it.next_clustered_row_projected(wanted),
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

/// Compatibility variant for tables whose current schema order may differ
/// from one or more SSTable SerializationHeaders.
pub fn merger_for_sources_with_mappings<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    mappings: &'a [ColumnOrdinalMapping],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    if mappings.iter().all(ColumnOrdinalMapping::is_identity) {
        return merger_for_sources(active_iter, flushing_iter, sstables, start, end);
    }
    build_merger_without_runs(
        active_iter,
        flushing_iter,
        sstables,
        Some(mappings),
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

/// Compatibility projection variant for mixed current/SSTable column order.
pub fn merger_for_projected_sources_with_mappings<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    mappings: &'a [ColumnOrdinalMapping],
    wanted: &'a [u16],
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    if mappings.iter().all(ColumnOrdinalMapping::is_identity) {
        return merger_for_projected_sources(
            active_iter,
            flushing_iter,
            sstables,
            wanted,
            start,
            end,
        );
    }
    build_merger_without_runs(
        active_iter,
        flushing_iter,
        sstables,
        Some(mappings),
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

fn build_merger_without_runs<'a, R: ReadAt + Send + Sync + 'static>(
    active_iter: Box<dyn Iterator<Item = Partition> + Send + 'a>,
    flushing_iter: Option<Box<dyn Iterator<Item = Partition> + Send + 'a>>,
    sstables: &'a [Arc<SSTableReader<R>>],
    mappings: Option<&'a [ColumnOrdinalMapping]>,
    mode: RunMode<'a>,
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
) -> Result<RangeMerger<'a, R>> {
    let mut sources: Vec<MergeSource<'a, R>> = Vec::with_capacity(2 + sstables.len());
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

    for (idx, sstable) in sstables.iter().enumerate() {
        let iter = match sstable.partitions_iter() {
            Ok(iter) => iter,
            Err(e) if mode.is_fail_soft() => {
                tracing::warn!(
                    %e,
                    sstable_index = idx,
                    mode = mode.label(),
                    "range scan: skipping SSTable whose iterator failed to open"
                );
                continue;
            }
            Err(e) => return Err(e),
        };
        let mapping = mappings.and_then(|m| m.get(idx));
        sources.push(MergeSource::SsTable {
            iter,
            mode,
            mapping,
            peeked_key: None,
            failed: false,
        });
    }

    RangeMerger::new(sources, start, end)
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
    /// In-progress partition for the fragment-streaming path
    /// ([`Self::next_fragment`]). `None` between partitions (the
    /// whole-partition `next_merged_partition` path never sets it).
    active: Option<ActivePartition>,
    /// Leaked allocation backing the per-run `Vec<Arc<SSTableReader>>`
    /// slices that `SsTableRunIter`s borrow from. `Some` only when
    /// the merger was built via `build_merger_with_runs`; freed by
    /// the `Drop` impl below. `*const` rather than `Box` because
    /// `Box::leak`'s output has to be re-wrapped manually.
    runs_arena: Option<*const [Vec<Arc<SSTableReader<R>>>]>,
}

/// One source's current row head while a partition is being streamed
/// in fragments.
enum RowCursor {
    /// In-memory rows (memtable source), pre-sorted by clustering key.
    Mem(std::vec::IntoIter<Row>),
    /// SSTable-backed source — rows pulled lazily via
    /// `MergeSource::next_fragment_row`, identified by source index.
    Sst { src: usize },
}

/// Streaming state for the partition currently being emitted as
/// [`PartitionFragment`]s. Holds at most one row per participating
/// source ("head") plus the merged partition header.
struct ActivePartition {
    key: DecoratedKey,
    deletion: ferrosa_sstable::types::DeletionTime,
    /// Carried out on the FIRST fragment, then `None`.
    pending_static_row: Option<Row>,
    first: bool,
    cursors: Vec<RowCursor>,
    /// `heads[i]` is the next unconsumed row from `cursors[i]`, or
    /// `None` if that cursor is exhausted. Peak resident rows for the
    /// merge proper is `cursors.len()` (one head each).
    heads: Vec<Option<Row>>,
    /// Source indices to refill back onto the heap once this partition
    /// is fully drained.
    refill_srcs: Vec<usize>,
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
            active: None,
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

    /// Pull the next [`PartitionFragment`]. Drives the **intra-partition
    /// streaming k-way row merge**: a single (possibly multi-million-row)
    /// partition is delivered as a sequence of fragments each holding
    /// `<= k` clustered rows, so resident memory is `O(num_sources + k)`
    /// rows regardless of partition width. Returns `Ok(None)` when every
    /// source is exhausted.
    ///
    /// Correctness — byte-identical to `next_merged_partition` flattened:
    ///
    /// 1. **Key selection / dedup** is the SAME heap discipline as
    ///    `next_merged_partition`: pop the smallest key, then pop every
    ///    other source whose head matches it.
    /// 2. **Header merge** mirrors `merge::merge_partitions`'s header step:
    ///    partition deletion is the max `marked_for_delete_at` across
    ///    sources; the static row is cell-merged via `merge::merge_rows`,
    ///    then partition-deletion-suppressed once (carried on the first
    ///    fragment only).
    /// 3. **Row merge** is a k-way merge by clustering key across all
    ///    sources' heads; rows sharing a clustering key are folded with
    ///    `merge::merge_rows` (cell-level LWW) — identical to
    ///    `merge_partitions`'s `pop/merge/push` over the clustering-sorted
    ///    concatenation.
    /// 4. **Deletion suppression** is applied per row exactly as
    ///    `merge::apply_deletions`: a row whose `primary_key_liveness`
    ///    predates the partition tombstone is dropped; surviving rows have
    ///    cells older than their own row tombstone dropped. Because both
    ///    rules are row-local they fragment without changing the result.
    pub fn next_fragment(&mut self, k: usize) -> Result<Option<PartitionFragment>> {
        debug_assert!(k >= 1, "next_fragment k must be >= 1");
        loop {
            if self.active.is_none() && !self.begin_active_partition()? {
                return Ok(None);
            }
            // Build one fragment (<= k rows) from the active partition.
            match self.emit_fragment(k)? {
                Some(fragment) => return Ok(Some(fragment)),
                // Active partition produced no fragment (all its rows were
                // deletion-suppressed and it was not the first fragment):
                // drop it and advance to the next key.
                None => continue,
            }
        }
    }

    /// Select the next key via the heap (respecting `end`), pop every
    /// source holding it, merge the header, and install the per-source
    /// row cursors into `self.active`. Returns `Ok(false)` at end-of-scan.
    fn begin_active_partition(&mut self) -> Result<bool> {
        if self.exhausted {
            return Ok(false);
        }
        let first = match self.heap.pop() {
            Some(e) => e,
            None => return Ok(false),
        };
        if let Some(ref e) = self.end {
            if first.key > *e {
                self.exhausted = true;
                return Ok(false);
            }
        }
        let key = first.key.clone();

        // Collect every source holding this key (heap-popped).
        let mut srcs: Vec<usize> = vec![first.src];
        while let Some(top) = self.heap.peek() {
            if top.key != key {
                break;
            }
            let entry = self.heap.pop().expect("peek succeeded");
            srcs.push(entry.src);
        }

        // Pop each source's header (and, for memtables, its sorted body).
        // Merge header: max-timestamp partition deletion, cell-merged
        // static row.
        let mut merged_deletion = ferrosa_sstable::types::DeletionTime::LIVE;
        let mut merged_static: Option<Row> = None;
        let mut cursors: Vec<RowCursor> = Vec::with_capacity(srcs.len());
        let mut refill_srcs: Vec<usize> = Vec::with_capacity(srcs.len());

        for &src in &srcs {
            let header = match self.sources[src].pop_partition_header()? {
                Some(h) => h,
                // Source ended between peek and pop — refill (it is now
                // exhausted) and skip. Mirrors next_merged_partition's
                // Ok(None) branch.
                None => {
                    refill_srcs.push(src);
                    continue;
                }
            };
            debug_assert_eq!(
                header.key, key,
                "popped header key must match the heap-selected key"
            );
            if header.deletion.marked_for_delete_at > merged_deletion.marked_for_delete_at {
                merged_deletion = header.deletion;
            }
            if let Some(sr) = header.static_row {
                merged_static = match merged_static.take() {
                    Some(prev) => Some(merge::merge_rows(prev, sr)),
                    None => Some(sr),
                };
            }
            match header.mem_body {
                Some(rows) => cursors.push(RowCursor::Mem(rows.into_iter())),
                None => cursors.push(RowCursor::Sst { src }),
            }
            refill_srcs.push(src);
        }

        // Partition-deletion suppression of the static row (mirrors
        // `apply_deletions`: drop cells older than the partition tombstone,
        // and drop the static row entirely if it has no surviving cells).
        if !merged_deletion.is_live() {
            if let Some(sr) = merged_static.as_mut() {
                let cut = merged_deletion.marked_for_delete_at;
                sr.cells.retain(|(_col, cell)| cell.timestamp >= cut);
                if sr.cells.is_empty() {
                    merged_static = None;
                }
            }
        }

        let cursor_count = cursors.len();
        self.active = Some(ActivePartition {
            key,
            deletion: merged_deletion,
            pending_static_row: merged_static,
            first: true,
            cursors,
            heads: vec![None; cursor_count],
            refill_srcs,
        });
        // Prime each cursor's head.
        self.prime_active_heads()?;
        Ok(true)
    }

    /// Load the first row of any cursor whose head is `None` and is not yet
    /// exhausted. Called after `begin_active_partition` and after each
    /// head is consumed.
    fn prime_active_heads(&mut self) -> Result<()> {
        // We split the borrow: read cursor descriptors, then fetch rows.
        let n = self.active.as_ref().map(|a| a.cursors.len()).unwrap_or(0);
        for i in 0..n {
            let needs = self
                .active
                .as_ref()
                .map(|a| a.heads[i].is_none())
                .unwrap_or(false);
            if !needs {
                continue;
            }
            let next = self.next_cursor_row(i)?;
            if let Some(active) = self.active.as_mut() {
                active.heads[i] = next;
            }
        }
        Ok(())
    }

    /// Pull one row from cursor `i` of the active partition.
    fn next_cursor_row(&mut self, i: usize) -> Result<Option<Row>> {
        // Determine the cursor kind without holding an active borrow over
        // the source fetch.
        enum Kind {
            Mem,
            Sst(usize),
            Done,
        }
        let kind = match self.active.as_ref().and_then(|a| a.cursors.get(i)) {
            Some(RowCursor::Mem(_)) => Kind::Mem,
            Some(RowCursor::Sst { src }) => Kind::Sst(*src),
            None => Kind::Done,
        };
        match kind {
            Kind::Mem => Ok(self.active.as_mut().and_then(|a| match &mut a.cursors[i] {
                RowCursor::Mem(it) => it.next(),
                RowCursor::Sst { .. } => None,
            })),
            Kind::Sst(src) => self.sources[src].next_fragment_row(),
            Kind::Done => Ok(None),
        }
    }

    /// Emit one fragment of up to `k` deletion-suppressed, cell-merged rows
    /// from the active partition. Returns `Ok(None)` only when the active
    /// partition is fully drained AND nothing needs emitting (no rows and
    /// not the first fragment) — in which case the active partition has
    /// been retired and its sources refilled.
    fn emit_fragment(&mut self, k: usize) -> Result<Option<PartitionFragment>> {
        let mut out: Vec<Row> = Vec::with_capacity(k);
        let partition_delete_at = self
            .active
            .as_ref()
            .map(|a| a.deletion.marked_for_delete_at)
            .unwrap_or(i64::MIN);
        let partition_deleted = self
            .active
            .as_ref()
            .map(|a| !a.deletion.is_live())
            .unwrap_or(false);

        while out.len() < k {
            // Pick the smallest clustering key across all live heads.
            let smallest = {
                let active = self.active.as_ref().expect("active set");
                let mut sk: Option<&[u8]> = None;
                for head in active.heads.iter().flatten() {
                    if sk.map(|c| head.clustering.as_slice() < c).unwrap_or(true) {
                        sk = Some(head.clustering.as_slice());
                    }
                }
                sk.map(|c| c.to_vec())
            };
            let Some(ck) = smallest else {
                break; // partition exhausted
            };

            // Fold every head at that clustering key (cell-level LWW),
            // advancing each consumed cursor by one row.
            let mut merged_row: Option<Row> = None;
            let n = self.active.as_ref().expect("active set").cursors.len();
            for i in 0..n {
                let matches = self
                    .active
                    .as_ref()
                    .and_then(|a| a.heads[i].as_ref())
                    .map(|r| r.clustering == ck)
                    .unwrap_or(false);
                if !matches {
                    continue;
                }
                let row = self
                    .active
                    .as_mut()
                    .and_then(|a| a.heads[i].take())
                    .expect("matched head present");
                merged_row = Some(match merged_row.take() {
                    Some(prev) => merge::merge_rows(prev, row),
                    None => row,
                });
                // Advance this cursor's head.
                let next = self.next_cursor_row(i)?;
                if let Some(a) = self.active.as_mut() {
                    a.heads[i] = next;
                }
            }

            let mut row = merged_row.expect("at least one head matched the smallest key");

            // Deletion suppression (mirrors merge::apply_deletions):
            // partition-level drop of rows older than the partition
            // tombstone, then row-level cell suppression.
            if partition_deleted && row.primary_key_liveness.timestamp < partition_delete_at {
                continue;
            }
            if !row.deletion.is_live() {
                let cut = row.deletion.marked_for_delete_at;
                row.cells.retain(|(_col, cell)| cell.timestamp >= cut);
            }
            out.push(row);
        }

        // Did we exhaust the partition?
        let exhausted = self
            .active
            .as_ref()
            .map(|a| a.heads.iter().all(Option::is_none))
            .unwrap_or(true);

        let is_first = self.active.as_ref().map(|a| a.first).unwrap_or(false);

        // Nothing to emit: no rows and not the first fragment. Retire the
        // active partition (refill its sources) and signal the caller to
        // advance to the next key.
        if out.is_empty() && !is_first {
            self.retire_active_partition()?;
            return Ok(None);
        }

        // Build the fragment. The first fragment carries the header
        // (deletion + static row); subsequent ones carry LIVE/None so a
        // flattening consumer never double-applies them.
        let (key, deletion, static_row) = {
            let active = self.active.as_mut().expect("active set");
            if active.first {
                active.first = false;
                (
                    active.key.clone(),
                    active.deletion,
                    active.pending_static_row.take(),
                )
            } else {
                (
                    active.key.clone(),
                    ferrosa_sstable::types::DeletionTime::LIVE,
                    None,
                )
            }
        };

        if exhausted {
            self.retire_active_partition()?;
        }

        Ok(Some(PartitionFragment {
            key,
            deletion,
            static_row,
            rows: out,
            first: is_first,
            last: exhausted,
        }))
    }

    /// Refill the active partition's sources back onto the heap and clear
    /// the active state.
    fn retire_active_partition(&mut self) -> Result<()> {
        let refill = match self.active.take() {
            Some(a) => a.refill_srcs,
            None => return Ok(()),
        };
        for src in refill {
            self.refill_source(src)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{group_disjoint_runs, merger_for_projected_sources, merger_for_sources};
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

    // ---- Intra-partition fragment streaming (P0 row-OOM fix) ----

    /// All fixture timestamps must be `>= test_header().min_timestamp`
    /// (1_000_000) or the SSTable writer's delta encoding underflows and
    /// silently corrupts the file. Base everything off this.
    const TS_BASE: i64 = 2_000_000;

    /// `ts` is a small relative offset; it is shifted by `TS_BASE` so the
    /// absolute timestamp clears `min_timestamp`. Relative ordering between
    /// rows is preserved (so LWW conflicts behave as written).
    fn row(clustering: i32, col: u16, value: &[u8], ts: i64) -> Row {
        let ts = TS_BASE + ts;
        Row {
            clustering: clustering.to_be_bytes().to_vec(),
            cells: vec![(col, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn partition_with_rows(key: &[u8], rows: Vec<Row>) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::from(key)),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    /// Drain a fragment merger built over `sstables` into the flattened
    /// `Vec<Partition>` it represents (one merged Partition per key, with
    /// all fragments of a key concatenated), while asserting the
    /// per-fragment + resident-row invariants. Returns `(partitions, max_resident_rows)`.
    fn drain_fragments(
        sstables: &[Arc<SSTableReader<Vec<u8>>>],
        k: usize,
    ) -> (Vec<Partition>, usize) {
        let empty_active: Box<dyn Iterator<Item = Partition> + Send> = Box::new(std::iter::empty());
        let mut merger = merger_for_sources(empty_active, None, sstables, None, None).unwrap();

        let mut out: Vec<Partition> = Vec::new();
        let mut cur: Option<Partition> = None;
        // The merger holds, at most, one row per source ("heads") plus the
        // fragment being built (<= k). We do not have direct access to the
        // internal heads count here, so we bound the *fragment* size and
        // additionally assert no single fragment ever exceeds k.
        let mut max_fragment = 0usize;
        loop {
            let frag = merger.next_fragment(k).unwrap();
            let Some(frag) = frag else { break };
            assert!(
                frag.rows.len() <= k,
                "fragment {} exceeds k={k}",
                frag.rows.len()
            );
            max_fragment = max_fragment.max(frag.rows.len());
            if frag.first {
                if let Some(p) = cur.take() {
                    out.push(p);
                }
                cur = Some(Partition {
                    key: frag.key.clone(),
                    deletion: frag.deletion,
                    static_row: frag.static_row.clone(),
                    rows: Vec::new(),
                });
            } else {
                // Non-first fragments must carry no header.
                assert!(
                    frag.deletion.is_live(),
                    "non-first fragment carried a deletion"
                );
                assert!(
                    frag.static_row.is_none(),
                    "non-first fragment carried a static row"
                );
            }
            cur.as_mut()
                .expect("first fragment seen before continuation")
                .rows
                .extend(frag.rows.clone());
            if frag.last {
                if let Some(p) = cur.take() {
                    out.push(p);
                }
            }
        }
        if let Some(p) = cur.take() {
            out.push(p);
        }
        (out, max_fragment)
    }

    /// Whole-partition reference: drive `next_merged_partition` to
    /// completion. This is the byte-identical oracle the fragment path
    /// must reproduce.
    fn drain_whole(sstables: &[Arc<SSTableReader<Vec<u8>>>]) -> Vec<Partition> {
        let empty_active: Box<dyn Iterator<Item = Partition> + Send> = Box::new(std::iter::empty());
        let mut merger = merger_for_sources(empty_active, None, sstables, None, None).unwrap();
        let mut out = Vec::new();
        while let Some(p) = merger.next_merged_partition().unwrap() {
            out.push(p);
        }
        out
    }

    /// MEMORY-BOUND (RED before the fix): one partition with N rows must
    /// stream as ceil(N/k) fragments each holding <= k rows. The
    /// whole-partition path materialises all N rows in one `Vec<Row>`; the
    /// fragment path never holds more than k rows of the partition body at
    /// once. We assert the resident-row watermark (max fragment size) stays
    /// <= k even though the partition has N >> k rows.
    #[test]
    fn single_wide_partition_streams_in_bounded_fragments() {
        let n: i32 = 5_000;
        let k = 64;
        let rows: Vec<Row> = (0..n)
            .map(|c| row(c, 0, format!("v{c}").as_bytes(), 1000))
            .collect();
        let reader = Arc::new(reader_from_partitions(&[partition_with_rows(b"hot", rows)]));

        let (partitions, max_fragment) = drain_fragments(std::slice::from_ref(&reader), k);
        assert_eq!(partitions.len(), 1, "one partition");
        assert_eq!(partitions[0].rows.len(), n as usize, "all rows reassembled");
        // The resident-row watermark: no fragment ever exceeds k. With the
        // pre-fix whole-partition path this would be N (5000) in one Vec.
        assert!(
            max_fragment <= k,
            "resident fragment rows {max_fragment} exceeded k={k} — partition was materialised whole"
        );
        // And it actually fragmented (proves the path isn't a single big chunk).
        assert!(n as usize > k, "fixture must force fragmentation");
        assert!(
            max_fragment == k,
            "expected fragments to fill to k; got max {max_fragment}"
        );

        // Equivalence with the whole-partition merge.
        let whole = drain_whole(std::slice::from_ref(&reader));
        assert_eq!(
            partitions, whole,
            "fragment-flattened != whole-partition merge"
        );
    }

    /// CORRECTNESS ORACLE: a single wide partition spread across MULTIPLE
    /// overlapping SSTables with LWW conflicts (newer timestamp wins per
    /// clustering key) must be byte-identical whether read whole or
    /// fragmented, for several values of k (forcing different batch
    /// boundaries).
    #[test]
    fn wide_partition_lww_conflicts_fragment_equiv_whole() {
        // SSTable A: rows 0..100 at ts=1000 (value "a{c}").
        // SSTable B: even rows at ts=2000 (value "b{c}") — newer, must win.
        // SSTable C: rows 50..150 at ts=1500 (value "c{c}").
        let a: Vec<Row> = (0..100)
            .map(|c| row(c, 0, format!("a{c}").as_bytes(), 1000))
            .collect();
        let b: Vec<Row> = (0..100)
            .filter(|c| c % 2 == 0)
            .map(|c| row(c, 0, format!("b{c}").as_bytes(), 2000))
            .collect();
        let c: Vec<Row> = (50..150)
            .map(|c| row(c, 0, format!("c{c}").as_bytes(), 1500))
            .collect();
        let ra = Arc::new(reader_from_partitions(&[partition_with_rows(b"wide", a)]));
        let rb = Arc::new(reader_from_partitions(&[partition_with_rows(b"wide", b)]));
        let rc = Arc::new(reader_from_partitions(&[partition_with_rows(b"wide", c)]));
        let sstables = vec![ra, rb, rc];

        let whole = drain_whole(&sstables);
        for k in [1usize, 2, 3, 7, 31, 1024] {
            let (frag, _) = drain_fragments(&sstables, k);
            assert_eq!(
                frag, whole,
                "fragment(k={k}) diverged from whole-partition merge"
            );
        }
    }

    /// CORRECTNESS ORACLE: tombstones / row deletes within a wide partition.
    /// Row-level tombstones (newer ts) must suppress older cells; a
    /// partition-level tombstone must suppress older rows — identically
    /// across fragment boundaries.
    #[test]
    fn wide_partition_tombstones_fragment_equiv_whole() {
        // Base rows 0..40 at ts=1000.
        let base: Vec<Row> = (0..40)
            .map(|c| row(c, 0, format!("v{c}").as_bytes(), 1000))
            .collect();
        // Tombstone source: row-level deletes for clustering 5,15,25 at ts=2000.
        let deletes: Vec<Row> = [5i32, 15, 25]
            .into_iter()
            .map(|c| Row {
                clustering: c.to_be_bytes().to_vec(),
                cells: vec![],
                deletion: DeletionTime::new(TS_BASE + 2000, 100),
                primary_key_liveness: LivenessInfo::NONE,
            })
            .collect();
        let rb = Arc::new(reader_from_partitions(&[partition_with_rows(
            b"wide", base,
        )]));
        let rd = Arc::new(reader_from_partitions(&[partition_with_rows(
            b"wide", deletes,
        )]));
        let sstables = vec![rb, rd];

        let whole = drain_whole(&sstables);
        for k in [1usize, 2, 4, 8, 1024] {
            let (frag, _) = drain_fragments(&sstables, k);
            assert_eq!(frag, whole, "tombstone fragment(k={k}) diverged from whole");
        }
    }

    /// CORRECTNESS ORACLE: a partition-level tombstone suppresses rows older
    /// than it across fragment boundaries, and keeps newer rows.
    #[test]
    fn wide_partition_partition_tombstone_fragment_equiv_whole() {
        // Old rows 0..30 at ts=1000 (suppressed); new rows 30..60 at ts=3000 (kept).
        let mut rows: Vec<Row> = (0..30)
            .map(|c| row(c, 0, format!("old{c}").as_bytes(), 1000))
            .collect();
        rows.extend((30..60).map(|c| row(c, 0, format!("new{c}").as_bytes(), 3000)));
        let mut p = partition_with_rows(b"wide", rows);
        p.deletion = DeletionTime::new(TS_BASE + 2000, 100);
        let rp = Arc::new(reader_from_partitions(&[p]));
        let sstables = vec![rp];

        let whole = drain_whole(&sstables);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].rows.len(), 30, "only the 30 newer rows survive");
        for k in [1usize, 4, 7, 1024] {
            let (frag, _) = drain_fragments(&sstables, k);
            assert_eq!(frag, whole, "partition-tombstone fragment(k={k}) diverged");
        }
    }

    /// Header with one static column (`s`), so a static-row cell at
    /// ordinal 0 is in range for the SSTable writer.
    fn static_header() -> SerializationHeader {
        let mut h = test_header();
        h.static_columns = vec![(
            b"s".to_vec(),
            "org.apache.cassandra.db.marshal.UTF8Type".into(),
        )];
        h
    }

    fn reader_from_partitions_with_header(
        partitions: &[Partition],
        header: SerializationHeader,
    ) -> SSTableReader<Vec<u8>> {
        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                verify_output: false,
                ..WriteOptions::default()
            },
            header,
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

    /// CORRECTNESS ORACLE: a static row + clustering rows. The static row
    /// rides the FIRST fragment only; clustering rows fragment underneath.
    #[test]
    fn wide_partition_static_row_fragment_equiv_whole() {
        let mut rows: Vec<Row> = (0..50)
            .map(|c| row(c, 0, format!("v{c}").as_bytes(), 1000))
            .collect();
        rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
        let mut p = partition_with_rows(b"wide", rows);
        p.static_row = Some(Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"static".to_vec(), TS_BASE + 1234))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        });
        let rp = Arc::new(reader_from_partitions_with_header(&[p], static_header()));
        let sstables = vec![rp];

        let whole = drain_whole(&sstables);
        for k in [1usize, 3, 8, 1024] {
            let (frag, _) = drain_fragments(&sstables, k);
            assert_eq!(frag, whole, "static-row fragment(k={k}) diverged");
            assert!(
                frag[0].static_row.is_some(),
                "static row must be reassembled"
            );
        }
    }

    /// Many partitions interleaved with one wide partition: the merger must
    /// stream the wide one in fragments and the narrow ones whole, in token
    /// order, byte-identical to the whole-partition path.
    #[test]
    fn mixed_narrow_and_wide_partitions_fragment_equiv_whole() {
        let mut partitions = Vec::new();
        for i in 0..20u32 {
            let key = format!("k{i:03}");
            if i == 10 {
                let rows: Vec<Row> = (0..500)
                    .map(|c| row(c, 0, format!("w{c}").as_bytes(), 1000))
                    .collect();
                partitions.push(partition_with_rows(key.as_bytes(), rows));
            } else {
                partitions.push(partition_with_rows(
                    key.as_bytes(),
                    vec![row(0, 0, format!("n{i}").as_bytes(), 1000)],
                ));
            }
        }
        // SSTable writer requires partitions in token (DecoratedKey) order.
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let reader = Arc::new(reader_from_partitions(&partitions));
        let sstables = vec![reader];

        let whole = drain_whole(&sstables);
        let (frag, max_fragment) = drain_fragments(&sstables, 16);
        assert!(max_fragment <= 16, "fragment exceeded k");
        assert_eq!(
            frag, whole,
            "mixed-width fragment stream diverged from whole"
        );
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

    #[test]
    fn projected_range_merger_propagates_truncated_sstable_tail_error() {
        let first = test_partition(b"pk1", 1, b"first");
        let second = test_partition(b"pk2", 1, b"second");
        let third = test_partition(b"pk3", 1, b"third");
        let good_before = reader_from_partitions(std::slice::from_ref(&first));
        let corrupt_reader = reader_with_truncated_tail(std::slice::from_ref(&second));
        let good_after = reader_from_partitions(std::slice::from_ref(&third));
        let sstables = vec![
            Arc::new(good_before),
            Arc::new(corrupt_reader),
            Arc::new(good_after),
        ];
        let wanted = [0];

        let mut merger = merger_for_projected_sources(
            Box::new(std::iter::empty()),
            None,
            &sstables,
            &wanted,
            None,
            None,
        )
        .unwrap();

        assert!(
            merger.next_merged_partition().unwrap().is_some(),
            "first readable partition should still be emitted"
        );
        let err = merger
            .next_merged_partition()
            .expect_err("projected range scan must fail closed on corrupt SSTable data");
        assert!(
            err.to_string().contains("read_exact_at")
                || err.to_string().contains("unexpected EOF")
                || err.to_string().contains("UnexpectedEof"),
            "error should identify the SSTable read failure, got: {err}"
        );
    }
}
