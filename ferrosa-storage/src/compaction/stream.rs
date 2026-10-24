//! Pull-driven compaction: one row per source, incremental staged BTI output.
//! Correctness: differential validator, legacy reorder, purge and cancel tests.
//! Last revised: 2026-10-23
//! Last changed: Replace whole-partition grouping with worker-owned row cursors.
//!
//! Modern sources never buffer a complete partition. Legacy rows use the existing
//! bounded external sorter (4 MiB plus one row, bounded merge fan-in). The one
//! unpurged fallback is spooled to staging disk, never cloned into the heap.

#[cfg(any(test, feature = "test-support"))]
use super::cancel_harness::CancelPoint;
use super::executor::{validate_row_writable, OutputTally};
use super::{metadata::CompactionTask, purge};
use crate::{
    external_sort::{ExternalSorter, SortedRows, SpillOrder, SpillRow},
    merge,
    range_merger::ColumnOrdinalMapping,
};
use ferrosa_common::{CancelToken, CellValue, Error, Result};
use ferrosa_sstable::{
    io::FileReadAt,
    reader::{PartitionIter, SSTableReader},
    statistics::SerializationHeader,
    types::{DeletionTime, LivenessInfo, Partition, Row},
    writer::SSTableWriter,
};
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

// Spill serialization borrows payloads; only a recovered row owns decoded bytes.
struct StoredRow(Row);
type CellRecord = (Option<Vec<u8>>, i64, i32, i32, Option<Vec<u8>>);
type RowRecord = (Vec<u8>, Vec<(u16, CellRecord)>, (i64, u32), (i64, i32, i32));
struct Cells<'a>(&'a [(u16, CellValue)]);
impl Serialize for Cells<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for (col, c) in self.0 {
            seq.serialize_element(&(
                col,
                (&c.value, c.timestamp, c.ttl, c.local_deletion_time, &c.path),
            ))?;
        }
        seq.end()
    }
}
struct BorrowedRow<'a>(&'a Row);
impl Serialize for BorrowedRow<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let r = self.0;
        (
            &r.clustering,
            Cells(&r.cells),
            (
                r.deletion.marked_for_delete_at,
                r.deletion.local_deletion_time,
            ),
            (
                r.primary_key_liveness.timestamp,
                r.primary_key_liveness.ttl,
                r.primary_key_liveness.local_deletion_time,
            ),
        )
            .serialize(s)
    }
}
impl Serialize for StoredRow {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        BorrowedRow(&self.0).serialize(s)
    }
}
impl<'de> Deserialize<'de> for StoredRow {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let (clustering, cells, dt, live) = RowRecord::deserialize(d)?;
        Ok(Self(Row {
            clustering,
            cells: cells
                .into_iter()
                .map(|(i, (value, timestamp, ttl, local_deletion_time, path))| {
                    (
                        i,
                        CellValue {
                            value,
                            timestamp,
                            ttl,
                            local_deletion_time,
                            path,
                        },
                    )
                })
                .collect(),
            deletion: DeletionTime::new(dt.0, dt.1),
            primary_key_liveness: LivenessInfo {
                timestamp: live.0,
                ttl: live.1,
                local_deletion_time: live.2,
            },
        }))
    }
}
impl SpillRow for StoredRow {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.0.clustering.capacity()
            + self.0.cells.capacity() * std::mem::size_of::<(u16, CellValue)>()
            + self
                .0
                .cells
                .iter()
                .map(|(_, c)| {
                    c.value.as_ref().map_or(0, Vec::capacity)
                        + c.path.as_ref().map_or(0, Vec::capacity)
                })
                .sum::<usize>()
    }
}
// Preserve original on-disk order on exact row metadata ties across spill runs.
#[derive(Serialize, Deserialize)]
struct OrderedRow {
    row: StoredRow,
    ordinal: u64,
}
impl SpillRow for OrderedRow {
    fn estimated_bytes(&self) -> usize {
        self.row.estimated_bytes() + 8
    }
}
#[derive(Clone)]
struct ClusteringOrder;
impl SpillOrder<OrderedRow> for ClusteringOrder {
    fn compare(&self, a: &OrderedRow, b: &OrderedRow) -> std::cmp::Ordering {
        a.row
            .0
            .clustering
            .cmp(&b.row.0.clustering)
            .then(a.ordinal.cmp(&b.ordinal))
    }
}
struct Source<'a> {
    iter: PartitionIter<'a, FileReadAt>,
    header: Option<Partition>,
    head: Option<Row>,
    spare: Option<Row>,
    spans: Vec<ferrosa_sstable::data::ValueSpan>,
    raw_spans: Vec<ferrosa_sstable::data::ValueSpan>,
    span_indices: Vec<usize>,
    fetched: std::cell::RefCell<Vec<bool>>,
    sorted: Option<SortedRows<OrderedRow, ClusteringOrder>>,
    previous_ck: Vec<u8>,
    previous_key: Option<(i64, Vec<u8>)>,
    mapping: &'a ColumnOrdinalMapping,
    legacy: bool,
}
impl Source<'_> {
    fn next_row(&mut self) -> Result<()> {
        // Keep raw descriptors even when schema remapping drops a column.
        // A fetched winner/tie already checked CRCs; validate every other span
        // before advancing the source or returning a row for publication.
        if self.head.is_some() && !self.legacy {
            let fetched = self.fetched.get_mut();
            for (i, span) in self.raw_spans.iter().enumerate() {
                if !fetched[i] {
                    self.iter.validate_value_span(*span)?;
                }
            }
        }
        self.head = if let Some(rows) = self.sorted.as_mut() {
            rows.next().transpose()?.map(|r| r.row.0)
        } else {
            let mut row = self
                .head
                .take()
                .or_else(|| self.spare.take())
                .unwrap_or_else(empty_row);
            if self
                .iter
                .next_clustered_row_pending_into(&mut row, &mut self.spans)?
            {
                self.raw_spans.clone_from(&self.spans);
                self.span_indices.clear();
                self.span_indices.extend(0..self.spans.len());
                let fetched = self.fetched.get_mut();
                fetched.clear();
                fetched.resize(self.spans.len(), false);
                Some(row)
            } else {
                self.spare = Some(row);
                None
            }
        };
        if let Some(row) = &mut self.head {
            if row.clustering < self.previous_ck {
                return Err(Error::InvalidData(
                    "compaction input rows out of clustering order; requires legacy rewrite".into(),
                ));
            }
            self.previous_ck.clear();
            self.previous_ck.extend_from_slice(&row.clustering);
            if self.legacy {
                self.mapping.remap_regular_row(row);
            } else {
                self.mapping
                    .remap_regular_metadata(row, &mut self.spans, &mut self.span_indices);
            }
        }
        Ok(())
    }
    fn copy_value(&self, cell: usize, out: &mut Vec<u8>) -> Result<()> {
        if self.legacy {
            out.clear();
            out.extend_from_slice(
                self.head.as_ref().expect("head").cells[cell]
                    .1
                    .value
                    .as_deref()
                    .unwrap_or(&[]),
            );
            Ok(())
        } else {
            self.iter.read_value_span(self.spans[cell], out)?;
            self.fetched.borrow_mut()[self.span_indices[cell]] = true;
            Ok(())
        }
    }
    fn next_partition(&mut self, dir: &Path, cancel: &CancelToken) -> Result<()> {
        let Some((key, deletion, mut static_row)) = self.iter.next_partition_header_only()? else {
            self.header = None;
            self.head = None;
            return Ok(());
        };
        if let Some((token, bytes)) = &self.previous_key {
            if (*token, bytes.as_slice()) >= (key.token.0, key.key.as_bytes()) {
                return Err(Error::InvalidData(
                    "aborting compaction: corrupt Data.db partitions out of token order".into(),
                ));
            }
        }
        let prev = self.previous_key.get_or_insert_with(|| (0, Vec::new()));
        prev.0 = key.token.0;
        prev.1.clear();
        prev.1.extend_from_slice(key.key.as_bytes());
        if let Some(row) = &mut static_row {
            self.mapping.remap_static_row(row);
        }
        self.header = Some(Partition {
            key,
            deletion,
            static_row,
            rows: Vec::new(),
        });
        self.sorted = None;
        self.previous_ck.clear();
        if self.legacy {
            std::fs::create_dir_all(dir)?;
            let mut sorter = ExternalSorter::new(dir, ClusteringOrder, 4 * 1024 * 1024);
            let mut ordinal = 0;
            while let Some(row) = self.iter.next_clustered_row()? {
                cancel
                    .check()
                    .map_err(|e| Error::InvalidData(e.to_string()))?;
                sorter.push(OrderedRow {
                    row: StoredRow(row),
                    ordinal,
                })?;
                ordinal += 1;
            }
            self.sorted = Some(sorter.finish()?);
        }
        self.next_row()
    }
}

fn empty_row() -> Row {
    Row {
        clustering: Vec::new(),
        cells: Vec::new(),
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::NONE,
    }
}
fn copy_optional(src: &Option<Vec<u8>>, dst: &mut Option<Vec<u8>>) {
    match (src, dst.as_mut()) {
        (Some(src), Some(dst)) => dst.clone_from(src),
        _ => *dst = src.clone(),
    }
}
fn copy_cell(src: &CellValue, dst: &mut CellValue) {
    copy_optional(&src.value, &mut dst.value);
    copy_optional(&src.path, &mut dst.path);
    dst.timestamp = src.timestamp;
    dst.ttl = src.ttl;
    dst.local_deletion_time = src.local_deletion_time;
}
#[derive(Default)]
struct MergeScratch {
    candidates: Vec<(usize, usize)>,
    tie_a: Vec<u8>,
    tie_b: Vec<u8>,
    deletion_source: usize,
    liveness_source: usize,
    cell_sources: Vec<usize>,
}
fn supersedes(
    sources: &[Source<'_>],
    candidate: (usize, usize),
    current: (usize, usize),
    a: &mut Vec<u8>,
    b: &mut Vec<u8>,
) -> Result<bool> {
    let cand = &sources[candidate.0].head.as_ref().expect("candidate").cells[candidate.1].1;
    let cur = &sources[current.0].head.as_ref().expect("current").cells[current.1].1;
    if cand.timestamp != cur.timestamp {
        return Ok(cand.timestamp > cur.timestamp);
    }
    if cand.value.is_some() != cur.value.is_some() {
        return Ok(cand.value.is_some());
    }
    if cand.value.is_none() {
        return Ok(cand.local_deletion_time > cur.local_deletion_time);
    }
    sources[candidate.0].copy_value(candidate.1, a)?;
    sources[current.0].copy_value(current.1, b)?;
    Ok((a.as_slice(), cand.local_deletion_time) > (b.as_slice(), cur.local_deletion_time))
}
fn copy_candidate(
    sources: &[Source<'_>],
    candidate: (usize, usize),
    out: &mut CellValue,
) -> Result<()> {
    let value = &sources[candidate.0].head.as_ref().expect("candidate").cells[candidate.1].1;
    copy_cell(value, out);
    if let Some(buffer) = &mut out.value {
        sources[candidate.0].copy_value(candidate.1, buffer)?;
    }
    Ok(())
}
/// Source priority is task input order, matching merge_partitions' supplied
/// source order. Preserve it across duplicate-row batches, including metadata
/// the LWW comparator leaves tied (row TTL/deletion time and exact cell ties).
/// Select cells by metadata before fetching values. Equal-timestamp byte ties
/// use two reusable comparison buffers; ordinary losing values are never owned.
fn merge_heads(
    sources: &[Source<'_>],
    group: &[usize],
    ck: &[u8],
    out: &mut Row,
    reset: bool,
    scratch: &mut MergeScratch,
) -> Result<()> {
    let MergeScratch {
        candidates,
        tie_a,
        tie_b,
        deletion_source,
        liveness_source,
        cell_sources,
    } = scratch;
    candidates.clear();
    let mut first = reset;
    for &i in group {
        let Some(row) = sources[i].head.as_ref().filter(|r| r.clustering == ck) else {
            continue;
        };
        if first {
            out.clustering.clone_from(&row.clustering);
            out.deletion = row.deletion;
            out.primary_key_liveness = row.primary_key_liveness;
            *deletion_source = i;
            *liveness_source = i;
            first = false;
        } else {
            if row.deletion.marked_for_delete_at > out.deletion.marked_for_delete_at
                || (row.deletion.marked_for_delete_at == out.deletion.marked_for_delete_at
                    && i < *deletion_source)
            {
                out.deletion = row.deletion;
                *deletion_source = i;
            }
            if row.primary_key_liveness.timestamp > out.primary_key_liveness.timestamp
                || (row.primary_key_liveness.timestamp == out.primary_key_liveness.timestamp
                    && i < *liveness_source)
            {
                out.primary_key_liveness = row.primary_key_liveness;
                *liveness_source = i;
            }
        }
        candidates.extend((0..row.cells.len()).map(|c| (i, c)));
    }
    let cell = |(i, c): (usize, usize)| &sources[i].head.as_ref().expect("candidate head").cells[c];
    candidates.sort_unstable_by(|a, b| {
        let a_cell = cell(*a);
        let b_cell = cell(*b);
        (a_cell.0, &a_cell.1.path, a).cmp(&(b_cell.0, &b_cell.1.path, b))
    });
    let mut used = 0;
    for j in 0..candidates.len() {
        let next = candidates[j];
        if used > 0 {
            let prev = candidates[used - 1];
            let a = cell(prev);
            let b = cell(next);
            if a.0 == b.0 && a.1.path == b.1.path {
                if supersedes(sources, next, prev, tie_a, tie_b)? {
                    candidates[used - 1] = next;
                }
                continue;
            }
        }
        candidates[used] = next;
        used += 1;
    }
    candidates.truncate(used);
    if reset {
        cell_sources.resize(candidates.len(), usize::MAX);
        for (j, &candidate) in candidates.iter().enumerate() {
            let (col, _) = cell(candidate);
            if j == out.cells.len() {
                out.cells.push((*col, CellValue::tombstone(0, 0)));
            }
            out.cells[j].0 = *col;
            cell_sources[j] = candidate.0;
            copy_candidate(sources, candidate, &mut out.cells[j].1)?;
        }
        out.cells.truncate(candidates.len());
    } else {
        for &candidate in candidates.iter() {
            let (col, value) = cell(candidate);
            match out
                .cells
                .binary_search_by(|(i, v)| (*i, &v.path).cmp(&(*col, &value.path)))
            {
                Ok(j) => {
                    let cur = &out.cells[j].1;
                    let ordering = if value.timestamp != cur.timestamp {
                        value.timestamp.cmp(&cur.timestamp)
                    } else if value.value.is_some() != cur.value.is_some() {
                        value.value.is_some().cmp(&cur.value.is_some())
                    } else if value.value.is_none() {
                        value.local_deletion_time.cmp(&cur.local_deletion_time)
                    } else {
                        sources[candidate.0].copy_value(candidate.1, tie_a)?;
                        (tie_a.as_slice(), value.local_deletion_time)
                            .cmp(&(cur.value.as_deref().unwrap_or(&[]), cur.local_deletion_time))
                    };
                    if ordering.is_gt() || (ordering.is_eq() && candidate.0 < cell_sources[j]) {
                        copy_candidate(sources, candidate, &mut out.cells[j].1)?;
                        cell_sources[j] = candidate.0;
                    }
                }
                Err(j) => {
                    let mut value = CellValue::tombstone(0, 0);
                    copy_candidate(sources, candidate, &mut value)?;
                    out.cells.insert(j, (*col, value));
                    cell_sources.insert(j, candidate.0);
                }
            }
        }
    }
    Ok(())
}

struct Fallback {
    header: Partition,
    path: std::path::PathBuf,
}
fn write_row(
    writer: &mut SSTableWriter,
    header: &SerializationHeader,
    table: &str,
    p: &mut Partition,
    scratch: &mut Vec<Row>,
    row: Row,
) -> std::result::Result<Row, String> {
    // Legacy whole-value collections can expand into several cells in this ONE row.
    p.rows = std::mem::take(scratch);
    p.rows.push(row);
    crate::memtable::expand_collection_blobs_in_place(p, header, table)
        .map_err(|e| e.to_string())?;
    let row = &p.rows[0];
    validate_row_writable(row, false, p, header)?;
    writer.write_partition_row(row).map_err(|e| e.to_string())?;
    let row = p.rows.pop().expect("one output row");
    *scratch = std::mem::take(&mut p.rows);
    Ok(row)
}
fn begin(
    writer: &mut SSTableWriter,
    header: &SerializationHeader,
    p: &mut Partition,
    table: &str,
) -> std::result::Result<(), String> {
    crate::memtable::expand_collection_blobs_in_place(p, header, table)
        .map_err(|e| e.to_string())?;
    if let Some(row) = &p.static_row {
        validate_row_writable(row, true, p, header)?;
    }
    writer
        .begin_partition(&p.key, p.deletion, p.static_row.as_ref())
        .map_err(|e| format!("write partition: {e}"))
}

/// Inputs are borrowed for the task; all cursor state stays worker-owned.
pub(super) struct Inputs<'a> {
    pub task: &'a CompactionTask,
    pub readers: &'a [Arc<SSTableReader<FileReadAt>>],
    pub mappings: &'a [ColumnOrdinalMapping],
}

/// Select and fold one clustering key, including adjacent duplicates in a source.
/// Only the head row of each source and the reusable output row are resident.
fn next_merged_row(
    sources: &mut [Source<'_>],
    group: &[usize],
    ck: &mut Vec<u8>,
    out: &mut Row,
    scratch: &mut MergeScratch,
    cancel: &CancelToken,
) -> std::result::Result<Option<usize>, String> {
    let Some(smallest) = group
        .iter()
        .filter_map(|&i| sources[i].head.as_ref())
        .min_by(|a, b| a.clustering.cmp(&b.clustering))
    else {
        return Ok(None);
    };
    ck.clear();
    ck.extend_from_slice(&smallest.clustering);
    let mut consumed = 0;
    let mut reset = true;
    loop {
        merge_heads(sources, group, ck, out, reset, scratch).map_err(|e| e.to_string())?;
        reset = false;
        for &i in group {
            if sources[i]
                .head
                .as_ref()
                .is_some_and(|r| r.clustering == *ck)
            {
                cancel.check().map_err(|e| e.to_string())?;
                consumed += 1;
                sources[i].next_row().map_err(|e| e.to_string())?;
            }
        }
        if !group.iter().any(|&i| {
            sources[i]
                .head
                .as_ref()
                .is_some_and(|r| r.clustering == *ck)
        }) {
            return Ok(Some(consumed));
        }
    }
}

/// Merge and write all input partitions. Deletion suppression precedes marker
/// purge. Until a nonempty output exists, a disk spool preserves the first fully
/// purged partition for the publication protocol's nonempty replacement rule.
pub(super) fn compact<F: FnMut(usize)>(
    inputs: Inputs<'_>,
    header: &SerializationHeader,
    writer: &mut SSTableWriter,
    staging: &Path,
    cancel: &CancelToken,
    mut observe: F,
) -> std::result::Result<(OutputTally, usize, u64), String> {
    let Inputs {
        task,
        readers,
        mappings,
    } = inputs;
    let table = task.table_id.to_string();
    let mut sources = Vec::with_capacity(readers.len());
    for (i, reader) in readers.iter().enumerate() {
        let mut source = Source {
            iter: reader.scan_iter().map_err(|e| e.to_string())?,
            header: None,
            head: None,
            spare: None,
            spans: Vec::new(),
            raw_spans: Vec::new(),
            span_indices: Vec::new(),
            fetched: std::cell::RefCell::new(Vec::new()),
            sorted: None,
            previous_ck: Vec::new(),
            previous_key: None,
            mapping: &mappings[i],
            legacy: task.inputs[i].legacy_format,
        };
        source
            .next_partition(&staging.join(format!("sort-{i}")), cancel)
            .map_err(|e| e.to_string())?;
        sources.push(source);
    }
    let mut tally = OutputTally::default();
    let mut input_rows = 0;
    let mut purged = 0;
    let mut group = Vec::with_capacity(sources.len());
    let mut ck = Vec::new();
    let mut merged_row = empty_row();
    let mut scratch = MergeScratch::default();
    let mut held: Option<Fallback> = None;
    let mut shell_rows = Vec::with_capacity(1);
    #[cfg(any(test, feature = "test-support"))]
    let mut iteration = 0;
    loop {
        let first = sources
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.header.as_ref().map(|p| (i, &p.key)))
            .min_by(|a, b| a.1.cmp(b.1))
            .map(|(i, _)| i);
        let Some(first) = first else { break };
        #[cfg(any(test, feature = "test-support"))]
        {
            crate::compaction::cancel_point!(
                &table,
                if iteration == 0 {
                    CancelPoint::MergePartitionFirst
                } else {
                    CancelPoint::MergePartitionMiddle
                }
            );
            iteration += 1;
        }
        cancel.check().map_err(|e| e.to_string())?;
        group.clear();
        for (i, s) in sources.iter().enumerate() {
            if s.header
                .as_ref()
                .is_some_and(|p| p.key == sources[first].header.as_ref().expect("first").key)
            {
                group.push(i);
            }
        }
        let mut p = sources[first].header.take().expect("first");
        for &i in &group {
            if i == first {
                continue;
            }
            let other = sources[i].header.take().expect("group");
            if other.deletion.marked_for_delete_at > p.deletion.marked_for_delete_at {
                p.deletion = other.deletion;
            }
            p.static_row = match (p.static_row, other.static_row) {
                (Some(a), Some(b)) => Some(merge::merge_rows(a, b)),
                (a, None) => a,
                (None, b) => b,
            };
        }
        merge::apply_deletions(&mut p);
        if let Some(policy) = task.purge {
            merge::reclaim_covers_table(&mut p, policy.table_delete);
        }
        let deletion = p.deletion;
        let policy = task
            .purge
            .filter(|_| !crate::table_tombstone::is_table_tombstone_key(&p.key));
        let capture = policy.is_some() && tally.partitions == 0 && held.is_none();
        let fallback_path = staging.join("purge-fallback.json");
        let mut fallback = if capture {
            Some(BufWriter::new(
                std::fs::File::create(&fallback_path).map_err(|e| e.to_string())?,
            ))
        } else {
            None
        };
        let original = if capture { Some(p.clone()) } else { None }; // header + static row only
        let mut part_purged = 0;
        if let Some(policy) = policy {
            part_purged += purge::purge_partition(&mut p, &policy).markers();
        }
        let mut begun = !p.deletion.is_live() || p.static_row.is_some();
        if begun {
            observe(group.len());
            begin(writer, header, &mut p, &table)?;
            fallback = None;
        }
        while let Some(consumed) = next_merged_row(
            &mut sources,
            &group,
            &mut ck,
            &mut merged_row,
            &mut scratch,
            cancel,
        )? {
            input_rows += consumed;
            let row = &mut merged_row;
            if (!deletion.is_live()
                && row.primary_key_liveness.timestamp < deletion.marked_for_delete_at)
                || task.purge.is_some_and(|policy| {
                    !policy.table_delete.is_live()
                        && row.primary_key_liveness.timestamp
                            < policy.table_delete.marked_for_delete_at
                })
            {
                continue;
            }
            if !row.deletion.is_live() {
                row.cells
                    .retain(|(_, c)| c.timestamp >= row.deletion.marked_for_delete_at);
            }
            if let Some(w) = &mut fallback {
                serde_json::to_writer(&mut *w, &BorrowedRow(row)).map_err(|e| e.to_string())?;
                w.write_all(b"\n").map_err(|e| e.to_string())?;
            }
            if let Some(policy) = policy {
                let mut stats = purge::PurgeStats::default();
                let changed = purge::purge_row(row, &policy, &mut stats);
                part_purged += stats.markers();
                if changed && purge::row_is_vacant(row) {
                    continue;
                }
            }
            if !begun {
                observe(group.len());
                begin(writer, header, &mut p, &table)?;
                begun = true;
                fallback = None;
            }
            let owned = std::mem::replace(&mut merged_row, empty_row());
            merged_row = write_row(writer, header, &table, &mut p, &mut shell_rows, owned)?;
            tally.rows += 1;
        }
        if !begun && part_purged == 0 {
            observe(group.len());
            begin(writer, header, &mut p, &table)?;
            begun = true;
            fallback = None;
        }
        if begun {
            writer.end_partition().map_err(|e| e.to_string())?;
            tally.partitions += 1;
            tally.min_token = tally.min_token.min(p.key.token.0);
            tally.max_token = tally.max_token.max(p.key.token.0);
            held = None;
        } else if let Some(mut w) = fallback {
            w.flush().map_err(|e| e.to_string())?;
            held = Some(Fallback {
                header: original.expect("captured header"),
                path: fallback_path,
            });
        }
        if !begun {
            observe(group.len());
        }
        purged += part_purged;
        for &i in &group {
            sources[i]
                .next_partition(&staging.join(format!("sort-{i}")), cancel)
                .map_err(|e| e.to_string())?;
        }
    }
    if tally.partitions == 0 {
        let Some(mut held) = held else {
            return Err("no partitions to compact".into());
        };
        begin(writer, header, &mut held.header, &table)?;
        let file = std::fs::File::open(held.path).map_err(|e| e.to_string())?;
        for row in
            serde_json::Deserializer::from_reader(BufReader::new(file)).into_iter::<StoredRow>()
        {
            cancel.check().map_err(|e| e.to_string())?;
            write_row(
                writer,
                header,
                &table,
                &mut held.header,
                &mut shell_rows,
                row.map_err(|e| e.to_string())?.0,
            )?;
            tally.rows += 1;
        }
        writer.end_partition().map_err(|e| e.to_string())?;
        tally.partitions = 1;
        tally.min_token = held.header.key.token.0;
        tally.max_token = held.header.key.token.0;
        crate::metrics::inc_compaction_purge_held_back();
    }
    Ok((tally, input_rows, purged))
}
