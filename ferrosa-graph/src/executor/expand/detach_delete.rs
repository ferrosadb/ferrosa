//! DETACH DELETE as ONE atomic batch whose size never sits in memory.
//!
//! Deleting a vertex tombstones every physical edge row incident to it, the
//! adjacency entries those edges derive, and the vertex row, all in one atomic
//! batch. An adjacency entry is keyed by (vertex, direction, label, neighbour)
//! without tenant scope, so an entry another scope's live edge still derives
//! must be written back LIVE at `timestamp + 1` instead of tombstoned. Both
//! halves have to commit together: written in chunks, a protected entry at
//! `timestamp + 1` would outrank a later chunk's tombstone at `timestamp`.
//!
//! The batch used to be a `Vec<Mutation>` plus a `HashMap` of entry updates and
//! a `HashSet` of removed rows, all proportional to the vertex's degree. Now:
//!
//! - **The batch is staged**, not collected: every mutation is serialized into a
//!   [`WriteSetStage`] (resident up to the spill threshold, then on disk) and
//!   committed with [`GraphWrite::write_staged_batch`], which applies the staged
//!   set through the storage engine's atomic batch one mutation at a time.
//! - **The protect pass is a sort-merge.** Pass 1 records every entry an
//!   incident edge derives (a candidate tombstone); pass 2 records every entry
//!   a live edge touching the vertex derives at `timestamp + 1` (a possible
//!   protector). Both go into one [`ExternalSorter`] keyed by the entry, so the
//!   merge sees each entry's records together and decides it with O(1) state.
//!
//! The in-memory version this replaces decided each entry the same way: a
//! candidate becomes the LAST protector the scan met (the order it overwrote its
//! map in) when one exists, else a tombstone; a protector derived from an edge
//! being deleted does not count; entries no deleted edge derives are untouched.

use super::*;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use ferrosa_storage::external_sort::{ExternalSorter, SpillOrder, SpillRow};
use ferrosa_storage::write_set_stage::{StagedWriteSet, WriteSetStage};

/// DETACH DELETE batches whose staged write-set spilled to disk.
static SPILLED_BATCHES: AtomicU64 = AtomicU64::new(0);
/// DETACH DELETE batches built without a spill backend (fully resident).
static IN_MEMORY_BATCHES: AtomicU64 = AtomicU64::new(0);

/// DETACH DELETE batches whose staged write-set spilled to disk (observability).
pub fn detach_delete_spilled_batches() -> u64 {
    SPILLED_BATCHES.load(AtomicOrdering::Relaxed)
}

/// DETACH DELETE batches built with no spill backend configured: the designed,
/// counted in-memory fallback (unit tests, embedded use).
pub fn detach_delete_in_memory_batches() -> u64 {
    IN_MEMORY_BATCHES.load(AtomicOrdering::Relaxed)
}

/// Record kind: an entry an edge being deleted derives (a candidate
/// tombstone). Sorts before [`PROTECTOR`] for the same source row, so a
/// deleted edge's own live derivation is recognised and skipped.
const CANDIDATE: u8 = 0;
/// Record kind: an entry a live edge derives at `timestamp + 1`.
const PROTECTOR: u8 = 1;

/// One record of the protect sort-merge.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct EntryRecord {
    /// The adjacency entry: keyspace, table, partition key, clustering.
    keyspace: String,
    table: String,
    key: Vec<u8>,
    clustering: Vec<u8>,
    /// The physical edge row it was derived from: edge table, partition key,
    /// clustering.
    source: (String, Vec<u8>, Vec<u8>),
    /// [`CANDIDATE`] or [`PROTECTOR`].
    kind: u8,
    /// Scan order. Among the protectors of one entry the highest wins.
    seq: u64,
    /// A protector's serialized live entry mutation; empty for a candidate.
    entry: Vec<u8>,
}

impl EntryRecord {
    fn same_entry(&self, other: &EntryRecord) -> bool {
        self.keyspace == other.keyspace
            && self.table == other.table
            && self.key == other.key
            && self.clustering == other.clustering
    }
}

impl SpillRow for EntryRecord {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.keyspace.capacity()
            + self.table.capacity()
            + self.key.capacity()
            + self.clustering.capacity()
            + self.source.0.capacity()
            + self.source.1.capacity()
            + self.source.2.capacity()
            + self.entry.capacity()
    }
}

/// Entry, then source row, then kind, then scan order: a total order, so the
/// merge is deterministic whether or not it spilled.
#[derive(Debug, Clone, Copy)]
struct EntryOrder;

impl SpillOrder<EntryRecord> for EntryOrder {
    fn compare(&self, a: &EntryRecord, b: &EntryRecord) -> Ordering {
        (&a.keyspace, &a.table, &a.key, &a.clustering)
            .cmp(&(&b.keyspace, &b.table, &b.key, &b.clustering))
            .then_with(|| a.source.cmp(&b.source))
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.seq.cmp(&b.seq))
    }
}

/// Where a DETACH DELETE stages its batch and sorts its entry records, and how
/// much of each it keeps resident. Holds the temp-dir reservations so they
/// outlive the staged batch they back.
struct DetachSpill {
    _stage_dir: Option<ferrosa_storage::TempSortTableReservation>,
    sort_dir: Option<ferrosa_storage::TempSortTableReservation>,
    stage: WriteSetStage,
    sorter: ExternalSorter<EntryRecord, EntryOrder>,
}

impl DetachSpill {
    fn new(config: &GraphEngineConfig) -> Result<Self> {
        let Some(reserver) = config.spill.as_ref() else {
            let prior = IN_MEMORY_BATCHES.fetch_add(1, AtomicOrdering::Relaxed);
            if prior == 0 {
                tracing::warn!(
                    "graph DETACH DELETE is building its batch in memory: no spill backend \
                     is configured (GraphEngineConfig::spill is None), so it is not bounded \
                     by spill_threshold_bytes. Expected only in unit tests and embedded use; \
                     further occurrences are counted by detach_delete_in_memory_batches()"
                );
            }
            let unused = std::env::temp_dir();
            return Ok(Self {
                _stage_dir: None,
                sort_dir: None,
                stage: WriteSetStage::with_threshold(&unused, u64::MAX),
                sorter: ExternalSorter::new(unused, EntryOrder, u64::MAX),
            });
        };
        let reserve = |label: &str| {
            reserver
                .reserve(label)
                .map_err(|e| GraphError::Internal(format!("DETACH DELETE spill setup failed: {e}")))
        };
        let stage_dir = reserve("detach-delete-batch")?;
        let sort_dir = reserve("detach-delete-entries")?;
        Ok(Self {
            stage: WriteSetStage::with_threshold(stage_dir.path(), config.spill_threshold_bytes),
            sorter: ExternalSorter::new(sort_dir.path(), EntryOrder, config.spill_threshold_bytes),
            _stage_dir: Some(stage_dir),
            sort_dir: Some(sort_dir),
        })
    }

    /// Hand one entry record to the external sorter, which spills it to a
    /// sorted run on disk past the threshold; nothing accumulates here.
    fn record(&mut self, record: EntryRecord) -> Result<()> {
        self.sorter.push(record).map_err(GraphError::Storage)
    }

    /// Serialize one mutation into the staged batch.
    fn stage(&mut self, mutation: &Mutation, buf: &mut Vec<u8>) -> Result<()> {
        buf.clear();
        buf.resize(mutation.serialized_size(), 0);
        mutation.serialize_into(buf);
        self.stage.append(buf).map_err(GraphError::Storage)
    }
}

/// The staged batch, ready to commit, and the reservations backing it.
pub(super) struct StagedDetach {
    pub(super) batch: StagedWriteSet,
    pub(super) edges_deleted: usize,
    _spill: Option<ferrosa_storage::TempSortTableReservation>,
}

/// Build the whole DETACH DELETE batch for `target` without holding it in
/// memory: edge tombstones, decided adjacency entries, then the vertex
/// tombstone, staged in that order.
pub(super) async fn stage_detach_delete(
    write_path: &dyn GraphWrite,
    schema: &Schema,
    edges: &[ferrosa_schema::metadata::table::TableMetadata],
    target: &VertexDeleteTarget<'_>,
    timestamp: i64,
    config: &GraphEngineConfig,
) -> Result<StagedDetach> {
    let mut spill = DetachSpill::new(config)?;
    let mut buf = Vec::new();
    let mut seq = 0u64;
    let edges_deleted = stage_incident_edges(
        write_path, schema, edges, target, timestamp, &mut spill, &mut buf, &mut seq,
    )
    .await?;
    record_protectors(
        write_path, schema, edges, target, timestamp, &mut spill, &mut seq,
    )
    .await?;
    let DetachSpill {
        _stage_dir: stage_dir,
        sort_dir,
        mut stage,
        sorter,
    } = spill;
    stage_decided_entries(sorter, timestamp, &mut stage, &mut buf)?;
    drop(sort_dir);
    let vertex = tombstone_mutation(
        &target.table.keyspace,
        &target.table.table,
        target.key,
        Some(target.clustering.to_vec()),
        timestamp,
    );
    buf.clear();
    buf.resize(vertex.serialized_size(), 0);
    vertex.serialize_into(&mut buf);
    stage.append(&buf).map_err(GraphError::Storage)?;
    let batch = stage.finish().map_err(GraphError::Storage)?;
    if batch.spilled_to_disk() {
        SPILLED_BATCHES.fetch_add(1, AtomicOrdering::Relaxed);
    }
    Ok(StagedDetach {
        batch,
        edges_deleted,
        _spill: stage_dir,
    })
}

/// Pass 1: stream every declared edge table; for each physical row incident to
/// `target`, stage its tombstone and record the entries it derives as
/// candidates. Returns the number of edges deleted.
#[allow(clippy::too_many_arguments)]
async fn stage_incident_edges(
    write_path: &dyn GraphWrite,
    schema: &Schema,
    edges: &[ferrosa_schema::metadata::table::TableMetadata],
    target: &VertexDeleteTarget<'_>,
    timestamp: i64,
    spill: &mut DetachSpill,
    buf: &mut Vec<u8>,
    seq: &mut u64,
) -> Result<usize> {
    let mut deleted = 0usize;
    for edge in edges {
        let table = TableId::new(&edge.keyspace, &edge.name);
        let mut partitions = write_path.range_read_stream_all(&table, 0).await?;
        while let Some(partition) = partitions.next().await {
            let partition = partition?;
            if is_partition_dead(&partition) {
                continue;
            }
            for row in &partition.rows {
                if crate::adjacency::schema::row_is_deleted(row)
                    || !edge_is_incident(write_path, schema, target, edge, &partition, row).await?
                {
                    continue;
                }
                let source = (
                    edge.name.clone(),
                    partition.key.key.as_bytes().to_vec(),
                    row.clustering.clone(),
                );
                for entry in
                    derive_adjacency_row_mutations(schema, &table, &partition.key, row, timestamp)
                {
                    spill.record(entry_record(&entry, &source, CANDIDATE, *seq, Vec::new())?)?;
                }
                *seq += 1;
                let tombstone = tombstone_mutation(
                    &edge.keyspace,
                    &edge.name,
                    &partition.key,
                    Some(row.clustering.clone()),
                    timestamp,
                );
                spill.stage(&tombstone, buf)?;
                deleted += 1;
            }
        }
    }
    Ok(deleted)
}

/// Pass 2: stream every declared edge table again and record, as protectors,
/// the entries each live row touching `target`'s logical id derives at
/// `timestamp + 1`. Only such a row can derive a candidate entry: an entry
/// names both endpoints, and every candidate has the vertex as one of them.
#[allow(clippy::too_many_arguments)]
async fn record_protectors(
    write_path: &dyn GraphWrite,
    schema: &Schema,
    edges: &[ferrosa_schema::metadata::table::TableMetadata],
    target: &VertexDeleteTarget<'_>,
    timestamp: i64,
    spill: &mut DetachSpill,
    seq: &mut u64,
) -> Result<()> {
    let vertex = target.logical_key.key.as_bytes();
    let restore_at = timestamp.saturating_add(1);
    for edge in edges {
        let table = TableId::new(&edge.keyspace, &edge.name);
        let mut partitions = write_path.range_read_stream_all(&table, 0).await?;
        while let Some(partition) = partitions.next().await {
            let partition = partition?;
            if is_partition_dead(&partition) {
                continue;
            }
            for row in &partition.rows {
                if crate::adjacency::schema::row_is_deleted(row) {
                    continue;
                }
                let entries =
                    derive_adjacency_row_mutations(schema, &table, &partition.key, row, restore_at);
                if !entries.iter().any(|e| e.key.key.as_bytes() == vertex) {
                    continue;
                }
                let source = (
                    edge.name.clone(),
                    partition.key.key.as_bytes().to_vec(),
                    row.clustering.clone(),
                );
                for entry in entries {
                    // The record owns its encoded entry: it may outlive this
                    // loop in a sorted run on disk.
                    let mut payload = vec![0u8; entry.serialized_size()];
                    entry.serialize_into(&mut payload);
                    spill.record(entry_record(&entry, &source, PROTECTOR, *seq, payload)?)?;
                }
                *seq += 1;
            }
        }
    }
    Ok(())
}

fn entry_record(
    entry: &Mutation,
    source: &(String, Vec<u8>, Vec<u8>),
    kind: u8,
    seq: u64,
    payload: Vec<u8>,
) -> Result<EntryRecord> {
    let [row] = entry.rows.as_slice() else {
        return Err(GraphError::Internal(format!(
            "adjacency derivation produced {} rows for one entry; expected exactly one",
            entry.rows.len()
        )));
    };
    Ok(EntryRecord {
        keyspace: entry.keyspace.clone(),
        table: entry.table.clone(),
        key: entry.key.key.as_bytes().to_vec(),
        clustering: row.clustering.clone(),
        source: source.clone(),
        kind,
        seq,
        entry: payload,
    })
}

/// What the merge has seen of the entry it is on.
struct EntryDecision {
    first: EntryRecord,
    has_candidate: bool,
    /// The protector with the highest scan order whose source edge is not
    /// being deleted.
    best: Option<EntryRecord>,
    /// Source of the last candidate seen; that edge's own protector record
    /// follows it immediately and does not count.
    deleted_source: Option<(String, Vec<u8>, Vec<u8>)>,
}

impl EntryDecision {
    fn new(record: EntryRecord) -> Self {
        let mut decision = Self {
            first: EntryRecord {
                keyspace: record.keyspace.clone(),
                table: record.table.clone(),
                key: record.key.clone(),
                clustering: record.clustering.clone(),
                source: (String::new(), Vec::new(), Vec::new()),
                kind: CANDIDATE,
                seq: 0,
                entry: Vec::new(),
            },
            has_candidate: false,
            best: None,
            deleted_source: None,
        };
        decision.observe(record);
        decision
    }

    fn observe(&mut self, record: EntryRecord) {
        if record.kind == CANDIDATE {
            self.has_candidate = true;
            self.deleted_source = Some(record.source);
            return;
        }
        if self.deleted_source.as_ref() == Some(&record.source) {
            return;
        }
        if self.best.as_ref().is_none_or(|best| record.seq > best.seq) {
            self.best = Some(record);
        }
    }

    /// Stage the decided entry: the winning protector, else a tombstone. An
    /// entry no deleted edge derives is left alone.
    fn stage(self, timestamp: i64, stage: &mut WriteSetStage, buf: &mut Vec<u8>) -> Result<()> {
        if !self.has_candidate {
            return Ok(());
        }
        if let Some(best) = self.best {
            return stage.append(&best.entry).map_err(GraphError::Storage);
        }
        let tombstone = tombstone_mutation(
            &self.first.keyspace,
            &self.first.table,
            &DecoratedKey::new(PartitionKey::new(self.first.key)),
            Some(self.first.clustering),
            timestamp,
        );
        buf.clear();
        buf.resize(tombstone.serialized_size(), 0);
        tombstone.serialize_into(buf);
        stage.append(buf).map_err(GraphError::Storage)
    }
}

/// Merge the sorted entry records and stage one decided mutation per
/// candidate entry.
fn stage_decided_entries(
    sorter: ExternalSorter<EntryRecord, EntryOrder>,
    timestamp: i64,
    stage: &mut WriteSetStage,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut current: Option<EntryDecision> = None;
    for record in sorter.finish().map_err(GraphError::Storage)? {
        let record = record.map_err(GraphError::Storage)?;
        match current.as_mut() {
            Some(decision) if decision.first.same_entry(&record) => decision.observe(record),
            _ => {
                if let Some(done) = current.replace(EntryDecision::new(record)) {
                    done.stage(timestamp, stage, buf)?;
                }
            }
        }
    }
    if let Some(done) = current {
        done.stage(timestamp, stage, buf)?;
    }
    Ok(())
}
