//! Background reconciliation for the adjacency index (T5).
//!
//! Safety net for dropped observer mutations (backpressure) and crash recovery
//! gaps. Runs as a tokio task, yielding between partition scans to avoid
//! competing with query workloads.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_schema::Schema;
use ferrosa_sstable::types::DeletionTime;
use ferrosa_storage::{Mutation, TableId};

use crate::adjacency::observer::derive_adjacency_mutations;
use crate::adjacency::schema::{adjacency_keyspace_name, row_is_deleted, DIRECTION_OUT};
use crate::executor::expand::extract_neighbor_id;

/// Reconciliation metrics.
#[derive(Debug, Default)]
pub struct ReconcileMetrics {
    /// Live edge rows checked against the index.
    pub entries_checked: usize,
    /// Missing or tombstoned entries of live edges written back.
    pub entries_repaired: usize,
    /// Live entries removed: orphans with no edge, and entries still live for
    /// an edge that is deleted (the pre-fix observer derived LIVE entries from
    /// an edge tombstone).
    pub orphans_removed: usize,
    /// Reads, scans and writes that failed. A pass with errors is INCOMPLETE:
    /// the index may still be missing entries, so a caller that gates queries
    /// on the pass must not treat it as healed.
    pub errors: usize,
}

impl ReconcileMetrics {
    /// Whether the pass checked everything it set out to check.
    pub fn is_complete(&self) -> bool {
        self.errors == 0
    }
}

/// Process-wide reconcile counters for `/metrics`. The repair counters are the
/// observable side of the deploy heal: a node starting on a build after
/// 330a0c29 over an index the pre-fix reconcile damaged reports the entries
/// it wrote back here (and in a WARN line).
static ENTRIES_REPAIRED_TOTAL: AtomicU64 = AtomicU64::new(0);
static ENTRIES_REMOVED_TOTAL: AtomicU64 = AtomicU64::new(0);
static ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static PASSES_TOTAL: AtomicU64 = AtomicU64::new(0);
static HEALS_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);
static HEALS_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);

fn record_pass(metrics: &ReconcileMetrics) {
    PASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
    ENTRIES_REPAIRED_TOTAL.fetch_add(metrics.entries_repaired as u64, Ordering::Relaxed);
    ENTRIES_REMOVED_TOTAL.fetch_add(metrics.orphans_removed as u64, Ordering::Relaxed);
    ERRORS_TOTAL.fetch_add(metrics.errors as u64, Ordering::Relaxed);
}

/// Record the outcome of a keyspace's first-use heal (see
/// `GraphEngine::ensure_adjacency_ready`).
pub fn record_heal(completed: bool) {
    let counter = if completed {
        &HEALS_COMPLETED_TOTAL
    } else {
        &HEALS_FAILED_TOTAL
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Append the adjacency reconcile counters in Prometheus text format.
pub fn render_prometheus(out: &mut String) {
    use std::fmt::Write as _;
    for (name, help, counter) in [
        (
            "ferrosa_graph_adjacency_entries_repaired_total",
            "Adjacency entries of live edges written back by reconcile.",
            &ENTRIES_REPAIRED_TOTAL,
        ),
        (
            "ferrosa_graph_adjacency_entries_removed_total",
            "Live adjacency entries of deleted or missing edges tombstoned by reconcile.",
            &ENTRIES_REMOVED_TOTAL,
        ),
        (
            "ferrosa_graph_adjacency_reconcile_errors_total",
            "Reconcile reads, scans and writes that failed (the pass was incomplete).",
            &ERRORS_TOTAL,
        ),
        (
            "ferrosa_graph_adjacency_reconcile_passes_total",
            "Adjacency reconcile passes run.",
            &PASSES_TOTAL,
        ),
        (
            "ferrosa_graph_adjacency_heals_completed_total",
            "Graph keyspaces whose first-use adjacency heal completed.",
            &HEALS_COMPLETED_TOTAL,
        ),
        (
            "ferrosa_graph_adjacency_heals_failed_total",
            "First-use adjacency heals that were incomplete; their queries failed retryably.",
            &HEALS_FAILED_TOTAL,
        ),
    ] {
        // Writing to a String cannot fail.
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {}", counter.load(Ordering::Relaxed));
    }
}

/// Number of partitions to read per batch during reconciliation.
/// Retained for future use when WritePath supports batched range reads.
#[allow(dead_code)]
const BATCH_LIMIT: usize = 1000;
const RECONCILE_YIELD_EVERY_CHECKED_ENTRIES: usize = 256;
const RECONCILE_YIELD_EVERY_PARTITIONS: usize = 32;

fn should_yield_during_reconciliation(processed: usize, yield_every: usize) -> bool {
    yield_every > 0 && processed > 0 && processed.is_multiple_of(yield_every)
}

async fn skip_immediate_reconciliation_tick(ticker: &mut tokio::time::Interval) {
    // `tokio::time::interval` ticks immediately on first poll. Reconciliation is
    // a safety-net scan over potentially large graph tables, so starting it
    // immediately on process boot competes with cluster formation and can keep a
    // runtime worker busy before the node has accepted user traffic. Consume the
    // immediate tick so the first pass runs after the configured interval.
    ticker.tick().await;
}

/// Whether an edge table's partition key is exactly its `graph.source` column
/// and its clustering key exactly its `graph.target` column, so an adjacency
/// entry's (vertex, neighbour) names the edge's storage key.
fn edge_keyed_by_source_and_target(meta: &ferrosa_schema::metadata::table::TableMetadata) -> bool {
    let (Some(source), Some(target)) = (
        meta.extensions.get("graph.source"),
        meta.extensions.get("graph.target"),
    ) else {
        return false;
    };
    meta.partition_key.as_slice() == std::slice::from_ref(source)
        && meta.clustering_key.len() == 1
        && &meta.clustering_key[0].0 == target
}

/// Run one reconciliation pass for a keyspace.
pub async fn reconcile_once(
    schema: &Schema,
    write_path: &WritePath,
    keyspace: &str,
) -> ReconcileMetrics {
    let snap = schema.snapshot();
    let mut metrics = ReconcileMetrics::default();

    // Find all edge tables in keyspace, along with their metadata.
    let edge_tables: Vec<_> = snap
        .tables
        .iter()
        .filter(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        })
        .map(|((ks, name), meta)| (TableId::new(ks, name), meta.clone()))
        .collect();

    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");
    let point_checkable_edge_tables: std::collections::HashSet<String> = edge_tables
        .iter()
        .filter(|(_, meta)| edge_keyed_by_source_and_target(meta))
        .map(|(tid, _)| format!("{}.{}", tid.keyspace, tid.table))
        .collect();

    // Phase 1: For each edge table, scan partitions and verify adjacency entries exist.
    for (edge_tid, edge_meta) in &edge_tables {
        // Verify this edge table has source and target extensions.
        if !edge_meta.extensions.contains_key("graph.source")
            || !edge_meta.extensions.contains_key("graph.target")
        {
            continue;
        }

        let edge_table_fqn = format!("{}.{}", edge_tid.keyspace, edge_tid.table);

        // Scan all edge table partitions — STREAMED, one partition per pull
        // (t_bc5f0e6f). This loop already yielded every N partitions, but it
        // pre-collected the WHOLE edge table into a `Vec<Partition>` first, so
        // the yielding bought latency without bounding memory: a background
        // safety-net scan could hold a tenant-sized table resident and OOM the
        // node. Nothing here needs more than the partition in hand.
        let mut partitions = match write_path.range_read_stream_all(edge_tid, 0).await {
            Ok(stream) => stream,
            Err(e) => {
                // Best-effort by design (this is a periodic safety net, not a
                // read path), but never silent: the next pass retries, and an
                // edge table that keeps failing is visible in the log.
                metrics.errors += 1;
                tracing::warn!(
                    table = %edge_table_fqn,
                    error = %e,
                    "adjacency reconcile: could not open the edge-table scan; \
                     skipping this table for this pass"
                );
                continue;
            }
        };

        let mut edge_partitions_scanned = 0usize;
        let mut entries_processed = 0usize;
        while let Some(partition) = partitions.next().await {
            let partition = match partition {
                Ok(partition) => partition,
                Err(e) => {
                    metrics.errors += 1;
                    tracing::warn!(
                        table = %edge_table_fqn,
                        partitions_scanned = edge_partitions_scanned,
                        error = %e,
                        "adjacency reconcile: edge-table scan failed mid-stream; \
                         abandoning this table for this pass"
                    );
                    break;
                }
            };
            edge_partitions_scanned += 1;

            let expected = expected_entries(schema, edge_tid, &partition, &mut metrics);
            for (_, entry) in expected {
                reconcile_entry(
                    write_path,
                    &adj_table_id,
                    &edge_table_fqn,
                    entry,
                    &mut metrics,
                )
                .await;
                entries_processed += 1;
                if should_yield_during_reconciliation(
                    entries_processed,
                    RECONCILE_YIELD_EVERY_CHECKED_ENTRIES,
                ) {
                    tokio::task::yield_now().await;
                }
            }

            if should_yield_during_reconciliation(
                edge_partitions_scanned,
                RECONCILE_YIELD_EVERY_PARTITIONS,
            ) {
                tokio::task::yield_now().await;
            }
        }
    }

    // Phase 2: Scan adjacency index for orphans.
    // For each adjacency entry, verify the source edge still exists.
    // Streamed for the same reason as phase 1 (t_bc5f0e6f): the adjacency
    // index is as large as the edge data it mirrors. The previous
    // `unwrap_or_default()` also swallowed the scan error whole — an
    // unreachable adjacency table looked exactly like an empty one, i.e. like
    // "no orphans to remove".
    let adj_partitions = match write_path.range_read_stream_all(&adj_table_id, 0).await {
        Ok(stream) => Some(stream),
        Err(e) => {
            metrics.errors += 1;
            tracing::warn!(
                keyspace = %keyspace,
                error = %e,
                "adjacency reconcile: could not open the adjacency-index scan; \
                 orphan removal is skipped for this pass"
            );
            None
        }
    };

    if let Some(mut adj_partitions) = adj_partitions {
        let mut adjacency_partitions_scanned = 0usize;
        while let Some(partition) = adj_partitions.next().await {
            let partition = match partition {
                Ok(partition) => partition,
                Err(e) => {
                    metrics.errors += 1;
                    tracing::warn!(
                        keyspace = %keyspace,
                        partitions_scanned = adjacency_partitions_scanned,
                        error = %e,
                        "adjacency reconcile: adjacency-index scan failed mid-stream; \
                         abandoning orphan removal for this pass"
                    );
                    break;
                }
            };
            adjacency_partitions_scanned += 1;
            let vertex_id = partition.key.key.as_bytes().to_vec();

            for row in &partition.rows {
                // Already removed.
                if row_is_deleted(row) {
                    continue;
                }
                // Standard composite: [u16 1][1B direction][...].
                // Direction byte sits at offset 2 after the u16 length prefix.
                if row.clustering.len() < 3 {
                    continue;
                }
                let direction = row.clustering[2];

                // Extract edge label and neighbor ID from clustering.
                let neighbor_id = match extract_neighbor_id(&row.clustering, None) {
                    Some(id) => id,
                    None => continue,
                };
                let edge_label = match extract_edge_label(&row.clustering) {
                    Some(label) => label,
                    None => continue,
                };

                // Determine which edge table this entry references.
                // The edge_table FQN is stored in the row's first cell value.
                let edge_table_fqn = match row.cells.first() {
                    Some((_, cell)) => match &cell.value {
                        Some(bytes) => match std::str::from_utf8(bytes) {
                            Ok(s) => s.to_string(),
                            Err(_) => continue,
                        },
                        None => continue, // tombstone cell
                    },
                    None => continue,
                };

                // Only an edge table keyed exactly (graph.source) / (graph.target)
                // can be point-checked from an adjacency entry. For any other
                // layout (agent_memory's typed_edges carries a tenant in its
                // partition key) the entry does not name the edge's key, and
                // checking the wrong key would delete every entry as an orphan.
                if !point_checkable_edge_tables.contains(&edge_table_fqn) {
                    continue;
                }
                // Parse "keyspace.table" from the FQN.
                let (edge_ks, edge_tbl) = match edge_table_fqn.split_once('.') {
                    Some(pair) => pair,
                    None => continue,
                };
                let edge_tid = TableId::new(edge_ks, edge_tbl);

                // Determine the source and target based on direction.
                let (source_id, target_id) = if direction == DIRECTION_OUT {
                    (vertex_id.clone(), neighbor_id.clone())
                } else {
                    (neighbor_id.clone(), vertex_id.clone())
                };

                // Verify the edge exists in the edge table.
                let source_key = DecoratedKey::new(PartitionKey::new(source_id));
                let edge_exists = match write_path.read(&edge_tid, &source_key).await {
                    Ok(Some(p)) => p
                        .rows
                        .iter()
                        .any(|r| r.clustering == target_id && !row_is_deleted(r)),
                    Ok(None) => false,
                    // A failed read proves nothing; deleting on it would drop a
                    // live edge from every traversal.
                    Err(e) => {
                        metrics.errors += 1;
                        tracing::warn!(
                            table = %edge_table_fqn,
                            error = %e,
                            "adjacency reconcile: could not read an edge to check an \
                             adjacency entry; keeping the entry for this pass"
                        );
                        continue;
                    }
                };

                if !edge_exists {
                    // Write a tombstone to remove this orphan adjacency entry.
                    match write_tombstone(
                        write_path,
                        &adj_table_id,
                        &partition.key,
                        &row.clustering,
                        &edge_label,
                    )
                    .await
                    {
                        Ok(()) => metrics.orphans_removed += 1,
                        Err(e) => {
                            metrics.errors += 1;
                            tracing::warn!(
                                table = %edge_table_fqn,
                                error = %e,
                                "adjacency reconcile: could not remove an orphan adjacency \
                                 entry; the next pass retries"
                            );
                        }
                    }
                }

                if should_yield_during_reconciliation(
                    metrics.entries_checked + metrics.orphans_removed,
                    RECONCILE_YIELD_EVERY_CHECKED_ENTRIES,
                ) {
                    tokio::task::yield_now().await;
                }
            }

            if should_yield_during_reconciliation(
                adjacency_partitions_scanned,
                RECONCILE_YIELD_EVERY_PARTITIONS,
            ) {
                tokio::task::yield_now().await;
            }
        }
    }

    record_pass(&metrics);
    metrics
}

/// One adjacency entry an edge partition implies, aggregated over every edge
/// row in the partition that derives it.
///
/// Several edges can share one entry: the entry is keyed by (vertex, label,
/// neighbour), and agent_memory's `typed_edges` holds one row per `edge_type`
/// between the same pair. The entry must stay live while ANY of them is live,
/// so it is judged on the aggregate, never on one row.
struct ExpectedEntry {
    vertex_key: DecoratedKey,
    clustering: Vec<u8>,
    /// The entry's `edge_table` cell value, from a live edge.
    edge_table_cell: Option<Vec<u8>>,
    /// Newest write timestamp among the live edges deriving this entry.
    live_at: Option<i64>,
    /// Newest deletion timestamp among the deleted edges deriving it.
    deleted_at: Option<i64>,
}

/// The newest timestamp a live row was written at: its primary-key liveness
/// or its newest cell. `None` for a row with neither, which holds no data.
fn edge_written_at(row: &ferrosa_sstable::types::Row) -> Option<i64> {
    let liveness = row
        .primary_key_liveness
        .has_timestamp()
        .then_some(row.primary_key_liveness.timestamp);
    let newest_cell = row.cells.iter().map(|(_, cell)| cell.timestamp).max();
    liveness.into_iter().chain(newest_cell).max()
}

/// The entries the write-time observer derives for every edge row of
/// `partition` — so a composite-key edge table is keyed by its `graph.source`
/// and `graph.target` columns, not by its raw key bytes — keyed by
/// (vertex key, clustering).
fn expected_entries(
    schema: &Schema,
    edge_tid: &TableId,
    partition: &ferrosa_sstable::types::Partition,
    metrics: &mut ReconcileMetrics,
) -> BTreeMap<(Vec<u8>, Vec<u8>), ExpectedEntry> {
    let mut expected: BTreeMap<(Vec<u8>, Vec<u8>), ExpectedEntry> = BTreeMap::new();
    for row in &partition.rows {
        let deleted = row_is_deleted(row);
        let written_at = edge_written_at(row);
        if !deleted {
            if written_at.is_none() {
                continue;
            }
            metrics.entries_checked += 1;
        }
        let edge = Mutation::new(
            edge_tid.keyspace.clone(),
            edge_tid.table.clone(),
            partition.key.clone(),
            vec![row.clone()],
            written_at.unwrap_or(row.deletion.marked_for_delete_at),
        );
        for derived in derive_adjacency_mutations(schema, edge_tid, &edge) {
            for entry_row in derived.rows {
                let slot = expected
                    .entry((
                        derived.key.key.as_bytes().to_vec(),
                        entry_row.clustering.clone(),
                    ))
                    .or_insert_with(|| ExpectedEntry {
                        vertex_key: derived.key.clone(),
                        clustering: entry_row.clustering.clone(),
                        edge_table_cell: None,
                        live_at: None,
                        deleted_at: None,
                    });
                if deleted {
                    let at = row.deletion.marked_for_delete_at;
                    slot.deleted_at = Some(slot.deleted_at.map_or(at, |d| d.max(at)));
                } else {
                    slot.live_at = slot.live_at.max(written_at);
                    if slot.edge_table_cell.is_none() {
                        slot.edge_table_cell = entry_row
                            .cells
                            .first()
                            .and_then(|(_, cell)| cell.value.clone());
                    }
                }
            }
        }
    }
    expected
}

/// What the index holds for one entry.
enum EntryState {
    Absent,
    /// Live, last written at this timestamp.
    Live(i64),
    /// Tombstoned at this timestamp.
    Deleted(i64),
}

/// Read one entry's state. `Err` is a failed read: the caller counts it and
/// leaves the entry alone (the pass is then incomplete).
async fn read_entry_state(
    write_path: &WritePath,
    adj_table_id: &TableId,
    vertex_key: &DecoratedKey,
    clustering: &[u8],
) -> ferrosa_common::Result<EntryState> {
    let Some(partition) = write_path.read(adj_table_id, vertex_key).await? else {
        return Ok(EntryState::Absent);
    };
    let Some(row) = partition
        .rows
        .iter()
        .find(|row| row.clustering == clustering)
    else {
        return Ok(EntryState::Absent);
    };
    if row_is_deleted(row) {
        return Ok(EntryState::Deleted(row.deletion.marked_for_delete_at));
    }
    Ok(match edge_written_at(row) {
        Some(at) => EntryState::Live(at),
        None => EntryState::Absent,
    })
}

/// The write that brings one entry in line with the edges behind it, if any.
///
/// Timestamps are chosen so a concurrent client write still wins:
/// - an entry of a live edge is written at the edge's own write time, or one
///   microsecond past a tombstone that shadows it (the pre-fix reconcile's,
///   or a deleted sibling's) — never at "now", which would outrank a delete
///   of the edge that lands while the pass runs;
/// - a live entry of a deleted edge is tombstoned at the later of the edge's
///   deletion and the entry's own write, so a re-create that lands while the
///   pass runs (written after the pass read the entry) still wins.
fn entry_repair(
    entry: &ExpectedEntry,
    state: &EntryState,
) -> Option<(ferrosa_sstable::types::Row, i64, bool)> {
    use ferrosa_common::cell::CellValue;
    use ferrosa_sstable::types::{LivenessInfo, Row};

    if let Some(live_at) = entry.live_at {
        let at = match *state {
            EntryState::Live(_) => return None,
            EntryState::Absent => live_at,
            EntryState::Deleted(deleted_at) => live_at.max(deleted_at.saturating_add(1)),
        };
        let cells = entry
            .edge_table_cell
            .clone()
            .map(|value| vec![(0, CellValue::live(value, at))])
            .unwrap_or_default();
        let row = Row {
            clustering: entry.clustering.clone(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(at),
        };
        return Some((row, at, true));
    }
    let (EntryState::Live(written_at), Some(deleted_at)) = (state, entry.deleted_at) else {
        return None;
    };
    let at = deleted_at.max(*written_at);
    let row = Row {
        clustering: entry.clustering.clone(),
        cells: vec![],
        deletion: DeletionTime::new(at, (at / 1_000_000).clamp(0, i64::from(u32::MAX)) as u32),
        primary_key_liveness: LivenessInfo::NONE,
    };
    Some((row, at, false))
}

/// Check one expected entry against the index and repair it.
async fn reconcile_entry(
    write_path: &WritePath,
    adj_table_id: &TableId,
    edge_table_fqn: &str,
    entry: ExpectedEntry,
    metrics: &mut ReconcileMetrics,
) {
    let state = match read_entry_state(
        write_path,
        adj_table_id,
        &entry.vertex_key,
        &entry.clustering,
    )
    .await
    {
        Ok(state) => state,
        Err(e) => {
            metrics.errors += 1;
            tracing::warn!(
                table = %edge_table_fqn,
                error = %e,
                "adjacency reconcile: could not read an adjacency partition; \
                 skipping its repair for this pass"
            );
            return;
        }
    };
    let Some((row, at, live)) = entry_repair(&entry, &state) else {
        return;
    };
    let repair = Mutation::new(
        adj_table_id.keyspace.clone(),
        adj_table_id.table.clone(),
        entry.vertex_key,
        vec![row],
        at,
    );
    match write_mutation(write_path, repair).await {
        Ok(()) if live => metrics.entries_repaired += 1,
        Ok(()) => metrics.orphans_removed += 1,
        Err(e) => {
            metrics.errors += 1;
            tracing::warn!(
                table = %edge_table_fqn,
                error = %e,
                "adjacency reconcile: could not write an adjacency repair; \
                 the next pass retries"
            );
        }
    }
}

/// Extract the edge label string from an adjacency clustering key.
///
/// Standard composite layout: [u16 1][1B direction][u16 label_len][label]...
fn extract_edge_label(clustering: &[u8]) -> Option<String> {
    if clustering.len() < 7 {
        return None;
    }
    // Skip component 0 (direction): [u16 len][bytes]
    let dir_len = u16::from_be_bytes([clustering[0], clustering[1]]) as usize;
    let label_len_pos = 2 + dir_len;
    if label_len_pos + 2 > clustering.len() {
        return None;
    }
    let label_len =
        u16::from_be_bytes([clustering[label_len_pos], clustering[label_len_pos + 1]]) as usize;
    let label_start = label_len_pos + 2;
    if label_start + label_len > clustering.len() {
        return None;
    }
    std::str::from_utf8(&clustering[label_start..label_start + label_len])
        .ok()
        .map(|s| s.to_string())
}

/// Write a mutation by decomposing it into individual row writes via WritePath.
///
/// Takes the `Mutation` by value: each row is MOVED into its `WritePath::write`
/// call instead of being deep-cloned (the previous `&Mutation` receiver forced a
/// `row.clone()` per row). The mutation is owned by the caller and dropped right
/// after, so consuming it removes one heap clone per repaired row on the
/// reconcile hot path with no change to which rows are written or their order.
async fn write_mutation(write_path: &WritePath, mutation: Mutation) -> ferrosa_common::Result<()> {
    let table_id = TableId::new(&mutation.keyspace, &mutation.table);
    for row in mutation.rows {
        write_path
            .write(
                &table_id,
                &mutation.key,
                row,
                mutation.timestamp,
                ferrosa_cluster::consistency::ConsistencyLevel::One,
                &ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
                    replication_factor: 1,
                },
            )
            .await?;
    }
    Ok(())
}

/// Write a tombstone row for an orphan adjacency entry.
async fn write_tombstone(
    write_path: &WritePath,
    adj_table_id: &TableId,
    vertex_key: &DecoratedKey,
    clustering: &[u8],
    _edge_label: &str,
) -> ferrosa_common::Result<()> {
    use ferrosa_sstable::types::{LivenessInfo, Row};

    let now_us = now_micros();
    let now_secs = (now_us / 1_000_000) as u32;

    let tombstone_row = Row {
        clustering: clustering.to_vec(),
        cells: vec![],
        deletion: DeletionTime::new(now_us, now_secs),
        primary_key_liveness: LivenessInfo::NONE,
    };

    write_path
        .write(
            adj_table_id,
            vertex_key,
            tombstone_row,
            now_us,
            ferrosa_cluster::consistency::ConsistencyLevel::One,
            &ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await
}

/// Returns the current time in microseconds since epoch.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Spawn the background reconciliation loop.
pub fn spawn_reconciliation(
    schema: Arc<Schema>,
    write_path: Arc<WritePath>,
    keyspace: String,
    interval: Duration,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        skip_immediate_reconciliation_tick(&mut ticker).await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let metrics = reconcile_once(&schema, &write_path, &keyspace).await;
                    if !metrics.is_complete() {
                        tracing::warn!(
                            keyspace = %keyspace,
                            checked = metrics.entries_checked,
                            repaired = metrics.entries_repaired,
                            orphans = metrics.orphans_removed,
                            errors = metrics.errors,
                            "adjacency reconciliation incomplete; the next pass retries"
                        );
                    } else if metrics.entries_repaired > 0 || metrics.orphans_removed > 0 {
                        tracing::info!(
                            keyspace = %keyspace,
                            checked = metrics.entries_checked,
                            repaired = metrics.entries_repaired,
                            orphans = metrics.orphans_removed,
                            "adjacency reconciliation complete"
                        );
                    }
                    tokio::task::yield_now().await;
                }
                _ = cancel.cancelled() => {
                    tracing::info!(keyspace = %keyspace, "reconciliation loop shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use crate::adjacency::observer::make_adjacency_mutation;
    use crate::adjacency::schema::DIRECTION_IN;

    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, SchemaConfig, TestAuditSink,
    };
    use ferrosa_sstable::types::{LivenessInfo, Row};
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use indexmap::IndexMap;

    #[test]
    fn reconcile_yield_policy_triggers_at_threshold() {
        assert!(!should_yield_during_reconciliation(0, 32));
        assert!(!should_yield_during_reconciliation(31, 32));
        assert!(should_yield_during_reconciliation(32, 32));
        assert!(should_yield_during_reconciliation(64, 32));
    }

    #[test]
    fn reconcile_yield_policy_can_be_disabled() {
        assert!(!should_yield_during_reconciliation(32, 0));
    }

    /// Virtual time: with the clock paused, time advances only when the runtime is
    /// idle and a timer is pending, so the two timers below fire strictly in deadline
    /// order however starved the CPU is. The earlier version raced a real 10 ms
    /// timeout against a real 50 ms interval, so a stall between two statements
    /// could let the interval elapse first.
    #[tokio::test(start_paused = true)]
    async fn reconciliation_timer_skips_immediate_first_tick() {
        let interval = Duration::from_millis(50);
        let mut ticker = tokio::time::interval(interval);

        // This would otherwise complete immediately. After the helper, the next
        // tick should wait for the configured interval instead of launching a
        // full reconciliation pass at process startup.
        skip_immediate_reconciliation_tick(&mut ticker).await;

        let started = tokio::time::Instant::now();
        let early = tokio::time::timeout(Duration::from_millis(10), ticker.tick()).await;
        assert!(
            early.is_err(),
            "reconciliation should not run again until the configured interval elapses"
        );

        // And it does run once the interval has elapsed: the tick lands exactly one
        // interval after the skipped one, not sooner and not later.
        ticker.tick().await;
        assert_eq!(started.elapsed(), interval);
    }

    fn test_storage_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: Default::default(),
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap()
    }

    /// Register the edge table and adjacency table schemas with the storage engine,
    /// and register the edge table metadata with the schema registry.
    fn setup_edge_and_adjacency(
        schema: &Schema,
        storage: &StorageEngine,
        keyspace: &str,
        edge_table_name: &str,
    ) {
        // Register keyspace in schema registry.
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: keyspace.to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();

        // Register the adjacency keyspace.
        let adj_ks = adjacency_keyspace_name(keyspace);
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: adj_ks.clone(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();

        // Build edge table metadata with graph extensions.
        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "edge".to_string());
        extensions.insert("graph.label".to_string(), edge_table_name.to_uppercase());
        extensions.insert("graph.source".to_string(), "src_id".to_string());
        extensions.insert("graph.target".to_string(), "dst_id".to_string());

        let mut columns = IndexMap::new();
        columns.insert(
            "src_id".to_string(),
            ColumnMetadata {
                name: "src_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "blob".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "dst_id".to_string(),
            ColumnMetadata {
                name: "dst_id".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "blob".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );

        let mut flags = HashSet::new();
        flags.insert(TableFlag::Compound);

        let edge_meta = TableMetadata {
            keyspace: keyspace.to_string(),
            name: edge_table_name.to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["src_id".to_string()],
            clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags,
            extensions,
            is_system: false,
        };

        schema.create_table_internal(edge_meta).unwrap();

        // Register the adjacency table in the schema registry.
        let adj_table_meta = crate::adjacency::schema::adjacency_table_metadata(keyspace);
        schema.create_table_internal(adj_table_meta).unwrap();

        // Register edge table with storage engine.
        let edge_storage_schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: edge_table_name.to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "dst_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        };
        storage.register_table(edge_storage_schema).unwrap();

        // Register adjacency table with storage engine.
        let adj_storage_schema = TableSchema {
            keyspace: adj_ks.clone(),
            table: "adjacency".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![
                ColumnDefinition {
                    name: "direction".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.ByteType".to_string(),
                },
                ColumnDefinition {
                    name: "edge_label".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "neighbor_id".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
                },
            ],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "edge_table".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(adj_storage_schema).unwrap();
    }

    /// Write an edge row into the edge table in storage.
    fn write_edge(
        storage: &StorageEngine,
        keyspace: &str,
        table: &str,
        source: &[u8],
        target: &[u8],
    ) {
        let edge_tid = TableId::new(keyspace, table);
        let key = DecoratedKey::new(PartitionKey::new(source.to_vec()));
        let row = Row {
            clustering: target.to_vec(),
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        storage.write(&edge_tid, &key, row, 1000).unwrap();
    }

    #[test]
    fn reconcile_metrics_default_is_zero() {
        let m = ReconcileMetrics::default();
        assert_eq!(m.entries_checked, 0);
        assert_eq!(m.entries_repaired, 0);
        assert_eq!(m.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_repairs_missing_adjacency_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        // Write edge data: alice -> bob, alice -> carol.
        write_edge(&storage, "social", "knows", b"alice", b"bob");
        write_edge(&storage, "social", "knows", b"alice", b"carol");

        // No adjacency entries exist yet. Reconciliation should repair them.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;

        // 2 edge rows checked.
        assert_eq!(metrics.entries_checked, 2);
        // 2 edges x 2 directions (OUT + IN) = 4 repairs.
        assert_eq!(metrics.entries_repaired, 4);
        assert_eq!(metrics.orphans_removed, 0);

        // Verify adjacency entries were created.
        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Check alice's OUT entries.
        let alice_key = DecoratedKey::new(PartitionKey::new(b"alice".to_vec()));
        let alice_partition = storage.read(&adj_tid, &alice_key).unwrap().unwrap();
        assert!(alice_partition.rows.iter().any(|r| {
            r.clustering.len() >= 3
                && r.clustering[2] == DIRECTION_OUT
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"bob".to_vec())
        }));
        assert!(alice_partition.rows.iter().any(|r| {
            r.clustering.len() >= 3
                && r.clustering[2] == DIRECTION_OUT
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"carol".to_vec())
        }));

        // Check bob's IN entry.
        let bob_key = DecoratedKey::new(PartitionKey::new(b"bob".to_vec()));
        let bob_partition = storage.read(&adj_tid, &bob_key).unwrap().unwrap();
        assert!(bob_partition.rows.iter().any(|r| {
            r.clustering.len() >= 3
                && r.clustering[2] == DIRECTION_IN
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"alice".to_vec())
        }));

        // Check carol's IN entry.
        let carol_key = DecoratedKey::new(PartitionKey::new(b"carol".to_vec()));
        let carol_partition = storage.read(&adj_tid, &carol_key).unwrap().unwrap();
        assert!(carol_partition.rows.iter().any(|r| {
            r.clustering.len() >= 3
                && r.clustering[2] == DIRECTION_IN
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"alice".to_vec())
        }));
    }

    #[tokio::test]
    async fn reconcile_is_idempotent_when_entries_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        write_edge(&storage, "social", "knows", b"alice", b"bob");

        // First reconciliation creates the entries.
        let wp = WritePath::direct(storage.clone());
        let m1 = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(m1.entries_repaired, 2); // OUT + IN

        // Second reconciliation should find everything in order.
        let m2 = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(m2.entries_checked, 1);
        assert_eq!(m2.entries_repaired, 0);
        assert_eq!(m2.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_removes_orphan_adjacency_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Manually write an adjacency entry without a corresponding edge.
        let orphan_mutation = make_adjacency_mutation(
            &adj_ks,
            b"orphan_src",
            DIRECTION_OUT,
            "knows",
            b"orphan_dst",
            "social.knows",
            1000,
        );
        for row in &orphan_mutation.rows {
            storage
                .write(
                    &adj_tid,
                    &orphan_mutation.key,
                    row.clone(),
                    orphan_mutation.timestamp,
                )
                .unwrap();
        }

        // Verify the orphan entry exists before reconciliation.
        let orphan_key = DecoratedKey::new(PartitionKey::new(b"orphan_src".to_vec()));
        let before = storage.read(&adj_tid, &orphan_key).unwrap();
        assert!(before.is_some());
        assert!(!before.unwrap().rows.is_empty());

        // Run reconciliation — should detect and remove the orphan.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(metrics.orphans_removed, 1);
    }

    /// Write a row tombstone over edge `source -> target` at `ts`.
    fn delete_edge(storage: &StorageEngine, table: &str, source: &[u8], target: &[u8], ts: i64) {
        let row = Row {
            clustering: target.to_vec(),
            cells: vec![],
            deletion: DeletionTime::new(ts, 0),
            primary_key_liveness: LivenessInfo::NONE,
        };
        let key = DecoratedKey::new(PartitionKey::new(source.to_vec()));
        storage
            .write(&TableId::new("social", table), &key, row, ts)
            .unwrap();
    }

    /// A deleted edge is not repaired back into the adjacency index, and an
    /// adjacency entry that is only a tombstone counts as missing for a live
    /// edge — otherwise a deleted edge comes back on the next pass, or a live
    /// one stays untraversable forever.
    #[tokio::test]
    async fn reconcile_skips_deleted_edges_and_restores_tombstoned_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();
        setup_edge_and_adjacency(&schema, &storage, "social", "knows");
        write_edge(&storage, "social", "knows", b"alice", b"bob");
        write_edge(&storage, "social", "knows", b"alice", b"carol");
        delete_edge(&storage, "knows", b"alice", b"carol", 2000);

        let wp = WritePath::direct(storage.clone());
        let first = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(
            (first.entries_checked, first.entries_repaired),
            (1, 2),
            "only the live edge is checked and given its OUT and IN entries"
        );

        // Tombstone alice's OUT entry for the live edge: it must come back.
        let adj_tid = TableId::new(adjacency_keyspace_name("social"), "adjacency");
        let entry = make_adjacency_mutation(
            &adjacency_keyspace_name("social"),
            b"alice",
            DIRECTION_OUT,
            // The edge table's graph.label, as the repair writes it.
            "KNOWS",
            b"bob",
            "social.knows",
            0,
        );
        let tombstone = Row {
            clustering: entry.rows[0].clustering.clone(),
            cells: vec![],
            deletion: DeletionTime::new(now_micros(), 0),
            primary_key_liveness: LivenessInfo::NONE,
        };
        storage
            .write(&adj_tid, &entry.key, tombstone, now_micros())
            .unwrap();
        let second = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(
            second.entries_repaired, 1,
            "a tombstoned entry for a live edge is missing and is repaired"
        );
    }

    #[tokio::test]
    async fn reconcile_no_edge_tables_returns_zero_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        // No edge tables registered — should return zero metrics.
        let wp = WritePath::direct(storage);
        let metrics = reconcile_once(&schema, &wp, "nonexistent").await;
        assert_eq!(metrics.entries_checked, 0);
        assert_eq!(metrics.entries_repaired, 0);
        assert_eq!(metrics.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_partial_repair_only_missing_direction() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        // Write edge data.
        write_edge(&storage, "social", "knows", b"alice", b"bob");

        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Manually write only the OUT adjacency entry.
        let out_mutation = make_adjacency_mutation(
            &adj_ks,
            b"alice",
            DIRECTION_OUT,
            "KNOWS",
            b"bob",
            "social.knows",
            1000,
        );
        for row in &out_mutation.rows {
            storage
                .write(
                    &adj_tid,
                    &out_mutation.key,
                    row.clone(),
                    out_mutation.timestamp,
                )
                .unwrap();
        }

        // Reconciliation should only repair the missing IN entry.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(metrics.entries_checked, 1);
        assert_eq!(metrics.entries_repaired, 1); // Only the IN entry was missing.
    }

    #[test]
    fn extract_edge_label_parses_correctly() {
        // Standard composite: [u16 1][1B direction][u16 label_len][label][...]
        let mut clustering = Vec::new();
        clustering.extend_from_slice(&1u16.to_be_bytes());
        clustering.push(DIRECTION_OUT);
        let label = b"KNOWS";
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        let neighbor = b"bob";
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(neighbor);

        assert_eq!(extract_edge_label(&clustering), Some("KNOWS".to_string()));
    }

    #[test]
    fn extract_edge_label_too_short() {
        assert_eq!(extract_edge_label(&[0, 0]), None);
    }
}
