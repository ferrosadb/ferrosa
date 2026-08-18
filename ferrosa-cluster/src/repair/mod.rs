//! Anti-entropy repair: periodic Merkle tree comparison across replicas.
//!
//! # Overview
//!
//! Each node builds a Merkle tree over its local data for a given token range.
//! During repair, two replicas exchange tree roots and walk down to find
//! divergent sub-ranges. Only the differing partitions are streamed.
//!
//! # Design
//!
//! - **MerkleTree**: A binary hash tree over sorted token ranges. Each leaf
//!   covers a sub-range and contains the XOR hash of all partition keys in
//!   that range.
//! - **RepairSession**: Coordinates tree exchange and diff streaming between
//!   two nodes for a given table and token range.
//! - **RepairScheduler**: Background task that periodically triggers repair
//!   sessions for all locally-owned token ranges.

pub mod cluster_view;
pub mod coordinator;
pub mod executor;
pub mod merkle;
pub mod rpc;
pub mod scheduler;
pub mod trigger;

pub use cluster_view::{
    ClusterRepairView, ClusterTopology, ProbeOutcome, RepairProbe, RingTopology, RpcRepairProbe,
};
pub use coordinator::{RepairCoordinator, SessionExecutor, SessionResult, SessionStats};
pub use executor::{
    InMemoryRepairStore, LocalRepairExecutor, RepairStore, StorageEngineRepairStore,
};
pub use merkle::MerkleTree;
pub use rpc::{RemoteRepairStore, RepairApplyHandler, RepairFetchHandler, RepairMerkleHandler};
pub use scheduler::{
    select_initiated_ranges, select_tables_for_tick, AutoRepairConfig, AutoRepairScheduler,
    InitiatedRange, RepairContext,
};
pub use trigger::{ClusterRepairTrigger, ExecutorProvider};

use ferrosa_sstable::types::Partition;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::error::Result;
use crate::raft::NodeState;
use crate::ring::TokenRing;

/// W8.7 — Compute the set of nodes that participate in repair for a
/// given token (per ADR-014 § "Token ownership").
///
/// Includes:
/// - Every voter (`NodeState::Normal`) that owns the token via
///   `ring.replicas(token, rf)`.
/// - Every learner with `owns_tokens=true` that owns the token.
///
/// Excludes:
/// - Learners with `owns_tokens=false` (analytics / witness — their
///   data converges via AppendEntries on the state machine, not via
///   anti-entropy on the ring).
/// - Joining / Leaving / Decommissioned nodes.
///
/// The repair scheduler iterates this set when deciding which peers to
/// exchange Merkle roots with for a given range.
pub fn repair_participants(ring: &TokenRing, token: i64, rf: usize) -> Vec<u64> {
    let candidates = ring.replicas(token, rf);
    candidates
        .into_iter()
        .filter(|&n| {
            ring.get_node(n).is_some_and(|info| match info.state {
                NodeState::Normal => true,
                NodeState::Learner { owns_tokens } => owns_tokens,
                _ => false,
            })
        })
        .collect()
}

/// Depth of the Merkle tree. 2^DEPTH = number of leaves (sub-ranges).
/// Depth 15 → 32768 leaves, giving fine-grained diffing.
pub const TREE_DEPTH: u32 = 15;

/// How many partitions a single `read_token_range` page returns
/// while streaming-building a Merkle tree. Bounds the peak working
/// set per build to `MERKLE_BUILD_BATCH × max_partition_size`.
///
/// Sized small (16) because partition size on production tables
/// varies by orders of magnitude — the fmem entity_store carries
/// a 768-dim embedding column plus arbitrary text and metadata,
/// so individual partitions range from a few KB to several MB.
/// At BATCH=200 the worst case crossed 1 GiB per build × multiple
/// concurrent builds on the same node and tripped the 2 GiB cgroup
/// limit (kernel oom-kill, docker reported `container oom` →
/// exit 137).
pub const MERKLE_BUILD_BATCH: usize = 16;

/// Cap on simultaneous Merkle builds **per node**. A repair run
/// triggers a build from every direction — the node's own
/// (initiator) `build_merkle` and one per inbound
/// `RepairMerkleRequest` from each peer trying to repair against
/// this replica. Without throttling, a 3-node RF=3 cluster ends
/// up with ~4 concurrent builds on the largest replica, each
/// holding a `MERKLE_BUILD_BATCH` page of decoded partitions.
/// Two at a time is enough to keep the work pipelined without
/// stacking that many in-flight pages.
pub const REPAIR_BUILD_CONCURRENCY: usize = 2;

/// Process-wide semaphore that throttles concurrent
/// `build_tree_for_range` calls. Used by both the local executor
/// (initiator path) and `RepairMerkleHandler` (RPC responder path)
/// so the same budget covers every direction.
pub static REPAIR_BUILD_SEMAPHORE: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(REPAIR_BUILD_CONCURRENCY));

/// Build a Merkle tree for a table's data within a token range by
/// **streaming** the local partitions in fixed-size pages.
///
/// The original implementation called `read_range(None, None,
/// usize::MAX)`, which materialised the entire table into a `Vec`
/// before filtering — observed to OOM the 2 GiB fmem container on
/// a 1.3 GB local replica long before the tree was built. The
/// streaming form walks the table with a token cursor and
/// `read_token_range(cursor, range_end, MERKLE_BUILD_BATCH)`,
/// hashing each batch into the tree and dropping it before the
/// next page. Peak memory is bounded by the batch size regardless
/// of table size.
pub fn build_tree_for_range(
    storage: &StorageEngine,
    table_id: &TableId,
    range_start: i64,
    range_end: i64,
) -> Result<MerkleTree> {
    let mut tree = MerkleTree::new(TREE_DEPTH, range_start, range_end);
    if range_start >= range_end {
        tree.compute_root();
        return Ok(tree);
    }
    storage
        .walk_token_range_for_digest(
            table_id,
            range_start,
            range_end,
            |key, deletion, static_row, emit_rows| {
                // Seed the digest stream with the partition header
                // — token, key bytes, partition deletion, static
                // row. Then fold each clustered row in via the
                // emit_rows continuation. The SSTable reader never
                // materialises a `Partition`; rows are decoded one
                // at a time and dropped as we hash them.
                let mut stream = crate::raft::handlers::PartitionDigestStream::new(
                    key.token.0,
                    key.key.as_bytes(),
                    deletion,
                    static_row,
                )
                .map_err(|e| {
                    ferrosa_common::Error::InvalidData(format!("partition digest init: {e}"))
                })?;
                emit_rows(&mut |row| {
                    stream.update_row(row).map_err(|e| {
                        ferrosa_common::Error::InvalidData(format!("partition digest update: {e}"))
                    })
                })?;
                let hash = stream.finalize().map_err(|e| {
                    ferrosa_common::Error::InvalidData(format!("partition digest finalize: {e}"))
                })? as u64;
                // Widen to 64 bits matching `partition_merkle_hash`.
                let hi = (hash as u32).rotate_left(16) as u64;
                let lo = hash & 0xFFFF_FFFF;
                tree.insert(key.token.0, (hi << 32) | lo);
                Ok(())
            },
        )
        .map_err(crate::error::ClusterError::Storage)?;
    tree.compute_root();
    Ok(tree)
}

/// The reconcile-direction decision for a single partition during anti-entropy
/// repair. Returned alongside the partition in [`RepairPlan`] so the caller
/// knows which side is the canonical "newer" copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairDirection {
    /// Only side A has this partition; B is missing it.
    AOnly,
    /// Only side B has this partition; A is missing it.
    BOnly,
    /// Both sides have it; A's max timestamp is strictly newer than B's.
    ANewer,
    /// Both sides have it; B's max timestamp is strictly newer than A's.
    BNewer,
    /// Both sides have it with the same max timestamp but the content
    /// digest differs. Cassandra-style tie-break is value comparison; we
    /// log + skip so the operator can inspect (matches the
    /// "warn-and-skip on identical-ts conflict" Aphyr-safe stance).
    SkippedTimestampTie,
}

/// Output of [`diff_partition_sets`]: the set of partitions to stream from
/// each side to the other, plus the partitions that were SkippedTimestampTie
/// (returned for observability, not streamed).
#[derive(Debug, Default)]
pub struct RepairPlan {
    /// Partitions to stream A → B (B is missing them or has staler content).
    pub a_to_b: Vec<Partition>,
    /// Partitions to stream B → A (A is missing them or has staler content).
    pub b_to_a: Vec<Partition>,
    /// Partitions holding at least one row that had to be settled by comparing
    /// values at an equal timestamp, rather than by timestamp alone.
    ///
    /// Observability, not an unresolved state: those rows are converged like
    /// any other, using the storage engine's deterministic last-write-wins. A
    /// non-zero count means writes collided at identical timestamps somewhere,
    /// which is worth an operator's attention even though repair settled it.
    pub timestamp_ties: Vec<ferrosa_common::DecoratedKey>,
}

/// What to do with one row that both replicas hold.
#[derive(Debug, PartialEq, Eq)]
enum RowVerdict {
    /// Both copies carry identical content — nothing to do.
    Agree,
    /// Which replicas need the other's copy, and whether settling this row
    /// required comparing values at an equal timestamp rather than deciding it
    /// by timestamp alone.
    Stream {
        to_a: bool,
        to_b: bool,
        tiebreak: bool,
    },
}

/// Newest timestamp anywhere in a single row.
///
/// The partition-wide maximum says nothing about the rows below it, which is
/// what made the partition-level comparison wrong: one recent row made a whole
/// partition look newer and swallowed every row only the other side held.
fn newest_row_timestamp(row: &ferrosa_sstable::types::Row) -> i64 {
    let mut ts = row.primary_key_liveness.timestamp;
    for (_, cell) in &row.cells {
        if cell.timestamp > ts {
            ts = cell.timestamp;
        }
    }
    if row.deletion.marked_for_delete_at > ts {
        ts = row.deletion.marked_for_delete_at;
    }
    ts
}

/// Index a row's cells the way the storage engine does — by `(column, path)`.
///
/// Keying by column alone would treat two elements of a collection as competing
/// writes and collapse the collection, exactly as
/// `ferrosa_storage::merge::merge_rows` documents.
fn cells_by_column(
    row: &ferrosa_sstable::types::Row,
) -> std::collections::BTreeMap<(u16, Option<Vec<u8>>), &ferrosa_common::CellValue> {
    row.cells
        .iter()
        .map(|(col, cell)| ((*col, cell.path.clone()), cell))
        .collect()
}

/// True when `from` carries anything that would change `to` once the engine
/// merges them — a cell `to` lacks, a cell that supersedes `to`'s, or newer
/// row-level state.
///
/// This is what decides whether a row needs to be streamed. Repair does not
/// build the merged row itself: the apply path writes row-by-row through the
/// engine, which merges cell-wise with the same last-write-wins rule. Sending
/// `from`'s copy therefore lands `merge(from, to)` on the receiver, so asking
/// "would this change them?" is exactly the right question — and it answers it
/// without copying either row.
fn contributes_to(from: &ferrosa_sstable::types::Row, to: &ferrosa_sstable::types::Row) -> bool {
    if from.deletion.marked_for_delete_at > to.deletion.marked_for_delete_at
        || from.primary_key_liveness.timestamp > to.primary_key_liveness.timestamp
    {
        return true;
    }
    let to_cells = cells_by_column(to);
    cells_by_column(from).iter().any(|(key, cell)| {
        to_cells
            .get(key)
            .is_none_or(|existing| ferrosa_storage::merge::cell_supersedes(cell, existing))
    })
}

/// True when both rows hold the same cell at the same timestamp with different
/// values — the case where "newest wins" does not decide a winner on its own.
fn has_equal_timestamp_cell_conflict(
    a: &ferrosa_sstable::types::Row,
    b: &ferrosa_sstable::types::Row,
) -> bool {
    let cells_b = cells_by_column(b);
    cells_by_column(a).iter().any(|(key, cell_a)| {
        cells_b.get(key).is_some_and(|cell_b| {
            cell_a.timestamp == cell_b.timestamp && cell_a.value != cell_b.value
        })
    })
}

/// Decide what to do with one row both replicas hold.
///
/// Reconciliation happens per ROW because that is the granularity the data has.
/// With `PRIMARY KEY ((tenant_id, session_id), entity_id)` a single partition
/// holds thousands of unrelated entities, so a verdict reached for the
/// partition is a verdict imposed on rows that had nothing to do with it.
///
/// Conflicts are settled by the storage engine's own cell-level last-write-wins
/// (`ferrosa_storage::merge::cell_supersedes`) rather than being left alone.
/// Because that rule is a total order over
/// `(timestamp, has_value, value_bytes, local_deletion_time)`, both replicas
/// arrive at the same row: each applies the copy it receives against its own,
/// and the merge is commutative. Leaving these rows alone instead left replicas
/// divergent forever while repair reported success.
fn reconcile_row(a: &ferrosa_sstable::types::Row, b: &ferrosa_sstable::types::Row) -> RowVerdict {
    if row_content_hash(a) == row_content_hash(b) {
        return RowVerdict::Agree;
    }
    let tiebreak = newest_row_timestamp(a) == newest_row_timestamp(b)
        && has_equal_timestamp_cell_conflict(a, b);
    RowVerdict::Stream {
        // B's copy changes A → A needs it, and vice versa. When each carries
        // something the other lacks, both are sent and both converge.
        to_a: contributes_to(b, a),
        to_b: contributes_to(a, b),
        tiebreak,
    }
}

/// The rows each replica is missing, and whether any of them needed the
/// The rows each replica is missing, and whether any of them needed the
/// value-comparison tiebreak.
#[derive(Default)]
struct PartitionMerge {
    /// Rows to stream A→B.
    to_b: Vec<ferrosa_sstable::types::Row>,
    /// Rows to stream B→A.
    to_a: Vec<ferrosa_sstable::types::Row>,
    /// At least one row was settled by comparing values at an equal timestamp
    /// rather than by timestamp alone, or partition-level state diverged.
    /// Reported as `timestamp_ties` for observability — the replicas still
    /// converge.
    tiebreak_used: bool,
    /// Partition-level state diverged, so both sides must exchange partitions
    /// even when no rows need to move: [`partition_with_rows`] carries the
    /// sender's deletion and static row, which is the only way that state
    /// travels.
    partition_state_diverged: bool,
}

/// Reconcile two copies of the same partition, row by row.
///
/// Returns only the rows that actually need to move. The apply path writes
/// row-by-row through the engine, so sending a partition trimmed to those rows
/// is both correct and less write amplification than resending all of them.
fn merge_partition_rows(a: &Partition, b: &Partition) -> PartitionMerge {
    let mut merge = PartitionMerge::default();

    // Partition-level state — the partition tombstone and the static row — is
    // not row data, and a per-row merge cannot carry it: a partition deleted at
    // a newer timestamp on one replica has nothing row-shaped to stream. Fall
    // back to whole-partition last-write-wins for these, sending the newer
    // side's partition entire so its deletion and static row travel with it.
    //
    // Found by the repair fuzz harness: reconciling only rows left a partition
    // tombstone stranded, and the replica without it stayed behind the LWW
    // winner forever while repair reported success.
    if a.deletion != b.deletion || a.static_row != b.static_row {
        merge.tiebreak_used = true;
        // Exchange in BOTH directions: each side merges what it receives
        // against its own copy, so both land on the same partition state.
        // Sending only the newer side would leave the sender un-updated and the
        // next pass would stream again — repair has to be idempotent. Rows are
        // still reconciled below; the two are independent.
        merge.partition_state_diverged = true;
    }

    let rows_b: std::collections::BTreeMap<&[u8], &ferrosa_sstable::types::Row> = b
        .rows
        .iter()
        .map(|r| (r.clustering.as_slice(), r))
        .collect();
    let rows_a: std::collections::BTreeMap<&[u8], &ferrosa_sstable::types::Row> = a
        .rows
        .iter()
        .map(|r| (r.clustering.as_slice(), r))
        .collect();

    for (clustering, row_a) in &rows_a {
        match rows_b.get(clustering) {
            // Present only on A — no ambiguity, B needs it.
            None => merge.to_b.push((*row_a).clone()),
            Some(row_b) => match reconcile_row(row_a, row_b) {
                RowVerdict::Agree => {}
                RowVerdict::Stream {
                    to_a,
                    to_b,
                    tiebreak,
                } => {
                    if to_a {
                        merge.to_a.push((*row_b).clone());
                    }
                    if to_b {
                        merge.to_b.push((*row_a).clone());
                    }
                    // The rows differ, yet neither carries anything that would
                    // change the other: row-level state diverged in a way
                    // cell-merge does not reconcile. Surface it rather than
                    // leave a silent divergence.
                    if tiebreak || !(to_a || to_b) {
                        merge.tiebreak_used = true;
                    }
                }
            },
        }
    }
    for (clustering, row_b) in &rows_b {
        if !rows_a.contains_key(clustering) {
            merge.to_a.push((*row_b).clone());
        }
    }
    merge
}

/// A copy of `src` carrying only `rows` — the trimmed partition to stream.
fn partition_with_rows(src: &Partition, rows: Vec<ferrosa_sstable::types::Row>) -> Partition {
    Partition {
        key: src.key.clone(),
        deletion: src.deletion,
        static_row: src.static_row.clone(),
        rows,
    }
}

/// Fold a per-row merge into a [`RepairPlan`].
fn push_merge_into_plan(a: &Partition, b: &Partition, plan: &mut RepairPlan) {
    let merge = merge_partition_rows(a, b);
    let force = merge.partition_state_diverged;
    if force || !merge.to_b.is_empty() {
        plan.a_to_b.push(partition_with_rows(a, merge.to_b));
    }
    if force || !merge.to_a.is_empty() {
        plan.b_to_a.push(partition_with_rows(b, merge.to_a));
    }
    if merge.tiebreak_used {
        plan.timestamp_ties.push(a.key.clone());
    }
}

/// Content hash of a single row, for comparing row sets across replicas.
fn row_content_hash(row: &ferrosa_sstable::types::Row) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.clustering.hash(&mut hasher);
    for (idx, cell) in &row.cells {
        idx.hash(&mut hasher);
        cell.value.hash(&mut hasher);
        cell.timestamp.hash(&mut hasher);
    }
    format!("{:?}", row.deletion).hash(&mut hasher);
    format!("{:?}", row.primary_key_liveness).hash(&mut hasher);
    hasher.finish()
}

/// Diff two partition sets known to cover the same token sub-range (typically
/// the leaves identified as divergent by [`MerkleTree::divergent_leaf_ranges`]).
///
/// For each partition key:
/// - present on only one side → stream that side's copy to the other.
/// - present on both with equal content → no-op.
/// - present on both with different content → reconciled ROW BY ROW, and only
///   the rows that need to move are streamed.
///
/// The granularity matters. Under `PRIMARY KEY ((tenant_id, session_id),
/// entity_id)` one partition holds thousands of unrelated entities, so any
/// verdict reached for a partition is imposed on rows that had nothing to do
/// with it. Deciding per partition went wrong in both directions: comparing
/// partition-wide max timestamps let one recent row swallow every row only the
/// other side held, and treating any contested row as a partition-level
/// conflict stranded all its innocent neighbours.
///
/// Per row: a row only one side holds is streamed. A row both hold is merged
/// with [`ferrosa_storage::merge::merge_rows`] — the same cell-level
/// last-write-wins the storage engine applies on every read and compaction —
/// and the merged row is sent to whichever side does not already hold it.
/// Because that merge is a total order over
/// `(timestamp, has_value, value_bytes, local_deletion_time)`, every replica
/// computes the same winner regardless of which side it calls "local", so the
/// replicas converge instead of oscillating.
///
/// `timestamp_ties` counts rows where the merge had to compare values at an
/// equal timestamp rather than settle it by timestamp alone. That is
/// observability, not a blocker: repair converges those rows too, because
/// leaving them alone left replicas divergent forever while reporting success.
pub fn diff_partition_sets(a: &[Partition], b: &[Partition]) -> RepairPlan {
    use std::collections::HashMap;
    // Key the lookups by raw partition-key bytes — DecoratedKey isn't Hash.
    let a_by_key: HashMap<&[u8], &Partition> =
        a.iter().map(|p| (p.key.key.as_bytes(), p)).collect();
    let b_by_key: HashMap<&[u8], &Partition> =
        b.iter().map(|p| (p.key.key.as_bytes(), p)).collect();
    let mut plan = RepairPlan::default();

    for (key, p_a) in &a_by_key {
        match b_by_key.get(key) {
            None => plan.a_to_b.push((*p_a).clone()),
            Some(p_b) => {
                // Reconcile row by row. Comparing partition-wide maxima decided
                // for every row in the partition at once, which both swallowed
                // the rows only the "older" side held and let one contested row
                // condemn all its neighbours.
                if partition_merkle_hash(p_a) != partition_merkle_hash(p_b) {
                    push_merge_into_plan(p_a, p_b, &mut plan);
                }
                // equal hash: no-op.
            }
        }
    }
    for (key, p_b) in &b_by_key {
        if a_by_key.contains_key(key) {
            continue;
        }
        plan.b_to_a.push((*p_b).clone());
    }
    plan
}

/// One emit event from the streaming diff. The executor pushes
/// these into bounded apply queues so the full diff is never
/// materialised in memory.
pub enum RepairDecision {
    /// Partition exists only in / is newer on side A — stream the
    /// owned partition to side B.
    AToB(Partition),
    /// Partition exists only in / is newer on side B — stream the
    /// owned partition to side A.
    BToA(Partition),
    /// At least one row in this partition was settled by comparing values at an
    /// equal timestamp rather than by timestamp alone. The rows themselves are
    /// converged via the `AToB`/`BToA` decisions; this is emitted purely so the
    /// caller can count how often writes collided.
    Tie(ferrosa_common::DecoratedKey),
}

/// Streaming counterpart of [`diff_partition_sets`]. Consumes both
/// vectors in token order (the chunked Fetch path already returns
/// them sorted) and invokes `emit` with a [`RepairDecision`] per
/// partition. The decision is moved out of the input — neither
/// side's `Vec<Partition>` retains it, so the caller can drop
/// the input vecs as soon as this returns.
///
/// This is the apply-phase counterpart of the build-phase
/// streaming work: `compute_repair_plan` materialises
/// `plan.a_to_b: Vec<Partition>` and `plan.b_to_a:
/// Vec<Partition>` by cloning every divergent partition out of
/// the input; on a dense chunk those clones alone cost
/// `~chunk_size × partition_size` of duplicate allocation,
/// which compounds across the many chunks a session walks. The
/// streaming form emits one decision per partition without
/// cloning — the caller batches decisions into the existing
/// `REPAIR_APPLY_CHUNK_PARTITIONS` queues and flushes via
/// `apply_partitions` when each fills.
///
/// Both inputs MUST be sorted by `(token, key_bytes)` — the
/// chunked Fetch handler sorts before responding so this is
/// satisfied by construction.
pub fn diff_partition_sets_streaming<F>(
    mut a_sorted: Vec<Partition>,
    mut b_sorted: Vec<Partition>,
    mut emit: F,
) -> std::result::Result<(), String>
where
    F: FnMut(RepairDecision) -> std::result::Result<(), String>,
{
    a_sorted.reverse(); // pop() takes from the tail — reverse so pop() yields in token order
    b_sorted.reverse();
    let mut a_head = a_sorted.pop();
    let mut b_head = b_sorted.pop();
    loop {
        match (a_head.take(), b_head.take()) {
            (None, None) => break,
            (Some(a), None) => {
                emit(RepairDecision::AToB(a))?;
                a_head = a_sorted.pop();
            }
            (None, Some(b)) => {
                emit(RepairDecision::BToA(b))?;
                b_head = b_sorted.pop();
            }
            (Some(a), Some(b)) => {
                // Order by (token, key_bytes). Murmur3 collisions on
                // a real partitioner are vanishingly rare, but the
                // tie-break keeps the merge-join correct in pathological
                // inputs.
                let a_cmp = (a.key.token.0, a.key.key.as_bytes());
                let b_cmp = (b.key.token.0, b.key.key.as_bytes());
                match a_cmp.cmp(&b_cmp) {
                    std::cmp::Ordering::Less => {
                        emit(RepairDecision::AToB(a))?;
                        a_head = a_sorted.pop();
                        b_head = Some(b);
                    }
                    std::cmp::Ordering::Greater => {
                        emit(RepairDecision::BToA(b))?;
                        b_head = b_sorted.pop();
                        a_head = Some(a);
                    }
                    std::cmp::Ordering::Equal => {
                        if partition_merkle_hash(&a) != partition_merkle_hash(&b) {
                            let merge = merge_partition_rows(&a, &b);
                            if merge.tiebreak_used {
                                emit(RepairDecision::Tie(a.key.clone()))?;
                            }
                            // Partition-level state travels only on a
                            // partition, so a divergence there forces an
                            // exchange even when no rows need to move.
                            let force = merge.partition_state_diverged;
                            if force || !merge.to_b.is_empty() {
                                emit(RepairDecision::AToB(partition_with_rows(&a, merge.to_b)))?;
                            }
                            if force || !merge.to_a.is_empty() {
                                emit(RepairDecision::BToA(partition_with_rows(&b, merge.to_a)))?;
                            }
                        }
                        a_head = a_sorted.pop();
                        b_head = b_sorted.pop();
                    }
                }
            }
        }
    }
    Ok(())
}

/// Compute a [`RepairPlan`] between two partition sets covering the same
/// `[range_start, range_end)` token range, using a Merkle tree to skip
/// identical sub-ranges before partition-level comparison.
///
/// Mirrors Cassandra's `nodetool repair` shape:
/// 1. Build a Merkle tree on each side.
/// 2. Find the leaves where the trees diverge.
/// 3. For each divergent leaf, filter partitions to that sub-range and
///    diff with [`diff_partition_sets`].
/// 4. Union the per-leaf diffs.
///
/// Pure function — does no I/O. Storage-side callers should first
/// [`StorageEngine::read_range`](ferrosa_storage::engine::StorageEngine::read_range)
/// to materialise the partition slices, then call this.
pub fn compute_repair_plan(
    parts_a: &[Partition],
    parts_b: &[Partition],
    range_start: i64,
    range_end: i64,
) -> RepairPlan {
    let mut tree_a = MerkleTree::new(TREE_DEPTH, range_start, range_end);
    let mut tree_b = MerkleTree::new(TREE_DEPTH, range_start, range_end);
    let in_range = |t: i64| t >= range_start && t < range_end;
    for p in parts_a.iter().filter(|p| in_range(p.key.token.0)) {
        tree_a.insert(p.key.token.0, partition_merkle_hash(p));
    }
    for p in parts_b.iter().filter(|p| in_range(p.key.token.0)) {
        tree_b.insert(p.key.token.0, partition_merkle_hash(p));
    }
    tree_a.compute_root();
    tree_b.compute_root();

    let divergent = tree_a.divergent_leaf_ranges(&tree_b);
    if divergent.is_empty() {
        return RepairPlan::default();
    }

    let mut plan = RepairPlan::default();
    for (sub_start, sub_end) in divergent {
        let a_in_leaf: Vec<Partition> = parts_a
            .iter()
            .filter(|p| p.key.token.0 >= sub_start && p.key.token.0 < sub_end)
            .cloned()
            .collect();
        let b_in_leaf: Vec<Partition> = parts_b
            .iter()
            .filter(|p| p.key.token.0 >= sub_start && p.key.token.0 < sub_end)
            .cloned()
            .collect();
        let sub_plan = diff_partition_sets(&a_in_leaf, &b_in_leaf);
        plan.a_to_b.extend(sub_plan.a_to_b);
        plan.b_to_a.extend(sub_plan.b_to_a);
        plan.timestamp_ties.extend(sub_plan.timestamp_ties);
    }
    plan
}

/// Hash of a partition's full content (key + deletion + static row + clustered
/// rows + every cell with timestamps), suitable for Merkle-tree leaf XOR
/// accumulation in anti-entropy repair.
///
/// **Why partition content, not just the key.** An earlier draft of this code
/// hashed only `partition.key.key.as_bytes()`. That made Merkle comparison
/// blind to content divergence: two replicas with the same set of keys but
/// different cells, timestamps, or deletions produced identical Merkle roots,
/// so the repair scheduler would conclude "nothing to do" and never reconcile
/// them. Repair MUST detect content divergence, so the hash MUST include
/// content.
///
/// Delegates to [`crate::raft::handlers::compute_partition_digest`], which is
/// the same CRC32 over a bincode-serialized `PartitionWire` that the digest
/// phase of `coordinate_read_with` uses, so Merkle agreement and digest-phase
/// agreement are guaranteed consistent. The CRC32 is widened to 64 bits to
/// match the Merkle leaf hash width — XOR collisions are theoretically
/// possible but cap at ~2^-32 per leaf for diverging content (Cassandra's
/// classic merkle accepts the same trade-off).
pub fn partition_merkle_hash(partition: &Partition) -> u64 {
    // Use the streaming digest so the Merkle build doesn't allocate
    // a Vec<u8> the size of each partition's serialised form on the
    // way to a 4-byte CRC. Byte-identical output is pinned by
    // `partition_digest_streaming_matches_legacy`.
    match crate::raft::handlers::compute_partition_digest_streaming(partition) {
        Ok(crc) => {
            // Two-step widen: high half = CRC, low half = CRC rotated.
            // Better than zero-padding because zero-pad leaves the upper
            // 32 bits constant, which throws away half the tree's XOR
            // distinguishing power.
            let lo = crc as u64;
            let hi = (crc.rotate_left(16)) as u64;
            (hi << 32) | lo
        }
        // Serialisation should not fail for any partition the storage engine
        // returned — fall back to 0 (silently identical) rather than panic.
        // Tracked as a metric in the repair coordinator (TODO).
        Err(_) => 0,
    }
}

/// Diff two Merkle trees and return the sub-ranges that differ.
///
/// Returns a list of `(range_start, range_end)` pairs where the two trees
/// have different hashes, indicating partitions that need to be streamed.
pub fn diff_trees(local: &MerkleTree, remote: &MerkleTree) -> Vec<(i64, i64)> {
    assert_eq!(local.depth, remote.depth, "trees must have same depth");
    assert_eq!(
        local.range_start, remote.range_start,
        "trees must cover same range"
    );
    assert_eq!(
        local.range_end, remote.range_end,
        "trees must cover same range"
    );

    let mut diffs = Vec::new();
    diff_subtree(local, remote, 0, &mut diffs);
    diffs
}

/// Recursively walk both trees, collecting differing leaf ranges.
fn diff_subtree(
    local: &MerkleTree,
    remote: &MerkleTree,
    node_idx: usize,
    diffs: &mut Vec<(i64, i64)>,
) {
    if node_idx >= local.nodes.len() || node_idx >= remote.nodes.len() {
        return;
    }

    // If hashes match, entire subtree is identical — skip.
    if local.nodes[node_idx] == remote.nodes[node_idx] {
        return;
    }

    // If this is a leaf node, record the diff range.
    let left_child = 2 * node_idx + 1;
    if left_child >= local.nodes.len() {
        // Leaf — compute the token range for this leaf.
        let num_leaves = 1usize << local.depth;
        let first_leaf = num_leaves - 1;
        let leaf_idx = node_idx - first_leaf;
        // Use unsigned arithmetic to avoid overflow on i64::MIN..i64::MAX.
        let rs = (local.range_start as i128 + i64::MAX as i128 + 1) as u128;
        let re = (local.range_end as i128 + i64::MAX as i128 + 1) as u128;
        let range_width = re.wrapping_sub(rs) / num_leaves as u128;
        let start_u = rs + leaf_idx as u128 * range_width;
        let end_u = if leaf_idx + 1 == num_leaves {
            re
        } else {
            rs + (leaf_idx as u128 + 1) * range_width
        };
        let bias = i64::MAX as i128 + 1;
        let start = (start_u as i128 - bias) as i64;
        let end = (end_u as i128 - bias) as i64;
        diffs.push((start, end));
        return;
    }

    // Internal node — recurse into children.
    diff_subtree(local, remote, left_child, diffs);
    diff_subtree(local, remote, left_child + 1, diffs);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal Partition for tests with a given partition-key bytes
    /// and a single cell whose value is `value` and whose timestamp is `ts`.
    /// The partition's token is computed from the key via Murmur3 so the
    /// token-to-leaf mapping in Merkle inserts is realistic.
    fn test_partition(key_bytes: &[u8], value: &[u8], ts: i64) -> Partition {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let key = PartitionKey::new(key_bytes.to_vec());
        let dk = DecoratedKey::new(key);
        let cell = CellValue::live(value.to_vec(), ts);
        let row = Row {
            clustering: vec![],
            cells: vec![(0, cell)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        }
    }

    /// Build a test partition with an explicit token, bypassing Murmur3.
    /// Useful when a test needs deterministic token placement.
    fn test_partition_at(token: i64, key_bytes: &[u8], value: &[u8], ts: i64) -> Partition {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let key = PartitionKey::new(key_bytes.to_vec());
        let dk = DecoratedKey {
            token: Token(token),
            key,
        };
        let cell = CellValue::live(value.to_vec(), ts);
        let row = Row {
            clustering: vec![],
            cells: vec![(0, cell)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        }
    }

    #[test]
    fn identical_trees_produce_no_diffs() {
        let mut t1 = MerkleTree::new(4, i64::MIN, i64::MAX);
        let mut t2 = MerkleTree::new(4, i64::MIN, i64::MAX);

        for i in 0..4u8 {
            let p = test_partition(&[b'k', i], b"value", 1000);
            let token = p.key.token.0;
            let hash = partition_merkle_hash(&p);
            t1.insert(token, hash);
            t2.insert(token, hash);
        }

        t1.compute_root();
        t2.compute_root();

        let diffs = diff_trees(&t1, &t2);
        assert!(diffs.is_empty(), "identical trees should have no diffs");
    }

    /// REGRESSION: prior to this fix, `partition_hash` hashed only the
    /// partition key bytes, so two replicas holding the same key with
    /// different cells produced identical Merkle hashes — repair was
    /// blind to content divergence (the exact failure mode anti-entropy
    /// is supposed to fix).
    #[test]
    fn partition_merkle_hash_detects_content_divergence() {
        let same_key = b"shared-pkey";
        let local = test_partition(same_key, b"value-a", 1_000);
        let remote = test_partition(same_key, b"value-b", 1_000);
        assert_ne!(
            partition_merkle_hash(&local),
            partition_merkle_hash(&remote),
            "two partitions with the same key but different cell values MUST hash differently"
        );

        // And the same hash for identical content (sanity check).
        let dup = test_partition(same_key, b"value-a", 1_000);
        assert_eq!(
            partition_merkle_hash(&local),
            partition_merkle_hash(&dup),
            "identical content must hash identically (deterministic across nodes)"
        );
    }

    // ---- per-row reconciliation ----
    //
    // These pin the granularity of the diff. `PRIMARY KEY ((tenant_id,
    // session_id), entity_id)` puts thousands of unrelated entities in ONE
    // partition, so any decision taken at partition granularity is taken for
    // rows that had nothing to do with it.

    /// Build a row with a single cell at column 0.
    fn row_at(clustering: &[u8], value: &[u8], ts: i64) -> ferrosa_sstable::types::Row {
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Row {
            clustering: clustering.to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    /// A row marker carrying no cells at all — what node1 actually held for the
    /// rows this bug stranded.
    fn empty_shell(clustering: &[u8], ts: i64) -> ferrosa_sstable::types::Row {
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Row {
            clustering: clustering.to_vec(),
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn multi_row_partition(key: &[u8], rows: Vec<ferrosa_sstable::types::Row>) -> Partition {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::DeletionTime;
        Partition {
            key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    /// The clustering keys a plan would stream, per direction.
    fn streamed(partitions: &[Partition]) -> std::collections::BTreeSet<Vec<u8>> {
        partitions
            .iter()
            .flat_map(|p| p.rows.iter().map(|r| r.clustering.clone()))
            .collect()
    }

    /// REGRESSION: one contested row must not strand every other row sharing
    /// its partition.
    ///
    /// Measured on the host cluster: partition (tenant 9a5f8fbf, session
    /// 32ee0a9d) held 4 rows on node1 against 7 on node2/node3 — 3 absent
    /// outright, 4 present as empty shells while node2 held them fully
    /// populated. Repair streamed nothing and reported `timestamp_ties: 1`,
    /// because classification ran per PARTITION: any single row that differed
    /// on both sides condemned the whole partition.
    #[test]
    fn a_contested_row_does_not_block_the_rest_of_its_partition() {
        let a = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"agreed", b"same", 900),
                row_at(b"tied", b"a-version", 500),
            ],
        )];
        let b = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"agreed", b"same", 900),
                row_at(b"tied", b"b-version", 500),
                row_at(b"only-b", b"new-row", 400),
            ],
        )];

        let plan = diff_partition_sets(&a, &b);
        let to_a = streamed(&plan.b_to_a);

        assert!(
            to_a.contains(b"only-b".as_slice()),
            "a row absent from A must be streamed even though another row in \
             the same partition is contested: {to_a:?}"
        );
        assert!(
            to_a.contains(b"tied".as_slice()),
            "the contested row now converges by deterministic LWW: {to_a:?}"
        );
        assert!(
            !to_a.contains(b"agreed".as_slice()),
            "a row both sides already agree on must not be streamed: {to_a:?}"
        );
        assert_eq!(
            plan.timestamp_ties.len(),
            1,
            "the genuine conflict must still be surfaced"
        );
    }

    /// Both replicas must land on the SAME row after a tie, or repair oscillates.
    ///
    /// Leaving equal-timestamp conflicts alone left replicas divergent forever
    /// while repair reported success. Converging them is only safe if every node
    /// picks the same winner, so the merge must not depend on which side is
    /// "local" -- otherwise each node keeps its own copy and repair never
    /// converges no matter how often it runs.
    #[test]
    fn an_equal_timestamp_conflict_converges_both_replicas_on_one_row() {
        let a = vec![multi_row_partition(b"k1", vec![row_at(b"c", b"left", 500)])];
        let b = vec![multi_row_partition(
            b"k1",
            vec![row_at(b"c", b"right", 500)],
        )];

        let plan = diff_partition_sets(&a, &b);

        let to_a: Vec<_> = plan.b_to_a.iter().flat_map(|p| p.rows.iter()).collect();
        let to_b: Vec<_> = plan.a_to_b.iter().flat_map(|p| p.rows.iter()).collect();

        // Exactly one side is short: the winner already sits on the other, and
        // re-sending it there would be pure write amplification.
        assert_eq!(
            to_a.len() + to_b.len(),
            1,
            "only the losing replica needs the row"
        );
        let (streamed, already_held) = if to_a.len() == 1 {
            (to_a[0], &b[0].rows[0])
        } else {
            (to_b[0], &a[0].rows[0])
        };
        assert_eq!(
            row_content_hash(streamed),
            row_content_hash(already_held),
            "the streamed row must equal what the other replica already holds — \
             that is what convergence means"
        );
        assert_eq!(plan.timestamp_ties.len(), 1, "the tie is still counted");
    }

    /// The deterministic winner must not depend on argument order.
    ///
    /// Each node runs the diff with ITSELF as one side. If the merge favoured
    /// the left argument, every node would pick its own copy and repair would
    /// never converge.
    #[test]
    fn the_equal_timestamp_winner_is_the_same_from_either_side() {
        let left = multi_row_partition(b"k1", vec![row_at(b"c", b"left", 500)]);
        let right = multi_row_partition(b"k1", vec![row_at(b"c", b"right", 500)]);

        let forward =
            diff_partition_sets(std::slice::from_ref(&left), std::slice::from_ref(&right));
        let reverse =
            diff_partition_sets(std::slice::from_ref(&right), std::slice::from_ref(&left));

        let winner = |plan: &RepairPlan| -> u64 {
            let row = plan
                .a_to_b
                .iter()
                .chain(plan.b_to_a.iter())
                .flat_map(|p| p.rows.iter())
                .next()
                .expect("a tie must produce a winning row");
            row_content_hash(row)
        };
        assert_eq!(
            winner(&forward),
            winner(&reverse),
            "the winner must be independent of which replica ran the diff"
        );
    }

    /// REGRESSION: a row that is an empty shell on one side is not a conflict.
    ///
    /// An empty row marker holds no cell values, so taking the populated copy
    /// cannot discard anyone's write — there is nothing on the empty side to
    /// discard. Calling it a conflict leaves the replica permanently short.
    #[test]
    fn an_empty_shell_loses_to_the_populated_copy_at_the_same_timestamp() {
        let a = vec![multi_row_partition(b"k1", vec![empty_shell(b"e1", 700)])];
        let b = vec![multi_row_partition(
            b"k1",
            vec![row_at(b"e1", b"real", 700)],
        )];

        let plan = diff_partition_sets(&a, &b);

        assert!(
            streamed(&plan.b_to_a).contains(b"e1".as_slice()),
            "the populated copy must be streamed over the empty shell"
        );
        assert!(
            plan.timestamp_ties.is_empty(),
            "an empty shell versus a populated row is not a conflict: {:?}",
            plan.timestamp_ties
        );
    }

    /// REGRESSION: a newer row elsewhere in the partition must not swallow the
    /// rows only the older side holds.
    ///
    /// The diff compared `newest_partition_timestamp`, a partition-wide
    /// maximum. One recent row on A made the whole partition "newer", so A
    /// streamed to B and B's unique rows were never sent back — repair
    /// reported success while A stayed short.
    #[test]
    fn rows_unique_to_the_older_side_survive_a_newer_partition_max() {
        let a = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"shared", b"v", 100),
                row_at(b"only-a", b"recent", 900),
            ],
        )];
        let b = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"shared", b"v", 100),
                row_at(b"only-b", b"older", 500),
            ],
        )];

        let plan = diff_partition_sets(&a, &b);

        assert!(
            streamed(&plan.a_to_b).contains(b"only-a".as_slice()),
            "A's unique row must reach B"
        );
        assert!(
            streamed(&plan.b_to_a).contains(b"only-b".as_slice()),
            "B's unique row must reach A even though A holds a newer row \
             elsewhere in the same partition"
        );
        assert!(plan.timestamp_ties.is_empty(), "neither row is contested");
    }

    /// The production repair path is the STREAMING diff — `executor.rs` calls
    /// `diff_partition_sets_streaming`, never `diff_partition_sets`. A fix that
    /// lands only in the one-shot form changes nothing on a live cluster.
    #[test]
    fn streaming_diff_resolves_per_row_like_the_one_shot_form() {
        let a = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"agreed", b"same", 900),
                row_at(b"tied", b"a-version", 500),
                empty_shell(b"shell", 700),
            ],
        )];
        let b = vec![multi_row_partition(
            b"k1",
            vec![
                row_at(b"agreed", b"same", 900),
                row_at(b"tied", b"b-version", 500),
                row_at(b"shell", b"real", 700),
                row_at(b"only-b", b"new-row", 400),
            ],
        )];

        let mut stream_to_a: std::collections::BTreeSet<Vec<u8>> = Default::default();
        let mut stream_to_b: std::collections::BTreeSet<Vec<u8>> = Default::default();
        let mut ties = 0usize;
        diff_partition_sets_streaming(a.clone(), b.clone(), |decision| {
            match decision {
                RepairDecision::AToB(p) => {
                    stream_to_b.extend(p.rows.iter().map(|r| r.clustering.clone()));
                }
                RepairDecision::BToA(p) => {
                    stream_to_a.extend(p.rows.iter().map(|r| r.clustering.clone()));
                }
                RepairDecision::Tie(_) => ties += 1,
            }
            Ok(())
        })
        .expect("streaming diff should not fail");

        let plan = diff_partition_sets(&a, &b);
        assert_eq!(
            stream_to_a,
            streamed(&plan.b_to_a),
            "streaming and one-shot must agree on the rows sent to A"
        );
        assert_eq!(
            stream_to_b,
            streamed(&plan.a_to_b),
            "streaming and one-shot must agree on the rows sent to B"
        );
        assert_eq!(ties, plan.timestamp_ties.len(), "and on the tie count");
        assert!(
            stream_to_a.contains(b"only-b".as_slice()) && stream_to_a.contains(b"shell".as_slice()),
            "the production path must repair the missing and shell rows: {stream_to_a:?}"
        );
        assert_eq!(ties, 1, "only the contested row is counted as a tie");
    }

    // ---- RepairSession::diff_partition_sets contracts ----

    #[test]
    fn diff_partition_sets_empty_inputs_produce_empty_plan() {
        let plan = diff_partition_sets(&[], &[]);
        assert!(plan.a_to_b.is_empty());
        assert!(plan.b_to_a.is_empty());
        assert!(plan.timestamp_ties.is_empty());
    }

    #[test]
    fn diff_partition_sets_identical_inputs_produce_empty_plan() {
        let a = vec![
            test_partition(b"k1", b"v1", 100),
            test_partition(b"k2", b"v2", 200),
        ];
        let b = a.clone();
        let plan = diff_partition_sets(&a, &b);
        assert!(plan.a_to_b.is_empty());
        assert!(plan.b_to_a.is_empty());
        assert!(plan.timestamp_ties.is_empty());
    }

    #[test]
    fn diff_partition_sets_missing_on_b_streams_a_to_b() {
        let a = vec![
            test_partition(b"k1", b"v1", 100),
            test_partition(b"k2", b"v2", 200),
        ];
        let b = vec![test_partition(b"k1", b"v1", 100)];
        let plan = diff_partition_sets(&a, &b);
        assert_eq!(plan.a_to_b.len(), 1, "k2 must be sent A→B");
        assert_eq!(plan.a_to_b[0].key.key.as_bytes(), b"k2");
        assert!(plan.b_to_a.is_empty());
    }

    #[test]
    fn diff_partition_sets_missing_on_a_streams_b_to_a() {
        let a = vec![test_partition(b"k1", b"v1", 100)];
        let b = vec![
            test_partition(b"k1", b"v1", 100),
            test_partition(b"k2", b"v2", 200),
        ];
        let plan = diff_partition_sets(&a, &b);
        assert!(plan.a_to_b.is_empty());
        assert_eq!(plan.b_to_a.len(), 1);
        assert_eq!(plan.b_to_a[0].key.key.as_bytes(), b"k2");
    }

    #[test]
    fn diff_partition_sets_newer_timestamp_wins() {
        let a = vec![test_partition(b"k1", b"old", 100)];
        let b = vec![test_partition(b"k1", b"new", 200)];
        let plan = diff_partition_sets(&a, &b);
        assert!(plan.a_to_b.is_empty(), "A is older, should not be sent");
        assert_eq!(plan.b_to_a.len(), 1, "B is newer, must be sent");
        assert_eq!(
            plan.b_to_a[0].rows[0].cells[0].1.value,
            Some(b"new".to_vec())
        );

        // Symmetric: same data, swap sides.
        let plan = diff_partition_sets(&b, &a);
        assert_eq!(plan.a_to_b.len(), 1, "now A (formerly B) is newer");
        assert_eq!(
            plan.a_to_b[0].rows[0].cells[0].1.value,
            Some(b"new".to_vec())
        );
        assert!(plan.b_to_a.is_empty());
    }

    /// A partition where one replica simply LACKS rows must be repaired, not
    /// written off as an unresolvable tie.
    ///
    /// Ties are decided on the partition's MAX timestamp. That is too coarse: a
    /// replica can be missing whole rows while its newest surviving cell shares
    /// a timestamp with the other side, so the partition is classified as a tie
    /// and nothing is streamed. The replica then stays short forever while
    /// repair reports success.
    ///
    /// There is no ambiguity to protect here. Every row the thin side holds is
    /// present and identical on the fat side, so the fat side is a strict
    /// superset — the union is the only sensible answer, and picking it does not
    /// require guessing at a last-write-wins order.
    ///
    /// Observed on the host cluster 2026-08-17: node1 held 78,494 distinct
    /// entity ids to node2/node3's 78,499, strictly missing 5 with none unique
    /// to itself, while repair from all three coordinators reported
    /// `partitions_streamed_in: 0, out: 0` and `timestamp_ties: 7`.
    #[test]
    fn a_strict_subset_partition_is_repaired_rather_than_called_a_tie() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        fn row_at(clustering: &[u8], value: &[u8], ts: i64) -> Row {
            Row {
                clustering: clustering.to_vec(),
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }
        }
        fn partition_of(rows: Vec<Row>) -> Partition {
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"k1".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows,
            }
        }

        // Both sides agree on r1 at ts=500 — so both partitions report a max
        // timestamp of 500. The fat side additionally holds r2, written EARLIER,
        // which is exactly the shape that hides behind a max-timestamp compare.
        let thin = vec![partition_of(vec![row_at(b"r1", b"same", 500)])];
        let fat = vec![partition_of(vec![
            row_at(b"r1", b"same", 500),
            row_at(b"r2", b"only-on-fat", 300),
        ])];

        let plan = diff_partition_sets(&thin, &fat);

        assert!(
            plan.timestamp_ties.is_empty(),
            "a strict subset is a missing row, not a conflict: {:?}",
            plan.timestamp_ties
        );
        assert_eq!(
            plan.b_to_a.len(),
            1,
            "the side holding the extra row must be streamed to the side missing it"
        );
        assert!(
            plan.a_to_b.is_empty(),
            "the thin side has nothing the fat side lacks"
        );

        // Symmetric: swapping the arguments must reach the same conclusion in
        // the other direction, or repair depends on which node initiated it.
        let plan = diff_partition_sets(&fat, &thin);
        assert!(plan.timestamp_ties.is_empty());
        assert_eq!(plan.a_to_b.len(), 1);
        assert!(plan.b_to_a.is_empty());
    }

    /// Both replicas hold rows the other lacks, and everything shared agrees.
    /// Streaming both ways converges both on the union; calling it a tie leaves
    /// both permanently short of different rows.
    #[test]
    fn each_side_missing_different_rows_converges_on_the_union() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let row = |clustering: &[u8], value: &[u8], ts: i64| Row {
            clustering: clustering.to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        let part = |rows: Vec<Row>| Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k1".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        };
        let a = vec![part(vec![
            row(b"shared", b"same", 500),
            row(b"only-a", b"a", 200),
        ])];
        let b = vec![part(vec![
            row(b"shared", b"same", 500),
            row(b"only-b", b"b", 200),
        ])];

        let plan = diff_partition_sets(&a, &b);
        assert!(
            plan.timestamp_ties.is_empty(),
            "disjoint extras are not a conflict"
        );
        assert_eq!(plan.a_to_b.len(), 1);
        assert_eq!(plan.b_to_a.len(), 1);
    }

    /// A row present on both sides at the same timestamp with DIFFERENT content
    /// is a real conflict and must still be surfaced, not silently merged.
    /// This is the case the tie was introduced for, and it must survive.
    #[test]
    fn a_conflicting_row_at_the_same_timestamp_is_still_a_tie() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let row = |value: &[u8], ts: i64| Row {
            clustering: b"same-row".to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        let part = |rows: Vec<Row>| Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k1".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        };
        // Same clustering, same timestamp, different value: the timestamp alone
        // does not pick a winner.
        let a = vec![part(vec![row(b"left", 500)])];
        let b = vec![part(vec![row(b"right", 500)])];

        let plan = diff_partition_sets(&a, &b);
        assert_eq!(
            plan.timestamp_ties.len(),
            1,
            "a genuine conflict must surface"
        );
        // Superseded semantics: this used to assert the row was streamed
        // NOWHERE, which left the replicas divergent forever while repair
        // reported success. Repair now settles it with the same deterministic
        // last-write-wins the storage engine applies on every read, so the
        // replicas converge and the counter becomes observability rather than
        // an unresolved state.
        let streamed = plan.a_to_b.len() + plan.b_to_a.len();
        assert_eq!(streamed, 1, "the losing replica must receive the winner");
    }

    /// A difference in partition-level state is not something a row-set union
    /// can reconcile, so it stays a tie even when the row sets nest cleanly.
    #[test]
    fn differing_partition_deletion_is_not_resolved_by_a_row_union() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let row = |ts: i64| Row {
            clustering: b"r".to_vec(),
            cells: vec![(0, CellValue::live(b"v".to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        let key = DecoratedKey::new(PartitionKey::new(b"k1".to_vec()));
        let a = vec![Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row(500)],
        }];
        let b = vec![Partition {
            key,
            deletion: DeletionTime {
                marked_for_delete_at: 500,
                local_deletion_time: 1,
            },
            static_row: None,
            rows: vec![row(500)],
        }];

        let plan = diff_partition_sets(&a, &b);
        assert_eq!(
            plan.timestamp_ties.len(),
            1,
            "a partition-level deletion difference must not be papered over"
        );
    }

    #[test]
    fn diff_partition_sets_equal_timestamp_different_content_converges_and_is_counted() {
        // Semantics changed deliberately: repair now converges equal-timestamp
        // cell conflicts by the same deterministic last-write-wins rule the
        // storage engine already applies, rather than leaving the replicas
        // divergent forever. The tie is still COUNTED, so the operator keeps the
        // signal without paying permanent divergence for it.
        let a = vec![test_partition(b"k1", b"left", 500)];
        let b = vec![test_partition(b"k1", b"right", 500)];
        let plan = diff_partition_sets(&a, &b);
        assert_eq!(plan.timestamp_ties.len(), 1, "the tie is still reported");
        assert_eq!(plan.timestamp_ties[0].key.as_bytes(), b"k1");
        assert!(
            !plan.a_to_b.is_empty() || !plan.b_to_a.is_empty(),
            "a tie must now converge the replicas instead of being a no-op"
        );
    }

    #[test]
    fn diff_partition_sets_mixed_scenario() {
        // k1 only on A → A→B
        // k2 newer on A → A→B
        // k3 newer on B → B→A
        // k4 only on B → B→A
        // k5 equal everything → no-op
        let a = vec![
            test_partition(b"k1", b"a1", 100),
            test_partition(b"k2", b"new", 500),
            test_partition(b"k3", b"old", 100),
            test_partition(b"k5", b"same", 999),
        ];
        let b = vec![
            test_partition(b"k2", b"old", 100),
            test_partition(b"k3", b"new", 500),
            test_partition(b"k4", b"b4", 100),
            test_partition(b"k5", b"same", 999),
        ];
        let plan = diff_partition_sets(&a, &b);
        let keys_a_to_b: std::collections::BTreeSet<&[u8]> =
            plan.a_to_b.iter().map(|p| p.key.key.as_bytes()).collect();
        let keys_b_to_a: std::collections::BTreeSet<&[u8]> =
            plan.b_to_a.iter().map(|p| p.key.key.as_bytes()).collect();
        assert_eq!(keys_a_to_b, [&b"k1"[..], &b"k2"[..]].into_iter().collect());
        assert_eq!(keys_b_to_a, [&b"k3"[..], &b"k4"[..]].into_iter().collect());
        assert!(plan.timestamp_ties.is_empty());
    }

    /// `diff_partition_sets_streaming` emits the same decisions
    /// as `diff_partition_sets` produces in its plan — same
    /// keys in a_to_b / b_to_a, same ties — but consumes the
    /// inputs by value instead of cloning each chosen partition
    /// into a plan vector. The executor uses it to drive a
    /// bounded apply queue without holding the full per-chunk
    /// diff in memory.
    #[test]
    fn diff_partition_sets_streaming_matches_one_shot_diff() {
        // Same mixed scenario as `diff_partition_sets_mixed_scenario`.
        let a = vec![
            test_partition(b"k1", b"a1", 100),
            test_partition(b"k2", b"new", 500),
            test_partition(b"k3", b"old", 100),
            test_partition(b"k5", b"same", 999),
        ];
        let b = vec![
            test_partition(b"k2", b"old", 100),
            test_partition(b"k3", b"new", 500),
            test_partition(b"k4", b"b4", 100),
            test_partition(b"k5", b"same", 999),
        ];

        // Both sides must be sorted by `(token, key_bytes)` per
        // the streaming-diff contract.
        let mut a_sorted = a.clone();
        a_sorted.sort_by(|p, q| {
            (p.key.token.0, p.key.key.as_bytes()).cmp(&(q.key.token.0, q.key.key.as_bytes()))
        });
        let mut b_sorted = b.clone();
        b_sorted.sort_by(|p, q| {
            (p.key.token.0, p.key.key.as_bytes()).cmp(&(q.key.token.0, q.key.key.as_bytes()))
        });

        let plan_one_shot = diff_partition_sets(&a, &b);
        let one_shot_a_to_b: std::collections::BTreeSet<Vec<u8>> = plan_one_shot
            .a_to_b
            .iter()
            .map(|p| p.key.key.as_bytes().to_vec())
            .collect();
        let one_shot_b_to_a: std::collections::BTreeSet<Vec<u8>> = plan_one_shot
            .b_to_a
            .iter()
            .map(|p| p.key.key.as_bytes().to_vec())
            .collect();
        let one_shot_ties: std::collections::BTreeSet<Vec<u8>> = plan_one_shot
            .timestamp_ties
            .iter()
            .map(|k| k.key.as_bytes().to_vec())
            .collect();

        let mut stream_a_to_b: std::collections::BTreeSet<Vec<u8>> = Default::default();
        let mut stream_b_to_a: std::collections::BTreeSet<Vec<u8>> = Default::default();
        let mut stream_ties: std::collections::BTreeSet<Vec<u8>> = Default::default();
        diff_partition_sets_streaming(a_sorted, b_sorted, |decision| {
            match decision {
                RepairDecision::AToB(p) => {
                    stream_a_to_b.insert(p.key.key.as_bytes().to_vec());
                }
                RepairDecision::BToA(p) => {
                    stream_b_to_a.insert(p.key.key.as_bytes().to_vec());
                }
                RepairDecision::Tie(k) => {
                    stream_ties.insert(k.key.as_bytes().to_vec());
                }
            }
            Ok(())
        })
        .expect("streaming diff must succeed on pre-sorted inputs");

        assert_eq!(stream_a_to_b, one_shot_a_to_b, "A→B decisions match");
        assert_eq!(stream_b_to_a, one_shot_b_to_a, "B→A decisions match");
        assert_eq!(stream_ties, one_shot_ties, "tie decisions match");
    }

    // ---- compute_repair_plan: Merkle-narrowed end-to-end diff ----

    #[test]
    fn compute_repair_plan_identical_sides_produces_empty_plan() {
        let parts = vec![
            test_partition(b"k1", b"v1", 100),
            test_partition(b"k2", b"v2", 200),
            test_partition(b"k3", b"v3", 300),
        ];
        let plan = compute_repair_plan(&parts, &parts, i64::MIN, i64::MAX);
        assert!(plan.a_to_b.is_empty());
        assert!(plan.b_to_a.is_empty());
        assert!(plan.timestamp_ties.is_empty());
    }

    #[test]
    fn compute_repair_plan_finds_divergence_across_multiple_leaves() {
        // Pick keys that hash to different Murmur3 tokens, ensuring they
        // land in different Merkle leaves (we have TREE_DEPTH=15, so
        // basically every distinct key goes to a different leaf).
        // Scenario:
        //   - k_only_a: only on side A → A→B
        //   - k_diff:   on both, A newer → A→B
        //   - k_only_b: only on side B → B→A
        //   - k_same:   identical on both → no-op
        let parts_a = vec![
            test_partition(b"k_only_a", b"a", 100),
            test_partition(b"k_diff", b"new", 500),
            test_partition(b"k_same", b"same", 1_000),
        ];
        let parts_b = vec![
            test_partition(b"k_diff", b"old", 100),
            test_partition(b"k_only_b", b"b", 100),
            test_partition(b"k_same", b"same", 1_000),
        ];
        let plan = compute_repair_plan(&parts_a, &parts_b, i64::MIN, i64::MAX);

        let to_b: std::collections::BTreeSet<&[u8]> =
            plan.a_to_b.iter().map(|p| p.key.key.as_bytes()).collect();
        let to_a: std::collections::BTreeSet<&[u8]> =
            plan.b_to_a.iter().map(|p| p.key.key.as_bytes()).collect();
        assert_eq!(
            to_b,
            [&b"k_only_a"[..], &b"k_diff"[..]].into_iter().collect()
        );
        assert_eq!(to_a, [&b"k_only_b"[..]].into_iter().collect());
        assert!(plan.timestamp_ties.is_empty());
    }

    #[test]
    fn compute_repair_plan_skips_identical_subranges() {
        // Same set of partitions on both sides, except one key on side A
        // has a newer cell. Merkle narrows to that leaf; the diff returns
        // only that one partition.
        let common: Vec<_> = (0..50u8)
            .map(|i| test_partition(&[b'c', i], b"shared", 1_000))
            .collect();
        let mut parts_a = common.clone();
        parts_a.push(test_partition(b"x", b"a", 5_000));
        let mut parts_b = common.clone();
        parts_b.push(test_partition(b"x", b"b", 4_000));
        let plan = compute_repair_plan(&parts_a, &parts_b, i64::MIN, i64::MAX);

        assert_eq!(plan.a_to_b.len(), 1, "only key 'x' should diverge");
        assert_eq!(plan.a_to_b[0].key.key.as_bytes(), b"x");
        assert!(plan.b_to_a.is_empty());
    }

    #[test]
    fn compute_repair_plan_filters_to_range() {
        // Two partitions per side, both with same partition-key bytes but
        // explicit tokens placed so the range bounds isolate one of them.
        // Using deterministic tokens keeps the test independent of Murmur3
        // hash distribution. Range is wide enough that TREE_DEPTH=15 yields
        // non-degenerate leaves (>= 32 768 tokens wide).
        let parts_a = vec![
            test_partition_at(1_000_000, b"in", b"a-new", 500),
            test_partition_at(9_000_000, b"out", b"a-new", 500),
        ];
        let parts_b = vec![
            test_partition_at(1_000_000, b"in", b"b-old", 100),
            test_partition_at(9_000_000, b"out", b"b-old", 100),
        ];
        let plan = compute_repair_plan(&parts_a, &parts_b, 0, 2_000_000);
        let to_b: Vec<&[u8]> = plan.a_to_b.iter().map(|p| p.key.key.as_bytes()).collect();
        assert_eq!(
            to_b,
            vec![&b"in"[..]],
            "only the in-range partition should appear in the plan"
        );
    }

    /// REGRESSION: the same content divergence must propagate up the
    /// Merkle tree so `diff_trees` reports the leaf containing the
    /// diverging partition.
    #[test]
    fn diff_trees_detects_content_divergence_at_same_key() {
        let same_key = b"shared-pkey";
        let local = test_partition(same_key, b"value-a", 1_000);
        let remote = test_partition(same_key, b"value-b", 1_000);
        let token = local.key.token.0;

        let mut t1 = MerkleTree::new(8, i64::MIN, i64::MAX);
        let mut t2 = MerkleTree::new(8, i64::MIN, i64::MAX);
        t1.insert(token, partition_merkle_hash(&local));
        t2.insert(token, partition_merkle_hash(&remote));
        t1.compute_root();
        t2.compute_root();

        let diffs = diff_trees(&t1, &t2);
        assert!(
            !diffs.is_empty(),
            "diff_trees must detect partition-content divergence"
        );
        assert!(
            diffs.iter().any(|&(s, e)| s <= token && token < e),
            "diff range must contain the divergent token; got {diffs:?} token={token}"
        );
    }

    #[test]
    fn different_trees_produce_diffs() {
        let mut t1 = MerkleTree::new(4, 0, 1000);
        let mut t2 = MerkleTree::new(4, 0, 1000);

        // Both have token 100.
        t1.insert(100, 0xAAAA);
        t2.insert(100, 0xAAAA);

        // Only t1 has token 500.
        t1.insert(500, 0xBBBB);

        t1.compute_root();
        t2.compute_root();

        let diffs = diff_trees(&t1, &t2);
        assert!(!diffs.is_empty(), "different trees should have diffs");
        // The diff should include a range containing token 500.
        let has_500 = diffs.iter().any(|&(start, end)| start <= 500 && 500 < end);
        assert!(has_500, "diff should include range containing token 500");
    }

    #[test]
    fn empty_trees_are_identical() {
        let mut t1 = MerkleTree::new(3, 0, 100);
        let mut t2 = MerkleTree::new(3, 0, 100);
        t1.compute_root();
        t2.compute_root();

        let diffs = diff_trees(&t1, &t2);
        assert!(diffs.is_empty());
    }

    // -----------------------------------------------------------------
    // W8.7 — repair includes owns_tokens=true learner; excludes
    // owns_tokens=false learner.
    // -----------------------------------------------------------------

    use crate::raft::NodeInfo;
    use uuid::Uuid;

    fn node_with(addr: &str, state: crate::raft::NodeState) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state,
            cql_broadcast: None,
        }
    }

    /// W8.7 RED. A learner with `owns_tokens=true` is included in the
    /// repair-participant set for a token it owns; a learner with
    /// `owns_tokens=false` is excluded; voters are always included.
    /// The learner's data converges via repair into agreement with
    /// the voters.
    #[test]
    fn learner_with_owns_tokens_true_participates_in_repair() {
        let mut ring = TokenRing::new();
        ring.add_node(
            1,
            node_with("10.0.0.1:7000", crate::raft::NodeState::Normal),
        );
        ring.add_node(
            2,
            node_with("10.0.0.2:7000", crate::raft::NodeState::Normal),
        );
        ring.add_node(
            3,
            node_with(
                "10.0.0.3:7000",
                crate::raft::NodeState::Learner { owns_tokens: true },
            ),
        );
        ring.add_node(
            4,
            node_with(
                "10.0.0.4:7000",
                crate::raft::NodeState::Learner { owns_tokens: false },
            ),
        );
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);
        ring.assign_tokens(3, &[150]);
        ring.assign_tokens(4, &[175]);

        let participants = super::repair_participants(&ring, 50, 4);
        assert!(
            participants.contains(&3),
            "owns_tokens=true learner must participate in repair: {participants:?}",
        );
        assert!(
            !participants.contains(&4),
            "owns_tokens=false learner must NOT participate in repair: {participants:?}",
        );
        assert!(participants.contains(&1));
        assert!(participants.contains(&2));
    }
}
