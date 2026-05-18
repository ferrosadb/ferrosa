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

pub mod coordinator;
pub mod executor;
pub mod merkle;

pub use coordinator::{RepairCoordinator, SessionExecutor, SessionResult, SessionStats};
pub use executor::{InMemoryRepairStore, LocalRepairExecutor, RepairStore};
pub use merkle::MerkleTree;

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

/// Build a Merkle tree for a table's data within a token range.
///
/// Reads all partitions from local storage, filters to the given token range,
/// computes a hash for each partition, and inserts them into the tree.
pub fn build_tree_for_range(
    storage: &StorageEngine,
    table_id: &TableId,
    range_start: i64,
    range_end: i64,
) -> Result<MerkleTree> {
    let mut tree = MerkleTree::new(TREE_DEPTH, range_start, range_end);

    let partitions = storage
        .read_range(table_id, None, None, usize::MAX)
        .map_err(crate::error::ClusterError::Storage)?;

    for partition in &partitions {
        let token = partition.key.token.0;
        if token < range_start || token >= range_end {
            continue;
        }
        tree.insert(token, partition_merkle_hash(partition));
    }

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
    /// Partition keys where both sides have identical max timestamps but
    /// different content digests. Operator must reconcile manually.
    pub timestamp_ties: Vec<ferrosa_common::DecoratedKey>,
}

/// Max timestamp across every cell + liveness in a partition, including the
/// static row. Returns `i64::MIN` for a fully-empty partition.
fn newest_partition_timestamp(p: &Partition) -> i64 {
    let mut ts = i64::MIN;
    let bump = |row: &ferrosa_sstable::types::Row, ts: &mut i64| {
        if row.primary_key_liveness.timestamp > *ts {
            *ts = row.primary_key_liveness.timestamp;
        }
        for (_, cell) in &row.cells {
            if cell.timestamp > *ts {
                *ts = cell.timestamp;
            }
        }
    };
    if let Some(ref sr) = p.static_row {
        bump(sr, &mut ts);
    }
    for row in &p.rows {
        bump(row, &mut ts);
    }
    if p.deletion.marked_for_delete_at > ts {
        ts = p.deletion.marked_for_delete_at;
    }
    ts
}

/// Diff two partition sets known to cover the same token sub-range (typically
/// the leaves identified as divergent by [`MerkleTree::divergent_leaf_ranges`]).
///
/// For each partition key:
/// - present on only one side → stream that side's copy to the other.
/// - present on both with different max timestamps → newer wins, stream to
///   the older side.
/// - present on both with equal max timestamps but different content digests
///   → record in `timestamp_ties`, do not stream. Last-write-wins with equal
///   timestamps is undefined under Cassandra's data model; better to surface
///   the conflict than silently pick a side.
/// - present on both with equal max timestamps AND equal content → no-op.
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
                let ts_a = newest_partition_timestamp(p_a);
                let ts_b = newest_partition_timestamp(p_b);
                if ts_a > ts_b {
                    plan.a_to_b.push((*p_a).clone());
                } else if ts_b > ts_a {
                    plan.b_to_a.push((*p_b).clone());
                } else if partition_merkle_hash(p_a) != partition_merkle_hash(p_b) {
                    plan.timestamp_ties.push(p_a.key.clone());
                }
                // equal ts + equal hash: no-op.
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
    match crate::raft::handlers::compute_partition_digest(partition) {
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

    #[test]
    fn diff_partition_sets_equal_timestamp_different_content_is_a_tie() {
        let a = vec![test_partition(b"k1", b"left", 500)];
        let b = vec![test_partition(b"k1", b"right", 500)];
        let plan = diff_partition_sets(&a, &b);
        assert!(plan.a_to_b.is_empty(), "ties must not be streamed");
        assert!(plan.b_to_a.is_empty(), "ties must not be streamed");
        assert_eq!(plan.timestamp_ties.len(), 1);
        assert_eq!(plan.timestamp_ties[0].key.as_bytes(), b"k1");
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
