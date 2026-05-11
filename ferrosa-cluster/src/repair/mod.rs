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

pub mod merkle;

pub use merkle::MerkleTree;

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
        let hash = partition_hash(partition.key.key.as_bytes());
        tree.insert(token, hash);
    }

    tree.compute_root();
    Ok(tree)
}

/// Compute a hash for a partition key's bytes.
fn partition_hash(key_bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key_bytes.hash(&mut hasher);
    hasher.finish()
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

    #[test]
    fn identical_trees_produce_no_diffs() {
        let mut t1 = MerkleTree::new(4, i64::MIN, i64::MAX);
        let mut t2 = MerkleTree::new(4, i64::MIN, i64::MAX);

        for token in [100i64, 200, 300, 400] {
            let hash = partition_hash(&token.to_be_bytes());
            t1.insert(token, hash);
            t2.insert(token, hash);
        }

        t1.compute_root();
        t2.compute_root();

        let diffs = diff_trees(&t1, &t2);
        assert!(diffs.is_empty(), "identical trees should have no diffs");
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
