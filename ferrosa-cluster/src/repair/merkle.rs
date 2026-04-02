//! Merkle tree for anti-entropy repair.
//!
//! A binary hash tree where each leaf covers a sub-range of the token space.
//! Internal nodes contain the XOR of their children's hashes.

use serde::{Deserialize, Serialize};

/// Binary Merkle tree over a token range.
///
/// The tree has `2^depth` leaves. Each leaf accumulates the XOR of all
/// partition hashes whose tokens fall in that leaf's sub-range. Internal
/// nodes are the XOR of their children.
///
/// Serializable so it can be exchanged between nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleTree {
    /// Depth of the tree (number of levels below the root).
    pub depth: u32,
    /// Start of the token range (inclusive).
    pub range_start: i64,
    /// End of the token range (exclusive).
    pub range_end: i64,
    /// Flat array of nodes. Index 0 is root, children of node i are
    /// at 2i+1 and 2i+2. Leaves start at index `2^depth - 1`.
    pub nodes: Vec<u64>,
}

impl MerkleTree {
    /// Create an empty tree with `2^depth` leaves covering `[range_start, range_end)`.
    pub fn new(depth: u32, range_start: i64, range_end: i64) -> Self {
        assert!(depth > 0, "depth must be > 0");
        assert!(depth <= 20, "depth must be <= 20 to avoid excessive memory");
        // Total nodes in a complete binary tree: 2^(depth+1) - 1.
        let total_nodes = (1usize << (depth + 1)) - 1;
        Self {
            depth,
            range_start,
            range_end,
            nodes: vec![0u64; total_nodes],
        }
    }

    /// Insert a partition hash at the given token position.
    ///
    /// XORs the hash into the appropriate leaf. Call `compute_root()` after
    /// all inserts to propagate hashes up the tree.
    pub fn insert(&mut self, token: i64, hash: u64) {
        let leaf_idx = self.token_to_leaf(token);
        let num_leaves = 1usize << self.depth;
        let node_idx = (num_leaves - 1) + leaf_idx;
        if node_idx < self.nodes.len() {
            self.nodes[node_idx] ^= hash;
        }
    }

    /// Propagate leaf hashes up to the root (post-order traversal).
    pub fn compute_root(&mut self) {
        let num_leaves = 1usize << self.depth;
        let first_leaf = num_leaves - 1;
        // Walk from the last internal node up to the root.
        for i in (0..first_leaf).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            self.nodes[i] = self.nodes[left] ^ self.nodes[right];
        }
    }

    /// Get the root hash.
    pub fn root_hash(&self) -> u64 {
        self.nodes[0]
    }

    /// Map a token to a leaf index in `[0, 2^depth)`.
    fn token_to_leaf(&self, token: i64) -> usize {
        let num_leaves = 1usize << self.depth;
        // Shift tokens to unsigned space to avoid overflow on i64::MIN..i64::MAX.
        let start = (self.range_start as i128 + i64::MAX as i128 + 1) as u128;
        let end = (self.range_end as i128 + i64::MAX as i128 + 1) as u128;
        let tok = (token as i128 + i64::MAX as i128 + 1) as u128;
        let range_width = end.wrapping_sub(start);
        if range_width == 0 {
            return 0;
        }
        let offset = tok.wrapping_sub(start);
        let leaf = (offset * num_leaves as u128 / range_width) as usize;
        leaf.min(num_leaves - 1)
    }

    /// Number of leaves in the tree.
    pub fn num_leaves(&self) -> usize {
        1usize << self.depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree_has_zero_root() {
        let mut tree = MerkleTree::new(4, 0, 1000);
        tree.compute_root();
        assert_eq!(tree.root_hash(), 0);
    }

    #[test]
    fn single_insert_changes_root() {
        let mut tree = MerkleTree::new(4, 0, 1000);
        tree.insert(500, 0xDEADBEEF);
        tree.compute_root();
        assert_ne!(tree.root_hash(), 0);
    }

    #[test]
    fn same_inserts_produce_same_root() {
        let mut t1 = MerkleTree::new(8, i64::MIN, i64::MAX);
        let mut t2 = MerkleTree::new(8, i64::MIN, i64::MAX);

        for i in 0..100 {
            let token = i * 1000;
            let hash = (i as u64) * 0x12345;
            t1.insert(token, hash);
            t2.insert(token, hash);
        }

        t1.compute_root();
        t2.compute_root();

        assert_eq!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn different_inserts_produce_different_root() {
        let mut t1 = MerkleTree::new(4, 0, 1000);
        let mut t2 = MerkleTree::new(4, 0, 1000);

        t1.insert(100, 0xAAAA);
        t2.insert(100, 0xBBBB);

        t1.compute_root();
        t2.compute_root();

        assert_ne!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn xor_is_order_independent() {
        let mut t1 = MerkleTree::new(4, 0, 1000);
        let mut t2 = MerkleTree::new(4, 0, 1000);

        // Insert in different orders.
        t1.insert(100, 0xAA);
        t1.insert(100, 0xBB);

        t2.insert(100, 0xBB);
        t2.insert(100, 0xAA);

        t1.compute_root();
        t2.compute_root();

        assert_eq!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn serialization_roundtrip() {
        let mut tree = MerkleTree::new(4, 0, 1000);
        tree.insert(500, 0xCAFE);
        tree.compute_root();

        let bytes = bincode::serialize(&tree).unwrap();
        let decoded: MerkleTree = bincode::deserialize(&bytes).unwrap();
        assert_eq!(tree, decoded);
    }

    #[test]
    fn leaf_count_matches_depth() {
        let tree = MerkleTree::new(10, 0, 1000);
        assert_eq!(tree.num_leaves(), 1024);
    }

    #[test]
    fn token_to_leaf_maps_correctly() {
        let tree = MerkleTree::new(4, 0, 1600);
        // 16 leaves, each covering 100 tokens.
        // Token 0 → leaf 0, token 99 → leaf 0, token 100 → leaf 1.
        assert_eq!(tree.token_to_leaf(0), 0);
        assert_eq!(tree.token_to_leaf(99), 0);
        assert_eq!(tree.token_to_leaf(100), 1);
        assert_eq!(tree.token_to_leaf(1599), 15);
    }
}
