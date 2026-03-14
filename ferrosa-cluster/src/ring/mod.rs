//! Token ring for data sharding across cluster nodes.
//!
//! Maps i64 tokens to node IDs via BTreeMap. Replica selection walks
//! clockwise from the partition token, collecting distinct nodes.

pub mod strategy;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

use crate::raft::{NodeInfo, Token};

/// Token ring mapping tokens to owning node IDs.
///
/// Uses BTreeMap for O(log n) lookup and efficient clockwise walks
/// via range queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenRing {
    /// Token → owning node_id.
    ring: BTreeMap<Token, u64>,
    /// Node metadata.
    nodes: BTreeMap<u64, NodeInfo>,
}

impl TokenRing {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a node's metadata.
    pub fn add_node(&mut self, node_id: u64, info: NodeInfo) {
        self.nodes.insert(node_id, info);
    }

    /// Remove a node and all its tokens.
    pub fn remove_node(&mut self, node_id: u64) {
        self.nodes.remove(&node_id);
        self.ring.retain(|_, &mut n| n != node_id);
    }

    /// Assign tokens to a node.
    pub fn assign_tokens(&mut self, node_id: u64, tokens: &[Token]) {
        for &token in tokens {
            self.ring.insert(token, node_id);
        }
    }

    /// Find RF replicas for a token using SimpleStrategy.
    /// Walks clockwise from token, collecting distinct node IDs.
    pub fn replicas(&self, token: Token, rf: usize) -> Vec<u64> {
        let mut result = Vec::with_capacity(rf);
        let mut seen = HashSet::new();

        // Walk clockwise from token (entries >= token)
        for (_, &node_id) in self.ring.range(token..) {
            if seen.insert(node_id) {
                result.push(node_id);
                if result.len() >= rf {
                    return result;
                }
            }
        }

        // Wrap around to the beginning of the ring
        for (_, &node_id) in self.ring.iter() {
            if seen.insert(node_id) {
                result.push(node_id);
                if result.len() >= rf {
                    return result;
                }
            }
        }

        result
    }

    /// Number of distinct nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total number of token positions.
    pub fn token_count(&self) -> usize {
        self.ring.len()
    }

    /// Get node info by ID.
    pub fn get_node(&self, node_id: u64) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    /// Get all node IDs.
    pub fn node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::NodeState;
    use uuid::Uuid;

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
        }
    }

    #[test]
    fn replicas_single_node_rf1() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.assign_tokens(1, &[0, 100, 200]);

        let replicas = ring.replicas(50, 1);
        assert_eq!(replicas, vec![1]);
    }

    #[test]
    fn replicas_three_nodes_rf3() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.add_node(3, make_node("10.0.0.3:7000"));
        // Place each node at a distinct token position.
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);
        ring.assign_tokens(3, &[300]);

        let mut replicas = ring.replicas(0, 3);
        replicas.sort_unstable();
        assert_eq!(replicas, vec![1, 2, 3]);
    }

    #[test]
    fn replicas_wraps_around() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        // Node 1 at a low token, node 2 at a mid-range token.
        ring.assign_tokens(1, &[i64::MIN]);
        ring.assign_tokens(2, &[0]);

        // Query near i64::MAX — should find no entry >= that token, then wrap.
        let replicas = ring.replicas(i64::MAX, 1);
        // After wrapping, the first entry is i64::MIN owned by node 1.
        assert_eq!(replicas, vec![1]);
    }

    #[test]
    fn replicas_skips_duplicate_vnodes() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        // Node 1 owns many vnodes, node 2 owns one.
        ring.assign_tokens(1, &[10, 20, 30, 40]);
        ring.assign_tokens(2, &[50]);

        // RF=2: should return node 1 once, then node 2.
        let replicas = ring.replicas(0, 2);
        assert_eq!(replicas.len(), 2);
        assert!(replicas.contains(&1));
        assert!(replicas.contains(&2));
        // Node 1 must not appear twice.
        assert_eq!(replicas.iter().filter(|&&n| n == 1).count(), 1);
    }

    #[test]
    fn replicas_rf_exceeds_nodes() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);

        // RF=3 but only 2 distinct nodes — should return 2.
        let replicas = ring.replicas(0, 3);
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn replicas_empty_ring() {
        let ring = TokenRing::new();
        let replicas = ring.replicas(42, 3);
        assert!(replicas.is_empty());
    }

    #[test]
    fn assign_tokens_and_count() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        let tokens: Vec<Token> = (0..256).map(|i| i as Token * 1_000_000).collect();
        ring.assign_tokens(1, &tokens);
        assert_eq!(ring.token_count(), 256);
    }

    #[test]
    fn remove_node_removes_tokens() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.assign_tokens(1, &[10, 20, 30]);
        assert_eq!(ring.token_count(), 3);

        ring.remove_node(1);
        assert_eq!(ring.token_count(), 0);
        assert_eq!(ring.node_count(), 0);
    }

    #[test]
    fn node_management() {
        let mut ring = TokenRing::new();
        let info = make_node("10.0.0.1:7000");
        ring.add_node(42, info.clone());

        assert_eq!(ring.node_count(), 1);
        assert!(ring.get_node(42).is_some());
        assert_eq!(ring.get_node(42).unwrap().addr, "10.0.0.1:7000");

        let ids = ring.node_ids();
        assert_eq!(ids, vec![42]);

        ring.remove_node(42);
        assert_eq!(ring.node_count(), 0);
        assert!(ring.get_node(42).is_none());
    }
}
