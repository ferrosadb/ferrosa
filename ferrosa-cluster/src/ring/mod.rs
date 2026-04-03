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
        // Only return nodes in Normal state — Joining nodes are still
        // receiving bootstrap data and should not serve reads.
        let mut result = Vec::with_capacity(rf);
        let mut seen = HashSet::new();

        let is_normal = |nid: u64| -> bool {
            self.nodes
                .get(&nid)
                .is_some_and(|n| n.state == crate::raft::NodeState::Normal)
        };

        // Walk clockwise from token (entries >= token)
        for (_, &node_id) in self.ring.range(token..) {
            if seen.insert(node_id) && is_normal(node_id) {
                result.push(node_id);
                if result.len() >= rf {
                    return result;
                }
            }
        }

        // Wrap around to the beginning of the ring
        for (_, &node_id) in self.ring.iter() {
            if seen.insert(node_id) && is_normal(node_id) {
                result.push(node_id);
                if result.len() >= rf {
                    return result;
                }
            }
        }

        result
    }

    /// Returns the primary owner of a token regardless of node state.
    /// Used by bootstrap streaming to find which node WILL own a partition.
    pub fn primary_owner(&self, token: Token) -> Option<u64> {
        self.ring
            .range(token..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &nid)| nid)
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

    /// Get all tokens assigned to a specific node.
    pub fn tokens_for_node(&self, node_id: u64) -> Vec<Token> {
        self.ring
            .iter()
            .filter(|(_, &n)| n == node_id)
            .map(|(&t, _)| t)
            .collect()
    }

    /// Select up to `count` batchlog replica host_ids, excluding the local node.
    ///
    /// Prefers nodes in a different datacenter. Falls back to same-DC nodes
    /// if the cluster is single-DC. Returns an empty vec if this is a
    /// single-node cluster.
    pub fn select_batchlog_replicas(&self, local_node_id: u64, count: usize) -> Vec<uuid::Uuid> {
        let local_dc = self.get_node(local_node_id).map(|n| n.data_center.clone());

        // Collect all nodes except local.
        let mut candidates: Vec<&NodeInfo> = self
            .nodes
            .iter()
            .filter(|(id, _)| **id != local_node_id)
            .map(|(_, info)| info)
            .collect();

        // Sort: different DC first, then same DC.
        if let Some(ref dc) = local_dc {
            candidates.sort_by_key(|n| if n.data_center == *dc { 1 } else { 0 });
        }

        candidates.iter().take(count).map(|n| n.host_id).collect()
    }

    /// Update the lifecycle state of a node.
    pub fn set_node_state(&mut self, node_id: u64, state: crate::raft::NodeState) {
        if let Some(info) = self.nodes.get_mut(&node_id) {
            info.state = state;
        }
    }

    /// Select replicas based on a [`strategy::ReplicationStrategy`].
    ///
    /// Dispatches to [`Self::replicas`] for `Simple` or [`Self::nts_replicas`] for
    /// `NetworkTopology`.
    pub fn replicas_for_strategy(
        &self,
        token: Token,
        strategy: &strategy::ReplicationStrategy,
    ) -> Vec<u64> {
        match strategy {
            strategy::ReplicationStrategy::Simple { replication_factor } => {
                self.replicas(token, *replication_factor)
            }
            strategy::ReplicationStrategy::NetworkTopology { dc_rf } => {
                self.nts_replicas(token, dc_rf)
            }
        }
    }

    /// Find replicas for a token using NetworkTopologyStrategy.
    ///
    /// Walks clockwise from `token`, filling per-DC quotas from `dc_rf`
    /// with rack diversity: prefers nodes from unrepresented racks before
    /// accepting duplicate racks. Matches Cassandra's
    /// `NetworkTopologyStrategy.calculateNaturalReplicas`.
    pub fn nts_replicas(
        &self,
        token: Token,
        dc_rf: &std::collections::HashMap<String, usize>,
    ) -> Vec<u64> {
        use std::collections::HashMap as Map;

        // Pre-compute: distinct racks per DC
        let mut dc_racks: Map<&str, HashSet<&str>> = Map::new();
        for info in self.nodes.values() {
            if dc_rf.contains_key(&info.data_center) {
                dc_racks
                    .entry(&info.data_center)
                    .or_default()
                    .insert(&info.rack);
            }
        }
        let rack_count: Map<&str, usize> = dc_racks
            .iter()
            .map(|(&dc, racks)| (dc, racks.len()))
            .collect();

        let total_rf: usize = dc_rf.values().sum();
        let mut result = Vec::with_capacity(total_rf);
        let mut seen = HashSet::new();

        // Per-DC state
        let mut needed: Map<&str, usize> =
            dc_rf.iter().map(|(dc, &rf)| (dc.as_str(), rf)).collect();
        let mut seen_racks: Map<&str, HashSet<&str>> = Map::new();
        let mut skipped: Map<&str, Vec<u64>> = Map::new();

        let all_filled = |needed: &Map<&str, usize>| needed.values().all(|&n| n == 0);

        // Clockwise iterator: range [token..] then wrap to [..token)
        let clockwise = self.ring.range(token..).chain(self.ring.range(..token));

        for (_, &node_id) in clockwise {
            if all_filled(&needed) {
                break;
            }
            if !seen.insert(node_id) {
                continue; // skip duplicate vnodes
            }

            let info = match self.nodes.get(&node_id) {
                Some(i) => i,
                None => continue,
            };
            let dc = info.data_center.as_str();
            let rack = info.rack.as_str();

            let dc_needed = match needed.get_mut(dc) {
                Some(n) if *n > 0 => n,
                _ => continue, // DC not in dc_rf or already filled
            };

            let dc_rack_count = rack_count.get(dc).copied().unwrap_or(0);
            let dc_seen_racks = seen_racks.entry(dc).or_default();

            if dc_seen_racks.len() < dc_rack_count {
                // Not all racks represented yet for this DC
                if dc_seen_racks.contains(rack) {
                    // This rack already represented; skip for now
                    skipped.entry(dc).or_default().push(node_id);
                    continue;
                }
                dc_seen_racks.insert(rack);
            }

            result.push(node_id);
            *dc_needed -= 1;

            // If all racks now covered for this DC, drain skipped nodes
            if dc_seen_racks.len() == dc_rack_count {
                let dc_skipped = skipped.entry(dc).or_default();
                while *dc_needed > 0 && !dc_skipped.is_empty() {
                    let skipped_node = dc_skipped.remove(0);
                    result.push(skipped_node);
                    *dc_needed -= 1;
                }
            }
        }

        // Final drain: if we went around the whole ring and some DCs still
        // have skipped nodes that weren't drained (e.g., more racks in dc_rf
        // than exist), drain them now.
        for (dc, dc_skipped) in &mut skipped {
            let dc_needed = match needed.get_mut(dc) {
                Some(n) if *n > 0 => n,
                _ => continue,
            };
            while *dc_needed > 0 && !dc_skipped.is_empty() {
                let node = dc_skipped.remove(0);
                result.push(node);
                *dc_needed -= 1;
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::NodeState;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn make_node_dc(addr: &str, dc: &str, rack: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: dc.to_string(),
            rack: rack.to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
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

    #[test]
    fn tokens_for_node_returns_assigned_tokens() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));

        ring.assign_tokens(1, &[10, 20, 30]);
        ring.assign_tokens(2, &[40, 50]);

        let mut node1_tokens = ring.tokens_for_node(1);
        node1_tokens.sort_unstable();
        assert_eq!(node1_tokens, vec![10, 20, 30]);

        let mut node2_tokens = ring.tokens_for_node(2);
        node2_tokens.sort_unstable();
        assert_eq!(node2_tokens, vec![40, 50]);

        // Non-existent node returns empty
        assert!(ring.tokens_for_node(99).is_empty());
    }

    #[test]
    fn set_node_state_updates_state() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        assert_eq!(ring.get_node(1).unwrap().state, NodeState::Normal);

        ring.set_node_state(1, NodeState::Leaving);
        assert_eq!(ring.get_node(1).unwrap().state, NodeState::Leaving);

        ring.set_node_state(1, NodeState::Decommissioned);
        assert_eq!(ring.get_node(1).unwrap().state, NodeState::Decommissioned);
    }

    #[test]
    fn decommission_removes_node_from_ring() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.assign_tokens(1, &[10, 20, 30]);
        ring.assign_tokens(2, &[40, 50, 60]);

        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.token_count(), 6);

        // Simulate decommission: mark leaving, then remove
        ring.set_node_state(1, NodeState::Leaving);
        assert_eq!(ring.get_node(1).unwrap().state, NodeState::Leaving);

        ring.remove_node(1);
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.token_count(), 3);
        assert!(ring.get_node(1).is_none());
        assert!(ring.tokens_for_node(1).is_empty());
    }

    // -----------------------------------------------------------------------
    // NTS replica selection tests
    // -----------------------------------------------------------------------

    #[test]
    fn nts_replicas_two_dc_basic() {
        // 6 nodes: 3 in dc1 (racks a,b,c), 3 in dc2 (racks a,b,c)
        // dc1_rf=3, dc2_rf=2 => total 5 replicas
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.add_node(2, make_node_dc("10.0.0.2:7000", "dc1", "rack-b"));
        ring.add_node(3, make_node_dc("10.0.0.3:7000", "dc1", "rack-c"));
        ring.add_node(4, make_node_dc("10.0.0.4:7000", "dc2", "rack-a"));
        ring.add_node(5, make_node_dc("10.0.0.5:7000", "dc2", "rack-b"));
        ring.add_node(6, make_node_dc("10.0.0.6:7000", "dc2", "rack-c"));

        // Interleave tokens so nodes alternate DCs
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(4, &[200]);
        ring.assign_tokens(2, &[300]);
        ring.assign_tokens(5, &[400]);
        ring.assign_tokens(3, &[500]);
        ring.assign_tokens(6, &[600]);

        let dc_rf = HashMap::from([("dc1".to_string(), 3usize), ("dc2".to_string(), 2usize)]);
        let replicas = ring.nts_replicas(0, &dc_rf);

        // Should have 5 total replicas
        assert_eq!(replicas.len(), 5);
        // 3 from dc1
        let dc1_count = replicas
            .iter()
            .filter(|&&id| ring.get_node(id).unwrap().data_center == "dc1")
            .count();
        assert_eq!(dc1_count, 3, "dc1 should have 3 replicas");
        // 2 from dc2
        let dc2_count = replicas
            .iter()
            .filter(|&&id| ring.get_node(id).unwrap().data_center == "dc2")
            .count();
        assert_eq!(dc2_count, 2, "dc2 should have 2 replicas");
        // All distinct
        let unique: HashSet<u64> = replicas.iter().copied().collect();
        assert_eq!(unique.len(), 5, "all replicas should be distinct nodes");
    }

    #[test]
    fn nts_replicas_prefers_rack_diversity() {
        // 4 nodes in dc1: 2 in rack-a, 2 in rack-b. dc1_rf=3.
        // The algorithm should pick from both racks before doubling up.
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.add_node(2, make_node_dc("10.0.0.2:7000", "dc1", "rack-a"));
        ring.add_node(3, make_node_dc("10.0.0.3:7000", "dc1", "rack-b"));
        ring.add_node(4, make_node_dc("10.0.0.4:7000", "dc1", "rack-b"));

        // Token order: node1(100), node2(200), node3(300), node4(400)
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);
        ring.assign_tokens(3, &[300]);
        ring.assign_tokens(4, &[400]);

        let dc_rf = HashMap::from([("dc1".to_string(), 3usize)]);
        let replicas = ring.nts_replicas(0, &dc_rf);

        assert_eq!(replicas.len(), 3);

        // Both racks must be represented
        let racks: HashSet<&str> = replicas
            .iter()
            .map(|&id| ring.get_node(id).unwrap().rack.as_str())
            .collect();
        assert!(racks.contains("rack-a"), "rack-a must be represented");
        assert!(racks.contains("rack-b"), "rack-b must be represented");
    }

    #[test]
    fn nts_replicas_wraps_around_ring() {
        // All nodes near the end of the ring. Query from near i64::MAX.
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.add_node(2, make_node_dc("10.0.0.2:7000", "dc1", "rack-b"));
        ring.assign_tokens(1, &[i64::MIN]);
        ring.assign_tokens(2, &[i64::MIN + 100]);

        let dc_rf = HashMap::from([("dc1".to_string(), 2usize)]);
        let replicas = ring.nts_replicas(i64::MAX, &dc_rf);

        assert_eq!(replicas.len(), 2);
        let unique: HashSet<u64> = replicas.iter().copied().collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn nts_replicas_rf_exceeds_dc_nodes() {
        // dc1 has 2 nodes but rf=3: should return only 2 from dc1
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.add_node(2, make_node_dc("10.0.0.2:7000", "dc1", "rack-b"));

        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);

        let dc_rf = HashMap::from([("dc1".to_string(), 3usize)]);
        let replicas = ring.nts_replicas(0, &dc_rf);

        // Can only return 2 (all dc1 nodes), not 3
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn nts_replicas_empty_ring() {
        let ring = TokenRing::new();
        let dc_rf = HashMap::from([("dc1".to_string(), 3usize)]);
        let replicas = ring.nts_replicas(42, &dc_rf);
        assert!(replicas.is_empty());
    }

    #[test]
    fn nts_replicas_dc_not_in_ring() {
        // dc_rf references "dc2" but no nodes are in dc2
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.assign_tokens(1, &[100]);

        let dc_rf = HashMap::from([("dc1".to_string(), 1usize), ("dc2".to_string(), 2usize)]);
        let replicas = ring.nts_replicas(0, &dc_rf);

        // Should get 1 from dc1, 0 from dc2 (no nodes exist)
        assert_eq!(replicas.len(), 1);
    }

    // -----------------------------------------------------------------------
    // replicas_for_strategy dispatch tests
    // -----------------------------------------------------------------------

    use crate::ring::strategy::ReplicationStrategy;

    #[test]
    fn replicas_for_strategy_simple_delegates() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);

        let strategy = ReplicationStrategy::Simple {
            replication_factor: 2,
        };
        let replicas = ring.replicas_for_strategy(0, &strategy);
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn replicas_for_strategy_nts_delegates() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node_dc("10.0.0.1:7000", "dc1", "rack-a"));
        ring.add_node(2, make_node_dc("10.0.0.2:7000", "dc2", "rack-a"));
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);

        let dc_rf = HashMap::from([("dc1".to_string(), 1usize), ("dc2".to_string(), 1usize)]);
        let strategy = ReplicationStrategy::NetworkTopology { dc_rf };
        let replicas = ring.replicas_for_strategy(0, &strategy);
        assert_eq!(replicas.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Batchlog replica selection tests
    // -----------------------------------------------------------------------

    #[test]
    fn select_batchlog_replicas_excludes_local() {
        let mut ring = TokenRing::new();
        let node1 = make_node("10.0.0.1:7000");
        let node2 = make_node("10.0.0.2:7000");
        let node3 = make_node("10.0.0.3:7000");

        let id2 = node2.host_id;
        let id3 = node3.host_id;

        ring.add_node(1, node1);
        ring.add_node(2, node2);
        ring.add_node(3, node3);
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[100]);
        ring.assign_tokens(3, &[200]);

        let replicas = ring.select_batchlog_replicas(1, 2);
        assert_eq!(replicas.len(), 2);
        // Should not include the local node's host_id
        let local_host_id = ring.get_node(1).unwrap().host_id;
        assert!(!replicas.contains(&local_host_id));
        // Should include both remote nodes
        assert!(replicas.contains(&id2));
        assert!(replicas.contains(&id3));
    }

    #[test]
    fn select_batchlog_replicas_single_node_returns_empty() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.assign_tokens(1, &[0]);

        let replicas = ring.select_batchlog_replicas(1, 2);
        assert!(
            replicas.is_empty(),
            "single node ring has no remote replicas"
        );
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn nts_replicas_never_exceed_available_nodes(
            num_dc1_nodes in 1usize..=8,
            num_dc2_nodes in 1usize..=8,
            dc1_rf in 1usize..=5,
            dc2_rf in 1usize..=5,
            query_token in prop::num::i64::ANY,
        ) {
            let mut ring = TokenRing::new();
            let mut next_id = 1u64;
            let mut token = -1_000_000i64;

            for i in 0..num_dc1_nodes {
                let rack = format!("rack-{}", i % 3);
                ring.add_node(next_id, make_node_dc(
                    &format!("10.0.1.{}:7000", next_id),
                    "dc1",
                    &rack,
                ));
                ring.assign_tokens(next_id, &[token]);
                next_id += 1;
                token += 100;
            }
            for i in 0..num_dc2_nodes {
                let rack = format!("rack-{}", i % 3);
                ring.add_node(next_id, make_node_dc(
                    &format!("10.0.2.{}:7000", next_id),
                    "dc2",
                    &rack,
                ));
                ring.assign_tokens(next_id, &[token]);
                next_id += 1;
                token += 100;
            }

            let dc_rf = HashMap::from([
                ("dc1".to_string(), dc1_rf),
                ("dc2".to_string(), dc2_rf),
            ]);
            let replicas = ring.nts_replicas(query_token, &dc_rf);

            // Invariant 1: no duplicate nodes
            let unique: HashSet<u64> = replicas.iter().copied().collect();
            prop_assert_eq!(unique.len(), replicas.len(),
                "replicas must be distinct");

            // Invariant 2: per-DC count <= min(dc_rf, dc_nodes)
            let dc1_replicas: Vec<_> = replicas.iter()
                .filter(|&&id| ring.get_node(id).unwrap().data_center == "dc1")
                .collect();
            let dc2_replicas: Vec<_> = replicas.iter()
                .filter(|&&id| ring.get_node(id).unwrap().data_center == "dc2")
                .collect();
            prop_assert!(dc1_replicas.len() <= dc1_rf.min(num_dc1_nodes),
                "dc1 replicas {} exceeds min(rf={}, nodes={})",
                dc1_replicas.len(), dc1_rf, num_dc1_nodes);
            prop_assert!(dc2_replicas.len() <= dc2_rf.min(num_dc2_nodes),
                "dc2 replicas {} exceeds min(rf={}, nodes={})",
                dc2_replicas.len(), dc2_rf, num_dc2_nodes);

            // Invariant 3: total replicas = sum of per-DC actual counts
            prop_assert_eq!(
                replicas.len(),
                dc1_replicas.len() + dc2_replicas.len(),
                "total should equal dc1 + dc2"
            );

            // Invariant 4: when enough nodes, per-DC count equals RF
            if num_dc1_nodes >= dc1_rf {
                prop_assert_eq!(dc1_replicas.len(), dc1_rf,
                    "dc1 should fill to rf={} with {} nodes", dc1_rf, num_dc1_nodes);
            }
            if num_dc2_nodes >= dc2_rf {
                prop_assert_eq!(dc2_replicas.len(), dc2_rf,
                    "dc2 should fill to rf={} with {} nodes", dc2_rf, num_dc2_nodes);
            }
        }
    }
}
