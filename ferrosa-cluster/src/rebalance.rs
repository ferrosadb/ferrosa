//! Token rebalancing for even distribution across cluster nodes.
//!
//! When nodes join or leave the cluster, token ownership can become skewed.
//! This module computes a rebalancing plan that moves tokens from over-represented
//! nodes to under-represented nodes until the maximum skew is below 5%.

use std::collections::BTreeMap;

use ferrosa_net::peer::PeerManager;
use futures::StreamExt;

use crate::raft::Token;
use crate::ring::TokenRing;
use crate::streaming::{StreamConfig, StreamSender, StreamedMutation};

/// A plan describing which tokens to move between nodes to achieve balance.
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    /// Individual token reassignments to execute.
    pub reassignments: Vec<TokenReassignment>,
}

/// A single token reassignment from one node to another.
#[derive(Debug, Clone)]
pub struct TokenReassignment {
    /// The token being moved.
    pub token: Token,
    /// The node currently owning this token.
    pub from_node: u64,
    /// The node that should own this token after rebalancing.
    pub to_node: u64,
}

/// Compute a rebalancing plan for the token ring.
///
/// Algorithm:
/// 1. Count tokens per node.
/// 2. Compute ideal tokens per node = total_tokens / num_nodes.
/// 3. Find over-represented (> ideal) and under-represented (< ideal) nodes.
/// 4. Move tokens from over to under until max skew < 5%.
/// 5. Return plan (empty if already balanced).
pub fn compute_rebalance(ring: &TokenRing) -> RebalancePlan {
    let node_ids = ring.node_ids();
    let num_nodes = node_ids.len();

    if num_nodes < 2 {
        return RebalancePlan {
            reassignments: vec![],
        };
    }

    let total_tokens = ring.token_count();
    if total_tokens == 0 {
        return RebalancePlan {
            reassignments: vec![],
        };
    }

    let ideal = total_tokens / num_nodes;
    if ideal == 0 {
        return RebalancePlan {
            reassignments: vec![],
        };
    }

    // Build per-node token ownership.
    let mut node_tokens: BTreeMap<u64, Vec<Token>> = BTreeMap::new();
    for &nid in &node_ids {
        node_tokens.insert(nid, ring.tokens_for_node(nid));
    }

    // Check if already balanced (max skew < 5%).
    let max_skew = compute_max_skew(&node_tokens, ideal);
    if max_skew < 0.05 {
        return RebalancePlan {
            reassignments: vec![],
        };
    }

    let mut reassignments = Vec::new();

    // Iteratively move tokens from over-represented to under-represented nodes.
    // We cap iterations to prevent infinite loops.
    let max_iterations = total_tokens;
    for _ in 0..max_iterations {
        // Find the most over-represented node and most under-represented node.
        let (over_node, over_count) = node_tokens
            .iter()
            .max_by_key(|(_, tokens)| tokens.len())
            .map(|(&nid, tokens)| (nid, tokens.len()))
            .unwrap();

        let (under_node, under_count) = node_tokens
            .iter()
            .min_by_key(|(_, tokens)| tokens.len())
            .map(|(&nid, tokens)| (nid, tokens.len()))
            .unwrap();

        // If the difference is 1 or less, we're as balanced as we can get.
        if over_count <= under_count + 1 {
            break;
        }

        // Check if max skew is already below 5%.
        let current_skew = compute_max_skew(&node_tokens, ideal);
        if current_skew < 0.05 {
            break;
        }

        // Move one token from over_node to under_node.
        if let Some(token) = node_tokens.get_mut(&over_node).and_then(|t| t.pop()) {
            node_tokens.entry(under_node).or_default().push(token);
            reassignments.push(TokenReassignment {
                token,
                from_node: over_node,
                to_node: under_node,
            });
        } else {
            break;
        }
    }

    RebalancePlan { reassignments }
}

/// Compute the maximum skew as a fraction: max(|count - ideal| / ideal) across all nodes.
fn compute_max_skew(node_tokens: &BTreeMap<u64, Vec<Token>>, ideal: usize) -> f64 {
    node_tokens
        .values()
        .map(|tokens| {
            let diff = (tokens.len() as f64 - ideal as f64).abs();
            diff / ideal as f64
        })
        .fold(0.0_f64, f64::max)
}

/// Execute a rebalancing plan by streaming affected ranges and proposing
/// token reassignments via Raft.
///
/// Steps:
/// 1. Compute the rebalance plan.
/// 2. If empty, return immediately.
/// 3. Stream data for reassigned token ranges from source to target nodes.
/// 4. Propose `AssignTokens` via Raft for each target node.
pub async fn execute_rebalance(
    raft: &crate::raft::FerrosRaft,
    ring: &TokenRing,
    storage: &ferrosa_storage::engine::StorageEngine,
    schema: &ferrosa_schema::Schema,
    peer_manager: &PeerManager,
    local_node_id: u64,
) -> crate::error::Result<()> {
    let plan = compute_rebalance(ring);
    if plan.reassignments.is_empty() {
        tracing::info!("rebalance: ring is already balanced, nothing to do");
        return Ok(());
    }

    tracing::info!(
        reassignments = plan.reassignments.len(),
        "rebalance: executing plan"
    );

    // Group reassignments by target node for batch AssignTokens proposals.
    let mut by_target: BTreeMap<u64, Vec<Token>> = BTreeMap::new();
    for r in &plan.reassignments {
        by_target.entry(r.to_node).or_default().push(r.token);
    }

    // Stream data for reassigned token ranges from source to target nodes.
    //
    // Only stream from the local node: each node in the cluster runs
    // execute_rebalance and streams the partitions it currently owns that
    // are being reassigned to a different node.
    let local_reassignments: Vec<&TokenReassignment> = plan
        .reassignments
        .iter()
        .filter(|r| r.from_node == local_node_id)
        .collect();

    if !local_reassignments.is_empty() {
        // Collect the set of tokens being moved away, grouped by target node.
        let mut tokens_by_target: BTreeMap<u64, Vec<Token>> = BTreeMap::new();
        for r in &local_reassignments {
            tokens_by_target.entry(r.to_node).or_default().push(r.token);
        }

        let schema_snap = schema.snapshot();
        let config = StreamConfig::default();
        let mut session_counter = 0_u64;

        for (ks, tbl) in schema_snap.tables.keys() {
            if ks.starts_with("system") {
                continue;
            }

            let table_id = ferrosa_storage::commitlog::TableId::new(ks, tbl);

            // Stream partitions directly to each target instead of grouping
            // the whole table into a map-of-Vec first. A partition belongs to
            // a reassignment if the token ring currently maps it to the local
            // node AND its token is in the set being moved to a target.
            let mut streamed_for_table = 0usize;
            let mut partitions = storage.range_iter(&table_id, None, None);

            while let Some(partition) = partitions.next().await {
                let partition = match partition {
                    Ok(partition) => partition,
                    Err(e) => {
                        tracing::warn!(%e, ks, tbl, "rebalance: failed to read partition");
                        continue;
                    }
                };
                let token = partition.key.token.0; // Token(i64) -> i64

                // Check if this token is being reassigned to any target.
                for (&target_node_id, target_tokens) in &tokens_by_target {
                    if !target_tokens.contains(&token) {
                        continue;
                    }

                    // Serialize rows via RowWire for full fidelity
                    // (clustering keys, all cells, deletion, liveness).
                    use crate::raft::handlers::RowWire;
                    let wire_rows: Vec<RowWire> =
                        partition.rows.iter().cloned().map(RowWire::from).collect();
                    let row_bytes = match bincode::serialize(&wire_rows) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::error!(
                                %e,
                                partition_key = ?partition.key,
                                "rebalance: failed to serialize rows, skipping partition"
                            );
                            continue;
                        }
                    };
                    let ts = partition
                        .rows
                        .first()
                        .and_then(|r| r.cells.first())
                        .map(|(_, cv)| cv.timestamp)
                        .unwrap_or(0);

                    let target_uuid = ring
                        .get_node(target_node_id)
                        .map(|n| n.host_id)
                        .unwrap_or_default();
                    session_counter += 1;
                    let mutation = StreamedMutation {
                        keyspace: ks.clone(),
                        table: tbl.clone(),
                        key: partition.key.key.as_bytes().to_vec(),
                        row: row_bytes,
                        timestamp: ts,
                    };

                    if let Err(e) = StreamSender::send_stream(
                        vec![mutation],
                        peer_manager,
                        target_uuid,
                        session_counter,
                        (i64::MIN, i64::MAX),
                        local_node_id,
                        &config,
                    )
                    .await
                    {
                        tracing::warn!(
                            %e,
                            target = target_node_id,
                            "rebalance: streaming failed for {ks}.{tbl}"
                        );
                    } else {
                        streamed_for_table += 1;
                    }
                }
            }

            if streamed_for_table > 0 {
                tracing::info!(
                    count = streamed_for_table,
                    "rebalance: streamed {ks}.{tbl} partition mutations"
                );
            }
        }

        tracing::info!(
            tables_scanned = schema_snap.tables.len(),
            "rebalance: data streaming complete"
        );
    }

    // Propose AssignTokens for each target node.
    for (node_id, tokens) in by_target {
        let cmd = crate::raft::RaftCommand {
            op: crate::raft::RaftOp::AssignTokens { node_id, tokens },
            schema_version: uuid::Uuid::new_v4(),
        };
        raft.client_write(cmd).await.map_err(|e| {
            crate::error::ClusterError::RaftError(format!(
                "rebalance: AssignTokens for node {node_id} failed: {e}"
            ))
        })?;
    }

    tracing::info!("rebalance: plan executed successfully");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
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

    #[test]
    fn compute_rebalance_evens_distribution() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.add_node(3, make_node("10.0.0.3:7000"));
        ring.add_node(4, make_node("10.0.0.4:7000"));

        // Uneven distribution: node 1 has 100 tokens, others have 0-20.
        let tokens_n1: Vec<Token> = (0..100).map(|i| i * 1000).collect();
        let tokens_n2: Vec<Token> = (100..120).map(|i| i * 1000).collect();
        let tokens_n3: Vec<Token> = (120..130).map(|i| i * 1000).collect();
        let tokens_n4: Vec<Token> = (130..170).map(|i| i * 1000).collect();

        ring.assign_tokens(1, &tokens_n1);
        ring.assign_tokens(2, &tokens_n2);
        ring.assign_tokens(3, &tokens_n3);
        ring.assign_tokens(4, &tokens_n4);

        let total = ring.token_count(); // 170
        let ideal = total / 4; // 42

        let plan = compute_rebalance(&ring);
        assert!(
            !plan.reassignments.is_empty(),
            "uneven distribution should produce reassignments"
        );

        // Apply the plan to a simulated ring and verify skew < 5%.
        let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
        for nid in [1, 2, 3, 4] {
            counts.insert(nid, ring.tokens_for_node(nid).len());
        }
        for r in &plan.reassignments {
            *counts.entry(r.from_node).or_default() -= 1;
            *counts.entry(r.to_node).or_default() += 1;
        }

        let max_skew = counts
            .values()
            .map(|&c| ((c as f64 - ideal as f64).abs()) / ideal as f64)
            .fold(0.0_f64, f64::max);

        assert!(
            max_skew < 0.05,
            "after rebalance, max skew should be < 5%, got {:.1}%",
            max_skew * 100.0
        );
    }

    #[test]
    fn compute_rebalance_noop_when_balanced() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.add_node(3, make_node("10.0.0.3:7000"));

        // Even distribution: 100 tokens each.
        let tokens_n1: Vec<Token> = (0..100).map(|i| i * 1000).collect();
        let tokens_n2: Vec<Token> = (100..200).map(|i| i * 1000).collect();
        let tokens_n3: Vec<Token> = (200..300).map(|i| i * 1000).collect();

        ring.assign_tokens(1, &tokens_n1);
        ring.assign_tokens(2, &tokens_n2);
        ring.assign_tokens(3, &tokens_n3);

        let plan = compute_rebalance(&ring);
        assert!(
            plan.reassignments.is_empty(),
            "balanced ring should produce empty plan"
        );
    }

    #[test]
    fn compute_rebalance_single_node_noop() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));

        let tokens: Vec<Token> = (0..256).collect();
        ring.assign_tokens(1, &tokens);

        let plan = compute_rebalance(&ring);
        assert!(
            plan.reassignments.is_empty(),
            "single node should produce empty plan"
        );
    }

    #[test]
    fn compute_rebalance_empty_ring_noop() {
        let ring = TokenRing::new();
        let plan = compute_rebalance(&ring);
        assert!(
            plan.reassignments.is_empty(),
            "empty ring should produce empty plan"
        );
    }

    #[test]
    fn compute_rebalance_two_nodes_uneven() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));

        // Node 1 has 200 tokens, node 2 has 0.
        let tokens: Vec<Token> = (0..200).map(|i| i * 100).collect();
        ring.assign_tokens(1, &tokens);

        let plan = compute_rebalance(&ring);
        assert!(
            !plan.reassignments.is_empty(),
            "2 nodes with all tokens on one should produce reassignments"
        );

        // After applying, each node should have ~100 tokens.
        let mut counts = BTreeMap::new();
        counts.insert(1u64, 200usize);
        counts.insert(2u64, 0usize);
        for r in &plan.reassignments {
            *counts.get_mut(&r.from_node).unwrap() -= 1;
            *counts.get_mut(&r.to_node).unwrap() += 1;
        }

        let ideal = 100.0_f64;
        for &count in counts.values() {
            let skew = ((count as f64 - ideal).abs()) / ideal;
            assert!(
                skew < 0.05,
                "each node's skew should be < 5%, got {:.1}%",
                skew * 100.0
            );
        }
    }

    #[test]
    fn reassignment_fields_are_correct() {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));

        // Node 1 has 10 tokens, node 2 has 0.
        let tokens: Vec<Token> = (0..10).collect();
        ring.assign_tokens(1, &tokens);

        let plan = compute_rebalance(&ring);
        for r in &plan.reassignments {
            assert_eq!(r.from_node, 1, "tokens should move from node 1");
            assert_eq!(r.to_node, 2, "tokens should move to node 2");
            assert!(
                tokens.contains(&r.token),
                "reassigned token should be from the original set"
            );
        }
    }
}

#[cfg(test)]
mod streaming_contract_tests {
    #[test]
    fn rebalance_must_not_read_tables_with_capped_materialized_range() {
        let source = include_str!("rebalance.rs")
            .split("#[cfg(test)]\nmod streaming_contract_tests")
            .next()
            .expect("production rebalance source must be present");
        assert!(
            !source.contains("REBALANCE_READ_LIMIT"),
            "rebalance must stream table contents instead of using a hard row cap"
        );
        assert!(
            !source.contains("storage.read_range(&table_id, None, None"),
            "rebalance must not materialize whole table ranges before streaming mutations"
        );
    }
}
