//! Token generation and schema sync helpers.

use bytes::Bytes;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_schema::Schema;
use uuid::Uuid;

/// Send the full schema snapshot to a peer over the bulk lane.
///
/// Used both after a force-promote rejoin (to sync schema + data replay) and
/// after a normal pair reconnection (to catch up schema changes the secondary
/// missed while it was offline).
pub(super) async fn send_schema_sync_to_peer(
    pm: &PeerManager,
    peer_host_id: Uuid,
    schema: &Schema,
) {
    let snap = schema.snapshot();
    let wire_snap = crate::pair::ddl::WireSchemaSnapshot::from_snapshot(&snap);
    match serde_json::to_vec(&wire_snap) {
        Ok(json) => {
            match pm
                .send(
                    peer_host_id,
                    Message::PairSchemaSync(Bytes::from(json)),
                    Lane::Bulk,
                )
                .await
            {
                Ok(_) => tracing::info!("schema snapshot sent to rejoined peer"),
                Err(e) => tracing::warn!(%e, "failed to send schema snapshot"),
            }
        }
        Err(e) => tracing::warn!(%e, "failed to serialize schema snapshot"),
    }
}

/// Generate a deterministic token for a node.
///
/// Uses a hash-like mixing of node_id and token index to produce well-distributed
/// token values across the i64 range. All nodes running the same code will compute
/// the same token assignments for the same (node_id, index) pair.
pub(crate) fn generate_deterministic_token(node_id: u64, index: usize) -> i64 {
    // Simple but effective: use wrapping multiply with a prime and XOR to spread bits.
    let mut h = node_id.wrapping_mul(0x517cc1b727220a95);
    h ^= (index as u64).wrapping_mul(0x6c62272e07bb0142);
    h = h.wrapping_mul(0x2545F4914F6CDD1D);
    h ^= h >> 32;
    h as i64
}

/// Generate the deterministic token list for one node.
///
/// Pure function of `(node_id, num_tokens)` — every node in the cluster
/// computes the SAME tokens for a given node_id. This is the property
/// that makes Raft-based topology convergence work: each node submits
/// `RaftOp::AssignTokens(self_id, deterministic_tokens_for_self)`, and
/// when those commands replicate to peers, the peers re-derive the
/// same tokens locally. Because the input depends only on node_id (not
/// on the local node's view of cluster membership), all nodes converge
/// to the same `token_map` regardless of the order they discovered
/// each other.
///
/// See `specs/in-process/bug-token-ring-inconsistency-causes-data-scatter.md`.
pub(crate) fn deterministic_tokens_for_node(node_id: u64, num_tokens: usize) -> Vec<i64> {
    (0..num_tokens)
        .map(|i| generate_deterministic_token(node_id, i))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::TokenRing;
    use std::collections::BTreeMap;

    /// Bug repro at unit level: when two nodes build their token rings
    /// from divergent member lists (the failure mode the bug spec
    /// describes — node sees a partial peer set during cluster formation),
    /// they disagree on which node owns a given token. This is the data-
    /// scatter mechanism: writes coordinated by a node with the partial
    /// view land on a different replica than reads coordinated by a node
    /// with the full view.
    #[test]
    fn divergent_member_lists_produce_divergent_token_ownership() {
        let n1: u64 = 1;
        let n2: u64 = 2;
        let n3: u64 = 3;
        let num_tokens = 32;

        // Node1's view: full member set [n1, n2, n3].
        let mut ring_full = TokenRing::new();
        for &nid in &[n1, n2, n3] {
            for tok in deterministic_tokens_for_node(nid, num_tokens) {
                ring_full.assign_tokens(nid, &[tok]);
            }
        }

        // Node3's view: PARTIAL member set [n1, n3] (missed n2 during formation).
        let mut ring_partial = TokenRing::new();
        for &nid in &[n1, n3] {
            for tok in deterministic_tokens_for_node(nid, num_tokens) {
                ring_partial.assign_tokens(nid, &[tok]);
            }
        }

        // For at least one token, the two rings must disagree on the owner.
        // Probe AT each of n2's token positions: ring_full returns n2, but
        // ring_partial doesn't have n2 in its map and falls through to the
        // next node clockwise (n1 or n3). This is the data-scatter mechanism.
        let mut diverged = 0usize;
        let mut total = 0usize;
        for &n2_token in &deterministic_tokens_for_node(n2, num_tokens) {
            total += 1;
            let owner_full = ring_full.primary_owner(n2_token);
            let owner_partial = ring_partial.primary_owner(n2_token);
            if owner_full != owner_partial {
                diverged += 1;
            }
        }
        assert_eq!(
            diverged, total,
            "every token n2 owns in the full ring must map to a DIFFERENT node \
             in the partial ring (because partial doesn't know n2 exists). \
             diverged={diverged}/{total} — if this is < total, the deterministic \
             generator collided n2's tokens with another node's, which itself is \
             a separate bug worth investigating."
        );
    }

    /// Convergence proof: when each node seeds only ITS OWN tokens
    /// locally and then applies the equivalent of `RaftOp::AssignTokens`
    /// for every other node (in any order), the resulting token_map is
    /// identical across nodes. This is the architectural fix the bug
    /// spec calls for.
    #[test]
    fn nodes_seeding_only_self_then_replicating_converge_to_identical_token_map() {
        let n1: u64 = 1;
        let n2: u64 = 2;
        let n3: u64 = 3;
        let num_tokens = 16;

        // What each node submits to Raft about itself:
        let n1_tokens = deterministic_tokens_for_node(n1, num_tokens);
        let n2_tokens = deterministic_tokens_for_node(n2, num_tokens);
        let n3_tokens = deterministic_tokens_for_node(n3, num_tokens);

        // Node1: seeds itself locally, then applies n2's then n3's commands.
        let mut tm_n1: BTreeMap<i64, u64> = BTreeMap::new();
        for &t in &n1_tokens {
            tm_n1.insert(t, n1);
        }
        for &t in &n2_tokens {
            tm_n1.insert(t, n2);
        }
        for &t in &n3_tokens {
            tm_n1.insert(t, n3);
        }

        // Node3: seeds itself locally, then applies n1's then n2's commands
        // (DIFFERENT replication order from node1's view).
        let mut tm_n3: BTreeMap<i64, u64> = BTreeMap::new();
        for &t in &n3_tokens {
            tm_n3.insert(t, n3);
        }
        for &t in &n1_tokens {
            tm_n3.insert(t, n1);
        }
        for &t in &n2_tokens {
            tm_n3.insert(t, n2);
        }

        assert_eq!(
            tm_n1, tm_n3,
            "convergence must hold regardless of replication order; if it doesn't, \
             two nodes with the same accepted Raft log would still disagree on ownership"
        );
        assert_eq!(
            tm_n1.len(),
            num_tokens * 3,
            "no token collisions across distinct node_ids — the deterministic generator \
             must produce non-overlapping token sets for distinct nodes"
        );
    }

    #[test]
    fn deterministic_tokens_are_stable_across_calls() {
        let a = deterministic_tokens_for_node(42, 10);
        let b = deterministic_tokens_for_node(42, 10);
        assert_eq!(a, b);
        assert_eq!(a.len(), 10);
    }

    #[test]
    fn deterministic_tokens_differ_across_node_ids() {
        let a = deterministic_tokens_for_node(1, 10);
        let b = deterministic_tokens_for_node(2, 10);
        assert_ne!(a, b);
    }
}
