# Bug: Token Ring Inconsistency Causes Data Scatter Across Nodes

**Severity:** P0 (data loss)
**Component:** ferrosa-cluster (token ring)
**Reproducer:** `tests/cluster/test_data_loss_reproduction.py::test_read_from_all_nodes`

## Issue

With RF=1, writes to the same partition key (same token) land on different
nodes depending on which coordinator handles the request. The token ring
view is inconsistent across nodes — each node thinks a different node owns
the same token range.

## Evidence

```
Wrote 100 rows to partition (tenant_id, session_id) = fixed UUIDs
RF=1 → all 100 should be on ONE node

Actual distribution:
  node1 (port 19042): 67/100
  node2 (port 19043): 67/100  
  node3 (port 19044): 33/100

Total unique across nodes: 100 (data exists but is scattered)
```

Node1 and node2 agree (67 each — likely node1 owns and node2 forwards).
Node3 thinks IT owns 33 of the 100 tokens (different ring view).

## Root Cause

Each node independently builds its token ring during `transition_to_cluster`
using `generate_deterministic_token(node_id, index)`. If the member list
or node IDs differ between nodes at transition time, they generate different
token assignments and disagree on ownership.

## Impact

- `SELECT count(*)` returns different results per node
- Data appears "lost" when reading from the "wrong" node
- With RF=1, there's no redundancy — each row exists on exactly one node
- The 67/33 split matches the production bug pattern exactly

## Fix

Token ring must be propagated through Raft consensus, not built independently.
The leader should propose the ring configuration and all followers should
adopt it identically. This ensures all nodes agree on token ownership.

## Implementation Notes

Implemented in commit `2faab48` (Option B from the design discussion):

- `transition_to_cluster` no longer assigns peer tokens locally. Each
  node seeds its local ring + state machine with **only itself** plus
  its own deterministic tokens. Coordinator can route to self
  immediately during the brief Raft-init window.
- After `raft.initialize()` returns on the seed, the seed authors
  `RaftOp::JoinNode + RaftOp::AssignTokens` for **every peer** via
  `raft_arc.client_write`. These commands replicate via AppendEntries.
- Each follower's `state_machine.apply_command(JoinNode|AssignTokens)`
  triggers `sync_ring()`, which rebuilds the live ring from the
  canonical Raft state.
- `generate_deterministic_token(node_id, i)` is a pure function of
  node_id, so the tokens the seed authors for a peer are byte-identical
  to what that peer would have generated for itself. No source-of-truth
  disagreement is possible; convergence holds regardless of replication
  order.

New helper: `controller::token::deterministic_tokens_for_node(nid, n)`.

Tests:
- `controller::token::tests::divergent_member_lists_produce_divergent_token_ownership`
  — pins the bug at unit level (probes at n2's tokens; ring without n2
  falls through to a different owner).
- `controller::token::tests::nodes_seeding_only_self_then_replicating_converge_to_identical_token_map`
  — proves the fix: identical token_map regardless of replication order.
- Two further determinism tests on the new helper.

`all_node_ids_for_bootstrap` (used by promotion bookkeeping later in
the spawn block) is reconstructed from the local peer view; that usage
doesn't drive token ownership and is safe with local information.

Trade-off: option A (each node submits only `JoinNode + AssignTokens`
for self) was rejected because it doubles the Raft commands during
bootstrap and creates a longer window where the local ring sees only
self. Option C (only seed builds, followers wait) was rejected because
it's structurally larger than B for the same convergence property.

Voter-equivalence-with-cluster-membership is asserted: the design
assumes every cluster member is a voter (no learners). If that ever
changes, `raft.membership()` would need filtering before use as the
canonical token-source list.
