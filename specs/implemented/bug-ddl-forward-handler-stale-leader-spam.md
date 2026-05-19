---
type: bug
priority: P2
status: draft
created: 2026-04-19
updated: 2026-04-20
reported-by: 2026-04-19 ferrosa-memory cluster cycle (writer-validation deploy)
---

# `ClusterDdlForwardHandler` logs `ERROR` on every forward after cluster formation because senders still hold the pair-mode primary view

## Observed

On a fresh 3-node cluster, ~15 seconds after Raft elects a leader (node1),
node2 starts logging a steady stream of:

```
ERROR ferrosa_cluster::ddl_path: ClusterDdlForwardHandler: execute_via_raft
  failed: not Raft leader; forward to node 1229782938247303441
```

29 such errors on node2 over ~5 minutes (≈ 1 every 10 s), zero on node1 or
node3. node1 is the Raft leader.

## Root cause (probable)

1. 18:05:52 — node2 enters `standalone → pair` with `role=primary`, peer=node1.
2. 18:05:58 — node2 transitions `pair → cluster`; Raft init spawned.
3. 18:05:59 — `raft leader elected, leader=1229…(node1)`. node2 is now a
   Raft follower; DDL path swapped to `Cluster`.
4. Some caller (likely a persistent coordinator/peer still using the
   pair-mode forward cache) keeps sending `Message::PairDdlForward` to
   node2 because node2 was its view of "pair primary".
5. node2's registered `ClusterDdlForwardHandler` calls
   `execute_via_raft(...)` which correctly errors with "not Raft leader;
   forward to node 1229…". The handler logs `ERROR` and returns `None`,
   leaving the sender to time out → retry → loop.

## Why it's not critical

- No data corruption.
- Not in a data-path; affects DDL only (rare ops).
- Self-resolves when the sender's leader cache expires or is refreshed.

## Why it should still be fixed

- `ERROR` log-level spam makes real errors harder to see.
- The forward is wasted — both sender and receiver spend cycles on
  doomed RPCs and their retries.
- Indicates a view-sync gap that could cause correctness issues in
  failover scenarios (e.g., pair-mode secondary receiving DDL after a
  primary swap).

## Proposed fix

Two independent changes:

1. **Handler side (ddl_path.rs:509)**: when `execute_via_raft` returns
   `NotLeader(leader_id)`, instead of logging `ERROR` and dropping, reply
   with a `Message::PairDdlRedirect { leader_id }`. Sender can redirect
   without waiting for a retry timer.
2. **Sender side (`forward_ddl_to_leader`, ddl_path.rs:313)**: query the
   current Raft leader via `ModeController` instead of the pair-mode
   primary cache when mode is `Cluster`. The cached "primary" is stale
   the moment Raft elects; don't rely on it.

Either alone breaks the spin loop; both together close the gap.

## Acceptance criteria

- [ ] Zero `execute_via_raft failed: not Raft leader` errors on any node
      after cluster formation completes (allow up to 30 s of startup
      noise while leader election stabilises).
- [ ] New unit test: pair primary transitions to Raft follower, a stale
      `PairDdlForward` lands on the ex-primary, the sender receives a
      redirect and retries against the Raft leader within one round-trip.

## Related

- `specs/implemented/bug-read-path-memory-growth-bloats-coordinator.md`
  (same cluster environment).
- `ferrosa-cluster/src/ddl_path.rs:309-517`.
