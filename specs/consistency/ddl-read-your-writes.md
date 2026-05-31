# Invariant: same-node read-your-writes for forwarded DDL

## Symptom that started this

The Cassandra example corpus produced `keyspace … not found — schema may still be
propagating` errors. Investigation showed two unrelated things:

1. **Most corpus failures are fixture artifacts, not a bug.** Doc fragments do
   `USE cycling` before any file creates the `cycling` keyspace (file sort order
   puts `cyclist_*` before `sai/create-vector-keyspace-cycling.cql`), and others
   reference keyspaces never created. On a single node, `CREATE KEYSPACE` →
   immediate use is read-your-writes consistent (reproduced 0/20 failures).
2. **A real cluster read-your-writes race exists** (this fix).

## Bottom-up invariants

| # | Invariant | Mechanism | Status before | After |
|---|-----------|-----------|---------------|-------|
| I1 | A node's `schema.snapshot()` reflects entries up to its `last_applied` | openraft applies committed entries in order | ✅ | ✅ |
| I2 | Leader read-your-writes: `client_write().await` returns after **leader apply** | openraft `client_write` | ✅ | ✅ |
| I3 | **Client read-your-writes on its connected node** after a DDL returns OK | — | ⚠️ leader-connected ✅; **follower-connected raced** | ✅ deterministic |
| I4 | Cluster-wide convergence (every node eventually applies) | Raft replication + apply | ✅ | ✅ |

## Root cause of the I3 gap

A client connects to one node and runs DDL → DML on that connection. For a
**follower-connected** client the DDL is forwarded to the leader; the leader
applies it and acks. The follower then returned OK to the client **without
waiting for its own state machine to apply** the entry. The leader-side barrier
(`wait_for_replication_to_catch_up`) waited for each follower's **`matched`
index** (log *replicated*) plus a fixed **50 ms `DDL_AGREEMENT_APPLY_DRAIN`
sleep** to approximate the follower's **apply**. A node cannot observe a
follower's apply progress from the leader (openraft metrics expose only
`matched`), so the apply was approximated with a sleep — a race, not a guarantee.
Under slow apply the follower served the next DML before applying the DDL →
"schema not found".

## Fix (condition-based, not timing-based)

A node **can** always observe its **own** `last_applied`. So:

1. `execute_via_raft` now returns the committed log index.
2. The leader's `ClusterDdlForwardHandler` returns that index in the
   `PairDdlAck` payload (8-byte big-endian; chained forwards relay it).
3. `forward_ddl_to_leader` (on the **client DDL path**, `local_raft = Some`)
   calls `wait_for_local_apply(raft, committed_index, timeout)` — polling the
   node's own `last_applied` — before returning OK to the client. This makes I3
   deterministic for follower-connected clients.
4. The fixed 50 ms `DDL_AGREEMENT_APPLY_DRAIN` sleep is removed; the leader-side
   replication wait (condition-based on `matched`) remains as a cross-node
   convergence aid only.

Membership/bootstrap forwards (`JoinNode`, schema hand-off) pass
`local_raft = None` — they have no waiting client and a rejoining node may lack a
usable local raft.

## Tests

- `tests/ddl_read_your_writes.rs::wait_for_local_apply_gives_follower_read_your_writes`
  — 3-node cluster, commits a DDL on the leader, then asserts a **follower**
  reaches the committed index via `wait_for_local_apply` (I3 mechanism).
- Existing multi-node DDL/forwarding integration tests guard the protocol change
  (ack now carries the index; empty acks are still handled).

## Not addressed here (separate concern)

Stale **reads** on a follower reached by a *different* connection than the one
that ran the DDL (no forward involved) — that needs read-index / leader reads and
is out of scope for same-connection read-your-writes.
