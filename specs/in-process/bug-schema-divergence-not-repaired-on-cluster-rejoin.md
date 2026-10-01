---
title: "bug: a rejoining node's schema divergence is never repaired in cluster mode"
type: bug
priority: P0
status: in-process
reported-by: agent
created: 2026-10-01
updated: 2026-10-01
---

# Cluster-mode rejoin never repairs a diverged schema

## Observed

A node that leaves a 3-node cluster, is restarted, and comes back can hold a
**stale schema** relative to the cluster, and nothing repairs it.

After a restart the node logs:

```text
INFO ferrosa_cluster::controller: this node has been a cluster member; rejoining rather than forming mode=degraded-cluster
INFO ferrosa_cluster::controller::cluster: raft recovered committed topology; skipping bootstrap streaming and promotion
INFO ferrosa_cluster::controller::cluster: non-leader: forwarding local schema to leader ks_count=5 table_count=89
```

`table_count=89` against a cluster that serves **101** tables. The divergence
persisted indefinitely (observed for over two hours, unmoving).

The user-visible consequence: a CQL driver that performs a statement requiring
schema agreement can never complete it, because the driver compares the schema
version of **every** node and requires them to be identical. The statement then
fails with a timeout even when the statement itself is a satisfied
`IF NOT EXISTS`:

```text
Error: statement failed (migration v0): CREATE KEYSPACE IF NOT EXISTS control
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}:
Request timeout: schema agreement not reached in time
```

Every process that opens a fresh session to the cluster fails this way. A
process holding a long-lived session keeps working, which masks the condition.

## Root cause

The `ReplaySchema` phase in `ferrosa-cluster/src/controller/cluster.rs`
(around line 2372) is **push-only**. It reads the node's *local* schema and
offers it to the cluster:

```rust
let user_ks / user_tables = schema_for_replay.snapshot();   // LOCAL schema
// leader:     propose the LOCAL schema through Raft
// non-leader: forward the LOCAL schema to the leader
```

A node that is **missing** tables therefore re-offers tables that already exist,
each a no-op that logs "may already exist", and it has no way to learn the
tables it lacks. The comment above the block reads "schema convergence", but the
code only enriches the cluster *from* each node; it never reconciles a node
*to* the cluster.

The mechanism that would fix it already exists, and is wired for **pair mode
only**:

- `send_schema_sync_to_peer(pm, peer_host_id, schema)` in
  `ferrosa-cluster/src/controller/token.rs` sends the full schema snapshot as
  `Message::PairSchemaSync` over the bulk lane. It is called from
  `ferrosa-cluster/src/controller/pair.rs` and nowhere else.
- `PairSchemaSyncHandler` in `ferrosa-cluster/src/pair/ddl.rs` receives that
  snapshot, registers tables missing locally with the storage engine,
  unregisters tables the snapshot dropped (via `StorageEngine::unregister_table`,
  as `DropTable` would), then applies it to the schema registry.
- That handler is registered at `controller/pair.rs:177` — pair mode only.
  `controller/cluster.rs` registers `RaftVote`, `RepairWrite`, `StreamEnd`,
  `BootstrapComplete` and `PairDdlForward`, but **not** `PairSchemaSync`.

So in cluster mode no node ever sends a schema snapshot to a rejoining peer, and
the receiving node has no handler registered even if one arrived.

Also ruled out: `run_snapshot_pusher` (P0-20) triggers on **log-index** lag
(`lag > lag_threshold`). A node whose Raft log is caught up but whose schema
registry is stale never trips it, so the pusher does not cover this case.

## Design invariant for the missing sender half

**Only the cluster's authoritative side may send a schema snapshot.**

`PairSchemaSyncHandler` does not merely add tables — it computes the set of
tables **its own** schema holds that the incoming snapshot lacks, and
**unregisters** each one (`StorageEngine::unregister_table`, which releases the
table and deletes its local SSTable directory) as `DropTable` would.

That is correct when the snapshot is authoritative and the receiver is behind.
It is **destructive in the other direction**: if a node with a stale schema sent
its snapshot to a healthy node, the healthy node would treat the tables the
snapshot lacks as dropped and delete them. A naive "send my schema to the leader
on rejoin" would therefore turn a benign divergence into data loss.

So the sender must be the leader (or a node that has confirmed its schema is not
behind), and the receiver must be the one that was behind. Any implementation
that pushes a *rejoining* node's schema outward must not reuse this handler
unmodified.

## Progress

- [x] Root cause identified and evidenced.
- [x] Unit RED: a returning member must have the schema-sync handler registered.
      Failed for the right reason before the change.
- [x] GREEN: the handler is registered on the returning-member
      (`DegradedCluster`) path. Full `ferrosa-cluster --lib` suite passes
      (1263 tests, parallel) with it.
- [x] Live reproduction written:
      `ferrosa-cluster/tests/cluster_schema_convergence_live.rs` — stop a node,
      create a table, restart it, assert the table becomes visible on it.
- [ ] **SENDER not yet implemented.** The receiver can now accept a snapshot, but
      nothing sends one, so the divergence still does not repair. Needs a join
      hook in cluster mode that resolves the peer's host id and, subject to the
      invariant above, sends the authoritative schema snapshot.
- [ ] Live test not yet green (waits on the sender).
- [ ] Idempotence and dropped-table guards under the new sender.

## Reproduction finding: short outages self-heal, so the test must outrun the Raft log

A live reproduction was written and run:
`ferrosa-cluster/tests/cluster_schema_convergence_live.rs` — stop node 3, create a
keyspace+table while it is down, restart it, assert the table is visible on it.

**It passes, and the pass is honest — but it does not reproduce the production
divergence.** Node 3's log shows why:

```text
existing commit log segments found — replaying for crash recovery
restored schema from local schema.json ks_count=4 table_count=0
WARN ... recoverable_tables=1 tables=default.rdf_triples   <- NOT the new keyspace
raft recovered committed topology; skipping bootstrap streaming and promotion
openraft ... applied index 18 -> 23
```

The DDL entries committed while node 3 was stopped were **still in its retained
Raft log**, so on restart openraft replayed them and the state machine applied
the `CreateKeyspace` / `CreateTable`. The node converged with no schema-sync path
involved.

The production divergence was permanent because the missed tables were **older
than the log's retained window** — node 1 forwarded `table_count=89` against a
cluster of 101, i.e. the entries that created those 12 tables had long since
been snapshotted and purged. Log replay cannot recover what is no longer in the
log, which is exactly the case with no catch-up path.

**Consequence for the fix and its test:** the reproduction must make the missed
DDL fall outside the retained Raft log before restarting the node — for example
by driving enough subsequent entries (or forcing a snapshot + purge) that the
earlier DDL is purged, then restarting. Only then does the missing schema-sync
path become observable. A restart-and-recheck test with a short outage will pass
against even the unfixed code and is therefore not a valid red test.

Two hazards the reproduction surfaced and now handles:

- taking a node down makes the internode lane reconnect, so a statement issued in
  that window is refused with `net: lane is reconnecting; retry later`; the test
  retries that bounded rather than failing on it.
- a reproduction that stops a cluster node must restart it on **every** exit,
  including a panic, or a failing run leaves the fixture a node short. An earlier
  version of this test did exactly that, and the stale state it left then made the
  next run pass vacuously (a fixed keyspace name plus `IF NOT EXISTS` meant the
  node already had the schema). The keyspace is now unique per run.

## Acceptance criteria

- [ ] A node that missed DDL while it was down converges in cluster mode without
      operator action: after it rejoins and restarts, the missed table is
      CQL-visible on that node and `SELECT` against it succeeds.
- [ ] The cluster-mode controller registers a handler for `MsgType::PairSchemaSync`.
- [ ] A rejoining peer is sent a schema snapshot on join in cluster mode.
- [ ] Tables dropped while the node was down are unregistered, as `DropTable`
      would (no orphaned SSTable directories).
- [ ] A redundant sync converges rather than corrupts (idempotent).
- [ ] Fail-loud is preserved: a snapshot that cannot be applied is logged at
      ERROR and does not silently report success.

## Why an in-process test is not sufficient

The failure is cluster-shaped: DDL commits while one node is down, that node
restarts and rejoins, and the divergence is only observable across real nodes
with real RPC. Per repo test policy the reproduction belongs in the ignored
live-cluster suite, gated behind `live-infra-tests` and
`FERROSA_TEST_CLUSTER_NODES`, and must go RED before the fix.
