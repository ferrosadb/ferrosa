---
crate: ferrosa-cluster
status: implemented
last_updated: 2026-08-07
executive_summary: >
  The distribution layer that turns single-node ferrosa-storage engines into a
  cluster: Raft metadata consensus (openraft 0.9 fork with CheckQuorum; a PreVote
  gate exists in the fork but defaults OFF until its network transport is built),
  tunable-CL read/write coordination with write backpressure and read repair, the
  Standalone→Pair→Forming→Cluster formation state machine, Merkle anti-entropy
  repair + hinted handoff, and Accord strict-serializable transactions. Consensus
  and transaction logic are heavily *tested in-crate* on deterministic harnesses;
  external/public Jepsen validation is tracked (ferrosa-jepsen) but not yet built.
---

# ferrosa-cluster — Architecture Overview

## Purpose & boundary

`ferrosa-cluster` is the **distribution boundary**. Below it sits
`ferrosa-storage` (a single-node engine); above it sit the query front-ends
(`ferrosa-cql`, `ferrosa-graph`, `ferrosa-sparql`, `ferrosa-flight`,
`ferrosa-session`) and the control surface (`ferrosa`, `ferrosa-ctl`). It is the
*only* place that knows how data and metadata are replicated, routed, and
reconciled across nodes.

It owns five concerns that share a token ring + a Raft-replicated metadata state
machine but are otherwise loosely coupled:

1. **Metadata consensus** — strongly-consistent DDL/membership/tokens via Raft.
2. **Data-path coordination** — eventually-consistent, tunable-CL reads/writes.
3. **Formation** — bringing nodes from standalone to a quorum-backed cluster.
4. **Anti-entropy** — Merkle repair + hinted handoff to converge replicas.
5. **Transactions** — Accord for strict-serializable multi-key / LWT operations.

## Module map

| Module | Responsibility |
|--------|----------------|
| `raft/` | openraft type config, `FerrosStateMachine`/`RaftState`, `SledLogStore`, network, handlers, `election_guard`, `snapshot_pusher`, `snapshot_transport`, `multi_dc_apply`, `group_id` |
| `coordinator/` | `ClusterCoordinator`: `write`, `read`, `batch` (batchlog), `cl_routing`, `truncate`, streaming range reads (`range_read_stream`, `stream_*`) |
| `consistency.rs` | `ConsistencyLevel` enum, `block_for`/`block_for_dc`, wire + string codecs |
| `controller/` | `ModeController`, `ClusterStateHolder`, `bootstrap/` 8-phase pipeline, `membership` (`MembershipChanger`), `cluster_rejoin`, `invite`, `operator`, `peer_*` |
| `mode.rs` | `DeploymentMode` enum + transition rules |
| `ring/` | `TokenRing`, SimpleStrategy + NetworkTopologyStrategy replica selection |
| `pair/` | two-node primary/secondary coordination, switchover, catch-up |
| `rebalance.rs` | token-skew rebalancing with data streaming |
| `repair/` | Merkle trees, repair coordinator/executor, scheduler, quarantine→refill trigger, RPC, cluster view |
| `hints/` | per-peer hint segments, delivery/replay, CRC + crash recovery |
| `accord/` | EPaxos-family transactions: coordinator, state machine, recovery, dep-wait, cross-shard/DC, electorate, durability, deterministic test cluster |
| `state.rs` | `SingleNodeClusterState` / `PairClusterState` / `RaftClusterState` |
| `write_path.rs`, `raft_forward.rs`, `ddl_path.rs`, `index_coordination.rs`, `streaming/`, `system_table_*` | write routing, leader forwarding, DDL routing, index build coordination, SSTable streaming, system-table persistence |

## Data flow (summary)

**Tunable-CL write.** A front-end calls `ClusterCoordinator::coordinate_write*`.
The coordinator acquires a `write_semaphore` permit (cap 128, backpressure),
resolves replicas from the `TokenRing`, computes the ack threshold
`cl.block_for(rf)`, writes locally and fans out to remotes, and returns once the
threshold is met. Failed replicas get hints; post-quorum stragglers are drained in
a detached task. See [data-flow.md](data-flow.md).

**Tunable-CL read.** `coordinate_read*` issues one full read + (block_for − 1)
digest reads, then resolves. On digest mismatch it fail-loud re-fetches the newest
copy and repairs stale replicas inline; on a corrupt local SSTable it fails over
to a healthy replica and enqueues a background anti-entropy refill.

**Range read.** The public write-path range-read surface is streaming-first:
projected scans use `range_read_projected_stream_all_from` /
`range_read_projected_stream_all_with`, and the old `Vec<Partition>`-returning
`range_read_projected` wrapper has been removed so projected scans cannot drift
back to local-only materialization.

**Keyed index read (t_430c4188).** `coordinate_index_read_in_partition`
serves `WHERE <full partition key> AND <indexed_col> = ?`: it resolves the
partition's replicas from the ring under the keyspace strategy and sends each an
`IndexReadInPartitionRequest` (`0x66`/`0x67`); the replica consults its
secondary index restricted to that partition and point-reads only the matching
rows (O(matching rows), never O(partition rows)). Results merge per token;
partial replica failures degrade to a partial union (logged); all-replicas-failed
errors. Exposed as `WritePath::index_read_in_partition` (Direct/Pair resolve
locally). Unlike `coordinate_index_read`, this never fans out to the whole ring.

**Full-text scatter-gather.** `coordinate_fulltext_search` fans an `fts_match`
index lookup out to every node — its hits span all token ranges (there is no
partition key) — running each node's local FTI via a `FulltextSearchRequest` RPC
and unioning/de-duping the matching keys. Partial failures degrade only if every
node fails; if at least one node completes, the union is returned even when it is
empty, so transient remote stream faults do not convert legitimate no-hit queries
into errors. Routed through `WritePath::fulltext_search` (Direct/Pair resolve
locally). Fixes the coordinator-local non-determinism of BUG-F-007.

**DDL / membership.** Mutations are wrapped as `RaftOp`, proposed via
`raft.client_write` (forwarded to the leader if needed via `raft_forward`),
committed, and applied deterministically into `RaftState` on every node. DROP
INDEX also calls `StorageEngine::drop_index` in Direct, pair, and Raft apply
paths so live `TableStore`/index-tracker state follows the replicated schema.

**Accord transaction.** `AccordCoordinator` runs PreAccept → (fast path or Accept)
→ Commit → Apply across the participating shards, dep-waiting on conflicting
transactions before applying to storage at the agreed HLC timestamp. See
[data-flow.md](data-flow.md).

## Key invariants

1. **CL is an ack/digest threshold, never a fake success.** Writes return only
   after `cl.block_for(rf)` acks; reads return only after `block_for` matching
   digests. A read that cannot resolve a digest mismatch returns `ReadTimeout`,
   not stale data.
2. **Raft serves only metadata.** Schema, membership, tokens, and cluster config
   flow through Raft; user data does not. This keeps the Raft log small and
   apply-fast, and is why write backpressure (`WRITE_CONCURRENCY_LIMIT = 128`)
   protects Raft heartbeats from data-path saturation.
3. **Repair never silently picks a winner.** Equal-timestamp content divergence is
   recorded as a timestamp tie and surfaced, not auto-resolved.
4. **Corruption is loud.** Corrupt SSTables fail over + enqueue refill; hint
   eviction sets `needs_repair` and logs ERROR; formation under-durability
   increments a counter. No path masquerades corruption as "not found".
5. **Single deterministic repair initiator per range.** Computed as a pure
   function of the ring (lowest live `host_id`), so each range is repaired once.
6. **Membership mutates four stores atomically.** `MembershipChanger` keeps the
   Raft state machine, openraft voter set, network node-map, and peer table in
   sync; a partial change is an error, not a silent skew.
7. **Reverse dialing uses the peer's canonical endpoint.** An inbound socket's
   source port is ephemeral. When the handshake advertises an internode
   host/port, `peer_events` resolves and uses it for the reverse pool and
   `connected_peers`; only legacy peers without a usable advertisement fall back
   to the observed IP plus the local internode port.

## Correctness evidence (be honest)

- **What exists:** ~1050 in-crate tests, including Accord state-machine/recovery
  scenarios, property tests (ballot invariant, recovery determinism, dep-wait
  cycle detection, HLC monotonicity), simulated bank/nemesis workloads on the
  deterministic `TestCluster`, Raft election-storm and snapshot-push suites, and a
  failure-mode matrix.
- **What does NOT exist yet:** a real external/public Jepsen run. `ferrosa-jepsen`
  (multi-language CQL drivers, Firecracker/Fly chaos plane, Knossos/Elle checking)
  is an **approved but unbuilt** standalone crate
  ([../specs/todo/jepsen-e2e-test-plan.md](../specs/todo/jepsen-e2e-test-plan.md)).
- **Implication:** consensus + transaction safety is *tested deterministically*,
  not *empirically validated under real partitions, clock skew, and disk faults.*
  This is the crate's headline FMEA gap — see [fmea.md](fmea.md).

## Position in the dependency graph

**Calls** `ferrosa-cdc`, `ferrosa-common`, `ferrosa-index`, `ferrosa-net`,
`ferrosa-schema`, `ferrosa-sstable`, `ferrosa-storage`.

**Called by** `ferrosa`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-flight`,
`ferrosa-graph`, `ferrosa-session`, `ferrosa-sparql`.

It is a hub crate: a wide fan-in of front-ends depend on its coordinator/consensus
API, so its public surface (CL semantics, `ModeController`, `MembershipChanger`,
repair) changes ripple broadly. See the [root crate index](../../specs/crates.md).
