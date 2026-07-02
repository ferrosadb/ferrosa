# ferrosa-cluster

> The distribution layer: Raft metadata consensus (openraft), tunable-consistency
> read/write coordination, the cluster-formation state machine, anti-entropy
> repair, hinted handoff, and Accord strict-serializable transactions.

## What this crate is

`ferrosa-cluster` turns a set of single-node `ferrosa-storage` engines into a
distributed database. It owns four largely independent subsystems that share a
token ring and a Raft-replicated metadata state machine:

1. **Raft metadata consensus** (`raft/`) — schema/DDL, membership, token
   assignments, and cluster config are replicated through openraft 0.9 (a pinned
   ferrosa fork with PreVote + CheckQuorum). The state machine is in-memory
   `RaftState`; the log is sled-backed.
2. **Read/write coordination** (`coordinator/`) — fans writes and reads out to
   replicas with tunable CQL consistency-level (CL) enforcement, write
   backpressure, read repair, batchlog, and range/streaming reads.
3. **Cluster formation** (`controller/`, `mode.rs`, `ring/`, `pair/`) — the
   `ModeController` state machine drives `Standalone → Pair → Forming → Cluster`,
   the token ring computes replicas (SimpleStrategy / NetworkTopologyStrategy),
   and pair mode gives two-node durability before a Raft quorum exists.
4. **Anti-entropy & repair** (`repair/`, `hints/`) — Merkle-tree repair sessions,
   an automatic repair scheduler, hinted handoff for transiently-down replicas,
   and a quarantine → refill trigger from the storage self-heal path.

It also implements **Accord** (`accord/`), an EPaxos-family protocol for
strict-serializable multi-key / cross-shard transactions and LWT.

> **Correctness-evidence honesty.** The Accord and Raft subsystems have extensive
> *in-crate, deterministic* tests (state-machine, recovery, property, and
> simulated-nemesis). There is **no external/public Jepsen run yet** — the
> `ferrosa-jepsen` end-to-end harness is an approved-but-unbuilt standalone crate
> ([specs/todo/jepsen-e2e-test-plan.md](../specs/todo/jepsen-e2e-test-plan.md)).
> Treat consensus/transaction correctness as *tested*, not *proven in the wild*.
> See [specs/fmea.md](specs/fmea.md).

## What's implemented

### Raft metadata consensus (`raft/`)
- openraft 0.9 with features `serde`, `storage-v2`, `loosen-follower-log-revert`,
  plus a pinned fork adding **PreVote** (`raft_enable_pre_vote`, default on) and
  **CheckQuorum** (`raft_check_quorum_ratio`, default 0.75) per ADR-012.
- `FerrosStateMachine` / `RaftState` — keyspaces, tables, roles/grants, indexes,
  types, UDFs/UDAs, members, token map, per-node index status, cluster config.
- `SledLogStore` — sled-backed log + meta trees, legacy-format migration, log
  inspection/reset tooling.
- `election_guard.rs` — `run_election_guard` watchdog (P0-17/P0-19): a burst
  detector and a 30 s rolling-window detector that call `elect(false)` to suppress
  divergence-driven election storms for 60 s; `ELECTION_STORM_TERM_JUMPS_TOTAL`.
- `snapshot_pusher.rs` — leader-side sweep (P0-20) that triggers snapshot +
  heartbeat to lagging followers; `INSTALLSNAPSHOT_PUSHES_TOTAL`.
- `snapshot_transport.rs` — snapshots travel on `Lane::Bulk` (not `Lane::Raft`),
  3 MiB chunks, 60 s per-chunk timeout.
- `multi_dc_apply.rs` / `group_id.rs` — per-DC Raft groups (UUID-v5 group ids),
  HLC reorder buffer + applied-txn ledger for cross-DC Accord apply (ADR-015).
- `raft_forward.rs` — forward client writes / membership updates to the leader.

### Coordination (`coordinator/`, `consistency.rs`, `write_path.rs`)
- `ConsistencyLevel` — full CQL CL set incl. `Serial`/`LocalSerial`; `block_for` /
  `block_for_dc` quorum math, wire + string codecs.
- `write_path.rs` — the front-end-facing replica-placement boundary (ADR-021):
  `replicas_for_key(token, strategy)` and `accord_replicas_for_key(key,
  replication)` resolve a key's RF replica host ids from the ring in cluster
  mode (`None` outside it, so the caller keeps its local/all-peers fallback;
  `Err` on an unparseable strategy). The CQL/Postgres LWT/Accord paths pass raw
  key bytes + keyspace replication and never touch the partitioner or ring — the
  CQL LWT path (`route_lwt_via_accord`) uses this for token-aware, RF-correct
  participant sets instead of replicating every key to every connected peer.
- `coordinator/write.rs` — replica fan-out with `cl.block_for(rf)` ack threshold,
  NTS / `LOCAL_QUORUM` / `EACH_QUORUM` per-DC variants, hinted handoff for failed
  replicas, post-quorum hint drain, lazy mutation encoding.
- `coordinator/read.rs` — two-phase digest reads, inline read repair on digest
  mismatch (fail-loud `ReadTimeout` rather than serve stale), corrupt-SSTable
  failover feeding the bounded `AntiEntropyRepairQueue` (cap 1024) — *serve now,
  repair in background* (LOCKED DESIGN). Also hosts the index scatter-gathers:
  `coordinate_index_read` (secondary index) and `coordinate_fulltext_search`
  (`fts_match` — fans out to every node's local FTI and unions/de-dupes the
  matching keys, since full-text hits span all token ranges; BUG-F-007). FTI
  scatter-gather is partial-failure tolerant: if at least one node completes, the
  union is returned even when it is empty, so a transient remote stream failure
  does not turn a valid no-hit search into a user-visible error.
- `coordinator/cl_routing.rs` — W8.4 learner-aware routing (voter-only quorums,
  leader-only serial, cross-DC Accord routing).
- `coordinator/batch.rs` — 3-phase logged batchlog (write → fan out → delete only
  on full success) with replay task; `DEFAULT_BATCH_CONCURRENCY = 32`.
- `coordinator/{range_read_stream,stream_*}.rs` — ADR-020 streaming range reads
  (default; legacy capped path behind `FERROSA_BULK_STREAMING_RANGE_READ=0`).
  `DEFAULT_RANGE_READ_LIMIT` (10_000) is **not** a result cap on streamable
  shapes: `range_read_limited_rows` and `coordinate_range_read_stream_limited_rows`
  honor the caller's own bound (a user `LIMIT N`) uncapped; the const now only
  bounds the truncation-detecting `range_read_limited_rows_checked` probe (for the
  still-accumulating `ORDER BY` shape, until spill-to-disk lands) and the legacy
  degraded RPC (spec: `../ferrosa/specs/proposed/streaming-range-reads-no-cap.md`).
- **Write backpressure**: `WRITE_CONCURRENCY_LIMIT = 128` semaphore prevents bulk
  CQL inserts from starving Raft heartbeats on the tokio runtime.

### Formation (`controller/`, `mode.rs`, `ring/`, `pair/`, `rebalance.rs`)
- `DeploymentMode` + `ModeController` — the `Standalone → Pair → Forming →
  Cluster` state machine with degraded states; `ClusterStateHolder` dispatches
  `SingleNode` / `Pair` / `Raft` cluster state.
- `controller/bootstrap/` — 8-phase formation pipeline (DeliverInvites →
  EstablishPools → CreateRaft → WaitLeader → ReplaySchema → BootstrapStream →
  Promote → DrainQueue).
- `ring/` — `TokenRing` (BTreeMap), SimpleStrategy + NetworkTopologyStrategy
  replica selection with rack diversity, learner `owns_tokens` handling.
- `controller/membership.rs` — `MembershipChanger` mutates the four membership
  stores atomically (state machine, openraft voters, network node-map, peers);
  add/remove voter, learner-only join, promote/demote, DC swap drain.
- `controller/cluster_rejoin.rs` — P0-21 rejoin after formation timeout;
  `CLUSTER_REJOIN_ATTEMPTS_TOTAL` / `_FAILURES_TOTAL`.
- `pair/` — two-node primary/secondary coordination (role from TCP direction,
  not UUID election), switchover, catch-up.
- `rebalance.rs` — token-skew rebalancing with data streaming.

### Repair & hints (`repair/`, `hints/`)
- `repair/merkle.rs` — depth-15 Merkle trees (32 768 leaves), content-aware
  partition hashing, divergent-leaf detection.
- `repair/{coordinator,executor}.rs` — Merkle-then-stream sessions with bounded
  fetch/apply chunks; deterministic single-initiator selection (no thundering
  herd); timestamp ties surfaced, never auto-resolved (Aphyr-safe).
- `repair/scheduler.rs` — `AutoRepairScheduler` / `AutoRepairConfig` (24 h default
  interval, round-robin tables), Prometheus metrics.
- `repair/trigger.rs` — quarantine → anti-entropy refill: a corrupt SSTable
  quarantined in storage schedules a targeted refill from a verified-healthy peer.
- `hints/` — per-peer on-disk hint segments (CRC32, crash-recoverable),
  byte-budget backpressure (no silent loss → `needs_repair` + ERROR), FIFO
  at-least-once delivery as `MutationForward`. **No time-based TTL** (budget cap).

### Accord transactions (`accord/`)
- `coordinator.rs` / `state_machine.rs` — PreAccept → {fast path | Accept} →
  Commit → [read-vote] → Apply, fast/slow quorum math, HLC timestamps + `TxnId`.
  The read-vote phase is the LWT `IF`-condition gate (`ReadPredicate::NotExists`
  for `INSERT IF NOT EXISTS`, `ReadRow` for generic `IF`); a general multi-key
  SQL transaction uses `ReadPredicate::Always`, which **skips the read-vote
  entirely** and always applies after commit (there is no `IF` to evaluate). The
  replica apply path is **dep-ordered**: `handle_apply` routes every real write
  through `apply.rs`'s `DepWaitApplier`, so a mutation persists only once all of
  its dependencies have applied locally (otherwise it parks and the cascade
  applies it in order).
- `recovery.rs` — Paxos-style recovery selecting by highest `accepted_ballot`.
- `transaction_commit.rs` — `AccordTransactionCommitter`: the cluster-side
  implementation of `ferrosa_storage`'s `TransactionCommitter` seam (ADR-021). CQL/
  Postgres `BEGIN`/`COMMIT` buffer DML and call it; it resolves replicas per key
  (injected resolver wrapping `WritePath::accord_replicas_for_key` + schema in prod),
  then drives ONE unconditional (`ReadPredicate::Always`) multi-key Accord
  transaction via `new_multi`, mapping the outcome to `Committed`/`Aborted`/`Err`
  (fail-loud — never acks an uncommitted txn).
- `apply.rs` — `DepWaitApplier` (dep-wait + `StorageApplier` seam) +
  `EngineStorageApplier`/`EngineStorageReader` (real persistence and linearizable
  read-at-`t`). **Multi-key (Phase 2/3):** `DepWaitApplier::try_apply_writeset`
  parks a transaction's WHOLE write-set and applies every key on resolve;
  `StorageApplier::apply_writeset` commits all of a txn's partitions in ONE atomic
  `apply_batch` (all-or-nothing — a failure on any key persists none); idempotency
  is keyed by `(txn_id, partition_key, t)` so writes 2..N of one transaction are
  never deduped/dropped.
- `wire.rs` — bincode payloads for each protocol message. **Multi-key:**
  `WriteSetEntry` + `ApplyV2Payload` back the additive `AccordPreAcceptV2`/
  `AccordApplyV2` wire codes; `AccordCoordinatorDriver::new_multi(write_set)` is
  the multi-key constructor (`new` is the one-entry degenerate case). Multi-key
  *execution is wired*: `run_transaction` drops the old fail-loud guard, builds a
  per-shard participant (`ParticipantSet::from_per_key` via the
  `with_per_key_replicas` resolver) and fans a per-replica `AccordApplyV2`
  (scoped to each replica's owned keys) out under per-shard quorum. Conflict
  ordering unions dependencies across ALL keys: PreAccept fans `AccordPreAcceptV2`
  (every key) so each replica registers the txn under all its keys and returns the
  dep union, serializing transactions that overlap on a non-first key (t_276e12).
- `dep_wait.rs` — waits-for graph with deterministic cycle-breaking.
- `cross_shard.rs` / `cross_dc_adapter.rs` — multi-shard atomicity, cross-DC glue.
- `electorate.rs` / `epoch.rs` — JoinElectorate membership gates, epoch tracking.
- `durability.rs`, `leaseholder.rs`, `linearizable_read.rs`, `two_phase_ddl.rs`.
- In-crate Jepsen-style tests: `jepsen_bank.rs`, `jepsen_nemesis.rs`,
  `recovery_scenarios.rs`, `proptests.rs` — all on the deterministic `TestCluster`.

## Dependencies

**Calls** (ferrosa crates this depends on):
`ferrosa-cdc`, `ferrosa-common`, `ferrosa-index`, `ferrosa-net`,
`ferrosa-schema`, `ferrosa-sstable`, `ferrosa-storage`.

**Called by** (crates that depend on this):
`ferrosa`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-flight`, `ferrosa-graph`,
`ferrosa-session`, `ferrosa-sparql`.

External: `openraft` (pinned fork), `sled`, `tokio`, `arc-swap`, `dashmap`,
`parking_lot`, `bincode`, `crc32fast`, `uuid`, `serde`, `tracing`.

## Tests

~1050 test functions across the crate (in-module `#[cfg(test)]` + `tests/`).
Notable integration suites: `failure_mode_matrix` (44), `raft_election_storm`
(36), `leader_snapshot_push` (31), `accord_lwt_concurrent` (21),
`accord_nemesis` (15), `correctness` (11), `cluster_formation` (10). All run on
deterministic in-process harnesses unless gated behind `live-infra-tests`.

Range-scan memory boundedness is guarded by two allocator-tracking suites:
`range_scan_streaming_memory_bound` (the coordinator **Stream** API is O(1) in N)
and `replica_scan_serialization_memory_bound` (drives the REAL wire serialization
+ `consume_range_stream`; pins the coordinator-side `Vec<Partition>`-consume
O(result) growth — `t_3fc6be3c`/`t_ee98faa0` — plus the producer/backpressure
bounds). The gated multi-node live confirmation is `fly_stream_scan_live` (feature
`live-infra-tests` + `FERROSA_TEST_FLY=1`), which drives
`deploy/fly-stream-scan/`; it panics loudly on missing infra rather than passing.

The multi-node `TestCluster` harness (`tests/common/raft_harness.rs`) runs
openraft with short timers (50 ms heartbeat, 200–400 ms election). To keep
election convergence deterministic when `cargo test` runs many runtime-heavy test
binaries in parallel, the harness holds one of `K = ceil(cores/4)`
**cross-process** slots (an `fs2` advisory file lock) for each cluster's lifetime,
bounding aggregate raft-worker oversubscription. Leader-dependent setup uses
`require_leader(timeout)`, which fails loud at the real precondition rather than
panicking later in `leader_node()`.

## Specs

- [Architecture overview](specs/overview.md) — subsystem map, invariants, position
- [Data flow](specs/data-flow.md) — tunable-CL write + Accord transaction diagrams
- [FMEA / known issues](specs/fmea.md) — ranked failure modes + real evidence gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later

Related reference specs: `specs/reference/cluster-formation-architecture.md`,
`specs/reference/anti-entropy-repair-architecture.md`,
`specs/decisions/015-multi-dc-raft-per-dc-accord.md`,
`specs/todo/jepsen-e2e-test-plan.md`.
