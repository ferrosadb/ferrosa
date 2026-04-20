---
type: coverage
scope: ferrosa-cluster (excluding accord/)
reviewed: 2026-04-18
reviewer: coverage-agent
status: draft
---

# Cluster Coverage Review — Consensus + Routing (non-Accord)

Scope: `ferrosa-cluster/src/**` excluding `src/accord/`. Compared against specs under `specs/`.

---

## 1. Feature Inventory

Each entry lists the primary `pub` item or operator-visible mechanism, its canonical source location, and a one-line description.

### 1.1 Raft Subsystem (`raft/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| R1 | `FerrosRaft` (type alias) | `raft/mod.rs:53` | openraft instance parameterised over `FerrosRaftConfig` |
| R2 | `RaftCommand` / `RaftOp` | `raft/mod.rs:127,237` | Commands committed to the replicated log (schema, topology, auth) |
| R3 | `RaftResponse` | `raft/mod.rs:250` | Discriminated response after apply |
| R4 | `NodeInfo` / `NodeState` | `raft/mod.rs:78,61` | Per-node topology metadata; states: Joining/Normal/Leaving/Dead |
| R5 | `IndexNodeStatus` | `raft/mod.rs:106` | Per-node secondary-index readiness flag |
| R6 | `FerrosStateMachine` + `RaftState` | `raft/state_machine.rs:114,55` | openraft `RaftStateMachine` impl; holds schema + ring + auth in memory |
| R7 | `apply_command` | `raft/state_machine.rs:263` | Dispatcher for all `RaftOp` variants → schema/auth/topology mutations |
| R8 | `seed_topology` / `recover_membership` | `raft/state_machine.rs:225,177` | Startup recovery helpers |
| R9 | `SnapshotData` + `SnapshotBuilder` | `raft/state_machine.rs:99,840` | openraft snapshot serialisation (bincode of `RaftState`) |
| R10 | `SledLogStore` | `raft/log_store.rs` | Persistent Raft log (sled KV); implements `RaftLogStorage` |
| R11 | `FerrosRaftNetworkFactory` | `raft/network.rs` | openraft `RaftNetworkFactory`; sends AppendEntries/Vote/Snapshot over priority pool |
| R12 | `RaftAppendHandler` | `raft/handlers.rs:417` | Inbound AppendEntries RPC handler |
| R13 | `RaftVoteHandler` | `raft/handlers.rs:482` | Inbound Vote RPC handler |
| R14 | `RaftSnapshotHandler` | `raft/handlers.rs:548` | Inbound InstallSnapshot RPC handler |
| R15 | `ReadRequestHandler` / `RangeReadHandler` / `IndexReadHandler` | `raft/handlers.rs:610,745,829` | Inbound data-read RPC handlers (used by `ClusterCoordinator`) |
| R16 | `LazyRaft` | `raft/handlers.rs:332` | Deferred-init Raft handle; allows handler registration before Raft node starts |
| R17 | `uuid_to_node_id` | `raft/mod.rs:267` | Maps node UUID → openraft u64 node ID |

### 1.2 Controller (`controller/`)

| # | Item | Location | Description |
|---|------|----------|-------------|| C1 | `ModeController` | `controller/mod.rs:138` | Top-level orchestrator; holds all shared state; lifecycle manager |
| C2 | `ModeControllerHandles` | `controller/mod.rs:215` | Arc handle set exposed to CQL/graph layers |
| C3 | `DeploymentMode` | `mode.rs:8` | `Standalone | Pair | Cluster`; drives `WritePath` + `DdlPath` selection |
| C4 | `is_cql_ready` | `controller/mod.rs:460` | Returns true when enough topology is stable to accept CQL |
| C5 | `shutdown` / `cancel_token` | `controller/mod.rs:480,511` | Graceful drain; broadcasts cancellation to all subsystems |
| C6 | `ClusterStateHolder` | `controller/mod.rs:74` | Enum wrapping `SingleNodeClusterState | PairClusterState | RaftClusterState` |
| C7 | `standalone_for_test` / `pair_secondary_for_test` | `controller/mod.rs:311,361` | Test factories |
| C8 | `ClusterInviteHandler` | `controller/cluster.rs:1259` | Handles inbound `ClusterInvite` messages during formation |
| C9 | `BootstrapCompleteHandler` | `controller/cluster.rs:514` | Signals when bootstrap streaming from all peers is acknowledged |
| C10 | `PeerEventListener` impl | `controller/peer_events.rs:15` | Reacts to connect/disconnect/suspect/recover/fail events |
| C11 | `on_inbound_peer` | `controller/peer_events.rs:182` | Inbound connection callback; drives Standalone→Pair transition |
| C12 | Transition to Pair | `controller/cluster.rs` (search `transition_to_pair`) | Sets up `PairNode`, `PairCoordinator`, swaps `WritePath`/`DdlPath` |
| C13 | Transition to Cluster | `controller/cluster.rs` (search `transition_to_cluster`) | Raft init, schema replay, bootstrap streaming, `TokenRing` seeding |
| C14 | `trigger_cluster_join` | `controller/cluster.rs` | Proposes `AddNode` + `AssignTokens` via Raft when peer connects in Cluster mode |
| C15 | `ContentionMetrics` | `controller/mod.rs:102` | Records `record_guard_hold` durations for `MutexGuard` contention |
| C16 | `generate_deterministic_token` | `controller/token.rs` | Stable initial token for a node (UUID-hash based) |
| C17 | `MembershipHandler` | `controller/membership.rs` | Handles Raft membership change proposals from operator |

### 1.3 Coordinator (`coordinator/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| CO1 | `ClusterCoordinator` | `coordinator/mod.rs:33` | Central coordinator; holds ring + peer manager + hint store |
| CO2 | `WRITE_CONCURRENCY_LIMIT` | `coordinator/mod.rs:30` | `Semaphore(128)` — write backpressure (Raft starvation fix P1) |
| CO3 | `MutationForwardHandler` | `coordinator/mod.rs:105` | Inbound mutation forward RPC; applies write locally |
| CO4 | `TruncateForwardHandler` / `encode_truncate_payload` | `coordinator/mod.rs:159,192` | Cluster-mode table truncate broadcast |
| CO5 | `RepairWriteHandler` | `coordinator/mod.rs:231` | Applies read-repair writes from coordinator |
| CO6 | `coordinate_write` / `coordinate_write_with` | `coordinator/write.rs:36,57` | Fan-out write to RF replicas, enforce CL, store hints on failure |
| CO7 | `coordinate_write_nts` | `coordinator/write.rs:242` | NetworkTopologyStrategy-aware write (per-DC quorum) |
| CO8 | `coordinate_read` / `coordinate_read_with` | `coordinator/read.rs:127,172` | Digest-compare multi-replica read with inline read-repair |
| CO9 | `coordinate_read_nts` | `coordinator/read.rs:673` | NTS-aware read (per-DC CL) |
| CO10 | `coordinate_range_read` | `coordinator/read.rs:753` | Token-range scan across replicas |
| CO11 | `coordinate_index_read` | `coordinator/read.rs:915` | Secondary-index lookup routed to index-ready replicas |
| CO12 | `select_index_ready_replicas` | `coordinator/read.rs:50` | Filters ring replicas by `IndexNodeStatus::Ready` |
| CO13 | `coordinate_batch` (BatchlogWrite/Replay) | `coordinator/batch.rs` | Two-phase batchlog: write → replicate → apply → delete |
| CO14 | `ReadRepairMetrics` | `coordinator/metrics.rs` | Prometheus counters for digest mismatches + repair success |

### 1.4 Write Path Abstraction (`write_path.rs`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| WP1 | `WritePath` enum | `write_path.rs:27` | `Direct | Pair | Cluster | Unavailable`; unified dispatch |
| WP2 | `write_batch` | `write_path.rs:64` | Routes batch writes through correct path |
| WP3 | `pk_read` / `read` / `range_read` / `index_read` | `write_path.rs:97,142,176,198` | Uniform read dispatch across all modes |
| WP4 | `truncate` | `write_path.rs:225` | Mode-aware truncate dispatch |
| WP5 | `write` | `write_path.rs:244` | Single-partition write dispatch |

### 1.5 Pair Subsystem (`pair/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| P1 | `PairRole` / `PairState` | `pair/mod.rs` | `Primary | Secondary`; mutable via `ArcSwap` |
| P2 | `PairNode` | `pair/node.rs:27` | Lifecycle: builds handler registry, starts networking, reacts to peer events |
| P3 | `PairEventListener` | `pair/node.rs:41` | Handles connect/disconnect/fail for the pair peer |
| P4 | `PairCoordinator` | `pair/coordinator.rs` | Write replication in pair mode; primary applies locally + forwards to secondary |
| P5 | `DdlCoordinator` + `DdlOperation` | `pair/ddl.rs:148,44` | DDL in pair mode; syncs schema to secondary |
| P6 | `PairDdlForwardHandler` | `pair/ddl.rs:419` | Inbound DDL forward on secondary; applies op directly |
| P7 | `PairSchemaSyncHandler` | `pair/ddl.rs:582` | Full schema snapshot sync on reconnect |
| P8 | `PairWriteForwardHandler` | `pair/handler.rs` | Inbound write forward on primary (secondary→primary) |
| P9 | `CatchupHandler` | `pair/catchup.rs` | Point-in-time catch-up replication after reconnect |
| P10 | `initiate_switchover` | `pair/switchover.rs:20` | Operator-triggered Primary↔Secondary role swap |
| P11 | `RoleSwapHandler` | `pair/switchover.rs:68` | Inbound role-swap acknowledgement handler |

### 1.6 DDL Path (`ddl_path.rs`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| D1 | `DdlPath` enum | `ddl_path.rs:24` | `Direct | Pair | Cluster | Unavailable`; routes DDL by mode |
| D2 | `DdlPath::execute` | `ddl_path.rs:69` | Top-level DDL dispatcher |
| D3 | `apply_direct` | `ddl_path.rs:147` | Standalone-mode: applies DDL locally to schema + engine |
| D4 | `ddl_op_to_raft_command` | `ddl_path.rs:341` | Translates `DdlOperation` → `RaftCommand` for Raft consensus |
| D5 | `execute_via_raft` | (inside ddl_path) | Submits command, waits for Raft commit + schema agreement |
| D6 | `ClusterDdlForwardHandler` | `ddl_path.rs:469` | On Raft non-leader: receives forwarded DDL and re-submits to leader |

### 1.7 Topology / Ring (`ring/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| T1 | `TokenRing` | `ring/mod.rs:18` | Consistent hash ring; BST keyed by `Token` (i64) |
| T2 | `replicas` | `ring/mod.rs:50` | Returns RF replicas for a token (wraps ring) |
| T3 | `nts_replicas` | `ring/mod.rs:180` | NetworkTopologyStrategy per-DC replica selection |
| T4 | `select_batchlog_replicas` | `ring/mod.rs:129` | Picks batchlog replicas avoiding local node |
| T5 | `set_node_state` | `ring/mod.rs:149` | Updates `NodeState` in ring (used after Raft log apply) |
| T6 | `ReplicationStrategy` | `ring/strategy.rs` | `SimpleStrategy | NetworkTopologyStrategy` |
| T7 | `RebalancePlan` / `compute_rebalance` | `rebalance.rs:17,41` | Computes token reassignments to reduce max skew |
| T8 | `execute_rebalance` | `rebalance.rs:144` | Streams data for each token move via `StreamSender` |

### 1.8 Streaming (`streaming/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| S1 | `StreamSender` + `send_stream` | `streaming/sender.rs:53,66` | Sends row-level mutations to a peer over Data lane |
| S2 | `send_sstable_files` | `streaming/sender.rs:197` | Sends raw SSTable files (bootstrap / decommission) |
| S3 | `StreamReceiver` + `receive_and_apply` | `streaming/receiver.rs:152,176` | Receives row mutations and applies to local engine |
| S4 | `SstableStreamReceiver` + `receive_and_write` | `streaming/receiver.rs:315,342` | Receives SSTable bytes and writes to disk |
| S5 | `StreamConfig` | `streaming/mod.rs:124` | Chunk size + concurrency limits |

### 1.9 Hinted Handoff (`hints/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| H1 | `HintStore` + `store` | `hints/mod.rs:115,177` | Persists hints per peer in segmented log files |
| H2 | `HintDrain` + `drain` | `hints/mod.rs:335` | Iterates pending hints for delivery |
| H3 | `evict_oldest` | `hints/mod.rs:268` | Enforces per-peer hint capacity (`HintConfig.max_bytes`) |
| H4 | `HintDeliveryTask` | `hints/delivery.rs` | Background task: delivers hints to reconnected peer |
| H5 | `needs_repair` | `hints/mod.rs:322` | Returns true if hints have overflowed (triggers anti-entropy flag) |

### 1.10 Repair (`repair/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| RE1 | `build_tree_for_range` | `repair/mod.rs:36` | Builds Merkle tree over a token range from local storage |
| RE2 | `diff_trees` | `repair/mod.rs:73` | Returns differing token ranges between two Merkle trees |
| RE3 | `MerkleTree` | `repair/merkle.rs` | Fixed-depth binary Merkle tree |

### 1.11 System Table Persistence

| # | Item | Location | Description |
|---|------|----------|-------------|
| ST1 | `SystemTableWriter` | `system_table_writer.rs` | Writes schema + topology to system keyspace SSTables after Raft apply |
| ST2 | `SystemTableLoader` | `system_table_loader.rs` | Reads system tables at startup to populate in-memory state |

### 1.12 Telemetry (`telemetry/`)

| # | Item | Location | Description |
|---|------|----------|-------------|
| TE1 | `FerrosaTelemetryLayer` | `telemetry/mod.rs:22` | tracing-subscriber layer with probabilistic sampling |
| TE2 | `TelemetryWriter` | `telemetry/writer.rs` | Writes sampled spans to storage telemetry table |
| TE3 | `TelemetryLayer` | `telemetry/layer.rs` | Full span capture for cluster-level tracing |

### 1.13 Index Coordination

| # | Item | Location | Description |
|---|------|----------|-------------|
| IX1 | `RemoteNodeBackend` + `build_remote` | `index_coordination.rs:72,82` | Dispatches index builds to remote `ferrosa-index-builder` node |
| IX2 | `IndexBuildRequestPayload` / `IndexBuildCompletePayload` | `index_coordination.rs:10,29` | Wire types for remote index RPC |

### 1.14 Configuration + Consistency

| # | Item | Location | Description |
|---|------|----------|-------------|
| CF1 | `ClusterConfig` + `from_env` | `config.rs:24,84` | Seeds, data_dir, election timeouts, hint config, RF defaults |
| CF2 | `ConsistencyLevel` | `consistency.rs:8` | Full CQL CL enum + `block_for` / `block_for_dc` quorum arithmetic |
| CF3 | `ClusterError` | `error.rs` | Typed cluster error enum; no silent-ok returns |

### 1.15 State Representations

| # | Item | Location | Description |
|---|------|----------|-------------|
| SS1 | `SingleNodeClusterState` | `state.rs:14` | Standalone mode; trivial peer list |
| SS2 | `PairClusterState` | `state.rs:27` | Pair mode; resolves peer broadcast addresses |
| SS3 | `RaftClusterState` | `state.rs:101` | Cluster mode; ring-based address resolution |
| SS4 | `BroadcastResolver` trait | `state.rs:83` | Abstraction over broadcast address lookup |

---

## 2. Spec Coverage Matrix

| Feature | Spec File | Coverage Quality |
|---------|-----------|-----------------|
| **Raft state machine** (`FerrosStateMachine`, `apply_command`, snapshots, log store) | `cluster-formation-architecture.md` §Raft Subsystem; `dsm-cluster-formation.md` rows P,Q,R,S,T | **Partial** — structure documented; no dedicated Raft correctness spec (failure modes, log compaction, membership change safety) |
| **Raft handlers** (Append/Vote/Snapshot/Read RPCs, `LazyRaft`) | `cluster-formation-architecture.md` §ClusterInvite; `dsm-cluster-formation.md` row Q | **Partial** — `LazyRaft` design rationale is in ADR-3; handler wire protocol not independently specced |
| **ModeController** (lifecycle, transitions, shutdown) | `cluster-formation-architecture.md` §ModeController; `dsm-cluster-formation.md` rows A0–A6 | **Full** — transitions, methods, and decomposition fully described |
| **DeploymentMode** (`Standalone|Pair|Cluster`) | `cluster-formation-state-machine.md`; `cluster-formation-architecture.md` §State Machine | **Partial** — spec calls for richer `FormationState` (Forming, Degraded variants); code still uses 3-variant `DeploymentMode` (Gap #2 in arch spec) |
| **ClusterInvite protocol** | `cluster-formation-architecture.md` §ClusterInvite; ADR-3 | **Full** — message format, delivery lane, retry count, propagation logic all documented |
| **Bootstrap phases A/B/C** | `cluster-formation-architecture.md` §Bootstrap; `dsm-cluster-formation.md` A1 | **Full** — all three phases described with sequence diagram |
| **Pair mode** (coordinator, DDL, catchup, switchover, node lifecycle) | `cluster-formation-architecture.md` §Pair Subsystem; `dsm-cluster-formation.md` rows I–O | **Full** — roles, transitions, DDL flow, switchover all specced |
| **WritePath / DdlPath abstractions** | `cluster-formation-architecture.md` §Data Flow; `dsm-cluster-formation.md` rows G,H | **Full** — per-mode routing tables documented |
| **ClusterCoordinator** (write, read, batch, NTS, read-repair) | `components.md` §coordinator; `observability-architecture.md` cluster spans | **Partial** — read-repair inline described; NTS write/read paths not separately specced; batch batchlog protocol not specced |
| **Write backpressure `Semaphore(128)`** | `CLAUDE.md` (Raft starvation fix P1) | **Partial** — listed in CLAUDE.md active work; no standalone spec doc |
| **TokenRing + ReplicationStrategy** | `cluster-formation-architecture.md` §Topology; `components.md` §ring | **Partial** — SimpleStrategy and NTS noted; per-DC token placement algorithm not specced |
| **Rebalance** (`compute_rebalance`, `execute_rebalance`) | `implemented/gap-S4-rebalance-data-streaming.md`; `todo/todo-add-node-post-formation.md` | **Partial** — intent and acceptance criteria documented; no architecture spec for the algorithm |
| **Streaming** (sender, receiver, SSTable transfer) | `cluster-formation-architecture.md` §Bootstrap; `implemented/gap-S4-rebalance-data-streaming.md` | **Partial** — high-level; no spec for chunking, ordering, flow control, or error recovery |
| **Hinted Handoff** (`HintStore`, `HintDrain`, delivery) | `todo/todo-hints-topology-change-wrong-node.md`; `fmea-cluster-formation.md` F19 | **Partial** — delivery-to-wrong-node bug documented; no architecture spec for hint lifecycle, capacity, segment format |
| **Repair** (Merkle tree, `diff_trees`) | `fmea-cluster-formation.md` F19; `hazards-cluster-formation.md` §Merkle | **Partial** — hazards documented (bad digest on serialize failure); no repair architecture spec or triggering policy |
| **SystemTableWriter / SystemTableLoader** | `cluster-formation-architecture.md` (referenced); `observability-architecture.md` | **Missing** — no spec for what system tables are written, when, or recovery semantics |
| **Telemetry** (`FerrosaTelemetryLayer`, `TelemetryWriter`) | `observability-architecture.md` | **Full** — span hierarchy, sampling rate, cancel-safety all specced |
| **Index coordination** (`RemoteNodeBackend`) | `remote-index-build-backend.md` | **Full** — backend modes, dispatch protocol, health check all specced |
| **ConsistencyLevel** (quorum math, NTS per-DC) | `components.md` §consistency; `cql.md` | **Partial** — CL values documented; `block_for_dc` (NTS quorum) logic not separately specced |
| **ClusterConfig** (election timeouts, RF defaults) | `CLAUDE.md` (Raft starvation fix P2 — election timeout change) | **Partial** — timeout values in CLAUDE.md; no config reference spec |
| **`ClusterDdlForwardHandler` stale-primary spam** | `todo/bug-ddl-forward-handler-stale-leader-spam.md` | **Full** — root cause, proposed fix, acceptance criteria all documented |
| **Degraded modes** (Pair disconnect, Cluster quorum loss) | `cluster-formation-architecture.md` §Degraded; `cluster-formation-state-machine.md` | **Partial** — target states defined; code resets to Standalone on disconnect (Gap #7 in arch spec — unimplemented) |
| **Formation hardcoded RF=1** | `todo/todo-formation-hardcoded-rf1-cl-one.md` | **Full** — bug described, fix approach documented |
| **Add-node post-formation** (no data streaming) | `todo/todo-add-node-post-formation.md` | **Full** — gap and expected Cassandra-compatible behaviour documented |
| **Hints delivered to wrong node on topology change** | `todo/todo-hints-topology-change-wrong-node.md` | **Full** — root cause and fix approach documented |
| **5+ node scaling** | `todo/todo-5plus-node-scaling.md` | **Full** — known gaps enumerated |
| **StoreView phantom-ID desync fix** (ferrosa-storage) | `in-process/bug-read-path-memory-growth-bloats-coordinator.md` (mentions "phantom-ID") | **Partial** — mentioned as part of the memory-growth investigation; no dedicated spec entry |

---

## 3. Gaps (Prioritised)

### P0 — Data-correctness risk, no spec

**G-P0-1: Hinted Handoff Architecture Spec missing.**
`HintStore` has per-peer segment files, capacity eviction (`evict_oldest`), and a `needs_repair` flag — but no spec covers the lifecycle (when hints are created, when delivered, what happens on capacity overflow, how `needs_repair` feeds into anti-entropy). The bug in `todo/todo-hints-topology-change-wrong-node.md` is documented, but the fix requires understanding the architecture that doesn't exist on paper.
_Action: Create `specs/hints-architecture.md`._

**G-P0-2: Repair triggering policy not specced.**
`repair/mod.rs` has `build_tree_for_range` and `diff_trees`, but there is no spec for when repair is triggered, which node initiates, what token ranges are repaired, or how the result feeds back into streaming. FMEA F19 flags hint overflow triggering repair, but the trigger mechanism is not implemented or specced.
_Action: Add repair triggering section to a new `specs/anti-entropy-architecture.md`._

### P1 — Functional gap, partially specced

**G-P1-1: `DeploymentMode` is 3-variant; spec requires `FormationState` with Forming + Degraded.**
`mode.rs` has `Standalone | Pair | Cluster`. The architecture spec (ADR-2) and state-machine spec both require `Forming` and `Degraded_{Pair,Cluster}` variants. Degraded handling currently resets to Standalone, losing peer context. This is Gap #2 and #7 in `cluster-formation-architecture.md`. No implementation has landed.
_Action: This is an existing tracked gap; create `specs/in-process/gap-formation-state-machine.md` to track implementation progress._

**G-P1-2: Batchlog protocol not specced.**
`coordinator/batch.rs` (703 lines) implements the Cassandra batchlog two-phase protocol (write to batchlog replicas → replicate mutations → delete from batchlog). No spec covers the protocol, failure modes (what if batchlog delete fails), or consistency guarantees. `project-plan-gap-closure.md` Sprint 1 notes that batchlog handlers need registration — registration is done but the protocol itself is unspecced.
_Action: Add `specs/batchlog-coordinator.md`._

**G-P1-3: SystemTableWriter / SystemTableLoader have no spec.**
These two files govern durable persistence of schema + topology to local SSTables after each Raft apply. There is no spec for what rows are written, in what order, what happens if a write fails mid-apply, or how `SystemTableLoader` resolves conflicts on restart. This is a potential split-brain recovery risk.
_Action: Add a section to `cluster-formation-architecture.md` or create `specs/system-table-persistence.md`._

### P2 — Quality / observability gap

**G-P2-1: Streaming protocol (chunking, error recovery) not specced.**
`streaming/sender.rs` and `streaming/receiver.rs` implement row-level and SSTable-file streaming with chunk/start/end framing. There is no spec for chunk sizing, back-pressure, what happens if the receiver crashes mid-stream, or how the sender detects completion. `execute_rebalance` calls `send_stream` but recovery on partial failure is not documented.

**G-P2-2: `ClusterConfig` has no reference spec.**
Election timeouts, hint capacity, write concurrency limit, and seed discovery parameters are all in `config.rs` but are not documented in any operator-facing or design spec. The Raft starvation fix changed election timeouts (1000/2000 → 3000/6000 ms) and is noted only in `CLAUDE.md`.

**G-P2-3: NTS per-DC quorum arithmetic not specced.**
`coordinate_write_nts` and `coordinate_read_nts` implement per-DC CL (`LOCAL_QUORUM`, `EACH_QUORUM`). The implementation exists but `block_for_dc` semantics and DC assignment (see `todo/todo-multi-dc-node-dc-assignment.md`) are not covered in a single coherent spec. Reading the code is required to understand the quorum contract.

---

## 4. Today's Fixes

### 4a. Phantom-ID `StoreView` Desync Fix (ferrosa-storage)

**Landed:** 2026-04-19. Location: `ferrosa-storage/src/store.rs:sstable_metadata` (line 1061+).

**Spec status:** Not independently documented. The bug is mentioned in `specs/in-process/bug-read-path-memory-growth-bloats-coordinator.md` (2026-04-19 update) as part of a batch of fixes deployed to a cluster image: "phantom-ID, invariant checks, truncated-SSTable quarantine". There is no dedicated bug spec or architecture note explaining the invariant (`sstable_ids` parallel vector must be co-updated with `sstables`). The existing inline comment at `store.rs:78–101` is the only documentation.

**Verdict:** Described only in passing within an unrelated bug report. Should have a dedicated entry in `specs/todo/` or `specs/archive/` so the fix is traceable.

### 4b. `ClusterDdlForwardHandler` Stale-Primary Spam

**Spec:** `specs/todo/bug-ddl-forward-handler-stale-leader-spam.md` — filed 2026-04-19, updated 2026-04-20.

**Status:** Fully specced as P2 bug; not yet fixed. The spec correctly identifies both the handler side (`ddl_path.rs:509` — log `ERROR` on `NotLeader`) and the sender side (`ddl_path.rs:313` — stale pair-mode primary cache used after cluster transition). Two independent fixes are proposed. Acceptance criteria include a unit test for the redirect path.

**Verdict:** Well-documented; no action needed on the spec itself. Implementation is pending.

---

## 5. Recommendations

**Rec 1 (P0): Create `specs/hints-architecture.md`.**
Cover: hint file format, segment rotation, per-peer capacity enforcement, delivery ordering, `needs_repair` flag semantics, and the token-remapping fix needed by `todo/todo-hints-topology-change-wrong-node.md`. This unblocks both the topology-change fix and the FMEA F19 repair trigger.

**Rec 2 (P0): Create `specs/anti-entropy-architecture.md`.**
Define when anti-entropy repair is triggered (hint overflow, manual operator command, scheduled), which node initiates for a given token range, how Merkle tree exchange proceeds over the internode protocol, and how the repair result feeds back into `StreamSender`. Currently the Merkle primitives exist with no caller specified.

**Rec 3 (P1): Create `specs/batchlog-coordinator.md`.**
Document the two-phase batchlog protocol: batchlog replica selection, write ordering, mutation replication, cleanup on success, and recovery on partial failure. Include the failure mode where batchlog delete succeeds but mutation replication fails (double-apply risk on replay).

**Rec 4 (P1): Add `specs/in-process/gap-formation-state-machine.md`.**
Track implementation of `FormationState` (Forming, Degraded variants) as a discrete work item. The spec design is complete in `cluster-formation-architecture.md`; what is missing is a task file linking to the specific files that need changing (`mode.rs`, `controller/peer_events.rs`, `controller/cluster.rs`) and acceptance criteria for each state transition.

**Rec 5 (P2): Add a dedicated bug spec for the `StoreView` phantom-ID fix.**
Move the fix description out of the memory-growth investigation document and into its own `specs/archive/bug-storeview-phantom-id-desync.md`. Include: the invariant violated, how it manifested (phantom SSTable paths), the detection mechanism added (panic on length mismatch), and regression test location. Keeps the storage fix independently traceable.
