# DSM Analysis: Cluster Formation Subsystem

> Last updated: 2026-04-02
> Scope: 38 modules across ferrosa-cluster, ferrosa-net, and ferrosa-cql, focused on cluster formation lifecycle

## Module Inventory

| ID | Module | Lines | Role |
|----|--------|------:|------|
| A0 | `controller/mod` | 471 | ModeController struct, public API, shared state (decomposed from controller.rs) |
| A1 | `controller/cluster` | 880 | Raft init, bootstrap streaming (all-node), schema replay |
| A2 | `controller/pair` | 249 | Pair formation, switchover, promotion |
| A3 | `controller/membership` | 322 | Membership changes, node join/leave |
| A4 | `controller/operator` | 94 | Operator-facing API (status, drain) |
| A5 | `controller/peer_events` | 200 | PeerEventListener implementation |
| A6 | `controller/token` | 52 | Token generation utilities |
| B | `mode` | 210 | DeploymentMode enum (Standalone/Pair/Cluster) |
| C | `state` | 425 | PairClusterState, RaftClusterState (+PeerManager CQL broadcast lookup) |
| D | `config` | 171 | ClusterConfig (seeds, data_dir, mode hint) |
| E | `error` | 153 | ClusterError, Result type alias |
| F | `consistency` | 243 | ConsistencyLevel enum + block_for logic |
| G | `ddl_path` | 887 | DdlPath enum routing (Direct/Pair/Cluster/Unavailable) |
| H | `write_path` | 221 | WritePath enum routing (Direct/Pair/Cluster/Unavailable) |
| I | `pair/mod` | 139 | PairRole, PairState types |
| J | `pair/coordinator` | 544 | PairCoordinator — write replication in pair mode |
| K | `pair/ddl` | 1100 | DdlCoordinator, DdlOperation — DDL in pair mode |
| L | `pair/catchup` | 205 | Catch-up replication RPC handler |
| M | `pair/switchover` | 116 | Role swap initiation + handler |
| N | `pair/node` | 315 | PairNode — lifecycle management, networking setup |
| O | `pair/handler` | 65 | PairWriteForwardHandler RPC |
| P | `raft/mod` | 463 | FerrosRaft type, RaftCommand, RaftOp, NodeInfo, NodeState |
| Q | `raft/handlers` | 1109 | Inbound Raft RPC handlers |
| R | `raft/state_machine` | 2363 | FerrosStateMachine, RaftState — applies committed ops |
| S | `raft/log_store` | 424 | SledLogStore — persistent Raft log |
| T | `raft/network` | 416 | FerrosRaftNetworkFactory — outbound Raft RPCs |
| U | `ring/mod` | 782 | TokenRing — consistent hash ring |
| V | `ring/strategy` | 187 | ReplicationStrategy |
| W | `coordinator/mod` | 532 | ClusterCoordinator, MutationForwardHandler, RepairWriteHandler |
| X | `coordinator/read` | 1837 | Read path with read-repair, digest comparison |
| Y | `coordinator/write` | 1062 | Cluster-mode write coordination with CL enforcement |
| Z | `coordinator/batch` | 671 | Batch log coordination |
| c | `net/peer` | 492 | PeerManager, PeerEventListener, peer_cql_broadcasts map |
| d | `net/pool` | 418 | PriorityPool — connection pool |
| e | `net/message` | 807 | Message enum — wire protocol messages (+CQL broadcast in handshake/ack) |
| f | `net/rpc/*` | 1093 | HandlerRegistry, RpcServer, RpcClient, RpcHandler trait |
| g | `net/handshake` | 428 | Internode handshake protocol (+CQL broadcast exchange) |
| h | `cql/server` | 806 | CQL native protocol server, connection management (+RAII slot guard) |

## Coupling Metrics

| Module | Fan-Out | Fan-In | Coupling (FI×FO) | Lines | Instability | Delta |
|--------|--------:|-------:|------------------:|------:|------------:|-------|
| **controller/cluster** | 18 | 1 | **18** | 880 | 0.95 | +134 LOC, +all-node streaming |
| **controller/mod** | 12 | 0 | 0 | 471 | 1.00 | |
| **net/peer** | 5 | 13 | **65** | 492 | 0.28 | +1 FI (state.rs), +32 LOC |
| **state** | 6 | 8 | **48** | 425 | 0.43 | +1 FO (PeerManager), +258 LOC |
| **pair/coordinator** | 4 | 8 | **32** | 544 | 0.33 | |
| **net/message** | 2 | 16 | **32** | 807 | 0.11 | +111 LOC (CQL broadcast msgs) |
| **net/handshake** | 4 | 3 | **12** | 428 | 0.57 | +77 LOC (CQL broadcast exchange) |
| **cql/server** | 3 | 2 | **6** | 806 | 0.60 | +160 LOC (RAII slot guard) |
| **net/rpc** | 2 | 10 | **20** | 1093 | 0.17 | +52 LOC |
| **raft/mod** | 1 | 9 | **9** | 463 | 0.10 | |
| **raft/handlers** | 4 | 3 | **12** | 1109 | 0.57 | +48 LOC |
| **ring/mod** | 1 | 6 | **6** | 782 | 0.14 | |
| **error** | 0 | 12 | 0 | 153 | 0.00 | |
| **consistency** | 0 | 6 | 0 | 243 | 0.00 | |
| **pair/mod** | 0 | 8 | 0 | 139 | 0.00 | |

## Former God Module: controller.rs (Decomposed)

The original 1977-line `controller.rs` has been decomposed into 7 sub-modules (A0–A6, totaling 2268 lines). The largest piece, `controller/cluster` (880 lines, fan-out 18), carries most of the original complexity: Raft init, all-node bootstrap streaming, schema replay. It remains the highest fan-out module but is now scoped to cluster formation only.

`controller/mod` (471 lines, fan-out 12) is the public API surface and shared state holder. Combined fan-out across the two largest controller modules is 30 (vs. 22 for the monolith), reflecting the new all-node streaming and CQL broadcast wiring. However, individual modules are more focused and testable.

## Dependency Cycles

### Cycle 1: ring → raft/mod (type dependency)

`ring/mod.rs` depends on `raft::NodeInfo`, `raft::Token`, `raft::NodeState`. These are cluster-wide data types misplaced in the raft module.

### Cycle 2: coordinator → pair/coordinator (cross-mode coupling)

`coordinator/mod.rs`, `coordinator/read.rs`, `coordinator/write.rs` all use `pair::coordinator::encode_mutation` / `decode_mutation`. The cluster-mode coordinator depends on pair-mode serialization functions.

### Cycle 3: raft/state_machine ↔ coordinator/read (knowledge cycle)

Raft state machine calls `coordinator::read::select_index_ready_replicas`. Coordinator depends on raft types and handlers. Bidirectional knowledge between consensus and query layers.

## Propagation Cost

| Change to | Directly affected | Transitively affected | % of subsystem |
|-----------|------------------:|----------------------:|---------------:|
| `net/message` (Message enum) | 14 | 14 | **37%** |
| `raft/mod` (NodeInfo/RaftOp) | 9 | 14 | **37%** |
| `pair/coordinator` (encode/decode) | 8 | 14 | **37%** |
| `net/peer` (PeerManager API) | 11 | 12 | **32%** |
| `error` (ClusterError) | 12 | 12 | **32%** |
| `pair/mod` (PairRole/PairState) | 8 | 12 | **32%** |
| `ring/mod` (TokenRing) | 6 | 10 | **26%** |

**Average propagation cost: ~33%** — marginally improved from ~34% due to the larger module count denominator (38 vs 32), but absolute coupling has increased. The new `state → PeerManager` cross-crate dependency means `net/peer` changes now propagate into `state` and from there into all system.peers consumers. Target remains <20%.

### New coupling path: PeerManager → state → system.peers

`state.rs` (`RaftClusterState`) now holds an `Option<Arc<PeerManager>>` to look up CQL broadcast addresses at query time. This creates a runtime dependency from Layer 2 (state) down to Layer 1 (net/peer), which respects layering but increases `net/peer` fan-in from 12 to 13 and `state` fan-out from 5 to 6. The coupling score for `net/peer` rises to **65** (was 60), making it the highest-coupling module in the subsystem.

## Layering Violations

Intended layering (bottom to top):

```
Layer 0 (Foundation):    error, consistency, mode, config
Layer 1 (Types):         net/message, net/rpc, net/pool, net/peer, ring/*, pair/mod
Layer 2 (Engines):       raft/*, pair/coordinator, pair/ddl, pair/catchup, pair/switchover
Layer 3 (Routing):       coordinator/*, ddl_path, write_path, pair/node
Layer 4 (Orchestration): controller
```

| Violation | From → To | Fix |
|-----------|-----------|-----|
| ring/mod (L1) → raft/mod (L2) | Ring uses Raft-defined types | Extract types to L0 |
| coordinator (L3) → pair/coordinator (L2) | Cluster coord uses pair serialization | Extract to shared wire.rs |
| raft/state_machine (L2) → coordinator/read (L3) | SM calls up into coordinator | Extract replica selection to L1 |
| raft/state_machine (L2) → system_table_writer (L3) | SM writes system tables directly | Inject via trait |

## Recommendations

### 1. Extract shared types from raft/mod → types.rs (HIGH)

Move `NodeInfo`, `NodeState`, `Token`, `IndexNodeStatus`, `uuid_to_node_id` to `ferrosa-cluster/src/types.rs`. Breaks ring→raft dependency and clarifies layering.

**Risk:** Low — mechanical move.

### 2. Extract encode_mutation/decode_mutation → wire.rs (HIGH)

Move from `pair/coordinator.rs` to `ferrosa-cluster/src/wire.rs`. Breaks cross-mode coupling.

**Risk:** Low — function relocation.

### 3. Split controller.rs into phase modules — DONE

Completed. Controller decomposed into 7 modules (A0–A6): mod, cluster, pair, membership, operator, peer_events, token. The largest piece (`controller/cluster`, 880 lines) remains above the 60-line function guideline internally but is now focused on cluster formation only.

### 4. Split raft/state_machine.rs (MEDIUM)

At 2363 lines (up from 2319), decompose into:
- `raft/state_machine/core.rs` — RaftState, snapshot
- `raft/state_machine/schema_ops.rs` — DDL apply
- `raft/state_machine/node_ops.rs` — JoinNode, LeaveNode, tokens

### 5. Extract select_index_ready_replicas (LOW)

Move from coordinator/read to ring/replica_selection.rs to break L2→L3 violation.

### 6. Reduce PeerManager coupling score (MEDIUM — NEW)

`net/peer` coupling score of 65 is now the highest in the subsystem. The `state.rs` → `PeerManager` dependency for CQL broadcast lookup could be decoupled by introducing a `BroadcastResolver` trait in ferrosa-cluster that PeerManager implements, inverting the dependency direction. This would reduce fan-in on `net/peer` and make `state.rs` testable without PeerManager.

## Summary

| Metric | Value | Target | Status | Delta |
|--------|-------|--------|--------|-------|
| Modules analyzed | 38 | — | — | +6 (controller decomposed) |
| Total lines | 20,952 | — | — | +2,559 |
| Avg propagation cost | ~33% | <20% | **High** | -1pp (denominator growth) |
| Layering violations | 4 | 0 | **Fix** | unchanged |
| Direct cycles | 1 | 0 | **Fix** | unchanged |
| God modules | 0 | 0 | **Done** | controller decomposed |
| Files >1000 lines | 5 | 0 | **Reduce** | unchanged (coordinator/read, pair/ddl, raft/handlers, raft/state_machine, net/rpc) |
| Highest coupling score | 65 (net/peer) | <30 | **Too high** | was 60 |
| Highest fan-out | 18 (controller/cluster) | <10 | **Too high** | was 22 (monolith) |
