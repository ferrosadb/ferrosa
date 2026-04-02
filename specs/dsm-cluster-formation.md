# DSM Analysis: Cluster Formation Subsystem

> Last updated: 2026-04-01
> Scope: 32 modules across ferrosa-cluster and ferrosa-net, focused on cluster formation lifecycle

## Module Inventory

| ID | Module | Lines | Role |
|----|--------|------:|------|
| A | `controller` | 1977 | Orchestrates all mode transitions, holds WritePath/DdlPath/State |
| B | `mode` | 69 | DeploymentMode enum (Standalone/Pair/Cluster) |
| C | `state` | 167 | PairClusterState, RaftClusterState, SingleNodeClusterState |
| D | `config` | 166 | ClusterConfig (seeds, data_dir, mode hint) |
| E | `error` | 153 | ClusterError, Result type alias |
| F | `consistency` | 243 | ConsistencyLevel enum + block_for logic |
| G | `ddl_path` | 864 | DdlPath enum routing (Direct/Pair/Cluster/Unavailable) |
| H | `write_path` | 220 | WritePath enum routing (Direct/Pair/Cluster/Unavailable) |
| I | `pair/mod` | 107 | PairRole, PairState types |
| J | `pair/coordinator` | 543 | PairCoordinator — write replication in pair mode |
| K | `pair/ddl` | 1100 | DdlCoordinator, DdlOperation — DDL in pair mode |
| L | `pair/catchup` | 205 | Catch-up replication RPC handler |
| M | `pair/switchover` | 116 | Role swap initiation + handler |
| N | `pair/node` | 313 | PairNode — lifecycle management, networking setup |
| O | `pair/handler` | 65 | PairWriteForwardHandler RPC |
| P | `raft/mod` | 452 | FerrosRaft type, RaftCommand, RaftOp, NodeInfo, NodeState |
| Q | `raft/handlers` | 1060 | Inbound Raft RPC handlers |
| R | `raft/state_machine` | 2319 | FerrosStateMachine, RaftState — applies committed ops |
| S | `raft/log_store` | 424 | SledLogStore — persistent Raft log |
| T | `raft/network` | 416 | FerrosRaftNetworkFactory — outbound Raft RPCs |
| U | `ring/mod` | 762 | TokenRing — consistent hash ring |
| V | `ring/strategy` | 187 | ReplicationStrategy |
| W | `coordinator/mod` | 530 | ClusterCoordinator, MutationForwardHandler, RepairWriteHandler |
| X | `coordinator/read` | 1711 | Read path with read-repair, digest comparison |
| Y | `coordinator/write` | 1057 | Cluster-mode write coordination with CL enforcement |
| Z | `coordinator/batch` | 669 | Batch log coordination |
| c | `net/peer` | 460 | PeerManager, PeerEventListener |
| d | `net/pool` | 409 | PriorityPool — connection pool |
| e | `net/message` | 588 | Message enum — wire protocol messages |
| f | `net/rpc/*` | 1041 | HandlerRegistry, RpcServer, RpcClient, RpcHandler trait |

## Coupling Metrics

| Module | Fan-Out | Fan-In | Coupling (FI×FO) | Lines | Instability |
|--------|--------:|-------:|------------------:|------:|------------:|
| **controller** | 22 | 0 | 0 | 1977 | 1.00 |
| **net/peer** | 5 | 10 | **50** | 460 | 0.33 |
| **pair/coordinator** | 4 | 8 | **32** | 543 | 0.33 |
| **net/message** | 2 | 14 | **28** | 588 | 0.13 |
| **net/rpc** | 2 | 10 | **20** | 1041 | 0.17 |
| **raft/mod** | 1 | 9 | **9** | 452 | 0.10 |
| **ring/mod** | 1 | 6 | **6** | 762 | 0.14 |
| **error** | 0 | 12 | 0 | 153 | 0.00 |
| **consistency** | 0 | 6 | 0 | 243 | 0.00 |
| **pair/mod** | 0 | 8 | 0 | 107 | 0.00 |

## God Module: controller.rs

Fan-out of **22** — imports from essentially every module in the subsystem. 1977 lines. Orchestrates standalone init, pair formation, cluster formation, DDL/write path swapping, hint store, and schema replay. While fan-in is 0 (consumed only by the binary), the sheer dependency count makes this the highest-risk module for change propagation.

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
| `net/message` (Message enum) | 14 | 14 | **44%** |
| `raft/mod` (NodeInfo/RaftOp) | 9 | 14 | **44%** |
| `pair/coordinator` (encode/decode) | 8 | 14 | **44%** |
| `error` (ClusterError) | 12 | 12 | **38%** |
| `pair/mod` (PairRole/PairState) | 8 | 12 | **38%** |
| `net/peer` (PeerManager API) | 10 | 10 | **31%** |
| `ring/mod` (TokenRing) | 6 | 10 | **31%** |

**Average propagation cost: ~34%** — a change to any module affects roughly one-third of the subsystem. Target: <20%.

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

### 3. Split controller.rs into phase modules (MEDIUM)

Decompose into:
- `controller/mod.rs` — struct, public API, shared state (~300 lines)
- `controller/pair.rs` — pair formation, switchover, promotion
- `controller/cluster.rs` — Raft init, schema replay
- `controller/recovery.rs` — degraded handling, fallback

**Risk:** Medium — async borrow handling.

### 4. Split raft/state_machine.rs (MEDIUM)

At 2319 lines, decompose into:
- `raft/state_machine/core.rs` — RaftState, snapshot
- `raft/state_machine/schema_ops.rs` — DDL apply
- `raft/state_machine/node_ops.rs` — JoinNode, LeaveNode, tokens

### 5. Extract select_index_ready_replicas (LOW)

Move from coordinator/read to ring/replica_selection.rs to break L2→L3 violation.

## Summary

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Modules analyzed | 32 | — | — |
| Total lines | 18,393 | — | — |
| Avg propagation cost | ~34% | <20% | **High** |
| Layering violations | 4 | 0 | **Fix** |
| Direct cycles | 1 | 0 | **Fix** |
| God modules | 1 (controller) | 0 | **Decompose** |
| Files >1000 lines | 5 | 0 | **Reduce** |
| Highest fan-out | 22 (controller) | <10 | **Too high** |
