# Cluster Formation Architecture

> Last updated: 2026-04-01
> Status: Draft — implements specs/cluster-formation-state-machine.md
> Scope: Formation state machine, ClusterInvite protocol, degraded modes, decommission

## Overview

Cluster formation is the subsystem that takes a ferrosa node from standalone operation through pair replication to a full Raft-managed cluster. It lives primarily in `ferrosa-cluster` with network transport in `ferrosa-net`.

The current implementation handles Standalone→Pair→Cluster transitions but has 7 known gaps (see [Spec vs Code Gaps](#spec-vs-code-gaps)). This architecture spec defines the target state after those gaps are closed.

## Component Diagram

```mermaid
graph TB
    subgraph "ferrosa-cluster"
        MC[ModeController<br/>controller.rs<br/>1977 lines]
        FSM[FormationState<br/>mode.rs]
        WP[WritePath<br/>write_path.rs]
        DP[DdlPath<br/>ddl_path.rs]
        CSH[ClusterStateHolder<br/>controller.rs]

        subgraph "Pair Subsystem"
            PC[PairCoordinator<br/>pair/coordinator.rs]
            DDL[DdlCoordinator<br/>pair/ddl.rs]
            CU[Catchup<br/>pair/catchup.rs]
            SW[Switchover<br/>pair/switchover.rs]
            PN[PairNode<br/>pair/node.rs]
        end

        subgraph "Raft Subsystem"
            RF[FerrosRaft<br/>raft/mod.rs]
            RSM[RaftStateMachine<br/>raft/state_machine.rs]
            RLS[SledLogStore<br/>raft/log_store.rs]
            RNF[RaftNetworkFactory<br/>raft/network.rs]
            RH[RaftHandlers<br/>raft/handlers.rs]
        end

        subgraph "Topology"
            TR[TokenRing<br/>ring/mod.rs]
            RS[ReplicaStrategy<br/>ring/strategy.rs]
            RB[Rebalance<br/>rebalance.rs]
        end

        CC[ClusterCoordinator<br/>coordinator/mod.rs]
        HS[HintStore<br/>hints/mod.rs]
        HD[HintDelivery<br/>hints/delivery.rs]
        ST[Streaming<br/>streaming/mod.rs]
    end

    subgraph "ferrosa-net"
        PM[PeerManager<br/>peer.rs]
        PP[PriorityPool<br/>pool.rs]
        MSG[Message enum<br/>message.rs]
        RPC[RpcServer<br/>rpc/server.rs]
        HR[HandlerRegistry<br/>rpc/handler.rs]
        SK[SkewTracker<br/>skew.rs]
        DISC[SeedDiscovery<br/>discovery/seeds.rs]
        HS2[Handshake<br/>handshake.rs]
    end

    MC --> FSM
    MC --> WP
    MC --> DP
    MC --> CSH
    MC --> PC
    MC --> DDL
    MC --> RF
    MC --> TR
    MC --> CC
    MC --> HS

    PC --> PM
    DDL --> PM
    CU --> PM
    SW --> PM

    RF --> RSM
    RF --> RLS
    RF --> RNF
    RNF --> PM
    RH --> RF

    CC --> TR
    CC --> PM
    CC --> HS

    PM --> PP
    PP --> RPC
    RPC --> HR
    PM --> SK
```

## State Machine

### Current States (code)

```
DeploymentMode { Standalone, Pair, Cluster }
```

### Target States (spec)

```mermaid
stateDiagram-v2
    [*] --> Standalone: boot (no seeds)

    Standalone --> Pair: 1st peer connects (T1)
    Pair --> Forming: 2nd peer OR ClusterInvite (T2)
    Forming --> Cluster: all peers + Raft leader (T3)
    Cluster --> Cluster: add/remove member (T4/T5)

    Pair --> Degraded_Pair: peer lost (T8a/T8b)
    Degraded_Pair --> Pair: peer reconnects
    Degraded_Pair --> Standalone: operator demotes

    Cluster --> Degraded_Cluster: quorum lost (T6c)
    Degraded_Cluster --> Cluster: quorum restored (T7)

    Forming --> Pair: formation timeout (T2 fallback)
```

### New Types Required

```rust
// Replace DeploymentMode with richer FormationState
pub enum FormationState {
    Standalone,
    Pair { role: PairRole, peer: PeerInfo },
    Forming {
        initiator: Uuid,
        known_peers: Vec<(Uuid, SocketAddr)>,
        connected: BTreeSet<Uuid>,
        deadline: Instant,
    },
    Cluster,
    DegradedPair {
        role: PairRole,
        peer: PeerInfo,      // remembered peer (disconnected)
        promoted: bool,       // operator ran ferrosa-ctl promote
    },
    DegradedCluster {
        missing: Vec<Uuid>,   // unreachable members
    },
}
```

## Data Flow: Cluster Formation (3 nodes, hub-and-spoke)

```mermaid
sequenceDiagram
    participant N1 as Node1 (seed)
    participant N2 as Node2 (joiner)
    participant N3 as Node3 (joiner)

    Note over N1,N3: Phase 1: Pair Formation
    N2->>N1: TCP connect (seed addr)
    N1->>N1: Standalone → Pair (Primary)
    N2->>N2: Standalone → Pair (Secondary)
    N1-->>N2: reverse pool + PairSchemaSync

    Note over N1,N3: Phase 2: Forming
    N3->>N1: TCP connect (seed addr)
    N1->>N1: Pair → Forming (2nd peer seen)
    N1->>N2: ClusterInvite {peers: [N1, N2, N3]}
    N1->>N3: ClusterInvite {peers: [N1, N2, N3]}

    Note over N1,N3: Phase 3: Mesh Completion
    N2->>N3: TCP connect (from invite)
    N3->>N2: TCP connect (from invite)
    N2->>N2: Pair → Forming
    N3->>N3: Pair → Forming

    Note over N1,N3: Phase 4: Handler Registration + ClusterInvite
    N1->>N1: Register Raft handlers via LazyRaft (before async init)
    N2->>N2: Register Raft handlers via LazyRaft
    N3->>N3: Register Raft handlers via LazyRaft
    N1->>N2: ClusterInvite on Data lane (10-attempt retry)
    N1->>N3: ClusterInvite on Data lane (10-attempt retry)

    Note over N1,N3: Phase 5: Raft Init
    N1->>N1: Raft::initialize([N1, N2, N3])
    N2->>N2: Raft::initialize([N1, N2, N3])
    N3->>N3: Raft::initialize([N1, N2, N3])
    N1-->>N2: RaftVote (N1 wins — was primary)
    N1-->>N3: RaftVote
    N1->>N1: Forming → Cluster (leader)

    Note over N1,N3: Phase 6: Bootstrap (A/B/C)
    N1->>N2: Phase A: schema replay via Raft
    N1->>N3: Phase A: schema replay via Raft
    Note over N1,N3: Phase B: all nodes stream local data to new token owners
    N1->>N1: Phase C: propose Joining → Normal for all nodes
    N2->>N2: Forming → Cluster (follower)
    N3->>N3: Forming → Cluster (follower)
```

## Data Flow: Write Path by Mode

```mermaid
flowchart LR
    subgraph Standalone
        W1[CQL Write] --> D1[Direct: local engine]
    end
    subgraph Pair
        W2[CQL Write] --> PC2{PairCoordinator}
        PC2 -->|Primary| L2[Local apply]
        L2 --> R2[Replicate to secondary]
        PC2 -->|Secondary| F2[Forward to primary]
    end
    subgraph Cluster
        W3[CQL Write] --> CC3[ClusterCoordinator]
        CC3 --> TR3[TokenRing: find replicas]
        TR3 --> FO3[Fan-out to RF replicas]
        FO3 --> CL3{CL satisfied?}
        CL3 -->|Yes| OK3[Success]
        CL3 -->|No + hints| HH3[Hinted Handoff]
    end
```

## Data Flow: DDL Path by Mode

| Mode | DDL Path | Consistency | Risk |
|------|----------|-------------|------|
| Standalone | Direct (local schema) | N/A | None |
| Pair | DdlCoordinator → sync to secondary | Best-effort | Secondary may miss DDL; catches up via PairSchemaSync |
| Forming | Direct (local only) | **Unreplicated** | DDL during this window only on initiator. Mitigated by Raft schema replay after leader election |
| Cluster | Raft consensus | Linearizable | None — full consensus |

## Component Responsibilities

### ModeController (`controller.rs`)

The orchestrator. Implements `PeerEventListener` (from ferrosa-net) and `InboundPeerCallback`. On peer events, evaluates the current `FormationState` and triggers transitions.

**Key methods:**
| Method | Lines | Trigger | Effect |
|--------|-------|---------|--------|
| `on_peer_connected` | 1208-1249 | PeerManager callback | Evaluates mode, dispatches to transition_to_* |
| `on_peer_disconnected` | 1251-1266 | PeerManager callback | Pair→Degraded, Cluster→Raft handles |
| `transition_to_pair` | 536-730 | 1st peer | Create PairCoordinator, swap WritePath/DdlPath |
| `transition_to_forming` | NEW | 2nd peer or ClusterInvite | Broadcast ClusterInvite, start mesh formation |
| `transition_to_cluster` | 741-1105 | All peers connected + Raft ready | Register handlers via LazyRaft, deliver ClusterInvite on Data lane, init Raft, TokenRing, ClusterCoordinator |
| `force_promote` | 489-504 | Operator command | Emergency: Degraded_Pair → Standalone primary |
| `switchover` | 509-534 | Operator command | Swap Primary/Secondary roles |
| `trigger_cluster_join` | 1112-1192 | Peer connects in Cluster mode | Propose JoinNode + AssignTokens via Raft |

### ClusterInvite Protocol

```rust
// ferrosa-net/src/message.rs
Message::ClusterInvite {
    initiator: Uuid,
    peers: Vec<(Uuid, SocketAddr)>,
}
Message::ClusterInviteAck {
    host_id: Uuid,
}
```

**Delivery:** ClusterInvite is sent on the **Data lane** (not the Raft lane) with
a 10-attempt retry loop, **synchronously before Raft init starts** (commits
808b72b, 30768c0). This ensures the invite is delivered even when Raft handlers
are not yet registered on the receiving node.

**Handler logic:**
1. ClusterInvite handler triggers cluster transition on receiving nodes (commit ba7599a)
2. For each peer in invite not already connected: initiate `PriorityPool::connect()`
3. If connected_peers >= 2: transition to Forming
4. Re-broadcast ClusterInvite to newly connected peers (propagation guarantee)

### Bootstrap Phases (all nodes)

After Raft leader election, bootstrap streaming runs on ALL nodes in 3 phases:

- **Phase A — Schema convergence:** Leader replays local schema state through Raft.
  Non-leaders replay schema via `PairDdlForward` to ensure DDL applied during
  the Pair/Direct window is captured on all nodes.
- **Phase B — Data streaming:** ALL nodes stream their local data to the new
  token owners based on the initial token ring assignment.
- **Phase C — Leader promotes:** Leader proposes state changes from `Joining`
  to `Normal` for all nodes via Raft.

### Role Assignment

**Current code:** `PairRole::elect()` — higher UUID wins (deterministic but arbitrary).

**Target (spec):** Connection direction determines role:
- Node receiving connection = **Primary** (seed, has data)
- Node initiating connection = **Secondary** (joiner)
- Deterministic from TCP connection direction — no UUID comparison needed
- Simpler, no race conditions, naturally maps to data authority

### Degraded Modes

**Current code:** Pair disconnect → mode reset to Standalone (loses pair context).

**Target:**

| State | Reads | Writes | DDL | Recovery |
|-------|-------|--------|-----|----------|
| Degraded_Pair (secondary lost) | Full | Full (unreplicated) | Full (local) | Auto on reconnect |
| Degraded_Pair (primary lost) | Stale only | Unavailable | Unavailable | Operator promote or wait |
| Degraded_Cluster (quorum intact) | Full | CL=QUORUM ok, CL=ALL fails | Full (Raft ok) | Auto on reconnect |
| Degraded_Cluster (quorum lost) | Stale only (CL=ONE) | Unavailable | Unavailable | Auto if nodes return; operator force-reconfig if permanent |

## Spec vs Code Gaps

| # | Gap | Severity | Sprint |
|---|-----|----------|--------|
| 1 | Role election: UUID comparison vs connection direction | Medium | 1 |
| 2 | Missing Forming/Degraded states in DeploymentMode | High | 1 |
| 3 | No ClusterInvite message — root cause of hub-and-spoke bug | Critical | 1 |
| 4 | No Forming→Pair fallback timeout | Medium | 1 |
| 5 | No decommission data streaming | High | 2 |
| 6 | Approval race in trigger_cluster_join | Medium | 2 |
| 7 | Degraded handling resets to Standalone (loses pair context) | High | 1 |

## Architectural Decision Records

### ADR-1: Connection Direction for Role Assignment

**Decision:** Replace UUID-based `PairRole::elect()` with connection-direction-based role assignment.

**Context:** The seed node (which receives connections) is the data authority. Making it Primary by virtue of receiving the connection is simpler and more intuitive than UUID comparison.

**Consequences:**
- `PairRole::elect()` removed
- `on_peer_connected` receives `is_inbound: bool` from `InboundPeerCallback`
- Inbound connection → this node is Primary; Outbound → this node is Secondary
- Force-promote flag still overrides (operator authority)

### ADR-2: FormationState Replaces DeploymentMode

**Decision:** Replace 3-variant `DeploymentMode` with richer `FormationState` that tracks degraded modes and forming state.

**Context:** Current code loses pair context on disconnect (resets to Standalone). The Forming state is needed as an intermediate between Pair and Cluster to allow mesh formation before Raft init.

**Consequences:**
- `mode.rs` grows from 69 lines to ~150
- `can_transition_to()` becomes a proper state machine with all valid transitions
- Degraded states preserve peer context for automatic recovery
- `ModeController` fields that track pair context separately (`pair_context`, `force_promoted`) can be moved into `FormationState` variants

### ADR-3: ClusterInvite with Propagation

**Decision:** Add `ClusterInvite` message type with mandatory re-broadcast to newly discovered peers. Deliver on Data lane with 10-attempt retry, synchronously before Raft init.

**Context:** In hub-and-spoke topology (single seed), non-seed nodes never discover each other. The invite propagates the full peer list transitively. Sending on the Raft lane failed because Raft handlers may not be registered on the receiving node yet (commit 808b72b). Delivering synchronously before Raft init (commit 30768c0) ensures the invite arrives before any Raft traffic.

**Consequences:**
- `MsgType::ClusterInvite` and `MsgType::ClusterInviteAck` in ferrosa-net codec
- Handler registered in ModeController; triggers cluster transition on receiving nodes (commit ba7599a)
- Sent on Data lane (not Raft lane) with 10-attempt retry loop
- Delivered synchronously before Raft init starts
- Convergence: all nodes eventually know all peers (bounded by peer count)
- DoS risk: attacker could send large peer lists -> mitigate with max_peers config + cluster name validation
