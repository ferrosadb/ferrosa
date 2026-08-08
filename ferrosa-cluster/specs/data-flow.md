---
crate: ferrosa-cluster
doc: data-flow
last_updated: 2026-08-07
---

# ferrosa-cluster — Data Flow

Three end-to-end flows: **formation endpoint discovery**, a
**tunable-consistency write** (the eventually-consistent data path), and an
**Accord transaction** (the strict-serializable path). They share the
`TokenRing` for replica selection and run over `ferrosa-net` peers.

> Note: identifiers below are written without raw angle brackets so the diagrams
> render — e.g. `Option Partition` rather than the generic-parameter form.

## 1. Formation endpoint discovery

An inbound TCP source port is ephemeral and is never a valid reverse-dial
target. The handshake's advertised internode host/port is authoritative when
present; observed IP plus the receiver's local internode port is a compatibility
fallback for older peers. The selected endpoint feeds both the outbound pool
and `connected_peers`, whose addresses are carried in `ClusterInvite`.

```mermaid
flowchart LR
    IN["Inbound socket + handshake"] --> ADV{"advertised internode endpoint usable?"}
    ADV -->|yes| CANON["resolve advertised host/port"]
    ADV -->|no| FALLBACK["observed IP + local internode port"]
    CANON --> TRACK["reverse pool + connected_peers"]
    FALLBACK --> TRACK
    TRACK --> INVITE["ClusterInvite peer targets"]
    INVITE --> RAFT["all members enter one Raft group"]
```

## 2. Tunable-CL write coordination

A front-end (e.g. `ferrosa-cql`) hands a mutation to
`ClusterCoordinator::coordinate_write*`. The coordinator gates on the write
semaphore (backpressure protecting Raft), resolves replicas, fans out, and
returns once `cl.block_for(rf)` acks arrive — hinting failed replicas.

```mermaid
flowchart TD
    FE["Front-end (ferrosa-cql / graph / flight)"] -->|coordinate_write key,row,ts,cl,rf| CO["ClusterCoordinator"]
    CO --> SEM{"acquire write_semaphore permit?<br/>cap WRITE_CONCURRENCY_LIMIT = 128"}
    SEM -->|no permit| UNAVAIL["return Unavailable<br/>(fail-loud backpressure)"]
    SEM -->|permit acquired| RING["TokenRing.replicas token,rf<br/>(SimpleStrategy / NTS)"]
    RING --> THRESH{"replicas.len gte cl.block_for rf ?"}
    THRESH -->|no| UNAVAIL2["return Unavailable"]
    THRESH -->|yes| FAN["fan out: local write + remote MutationForward"]

    FAN --> L["local StorageEngine.apply"]
    FAN --> R1["replica 2 (remote peer)"]
    FAN --> R2["replica 3 (remote peer)"]

    L --> COLLECT["collect acks"]
    R1 --> COLLECT
    R2 --> COLLECT

    COLLECT --> MET{"acks gte block_for rf ?"}
    MET -->|no, timed out| WT["return WriteTimeout"]
    MET -->|yes| HINT["store hints for failed replicas<br/>(HintStore, byte-budget capped)"]
    HINT --> DRAIN["spawn post-quorum hint drain<br/>(detached, holds permit)"]
    DRAIN --> OK["return Ok to front-end"]

    HINT -.->|no hint store| WARN["log ERROR: divergent replicas<br/>need anti-entropy repair"]
```

Key points: the permit is held by the post-quorum drain task so stragglers are
captured as hints without blocking the client. NTS / `LOCAL_QUORUM` /
`EACH_QUORUM` variants compute per-DC ack thresholds via `block_for_dc`. The
symmetric read path issues one full read plus `block_for - 1` digest reads, then
on digest mismatch fail-loud re-fetches the newest copy and repairs stale
replicas inline before returning.

## 3. Accord transaction (strict-serializable)

`AccordCoordinator` runs the EPaxos-family protocol across the shards a
transaction touches. The common case commits in one round trip (fast path); a
conflict forces the Accept phase (slow path). Apply waits on conflicting
dependencies before writing to storage at the agreed HLC timestamp.

```mermaid
sequenceDiagram
    participant FE as Front-end (LWT / multi-key txn)
    participant CO as AccordCoordinator
    participant HLC as HybridLogicalClock
    participant R as Replicas (per shard)
    participant DW as DepWaitGraph
    participant ST as StorageApplier

    FE->>CO: begin txn (keys, mutation)
    CO->>HLC: now -> t0
    CO->>CO: TxnId from t0, node

    Note over CO,R: PreAccept phase
    CO->>R: PreAccept t0, keys
    R->>R: scan ConflictIndex -> deps, propose t gte t0
    R-->>CO: PreAcceptOk t, deps

    alt fast quorum agrees t == t0 and identical deps
        Note over CO,R: Fast path (1 RTT) — FastPathCommit
    else any t gt t0 or differing deps
        Note over CO,R: Slow path — Accept phase (2 RTT)
        CO->>R: Accept t_merged max, deps_merged union, fresh ballot
        R-->>CO: AcceptOk (or NACK on higher ballot)
    end

    CO->>R: Commit final t, deps (fire-and-forget)

    Note over CO,R: Apply phase
    CO->>R: Apply t, deps, mutation bytes
    R->>DW: wait until all deps reach Applied
    DW-->>R: deps applied (cycle? abort highest t0)
    R->>ST: apply mutation at timestamp t (idempotent)
    ST-->>R: persisted
    R-->>CO: applied
    CO-->>FE: txn result

    Note over CO,R: Coordinator failure -> RecoveryCoordinator<br/>re-proposes by highest accepted_ballot (Paxos rule)
```

Fast/slow quorum sizes: `slow = rf/2 + 1`; `fast = (3f+1)/2 + 1` where
`f = rf - slow`. Cross-shard transactions dispatch PreAccept to every
participating shard and abort atomically if any shard fails. Cross-DC apply routes
`AccordApply` entries through per-DC Raft, buffered by HLC in a reorder buffer and
deduplicated by an applied-txn ledger (ADR-015).

> Correctness caveat: both flows are validated by deterministic in-crate tests
> only. The fast/slow-path, recovery, and dep-wait logic have property + scenario
> coverage, but there is **no external Jepsen run** confirming these flows hold
> under real partitions, clock skew, and disk faults — see [fmea.md](fmea.md)
> CL-1 and [../specs/todo/jepsen-e2e-test-plan.md](../specs/todo/jepsen-e2e-test-plan.md).
