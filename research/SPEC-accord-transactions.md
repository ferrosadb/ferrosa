# SPEC: Accord Distributed Transactions for Ferrosa

**Feature:** `ferrosa-accord`
**Status:** Draft
**Authors:** Ferrosa Contributors
**Research basis:** CEP-15 (Accord), Caesar (DSN 2017), Tempo (EuroSys 2021),
Atlas (EuroSys 2020), EPaxos Correctness (Sutra 2019),
EPaxos Revisited (NSDI 2021), ROLL Bounds (DISC 2020)

---

## 1. Overview

This document specifies the design and implementation requirements for grafting the
Accord leaderless distributed transaction protocol onto Ferrosa's existing storage and
cluster stack. The result is a system that provides strict-serializable ACID transactions
across multiple CQL tables and partitions, with no GC pauses, no global leader bottleneck,
and no performance regression on the single-key write hot path.

### 1.1 Goals

- **G1.** Single-key writes execute in 1 RTT at P50 under normal conditions
- **G2.** Multi-key transactions (cross-partition) execute in 1–2 RTTs
- **G3.** Strict serializability for all writes regardless of CL setting
- **G4.** Linearizable reads with no extra RTT in the no-conflict case
- **G5.** Secondary indexes are always consistent within a transaction
- **G6.** Node failure does not cause lost commits or stale reads after recovery
- **G7.** All Jepsen correctness tests pass (bank, long-fork, monotonic, register)
- **G8.** No regression in P50 write latency vs current QUORUM baseline (±15%)

### 1.2 Non-Goals

- Byzantine fault tolerance
- Cross-datacenter transactions in the initial release
- Compatibility with the existing Cassandra LWT (Paxos-based) API

### 1.3 Relation to Existing Code

```
ferrosa-common       ← add: HybridLogicalClock, Timestamp, TxnId types
ferrosa-storage      ← add: ConflictIndex, MemIndex; extend: CommitLog, Memtable
ferrosa-cluster      ← add: AccordStateMachine, ReorderBuffer, ElectorateConfig
ferrosa-net          ← extend: heartbeat with skew/latency measurement
ferrosa-cql          ← extend: parser for BEGIN/COMMIT/ROLLBACK; 2i query planner
ferrosa-index        ← extend: MemIndex integration; eager index build on flush
```

---

## 2. Background and Motivation

Ferrosa currently provides tunable consistency (ONE through ALL) via leaderless
quorum writes, matching Cassandra's semantics. This gives no isolation between
concurrent writes: two clients writing different columns of the same row can
observe each other's partial state. ACID transactions require that either all
writes in a transaction are visible atomically, or none are.

The Accord protocol (Apache Cassandra Enhancement Proposal 15) solves this by
assigning each transaction a globally unique execution timestamp and enforcing that
conflicting transactions execute in timestamp order on all replicas. It is the first
leaderless protocol to achieve strict-serializable isolation with single-RTT fast-path
latency under typical conditions.

Key properties that make Accord suitable for Ferrosa:

- **Leaderless:** no single-node bottleneck; any node can coordinate a transaction
- **Single RTT fast path:** conflicts detected and resolved in one network round trip
  using the Timestamp Reorder Buffer
- **Configurable failure tolerance:** fast-path quorum size is independent of RF,
  allowing fast-path availability even under minority node failure
- **Superset dependency model:** avoids the livelock inherent in Caesar's precise
  dependency tracking

---

## 3. Architecture

### 3.1 Write Path with Accord

All writes are routed through Accord, including simple single-key `INSERT` and
`UPDATE` statements. This eliminates the mixing problem where non-Accord writes
are invisible to Accord's dependency tracking.

```
Client
  │  CQL or BEGIN TRANSACTION block
  ▼
ferrosa-cql parser
  │  Builds AccordTxn with read/write sets
  ▼
ferrosa-cluster: AccordCoordinator
  │
  ├─[leaseholder path]──────────────────────────────────────────────────────┐
  │  If self == leaseholder(token_range):                                   │
  │    local conflict check (no network)                                    │
  │    assign t0 from local HLC                                             │
  │    broadcast PreAccept to other RF-1 replicas                           │
  │    fast-path quorum = local + 1 follower (2/3 for RF=3, f_fast=0)      │
  │    ←── PreAcceptOK ───────────────────────────────────────────────────  │
  │    local Execute (no network)                                           │
  │    broadcast Apply to followers                                         │
  │    ─── ACK to client ────────────────────────────────────────────────── │
  │    Total: 1 RTT                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
  │
  └─[non-leaseholder path]
     forward to leaseholder OR coordinate directly:
       broadcast PreAccept to all electorate members   ──┐  1 RTT
       ←── PreAcceptOK ────────────────────────────────  │
       [fast path]: broadcast Commit                      │
       [slow path]: broadcast Accept ──┐  1 RTT          │
                    ←── AcceptOK ──────┘                  │
                    broadcast Commit                      │
       broadcast Read (for txn reads)  ───────────────── ┘
       ←── ReadOK
       compute result
       broadcast Apply
       ─── ACK to client
       Total: 1 RTT (fast) or 2 RTTs (slow)
```

### 3.2 Core Data Structures

#### 3.2.1 Timestamp

```rust
/// Globally unique, totally ordered transaction timestamp.
/// Fields MUST be sorted in this exact order for PartialOrd to be correct.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(C)]
pub struct Timestamp {
    /// Configuration epoch. Incremented on electorate reconfiguration.
    /// Transactions in different epochs cannot form fast-path quorums together.
    pub epoch: u64,

    /// Wall-clock time in nanoseconds since Unix epoch, from local HLC.
    /// Loosely synchronized across nodes via NTP/PTP.
    pub time: u64,

    /// Logical sequence number. Incremented when this node bumps a timestamp
    /// due to a conflict with a higher timestamp. Starts at 0 for coordinator-
    /// assigned timestamps.
    pub seq: u32,

    /// Node ID of the process that last assigned or bumped this timestamp.
    /// Ensures global uniqueness: two nodes assigning the same (time, seq)
    /// will have different node IDs.
    pub node: NodeId,
}

impl Timestamp {
    /// Construct the initial t0 for a new transaction coordinated by `node`.
    pub fn new(epoch: u64, hlc_now: u64, node: NodeId) -> Self {
        Timestamp { epoch, time: hlc_now, seq: 0, node }
    }

    /// Bump this timestamp to be strictly greater than `other`.
    /// Used by replicas in PreAccept when they have seen a conflicting
    /// transaction with a higher timestamp.
    pub fn bump_past(&self, other: &Timestamp, node: NodeId) -> Timestamp {
        let mut bumped = *other;
        bumped.seq = other.seq.saturating_add(1);
        bumped.node = node;
        bumped
    }
}
```

#### 3.2.2 Transaction State (Per Replica, Per Transaction)

```rust
/// All state a replica maintains for a single Accord transaction.
/// Persisted to the commit log before sending any protocol reply.
#[derive(Debug, Clone)]
pub struct TxnState {
    pub txn_id:          TxnId,
    pub t0:              Timestamp,   // coordinator's initial proposed timestamp
    pub t:               Timestamp,   // current / committed execution timestamp
    pub t_max:           Timestamp,   // highest t witnessed for this txn

    /// Dependency set. In PreAccept: union of all deps from electorate members.
    /// In Accept: union of deps with t0_γ < t (not t0_τ). See §3.4.
    pub deps:            HashSet<TxnId>,

    // Phase flags — only one may be true at a time
    pub pre_accepted:    bool,
    pub accepted:        bool,
    pub committed:       bool,
    pub applied:         bool,

    /// CRITICAL: These two ballot fields must be tracked separately.
    /// Using a single variable is the EPaxos correctness bug (Sutra 2019).
    /// max_ballot_seen: highest ballot this replica has promised (joined).
    /// accepted_ballot: highest ballot at which this replica actually voted.
    /// Recovery coordinators select values by max(accepted_ballot), not max(max_ballot_seen).
    pub max_ballot_seen: BallotNumber,
    pub accepted_ballot: BallotNumber,

    /// Persisted result of execution. Required for recovery: if a txn is
    /// applied at all replicas of shard A but fails to apply at shard B,
    /// recovery re-applies using the result persisted at A.
    pub result:          Option<Bytes>,
}
```

#### 3.2.3 Conflict Index

```rust
/// Per-shard index of in-flight transactions for conflict detection.
/// Must support O(log n) lookup of max timestamp for overlapping key ranges.
pub struct ConflictIndex {
    /// Single-partition writes: HashMap for O(1) exact-key lookup.
    /// Covers the common case (>95% of Cassandra operations).
    single_key: HashMap<PartitionKey, SmallVec<[InFlightWrite; 4]>>,

    /// Range-spanning operations: BTreeMap for O(log n) range overlap detection.
    /// Used for multi-partition queries, table scans, and secondary index lookups.
    range_ops:  BTreeMap<TokenRange, BTreeSet<(Timestamp, TxnId)>>,

    /// Indexed column projections for transactional 2i queries.
    /// column_id → value → list of in-flight transactions writing that value.
    indexed_writes: HashMap<ColumnId, HashMap<CellValue, Vec<TxnId>>>,
}

#[derive(Debug, Clone)]
pub struct InFlightWrite {
    pub txn_id:       TxnId,
    pub t0:           Timestamp,
    pub accord_ts:    Option<Timestamp>,  // None until committed
    pub status:       TxnStatus,
}

impl ConflictIndex {
    /// Called in PreAccept handler. Returns max t0 of all conflicting in-flight txns.
    /// O(1) for single-partition writes, O(log n) for range operations.
    pub fn max_conflicting_timestamp(&self, txn: &TxnPayload) -> Option<Timestamp>;

    /// Returns all conflicting txn IDs where t0_γ < t0_τ.
    /// These become the initial dep set for PreAccept.
    pub fn deps_before_t0(&self, txn: &TxnPayload, t0: &Timestamp) -> HashSet<TxnId>;

    /// Returns all conflicting txn IDs where t0_γ < t.
    /// These become the dep set for Accept (note: t not t0).
    pub fn deps_before_t(&self, txn: &TxnPayload, t: &Timestamp) -> HashSet<TxnId>;

    /// Register a new in-flight transaction. Called at PreAccept time.
    pub fn register(&mut self, txn: &TxnPayload, entry: InFlightWrite);

    /// Remove a completed transaction. Called when Applied.
    /// GC keeps the index bounded: ~500 entries at 100K TPS × 5ms avg latency.
    pub fn remove(&mut self, txn_id: &TxnId);
}
```

---

## 4. Protocol Specification

### 4.1 Phase 1: PreAccept

**Coordinator → Electorate:**

```
PreAccept {
    txn_id: TxnId,
    t0:     Timestamp,      // coordinator's proposed execution timestamp
    payload: TxnPayload,    // read/write sets
    epoch:  u64,            // coordinator's current epoch
}
```

**Replica handler (MUST be written to commit log before replying):**

```
HANDLE PreAccept(txn_id, t0_proposed, payload, epoch):
  if max_ballot_seen[txn_id] > 0:
      return NACK(max_ballot_seen[txn_id])
  if pre_accepted[txn_id] OR accepted[txn_id] OR committed[txn_id] OR applied[txn_id]:
      return (idempotent: re-send last reply if available, else ignore)

  if epoch != current_epoch OR NOT ready_electorate[epoch]:
      // Signal to coordinator that epoch has changed
      reply PreAcceptOK with t.epoch = current_epoch (coordinator fetches new config)
      goto SLOW PATH below

  max_conflict_t = conflict_index.max_conflicting_timestamp(payload)

  if t0_proposed > max_conflict_t:
      t = t0_proposed           // fast path vote: accept coordinator's timestamp
  else:
      t = max_conflict_t.bump_past(self.node_id)   // propose higher timestamp

  t_max[txn_id] = t
  pre_accepted[txn_id] = true

  // Dep set uses t0_proposed comparison (not t):
  // captures what was concurrent at proposal time.
  deps = conflict_index.deps_before_t0(payload, t0_proposed)

  PERSIST AccordPreAccepted { txn_id, t0: t0_proposed, t, deps, ballot: 0 }
  reply PreAcceptOK { t, deps }
```

**Coordinator fast-path decision:**

```
COLLECT PreAcceptOK from simple quorum Q_τ:
  deps = union of all p.deps for p in Q_τ

  fast_quorum_size = ceil((|electorate| + f_fast + 1) / 2)

  if EXISTS F_τ ⊆ Q_τ WHERE |F_τ| >= fast_quorum_size AND all p in F_τ: p.t == t0:
      // Fast path: commit with original t0
      broadcast Commit(txn_id, t0, t0, deps)
      goto EXECUTE
  else:
      t = max(p.t for p in Q_τ)
      broadcast Accept(ballot=0, txn_id, t0, t, deps)
```

### 4.2 Phase 2: Accept (Slow Path Only)

**Coordinator → All shards:**

```
Accept {
    ballot:  BallotNumber,
    txn_id:  TxnId,
    t0:      Timestamp,
    t:       Timestamp,    // decided execution timestamp (may be > t0)
    deps:    HashSet<TxnId>,
}
```

**Replica handler:**

```
HANDLE Accept(ballot, txn_id, t0, t, deps):
  if ballot < max_ballot_seen[txn_id]:
      return NACK(max_ballot_seen[txn_id])
  if committed[txn_id] OR applied[txn_id]:
      return (ignore; already past this phase)

  max_ballot_seen[txn_id] = ballot
  accepted_ballot[txn_id] = ballot   // SEPARATE FIELD — see §6.1
  t_max[txn_id] = max(t, t_max[txn_id])
  accepted[txn_id] = true

  // Accept dep set uses t comparison (not t0):
  accept_deps = conflict_index.deps_before_t(payload, t)

  PERSIST AccordAccepted { txn_id, t0, t, deps: accept_deps, accepted_ballot: ballot }
  reply AcceptOK { deps: accept_deps }

COLLECT AcceptOK from simple quorum Q_τ:
  deps = union of all p.deps for p in Q_τ
  broadcast Commit(txn_id, t0, t, deps)
```

### 4.3 Phase 3: Commit

```
Commit {
    txn_id: TxnId,
    t0:     Timestamp,
    t:      Timestamp,
    deps:   HashSet<TxnId>,
}
```

**Replica handler:**

```
HANDLE Commit(txn_id, t0, t, deps):
  t_max[txn_id] = t
  committed[txn_id] = true
  PERSIST AccordCommitted { txn_id, t, deps }
  // Wake any transactions awaiting committed[txn_id]
```

### 4.4 Phase 4: Execute

**Coordinator → nearest replica of each shard:**

```
Read {
    txn_id:    TxnId,
    t:         Timestamp,
    shard_deps: HashSet<TxnId>,  // deps that access this shard
}
```

**Replica handler:**

```
HANDLE Read(txn_id, t, shard_deps):
  // Wait for all shard deps to at least commit
  for γ in shard_deps:
      await committed[γ]

  // Wait for shard deps with lower timestamp to fully apply
  for γ in shard_deps:
      if γ.t < t:
          await applied[γ]

  reads = local_storage.read(txn_id.read_set ∩ this_shard)
  reply ReadOK { reads }
```

**Coordinator on receiving ReadOK from all shards:**

```
result = execute_transaction(reads_per_shard)
broadcast Apply(txn_id, t, deps, result)
send result to client   // client receives ACK here
```

### 4.5 Phase 5: Apply

```
Apply {
    txn_id: TxnId,
    t:      Timestamp,
    deps:   HashSet<TxnId>,
    result: Bytes,
}
```

**Replica handler:**

```
HANDLE Apply(txn_id, t, deps, result):
  if applied[txn_id]:
      return   // idempotent

  for γ in deps:
      await committed[γ]
  for γ in deps:
      if γ.t < t:
          await applied[γ]

  // CRITICAL: persist result BEFORE setting applied flag.
  // Result is needed by recovery if Apply reaches shard A but not shard B.
  memtable.write(txn_id.write_set, t)
  mem_index.apply(txn_id.indexed_writes, t)
  PERSIST AccordApplied { txn_id, t, result }

  applied[txn_id] = true
  conflict_index.remove(txn_id)
  // Wake any transactions awaiting applied[txn_id]
```

---

## 5. Recovery Protocol

Recovery is invoked by the failure detector in `ferrosa-net` when a coordinator
is suspected. The failure detector (heartbeat timeout, default 5s) triggers
recovery on any replica that has witnessed the transaction.

```
RECOVER(txn_id, t0):
  ballot = fresh_ballot()    // monotonically increasing, larger than any seen
  broadcast Recover(ballot, txn_id, t0) to all replicas in P_τ[t0]

HANDLE Recover(ballot, txn_id, t0) on replica p:
  if ballot <= max_ballot_seen[txn_id]:
      reply NACK(max_ballot_seen[txn_id])
      return
  max_ballot_seen[txn_id] = ballot

  // Identify superseding and waiting transactions (see Accord Algorithm 3)
  accepts  = { γ | γ ∼ τ AND τ ∉ deps[γ] AND accepted[γ] }
  commits  = { γ | γ ∼ τ AND τ ∉ deps[γ] AND committed[γ] }
  wait     = { γ ∈ accepts | t0_γ < t0_τ AND t_γ > t0_τ }
  super    = { γ ∈ accepts | t0_γ > t0_τ } ∪ { γ ∈ commits | t_γ > t0_τ }

  if NOT pre_accepted[txn_id]:
      run PreAccept handler locally (lines 3–10 of §4.1)

  if NOT (accepted OR committed OR applied)[txn_id]:
      deps[txn_id] = { γ | γ ∼ τ AND t0_γ < t0_τ }

  reply RecoverOK(state: txn_state[txn_id], superseding: super, wait: wait)

ON COLLECTING RecoverOK from recovery quorum R_τ:
  if ANY p.applied:
      broadcast Apply(t0, p.t, p.deps, p.result) using p with applied=true
      done

  if ANY p.committed:
      broadcast Commit(t0, p.t, p.deps) using p with committed=true
      goto EXECUTE
      done

  if ANY p.accepted:
      // CRITICAL: select by accepted_ballot, NOT max_ballot_seen
      best = p in R_τ with max(accepted_ballot[p])
      goto Accept(ballot, txn_id, t0, best.t, best.deps)
      done

  // No accepted or committed state found — determine safe timestamp:
  t = t0
  higher_voters = count(p in electorate where p.t > p.t0)
  if higher_voters > |electorate| - fast_quorum_size:
      t = max(p.t for p in R_τ)
  else if ANY p.superseding non-empty:
      t = max(p.t for p in R_τ)
  else if UNION(p.wait for p in R_τ) non-empty:
      await committed(γ) for all γ in UNION(p.wait)
      restart RECOVER(txn_id, t0)
      done

  deps = union(p.deps for p in R_τ)
  goto Accept(ballot, txn_id, t0, t, deps)
```

---

## 6. Correctness Requirements

### 6.1 The Two-Ballot-Variable Invariant (MANDATORY)

**Background:** Sutra (2019) proved that EPaxos's use of a single ballot variable
to track both "highest ballot joined" and "highest ballot voted in" allows replicas
to misreport their voting history during recovery, producing linearizability violations.
The specific counter-example requires 3 processes, 2 conflicting commands, and 24
carefully ordered recovery steps.

**Requirement:** `TxnState` MUST maintain two separate ballot fields:

```rust
pub max_ballot_seen: BallotNumber,  // highest ballot this replica has promised
pub accepted_ballot: BallotNumber,  // highest ballot at which this replica VOTED
```

These fields have different semantics and must never be conflated:

- `max_ballot_seen` is updated when the replica *joins* a ballot (sends a promise)
- `accepted_ballot` is updated only when the replica *votes* (sends AcceptOK)
- Recovery coordinators MUST select the authoritative value using `max(accepted_ballot)`,
  not `max(max_ballot_seen)`

**Audit rule:** Any code path in the recovery handler that reads ballot information
from `RecoverOK` responses and uses it to decide which value to propose MUST use
`accepted_ballot`. This is enforceable via distinct Rust types:

```rust
/// Opaque type representing a ballot at which a value was accepted (voted).
/// Distinct from BallotNumber to prevent confusion with "joined" ballots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AcceptedBallot(BallotNumber);

/// Opaque type representing the highest ballot a replica has promised not to
/// participate in lower ballots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PromisedBallot(BallotNumber);
```

### 6.2 Timestamp Uniqueness Invariant

No two transactions may have the same `(epoch, time, seq, node)` tuple. The `node`
field ensures uniqueness between nodes. The `seq` field ensures uniqueness within
a node when two transactions are assigned in the same nanosecond. `seq` must be
monotonically incremented until it produces a tuple not already in the conflict index.

### 6.3 Dependency Completeness Invariant

For any two committed conflicting transactions γ and τ with `t_γ < t_τ`:

```
γ ∈ deps(τ)
```

This is guaranteed by:

- PreAccept dep filter: `deps = { γ | γ ∼ τ AND t0_γ < t0_τ }`
- Accept dep filter: `deps = { γ | γ ∼ τ AND t0_γ < t_τ }`
- Recovery dependency safety proof (Accord Section 3.4, Property 3.3)

The superset dep model permits `deps(τ)` to contain additional entries beyond
the minimum required — this is safe (adds unnecessary waits, not incorrect waits).
What is NOT safe: `deps(τ)` missing a γ with `t_γ < t_τ`.

### 6.4 Applied Result Durability Invariant

The `AccordApplied` commit log entry, including `result: Bytes`, MUST be written
and fsynced to local NVMe BEFORE:

1. Setting `applied[txn_id] = true` in memory
2. Releasing the dep-wait for any transaction waiting on this one
3. Sending any `ApplyOK` acknowledgment

Violation: if a node applies a transaction and crashes before persisting the result,
and the transaction's shard partners have not yet applied it, recovery cannot reconstruct
the result without re-executing the transaction — which is impossible for non-deterministic
reads that were already sent to the client.

---

## 7. Timestamp Reorder Buffer

The Reorder Buffer delays processing of incoming `PreAccept` messages until their
arrival deadline, ensuring that conflicting messages with earlier timestamps arrive
in order. This eliminates slow-path retries caused by clock skew and is the primary
mechanism by which Accord achieves single-RTT fast-path consensus.

### 7.1 Deadline Formula

For a `PreAccept` arriving at replica P, sent by coordinator C, proposing timestamp `t0`:

```
Deadline(t0, C, P) = wall_clock(t0.time)
                   + SkewMax
                   + max(Latency(C', P) | C' ∈ all_coordinators)
                   - Latency(C, P)
```

Where:

- `SkewMax` = P99.9 of observed clock offsets between all node pairs (measured via
  heartbeat timestamps, NOT theoretical NTP bounds)
- `Latency(C, P)` = P99 of one-way message delay from C to P (derived from heartbeat
  RTTs divided by 2)

Guarantee: if skew and latency bounds are not exceeded, any conflicting transaction
with `t0_γ < t0_τ` sent by any coordinator C' will arrive at P before Deadline(t0_τ, C, P).
After the deadline, P can safely process the PreAccept for τ in timestamp order.

### 7.2 Implementation Requirements

```rust
pub struct ReorderBuffer {
    /// Priority queue ordered by deadline (earliest deadline first).
    /// Use a timer wheel, NOT individual tokio::time::sleep calls.
    /// One sleep per enqueued message = O(n) timers = unacceptable overhead.
    queue: TimerWheel<PendingPreAccept>,

    /// Per-node P99 one-way latency estimates, updated each heartbeat.
    latency_table: Arc<RwLock<HashMap<NodeId, Duration>>>,

    /// P99.9 clock skew estimate across all node pairs, updated each heartbeat.
    /// Stored as nanoseconds in an AtomicU64 for lock-free reads on the hot path.
    skew_max_ns: Arc<AtomicU64>,
}
```

- The reorder buffer is **in-memory only** and need not survive restarts.
  Buffered messages that are lost on restart will be re-sent by coordinators
  after their timeout expires.
- Buffer depth is bounded: `SkewMax + max_latency` × TPS per shard.
  At 1ms total and 100K TPS per shard ≈ 100 entries per shard. Negligible.
- Heartbeat messages in `ferrosa-net` MUST be extended to include:
  - `sent_at: u64` (sender's HLC timestamp)
  - `recv_at: u64` (receiver fills this on receipt for RTT calculation)

### 7.3 Skew Measurement

On each heartbeat receive at node P from node Q:

```
observed_skew = |Q.sent_at - P.local_clock_at_receipt|
skew_sample_window.push(observed_skew)
SkewMax = percentile(skew_sample_window, 99.9)
```

This is empirical measurement, not the NTP error bound. Do not use `/etc/ntp.conf`
drift values — measure actual observed skew between Ferrosa nodes.

---

## 8. Flexible Electorates

### 8.1 Quorum Sizing

For an electorate of size `|E|` and fast-path failure parameter `f_fast`:

```
fast_quorum_size = ceil((|E| + f_fast + 1) / 2)
slow_quorum_size = f_slow + 1
```

**Default configuration for RF=3:**

- `f_fast = 0`: fast quorum = 2/3. Fast path survives any single failure.
- `f_slow = 1`: slow quorum = 2/3 (majority). System makes progress with 2/3 nodes.

**For RF=5:**

- `f_fast = 1`: fast quorum = 4/5. Fast path survives any single failure.
- `f_slow = 2`: slow quorum = 3/5. System makes progress with 3/5 nodes.

**Enforcement:** `fast_quorum_size` MUST be computed dynamically from the current
electorate size. It MUST NOT be hardcoded. When the electorate shrinks due to node
failure, the quorum threshold automatically adjusts.

```rust
pub fn fast_quorum_size(electorate_size: usize, f_fast: usize) -> usize {
    // Ceiling integer division: (a + b - 1) / b
    (electorate_size + f_fast + 1 + 1) / 2
}
```

### 8.2 Electorate Reconfiguration

Electorate configs are managed by the existing openraft metadata group.
A new `ElectorateEpoch` is committed to the Raft log when:

- A node is detected as permanently failed (operator decommission)
- A new node joins the cluster (after receiving JoinElectorate notifications)

**Epoch field in Timestamp:** the `epoch` field in `Timestamp` matches the
electorate epoch at the time the coordinator assigned `t0`. A replica at epoch
`e2 > e1` must not participate in a fast-path decision for a transaction from
epoch `e1`. It signals this by returning a PreAcceptOK with `t.epoch = e2`,
which causes the coordinator to fall back to the slow path and fetch the new config.

**JoinElectorate protocol:** A new electorate member MUST NOT vote in any fast-path
decision until it has received `JoinElectorate` notifications from at least
`|E_old| - |F_old| + 1` members of the prior electorate. These notifications
carry all transactions that were fast-path committed under previous configs.
The `ready_electorate[epoch]` flag gates this.

---

## 9. Transactional Secondary Indexes (2i)

### 9.1 Architecture

A 2i query with transactional consistency queries five layers:

```
Layer 1: CommitIndex (in-flight txn column projections)   │ RAM, ~5μs
Layer 2: CommitIndex (committed, not yet applied)          │ RAM, ~5μs
Layer 3: MemIndex (applied to memtable, not flushed)       │ RAM, ~2μs
Layer 4: Unindexed SSTables (flushed, index build pending) │ Block cache, ~5μs/SSTable
Layer 5: Persistent secondary index                        │ NVMe/S3, ~5μs–25ms
```

Layers 1–4 are the "recently committed" scan. They are bounded in size and hot in
the block cache or process memory. The query algorithm:

```
READ_2I(column, value, read_ts):
  // Step 1: Persistent index (Layer 5)
  base_keys = persistent_index.lookup(column, value)

  // Step 2: MemIndex (Layer 3)
  mem_hits = mem_index.lookup(column, value, read_ts)

  // Step 3: CommitIndex scan (Layers 1–2)
  pending_deps = []
  inflight_hits = []
  for txn in commit_index.inflight_writing(column, value):
      match txn.status:
          PreAccepted | Committed(no accord_ts):
              pending_deps.push(txn.txn_id)
          Committed(accord_ts) if accord_ts <= read_ts:
              inflight_hits.push(txn.partition_key)
          Applied:
              // Already in MemIndex; skip

  // Step 4: Dep-wait (only if in-flight conflicts exist)
  if pending_deps non-empty:
      await all pending_deps reach Committed status
      // Re-evaluate just those deps with now-known accord_ts
      for dep in pending_deps:
          if dep.accord_ts <= read_ts:
              inflight_hits.push(dep.partition_key)

  // Step 5: Unindexed SSTables (Layer 4)
  unindexed = storage.sstables_flushed_after(index.last_built_flush_id)
  for sstable in unindexed:
      if sstable.bloom_filter.might_contain(column, value):
          sstable_hits += sstable.scan_column(column, value, accord_ts_le=read_ts)

  // Step 6: Merge, apply deletions
  all_candidates = base_keys ∪ mem_hits ∪ inflight_hits ∪ sstable_hits
  deletions = mem_index.deletes(column, value, read_ts)
             ∪ commit_index.deletes(column, value, read_ts)
  result_keys = all_candidates - deletions

  // Step 7: Fetch base rows (standard Accord read, enforces dep-wait)
  return [accord_read(key, read_ts) for key in result_keys]
```

### 9.2 MemIndex

The MemIndex is an in-memory B-tree maintained atomically with the memtable,
updated in the Apply phase, and garbage-collected on flush:

```rust
pub struct MemIndex {
    /// column_value → (accord_ts → MemIndexEntry)
    /// BTreeMap allows range-scan by value AND timestamp filtering.
    entries: BTreeMap<CellValue, BTreeMap<Timestamp, MemIndexEntry>>,

    /// PartitionKey → set of indexed values written here.
    /// Needed for DELETE: removes old index entries when value changes.
    by_partition: HashMap<PartitionKey, HashSet<CellValue>>,
}
```

**Atomicity requirement:** `mem_index.apply()` and `memtable.write()` MUST be
called within the same logical transaction in the Apply handler. They may be
separate function calls but must not be interleaved with any other Apply handler
for the same shard.

**Flush GC:** when an SSTable is flushed, `mem_index.flush_gc(flushed_up_to_ts)`
removes entries with `accord_ts <= flushed_up_to_ts`. Those entries are now
covered by the persistent index (once the async index build completes for the
new SSTable).

### 9.3 Eager Index Build

The async secondary index build MUST be triggered immediately after each SSTable
flush, at `Priority::High`, not deferred to compaction. This keeps Layer 4
(unindexed SSTables) at 0–1 entries in steady state, making Step 5 nearly free.

```rust
// In ferrosa-storage, flush completion hook:
async fn on_flush_complete(&self, sstable: SSTableRef, flush_id: FlushId) {
    self.mem_index.flush_gc(sstable.max_accord_ts);
    self.index_builder
        .schedule_build(sstable, flush_id, Priority::High)
        .await;
}
```

### 9.4 Non-Transactional Index Mode

Indexes on high-frequency write columns (e.g. `last_seen_at`, `updated_at`) may
opt out of CommitIndex tracking:

```cql
CREATE INDEX idx_last_seen ON users (last_seen_at)
  WITH OPTIONS = {'consistency': 'eventual'};
```

Eventual-mode indexes skip Steps 3–5 of READ_2I and use only the persistent index.
This is the existing behavior and is appropriate when staleness of a few seconds is
acceptable and the indexed column is written on every operation (high CommitIndex
churn would hurt performance without improving correctness for the use case).

---

## 10. Commit Log Extensions

The following entry types MUST be added to `ferrosa-storage`'s commit log.

```rust
pub enum CommitLogEntry {
    // Existing entries unchanged
    DataMutation { partition_key, cells, write_timestamp },
    SchemaChange  { ddl_statement },

    // Accord protocol state (new)
    AccordPreAccepted {
        txn_id:  TxnId,
        t0:      Timestamp,
        t:       Timestamp,
        deps:    SmallVec<[TxnId; 8]>,
    },
    AccordAccepted {
        txn_id:          TxnId,
        t0:              Timestamp,
        t:               Timestamp,
        deps:            SmallVec<[TxnId; 8]>,
        accepted_ballot: AcceptedBallot,   // distinct type; see §6.1
    },
    AccordCommitted {
        txn_id: TxnId,
        t:      Timestamp,
        deps:   SmallVec<[TxnId; 8]>,
    },
    AccordApplied {
        txn_id: TxnId,
        t:      Timestamp,
        result: Bytes,   // serialized transaction result; required for recovery
    },
}
```

**Write ordering invariant:** the commit log entry for each phase MUST be written
and fsynced before the protocol reply for that phase is sent over the network.
Specifically:

- `AccordPreAccepted` before `PreAcceptOK`
- `AccordAccepted` before `AcceptOK`
- `AccordApplied` before `applied[txn_id] = true`

**GC policy:** `AccordPreAccepted`, `AccordAccepted`, and `AccordCommitted` entries
may be GC'd once the corresponding `AccordApplied` entry has been flushed to an SSTable.
`AccordApplied` entries may be GC'd once the SSTable containing the write has been
uploaded to S3 and confirmed durable.

---

## 11. Test Requirements

### 11.1 Unit Tests

| Test | Component | Requirement |
|------|-----------|-------------|
| `timestamp_ordering` | `Timestamp` | Total order consistent across all field combinations |
| `timestamp_uniqueness` | `Timestamp` | `bump_past` always returns t > other |
| `conflict_index_single_key` | `ConflictIndex` | O(1) lookup; correct max_ts returned |
| `conflict_index_range_overlap` | `ConflictIndex` | Range queries detect correct overlaps |
| `mem_index_apply_gc` | `MemIndex` | Entries removed on flush_gc at correct ts bound |
| `mem_index_update_replaces` | `MemIndex` | Old value entry removed when column value changes |
| `mem_index_delete_removes` | `MemIndex` | DELETE removes entry from index |
| `reorder_buffer_ordering` | `ReorderBuffer` | Messages processed in t0 order, not arrival order |
| `reorder_buffer_deadline` | `ReorderBuffer` | Deadline formula uses measured skew/latency |
| `fast_quorum_size_formula` | `ElectorateConfig` | Matches `ceil((E + f + 1) / 2)` for all inputs |
| `dep_filter_preaccept_vs_accept` | `AccordStateMachine` | PreAccept uses t0, Accept uses t |
| `ballot_variable_separation` | `TxnState` | `accepted_ballot` and `max_ballot_seen` update independently |

### 11.2 The 24-Step EPaxos Correctness Test (MANDATORY CI GATE)

This test encodes the exact counter-example from Sutra (2019) that demonstrates
the single-ballot-variable safety violation. It MUST pass before any recovery code
merges. It MUST run on every CI build. A failing result means the recovery protocol
has the ballot variable bug.

**Setup:**

- 3 simulated replicas: `p1`, `p2`, `p3`
- 2 conflicting transactions: `c1`, `c2` (same token range)
- Network is fully controlled: no messages deliver except those explicitly injected
- All timers and clocks are synthetic (deterministic, not wall-clock)

**Step sequence** (messages injected in this exact order):

```
Step  1:  p3 sends PreAccept(c1) to {p1, p2, p3}      [c1 starts at p3]
Step  2:  p1 sends PreAccept(c2) to {p1, p2, p3}      [c2 starts at p1]
Step  3:  p3 delivers PreAccept(c2): sees c1 first → replies PreAcceptOK(c2, deps={c1})
Step  4:  p3 sends Recover(ballot=2, c2, t0_c2) to {p2, p3}  [partial recovery begins]
Step  5:  p2 delivers Recover: promises ballot=2, replies RecoverOK
Step  6:  p3 delivers Recover: promises ballot=2, replies RecoverOK(deps={c1}, accepted_ballot=0)
Step  7:  p3 (recovery coord): collects RecoverOK → sends Accept(ballot=2, c2, deps={c1}) to {p2,p3}
          [dep(c2)={c1} accepted at ballot 2 on p3]
Step  8:  p2 sends Recover(ballot=3, c2, t0_c2) to {p1, p2}  [second recovery]
Step  9:  p1 delivers Recover: promises ballot=3
Step 10:  p2 delivers Recover: promises ballot=3
Step 11:  p2 (recovery coord): collects → sees no accepted state → sends Accept(ballot=3, c2, deps={})
Step 12:  p1 replies PreAcceptOK(c2, deps={})          [p1 never saw c1]
Step 13:  p2 Accept(ballot=3, deps={}) delivered at {p1, p2} → accepted
          [dep(c2)={} accepted at ballot 3 on p2]
Step 14:  p1 sends AcceptOK(ballot=3)
Step 15:  p1 sends Recover(ballot=4, c2, t0_c2) to {p1, p3}  [third recovery]
Step 16:  p3 delivers Recover(ballot=4): updates max_ballot_seen=4 (but does NOT re-vote)
          [With bug: p3.max_ballot = 4; reports accepted_ballot=4 (WRONG — voted at ballot 2)]
          [Correct:  p3.accepted_ballot = 2; max_ballot_seen = 4 (separate fields)]
Step 17:  p1 sends Recover(ballot=5, c2, t0_c2) to {p1, p3}  [fourth recovery]
Step 18:  p3 delivers Recover(ballot=5)
Step 19:  p1 delivers Recover(ballot=5)
Step 20:  p1 delivers Recover(ballot=5)                [duplicate — idempotency test]
Step 21:  p1 (recovery coord) finalizes: selects p3's state as authoritative
          [With bug: p3 claims accepted_ballot=4 > p2's accepted_ballot=3 → picks deps={c1}]
          [Correct:  p3.accepted_ballot=2 < p2's accepted_ballot=3 → picks deps={}]
Step 22:  p3 replies AcceptOK(ballot=5)
Step 23:  p2 commits dep(c2)={}    [from step 13 recovery]
Step 24:  p1 commits dep(c2)={c1}  [from step 21 recovery, if bug present]

ASSERT: p1.committed_deps(c2) == p2.committed_deps(c2)
ASSERT: linearizability_check(p1, p2, p3) == true
```

**What this test proves:** if `accepted_ballot` and `max_ballot_seen` are the same
field, step 16 corrupts `accepted_ballot` from 2 to 4 without a corresponding vote.
Step 21 then picks `{c1}` over `{}` because it incorrectly believes ballot 4 > ballot 3.
p2 commits `{}` and p1 commits `{c1}`. Two replicas disagree. Linearizability broken.

With correct separate fields, step 16 updates only `max_ballot_seen=4`. Step 21 sees
`p3.accepted_ballot=2` and `p2.accepted_ballot=3`, correctly selects p2's value `{}`.
Both replicas commit `{}`. Consistent.

### 11.3 Jepsen Test Suite

All of the following Jepsen tests MUST pass before the feature is considered production-ready:

#### Jepsen: Register (Single-Key Linearizability)

- Concurrent reads and writes to a single CQL row
- Verifies that reads always observe the most recently committed write
- Failure injection: kill minority nodes during concurrent operations
- **Pass criterion:** Knossos linearizability checker finds no violations

#### Jepsen: Bank (Multi-Key Atomicity)

- 100 accounts, concurrent transfers between random pairs
- Each transfer: read both accounts, compute new balances, write both atomically
- Readers concurrently read all accounts and sum balances
- **Pass criterion:** total balance never changes. If any read observes a total ≠ initial,
  atomicity is violated.

#### Jepsen: Long-Fork

- Two concurrent transactions each read two keys and write one key
- Tests for the long-fork anomaly (G2 isolation violation)
- **Pass criterion:** no execution history admits a long-fork interpretation under Knossos

#### Jepsen: Monotonic Reads

- A single client reads the same key repeatedly
- **Pass criterion:** the sequence of values observed is monotonically non-decreasing
  in Accord timestamp order (no value is observed, then a prior value appears)

#### Jepsen: Write Skew

- Two concurrent transactions each read a shared counter and write to different keys
- Classic isolation anomaly: both see value 0, both decide to decrement
- Result: both committed but combined effect violates the application invariant
- **Pass criterion:** with strict serializability, only one transaction should be
  allowed to commit if both base their write on reading the same value

#### Jepsen: Nemesis Configuration

All tests run with the following nemesis operations active:

- `partition`: random minority network partition, 30s duration
- `kill`: kill random minority of nodes (restart within 5s)
- `slow`: add 100ms jitter to 20% of internode messages
- `clock-skew`: introduce ±5ms clock skew on random nodes
- `pause`: SIGSTOP a random node for 10s (simulates GC pause)

### 11.4 Performance Regression Tests

The following benchmarks MUST be run and results recorded for each release:

| Benchmark | Baseline | Allowed Regression |
|-----------|----------|--------------------|
| Single-key write P50 (same AZ) | current QUORUM | +15% |
| Single-key write P99 (same AZ) | current QUORUM | +25% |
| Single-key read P50 (no conflict) | current QUORUM | −10% (improvement expected) |
| Multi-key 2-partition txn P50 | N/A (new feature) | < 2× single-key write |
| Jepsen bank throughput | N/A (new feature) | > 10K TPS on 3-node cluster |
| Conflict index lookup P99 | N/A (new) | < 50μs |
| Reorder buffer overhead P99 | N/A (new) | < 5ms added to write latency |

---

## 12. Implementation Phases

### Phase 0: Foundation Crates (no user-visible change)

**Gate:** all Phase 0 unit tests pass; 24-step counter-example test exists and passes

- [ ] `ferrosa-hlc`: Hybrid Logical Clock, `Timestamp` type with epoch field
- [ ] `ferrosa-accord-types`: `TxnId`, `TxnState` with two ballot fields, `BallotNumber`,
      `AcceptedBallot`, `PromisedBallot` (distinct Rust types)
- [ ] `ConflictIndex` in `ferrosa-storage`: single-key + range paths + indexed_writes
- [ ] `MemIndex` in `ferrosa-storage`: BTreeMap, apply, flush_gc, lookup, delete handling
- [ ] Commit log entry types: `AccordPreAccepted`, `AccordAccepted`, `AccordCommitted`,
      `AccordApplied`
- [ ] Heartbeat extension in `ferrosa-net`: `sent_at`, `recv_at` fields; per-link RTT tracking
- [ ] `ReorderBuffer` with timer wheel; skew measurement from heartbeats
- [ ] **The 24-step EPaxos correctness test** as a mandatory CI gate

### Phase 1: Single-Key Accord (replaces existing quorum write path)

**Gate:** Jepsen register test passes; P50 write latency within 15% of baseline

- [ ] `AccordStateMachine` in `ferrosa-cluster`: PreAccept, Accept, Commit, Execute, Apply
      state machine with all phase transitions
- [ ] Leaseholder assignment in token ring metadata (openraft-managed, epoch-bounded)
- [ ] Leaseholder fast path: local conflict check + broadcast to 2 followers = 1 RTT
- [ ] Linearizable local read: dep-check against ConflictIndex before serving from memtable
- [ ] Recovery coordinator: triggered by `ferrosa-net` failure detector (heartbeat timeout)
- [ ] Commit log replay on startup: reconstruct AccordStateMachine state from persisted entries
- [ ] `ElectorateConfig` in openraft state machine with epoch management

### Phase 2: Multi-Key Transactions

**Gate:** Jepsen bank test passes; write skew test passes

- [ ] `BEGIN TRANSACTION / COMMIT / ROLLBACK` in `ferrosa-cql` parser
- [ ] Client session state for multi-statement transaction accumulation
- [ ] Read-set / write-set extraction from accumulated CQL statements
- [ ] Cross-shard Execute: parallel Read RPCs to nearest replica per shard
- [ ] Coordinator failure handling: client-side retry with same `TxnId`
- [ ] Conflict detection across shards: ConflictIndex partitioned by token range

### Phase 3: Transactional 2i

**Gate:** 2i correctness test (concurrent write + 2i read, no stale result); dep-wait latency < 5ms P99

- [ ] `MemIndex` integrated into Apply phase (atomic with memtable write)
- [ ] `READ_2I` algorithm in `ferrosa-cql` 2i query planner
- [ ] CommitIndex `indexed_writes` projection populated at PreAccept time
- [ ] Dep-wait for in-flight transactions in 2i query path
- [ ] Eager index build trigger in `ferrosa-storage` flush completion hook
- [ ] Unindexed SSTable bloom filter + BTI scan (Step 5 of READ_2I)
- [ ] `eventual` consistency mode for non-transactional indexes

### Phase 4: Electorate Reconfiguration

**Gate:** chaos test: kill minority during transactions; no lost commits; Jepsen all-tests pass with nemesis

- [ ] Epoch field propagation through all protocol messages
- [ ] Slow-path fallback when epoch mismatch detected at PreAccept
- [ ] `JoinElectorate` protocol: new members wait for fast-path history from prior electorate
- [ ] `JoinShard` protocol for new replicas
- [ ] `ready_electorate[epoch]` flag gating fast-path participation
- [ ] Electorate shrink on node failure: openraft commit → epoch increment → quorum resize

---

## 13. Open Questions

1. **Should `CL=ONE` bypass Accord entirely?** Write-only workloads (telemetry, IoT, event logs)
   that never need to read their own writes could skip the PreAccept round-trip. However this
   reintroduces the mixing problem. Proposed resolution: `CL=ONE` writes still go through Accord
   but skip the Execute phase (fire-and-forget after Commit). They appear in the ConflictIndex
   and are dep-tracked by subsequent transactions.

2. **Schema changes as Accord transactions.** ALTER TABLE and CREATE INDEX need to conflict with
   all DML on the affected table. Modeling DDL as Accord transactions that span "all partitions
   of table X" is correct but complex. Phase 1 can treat DDL as a blocking operation (drain
   in-flight transactions, apply DDL, resume). Phase 4 can model DDL properly.

3. **SUBSCRIBE ordering with Accord timestamps.** CDC consumers currently see changes in Apply
   order. With Accord, Apply order is deterministic (Accord `t` ordering). This is strictly
   more correct. Document as a behavior change; expose `accord_ts` in the change event payload.

4. **S3 upload queue depth under Accord write amplification.** Each transaction generates
   4 commit log entries (PreAccepted, Accepted, Committed, Applied) vs 1 today. Monitor
   `FERROSA_S3_UPLOAD_QUEUE_DEPTH` under load; may need to increase from 16 to 64.

5. **Result persistence format in SSTables.** The `AccordApplied.result` bytes need a home in
   the SSTable format. Options: (a) separate "accord results" column family, (b) inline with
   the row data as a hidden system column, (c) separate file per SSTable. Option (b) is
   simplest but adds per-row overhead. Needs profiling.

---

## 14. Reference Documents

| Document | Location |
|----------|----------|
| Accord paper (CEP-15) | `docs/papers/accord-cep15.pdf` |
| Caesar distillation | `docs/papers/caesar-chasing-fast-decisions.md` |
| Tempo distillation | `docs/papers/tempo-timestamp-stability.md` |
| Atlas distillation | `docs/papers/atlas-planet-scale-smr.md` |
| EPaxos correctness bug | `docs/papers/epaxos-correctness-bug.md` |
| EPaxos Revisited + ROLL bounds | `docs/papers/epaxos-revisited-and-roll-bounds.md` |
| Implementation guide | `docs/papers/accord-implementation-guide.md` |
| Architecture brainstorm | `docs/papers/accord-ferrosa-architecture-brainstorm.md` |
| 2i consistent index design | `docs/papers/2i-accord-consistent-index.md` |
| 2i latency analysis | `docs/papers/2i-latency-analysis.md` |
| cassandra-accord (Java reference impl) | <https://github.com/apache/cassandra-accord> |
| Fantoch (Rust SMR framework) | <https://github.com/vitorenesduarte/fantoch> |
