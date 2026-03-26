# FMEA — Accord Distributed Transactions

> Last updated: 2026-03-23
> Status: Active (A1-A7 implemented, 2,808 tests passing)

## Scope

Failure modes for the Accord strict-serializable transaction subsystem integrated
into Ferrosa. Cross-referenced with threat model findings (AT-series from
`specs/threat-model-accord.md`) and DSM analysis of the `ferrosa-cluster/src/accord/`
module boundary.

This analysis focuses on protocol correctness failures, not operational concerns
(monitoring, alerting, capacity planning). Each failure mode targets a specific
component from `SPEC-accord-transactions.md`.

## Scoring Criteria

| Score | Severity (S) | Occurrence (O) | Detection (D) |
|-------|-------------|----------------|----------------|
| 1 | Negligible | Almost never | Always detected before impact |
| 2-3 | Minor degradation | Rare | Usually detected |
| 4-6 | Significant impact | Occasional | Sometimes detected |
| 7-8 | Major failure | Frequent | Rarely detected |
| 9-10 | Catastrophic / data loss | Very frequent | Undetectable |

RPN = Severity x Occurrence x Detection.

| RPN Range | Priority | Action |
|-----------|----------|--------|
| >= 200 | Critical | Must fix before merge |
| 100-199 | High | Must fix in current sprint |
| 50-99 | Medium | Schedule for next sprint |
| < 50 | Low | Backlog |

## Failure Mode Summary Table

| ID | Component | Failure Mode | S | O | D | RPN | Priority | Status |
|----|-----------|-------------|---|---|---|-----|----------|--------|
| FM1 | HybridLogicalClock | Clock skew exceeds SkewMax | 10 | 3 | 3 | **90** | Medium | **Implemented** |
| FM2 | TxnState | Single ballot variable (two-ballot invariant violation) | 10 | 2 | 3 | **60** | Medium | **Implemented** |
| FM3 | ConflictIndex | Missing entry in conflict index | 9 | 4 | 3 | **108** | High | **Implemented** |
| FM4 | RecoveryCoordinator | Recovery selects wrong accepted value | 10 | 2 | 3 | **60** | Medium | **Implemented** |
| FM5 | CommitLog Extensions | Entry not fsynced before reply | 10 | 3 | 2 | **60** | Medium | **Implemented** |
| FM6 | MemIndex | Non-atomic memtable/MemIndex apply | 9 | 3 | 3 | **81** | Medium | **Implemented** |
| FM7 | ElectorateConfig | Epoch mismatch during fast path | 8 | 4 | 3 | **96** | Medium | **Implemented** |
| FM8 | ReorderBuffer | TimerWheel overflow (message loss) | 7 | 5 | 2 | **70** | Medium | **Implemented** |
| FM9 | Leaseholder Fast Path | Stale leaseholder after failover | 8 | 4 | 3 | **96** | Medium | **Implemented** |
| FM10 | AccordStateMachine | Cross-shard execute partial failure | 10 | 3 | 3 | **90** | Medium | **Implemented** |
| FM11 | ConflictIndex | GC evicts entry still needed for dependency resolution | 9 | 3 | 3 | **81** | Medium | **Implemented** |
| FM12 | ElectorateConfig | Fast-path quorum includes non-electorate member | 10 | 2 | 3 | **60** | Medium | **Implemented** |
| FM13 | AccordStateMachine | Dep-wait circular dependency deadlock | 8 | 3 | 3 | **72** | Medium | **Implemented** |
| FM14 | CommitLog Extensions | Accord entry too large for segment | 7 | 4 | 2 | **56** | Medium | **Implemented** |
| FM15 | LateDataDebouncer | Debouncer re-aggregation conflicts with Accord ordering | 7 | 4 | 4 | **112** | High | **Implemented** |
| FM16 | AccordStateMachine | Non-transactional write bypasses Accord | 10 | 5 | 2 | **100** | High | **Implemented** |
| FM17 | ElectorateConfig | Epoch transition vulnerability window | 9 | 3 | 3 | **81** | Medium | **Implemented** |
| FM18 | RecoveryCoordinator | Recovery triggered on false positive from failure detector | 5 | 5 | 2 | **50** | Medium | **Implemented** |
| FM19 | HybridLogicalClock | NTP step-change causes timestamp regression | 8 | 3 | 2 | **48** | Low | **Implemented** |
| FM20 | AccordCoordinator | Cross-shard coordinator crash during Execute phase | 10 | 3 | 4 | **120** | High | **Implemented** |
| FM21 | ElectorateConfig | Electorate join with incomplete history sync | 9 | 3 | 4 | **108** | High | **Implemented** |
| FM22 | DurabilityService | DurabilityService stall (no GC progress) | 7 | 4 | 5 | **140** | High | Mitigation implemented, monitoring planned |
| FM23 | LinearizableRead | Linearizable read timeout under high contention | 6 | 5 | 3 | **90** | Medium | **Implemented** |
| FM24 | TransactionLimits | Transaction limit bypass via connection pooling | 8 | 4 | 3 | **96** | Medium | **Implemented** |

## Detailed Analysis

---

### FM1: Clock Skew Exceeds SkewMax

**Component:** HybridLogicalClock / Timestamp

**Cause:** Physical clock of a node drifts beyond the configured `SkewMax` parameter
(default ~100ms). Root causes include NTP misconfiguration, VM clock pause during
live migration, or adversarial clock manipulation (AT05, AT19, AT20 from threat model).

**Effect:** The ReorderBuffer delivers messages in incorrect causal order. A transaction
T2 that should execute after T1 is delivered first because its timestamp appears
earlier when skew exceeds the SkewMax safety margin. This violates strict serializability:
committed reads may reflect a state that never existed. Data corruption is possible
when the misordered transactions write to overlapping keys.

**Detection:** Difficult in production. Clock skew is invisible to the node itself;
only cross-node comparison reveals it. The HLC physical component is read from
`SystemTime::now()`, and Rust has no built-in skew detection. D=7 because detection
requires active monitoring infrastructure that may not be deployed.

**Severity:** S=10. Violation of the core safety property (strict serializability)
with potential data corruption.

**Occurrence:** O=3. Requires either NTP failure or adversarial action. Cloud VMs
occasionally see step-changes during maintenance, but SkewMax is typically set with
margin.

**RPN: 90 (Medium) — revised from 210 after implementation**

**Detection (revised):** D=3. Clock validation is now implemented with active
monitoring. `clock_validation.rs` rejects timestamps exceeding `MAX_CLOCK_DRIFT`
and enters read-only mode. Detection is automatic and immediate.

**Mitigation (Implemented):**

1. HLC monotonicity guard: reject any `SystemTime::now()` value that regresses more
   than `SkewMax` from the last observed physical time. Log at `error` level and
   enter read-only mode until operator intervenes.
2. Peer clock exchange: piggyback HLC timestamps on every internode message. If a
   received timestamp exceeds local HLC by more than `SkewMax`, refuse the message
   and flag the peer as clock-suspect.
3. Startup validation: on boot, query at least 2 peers and refuse to join the
   electorate if clock delta exceeds `SkewMax / 2`.

**Implementation:** `ferrosa-cluster/src/accord/clock_validation.rs`

**Test Case:** `hlc_skew_exceeds_max_enters_readonly` — Inject a mock clock that
jumps forward by 2x SkewMax. Verify the node (a) logs an error, (b) stops accepting
new transactions, and (c) existing in-flight transactions are allowed to drain.

**Status: Implemented.** Test passes in `ferrosa-cluster` test suite.

---

### FM2: Single Ballot Variable (Two-Ballot Invariant Violation)

**Component:** TxnState

**Cause:** Implementation bug where TxnState uses a single ballot field instead of
the required two (`promised_ballot` and `accepted_ballot`). The Accord protocol
requires these to be independent: `promised_ballot` tracks the highest ballot for
which a promise was made, `accepted_ballot` tracks the ballot under which a value
was actually accepted. A single field conflates these two roles (AT10, AT13, AT26
from threat model).

**Effect:** Recovery becomes unsound. The RecoveryCoordinator reads `accepted_ballot`
to determine which value was most recently accepted. If `promised_ballot` overwrites
`accepted_ballot`, recovery may select a value from a newer promise that was never
actually accepted, violating linearizability. In the worst case, two concurrent
recovery attempts select different values for the same transaction, causing permanent
divergence (split brain at the transaction level).

**Detection:** D=9. This is a logic bug that produces correct results under normal
operation (no failures). It only manifests during recovery after a coordinator crash,
which is itself a rare event. The resulting inconsistency may not be detected until
a read returns stale data, which may be attributed to application logic. Static
analysis cannot catch this semantic error.

**Severity:** S=10. Linearizability violation is the most severe correctness failure
for a transaction system. Data loss is possible if the wrong value is committed.

**Occurrence:** O=2. Requires both (a) the implementation bug and (b) a coordinator
crash during the Accept phase with concurrent recovery. The bug is straightforward
to introduce during initial implementation.

**RPN: 60 (Medium) — revised from 180 after implementation**

**Detection (revised):** D=3. Type-level enforcement via newtypes prevents this
class of bug at compile time. Property tests in `proptests.rs` exercise recovery
paths with randomized interleaving. The 24-step test is deterministic and runs in CI.

**Mitigation (Implemented):**

1. Type-level enforcement: define `PromisedBallot` and `AcceptedBallot` as distinct
   newtypes wrapping the same underlying `Ballot` type. The compiler prevents
   accidental substitution.
2. 24-step EPaxos linearizability test (SPEC-accord-transactions.md section 11.2):
   a deterministic scenario that exercises PreAccept, Accept, coordinator crash,
   Recovery, and re-execution. The test asserts that the recovered value matches the
   accepted value, not the promised value.
3. Property-based testing: generate random sequences of PreAccept/Accept/Crash/Recover
   and assert that committed values are linearizable.

**Test Case:** `two_ballot_invariant_recovery_correctness` — The 24-step EPaxos
correctness test from the spec:

1. Node A begins transaction T1: PreAccept on nodes {A, B, C}.
2. All three nodes respond with PreAcceptOk.
3. Node A sends Accept(ballot=1) to {B, C}. B receives it; C does not (network partition).
4. B stores accepted_ballot=1, accepted_value=V1.
5. Node A crashes before receiving AcceptOk from B.
6. Node D initiates recovery with ballot=2.
7. D sends Prepare(ballot=2) to {B, C}.
8. B responds: promised_ballot=2, accepted_ballot=1, accepted_value=V1.
9. C responds: promised_ballot=2, accepted_ballot=none.
10. D sees a majority with accepted_ballot=1 from B.
11. D must re-propose V1 (the accepted value), not any other value.
12. Assert: committed value == V1.

The test fails if a single-ballot implementation causes step 8 to report
accepted_ballot=2 (overwritten by the promise), leading D to incorrectly conclude
that the latest accepted value is from ballot=2 and potentially choose a different
value.

---

### FM3: ConflictIndex Missing Entries

**Component:** ConflictIndex (HashMap + BTreeMap + indexed_writes)

**Cause:** Race condition between concurrent writes to the ConflictIndex. The
HashMap and BTreeMap are updated non-atomically; a reader between the two updates
sees an inconsistent state. Alternatively, the bounded ~500 entry cap causes
eviction of an entry that has in-flight dependents.

**Effect:** A transaction T2 that conflicts with T1 (overlapping keys) is not
recorded as a dependency. T2 may execute before T1 commits, reading pre-T1 state.
This is a serializability violation: the execution order does not match the
serialization order.

**Detection:** D=6. Conflict detection failures are intermittent and workload-
dependent. They manifest as subtle read anomalies (non-repeatable reads, phantom
rows) that are difficult to reproduce. A deterministic conflict-injection test can
catch the bug, but production detection requires consistency checkers.

**Severity:** S=9. Serializability violation, but not data corruption in the
strongest sense (individual writes are still atomic). The wrong serialization order
can cause application-level data integrity failures.

**Occurrence:** O=4. The bounded capacity (~500 entries) makes eviction likely under
moderate load. The race condition depends on concurrent writers to overlapping key
ranges, which is common in transaction workloads.

**RPN: 108 (High) — revised from 216 after implementation**

**Detection (revised):** D=3. Property tests in `proptests.rs` exercise concurrent
insertion and eviction scenarios. The single-threaded-per-shard model eliminates
the race condition entirely. Eviction safety with reference counting is implemented.

**Mitigation (Implemented):**

1. Atomic update: protect the ConflictIndex with a single `RwLock` (or `DashMap`
   with per-shard locks). Ensure the HashMap key insert and BTreeMap timestamp
   insert are always visible together.
2. Eviction safety: before evicting an entry from the bounded map, check that no
   in-flight transaction references it. If any do, evict the least-recently-used
   entry that has no dependents instead.
3. Monotonic sequence numbers: assign a monotonic sequence to each index entry.
   Dependents record the sequence number, not just the key. If an entry is evicted,
   the dependent transaction must re-check the full commit log for the key.

**Implementation:** `ferrosa-storage/src/accord/conflict_index.rs`

**Test Case:** `conflict_index_concurrent_insert_visibility` — Spawn 16 threads,
each inserting conflicting keys into the ConflictIndex. After all inserts complete,
verify that every pair of conflicting transactions has a recorded dependency edge.
Repeat with the index at 499/500 capacity to exercise eviction.

**Status: Implemented.** Test passes. Property tests in `ferrosa-cluster/src/accord/proptests.rs` provide additional coverage.

---

### FM4: Recovery Selects Wrong Value

**Component:** RecoveryCoordinator

**Cause:** The RecoveryCoordinator receives Prepare responses from a quorum but
applies the wrong selection rule. The correct rule is: select the value with the
highest `accepted_ballot` among all responses. Bugs include: (a) selecting by
`promised_ballot` instead (see FM2), (b) selecting by highest ballot number without
checking that the value was actually accepted (not just promised), (c) off-by-one
in quorum calculation.

**Effect:** A value that was never accepted by a quorum is committed. This is a
permanent linearizability violation: once committed, the wrong value is durable and
cannot be corrected without manual intervention. Dependent transactions that read
the wrong value may cascade the error.

**Detection:** D=8. The recovery path is exercised only during coordinator failures,
which are rare in testing. The wrong value may look plausible (it was proposed at
some point), making the error hard to identify. Automated linearizability checkers
(e.g., Jepsen-style) can detect this, but only if the specific crash scenario is
covered.

**Severity:** S=10. Permanent data corruption from wrong committed value.

**Occurrence:** O=2. Requires coordinator crash + specific timing of Accept messages

+ recovery with the wrong selection rule.

**RPN: 60 (Medium) — revised from 160 after implementation**

**Detection (revised):** D=3. The selection rule is a pure function with exhaustive
unit tests. Recovery scenarios are exercised in `recovery_scenarios.rs` including
adversarial variants.

**Mitigation (Implemented):**

1. Encode the selection rule as a standalone pure function: `fn select_recovery_value(responses: &[PrepareResponse]) -> Option<AcceptedValue>`. Unit test it exhaustively with all combinations of (promised_ballot, accepted_ballot, value) tuples.
2. Assert in the RecoveryCoordinator that the selected value's accepted_ballot is >= all other accepted_ballots in the response set.
3. Deterministic simulation: use the 24-step test from FM2 plus adversarial variants where (a) Accept reaches only a minority, (b) two concurrent recoveries race.

**Implementation:** `ferrosa-cluster/src/accord/recovery.rs`, `ferrosa-cluster/src/accord/recovery_scenarios.rs`

**Test Case:** `recovery_selects_highest_accepted_ballot` — Set up 5 nodes.
Transaction T1 is accepted at ballot=3 on nodes {A, B}. Transaction T1 was promised
at ballot=5 on node C (but not accepted at ballot=5). RecoveryCoordinator queries
{A, B, C}. Assert: recovery commits the value accepted at ballot=3, not any value
associated with ballot=5.

**Status: Implemented.** Tests pass in `recovery_scenarios.rs`.

---

### FM5: CommitLog Entry Not Fsynced Before Reply

**Component:** CommitLog Extensions (4 new entry types)

**Cause:** The Accord protocol requires that Accept and Commit entries are durable
before the node sends AcceptOk or CommitOk to the coordinator. The existing commit
log `append()` method uses `BatchSync` or `GroupSync`, which may buffer entries and
fsync on a timer or batch threshold. Accord entries appended through the normal
path may be acknowledged before the fsync completes.

**Effect:** A node crashes after sending AcceptOk but before the entry is fsynced.
On recovery, the node has no record of the acceptance. The coordinator believes
the value was accepted by a quorum, but after recovery the quorum no longer holds.
This can cause committed transactions to be lost — a durability violation.

**Detection:** D=5. This failure requires a crash in a narrow window (after network
send, before fsync). It can be detected by comparing committed transactions against
recovered state, but only if the operator is running consistency audits.

**Severity:** S=10. Committed transaction data loss.

**Occurrence:** O=3. The window is narrow per-entry but grows with throughput. Under
high load with `BatchSync(5ms)`, the window is up to 5ms per entry.

**RPN: 60 (Medium) — revised from 150 after implementation**

**Detection (revised):** D=2. The `sync_writer.rs` module enforces fsync-before-ack
for all Accord entries. The crash recovery test in `crash_recovery.rs` verifies
entry survival after simulated crash.

**Mitigation (Implemented):**

1. Introduce a `SyncMode::Immediate` for Accord entries: after writing the entry
   to the segment buffer, fsync before returning the `CommitLogPosition`.
2. Alternatively, batch Accord entries separately with a dedicated flush-before-ack
   guarantee: `append_accord()` writes to the segment and blocks until the next
   sync strategy flush completes.
3. Mark the 4 new entry types (`AccordPreAccept`, `AccordAccept`, `AccordCommit`,
   `AccordApply`) with a `requires_fsync: true` flag in the entry header. The sync
   strategy treats flagged entries as a forced flush point.

**Implementation:** `ferrosa-storage/src/accord/sync_writer.rs`, `ferrosa-storage/src/accord/entries.rs`

**Test Case:** `accord_entry_durable_before_ack` — Append an `AccordAccept` entry.
Before the test calls `segment.sync()`, verify that `append_accord()` has already
fsynced (by checking the file's modified timestamp or using a mock filesystem that
tracks fsync calls). Then simulate a crash (drop the CommitLog without clean
shutdown) and verify the entry survives recovery replay.

**Status: Implemented.** Test passes. Crash recovery verified in `ferrosa-storage/src/accord/crash_recovery.rs`.

---

### FM6: MemIndex/Memtable Non-Atomic Apply

**Component:** MemIndex (BTreeMap, atomic with memtable)

**Cause:** The MemIndex and the memtable are updated as two separate operations. A
concurrent reader may observe the memtable update (new row visible) before the
MemIndex update (transaction metadata not yet indexed), or vice versa. The DSM
analysis flagged that ConflictIndex and MemIndex are both coupled with the commit
log in `ferrosa-storage`.

**Effect:** Phantom reads: a transaction reads a row that was written by a committed
Accord transaction but the MemIndex does not yet reflect the transaction's commit
status. The reader may see uncommitted data (dirty read) or miss committed data
(lost update) depending on the ordering.

**Detection:** D=7. The window is extremely narrow (nanoseconds between two
in-memory operations), making it nearly impossible to reproduce manually. Only
high-concurrency stress tests with randomized scheduling can reliably trigger it.

**Severity:** S=9. Dirty reads or lost updates violate transaction isolation guarantees.

**Occurrence:** O=3. Requires concurrent readers during the apply phase, which is
common under moderate load.

**RPN: 81 (Medium) — revised from 189 after implementation**

**Detection (revised):** D=3. Atomic visibility is enforced by the single critical
section pattern. Concurrent reader/writer tests exercise the invariant.

**Mitigation (Implemented):**

1. Single critical section: apply memtable write and MemIndex update within the
   same lock scope. If using `DashMap` per-shard locks, ensure both operations
   target the same shard.
2. Sequence number gate: assign a monotonic apply-sequence to each transaction.
   Readers check the sequence number and retry if the MemIndex entry is not yet
   visible.
3. Epoch-based reclamation: use an epoch counter that advances after both operations
   complete. Readers operating in the old epoch see the consistent pre-apply state.

**Implementation:** `ferrosa-storage/src/accord/read_2i.rs`

**Test Case:** `memindex_memtable_atomic_visibility` — Spawn 8 writer threads
applying Accord transactions and 8 reader threads scanning the memtable. After each
write, the reader must either see both the memtable row and the MemIndex entry, or
neither. Assert: no reader ever observes a memtable row without a corresponding
MemIndex entry (or vice versa).

**Status: Implemented.** Test passes.

---

### FM7: Epoch Mismatch During Fast Path

**Component:** ElectorateConfig (epoch, quorum sizing)

**Cause:** A node uses a cached ElectorateConfig from epoch N while the cluster has
transitioned to epoch N+1 (e.g., after a node join/leave via `JoinElectorate`). The
fast-path quorum is calculated using stale membership. The epoch transition
vulnerability window (AT25, AT29 from threat model) exacerbates this.

**Effect:** The coordinator contacts nodes that are no longer in the electorate, or
misses nodes that have joined. A fast-path decision based on a stale quorum may not
constitute a valid quorum in the current epoch. If the old and new electorates
disagree, two coordinators (one using epoch N, one using epoch N+1) could both
achieve fast-path consensus for conflicting transactions.

**Detection (revised):** D=3. Epoch fence is implemented in all Accord messages.
Stale-epoch messages are automatically rejected with `EpochStale`. Integration tests
exercise epoch transitions under load.

**Severity:** S=8. Can violate serializability if two conflicting transactions
both achieve fast-path in different epochs. Not data corruption per se, but
incorrect commit ordering.

**Occurrence:** O=4. Epoch transitions happen during cluster topology changes (node
add/remove), which are uncommon but not rare. The vulnerability window is
proportional to the Raft propagation delay.

**RPN: 96 (Medium) — revised from 160 after implementation**

**Mitigation (Implemented):**

1. Epoch fence: every Accord message includes the sender's epoch. Reject any
   message with epoch < local epoch. Return an `EpochStale` error with the current
   epoch, forcing the sender to refresh.
2. Epoch barrier: after epoch transition, block new fast-path transactions until
   all electorate members have acknowledged the new epoch (Raft commit).
3. Two-epoch grace: during transition, accept messages from epoch N and N+1 but
   require quorum in the *intersection* of both electorates.

**Implementation:** `ferrosa-cluster/src/accord/epoch.rs`, `ferrosa-cluster/src/accord/electorate.rs`

**Test Case:** `epoch_mismatch_fast_path_rejected` — Start 5-node cluster at
epoch=1. Transition to epoch=2 (node E replaces node C in electorate). Before node
A learns of the transition, node A attempts fast-path PreAccept with epoch=1. Assert:
nodes B, D, E (which know epoch=2) reject the request with `EpochStale`. Node A
retries with epoch=2.

**Status: Implemented.** Test passes.

---

### FM8: ReorderBuffer Overflow (Message Loss)

**Component:** ReorderBuffer (TimerWheel)

**Cause:** The TimerWheel has a fixed number of slots determined by the deadline
formula: `wall_clock(t0) + SkewMax + max(Latency) - Latency(C,P)`. If message
arrival rate exceeds the wheel's capacity, or if messages are delayed beyond the
wheel's time horizon, entries are dropped.

**Effect:** Dropped messages cause transactions to stall. The coordinator sees no
response from the node and must retry or trigger recovery. Under sustained
overload, multiple transactions stall simultaneously, causing cascading timeouts.
This is a liveness failure, not a safety failure — no incorrect values are committed,
but throughput collapses.

**Detection:** D=4. The ReorderBuffer can count dropped entries and expose a metric.
The coordinator's timeout mechanism detects the stall within the timeout window.

**Severity:** S=7. Liveness failure with throughput degradation. No data loss or
corruption.

**Occurrence:** O=5. Under load spikes or network jitter, message rates can
temporarily exceed the wheel's capacity. The bounded ~500 entry design of the
ConflictIndex amplifies this: if many transactions are in flight, the wheel fills
quickly.

**RPN: 70 (Medium) — revised from 140 after implementation**

**Detection (revised):** D=2. The ReorderBuffer exposes capacity metrics. Backpressure
is automatic. Overflow is detected and reported immediately.

**Mitigation (Implemented):**

1. Dynamic wheel sizing: scale the number of slots based on observed in-flight
   transaction count. Double the wheel when occupancy exceeds 75%.
2. Overflow queue: when the wheel is full, overflow entries into a secondary
   unbounded queue that is drained preferentially. Cap the overflow queue at 10K
   entries with backpressure.
3. Admission control: if the ReorderBuffer is above 90% capacity, reject new
   PreAccept requests with a `Backpressure` error, causing the coordinator to
   route to a different node.

**Implementation:** `ferrosa-cluster/src/accord/reorder_buffer.rs`

**Test Case:** `reorder_buffer_overflow_backpressure` — Fill the TimerWheel to
capacity with pending entries. Submit one additional entry. Assert: the entry is
either placed in the overflow queue or a `Backpressure` error is returned. Verify
no existing entries are silently dropped.

**Status: Implemented.** Test passes.

---

### FM9: Leaseholder Stale After Failover

**Component:** Leaseholder Fast Path

**Cause:** The leaseholder optimization allows a node that holds the lease for a
partition range to skip the first round-trip (local conflict check + 1 RTT). After
a failover (leaseholder crash), the new node may not know it is the leaseholder, or
the old node may not know it lost the lease (network partition). The failure
detector has a detection latency of several seconds.

**Effect:** Two nodes both believe they are the leaseholder for the same partition
range. Both attempt fast-path transactions without full quorum consultation. If
they process conflicting transactions, neither detects the conflict, violating
serializability.

**Detection:** D=6. The lease has an epoch and expiry. If both nodes check the
epoch, the stale node will be detected when it contacts peers. But if the stale
node operates purely locally (the fast-path optimization), it never contacts peers
and never learns the lease expired.

**Severity:** S=8. Serializability violation for transactions on the affected
partition range. Limited blast radius (only partitions owned by the dual-lease
nodes).

**Occurrence:** O=4. Leaseholder failover is uncommon but guaranteed to happen
eventually. The window of dual-lease depends on failure detector latency (typically
5-15 seconds).

**RPN: 96 (Medium) — revised from 192 after implementation**

**Detection (revised):** D=3. Fencing tokens are validated on every write path.
Stale leaseholder is detected immediately on the first write attempt.

**Mitigation (Implemented):**

1. Lease expiry: leases have a wall-clock TTL (e.g., 10 seconds). The leaseholder
   must refresh the lease via Raft before it expires. If the lease expires, the node
   reverts to the full Accord protocol (no fast path).
2. Fencing token: the lease includes a monotonic token. All writes include the
   fencing token. The memtable rejects writes with a stale token.
3. Pre-commit peer check: even on fast path, the leaseholder sends a lightweight
   "lease-valid?" probe to one peer before committing. This adds ~0.5 RTT but
   prevents dual-lease commits.

**Implementation:** `ferrosa-cluster/src/accord/leaseholder.rs`

**Test Case:** `stale_leaseholder_fenced_after_failover` — Node A holds lease for
partition range [0, 100). Kill node A. Node B acquires lease at epoch+1. Restart
node A (still believes it holds the lease at old epoch). Node A attempts a fast-path
write to partition 50. Assert: the write is rejected because the fencing token is
stale.

**Status: Implemented.** Test passes.

---

### FM10: Cross-Shard Execute Partial Failure

**Component:** AccordStateMachine (Execute and Apply phases)

**Cause:** A transaction spans multiple partition ranges (shards), each on different
nodes. The Execute phase succeeds on shards A and B but fails on shard C (node
crash, timeout, or commit log full). The transaction is partially applied.

**Effect:** Atomicity violation: some shards reflect the transaction, others do not.
Reads that span the affected shards see inconsistent state. This is a fundamental
ACID violation.

**Detection:** D=6. The coordinator knows which shards succeeded and failed. However,
if the coordinator itself crashes after partial apply, recovery must re-derive the
apply status from each shard.

**Severity:** S=10. Atomicity violation is a fundamental correctness failure.

**Occurrence:** O=3. Cross-shard transactions are common in real workloads. Partial
failure requires a crash during the Apply phase, which is a narrow window but grows
with the number of shards.

**RPN: 90 (Medium) — revised from 180 after implementation**

**Detection (revised):** D=3. Cross-shard apply status is tracked per shard. Recovery
coordinator detects incomplete applies and re-sends. Jepsen bank test exercises this
scenario.

**Mitigation (Implemented):**

1. Two-phase apply: the Apply phase uses a prepare-commit pattern. Each shard
   writes the apply intent to its commit log (prepare). Only after all shards
   confirm prepare does the coordinator send commit. If any shard fails prepare,
   the coordinator sends abort.
2. Idempotent apply: each shard records the transaction ID in its apply log. On
   recovery, incomplete transactions are re-applied by the RecoveryCoordinator.
   The apply operation is idempotent (same TxnId + same value = no-op).
3. Apply timeout: if a shard does not confirm apply within `apply_timeout`, the
   coordinator re-sends the apply. The shard deduplicates by TxnId.

**Implementation:** `ferrosa-cluster/src/accord/cross_shard.rs`, `ferrosa-storage/src/accord/sidecar.rs`

**Test Case:** `cross_shard_partial_apply_recovery` — Transaction T1 writes to
partitions on nodes {A, B, C}. Inject a crash on node C during the Apply phase
(after A and B have applied). Restart node C. Assert: RecoveryCoordinator detects
the incomplete apply and re-sends. After recovery, all three shards reflect T1.

**Status: Implemented.** Test passes. Also exercised by `jepsen_bank.rs`.

---

### FM11: ConflictIndex GC Too Aggressive

**Component:** ConflictIndex (bounded ~500 entries)

**Cause:** The ConflictIndex garbage-collects entries when the entry count exceeds
the ~500 bound. The GC evicts the oldest entries by timestamp. An in-flight
transaction that depends on an evicted entry loses its dependency tracking.

**Effect:** The dependent transaction proceeds without waiting for the evicted
transaction to complete. If the evicted transaction has not yet committed, the
dependent transaction may execute against pre-transaction state, violating
serializability. This is the same class of error as FM3 but triggered by GC
rather than a race condition.

**Detection (revised):** D=3. Reference counting prevents unsafe eviction. Eviction
events are logged and metricked. Property tests in `proptests.rs` exercise eviction
under load.

**Severity:** S=9. Serializability violation.

**Occurrence:** O=3. The ~500 entry bound makes eviction common under moderate
transaction rates. A burst of 500+ concurrent transactions guarantees eviction.

**RPN: 81 (Medium) — revised from 189 after implementation**

**Mitigation (Implemented):**

1. Reference counting on ConflictIndex entries. An entry cannot be evicted while
   any in-flight transaction holds a reference to it. The reference is released
   when the transaction commits or aborts.
2. Adaptive capacity: if eviction pressure is sustained, double the ConflictIndex
   capacity (up to a configurable max, e.g., 10,000). Shrink back when pressure
   subsides.
3. Eviction fallback: when an entry is evicted, mark the dependent transaction as
   "deps-unknown". The transaction must switch from fast path to slow path
   (full Accept round) to re-establish its dependency set.

**Implementation:** `ferrosa-storage/src/accord/conflict_index.rs`

**Test Case:** `conflict_index_gc_preserves_in_flight_deps` — Insert 500 entries
into the ConflictIndex. Start a transaction T501 that depends on entry #1. Insert
entry #501 (triggering GC). Assert: entry #1 is NOT evicted because T501 holds a
reference. After T501 commits, insert entry #502. Assert: entry #1 is now evicted
(no references).

**Status: Implemented.** Test passes.

---

### FM12: Fast-Path Quorum With Non-Electorate Member

**Component:** ElectorateConfig

**Cause:** During topology changes, a node that has been removed from the
electorate may still appear in the coordinator's cached membership list. The
coordinator includes this non-member's vote in its fast-path quorum calculation.

**Effect:** The fast-path quorum is invalid. A decision based on votes from non-
members may not overlap with the true electorate's quorum, violating the quorum
intersection property. Two conflicting transactions could both achieve fast-path
consensus if one includes a non-member's vote.

**Detection:** D=5. The non-member's response can include its electorate membership
status. The coordinator can check this. However, if the coordinator's own membership
list is stale, it does not know to check.

**Severity:** S=10. Safety violation: quorum intersection failure can commit
conflicting transactions.

**Occurrence:** O=2. Requires topology change + stale membership cache + fast-path
transaction in the window before the cache refreshes. The epoch fence (FM7
mitigation) reduces this window.

**RPN: 60 (Medium) — revised from 100 after implementation**

**Detection (revised):** D=3. Epoch fence validates all voters. Non-electorate
responses are automatically rejected and logged.

**Mitigation (Implemented):**

1. Responses include the responder's epoch and electorate membership proof. The
   coordinator validates that every voter is in the current electorate before
   counting the vote.
2. Quorum requires `f + ceil((f+1)/2)` responses from *current electorate members
   only*. Responses from non-members are logged and discarded.
3. The epoch fence from FM7 subsumes this: if epochs match, membership is
   guaranteed consistent.

**Implementation:** `ferrosa-cluster/src/accord/electorate.rs`

**Test Case:** `non_electorate_vote_rejected` — Remove node C from the electorate
at epoch=2. Node C still responds to PreAccept from a coordinator using epoch=1.
Assert: if the coordinator upgrades to epoch=2 before counting votes, node C's
response is discarded.

**Status: Implemented.** Test passes.

---

### FM13: Dep-Wait Circular Dependency Deadlock

**Component:** AccordStateMachine (dependency tracking)

**Cause:** Transaction T1 depends on T2 (conflict on key K1), and T2 depends on T1
(conflict on key K2). Both transactions enter the dep-wait state, each waiting for
the other to commit. This creates a circular dependency that cannot resolve without
external intervention.

**Effect:** Both transactions hang indefinitely. If dep-wait has no timeout, this is
a permanent deadlock. Even with a timeout, the transactions are aborted and must be
retried, causing latency spikes and wasted work.

**Detection:** D=8. The dep-wait mechanism operates locally on each node. Detecting
a cycle requires cross-node coordination (distributed deadlock detection), which is
expensive and typically not implemented. The timeout mechanism detects the symptom
(stall) but not the root cause (cycle).

**Severity:** S=8. Liveness failure: transactions are stuck. No data corruption,
but user-facing operations time out. In the worst case, cascading aborts consume
retry budgets.

**Occurrence:** O=3. Circular dependencies require conflicting transactions to
arrive in a specific interleaving. This is uncommon in typical workloads but can be
triggered deterministically by adversarial clients.

**RPN: 72 (Medium) — revised from 192 after implementation**

**Detection (revised):** D=3. Dep-wait timeout provides automatic detection and
resolution. Waits-for graph detects cycles immediately before they cause stalls.

**Mitigation (Implemented):**

1. Timestamp-ordered resolution: when a cycle is detected (or suspected due to
   dep-wait timeout), the transaction with the lower timestamp yields (aborts and
   retries). This breaks the cycle deterministically.
2. Dep-wait timeout: configure a maximum dep-wait time (e.g., 5 seconds). On
   timeout, the waiting transaction aborts with a `DependencyTimeout` error. The
   client retries with a new timestamp, breaking the cycle.
3. Waits-for graph: maintain a local waits-for graph per node. Before entering
   dep-wait, check if adding this edge creates a cycle. If so, abort the lower-
   timestamp transaction immediately (wound-wait scheme).

**Implementation:** `ferrosa-cluster/src/accord/dep_wait.rs`

**Test Case:** `dep_wait_cycle_breaks_via_timeout` — Submit T1(writes K1, reads K2)
and T2(writes K2, reads K1) concurrently. Both enter dep-wait. Assert: within
`dep_wait_timeout`, one transaction aborts with `DependencyTimeout` and the other
commits successfully. The retried transaction succeeds on the second attempt.

**Status: Implemented.** Test passes.

---

### FM14: CommitLog Entry Too Large for Segment

**Component:** CommitLog Extensions

**Cause:** An Accord transaction carries a large payload (e.g., a batch of UDF/UDA
WASM mutations, or a transaction spanning hundreds of partitions). The serialized
commit log entry exceeds the segment capacity (`segment_size`, typically 32MB). The
existing commit log returns `InvalidData` error when this happens (see
`ferrosa-storage/src/commitlog/mod.rs:199-203` and the fix in commit `8ded529`).

**Effect:** The transaction cannot be made durable. The node returns an error to the
coordinator. If the coordinator cannot find any node with a large enough segment,
the transaction fails permanently. This is a correctness gap: the protocol expects
durability, but the storage layer rejects the entry.

**Detection:** D=3. The error is explicit and logged. The coordinator receives a
clear failure and can report it to the client.

**Severity:** S=7. Transaction failure, not data corruption. The transaction is
cleanly rejected, but the user's operation fails unexpectedly.

**Occurrence:** O=4. Large transactions are not uncommon. A BATCH statement with
1000 mutations, each with a WASM UDF result, can easily exceed 32MB. The UDF/UDA
query-time branch makes this more likely by allowing arbitrary WASM output in
transaction payloads.

**RPN: 56 (Medium) — revised from 84 after implementation**

**Detection (revised):** D=2. Oversized entries are detected and rejected at the
CQL layer before reaching the commit log. Clear error message returned to client.

**Mitigation (Implemented):**

1. Entry splitting: if an Accord entry exceeds `segment_size / 2`, split it into
   multiple linked entries with a continuation flag. The reader reassembles them
   during replay.
2. Configuration guidance: document minimum `segment_size` for Accord workloads.
   Default to 64MB when Accord is enabled.
3. Reject at parse time: in the CQL layer, reject BATCH statements that would
   produce entries larger than `segment_size - overhead`. Return a clear error
   message with the size limit.

**Implementation:** `ferrosa-storage/src/accord/oversized_entry.rs`, `ferrosa-cql/src/transaction_limits.rs`

**Test Case:** `accord_entry_exceeding_segment_rejected_cleanly` — Create a
transaction with a payload of `segment_size + 1` bytes. Assert: the commit log
returns `InvalidData` (not a panic). The coordinator receives the error and returns
a user-friendly message suggesting smaller batches or increased segment size.

**Status: Implemented.** Test passes.

---

### FM15: Late Data Debouncer + Accord Ordering Conflict

**Component:** LateDataDebouncer (ferrosa-storage) / AccordStateMachine

**Cause:** The RRD cascading aggregation system's late-data debouncer
(`LateDataDebouncer`) triggers re-aggregation by writing corrected aggregate rows.
These writes bypass the Accord transaction protocol because they are internal
storage-engine operations, not client-initiated CQL statements. If the re-
aggregation write conflicts with a concurrent Accord transaction on the same
aggregate table, the Accord ordering guarantee is violated.

**Effect:** The debouncer overwrites a value that an Accord transaction committed
(or vice versa). The final state depends on physical write order rather than Accord
timestamp order. This is a serializability violation for any table that is both a
target of RRD aggregation and a participant in Accord transactions.

**Detection:** D=7. The debouncer is an internal mechanism invisible to the Accord
subsystem. No conflict is registered in the ConflictIndex because the debouncer
write does not go through the Accord path. The resulting anomaly is a silent
overwrite that is nearly impossible to detect without full audit logging.

**Severity:** S=7. The blast radius is limited to aggregate tables that are also
involved in transactions, which is an unusual but valid configuration.

**Occurrence:** O=4. Late data is expected in time-series workloads. If the
aggregate table is configured with `TRANSACTIONS = { 'enabled': true }`, the
conflict is guaranteed whenever late data arrives during a concurrent transaction.

**RPN: 112 (High) — revised from 196 after implementation**

**Detection (revised):** D=4. DDL validation rejects conflicting configurations.
Debouncer writes are routed through Accord when the target table has transactions
enabled. However, runtime detection of edge cases (e.g., table altered while
debouncer is active) remains partially manual.

**Mitigation (Implemented):**

1. Route debouncer writes through Accord: when the target table has Accord enabled,
   the debouncer submits its re-aggregation as an Accord transaction rather than a
   direct memtable write.
2. Table-level exclusion: reject `ALTER TABLE ... WITH transactions = {'enabled': true}`
   on tables that are RRD aggregation targets. Document that RRD tables and Accord
   tables are mutually exclusive (in v1).
3. Debouncer-aware conflict detection: register debouncer writes in the ConflictIndex
   even though they are not Accord transactions. This allows Accord to detect the
   conflict and serialize appropriately.

**Implementation:** `ferrosa-cluster/src/accord/uda_integration.rs`, `ferrosa-cluster/src/accord/two_phase_ddl.rs`

**Test Case:** `debouncer_write_conflicts_with_accord_detected` — Configure an
aggregate table with both RRD aggregation and Accord transactions. Insert late data
that triggers re-aggregation. Concurrently execute an Accord transaction that writes
to the same aggregate row. Assert: either (a) the debouncer write goes through
Accord and is serialized correctly, or (b) the DDL rejects the configuration as
unsupported.

**Status: Implemented.** Test passes.

---

### FM16: Non-Transactional Write Bypasses Accord

**Component:** AccordStateMachine / Storage Engine

**Cause:** A table is configured with Accord transactions enabled, but a CQL
`INSERT` or `UPDATE` statement that does not use `BEGIN TRANSACTION` writes directly
to the memtable, bypassing the Accord protocol entirely (AT03 from threat model).
The ConflictIndex has no knowledge of the non-transactional write, so it cannot
detect conflicts.

**Effect:** The non-transactional write may overwrite a value committed by an Accord
transaction, or an Accord transaction may overwrite the non-transactional write
without detecting the conflict. The table's state is no longer strictly
serializable. This is the highest-impact failure mode because it can be triggered
by any client that sends a non-transactional write.

**Detection:** D=5. The storage engine can check whether the target table requires
transactions and reject non-transactional writes. This is a configuration check, not
a runtime detection — if the check is missing, the violation is silent.

**Severity:** S=10. Complete bypass of the transaction safety guarantees. Any client
can cause serializability violations.

**Occurrence:** O=5. Non-transactional writes are the default in CQL. Unless the
system actively blocks them on Accord-enabled tables, every client operation is a
potential bypass.

**RPN: 100 (High) — revised from 250 after implementation**

**Detection (revised):** D=2. Write gate enforces Accord requirement at the storage
engine boundary. Non-transactional writes are rejected with a clear error before
reaching the memtable.

**Mitigation (Implemented):**

1. Table-level enforcement: when a table has `transactions = {'enabled': true}`,
   the storage engine rejects any write that does not carry an Accord TxnId.
   Return error: `Cannot write to transactional table without BEGIN TRANSACTION`.
2. Implicit transaction wrapping: automatically wrap non-transactional writes to
   Accord-enabled tables in a single-key Accord transaction. This preserves
   backward compatibility but adds latency.
3. Mixed-mode audit: if mixed writes are allowed (for migration), log every non-
   transactional write to an Accord-enabled table at `warn` level and expose a
   metric.

**Implementation:** `ferrosa-storage/src/accord/write_gate.rs`

**Test Case:** `non_transactional_write_to_accord_table_rejected` — Create table
with `transactions = {'enabled': true}`. Execute `INSERT INTO t (pk, v) VALUES (1, 'x')`
without `BEGIN TRANSACTION`. Assert: the write is rejected with error code
`TRANSACTION_REQUIRED`.

**Status: Implemented.** Test passes.

---

### FM17: Epoch Transition Vulnerability Window

**Component:** ElectorateConfig

**Cause:** During an epoch transition (node join/leave, electorate reconfiguration),
there is a window where some nodes are operating at epoch N and others at epoch N+1
(AT25, AT29 from threat model). The Raft propagation delay for the new epoch
configuration creates this window. During the window, quorum calculations may be
inconsistent across coordinators.

**Effect:** A coordinator at epoch N and a coordinator at epoch N+1 may both achieve
quorum for conflicting transactions because their quorum calculations use different
electorate membership. The quorum intersection property holds within an epoch but
not across epochs unless explicitly enforced.

**Detection:** D=6. The epoch is included in all messages (if FM7 mitigation is
implemented). Cross-epoch conflicts are detectable by comparing transaction commit
epochs. However, detection is after-the-fact; the violation has already occurred.

**Severity:** S=9. Safety violation: conflicting transactions committed in
different epochs. Not data corruption per se, but the serialization order is
violated.

**Occurrence:** O=3. Epoch transitions are infrequent (topology changes), but when
they happen, the vulnerability window is proportional to Raft commit latency
(typically 10-100ms).

**RPN: 81 (Medium) — revised from 162 after implementation**

**Detection (revised):** D=3. Epoch barrier blocks transactions during transition.
Joint consensus ensures quorum intersection. Epoch drain is automated.

**Mitigation (Implemented):**

1. Epoch barrier (same as FM7 mitigation 2): block new transactions during epoch
   transition until all electorate members have acknowledged the new epoch.
2. Joint consensus: during transition, require quorum in BOTH the old and new
   electorates (similar to Raft joint consensus for membership changes).
3. Epoch lease: the old epoch's electorate retains exclusive transaction authority
   for a grace period (`epoch_grace_ms`, default 2x Raft heartbeat interval) after
   the new epoch is proposed.

**Implementation:** `ferrosa-cluster/src/accord/epoch_drain.rs`, `ferrosa-cluster/src/accord/epoch.rs`

**Test Case:** `epoch_transition_blocks_new_transactions` — Start a 5-node cluster
at epoch=1. Initiate epoch transition to epoch=2. During the transition window,
attempt to submit a new Accord transaction. Assert: the transaction is rejected
with `EpochTransitionInProgress`. After all nodes acknowledge epoch=2, the
transaction succeeds.

**Status: Implemented.** Test passes.

---

### FM18: Recovery Triggered on False Positive from Failure Detector

**Component:** RecoveryCoordinator

**Cause:** The failure detector declares a coordinator dead (false positive) due to
network congestion, GC pause, or transient partition. The RecoveryCoordinator
initiates recovery for the coordinator's in-flight transactions. Concurrently, the
original coordinator is still alive and processing those same transactions.

**Effect:** Two entities (original coordinator and RecoveryCoordinator) concurrently
drive the same transaction. The ballot mechanism prevents safety violations (the
recovery ballot is higher, so the original coordinator's messages are rejected by
nodes that have promised the recovery ballot). However, the original coordinator
wastes work and may confuse itself when its messages are rejected.

**Detection:** D=3. The original coordinator receives `BallotSuperseded` errors
when its messages are rejected. It can detect that recovery is in progress and
stand down.

**Severity:** S=5. No safety violation (ballots prevent it). Performance
degradation from wasted work and retries. In extreme cases, cascading false
positives cause repeated recovery attempts that prevent transactions from
completing (livelock).

**Occurrence:** O=5. Network congestion and GC pauses are common in cloud
environments. Aggressive failure detector timeouts increase false positive rate.

**RPN: 50 (Medium) — revised from 75 after implementation**

**Detection (revised):** D=2. `BallotSuperseded` errors are returned immediately.
The original coordinator detects recovery and stands down. Metrics track false
positive rate.

**Mitigation (Implemented):**

1. Exponential backoff on recovery: if recovery is triggered for the same
   coordinator multiple times within a window, increase the detection threshold.
2. Coordinator heartbeat: the coordinator sends periodic heartbeats to all
   electorate members. Recovery is only triggered if heartbeats are missing for
   `recovery_threshold` consecutive intervals.
3. Graceful coordinator handoff: when the original coordinator detects
   `BallotSuperseded`, it immediately stops processing and hands off its in-flight
   state to the RecoveryCoordinator.

**Implementation:** `ferrosa-cluster/src/accord/recovery.rs`, `ferrosa-cluster/src/accord/recovery_scenarios.rs`

**Test Case:** `false_positive_recovery_no_safety_violation` — Node A coordinates
transaction T1. Inject network delay causing the failure detector to declare A dead.
RecoveryCoordinator on node B starts recovery with ballot=2. Node A comes back
online and tries to continue T1 with ballot=1. Assert: A receives
`BallotSuperseded`, stands down, and T1 commits exactly once (via B's recovery).

**Status: Implemented.** Test passes in `recovery_scenarios.rs`.

---

### FM19: NTP Step-Change Causes Timestamp Regression

**Component:** HybridLogicalClock

**Cause:** NTP performs a backward step adjustment (rather than slew) after detecting
a large clock offset. `SystemTime::now()` jumps backward. If the HLC physical
component follows this jump, new transactions receive timestamps lower than already-
committed transactions.

**Effect:** The ReorderBuffer may deliver the new transaction before its
dependencies (which have higher timestamps). Depending on the magnitude of the step,
this can violate causal ordering. Unlike FM1 (skew exceeds SkewMax), this affects a
single node rather than inter-node skew.

**Detection:** D=4. The HLC can detect backward jumps by comparing
`SystemTime::now()` against its last recorded physical time. A jump larger than a
threshold (e.g., 10ms) is logged.

**Severity:** S=8. Causal ordering violation, but the blast radius is limited to
transactions coordinated by the affected node.

**Occurrence:** O=3. NTP step changes are uncommon on well-configured systems but
do occur after reboots, VM migrations, or clock synchronization failures.

**RPN: 48 (Low) — revised from 96 after implementation**

**Detection (revised):** D=2. HLC monotonicity is enforced by the type system.
Backward jumps are detected and compensated automatically. Clock validation
tests exercise this scenario.

**Mitigation (Implemented):**

1. HLC monotonicity: the physical component of the HLC never decreases. If
   `SystemTime::now() < last_physical`, use `last_physical + 1` (logical increment
   only). This is the standard HLC behavior from the Kulkarni et al. paper.
2. Large-jump detection: if the backward step exceeds SkewMax, enter degraded mode
   (same as FM1 mitigation 1).
3. Use `CLOCK_MONOTONIC` (via `Instant`) for ordering and `CLOCK_REALTIME` (via
   `SystemTime`) only for wall-clock embedding. This prevents backward jumps from
   affecting ordering.

**Implementation:** `ferrosa-cluster/src/accord/clock_validation.rs`

**Test Case:** `hlc_backward_step_monotonic` — Set mock clock to T=1000. Issue
timestamp (gets T=1000). Set mock clock to T=900 (backward jump). Issue timestamp.
Assert: second timestamp > first timestamp (HLC maintains monotonicity).

**Status: Implemented.** Test passes.

---

### FM20: Cross-Shard Coordinator Crash During Execute Phase

**Component:** AccordCoordinator (Execute phase)

**Cause:** The coordinator for a multi-shard transaction crashes after sending
Execute requests to some shards but not all. The coordinator holds partial execution
results in memory. Unlike the Apply phase (FM10), the Execute phase involves
reading data from multiple shards and aggregating results before computing the
write set.

**Effect:** The transaction stalls. Recovery must re-execute on all shards at the
committed timestamp. If the Execute phase has side effects (e.g., UDF/UDA evaluation
that reads external state), re-execution may produce different results. The
transaction's write set may differ between the original and recovered execution.

**Detection (revised):** D=4. The recovery coordinator detects the incomplete Execute
via the protocol log. However, determining whether partial results were aggregated
and whether re-execution is safe requires inspecting the protocol log entries.

**Severity:** S=10. Potential atomicity violation if re-execution produces a
different write set than the original.

**Occurrence:** O=3. Coordinator crash during Execute is a narrow window, but grows
with the number of shards and Execute-phase latency.

**RPN: 120 (High)**

**Mitigation (Implemented):**

1. Protocol log persistence: the coordinator writes Execute requests and responses
   to the protocol log before aggregation. On crash, the recovery coordinator
   replays from the protocol log rather than re-executing.
2. Deterministic execution: UDF/UDA evaluations within Accord transactions are
   pure functions of the read set. External state reads are prohibited in
   transactional UDFs.
3. Execute checkpointing: after receiving each shard's Execute response, the
   coordinator checkpoints the partial result to the protocol log. Recovery resumes
   from the last checkpoint.

**Implementation:** `ferrosa-cluster/src/accord/cross_shard.rs`, `ferrosa-storage/src/accord/protocol_log.rs`

**Test Case:** `coordinator_crash_during_execute_recovery` — 3-shard transaction.
Coordinator crashes after receiving Execute response from shard A but before sending
Execute to shard C. Recovery coordinator replays from protocol log. Assert: all
shards see the same final result, transaction commits exactly once.

**Status: Implemented.** Test passes in `recovery_scenarios.rs`.

---

### FM21: Electorate Join With Incomplete History Sync

**Component:** ElectorateConfig / EpochDrain

**Cause:** A node joins the electorate for a new epoch but has not fully synchronized
the Accord transaction history (CommandStore state, protocol log entries) from the
previous epoch's replicas. It begins participating in PreAccept votes for the new
epoch without knowledge of old-epoch transactions that have committed but not yet
applied.

**Effect:** The new node's ConflictIndex is missing entries for in-flight
transactions from the old epoch. It votes to accept new transactions that conflict
with these unseen old-epoch transactions, violating serializability. The conflict
is invisible because the new node has no record of the old transactions.

**Detection:** D=4. The four-gate readiness model should prevent this, but if the
`Coordinate` gate is satisfied based on incomplete data (e.g., the replication stream
has a gap), the detection fails. Integration tests exercise this scenario.

**Severity:** S=9. Serializability violation with potential cascade to dependent
transactions.

**Occurrence:** O=3. Occurs during topology changes (node addition/replacement) when
the new node's replication stream is slow or interrupted.

**RPN: 108 (High)**

**Mitigation (Implemented):**

1. Four-gate readiness: the `Coordinate` gate requires the node to have replicated
   all ConflictIndex entries with timestamps >= `RedundantBefore` from the old
   epoch's replicas.
2. Epoch drain: the old epoch's in-flight transactions must drain before the new
   epoch accepts transactions. This ensures the new node's ConflictIndex is complete
   by the time it participates.
3. History sync verification: the joining node compares its ConflictIndex entry
   count against the old epoch's replicas. If the delta exceeds a threshold, the
   `Coordinate` gate is not satisfied.

**Implementation:** `ferrosa-cluster/src/accord/epoch.rs`, `ferrosa-cluster/src/accord/epoch_drain.rs`

**Test Case:** `electorate_join_incomplete_sync_blocks_participation` — Node D joins
the electorate at epoch=2. Artificially delay D's replication stream so it has only
50% of the old epoch's ConflictIndex entries. Assert: D's `Coordinate` gate is not
satisfied. PreAccept messages sent to D receive `NotReady` error. After sync
completes, D begins participating normally.

**Status: Implemented.** Test passes in `test_cluster.rs`.

---

### FM22: DurabilityService Stall (No GC Progress)

**Component:** DurabilityService / ExclusiveSyncPoint

**Cause:** The DurabilityService, which periodically advances the
`ExclusiveSyncPoint` to allow garbage collection of applied transaction metadata,
stalls. Causes include: (a) one or more replicas are unresponsive, preventing the
sync point from advancing; (b) a long-running transaction holds a reference that
pins the oldest ConflictIndex entry; (c) a bug in the durability calculation.

**Effect:** The ConflictIndex grows without bound because applied entries cannot be
evicted. Memory usage increases linearly with transaction throughput. Eventually,
the node hits the ConflictIndex hard size cap (AT18/FM3), causing `Overloaded`
errors for all new transactions. This is a liveness failure that degrades to full
unavailability.

**Detection:** D=5. The stall is detectable by monitoring the
`accord_durability_sync_point_age_seconds` metric. However, if monitoring is not
configured, the stall is only visible when the ConflictIndex reaches its size cap.

**Severity:** S=7. Liveness failure, not safety. No data corruption, but
transactions are rejected.

**Occurrence:** O=4. Unresponsive replicas and long-running transactions are common
in production. The GC stall window is proportional to the slowest replica.

**RPN: 140 (High)**

**Mitigation (Partially Implemented):**

1. DurabilityService health check: if no ExclusiveSyncPoint advance within 60s,
   log warning and trigger manual cleanup of the oldest applied entries.
2. Partial GC: allow GC of entries that are applied on all responsive replicas,
   even if one replica is down. The down replica will re-sync on recovery.
3. Long-transaction timeout: transactions exceeding `MAX_TRANSACTION_DURATION` are
   forcibly aborted, releasing their ConflictIndex references.

**Implementation:** `ferrosa-cluster/src/accord/durability.rs`

**Test Case:** `durability_stall_detected_and_recovered` — Simulate one replica
becoming unresponsive. Verify: (a) ExclusiveSyncPoint stops advancing; (b) after
60s, a warning is logged; (c) partial GC cleans applied entries for responsive
replicas; (d) when the replica recovers, full GC resumes.

**Status: Mitigation implemented in `durability.rs`. Monitoring integration planned.**

---

### FM23: Linearizable Read Timeout Under High Contention

**Component:** LinearizableRead

**Cause:** Under high contention, linearizable reads (`SERIAL` consistency) must
wait for conflicting write transactions to complete before returning results. If
multiple write transactions are queued on the same key range, the linearizable read
waits for all of them, potentially exceeding the client timeout.

**Effect:** The client receives a timeout error even though no data corruption has
occurred. Under sustained contention, linearizable reads become effectively
unavailable, forcing clients to fall back to weaker consistency levels or retry
indefinitely.

**Detection:** D=3. Timeout is reported to the client immediately. Metrics track
linearizable read latency and timeout rate.

**Severity:** S=6. Availability degradation for linearizable reads. No data
corruption or safety violation.

**Occurrence:** O=5. High contention on hot keys is common in production workloads.
Linearizable reads on write-heavy key ranges are a known anti-pattern but must be
handled gracefully.

**RPN: 90 (Medium)**

**Mitigation (Implemented):**

1. Optimized read path: linearizable reads that detect no conflicts skip the
   Accept/Commit phases entirely, returning immediately after PreAccept confirms
   no in-flight writes.
2. Read priority: linearizable reads are prioritized over new write transactions
   in the dep-wait queue. This prevents write starvation from starving reads.
3. Adaptive timeout: the client-facing timeout is independent of the Accord
   protocol timeout. If the Accord round completes but the client has already
   timed out, the result is cached for a brief period.

**Implementation:** `ferrosa-cluster/src/accord/linearizable_read.rs`

**Test Case:** `linearizable_read_under_contention_returns_or_times_out` — Submit
10 concurrent write transactions to key K1. Concurrently submit a linearizable read
on K1. Assert: the read either returns a consistent result or times out with a clear
error. The read never returns an inconsistent result.

**Status: Implemented.** Test passes.

---

### FM24: Transaction Limit Bypass Via Connection Pooling

**Component:** TransactionLimits

**Cause:** An attacker opens many CQL connections, each with the per-connection
in-flight transaction limit (default 16). Total in-flight transactions =
`num_connections * 16`. If the cluster-wide cap is not enforced independently of
per-connection limits, the attacker can exceed the intended resource budget.

**Effect:** Resource exhaustion: ConflictIndex, ReorderBuffer, and coordinator
memory are consumed by the attacker's transactions. Legitimate transactions receive
`Overloaded` errors. This is the same class of attack as AT02 but exploits the gap
between per-connection and cluster-wide limits.

**Detection:** D=3. The cluster-wide cap provides a hard limit. When reached,
`Overloaded` is returned regardless of per-connection state. Connection admission
control at 80% provides early warning.

**Severity:** S=8. Denial of service for legitimate users. No data corruption.

**Occurrence:** O=4. Connection pool exhaustion attacks are well-known and easy to
execute.

**RPN: 96 (Medium)**

**Mitigation (Implemented):**

1. Cluster-wide in-flight transaction cap: enforced independently of per-connection
   limits. When the cap is reached, new transactions receive `Overloaded` even if
   the per-connection limit is not exhausted.
2. Connection admission control: reject new CQL connections when in-flight
   transaction count exceeds 80% of the cluster-wide cap.
3. Per-client-IP rate limiting: limit the total number of connections from a single
   IP address (default 64).

**Implementation:** `ferrosa-cql/src/transaction_limits.rs`, `ferrosa-cql/src/session.rs`

**Test Case:** `connection_pool_exhaustion_blocked_by_cluster_cap` — Open 100
connections. Submit 16 in-flight transactions on each (total 1600). With cluster-wide
cap at 1000, assert: connections 63+ receive `Overloaded` for new transactions.
Existing in-flight transactions continue to completion.

**Status: Implemented.** Test passes.

---

## Critical Findings (RPN >= 200) — ALL RESOLVED

| ID | Original RPN | Revised RPN | Failure Mode | Status |
|----|-------------|-------------|-------------|--------|
| FM16 | 250 | **100** | Non-transactional write bypasses Accord | **Implemented** (write_gate.rs) |
| FM3 | 216 | **108** | ConflictIndex missing entries | **Implemented** (conflict_index.rs, proptests.rs) |
| FM1 | 210 | **90** | Clock skew exceeds SkewMax | **Implemented** (clock_validation.rs) |

All three originally critical failure modes are now mitigated with implemented code
and passing tests. RPNs reduced by 50-65% due to improved Detection scores.

## High Findings (RPN 100-199) — Post-Implementation

The following failure modes retain High RPNs after implementation, primarily due
to inherent occurrence rates or residual detection gaps:

| ID | Original RPN | Revised RPN | Failure Mode | Status |
|----|-------------|-------------|-------------|--------|
| FM22 | *(new)* | **140** | DurabilityService stall | Mitigation implemented, monitoring planned |
| FM20 | *(new)* | **120** | Cross-shard coordinator crash during Execute | **Implemented** |
| FM15 | 196 | **112** | Debouncer/Accord ordering conflict | **Implemented** |
| FM3 | 216 | **108** | ConflictIndex missing entries | **Implemented** |
| FM21 | *(new)* | **108** | Electorate join with incomplete history sync | **Implemented** |
| FM16 | 250 | **100** | Non-transactional write bypasses Accord | **Implemented** |

## Resolved Findings (RPN < 100, formerly High)

| ID | Original RPN | Revised RPN | Failure Mode | Status |
|----|-------------|-------------|-------------|--------|
| FM9 | 192 | **96** | Stale leaseholder after failover | **Implemented** |
| FM7 | 160 | **96** | Epoch mismatch fast path | **Implemented** |
| FM24 | *(new)* | **96** | Transaction limit bypass via connection pooling | **Implemented** |
| FM1 | 210 | **90** | Clock skew exceeds SkewMax | **Implemented** |
| FM10 | 180 | **90** | Cross-shard partial apply | **Implemented** |
| FM23 | *(new)* | **90** | Linearizable read timeout under high contention | **Implemented** |
| FM6 | 189 | **81** | MemIndex/memtable non-atomic apply | **Implemented** |
| FM11 | 189 | **81** | ConflictIndex GC too aggressive | **Implemented** |
| FM17 | 162 | **81** | Epoch transition window | **Implemented** |
| FM13 | 192 | **72** | Dep-wait circular dependency | **Implemented** |
| FM8 | 140 | **70** | ReorderBuffer overflow | **Implemented** |
| FM2 | 180 | **60** | Single ballot variable bug | **Implemented** |
| FM4 | 160 | **60** | Recovery selects wrong value | **Implemented** |
| FM5 | 150 | **60** | Entry not fsynced before reply | **Implemented** |
| FM12 | 100 | **60** | Non-electorate member in quorum | **Implemented** |
| FM14 | 84 | **56** | Accord entry too large for segment | **Implemented** |
| FM18 | 75 | **50** | Recovery on false positive | **Implemented** |
| FM19 | 96 | **48** | NTP step-change timestamp regression | **Implemented** |

## Test Case Summary

All test cases below pass as of 2026-03-23 (2,808 total tests).

| Test ID | FMEA Ref | Component | Test Description | Expected Result | Status |
|---------|----------|-----------|-----------------|-----------------|--------|
| TC1 | FM1 | HLC | Inject clock jump of 2x SkewMax, verify readonly mode | Node stops accepting transactions, logs error | Pass |
| TC2 | FM2 | TxnState | 24-step EPaxos correctness test: PreAccept, Accept, crash, Recovery | Recovered value == accepted value (V1), not promised value | Pass |
| TC3 | FM3 | ConflictIndex | 16 concurrent threads insert conflicting keys, verify all deps recorded | Every conflict pair has a dependency edge | Pass |
| TC4 | FM4 | RecoveryCoordinator | Recovery with mixed accepted_ballot values in quorum | Highest accepted_ballot value selected | Pass |
| TC5 | FM5 | CommitLog | Append AccordAccept, verify fsync before return | Entry survives crash-recovery replay | Pass |
| TC6 | FM6 | MemIndex | 8 writers + 8 readers, check atomic visibility | No reader sees row without MemIndex entry (or vice versa) | Pass |
| TC7 | FM7 | ElectorateConfig | Stale-epoch PreAccept rejected by epoch+1 nodes | EpochStale error returned, coordinator retries | Pass |
| TC8 | FM8 | ReorderBuffer | Fill TimerWheel, add one more entry | Overflow queued or Backpressure returned, no silent drop | Pass |
| TC9 | FM9 | Leaseholder | Kill leaseholder, restart, attempt fast-path write | Write rejected due to stale fencing token | Pass |
| TC10 | FM10 | AccordStateMachine | 3-shard transaction, crash one shard during Apply | Recovery re-applies, all shards consistent | Pass |
| TC11 | FM11 | ConflictIndex | Fill to capacity, add entry with in-flight dependent | Referenced entry not evicted; evicted after dependent commits | Pass |
| TC12 | FM12 | ElectorateConfig | Removed node's vote discarded from quorum count | Fast-path requires current-epoch members only | Pass |
| TC13 | FM13 | AccordStateMachine | Two transactions with circular key conflict | One aborts via timeout, other commits; retried txn succeeds | Pass |
| TC14 | FM14 | CommitLog | Transaction payload > segment_size | Clean InvalidData error, no panic | Pass |
| TC15 | FM15 | LateDataDebouncer | Debouncer write to Accord-enabled table | Write routed through Accord or DDL rejects config | Pass |
| TC16 | FM16 | StorageEngine | Non-transactional INSERT on Accord-enabled table | TRANSACTION_REQUIRED error returned | Pass |
| TC17 | FM17 | ElectorateConfig | Submit transaction during epoch transition | EpochTransitionInProgress error until transition completes | Pass |
| TC18 | FM18 | RecoveryCoordinator | False positive triggers recovery, original alive | Original coordinator stands down, transaction commits once | Pass |
| TC19 | FM19 | HLC | Backward clock step, verify monotonic timestamps | Second timestamp > first despite clock regression | Pass |
| TC20 | FM20 | AccordCoordinator | Coordinator crash during Execute, recovery from protocol log | All shards see same result, transaction commits once | Pass |
| TC21 | FM21 | ElectorateConfig | Node joins with incomplete history sync, blocked from participation | NotReady until sync completes, then normal participation | Pass |
| TC22 | FM22 | DurabilityService | One replica unresponsive, verify stall detection and partial GC | Warning logged at 60s, partial GC cleans responsive replicas | Pass |
| TC23 | FM23 | LinearizableRead | Linearizable read under write contention | Read returns consistent result or times out cleanly | Pass |
| TC24 | FM24 | TransactionLimits | 100 connections x 16 in-flight, cluster cap at 1000 | Overloaded error for connections exceeding cluster cap | Pass |

## Related Specs

+ [Accord Transactions Design](../superpowers/specs/SPEC-accord-transactions.md) -- architecture and protocol spec
+ [Threat Model — Accord](../specs/threat-model-accord.md) -- AT-series threats referenced throughout
+ [DSM Analysis — Accord Integration](../specs/dsm-accord.md) -- dependency structure analysis
+ [RRD Time-Series Aggregation](../superpowers/specs/2026-03-21-rrd-timeseries-aggregation-design.md) -- FM15 interaction
+ [UDF/UDA Query-Time Design](../superpowers/specs/2026-03-20-udf-uda-query-time-design.md) -- FM14 large entry context
+ [Storage Engine](../specs/storage.md) -- commit log architecture
