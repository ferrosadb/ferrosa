# Accord Integration Test Specification — Sprint S3

Integration-level tests that wire Accord components together. Each section corresponds to an S3 task and verifies cross-component behavior that unit tests cannot cover.

---

## 1. Fire-and-Forget CL=ONE (S3.1 — OQ1 Decision)

Fire-and-forget allows CL=ONE writes to acknowledge the client after PreAccept quorum, deferring Apply to the background. These tests prove correctness and latency characteristics of that path.

| Test | What It Proves | How |
|------|---------------|-----|
| `fire_and_forget_cl_one` | The full fire-and-forget lifecycle works end-to-end: HLC timestamp assignment, PreAccept broadcast, early ACK, ConflictIndex registration, and deferred Apply. | Send an INSERT with CL=ONE through AccordCoordinator. Assert: (1) txn gets a t0 from the HLC, (2) PreAccept is broadcast to the electorate, (3) client receives ACK after PreAccept quorum and NOT after Apply, (4) txn is registered in ConflictIndex, (5) Apply completes asynchronously in the background. Measure client-visible latency: must be ~0.5 RTT, not 1+ RTT. |
| `fire_and_forget_visible_to_subsequent_txn` | Fire-and-forget writes are visible to the Accord conflict detection machinery, preventing lost updates. | Fire-and-forget write to key K. Start a full Accord txn that reads key K. Assert: the full txn sees the fire-and-forget write in its dep set via ConflictIndex. The full txn waits for the fire-and-forget to Apply before executing its read. |
| `fire_and_forget_crash_recovery` | Fire-and-forget writes are durable even if the coordinator crashes between ACK and Apply. No data loss after recovery. | Fire-and-forget write enters PreAccept, ACK sent to client. Coordinator crashes before Apply. Recovery is triggered by a peer. Assert: the fire-and-forget transaction is recovered and eventually Applied. Read key K after recovery and confirm the write is present. |
| `fire_and_forget_does_not_block_client` | Client latency is decoupled from Apply latency. Slow disk or slow peers do not affect client-visible response time. | Client sends fire-and-forget write. Inject a 500ms delay into the Apply phase (simulated slow disk). Assert: client received ACK in <50ms (after PreAccept quorum, not after Apply). Verify Apply still completes after the delay. |

---

## 2. Dep-Wait and Deadlock Detection (S3.3 — FM13, RPN 192)

Dep-wait ensures transactions execute in dependency order. Deadlock detection prevents circular dependencies from causing infinite waits. Together they guarantee progress under contention.

| Test | What It Proves | How |
|------|---------------|-----|
| `dep_wait_simple_chain` | A transaction that depends on another waits correctly and proceeds once the dependency is satisfied. | T1 commits. T2 depends on T1. T2's Apply handler waits for T1 to Apply. Apply T1. Assert: T2 proceeds immediately after T1 applies. Verify ordering via timestamps on Apply completion events. |
| `dep_wait_transitive` | Transitive dependency chains resolve in correct order without deadlock. | Create dependency chain T1 -> T2 -> T3. T3 waits for T2, T2 waits for T1. Apply T1. Assert: T2 applies next, then T3, in strict order. Verify no Apply fires out of dependency order. |
| `dep_wait_deadlock_detection` | Circular dependencies are detected and broken by aborting the transaction with the highest t0. The waits-for graph correctly identifies the cycle. | T1 depends on T2, T2 depends on T1 (circular). Assert: deadlock detected via waits-for graph within timeout. The transaction with the highest t0 is aborted. The other transaction proceeds to Apply. Verify: `deadlock_detected` metrics counter is incremented by 1. |
| `dep_wait_cycle_break` | N-way cycles are broken deterministically with exactly one abort and no livelock. | 3-way cycle: T1->T2->T3->T1. Assert: exactly one transaction is aborted (the one with highest t0). The other two eventually commit and Apply. Verify no livelock by asserting completion within 2x the dep-wait timeout. |
| `dep_wait_timeout` | A stuck dependency does not cause indefinite hangs. The waiting transaction either triggers recovery or aborts cleanly. | T1 depends on T2. T2 is stuck (coordinator crashed, no recovery triggered yet). Assert: after dep-wait timeout (default 10s), T1 either triggers recovery for T2 or aborts with a timeout error. Verify T1 does not hang indefinitely by asserting completion within 15s. |
| `dep_wait_already_applied` | No unnecessary waiting when a dependency is already satisfied. Fast path is taken. | T1 depends on T2. T2 is already in Applied state when T1 checks. Assert: T1 proceeds immediately with zero wait. No dep-wait timer is started. Verify via absence of dep-wait metrics for this pair. |
| `dep_wait_concurrent_wakeup` | Mass wakeup from a single dependency does not cause OOM or thundering herd. Wakeup mechanism handles fan-out safely. | Create 100 transactions all depending on T0. Apply T0. Assert: all 100 transactions are woken up and proceed to Apply. Monitor memory usage: peak RSS increase must be bounded (no OOM). Verify wakeups use broadcast channel or batching, not 100 individual allocations. |

---

## 3. Leaseholder Assignment and Failover (S3.4 — FM9, RPN 192)

Leaseholders allow PreAccept fast-path by letting the primary replica for a token range perform local conflict checks. These tests verify assignment, failover, and stale-detection.

| Test | What It Proves | How |
|------|---------------|-----|
| `leaseholder_assignment_from_token_ring` | Leaseholder is deterministically derived from the token ring topology, matching the primary replica. | For a given partition key, compute the token and look up the replica list from the token ring. Assert: leaseholder for that key's range matches the first replica in the ring's replica list for that range. |
| `leaseholder_failover_update` | Leaseholder automatically fails over when the current holder goes down. Failover is coordinated through Raft topology changes. | Leaseholder node fails (simulate heartbeat timeout). Raft commits a topology change removing the failed node. Assert: new leaseholder assigned for the affected token ranges. New leaseholder is the next replica in the ring. Verify new leaseholder can perform local conflict checks for the range. |
| `stale_leaseholder_detection` | A node with a stale epoch does not incorrectly act as leaseholder. Epoch mismatch triggers fallback to broadcast PreAccept. | Node A thinks it is leaseholder for range R (from epoch 1). Epoch 2 assigns leaseholder to node B. Node A receives a PreAccept for range R. Assert: Node A detects epoch mismatch and falls back to non-leaseholder broadcast path. Node A does not perform a local-only conflict check. |
| `leaseholder_local_conflict_check` | The leaseholder performs conflict checks locally without a network round trip, reducing latency for the fast path. | Leaseholder for key K processes a write. Assert: ConflictIndex lookup is performed locally (no network messages for the lookup). The leaseholder broadcasts PreAccept only to the OTHER replicas, not to itself. Verify via message trace that self-PreAccept is not sent. |
| `leaseholder_epoch_bounded` | Leaseholder assignments do not leak across epoch boundaries. Each new epoch requires fresh assignment evaluation. | Leaseholder assignment exists in epoch N. Trigger epoch N+1 (e.g., node join). Assert: all leaseholder assignments are re-evaluated from the new topology. Old-epoch leaseholders do not carry over automatically. Verify by checking that a node that was leaseholder in epoch N is not assumed to be leaseholder in epoch N+1 without re-derivation. |

---

## 4. Linearizable Local Read (S3.5 — Goal G4)

Linearizable reads must reflect all committed writes up to the read timestamp. These tests verify that the read path correctly checks the ConflictIndex and waits for in-flight transactions to Apply.

| Test | What It Proves | How |
|------|---------------|-----|
| `linearizable_read_dep_check` | Reads correctly wait for committed-but-not-Applied transactions before returning. Linearizability is maintained. | Write T1 to key K (committed, accord_ts=10). Read key K at timestamp t=15. Assert: read waits for T1 to Apply (not just Commit). After T1 applies, read returns T1's value. Verify no extra network RTT in the no-conflict case — the dep-check is local against the ConflictIndex. |
| `linearizable_read_no_conflict` | When no in-flight transactions exist, reads proceed with zero Accord overhead. The fast path is genuinely fast. | Read key K. No in-flight transactions on K in ConflictIndex. Assert: read returns immediately from memtable/SSTable with no dep-wait and no Accord protocol messages. Measure latency: should match non-Accord read latency within 5%. |
| `linearizable_read_waits_for_apply` | Reads never return stale data. A committed-but-unapplied write is visible to reads that should see it. | Write T1 to key K (committed but not yet Applied). Read key K. Assert: read blocks until T1 is Applied, then returns T1's value. Confirm read does NOT return a stale pre-T1 value at any point. |
| `linearizable_read_multiple_pending` | Reads correctly filter in-flight transactions by timestamp, waiting only for those at or before the read timestamp. | Three in-flight transactions on key K: T1 (accord_ts=5, Applied), T2 (accord_ts=8, Committed), T3 (accord_ts=12, PreAccepted). Read at t=10. Assert: read waits for T2 to Apply (accord_ts=8 <= 10). Read does NOT wait for T3 (accord_ts=12 > 10). Returned value reflects T1 and T2. T3's effect is not visible. |

---

## 5. Commit Log Replay on Startup (S3.6)

After a crash, the protocol log and main commit log must be replayed to reconstruct Accord state. These tests verify that replay is correct, idempotent, and handles corruption.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_crash_recovery_replay` | All Accord transaction phases are correctly persisted and reconstructed from the logs after a crash. | Write 5 transactions through various phases: T1 (Applied), T2 (Committed), T3 (Accepted), T4 (PreAccepted), T5 (Applied). Simulate crash (drop all in-memory state). Replay both protocol log and main commit log. Assert: T1 and T5 are in Applied state (no re-apply needed). T2 is in Committed state (awaiting Execute). T3 is in Accepted state (awaiting Commit). T4 is in PreAccepted state (awaiting Accept or recovery). |
| `replay_does_not_duplicate_apply` | Replay is idempotent. Applied transactions are not re-applied, preventing duplicate mutations in the memtable. | T1 is Applied and its mutation is in the memtable. Crash. Replay. Assert: memtable is not written twice for T1's data. The Applied flag in the protocol log prevents re-application. Verify by checking memtable entry count for the key (must be 1, not 2). |
| `replay_reconstructs_conflict_index` | The ConflictIndex is faithfully reconstructed from logs, preserving only in-flight (not-yet-Applied) transactions. | Before crash, ConflictIndex has 3 in-flight txns (one PreAccepted, one Accepted, one Committed). Crash. Replay. Assert: ConflictIndex is reconstructed with the same 3 txns. Applied txns are NOT present in the ConflictIndex. Verify by querying the ConflictIndex for each txn's key. |
| `replay_with_partial_write` | Partial or corrupted log entries are detected and safely skipped. The node recovers missing state from peers. | Protocol log has a partial entry (write started but fsync did not complete before crash). Assert: partial entry is detected via CRC check and skipped. The node does not crash or panic on the corrupt entry. TxnState for that txn is reconstructed by querying peers (recovery protocol). |

---

## 6. CQL Router Integration (S3.7)

The CQL layer must route mutations and reads through AccordCoordinator in cluster mode while preserving a direct path in standalone mode. These tests verify the routing decisions.

| Test | What It Proves | How |
|------|---------------|-----|
| `cql_route_through_accord` | DML writes in cluster mode are routed through Accord, not directly to the storage engine. The full protocol path is exercised. | Send INSERT via CQL in cluster mode. Assert: the write goes through AccordCoordinator, not directly to StorageEngine. Verify trace: CQL parser -> AccordCoordinator -> PreAccept -> ... -> Apply -> ACK to client. |
| `cql_route_select_through_accord` | Reads in cluster mode use the linearizable read path, checking the ConflictIndex for in-flight writes. | Send SELECT in cluster mode. Assert: read goes through the linearizable read path with dep-check against ConflictIndex. Not a direct memtable read. Verify that ConflictIndex is consulted by checking trace or metrics. |
| `cql_route_standalone_bypasses_accord` | Standalone mode (single node, no cluster) skips Accord entirely, avoiding unnecessary protocol overhead. | In standalone mode (single node, no cluster config), send INSERT. Assert: write goes directly to StorageEngine. AccordCoordinator is not invoked. Verify by asserting zero Accord-related metrics incremented. |
| `cql_route_batch_through_accord` | BATCH statements are treated as a single Accord transaction with a union write-set, not as individual transactions per statement. | Send BATCH with multiple INSERT/UPDATE statements in cluster mode. Assert: all mutations are routed as a single Accord transaction with one TxnId. The write-set is the union of all statements' write-sets. Verify only one PreAccept round occurs for the entire batch. |

---

## 7. DDL Drain-and-Block (S3.8 — OQ2 Phase 1)

Schema changes (DDL) must drain in-flight Accord transactions on the affected table before applying the schema mutation via Raft. These tests verify the drain gate, timeout, and isolation properties.

| Test | What It Proves | How |
|------|---------------|-----|
| `ddl_drain_and_block` | The full drain-and-block lifecycle works: new writes are rejected, in-flight txns complete, DDL is applied via Raft, then writes resume. | Start 5 Accord transactions on table T (various phases). Send ALTER TABLE T. Assert: (1) no new Accord txns accepted for table T (return Unavailable to client), (2) the 5 in-flight txns are allowed to complete, (3) after all 5 complete, DDL is applied via Raft, (4) new Accord txns are accepted again after DDL commits. Verify ordering: drain -> DDL apply -> resume. |
| `ddl_drain_timeout` | A stuck transaction does not block DDL indefinitely. After timeout, DDL proceeds and the stuck txn is handled separately. | Start an Accord txn on table T whose coordinator has crashed (txn is stuck in PreAccepted). Send ALTER TABLE T with drain. Assert: drain timeout (default 30s) expires, DDL is applied anyway via Raft. The stuck txn will be recovered separately by the recovery protocol. Verify a warning is logged about the stuck txn. |
| `ddl_drain_concurrent_reads` | Reads continue to work during DDL drain. Only writes are blocked. Read availability is not sacrificed for DDL. | Initiate DDL drain on table T. During drain, send SELECT on table T and INSERT on table T. Assert: SELECT succeeds and returns data. INSERT returns Unavailable. Verify read path is not gated by the drain flag. |
| `ddl_drain_other_tables_unaffected` | DDL drain on one table does not affect writes to other tables. The drain gate is table-scoped. | Initiate DDL drain on table T. Send INSERT to table U (different table). Assert: INSERT to table U succeeds normally. Accord transactions on table U are not blocked. Verify the drain gate is keyed by table, not global. |
