# Accord Test Specification: Multi-Key Transactions (S5) and Electorate Reconfiguration (S7)

This document specifies tests for Ferrosa's Accord consensus integration covering multi-key transaction support (Sprint S5) and electorate reconfiguration (Sprint S7). Each section maps to a sprint deliverable with traceability to failure modes (FM), risk priority numbers (RPN), and acceptance tests (AT) where applicable.

---

## 1. BEGIN TRANSACTION Parser (S5.1)

| Test | What It Proves | How |
|------|---------------|-----|
| `parse_begin_commit_rollback` | The CQL parser correctly recognizes `BEGIN TRANSACTION` blocks containing multiple DML sub-statements and produces the expected AST, and that `ROLLBACK` is parsed as a standalone statement. | Parse `BEGIN TRANSACTION; INSERT INTO t (id, v) VALUES (1, 'a'); UPDATE t SET v = 'b' WHERE id = 2; COMMIT;`. Assert: produces `Statement::BeginTransaction` containing exactly 2 sub-statements (one Insert, one Update) in order. Parse `ROLLBACK;` separately. Assert: produces `Statement::Rollback`. |
| `parse_nested_transaction_rejected` | Nested transactions are rejected at parse time, preventing undefined behavior in the Accord coordinator. | Parse `BEGIN TRANSACTION; BEGIN TRANSACTION; INSERT INTO t (id, v) VALUES (1, 'a'); COMMIT; COMMIT;`. Assert: returns a parse error with a message indicating nested transactions are not supported. No AST is produced. |
| `parse_ddl_in_transaction_rejected` | DDL statements are rejected inside transaction blocks, enforcing the separation between schema changes (Raft) and data transactions (Accord). | Parse `BEGIN TRANSACTION; CREATE TABLE t (id INT PRIMARY KEY, v TEXT); COMMIT;`. Assert: returns a parse error with a message indicating DDL is not allowed inside transactions. Also test with `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`. All must fail. |
| `parse_empty_transaction` | Empty transactions (no-op) are legal and do not cause parser errors. They produce a valid AST that the coordinator can handle as a no-op commit. | Parse `BEGIN TRANSACTION; COMMIT;`. Assert: produces `Statement::BeginTransaction` with an empty sub-statement list (length 0). No error is returned. |

---

## 2. Read-Set / Write-Set Extraction (S5.2)

| Test | What It Proves | How |
|------|---------------|-----|
| `readset_writeset_extraction` | The planner correctly separates SELECT statements into the read set and INSERT/UPDATE statements into the write set, keyed by (table, partition_key). | Construct a transaction AST: `SELECT * FROM t WHERE id = 1; INSERT INTO t (id, v) VALUES (2, 'x'); UPDATE t SET v = 'y' WHERE id = 3;`. Run read/write set extraction. Assert: `read_set = {(t, id=1)}`, `write_set = {(t, id=2), (t, id=3)}`. Sets are disjoint for this input. |
| `readset_writeset_cross_table` | Multi-table transactions produce read and write sets that correctly track which table each key belongs to. The Accord coordinator uses this to route to the correct shard electorates. | Transaction: `SELECT * FROM t1 WHERE id = 10; INSERT INTO t2 (id, v) VALUES (20, 'a');`. Assert: `read_set = {(t1, id=10)}`, `write_set = {(t2, id=20)}`. Verify that the sets are partitioned by table name and that a union across tables is formed when both tables appear in the same set. |
| `readset_writeset_overlapping` | When a transaction reads and writes the same key, the key appears in both the read set and write set. This is required for Accord's dependency tracking — the read must observe the prior state, and the write must be ordered after. | Transaction: `SELECT * FROM t WHERE id = 1; UPDATE t SET v = 'x' WHERE id = 1;`. Assert: `(t, id=1)` appears in both `read_set` and `write_set`. Neither set is a strict subset of the other for this input. |
| `readset_writeset_batch_in_txn` | BATCH statements inside a transaction are decomposed into their constituent statements, and each contributes to the union read/write set. | Transaction: `BEGIN BATCH INSERT INTO t (id, v) VALUES (1, 'a'); INSERT INTO t (id, v) VALUES (2, 'b'); APPLY BATCH;`. Assert: `write_set = {(t, id=1), (t, id=2)}`. The batch is not treated as an opaque unit — individual keys are extracted. |

---

## 3. Cross-Shard Execute (S5.3 — FM10, RPN 180)

| Test | What It Proves | How |
|------|---------------|-----|
| `cross_shard_execute_all_or_nothing` | Cross-shard transactions are atomic: if all shards respond, the Execute phase succeeds and the result reflects data from all participating shards. | Set up 2 shards (A, B) with pre-loaded data. Transaction reads key k1 from shard A and key k2 from shard B. Both shards respond to Read RPCs. Assert: Execute returns success. Result contains data from both k1 and k2. Both shards apply the writes. |
| `cross_shard_partial_failure_abort` | If any shard is unreachable during the Execute phase, the entire transaction aborts. No partial application occurs (atomicity guarantee). | Set up 2 shards. Partition shard B (drop all network traffic). Transaction reads from both shards. Assert: Execute returns an error (timeout or unavailable). Verify shard A has NOT applied any writes from this transaction. Client receives a retryable error. |
| `cross_shard_execute_parallel` | Read RPCs to multiple shards are sent concurrently, not sequentially. This is critical for latency — a 3-shard transaction should take ~max(latencies), not ~sum(latencies). | Set up 3 shards with injected delays (shard A: 10ms, shard B: 20ms, shard C: 30ms). Time the Execute phase. Assert: total latency is approximately 30ms (the max), not 60ms (the sum). Verify via RPC logs that all 3 Read requests were dispatched before any response was received. |
| `cross_shard_dep_wait_per_shard` | Dependency waiting is per-shard: if transaction T2 depends on T1 at shard A but not at shard B, only shard A blocks. Shard B returns immediately, and the overall latency is the max of the per-shard waits. | Set up 2 shards. Commit T1 touching only shard A. Submit T2 touching both shards, with T1 in its deps. Assert: shard A's Read handler blocks until T1 is Applied. Shard B's Read handler returns immediately. Overall Execute latency equals shard A's wait time (not shard A wait + shard B wait). |
| `cross_shard_result_deterministic` | The Execute function is pure/deterministic: given the same inputs (read results from shards), it produces the same output. This is required for recovery — if a coordinator crashes mid-Apply, the recovery coordinator must re-execute and get the same result. | Execute the same transaction twice with identical shard read results. Assert: both executions produce byte-identical results. This must hold even if wall-clock time differs between executions. |

---

## 4. Client Retry and Idempotency (S5.4)

| Test | What It Proves | How |
|------|---------------|-----|
| `client_retry_same_txnid_idempotent` | Client retries with the same TxnId are idempotent. If the transaction already committed, the client receives the cached result without duplicate execution. This prevents double-application when the ACK is lost. | Submit transaction T1 with TxnId X. Coordinator commits T1 but the ACK to the client is dropped (simulated). Client retries with the same TxnId X. Assert: the recovery/new coordinator finds T1 already committed, returns the original result. No writes are executed a second time. Result matches the first execution exactly. |
| `client_retry_different_txnid_is_new` | Retrying with a different TxnId creates a brand new, independent transaction. There is no implicit correlation between transactions — identity is solely determined by TxnId. | Submit transaction T1 with TxnId X. It commits. Client retries the same logical operation but with TxnId Y (a new ID). Assert: T2 (TxnId Y) is treated as a completely new transaction. It goes through the full PreAccept/Accept/Commit/Execute/Apply path. Its deps are computed independently. |
| `client_retry_after_apply` | Even after a transaction is fully Applied on all shards, a retry with the same TxnId returns the cached result. The result cache must survive beyond the Apply phase. | Submit transaction T1. Wait for Apply on all shards. Client retries with the same TxnId. Assert: returns the cached result immediately. No re-execution occurs. Latency of the retry is significantly lower than the original (no consensus round needed). |

---

## 5. Cross-Shard Conflict Detection (S5.5)

| Test | What It Proves | How |
|------|---------------|-----|
| `cross_shard_conflict_detection` | The ConflictIndex correctly detects write-write conflicts on a per-shard basis, even when one of the conflicting transactions spans multiple shards. | Transaction T1: write_set = {(shard_A, k1), (shard_B, k2)}. Transaction T2: write_set = {(shard_B, k2)}. Submit T1, then T2. Assert: shard B's ConflictIndex detects that T2 conflicts with T1 on key k2. T2's dependency set contains T1. Shard A's ConflictIndex has no entry for T2 (T2 does not touch shard A). |
| `cross_shard_no_false_conflict` | Non-overlapping transactions on different shards produce no false conflicts. The ConflictIndex is precise — it does not over-approximate. | Transaction T1: write_set = {(shard_A, k1)}. Transaction T2: write_set = {(shard_B, k2)}. Different keys, different shards. Assert: neither T1 nor T2 appears in the other's dependency set. ConflictIndex on shard A contains only T1. ConflictIndex on shard B contains only T2. |

---

## 6. Transaction Limits (S5.8, S5.9 — AT01, AT02)

| Test | What It Proves | How |
|------|---------------|-----|
| `transaction_connection_limit` | Per-connection transaction concurrency is bounded to prevent a single client from monopolizing coordinator resources. The default limit is 16 concurrent open transactions per connection. | Open 1 CQL connection. Start 16 `BEGIN TRANSACTION` blocks without issuing `COMMIT` on any. Attempt to start a 17th `BEGIN TRANSACTION`. Assert: the 17th returns an `Overloaded` error code. The first 16 remain active and can still be committed. |
| `transaction_timeout_abort` | Transactions that exceed the timeout are automatically aborted, preventing leaked ConflictIndex entries and resource exhaustion from abandoned transactions. | Start `BEGIN TRANSACTION`. Do not send any further statements for 10 seconds (the default transaction timeout). After the timeout, send `COMMIT`. Assert: `COMMIT` returns an error indicating the transaction was aborted due to timeout. Verify the ConflictIndex entry for this transaction has been cleaned up (no leaked state). |
| `transaction_max_keys_limit` | Transactions exceeding the maximum partition key count are rejected at CQL parse/validation time, before entering the Accord consensus path. This bounds the size of read/write sets and prevents pathological ConflictIndex growth. | Submit `BEGIN TRANSACTION` with 129 INSERT statements, each targeting a distinct partition key (default max is 128). Assert: rejected with an error message indicating the maximum key count (128) was exceeded. Verify the error occurs at CQL validation, not during PreAccept. No Accord messages are sent. |
| `transaction_max_keys_configurable` | The maximum key limit is configurable via environment variable, allowing operators to tune the tradeoff between transaction expressiveness and ConflictIndex overhead. | Set `FERROSA_ACCORD_MAX_KEYS=256`. Submit a transaction with 200 distinct partition keys. Assert: accepted (200 < 256). Submit a transaction with 257 distinct partition keys. Assert: rejected with max keys exceeded error referencing the configured limit of 256. |

---

## 7. Electorate Epoch Propagation (S7.1, S7.2 — FM7)

| Test | What It Proves | How |
|------|---------------|-----|
| `epoch_propagation_all_messages` | Every Accord protocol message carries the sender's current epoch. This is the foundation for epoch-aware quorum validation — without it, stale-epoch votes cannot be detected. | Construct and serialize each message type: PreAccept, Accept, Commit, Read, Apply, Recover. For each, set the sender's epoch to a known value (e.g., epoch=5). Serialize, then deserialize. Assert: the deserialized message contains `epoch=5`. Verify the epoch field is present in the wire format (not implicit or derived). |
| `epoch_mismatch_slow_path_fallback` | When a coordinator at an older epoch contacts a replica at a newer epoch, the coordinator detects the mismatch and falls back to the slow path. This prevents fast-path commits with an outdated electorate view. | Coordinator at epoch 1 sends PreAccept to replica at epoch 2. Replica responds with PreAcceptOK including `epoch=2` in its response. Assert: coordinator detects `response.epoch (2) != coordinator.epoch (1)`. Coordinator does NOT count this response toward a fast-path quorum. Coordinator initiates slow path and fetches the new electorate configuration from Raft before proceeding. |
| `epoch_mismatch_all_replicas` | If all replicas have advanced past the coordinator's epoch, neither fast-path nor slow-path quorum can be formed until the coordinator updates. This forces epoch synchronization before any commit. | Coordinator at epoch 1, all 3 replicas at epoch 2. Coordinator sends PreAccept to all 3. All respond with epoch=2. Assert: coordinator cannot form fast-path quorum (0 matching-epoch responses). Coordinator attempts slow path but also cannot proceed (epoch 1 electorate may differ from epoch 2 electorate). Coordinator must update to epoch 2 by reading the Raft log before retrying. |

---

## 8. JoinElectorate Protocol (S7.3 — FM12, AT25)

| Test | What It Proves | How |
|------|---------------|-----|
| `join_electorate_four_gates` | A new node must pass all four readiness gates before participating in fast-path voting. This prevents a partially-ready node from casting votes that could lead to inconsistent commits. | New node N joins. Simulate gate completion in order: (1) Metadata gate — N receives electorate config from Raft. (2) Coordinate gate — N replicates sufficient ConflictIndex state from existing members. (3) Data gate — N streams data for its assigned token ranges. (4) Reads gate — N can serve reads at the current Accord timestamp. Assert: `ready_electorate[epoch]` is NOT set after gates 1, 2, or 3 individually. It IS set only after all 4 gates pass. Before `ready_electorate` is set, N's votes are not counted by coordinators. |
| `join_electorate_receives_history` | The new node receives complete fast-path transaction history from the prior epoch before participating. Without this, the node might miss committed transactions and vote inconsistently. | Electorate E_old has 3 members, f_old=1. New node N joins. N must receive JoinElectorate notifications from `E_old - f_old + 1 = 3` members (i.e., at least 2 of 3). Each notification carries the set of fast-path committed transactions from the prior epoch. Assert: after receiving notifications from 2 members, N has the complete fast-path history (union of both notification payloads). N can now verify its ConflictIndex is consistent with the committed history. |
| `join_electorate_premature_rejected` | If a node prematurely sets `ready_electorate[epoch]` before completing all gates, coordinators must not count its votes. This is the enforcement mechanism for the four-gate protocol. | New node N skips the Data gate but sets `ready_electorate[epoch]` anyway (simulated bug/adversarial behavior). Coordinator sends PreAccept to N. N responds with PreAcceptOK. Assert: coordinator validates that N has completed all gates (via the electorate config from Raft). N is NOT listed as a full member. Coordinator discards N's response. N's vote is not counted toward any quorum. |

---

## 9. Electorate Shrink and Quorum Resize (S7.4, S7.6 — FM12)

| Test | What It Proves | How |
|------|---------------|-----|
| `electorate_shrink_quorum_resize` | When the electorate shrinks (node decommission/failure), the fast-path quorum size is recomputed. With RF=3, f_fast=0, the quorum is `ceil((E+f+1)/2)`. Shrinking from 3 to 2 nodes means fast-path requires unanimity (2 of 2). | RF=3, f_fast=0. Initial electorate = {A, B, C}. fast_quorum = ceil((3+0+1)/2) = 2. Node C is decommissioned (Raft commits the removal). New electorate = {A, B}. Assert: fast_quorum is recomputed to ceil((2+0+1)/2) = 2 (unanimous agreement required). Transactions still commit via fast path with 2 of 2 responses. |
| `electorate_vote_validation` | Votes from nodes not in the current epoch's electorate are discarded. This prevents decommissioned or rogue nodes from influencing consensus. | Electorate at epoch 3 = {A, B, C}. Node D (not in the electorate) sends a PreAcceptOK to the coordinator. Assert: coordinator checks D's membership in the epoch 3 electorate. D is not a member. The response is discarded and not counted toward any quorum. The event is logged as suspicious. |
| `electorate_stale_epoch_response` | Responses from nodes reporting a stale epoch are discarded. This prevents a lagging node from casting votes based on an outdated electorate configuration. | Coordinator at epoch 3 sends PreAccept. Node B responds with PreAcceptOK but reports epoch=1 (severely stale). Assert: coordinator detects `response.epoch (1) < coordinator.epoch (3)`. Response is discarded. Not counted toward quorum. If insufficient responses remain, coordinator may need to wait for other replicas or trigger recovery. |

---

## 10. Epoch Transition Drain (S7.5 — FM17, AT29)

| Test | What It Proves | How |
|------|---------------|-----|
| `epoch_drain_period` | Epoch transitions have a drain period during which no new transactions are accepted for the old epoch, but in-flight transactions are allowed to complete. The drain period is configurable and must exceed SkewMax + max_transaction_timeout to ensure all in-flight transactions can finish. | Trigger epoch transition from epoch 1 to epoch 2. Assert: (1) New `BEGIN TRANSACTION` requests for epoch 1 are rejected with "epoch transitioning" error. (2) In-flight transactions started before the transition continue to make progress. (3) Drain period defaults to 30s and is configurable via `FERROSA_ACCORD_DRAIN_PERIOD_SECS`. (4) After the drain period expires, epoch 2 becomes active and accepts new transactions. |
| `epoch_drain_timeout_abort` | Transactions that are still in-flight when the drain period expires are forcibly aborted. The epoch transition must not be held hostage by a stuck transaction. | Start transaction T1 in epoch 1. Trigger epoch transition. Simulate T1 being stuck (coordinator crashed, network partition). Drain period expires. Assert: T1 is aborted. Its ConflictIndex entries are cleaned up. Epoch 2 becomes active. T1 can be retried as a new transaction in epoch 2. |
| `epoch_drain_cross_epoch_txn` | Transactions that started before an epoch transition are allowed to complete in their original epoch. They do not need to be restarted in the new epoch, which would cause unnecessary client-visible failures. | Start transaction T1 in epoch 1. T1 reaches the Accept phase. Epoch transition to epoch 2 begins. Assert: T1 is allowed to proceed through Commit, Execute, and Apply in epoch 1. T1 completes successfully. The epoch transition waits for T1 during the drain period. T1's result is valid and durable. |

---

## 11. Two-Phase DDL (S7.10 — OQ2 Phase 4)

| Test | What It Proves | How |
|------|---------------|-----|
| `two_phase_ddl_dep_wait` | DDL operations are integrated into Accord's dependency graph via a "DDL pending" marker. New transactions that start after the marker automatically dep-wait on the DDL, ensuring schema changes are linearized with respect to DML. | Broadcast a "DDL pending" marker for `ALTER TABLE t ADD COLUMN c INT` via Raft. Start new Accord transaction T1 that touches table t. Assert: T1's dependency set includes the DDL marker. T1's Execute phase blocks until the DDL is Applied. After the DDL applies, T1 proceeds and can reference the new schema (column c exists). |
| `two_phase_ddl_concurrent_dml` | In-flight DML transactions that started before the DDL marker are not blocked by the DDL. The DDL waits for them to complete, preserving linearizability. | Start 10 Accord transactions (T1-T10) touching table t. While they are in-flight, broadcast the "DDL pending" marker. Assert: all 10 transactions complete without being blocked by the DDL. The DDL applies only after all 10 have completed (their Apply phases finish). New transactions started after the marker dep-wait on the DDL. |
| `two_phase_ddl_schema_change_visible` | After a DDL applies, new Accord transactions see the updated schema. This verifies end-to-end schema evolution through the two-phase protocol. | DDL: `ALTER TABLE t ADD COLUMN c INT`. Wait for the DDL to apply. Start a new Accord transaction: `UPDATE t SET c = 42 WHERE id = 1;`. Assert: the transaction succeeds. Column c exists in the schema used by the transaction planner. The write is applied correctly. Reading back `SELECT c FROM t WHERE id = 1` returns 42. |
| `two_phase_ddl_abort_on_timeout` | If the DDL drain times out (an in-flight transaction is stuck), the DDL applies anyway. The stuck transaction will be recovered separately and must handle the schema change gracefully. | Broadcast "DDL pending" marker. Start transaction T1 that touches the affected table. Simulate T1's coordinator crashing (T1 is stuck in Accept phase). DDL drain timeout expires. Assert: the DDL applies despite T1 being in-flight. When T1 is eventually recovered (by a new coordinator), it either (a) completes with the old schema if it was already committed, or (b) fails with a schema mismatch error and must be re-planned by the client. |

---

## Traceability Matrix

| Sprint | Deliverable | Failure Mode / Risk | Tests |
|--------|------------|---------------------|-------|
| S5.1 | BEGIN TRANSACTION parser | — | Sections 1 |
| S5.2 | Read-set / write-set extraction | — | Section 2 |
| S5.3 | Cross-shard Execute | FM10, RPN 180 | Section 3 |
| S5.4 | Client retry / idempotency | — | Section 4 |
| S5.5 | Cross-shard conflict detection | — | Section 5 |
| S5.8 | Transaction connection limit | AT01 | Section 6 (connection_limit) |
| S5.9 | Transaction key/timeout limits | AT02 | Section 6 (timeout, max_keys) |
| S7.1–S7.2 | Epoch propagation | FM7 | Section 7 |
| S7.3 | JoinElectorate protocol | FM12, AT25 | Section 8 |
| S7.4, S7.6 | Electorate shrink / quorum resize | FM12 | Section 9 |
| S7.5 | Epoch transition drain | FM17, AT29 | Section 10 |
| S7.10 | Two-phase DDL | OQ2 Phase 4 | Section 11 |
