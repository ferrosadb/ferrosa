# Project Plan — Accord Distributed Transactions

> Last updated: 2026-03-21
> Status: Draft
> Source: specs/accord.md, specs/fmea-accord.md, specs/threat-model-accord.md, specs/dsm-accord.md

## Overview

Four implementation phases from SPEC-accord-transactions.md §12, broken into timeboxed sprints. Tasks are prioritized by FMEA RPN score and threat model risk level. Each task includes testable success criteria and the source phase that identified the need.

## Risk Register

| Risk | Source | RPN/Risk | Mitigation Sprint |
|------|--------|----------|-------------------|
| Non-transactional write bypasses Accord | FM16 | 250 Critical | S1 |
| ConflictIndex missing entries | FM3 | 216 Critical | S1 |
| Clock skew exceeds SkewMax | FM1 | 210 Critical | S1 |
| Late data debouncer vs Accord ordering | FM15 | 196 High | S2 |
| Dep-wait circular dependency deadlock | FM13 | 192 High | S3 |
| Stale leaseholder after failover | FM9 | 192 High | S3 |
| MemIndex non-atomic apply | FM6 | 189 High | S4 |
| ConflictIndex GC evicts needed entry | FM11 | 189 High | S2 |
| Two-ballot invariant violation | FM2 | 180 High | S1 |
| Cross-shard partial failure | FM10 | 180 High | S3 |
| Epoch transition vulnerability | FM17 | 162 High | S5 |
| Epoch mismatch during fast path | FM7 | 160 High | S5 |
| Recovery selects wrong value | FM4 | 160 High | S2 |
| Commit log not fsynced before reply | FM5 | 150 High | S1 |
| ReorderBuffer overflow | FM8 | 140 High | S2 |
| Fast-path with non-electorate member | FM12 | 100 High | S5 |
| NTP step-change timestamp regression | FM19 | 96 Medium | S2 |
| Accord entry too large for segment | FM14 | 84 Medium | S1 |
| False positive recovery trigger | FM18 | 75 Medium | S3 |

## Phase 0: Foundation Crates — Sprints S1-S2

**Gate:** All unit tests pass; 24-step EPaxos correctness test exists and passes.

### Sprint S1: Core Types and Critical Mitigations (2 weeks)

Priority: Critical FMEA items (FM16, FM3, FM1, FM2, FM5, FM14)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S1.1 | Implement `Timestamp` type in ferrosa-common with `epoch`, `time`, `seq`, `node` fields; derive `Ord` with field-order correctness | S | `Timestamp::new()` and `bump_past()` produce globally unique, totally ordered values. `PartialOrd` agrees with manual comparison on all field combinations. | `timestamp_ordering`, `timestamp_uniqueness` | Spec §3.2.1, FM1 |
| S1.2 | Implement `HybridLogicalClock` in ferrosa-common: monotonic physical component, logical increment on conflict, `MAX_CLOCK_DRIFT` rejection | M | HLC never regresses. `tick()` advances monotonically. Reject `SystemTime::now()` regression > `SkewMax`. Enter read-only mode on drift violation. | `hlc_monotonicity`, `hlc_skew_exceeds_max_enters_readonly` | Spec §7.3, FM1 |
| S1.3 | Define `TxnId`, `BallotNumber`, `AcceptedBallot` (newtype), `PromisedBallot` (newtype) in ferrosa-common | S | Types are distinct at compile time. `AcceptedBallot` cannot be compared with `PromisedBallot` without explicit conversion. | `ballot_type_safety` (compile-fail test) | Spec §6.1, FM2 |
| S1.4 | Implement `TxnState` with two ballot fields, phase flags, deps `HashSet` | S | `max_ballot_seen` and `accepted_ballot` update independently. Invariant `accepted_ballot <= max_ballot_seen` enforced by assertion on every mutation. | `ballot_variable_separation`, `txnstate_invariant_assertion` | Spec §3.2.2, FM2 |
| S1.5 | Implement dual-log architecture: protocol log (local-only, not S3) for PreAccepted/Accepted/Committed entries; main commit log for AccordApplied + data mutations | L | Protocol entries never appear in S3 upload queue. Main log stays at ~1x volume. Protocol log uses smaller segments with aggressive GC after Applied. | `protocol_log_not_uploaded`, `protocol_log_gc_after_applied`, `accord_commitlog_roundtrip` | Spec §10, FM5, FM14, OQ4 |
| S1.6 | Implement fsync-before-ack wrapper: commit log write returns only after fsync completes | S | `write_and_sync()` blocks until fsync. Mock test verifies no protocol reply is sent before fsync returns. | `fsync_before_ack_ordering` | Spec §10, FM5 |
| S1.7 | Implement `ConflictIndex` in ferrosa-storage: `single_key` HashMap, `range_ops` BTreeMap, `indexed_writes` HashMap; single-threaded per shard | L | O(1) lookup for single-key. O(log n) for range overlap. `max_conflicting_timestamp()` returns correct max. `register()` and `remove()` maintain invariants. Hard cap on entries (default 100K). | `conflict_index_single_key`, `conflict_index_range_overlap`, `conflict_index_bounded_size` | Spec §3.2.3, FM3, FM16 |
| S1.8 | Implement Accord write gate: non-transactional writes to keys with in-flight Accord transactions must check ConflictIndex | M | A non-transactional `INSERT` to a key with an in-flight Accord transaction either blocks or is routed through Accord. Test: concurrent non-txn write + Accord transaction on same key; assert no bypass. | `non_transactional_write_accord_gate` | FM16 (RPN 250), AT03 |
| S1.9 | Implement the 24-step EPaxos correctness test as mandatory CI gate | M | Test encodes exact counter-example from Sutra (2019). 3 simulated replicas, 2 conflicting transactions, 24 ordered steps. Assert: `p1.committed_deps(c2) == p2.committed_deps(c2)`. | `epaxos_24_step_correctness` | Spec §11.2, FM2 |

### Sprint S2: ReorderBuffer, Heartbeat Extension, Recovery Foundation (2 weeks)

Priority: High FMEA items (FM15, FM11, FM4, FM8, FM19)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S2.1 | Extend ferrosa-net heartbeat with `sent_at` and `recv_at` fields; implement per-link RTT tracking | M | Each heartbeat response includes round-trip data. Per-peer P99 latency computed from sliding window. | `heartbeat_rtt_tracking`, `per_peer_latency_p99` | Spec §7.2-7.3, FM1 |
| S2.2 | Implement SkewMax measurement from heartbeat data: P99.9 of observed clock offsets, hard ceiling (default 2s), per-node outlier rejection | M | SkewMax derives from empirical measurements only. Single outlier node does not inflate global SkewMax. Hard ceiling enforced. | `skew_max_measurement`, `skew_outlier_rejection`, `skew_hard_ceiling` | Spec §7.3, FM1, AT19 |
| S2.3 | Implement `ReorderBuffer` with `TimerWheel` (not per-message sleep); bounded capacity; deadline formula | L | Messages processed in `t0` order, not arrival order. Deadline formula matches spec §7.1. Overflow returns backpressure error (not message loss). Capacity bounded. | `reorder_buffer_ordering`, `reorder_buffer_deadline`, `reorder_buffer_overflow_backpressure` | Spec §7.2, FM8 |
| S2.4 | Implement ConflictIndex GC: remove entries only after `AccordApplied` flushed to SSTable; never evict entries with in-flight dependents | M | Entries with dep-waiters are never evicted. GC runs after Apply. Hard cap evicts only fully-applied entries. | `conflict_index_gc_respects_deps`, `conflict_index_gc_after_apply` | FM11 (RPN 189) |
| S2.5 | Implement `RecoveryCoordinator` foundation: ballot generation, `Recover` message broadcast, `RecoverOK` collection | L | Fresh ballot monotonically increasing. Recovery contacts majority of electorate. Selects value by `max(accepted_ballot)`, NOT `max(max_ballot_seen)`. | `recovery_selects_by_accepted_ballot`, `recovery_majority_quorum` | Spec §5, FM4 |
| S2.6 | Integrate late data debouncer with Accord ordering: debouncer must use Accord timestamp (not wall clock) for re-aggregation ordering | M | Re-aggregation order deterministic across replicas when using Accord timestamps. No divergence between nodes under concurrent late data + Accord transactions. | `debouncer_accord_timestamp_ordering` | FM15 (RPN 196) |
| S2.7 | NTP step-change guard: detect and handle `SystemTime::now()` regressions > configurable threshold | S | HLC refuses to regress. Step-change detected and logged. Node enters degraded mode if drift exceeds threshold. | `hlc_ntp_step_change_guard` | FM19 |
| S2.8 | Add Accord protocol message types to ferrosa-net: `PreAccept`, `PreAcceptOK`, `Accept`, `AcceptOK`, `Commit`, `Read`, `ReadOK`, `Apply`, `ApplyOK`, `Recover`, `RecoverOK` | M | All 11 message types serialize/deserialize correctly. Message size bounded. | `accord_message_roundtrip` | Spec §4, DSM |
| S2.9 | Add `MAX_CLOCK_DRIFT` validation at replica: reject `PreAccept` if `t0 > local_hlc + MAX_CLOCK_DRIFT` | S | Replica rejects future timestamps. Does not advance local HLC for rejected timestamps. | `reject_future_timestamp_preaccept` | AT05, AT20 |

## Phase 1: Single-Key Accord — Sprints S3-S4

**Gate:** Jepsen register test passes; P50 write latency within 15% of QUORUM baseline.

### Sprint S3: AccordStateMachine and Coordinator (2 weeks)

Priority: High FMEA items (FM13, FM9, FM10)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S3.1 | Implement `AccordStateMachine` in ferrosa-cluster/accord/: PreAccept, Accept, Commit, Execute, Apply handlers with all phase transitions. Include `FireAndForget` flag for CL=ONE: ACK after PreAccept quorum, skip Execute/Apply wait, complete asynchronously. | XL | State machine transitions match spec §4.1-4.5. Each handler persists to protocol log before replying. Idempotent on duplicate messages. Fire-and-forget mode returns ACK after PreAccept quorum. | `state_machine_phase_transitions`, `state_machine_idempotency`, `fire_and_forget_cl_one` | Spec §4, FM5, OQ1 |
| S3.2 | Implement `AccordCoordinator`: leaseholder detection, fast-path decision, slow-path fallback | L | Leaseholder path: 1 RTT. Non-leaseholder path: 2 RTTs. Fast-path quorum = `ceil((E + f_fast + 1) / 2)`. Slow-path quorum = majority. | `coordinator_fast_path_1rtt`, `coordinator_slow_path_2rtt`, `fast_quorum_size_formula` | Spec §3.1, §8.1 |
| S3.3 | Implement dep-wait with cycle detection: transactions waiting on dependencies must detect and break circular waits | L | Deadlock detection via waits-for graph. Cycle broken by aborting the transaction with highest `t0`. Timeout fallback (default 10s). | `dep_wait_deadlock_detection`, `dep_wait_cycle_break` | FM13 (RPN 192) |
| S3.4 | Implement leaseholder assignment in token ring metadata (openraft-managed, epoch-bounded) with staleness detection | M | Leaseholder updated on failover. Stale leaseholder detected via epoch mismatch. Non-leaseholder coordinator falls back to broadcast path. | `leaseholder_failover_update`, `stale_leaseholder_detection` | FM9 (RPN 192) |
| S3.5 | Implement linearizable local read: dep-check against ConflictIndex before serving from memtable | M | Read at timestamp `t` waits for all in-flight transactions with `accord_ts <= t` to apply. No stale reads after a committed write. | `linearizable_read_dep_check` | Spec §3.1, G4 |
| S3.6 | Commit log replay on startup: reconstruct AccordStateMachine state from persisted entries | M | After crash, all TxnState recovered from commit log. In-flight transactions resume from last persisted phase. Applied transactions not re-applied. | `accord_crash_recovery_replay` | Spec §12 Phase 1 |
| S3.7 | Wire CQL router to AccordCoordinator: INSERT/UPDATE/DELETE route through Accord when cluster mode active | M | All DML statements in cluster mode go through Accord. Single-key writes use leaseholder fast path. `WritePath::Cluster` dispatches to AccordCoordinator. | `cql_route_through_accord` | Spec §3.1 |
| S3.8 | Implement DDL drain-and-block: stop accepting new Accord txns for table, wait for in-flight to complete, apply DDL via Raft, resume | M | DDL on a table with active Accord transactions drains in-flight txns before applying. New txns rejected with `Unavailable` during drain. DDL applied via existing Raft path. | `ddl_drain_and_block`, `ddl_drain_timeout` | OQ2 Phase 1 |

### Sprint S4: MemIndex, Index Integration, Jepsen Register (2 weeks)

Priority: High FMEA items (FM6)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S4.1 | Implement `MemIndex` in ferrosa-storage: BTreeMap-based, lookup by column value + timestamp filter | M | Point lookup by column value returns all matching partition keys with `accord_ts <= read_ts`. Range scan supported. Deletion removes old index entries. | `mem_index_apply_gc`, `mem_index_update_replaces`, `mem_index_delete_removes` | Spec §9.2 |
| S4.2 | Atomic MemIndex + memtable apply: both updated in same logical transaction in Apply handler | M | No interleaving between MemIndex update and memtable write for same shard. Crash between the two operations: both recovered from same commit log entry. | `mem_index_memtable_atomicity`, `mem_index_crash_recovery` | FM6 (RPN 189) |
| S4.3 | MemIndex flush GC: `flush_gc(flushed_up_to_ts)` removes entries covered by persistent index | S | After flush, entries with `accord_ts <= flushed_up_to_ts` removed. Entries above threshold retained. | `mem_index_flush_gc_boundary` | Spec §9.2 |
| S4.4 | Eager index build trigger: `on_flush_complete` schedules async index build at `Priority::High` | S | Index build triggered immediately after flush. Layer 4 (unindexed SSTables) stays at 0-1 entries in steady state. | `eager_index_build_on_flush` | Spec §9.3 |
| S4.5 | Jepsen register test: concurrent reads and writes to single CQL row, kill minority nodes | L | Knossos linearizability checker finds no violations. Test runs with partition + kill nemesis. | `jepsen_register_linearizability` | Spec §11.3 |
| S4.6 | Performance baseline: measure single-key write P50/P99 vs current QUORUM | M | P50 within +15% of baseline. P99 within +25%. Results recorded in benchmark history. | `perf_single_key_write_p50`, `perf_single_key_write_p99` | Spec §11.4, G8 |
| S4.7 | Serialize `DeleteTarget` variants (Column + MapElement) in Accord commit log entries | S | `DeleteTarget::MapElement{column, key}` roundtrips through commit log. Apply handler processes both variants correctly. | `accord_delete_target_serialization` | UDF/UDA branch |
| S4.8 | Handle `WhereClause.token_fn` in Accord routing: range scans routed as range operations (not point lookups) in ConflictIndex | S | Token-range predicate queries register in `range_ops` BTreeMap, not `single_key` HashMap. Range conflicts detected correctly. | `accord_token_fn_range_routing` | UDF/UDA branch |
| S4.9 | Implement `.accord` sidecar file: on SSTable flush, write companion file with `AccordApplied` results keyed by `TxnId`. Delete after all-shard ExclusiveSyncPoint confirmation. | M | Sidecar written alongside SSTable. S3 upload includes sidecar. Recovery reads results from sidecar. Normal reads never touch it. Deleted after all-shard confirmation. | `accord_sidecar_write_on_flush`, `accord_sidecar_recovery_read`, `accord_sidecar_gc` | OQ5 |
| S4.10 | Add `accord_ts` and `apply_ts` dual timestamps to SUBSCRIBE change event payload. Default ordering by `accord_ts`. | S | SUBSCRIBE events carry both timestamps. Events ordered by `accord_ts` (commit order). Consumers can sort by `apply_ts` for arrival order. Additive wire change — old consumers unaffected. | `subscribe_dual_timestamps`, `subscribe_accord_ts_ordering` | OQ3 |

## Phase 2: Multi-Key Transactions — Sprint S5

**Gate:** Jepsen bank test passes; write skew test passes.

### Sprint S5: Cross-Partition Transactions (2 weeks)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S5.1 | Add `BEGIN TRANSACTION / COMMIT / ROLLBACK` to ferrosa-cql parser | M | Parser produces `Statement::BeginTransaction`, `Statement::Commit`, `Statement::Rollback`. Multi-statement accumulation in client session state. | `parse_begin_commit_rollback` | Spec §12 Phase 2 |
| S5.2 | Read-set / write-set extraction from accumulated CQL statements | M | Each statement in a transaction block contributes to the union read-set and write-set. Partition keys and column references extracted. | `readset_writeset_extraction` | Spec §12 Phase 2 |
| S5.3 | Cross-shard Execute: parallel Read RPCs to nearest replica per shard, with partial failure handling | L | All shards must respond for Execute to succeed. Partial failure (one shard unreachable) retries or aborts. No partial application. | `cross_shard_execute_all_or_nothing`, `cross_shard_partial_failure_abort` | FM10 (RPN 180) |
| S5.4 | Client-side retry with same `TxnId` on coordinator failure | M | Client re-sends transaction with same `TxnId`. Recovery coordinator detects duplicate and returns existing result (idempotent). | `client_retry_same_txnid_idempotent` | Spec §12 Phase 2 |
| S5.5 | Conflict detection across shards: ConflictIndex partitioned by token range | M | Multi-partition transaction registers in ConflictIndex for each shard's token range. Cross-shard conflicts detected. | `cross_shard_conflict_detection` | Spec §12 Phase 2 |
| S5.6 | Jepsen bank test: 100 accounts, concurrent transfers, total balance invariant | L | Total balance never changes. All transfers atomic. Runs with partition + kill + slow + clock-skew nemesis. | `jepsen_bank_atomicity` | Spec §11.3 |
| S5.7 | Jepsen write-skew test: two concurrent transactions reading shared counter | M | With strict serializability, only one transaction commits if both base write on same read value. | `jepsen_write_skew` | Spec §11.3 |
| S5.8 | Per-connection limit on concurrent in-flight transactions (default 16) and transaction timeout (default 10s) | S | More than 16 concurrent transactions per connection returns `Overloaded`. Transaction not committed within 10s auto-aborts. | `transaction_connection_limit`, `transaction_timeout_abort` | AT02, FM16 |
| S5.9 | Max keys per transaction limit (default 128, configurable) | S | Transaction with > 128 keys rejected at CQL parse time before entering Accord path. | `transaction_max_keys_limit` | AT01 |

## Phase 3: Transactional 2i — Sprint S6

**Gate:** 2i correctness test passes; dep-wait latency < 5ms P99.

### Sprint S6: Transactional Secondary Indexes (2 weeks)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S6.1 | Implement `READ_2I` 5-layer algorithm in ferrosa-cql 2i query planner | XL | Layers 1-5 queried in order. Results merged with deletion handling. Dep-wait for in-flight conflicts. No phantom reads. | `read_2i_five_layer_merge`, `read_2i_no_phantom_reads` | Spec §9.1 |
| S6.2 | CommitIndex `indexed_writes` projection populated at PreAccept time | M | Column projections for indexed columns tracked in ConflictIndex. 2i query can enumerate in-flight writes to a specific column value. | `commit_index_indexed_writes` | Spec §9.1 |
| S6.3 | Dep-wait for in-flight transactions in 2i query path | M | Pending deps awaited until Committed. Re-evaluated with known `accord_ts`. Timeout if dep-wait exceeds 5ms P99. | `2i_dep_wait_latency` | Spec §9.1, Gate |
| S6.4 | Unindexed SSTable bloom filter + BTI scan (Step 5 of READ_2I) | M | Bloom filter pre-check before scanning. Only SSTables flushed after `index.last_built_flush_id` are scanned. | `2i_unindexed_sstable_scan` | Spec §9.1 |
| S6.5 | `eventual` consistency mode for non-transactional indexes | S | `CREATE INDEX ... WITH OPTIONS = {'consistency': 'eventual'}` skips Steps 3-5. Uses only persistent index. | `2i_eventual_mode` | Spec §9.4 |
| S6.6 | 2i correctness test: concurrent write + 2i read, assert no stale result | L | Writer inserts row with indexed column. Concurrent reader queries via 2i. Reader never sees pre-insert state after writer's Accord transaction commits. | `2i_concurrent_write_read_consistency` | Spec §12 Phase 3 Gate |

## Phase 4: Electorate Reconfiguration — Sprint S7

**Gate:** Chaos test: kill minority during transactions; no lost commits; Jepsen all-tests pass with nemesis.

### Sprint S7: Epoch Management and Chaos Testing (2 weeks)

Priority: High FMEA items (FM17, FM7, FM12)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| S7.1 | Epoch field propagation through all protocol messages | M | Every Accord message includes `epoch`. Replicas validate epoch matches their current config. | `epoch_propagation_all_messages` | Spec §8.2, FM7 |
| S7.2 | Slow-path fallback when epoch mismatch detected at PreAccept | M | Replica at epoch `e2 > e1` returns `PreAcceptOK` with `t.epoch = e2`. Coordinator detects mismatch, falls back to slow path, fetches new config. | `epoch_mismatch_slow_path_fallback` | Spec §8.2, FM7 |
| S7.3 | `JoinElectorate` protocol: new members wait for fast-path history from prior electorate | L | New member receives `JoinElectorate` from `E_old - F_old + 1` members. All fast-path committed transactions under previous configs transferred. `ready_electorate[epoch]` only set after all four gates pass. | `join_electorate_four_gates` | Spec §8.2, FM12, AT25 |
| S7.4 | Electorate shrink on node failure: openraft commit -> epoch increment -> quorum resize | M | `fast_quorum_size` recomputed dynamically. Quorum threshold adjusts when electorate shrinks. Never hardcoded. | `electorate_shrink_quorum_resize` | Spec §8.1, FM12 |
| S7.5 | Epoch transition drain period: no new transactions for old epoch, in-flight allowed to complete | M | Drain period configurable (default 30s). Exceeds `SkewMax + max_transaction_timeout`. Transactions not completed in drain period aborted and retried. | `epoch_drain_period` | FM17, AT29 |
| S7.6 | Electorate validation: coordinator only counts votes from nodes in epoch-scoped electorate set | S | Responses from unknown `host_id`s discarded. Stale epoch responses rejected. Non-electorate votes not counted toward quorum. | `electorate_vote_validation` | FM12, AT23 |
| S7.7 | Jepsen all-tests with full nemesis: partition + kill + slow + clock-skew + pause | XL | Register, bank, long-fork, monotonic, write-skew all pass with all nemesis operations active simultaneously. | `jepsen_full_nemesis_suite` | Spec §11.3 |
| S7.8 | Chaos test: kill minority during active transactions, verify no lost commits | L | Start 100 concurrent transactions. Kill 1 of 3 nodes mid-transaction. All committed transactions durable after recovery. Zero data loss. | `chaos_minority_kill_no_lost_commits` | Phase 4 Gate |
| S7.9 | Performance regression suite: all benchmarks from spec §11.4 | M | Single-key write P50 within +15%. Read P50 within -10%. Multi-key txn P50 < 2x single-key. Conflict index P99 < 50us. ReorderBuffer P99 < 5ms. | `perf_regression_suite` | Spec §11.4 |
| S7.10 | Implement two-phase DDL: broadcast "DDL pending" marker via Raft, new txns dep-wait on it, apply after pre-marker txns complete. Replaces drain-and-block from S3.8. | L | DDL pending marker causes new Accord txns to include DDL in dep set. Pre-marker txns allowed to complete. DDL applied after drain. Very short unavailability window. | `two_phase_ddl_dep_wait`, `two_phase_ddl_concurrent_dml` | OQ2 Phase 4 |

## Sprint Timeline

```
Week 1-2:   S1 — Core types, critical mitigations, 24-step test
Week 3-4:   S2 — ReorderBuffer, heartbeat, recovery foundation
Week 5-6:   S3 — AccordStateMachine, coordinator, dep-wait
Week 7-8:   S4 — MemIndex, index integration, Jepsen register
Week 9-10:  S5 — Multi-key transactions, Jepsen bank
Week 11-12: S6 — Transactional 2i
Week 13-14: S7 — Electorate reconfiguration, full Jepsen + chaos
```

Total: 14 weeks (7 sprints x 2 weeks each)

## UDF/UDA Branch Merge Points

The `feature/udf-uda-query-time` branch must be merged before or during Sprint S1 to establish the baseline. Key integration points:

| Sprint | Integration Task | Branch Change |
|--------|-----------------|---------------|
| S1 | Commit log oversized entry handling | Already returns `Err` instead of panic — Accord benefits directly |
| S1 | ConflictIndex interacts with `DeleteTarget` enum | `DeleteTarget::MapElement` must be serialized in Accord entries |
| S2 | Late data debouncer ordering | Must use Accord timestamps, not wall clock |
| S4 | `WhereClause.token_fn` routing | Range predicates route through `range_ops` in ConflictIndex |
| S4 | Row-level deletion LWW in memtable | Must be idempotent when replayed from Accord log |

## Phase Gates Summary

| Phase | Gate | Verification |
|-------|------|-------------|
| 0 (S1-S2) | All Phase 0 unit tests pass; 24-step EPaxos correctness test in CI | `cargo test -p ferrosa-common -p ferrosa-storage -p ferrosa-cluster -- accord` |
| 1 (S3-S4) | Jepsen register passes; P50 within 15% of baseline | Jepsen suite + benchmark comparison |
| 2 (S5) | Jepsen bank + write-skew pass | Jepsen suite with bank + write-skew workloads |
| 3 (S6) | 2i correctness test passes; dep-wait P99 < 5ms | Integration test + latency benchmark |
| 4 (S7) | Chaos test + Jepsen all-tests with full nemesis | Full Jepsen + chaos suite |
