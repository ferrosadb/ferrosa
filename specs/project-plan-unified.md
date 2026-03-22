# Unified Project Plan — Ferrosa Ecosystem

> Last updated: 2026-03-21
> Status: Draft
> Priority order: ferrosa (core DB) → ferrosa-memory → ferrosa-dbaas → Temporal compatibility
> Strategy: Accord-first — no throwaway LWT; build CAS semantics on Accord

## Assumptions

| # | Assumption | Status |
|---|-----------|--------|
| A1 | ferrosa-memory connects to ferrosa via CQL v5 over TCP | Unvalidated |
| A2 | ferrosa-dbaas uses ferrosa in all three modes (standalone, pair, cluster) | Validated (CLAUDE.md) |
| A3 | Temporal uses Cassandra's gocql Go driver, which speaks CQL v4/v5 | Validated (research) |
| A4 | Temporal's LWT usage is single-partition CAS (Paxos), not multi-partition | Validated (all IF conditions on same partition key) |
| A5 | ferrosa-memory does not require transactions or LWT | Unvalidated — needs verification |
| A6 | ferrosa-dbaas control plane uses BATCH for denormalized writes, not LWT | Validated (CLAUDE.md design invariants) |
| A7 | Accord is a superset of LWT — single-partition CAS is just an Accord txn with a read-before-write in Execute | Validated (spec §4.4) |
| A8 | LWT semantics (IF NOT EXISTS, IF col = ?) are implemented as Accord transactions, not a separate CAS mechanism | Decision — eliminates throwaway code |
| A9 | Temporal requires LOCAL_SERIAL consistency level for LWT reads | Validated (research) |
| A10 | Accord S1-S3 (foundation + state machine) must land before LWT is functional | Validated — LWT needs the Execute phase to do read-before-write |

## Estimation Methodology

- **Mode:** Story points (relative sizing) for tactical work (< 8 weeks per stream)
- **Basis:** Decomposition + analogy to completed ferrosa sprints
- **Reference stories:** S=1-2 days, M=3-5 days, L=5-8 days, XL=8-12 days
- **Buffer:** 20% contingency per sprint for discovery/rework
- **Velocity assumption:** Based on observed ferrosa velocity (~8 tasks/sprint at S-L mix)

## Value Stream Analysis

```
ferrosa-memory ──needs──→ Core CQL (INSERT/SELECT/UPDATE/DELETE)     ✓ EXISTS
                          Collections (map/set/list mutations)       ✓ EXISTS
                          UDTs, frozen<>                             ✓ EXISTS
                          HNSW vector indexes                        ✓ EXISTS
                          Phonetic indexes                           ✓ EXISTS
                          Property graph (Cypher)                    ✓ EXISTS
                          SUBSCRIBE                                  ✓ EXISTS
                          BATCH (logged)                             ✓ EXISTS
                          TTL                                        ✓ EXISTS

ferrosa-dbaas  ──needs──→ Everything ferrosa-memory needs            ✓ EXISTS
                          Multi-node cluster (pair + Raft ring)      ✓ EXISTS
                          SUBSCRIBE DELTA                            ✓ EXISTS
                          Schema replication                         ✓ EXISTS

Temporal       ──needs──→ Everything above, PLUS:
                          IF NOT EXISTS on INSERT (LWT)              ✗ PARSED, NOT ENFORCED
                          IF column = ? on UPDATE (LWT)              ✗ NOT PARSED
                          IF column = ? on DELETE (LWT)              ✗ NOT PARSED
                          Batch CAS (logged batch with LWT)          ✗ MISSING
                          Pagination / paging state                  ✗ MISSING
                          SERIAL / LOCAL_SERIAL consistency          ✗ MISSING
                          toTimestamp(now()) function                 ✗ MISSING
                          TTL() function (read remaining TTL)        ✗ MISSING
```

## Strategy: Accord-First

Instead of building throwaway single-partition Paxos, we build Accord foundations first,
then implement LWT as Accord transactions:

```
IF NOT EXISTS → Accord txn with read_set={PK}, write_set={PK}
                Execute phase: read row, check existence
                Apply phase: write if not exists, return [applied]=true/false

IF col = ?   → Accord txn with read_set={PK}, write_set={PK}
                Execute phase: read row, evaluate conditions
                Apply phase: write if conditions met, return [applied]=true/false

Batch CAS    → Accord txn with read_set=union(PKs), write_set=union(PKs)
                Execute phase: read all rows, check all conditions
                Apply phase: apply all mutations if all conditions pass
```

This is elegant: LWT is just a pattern on top of Accord, not a separate mechanism.
Temporal's single-partition CAS benefits from Accord's leaseholder fast path (1 RTT).

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|-----------|
| ferrosa-memory has undiscovered CQL gaps | Medium | High | Run integration tests early (Sprint M1) |
| Accord foundation takes longer than 6 weeks | Medium | High | Accord S1-S2 are well-specified with 302 tests; track velocity against plan |
| Temporal's gocql driver uses CQL protocol features we haven't implemented | High | High | Run Temporal schema DDL against ferrosa first; instrument rejected queries |
| Pagination requires significant protocol changes | Medium | Medium | CQL v5 paging is well-specified; implement at frame level |
| Batch CAS on Accord is untested pattern | Medium | High | Temporal's batch CAS is always same-partition; this is Accord single-shard fast path |

---

## Stream 1: ferrosa-memory Integration (Weeks 1-2)

**Goal:** ferrosa-memory connects to ferrosa, all 12 MCP tools work end-to-end.

**Gate:** `cargo test` in ferrosa-memory passes with ferrosa as backend.

### Sprint M1: Integration Validation

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| M1.1 | Stand up ferrosa in standalone mode, run ferrosa-memory integration tests against it | M | All 12 MCP tools execute without CQL errors. Identify any failing queries. | `ferrosa_memory_integration_smoke` |
| M1.2 | Fix any CQL gaps discovered by M1.1 | S-L | All ferrosa-memory tests pass | Per-gap tests |
| M1.3 | Performance baseline: measure ferrosa-memory latency vs Cassandra | S | Latency comparison recorded. No P99 > 100ms for single-key operations. | `ferrosa_memory_latency_baseline` |
| M1.4 | Document ferrosa + ferrosa-memory deployment in docker-compose | S | `docker-compose up` starts both services, tools work | `docker_compose_smoke` |

**Estimated effort:** 1 sprint (2 weeks). Mostly validation, not implementation.

---

## Stream 2: ferrosa-dbaas Integration (Weeks 3-4)

**Goal:** ferrosa-dbaas control plane works with ferrosa in all three modes.

**Gate:** dbaas-api can provision a tenant, metering writes raw samples, billing reads aggregates.

### Sprint D1: Control Plane Validation

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| D1.1 | Run ferrosa-dbaas schema DDL against ferrosa (4 keyspaces: control, billing, metrics, audit) | M | All tables, types, indexes created successfully | `dbaas_schema_ddl` |
| D1.2 | Run ferrosa-dbaas integration tests against ferrosa | M | Tenant CRUD, metering writes, billing reads all pass | `dbaas_integration_smoke` |
| D1.3 | Test pair mode: provision HA tenant with 2 ferrosa VMs | L | Pair replication works. Failover works. | `dbaas_pair_mode_ha` |
| D1.4 | Test ring mode: provision Scale tenant with 3 ferrosa nodes | L | Raft quorum, QUORUM reads/writes, node failure recovery | `dbaas_ring_mode_scale` |
| D1.5 | Fix any CQL gaps discovered | S-L | All dbaas tests pass | Per-gap tests |

**Estimated effort:** 1 sprint (2 weeks).

---

## Stream 3: Accord Foundation + CQL Permanents (Weeks 5-10)

**Goal:** Accord state machine operational for single-key writes. LWT-independent CQL
features (pagination, functions, IF condition parsing) built in parallel.

This stream merges Accord S1-S3 with the permanent CQL work that Temporal needs regardless
of the transaction mechanism.

### Sprint A1: Accord Types + CQL Parsing (Weeks 5-6)

Combines Accord S1 (core types) with Temporal CQL parsing work. Both are foundational,
no dependencies between them, maximum parallelism.

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A1.1 | Implement `Timestamp` type (epoch, time, seq, node), `HybridLogicalClock`, `TxnId`, `AcceptedBallot`/`PromisedBallot` newtypes | M | Types compile. Ord correct. Ballots are distinct types. HLC monotonic. | 38 tests (Layer 1 test spec) | Accord S1.1-S1.3 |
| A1.2 | Implement `TxnState` with two ballot fields, phase flags, deps | S | Invariant `accepted_ballot <= max_ballot_seen` enforced. Phase transitions forward-only. | 9 tests (Layer 1.3 test spec) | Accord S1.4 |
| A1.3 | Implement `ConflictIndex` (HashMap + BTreeMap + indexed_writes) | L | O(1) single-key, O(log n) range. Hard cap. GC after Applied. | 9 tests (Layer 1.4 test spec) | Accord S1.7 |
| A1.4 | Implement dual-log architecture: protocol log (local-only) + main log | L | Protocol entries never in S3 queue. GC after Applied. Replay on startup. | 7 tests (infrastructure spec) | Accord S1.5 |
| A1.5 | Implement fsync-before-ack wrapper | S | Persist before reply on all handlers. | 5 tests (infrastructure spec) | Accord S1.6 |
| A1.6 | Build `TestCluster` deterministic message harness | L | FIFO/out-of-order delivery, drop, drain, no-tokio. | 6 tests (infrastructure spec) | Missing task X1 |
| A1.7 | Implement 24-step EPaxos correctness test | M | Test passes. Mutation testing proves it catches real bugs. | 1 test, 24 assertions + 5 mutations (Layer 5) | Accord S1.9 |
| A1.8 | Parse IF conditions on UPDATE/DELETE: `IF col = ?`, `IF col != ?`, `IF col IN ?` | M | Parser produces `if_conditions: Vec<IfCondition>`. Multiple conditions with AND. | `parse_update_if_condition`, `parse_delete_if_condition` | Temporal T1.2 |
| A1.9 | Implement pagination: `page_size` in QUERY frame, `paging_state` in RESULT frame | L | 500-row table, page_size=100 → 5 pages. Paging state is opaque, cross-node compatible. | `pagination_basic`, `pagination_multi_page`, `pagination_state_roundtrip` | Temporal T3.1-T3.2 |
| A1.10 | Implement Accord write gate (non-transactional writes check ConflictIndex) | M | Non-txn INSERT to key with in-flight Accord txn is routed through Accord. | 4 tests (infrastructure spec) | Accord S1.8, FM16 |

### Sprint A2: Accord Protocol + CQL Functions (Weeks 7-8)

Combines Accord S2 (ReorderBuffer, heartbeat, recovery) with Temporal's CQL function work.

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A2.1 | Extend heartbeat with `sent_at`/`recv_at`, per-link RTT, SkewMax measurement | L | P99.9 skew from heartbeats. Hard ceiling. Outlier rejection. | 7 tests (infrastructure spec) | Accord S2.1-S2.2 |
| A2.2 | Implement `ReorderBuffer` with TimerWheel | L | Messages in t0 order. Deadline formula correct. Overflow → backpressure. | 5 tests (infrastructure spec) | Accord S2.3 |
| A2.3 | Add 11 Accord protocol message types to ferrosa-net | M | All roundtrip correctly. Unique type codes. Size bounded. | 4 tests (infrastructure spec) | Accord S2.8 |
| A2.4 | Implement `RecoveryCoordinator` foundation | L | Selects by `max(accepted_ballot)`. Majority quorum. | Layer 3.4 + Layer 4.3 tests | Accord S2.5 |
| A2.5 | ConflictIndex GC: respect in-flight deps, evict only applied | M | Dep-waiters never evicted. | 2 tests (infrastructure spec) | Accord S2.4 |
| A2.6 | `MAX_CLOCK_DRIFT` validation at replica | S | Reject future timestamps. Don't advance HLC. | 2 tests (infrastructure spec) | Accord S2.9 |
| A2.7 | Implement `now()` → Timeuuid, `toTimestamp(timeuuid)` → timestamp, `TTL(column)` → int | M | `SELECT toTimestamp(now())` works. `SELECT TTL(v)` returns remaining TTL. | `cql_function_now`, `cql_function_to_timestamp`, `cql_function_ttl` | Temporal T3.3-T3.5 |
| A2.8 | Add SERIAL and LOCAL_SERIAL consistency levels + serial_consistency in QUERY frame | M | Wire codes 0x0008/0x0009. Parsed from frame flags. | `consistency_serial_wire_code`, `frame_serial_consistency_parse` | Temporal T2.3-T2.4 |
| A2.9 | Late data debouncer Accord timestamp ordering | M | Deterministic across replicas. | 4 tests (UDF integration spec) | Accord S2.6 |

### Sprint A3: AccordStateMachine + LWT on Accord (Weeks 9-10)

The state machine lands, and LWT is implemented as an Accord transaction pattern.

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A3.1 | Implement `AccordStateMachine`: PreAccept, Accept, Commit, Execute, Apply handlers. FireAndForget flag for CL=ONE. | XL | Phase transitions match spec. Persists before reply. Idempotent. Fire-and-forget ACKs after PreAccept. | 8 tests (Layer 2.1) + 7 tests (Layer 2.2) + 20 tests (Layer 3) | Accord S3.1 |
| A3.2 | Implement `AccordCoordinator`: leaseholder fast path, slow path fallback | L | 1 RTT leaseholder, 2 RTT non-leaseholder. Quorum formula correct. | 5 tests (Layer 4.2) + quorum tests | Accord S3.2 |
| A3.3 | Wire CQL router to AccordCoordinator for all DML in cluster mode | M | INSERT/UPDATE/DELETE go through Accord. | `cql_route_through_accord` | Accord S3.7 |
| A3.4 | Implement LWT as Accord transaction: IF NOT EXISTS → Accord txn with read-before-write in Execute | L | `INSERT IF NOT EXISTS` returns `[applied]=true/false`. Correct result set format. Single-partition, 1 RTT via leaseholder. | `lwt_insert_if_not_exists`, `lwt_result_set_format` | Temporal T1.1, T1.6 |
| A3.5 | Implement LWT: IF conditions on UPDATE/DELETE → Accord txn with condition evaluation in Execute | L | `UPDATE ... IF col = ?` succeeds when condition met, returns `[applied]=false` with current values otherwise. | `lwt_update_if_condition`, `lwt_delete_if_condition` | Temporal T1.3-T1.5 |
| A3.6 | Implement Batch CAS: logged batch with LWT → single Accord txn, all conditions checked in Execute | XL | Batch with multiple LWT statements: all-or-nothing. Per-row `[applied]` on failure. | `batch_cas_all_or_nothing`, `batch_cas_result_format` | Temporal T2.1-T2.2 |
| A3.7 | Dep-wait with cycle detection | L | Waits-for graph. Cycle broken by aborting highest t0. Timeout 10s. | 7 tests (integration spec) | Accord S3.3 |
| A3.8 | DDL drain-and-block | M | Table unavailable during DDL. In-flight complete. | 4 tests (integration spec) | Accord S3.8 |

---

## Stream 4: Temporal Validation + Accord Completion (Weeks 11-18)

**Goal:** Temporal runs against ferrosa. Accord gets MemIndex, multi-key, and Jepsen.

### Sprint A4: Temporal Integration + MemIndex (Weeks 11-12)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A4.1 | Run Temporal schema DDL against ferrosa (14 tables, 2 indexes, 1 UDT) | L | `temporal-sql-tool create` completes without errors | `temporal_schema_ddl` | Temporal T3.7 |
| A4.2 | Run Temporal Go integration tests against ferrosa | XL | Shard store, execution store, history store, metadata store tests pass. Identify remaining gaps. | `temporal_integration_suite` | Temporal T3.8 |
| A4.3 | Fix Temporal integration gaps discovered by A4.2 | L | All Temporal persistence tests pass | Per-gap tests | |
| A4.4 | Implement `MemIndex` (BTreeMap, atomic with memtable, flush GC) | M | Lookup, update-replaces, delete-removes, flush GC boundary. | 13 tests (memindex spec) | Accord S4.1-S4.3 |
| A4.5 | Leaseholder assignment with staleness detection | M | Failover updates. Epoch mismatch detected. | 5 tests (integration spec) | Accord S3.4 |
| A4.6 | Linearizable local read (dep-check against ConflictIndex) | M | Read waits for in-flight txns to Apply. | 4 tests (integration spec) | Accord S3.5 |

### Sprint A5: Jepsen Register + Crash Recovery (Weeks 13-14)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A5.1 | Build Jepsen infrastructure (cluster provisioning, client, nemesis) | XL | 3-node cluster, CQL client, partition+kill+slow+clock-skew nemesis | 8 tests (system spec) | Missing task X2 |
| A5.2 | Jepsen register test | L | Knossos linearizability: zero violations with kill + partition nemesis | `jepsen_register_linearizability` | Accord S4.5 |
| A5.3 | Commit log replay on startup | M | TxnState reconstructed. Applied txns not re-applied. ConflictIndex rebuilt. | 4 tests (integration spec) | Accord S3.6 |
| A5.4 | Sidecar `.accord` file (write on flush, recovery read, GC) | M | Zero per-row overhead. Recovery reads sidecar. Normal reads ignore it. | 6 tests (memindex spec) | Accord S4.9 |
| A5.5 | Eager index build on flush | S | Index build at Priority::High after each flush. | 3 tests (memindex spec) | Accord S4.4 |
| A5.6 | Performance baseline: P50/P99 vs QUORUM baseline | M | P50 within +15%. | `perf_single_key_write_p50/p99` | Accord S4.6 |
| A5.7 | SUBSCRIBE dual timestamps (accord_ts + apply_ts) | S | Events carry both. Default ordering by accord_ts. | 4 tests (memindex spec) | Accord S4.10 |
| A5.8 | ExclusiveSyncPoint / DurabilityService | L | GC coordinator for sidecar files, protocol log, ConflictIndex | 7 tests (system spec) | Missing task X3 |

### Sprint A6: Multi-Key Transactions + Jepsen Bank (Weeks 15-16)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A6.1 | BEGIN TRANSACTION / COMMIT / ROLLBACK parser | M | Multi-statement accumulation in session state. | `parse_begin_commit_rollback` | Accord S5.1 |
| A6.2 | Read-set / write-set extraction | M | Union sets from accumulated statements. | `readset_writeset_extraction` | Accord S5.2 |
| A6.3 | Cross-shard Execute with partial failure handling | L | All-or-nothing. Parallel reads. | 5 tests (multikey spec) | Accord S5.3 |
| A6.4 | Client retry with same TxnId (idempotent) | M | Recovery returns existing result. | 3 tests (multikey spec) | Accord S5.4 |
| A6.5 | Transaction limits (16 concurrent, 10s timeout, 128 max keys) | S | Overloaded on limit. Auto-abort on timeout. | 4 tests (multikey spec) | Accord S5.8-S5.9 |
| A6.6 | Jepsen bank test | L | Total balance invariant. Atomicity with nemesis. | `jepsen_bank_atomicity` | Accord S5.6 |
| A6.7 | Jepsen write-skew test | M | No lost updates under strict serializability. | `jepsen_write_skew` | Accord S5.7 |
| A6.8 | Accord observability metrics (9 Prometheus gauges/counters) | M | txn_in_flight, recovery_in_progress, fast_path_ratio, etc. | 9 tests (system spec) | Missing task X4 |

### Sprint A7: Transactional 2i + Electorate (Weeks 17-18)

| # | Task | Size | Success Criteria | Tests | Source |
|---|------|------|-----------------|-------|--------|
| A7.1 | READ_2I 5-layer algorithm | XL | Layers 1-5 queried. Dep-wait. No phantoms. | 8 tests (memindex spec) | Accord S6.1 |
| A7.2 | Epoch propagation + mismatch fallback | M | Every message carries epoch. Mismatch → slow path. | 3 tests (multikey spec) | Accord S7.1-S7.2 |
| A7.3 | JoinElectorate protocol (4 gates) | L | New member waits for all gates. | 3 tests (multikey spec) | Accord S7.3 |
| A7.4 | Electorate shrink + quorum resize | M | Dynamic quorum. Vote validation. | 3 tests (multikey spec) | Accord S7.4, S7.6 |
| A7.5 | Epoch transition drain | M | 30s drain. In-flight complete. | 3 tests (multikey spec) | Accord S7.5 |
| A7.6 | Two-phase DDL | L | DDL pending marker. Dep-wait. | 4 tests (multikey spec) | Accord S7.10 |
| A7.7 | Jepsen full-nemesis suite + chaos test | XL | All workloads + all nemesis. Zero data loss. | 5 tests (system spec) | Accord S7.7-S7.8 |
| A7.8 | Performance regression suite | M | All benchmarks pass thresholds. | 7 tests (system spec) | Accord S7.9 |
| A7.9 | UDF/UDA branch integration (DeleteTarget, token_fn, LWW, pk_indexes) | M | All roundtrips. Deterministic. Idempotent. | 23 tests (UDF integration spec) | UDF branch merge |

---

## Unified Timeline

```
Week  1-2:  M1  — ferrosa-memory integration validation
Week  3-4:  D1  — ferrosa-dbaas control plane validation
Week  5-6:  A1  — Accord types + ConflictIndex + 24-step test + pagination + IF parsing
Week  7-8:  A2  — ReorderBuffer + heartbeat + recovery + CQL functions + SERIAL CL
Week  9-10: A3  — AccordStateMachine + LWT-on-Accord + Batch CAS + dep-wait
Week 11-12: A4  — Temporal integration + MemIndex + leaseholder
Week 13-14: A5  — Jepsen register + crash recovery + sidecar + DurabilityService
Week 15-16: A6  — Multi-key transactions + Jepsen bank + observability
Week 17-18: A7  — Transactional 2i + electorate + full Jepsen + chaos
```

**Total: 18 weeks (9 sprints)**

Reduced from 24 weeks by eliminating throwaway LWT and merging Accord foundation with
CQL permanent work.

## Phase Gates

| Phase | Week | Gate | Verification |
|-------|------|------|-------------|
| Memory | 2 | ferrosa-memory 12 MCP tools work | `cargo test` in ferrosa-memory |
| DBaaS | 4 | Control plane CRUD + metering + billing work | `cargo test` in ferrosa-dbaas |
| Accord Phase 0 | 8 | 24-step EPaxos test + foundation unit tests | CI gate (302 tests from 7 spec files) |
| LWT on Accord | 10 | IF NOT EXISTS + IF conditions + Batch CAS pass | Unit + property tests |
| Temporal | 12 | Temporal schema DDL + Go persistence tests pass | `temporal-sql-tool create` + Go suite |
| Accord Phase 1 | 14 | Jepsen register + P50 within 15% baseline | Jepsen + benchmark |
| Accord Phase 2 | 16 | Jepsen bank + write-skew | Jepsen suite |
| Accord Phase 4 | 18 | Jepsen full nemesis + chaos + electorate | Full Jepsen + chaos |

## Critical Path

```
M1 ──┐
     ├──→ A1 ──→ A2 ──→ A3 ──→ A4 ──→ A5 ──→ A6 ──→ A7
D1 ──┘    │            │      │
          │            │      └─ LWT functional (Temporal can start)
          │            └─ SERIAL CL + functions (Temporal CQL ready)
          └─ Pagination (Temporal pagination ready)
```

All three Temporal prerequisites (LWT, pagination, functions) converge at week 10 (A3).
Temporal validation runs in A4 (weeks 11-12). The remaining 6 weeks (A5-A7) harden Accord
with Jepsen, multi-key, 2i, and electorate — none of which Temporal blocks on.

## Test Count by Sprint

| Sprint | New Tests | Cumulative |
|--------|-----------|-----------|
| M1 | ~12 | 12 |
| D1 | ~10 | 22 |
| A1 | ~82 | 104 |
| A2 | ~30 | 134 |
| A3 | ~55 | 189 |
| A4 | ~30 | 219 |
| A5 | ~33 | 252 |
| A6 | ~32 | 284 |
| A7 | ~70 | 354 |
| **Total** | **354** | |
