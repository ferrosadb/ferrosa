# Unified Project Plan — Ferrosa Ecosystem

> Last updated: 2026-03-21
> Status: Draft
> Priority order: ferrosa (core DB) → ferrosa-memory → ferrosa-dbaas → Temporal compatibility

## Assumptions

| # | Assumption | Status |
|---|-----------|--------|
| A1 | ferrosa-memory connects to ferrosa via CQL v5 over TCP | Unvalidated |
| A2 | ferrosa-dbaas uses ferrosa in all three modes (standalone, pair, cluster) | Validated (CLAUDE.md) |
| A3 | Temporal uses Cassandra's gocql Go driver, which speaks CQL v4/v5 | Validated (research) |
| A4 | Temporal's LWT usage is single-partition CAS (Paxos), not multi-partition | Validated (all IF conditions on same partition key) |
| A5 | ferrosa-memory does not require transactions or LWT | Unvalidated — needs verification |
| A6 | ferrosa-dbaas control plane uses BATCH for denormalized writes, not LWT | Validated (CLAUDE.md design invariants) |
| A7 | Accord transactions are a superset of LWT but 14 weeks of work | Validated (project plan) |
| A8 | Single-partition CAS (LWT) can be implemented without full Accord | Validated — Cassandra does this with Paxos |
| A9 | Temporal requires LOCAL_SERIAL consistency level for LWT reads | Validated (research) |

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
                          ─────────────────────────────────────────
                          BLOCKING GAPS: TBD (need integration test)

ferrosa-dbaas  ──needs──→ Everything ferrosa-memory needs            ✓ EXISTS
                          Multi-node cluster (pair + Raft ring)      ✓ EXISTS
                          SUBSCRIBE DELTA                            ✓ EXISTS
                          Schema replication                         ✓ EXISTS
                          ─────────────────────────────────────────
                          BLOCKING GAPS: TBD (need integration test)

Temporal       ──needs──→ Everything above, PLUS:
                          IF NOT EXISTS on INSERT (LWT)              ✗ PARSED, NOT ENFORCED
                          IF column = ? on UPDATE (LWT)              ✗ NOT PARSED
                          IF column = ? on DELETE (LWT)              ✗ NOT PARSED
                          Batch CAS (logged batch with LWT)          ✗ MISSING
                          Pagination / paging state                  ✗ MISSING
                          LOCAL_SERIAL consistency level              ✗ MISSING
                          toTimestamp(now()) function                 ✗ MISSING
                          Clustering ORDER BY DESC                   ? NEEDS CHECK
                          ─────────────────────────────────────────
                          BLOCKING GAPS: 7 features, all P0
```

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|-----------|
| ferrosa-memory has undiscovered CQL gaps | Medium | High | Run integration test suite early (Sprint M1) |
| LWT without Accord is a dead-end | Low | Medium | Design LWT as stepping stone; Accord replaces Paxos-based CAS later |
| Temporal's gocql driver uses CQL protocol features we haven't implemented | High | High | Run Temporal schema DDL against ferrosa first; instrument rejected queries |
| Pagination requires significant protocol changes | Medium | Medium | CQL v5 paging is well-specified; implement at frame level |
| Batch CAS semantics are complex (Scylla got it wrong) | High | High | Study Cassandra source, write property tests before implementation |

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

**Estimated effort:** 1 sprint (2 weeks). Most work is validation, not implementation.

---

## Stream 2: ferrosa-dbaas Integration (Weeks 3-4)

**Goal:** ferrosa-dbaas control plane works with ferrosa in all three modes.

**Gate:** dbaas-api can provision a tenant, metering writes raw samples, billing reads aggregates.

### Sprint D1: Control Plane Validation

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| D1.1 | Run ferrosa-dbaas schema DDL against ferrosa (all 4 keyspaces: control, billing, metrics, audit) | M | All tables, types, indexes created successfully | `dbaas_schema_ddl` |
| D1.2 | Run ferrosa-dbaas integration tests against ferrosa | M | Tenant CRUD, metering writes, billing reads all pass | `dbaas_integration_smoke` |
| D1.3 | Test pair mode: provision HA tenant with 2 ferrosa VMs | L | Pair replication works. Write to primary, read from secondary. Failover works. | `dbaas_pair_mode_ha` |
| D1.4 | Test ring mode: provision Scale tenant with 3 ferrosa nodes | L | Raft quorum, QUORUM reads/writes, node failure recovery | `dbaas_ring_mode_scale` |
| D1.5 | Fix any CQL gaps discovered by D1.1-D1.4 | S-L | All dbaas tests pass | Per-gap tests |

**Estimated effort:** 1 sprint (2 weeks). Heavier testing than Stream 1 due to multi-node modes.

---

## Stream 3: Temporal Pre-Requisite CQL Features (Weeks 5-10)

**Goal:** All CQL features Temporal requires are implemented and tested.

**Gate:** Temporal schema DDL executes. Temporal's Go integration test suite passes against ferrosa.

This is the largest stream. Temporal needs 7 missing features, ordered by dependency:

### Sprint T1: LWT Foundation — IF NOT EXISTS + IF Conditions (Weeks 5-6)

LWT is Temporal's most critical requirement. Every workflow state mutation uses it.

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| T1.1 | Enforce IF NOT EXISTS on INSERT: read-before-write CAS in storage engine. If row exists, return `[applied]=false` with existing row. | L | `INSERT INTO t (id, v) VALUES (1, 'a') IF NOT EXISTS` returns `[applied]=true` first time, `[applied]=false` with existing values second time. | `lwt_insert_if_not_exists`, `lwt_insert_if_not_exists_idempotent` |
| T1.2 | Parse IF conditions on UPDATE: `UPDATE t SET v = ? WHERE id = ? IF col = ?`. Add `IfCondition { column, operator, value }` to AST. | M | Parser produces `UpdateStatement.if_conditions: Vec<IfCondition>`. Supports `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`. | `parse_update_if_condition`, `parse_update_if_multi_condition` |
| T1.3 | Enforce IF conditions on UPDATE: read current row, check conditions, apply or return `[applied]=false`. | L | `UPDATE t SET v = 'b' WHERE id = 1 IF v = 'a'` succeeds when v='a', returns `[applied]=false` with current values when v!='a'. | `lwt_update_if_condition`, `lwt_update_if_condition_false` |
| T1.4 | Parse and enforce IF conditions on DELETE: `DELETE FROM t WHERE id = ? IF col = ?` | M | Same semantics as UPDATE IF. Returns `[applied]=true/false`. | `lwt_delete_if_condition` |
| T1.5 | Parse and enforce IF EXISTS on UPDATE/DELETE (simple form, no column condition) | S | `UPDATE t SET v = ? WHERE id = ? IF EXISTS` returns `[applied]=false` if row doesn't exist. | `lwt_update_if_exists`, `lwt_delete_if_exists` |
| T1.6 | LWT result set format: return `[applied]` boolean column plus existing row values on failure | M | Result set contains `[applied]` as first column. On `[applied]=false`, remaining columns contain the current row values (Temporal reads these). | `lwt_result_set_format` |
| T1.7 | Single-partition CAS atomicity: LWT operations on same partition key are serialized (mutual exclusion) | L | Two concurrent `INSERT IF NOT EXISTS` on same PK: exactly one gets `[applied]=true`. No lost updates. Use per-partition lock or Paxos-lite. | `lwt_single_partition_serialization` |

### Sprint T2: Batch CAS + Consistency Levels (Weeks 7-8)

Temporal wraps every workflow mutation in a logged batch with CAS conditions.

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| T2.1 | Batch CAS: logged batch containing LWT statements executes atomically. All conditions checked, all-or-nothing apply. | XL | Batch with 3 statements (INSERT IF NOT EXISTS + UPDATE IF range_id = ? + INSERT). If any condition fails, entire batch fails with `[applied]=false`. | `batch_cas_all_or_nothing`, `batch_cas_partial_failure` |
| T2.2 | Batch CAS result format: on failure, return per-statement `[applied]` plus the offending row values | L | Temporal's `MapExecuteBatchCAS` reads per-row results to determine which condition failed. Format must match gocql expectations. | `batch_cas_result_format` |
| T2.3 | Add SERIAL and LOCAL_SERIAL consistency levels | M | `ConsistencyLevel::Serial` and `LocalSerial` variants. CQL wire codes 0x0008 and 0x0009. Used for LWT reads (serial consistency for the CAS operation). | `consistency_serial_wire_code`, `consistency_local_serial` |
| T2.4 | Serial consistency in QUERY/EXECUTE frames: parse `serial_consistency` field from CQL v5 frame when `flags & 0x0010` is set | M | Driver sends serial_consistency=LOCAL_SERIAL. Server reads it from the frame and uses it for LWT operations. | `frame_serial_consistency_parse` |
| T2.5 | LWT read-your-writes: after a successful LWT write at LOCAL_SERIAL, a subsequent read at LOCAL_QUORUM must see the written value | L | Write with IF NOT EXISTS (serial). Read with QUORUM. Assert: read sees the write. No stale reads after LWT. | `lwt_read_your_writes` |

### Sprint T3: Pagination + Built-in Functions (Weeks 9-10)

Temporal paginates over history nodes and task queues.

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| T3.1 | Implement result set pagination: `page_size` in QUERY frame, `paging_state` in RESULT frame | L | Query with page_size=100 on a table with 500 rows returns 100 rows + a paging_state token. Second query with same paging_state returns next 100. After 5 pages, paging_state is empty. | `pagination_basic`, `pagination_multi_page`, `pagination_end` |
| T3.2 | Pagination state encoding: opaque token that encodes the last-seen clustering key position | M | Paging state is a serialized cursor (partition key + clustering key position). Deserializable across different nodes (for driver-level routing). | `pagination_state_roundtrip`, `pagination_cross_node` |
| T3.3 | Implement `now()` CQL function: returns a Timeuuid (type 1 UUID) | M | `SELECT now() FROM system.local` returns a valid Timeuuid. `INSERT INTO t (id, ts) VALUES (1, now())` stores a Timeuuid. | `cql_function_now` |
| T3.4 | Implement `toTimestamp(timeuuid)` CQL function: converts Timeuuid to timestamp | S | `SELECT toTimestamp(now()) FROM system.local` returns a timestamp. | `cql_function_to_timestamp` |
| T3.5 | Implement `TTL(column)` CQL function: returns remaining TTL of a cell | M | `INSERT INTO t (id, v) VALUES (1, 'a') USING TTL 3600`. `SELECT TTL(v) FROM t WHERE id = 1` returns ~3600. | `cql_function_ttl` |
| T3.6 | Verify clustering ORDER BY DESC works for Temporal's history_node table | S | `CREATE TABLE ... WITH CLUSTERING ORDER BY (txn_id DESC)`. SELECT returns rows in descending txn_id order. | `clustering_order_desc` |
| T3.7 | Run Temporal schema DDL against ferrosa: all 14 tables, 2 indexes, 1 UDT created successfully | L | `temporal-sql-tool create -plugin cassandra` completes without errors. All tables visible in system_schema. | `temporal_schema_ddl` |
| T3.8 | Run Temporal Go integration tests against ferrosa | XL | Temporal's persistence test suite (shard store, execution store, history store, metadata store) passes. Identify remaining gaps. | `temporal_integration_suite` |

---

## Stream 4: Accord Transactions (Weeks 11-24)

**Goal:** Strict-serializable ACID transactions across partitions.

**Gate:** Jepsen full-nemesis suite passes.

Accord is the long-pole. It replaces the single-partition CAS (from Stream 3) with a
leaderless protocol that also handles multi-partition transactions. The single-partition
LWT from Stream 3 is NOT throwaway — it becomes the "fast path" in Accord for single-partition
CAS operations.

### Transition Path: LWT → Accord

| Feature | Stream 3 (LWT) | Stream 4 (Accord) |
|---------|----------------|-------------------|
| Single-partition IF NOT EXISTS | Per-partition lock/Paxos-lite | Accord fast path (leaseholder, 1 RTT) |
| Single-partition IF col = ? | Per-partition lock/Paxos-lite | Accord fast path |
| Batch CAS (same partition) | Per-partition lock | Accord single-shard |
| Multi-partition transactions | NOT SUPPORTED | Accord multi-shard (2 RTTs) |
| Serializable reads | LOCAL_SERIAL as CL | Accord linearizable reads via dep-check |

Stream 3's `T1.7` (per-partition serialization) is the component that gets replaced by
Accord's AccordStateMachine. The parser work (IF conditions), result format, and pagination
are permanent.

### Accord Sprint Plan (from specs/accord-project-plan.md)

| Sprint | Weeks | Focus | Gate |
|--------|-------|-------|------|
| S1 | 11-12 | Core types, ConflictIndex, protocol log, 24-step test | Unit tests pass |
| S2 | 13-14 | ReorderBuffer, heartbeat, recovery foundation | Phase 0 gate |
| S3 | 15-16 | AccordStateMachine, coordinator, dep-wait, DDL drain | State machine tests pass |
| S4 | 17-18 | MemIndex, sidecar files, Jepsen register, perf baseline | Phase 1 gate |
| S5 | 19-20 | BEGIN/COMMIT/ROLLBACK, cross-shard, Jepsen bank | Phase 2 gate |
| S6 | 21-22 | Transactional 2i, READ_2I algorithm | Phase 3 gate |
| S7 | 23-24 | Electorate reconfiguration, full Jepsen + chaos | Phase 4 gate |

See `specs/accord-project-plan.md` for the full 56-task breakdown with 302 specified tests
across 7 test spec documents.

---

## Stream 5: Missing Project Plan Tasks (Cross-Cutting)

These items were identified during the test spec audit as having tests but no sprint task.
They need to be scheduled.

| # | Task | Size | Target Sprint | Rationale |
|---|------|------|--------------|-----------|
| X1 | Build `TestCluster` deterministic message harness | L | S1 (Accord) | Prerequisite for 24-step test and all Layer 4 scenario tests |
| X2 | Build Jepsen infrastructure (cluster provisioning, Go/Clojure client, nemesis ops) | XL | T3 or S3 | Prerequisite for S4.5 (Jepsen register) and T3.8 (Temporal integration) |
| X3 | Implement ExclusiveSyncPoint / DurabilityService | L | S4 (Accord) | Required for sidecar file GC, protocol log GC, ConflictIndex cleanup |
| X4 | Implement Accord observability metrics (9 Prometheus gauges/counters) | M | S3 (Accord) | Required by threat model for alarm thresholds |
| X5 | Build performance benchmark harness with CI regression detection | M | T3 or S4 | Required for S4.6 (perf baseline) and S7.9 (perf regression suite) |

---

## Unified Timeline

```
Week  1-2:  M1 — ferrosa-memory integration validation
Week  3-4:  D1 — ferrosa-dbaas control plane validation
Week  5-6:  T1 — LWT: IF NOT EXISTS, IF conditions, single-partition CAS
Week  7-8:  T2 — Batch CAS, SERIAL/LOCAL_SERIAL consistency
Week  9-10: T3 — Pagination, built-in functions, Temporal schema + integration
Week 11-12: S1 — Accord foundation: types, ConflictIndex, 24-step test
Week 13-14: S2 — Accord: ReorderBuffer, heartbeat, recovery
Week 15-16: S3 — Accord: state machine, coordinator, dep-wait
Week 17-18: S4 — Accord: MemIndex, sidecar, Jepsen register
Week 19-20: S5 — Accord: multi-key transactions, Jepsen bank
Week 21-22: S6 — Accord: transactional 2i
Week 23-24: S7 — Accord: electorate, full Jepsen + chaos
```

**Total: 24 weeks (12 sprints)**

- Weeks 1-4: Integration validation (ferrosa-memory + ferrosa-dbaas)
- Weeks 5-10: Temporal CQL compatibility (LWT, batch CAS, pagination)
- Weeks 11-24: Accord distributed transactions (full protocol)

## Phase Gates

| Phase | Week | Gate | Verification |
|-------|------|------|-------------|
| Memory | 2 | ferrosa-memory 12 MCP tools work | `cargo test` in ferrosa-memory |
| DBaaS | 4 | Control plane CRUD + metering + billing work | `cargo test` in ferrosa-dbaas |
| Temporal CQL | 6 | LWT IF NOT EXISTS + IF conditions pass | Unit + property tests |
| Temporal CQL | 8 | Batch CAS + serial consistency pass | Batch CAS property tests |
| Temporal CQL | 10 | Temporal schema DDL + Go integration pass | `temporal-sql-tool create` + Go test suite |
| Accord Phase 0 | 14 | 24-step EPaxos test + unit tests | CI gate |
| Accord Phase 1 | 18 | Jepsen register + P50 within 15% | Jepsen + benchmark |
| Accord Phase 2 | 20 | Jepsen bank + write-skew | Jepsen suite |
| Accord Phase 3 | 22 | 2i correctness + dep-wait P99 < 5ms | Integration + benchmark |
| Accord Phase 4 | 24 | Jepsen full nemesis + chaos | Full Jepsen + chaos suite |

## Test Count by Stream

| Stream | Tests | Source |
|--------|-------|--------|
| ferrosa-memory (M1) | ~12 | Integration smoke |
| ferrosa-dbaas (D1) | ~10 | Integration + multi-node |
| Temporal CQL (T1-T3) | ~30 | LWT, batch CAS, pagination, functions |
| Accord (S1-S7) | ~302 | 7 test spec documents |
| **Total** | **~354** | |

## Critical Path

```
ferrosa-memory (M1) ─┐
                     ├─→ LWT Foundation (T1) ─→ Batch CAS (T2) ─→ Pagination (T3) ─→ Temporal runs
ferrosa-dbaas (D1) ──┘                                                    │
                                                                           ↓
                                          Accord S1 ─→ S2 ─→ S3 ─→ S4 ─→ S5 ─→ S6 ─→ S7
                                          (can start parallel with T2 if team capacity allows)
```

M1 and D1 are parallel. T1-T3 are sequential. Accord S1 can start as early as week 7
if a second developer is available, since Accord foundation types (S1.1-S1.4) don't depend
on LWT implementation. The critical path is: T1 → T2 → T3 → Temporal runs (week 10).
Accord is the long pole but not on the critical path for Temporal compatibility.
