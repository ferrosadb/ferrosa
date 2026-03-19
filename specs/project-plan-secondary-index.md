# Project Plan: Secondary Index Pipeline

> Last updated: 2026-03-18

## Sprint Overview

| Sprint | Focus | Size | Risk Items Addressed |
|--------|-------|------|---------------------|
| 1 | Core infrastructure: MemtableIndex + sidecar write + read_by_index | L | F1, F2, F3, F11 (OOM cap), T2, T5 |
| 2 | Query planner + EXPLAIN + route_select integration | M | F5, F6, F7, F8 |
| 3 | Flush + compaction integration + crash recovery | M | F4, F9, F10, T1, T5 |
| 4 | Multi-index intersection + tests + benchmarks | M | F7, F5, TC1-TC10 |
| Backlog | Cost-based planner, vector index integration, S3 sidecar upload | — | Future |

---

## Sprint 1: Core Infrastructure

**Goal**: A single secondary index accelerates a point lookup query end-to-end.

| # | Task | Size | Tests | Source |
|---|------|------|-------|--------|
| 1.1 | Implement persistent red-black tree (`MemtableIndex`) with ArcSwap | M | Unit: insert/lookup/range, concurrent read/write (TC1) | Architect, F1 |
| 1.2 | Add `MemtableIndex` to `TableStore` — create per declared index, insert on write | S | Integration: write row, lookup via memtable index | Architect, F8 |
| 1.3 | Implement sidecar file format: header (magic/version/count/CRC) + sorted entries | M | Unit: write/read roundtrip, corrupt byte detection (TC3) | Architect, T1 |
| 1.4 | Wire `IndexBuilder`/`IndexReader` to sidecar format via BTreeIndexFactory | S | Unit: build from entries, open reader, lookup | Architect |
| 1.5 | Add `StorageEngine::read_by_index()` — merge memtable index + sidecar readers | M | Integration: write, flush, query via index (TC4) | Architect, T5 |
| 1.6 | Add result cap (10K RowPositions) with error message (M4) | S | Test: 100K rows with boolean index, verify cap (TC10) | FMEA F11, Threat T2 |
| 1.7 | Handle null indexed column values gracefully on write | S | Test: INSERT with null, write succeeds (TC7) | FMEA F8 |

**Success criteria**: `INSERT INTO t (pk, val) VALUES (1, 'x'); SELECT * FROM t WHERE val = 'x';` uses the index (verified by log or future EXPLAIN).

---

## Sprint 2: Query Planner + Integration

**Goal**: The CQL SELECT path uses indexes automatically when available. EXPLAIN shows the plan.

| # | Task | Size | Tests | Source |
|---|------|------|-------|--------|
| 2.1 | Create `ScanPlan` enum and `plan()` function in `ferrosa-cql/src/planner.rs` | M | Unit: plan returns correct variant for PK/index/fullscan cases | Architect |
| 2.2 | Wire planner into `route_select` — replace the TODO at line 801 | M | Integration: indexed query skips full scan | Architect |
| 2.3 | Parse EXPLAIN statement, route to planner, return plan as text result | S | Test: `EXPLAIN SELECT ... WHERE indexed_col = 'x'` returns "SingleIndex" | Architect |
| 2.4 | Handle index not covering all WHERE clauses — SingleIndex + post-filter | S | Test: WHERE indexed_col = 'x' AND non_indexed = 'y' | FMEA F6 |
| 2.5 | Keyspace/table scoping: planner resolves indexes by (ks, table, column) | S | Test: same column name, different tables, correct index used (TC6 variant) | FMEA F6 |

**Success criteria**: `EXPLAIN SELECT * FROM t WHERE val = 'x'` returns `SingleIndex(idx_val)`. Query returns correct results.

---

## Sprint 3: Flush, Compaction, Crash Recovery

**Goal**: Indexes persist across restarts and compaction. No data loss on crash.

| # | Task | Size | Tests | Source |
|---|------|------|-------|--------|
| 3.1 | Serialize MemtableIndex to sidecar file during `TableStore::flush()` | M | Integration: flush, restart, query via sidecar index | Architect |
| 3.2 | On startup, detect SSTables without sidecar → rebuild from SSTable data | M | Test: delete sidecar, restart, verify index rebuilt (TC4) | Threat T5, FMEA F4 |
| 3.3 | Merge sidecar indexes during compaction (merge-sort input sidecars) | M | Test: compact 3 SSTables, verify merged sidecar correct | FMEA F9 |
| 3.4 | Tombstone-aware merge: skip entries for deleted rows | S | Test: insert, delete, compact, query — deleted rows absent (TC8) | FMEA F9 |
| 3.5 | Benchmark flush latency with 1-5 indexes on 100K-row memtable | S | TC9: verify < 2x non-indexed flush | FMEA F10 |

**Success criteria**: Write 10K rows, flush, compact, kill process, restart — indexed query returns all 10K rows.

---

## Sprint 4: Multi-Index Intersection + Hardening

**Goal**: Queries with multiple indexed WHERE clauses use all matching indexes. Full test coverage.

| # | Task | Size | Tests | Source |
|---|------|------|-------|--------|
| 4.1 | Implement `IndexIntersection` execution: collect RowPositions from each index, intersect, fetch | M | Test: two indexes, query matching both, correct intersection (TC6) | Architect |
| 4.2 | Planner returns `IndexIntersection` when 2+ WHERE columns match indexes | S | Unit: plan with 2 indexed columns | Architect |
| 4.3 | Post-fetch validation: verify fetched row matches indexed column value (M2) | S | Test: simulate corrupt sidecar, verify wrong rows filtered out | Threat T1 |
| 4.4 | CRC32 validation on sidecar open (M1) | S | TC3: corrupt byte in sidecar header | Threat T1 |
| 4.5 | Concurrent writer/reader stress test for MemtableIndex | S | TC1: 10 writers + 10 readers, no missing keys | FMEA F1 |
| 4.6 | Property test: roundtrip (write → flush → read_by_index) for all CQL types | M | Proptest with Int/Text/UUID/Timestamp indexed columns | General |
| 4.7 | End-to-end example test: create index, insert, query, verify via EXPLAIN | S | CQL script in examples/ | General |

**Success criteria**: `WHERE a = 1 AND b = 'x'` with indexes on both columns uses `IndexIntersection`. All TC1-TC10 pass.

---

## Backlog

| Task | Size | Notes |
|------|------|-------|
| Cost-based planner: cardinality estimates in sidecar headers | L | v2 — compare index scan cost vs full scan |
| S3 sidecar upload alongside SSTable upload | M | Required for distributed index reads |
| Vector index integration into secondary index pipeline | L | Separate trait hierarchy, needs adapter |
| Max indexes per table limit (M7) | S | Configuration + enforcement |
| Async sidecar build (decouple from flush critical path) | M | If F10 benchmark shows flush latency too high |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation | Sprint |
|------|-----------|--------|------------|--------|
| Low-selectivity index OOM (T2) | High | High | Result cap at 10K (M4) | Sprint 1 |
| Sidecar corruption (T1) | Medium | High | CRC32 + post-fetch validation | Sprint 4 |
| Stale index after crash (T5) | Medium | High | Startup rebuild + fallback to scan | Sprint 3 |
| Write amplification (T4) | Medium | Medium | Index count limit + metrics | Backlog |
| Flush latency with many indexes (F10) | Medium | Medium | Benchmark, async if needed | Sprint 3 |
