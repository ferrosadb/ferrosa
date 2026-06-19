# Indexing Subsystem Coverage Review

> Zone: `ferrosa-index/`, `ferrosa-index-builder/`, `ferrosa-storage/src/index/`
> Date: 2026-04-18
> Reviewer: automated coverage agent

---

## 1. Feature Inventory

### 1.1 ferrosa-index — Index Types

| Index Type | Subtype / Variant | Key File | Lines |
|---|---|---|---|
| BTree | — | `ferrosa-index/src/btree.rs` | 466 |
| Hash | — | `ferrosa-index/src/hash.rs` | 344 |
| Composite | Multi-column, prefix scan | `ferrosa-index/src/composite.rs` | 737 |
| Filtered | Predicate-gated wrapper | `ferrosa-index/src/filtered.rs` | 444 |
| Phonetic / Soundex | — | `ferrosa-index/src/phonetic/soundex.rs` | 135 |
| Phonetic / Metaphone | — | `ferrosa-index/src/phonetic/metaphone.rs` | 260 |
| Phonetic / DoubleMetaphone | — | `ferrosa-index/src/phonetic/double_metaphone.rs` | 290 |
| Phonetic / Caverphone | — | `ferrosa-index/src/phonetic/caverphone.rs` | 172 |
| Phonetic dispatcher | Algorithm enum + factory | `ferrosa-index/src/phonetic/mod.rs` | 431 |
| Vector / HNSW | L2, cosine, inner-product | `ferrosa-index/src/vector/hnsw.rs` | 620 |
| Vector / IVFFlat | k-means, multi-probe | `ferrosa-index/src/vector/ivfflat.rs` | 504 |
| Vector distance utils | L2, cosine, inner-product | `ferrosa-index/src/vector/mod.rs` | 260 |
| FullText / Analyzer | Standard, Simple, Keyword | `ferrosa-index/src/fulltext/analyzer.rs` | 231 |
| FullText / Stemmer | Porter stemmer | `ferrosa-index/src/fulltext/stemmer.rs` | 395 |
| FullText / Builder | Inverted index builder + FTI serializer | `ferrosa-index/src/fulltext/builder.rs` | 265 |
| FullText / Reader | BM25 search, query eval | `ferrosa-index/src/fulltext/reader.rs` | 511 |
| FullText / Query | AND/OR/NOT/Prefix/Phrase parser | `ferrosa-index/src/fulltext/query.rs` | 354 |
| FullText / Merge | Compaction FTI merge | `ferrosa-index/src/fulltext/merge.rs` | 223 |
| FullText / Scoring | BM25 scoring engine | `ferrosa-index/src/fulltext/scoring.rs` | 134 |
| Core traits & types | `IndexBuilder`, `IndexReader`, `IndexFactory`, `IndexCapabilities` | `ferrosa-index/src/lib.rs` | 324 |

**Total distinct index types: 10** (BTree, Hash, Composite, Filtered, Phonetic×4, Vector×2, FullText).

### 1.2 ferrosa-index-builder — Remote Build Binary

| Component | File | Notes |
|---|---|---|
| HTTP server (push mode) | `ferrosa-index-builder/src/server.rs` | `POST /internal/index/build`, `GET /health` |
| Worker pool | `ferrosa-index-builder/src/worker.rs` | Bounded OS-thread pool wrapping `LocalBackend` |
| Pull-mode manifest watcher | `ferrosa-index-builder/src/pull.rs` | Polls `GET /internal/manifest`, checks S3, enqueues missing sidecars |
| CLI entry point | `ferrosa-index-builder/src/main.rs` | Clap CLI, push vs. pull mode selection, object store init |

### 1.3 ferrosa-storage/src/index/ — Engine Coordination

| Component | File | Lines | Notes |
|---|---|---|---|
| `IndexBuildScheduler` + `LocalBackend` | `scheduler.rs` | 1163 | OS-thread worker loop, `IndexBuildJob`, `BuildPriority` |
| `RemoteBackend` + `CircuitBreaker` | `remote_backend.rs` | 487 | ureq blocking HTTP, per-endpoint circuit breaker, local fallback |
| `IndexBackendConfig` / `FERROSA_INDEX_BACKEND` | `remote_backend.rs:53` | — | `from_env()` reads `FERROSA_INDEX_BACKEND={local,remote,off}` |
| `SidecarWriter` / `SidecarReader` | `sidecar.rs` | 626 | FXSI format, CRC32, binary-search lookup |
| `IndexStateTracker` | `tracker.rs` | 432 | Per-index status state machine (Pending→Indexed/Failed) |
| `SecondaryIndexesVirtualTable` | `virtual_table.rs` | 300 | Observability via system virtual table |
| `EagerIndexBuilder` | `ferrosa-storage/src/memtable/eager_index.rs` | 291 | Flush/compaction hooks submitting to scheduler |

### 1.4 Memtable-Level Index (fresh writes)

| Component | File | Lines | Notes |
|---|---|---|---|
| `MemtableIndex` | `memtable/index.rs` | 547 | Persistent red-black tree via `ArcSwap`, lock-free |
| `MemIndex` | `memtable/mem_index.rs` | 586 | BTreeMap-backed, timestamp-aware, range scan |
| `VectorMemtableIndex` | `memtable/vector_index.rs` | 271 | In-memory HNSW for flush-time ANN |

### 1.5 Build Modes (`FERROSA_INDEX_BACKEND`)

| Mode | Engine behavior | Builder required | Env var value |
|---|---|---|---|
| `local` (default) | `LocalBackend`, 2 worker threads | No | `FERROSA_INDEX_BACKEND=local` |
| `remote` | `RemoteBackend` HTTP push, circuit-breaker, local fallback | Yes (push mode) | `FERROSA_INDEX_BACKEND=remote` |
| `off` | No scheduler, no worker threads, flush hooks no-op | Yes (pull mode) | `FERROSA_INDEX_BACKEND=off` |

---

## 2. Spec Coverage Matrix

| Feature | Spec Document | Status in Spec |
|---|---|---|
| Secondary index pipeline (memtable, sidecar, query planner, compaction) | `specs/secondary-index-pipeline.md` | Draft, implemented |
| FullText index (FTI format, analyzer, BM25, CQL `fts_match`) | `specs/fulltext-index-architecture.md` | Implemented |
| Remote index build backend (`FERROSA_INDEX_BACKEND`, `RemoteBackend`, pull mode) | `specs/remote-index-build-backend.md` | Draft, core implemented |
| Secondary index FMEA (F1–F12, all mitigated) | `specs/archive/analysis/fmea-secondary-index.md` | All 12 implemented |
| Secondary index threat model | `specs/archive/analysis/threat-model-secondary-index.md` | Archived |
| Phonetic algorithms | No dedicated spec | Implemented only |
| Vector HNSW / IVFFlat sidecar persistence | No spec | Not implemented |
| Vector ANN (`ORDER BY ... ANN OF`) CQL execution | No spec | Deferred (logged, returns unordered) |
| `GET /internal/manifest` engine endpoint (required by pull mode) | Mentioned in `remote-index-build-backend.md` | Not implemented |
| Index backfill for pre-existing SSTables on `add_index` | `secondary-index-pipeline.md` mentions | Partially (tracker registers; no backfill build) |
| Query planner (`ScanPlan`, `EXPLAIN`) | `secondary-index-pipeline.md` | Not implemented |
| Multi-index intersection | `secondary-index-pipeline.md` | Not implemented |
| `LanguageAnalyzer` (non-English FTS) | `fulltext-index-architecture.md` mentions | Not implemented (only Standard/Simple/Keyword) |
| Positional phrase queries | `fulltext-index-architecture.md` | Approximated (co-occurrence, not position-aware) |
| FTS observability metrics | `fulltext-index-architecture.md` | No metrics instrumented |
| Builder observability metrics | `remote-index-build-backend.md` | Defined in spec, not confirmed wired |

---

## 3. Test Coverage

### 3.1 ferrosa-index unit tests

| File | Test count | Coverage |
|---|---|---|
| `btree.rs` | 11 | Insert, lookup, range scan, empty index, corruption |
| `hash.rs` | 8 | Insert, lookup, missing key, serialization |
| `composite.rs` | 11 | Multi-column encode/decode, prefix scan, range |
| `filtered.rs` | 6 | Predicate evaluation, pass-through |
| `phonetic/mod.rs` | 4 | Factory, serialization |
| `phonetic/soundex.rs` | 7 | Encoding correctness |
| `phonetic/metaphone.rs` | 8 | Encoding correctness |
| `phonetic/double_metaphone.rs` | 8 | Primary/alternate code |
| `phonetic/caverphone.rs` | 6 | Encoding correctness |
| `vector/mod.rs` | 14 | L2/cosine/inner-product distances |
| `vector/hnsw.rs` | 6 | Build, nearest, unsupported ops, dimension mismatch |
| `vector/ivfflat.rs` | 8 | Build, nearest, multi-probe cross-cluster, empty |
| `fulltext/analyzer.rs` | 9 | Lowercase, stop words, stemming, language |
| `fulltext/scoring.rs` | 6 | BM25 formula correctness |
| `fulltext/query.rs` | 12 | AND/OR/NOT/Prefix/Phrase/precedence parsing |
| `fulltext/stemmer.rs` | 3 | Porter steps |
| `fulltext/builder.rs` | 3 | Build + serialize |
| `fulltext/reader.rs` | 11 | Lookup, search, BM25 ranking, wildcard expansion |
| `fulltext/merge.rs` | 5 | Two-index merge, tombstone handling |
| `lib.rs` | 2 | IndexCapabilities bitflag ops |
| **Total** | **148** | — |

### 3.2 ferrosa-storage/src/index/ unit tests

| File | Test count |
|---|---|
| `sidecar.rs` | 12 |
| `tracker.rs` | 7 |
| `virtual_table.rs` | 5 |
| `scheduler.rs` | 17 |
| `remote_backend.rs` | 7 |
| **Total** | **48** |

### 3.3 Memtable index unit tests

| File | Test count |
|---|---|
| `memtable/index.rs` | 7 + 2 concurrency |
| `memtable/mem_index.rs` | 13 |
| `memtable/vector_index.rs` | 9 |
| `memtable/eager_index.rs` | 3 |
| **Total** | **34** |

### 3.4 ferrosa-index-builder unit tests

| File | Test count | Notes |
|---|---|---|
| `worker.rs` | 2 | Parse helpers (`parse_index_type`, `parse_priority`) |
| `server.rs` | 0 | Handler setup only, no HTTP tests |
| `pull.rs` | 0 | No tests |
| **Total** | **2** | Extremely sparse |

### 3.5 Integration tests

| Test | File | Coverage |
|---|---|---|
| `flush_produces_sidecar_index_files_via_scheduler` | `ferrosa-storage/tests/compaction_index_rebuild.rs` | Flush → sidecar written → file exists |
| `add_index_registers_in_tracker` | same | `add_index` registers in tracker, no backfill |
| `fts_sidecar_created_on_flush` | `ferrosa-storage/src/engine.rs:9497` | FTI sidecar exists after flush, readable |
| `fts_end_to_end_insert_query` | `ferrosa-storage/src/engine.rs:9545` | Write → flush → `fulltext_search` returns hits |

**No integration test** covers: `RemoteBackend` end-to-end, pull-mode, circuit-breaker state transitions under real HTTP failure, vector sidecar round-trip.

---

## 4. Gaps

### P0 — Correctness / Data Loss Risk

**G1. Vector index has no sidecar persistence.**
`VectorMemtableIndex` has a `drain()` method designed for flush, but the storage flush path (`store.rs`) never calls it. Vector data exists only in-memory and is lost on flush. HNSW and IVFFlat build-and-serialize are implemented in `ferrosa-index` but never triggered from the write path. Result: `ORDER BY ... ANN OF` queries silently fall back to unordered results with a log warning, missing the memtable data entirely.

**G2. `GET /internal/manifest` endpoint does not exist.**
`ferrosa-index-builder`'s pull mode polls `GET /internal/manifest` (implemented in `pull.rs`). The engine has no such endpoint. The `backend=off` deployment model described in `specs/remote-index-build-backend.md` is broken end-to-end: pull mode will 404 on every poll cycle.

### P1 — Functional Completeness

**G3. No query planner (`ScanPlan`) or `EXPLAIN` statement.**
`specs/secondary-index-pipeline.md` specifies `ScanPlan` with `SingleIndex`, `IndexIntersection`, `PkLookup`, `FullScan` variants and an `EXPLAIN` statement. None of this exists. Secondary index queries other than BTree point-lookups and `fts_match` are unreachable through the CQL path without `ALLOW FILTERING`, defeating the purpose of the index pipeline.

**G4. Index backfill is a stub.**
`add_index_registers_in_tracker` integration test confirms that `engine.add_index()` registers in the tracker but the FMEA item F4 mitigation ("Startup rebuilds missing sidecars") is noted as implemented for the crash-recovery path only. Adding an index to a table that already has flushed SSTables leaves those SSTables without sidecars indefinitely. The scheduler is not given backfill jobs for pre-existing generation files.

**G5. `ferrosa-index-builder` has near-zero test coverage.**
The push-mode HTTP handler has zero tests. The pull-mode loop has zero tests. Only two trivial string-parse helpers are tested in `worker.rs`. The `server.rs` integration scenario (POST → worker → S3 write → response) described in the spec's test strategy has no implementation.

### P2 — Spec Debt / Observability

**G6. FTS observability metrics not instrumented.**
`fulltext-index-architecture.md` defines four metrics (`ferrosa_index_fulltext_terms`, `_queries_total`, `_query_duration_seconds`, `_build_duration_seconds`). None appear in the codebase. The `SecondaryIndexesVirtualTable` only tracks sidecar build state, not FTS query throughput or quality.

**G7. Phrase queries are positional-index approximation only.**
The FTI spec promises positional phrase search ("exact phrase"). The reader approximates it by requiring all words co-occur in a document, ignoring term order. This is documented in comments but not reflected in the spec status. A query for `"S3 backed"` will match a document containing "backed S3" in the wrong order.

---

## 5. Recommendations

**R1 (P0 — G1). Wire `VectorMemtableIndex::drain()` into the flush path.**
In `ferrosa-storage/src/store.rs`, after the SSTable flush, iterate `vector_indexes` (analogous to how `fulltext_indexes` is iterated at `store.rs:589`), serialize via `HnswFactory` or `IvfFlatFactory`, and write a vector sidecar. This unblocks the ANN execution path. Spec the sidecar format (suggest reusing `SidecarWriter` with raw f32 blob entries) before implementing.

**R2 (P0 — G2). Implement `GET /internal/manifest` on the engine's internal HTTP server.**
The pull-mode deployment model is blocked. The endpoint should return a JSON array of `ManifestEntry` (sstable_id, table_id, keyspace, table, indexes). Wire it into the existing internal HTTP server (wherever `ferrosa-ctl` or health endpoints live). Add a test that verifies pull-mode deduplicates already-built sidecars.

**R3 (P1 — G3). Implement the `ScanPlan` query planner.**
The secondary index pipeline spec is complete. Priority order: `PkLookup` (already works), `SingleIndex` via `read_by_index` (required for non-FTS indexes to be useful), `ALLOW FILTERING` error for `FullScan` on tables with declared indexes, `IndexIntersection` last. `EXPLAIN` can follow once `SingleIndex` is working. Without this, BTree/Hash/Composite/Phonetic indexes build but are never consulted by the CQL layer.

**R4 (P1 — G5). Add HTTP integration tests to `ferrosa-index-builder`.**
Use `axum::Server` in a test with `reqwest` or `ureq` to verify: (a) successful build request returns `status: completed`, (b) bad index type returns `status: failed`, (c) `GET /health` reflects active worker counts. Use an in-memory `ObjectStore` implementation. These tests should run without `FERROSA_TEST_CONTAINERS`.

**R5 (P2 — G6). Instrument FTS query metrics.**
Add `tracing` spans or Prometheus counters in `StorageEngine::fulltext_search()` for query count and duration, and in the flush path for FTI build duration. These are the four metrics committed to in the spec and are needed for production observability of the FTS subsystem.

---

## Summary

**Index types implemented: 10** (BTree, Hash, Composite, Filtered, Phonetic×4, Vector×2 as HNSW+IVFFlat, FullText).
Unit tests: 148 in `ferrosa-index`, 48 in `ferrosa-storage/src/index/`, 34 in memtable index modules, 2 in `ferrosa-index-builder`.
Integration tests: 4 (flush→sidecar, FTI round-trip). No remote/pull/vector sidecar integration tests.

Top 3 gaps by severity:
1. **G1 (P0)**: Vector index data is silently dropped on flush — sidecar persistence path is not wired.
2. **G2 (P0)**: `GET /internal/manifest` does not exist, rendering `backend=off` pull-mode non-functional.
3. **G3 (P1)**: No query planner means BTree/Hash/Composite/Phonetic indexes are built but never used by the CQL engine.
