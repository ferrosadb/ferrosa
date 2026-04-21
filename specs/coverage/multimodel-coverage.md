# Multi-Model Query Layer — Coverage Review

> Date: 2026-04-18
> Scope: `ferrosa-graph/`, `ferrosa-sparql/`, `ferrosa-udf/`
> Reviewer: automated audit via forge MCP + source read

---

## 1. Feature Inventory

### ferrosa-graph (26 features identified)

| # | Feature | Module |
|---|---------|--------|
| 1 | Cypher lexer | `parser/lexer.rs` |
| 2 | Recursive descent parser | `parser/parse_impl.rs` |
| 3 | AST (Statement, Pattern, Expr, ReturnClause) | `parser/ast.rs` |
| 4 | MATCH planning (anchor + hop expansion) | `planner/physical.rs` |
| 5 | CREATE planning and execution | `planner/physical.rs`, `executor/expand.rs` |
| 6 | SET planning and execution | `planner/physical.rs`, `executor/expand.rs` |
| 7 | DELETE planning and execution | `planner/physical.rs`, `executor/expand.rs` |
| 8 | SUBSCRIBE / UNSUBSCRIBE streaming (SSE) | `executor/subscribe.rs`, `http.rs` |
| 9 | Aggregation framework (count, sum, avg, min, max, collect) | `executor/aggregate.rs` |
| 10 | Variable-length paths `[*1..N]` with BFS | `executor/varpath.rs` |
| 11 | Hop property filtering | `executor/expand.rs` |
| 12 | Expression evaluator (WHERE, RETURN) | `executor/eval.rs` |
| 13 | Built-in scalar functions (id, type, toString, toInteger, toFloat, coalesce, size, keys, abs) | `executor/eval.rs` |
| 14 | Leapfrog triejoin (WCO joins) | `executor/leapfrog.rs` |
| 15 | Sort (ORDER BY) | `executor/expand.rs` |
| 16 | Adjacency index observer (OUT + IN entries per edge write) | `adjacency/observer.rs` |
| 17 | Adjacency index schema | `adjacency/schema.rs` |
| 18 | Background reconciliation task | `adjacency/reconcile.rs` |
| 19 | Logical planner — label resolution + per-hop auth (T3) | `planner/logical.rs` |
| 20 | Bolt v5 wire protocol (PackStream codec, chunked framing, version negotiation) | `bolt/` |
| 21 | Bolt TCP server (RUN, PULL, BEGIN, COMMIT, ROLLBACK) | `bolt/server.rs` |
| 22 | Graph HTTP endpoint on port 7474 (POST /graph/query, /graph/explain, /graph/schema, /graph/health) | `http.rs` |
| 23 | Basic auth + TLS enforcement | `http.rs` |
| 24 | Schema-typed edge table discovery (keyspaces with `graph.*` extensions) | `engine.rs`, `adjacency/reconcile.rs` |
| 25 | GraphEngine composition root (execute, explain, schema, subscribe, shutdown) | `engine.rs` |
| 26 | Error sanitization and CatchPanicLayer | `http.rs` |

**Notable absence:** MERGE statement is not present in the AST (`parser/ast.rs` has no `Merge` variant) and not in `parse_impl.rs`. The spec and `overview.md` list MATCH/CREATE/MERGE/DELETE, but MERGE is unimplemented.

**Notable absence:** Schema-typed edge tables (`typed_edges`, `folded_into`, `mentioned_in`, `co_occurs_with`, `supersedes`, `derived_edges_by_pred`, `derived_edges_by_src`) are defined and written by `ferrosa-memory`, but there is no code in `ferrosa-graph/src/` that references or validates these table names. The graph engine treats any table with `graph.*` schema extensions as an edge table; the named semantic edge types are a ferrosa-memory convention, not a graph-engine invariant.

---

### ferrosa-sparql (18 features identified)

| # | Feature | Module |
|---|---------|--------|
| 1 | SPARQL 1.2 / RDF* parser via spargebra | `engine.rs` |
| 2 | SPARQL algebra → QueryPlan (SELECT, ASK) | `planner.rs` |
| 3 | SubjectLookup, PredicateScan, ObjectScan, FullScan, PropertyPath ops | `planner.rs` |
| 4 | UNION support (planner) | `planner.rs` |
| 5 | Nested-loop binding-set join executor | `executor.rs` |
| 6 | ORDER BY | `executor.rs` |
| 7 | DISTINCT | `executor.rs` |
| 8 | LIMIT / OFFSET (with bounds guard) | `executor.rs` |
| 9 | FILTER expression evaluator (Equal, Greater, Less, And, Or, Not, Bound, arithmetic) | `filter.rs` |
| 10 | Property path BFS with cycle detection (`+`, `*`, `?`) | `property_path.rs` |
| 11 | RDF triple ↔ CQL row translation, `rdf_triples` table schema | `triple_store.rs` |
| 12 | SPARQL UPDATE INSERT DATA / DELETE DATA | `update.rs` |
| 13 | RDF* annotation type + `evaluate_rdf_star_pattern` stub | `rdf_star.rs` |
| 14 | SPARQL JSON Results format | `results.rs` |
| 15 | N-Triples format | `results.rs` |
| 16 | Turtle format (stub — outputs N-Triples body) | `results.rs` |
| 17 | Content negotiation (Accept header) | `results.rs`, `http.rs` |
| 18 | HTTP endpoint: POST/GET `/sparql`, POST `/sparql/update`, GET `/sparql/health` on port 8080 | `http.rs` |

**BUG-S1–S20 audit status:** The `engine.rs` and `executor.rs` source contain fix markers for BUG-S1, S2, S3, S10, S11, S12, S13, S17, S18. These were resolved on the `feat/sparql-endpoint` branch and are in the current codebase. BUG-S4 (binding type join validation), BUG-S6 (ASK format), BUG-S8 (reverse edge index), BUG-S9 (predicate scan filtering), BUG-S14 (full property path coverage), BUG-S15 (RDF* execution — only a stub), BUG-S16 (SPARQL UPDATE complete) were addressed: UPDATE is implemented; property paths are implemented for `+`/`*`/`?`; RDF* `evaluate_rdf_star_pattern` exists but is a stub that returns empty annotations. The Turtle serializer outputs N-Triples body content (not real Turtle), which is a silent format violation.

**No integration tests exist** under `ferrosa-sparql/tests/`. All 77 tests cited in `components.md` are inline `mod tests` within source files.

---

### ferrosa-udf (9 features identified)

| # | Feature | Module |
|---|---------|--------|
| 1 | WIT contract (`cql-value` type + `invoke` export) | `wit/ferrosa-udf.wit` |
| 2 | Wasmtime Component Model compilation and caching (moka, 256-slot) | `executor.rs` |
| 3 | Instance pool (max 8 per function, acquire/release/drain) | `executor.rs` |
| 4 | Scalar UDF invocation (`call`, `call_by_key`) | `executor.rs` |
| 5 | User-defined aggregate (UDA) instance lifecycle | `executor.rs` |
| 6 | CqlValue ↔ WIT bidirectional conversion (all CQL types incl. UDT, decimal, duration) | `convert.rs` |
| 7 | Fuel-based CPU metering (1M fuel per invocation, 10M aggregate) | `sandbox.rs`, `executor.rs` |
| 8 | Epoch-based preemption (background `udf-epoch-ticker` thread) | `executor.rs` |
| 9 | Bump-allocator arena for row-level allocation reuse | `arena.rs` |

**DDL replication:** `ferrosa-udf` itself has no DDL replication code. Per `components.md`, `CREATE FUNCTION` / `DROP FUNCTION` DDL is parsed by `ferrosa-cql`, routed through `DdlPath`, and replicated via `DdlOperation`/`RaftCommand`. The UDF crate is a pure execution library — it exposes `compile`, `invalidate`, `call` — and the DDL replication belongs to `ferrosa-cluster`. The invalidation path (`UdfExecutor::invalidate`) exists as the receiver for a post-DDL notification, but the caller-side plumbing (Raft-applied command → invalidate) lives outside this crate.

**No dedicated test directory.** All 30+ tests are inline `mod tests` in `executor.rs` and `convert.rs`.

---

## 2. Spec Coverage Matrix

| Feature Area | Spec Reference | Code Present | Tests Present | Status |
|---|---|---|---|---|
| **ferrosa-graph** | | | | |
| MATCH/CREATE/SET/DELETE | `graph-gap-closure.md`, `components.md` | Yes | Integration tests in `ferrosa-graph/tests/` | Covered |
| MERGE statement | `overview.md` mentions MATCH/CREATE/MERGE/DELETE | No AST variant | None | **GAP — unimplemented** |
| SUBSCRIBE streaming | G1 in `graph-gap-closure.md` | Yes | None (no test exercises subscribe endpoint) | Partial |
| Aggregation (G2) | `graph-gap-closure.md` | Yes | Integration tests | Covered |
| Variable-length paths (G3) | `graph-gap-closure.md` | Yes | Integration tests | Covered |
| Hop property filtering (G4) | `graph-gap-closure.md` | Yes | Integration tests | Covered |
| Scalar functions in RETURN (G5) | `graph-gap-closure.md` | Yes | Integration tests | Covered |
| Bolt v5 (G6) | `graph-gap-closure.md` | Yes | No Bolt-level tests | Partial |
| WCO / leapfrog triejoin (G7) | `graph-gap-closure.md` | Yes | No triejoin-specific tests | Partial |
| Schema-typed edge tables | `bug-ferrosa-memory-bypasses-graph-api-for-writes.md` | Named tables not owned by graph engine | None | **GAP — schema coupling unresolved** |
| DISTINCT modifier | `gap-S4-graph-distinct-negative.md` | Marked implemented | Integration tests | Covered |
| Cluster read routing | `gap-S2-graph-cluster-read-routing.md` | Marked implemented | No cluster integration tests | Partial |
| **ferrosa-sparql** | | | | |
| Parser (spargebra, SPARQL 1.2/RDF*) | `bug-sparql-endpoint-audit.md`, `components.md` | Yes | Inline tests | Covered |
| Tenant isolation (BUG-S1, S2) | `bug-sparql-endpoint-audit.md` | Fixed | Inline tests | Covered |
| Partition key decoding (BUG-S3) | `bug-sparql-endpoint-audit.md` | Fixed | Inline tests | Covered |
| FILTER eval | `components.md` | Yes | 7 inline tests | Covered |
| Property path BFS | `components.md` | Yes | 11 inline tests | Covered |
| Content negotiation | `bug-sparql-endpoint-audit.md` BUG-S7 | Yes | 11 inline results tests | Covered |
| Turtle serializer | `components.md` | Stub (outputs N-Triples) | Tests pass (but format wrong) | **GAP — silent format violation** |
| RDF* execution (BUG-S15) | `bug-sparql-endpoint-audit.md` | Stub, returns empty | 2 inline tests (parse only) | **GAP — unimplemented** |
| Reverse edge index (BUG-S8) | `bug-sparql-endpoint-audit.md` | Missing `typed_edges_by_dst` view | None | **GAP** |
| SPARQL UPDATE | `components.md` | Yes | 2 inline tests | Covered |
| No external integration tests | `components.md` | 0 files in `tests/` | None | Gap |
| **ferrosa-udf** | | | | |
| Wasmtime compilation + caching | `components.md` | Yes | 30 inline tests | Covered |
| CqlValue ↔ WIT conversion | `components.md` | Yes | 42 inline tests | Covered |
| Fuel metering + epoch preemption | `components.md` | Yes | 2 inline sandbox tests | Covered (lightly) |
| DDL replication (caller side) | `components.md` | In ferrosa-cluster, not here | No e2e test | Partial |
| UDA lifecycle | `components.md` | Yes | Inline tests | Covered |
| ferrosa-memory SPARQL client | `ferrosa-memory/specs/ARCHITECTURE.md` | Missing — ferrosa-memory reads via Cypher only | None | **GAP — no SPARQL driver in ferrosa-memory** |

---

## 3. Test Coverage

### ferrosa-graph

- **`ferrosa-graph/tests/parser_proptest.rs`** — 2 property-based tests: parser and lexer never panic on arbitrary input (up to 200 chars). Correctness is not checked, only panic safety.
- **`ferrosa-graph/tests/graph_http_integration.rs`** — Integration tests exercising the full Axum router via `tower::ServiceExt`. Tests exercise schema creation, CREATE/MATCH queries, aggregations, and auth. Exact test count not enumerated here but the file is 841+ lines of setup + cases.
- **`executor/expand.rs`, `engine.rs`, etc.** — inline unit tests within source modules (engine serialization, schema, misc).
- **Missing:** No test exercises the Bolt TCP server. No test exercises SUBSCRIBE/SSE streaming. No test exercises leapfrog triejoin in isolation. No test exercises the adjacency reconcile task under divergence conditions.

### ferrosa-sparql

- **No `tests/` directory.** All 77 tests (per `components.md`) are inline `mod tests` blocks within `executor.rs` (20 tests), `planner.rs` (13 tests), `property_path.rs` (11 tests), `results.rs` (11 tests), `filter.rs` (7 tests), `engine.rs` (8 tests), `rdf_star.rs` (2 tests), `update.rs` (2 tests), `triple_store.rs` (2 tests), `namespace.rs` (1 test).
- **Missing:** No end-to-end test hits the HTTP endpoint with a real storage backend. No test exercises content negotiation at the HTTP layer. No test covers multi-keyspace isolation round-trip.

### ferrosa-udf

- **No `tests/` directory.** 30 tests in `executor.rs`, 42 tests in `convert.rs`, 2 tests in `sandbox.rs`.
- **Missing:** No test invokes a real WASM binary. No test covers the DDL replication path (compile → Raft commit → invalidate). No test covers epoch interruption firing on a long-running WASM function.

---

## 4. Gaps

### P0 Gaps

**P0-1: MERGE statement not implemented in ferrosa-graph**
The `overview.md` and external documentation list MERGE as a supported Cypher operation. There is no `Merge` AST variant, no parser branch for `MERGE`, and no executor path. Clients using MERGE (including `ferrosa-memory` if/when writes are moved to Cypher) will fail. This is a correctness gap, not a missing test.

**P0-2: ferrosa-memory bypasses the graph API with direct CQL writes to graph-owned tables**
Documented in `/Users/bkearns/src/ferrosa-memory/specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md`. `ferrosa-memory-core/src/cql_storage.rs` issues `INSERT INTO {ks}.typed_edges`, `folded_into`, `mentioned_in`, `co_occurs_with`, `supersedes`, `derived_edges_by_pred`, `derived_edges_by_src` directly via CQL. The graph engine's adjacency invariants, reconciliation task, and Cypher-level hooks (auth, audit, rate limits) are bypassed entirely. Any schema change to those tables silently corrupts ferrosa-memory writes. The fix requires MERGE support in ferrosa-graph (P0-1 above) before it can be resolved.

### P1 Gaps

**P1-1: RDF* execution is a stub**
`ferrosa-sparql/src/rdf_star.rs:evaluate_rdf_star_pattern` returns empty annotations unconditionally with a tracing warning: `"RDF* annotation queries are not yet fully implemented"`. The spargebra parser correctly parses RDF* quoted triples (tested). The `edge_annotations` table is documented in the module but does not exist in storage. Any query using `<< ?s ?p ?o >> ?prop ?val` syntax silently returns no annotations.

**P1-2: Turtle serializer is a silent format violation**
`ferrosa-sparql/src/results.rs:to_turtle()` delegates to `to_ntriples()` — it returns N-Triples content with a `text/turtle` content type. Clients requesting Turtle receive syntactically valid N-Triples but semantically wrong Turtle (missing prefix declarations, different syntax). Existing tests pass because they don't validate format semantics.

**P1-3: No SPARQL client in ferrosa-memory**
Per `ferrosa-memory/specs/ARCHITECTURE.md`, ferrosa-memory reads graph data via the Cypher HTTP endpoint on port 7474. There is no SPARQL client — ferrosa-memory cannot query its RDF data via SPARQL, even though ferrosa-sparql is running on port 8080 and the data is stored in `rdf_triples`. This means the SPARQL endpoint is untested against real ferrosa-memory workloads and inaccessible to the primary consumer that would benefit from SPARQL's RDF-native query capabilities.

### P2 Gaps

**P2-1: Bolt server has no tests**
The Bolt v5 implementation (`bolt/`) has no unit or integration tests. Codec round-trips, handshake negotiation, and message dispatch are untested. A Bolt protocol regression would only be caught by a driver-compatibility test run.

**P2-2: Reverse edge index (ObjectScan) is missing**
BUG-S8 from `bug-sparql-endpoint-audit.md` is not resolved: `?s ?p :bob` style queries (object-bound, subject-free) fall back to a full table scan capped at 10,000 rows. The `typed_edges_by_dst` materialized view documented in `specs/sparql-endpoint-architecture.md` does not exist. Queries over large datasets silently return incomplete results.

**P2-3: SUBSCRIBE streaming has no test coverage**
`executor/subscribe.rs` and the SSE endpoint in `http.rs` have no tests. The subscription registry, per-connection limits, and SSE stream lifecycle are exercised only at compile time.

---

## 5. Recommendations

**R1: Implement MERGE before moving ferrosa-memory writes to Cypher.**
MERGE is the natural Cypher idiom for idempotent upserts. Without it, ferrosa-memory would need to issue separate MATCH + CREATE sequences, which are not atomic and lose the "write once, reconcile" guarantee. MERGE unlocks P0-2. Estimated scope: extend AST, parser, physical planner, and executor — roughly the same complexity as CREATE.

**R2: Promote SPARQL unit tests to an external `tests/` integration suite.**
All 77 SPARQL tests are inline and use mock data. A single integration test file that starts `SparqlEngine` with a real `StorageEngine` and round-trips INSERT DATA → SELECT → content negotiation → Turtle would have caught both the Turtle format bug (P1-2) and the RDF* stub (P1-1) before merge.

**R3: Implement proper Turtle serialization (P1-2) and the RDF* `edge_annotations` table (P1-1) together.**
Both involve the same read path from storage into RDF graph data. Implementing `edge_annotations` as a CQL table (mirroring `rdf_star.rs`'s schema comments) and wiring `evaluate_rdf_star_pattern` to read from it is a bounded effort. The Turtle serializer fix is ~50 lines. Doing them together closes the SPARQL spec compliance gap.

**R4: Add a Bolt protocol smoke test using the Neo4j Rust or Python driver.**
The Bolt server is the primary interface for production graph clients. A single `FERROSA_TEST_CONTAINERS=1` integration test that connects a real driver, issues a MATCH, and validates the response would catch codec regressions early. This is a testing-infrastructure gap, not an implementation gap.

**R5: Create and enforce a graph table access-control boundary.**
Until MERGE is implemented and ferrosa-memory is migrated (R1 + P0-2), add CQL role-based access control to restrict writes to `typed_edges`, `folded_into`, and the other semantic edge tables to the ferrosa-graph service account. This is tracked in `ferrosa-memory/specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md`. It is a defense-in-depth measure that makes the boundary auditable even before the code migration completes.

---

## Summary

| Crate | Features identified | P0 gaps | P1 gaps | P2 gaps |
|---|---|---|---|---|
| ferrosa-graph | 26 | 2 (MERGE missing; ferrosa-memory write bypass) | 0 | 2 (Bolt untested; SUBSCRIBE untested) |
| ferrosa-sparql | 18 | 0 | 3 (RDF* stub; Turtle format; no ferrosa-memory SPARQL client) | 1 (reverse edge index missing) |
| ferrosa-udf | 9 | 0 | 0 | 1 (Bolt/DDL replication e2e test missing) |

**Top 3 gaps:**
1. MERGE not implemented — blocks safe migration of ferrosa-memory writes to Cypher.
2. ferrosa-memory bypasses graph API — adjacency invariants violated on every edge write.
3. RDF* execution is a stub — SPARQL-star queries silently return empty results.
