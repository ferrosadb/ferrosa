# Bug: SPARQL Endpoint Audit (feat/sparql-endpoint branch)

**Date:** 2026-04-05
**Branch:** `feat/sparql-endpoint` (commits `e1369d7`, `7380090`)
**Total bugs:** 20 (4 critical, 6 high, 6 medium, 4 low)

## CRITICAL — Must Fix Before Merge

### BUG-S1: Keyspace/Tenant Isolation Bypass
- **File:** `ferrosa-sparql/src/engine.rs:49`
- **Issue:** `keyspace` HTTP parameter ignored (marked `_keyspace`). All queries execute against hardcoded "default" graph.
- **Impact:** Cross-tenant data leakage. Complete loss of isolation.
- **Fix:** Thread keyspace through engine → executor → CQL queries.

### BUG-S2: Hardcoded Keyspace in Executor
- **File:** `ferrosa-sparql/src/executor.rs:142`
- **Issue:** Executor ignores `graph` parameter, always uses hardcoded keyspace "rdf".
- **Impact:** No multi-graph support; all queries hit same table.
- **Fix:** Use graph from execution plan in all CQL queries.

### BUG-S3: Partition Key Decoding Broken
- **File:** `ferrosa-sparql/src/executor.rs:166`
- **Issue:** Subject extraction treats entire composite partition key `(graph, subject)` as subject without decoding length-prefixed components.
- **Impact:** All subject bindings are corrupted/truncated. Breaks all queries.
- **Fix:** Properly decode composite partition key using CQL length-prefix format.

### BUG-S4: Binding Type Compatibility Not Validated
- **File:** `ferrosa-sparql/src/executor.rs:84, 48, 66`
- **Issue:** Join doesn't check binding_type compatibility. A variable bound to URI in one pattern and literal in another passes silently.
- **Impact:** Semantically incorrect results. SPARQL spec violation.
- **Fix:** Compare `binding_type` alongside `value` during join.

## HIGH — Fix Before Testing

### BUG-S5: OFFSET Out-of-Bounds Panic
- **File:** `ferrosa-sparql/src/executor.rs:106`
- **Issue:** `binding_sets[start..end]` panics when OFFSET > result set size.
- **Fix:** `let start = start.min(binding_sets.len());`

### BUG-S6: ASK Query Returns Wrong Format
- **File:** `ferrosa-sparql/src/http.rs:113-118`
- **Issue:** ASK queries return SELECT-style bindings, not `{"boolean": true/false}`.
- **Fix:** Detect ASK queries and return W3C boolean result format.

### BUG-S7: No Content Negotiation
- **File:** `ferrosa-sparql/src/http.rs:111-129`
- **Issue:** Always returns JSON regardless of Accept header. Spec requires Turtle/N-Triples support.
- **Fix:** Parse Accept header, dispatch to results.rs formatters.

### BUG-S8: Reverse Edge Index Missing (ObjectScan full scan)
- **File:** `ferrosa-sparql/src/planner.rs:182-188`, `executor.rs:154-159`
- **Issue:** ObjectScan (`?s ?p :bob`) does full table scan capped at 10,000 rows. Missing data silently.
- **Prereq:** Create `typed_edges_by_dst` materialized view per `specs/sparql-endpoint-architecture.md`.
- **Fix:** Use reverse index for ObjectScan, or reject until index exists.

### BUG-S9: PredicateScan Doesn't Filter by Predicate
- **File:** `ferrosa-sparql/src/executor.rs:154-159`
- **Issue:** PredicateScan fetches up to 10,000 unfiltered rows. No actual predicate filtering.
- **Fix:** Add predicate filter to CQL WHERE clause or post-fetch filter.

### BUG-S10: Silent CQL Key Decoding Failures
- **File:** `ferrosa-sparql/src/executor.rs:224-245`
- **Issue:** `extract_clustering_string` returns empty string on malformed keys instead of errors.
- **Fix:** Return `Result<String>`, log errors.

## MEDIUM — Fix Before Release

### BUG-S11: Subject Binding Always Assumes URI
- **File:** `ferrosa-sparql/src/executor.rs:42`
- **Issue:** Hardcodes `binding_type: "uri"` for subjects. Blank nodes mislabeled.
- **Fix:** Check data for blank node prefix (`_:`).

### BUG-S12: No Keyspace Validation
- **File:** `ferrosa-sparql/src/http.rs:75-78`
- **Issue:** Accepts arbitrary keyspace names without checking schema.
- **Fix:** Validate against schema registry before query.

### BUG-S13: FILTER / ORDER BY / DISTINCT Not Implemented
- **File:** `ferrosa-sparql/src/planner.rs:124-135`
- **Issue:** Planner claims post-fetch evaluation but executor has no implementation. These are silently ignored.
- **Fix:** Implement filter evaluation, sort, and dedup on binding sets.

### BUG-S14: Property Paths Not Supported
- **File:** `ferrosa-sparql/src/planner.rs:138-142`
- **Issue:** `?s foaf:knows+ ?o` returns "unsupported graph pattern" error.
- **Fix:** Translate to BFS/DFS via graph engine internal API.

### BUG-S15: No RDF* Annotation Support
- **File:** entire crate (missing `rdf_star.rs`)
- **Issue:** `<< ?s ?p ?o >> ?prop ?val` syntax not handled. No edge_annotations join.
- **Fix:** Add rdf_star.rs module, join with `edge_annotations` table.

### BUG-S16: No SPARQL UPDATE Support
- **File:** entire crate (missing `update.rs`)
- **Issue:** INSERT DATA, DELETE DATA, MODIFY not implemented. Returns "only SELECT and ASK" error.
- **Fix:** Add update.rs with StorageEngine write integration.

## LOW

### BUG-S17: No Query Size Limit
- **File:** `ferrosa-sparql/src/http.rs:68`
- **Issue:** Accepts arbitrarily large request bodies. DoS risk.
- **Fix:** Add `axum::extract::ContentLengthLimit` (e.g., 1MB).

### BUG-S18: Object Binding Type Not Validated
- **File:** `ferrosa-sparql/src/executor.rs:78-82`
- **Issue:** Trusts stored `obj_type` without validation.
- **Fix:** Validate against `{uri, literal, bnode}`.

### BUG-S19: Health Endpoint Ignores Auth
- **File:** `ferrosa-sparql/src/http.rs:103-109`
- **Issue:** `/sparql/health` returns 200 even when auth required.
- **Fix:** Respect `auth_disabled` flag.

### BUG-S20: Truncated Key Recovery
- **File:** `ferrosa-sparql/src/executor.rs:238`
- **Issue:** Truncated clustering keys produce empty strings silently.
- **Fix:** Log or return error on truncated components.

## Fix Priority Order

1. **BUG-S1 + S2** (tenant isolation) — security blocker
2. **BUG-S3** (partition key decoding) — correctness blocker, all queries broken
3. **BUG-S4 + S5** (join validation + panic) — correctness + stability
4. **BUG-S8 + S9** (reverse index + predicate scan) — performance, incomplete results
5. **BUG-S13** (FILTER/ORDER/DISTINCT) — basic SPARQL compliance
6. **BUG-S6 + S7** (ASK format + content negotiation) — spec compliance
7. **BUG-S14 + S15 + S16** (property paths, RDF*, UPDATE) — feature completeness
