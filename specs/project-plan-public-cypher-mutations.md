---
type: plan
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-20
for: specs/todo/todo-implement-public-cypher-mutations-for-client-graph-writes.md
related:
  - specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md
  - specs/decisions/design-cql-role-auth-rollout.md
  - /Users/bkearns/src/ferrosa-suite/ferrosa-memory/specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md
  - /Users/bkearns/src/ferrosa-suite/ferrosa-memory/specs/todo/todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md
  - specs/coverage/multimodel-coverage.md
---

# Sprint Plan — Public Cypher Mutations for Client Graph Writes

Implements the P1 todo `todo-implement-public-cypher-mutations-for-client-graph-writes.md`.
Closes the architectural leak in which `ferrosa-memory` writes graph-owned
tables directly because the public graph API has no mutation path.

**Total slices:** 11 TDD slices. **Batches:** 3. **Estimated sprints:** 3.

## Pre-requisites — blockers from the role-auth rollout

Phase 1 (slices 1–7) has **zero dependency** on role-auth and can start immediately.

Phase 2 auth slice (slice 8) is gated on Sprint A + Sprint B of
`specs/decisions/design-cql-role-auth-rollout.md`:

- `PasswordAuthenticator` SASL PLAIN wired into `ferrosa-cql/src/server.rs`
- `has_permission` gap-closed in `ferrosa-cql/src/router.rs`
- `auth_middleware` in `ferrosa/src/web/auth.rs` no longer a no-op
- `graph_engine` role seeded with `MODIFY` on graph tables
- `app_reader` role seeded with `SELECT`-only on those same tables

Files currently under in-flight agent edits — do not touch until their PRs land:
`ferrosa-cql/src/server.rs`, `ferrosa-cql/src/router.rs`, `ferrosa/src/main.rs`.

## Dependency DAG

```
Slice 1 (token MERGE) ─┐
Slice 2 (AST Merge)    ┘ parallel
                       │
                       ▼
Slice 3 (parser MERGE node)
    ▼
Slice 4 (parser MERGE rel + multi-clause + trailing SET)
    ├──► Slice 5 (logical planner validate)
    │        └──► Slice 6 (physical planner MergeUpsert)
    │                 └──► Slice 7 (executor execute_merge)
    │                          ├──► Slice 8 (auth: app_reader denied)   [gated on role-auth A+B]
    │                          ├──► Slice 9 (HTTP E2E round-trip)
    │                          │        └──► Slice 10 (idempotency + adjacency invariants)
    │                          └──► Slice 11 (migration-proof ferrosa-memory shapes)
```

Parallelism:
- Batch 1: slices 1–2 in parallel
- Batch 2: slices 5–6 in parallel after slice 4
- Batch 3: slices 8, 9, 11 in parallel after slice 7; slice 10 after slice 9

## TDD slices

### Slice 1 — lexer `MERGE` keyword

- `ferrosa-graph/src/parser/token.rs` — add `Keyword::Merge`
- `ferrosa-graph/src/parser/lexer.rs` — case-insensitive `"merge"` branch
- **Test (first):** `keyword_merge_round_trips` — `"MERGE"` and `"merge"` lex to `TokenKind::Keyword(Keyword::Merge)`
- **LOC:** ~10

### Slice 2 — AST `Merge` variant

- `ferrosa-graph/src/parser/ast.rs` — `Statement::Merge { patterns: Vec<Pattern>, set_clause: Vec<Assignment> }`
- **Test (first):** `construct_merge_statement` — build a single-node Merge, assert `matches!(stmt, Statement::Merge { .. })`
- **Acceptance:** exhaustive-match compile errors surface every callsite that must be updated
- **LOC:** ~20

### Slice 3 — parse MERGE for node patterns

- `ferrosa-graph/src/parser/parse_impl.rs` — `parse_merge()` + `parse_statement()` arm
- **Tests (first):**
  - `parse_merge_node_succeeds` — `MERGE (n:Entity {entity_id: 'x'}) RETURN n` parses
  - `parse_merge_node_unlabeled_succeeds` — `MERGE (n) RETURN n` parses (label resolution is planner's job)
  - `parse_unsupported_keyword_errors` — `UPSERT ...` gives an explicit error, **not a silent fallback**
- **LOC:** ~60

### Slice 4 — parse MERGE relationships, multi-clause, trailing SET

- Extend `parse_merge()` to handle `MERGE (a)-[r:TYPE]->(b)` and accumulate consecutive MERGE clauses into one `Statement::Merge` with optional trailing `SET`
- Target shape (from the todo):
  ```cypher
  MERGE (a:Entity {entity_id: $src})
  MERGE (b:Entity {entity_id: $dst})
  MERGE (a)-[r:TYPED_EDGE {edge_type: $t}]->(b)
  SET r.weight = $w
  RETURN r
  ```
- **Tests (first):**
  - `parse_merge_relationship_succeeds` — canonical ferrosa-memory edge-upsert parses; `patterns.len() == 3`, `set_clause.len() >= 1`
  - `parse_merge_rel_missing_endpoints_errors`
- **LOC:** ~80

### Slice 5 — logical planner: validate Merge

- `ferrosa-graph/src/planner/logical.rs`
  - `permission_for_statement()` → `Permission::Modify` for Merge
  - `validate()` patterns-match arm treats each MERGE pattern like `Create` (label resolution, perm check)
- **Tests (first):**
  - `validate_merge_requires_modify_permission` — `Select`-only auth → `Err(GraphError::PermissionDenied)`
  - `validate_merge_resolves_bindings` — superuser → `Ok(LogicalPlan)` with bindings populated
- **LOC:** ~30

### Slice 6 — physical planner: `MergeUpsert`

- `ferrosa-graph/src/planner/physical.rs`
  ```rust
  enum PhysicalPlan { ... , MergeUpsert { merges: Vec<MergeOp>, set_clause: Vec<(String, String, Expr)> } }
  struct MergeOp {
      var: Option<String>,
      table: ResolvedTable,
      match_props: Vec<(String, Expr)>,
      create_props: Vec<(String, Expr)>,
  }
  ```
- `plan_merge()` dispatched from `plan()`; `format_plan()` arm for EXPLAIN
- **Test (first):** `plan_merge_produces_merge_upsert` — `MergeUpsert { merges, .. }` with correct arity and table name
- **LOC:** ~80

### Slice 7 — executor: `execute_merge`

Largest and most critical slice. MERGE = "match or create":
1. Read the row by the deterministic key derived from match-property bytes
2. If found, skip create and proceed to SET
3. If not found, call `write_path.write()` (same path `execute_create` uses — **keeps adjacency observers firing**)
4. Apply `set_clause` assignments

**File:** `ferrosa-graph/src/executor/expand.rs`

**Tests (first, in `ferrosa-graph/tests/graph_http_integration.rs`):**
- `merge_node_is_idempotent` — MERGE same node twice; MATCH returns exactly 1
- `merge_relationship_is_idempotent` — MERGE same `(a)-[r:TYPED_EDGE]->(b)` twice; 1 edge
- `merge_set_updates_properties` — MERGE then `SET r.weight = 2.0`; MATCH returns `weight = 2.0`
- `merge_missing_endpoint_returns_error` — unknown label on endpoint → HTTP 400, not panic/500

**LOC:** ~120

### Slice 8 — auth: enforce `Modify` on graph mutation layer

**GATED on role-auth Sprint A+B.**

- Validation slice — no change to `ferrosa-schema/src/auth/permission.rs` expected
- **Test (first):** `merge_denied_for_app_reader_role` in `ferrosa-graph/tests/graph_http_integration.rs` — `POST /graph/query` with `Authorization: Basic <app_reader:ferrosa_user>` + MERGE → HTTP 403
- **Contrast test:** same request with `graph_engine` creds → 200
- **LOC:** ~30 (mostly test setup)

### Slice 9 — HTTP E2E round-trip

**File:** `ferrosa-graph/tests/graph_http_integration.rs` — new "mutation path" section

**Tests (first):**
- `http_create_then_match_round_trip`
- `http_merge_then_match_round_trip`
- `http_mutation_returns_json_result` — body is `{"columns":..., "rows":..., "stats":...}`
- `http_unsupported_mutation_returns_400` — explicit error body, not 500

**LOC:** ~100

### Slice 10 — idempotency + adjacency invariants

**File:** `ferrosa-graph/tests/graph_http_integration.rs`

**Tests (first):**
- `repeated_merge_does_not_create_duplicate_edges` — 5× MERGE same typed edge → MATCH returns 1
- `merge_triggers_adjacency_index_entry` — after MERGE of a relationship, hop query `MATCH (a)-[:TYPED_EDGE]->(b) RETURN b` returns the correct target (verifies observer fired on the CREATE arm)

**LOC:** ~60

### Slice 11 — migration-proof: ferrosa-memory shapes

**File:** `ferrosa-graph/tests/graph_http_integration.rs` (`migration_proof` section)

**Tests (first):**
- `migration_proof_typed_edge_upsert_no_direct_table_ref`
- `migration_proof_folded_into`
- `migration_proof_mentioned_in`
- `migration_proof_supersedes`
- `migration_proof_no_direct_table_reference` — string-grep assertion that none of the migration query bodies contain `typed_edges`, `folded_into`, `mentioned_in`, or `supersedes` as raw CQL identifiers

**LOC:** ~80

## Risk register

**R1 (HIGH) — Idempotency under concurrent MERGE.** Ferrosa has no row-level
locking. Two concurrent MERGE calls with the same match properties will both
read "not found", both write, and create two rows. `execute_create` today uses
`uuid::Uuid::new_v4()` for fresh keys. MERGE must instead derive a
**deterministic content-addressed key** from the match-property bytes (e.g.
`blake3(canonical_bytes(sorted(match_props)))`) so concurrent writes converge to
the same partition key. Without this, idempotency tests pass in serial and fail
under concurrency.

**R2 (MEDIUM) — Multi-clause MERGE planner scope creep.** The canonical
ferrosa-memory query chains three MERGE clauses; the relationship MERGE depends
on bindings introduced by earlier MERGE clauses. The parser (slice 4) must thread
bindings across clauses; the physical planner (slice 6) must serialize them in
order (MERGE a, MERGE b, MERGE rel using already-resolved a/b). Treating each
MERGE as independent will fail at relationship-endpoint lookup.

**R3 (MEDIUM) — Adjacency observer bypass.** The `AdjacencyIndexObserver` fires
on `StorageEngine::register_observer` notifications raised inside
`write_path.write()`. If `execute_merge` takes a shortcut for its read-before-
write (e.g., calls a lower-level storage API), the observer may be bypassed,
leaving the adjacency index stale — hop queries would silently return no
results. **Constraint:** the create arm of `execute_merge` must call the same
`write_path.write()` that `execute_create` uses; the read uses `write_path.read()`.

## Timebox — 3 sprints

**Sprint 1 (days 1–5) — MERGE parsing and planning.** Slices 1–6. Exit:
`EXPLAIN` returns a valid `MergeUpsert` plan for the canonical ferrosa-memory
query shapes; no executor yet.

**Sprint 2 (days 6–10) — MERGE execution and HTTP integration.** Slices 7, 9,
10. Exit: `POST /graph/query` with MERGE returns 200; repeated MERGE does not
duplicate; subsequent MATCH returns merged state; EXPLAIN correct for full
multi-clause form.

**Sprint 3 (days 11–15) — Auth + migration proof.** Slice 8 (gated on role-auth
A+B), slice 11. Exit: `app_reader` denied on MERGE; all four ferrosa-memory edge
shapes work through public Cypher with no direct table references; docs updated.

## Exit criteria — mapped to todo acceptance

| Acceptance criterion | Satisfied by |
|---|---|
| Public HTTP/Cypher mutation for CREATE, SET, MERGE | slices 7, 9 |
| MERGE end-to-end: AST, parser, planner, executor | slices 1–7 |
| Mutations preserve adjacency/index invariants | slices 7, 10 |
| Auth checks at graph mutation layer | slices 5, 8 |
| Idempotent repeated MERGE | slices 7, 10 |
| ferrosa-memory shapes via public Cypher | slice 11 |
| Public docs describe supported subset | post-slice-11 docs update |

## Critical files

- `ferrosa-graph/src/parser/token.rs`
- `ferrosa-graph/src/parser/ast.rs`
- `ferrosa-graph/src/parser/lexer.rs`
- `ferrosa-graph/src/parser/parse_impl.rs`
- `ferrosa-graph/src/planner/logical.rs`
- `ferrosa-graph/src/planner/physical.rs`
- `ferrosa-graph/src/executor/expand.rs`
- `ferrosa-graph/tests/graph_http_integration.rs`
