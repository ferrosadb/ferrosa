# Bug: PREPARE metadata omits `ANN OF ?` bind marker

Status: DONE
Priority: P0
Area: ferrosa-cql / CQL native protocol / PREPARE metadata

## Problem

`PREPARE` for vector ANN queries such as:

```sql
SELECT fold_id, depth, fold_summary, token_count, raw_trajectory
FROM agent_memory.trajectory_folds
WHERE session_id = ? AND tenant_id = ?
ORDER BY fold_embedding ANN OF ?
LIMIT 5
```

currently reports only the two `WHERE` bind columns in PREPARE metadata, while `count_bind_markers()` correctly counts all three placeholders. Strict drivers reject the mismatch and Ferrosa returns:

```text
expected 3 bind-marker column spec(s) but resolved only 2
```

This breaks the ferrosa-memory ANN recall path and forces fallback to non-ANN `LIMIT` retrieval.

## Root Cause

`ferrosa-cql/src/connection.rs::analyze_prepared_columns()` handles SELECT bind markers from `where_clauses`, but does not inspect `SelectStatement::ann_of`. The parser and AST already represent `ANN OF ?`, and `count_bind_markers()` already counts it.

## TDD Blueprint

### RED

Add a unit test in `ferrosa-cql/src/connection.rs` proving SELECT bind-marker column collection includes the vector column from `ANN OF ?` after ordinary `WHERE` bind markers.

Expected failing behavior before the fix:

```text
left: ["session_id", "tenant_id"]
right: ["session_id", "tenant_id", "fold_embedding"]
```

or an equivalent compile failure while introducing the tested helper.

### GREEN

Introduce a small helper used by `analyze_prepared_columns()` for SELECT statements:

```rust
fn select_bind_marker_columns(s: &SelectStatement) -> Vec<&str>
```

It must append columns in bind-marker order:

1. `WHERE` bind markers, preserving existing order.
2. `ANN OF ?` vector column, if present and if the ANN term is a bind marker.

Then update the SELECT branch of `analyze_prepared_columns()` to resolve all returned column names into `bound_columns`.

### REFACTOR / Verification

Run:

```bash
cargo test -p ferrosa-cql select_bind_marker_columns_includes_ann_of_bind_marker -- --nocapture
cargo test -p ferrosa-cql count_bind_markers_select_ann_of_placeholder -- --nocapture
cargo test -p ferrosa-cql
```

Optional live verification after deployment:

1. Rebuild FerrosaDB image used by the ferrosa-memory cluster.
2. Restart `ferrosa-memory-node{1,2,3}`.
3. Trigger ferrosa-memory recall/hybrid search.
4. Confirm MCP logs no longer show `expected 3 bind-marker column spec(s) but resolved only 2` for `ORDER BY fold_embedding ANN OF ?`.

## Acceptance Criteria

- A test fails before production code changes and passes after the fix.
- SELECT PREPARE metadata includes `fold_embedding` for `ORDER BY fold_embedding ANN OF ?`.
- Existing `count_bind_markers` tests still pass.
- `cargo test -p ferrosa-cql` passes.
