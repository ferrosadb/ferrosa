---
type: todo
priority: P1
status: in_progress
created: 2026-05-20
updated: 2026-05-22
---

# UDF predicates in WHERE

## Why

The AST and router only partially support function calls in predicates.
`WHERE column = udf(args) ALLOW FILTERING` can be implemented before broader
boolean predicate support, but `WHERE udf(column) = true` needs AST changes.

## Acceptance Criteria

- [ ] RHS scalar UDF predicates evaluate correctly with `ALLOW FILTERING`.
- [x] Unsupported LHS/boolean UDF predicates fail with a clear error.
- [x] WASM UDF errors do not silently filter out rows.
- Tests cover parser, router evaluation, and authorization constraints.

## Progress Notes

- Parser rejects `WHERE udf(column) = true` with a clear message directing
  users to RHS scalar UDF predicates with `ALLOW FILTERING`.
- Router detects non-built-in RHS function calls in WHERE and requires
  `ALLOW FILTERING`.
- `evaluate_where_rhs_term` resolves scalar UDF calls and propagates resolution
  or execution errors instead of converting failures into "row did not match".
- Missing: an end-to-end test with a real WASM component proving
  `WHERE column = udf(args) ALLOW FILTERING` returns matching rows.
