---
type: todo
priority: P1
status: draft
created: 2026-05-20
updated: 2026-05-20
---

# UDF predicates in WHERE

## Why

The AST and router only partially support function calls in predicates.
`WHERE column = udf(args) ALLOW FILTERING` can be implemented before broader
boolean predicate support, but `WHERE udf(column) = true` needs AST changes.

## Acceptance Criteria

- RHS scalar UDF predicates evaluate correctly with `ALLOW FILTERING`.
- Unsupported LHS/boolean UDF predicates fail with a clear error.
- WASM UDF errors do not silently filter out rows.
- Tests cover parser, router evaluation, and authorization constraints.

