# Implemented Evidence: Cypher Parser — Negative Patterns and DISTINCT

**Severity:** Medium
**Component:** ferrosa-graph
**Files:** `ferrosa-graph/src/parser/parse_impl.rs:598,811`

## Original Issue

Two Cypher features are parsed but silently ignored:

1. **Negative pattern expressions** (line 598): `WHERE NOT (a)-[:KNOWS]->(b)` is consumed but the negation is not applied. Results include matches that should be excluded.

2. **DISTINCT modifier** (line 811): `RETURN DISTINCT x` is consumed but deduplication is not performed. Duplicate rows may appear in results.

## Impact

Queries silently return incorrect results. No error or warning to the user.

## Implementation Evidence

- `ferrosa-graph/src/executor/expand.rs` evaluates `Expr::PatternPredicate`
  with the parsed `negated` flag and returns `!exists` for negative pattern
  predicates.
- `ferrosa-graph/src/executor/expand.rs`, `executor/leapfrog.rs`, and
  `executor/varpath.rs` apply `ReturnClause::distinct` by sorting and
  deduplicating projected rows.
- `ferrosa-graph/tests/graph_http_integration.rs` includes
  `return_distinct_deduplicates_projected_rows` and
  `negative_pattern_predicate_filters_existing_relationships`, which verify
  projected-row deduplication for `RETURN DISTINCT` and filtering for
  `WHERE NOT (a)-[:REL]->(...)`.

## Verification Plan Before Archive

Run the graph HTTP integration tests that cover `RETURN DISTINCT` and negative
pattern predicates on a clean checkout. Archive this item only after attaching
the command output, including the exact test filters and pass/fail summary.
