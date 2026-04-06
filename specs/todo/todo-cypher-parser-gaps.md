# TODO: Cypher Parser — Negative Patterns and DISTINCT Ignored

**Severity:** Medium
**Component:** ferrosa-graph
**Files:** `ferrosa-graph/src/parser/parse_impl.rs:598,811`

## Issue

Two Cypher features are parsed but silently ignored:

1. **Negative pattern expressions** (line 598): `WHERE NOT (a)-[:KNOWS]->(b)` is consumed but the negation is not applied. Results include matches that should be excluded.

2. **DISTINCT modifier** (line 811): `RETURN DISTINCT x` is consumed but deduplication is not performed. Duplicate rows may appear in results.

## Impact

Queries silently return incorrect results. No error or warning to the user.

## Fix

Wire the negation flag into the executor's filter evaluation. Implement dedup in the result projection step.
