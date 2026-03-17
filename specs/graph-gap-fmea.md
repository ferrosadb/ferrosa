# FMEA — Graph Gap Closure

> Last updated: 2026-03-16
> Status: Draft

## Scope

Failure modes for the 7 gaps (G1–G7) identified in `graph-gap-closure.md`,
cross-referenced with threats T12–T16 from the delta threat model.

## FMEA Table

| ID | Component | Failure Mode | Effect | S | O | D | RPN | Mitigation | Test Case |
|----|-----------|-------------|--------|---|---|---|-----|-----------|-----------|
| F1 | Expression Evaluator | Type mismatch in comparison (e.g., int > string) | Incorrect WHERE filtering, data leakage | 7 | 5 | 3 | **105** | NULL propagation on type mismatch; never panic | `eval_compare_int_vs_string_returns_null` |
| F2 | Variable-Length Paths | Cycle in graph causes infinite BFS | OOM, query hangs until timeout | 9 | 4 | 2 | **72** | Visited set (cycle detection); hard max-hops cap (10) | `varpath_cycle_terminates` |
| F3 | Variable-Length Paths | Exponential fan-out at each hop | Memory exhaustion, CQL starvation (T13) | 9 | 5 | 3 | **135** | Total visited-vertex budget (default 100K); per-hop fan-out still applies | `varpath_fanout_budget_exceeded_returns_error` |
| F4 | SUBSCRIBE | Subscription leak on client disconnect | Memory+CPU leak from orphan background tasks | 7 | 4 | 4 | **112** | Drop guard on SSE/WebSocket close; subscription registry cleanup | `subscribe_cleanup_on_disconnect` |
| F5 | SUBSCRIBE | Too many concurrent subscriptions (T12) | Thread pool exhaustion, query latency spike | 7 | 3 | 3 | **63** | Per-connection limit (8); global limit (configurable) | `subscribe_limit_per_connection` |
| F6 | Aggregation | `collect()` on large result set | OOM from materializing all rows | 8 | 3 | 3 | **72** | `collect()` size limit (default 10K elements) | `aggregate_collect_limit_exceeded` |
| F7 | Aggregation | High-cardinality GROUP BY | Memory proportional to unique groups | 6 | 3 | 4 | **72** | Max group count limit (default 100K) | `aggregate_max_groups_exceeded` |
| F8 | Aggregation | Division by zero in AVG with zero rows | Panic or NaN in result | 5 | 3 | 2 | **30** | Return NULL for empty aggregation | `aggregate_avg_empty_returns_null` |
| F9 | Hop Filtering | Property filter references non-existent column | Silent skip (no match) vs. error | 4 | 4 | 3 | **48** | Return NULL for missing properties (consistent with Cypher semantics) | `hop_filter_missing_prop_returns_no_match` |
| F10 | Bolt Codec | Malformed PackStream causes decode panic (T15) | Connection crash, potential DoS | 8 | 3 | 3 | **72** | Message size limit; proptest fuzzing; catch panics at connection boundary | `bolt_fuzz_never_panics` |
| F11 | Bolt Handshake | Version negotiation fails silently | Client connects but can't execute queries | 4 | 3 | 2 | **24** | Explicit error response with supported versions | `bolt_unsupported_version_returns_error` |
| F12 | Expression Evaluator | Deeply nested boolean expression in WHERE | Stack overflow in recursive eval | 7 | 2 | 2 | **28** | Reuse parser depth limit (64); iterative eval for AND/OR chains | `eval_deep_nesting_returns_error` |
| F13 | SUBSCRIBE | Delta mode misses intermediate state | Client sees inconsistent snapshots | 5 | 4 | 5 | **100** | Delta = diff between consecutive full snapshots; document eventual consistency | `subscribe_delta_captures_changes` |
| F14 | Function Calls | Unknown function name in RETURN | Unclear error vs. silent NULL | 4 | 4 | 2 | **32** | Return validation error at plan time listing known functions | `unknown_function_returns_validation_error` |

## Risk Priority Summary

| RPN Range | Count | Action |
|-----------|-------|--------|
| >= 100 (Critical) | 3 | F1 (105), F4 (112), F3 (135) — Must fix before shipping |
| 50–99 (High) | 5 | F2, F5, F6, F7, F10, F13 — Fix in same sprint |
| < 50 (Medium) | 4 | F8, F9, F11, F12, F14 — Fix as encountered |

## Test Plan

### Critical (Sprint 1)

1. **F3**: Variable-length path budget — write test with dense graph (each node has 100 edges), verify `[*1..5]` hits budget before OOM
1. **F4**: Subscription cleanup — open SSE stream, drop client, verify background task is aborted within 1s
1. **F1**: Type-safe expression eval — parameterized test with all type combinations (int/float/string/bool/null x all CompareOps)

### High (Sprint 1–2)

1. **F13**: Delta subscribe — insert 3 rows between polls, verify delta contains exactly 3 new rows
1. **F2**: Cycle detection — create A->B->C->A cycle, run `[*1..10]`, verify terminates and does not revisit
1. **F5**: Subscription limit — open 9 subscriptions on one connection, verify 9th is rejected
1. **F6**: Collect limit — `collect()` on 20K-row result with 10K limit, verify error
1. **F7**: Group limit — GROUP BY on column with 200K distinct values, verify error at 100K
1. **F10**: Bolt fuzz — proptest on PackStream decoder with random bytes

### Medium (Sprint 2+)

1. **F8**: Empty AVG — `RETURN avg(n.score)` with no matching rows, verify NULL
1. **F9**: Missing property — hop filter `{nonexistent: 42}` returns zero matches
1. **F11**: Bolt version — connect with unsupported version, verify error message
1. **F12**: Deep eval nesting — 100-level nested AND, verify error not panic
1. **F14**: Unknown function — `RETURN foo(n)` at plan time returns error listing known functions
