# TODO: ARRAY(subquery) and IN (subquery) materialize every row without a memory bound

**Severity:** Medium (memory exhaustion on a large IN subquery)
**Component:** ferrosa-sql (general engine)
**Files:** `ferrosa-sql/src/general/eval.rs` (`CExpr::ArraySubquery`), `ferrosa-sql/src/parser_scalar.rs:578`

## Issue

`ARRAY(subquery)` collects every row of its subquery into one `ArrayValue`. For an
uncorrelated subquery, `memoized` then keeps that array for the whole statement. The parser
rewrites `x IN (SELECT ...)` to `x = ANY(ARRAY(subquery))`, so an IN over a large table builds
an array in memory that no spill pool is charged for and no byte ceiling bounds. EXISTS and
scalar subqueries stream (commit 94274af5, "stream subquery rows instead of collecting them").

PostgreSQL builds a hashed subplan only when it fits in `work_mem`, and falls back to
re-scanning otherwise.

## What closes it

Either charge the array to the `SpillCtx` pool and fail loudly with `54000` past a byte
budget, or plan `IN (subquery)` as a semi-join that streams. Add a test that crosses the
budget and gets an error, never truncation.

Recorded 2026-11-09 while consolidating PR 656, because the forge task board was unreachable.
