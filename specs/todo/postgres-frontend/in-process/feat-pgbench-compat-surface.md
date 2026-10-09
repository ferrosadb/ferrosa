---
type: work-item
status: in-process
priority: P0
created: 2026-10-09
updated: 2026-10-09
owner: agent
pr: ferrosadb/ferrosa#544
---

# pgbench / PG-wire compatibility surface

> PR: **ferrosadb/ferrosa#544** — "pgbench smoke-check: all changes required to pass
> the PostgreSQL/pgbench gate". Everything required to get through the pgbench smoke
> check is staged there.

## Goal

A clean pass of `pg-runner.sh probe <target>` against ferrosa, then `pgbench -i` followed
by a `pgbench` run. Only then is the 1-node / 3-node / native comparison meaningful —
before this, ~30 % of explicit transactions were being abandoned, and pgbench is almost
entirely explicit transactions.

**The probe is the acceptance test.** Re-run it after each increment and watch the
failure list shrink:

```
fly ssh console -a maas-loadtest-pgrunner -C "/usr/local/bin/pg-runner.sh probe f1"
```

## Ground truth (live-captured 2026-10-09)

| # | Statement | Status |
|---|---|---|
| 1 | `DROP TABLE IF EXISTS a, b, c, d` | **DONE** — full stack + 5 parser tests |
| 2 | `CREATE TABLE t (...)` with **no PK** | **OPEN** — ferrosa requires a PK at CREATE. A *semantic* gap, not syntax |
| 3 | `ALTER TABLE t ADD PRIMARY KEY (col)` | **OPEN** — `TableUpdates` has no PK-change support |
| 4 | `COPY t FROM STDIN` | **OPEN** — needs the CopyIn/CopyData/CopyDone wire handshake |
| 5 | `TRUNCATE t` | **OPEN** — `StorageEngine::truncate()` already exists |
| 6 | `VACUUM` / `VACUUM ANALYZE` | **OPEN** — accept-and-report (ferrosa has no vacuum) |
| 7 | `\|\|` string concat | **OPEN** — the probe's own table census uses it |
| 8 | **multi-row `INSERT ... VALUES (...), (...)`** | **OPEN — parser only.** See below |

Items 2 and 3 are the real ones: pgbench creates PK-less tables and adds the primary key
afterwards, so this is a genuine semantic difference from PostgreSQL, not parser surface.
Item 4 is the largest single piece (a wire-protocol handshake).

## Item 8 — multi-row INSERT (explicitly in scope)

**Not implemented. Do not read "parsed" as "working".**

- **Parser: DONE.** `InsertStmt.values` became `rows: Vec<Vec<ScalarValue>>`, with a
  per-row arity check, and `parse_scalar_value` accepts `now()` and bare
  `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` as `ScalarValue::Func`.
  `insert_placeholders` and `insert_param_targets` flatten every row for the extended
  protocol.
- **Execution: NOT DONE.** `execute_insert` is single-row only and **fails loud** with
  SQLSTATE `0A000` for `rows.len() != 1`, rather than writing row 1 and silently dropping
  rows 2..N. Silent row loss is the failure mode this repo treats as unacceptable, so
  fail-loud is the deliberate holding position, not an oversight.

### CRITICAL: the SQL layer is correct; the loss is BELOW it

An execution implementation exists and is **correct in-process**, yet on the live
cluster the same statement reports `INSERT 0 3` and persists only row 1 (reproduced
twice on a fresh table). The `0A000` guard is therefore back in place: a silent
row-drop announced as a successful count is strictly worse than an error.

**Two hypotheses were eliminated, both by negative control rather than by argument:**

1. **Per-row state reused across rows.** `col_values` / `sql_values` / `regular_cells`
   were declared outside the per-row loop, so `regular_cells` accumulated every earlier
   row's cells. Hoisting them back out and re-running the value-asserting test still
   PASSES — the duplicate cells resolve last-wins, so the bug is real but benign here.
   It cannot cause the row-drop. (The per-row declarations are kept: they are correct
   and cheap.)
2. **The buffered write-set.** The server reaches INSERT via `Some(txn_writes_mut())`,
   the one path an autocommit test never touches — so this was the prime suspect.
   Test `a_buffered_multi_row_insert_applies_every_row` drives a 3-mutation write-set
   through `write_atomic_batch` exactly as COMMIT does, and all three rows land.

Four in-process multi-row tests are green, spanning the SQL, MVCC and server layers:

```
mvcc::tests::staged_multi_row_accord_commit_is_visible_as_one_snapshot_version ... ok
server::txn_atomicity_tests::multi_row_snapshots_hide_partial_replica_apply      ... ok
query::txn_buffer_tests::multi_row_insert_writes_every_row_and_reports_the_count ... ok
query::txn_buffer_tests::a_buffered_multi_row_insert_applies_every_row           ... ok
```

**Conclusion: the defect is in the live multi-node apply path, below ferrosa-postgres.**
A single-node in-process engine cannot exhibit it. Next step: reproduce with a real
clustered process test (3 nodes, real transport) — a mock or single-node harness will
keep passing — and instrument what the coordinator actually persists for a
multi-mutation batch before suspecting the SQL layer again.

**Re-enabling execution** requires that clustered repro first, then: drop the guard,
assert one `CommandComplete` tagged `INSERT 0 3`, three rows landed, each carrying its
own value.

### What execution needs

`execute_insert` (`ferrosa-postgres/src/query.rs`) resolves, per row: the column values,
the partition/clustering key, the regular cells, and the RETURNING row. To support N rows
that whole block must move inside a loop over `ins.rows`, with:

- the write per row via `apply_or_buffer(..., txn.as_deref_mut(), ...)` — the borrowed
  `Option<&mut Vec<PgWrite>>` must be re-borrowed per iteration, which is what makes this
  a refactor rather than a loop-and-done;
- **one** combined result: a single `RowDescription`, one `DataRow` per inserted row, and
  a single `CommandComplete "INSERT 0 N"` — not N concatenated result sets;
- a failure on row *k* must leave no partially-announced result: rows already buffered into
  an open transaction stay buffered (they commit or roll back with it), and the error is
  reported untouched.

TDD it: a RED test asserting 3 rows land (all three readable back) and that
`CommandComplete` reports `INSERT 0 3`; plus a negative control asserting the current
`0A000` fail-loud so the guard cannot silently regress to dropping rows.

## Also staged on #544 (not pgwire surface, but required to get through the gate)

- zero-byte SSTable components dropped on the receiver (Rows.db corruption);
- half-open lane never reconnects → probe-first detection bounded by a failure window;
- the coordinator's local Apply bypassed the abandon (opaque 58000 → retryable 40001);
- a dependency this replica has no live state for must not park its waiter — the cure for
  the ~30 % abandonment.

## Operator tunables

`FERROSA_ACCORD_TXN_TIMEOUT_SECS` (10) · `FERROSA_ACCORD_BARRIER_TIMEOUT_SECS` (5) ·
`FERROSA_NET_LANE_FAILURE_THRESHOLD` (3) · `FERROSA_NET_LANE_FAILURE_WINDOW_MS` (30000) ·
`FERROSA_NET_LANE_PROBE_TIMEOUT_MS` (2000)
