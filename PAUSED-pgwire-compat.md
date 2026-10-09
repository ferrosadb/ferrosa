# PAUSED — pgbench/PG-wire compatibility surface

> Paused 2026-10-09. Work item: `specs/todo/postgres-frontend/in-process/feat-pgbench-compat-surface.md`
> Branch: `feat/pgwire-pgbench-compat` (worktree `.wt-pgcompat`, off `e3d72e44`).
> Last commit: `491dff28` "wip(pgwire): DROP TABLE green (5 tests); multi-row INSERT + value functions in progress".

## State when paused — the tree DOES NOT COMPILE

That is deliberate: `InsertStmt.values` was changed to `rows: Vec<Vec<ScalarValue>>` and the
parser was updated, but these call sites were NOT yet updated:

- `ferrosa-sql/src/parser.rs` — three pre-existing tests assert `ins.values`
  (search `ins.values`; ~lines 1930 / 1949 / 2033). Change to nested `ins.rows`.
- `ferrosa-postgres/src/query.rs` — `execute_insert` reads `ins.values`.
- `ferrosa-postgres/src/extended.rs` — the prepared-INSERT path.

## Green so far

- **Cycle 1 — DROP TABLE** (complete, full stack): `Statement::DropTable` +
  `parse_drop` (multi-table, `IF EXISTS`, schema-qualified) in `parser_ddl.rs`;
  `DdlExecutor::drop_table` → existing `DdlOperation::DropTable` in `ddl.rs`;
  `execute_drop_table` (42P01 unless IF EXISTS → skip; 25001 inside a txn);
  dispatch in `query.rs`; per-table `Permission::Drop` in `authz.rs`.
  5 new parser tests. Verified: `cargo test -p ferrosa-sql --lib` **166 passed**,
  `cargo test -p ferrosa-postgres` **272 passed**.
- **Cycle 2 — multi-row INSERT + value functions** (parser done, callers pending):
  `parse_values_row` (per-row arity check), `parse_scalar_value` now accepts
  `now()` and bare `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` as
  `ScalarValue::Func`. 5 new tests written, not yet compiled.

## Remaining to make `pgbench -i` work (ground truth, live-captured 2026-10-09)

| # | Statement | Today |
|---|---|---|
| 1 | `DROP TABLE IF EXISTS a, b, c, d` | DONE |
| 2 | `CREATE TABLE t (...)` with **no PK**, PK added later | ferrosa REQUIRES a PK at CREATE — a semantic gap, not just syntax |
| 3 | `ALTER TABLE t ADD PRIMARY KEY (col)` | unsupported; `TableUpdates` has no PK-change support today |
| 4 | `COPY t FROM STDIN` | unsupported; needs a CopyIn/CopyData/CopyDone wire handshake |
| 5 | `TRUNCATE t` | unsupported; `StorageEngine::truncate()` already exists |
| 6 | `VACUUM` / `VACUUM ANALYZE` | unsupported; accept-and-report (ferrosa has no vacuum) |
| 7 | `'a' \|\| 'b'` | lexer has no `\|\|` token |
| 8 | multi-row `VALUES (...),(...)` | cycle 2 (callers pending) |
| 9 | `now()` / `CURRENT_TIMESTAMP` in VALUES | cycle 2 (callers pending) |
| 10 | `character(N)` (no `VARYING`) | `parse_pg_type` demands `VARYING` after `character` |

## Resume order

1. Fix the three call sites above → `frg run --tee -- cargo test -p ferrosa-sql -p ferrosa-postgres`.
2. Map `ScalarValue::Func` in `execute_insert`; add `NOW` to `eval_scalar_func`.
3. `character(N)` alias; `WITH (FILLFACTOR=...)` ignore.
4. ALTER ADD PRIMARY KEY, COPY, TRUNCATE, VACUUM.
5. Re-run `bench/maas-loadtest/runner-pg/pg-runner.sh probe f1` on a fresh cluster.
