---
title: PostgreSQL front end — what Postgrex, Ecto and Oban need
status: todo
source: census
updated: 2026-11-08
executive_summary: >-
  Every statement Postgrex 0.19, Ecto SQL 3.12 and Oban 2.19 send to run Oban's
  migrations (v12) and execute one job, captured from PostgreSQL 16 with
  log_statement=all and replayed against the ferrosa front end. Postgrex fails at
  connect, on its type-bootstrap query. The list below is the gap, grouped by feature
  and ordered by when a client hits it.
---

# PostgreSQL front end — Postgrex, Ecto and Oban

## The census

* **Where it came from.** `ferrosa-postgres/tests/fixtures/oban_census.sql` holds 58
  distinct statements, in the order PostgreSQL 16 received them from an Elixir client.
* **What the client did:** `Postgrex.start_link`, then `Ecto.Migrator` running
  `Oban.Migration.up(version: 12)`, then `Oban.insert` and one job executed by the
  `default` queue.
* **How Oban was run:** `notifier: Oban.Notifiers.PG` and `peer: Oban.Peers.Global`, so
  no `LISTEN/NOTIFY` and no `oban_peers` writes.
* **Today's result.** Against ferrosa (main, v0.35.1) the client fails at connect:
  `42601 expected FROM, found LParen` on the type bootstrap.

## Gaps, in the order a client hits them

| # | Statement shape | Needed by | Gap |
|---|---|---|---|
| 1 | Type bootstrap: `ARRAY(SELECT …)`, three-way `LEFT JOIN`, `coalesce`, `NOT EXISTS`, `OR` in a join condition, `pg_range` | Postgrex, every connect | Scalar subquery `ARRAY(…)`; more than one join; `NOT EXISTS` (W1); ~~`pg_range` projection~~ **Done (W3):** empty, streamed |
| 2 | `CREATE TABLE IF NOT EXISTS … ` over the **extended** protocol | Ecto | ~~Extended-protocol `Parse` of DDL is refused~~ **Done (W3):** DDL prepares as `PreparedKind::Utility` |
| 3 | DDL inside `BEGIN … COMMIT` | Ecto: every migration runs in a transaction | ~~`25001`~~ **Done (W3):** buffered, visible inside the block, replayed at `COMMIT` (`ddl_txn`; failure semantics in the crate README) |
| 4 | `LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE` | Ecto migration lock | ~~Not parsed~~ **Done (W3):** blocking on a standalone node, read-set conflict (`40001`) on a cluster (`lock`) |
| 5 | `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`, `'x'::schema.type` casts, `pg_type` rows for the enum and its array | Oban schema | ~~No user types~~ **Done (W3):** enum DDL, validated enum columns, `pg_type` rows; `::enum` / `::regnamespace` casts await W1's cast engine (resolvers provided) |
| 6 | `bigserial`, `nextval`; `DEFAULT` expressions: literal, `ARRAY[]::jsonb[]`, `timezone('UTC', now())` | Oban schema and inserts | ~~No sequences (`SERIAL` refused)~~ **Done (W3):** `bigserial` recorded per D15, ids minted by `serial_id` (applied at INSERT by W2); defaults: see row 9 / W2 |
| 7 | `jsonb[]`, `text[]` columns and array parameters (`'{a,b}'`) | Oban schema and runtime | Array column types |
| 8 | `CREATE INDEX [IF NOT EXISTS] … [WHERE …] [USING gin]`, `DROP INDEX IF EXISTS` | Oban schema | Not parsed |
| 9 | `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN … TYPE / SET DEFAULT / SET NOT NULL`, `ADD/DROP CONSTRAINT … CHECK … [NOT VALID]`, `SET UNLOGGED` | Oban schema | ~~`ALTER` not supported (T-132b)~~ **Done (W3):** all census forms; `CHECK` enforced via W1's engine; DEFAULTs recorded (applied at INSERT by W2) |
| 10 | `COMMENT ON TABLE … IS '12'` and `pg_catalog.obj_description(oid, 'pg_class')` | Oban migration versioning | ~~No comments, no `pg_description`~~ **Done (W3):** `COMMENT ON TABLE`, streamed `pg_description`. `obj_description(...)` itself awaits W1 (function calls in projections) |
| 11 | `DO $$ … $$`, `CREATE FUNCTION … LANGUAGE plpgsql`, `CREATE TRIGGER`, `DROP FUNCTION/TRIGGER` | Stock Oban migrations v1–v10; dropped again by v11 | No procedural language |
| 12 | `INSERT … RETURNING "id"` with a sequence default, jsonb and enum and array parameters | `Oban.insert` | Sequence default; enum and array values |
| 13 | `WITH s AS (SELECT … ORDER BY … LIMIT $n FOR UPDATE SKIP LOCKED) UPDATE t SET … FROM s WHERE t.id = s.id AND … RETURNING …` | Oban fetch (every poll) | CTE, `FOR UPDATE SKIP LOCKED`, `UPDATE … FROM`, `UPDATE … RETURNING`, non-key predicates |
| 14 | `UPDATE t SET … WHERE id = $1 AND state = ANY($2) AND attempted_at = $3` | Oban ack (every job) | `= ANY($n)` over an array parameter; conditional update on non-key columns |
| 15 | Stager, pruner, lifeline: `UPDATE … WHERE id IN (SELECT … LIMIT)`, `DELETE … WHERE id IN (SELECT … LIMIT)`, `array_append` | Oban plugins (scheduled and retried jobs, cleanup) | Subquery `IN`, `array_append` |

## Decisions needed before implementation

* **Schema path.** Run Oban's stock migrations (needs #11: PL/pgSQL `DO` blocks,
  functions and triggers), or have the application create Oban's final v12 schema in its
  own migration (Oban checks only the version comment, #10).
* **Cluster scope.** `FOR UPDATE SKIP LOCKED` needs row ownership. Local writer ownership
  exists; distributed writer ownership does not yet (cluster `READ COMMITTED` is `0A000`).
* **Sequences.** A cluster-safe `nextval` (an Accord- or CAS-backed allocator handing out
  blocks), or a unique non-dense 64-bit id.

## Decisions taken (2026-11-08)

* D12: stock Oban migrations run unchanged.
* D13: PL/pgSQL through the UDF substrate plus a host interface.
* D14: `FOR UPDATE SKIP LOCKED` relies on Accord.
* D15: `bigserial` gives unique 64-bit ids.

Details are in `decisions.md`.

## Program

Measured against the engine on `main` (v0.35.1).

### W1 — Expression engine (`ferrosa-sql`)

**Foundation; everything else depends on it.**

Today a predicate is only `column <op> literal | $n`, with no projected expressions, no
functions, no subqueries, one join and no CTEs.

* Scalar expressions in `SELECT`, `WHERE`, `SET`, `ON`, `RETURNING` and `DEFAULT`:
  * column-to-column comparison, arithmetic and casts;
  * `coalesce`, `now`, `timezone`, `char_length`, `mod`, `array_append`,
    `json_build_object`;
  * `CASE`, `IN (list)`, `= ANY(array)`, `IS [NOT] NULL`.
* `EXISTS`, scalar subqueries and `ARRAY(subquery)`, including correlated ones.
* N-way `LEFT`/`INNER` joins with arbitrary `ON`.
* `WITH` (non-recursive CTEs).
* Parameter type inference for `Describe` over all of the above.

Size: roughly 4–6k lines.

### W2 — DML

* `UPDATE`/`DELETE` with arbitrary `WHERE`: read the matching rows, then write them by
  key inside the transaction.
* `UPDATE … FROM`, and `RETURNING` on `UPDATE`/`DELETE`.
* Column defaults, and D15 ids.
* `FOR UPDATE [SKIP LOCKED|NOWAIT]` accepted per D14.

Size: roughly 2–3k lines.

### W3 — DDL and catalog

* DDL over the extended protocol.
* Transactional DDL: buffered until `COMMIT` and visible inside its own transaction.
* Enum types: `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`, casts, and `pg_type` rows
  for the enum and its array.
* `text[]` and `jsonb[]` columns.
* `ALTER TABLE` variants: enforced `CHECK`, `NOT VALID`, `SET UNLOGGED`.
* `CREATE`/`DROP INDEX`, including partial, `USING gin` and `DESC`.
* `COMMENT ON`, `pg_description` and `obj_description`.
* `LOCK TABLE`.
* `pg_range`, so the Postgrex bootstrap can run.

Size: roughly 3–4k lines.

### W4 — PL/pgSQL (D13)

* The subset's parser.
* The `DO` interpreter.
* The PL/pgSQL → AssemblyScript converter.
* The WIT host interface: query, execute and notify, inside the calling transaction.
* Triggers: stored, fired from the write path, with `NEW`, `OLD` and `TG_OP`.
* `LISTEN/NOTIFY`: wire `NotificationResponse`, plus delivery between nodes.

Size: roughly 5–7k lines.

### Lean

Under `specs/lean4/Ferrosa/Sql/`, one model per feature, written before its code:

| Model | Covers |
|---|---|
| `Expr` | Evaluation and three-valued logic; subquery semantics |
| `Dml` | A read-then-write update equals the declarative update under the snapshot |
| `Ddl` | Transactional DDL: commit applies all of it, rollback none |
| `Plpgsql` | Interpreter and compiled function agree on the subset |
| `Notify` | Delivered at most once, in commit order |

### Order and PRs

| Step | Delivers | Proven by |
|---|---|---|
| 1 | W1 expressions and subqueries | Postgrex connects: census statement 1 |
| 2 | W3 (DDL and catalog) together with W4 (PL/pgSQL) | `Oban.Migration.up(version: 12)` applies |
| 3 | W2 (DML) | `Oban.insert`, then claim, run and acknowledge |

**Acceptance:** the Elixir probe (`Postgrex` → `Ecto.Migrator` → `Oban`) runs one job
against ferrosa.

## Found while building W1

* **OID columns are `int4`.** The `pg_catalog` projections type every OID column as
  `ColumnType::Int` (`int4`, OID 23), but synthetic OIDs lie in `[16384, u32::MAX]`.
  * Text results are correct.
  * In binary results an OID at or above 2³¹ fails to encode, because `int4` is signed.
  * Fix: an `oid` column type (OID 26, unsigned 4-byte binary).
  * Status: FIXED. `ColumnType::Oid` (unsigned 4-byte binary, text and Bind), every catalog OID column typed `oid`. Lean: `Sql/Oid.lean`. Tests: `an_oid_above_two_to_the_31_encodes_and_decodes_as_unsigned`, `catalog_oid_columns_are_typed_oid`.
* **Operator chains escaped the depth limit.** Fixed (2cb904c3): a long
  `AND`/`+`/`||` chain built a tree as deep as the chain, and every recursive walk
  of it could overflow a worker stack. Each link now counts against
  `MAX_EXPR_DEPTH`, and recursion grows the stack on demand. Lean:
  `Expr.lean` `accepted_chain_depth_bounded`, `deepenLoop_refuses`. Tests:
  `parser_depth.rs` `long_*_chain`.
* **Unqualified catalog names.** Fixed: `pg_type` meant `public.pg_type`.
  PostgreSQL searches `pg_catalog` first, so `catalog::parse_pg` now qualifies
  catalog relations (a CTE of the same name still shadows them). Test:
  `unqualified_catalog_relations_resolve_in_pg_catalog`.
* **`pg_type` had only two columns and listed only used types.** Fixed:
  * every built-in type is listed, with PostgreSQL's I/O function names,
    `typelem`, `typrelid` and `typbasetype`;
  * `pg_attribute` now has `attisdropped`.
  * Postgrex picks its codecs from these columns.
* **`LIMIT $n` was not parsed.** Fixed: `LIMIT`/`OFFSET` take `$n` (and `LIMIT ALL`); NULL is no bound, a negative value is 2201W/2201X. Lean: `Limit.lean`.
* **A no-`FROM` select uses the narrow scalar grammar.**
  * `SELECT coalesce($1, 0)` and `SELECT pg_notify(...)` fail to parse.
  * Route them to the general planner over a single empty row.
  * Open, W1.
* **An uncorrelated subquery is re-run for every outer row.** Correct but slow.
  Cache the result of a subquery that reads no outer column. Open, W1 (perf).
* **Not implemented yet; each is refused loudly (0A000):**
  * numeric division;
  * `AT TIME ZONE` with a zone other than UTC;
  * `obj_description` (it needs W3's `pg_description`);
  * `json_build_object`.

  Open, W1. Wire them up as the Oban statements need them.
* **`x IN (SELECT …)` was read as a scalar subquery inside a list.** It failed
  with 21000 once the subquery returned more than one row. Fixed: `IN (subquery)`
  and `op ANY (subquery)` now lower to `op ANY (ARRAY(subquery))`, which is how
  PostgreSQL defines them; `NOT IN` is the negation. Lean:
  `anyLoop_is_anyEq` and `anyLoop_true_absorbs` (the evaluator's early-exit
  fold equals `anyEq`).
* **`array_append` is missing** (census row 15). Open, W1/W2.

## Found while building W3

Bugs found in existing Ferrosa code while building this programme. Each is fixed in the
workstream that found it, with a failing test first and a Lean model; the row records the
fixing commit.

| Found by | Bug | Status |
|---|---|---|
| W3 | Enum `ADD VALUE` (as first built in W3, 18ec1c1a) rewrote the whole label list, so two concurrent adds of different labels lost one. Now one key per added label; test `pg_enum::tests::concurrent_add_values_both_survive`, Lean `Ferrosa.Sql.Enum.concurrent_adds_both_survive` | Fixed in 72a8ecfc |
| W3 | `CREATE TABLE … col type NOT NULL` dropped the `NOT NULL` of every non-key column (T-132a), so a NULL was stored silently. Now recorded (`pg.not_null.<col>`) and enforced on INSERT (omitted or NULL) and UPDATE: `23502`. Test `not_null_on_a_regular_column_is_enforced`; Lean `Ferrosa.Sql.Constraint` (`unenforced_not_null_stores_a_null`, `admitted_writes_preserve_constraints`) | Fixed in 44224a7a |
