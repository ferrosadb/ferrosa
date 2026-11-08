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
| 1 | Type bootstrap: `ARRAY(SELECT …)`, three-way `LEFT JOIN`, `coalesce`, `NOT EXISTS`, `OR` in a join condition, `pg_range` | Postgrex, every connect | Scalar subquery `ARRAY(…)`; more than one join; `NOT EXISTS`; `pg_range` projection |
| 2 | `CREATE TABLE IF NOT EXISTS … ` over the **extended** protocol | Ecto | Extended-protocol `Parse` of DDL is refused |
| 3 | DDL inside `BEGIN … COMMIT` | Ecto: every migration runs in a transaction | `25001` |
| 4 | `LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE` | Ecto migration lock | Not parsed |
| 5 | `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`, `'x'::schema.type` casts, `pg_type` rows for the enum and its array | Oban schema | No user types |
| 6 | `bigserial`, `nextval`; `DEFAULT` expressions: literal, `ARRAY[]::jsonb[]`, `timezone('UTC', now())` | Oban schema and inserts | No sequences (`SERIAL` refused); defaults |
| 7 | `jsonb[]`, `text[]` columns and array parameters (`'{a,b}'`) | Oban schema and runtime | Array column types |
| 8 | `CREATE INDEX [IF NOT EXISTS] … [WHERE …] [USING gin]`, `DROP INDEX IF EXISTS` | Oban schema | Not parsed |
| 9 | `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN … TYPE / SET DEFAULT / SET NOT NULL`, `ADD/DROP CONSTRAINT … CHECK … [NOT VALID]`, `SET UNLOGGED` | Oban schema | `ALTER` not supported (T-132b) |
| 10 | `COMMENT ON TABLE … IS '12'` and `pg_catalog.obj_description(oid, 'pg_class')` | Oban migration versioning | No comments, no `pg_description` |
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
  * Status: on W1's work list. Under the owner's rule, a found bug is fixed with a
    test and a Lean spec before the work is done.
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
