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
| 7 | `jsonb[]`, `text[]` columns and array parameters (`'{a,b}'`) | Oban schema and runtime | ~~Array column types~~ **Done (W3):** frozen-list storage, text and binary array input; NULL elements refused |
| 8 | `CREATE INDEX [IF NOT EXISTS] … [WHERE …] [USING gin]`, `DROP INDEX IF EXISTS` | Oban schema | ~~Not parsed~~ **Done (W3):** single-column btree/hash built; composite/partial/DESC/gin recorded, not used for planning |
| 9 | `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN … TYPE / SET DEFAULT / SET NOT NULL`, `ADD/DROP CONSTRAINT … CHECK … [NOT VALID]`, `SET UNLOGGED` | Oban schema | ~~`ALTER` not supported (T-132b)~~ **Done (W3):** all census forms; `CHECK` enforced via W1's engine; DEFAULTs recorded (applied at INSERT by W2) |
| 10 | `COMMENT ON TABLE … IS '12'` and `pg_catalog.obj_description(oid, 'pg_class')` | Oban migration versioning | ~~No comments, no `pg_description`~~ **Done (W3):** `COMMENT ON TABLE`, streamed `pg_description`. `obj_description(...)` itself awaits W1 (function calls in projections) |
| 11 | `DO $$ … $$`, `CREATE FUNCTION … LANGUAGE plpgsql`, `CREATE TRIGGER`, `DROP FUNCTION/TRIGGER` | Stock Oban migrations v1–v10; dropped again by v11 | **W4 done (feat/pg-w4-plpgsql)**, statement by statement under "W4 status" below. Still waiting on W1/W3: the DO guard's `'public'::regnamespace`, `CREATE TYPE … AS ENUM`, and DDL inside a migration's transaction (25001 until transactional DDL lands) |
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

#### W4 status (2026-11-08, feat/pg-w4-plpgsql)

| Census statement | Today |
|---|---|
| `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE … 'public'::regnamespace::oid) THEN CREATE TYPE … END IF; END$$` | The DO block parses and interprets. Its EXISTS guard reads at most one row. The guard stops at `'public'::regnamespace` (0A000, W1/W3); the CREATE TYPE is W3's; DDL inside the block's transaction is 25001 until W3. Pending test: `awaits_w1_w3_the_census_do_block_creates_the_enum` |
| `CREATE OR REPLACE FUNCTION "public".oban_jobs_notify() … LANGUAGE plpgsql` (v1 and v2) | Stored and compiled. Fires: the v1 body runs unchanged and delivers its JSON payloads (`the_oban_v1_notify_trigger_notifies_on_insert_and_on_state_change`, on a text `state` column). Inside a migration transaction: 25001 until W3 |
| `DROP TRIGGER IF EXISTS oban_notify ON "public".oban_jobs` | Works |
| `CREATE TRIGGER oban_notify AFTER INSERT [OR UPDATE OF state] … EXECUTE PROCEDURE …` | Stored with the table; fires once per row inside the transaction |
| `CREATE OR REPLACE FUNCTION "public".oban_wrap_id(value bigint) RETURNS int … IMMUTABLE` | Stored and compiled. Calling it from SQL is not wired (the census never calls it) |
| `DROP FUNCTION IF EXISTS … oban_wrap_id(value bigint)` / `… oban_jobs_notify()` | Works. Refused with 2BP01 while a trigger depends on the function |
| `pg_notify` / `NOTIFY` / `LISTEN` | Delivered at commit, in commit order, at most once. Node-local: a cluster node refuses LISTEN with 0A000 |

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
* **An uncorrelated subquery was re-run for every outer row.** Fixed: the planner
  marks a subquery correlated exactly when a column inside it resolves to an
  enclosing query, and memoises the uncorrelated ones per statement. Lean:
  `memo_is_sound`, `uncorrelated_iff_no_outer_reference`. Test:
  `an_uncorrelated_subquery_is_scanned_once_however_many_outer_rows`.
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
* **A type mismatch in a comparison was silently UNKNOWN.** Fixed, in both the
  legacy and the general planner. `WHERE ts > '2020-01-01'` compared a timestamp
  with a string, got UNKNOWN on every row, and returned nothing.
  * A string literal is now read as the type it faces, at plan time: timestamp,
    uuid, integer and so on. An invalid literal is a 22007 or 22P02 error.
  * Two typed operands of different families are refused (42883), at plan time
    and at run time.
  * Text-to-timestamp casts now parse ISO input. Only a zero UTC offset is
    accepted; any other offset is 0A000.
  * Lean: `strict_unknown_only_from_null`, `strict_agrees_on_same_type`,
    `strict_refuses_mismatch`.
  * Tests: `a_string_literal_compared_with_a_timestamp_is_read_as_a_timestamp`,
    `a_literal_that_cannot_be_the_other_type_is_an_error_not_unknown`,
    `comparing_two_typed_values_of_different_types_is_an_error_not_unknown`.
* **A no-`FROM` select only had the narrow scalar grammar.** Fixed: what the
  scalar path cannot read is a general select over one empty row. Covers
  PL/pgSQL's `SELECT <expr>` with `$n` bindings (W4). Test:
  `plpgsql_expression_statements_evaluate`.


## Found while building W3

Bugs found in existing Ferrosa code while building this programme. Each is fixed in the
workstream that found it, with a failing test first and a Lean model; the row records the
fixing commit.

| Found by | Bug | Status |
|---|---|---|
| W3 | Enum `ADD VALUE` (as first built in W3, 18ec1c1a) rewrote the whole label list, so two concurrent adds of different labels lost one. Now one key per added label; test `pg_enum::tests::concurrent_add_values_both_survive`, Lean `Ferrosa.Sql.Enum.concurrent_adds_both_survive` | Fixed in 72a8ecfc |
| W3 | `CREATE TABLE … col type NOT NULL` dropped the `NOT NULL` of every non-key column (T-132a), so a NULL was stored silently. Now recorded (`pg.not_null.<col>`) and enforced on INSERT (omitted or NULL) and UPDATE: `23502`. Test `not_null_on_a_regular_column_is_enforced`; Lean `Ferrosa.Sql.Constraint` (`unenforced_not_null_stores_a_null`, `admitted_writes_preserve_constraints`) | Fixed in 44224a7a |

## Found while building W4

| # | Found | Fix | Commit |
|---|---|---|---|
| 1 | The census fixture stores each statement on one line, so the capture flattened newlines. In Oban's v1 notify function that put the rest of the body inside a `--` comment, and a replay would send a function with no `END`. Found by `ferrosa-plpgsql/tests/census.rs`. | The fixture's comment is now `/* … */`, which PostgreSQL reads identically. Lean: `flattening_a_line_comment_swallows_code`, `a_block_comment_survives_flattening` | fixed in ff51578c |
| 2 | A trigger whose write fires itself overflowed the stack before the depth bound was reached: each nesting level lived on the stack. | Each level is boxed, so the bound ends a cascade with 54001. Test: `a_trigger_that_fires_itself_stops_at_the_depth_bound`. The bound is the Lean trigger model's assumption; once-per-row is `fires_exactly_once` | fixed in 49396d2f |
| 3 | A multi-statement simple query (`BEGIN; NOTIFY …; COMMIT` in one `Query`) is refused with a parse error. PostgreSQL runs each statement. Postgrex and Ecto never send one. | Not fixed: this is the general simple-query dispatcher, which belongs to no W4 file. Needs a decision on who owns it | open: outside W4 |

