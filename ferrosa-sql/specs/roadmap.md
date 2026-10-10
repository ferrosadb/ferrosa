---
crate: ferrosa-sql
doc: roadmap
last_updated: 2026-10-09
---

# ferrosa-sql — Roadmap

Sourced from the supported-subset review, the FMEA gaps ([fmea.md](fmea.md)), and
the `ferrosa-postgres` consumer needs. The engine is intentionally an M1 slice
(D3, bespoke, no DataFusion); the roadmap is mostly *widening the SQL surface*
toward the Postgres queries real clients send.

## Now (highest value)

- **(done) `::` cast (`expr::type_name`), sufficient for pgbench's object-existence
  query.** `SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass`
  is what `pgbench -i` runs to check whether a table exists; before this the lexer
  had no token for `:` and the query died with `bad token: :`. The lexer now yields
  ONE `Tok::Cast` for `::` (a lone `:` stays a loud `bad token: :`, mirroring the
  `||`/`|` precedent). It is a POSTFIX operator — `oid = $1::regclass` is
  `oid = ($1::regclass)`, and `'a' || 'b'::regclass` is `'a' || ('b'::regclass)` —
  so it binds tighter than both the comparison operators and `||`; that precedence
  is pinned by `the_pgbench_object_existence_query_parses_a_regclass_cast` and
  `a_cast_binds_tighter_than_string_concatenation`. The name may be bare or
  schema-qualified (`pg_catalog.regclass`), and the target is recorded as
  `Term::Cast` / `ScalarValue::Cast` (`CastTarget`). The parser only *records* the
  cast: its **semantics are the front end's** (`ferrosa-postgres::query::resolve_casts`
  and `catalog::resolve_regclass`), because the `pg_class.oid` scheme is
  PostgreSQL-specific and this crate owns no catalog. Only `regclass` is implemented.
  **Refused by name, never accepted-and-ignored**: any other target is
  `ParseError::UnsupportedCast(<type as written>)` (`0A000`), and the `CAST(x AS t)`
  spelling is `ParseError::UnsupportedCastExpr` (`0A000`) rather than mis-parsed as a
  column named `CAST` (a bare `cast` column is still allowed). A cast that reaches the
  pure engine unrewritten is `ExecError::UnresolvedCast` (`0A000`), not a no-op.

- **(done) Scalar subqueries `( SELECT ... )` in the no-`FROM` select list.** An
  LParen followed by `SELECT` in `parse_scalar_primary` begins a scalar subquery
  operand (`ScalarValue::Subquery`), closed by the matching RParen; the inner
  select reuses the full table-`SELECT` grammar (`parse_select_stmt`). The
  front end (`ferrosa-postgres::query::eval_scalar_subquery`) runs the inner query
  and takes its single value with PostgreSQL `EXPR_SUBLINK` semantics: no rows ⇒
  NULL (distinct from the empty string), more than one row ⇒ `21000`
  `cardinality_violation`, more than one output column ⇒ `42601` refused *before*
  any row. The subquery's column type is the inner query's single output column
  type (so `count(*)` types as int), and `||` still coerces it to text. This is
  what makes pgbench's census line
  `select (select count(*) from pgbench_accounts)||'|'||…` evaluate. An LParen not
  followed by `SELECT` (`SELECT (1)`) is still refused.

  **NOT supported, and refused by name (`0A000`)**: `||` over a `FROM` relation
  (`SELECT name || '!' FROM t`) and every other select-list expression form
  (arithmetic, function calls over columns, `CASE`). A scalar subquery is only a
  no-`FROM` select-list operand: it does not appear in a `FROM` relation's
  projection, in `WHERE`/`HAVING`, in `VALUES`, or nested in another subquery's
  projection, and those forms are refused at parse (or, in DML values, by
  `substitute_param`). The reason is structural, not an omission: the planner's
  projection is a `Vec<usize>` of column indices
  (`plan.rs::simple_projection` + `exec::try_project`), not computed cells, so
  expressions over a relation need a real select-list expression tree, a new
  projection operator, an aggregate-mode `Slot` variant, authz walking, and a
  `Value`-to-text renderer. `WHERE`/DML grammar untouched.
- **(done) Parse the maintenance statements** (pgbench `-i`/reset). `TRUNCATE
  [TABLE] t [, …]`, `VACUUM [FULL] [ANALYZE|ANALYSE] [t]` and `ANALYZE|ANALYSE
  [t]` now parse to `Statement::{Truncate,Vacuum,Analyze}`; the Postgres front
  end executes them (replicated `TRUNCATE`; `VACUUM` flushes and submits
  compaction; `ANALYZE` is an accepted no-op).
  `TRUNCATE … CASCADE` / `… RESTART IDENTITY` are refused by name.

- **(done) `FOREIGN KEY` / column `REFERENCES` grammar.** `[CONSTRAINT <name>]
  FOREIGN KEY (<cols>) REFERENCES <parent> [(<pcols>)]` (table level) and a column-level
  `REFERENCES` parse into `ForeignKeyConstraint`, recorded on
  `CreateTableStmt::foreign_keys` and `AlterOperation::AddForeignKey`; `ALTER TABLE …
  ADD [CONSTRAINT <name>] FOREIGN KEY …` is accepted. The referenced-column list is
  optional (`parent_columns: None` ⇒ the parent's primary key; the executor resolves it).
  The grammar accepts only what the front end enforces — the default `NO ACTION` and
  `RESTRICT` — and refuses `ON DELETE`/`ON UPDATE CASCADE`/`SET NULL`/`SET DEFAULT`,
  `MATCH FULL`/`PARTIAL`, `DEFERRABLE`, `INITIALLY`, `NOT VALID` **by name**
  (`ParseError::UnsupportedAlter`). `ALTER TABLE` has no `expect_end`, so the referential
  tail must be validated explicitly; it is. The five statements `pgbench -i
  --foreign-keys` emits are pinned by `the_five_pgbench_foreign_keys_parse`; ENFORCEMENT
  (the index-backed child-side and parent-side checks) lives in `ferrosa-postgres`, which
  enforces the `ALTER TABLE … ADD FOREIGN KEY` form and refuses a `CREATE TABLE`-time
  `FOREIGN KEY` by name (`0A000`) rather than accept one it cannot enforce.

- **(done) Stream the result to the wire** (FMEA SQL-12, `t_f348ba0b`).
  `execute_streaming` + `RowSink` deliver rows as the pipeline yields them and
  the Postgres front end forwards them with backpressure. Follow-up: retire the
  collecting `execute`/`execute_with` once no test or tool needs a `Vec`.

- **`IS NULL` / `IS NOT NULL`** (FMEA SQL-1). Add the `IS`/`NOT NULL` tokens and
  grammar plus an `IsNull` predicate path. Today NULL filtering is impossible in
  SQL because `col = NULL` is UNKNOWN under Kleene logic. Top gap.
- **Implicit string → typed coercion in predicates** (FMEA SQL-8). Coerce a
  `Text` RHS to the column's type (`Date`/`Timestamp`/`Numeric`/`Inet`) at bind
  time so naturally-written `where d = '2024-01-01'` works, not only
  `DATE '2024-01-01'`. Fail loud on an uncoercible body.
- **Integer SUM/AVG overflow safety + `Numeric` aggregation** (FMEA SQL-6).
  Promote integer running sums to `BigInt`/checked add and feed `Numeric`
  columns through `add_numeric`.

## Next

- **PG DDL execution** (D10): T-130 parses `CREATE TABLE`; T-132a creates the
  schema. `DROP TABLE`, `ALTER TABLE ADD/DROP COLUMN` and `CREATE/DROP INDEX`
  are separate packets. The PG type map is a fourth string-to-type table and
  must converge with the existing three (research/type-threading-map.md).

- **jsonb** (D11): T-160 added the value and column types (done). Next: the
  wire codec (T-161a), jsonb literal parsing and casts, operators and SRFs
  (T-162 onward), and a real `jsonpath` value in place of the text holder.

- **Richer joins** (FMEA SQL-3): `LEFT`/`RIGHT`/`FULL` outer joins, a multi-table
  FROM / join list, and `ON` predicates beyond a single `a = b` (AND-of-equalities,
  inequality join conditions).
- **Scalar expressions** (FMEA SQL-4): an expression evaluator over `Value` for
  projection arithmetic/functions (`a + b`, `UPPER(x)`, `||`, `CASE`) and
  expression predicates (LHS/RHS richer than column-vs-literal).
- **Set-based DML** (FMEA SQL-7): multi-row `INSERT ... VALUES`, range/`IN`
  predicates in `UPDATE`/`DELETE` WHERE.
- **Bounded operators** (FMEA SQL-5): cap or spill `hash_join` build side, `sort`,
  and `hash_aggregate` input to satisfy Power-of-10 rule 3; lean on
  predicate/projection pushdown into the storage-backed provider.

## Later

- **Subqueries, CTEs (`WITH`), `UNION`/`INTERSECT`/`EXCEPT`, window functions**
  (FMEA SQL-2) — each a separate large effort; sequence by `ferrosa-postgres`
  client demand.
- **Collections / UDT / tuple / vector value types** (FMEA SQL-10) once a
  consuming query needs them over this path.
- **Property-test the round-trip and Kleene tables** as a regression net
  independent of `ferrosa-postgres`.

## Non-goals

- Postgres wire framing, transaction execution (Accord), and storage/S3 framing —
  those belong to `ferrosa-postgres` and the storage layer, not here.
- Embedding DataFusion / Arrow (decision **D3**) — the bespoke value model and
  semantics stay owned in this crate.
