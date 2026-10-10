# ferrosa-sql

> The bespoke relational query engine (parser + planner + physical operators)
> behind Ferrosa's Postgres front-end. **No DataFusion / Arrow** (decision **D3**)
> — it owns its own value model, three-valued NULL logic, and C-collation
> ordering.

## What this crate is

A self-contained relational engine for a **deliberately small SQL subset**: a
hand-written lexer + recursive-descent parser, a binder/planner that resolves
references against a `Catalog`, and a set of Volcano-style physical operators
(scan, filter, project, hash join, sort, hash aggregate, limit/offset). It was
written from scratch rather than embedding DataFusion (decision **D3**) so the
type model, NULL semantics, and row ordering are owned and auditable end-to-end.

`ferrosa-postgres` lowers each incoming SQL statement onto this engine. The
engine knows nothing about the Postgres wire protocol, transactions, or storage
framing — it operates over an abstract `TableProvider` / `Catalog`, backed by an
in-memory table in tests and by Ferrosa storage in production.

## What's implemented

**Parser** (`parse`, `parse_statement`):

- Boolean expressions nest (parentheses, `NOT`) at most `MAX_EXPR_DEPTH`
  (256) levels; deeper is `ParseError::TooDeep` (PG `54001`). The parser
  recurses per level, so an unbounded depth overflows the worker stack and
  aborts the process (`tests/parser_depth.rs`).

- `SELECT [DISTINCT] <* | items> FROM t [alias] [INNER JOIN t2 [alias] ON a.x = b.y]
  [WHERE <bool-expr>] [GROUP BY ...] [HAVING <bool-expr>]
  [ORDER BY ... [ASC|DESC]] [LIMIT n] [OFFSET m]` — one inner equi-join only.
- No-`FROM` scalar selects: `SELECT 1`, `SELECT version()` (zero-arg func),
  `SELECT $1`, `SELECT TRUE`, `||` string concatenation over those scalars
  (`SELECT 'a' || 'b'`, `SELECT 'a' || $1 || current_database()`), a postfix
  `::type_name` cast (`SELECT 'kv'::regclass`), and a **scalar
  subquery** `( SELECT ... )` as an operand
  (`SELECT (SELECT count(*) FROM t) || '|'`). `||` is left-associative, evaluates
  to `text`, and propagates NULL (NULL on either side ⇒ NULL, which is *not* the
  empty string). A scalar subquery evaluates to its inner query's single value
  with PostgreSQL `EXPR_SUBLINK` semantics: no rows ⇒ NULL, more than one row ⇒
  `21000 cardinality_violation`, more than one output column ⇒ refused by name
  (`42601`); its column type is the inner query's single output column type. The
  `SELECT` list is still not a general expression grammar: `||` over a `FROM`
  relation (`SELECT name || '!' FROM t`), parenthesised non-subquery scalars
  (`SELECT (1)`), and every other select-list expression form is refused by name
  (`ParseError::UnsupportedSelectExpr`). A subquery outside the no-`FROM` select
  list (a `FROM` relation's projection, `WHERE`, `VALUES`) is not parsed.
- DML (single-row, key-equality WHERE): `INSERT INTO t (cols) VALUES (...)`,
  `UPDATE t SET ... WHERE k = v [AND ...]`, `DELETE FROM t WHERE k = v [AND ...]`.
- **`COPY t [(cols)] FROM STDIN [[WITH] (<options>)]`** → `Statement::CopyFromStdin` (the
  payload arrives later as `CopyData` frames, so `ferrosa-postgres` drives it). Options are the
  parenthesised form: `FORMAT (text|csv)`, `DELIMITER '<c>'`, `NULL '<s>'` and `HEADER` are
  applied; `FREEZE [ON|OFF]` is **accepted and recorded but not applied**
  (`CopyFromStdinStmt::freeze`) — PostgreSQL freezes the loaded rows into heap pages and ferrosa
  is an LSM with no heap pages and therefore no frozen-row concept, so the option has no
  analogue and is accepted for client compatibility only. `pgbench -i` writes
  `with (freeze on)` for every ordinary table on PostgreSQL v14+, so this is the option that lets
  it load. Every other option name, a bad option *value*, and `COPY ... TO` are refused **by
  name** as a COPY refusal (`ParseError::UnsupportedCopy`, whose message names the option) —
  never reported as an ALTER TABLE form, which is what `ParseError::UnsupportedAlter` means.
- PG DDL (T-130, D10), **parsed only** (no execution or schema creation yet; the
  Postgres front end answers `0A000`): `CREATE TABLE [IF NOT EXISTS] [public.]t
  (col type [NOT NULL | PRIMARY KEY], ..., [CONSTRAINT n] PRIMARY KEY (a, b))`
  into `Statement::CreateTable`. Types: smallint/int/bigint, real/double
  precision, numeric(p,s), boolean, text, varchar(n), bytea, uuid, date, time,
  timestamp[tz], inet, `jsonb`, `json` (`PgType::storage()` maps `json` to
  `jsonb`, D11). Double-quoted identifiers are supported. Refused by name with
  `ParseError::UnsupportedClause`: `CHECK`,
  `SERIAL` types, `DEFAULT`, a schema other than `public`, `UNIQUE`. An optional
  trailing `WITH (key = value, ...)` storage-parameter clause is parsed and each
  accepted pair is recorded on `CreateTableStmt::storage_parameters` — `fillfactor`
  and `autovacuum_enabled` are PostgreSQL physical-layout / background-maintenance
  hints ferrosa has no equivalent for (it is an LSM/SSTable store: no heap pages, no
  autovacuum), so they are recorded but not applied. Any other option name is refused
  by name (`ParseError::UnsupportedStorageParameter`, `0A000`) rather than dropped;
  pgbench's `create table ... with (fillfactor=100)` is the motivating case.
- **`FOREIGN KEY` / column `REFERENCES` grammar** (parsed here; **enforced** by the
  Postgres front end — see `ferrosa-postgres`'s README and roadmap). A table-level
  `[CONSTRAINT <name>] FOREIGN KEY (<cols>) REFERENCES <parent> [(<pcols>)]` and a
  column-level `REFERENCES` both parse into `ForeignKeyConstraint` and are recorded on
  `CreateTableStmt::foreign_keys` / `AlterOperation::AddForeignKey`. The referenced-column
  list is optional: `REFERENCES parent` leaves `parent_columns` `None`, meaning the
  parent's primary key — the executor resolves it, so the AST records only what was
  written. **An FK that parses but is not enforced is a lie**: the grammar accepts only
  the referential actions the front end actually implements — the default **NO ACTION**
  and its immediate equivalent **RESTRICT** — and refuses `ON DELETE`/`ON UPDATE`
  `CASCADE`, `SET NULL`, `SET DEFAULT`, `MATCH FULL`/`PARTIAL`, `DEFERRABLE`, `INITIALLY`
  and `NOT VALID` **by name** (`ParseError::UnsupportedAlter`), so a client that asked to
  CASCADE never receives a NO ACTION constraint in its place. The five statements
  `pgbench -i --foreign-keys` emits are pinned by
  `the_five_pgbench_foreign_keys_parse`. `ferrosa-postgres` **enforces** the
  `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` form and **refuses a `CREATE TABLE`-time
  `FOREIGN KEY` / column `REFERENCES` by name** (`0A000`) rather than accepting a constraint
  it cannot enforce — so a parsed FK is never silently left unchecked.
- Maintenance statements **parsed** (executed by the Postgres front end):
  `TRUNCATE [TABLE] t [, …]` into `Statement::Truncate`; `VACUUM [FULL]
  [ANALYZE|ANALYSE] [t]` into `Statement::Vacuum`; `ANALYZE|ANALYSE [t]` into
  `Statement::Analyze`. `TRUNCATE … CASCADE` / `… RESTART IDENTITY` are refused
  by name (no foreign keys, no sequences to honour).
- jsonb values (T-160): `Value::Jsonb(ferrosa_jsonb::JsonbValue)`, plus
  `Value::JsonPath(String)` (text until the path parser, T-162) and
  `Value::TextArray`; `ColumnType` gains `Jsonb`, `Json`, `JsonPath`, `TextArray`.
  Equality, hashing, GROUP BY, DISTINCT and hash-join keys use jsonb value
  equality (`1` == `1.0`); `sql_cmp` and `spill::canonical_cmp` use the D18
  order. Spill records carry the validated cell (base64 in JSON, validated on
  read) and `row_bytes` counts the real cell bytes. Operators, casts, SRFs and
  parsing jsonb literals are later packets.
- Transaction / session statements **parsed** (not executed here): `BEGIN`/`START`,
  `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `SET`, `RESET`.
- WHERE/HAVING boolean expressions: `AND` / `OR` / `NOT` with parentheses and
  the six comparison operators `= != <> < <= > >=`. RHS is a literal, a `$N`, or
  either followed by a postfix `::type_name` cast (`oid = $1::regclass`).
- **`::` cast (`expr::type_name`).** A POSTFIX cast, so it binds tighter than the
  operator beside it: `oid = $1::regclass` is `oid = ($1::regclass)`, and
  `'a' || 'b'::regclass` is `'a' || ('b'::regclass)`. The name may be bare
  (`::regclass`) or schema-qualified (`::pg_catalog.regclass`). The parser records
  the target (`CastTarget`); its **semantics** are the front end's (see
  `ferrosa-postgres`): `::regclass` resolves a relation name to the `pg_class.oid`
  of the relation it names, so `oid = $1::regclass` compares like PostgreSQL and an
  unresolvable name is an error, never a zero OID. Only `regclass` is implemented;
  **any other target is refused by name** (`ParseError::UnsupportedCast`, `0A000`)
  rather than accepted and ignored, and the `CAST(x AS t)` spelling is likewise
  refused by name (`ParseError::UnsupportedCastExpr`) — never mis-parsed as a column
  named `CAST`. A lone `:` is still a loud `bad token: :`: the `::` arm consumes both
  colons, a single one falls through to the catch-all.
- Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`.
- Literals: int, float, string (with `''` escape), `TRUE`/`FALSE`/`NULL`, `$N`
  params, and typed literals `TIMESTAMP/DATE/TIME/INET/NUMERIC(=DECIMAL) '...'`.

**Value model** (`types`): `Null`, `Int(i64)`, `Text`, `Bool`,
`Float(OrderedFloat<f64>)`, `Uuid`, `Bytea`, `Timestamp(i64 µs)`, `Date(i32 days)`,
`Time(i64 µs)`, `Inet(IpAddr)`, `Numeric { unscaled: BigInt, scale }` (normalized,
arbitrary precision). `Value::sql_cmp` implements three-valued comparison (NULL or
type-mismatch or NaN ⇒ UNKNOWN), with Int↔Float promotion and value-aligned
numeric compare.

**Operators** (`exec`): `seq_scan`, `filter`, `project`, `hash_join` (inner
equi-join; NULL keys never match), `sort` (stable, multi-key, Postgres NULL
placement: ASC⇒NULLS LAST, DESC⇒NULLS FIRST), `hash_aggregate` (first-seen group
order; ungrouped-empty yields one row), `dedup` (DISTINCT, first-occurrence
order), `limit_offset`.

**Spill** (`spill`): the four BLOCKING operators — `sort`, `hash_aggregate`,
`hash_join` and `dedup` — cannot emit a first row before consuming their whole
input, so they **spill to disk** rather than buffering it (forge `t_50d99192`).
They reuse `ferrosa_storage::external_sort::ExternalSorter`, the same bounded
external merge sort behind `ferrosa-cql` and `ferrosa-graph`: accumulate to a
byte threshold, spill sorted runs, cascade-merge to a bounded fan-in, k-way merge
on finish, fail loud on any spill/merge I/O error.

- **Nothing caps a result.** The only knob is a resident-BYTES threshold — a work
  bound. A query that could be answered is never refused or truncated.
- **Temp location is configurable per node**: inject a `SpillReserver`, or set
  `FERROSA_SQL_TEMP_DIR`; the threshold follows the storage engine's
  `FERROSA_RANGE_SPILL_THRESHOLD_*`.
- **Cancellation and cleanup share one path**: the temp directory is held by a
  `TempSortTableReservation` moved into the output stream, so dropping the stream
  — exhausted, cancelled or abandoned — removes it. `sweep_orphaned_temp_dirs`
  reclaims what a killed process left behind, and runs once on first use.
- **Output order is unchanged.** Each sort-based operator restores the in-memory
  operator's order with a second sort on the arrival tag, so first-seen group
  order, DISTINCT first-occurrence order and the join's left-input order all
  survive. Grouping and DISTINCT sort under a type-aware total order consistent
  with `Value`'s structural `Eq`, never under `sql_cmp` (which would merge
  `Int(1)` with `Text("1")`).

**Planner** (`plan`): `execute`, `execute_with` (caller-supplied spill context),
`describe` (RowDescription shape without running
operators), `infer_param_types` (extended-protocol ParameterDescription).
Fail-loud binding: unknown table/column, ambiguous unqualified column, unknown
qualifier, non-grouped column, aggregate-in-WHERE, invalid ORDER BY ordinal, and
missing `$N` parameter all return a typed `ExecError`; a spill/merge I/O failure
returns `ExecError::Spill` (SQLSTATE `58030` on the wire) rather than a short
result.

## How it works

```
parse_statement ─▶ Statement (ast)
                     └─ Select(SelectStmt) ─▶ execute(stmt, catalog, schema, params)
                                                 │ resolve_scope (bind via Catalog)
                                                 │ seq_scan [→ hash_join*] → filter
                                                 │ → simple project | hash_aggregate*
                                                 │ → dedup* → sort* → limit_offset
                                                 ▼
                                              RowSink::columns, then ::row per row
```

`*` marks a blocking operator: it spills to disk past the context's byte
threshold and streams its output. The pipeline is drained one row at a time into
a `RowSink` (`execute_streaming`), so a caller that forwards rows holds O(batch),
not O(result); the sink may block for backpressure and may stop the query early
with `ControlFlow::Break`. An `Err` after rows were delivered means the delivered
rows are an incomplete result and MUST be reported. `execute`/`execute_with`
gather the rows into a `QueryResult` for tests and tools — they hold the whole
result and are not for the wire (FMEA SQL-12, `t_f348ba0b`).

`open_cursor` returns the same pipeline as a pull-based `RowCursor` instead of
pushing it into a sink. The cursor owns everything it reads (its scans, copies
of the predicates and parameters) and is `Send`, so a caller that wants rows on
demand can hold it between pulls without holding a thread and pull the next
chunk on any thread. The PostgreSQL front end does this for portals suspended
by `max_rows` (FMEA SQL-13). To make that possible, `TableProvider::scan` returns
an owned, `Send` iterator, and `RowStream`/`TryRowStream` are `Send`.

## Public API (key entry points)

| Area | Items |
|------|-------|
| Parse | `parse`, `parse_statement`, `ParseError` |
| AST | `Statement`, `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `CreateTableStmt`, `DropTableStatement`, `TruncateStatement`, `VacuumStmt`, `AnalyzeStmt`, `Expr`, `Operand`, `Term`, `CastTarget`, `Projection`, `SelectItem`, `OrderItem`, `ScalarItem`, `ScalarValue`, `AggArg` |
| Plan | `execute_streaming`, `RowSink`, `open_cursor`, `RowCursor`, `execute`, `execute_with`, `describe`, `infer_param_types`, `QueryResult`, `ExecError` |
| Operators | `seq_scan`, `filter`, `project`, `hash_join`, `sort`, `hash_aggregate`, `dedup`, `limit_offset`, `fallible`, `try_filter`, `try_project`, `Predicate`, `CmpOp`, `AggFunc`, `SortKey`, `SortDir`, `RowStream`, `TryRowStream` |
| Spill | `SpillCtx`, `SpillReserver`, `DirReserver`, `SpillStats`, `SpillError`, `default_temp_root`, `sweep_orphaned_temp_dirs` |
| Catalog | `Catalog`, `MapCatalog`, `SharedTable`, `TableProvider`, `InMemoryTable` |
| Types | `Value`, `Row`, `Column`, `ColumnType`, `RelSchema` |

## Dependencies

**Calls** (ferrosa crates this depends on): `ferrosa-storage` only, for the
bounded external merge sort and the cancellable temp-table reservation the
blocking operators spill through. Nothing else from it is used, and the engine
still carries its own `Value`/`Row`/`RelSchema` model — it does not adopt
`CqlValue`.

This is a deliberate change from the crate's original standalone-leaf position
(forge `t_50d99192`): reimplementing an external merge sort here to preserve the
leaf status would have duplicated machinery `ferrosa-cql` and `ferrosa-graph`
already share, and duplicated its failure handling with it.

External crates: `ordered-float` (total-order `f64` for join/group keys),
`num-bigint` (arbitrary-precision `Numeric`), `uuid`, `chrono` (std-only, typed
temporal literal parsing), `serde` + `serde_json` (spilled runs are
length-prefixed JSON records), `tracing`.

**Called by**: `ferrosa-postgres` — lowers parsed SQL onto this engine's
operators and serves results over the Postgres wire.

## Tests

In-crate unit tests (no `#[ignore]`, no live-infra): `exec.rs`, `parser.rs`,
`plan.rs`, `types.rs`, `catalog.rs`, `spill.rs` — 186 total. They cover
NULL/Kleene logic, sort NULL placement, aggregate edge cases, numeric
normalization, join key resolution, binder fail-loud paths, the `TRUNCATE` /
`VACUUM` / `ANALYZE` grammar (including the named refusals), and the spill
module's orders, replay buffer and orphan sweep.

`tests/spill_operators.rs` (11 tests, including a negative control that proves
the budget assertions fail when the threshold is unreachable) holds the per-operator spill invariant:
given an input larger than the threshold, the operator returns EVERY row, peak
resident rows stay an order of magnitude below the rows processed, output order
is unchanged, a dropped stream removes its temp directory, and a reservation
failure is loud.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — supported surface vs gaps, ranked by RPN
- [Roadmap](specs/roadmap.md) — Now / Next / Later
