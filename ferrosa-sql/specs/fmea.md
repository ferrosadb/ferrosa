---
crate: ferrosa-sql
doc: fmea
last_updated: 2026-09-15
---

# ferrosa-sql — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This engine is a deliberately small SQL subset (D3, bespoke,
no DataFusion); the dominant risk class is **unsupported SQL surface** — a query
a real Postgres client sends that the parser rejects or the planner cannot
express. Those are mostly *fail-loud* (a `ParseError` / `ExecError`), which is the
correct behavior, but each is a coverage gap a consumer may hit.

## Supported surface (for reference)

`SELECT [DISTINCT]`, `*` / column / aggregate projection, one `INNER JOIN ... ON
a=b`, `WHERE`/`HAVING` with `AND`/`OR`/`NOT` + `= != <> < <= > >=`, `GROUP BY`,
`ORDER BY [ASC|DESC]` (column, output name, or ordinal), `LIMIT`/`OFFSET`,
`COUNT/SUM/MIN/MAX/AVG`, `$N` params, typed literals
`TIMESTAMP/DATE/TIME/INET/NUMERIC '...'`; single-row `INSERT`/`UPDATE`/`DELETE`
with key-equality WHERE; `BEGIN/COMMIT/ROLLBACK/SET/RESET` parsed for the
front-end.

## Failure modes

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| SQL-1 | **`IS NULL` / `IS NOT NULL` not parsed.** WHERE only supports the six comparison operators; `IS` is not a token. `col = NULL` is UNKNOWN under Kleene logic, so NULL filtering is impossible via SQL. | Common NULL queries fail to parse (or silently return zero rows if a client rewrites to `= NULL`); a fundamental SQL predicate is missing | 8 | 7 | 4 | 224 | **Open gap.** Add `IS [NOT] NULL` to the lexer/grammar and an `IsNull`/`IsNotNull` predicate. Top roadmap item. |
| SQL-2 | **Subqueries, CTEs (`WITH`), `UNION`/`INTERSECT`/`EXCEPT`, and window functions unsupported.** None are in the grammar; the FROM is a single table + at most one join. | Whole query classes rejected with `ParseError`; clients/ORMs depending on them break | 8 | 6 | 3 | 144 | **Open gap (fail-loud).** Out of M1 scope by design; each is a separate large effort. Document the boundary so consumers degrade gracefully. |
| SQL-3 | **Only INNER equi-join, exactly one, equality on a single column.** No LEFT/RIGHT/FULL/CROSS, no multi-table FROM, no `ON` with `AND`/inequality, no join-then-join. | Multi-table reporting queries and outer joins rejected | 7 | 6 | 3 | 126 | **Open gap (fail-loud).** `Join` AST models exactly `t ON a=b`. Widen to a join list + richer `ON` predicate. |
| SQL-4 | **No arithmetic / scalar expressions / functions in projection or predicates** (`a + b`, `UPPER(x)`, `col || 'x'`, `CASE`). Projection is a bare column or aggregate; comparison LHS is a column or aggregate, RHS is a literal/`$N`. | Computed columns and expression predicates rejected | 6 | 6 | 3 | 108 | **Open gap (fail-loud).** Needs an expression evaluator over `Value`. |
| SQL-5 | **Blocking operators materialized their whole input.** `hash_join` built the right side into a `HashMap` *and* accumulated every output row (`O(left × right)` under key skew); `sort`/`hash_aggregate`/DISTINCT dedup took and returned `Vec<Row>`. | Large joins/sorts/aggregations could exhaust memory (OOM) on big inputs | 7 | 3 | 5 | 105 | **FIXED** (forge `t_50d99192`). All four now spill through `ferrosa_storage::external_sort` — see `src/spill.rs` and `tests/spill_operators.rs`. Peak is bounded by a resident-bytes threshold, not by row count; nothing caps a result. Residual RPN below (SQL-12). |
| SQL-T029 | **Docs disagreed on whether the blocking operators spill** (commit 641e8c19 says yes; t_50d99192 / FM-43 said no). | PG expressions and SRFs (D11a) would build on an executor of unknown memory behavior | 7 | 2 | 2 | 28 | **PROVEN spilling** (T-029, 2026-09-28). `tests/spill_operators.rs` passes 10/10 on main with a 256-byte threshold, asserting `ctx.stats().spilled()` and peak resident rows below rows/10 for sort, hash aggregate, hash join (skew) and dedup. Forcing the threshold to `u64::MAX` fails all 5 operator tests; a permanent control test now pins that. t_50d99192 can close. |
| SQL-12 | **(closed) The final `QueryResult.rows` was a `Vec`.** Every operator feeding it streamed or spilled, but `execute` drained the pipeline into memory because `ferrosa-postgres::render_result` built all `DataRow` messages before writing any. | A result set larger than RAM OOMed at the front-end boundary rather than inside an operator | 7 | 1 | 2 | 14 | **FIXED** (forge `t_f348ba0b`). `execute_streaming` drains the pipeline into a `RowSink` one row at a time; the Postgres front end forwards rows through a bounded channel with socket backpressure, so the boundary holds O(batch). Evidence: `tests/streaming_result.rs` (scan never runs ahead of its consumer by more than 2 rows; a spilling ORDER BY delivers every row with peak resident rows an order of magnitude below the count; an early stop stops the scan) and `ferrosa-postgres/tests/pg_streaming_results.rs` (12 MiB result, 4 MiB live-heap budget, every row returned). `execute`/`execute_with` remain as collecting conveniences for tests and tools; nothing on the wire path calls them. |
| SQL-Tf348ba0b | **An error after rows were delivered is an incomplete result.** With streaming delivery a spill failure can arrive after the sink has already received rows; a caller that treats end-of-stream as success would report a truncated result as complete. | A client sees a short result and a success tag | 8 | 2 | 3 | 48 | **Mitigated.** `execute_streaming` returns `Err` after partial delivery and documents that the caller MUST report it; the Postgres front end sends an `ErrorResponse` after the rows already written and never a `CommandComplete` (`result_stream::tests::a_mid_stream_error_follows_the_rows_it_interrupts`). A sink that stops the query early gets `Ok(())` because it knows why it stopped (`tests/streaming_result.rs::a_sink_that_stops_early_stops_the_scan`). |
| SQL-T130-1 | **An out-of-scope DDL clause is silently dropped** (`FOREIGN KEY`, `CHECK`, `SERIAL`, `DEFAULT`, foreign schema, `UNIQUE`), so the table is created without a constraint or default the migration asked for. | Data integrity the app relies on is absent with no error | 8 | 4 | 2 | 64 | **Mitigated.** Each clause returns `ParseError::UnsupportedClause(<clause>)` whose message names it; one test per clause (`foreign_key_*`, `check_*`, `serial_*`, `default_*`, `foreign_schema_*`, `unique_*`) plus `pg_ddl_out_of_scope_clause_named_in_error`. |
| SQL-T130-2 | **An expression operator inside a refused clause (`DEFAULT 1 + 1`) hit the lexer first** and surfaced as an anonymous `BadToken`, hiding the clause name. | Refusal is loud but does not name the clause | 4 | 5 | 2 | 40 | **Fixed.** `CREATE` statements re-lex leniently (`Tok::Other`) so the parser reaches and names the clause; a stray token elsewhere is `Unexpected`. |
| SQL-T130-3 | **`CREATE TABLE` parses but nothing executes it**; a client could believe the table exists. | Later INSERT fails with unknown table | 6 | 5 | 2 | 60 | **Mitigated.** `ferrosa-postgres` dispatch answers SQLSTATE `0A000` "parsed but not yet executable" (never `CommandComplete`). Removed by T-132a. |
| SQL-T130-4 | **Table without a usable PRIMARY KEY** (none, duplicate, or naming an undefined column) would reach schema creation. | A table with no partition key cannot exist in the storage model | 7 | 3 | 2 | 42 | **Mitigated.** `MissingPrimaryKey`, `MultiplePrimaryKeys`, `UnknownPrimaryKeyColumn`, `DuplicateColumn` are typed parse errors, each tested. |
| SQL-T130-5 | **`json` differs from Postgres**: parsed as `PgType::Json` but stored as `jsonb` (D11), so key order and duplicate keys are not preserved. | Behavioral difference from real PG | 3 | 5 | 3 | 45 | **Documented** (D11). `PgType::storage()` is the single place the collapse happens; the catalog must report `jsonb`. |
| SQL-6 | **`SUM`/`AVG` over `Int` accumulate into `i64` with no overflow check** (`int_sum += n`). Numeric/decimal columns are not summed (only `Int`/`Float` feed `add_numeric`). | Large integer sums can wrap (panic in debug, silent wrap in release); SUM/AVG over a `Numeric` column silently yields NULL | 6 | 3 | 6 | 108 | **Open gap.** Promote integer SUM to `BigInt`/checked add; route `Numeric` through `add_numeric`. |
| SQL-7 | **DML is single-row with key-equality WHERE only.** `INSERT` takes one `VALUES` tuple; `UPDATE`/`DELETE` WHERE is `col = val [AND ...]` (no ranges, no `IN`, no multi-row). Cassandra-style upsert assumption. | Bulk DML and range deletes rejected; an `UPDATE ... WHERE x > 5` won't parse | 5 | 5 | 3 | 75 | **By design (M1).** Documented; widen when the front-end needs set-based DML. |
| SQL-8 | **Numeric/temporal values only reach predicates via *typed literals*.** A bare `'2024-01-01'` lexes as `Text`; comparing it to a `Date` column is UNKNOWN (no implicit cast). The client must write `DATE '...'`/`NUMERIC '...'`. | Naturally-written date/decimal predicates silently match nothing | 6 | 4 | 5 | 120 | **Partial.** Typed-literal syntax works and fails loud on bad bodies; implicit string→typed coercion against the column type is the gap. |
| SQL-9 | **No `ORDER BY ... NULLS FIRST/LAST` override, no `COLLATE`, no explicit `ASC NULLS ...`.** NULL placement is fixed (ASC⇒LAST, DESC⇒FIRST); text ordering is `String`'s byte/`Ord` (C-collation), not locale-aware. | Queries needing a specific NULL placement or locale collation get a different order than Postgres | 4 | 4 | 5 | 80 | **By design.** C-collation + fixed NULL placement match the documented invariant; surface as a known difference. |
| SQL-10 | **Unsupported `Value` types decode/compare as UNKNOWN/absent.** Collections (List/Set/Map/Tuple/UDT/Vector) and binary-format numeric are out of scope; cross-type `sql_cmp` is UNKNOWN, not an error. | A column the provider can't map to a supported `Value` compares as no-match rather than erroring | 5 | 3 | 6 | 90 | **Documented in `types.rs`.** Widen the type set as the storage provider needs; consider fail-loud on unmappable types. |
| SQL-11 | **`describe`/`execute`/`infer_param_types` derive column shape independently.** They share `resolve_scope`, but projection/aggregate column derivation is duplicated; drift would make `RowDescription` disagree with the actual rows. | Postgres client desync (wrong column metadata vs data) | 7 | 2 | 6 | 84 | **Mitigated**: both call the same `simple_projection`/aggregate-column helpers; covered by plan tests. Keep them sharing one derivation. |

## Top risks to act on

1. **SQL-1 (RPN 224)** — `IS NULL`/`IS NOT NULL` is missing and unworkaroundable
   in SQL given Kleene `= NULL`. Highest-value parser gap.
2. **SQL-2 (RPN 144)** — subquery / CTE / `UNION` / window absence is the broadest
   class of rejected queries; the boundary must be documented so `ferrosa-postgres`
   returns a clear "unsupported" rather than a confusing parse error.
3. **SQL-3 / SQL-8 (RPN 126 / 120)** — single-INNER-join and the typed-literal-only
   path for dates/decimals are the next most likely real-client surprises.

## Detection assets

- In-crate unit tests: `parser.rs` (44), `plan.rs` (42), `exec.rs` (24),
  `types.rs` (15), `catalog.rs` (2) — exercise Kleene logic, NULL sort placement,
  aggregate edge cases, numeric normalization, and binder fail-loud paths.
- `ferrosa-postgres` integration tests drive the engine end-to-end over the wire
  (the real consumer-facing surface check).
