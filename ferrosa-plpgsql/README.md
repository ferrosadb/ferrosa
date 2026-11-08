# ferrosa-plpgsql

> The PL/pgSQL subset Ferrosa's PostgreSQL front end runs (decision D13): parser,
> interpreter, and the converter to AssemblyScript that `ferrosa-udf` compiles.

## What this crate is

A pure library (no storage, no wasm, no server) over one trait, `Host`:

| Host call | Meaning |
|---|---|
| `query(sql, params) -> cursor` | start a query in the calling transaction |
| `fetch(cursor, max) -> rows` | at most `max` more rows; empty means exhausted |
| `close(cursor)` | stop a query early |
| `execute(sql, params) -> count` | run a statement for its effect |
| `notify(channel, payload)` | queue a notification, delivered only if the transaction commits |

Values cross as `Value { type_oid, text }` — PostgreSQL's lossless text form — with the text
in an `Arc<str>`, because a variable keeps its value while it is bound as a parameter.

## What's implemented

- **Parser** (`parse_do_block`, `parse_function`): `DECLARE` with typed locals and
  initialisers, `BEGIN … END`, `IF / ELSIF / ELSE / END IF`, `IF [NOT] EXISTS (query)`,
  assignment (`:=` and `=`), `RETURN [expr]`, `PERFORM expr`, `PERFORM pg_notify(c, p)`,
  `SELECT … INTO`, `NULL;`, SQL statements run through the host (`INSERT/UPDATE/DELETE`
  with variables bound, utility statements such as `CREATE TYPE` without — PostgreSQL never
  binds parameters into a utility statement), `--` and nested `/* */` comments, quoted
  identifiers, dollar quotes, `NEW.col` / `OLD.col` / `TG_OP` in trigger functions, function
  arguments by name and by `$n`.
- **Refusals** (`0A000`, naming the construct): `RAISE`, `LOOP`, `WHILE`, `FOR`, `FOREACH`,
  `EXIT`, `CONTINUE`, statement `CASE`, `EXECUTE`, `GET DIAGNOSTICS`, cursors, `COMMIT`,
  `ROLLBACK`, `EXCEPTION`, labels, nested blocks, `CONSTANT`, `ALIAS`, `NOT NULL`
  declarations, `INTO STRICT`, `RETURNING … INTO`, `RETURN NEXT/QUERY`, `NEW`/`OLD` as whole
  records, assignment to `NEW`/`OLD`, `TG_*` other than `TG_OP`, unsupported types.
- **Interpreter** (`interpret`): every expression is `SELECT <expr>` through the host with
  its variables bound as `$n` (an expression that is one variable is that variable). `EXISTS`
  fetches one row and closes; `PERFORM` drains in batches of `FETCH_BATCH`; scalar reads
  fetch one row. Runtime errors carry PostgreSQL's SQLSTATEs (`2F005`, `42804`, `42601`,
  `22023`, `0A000`).

- **Converter** (`assemblyscript::convert`): the interpreter's algorithm over the same slots,
  as AssemblyScript against the canonical ABI of the `routine` world (`ferrosa-udf`'s
  `src/wit/plpgsql.wit`). SQL is a static UTF-8 data segment; parameters are copied from
  slots; every fallible step returns the error result. Runtime errors share SQLSTATE and text
  with the interpreter (`messages`). `ferrosa-udf/tests/plpgsql_compiled.rs` runs both and
  compares results and host calls.
- **Front-end statements** (`utility::parse_utility`): `DO [LANGUAGE plpgsql] 'body'`,
  `CREATE [OR REPLACE] FUNCTION name([name] type, …) RETURNS type|trigger|void AS 'body'
  LANGUAGE plpgsql [IMMUTABLE|STABLE|VOLATILE]`, `DROP FUNCTION [IF EXISTS] name[(types)]`,
  `CREATE TRIGGER name AFTER INSERT|UPDATE [OF cols] [OR …] ON t FOR EACH ROW EXECUTE
  PROCEDURE|FUNCTION f()`, `DROP TRIGGER [IF EXISTS] name ON t`, `LISTEN`, `UNLISTEN [*]`,
  `NOTIFY ch [, 'payload']`, `SELECT pg_notify(a, b)` with literal or `$n` arguments. Any other
  statement is `None` (the SQL engine's). Refused by name: other languages, `OUT`/`INOUT`/
  `VARIADIC`, argument defaults, `SETOF`/`TABLE`, `STRICT`, `SECURITY`, `BEFORE`/`INSTEAD OF`,
  `FOR EACH STATEMENT`, `DELETE`/`TRUNCATE` events, `WHEN`, trigger arguments, constraint
  triggers, `CREATE OR REPLACE TRIGGER`, `CASCADE`.
- **`testing::ScriptedHost`**: the recording host both runtimes are tested against.

A compiled function allocates from a bump heap reclaimed after each call, so a `PERFORM`
over a very large result can reach the sandbox memory limit (`54000`) — it fails loudly.

## Variable binding rule

PostgreSQL's default `plpgsql.variable_conflict = error` raises when a name could be a
column or a variable. This subset binds the variable, except in name positions: after a
`.`, after `::` or `AS`, before `(`, in an `INSERT` target list, in an `UPDATE … SET`
target, in a DML head, and anywhere in a utility statement.

## Proofs

`specs/lean4/Ferrosa/Sql/Plpgsql.lean`: the converted program, run, equals the interpreter
(`convert_agrees`, `convertList_agrees`, `compiled_returns_what_the_interpreter_returns`), and
`EXISTS` reads nothing past the first row (`exists_ignores_everything_after_the_first_row`).

## Dependencies

`thiserror` only. Called by `ferrosa-udf` (the component runner) and `ferrosa-postgres`.

## Tests

```bash
cargo test -p ferrosa-plpgsql
```
