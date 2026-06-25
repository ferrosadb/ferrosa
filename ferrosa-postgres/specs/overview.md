---
crate: ferrosa-postgres
status: developer-preview
last_updated: 2026-06-19
executive_summary: >
  The PostgreSQL v3 wire-protocol front-end for ferrosa. Implements the
  frontend/backend protocol (startup, SCRAM-SHA-256, simple + extended query),
  and lowers SQL onto the bespoke ferrosa-sql relational engine over live
  ferrosa storage. SELECT (incl. one JOIN) and single-row INSERT/UPDATE/DELETE
  are supported; it shares the storage row codec with CQL via ferrosa-row-bridge
  (D10) and is differential-tested against real PostgreSQL 16. DML in a
  BEGIN/COMMIT block buffers and commits atomically through Accord (FMEA PG-1);
  ROLLBACK discards the buffer. Developer preview: $N params in DML, RETURNING,
  and ON CONFLICT are still in progress, as is read-your-writes inside an open
  transaction.
---

# ferrosa-postgres — Architecture Overview

## Purpose & boundary

`ferrosa-postgres` is the **protocol skin + storage glue** that lets unmodified
Postgres drivers speak to ferrosa. Its boundary is deliberately narrow:

- It owns the **wire** (codec, message types), the **connection/auth state
  machine** (startup, SCRAM), the **session** (prepared statements, portals,
  transaction status), and the **lowering** of a parsed statement to engine
  reads/writes.
- It does **not** own query planning, binding, or operators — those are
  `ferrosa-sql`. It does **not** own the storage row encoding — that is
  `ferrosa-row-bridge` (the same codec CQL uses, **D10**).

## Module map

| Module | LoC | Responsibility |
|--------|-----|----------------|
| `codec` (`src/codec.rs`) | ~684 | Frame/parse the v3 wire: `read_startup`, `read_frontend`, backend encode, `MAX_MESSAGE_LEN` |
| `messages` (`src/messages.rs`) | ~423 | `FrontendMessage`/`BackendMessage`/`StartupFrame`, `FieldDescription`, `TransactionStatus` |
| `scram` (`src/scram.rs`) | ~281 | SCRAM-SHA-256 primitives: `ScramVerifier`, `server_first`, `verify_client_final` |
| `handshake` (`src/handshake.rs`) | ~299 | Sans-IO SCRAM phase machine + `VerifierStore` trait |
| `store` (`src/store.rs`) | ~170 | `SchemaVerifierStore`: bridge the handshake to the live `ferrosa-schema` role store |
| `connection` (`src/connection.rs`) | ~337 | Sans-IO `Connection`: startup/SSL/SASL → `Ready`; `take_inbuf` for pipelined first query |
| `extended` (`src/extended.rs`) | ~453 | Per-connection `Session`: Parse/Bind/Close/Sync, prepared statements + portals, txn `I`/`T`/`E` |
| `query` (`src/query.rs`) | ~1927 | `execute_query`, DML (INSERT/UPDATE/DELETE), value codecs (text+binary), SQLSTATE mapping, `load_catalog` |
| `storage_provider` (`src/storage_provider.rs`) | ~758 | `load_table`: async-materialize a storage scan into a sync `InMemoryTable`; `cql_to_value`; R15 guard |
| `catalog` (`src/catalog.rs`) | ~537 | `pg_catalog` projection (`pg_namespace`/`pg_class`/`pg_attribute`/`pg_type`) with deterministic OIDs |
| `server` (`src/server.rs`) | ~540 | tokio TCP front-end: `serve`, `QueryContext`, `handle_connection`, the post-auth query loop |
| `lib` (`src/lib.rs`) | ~37 | Module wiring + public re-exports |

## Connection lifecycle

```text
TCP accept (server::serve)
  → handle_connection
     Phase 1: Connection::on_bytes drives startup + SCRAM until ReadyForQuery
       SSLRequest        → 'N' (TLS declined; not wired)
       Startup           → AuthenticationSASL (SCRAM-SHA-256)
       SASLInitial/Final → AuthenticationOk + ParameterStatus* + BackendKeyData + ReadyForQuery
     Phase 2: query_loop frames Q / Parse / Bind / Describe / Execute / Sync / Close / Terminate
```

Note: the sans-IO `connection::Connection` also contains a minimal `Ready`-phase
fallback (it answers `Q` with `0A000` "not yet implemented"); the **real**
post-auth path is `server::query_loop`, which is what every driver test and the
differential oracle exercise.

## Data flow

**Read path (`SELECT`):** SQL string → `ferrosa_sql::parse_statement` →
`query::load_catalog` resolves every referenced table (FROM + optional JOIN) by
draining `StorageEngine::range_iter` (async) up front and decomposing each
`Partition` with the shared `ferrosa_row_bridge::partition_to_rows_with_storage_mapping`
into an `InMemoryTable` → `ferrosa_sql::execute` runs the sync operators →
`render_result` emits `RowDescription` + `DataRow`s (per the result formats) +
`CommandComplete "SELECT n"`. The caller appends one `ReadyForQuery`.

**Write path (`INSERT`/`UPDATE`/`DELETE`):** parse → resolve each value to a
`CqlValue` driven by the target column's `CqlType` (`value_to_cql`, fail-loud on
type mismatch `42804` / out-of-range `22003`) → `build_decorated_key` +
`build_row`/`build_delete_row` (the SAME `ferrosa-row-bridge` encoder the engine
and CQL decode) → `Mutation` → `engine.write_atomic_batch` → `CommandComplete
"INSERT 0 1"` / `"UPDATE 1"` / `"DELETE 1"`.

See [data-flow.md](data-flow.md) for the sequence diagrams.

## Type model & wire parity

`query` renders/parses each `ferrosa_sql::Value` to/from its exact Postgres text
form and (for most) the binary form, with OIDs/sizes advertised in
`RowDescription`: `Int→int4(23)`, `Text→text(25)`, `Bool→bool(16)`,
`Float→float8(701)`, `Uuid→uuid(2950)`, `Bytea→bytea(17)`,
`Timestamp→timestamp(1114)`, `Date→date(1082)`, `Time→time(1083)`,
`Inet→inet(869)`, `Numeric→numeric(1700)`. Binary `numeric` is out of scope (it
falls back to text bytes — documented). The storage value bridge
(`cql_to_value`) maps CQL scalars onto this model; `Duration` and collections
(`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`) are known-lossy and read as NULL.

## Key invariants

1. **Fail loud, never fake.** Every failure maps to a concrete SQLSTATE + one
   `ErrorResponse`; the front-end never returns a fake empty result on error
   (parse `42601`, undefined table `42P01`, storage `58000`, etc.).
2. **Missing table ≠ empty table (R15 guard).** `load_table` decides existence
   from schema metadata, not from an empty stream, so a typo'd table errors
   (`42P01`) instead of silently scanning nothing.
3. **One storage row encoder.** All reads/writes route through
   `ferrosa-row-bridge`, so Postgres-written rows are byte-identical to CQL.
4. **No `ferrosa-cql` dependency (D10).** Structural — enforced by the crate
   graph.
5. **Async storage, sync engine.** The scan is materialized up front
   (`load_table` awaits the stream); the sync operators never `block_on` the
   runtime.

## Position in the dependency graph

Depends on `ferrosa-common`, `ferrosa-row-bridge`, `ferrosa-schema`,
`ferrosa-sql`, `ferrosa-sstable`, `ferrosa-storage`. Depended on by `ferrosa`
(the main binary). See the [root crate index](../../specs/crates.md) for the full
graph.
