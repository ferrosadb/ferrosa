# ferrosa-postgres

> The PostgreSQL v3 wire-protocol front-end for ferrosa (developer preview) —
> SCRAM auth, the simple + extended query protocols, and SELECT/INSERT/UPDATE/
> DELETE over live ferrosa storage, differential-tested against real PostgreSQL 16.

## What this crate is

A Postgres frontend/backend (v3) listener that lets unmodified Postgres drivers
(`tokio-postgres`, JDBC, psql, ORMs) talk to ferrosa. It owns the wire codec, the
connection/SCRAM state machine, and the query lowering that turns a SQL string
into reads/writes against the ferrosa `StorageEngine`. It is **not** the
relational query engine — planning/binding/operators live in `ferrosa-sql`; this
crate is the protocol skin plus the storage glue.

Decision **D10**: the storage row codec is shared with the CQL front-end via the
neutral `ferrosa-row-bridge` crate, so a row written through Postgres decodes
byte-identically over CQL — without `ferrosa-postgres` ever depending on the
~54k-LOC `ferrosa-cql` crate.

This is a **developer preview**. See [specs/fmea.md](specs/fmea.md) for the exact
supported-vs-not surface (key gaps: `$N` params in DML, `RETURNING`, and `ON
CONFLICT` are still in progress; transaction atomicity via Accord is now wired —
see below).

## What's implemented

- **Wire protocol (v3)** — startup (incl. `SSLRequest`, declined with `N` since
  TLS is not wired), the sans-IO [`Connection`] phase machine
  (`AwaitingStartup → Authenticating → Ready → Closed`), and message framing
  (`codec` / `messages`).
- **Authentication** — SCRAM-SHA-256 (`scram` + `handshake`), driven against the
  live `ferrosa-schema` role store via [`SchemaVerifierStore`]. Fail loud: an
  unknown role / bad proof never authenticates.
- **Simple query protocol (`Q`)** — `execute_query` lowers one SQL string:
  `SELECT` (incl. a single `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`),
  no-`FROM` scalar selects (`SELECT 1`, `SELECT version()`,
  `current_database()`), and single-row `INSERT` / `UPDATE` / `DELETE`.
- **Extended query protocol** — `Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`
  with a per-connection [`Session`] (prepared statements + portals), `$N`
  parameter type inference (`ParameterDescription`), text + binary parameter and
  result encodings, and Postgres error-skip-until-`Sync` semantics. **Only
  `SELECT`** (and no-`FROM` expression selects) can be prepared.
- **Transaction atomicity via Accord** (FMEA PG-1) — `BEGIN`/`COMMIT`/`ROLLBACK`
  drive the `ReadyForQuery` status byte `I`/`T`/`E`, and DML inside an open `T`
  block is **buffered** as a `ferrosa_storage::accord::TransactionWrite` instead
  of applied (`apply_or_buffer` in `query.rs`). `COMMIT` drives the whole
  write-set through the injected `TransactionCommitter` as one atomic multi-key
  Accord transaction (`commit_txn` in `server.rs`); `ROLLBACK` discards the
  buffer so the writes are **never applied**. Fail-loud: a statement that errors
  inside a block poisons it (`T → E`, only `COMMIT`/`ROLLBACK` accepted, `25P02`);
  in standalone mode (no committer) a `COMMIT` carrying buffered DML fails loud
  (`0A000`, cluster mode required) rather than faking atomicity; an empty
  write-set commits cleanly. The buffer is capped at `MAX_TXN_WRITES` (10 000).
  This mirrors the CQL `CqlTransaction` path. *Not yet:* read-your-writes inside
  the open block (buffered writes are not visible to in-transaction reads).
- **DML execution** — INSERT/UPDATE/DELETE build storage rows through the shared
  `ferrosa-row-bridge` encoder. **Autocommit** (no open transaction) applies
  immediately via `engine.write_atomic_batch`; **inside a transaction** the write
  is buffered (see above). UPDATE/DELETE are Cassandra-style blind
  upserts/tombstones keyed by a full-primary-key equality `WHERE` (reported as
  `UPDATE 1` / `DELETE 1`).
- **`pg_catalog` projection** — `catalog` projects `pg_namespace`/`pg_class`/
  `pg_attribute`/`pg_type` from live schema metadata with deterministic OIDs.
- **TCP server** — `serve` / `QueryContext`: one spawned task per connection over
  a tokio `TcpListener`, sharing the auth store and the storage+schema context.

## Data flow

**Read (`SELECT`):** `Q`/`Execute` → `ferrosa_sql::parse_statement` →
`load_catalog` materializes each referenced table (the async
`StorageEngine::range_iter` stream is drained up front and decomposed via the
shared `ferrosa_row_bridge::partition_to_rows_with_storage_mapping`) into an
`InMemoryTable` → `ferrosa_sql::execute` runs the sync operators →
`RowDescription` + `DataRow`s + `CommandComplete "SELECT n"`.

**Write (`INSERT`/`UPDATE`/`DELETE`):** parse → resolve each value to a
`CqlValue` by the column's CQL type (`value_to_cql`) → `build_decorated_key` +
`build_row`/`build_delete_row` (the SAME `ferrosa-row-bridge` encoder CQL uses) →
build a `Mutation` → `apply_or_buffer`: **autocommit** →
`engine.write_atomic_batch` and `CommandComplete`; **in a transaction** →
serialize the `Mutation` into a buffered `TransactionWrite` (applied later by the
committer on `COMMIT`) and `CommandComplete`.

See [specs/data-flow.md](specs/data-flow.md) for the sequence diagrams.

## Public API (key entry points)

| Area | Items |
|------|-------|
| Server | `server::serve`, `server::QueryContext`, `server::handle_connection` |
| Connection | `connection::Connection`, `ConnError` |
| Auth | `handshake::Handshake`, `VerifierStore`, `store::SchemaVerifierStore`, `scram::{ScramVerifier, ScramServerFirst, server_first, verify_client_final}` |
| Simple query | `query::execute_query` |
| Extended query | `extended::Session` (`on_parse`/`on_bind`/`on_close`/`on_sync`), `query::decode_param`/`encode_value` |
| Storage glue | `storage_provider::load_table`, `cql_to_value`, `LoadError` |
| Catalog | `catalog::{type_oid, …}` |
| Codec / messages | `codec::{read_startup, read_frontend, MAX_MESSAGE_LEN}`, `messages::{FrontendMessage, BackendMessage, TransactionStatus, …}` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `CqlValue`, `CqlType`, `DecoratedKey`, `PartitionKey`,
  `CellValue` (the shared value/key model).
- **`ferrosa-row-bridge`** — the canonical row codec and partition→row
  decomposition (`build_decorated_key`, `build_row`, `build_delete_row`,
  `partition_to_rows_with_storage_mapping`, `parse_cql_type_in_keyspace`). D10:
  the SAME code `ferrosa-cql` uses, so there is no row-ordering divergence and no
  dependency on `ferrosa-cql`.
- **`ferrosa-schema`** — keyspace/table metadata, column kinds, the role store
  (`scram_credential`) the verifier reads.
- **`ferrosa-sql`** — the bespoke relational engine: `parse_statement`,
  `execute`, `describe`, `infer_param_types`, `MapCatalog`, `Value`/`Column`.
- **`ferrosa-sstable`** — names the `Partition` type `range_iter` streams.
- **`ferrosa-storage`** — `StorageEngine` (`range_iter`, `write_atomic_batch`),
  `Mutation`, `TableId`.

**Notably does NOT depend on `ferrosa-cql`** (decision D10).

**Called by**:

- **`ferrosa`** — the main binary mounts the Postgres listener.

## Tests

~111 in-crate unit tests (codec/messages/scram/handshake/connection/extended/
query/storage_provider/catalog/store) run with no infrastructure, plus
integration tests:

- `tests/m1_join_live.rs` (5) — full stack over a real `tokio-postgres` driver
  in-process: SCRAM → JOIN, parameterized extended query, GROUP BY/ORDER
  BY/LIMIT, error-recovery-after-`Sync`. Local temp engine, no Docker.
- `tests/scram_live.rs` (3) — real-driver SCRAM + `SELECT 1`, extended
  expression select, wrong-password rejection. In-process loopback.
- `tests/differential_oracle.rs` (3, `#[cfg(feature = "live-infra-tests")]`) —
  runs a fixed corpus + DML against BOTH real PostgreSQL 16 (container) and
  ferrosa over the same data and asserts agreement. Gated; panics with setup
  instructions if `FERROSA_TEST_CONTAINERS=1` is unset (never a silent skip).

```bash
cargo test -p ferrosa-postgres                       # unit + in-process integration
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-postgres \
  --features live-infra-tests --test differential_oracle -- --nocapture
```

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — supported-vs-not surface + RPN-ranked gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- [Data flow](specs/data-flow.md) — SELECT + INSERT sequence diagrams

Public marketing page: `docs/database/postgres.html` (ferrosadb.com).
