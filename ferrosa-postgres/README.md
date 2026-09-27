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
supported-vs-not surface. PostgreSQL transactions use a PostgreSQL-owned MVCC
manager; CQL/Cassandra transactions remain on Accord. In cluster mode, PostgreSQL
commits submit PG-owned mutations and their read/write table set through Accord.
Accord apply also carries PostgreSQL row-version metadata to replicas, where each
node retains history for its active snapshots. The distributed path is covered
by native-driver tests; the Jepsen strict-serializability workload remains the
acceptance gate before making a system-wide guarantee.
Other query gaps include `ON CONFLICT`, `UPDATE`/`DELETE … RETURNING`, and
`= ANY($N)` / IN-lists.

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
  result encodings, and Postgres error-skip-until-`Sync` semantics. `SELECT`,
  no-`FROM` expression selects, AND parameterized `INSERT` / `UPDATE` / `DELETE`
  can be prepared. This is the path `Ecto.Repo.insert/update/delete/all` drives.
- **Parameterized DML + `INSERT … RETURNING`** — `$N` placeholders in `INSERT`
  VALUES, `UPDATE` SET/WHERE, and `DELETE` WHERE are bound at `Bind` and
  substituted at `Execute` (fail-loud `08P01` if a `$N` has no bound value).
  Param OIDs in `Describe` are inferred from each placeholder's target column
  (so a driver that does not pre-declare OIDs gets a concrete type). `INSERT …
  RETURNING col,…` / `RETURNING *` echoes the just-written values as a `DataRow`
  (built in-memory — no storage read-back), exactly what Ecto needs to recover a
  generated/echoed key. RETURNING rows honor the portal's result formats (binary
  works). `UPDATE`/`DELETE … RETURNING`, `ON CONFLICT`, and `= ANY($N)` are
  **not yet supported** and fail loud (`0A000`/parse error), never silently.
- **PostgreSQL MVCC transactions** — `BEGIN ISOLATION LEVEL SERIALIZABLE` pins a
  local or Accord cluster timestamp. Simple and extended `SELECT` use sparse row
  overlays to restore versions changed after that timestamp; in-transaction
  inserts, updates, and deletes are visible to their own reads. Commit validates
  the transaction's read and write tables against local PostgreSQL MVCC epochs
  and, in cluster mode, against the Accord snapshot before atomically applying
  the buffered mutation batch. Row versions are staged before replica storage
  apply and made visible after the atomic batch succeeds. Conflicts return
  `40001`; rollback and failed transactions
  discard uncommitted mutations. Read validation is conservative at whole-table
  granularity, so unrelated writes to a read table can cause aborts. Versions
  older than every live snapshot are reclaimed automatically. The buffered write
  set has a fixed `MAX_TXN_WRITES` limit of 10,000. This is not an environment
  tunable. Distributed row-version history is in-memory and scoped to active
  process snapshots; storage serves snapshots begun after restart. The Jepsen
  strict-serializability workload remains outstanding.
- **DML execution** — INSERT/UPDATE/DELETE build storage rows through the shared
  `ferrosa-row-bridge` encoder. **Autocommit** uses the PostgreSQL MVCC commit
  path; **inside a transaction** the write is buffered until commit (see above).
  UPDATE/DELETE are Cassandra-style blind
  upserts/tombstones keyed by a full-primary-key equality `WHERE` (reported as
  `UPDATE 1` / `DELETE 1`).
- **`pg_catalog` projection** — `catalog` projects `pg_namespace`/`pg_class`/
  `pg_attribute`/`pg_type` from live schema metadata with deterministic OIDs.
- **TCP server** — `serve` / `QueryContext`: one spawned task per connection over
  a tokio `TcpListener`, sharing the auth store and the storage+schema context.

## Data flow

**Read (`SELECT`):** `Q`/`Execute` → `ferrosa_sql::parse_statement` →
`load_catalog` opens each referenced table as a streamed storage provider. A
snapshot's sparse MVCC row overlay replaces current versions and restores
deleted historical rows as the scan passes. The provider uses a bounded channel
and decodes one storage partition at a time, but the relational executor
materializes base scan rows and `QueryResult.rows`; rendering then builds a
second vector of all wire messages before sending. Thus query execution and
protocol output are not end-to-end streaming and peak memory grows with result
size. `offload::execute_offloaded` runs the sync operators **on a blocking
thread** → `RowDescription` + `DataRow`s +
`CommandComplete "SELECT n"`.

`ferrosa_sql::execute` is synchronous and CPU-bound (scan, filter, sort,
hash-aggregate, hash-join). It must never be called inline from the async
handlers: doing so pins an async worker for the whole query and starves
connection keepalives — the failure mode PR #131 fixed on the CQL path. Both
call sites (simple query in `query.rs`, extended query in `server.rs`) go
through `offload::execute_offloaded`, and
`offload::tests::executor_does_not_run_on_the_async_worker` fails if either
regresses (forge t_d3b2dec1).

Known limitation: source-side scanning is bounded by one partition plus the
channel, but the synchronous executor collects rows and the wire path collects
encoded messages. A full-table query can therefore use memory proportional to
its input/result size. End-to-end streaming requires changes to `ferrosa_sql`
and the PostgreSQL message writer; streaming only the storage loader does not
remove the materialization peak.

**Write (`INSERT`/`UPDATE`/`DELETE`):** parse → resolve each value to a
`CqlValue` by the column's CQL type (`value_to_cql`) → `build_decorated_key` +
`build_row`/`build_delete_row` (the SAME `ferrosa-row-bridge` encoder CQL uses) →
build a `Mutation` → `apply_or_buffer`: **autocommit** → apply and publish MVCC
row versions; **in a transaction** → buffer a PostgreSQL-owned `PgWrite`, later
atomically applied by the PostgreSQL MVCC commit path. With a cluster committer,
the PG-owned mutation batch is submitted through Accord. PostgreSQL MVCC read
validation remains process-local and is not part of Accord's decision. CQL
transaction writes retain their existing Accord contract.

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

134 in-crate unit tests (codec/messages/scram/handshake/connection/extended/
query/storage_provider/catalog/store + `mvcc`/transaction tests) run with no
infrastructure, plus integration tests:

- `tests/m1_join_live.rs` (15) — full stack over a real `tokio-postgres` driver
  in-process: SCRAM → JOIN, parameterized extended query, GROUP BY/ORDER
  BY/LIMIT, error-recovery-after-`Sync`, AND the parameterized DML path
  (`INSERT`/`UPDATE`/`DELETE` via `$N`, `INSERT … RETURNING id`/`*`,
  `UPDATE`/`DELETE … RETURNING` fail-loud, and extended-protocol DML inside a
  transaction: BEGIN/INSERT RETURNING/ROLLBACK discards and BEGIN/INSERT/COMMIT
  uses the PostgreSQL MVCC manager). Local temp engine, no Docker.
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
