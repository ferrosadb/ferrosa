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
commits submit PG-owned mutations and a snapshot through Accord. A global PG
marker conservatively conflicts all PostgreSQL data commits; the read/write table
set is not yet used for per-table Accord validation. Accord apply carries
PostgreSQL row-version metadata to replicas, where each node retains history for
its active snapshots. The native-driver Jepsen workload covers transfer,
register, exact-key predicate/phantom, and write-skew histories. Its single-replica pause
schedule checks history validity and convergence on the active quorum; it does
not establish post-resume catch-up for the paused replica or mixed
CQL/PostgreSQL serializability.
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
- **PostgreSQL MVCC transactions** — explicit `BEGIN ISOLATION LEVEL
  SERIALIZABLE` pins a
  local or Accord cluster timestamp. Simple and extended `SELECT` use sparse row
  overlays to restore versions changed after that timestamp; in-transaction
  inserts, updates, and deletes are visible to their own reads. Commit validates
  the transaction's read and write tables against local PostgreSQL MVCC epochs
  in standalone mode. In cluster mode, a global PostgreSQL commit marker checks
  the snapshot against every intervening PostgreSQL data commit; the table set
  is not yet used for per-table Accord validation, so unrelated transactions can
  cause conservative `40001` aborts. Accord atomically applies the buffered
  mutation batch. Row versions are staged before replica storage
  apply and made visible after the atomic batch succeeds. Conflicts return
  `40001`; rollback and failed transactions
  discard uncommitted mutations. Only explicit `SERIALIZABLE` is supported;
  explicit `READ COMMITTED` and `REPEATABLE READ` fail with `0A000`. Unqualified
  `BEGIN` retains its legacy behavior and is not labeled strict serializable.
  Versions older than every live snapshot are reclaimed automatically. The
  buffered write set defaults to a 10,000 mutation cap, and each storage scan
  defaults to a 64-row channel; both are startup-configurable. PostgreSQL also
  expires active snapshots past a configurable maximum age so old transactions
  cannot retain history indefinitely. Distributed row-version history is
  in-memory and scoped to active process snapshots; storage serves snapshots
  begun after restart. The opt-in Jepsen history checker covers transfers,
  register updates, predicate reads/phantom insertion, and write skew. Its fault
  mode excludes the resumed node from convergence checks; see the Jepsen crate
  guide for the exact boundary.
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

## PostgreSQL MVCC resource bounds

These startup environment variables can be changed without recompiling. Invalid
values log an error and the process uses the complete defaults:

| Environment variable | Default | Bound |
|---|---:|---|
| `FERROSA_POSTGRES_MAX_TXN_WRITES` | `10000` | Buffered mutations per transaction |
| `FERROSA_POSTGRES_SCAN_BUFFER_ROWS` | `64` | In-flight rows between storage and the SQL executor |
| `FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS` | `600000` | Maximum active snapshot age; later use returns `40001` |
| `FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS` | `1000` | Background snapshot expiry and history-pruning interval |

The scan buffer is a storage-side backpressure bound. The relational executor
and PostgreSQL protocol renderer still materialize full query results. See the
public [`PROFILE.md`](../PROFILE.md) for tuning guidance and caveats.

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
the PG-owned mutation batch and snapshot are submitted through Accord after
local MVCC validation. Accord validates the cluster snapshot against the
PostgreSQL commit marker and atomically applies the batch. The marker is
conservative across tables, so unrelated PostgreSQL writes can cause `40001`.
CQL transaction writes retain their existing Accord contract.

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
