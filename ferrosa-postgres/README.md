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

- **Wire protocol (v3)** — startup, the sans-IO [`Connection`] phase machine
  (`AwaitingStartup → [AwaitingTls →] Authenticating → Ready → Closed`), and
  message framing (`codec` / `messages`).
- **TLS** (t_e1c819ad) — `SSLRequest` is answered `S` and upgraded with rustls
  when `[postgres] tls_cert` / `tls_key` are set (env `FERROSA_POSTGRES_TLS_CERT`
  / `_KEY`); otherwise `N`. `[postgres] require_tls = true`
  (`FERROSA_POSTGRES_REQUIRE_TLS`) refuses a StartupMessage that did not
  negotiate TLS with a FATAL `28000`. The acceptor comes from
  `ferrosa_net::tls` — the same PEM loading and the single crypto provider the
  CQL and internode listeners use. Bytes pipelined after `SSLRequest` are
  refused (CVE-2021-23214 shape), as is a second `SSLRequest`.
- **Authentication** — SCRAM-SHA-256 (`scram` + `handshake`), driven against the
  live `ferrosa-schema` role store via [`SchemaVerifierStore`]. Fail loud: an
  unknown role / bad proof never authenticates, and a `NOLOGIN` role is refused
  even with a valid proof.
- **Failed-login limiter** (t_e1c819ad) — every login is admitted through the
  schema's shared per-user limiter (`Schema::check_login_rate_limit`, the one
  `Schema::authenticate` uses for CQL); unknown roles and bad proofs count as
  failures. A lockout earned over PostgreSQL also refuses CQL for that user, and
  vice versa. Refusal: FATAL `28000` "login throttled". The limiter is in-memory
  per node.
- **Authorization** (t_e1c819ad, `authz`) — every statement is checked with
  `Schema::check_permission`, the CQL router's model: `SELECT` needs `SELECT` on
  each table read, `INSERT`/`UPDATE`/`DELETE` need `MODIFY`, `RETURNING` also
  needs `SELECT`, table-free statements need nothing. Checked on simple query,
  extended `Describe`, and `Execute`; a denial is `42501`
  `insufficient_privilege`. The mapping is an exhaustive match, so a new
  statement kind cannot compile without a rule.
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
- **Corrupt stored cells fail the query (t_cf7ca2cc)** — the shared row bridge
  returns `RowDecodeError`; the streaming scan records a failure naming
  `keyspace.table` and the column, so the client gets an error rather than a NULL
  (FMEA `PG-Tcf7ca2cc`).
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

The scan buffer is a storage-side backpressure bound. The result side is
bounded too: rows reach the client in batches of `RESULT_BATCH_ROWS` (16) through
a channel of `RESULT_CHANNEL_BATCHES` (2), with backpressure from the socket. See
the public [`PROFILE.md`](../PROFILE.md) for tuning guidance and caveats.

## Data flow

**Read (`SELECT`):** `Q`/`Execute` → `ferrosa_sql::parse_statement` →
`load_catalog` opens each referenced table as a streamed storage provider. A
snapshot's sparse MVCC row overlay replaces current versions and restores
deleted historical rows as the scan passes. The provider uses a bounded channel
and decodes one storage partition at a time. `ferrosa_sql::execute_streaming`
runs **on a blocking thread** and pushes its output through a bounded channel of
row batches (`result_stream`); the async side encodes each batch to `DataRow`s
and writes it to the socket, so nothing gathers the result:
`RowDescription` + `DataRow`s streamed + `CommandComplete "SELECT n"`.

**Extended protocol.** `Execute` honours `max_rows`: the portal returns that many
rows, answers `PortalSuspended`, and keeps its running query, so the next
`Execute` continues from the next row (no gap, no duplicate). `Close`, a rebind
of the portal name, `Sync` outside a transaction block, or a disconnect drops the
query, which stops the executor. **Simple protocol** streams the same way.

**Errors mid-stream.** A failure after rows were sent (an unencodable value, a
spill error, a storage error during the scan) is reported as an `ErrorResponse`
after the rows already written — as PostgreSQL does — never as a
`CommandComplete` and never as a silently short result (FMEA PG-Tf348ba0b).

The executor (`ferrosa_sql::open_cursor`, then `RowCursor::next_row`) is
synchronous and CPU-bound (scan, filter, sort, hash-aggregate, hash-join). It
must never run inline on the async handlers: doing so pins an async worker for
the whole query and starves connection keepalives — the failure mode PR #131
fixed on the CQL path. Both call sites (simple and extended) start it through
`result_stream::open_stream`, and
`result_stream::tests::executor_does_not_run_on_the_async_worker` fails if that
regresses (forge t_d3b2dec1).

**A waiting query holds no thread.** The query is an owned `RowCursor`, pulled
on a blocking thread one fetch (16 rows) at a time; the next fetch starts as
soon as a batch arrives. A fetch waits only on the executor's inputs, never on
the client, so a suspended portal or a client that stopped reading its socket
holds no blocking thread. Below it, the storage range scan pauses after a
10 ms grace and gives back its pool slot and thread too, keeping its exact
position (ferrosa-storage ST-82). Before this, each suspended portal parked
two blocking threads, and about `cores` idle clients exhausted the listener
runtime's bounded blocking pool (`tests/pg_suspended_portals_hold_no_thread.rs`:
16 portals beside a 4-thread pool, every thread free, another session's
SELECT completes; `tests/pg_stalled_reader_holds_no_thread.rs`: the same for
8 clients that stopped reading their sockets).

**Limits on suspended portals** (`portal_limits.rs`, FMEA PG-14/PG-15). A
suspended portal still holds its query (a few batches of rows, the storage
scan's open SSTable readers, spilled sort runs), so how many may wait, and for
how long, is bounded:

| `[postgres]` TOML | Environment | Default |
|---|---|---|
| `max_suspended_portals_per_connection` | `FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS_PER_CONNECTION` | 64 |
| `max_suspended_portals` | `FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS` | 2048 |
| `suspended_portal_idle_timeout_ms` | `FERROSA_POSTGRES_SUSPENDED_PORTAL_IDLE_TIMEOUT_MS` | 600000 |

TOML wins over the environment; a malformed value is logged at ERROR and the
defaults apply. A fresh portal executed with `max_rows` (the only kind that
can suspend) takes its place under both limits before it runs; past either
it is refused with SQLSTATE `53000` before any `DataRow`, as PostgreSQL
refuses a resource limit before output. A portal that completes without
suspending gives its place back. At the limit, such an `Execute` is refused
even if its result would have fit in `max_rows`. A portal left untouched past the idle timeout is closed
by its connection (which wakes for it even when the client sends nothing);
a later `Execute` on it answers `57014` naming the timeout, never a silent
restart. `Close`, a rebind, `Sync` outside a block, the end of a transaction
(PostgreSQL destroys a transaction's portals) and disconnect release a portal
at once. A portal run to its end answers a further `Execute` with no rows
(`SELECT 0`), as PostgreSQL does, instead of re-running its query. Metrics:
`ferrosa_pg_suspended_portals` (gauge), `ferrosa_pg_suspended_portal_refusals_total`,
`ferrosa_pg_suspended_portal_expiries_total`; refusals WARN when they start
and INFO when admission resumes.

**What a suspended portal sees of concurrent writes.** Rows changed by
PostgreSQL transactions follow the portal's MVCC snapshot (the overlay in
`storage_provider`). Rows written outside PostgreSQL (CQL, or the storage
engine directly) are read as the storage scan reaches them: a storage range
scan is not a snapshot, so a row written ahead of the scan's position may or
may not appear, suspended or not. Every row that existed when the portal
started and was not deleted appears exactly once, in storage (token) order
(`tests/pg_portal_resume.rs`).

**DDL (`CREATE TABLE [IF NOT EXISTS]`, T-132a).** Simple protocol only. The
statement is planned into a `TableMetadata` (`ddl.rs`) and applied through the
same `ferrosa_cluster::ddl_path::DdlPath` the CQL router uses, via `ClusterDdl`
(direct when standalone, coordinator in pair mode, Raft-replicated in a
cluster). Keys: one primary-key column is the partition key; a composite key is
the first column as partition key and the rest as ascending clustering columns.
Types go through `pg_types::cql_type_for_pg_name`; an unmapped type is `42704`
naming it; `json`/`jsonb` create a CQL `jsonb` column (T-161a, D11).
`varchar(n)` and `numeric(p,s)` store as unbounded `text`/`decimal`: the length
and precision are not enforced. An existing table is `42P07`, or a success
under `IF NOT EXISTS` with no NOTICE (there is no `NoticeResponse`). A missing
keyspace is `3F000`; DDL in a transaction block is `25001`; a context without a
`ddl` executor refuses `0A000`. Unsupported clauses keep their `0A000` names.
`CREATE TABLE` requires `CREATE` on the target keyspace, checked at dispatch
in `authz::statement_permissions` before the executor reads the schema (42501). `DROP`/`ALTER` are T-132b; extended-protocol `Parse` of DDL is refused.

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
| Server | `server::serve`, `server::QueryContext`, `server::PgTls`, `server::handle_connection` |
| Connection | `connection::Connection`, `ConnError`, `TlsPolicy` |
| Auth | `handshake::Handshake`, `VerifierStore` (verifier + limiter hooks), `store::SchemaVerifierStore`, `scram::{ScramVerifier, ScramServerFirst, server_first, verify_client_final}` |
| Simple query | `query::execute_query` |
| Extended query | `extended::Session` (`on_parse`/`on_bind`/`on_close`/`on_sync`), `query::decode_param_checked` (fails loud: `22P02` text parse, `22P03` binary, `42704` unmapped OID)/`encode_value` |
| Storage glue | `storage_provider::load_table`, `cql_to_value`, `LoadError` |
| Catalog | `catalog::{pg_attribute, pg_type, catalog_tables}` (fallible: `PgTypeError`) |
| Type map | `pg_types::{pg_type_of, pg_type_of_column, for_column_type, cql_type_for_pg_name, PgType, PgTypeError}` — the one `CqlType` ↔ Postgres type map (OID, typname, typlen, engine `ColumnType`, binary support); catalog, storage provider, RowDescription and parameter inference all read it |
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
  (`scram_credential`) the verifier reads, the shared login limiter
  (`check_login_rate_limit` / `record_login_failure` / `complete_login`) and
  `check_permission`.
- **`ferrosa-net`** — `tls::optional_server_config`, the shared TLS acceptor
  builder and crypto provider.
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
- `tests/security_live.rs` (7) — against the schema-backed role store: `42501`
  for a role without `SELECT` (simple + extended) / without `MODIFY` / `RETURNING`
  without `SELECT`; repeated bad passwords lock the user out of PostgreSQL AND
  CQL; TLS handshake succeeds and plaintext is refused when required.
- `tests/differential_oracle.rs` (3, `#[cfg(feature = "live-infra-tests")]`) —
  runs a fixed corpus + DML against BOTH real PostgreSQL 16 (container) and
  ferrosa over the same data and asserts agreement. Gated; panics with setup
  instructions if `FERROSA_TEST_CONTAINERS=1` is unset (never a silent skip).
  Also holds the jsonb differential (T-301): `differential_oracle_jsonb_corpus_agrees`
  (80 documents x literal / text `$1` / binary `$1`, read back in text and binary
  format, byte for byte against `SELECT doc::text` on postgres:16),
  `differential_oracle_jsonb_bad_binary_version_agrees` and
  `differential_oracle_jsonb_plain_select_equals_cast_on_postgres`.
- `tests/jsonb_slice.rs` (4) — the same corpus (`tests/common/jsonb_corpus.rs`)
  through the in-process server with `tokio-postgres`, no infrastructure, against
  PostgreSQL 16's recorded output.

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

## jsonb (T-150, T-160, T-161a)

`CqlType::Jsonb` maps to the engine `ColumnType::Jsonb` (OID 3802; `json` 114, `jsonpath` 4072 and `text[]` 1009 have `ALL_PG_TYPES` entries, all `binary: false`). `storage_provider::cql_to_value` and `query::value_to_cql` move the validated cell across unchanged as `Value::Jsonb` (T-160). The wire codec is T-161a: `query::encode_value` refuses jsonb, jsonpath and `text[]` values in both formats, and binding anything but a jsonb value to a jsonb column is `0A000`.

`CqlType::Jsonb` has a named arm in `pg_types::column_type_of` that advertises `text`; reading or writing a jsonb value is refused (`0A000` on write, a conversion error on read) until T-160 adds the SQL value and T-161a the wire codec (T-150).

### jsonb in a PRIMARY KEY (T-154a)

`CREATE TABLE t (doc jsonb PRIMARY KEY)` (or jsonb in a composite key) is
refused with `42P16` naming the column. Postgres accepts it; ferrosa keeps
jsonb out of key bytes (D3). The schema registry's own check runs again before
the change is handed to the DDL path.

`CqlType::Jsonb` maps to the engine `ColumnType::Jsonb` (OID 3802, typlen -1, `binary: true`; `json` 114, `jsonpath` 4072 and `text[]` 1009 have `ALL_PG_TYPES` entries, all `binary: false`). `storage_provider::cql_to_value` and `query::value_to_cql` move the validated cell across unchanged as `Value::Jsonb` (T-160). The wire codec is `jsonb_wire` (T-161a): text format is the PostgreSQL 16 jsonb text (`TextStyle::PgText`, D26; `{"aa":2,"b":1}` prints `{"b": 1, "aa": 2}`, scale kept); binary format is `0x01` then that text (`jsonb_send`). `render_value` never returns NULL for a jsonb value: a corrupt cell is `XX001`, an over-budget print `54000`, and jsonpath/`text[]` are `0A000` (T-161b). Input is parsed with `ferrosa_jsonb::parse_text_observed` under `QueryContext::jsonb_limits` (resolved from `[jsonb]` in `ferrosa/src/main.rs`, no default): a parameter declared 3802 or 114 (or bound to a jsonb column after `Describe`) is parsed in `decode_param_checked`; an untyped string literal or a text-declared parameter bound to a jsonb column is parsed in `value_to_cql`. SQLSTATEs: `22P02` invalid JSON (byte offset, input never echoed), `22P05` a `\u0000` escape (a PG text value cannot hold NUL; `NulPolicy::Reject`), `XX000` unknown binary version byte and `08P01` no version byte (both what PostgreSQL 16 answers), `22030` duplicate key under the strict policy, `54000` over a limit, `XX001` corrupt stored cell. Duplicate keys resolve last-wins and are logged as a `jsonb_duplicate_keys_dropped` edge line (D6b). DDL: `jsonb` and `json` create a CQL `jsonb` column (`json` is stored as jsonb, D11); Off a standalone node such DDL is refused `0A000` by `Schema::check_create_table_jsonb` (T-300, D24). An unspecified-OID binary-format parameter is decoded as text: clients get 3802 from `Describe`, and a raw Bind without one fails 22P02 rather than writing.

### Slice acceptance evidence (T-301)

Acceptance for the PG-first slice (D24): `CREATE TABLE ... jsonb` through PG DDL;
`INSERT` by literal, text `$1` and binary `$1`; `SELECT` in text and binary
format. `tests/jsonb_slice.rs` runs without infrastructure. The differential
oracle sends the identical statements to a postgres:16 container and to ferrosa
and compares byte for byte (`SELECT doc::text` there, `SELECT doc` here) with no
tolerance. Live run (podman, `FERROSA_TEST_CONTAINERS=1`): 80 corpus cases x 3
paths = 240 runs, 0 differences, 0 stale expectations. The oracle found two
differences, both fixed: a `\u0000` escape was accepted (Postgres: `22P05`), and
a bad binary version byte answered `22P03` (Postgres: `XX000`; `08P01` for an
empty value). There are no named divergences: `NAMED_DIVERGENCES` in the oracle is
empty and fails if an entry stops differing.
