---
crate: ferrosa-postgres
status: developer-preview
last_updated: 2026-09-27
executive_summary: >
  The PostgreSQL v3 wire-protocol front-end for ferrosa. Implements the
  frontend/backend protocol (startup, SCRAM-SHA-256, simple + extended query),
  and lowers SQL onto the bespoke ferrosa-sql relational engine over live
  ferrosa storage. SELECT (incl. one JOIN) and single-row INSERT/UPDATE/DELETE
  are supported; it shares the storage row codec with CQL via ferrosa-row-bridge
  (D10) and is differential-tested against real PostgreSQL 16. Explicit
  PostgreSQL SERIALIZABLE transactions use MVCC snapshots with read-your-writes
  and conflict validation. In standalone mode, validation uses local table
  epochs. In cluster mode, a global PostgreSQL marker conservatively rejects a
  snapshot if any PostgreSQL data transaction committed after it; the supplied
  table set is not yet used for per-table Accord validation. Explicit isolation
  modes other than SERIALIZABLE fail with `0A000`. Accord applies PostgreSQL
  row-version metadata to every replica. CQL/Cassandra transactions remain on
  Accord's existing path. The native-driver Jepsen workload records transfer,
  register, predicate/phantom, and write-skew histories, including a single
  replica pause. That schedule checks the history and final-state convergence
  on the active quorum; it does not assert catch-up of the resumed replica or
  mixed CQL/PostgreSQL serializability. `ON CONFLICT` remains unsupported.
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
| `connection` (`src/connection.rs`) | ~520 | Sans-IO `Connection`: startup/`SSLRequest` (`TlsPolicy`)/SASL → `Ready`; `take_inbuf` for pipelined first query |
| `authz` (`src/authz.rs`) | ~180 | Statement → required `(Permission, Resource)`; `authorize` via `Schema::check_permission`, `42501` on denial |
| `extended` (`src/extended.rs`) | ~453 | Per-connection `Session`: Parse/Bind/Close/Sync, prepared statements + portals, txn `I`/`T`/`E` |
| `query` (`src/query.rs`) | ~1927 | `execute_query`, DML (INSERT/UPDATE/DELETE), value codecs (text+binary), SQLSTATE mapping, `load_catalog` |
| `storage_provider` (`src/storage_provider.rs`) | ~758 | `load_table`: bounded async-to-sync streaming provider; `cql_to_value`; R15 guard |
| `catalog` (`src/catalog.rs`) | ~537 | `pg_catalog` projection (`pg_namespace`/`pg_class`/`pg_attribute`/`pg_type`) with deterministic OIDs |
| `server` (`src/server.rs`) | ~540 | tokio TCP front-end: `serve`, `QueryContext`, `handle_connection`, the post-auth query loop (wakes to expire idle suspended portals) |
| `result_stream` (`src/result_stream.rs`) | ~600 | Pull-driven result delivery: an owned `RowCursor` fetched 16 rows at a time on a blocking thread; a waiting query holds no thread |
| `portal_limits` (`src/portal_limits.rs`) | ~380 | `PortalLimits`/`SuspendedPortals`: per-connection and per-node caps on suspended portals (`53000`), idle timeout (`57014`), metrics |
| `lib` (`src/lib.rs`) | ~37 | Module wiring + public re-exports |

## Connection lifecycle

```text
TCP accept (server::serve)
  → handle_connection
     Phase 1: Connection::on_bytes drives startup + SCRAM until ReadyForQuery
       SSLRequest        → 'S' + rustls handshake (PgTls configured) | 'N' (no cert)
       Startup           → FATAL 28000 if require_tls and no TLS
                         → limiter admit (FATAL 28000 "login throttled") → AuthenticationSASL
       SASLInitial/Final → bad proof: record_failure, FATAL 28P01
                         → complete_login (NOLOGIN refused) → AuthenticationOk + … + ReadyForQuery
     Phase 2: query_loop (as the authenticated AuthContext) frames Q / Parse / Bind /
       Describe / Execute / Sync / Close / Terminate; authz::authorize before any
       statement touches storage (42501 on denial)
```

Note: the sans-IO `connection::Connection` also contains a minimal `Ready`-phase
fallback (it answers `Q` with `0A000` "not yet implemented"); the **real**
post-auth path is `server::query_loop`, which is what every driver test and the
differential oracle exercise.

## Data flow

**Read path (`SELECT`):** SQL string → `ferrosa_sql::parse_statement` →
`query::load_catalog` resolves every referenced table (FROM + optional JOIN) by
opening each referenced table as a bounded-channel storage provider. The scan
producer decodes storage partitions as the synchronous executor pulls rows;
the scan channel capacity defaults to 64 and is configurable. The executor is
an owned `ferrosa_sql::RowCursor`, pulled 16 rows at a time on a blocking
thread (`result_stream`), with the next fetch started as each batch arrives;
each batch is encoded and written to the socket, so response memory is
O(batch). A fetch never waits on the client, so a query waiting for its client
(suspended portal, undrained socket) holds no thread; its storage scan pauses
after 10 ms and holds none either (ferrosa-storage ST-68). Extended `Execute`
honours `max_rows` with `PortalSuspended`; suspended portals are capped per
connection and per node (`53000` past either) and closed after an idle
timeout (`portal_limits`, PG-14/PG-15). → `RowDescription`
+ streamed `DataRow`s + `CommandComplete "SELECT n"`. The caller appends one
`ReadyForQuery`.

**Write path (`INSERT`/`UPDATE`/`DELETE`):** parse → resolve each value to a
`CqlValue` driven by the target column's `CqlType` (`value_to_cql`, fail-loud on
type mismatch `42804` / out-of-range `22003`) → `build_decorated_key` +
`build_row`/`build_delete_row` (the SAME `ferrosa-row-bridge` encoder the engine
and CQL decode) → `Mutation` → `engine.write_atomic_batch` → `CommandComplete
"INSERT 0 1"` / `"UPDATE 1"` / `"DELETE 1"`.

See [data-flow.md](data-flow.md) for the sequence diagrams.

## Type model & wire parity

One module, `pg_types`, maps every `CqlType` to a `PgType { oid, typname, typlen,
column_type, binary, text_rendered }` through an exhaustive match (T-023). It
replaced `catalog::type_oid`/`type_name`, `storage_provider::engine_column_type`
and `query::cql_type_to_column_type`/`column_type_size`. CQL `float` and `double`
both map to `float8` (701): the engine's one float column carries an `f64`
(this fixed the drift of board task t_cd417149, where the storage provider typed
them `text`). Collections, tuples, vectors, UDTs and `duration` are named arms
that map to `text` with `text_rendered` set. A stored type string that does not
resolve is a `PgTypeError` (catalog projection, parameter inference), never a
silent `text`. `jsonb` (3802) is a full type with a wire codec (T-161a, see the jsonb section);
`json` (114) and `jsonpath` (4072) have `ALL_PG_TYPES` entries without one (T-161b).

**DDL.** `Statement::CreateTable` executes in `ddl.rs`: `plan_create_table`
builds the `TableMetadata` (first key column = partition key, rest = ascending
clustering columns; types via `cql_type_for_pg_name`), and `ClusterDdl` applies it
through `ferrosa_cluster::ddl_path::DdlPath`, the path CQL DDL uses (Raft in
cluster mode). `QueryContext.ddl` carries the executor into `ReadEnv`. Parse
errors map to typed SQLSTATEs (`query::parse_error_sqlstate`). See FMEA
`PG-T132a-*`.

`query` renders/parses each `ferrosa_sql::Value` to/from its exact Postgres text
form and (for most) the binary form, with OIDs/sizes advertised in
`RowDescription`: `Int→int4(23)`, `Text→text(25)`, `Bool→bool(16)`,
`Float→float8(701)`, `Uuid→uuid(2950)`, `Bytea→bytea(17)`,
`Timestamp→timestamp(1114)`, `Date→date(1082)`, `Time→time(1083)`,
`Inet→inet(869)`, `Numeric→numeric(1700)`. Binary `numeric` and unsupported
composites are rejected explicitly: the server does not send text bytes under a
binary numeric OID or turn stored collection/duration values into SQL NULL.
Bound parameters decode through `decode_param_checked` and fail loud: `22P02`
(text value does not parse), `22P03` (malformed binary), `42704` (non-zero OID
with no mapping, e.g. jsonpath/timestamptz; jsonb 3802 and json 114 are mapped since T-161a), `0A000` (binary numeric). Only
OID 0 (unspecified) is taken as UTF-8 text; nothing becomes NULL on error.
The storage value bridge (`cql_to_value`) maps supported CQL scalars onto this
model and reports a scan error for values without a representation.

## Key invariants

## PostgreSQL MVCC resource bounds

At startup, `FERROSA_POSTGRES_MAX_TXN_WRITES` sets the per-transaction buffered
mutation cap (default 10,000), `FERROSA_POSTGRES_SCAN_BUFFER_ROWS` sets the
storage-to-executor row channel capacity (64), and
`FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS` expires older active snapshots
(600,000 ms). `FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS` controls the
expiry/pruning sweep cadence (1,000 ms). Invalid values log an error and select
the complete default set without stopping startup. Expired transactions fail
on subsequent snapshot validation with SQLSTATE `40001`. Tuning guidance and
query-materialization caveats are in the public
[`PROFILE.md`](../../PROFILE.md).

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
5. **Async storage, sync engine.** The provider bridges the async storage scan
   to the sync executor through a bounded channel and blocking iterator, and the
   executor's rows return to the async side through another. Neither the scan
   nor the result is ever gathered.

## Position in the dependency graph

Depends on `ferrosa-common`, `ferrosa-row-bridge`, `ferrosa-schema`,
`ferrosa-sql`, `ferrosa-sstable`, `ferrosa-storage`. Depended on by `ferrosa`
(the main binary). See the [root crate index](../../specs/crates.md) for the full
graph.

## jsonb (T-150, T-160, T-161a)

`CqlType::Jsonb` maps to the engine `ColumnType::Jsonb` (OID 3802, typlen -1, `binary: true`; `json` 114, `jsonpath` 4072 and `text[]` 1009 have `ALL_PG_TYPES` entries, all `binary: false`). `storage_provider::cql_to_value` and `query::value_to_cql` move the validated cell across unchanged as `Value::Jsonb` (T-160). The wire codec is `jsonb_wire` (T-161a): text format is the PostgreSQL 16 jsonb text (`TextStyle::PgText`, D26; `{"aa":2,"b":1}` prints `{"b": 1, "aa": 2}`, scale kept); binary format is `0x01` then that text (`jsonb_send`). `render_value` never returns NULL for a jsonb value: a corrupt cell is `XX001`, an over-budget print `54000`, and jsonpath/`text[]` are `0A000` (T-161b). Input is parsed with `ferrosa_jsonb::parse_text_observed` under `QueryContext::jsonb_limits` (resolved from `[jsonb]` in `ferrosa/src/main.rs`, no default): a parameter declared 3802 or 114 (or bound to a jsonb column after `Describe`) is parsed in `decode_param_checked`; an untyped string literal or a text-declared parameter bound to a jsonb column is parsed in `value_to_cql`. SQLSTATEs: `22P02` invalid JSON (byte offset, input never echoed), `22P03` bad or missing binary version byte, `22030` duplicate key under the strict policy, `54000` over a limit, `XX001` corrupt stored cell. Duplicate keys resolve last-wins and are logged as a `jsonb_duplicate_keys_dropped` edge line (D6b). DDL: `jsonb` and `json` create a CQL `jsonb` column (`json` is stored as jsonb, D11); Off a standalone node such DDL is refused `0A000` by `Schema::check_create_table_jsonb` (T-300, D24). An unspecified-OID binary-format parameter is decoded as text: clients get 3802 from `Describe`, and a raw Bind without one fails 22P02 rather than writing.

T-301 acceptance: `tests/jsonb_slice.rs` (no infrastructure) and the `differential_oracle_jsonb_*` tests compare ferrosa with postgres:16 byte for byte over jsonb DDL, INSERT (literal, text `$1`, binary `$1`) and SELECT (text, binary). See the README section "Slice acceptance evidence (T-301)".
