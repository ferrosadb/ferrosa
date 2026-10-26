---
crate: ferrosa-postgres
status: developer-preview
last_updated: 2026-10-26
executive_summary: >
  The PostgreSQL v3 wire-protocol front-end for ferrosa. Implements the
  frontend/backend protocol (startup, SCRAM-SHA-256, simple + extended query),
  and lowers SQL onto the bespoke ferrosa-sql relational engine over live
  ferrosa storage. SELECT (incl. one JOIN) and INSERT (one or more rows,
  `VALUES (…),(…)` applied atomically) / UPDATE / DELETE
  are supported; it shares the storage row codec with CQL via ferrosa-row-bridge
  (D10) and is differential-tested against real PostgreSQL 16. Explicit
  PostgreSQL SERIALIZABLE transactions use MVCC snapshots with read-your-writes
  and conflict validation. In standalone mode, validation uses local table
  epochs. In cluster mode, a global PostgreSQL marker conservatively rejects a
  snapshot if any PostgreSQL data transaction committed after it; the supplied
  table set is not yet used for per-table Accord validation. Direct/default and
  explicit READ COMMITTED use statement snapshots and bounded local writer
  ownership; cluster READ COMMITTED remains a separate implementation requirement.
  REPEATABLE READ and explicit cluster READ COMMITTED currently fail with `0A000`.
  Accord applies PostgreSQL
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
| `query` (`src/query.rs`) | ~2000 | `execute_query`, DML (INSERT/UPDATE/DELETE), `execute_truncate` (table-tombstone `TRUNCATE`), value codecs (text+binary), SQLSTATE mapping, `load_catalog` |
| `storage_provider` (`src/storage_provider.rs`) | ~758 | `load_table`: bounded async-to-sync streaming provider; `cql_to_value`; R15 guard |
| `catalog` (`src/catalog.rs`) | ~956 | `pg_catalog` projection (`pg_namespace`/`pg_class`/`pg_attribute`/`pg_type`/`pg_index`/`pg_constraint`) with deterministic OIDs. Ordinary columns take attnums 1..n; a reserved `_sys_` column takes a NEGATIVE attnum, as Postgres numbers its own system columns, so it cannot shift every other column's ordinal. `pg_index`/`pg_constraint` carry a table's *primary key* (a `<table>_pkey` index row in `pg_class` too); a table whose only key is the synthesized `_sys_ck_` gets none |
| `server` (`src/server.rs`) | ~540 | tokio TCP front-end: `serve`, `QueryContext`, `handle_connection`, the post-auth query loop (wakes to expire idle suspended portals) |

**`::` casts.** A cast target is resolved BEFORE planning (`query::resolve_casts`):
`<name>::pg_catalog.regclass` becomes the relation's catalog OID via `catalog::resolve_regclass`, matching
what the `pg_catalog` projection reports. Unsupported targets, and `CAST(x AS t)`, are refused by name.
`load_catalog_with_mvcc` serves a query's `FROM pg_catalog.*` from `catalog_tables`.

| `result_stream` (`src/result_stream.rs`) | ~600 | Pull-driven result delivery: an owned `RowCursor` fetched 16 rows at a time on a blocking thread; a waiting query holds no thread |
| `portal_limits` (`src/portal_limits.rs`) | ~380 | `PortalLimits`/`SuspendedPortals`: per-connection and per-node caps on suspended portals (`53000`), idle timeout (`57014`), metrics |
| `synthetic_key` (`src/synthetic_key.rs`) | ~147 | Mints the per-row v1 TimeUUID value for the synthetic `_sys_ck_` key column of a PK-less table: random per-process node field, monotonic time field, process-local clock sequence |
| `pg_key` (`src/pg_key.rs`) | ~168 | The **declared** PostgreSQL primary key (`pg.primary_key` extension), which is not the storage key: a PK-less table reports none, and `ALTER TABLE ADD PRIMARY KEY` gives one the storage key does not have |
| `ddl` (`src/ddl.rs`) | ~823 | PG DDL execution (`CREATE`/`DROP`/`ALTER TABLE`) through the same `ferrosa_cluster::ddl_path::DdlPath` CQL DDL uses |
| `lib` (`src/lib.rs`) | ~37 | Module wiring + public re-exports |

## Connection lifecycle

```text
TCP accept (server::serve)
  → handle_connection
     Phase 1: Connection::on_bytes drives startup + SCRAM until ReadyForQuery
       SSLRequest        → 'S' + rustls handshake (PgTls configured) | 'N' (no cert)
       Startup           → FATAL 28000 if require_tls and no TLS
                         → limiter admit (FATAL 28000 "login throttled") → AuthenticationSASL
                         → no verifier AND no login role on the node: FATAL 28000
                           "no roles are configured on this node, …" (ADR-024),
                           NOT charged to the limiter
                         → no verifier on a node that HAS login roles: FATAL 28P01,
                           record_failure
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
**consults the cluster**: when the deployment has a range-read path
(`QueryContext::cluster_read`, wired by the binary to the live
`ferrosa_cluster::write_path::WritePath`), the producer opens
`WritePath::range_read_stream_all_with` at the level
`cluster_read::read_consistency()` resolves (`FERROSA_PG_READ_CONSISTENCY`,
defaulting to `ConsistencyLevel::Quorum`) with the keyspace's replication
strategy, so the
scan fans out across the ring instead of reading only this node's local
`StorageEngine`. A cluster read that cannot reach the replicas the level
requires is a **loud failure** recorded in `ScanFailure` (`58000`) — never a
downgrade to a local-only scan, which would be a silent subset. A standalone
node, and any in-process unit test (`ClusterRead::disabled()`), keeps the local
`range_iter` path, which IS complete there. The scan
producer decodes storage partitions as the synchronous executor pulls rows;
the scan channel capacity defaults to 64 and is configurable. The executor is
an owned `ferrosa_sql::RowCursor`, pulled 16 rows at a time on a blocking
thread (`result_stream`), with the next fetch started as each batch arrives;
each batch is encoded and written to the socket, so response memory is
O(batch). A fetch never waits on the client, so a query waiting for its client
(suspended portal, undrained socket) holds no thread; its storage scan pauses
after 10 ms and holds none either (ferrosa-storage ST-82). Extended `Execute`
honours `max_rows` with `PortalSuspended`; suspended portals are capped per
connection and per node (`53000` past either) and closed after an idle
timeout (`portal_limits`, PG-14/PG-15). Each projection item's output alias
(`expr AS name`, or a bare `expr name`) becomes its `RowDescription` field name
while the row cells are unchanged. → `RowDescription`
+ streamed `DataRow`s + `CommandComplete "SELECT n"`. The caller appends one
`ReadyForQuery`.

**Write path (`INSERT`/`UPDATE`/`DELETE`):** parse → resolve each value to a
`CqlValue` driven by the target column's `CqlType` (`value_to_cql`, fail-loud on
type mismatch `42804` / out-of-range `22003`) → `build_decorated_key` +
`build_row`/`build_delete_row` (the SAME `ferrosa-row-bridge` encoder the engine
and CQL decode) → `Mutation`. An `UPDATE` whose SET right-hand side reads a column
(`SET c = c + 1`) first resolves its key `WHERE`, does a **read-modify-write**
(`read_current_row_for_update`: committed point read + the transaction's own buffered
writes overlaid), evaluates each side with `eval_set_expr`/`apply_arith` and writes the
result (`42883` for an operand without an operator, `22003` on integer overflow, NULL
propagating); a constant SET keeps the blind-upsert path and reads nothing. An `INSERT`
of N rows builds and validates EVERY
row first, then applies the whole set as ONE atomic batch (`apply_batch_or_buffer`):
**autocommit** → `engine.write_atomic_batch` (or one MVCC commit), **in a
transaction** → buffer the rows into the write-set for one commit at `COMMIT`.
The statement announces its count only when every row landed → `CommandComplete
"INSERT 0 N"` / `"UPDATE 1"` / `"DELETE 1"`.

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

**TRUNCATE / VACUUM / ANALYZE.** `Statement::Truncate` executes in
`query::execute_truncate`. Each named table is truncated by writing ONE
**table-level tombstone** — a reserved-partition `Mutation`
(`ferrosa_storage::table_tombstone::table_tombstone_row`) — through the SAME
`apply_or_buffer` seam every DML write uses, so it is a **normal replicated
write**; the front-end never truncates locally (never a node-local
`StorageEngine::truncate`, which would empty one replica and leave the others
holding the old data). `Statement::Truncate` is in the "data statement" set, so in
autocommit it is wrapped in the implicit transaction and, in cluster mode, commits
through the cluster (`AccordTransactionCommitter`) like any other write;
standalone it applies locally. `Statement::Vacuum` flushes the
named table's memtables and submits compaction (`force_compact_all`, all tables)
before answering `CommandComplete "VACUUM"` — asynchronous, and reclamation depends
on the purge policy, so the tag does not mean "space reclaimed". `Statement::Analyze`
is answered `"ANALYZE"` and collects no statistics.

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

DML literals bind through `query::value_to_cql`, coercing each value to its target
column's CQL type. A `numeric` column accepts an integer literal (widened at scale
0) and a decimal literal (recovered from its shortest round-trip text, `1.5` →
unscaled 15 / scale 1), plus a value arriving as TEXT — an untyped string literal or
a COPY FROM STDIN payload cell — via the same `parse_numeric_text` the numeric
text-parameter path uses. The accepted set is widened, not loosened: a non-numeric
string into `numeric` is `22P02`, and any other type mismatch is `42804`.

## Key invariants

## PostgreSQL MVCC resource bounds

At startup, `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES` sets the resident buffer a
transaction's write-set may hold before it SPILLS to a staging file (default
8 MiB — a streaming buffer size, never a cap), `FERROSA_POSTGRES_SCAN_BUFFER_ROWS`
sets the storage-to-executor row channel capacity (64), and
`FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS` expires older active snapshots
(600,000 ms). `FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS` controls the
snapshot-registry expiry cadence (1,000 ms). Invalid values log an error and select
the complete default set without stopping startup. Expired transactions fail
on subsequent snapshot validation with SQLSTATE `40001`. Tuning guidance and
query-materialization caveats are in the public
[`PROFILE.md`](../../PROFILE.md).

MVCC publishes persistent `imbl` HAMT roots through `arc-swap`. Active snapshots
own structurally shared roots and immutable `Arc` row payloads. Dropping the last
lease or expiring a snapshot removes its registry root; neither path sweeps
unrelated row histories. Local commits reserve changed keys and their point/scan
read scopes before storage apply, publish before-images before any physical
change, and invoke storage exactly once outside metadata CAS retries. Disjoint
writers and snapshot reads can progress while another storage apply pauses.
Overlapping reservations or stale reads fail with `40001` before storage effects.



Cursor resolution and the first bounded row fetch run in one blocking job.
A one-shot column publication preserves early metadata without requeueing the
cursor. The stream owns cancellation before awaiting that publication, so a
dropped opening future stops its producer. Queue-order and cancellation
regressions exercise the production opening path with one blocking worker.

Owned decoded storage rows move text, ASCII, blob, numeric and validated JSON
payloads into SQL values. The borrowed conversion API remains available for
callers retaining their input. Production-path identity/capacity tests require
text/blob allocations to survive the conversion unchanged; scalar/error results
match the borrowed conversion. This removes one intermediate payload copy and
does not change cluster reads, snapshot selection or wire encoding.

Fresh local inserts represent their pre-insert absence once in the current head.
An older snapshot missing that key resolves this shared absence without copying
an entry into every reader root. First-touch rows that already exist physically
still publish their before-image to active roots before storage apply; later
updates preserve the original absence fact and each snapshot's existing image.
The allocation regression compares 128/256-key commits with 1 versus 16 held
readers, and a paused-apply test checks both absent and pre-existing rows.
Storage/WAL work itself may block. Cluster prepare publishes timestamped images;
apply retains each key's latest applied image and future prepares, while active
roots retain their historical images. Applied-transaction replay is idempotent. A new cluster snapshot requesting
history older than a retained head fails with `40001` instead of falling through
to current physical storage.

Streaming providers resolve the live snapshot root after each physical read,
restore deleted rows once, and keep staged own writes ahead of snapshot images.
Restoration enumerates shared key-only roots and resolves each payload through
the live registry, so a held iterator cannot pin an expired historical table.
They retain an emitted-key set of O(scanned keys) for late-delete reconciliation,
in addition to the bounded row channel; they do not retain the scanned rows.
Cancellation drops the producer's keys and lease. Expiry is checked during the
scan and returns `40001`. Current heads, active roots, outstanding prepares and
the applied-transaction identity set remain resident; this is no total MVCC
memory cap or durable version store.

### Coherence with the consensus write path

A buffered transaction is not committed here — it is committed by Accord, which
registers an incoming transaction in its conflict index **under every key** and
all-or-nothing. Those limits must therefore agree, or the front end accepts a
transaction consensus cannot register: the PreAccept is refused by every replica,
the coordinator collects zero votes, and the client sees only the opaque
`Accord quorum unavailable`.

The front end no longer refuses on its own: `Session.txn_writes` is a spilling
`TxnWriteSet`, so a write-set past `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES` SPILLS
rather than being declined (FMEA PG-12). The old
`FERROSA_POSTGRES_MAX_TXN_WRITES` / SQLSTATE `53400` refusal and the knob that
carried it are both GONE; the consensus conflict index is likewise unbounded
(grown, not refused — see `ferrosa-cluster` CL-49), so the two no longer
disagree. The consensus-side `FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY` setting was
removed with the index's fixed capacity.

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
6. **`TRUNCATE` is a replicated table tombstone, and its scope is the whole
   ring.** `TRUNCATE` writes ONE reserved-partition table tombstone through the
   ordinary `apply_or_buffer` write seam, so it is transactional (buffers in a
   `BEGIN`, applies on `COMMIT`, discarded by `ROLLBACK`) and replicated as a
   write. It is *logically immediate* (reads return no rows the moment it commits,
   table-wide, via `apply_table_deletion`) but *physically lazy* (bytes go at the
   next compaction); `TRUNCATE` then `VACUUM` is strictly equivalent in effect to
   an immediate truncate, and that split is deliberate for client compatibility.
   In cluster mode the tombstone's commit is replicated to **every node serving
   the table at `ConsistencyLevel::All`**: `CL=ALL` alone is not sufficient,
   because the ordinary write path would route the marker by its reserved key's
   token to that key's **RF replica set** — a proper subset of the ring when
   `RF < node count` — leaving the nodes outside it serving the truncated rows. So
   the marker is *also* fanned out to the **whole ring**
   (`WritePath::write_all_serving_nodes`) and **every** target must acknowledge;
   a node that does not ack fails the commit **loudly** — no quorum degrade and no
   hint fallback. The reserved marker is exempt from purge, so nothing can be
   resurrected; it lives until the table is dropped. The marker is also **skipped
   when a transaction's MVCC row images are built** — the commit row-image build
   and the transaction read overlay share
   `apply_pending_writes_with_partition_keys` — because its reserved key is marker
   magic bytes, not a value of any key column, so decoding it as a data row would
   fail loud for a PK-less (synthetic `_sys_ck_` uuid) or `int`-keyed table. That was
   the `COMMIT` failure that killed `pgbench -i`'s transactional load after every
   row had landed. See FMEA `PG-TRUNCATE-03`.

## Position in the dependency graph

Depends on `ferrosa-common`, `ferrosa-row-bridge`, `ferrosa-schema`,
`ferrosa-sql`, `ferrosa-sstable`, `ferrosa-storage`. Depended on by `ferrosa`
(the main binary). See the [root crate index](../../specs/crates.md) for the full
graph.

## jsonb (T-150, T-160, T-161a)

`CqlType::Jsonb` maps to the engine `ColumnType::Jsonb` (OID 3802, typlen -1, `binary: true`; `json` 114, `jsonpath` 4072 and `text[]` 1009 have `ALL_PG_TYPES` entries, all `binary: false`). `storage_provider::cql_to_value` and `query::value_to_cql` move the validated cell across unchanged as `Value::Jsonb` (T-160). The wire codec is `jsonb_wire` (T-161a): text format is the PostgreSQL 16 jsonb text (`TextStyle::PgText`, D26; `{"aa":2,"b":1}` prints `{"b": 1, "aa": 2}`, scale kept); binary format is `0x01` then that text (`jsonb_send`). `render_value` never returns NULL for a jsonb value: a corrupt cell is `XX001`, an over-budget print `54000`, and jsonpath/`text[]` are `0A000` (T-161b). Input is parsed with `ferrosa_jsonb::parse_text_observed` under `QueryContext::jsonb_limits` (resolved from `[jsonb]` in `ferrosa/src/main.rs`, no default): a parameter declared 3802 or 114 (or bound to a jsonb column after `Describe`) is parsed in `decode_param_checked`; an untyped string literal or a text-declared parameter bound to a jsonb column is parsed in `value_to_cql`. SQLSTATEs: `22P02` invalid JSON (byte offset, input never echoed), `22P03` bad or missing binary version byte, `22030` duplicate key under the strict policy, `54000` over a limit, `XX001` corrupt stored cell. Duplicate keys resolve last-wins and are logged as a `jsonb_duplicate_keys_dropped` edge line (D6b). DDL: `jsonb` and `json` create a CQL `jsonb` column (`json` is stored as jsonb, D11) on every deployment mode — there is no capability gate (the former off-standalone refusal, T-300/D24/D32, was REMOVED, not disabled; jsonb is a first-class data type). The one surviving jsonb DDL constraint is placement (T-154a, D3): jsonb is still refused in a key column. An unspecified-OID binary-format parameter is decoded as text: clients get 3802 from `Describe`, and a raw Bind without one fails 22P02 rather than writing.

T-301 acceptance: `tests/jsonb_slice.rs` (no infrastructure) and the `differential_oracle_jsonb_*` tests compare ferrosa with postgres:16 byte for byte over jsonb DDL, INSERT (literal, text `$1`, binary `$1`) and SELECT (text, binary). See the README section "Slice acceptance evidence (T-301)".

### Extended-protocol transaction boundary

Extended protocol: each implicit pipeline owns one snapshot and staged write-set until `Sync`. `handle_frontend` commits or rolls it back at that boundary; each `Execute` does not independently publish its write. Explicit transactions retain their existing COMMIT/ROLLBACK boundary.


Scalar primary-key SELECTs now request exactly one physical partition for
literal and bound-parameter equality, using the canonical write-side key codec.
The snapshot tail resolves only that row, including a late delete and own staged
insert. Cluster reads use `WritePath::pk_read` with the configured consistency
and replication strategy; failures propagate without a local fallback. Joins,
compound/clustering keys, floating/decimal keys and other unproved key families
retain scans. Floating signed zero cannot use byte-key equality because SQL
equates both signs. Selective-decode, recording-reader and snapshot tests cover
this path. [Current PR #613 results](../../deploy/fly-bench/pipeline/RESULTS.md)
measure roughly 90% of PostgreSQL throughput in the small warm workload and
retain the local lookup gap. The [experiment record](../../deploy/fly-bench/pipeline/HISTORY.md)
contains earlier revisions, staged measurements and placement controls.


### Response buffering and encoding

The connection reuses one response buffer, flushing at Sync, simple-query completion,
explicit Flush (including error-pending state), and an 8 KiB streaming threshold.
COPY flushes pending replies before its input handshake. Common scalar DataRows
encode directly into that buffer; specialized codecs retain their checked fallback.
A failed row rolls back its partial bytes while preserving earlier rows. Collecting
ReplySink callers keep the existing message interface. Ownership/allocation tests,
wire parity, bounded streaming, Sync batching and explicit Flush tests cover this
change. Current measurements and limits are in [the benchmark results](../../deploy/fly-bench/pipeline/RESULTS.md).


### Row validation and MVCC replacement

Storage reads retain their configured consistency path and complete shared CQL
decoding. A borrowed validation pass preserves SQL representability errors before
MVCC lookup. Only key fields become SQL values before lookup; a full SQL row is
materialized when no MVCC or staged image replaces it. This removes throwaway SQL
row materialization, while physical CQL decoding and the owned MVCC image copy
remain. Tests pin zero-allocation payload validation, conversion-error parity, and
a corrupt physical payload failing even when MVCC holds a valid replacement.
The [experiment record](../../deploy/fly-bench/pipeline/HISTORY.md) retains the staged measurements.

## Transaction timestamp expressions (t_6b946676)

`CURRENT_TIMESTAMP` and `now()` work in no-FROM SELECTs and DML scalar values,
including stock pgbench history INSERTs and UPDATE assignments. The session
captures a statement wall clock before execution; BEGIN pins that clock until
COMMIT/ROLLBACK. An implicit extended-protocol pipeline keeps its clock until
Sync. Autocommit statements capture a fresh clock. Parse/Describe do not freeze
prepared values; Execute evaluates them using its transaction's clock.

Clock SELECT results advertise `timestamptz` (OID 1184), with UTC text (`+00`) and
PostgreSQL-epoch binary microseconds. Assignment to a `timestamp` column uses UTC
and RETURNING retains OID 1114. The SQL column type distinguishes the two temporal
types; both use the existing microsecond value representation. Persisted CQL
`timestamp` cells still have millisecond precision. `timestamptz` table DDL,
non-UTC session timezone conversion, precision arguments, and other clock
functions remain unsupported. Unknown functions fail loudly.


### PostgreSQL table incarnation isolation (2026-10-26)

PostgreSQL MVCC uses `(qualified table name, TableMetadata.id)` for images, row conflicts, scan epochs and reservations. Statement-bound UUIDs accompany buffered writes and survive write-set spill. Local COMMIT validates the entire set before storage effects while holding a synchronous shared schema lifecycle lease; a retired incarnation aborts the transaction with SQLSTATE `40001`, including its unrelated writes. Reads bind the same UUID and refuse rebinding a retained snapshot to a replacement. Unrelated table histories remain intact.

Accord partition metadata and row images carry the UUID in the canonical Cap'n Proto schema. The replica apply lease spans validation, storage apply and MVCC publication. Missing/invalid legacy UUID metadata is rejected, including replay; it is never assigned the current table ID. Upgrade requires draining/checkpointing legacy PostgreSQL Accord work and upgrading all participating readers/writers together. Additive fields alone do not make mixed old/new binaries safe: old readers ignore the UUID. This change does not rewrite historical metadata or introduce a storage-WAL migration.

The existing distributed TRUNCATE all-serving fan-out bypasses Accord MVCC observers. Its raw marker protocol is unchanged; although buffered/local incarnation checks apply, this change does not fence that remote fan-out against concurrent remote DROP.


### PostgreSQL index DDL readiness boundary (t_9fba4678)

`ClusterDdl::create_index` applies schema DDL through `DdlPath`, then on Direct
awaits engine declaration and complete tracker readiness, with a 30-second
post-apply deadline. It checks the pre-apply table UUID under a short synchronous
schema lifecycle read lease on each observation; no lease spans an await. A
retired table, missing registration, build failure, or changed deployment path
returns an error. SSTable retirement follows the tracker's replacement coverage;
the wait does not cache the original pending generation list or mark anything
current. Future cancellation drops only the wait; schema/backfill remain durable.

Pair/cluster index DDL is refused before index creation because DdlPath schema
acknowledgements have no all-serving-replica build readiness guarantee. Completing
that protocol needs incarnation-aware replica evidence and membership fencing
outside Raft apply. The SQL CREATE INDEX parser and wire CancelRequest remain
unfinished. No query, MVCC, timestamp, storage guard or shared apply code changes
are part of this boundary.

Binding a replacement lazily reclaims only retired-incarnation MVCC heads and scan keys. Active snapshots retain their immutable historical roots until release/expiry; unrelated tables are not cleared.

## Local READ COMMITTED (t_3c15a13f)

Default and explicit RC transactions on Direct/single-node execution use a fresh
committed view for each statement, including UPDATE/DELETE candidate selection,
with staged own writes overlaid. A row must match initially before it can wait
for ownership. After waiting, the row and full predicate are re-evaluated before
computing the mutation. The existing streaming provider supplies bounded row
channels and MVCC recovery of deleted rows; stable declared-key index lookups keep
the normal keyed path. A raced index uses the statement's MVCC scan.

`writer_ownership.rs` is a focused child module of `mvcc`: table-UUID-scoped,
1,024 partition hash stripes per live table, owned by the staged transaction and
moved to the commit scope until after MVCC publication. Stable transaction identities
and explicit FIFO predecessor edges detect cycles; the closing requester receives
`40P01`. Default lock timeout is disabled. A short metadata mutex protects atomic
admission/handoff/removal, never evaluation, await or storage I/O. Pending RAII guards
cover grant-before-wakeup cancellation as well as queued cancellation. Payload
staging still spills. Hash collisions can serialize disjoint partitions within a
table (1/1,024 for uniform keys); unrelated tables never share ownership.

Direct RC validates the original lease/incarnations, obtains all remaining writer
scopes, then uses a fresh commit view. SERIALIZABLE uses its original snapshot.
Late synchronous bulk writers keep conservative conflict validation to avoid
acknowledging stale timestamps; full COPY contention semantics remain required.
Execution-future cancellation aborts an explicit transaction and releases its
writers. Error handling preserves `25P02` through Sync until COMMIT/ROLLBACK;
COMMIT of an aborted transaction reports ROLLBACK. SQL transaction timestamps are
unchanged. No schema/cluster/storage internals or dependencies changed.

The local ownership domain does not coordinate remote Accord or CQL writers.
Distributed READ COMMITTED needs distributed ownership and statement-snapshot
ordering; this cycle does not claim cluster parity or add wire CancelRequest.

### Writer admission and original lifetime (t_87e93bce)

Each local manager admits at most 16,384 live writer identities and 262,144 held
plus pending resource references, returning `53200` on exhaustion. Empty entries
are reclaimed. Cycle checks are iterative and bounded by admitted identities;
these budgets are resource limits, not a byte-accurate memory claim. No row payload
enters the graph, and existing 1,024-stripe collision behavior is unchanged.

The original snapshot owns an absolute lifetime and expiry notification, shared
with its writer lease. RC visibility refresh never resets that lifetime. The reaper
publishes expiry notifications only after successful CAS; acquisition and the idle
session perform cleanup. Idle expiry discards writes/read state/portals and retains
the expired snapshot for the next command's `40001`. Waiting expiry also returns
`40001`. Repeated BEGIN preserves staged writes, identity and lifetime. Commit
refresh carries the original lifetime through pre-apply validation; post-apply
expiry cannot revoke ownership or interrupt publication. No statement is retried.

SET/RESET/SHOW positive lock_timeout remains t_cc11529a. Native CancelRequest and
prompt blocked-execution EOF/Terminate remain t_b254fd86. This cycle tests execution
future/drop cancellation only, not wire cancellation. Distributed RC and early
COPY ownership remain t_b6306006 and t_4c825e03.
