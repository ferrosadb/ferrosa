---
crate: ferrosa-postgres
doc: roadmap
last_updated: 2026-10-10
---

# ferrosa-postgres — Roadmap

Sourced from in-code fail-loud `0A000`/preview gaps, the FMEA
([fmea.md](fmea.md)), and the dependency/usage review. There are no `TODO`/
`FIXME` markers in the source — the open work is encoded as fail-loud
`feature_not_supported` paths and documented lossy fallbacks instead.

## Done (recent)

- **The transactional `TRUNCATE` row-image decode fix (FMEA PG-TRUNCATE-03).** Building a
  transaction's MVCC row images — and the transaction's read overlay — used to decode EVERY
  buffered mutation's key as its column type, including the `TRUNCATE` table-tombstone
  marker, whose reserved partition key is marker magic bytes, not a value of any key column.
  A PK-less table (synthetic `_sys_ck_` uuid) or an `int`-keyed one failed loud: `pgbench -i`
  lost its whole `--scale 10` load at `COMMIT` (`build transaction row image failed: uuid
  requires 16 bytes`) *after* all ~1.1M rows had landed; reading the table back inside the
  transaction failed too (`transaction overlay failed: …`), and autocommit failed the same
  way (`write failed: …`). A `text`-keyed table silently accepted the magic as a bogus key,
  which is why the earlier text-keyed `TRUNCATE` tests were green. **Fix:** skip a
  table-tombstone mutation when row images are built
  (`storage_provider::apply_pending_writes_with_partition_keys`, the choke point the commit
  build and the read overlay share) — the marker is not a data row, and the commit path still
  applies it (and, on a cluster, replicates it to every serving node). Tests:
  `truncate_inside_a_transaction_commits_on_a_pkless_table`,
  `reading_inside_a_transaction_after_a_truncate_does_not_decode_the_tombstone`,
  `autocommit_truncate_on_a_pkless_table_commits`,
  `truncate_inside_a_transaction_commits_when_a_tombstone_already_exists`.

- **`::` casts, and `pg_catalog.*` as a queryable relation** (`catalog::resolve_regclass`,
  `query::resolve_casts`, `load_catalog_with_mvcc`). `$1::pg_catalog.regclass` resolves the relation
  name to its catalog OID, so pgbench's object-existence check
  (`SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass`) now runs — it was
  dying as `bad token: :` because the lexer had no `::` token at all. Unsupported cast targets and
  `CAST(x AS t)` are refused by name; `ParseError`→SQLSTATE mapping now runs on the extended-protocol
  parse path too, so those refusals carry `0A000` instead of a blanket `42601`.

- **Enforced `FOREIGN KEY`s, backed by real secondary indexes** (`pg_fk.rs`,
  `ddl::execute_add_foreign_key`, `query::execute_{insert,update,delete}`). `ALTER TABLE …
  ADD [CONSTRAINT <name>] FOREIGN KEY (<col>) REFERENCES <parent> [(<pcol>)]` — with the
  referenced-column list omitted, defaulting to the parent's primary key, exactly what
  `pgbench -i --foreign-keys` emits — now parses (`ferrosa-sql`), is recorded as a
  `pg.foreign_key.<name>` extension on the child, and is **enforced**. `ADD FOREIGN KEY`
  also builds a **real secondary index** over the child's referencing column (through the
  same `DdlExecutor::create_index` path `ADD PRIMARY KEY` uses), so the parent-side check is
  a lookup. The check is a **normal read**: a point read when the referenced column is the
  parent's storage key, otherwise a lookup through the parent's `<parent>_pkey` index (or
  the child's FK index for the parent side). Because the read lands in the read set a
  serializable COMMIT validates — a concurrent delete of the other side bumps that table's
  epoch and fails the commit (`mvcc::validate_snapshot`). Child `INSERT`/`UPDATE` with a
  missing parent is `23503`; parent `DELETE` with live children is `23503`; each names the
  constraint, the child `table.column` and the value, and a present parent is admitted (the
  positive control). `CREATE TABLE … FOREIGN KEY …` / column `REFERENCES` parse in
  `ferrosa-sql` but are **refused by name** (`0A000`) at the front end rather than accepted: a
  CREATE-time FK cannot be enforced (`CREATE TABLE` cannot build the child index the
  constraint needs), so accepting it would record a constraint that is never checked. The
  enforced form is `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`, which `pgbench -i
  --foreign-keys` emits.

  **Refused by name rather than recorded and mis-enforced (`0A000`):** a multi-column FK or
  multi-column referenced key (ferrosa secondary indexes are single-column —
  `build_replicated_index` uses `target_columns.first()`); a referenced column that is not
  the parent's key (would force a scan per check).

  **Unimplemented (named gaps, in priority order):**
  1. **Referential actions.** `ON DELETE`/`ON UPDATE CASCADE`, `SET NULL`, `SET DEFAULT`,
     `MATCH FULL`/`PARTIAL`, `DEFERRABLE`/`INITIALLY`, `NOT VALID` are refused by the parser
     (`0A000`). There is no cascade, no deferral, no skip-validation — a client that asked
     for one is told, never handed a `NO ACTION` constraint in its place.
  2. **A parent row written earlier in the same uncommitted transaction is not visible to the
     child-side check** — the probe reads committed storage, not the session's pending
     write-set. Cross-statement parent-then-child inserts in one transaction (and a
     self-referencing table) can therefore be wrongly refused. This is a REAL semantic
     deviation from PostgreSQL, which sees a transaction's own uncommitted writes; this
     front end is not referential-integrity-equivalent. `pgbench` loads parents and children
     in separate statements that flush, so it is unaffected.
  3. **The parent-side probe iterates every table in the schema** for constraints that
     reference the parent (O(tables) per parent DELETE, not per row). Fine for a handful of
     tables; revisit if a schema grows large.

- **`CREATE TABLE ... WITH (key = value, ...)` (table storage parameters).** pgbench's
  own schema emits `with (fillfactor=100)`, which stopped `pgbench -i` at
  `expected end of statement, found Ident("with")`. `ferrosa-sql` now parses the clause:
  the pure physical-layout hints `fillfactor` and `autovacuum_enabled` are recorded on
  `CreateTableStmt::storage_parameters` and not applied (ferrosa is an LSM store — no
  heap pages, no autovacuum, so there is nothing to configure and no query-visible
  effect to miss); every other option name is refused `0A000` naming it rather than
  dropped, and a bare unparenthesised `WITH` is a loud parse error.

- **Scalar subqueries `( SELECT ... )` in a no-`FROM` select list.** A scalar
  subquery used as a select-list operand (`select (select count(*) from
  pgbench_accounts)||'|'||…`, pgbench's census line) now evaluates end to end.
  `query::execute_scalar_select`/`eval_scalar_value` became async and carry a
  `ScalarReadCtx` (the `ReadEnv` a subquery reads over, plus the session's pending
  writes); `eval_scalar_subquery` runs the inner query and takes its single value
  with PostgreSQL `EXPR_SUBLINK` semantics: no rows ⇒ NULL (distinct from the empty
  string), more than one row ⇒ `21000 cardinality_violation`, more than one output
  column ⇒ `42601` ("subquery must return only one column") refused *before* any
  row. Its column type is the inner query's single output column type (so
  `count(*)` types as int), and `||` still coerces it to text. `ResultStream` grew
  `next_row` for this internal pull. `substitute_param` refuses a subquery in a DML
  value (`0A000`; the grammar never builds one). A subquery is only a no-`FROM`
  select-list operand: a `FROM` relation's projection, `WHERE`/`HAVING`, `VALUES`,
  and nested subqueries are still unsupported (see `ferrosa-sql`'s roadmap).
- **Numeric/decimal literal binding in DML.** `INSERT INTO t (a, b) VALUES (1, 1.5)`
  into a `numeric` column failed `42804 value does not match column type Decimal`,
  which blocked `pgbench -i` and the PostgreSQL smoke check. `query::value_to_cql`
  now widens an integer literal (scale 0) and a decimal literal (recovered from its
  shortest round-trip text) to `CqlValue::Decimal`, and parses a TEXT value — an
  untyped string literal, or a COPY FROM STDIN payload cell — with the same
  `parse_numeric_text` the numeric text-parameter path uses. A non-numeric string is
  still refused `22P02`; the type check is widened, not loosened.
- **Declared primary key visible to a client.** `catalog` now projects `pg_index`,
  `pg_constraint` and a `<table>_pkey` index row in `pg_class`, all built from
  `pg_key::of`. psql's describe-table joins `pg_index.indisprimary` to `pg_class` and reads
  `pg_constraint`, so a declared `PRIMARY KEY` — including one `ALTER TABLE ADD PRIMARY KEY`
  recorded — now shows instead of a keyless table. A table whose only key is the synthesized
  `_sys_ck_` reports **none**: `pg_key::of` returns the DECLARED key, and the rows are built
  from it, so the front end can never advertise ferrosa's internal column as a PostgreSQL key.
  `indkey`/`conkey` carry the same attnums `pg_attribute` gives the columns (a synthesized
  `_sys_ck_` is a negative system attnum; a real key column keeps its positive ordinal).
- **`TRUNCATE` (replicated table tombstone) + `VACUUM` (flush + compact) / `ANALYZE` (no-op)**
  (pgbench `-i`/reset and routine maintenance). `TRUNCATE [TABLE] t [, …]` executes in
  `query::execute_truncate`: each named table is truncated by writing ONE
  **table-level tombstone** (a reserved-partition `Mutation`) through the SAME
  `apply_or_buffer` write seam every DML uses, so it is a transactional, replicated
  write bufferable in a `BEGIN` and discarded by `ROLLBACK` — never a node-local
  `StorageEngine::truncate` that would leave the replicas disagreeing. It is
  *logically immediate* (reads return no rows at once, table-wide) and *physically
  lazy* (bytes go at the next compaction); `TRUNCATE` then `VACUUM` is strictly
  equivalent in effect to an immediate truncate, and that split is deliberate for
  client compatibility. In cluster mode the tombstone is replicated to **every node
  serving the table at `ConsistencyLevel::All`** — `CL=ALL` alone is not enough,
  because the ordinary write path would scope it to the reserved key's RF replica
  set — and the commit fails **loudly** if any serving node does not acknowledge.
  Missing table → `42P01`.
  `VACUUM [FULL] [ANALYZE|ANALYSE]` flushes and submits compaction — in an LSM
  store that is the vacuum, so it is NOT a no-op — then answers `CommandComplete
  "VACUUM"`. Asynchronous: it does not wait for compaction, and reclamation
  depends on the purge policy. `ANALYZE|ANALYSE` answers `"ANALYZE"` and collects
  no statistics, which is a real no-op.

- **PK-less `CREATE TABLE` end to end.** A table that declares no `PRIMARY KEY` gets a
  synthetic `_sys_ck_` column (a v1 TimeUUID, reported as `uuid`) as its partition key, so
  every row is unique by construction. The front end mints a key per row on `INSERT`; the
  column is hidden from `SELECT *` but selectable by name and listed by `pg_attribute` at a
  negative `attnum`. The declared PostgreSQL key is recorded separately
  (`pg_key`/`pg.primary_key`) because it is not the storage key: a PK-less table reports
  *none*, and `ALTER TABLE ADD PRIMARY KEY` will give it one the storage key does not have.

- **PostgreSQL MVCC transaction semantics.** Explicit SERIALIZABLE begins
  pin a read timestamp; simple and extended SELECT use version overlays; buffered
  writes are visible to their own transaction; commit checks table-level read
  and write epochs in standalone mode and atomically applies through the local
  storage engine. In cluster mode a global PostgreSQL commit marker currently
  validates snapshots, conservatively aborting after any intervening PG commit.
  Write skew, predicate conflicts, lost updates, and real-time-order violations
  abort or return the committed value as appropriate; versions are reclaimed
  after the oldest active snapshot advances. Explicit isolation levels other
  than SERIALIZABLE fail loud. CQL transactions remain on Accord.
- **PostgreSQL strict-serializability workload.** The native-driver Jepsen test
  checks transfers, register updates, predicate/phantom histories, and
  write-skew histories. Its fault schedule pauses one replica and verifies the
  history and final state on the active quorum.
- **Parameterized DML** (was FMEA PG-2, `feat/pg-extended-crud`).
  `INSERT`/`UPDATE`/`DELETE` accept bound `$N` parameters over the extended
  protocol: prepared as `PreparedKind::{Insert,Update,Delete}`, substituted at
  `Execute` via `substitute_param` (fail-loud `08P01` on an unbound `$N`), with
  param OIDs inferred from each placeholder's target column. Transactional
  parameterized DML buffers into the same session write-set.
- **`INSERT … RETURNING col,…`/`*`** (part of FMEA PG-3, `feat/pg-extended-crud`).
  Echoes the just-written values as a `DataRow` (built in-memory, no storage
  read-back) — the `Ecto.Repo.insert` generated-key path; works inside a
  transaction (rows returned now, write commits at COMMIT).

## Now (highest value)

- **(done) Non-resident front-end write-set (FMEA PG-12).** `Session.txn_writes` is
  now a `TxnWriteSet` over the streaming, threshold-bounded
  `ferrosa_storage::write_set_stage::WriteSetStage` (spill past
  `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES`), and `COMMIT` drives
  `write_atomic_batch` through the `WriteSetSource` trait so all three passes read the
  set as a stream under the one fsync group — the `Vec<Mutation>` is gone. The
  `FERROSA_POSTGRES_MAX_TXN_WRITES` refusal is removed (a larger write-set spills, never
  refused). **Remaining, named:** `prepare_row_changes` (MVCC history) and the cluster
  branch's `prepare_accord_writes` (Accord apply payload) still materialize their `Vec`s;
  bounding them needs an on-disk version store / a streaming Accord apply.
- **Replica catch-up and mixed-protocol correctness** (FMEA PG-11). The fault
  schedule checks PostgreSQL transaction histories while one replica is
  paused, then checks final-state convergence on the active quorum. Verify
  post-resume Accord catch-up and histories that mix PostgreSQL transactions
  with CQL writes before making those broader claims.
- **End-to-end SELECT streaming.** The storage provider is bounded, but
  `ferrosa_sql` collects base scans/results and PostgreSQL rendering buffers all
  wire messages. Stream through the executor and socket writer to bound memory.

## Next

- **`UPDATE`/`DELETE … RETURNING`** (FMEA PG-3) — today fail loud `0A000`; only
  `INSERT … RETURNING` is wired.
- **`ON CONFLICT` (upsert)** — today a parse error; the common ORM upsert idiom.
- **`= ANY($N)` / IN-list parameter expansion** — Ecto `where: x in ^ids`.
- **Multi-row `INSERT ... VALUES`** — **DONE.** `INSERT` of one or more rows executes
  every row; `execute_insert` builds and validates the whole statement first and applies
  it as ONE atomic batch (`apply_batch_or_buffer`), so it is all rows or none. Asserted by
  value, per row, at the SQL, transaction, and real-3-node-cluster layers (see FMEA
  `PG-MULTIROW-01` and `tests/pg_multirow_cluster_rowdrop.rs`). The real internode
  transport is still not exercised by the clustered harness.
- **Richer `UPDATE`/`DELETE` `WHERE`** (range/non-key predicates), which today are
  restricted to single-row, full-PK equality.
- **`CREATE TABLE` without a `PRIMARY KEY`** — today refused (`MissingPrimaryKey` ->
  `0A000`), which is what stops `pgbench -i`
  (`pgbench_accounts (aid, bid, abalance, filler char(84))` is declared with no key).
  **Decided:** the table gets a synthetic **`_sys_ck_`** column — a v1 TimeUUID
  (`ferrosa_common::timeuuid::v1_timeuuid`) — as its partition key, so every row is
  unique by construction. Keying on the user's *first* column was implemented and then
  reverted: it assumes that column is unique and silently loses writes where it is not.
  The column is invisible to `SELECT *` and discoverable exactly the way Postgres's own
  system columns are: `pg_catalog.pg_attribute` lists it with a **negative `attnum`**
  (Postgres's marking for a system column) and it can be selected by name, as
  `SELECT ctid FROM t` works in Postgres. `_sys_` is reserved in any casing
  (`ferrosa_common::timeuuid::is_reserved_column_name`), so a future system column
  cannot collide with a user's table.
  **Done:** the reserved-name rule and the v1 TimeUUID mint (`ferrosa-common`); synthesis in
  `plan_create_table`; per-row minting on `INSERT` (`synthetic_key.rs`), proven end to end;
  `SELECT *` hiding the column (`result_stream.rs`, filtered only for a star projection, so
  naming the column still returns it); `pg_attribute` giving it a **negative `attnum`**
  (`catalog.rs`); and `ALTER TABLE` (`ddl.rs::execute_alter_table`) for `ADD PRIMARY KEY` —
  which records the declared key and builds a secondary index when that key is not the storage
  key — plus `ADD COLUMN` and `DROP COLUMN`. The remaining ALTER forms (RENAME, ALTER COLUMN
  TYPE/DEFAULT, non-key constraints) are refused by name, because `TableUpdates` cannot express
  them and approximating one would leave a client believing a change took effect.
  **COPY FROM STDIN — complete end to end.** The wire frames, the statement, the payload decoder
  and the connection state machine are all in and tested (`FrontendMessage::{CopyData, CopyDone,
  CopyFail}`, `BackendMessage::CopyInResponse`, `CopyFromStdin`, `copy_decode`, `copy_stdin`).

  The state machine (`copy_stdin::drive`) exists because COPY is the one statement that cannot be
  answered in a single step: the client sends the payload only AFTER `CopyInResponse`. It is driven
  from `query_loop`, where the frame buffer and the stream are in scope; a leading-`COPY` byte
  compare guards the intercept so ordinary statements are not parsed twice.

  Five properties are the whole point, and each has a test:

  - **Nothing is acknowledged that cannot run.** The table, the column list and the payload options
    are resolved *before* `CopyInResponse`, so a COPY that is going to fail never has the client
    stream a payload at it (`42P01` / `42P16` / `42703`; `25P02` in an aborted transaction block).
  - **A failure keeps draining.** Once the payload starts the client is sending regardless, so a
    failure is remembered, the remaining frames are consumed and discarded, and the error is sent
    when the client finishes. Returning early would leave those bytes for the statement parser to
    read as SQL.
  - **No second write path.** Rows go through `query::execute_insert`, so COPY gets the same type
    coercion and the same synthetic `_sys_ck_` minting as an INSERT.
  - **Transactional, like any DML.** In autocommit the staged write set is flushed with the very
    parameters the autocommit INSERT path uses (bounded by `FLUSH_EVERY`), and a failed COPY drops
    the unflushed tail rather than committing part of it. Inside `BEGIN` the rows buffer into the
    transaction's write-set through the SAME `apply_or_buffer` seam INSERT and `TRUNCATE` use:
    they are visible at `COMMIT` and discarded by `ROLLBACK`, with **no `25001` refusal**. The
    write-set's own staging buffer bounds residency — a write-set past
    `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES` SPILLS, it is never refused. A COPY that fails
    mid-payload **aborts** the transaction, so a later `COMMIT` rolls back rather than committing a
    partial load: the partial-commit trap the old `25001` refusal stood in front of. This is what
    lets `pgbench -i`, which wraps its `COPY`s in one `BEGIN`/`COMMIT`, load.
  - **The legacy `\.` end-of-data marker ends the payload.** `pgbench -i` streams its rows, then a
    `\.` line, and only then a `CopyDone`. `copy_decode` treats a lone `\.` line as end-of-data —
    never a row — so the marker is not refused as a malformed escape (`22P04`) and the load the
    client's `PQendcopy` waits on completes. Bytes the client flushes after the marker are ignored.
    Tested end to end through `query_loop` (`pgbench_legacy_copy_end_marker_lands_rows`), together
    with the fast-path gate agreeing with the parser on a trailing-`;` statement
    (`a_copy_statement_with_a_trailing_semicolon_still_enters_copy_mode`).
  - **Exactly one `CopyInResponse`.** The ack is the single cue to start streaming, so it is sent
    once and only once: the reply buffer is written for the ack, then **emptied** before the tail
    (`COPY n` / the error, plus `ReadyForQuery`) is written. Reusing it unemptied re-emitted the `G`
    *after* the payload, which re-cued the client into copy mode — psql answered
    `CopyFail "trying to exit copy mode"` and `pgbench -i` died with a bare `PQendcopy failed`. A
    two-direction wire capture on a live cluster confirmed the double dispatch was the server's,
    for a single client `Q`. Tested by `copy_from_stdin_is_acknowledged_exactly_once`,
    `a_failed_copy_is_acknowledged_exactly_once`, and the single-ack assertions through
    `query_loop` (`pgbench_legacy_copy_end_marker_lands_rows`,
    `copy_inside_a_transaction_over_the_wire_enters_copy_mode_and_lands_rows`).

  A row whose field count does not match the column list is refused (`22P04`) rather than padded.
  The option list is parsed by `ferrosa-sql`: `FREEZE [ON|OFF]` — a heap-page concept an LSM has no
  frozen rows for — is accepted-and-recorded there (never applied), which is what lets `pgbench
  -i` (PostgreSQL v14+ writes `with (freeze on)`) load; every other option, a bad option value and
  `COPY ... TO` are refused **by name** as COPY refusals (`ParseError::UnsupportedCopy`, `0A000`) —
  never reported as an `ALTER TABLE` form.
  Minting is done on this path too: every payload row goes through `execute_insert`, so a COPY
  mints `_sys_ck_` for a PK-less table exactly as an INSERT does. The projection that *reads*
  `pg_key` also exists now (`pg_index`/`pg_constraint` plus the `<table>_pkey` index row in
  `pg_class`), so the key a table declared — or `ALTER TABLE ADD PRIMARY KEY` recorded — is
  visible to psql's describe-table, which joins `pg_index.indisprimary` to `pg_class` and reads
  `pg_constraint`.
  See `ferrosa-common`'s README section "Reserved `_sys_` columns"
- **Binary `numeric`** result/param encoding (FMEA PG-7), removing the
  text-bytes fallback.
- **Real query cancellation** (FMEA PG-8) — mint a real `BackendKeyData`
  cancel key. (TLS on the wire, statement authorization and the shared
  failed-login limiter landed in t_e1c819ad.)
- **Mutual TLS** — client-certificate authentication for the PG listener
  (tracked with the internode mTLS work, t_b6c820f4).
- **Harden the SCRAM unknown-role oracle** — run the exchange against a dummy
  verifier so `UnknownRole` is not a user-enumeration signal (threat-model note
  in `handshake.rs`).

- **DDL follow-ups to T-132a** — `DROP`/`ALTER TABLE` (T-132b); DDL through the
  extended protocol (`Parse` refuses it); `NoticeResponse` so `IF NOT EXISTS`
  can emit the PG NOTICE; `timestamptz` (no `pg_types` entry, refused `42704`);
  enforcing `varchar(n)` / `numeric(p,s)`; cluster-mode jsonb DDL gate
  (T-300).

## Later

- **CQL `Duration` + collections** (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`)
  support (FMEA PG-4) — widen `ferrosa_sql::Value` and the
  `cql_to_value`/`value_to_cql` bridges. Until then, scans fail explicitly when
  they encounter one of these values.
- **Exact float/numeric text-format parity** with Postgres (FMEA PG-9).
- **Real affected-row counts** for `UPDATE`/`DELETE` (FMEA PG-10) — read-before-
  write so the count reflects matches rather than always reporting `1`.
- **Session GUCs** (`SET`/`RESET`) and a broader scalar-function surface
  (`now()`, etc.).
- **More `pg_catalog`/`information_schema` coverage** as drivers/ORMs demand it.

## Non-goals

- Query planning / binding / relational operators — those live in `ferrosa-sql`.
- The storage row encoding — that is `ferrosa-row-bridge` (shared with CQL, D10).
- Cassandra wire compatibility — that is the CQL front-end (`ferrosa-cql`).

## jsonb (T-150)

Done: type threading (T-150); engine `Value::Jsonb` and the storage mapping (T-160); OID 3802 text/binary codec, jsonb input parsing under the configured limits, `jsonb`/`json` DDL names, catalog row (T-161a). Remaining: json 114 and jsonpath 4072 result codecs, casts and operators (T-161b, T-162); the per-edge `jsonb_duplicate_keys_dropped_total` counter (only the edge log line exists here); the strict duplicate-key mode is honored from `[jsonb]` but has no per-session or table switch; a typed `CorruptJsonb` from the row bridge (T-151) so a corrupt stored cell reports `XX001` instead of the scan-failure `58000`.

T-301 (PG-first slice acceptance) is done: `tests/jsonb_slice.rs` and the postgres:16 differential over jsonb DDL, INSERT and SELECT, with no unexplained differences. Deferred: the cluster-mode refusal test (waits on the T-300 gate), operators and SRFs against the oracle (T-162).
