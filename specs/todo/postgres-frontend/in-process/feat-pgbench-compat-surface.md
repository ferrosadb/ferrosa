---
type: work-item
status: in-process
priority: P0
created: 2026-10-09
updated: 2026-10-10
owner: agent
pr: ferrosadb/ferrosa#544
---

# pgbench / PG-wire compatibility surface

> PR: **ferrosadb/ferrosa#544** — "pgbench smoke-check: all changes required to pass
> the PostgreSQL/pgbench gate". Everything required to get through the pgbench smoke
> check is staged there.

## Goal

A clean pass of `pg-runner.sh probe <target>` against ferrosa, then `pgbench -i` followed
by a `pgbench` run. Only then is the 1-node / 3-node / native comparison meaningful —
before this, ~30 % of explicit transactions were being abandoned, and pgbench is almost
entirely explicit transactions.

**The probe is the acceptance test.** Re-run it after each increment and watch the
failure list shrink:

```
fly ssh console -a maas-loadtest-pgrunner -C "/usr/local/bin/pg-runner.sh probe f1"
```

## Ground truth (live-captured 2026-10-09)

| # | Statement | Status |
|---|---|---|
| 1 | `DROP TABLE IF EXISTS a, b, c, d` | **DONE** — full stack + 5 parser tests |
| 2 | `CREATE TABLE t (...)` with **no PK** | **OPEN** — ferrosa requires a PK at CREATE. A *semantic* gap, not syntax |
| 3 | `ALTER TABLE t ADD PRIMARY KEY (col)` | **OPEN** — `TableUpdates` has no PK-change support |
| 4 | `COPY t FROM STDIN` | **DONE** — CopyIn/CopyData/CopyDone wire handshake, transactional |
| 5 | `TRUNCATE t` | **DONE** — a replicated table-tombstone write |
| 6 | `VACUUM` / `VACUUM ANALYZE` | **DONE** — flush + submit compaction |
| 7 | `\|\|` string concat | **DONE** |
| 8 | **multi-row `INSERT ... VALUES (...), (...)`** | **DONE** — parses AND executes every row atomically. See below |

Items 2 and 3 are the real ones: pgbench creates PK-less tables and adds the primary key
afterwards, so this is a genuine semantic difference from PostgreSQL, not parser surface.

### Live evidence (2026-10-11): the UPDATE gate is what the bench actually hits

With the lane fix deployed (`pgwire-a4ef88be`), `pgbench -i --scale 10 --foreign-keys` now
completes (`rc=0`, 46 s, PKs and FKs created) and the census reads back exactly
`1000000|100|10`. The bench still cannot run a single transaction:

```
$ pg-runner.sh raw f3 -n -c 1 -t 1
pgbench: error: client 0 script 0 aborted in command 5 query 0:
  ERROR:  UPDATE WHERE supports only key columns; "aid" is not a key
number of transactions actually processed: 0/1
```

Command 5 is `UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid`.
The rows ARE there and `aid` IS the declared primary key — but the gate at
`ferrosa-postgres/src/query.rs:3225` (and the twin for DELETE at `:3652`) admits only a
column that is a **storage key**. On a table whose PK arrived by `ALTER … ADD PRIMARY KEY`,
that is the synthetic `_sys_ck_`, never `aid`. So items 2/3 above are not just a CREATE-time
semantic gap: they are what makes the UPDATE gate unsatisfiable for the exact statement
tcpb issues, and therefore what blocks every tps comparison.

Consequence for the acceptance test: the probe's step 2 (`scan of public.fprobe`) also still
fails, and no tps number exists yet. **The gate is not to be widened** — accepting a
non-key WHERE without a path that can actually find those rows would trade a loud `0A000`
for a silent full-table rewrite. The correct fix is the item's decided design: a secondary
index (or an equivalent find-rows-by-declared-PK path) so `WHERE aid = ?` is answerable.

This is tracked as Forge **t_c83ed1f2** (priority 60). Its FIRST STEP is an `#[ignore]`d red at
the real entry point — drive the UPDATE through the PG statement path over a table whose PK
arrived by ALTER, asserting the row's VALUE changes — so the gate stays green and the finding
survives as the specification of "fixed". **That red is not written yet**, and until it is, this
is "found and tracked", not "held down by a test".

The comparison this blocks is tracked as Forge **t_4b5e0fce** (child of t_c83ed1f2). The
scale-100 superlinearity is tracked separately as **t_6fa292c5**, and the inert window knob as
**t_2a8ab545**.

## Item 8 — multi-row INSERT (DONE)

**Implemented: multi-row INSERT parses AND executes every row, atomically.**

- **Parser: DONE.** `InsertStmt.values` became `rows: Vec<Vec<ScalarValue>>`, with a
  per-row arity check, and `parse_scalar_value` accepts `now()` and bare
  `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` as `ScalarValue::Func`.
  `insert_placeholders` and `insert_param_targets` flatten every row for the extended
  protocol.
- **Execution: DONE.** `execute_insert` builds and validates EVERY row of the statement —
  value resolution, foreign-key checks, key ordering, RETURNING projection — and only then
  writes them as ONE atomic batch (`apply_batch_or_buffer`): one MVCC commit, one
  `write_atomic_batch`, or one buffered write-set. The former `0A000` fail-loud refusal is
  gone.

### Root cause and fix: one statement, one atomic apply

The former `0A000` refusal was a fail-loud holding position, and the reason recorded for it
was a live report that the same statement returned `INSERT 0 N` while persisting only row
1. That exact live symptom was never reproduced in-process, and the clustered harness
(`ferrosa-postgres/tests/pg_multirow_cluster_rowdrop.rs`) did not reproduce it either.

What the old loop DID get wrong is **statement atomicity**, and that is what the fix
addresses. It applied each row to storage as it was built: row 1 was committed (or
buffered) before row 2 was even resolved. A failure on a later row therefore left the
earlier rows behind while the statement errored — a partial multi-row INSERT, and on a
cluster a single statement scattered across N transactions. Both are exactly the class of
"announce a count you did not fully write" outcome the refusal existed to prevent.

`execute_insert` now builds and validates EVERY row first, then applies the whole set once
(`apply_batch_or_buffer`). The properties now pinned by test:

| property | test |
|---|---|
| every row written once, own values (2-row minimum, incl. the LAST row) | `a_two_row_insert_writes_both_rows_with_their_own_values` |
| larger N, no value bleed between rows | `a_larger_multi_row_insert_gives_every_row_its_own_values` |
| duplicate keys in one statement → last-write-wins, one row | `duplicate_keys_in_one_multi_row_insert_are_last_write_wins` |
| NULL round-trips as NULL, not `''`, not absent | `a_multi_row_insert_stores_null_distinctly_from_a_value` |
| a failure on a later row writes NOTHING (atomicity) | `a_multi_row_insert_that_fails_on_a_later_row_writes_nothing` |
| explicit txn: COMMIT persists every row; ROLLBACK discards all | `a_multi_row_insert_in_a_committed_transaction_writes_every_row`, `a_multi_row_insert_in_a_rolled_back_transaction_writes_nothing` |
| real 3-node cluster: both rows on every replica, by value | `pg_multirow_cluster_rowdrop::multi_row_insert_persists_every_row_on_a_real_cluster` |

Each asserts the VALUES of the rows, never the count alone: the count is the property the
defect PRESERVES, so a `count(*) == N` check would pass a write that landed one row.

The two hypotheses the earlier investigation eliminated by negative control stand: the
per-row value construction was already correct (it kept its per-row declarations, and the
value tests pass with them), and the buffered write-set machinery applies a multi-mutation
set correctly. The defect was neither — it was the *granularity* of the apply.

**What remains below the SQL layer is unproven, not disproven.** The clustered harness
elides the real internode transport (`PeerManager`/TCP lanes, `AccordAccess::live`). If a
loss ever appears only behind that real transport, the place to audit is still `is_apply_ok`
in `coordinator.rs`, which accepts an empty-body `AccordApplyOK` (`b.is_empty()`) as an ack
for any transaction. No speculative change is made there.

### What execution does now

`execute_insert` (`ferrosa-postgres/src/query.rs`) resolves, per row, the column values,
the partition/clustering key, the regular cells, and the RETURNING row, collecting one
`Mutation` per row; any row's failure returns before anything is applied. The whole set is
then written once via `apply_batch_or_buffer(..., txn, mutations, &tag)`. The result is ONE
`RowDescription`, one `DataRow` per inserted row, and a single `CommandComplete
`"INSERT 0 N"` — never N concatenated result sets. A buffered write-set still commits or
rolls back as a unit.

### PK-less CREATE TABLE: use a synthetic incrementing-id column at position 0

`CREATE TABLE` with no PRIMARY KEY is refused (`MissingPrimaryKey` -> 0A000), and that is
what stops `pgbench -i` — it dies at
`pgbench_accounts (aid, bid, abalance, filler char(84))`.

**A shadow key on the user's first column is WRONG** and was implemented, then reverted
(`e347f3eb` -> `b91a232a`). It assumes the first column is unique; where it is not, rows
collide and the write is lost. Do not reintroduce it.

Instead the table gets a **synthetic column at position 0 carrying a globally unique
incrementing id**, and the partition key is that column. Every row is then unique by
construction, with no assumption about the user's data.

Two constraints drive the design:

1. **The column must be invisible to PostgreSQL clients.** pgbench's
   `COPY pgbench_accounts FROM STDIN` sends exactly the four declared columns; a visible
   fifth column makes COPY and `INSERT ... VALUES` (no column list) fail. It must also be
   excluded from `SELECT *`.
2. **The id must be globally unique, not a per-node counter.** Three nodes each counting
   from zero would collide on the shared key exactly as a non-unique first column would —
   the same bug, relabelled. Use a time-ordered v1 timeuuid (ordered *and* unique) or, if
   v1 is unavailable, `uuid::Uuid::new_v4()` (already a dependency; unique, unordered),
   or the Accord HLC in `ferrosa-common/src/accord.rs` (carries a node id).

**Cheapest correct route — avoid a 151-site refactor.** Adding `hidden: bool` to
`ColumnMetadata` (`ferrosa-schema/src/metadata/column.rs`) touches **151** construction
sites across the workspace and the type derives no `Default`. Prefer instead:
keep the synthetic column in `columns` (so storage indexes and row encoding work
unchanged) and carry its hidden-ness **in the Postgres front end by a reserved name**
(e.g. `ferrosa_row_id`), rejecting that name in user DDL. Then `SELECT *`, COPY and
INSERT arity all filter it in one place, and no schema-metadata change is needed.
Only if CQL clients must also not see it does the metadata field become necessary.

**Status 2026-10-11 (later, corrected): the gap is bigger than the predicate — it is the whole
read path that is missing.** The column synthesis exists (`ferrosa-common/src/timeuuid.rs`:
`_sys_ck_`, a v1 TimeUUID, with `is_reserved_column_name`), `AddPrimaryKey(Vec<String>)` exists in
the AST (`ferrosa-sql/src/ast.rs:207`), and `ALTER … ADD PRIMARY KEY` is parsed, recorded AND
indexed: `execute_add_primary_key` (`ferrosa-postgres/src/ddl.rs:302`) stores the declared key as
`pg_key::PRIMARY_KEY_EXTENSION`/`encode(columns)` and builds the **`{table}_pkey`** secondary index
over it when that key is not the storage key (ddl.rs:343-346). The live init proves the machinery
works — `pgbench -i`'s "creating primary keys…" phase succeeds on the bench cluster.

What is missing is any **reader** of that index. `ferrosa-postgres/src/query.rs` contains ZERO
references to `_pkey` and ZERO to `pg_key::of`, so the declared key cannot be resolved by index
anywhere at runtime. Consequences, all from the same root: the UPDATE/DELETE gate
(`query.rs:3222-3230`, DELETE twin `:3652`) refuses the WHERE outright (`0A000 UPDATE WHERE supports
only key columns; "aid" is not a key` — reproduced live on the bench cluster and now in-process by
`update_and_select_by_a_primary_key_added_by_alter_match_the_row_they_name` in
`ferrosa-postgres/src/server.rs`), and tpcb command 1's `SELECT … WHERE aid = ?` has no index to use
either.

An earlier draft of this paragraph claimed the gap was "ONE consumer" — the gate consulting
`pg_key::of`. That was wrong in scope: consulting the declared key without an index read would trade
a loud `0A000` for a silent full-table rewrite, which is exactly the outcome the item's own decision
section forbids. **The shape to copy already exists in the repo**, at `ddl.rs:449-452` (the FK child
check): *"a point read when it is the storage key, otherwise the `<parent>_pkey` secondary index. A
non-key column would need a per-check scan, which is not a lookup and is refused by name rather than
recorded."* The fix is to resolve the declared key through `{table}_pkey` into the storage key, then
let the existing DML path run unchanged — never to widen the gate. Scoped as t_c83ed1f2 (the read
half also touches t_6fa292c5).

Fixture note for the green: the recorded extension is the **bare column name** (query.rs:5813-5816
asserts `Some(&"v".to_string())`), not an encoded form, so a test fixture must set the extension to
`"aid"` rather than `pg_key::encode([...])`.

### Seeing the system columns: PG's own model beats a bespoke options table

Decision: the synthetic key is `_sys_ck_`, a v1 TimeUUID, and its `node` field is a
**random 48-bit value chosen once per process** (RFC 4122 permits exactly this — it is
what v1 does with MAC addresses). No cluster plumbing, ~2^-48 collision odds. Plumbing the
real node id from `main.rs:3252` or the engine's `node_id` stays available as an upgrade;
`v1_timeuuid(time, clock_seq, node)` takes `node` as a parameter precisely so that swap
touches one call site.

The open question was how a user finds a column that `SELECT *` hides. Two facts settle
most of it, both already true in this tree:

1. **`pg_attribute` already lists every column, including a hidden one.**
   `catalog.rs::pg_attribute` projects one row per column per table with `attname`,
   `atttypid` and `attnum`. So `SELECT * FROM pg_catalog.pg_attribute` is *already* the
   discovery path — no new machinery needed for "users can see the system columns".
2. **Postgres has a native name for exactly this.** Its own system columns (`ctid`, `xmin`,
   `xmax`, `cmin`, `cmax`) are hidden from `SELECT *`, are listed in `pg_attribute` with a
   **negative `attnum`**, and *are* selectable when named explicitly
   (`SELECT ctid FROM t` works). ferrosa currently gives every column a 1-based positive
   `attnum` (`catalog.rs::attribute_row`).

**Recommendation: model `_sys_ck_` as a system column the way Postgres models `ctid`.**
Hidden from `SELECT *`; listed in `pg_attribute` with a negative `attnum`; selectable by
name. That gives discoverability and explicit access with *no new concept* — every
Postgres user already knows how `ctid` behaves, which is the least-surprise outcome by
construction.

**DECIDED (owner): the `pg_attribute` route. No options table.** The system column is
invisible to `SELECT *`, listed in `pg_attribute` with a negative `attnum`, and selectable
by name. `SELECT *` stays clean. Documentation is part of the definition of done: the
behaviour is written up in `ferrosa-common/README.md` ("Reserved `_sys_` columns") and
`ferrosa-common/specs/overview.md`, and tracked in `ferrosa-postgres/specs/roadmap.md`
"Next" — which is also where `pg_attribute`'s negative-`attnum` convention for system
columns is recorded for the front-end. Keep those in sync when the wiring lands.

**Rejected: the options table.** A
`_sys_*` relation the user `UPDATE`s to toggle visibility would let `SELECT *` include the
system columns. Costs: a writable system relation (the existing virtual tables —
`ferrosa-postgres/src/catalog.rs` projections, `ferrosa-cql/src/virtual_tables/` including
`rrd_runtime_settings.rs` — are all **read-only projections**), plus somewhere to persist
the setting, plus a session/global scope rule. Worth it only if the toggle is genuinely
wanted; the `pg_attribute` + explicit-select path covers "let users see them" for free.

Not yet decided, and needed before building: whether `SELECT *` should ever include
`_sys_ck_`, and if the options table is wanted at all — the two are the same question.

## Also staged on #544 (not pgwire surface, but required to get through the gate)

- zero-byte SSTable components dropped on the receiver (Rows.db corruption);
- half-open lane never reconnects → probe-first detection. **The failure WINDOW was
  REMOVED 2026-10-11** (`45374bb9`, `a4ef88be`): an elapsed-time reset cleared the failure
  run between uses, so a half-open lane whose traffic is sparse (a Raft heartbeat every few
  minutes) never reached the probe — the live 20-minute `timeout: Raft lane timeout` loop.
  A run is now cleared ONLY by a successful round trip. See
  `ferrosa-net/specs/in-process/bug-lane-half-open-never-reconnects.md`;
- the coordinator's local Apply bypassed the abandon (opaque 58000 → retryable 40001);
- a dependency this replica has no live state for must not park its waiter — the cure for
  the ~30 % abandonment.

## Operator tunables

`FERROSA_ACCORD_TXN_TIMEOUT_SECS` (10) · `FERROSA_ACCORD_BARRIER_TIMEOUT_SECS` (5) ·
`FERROSA_NET_LANE_FAILURE_THRESHOLD` (3) · `FERROSA_NET_LANE_PROBE_TIMEOUT_MS` (2000)

`FERROSA_NET_LANE_FAILURE_WINDOW_MS` is **no longer consulted** (removed 2026-10-11 with the
window itself). It is still wired but inert; removing the constant, its env var, and the
ignored second parameter of `LaneHealth::new` is a tracked follow-up.

## TRUNCATE semantics to document (operator directive)

Record in the crate docs when the table-tombstone change lands — this is the wording the
operator asked for, in substance rather than verbatim:

- On ferrosa a `TRUNCATE` is **logically immediate**: reads return no rows as soon as it
  commits, because the tombstone hides everything older than it.
- **Physical reclamation is lazy** — the bytes go away at the next compaction, not at commit.
- `TRUNCATE` followed by `VACUUM` is therefore what forces reclamation promptly, and that pair
  is **strictly equivalent in effect** to an immediate truncate.
- For **client compatibility** the logical effect is deliberately kept synchronous and
  reclamation deliberately left lazy. Making reclamation synchronous would be a **purposeful,
  separate change**, not something to slip in.

The point of writing it down: a client that truncates and immediately measures disk will see
no change, and that must be an understood, documented property rather than a surprise.
