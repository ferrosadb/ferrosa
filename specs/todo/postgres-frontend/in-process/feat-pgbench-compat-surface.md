---
type: work-item
status: in-process
priority: P0
created: 2026-10-09
updated: 2026-10-09
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
| 4 | `COPY t FROM STDIN` | **OPEN** — needs the CopyIn/CopyData/CopyDone wire handshake |
| 5 | `TRUNCATE t` | **OPEN** — `StorageEngine::truncate()` already exists |
| 6 | `VACUUM` / `VACUUM ANALYZE` | **OPEN** — accept-and-report (ferrosa has no vacuum) |
| 7 | `\|\|` string concat | **OPEN** — the probe's own table census uses it |
| 8 | **multi-row `INSERT ... VALUES (...), (...)`** | **OPEN — parser only.** See below |

Items 2 and 3 are the real ones: pgbench creates PK-less tables and adds the primary key
afterwards, so this is a genuine semantic difference from PostgreSQL, not parser surface.
Item 4 is the largest single piece (a wire-protocol handshake).

## Item 8 — multi-row INSERT (explicitly in scope)

**Not implemented. Do not read "parsed" as "working".**

- **Parser: DONE.** `InsertStmt.values` became `rows: Vec<Vec<ScalarValue>>`, with a
  per-row arity check, and `parse_scalar_value` accepts `now()` and bare
  `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` as `ScalarValue::Func`.
  `insert_placeholders` and `insert_param_targets` flatten every row for the extended
  protocol.
- **Execution: NOT DONE.** `execute_insert` is single-row only and **fails loud** with
  SQLSTATE `0A000` for `rows.len() != 1`, rather than writing row 1 and silently dropping
  rows 2..N. Silent row loss is the failure mode this repo treats as unacceptable, so
  fail-loud is the deliberate holding position, not an oversight.

### CRITICAL: the SQL layer is correct; the loss is BELOW it

An execution implementation exists and is **correct in-process**, yet on the live
cluster the same statement reports `INSERT 0 3` and persists only row 1 (reproduced
twice on a fresh table). The `0A000` guard is therefore back in place: a silent
row-drop announced as a successful count is strictly worse than an error.

**Two hypotheses were eliminated, both by negative control rather than by argument:**

1. **Per-row state reused across rows.** `col_values` / `sql_values` / `regular_cells`
   were declared outside the per-row loop, so `regular_cells` accumulated every earlier
   row's cells. Hoisting them back out and re-running the value-asserting test still
   PASSES — the duplicate cells resolve last-wins, so the bug is real but benign here.
   It cannot cause the row-drop. (The per-row declarations are kept: they are correct
   and cheap.)
2. **The buffered write-set.** The server reaches INSERT via `Some(txn_writes_mut())`,
   the one path an autocommit test never touches — so this was the prime suspect.
   Test `a_buffered_multi_row_insert_applies_every_row` drives a 3-mutation write-set
   through `write_atomic_batch` exactly as COMMIT does, and all three rows land.

Four in-process multi-row tests are green, spanning the SQL, MVCC and server layers:

```
mvcc::tests::staged_multi_row_accord_commit_is_visible_as_one_snapshot_version ... ok
server::txn_atomicity_tests::multi_row_snapshots_hide_partial_replica_apply      ... ok
query::txn_buffer_tests::multi_row_insert_writes_every_row_and_reports_the_count ... ok
query::txn_buffer_tests::a_buffered_multi_row_insert_applies_every_row           ... ok
```

**Conclusion: the defect is in the live multi-node apply path, below ferrosa-postgres.**
There is deliberately NO in-process test for it, because none can bite. Six
combinations were run and all six are green with the bug present:

| path | with bug | correct |
|---|---|---|
| autocommit, values asserted | pass | pass |
| buffered, mutations built directly | pass | pass |
| buffered, driven through execute_insert | pass | pass |

A single-process engine simply cannot exhibit the loss, so any test written here would
pass and prove nothing. The test that matters must drive a real wire connection into a
real multi-node cluster and assert the read-back — which is what the probe does. Do not
add an in-process "regression test" for this; it would be false assurance.
A single-node in-process engine cannot exhibit it. Next step: reproduce with a real
clustered process test (3 nodes, real transport) — a mock or single-node harness will
keep passing — and instrument what the coordinator actually persists for a
multi-mutation batch before suspecting the SQL layer again.

**Re-enabling execution** requires that clustered repro first, then: drop the guard,
assert one `CommandComplete` tagged `INSERT 0 3`, three rows landed, each carrying its
own value.

### CLUSTERED REPRO ATTEMPT — NOT REPRODUCED (2026-10-09)

A real multi-node clustered test now exists and **does not reproduce the loss**:
`ferrosa-postgres/tests/pg_multirow_cluster_rowdrop.rs`. It stands up 3 independent
Accord nodes — each with a real `AccordStateMachine`, a real `StorageEngine`, a real
`AccordHandler`, and its own real PG wire listener — wired to the real
`AccordTransactionCommitter` (`commit_postgres`). Only the peer *socket* is elided (an
in-process transport routes every message to the addressee's real handler); the
protocol, state machines, storage, SQL layer and PG front end are the production ones.

What was tried, and what was observed:

| # | Experiment | Result |
|---|---|---|
| 1 | **The exact live statement, guard lifted**: `INSERT INTO users (id,name) VALUES (201,'m-one'),(202,'m-two')` over a native `tokio-postgres` client to node 0, RF=3 (every key on all three nodes), implicit transaction → `commit_postgres` → Accord. | client received **`INSERT 0 2`**, and **both rows were readable from ALL THREE nodes**. 3/3 runs. No drop. |
| 2 | Same statement, RF=1 **one shard per key** (each row owned by a *different* node — the multi-shard shape of a real ring). | both rows present on their owners. No drop. |
| 3 | Direct committer-level multi-key commit (two raw key mutations) on the same cluster, RF=3 and RF=1. | every key lands on every owning replica once the post-quorum apply converges (bounded 5 s window). No drop. |

Accord's apply is asynchronous: the coordinator returns at apply **quorum**, and a
non-quorum replica applies in the background — experiment 3 needed that bounded window
to see the third replica, which is normal Accord, not divergence.

**Conclusion.** The multi-node apply path *below* `ferrosa-postgres` persists every key
of an N-key write-set on a real 3-node cluster, in both the single-shard (RF=3) and the
multi-shard (RF=1-per-key) topology. The live row-drop is **not** reproduced, so the
`0A000` guard stays: a silently dropped row announced as `INSERT 0 N` is still strictly
worse than an error. No speculative fix is applied — the SQL/MVCC/buffered-write-set
hypotheses stay disproved and the clustered apply path is now shown correct too.

**Next concrete hypothesis — the real internode transport.** The one component the
harness elides is the real wire: `PeerManager`/TCP lanes with connection pools,
per-lane timeouts, retries, and the `AccordAccess::live` committer factory (gated on
`WritePath::Cluster`). Everything above the socket is already exercised. The next step
is to stand up real `RpcServer`s on loopback with the Accord `MsgType`s registered and a
real `PeerManager` transport — the exact shape `tests/range_scan_multi_replica_paging.rs`
already uses for range reads — and drive the same statement. The specific thing to audit
there is the one place a transport anomaly can satisfy a shard quorum **without** the
write being applied: `is_apply_ok` in `coordinator.rs` accepts an **empty-body**
`AccordApplyOK` (`b.is_empty()`) as an ack for *any* transaction, so a stale/duplicate/
misrouted empty ack can count a shard as applied when it was not. If the loss appears
only behind the real transport, look there first.

Re-run the whole-statement experiment with the guard temporarily lifted (`if false &&`
on the `rows.len() != 1` check in `execute_insert`) and:

```
cargo test -p ferrosa-postgres --test pg_multirow_cluster_rowdrop -- --nocapture
```


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

Still to build: the column synthesis, server-side per-row assignment on INSERT and COPY,
the wire exclusions, and `ALTER TABLE ... ADD PRIMARY KEY` (not parsed at all today — a
new statement type; where it names a column other than the synthetic key it becomes a
secondary index, where it names nothing new it is a no-op).

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

### What execution needs

`execute_insert` (`ferrosa-postgres/src/query.rs`) resolves, per row: the column values,
the partition/clustering key, the regular cells, and the RETURNING row. To support N rows
that whole block must move inside a loop over `ins.rows`, with:

- the write per row via `apply_or_buffer(..., txn.as_deref_mut(), ...)` — the borrowed
  `Option<&mut Vec<PgWrite>>` must be re-borrowed per iteration, which is what makes this
  a refactor rather than a loop-and-done;
- **one** combined result: a single `RowDescription`, one `DataRow` per inserted row, and
  a single `CommandComplete "INSERT 0 N"` — not N concatenated result sets;
- a failure on row *k* must leave no partially-announced result: rows already buffered into
  an open transaction stay buffered (they commit or roll back with it), and the error is
  reported untouched.

TDD it: a RED test asserting 3 rows land (all three readable back) and that
`CommandComplete` reports `INSERT 0 3`; plus a negative control asserting the current
`0A000` fail-loud so the guard cannot silently regress to dropping rows.

## Also staged on #544 (not pgwire surface, but required to get through the gate)

- zero-byte SSTable components dropped on the receiver (Rows.db corruption);
- half-open lane never reconnects → probe-first detection bounded by a failure window;
- the coordinator's local Apply bypassed the abandon (opaque 58000 → retryable 40001);
- a dependency this replica has no live state for must not park its waiter — the cure for
  the ~30 % abandonment.

## Operator tunables

`FERROSA_ACCORD_TXN_TIMEOUT_SECS` (10) · `FERROSA_ACCORD_BARRIER_TIMEOUT_SECS` (5) ·
`FERROSA_NET_LANE_FAILURE_THRESHOLD` (3) · `FERROSA_NET_LANE_FAILURE_WINDOW_MS` (30000) ·
`FERROSA_NET_LANE_PROBE_TIMEOUT_MS` (2000)

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
