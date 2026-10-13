# PostgreSQL front end — transaction, buffer and bulk paths: code-inferred ground truth

Scope: `ferrosa-postgres` (PG wire front end over the MVCC layer) in
`/Users/bkearns/src/ferrosa-suite/.wt-int2`, branch `feat/read-in-transaction`, HEAD `32fc001a`.
Read-only recon: no source file was modified.
Every claim below carries a `file:line` citation. Where two sources disagree, the disagreement is
recorded under **Contradictions** rather than resolved silently. Anything the code does not settle is
under **Could not determine**.

Evidence classes used per claim:

- `[code]` — readable from the source as written.
- `[test]` — asserted by an existing test.
- `[repo-doc]` — a spec/FMEA/README statement in this repo (may be stale; treated as a claim, not proof).
- `[git]` — a commit message in this workspace.
- `[external]` — PostgreSQL 16 `pgbench.c` (`REL_16_STABLE`), fetched during this recon. Exact
  evidence needed to settle "is `pgbench -i`'s COPY in a transaction", which the repo does not contain.

---

## 0. Short answers

| # | Question | Answer | Anchor |
|---|----------|--------|--------|
| 1 | Does a plain `BEGIN` open a **tracked** transaction? | **Yes** — `TransactionStatus::InTransaction` + a snapshot | `extended.rs:324-336`, `server.rs:688-704` |
| 1 | What isolation does it record? | `txn_isolation = None` ("session default") — not "serializable", not "untracked" | `parser.rs:947-949`, `extended.rs:90-92` |
| 1 | Is the session write-set buffer engaged? | **Yes** — writes are buffered from the first DML | `server.rs:854-862`, `server.rs:1785-1790` |
| 1 | What does `None` disable vs explicit `SERIALIZABLE`? | (a) per-txn snapshot reads, (b) read-set recording | `server.rs:825-836`, `server.rs:716-719` |
| 2 | Are a plain BEGIN's statements buffered or applied immediately? | **Buffered** (DML, COPY, TRUNCATE); **refused** (DDL) | §2 |
| 3 | `COPY ... FROM STDIN` in an open transaction: buffer or write through? | **Buffers into the session write-set** — does **not** write through | `copy_stdin.rs:266-293` |
| 3 | Is `pgbench -i`'s accounts COPY inside that transaction? | **Yes** — one `begin` … `commit` encloses TRUNCATE + INSERTs + COPY | `[external] pgbench.c:4940, 4943, 4979, 5057, 5064` |
| 4 | What type holds an in-flight transaction's changes? | The **front end's** `TxnWriteSet` (spilling staging), **not** MVCC | `extended.rs:103`, `txn_write_set.rs:47-52` |
| 4 | What does MVCC hold? | **Committed version history** (`versions`, `distributed_versions`), not a per-txn buffer | `mvcc.rs:254-267` |
| 5 | Who records reads, and when? | `track_select_reads`, gated on `in_txn && isolation == SERIALIZABLE`; point reads by key, scans by table | `server.rs:715-740` |
| 5 | What is it used for at COMMIT? | `validate_commit` → serialization conflict (`40001`) | `mvcc.rs:750-798`, `server.rs:1027`, `server.rs:932` |
| 6 | Abandon / serialization / network → SQLSTATE? | `40001` / `40001` / `58000` | `server.rs:1183-1218` |

---

## 1. What a plain `BEGIN` does

### 1.1 It IS a tracked transaction

`execute_simple_inner` routes `Statement::Begin` to `begin_block` — `server.rs:656-658`:

```
656|        ferrosa_sql::Statement::Begin { isolation } => {
657|            Ok(begin_block(ctx, session, isolation).await)
```

`begin_block` takes a snapshot and opens the block — `server.rs:677-705`:

- isolation gate: a *non-serializable* explicit level is refused `0A000` — `server.rs:682-687`.
- snapshot for a plain `BEGIN`: if an Accord committer exists, the snapshot is taken **through the
  committer** and carries a cluster timestamp; otherwise a local snapshot — `server.rs:688-700`.
- `session.begin_txn(isolation, snapshot)` — `server.rs:701`.

`Session::begin_txn` sets `TransactionStatus::InTransaction` and stores the snapshot — `extended.rs:324-336`:

```
331|        if matches!(self.txn, TransactionStatus::Idle) {
332|            self.txn = TransactionStatus::InTransaction;
333|            self.txn_isolation = isolation;
334|            self.txn_snapshot = Some(snapshot);
335|        }
```

`in_txn()` is `matches!(self.txn, TransactionStatus::InTransaction)` — `extended.rs:311-313`. So after a
plain `BEGIN`, `in_txn() == true`. Asserted by `[test] server.rs:2874` (`assert!(session.in_txn())`).

Note the `Idle` gate at `extended.rs:331`: a repeated `BEGIN` while already in-transaction does **not**
replace the isolation or the snapshot (but does clear the write-set and read tables first, `extended.rs:329-330`).

### 1.2 What isolation it records: `None`, not "serializable" and not "untracked"

The parser yields `isolation: None` for a bare `BEGIN`/`BEGIN WORK`/`START TRANSACTION` — the isolation
clause is only produced when `ISOLATION LEVEL …` is literally present — `parser.rs:922-952`:

```
947|        } else {
948|            None
949|        };
```

`Session`'s field doc states the meaning explicitly — `extended.rs:90-92`:

```
90|    /// Explicit isolation mode for the current transaction. `None` means the
91|    /// session default.
92|    txn_isolation: Option<ferrosa_sql::IsolationLevel>,
```

So there are three distinct states, and they must not be conflated:

| State | `in_txn()` | `txn_isolation()` | write-set buffer | reads use BEGIN snapshot | read-set recorded |
|---|---|---|---|---|---|
| plain `BEGIN` | `true` (`extended.rs:331-335`) | `None` (`extended.rs:333`) | **engaged** (`server.rs:854-862`) | **no** (`server.rs:826-836`) | **no** (`server.rs:716-719`) |
| `BEGIN ISOLATION LEVEL SERIALIZABLE` | `true` | `Some(Serializable)` | engaged | **yes** (`server.rs:829-832`) | **yes** (`server.rs:716-719`) |
| untracked (autocommit, no BEGIN) | `false` (`extended.rs:311-313`) | `None` (`extended.rs:151`) | **absent** — applies immediately (`query.rs:1574-1604`) | n/a | n/a |

Consequences of `None` (this is the substance of the "isolation None" vs "untracked" distinction):

1. **Reads do not read the transaction's own BEGIN snapshot.** `read_snapshot` uses the transaction
   snapshot only under an explicit `Serializable`; otherwise it takes a *fresh* snapshot per statement —
   `server.rs:825-836`:

   ```
   826|    if session.in_txn()
   827|        && session.txn_isolation() == Some(ferrosa_sql::IsolationLevel::Serializable)
   828|    {
   ...
   834|        ctx.mvcc.snapshot()
   ```

   A plain `BEGIN`'s snapshot is therefore used at COMMIT (`server.rs:923-926`) but *not* by its own SELECTs.
2. **No read-set is recorded.** `track_select_reads` returns early unless the isolation is explicitly
   `Serializable` — `server.rs:715-719`:

   ```
   716|    if !(session.in_txn()
   717|        && session.txn_isolation() == Some(ferrosa_sql::IsolationLevel::Serializable))
   718|    {
   719|        return;
   720|    }
   ```

   (The task text cites this gate as `server.rs:710-714`; in this revision `710-714` is the doc comment and
   the executable gate is `716-720`. Line drift only — same code.)

But the **write-set buffer IS engaged** for a plain `BEGIN`, because buffering is keyed on `in_txn()`
alone and never on isolation — `server.rs:850-864`:

```
853|    let snapshot = read_snapshot(ctx, session);
854|    let in_txn = session.in_txn();
...
857|        sql,
858|        if in_txn {
859|            Some(session.txn_writes_mut())
860|        } else {
861|            None
862|        },
```

`Session.txn_writes_mut()` returns the staged write-set — `extended.rs:360-362`; the field is
`txn_writes: TxnWriteSet` — `extended.rs:103`.

### 1.3 The implicit (autocommit) counterpart, for contrast

When **not** in a transaction and a committer exists, a data statement is wrapped in an implicit
transaction that is committed immediately — `server.rs:563-607` (`begin_implicit_transaction` at
`server.rs:513-536`, commit at `server.rs:598`). So "untracked" does not mean "no Accord ordering" in
cluster mode; it means "no *client-visible* transaction whose fate outlives the statement".

---

## 2. Buffered until COMMIT, or applied immediately? — per statement class

The dispatch comment states the rule — `server.rs:668-672`:

```
668|        // Data + session statements: delegate to the stateless executor. (It
669|        // re-parses; cheap, and keeps the executor self-contained.) In an open
670|        // transaction, DML is BUFFERED into the session write-set instead of
671|        // applied; autocommit (no open txn) applies immediately.
672|        other => run_data_statement(ctx, session, sql, &other, out).await,
```

**The earlier belief that a plain BEGIN's statements apply immediately is false.** A buffered statement
still gets an immediate `CommandComplete` (`INSERT 0 1`) — `query.rs:1531-1536` — but nothing is written;
asserted by `[test] server.rs:2876-2890` (`row_count == 0` before COMMIT) and
`[test] copy_stdin.rs:917-923`.

| Class | Inside `BEGIN` | Outside (autocommit) | Anchor |
|---|---|---|---|
| DML (`INSERT`/`UPDATE`/`DELETE`) | **Buffered** — `buffer.push(PgWrite(mutation))`, never applied; COMMIT applies atomically, ROLLBACK discards | Applied immediately through `commit_mutations` | `query.rs:1561-1573` (buffer), `query.rs:1574-1604` (apply); `server.rs:661-667` (ROLLBACK discards) |
| Multi-row `INSERT` | **Buffered**, whole statement as one unit | Applied as **one atomic batch** | `query.rs:1646-1659`, `query.rs:1660-1688` |
| `COPY ... FROM STDIN` | **Buffered** into the *same* session write-set | Staged locally, then flushed every `FLUSH_EVERY = 1000` rows | `copy_stdin.rs:276-289`; `copy_stdin.rs:63`, `copy_stdin.rs:466-472`, `copy_stdin.rs:243-252` |
| `TRUNCATE` | **Buffered** as a table-tombstone mutation | Applied immediately (tombstone committed/replicated) | `query.rs:1399-1411`, `query.rs:1769-1770`; `query.rs:1707-1712` |
| DDL (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`) | **Refused `25001`** — "DDL is not transactional here" | Applied through the schema-change path | `ddl.rs:519-524`, `ddl.rs:566-571`, `ddl.rs:177-182`; dispatch passes `in_txn: txn.is_some()` — `query.rs:1346-1392` |
| `VACUUM`/`ANALYZE`/`SET`/`RESET` | not buffered — VACUUM/ANALYZE execute maintenance; SET/RESET refused `0A000` | same | `query.rs:1428-1465`, `query.rs:1341-1344` |
| `BEGIN`/`COMMIT`/`ROLLBACK` reaching the executor without session state | refused `0A000` | — | `query.rs:1336-1339` |
| `COPY FROM STDIN` reaching the executor (not the connection loop) | refused `0A000` | — | `query.rs:1375-1378` |

Two consequences worth stating plainly:

- A failed statement inside a transaction **poisons** it (`T` → `E`), so the later COMMIT becomes a
  ROLLBACK rather than committing a partial buffer — `server.rs:866-875`, `server.rs:905-910`,
  `extended.rs:440-444`. A DDL refusal inside a transaction therefore poisons the transaction too, since
  the refusal is an `ErrorResponse` on that path (`server.rs:866-875`).
- Read-your-own-writes inside the transaction works: the pending write-set is decoded and handed to the
  read path — `server.rs:855-864`, `query.rs:1004-1017`, `txn_write_set.rs:141-153`.

---

## 3. The COPY path (highest-value question)

### 3.1 In an open transaction, COPY **buffers**; it does not write through

The transaction-vs-autocommit decision for COPY is made in exactly one place — `copy_stdin.rs:260-293`:

```
276|    if in_txn {
...
280|        insert_rows(
281|            ctx,
282|            stmt,
283|            columns,
284|            Some(session.txn_writes_mut()),
285|            rows,
286|            inserted,
287|            false,          // flush_locally = false
288|        )
289|        .await
290|    } else {
291|        insert_rows(ctx, stmt, columns, Some(buffer), rows, inserted, true).await
292|    }
```

- `in_txn` is snapshotted once from `session.in_txn()` — `copy_stdin.rs:98`.
- Every row goes through `query::execute_insert` with the session write-set — `copy_stdin.rs:449-458`,
  which reaches `apply_or_buffer_silent`'s buffer branch — `query.rs:1561-1573`.
- `flush_locally = false` inside the transaction: no mid-statement flush, so an open transaction is never
  committed mid-COPY — `copy_stdin.rs:277-279`, `copy_stdin.rs:466-472`.
- Success reports `COPY n` and leaves the rows in the write-set — `copy_stdin.rs:233-241`.
- A failure poisons the transaction so a partial load cannot commit — `copy_stdin.rs:219-232`.

Asserted by `[test]`:

- `copy_inside_a_transaction_buffers_and_applies_on_commit` — `copy_stdin.rs:886-933`
  (`session.txn_writes().len() == 3` at `:913`, `row_count == 0` before COMMIT at `:917-923`, `1` after).
- `copy_inside_a_rolled_back_transaction_leaves_no_rows` — `copy_stdin.rs:940-974`.
- `a_failed_copy_inside_a_transaction_must_not_commit_a_partial_load` — `copy_stdin.rs:980-1007`.
- `a_large_transactional_copy_stays_bounded_and_commits_distinct_rows` — `copy_stdin.rs:1143-1201`
  (12 000 staged rows, spilled, all land at COMMIT).

### 3.2 The "autocommit semantics" in `copy_stdin.rs` are the **else** branch only

`FLUSH_EVERY = 1000` — `copy_stdin.rs:63` — and the flush call — `copy_stdin.rs:466-472` — are reached
only when `flush_locally` is true, which per `copy_stdin.rs:291` is the **autocommit** branch. The
autocommit branch also applies the remaining tail once at `CopyDone` — `copy_stdin.rs:243-252` → `flush()`
at `copy_stdin.rs:479-510` → `query::commit_mutations` with an empty read set and a fresh snapshot
(`copy_stdin.rs:489-496`). The test named in the task,
`autocommit_copy_into_a_pkless_table_still_works` (`copy_stdin.rs:1112-1131`), exercises exactly that
branch and says so in its own doc — `copy_stdin.rs:1112-1113`: "an AUTOCOMMIT COPY (no open transaction)".

So: **autocommit COPY is not atomic** — it is a series of independent 1000-row commits. **Transactional
COPY is atomic** — one write-set, one COMMIT. These are two different code paths, not one path with
autocommit semantics.

### 3.3 `pgbench -i`'s COPY really is in one transaction — but the code settles this without pgbench

`[repo-doc] specs/reference/commit-path-and-transaction-boundary-review.md:81` states "`pgbench -i`'s
entire load is `COPY` inside one `BEGIN … COMMIT`". `[repo-doc] ferrosa-postgres/specs/fmea.md:91`
(PG-TRUNCATE-03) says "`pgbench -i` runs `TRUNCATE` inside its load transaction", and
`[repo-doc] ferrosa-postgres/specs/overview.md:275` says the row-image bug "killed `pgbench -i`'s
transactional load after every row had landed".

This is confirmable directly against the client, so it was checked rather than assumed.
`[external] postgres/src/bin/pgbench/pgbench.c` (REL_16_STABLE), `initGenerateDataClientSide`:

```
4940| 	executeStatement(con, "begin");            <- transaction opens
4943| 	initTruncateTables(con);                   <- TRUNCATE inside it
4951..4967| INSERT into pgbench_branches / pgbench_tellers
4975| 	copy_statement = "copy pgbench_accounts from stdin with (freeze on)";
4979| 	res = PQexec(con, copy_statement);         <- the COPY, inside the txn
5057| 	if (PQputline(con, "\\.\n"))               <- legacy end-of-data marker
5059| 	if (PQendcopy(con))
5064| 	executeStatement(con, "commit");           <- transaction closes
```

The DDL steps are *not* in that transaction: the init driver runs `initDropTables` / `initCreateTables` /
`initCreatePrimaryKeys` as separate steps — `[external] pgbench.c:5240-5266`, with `"drop table if
exists"` at `pgbench.c:4726`, `"truncate table "` at `pgbench.c:4906`, and the `alter table … add primary
key` statements emitted by `initCreatePKeys` (`pgbench.c:5262`). That is consistent with this front end
refusing DDL inside a transaction (`ddl.rs:519-524/566-571/177-182`) while still supporting `pgbench -i`.

`BEGIN` and the COPY arrive as **separate** simple-Query messages, which this front end requires anyway:
one `Query` string is parsed as exactly one statement (`parser.rs:144-146` trims only a trailing `;`), and
an interior `;` is a hard parse error (`parser.rs:464`, `BadToken`). The COPY fast path keys on that: it
fires only when the Query's first four non-space bytes are `copy` — `server.rs:346-369`:

```
350|                    let head = sql.trim_start();
351|                    if !session.is_error_pending()
352|                        && head.len() >= 4
353|                        && head.as_bytes()[..4].eq_ignore_ascii_case(b"copy")
...
358|                            crate::copy_stdin::drive(stream, frames, read_buf, ctx, &mut session, &copy)
```

`drive` is called with the live `session` (`server.rs:363`), whose `TransactionStatus` was set by the
preceding `BEGIN`, so `in_txn` at `copy_stdin.rs:98` is `true` and §3.1 applies.

**Therefore the hypothesis that `pgbench -i`'s bulk load is applied while its transaction is still
undecided is falsified by the code**: there is no code path by which an in-transaction COPY writes
through to storage before COMMIT. The only COPY path that applies rows before the enclosing commit is the
autocommit branch, and `pgbench -i`'s COPY is not on it (both `[external]` §3.3 and, if one prefers
in-repo evidence, the fact that the load's TRUNCATE and INSERTs precede it in the same BEGIN —
`[external] pgbench.c:4940-4967`).

---

## 4. Where MVCC tracks an in-flight transaction's changes

**It does not.** This is the central divergence from the stated design intent, and it is provable.

### 4.1 The in-flight write-set lives in the front end, not in MVCC

`Session.txn_writes: TxnWriteSet` — `extended.rs:95-103`:

```
95|    /// Buffered DML write-set for the open `BEGIN`/`COMMIT` block. DML inside a
96|    /// transaction is BUFFERED here instead of applied; `COMMIT` drives the whole
97|    /// set through the PostgreSQL MVCC manager atomically; `ROLLBACK`/`end_txn` clears it
```

`TxnWriteSet` is a threshold-bounded, **spilling** staging structure, not a resident vector —
`txn_write_set.rs:47-52`:

```
47|pub struct TxnWriteSet {
48|    stage: WriteSetStage,
49|    count: usize,
50|}
```

- one write is serialized to a `Mutation` frame and appended to the stage — `txn_write_set.rs:88-95`;
- the commit path consumes the staging as a stream — `txn_write_set.rs:160-162`;
- the only materialization is the read-your-own-writes peek — `txn_write_set.rs:141-153`;
- there is deliberately no capacity refusal (the retired `FERROSA_POSTGRES_MAX_TXN_WRITES`/`53400`) —
  `txn_write_set.rs:16-18`, `mvcc.rs:6-9`.

The mutation a write becomes is `PgWrite(pub(crate) Mutation)` — `mvcc.rs:227-230`.

### 4.2 What MVCC actually holds: committed version history + live snapshots + read rows

`MvccManager` state — `mvcc.rs:254-267`:

```
255|struct State {
256|    commit_seq: u64,
257|    next_snapshot_id: u64,
258|    active_snapshots: HashMap<u64, ActiveSnapshot>,
259|    versions: HashMap<RowKey, BTreeMap<u64, Option<Row>>>,
260|    distributed_versions: HashMap<RowKey, BTreeMap<Timestamp, Option<Row>>>,
261|    /// Rows read BY KEY by each live snapshot. ...
264|    read_rows: HashMap<u64, HashSet<RowKey>>,
265|    applied_accord_txns: HashSet<(TxnId, Timestamp)>,
266|    table_epochs: HashMap<String, u64>,
267|}
```

- `versions` is keyed by **commit sequence** (`u64`), `distributed_versions` by **Accord timestamp** —
  `mvcc.rs:259-260`, comparison semantics spelled out at `mvcc.rs:762-764`.
- Both are *committed* histories, pruned against the oldest live snapshot — `mvcc.rs:867-893`. They are not
  per-transaction staging: an in-flight transaction has written nothing here yet.
- `read_rows` is per **snapshot id** and dropped with the snapshot — `mvcc.rs:886-892`, `mvcc.rs:261-264`.

So the owner's stated intent — "the MVCC system IS the concurrency buffer … empty otherwise" — is **not**
what the code does. MVCC holds post-commit history and read sets; the front-end `TxnWriteSet` holds the
in-flight changes. Recorded as a contradiction in §7.

### 4.3 How the write-set becomes the commit's apply

`commit_txn` — `server.rs:901-1220`:

1. Take the staged set (leaves the session empty) — `server.rs:912-921` → `extended.rs:422-427` →
   `txn_write_set.rs:160-162`;
2. take the scanned read tables and the txn snapshot — `server.rs:922-926`;
3. empty set ⇒ validate only and commit — `server.rs:928-987`;
4. otherwise stream the staging once to learn the written tables (no materialization) —
   `server.rs:995-1007`;
5. **cluster path** (`ctx.accord.committer().is_some()`): `validate_commit` first (`server.rs:1027`), then
   `prepare_accord_writes_streaming` into a `StagedAccordWriteSetBuilder` (`server.rs:1055-1061`), then
   `committer.commit_postgres(...)` (`server.rs:1100-1102`);
6. **standalone path**: `query::commit_mutations(...)` — `server.rs:1134-1143`.

`commit_mutations` — `query.rs:1784-1817` — builds the MVCC row images
(`prepare_row_changes_from_source`, `query.rs:1795`), records each *written* key as a point read
(`query.rs:1800-1808`), then calls `mvcc.commit(snapshot, read_tables, || engine.write_atomic_batch(source))`
(`query.rs:1809-1816`). `MvccManager::commit` validates, applies, then inserts the version entries and
advances `commit_seq` — `mvcc.rs:546-581`.

On the cluster path, the accord apply observer writes the committed history instead, idempotently —
`mvcc.rs:596-638` (`record_applied_accord_commit`) and its `prepare`/`on` hooks — `mvcc.rs:661-738`.
Note the ordering: `prepare_postgres_apply` inserts the distributed versions **before** the storage batch,
deliberately, so a racing scan can retain its Accord snapshot — `apply.rs:1300-1319`, `mvcc.rs:614-617`.

---

## 5. Read-set tracking: who records what, and what it is used for at COMMIT

### 5.1 The recorder and its gate

`track_select_reads` — `server.rs:715-740` — called from the simple-query data path for a `SELECT` —
`server.rs:850-852`. Two arms:

- **Point read** (`WHERE <partition-key> = <literal>`, single table, no join): recorded as the **row**, and
  the table is *not* recorded — `server.rs:721-726`, `server.rs:747-778`
  (`point_read_row_key` checks `table.partition_key.contains(&column.name)` — `server.rs:771-773`);
- **Scan / join**: recorded as the **table** (and the joined table) — `server.rs:727-739`.

Gate: only when `in_txn && isolation == Some(Serializable)` — `server.rs:716-720`. A plain `BEGIN` records
nothing (§1.2).

Storage of point reads: `MvccManager::record_point_read` inserts under the live snapshot id, and no-ops for
a snapshot that has already left `active_snapshots` — `mvcc.rs:484-489`. The only two call sites are
`server.rs:723` (gated SELECT) and `query.rs:1801` (the standalone commit path's written keys).

### 5.2 What it is used for at COMMIT

`validate_snapshot` — `mvcc.rs:750-798`:

1. snapshot still live, else `SnapshotExpired` — `mvcc.rs:755-757`;
2. **row-granular first**: any row in `read_rows[snapshot.id]` whose local version key `> read_ts`
   (or, when `cluster_ts` is present, whose distributed version `> cluster_ts`) ⇒
   `SerializationFailure` — `mvcc.rs:765-784`;
3. **table-granular, scans only**: any read table whose `table_epochs` entry `> read_ts` ⇒
   `SerializationFailure` — `mvcc.rs:789-796`.

Reached from COMMIT via `MvccManager::validate_commit` (`mvcc.rs:461-468`) at `server.rs:932` (empty
write-set) and `server.rs:1027` (cluster path), and via `MvccManager::commit` (`mvcc.rs:553`) on the
standalone path. The set handed over is the **scanned** tables only; written tables are folded into
`write_tables` for the Accord `tables` argument and are *not* used as read evidence — `server.rs:993-999`
(comment: "a table this transaction merely WROTE is judged by the rows it wrote, not by table identity"),
`server.rs:1098`.

Also relevant: a write records the key it wrote as a point read, so same-key write/write still conflicts
while disjoint writes do not — `query.rs:1796-1808` (comment) and `[repo-doc]
specs/todo/bug-txn-read-modify-write-loses-the-cell.md:620-626`.

The pre-statement liveness probe is deliberately *not* `validate_commit`; only expiry aborts a later
statement — `server.rs:883-892`, `mvcc.rs:470-477`.

---

## 6. Commit-path failures → SQLSTATE on the wire

All arms are in `commit_txn`'s final `match outcome` — `server.rs:1179-1219` — plus its earlier
empty-write-set twin — `server.rs:931-987`. `error_response` writes `S=ERROR`, `C=<sqlstate>`, `M=<msg>` —
`query.rs:59-67`.

| Failure | Where produced | Mapping | SQLSTATE |
|---|---|---|---|
| **Abandon** (Accord dependency-wait bound expired; `TxnAbandoned`) | `coordinator.rs:557-560`; Display carries the load-bearing `abandoned:` prefix — `coordinator.rs:571-582` | committer erases the type to `reason: String` (`transaction_commit.rs:319-321`) → `Error::InvalidData` (`server.rs:1119-1121`) → matched on the `abandoned:` prefix | **`40001`** "transaction was abandoned and NOT committed" — `server.rs:1208-1214` |
| **Serialization failure** (MVCC conflict) | `validate_snapshot` — `mvcc.rs:772`, `mvcc.rs:780`, `mvcc.rs:795` | `MvccCommitError::SerializationFailure` | **`40001`** — `server.rs:1183-1186` (and `server.rs:935-940` for the empty set) |
| **Serialization failure** (Accord aborted) | `commit_postgres` → `CommitOutcome::Aborted` — `transaction_commit.rs:316-318` | `server.rs:1107-1109` → `SerializationFailure` | **`40001`** — `server.rs:1183-1186` |
| **Stale snapshot** at cluster validation | `validate_postgres_snapshot` → `Ok(false)` — `transaction_commit.rs:250` | `server.rs:971-976` | **`40001`** — `server.rs:972-975` |
| **Snapshot expired** | `mvcc.rs:755-757` | `MvccCommitError::SnapshotExpired` | **`40001`** — `server.rs:1187-1190`; pre-statement probe also `40001` — `server.rs:888-891` |
| **Backpressure** (overloaded / disk reserve) | `Error::is_backpressure()` — `ferrosa-common/src/error.rs:65-71` | `write_error_response` / the explicit arm | **`53000`** — `server.rs:1196-1201`, `server.rs:941-946`, `query.rs:1262-1278` |
| **Network** (Accord driver `Network`, `QuorumUnavailable`, codec) | `coordinator.rs:566-568` ("Accord network error: …") | erased to `reason: String` → `InvalidData` → **no** `abandoned:`/backpressure match | **`58000`** `system_error` — `server.rs:1215-1218` |
| **Other storage fault** / missing cluster timestamp / staging or prepare I/O | `server.rs:1091-1097`, `server.rs:914-920`, `server.rs:1062-1081` | `MvccCommitError::Storage` unmatched | **`58000`** |
| **Non-serializable explicit isolation** (`BEGIN ISOLATION LEVEL READ COMMITTED`) | `server.rs:682-687` | immediate refusal, never opens the txn | **`0A000`** |
| **Statement after a poisoned transaction** | `server.rs:633-643`; extended path `copy_stdin.rs:306-311` | — | **`25P02`** |
| **Client/peer vanishes mid-COPY** | `copy_stdin.rs:201-206` | `Ok(())` — no error is sent (nobody left to send to) | **none** |
| **Protocol framing violation** | `server.rs:398-405`, `server.rs:1838-1840` | `write_fatal`, severity `FATAL` — `server.rs:222-236` | **`08P01`** |
| **Client disconnects while in a transaction** | no callback exists; `Session` is a local dropped by the loop | buffered write-set is dropped unapplied | **none** |

Two things this table makes explicit:

- **There is no `40001`-vs-`58000` collapse for abandon** — that was fixed and is now prefix-matched
  (`server.rs:1202-1214`, comment). But the classification is **string-based**: the typed
  `AccordDriverError` is erased to `reason: String` in the committer trait before it reaches the front end
  (`transaction_commit.rs:319-321`, noted in `server.rs:1110-1121`), so a `Display` change silently
  reclassifies an abandon as `58000`.
- **A network failure from the Accord commit is `58000 system_error`**, i.e. classified as "something is
  broken", not as retryable — including `QuorumUnavailable` and `SnapshotStale` if either ever reaches the
  front end as an erased reason rather than as `CommitOutcome::Aborted`.

---

## 7. Contradictions (recorded as contradictions, not resolved)

**C1 — "MVCC is the concurrency buffer" vs the code.** The owner's stated intent (and
`[repo-doc] specs/reference/commit-path-and-transaction-boundary-review.md:39-52`, `[git] a47762d6`,
`[git] 32fc001a`) is that MVCC tracks an in-flight transaction's changes and is empty otherwise. The code
puts the in-flight changes in `Session.txn_writes: TxnWriteSet` (`extended.rs:103`,
`txn_write_set.rs:47-52`) and uses MVCC for committed history + read sets (`mvcc.rs:254-267`). Both layers
exist; they are not the same layer.

**C2 — The review doc's own F1 retraction is right about buffering but wrong about the COPY lead.**
`[repo-doc] …review.md:59-86` retracts F1 ("the code states the opposite: DML is buffered") and then names
the COPY path as the remaining lead: "`COPY` does not participate in the session write-set buffer the way
single-statement DML does". The code says the opposite of *that* too: in-transaction COPY takes the
*same* `apply_or_buffer_silent` seam (`copy_stdin.rs:449-458` → `query.rs:1561-1573`) and has its own
test asserting the buffering (`copy_stdin.rs:886-933`). The module doc says it in one sentence —
`copy_stdin.rs:22-28`.

**C3 — "a plain BEGIN stores isolation `None`, so the statement-level path does not treat it as a tracked
transaction" is half wrong.** `[repo-doc] …review.md:106-107` and
`[repo-doc] specs/todo/bug-txn-read-modify-write-loses-the-cell.md:677-679` ("a plain `BEGIN` stores
isolation `None`, and the non-serializable path records nothing"). The second clause is correct (read sets
are empty). The first is not: a plain `BEGIN` sets `TransactionStatus::InTransaction`
(`extended.rs:331-335`) and *does* engage the write-set buffer (`server.rs:854-862`, `server.rs:1785-1790`).
`None` disables *reads* and *read-set recording* (`server.rs:826-836`, `server.rs:716-720`), not tracking.

**C4 — the RT-2 "layer 2" cause is stale, its conclusion is not.** `[repo-doc]
specs/todo/bug-txn-read-modify-write-loses-the-cell.md:643-657` says the row check is inert live because
the commit-path snapshot "carries NO cluster timestamp" and `snapshot_with_cluster_ts` "is used only by
tests". In this revision `begin_block` takes the snapshot *through the committer*, so it **does** carry a
cluster timestamp (`server.rs:688-700`), and `snapshot_with_cluster_ts` is called at `server.rs:690`.
The conclusion still holds, but by a different mechanism: `read_rows` is only ever populated at
`server.rs:723` (gated on SERIALIZABLE, so empty for a plain `BEGIN`) and `query.rs:1801` (the *standalone*
`commit_mutations`). The **cluster/Accord** commit path (`server.rs:1026-1133`) records no point reads, so
the row-granular check at `mvcc.rs:765-784` is inert there — which is what `…review.md:643-657` observed.

**C5 — "the row check examines `versions` (empty — nothing writes it on the Accord path)".** Correct as
stated (`mvcc.rs:596-638` writes `distributed_versions`, not `versions`), but note it *is* the mechanism
that makes the check inert on the Accord path for reads, independent of `cluster_ts`.

**C6 — the task's cited line for the SERIALIZABLE read-set gate.** The task cites `server.rs:710-714`;
in this revision that range is the doc comment and the executable gate is `server.rs:716-720`. Same code,
shifted lines. (Both `…review.md:64` and the earlier `…review.md` revisions cite `server.rs:709/715-722`
for the same function, so several line references in the docs are from different revisions.)

**C7 — the 669,315-row observation vs every code path found.** The observed fact
(`[repo-doc] …review.md:76-97`: a run whose commit was reported abandoned left **669,315 of 1,000,000**
rows readable) is not explained by any path in this front end:

- In-transaction COPY buffers and commits atomically (`copy_stdin.rs:276-289`, `copy_stdin.rs:886-933`),
  and `pgbench -i`'s COPY *is* in-transaction (`[external] pgbench.c:4940-5064`). A single abandoned
  COMMIT of that transaction should leave **0** rows.
- The only COPY path that applies rows before an enclosing commit is autocommit
  (`copy_stdin.rs:243-252`), which `pgbench -i`'s COPY is not on.
- The coordinator's own claim is that once the apply quorum is secured the transaction "is never reported
  as abandoned" (`[code] coordinator.rs:550-552`), so "abandoned but applied" is asserted not to happen.
- 669,315 is **not** a multiple of `FLUSH_EVERY = 1000` (`copy_stdin.rs:63`), so even the autocommit
  batching hypothesis does not reproduce the number: a partial tail is dropped on failure
  (`copy_stdin.rs:219-232`) and the final remainder flushes only at `CopyDone` (`copy_stdin.rs:243-252`).

Remaining candidate explanations, all unproven: the rows came from an **earlier, separate** load in the
same run (the count is from a table census, and `[repo-doc] …review.md:86` itself warns "a count proves
nothing"); or the abandon was reported for a **later** commit than the one that made the rows durable; or
an abandon was reported *after* the apply quorum was secured, contradicting `coordinator.rs:550-552`.
The decisive experiment is unchanged and is the only thing that would settle it: `BEGIN; COPY (3 rows);
<force the COMMIT to fail>;` followed by a **point lookup** of a known key asserted absent
(`[repo-doc] …review.md:233-236`).

**C8 — autocommit COPY is not atomic, and nothing says so in the operator-facing docs.**
`copy_stdin.rs:30-35` and `copy_stdin.rs:55-63` document the `FLUSH_EVERY` batching as a memory bound, but
the observable consequence — an autocommit `COPY` that fails at row *n* leaves `⌊n/1000⌋ × 1000` rows
applied and cannot roll them back — is stated only inside the failure branch's comment
(`copy_stdin.rs:224-227`). In-transaction COPY does not have this property.

---

## 8. Could not determine

1. **Which mechanism produced the 669,315 rows.** No path in this front end applies an in-transaction
   COPY early (§7 C7). Settling it needs the point-lookup-after-forced-commit-failure experiment, not
   another code read.
2. **Whether the abandon report in that run came from the load's own COMMIT or from a different
   transaction.** The code makes a single abandoned COMMIT of that transaction leave 0 rows, so the two
   are not the same event — but nothing in the repo pins which COMMIT the operator saw. The node log line
   ("abandoning the transaction (NOT committed; safe to retry)", `coordinator.rs:579-580`) is not
   correlated with the load in any artefact available here.
3. **The exact transport shape of a live `pgbench -i` against this server.** `[external] pgbench.c` proves
   the *client* wraps the COPY in `begin`…`commit`; it does not prove the front end's `session.in_txn()`
   was `true` at `copy_stdin.rs:98` for that connection (e.g. an intervening error/`Sync`, an
   `is_error_pending()` state at `server.rs:351`, or a mid-roll image). No captured wire trace or server
   log from that connection was available here.
4. **Whether `prepare_accord_writes_streaming` (`query.rs:2027`) is expected to record written keys as
   point reads on the cluster path.** It does not (`record_point_read` has exactly two call sites:
   `server.rs:723`, `query.rs:1801`), and no doc in the repo states whether that is intended or an
   omission — so the row-granular conflict check's inertness on the Accord path (`mvcc.rs:765-784`) is
   provable but its intent is not.
5. **Why a plain `BEGIN` takes a snapshot it then does not read from.** `begin_block` takes a
   committer-backed snapshot (`server.rs:688-700`) and `begin_txn` stores it (`extended.rs:334`), but
   `read_snapshot` ignores it unless the isolation is explicitly `Serializable` (`server.rs:826-836`), and
   the pre-statement expiry probe (`server.rs:883-892`) and commit validation (`server.rs:923-926`) do use
   it. Whether that asymmetry is intended (PostgreSQL's default is READ COMMITTED, so per-statement
   snapshots are arguably faithful) or an oversight is not stated anywhere I found.
6. **Behaviour of the extended-protocol `Execute` path for COPY.** COPY reaches the executor only as a
   `0A000` refusal (`query.rs:1375-1378`); the fast path that actually drives COPY lives in the
   simple-query connection loop (`server.rs:346-369`). Whether a client attempting COPY via Parse/Bind/
   Execute is expected to work is not documented. (Task text mentions `server.rs:1776` as an "applies
   immediately" site: in this revision `1771-1777` is the comment and the executable buffering decision is
   at `1785-1790` — same code, shifted lines.)
7. **`apply.rs` in `ferrosa-postgres`.** There is no `ferrosa-postgres/src/apply.rs`; the apply path named
   in the task is `ferrosa-cluster/src/accord/apply.rs` (e.g. the `postgres_mvcc_metadata` gate at
   `apply.rs:1300`). If a PG-side `apply.rs` was expected, it does not exist in this revision.

---

## 9. Files read for this document (all read-only)

`ferrosa-postgres/src/{server.rs, query.rs, copy_stdin.rs, mvcc.rs, extended.rs, ddl.rs, txn_write_set.rs, lib.rs}`,
`ferrosa-cluster/src/accord/{apply.rs, coordinator.rs, transaction_commit.rs}`,
`ferrosa-sql/src/parser.rs`, `ferrosa-common/src/error.rs`,
`specs/reference/commit-path-and-transaction-boundary-review.md`,
`specs/todo/bug-txn-read-modify-write-loses-the-cell.md`,
`ferrosa-postgres/specs/{overview.md, fmea.md}`,
`[external] postgres REL_16_STABLE src/bin/pgbench/pgbench.c` (saved at
`/Users/bkearns/.hermes/cache/scratch/pgbench.c`).
