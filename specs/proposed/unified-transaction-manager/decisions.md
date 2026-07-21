---
title: Unified Transaction Manager — Decision Record
status: proposed
component: transaction-manager
executive_summary: >
  Replace ferrosa's fragile connection-keyed BEGIN/COMMIT transaction model with a
  connection-independent, txn-id-keyed transaction manager backed by MVCC storage and
  committed through Accord. The txn-id is exposed through the CQL surface (BEGIN returns
  it as a result row; statements reference it via `IN TRANSACTION <id>`), keeping the
  wire 100% compatible with stock Cassandra drivers while enabling full interactive SQL
  transactions (read-your-writes, isolation levels, savepoints) across a Postgres/SQL
  front-end. Phase A (registry + CQL surface + Accord commit, single-node registry)
  unblocks strict-serializable list-append certification immediately; MVCC storage and
  the SQL front-end are the larger "full SQL transaction management" phases.
last_revised: 2026-07-20
---

# Unified Transaction Manager — Decision Record

## Context

ferrosa layered a stateful, multi-round-trip transaction (`BEGIN → UPDATE → COMMIT`)
onto CQL and keyed the transaction state to the **TCP connection**
(`ferrosa-cql/src/session.rs::CqlTransaction`, held per-connection in
`ferrosa-cql/src/connection.rs`). Elle `list-append` certification exposed that this is
fragile by construction: stock Cassandra drivers make no guarantee that a statement
sequence stays on one connection (they pipeline, pool, reconnect), and the server
dispatches QUERY on concurrent spawned tasks sharing one `Arc<Mutex<CqlTransaction>>`.
The result was ~24% indeterminate commits from transaction-lifecycle desync
(`COMMIT outside of a transaction` / `nested transactions`), which tainted the Elle
anomaly graph even though the underlying Accord commits are reliable.

This is a **design mismatch**, not a single bug: CQL is a stateless request/response
protocol, and keying interactive-transaction state to the connection assumes an affinity
the ecosystem does not provide.

## Decision

Adopt a **connection-independent, txn-id-keyed unified transaction manager**, backed by
**MVCC storage** and committed through **Accord**, shared by all wire front-ends (CQL
today; Postgres/SQL next).

- **Txn-id lives on the CQL surface, not in the protocol frame.** `BEGIN TRANSACTION`
  allocates a transaction id and returns it as an ordinary `RESULT: Rows` frame
  (`[txn_id]`). Subsequent statements reference it in CQL text
  (`UPDATE … IN TRANSACTION <id>`, `SELECT … IN TRANSACTION <id>`,
  `COMMIT TRANSACTION <id>`, `ROLLBACK TRANSACTION <id>`). This uses only standard
  `QUERY`/`RESULT` frames, so java-driver, gocql, python, and the scylla driver all work
  unmodified. Application logic threads the id (a documented CQL extension).
- **State is keyed by txn-id in a server-side registry**, never the connection. A
  statement may arrive on any connection; reconnects/pooling/interleaving become
  harmless.
- **MVCC provides the read side** of full transaction semantics: each transaction reads a
  consistent snapshot at its read timestamp and sees its own staged writes
  (read-your-writes); provisional writes are **intents** until commit. This is what makes
  interactive SQL transactions (read → branch → write → commit) correct.
- **Accord provides the distributed atomic commit** (existing
  `AccordTransactionCommitter`): on commit, intents become committed versions at the
  Accord execution timestamp; on abort, intents are discarded.
- **The core is front-end-agnostic and commit-protocol-agnostic.** CQL exposes the txn-id
  explicitly; a future Postgres/SQL front-end maps its connection→txn-id internally
  (presenting standard connection-sticky `BEGIN/COMMIT` to psql/JDBC). The commit seam is
  pluggable (Accord for cross-partition strict-serializability; a single Raft group for
  one-shard transactions).

## Rejected alternatives

| Option | Why rejected |
|---|---|
| **Protocol-frame txn-id** (new opcode / header field) | Breaks wire compatibility — no stock Cassandra driver carries it. |
| **#2: single-request `BATCH` transactions** (Accord-routed BATCH) | Simpler and wire-compatible, but **structurally cannot express interactive SQL** (read → decide → write). A dead-end for the SQL goal. Retained as an *optimization* for blind-write transactions, not the general model. |
| **#3: harden the connection-keyed model** (serialize + ship a pinning client) | Still fragile; still needs driver/harness work; does not generalize to SQL. |

## Locked constraints

1. **Wire-compatible with stock Cassandra drivers** — no new opcodes or frame fields.
2. **Preserve interactive multi-round-trip transactions** — `BEGIN … read … write … COMMIT`.
3. **Accord remains the strict-serializable distributed commit** (existing machinery).
4. **Front-end-agnostic core** — one transaction manager under CQL and future Postgres/SQL.
5. **MVCC is in scope** — full SQL transaction management requires snapshot reads +
   write intents + GC, not the current Cassandra-style newest-wins LWW.
6. **No regression to PR #283** — the four Accord correctness fixes ship independently;
   this refactor sits above them at the session/transaction-manager layer.
7. **Transactions are bounded by NVMe, not RAM.** A transaction's staged writes / MVCC
   write intents live in a **per-transaction temp table on local NVMe**, not in memory.
   The in-RAM registry holds only per-transaction *metadata* (id, read_ts, owner,
   deadline, temp-table handle). On commit, the temp table is promoted into the permanent
   SSTable write path at the Accord execution timestamp; on abort it is dropped. This
   bounds a transaction's size by NVMe capacity (enabling large/bulk transactions),
   keeps RAM bounded regardless of transaction size, and — because temp tables are
   on-disk — makes intents recoverable across a process restart. It maps directly onto
   ferrosa's existing "NVMe as write-behind cache, S3 as durable" model.
8. **Transactions are bounded in time by a tunable open-timeout (default 10s).** A
   transaction open longer than `transaction_open_timeout` is aborted (temp table
   dropped, entry evicted). `CqlTransaction` already carries a per-transaction `deadline`;
   this makes the default configurable and **registry-enforced** by a reaper, not only
   checked lazily on the next statement. The time bound is the natural *upper* bound on a
   transaction (complementing the NVMe *size* bound) and, critically, it **bounds MVCC
   version retention**: the read low-water-mark can never be older than the oldest open
   transaction (≤ timeout), so version-GC need only retain roughly one timeout-window of
   versions — capping MVCC storage.

## Open decisions (to confirm)

| # | Decision | Recommendation |
|---|---|---|
| D1 | **Registry locality in a multi-node cluster** — where does a transaction's entry live, and how do statements for a txn-id reach it? | Encode the owning coordinator in the txn-id; receiving nodes **forward by txn-id** (NewSQL-style transaction record). Phase A ships **single-node registry** (sufficient for the pinned-coordinator Elle workload and to unblock the cert); multi-node routing is Phase B. |
| D2 | **Txn-id type + unguessability + auth scope** | 128-bit random (or TimeUUID with random low bits); registry entry records the authenticated principal/tenant; a statement referencing a txn-id it does not own fails loud. |
| D3 | **Abandoned-transaction policy** | Per-txn deadline (already present on `CqlTransaction`) + registry TTL eviction; on eviction/abort/expire the temp table is dropped (NVMe reclaimed). Registry entry *count* is bounded (fail-loud on overflow); per-transaction *data* is bounded by NVMe (constraint 7), and a transaction that would exceed the NVMe budget fails loud rather than evicting live data. |
| D4 | **Isolation levels** | Snapshot Isolation as the default read semantics (MVCC read-ts); Serializable via Accord for the commit. Expose `SERIALIZABLE` explicitly; document SI as the interactive-read default. |
| D5 | **Compat shim for existing connection-keyed BEGIN** | Keep `BEGIN TRANSACTION` (no id) working during transition by internally allocating a txn-id bound to the connection; deprecate with a warning; remove after the SQL front-end lands. |
| D6 | **MVCC version retention / GC** | Retain versions until below the cluster read low-water-mark (oldest live snapshot); GC on compaction, analogous to `gc_grace`. The open-timeout (constraint 8, default 10s) bounds the low-water-mark to roughly one timeout window, so retention is small and predictable — GC is not at the mercy of an arbitrarily long-lived reader. |
| D7 | **Open-transaction timeout default + config surface** | `transaction_open_timeout`, default **10s**, per-`USING TIMEOUT` override on `BEGIN` up to a hard cluster max; registry reaper enforces it (not just lazy check). Rejected: unbounded transactions (would defeat both the NVMe bound and MVCC-GC bound). |

## Consequences

- **Immediate:** Phase A unblocks strict-serializable list-append certification (the Elle
  harness adopts `IN TRANSACTION <id>`; the connection-affinity desync disappears by
  construction — no driver or raw-socket work).
- **Strategic:** establishes the transaction core (registry + MVCC + Accord commit) that a
  Postgres/SQL front-end plugs into, turning "SQL transactions on ferrosa" from a rewrite
  into two scoped additions (MVCC storage, front-end mapping).
- **Cost:** a server-side transaction registry with lifecycle/security/eviction, and a
  real MVCC subsystem in the storage engine (versioned rows + intents + snapshot reads +
  GC). Both are load-bearing for the SQL vision and are phased.

See [architecture.md](architecture.md), [fmea.md](fmea.md), and
[project-plan.md](project-plan.md).
