---
title: Unified Transaction Manager — Project Plan
status: proposed
component: transaction-manager
executive_summary: >
  Six phases. Phase A (txn-id registry + CQL `IN TRANSACTION <id>` surface + Accord commit,
  single-node registry) is the quick win — it unblocks strict-serializable list-append
  certification and deletes the connection-affinity bug class. Phases B–C add multi-node
  txn-id forwarding and remove dead state. Phases D–F are the "full SQL transaction
  management" build: MVCC storage (versions + intents + snapshot reads + GC), isolation
  levels, a Postgres/SQL front-end, and durable/recoverable transaction state for
  coordinator failover. Each phase has explicit acceptance gates; MVCC ships behind a
  property-tested version-visibility invariant.
last_revised: 2026-07-20
---

# Unified Transaction Manager — Project Plan

Dependencies: A → B, A → C, A → D(MVCC) → E(isolation) → F(SQL front-end); F′ (durable
state) depends on B. MVCC (D) is the critical-path enabler for interactive SQL.

## Phase A — Txn-id registry + CQL surface + Accord commit (single-node) — QUICK WIN

Unblocks the Elle certification and removes the desync bug class.

- **A1** `TransactionRegistry` (`txn-id → CqlTransaction`) with lifecycle
  (begin/stage/read/commit/abort), bounded size + per-txn deadline. Replaces the
  per-connection `Arc<Mutex<CqlTransaction>>`.
- **A1b** Open-timeout reaper (constraint 8): `transaction_open_timeout` config, default
  **10s**, with an optional `BEGIN … USING TIMEOUT` override up to a hard cluster max; a
  background sweep actively aborts+evicts past-deadline transactions (`CqlTransaction`
  already has the `deadline`/`check_deadline` primitive — make it config-driven and
  reaper-enforced). Gate: a transaction idle past the timeout is aborted and its resources
  reclaimed without a client statement.
- **A2** Parser + surface: `BEGIN TRANSACTION` returns `RESULT: Rows [[txn_id]]`;
  `… IN TRANSACTION <id>` on UPDATE/SELECT/DELETE; `COMMIT|ROLLBACK TRANSACTION <id>`.
- **A3** Route staged set through the existing `AccordTransactionCommitter` (unchanged).
  Phase A stages in the existing in-RAM `Vec<BatchOp>` (small transactions) to unblock the
  cert; the **NVMe temp-table staging store** (constraint 7 — transactions bounded by
  NVMe) lands with the intent store in Phase D.
- **A4** Compat shim (D5): bare `BEGIN` allocates an internal id bound to the connection;
  deprecation warning; disallow mixing modes on one connection.
- **A5** Remove the dead `TransactionState` bool (session.rs:19) — it is test-only.
- **A6** Migrate `ferrosa-jepsen/examples/elle_list_append.rs` to the `IN TRANSACTION <id>`
  flow.
- **Gates:** unit tests for registry lifecycle + auth scope (F1) + eviction (F2);
  a single-node Elle `list-append` run reaches `valid? true` with `:info` ≈ 0 (no
  connection-affinity desync). Full `ferrosa-cql` + `ferrosa-cluster` suites green.

## Phase B — Multi-node txn-id routing

- **B1** Encode the owning coordinator in the txn-id.
- **B2** Forward `… IN TRANSACTION <id>` for a non-local id to the owner; unknown/foreign
  id fails loud (F6).
- **B3** Per-txn-id serialization across forwarded statements (F7).
- **Gates:** 3-node test — a client whose statements land on different nodes still applies
  each exactly once; foreign-id rejection test.

## Phase C — Cleanup + docs

- **C1** Deprecate/remove the connection-keyed shim once safe.
- **C2** Crate docs (`ferrosa-cql`, `ferrosa-session`) + a CQL transaction-extension spec.

## Phase D — MVCC storage (the full-SQL enabler)

The largest addition; ships behind a property-tested invariant, not by inspection.

- **D1** Versioned rows in `ferrosa-storage`: retain committed versions by commit ts;
  snapshot read returns newest version `≤ read_ts`.
- **D2** Write intents on a **per-transaction NVMe temp table** (constraint 7):
  provisional, txn-scoped records staged on local NVMe (memtable spill + temp local
  SSTable segments, never published to S3 until commit); read-your-writes; conflict
  visibility; per-transaction + global NVMe budget with fail-loud abort on exhaustion
  (F2/F2b). Bounds transaction size by disk, not RAM.
- **D3** Commit flips this txn's intents to committed versions at the Accord execution ts;
  abort/crash sweeps intents (F5).
- **D4** Version GC below the cluster read low-water-mark, on compaction (F4).
- **D5** read_ts from the shared HLC (reuse the t_813caf39 witness work) so snapshots are
  coherent with commit order (F8).
- **Gates:** property test — no snapshot ever misses a version `≤` its read_ts under
  concurrent GC; abort/crash leaves no live intents; multi-key atomicity extended to
  intents (F11).

## Phase E — Isolation levels

- **E1** Snapshot Isolation as the interactive-read default (read@read_ts ∪ own intents).
- **E2** Serializable via Accord for the commit; expose `SERIALIZABLE`.
- **E3** Savepoints (`SAVEPOINT`/`ROLLBACK TO`) as intent checkpoints.
- **Gates:** Elle `rw-register` / an SI checker passes for interactive read-write
  workloads; savepoint round-trip tests.

## Phase F — SQL/Postgres front-end + durable txn state

- **F1** Postgres wire front-end mapping `(pg-connection → txn-id)` onto the registry;
  standard `BEGIN/COMMIT` UX to psql/JDBC; reuses MVCC + Accord.
- **F2** Durable/recoverable registry entries (replicate or reconstruct) so a coordinator
  failure fails a long interactive transaction cleanly, or resumes it (F3).
- **Gates:** psql interactive-transaction acceptance suite; kill-owner test yields a clean
  abort with no partial commit.

## Sequencing note

Phase A is independently shippable and is the immediate deliverable (unblocks the cert,
deletes the bug class). Phases D–F are a multi-sprint platform initiative — the "full SQL
transaction management with MVCC" vision — and should be scheduled deliberately, not
folded into the harness fix.
