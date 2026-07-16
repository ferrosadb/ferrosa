# Storage Multi-Write Batch / Transaction Primitive

> Docs triage note (2026-07-15): moved from `specs/todo/` to `specs/implemented/`.
> Implementation evidence: `ferrosa-storage/src/engine.rs` exposes `BatchOp`,
> `apply_batch`, and `BatchTxn`; `ferrosa-storage/tests/batch_primitive.rs`
> covers atomic visibility, all-or-nothing failure, restart durability, empty
> batches, unregistered-table errors, transaction commit/abort, periodic-sync
> durability, oversized-entry rejection, and partition tombstones.
> Verification run: `cargo test -p ferrosa-storage --test batch_primitive`
> (9 passed).

> Spec: URS-QEC-X02 (one real `StorageEngine` batch/transaction primitive for
> delete-cascade, Bolt transactions, and forget — not three divergent paths).
> Relates: `specs/bug-accord-lwt-acks-phantom-write.md`,
> `specs/feat-query-engine-completeness.md` (D03 / B02 / X01 / X02).
> Status: implemented and locally verified.

## 1. Problem

Three consumers need to apply a set of mixed row writes and tombstones as **one
atomic, durable, all-or-nothing unit**, and today they either don't have a path
or use divergent ad-hoc ones:

1. **Accord LWT apply** (`ferrosa-cluster/src/accord/apply.rs`) — the committed
   `StorageApplier::apply` is a `NoopStorageApplier` in production; the real
   applier must persist exactly **one** mutation (the LWT row write/tombstone)
   to the engine, fail-loud, idempotently. Today the LWT-via-Accord route
   fabricates `[applied]=true` without writing (`router.rs:1029-1041`) — a
   "fake success", the worst failure class.
2. **Cypher delete-cascade** (D03, `ferrosa-graph`) — `DETACH DELETE` must
   tombstone a **vertex row + N incident edge rows + the derived adjacency
   index rows** together. Adjacency rows are produced by
   `AdjacencyIndexObserver` (`derive_adjacency_mutations`). Today derived
   observer mutations are applied **outside** the atomic batch boundary, in
   `StorageEngine::dispatch_sync_observers`, with **swallowed errors**
   (`let _ = state.store.write(...)`, `continue` on commit-log failure) — both
   an atomicity hole and a fail-loud violation.
3. **Bolt explicit transactions** (B02, M2) — `BEGIN`/`run`*/`COMMIT`/`ROLLBACK`
   must stage statements and apply them atomically at `COMMIT`, dropping them on
   `ROLLBACK`. `ferrosa-cql/src/session.rs::TransactionState` parses the
   transitions but has **no production storage callers** — staged statements
   currently execute as independent non-atomic writes.

## 2. What already exists (build on, don't replace)

`StorageEngine::write_atomic_batch(mutations: Vec<Mutation>)`
(`ferrosa-storage/src/engine.rs:5042`) already provides the core guarantee:

- Preflight admission check for every target table (all-or-nothing rejection on
  overload, **before** any commit-log append).
- **Phase 1:** append every `Mutation` to the commit log.
- **Phase 2:** apply every mutation to its memtable, advance commit-log positions.
- **Phase 3:** dispatch observers.

`Mutation` (`ferrosa-storage/src/commitlog/mutation.rs`) is **already the mixed-op
carrier**: a `Row` encodes a live write, a partition/row tombstone (`DeletionTime`),
or per-cell tombstones (`value_len = -1`). A tombstone is just a `Row` with a
deletion marker — no separate type is needed. `write_atomic_batch` is already the
single-node fast path for CQL `BATCH` (`router.rs:5729`).

**Conclusion:** the primitive is ~80% present. This spec formalizes it as *the*
shared API, closes the observer atomicity/fail-loud holes, and adds a thin Bolt
transaction handle — it does not introduce a parallel mechanism.

## 3. API

A small op-level façade over `Mutation`, plus a one-shot apply and an optional
staging handle. All in `ferrosa-storage`.

```rust
/// One operation in an atomic batch. `Write` and `Tombstone` both lower to a
/// `Row` inside a `Mutation`; the enum exists so callers express intent without
/// hand-building `Row` deletion markers.
pub enum BatchOp {
    /// Upsert a row (or static row) at `key` in `keyspace.table` @ `timestamp`.
    Write {
        keyspace: String,
        table: String,
        key: DecoratedKey,
        row: Row,
        timestamp: i64,
    },
    /// Tombstone. `clustering = None` deletes the whole partition; `Some(bytes)`
    /// deletes one clustered row. Lowers to a `Row` carrying a `DeletionTime`.
    Tombstone {
        keyspace: String,
        table: String,
        key: DecoratedKey,
        clustering: Option<Vec<u8>>,
        timestamp: i64,
    },
}

impl StorageEngine {
    /// Apply a batch of mixed ops atomically and durably. The whole group is
    /// appended to the commit log and **synchronously fsynced (force_sync)
    /// before any op becomes visible**, so `Ok` implies on-disk durability even
    /// under the production-default `Periodic` sync strategy. All-or-nothing: a
    /// full preflight (table registration, admission, and per-entry size vs.
    /// segment capacity) rejects the batch before any append, so on failure
    /// **none** are applied and an `Err` is returned. Ops touching the same key
    /// are applied in `ops` order. Not read-isolated (see Guarantees §4).
    pub fn apply_batch(&self, ops: Vec<BatchOp>) -> ferrosa_common::Result<()>;

    /// Open a staging handle (Bolt explicit tx). Stages ops in memory; nothing
    /// is durable until `commit`.
    pub fn begin_batch(&self) -> BatchTxn<'_>;
}

pub struct BatchTxn<'e> { /* &engine + Vec<BatchOp> */ }

impl<'e> BatchTxn<'e> {
    pub fn stage(&mut self, op: BatchOp);          // append to pending set
    pub fn len(&self) -> usize;
    pub fn commit(self) -> ferrosa_common::Result<()>;  // == engine.apply_batch(self.ops)
    pub fn abort(self);                            // drop pending ops; no I/O
}
```

`apply_batch` lowers each `BatchOp` to a `Mutation` (coalescing ops on the same
`(keyspace, table, key)` into one `Mutation` with multiple `Row`s where cheap)
and calls the existing `write_atomic_batch` machinery. `BatchTxn::commit`
delegates to `apply_batch`. No second durability path is introduced.

### Why both a one-shot and a handle
- Accord apply and Cypher delete-cascade are **complete sets known up front** →
  `apply_batch(ops)` (one call, fail-loud).
- Bolt holds the set open across multiple `run` messages until `COMMIT` →
  needs the `begin/stage/commit/abort` handle. `abort`/`ROLLBACK` is trivially
  correct because nothing is durable until `commit`.

## 4. Guarantees

- **Atomicity (all-or-nothing, crash-atomic).** A **full preflight** runs before
  any commit-log append and rejects the entire batch on any condition that could
  otherwise make an individual append fail partway: (a) unregistered target
  table, (b) write/memtable overload admission, and (c) **any entry larger than a
  commit-log segment** (`CommitLog::max_entry_size`). Because every per-append
  failure mode is screened up front, the Phase-1 append loop cannot fail after
  appending a prefix, so no replay-durable partial batch can exist. The appended
  group is the durable commit point (see Durability); memtable apply happens only
  after the whole group is durably synced.
- **Durability (synchronous, strategy-independent).** After Phase-1 appends and
  **before** the batch becomes visible in the memtable, `write_atomic_batch`
  calls `commit_log.force_sync()` — a real fsync that propagates failures as
  `Err` (fail-loud). This makes the guarantee hold under the **production-default
  `Periodic` sync strategy**, where `append()` alone only *schedules* a
  background fsync up to `sync_interval` later: without the barrier the rows
  would become readable while still unsynced, so a crash could lose an acked,
  already-visible batch. With the barrier, `apply_batch` returns `Ok` only after
  the whole group is on disk. Recovery replays the whole group; partial/torn
  final groups are discarded by the existing commit-log framing/CRC.
- **Isolation — explicitly NOT provided (documented non-isolation contract).**
  The batch is applied to per-key memtable shards one mutation at a time with
  **no batch-level visibility barrier or snapshot**. A concurrent reader running
  between two ops of the same batch can observe a **partial** batch (op 1 visible,
  op 2 not yet). Callers needing cross-op atomic visibility must serialize their
  own reads or rely on Accord ordering. This primitive provides atomic
  *durability* (all-or-nothing on disk) and immediate post-return visibility,
  **not** read isolation / serializable concurrency control. If Phase-2 memtable
  apply fails after the durable sync, the error is surfaced (`Err`), but the
  batch is already durable and replays all-or-nothing on the next restart — the
  durable outcome stays atomic even though the live in-memory view may be
  transiently incomplete until that replay.
- **Fail-loud (X01).** Every step returns `Result`. **No** `let _ = write(...)`,
  no `continue`-on-error swallowing. Observer-derived mutations (§5.2) become
  part of the batch and surface their errors. An empty batch is `Ok(())`.
- **Idempotency.** Each `Mutation` carries a `mutation_id`; replay dedups.
  Re-applying the same Accord txn at the same timestamp is safe (required by the
  `StorageApplier` contract).
- **Ordering.** Ops apply in `ops` vector order; last writer to a cell wins by
  `(timestamp, ...)` per existing cell-merge rules.

## 5. Consumer mappings

### 5.1 Accord LWT apply — 1 op
The production `EngineStorageApplier` decodes `ApplyMutation.data` (the
serialized `(table, key, row)` the coordinator now carries instead of the
placeholder key) into a single `BatchOp::Write` **or** `BatchOp::Tombstone`
(DELETE … IF) and calls `apply_batch(vec![op])`. Returns `ApplyError` (fail-loud)
on engine `Err`; `Ok` only after the row is durably persisted. This replaces the
phantom-write stub: `[applied]=true` is returned only after `apply_batch` succeeds.

### 5.2 Cypher delete-cascade — vertex + N edges + adjacency
`DETACH DELETE v` builds one batch:
- `Tombstone` for the vertex row,
- one `Tombstone` per incident edge row (both directions),
- the **adjacency index tombstones**.

To make adjacency atomic with the edges (today's hole), `apply_batch` runs sync
observers **inside** the batch: before commit, it folds each sync observer's
`on_write`-derived mutations into the **same** commit-log group and memtable
apply, propagating their errors (no swallow). Net effect: vertex + edges +
adjacency are one crash-atomic, fail-loud unit. (Async observers stay best-effort
post-commit, as today, and are out of scope for atomicity.)

### 5.3 Bolt explicit transaction (B02) — N ops, deferred
`BEGIN` → `engine.begin_batch()` stored on the session keyed by `tx_id`;
`TransactionState.in_transaction = true`. Each `run` materializes its
statement(s) into `BatchOp`s and `stage`s them (no durable write yet).
`COMMIT` → `txn.commit()` (= `apply_batch`), fail-loud on error → Bolt `FAILURE`.
`ROLLBACK` / connection drop → `txn.abort()` (drop pending; no I/O). DDL inside a
tx stays rejected by the existing `validate_and_transition`. The same handle backs
the `ferrosa-memory` "forget" multi-row delete (D03), replacing its interim
application-level forget-journal once available.

## 6. Crash semantics

| When crash occurs | Outcome |
| --- | --- |
| Before commit-log append + force_sync completes | Batch not durable; nothing was made visible (sync precedes memtable apply). Replay applies nothing. Caller saw `Err` or no ack. All-or-nothing holds. |
| After force_sync, before/ during memtable apply | The whole group is already fsynced. Replay re-applies the **entire** group (all ops + derived observer mutations) from the log. Memtable rebuilt atomically. An acked batch is never lost. |
| Mid-`BatchTxn` (staged, not committed) | Staged ops live only in memory; lost on crash exactly like `ROLLBACK`. Nothing durable, nothing replayed. |
| Partial/torn final log group | Discarded by existing commit-log CRC/framing on recovery; never partially applied. |

## 7. Non-goals / out of scope

- No new on-disk format: reuses `Mutation` + commit-log group commit.
- No cross-partition serializable isolation or MVCC — Accord owns ordering.
- No distributed/batchlog coordination — that is the multi-node `BATCH` path
  (`coordinate_logged_batch`); this primitive is the single-node atomic substrate
  it and Accord apply both land on.
- Async-observer atomicity (they remain best-effort, post-commit).

## 8. Implementation notes (for the TDD step that follows)

1. Add `BatchOp` + lowering to `Mutation` in `ferrosa-storage`.
2. Add `apply_batch` delegating to the existing `write_atomic_batch` core; add
   `begin_batch`/`BatchTxn`.
3. **Fix the fail-loud/atomicity hole:** fold sync-observer-derived mutations
   into the batch's commit-log group + memtable apply and **propagate errors**
   (remove `let _ =` / `continue` swallowing in `dispatch_sync_observers`, or
   route delete-cascade through the new in-batch observer fold).
4. Red tests first: (a) one-op write + one-op tombstone round-trip atomic;
   (b) multi-op batch all-or-nothing on an injected mid-batch failure (assert
   **none** applied); (c) delete-cascade: vertex+edge+adjacency all gone after a
   single `apply_batch`, and a failed adjacency derivation aborts the whole batch
   (fail-loud) rather than silently dropping; (d) `BatchTxn` abort leaves nothing
   durable; commit applies all.
```
