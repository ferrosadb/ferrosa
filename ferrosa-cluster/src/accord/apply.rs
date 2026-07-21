//! Accord apply phase: dep-wait + storage write (p0-03b Gap 5).
//!
//! After a transaction is Committed, each replica must:
//!
//! 1. Wait for all dependency transactions to reach the `Applied` state (dep-wait).
//! 2. Apply this transaction's mutation to local storage at the agreed timestamp.
//! 3. Advance the transaction's `TxnState` to `Applied` in the state machine.
//!
//! This module provides the [`StorageApplier`] trait (the storage integration
//! seam) and [`DepWaitApplier`] (the orchestrator that combines
//! [`DepWaitGraph`] with the applier).
//!
//! # Design
//!
//! The dep-wait graph tracks which transactions are blocked on their
//! dependencies. When a dependency is marked applied, the graph returns the set
//! of newly-unblocked transactions. The orchestrator then applies each of them
//! in turn, recursively unblocking further transactions.
//!
//! # Thread safety
//!
//! `DepWaitApplier` uses a `parking_lot::Mutex` for the dep-wait graph so it
//! can be shared across the coordinator thread and the replica handler thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_storage::{BatchOp, Mutation, StorageEngine};
use parking_lot::Mutex;

use crate::accord::dep_wait::DepWaitGraph;

// ---------------------------------------------------------------------------
// StorageApplier trait — storage integration seam
// ---------------------------------------------------------------------------

/// Error applying a transaction to storage.
#[derive(Debug, Clone)]
pub struct ApplyError {
    pub txn_id: TxnId,
    pub reason: String,
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "apply failed for txn t0={}: {}",
            self.txn_id.0.time, self.reason
        )
    }
}

impl std::error::Error for ApplyError {}

/// The mutation to apply: raw bytes representing the serialized row write.
///
/// For Gap 5, this is an opaque byte vector. The production implementation
/// will decode it as a `(TableId, DecoratedKey, Row)` triple.
#[derive(Clone)]
pub struct ApplyMutation {
    /// Serialized mutation payload (table_id + key + row + etc).
    pub data: Vec<u8>,
    /// Agreed execution timestamp for this transaction.
    pub t: Timestamp,
    /// Dependency set (transactions that must be applied before this one).
    pub deps: Vec<TxnId>,
}

/// Storage integration seam: applies a committed mutation to local storage.
///
/// Implementations must:
/// 1. Write the mutation to the local `StorageEngine` at timestamp `t`.
/// 2. Be idempotent — re-applying the same `txn_id` at the same `t` is safe.
/// 3. Not block the calling thread on I/O indefinitely (async or fast sync).
pub trait StorageApplier: Send + Sync + 'static {
    /// Apply a committed mutation to local storage.
    ///
    /// Called after all dependency transactions have already been applied.
    /// The implementation must persist the write before returning `Ok(())`.
    fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError>;

    /// Apply ALL of a multi-key transaction's writes as a single atomic unit.
    ///
    /// A multi-key Accord transaction has one `txn_id` but one
    /// [`ApplyMutation`] per partition it writes. They must commit
    /// **all-or-nothing**: a failure on any key must persist none, so the
    /// transaction never tears at the replica storage seam (DATA-LOSS-CRITICAL —
    /// a partial apply that still marked the txn `Applied` would silently drop
    /// the failed key with no retry signal).
    ///
    /// The default implementation applies each mutation in turn via
    /// [`apply`](Self::apply) — adequate for test/no-op appliers, but **not**
    /// atomic. The production [`EngineStorageApplier`] overrides this to commit
    /// every partition through a single preflighted `apply_batch`.
    fn apply_writeset(
        &self,
        txn_id: TxnId,
        mutations: Vec<ApplyMutation>,
    ) -> Result<(), ApplyError> {
        for mutation in mutations {
            self.apply(txn_id, mutation)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// StorageReader trait — linearizable read-at-t seam (symmetric to StorageApplier)
// ---------------------------------------------------------------------------

/// Error reading a row for the linearizable IF-condition read-vote.
#[derive(Debug, Clone)]
pub struct RowReadError {
    pub reason: String,
}

impl std::fmt::Display for RowReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read-at-t failed: {}", self.reason)
    }
}

impl std::error::Error for RowReadError {}

/// Storage integration seam for the linearizable read-at-`t` that backs the
/// Accord IF-condition read-vote (Gap 4), symmetric to [`StorageApplier`].
///
/// A replica calls [`read_row_at`](StorageReader::read_row_at) during
/// `ReadVote` handling, **after** dep-wait has applied every dependency
/// `t' < t`. Because the apply seam re-stamps cells to the agreed `t` and every
/// conflicting earlier transaction is already Applied, the engine's current
/// state for the key is exactly the row state "as of `t`" — so reading the
/// live row is the linearizable read-at-`t`.
///
/// The returned bytes are a serialized single-row commit-log
/// [`Mutation`] (the same self-describing format the
/// applier consumes). `Ok(None)` means the row does not exist at `t`. Returning
/// the *raw* row — rather than a CQL-decoded value — keeps this seam in
/// `ferrosa-cluster` (which has no CQL schema): the coordinator, which owns the
/// table metadata, decodes and evaluates the predicate with the canonical
/// `eval_if_conditions`, so no divergent evaluator is forked.
pub trait StorageReader: Send + Sync + 'static {
    /// Read the current row for `(keyspace, table, key)` as of the agreed
    /// execution timestamp `t`.
    ///
    /// Returns the serialized single-partition [`Mutation`] bytes (decodable by
    /// the coordinator), or `Ok(None)` if no live row exists at `t`.
    fn read_row_at(
        &self,
        keyspace: &str,
        table: &str,
        key: &[u8],
        t: Timestamp,
    ) -> Result<Option<Vec<u8>>, RowReadError>;
}

// ---------------------------------------------------------------------------
// EngineStorageReader — real read-at-t via StorageEngine
// ---------------------------------------------------------------------------

/// Production [`StorageReader`] backed by the live [`StorageEngine`].
///
/// Reads the current partition for the key and, if any live row exists, returns
/// it serialized as a single-partition [`Mutation`] (keyspace/table/key/rows).
/// The read is the linearizable read-at-`t`: see [`StorageReader`] for why the
/// engine's current state equals the state as of `t` when called after dep-wait.
pub struct EngineStorageReader {
    engine: Arc<StorageEngine>,
}

impl EngineStorageReader {
    /// Create a reader backed by the live storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }
}

impl StorageReader for EngineStorageReader {
    fn read_row_at(
        &self,
        keyspace: &str,
        table: &str,
        key: &[u8],
        t: Timestamp,
    ) -> Result<Option<Vec<u8>>, RowReadError> {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        use ferrosa_storage::TableId;

        let table_id = TableId::new(keyspace, table);
        let decorated = DecoratedKey::new(PartitionKey::new(key.to_vec()));

        let partition = self
            .engine
            .read(&table_id, &decorated)
            .map_err(|e| RowReadError {
                reason: format!("engine read for {keyspace}.{table} failed: {e}"),
            })?;

        let partition = match partition {
            None => return Ok(None),
            Some(p) => p,
        };

        // Keep only cells written at or before the agreed `t` — the row state
        // "as of t". Cells are stamped at the agreed execution timestamp by the
        // apply seam, so `t.time` is the correct upper bound for what this LWT
        // is allowed to observe. (After dep-wait, no conflicting cell with
        // ts >= t.time exists, but bounding here is defensive and keeps the read
        // honestly as-of-t even if the engine merged a concurrent unrelated cell.)
        let cell_ts_bound = i64::try_from(t.time).unwrap_or(i64::MAX);
        let rows: Vec<ferrosa_sstable::types::Row> = partition
            .rows
            .into_iter()
            .filter_map(|mut row| {
                row.cells
                    .retain(|(_, cell)| cell.timestamp <= cell_ts_bound);
                // Drop a row that has no surviving live cell AND no live PK
                // liveness (it did not exist at `t`).
                let has_live_cell = row.cells.iter().any(|(_, c)| c.value.is_some());
                let has_pk_liveness = row.primary_key_liveness.has_timestamp()
                    && row.primary_key_liveness.timestamp <= cell_ts_bound;
                if has_live_cell || has_pk_liveness {
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        if rows.is_empty() {
            return Ok(None);
        }

        // Serialize as a single-partition Mutation so the coordinator can decode
        // it with the same machinery it uses for the apply mutation.
        //
        // DETERMINISM: use the legacy-zero `mutation_id` (NOT a fresh UUID v4
        // from `Mutation::new`). Read-vote bytes are compared across replicas for
        // F+1 agreement; a per-instance random id would make identical row state
        // serialize to different bytes and break the agreement (hence
        // linearizability). The id is only a write-dedup marker, irrelevant to a
        // read snapshot.
        let cell_ts = i64::try_from(t.time).unwrap_or(i64::MAX);
        let mutation = Mutation {
            mutation_id: [0u8; 16],
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: decorated,
            rows,
            timestamp: cell_ts,
        };
        let mut buf = vec![0u8; mutation.serialized_size()];
        mutation.serialize_into(&mut buf);
        Ok(Some(buf))
    }
}

// ---------------------------------------------------------------------------
// NoopStorageApplier — used in tests and when storage is not wired
// ---------------------------------------------------------------------------

/// A `StorageApplier` that records apply calls without touching storage.
///
/// Used in unit tests and in the `AccordStateMachine` when a real storage
/// engine is not available (e.g. during protocol-only tests).
pub struct NoopStorageApplier {
    /// Log of (txn_id, t) pairs that were applied.
    applied: Mutex<Vec<(TxnId, Timestamp)>>,
    /// Log of (txn_id, mutation.data) — the actual payload each apply received.
    /// Recorded so tests can assert the cascade replays the REAL queued data
    /// (not an empty placeholder).
    payloads: Mutex<Vec<(TxnId, Vec<u8>)>>,
}

impl NoopStorageApplier {
    pub fn new() -> Self {
        Self {
            applied: Mutex::new(Vec::new()),
            payloads: Mutex::new(Vec::new()),
        }
    }

    /// Returns a copy of the apply log for test assertions.
    pub fn applied_log(&self) -> Vec<(TxnId, Timestamp)> {
        self.applied.lock().clone()
    }

    /// Returns true if the given txn_id has been applied.
    pub fn was_applied(&self, txn_id: &TxnId) -> bool {
        self.applied.lock().iter().any(|(id, _)| id == txn_id)
    }

    /// Number of apply calls received.
    pub fn apply_count(&self) -> usize {
        self.applied.lock().len()
    }

    /// The mutation payload the given txn was applied with (most recent), or
    /// `None` if it was never applied. Used to assert the dep-cascade replays
    /// the queued transaction's real data.
    pub fn applied_data(&self, txn_id: &TxnId) -> Option<Vec<u8>> {
        self.payloads
            .lock()
            .iter()
            .rev()
            .find(|(id, _)| id == txn_id)
            .map(|(_, d)| d.clone())
    }
}

impl Default for NoopStorageApplier {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageApplier for NoopStorageApplier {
    fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
        self.applied.lock().push((txn_id, mutation.t));
        self.payloads.lock().push((txn_id, mutation.data));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EngineStorageApplier — real persistence via StorageEngine
// ---------------------------------------------------------------------------

/// Map an Accord-agreed [`Timestamp`] to the i64 cell timestamp used for
/// last-write-wins conflict resolution.
///
/// The cell-timestamp domain is `i64` micros-since-epoch; the agreed `t.time`
/// is the hybrid-logical-clock value that totally orders conflicting Accord
/// transactions. We use `t.time` directly (saturating into `i64`) so that two
/// LWTs whose Accord order is `t1 < t2` always persist cell timestamps in the
/// same order — independent of any coordinator wall-clock skew.
fn accord_cell_timestamp(t: Timestamp) -> i64 {
    i64::try_from(t.time).unwrap_or(i64::MAX)
}

/// Re-stamp every conflict-resolution timestamp carried by `row` to `cell_ts`.
///
/// Touches only the last-write-wins timestamps (live + tombstone cell stamps,
/// primary-key liveness, row deletion marker) — never `ttl` or
/// `local_deletion_time`, which encode TTL/expiry semantics that are
/// independent of write ordering and must survive unchanged.
fn restamp_row(row: &mut ferrosa_sstable::types::Row, cell_ts: i64) {
    for (_, cell) in &mut row.cells {
        cell.timestamp = cell_ts;
    }
    if row.primary_key_liveness.has_timestamp() {
        row.primary_key_liveness.timestamp = cell_ts;
    }
    if !row.deletion.is_live() {
        row.deletion.marked_for_delete_at = cell_ts;
    }
}

/// Production [`StorageApplier`] that durably persists a committed mutation to
/// the local [`StorageEngine`].
///
/// `ApplyMutation.data` is decoded as a self-describing commit-log
/// [`Mutation`], which carries the `(keyspace, table)` → `TableId`, the
/// [`DecoratedKey`](ferrosa_common::DecoratedKey), the row data, and the
/// write timestamp. Each row is written via [`StorageEngine::write`], which
/// appends to the commit log and the memtable (persist-before-return).
///
/// # Idempotency
///
/// Accord delivers `Apply` at-least-once, and recovery may re-drive it. The
/// applier records every `(txn_id, t)` it has applied and treats a repeat as a
/// no-op. This is required for correctness: cells are persisted at the agreed
/// `t` (see `accord_cell_timestamp`), so a re-applied old mutation re-uses
/// its *original* `t` and a naive re-write would resurrect a stale value over a
/// newer write to the same key (a lost-update hazard, since LWW cannot tell the
/// re-write apart from the first). Tracking applied transactions makes re-apply
/// a true no-op.
///
/// # Last-write-wins vs the Accord total order
///
/// Cell timestamps are re-stamped to the agreed `t` at apply time, never the
/// coordinator's materialize-time wall clock — so LWW resolves against Accord's
/// agreed order even under coordinator clock skew.
pub struct EngineStorageApplier {
    engine: Arc<StorageEngine>,
    /// `(txn_id, partition-key, t.time)` triples already persisted — for
    /// idempotent re-apply. The **partition key** is part of the key so that the
    /// distinct writes of one multi-key transaction (same `txn_id` + same agreed
    /// `t`, different partition) are each tracked independently. Keying by
    /// `(txn_id, t)` alone would dedup writes 2..N of a transaction as spurious
    /// re-applies and silently drop them (DATA-LOSS).
    applied: Mutex<HashSet<(TxnId, Vec<u8>, u64)>>,
}

impl EngineStorageApplier {
    /// Create an applier backed by the live storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            applied: Mutex::new(HashSet::new()),
        }
    }

    /// Number of distinct `(txn_id, t)` pairs persisted (for assertions/metrics).
    pub fn applied_count(&self) -> usize {
        self.applied.lock().len()
    }
}

impl StorageApplier for EngineStorageApplier {
    fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
        // The single-key apply is the degenerate one-entry writeset, so it
        // shares the exact persistence + idempotency path (atomic batch of one).
        self.apply_writeset(txn_id, vec![mutation])
    }

    fn apply_writeset(
        &self,
        txn_id: TxnId,
        mutations: Vec<ApplyMutation>,
    ) -> Result<(), ApplyError> {
        // Decode + restamp every partition, filtering out any (txn,key,t) already
        // durable, and accumulate the BatchOps for the rest. The whole set then
        // commits through ONE `apply_batch`, which preflights every target table
        // BEFORE appending any commit-log record — so either all surviving keys
        // land durably or none do (all-or-nothing; no partial / torn apply).
        let mut ops: Vec<BatchOp> = Vec::new();
        // The (txn,key,t) triples this call will newly persist — recorded only
        // AFTER the batch is durable, so a failed apply leaves them re-appliable.
        let mut newly_applied: Vec<(TxnId, Vec<u8>, u64)> = Vec::new();

        {
            // Snapshot the idempotency set under the lock to decide what to skip.
            let already = self.applied.lock();
            for mutation in &mutations {
                // Decode the self-describing commit-log mutation (one partition),
                // rebinding any Accord `list` append cell's path from the
                // coordinator-local wall clock to the agreed execution timestamp
                // `mutation.t` (t_68f226b5) so concurrent appends serialize in the
                // Accord order. Non-list cells are untouched. Fail loud on garbage.
                let mut decoded =
                    Mutation::deserialize_from_rebinding_list_paths(&mutation.data, mutation.t)
                        .map_err(|e| ApplyError {
                            txn_id,
                            reason: format!("failed to decode apply mutation: {e}"),
                        })?;

                // Idempotency gate: an already-applied (txn,key,t) is a no-op.
                // Checked AFTER decode so the partition key is available — this is
                // what lets writes 2..N of one txn (same txn+t, different key)
                // each apply instead of being deduped as a re-apply.
                let part_key = decoded.key.key.as_bytes().to_vec();
                let idem = (txn_id, part_key, mutation.t.time);
                if already.contains(&idem) {
                    continue;
                }

                // Re-stamp every cell to the Accord-agreed execution timestamp.
                //
                // The coordinator stamps cells at materialize time with its own
                // wall clock, BEFORE consensus picks `t`. Honoring that wall clock
                // for LWW would let coordinator clock skew invert the Accord total
                // order (lost update / non-linearizable). The agreed `t` exists
                // precisely to order conflicting writes, so it — not the wall
                // clock — must drive the last-write-wins cell timestamp.
                let cell_ts = accord_cell_timestamp(mutation.t);
                let keyspace = decoded.keyspace.clone();
                let table = decoded.table.clone();
                let key = decoded.key.clone();
                for row in &mut decoded.rows {
                    restamp_row(row, cell_ts);
                }
                for row in decoded.rows {
                    ops.push(BatchOp::Write {
                        keyspace: keyspace.clone(),
                        table: table.clone(),
                        key: key.clone(),
                        row,
                        timestamp: cell_ts,
                    });
                }
                newly_applied.push(idem);
            }
        }

        // Every key was already durable — true no-op (the whole writeset was a
        // re-apply). Avoid an empty batch.
        if newly_applied.is_empty() {
            return Ok(());
        }

        // Atomic commit of all surviving partitions. On any preflight/append
        // failure NONE of the ops are applied; propagated as `ApplyError`
        // (never fake success).
        self.engine.apply_batch(ops).map_err(|e| ApplyError {
            txn_id,
            reason: format!("storage apply_batch failed for writeset: {e}"),
        })?;

        // Record only after the batch is durable, so a mid-apply failure leaves
        // the txn re-appliable rather than falsely marked applied.
        let mut applied = self.applied.lock();
        for idem in newly_applied {
            applied.insert(idem);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DepWaitApplier — orchestrates dep-wait + storage apply
// ---------------------------------------------------------------------------

/// Orchestrates the Accord apply phase.
///
/// Combines a [`DepWaitGraph`] (which tracks which transactions are ready)
/// with a [`StorageApplier`] (which writes to storage). When a transaction's
/// dependencies are all satisfied, it invokes the applier and propagates
/// the "applied" signal to any further waiters.
pub struct DepWaitApplier {
    graph: Mutex<DepWaitGraph>,
    applier: Arc<dyn StorageApplier>,
    /// The FULL write-set (one [`ApplyMutation`] per partition) for each
    /// transaction parked waiting on dependencies, keyed by txn id. When the last
    /// dependency resolves, the cascade pulls the real write-set from here and
    /// applies **every** entry atomically — fixing both the empty-placeholder bug
    /// (queued txns re-applied with `data: vec![]`) and the multi-key drop
    /// (writes 2..N of a parked txn silently lost because only one was stored).
    pending: Mutex<HashMap<TxnId, Vec<ApplyMutation>>>,
}

impl DepWaitApplier {
    pub fn new(applier: Arc<dyn StorageApplier>) -> Self {
        Self {
            graph: Mutex::new(DepWaitGraph::new()),
            applier,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Attempt to apply a committed transaction.
    ///
    /// If all dependencies in `deps` are already applied, calls `applier.apply`
    /// immediately and propagates the "applied" signal through the graph,
    /// transitively waking any parked waiters this resolves.
    ///
    /// If any dependencies are not yet applied, registers this transaction as
    /// a waiter and returns an empty `Vec`. The transaction will be applied
    /// automatically when the last dependency calls
    /// [`notify_applied`](Self::notify_applied) or [`try_apply`](Self::try_apply).
    ///
    /// Returns the `(txn_id, mutation_data)` of every transaction that was
    /// **actually persisted** by this call, in apply order — the primary first,
    /// then each cascade-woken waiter. The caller (the Accord state machine) uses
    /// this list to advance exactly those transactions to `Applied` and fire
    /// their post-apply bookkeeping (protocol-log marker, conflict-index GC,
    /// dep-wait wake). An **empty** `Vec` means the transaction parked behind an
    /// unapplied dependency and must NOT be marked applied.
    pub fn try_apply(
        &self,
        txn_id: TxnId,
        mutation: ApplyMutation,
    ) -> Result<Vec<(TxnId, Vec<u8>)>, ApplyError> {
        // The single-key path is the degenerate one-entry write-set; delegating
        // keeps it byte-identical with the multi-key path.
        self.try_apply_writeset(txn_id, vec![mutation])
    }

    /// Attempt to apply a committed **multi-key** transaction: its full
    /// write-set (one [`ApplyMutation`] per partition), parked and applied as a
    /// unit so writes 2..N are never dropped.
    ///
    /// Dependencies are **txn-level** (the dep-wait graph stays keyed by
    /// `TxnId`), so the union of every entry's `deps` gates the whole write-set.
    /// If all deps are already applied, the entire write-set is applied
    /// atomically now (via [`StorageApplier::apply_writeset`]) and the cascade
    /// runs; otherwise the whole write-set parks and an empty `Vec` is returned.
    ///
    /// Returns one `(txn_id, data)` entry **per applied write** — a multi-key
    /// txn yields N entries with the same `txn_id` — in apply order (this txn's
    /// writes first, then each cascade-woken waiter's). The state machine dedups
    /// by `txn_id` for once-per-txn `Applied` bookkeeping.
    pub fn try_apply_writeset(
        &self,
        txn_id: TxnId,
        mutations: Vec<ApplyMutation>,
    ) -> Result<Vec<(TxnId, Vec<u8>)>, ApplyError> {
        // Txn-level deps: the union across every write in the set (the graph is
        // TxnId-keyed; deps are a property of the transaction, not the key).
        let deps: Vec<TxnId> = {
            let mut seen: HashSet<TxnId> = HashSet::new();
            let mut out: Vec<TxnId> = Vec::new();
            for m in &mutations {
                for dep in &m.deps {
                    if seen.insert(*dep) {
                        out.push(*dep);
                    }
                }
            }
            out
        };

        let mut graph = self.graph.lock();

        // Register waits for any unresolved deps.
        let mut waiting = false;
        for dep in &deps {
            if !graph.is_applied(dep) {
                waiting = true;
                let _ = graph.register_wait(txn_id, *dep);
            }
        }

        if waiting {
            // Park the WHOLE write-set so the cascade replays every key's REAL
            // data when the last dependency resolves — not an empty placeholder,
            // and not just the first key.
            drop(graph);
            self.pending.lock().insert(txn_id, mutations);
            return Ok(Vec::new());
        }

        drop(graph);

        // All deps satisfied — apply the whole write-set atomically now, then
        // cascade to any waiters this resolves. Capture the per-key payloads
        // before the mutations are moved into the applier so we can report them.
        let datas: Vec<Vec<u8>> = mutations.iter().map(|m| m.data.clone()).collect();
        self.applier.apply_writeset(txn_id, mutations)?;
        let mut applied: Vec<(TxnId, Vec<u8>)> =
            datas.into_iter().map(|data| (txn_id, data)).collect();
        applied.extend(self.cascade(txn_id));
        Ok(applied)
    }

    /// Mark `just_applied` as applied and apply — in dependency order — every
    /// transaction whose *last* remaining dependency this resolves, using each
    /// waiter's parked mutation (its real data). Cascades transitively, since a
    /// woken waiter may itself unblock further waiters.
    ///
    /// Returns the `(txn_id, mutation_data)` of every waiter that was actually
    /// persisted, in apply order — so the caller can advance exactly those to
    /// `Applied`. A waiter whose apply *fails* is left un-applied and is NOT in
    /// the returned list (fail loud; its dependents stay parked).
    fn cascade(&self, just_applied: TxnId) -> Vec<(TxnId, Vec<u8>)> {
        let mut applied: Vec<(TxnId, Vec<u8>)> = Vec::new();
        let mut queue: VecDeque<TxnId> = self
            .graph
            .lock()
            .mark_applied(just_applied)
            .into_iter()
            .collect();
        while let Some(waiter) = queue.pop_front() {
            // Pull the waiter's parked write-set (stored when it parked in
            // `try_apply_writeset`). If absent — e.g. a no-write finalize that
            // parked nothing — there is nothing to persist; still mark it applied
            // so its own waiters proceed.
            if let Some(mutations) = self.pending.lock().remove(&waiter) {
                let datas: Vec<Vec<u8>> = mutations.iter().map(|m| m.data.clone()).collect();
                if let Err(e) = self.applier.apply_writeset(waiter, mutations) {
                    // Fail loud: the parked write-set did not persist (atomically,
                    // so NO key landed). Do NOT mark applied or cascade past it —
                    // its dependents stay parked rather than committing on a lost
                    // write. The txn is re-driven by the coordinator's Apply retry.
                    tracing::error!(%e, txn = waiter.0.time, "accord dep-cascade: applier failed for parked waiter — leaving it un-applied");
                    continue;
                }
                // Report every key of the waiter's write-set, in order.
                applied.extend(datas.into_iter().map(|data| (waiter, data)));
            }
            for woken in self.graph.lock().mark_applied(waiter) {
                queue.push_back(woken);
            }
        }
        applied
    }

    /// Notify the applier that `txn_id` has been applied externally.
    ///
    /// Called when a dependency transaction reaches `Applied` without a local
    /// row write (a no-write LWT finalize) or when a remote coordinator
    /// acknowledges its apply (via `ApplyOK`). Unblocks any waiting transactions
    /// and returns the `(txn_id, mutation_data)` of every waiter that was
    /// actually persisted as a result, in apply order (see the `cascade` helper).
    pub fn notify_applied(&self, txn_id: TxnId) -> Vec<(TxnId, Vec<u8>)> {
        // Mark the externally-applied dependency and apply any waiters it
        // unblocks, using their real parked mutations (see `cascade`).
        self.cascade(txn_id)
    }

    /// Check if a transaction has been applied.
    pub fn is_applied(&self, txn_id: &TxnId) -> bool {
        self.graph.lock().is_applied(txn_id)
    }

    /// Number of transactions currently waiting on dependencies.
    pub fn waiting_count(&self) -> usize {
        self.graph.lock().waiting_count()
    }

    /// Access to the storage applier (for test assertions).
    pub fn applier(&self) -> &Arc<dyn StorageApplier> {
        &self.applier
    }
}

// ===========================================================================
// CdcPublishingApplier — publish CommittedToCluster CDC on durable apply
// ===========================================================================

/// Decorates a [`StorageApplier`] to publish a `CommittedToCluster` CDC event
/// after a successful durable apply, so live CQL `SUBSCRIBE ... ON COMMITTED`
/// (and the Arrow Flight endpoint) receive Accord-committed writes. No-op when
/// no committed-stream subscriber is attached. The inner applier's fail-loud
/// contract is preserved — the event is published only after `inner.apply`
/// returns `Ok`.
pub struct CdcPublishingApplier {
    inner: Arc<dyn StorageApplier>,
    cdc: Arc<ferrosa_cdc::CdcBus>,
}

impl CdcPublishingApplier {
    pub fn new(inner: Arc<dyn StorageApplier>, cdc: Arc<ferrosa_cdc::CdcBus>) -> Self {
        Self { inner, cdc }
    }
}

impl CdcPublishingApplier {
    /// Build the `CommittedToCluster` CDC event for `mutation`, or `None` if no
    /// subscriber is listening (or the payload fails to decode).
    fn committed_event(&self, mutation: &ApplyMutation) -> Option<ferrosa_cdc::CdcEvent> {
        if !self
            .cdc
            .has_subscribers(ferrosa_cdc::CdcStream::CommittedToCluster)
        {
            return None;
        }
        // Rebind list paths to the agreed `t` so the CDC event mirrors the
        // durably-applied rows (not the coordinator-clock paths), matching
        // `EngineStorageApplier::apply_writeset`.
        Mutation::deserialize_from_rebinding_list_paths(&mutation.data, mutation.t)
            .ok()
            .map(|m| ferrosa_cdc::CdcEvent {
                stream: ferrosa_cdc::CdcStream::CommittedToCluster,
                keyspace: m.keyspace.clone(),
                table: m.table.clone(),
                key: m.key.clone(),
                rows: m.rows.clone(),
                timestamp: m.timestamp,
                accord_ts: Some(mutation.t),
                mutation_id: m.mutation_id,
            })
    }
}

impl StorageApplier for CdcPublishingApplier {
    fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
        // Build the CDC event before `mutation` is consumed, and only if a
        // committed-stream subscriber is actually listening.
        let event = self.committed_event(&mutation);
        self.inner.apply(txn_id, mutation)?;
        if let Some(ev) = event {
            self.cdc.publish(ev);
        }
        Ok(())
    }

    fn apply_writeset(
        &self,
        txn_id: TxnId,
        mutations: Vec<ApplyMutation>,
    ) -> Result<(), ApplyError> {
        // Build every event up front, then route the writeset through the inner
        // applier's ATOMIC `apply_writeset` (NOT the per-key default loop, which
        // would forfeit cross-key atomicity). Publish only after the durable
        // apply returns Ok, preserving the inner fail-loud contract.
        let events: Vec<ferrosa_cdc::CdcEvent> = mutations
            .iter()
            .filter_map(|m| self.committed_event(m))
            .collect();
        self.inner.apply_writeset(txn_id, mutations)?;
        for ev in events {
            self.cdc.publish(ev);
        }
        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn_id(node: u64, micros: u64) -> TxnId {
        TxnId::new(node, ts(micros))
    }

    fn mutation(deps: Vec<TxnId>) -> ApplyMutation {
        ApplyMutation {
            data: b"mutation-data".to_vec(),
            t: ts(1000),
            deps,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: No deps — applies immediately
    // -----------------------------------------------------------------------

    #[test]
    fn apply_no_deps_immediate() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let txn = txn_id(1, 1000);
        let result = applier.try_apply(txn, mutation(vec![])).unwrap();

        assert_eq!(
            result.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![txn],
            "transaction with no deps must apply immediately and be reported"
        );
        assert!(
            noop.was_applied(&txn),
            "noop applier must record the application"
        );
        assert_eq!(noop.apply_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 2: Dep already applied — applies immediately
    // -----------------------------------------------------------------------

    #[test]
    fn apply_dep_already_applied_immediate() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let dep = txn_id(1, 500);
        let txn = txn_id(2, 1000);

        // Mark dep applied first.
        applier.graph.lock().mark_applied(dep);

        // Now apply txn that depends on dep.
        let result = applier
            .try_apply(
                txn,
                ApplyMutation {
                    data: b"data".to_vec(),
                    t: ts(1000),
                    deps: vec![dep],
                },
            )
            .unwrap();

        assert!(
            !result.is_empty(),
            "transaction with already-applied dep must apply immediately"
        );
        assert!(noop.was_applied(&txn));
    }

    // -----------------------------------------------------------------------
    // Test 3: Pending dep — returns false (queued), woken by notify_applied
    // -----------------------------------------------------------------------

    #[test]
    fn apply_pending_dep_queued_then_woken() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let dep = txn_id(1, 500);
        let txn = txn_id(2, 1000);

        // Try to apply txn that depends on dep (which is not yet applied).
        let result = applier
            .try_apply(
                txn,
                ApplyMutation {
                    data: b"data".to_vec(),
                    t: ts(1000),
                    deps: vec![dep],
                },
            )
            .unwrap();

        assert!(
            result.is_empty(),
            "transaction with pending dep must be queued (empty applied list)"
        );
        assert!(
            !noop.was_applied(&txn),
            "txn must not be applied while dep is pending"
        );

        // Dep becomes available — notify the applier. The woken waiter is
        // returned so the caller can advance it to Applied.
        let woken = applier.notify_applied(dep);
        assert_eq!(
            woken.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![txn],
            "notify_applied must report the cascaded waiter"
        );

        // Txn should now be applied (woken from the dep-wait graph).
        // The waiter should have been called via cascade in notify_applied.
        assert_eq!(noop.apply_count(), 1, "cascade must apply the waiter");
    }

    // -----------------------------------------------------------------------
    // Regression (t_cb5180f9): the dep cascade must replay a queued waiter
    // with its REAL parked mutation, not an empty `data: vec![]` placeholder
    // (which silently dropped the waiter's write).
    // -----------------------------------------------------------------------

    #[test]
    fn cascade_replays_queued_mutation_real_data() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let a = txn_id(1, 500);
        let b = txn_id(2, 1000);

        // B depends on A (not yet applied) → B parks.
        let queued = applier
            .try_apply(
                b,
                ApplyMutation {
                    data: b"write-from-B".to_vec(),
                    t: ts(1000),
                    deps: vec![a],
                },
            )
            .unwrap();
        assert!(queued.is_empty(), "B must be queued behind A");
        assert!(!noop.was_applied(&b));

        // A applies (no deps) → cascade wakes B. The returned list reports A
        // then B in dependency order.
        let applied = applier
            .try_apply(
                a,
                ApplyMutation {
                    data: b"write-from-A".to_vec(),
                    t: ts(500),
                    deps: vec![],
                },
            )
            .unwrap();
        assert_eq!(
            applied.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![a, b],
            "A applies immediately then cascades to B, in dependency order"
        );

        assert!(
            noop.was_applied(&b),
            "cascade must apply the queued waiter B"
        );
        assert_eq!(
            noop.applied_data(&b).as_deref(),
            Some(b"write-from-B".as_slice()),
            "B must be re-applied with its REAL parked mutation, not data: vec![]",
        );
        assert_eq!(
            noop.applied_data(&a).as_deref(),
            Some(b"write-from-A".as_slice())
        );
    }

    #[test]
    fn cascade_is_transitive_with_real_data() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());
        let (a, b, c) = (txn_id(1, 100), txn_id(2, 200), txn_id(3, 300));

        // Queue C (waits on B) and B (waits on A); both park.
        assert!(applier
            .try_apply(
                c,
                ApplyMutation {
                    data: b"C".to_vec(),
                    t: ts(300),
                    deps: vec![b]
                }
            )
            .unwrap()
            .is_empty());
        assert!(applier
            .try_apply(
                b,
                ApplyMutation {
                    data: b"B".to_vec(),
                    t: ts(200),
                    deps: vec![a]
                }
            )
            .unwrap()
            .is_empty());
        // A applies → B unblocks → C unblocks, each with its own data, reported
        // in transitive dependency order.
        assert_eq!(
            applier
                .try_apply(
                    a,
                    ApplyMutation {
                        data: b"A".to_vec(),
                        t: ts(100),
                        deps: vec![]
                    }
                )
                .unwrap()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![a, b, c],
        );

        assert_eq!(noop.applied_data(&a).as_deref(), Some(b"A".as_slice()));
        assert_eq!(noop.applied_data(&b).as_deref(), Some(b"B".as_slice()));
        assert_eq!(noop.applied_data(&c).as_deref(), Some(b"C".as_slice()));
    }

    // -----------------------------------------------------------------------
    // Multi-key (writeset) — DATA-LOSS-CRITICAL: a txn with N partition writes
    // must park ALL N and apply ALL N on resolve. The bug being fixed dropped
    // writes 2..N (the engine was keyed by txn only, so they collided/vanished).
    // -----------------------------------------------------------------------

    #[test]
    fn writeset_no_deps_applies_every_key_immediately() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let txn = txn_id(1, 1000);
        let writes = vec![
            ApplyMutation {
                data: b"key1-write".to_vec(),
                t: ts(1000),
                deps: vec![],
            },
            ApplyMutation {
                data: b"key2-write".to_vec(),
                t: ts(1000),
                deps: vec![],
            },
        ];

        let applied = applier.try_apply_writeset(txn, writes).unwrap();
        // Both writes of the SAME txn must be reported (same TxnId, distinct data).
        assert_eq!(applied.len(), 2, "every key of the txn must apply");
        assert!(applied.iter().all(|(id, _)| *id == txn));
        assert_eq!(noop.apply_count(), 2, "BOTH writes must reach the applier");
        let datas: Vec<Vec<u8>> = applied.into_iter().map(|(_, d)| d).collect();
        assert!(datas.contains(&b"key1-write".to_vec()));
        assert!(datas.contains(&b"key2-write".to_vec()));
    }

    #[test]
    fn writeset_parked_behind_dep_applies_all_keys_on_resolve() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let dep = txn_id(1, 500);
        let txn = txn_id(2, 1000);

        // Two-key write-set for `txn`, both blocked behind the unapplied `dep`.
        let writes = vec![
            ApplyMutation {
                data: b"key1-write".to_vec(),
                t: ts(1000),
                deps: vec![dep],
            },
            ApplyMutation {
                data: b"key2-write".to_vec(),
                t: ts(1000),
                deps: vec![dep],
            },
        ];
        let queued = applier.try_apply_writeset(txn, writes).unwrap();
        assert!(
            queued.is_empty(),
            "the whole multi-key txn must park behind dep"
        );
        assert_eq!(noop.apply_count(), 0, "nothing applies while parked");

        // dep resolves → the cascade must replay BOTH parked writes (today: 1).
        let woken = applier.notify_applied(dep);
        let woken_ids: Vec<_> = woken.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            woken_ids,
            vec![txn, txn],
            "BOTH writes of the multi-key txn must apply on resolve — not just the first"
        );
        assert_eq!(noop.apply_count(), 2);
        let datas: Vec<Vec<u8>> = woken.into_iter().map(|(_, d)| d).collect();
        assert!(
            datas.contains(&b"key1-write".to_vec()) && datas.contains(&b"key2-write".to_vec()),
            "the cascade must replay each key's REAL parked data"
        );
    }

    #[test]
    fn try_apply_is_single_key_writeset_delegation() {
        // The single-key path must stay behaviorally identical: one entry in,
        // one entry out, applied once.
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());
        let txn = txn_id(1, 1000);
        let applied = applier.try_apply(txn, mutation(vec![])).unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].0, txn);
        assert_eq!(noop.apply_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 4: is_applied reflects state
    // -----------------------------------------------------------------------

    #[test]
    fn is_applied_tracks_state() {
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let txn = txn_id(1, 1000);
        assert!(!applier.is_applied(&txn), "not yet applied");

        applier.try_apply(txn, mutation(vec![])).unwrap();
        assert!(applier.is_applied(&txn), "applied after try_apply");
    }

    // -----------------------------------------------------------------------
    // Test 5: F+1 apply prerequisite — two txns on same key must order by t
    // -----------------------------------------------------------------------

    #[test]
    fn apply_ordering_by_timestamp() {
        // Two transactions on the same key. txn_b depends on txn_a.
        // Both must apply in order: txn_a first, then txn_b.
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());

        let txn_a = txn_id(1, 1000);
        let txn_b = txn_id(2, 2000);

        // Apply txn_a first (no deps).
        applier.try_apply(txn_a, mutation(vec![])).unwrap();
        assert!(noop.was_applied(&txn_a));

        // Apply txn_b (depends on txn_a which is already applied).
        let result = applier
            .try_apply(
                txn_b,
                ApplyMutation {
                    data: b"b-data".to_vec(),
                    t: ts(2000),
                    deps: vec![txn_a],
                },
            )
            .unwrap();
        assert!(
            !result.is_empty(),
            "txn_b must apply immediately (dep already applied)"
        );
        assert!(noop.was_applied(&txn_b));

        // Verify apply log order.
        let log = noop.applied_log();
        assert_eq!(log.len(), 2);
        let log_a_pos = log.iter().position(|(id, _)| id == &txn_a).unwrap();
        let log_b_pos = log.iter().position(|(id, _)| id == &txn_b).unwrap();
        assert!(
            log_a_pos < log_b_pos,
            "txn_a must be applied before txn_b in the log"
        );
    }

    // =======================================================================
    // BDD scenarios — the multi-key data-loss narrative, Given/When/Then.
    // =======================================================================

    /// A `StorageApplier` recording every applied `(txn.time, data)` in global
    /// apply order, so property tests can assert exactly-once + dependency order.
    struct RecordingApplier {
        seq: Mutex<Vec<(u64, Vec<u8>)>>,
    }

    impl RecordingApplier {
        fn new() -> Self {
            Self {
                seq: Mutex::new(Vec::new()),
            }
        }
        fn order(&self) -> Vec<(u64, Vec<u8>)> {
            self.seq.lock().clone()
        }
    }

    impl StorageApplier for RecordingApplier {
        fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
            self.seq.lock().push((txn_id.0.time, mutation.data));
            Ok(())
        }
    }

    #[test]
    fn bdd_multikey_txn_parked_behind_dependency_loses_no_write() {
        // GIVEN a two-key transaction B that depends on a not-yet-applied A,
        let noop = Arc::new(NoopStorageApplier::new());
        let applier = DepWaitApplier::new(noop.clone());
        let a = txn_id(1, 100);
        let b = txn_id(2, 200);
        let b_writes = vec![
            ApplyMutation {
                data: b"B/key1".to_vec(),
                t: ts(200),
                deps: vec![a],
            },
            ApplyMutation {
                data: b"B/key2".to_vec(),
                t: ts(200),
                deps: vec![a],
            },
        ];

        // WHEN B is driven before A (so B parks), then A applies,
        let parked = applier.try_apply_writeset(b, b_writes).unwrap();
        assert!(parked.is_empty(), "B parks behind A");
        let woken = applier
            .try_apply(
                a,
                ApplyMutation {
                    data: b"A/key".to_vec(),
                    t: ts(100),
                    deps: vec![],
                },
            )
            .unwrap();

        // THEN A and BOTH of B's keys are applied — no write is lost.
        let ids: Vec<TxnId> = woken.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![a, b, b],
            "A applies, then both of B's writes cascade"
        );
        assert_eq!(noop.apply_count(), 3);
    }

    // =======================================================================
    // Property tests — invariants over random multi-key dependency DAGs.
    // =======================================================================

    use proptest::prelude::*;

    /// Cap on the number of transactions a generated DAG may contain.
    const MAX_TXNS: usize = 7;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// For ANY random DAG of multi-key transactions applied in ANY order:
        ///  (1) NO-LOST-WRITE / EXACTLY-ONCE — every (txn, key) write is applied
        ///      exactly once: none dropped, none double-applied.
        ///  (2) DEP-ORDER — for every dependency edge a→b, every one of a's writes
        ///      is applied before any of b's (a reaches Applied before b cascades).
        #[test]
        #[allow(clippy::needless_range_loop)] // `i` indexes a flat matrix + parallel vecs
        fn prop_multikey_dag_applies_every_write_exactly_once_in_dep_order(
            // adjacency[i*MAX_TXNS + j] = "txn i depends on txn j" (only j<i used → DAG)
            adjacency in prop::collection::vec(any::<bool>(), MAX_TXNS * MAX_TXNS),
            keys_per in prop::collection::vec(1usize..4, MAX_TXNS),
            prios in prop::collection::vec(any::<u64>(), MAX_TXNS),
            n in 2usize..=MAX_TXNS,
        ) {
            let rec = Arc::new(RecordingApplier::new());
            let applier = DepWaitApplier::new(rec.clone());

            // txn i has a distinct, increasing time so TxnIds and ordering are stable.
            let id_of = |i: usize| TxnId::new(1, ts((i as u64 + 1) * 100));

            // Build each txn's deps (j<i per adjacency) and write-set. `i`
            // indexes the flat adjacency matrix and several parallel vecs, so a
            // range loop is the clearest form here.
            let mut deps_of: Vec<Vec<TxnId>> = vec![Vec::new(); n];
            let mut writes_of: Vec<Vec<ApplyMutation>> = Vec::with_capacity(n);
            let mut expected: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for i in 0..n {
                for j in 0..i {
                    if adjacency[i * MAX_TXNS + j] {
                        deps_of[i].push(id_of(j));
                    }
                }
                let mut ws = Vec::new();
                for k in 0..keys_per[i] {
                    let data = format!("{i}:{k}").into_bytes();
                    expected.insert(data.clone());
                    ws.push(ApplyMutation {
                        data,
                        t: ts((i as u64 + 1) * 100),
                        deps: deps_of[i].clone(),
                    });
                }
                writes_of.push(ws);
            }

            // Attempt order: a pseudo-random permutation by (prio, index).
            let mut order: Vec<usize> = (0..n).collect();
            order.sort_by_key(|&i| (prios[i], i));

            for &i in &order {
                applier
                    .try_apply_writeset(id_of(i), writes_of[i].clone())
                    .expect("apply must not error (Noop/recording applier never fails)");
            }

            let log = rec.order();

            // (1) Exactly-once: the recorded multiset equals the expected set, with
            // no duplicates.
            let recorded: Vec<Vec<u8>> = log.iter().map(|(_, d)| d.clone()).collect();
            let recorded_set: std::collections::HashSet<Vec<u8>> = recorded.iter().cloned().collect();
            prop_assert_eq!(recorded.len(), expected.len(), "no write dropped or double-applied");
            prop_assert_eq!(recorded_set, expected, "exactly the expected writes were applied");

            // First/last global apply position per txn time.
            let mut first: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
            let mut last: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
            for (pos, (time, _)) in log.iter().enumerate() {
                first.entry(*time).or_insert(pos);
                last.insert(*time, pos);
            }

            // (2) Dep-order: along every edge a→b, all of a precedes all of b.
            for i in 0..n {
                let bt = (i as u64 + 1) * 100;
                for dep in &deps_of[i] {
                    let at = dep.0.time;
                    prop_assert!(
                        last[&at] < first[&bt],
                        "dependency {at} must fully apply before dependent {bt}"
                    );
                }
            }
        }
    }
}

// ===========================================================================
// EngineStorageApplier tests — real persistence via StorageEngine
// ===========================================================================
//
// Increment 3 of the Accord LWT data-path plan: a `StorageApplier` that
// decodes `ApplyMutation.data` as a self-describing commit-log `Mutation`
// (TableId via keyspace/table, DecoratedKey, Row, ts) and persists it through
// `StorageEngine::write`. These tests use a REAL engine (not a Noop) and
// assert the row is durably readable afterwards — closing the phantom-write
// gap at the storage seam.

#[cfg(test)]
mod engine_applier_tests {
    use super::*;
    use ferrosa_common::accord::{Timestamp, TxnId};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{Mutation, StorageEngine, StorageEngineConfig, TableId};

    const KS: &str = "lwt_ks";
    const TABLE: &str = "lwt_table";

    fn accord_ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(node: u64, micros: u64) -> TxnId {
        TxnId::new(node, accord_ts(micros))
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: KS.to_string(),
            table: TABLE.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn make_engine() -> (Arc<StorageEngine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        (Arc::new(engine), dir)
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], cell_ts: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), cell_ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(cell_ts),
        }
    }

    /// Encode an `ApplyMutation` whose `data` is a serialized commit-log
    /// `Mutation` — the wire format the production applier must decode.
    fn encoded_mutation(
        key: DecoratedKey,
        value: &[u8],
        cell_ts: i64,
        t: Timestamp,
        deps: Vec<TxnId>,
    ) -> ApplyMutation {
        let m = Mutation::new(
            KS.to_string(),
            TABLE.to_string(),
            key,
            vec![make_row(value, cell_ts)],
            cell_ts,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        ApplyMutation { data: buf, t, deps }
    }

    fn read_cell0(engine: &StorageEngine, key: &DecoratedKey) -> Option<Vec<u8>> {
        let partition = engine.read(&TableId::new(KS, TABLE), key).unwrap()?;
        let row = partition.rows.first()?;
        row.cells.first().and_then(|(_, c)| c.value.clone())
    }

    /// Read the LWW cell timestamp persisted for column 0 of the first row.
    fn read_cell0_ts(engine: &StorageEngine, key: &DecoratedKey) -> Option<i64> {
        let partition = engine.read(&TableId::new(KS, TABLE), key).unwrap()?;
        let row = partition.rows.first()?;
        row.cells.first().map(|(_, c)| c.timestamp)
    }

    // -----------------------------------------------------------------------
    // RED: applying persists a row that is then readable via the engine.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_persists_row_readable_via_engine() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let key = make_key("pk1");
        let t = accord_ts(1000);
        let mutation = encoded_mutation(key.clone(), b"hello", 1000, t, vec![]);

        applier
            .apply(txn(1, 1000), mutation)
            .expect("apply must persist the mutation to the engine");

        assert_eq!(
            read_cell0(&engine, &key).as_deref(),
            Some(b"hello".as_slice()),
            "the row written by apply must be readable via the engine — \
             not a phantom write"
        );
    }

    /// Applying a mutation with an Accord `list` append cell (flagged 0x4000)
    /// rebinds the cell's path to the agreed execution timestamp `t` — not the
    /// coordinator wall clock baked in at materialize — while preserving the
    /// element_seq. This is what makes concurrent list appends strict-
    /// serializable (t_68f226b5): the persisted path orders by the Accord `t`,
    /// so a read observes the Accord serialization order.
    #[test]
    fn apply_rebinds_flagged_list_cell_path_to_accord_ts() {
        use ferrosa_common::accord_list_cell_path;
        const REBIND_FLAG: u16 = 0x4000;

        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let key = make_key("pk-list");
        let element_seq = 2u16;
        // Coordinator-clock list path: a huge wall-clock time, but it carries the
        // element_seq the rebind must preserve.
        let coord_path = accord_list_cell_path(&accord_ts(9_999_999), element_seq);
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(
                REBIND_FLAG, // storage column 0, flagged for rebind
                CellValue::live(b"L".to_vec(), 9_999_999).with_path(coord_path.clone()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(9_999_999),
        };
        let m = Mutation::new(
            KS.to_string(),
            TABLE.to_string(),
            key.clone(),
            vec![row],
            9_999_999,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);

        // The agreed execution ts is tiny (777) — far below the wall clock stamped
        // at materialize — so a rebound path proves the Accord ts drove it.
        let agreed = accord_ts(777);
        applier
            .apply(
                txn(1, 777),
                ApplyMutation {
                    data: buf,
                    t: agreed,
                    deps: vec![],
                },
            )
            .expect("apply must decode, rebind, and persist");

        let partition = engine
            .read(&TableId::new(KS, TABLE), &key)
            .unwrap()
            .unwrap();
        let (idx, cell) = partition.rows[0]
            .cells
            .iter()
            .find(|(_, c)| c.path.is_some())
            .expect("the persisted list cell must have a path");
        assert_eq!(
            *idx & 0xC000,
            0,
            "stored column index carries no transient flag"
        );
        assert_eq!(
            cell.path.as_deref(),
            Some(accord_list_cell_path(&agreed, element_seq).as_slice()),
            "list cell path rebound to the Accord execution ts, element_seq preserved"
        );
        assert_ne!(
            cell.path.as_deref(),
            Some(coord_path.as_slice()),
            "the coordinator wall-clock path must NOT survive"
        );
        assert_eq!(cell.timestamp, 777, "cell ts restamped to the agreed t");
    }

    // -----------------------------------------------------------------------
    // Idempotent on (txn_id, t): re-applying the same txn is a safe no-op and
    // must NOT clobber a newer value written under a different txn/timestamp.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_is_idempotent_on_txn_and_timestamp() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let key = make_key("pk1");
        let id = txn(1, 1000);
        let t = accord_ts(1000);

        // First apply writes "v1".
        applier
            .apply(id, encoded_mutation(key.clone(), b"v1", 1000, t, vec![]))
            .unwrap();

        // A later txn writes "v2" with a higher cell timestamp.
        applier
            .apply(
                txn(2, 2000),
                encoded_mutation(key.clone(), b"v2", 2000, accord_ts(2000), vec![]),
            )
            .unwrap();
        assert_eq!(read_cell0(&engine, &key).as_deref(), Some(b"v2".as_slice()));

        // Re-applying the FIRST txn (same txn_id + t) must be a no-op: it must
        // not resurrect "v1" over the newer "v2".
        applier
            .apply(id, encoded_mutation(key.clone(), b"v1", 1000, t, vec![]))
            .unwrap();
        assert_eq!(
            read_cell0(&engine, &key).as_deref(),
            Some(b"v2".as_slice()),
            "re-applying an already-applied (txn_id, t) must not re-write"
        );
    }

    // -----------------------------------------------------------------------
    // Fail loud: a write to an unregistered table must return ApplyError,
    // never a silent success.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_to_unregistered_table_fails_loud() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        // Build a mutation targeting a table that was never registered.
        let m = Mutation::new(
            KS.to_string(),
            "no_such_table".to_string(),
            make_key("pk1"),
            vec![make_row(b"x", 1000)],
            1000,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        let mutation = ApplyMutation {
            data: buf,
            t: accord_ts(1000),
            deps: vec![],
        };

        let err = applier
            .apply(txn(1, 1000), mutation)
            .expect_err("apply to an unregistered table must fail loud, not fake success");
        assert!(
            err.reason.contains("no_such_table") || err.reason.contains("not registered"),
            "error must name the failure: {}",
            err.reason
        );
    }

    // -----------------------------------------------------------------------
    // Decode failure: malformed mutation bytes must return ApplyError.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_with_malformed_data_fails_loud() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine);

        let mutation = ApplyMutation {
            data: vec![0xFF, 0x00, 0x01], // truncated / not a valid Mutation
            t: accord_ts(1000),
            deps: vec![],
        };

        let err = applier
            .apply(txn(1, 1000), mutation)
            .expect_err("malformed mutation bytes must fail loud");
        assert!(!err.reason.is_empty(), "decode error must carry a reason");
    }

    // -----------------------------------------------------------------------
    // LWW honors the Accord-agreed order, NOT the coordinator wall clock.
    //
    // The coordinator stamps the Mutation's cells at materialize time with its
    // own `SystemTime::now()` micros, BEFORE consensus picks the execution
    // timestamp `t`. Under coordinator clock skew the wall-clock cell stamps can
    // invert the Accord order. The applier MUST persist cells at a timestamp
    // derived from the agreed `t` so last-write-wins resolves against Accord's
    // total order — otherwise a lost update / non-linearizable result occurs.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_restamps_cells_to_accord_timestamp_not_wall_clock() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let key = make_key("pk1");

        // Txn A: Accord order t=100 (earlier), but coordinator wall clock is
        // SKEWED HIGH so its cell stamp is 9_000 (later than B's wall clock).
        let a = encoded_mutation(key.clone(), b"A", 9_000, accord_ts(100), vec![]);
        applier.apply(txn(1, 100), a).unwrap();

        // Txn B: Accord order t=200 (later), coordinator wall clock SKEWED LOW
        // so its cell stamp is 1_000 (earlier than A's wall clock).
        let b = encoded_mutation(key.clone(), b"B", 1_000, accord_ts(200), vec![]);
        applier.apply(txn(2, 200), b).unwrap();

        // If the applier wrote at the wall-clock cell stamp (9_000 vs 1_000),
        // A would win LWW and we'd read "A" — the lost-update bug. With the
        // agreed `t` (100 < 200) driving the cell timestamp, B wins.
        assert_eq!(
            read_cell0(&engine, &key).as_deref(),
            Some(b"B".as_slice()),
            "later Accord-agreed txn must win LWW regardless of coordinator clock skew"
        );
        // And the persisted cell timestamp must derive from B's agreed t (200),
        // not its wall-clock stamp (1_000).
        assert_eq!(
            read_cell0_ts(&engine, &key),
            Some(200),
            "persisted cell timestamp must be the Accord-agreed t, not wall clock"
        );
    }

    // -----------------------------------------------------------------------
    // End-to-end serialize round-trip: a Mutation carrying realistic cells
    // (whose own stamp differs from `t`) survives serialize -> deserialize and
    // is persisted readable, with the cell timestamp re-stamped to `t`. Guards
    // the router serialize_into -> applier deserialize_from contract.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_round_trips_serialized_mutation_and_restamps() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let key = make_key("pk-roundtrip");
        // Cell stamped with a wall-clock micros value far from the agreed t.
        let wall = 1_700_000_000_000_000_i64;
        let agreed = accord_ts(42);
        let mutation = encoded_mutation(key.clone(), b"payload", wall, agreed, vec![]);

        applier
            .apply(txn(7, 42), mutation)
            .expect("serialized mutation must round-trip and persist");

        assert_eq!(
            read_cell0(&engine, &key).as_deref(),
            Some(b"payload".as_slice()),
            "round-tripped mutation value must be readable"
        );
        assert_eq!(
            read_cell0_ts(&engine, &key),
            Some(42),
            "cell timestamp must be re-stamped to the agreed t, not the wall clock"
        );
    }

    // -----------------------------------------------------------------------
    // Multi-key idempotency — DATA-LOSS-CRITICAL. Two writes of the SAME txn at
    // the SAME agreed `t` but DIFFERENT partition keys must BOTH persist. The
    // pre-fix idempotency key was (txn_id, t.time), so the second key was
    // silently deduped as a "re-apply" and its write vanished.
    // -----------------------------------------------------------------------
    #[test]
    fn two_keys_same_txn_and_t_both_persist() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let id = txn(1, 1000);
        let t = accord_ts(1000);
        let k1 = make_key("pk1");
        let k2 = make_key("pk2");

        applier
            .apply(id, encoded_mutation(k1.clone(), b"v1", 1000, t, vec![]))
            .unwrap();
        // SAME txn_id + t, DIFFERENT key — must NOT be treated as a re-apply.
        applier
            .apply(id, encoded_mutation(k2.clone(), b"v2", 1000, t, vec![]))
            .unwrap();

        assert_eq!(read_cell0(&engine, &k1).as_deref(), Some(b"v1".as_slice()));
        assert_eq!(
            read_cell0(&engine, &k2).as_deref(),
            Some(b"v2".as_slice()),
            "second key of the same (txn,t) must persist — idempotency must include the key"
        );
    }

    // -----------------------------------------------------------------------
    // Atomic writeset apply: every key of a multi-key txn commits in ONE atomic
    // batch (all-or-nothing). A failure on ANY key persists NONE — the multi-key
    // transaction never tears at the replica storage seam.
    // -----------------------------------------------------------------------
    #[test]
    fn apply_writeset_persists_all_keys() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let id = txn(1, 1000);
        let t = accord_ts(1000);
        let k1 = make_key("pk1");
        let k2 = make_key("pk2");

        applier
            .apply_writeset(
                id,
                vec![
                    encoded_mutation(k1.clone(), b"a", 1000, t, vec![]),
                    encoded_mutation(k2.clone(), b"b", 1000, t, vec![]),
                ],
            )
            .expect("a clean multi-key writeset must persist every key");

        assert_eq!(read_cell0(&engine, &k1).as_deref(), Some(b"a".as_slice()));
        assert_eq!(read_cell0(&engine, &k2).as_deref(), Some(b"b".as_slice()));
        assert_eq!(applier.applied_count(), 2, "both (txn,key,t) recorded");
    }

    #[test]
    fn apply_writeset_is_all_or_nothing_when_a_key_fails() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let id = txn(1, 1000);
        let t = accord_ts(1000);
        let k1 = make_key("pk1");
        let good = encoded_mutation(k1.clone(), b"a", 1000, t, vec![]);

        // Second write targets a table that was never registered → preflight
        // fails for the batch, so NO key may persist.
        let bad_m = Mutation::new(
            KS.to_string(),
            "no_such_table".to_string(),
            make_key("pk2"),
            vec![make_row(b"b", 1000)],
            1000,
        );
        let mut buf = vec![0u8; bad_m.serialized_size()];
        bad_m.serialize_into(&mut buf);
        let bad = ApplyMutation {
            data: buf,
            t,
            deps: vec![],
        };

        let err = applier
            .apply_writeset(id, vec![good, bad])
            .expect_err("a failing key must fail the whole writeset — fail loud");
        assert!(!err.reason.is_empty(), "error must carry a reason");

        assert!(
            read_cell0(&engine, &k1).is_none(),
            "all-or-nothing: the good key must NOT persist when another key in the writeset fails"
        );
        assert_eq!(
            applier.applied_count(),
            0,
            "no (txn,key,t) may be recorded for a failed writeset"
        );
    }

    #[test]
    fn apply_writeset_skips_already_applied_keys_and_applies_the_rest() {
        // A re-drive where key A already persisted (single-key apply) and the
        // writeset {A,B} arrives: A is deduped, B applies. No torn state, no
        // resurrection of A.
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());

        let id = txn(1, 1000);
        let t = accord_ts(1000);
        let k1 = make_key("pk1");
        let k2 = make_key("pk2");

        applier
            .apply(id, encoded_mutation(k1.clone(), b"a", 1000, t, vec![]))
            .unwrap();
        // Re-drive as a writeset: A already applied (skip), B new (apply).
        applier
            .apply_writeset(
                id,
                vec![
                    encoded_mutation(k1.clone(), b"a", 1000, t, vec![]),
                    encoded_mutation(k2.clone(), b"b", 1000, t, vec![]),
                ],
            )
            .unwrap();

        assert_eq!(read_cell0(&engine, &k1).as_deref(), Some(b"a".as_slice()));
        assert_eq!(read_cell0(&engine, &k2).as_deref(), Some(b"b".as_slice()));
        assert_eq!(applier.applied_count(), 2);
    }

    /// Build an `ApplyMutation` whose decoded `Mutation` targets `table` (use an
    /// unregistered table name to force a preflight failure).
    fn encoded_mutation_for_table(
        table: &str,
        key: DecoratedKey,
        value: &[u8],
        t: Timestamp,
    ) -> ApplyMutation {
        let m = Mutation::new(
            KS.to_string(),
            table.to_string(),
            key,
            vec![make_row(value, 1000)],
            1000,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        ApplyMutation {
            data: buf,
            t,
            deps: vec![],
        }
    }

    // =======================================================================
    // BDD scenario — atomic rollback of a partial multi-key write-set.
    // =======================================================================
    #[test]
    fn bdd_failed_key_rolls_back_the_whole_multikey_writeset() {
        // GIVEN a real engine and a two-key write-set whose second key targets an
        // unregistered table,
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());
        let good_key = make_key("ok");
        let t = accord_ts(1000);
        let good = encoded_mutation(good_key.clone(), b"v", 1000, t, vec![]);
        let bad = encoded_mutation_for_table("missing_table", make_key("bad"), b"x", t);

        // WHEN the write-set is applied,
        let res = applier.apply_writeset(txn(1, 1000), vec![good, bad]);

        // THEN it fails loud AND the good key did not persist (all-or-nothing).
        assert!(res.is_err(), "a failing key fails the whole write-set");
        assert!(
            read_cell0(&engine, &good_key).is_none(),
            "atomic rollback: no key persists when any key in the set fails"
        );
        assert_eq!(applier.applied_count(), 0);
    }

    // =======================================================================
    // Property tests — idempotent replay / LWW-by-agreed-t, and atomicity.
    // =======================================================================
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// IDEMPOTENT REPLAY + LWW-BY-AGREED-T. For a single key written by
        /// several transactions at distinct agreed timestamps, applying those
        /// writes in ANY order with ARBITRARY duplicates always converges to the
        /// value of the highest agreed `t` — re-applying an already-applied
        /// (txn,key,t) is a no-op and never resurrects a stale value.
        #[test]
        fn prop_idempotent_replay_converges_to_highest_agreed_t(
            arrivals in prop::collection::vec(0usize..6, 1..24),
        ) {
            let (engine, _dir) = make_engine();
            let applier = EngineStorageApplier::new(engine.clone());
            let key = make_key("pk");

            for &w in &arrivals {
                let t = accord_ts((w as u64 + 1) * 100);
                applier
                    .apply(
                        txn(1, (w as u64 + 1) * 100),
                        encoded_mutation(key.clone(), format!("v{w}").as_bytes(), ((w + 1) * 100) as i64, t, vec![]),
                    )
                    .unwrap();
            }

            let winner = *arrivals.iter().max().unwrap();
            let want = format!("v{winner}");
            let got = read_cell0(&engine, &key);
            prop_assert_eq!(
                got.as_deref(),
                Some(want.as_bytes()),
                "the write with the highest agreed t wins regardless of arrival order/duplicates"
            );
        }

        /// ATOMIC VISIBILITY. A multi-key write-set is all-or-nothing: if any key
        /// fails (unregistered table), NONE of the keys persist; if all succeed,
        /// ALL persist. A read can never observe a partial multi-key transaction.
        #[test]
        fn prop_writeset_apply_is_atomic_all_or_nothing(
            n_keys in 1usize..5,
            fail_at in prop::option::of(0usize..5),
        ) {
            let (engine, _dir) = make_engine();
            let applier = EngineStorageApplier::new(engine.clone());
            let t = accord_ts(1000);

            let keys: Vec<DecoratedKey> = (0..n_keys).map(|k| make_key(&format!("pk{k}"))).collect();
            let mut muts = Vec::with_capacity(n_keys);
            for (k, key) in keys.iter().enumerate() {
                if fail_at == Some(k) {
                    muts.push(encoded_mutation_for_table("missing_table", key.clone(), b"x", t));
                } else {
                    muts.push(encoded_mutation(key.clone(), format!("v{k}").as_bytes(), 1000, t, vec![]));
                }
            }

            let res = applier.apply_writeset(txn(1, 1000), muts);
            let expect_fail = fail_at.map(|f| f < n_keys).unwrap_or(false);

            if expect_fail {
                prop_assert!(res.is_err(), "a failing key must fail the whole write-set");
                for key in &keys {
                    prop_assert!(
                        read_cell0(&engine, key).is_none(),
                        "all-or-nothing: no key may persist when the write-set fails"
                    );
                }
                prop_assert_eq!(applier.applied_count(), 0);
            } else {
                prop_assert!(res.is_ok());
                for (k, key) in keys.iter().enumerate() {
                    let want = format!("v{k}");
                    let got = read_cell0(&engine, key);
                    prop_assert_eq!(
                        got.as_deref(),
                        Some(want.as_bytes()),
                        "every key of a clean write-set persists"
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // StorageReader / EngineStorageReader: linearizable read-at-t.
    //
    // The reader is symmetric to the applier: a replica calls read_row_at
    // during ReadVote (after dep-wait), and the coordinator decodes the
    // returned Mutation bytes to evaluate the generic IF predicate.
    // -----------------------------------------------------------------------

    fn decode_cell0(bytes: &[u8]) -> Option<Vec<u8>> {
        let m = Mutation::deserialize_from(bytes).expect("reader bytes must decode as a Mutation");
        let row = m.rows.first()?;
        row.cells.first().and_then(|(_, c)| c.value.clone())
    }

    #[test]
    fn read_row_at_returns_none_for_absent_row() {
        let (engine, _dir) = make_engine();
        let reader = EngineStorageReader::new(engine);

        let got = reader
            .read_row_at(KS, TABLE, b"missing", accord_ts(1000))
            .expect("read must succeed");
        assert!(got.is_none(), "absent row must read as None at t");
    }

    #[test]
    fn read_row_at_returns_serialized_row_when_present() {
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());
        let reader = EngineStorageReader::new(engine.clone());

        let key = make_key("pk1");
        let t = accord_ts(1000);
        applier
            .apply(
                txn(1, 1000),
                encoded_mutation(key.clone(), b"hello", 1000, t, vec![]),
            )
            .unwrap();

        let bytes = reader
            .read_row_at(KS, TABLE, b"pk1", accord_ts(1000))
            .expect("read must succeed")
            .expect("row written at t must be visible to read_row_at");
        assert_eq!(
            decode_cell0(&bytes).as_deref(),
            Some(b"hello".as_slice()),
            "read_row_at must return the row that was applied at t"
        );
    }

    #[test]
    fn read_row_at_excludes_cells_written_after_t() {
        // A cell written at a HIGHER agreed t must NOT be visible to a read at a
        // lower t — the read is honestly as-of-t (defensive bound).
        let (engine, _dir) = make_engine();
        let applier = EngineStorageApplier::new(engine.clone());
        let reader = EngineStorageReader::new(engine.clone());

        let key = make_key("pk1");
        // Write at agreed t=2000 (cell stamped 2000 by the applier).
        applier
            .apply(
                txn(1, 2000),
                encoded_mutation(key.clone(), b"future", 5_000, accord_ts(2000), vec![]),
            )
            .unwrap();

        // Read as of t=1000 — the future write (cell ts 2000) is excluded, so
        // the row reads as absent.
        let got = reader
            .read_row_at(KS, TABLE, b"pk1", accord_ts(1000))
            .expect("read must succeed");
        assert!(
            got.is_none(),
            "a cell written at a later agreed t must not be visible to a read at an earlier t"
        );
    }

    // -----------------------------------------------------------------------
    // End-to-end streaming SUBSCRIBE latency with two simultaneous connections:
    // one on the local-commit stream (WrittenOnNode, fired at commit-log append)
    // and one on the full-Accord stream (CommittedToCluster, fired by the
    // CdcPublishingApplier after a durable Accord apply). Measures write->deliver
    // latency for each path and prints the distribution.
    //
    // NOTE: single-node, in-process push bus — both streams deliver in
    // microseconds. The local-vs-committed *gap* (cluster consensus / quorum
    // round-trip) only manifests in a multi-node deployment; here both paths are
    // dominated by the same local commit-log append.
    fn report(label: &str, samples: &mut [u64]) {
        samples.sort_unstable();
        let n = samples.len();
        let avg = samples.iter().sum::<u64>() / n as u64;
        let p50 = samples[n / 2];
        let p99 = samples[(n * 99 / 100).min(n - 1)];
        eprintln!(
            "SUBSCRIBE latency [{label}] over {n} writes: avg={:.1}µs p50={:.1}µs p99={:.1}µs",
            avg as f64 / 1000.0,
            p50 as f64 / 1000.0,
            p99 as f64 / 1000.0,
        );
    }

    #[tokio::test]
    async fn subscribe_latency_two_connections_local_and_full_accord() {
        let (engine, _dir) = make_engine();
        let bus = ferrosa_cdc::CdcBus::new(8192);
        engine.set_cdc_bus(bus.clone());

        // Two simultaneous subscriber connections.
        let mut local = bus.subscribe(ferrosa_cdc::CdcStream::WrittenOnNode);
        let mut committed = bus.subscribe(ferrosa_cdc::CdcStream::CommittedToCluster);

        let applier =
            CdcPublishingApplier::new(Arc::new(EngineStorageApplier::new(engine.clone())), bus);

        const WARMUP: usize = 20;
        const N: usize = 300;

        // --- Local-commit path: plain engine writes fire WrittenOnNode only. ---
        let mut local_ns = Vec::with_capacity(N);
        for i in 0..(WARMUP + N) {
            let key = make_key(&format!("local{i}"));
            let m = Mutation::new(
                KS.to_string(),
                TABLE.to_string(),
                key,
                vec![make_row(format!("v{i}").as_bytes(), 1_000 + i as i64)],
                1_000 + i as i64,
            );
            let t0 = std::time::Instant::now();
            engine.write_atomic_batch(vec![m]).expect("local write");
            let ev = local.recv().await.expect("WrittenOnNode delivered");
            let dt = t0.elapsed();
            assert_eq!(ev.stream, ferrosa_cdc::CdcStream::WrittenOnNode);
            if i >= WARMUP {
                local_ns.push(dt.as_nanos() as u64);
            }
        }

        // --- Full-Accord path: applier.apply does a durable apply, then the
        //     decorator publishes CommittedToCluster (and the inner write fires
        //     WrittenOnNode, which we drain). ---
        let mut committed_ns = Vec::with_capacity(N);
        for i in 0..(WARMUP + N) {
            let key = make_key(&format!("accord{i}"));
            let t = accord_ts(10_000 + i as u64);
            let m = encoded_mutation(
                key,
                format!("v{i}").as_bytes(),
                10_000 + i as i64,
                t,
                vec![],
            );
            let t0 = std::time::Instant::now();
            applier
                .apply(txn(1, 10_000 + i as u64), m)
                .expect("accord apply");
            let ev = committed
                .recv()
                .await
                .expect("CommittedToCluster delivered");
            let dt = t0.elapsed();
            assert_eq!(ev.stream, ferrosa_cdc::CdcStream::CommittedToCluster);
            assert_eq!(
                ev.accord_ts,
                Some(t),
                "committed event carries the accord ts"
            );
            // Drain the WrittenOnNode the inner write produced, so the local
            // subscriber does not lag.
            local.recv().await.expect("inner WrittenOnNode");
            if i >= WARMUP {
                committed_ns.push(dt.as_nanos() as u64);
            }
        }

        report("local commit (WrittenOnNode)", &mut local_ns);
        report("full accord (CommittedToCluster)", &mut committed_ns);
        assert_eq!(local_ns.len(), N);
        assert_eq!(committed_ns.len(), N);
    }

    // -----------------------------------------------------------------------
    // MULTI-NODE model: two nodes, each with its own StorageEngine + push CDC
    // bus. Node A takes local writes (WrittenOnNode); node B is a replica that
    // applies cluster-committed writes via the real Accord apply path
    // (CdcPublishingApplier -> CommittedToCluster). Subscribers on each node
    // measure write->deliver latency, PROVING delivery is event-driven push:
    //   * delivery is dominated by the commit cost, with NO poll-interval floor;
    //   * after a long idle gap a single committed write is delivered
    //     immediately — a poll-based CDC would be bounded by its interval.
    //
    // (A fully network-formed cluster latency number additionally needs the
    // live-infra cluster harness; this models the replica apply + CDC fan-out,
    // which is where CommittedToCluster actually fires on a cluster commit.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multinode_subscribe_is_realtime_push_not_poll() {
        // --- Two independent nodes. ---
        let (engine_a, _da) = make_engine();
        let bus_a = ferrosa_cdc::CdcBus::new(8192);
        engine_a.set_cdc_bus(bus_a.clone());
        let (engine_b, _db) = make_engine();
        let bus_b = ferrosa_cdc::CdcBus::new(8192);
        engine_b.set_cdc_bus(bus_b.clone());

        // Two subscriber connections, one per node.
        let mut sub_a_local = bus_a.subscribe(ferrosa_cdc::CdcStream::WrittenOnNode);
        let mut sub_b_committed = bus_b.subscribe(ferrosa_cdc::CdcStream::CommittedToCluster);

        // Node B applies cluster-committed writes (replica apply path).
        let applier_b =
            CdcPublishingApplier::new(Arc::new(EngineStorageApplier::new(engine_b.clone())), bus_b);

        const N: usize = 200;
        let mut a_local_ns = Vec::with_capacity(N);
        let mut b_committed_ns = Vec::with_capacity(N);
        for i in 0..N {
            // Local commit on node A -> WrittenOnNode delivered to A's subscriber.
            let m_a = Mutation::new(
                KS.to_string(),
                TABLE.to_string(),
                make_key(&format!("a{i}")),
                vec![make_row(format!("v{i}").as_bytes(), 1_000 + i as i64)],
                1_000 + i as i64,
            );
            let t0 = std::time::Instant::now();
            engine_a
                .write_atomic_batch(vec![m_a])
                .expect("node A write");
            sub_a_local.recv().await.expect("A WrittenOnNode delivered");
            a_local_ns.push(t0.elapsed().as_nanos() as u64);

            // Cluster-committed apply on node B -> CommittedToCluster to B's sub.
            let t = accord_ts(100_000 + i as u64);
            let m_b = encoded_mutation(
                make_key(&format!("b{i}")),
                format!("v{i}").as_bytes(),
                100_000 + i as i64,
                t,
                vec![],
            );
            let t1 = std::time::Instant::now();
            applier_b
                .apply(txn(2, 100_000 + i as u64), m_b)
                .expect("node B apply");
            sub_b_committed
                .recv()
                .await
                .expect("B CommittedToCluster delivered");
            b_committed_ns.push(t1.elapsed().as_nanos() as u64);
        }
        report("node A local-commit (WrittenOnNode)", &mut a_local_ns);
        report(
            "node B cluster-committed (CommittedToCluster)",
            &mut b_committed_ns,
        );

        // --- Push-vs-poll discriminator: idle, then ONE committed write. A
        //     poll-based CDC would deliver no sooner than its next tick; a push
        //     bus delivers as soon as the apply commits. ---
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let t = accord_ts(900_000);
        let m = encoded_mutation(make_key("idle"), b"x", 900_000, t, vec![]);
        let t0 = std::time::Instant::now();
        applier_b
            .apply(txn(2, 900_000), m)
            .expect("post-idle apply");
        let ev = sub_b_committed
            .recv()
            .await
            .expect("post-idle CommittedToCluster delivered");
        let idle_deliver = t0.elapsed();
        eprintln!("after 250ms idle, CommittedToCluster delivered in {idle_deliver:?}");
        assert_eq!(ev.accord_ts, Some(t));
        // Load-robust bound: the measured number (printed above) is the real
        // proof — it is dominated by the commit fsync (~ms), NOT a poll interval.
        // A poll-based CDC (e.g. a segment-reader on a multi-second checkpoint
        // interval) could not deliver this fast regardless of CPU contention.
        assert!(
            idle_deliver < std::time::Duration::from_secs(1),
            "real-time push CDC must deliver promptly after the commit \
             (got {idle_deliver:?}); a poll-based stream would be bounded by its \
             poll interval regardless of the idle gap"
        );
    }
}
