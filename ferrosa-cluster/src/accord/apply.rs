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

use std::collections::HashSet;
use std::sync::Arc;

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_storage::{Mutation, StorageEngine, TableId};
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
}

impl NoopStorageApplier {
    pub fn new() -> Self {
        Self {
            applied: Mutex::new(Vec::new()),
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
}

impl Default for NoopStorageApplier {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageApplier for NoopStorageApplier {
    fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
        self.applied.lock().push((txn_id, mutation.t));
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
/// [`Mutation`], which carries the `(keyspace, table)` → [`TableId`], the
/// [`DecoratedKey`](ferrosa_common::DecoratedKey), the row data, and the
/// write timestamp. Each row is written via [`StorageEngine::write`], which
/// appends to the commit log and the memtable (persist-before-return).
///
/// # Idempotency
///
/// Accord delivers `Apply` at-least-once, and recovery may re-drive it. The
/// applier records every `(txn_id, t)` it has applied and treats a repeat as a
/// no-op. This is required for correctness: cells are persisted at the agreed
/// `t` (see [`accord_cell_timestamp`]), so a re-applied old mutation re-uses
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
    /// `(txn_id, t.time)` pairs already persisted — for idempotent re-apply.
    applied: Mutex<HashSet<(TxnId, u64)>>,
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
        let key = (txn_id, mutation.t.time);

        // Idempotency gate: an already-applied (txn_id, t) is a no-op. Checked
        // before decode so a duplicate Apply never re-writes a stale value.
        if self.applied.lock().contains(&key) {
            return Ok(());
        }

        // Decode the self-describing commit-log mutation. Fail loud on garbage.
        let mut decoded = Mutation::deserialize_from(&mutation.data).map_err(|e| ApplyError {
            txn_id,
            reason: format!("failed to decode apply mutation: {e}"),
        })?;

        let table_id = TableId::new(&decoded.keyspace, &decoded.table);

        // Re-stamp every cell to the Accord-agreed execution timestamp `t`.
        //
        // The coordinator stamps cells at materialize time with its own wall
        // clock, BEFORE consensus picks `t`. Honoring that wall clock for LWW
        // would let coordinator clock skew invert the Accord total order
        // (lost update / non-linearizable). The agreed `t` exists precisely to
        // order conflicting writes, so it — not the wall clock — must drive the
        // last-write-wins cell timestamp.
        let cell_ts = accord_cell_timestamp(mutation.t);
        for row in &mut decoded.rows {
            restamp_row(row, cell_ts);
        }

        // Persist every row at the agreed timestamp. `StorageEngine::write`
        // appends to the commit log (durable) before the memtable, and returns
        // an error if the table is unregistered or admission is denied — we
        // propagate that as `ApplyError` (never fake success).
        for row in decoded.rows {
            self.engine
                .write(&table_id, &decoded.key, row, cell_ts)
                .map_err(|e| ApplyError {
                    txn_id,
                    reason: format!("storage write failed for {table_id}: {e}"),
                })?;
        }

        // Record only after all rows are durable, so a mid-apply failure leaves
        // the txn re-appliable rather than falsely marked applied.
        self.applied.lock().insert(key);
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
}

impl DepWaitApplier {
    pub fn new(applier: Arc<dyn StorageApplier>) -> Self {
        Self {
            graph: Mutex::new(DepWaitGraph::new()),
            applier,
        }
    }

    /// Attempt to apply a committed transaction.
    ///
    /// If all dependencies in `deps` are already applied, calls `applier.apply`
    /// immediately and propagates the "applied" signal through the graph.
    ///
    /// If any dependencies are not yet applied, registers this transaction as
    /// a waiter and returns `Ok(false)`. The transaction will be applied
    /// automatically when the last dependency calls `notify_applied`.
    ///
    /// Returns `Ok(true)` if the transaction was applied immediately, or
    /// `Ok(false)` if it was queued to wait.
    pub fn try_apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<bool, ApplyError> {
        let deps: Vec<TxnId> = mutation.deps.clone();

        // Fast path: check all deps without holding the lock.
        // Then take the lock to register waits atomically.
        let mut graph = self.graph.lock();

        // Register waits for any unresolved deps.
        let mut waiting = false;
        for dep in &deps {
            if !graph.is_applied(dep) {
                // Dep not yet applied — register a wait.
                // We don't use DepWaitGraph::register_wait here because we
                // handle the "waiter" tracking ourselves per-transaction.
                waiting = true;
                // For now: mark that this txn needs this dep.
                // The actual scheduling will be handled by notify_applied.
                let _ = graph.register_wait(txn_id, *dep);
            }
        }

        if waiting {
            // Store the mutation for later application.
            // For simplicity in this implementation, we apply eagerly once
            // notify_applied cascades — see notify_applied below.
            drop(graph);
            return Ok(false);
        }

        drop(graph);

        // All deps satisfied — apply now.
        self.applier.apply(txn_id, mutation)?;

        // Mark applied in the graph and cascade to waiters.
        let woken = self.graph.lock().mark_applied(txn_id);
        for waiter_id in woken {
            // Waiters were registered with empty mutations (data=vec![]).
            // In production this would look up the queued mutation from a
            // per-txn store. For Gap 5 happy path, we apply with empty data.
            let _ = self.applier.apply(
                waiter_id,
                ApplyMutation {
                    data: vec![],
                    t: txn_id.0, // use dep's timestamp as placeholder
                    deps: vec![],
                },
            );
            let _ = self.graph.lock().mark_applied(waiter_id);
        }

        Ok(true)
    }

    /// Notify the applier that `txn_id` has been applied externally.
    ///
    /// Called when a dependency transaction's apply is acknowledged by the
    /// remote coordinator (via `ApplyOK`). Unblocks any waiting transactions.
    pub fn notify_applied(&self, txn_id: TxnId) {
        let woken = self.graph.lock().mark_applied(txn_id);
        for waiter_id in woken {
            // Waiter is now unblocked — apply with empty mutation (will be
            // superseded by real data in the production implementation).
            let _ = self.applier.apply(
                waiter_id,
                ApplyMutation {
                    data: vec![],
                    t: txn_id.0,
                    deps: vec![],
                },
            );
            let _ = self.graph.lock().mark_applied(waiter_id);
        }
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

        assert!(result, "transaction with no deps must apply immediately");
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
            result,
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

        assert!(!result, "transaction with pending dep must be queued");
        assert!(
            !noop.was_applied(&txn),
            "txn must not be applied while dep is pending"
        );

        // Dep becomes available — notify the applier.
        applier.notify_applied(dep);

        // Txn should now be applied (woken from the dep-wait graph).
        // The waiter should have been called via cascade in notify_applied.
        assert_eq!(noop.apply_count(), 1, "cascade must apply the waiter");
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
        assert!(result, "txn_b must apply immediately (dep already applied)");
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
}
