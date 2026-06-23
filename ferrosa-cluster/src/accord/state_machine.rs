//! AccordStateMachine: core Accord consensus protocol handler.
//!
//! This module implements the replica-side state machine for the Accord
//! distributed transaction protocol. Each handler method processes a single
//! protocol message and returns zero or one response messages.
//!
//! # Handler contract
//!
//! Every handler follows this sequence:
//! 1. Validate ballot (NACK if a higher ballot has been promised)
//! 2. Look up or create [`TxnState`] for the transaction
//! 3. Transition the phase forward (never backward)
//! 4. Persist to the [`SyncWriter`] before producing a reply
//! 5. Return the response message (or nothing for fire-and-forget)
//!
//! # Idempotency
//!
//! Duplicate messages for the same (txn_id, phase) are idempotent: the
//! state machine returns the same response without re-persisting.
//!
//! # Persist-before-reply
//!
//! All handlers that produce a response call [`SyncWriter::write_and_sync`]
//! before returning the response. If fsync fails, no response is returned
//! (the caller must not send a reply).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ferrosa_common::accord::{
    AcceptedBallot, BallotNumber, PromisedBallot, Timestamp, TxnId, TxnPhase, TxnState,
};
use ferrosa_storage::accord::conflict_index::{ConflictIndex, InFlightWrite, TxnStatus};
use ferrosa_storage::accord::sync_writer::SyncWriter;
use tokio::sync::Notify;

use crate::accord::apply::{
    ApplyMutation, DepWaitApplier, NoopStorageApplier, StorageApplier, StorageReader,
};

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response from the state machine to a protocol message.
#[derive(Debug, Clone)]
pub enum SmResponse {
    /// PreAcceptOK: agreement (possibly with adjusted timestamp and deps).
    PreAcceptOK {
        txn_id: TxnId,
        t: Timestamp,
        deps: Vec<TxnId>,
    },
    /// AcceptOK: ballot accepted.
    AcceptOK {
        txn_id: TxnId,
        ballot: BallotNumber,
        deps: Vec<TxnId>,
    },
    /// NACK: a higher ballot has been promised.
    Nack {
        txn_id: TxnId,
        promised: PromisedBallot,
    },
    /// No response (fire-and-forget: Commit, Apply).
    None,
}

// ---------------------------------------------------------------------------
// AccordStateMachine
// ---------------------------------------------------------------------------

/// Replica-side Accord consensus state machine.
///
/// Owns per-transaction state, a conflict index for dependency detection,
/// and a sync writer for persist-before-reply durability.
pub struct AccordStateMachine {
    /// This replica's node ID.
    node_id: u64,
    /// Per-transaction consensus state.
    txn_states: HashMap<TxnId, TxnState>,
    /// Shard-local conflict index for dependency detection.
    conflict_index: ConflictIndex,
    /// Fsync-before-ack writer (production or mock).
    sync_writer: Arc<dyn SyncWriter>,
    /// Waiters: transactions blocked on a dependency becoming committed.
    /// Maps dep_txn_id -> list of txn_ids waiting on it.
    dep_waiters: HashMap<TxnId, Vec<TxnId>>,
    /// Committed transactions (for waiter notification tracking).
    committed_txns: HashSet<TxnId>,
    /// Notified waiters from the last commit (for test inspection).
    last_notified: Vec<TxnId>,
    /// Dep-ordered apply engine: wraps the storage [`StorageApplier`] in a
    /// [`DepWaitApplier`] so a committed transaction is persisted only after all
    /// of its dependencies have applied on this replica. `handle_apply` routes
    /// every real write through this (never the raw applier directly), which is
    /// what enforces dependency order at the replica apply path — the prerequisite
    /// for serializable multi-key transactions. The inner applier defaults to
    /// [`NoopStorageApplier`] for protocol-only tests; production wires a real
    /// engine-backed applier via [`AccordStateMachine::with_applier`].
    apply_engine: Arc<DepWaitApplier>,
    /// Optional storage read seam for the generic-`IF` linearizable read-at-`t`
    /// (Gap 4). `None` keeps the conflict-index existence path used by
    /// `INSERT IF NOT EXISTS`. Production wires an engine-backed reader via
    /// [`AccordStateMachine::with_applier_and_reader`] so generic predicates can
    /// read the real row at `t`.
    reader: Option<Arc<dyn StorageReader>>,
    /// Broadcast wake fired after every successful [`Self::handle_apply`].
    ///
    /// The `ReadVote` dep-wait (see the `AccordRead` handler) parks on this
    /// notify after dropping the state lock, so a conflicting transaction
    /// reaching `Applied` re-wakes any read that is blocked waiting for it.
    /// Using a broadcast `Notify::notify_waiters` (rather than per-txn
    /// channels) keeps the apply path lock-cheap and is safe because the waiter
    /// always re-checks the unapplied-conflict condition under the lock after a
    /// wake — a spurious or coalesced wake just costs one extra re-check.
    applied_notify: Arc<Notify>,
}

impl AccordStateMachine {
    /// Create a new state machine for the given node.
    ///
    /// Uses a [`NoopStorageApplier`]: the apply seam records `(txn_id, t)` but
    /// does NOT persist the row. Production must use [`Self::with_applier`] to
    /// supply a real engine-backed applier.
    pub fn new(node_id: u64, sync_writer: Arc<dyn SyncWriter>) -> Self {
        Self {
            node_id,
            txn_states: HashMap::new(),
            conflict_index: ConflictIndex::new(100_000),
            sync_writer,
            dep_waiters: HashMap::new(),
            committed_txns: HashSet::new(),
            last_notified: Vec::new(),
            apply_engine: Arc::new(DepWaitApplier::new(Arc::new(NoopStorageApplier::new()))),
            reader: None,
            applied_notify: Arc::new(Notify::new()),
        }
    }

    /// Create with a custom conflict index capacity.
    ///
    /// Uses a [`NoopStorageApplier`] (see [`Self::new`]).
    pub fn with_capacity(
        node_id: u64,
        sync_writer: Arc<dyn SyncWriter>,
        conflict_index_capacity: usize,
    ) -> Self {
        Self {
            node_id,
            txn_states: HashMap::new(),
            conflict_index: ConflictIndex::new(conflict_index_capacity),
            sync_writer,
            dep_waiters: HashMap::new(),
            committed_txns: HashSet::new(),
            last_notified: Vec::new(),
            apply_engine: Arc::new(DepWaitApplier::new(Arc::new(NoopStorageApplier::new()))),
            reader: None,
            applied_notify: Arc::new(Notify::new()),
        }
    }

    /// Create with a real [`StorageApplier`] (production wiring).
    ///
    /// The applier persists each committed mutation to the local storage engine
    /// during [`Self::handle_apply`], closing the phantom-write gap
    /// (`bug-accord-lwt-acks-phantom-write.md`).
    pub fn with_applier(
        node_id: u64,
        sync_writer: Arc<dyn SyncWriter>,
        applier: Arc<dyn StorageApplier>,
    ) -> Self {
        Self {
            node_id,
            txn_states: HashMap::new(),
            conflict_index: ConflictIndex::new(100_000),
            sync_writer,
            dep_waiters: HashMap::new(),
            committed_txns: HashSet::new(),
            last_notified: Vec::new(),
            apply_engine: Arc::new(DepWaitApplier::new(applier)),
            reader: None,
            applied_notify: Arc::new(Notify::new()),
        }
    }

    /// Create with a real [`StorageApplier`] **and** a [`StorageReader`]
    /// (full production wiring).
    ///
    /// The reader backs the generic-`IF` linearizable read-at-`t` (Gap 4): on a
    /// `ReadVote` carrying [`ReadPredicate::ReadRow`](crate::accord::wire::ReadPredicate),
    /// the replica reads the row at `t` and returns its bytes for the coordinator
    /// to evaluate. `INSERT IF NOT EXISTS` still uses the existence path.
    pub fn with_applier_and_reader(
        node_id: u64,
        sync_writer: Arc<dyn SyncWriter>,
        applier: Arc<dyn StorageApplier>,
        reader: Arc<dyn StorageReader>,
    ) -> Self {
        Self {
            node_id,
            txn_states: HashMap::new(),
            conflict_index: ConflictIndex::new(100_000),
            sync_writer,
            dep_waiters: HashMap::new(),
            committed_txns: HashSet::new(),
            last_notified: Vec::new(),
            apply_engine: Arc::new(DepWaitApplier::new(applier)),
            reader: Some(reader),
            applied_notify: Arc::new(Notify::new()),
        }
    }

    /// Read the row at `t` via the wired [`StorageReader`], if any.
    ///
    /// Returns `Ok(None)` when no reader is wired (the caller should fall back to
    /// the existence path) or when the row does not exist at `t`. The `Some`
    /// variant carries the serialized single-partition `Mutation` bytes for the
    /// coordinator to decode and evaluate the IF predicate against.
    pub fn read_row_bytes_at(
        &self,
        keyspace: &str,
        table: &str,
        key: &[u8],
        t: Timestamp,
    ) -> Option<Vec<u8>> {
        let reader = self.reader.as_ref()?;
        match reader.read_row_at(keyspace, table, key, t) {
            Ok(bytes) => bytes,
            Err(e) => {
                // Fail loud in logs; the coordinator treats a missing row-vote as
                // "no dissent" only when it has F+1 agreement, so a read error
                // here surfaces as the replica abstaining (no current_row), never
                // as a fabricated success.
                tracing::error!(%e, keyspace, table, "accord: read_row_at failed during ReadVote");
                None
            }
        }
    }

    /// Whether a [`StorageReader`] is wired (generic-`IF` read-at-`t` available).
    pub fn has_reader(&self) -> bool {
        self.reader.is_some()
    }

    /// Handle to the broadcast notify fired after every successful apply.
    ///
    /// A `ReadVote` dep-wait acquires this, drops the state lock, and parks on
    /// [`Notify::notified`]; a conflicting transaction reaching `Applied` (via
    /// [`Self::handle_apply`]) wakes it so it can re-check its condition.
    pub fn applied_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.applied_notify)
    }

    /// Conflicting transactions on `key` that a linearizable read-at-`t` must
    /// wait for before reading: those that are **`Committed`** (their final
    /// execution timestamp is locked) ordered **before** `t` (`t_committed < t`)
    /// and not yet `Applied` on this replica.
    ///
    /// This is the dep-wait set for the `AccordRead` (ReadVote) handler. Reading
    /// the row before such a transaction applies would observe stale state — the
    /// concurrent-`INSERT IF NOT EXISTS` double-apply (the ReadVote
    /// linearizability gap): two contenders both read the key as absent before
    /// either applies, both gates pass, and both insert.
    ///
    /// # Which phases are waited on
    ///
    /// - `PreAccepted` / `Accepted`: a genuinely concurrent contender whose `t0 <
    ///   t` (so it may yet commit with a final `t' < t` and apply a write before
    ///   our read is valid). We wait for it to resolve — it will either apply, be
    ///   finalized as a no-write, or its final `t` will be re-evaluated against
    ///   `t` once committed (the caller re-checks this set in a bounded loop, so a
    ///   contender that commits *after* `t` simply drops out of the set on the
    ///   next iteration).
    /// - `Committed`: wait only if its locked final `t < t` (it is ordered before
    ///   us and will apply or no-write-finalize). A `Committed` txn with `t > t`
    ///   is ordered after us and is NOT a dependency.
    /// - `Applied` / pruned: already applied — never waited on.
    ///
    /// A failed transaction never lingers here: a replica whose `PreAccept`
    /// fsync fails rolls back its conflict-index registration (see
    /// [`Self::handle_preaccept`]), and a committed-but-condition-failed LWT is
    /// finalized to `Applied` as a no-write. So the wait is always bounded by
    /// real protocol progress, not by abandoned phantom entries.
    pub fn unapplied_conflicts_before(&self, key: &[u8], t: &Timestamp) -> Vec<TxnId> {
        let mut pending = Vec::new();
        for dep_id in self.conflict_index.deps_before_t(key, t) {
            if let Some(state) = self.txn_states.get(&dep_id) {
                let wait = match state.phase {
                    // Already applied: nothing to wait for.
                    TxnPhase::Applied => false,
                    // Final t locked: dependency only if ordered before us.
                    TxnPhase::Committed => state.t < *t,
                    // Final t not yet locked but t0 < t (from deps_before_t):
                    // a possible ordered-before write still in flight — wait.
                    TxnPhase::PreAccepted | TxnPhase::Accepted => true,
                };
                if wait {
                    pending.push(dep_id);
                }
            }
        }
        pending
    }

    /// Get the current state for a transaction (if any).
    pub fn get_state(&self, txn_id: &TxnId) -> Option<&TxnState> {
        self.txn_states.get(txn_id)
    }

    /// Get the last set of waiters notified by a commit.
    pub fn last_notified_waiters(&self) -> &[TxnId] {
        &self.last_notified
    }

    /// Register a waiter: `waiter_txn` is waiting for `dep_txn` to commit.
    pub fn register_dep_waiter(&mut self, dep_txn: TxnId, waiter_txn: TxnId) {
        self.dep_waiters
            .entry(dep_txn)
            .or_default()
            .push(waiter_txn);
    }

    // -----------------------------------------------------------------------
    // PreAccept handler
    // -----------------------------------------------------------------------

    /// Handle a PreAccept message.
    ///
    /// 1. Check ballot: NACK if a higher ballot has been promised.
    /// 2. Check idempotency: if already PreAccepted with same t0, return
    ///    cached response.
    /// 3. Query ConflictIndex for dependencies and timestamp conflicts.
    /// 4. Register in ConflictIndex.
    /// 5. Create/update TxnState.
    /// 6. Persist before reply.
    /// 7. Return PreAcceptOK.
    pub fn handle_preaccept(
        &mut self,
        txn_id: TxnId,
        t0: Timestamp,
        key: &[u8],
        ballot: BallotNumber,
        _epoch: u64,
    ) -> SmResponse {
        // Check if we've already promised a higher ballot for this txn.
        if let Some(state) = self.txn_states.get(&txn_id) {
            if ballot < (state.max_ballot_seen.0) {
                return SmResponse::Nack {
                    txn_id,
                    promised: state.max_ballot_seen,
                };
            }

            // Idempotent: if already preaccepted (or beyond), return cached.
            if state.phase == TxnPhase::PreAccepted && state.t0 == t0 {
                return SmResponse::PreAcceptOK {
                    txn_id,
                    t: state.t,
                    deps: state.deps.iter().copied().collect(),
                };
            }

            // Reject PreAccept if already Accepted or beyond.
            if state.phase.rank() >= TxnPhase::Accepted.rank() {
                return SmResponse::Nack {
                    txn_id,
                    promised: state.max_ballot_seen,
                };
            }
        }

        // Query ConflictIndex for deps using t0 (not t).
        let deps = self.conflict_index.deps_before_t0(key, &t0);

        // Check for timestamp conflict: if any conflicting t0 >= our t0,
        // we need to bump our timestamp past it.
        let max_conflict = self.conflict_index.max_conflicting_timestamp(key);
        let t = match max_conflict {
            Some(ct) if ct >= t0 => t0.bump_past(&ct, self.node_id),
            _ => t0,
        };

        // Register in ConflictIndex.
        let entry = InFlightWrite {
            txn_id,
            t0,
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        // Don't fail the protocol message on capacity errors, but log them.
        if let Err(e) = self.conflict_index.register(key, entry) {
            tracing::error!(%e, "accord: conflict_index register failed");
        }

        // Track whether THIS call created the TxnState, so a persist failure can
        // roll back cleanly (a retried/duplicate PreAccept must not be erased).
        let newly_created = !self.txn_states.contains_key(&txn_id);

        // Create or update TxnState.
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));
        state.pre_accept(t, deps.clone());

        // Persist before reply.
        let data = format!("PreAccepted:{}:{}", txn_id.0.time, t.time);
        let result = self.sync_writer.write_and_sync(data.as_bytes());
        if !result.is_ok() {
            // Persist failed: this PreAccept is NOT durable. Roll back the
            // conflict-index registration (and the TxnState if we created it) so
            // the non-durable txn does not linger as a phantom dependency that
            // would make later linearizable reads on this key dep-wait until
            // timeout. Mirrors the commit/apply persist-before-advance discipline.
            self.conflict_index.remove(&txn_id);
            if newly_created {
                self.txn_states.remove(&txn_id);
            }
            return SmResponse::None;
        }

        let deps_vec: Vec<TxnId> = deps.into_iter().collect();
        SmResponse::PreAcceptOK {
            txn_id,
            t,
            deps: deps_vec,
        }
    }

    // -----------------------------------------------------------------------
    // Accept handler
    // -----------------------------------------------------------------------

    /// Handle an Accept message.
    ///
    /// 1. Check ballot: NACK if a higher ballot has been promised.
    /// 2. If already Committed or Applied, this is a no-op (return AcceptOK).
    /// 3. Update TxnState with accepted ballot, timestamp, deps.
    /// 4. Persist before reply.
    /// 5. Return AcceptOK.
    pub fn handle_accept(
        &mut self,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
        ballot: BallotNumber,
    ) -> SmResponse {
        // Look up or create state.
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));

        // If already committed or applied, Accept is a no-op.
        if state.phase == TxnPhase::Committed || state.phase == TxnPhase::Applied {
            return SmResponse::AcceptOK {
                txn_id,
                ballot,
                deps: state.deps.iter().copied().collect(),
            };
        }

        // Check ballot against promised ballot.
        if ballot < (state.max_ballot_seen.0) {
            return SmResponse::Nack {
                txn_id,
                promised: state.max_ballot_seen,
            };
        }

        // Accept: update both ballot fields, timestamp, deps, and phase.
        let deps_set: HashSet<TxnId> = deps.iter().copied().collect();
        state.accept(AcceptedBallot(ballot), t, deps_set);

        // Persist before reply.
        let data = format!("Accepted:{}:{}:{}", txn_id.0.time, t.time, ballot.0);
        let result = self.sync_writer.write_and_sync(data.as_bytes());
        if !result.is_ok() {
            return SmResponse::None;
        }

        SmResponse::AcceptOK {
            txn_id,
            ballot,
            deps,
        }
    }

    // -----------------------------------------------------------------------
    // Commit handler
    // -----------------------------------------------------------------------

    /// Handle a Commit message (fire-and-forget, no response).
    ///
    /// 1. Create or update TxnState.
    /// 2. Lock the final timestamp and deps.
    /// 3. Persist.
    /// 4. Wake any dep waiters.
    pub fn handle_commit(
        &mut self,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
    ) -> SmResponse {
        {
            let state = self
                .txn_states
                .entry(txn_id)
                .or_insert_with(|| TxnState::new(txn_id, t0));

            // Idempotent: if already committed or applied, no-op.
            if state.phase == TxnPhase::Committed || state.phase == TxnPhase::Applied {
                return SmResponse::None;
            }
        }

        // Persist the commit BEFORE advancing committed-visible state. Mirrors
        // `handle_apply`: the durable write must succeed before the txn becomes
        // observable as Committed and before its dependency waiters are woken.
        // If fsync fails we send no reply and leave the txn un-committed — the
        // coordinator gets no ack and the commit can be recovered/retried. The
        // old code mutated state (state.commit + committed_txns + woke deps)
        // before the fsync and only logged on failure, so a non-durable commit
        // could unblock dependents (phantom-commit hazard on the commit path).
        let data = format!("Committed:{}:{}", txn_id.0.time, t.time);
        if !self.sync_writer.write_and_sync(data.as_bytes()).is_ok() {
            tracing::error!(
                "accord: sync_writer failed during commit — not advancing to Committed"
            );
            return SmResponse::None;
        }

        // Durable — now safe to advance committed state and wake dependents.
        let deps_set: HashSet<TxnId> = deps.into_iter().collect();
        if let Some(state) = self.txn_states.get_mut(&txn_id) {
            state.commit(t, deps_set);
        }
        self.committed_txns.insert(txn_id);

        // Wake dep waiters.
        self.last_notified.clear();
        if let Some(waiters) = self.dep_waiters.remove(&txn_id) {
            self.last_notified = waiters;
        }

        SmResponse::None
    }

    // -----------------------------------------------------------------------
    // Apply handler
    // -----------------------------------------------------------------------

    /// Handle an Apply (fire-and-forget) — **dep-ordered** at the replica.
    ///
    /// 1. Idempotent no-op if already Applied; ignore an unknown txn.
    /// 2. No-write finalize (empty payload): an LWT whose IF condition did NOT
    ///    hold still *commits* but writes no row. It touches no storage, so there
    ///    is no dependency order to respect — advance it to `Applied` directly,
    ///    then ask the apply engine which parked waiters this unblocks and
    ///    advance them too (so a later read at `t' > t` does not block on this
    ///    phantom dependency until its dep-wait timeout).
    /// 3. Real write: route the mutation through the [`DepWaitApplier`]. If every
    ///    dependency has already applied on this replica, it persists the row
    ///    immediately and cascades to any waiters this unblocks; otherwise it
    ///    parks the mutation and returns an empty list — and we must NOT advance
    ///    the txn to `Applied`. Routing through the engine (never the raw applier)
    ///    is what enforces dependency order at the replica apply path.
    /// 4. For every transaction the engine actually persisted (the primary plus
    ///    any cascade-woken waiters), run the `bookkeep_applied` helper:
    ///    the protocol-log marker, `Applied` flag, conflict-index GC, and the
    ///    dep-wait wake. The storage write precedes the marker; the applier is
    ///    idempotent on `(txn_id, t)`, so a crash between them is recovered by the
    ///    per-txn Apply retry (never a falsely-`Applied` txn).
    pub fn handle_apply(&mut self, txn_id: TxnId, result_data: Vec<u8>) -> SmResponse {
        // A single-key Apply is the degenerate one-entry write-set. An empty
        // payload is the no-write finalize, represented as an empty write-set.
        let writes = if result_data.is_empty() {
            Vec::new()
        } else {
            vec![result_data]
        };
        self.handle_apply_writeset(txn_id, writes)
    }

    /// Apply a **multi-key** transaction: its full write-set, one encoded
    /// mutation per partition this replica owns. Every write is routed through
    /// the dep-ordered apply engine as one unit (parked together, applied
    /// atomically), so writes 2..N are never dropped.
    ///
    /// An empty `writes` (or all-empty entries) is the no-write finalize: the txn
    /// committed but writes no row (a failed-`IF` LWT), so it is advanced to
    /// `Applied` without touching storage and its dep-waiters are woken.
    ///
    /// Whatever the engine actually persists — the primary's N writes plus any
    /// cascade-woken waiters — is **deduplicated by `TxnId`** so each transaction
    /// is advanced to `Applied` and fsyncs its protocol-log marker exactly once
    /// (N markers per txn would corrupt the log).
    pub fn handle_apply_writeset(&mut self, txn_id: TxnId, writes: Vec<Vec<u8>>) -> SmResponse {
        // Read the agreed timestamp + deps from committed state BEFORE mutating,
        // so each mutation we hand to storage carries the real `(t, deps)`.
        let (t, deps): (Timestamp, Vec<TxnId>) = match self.txn_states.get(&txn_id) {
            Some(state) => {
                // Already applied: idempotent — do not re-persist or re-apply.
                if state.phase == TxnPhase::Applied {
                    return SmResponse::None;
                }
                (state.t, state.deps.iter().copied().collect())
            }
            // No state for this txn: nothing to apply.
            None => return SmResponse::None,
        };

        // Drop no-write entries (empty payloads): a key the replica owns but for
        // which this txn writes no row.
        let writes: Vec<Vec<u8>> = writes.into_iter().filter(|d| !d.is_empty()).collect();

        // No-write finalize: nothing to persist and no dependency ordering to
        // respect. Advance to Applied, then advance any waiters it unblocks.
        if writes.is_empty() {
            if self.bookkeep_applied(txn_id, Vec::new()).is_err() {
                // Marker fsync failed — fail loud, leave Committed; Apply retries.
                return SmResponse::None;
            }
            let woken = self.apply_engine.notify_applied(txn_id);
            self.bookkeep_applied_dedup(woken);
            return SmResponse::None;
        }

        // Real write-set: route through the dep-ordered apply engine. It persists
        // every key atomically (idempotent on `(txn_id, key, t)`) once every
        // dependency has applied on this replica; otherwise it parks the set.
        let mutations: Vec<ApplyMutation> = writes
            .into_iter()
            .map(|data| ApplyMutation {
                data,
                t,
                deps: deps.clone(),
            })
            .collect();
        let applied = match self.apply_engine.try_apply_writeset(txn_id, mutations) {
            Ok(applied) => applied,
            Err(e) => {
                // Storage apply failed: do NOT advance to Applied — fail loud,
                // never fake success. The coordinator gets no implicit ack and
                // the Apply can be retried.
                tracing::error!(%e, "accord: storage applier failed — not advancing to Applied");
                return SmResponse::None;
            }
        };

        if applied.is_empty() {
            // Parked behind an unapplied dependency. This txn advances to Applied
            // later, when its last dependency's apply cascades to it.
            return SmResponse::None;
        }

        // Persisted: the primary's writes plus any cascade-woken waiters. Dedup
        // by TxnId so each txn is marked Applied / fsynced exactly once.
        self.bookkeep_applied_dedup(applied);
        SmResponse::None
    }

    /// Run [`bookkeep_applied`] **once per distinct `TxnId`** in `applied`.
    ///
    /// A multi-key txn appears as N entries with the same `TxnId` (one per
    /// applied key); the post-apply bookkeeping — protocol-log marker fsync,
    /// `Applied` advance, conflict-index GC, dep-wait wake — is a per-transaction
    /// action and must fire exactly once. The first entry's data is used; later
    /// same-txn entries are skipped (their write is already durable). Each is
    /// best-effort: a fsync failure leaves that txn `Committed` for its Apply
    /// retry to re-drive (storage apply is idempotent), never falsely `Applied`.
    fn bookkeep_applied_dedup(&mut self, applied: Vec<(TxnId, Vec<u8>)>) {
        let mut bookkept: HashSet<TxnId> = HashSet::new();
        for (applied_id, applied_data) in applied {
            if bookkept.insert(applied_id) {
                let _ = self.bookkeep_applied(applied_id, applied_data);
            }
        }
    }

    /// Post-apply bookkeeping for a transaction the apply engine has **already
    /// persisted** (or a no-write finalize): persist the protocol-log marker,
    /// advance the in-memory state to `Applied`, GC the conflict index, and wake
    /// any `ReadVote` dep-wait parked on this transaction.
    ///
    /// Returns `Err(())` if the marker fsync fails — the caller must then leave
    /// the txn `Committed` (never falsely `Applied`); the per-txn Apply retry
    /// re-drives it, and because the storage applier is idempotent on
    /// `(txn_id, t)` the re-driven apply is a no-op on storage.
    fn bookkeep_applied(&mut self, txn_id: TxnId, result_data: Vec<u8>) -> Result<(), ()> {
        // Persist the protocol-log marker. Fail loud (return Err) on fsync
        // failure so the caller does not advance a non-durable apply.
        let data = format!("Applied:{}", txn_id.0.time);
        if !self.sync_writer.write_and_sync(data.as_bytes()).is_ok() {
            return Err(());
        }

        if let Some(state) = self.txn_states.get_mut(&txn_id) {
            state.apply(result_data);
        }
        self.conflict_index.mark_applied(&txn_id);

        // Wake any ReadVote dep-wait parked on a conflicting transaction reaching
        // Applied. Broadcast (all parked reads re-check their own condition under
        // the lock); see [`Self::applied_notify`].
        self.applied_notify.notify_waiters();

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Recover handler
    // -----------------------------------------------------------------------

    /// Handle a Recover message.
    ///
    /// Updates promised ballot (NOT accepted ballot) and returns current
    /// state for the transaction.
    pub fn handle_recover(
        &mut self,
        txn_id: TxnId,
        t0: Timestamp,
        ballot: BallotNumber,
    ) -> Option<TxnState> {
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));

        // NACK if recovery ballot is lower than our promised ballot.
        if ballot < (state.max_ballot_seen.0) {
            return None;
        }

        // Update promised ballot only (NOT accepted ballot).
        if ballot > (state.max_ballot_seen.0) {
            state.join_ballot(PromisedBallot(ballot));
        }

        Some(state.clone())
    }

    /// Mutable access to the conflict index (for test setup).
    pub fn conflict_index_mut(&mut self) -> &mut ConflictIndex {
        &mut self.conflict_index
    }

    /// Read access to the conflict index.
    pub fn conflict_index(&self) -> &ConflictIndex {
        &self.conflict_index
    }

    /// Evaluate whether the LWT IF condition holds for `INSERT IF NOT EXISTS`
    /// on `key` at the agreed execution timestamp `t`.
    ///
    /// Returns `true` if the row does NOT exist at `t` (the condition holds —
    /// the write should proceed). Returns `false` if a prior transaction has
    /// already been Applied for this key with a commit timestamp `<= t`.
    ///
    /// This is the linearizable read that makes Gap 4 correct: called by the
    /// `AccordHandler` when processing a `ReadVote` message, after the
    /// transaction has committed (so `t` is the agreed execution timestamp).
    ///
    /// # Note on correctness
    ///
    /// The condition is evaluated by inspecting the `txn_states` map for any
    /// transaction in `Applied` phase whose conflict-index entry covers this
    /// `key`. This is correct because:
    /// 1. The caller waits until Commit before sending ReadVote.
    /// 2. All deps (earlier transactions on this key) must have Applied before
    ///    this transaction's commit timestamp `t` was chosen.
    /// 3. Therefore, any Applied transaction visible here happened before `t`.
    ///
    /// A full storage-backed implementation would read the actual row from the
    /// `StorageEngine` at timestamp `t`.
    pub fn read_condition_holds_at(&self, key: &[u8], t: &Timestamp) -> bool {
        // Check if any Applied transaction in the conflict index covers this key
        // with a commit timestamp <= t. If so, the row exists and the INSERT IF
        // NOT EXISTS condition does NOT hold.
        //
        // We use the conflict_index to find in-flight transactions on this key,
        // then check if any have reached Applied state in txn_states.
        //
        // For transactions that have already been pruned from txn_states (via
        // prune_applied), we conservatively assume they were Applied and thus
        // the row exists — INSERT IF NOT EXISTS condition does not hold.
        let conflicting = self.conflict_index.deps_before_t0(key, t);
        if conflicting.is_empty() {
            // No conflicting transactions before `t` on this key — row does not
            // exist yet — INSERT IF NOT EXISTS condition holds.
            return true;
        }

        // Check if any conflicting transaction has reached Applied state.
        for dep_id in &conflicting {
            if let Some(state) = self.txn_states.get(dep_id) {
                if state.phase == TxnPhase::Applied {
                    // A prior transaction wrote to this key and has been applied.
                    // The row exists — INSERT IF NOT EXISTS condition does NOT hold.
                    return false;
                }
            } else {
                // Transaction not in txn_states — it was either pruned (meaning
                // it was Applied and then GC'd) or it never reached this replica.
                // If it's in the conflict index but not in txn_states, it was
                // pruned after Apply — the row exists.
                return false;
            }
        }

        // All conflicting transactions exist in txn_states but none are Applied
        // yet (they may be Committed but awaiting dep-wait). The row does not
        // yet exist in storage — condition holds.
        true
    }

    /// Remove transactions that have reached the Applied phase.
    ///
    /// Applied transactions are fully committed and their mutations have been
    /// persisted. Retaining them in `txn_states` and `committed_txns`
    /// indefinitely causes unbounded memory growth under sustained write load.
    ///
    /// Returns the number of entries removed.
    pub fn prune_applied(&mut self) -> usize {
        let before = self.txn_states.len() + self.committed_txns.len();

        // Collect TxnIds that have reached Applied phase.
        let applied_ids: Vec<TxnId> = self
            .txn_states
            .iter()
            .filter(|(_, state)| state.phase == TxnPhase::Applied)
            .map(|(id, _)| *id)
            .collect();

        for id in &applied_ids {
            self.txn_states.remove(id);
            self.committed_txns.remove(id);
            self.dep_waiters.remove(id);
        }

        // Also GC the conflict index for applied transactions.
        self.conflict_index.gc_applied();

        let after = self.txn_states.len() + self.committed_txns.len();
        before - after
    }

    /// Number of transactions currently tracked.
    pub fn txn_count(&self) -> usize {
        self.txn_states.len()
    }

    /// Number of committed transactions tracked.
    pub fn committed_count(&self) -> usize {
        self.committed_txns.len()
    }
}

// ---------------------------------------------------------------------------
// Production wiring: construct a state machine with a real storage applier
// ---------------------------------------------------------------------------

/// Build the production [`AccordStateMachine`] wired to durably persist
/// applied LWT mutations to the live
/// [`StorageEngine`](ferrosa_storage::engine::StorageEngine).
///
/// This is the single construction seam used by the cluster controller
/// (`controller/cluster.rs`). It wires an
/// [`EngineStorageApplier`](crate::accord::apply::EngineStorageApplier) so that
/// `handle_apply` writes the row to storage BEFORE marking the transaction
/// `Applied` — closing the production phantom-write gap
/// (`bug-accord-lwt-acks-phantom-write.md`), where a replica recorded
/// `(txn_id, t)` and returned `ApplyOK` while nothing was persisted.
///
/// `node_id` is the Accord node identifier, `sync_writer` the protocol-log
/// fsync writer, and `storage` the live engine handle whose write/batch path
/// the applier persists through.
pub fn build_accord_state_machine(
    node_id: u64,
    sync_writer: Arc<dyn SyncWriter>,
    storage: Arc<ferrosa_storage::engine::StorageEngine>,
) -> AccordStateMachine {
    let engine_applier = Arc::new(crate::accord::apply::EngineStorageApplier::new(
        storage.clone(),
    ));
    // When a CDC bus is attached, publish CommittedToCluster on apply so live
    // CQL SUBSCRIBE ... ON COMMITTED (and Arrow Flight) see Accord-committed
    // writes; otherwise use the engine applier directly.
    let applier: Arc<dyn crate::accord::apply::StorageApplier> = match storage.cdc_bus() {
        Some(bus) => Arc::new(crate::accord::apply::CdcPublishingApplier::new(
            engine_applier,
            bus,
        )),
        None => engine_applier,
    };
    let reader = Arc::new(crate::accord::apply::EngineStorageReader::new(storage));
    AccordStateMachine::with_applier_and_reader(node_id, sync_writer, applier, reader)
}

// Helper extension for TxnPhase to expose rank for comparison.
trait TxnPhaseExt {
    fn rank(&self) -> u8;
}

impl TxnPhaseExt for TxnPhase {
    fn rank(&self) -> u8 {
        match self {
            TxnPhase::PreAccepted => 0,
            TxnPhase::Accepted => 1,
            TxnPhase::Committed => 2,
            TxnPhase::Applied => 3,
        }
    }
}

// ===========================================================================
// Tests — 39 total
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::apply::{ApplyError, ApplyMutation, StorageApplier};
    use ferrosa_storage::accord::sync_writer::{MockSyncWriter, SyncWriteCall};
    use parking_lot::Mutex;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// A `StorageApplier` that captures the full mutation payload (not just
    /// `(txn_id, t)` like `NoopStorageApplier`) so a test can assert the exact
    /// mutation bytes that reached the storage seam.
    struct CapturingApplier {
        captured: Mutex<Vec<(TxnId, Vec<u8>, Timestamp)>>,
    }

    impl CapturingApplier {
        fn new() -> Self {
            Self {
                captured: Mutex::new(Vec::new()),
            }
        }

        /// All `(txn_id, mutation_data, t)` triples captured, in apply order.
        fn captured(&self) -> Vec<(TxnId, Vec<u8>, Timestamp)> {
            self.captured.lock().clone()
        }
    }

    impl StorageApplier for CapturingApplier {
        fn apply(&self, txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
            self.captured
                .lock()
                .push((txn_id, mutation.data, mutation.t));
            Ok(())
        }
    }

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    fn make_sm(node_id: u64) -> (AccordStateMachine, Arc<MockSyncWriter>) {
        let writer = Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(node_id, writer.clone());
        (sm, writer)
    }

    #[allow(dead_code)]
    fn make_sm_with_capacity(
        node_id: u64,
        cap: usize,
    ) -> (AccordStateMachine, Arc<MockSyncWriter>) {
        let writer = Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::with_capacity(node_id, writer.clone(), cap);
        (sm, writer)
    }

    // =======================================================================
    // Layer 2.1 — Phase transitions (8 tests)
    // =======================================================================

    /// PreAccept on new txn sets phase to PreAccepted.
    #[test]
    fn sm_preaccept_sets_pre_accepted() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        assert!(matches!(resp, SmResponse::PreAcceptOK { .. }));

        let state = sm.get_state(&txn_id).expect("state must exist");
        assert_eq!(state.phase, TxnPhase::PreAccepted);
    }

    /// Accept after PreAccept moves to Accepted.
    #[test]
    fn sm_accept_clears_preaccept() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);

        let resp = sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        assert!(matches!(resp, SmResponse::AcceptOK { .. }));

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Accepted);
    }

    /// Commit after Accept moves to Committed.
    #[test]
    fn sm_commit_clears_accepted() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Committed);
    }

    /// Apply after Committed moves to Applied.
    #[test]
    fn sm_apply_clears_committed() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        sm.handle_apply(txn_id, vec![42]);

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Applied);
    }

    /// Number of `write_and_sync` calls (one per protocol-log marker fsync).
    fn marker_fsyncs(writer: &MockSyncWriter) -> usize {
        writer
            .calls()
            .iter()
            .filter(|c| matches!(c, SyncWriteCall::Write))
            .count()
    }

    // -----------------------------------------------------------------------
    // Step 3 — multi-key apply bookkeeping. A multi-key txn yields N applied
    // entries with the SAME TxnId; the state machine must advance it to Applied
    // and fsync its marker EXACTLY ONCE (not N times — N markers corrupt the
    // protocol log), and EVERY write must reach the storage applier.
    // -----------------------------------------------------------------------

    #[test]
    fn multi_key_apply_fsyncs_applied_marker_once_per_txn() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let before = marker_fsyncs(&writer);

        // Two-key write-set for one txn.
        let resp = sm.handle_apply_writeset(txn_id, vec![b"w1".to_vec(), b"w2".to_vec()]);
        assert!(matches!(resp, SmResponse::None));

        assert_eq!(
            marker_fsyncs(&writer) - before,
            1,
            "the Applied marker must be fsynced ONCE per txn, not once per key"
        );
        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::Applied);
    }

    #[test]
    fn multi_key_apply_routes_every_write_to_the_applier() {
        let capturing = Arc::new(CapturingApplier::new());
        let writer = Arc::new(MockSyncWriter::new());
        let mut sm = AccordStateMachine::with_applier(1, writer, capturing.clone());

        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        sm.handle_apply_writeset(txn_id, vec![b"w1".to_vec(), b"w2".to_vec()]);

        let captured = capturing.captured();
        assert_eq!(
            captured.len(),
            2,
            "BOTH keys' writes must reach the storage applier — none dropped"
        );
        let datas: Vec<Vec<u8>> = captured.iter().map(|(_, d, _)| d.clone()).collect();
        assert!(datas.contains(&b"w1".to_vec()) && datas.contains(&b"w2".to_vec()));
        assert!(
            captured
                .iter()
                .all(|(id, _, t)| *id == txn_id && *t == ts(1001)),
            "every write carries the txn id and the agreed execution timestamp"
        );
    }

    #[test]
    fn single_key_handle_apply_still_fsyncs_exactly_one_marker() {
        // Regression guard: the single-key path must stay byte-identical — one
        // marker, Applied phase, just like before the writeset refactor.
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let before = marker_fsyncs(&writer);

        sm.handle_apply(txn_id, vec![42]);

        assert_eq!(marker_fsyncs(&writer) - before, 1);
        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::Applied);
    }

    /// Duplicate PreAccept returns same response.
    #[test]
    fn sm_idempotent_preaccept() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp1 = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        let resp2 = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);

        // Both should be PreAcceptOK with identical t and deps.
        match (&resp1, &resp2) {
            (
                SmResponse::PreAcceptOK {
                    t: t1, deps: d1, ..
                },
                SmResponse::PreAcceptOK {
                    t: t2, deps: d2, ..
                },
            ) => {
                assert_eq!(t1, t2, "idempotent PreAccept must return same t");
                assert_eq!(d1, d2, "idempotent PreAccept must return same deps");
            }
            _ => panic!("expected two PreAcceptOK responses"),
        }
    }

    /// Duplicate Commit is no-op.
    #[test]
    fn sm_idempotent_commit() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));

        // First commit.
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let state1 = sm.get_state(&txn_id).unwrap().clone();
        let calls_after_first = writer.calls().len();

        // Second commit (duplicate).
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let state2 = sm.get_state(&txn_id).unwrap();

        // State should not have changed.
        assert_eq!(state1.phase, state2.phase);
        assert_eq!(state1.t, state2.t);

        // No additional persist calls for the duplicate.
        assert_eq!(
            writer.calls().len(),
            calls_after_first,
            "duplicate commit should not persist again"
        );
    }

    /// PreAccept rejected if already Accepted.
    #[test]
    fn sm_reject_preaccept_after_accept() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));

        // PreAccept after Accept should be rejected.
        let resp = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        assert!(
            matches!(resp, SmResponse::Nack { .. }),
            "PreAccept after Accept must be NACKed"
        );
    }

    /// Accept with lower ballot rejected.
    #[test]
    fn sm_reject_accept_lower_ballot() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 5.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(5));

        // Accept at ballot 3 (lower) should be NACKed.
        let resp = sm.handle_accept(txn_id, t0, ts(1002), vec![], BallotNumber(3));
        assert!(
            matches!(resp, SmResponse::Nack { .. }),
            "Accept with lower ballot must be NACKed"
        );
    }

    // =======================================================================
    // Layer 2.2 — Ballot management (7 tests)
    // =======================================================================

    /// Initial PreAccept uses ballot 0.
    #[test]
    fn sm_preaccept_ballot_zero() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        let state = sm.get_state(&txn_id).unwrap();

        assert_eq!(
            state.max_ballot_seen,
            PromisedBallot::default(),
            "initial PreAccept should have ballot 0"
        );
        assert_eq!(
            state.accepted_ballot,
            AcceptedBallot::default(),
            "initial PreAccept accepted_ballot should be 0"
        );
    }

    /// Recover updates promised ballot, NOT accepted.
    #[test]
    fn sm_recover_updates_promised_only() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // PreAccept first.
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);

        // Recover with ballot 5.
        let result = sm.handle_recover(txn_id, t0, BallotNumber(5));
        assert!(result.is_some());

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(
            state.max_ballot_seen,
            PromisedBallot(BallotNumber(5)),
            "Recover should update promised ballot"
        );
        assert_eq!(
            state.accepted_ballot,
            AcceptedBallot::default(),
            "Recover must NOT update accepted ballot"
        );
    }

    /// Accept sets accepted_ballot.
    #[test]
    fn sm_accept_updates_accepted_ballot() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(3));
        let state = sm.get_state(&txn_id).unwrap();

        assert_eq!(
            state.accepted_ballot,
            AcceptedBallot(BallotNumber(3)),
            "Accept must set accepted_ballot"
        );
        assert!(
            (state.max_ballot_seen.0).0 >= 3,
            "max_ballot_seen must be >= accepted_ballot"
        );
    }

    /// Recovery after accept preserves accepted_ballot.
    #[test]
    fn sm_recover_after_accept_preserves_accepted() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 3.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(3));

        // Recover at ballot 7.
        sm.handle_recover(txn_id, t0, BallotNumber(7));

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(
            state.accepted_ballot,
            AcceptedBallot(BallotNumber(3)),
            "Recover must NOT change accepted_ballot"
        );
        assert_eq!(
            state.max_ballot_seen,
            PromisedBallot(BallotNumber(7)),
            "Recover must update promised ballot"
        );
    }

    /// Recovery picks by accepted_ballot (not max_ballot_seen).
    #[test]
    fn sm_recovery_selection_uses_accepted_ballot() {
        // This test verifies the returned state has the correct
        // accepted_ballot for the RecoveryCoordinator to use.
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 3.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(3));

        // Recover at ballot 10 — promised goes to 10 but accepted stays 3.
        let recovered_state = sm.handle_recover(txn_id, t0, BallotNumber(10)).unwrap();

        assert_eq!(
            recovered_state.accepted_ballot,
            AcceptedBallot(BallotNumber(3)),
            "recovered state must have accepted_ballot=3"
        );
        assert_eq!(
            recovered_state.max_ballot_seen,
            PromisedBallot(BallotNumber(10)),
            "recovered state must have promised=10"
        );
    }

    /// NACK response includes current promised ballot.
    #[test]
    fn sm_nack_carries_promised_ballot() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 10.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(10));

        // Accept at lower ballot 3 — should NACK.
        let resp = sm.handle_accept(txn_id, t0, ts(1002), vec![], BallotNumber(3));
        match resp {
            SmResponse::Nack { promised, .. } => {
                assert_eq!(
                    promised,
                    PromisedBallot(BallotNumber(10)),
                    "NACK must carry the current promised ballot"
                );
            }
            other => panic!("expected Nack, got {:?}", other),
        }
    }

    /// Higher ballot preempts lower in-progress.
    #[test]
    fn sm_higher_ballot_preempts_lower() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 3.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(3));

        // Accept at ballot 7 (higher) should succeed.
        let resp = sm.handle_accept(txn_id, t0, ts(1002), vec![], BallotNumber(7));
        assert!(
            matches!(resp, SmResponse::AcceptOK { ballot, .. } if ballot == BallotNumber(7)),
            "higher ballot must succeed"
        );

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.accepted_ballot, AcceptedBallot(BallotNumber(7)));
    }

    // =======================================================================
    // Layer 2.3 — Dependency sets (6 tests)
    // =======================================================================

    /// PreAccept deps come from ConflictIndex lookup.
    #[test]
    fn sm_preaccept_deps_from_conflict_index() {
        let (mut sm, _writer) = make_sm(1);
        let key = b"shared_key";

        // Register a pre-existing txn in the conflict index.
        let existing_txn = txn(2, 500);
        let entry = InFlightWrite {
            txn_id: existing_txn,
            t0: ts(500),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        let _ = sm.conflict_index_mut().register(key, entry);

        // PreAccept a new txn on the same key with t0=1000 (> 500).
        let new_txn = txn(1, 1000);
        let resp = sm.handle_preaccept(new_txn, ts(1000), key, BallotNumber(0), 0);

        match resp {
            SmResponse::PreAcceptOK { deps, .. } => {
                assert!(
                    deps.contains(&existing_txn),
                    "deps must include the conflicting txn from ConflictIndex"
                );
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }
    }

    /// PreAccept uses t0 for dep filter (not t).
    #[test]
    fn sm_preaccept_deps_use_t0_not_t() {
        let (mut sm, _writer) = make_sm(1);
        let key = b"shared_key";

        // Register txn with t0=800.
        let txn_800 = txn(2, 800);
        let entry = InFlightWrite {
            txn_id: txn_800,
            t0: ts(800),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        let _ = sm.conflict_index_mut().register(key, entry);

        // Register txn with t0=1200.
        let txn_1200 = txn(3, 1200);
        let entry = InFlightWrite {
            txn_id: txn_1200,
            t0: ts(1200),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        let _ = sm.conflict_index_mut().register(key, entry);

        // PreAccept with t0=1000. deps_before_t0 should include 800 but not 1200.
        let new_txn = txn(1, 1000);
        let resp = sm.handle_preaccept(new_txn, ts(1000), key, BallotNumber(0), 0);

        match resp {
            SmResponse::PreAcceptOK { deps, .. } => {
                assert!(deps.contains(&txn_800), "should include t0=800 < t0=1000");
                assert!(
                    !deps.contains(&txn_1200),
                    "should NOT include t0=1200 > t0=1000"
                );
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }
    }

    /// Accept uses t (final timestamp) for dep filter, not t0.
    #[test]
    fn sm_accept_deps_use_t_not_t0() {
        // Accept handler receives deps from the coordinator (already computed).
        // This test verifies the deps are stored as provided, and that the
        // Accept handler correctly uses the provided deps (which were computed
        // using t, not t0, by the coordinator).
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        let t = ts(2000); // final timestamp is different from t0

        // Deps computed by coordinator using t (not t0).
        let dep = txn(2, 1500);
        let resp = sm.handle_accept(txn_id, t0, t, vec![dep], BallotNumber(1));

        match resp {
            SmResponse::AcceptOK { deps, .. } => {
                assert!(deps.contains(&dep), "Accept must store provided deps");
            }
            other => panic!("expected AcceptOK, got {:?}", other),
        }

        let state = sm.get_state(&txn_id).unwrap();
        assert!(
            state.deps.contains(&dep),
            "stored state must contain the dep"
        );
        assert_eq!(state.t, t, "stored t must be the accept timestamp");
    }

    /// Deps union across quorum responses.
    #[test]
    fn sm_deps_union_across_quorum() {
        // Simulate two replicas returning different dep sets.
        // The coordinator (not the SM) computes the union. Here we verify
        // that the SM stores whatever deps are passed to Accept.
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let dep_a = txn(2, 500);
        let dep_b = txn(3, 600);

        // Union of deps from two replicas: {dep_a, dep_b}.
        let union_deps = vec![dep_a, dep_b];
        sm.handle_accept(txn_id, t0, ts(1001), union_deps, BallotNumber(1));

        let state = sm.get_state(&txn_id).unwrap();
        assert!(state.deps.contains(&dep_a));
        assert!(state.deps.contains(&dep_b));
        assert_eq!(state.deps.len(), 2);
    }

    /// Extra deps are safe (conservative).
    #[test]
    fn sm_deps_superset_is_safe() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Include an extra dep that doesn't actually conflict.
        let real_dep = txn(2, 500);
        let extra_dep = txn(3, 999);
        let deps = vec![real_dep, extra_dep];

        sm.handle_accept(txn_id, t0, ts(1001), deps, BallotNumber(1));

        let state = sm.get_state(&txn_id).unwrap();
        // Both deps stored — extra deps are safe (conservative ordering).
        assert!(state.deps.contains(&real_dep));
        assert!(state.deps.contains(&extra_dep));
        assert_eq!(state.deps.len(), 2);
    }

    /// Missing deps are unsafe — smaller dep set is strictly wrong.
    #[test]
    fn sm_deps_missing_is_unsafe() {
        // This documents the invariant: the coordinator must use the union.
        // If a dep is missing, the SM stores what it's given. The test shows
        // that a smaller set does not include the missing dep.
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let dep_a = txn(2, 500);
        let _dep_b = txn(3, 600); // intentionally omitted

        sm.handle_accept(txn_id, t0, ts(1001), vec![dep_a], BallotNumber(1));

        let state = sm.get_state(&txn_id).unwrap();
        assert!(state.deps.contains(&dep_a));
        assert_eq!(
            state.deps.len(),
            1,
            "missing dep_b means only 1 dep stored — this is unsafe"
        );
    }

    // =======================================================================
    // Layer 2.4 — Persist-before-reply (4 tests)
    // =======================================================================

    /// PreAccept persists to protocol log before replying.
    #[test]
    fn sm_preaccept_persists_before_reply() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        assert!(matches!(resp, SmResponse::PreAcceptOK { .. }));

        let calls = writer.calls();
        assert!(calls.len() >= 2, "must have at least Write + Fsync");
        assert_eq!(calls[0], SyncWriteCall::Write, "first call must be Write");
        assert_eq!(calls[1], SyncWriteCall::Fsync, "second call must be Fsync");
    }

    /// Accept persists before replying.
    #[test]
    fn sm_accept_persists_before_reply() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp = sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        assert!(matches!(resp, SmResponse::AcceptOK { .. }));

        let calls = writer.calls();
        assert!(calls.len() >= 2, "must have at least Write + Fsync");
        assert_eq!(calls[0], SyncWriteCall::Write);
        assert_eq!(calls[1], SyncWriteCall::Fsync);
    }

    /// Apply persists to main log before setting applied flag.
    #[test]
    fn sm_apply_persists_before_flag() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Set up a committed transaction.
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        // Clear write log to isolate the apply calls.
        writer.call_log.lock().unwrap().clear();

        sm.handle_apply(txn_id, vec![42]);

        let calls = writer.calls();
        assert!(calls.len() >= 2, "apply must Write + Fsync");
        assert_eq!(calls[0], SyncWriteCall::Write);
        assert_eq!(calls[1], SyncWriteCall::Fsync);

        // Applied flag must be set AFTER fsync.
        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Applied);
    }

    /// Crash after persist but before flag = safe (replay recovers).
    #[test]
    fn sm_crash_between_persist_and_flag() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Set up committed state.
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        // Simulate crash: fsync succeeds for persist but we "crash" before
        // setting the applied flag by making fsync fail on the apply call.
        writer.set_fsync_failure(true);

        sm.handle_apply(txn_id, vec![42]);

        // Applied flag must NOT be set because fsync "failed" (simulated crash).
        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "phase must still be Committed when fsync fails during apply"
        );

        // On recovery: re-enable fsync and retry apply.
        writer.set_fsync_failure(false);
        sm.handle_apply(txn_id, vec![42]);

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(
            state.phase,
            TxnPhase::Applied,
            "replay after crash must successfully apply"
        );
    }

    /// RED (inc1 of accord-lwt-real-data-path): the apply seam must invoke a
    /// `StorageApplier` with the mutation payload. Today `handle_apply` only
    /// writes `"Applied:{t}"` to the protocol log and NEVER writes the row —
    /// the phantom-write bug (`bug-accord-lwt-acks-phantom-write.md`). This
    /// test drives a committed txn to `handle_apply` carrying mutation bytes
    /// and a capturing applier, then asserts the applier received exactly those
    /// bytes. It does not compile today: `AccordStateMachine` has no
    /// `StorageApplier` field and no `with_applier` constructor. inc2 adds the
    /// field + wiring to turn this GREEN; inc1+inc2 are committed together.
    #[test]
    fn sm_apply_invokes_storage_applier_with_mutation() {
        let writer = Arc::new(MockSyncWriter::new());
        let applier = Arc::new(CapturingApplier::new());
        // inc2 adds `with_applier`; until then this line fails to compile,
        // which is the intended RED state for this commit.
        let mut sm = AccordStateMachine::with_applier(1, writer.clone(), applier.clone());

        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        let t = ts(1001);
        let mutation_bytes = b"real-row-mutation".to_vec();

        // Drive a full lifecycle up to Apply.
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, t, vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, t, vec![]);
        sm.handle_apply(txn_id, mutation_bytes.clone());

        // The applier must have been invoked with the mutation payload.
        let captured = applier.captured();
        assert_eq!(
            captured.len(),
            1,
            "handle_apply must invoke the StorageApplier exactly once"
        );
        let (got_txn, got_data, got_t) = &captured[0];
        assert_eq!(*got_txn, txn_id, "applier must receive the txn id");
        assert_eq!(
            *got_data, mutation_bytes,
            "applier must receive the mutation bytes (no phantom write)"
        );
        assert_eq!(
            *got_t, t,
            "applier must receive the agreed execution timestamp"
        );
    }

    /// RED (Phase 0, t_59629c9b): the replica apply path must be DEP-ORDERED.
    /// Two committed txns on the same key, B depends on A. If Apply(B) is
    /// delivered before Apply(A), B's mutation must NOT hit storage until A has
    /// applied — then both apply in dependency order. Today `handle_apply` calls
    /// the applier DIRECTLY (no dep-wait at the state-machine apply path), so B
    /// applies immediately and out of order — this test fails until the apply
    /// path routes through dep-ordered application.
    #[test]
    fn sm_apply_is_dep_ordered_parks_until_dependency_applies() {
        let writer = Arc::new(MockSyncWriter::new());
        let applier = Arc::new(CapturingApplier::new());
        let mut sm = AccordStateMachine::with_applier(1, writer.clone(), applier.clone());

        let a = txn(1, 1000);
        let b = txn(2, 2000);
        let ta = ts(1001);
        let tb = ts(2001);
        let key = b"shared-key";

        // Commit A (no deps) and B (deps = [A]) on the same key.
        sm.handle_preaccept(a, ts(1000), key, BallotNumber(0), 0);
        sm.handle_accept(a, ts(1000), ta, vec![], BallotNumber(1));
        sm.handle_commit(a, ts(1000), ta, vec![]);

        sm.handle_preaccept(b, ts(2000), key, BallotNumber(0), 0);
        sm.handle_accept(b, ts(2000), tb, vec![a], BallotNumber(1));
        sm.handle_commit(b, ts(2000), tb, vec![a]);

        // Deliver Apply(B) BEFORE Apply(A). B depends on A (not yet applied), so
        // B must PARK — its write must not reach storage out of dependency order.
        sm.handle_apply(b, b"write-b".to_vec());
        assert!(
            applier.captured().is_empty(),
            "B must not apply before its dependency A (dep-ordered apply)"
        );

        // Now apply A → the cascade must then apply B, in dependency order.
        sm.handle_apply(a, b"write-a".to_vec());
        let ids: Vec<_> = applier.captured().iter().map(|(id, _, _)| *id).collect();
        assert_eq!(
            ids,
            vec![a, b],
            "after A applies, A then B must apply in dependency order"
        );
    }

    /// Commit must persist (fsync) BEFORE advancing committed-visible state.
    /// A disk failure during commit must NOT mark the txn Committed, must NOT
    /// add it to the committed set, and must NOT wake its dependency waiters —
    /// otherwise a non-durable commit could unblock dependents, a phantom-commit
    /// hazard on the commit path. Mirrors `sm_crash_between_persist_and_flag`
    /// for the apply path.
    #[test]
    fn sm_crash_during_commit_fsync_does_not_advance() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Advance to Accepted (ready to commit).
        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));

        // A dependency waiter parked on this txn must NOT be woken by a
        // non-durable commit.
        sm.dep_waiters.insert(txn_id, vec![txn(2, 2000)]);

        // Inject disk failure for the commit's durable write.
        writer.set_fsync_failure(true);
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        // Commit must NOT have advanced: not durable => not observable.
        let state = sm.get_state(&txn_id).unwrap();
        assert_ne!(
            state.phase,
            TxnPhase::Committed,
            "fsync failed during commit: phase must NOT be Committed (non-durable)"
        );
        assert_eq!(
            sm.committed_count(),
            0,
            "fsync failed during commit: committed set must be empty"
        );
        assert!(
            sm.last_notified.is_empty(),
            "fsync failed during commit: dependency waiters must NOT be woken"
        );

        // On recovery: re-enable fsync and retry commit.
        writer.set_fsync_failure(false);
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "retry after disk heal must commit"
        );
        assert_eq!(
            sm.committed_count(),
            1,
            "healed commit must be in committed set"
        );
        assert_eq!(
            sm.last_notified,
            vec![txn(2, 2000)],
            "healed commit must wake the parked dependency waiter"
        );
    }

    // =======================================================================
    // Layer 3.1 — PreAccept handler (6 tests)
    // =======================================================================

    /// No conflict: t unchanged, deps from local ConflictIndex.
    #[test]
    fn preaccept_no_conflict_fast_path() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp = sm.handle_preaccept(txn_id, t0, b"unique_key", BallotNumber(0), 0);

        match resp {
            SmResponse::PreAcceptOK { t, deps, .. } => {
                assert_eq!(t, t0, "no conflict: t must equal t0");
                assert!(deps.is_empty(), "no conflict: deps must be empty");
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }
    }

    /// Conflict: t bumped past conflicting t0.
    #[test]
    fn preaccept_conflict_bumps_timestamp() {
        let (mut sm, _writer) = make_sm(1);
        let key = b"contested_key";

        // Register existing txn with t0=1500 on the same key.
        let existing = txn(2, 1500);
        let entry = InFlightWrite {
            txn_id: existing,
            t0: ts(1500),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        let _ = sm.conflict_index_mut().register(key, entry);

        // PreAccept new txn with t0=1000 (lower than existing t0=1500).
        let new_txn = txn(1, 1000);
        let resp = sm.handle_preaccept(new_txn, ts(1000), key, BallotNumber(0), 0);

        match resp {
            SmResponse::PreAcceptOK { t, .. } => {
                assert!(
                    t > ts(1500),
                    "t must be bumped past conflicting t0=1500, got {:?}",
                    t
                );
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }
    }

    /// New txn registered in ConflictIndex after PreAccept.
    #[test]
    fn preaccept_registers_in_conflict_index() {
        let (mut sm, _writer) = make_sm(1);
        let key = b"new_key";
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        assert!(
            sm.conflict_index().is_empty(),
            "conflict index should start empty"
        );

        sm.handle_preaccept(txn_id, t0, key, BallotNumber(0), 0);

        assert!(
            !sm.conflict_index().is_empty(),
            "txn must be registered in conflict index after PreAccept"
        );
        assert!(
            sm.conflict_index().max_conflicting_timestamp(key).is_some(),
            "key must be findable in conflict index"
        );
    }

    /// Higher promised ballot NACKs PreAccept.
    #[test]
    fn preaccept_nack_if_higher_ballot_seen() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept at ballot 5 (sets max_ballot_seen >= 5).
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(5));

        // PreAccept at ballot 0 should be NACKed.
        let resp = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        assert!(
            matches!(resp, SmResponse::Nack { .. }),
            "PreAccept with lower ballot must be NACKed"
        );
    }

    /// Exact same PreAccept returns cached response.
    #[test]
    fn preaccept_idempotent_on_duplicate() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        let resp1 = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        let calls_after_first = writer.calls().len();

        let resp2 = sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);

        match (&resp1, &resp2) {
            (
                SmResponse::PreAcceptOK {
                    t: t1, deps: d1, ..
                },
                SmResponse::PreAcceptOK {
                    t: t2, deps: d2, ..
                },
            ) => {
                assert_eq!(t1, t2);
                assert_eq!(d1, d2);
            }
            _ => panic!("both responses must be PreAcceptOK"),
        }

        // Idempotent call should not trigger additional persist.
        // (The cached path returns early before persisting.)
        let calls_after_second = writer.calls().len();
        assert_eq!(
            calls_after_first, calls_after_second,
            "idempotent PreAccept must not re-persist"
        );
    }

    /// Different epoch triggers slow path (bumped timestamp).
    #[test]
    fn preaccept_epoch_mismatch() {
        let (mut sm, _writer) = make_sm(1);
        let key = b"epoch_key";

        // Register existing txn with epoch=0.
        let existing = txn(2, 500);
        let entry = InFlightWrite {
            txn_id: existing,
            t0: ts(500),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        let _ = sm.conflict_index_mut().register(key, entry);

        // PreAccept with epoch=1 (mismatch from epoch=0 on existing txns).
        // The epoch mismatch means the coordinator's proposed t0 may be
        // from a different configuration — this triggers conflict detection.
        let new_txn = txn(1, 1000);
        let mut t0 = ts(1000);
        t0.epoch = 1; // Different epoch

        let resp = sm.handle_preaccept(new_txn, t0, key, BallotNumber(0), 1);

        // Should still get a valid response (PreAcceptOK).
        assert!(
            matches!(resp, SmResponse::PreAcceptOK { .. }),
            "epoch mismatch should still produce PreAcceptOK"
        );
    }

    // =======================================================================
    // Layer 3.2 — Accept handler (5 tests)
    // =======================================================================

    /// Normal Accept sets accepted_ballot and deps.
    #[test]
    fn accept_normal() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        let t = ts(1001);
        let dep = txn(2, 500);

        let resp = sm.handle_accept(txn_id, t0, t, vec![dep], BallotNumber(1));

        match resp {
            SmResponse::AcceptOK { ballot, deps, .. } => {
                assert_eq!(ballot, BallotNumber(1));
                assert!(deps.contains(&dep));
            }
            other => panic!("expected AcceptOK, got {:?}", other),
        }

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Accepted);
        assert_eq!(state.accepted_ballot, AcceptedBallot(BallotNumber(1)));
        assert_eq!(state.t, t);
    }

    /// Lower ballot Accept NACKed.
    #[test]
    fn accept_nack_lower_ballot() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // First accept at ballot 5.
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(5));

        // Lower ballot (3) should be NACKed.
        let resp = sm.handle_accept(txn_id, t0, ts(1002), vec![], BallotNumber(3));
        assert!(matches!(resp, SmResponse::Nack { .. }));
    }

    /// Accept after PreAccept works.
    #[test]
    fn accept_after_preaccept() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::PreAccepted);

        let resp = sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        assert!(matches!(resp, SmResponse::AcceptOK { .. }));
        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::Accepted);
    }

    /// Accept without PreAccept (recovery path) works.
    #[test]
    fn accept_skipped_preaccept() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        // Accept directly without PreAccept — this happens in recovery.
        let resp = sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        assert!(matches!(resp, SmResponse::AcceptOK { .. }));

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.phase, TxnPhase::Accepted);
    }

    /// Accept after Commit is no-op (returns AcceptOK with committed state).
    #[test]
    fn accept_after_commit() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(1001), vec![]);

        // Accept after commit should be a no-op.
        let resp = sm.handle_accept(txn_id, t0, ts(1002), vec![], BallotNumber(2));
        match resp {
            SmResponse::AcceptOK { .. } => {
                // Correct: no-op returns AcceptOK with committed state.
            }
            other => panic!("expected AcceptOK (no-op), got {:?}", other),
        }

        // Phase must still be Committed (not regressed).
        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::Committed);
    }

    // =======================================================================
    // Layer 3.3 — Commit handler (3 tests)
    // =======================================================================

    /// Commit locks the final timestamp.
    #[test]
    fn commit_sets_final_timestamp() {
        let (mut sm, _writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        let t_final = ts(2000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1500), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, t_final, vec![]);

        let state = sm.get_state(&txn_id).unwrap();
        assert_eq!(state.t, t_final, "commit must lock the final timestamp");
        assert_eq!(state.phase, TxnPhase::Committed);
    }

    /// Duplicate Commit is idempotent.
    #[test]
    fn commit_idempotent() {
        let (mut sm, writer) = make_sm(1);
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);

        sm.handle_preaccept(txn_id, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(1001), vec![], BallotNumber(1));

        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let calls_after_first = writer.calls().len();

        sm.handle_commit(txn_id, t0, ts(1001), vec![]);
        let calls_after_second = writer.calls().len();

        assert_eq!(
            calls_after_first, calls_after_second,
            "duplicate commit must not re-persist"
        );

        assert_eq!(sm.get_state(&txn_id).unwrap().phase, TxnPhase::Committed);
    }

    /// Commit wakes transactions waiting on deps.
    #[test]
    fn commit_wakes_dep_waiters() {
        let (mut sm, _writer) = make_sm(1);
        let dep_txn = txn(1, 1000);
        let waiter_txn = txn(2, 2000);
        let t0 = ts(1000);

        // Set up the dependency transaction.
        sm.handle_preaccept(dep_txn, t0, b"key1", BallotNumber(0), 0);
        sm.handle_accept(dep_txn, t0, ts(1001), vec![], BallotNumber(1));

        // Register a waiter.
        sm.register_dep_waiter(dep_txn, waiter_txn);

        // Commit the dependency.
        sm.handle_commit(dep_txn, t0, ts(1001), vec![]);

        let notified = sm.last_notified_waiters();
        assert_eq!(notified.len(), 1);
        assert_eq!(
            notified[0], waiter_txn,
            "commit must wake waiting transactions"
        );
    }

    #[test]
    fn prune_applied_removes_completed_transactions() {
        let (mut sm, _writer) = make_sm(1);
        let tid = txn(1, 100);
        let t0 = ts(100);

        // Drive through full lifecycle: PreAccept -> Accept -> Commit -> Apply
        sm.handle_preaccept(tid, t0, b"key", BallotNumber(0), 0);
        sm.handle_accept(tid, t0, ts(101), vec![], BallotNumber(0));
        sm.handle_commit(tid, t0, ts(101), vec![]);
        sm.handle_apply(tid, b"result".to_vec());

        assert_eq!(sm.txn_count(), 1, "transaction should be tracked");
        assert_eq!(sm.committed_count(), 1, "committed set should have entry");

        let pruned = sm.prune_applied();
        assert!(pruned > 0, "should prune at least one entry");
        assert_eq!(sm.txn_count(), 0, "txn_states should be empty after prune");
        assert_eq!(
            sm.committed_count(),
            0,
            "committed_txns should be empty after prune"
        );
    }

    #[test]
    fn prune_applied_preserves_in_flight_transactions() {
        let (mut sm, _writer) = make_sm(1);
        let applied = txn(1, 100);
        let in_flight = txn(1, 200);
        let t0 = ts(100);

        // applied goes through full lifecycle.
        sm.handle_preaccept(applied, t0, b"k1", BallotNumber(0), 0);
        sm.handle_accept(applied, t0, ts(101), vec![], BallotNumber(0));
        sm.handle_commit(applied, t0, ts(101), vec![]);
        sm.handle_apply(applied, b"done".to_vec());

        // in_flight is only preaccepted (still in consensus).
        sm.handle_preaccept(in_flight, ts(200), b"k2", BallotNumber(0), 0);

        assert_eq!(sm.txn_count(), 2);

        sm.prune_applied();

        assert_eq!(
            sm.txn_count(),
            1,
            "only the applied transaction should be pruned"
        );
        assert!(
            sm.get_state(&in_flight).is_some(),
            "in-flight transaction must survive pruning"
        );
        assert!(
            sm.get_state(&applied).is_none(),
            "applied transaction must be pruned"
        );
    }
}

// ===========================================================================
// Production-wiring tests — the state machine built for production
// (`build_accord_state_machine`) MUST durably persist an applied LWT mutation
// to the live StorageEngine. This is the headline proof that the PRODUCTION
// construction path (controller/cluster.rs) is not a phantom write: a replica
// that returns ApplyOK must have actually written the row.
//
// These tests exercise the SAME factory the controller calls (not just the
// apply unit seam covered in apply.rs), driving a real serialized commit-log
// Mutation through the full PreAccept -> Accept -> Commit -> Apply lifecycle
// and then reading the row back through the engine.
// ===========================================================================

#[cfg(test)]
mod production_wiring_tests {
    use super::*;
    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;
    use ferrosa_storage::engine::StorageEngine;
    use ferrosa_storage::{Mutation, StorageEngineConfig, TableId};

    const KS: &str = "lwt_ks";
    const TABLE: &str = "lwt_table";

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
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

    /// Serialize a commit-log `Mutation` — the apply-payload wire format the
    /// production applier decodes.
    fn serialized_mutation(key: DecoratedKey, value: &[u8], cell_ts: i64) -> Vec<u8> {
        let m = Mutation::new(
            KS.to_string(),
            TABLE.to_string(),
            key,
            vec![make_row(value, cell_ts)],
            cell_ts,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        buf
    }

    fn read_cell0(engine: &StorageEngine, key: &DecoratedKey) -> Option<Vec<u8>> {
        let partition = engine.read(&TableId::new(KS, TABLE), key).unwrap()?;
        let row = partition.rows.first()?;
        row.cells.first().and_then(|(_, c)| c.value.clone())
    }

    /// RED (inc6 / task #31): the PRODUCTION state machine built by
    /// `build_accord_state_machine` must durably persist an applied LWT to the
    /// live engine. With the old wiring (`AccordStateMachine::new`, a
    /// `NoopStorageApplier`) `handle_apply` records `(txn_id, t)` and returns
    /// without writing the row — the engine read below returns `None` and this
    /// assertion FAILS. With the engine-backed applier it persists and the row
    /// is readable: the phantom write is gone.
    #[test]
    fn production_state_machine_persists_applied_lwt_to_engine() {
        let (engine, _dir) = make_engine();

        // Build EXACTLY as the controller does: the production factory.
        let writer = Arc::new(MockSyncWriter::new());
        let mut sm = build_accord_state_machine(7, writer, engine.clone());

        let key = make_key("pk-prod");
        let txn_id = txn(1, 1000);
        let t0 = ts(1000);
        let t = ts(1001);
        let payload = serialized_mutation(key.clone(), b"durable", 1001);

        // Full lifecycle to Apply, carrying the real serialized mutation.
        sm.handle_preaccept(txn_id, t0, key.key.as_bytes(), BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, t, vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, t, vec![]);
        sm.handle_apply(txn_id, payload);

        // The replica returned (would return) ApplyOK only after marking Applied;
        // Applied is reached ONLY if the engine write succeeded. Prove the row is
        // actually durable and readable — not a phantom write.
        assert_eq!(
            sm.get_state(&txn_id).map(|s| s.phase),
            Some(TxnPhase::Applied),
            "production apply must reach Applied (engine write succeeded)"
        );
        assert_eq!(
            read_cell0(&engine, &key).as_deref(),
            Some(b"durable".as_slice()),
            "applied LWT must be durably persisted + readable via the engine — \
             the production phantom write must be gone"
        );
    }

    /// Fail loud: if the production applier cannot persist (target table not
    /// registered), `handle_apply` must NOT advance to Applied — so the replica
    /// does NOT emit a spurious ApplyOK for a write that never landed.
    #[test]
    fn production_apply_failure_does_not_fake_applied() {
        let (engine, _dir) = make_engine();
        let writer = Arc::new(MockSyncWriter::new());
        let mut sm = build_accord_state_machine(7, writer, engine.clone());

        let key = make_key("pk-missing-table");
        let txn_id = txn(2, 2000);
        let t0 = ts(2000);
        let t = ts(2001);

        // Mutation targets a table that was never registered on the engine.
        let m = Mutation::new(
            KS.to_string(),
            "unregistered_table".to_string(),
            key.clone(),
            vec![make_row(b"x", 2001)],
            2001,
        );
        let mut payload = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut payload);

        sm.handle_preaccept(txn_id, t0, key.key.as_bytes(), BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, t, vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, t, vec![]);
        sm.handle_apply(txn_id, payload);

        assert_ne!(
            sm.get_state(&txn_id).map(|s| s.phase),
            Some(TxnPhase::Applied),
            "a failed engine apply must NOT mark the txn Applied (no fake ApplyOK)"
        );
    }
}
