//! RPC handlers for inbound Accord consensus messages.
//!
//! Each handler deserializes the incoming message, dispatches to the local
//! `AccordStateMachine` (via the shared `AccordState`), and returns the
//! appropriate response message.
//!
//! These are registered in `controller/cluster.rs` alongside Raft and
//! data-path handlers.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ferrosa_common::accord::{Timestamp, TxnId, TxnPhase};

use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use super::state_machine::{execute_apply_durability, AccordStateMachine, ApplyStep, SmResponse};
use super::wire::{
    AcceptOkPayload, AcceptPayload, ApplyOkPayload, ApplyPayload, ApplyV2Payload, CommitOkPayload,
    CommitPayload, PreAcceptOkPayload, PreAcceptPayload, PreAcceptV2Payload, ReadVoteOkPayload,
    ReadVotePayload, RecoverPayload,
};

/// Shared mutable access to the Accord state machine.
///
/// Wrapped in a Mutex because the state machine is single-threaded
/// (Accord's per-shard model). In production this would be sharded
/// by token range; for now a single lock suffices.
pub type AccordState = Arc<parking_lot::Mutex<AccordStateMachine>>;

/// A shared, swappable slot that publishes a node's live [`AccordState`] from
/// the cluster controller (which creates it during formation) to the session
/// layer (whose transaction committer needs it to cast the coordinator's own
/// PreAccept vote locally — a node is never in its own peer map).
/// PostgreSQL MVCC observers registered before formation are retained here and
/// installed before the replica state becomes visible to message handlers.
///
/// The session's `SessionCore` is built *before* the controller forms the
/// cluster and creates the state, so the two cannot share a plain `AccordState`
/// at construction time. This slot is created empty up front, handed to both
/// sides, and filled by the controller at formation; the committer reads it on
/// demand. Empty until formation (and in standalone/tests), in which case the
/// committer falls back to remote-only votes (correct when peers are the
/// replicas).
#[derive(Clone)]
pub struct AccordStateSlot {
    inner: Arc<AccordStateSlotInner>,
}

struct AccordStateSlotInner {
    state: arc_swap::ArcSwapOption<parking_lot::Mutex<AccordStateMachine>>,
    postgres_mvcc_observers:
        parking_lot::Mutex<Vec<Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>>>,
}

impl AccordStateSlot {
    pub fn load_full(&self) -> Option<AccordState> {
        self.inner.state.load_full()
    }

    /// Register an MVCC observer on the published state, or retain it until
    /// cluster formation publishes the local replica state.
    pub fn register_postgres_mvcc_observer(
        &self,
        observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
    ) -> Result<(), String> {
        let mut observers = self.inner.postgres_mvcc_observers.lock();
        if observers
            .iter()
            .any(|registered| Arc::ptr_eq(registered, &observer))
        {
            return Ok(());
        }
        if let Some(state) = self.load_full() {
            state
                .lock()
                .register_postgres_mvcc_observer(observer.clone())?;
        }
        observers.push(observer);
        Ok(())
    }

    fn publish(&self, state: AccordState) -> Result<(), String> {
        let observers = self.inner.postgres_mvcc_observers.lock();
        for observer in observers.iter() {
            state
                .lock()
                .register_postgres_mvcc_observer(observer.clone())?;
        }
        self.inner.state.store(Some(state));
        Ok(())
    }
}

/// An empty [`AccordStateSlot`] — the initial state before the controller
/// publishes this node's live `AccordState`.
pub fn empty_accord_state_slot() -> AccordStateSlot {
    AccordStateSlot {
        inner: Arc::new(AccordStateSlotInner {
            state: arc_swap::ArcSwapOption::empty(),
            postgres_mvcc_observers: parking_lot::Mutex::new(Vec::new()),
        }),
    }
}

/// Publish `state` into `slot` and return it, so the node's [`AccordHandler`]
/// and the session-layer committer observe the **same** `AccordStateMachine`
/// instance. The controller calls this once during cluster formation, then
/// serves the returned state from its handler — guaranteeing the coordinator's
/// local self-vote uses exactly the state its remote peers see.
pub fn publish_accord_state(
    slot: &AccordStateSlot,
    state: AccordState,
) -> Result<AccordState, String> {
    slot.publish(state.clone())?;
    Ok(state)
}

// ---------------------------------------------------------------------------
// AccordHandler — single handler for all 6 inbound Accord message types
// ---------------------------------------------------------------------------

/// Handles all inbound Accord consensus messages by dispatching to the
/// local `AccordStateMachine`.
pub struct AccordHandler {
    state: AccordState,
    local_node_id: u64,
}

/// Re-exported from [`crate::accord::state_machine`], where both Accord bounds
/// and their resolvers live. Kept here as the operator-facing name.
pub use crate::accord::state_machine::DEFAULT_TXN_TIMEOUT;

/// Per-iteration cap on a single `notified()` wait. A coalesced/lost broadcast
/// wake (the apply fired between our unlock and re-arming the notify) costs at
/// most this long before the loop re-checks the condition under the lock again,
/// so the wait can never hang past `READ_DEP_WAIT_TIMEOUT`.
const READ_DEP_WAIT_POLL: std::time::Duration = std::time::Duration::from_millis(25);

impl AccordHandler {
    pub fn new(state: AccordState, local_node_id: u64) -> Self {
        Self {
            state,
            local_node_id,
        }
    }

    /// Apply a decoded multi-key write-set on this replica and ack, or `None` if the
    /// transaction could not be brought to `Applied`.
    ///
    /// Shared by the bincode `AccordApplyV2` arm and the Cap'n Proto
    /// `AccordApplyV2Capnp` arm so both apply the SAME way — the wire encoding is a
    /// transport detail, never an apply-behaviour fork.
    async fn apply_writeset_and_ack(&self, txn_id: TxnId, writes: Vec<Vec<u8>>) -> Option<Message> {
        let apply_status = self.apply_writeset_durably(txn_id, writes).await?;
        if !matches!(apply_status, SmResponse::NoWriteFinalized)
            && !await_txn_applied(&self.state, txn_id).await
        {
            return None;
        }
        let ok = ApplyOkPayload {
            txn_id,
            from: self.local_node_id,
        };
        let bytes = bincode::serialize(&ok).ok()?;
        Some(Message::AccordApplyOK(Bytes::from(bytes)))
    }

    /// Run one inbound apply to completion **without holding the state lock across
    /// the blocking durable write**.
    ///
    /// Phase 1 decides under the lock and reserves the txn; phase 2 runs the storage
    /// batch (and its commit-log fsync) on the blocking pool with the lock RELEASED;
    /// phase 3 marks the txn `Applied` under the lock. Every other inbound Accord arm
    /// takes only the state lock, so none of them is serialized behind an in-flight
    /// apply's fsync — which is what produced `orphan RPC response ... no pending
    /// caller`. Durability ordering is unchanged: the ack is still sent only after
    /// `write_atomic_batch` returned and the txn reached `Applied`.
    async fn apply_writeset_durably(
        &self,
        txn_id: TxnId,
        writes: Vec<Vec<u8>>,
    ) -> Option<SmResponse> {
        let step =
            on_state_machine(&self.state, move |sm| sm.begin_apply_owned(txn_id, writes)).await?;
        match step {
            ApplyStep::Done(resp) => Some(resp),
            ApplyStep::Durable(work) => {
                let outcome = run_blocking(move || execute_apply_durability(*work)).await?;
                on_state_machine(&self.state, move |sm| sm.finish_apply(txn_id, outcome)).await
            }
        }
    }
}

/// Run `f` against the state machine on tokio's blocking pool.
///
/// Every protocol step that persists (PreAccept, Accept, Commit, Apply) fsyncs
/// the protocol log before it returns, and the storage read of a read-vote hits
/// disk. Both happen while the state machine's `parking_lot` mutex is held, so
/// doing them on an async worker blocks that worker for the length of the
/// fsync and blocks every other worker that reaches for the mutex. On a slow
/// disk that froze the whole runtime: heartbeats and Raft stopped with it, and
/// peers' Accord RPCs timed out. On the blocking pool the same wait parks a
/// blocking thread instead, and the runtime keeps serving.
///
/// A panic inside `f` is resumed on the caller. `None` means the task was
/// cancelled (the runtime is shutting down); it is logged and the caller sends
/// no reply.
pub(crate) async fn on_state_machine<R, F>(state: &AccordState, f: F) -> Option<R>
where
    R: Send + 'static,
    F: FnOnce(&mut AccordStateMachine) -> R + Send + 'static,
{
    let state = Arc::clone(state);
    match tokio::task::spawn_blocking(move || f(&mut state.lock())).await {
        Ok(result) => Some(result),
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => {
            tracing::error!(error = %e, "accord: state-machine task was cancelled; no reply");
            None
        }
    }
}

/// Run `f` on tokio's blocking pool WITHOUT the state-machine lock.
///
/// The durable half of an apply (`write_atomic_batch` → commit-log fsync) must not
/// run on an async worker (CL-21) and must not hold the state lock, or every
/// inbound Accord arm — which takes that same lock — is serialized behind one apply
/// and its replies arrive after their callers gave up (`orphan RPC response`). `f`
/// holds the apply gate itself, so the dep-ordered engine is still single-writer.
///
/// A panic inside `f` is resumed on the caller. `None` means the task was cancelled
/// (the runtime is shutting down); it is logged and the caller sends no reply.
async fn run_blocking<R, F>(f: F) -> Option<R>
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => Some(result),
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => {
            tracing::error!(error = %e, "accord: blocking task was cancelled; no reply");
            None
        }
    }
}

/// Block until every conflicting transaction ordered before `t` (`t0 < t`) has
/// reached `Applied` on the replica behind `state`.
///
/// Returns `true` if all conflicts applied (a read-at-`t` may now proceed
/// linearizably), `false` if the pending set stayed FROZEN for `timeout` (the
/// caller MUST ABSTAIN — never read stale). Shared by the inbound `AccordRead`
/// handler (remote replicas) and by the coordinator's own local read-vote (its
/// self-send Apply is unreachable, so it reads its local state machine directly).
///
/// # The bound is a stall backstop, not a wait budget
///
/// `timeout` does not bound how long this read may wait for a dependency that is
/// still being resolved; it bounds how long the wait may make NO PROGRESS. The
/// wait decides on EVIDENCE OF PROGRESS — the read's dependency set *and each
/// dependency's protocol phase* ([`AccordStateMachine::dependency_wait_signature`])
/// plus the local engine's completed-resolution watermark
/// ([`AccordStateMachine::applied_epoch`]) — and restarts the backstop clock
/// every time either moves. A snapshot barrier queued behind a live,
/// merely-behind engine (the PostgreSQL `SERIALIZABLE` batch on the one global
/// barrier key, where each predecessor's apply drains the set) therefore keeps
/// extending its clock instead of abstaining, however long the total drain
/// takes. The bound fires only when the set, every dependency's phase, AND the
/// engine watermark are all frozen for a full interval — the signature of a
/// genuinely stuck dependency, from which the read can make no further progress
/// and must fail loud rather than hang or read stale. Widening the budget is
/// therefore never the fix; the engine's own progress is.
///
/// # Why this still terminates
///
/// Every reset requires a completed resolution or a forward phase advance. The
/// dependency set can only be added to by a transaction with an even-earlier
/// `t0`, and every dependency is either `Committed` (its execution timestamp is
/// locked in the commit log, so the engine's idempotent Apply retry drives it to
/// `Applied`) or is finalized out of the set by the protocol; the set therefore
/// drains. A wait that is *not* draining has, by construction, no reset source,
/// so the backstop fires within `timeout`. The only way to wait indefinitely is
/// for the engine to keep resolving unrelated work forever while never resolving
/// this dependency — and a dependency that a live engine cannot resolve is
/// exactly the poisoned/absent case the phase detail log names, which the
/// protocol's finalize paths (CL-23/CL-36) exist to clear.
///
/// # Deadlock safety
///
/// The `parking_lot` state lock is acquired only to *compute* the pending set
/// and to grab the apply-notify handle, on the blocking pool (see the private
/// helper `on_state_machine`), then released BEFORE every `.await`.
/// `handle_apply` (which fires the notify that unblocks us) takes the same lock,
/// so holding it across the await would deadlock.
pub async fn await_conflicting_deps_applied(state: &AccordState, key: &[u8], t: Timestamp) -> bool {
    await_conflicting_deps_applied_within(
        state,
        key,
        t,
        crate::accord::state_machine::configured_barrier_timeout(),
    )
    .await
}

/// [`await_conflicting_deps_applied`] with an explicit bound.
///
/// The bound is a parameter rather than a process-global read so a test can
/// exercise expiry in microseconds without mutating the environment (which is
/// racy under parallel tests).
pub(crate) async fn await_conflicting_deps_applied_within(
    state: &AccordState,
    key: &[u8],
    t: Timestamp,
    timeout: std::time::Duration,
) -> bool {
    // The evidence of progress: the dependency-set SIGNATURE (what the read is
    // waiting on, and each dependency's protocol phase) and the engine's
    // `applied_epoch` (the local apply path completed a resolution). Both are
    // `None` until the first poll; `stall_deadline` is the BACKSTOP — pushed
    // forward whenever either moved, reached only when BOTH have been frozen for
    // the whole `timeout`.
    let mut last_signature: Option<Vec<(TxnId, u8)>> = None;
    let mut last_epoch: Option<u64> = None;
    let mut stall_deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Compute the pending set and grab the notify UNDER the lock, then drop
        // the lock before awaiting. The future only enrolls on first poll, so a
        // wake fired between unlock and poll could be missed; the bounded poll
        // timeout below makes such a missed wake self-correcting (the loop
        // re-checks the condition under the lock) rather than a hang.
        let owned_key = key.to_vec();
        let pending = on_state_machine(state, move |sm| {
            let signature = sm.dependency_wait_signature(&owned_key, &t);
            if signature.is_empty() {
                None
            } else {
                Some((signature, sm.applied_epoch(), sm.applied_notify()))
            }
        })
        .await;
        let (signature, epoch, notify) = match pending {
            Some(None) => return true,
            Some(Some(sample)) => sample,
            // Cancelled at shutdown (already logged): abstain.
            None => return false,
        };

        let now = tokio::time::Instant::now();
        if last_signature.as_ref() != Some(&signature) || last_epoch != Some(epoch) {
            // A dependency advanced (or the set changed), or the local engine
            // completed a resolution since the last poll: real progress toward
            // this read. Restart the stall clock so a live-but-behind engine is
            // never mistaken for a stuck dependency.
            last_signature = Some(signature);
            last_epoch = Some(epoch);
            stall_deadline = now + timeout;
        } else if now >= stall_deadline {
            // Frozen for the whole backstop: neither the dependency set (with its
            // phases) nor the apply path moved. Name the poison. The bare timeout
            // says only "dependencies were not applied locally"; without each
            // stuck transaction's phase and agreed t, a PreAccepted orphan and a
            // Committed-but-unapplied txn look the same from the log, and they
            // need different fixes.
            let owned_key = key.to_vec();
            let stuck =
                on_state_machine(state, move |sm| sm.pending_conflicts_detail(&owned_key, &t))
                    .await;
            tracing::error!(
                pending = ?stuck,
                "accord: ReadVote dep-wait stalled for {:?} with the dependency set and the \
                 apply watermark both frozen — abstaining (fail-loud)",
                timeout
            );
            return false;
        }

        // Wait for the next apply (broadcast) or the per-iteration poll cap,
        // whichever comes first, but never past the current stall deadline.
        let wait = READ_DEP_WAIT_POLL.min(stall_deadline.saturating_duration_since(now));
        let _ = tokio::time::timeout(wait, notify.notified()).await;
        // Loop: re-check the pending set under the lock.
    }
}

/// Wait until one exact transaction reaches `Applied` on this replica.
///
/// Apply may park behind an ordered dependency, so calling `handle_apply*` is
/// not itself a durable Apply acknowledgement. This bounded wait is shared by
/// inbound handlers and the coordinator's local self-apply path.
pub async fn await_txn_applied(state: &AccordState, txn_id: TxnId) -> bool {
    await_txn_applied_within(
        state,
        txn_id,
        crate::accord::state_machine::configured_txn_timeout(),
    )
    .await
}

/// [`await_txn_applied`] with an explicit bound (see
/// [`await_conflicting_deps_applied_within`] for why the bound is a parameter).
pub(crate) async fn await_txn_applied_within(
    state: &AccordState,
    txn_id: TxnId,
    timeout: std::time::Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let observed = on_state_machine(state, move |sm| {
            // Read the applied-and-pruned tombstone in the SAME critical section as the
            // live state: a prune landing between two separate reads would otherwise drop
            // a durably-applied txn on the floor and refuse its ack.
            let applied_pruned = sm.was_applied_and_pruned(&txn_id);
            let live = sm.get_state(&txn_id).map(|txn| {
                (
                    txn.phase,
                    txn.deps.len(),
                    txn.result.as_ref().map_or(0, Vec::len),
                    sm.applied_notify(),
                )
            });
            (applied_pruned, live)
        })
        .await;
        let (applied_pruned, live) = match observed {
            Some(observed) => observed,
            // Cancelled at shutdown (already logged): refuse the ack.
            None => return false,
        };
        let (phase, dependency_count, result_bytes, notify) = match live {
            Some(txn) => txn,
            // This replica durably applied the txn and then pruned it. The apply is still
            // true — the missing `TxnState` is bookkeeping, not a reason to refuse work
            // this replica already did. Ack it.
            None if applied_pruned => return true,
            None => {
                tracing::error!(
                    txn_id = ?txn_id,
                    "accord: Apply target is absent from local state — refusing ApplyOK"
                );
                return false;
            }
        };
        if phase == TxnPhase::Applied {
            return true;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            // Name the poison (see `AccordStateMachine::dep_detail`): a dep this
            // replica never registered needs a different fix from one that is
            // registered here but stuck.
            let detail = on_state_machine(state, move |sm| sm.dep_detail(&txn_id)).await;
            let phase = Some(phase);
            tracing::error!(
                txn_id = ?txn_id,
                ?phase,
                dependency_count,
                result_bytes,
                deps = ?detail,
                "accord: Apply timed out after {:?} waiting for ordered dependencies — refusing ApplyOK",
                timeout
            );
            return false;
        }

        let wait = READ_DEP_WAIT_POLL.min(deadline - now);
        let _ = tokio::time::timeout(wait, notify.notified()).await;
    }
}

#[async_trait]
impl RpcHandler for AccordHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::AccordPreAccept(b) => {
                let payload: PreAcceptPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordPreAccept: deserialize failed: {e}"))
                    .ok()?;
                let resp = on_state_machine(&self.state, move |sm| {
                    sm.handle_preaccept(
                        payload.txn_id,
                        payload.t0,
                        &payload.key,
                        payload.ballot,
                        payload.epoch,
                    )
                })
                .await?;
                match resp {
                    SmResponse::PreAcceptOK { t, deps, .. } => {
                        let ok = PreAcceptOkPayload {
                            from: self.local_node_id,
                            t,
                            deps,
                            snapshot_stale: false,
                        };
                        let bytes = bincode::serialize(&ok).ok()?;
                        Some(Message::AccordPreAcceptOK(Bytes::from(bytes)))
                    }
                    SmResponse::Nack { .. } => {
                        // Return empty PreAcceptOK to signal rejection.
                        Some(Message::AccordPreAcceptOK(Bytes::new()))
                    }
                    _ => Some(Message::AccordPreAcceptOK(Bytes::new())),
                }
            }

            Message::AccordPreAcceptV2(b) => {
                // Multi-key PreAccept: register the txn under EVERY key it writes
                // and return the UNION of dependencies across all keys, so a txn
                // overlapping on a non-first key is serialized (t_276e12). The
                // single-key AccordPreAccept arm above is the degenerate case.
                let payload: PreAcceptV2Payload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordPreAcceptV2: deserialize failed: {e}"))
                    .ok()?;
                let snapshot_ts = payload.snapshot_ts;
                let proposed_t = payload.t0;
                let resp = on_state_machine(&self.state, move |sm| {
                    let key_refs: Vec<&[u8]> = payload.keys.iter().map(|k| k.as_slice()).collect();
                    sm.handle_preaccept_multi_with_snapshot(
                        payload.txn_id,
                        payload.t0,
                        &key_refs,
                        payload.ballot,
                        payload.epoch,
                        snapshot_ts,
                    )
                })
                .await?;
                match resp {
                    SmResponse::PreAcceptOK { t, deps, .. } => {
                        let ok = PreAcceptOkPayload {
                            from: self.local_node_id,
                            t,
                            deps,
                            snapshot_stale: false,
                        };
                        let bytes = bincode::serialize(&ok).ok()?;
                        Some(Message::AccordPreAcceptOK(Bytes::from(bytes)))
                    }
                    SmResponse::SnapshotStale => {
                        let stale = PreAcceptOkPayload {
                            from: self.local_node_id,
                            t: proposed_t,
                            deps: Vec::new(),
                            snapshot_stale: true,
                        };
                        let bytes = bincode::serialize(&stale).ok()?;
                        Some(Message::AccordPreAcceptOK(Bytes::from(bytes)))
                    }
                    SmResponse::Nack { .. } => Some(Message::AccordPreAcceptOK(Bytes::new())),
                    _ => Some(Message::AccordPreAcceptOK(Bytes::new())),
                }
            }

            Message::AccordAccept(b) => {
                let payload: AcceptPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordAccept: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                let response = on_state_machine(&self.state, move |sm| {
                    sm.handle_accept(
                        payload.txn_id,
                        payload.t0,
                        payload.t,
                        payload.deps,
                        payload.ballot,
                    )
                })
                .await?;
                let deps = match response {
                    crate::accord::state_machine::SmResponse::AcceptOK { deps, .. } => deps,
                    _ => return None,
                };
                let ok = AcceptOkPayload { txn_id, deps };
                let bytes = bincode::serialize(&ok).ok()?;
                Some(Message::AccordAcceptOK(Bytes::from(bytes)))
            }

            Message::AccordCommit(b) => {
                let payload: CommitPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordCommit: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                on_state_machine(&self.state, move |sm| {
                    sm.handle_commit(payload.txn_id, payload.t0, payload.t, payload.deps)
                })
                .await?;
                // Commit is fire-and-forget in Accord, but the request-response
                // transport needs a reply, and the coordinator's commit quorum
                // verifies it proves THIS transaction was committed. Echo the
                // inbound `txn_id` in a structured `CommitOkPayload`, exactly as
                // the Apply ack does — a bare reply proves nothing about which
                // transaction, if any, this replica committed.
                let ok = CommitOkPayload {
                    txn_id,
                    from: self.local_node_id,
                };
                let bytes = bincode::serialize(&ok).ok()?;
                Some(Message::AccordCommit(Bytes::from(bytes)))
            }

            Message::AccordApply(b) => {
                let payload: ApplyPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordApply: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                // A single-key Apply is the degenerate one-entry write-set; an empty
                // payload is the no-write finalize. Routed through the lock-free apply
                // so the durable write-set does not hold the state lock across its fsync.
                let writes = if payload.result_data.is_empty() {
                    Vec::new()
                } else {
                    vec![payload.result_data]
                };
                let apply_status = self.apply_writeset_durably(txn_id, writes).await?;
                if !matches!(
                    apply_status,
                    crate::accord::state_machine::SmResponse::NoWriteFinalized
                ) && !await_txn_applied(&self.state, txn_id).await
                {
                    return None;
                }
                // Gap 5: return a structured ApplyOK so the coordinator can
                // count F+1 acknowledged applies before returning to the client.
                let ok = ApplyOkPayload {
                    txn_id,
                    from: self.local_node_id,
                };
                let bytes = bincode::serialize(&ok).ok()?;
                Some(Message::AccordApplyOK(Bytes::from(bytes)))
            }

            Message::AccordApplyV2(b) => {
                // Multi-key Apply (bincode body — sent to every peer that has NOT
                // advertised `CAP_ACCORD_CAPNP`): the coordinator already scoped this
                // payload to exactly the keys this replica is a participant for
                // (per-replica filtered fan-out), so the replica applies every write it
                // was sent — the same "coordinator scopes, replica trusts" invariant as
                // the v1 AccordApply arm, generalized to N partitions. The writes are
                // routed as ONE write-set so they park/apply atomically
                // (DATA-LOSS-CRITICAL: writes 2..N must never be dropped).
                let payload: ApplyV2Payload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordApplyV2: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                let writes: Vec<Vec<u8>> = payload.writes.into_iter().map(|w| w.mutation).collect();
                self.apply_writeset_and_ack(txn_id, writes).await
            }

            Message::AccordApplyV2Capnp(b) => {
                // Same multi-key Apply, but the body is a Cap'n Proto `accord.applyV2`
                // frame (`ferrosa_net::protocol::encode_accord_apply_v2`). Only a peer
                // that advertised `CAP_ACCORD_CAPNP` is ever sent this type.
                let (txn_id, writes) = match ferrosa_net::protocol::decode_accord_apply_v2(&b) {
                    Ok(ferrosa_net::protocol::AccordControlMessage::ApplyV2 { txn_id, writes }) => {
                        let txn_id = TxnId(Timestamp {
                            epoch: txn_id.epoch,
                            time: txn_id.time,
                            seq: txn_id.seq,
                            node: txn_id.node,
                        });
                        let writes = writes.into_iter().map(|w| w.mutation).collect();
                        (txn_id, writes)
                    }
                    Ok(other) => {
                        // A valid Accord envelope of the wrong kind on the Apply type is a
                        // routing bug, not a payload to reinterpret as a write-set.
                        tracing::error!(
                            "AccordApplyV2Capnp: expected an ApplyV2 payload, got {other:?}"
                        );
                        return None;
                    }
                    Err(error) => {
                        tracing::error!("AccordApplyV2Capnp: capnp decode failed: {error}");
                        return None;
                    }
                };
                self.apply_writeset_and_ack(txn_id, writes).await
            }

            Message::AccordApplyV2Region(b) => {
                // The region-REFERENCE multi-key Apply: a capnp HEADER (txn stamp + an
                // (offset, length) index) followed by ONE contiguous REGION holding the
                // write-set payload bytes. Only a peer that advertised
                // `CAP_ACCORD_APPLY_REGION` is ever sent this type. The decode BORROWS
                // the region — there is no per-entry owned wire structure; each entry is
                // read by offset. The mutations are then materialized into the `Vec<u8>`
                // per write the storage applier requires (`ApplyMutation.data` moves
                // owned bytes through the blocking apply seam); that is the applier's
                // input, not a decode of the message into owned entries.
                let view = match ferrosa_net::protocol::decode_accord_apply_v2_region(&b) {
                    Ok(view) => view,
                    Err(error) => {
                        tracing::error!("AccordApplyV2Region: region decode failed: {error}");
                        return None;
                    }
                };
                let txn_id = TxnId(Timestamp {
                    epoch: view.txn_id.epoch,
                    time: view.txn_id.time,
                    seq: view.txn_id.seq,
                    node: view.txn_id.node,
                });
                let mut writes: Vec<Vec<u8>> = Vec::with_capacity(view.len());
                for entry in view.mutations() {
                    match entry {
                        Ok(bytes) => writes.push(bytes.to_vec()),
                        Err(error) => {
                            tracing::error!(
                                "AccordApplyV2Region: entry out of the region's index: {error}"
                            );
                            return None;
                        }
                    }
                }
                self.apply_writeset_and_ack(txn_id, writes).await
            }

            Message::AccordRecover(b) => {
                let payload: RecoverPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordRecover: deserialize failed: {e}"))
                    .ok()?;
                let state = on_state_machine(&self.state, move |sm| {
                    sm.handle_recover(payload.txn_id, payload.t0, payload.ballot)
                })
                .await?;
                let bytes = bincode::serialize(&state).ok()?;
                Some(Message::AccordRecoverOK(Bytes::from(bytes)))
            }

            Message::AccordRead(b) => {
                // Gap 4: Linearizable read-vote.
                //
                // Decode the ReadVotePayload and evaluate the IF condition by
                // checking whether the row at the agreed timestamp `t` exists.
                //
                // For `INSERT IF NOT EXISTS`, the condition holds iff the row
                // does NOT exist (i.e., the state machine has not yet applied
                // a write for this key).
                //
                // This implementation evaluates the condition using the state
                // machine's committed/applied tracking:
                // - If a transaction for this key is in Applied state → row exists
                //   → condition does NOT hold (INSERT IF NOT EXISTS fails).
                // - Otherwise → row does not exist → condition holds.
                //
                // A full production implementation would read actual storage.
                if let Ok(vote_req) = bincode::deserialize::<ReadVotePayload>(&b) {
                    use crate::accord::wire::ReadPredicate;

                    // DEP-WAIT (linearizability): before evaluating the IF
                    // condition at the agreed `t`, every conflicting transaction
                    // ordered before `t` (t0 < t) that is committed-but-not-yet-
                    // Applied on this replica must first reach `Applied`. Without
                    // this, two genuinely concurrent `INSERT IF NOT EXISTS` both
                    // observe the key as absent before either applies, both gates
                    // pass, and BOTH apply — a lost-update / double-apply. We park
                    // on the state machine's apply-notify, re-checking the
                    // condition under the lock after each wake, with a BOUNDED
                    // total timeout. On timeout we ABSTAIN (return no row /
                    // condition_holds=false) rather than read stale: the
                    // coordinator's F+1 agreement then fails loud instead of
                    // letting a stale read masquerade as success.
                    //
                    // This applies to BOTH predicate kinds: the existence path
                    // (`read_condition_holds_at`) and the generic read-row path
                    // share the same staleness hazard, so they share the dep-wait.
                    //
                    // CRITICAL: the parking_lot state lock is NEVER held across
                    // an `.await` — handle_apply needs the same lock to make
                    // progress (and to fire the notify that unblocks us), so
                    // holding it across the wait would deadlock.
                    // `NotExists` names no table or clustering, so there is no row
                    // to read. Answering it from the conflict index (partition-key
                    // bytes only) reported other tables' rows as existing
                    // (t_7a0acbc8). Abstain: the coordinator's F+1 agreement then
                    // fails loud. Current coordinators send `ReadRow` (t_fe2426bb).
                    if matches!(vote_req.predicate, ReadPredicate::NotExists) {
                        tracing::error!(
                            txn_id = ?vote_req.txn_id,
                            "accord: refusing a NotExists read-vote (no table to read; \
                             the coordinator predates t_fe2426bb) — abstaining"
                        );
                        return Some(Message::AccordReadOK(Bytes::new()));
                    }
                    if !await_conflicting_deps_applied(&self.state, &vote_req.key, vote_req.t).await
                    {
                        // Empty ReadOK is the wire-level abstention already
                        // recognized by the coordinator. A serialized false vote
                        // would incorrectly turn a timeout into ConditionNotMet.
                        return Some(Message::AccordReadOK(Bytes::new()));
                    }

                    let txn_id = vote_req.txn_id;
                    let (condition_holds, current_row) =
                        on_state_machine(&self.state, move |sm| match &vote_req.predicate {
                            // Refused above; unreachable here. Answer "does not
                            // hold" rather than panic on the serving path.
                            ReadPredicate::NotExists => (false, vec![]),
                            // Unconditional transaction: no IF to evaluate, always
                            // holds. Defensive — the coordinator skips the read-vote
                            // for `Always`, so this arm is not normally reached.
                            ReadPredicate::Always => (true, vec![]),
                            ReadPredicate::SnapshotBarrier => (true, vec![]),
                            // Generic IF col=val: the replica does the read-at-`t`
                            // and returns the row bytes; the coordinator (which owns
                            // the table schema) evaluates the predicate via the
                            // injected gate wrapping `eval_if_conditions` and GATES the
                            // Apply on it. The replica reports `condition_holds=true`
                            // as a neutral value — the coordinator's evaluation is
                            // authoritative.
                            //
                            // Linearizability of THIS read rests on three guarantees:
                            // (1) the dep-wait above blocked until every conflicting
                            //     dep `t0 < t` Applied locally, so the engine's state
                            //     is the row as-of-`t`;
                            // (2) the coordinator requires F+1 *identical* row bytes
                            //     (`agreed_row`) before evaluating the predicate and
                            //     fails loud on divergence — so the gate verdict is
                            //     never taken on a non-quorum / skewed read; and
                            // (3) `EngineStorageReader::read_row_at` bounds cells to
                            //     `ts <= t.time` (as-of-`t`).
                            ReadPredicate::ReadRow { .. }
                            | ReadPredicate::ReadClusteringRow { .. } => {
                                let read = vote_req
                                    .predicate
                                    .row_read()
                                    .expect("a row-reading predicate names its row");
                                let row = sm.read_row_bytes_at(read, &vote_req.key, vote_req.t);
                                (true, row.unwrap_or_default())
                            }
                        })
                        .await?;
                    let ok = ReadVoteOkPayload {
                        txn_id,
                        from: self.local_node_id,
                        condition_holds,
                        current_row,
                    };
                    let resp_bytes = bincode::serialize(&ok).ok()?;
                    Some(Message::AccordReadOK(Bytes::from(resp_bytes)))
                } else {
                    // An undecodable vote (a predicate from a newer coordinator,
                    // or corruption) abstains. Echoing the request back, as this
                    // used to, handed the coordinator bytes it then tried to
                    // decode as a vote.
                    tracing::error!(
                        bytes = b.len(),
                        "accord: could not decode a ReadVote request — abstaining"
                    );
                    Some(Message::AccordReadOK(Bytes::new()))
                }
            }

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::state_machine::AccordStateMachine;
    use crate::accord::wire::{ApplyPayload, ApplyV2Payload, WriteSetEntry};
    use ferrosa_common::accord::{BallotNumber, TxnId, TxnPhase};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    /// Drive a transaction to `Committed` on this state (the phase whose Apply
    /// can park behind unapplied ordered dependencies).
    fn commit(state: &AccordState, txn_id: TxnId, t0: u64, t: u64, deps: Vec<TxnId>) {
        let key = b"bounded-wait-key";
        let mut sm = state.lock();
        sm.handle_preaccept(txn_id, ts(t0), key, BallotNumber(0), 0);
        sm.handle_accept(txn_id, ts(t0), ts(t), deps.clone(), BallotNumber(1));
        sm.handle_commit(txn_id, ts(t0), ts(t), deps);
    }

    /// The bound expiry must REFUSE the apply ack, promptly.
    ///
    /// This is the half of the abandon contract that lives on the replica: when a
    /// dependency never applies, the wait must give up at its bound and report
    /// `false` — NOT hang, and NOT report a transaction that never reached
    /// `Applied` as applied. The coordinator turns that refusal into an abandoned
    /// (rolled back, retryable) transaction instead of a permanently poisoned key.
    #[tokio::test]
    async fn await_txn_applied_refuses_at_its_bound_when_a_dependency_never_applies() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));

        let dep = TxnId::new(1, ts(1000));
        let target = TxnId::new(2, ts(2000));
        // `dep` is Committed but NEVER applied — exactly the stall the bound exists
        // for. `target` depends on it, so `target` can never reach Applied.
        commit(&state, dep, 1000, 1001, vec![]);
        commit(&state, target, 2000, 2001, vec![dep]);

        let started = std::time::Instant::now();
        let applied =
            await_txn_applied_within(&state, target, std::time::Duration::from_millis(30)).await;
        let elapsed = started.elapsed();

        assert!(
            !applied,
            "a transaction still parked on an unapplied dependency must be refused"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "the wait must expire at its bound, not hang (took {elapsed:?})"
        );
    }

    /// Control: the wait reports success for a transaction that really applied.
    ///
    /// Without this, "returns false" is satisfied by a wait that always refuses.
    #[tokio::test]
    async fn await_txn_applied_reports_success_for_an_applied_transaction() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));

        let txn_id = TxnId::new(1, ts(1000));
        commit(&state, txn_id, 1000, 1001, vec![]);
        // No dependencies: the apply persists and the txn advances to Applied.
        state.lock().handle_apply(txn_id, b"write".to_vec());

        assert!(
            await_txn_applied_within(&state, txn_id, std::time::Duration::from_millis(50)).await,
            "an Applied transaction must be reported as applied"
        );
    }

    /// RED-FIRST: a durably-Applied transaction that was then PRUNED must still ack.
    ///
    /// Observed live: a follower applied at `00:50:17.63`, `maintenance: pruned applied
    /// Accord transactions pruned=4` at `00:50:18.95`, then `Apply target is absent from
    /// local state — refusing ApplyOK` at `00:50:18.97`. `prune_applied` drops the volatile
    /// `TxnState` and leaves only a tombstone, so the ack path re-looked-up `get_state`,
    /// found `None`, and a replica REFUSED TO ACK a transaction it had correctly and
    /// durably applied. Before the fix this test is RED: the wait returns `false`.
    #[tokio::test]
    async fn await_txn_applied_survives_prune_applied() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));

        let txn_id = TxnId::new(1, ts(1000));
        commit(&state, txn_id, 1000, 1001, vec![]);
        // Durable apply: the state machine persists BEFORE it marks the txn Applied.
        state.lock().handle_apply(txn_id, b"write".to_vec());
        assert_eq!(
            state.lock().get_state(&txn_id).map(|s| s.phase),
            Some(TxnPhase::Applied)
        );

        // The maintenance tick forgets the applied txn; only the tombstone remains.
        let pruned = state.lock().prune_applied();
        assert!(
            pruned > 0,
            "precondition: prune must have removed the applied txn"
        );
        assert!(
            state.lock().get_state(&txn_id).is_none(),
            "precondition: only the applied tombstone remains"
        );

        // The retried Apply must still be ACKed.
        assert!(
            await_txn_applied_within(&state, txn_id, std::time::Duration::from_millis(50)).await,
            "a transaction this replica durably applied must still ack after prune_applied"
        );
    }

    /// NEGATIVE CONTROL for the prune fix: it must not turn the ack gate into an
    /// always-ack. A transaction this replica NEVER registered (never applied) must
    /// still be refused.
    #[tokio::test]
    async fn await_txn_applied_still_refuses_a_never_registered_txn() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let never = TxnId::new(9, ts(9999));
        assert!(
            !await_txn_applied_within(&state, never, std::time::Duration::from_millis(30)).await,
            "an id this replica never applied must still be refused"
        );
    }

    /// Register a committed transaction on an explicit `key` (drives
    /// PreAccept→Accept→Commit so both the conflict index and `txn_states` know
    /// it). Used to build a dependency set a read must wait on.
    fn commit_on(
        state: &AccordState,
        key: &[u8],
        txn_id: TxnId,
        t0: u64,
        t: u64,
        deps: Vec<TxnId>,
    ) {
        let mut sm = state.lock();
        sm.handle_preaccept(txn_id, ts(t0), key, BallotNumber(0), 0);
        sm.handle_accept(txn_id, ts(t0), ts(t), deps.clone(), BallotNumber(1));
        sm.handle_commit(txn_id, ts(t0), ts(t), deps);
    }

    /// RED-FIRST: a dep-wait must keep waiting while the engine DRAINS its
    /// dependency set, even when the TOTAL wait outlasts the bound.
    ///
    /// This is the invariant the PostgreSQL snapshot barrier got wrong: on the
    /// single cluster-global barrier key a barrier queues behind every earlier
    /// transaction, so its total wait is the whole queue's drain time — often
    /// longer than the barrier budget. Before the fix the wait was decided by a
    /// wall clock and abstained at `timeout` ("dependencies were not applied
    /// locally") while the engine was applying the queue one predecessor at a
    /// time; the client saw a live quorum fail a transaction the engine was still
    /// resolving. The wait must decide on the PROGRESS of its dependency set, so
    /// each predecessor that applies restarts the backstop clock.
    #[tokio::test]
    async fn dep_wait_keeps_waiting_while_the_engine_drains_the_dependency_queue() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let key = b"bounded-wait-key";
        let read_t = ts(10_000);

        // Three conflicting dependencies ordered before the read, none applied.
        let d0 = TxnId::new(1, ts(1000));
        let d1 = TxnId::new(1, ts(2000));
        let d2 = TxnId::new(1, ts(3000));
        commit_on(&state, key, d0, 1000, 1001, vec![]);
        commit_on(&state, key, d1, 2000, 2001, vec![]);
        commit_on(&state, key, d2, 3000, 3001, vec![]);

        let bound = std::time::Duration::from_millis(300);
        // The engine drains the queue over 400 ms — each apply < the bound apart,
        // but the WHOLE drain longer than the bound.
        let engine = state.clone();
        let drain = tokio::spawn(async move {
            for (id, wait_ms) in [(d0, 100u64), (d1, 100), (d2, 200)] {
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                engine.lock().handle_apply(id, b"row".to_vec());
            }
        });

        let started = std::time::Instant::now();
        let applied = await_conflicting_deps_applied_within(&state, key, read_t, bound).await;
        drain.await.expect("drain task panicked");

        assert!(
            applied,
            "a dependency set the engine is actively draining must not abstain on a wall \
             clock; abstained after {:?} with the queue still draining",
            started.elapsed()
        );
    }

    /// The other half of the evidence: the engine can be resolving OTHER work
    /// while a dependency sits in `Committed`, and that is still progress.
    ///
    /// A dependency that is committed but waiting its turn behind the apply path
    /// does not change the read's dependency SET between polls, so a set-only
    /// signal would abstain. The engine's `applied_epoch` covers it: advancing it
    /// is a completed resolution, and the wait must restart on it.
    #[tokio::test]
    async fn dep_wait_keeps_waiting_while_the_engine_completes_unrelated_resolutions() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let key = b"bounded-wait-key";
        let other_key = b"unrelated-key";
        let read_t = ts(10_000);

        // The read's one dependency — committed, and slow to apply.
        let dep = TxnId::new(1, ts(1000));
        commit_on(&state, key, dep, 1000, 1001, vec![]);
        // Unrelated transactions on a DIFFERENT key: applying them moves the
        // engine watermark but never the read's dependency set.
        let p0 = TxnId::new(1, ts(2000));
        let p1 = TxnId::new(1, ts(3000));
        let p2 = TxnId::new(1, ts(4000));
        commit_on(&state, other_key, p0, 2000, 2001, vec![]);
        commit_on(&state, other_key, p1, 3000, 3001, vec![]);
        commit_on(&state, other_key, p2, 4000, 4001, vec![]);

        let bound = std::time::Duration::from_millis(300);
        let engine = state.clone();
        let drain = tokio::spawn(async move {
            // Unrelated resolutions keep the engine watermark moving, each well
            // inside the bound; the read's own dependency applies last.
            for (id, wait_ms) in [(p0, 100u64), (p1, 100), (p2, 200), (dep, 100)] {
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                engine.lock().handle_apply(id, b"row".to_vec());
            }
        });

        let applied = await_conflicting_deps_applied_within(&state, key, read_t, bound).await;
        drain.await.expect("drain task panicked");

        assert!(
            applied,
            "an engine completing unrelated resolutions is alive; the wait must not abstain"
        );
    }

    /// The BACKSTOP: if neither the dependency set (or its phases) nor the engine
    /// watermark moves for the whole bound, the dependency is genuinely stuck and
    /// the wait must abstain — promptly, not hang.
    #[tokio::test]
    async fn dep_wait_abstains_when_neither_the_set_nor_the_engine_moves() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let key = b"bounded-wait-key";

        // One committed dependency that NOTHING ever applies — the stall the
        // backstop exists for.
        let dep = TxnId::new(1, ts(1000));
        commit_on(&state, key, dep, 1000, 1001, vec![]);

        let started = std::time::Instant::now();
        let applied = await_conflicting_deps_applied_within(
            &state,
            key,
            ts(10_000),
            std::time::Duration::from_millis(100),
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            !applied,
            "a frozen dependency set and a frozen engine watermark must abstain"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "the backstop must fire at its bound, not hang (took {elapsed:?})"
        );
    }

    /// `publish_accord_state` must make the slot observe the EXACT `AccordState`
    /// instance the handler serves — same `Arc`, not a clone of the inner
    /// machine. If they diverged, the coordinator's local self-vote would run
    /// against different protocol state than its remote peers see, corrupting
    /// dependency agreement. The slot starts empty (standalone/pre-formation).
    #[test]
    fn publish_accord_state_shares_the_same_instance_with_the_handler() {
        let slot = empty_accord_state_slot();
        assert!(
            slot.load_full().is_none(),
            "a fresh slot must be empty until the controller publishes state"
        );

        let sm = AccordStateMachine::new(7, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let served = publish_accord_state(&slot, state.clone()).expect("publish state");

        // The handler is constructed from the returned/served state.
        let _handler = AccordHandler::new(served.clone(), 7);

        let published = slot
            .load_full()
            .expect("slot must be populated after publish");
        assert!(
            Arc::ptr_eq(&published, &state),
            "the slot must hold the exact same Arc the handler serves"
        );
        assert!(
            Arc::ptr_eq(&served, &state),
            "the returned state (handed to the handler) must be the same instance"
        );
    }

    /// AccordApplyV2 must apply EVERY write it was sent (the coordinator already
    /// scoped the payload to this replica's keys) as one atomic write-set,
    /// advance the txn to Applied, and ack with AccordApplyOK.
    #[tokio::test]
    async fn accord_apply_v2_applies_full_writeset_and_acks() {
        let writer = std::sync::Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(1, writer);
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state.clone(), 1);

        let txn_id = TxnId::new(1, ts(1000));
        // Commit a multi-key txn so the apply has agreed (t, deps) to read.
        {
            let mut sm = state.lock();
            sm.handle_preaccept(txn_id, ts(1000), b"ka", BallotNumber(0), 0);
            sm.handle_commit(txn_id, ts(1000), ts(1001), vec![]);
        }

        let payload = ApplyV2Payload {
            txn_id,
            writes: vec![
                WriteSetEntry {
                    key: b"ka".to_vec(),
                    mutation: b"mut-a".to_vec(),
                },
                WriteSetEntry {
                    key: b"kb".to_vec(),
                    mutation: b"mut-b".to_vec(),
                },
            ],
        };
        let bytes = bincode::serialize(&payload).unwrap();

        let peer: PeerId = (
            uuid::Uuid::from_u128(2),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let resp = handler
            .handle(peer, Message::AccordApplyV2(Bytes::from(bytes)))
            .await;

        // Acked.
        assert!(
            matches!(resp, Some(Message::AccordApplyOK(_))),
            "replica must ack the multi-key apply with AccordApplyOK"
        );
        // Both writes applied → txn advanced to Applied exactly once.
        assert_eq!(
            state.lock().get_state(&txn_id).unwrap().phase,
            TxnPhase::Applied,
            "the multi-key txn must reach Applied after AccordApplyV2"
        );
    }

    #[tokio::test]
    async fn absent_no_write_apply_acks_and_releases_merged_dependency() {
        let writer = std::sync::Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(1, writer);
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state.clone(), 1);

        let absent_dependency = TxnId::new(2, ts(1000));
        let waiting_txn = TxnId::new(1, ts(1001));
        {
            let mut sm = state.lock();
            sm.handle_preaccept(waiting_txn, ts(1001), b"key", BallotNumber(0), 0);
            sm.handle_commit(waiting_txn, ts(1001), ts(1002), vec![absent_dependency]);
            sm.handle_apply_writeset(waiting_txn, vec![b"mutation".to_vec()]);
        }

        let payload = ApplyPayload {
            txn_id: absent_dependency,
            result_data: Vec::new(),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let peer: PeerId = (
            uuid::Uuid::from_u128(2),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let unexpected_write = ApplyPayload {
            txn_id: absent_dependency,
            result_data: b"unexpected-write".to_vec(),
        };
        let response = handler
            .handle(
                peer,
                Message::AccordApply(Bytes::from(bincode::serialize(&unexpected_write).unwrap())),
            )
            .await;
        assert!(
            response.is_none(),
            "a missing real write must not be acknowledged"
        );

        let response = handler
            .handle(peer, Message::AccordApply(Bytes::from(bytes)))
            .await;

        assert!(matches!(response, Some(Message::AccordApplyOK(_))));
        assert_eq!(
            state.lock().get_state(&waiting_txn).unwrap().phase,
            TxnPhase::Applied
        );
    }

    /// AccordPreAcceptV2 must union dependencies across ALL the transaction's
    /// keys — a conflict registered on a non-first key must appear in the deps
    /// the replica returns (t_276e12).
    #[tokio::test]
    async fn accord_preaccept_v2_unions_deps_across_keys_over_the_wire() {
        let writer = std::sync::Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(1, writer);
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state.clone(), 1);

        // A pre-existing txn registered (via normal PreAccept) only on key k2.
        let conflict = TxnId::new(2, ts(500));
        {
            let mut sm = state.lock();
            sm.handle_preaccept(conflict, ts(500), b"k2", BallotNumber(0), 0);
        }

        // New multi-key txn preaccepts {k1, k2} at t0=1000 over AccordPreAcceptV2.
        let txn_id = TxnId::new(1, ts(1000));
        let payload = PreAcceptV2Payload {
            txn_id,
            t0: ts(1000),
            keys: vec![b"k1".to_vec(), b"k2".to_vec()],
            ballot: BallotNumber(0),
            epoch: 0,
            snapshot_ts: None,
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let peer: PeerId = (
            uuid::Uuid::from_u128(3),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let resp = handler
            .handle(peer, Message::AccordPreAcceptV2(Bytes::from(bytes)))
            .await;

        match resp {
            Some(Message::AccordPreAcceptOK(b)) => {
                let ok: PreAcceptOkPayload = bincode::deserialize(&b).expect("PreAcceptOk decodes");
                assert!(
                    ok.deps.contains(&conflict),
                    "V2 PreAccept must union deps across all keys — the conflict on the \
                     non-first key k2 must appear in the returned deps"
                );
            }
            other => panic!("expected AccordPreAcceptOK, got {:?}", other),
        }
    }

    /// A `SyncWriter` whose fsync takes `delay` of wall time, the way
    /// `File::sync_all` does on a saturated disk.
    struct SlowSyncWriter {
        delay: std::time::Duration,
    }

    impl ferrosa_storage::accord::sync_writer::SyncWriter for SlowSyncWriter {
        fn write_and_sync(
            &self,
            _data: &[u8],
        ) -> ferrosa_storage::accord::sync_writer::SyncWriteResult {
            std::thread::sleep(self.delay);
            ferrosa_storage::accord::sync_writer::SyncWriteResult::Ok
        }
    }

    /// A slow fsync must not freeze the async runtime serving the handler.
    ///
    /// Every PreAccept persists before it replies. The handler ran that fsync
    /// inline on a runtime worker while holding the state machine's
    /// `parking_lot` mutex, so one slow fsync blocked its own worker and every
    /// other worker that reached for the mutex. With the disk slow on all
    /// three nodes, every runtime froze together: heartbeats went unanswered,
    /// the Raft leader stepped down, and Accord RPCs hit the Data lane timeout.
    /// That is how the first LWT on a healthy cluster failed.
    ///
    /// Four concurrent PreAccepts against a 400 ms fsync, on a two-worker
    /// runtime, while a 20 ms ticker stands in for the heartbeat. The ticker
    /// must keep running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_fsync_does_not_freeze_the_runtime_serving_preaccepts() {
        let writer = std::sync::Arc::new(SlowSyncWriter {
            delay: std::time::Duration::from_millis(400),
        });
        let sm = AccordStateMachine::new(1, writer);
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = std::sync::Arc::new(AccordHandler::new(state, 1));

        let ticker = tokio::spawn(async {
            let mut worst = std::time::Duration::ZERO;
            let mut last = tokio::time::Instant::now();
            for _ in 0..60 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let now = tokio::time::Instant::now();
                worst = worst.max(now - last);
                last = now;
            }
            worst
        });

        let calls: Vec<_> = (0..4u64)
            .map(|i| {
                let handler = std::sync::Arc::clone(&handler);
                tokio::spawn(async move {
                    let payload = crate::accord::wire::PreAcceptPayload {
                        txn_id: TxnId::new(1, ts(1000 + i)),
                        t0: ts(1000 + i),
                        key: format!("key-{i}").into_bytes(),
                        ballot: BallotNumber(0),
                        epoch: 0,
                    };
                    let bytes = bincode::serialize(&payload).unwrap();
                    let peer: PeerId = (
                        uuid::Uuid::from_u128(2),
                        "127.0.0.1:0".parse().expect("valid socket addr"),
                    );
                    handler
                        .handle(peer, Message::AccordPreAccept(Bytes::from(bytes)))
                        .await
                })
            })
            .collect();

        // Read-votes on other keys take the same mutex for their dep-wait
        // check. They must not park a runtime worker behind a PreAccept that
        // holds the mutex through its fsync.
        let reads: Vec<_> = (0..4u64)
            .map(|i| {
                let handler = std::sync::Arc::clone(&handler);
                tokio::spawn(async move {
                    let payload = ReadVotePayload {
                        txn_id: TxnId::new(3, ts(2000 + i)),
                        t: ts(2000 + i),
                        key: format!("read-key-{i}").into_bytes(),
                        predicate: crate::accord::wire::ReadPredicate::ReadRow {
                            keyspace: "ks".into(),
                            table: "t".into(),
                        },
                    };
                    let bytes = bincode::serialize(&payload).unwrap();
                    let peer: PeerId = (
                        uuid::Uuid::from_u128(3),
                        "127.0.0.1:0".parse().expect("valid socket addr"),
                    );
                    handler
                        .handle(peer, Message::AccordRead(Bytes::from(bytes)))
                        .await
                })
            })
            .collect();

        for call in calls {
            let resp = call.await.expect("PreAccept task must not panic");
            assert!(
                matches!(&resp, Some(Message::AccordPreAcceptOK(b)) if !b.is_empty()),
                "every PreAccept must still vote, got {resp:?}"
            );
        }
        for read in reads {
            let resp = read.await.expect("ReadVote task must not panic");
            assert!(
                matches!(&resp, Some(Message::AccordReadOK(b)) if !b.is_empty()),
                "every read-vote on an uncontended key must answer, got {resp:?}"
            );
        }
        let worst = ticker.await.expect("ticker must not panic");
        assert!(
            worst < std::time::Duration::from_millis(250),
            "a 20 ms ticker on the handler's runtime stalled for {worst:?} while \
             PreAccepts fsynced: the fsync is blocking runtime workers"
        );
    }

    /// `NotExists` names no table or clustering, so a replica cannot answer it
    /// from storage. It used to answer from the conflict index (partition-key
    /// bytes only), which reported rows of other tables as existing
    /// (t_7a0acbc8). The replica now abstains, so a coordinator that still
    /// sends it (a pre-upgrade node) fails loud instead of being told a wrong
    /// answer (t_fe2426bb).
    /// A read-vote this replica cannot decode (a predicate from a newer
    /// coordinator, or corruption) must abstain. It used to echo the request
    /// bytes back as the vote, which the coordinator then tried to decode as
    /// one.
    #[tokio::test]
    async fn an_undecodable_read_vote_abstains() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state, 1);
        let peer: PeerId = (
            uuid::Uuid::from_u128(3),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let resp = handler
            .handle(
                peer,
                Message::AccordRead(Bytes::from_static(b"\xff\xfe not a vote")),
            )
            .await;
        assert!(
            matches!(&resp, Some(Message::AccordReadOK(b)) if b.is_empty()),
            "an undecodable read-vote must abstain, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn a_not_exists_read_vote_abstains() {
        let sm = AccordStateMachine::new(1, std::sync::Arc::new(MockSyncWriter::new()));
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state, 1);
        let payload = ReadVotePayload {
            txn_id: TxnId::new(3, ts(2000)),
            t: ts(2000),
            key: b"fresh-key".to_vec(),
            predicate: crate::accord::wire::ReadPredicate::NotExists,
        };
        let peer: PeerId = (
            uuid::Uuid::from_u128(3),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let resp = handler
            .handle(
                peer,
                Message::AccordRead(Bytes::from(bincode::serialize(&payload).unwrap())),
            )
            .await;
        assert!(
            matches!(&resp, Some(Message::AccordReadOK(b)) if b.is_empty()),
            "a NotExists read-vote must abstain, got {resp:?}"
        );
    }

    /// A one-shot gate: a caller ANNOUNCES it is inside a durable step and BLOCKS
    /// until released — a `File::sync_all` the test drives by hand.
    struct Gate {
        barrier: std::sync::Mutex<BarrierState>,
        cv: std::sync::Condvar,
    }

    #[derive(Default)]
    struct BarrierState {
        inside: bool,
        released: bool,
    }

    impl Gate {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                barrier: std::sync::Mutex::new(BarrierState::default()),
                cv: std::sync::Condvar::new(),
            })
        }

        /// Block until a caller is INSIDE the gated section, or `timeout` elapses.
        /// Returns whether a caller was observed inside.
        fn wait_until_inside(&self, timeout: std::time::Duration) -> bool {
            let guard = self.barrier.lock().expect("gate lock");
            let (guard, _) = self
                .cv
                .wait_timeout_while(guard, timeout, |state| !state.inside)
                .expect("gate lock");
            guard.inside
        }

        /// Enter the gated section and block until [`Self::release`].
        fn enter_and_block(&self) {
            let mut guard = self.barrier.lock().expect("gate lock");
            guard.inside = true;
            self.cv.notify_all();
            while !guard.released {
                guard = self.cv.wait(guard).expect("gate lock");
            }
        }

        fn release(&self) {
            let mut guard = self.barrier.lock().expect("gate lock");
            guard.released = true;
            self.cv.notify_all();
        }
    }

    /// A [`StorageApplier`](crate::accord::apply::StorageApplier) whose atomic
    /// write-set step blocks in [`Gate`] — the test stand-in for
    /// `StorageEngine::write_atomic_batch` → `commit_log.force_sync()`, the durable
    /// step the apply walk holds the state machine's mutex across.
    struct BlockingApplier {
        gate: std::sync::Arc<Gate>,
    }

    impl crate::accord::apply::StorageApplier for BlockingApplier {
        fn apply(
            &self,
            _txn_id: TxnId,
            _mutation: crate::accord::apply::ApplyMutation,
        ) -> Result<(), crate::accord::apply::ApplyError> {
            Ok(())
        }

        fn apply_writeset_borrowed(
            &self,
            _txn_id: TxnId,
            _mutations: &[crate::accord::apply::MutationView<'_>],
        ) -> Result<(), crate::accord::apply::ApplyError> {
            self.gate.enter_and_block();
            Ok(())
        }
    }

    /// **Invariant 3 — an inbound Accord RPC must not be blocked by an in-flight
    /// apply.** The apply path persists (its protocol-log `Applied` marker here; the
    /// storage commit-log `force_sync` in production) while holding the state
    /// machine's `parking_lot` mutex, and every inbound Accord arm takes that same
    /// mutex — so while one apply fsyncs, a PreAccept/Recover cannot even be READ,
    /// let alone answered, and its coordinator logs
    /// `orphan RPC response ... no pending caller`.
    ///
    /// This drives a real write to `Committed`, holds its Apply inside the durable
    /// write, and asserts that a *concurrently issued* `AccordRecover` reaches its
    /// handler and answers with the transaction's registered state. The assertion is
    /// about the apply's real durable step (the `StorageApplier` seam, where
    /// `write_atomic_batch` lives), never about a test-only seam on the handler path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_inbound_accord_rpc_completes_while_an_apply_is_inside_its_durable_write() {
        let gate = Gate::new();
        let applier: std::sync::Arc<dyn crate::accord::apply::StorageApplier> =
            std::sync::Arc::new(BlockingApplier {
                gate: std::sync::Arc::clone(&gate),
            });
        let sm = AccordStateMachine::with_applier(
            1,
            std::sync::Arc::new(MockSyncWriter::new()),
            applier,
        );
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = std::sync::Arc::new(AccordHandler::new(state.clone(), 1));
        let peer: PeerId = (
            uuid::Uuid::from_u128(9),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );

        // A real write, committed and ready to apply.
        let txn_id = TxnId::new(7, ts(3000));
        {
            let mut sm = state.lock();
            sm.handle_preaccept(txn_id, ts(3000), b"lock-free-rpc-key", BallotNumber(0), 0);
            sm.handle_commit(txn_id, ts(3000), ts(3001), vec![]);
        }

        let apply_bytes = bincode::serialize(&ApplyPayload {
            txn_id,
            result_data: b"row".to_vec(),
        })
        .unwrap();
        let apply = tokio::spawn({
            let handler = std::sync::Arc::clone(&handler);
            async move {
                handler
                    .handle(peer, Message::AccordApply(Bytes::from(apply_bytes)))
                    .await
            }
        });

        assert!(
            gate.wait_until_inside(std::time::Duration::from_secs(2)),
            "the apply must reach its durable write-set step"
        );

        // While it is inside that write, a read-only inbound RPC must complete.
        let recover_bytes = bincode::serialize(&crate::accord::wire::RecoverPayload {
            txn_id,
            t0: ts(3000),
            ballot: BallotNumber(0),
        })
        .unwrap();
        let recover = tokio::spawn({
            let handler = std::sync::Arc::clone(&handler);
            async move {
                handler
                    .handle(peer, Message::AccordRecover(Bytes::from(recover_bytes)))
                    .await
            }
        });

        let answered = tokio::time::timeout(std::time::Duration::from_millis(750), recover).await;
        // ALWAYS unblock the durable step, pass or fail: the applier's blocking
        // thread never returns otherwise and the runtime hangs on shutdown instead
        // of reporting. A test that hangs is not a control.
        gate.release();
        let answered = answered
            .expect(
                "an inbound Accord RPC must complete while an apply is inside its durable write; \
                 it is serialized behind the state lock that the apply holds across its fsync",
            )
            .expect("the RPC task must not panic");

        // VALUE assertion: the Recover answered with THIS transaction's state.
        match answered {
            Some(Message::AccordRecoverOK(b)) => {
                let st: Option<ferrosa_common::accord::TxnState> =
                    bincode::deserialize(&b).expect("RecoverOK decodes to a TxnState");
                let st = st.expect("the registered transaction comes back, not None");
                assert_eq!(st.txn_id, txn_id, "the recover reply names the transaction");
                assert_eq!(
                    st.phase,
                    TxnPhase::Committed,
                    "the transaction is Committed (not yet Applied) while its apply is \
                     mid-durable-write"
                );
            }
            other => panic!("expected an AccordRecoverOK reply, got {other:?}"),
        }

        // The durable step is already released above; the apply now completes and acks.
        let ack = apply.await.expect("the apply task must not panic");
        assert!(
            matches!(&ack, Some(Message::AccordApplyOK(b)) if !b.is_empty()),
            "the apply still acks after its durable write completes, got {ack:?}"
        );
    }

    /// A `StorageApplier` that records which transactions it has durably applied,
    /// so a test can observe the STORAGE half of a replica's apply independently of
    /// the state machine's phase. `apply` returning `Ok` models the production applier
    /// returning from its commit-log fsync — the durability instant for the rows.
    struct DurableApplier {
        applied: std::sync::Mutex<Vec<TxnId>>,
    }

    impl DurableApplier {
        fn new() -> Self {
            Self {
                applied: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn durably_applied(&self, txn_id: &TxnId) -> bool {
            self.applied.lock().unwrap().contains(txn_id)
        }
    }

    impl crate::accord::apply::StorageApplier for DurableApplier {
        fn apply(
            &self,
            txn_id: TxnId,
            _mutation: crate::accord::apply::ApplyMutation,
        ) -> Result<(), crate::accord::apply::ApplyError> {
            self.applied.lock().unwrap().push(txn_id);
            Ok(())
        }
    }

    /// G1, at the smallest level it is reachable: ONE inbound Apply, in process.
    ///
    /// A replica that durably wrote a transaction's rows must not withhold
    /// `ApplyOK`. The protocol-log `Applied` marker is the journal's *record* of
    /// the apply; the durability instant for the DATA is the storage write, whose
    /// commit-log fsync has already succeeded by the time the marker is written.
    /// When the marker's own fsync fails and that failure is allowed to withhold
    /// the ack, the coordinator loses this replica's acknowledgment, reports
    /// `TxnAbandoned`, and the client is told "NOT committed; safe to retry" while
    /// the row is durable and readable here — finding G1.
    ///
    /// RED as written: the marker fsync fails, so the transaction never advances to
    /// `Applied`, `await_txn_applied` returns `false`, and the handler answers `None`.
    /// The value assertion on `AccordApplyOK` therefore fails on the ACK GATE's
    /// behaviour, not on a missing symbol. The row itself IS durable — the first
    /// assertion (the precondition) passes today and is what makes the failure a
    /// lie about durability rather than a missing write.
    #[tokio::test]
    async fn apply_acks_when_the_rows_are_durable_even_if_the_marker_fsync_fails() {
        let writer = std::sync::Arc::new(MockSyncWriter::new());
        let applier = std::sync::Arc::new(DurableApplier::new());
        let sm = AccordStateMachine::with_applier(1, writer.clone(), applier.clone());
        let state: AccordState = std::sync::Arc::new(parking_lot::Mutex::new(sm));
        let handler = AccordHandler::new(state.clone(), 1);

        let txn_id = TxnId::new(1, ts(1000));
        {
            let mut sm = state.lock();
            sm.handle_preaccept(txn_id, ts(1000), b"g1-key", BallotNumber(0), 0);
            sm.handle_commit(txn_id, ts(1000), ts(1001), vec![]);
        }

        // Only the protocol-log marker's fsync fails; the storage write (the
        // durability instant) still succeeds on the applier.
        writer.set_fsync_failure(true);

        let payload = ApplyV2Payload {
            txn_id,
            writes: vec![WriteSetEntry {
                key: b"g1-key".to_vec(),
                mutation: b"durable-row".to_vec(),
            }],
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let peer: PeerId = (
            uuid::Uuid::from_u128(2),
            "127.0.0.1:0".parse().expect("valid socket addr"),
        );
        let resp = handler
            .handle(peer, Message::AccordApplyV2(Bytes::from(bytes)))
            .await;

        assert!(
            applier.durably_applied(&txn_id),
            "precondition: the storage write must have durably applied the rows, so the \
             withheld ack below is a lie about durability"
        );
        assert!(
            matches!(resp, Some(Message::AccordApplyOK(_))),
            "a replica holding a DURABLY WRITTEN row must not withhold ApplyOK because the \
             protocol-log Applied marker's fsync failed — that is the abandon-with-durable-data \
             hazard (G1); got {resp:?}"
        );
    }
}
