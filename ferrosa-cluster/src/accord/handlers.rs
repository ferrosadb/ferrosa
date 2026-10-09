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

use super::state_machine::{AccordStateMachine, SmResponse};
use super::wire::{
    AcceptOkPayload, AcceptPayload, ApplyOkPayload, ApplyPayload, ApplyV2Payload, CommitPayload,
    PreAcceptOkPayload, PreAcceptPayload, PreAcceptV2Payload, ReadVoteOkPayload, ReadVotePayload,
    RecoverPayload,
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

/// Default upper bound on how long a transaction may wait for its ordered
/// dependencies to reach `Applied` before it is abandoned.
///
/// When the bound expires the transaction is **not** left as a permanent
/// blocker: it is rolled back (never applied) and the client is told it was not
/// committed so it can retry.
///
/// Single-sourced from [`crate::accord::epoch_drain::DEFAULT_TXN_TIMEOUT`] on
/// purpose. The two are one policy: the epoch drain period is sized as
/// `SkewMax + DEFAULT_TXN_TIMEOUT` so an in-flight transaction is never cut off
/// by a drain shorter than the transaction bound. An operator who raises the
/// bound via `FERROSA_ACCORD_TXN_TIMEOUT_SECS` (config `[accord]
/// txn_timeout_secs`) must raise the drain with it.
pub const DEFAULT_TXN_TIMEOUT: std::time::Duration =
    crate::accord::epoch_drain::DEFAULT_TXN_TIMEOUT;

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

/// Block until every conflicting transaction ordered before `t` (`t0 < t`) has
/// reached `Applied` on the replica behind `state`, or until
/// `READ_DEP_WAIT_TIMEOUT` elapses.
///
/// Returns `true` if all conflicts applied (a read-at-`t` may now proceed
/// linearizably), `false` on timeout (the caller MUST ABSTAIN — never read
/// stale). Shared by the inbound `AccordRead` handler (remote replicas) and by
/// the coordinator's own local read-vote (its self-send Apply is unreachable, so
/// it reads its local state machine directly).
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
        crate::accord::state_machine::configured_txn_timeout(),
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Compute the pending set and grab the notify UNDER the lock, then drop
        // the lock before awaiting. The future only enrolls on first poll, so a
        // wake fired between unlock and poll could be missed; the bounded poll
        // timeout below makes such a missed wake self-correcting (the loop
        // re-checks the condition under the lock) rather than a hang.
        let owned_key = key.to_vec();
        let pending = on_state_machine(state, move |sm| {
            if sm.unapplied_conflicts_before(&owned_key, &t).is_empty() {
                None
            } else {
                Some(sm.applied_notify())
            }
        })
        .await;
        let notify = match pending {
            Some(None) => return true,
            Some(Some(notify)) => notify,
            // Cancelled at shutdown (already logged): abstain.
            None => return false,
        };

        let now = tokio::time::Instant::now();
        if now >= deadline {
            tracing::error!(
                "accord: ReadVote dep-wait timed out after {:?} waiting for conflicting \
                 transactions to apply — abstaining (fail-loud)",
                timeout
            );
            return false;
        }

        // Wait for the next apply (broadcast) or the per-iteration poll cap,
        // whichever comes first, but never past the overall deadline.
        let wait = READ_DEP_WAIT_POLL.min(deadline - now);
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
            sm.get_state(&txn_id).map(|txn| {
                (
                    txn.phase,
                    txn.deps.len(),
                    txn.result.as_ref().map_or(0, Vec::len),
                    sm.applied_notify(),
                )
            })
        })
        .await;
        let (phase, dependency_count, result_bytes, notify) = match observed {
            Some(Some(txn)) => txn,
            Some(None) => {
                tracing::error!(
                    txn_id = ?txn_id,
                    "accord: Apply target is absent from local state — refusing ApplyOK"
                );
                return false;
            }
            // Cancelled at shutdown (already logged): refuse the ack.
            None => return false,
        };
        if phase == TxnPhase::Applied {
            return true;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            let phase = Some(phase);
            tracing::error!(
                txn_id = ?txn_id,
                ?phase,
                dependency_count,
                result_bytes,
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
                on_state_machine(&self.state, move |sm| {
                    sm.handle_commit(payload.txn_id, payload.t0, payload.t, payload.deps)
                })
                .await?;
                // Commit is fire-and-forget in Accord but we need a response
                // for the request-response transport.
                Some(Message::AccordCommit(Bytes::new()))
            }

            Message::AccordApply(b) => {
                let payload: ApplyPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordApply: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                let apply_status = on_state_machine(&self.state, move |sm| {
                    sm.handle_apply(txn_id, payload.result_data)
                })
                .await?;
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
                // Multi-key Apply: the coordinator already scoped this payload to
                // exactly the keys this replica is a participant for (per-replica
                // filtered fan-out), so the replica applies every write it was
                // sent — the same "coordinator scopes, replica trusts" invariant
                // as the v1 AccordApply arm, generalized to N partitions. The
                // writes are routed as ONE write-set so they park/apply atomically
                // (DATA-LOSS-CRITICAL: writes 2..N must never be dropped).
                let payload: ApplyV2Payload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordApplyV2: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                let writes: Vec<Vec<u8>> = payload.writes.into_iter().map(|w| w.mutation).collect();
                let apply_status = on_state_machine(&self.state, move |sm| {
                    sm.handle_apply_writeset(txn_id, writes)
                })
                .await?;
                if !matches!(
                    apply_status,
                    crate::accord::state_machine::SmResponse::NoWriteFinalized
                ) && !await_txn_applied(&self.state, txn_id).await
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
}
