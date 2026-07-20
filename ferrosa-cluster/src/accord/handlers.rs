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
use ferrosa_common::accord::Timestamp;

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
///
/// The session's `SessionCore` is built *before* the controller forms the
/// cluster and creates the state, so the two cannot share a plain `AccordState`
/// at construction time. This slot is created empty up front, handed to both
/// sides, and filled by the controller at formation; the committer reads it on
/// demand. Empty until formation (and in standalone/tests), in which case the
/// committer falls back to remote-only votes (correct when peers are the
/// replicas).
pub type AccordStateSlot = Arc<arc_swap::ArcSwapOption<parking_lot::Mutex<AccordStateMachine>>>;

/// An empty [`AccordStateSlot`] — the initial state before the controller
/// publishes this node's live `AccordState`.
pub fn empty_accord_state_slot() -> AccordStateSlot {
    Arc::new(arc_swap::ArcSwapOption::empty())
}

/// Publish `state` into `slot` and return it, so the node's [`AccordHandler`]
/// and the session-layer committer observe the **same** `AccordStateMachine`
/// instance. The controller calls this once during cluster formation, then
/// serves the returned state from its handler — guaranteeing the coordinator's
/// local self-vote uses exactly the state its remote peers see.
pub fn publish_accord_state(slot: &AccordStateSlot, state: AccordState) -> AccordState {
    slot.store(Some(state.clone()));
    state
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

/// Total bound on how long a `ReadVote` dep-wait will block for conflicting
/// transactions to reach `Applied` before abstaining (fail-loud).
const READ_DEP_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
/// and to grab the apply-notify handle, then released BEFORE every `.await`.
/// `handle_apply` (which fires the notify that unblocks us) takes the same lock,
/// so holding it across the await would deadlock.
pub async fn await_conflicting_deps_applied(state: &AccordState, key: &[u8], t: Timestamp) -> bool {
    let deadline = tokio::time::Instant::now() + READ_DEP_WAIT_TIMEOUT;
    loop {
        // Compute the pending set and grab the notify UNDER the lock, then drop
        // the lock before awaiting. The future only enrolls on first poll, so a
        // wake fired between unlock and poll could be missed; the bounded poll
        // timeout below makes such a missed wake self-correcting (the loop
        // re-checks the condition under the lock) rather than a hang.
        let notify = {
            let sm = state.lock();
            if sm.unapplied_conflicts_before(key, &t).is_empty() {
                return true;
            }
            sm.applied_notify()
        };

        let now = tokio::time::Instant::now();
        if now >= deadline {
            tracing::error!(
                "accord: ReadVote dep-wait timed out after {:?} waiting for conflicting \
                 transactions to apply — abstaining (fail-loud)",
                READ_DEP_WAIT_TIMEOUT
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

#[async_trait]
impl RpcHandler for AccordHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::AccordPreAccept(b) => {
                let payload: PreAcceptPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordPreAccept: deserialize failed: {e}"))
                    .ok()?;
                let mut sm = self.state.lock();
                let resp = sm.handle_preaccept(
                    payload.txn_id,
                    payload.t0,
                    &payload.key,
                    payload.ballot,
                    payload.epoch,
                );
                drop(sm);
                match resp {
                    SmResponse::PreAcceptOK { t, deps, .. } => {
                        let ok = PreAcceptOkPayload {
                            from: self.local_node_id,
                            t,
                            deps,
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
                let key_refs: Vec<&[u8]> = payload.keys.iter().map(|k| k.as_slice()).collect();
                let mut sm = self.state.lock();
                let resp = sm.handle_preaccept_multi(
                    payload.txn_id,
                    payload.t0,
                    &key_refs,
                    payload.ballot,
                    payload.epoch,
                );
                drop(sm);
                match resp {
                    SmResponse::PreAcceptOK { t, deps, .. } => {
                        let ok = PreAcceptOkPayload {
                            from: self.local_node_id,
                            t,
                            deps,
                        };
                        let bytes = bincode::serialize(&ok).ok()?;
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
                let mut sm = self.state.lock();
                let _resp = sm.handle_accept(
                    payload.txn_id,
                    payload.t0,
                    payload.t,
                    payload.deps,
                    payload.ballot,
                );
                drop(sm);
                let ok = AcceptOkPayload {
                    txn_id: payload.txn_id,
                };
                let bytes = bincode::serialize(&ok).ok()?;
                Some(Message::AccordAcceptOK(Bytes::from(bytes)))
            }

            Message::AccordCommit(b) => {
                let payload: CommitPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordCommit: deserialize failed: {e}"))
                    .ok()?;
                let mut sm = self.state.lock();
                sm.handle_commit(payload.txn_id, payload.t0, payload.t, payload.deps);
                drop(sm);
                // Commit is fire-and-forget in Accord but we need a response
                // for the request-response transport.
                Some(Message::AccordCommit(Bytes::new()))
            }

            Message::AccordApply(b) => {
                let payload: ApplyPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("AccordApply: deserialize failed: {e}"))
                    .ok()?;
                let txn_id = payload.txn_id;
                let mut sm = self.state.lock();
                sm.handle_apply(txn_id, payload.result_data);
                drop(sm);
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
                let mut sm = self.state.lock();
                sm.handle_apply_writeset(txn_id, writes);
                drop(sm);
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
                let mut sm = self.state.lock();
                let state = sm.handle_recover(payload.txn_id, payload.t0, payload.ballot);
                drop(sm);
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
                    if !await_conflicting_deps_applied(&self.state, &vote_req.key, vote_req.t).await
                    {
                        // Dep-wait timed out: abstain. No current_row, and
                        // condition_holds=false so neither the existence path nor
                        // a generic read fabricates a "row absent" success.
                        let ok = ReadVoteOkPayload {
                            txn_id: vote_req.txn_id,
                            from: self.local_node_id,
                            condition_holds: false,
                            current_row: vec![],
                        };
                        let resp_bytes = bincode::serialize(&ok).ok()?;
                        return Some(Message::AccordReadOK(Bytes::from(resp_bytes)));
                    }

                    let sm = self.state.lock();
                    let (condition_holds, current_row) = match &vote_req.predicate {
                        // INSERT IF NOT EXISTS: existence path (no schema needed).
                        // condition holds iff the row does NOT exist at `t`.
                        ReadPredicate::NotExists => (
                            sm.read_condition_holds_at(&vote_req.key, &vote_req.t),
                            vec![],
                        ),
                        // Unconditional transaction: no IF to evaluate, always
                        // holds. Defensive — the coordinator skips the read-vote
                        // for `Always`, so this arm is not normally reached.
                        ReadPredicate::Always => (true, vec![]),
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
                        ReadPredicate::ReadRow { keyspace, table } => {
                            let row =
                                sm.read_row_bytes_at(keyspace, table, &vote_req.key, vote_req.t);
                            (true, row.unwrap_or_default())
                        }
                    };
                    drop(sm);
                    let ok = ReadVoteOkPayload {
                        txn_id: vote_req.txn_id,
                        from: self.local_node_id,
                        condition_holds,
                        current_row,
                    };
                    let resp_bytes = bincode::serialize(&ok).ok()?;
                    Some(Message::AccordReadOK(Bytes::from(resp_bytes)))
                } else {
                    // Fallback: echo request bytes (backward compat).
                    Some(Message::AccordReadOK(b))
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
    use crate::accord::wire::{ApplyV2Payload, WriteSetEntry};
    use ferrosa_common::accord::{BallotNumber, TxnId, TxnPhase};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
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
        let served = publish_accord_state(&slot, state.clone());

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
}
