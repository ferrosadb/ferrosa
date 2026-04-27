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

use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use super::state_machine::{AccordStateMachine, SmResponse};
use super::wire::{
    AcceptOkPayload, AcceptPayload, ApplyOkPayload, ApplyPayload, CommitPayload,
    PreAcceptOkPayload, PreAcceptPayload, ReadVoteOkPayload, ReadVotePayload, RecoverPayload,
};

/// Shared mutable access to the Accord state machine.
///
/// Wrapped in a Mutex because the state machine is single-threaded
/// (Accord's per-shard model). In production this would be sharded
/// by token range; for now a single lock suffices.
pub type AccordState = Arc<parking_lot::Mutex<AccordStateMachine>>;

// ---------------------------------------------------------------------------
// AccordHandler — single handler for all 6 inbound Accord message types
// ---------------------------------------------------------------------------

/// Handles all inbound Accord consensus messages by dispatching to the
/// local `AccordStateMachine`.
pub struct AccordHandler {
    state: AccordState,
    local_node_id: u64,
}

impl AccordHandler {
    pub fn new(state: AccordState, local_node_id: u64) -> Self {
        Self {
            state,
            local_node_id,
        }
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
                    let sm = self.state.lock();
                    // Check if any transaction for this key was already applied.
                    // We use txn_count as a proxy: if no transactions have been
                    // applied (Applied phase) for this key's epoch, condition holds.
                    // More precisely: check if there's an Applied txn with a
                    // commit timestamp <= vote_req.t.
                    let condition_holds = sm.read_condition_holds_at(&vote_req.key, &vote_req.t);
                    drop(sm);
                    let ok = ReadVoteOkPayload {
                        txn_id: vote_req.txn_id,
                        from: self.local_node_id,
                        condition_holds,
                        current_row: vec![],
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
