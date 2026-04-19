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
use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};

/// Shared mutable access to the Accord state machine.
///
/// Wrapped in a Mutex because the state machine is single-threaded
/// (Accord's per-shard model). In production this would be sharded
/// by token range; for now a single lock suffices.
pub type AccordState = Arc<parking_lot::Mutex<AccordStateMachine>>;

// ---------------------------------------------------------------------------
// Wire types (bincode-serialized payloads)
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
struct PreAcceptPayload {
    txn_id: TxnId,
    t0: Timestamp,
    key: Vec<u8>,
    ballot: BallotNumber,
    epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PreAcceptOkPayload {
    from: u64,
    t: Timestamp,
    deps: Vec<TxnId>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AcceptPayload {
    txn_id: TxnId,
    t0: Timestamp,
    t: Timestamp,
    deps: Vec<TxnId>,
    ballot: BallotNumber,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AcceptOkPayload {
    txn_id: TxnId,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CommitPayload {
    txn_id: TxnId,
    t0: Timestamp,
    t: Timestamp,
    deps: Vec<TxnId>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ApplyPayload {
    txn_id: TxnId,
    result_data: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RecoverPayload {
    txn_id: TxnId,
    t0: Timestamp,
    ballot: BallotNumber,
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
                let mut sm = self.state.lock();
                sm.handle_apply(payload.txn_id, payload.result_data);
                drop(sm);
                Some(Message::AccordApplyOK(Bytes::new()))
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
                // Read is dispatched to the state machine for conflict tracking.
                // For now, echo the request back as ReadOK.
                Some(Message::AccordReadOK(b))
            }

            _ => None,
        }
    }
}
