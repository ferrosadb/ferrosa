//! Accord protocol wire types — shared between the coordinator and replica
//! handler, serialized via bincode over `ferrosa-net`'s opaque `Bytes` payload.
//!
//! All types in this module are `pub(crate)` — they are an internal
//! serialisation contract and must not leak through the crate's public API.

use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// Coordinator → Replica
// ---------------------------------------------------------------------------

/// PreAccept request sent from coordinator to each replica.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PreAcceptPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) key: Vec<u8>,
    pub(crate) ballot: BallotNumber,
    pub(crate) epoch: u64,
}

/// Accept request sent from coordinator to each replica (slow path).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AcceptPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) t: Timestamp,
    pub(crate) deps: Vec<TxnId>,
    pub(crate) ballot: BallotNumber,
}

/// Commit broadcast from coordinator to all replicas.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct CommitPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) t: Timestamp,
    pub(crate) deps: Vec<TxnId>,
}

/// Apply request broadcast from coordinator to all replicas.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ApplyPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) result_data: Vec<u8>,
}

/// Recovery probe from a recovery coordinator.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct RecoverPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) ballot: BallotNumber,
}

// ---------------------------------------------------------------------------
// Replica → Coordinator
// ---------------------------------------------------------------------------

/// PreAcceptOK response from a replica.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PreAcceptOkPayload {
    /// The replica's node ID, so the coordinator knows who responded.
    pub(crate) from: u64,
    /// Replica's proposed execution timestamp (may differ from t0 if conflict).
    pub(crate) t: Timestamp,
    /// Dependency set detected by this replica.
    pub(crate) deps: Vec<TxnId>,
}

/// AcceptOK response from a replica (slow path).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AcceptOkPayload {
    pub(crate) txn_id: TxnId,
}
