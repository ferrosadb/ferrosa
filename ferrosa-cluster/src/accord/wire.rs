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

// ---------------------------------------------------------------------------
// Gap 4: Linearizable read-vote (coordinator → replica → coordinator)
// ---------------------------------------------------------------------------

/// Read-vote request: coordinator asks each replica to read the current row
/// value *within the Accord epoch* so that the IF condition can be evaluated
/// linearly across F+1 replicas at the agreed execution timestamp `t`.
///
/// Sent from coordinator to each replica after consensus (Commit phase) but
/// before the LWT result is returned to the client.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadVotePayload {
    pub(crate) txn_id: TxnId,
    /// Agreed execution timestamp (from Commit).
    pub(crate) t: Timestamp,
    /// Partition key bytes.
    pub(crate) key: Vec<u8>,
}

/// Read-vote response from a replica.
///
/// Each replica reads the row at timestamp `t` (after waiting for all deps
/// to be applied) and reports whether the IF condition held.
///
/// For `INSERT IF NOT EXISTS`, `condition_holds` is true iff the row did NOT
/// exist at timestamp `t` (i.e., the write should apply).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadVoteOkPayload {
    pub(crate) txn_id: TxnId,
    /// The replica that sent this response.
    pub(crate) from: u64,
    /// True if the IF condition held (the write should be applied).
    pub(crate) condition_holds: bool,
    /// Serialized current row value (empty when condition holds, populated
    /// when it does not — used to build the [applied]=false result set).
    pub(crate) current_row: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Gap 5: Apply-phase acknowledgement (coordinator → replica → coordinator)
// ---------------------------------------------------------------------------

/// ApplyOK response from a replica (used by coordinator to wait for F+1
/// apply acknowledgements before returning the LWT result to the client).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ApplyOkPayload {
    pub(crate) txn_id: TxnId,
    /// The replica that sent this acknowledgement.
    pub(crate) from: u64,
}
