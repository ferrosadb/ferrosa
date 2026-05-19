//! Wire protocol for anti-entropy repair.
//!
//! Three request/response pairs:
//!
//! | Initiator → peer        | Peer → initiator         | Purpose                              |
//! |-------------------------|--------------------------|--------------------------------------|
//! | `RepairMerkleRequest`   | `RepairMerkleResponse`   | Build Merkle tree for table+range    |
//! | `RepairFetchRequest`    | `RepairFetchResponse`    | Fetch partitions in a sub-range      |
//! | `RepairApplyRequest`    | `RepairApplyResponse`    | Apply received partitions (LWW)      |
//!
//! Payloads are bincode-serialized to match the existing `ReadRequest` /
//! `ReadResponse` convention. The `MerkleTree` and `PartitionWire` types
//! already implement `Serialize`/`Deserialize` and travel without
//! per-field conversion.
//!
//! Handlers live in this module and are registered into the cluster's
//! [`HandlerRegistry`](ferrosa_net::rpc::handler::HandlerRegistry) during
//! startup. [`RemoteRepairStore`] is the client side — a
//! [`RepairStore`] implementation that talks to a peer
//! over the [`PeerManager`].

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_sstable::types::Partition;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::raft::handlers::{partition_from_wire, partition_to_wire, PartitionWire};

use super::merkle::MerkleTree;
use super::RepairStore;

// ── Payloads ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairMerkleRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub range_start: i64,
    pub range_end: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairMerkleResponsePayload {
    pub tree: MerkleTree,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairFetchRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub range_start: i64,
    pub range_end: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairFetchResponsePayload {
    pub partitions: Vec<PartitionWire>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairApplyRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub partitions: Vec<PartitionWire>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairApplyResponsePayload {
    pub applied: u64,
    pub error: Option<String>,
}

// ── Server-side handlers ─────────────────────────────────────────────────

/// Handler for inbound `RepairMerkleRequest` RPCs.
/// Builds the Merkle tree from local storage and sends it back.
pub struct RepairMerkleHandler {
    storage: Arc<StorageEngine>,
}

impl RepairMerkleHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for RepairMerkleHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RepairMerkleRequest(b) => b,
            _ => return None,
        };
        let req: RepairMerkleRequestPayload = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%e, "RepairMerkleHandler: failed to deserialize request");
                return None;
            }
        };
        let table_id = TableId::new(&req.keyspace, &req.table);
        let tree = match super::build_tree_for_range(
            &self.storage,
            &table_id,
            req.range_start,
            req.range_end,
        ) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(%e, ?table_id, "RepairMerkleHandler: build_tree_for_range failed");
                return None;
            }
        };
        let resp = RepairMerkleResponsePayload { tree };
        match bincode::serialize(&resp) {
            Ok(body) => Some(Message::RepairMerkleResponse(Bytes::from(body))),
            Err(e) => {
                tracing::warn!(%e, "RepairMerkleHandler: failed to serialize response");
                None
            }
        }
    }
}

/// Handler for inbound `RepairFetchRequest` RPCs.
/// Reads partitions in the requested token sub-range and sends them back.
pub struct RepairFetchHandler {
    storage: Arc<StorageEngine>,
}

impl RepairFetchHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for RepairFetchHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RepairFetchRequest(b) => b,
            _ => return None,
        };
        let req: RepairFetchRequestPayload = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%e, "RepairFetchHandler: failed to deserialize request");
                return None;
            }
        };
        let table_id = TableId::new(&req.keyspace, &req.table);
        // Token-bounded read: repair asks "everything in this token
        // sub-range," which the new `read_token_range` primitive answers
        // directly. Limit matches the storage materialisation cap.
        const REPAIR_LEAF_READ_LIMIT: usize = 10_000;
        let in_range_partitions = match StorageEngine::read_token_range(
            &self.storage,
            &table_id,
            req.range_start,
            req.range_end,
            REPAIR_LEAF_READ_LIMIT,
        ) {
            Ok(ps) => ps,
            Err(e) => {
                tracing::warn!(%e, ?table_id, "RepairFetchHandler: read_token_range failed");
                return None;
            }
        };
        let in_range: Vec<PartitionWire> = in_range_partitions
            .into_iter()
            .map(partition_to_wire)
            .collect();
        let resp = RepairFetchResponsePayload {
            partitions: in_range,
        };
        match bincode::serialize(&resp) {
            Ok(body) => Some(Message::RepairFetchResponse(Bytes::from(body))),
            Err(e) => {
                tracing::warn!(%e, "RepairFetchHandler: failed to serialize response");
                None
            }
        }
    }
}

/// Handler for inbound `RepairApplyRequest` RPCs.
/// Applies the received partitions to local storage (LWW per-cell).
pub struct RepairApplyHandler {
    storage: Arc<StorageEngine>,
}

impl RepairApplyHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for RepairApplyHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RepairApplyRequest(b) => b,
            _ => return None,
        };
        let req: RepairApplyRequestPayload = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%e, "RepairApplyHandler: failed to deserialize request");
                return None;
            }
        };
        let table_id = TableId::new(&req.keyspace, &req.table);
        let partitions: Vec<Partition> = req
            .partitions
            .into_iter()
            .map(partition_from_wire)
            .collect();
        let mut applied: u64 = 0;
        let mut last_err: Option<String> = None;
        for partition in &partitions {
            for row in &partition.rows {
                let ts = row
                    .cells
                    .iter()
                    .map(|(_, c)| c.timestamp)
                    .max()
                    .unwrap_or(row.primary_key_liveness.timestamp);
                if let Err(e) = self
                    .storage
                    .write(&table_id, &partition.key, row.clone(), ts)
                {
                    last_err = Some(format!("apply write: {e}"));
                    // Continue applying the rest — a single failed row
                    // shouldn't lose convergence on the others.
                } else {
                    applied += 1;
                }
            }
        }
        let resp = RepairApplyResponsePayload {
            applied,
            error: last_err,
        };
        match bincode::serialize(&resp) {
            Ok(body) => Some(Message::RepairApplyResponse(Bytes::from(body))),
            Err(e) => {
                tracing::warn!(%e, "RepairApplyHandler: failed to serialize response");
                None
            }
        }
    }
}

// ── Client-side RepairStore impl ─────────────────────────────────────────

/// [`RepairStore`] implementation that reaches a remote peer via the
/// [`PeerManager`]. Slot-in replacement for an `Arc<StorageEngine>` on
/// the `remotes` map of [`super::executor::LocalRepairExecutor`] — once
/// this is wired, the same `LocalRepairExecutor` becomes a real cross-node
/// repair executor.
pub struct RemoteRepairStore {
    pub host_id: Uuid,
    pub peer_manager: Arc<PeerManager>,
}

#[async_trait]
impl RepairStore for RemoteRepairStore {
    async fn read_range(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String> {
        let req = RepairFetchRequestPayload {
            keyspace: table.keyspace.clone(),
            table: table.table.clone(),
            range_start,
            range_end,
        };
        let body = bincode::serialize(&req).map_err(|e| format!("serialize: {e}"))?;
        // Lane::Bulk: a Fetch response carries up to REPAIR_LEAF_READ_LIMIT
        // (10 000) partitions — that's bulk transfer semantics, not a
        // transactional read. Lane::Data's 10s timeout fires before a
        // multi-GB-replica peer can finish its read_token_range scan,
        // every session's caller drops, and repair never converges.
        // Bulk gives 60s, which matches SSTable streaming's lane choice.
        let resp = self
            .peer_manager
            .send(
                self.host_id,
                Message::RepairFetchRequest(Bytes::from(body)),
                Lane::Bulk,
            )
            .await
            .map_err(|e| format!("send: {e}"))?;
        let body = match resp {
            Message::RepairFetchResponse(b) => b,
            other => {
                return Err(format!(
                    "expected RepairFetchResponse, got {:?}",
                    std::mem::discriminant(&other)
                ))
            }
        };
        let resp: RepairFetchResponsePayload =
            bincode::deserialize(&body).map_err(|e| format!("deserialize: {e}"))?;
        Ok(resp
            .partitions
            .into_iter()
            .map(partition_from_wire)
            .collect())
    }

    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String> {
        let req = RepairApplyRequestPayload {
            keyspace: table.keyspace.clone(),
            table: table.table.clone(),
            partitions: partitions.iter().cloned().map(partition_to_wire).collect(),
        };
        let body = bincode::serialize(&req).map_err(|e| format!("serialize: {e}"))?;
        let resp = self
            .peer_manager
            .send(
                self.host_id,
                Message::RepairApplyRequest(Bytes::from(body)),
                Lane::Bulk,
            )
            .await
            .map_err(|e| format!("send: {e}"))?;
        let body = match resp {
            Message::RepairApplyResponse(b) => b,
            other => {
                return Err(format!(
                    "expected RepairApplyResponse, got {:?}",
                    std::mem::discriminant(&other)
                ))
            }
        };
        let resp: RepairApplyResponsePayload =
            bincode::deserialize(&body).map_err(|e| format!("deserialize: {e}"))?;
        match resp.error {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode + decode the three payload structs to confirm bincode +
    /// serde round-trip cleanly. Catches breaking schema changes early.
    #[test]
    fn merkle_request_payload_roundtrips() {
        let req = RepairMerkleRequestPayload {
            keyspace: "ks".into(),
            table: "t".into(),
            range_start: -1_000,
            range_end: 1_000,
        };
        let bytes = bincode::serialize(&req).unwrap();
        let decoded: RepairMerkleRequestPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.keyspace, "ks");
        assert_eq!(decoded.table, "t");
        assert_eq!(decoded.range_start, -1_000);
        assert_eq!(decoded.range_end, 1_000);
    }

    #[test]
    fn merkle_response_payload_roundtrips() {
        let mut tree = MerkleTree::new(4, 0, 1_000);
        tree.insert(500, 0xCAFE_BABE);
        tree.compute_root();
        let resp = RepairMerkleResponsePayload { tree: tree.clone() };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: RepairMerkleResponsePayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.tree, tree);
    }

    #[test]
    fn apply_response_carries_error_flag() {
        let ok = RepairApplyResponsePayload {
            applied: 42,
            error: None,
        };
        let bytes = bincode::serialize(&ok).unwrap();
        let decoded: RepairApplyResponsePayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.applied, 42);
        assert!(decoded.error.is_none());

        let err = RepairApplyResponsePayload {
            applied: 0,
            error: Some("disk full".into()),
        };
        let bytes = bincode::serialize(&err).unwrap();
        let decoded: RepairApplyResponsePayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.applied, 0);
        assert_eq!(decoded.error.as_deref(), Some("disk full"));
    }
}
