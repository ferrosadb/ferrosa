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
    /// Resume token for chunked fetch: the server reads partitions
    /// whose tokens fall in `[cursor.unwrap_or(range_start), range_end)`.
    /// `None` (first call) means start at `range_start`.
    #[serde(default)]
    pub cursor: Option<i64>,
    /// Hard cap on partitions returned in this chunk. The client
    /// loops fetching chunks until `next_cursor` is `None`. Sized
    /// so the working set per chunk stays well under the per-node
    /// memory cap regardless of partition size; the executor sets
    /// this to `REPAIR_FETCH_CHUNK_PARTITIONS`.
    #[serde(default = "default_fetch_limit")]
    pub limit: u32,
    /// Hard cap on estimated partition *bytes* returned in this chunk.
    /// Complements `limit` so a span of wide partitions can't blow the peer's
    /// memory: the server stops early (reporting `next_cursor`) once either
    /// bound is reached. Defaults — for peers that don't send one — to
    /// `REPAIR_FETCH_CHUNK_BYTES`.
    #[serde(default = "default_fetch_max_bytes")]
    pub max_bytes: u64,
}

/// Backwards-compatible default for the `limit` field on payloads
/// deserialised from peers that don't yet send one. Matches
/// `RepairStore::READ_RANGE_CHUNK_DEFAULT`.
fn default_fetch_limit() -> u32 {
    super::executor::REPAIR_FETCH_CHUNK_PARTITIONS as u32
}

/// Backwards-compatible default for the `max_bytes` field on payloads from
/// peers that predate the byte budget.
fn default_fetch_max_bytes() -> u64 {
    super::executor::REPAIR_FETCH_CHUNK_BYTES as u64
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairFetchResponsePayload {
    pub partitions: Vec<PartitionWire>,
    /// Resume token for the next chunk. `None` indicates the
    /// server returned every remaining partition in
    /// `[cursor, range_end)` — caller should stop looping.
    #[serde(default)]
    pub next_cursor: Option<i64>,
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
        // Share the same build budget as the initiator path —
        // every repair touches the local table twice (once for
        // this node's own session, once on behalf of each peer
        // initiating against this node), so without one semaphore
        // governing both, a cluster of N stacks N full-table
        // walks here per repair run.
        let _permit = match super::REPAIR_BUILD_SEMAPHORE.acquire().await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(%e, "RepairMerkleHandler: build semaphore closed");
                return None;
            }
        };
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
        // Token-bounded chunked read: the client loops calling
        // Fetch with `cursor` carried forward from the prior
        // response's `next_cursor` until we report `next_cursor:
        // None`. Each chunk's `limit` caps the number of
        // partitions in flight so per-RPC memory is bounded.
        let chunk_start = req.cursor.unwrap_or(req.range_start);
        let limit = req.limit.max(1) as usize;
        let max_bytes = (req.max_bytes as usize).max(1);
        // Token-ordered, byte-bounded read: the engine stops at `limit`
        // partitions or `max_bytes` of content (whichever comes first) and
        // returns the token-ordered resume cursor directly, so each chunk's
        // working set is bounded by bytes regardless of partition width. The
        // client loops on `next_cursor` until it is `None`.
        let (in_range_partitions, next_cursor) = match StorageEngine::read_token_range_bounded(
            &self.storage,
            &table_id,
            chunk_start,
            req.range_end,
            limit,
            max_bytes,
        ) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%e, ?table_id, "RepairFetchHandler: read_token_range_bounded failed");
                return None;
            }
        };
        // Emit the response with the borrowed serializer: no `Vec<PartitionWire>`
        // and no `Arc::unwrap_or_clone`, so the repair fetch stops deep-copying
        // every cell value of every returned partition (~5.8 % of node CPU in
        // `Partition::clone` + `Vec<Row>::clone` in the FP capture). The bytes
        // are identical to `bincode::serialize(&RepairFetchResponsePayload{..})`
        // — pinned by `repair_fetch_response_borrowed_emit_matches_legacy`.
        let mut body: Vec<u8> = Vec::new();
        let count = in_range_partitions.len() as u64;
        bincode::serialize_into(&mut body, &count)
            .expect("RepairFetchResponsePayload length serialization is infallible");
        for partition in in_range_partitions.iter() {
            if let Err(e) =
                crate::raft::handlers::serialize_partition_to_wire_borrowed(&mut body, partition)
            {
                tracing::warn!(%e, "RepairFetchHandler: failed to serialize response");
                return None;
            }
        }
        bincode::serialize_into(&mut body, &next_cursor)
            .expect("RepairFetchResponsePayload cursor serialization is infallible");
        Some(Message::RepairFetchResponse(Bytes::from(body)))
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
        let mut applied: u64 = 0;
        let mut last_err: Option<String> = None;
        // The whole partition — deletion, static row and rows. `PartitionWire`
        // carries all three; writing rows alone dropped the other two (P0-3).
        for partition in req.partitions.into_iter().map(partition_from_wire) {
            match self.storage.apply_partition(&table_id, partition) {
                Ok(written) => applied += written as u64,
                Err(e) => {
                    tracing::warn!(%e, ?table_id, "RepairApplyHandler: partition apply failed");
                    last_err = Some(format!("apply write: {e}"));
                    // Continue applying the rest — a single failed partition
                    // shouldn't lose convergence on the others.
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
        // Loop the chunked path until exhausted, accumulating the
        // result. Callers that care about memory should use
        // `read_range_chunked` directly. Kept for compatibility +
        // tests that want the one-shot semantics.
        let mut out: Vec<Partition> = Vec::new();
        let mut cursor: Option<i64> = None;
        loop {
            let (chunk, next) = self
                .read_range_chunked(
                    table,
                    range_start,
                    range_end,
                    cursor,
                    super::executor::REPAIR_FETCH_CHUNK_PARTITIONS,
                )
                .await?;
            out.extend(chunk);
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(out)
    }

    async fn read_range_chunked(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        cursor: Option<i64>,
        limit: usize,
    ) -> Result<(Vec<Partition>, Option<i64>), String> {
        let req = RepairFetchRequestPayload {
            keyspace: table.keyspace.clone(),
            table: table.table.clone(),
            range_start,
            range_end,
            cursor,
            limit: limit.min(u32::MAX as usize) as u32,
            max_bytes: super::executor::REPAIR_FETCH_CHUNK_BYTES as u64,
        };
        let body = bincode::serialize(&req).map_err(|e| format!("serialize: {e}"))?;
        // Lane::Bulk: chunked responses are still bulk (each
        // chunk carries up to `limit` decoded partitions) but
        // the per-chunk size is bounded by the executor's choice
        // of `REPAIR_FETCH_CHUNK_PARTITIONS`. The 60 s timeout
        // sits comfortably above the per-chunk encode + read
        // latency even on a multi-GB replica.
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
        let parts: Vec<Partition> = resp
            .partitions
            .into_iter()
            .map(partition_from_wire)
            .collect();
        Ok((parts, resp.next_cursor))
    }

    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String> {
        // Chunk the apply payload so the wire body, the peer's
        // bincode-deserialise buffer, and the peer's in-flight
        // applied state all stay bounded. Without this the apply
        // RPC carries the full diff for a span — fine when spans
        // are small, but the moment a span happens to be dense
        // the peer materialises that whole payload in memory
        // (decoded `PartitionWire` → `Partition`) and the cgroup
        // OOMs.
        for chunk in partitions.chunks(super::executor::REPAIR_APPLY_CHUNK_PARTITIONS) {
            let req = RepairApplyRequestPayload {
                keyspace: table.keyspace.clone(),
                table: table.table.clone(),
                partitions: chunk.iter().cloned().map(partition_to_wire).collect(),
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
            if let Some(e) = resp.error {
                return Err(e);
            }
        }
        Ok(())
    }

    async fn build_merkle(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<super::merkle::MerkleTree, String> {
        let req = RepairMerkleRequestPayload {
            keyspace: table.keyspace.clone(),
            table: table.table.clone(),
            range_start,
            range_end,
        };
        let body = bincode::serialize(&req).map_err(|e| format!("serialize: {e}"))?;
        // Lane::Bulk: a Merkle build over a multi-GB replica can
        // legitimately take seconds — same lane that Fetch/Apply
        // already use so a single repair session shares one TCP
        // pipe with its data exchange. Response payload is tiny
        // (TREE_DEPTH=15 → ~256 KB of leaf hashes).
        let resp = self
            .peer_manager
            .send(
                self.host_id,
                Message::RepairMerkleRequest(Bytes::from(body)),
                Lane::Bulk,
            )
            .await
            .map_err(|e| format!("send: {e}"))?;
        let body = match resp {
            Message::RepairMerkleResponse(b) => b,
            other => {
                return Err(format!(
                    "expected RepairMerkleResponse, got {:?}",
                    std::mem::discriminant(&other)
                ))
            }
        };
        let resp: RepairMerkleResponsePayload =
            bincode::deserialize(&body).map_err(|e| format!("deserialize: {e}"))?;
        Ok(resp.tree)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A multi-row, multi-cell partition: exercises every layer of the wire
    /// encoding so a byte-equivalence assertion below is not trivially true.
    fn repair_fetch_fixture() -> Partition {
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"pk-bytes".to_vec())),
            deletion: DeletionTime::new(7, 7),
            static_row: Some(Row {
                clustering: Vec::new(),
                cells: vec![(9, CellValue::live(b"static".to_vec(), 999))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(999),
            }),
            rows: (0..3)
                .map(|i| Row {
                    clustering: vec![i as u8],
                    cells: vec![
                        (
                            0,
                            CellValue::live(format!("v-{i}").into_bytes(), 12_345 + i as i64),
                        ),
                        (
                            1,
                            CellValue::live(format!("w-{i}").into_bytes(), 77 + i as i64)
                                .with_path(b"elem".to_vec()),
                        ),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(12_345 + i as i64),
                })
                .collect(),
        }
    }

    /// The repair Fetch response must emit the SAME bytes whether it is built
    /// by materialising `Vec<PartitionWire>` (which clones every cell value) or
    /// by streaming each partition through the borrowed serializer. The
    /// borrowed path exists so the repair fetch stops deep-cloning ~5.8 % of
    /// node CPU in `Partition::clone`; a byte mismatch would corrupt repair.
    #[test]
    fn repair_fetch_response_borrowed_emit_matches_legacy() {
        let parts = vec![repair_fetch_fixture(), repair_fetch_fixture()];

        // Legacy: clone each partition into `PartitionWire`, then serialize.
        let legacy = bincode::serialize(&RepairFetchResponsePayload {
            partitions: parts.iter().cloned().map(partition_to_wire).collect(),
            next_cursor: Some(1234),
        })
        .unwrap();

        // Borrowed: `partitions.len()` then each partition by reference,
        // then `next_cursor` — byte-identical to the struct serialization.
        let mut borrowed: Vec<u8> = Vec::new();
        let count = parts.len() as u64;
        bincode::serialize_into(&mut borrowed, &count).unwrap();
        for p in &parts {
            crate::raft::handlers::serialize_partition_to_wire_borrowed(&mut borrowed, p).unwrap();
        }
        bincode::serialize_into(&mut borrowed, &Some(1234i64)).unwrap();

        assert_eq!(
            borrowed, legacy,
            "borrowed repair-fetch emission diverged from the legacy collect"
        );
    }

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

    /// P0-3: `PartitionWire` carries the static row and partition deletion
    /// over the wire, but the remote apply handler wrote `partition.rows`
    /// only, so both were dropped on the replica being repaired.
    #[tokio::test]
    async fn remote_repair_apply_lands_static_row_and_partition_deletion() {
        use crate::partition_state_fixture as fx;
        let dir = tempfile::tempdir().unwrap();
        let storage = fx::storage(dir.path());
        fx::seed_stale_row(&storage);

        let req = RepairApplyRequestPayload {
            keyspace: fx::KEYSPACE.to_string(),
            table: fx::TABLE.to_string(),
            partitions: vec![partition_to_wire(fx::source_partition())],
        };
        let body = Bytes::from(bincode::serialize(&req).unwrap());
        let reply = RepairApplyHandler::new(storage.clone())
            .handle(
                (Uuid::nil(), "127.0.0.1:7000".parse().unwrap()),
                Message::RepairApplyRequest(body),
            )
            .await
            .expect("apply handler replies");
        let Message::RepairApplyResponse(resp) = reply else {
            panic!("unexpected reply {reply:?}");
        };
        let resp: RepairApplyResponsePayload = bincode::deserialize(&resp).unwrap();
        assert_eq!(resp.error, None);

        fx::assert_receiver_matches_source(&storage, "repair RPC apply");
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
