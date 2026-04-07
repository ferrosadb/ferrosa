//! RPC handlers that bridge ferrosa-net messages to openraft API calls and
//! local storage reads.
//!
//! Each handler implements [`RpcHandler`] and is registered in the
//! `HandlerRegistry` during cluster initialization (Task 2).  The handlers
//! are the inbound counterparts of the outbound serialization done in
//! [`super::network`].
//!
//! # Handler Map
//!
//! | Message variant            | Handler              | Response variant          |
//! |----------------------------|----------------------|---------------------------|
//! | `RaftAppendEntries(bytes)` | [`RaftAppendHandler`]| `RaftAppendResponse(bytes)`|
//! | `RaftVote(bytes)`          | [`RaftVoteHandler`]  | `RaftVoteResponse(bytes)` |
//! | `RaftInstallSnapshot(bytes)`| [`RaftSnapshotHandler`]| `RaftAppendResponse(bytes)`|
//! | `ReadRequest(bytes)`       | [`ReadRequestHandler`]| `ReadResponse(bytes)`    |
//!
//! # Serialization of Partition Data
//!
//! [`ferrosa_sstable::types::Partition`] and its sub-types do not implement
//! `serde::Serialize`/`Deserialize` (those crates have no serde dependency).
//! `ReadResponsePayload` therefore carries a [`PartitionWire`] type that
//! mirrors the partition structure using only owned primitives and can be
//! serialized with bincode.  The conversion helpers [`partition_to_wire`] and
//! [`partition_from_wire`] convert between the two representations.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use openraft::raft::{
    AppendEntriesRequest, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use serde::{Deserialize, Serialize};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use super::FerrosRaftConfig;

// ---------------------------------------------------------------------------
// Wire types (serde-capable mirrors for sstable types)
// ---------------------------------------------------------------------------

/// Serializable mirror of [`DeletionTime`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeletionTimeWire {
    pub marked_for_delete_at: i64,
    pub local_deletion_time: u32,
}

impl From<DeletionTime> for DeletionTimeWire {
    fn from(d: DeletionTime) -> Self {
        Self {
            marked_for_delete_at: d.marked_for_delete_at,
            local_deletion_time: d.local_deletion_time,
        }
    }
}

impl From<DeletionTimeWire> for DeletionTime {
    fn from(w: DeletionTimeWire) -> Self {
        DeletionTime {
            marked_for_delete_at: w.marked_for_delete_at,
            local_deletion_time: w.local_deletion_time,
        }
    }
}

/// Serializable mirror of [`LivenessInfo`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LivenessInfoWire {
    pub timestamp: i64,
    pub ttl: i32,
    pub local_deletion_time: i32,
}

impl From<LivenessInfo> for LivenessInfoWire {
    fn from(l: LivenessInfo) -> Self {
        Self {
            timestamp: l.timestamp,
            ttl: l.ttl,
            local_deletion_time: l.local_deletion_time,
        }
    }
}

impl From<LivenessInfoWire> for LivenessInfo {
    fn from(w: LivenessInfoWire) -> Self {
        LivenessInfo {
            timestamp: w.timestamp,
            ttl: w.ttl,
            local_deletion_time: w.local_deletion_time,
        }
    }
}

/// Serializable mirror of [`CellValue`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CellValueWire {
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
    pub ttl: i32,
    pub local_deletion_time: i32,
}

impl From<CellValue> for CellValueWire {
    fn from(c: CellValue) -> Self {
        Self {
            value: c.value,
            timestamp: c.timestamp,
            ttl: c.ttl,
            local_deletion_time: c.local_deletion_time,
        }
    }
}

impl From<CellValueWire> for CellValue {
    fn from(w: CellValueWire) -> Self {
        CellValue {
            value: w.value,
            timestamp: w.timestamp,
            ttl: w.ttl,
            local_deletion_time: w.local_deletion_time,
        }
    }
}

/// Serializable mirror of [`Row`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowWire {
    pub clustering: Vec<u8>,
    pub cells: Vec<(u16, CellValueWire)>,
    pub deletion: DeletionTimeWire,
    pub primary_key_liveness: LivenessInfoWire,
}

impl From<Row> for RowWire {
    fn from(r: Row) -> Self {
        Self {
            clustering: r.clustering,
            cells: r.cells.into_iter().map(|(i, c)| (i, c.into())).collect(),
            deletion: r.deletion.into(),
            primary_key_liveness: r.primary_key_liveness.into(),
        }
    }
}

impl From<RowWire> for Row {
    fn from(w: RowWire) -> Self {
        Row {
            clustering: w.clustering,
            cells: w.cells.into_iter().map(|(i, c)| (i, c.into())).collect(),
            deletion: w.deletion.into(),
            primary_key_liveness: w.primary_key_liveness.into(),
        }
    }
}

/// Serializable mirror of [`Partition`].
///
/// Carries all partition fields as owned primitives so that bincode can
/// serialize the full partition over the wire.  The decorated key is split
/// into `token` (i64) and `key_bytes` (raw bytes of the partition key).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionWire {
    pub token: i64,
    pub key_bytes: Vec<u8>,
    pub deletion: DeletionTimeWire,
    pub static_row: Option<RowWire>,
    pub rows: Vec<RowWire>,
}

/// Convert a [`Partition`] into its wire representation.
pub fn partition_to_wire(p: Partition) -> PartitionWire {
    PartitionWire {
        token: p.key.token.0,
        key_bytes: p.key.key.as_bytes().to_vec(),
        deletion: p.deletion.into(),
        static_row: p.static_row.map(Into::into),
        rows: p.rows.into_iter().map(Into::into).collect(),
    }
}

/// Reconstruct a [`Partition`] from its wire representation.
///
/// The token is taken from the wire; the [`DecoratedKey`] is rebuilt by
/// constructing the key from raw bytes and keeping the transmitted token
/// rather than recomputing it (avoids hashing on the receiver side and
/// preserves the sender's view of the token exactly).
pub fn partition_from_wire(w: PartitionWire) -> Partition {
    let key = PartitionKey::new(w.key_bytes);
    let decorated = DecoratedKey {
        token: Token(w.token),
        key,
    };
    Partition {
        key: decorated,
        deletion: w.deletion.into(),
        static_row: w.static_row.map(Into::into),
        rows: w.rows.into_iter().map(Into::into).collect(),
    }
}

// ---------------------------------------------------------------------------
// Payload types
// ---------------------------------------------------------------------------

/// Payload for a remote read request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequestPayload {
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Raw partition key bytes.
    pub key: Vec<u8>,
    /// If true, return only the CRC32 digest, not the full partition data.
    pub digest_only: bool,
    /// Maximum number of rows to return per response page.
    /// 0 means unlimited (backwards-compatible default).
    #[serde(default)]
    pub page_size: u32,
    /// Opaque page state from a previous response. Empty = start from beginning.
    #[serde(default)]
    pub page_state: Vec<u8>,
}

/// Payload for a remote read response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponsePayload {
    /// True if the partition was found.
    pub found: bool,
    /// Partition data, or `None` if `digest_only` was set or the key was not found.
    pub partition: Option<PartitionWire>,
    /// Newest row timestamp in the partition (microseconds since epoch).
    /// `i64::MIN` if not found.
    pub timestamp: i64,
    /// CRC32 digest of the serialized partition.  `None` if not found.
    pub digest: Option<u32>,
    /// If true, more pages are available. Send another ReadRequest with
    /// `page_state` set to `next_page_state` to get the next chunk.
    #[serde(default)]
    pub has_more: bool,
    /// Opaque state for fetching the next page.
    #[serde(default)]
    pub next_page_state: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Digest helper
// ---------------------------------------------------------------------------

/// Compute a CRC32 digest of a partition by hashing its wire encoding.
///
/// Uses `crc32fast` for speed.  The digest is over the bincode-serialized
/// [`PartitionWire`] so that it is byte-for-byte identical on every node that
/// holds the same partition.
///
/// Returns an error if the partition cannot be serialized to bincode.
pub fn compute_partition_digest(partition: &Partition) -> Result<u32, bincode::Error> {
    // Clone into a wire type so we can serialize without mutating the caller's value.
    let wire = PartitionWire {
        token: partition.key.token.0,
        key_bytes: partition.key.key.as_bytes().to_vec(),
        deletion: DeletionTimeWire {
            marked_for_delete_at: partition.deletion.marked_for_delete_at,
            local_deletion_time: partition.deletion.local_deletion_time,
        },
        static_row: partition.static_row.clone().map(RowWire::from),
        rows: partition.rows.iter().cloned().map(RowWire::from).collect(),
    };

    let bytes = bincode::serialize(&wire)?;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes);
    Ok(hasher.finalize())
}

/// Extract the newest timestamp from any row in the partition (including the
/// static row), or `i64::MIN` if there are no rows.
fn newest_timestamp(partition: &Partition) -> i64 {
    let mut ts = i64::MIN;

    if let Some(ref sr) = partition.static_row {
        let row_ts = row_max_timestamp(sr);
        if row_ts > ts {
            ts = row_ts;
        }
    }

    for row in &partition.rows {
        let row_ts = row_max_timestamp(row);
        if row_ts > ts {
            ts = row_ts;
        }
    }

    ts
}

fn row_max_timestamp(row: &Row) -> i64 {
    let mut ts = if row.primary_key_liveness.has_timestamp() {
        row.primary_key_liveness.timestamp
    } else {
        i64::MIN
    };

    for (_, cell) in &row.cells {
        if cell.timestamp > ts {
            ts = cell.timestamp;
        }
    }

    ts
}

// ---------------------------------------------------------------------------
// LazyRaft — waits for the Raft instance to be initialized
// ---------------------------------------------------------------------------

/// A lazy reference to the Raft instance that becomes available after async
/// initialization completes. Handlers are registered immediately (before
/// `FerrosRaft::new()` returns) and use this to wait for the instance.
#[derive(Clone)]
pub struct LazyRaft {
    rx: tokio::sync::watch::Receiver<Option<Arc<super::FerrosRaft>>>,
}

impl LazyRaft {
    /// Create a new lazy Raft reference and the sender to publish the instance.
    pub fn channel() -> (
        tokio::sync::watch::Sender<Option<Arc<super::FerrosRaft>>>,
        Self,
    ) {
        let (tx, rx) = tokio::sync::watch::channel(None);
        (tx, Self { rx })
    }

    /// Wait for the Raft instance to be initialized, retrying with backoff.
    ///
    /// Makes up to 3 attempts with 5-second backoff between retries (total
    /// ~20 seconds worst case). Returns `None` only after all retries are
    /// exhausted.
    async fn get(&self) -> Option<Arc<super::FerrosRaft>> {
        // If already available, return immediately.
        // Scope the borrow so the RwLockReadGuard is dropped before any await.
        let cached = { self.rx.borrow().clone() };
        if cached.is_some() {
            return cached;
        }

        for attempt in 1..=3u32 {
            // Try a single wait attempt. The helper maps away the non-Send
            // watch::Ref so the extracted result is Send-safe.
            let extracted = Self::try_wait(&self.rx, std::time::Duration::from_secs(5)).await;
            match extracted {
                Ok(value) => return value,
                Err(true) => {
                    // Channel closed.
                    tracing::warn!(
                        attempt,
                        "LazyRaft: channel closed before Raft initialization"
                    );
                    return None;
                }
                Err(false) => {
                    // Timeout.
                    if attempt < 3 {
                        tracing::warn!(
                            attempt,
                            "LazyRaft: timed out waiting for Raft initialization, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }

        tracing::warn!("LazyRaft: all 3 attempts exhausted waiting for Raft initialization");
        None
    }

    /// Single wait attempt that maps the non-Send `watch::Ref` into an owned
    /// value before returning. Returns:
    /// - `Ok(value)` if the watch fires with a value
    /// - `Err(true)` if the channel is closed
    /// - `Err(false)` if the timeout expired
    async fn try_wait(
        rx: &tokio::sync::watch::Receiver<Option<Arc<super::FerrosRaft>>>,
        timeout: std::time::Duration,
    ) -> Result<Option<Arc<super::FerrosRaft>>, bool> {
        let mut rx = rx.clone();
        let result = tokio::time::timeout(timeout, rx.wait_for(|v| v.is_some())).await;
        match result {
            Ok(Ok(guard)) => Ok(guard.clone()),
            Ok(Err(_)) => Err(true),
            Err(_) => Err(false),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftAppendHandler
// ---------------------------------------------------------------------------

/// Handles inbound `RaftAppendEntries` RPCs.
///
/// Deserializes the request with bincode, forwards it to the local Raft
/// instance, and returns the serialized response as `RaftAppendResponse`.
pub struct RaftAppendHandler {
    raft: LazyRaft,
}

impl RaftAppendHandler {
    pub fn new(raft: LazyRaft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RpcHandler for RaftAppendHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RaftAppendEntries(b) => b,
            _ => return None,
        };

        let raft = self.raft.get().await?;

        let req: AppendEntriesRequest<FerrosRaftConfig> = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("RaftAppendHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| {
                tracing::warn!("RaftAppendHandler: append_entries failed: {e}");
                e
            })
            .ok()?;

        let resp_bytes = bincode::serialize(&resp)
            .map_err(|e| {
                tracing::warn!("RaftAppendHandler: failed to serialize response: {e}");
                e
            })
            .ok()?;

        Some(Message::RaftAppendResponse(Bytes::from(resp_bytes)))
    }
}

// ---------------------------------------------------------------------------
// RaftVoteHandler
// ---------------------------------------------------------------------------

/// Handles inbound `RaftVote` RPCs.
///
/// Deserializes the vote request, forwards it to the local Raft instance, and
/// returns the serialized vote response.
pub struct RaftVoteHandler {
    raft: LazyRaft,
}

impl RaftVoteHandler {
    pub fn new(raft: LazyRaft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RpcHandler for RaftVoteHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RaftVote(b) => b,
            _ => return None,
        };

        let raft = self.raft.get().await?;

        let req: VoteRequest<u64> = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("RaftVoteHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let resp: VoteResponse<u64> = raft
            .vote(req)
            .await
            .map_err(|e| {
                tracing::warn!("RaftVoteHandler: vote failed: {e}");
                e
            })
            .ok()?;

        let resp_bytes = bincode::serialize(&resp)
            .map_err(|e| {
                tracing::warn!("RaftVoteHandler: failed to serialize response: {e}");
                e
            })
            .ok()?;

        Some(Message::RaftVoteResponse(Bytes::from(resp_bytes)))
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotHandler
// ---------------------------------------------------------------------------

/// Handles inbound `RaftInstallSnapshot` RPCs.
///
/// Deserializes the snapshot request, forwards it to the local Raft instance,
/// and returns the serialized response as `RaftAppendResponse` (matching the
/// convention used in [`super::network`]).
pub struct RaftSnapshotHandler {
    raft: LazyRaft,
}

impl RaftSnapshotHandler {
    pub fn new(raft: LazyRaft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RpcHandler for RaftSnapshotHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RaftInstallSnapshot(b) => b,
            _ => return None,
        };

        let raft = self.raft.get().await?;

        let req: InstallSnapshotRequest<FerrosRaftConfig> = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("RaftSnapshotHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let resp: InstallSnapshotResponse<u64> = raft
            .install_snapshot(req)
            .await
            .map_err(|e| {
                tracing::warn!("RaftSnapshotHandler: install_snapshot failed: {e}");
                e
            })
            .ok()?;

        let resp_bytes = bincode::serialize(&resp)
            .map_err(|e| {
                tracing::warn!("RaftSnapshotHandler: failed to serialize response: {e}");
                e
            })
            .ok()?;

        // Reuses `RaftAppendResponse` as the snapshot ack wire type, matching
        // the decode side in `FerrosRaftNetwork::install_snapshot`.
        Some(Message::RaftAppendResponse(Bytes::from(resp_bytes)))
    }
}

// ---------------------------------------------------------------------------
// ReadRequestHandler
// ---------------------------------------------------------------------------

/// Handles inbound `ReadRequest` RPCs from remote coordinators.
///
/// Deserializes a [`ReadRequestPayload`], reads the partition from the local
/// [`StorageEngine`], and returns a [`ReadResponsePayload`] encoded as
/// `ReadResponse`.
///
/// If `digest_only` is set the partition data is omitted from the response;
/// only the CRC32 digest and newest timestamp are returned.  This supports
/// the digest-first read repair protocol.
pub struct ReadRequestHandler {
    storage: Arc<StorageEngine>,
}

impl ReadRequestHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for ReadRequestHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::ReadRequest(b) => b,
            _ => return None,
        };

        let req: ReadRequestPayload = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("ReadRequestHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let table_id = TableId::new(&req.keyspace, &req.table);
        let key = DecoratedKey::new(PartitionKey::new(req.key));

        // MUST use read_local — this handler is the RECEIVING end of a
        // remote ReadRequest. Using read() would route through the coordinator
        // which sends another ReadRequest → infinite recursion → stack overflow.
        let payload = match self.storage.read(&table_id, &key) {
            Ok(Some(mut partition)) => {
                let ts = newest_timestamp(&partition);
                let digest = match compute_partition_digest(&partition) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        tracing::warn!("ReadRequestHandler: digest computation failed: {e}");
                        None
                    }
                };

                // Paging: if page_size > 0, return only a page of rows.
                let page_size = req.page_size as usize;
                let page_offset: usize = if req.page_state.len() >= 8 {
                    u64::from_le_bytes(req.page_state[..8].try_into().unwrap_or([0; 8])) as usize
                } else {
                    0
                };

                let (has_more, next_page_state) = if page_size > 0
                    && !req.digest_only
                    && partition.rows.len() > page_offset + page_size
                {
                    // Truncate to this page
                    let end = (page_offset + page_size).min(partition.rows.len());
                    partition.rows = partition.rows[page_offset..end].to_vec();
                    let next_offset = end as u64;
                    (true, next_offset.to_le_bytes().to_vec())
                } else if page_size > 0 && !req.digest_only && page_offset > 0 {
                    // Last page: slice from offset to end
                    partition.rows = partition.rows[page_offset..].to_vec();
                    (false, vec![])
                } else {
                    (false, vec![])
                };

                let wire_partition = if req.digest_only {
                    None
                } else {
                    Some(partition_to_wire(partition))
                };
                ReadResponsePayload {
                    found: true,
                    partition: wire_partition,
                    timestamp: ts,
                    digest,
                    has_more,
                    next_page_state,
                }
            }
            Ok(None) => ReadResponsePayload {
                found: false,
                partition: None,
                timestamp: i64::MIN,
                digest: None,
                has_more: false,
                next_page_state: vec![],
            },
            Err(e) => {
                tracing::warn!("ReadRequestHandler: storage read failed: {e}");
                return None;
            }
        };

        let resp_bytes = bincode::serialize(&payload)
            .map_err(|e| {
                tracing::warn!("ReadRequestHandler: failed to serialize response: {e}");
                e
            })
            .ok()?;

        Some(Message::ReadResponse(Bytes::from(resp_bytes)))
    }
}

// ---------------------------------------------------------------------------
// RangeReadHandler
// ---------------------------------------------------------------------------

/// Payload for a remote range-read request (full-table scan on one node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeReadRequestPayload {
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
}

/// Payload for a remote range-read response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeReadResponsePayload {
    /// All partitions held locally for this table, in token order.
    pub partitions: Vec<PartitionWire>,
    /// `true` when the result set hit the 1M partition cap and may be incomplete.
    #[serde(default)]
    pub truncated: bool,
}

/// Handles inbound `RangeReadRequest` RPCs from remote coordinators.
///
/// Returns all partitions stored locally for the requested table.  The
/// coordinator that sent the request collects responses from all nodes,
/// deduplicates, and aggregates — enabling correct `SELECT COUNT(*)` and
/// other full-table operations on distributed data.
pub struct RangeReadHandler {
    storage: Arc<StorageEngine>,
}

impl RangeReadHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for RangeReadHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RangeReadRequest(b) => b,
            _ => return None,
        };

        let req: RangeReadRequestPayload = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("RangeReadHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let table_id = ferrosa_storage::TableId::new(&req.keyspace, &req.table);

        let partitions = match self.storage.read_range(&table_id, None, None, 1_000_000) {
            Ok(ps) => ps,
            Err(e) => {
                tracing::warn!("RangeReadHandler: read_range failed: {e}");
                // Return empty response instead of None — a None response
                // means NO response at all, causing the coordinator to wait
                // for the full 120s timeout and hang the client.
                vec![]
            }
        };

        let truncated = partitions.len() >= 1_000_000;
        let wire_partitions = partitions.into_iter().map(partition_to_wire).collect();
        let payload = RangeReadResponsePayload {
            partitions: wire_partitions,
            truncated,
        };

        let resp_bytes = match bincode::serialize(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("RangeReadHandler: failed to serialize response: {e}");
                // Serialize an empty response instead of dropping the message.
                bincode::serialize(&RangeReadResponsePayload {
                    partitions: vec![],
                    truncated: false,
                })
                .unwrap_or_default()
            }
        };

        Some(Message::RangeReadResponse(Bytes::from(resp_bytes)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};

    use ferrosa_common::schema::{ColumnDefinition, TableSchema};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_peer_id() -> PeerId {
        (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap())
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    /// Build a simple partition with one row containing one live cell.
    fn make_partition(key_bytes: &[u8], ts: i64) -> Partition {
        let key = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));
        Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"value".to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    // -----------------------------------------------------------------------
    // Wire round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn partition_wire_roundtrip() {
        let original = make_partition(b"mykey", 1_000_000);
        let wire = partition_to_wire(original.clone());
        let reconstructed = partition_from_wire(wire);
        assert_eq!(reconstructed.key.token, original.key.token);
        assert_eq!(
            reconstructed.key.key.as_bytes(),
            original.key.key.as_bytes()
        );
        assert_eq!(reconstructed.rows.len(), 1);
        assert_eq!(
            reconstructed.rows[0].cells[0].1.value,
            original.rows[0].cells[0].1.value
        );
    }

    #[test]
    fn read_request_payload_serde_roundtrip() {
        let req = ReadRequestPayload {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: b"the_key".to_vec(),
            digest_only: false,
            page_size: 0,
            page_state: vec![],
        };
        let bytes = bincode::serialize(&req).expect("serialize");
        let decoded: ReadRequestPayload = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded.keyspace, req.keyspace);
        assert_eq!(decoded.table, req.table);
        assert_eq!(decoded.key, req.key);
        assert_eq!(decoded.digest_only, req.digest_only);
    }

    #[test]
    fn read_response_payload_serde_roundtrip() {
        let partition = make_partition(b"k", 42);
        let resp = ReadResponsePayload {
            found: true,
            partition: Some(partition_to_wire(partition)),
            timestamp: 42,
            digest: Some(0xDEAD_BEEF),
            has_more: false,
            next_page_state: vec![],
        };
        let bytes = bincode::serialize(&resp).expect("serialize");
        let decoded: ReadResponsePayload = bincode::deserialize(&bytes).expect("deserialize");
        assert!(decoded.found);
        assert_eq!(decoded.timestamp, 42);
        assert_eq!(decoded.digest, Some(0xDEAD_BEEF));
        assert!(decoded.partition.is_some());
    }

    // -----------------------------------------------------------------------
    // Digest tests
    // -----------------------------------------------------------------------

    #[test]
    fn partition_digest_is_deterministic() {
        let p = make_partition(b"abc", 999);
        let d1 = compute_partition_digest(&p).unwrap();
        let d2 = compute_partition_digest(&p).unwrap();
        assert_eq!(d1, d2, "same partition must produce the same digest");
    }

    #[test]
    fn partition_digest_detects_value_difference() {
        let p1 = make_partition(b"abc", 999);
        let mut p2 = make_partition(b"abc", 999);
        // Change the cell value
        p2.rows[0].cells[0].1.value = Some(b"different".to_vec());

        let d1 = compute_partition_digest(&p1).unwrap();
        let d2 = compute_partition_digest(&p2).unwrap();
        assert_ne!(
            d1, d2,
            "different partition data must produce different digest"
        );
    }

    #[test]
    fn partition_digest_detects_key_difference() {
        let p1 = make_partition(b"key1", 100);
        let p2 = make_partition(b"key2", 100);
        assert_ne!(
            compute_partition_digest(&p1).unwrap(),
            compute_partition_digest(&p2).unwrap(),
            "different keys must produce different digest"
        );
    }

    #[test]
    fn partition_digest_detects_row_count_difference() {
        let mut p1 = make_partition(b"abc", 500);
        let mut p2 = make_partition(b"abc", 500);

        // Add a second row to p2 so it has more rows than p1.
        p2.rows.push(Row {
            clustering: vec![1],
            cells: vec![(0, CellValue::live(b"extra".to_vec(), 500))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(500),
        });

        // Sanity: the same partition is equal to itself.
        assert_eq!(
            compute_partition_digest(&p1).unwrap(),
            compute_partition_digest(&p1).unwrap(),
            "same partition must be equal to itself"
        );

        // Make sure row counts differ.
        assert_ne!(p1.rows.len(), p2.rows.len());

        // Different row counts must produce different digests.
        assert_ne!(
            compute_partition_digest(&p1).unwrap(),
            compute_partition_digest(&p2).unwrap(),
            "different row counts must produce different digest"
        );

        // Conversely, adding the same row to p1 must make the digests agree.
        p1.rows.push(Row {
            clustering: vec![1],
            cells: vec![(0, CellValue::live(b"extra".to_vec(), 500))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(500),
        });
        assert_eq!(
            compute_partition_digest(&p1).unwrap(),
            compute_partition_digest(&p2).unwrap(),
            "identical row counts and content must produce the same digest"
        );
    }

    // -----------------------------------------------------------------------
    // ReadRequestHandler tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_request_handler_reads_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        // Write a partition directly to storage.
        let table_id = TableId::new("test_ks", "test_tbl");
        let key_bytes = b"my_key".as_slice();
        let dk = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 5000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(5000),
        };
        storage.write(&table_id, &dk, row, 5000).unwrap();

        let handler = ReadRequestHandler::new(storage);
        let req = ReadRequestPayload {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: key_bytes.to_vec(),
            digest_only: false,
            page_size: 0,
            page_state: vec![],
        };
        let req_bytes = bincode::serialize(&req).unwrap();
        let msg = Message::ReadRequest(Bytes::from(req_bytes));

        let response = handler.handle(make_peer_id(), msg).await;
        assert!(response.is_some(), "should return a ReadResponse");

        let Message::ReadResponse(resp_bytes) = response.unwrap() else {
            panic!("expected ReadResponse");
        };
        let resp: ReadResponsePayload = bincode::deserialize(&resp_bytes).unwrap();

        assert!(resp.found, "partition should be found");
        assert!(resp.partition.is_some(), "full partition data expected");
        assert_eq!(resp.timestamp, 5000);
        assert!(resp.digest.is_some(), "digest should be populated");
    }

    #[tokio::test]
    async fn read_request_handler_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let handler = ReadRequestHandler::new(storage);
        let req = ReadRequestPayload {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: b"nonexistent".to_vec(),
            digest_only: false,
            page_size: 0,
            page_state: vec![],
        };
        let req_bytes = bincode::serialize(&req).unwrap();
        let msg = Message::ReadRequest(Bytes::from(req_bytes));

        let response = handler.handle(make_peer_id(), msg).await;
        assert!(response.is_some(), "handler should always respond");

        let Message::ReadResponse(resp_bytes) = response.unwrap() else {
            panic!("expected ReadResponse");
        };
        let resp: ReadResponsePayload = bincode::deserialize(&resp_bytes).unwrap();

        assert!(!resp.found, "key should not be found");
        assert!(resp.partition.is_none());
        assert_eq!(resp.timestamp, i64::MIN);
        assert!(resp.digest.is_none());
    }

    #[tokio::test]
    async fn read_request_handler_digest_only_mode() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key_bytes = b"dk".as_slice();
        let dk = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"data".to_vec(), 9000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(9000),
        };
        storage.write(&table_id, &dk, row, 9000).unwrap();

        let handler = ReadRequestHandler::new(storage);
        let req = ReadRequestPayload {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: key_bytes.to_vec(),
            digest_only: true,
            page_size: 0,
            page_state: vec![],
        };
        let req_bytes = bincode::serialize(&req).unwrap();
        let msg = Message::ReadRequest(Bytes::from(req_bytes));

        let response = handler.handle(make_peer_id(), msg).await;
        let Message::ReadResponse(resp_bytes) = response.unwrap() else {
            panic!("expected ReadResponse");
        };
        let resp: ReadResponsePayload = bincode::deserialize(&resp_bytes).unwrap();

        assert!(resp.found);
        assert!(
            resp.partition.is_none(),
            "digest_only: partition data must be omitted"
        );
        assert!(resp.digest.is_some(), "digest must be present");
        assert_eq!(resp.timestamp, 9000);
    }

    #[tokio::test]
    async fn read_request_handler_ignores_wrong_message_type() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let handler = ReadRequestHandler::new(storage);

        let response = handler
            .handle(
                make_peer_id(),
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
            )
            .await;
        assert!(
            response.is_none(),
            "should return None for non-ReadRequest messages"
        );
    }

    // -----------------------------------------------------------------------
    // Vote serialization round-trip (tests serde codec without live Raft)
    // -----------------------------------------------------------------------

    #[test]
    fn raft_vote_request_serde_roundtrip() {
        use openraft::{CommittedLeaderId, LogId, Vote};

        let req = VoteRequest {
            vote: Vote::new(3, 7),
            last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 0), 12)),
        };

        let bytes = bincode::serialize(&req).expect("serialize VoteRequest");
        let decoded: VoteRequest<u64> =
            bincode::deserialize(&bytes).expect("deserialize VoteRequest");

        assert_eq!(decoded.vote, req.vote);
        assert_eq!(decoded.last_log_id, req.last_log_id);
    }

    #[test]
    fn raft_vote_response_serde_roundtrip() {
        use openraft::Vote;

        let resp = VoteResponse {
            vote: Vote::new(3, 7),
            vote_granted: true,
            last_log_id: None,
        };

        let bytes = bincode::serialize(&resp).expect("serialize VoteResponse");
        let decoded: VoteResponse<u64> =
            bincode::deserialize(&bytes).expect("deserialize VoteResponse");

        assert_eq!(decoded.vote, resp.vote);
        assert_eq!(decoded.vote_granted, resp.vote_granted);
    }

    // -----------------------------------------------------------------------
    // Newest timestamp helper
    // -----------------------------------------------------------------------

    #[test]
    fn newest_timestamp_from_cells() {
        let key = DecoratedKey::new(PartitionKey::new(b"k".to_vec()));
        let partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![
                Row {
                    clustering: vec![],
                    cells: vec![
                        (0, CellValue::live(b"a".to_vec(), 100)),
                        (1, CellValue::live(b"b".to_vec(), 500)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(200),
                },
                Row {
                    clustering: vec![1],
                    cells: vec![(0, CellValue::live(b"c".to_vec(), 300))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(300),
                },
            ],
        };

        assert_eq!(newest_timestamp(&partition), 500);
    }

    #[test]
    fn newest_timestamp_empty_partition() {
        let key = DecoratedKey::new(PartitionKey::new(b"k".to_vec()));
        let partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        };
        assert_eq!(newest_timestamp(&partition), i64::MIN);
    }
}
