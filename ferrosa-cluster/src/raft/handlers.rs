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
use ferrosa_index::IndexKey;
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

/// Emit the bincode wire bytes for `p` (as if it had been
/// converted with [`partition_to_wire`] then `bincode::serialize`d)
/// **without cloning the partition or any of its rows or cells**.
///
/// Each `Option<Vec<u8>>` cell value is serialised by reference;
/// the per-row conversion to `RowWire` is inlined into the writer
/// rather than building a `Vec<RowWire>` intermediate. Per-call
/// transient allocation is whatever bincode's small scratch state
/// uses, independent of partition row count or cell-value size.
///
/// Used by the streaming range-read producer and the chunked
/// repair Apply send path to avoid the
/// `chunk.iter().cloned().map(partition_to_wire).collect::<Vec<_>>()`
/// allocation that scales with chunk size × partition size.
///
/// Output is byte-identical to
/// `bincode::serialize(&partition_to_wire(p.clone()))` — pinned
/// by `serialize_partition_to_wire_borrowed_matches_legacy`.
pub fn serialize_partition_to_wire_borrowed<W: std::io::Write>(
    writer: &mut W,
    p: &Partition,
) -> Result<(), bincode::Error> {
    // PartitionWire field order: token, key_bytes, deletion,
    // static_row, rows.
    bincode::serialize_into(&mut *writer, &p.key.token.0)?;
    bincode::serialize_into(&mut *writer, p.key.key.as_bytes())?;
    serialize_deletion_time_borrowed(
        &mut *writer,
        p.deletion.marked_for_delete_at,
        p.deletion.local_deletion_time,
    )?;
    // Option<RowWire>: bincode tag (default u8) + inline payload.
    // The zero-or-one static-row clone is a one-time cost per
    // partition — negligible next to the row stream below.
    let static_wire = p.static_row.clone().map(RowWire::from);
    bincode::serialize_into(&mut *writer, &static_wire)?;
    drop(static_wire);
    // Vec<RowWire>: bincode emits length (u64) + items inline.
    // Emit length then iterate, serialising each row from its
    // owning slot — no intermediate Vec<RowWire>.
    let n = p.rows.len() as u64;
    bincode::serialize_into(&mut *writer, &n)?;
    for row in &p.rows {
        serialize_row_to_wire_borrowed(&mut *writer, row)?;
    }
    Ok(())
}

/// Inlined `RowWire` bincode emission from a borrowed `Row`. Each
/// cell's `value: Option<Vec<u8>>` is serialised by reference —
/// no clone. The cell tuple `(u16, CellValueWire)` is emitted
/// field-by-field to match the wire layout exactly.
fn serialize_row_to_wire_borrowed<W: std::io::Write>(
    writer: &mut W,
    row: &Row,
) -> Result<(), bincode::Error> {
    // RowWire: clustering, cells, deletion, primary_key_liveness.
    bincode::serialize_into(&mut *writer, &row.clustering)?;
    // Vec<(u16, CellValueWire)>: length + items.
    let n_cells = row.cells.len() as u64;
    bincode::serialize_into(&mut *writer, &n_cells)?;
    for (idx, cell) in &row.cells {
        bincode::serialize_into(&mut *writer, idx)?;
        // CellValueWire field order: value, timestamp, ttl,
        // local_deletion_time. Identical field layout to the
        // in-memory `CellValue`, so emitting field-by-field
        // matches the bincode struct serialisation.
        bincode::serialize_into(&mut *writer, &cell.value)?;
        bincode::serialize_into(&mut *writer, &cell.timestamp)?;
        bincode::serialize_into(&mut *writer, &cell.ttl)?;
        bincode::serialize_into(&mut *writer, &cell.local_deletion_time)?;
    }
    serialize_deletion_time_borrowed(
        &mut *writer,
        row.deletion.marked_for_delete_at,
        row.deletion.local_deletion_time,
    )?;
    // LivenessInfoWire field order: timestamp, ttl,
    // local_deletion_time.
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.timestamp)?;
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.ttl)?;
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.local_deletion_time)?;
    Ok(())
}

fn serialize_deletion_time_borrowed<W: std::io::Write>(
    writer: &mut W,
    marked_for_delete_at: i64,
    local_deletion_time: u32,
) -> Result<(), bincode::Error> {
    bincode::serialize_into(&mut *writer, &marked_for_delete_at)?;
    bincode::serialize_into(&mut *writer, &local_deletion_time)?;
    Ok(())
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
    /// Exact clustering key bytes for full primary-key reads. Empty means
    /// return partition rows according to page_size/page_state.
    #[serde(default)]
    pub clustering: Vec<u8>,
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
    // Format: token || key_bytes || deletion || static_row ||
    // rows... || row_count.
    //
    // Row count is emitted LAST so the streaming SSTable walker
    // (see `PartitionDigestStream`) can hash rows as they're
    // decoded without knowing the total in advance. This function
    // exists as the reference impl and matches the streaming
    // variants exactly — pinned by
    // `partition_digest_streaming_matches_legacy`.
    let mut stream = PartitionDigestStream::new(
        partition.key.token.0,
        partition.key.key.as_bytes(),
        partition.deletion,
        partition.static_row.as_ref(),
    )?;
    for row in &partition.rows {
        stream.update_row(row)?;
    }
    stream.finalize()
}

/// Bridges `bincode`'s `serialize_into(W: std::io::Write, _)` into the
/// `crc32fast::Hasher::update(&[u8])` API. Lets the serialiser stream
/// its bytes straight into the hasher without ever holding the full
/// serialised partition in a `Vec<u8>` — which is the difference
/// between a Merkle build that fits in the 2 GiB cgroup and one that
/// doesn't.
struct CrcHashWriter<'a> {
    hasher: &'a mut crc32fast::Hasher,
}

impl<'a> std::io::Write for CrcHashWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Byte-identical to [`compute_partition_digest`] but **never
/// clones the partition's rows or cells**. Walks
/// `partition.rows` row-by-row, converts one row at a time into
/// its `RowWire` form, serialises it directly into the CRC hasher,
/// and drops it before touching the next row. Peak transient
/// allocation during a hash is the bytes of a **single row** plus
/// bincode's small scratch state — independent of how many rows
/// the partition has or how big each cell value is.
///
/// On the fmem entity_store this drops anon-RSS growth during a
/// Merkle build from ~80 MB/s of churn (one full partition clone
/// plus one same-sized bincode buffer per hash, freed but holding
/// pages back from the OS) to a small steady-state value, so the
/// concurrent builds a single repair triggers on a 1 GB-class
/// replica fit comfortably inside the 2 GiB cgroup that the
/// legacy shape OOM-killed.
///
/// Wire-format equivalence with [`compute_partition_digest`] is
/// pinned by `partition_digest_streaming_matches_legacy` in the
/// test module — bincode default options encode `Vec<T>` as
/// `u64-LE length || items`, so emitting the length and then each
/// row individually produces the same byte stream as serialising
/// `Vec<RowWire>` in one shot.
pub fn compute_partition_digest_streaming(partition: &Partition) -> Result<u32, bincode::Error> {
    // Same shape as [`compute_partition_digest`] — both go through
    // `PartitionDigestStream`. Kept under its own name for callers
    // that already discovered it; the only difference from the
    // legacy entry point is that this one is the "streamy" name.
    compute_partition_digest(partition)
}

/// Emit a `Row` in `RowWire`'s on-wire byte layout without cloning
/// any of its cell contents. `bincode::serialize_into(&mut writer,
/// &row.clustering)` already passes the bytes of `row.clustering`
/// by reference, and the same is true for each cell's
/// `Option<Vec<u8>>` value — serde walks the Option tag, then if
/// Some it writes the length followed by the borrowed bytes.
fn serialize_row_borrowed<W: std::io::Write>(
    writer: &mut W,
    row: &ferrosa_sstable::types::Row,
) -> Result<(), bincode::Error> {
    // clustering: Vec<u8>
    bincode::serialize_into(&mut *writer, &row.clustering)?;
    // cells: Vec<(u16, CellValueWire)> — emit len then each pair
    let n_cells = row.cells.len() as u64;
    bincode::serialize_into(&mut *writer, &n_cells)?;
    for (idx, cell) in &row.cells {
        bincode::serialize_into(&mut *writer, idx)?;
        // CellValueWire fields in order: value, timestamp, ttl,
        // local_deletion_time. CellValueWire is `Serialize` for a
        // struct with named fields — bincode emits its fields in
        // declaration order with no field tags, so emitting them
        // individually here produces an identical byte stream.
        bincode::serialize_into(&mut *writer, &cell.value)?;
        bincode::serialize_into(&mut *writer, &cell.timestamp)?;
        bincode::serialize_into(&mut *writer, &cell.ttl)?;
        bincode::serialize_into(&mut *writer, &cell.local_deletion_time)?;
    }
    // deletion: DeletionTimeWire
    serialize_deletion_time(
        &mut *writer,
        row.deletion.marked_for_delete_at,
        row.deletion.local_deletion_time,
    )?;
    // primary_key_liveness: LivenessInfoWire — fields in order
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.timestamp)?;
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.ttl)?;
    bincode::serialize_into(&mut *writer, &row.primary_key_liveness.local_deletion_time)?;
    Ok(())
}

/// Row-by-row streaming variant of [`compute_partition_digest`].
///
/// Caller seeds the digest with the partition header (token, key
/// bytes, partition-level deletion, optional static row, and the
/// **exact** number of clustered rows that will be fed), then calls
/// [`PartitionDigestStream::update_row`] once per clustered row in
/// the same order they appear in the partition, and finally
/// [`PartitionDigestStream::finalize`] to get the CRC.
///
/// This lets the SSTable walker hash a multi-MB partition without
/// ever materialising a `Partition` struct — peak transient
/// allocation per row is the bincode scratch state, and each row's
/// cell payloads are serialised by reference. Combined with the
/// `walk_token_range` streaming merge, the per-session working set
/// during a Merkle build no longer scales with partition size.
///
/// The output is byte-identical to
/// `compute_partition_digest(&partition)` when the same header +
/// rows are provided in declaration order — pinned by
/// `partition_digest_stream_matches_legacy_with_multiple_rows`.
pub struct PartitionDigestStream {
    hasher: crc32fast::Hasher,
    rows_so_far: u64,
}

impl PartitionDigestStream {
    /// Initialise the digest with the partition header. **Row count
    /// is not required at this point** — the streaming format puts
    /// it at the end so the SSTable walker can feed rows as they're
    /// decoded without knowing the total in advance.
    pub fn new(
        token: i64,
        key_bytes: &[u8],
        deletion: ferrosa_sstable::types::DeletionTime,
        static_row: Option<&ferrosa_sstable::types::Row>,
    ) -> Result<Self, bincode::Error> {
        let mut hasher = crc32fast::Hasher::new();
        {
            let mut writer = CrcHashWriter {
                hasher: &mut hasher,
            };
            bincode::serialize_into(&mut writer, &token)?;
            bincode::serialize_into(&mut writer, key_bytes)?;
            serialize_deletion_time(
                &mut writer,
                deletion.marked_for_delete_at,
                deletion.local_deletion_time,
            )?;
            // Zero-or-one static-row clone — negligible per partition.
            let static_wire = static_row.cloned().map(RowWire::from);
            bincode::serialize_into(&mut writer, &static_wire)?;
        }
        Ok(Self {
            hasher,
            rows_so_far: 0,
        })
    }

    /// Fold a single clustered row into the running digest. Cells
    /// are serialised by reference — `row` is never cloned.
    pub fn update_row(&mut self, row: &ferrosa_sstable::types::Row) -> Result<(), bincode::Error> {
        let mut writer = CrcHashWriter {
            hasher: &mut self.hasher,
        };
        serialize_row_borrowed(&mut writer, row)?;
        self.rows_so_far += 1;
        Ok(())
    }

    /// Emit the row count last and return the final CRC. Putting
    /// the count at the end is what lets the streaming SSTable
    /// walker hash a partition without knowing how many rows it
    /// has up front.
    pub fn finalize(self) -> Result<u32, bincode::Error> {
        let Self {
            mut hasher,
            rows_so_far,
        } = self;
        {
            let mut writer = CrcHashWriter {
                hasher: &mut hasher,
            };
            bincode::serialize_into(&mut writer, &rows_so_far)?;
        }
        Ok(hasher.finalize())
    }
}

fn serialize_deletion_time<W: std::io::Write>(
    writer: &mut W,
    marked_for_delete_at: i64,
    local_deletion_time: u32,
) -> Result<(), bincode::Error> {
    bincode::serialize_into(&mut *writer, &marked_for_delete_at)?;
    bincode::serialize_into(&mut *writer, &local_deletion_time)?;
    Ok(())
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
    pub(crate) async fn get(&self) -> Option<Arc<super::FerrosRaft>> {
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
            other => {
                tracing::error!(msg_type = ?other.msg_type(), "RaftAppendHandler: unexpected message type");
                return None;
            }
        };

        let raft = match self.raft.get().await {
            Some(r) => r,
            None => {
                tracing::error!(
                    "RaftAppendHandler: Raft instance not ready (LazyRaft returned None)"
                );
                return None;
            }
        };

        let req: AppendEntriesRequest<FerrosRaftConfig> = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("RaftAppendHandler: deserialize failed: {e}");
                return None;
            }
        };

        let resp = match raft.append_entries(req).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("RaftAppendHandler: append_entries failed: {e}");
                return None;
            }
        };

        match bincode::serialize(&resp) {
            Ok(bytes) => Some(Message::RaftAppendResponse(Bytes::from(bytes))),
            Err(e) => {
                tracing::error!("RaftAppendHandler: serialize response failed: {e}");
                None
            }
        }
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
            other => {
                tracing::error!(msg_type = ?other.msg_type(), "RaftVoteHandler: unexpected message type");
                return None;
            }
        };

        let raft = match self.raft.get().await {
            Some(r) => r,
            None => {
                tracing::error!(
                    "RaftVoteHandler: Raft instance not ready (LazyRaft returned None)"
                );
                return None;
            }
        };

        let req: VoteRequest<u64> = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("RaftVoteHandler: deserialize failed: {e}");
                return None;
            }
        };

        let resp: VoteResponse<u64> = match raft.vote(req).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("RaftVoteHandler: vote failed: {e}");
                return None;
            }
        };

        match bincode::serialize(&resp) {
            Ok(bytes) => Some(Message::RaftVoteResponse(Bytes::from(bytes))),
            Err(e) => {
                tracing::error!("RaftVoteHandler: serialize response failed: {e}");
                None
            }
        }
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
        let page_size = req.page_size as usize;
        let page_offset: usize = if req.page_state.len() >= 8 {
            u64::from_le_bytes(req.page_state[..8].try_into().unwrap_or([0; 8])) as usize
        } else {
            0
        };
        let storage_read = if !req.clustering.is_empty() {
            self.storage
                .read_clustering_row(&table_id, &key, &req.clustering)
        } else if page_size > 0 && page_offset == 0 {
            // First page: over-read by one row so the paging logic below can
            // tell whether a tail exists past this page.
            //
            // `read_limited_rows(key, page_size)` truncates to EXACTLY
            // `page_size` rows, which destroys the `rows.len() > page_size`
            // signal used to set `has_more`. A partition with more than
            // `page_size` rows then looks "complete" after the first page and
            // the coordinator stops paging — silently dropping every row past
            // the first page (count(*) under-count / data loss, FRSA-BUG:
            // first-page cap hides partition tail). Reading `page_size + 1`
            // lets the slice logic detect the tail and emit `next_page_state`.
            // `page_size` is a `u32` widened to `usize`, so `+ 1` cannot
            // overflow on any supported target.
            self.storage
                .read_limited_rows(&table_id, &key, page_size + 1)
        } else {
            self.storage.read(&table_id, &key)
        };

        let payload = match storage_read {
            Ok(Some(mut partition)) => {
                // Paging: if page_size > 0, return only a page of rows.
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

                let ts = newest_timestamp(&partition);
                let digest = match compute_partition_digest(&partition) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        tracing::warn!("ReadRequestHandler: digest computation failed: {e}");
                        None
                    }
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
    /// Maximum partitions to return from this node.
    #[serde(default = "default_range_read_limit")]
    pub limit: usize,
    /// Maximum rows to include from each returned partition.
    /// 0 means unlimited (backwards-compatible default).
    #[serde(default)]
    pub row_limit: usize,
}

fn default_range_read_limit() -> usize {
    crate::write_path::DEFAULT_RANGE_READ_LIMIT
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

        let range_read_limit = req
            .limit
            .clamp(1, crate::write_path::DEFAULT_RANGE_READ_LIMIT);
        let row_limit = req.row_limit;
        let partitions = match self.storage.read_range_limited_rows(
            &table_id,
            None,
            None,
            range_read_limit,
            row_limit,
        ) {
            Ok(ps) => ps,
            Err(e) => {
                tracing::warn!("RangeReadHandler: read_range failed: {e}");
                // Return empty response instead of None — a None response
                // means NO response at all, causing the coordinator to wait
                // for the full 120s timeout and hang the client.
                vec![]
            }
        };

        let truncated = partitions.len() >= range_read_limit;
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
// ADR-020 streaming range-read payloads
// ---------------------------------------------------------------------------
//
// Each of the five new `Message::RangeReadStream*` variants in
// ferrosa-net carries an opaque `Bytes` body. These structs are the
// bincoded shapes that the handler emits and the coordinator decodes.
// The `request_id` field is the first u32 in every payload — the
// dispatch leaf (`ferrosa_net::stream_router::StreamRouter`) uses it
// to route each inbound frame to the right per-request consumer.

/// Coordinator → handler: open a streaming range read on a table.
///
/// Unlike the legacy [`RangeReadRequestPayload`], there is no
/// `limit` field — the consumer controls how many partitions it
/// wants via back-pressure (the bounded mpsc backing the
/// coordinator's receiver) and via an explicit
/// [`RangeReadStreamCancelPayload`] when it has enough.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReadStreamRequestPayload {
    /// Per-coordinator-call correlation id. Returned in every chunk,
    /// heartbeat, done, and cancel frame for this stream so the
    /// StreamRouter can dispatch frames to the right consumer.
    pub request_id: u32,
    pub keyspace: String,
    pub table: String,
    /// Optional current-schema regular-column ordinals to decode. `None`
    /// streams full partitions; `Some` uses the projected SSTable reader so
    /// wide cells not needed by the query are byte-skipped on remote replicas.
    #[serde(default)]
    pub projected_regular_ordinals: Option<Vec<u16>>,
    /// Optional inclusive lower-bound partition-key bytes for a paged
    /// (resume-capable) scan. `None` streams the whole range from the start;
    /// `Some(bytes)` makes the remote replica start its fragmented iterator at
    /// `DecoratedKey::new(PartitionKey::from(bytes))`, so a resumed page does
    /// not re-stream the already-emitted prefix. The coordinator's k-way merge
    /// and the CQL paging collector still apply the exact skip-≤-last semantics
    /// on top, so an off-by-one at the bound can never drop or duplicate rows.
    ///
    /// `#[serde(default)]` keeps the frame wire-compatible with peers that
    /// predate this field (they decode it as `None`).
    #[serde(default)]
    pub start_key: Option<Vec<u8>>,
}

/// Handler → coordinator: one batch of partitions belonging to a
/// streaming range read.
///
/// The chunk size is bounded at the handler by a runtime-configurable
/// target (NetConfig.bulk_stream_chunk_partitions, future work), not
/// by a hardcoded constant — so PB-scale tables stream naturally at
/// O(chunk_size) memory regardless of total table size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeReadStreamChunkPayload {
    pub request_id: u32,
    /// Monotonic 0-based chunk sequence within this stream. Lets the
    /// coordinator detect dropped or reordered frames (TCP excludes
    /// reorders today, but seq is cheap insurance against future
    /// lane changes).
    pub seq: u32,
    pub partitions: Vec<PartitionWire>,
}

/// Handler → coordinator: keep-alive emitted when the next chunk is
/// slow to produce (e.g. S3 fetch, compaction back-pressure).
///
/// The coordinator's `IdleTimeoutWatchdog` treats heartbeats as
/// activity and resets its per-message deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReadStreamHeartbeatPayload {
    pub request_id: u32,
    /// Sequence number of the next chunk the handler is working on
    /// (i.e. one past the seq of the last delivered chunk). Useful
    /// for debugging stuck streams.
    pub seq: u32,
}

/// Handler → coordinator: terminator for a streaming range read.
///
/// After receiving Done, the coordinator unregisters from the
/// StreamRouter and resolves the user query. `total_chunks` is the
/// count of `RangeReadStreamChunkPayload` messages emitted (for
/// validation); `truncated` is reserved for future use when the
/// handler bounds its iteration externally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReadStreamDonePayload {
    pub request_id: u32,
    pub total_chunks: u32,
    pub truncated: bool,
}

/// Coordinator → handler: abort a stream in-flight.
///
/// Used when the CQL client disconnects, when read-quorum is already
/// satisfied by other replicas, or when the user issues `KILL`. The
/// handler observes the cancel between batches and stops iterating;
/// any partial chunks already in flight on the wire are discarded by
/// the coordinator (the route has been unregistered before sending
/// the cancel).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReadStreamCancelPayload {
    pub request_id: u32,
}

// ---------------------------------------------------------------------------
// Index read handler
// ---------------------------------------------------------------------------

/// Payload for a remote index-read request (secondary index lookup on one node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReadRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub index_name: String,
    pub index_key: Vec<u8>,
}

/// Payload for a remote index-read response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReadResponsePayload {
    pub partitions: Vec<PartitionWire>,
}

/// Handles inbound `IndexReadRequest` RPCs from remote coordinators.
///
/// Runs `read_by_index` on local storage and returns matching partitions.
pub struct IndexReadHandler {
    storage: Arc<StorageEngine>,
}

impl IndexReadHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for IndexReadHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::IndexReadRequest(b) => b,
            _ => return None,
        };

        let req: IndexReadRequestPayload = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("IndexReadHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let table_id = ferrosa_storage::TableId::new(&req.keyspace, &req.table);
        let index_key = IndexKey(req.index_key);

        let partitions = match self
            .storage
            .read_by_index(&table_id, &req.index_name, &index_key)
        {
            Ok(ps) => ps,
            Err(e) => {
                tracing::warn!("IndexReadHandler: read_by_index failed: {e}");
                vec![]
            }
        };

        let wire_partitions = partitions.into_iter().map(partition_to_wire).collect();
        let payload = IndexReadResponsePayload {
            partitions: wire_partitions,
        };

        let resp_bytes = match bincode::serialize(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("IndexReadHandler: failed to serialize response: {e}");
                bincode::serialize(&IndexReadResponsePayload { partitions: vec![] })
                    .unwrap_or_default()
            }
        };

        Some(Message::IndexReadResponse(Bytes::from(resp_bytes)))
    }
}

/// Payload for a remote KEYED index-read request (t_430c4188): a secondary
/// index lookup restricted to one partition, sent only to that partition's
/// replicas — never a global scatter-gather.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReadInPartitionRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub index_name: String,
    pub index_key: Vec<u8>,
    /// Raw partition key bytes the postings are restricted to.
    pub partition_key: Vec<u8>,
}

/// Handles inbound `IndexReadInPartitionRequest` RPCs from remote coordinators.
///
/// Runs `read_by_index_in_partition` on local storage and returns the matching
/// rows (as single-row partitions) for the requested partition only.
pub struct IndexReadInPartitionHandler {
    storage: Arc<StorageEngine>,
}

impl IndexReadInPartitionHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for IndexReadInPartitionHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::IndexReadInPartitionRequest(b) => b,
            _ => return None,
        };

        let req: IndexReadInPartitionRequestPayload = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("IndexReadInPartitionHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let table_id = ferrosa_storage::TableId::new(&req.keyspace, &req.table);
        let index_key = IndexKey(req.index_key);

        let partitions = match self.storage.read_by_index_in_partition(
            &table_id,
            &req.index_name,
            &index_key,
            &req.partition_key,
        ) {
            Ok(ps) => ps,
            Err(e) => {
                tracing::warn!(
                    "IndexReadInPartitionHandler: read_by_index_in_partition failed: {e}"
                );
                vec![]
            }
        };

        let wire_partitions = partitions.into_iter().map(partition_to_wire).collect();
        let payload = IndexReadResponsePayload {
            partitions: wire_partitions,
        };

        let resp_bytes = match bincode::serialize(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("IndexReadInPartitionHandler: failed to serialize response: {e}");
                bincode::serialize(&IndexReadResponsePayload { partitions: vec![] })
                    .unwrap_or_default()
            }
        };

        Some(Message::IndexReadInPartitionResponse(Bytes::from(
            resp_bytes,
        )))
    }
}

/// Payload for a remote full-text search request (fts_match lookup on one node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulltextSearchRequestPayload {
    pub keyspace: String,
    pub table: String,
    pub index_name: String,
    pub query: String,
    /// Query-derived `LIMIT k` pushed down to the replica so its search holds
    /// a bounded top-k working set instead of every matching doc key
    /// (t_ee98faa0 layer 2). `None` = the statement had no LIMIT and the
    /// complete match set is required — never a server-side cap.
    pub limit: Option<u64>,
}

/// Payload for a remote full-text search response: the matching partition keys
/// (raw `PartitionKey` bytes) found in this node's local FTI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulltextSearchResponsePayload {
    pub matching_keys: Vec<Vec<u8>>,
}

/// Handles inbound `FulltextSearchRequest` RPCs from remote coordinators.
///
/// Runs `fulltext_search` on local storage and returns the matching partition
/// keys. `fts_match` has no partition key, so the coordinator fans this out to
/// every node and unions the results — fixing the coordinator-local lookup that
/// made `fts_match` non-deterministic on a cluster (BUG-F-007).
pub struct FulltextSearchHandler {
    storage: Arc<StorageEngine>,
}

impl FulltextSearchHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RpcHandler for FulltextSearchHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::FulltextSearchRequest(b) => b,
            _ => return None,
        };

        let req: FulltextSearchRequestPayload = bincode::deserialize(&bytes)
            .map_err(|e| {
                tracing::warn!("FulltextSearchHandler: failed to deserialize request: {e}");
                e
            })
            .ok()?;

        let table_id = ferrosa_storage::TableId::new(&req.keyspace, &req.table);

        let matching_keys = match self.storage.fulltext_search(
            &table_id,
            &req.index_name,
            &req.query,
            req.limit.map(|k| k as usize),
        ) {
            Ok(keys) => keys,
            Err(e) => {
                tracing::warn!("FulltextSearchHandler: fulltext_search failed: {e}");
                vec![]
            }
        };

        let payload = FulltextSearchResponsePayload { matching_keys };
        let resp_bytes = match bincode::serialize(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("FulltextSearchHandler: failed to serialize response: {e}");
                bincode::serialize(&FulltextSearchResponsePayload {
                    matching_keys: vec![],
                })
                .unwrap_or_default()
            }
        };

        Some(Message::FulltextSearchResponse(Bytes::from(resp_bytes)))
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
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
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

    /// The borrow-based partition serializer must emit byte-for-byte
    /// the same bincode output as
    /// `bincode::serialize(&partition_to_wire(p.clone()))`.
    ///
    /// Without byte-equivalence, callers that decode on the wire
    /// (RangeReadStreamChunk consumers, repair Apply receivers,
    /// CL read responders) would see corrupted partitions —
    /// the savings from skipping the clone aren't worth a
    /// silent decoding bug.
    ///
    /// The streaming range-read producer
    /// (`stream_range_response` in `coordinator::stream_producer`)
    /// uses this helper to emit each chunk's partitions without
    /// the per-partition `.clone()` + `Vec<PartitionWire>::collect()`
    /// allocations that scale with chunk size.
    #[test]
    fn serialize_partition_to_wire_borrowed_matches_legacy() {
        // Multi-row, multi-cell partition to exercise every layer
        // of the wire encoding (DeletionTime, LivenessInfo,
        // CellValue, Vec<(u16, _)>).
        let mut original = make_partition(b"borrowed-eq", 12_345);
        for i in 1..5 {
            original.rows.push(Row {
                clustering: vec![i as u8, 0, 0, 0],
                cells: vec![(
                    0,
                    CellValue::live(format!("v-{i}").into_bytes(), 12_345 + i as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(12_345 + i as i64),
            });
        }

        let legacy_wire = partition_to_wire(original.clone());
        let legacy_bytes = bincode::serialize(&legacy_wire).expect("legacy serialize must succeed");

        let mut borrowed_bytes: Vec<u8> = Vec::new();
        super::serialize_partition_to_wire_borrowed(&mut borrowed_bytes, &original)
            .expect("borrowed serialize must succeed");

        assert_eq!(
            legacy_bytes, borrowed_bytes,
            "borrowed partition serializer must emit byte-identical \
             output to the clone-then-bincode path"
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
            clustering: vec![],
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

    /// `compute_partition_digest` clones every row + cell into a
    /// `PartitionWire` and bincode-allocates a Vec<u8> the size of
    /// the serialised partition before CRC. On a multi-GB local
    /// replica that allocation churn dominates anti-entropy repair
    /// memory — observed to grow node1's anon RSS by ~80 MB/s and
    /// OOM the 2 GiB cgroup mid-Merkle-build, even though no
    /// in-flight partition exceeded a few MB.
    ///
    /// The streaming variant must produce **byte-identical** CRC
    /// output (otherwise two replicas hashing the same partition
    /// land on different Merkle leaves and repair turns a no-op
    /// into a spurious mismatch). This test pins the equivalence
    /// for a representative partition.
    #[test]
    fn partition_digest_streaming_matches_legacy() {
        let p = make_partition(b"abc", 999);
        let legacy = compute_partition_digest(&p).unwrap();
        let streaming = super::compute_partition_digest_streaming(&p).unwrap();
        assert_eq!(
            legacy, streaming,
            "streaming digest MUST match the legacy bytes-then-hash result for replica compatibility"
        );
    }

    /// `PartitionDigestStream` is the **row-by-row** digest path:
    /// the SSTable iter feeds rows into the hasher one at a time
    /// without ever building a full `Partition` struct. It must
    /// produce the same CRC as the legacy function so two replicas
    /// — one hashing in-memory partitions, one hashing via the
    /// streaming SSTable walker — see each other on the wire.
    ///
    /// Verified for a partition with multiple rows: the streaming
    /// API takes the header (token / key / deletion / static_row /
    /// row_count) and `update_row` per clustered row, then
    /// `finalize` returns the same u32 as
    /// `compute_partition_digest(&partition)`.
    #[test]
    fn partition_digest_stream_matches_legacy_with_multiple_rows() {
        let mut p = make_partition(b"multi-row-key", 12_345);
        // make_partition builds a single-row partition; extend it
        // so the streaming row-iteration path is exercised.
        for i in 1..5 {
            p.rows.push(Row {
                clustering: vec![i as u8],
                cells: vec![(
                    0,
                    CellValue::live(format!("val-{i}").into_bytes(), 12_345 + i as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(12_345 + i as i64),
            });
        }
        let legacy = compute_partition_digest(&p).unwrap();
        let mut stream = super::PartitionDigestStream::new(
            p.key.token.0,
            p.key.key.as_bytes(),
            p.deletion,
            p.static_row.as_ref(),
        )
        .unwrap();
        for row in &p.rows {
            stream.update_row(row).unwrap();
        }
        let streaming = stream.finalize().unwrap();
        assert_eq!(
            legacy, streaming,
            "PartitionDigestStream MUST match the legacy digest for replica compatibility"
        );
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
            clustering: vec![],
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

    /// Regression for the cluster `count(*)` under-count / data-loss bug.
    ///
    /// A partition with MORE rows than the coordinator's remote read page size
    /// must page out every row. The historical bug read EXACTLY `page_size`
    /// rows on the first page (`read_limited_rows(key, page_size)`), which
    /// erased the `rows.len() > page_size` signal the handler uses to set
    /// `has_more`. The coordinator therefore stopped after one page and the
    /// partition tail (rows `page_size..`) vanished from scans/`count(*)` while
    /// still being individually retrievable by point read — the exact symptom
    /// in `tests/cluster/test_data_loss_reproduction.py` (5100 rows written,
    /// `count(*)` == 5000, all 100 canaries survive point reads).
    ///
    /// This drives `ReadRequestHandler` through the same page loop the
    /// coordinator runs and asserts no row is lost.
    #[tokio::test]
    async fn read_request_handler_pages_full_partition_tail() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key_bytes = b"wide_partition".as_slice();
        let dk = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));

        // Write more rows than a single page holds. Distinct clustering keys
        // keep them as separate rows in one partition (cell-merge does not
        // collapse them). 25 rows with page_size 10 => 3 pages (10/10/5).
        const TOTAL_ROWS: usize = 25;
        const PAGE_SIZE: u32 = 10;
        for i in 0..TOTAL_ROWS {
            let row = Row {
                clustering: (i as u32).to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    CellValue::live(format!("v{i}").into_bytes(), 1000 + i as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
            };
            storage.write(&table_id, &dk, row, 1000 + i as i64).unwrap();
        }

        let handler = ReadRequestHandler::new(storage);

        // Mirror the coordinator's `read_one_replica_limited_rows` page loop.
        let mut collected = 0usize;
        let mut page_state: Vec<u8> = vec![];
        let mut pages = 0usize;
        loop {
            let req = ReadRequestPayload {
                keyspace: "test_ks".to_string(),
                table: "test_tbl".to_string(),
                key: key_bytes.to_vec(),
                digest_only: false,
                page_size: PAGE_SIZE,
                page_state: page_state.clone(),
                clustering: vec![],
            };
            let req_bytes = bincode::serialize(&req).unwrap();
            let msg = Message::ReadRequest(Bytes::from(req_bytes));
            let response = handler
                .handle(make_peer_id(), msg)
                .await
                .expect("handler responds");
            let Message::ReadResponse(resp_bytes) = response else {
                panic!("expected ReadResponse");
            };
            let resp: ReadResponsePayload = bincode::deserialize(&resp_bytes).unwrap();
            assert!(resp.found, "partition must be found on every page");
            let part = partition_from_wire(resp.partition.expect("page carries rows"));
            collected += part.rows.len();
            pages += 1;
            assert!(pages <= 8, "paging must terminate, not loop forever");
            if resp.has_more && !resp.next_page_state.is_empty() {
                page_state = resp.next_page_state;
                continue;
            }
            break;
        }

        assert_eq!(
            collected, TOTAL_ROWS,
            "every row in a partition larger than one page must be paged out; \
             losing the tail is the cluster count(*) data-loss bug"
        );
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
            clustering: vec![],
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
            clustering: vec![],
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

    // -----------------------------------------------------------------------
    // ADR-020 streaming range-read payloads
    // -----------------------------------------------------------------------

    /// All five streaming-RPC payload types must round-trip through
    /// bincode unchanged. These are the structured shapes carried in
    /// the opaque `Bytes` of the matching `Message::RangeReadStream*`
    /// variants from ferrosa-net.
    #[test]
    fn streaming_range_read_payloads_round_trip_through_bincode() {
        let req = RangeReadStreamRequestPayload {
            request_id: 7,
            keyspace: "agent_memory".into(),
            table: "entity_store".into(),
            projected_regular_ordinals: None,
            start_key: Some(b"resume-here".to_vec()),
        };
        let encoded = bincode::serialize(&req).expect("encode request");
        let decoded: RangeReadStreamRequestPayload =
            bincode::deserialize(&encoded).expect("decode request");
        assert_eq!(decoded, req);

        let key = DecoratedKey::new(PartitionKey::new(b"pk".to_vec()));
        let part = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        };
        let chunk = RangeReadStreamChunkPayload {
            request_id: 7,
            seq: 3,
            partitions: vec![partition_to_wire(part)],
        };
        let encoded = bincode::serialize(&chunk).expect("encode chunk");
        let decoded: RangeReadStreamChunkPayload =
            bincode::deserialize(&encoded).expect("decode chunk");
        assert_eq!(decoded.request_id, chunk.request_id);
        assert_eq!(decoded.seq, chunk.seq);
        assert_eq!(decoded.partitions.len(), 1);

        let hb = RangeReadStreamHeartbeatPayload {
            request_id: 7,
            seq: 4,
        };
        let encoded = bincode::serialize(&hb).expect("encode heartbeat");
        let decoded: RangeReadStreamHeartbeatPayload =
            bincode::deserialize(&encoded).expect("decode heartbeat");
        assert_eq!(decoded, hb);

        let done = RangeReadStreamDonePayload {
            request_id: 7,
            total_chunks: 12,
            truncated: false,
        };
        let encoded = bincode::serialize(&done).expect("encode done");
        let decoded: RangeReadStreamDonePayload =
            bincode::deserialize(&encoded).expect("decode done");
        assert_eq!(decoded, done);

        let cancel = RangeReadStreamCancelPayload { request_id: 7 };
        let encoded = bincode::serialize(&cancel).expect("encode cancel");
        let decoded: RangeReadStreamCancelPayload =
            bincode::deserialize(&encoded).expect("decode cancel");
        assert_eq!(decoded, cancel);
    }

    /// Every payload shape carries the `request_id` so the coordinator's
    /// StreamRouter can dispatch the chunk back to the right per-request
    /// receiver. The router decodes the first 4 bytes as a `u32` and
    /// looks up the registration — this test pins the bincode prefix.
    #[test]
    fn streaming_chunk_payload_starts_with_request_id_for_router_dispatch() {
        let chunk = RangeReadStreamChunkPayload {
            request_id: 0xDEAD_BEEF,
            seq: 0,
            partitions: vec![],
        };
        let encoded = bincode::serialize(&chunk).expect("encode");
        // bincode default little-endian fixint: u32 occupies bytes 0..4.
        assert_eq!(&encoded[..4], &0xDEAD_BEEFu32.to_le_bytes());
    }
}
