//! ADR-020 streaming variant of `coordinate_range_read_limited_rows`.
//!
//! Replaces the legacy single-shot `RangeReadRequest` per-replica
//! RPC with a multi-message streaming RPC keyed by `request_id`.
//! Local reads stay direct (no internode hop); each remote replica
//! receives a `RangeReadStreamRequest` and streams chunks back via
//! `Lane::Bulk` until a `RangeReadStreamDone` terminator. The
//! coordinator's `StreamRouter` dispatches the inbound chunks to
//! the per-request `mpsc::Receiver`; `consume_range_stream`
//! assembles them under an `IdleTimeoutWatchdog`.
//!
//! Wall-clock is unbounded — PB-scale scans take as long as they
//! take. Only genuine stalls (peer crashed mid-stream, network
//! partition with no further chunks or heartbeats) abort the
//! consume.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use ferrosa_net::codec::Lane;
use ferrosa_net::idle_timeout::IdleTimeoutWatchdog;
use ferrosa_net::message::Message;
use ferrosa_net::task_pool::TaskPool;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use super::stream_consumer::{consume_range_stream, StreamConsumeError};
use super::ClusterCoordinator;
use crate::error::ClusterError;
use crate::raft::handlers::{
    partition_from_wire, RangeReadStreamChunkPayload, RangeReadStreamDonePayload,
    RangeReadStreamHeartbeatPayload, RangeReadStreamRequestPayload,
};

/// Idle deadline on the streaming receiver. Reset on every chunk OR
/// heartbeat. A producer that stops sending entirely for longer
/// than this aborts the consume. Tunable later via NetConfig.
///
/// The Phase 1 handler emits a heartbeat every 3 s while a slow
/// storage read blocks (see stream_request_handler::HEARTBEAT_INTERVAL).
/// 30 s leaves room for runtime starvation under heavy concurrent
/// compaction load — if the handler can't even get scheduled for
/// 30 s the peer is genuinely stuck and aborting is correct.
const STREAMING_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-request buffer for the StreamRouter receiver. Bounded so a
/// slow consumer back-pressures the inbound dispatch (chunks queue
/// up at the lane until the consumer drains).
const STREAM_RECEIVER_BUFFER: usize = 32;

/// Default per-chunk partition count emitted by the streaming
/// range-read handler. Picked so each chunk message fits comfortably
/// inside the Bulk lane MTU envelope even for wide partitions, while
/// still amortizing the per-message frame overhead. Tunable later
/// via NetConfig.
pub const STREAMING_CHUNK_PARTITIONS: usize = 64;

pub type ClusterPartitionStream =
    Pin<Box<dyn Stream<Item = crate::error::Result<Partition>> + Send>>;

impl ClusterCoordinator {
    /// COUNT(*) fast path. Returns the local replica's row count
    /// for `[start, end]` on `table_id`. Bypasses the streaming
    /// range-read RPC entirely — calls `StorageEngine::count_range`
    /// which uses the metadata-only merger
    /// (`range_merger::merger_for_metadata_sources`) so cell
    /// payloads are byte-skipped at every SSTable.
    ///
    /// Consistency: returns the LOCAL replica's view. For
    /// quorum / all consistency on COUNT(*), shipping partition
    /// keys across replicas would defeat the optimization — that
    /// is a separate design (and matches Cassandra's "COUNT is
    /// eventually consistent by default" semantics).
    pub fn coordinate_range_count(&self, table_id: &TableId) -> crate::error::Result<u64> {
        self.storage
            .count_range(table_id, None, None)
            .map_err(ClusterError::Storage)
    }
}

/// Number of REMOTE replicas to query for a range read at the given
/// consistency level. Local replica always reads directly (counts as
/// one satisfied response), so this returns the *additional* remote
/// count needed.
///
/// For RF == node_count (every node owns every token range, typical
/// in the dev/test cluster), CL=ONE / LOCAL_ONE is satisfied by the
/// local read alone and we return 0 — full fan-out is wasted work
/// since dedup would just collapse identical replica copies anyway.
///
/// For RF < node_count we cannot prove the local node owns every
/// partition without a token-range-aware query plan; conservatively
/// fall back to the existing all-remotes fan-out until that proper
/// path lands. (Filed as next-step in
/// bug-streaming-range-read-perf-50x-floor.md.)
fn remote_count_for_cl(
    cl: crate::consistency::ConsistencyLevel,
    rf: usize,
    node_count: usize,
    remote_count: usize,
) -> usize {
    use crate::consistency::ConsistencyLevel as CL;
    // RF<cluster — fall back to full fan-out for correctness.
    if rf < node_count {
        return remote_count;
    }
    // RF==cluster — local has every partition. Apply CL.
    match cl {
        CL::One | CL::LocalOne => 0,
        // QUORUM/ALL: contact enough remotes to satisfy CL beyond
        // the local response. For RF=N, QUORUM = floor(N/2)+1.
        CL::Quorum | CL::LocalQuorum | CL::EachQuorum => {
            let needed = rf / 2 + 1; // QUORUM count including local
            needed.saturating_sub(1).min(remote_count)
        }
        CL::All => remote_count,
        // Any/Two/Three are unusual or write-only — keep
        // conservative full fan-out so we never under-read.
        _ => remote_count,
    }
}

/// Deduplicate partitions by token — multiple replicas (RF=N) each
/// return a copy of every partition they own; without this, COUNT(*)
/// and full-table scans return N× the real partition count. Mirrors
/// the dedup loop at the end of `coordinate_range_read_limited_rows`
/// in `read.rs` so the streaming and legacy paths return the same
/// shape to the CQL layer.
fn dedup_by_token(partitions: Vec<Partition>) -> Vec<Partition> {
    let mut by_token: BTreeMap<i64, Vec<Partition>> = BTreeMap::new();
    for p in partitions {
        by_token.entry(p.key.token.0).or_default().push(p);
    }
    by_token
        .into_values()
        .map(|group| {
            if group.len() == 1 {
                group.into_iter().next().unwrap()
            } else {
                ferrosa_storage::merge::merge_partitions(group)
            }
        })
        .collect()
}

async fn read_local_range_stream_limited_rows(
    storage: &ferrosa_storage::StorageEngine,
    table_id: &TableId,
    limit: usize,
    row_limit: usize,
) -> ferrosa_common::Result<Vec<Partition>> {
    if row_limit > 0 {
        return storage.read_range_limited_rows(table_id, None, None, limit, row_limit);
    }

    let mut stream = storage.range_iter(table_id, None, None);
    let mut partitions = Vec::with_capacity(limit);

    while partitions.len() < limit {
        let Some(next) = stream.next().await else {
            break;
        };
        let mut partition = next?;
        if row_limit > 0 {
            partition.rows.truncate(row_limit);
        }
        partitions.push(partition);
    }

    Ok(partitions)
}

fn apply_row_limit(mut partition: Partition, row_limit: usize) -> Partition {
    if row_limit > 0 {
        partition.rows.truncate(row_limit);
    }
    partition
}

fn next_remote_error(err: StreamConsumeError) -> crate::error::Result<Partition> {
    Err(ClusterError::Internal(format!(
        "streaming range read: {err:?}"
    )))
}

async fn forward_remote_range_stream(
    receiver: mpsc::Receiver<Message>,
    request_id: u32,
    expected_done: usize,
    tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    if let Err(err) =
        forward_remote_range_stream_inner(receiver, request_id, expected_done, tx.clone()).await
    {
        let _ = tx.send(next_remote_error(err)).await;
    }
}

async fn forward_remote_range_stream_inner(
    receiver: mpsc::Receiver<Message>,
    request_id: u32,
    expected_done: usize,
    tx: mpsc::Sender<crate::error::Result<Partition>>,
) -> Result<(), StreamConsumeError> {
    let mut watchdog = IdleTimeoutWatchdog::new(receiver, STREAMING_IDLE_TIMEOUT);
    let mut delivered_done = 0usize;

    loop {
        if delivered_done >= expected_done {
            return Ok(());
        }

        let next = watchdog
            .next()
            .await
            .map_err(|elapsed| StreamConsumeError::IdleTimeout {
                request_id,
                idle_timeout: elapsed.idle_timeout,
            })?;

        let frame = match next {
            Some(msg) => msg,
            None => {
                return Err(StreamConsumeError::ChannelClosedBeforeDone {
                    delivered_done,
                    expected_done,
                });
            }
        };

        match frame {
            Message::RangeReadStreamChunk(bytes) => {
                let chunk =
                    bincode::deserialize::<RangeReadStreamChunkPayload>(&bytes).map_err(|e| {
                        StreamConsumeError::Decode {
                            request_id,
                            which: "RangeReadStreamChunk",
                            message: e.to_string(),
                        }
                    })?;
                for partition in chunk.partitions.into_iter().map(partition_from_wire) {
                    if tx.send(Ok(partition)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Message::RangeReadStreamHeartbeat(bytes) => {
                let _heartbeat = bincode::deserialize::<RangeReadStreamHeartbeatPayload>(&bytes)
                    .map_err(|e| StreamConsumeError::Decode {
                        request_id,
                        which: "RangeReadStreamHeartbeat",
                        message: e.to_string(),
                    })?;
            }
            Message::RangeReadStreamDone(bytes) => {
                let done =
                    bincode::deserialize::<RangeReadStreamDonePayload>(&bytes).map_err(|e| {
                        StreamConsumeError::Decode {
                            request_id,
                            which: "RangeReadStreamDone",
                            message: e.to_string(),
                        }
                    })?;
                if done.truncated {
                    tracing::warn!(
                        request_id,
                        "streaming range read: remote replica reported truncated stream"
                    );
                }
                delivered_done += 1;
            }
            other => {
                return Err(StreamConsumeError::UnexpectedFrame {
                    msg_type: other.msg_type(),
                });
            }
        }
    }
}

async fn merge_local_and_single_remote_stream(
    storage: std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    row_limit: usize,
    projected_regular_ordinals: Option<Vec<u16>>,
    remote_rx: mpsc::Receiver<crate::error::Result<Partition>>,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    // `row_limit > 0` only for `LIMIT N` queries with partition-key
    // equality predicates (see `safe_partition_key_filter_row_limit`). Those
    // target specific bounded partitions, so the legacy whole-partition
    // token merge (with a correct PER-PARTITION row cap) is used. The full
    // `SELECT *` path is `row_limit == 0` and takes the fragment-aware
    // streaming merge below, which bounds memory on BOTH replica copies.
    if row_limit > 0 {
        merge_local_and_single_remote_whole(
            storage,
            table_id,
            row_limit,
            projected_regular_ordinals,
            remote_rx,
            out_tx,
        )
        .await;
        return;
    }
    merge_local_and_single_remote_fragmented(
        storage,
        table_id,
        projected_regular_ordinals,
        remote_rx,
        out_tx,
    )
    .await;
}

/// Whole-partition token merge for capped (`row_limit > 0`) reads. The
/// per-partition row cap is applied to the merged partition, which is
/// correct because each side delivers ONE `Partition` per token here.
async fn merge_local_and_single_remote_whole(
    storage: std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    row_limit: usize,
    projected_regular_ordinals: Option<Vec<u16>>,
    mut remote_rx: mpsc::Receiver<crate::error::Result<Partition>>,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    let mut local_stream: ClusterPartitionStream = if let Some(wanted) = projected_regular_ordinals
    {
        Box::pin(
            storage
                .range_iter_projected(&table_id, wanted, None, None, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    } else {
        Box::pin(
            storage
                .range_iter(&table_id, None, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    };
    let mut local_next = local_stream.next().await;
    let mut remote_next = remote_rx.recv().await;

    loop {
        match (local_next.take(), remote_next.take()) {
            (None, None) => return,
            (Some(Err(err)), _) => {
                let _ = out_tx.send(Err(err)).await;
                return;
            }
            (_, Some(Err(err))) => {
                let _ = out_tx.send(Err(err)).await;
                return;
            }
            (Some(Ok(local)), None) => {
                if out_tx
                    .send(Ok(apply_row_limit(local, row_limit)))
                    .await
                    .is_err()
                {
                    return;
                }
                local_next = local_stream.next().await;
            }
            (None, Some(Ok(remote))) => {
                if out_tx
                    .send(Ok(apply_row_limit(remote, row_limit)))
                    .await
                    .is_err()
                {
                    return;
                }
                remote_next = remote_rx.recv().await;
            }
            (Some(Ok(local)), Some(Ok(remote))) => {
                let local_token = local.key.token.0;
                let remote_token = remote.key.token.0;
                if local_token < remote_token {
                    if out_tx
                        .send(Ok(apply_row_limit(local, row_limit)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    local_next = local_stream.next().await;
                    remote_next = Some(Ok(remote));
                } else if remote_token < local_token {
                    if out_tx
                        .send(Ok(apply_row_limit(remote, row_limit)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    local_next = Some(Ok(local));
                    remote_next = remote_rx.recv().await;
                } else {
                    let merged = ferrosa_storage::merge::merge_partitions(vec![local, remote]);
                    if out_tx
                        .send(Ok(apply_row_limit(merged, row_limit)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    local_next = local_stream.next().await;
                    remote_next = remote_rx.recv().await;
                }
            }
        }
    }
}

/// A peekable cursor over a fragmented partition stream (the local engine
/// stream or the remote replica's forwarded stream). Each upstream item is a
/// `<= K`-row `Partition` fragment; fragments of one partition key arrive
/// consecutively in clustering order, and the FIRST fragment of a key carries
/// the partition header (`deletion` + `static_row`).
///
/// The cursor keeps ONE peeked fragment at a time, so resident memory is
/// `O(K)` rows — a wide partition is never buffered whole.
struct FragmentCursor<S> {
    stream: S,
    /// The peeked-but-not-yet-consumed next fragment. `None` once primed and
    /// exhausted.
    peeked: Option<Partition>,
    primed: bool,
    done: bool,
}

/// The merged header (key + deletion + static row) for one partition key.
struct TokenHeader {
    key: ferrosa_common::key::DecoratedKey,
    deletion: ferrosa_sstable::types::DeletionTime,
    static_row: Option<ferrosa_sstable::types::Row>,
}

impl<S> FragmentCursor<S>
where
    S: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin,
{
    fn new(stream: S) -> Self {
        Self {
            stream,
            peeked: None,
            primed: false,
            done: false,
        }
    }

    /// Ensure `self.peeked` holds the next fragment (if any). Returns `Err`
    /// on upstream error.
    async fn ensure_peeked(&mut self) -> Result<(), ClusterError> {
        if self.primed || self.done {
            return Ok(());
        }
        match self.stream.next().await {
            None => self.done = true,
            Some(Err(e)) => return Err(e),
            Some(Ok(p)) => self.peeked = Some(p),
        }
        self.primed = true;
        Ok(())
    }

    /// Token of the next fragment, or `None` at end-of-stream.
    async fn peek_token(&mut self) -> Result<Option<i64>, ClusterError> {
        self.ensure_peeked().await?;
        Ok(self.peeked.as_ref().map(|p| p.key.token.0))
    }

    /// Consume the next fragment for `token`, returning its `(header_if_first,
    /// rows)`. The header is `Some` only on the FIRST fragment of the token
    /// (detected by the fragment carrying a header — equivalently, the first
    /// fragment popped for a token in this cursor since the previous token).
    /// Returns `Ok(None)` when the next fragment belongs to a different token
    /// or the stream ended.
    async fn take_fragment_for(
        &mut self,
        token: i64,
        first: bool,
    ) -> Result<Option<(Option<TokenHeader>, Vec<ferrosa_sstable::types::Row>)>, ClusterError> {
        self.ensure_peeked().await?;
        match self.peeked.as_ref() {
            Some(p) if p.key.token.0 == token => {
                let p = self.peeked.take().expect("checked Some");
                self.primed = false;
                let header = if first {
                    Some(TokenHeader {
                        key: p.key,
                        deletion: p.deletion,
                        static_row: p.static_row,
                    })
                } else {
                    None
                };
                Ok(Some((header, p.rows)))
            }
            _ => Ok(None),
        }
    }
}

/// A row puller over one side's fragments for a single token. Pulls rows in
/// clustering order, transparently advancing across that token's fragments,
/// and captures the token header on the first fragment. Holds at most one
/// fragment's rows (`<= K`) resident.
struct TokenRowPuller<'c, S> {
    cursor: &'c mut FragmentCursor<S>,
    token: i64,
    rows: std::vec::IntoIter<ferrosa_sstable::types::Row>,
    first: bool,
    exhausted: bool,
    header: Option<TokenHeader>,
}

impl<'c, S> TokenRowPuller<'c, S>
where
    S: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin,
{
    /// Begin pulling `token`'s rows from `cursor`, consuming the first
    /// fragment so the header is available immediately.
    async fn begin(
        cursor: &'c mut FragmentCursor<S>,
        token: i64,
    ) -> Result<TokenRowPuller<'c, S>, ClusterError> {
        let (header, rows) = match cursor.take_fragment_for(token, true).await? {
            Some((h, rows)) => (h, rows),
            None => (None, Vec::new()),
        };
        Ok(TokenRowPuller {
            cursor,
            token,
            rows: rows.into_iter(),
            first: false,
            exhausted: false,
            header,
        })
    }

    fn take_header(&mut self) -> Option<TokenHeader> {
        self.header.take()
    }

    /// Next row of this token, or `None` when the token is exhausted.
    async fn next_row(&mut self) -> Result<Option<ferrosa_sstable::types::Row>, ClusterError> {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(Some(row));
            }
            if self.exhausted {
                return Ok(None);
            }
            match self
                .cursor
                .take_fragment_for(self.token, self.first)
                .await?
            {
                Some((_, rows)) => {
                    self.rows = rows.into_iter();
                }
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
    }
}

/// Fragment-aware, memory-bounded streaming merge of the LOCAL fragmented
/// stream and the single REMOTE replica's fragmented stream.
///
/// Both sides arrive as `<= K`-row `Partition` fragments. This performs a
/// token-ordered, then clustering-ordered, 2-way row merge and RE-EMITS
/// `<= K`-row fragments, so a single wide partition is never materialised on
/// either side. Correctness mirrors `range_merger::next_fragment` and the
/// whole-partition `merge_partitions(vec![local, remote])`:
///
/// - Per token, merged partition deletion is the max `marked_for_delete_at`
///   across replicas; the static row is cell-merged via `merge::merge_rows`
///   then partition-deletion-suppressed.
/// - Rows sharing a clustering key are folded with `merge::merge_rows`
///   (cell-level LWW).
/// - Deletion suppression is applied per row exactly as
///   `merge::apply_deletions`. Since each replica already delivers
///   deletion-suppressed rows, re-running suppression is idempotent.
async fn merge_local_and_single_remote_fragmented(
    storage: std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    projected_regular_ordinals: Option<Vec<u16>>,
    remote_rx: mpsc::Receiver<crate::error::Result<Partition>>,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    let local_stream: ClusterPartitionStream = if let Some(wanted) = projected_regular_ordinals {
        Box::pin(
            storage
                .range_iter_projected_fragmented(&table_id, wanted, None, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    } else {
        Box::pin(
            storage
                .range_iter_fragmented(&table_id, None, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    };
    let local = FragmentCursor::new(local_stream);
    let remote = FragmentCursor::new(ReceiverStream { rx: remote_rx });
    let k = super::stream_request_handler::stream_chunk_row_cap();
    run_fragment_merge(local, remote, k, out_tx).await;
}

/// 2-way fragment-aware streaming merge core, generic over the two fragment
/// stream cursors so it can be unit-tested with in-memory streams. See
/// [`merge_local_and_single_remote_fragmented`] for the correctness contract.
async fn run_fragment_merge<L, R>(
    mut local: FragmentCursor<L>,
    mut remote: FragmentCursor<R>,
    k: usize,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) where
    L: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin,
    R: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin,
{
    macro_rules! send_or_abort {
        ($item:expr) => {
            if out_tx.send($item).await.is_err() {
                return;
            }
        };
    }
    macro_rules! try_or_forward {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(err) => {
                    let _ = out_tx.send(Err(err)).await;
                    return;
                }
            }
        };
    }

    loop {
        let lt = try_or_forward!(local.peek_token().await);
        let rt = try_or_forward!(remote.peek_token().await);
        let (token, use_local, use_remote) = match (lt, rt) {
            (None, None) => return,
            (Some(t), None) => (t, true, false),
            (None, Some(t)) => (t, false, true),
            (Some(a), Some(b)) => {
                if a < b {
                    (a, true, false)
                } else if b < a {
                    (b, false, true)
                } else {
                    (a, true, true)
                }
            }
        };

        // Begin per-side pullers for this token (each consumes the side's
        // first fragment and captures its header).
        let mut l_puller = if use_local {
            Some(try_or_forward!(
                TokenRowPuller::begin(&mut local, token).await
            ))
        } else {
            None
        };
        let mut r_puller = if use_remote {
            Some(try_or_forward!(
                TokenRowPuller::begin(&mut remote, token).await
            ))
        } else {
            None
        };

        // Merge the header(s).
        let mut header: Option<TokenHeader> = None;
        let mut merge_header = |src: Option<TokenHeader>| {
            let Some(src) = src else { return };
            match &mut header {
                None => header = Some(src),
                Some(d) => {
                    if src.deletion.marked_for_delete_at > d.deletion.marked_for_delete_at {
                        d.deletion = src.deletion;
                    }
                    d.static_row = match (d.static_row.take(), src.static_row) {
                        (None, None) => None,
                        (Some(r), None) | (None, Some(r)) => Some(r),
                        (Some(a), Some(b)) => Some(ferrosa_storage::merge::merge_rows(a, b)),
                    };
                }
            }
        };
        if let Some(p) = l_puller.as_mut() {
            merge_header(p.take_header());
        }
        if let Some(p) = r_puller.as_mut() {
            merge_header(p.take_header());
        }
        let mut header = header.expect("token present implies a header");

        // Partition-deletion suppression of the static row.
        if !header.deletion.is_live() {
            if let Some(sr) = header.static_row.as_mut() {
                let cut = header.deletion.marked_for_delete_at;
                sr.cells.retain(|(_c, cell)| cell.timestamp >= cut);
                if sr.cells.is_empty() {
                    header.static_row = None;
                }
            }
        }
        let partition_deleted = !header.deletion.is_live();
        let partition_cut = header.deletion.marked_for_delete_at;

        // Prime one head per side.
        let mut l_head = match l_puller.as_mut() {
            Some(p) => try_or_forward!(p.next_row().await),
            None => None,
        };
        let mut r_head = match r_puller.as_mut() {
            Some(p) => try_or_forward!(p.next_row().await),
            None => None,
        };

        let mut out_rows: Vec<ferrosa_sstable::types::Row> = Vec::with_capacity(k);
        let mut first_fragment = true;

        loop {
            // Smallest clustering across live heads.
            let smallest: Option<Vec<u8>> = {
                let mut sk: Option<&[u8]> = None;
                if let Some(r) = l_head.as_ref() {
                    sk = Some(r.clustering.as_slice());
                }
                if let Some(r) = r_head.as_ref() {
                    if sk.map(|c| r.clustering.as_slice() < c).unwrap_or(true) {
                        sk = Some(r.clustering.as_slice());
                    }
                }
                sk.map(|c| c.to_vec())
            };
            let Some(ck) = smallest else { break };

            let mut merged: Option<ferrosa_sstable::types::Row> = None;
            if l_head.as_ref().map(|r| r.clustering == ck).unwrap_or(false) {
                merged = l_head.take();
                if let Some(p) = l_puller.as_mut() {
                    l_head = try_or_forward!(p.next_row().await);
                }
            }
            if r_head.as_ref().map(|r| r.clustering == ck).unwrap_or(false) {
                let row = r_head.take().expect("matched remote head");
                merged = Some(match merged.take() {
                    Some(prev) => ferrosa_storage::merge::merge_rows(prev, row),
                    None => row,
                });
                if let Some(p) = r_puller.as_mut() {
                    r_head = try_or_forward!(p.next_row().await);
                }
            }
            let mut row = merged.expect("a head matched the smallest clustering");

            // Deletion suppression (mirrors merge::apply_deletions).
            if partition_deleted && row.primary_key_liveness.timestamp < partition_cut {
                continue;
            }
            if !row.deletion.is_live() {
                let cut = row.deletion.marked_for_delete_at;
                row.cells.retain(|(_c, cell)| cell.timestamp >= cut);
            }
            out_rows.push(row);

            if out_rows.len() >= k {
                let frag = build_out_fragment(&mut header, &mut first_fragment, &mut out_rows);
                send_or_abort!(Ok(frag));
            }
        }

        // Final fragment for this token (always emitted, so an empty
        // partition / a partition whose rows were all suppressed still
        // yields its header exactly once).
        let frag = build_out_fragment(&mut header, &mut first_fragment, &mut out_rows);
        send_or_abort!(Ok(frag));
    }
}

/// Adapter so an `mpsc::Receiver` of partition results drives the same
/// `Stream`-based cursor as the local engine stream.
struct ReceiverStream {
    rx: mpsc::Receiver<crate::error::Result<Partition>>,
}

impl futures::Stream for ReceiverStream {
    type Item = Result<Partition, ClusterError>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Build one output fragment, draining `out_rows`. The FIRST fragment of a
/// token carries the merged header (`deletion` + `static_row`); subsequent
/// fragments carry `LIVE` / `None` so the CQL bridge never double-applies
/// them.
fn build_out_fragment(
    header: &mut TokenHeader,
    first_fragment: &mut bool,
    out_rows: &mut Vec<ferrosa_sstable::types::Row>,
) -> Partition {
    let rows = std::mem::take(out_rows);
    if *first_fragment {
        *first_fragment = false;
        Partition {
            key: header.key.clone(),
            deletion: header.deletion,
            static_row: header.static_row.take(),
            rows,
        }
    } else {
        Partition {
            key: header.key.clone(),
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }
}

impl ClusterCoordinator {
    /// Uncapped streaming range-read entry point.
    ///
    /// This is used by full-table CQL scans whose result must be complete
    /// (`ALLOW FILTERING`, `SELECT DISTINCT`, and uncapped `SELECT *`). The
    /// legacy materializing range RPC is intentionally not used here because it
    /// applies `DEFAULT_RANGE_READ_LIMIT` and would silently return partial
    /// query results.
    pub async fn coordinate_range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        self.coordinate_range_read_stream_all_with(
            table_id,
            row_limit,
            self.default_cl,
            self.default_rf,
        )
        .await
    }

    /// Uncapped streaming range-read entry point with per-query consistency
    /// and keyspace replication factor.
    pub async fn coordinate_range_read_stream_all_with(
        &self,
        table_id: &TableId,
        row_limit: usize,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        self.coordinate_range_read_stream_all_with_projection(
            table_id,
            row_limit,
            cl,
            replication_factor,
            None,
        )
        .await
    }

    pub async fn coordinate_range_read_projected_stream_all_with(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        self.coordinate_range_read_stream_all_with_projection(
            table_id,
            0,
            cl,
            replication_factor,
            Some(wanted),
        )
        .await
    }

    /// Resume-capable streaming range scan with an inclusive lower-bound key.
    ///
    /// Backs `WritePath::range_read_stream_all_from` (the coordinator-side
    /// paging cursor). Only the local-only fan-out shape (CL=ONE with the
    /// keyspace RF spanning the ring, i.e. no remote replica must be consulted)
    /// is supported; any shape requiring a cross-replica merge is refused
    /// rather than returning a partial scan, matching the unbounded scan guard.
    pub async fn coordinate_range_read_stream_from(
        &self,
        table_id: &TableId,
        start: Option<&ferrosa_common::key::DecoratedKey>,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let node_count = node_ids.len();
        let all_remotes_len = node_ids
            .iter()
            .filter(|&&id| id != self.local_node_id)
            .count();
        drop(ring);

        let cl_remote_count =
            remote_count_for_cl(cl, replication_factor, node_count, all_remotes_len);

        if cl_remote_count > 0 {
            return Err(ClusterError::Internal(
                "paged cluster range scan with a resume key requires token-aware k-way stream merge across replicas; refusing to return a partial scan".into(),
            ));
        }

        Ok(Box::pin(
            self.storage
                .range_iter_fragmented(table_id, start, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        ))
    }

    /// Projection-aware resume-capable streaming range scan. See
    /// [`Self::coordinate_range_read_stream_from`]; refuses any cross-replica
    /// shape.
    pub async fn coordinate_range_read_projected_stream_from(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        start: Option<&ferrosa_common::key::DecoratedKey>,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let node_count = node_ids.len();
        let all_remotes_len = node_ids
            .iter()
            .filter(|&&id| id != self.local_node_id)
            .count();
        drop(ring);

        let cl_remote_count =
            remote_count_for_cl(cl, replication_factor, node_count, all_remotes_len);

        if cl_remote_count > 0 {
            return Err(ClusterError::Internal(
                "paged projected cluster range scan with a resume key requires token-aware k-way stream merge across replicas; refusing to return a partial scan".into(),
            ));
        }

        Ok(Box::pin(
            self.storage
                .range_iter_projected_fragmented(table_id, wanted, start, None)
                .map(|item| item.map_err(ClusterError::Storage)),
        ))
    }

    async fn coordinate_range_read_stream_all_with_projection(
        &self,
        table_id: &TableId,
        row_limit: usize,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
        projected_regular_ordinals: Option<Vec<u16>>,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let local_id = self.local_node_id;
        let node_count = nodes.len();
        let all_remotes: Vec<(uuid::Uuid, String)> = nodes
            .iter()
            .filter(|(id, _)| *id != local_id)
            .filter_map(|(_, host)| host.clone())
            .collect();
        let cl_remote_count =
            remote_count_for_cl(cl, replication_factor, node_count, all_remotes.len());
        let remotes: Vec<(uuid::Uuid, String)> =
            all_remotes.into_iter().take(cl_remote_count).collect();
        let expected_done = remotes.len();

        if expected_done > 1 {
            return Err(ClusterError::Internal(
                "unbounded cluster range scan with multiple remote replicas requires token-aware k-way stream merge; refusing to materialize full results".into(),
            ));
        }

        if expected_done == 0 {
            // Local-only fan-out (CL=ONE / RF==cluster): no cross-replica
            // merge is needed.
            //
            // When `row_limit == 0` (the true full `SELECT *` / unbounded
            // scan — the dominant OOM path) use the FRAGMENTED iterators: a
            // single wide partition streams as bounded `<= K`-row fragments
            // rather than one giant `Vec<Row>`. The CQL row bridge flattens
            // each fragment's rows independently, so the result is
            // byte-identical to the whole-partition path.
            //
            // When `row_limit > 0` (a `LIMIT N` query with partition-key
            // equality predicates — see `safe_partition_key_filter_row_limit`)
            // the per-partition row cap must be applied to the WHOLE
            // partition, not each fragment, so we keep the whole-partition
            // iterator. Such queries target specific partition keys with a
            // small N, so the partition is already bounded — no OOM risk.
            let stream: ClusterPartitionStream = match (projected_regular_ordinals, row_limit) {
                (Some(wanted), 0) => Box::pin(
                    self.storage
                        .range_iter_projected_fragmented(table_id, wanted, None, None)
                        .map(|item| item.map_err(ClusterError::Storage)),
                ),
                (None, 0) => Box::pin(
                    self.storage
                        .range_iter_fragmented(table_id, None, None)
                        .map(|item| item.map_err(ClusterError::Storage)),
                ),
                (Some(wanted), _) => Box::pin(
                    self.storage
                        .range_iter_projected(table_id, wanted, None, None, None)
                        .map(move |item| {
                            let partition = item.map_err(ClusterError::Storage)?;
                            Ok(apply_row_limit(partition, row_limit))
                        }),
                ),
                (None, _) => Box::pin(self.storage.range_iter(table_id, None, None).map(
                    move |item| {
                        let partition = item.map_err(ClusterError::Storage)?;
                        Ok(apply_row_limit(partition, row_limit))
                    },
                )),
            };
            return Ok(stream);
        }

        let request_id = self.next_stream_request_id();
        let receiver = self
            .stream_router
            .register(request_id, STREAM_RECEIVER_BUFFER);

        let req_payload = RangeReadStreamRequestPayload {
            request_id,
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            projected_regular_ordinals: projected_regular_ordinals.clone(),
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).map_err(|e| {
            ClusterError::Internal(format!("streaming range read: encode request: {e}"))
        })?);

        let (host_id, _addr) = remotes.first().ok_or_else(|| {
            ClusterError::Internal("streaming range read: missing remote replica".into())
        })?;
        if let Err(e) = self
            .peer_manager
            .fire(
                *host_id,
                Message::RangeReadStreamRequest(req_body),
                Lane::Bulk,
            )
            .await
        {
            self.stream_router.unregister(request_id);
            return Err(ClusterError::Internal(format!(
                "streaming range read: remote replica fire failed ({host_id}): {e}"
            )));
        }

        let (remote_tx, remote_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
        let router = self.stream_router.clone();
        TaskPool::current("range-read-forward").spawn(async move {
            forward_remote_range_stream(receiver, request_id, 1, remote_tx).await;
            router.unregister(request_id);
        });

        let (out_tx, out_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
        let storage = self.storage.clone();
        let table_id = table_id.clone();
        TaskPool::current("range-read-merge").spawn(async move {
            merge_local_and_single_remote_stream(
                storage,
                table_id,
                row_limit,
                projected_regular_ordinals,
                remote_rx,
                out_tx,
            )
            .await;
        });

        let stream = futures::stream::unfold(out_rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }

    /// ADR-020 streaming range-read entry point.
    ///
    /// Registers a per-call route on the shared `StreamRouter`,
    /// fires a `RangeReadStreamRequest` to every remote replica,
    /// reads the local replica directly, and consumes the streamed
    /// chunks under the idle-timeout watchdog. Always unregisters
    /// the route on exit (success or error) so the routing table
    /// does not leak.
    ///
    /// `limit` and `row_limit` are passed through to the local read
    /// for parity with the legacy method but are not enforced on
    /// remote replicas in Phase 1 — the remote handler reads via
    /// the existing storage path which already caps at 10K
    /// partitions per replica. Phase 2's lazy storage iterator
    /// removes the cap and adds an explicit max-partitions hint to
    /// the request payload.
    pub async fn coordinate_range_read_stream_limited_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<Vec<Partition>> {
        let limit = limit.clamp(1, crate::write_path::DEFAULT_RANGE_READ_LIMIT);

        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let local_id = self.local_node_id;
        let node_count = nodes.len();
        let all_remotes: Vec<(uuid::Uuid, String)> = nodes
            .iter()
            .filter(|(id, _)| *id != local_id)
            .filter_map(|(_, host)| host.clone())
            .collect();

        // CL-aware fan-out. The local replica counts as one
        // satisfied response, so we only need to contact
        // additional remotes when the configured consistency
        // demands more than one response AND the local node
        // doesn't already own every token range.
        //
        // RF=cluster_size case (every node owns every partition,
        // typical for the test cluster): CL=ONE / LOCAL_ONE is
        // satisfied entirely by the local read.
        // RF<cluster_size case: token-ownership-aware fan-out is
        // required for correctness — we conservatively fall back
        // to the full fan-out for those tables until the proper
        // per-token-range query path lands.
        let cl_remote_count = remote_count_for_cl(
            self.default_cl,
            self.default_rf,
            node_count,
            all_remotes.len(),
        );
        let remotes: Vec<(uuid::Uuid, String)> =
            all_remotes.into_iter().take(cl_remote_count).collect();
        let expected_done = remotes.len();

        // Local read goes direct — no internode hop.
        let mut all_partitions = match read_local_range_stream_limited_rows(
            self.storage.as_ref(),
            table_id,
            limit,
            row_limit,
        )
        .await
        {
            Ok(ps) => ps,
            Err(e) => return Err(ClusterError::Storage(e)),
        };

        // No remote replicas → done after the local read.
        if expected_done == 0 {
            return Ok(dedup_by_token(all_partitions));
        }

        let request_id = self.next_stream_request_id();
        let receiver = self
            .stream_router
            .register(request_id, STREAM_RECEIVER_BUFFER);

        // Fire RangeReadStreamRequest to every remote replica.
        let req_payload = RangeReadStreamRequestPayload {
            request_id,
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            projected_regular_ordinals: None,
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).map_err(|e| {
            ClusterError::Internal(format!("streaming range read: encode request: {e}"))
        })?);

        let mut fire_failures: Vec<(uuid::Uuid, String)> = Vec::new();
        for (host_id, _addr) in &remotes {
            if let Err(e) = self
                .peer_manager
                .fire(
                    *host_id,
                    Message::RangeReadStreamRequest(req_body.clone()),
                    Lane::Bulk,
                )
                .await
            {
                tracing::warn!(
                    request_id,
                    peer = %host_id,
                    "streaming range read: failed to fire request: {e}"
                );
                fire_failures.push((*host_id, e.to_string()));
            }
        }

        // If every fire failed, no Done will ever arrive — bail out
        // immediately rather than hanging on the watchdog.
        if fire_failures.len() == expected_done {
            self.stream_router.unregister(request_id);
            return Err(ClusterError::Internal(format!(
                "streaming range read: every replica fire failed ({fire_failures:?})"
            )));
        }

        // Consume only the replicas we successfully fired to.
        let live_remote_count = expected_done - fire_failures.len();
        let consume_result = consume_range_stream(
            receiver,
            STREAMING_IDLE_TIMEOUT,
            live_remote_count,
            request_id,
        )
        .await;

        // Always unregister so the routing table doesn't leak.
        self.stream_router.unregister(request_id);

        match consume_result {
            Ok(outcome) => {
                all_partitions.extend(outcome.partitions);
                if !fire_failures.is_empty() {
                    tracing::warn!(
                        request_id,
                        failed = fire_failures.len(),
                        succeeded = live_remote_count,
                        "streaming range read: partial — some replicas could not be reached"
                    );
                }
                Ok(dedup_by_token(all_partitions))
            }
            Err(StreamConsumeError::IdleTimeout { idle_timeout, .. }) => {
                Err(ClusterError::Internal(format!(
                    "streaming range read: idle timeout after {idle_timeout:?}"
                )))
            }
            Err(StreamConsumeError::ChannelClosedBeforeDone {
                delivered_done,
                expected_done,
            }) => Err(ClusterError::Internal(format!(
                "streaming range read: channel closed after {delivered_done}/{expected_done} Done frames"
            ))),
            Err(e) => Err(ClusterError::Internal(format!(
                "streaming range read: {e:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::ConsistencyLevel as CL;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

    fn dk(tok_seed: &[u8]) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::from(tok_seed))
    }

    fn trow(clustering: i32, value: &[u8], ts: i64) -> Row {
        Row {
            clustering: clustering.to_be_bytes().to_vec(),
            cells: vec![(0, ferrosa_common::CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    /// Split a whole partition into `<= k`-row fragment `Partition`s per the
    /// wire contract: the first fragment carries the header (deletion +
    /// static row), later fragments carry `LIVE` / `None`.
    fn fragment_partition(p: &Partition, k: usize) -> Vec<Partition> {
        let mut out = Vec::new();
        let mut first = true;
        let mut chunks = p.rows.chunks(k).peekable();
        // Always emit at least one fragment so an empty / header-only
        // partition still carries its header.
        if chunks.peek().is_none() {
            out.push(Partition {
                key: p.key.clone(),
                deletion: p.deletion,
                static_row: p.static_row.clone(),
                rows: Vec::new(),
            });
            return out;
        }
        for chunk in chunks {
            if first {
                first = false;
                out.push(Partition {
                    key: p.key.clone(),
                    deletion: p.deletion,
                    static_row: p.static_row.clone(),
                    rows: chunk.to_vec(),
                });
            } else {
                out.push(Partition {
                    key: p.key.clone(),
                    deletion: DeletionTime::LIVE,
                    static_row: None,
                    rows: chunk.to_vec(),
                });
            }
        }
        out
    }

    fn stream_of(
        frags: Vec<Partition>,
    ) -> impl futures::Stream<Item = Result<Partition, ClusterError>> + Unpin {
        futures::stream::iter(frags.into_iter().map(Ok))
    }

    /// Flatten a fragment stream into whole partitions (one per key), the way
    /// the CQL row bridge does (append rows per item).
    fn flatten(frags: Vec<Partition>) -> Vec<Partition> {
        let mut out: Vec<Partition> = Vec::new();
        for f in frags {
            match out.last_mut() {
                Some(p) if p.key == f.key => p.rows.extend(f.rows),
                _ => out.push(f),
            }
        }
        out
    }

    /// Drive `run_fragment_merge` to completion over two in-memory fragment
    /// streams and collect the emitted fragments.
    async fn drive_merge(
        local: Vec<Partition>,
        remote: Vec<Partition>,
        k: usize,
    ) -> Vec<Partition> {
        let (tx, mut rx) = mpsc::channel(1024);
        let lc = FragmentCursor::new(stream_of(local));
        let rc = FragmentCursor::new(stream_of(remote));
        let driver = tokio::spawn(async move { run_fragment_merge(lc, rc, k, tx).await });
        let mut out = Vec::new();
        while let Some(item) = rx.recv().await {
            out.push(item.expect("merge produced an error"));
        }
        driver.await.unwrap();
        out
    }

    /// The fragment-aware cross-replica merge must reproduce, per token, the
    /// whole-partition `merge_partitions(vec![local, remote])` result —
    /// byte-identical after flattening — for LWW conflicts, tombstones,
    /// static rows, and disjoint tokens, across several `k`.
    #[tokio::test]
    async fn fragment_cross_replica_merge_equiv_whole_partition_merge() {
        // Token-ordered keys. (DecoratedKey orders by token then key bytes;
        // we sort our inputs by key to feed both streams in order.)
        let mut keys = [dk(b"alpha"), dk(b"bravo"), dk(b"charlie"), dk(b"delta")];
        keys.sort();

        // key0: LWW conflict on shared clustering keys (remote newer wins on evens).
        let local0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..30)
                .map(|c| trow(c, format!("L{c}").as_bytes(), 1000))
                .collect(),
        };
        let remote0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..30)
                .filter(|c| c % 2 == 0)
                .map(|c| trow(c, format!("R{c}").as_bytes(), 2000))
                .collect(),
        };
        // key1: local-only (disjoint token from remote).
        let local1 = Partition {
            key: keys[1].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..12)
                .map(|c| trow(c, format!("x{c}").as_bytes(), 1500))
                .collect(),
        };
        // key2: remote-only.
        let remote2 = Partition {
            key: keys[2].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..7)
                .map(|c| trow(c, format!("y{c}").as_bytes(), 1500))
                .collect(),
        };
        // key3: both sides, with a static row on local and a partition
        // tombstone on remote suppressing local's older rows.
        let mut local3 = Partition {
            key: keys[3].clone(),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![(0, ferrosa_common::CellValue::live(b"st".to_vec(), 9000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: (0..20)
                .map(|c| trow(c, format!("o{c}").as_bytes(), 1000))
                .collect(),
        };
        local3.rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
        let remote3 = Partition {
            key: keys[3].clone(),
            deletion: DeletionTime::new(1500, 100), // suppresses local's ts=1000 rows
            static_row: None,
            rows: (20..40)
                .map(|c| trow(c, format!("n{c}").as_bytes(), 3000))
                .collect(),
        };

        // Whole-partition reference per token.
        let reference = {
            let mut r = vec![
                ferrosa_storage::merge::merge_partitions(vec![local0.clone(), remote0.clone()]),
                local1.clone(),
                remote2.clone(),
                ferrosa_storage::merge::merge_partitions(vec![local3.clone(), remote3.clone()]),
            ];
            r.sort_by(|a, b| a.key.cmp(&b.key));
            r
        };

        let mut local_whole = [local0, local1, local3];
        local_whole.sort_by(|a, b| a.key.cmp(&b.key));
        let mut remote_whole = [remote0, remote2, remote3];
        remote_whole.sort_by(|a, b| a.key.cmp(&b.key));

        for k in [1usize, 2, 3, 8, 1024] {
            let local_frags: Vec<Partition> = local_whole
                .iter()
                .flat_map(|p| fragment_partition(p, k))
                .collect();
            let remote_frags: Vec<Partition> = remote_whole
                .iter()
                .flat_map(|p| fragment_partition(p, k))
                .collect();

            let emitted = drive_merge(local_frags, remote_frags, k).await;
            // No emitted fragment exceeds k rows (bounded memory contract).
            for f in &emitted {
                assert!(
                    f.rows.len() <= k,
                    "emitted fragment {} exceeds k={k}",
                    f.rows.len()
                );
            }
            let merged = flatten(emitted);
            assert_eq!(
                merged, reference,
                "fragment cross-replica merge(k={k}) diverged from whole-partition merge"
            );
        }
    }

    /// RF=cluster_size: local replica owns every partition, so
    /// CL=ONE / LOCAL_ONE needs zero remote replicas. QUORUM needs
    /// floor(RF/2)+1 total responses → that count minus 1 (local)
    /// is the remote count. ALL needs every remote.
    #[test]
    fn cl_remote_count_rf_equals_cluster() {
        // 3 nodes, RF=3, 2 remotes.
        assert_eq!(remote_count_for_cl(CL::One, 3, 3, 2), 0);
        assert_eq!(remote_count_for_cl(CL::LocalOne, 3, 3, 2), 0);
        assert_eq!(remote_count_for_cl(CL::Quorum, 3, 3, 2), 1);
        assert_eq!(remote_count_for_cl(CL::LocalQuorum, 3, 3, 2), 1);
        assert_eq!(remote_count_for_cl(CL::All, 3, 3, 2), 2);
        // 5 nodes, RF=5, 4 remotes.
        assert_eq!(remote_count_for_cl(CL::One, 5, 5, 4), 0);
        assert_eq!(remote_count_for_cl(CL::Quorum, 5, 5, 4), 2);
        assert_eq!(remote_count_for_cl(CL::All, 5, 5, 4), 4);
    }

    /// RF<cluster_size: we cannot prove the local node owns every
    /// token range, so we fall back to the full fan-out for
    /// correctness — under-reading would surface as missing
    /// partitions for whichever ranges the local node doesn't own.
    /// Replace with a token-aware query plan in a follow-up.
    #[test]
    fn cl_remote_count_rf_less_than_cluster_falls_back_to_full_fanout() {
        // 5 nodes, RF=3 — local may not own every range.
        assert_eq!(remote_count_for_cl(CL::One, 3, 5, 4), 4);
        assert_eq!(remote_count_for_cl(CL::Quorum, 3, 5, 4), 4);
        assert_eq!(remote_count_for_cl(CL::All, 3, 5, 4), 4);
    }

    #[test]
    fn coordinate_streaming_range_read_does_not_call_vec_local_read() {
        let source = include_str!("range_read_stream.rs");
        let body = source
            .split("pub async fn coordinate_range_read_stream_limited_rows")
            .nth(1)
            .and_then(|rest| rest.split("#[cfg(test)]").next())
            .expect("streaming coordinator body must be present");

        assert!(
            !body.contains("read_local_range_limited_rows"),
            "streaming range coordinator must not call the Vec-returning local read helper: {body}"
        );
        assert!(
            body.contains("read_local_range_stream_limited_rows"),
            "streaming range coordinator must route local reads through the bounded streaming helper: {body}"
        );
        let helper = source
            .split("async fn read_local_range_stream_limited_rows")
            .nth(1)
            .and_then(|rest| rest.split("impl ClusterCoordinator").next())
            .expect("streaming local read helper must be present");
        assert!(
            helper.contains("range_iter") && helper.contains("while partitions.len() < limit"),
            "streaming local read helper must pull from range_iter under the requested limit: {helper}"
        );
    }

    #[test]
    fn unbounded_streaming_range_read_boundary_must_not_return_vec() {
        let source = include_str!("range_read_stream.rs");
        let body = source
            .split("async fn coordinate_range_read_stream_all_with_projection")
            .nth(1)
            .and_then(|rest| {
                rest.split("pub async fn coordinate_range_read_stream_limited_rows")
                    .next()
            })
            .expect("unbounded streaming range-read implementation body must be present");

        assert!(
            !body.contains("Result<Vec<Partition>>"),
            "unbounded streaming range reads must expose a partition stream, not materialize Vec<Partition>"
        );
        assert!(
            !body.contains("let mut all_partitions"),
            "unbounded streaming range reads must not accumulate local and remote partitions before returning"
        );
        assert!(
            body.contains("refusing to materialize full results"),
            "remote unbounded scans that need replica merge must fail clearly instead of falling back to materialization"
        );
    }
}
