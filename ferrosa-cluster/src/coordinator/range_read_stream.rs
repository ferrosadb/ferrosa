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

use super::stream_consumer::StreamConsumeError;
use super::ClusterCoordinator;
use crate::error::ClusterError;
use crate::raft::handlers::{
    partition_from_wire, RangeReadStreamCancelPayload, RangeReadStreamChunkPayload,
    RangeReadStreamDonePayload, RangeReadStreamHeartbeatPayload, RangeReadStreamRequestPayload,
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

/// Type-erased fragment stream, so heterogeneous sources (the local engine
/// stream and N remote replica receivers) can share one
/// `FragmentCursor<BoxedFragmentStream>` type inside the N-way merge.
type BoxedFragmentStream = Pin<Box<dyn Stream<Item = Result<Partition, ClusterError>> + Send>>;

impl ClusterCoordinator {
    /// COUNT(*) over the whole token ring for `table_id`.
    ///
    /// Fast path: when the local node provably owns every token range at the
    /// configured consistency (`range_read_remotes` empty — e.g. CL=ONE with
    /// the keyspace RF spanning the cluster), count locally via
    /// `StorageEngine::count_range`, which uses the metadata-only merger
    /// (`range_merger::merger_for_metadata_sources`) so cell payloads are
    /// byte-skipped at every SSTable.
    ///
    /// Fan-out path: otherwise the local replica holds only the partitions in
    /// its owned token ranges, so a local-only count would UNDERCOUNT. The
    /// count then drives the same CL-selected, token-deduped streaming
    /// range-read fan-out the full `SELECT` uses and sums the live rows.
    pub async fn coordinate_range_count(&self, table_id: &TableId) -> crate::error::Result<u64> {
        // Correctness over speed (forge t_8c4e44e8): the local replica only
        // holds the partitions whose tokens fall in ITS owned ranges. When the
        // keyspace RF does not span the whole ring (`RF < node_count`, or any
        // CL that demands more than the local response), the local-only
        // metadata count silently returns a nondeterministic UNDERCOUNT — it
        // tallies only the locally-resident subset while a full `SELECT`
        // (which fans out + dedups by token) sees every row.
        //
        // Reuse the same CL/RF fan-out decision the streaming range read uses.
        // When `range_read_remotes` is empty the local node provably owns every
        // token range at this CL, so the fast metadata path is exact. Otherwise
        // we MUST fan out across replicas and count the token-deduped result —
        // never the local subset.
        let remotes = self.range_read_remotes(self.default_cl, self.default_rf);
        if remotes.is_empty() {
            return self
                .storage
                .count_range(table_id, None, None)
                .map_err(ClusterError::Storage);
        }

        // Fan out the unbounded streaming range read (local + CL-selected
        // remotes, token-aware N-way merge, deduped by token) and count the
        // live rows. Fragments of one partition share a key, so summing
        // `rows.len()` across fragments yields that partition's row count; the
        // static row only rides the first fragment. This matches
        // `StorageEngine::count_range`'s COUNT(*) semantics (one per row, plus
        // one for a present static row) but over the WHOLE ring.
        let mut stream = self
            .coordinate_range_read_stream_all_with(table_id, 0, self.default_cl, self.default_rf)
            .await?;
        let mut total: u64 = 0;
        while let Some(item) = stream.next().await {
            let partition = item?;
            total = total.saturating_add(partition.rows.len() as u64);
            if partition.static_row.is_some() {
                total = total.saturating_add(1);
            }
        }
        Ok(total)
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

fn apply_row_limit(mut partition: Partition, row_limit: usize) -> Partition {
    if row_limit > 0 {
        partition.rows.truncate(row_limit);
    }
    partition
}

/// Apply a per-partition row cap to a MERGED fragment stream (the output of
/// `run_fragment_merge_nway`), forwarding capped fragments to `out_tx`.
///
/// A partition arrives as a contiguous run of fragments sharing one `key` (the
/// header — partition deletion + static row — rides on the first fragment).
/// We count emitted clustering rows per key and truncate at `row_limit`,
/// dropping the fully-capped tail fragments. The result is byte-identical to
/// running [`apply_row_limit`] on the whole merged partition — but streaming,
/// so memory stays `O(num_sources + k)`. `row_limit == 0` means "no cap" and
/// forwards everything unchanged.
///
/// Used for capped (`LIMIT N` + partition-key equality) multi-replica range
/// scans, which target bounded partitions; this replaces the former loud
/// refusal of that shape.
async fn apply_per_partition_row_limit(
    mut in_rx: mpsc::Receiver<crate::error::Result<Partition>>,
    row_limit: usize,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    let mut cur_key: Option<ferrosa_common::DecoratedKey> = None;
    let mut emitted: usize = 0;
    while let Some(item) = in_rx.recv().await {
        let mut p = match item {
            Ok(p) => p,
            Err(e) => {
                let _ = out_tx.send(Err(e)).await;
                return;
            }
        };
        if row_limit == 0 {
            if out_tx.send(Ok(p)).await.is_err() {
                return;
            }
            continue;
        }
        if cur_key.as_ref() != Some(&p.key) {
            cur_key = Some(p.key.clone());
            emitted = 0;
        }
        let remaining = row_limit.saturating_sub(emitted);
        if p.rows.len() > remaining {
            p.rows.truncate(remaining);
        }
        emitted += p.rows.len();
        // Forward when the fragment still carries rows, or it is the
        // header-bearing first fragment of a partition (partition deletion or a
        // static row must survive a row cap). Otherwise it is a fully-capped
        // tail fragment — drop it.
        let carries_header =
            p.static_row.is_some() || p.deletion != ferrosa_sstable::types::DeletionTime::LIVE;
        if (!p.rows.is_empty() || carries_header) && out_tx.send(Ok(p)).await.is_err() {
            return;
        }
    }
}

/// Map a streaming-fragment failure to the error surfaced to the client.
///
/// t_4b94ab56: a remote replica's fragment stream closing or going idle before
/// its terminating `Done` is a TRANSIENT, RETRYABLE condition (a replica dropped
/// mid-stream), not an internal bug. Range scans are idempotent, so we surface
/// these as `ReadTimeout` — which standard drivers (and ferrosa-memory) retry —
/// instead of an opaque `internal: ChannelClosedBeforeDone`. Genuine protocol
/// faults (decode / unexpected frame) stay `Internal` so they remain loud for
/// diagnosis.
fn next_remote_error(err: StreamConsumeError) -> crate::error::Result<Partition> {
    let cluster_err = match err {
        StreamConsumeError::ChannelClosedBeforeDone {
            delivered_done,
            expected_done,
        } => {
            tracing::warn!(
                delivered_done,
                expected_done,
                "streaming range read: remote stream closed before Done — returning retryable ReadTimeout"
            );
            ClusterError::ReadTimeout {
                // Per-replica fragment granularity; representative — the exact
                // fan-out CL is not threaded to this depth.
                consistency: "ONE".to_string(),
                received: delivered_done,
                required: expected_done,
                // The scan was incomplete, so no usable data — signal retry.
                data_present: false,
            }
        }
        StreamConsumeError::IdleTimeout {
            request_id,
            idle_timeout,
        } => {
            tracing::warn!(
                request_id,
                ?idle_timeout,
                "streaming range read: idle timeout — returning retryable ReadTimeout"
            );
            ClusterError::ReadTimeout {
                consistency: "ONE".to_string(),
                received: 0,
                required: 1,
                data_present: false,
            }
        }
        // Decode / unexpected-frame faults are genuine protocol bugs — keep them
        // loud (non-retryable) so they surface for diagnosis.
        other => ClusterError::Internal(format!("streaming range read: {other:?}")),
    };
    Err(cluster_err)
}

/// How a per-replica forwarder task ended. Anything other than `Completed`
/// means the REMOTE producer may still be streaming — the coordinator must
/// fire a `RangeReadStreamCancel` so the replica stops between batches instead
/// of scanning the whole table into `Lane::Bulk` for nobody (t_3fc6be3c: the
/// uncancelled `pages × replicas` producers starved live streams into
/// heartbeat-defunct connections and OOM-killed replicas).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardOutcome {
    /// The stream terminated normally (every expected `Done` delivered).
    Completed,
    /// The consumer dropped the merged stream mid-flight (a paged read fills
    /// its page and abandons the rest — every page but the last does this).
    Abandoned,
    /// The stream failed (idle timeout, route closed, decode error); the
    /// error was forwarded to the consumer as a loud, retryable failure.
    Failed,
}

async fn forward_remote_range_stream(
    receiver: mpsc::Receiver<Message>,
    request_id: u32,
    expected_done: usize,
    tx: mpsc::Sender<crate::error::Result<Partition>>,
) -> ForwardOutcome {
    match forward_remote_range_stream_inner(receiver, request_id, expected_done, tx.clone()).await {
        Ok(outcome) => outcome,
        Err(err) => {
            let _ = tx.send(next_remote_error(err)).await;
            ForwardOutcome::Failed
        }
    }
}

async fn forward_remote_range_stream_inner(
    receiver: mpsc::Receiver<Message>,
    request_id: u32,
    expected_done: usize,
    tx: mpsc::Sender<crate::error::Result<Partition>>,
) -> Result<ForwardOutcome, StreamConsumeError> {
    let mut watchdog = IdleTimeoutWatchdog::new(receiver, STREAMING_IDLE_TIMEOUT);
    let mut delivered_done = 0usize;

    loop {
        if delivered_done >= expected_done {
            return Ok(ForwardOutcome::Completed);
        }

        // Notice consumer abandonment PROMPTLY — even when the producer is
        // between chunks and nothing is arriving — so the caller can cancel
        // the remote producer instead of waiting for the next frame (or the
        // 30 s idle timeout) to observe the closed channel.
        let next = tokio::select! {
            biased;
            _ = tx.closed() => return Ok(ForwardOutcome::Abandoned),
            next = watchdog.next() => next,
        };
        let next = next.map_err(|elapsed| StreamConsumeError::IdleTimeout {
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
                        return Ok(ForwardOutcome::Abandoned);
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
                    return Err(StreamConsumeError::TruncatedReplica { request_id });
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

    /// Box the underlying stream into a type-erased `FragmentCursor`, so
    /// heterogeneous sources (local engine stream + remote receivers) can be
    /// collected into a single `Vec<FragmentCursor<BoxedFragmentStream>>` and
    /// fed to [`run_fragment_merge_nway`].
    ///
    /// The cursor must be freshly constructed (un-primed): boxing a cursor that
    /// already holds a peeked fragment would silently drop it. All call sites
    /// box right after [`FragmentCursor::new`].
    fn boxed(self) -> FragmentCursor<BoxedFragmentStream>
    where
        S: Send + 'static,
    {
        debug_assert!(
            !self.primed && self.peeked.is_none() && !self.done,
            "FragmentCursor::boxed requires an un-primed cursor"
        );
        FragmentCursor {
            stream: Box::pin(self.stream),
            peeked: None,
            primed: false,
            done: false,
        }
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

/// Fold one source's partition header (`deletion` + `static_row`) into the
/// running merged header for a token. Mirrors the per-token header merge in
/// [`merge_partitions`](ferrosa_storage::merge::merge_partitions): partition
/// deletion is the max `marked_for_delete_at`, the static row is cell-merged
/// via [`merge::merge_rows`](ferrosa_storage::merge::merge_rows).
fn fold_token_header(dst: &mut Option<TokenHeader>, src: Option<TokenHeader>) {
    let Some(src) = src else { return };
    match dst {
        None => *dst = Some(src),
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
}

/// Apply partition-deletion suppression to the merged static row, exactly as
/// `ferrosa_storage::merge::apply_deletions`: when the partition is deleted,
/// static cells older than the deletion are dropped and an emptied static row
/// collapses to `None`.
fn suppress_static_under_partition_deletion(header: &mut TokenHeader) {
    if header.deletion.is_live() {
        return;
    }
    if let Some(sr) = header.static_row.as_mut() {
        let cut = header.deletion.marked_for_delete_at;
        sr.cells.retain(|(_c, cell)| cell.timestamp >= cut);
        if sr.cells.is_empty() {
            header.static_row = None;
        }
    }
}

/// Apply per-row deletion suppression, mirroring
/// `ferrosa_storage::merge::apply_deletions`. Returns `true` if the row survives
/// (caller emits it), `false` if it is suppressed by the partition tombstone.
fn suppress_row_deletions(
    row: &mut ferrosa_sstable::types::Row,
    partition_deleted: bool,
    partition_cut: i64,
) -> bool {
    if partition_deleted && row.primary_key_liveness.timestamp < partition_cut {
        return false;
    }
    if !row.deletion.is_live() {
        let cut = row.deletion.marked_for_delete_at;
        row.cells.retain(|(_c, cell)| cell.timestamp >= cut);
    }
    true
}

/// N-way fragment-aware streaming merge core, generalising
/// [`run_fragment_merge`] to an arbitrary number of homogeneous fragment-stream
/// cursors (one per replica). See [`merge_local_and_single_remote_fragmented`]
/// for the per-token correctness contract; the same rules hold across N sources
/// because [`merge::merge_rows`](ferrosa_storage::merge::merge_rows) is an
/// order-independent cell-level LWW fold, so folding same-clustering rows from
/// any subset of sources in any order yields the byte-identical result as
/// [`merge_partitions`](ferrosa_storage::merge::merge_partitions).
///
/// Resident set is `O(num_sources + k)`: each cursor holds at most one peeked
/// `<= k`-row fragment, and the merge buffers at most `k` output rows plus one
/// head row per live source before emitting a bounded fragment.
async fn run_fragment_merge_nway<S>(
    cursors: Vec<FragmentCursor<S>>,
    k: usize,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) where
    S: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin,
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

    let mut cursors = cursors;

    loop {
        // Smallest token across all live sources, and which sources hold it.
        let mut min_token: Option<i64> = None;
        for c in cursors.iter_mut() {
            let t = try_or_forward!(c.peek_token().await);
            if let Some(t) = t {
                min_token = Some(match min_token {
                    Some(m) => m.min(t),
                    None => t,
                });
            }
        }
        let Some(token) = min_token else {
            return;
        };

        // Begin a row puller for every source holding `token`. The puller
        // consumes the source's first fragment for the token and captures its
        // header. Sources without the token are skipped (puller is `None`).
        let mut pullers: Vec<Option<TokenRowPuller<'_, S>>> = Vec::with_capacity(cursors.len());
        for c in cursors.iter_mut() {
            let has = try_or_forward!(c.peek_token().await) == Some(token);
            if has {
                pullers.push(Some(try_or_forward!(TokenRowPuller::begin(c, token).await)));
            } else {
                pullers.push(None);
            }
        }

        // Merge the headers across all participating sources.
        let mut header: Option<TokenHeader> = None;
        for p in pullers.iter_mut().flatten() {
            fold_token_header(&mut header, p.take_header());
        }
        let mut header = header.expect("token present implies at least one header");

        suppress_static_under_partition_deletion(&mut header);
        let partition_deleted = !header.deletion.is_live();
        let partition_cut = header.deletion.marked_for_delete_at;

        // Prime one head row per live source.
        let mut heads: Vec<Option<ferrosa_sstable::types::Row>> = Vec::with_capacity(pullers.len());
        for p in pullers.iter_mut() {
            let head = match p {
                Some(puller) => try_or_forward!(puller.next_row().await),
                None => None,
            };
            heads.push(head);
        }

        let mut out_rows: Vec<ferrosa_sstable::types::Row> = Vec::with_capacity(k);
        let mut first_fragment = true;

        loop {
            // Smallest clustering across all live heads.
            let mut smallest: Option<&[u8]> = None;
            for h in heads.iter() {
                if let Some(r) = h.as_ref() {
                    if smallest
                        .map(|c| r.clustering.as_slice() < c)
                        .unwrap_or(true)
                    {
                        smallest = Some(r.clustering.as_slice());
                    }
                }
            }
            let Some(ck) = smallest.map(|c| c.to_vec()) else {
                break;
            };

            // Fold every source whose head matches `ck`, advancing those
            // sources. `merge_rows` is order-independent, so left-fold across
            // the participating sources reproduces `merge_partitions`.
            let mut merged: Option<ferrosa_sstable::types::Row> = None;
            for (i, head) in heads.iter_mut().enumerate() {
                if head.as_ref().map(|r| r.clustering == ck).unwrap_or(false) {
                    let row = head.take().expect("matched head");
                    merged = Some(match merged.take() {
                        Some(prev) => ferrosa_storage::merge::merge_rows(prev, row),
                        None => row,
                    });
                    if let Some(puller) = pullers[i].as_mut() {
                        *head = try_or_forward!(puller.next_row().await);
                    }
                }
            }
            let mut row = merged.expect("a head matched the smallest clustering");

            if !suppress_row_deletions(&mut row, partition_deleted, partition_cut) {
                continue;
            }
            out_rows.push(row);

            if out_rows.len() >= k {
                let frag = build_out_fragment(&mut header, &mut first_fragment, &mut out_rows);
                send_or_abort!(Ok(frag));
            }
        }

        // Final fragment for this token — always emitted so an empty / fully
        // suppressed partition still yields its header exactly once.
        let frag = build_out_fragment(&mut header, &mut first_fragment, &mut out_rows);
        send_or_abort!(Ok(frag));
    }
}

/// 2-way fragment-aware streaming merge core, generic over the two fragment
/// stream cursors so it can be unit-tested with in-memory streams. See
/// [`merge_local_and_single_remote_fragmented`] for the correctness contract.
///
/// This is exactly the `N == 2` case of [`run_fragment_merge_nway`]: both
/// cursors are type-erased via [`FragmentCursor::boxed`] and merged through the
/// shared N-way core, so the 2-way and N-way paths share identical semantics by
/// construction. The cursors must be freshly constructed (un-primed) — every
/// call site boxes right after [`FragmentCursor::new`].
async fn run_fragment_merge<L, R>(
    local: FragmentCursor<L>,
    remote: FragmentCursor<R>,
    k: usize,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) where
    L: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin + Send + 'static,
    R: futures::Stream<Item = Result<Partition, ClusterError>> + Unpin + Send + 'static,
{
    run_fragment_merge_nway(vec![local.boxed(), remote.boxed()], k, out_tx).await;
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

/// Fan out a `RangeReadStreamRequest` to each remote replica in `remotes`,
/// spawning one forwarder task per replica that drains the StreamRouter
/// receiver into a bounded per-replica `mpsc` channel. Returns one boxed
/// fragment stream per replica that was successfully fired to, plus the count
/// of fire failures. Each returned stream is memory-bounded by
/// `STREAM_RECEIVER_BUFFER` (back-pressures the inbound dispatch).
///
/// `start_key`, when `Some`, is shipped as the inclusive lower bound so each
/// remote streams only the resumed suffix of the range (see
/// [`RangeReadStreamRequestPayload::start_key`]).
///
/// At least one returned stream is required by the caller for a multi-replica
/// merge; if every fire fails the caller surfaces an error rather than
/// returning a partial scan.
struct RemoteFanout {
    streams: Vec<ClusterPartitionStream>,
    fire_failures: usize,
}

impl ClusterCoordinator {
    async fn fan_out_remote_fragment_streams(
        &self,
        table_id: &TableId,
        remotes: &[(uuid::Uuid, String)],
        projected_regular_ordinals: Option<&[u16]>,
        start_key: Option<&ferrosa_common::key::DecoratedKey>,
    ) -> crate::error::Result<RemoteFanout> {
        let mut streams: Vec<ClusterPartitionStream> = Vec::with_capacity(remotes.len());
        let mut fire_failures = 0usize;

        for (host_id, _addr) in remotes {
            let request_id = self.next_stream_request_id();
            let receiver = self
                .stream_router
                .register(request_id, STREAM_RECEIVER_BUFFER);

            let req_payload = RangeReadStreamRequestPayload {
                request_id,
                keyspace: table_id.keyspace.clone(),
                table: table_id.table.clone(),
                projected_regular_ordinals: projected_regular_ordinals.map(|w| w.to_vec()),
                start_key: start_key.map(|k| k.key.as_bytes().to_vec()),
            };
            let req_body = Bytes::from(bincode::serialize(&req_payload).map_err(|e| {
                ClusterError::Internal(format!("streaming range read: encode request: {e}"))
            })?);

            if let Err(e) = self
                .peer_manager
                .fire(
                    *host_id,
                    Message::RangeReadStreamRequest(req_body),
                    Lane::Bulk,
                )
                .await
            {
                tracing::warn!(
                    request_id,
                    peer = %host_id,
                    "streaming range read: failed to fire request: {e}"
                );
                self.stream_router.unregister(request_id);
                fire_failures += 1;
                continue;
            }

            let (remote_tx, remote_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
            let router = self.stream_router.clone();
            let peer_manager = self.peer_manager.clone();
            let peer_host = *host_id;
            TaskPool::current("range-read-forward").spawn(async move {
                let outcome = forward_remote_range_stream(receiver, request_id, 1, remote_tx).await;
                router.unregister(request_id);
                // Anything but a clean Done means the remote producer may
                // still be streaming for nobody — tell it to stop. Fired from
                // THIS live async task (not a Drop guard: the reverted
                // 3ec30240 guard never actually sent a cancel on the live
                // cluster), so the fire is a plain awaited call that either
                // succeeds or logs loudly.
                if outcome != ForwardOutcome::Completed {
                    fire_stream_cancel(&peer_manager, peer_host, request_id, outcome).await;
                }
            });

            let stream = futures::stream::unfold(remote_rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            streams.push(Box::pin(stream));
        }

        Ok(RemoteFanout {
            streams,
            fire_failures,
        })
    }
}

/// Fire a best-effort `RangeReadStreamCancel` for `request_id` to `host_id`.
///
/// t_3fc6be3c — without this, a consumer that abandons a coordinated stream
/// mid-flight (every paged read does, on every page but the last) leaves the
/// remote producer scanning the WHOLE remaining table and firing chunks onto
/// `Lane::Bulk` for nobody; `pages × replicas` of those starved live streams
/// (heartbeat-defunct connections) and OOM-killed replicas. The producer's
/// request handler observes the `CancellationToken` between batches and stops
/// within one chunk.
///
/// Logged at info on success so a live deploy can verify cancels are actually
/// flowing (the reverted Drop-guard approach silently sent zero), and at warn
/// on failure — the producer then streams until Done or connection teardown,
/// which is degraded-but-visible, never silent.
async fn fire_stream_cancel(
    peers: &ferrosa_net::peer::PeerManager,
    host_id: uuid::Uuid,
    request_id: u32,
    outcome: ForwardOutcome,
) {
    let payload = RangeReadStreamCancelPayload { request_id };
    let body = bincode::serialize(&payload)
        .expect("RangeReadStreamCancelPayload serialization is infallible");
    match peers
        .fire(
            host_id,
            Message::RangeReadStreamCancel(Bytes::from(body)),
            Lane::Bulk,
        )
        .await
    {
        Ok(()) => tracing::info!(
            request_id,
            peer = %host_id,
            ?outcome,
            "streaming range read: stream ended without Done; fired RangeReadStreamCancel to stop the remote producer"
        ),
        Err(e) => tracing::warn!(
            request_id,
            peer = %host_id,
            ?outcome,
            "streaming range read: cancel fire failed — remote producer keeps streaming until Done or disconnect: {e}"
        ),
    }
}

/// Drive a token-aware N-way fragment merge of the LOCAL fragmented stream and
/// one or more REMOTE replica fragment streams, emitting `<= k`-row fragments
/// into `out_tx`. The local stream is start-bounded by `start` (the resume
/// cursor); the remote streams are already start-bounded at their producers.
/// Resident set is `O(num_sources + k)` — every cursor holds at most one peeked
/// fragment and the merge buffers at most `k` output rows.
async fn merge_local_and_remotes_fragmented(
    storage: std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    projected_regular_ordinals: Option<Vec<u16>>,
    start: Option<ferrosa_common::key::DecoratedKey>,
    remote_streams: Vec<ClusterPartitionStream>,
    out_tx: mpsc::Sender<crate::error::Result<Partition>>,
) {
    let local_stream: ClusterPartitionStream = if let Some(wanted) = projected_regular_ordinals {
        Box::pin(
            storage
                .range_iter_projected_fragmented(&table_id, wanted, start.as_ref(), None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    } else {
        Box::pin(
            storage
                .range_iter_fragmented(&table_id, start.as_ref(), None)
                .map(|item| item.map_err(ClusterError::Storage)),
        )
    };

    let mut cursors: Vec<FragmentCursor<BoxedFragmentStream>> =
        Vec::with_capacity(1 + remote_streams.len());
    cursors.push(FragmentCursor::new(local_stream).boxed());
    for rs in remote_streams {
        cursors.push(FragmentCursor::new(rs).boxed());
    }

    let k = super::stream_request_handler::stream_chunk_row_cap();
    run_fragment_merge_nway(cursors, k, out_tx).await;
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

    /// Compute the CL-selected remote replica set for a range scan: the ordered
    /// remote `(host_id, addr)` list truncated to the count
    /// [`remote_count_for_cl`] demands beyond the local read.
    fn range_read_remotes(
        &self,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> Vec<(uuid::Uuid, String)> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let local_id = self.local_node_id;
        let node_count = node_ids.len();
        let all_remotes: Vec<(uuid::Uuid, String)> = node_ids
            .iter()
            .filter(|&&id| id != local_id)
            .filter_map(|&id| ring.get_node(id).map(|n| (n.host_id, n.addr.clone())))
            .collect();
        drop(ring);

        let cl_remote_count =
            remote_count_for_cl(cl, replication_factor, node_count, all_remotes.len());
        all_remotes.into_iter().take(cl_remote_count).collect()
    }

    /// Drive the local-start-bounded fragmented stream + the CL-selected remote
    /// replica fragment streams (each start-bounded at its producer) through the
    /// token-aware N-way merge, returning the merged partition stream. Shared by
    /// the paged (`*_stream_from`) variants. `start` is the inclusive resume
    /// cursor (`None` on the first page).
    async fn paged_multi_replica_stream(
        &self,
        table_id: &TableId,
        wanted: Option<Vec<u16>>,
        start: Option<&ferrosa_common::key::DecoratedKey>,
        remotes: &[(uuid::Uuid, String)],
    ) -> crate::error::Result<ClusterPartitionStream> {
        let fanout = self
            .fan_out_remote_fragment_streams(table_id, remotes, wanted.as_deref(), start)
            .await?;
        if fanout.streams.is_empty() {
            return Err(ClusterError::Internal(format!(
                "paged streaming range read: every replica fire failed ({} of {})",
                fanout.fire_failures,
                remotes.len()
            )));
        }
        if fanout.fire_failures > 0 {
            tracing::warn!(
                failed = fanout.fire_failures,
                succeeded = fanout.streams.len(),
                "paged streaming range read: partial fan-out — some replicas could not be reached"
            );
        }

        let (out_tx, out_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
        let storage = self.storage.clone();
        let table_id = table_id.clone();
        let start = start.cloned();
        TaskPool::current("range-read-merge-nway-paged").spawn(async move {
            merge_local_and_remotes_fragmented(
                storage,
                table_id,
                wanted,
                start,
                fanout.streams,
                out_tx,
            )
            .await;
        });
        let stream = futures::stream::unfold(out_rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }

    /// Resume-capable streaming range scan with an inclusive lower-bound key.
    ///
    /// Backs `WritePath::range_read_stream_all_from` (the coordinator-side
    /// paging cursor). The local-only fan-out shape (CL=ONE with the keyspace RF
    /// spanning the ring) streams the local fragmented iterator directly; a
    /// multi-replica shape fans out a start-bounded fragment stream to each
    /// CL-selected replica and merges them with the local stream through the
    /// token-aware N-way fragment merge (`run_fragment_merge_nway`). The
    /// resume cursor (`start`) is shipped to every replica so a resumed page
    /// never re-streams the already-emitted prefix; the CQL paging collector
    /// applies the exact skip-≤-last semantics on top, so the merged page is
    /// gap- and duplicate-free even mid-wide-partition.
    pub async fn coordinate_range_read_stream_from(
        &self,
        table_id: &TableId,
        start: Option<&ferrosa_common::key::DecoratedKey>,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let remotes = self.range_read_remotes(cl, replication_factor);

        if remotes.is_empty() {
            return Ok(Box::pin(
                self.storage
                    .range_iter_fragmented(table_id, start, None)
                    .map(|item| item.map_err(ClusterError::Storage)),
            ));
        }

        self.paged_multi_replica_stream(table_id, None, start, &remotes)
            .await
    }

    /// Projection-aware resume-capable streaming range scan. See
    /// [`Self::coordinate_range_read_stream_from`]; serves the multi-replica
    /// shape via the same start-bounded N-way fragment merge, byte-skipping
    /// unprojected cells at every replica.
    pub async fn coordinate_range_read_projected_stream_from(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        start: Option<&ferrosa_common::key::DecoratedKey>,
        cl: crate::consistency::ConsistencyLevel,
        replication_factor: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let remotes = self.range_read_remotes(cl, replication_factor);

        if remotes.is_empty() {
            return Ok(Box::pin(
                self.storage
                    .range_iter_projected_fragmented(table_id, wanted, start, None)
                    .map(|item| item.map_err(ClusterError::Storage)),
            ));
        }

        self.paged_multi_replica_stream(table_id, Some(wanted), start, &remotes)
            .await
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

        // Multiple remote replicas: token-aware N-way streaming merge of the
        // local fragmented stream + one fragment stream per remote replica.
        // Bounded memory (`O(num_sources + k)`), no materialization.
        //
        // `row_limit == 0` (full scan) forwards the merged fragments directly.
        // `row_limit > 0` (a `LIMIT N` query with partition-key equality
        // predicates, targeting bounded specific partitions) streams a
        // per-partition row cap over the merged output via
        // `apply_per_partition_row_limit` — byte-identical to capping the whole
        // merged partition, but without buffering it.
        if expected_done > 1 {
            let fanout = self
                .fan_out_remote_fragment_streams(
                    table_id,
                    &remotes,
                    projected_regular_ordinals.as_deref(),
                    None,
                )
                .await?;
            if fanout.streams.is_empty() {
                return Err(ClusterError::Internal(format!(
                    "streaming range read: every replica fire failed ({} of {})",
                    fanout.fire_failures, expected_done
                )));
            }
            if fanout.fire_failures > 0 {
                tracing::warn!(
                    failed = fanout.fire_failures,
                    succeeded = fanout.streams.len(),
                    "streaming range read: partial fan-out — some replicas could not be reached"
                );
            }

            // N-way merge -> merge_rx.
            let (merge_tx, merge_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
            let storage = self.storage.clone();
            let merge_table_id = table_id.clone();
            TaskPool::current("range-read-merge-nway").spawn(async move {
                merge_local_and_remotes_fragmented(
                    storage,
                    merge_table_id,
                    projected_regular_ordinals,
                    None,
                    fanout.streams,
                    merge_tx,
                )
                .await;
            });

            // Apply the per-partition row cap as a streaming stage when capped.
            let out_rx = if row_limit > 0 {
                let (cap_tx, cap_rx) = mpsc::channel(STREAM_RECEIVER_BUFFER);
                TaskPool::current("range-read-merge-nway-cap").spawn(async move {
                    apply_per_partition_row_limit(merge_rx, row_limit, cap_tx).await;
                });
                cap_rx
            } else {
                merge_rx
            };
            let stream = futures::stream::unfold(out_rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            return Ok(Box::pin(stream));
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
            start_key: None,
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
        // `limit` is the caller's own bound (a user `LIMIT N`, or the
        // `DEFAULT_RANGE_READ_LIMIT + 1` probe of the truncation-detecting
        // checked reader) — NOT a server-side result cap. Do not re-clamp it to
        // 10_000: a user `LIMIT 20000` must return up to 20000 rows. Memory is
        // bounded by the caller's chosen `limit`. Floor at 1 (a 0-limit bounded
        // read is meaningless).
        let limit = limit.max(1);

        // BOUNDED-STREAMING CONSUME PATH (`t_ee98faa0` / `t_3fc6be3c`).
        //
        // The former shape accumulated EVERY partition from EVERY replica into a
        // `Vec` (the legacy Vec-accumulating consumer's `StreamConsumeOutcome`
        // partitions), then extended a local-read Vec with them and deduped by
        // token — peak `O(result)`, which OOM-killed the coordinator at the
        // intentional node RAM cap.
        //
        // Route through the already-bounded, token-deduped, `<= k`-row-fragment
        // N-way merge stream (`coordinate_range_read_stream_all_with_projection`,
        // the same path the uncapped `SELECT *` uses) and collect AT MOST `limit`
        // whole partitions. Fragments of one partition key arrive consecutively,
        // so we fold them into the current partition and only start a new one on
        // a key change. Peak resident set is `O(k + partitions in flight)` from
        // the bounded merge, plus the caller's OWN `limit` result — never the
        // whole table. `row_limit` (per-partition row cap for PK-equality
        // `LIMIT N` shapes) is applied by the stream itself.
        let mut stream = self
            .coordinate_range_read_stream_all_with_projection(
                table_id,
                row_limit,
                self.default_cl,
                self.default_rf,
                None,
            )
            .await?;

        let mut out: Vec<Partition> = Vec::new();
        while let Some(item) = stream.next().await {
            let fragment = item?;
            match out.last_mut() {
                // Same partition key as the previous fragment: fold its rows in
                // (the header rode the first fragment; later fragments carry
                // `LIVE`/`None`). Folding is always allowed — even at
                // `out.len() == limit` — so the last (limit-th) partition is
                // never truncated mid-partition when its fragments span the
                // `limit` boundary. This reconstructs the whole partition without
                // ever buffering more than the bounded merge already holds.
                Some(last) if last.key == fragment.key => {
                    last.rows.extend(fragment.rows);
                }
                // New partition key. If we already hold `limit` WHOLE
                // partitions, the previous one is complete (a key change proves
                // it), so stop before starting the (limit+1)-th.
                _ => {
                    if out.len() >= limit {
                        break;
                    }
                    out.push(fragment);
                }
            }
        }

        // We stopped at `limit` distinct partitions (or end-of-stream); the
        // merge stream is deduped by token already, so no post-dedup is needed.
        // Dropping `stream` here closes the bounded receiver, which cancels /
        // back-pressures the still-running producers.
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::ConsistencyLevel as CL;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

    #[test]
    fn channel_closed_before_done_is_retryable_read_timeout() {
        // t_4b94ab56: a remote stream closing before Done must surface as a
        // RETRYABLE ReadTimeout (drivers/fmem retry the idempotent scan), not an
        // opaque `internal: ChannelClosedBeforeDone`.
        let e = next_remote_error(StreamConsumeError::ChannelClosedBeforeDone {
            delivered_done: 0,
            expected_done: 1,
        });
        match e {
            Err(ClusterError::ReadTimeout {
                received,
                required,
                data_present,
                ..
            }) => {
                assert_eq!(received, 0);
                assert_eq!(required, 1);
                assert!(!data_present, "incomplete scan must signal retry");
            }
            other => panic!("expected retryable ReadTimeout, got {other:?}"),
        }
    }

    #[test]
    fn idle_timeout_is_retryable_read_timeout() {
        let e = next_remote_error(StreamConsumeError::IdleTimeout {
            request_id: 7,
            idle_timeout: std::time::Duration::from_secs(30),
        });
        assert!(matches!(e, Err(ClusterError::ReadTimeout { .. })));
    }

    #[test]
    fn decode_fault_stays_internal_non_retryable() {
        // A genuine protocol/decode bug must stay loud and non-retryable.
        let e = next_remote_error(StreamConsumeError::Decode {
            request_id: 1,
            which: "RangeReadStreamChunk",
            message: "boom".to_string(),
        });
        assert!(matches!(e, Err(ClusterError::Internal(_))));
    }

    #[tokio::test]
    async fn truncated_remote_done_is_a_stream_error() {
        let (frame_tx, frame_rx) = mpsc::channel(4);
        let (partition_tx, _partition_rx) = mpsc::channel(4);
        let done = RangeReadStreamDonePayload {
            request_id: 42,
            total_chunks: 0,
            truncated: true,
        };
        frame_tx
            .send(Message::RangeReadStreamDone(bytes::Bytes::from(
                bincode::serialize(&done).unwrap(),
            )))
            .await
            .unwrap();
        drop(frame_tx);

        let result = forward_remote_range_stream_inner(frame_rx, 42, 1, partition_tx).await;
        assert!(
            result.is_err(),
            "truncated Done must fail the remote stream instead of being accepted as success"
        );
    }

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

    /// Drive an N-way fragment merge over several in-memory fragment streams
    /// and collect the emitted fragments. Mirrors `drive_merge` for the 2-way
    /// case; the N-way merge core is `run_fragment_merge_nway`.
    async fn drive_merge_nway(sources: Vec<Vec<Partition>>, k: usize) -> Vec<Partition> {
        let (tx, mut rx) = mpsc::channel(1024);
        let cursors: Vec<FragmentCursor<_>> = sources
            .into_iter()
            .map(|s| FragmentCursor::new(stream_of(s)))
            .collect();
        let driver = tokio::spawn(async move { run_fragment_merge_nway(cursors, k, tx).await });
        let mut out = Vec::new();
        while let Some(item) = rx.recv().await {
            out.push(item.expect("nway merge produced an error"));
        }
        driver.await.unwrap();
        out
    }

    /// The N-way (multi-replica) fragment merge must reproduce, per token, the
    /// whole-partition `merge_partitions(vec![s0, s1, s2])` result —
    /// byte-identical after flattening — for LWW across THREE replicas,
    /// tombstones, static rows, and disjoint tokens, across several `k`, while
    /// no emitted fragment exceeds `k` rows (bounded memory across all sources).
    #[tokio::test]
    async fn fragment_nway_replica_merge_equiv_whole_partition_merge() {
        let mut keys = [dk(b"alpha"), dk(b"bravo"), dk(b"charlie"), dk(b"delta")];
        keys.sort();

        // key0: 3-way LWW — s2 newest wins on its keys, s1 beats s0 on overlap.
        let s0k0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..30)
                .map(|c| trow(c, format!("a{c}").as_bytes(), 1000))
                .collect(),
        };
        let s1k0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..30)
                .filter(|c| c % 2 == 0)
                .map(|c| trow(c, format!("b{c}").as_bytes(), 2000))
                .collect(),
        };
        let s2k0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..30)
                .filter(|c| c % 3 == 0)
                .map(|c| trow(c, format!("c{c}").as_bytes(), 3000))
                .collect(),
        };
        // key1: only s1 has it (disjoint).
        let s1k1 = Partition {
            key: keys[1].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..9)
                .map(|c| trow(c, format!("d{c}").as_bytes(), 1500))
                .collect(),
        };
        // key2: s0 + s2, with a partition tombstone on s2 suppressing s0's old rows.
        let s0k2 = Partition {
            key: keys[2].clone(),
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
        let s2k2 = Partition {
            key: keys[2].clone(),
            deletion: DeletionTime::new(1500, 100),
            static_row: None,
            rows: (20..36)
                .map(|c| trow(c, format!("n{c}").as_bytes(), 3000))
                .collect(),
        };

        let reference = {
            let mut r = vec![
                ferrosa_storage::merge::merge_partitions(vec![
                    s0k0.clone(),
                    s1k0.clone(),
                    s2k0.clone(),
                ]),
                s1k1.clone(),
                ferrosa_storage::merge::merge_partitions(vec![s0k2.clone(), s2k2.clone()]),
            ];
            r.sort_by(|a, b| a.key.cmp(&b.key));
            r
        };

        let mut s0 = [s0k0, s0k2];
        s0.sort_by(|a, b| a.key.cmp(&b.key));
        let mut s1 = [s1k0, s1k1];
        s1.sort_by(|a, b| a.key.cmp(&b.key));
        let s2 = [s2k0, s2k2]; // already key-sorted (keys[0] < keys[2])

        for k in [1usize, 2, 3, 7, 8, 1024] {
            let frag = |ps: &[Partition]| -> Vec<Partition> {
                ps.iter().flat_map(|p| fragment_partition(p, k)).collect()
            };
            let emitted = drive_merge_nway(vec![frag(&s0), frag(&s1), frag(&s2)], k).await;
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
                "N-way fragment merge(k={k}) diverged from whole-partition merge"
            );
        }
    }

    /// A merged row, flattened to (decorated_key, clustering, row) so the paging
    /// simulation can compare against the whole-merge reference and apply the
    /// skip-≤-last cursor in true (token, clustering) order, exactly as the CQL
    /// paging collector does.
    fn flatten_to_rows(parts: &[Partition]) -> Vec<(DecoratedKey, Vec<u8>, Row)> {
        let mut out = Vec::new();
        for p in parts {
            for r in &p.rows {
                out.push((p.key.clone(), r.clustering.clone(), r.clone()));
            }
        }
        out
    }

    /// Start-bound a whole partition source at the inclusive resume cursor,
    /// mirroring the producer-side `range_iter_fragmented(start, ..)` (which is
    /// bounded in TOKEN order, since `start` is a `DecoratedKey`): keep
    /// partitions with key >= cursor key in token order; in the cursor's own
    /// partition keep only rows with clustering > cursor.ck.
    fn resume_source(
        src: &[Partition],
        cursor: &Option<(DecoratedKey, Vec<u8>)>,
    ) -> Vec<Partition> {
        let Some((ckey, ck)) = cursor else {
            return src.to_vec();
        };
        let mut out = Vec::new();
        for p in src {
            if p.key < *ckey {
                continue;
            }
            let mut p2 = p.clone();
            if p.key == *ckey {
                // Inclusive re-seek lands on the cursor partition; drop rows
                // already past the cursor's clustering.
                p2.rows.retain(|r| r.clustering > *ck);
            }
            out.push(p2);
        }
        out
    }

    /// Paged N-way traversal must equal the whole N-way merge — no gaps, no
    /// duplicates — including a wide partition replicated across ALL sources and
    /// spanning multiple pages. The simulation drives the real
    /// `run_fragment_merge_nway` once per page over start-bounded sources, takes
    /// `page_size` merged rows, advances an inclusive (partition_key, clustering)
    /// cursor, and concatenates the pages. This is exactly the resume contract
    /// the CQL paging collector applies on top of the coordinator stream.
    #[tokio::test]
    async fn paged_nway_traversal_equals_whole_nway_merge() {
        let mut keys = [dk(b"alpha"), dk(b"bravo"), dk(b"charlie"), dk(b"delta")];
        keys.sort();

        // keys[0]: WIDE partition replicated across all 3 sources, with 3-way
        // LWW overlap, so a page boundary can fall mid-partition.
        let wide = |tag: char, modulo: i32, ts: i64| Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..50)
                .filter(|c| c % modulo == 0)
                .map(|c| trow(c, format!("{tag}{c}").as_bytes(), ts))
                .collect(),
        };
        let s0w = wide('a', 1, 1000); // every clustering
        let s1w = wide('b', 2, 2000); // evens, newer
        let s2w = wide('c', 3, 3000); // multiples of 3, newest

        // keys[1]: only s1. keys[2]: s0 + s2 with a partition tombstone on s2.
        let s1k1 = Partition {
            key: keys[1].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..9)
                .map(|c| trow(c, format!("d{c}").as_bytes(), 1500))
                .collect(),
        };
        let s0k2 = Partition {
            key: keys[2].clone(),
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
        let s2k2 = Partition {
            key: keys[2].clone(),
            deletion: DeletionTime::new(1500, 100),
            static_row: None,
            rows: (20..36)
                .map(|c| trow(c, format!("n{c}").as_bytes(), 3000))
                .collect(),
        };

        let mut s0 = vec![s0w.clone(), s0k2.clone()];
        s0.sort_by(|a, b| a.key.cmp(&b.key));
        let mut s1 = vec![s1w.clone(), s1k1.clone()];
        s1.sort_by(|a, b| a.key.cmp(&b.key));
        let s2 = vec![s2w.clone(), s2k2.clone()];

        // Whole-merge reference: rows in (token, clustering) order, post-LWW.
        let k_ref = 8usize;
        let whole = {
            let frag = |ps: &[Partition]| -> Vec<Partition> {
                ps.iter()
                    .flat_map(|p| fragment_partition(p, k_ref))
                    .collect()
            };
            let emitted = drive_merge_nway(vec![frag(&s0), frag(&s1), frag(&s2)], k_ref).await;
            flatten_to_rows(&flatten(emitted))
        };

        // Page across several page sizes and fragment caps. The fragment cap k
        // is independent of page_size — both must be respected.
        for page_size in [1usize, 3, 7, 50] {
            for k in [1usize, 4, 8] {
                let mut cursor: Option<(DecoratedKey, Vec<u8>)> = None;
                let mut paged: Vec<(DecoratedKey, Vec<u8>, Row)> = Vec::new();

                // Hard iteration cap: at most one page per reference row, plus a
                // final empty page. Guards against a paging bug looping forever.
                let max_pages = whole.len() + 2;
                let mut pages = 0usize;
                loop {
                    pages += 1;
                    assert!(pages <= max_pages, "paging did not terminate");

                    let frag = |ps: &[Partition]| -> Vec<Partition> {
                        resume_source(ps, &cursor)
                            .iter()
                            .flat_map(|p| fragment_partition(p, k))
                            .collect()
                    };
                    let emitted = drive_merge_nway(vec![frag(&s0), frag(&s1), frag(&s2)], k).await;
                    for f in &emitted {
                        assert!(f.rows.len() <= k, "fragment {} exceeds k={k}", f.rows.len());
                    }
                    let merged = flatten_to_rows(&flatten(emitted));

                    // Skip rows already emitted (<= cursor) in (token,
                    // clustering) order, then take a page.
                    let mut took = 0usize;
                    for (pk, ck, row) in merged.into_iter() {
                        if let Some((cpk, cck)) = cursor.as_ref() {
                            if (&pk, &ck) <= (cpk, cck) {
                                continue;
                            }
                        }
                        paged.push((pk.clone(), ck.clone(), row));
                        cursor = Some((pk, ck));
                        took += 1;
                        if took == page_size {
                            break;
                        }
                    }
                    if took == 0 {
                        break; // drained
                    }
                }

                assert_eq!(
                    paged, whole,
                    "paged N-way traversal (page_size={page_size}, k={k}) diverged from whole merge"
                );
            }
        }
    }

    /// Drive an N-way merge then the streaming per-partition row cap, collecting
    /// the capped fragments — mirrors the coordinator's capped multi-replica path.
    async fn drive_merge_nway_capped(
        sources: Vec<Vec<Partition>>,
        k: usize,
        row_limit: usize,
    ) -> Vec<Partition> {
        let (mid_tx, mid_rx) = mpsc::channel(1024);
        let cursors: Vec<FragmentCursor<_>> = sources
            .into_iter()
            .map(|s| FragmentCursor::new(stream_of(s)))
            .collect();
        let driver = tokio::spawn(async move { run_fragment_merge_nway(cursors, k, mid_tx).await });
        let (out_tx, mut out_rx) = mpsc::channel(1024);
        let capper =
            tokio::spawn(
                async move { apply_per_partition_row_limit(mid_rx, row_limit, out_tx).await },
            );
        let mut out = Vec::new();
        while let Some(item) = out_rx.recv().await {
            out.push(item.expect("capped nway merge produced an error"));
        }
        driver.await.unwrap();
        capper.await.unwrap();
        out
    }

    /// Capped (`LIMIT N` + PK-equality) multi-replica scans: the streaming
    /// per-partition row cap over the N-way merge must be byte-identical to
    /// `apply_row_limit` on the whole merged partition — for several `k` and
    /// `row_limit`, preserving static rows / partition headers, and never
    /// emitting more than `row_limit` rows for any partition.
    #[tokio::test]
    async fn nway_capped_per_partition_row_limit_equiv_whole_capped() {
        let mut keys = [dk(b"alpha"), dk(b"bravo"), dk(b"charlie")];
        keys.sort();

        // key0: 2-source overlap (wide). key1: s1 only. key2: s0 only, static row.
        let s0k0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..40)
                .map(|c| trow(c, format!("a{c}").as_bytes(), 1000))
                .collect(),
        };
        let s1k0 = Partition {
            key: keys[0].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..40)
                .filter(|c| c % 2 == 0)
                .map(|c| trow(c, format!("b{c}").as_bytes(), 2000))
                .collect(),
        };
        let s1k1 = Partition {
            key: keys[1].clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..15)
                .map(|c| trow(c, format!("d{c}").as_bytes(), 1500))
                .collect(),
        };
        let s0k2 = Partition {
            key: keys[2].clone(),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![(0, ferrosa_common::CellValue::live(b"st".to_vec(), 9000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: (0..10)
                .map(|c| trow(c, format!("o{c}").as_bytes(), 1000))
                .collect(),
        };

        let whole = {
            let mut r = vec![
                ferrosa_storage::merge::merge_partitions(vec![s0k0.clone(), s1k0.clone()]),
                s1k1.clone(),
                s0k2.clone(),
            ];
            r.sort_by(|a, b| a.key.cmp(&b.key));
            r
        };
        let mut s0 = [s0k0, s0k2];
        s0.sort_by(|a, b| a.key.cmp(&b.key));
        let s1 = [s1k0, s1k1]; // keys[0] < keys[1]

        for k in [1usize, 3, 8] {
            for limit in [1usize, 5, 1000] {
                let reference: Vec<Partition> = whole
                    .iter()
                    .cloned()
                    .map(|p| apply_row_limit(p, limit))
                    .collect();
                let frag = |ps: &[Partition]| -> Vec<Partition> {
                    ps.iter().flat_map(|p| fragment_partition(p, k)).collect()
                };
                let capped = drive_merge_nway_capped(vec![frag(&s0), frag(&s1)], k, limit).await;
                let merged = flatten(capped);
                for p in &merged {
                    assert!(
                        p.rows.len() <= limit,
                        "capped partition has {} rows > limit {limit}",
                        p.rows.len()
                    );
                }
                assert_eq!(
                    merged, reference,
                    "capped N-way merge(k={k},limit={limit}) diverged from whole-then-cap"
                );
            }
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

    /// The `..._limited_rows` (Vec-returning) coordinator MUST NOT accumulate
    /// every partition from every replica via the Vec-accumulating
    /// `consume_range_stream` before returning (that was the `O(result)` OOM,
    /// `t_ee98faa0` / `t_3fc6be3c`). It must drink the bounded, token-deduped
    /// N-way merge stream and collect only up to the caller's own `limit`.
    #[test]
    fn coordinate_streaming_limited_rows_does_not_accumulate_via_consume_range_stream() {
        let source = include_str!("range_read_stream.rs");
        let raw_body = source
            .split("pub async fn coordinate_range_read_stream_limited_rows")
            .nth(1)
            .and_then(|rest| rest.split("#[cfg(test)]").next())
            .expect("streaming coordinator body must be present");
        // Strip `//` line comments so a doc/inline comment that *names* the old
        // anti-pattern (to explain what was removed) does not trip the CODE
        // invariants below.
        let body: String = raw_body
            .lines()
            .map(|l| match l.find("//") {
                Some(i) => &l[..i],
                None => l,
            })
            .collect::<Vec<_>>()
            .join("\n");
        let body = body.as_str();

        assert!(
            !body.contains("consume_range_stream("),
            "the limited-rows coordinator must NOT call the Vec-accumulating consume_range_stream \
             (that accumulates O(result) partitions before returning): {body}"
        );
        assert!(
            !body.contains("all_partitions.extend"),
            "the limited-rows coordinator must NOT accumulate local + remote partitions in a Vec \
             before returning — it must stream through the bounded merge: {body}"
        );
        assert!(
            body.contains("coordinate_range_read_stream_all_with_projection"),
            "the limited-rows coordinator must route through the bounded, token-deduped N-way \
             merge stream, then collect at most `limit` partitions: {body}"
        );
        assert!(
            body.contains("stream.next().await"),
            "the limited-rows coordinator must pull the merge stream one partition at a time \
             (bounded resident set), not materialize it: {body}"
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
            body.contains("run_fragment_merge_nway")
                || body.contains("merge_local_and_remotes_fragmented"),
            "multi-replica unbounded scans must stream through the token-aware N-way fragment merge, not materialize"
        );
        assert!(
            body.contains("apply_per_partition_row_limit"),
            "capped (row_limit>0) multi-replica scans must stream a per-partition row cap over the N-way merge (no longer a loud refusal)"
        );
    }
}
