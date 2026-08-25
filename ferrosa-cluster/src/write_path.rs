//! Write path abstraction for runtime mode transitions.
//!
//! The CQL router calls `WritePath::write()` for all DML mutations. The
//! active implementation is swapped atomically via `ArcSwap` when the
//! deployment mode changes (standalone → pair → cluster).
//!
//! - `WritePath::Direct` — standalone mode, writes directly to `StorageEngine`.
//! - `WritePath::Pair` — pair mode, delegates to `PairCoordinator::coordinate_write()`.

use std::pin::Pin;
use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{Mutation, TableId};
use futures::{Stream, StreamExt};

use crate::consistency::ConsistencyLevel;
use crate::coordinator::ClusterCoordinator;
use crate::error::ClusterError;
use crate::pair::coordinator::PairCoordinator;
use crate::ring::strategy::ReplicationStrategy;

/// Default partition bound for the two range-read shapes that still need a
/// hard bound — NOT a result cap on the streamable shapes.
///
/// After the streaming-range-reads work (spec: `streaming-range-reads-no-cap`),
/// the O(1)-streamable shapes (simple `WHERE … ALLOW FILTERING` scans, streaming
/// scalar aggregates, `SELECT DISTINCT <partition key>`, and any user `LIMIT N`)
/// are bounded ONLY by the query's own `LIMIT` — never by this constant. The
/// two remaining users of this bound are:
///
/// 1. `range_read_limited_rows_checked` — the truncation-detecting probe for the
///    still-accumulating complex shapes (`ORDER BY` global sort / function
///    projection) that must fail loud rather than compute a wrong answer over a
///    clipped window, until spill-to-disk lands (spec step 5).
/// 2. The legacy non-streaming coordinated range RPC selected by
///    `FERROSA_BULK_STREAMING_RANGE_READ=0` (a documented, degraded
///    mixed-version-upgrade opt-out), where it caps per-replica partitions.
///
/// It is NOT applied as a result cap on `range_read_limited_rows` (a user/page
/// bound) nor on the streaming `*_stream_all_*` scans.
pub const DEFAULT_RANGE_READ_LIMIT: usize = 10_000;

pub type PartitionResultStream =
    Pin<Box<dyn Stream<Item = crate::error::Result<Partition>> + Send>>;

/// Resume position for a paged streaming scan (t_a0f922a3).
///
/// `key` is the partition the previous page stopped in (INCLUSIVE lower
/// bound). `clustering`, when `Some`, is the raw clustering bytes of the last
/// row already delivered in that partition: every producer (local iterator
/// wrapper and each remote replica) drops rows of `key`'s partition whose
/// clustering is `<=` it, so a resumed page of a WIDE partition re-streams
/// neither over the wire nor into the merge. The CQL paging collector still
/// applies the same skip on top (idempotent — both sides compare the same raw
/// clustering bytes), so an off-by-one can never drop or duplicate rows.
#[derive(Debug, Clone)]
pub struct ScanResume {
    pub key: DecoratedKey,
    pub clustering: Option<Vec<u8>>,
}

/// Apply the within-partition resume filter to one streamed fragment: drop
/// every row of the resume partition whose clustering bytes are `<=` the
/// resume clustering. Fragments of other partitions pass through untouched.
/// Returns `None` when the filtered fragment carries neither rows nor header
/// state (nothing left to deliver).
pub fn filter_resumed_fragment(
    mut partition: Partition,
    resume_key: &[u8],
    resume_clustering: &[u8],
) -> Option<Partition> {
    if partition.key.key.as_bytes() != resume_key {
        return Some(partition);
    }
    partition
        .rows
        .retain(|row| row.clustering.as_slice() > resume_clustering);
    let carries_header = partition.static_row.is_some()
        || partition.deletion != ferrosa_sstable::types::DeletionTime::LIVE;
    if partition.rows.is_empty() && !carries_header {
        return None;
    }
    Some(partition)
}

/// Wrap a fragmented partition stream with the within-partition resume skip
/// (see [`filter_resumed_fragment`]). No-op when the resume carries no
/// clustering position.
///
/// FAIL LOUD (t_a0f922a3): the resume skip drops rows `<= resume_ck` as the
/// already-delivered prefix. That is correct ONLY if the fragment stream is
/// monotonically ascending in raw clustering. A legacy/corrupt or mis-sorted
/// SSTable emits a wide partition as two concatenated ascending runs; the
/// second run restarts BELOW `resume_ck`, so this filter would SILENTLY drop
/// every one of its rows — the paged scan under-delivers ~half the partition
/// with no error (the exact typed_edges symptom). The storage-side guard in
/// `range_merger::emit_fragment` only fires on a full read that reaches the
/// inversion; a paged scan terminates before reaching it, so the drop stays
/// silent. We therefore detect the non-monotonic delivery HERE, where the drop
/// happens, and surface it as a loud error (compact the table to fix the
/// on-disk order) instead of returning a silent partial. Monotonic streams
/// never trip this, so it is inert in steady state.
pub(crate) fn resume_filtered_stream(
    stream: PartitionResultStream,
    resume: Option<&ScanResume>,
) -> PartitionResultStream {
    let Some(resume) = resume else { return stream };
    let Some(clustering) = resume.clustering.clone() else {
        return stream;
    };
    let key = resume.key.key.as_bytes().to_vec();
    // Last raw clustering seen for the resume partition across fragments; used
    // to detect a regression (non-monotonic delivery) before the resume filter
    // silently drops the regressed run.
    let mut last_clustering: Option<Vec<u8>> = None;
    let checked = stream.map(move |item| {
        let p = item?;
        if p.key.key.as_bytes() == key {
            for row in &p.rows {
                if let Some(ref last) = last_clustering {
                    if row.clustering.as_slice() < last.as_slice() {
                        let hx = |b: &[u8]| {
                            b[..b.len().min(24)]
                                .iter()
                                .map(|x| format!("{x:02x}"))
                                .collect::<String>()
                        };
                        return Err(ClusterError::Storage(ferrosa_common::Error::InvalidData(
                            format!(
                                "resumed range scan received a non-monotonic fragment for the \
                                 resume partition (prev={} > next={}). The SSTable stores this \
                                 partition's rows out of clustering order (legacy or corrupt \
                                 file); the resume filter would silently drop the regressed run \
                                 and under-deliver the page. Compact this table to rewrite the \
                                 SSTable in sorted order.",
                                hx(last),
                                hx(&row.clustering),
                            ),
                        )));
                    }
                }
                last_clustering = Some(row.clustering.clone());
            }
        }
        Ok(filter_resumed_fragment(p, &key, &clustering))
    });
    Box::pin(checked.filter_map(|res| async move {
        match res {
            Ok(Some(p)) => Some(Ok(p)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }))
}

fn local_range_stream(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    row_limit: usize,
) -> PartitionResultStream {
    let stream = engine.range_iter(table_id, None, None).map(move |item| {
        let mut partition = item.map_err(crate::error::ClusterError::Storage)?;
        if row_limit > 0 {
            partition.rows.truncate(row_limit);
        }
        Ok(partition)
    });
    Box::pin(stream)
}

/// Fragmented (intra-partition streaming) range stream with an optional
/// resume position. Used by the coordinator-side paging cursor to resume an
/// unbounded `SELECT *` scan at the last partition key (and, for a wide
/// partition, the last delivered clustering position) without materializing
/// the whole table. `row_limit == 0` (the unbounded scan shape) is the only
/// caller, so wide partitions stream as bounded fragments.
fn local_range_stream_from(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    resume: Option<&ScanResume>,
) -> PartitionResultStream {
    let stream = engine
        .range_iter_fragmented(table_id, resume.map(|r| &r.key), None)
        .map(|item| item.map_err(crate::error::ClusterError::Storage));
    resume_filtered_stream(Box::pin(stream), resume)
}

fn local_projected_range_stream_from(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    wanted: Vec<u16>,
    resume: Option<&ScanResume>,
) -> PartitionResultStream {
    let stream = engine
        .range_iter_projected_fragmented(table_id, wanted, resume.map(|r| &r.key), None)
        .map(|item| item.map_err(crate::error::ClusterError::Storage));
    resume_filtered_stream(Box::pin(stream), resume)
}

fn local_projected_range_stream(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    wanted: Vec<u16>,
    partition_limit: Option<usize>,
) -> PartitionResultStream {
    let stream = engine
        .range_iter_projected(table_id, wanted, partition_limit, None, None)
        .map(|item| item.map_err(crate::error::ClusterError::Storage));
    Box::pin(stream)
}

fn cluster_error_to_common(err: ClusterError) -> ferrosa_common::Error {
    match err {
        ClusterError::Overloaded(msg) => {
            ferrosa_common::Error::InvalidData(format!("overloaded: {msg}"))
        }
        other => ferrosa_common::Error::InvalidData(format!("cluster: {other}")),
    }
}

/// Build a `CommittedToCluster` CDC event for a regular-CL write, if the
/// engine's shared CDC bus has a committed-stream subscriber.
///
/// Coordinator-side: a regular-CL write reaches "committed to cluster" when the
/// coordinator achieves its consistency level (the caller publishes only after
/// the write returns `Ok`). Ordered by the write `timestamp` — there is no
/// Accord timestamp for non-Accord writes (decision O-8). Returns the bus +
/// event so the row is cloned only when a subscriber is actually listening; the
/// caller publishes after success. `None` (no allocation) otherwise.
fn committed_cdc_event(
    storage: &StorageEngine,
    table_id: &TableId,
    key: &DecoratedKey,
    rows: &[Row],
    timestamp: i64,
) -> Option<(Arc<ferrosa_cdc::CdcBus>, ferrosa_cdc::CdcEvent)> {
    let bus = storage.cdc_bus()?;
    if !bus.has_subscribers(ferrosa_cdc::CdcStream::CommittedToCluster) {
        return None;
    }
    let event = ferrosa_cdc::CdcEvent {
        stream: ferrosa_cdc::CdcStream::CommittedToCluster,
        keyspace: table_id.keyspace.clone(),
        table: table_id.table.clone(),
        key: key.clone(),
        rows: rows.to_vec(),
        timestamp,
        accord_ts: None,
        mutation_id: uuid::Uuid::new_v4().into_bytes(),
    };
    Some((bus, event))
}

/// Run a node-local full-text index scan on a blocking thread.
///
/// [`StorageEngine::fulltext_search`] is synchronous and blocking (directory
/// enumeration + sequential sidecar walks + whole-file reads). Invoking it
/// inline on an async worker starves that worker's other futures — raft
/// heartbeats and CQL keepalives — which was a co-cause of the Bulk-lane
/// timeout → retry storm during the OOM cascade (t_8fc24ce2; same class as the
/// PR #131 range-read offload). Offloading it keeps the async runtime
/// responsive while the scan runs.
async fn offloaded_fulltext_search(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    index_name: &str,
    query: &str,
    limit: Option<usize>,
) -> crate::error::Result<Vec<Vec<u8>>> {
    let table_id = table_id.clone();
    let index_name = index_name.to_string();
    let query = query.to_string();
    tokio::task::spawn_blocking(move || {
        engine.fulltext_search(&table_id, &index_name, &query, limit)
    })
    .await
    .map_err(|e| crate::error::ClusterError::Internal(format!("fulltext_search task join: {e}")))?
    .map_err(crate::error::ClusterError::Storage)
}

/// Node-local streaming fulltext walk for Direct/Pair modes: the blocking
/// `fulltext_search_each` walk feeds bounded key batches into the returned
/// channel; the walk's own working set is O(1) (t_4ae47a9f layer 2b), so
/// resident memory is O(channel + one batch) regardless of match count.
/// Dropping the receiver stops the walk on its next key (Break).
fn local_fulltext_key_stream(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    index_name: &str,
    query: &str,
) -> tokio::sync::mpsc::Receiver<crate::error::Result<Vec<Vec<u8>>>> {
    const BATCH_KEYS: usize = 4_096;
    let (tx, rx) = tokio::sync::mpsc::channel::<crate::error::Result<Vec<Vec<u8>>>>(8);
    let table_id = table_id.clone();
    let index_name = index_name.to_string();
    let query = query.to_string();
    tokio::task::spawn_blocking(move || {
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(BATCH_KEYS);
        let result = engine.fulltext_search_each(&table_id, &index_name, &query, &mut |key| {
            batch.push(key);
            if batch.len() >= BATCH_KEYS {
                let full = std::mem::take(&mut batch);
                if tx.blocking_send(Ok(full)).is_err() {
                    return std::ops::ControlFlow::Break(());
                }
            }
            std::ops::ControlFlow::Continue(())
        });
        match result {
            Ok(()) => {
                if !batch.is_empty() {
                    let _ = tx.blocking_send(Ok(batch));
                }
            }
            Err(e) => {
                let _ = tx.blocking_send(Err(crate::error::ClusterError::Storage(e)));
            }
        }
    });
    rx
}

/// The active write path. Swapped atomically via `ArcSwap` when the
/// deployment mode changes (standalone → pair → cluster).
///
/// Uses enum dispatch instead of trait objects so that `ArcSwap` works
/// (trait objects are `!Sized` and `ArcSwap` requires `Sized`).
pub enum WritePath {
    /// Standalone mode: writes directly to StorageEngine.
    Direct(Arc<StorageEngine>),
    /// Pair mode: delegates to PairCoordinator.
    Pair(Arc<PairCoordinator>),
    /// Cluster mode: delegates to ClusterCoordinator with CL enforcement.
    Cluster(Arc<ClusterCoordinator>),
    /// Degraded pair: peer lost, writes rejected, **local reads preserved**.
    ///
    /// The `PairCoordinator` is retained so `local_storage()` can serve
    /// stale reads of replicated data while writes remain rejected until
    /// an operator promotes the node.
    DegradedPair(Arc<PairCoordinator>),
    /// Fully unavailable (used in tests / edge cases).
    Unavailable,
}

impl WritePath {
    /// Create a standalone write path.
    pub fn direct(engine: Arc<StorageEngine>) -> Self {
        Self::Direct(engine)
    }

    /// Create a pair mode write path.
    pub fn pair(coordinator: Arc<PairCoordinator>) -> Self {
        Self::Pair(coordinator)
    }

    /// Create a degraded pair write path.
    pub fn degraded_pair(coordinator: Arc<PairCoordinator>) -> Self {
        Self::DegradedPair(coordinator)
    }

    /// Create a cluster mode write path.
    pub fn cluster(coordinator: Arc<ClusterCoordinator>) -> Self {
        Self::Cluster(coordinator)
    }

    /// Create an unavailable write path (degraded pair mode).
    pub fn unavailable() -> Self {
        Self::Unavailable
    }

    /// Write a logged batch atomically. In standalone mode this goes
    /// through `StorageEngine::write_atomic_batch()`. In pair mode each
    /// mutation is forwarded individually (atomic guarantee comes from
    /// the batchlog). In cluster mode the `ClusterCoordinator` handles
    /// the 3-phase batchlog protocol.
    pub async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        _cl: ConsistencyLevel,
        _rf: usize,
    ) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.write_atomic_batch(mutations),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::DegradedPair(_) => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                // Pair mode: forward each mutation individually.
                for m in mutations {
                    coordinator
                        .coordinate_write(&m)
                        .await
                        .map_err(|e| ferrosa_common::Error::InvalidData(format!("pair: {e}")))?;
                    if let Some((bus, ev)) = committed_cdc_event(
                        coordinator.local_storage(),
                        &TableId::new(&m.keyspace, &m.table),
                        &m.key,
                        &m.rows,
                        m.timestamp,
                    ) {
                        bus.publish(ev);
                    }
                }
                Ok(())
            }
            Self::Cluster(coordinator) => {
                // Capture committed-CDC events before `mutations` is moved into
                // the batch (clones only when a committed subscriber exists).
                let pending: Vec<_> = mutations
                    .iter()
                    .filter_map(|m| {
                        committed_cdc_event(
                            &coordinator.storage,
                            &TableId::new(&m.keyspace, &m.table),
                            &m.key,
                            &m.rows,
                            m.timestamp,
                        )
                    })
                    .collect();
                let res = coordinator
                    .coordinate_logged_batch(mutations)
                    .await
                    .map_err(cluster_error_to_common);
                if res.is_ok() {
                    for (bus, ev) in pending {
                        bus.publish(ev);
                    }
                }
                res
            }
        }
    }

    /// Read a single partition by key with CL enforcement.
    ///
    /// - `Direct` / `Pair`: reads from local storage (single-node case).
    /// - `Cluster`: routes through ClusterCoordinator with digest protocol.
    /// - `Unavailable`: returns an error.
    pub async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        self.pk_read_limited_rows(table_id, key, cl, strategy, 0)
            .await
    }

    /// Read a single partition by key with CL enforcement, optionally
    /// retaining only the first `row_limit` clustered rows.
    pub async fn pk_read_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
        row_limit: usize,
    ) -> ferrosa_common::Result<Option<Partition>> {
        match self {
            Self::Direct(engine) => engine.read_limited_rows(table_id, key, row_limit),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .read_limited_rows(table_id, key, row_limit),
            Self::Cluster(coordinator) => {
                let rows_opt = match strategy {
                    ReplicationStrategy::Simple { replication_factor } => {
                        coordinator
                            .coordinate_read_with_limited_rows(
                                table_id,
                                key,
                                cl,
                                *replication_factor,
                                row_limit,
                            )
                            .await
                    }
                    ReplicationStrategy::NetworkTopology { .. } => {
                        coordinator
                            .coordinate_read_nts_limited_rows(
                                table_id, key, cl, strategy, row_limit,
                            )
                            .await
                    }
                };
                match rows_opt {
                    Ok(Some(rows)) if !rows.is_empty() => Ok(Some(Partition {
                        key: key.clone(),
                        deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                        static_row: None,
                        rows,
                    })),
                    Ok(_) => Ok(None),
                    Err(e) => Err(ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
                }
            }
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, reads rejected until operator promotes".into(),
            )),
        }
    }

    /// Read exactly one clustered row by full primary key with CL enforcement.
    pub async fn pk_read_clustering_row(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        clustering: &[u8],
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        match self {
            Self::Direct(engine) => engine.read_clustering_row(table_id, key, clustering),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .read_clustering_row(table_id, key, clustering),
            Self::Cluster(coordinator) => {
                let rows_opt = match strategy {
                    ReplicationStrategy::Simple { replication_factor } => {
                        coordinator
                            .coordinate_read_clustering_row(
                                table_id,
                                key,
                                clustering,
                                cl,
                                *replication_factor,
                            )
                            .await
                    }
                    ReplicationStrategy::NetworkTopology { .. } => {
                        coordinator
                            .coordinate_read_nts_clustering_row(
                                table_id, key, clustering, cl, strategy,
                            )
                            .await
                    }
                };
                match rows_opt {
                    Ok(Some(rows)) if !rows.is_empty() => Ok(Some(Partition {
                        key: key.clone(),
                        deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                        static_row: None,
                        rows,
                    })),
                    Ok(_) => Ok(None),
                    Err(e) => Err(ferrosa_common::Error::InvalidData(format!("cluster: {e}"))),
                }
            }
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, reads rejected until operator promotes".into(),
            )),
        }
    }

    /// Read a single partition by key, routing to the correct replica.
    ///
    /// - `Direct` / `Pair`: reads from local storage (single-node).
    /// - `Cluster`: routes through ClusterCoordinator to the correct replica.
    /// - `Unavailable`: returns None.
    pub async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        match self {
            Self::Direct(engine) => engine.read(table_id, key),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => {
                coordinator.local_storage().read(table_id, key)
            }
            Self::Cluster(coordinator) => match coordinator.coordinate_read(table_id, key).await {
                Ok(Some(rows)) => Ok(Some(Partition {
                    key: key.clone(),
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    static_row: None,
                    rows,
                })),
                Ok(None) => Ok(None),
                Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                    "coordinate_read: {e}"
                ))),
            },
            Self::Unavailable => Ok(None),
        }
    }

    /// Scatter a full-table range read to all nodes that hold data for
    /// `table_id` and return the deduplicated union of all partitions.
    ///
    /// - `Direct` / `Pair`: reads from local storage only (single-node case).
    /// - `Cluster`: fans out to every ring node and merges results.
    /// - `Unavailable`: returns error (degraded mode).
    ///
    /// Errors are propagated — callers MUST handle them. Silently returning
    /// empty results on failure causes data loss (see BUG: large-write-causes-
    /// data-loss-in-partition).
    pub async fn range_read(&self, table_id: &TableId) -> crate::error::Result<Vec<Partition>> {
        self.range_read_with(
            table_id,
            ConsistencyLevel::One,
            &ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await
    }

    /// Scatter a full-table range read with caller consistency and keyspace
    /// replication strategy, then collect the streamed partitions.
    pub async fn range_read_with(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<Vec<Partition>> {
        let mut stream = self
            .range_read_stream_all_with(table_id, 0, cl, strategy)
            .await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Stream every partition for full-scan consumers.
    ///
    /// This is the unbounded counterpart to `range_read_limited_rows`: callers
    /// pull one partition at a time and can cancel once protocol LIMIT/page
    /// semantics are satisfied. Cluster mode currently supports only local-only
    /// unbounded scans; if the configured CL/RF would require remote duplicate
    /// merge, it fails loudly instead of materializing the entire result.
    pub async fn range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> crate::error::Result<PartitionResultStream> {
        self.range_read_stream_all_with(
            table_id,
            row_limit,
            ConsistencyLevel::One,
            &ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await
    }

    /// Stream every partition with the caller's requested consistency and
    /// table/keyspace replication strategy.
    pub async fn range_read_stream_all_with(
        &self,
        table_id: &TableId,
        row_limit: usize,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_range_stream(engine.clone(), table_id, row_limit)),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => Ok(local_range_stream(
                coordinator.local_storage().clone(),
                table_id,
                row_limit,
            )),
            Self::Cluster(coordinator) => {
                if coordinator.streaming_range_reads {
                    coordinator
                        .coordinate_range_read_stream_all_with(
                            table_id,
                            row_limit,
                            cl,
                            strategy.replication_factor(),
                        )
                        .await
                } else {
                    Err(crate::error::ClusterError::Internal(
                        "uncapped range_read is unavailable because FERROSA_BULK_STREAMING_RANGE_READ=0 selected the legacy capped range RPC; refusing to return a partial scan".into(),
                    ))
                }
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "range read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Stream every partition starting at a resume position, using the
    /// fragmented (intra-partition streaming) iterators.
    ///
    /// This backs the coordinator-side paging cursor for unbounded `SELECT *`
    /// scans: the previous page's continuation token is decoded into a
    /// [`ScanResume`] — the last partition key (inclusive) plus the last
    /// delivered clustering position within it — the scan resumes there, and
    /// every producer drops the already-emitted prefix of that partition.
    /// `resume == None` is the first page.
    ///
    /// In cluster mode the local-only fan-out (CL=ONE with the keyspace RF
    /// spanning the ring) streams the local fragmented iterator directly; a
    /// multi-replica shape fans out a resume-bounded fragment stream to each
    /// CL-selected replica and merges them with the local stream through the
    /// coordinator's token-aware N-way fragment merge. The resume position is
    /// shipped to every replica so a resumed page never re-streams the
    /// already-emitted prefix over the wire — including mid-wide-partition.
    pub async fn range_read_stream_all_from(
        &self,
        table_id: &TableId,
        resume: Option<&ScanResume>,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_range_stream_from(engine.clone(), table_id, resume)),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => Ok(
                local_range_stream_from(coordinator.local_storage().clone(), table_id, resume),
            ),
            Self::Cluster(coordinator) => {
                if !coordinator.streaming_range_reads {
                    return Err(crate::error::ClusterError::Internal(
                        "uncapped range_read is unavailable because FERROSA_BULK_STREAMING_RANGE_READ=0 selected the legacy capped range RPC; refusing to return a partial scan".into(),
                    ));
                }
                coordinator
                    .coordinate_range_read_stream_from(
                        table_id,
                        resume,
                        cl,
                        strategy.replication_factor(),
                    )
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "range read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Projection-aware resume-capable streaming range read. Mirrors
    /// [`Self::range_read_stream_all_from`] for the `SELECT col1, col2 FROM t`
    /// (no WHERE) paged scan shape, byte-skipping unprojected cells in the
    /// SSTable layer.
    pub async fn range_read_projected_stream_all_from(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        resume: Option<&ScanResume>,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_projected_range_stream_from(
                engine.clone(),
                table_id,
                wanted,
                resume,
            )),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => {
                Ok(local_projected_range_stream_from(
                    coordinator.local_storage().clone(),
                    table_id,
                    wanted,
                    resume,
                ))
            }
            Self::Cluster(coordinator) => {
                if !coordinator.streaming_range_reads {
                    return Err(crate::error::ClusterError::Internal(
                        "uncapped range_read is unavailable because FERROSA_BULK_STREAMING_RANGE_READ=0 selected the legacy capped range RPC; refusing to return a partial scan".into(),
                    ));
                }
                coordinator
                    .coordinate_range_read_projected_stream_from(
                        table_id,
                        wanted,
                        resume,
                        cl,
                        strategy.replication_factor(),
                    )
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "coordinate_range_read_projected_stream_from unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// COUNT(*) fast path. Returns the total row count for
    /// `table_id` without materializing any partition into a Vec
    /// and without decoding any cell payloads. Uses
    /// `StorageEngine::count_range` which drives the metadata-only
    /// k-way merger across memtable + flushing memtable + per-SSTable
    /// streaming readers. Per-replica view (LOCAL consistency).
    pub async fn count_range(&self, table_id: &TableId) -> crate::error::Result<u64> {
        match self {
            Self::Direct(engine) => engine
                .count_range(table_id, None, None)
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .count_range(table_id, None, None)
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => coordinator.coordinate_range_count(table_id).await,
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "count_range unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// COUNT(*) at an EXPLICIT consistency level.
    ///
    /// The CQL layer must call this and pass `ctx.consistency`: the ADR-020
    /// COUNT(*) fast path otherwise answers at the node's default CL while
    /// the full `SELECT` on the same table answers at the client's, and the
    /// mismatch under-reports silently.
    ///
    /// Only the clustered arm can honour a CL — the direct and pair arms are
    /// a single local replica by construction, so they keep their local view
    /// exactly as `count_range` does.
    pub async fn count_range_with(
        &self,
        table_id: &TableId,
        cl: crate::consistency::ConsistencyLevel,
    ) -> crate::error::Result<u64> {
        match self {
            Self::Cluster(coordinator) => {
                coordinator.coordinate_range_count_with(table_id, cl).await
            }
            _ => self.count_range(table_id).await,
        }
    }

    /// Projection-aware streaming range read for query shapes that only need a
    /// subset of regular cells to evaluate predicates. This keeps COUNT(*) with
    /// ALLOW FILTERING on wide tables out of the Vec-returning materialization
    /// path while preserving fail-closed consistency semantics.
    pub async fn range_read_projected_stream_all_with(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        partition_limit: Option<usize>,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_projected_range_stream(
                engine.clone(),
                table_id,
                wanted,
                partition_limit,
            )),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => {
                Ok(local_projected_range_stream(
                    coordinator.local_storage().clone(),
                    table_id,
                    wanted,
                    partition_limit,
                ))
            }
            Self::Cluster(coordinator) => {
                if partition_limit.is_some() {
                    return Err(crate::error::ClusterError::Internal(
                        "projected cluster range scan with partition_limit is not implemented; refusing to return partial results".into(),
                    ));
                }
                coordinator
                    .coordinate_range_read_projected_stream_all_with(
                        table_id,
                        wanted,
                        cl,
                        strategy.replication_factor(),
                    )
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "coordinate_range_read_projected_stream_all_with unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Read up to `limit` partitions for unordered full-scan consumers.
    ///
    /// This lets CQL `LIMIT` and protocol page-size produce the first page
    /// promptly instead of materializing the full default scan window before
    /// applying row-level bounds. `limit` is the caller's OWN bound and is no
    /// longer re-clamped to `DEFAULT_RANGE_READ_LIMIT`; the storage layer's
    /// Vec-materialization OOM guard fail-louds if a caller asks this
    /// `Vec`-returning API for more than it can safely materialize, so large
    /// user `LIMIT`s route through the streaming scan instead.
    pub async fn range_read_limited(
        &self,
        table_id: &TableId,
        limit: usize,
    ) -> crate::error::Result<Vec<Partition>> {
        self.range_read_limited_rows(table_id, limit, 0).await
    }

    /// Read up to `limit` partitions and, when `row_limit > 0`, include at most
    /// `row_limit` rows from each returned partition. The row cap is intended
    /// for safe query shapes where predicates are partition-key-only and any row
    /// from a matching partition satisfies the filter.
    pub async fn range_read_limited_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<Vec<Partition>> {
        // The `limit` here is the caller's OWN bound — a user `LIMIT N` or the
        // protocol page size — so it must NOT be re-clamped to a server-side
        // `DEFAULT_RANGE_READ_LIMIT` result cap. A user asking for `LIMIT 20000`
        // must receive up to 20000 rows; memory is bounded by the caller's
        // chosen `limit`, not a magic 10_000. `limit == 0` is meaningless for a
        // bounded read, so floor it at 1. (The truncation-detecting
        // `range_read_limited_rows_checked` keeps its own bound for the
        // still-accumulating complex shapes until spill lands — step 5.)
        let limit = limit.max(1);
        self.range_read_partitions_inner(table_id, limit, row_limit)
            .await
    }

    /// Shared range-read dispatch with an explicit partition `limit` and no
    /// hard-cap clamping. Callers are responsible for bounding `limit`. This
    /// exists so [`Self::range_read_limited_rows_checked`] can probe exactly one
    /// partition past the hard cap to detect truncation, which the public
    /// clamping wrapper cannot express.
    async fn range_read_partitions_inner(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<Vec<Partition>> {
        match self {
            Self::Direct(engine) => engine
                .read_range_limited_rows(table_id, None, None, limit, row_limit)
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .read_range_limited_rows(table_id, None, None, limit, row_limit)
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => {
                coordinator
                    .coordinate_range_read_limited_rows(table_id, limit, row_limit)
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "range read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Truncation-aware variant of [`Self::range_read_limited_rows`].
    ///
    /// Reads up to `limit + 1` partitions internally and returns at most the
    /// first `limit`, plus a `truncated` flag that is `true` when more than
    /// `limit` partitions existed (i.e. the cap clipped the result). Callers
    /// that must not silently truncate — complex query shapes (ORDER BY /
    /// DISTINCT / aggregate / function projection over a full scan) where the
    /// cap is the engine's `DEFAULT_RANGE_READ_LIMIT` rather than a user
    /// `LIMIT` — use this to fail loud instead of computing a wrong answer over
    /// a clipped set.
    ///
    /// The internal probe of `limit + 1` is still bounded by
    /// `DEFAULT_RANGE_READ_LIMIT + 1`; this method does not widen the hard cap.
    pub async fn range_read_limited_rows_checked(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<(Vec<Partition>, bool)> {
        // Apply the same hard cap the public reader uses, then probe exactly one
        // partition past it via the unclamped inner dispatch. This distinguishes
        // "the table has at most `effective_limit` partitions" from "the table
        // has more and we are about to clip it" even when `limit` is the hard
        // cap itself (the silent-truncation case we exist to catch).
        let effective_limit = limit.clamp(1, DEFAULT_RANGE_READ_LIMIT);
        let probe_limit = effective_limit.saturating_add(1);
        let mut partitions = self
            .range_read_partitions_inner(table_id, probe_limit, row_limit)
            .await?;
        let truncated = partitions.len() > effective_limit;
        if truncated {
            partitions.truncate(effective_limit);
        }
        Ok((partitions, truncated))
    }

    /// Read by secondary index, scattering to all nodes in cluster mode.
    ///
    /// - `Direct` / `Pair`: reads from local storage only.
    /// - `Cluster`: fans out to every ring node, each runs a local
    ///   `read_by_index`, and results are merged and deduplicated.
    /// - `Unavailable`: returns error.
    pub async fn index_read(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        match self {
            Self::Direct(engine) => engine
                .read_by_index(table_id, index_name, index_key)
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .read_by_index(table_id, index_name, index_key)
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => {
                coordinator
                    .coordinate_index_read(table_id, index_name, index_key)
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "index read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// KEYED secondary-index read (t_430c4188): consult the index for
    /// `index_key` restricted to the partition `key`.
    ///
    /// - `Direct` / `Pair`: local `read_by_index_in_partition`.
    /// - `Cluster`: routes to the PARTITION'S replicas under `strategy` (normal
    ///   keyed routing) — never the global scatter-gather of
    ///   [`index_read`](Self::index_read).
    /// - `Unavailable`: returns error.
    ///
    /// Per-node work is O(rows matching the indexed value), never O(partition
    /// rows) — this is what serves `WHERE <full partition key> AND
    /// <indexed_col> = ?` without a full-partition filtering scan.
    pub async fn index_read_in_partition(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        match self {
            Self::Direct(engine) => engine
                .read_by_index_in_partition(table_id, index_name, index_key, key.key.as_bytes())
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => coordinator
                .local_storage()
                .read_by_index_in_partition(table_id, index_name, index_key, key.key.as_bytes())
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => {
                coordinator
                    .coordinate_index_read_in_partition(
                        table_id, key, index_name, index_key, cl, strategy,
                    )
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "keyed index read unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Full-text (`fts_match`) index lookup, returning matching partition keys.
    ///
    /// In standalone/pair mode this hits local storage. In cluster mode the
    /// coordinator fans out to every node and unions the matching keys, because
    /// `fts_match` carries no partition key and its hits span all token ranges —
    /// a coordinator-local lookup made the result non-deterministic (BUG-F-007).
    ///
    /// `limit` is the query-derived `LIMIT k` pushed down to every replica so
    /// each holds a bounded top-k working set instead of every matching doc
    /// key (t_ee98faa0 layer 2). `None` = complete match set (no-LIMIT
    /// statement) — never a server-side cap.
    pub async fn fulltext_search(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
        limit: Option<usize>,
    ) -> crate::error::Result<Vec<Vec<u8>>> {
        match self {
            Self::Direct(engine) => {
                offloaded_fulltext_search(engine.clone(), table_id, index_name, query, limit).await
            }
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => {
                offloaded_fulltext_search(
                    coordinator.local_storage().clone(),
                    table_id,
                    index_name,
                    query,
                    limit,
                )
                .await
            }
            Self::Cluster(coordinator) => {
                coordinator
                    .coordinate_fulltext_search(table_id, index_name, query, limit)
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "fulltext search unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Streaming fulltext search for the no-`LIMIT` / escalated shape
    /// (t_4ae47a9f): yields batches of matching doc keys through a bounded
    /// channel instead of one O(matches) `Vec` — the materialized union of a
    /// broad `fts_match` is what OOM-killed replicas (t_8fc24ce2).
    ///
    /// Batch contract: cluster mode dedups across replicas at the
    /// coordinator; Direct/Pair batches may contain cross-source duplicates
    /// (memtable vs sidecar) — the caller dedups, exactly as it must for the
    /// cluster path's cross-page keys. An `Err` item fails the whole search
    /// (no silent partial match set); dropping the receiver cancels all
    /// in-flight work.
    ///
    /// `FERROSA_BULK_STREAMING_FULLTEXT=0` (mixed-version rolling upgrades)
    /// falls back to the legacy materializing union delivered as one batch —
    /// degraded to the old memory profile, loudly logged at startup.
    pub async fn fulltext_search_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
    ) -> crate::error::Result<tokio::sync::mpsc::Receiver<crate::error::Result<Vec<Vec<u8>>>>> {
        match self {
            Self::Direct(engine) => Ok(local_fulltext_key_stream(
                engine.clone(),
                table_id,
                index_name,
                query,
            )),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => {
                Ok(local_fulltext_key_stream(
                    coordinator.local_storage().clone(),
                    table_id,
                    index_name,
                    query,
                ))
            }
            Self::Cluster(coordinator) => {
                if coordinator.streaming_fulltext {
                    coordinator
                        .coordinate_fulltext_search_stream(table_id, index_name, query)
                        .await
                } else {
                    // Legacy fallback: materialize the union (old memory
                    // profile) and deliver it as a single batch.
                    let keys = coordinator
                        .coordinate_fulltext_search(table_id, index_name, query, None)
                        .await?;
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    let _ = tx.send(Ok(keys)).await;
                    Ok(rx)
                }
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "fulltext search unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Resolve a partition `token`'s replica **host ids** under the keyspace
    /// `strategy`, for an Accord transaction's participant set (ADR-021).
    ///
    /// `Some(..)` only in **cluster** mode — replica placement is the ring's
    /// concern and lives behind this boundary so the CQL/Postgres front-ends
    /// never touch the ring/partitioner. Returns `None` in standalone/pair/
    /// degraded modes, where the caller uses the local node (Accord transactions
    /// are a cluster feature; a single-node deployment is one trivial replica).
    pub fn replicas_for_key(
        &self,
        token: crate::raft::Token,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> Option<Vec<uuid::Uuid>> {
        match self {
            Self::Cluster(coordinator) => Some(coordinator.replica_host_ids_for(token, strategy)),
            Self::Direct(_) | Self::Pair(_) | Self::DegradedPair(_) | Self::Unavailable => None,
        }
    }

    /// Resolve the Accord participant replica **host ids** that own a partition
    /// `key` under the keyspace `replication`, computing the key's token and
    /// parsing the strategy here so the CQL/Postgres front-ends pass raw key
    /// bytes + keyspace metadata and never touch the partitioner or ring
    /// (ADR-021).
    ///
    /// - `Ok(Some(replicas))` in **cluster** mode — the RF replicas owning the
    ///   key's token (a proper subset of the cluster when RF < node count).
    /// - `Ok(None)` outside cluster mode (no ring) — the caller falls back to
    ///   its local/all-live-peers participant set.
    /// - `Err(_)` if `replication` cannot be parsed into a strategy — fail loud
    ///   rather than silently resolving to an empty/default set.
    pub fn accord_replicas_for_key(
        &self,
        key: &[u8],
        replication: &ferrosa_schema::metadata::keyspace::ReplicationParams,
    ) -> Result<Option<Vec<uuid::Uuid>>, crate::ring::strategy::StrategyParseError> {
        let strategy = crate::ring::strategy::ReplicationStrategy::try_from(replication)?;
        let token = ferrosa_common::Token::from_key(key).0;
        Ok(self.replicas_for_key(token, &strategy))
    }

    /// Truncate a table. In standalone/pair mode this truncates local storage.
    /// In cluster mode the coordinator fans out to all nodes.
    pub async fn truncate(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.truncate(table_id),
            Self::Pair(coordinator) => coordinator.local_storage().truncate(table_id),
            Self::Cluster(coordinator) => coordinator
                .coordinate_truncate(table_id)
                .await
                .map_err(|e| ferrosa_common::Error::InvalidData(format!("cluster truncate: {e}"))),
            Self::DegradedPair(_) => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, truncate rejected until operator promotes".into(),
            )),
        }
    }

    /// Write a row. In standalone mode this goes directly to storage.
    /// In pair mode this goes through the PairCoordinator which handles
    /// replication (primary) or forwarding (secondary).
    /// In cluster mode the replication strategy determines whether to use
    /// SimpleStrategy or NetworkTopologyStrategy DC-aware coordination.
    pub async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        match self {
            Self::Direct(engine) => engine.write(table_id, key, row, timestamp),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::DegradedPair(_) => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                let mutation = Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key.clone(),
                    vec![row],
                    timestamp,
                );
                let res = coordinator
                    .coordinate_write(&mutation)
                    .await
                    .map_err(cluster_error_to_common);
                if res.is_ok() {
                    if let Some((bus, ev)) = committed_cdc_event(
                        coordinator.local_storage(),
                        table_id,
                        &mutation.key,
                        &mutation.rows,
                        mutation.timestamp,
                    ) {
                        bus.publish(ev);
                    }
                }
                res
            }
            Self::Cluster(coordinator) => {
                // Capture the committed-CDC payload before `row` is moved into
                // the coordinate call; clones only if a subscriber is listening.
                let pending = committed_cdc_event(
                    &coordinator.storage,
                    table_id,
                    key,
                    std::slice::from_ref(&row),
                    timestamp,
                );
                let res = match strategy {
                    ReplicationStrategy::Simple { replication_factor } => coordinator
                        .coordinate_write_with(
                            table_id,
                            key,
                            row,
                            timestamp,
                            cl,
                            *replication_factor,
                        )
                        .await
                        .map_err(cluster_error_to_common),
                    ReplicationStrategy::NetworkTopology { .. } => coordinator
                        .coordinate_write_nts(table_id, key, row, timestamp, cl, strategy)
                        .await
                        .map_err(cluster_error_to_common),
                };
                if res.is_ok() {
                    if let Some((bus, ev)) = pending {
                        bus.publish(ev);
                    }
                }
                res
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::strategy::ReplicationStrategy;
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{
            CommitLogBatchConfig, CommitLogConfig, CompactionConfig, StorageEngineConfig,
            SyncStrategyConfig,
        };
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: CommitLogBatchConfig::default(),
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
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

    #[tokio::test]
    async fn direct_write_path_delegates_to_storage() {
        assert_eq!(
            DEFAULT_RANGE_READ_LIMIT, 10_000,
            "cluster range reads must not use the historical 1M materialization window"
        );

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        // Register a table
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
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

        let wp = WritePath::direct(storage.clone());
        let table_id = TableId::new("ks", "tbl");
        let key = DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        let strategy = ReplicationStrategy::Simple {
            replication_factor: 1,
        };
        WritePath::write(
            &wp,
            &table_id,
            &key,
            row,
            1000,
            ConsistencyLevel::One,
            &strategy,
        )
        .await
        .unwrap();

        // Verify data was written
        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "DirectWritePath should write to storage");
    }

    #[test]
    fn replicas_for_key_is_none_outside_cluster_mode() {
        // Standalone/pair/degraded modes have no ring, so the caller falls back
        // to the local node. Cluster-mode resolution is covered by the ring's
        // `replica_host_ids_for_strategy` test.
        let strategy = ReplicationStrategy::Simple {
            replication_factor: 3,
        };
        assert_eq!(
            WritePath::unavailable().replicas_for_key(0, &strategy),
            None
        );
    }

    #[test]
    fn committed_cdc_event_only_when_subscribed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let bus = ferrosa_cdc::CdcBus::new(16);
        storage.set_cdc_bus(bus.clone());

        let table_id = TableId::new("ks", "t");
        let key = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(vec![1]),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        // No committed-stream subscriber → no event built (no allocation).
        assert!(
            committed_cdc_event(&storage, &table_id, &key, std::slice::from_ref(&row), 1000)
                .is_none()
        );

        // With a subscriber → event built with the right fields, accord_ts None.
        let _sub = bus.subscribe(ferrosa_cdc::CdcStream::CommittedToCluster);
        let (_bus, ev) =
            committed_cdc_event(&storage, &table_id, &key, std::slice::from_ref(&row), 1000)
                .expect("committed CDC event built when subscribed");
        assert_eq!(ev.stream, ferrosa_cdc::CdcStream::CommittedToCluster);
        assert_eq!(ev.keyspace, "ks");
        assert_eq!(ev.table, "t");
        assert_eq!(ev.rows, vec![row]);
        assert!(ev.accord_ts.is_none());
    }

    #[tokio::test]
    async fn direct_index_read_returns_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "label".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage
            .register_table_with_indexes(schema, vec![("label_idx".to_string(), 0)])
            .unwrap();

        let table_id = TableId::new("ks", "tbl");

        // Insert 4 rows all with the same indexed value.
        for i in 0..4u8 {
            let pk = format!("user{i}");
            let key = DecoratedKey::new(PartitionKey::new(pk.into_bytes()));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"shared".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            storage.write(&table_id, &key, row, 1000).unwrap();
        }

        // Verify base data is readable.
        let key0 = DecoratedKey::new(PartitionKey::new(b"user0".to_vec()));
        let base = storage.read(&table_id, &key0).unwrap();
        assert!(base.is_some(), "base partition should be readable for key0");

        // Verify index read returns all rows.
        let index_key = ferrosa_index::IndexKey(b"shared".to_vec());
        let direct = storage
            .read_by_index(&table_id, "label_idx", &index_key)
            .unwrap();
        assert_eq!(
            direct.len(),
            4,
            "engine.read_by_index must return all 4 rows (direct check)"
        );

        let wp = WritePath::direct(storage);
        let partitions = wp
            .index_read(&table_id, "label_idx", &index_key)
            .await
            .unwrap();
        assert_eq!(
            partitions.len(),
            4,
            "index_read must return all 4 rows with label='shared'"
        );
    }

    #[test]
    fn unbounded_write_path_range_read_must_not_collect_local_streams() {
        let source = include_str!("write_path.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production write_path source must be present");
        assert!(
            !source.contains("async fn collect_uncapped_local_range"),
            "unbounded local range reads must be exposed as streams, not collected into Vec<Partition>"
        );
        let range_read_body = source
            .split("pub async fn range_read(&self, table_id: &TableId)")
            .nth(1)
            .and_then(|rest| rest.split("pub async fn read").next())
            .expect("range_read body must be present");
        assert!(
            !range_read_body.contains("coordinate_range_read_stream_all(table_id, 0)"),
            "cluster range_read must not call the Vec-returning unbounded streaming coordinator"
        );
        assert!(
            source.contains("pub async fn range_read_stream_all_with")
                && source.contains("strategy.replication_factor()")
                && source.contains("coordinate_range_read_stream_all_with"),
            "cluster streaming full scans must carry caller consistency and keyspace replication into the coordinator"
        );
        assert!(
            source.contains("pub async fn range_read_with")
                && source.contains(".range_read_stream_all_with(table_id, 0, cl, strategy)"),
            "materializing range reads must collect from the per-query streaming boundary"
        );
        let projected_body = source
            .split("pub async fn range_read_projected_stream_all_with")
            .nth(1)
            .and_then(|rest| rest.split("/// Read up to `limit` partitions").next())
            .expect("projected streaming range-read body must be present");
        assert!(
            projected_body.contains("local_projected_range_stream")
                && projected_body.contains("coordinate_range_read_projected_stream_all_with"),
            "projected scans must expose a stream and fail clearly when cluster semantics would under-read"
        );
    }

    // ── Accord per-key replica resolution (ADR-021) ───────────────────────────
    // The write path owns replica placement; the CQL/PG front-ends ask it for a
    // key's Accord participant set rather than resolving the ring themselves.

    fn simple_replication(rf: usize) -> ferrosa_schema::metadata::keyspace::ReplicationParams {
        ferrosa_schema::metadata::keyspace::ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: std::collections::HashMap::from([(
                "replication_factor".to_string(),
                rf.to_string(),
            )]),
        }
    }

    fn ring_node(addr: &str) -> crate::raft::NodeInfo {
        crate::raft::NodeInfo {
            host_id: uuid::Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: crate::raft::NodeState::Normal,
            cql_broadcast: None,
        }
    }

    #[test]
    fn accord_replicas_for_key_is_none_outside_cluster_mode() {
        // Standalone/pair/degraded modes have no ring, so the resolver yields
        // None and the caller falls back to its local/all-live-peers set rather
        // than guessing a placement.
        let params = simple_replication(3);
        assert_eq!(
            WritePath::unavailable()
                .accord_replicas_for_key(b"some-partition-key", &params)
                .expect("a valid strategy parses"),
            None
        );
    }

    #[test]
    fn accord_replicas_for_key_fails_loud_on_bad_strategy() {
        // An unparseable replication strategy must surface an error, never
        // silently resolve to an empty/default replica set (fail loud).
        let params = ferrosa_schema::metadata::keyspace::ReplicationParams {
            strategy: "NoSuchStrategy".to_string(),
            options: std::collections::HashMap::new(),
        };
        assert!(
            WritePath::unavailable()
                .accord_replicas_for_key(b"k", &params)
                .is_err(),
            "unknown replication strategy must fail loud"
        );
    }

    #[test]
    fn accord_replicas_for_key_resolves_rf_subset_in_cluster_mode() {
        // Token-aware: over a 3-node ring with RF=2, a key resolves to exactly
        // its two owning replicas — a proper subset of the cluster, not "all
        // live peers". This is what makes the live Accord path RF-correct.
        use ferrosa_net::peer::{PeerEventListener, PeerManager};
        use ferrosa_net::rpc::handler::PeerId;

        struct NoopListener;
        impl PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _: PeerId) {}
            fn on_peer_disconnected(&self, _: PeerId) {}
            fn on_peer_suspected(&self, _: PeerId) {}
            fn on_peer_recovered(&self, _: uuid::Uuid) {}
            fn on_peer_failed(&self, _: uuid::Uuid) {}
        }

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let mut ring = crate::ring::TokenRing::new();
        let n1 = ring_node("10.0.0.1:7000");
        let n2 = ring_node("10.0.0.2:7000");
        let n3 = ring_node("10.0.0.3:7000");
        let members = [n1.host_id, n2.host_id, n3.host_id];
        ring.add_node(1, n1);
        ring.add_node(2, n2);
        ring.add_node(3, n3);
        ring.assign_tokens(1, &[-3_000_000_000_000_000_000]);
        ring.assign_tokens(2, &[0]);
        ring.assign_tokens(3, &[3_000_000_000_000_000_000]);

        let coordinator = ClusterCoordinator::new(
            std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(ring)),
            std::sync::Arc::new(PeerManager::new(
                std::sync::Arc::new(ferrosa_net::config::NetConfig::default()),
                uuid::Uuid::new_v4(),
                std::sync::Arc::new(NoopListener),
            )),
            1,
            storage,
            2,
            ConsistencyLevel::One,
        );
        let write_path = WritePath::cluster(std::sync::Arc::new(coordinator));

        let params = simple_replication(2);
        let replicas = write_path
            .accord_replicas_for_key(b"some-partition-key", &params)
            .expect("a valid strategy parses")
            .expect("cluster mode resolves a concrete replica set");

        assert_eq!(replicas.len(), 2, "RF=2 → exactly two owning replicas");
        assert!(
            replicas.iter().all(|r| members.contains(r)),
            "every resolved replica must be a ring member"
        );
    }

    // ---- resume-filter fail-loud on non-monotonic delivery (t_a0f922a3) ----

    fn resume_row(clustering: &[u8]) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    fn resume_partition(key: &[u8], clusterings: &[&[u8]]) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: clusterings.iter().map(|c| resume_row(c)).collect(),
        }
    }

    /// A mis-sorted SSTable delivers a wide partition as two concatenated
    /// ascending runs. On a resumed page the second run restarts BELOW the
    /// resume clustering; the resume filter would silently drop it and
    /// under-deliver the page. The stream MUST fail loud instead.
    #[tokio::test]
    async fn resume_filtered_stream_fails_loud_on_non_monotonic_resume_partition() {
        let key = b"P";
        // run 1: 0x10,0x20,0x30 (all > resume_ck 0x05, kept); then run 2 restarts
        // at 0x01 (< resume_ck, would be silently dropped) — a regression.
        let frag1 = resume_partition(key, &[&[0x10], &[0x20], &[0x30]]);
        let frag2 = resume_partition(key, &[&[0x01], &[0x02]]);
        let items: Vec<crate::error::Result<Partition>> = vec![Ok(frag1), Ok(frag2)];
        let stream: PartitionResultStream = Box::pin(futures::stream::iter(items));
        let resume = ScanResume {
            key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
            clustering: Some(vec![0x05]),
        };
        let mut out = resume_filtered_stream(stream, Some(&resume));
        let mut saw_err = false;
        while let Some(item) = out.next().await {
            if let Err(e) = item {
                assert!(
                    format!("{e}").contains("non-monotonic"),
                    "unexpected error: {e}"
                );
                saw_err = true;
                break;
            }
        }
        assert!(
            saw_err,
            "resume_filtered_stream MUST fail loud on a non-monotonic resume partition, \
             not silently drop the regressed run"
        );
    }

    /// The common case: a monotonic stream whose prefix (`<= resume_ck`) is the
    /// already-delivered rows. The filter drops the prefix and delivers the
    /// tail with NO error — the guard is inert in steady state.
    #[tokio::test]
    async fn resume_filtered_stream_passes_monotonic_prefix_drop() {
        let key = b"P";
        // ascending: 0x01,0x05 are the already-delivered prefix (<= 0x05, dropped);
        // 0x06,0x07 are new (> 0x05, kept). Strictly monotonic across fragments.
        let frag1 = resume_partition(key, &[&[0x01], &[0x05]]);
        let frag2 = resume_partition(key, &[&[0x06], &[0x07]]);
        let items: Vec<crate::error::Result<Partition>> = vec![Ok(frag1), Ok(frag2)];
        let stream: PartitionResultStream = Box::pin(futures::stream::iter(items));
        let resume = ScanResume {
            key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
            clustering: Some(vec![0x05]),
        };
        let mut out = resume_filtered_stream(stream, Some(&resume));
        let mut delivered: Vec<u8> = Vec::new();
        while let Some(item) = out.next().await {
            let p = item.expect("monotonic prefix drop must not error");
            for r in &p.rows {
                delivered.push(r.clustering[0]);
            }
        }
        assert_eq!(
            delivered,
            vec![0x06, 0x07],
            "only rows strictly greater than the resume clustering survive"
        );
    }
}
