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

/// Default upper bound for unordered range reads when the caller does not
/// provide a tighter page/limit bound.
pub const DEFAULT_RANGE_READ_LIMIT: usize = 10_000;

pub type PartitionResultStream =
    Pin<Box<dyn Stream<Item = crate::error::Result<Partition>> + Send>>;

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
/// inclusive lower-bound key. Used by the coordinator-side paging cursor to
/// resume an unbounded `SELECT *` scan at the last partition key without
/// materializing the whole table. `row_limit == 0` (the unbounded scan shape)
/// is the only caller, so wide partitions stream as bounded fragments.
fn local_range_stream_from(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    start: Option<&DecoratedKey>,
) -> PartitionResultStream {
    let stream = engine
        .range_iter_fragmented(table_id, start, None)
        .map(|item| item.map_err(crate::error::ClusterError::Storage));
    Box::pin(stream)
}

fn local_projected_range_stream_from(
    engine: Arc<StorageEngine>,
    table_id: &TableId,
    wanted: Vec<u16>,
    start: Option<&DecoratedKey>,
) -> PartitionResultStream {
    let stream = engine
        .range_iter_projected_fragmented(table_id, wanted, start, None)
        .map(|item| item.map_err(crate::error::ClusterError::Storage));
    Box::pin(stream)
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
    /// Degraded: peer lost, writes rejected until operator promotes.
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
            Self::Pair(coordinator) => {
                // Pair mode: forward each mutation individually.
                for m in mutations {
                    coordinator
                        .coordinate_write(&m)
                        .await
                        .map_err(|e| ferrosa_common::Error::InvalidData(format!("pair: {e}")))?;
                }
                Ok(())
            }
            Self::Cluster(coordinator) => coordinator
                .coordinate_logged_batch(mutations)
                .await
                .map_err(cluster_error_to_common),
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
            Self::Pair(coordinator) => coordinator
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
            Self::Pair(coordinator) => coordinator
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
            Self::Pair(coordinator) => coordinator.local_storage().read(table_id, key),
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
            Self::Pair(coordinator) => Ok(local_range_stream(
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

    /// Stream every partition starting at an inclusive lower-bound key, using
    /// the fragmented (intra-partition streaming) iterators.
    ///
    /// This backs the coordinator-side paging cursor for unbounded `SELECT *`
    /// scans: the previous page's continuation token is decoded into a `start`
    /// key, the scan resumes there, and the consumer drops rows already emitted
    /// within that partition. `start == None` is the first page.
    ///
    /// In cluster mode the local-only fan-out (CL=ONE with the keyspace RF
    /// spanning the ring) streams the local fragmented iterator directly; a
    /// multi-replica shape fans out a start-bounded fragment stream to each
    /// CL-selected replica and merges them with the local stream through the
    /// coordinator's token-aware N-way fragment merge. The resume key is shipped
    /// to every replica so a resumed page never re-streams the already-emitted
    /// prefix.
    pub async fn range_read_stream_all_from(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_range_stream_from(engine.clone(), table_id, start)),
            Self::Pair(coordinator) => Ok(local_range_stream_from(
                coordinator.local_storage().clone(),
                table_id,
                start,
            )),
            Self::Cluster(coordinator) => {
                if !coordinator.streaming_range_reads {
                    return Err(crate::error::ClusterError::Internal(
                        "uncapped range_read is unavailable because FERROSA_BULK_STREAMING_RANGE_READ=0 selected the legacy capped range RPC; refusing to return a partial scan".into(),
                    ));
                }
                coordinator
                    .coordinate_range_read_stream_from(
                        table_id,
                        start,
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

    /// Projection-aware resume-capable streaming range read with an inclusive
    /// lower-bound key. Mirrors [`Self::range_read_stream_all_from`] for the
    /// `SELECT col1, col2 FROM t` (no WHERE) paged scan shape, byte-skipping
    /// unprojected cells in the SSTable layer.
    pub async fn range_read_projected_stream_all_from(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        start: Option<&DecoratedKey>,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(local_projected_range_stream_from(
                engine.clone(),
                table_id,
                wanted,
                start,
            )),
            Self::Pair(coordinator) => Ok(local_projected_range_stream_from(
                coordinator.local_storage().clone(),
                table_id,
                wanted,
                start,
            )),
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
                        start,
                        cl,
                        strategy.replication_factor(),
                    )
                    .await
            }
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "range_read_projected unavailable: write path is in degraded mode".into(),
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
            Self::Pair(coordinator) => coordinator
                .local_storage()
                .count_range(table_id, None, None)
                .map_err(crate::error::ClusterError::Storage),
            Self::Cluster(coordinator) => coordinator.coordinate_range_count(table_id),
            Self::Unavailable => Err(crate::error::ClusterError::Internal(
                "count_range unavailable: write path is in degraded mode".into(),
            )),
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
            Self::Pair(coordinator) => Ok(local_projected_range_stream(
                coordinator.local_storage().clone(),
                table_id,
                wanted,
                partition_limit,
            )),
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
                "range_read_projected unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// Projection-aware range read. Returns partitions whose
    /// `rows[*].cells` contains only cells whose ordinals are in
    /// `wanted` — SSTable cells outside the projection are
    /// byte-skipped via `read_cell_skip`. Used by the CQL fast
    /// path for `SELECT col1, col2 FROM t` on wide tables (esp.
    /// embedding vectors).
    ///
    /// The caller is expected to have already verified that the
    /// projection is safe — i.e., no WHERE clause references
    /// cells outside `wanted` — otherwise predicates would fail
    /// to evaluate against the trimmed rows. CQL router enforces
    /// this; raw callers should too.
    pub async fn range_read_projected(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        partition_limit: Option<usize>,
    ) -> crate::error::Result<Vec<Partition>> {
        use futures::stream::StreamExt;
        let engine = match self {
            Self::Direct(engine) => engine.clone(),
            Self::Pair(coordinator) => coordinator.local_storage().clone(),
            Self::Cluster(coordinator) => coordinator.storage.clone(),
            Self::Unavailable => {
                return Err(crate::error::ClusterError::Internal(
                    "range_read_projected unavailable: write path is in degraded mode".into(),
                ));
            }
        };
        // Push `partition_limit` into the producer so the merger
        // stops emitting after N partitions — without this, the
        // bounded mpsc buffer means the producer races ahead by
        // `STREAM_BUFFER` body decodes after the consumer has
        // already read enough, and on cold cache each of those
        // wasted body decodes is ~hundreds of ms.
        let mut stream = engine.range_iter_projected(table_id, wanted, partition_limit, None, None);
        let cap = partition_limit.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(crate::error::ClusterError::Storage)?);
            if out.len() >= cap {
                drop(stream);
                break;
            }
        }
        Ok(out)
    }

    /// Read up to `limit` partitions for unordered full-scan consumers.
    ///
    /// This lets CQL `LIMIT` and protocol page-size produce the first page
    /// promptly instead of materializing the full default scan window before
    /// applying row-level bounds. The hard cap remains
    /// `DEFAULT_RANGE_READ_LIMIT`; callers cannot request more through this API.
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
        let limit = limit.clamp(1, DEFAULT_RANGE_READ_LIMIT);
        match self {
            Self::Direct(engine) => engine
                .read_range_limited_rows(table_id, None, None, limit, row_limit)
                .map_err(crate::error::ClusterError::Storage),
            Self::Pair(coordinator) => coordinator
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
            Self::Pair(coordinator) => coordinator
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
            Self::Pair(coordinator) => {
                let mutation = Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key.clone(),
                    vec![row],
                    timestamp,
                );
                coordinator
                    .coordinate_write(&mutation)
                    .await
                    .map_err(cluster_error_to_common)
            }
            Self::Cluster(coordinator) => match strategy {
                ReplicationStrategy::Simple { replication_factor } => coordinator
                    .coordinate_write_with(table_id, key, row, timestamp, cl, *replication_factor)
                    .await
                    .map_err(cluster_error_to_common),
                ReplicationStrategy::NetworkTopology { .. } => coordinator
                    .coordinate_write_nts(table_id, key, row, timestamp, cl, strategy)
                    .await
                    .map_err(cluster_error_to_common),
            },
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
            .and_then(|rest| rest.split("/// Projection-aware range read").next())
            .expect("projected streaming range-read body must be present");
        assert!(
            projected_body.contains("local_projected_range_stream")
                && projected_body.contains("coordinate_range_read_projected_stream_all_with"),
            "projected scans must expose a stream and fail clearly when cluster semantics would under-read"
        );
    }
}
