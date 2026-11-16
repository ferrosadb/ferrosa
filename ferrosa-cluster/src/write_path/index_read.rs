//! Secondary-index reads through [`WritePath`]: global and keyed (in-partition) consults across Direct, Pair and Cluster.

use super::*;

impl WritePath {
    /// Stream a global secondary-index lookup in row order — `(partition
    /// key, clustering)` — strictly after `after` when given, without
    /// materializing the matching partitions. Every layer holds O(sources):
    /// each node merges its posting sources in row order, and the cluster path
    /// merges the nodes in row order, dropping replica copies by adjacency.
    /// A page resumes by passing the last row it delivered as `after`.
    pub async fn index_read_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        after: Option<&ferrosa_index::RowPosition>,
    ) -> crate::error::Result<PartitionResultStream> {
        match self {
            Self::Direct(engine) => Ok(Box::pin(
                engine
                    .read_by_index_stream_after(table_id, index_name, index_key, after.cloned())
                    .map(|item| item.map_err(ClusterError::Storage)),
            )),
            Self::Pair(coordinator) | Self::DegradedPair(coordinator) => Ok(Box::pin(
                coordinator
                    .local_storage()
                    .read_by_index_stream_after(table_id, index_name, index_key, after.cloned())
                    .map(|item| item.map_err(ClusterError::Storage)),
            )),
            Self::Cluster(coordinator) => {
                coordinator
                    .coordinate_index_read_stream(table_id, index_name, index_key, after)
                    .await
            }
            Self::Unavailable => Err(ClusterError::Internal(
                "index read stream unavailable: write path is in degraded mode".into(),
            )),
        }
    }

    /// KEYED secondary-index read (t_430c4188): consult the index for
    /// `index_key` restricted to the partition `key`.
    ///
    /// - `Direct` / `Pair`: local `read_by_index_in_partition`.
    /// - `Cluster`: routes to the PARTITION'S replicas under `strategy` (normal
    ///   keyed routing) — never the global scatter-gather of
    ///   [`index_read_stream`](Self::index_read_stream).
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
}
