//! Graph execution I/O seam. Ordinary execution delegates to the live routing
//! path; explicit transactions provide a private mutation overlay.
use async_trait::async_trait;
use ferrosa_cluster::{
    consistency::ConsistencyLevel,
    ring::strategy::ReplicationStrategy,
    write_path::{PartitionResultStream, WritePath},
};
use ferrosa_common::DecoratedKey;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_storage::{Mutation, TableId};
use std::sync::Arc;

/// Storage operations used by graph execution, including a transaction's own reads.
#[async_trait]
pub trait GraphWrite: Send + Sync {
    async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>>;
    async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>>;
    async fn range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream>;
    async fn count_range_with(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_cluster::error::Result<u64>;
    fn declares_index_locally(&self, table_id: &TableId, index_name: &str) -> bool;
    fn index_is_current(&self, table_id: &TableId, index_name: &str) -> bool;
    async fn index_read_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        after: Option<&ferrosa_index::RowPosition>,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream>;
    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()>;
    async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> ferrosa_common::Result<()>;
}

#[async_trait]
impl GraphWrite for WritePath {
    async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        WritePath::read(self, table_id, key).await
    }
    async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        WritePath::pk_read(self, table_id, key, cl, strategy).await
    }
    async fn range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
        WritePath::range_read_stream_all(self, table_id, row_limit).await
    }
    async fn count_range_with(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_cluster::error::Result<u64> {
        WritePath::count_range_with(self, table_id, cl, strategy).await
    }
    fn declares_index_locally(&self, table_id: &TableId, index_name: &str) -> bool {
        WritePath::declares_index_locally(self, table_id, index_name)
    }
    fn index_is_current(&self, table_id: &TableId, index_name: &str) -> bool {
        WritePath::index_is_current(self, table_id, index_name)
    }
    async fn index_read_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        after: Option<&ferrosa_index::RowPosition>,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
        WritePath::index_read_stream(self, table_id, index_name, index_key, after).await
    }
    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        WritePath::write(self, table_id, key, row, timestamp, cl, strategy).await
    }
    async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> ferrosa_common::Result<()> {
        WritePath::write_batch(self, mutations, cl, rf).await
    }
}

#[async_trait]
impl<T: GraphWrite + ?Sized> GraphWrite for Arc<T> {
    async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        T::read(self.as_ref(), table_id, key).await
    }
    async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        T::pk_read(self.as_ref(), table_id, key, cl, strategy).await
    }
    async fn range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
        T::range_read_stream_all(self.as_ref(), table_id, row_limit).await
    }
    async fn count_range_with(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_cluster::error::Result<u64> {
        T::count_range_with(self.as_ref(), table_id, cl, strategy).await
    }
    fn declares_index_locally(&self, table_id: &TableId, index_name: &str) -> bool {
        T::declares_index_locally(self.as_ref(), table_id, index_name)
    }
    fn index_is_current(&self, table_id: &TableId, index_name: &str) -> bool {
        T::index_is_current(self.as_ref(), table_id, index_name)
    }
    async fn index_read_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        after: Option<&ferrosa_index::RowPosition>,
    ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
        T::index_read_stream(self.as_ref(), table_id, index_name, index_key, after).await
    }
    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        T::write(self.as_ref(), table_id, key, row, timestamp, cl, strategy).await
    }
    async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> ferrosa_common::Result<()> {
        T::write_batch(self.as_ref(), mutations, cl, rf).await
    }
}
