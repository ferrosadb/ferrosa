//! Async data store trait — the single interface for reads and writes.
//!
//! All drivers (CQL, SPARQL, Graph) use `Arc<dyn DataStore>` instead
//! of `Arc<StorageEngine>` directly. In standalone mode, the impl is
//! `LocalDataStore` (wraps StorageEngine). In cluster mode, the impl
//! routes through the ClusterCoordinator to the correct replica.
//!
//! This eliminates the 87+ direct `engine.read()` / `engine.write()`
//! calls that bypassed the coordinator.

use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::Result;
use ferrosa_sstable::types::{Partition, Row};

use crate::TableId;

/// Async interface for data reads and writes.
///
/// Implementations route to local storage or remote replicas depending
/// on the deployment mode.
#[async_trait::async_trait]
pub trait DataStore: Send + Sync {
    /// Read a single partition by key.
    async fn read(&self, table_id: &TableId, key: &DecoratedKey) -> Result<Option<Partition>>;

    /// Write a single row.
    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> Result<()>;

    /// Read all partitions in a table (for range scans).
    async fn read_range(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Result<Vec<Partition>>;
}

/// Local data store — reads/writes directly to StorageEngine.
/// Used in standalone and pair modes.
pub struct LocalDataStore {
    engine: Arc<crate::engine::StorageEngine>,
}

impl LocalDataStore {
    pub fn new(engine: Arc<crate::engine::StorageEngine>) -> Self {
        Self { engine }
    }

    /// Access the underlying engine for operations that don't go through
    /// the DataStore interface (table registration, flush, compaction, etc.)
    pub fn engine(&self) -> &Arc<crate::engine::StorageEngine> {
        &self.engine
    }
}

#[async_trait::async_trait]
impl DataStore for LocalDataStore {
    async fn read(&self, table_id: &TableId, key: &DecoratedKey) -> Result<Option<Partition>> {
        self.engine.read(table_id, key)
    }

    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> Result<()> {
        self.engine.write(table_id, key, row, timestamp)
    }

    async fn read_range(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Result<Vec<Partition>> {
        self.engine.read_range(table_id, start, end, limit)
    }
}
