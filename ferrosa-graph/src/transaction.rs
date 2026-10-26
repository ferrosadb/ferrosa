//! Connection-owned deferred graph writes. No storage or observer sees a staged
//! mutation. Reads merge the committed view and these mutations using storage's
//! reconciliation rules; COMMIT lowers the write set to the shared BatchTxn.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferrosa_cluster::{
    consistency::ConsistencyLevel,
    error::ClusterError,
    ring::strategy::ReplicationStrategy,
    write_path::{PartitionResultStream, WritePath},
};
use ferrosa_common::DecoratedKey;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};
use ferrosa_storage::{BatchOp, Mutation, StorageEngine, TableId};
use futures::StreamExt;

use crate::executor::io::GraphWrite;

/// Bounds staged mutations and materialized transaction results separately.
/// Accounting includes a conservative allowance for row/cell allocation metadata.
pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

tokio::task_local! {
    // Scoped only while executing and draining a transaction RUN. Owned executor
    // contexts capture the Arc, so every nested UNWIND/FOREACH/CALL uses this I/O.
    pub(crate) static TRANSACTION_IO: Arc<LocalTransaction>;
}

#[derive(Default)]
struct Pending {
    mutations: Vec<Mutation>,
    bytes: usize,
}

pub(crate) struct LocalTransaction {
    pub(crate) path: Arc<WritePath>,
    pub(crate) keyspace: String,
    pub(crate) deadline: Instant,
    storage: Arc<StorageEngine>,
    pending: Mutex<Pending>,
    // Retained across RUNs until the connection consumes/drops the transaction.
    // The async slot also prevents concurrent internal callers reacquiring it.
    pub(crate) writer_admission: tokio::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
}

fn invalid(message: impl Into<String>) -> ferrosa_common::Error {
    ferrosa_common::Error::InvalidData(message.into())
}

impl LocalTransaction {
    pub(crate) fn new(
        path: Arc<WritePath>,
        keyspace: String,
        timeout: Duration,
    ) -> ferrosa_common::Result<Self> {
        let WritePath::Direct(storage) = path.as_ref() else {
            return Err(invalid("Bolt explicit transactions require the single-node direct path; distributed transaction execution still needs Accord read/write-set integration"));
        };
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| invalid("transaction timeout is too large"))?;
        Ok(Self {
            storage: storage.clone(),
            path,
            keyspace,
            deadline,
            pending: Mutex::new(Pending::default()),
            writer_admission: tokio::sync::Mutex::new(None),
        })
    }

    pub(crate) fn check_deadline(&self) -> ferrosa_common::Result<()> {
        if Instant::now() >= self.deadline {
            return Err(invalid("transaction timeout expired"));
        }
        Ok(())
    }

    fn stage(&self, mutations: Vec<Mutation>) -> ferrosa_common::Result<()> {
        self.check_deadline()?;
        let bytes = mutations.iter().try_fold(0usize, |total, m| {
            let overhead = 512usize.saturating_add(
                m.rows
                    .iter()
                    .map(|r| 128usize.saturating_add(r.cells.len().saturating_mul(128)))
                    .sum::<usize>(),
            );
            total
                .checked_add(m.serialized_size().saturating_mul(2))
                .and_then(|n| n.checked_add(overhead))
                .ok_or_else(|| invalid("transaction pending memory limit exceeded"))
        })?;
        let mut pending = self.pending.lock().unwrap();
        if pending.bytes.saturating_add(bytes) > MAX_PENDING_BYTES {
            return Err(invalid("transaction pending memory limit exceeded (8 MiB)"));
        }
        pending.bytes += bytes;
        pending.mutations.extend(mutations);
        Ok(())
    }

    fn overlay(
        &self,
        table: &TableId,
    ) -> ferrosa_common::Result<BTreeMap<DecoratedKey, Vec<Partition>>> {
        self.check_deadline()?;
        let pending = self.pending.lock().unwrap();
        let mut partitions: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for m in &pending.mutations {
            if m.keyspace == table.keyspace && m.table == table.table {
                partitions
                    .entry(m.key.clone())
                    .or_default()
                    .push(Partition {
                        key: m.key.clone(),
                        deletion: DeletionTime::LIVE,
                        static_row: None,
                        rows: m.rows.clone(),
                    });
            }
        }
        Ok(partitions)
    }

    /// Called under the graph publication barrier, after all RUN streams have
    /// been drained. A commit attempt consumes the write set regardless of result.
    pub(crate) fn commit(&self) -> ferrosa_common::Result<()> {
        self.check_deadline()?;
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        let mut batch = self.storage.begin_batch();
        for m in pending.mutations {
            for row in m.rows {
                batch.stage(BatchOp::Write {
                    keyspace: m.keyspace.clone(),
                    table: m.table.clone(),
                    key: m.key.clone(),
                    row,
                    timestamp: m.timestamp,
                });
            }
        }
        batch.commit()
    }
}

fn merged(parts: Vec<Partition>, limit: usize) -> Partition {
    let mut partition = ferrosa_storage::merge_partitions(parts);
    partition
        .rows
        .retain(|row| !crate::adjacency::schema::row_is_deleted(row));
    if limit != 0 {
        partition.rows.truncate(limit);
    }
    partition
}

#[async_trait]
impl GraphWrite for LocalTransaction {
    async fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        let mut parts = self.overlay(table_id)?.remove(key).unwrap_or_default();
        if let Some(base) = self.storage.read(table_id, key)? {
            parts.insert(0, base);
        }
        if parts.is_empty() {
            return Ok(None);
        }
        let partition = merged(parts, 0);
        Ok((!partition.rows.is_empty() || partition.static_row.is_some()).then_some(partition))
    }

    async fn pk_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        _cl: ConsistencyLevel,
        _strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<Option<Partition>> {
        self.read(table_id, key).await
    }

    async fn range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> Result<PartitionResultStream, ClusterError> {
        let mut overlay = self.overlay(table_id).map_err(ClusterError::Storage)?;
        // Do not push a limit below reconciliation: a pending tombstone could
        // remove the limited prefix and hide a later committed row.
        let mut base = self.path.range_read_stream_all(table_id, 0).await?;
        let deadline = self.deadline;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(partition) = base.next().await {
                if Instant::now() >= deadline { Err(ClusterError::Storage(invalid("transaction timeout expired")))?; }
                let partition = partition?;
                let mut parts = overlay.remove(&partition.key).unwrap_or_default();
                parts.insert(0, partition);
                let partition = merged(parts, row_limit);
                if !partition.rows.is_empty() || partition.static_row.is_some() { yield partition; }
            }
            for parts in overlay.into_values() {
                let partition = merged(parts, row_limit);
                if !partition.rows.is_empty() || partition.static_row.is_some() { yield partition; }
            }
        }))
    }

    async fn count_range_with(
        &self,
        table_id: &TableId,
        _cl: ConsistencyLevel,
        _strategy: &ReplicationStrategy,
    ) -> Result<u64, ClusterError> {
        let mut stream = self.range_read_stream_all(table_id, 0).await?;
        let mut count = 0;
        while let Some(partition) = stream.next().await {
            count += partition?.rows.len() as u64;
        }
        Ok(count)
    }

    // Committed indexes do not contain pending keys; the graph planner's normal
    // scan fallback must reconcile the overlay before evaluating predicates.
    fn declares_index_locally(&self, _table_id: &TableId, _index_name: &str) -> bool {
        false
    }
    fn index_is_current(&self, _table_id: &TableId, _index_name: &str) -> bool {
        false
    }
    async fn index_read_stream(
        &self,
        _table_id: &TableId,
        _index_name: &str,
        _index_key: &ferrosa_index::IndexKey,
        _after: Option<&ferrosa_index::RowPosition>,
    ) -> Result<PartitionResultStream, ClusterError> {
        Err(ClusterError::Internal(
            "transaction index reads must use the overlay scan".into(),
        ))
    }

    async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        _cl: ConsistencyLevel,
        _strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        self.stage(vec![Mutation::new(
            table_id.keyspace.clone(),
            table_id.table.clone(),
            key.clone(),
            vec![row],
            timestamp,
        )])
    }
    async fn write_batch(
        &self,
        mutations: Vec<Mutation>,
        _cl: ConsistencyLevel,
        _rf: usize,
    ) -> ferrosa_common::Result<()> {
        self.stage(mutations)
    }
}
