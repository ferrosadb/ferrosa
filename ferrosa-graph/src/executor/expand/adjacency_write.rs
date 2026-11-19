//! Adjacency-index writes that accompany a graph edge write: deriving the entries from the
//! written row, writing them with a bounded retry over replica schema lag, and restoring
//! entries a deleted edge shared with live siblings.

use super::*;
use std::sync::Arc;

/// The adjacency entries an edge row implies. Derived from the borrowed row,
/// so a caller derives first and then moves the row into its base write
/// instead of cloning it to keep it alive for the derive.
pub(super) fn explicit_adjacency_for(
    schema: Option<&Schema>,
    table_id: &TableId,
    key: &DecoratedKey,
    row: &Row,
    timestamp: i64,
) -> Vec<Mutation> {
    match schema {
        Some(schema) => derive_adjacency_row_mutations(schema, table_id, key, row, timestamp),
        None => Vec::new(),
    }
}

/// Write the entries [`explicit_adjacency_for`] derived, once their base row
/// has landed.
pub(super) async fn write_explicit_adjacency_entries(
    write_path: &dyn GraphWrite,
    table_id: &TableId,
    derived_entries: Vec<Mutation>,
    schema: Option<&Schema>,
) -> Result<()> {
    let Some(schema) = schema else {
        debug_assert!(
            derived_entries.is_empty(),
            "entries need a schema to derive"
        );
        return Ok(());
    };

    for derived in derived_entries {
        let adj_table_id = TableId::new(&derived.keyspace, &derived.table);
        let strategy = graph_replication_strategy(Some(schema), &derived.keyspace)?;
        for derived_row in derived.rows {
            adjacency_write_with_retry(
                write_path,
                &adj_table_id,
                &derived.key,
                derived_row,
                derived.timestamp,
                &strategy,
                table_id,
            )
            .await?;
        }
    }

    Ok(())
}

/// After `DELETE r` tombstoned edge `deleted`'s adjacency entries, write back
/// any of them that another live edge in the same partition still derives.
///
/// An entry is keyed by (vertex, label, neighbour), so edges that differ only
/// in a non-endpoint key column share it — agent_memory's `typed_edges` keeps
/// one row per `edge_type` between the same pair, in one partition. Deleting
/// one of them tombstoned the entry its siblings need. The restore is written
/// one microsecond after the delete, so it outranks both the explicit
/// tombstone and the one the observer derived. (A sibling in ANOTHER partition
/// is not seen here; the background reconcile restores that entry.)
pub(super) async fn restore_entries_shared_with_live_edges(
    write_path: &dyn GraphWrite,
    table_id: &TableId,
    key: &DecoratedKey,
    deleted: &Row,
    timestamp: i64,
    schema: Option<&Schema>,
) -> Result<()> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let derive = |row: &Row, ts: i64| {
        let edge = Mutation::new(
            table_id.keyspace.clone(),
            table_id.table.clone(),
            key.clone(),
            vec![Arc::new(row.clone())],
            ts,
        );
        derive_adjacency_mutations(schema, table_id, &edge)
    };
    let tombstoned: HashSet<(Vec<u8>, Vec<u8>)> = derive(deleted, timestamp)
        .iter()
        .flat_map(|m| {
            m.rows
                .iter()
                .map(|r| (m.key.key.as_bytes().to_vec(), r.clustering.clone()))
        })
        .collect();
    if tombstoned.is_empty() {
        return Ok(());
    }
    let strategy = graph_replication_strategy(Some(schema), &table_id.keyspace)?;
    let Some(partition) = write_path
        .pk_read(table_id, key, graph_write_consistency(), &strategy)
        .await?
    else {
        return Ok(());
    };
    let restore_at = timestamp.saturating_add(1);
    let siblings = partition.rows.iter().filter(|row| {
        row.clustering != deleted.clustering && !crate::adjacency::schema::row_is_deleted(row)
    });
    for sibling in siblings {
        for derived in derive(sibling, restore_at) {
            let adj_table_id = TableId::new(&derived.keyspace, &derived.table);
            let adj_strategy = graph_replication_strategy(Some(schema), &derived.keyspace)?;
            for entry in derived.rows {
                let entry_id = (
                    derived.key.key.as_bytes().to_vec(),
                    entry.clustering.clone(),
                );
                if !tombstoned.contains(&entry_id) {
                    continue;
                }
                adjacency_write_with_retry(
                    write_path,
                    &adj_table_id,
                    &derived.key,
                    entry,
                    restore_at,
                    &adj_strategy,
                    table_id,
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Write a derived adjacency row, retrying on transient failures that arise
/// when the `system_graph_<ks>.adjacency` table was just created via Raft DDL
/// and a follower hasn't yet finished applying the CREATE TABLE on its own
/// state machine.
///
/// The window is narrow — openraft commits once a majority has replicated the
/// log entry, but each follower's `apply` to the local StorageEngine happens
/// in a separate task and can lag a few hundred ms behind on a busy CI
/// runner. When the coordinator immediately fans out the first derived
/// adjacency write at CL=QUORUM, lagging followers reject it with
/// `table not registered: system_graph_<ks>.adjacency` (logged as
/// `MutationForward write failed — not sending ACK`), the coordinator
/// returns `WriteTimeout(received=1, required=2)`, and the graph HTTP
/// request hangs until the client's read timeout.
///
/// Retrying with exponential backoff lets the followers catch up. The write
/// is content-addressed and idempotent (LWW with monotonic timestamps), so
/// duplicate forwards to replicas that already applied are safe.
async fn adjacency_write_with_retry(
    write_path: &dyn GraphWrite,
    adj_table_id: &TableId,
    key: &DecoratedKey,
    row: Arc<Row>,
    timestamp: i64,
    strategy: &ReplicationStrategy,
    edge_table_id: &TableId,
) -> Result<()> {
    // Total budget is ~3 seconds across 6 attempts: 100, 200, 400, 800,
    // 1600 ms. Picks up where DDL_AGREEMENT_APPLY_DRAIN (50 ms) stops.
    const BACKOFFS_MS: &[u64] = &[100, 200, 400, 800, 1600];
    let mut attempt = 0usize;
    let mut backoffs = BACKOFFS_MS.iter().copied();
    loop {
        match write_path
            .write(
                adj_table_id,
                key,
                // Every attempt shares the one derived row; it reaches the
                // memtable as this same allocation.
                Arc::clone(&row),
                timestamp,
                graph_write_consistency(),
                strategy,
            )
            .await
        {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(
                        adj_table = %adj_table_id,
                        attempt,
                        "adjacency write succeeded after retries (follower apply lag)"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                if !is_transient_replica_lag(&e) {
                    return Err(GraphError::Storage(ferrosa_common::Error::InvalidData(
                        format!(
                            "failed to write derived adjacency row for {}.{}: {e}",
                            edge_table_id.keyspace, edge_table_id.table
                        ),
                    )));
                }
                let Some(backoff_ms) = backoffs.next() else {
                    // Retry budget exhausted — surface the last transient error
                    // so the operator sees the actual cluster state.
                    return Err(GraphError::Storage(ferrosa_common::Error::InvalidData(
                        format!(
                            "failed to write derived adjacency row for {}.{} after \
                             {attempt} retries: {e}",
                            edge_table_id.keyspace, edge_table_id.table
                        ),
                    )));
                };
                tracing::warn!(
                    adj_table = %adj_table_id,
                    attempt = attempt + 1,
                    backoff_ms,
                    %e,
                    "adjacency write hit replica schema lag — retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                attempt += 1;
            }
        }
    }
}

/// Distinguish a transient replica-schema-lag write timeout (retryable) from
/// a real cluster failure (not retryable). The cluster coordinator surfaces a
/// write timeout as the typed `Error::WriteTimeout` (t_43f70b3a; it was once
/// `InvalidData("cluster: write timeout: …")`, and that text is still accepted
/// from a path that wraps it). Any other error (unavailable, validation,
/// storage I/O) is final.
pub(super) fn is_transient_replica_lag(e: &ferrosa_common::Error) -> bool {
    if matches!(e, ferrosa_common::Error::WriteTimeout { .. }) {
        return true;
    }
    let msg = e.to_string();
    msg.contains("cluster: write timeout") || msg.contains("table not registered")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_cluster::consistency::ConsistencyLevel;
    use ferrosa_cluster::write_path::{PartitionResultStream, WritePath};
    use ferrosa_common::schema::TableSchema;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Delegates to a real `WritePath`, except that the first `lagging`
    /// writes fail with the typed write timeout a lagging follower produces.
    struct LaggingFollower {
        inner: WritePath,
        lagging: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl GraphWrite for LaggingFollower {
        async fn read(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
        ) -> ferrosa_common::Result<Option<ferrosa_sstable::types::Partition>> {
            GraphWrite::read(&self.inner, table_id, key).await
        }
        async fn pk_read(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
            cl: ConsistencyLevel,
            strategy: &ReplicationStrategy,
        ) -> ferrosa_common::Result<Option<ferrosa_sstable::types::Partition>> {
            GraphWrite::pk_read(&self.inner, table_id, key, cl, strategy).await
        }
        async fn range_read_stream_all(
            &self,
            table_id: &TableId,
            row_limit: usize,
        ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
            GraphWrite::range_read_stream_all(&self.inner, table_id, row_limit).await
        }
        async fn count_range_with(
            &self,
            table_id: &TableId,
            cl: ConsistencyLevel,
            strategy: &ReplicationStrategy,
        ) -> ferrosa_cluster::error::Result<u64> {
            GraphWrite::count_range_with(&self.inner, table_id, cl, strategy).await
        }
        fn declares_index_locally(&self, table_id: &TableId, index_name: &str) -> bool {
            GraphWrite::declares_index_locally(&self.inner, table_id, index_name)
        }
        fn index_is_current(&self, table_id: &TableId, index_name: &str) -> bool {
            GraphWrite::index_is_current(&self.inner, table_id, index_name)
        }
        async fn index_read_stream(
            &self,
            table_id: &TableId,
            index_name: &str,
            index_key: &ferrosa_index::IndexKey,
            after: Option<&ferrosa_index::RowPosition>,
        ) -> ferrosa_cluster::error::Result<PartitionResultStream> {
            GraphWrite::index_read_stream(&self.inner, table_id, index_name, index_key, after).await
        }
        async fn write(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
            row: Arc<Row>,
            timestamp: i64,
            cl: ConsistencyLevel,
            strategy: &ReplicationStrategy,
        ) -> ferrosa_common::Result<()> {
            let lagging = self.lagging.load(Ordering::SeqCst);
            if lagging > 0 {
                self.lagging.store(lagging - 1, Ordering::SeqCst);
                return Err(ferrosa_common::Error::WriteTimeout {
                    consistency: "QUORUM".to_string(),
                    received: 1,
                    required: 2,
                });
            }
            GraphWrite::write(&self.inner, table_id, key, row, timestamp, cl, strategy).await
        }
        async fn write_batch(
            &self,
            mutations: Vec<Mutation>,
            cl: ConsistencyLevel,
            rf: usize,
        ) -> ferrosa_common::Result<()> {
            GraphWrite::write_batch(&self.inner, mutations, cl, rf).await
        }
        async fn write_staged_batch(
            &self,
            batch: ferrosa_storage::write_set_stage::StagedWriteSet,
            cl: ferrosa_cluster::consistency::ConsistencyLevel,
            rf: usize,
        ) -> ferrosa_common::Result<()> {
            GraphWrite::write_staged_batch(&self.inner, batch, cl, rf).await
        }
    }

    /// RED for p0-oom-audit clone-on-row-data at `adjacency_write_with_retry`:
    /// every attempt used to deep-copy the derived row. Now each attempt
    /// shares the caller's allocation, so the row the memtable stores IS the
    /// caller's row (pointer identity), even after a retried attempt.
    #[tokio::test]
    async fn a_retried_adjacency_write_shares_one_row_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            ferrosa_storage::StorageEngine::new(
                ferrosa_storage::StorageEngineConfig::test_config(dir.path()),
                None,
            )
            .unwrap(),
        );
        storage
            .register_table(TableSchema {
                keyspace: "adj_ks".to_string(),
                table: "adjacency".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: Default::default(),
            })
            .unwrap();
        let table = TableId::new("adj_ks", "adjacency");
        let key = DecoratedKey::new(PartitionKey::new(b"vertex".to_vec()));
        let row = Arc::new(Row {
            clustering: vec![],
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(7),
        });
        let lagging = LaggingFollower {
            inner: WritePath::direct(Arc::clone(&storage)),
            lagging: AtomicUsize::new(1),
        };
        adjacency_write_with_retry(
            &lagging,
            &table,
            &key,
            Arc::clone(&row),
            7,
            &ReplicationStrategy::Simple {
                replication_factor: 1,
            },
            &table,
        )
        .await
        .expect("the write lands after one lagging attempt");
        assert_eq!(lagging.lagging.load(Ordering::SeqCst), 0, "one retry ran");

        let stored = storage
            .read(&table, &key)
            .unwrap()
            .expect("the adjacency row was written");
        assert_eq!(stored.rows.len(), 1);
        assert!(
            Arc::ptr_eq(&stored.rows[0], &row),
            "the memtable must hold the caller's row allocation, not a per-attempt copy"
        );
    }
}
