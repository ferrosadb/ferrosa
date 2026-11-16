//! Adjacency-index writes that accompany a graph edge write: deriving the entries from the
//! written row, writing them with a bounded retry over replica schema lag, and restoring
//! entries a deleted edge shared with live siblings.

use super::*;

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
    schema.map_or_else(Vec::new, |schema| {
        derive_adjacency_row_mutations(schema, table_id, key, row, timestamp)
    })
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
            vec![row.clone()],
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
    row: Row,
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
                row.clone(),
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
