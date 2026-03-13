//! Background reconciliation for the adjacency index (T5).
//!
//! Safety net for dropped observer mutations (backpressure) and crash recovery
//! gaps. Runs as a tokio task, yielding between partition scans to avoid
//! competing with query workloads.

use std::sync::Arc;
use std::time::Duration;

use ferrosa_schema::Schema;
use ferrosa_storage::{StorageEngine, TableId};

/// Reconciliation metrics.
#[derive(Debug, Default)]
pub struct ReconcileMetrics {
    pub entries_checked: usize,
    pub entries_repaired: usize,
    pub orphans_removed: usize,
}

/// Run one reconciliation pass for a keyspace.
pub fn reconcile_once(
    schema: &Schema,
    _storage: &StorageEngine,
    keyspace: &str,
) -> ReconcileMetrics {
    let snap = schema.snapshot();
    let mut metrics = ReconcileMetrics::default();

    // Find all edge tables in keyspace
    let edge_tables: Vec<_> = snap
        .tables
        .iter()
        .filter(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        })
        .map(|((ks, name), _meta)| TableId::new(ks, name))
        .collect();

    let _adj_ks = crate::adjacency::schema::adjacency_keyspace_name(keyspace);

    for _edge_tid in &edge_tables {
        // Phase 1: Scan edge table partitions
        // For each edge row, verify adjacency entries exist
        // Repair missing entries using make_adjacency_mutation pattern
        //
        // Full implementation requires StorageEngine read_range API
        // which will be wired in Chunk 4. For now, count discovered edge tables.
        metrics.entries_checked += 1;
    }

    metrics
}

/// Spawn the background reconciliation loop.
pub fn spawn_reconciliation(
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    keyspace: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let metrics = reconcile_once(&schema, &storage, &keyspace);
            if metrics.entries_repaired > 0 || metrics.orphans_removed > 0 {
                tracing::info!(
                    keyspace = %keyspace,
                    checked = metrics.entries_checked,
                    repaired = metrics.entries_repaired,
                    orphans = metrics.orphans_removed,
                    "adjacency reconciliation complete"
                );
            }
            tokio::task::yield_now().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_metrics_default_is_zero() {
        let m = ReconcileMetrics::default();
        assert_eq!(m.entries_checked, 0);
        assert_eq!(m.entries_repaired, 0);
        assert_eq!(m.orphans_removed, 0);
    }
}
