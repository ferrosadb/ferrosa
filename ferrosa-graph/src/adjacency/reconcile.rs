//! Background reconciliation for the adjacency index (T5).
//!
//! Safety net for dropped observer mutations (backpressure) and crash recovery
//! gaps. Runs as a tokio task, yielding between partition scans to avoid
//! competing with query workloads.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_schema::Schema;
use ferrosa_sstable::types::DeletionTime;
use ferrosa_storage::TableId;

use crate::adjacency::observer::make_adjacency_mutation;
use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};
use crate::executor::expand::extract_neighbor_id;

/// Reconciliation metrics.
#[derive(Debug, Default)]
pub struct ReconcileMetrics {
    pub entries_checked: usize,
    pub entries_repaired: usize,
    pub orphans_removed: usize,
}

/// Number of partitions to read per batch during reconciliation.
/// Retained for future use when WritePath supports batched range reads.
#[allow(dead_code)]
const BATCH_LIMIT: usize = 1000;

/// Run one reconciliation pass for a keyspace.
pub async fn reconcile_once(
    schema: &Schema,
    write_path: &WritePath,
    keyspace: &str,
) -> ReconcileMetrics {
    let snap = schema.snapshot();
    let mut metrics = ReconcileMetrics::default();

    // Find all edge tables in keyspace, along with their metadata.
    let edge_tables: Vec<_> = snap
        .tables
        .iter()
        .filter(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        })
        .map(|((ks, name), meta)| (TableId::new(ks, name), meta.clone()))
        .collect();

    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    // Phase 1: For each edge table, scan partitions and verify adjacency entries exist.
    for (edge_tid, edge_meta) in &edge_tables {
        // Verify this edge table has source and target extensions.
        if !edge_meta.extensions.contains_key("graph.source")
            || !edge_meta.extensions.contains_key("graph.target")
        {
            continue;
        }

        let edge_label = edge_tid.table.clone();
        let edge_table_fqn = format!("{}.{}", edge_tid.keyspace, edge_tid.table);

        // Scan all edge table partitions.
        let partitions = match write_path.range_read(edge_tid).await {
            Ok(p) => p,
            Err(_) => continue,
        };

        for partition in &partitions {
            let source_id = partition.key.key.as_bytes().to_vec();
            let source_key = partition.key.clone();

            for row in &partition.rows {
                let target_id = row.clustering.clone();
                metrics.entries_checked += 1;

                // Check OUT adjacency: source -> target
                if !adjacency_entry_exists(
                    write_path,
                    &adj_table_id,
                    &source_key,
                    DIRECTION_OUT,
                    &edge_label,
                    &target_id,
                )
                .await
                {
                    // Repair: create OUT adjacency entry.
                    let mutation = make_adjacency_mutation(
                        &adj_ks,
                        &source_id,
                        DIRECTION_OUT,
                        &edge_label,
                        &target_id,
                        &edge_table_fqn,
                        now_micros(),
                    );
                    if write_mutation(write_path, &mutation).await.is_ok() {
                        metrics.entries_repaired += 1;
                    }
                }

                // Check IN adjacency: target -> source
                let target_key = DecoratedKey::new(PartitionKey::new(target_id.clone()));
                if !adjacency_entry_exists(
                    write_path,
                    &adj_table_id,
                    &target_key,
                    DIRECTION_IN,
                    &edge_label,
                    &source_id,
                )
                .await
                {
                    // Repair: create IN adjacency entry.
                    let mutation = make_adjacency_mutation(
                        &adj_ks,
                        &target_id,
                        DIRECTION_IN,
                        &edge_label,
                        &source_id,
                        &edge_table_fqn,
                        now_micros(),
                    );
                    if write_mutation(write_path, &mutation).await.is_ok() {
                        metrics.entries_repaired += 1;
                    }
                }
            }
        }
    }

    // Phase 2: Scan adjacency index for orphans.
    // For each adjacency entry, verify the source edge still exists.
    let adj_partitions = write_path
        .range_read(&adj_table_id)
        .await
        .unwrap_or_default();

    {
        for partition in &adj_partitions {
            let vertex_id = partition.key.key.as_bytes().to_vec();

            for row in &partition.rows {
                // Parse direction from clustering[0].
                if row.clustering.is_empty() {
                    continue;
                }
                let direction = row.clustering[0];

                // Extract edge label and neighbor ID from clustering.
                let neighbor_id = match extract_neighbor_id(&row.clustering, None) {
                    Some(id) => id,
                    None => continue,
                };
                let edge_label = match extract_edge_label(&row.clustering) {
                    Some(label) => label,
                    None => continue,
                };

                // Determine which edge table this entry references.
                // The edge_table FQN is stored in the row's first cell value.
                let edge_table_fqn = match row.cells.first() {
                    Some((_, cell)) => match &cell.value {
                        Some(bytes) => match std::str::from_utf8(bytes) {
                            Ok(s) => s.to_string(),
                            Err(_) => continue,
                        },
                        None => continue, // tombstone cell
                    },
                    None => continue,
                };

                // Parse "keyspace.table" from the FQN.
                let (edge_ks, edge_tbl) = match edge_table_fqn.split_once('.') {
                    Some(pair) => pair,
                    None => continue,
                };
                let edge_tid = TableId::new(edge_ks, edge_tbl);

                // Determine the source and target based on direction.
                let (source_id, target_id) = if direction == DIRECTION_OUT {
                    (vertex_id.clone(), neighbor_id.clone())
                } else {
                    (neighbor_id.clone(), vertex_id.clone())
                };

                // Verify the edge exists in the edge table.
                let source_key = DecoratedKey::new(PartitionKey::new(source_id));
                let edge_exists = match write_path.read(&edge_tid, &source_key).await {
                    Ok(Some(p)) => p.rows.iter().any(|r| r.clustering == target_id),
                    _ => false,
                };

                if !edge_exists {
                    // Write a tombstone to remove this orphan adjacency entry.
                    if write_tombstone(
                        write_path,
                        &adj_table_id,
                        &partition.key,
                        &row.clustering,
                        &edge_label,
                    )
                    .await
                    .is_ok()
                    {
                        metrics.orphans_removed += 1;
                    }
                }
            }
        }
    }

    metrics
}

/// Check whether a specific adjacency entry exists for a vertex.
async fn adjacency_entry_exists(
    write_path: &WritePath,
    adj_table_id: &TableId,
    vertex_key: &DecoratedKey,
    direction: u8,
    edge_label: &str,
    neighbor_id: &[u8],
) -> bool {
    let partition = match write_path.read(adj_table_id, vertex_key).await {
        Ok(Some(p)) => p,
        _ => return false,
    };

    // Build the expected clustering prefix to match against.
    let mut expected_clustering = Vec::new();
    expected_clustering.push(direction);
    expected_clustering.extend_from_slice(&(edge_label.len() as u16).to_be_bytes());
    expected_clustering.extend_from_slice(edge_label.as_bytes());
    expected_clustering.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
    expected_clustering.extend_from_slice(neighbor_id);

    partition
        .rows
        .iter()
        .any(|row| row.clustering == expected_clustering)
}

/// Extract the edge label string from an adjacency clustering key.
///
/// Clustering format:
///   direction(1 byte) + edge_label_len(2 bytes BE) + edge_label + neighbor_id_len(2 bytes BE) + neighbor_id
fn extract_edge_label(clustering: &[u8]) -> Option<String> {
    if clustering.len() < 3 {
        return None;
    }
    let label_len = u16::from_be_bytes([clustering[1], clustering[2]]) as usize;
    if 3 + label_len > clustering.len() {
        return None;
    }
    std::str::from_utf8(&clustering[3..3 + label_len])
        .ok()
        .map(|s| s.to_string())
}

/// Write a mutation by decomposing it into individual row writes via WritePath.
async fn write_mutation(
    write_path: &WritePath,
    mutation: &ferrosa_storage::Mutation,
) -> ferrosa_common::Result<()> {
    let table_id = TableId::new(&mutation.keyspace, &mutation.table);
    for row in &mutation.rows {
        write_path
            .write(
                &table_id,
                &mutation.key,
                row.clone(),
                mutation.timestamp,
                ferrosa_cluster::consistency::ConsistencyLevel::One,
                &ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
                    replication_factor: 1,
                },
            )
            .await?;
    }
    Ok(())
}

/// Write a tombstone row for an orphan adjacency entry.
async fn write_tombstone(
    write_path: &WritePath,
    adj_table_id: &TableId,
    vertex_key: &DecoratedKey,
    clustering: &[u8],
    _edge_label: &str,
) -> ferrosa_common::Result<()> {
    use ferrosa_sstable::types::{LivenessInfo, Row};

    let now_us = now_micros();
    let now_secs = (now_us / 1_000_000) as u32;

    let tombstone_row = Row {
        clustering: clustering.to_vec(),
        cells: vec![],
        deletion: DeletionTime::new(now_us, now_secs),
        primary_key_liveness: LivenessInfo::NONE,
    };

    write_path
        .write(
            adj_table_id,
            vertex_key,
            tombstone_row,
            now_us,
            ferrosa_cluster::consistency::ConsistencyLevel::One,
            &ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await
}

/// Returns the current time in microseconds since epoch.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Spawn the background reconciliation loop.
pub fn spawn_reconciliation(
    schema: Arc<Schema>,
    write_path: Arc<WritePath>,
    keyspace: String,
    interval: Duration,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let metrics = reconcile_once(&schema, &write_path, &keyspace).await;
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
                _ = cancel.cancelled() => {
                    tracing::info!(keyspace = %keyspace, "reconciliation loop shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, SchemaConfig, TestAuditSink,
    };
    use ferrosa_sstable::types::{LivenessInfo, Row};
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use indexmap::IndexMap;

    fn test_storage_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap()
    }

    /// Register the edge table and adjacency table schemas with the storage engine,
    /// and register the edge table metadata with the schema registry.
    fn setup_edge_and_adjacency(
        schema: &Schema,
        storage: &StorageEngine,
        keyspace: &str,
        edge_table_name: &str,
    ) {
        // Register keyspace in schema registry.
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: keyspace.to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::new(),
                },
            })
            .unwrap();

        // Register the adjacency keyspace.
        let adj_ks = adjacency_keyspace_name(keyspace);
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: adj_ks.clone(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::new(),
                },
            })
            .unwrap();

        // Build edge table metadata with graph extensions.
        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "edge".to_string());
        extensions.insert("graph.label".to_string(), edge_table_name.to_uppercase());
        extensions.insert("graph.source".to_string(), "src_id".to_string());
        extensions.insert("graph.target".to_string(), "dst_id".to_string());

        let mut columns = IndexMap::new();
        columns.insert(
            "src_id".to_string(),
            ColumnMetadata {
                name: "src_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "blob".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "dst_id".to_string(),
            ColumnMetadata {
                name: "dst_id".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "blob".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );

        let mut flags = HashSet::new();
        flags.insert(TableFlag::Compound);

        let edge_meta = TableMetadata {
            keyspace: keyspace.to_string(),
            name: edge_table_name.to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["src_id".to_string()],
            clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags,
            extensions,
            is_system: false,
        };

        schema.create_table_internal(edge_meta).unwrap();

        // Register the adjacency table in the schema registry.
        let adj_table_meta = crate::adjacency::schema::adjacency_table_metadata(keyspace);
        schema.create_table_internal(adj_table_meta).unwrap();

        // Register edge table with storage engine.
        let edge_storage_schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: edge_table_name.to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "dst_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        };
        storage.register_table(edge_storage_schema).unwrap();

        // Register adjacency table with storage engine.
        let adj_storage_schema = TableSchema {
            keyspace: adj_ks.clone(),
            table: "adjacency".to_string(),
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_columns: vec![
                ColumnDefinition {
                    name: "direction".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.ByteType".to_string(),
                },
                ColumnDefinition {
                    name: "edge_label".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "neighbor_id".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
                },
            ],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "edge_table".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(adj_storage_schema).unwrap();
    }

    /// Write an edge row into the edge table in storage.
    fn write_edge(
        storage: &StorageEngine,
        keyspace: &str,
        table: &str,
        source: &[u8],
        target: &[u8],
    ) {
        let edge_tid = TableId::new(keyspace, table);
        let key = DecoratedKey::new(PartitionKey::new(source.to_vec()));
        let row = Row {
            clustering: target.to_vec(),
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        storage.write(&edge_tid, &key, row, 1000).unwrap();
    }

    #[test]
    fn reconcile_metrics_default_is_zero() {
        let m = ReconcileMetrics::default();
        assert_eq!(m.entries_checked, 0);
        assert_eq!(m.entries_repaired, 0);
        assert_eq!(m.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_repairs_missing_adjacency_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        // Write edge data: alice -> bob, alice -> carol.
        write_edge(&storage, "social", "knows", b"alice", b"bob");
        write_edge(&storage, "social", "knows", b"alice", b"carol");

        // No adjacency entries exist yet. Reconciliation should repair them.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;

        // 2 edge rows checked.
        assert_eq!(metrics.entries_checked, 2);
        // 2 edges x 2 directions (OUT + IN) = 4 repairs.
        assert_eq!(metrics.entries_repaired, 4);
        assert_eq!(metrics.orphans_removed, 0);

        // Verify adjacency entries were created.
        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Check alice's OUT entries.
        let alice_key = DecoratedKey::new(PartitionKey::new(b"alice".to_vec()));
        let alice_partition = storage.read(&adj_tid, &alice_key).unwrap().unwrap();
        assert!(alice_partition.rows.iter().any(|r| {
            r.clustering[0] == DIRECTION_OUT
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"bob".to_vec())
        }));
        assert!(alice_partition.rows.iter().any(|r| {
            r.clustering[0] == DIRECTION_OUT
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"carol".to_vec())
        }));

        // Check bob's IN entry.
        let bob_key = DecoratedKey::new(PartitionKey::new(b"bob".to_vec()));
        let bob_partition = storage.read(&adj_tid, &bob_key).unwrap().unwrap();
        assert!(bob_partition.rows.iter().any(|r| {
            r.clustering[0] == DIRECTION_IN
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"alice".to_vec())
        }));

        // Check carol's IN entry.
        let carol_key = DecoratedKey::new(PartitionKey::new(b"carol".to_vec()));
        let carol_partition = storage.read(&adj_tid, &carol_key).unwrap().unwrap();
        assert!(carol_partition.rows.iter().any(|r| {
            r.clustering[0] == DIRECTION_IN
                && extract_neighbor_id(&r.clustering, Some("knows")) == Some(b"alice".to_vec())
        }));
    }

    #[tokio::test]
    async fn reconcile_is_idempotent_when_entries_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        write_edge(&storage, "social", "knows", b"alice", b"bob");

        // First reconciliation creates the entries.
        let wp = WritePath::direct(storage.clone());
        let m1 = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(m1.entries_repaired, 2); // OUT + IN

        // Second reconciliation should find everything in order.
        let m2 = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(m2.entries_checked, 1);
        assert_eq!(m2.entries_repaired, 0);
        assert_eq!(m2.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_removes_orphan_adjacency_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Manually write an adjacency entry without a corresponding edge.
        let orphan_mutation = make_adjacency_mutation(
            &adj_ks,
            b"orphan_src",
            DIRECTION_OUT,
            "knows",
            b"orphan_dst",
            "social.knows",
            1000,
        );
        for row in &orphan_mutation.rows {
            storage
                .write(
                    &adj_tid,
                    &orphan_mutation.key,
                    row.clone(),
                    orphan_mutation.timestamp,
                )
                .unwrap();
        }

        // Verify the orphan entry exists before reconciliation.
        let orphan_key = DecoratedKey::new(PartitionKey::new(b"orphan_src".to_vec()));
        let before = storage.read(&adj_tid, &orphan_key).unwrap();
        assert!(before.is_some());
        assert!(!before.unwrap().rows.is_empty());

        // Run reconciliation — should detect and remove the orphan.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(metrics.orphans_removed, 1);
    }

    #[tokio::test]
    async fn reconcile_no_edge_tables_returns_zero_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        // No edge tables registered — should return zero metrics.
        let wp = WritePath::direct(storage);
        let metrics = reconcile_once(&schema, &wp, "nonexistent").await;
        assert_eq!(metrics.entries_checked, 0);
        assert_eq!(metrics.entries_repaired, 0);
        assert_eq!(metrics.orphans_removed, 0);
    }

    #[tokio::test]
    async fn reconcile_partial_repair_only_missing_direction() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = test_schema();

        setup_edge_and_adjacency(&schema, &storage, "social", "knows");

        // Write edge data.
        write_edge(&storage, "social", "knows", b"alice", b"bob");

        let adj_ks = adjacency_keyspace_name("social");
        let adj_tid = TableId::new(&adj_ks, "adjacency");

        // Manually write only the OUT adjacency entry.
        let out_mutation = make_adjacency_mutation(
            &adj_ks,
            b"alice",
            DIRECTION_OUT,
            "knows",
            b"bob",
            "social.knows",
            1000,
        );
        for row in &out_mutation.rows {
            storage
                .write(
                    &adj_tid,
                    &out_mutation.key,
                    row.clone(),
                    out_mutation.timestamp,
                )
                .unwrap();
        }

        // Reconciliation should only repair the missing IN entry.
        let wp = WritePath::direct(storage.clone());
        let metrics = reconcile_once(&schema, &wp, "social").await;
        assert_eq!(metrics.entries_checked, 1);
        assert_eq!(metrics.entries_repaired, 1); // Only the IN entry was missing.
    }

    #[test]
    fn extract_edge_label_parses_correctly() {
        let mut clustering = Vec::new();
        clustering.push(DIRECTION_OUT);
        let label = b"KNOWS";
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        let neighbor = b"bob";
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(neighbor);

        assert_eq!(extract_edge_label(&clustering), Some("KNOWS".to_string()));
    }

    #[test]
    fn extract_edge_label_too_short() {
        assert_eq!(extract_edge_label(&[0, 0]), None);
    }
}
