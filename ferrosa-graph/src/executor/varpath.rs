//! Variable-length path traversal executor.
//!
//! Executes `PhysicalPlan::ExpandVarLength` plans by performing BFS traversal
//! from an anchor through repeated hops within the specified min/max range.
//! Uses a visited set for cycle detection (FMEA F2) and a total vertex
//! budget for DoS protection (threat T13, FMEA F3).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::VirtualTableRegistry;
use ferrosa_storage::TableId;

use crate::adjacency::schema::adjacency_keyspace_name;
use crate::error::{GraphError, Result};
use crate::executor::eval;
use crate::executor::expand::{
    build_columns, check_timeout, extract_column_bytes_from_row, extract_neighbor_id,
    graph_vertex_lookup_key, row_to_json, sort_rows, table_metadata_for, GraphEngineConfig,
};
use crate::executor::result::{GraphResult, QueryStats};
use crate::parser::Direction;
use crate::parser::ReturnClause;
use crate::planner::physical::{Anchor, Hop};

/// Execute a variable-length path expansion using BFS.
///
/// Traverses from anchor vertices through `min_hops..=max_hops` repetitions
/// of the given hop. Uses a visited set for cycle detection and a total
/// vertex budget for DoS protection (threat T13, FMEA F3).
#[allow(clippy::too_many_arguments)]
pub async fn execute_var_length(
    write_path: &WritePath,
    keyspace: &str,
    anchor: &Anchor,
    hop: &Hop,
    min_hops: u32,
    max_hops: u32,
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();

    // Step 1: Anchor lookup — same as execute_expand.
    let is_virtual = virtual_tables
        .map(|vt| {
            vt.get(&anchor.table.keyspace, &anchor.table.table)
                .is_some()
        })
        .unwrap_or(false);

    let anchor_table_id = TableId::new(&anchor.table.keyspace, &anchor.table.table);

    let anchor_partitions = if is_virtual {
        // Virtual tables are not supported for variable-length paths yet.
        // Fall back to storage (which will return empty for virtual tables).
        vec![]
    } else {
        write_path.range_read(&anchor_table_id).await?
    };
    stats.vertices_read += anchor_partitions.len();
    check_timeout(start, config.query_timeout)?;

    // Apply WHERE filters to anchor partitions.
    let anchor_var = anchor.var.as_deref().unwrap_or("_anon");
    let mut seed_keys: Vec<DecoratedKey> = Vec::with_capacity(anchor_partitions.len());
    let anchor_meta = table_metadata_for(schema, &anchor.table.keyspace, &anchor.table.table);
    for partition in &anchor_partitions {
        if let Some(meta) = anchor_meta.as_ref() {
            for row in &partition.rows {
                let row_json = row_to_json(meta, partition, row);
                let mut bindings = HashMap::new();
                bindings.insert(anchor_var.to_string(), row_json.clone());
                if !anchor.props.is_empty() {
                    let mut props_pass = true;
                    for (prop, expected) in &anchor.props {
                        let filter = crate::parser::Expr::Comparison {
                            left: Box::new(crate::parser::Expr::Property {
                                var: anchor_var.to_string(),
                                name: prop.clone(),
                            }),
                            op: crate::parser::CompareOp::Eq,
                            right: Box::new(expected.clone()),
                        };
                        if !eval::filter_passes(&filter, &bindings)? {
                            props_pass = false;
                            break;
                        }
                    }
                    if !props_pass {
                        continue;
                    }
                }

                let mut passes = true;
                for filter in &anchor.filters {
                    if !eval::filter_passes(filter, &bindings)? {
                        passes = false;
                        break;
                    }
                }
                if passes {
                    seed_keys.push(
                        graph_vertex_lookup_key(meta, partition, row, &anchor.props)
                            .unwrap_or_else(|| partition.key.clone()),
                    );
                }
            }
        } else {
            let anchor_col_names = super::expand::column_names_for_table(
                schema,
                &anchor.table.keyspace,
                &anchor.table.table,
            );
            let hex_id = hex::encode(partition.key.key.as_bytes());
            let row_json = eval::partition_to_json(partition, &hex_id, &anchor_col_names);
            let mut bindings = HashMap::new();
            bindings.insert(anchor_var.to_string(), row_json);

            let mut passes = true;
            for filter in &anchor.filters {
                if !eval::filter_passes(filter, &bindings)? {
                    passes = false;
                    break;
                }
            }
            if passes {
                seed_keys.push(partition.key.clone());
            }
        }
    }

    // Step 2: BFS traversal.
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");
    let fallback_edge_table_id = hop
        .edge_table
        .as_ref()
        .map(|edge_table| TableId::new(&edge_table.keyspace, &edge_table.table));

    // Track all visited vertex keys (as bytes) for cycle detection (FMEA F2).
    // We mark frontier vertices as visited before expanding them, so the
    // initial seed keys are added here.
    let mut visited: HashSet<Vec<u8>> = HashSet::new();

    // Current frontier — the vertices at the current BFS depth.
    let mut frontier: Vec<DecoratedKey> = seed_keys;

    // Mark initial frontier (anchors) as visited for cycle detection.
    for key in &frontier {
        visited.insert(key.key.as_bytes().to_vec());
    }

    // Collect result vertices: all vertices reachable at depths [min_hops, max_hops].
    let mut result_keys: Vec<DecoratedKey> = Vec::new();

    // If min_hops is 0, include the anchor vertices in the result.
    if min_hops == 0 {
        result_keys.extend(frontier.clone());
    }

    for depth in 1..=max_hops {
        check_timeout(start, config.query_timeout)?;

        let mut next_frontier: Vec<DecoratedKey> = Vec::new();

        for vertex_key in &frontier {
            // Read adjacency entries for this vertex. If adjacency has not been
            // materialized yet (for example in tiny HTTP seam tests), fall back
            // to the edge table's source-partition rows.
            let mut neighbor_ids = Vec::new();
            let adj_partition = write_path.read(&adj_table_id, vertex_key).await?;
            if let Some(partition) = adj_partition {
                stats.edges_read += partition.rows.len();
                for row in &partition.rows {
                    if let Some(neighbor_id) =
                        extract_neighbor_id(&row.clustering, hop.edge_label.as_deref())
                    {
                        neighbor_ids.push(neighbor_id);
                    }
                }
            }

            if neighbor_ids.is_empty() {
                if let (Some(edge_table_id), Some(edge_table)) =
                    (&fallback_edge_table_id, hop.edge_table.as_ref())
                {
                    if let Some(meta) =
                        table_metadata_for(schema, &edge_table.keyspace, &edge_table.table)
                    {
                        let Some(source_col) = meta.extensions.get("graph.source") else {
                            continue;
                        };
                        let Some(target_col) = meta.extensions.get("graph.target") else {
                            continue;
                        };
                        for partition in write_path.range_read(edge_table_id).await? {
                            stats.edges_read += partition.rows.len();
                            for row in &partition.rows {
                                let source = extract_column_bytes_from_row(
                                    &meta,
                                    partition.key.key.as_bytes(),
                                    row,
                                    source_col,
                                );
                                let target = extract_column_bytes_from_row(
                                    &meta,
                                    partition.key.key.as_bytes(),
                                    row,
                                    target_col,
                                );
                                let current = vertex_key.key.as_bytes();
                                match hop.direction {
                                    Direction::Out if source.as_deref() == Some(current) => {
                                        if let Some(target) = target {
                                            neighbor_ids.push(target);
                                        }
                                    }
                                    Direction::In if target.as_deref() == Some(current) => {
                                        if let Some(source) = source {
                                            neighbor_ids.push(source);
                                        }
                                    }
                                    Direction::Both => {
                                        if source.as_deref() == Some(current) {
                                            if let Some(target) = target.clone() {
                                                neighbor_ids.push(target);
                                            }
                                        }
                                        if target.as_deref() == Some(current) {
                                            if let Some(source) = source {
                                                neighbor_ids.push(source);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }

            for neighbor_id in neighbor_ids {
                // Cycle detection (FMEA F2): skip already-visited vertices.
                if visited.contains(&neighbor_id) {
                    continue;
                }

                // Budget check (FMEA F3, threat T13): cap total visited.
                if visited.len() >= config.max_var_path_visited {
                    return Err(GraphError::ResourceLimit(format!(
                        "variable-length path visited vertex budget exceeded: {} (limit: {})",
                        visited.len(),
                        config.max_var_path_visited
                    )));
                }

                visited.insert(neighbor_id.clone());
                next_frontier.push(DecoratedKey::new(PartitionKey::new(neighbor_id)));
            }
        }

        stats.vertices_read += next_frontier.len();

        // Collect result vertices if we are at or past min_hops.
        if depth >= min_hops {
            result_keys.extend(next_frontier.clone());
        }

        // If the frontier is empty, BFS is complete (no more vertices to explore).
        if next_frontier.is_empty() {
            break;
        }

        frontier = next_frontier;
    }

    // Step 3: Build result from return clause, projecting property values.
    let columns = build_columns(return_clause);

    // Determine which table to read full vertex data from. If the hop has a
    // vertex_table, use that; otherwise fall back to the anchor table.
    let proj_table_id = if let Some(ref vt) = hop.vertex_table {
        TableId::new(&vt.keyspace, &vt.table)
    } else {
        anchor_table_id
    };

    // Resolve column names for the projection table (may differ from anchor).
    let proj_col_names = if let Some(ref vt) = hop.vertex_table {
        super::expand::column_names_for_table(schema, &vt.keyspace, &vt.table)
    } else {
        super::expand::column_names_for_table(schema, &anchor.table.keyspace, &anchor.table.table)
    };

    // Determine the variable name for result binding.
    let result_var = hop.var.as_deref().unwrap_or(anchor_var);

    // No `max_result_rows` break here: it SILENTLY TRUNCATED the result — no
    // error, no flag — so a client could not tell a partial answer from a
    // complete one. Removed with the same reasoning as the expand path
    // (t_4ce82a3e): an unbounded ORDER BY spills below instead of being capped.
    // Traversal is still bounded by `max_var_path_visited`, which limits WORK
    // (a DoS control), not the result buffer.
    let mut rows = Vec::new();
    for key in &result_keys {
        let partition = write_path.read(&proj_table_id, key).await?;
        let hex_id = hex::encode(key.key.as_bytes());

        let row_json = if let Some(ref part) = partition {
            eval::partition_to_json(part, &hex_id, &proj_col_names)
        } else {
            serde_json::Value::String(hex_id.clone())
        };
        let mut bindings = HashMap::new();
        bindings.insert(result_var.to_string(), row_json.clone());
        if !hop.target_props.is_empty() {
            let mut target_passes = true;
            for (prop, expected) in &hop.target_props {
                let filter = crate::parser::Expr::Comparison {
                    left: Box::new(crate::parser::Expr::Property {
                        var: result_var.to_string(),
                        name: prop.clone(),
                    }),
                    op: crate::parser::CompareOp::Eq,
                    right: Box::new(expected.clone()),
                };
                if !eval::filter_passes(&filter, &bindings)? {
                    target_passes = false;
                    break;
                }
            }
            if !target_passes {
                continue;
            }
        }
        // Also bind the anchor var so expressions referencing it work.
        if result_var != anchor_var {
            bindings.insert(anchor_var.to_string(), row_json);
        }

        let row: Vec<serde_json::Value> = return_clause
            .items
            .iter()
            .map(|item| eval::eval_expr(&item.expr, &bindings).unwrap_or(serde_json::Value::Null))
            .collect();
        rows.push(row);
    }

    // Apply ORDER BY. An unbounded one spills through the storage engine's
    // bounded external merge sort; `spill_order_by` returns false (leaving the
    // in-memory sort) when it cannot apply — no backend, a LIMIT is present, or
    // an order term is not a projected column.
    if !return_clause.order_by.is_empty() {
        let spilled =
            crate::executor::spill::spill_order_by(&mut rows, &columns, return_clause, config)
                .await?;
        if !spilled {
            sort_rows(&mut rows, &columns, &return_clause.order_by);
        }
    }

    // Apply DISTINCT.
    if return_clause.distinct {
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        rows.dedup();
    }

    // Apply LIMIT.
    if let Some(limit) = return_clause.limit {
        let limit = limit.max(0) as usize;
        rows.truncate(limit);
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns,
        rows,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::physical::Anchor;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use std::sync::Arc;
    use std::time::Duration;

    use crate::adjacency::schema::adjacency_keyspace_name;
    use crate::parser::{Direction, Expr, ReturnClause, ReturnItem};
    use crate::planner::logical::ResolvedTable;
    use crate::planner::physical::Hop;

    fn test_config() -> GraphEngineConfig {
        GraphEngineConfig {
            query_timeout: Duration::from_secs(5),
            max_result_rows: 1000,
            max_fan_out_per_hop: 1000,
            max_groups: 1000,
            max_collect_size: 1000,
            max_var_path_visited: 100,
            ..GraphEngineConfig::default()
        }
    }

    fn person_table() -> ResolvedTable {
        ResolvedTable {
            keyspace: "test_ks".to_string(),
            table: "person_v".to_string(),
            graph_type: "vertex".to_string(),
            label: "Person".to_string(),
        }
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: Default::default(),
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
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_vertex_schema(keyspace: &str, table: &str) -> TableSchema {
        // Vertex partitions have no clustering columns: the test's
        // `write_vertex` writes a partition-only row (clustering: vec![]).
        TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        }
    }

    fn make_adjacency_schema(keyspace: &str, table: &str) -> TableSchema {
        // Adjacency rows carry composite clustering: see `adjacency_clustering`.
        TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        }
    }

    /// Create storage and register the vertex table and adjacency table.
    fn setup_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let storage = test_storage(dir);
        // Register vertex table.
        storage
            .register_table(make_vertex_schema("test_ks", "person_v"))
            .unwrap();
        // Register adjacency table.
        let adj_ks = adjacency_keyspace_name("test_ks");
        storage
            .register_table(make_adjacency_schema(&adj_ks, "adjacency"))
            .unwrap();
        storage
    }

    /// Write a vertex partition for the given key.
    fn write_vertex(storage: &StorageEngine, keyspace: &str, table: &str, key_bytes: &[u8]) {
        let table_id = TableId::new(keyspace, table);
        let dk = DecoratedKey::new(PartitionKey::new(key_bytes.to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        storage.write(&table_id, &dk, row, 1).unwrap();
    }

    /// Build an adjacency row clustering key in standard composite layout:
    /// [u16 1][1B direction][u16 label_len][label][u16 id_len][id].
    fn adjacency_clustering(label: &str, neighbor_id: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_be_bytes()); // direction component len
        out.push(0x01); // direction byte (out)
        let label_bytes = label.as_bytes();
        out.extend_from_slice(&(label_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(label_bytes);
        out.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
        out.extend_from_slice(neighbor_id);
        out
    }

    /// Write an adjacency entry: from `src` to `dst` with the given edge label.
    fn write_adjacency(
        storage: &StorageEngine,
        keyspace: &str,
        src: &[u8],
        label: &str,
        dst: &[u8],
    ) {
        let adj_ks = adjacency_keyspace_name(keyspace);
        let adj_table_id = TableId::new(&adj_ks, "adjacency");
        let dk = DecoratedKey::new(PartitionKey::new(src.to_vec()));
        let clustering = adjacency_clustering(label, dst);
        let row = Row {
            clustering,
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        storage.write(&adj_table_id, &dk, row, 1).unwrap();
    }

    fn make_anchor(var: &str) -> Anchor {
        Anchor {
            var: Some(var.to_string()),
            table: person_table(),
            props: vec![],
            filters: vec![],
        }
    }

    fn make_hop(var: &str) -> Hop {
        Hop {
            var: Some(var.to_string()),
            rel_var: None,
            edge_label: Some("KNOWS".to_string()),
            direction: Direction::Out,
            edge_table: None,
            vertex_table: Some(person_table()),
            prop_filters: vec![],
            target_props: vec![],
        }
    }

    fn simple_return(var: &str) -> ReturnClause {
        ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Var(var.to_string()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        }
    }

    #[tokio::test]
    async fn varpath_single_hop() {
        // Graph: A -> B (only A is written to person_v; B is discovered via adjacency)
        let tmp = tempfile::tempdir().unwrap();
        let storage = setup_storage(tmp.path());

        // Only write vertex A to the anchor table so that A is the sole anchor.
        write_vertex(&storage, "test_ks", "person_v", b"A");
        write_adjacency(&storage, "test_ks", b"A", "KNOWS", b"B");

        let wp = WritePath::direct(storage);
        let result = execute_var_length(
            &wp,
            "test_ks",
            &make_anchor("a"),
            &make_hop("b"),
            1,
            1,
            &simple_return("b"),
            &test_config(),
            Instant::now(),
            None,
            None,
        )
        .await
        .unwrap();

        // Should find exactly B reachable from A in 1 hop.
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn varpath_cycle_terminates() {
        // Graph: A -> B -> C -> A (cycle)
        // Only A is in the anchor table; B and C are discovered via BFS.
        let tmp = tempfile::tempdir().unwrap();
        let storage = setup_storage(tmp.path());

        write_vertex(&storage, "test_ks", "person_v", b"A");
        write_adjacency(&storage, "test_ks", b"A", "KNOWS", b"B");
        write_adjacency(&storage, "test_ks", b"B", "KNOWS", b"C");
        write_adjacency(&storage, "test_ks", b"C", "KNOWS", b"A");

        let wp = WritePath::direct(storage);
        let result = execute_var_length(
            &wp,
            "test_ks",
            &make_anchor("a"),
            &make_hop("b"),
            1,
            10,
            &simple_return("b"),
            &test_config(),
            Instant::now(),
            None,
            None,
        )
        .await
        .unwrap();

        // A->B at depth 1, B->C at depth 2, C->A at depth 3 is cycle (A visited).
        // Result should have B and C (2 vertices).
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn varpath_budget_exceeded() {
        // Create a chain of vertices that exceeds the visited budget.
        // Only V0000 is in the anchor table; rest are discovered via BFS.
        let tmp = tempfile::tempdir().unwrap();
        let storage = setup_storage(tmp.path());

        let mut config = test_config();
        config.max_var_path_visited = 5;

        // Write only the first vertex to the anchor table.
        write_vertex(&storage, "test_ks", "person_v", b"V0000");

        // Write adjacency chain: V0000->V0001->V0002->...->V0019
        for i in 0..20u32 {
            if i > 0 {
                let prev = format!("V{:04}", i - 1);
                let key = format!("V{i:04}");
                write_adjacency(
                    &storage,
                    "test_ks",
                    prev.as_bytes(),
                    "KNOWS",
                    key.as_bytes(),
                );
            }
        }

        let anchor = Anchor {
            var: Some("a".to_string()),
            table: person_table(),
            props: vec![],
            filters: vec![],
        };

        let wp = WritePath::direct(storage);
        let result = execute_var_length(
            &wp,
            "test_ks",
            &anchor,
            &make_hop("b"),
            1,
            10,
            &simple_return("b"),
            &config,
            Instant::now(),
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("budget exceeded"),
            "expected budget exceeded error, got: {err}"
        );
    }
}
