//! Expand executor: traverses graph patterns via the adjacency index.
//!
//! Executes a `PhysicalPlan::Expand` by:
//! 1. Looking up the anchor vertex via `storage.read_range()` (or
//!    `VirtualTable::read()` if the source is a virtual table)
//! 2. For each hop, reading the adjacency index to find neighbors
//! 3. Building a `GraphResult` with columns from the return clause

use std::time::{Duration, Instant};

use ferrosa_common::DecoratedKey;
use ferrosa_schema::VirtualTableRegistry;
use ferrosa_storage::{StorageEngine, TableId};

use crate::adjacency::schema::adjacency_keyspace_name;
use crate::error::{GraphError, Result};
use crate::executor::result::{GraphResult, QueryStats};
use crate::parser::{Expr, ReturnClause, ReturnItem};
use crate::planner::physical::{Anchor, Hop, PhysicalPlan};

/// Configuration for the graph query engine (T4 DoS limits).
#[derive(Debug, Clone)]
pub struct GraphEngineConfig {
    /// Maximum query execution time.
    pub query_timeout: Duration,
    /// Maximum number of result rows.
    pub max_result_rows: usize,
    /// Maximum fan-out per hop (T4: limits adjacency reads).
    pub max_fan_out_per_hop: usize,
}

impl Default for GraphEngineConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_result_rows: 10_000,
            max_fan_out_per_hop: 10_000,
        }
    }
}

/// Execute a physical plan against the storage engine.
///
/// If `virtual_tables` is provided, the executor checks the registry before
/// going to storage for anchor lookups. Virtual tables (e.g. in
/// `system_observability`) return rows directly from memory.
pub fn execute(
    plan: PhysicalPlan,
    storage: &StorageEngine,
    keyspace: &str,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
) -> Result<GraphResult> {
    let start = Instant::now();

    match plan {
        PhysicalPlan::Expand {
            anchor,
            hops,
            return_clause,
        } => execute_expand(
            storage,
            keyspace,
            &anchor,
            &hops,
            &return_clause,
            config,
            start,
            virtual_tables,
        ),
    }
}

/// Execute an Expand plan.
#[allow(clippy::too_many_arguments)]
fn execute_expand(
    storage: &StorageEngine,
    keyspace: &str,
    anchor: &Anchor,
    hops: &[Hop],
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();

    // Step 1: Anchor lookup.
    // Check if the anchor table is a virtual table before going to storage.
    let is_virtual = virtual_tables
        .map(|vt| {
            vt.get(&anchor.table.keyspace, &anchor.table.table)
                .is_some()
        })
        .unwrap_or(false);

    if is_virtual {
        // Virtual table path: read rows directly from the registry.
        return execute_virtual_anchor(
            virtual_tables.expect("checked above"),
            anchor,
            return_clause,
            config,
            start,
        );
    }

    // Storage path: read all partitions from the anchor table.
    let anchor_table_id = TableId::new(&anchor.table.keyspace, &anchor.table.table);
    let anchor_partitions =
        storage.read_range(&anchor_table_id, None, None, config.max_result_rows)?;
    stats.vertices_read += anchor_partitions.len();
    check_timeout(start, config.query_timeout)?;

    // Collect anchor vertex keys.
    let mut current_keys: Vec<DecoratedKey> =
        anchor_partitions.iter().map(|p| p.key.clone()).collect();

    // Step 2: For each hop, traverse adjacency index.
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    for hop in hops {
        check_timeout(start, config.query_timeout)?;

        let mut next_keys = Vec::new();
        for vertex_key in &current_keys {
            // Read adjacency entries for this vertex.
            let adj_partition = storage.read(&adj_table_id, vertex_key)?;
            if let Some(partition) = adj_partition {
                stats.edges_read += partition.rows.len();

                for row in &partition.rows {
                    if let Some(neighbor_id) =
                        extract_neighbor_id(&row.clustering, hop.edge_label.as_deref())
                    {
                        next_keys.push(DecoratedKey::new(ferrosa_common::PartitionKey::new(
                            neighbor_id,
                        )));
                    }
                }

                // T4: fan-out limit per hop.
                if next_keys.len() > config.max_fan_out_per_hop {
                    return Err(GraphError::ResourceLimit(format!(
                        "fan-out limit exceeded: {} neighbors (limit: {})",
                        next_keys.len(),
                        config.max_fan_out_per_hop
                    )));
                }
            }
        }

        stats.vertices_read += next_keys.len();
        current_keys = next_keys;
    }

    // Step 3: Build result from return clause.
    let columns = build_columns(return_clause);

    // For Phase 1: return vertex IDs as hex strings.
    let mut rows = Vec::new();
    for key in &current_keys {
        if rows.len() >= config.max_result_rows {
            break;
        }
        let hex_id = hex::encode(key.key.as_bytes());
        // Each row has one value per column. For Phase 1, all columns
        // get the vertex ID hex string.
        let row: Vec<serde_json::Value> = columns
            .iter()
            .map(|_| serde_json::Value::String(hex_id.clone()))
            .collect();
        rows.push(row);
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns,
        rows,
        stats,
    })
}

/// Execute an anchor lookup against a virtual table.
///
/// Virtual tables return `VirtualRow` instances with `CellValue` cells.
/// Each cell's byte value is converted to a JSON string for the graph result.
/// Virtual tables do not support hop traversal — they are leaf sources only.
fn execute_virtual_anchor(
    registry: &VirtualTableRegistry,
    anchor: &Anchor,
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();

    let vtable = registry
        .get(&anchor.table.keyspace, &anchor.table.table)
        .ok_or_else(|| {
            GraphError::Internal(format!(
                "virtual table {}.{} disappeared from registry",
                anchor.table.keyspace, anchor.table.table
            ))
        })?;

    let virtual_rows = vtable.read(None);
    stats.vertices_read = virtual_rows.len();
    check_timeout(start, config.query_timeout)?;

    let vtable_columns = vtable.columns();
    let return_columns = build_columns(return_clause);

    let mut rows = Vec::new();
    for vrow in &virtual_rows {
        if rows.len() >= config.max_result_rows {
            break;
        }

        let row: Vec<serde_json::Value> = return_columns
            .iter()
            .map(|col_name| {
                // Try to find a matching column in the virtual table's schema.
                // Column references may be "var.prop" (e.g. "n.peer_address")
                // or just "prop" — extract the property name after the dot.
                let prop_name = col_name.split('.').next_back().unwrap_or(col_name);

                let col_idx = vtable_columns.iter().position(|c| c.name == prop_name);

                match col_idx {
                    Some(idx) if idx < vrow.cells.len() => cell_value_to_json(&vrow.cells[idx]),
                    _ => serde_json::Value::Null,
                }
            })
            .collect();

        rows.push(row);
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: return_columns,
        rows,
        stats,
    })
}

/// Convert a `CellValue` to a JSON value for graph result output.
///
/// Live cells with byte values are converted to UTF-8 strings if possible,
/// otherwise to hex-encoded strings. Tombstones become `null`.
fn cell_value_to_json(cell: &ferrosa_common::CellValue) -> serde_json::Value {
    match &cell.value {
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::Value::String(hex::encode(bytes)),
        },
        None => serde_json::Value::Null,
    }
}

/// Build column names from the return clause.
fn build_columns(return_clause: &ReturnClause) -> Vec<String> {
    return_clause
        .items
        .iter()
        .map(column_name_for_item)
        .collect()
}

/// Determine the column name for a return item.
fn column_name_for_item(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    expr_to_column_name(&item.expr)
}

/// Convert an expression to a display-friendly column name.
fn expr_to_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Property { var, name } => format!("{var}.{name}"),
        Expr::Var(v) => v.clone(),
        _ => "?".to_string(),
    }
}

/// Extract the neighbor ID from an adjacency row's clustering key.
///
/// Clustering format:
///   direction(1 byte) + edge_label_len(2 bytes BE) + edge_label + neighbor_id_len(2 bytes BE) + neighbor_id
///
/// If `expected_label` is Some, only returns the neighbor ID if the edge label matches.
pub fn extract_neighbor_id(clustering: &[u8], expected_label: Option<&str>) -> Option<Vec<u8>> {
    // Minimum: 1 (direction) + 2 (label_len) + 0 (label) + 2 (id_len) + 0 (id)
    if clustering.len() < 5 {
        return None;
    }

    let mut pos = 1; // skip direction byte

    // Read edge label length (2 bytes BE).
    let label_len = u16::from_be_bytes([clustering[pos], clustering[pos + 1]]) as usize;
    pos += 2;

    if pos + label_len > clustering.len() {
        return None;
    }

    let label_bytes = &clustering[pos..pos + label_len];
    pos += label_len;

    // Check label filter.
    if let Some(expected) = expected_label {
        let label_str = std::str::from_utf8(label_bytes).ok()?;
        if !label_str.eq_ignore_ascii_case(expected) {
            return None;
        }
    }

    // Read neighbor ID length (2 bytes BE).
    if pos + 2 > clustering.len() {
        return None;
    }
    let id_len = u16::from_be_bytes([clustering[pos], clustering[pos + 1]]) as usize;
    pos += 2;

    if pos + id_len > clustering.len() {
        return None;
    }

    Some(clustering[pos..pos + id_len].to_vec())
}

/// Check whether the query has exceeded its timeout.
pub fn check_timeout(start: Instant, timeout: Duration) -> Result<()> {
    if start.elapsed() > timeout {
        return Err(GraphError::Timeout);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ferrosa_common::CellValue;
    use ferrosa_common::DataType;
    use ferrosa_schema::virtual_table::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
    };

    /// A test virtual table that returns a fixed set of rows.
    #[derive(Debug)]
    struct TestVirtualTable {
        table_name: String,
        ks: String,
        cols: Vec<VirtualColumnDef>,
        rows: Vec<VirtualRow>,
    }

    impl VirtualTable for TestVirtualTable {
        fn name(&self) -> &str {
            &self.table_name
        }
        fn keyspace(&self) -> &str {
            &self.ks
        }
        fn columns(&self) -> &[VirtualColumnDef] {
            &self.cols
        }
        fn primary_key_columns(&self) -> &[usize] {
            &[0]
        }
        fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
            self.rows.clone()
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    /// Build a plan whose anchor points at the given keyspace/table.
    fn virtual_anchor_plan(
        keyspace: &str,
        table: &str,
        return_properties: &[(&str, &str)],
    ) -> PhysicalPlan {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        use crate::planner::logical::ResolvedTable;
        use crate::planner::physical::Anchor;

        let items: Vec<ReturnItem> = return_properties
            .iter()
            .map(|(var, prop)| ReturnItem {
                expr: Expr::Property {
                    var: var.to_string(),
                    name: prop.to_string(),
                },
                alias: None,
            })
            .collect();

        PhysicalPlan::Expand {
            anchor: Anchor {
                var: Some("n".to_string()),
                table: ResolvedTable {
                    keyspace: keyspace.to_string(),
                    table: table.to_string(),
                    graph_type: "vertex".to_string(),
                    label: "Connections".to_string(),
                },
                filters: vec![],
            },
            hops: vec![],
            return_clause: ReturnClause {
                distinct: false,
                items,
                order_by: vec![],
                limit: None,
            },
        }
    }

    #[test]
    fn execute_virtual_table_anchor_returns_rows() {
        let registry = VirtualTableRegistry::new();
        let vtable = Arc::new(TestVirtualTable {
            table_name: "connections".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![
                VirtualColumnDef {
                    name: "peer_address".to_string(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "state".to_string(),
                    data_type: DataType::Text,
                },
            ],
            rows: vec![
                VirtualRow {
                    cells: vec![
                        CellValue::live(b"10.0.0.1".to_vec(), 1000),
                        CellValue::live(b"ready".to_vec(), 1000),
                    ],
                },
                VirtualRow {
                    cells: vec![
                        CellValue::live(b"10.0.0.2".to_vec(), 1000),
                        CellValue::live(b"startup".to_vec(), 1000),
                    ],
                },
            ],
        });
        registry.register(vtable);

        let plan = virtual_anchor_plan(
            "system_observability",
            "connections",
            &[("n", "peer_address"), ("n", "state")],
        );

        // Create a dummy storage engine for the call (won't be used).
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let result = execute(
            plan,
            &storage,
            "system_observability",
            &config,
            Some(&registry),
        )
        .unwrap();

        assert_eq!(result.columns, vec!["n.peer_address", "n.state"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0],
            vec![
                serde_json::Value::String("10.0.0.1".to_string()),
                serde_json::Value::String("ready".to_string()),
            ]
        );
        assert_eq!(
            result.rows[1],
            vec![
                serde_json::Value::String("10.0.0.2".to_string()),
                serde_json::Value::String("startup".to_string()),
            ]
        );
        assert_eq!(result.stats.vertices_read, 2);
    }

    #[test]
    fn execute_virtual_table_missing_column_returns_null() {
        let registry = VirtualTableRegistry::new();
        let vtable = Arc::new(TestVirtualTable {
            table_name: "connections".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![VirtualColumnDef {
                name: "peer_address".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(b"10.0.0.1".to_vec(), 1000)],
            }],
        });
        registry.register(vtable);

        // Request a column that doesn't exist in the virtual table.
        let plan = virtual_anchor_plan(
            "system_observability",
            "connections",
            &[("n", "peer_address"), ("n", "nonexistent")],
        );

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let result = execute(
            plan,
            &storage,
            "system_observability",
            &config,
            Some(&registry),
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            serde_json::Value::String("10.0.0.1".to_string())
        );
        assert_eq!(result.rows[0][1], serde_json::Value::Null);
    }

    #[test]
    fn execute_virtual_table_tombstone_returns_null() {
        let registry = VirtualTableRegistry::new();
        let vtable = Arc::new(TestVirtualTable {
            table_name: "test_table".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![VirtualColumnDef {
                name: "val".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::tombstone(1000, 1700000000)],
            }],
        });
        registry.register(vtable);

        let plan = virtual_anchor_plan("system_observability", "test_table", &[("n", "val")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let result = execute(
            plan,
            &storage,
            "system_observability",
            &config,
            Some(&registry),
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], serde_json::Value::Null);
    }

    #[test]
    fn execute_virtual_table_empty_returns_no_rows() {
        let registry = VirtualTableRegistry::new();
        let vtable = Arc::new(TestVirtualTable {
            table_name: "empty_table".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![VirtualColumnDef {
                name: "val".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![],
        });
        registry.register(vtable);

        let plan = virtual_anchor_plan("system_observability", "empty_table", &[("n", "val")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let result = execute(
            plan,
            &storage,
            "system_observability",
            &config,
            Some(&registry),
        )
        .unwrap();

        assert!(result.rows.is_empty());
        assert_eq!(result.stats.vertices_read, 0);
    }

    #[test]
    fn execute_non_virtual_table_falls_through_to_storage() {
        // When virtual tables registry exists but table is NOT registered,
        // execution should fall through to the normal storage path.
        let registry = VirtualTableRegistry::new();
        // Don't register any tables.

        let plan = virtual_anchor_plan("social", "person_v", &[("n", "name")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        // This should succeed (empty result from storage, not error).
        let result = execute(plan, &storage, "social", &config, Some(&registry)).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn execute_without_virtual_registry_falls_through() {
        // When no virtual table registry is provided, execute normally.
        let plan = virtual_anchor_plan("social", "person_v", &[("n", "name")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let result = execute(plan, &storage, "social", &config, None).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn execute_virtual_table_respects_max_rows() {
        let registry = VirtualTableRegistry::new();
        // Create a virtual table with many rows.
        let rows: Vec<VirtualRow> = (0..100)
            .map(|i| VirtualRow {
                cells: vec![CellValue::live(format!("row_{i}").into_bytes(), 1000)],
            })
            .collect();

        let vtable = Arc::new(TestVirtualTable {
            table_name: "big_table".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![VirtualColumnDef {
                name: "val".to_string(),
                data_type: DataType::Text,
            }],
            rows,
        });
        registry.register(vtable);

        let plan = virtual_anchor_plan("system_observability", "big_table", &[("n", "val")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig {
            max_result_rows: 5,
            ..Default::default()
        };

        let result = execute(
            plan,
            &storage,
            "system_observability",
            &config,
            Some(&registry),
        )
        .unwrap();
        assert_eq!(result.rows.len(), 5);
    }

    #[test]
    fn cell_value_to_json_utf8() {
        let cell = CellValue::live(b"hello".to_vec(), 1000);
        let json = cell_value_to_json(&cell);
        assert_eq!(json, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn cell_value_to_json_non_utf8_hex() {
        let cell = CellValue::live(vec![0xFF, 0xFE], 1000);
        let json = cell_value_to_json(&cell);
        assert_eq!(json, serde_json::Value::String("fffe".to_string()));
    }

    #[test]
    fn cell_value_to_json_tombstone() {
        let cell = CellValue::tombstone(1000, 1700000000);
        let json = cell_value_to_json(&cell);
        assert_eq!(json, serde_json::Value::Null);
    }

    /// Helper to create a StorageEngine for tests using a temp directory.
    fn test_storage_engine(dir: &std::path::Path) -> ferrosa_storage::StorageEngine {
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
        };

        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.to_path_buf(),
        };
        ferrosa_storage::StorageEngine::new(config, None).unwrap()
    }

    #[test]
    fn extract_neighbor_id_basic() {
        // Build a clustering key: direction=0, label="KNOWS", neighbor_id=[1,2,3]
        let label = b"KNOWS";
        let neighbor = vec![1u8, 2, 3];
        let mut clustering = Vec::new();
        clustering.push(0u8); // direction OUT
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(&neighbor);

        let result = extract_neighbor_id(&clustering, None);
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn extract_neighbor_id_with_matching_label() {
        let label = b"KNOWS";
        let neighbor = vec![4u8, 5, 6];
        let mut clustering = Vec::new();
        clustering.push(0u8);
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(&neighbor);

        let result = extract_neighbor_id(&clustering, Some("KNOWS"));
        assert_eq!(result, Some(vec![4, 5, 6]));
    }

    #[test]
    fn extract_neighbor_id_case_insensitive_label() {
        let label = b"KNOWS";
        let neighbor = vec![7u8, 8];
        let mut clustering = Vec::new();
        clustering.push(0u8);
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(&neighbor);

        let result = extract_neighbor_id(&clustering, Some("knows"));
        assert_eq!(result, Some(vec![7, 8]));
    }

    #[test]
    fn extract_neighbor_id_wrong_label_returns_none() {
        let label = b"KNOWS";
        let neighbor = vec![1u8];
        let mut clustering = Vec::new();
        clustering.push(0u8);
        clustering.extend_from_slice(&(label.len() as u16).to_be_bytes());
        clustering.extend_from_slice(label);
        clustering.extend_from_slice(&(neighbor.len() as u16).to_be_bytes());
        clustering.extend_from_slice(&neighbor);

        let result = extract_neighbor_id(&clustering, Some("WORKS_AT"));
        assert_eq!(result, None);
    }

    #[test]
    fn extract_neighbor_id_too_short() {
        let result = extract_neighbor_id(&[0, 0], None);
        assert_eq!(result, None);
    }

    #[test]
    fn extract_neighbor_id_empty_label_and_id() {
        // direction + label_len(0) + id_len(0)
        let clustering = vec![0u8, 0, 0, 0, 0];
        let result = extract_neighbor_id(&clustering, None);
        assert_eq!(result, Some(vec![]));
    }

    #[test]
    fn check_timeout_not_expired() {
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        assert!(check_timeout(start, timeout).is_ok());
    }

    #[test]
    fn check_timeout_expired() {
        // Use a zero timeout to ensure it's always expired.
        let start = Instant::now() - Duration::from_secs(1);
        let timeout = Duration::from_millis(0);
        let result = check_timeout(start, timeout);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GraphError::Timeout));
    }

    #[test]
    fn expr_to_column_name_property() {
        let name = expr_to_column_name(&Expr::Property {
            var: "n".into(),
            name: "age".into(),
        });
        assert_eq!(name, "n.age");
    }

    #[test]
    fn expr_to_column_name_var() {
        let name = expr_to_column_name(&Expr::Var("n".into()));
        assert_eq!(name, "n");
    }

    #[test]
    fn expr_to_column_name_other() {
        let name = expr_to_column_name(&Expr::Literal(crate::parser::Literal::Integer(42)));
        assert_eq!(name, "?");
    }

    #[test]
    fn config_defaults() {
        let config = GraphEngineConfig::default();
        assert_eq!(config.query_timeout, Duration::from_secs(30));
        assert_eq!(config.max_result_rows, 10_000);
        assert_eq!(config.max_fan_out_per_hop, 10_000);
    }
}
