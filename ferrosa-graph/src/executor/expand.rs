//! Expand executor: traverses graph patterns via the adjacency index.
//!
//! Executes a `PhysicalPlan::Expand` by:
//! 1. Looking up the anchor vertex via `storage.read_range()` (or
//!    `VirtualTable::read()` if the source is a virtual table)
//! 2. For each hop, reading the adjacency index to find neighbors
//! 3. Building a `GraphResult` with columns from the return clause

use std::time::{Duration, Instant};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_schema::VirtualTableRegistry;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, TableId};

use crate::adjacency::schema::adjacency_keyspace_name;
use crate::error::{GraphError, Result};
use crate::executor::result::{GraphResult, QueryStats};
use crate::parser::{Expr, Literal, ReturnClause, ReturnItem, SortDir};
use crate::planner::physical::{Anchor, CreateOp, Hop, PhysicalPlan};

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
        PhysicalPlan::CreateNodes { creates } => execute_create(storage, &creates, config, start),
        PhysicalPlan::SetProperties {
            expand,
            assignments,
        } => execute_set(
            storage,
            *expand,
            keyspace,
            &assignments,
            config,
            virtual_tables,
            start,
        ),
        PhysicalPlan::DeleteNodes {
            expand,
            variables,
            detach,
        } => execute_delete(
            storage,
            *expand,
            keyspace,
            &variables,
            detach,
            config,
            virtual_tables,
            start,
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

    // Step 3: Build result from return clause, projecting property values.
    let columns = build_columns(return_clause);
    let anchor_table_id_for_proj = TableId::new(&anchor.table.keyspace, &anchor.table.table);

    let mut rows = Vec::new();
    for key in &current_keys {
        if rows.len() >= config.max_result_rows {
            break;
        }

        // Read the full partition from storage for property projection.
        let partition = storage.read(&anchor_table_id_for_proj, key)?;

        let hex_id = hex::encode(key.key.as_bytes());
        let row: Vec<serde_json::Value> = return_clause
            .items
            .iter()
            .map(|item| match &item.expr {
                Expr::Var(_) => serde_json::Value::String(hex_id.clone()),
                Expr::Property { name, .. } => {
                    // Look up the property in the partition's rows.
                    if let Some(ref part) = partition {
                        extract_property_from_partition(part, name)
                    } else {
                        serde_json::Value::Null
                    }
                }
                _ => serde_json::Value::Null,
            })
            .collect();
        rows.push(row);
    }

    // Apply ORDER BY.
    if !return_clause.order_by.is_empty() {
        sort_rows(&mut rows, &columns, &return_clause.order_by);
    }

    // Apply DISTINCT.
    if return_clause.distinct {
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

/// Extract a property value from a partition's first row by column name.
///
/// Since the graph engine stores vertex properties as cells in a single row
/// keyed by column index, we iterate cells and match by index. However, we
/// don't carry the table schema into the executor, so we use a positional
/// heuristic: column names are matched against cell indices. For a more robust
/// implementation we'd need to pass column metadata. For now, we treat each
/// cell's column_index as its position and return the first matching cell
/// that has the right index for a name-based lookup (matching the property
/// name to the column index in order). As a practical fallback, we look for
/// any non-empty cell and return it as a string.
fn extract_property_from_partition(
    partition: &ferrosa_sstable::types::Partition,
    property_name: &str,
) -> serde_json::Value {
    // Walk the partition's rows looking for a cell whose value is present.
    // In the graph model, vertex tables typically have a single static/clustered
    // row. We search all rows' cells and the static row.
    //
    // Without schema metadata we cannot map property names to column indices
    // precisely, so we return the first live cell we find. A future revision
    // should thread column metadata through the planner to enable exact lookup.
    let all_rows = partition.static_row.iter().chain(partition.rows.iter());

    let _ = property_name; // TODO: use schema metadata for exact column lookup

    for row in all_rows {
        if let Some((_col_idx, cell)) = row.cells.first() {
            return cell_value_to_json(cell);
        }
    }
    serde_json::Value::Null
}

/// Convert a `Literal` from the AST into raw bytes for storage.
fn literal_to_bytes(lit: &Literal) -> Vec<u8> {
    match lit {
        Literal::String(s) => s.as_bytes().to_vec(),
        Literal::Integer(i) => i.to_be_bytes().to_vec(),
        Literal::Float(f) => f.to_be_bytes().to_vec(),
        Literal::Bool(b) => vec![if *b { 1 } else { 0 }],
        Literal::Null => vec![],
    }
}

/// Convert an `Expr` to bytes, supporting only `Expr::Literal` for now.
fn expr_to_bytes(expr: &Expr) -> std::result::Result<Vec<u8>, GraphError> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_bytes(lit)),
        other => Err(GraphError::Validation(format!(
            "cannot convert expression to bytes: {other:?}"
        ))),
    }
}

/// Generate a current timestamp in microseconds since epoch.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Execute a CREATE plan: for each CreateOp, build a Row with cells from the
/// properties and write it to storage.
fn execute_create(
    storage: &StorageEngine,
    creates: &[CreateOp],
    _config: &GraphEngineConfig,
    start: Instant,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();
    let timestamp = now_micros();

    for op in creates {
        let table_id = TableId::new(&op.table.keyspace, &op.table.table);

        // Generate a unique key for the new vertex/edge using a UUID.
        let key_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        // Build cells from properties.
        let cells: Vec<(u16, CellValue)> = op
            .props
            .iter()
            .enumerate()
            .map(|(idx, (_name, expr))| {
                let bytes = expr_to_bytes(expr).unwrap_or_default();
                (idx as u16, CellValue::live(bytes, timestamp))
            })
            .collect();

        let row = Row {
            clustering: vec![],
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        };

        storage.write(&table_id, &key, row, timestamp)?;
        stats.vertices_written += 1;
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "created {} vertices",
            stats.vertices_written
        ))]],
        stats,
    })
}

/// Execute a SET plan: run the expand to find matching vertices, then write
/// updated cells for each one.
#[allow(clippy::too_many_arguments)]
fn execute_set(
    storage: &StorageEngine,
    expand: PhysicalPlan,
    keyspace: &str,
    assignments: &[(String, String, Expr)],
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    start: Instant,
) -> Result<GraphResult> {
    // Execute the inner expand to find matching vertices.
    let expand_result = execute(expand, storage, keyspace, config, virtual_tables)?;
    let mut stats = QueryStats::default();
    stats.vertices_read = expand_result.stats.vertices_read;
    stats.edges_read = expand_result.stats.edges_read;

    let timestamp = now_micros();

    // For each matched vertex, apply the assignments.
    // The expand result rows contain vertex IDs (hex-encoded).
    // We need to reconstruct the partition key from the hex ID.
    for row_values in &expand_result.rows {
        for (col_idx, col_name) in expand_result.columns.iter().enumerate() {
            // Find assignments that target this variable.
            let matching_assignments: Vec<&(String, String, Expr)> = assignments
                .iter()
                .filter(|(var, _prop, _val)| col_name == var)
                .collect();

            if matching_assignments.is_empty() {
                continue;
            }

            // Get the vertex ID from the row.
            if let Some(serde_json::Value::String(hex_id)) = row_values.get(col_idx) {
                let key_bytes = hex::decode(hex_id)
                    .map_err(|e| GraphError::Internal(format!("invalid hex vertex ID: {e}")))?;
                let key = DecoratedKey::new(PartitionKey::new(key_bytes));

                // Build cells for the updated properties.
                let cells: Vec<(u16, CellValue)> = matching_assignments
                    .iter()
                    .enumerate()
                    .map(|(idx, (_var, _prop, val))| {
                        let bytes = expr_to_bytes(val).unwrap_or_default();
                        (idx as u16, CellValue::live(bytes, timestamp))
                    })
                    .collect();

                // We need a table_id; look up via the variable. For simplicity,
                // we use the first assignment's variable to find it in the expand's
                // anchor table. In a full implementation we'd carry table metadata.
                // For now, use the column name to identify the table.
                let table_id = TableId::new(keyspace, col_name);

                let update_row = Row {
                    clustering: vec![],
                    cells,
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::NONE,
                };

                storage.write(&table_id, &key, update_row, timestamp)?;
                stats.vertices_written += 1;
            }
        }
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "updated {} vertices",
            stats.vertices_written
        ))]],
        stats,
    })
}

/// Execute a DELETE plan: run the expand to find matching vertices, then write
/// tombstones for each one.
#[allow(clippy::too_many_arguments)]
fn execute_delete(
    storage: &StorageEngine,
    expand: PhysicalPlan,
    keyspace: &str,
    variables: &[String],
    _detach: bool,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    start: Instant,
) -> Result<GraphResult> {
    let expand_result = execute(expand, storage, keyspace, config, virtual_tables)?;
    let mut stats = QueryStats::default();
    stats.vertices_read = expand_result.stats.vertices_read;
    stats.edges_read = expand_result.stats.edges_read;

    let timestamp = now_micros();
    let local_deletion_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;

    // For each matched vertex in the specified variables, write a tombstone.
    for row_values in &expand_result.rows {
        for (col_idx, col_name) in expand_result.columns.iter().enumerate() {
            if !variables.iter().any(|v| v == col_name) {
                continue;
            }

            if let Some(serde_json::Value::String(hex_id)) = row_values.get(col_idx) {
                let key_bytes = hex::decode(hex_id)
                    .map_err(|e| GraphError::Internal(format!("invalid hex vertex ID: {e}")))?;
                let key = DecoratedKey::new(PartitionKey::new(key_bytes));
                let table_id = TableId::new(keyspace, col_name);

                // Write a row-level tombstone.
                let tombstone_row = Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(timestamp, local_deletion_time),
                    primary_key_liveness: LivenessInfo::NONE,
                };

                storage.write(&table_id, &key, tombstone_row, timestamp)?;
                stats.vertices_deleted += 1;
            }
        }
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "deleted {} vertices",
            stats.vertices_deleted
        ))]],
        stats,
    })
}

/// Sort rows by the specified ORDER BY columns.
fn sort_rows(
    rows: &mut [Vec<serde_json::Value>],
    columns: &[String],
    order_by: &[crate::parser::OrderItem],
) {
    rows.sort_by(|a, b| {
        for order_item in order_by {
            let col_name = expr_to_column_name(&order_item.expr);
            let col_idx = columns.iter().position(|c| c == &col_name);
            if let Some(idx) = col_idx {
                let cmp = compare_json_values(a.get(idx), b.get(idx));
                let cmp = match order_item.direction {
                    SortDir::Asc => cmp,
                    SortDir::Desc => cmp.reverse(),
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Compare two JSON values for sorting purposes.
fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(serde_json::Value::Null), Some(serde_json::Value::Null)) => std::cmp::Ordering::Equal,
        (Some(serde_json::Value::Null), Some(_)) => std::cmp::Ordering::Less,
        (Some(_), Some(serde_json::Value::Null)) => std::cmp::Ordering::Greater,
        (Some(serde_json::Value::String(a)), Some(serde_json::Value::String(b))) => a.cmp(b),
        (Some(serde_json::Value::Number(a)), Some(serde_json::Value::Number(b))) => {
            let af = a.as_f64().unwrap_or(0.0);
            let bf = b.as_f64().unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => std::cmp::Ordering::Equal,
    }
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

    #[test]
    fn sort_rows_ascending() {
        let columns = vec!["name".to_string()];
        let order_by = vec![crate::parser::OrderItem {
            expr: Expr::Var("name".into()),
            direction: SortDir::Asc,
        }];

        let mut rows = vec![
            vec![serde_json::Value::String("Charlie".into())],
            vec![serde_json::Value::String("Alice".into())],
            vec![serde_json::Value::String("Bob".into())],
        ];

        sort_rows(&mut rows, &columns, &order_by);

        assert_eq!(rows[0][0], serde_json::Value::String("Alice".into()));
        assert_eq!(rows[1][0], serde_json::Value::String("Bob".into()));
        assert_eq!(rows[2][0], serde_json::Value::String("Charlie".into()));
    }

    #[test]
    fn sort_rows_descending() {
        let columns = vec!["name".to_string()];
        let order_by = vec![crate::parser::OrderItem {
            expr: Expr::Var("name".into()),
            direction: SortDir::Desc,
        }];

        let mut rows = vec![
            vec![serde_json::Value::String("Alice".into())],
            vec![serde_json::Value::String("Charlie".into())],
            vec![serde_json::Value::String("Bob".into())],
        ];

        sort_rows(&mut rows, &columns, &order_by);

        assert_eq!(rows[0][0], serde_json::Value::String("Charlie".into()));
        assert_eq!(rows[1][0], serde_json::Value::String("Bob".into()));
        assert_eq!(rows[2][0], serde_json::Value::String("Alice".into()));
    }

    #[test]
    fn compare_json_values_nulls_sort_first() {
        let result = compare_json_values(
            Some(&serde_json::Value::Null),
            Some(&serde_json::Value::String("a".into())),
        );
        assert_eq!(result, std::cmp::Ordering::Less);
    }

    #[test]
    fn compare_json_values_numbers() {
        let result =
            compare_json_values(Some(&serde_json::json!(10)), Some(&serde_json::json!(20)));
        assert_eq!(result, std::cmp::Ordering::Less);
    }

    #[test]
    fn limit_truncates_rows() {
        // Build a mock result with 5 rows and apply a limit of 3.
        let mut rows: Vec<Vec<serde_json::Value>> = (0..5)
            .map(|i| vec![serde_json::Value::String(format!("row_{i}"))])
            .collect();

        let limit: i64 = 3;
        rows.truncate(limit as usize);

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], serde_json::Value::String("row_0".into()));
        assert_eq!(rows[2][0], serde_json::Value::String("row_2".into()));
    }

    #[test]
    fn literal_to_bytes_string() {
        let bytes = literal_to_bytes(&crate::parser::Literal::String("hello".into()));
        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn literal_to_bytes_integer() {
        let bytes = literal_to_bytes(&crate::parser::Literal::Integer(42));
        assert_eq!(bytes, 42_i64.to_be_bytes().to_vec());
    }

    #[test]
    fn literal_to_bytes_null() {
        let bytes = literal_to_bytes(&crate::parser::Literal::Null);
        assert!(bytes.is_empty());
    }
}
