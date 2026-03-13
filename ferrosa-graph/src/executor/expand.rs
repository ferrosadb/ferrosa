//! Expand executor: traverses graph patterns via the adjacency index.
//!
//! Executes a `PhysicalPlan::Expand` by:
//! 1. Looking up the anchor vertex via `storage.read_range()`
//! 2. For each hop, reading the adjacency index to find neighbors
//! 3. Building a `GraphResult` with columns from the return clause

use std::time::{Duration, Instant};

use ferrosa_common::DecoratedKey;
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
pub fn execute(
    plan: PhysicalPlan,
    storage: &StorageEngine,
    keyspace: &str,
    config: &GraphEngineConfig,
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
        ),
    }
}

/// Execute an Expand plan.
fn execute_expand(
    storage: &StorageEngine,
    keyspace: &str,
    anchor: &Anchor,
    hops: &[Hop],
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();

    // Step 1: Anchor lookup — read all partitions from the anchor table.
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
