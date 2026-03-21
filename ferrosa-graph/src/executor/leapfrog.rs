//! Leapfrog triejoin: worst-case optimal (WCO) multi-way join.
//!
//! For cyclic graph patterns (e.g., triangle queries like
//! `MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)`),
//! leapfrog triejoin intersects sorted adjacency lists to find
//! variable bindings that satisfy all edges simultaneously.
//!
//! This avoids the intermediate blowup of nested-loop Expand on
//! multi-way patterns, achieving worst-case optimal time complexity.

use std::collections::HashMap;
use std::time::Instant;

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::VirtualTableRegistry;
use ferrosa_storage::{StorageEngine, TableId};

use crate::adjacency::schema::adjacency_keyspace_name;
use crate::error::{GraphError, Result};
use crate::executor::expand::{
    build_columns, check_timeout, extract_neighbor_id, GraphEngineConfig,
};
use crate::executor::result::{GraphResult, QueryStats};
use crate::executor::{eval, sort_rows};
use crate::parser::ReturnClause;
use crate::planner::physical::WcoJoinPlan;

/// A sorted iterator over neighbor IDs from the adjacency index.
/// Supports seek (advance to a position >= target) and next.
struct AdjacencyIterator {
    /// All neighbor IDs for this vertex+direction+label, sorted.
    neighbors: Vec<Vec<u8>>,
    /// Current position in the neighbors list.
    pos: usize,
}

impl AdjacencyIterator {
    /// Create from a vertex's adjacency partition, filtering by optional label.
    fn from_partition(
        storage: &StorageEngine,
        adj_table_id: &TableId,
        vertex_key: &DecoratedKey,
        edge_label: Option<&str>,
    ) -> Self {
        let partition = storage.read(adj_table_id, vertex_key).ok().flatten();
        let mut neighbors = Vec::new();
        if let Some(p) = partition {
            for row in &p.rows {
                if let Some(nid) = extract_neighbor_id(&row.clustering, edge_label) {
                    neighbors.push(nid);
                }
            }
        }
        neighbors.sort();
        neighbors.dedup();
        Self { neighbors, pos: 0 }
    }

    /// Create from an already-known list of neighbor IDs (pre-sorted, pre-deduped).
    fn from_sorted(neighbors: Vec<Vec<u8>>) -> Self {
        Self { neighbors, pos: 0 }
    }

    /// Current value, or None if exhausted.
    fn current(&self) -> Option<&[u8]> {
        self.neighbors.get(self.pos).map(|v| v.as_slice())
    }

    /// Advance to the next element.
    fn next(&mut self) {
        if self.pos < self.neighbors.len() {
            self.pos += 1;
        }
    }

    /// Seek forward to the first element >= target.
    fn seek(&mut self, target: &[u8]) {
        while self.pos < self.neighbors.len() && self.neighbors[self.pos].as_slice() < target {
            self.pos += 1;
        }
    }

    /// Whether the iterator is exhausted.
    fn is_exhausted(&self) -> bool {
        self.pos >= self.neighbors.len()
    }

    /// Reset to the beginning (used in tests).
    #[cfg(test)]
    fn reset(&mut self) {
        self.pos = 0;
    }
}

/// Leapfrog join: intersect multiple sorted iterators.
/// Returns the set of values present in ALL iterators.
fn leapfrog_join(iterators: &mut [AdjacencyIterator], max_results: usize) -> Vec<Vec<u8>> {
    if iterators.is_empty() || iterators.iter().any(|it| it.is_exhausted()) {
        return vec![];
    }

    let mut results = Vec::new();

    loop {
        if results.len() >= max_results {
            break;
        }

        // Find the iterator with the maximum current value.
        let max_val = iterators
            .iter()
            .filter_map(|it| it.current())
            .max()
            .map(|v| v.to_vec());

        let max_val = match max_val {
            Some(v) => v,
            None => break, // Some iterator exhausted
        };

        // Seek all iterators to >= max_val.
        for it in iterators.iter_mut() {
            it.seek(&max_val);
            if it.is_exhausted() {
                return results;
            }
        }

        // Check if all iterators are at the same value.
        let all_equal = iterators
            .iter()
            .all(|it| it.current() == Some(max_val.as_slice()));

        if all_equal {
            results.push(max_val);
            // Advance all iterators past the match.
            for it in iterators.iter_mut() {
                it.next();
            }
        }
        // Otherwise, the next iteration will converge on the new max.
    }

    results
}

/// Execute a worst-case optimal join plan using leapfrog triejoin.
///
/// Algorithm overview (triangle example: `(a)->(b)->(c)->(a)`):
/// 1. Get all candidate vertices for the first variable (a).
/// 2. For each candidate binding of `a`:
///    a. Build adjacency iterators for relations involving `a` as source.
///    b. Use leapfrog_join to find `b` values that satisfy `(a)->(b)`.
///    c. For each `b`, build iterators for remaining relations and intersect.
///    d. Check that the closing edge `(c)->(a)` is satisfied.
/// 3. Collect matched bindings and project via return clause.
#[allow(clippy::too_many_arguments)]
pub fn execute_wco_join(
    storage: &StorageEngine,
    keyspace: &str,
    plan: &WcoJoinPlan,
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<GraphResult> {
    let _ = virtual_tables; // Reserved for future virtual table support.
    let mut stats = QueryStats::default();

    if plan.variables.is_empty() || plan.relations.is_empty() {
        stats.execution_ms = start.elapsed().as_millis() as u64;
        return Ok(GraphResult {
            columns: build_columns(return_clause),
            rows: vec![],
            stats,
        });
    }

    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    // Get all candidate vertices for the first variable.
    let first_var = &plan.variables[0];
    let first_table = plan.var_tables.get(first_var).ok_or_else(|| {
        GraphError::Validation(format!("no resolved table for variable '{first_var}'"))
    })?;
    let first_table_id = TableId::new(&first_table.keyspace, &first_table.table);
    let candidates = storage.read_range(&first_table_id, None, None, config.max_result_rows)?;
    stats.vertices_read += candidates.len();
    check_timeout(start, config.query_timeout)?;

    let columns = build_columns(return_clause);

    // For each candidate binding of the first variable, do variable elimination.
    let mut result_rows: Vec<Vec<serde_json::Value>> = Vec::new();

    for candidate in &candidates {
        if result_rows.len() >= config.max_result_rows {
            break;
        }
        check_timeout(start, config.query_timeout)?;

        let first_key_bytes = candidate.key.key.as_bytes().to_vec();

        // Recursive variable elimination: bind first_var, then find valid
        // bindings for remaining variables.
        let mut var_bindings: HashMap<String, Vec<u8>> = HashMap::new();
        var_bindings.insert(first_var.clone(), first_key_bytes);

        enumerate_bindings(
            storage,
            &adj_table_id,
            plan,
            &plan.variables,
            1, // Start from second variable.
            &mut var_bindings,
            &mut result_rows,
            return_clause,
            &columns,
            config,
            start,
            &mut stats,
            schema,
        )?;
    }

    // Apply ORDER BY.
    if !return_clause.order_by.is_empty() {
        sort_rows(&mut result_rows, &columns, &return_clause.order_by);
    }

    // Apply DISTINCT.
    if return_clause.distinct {
        result_rows.dedup();
    }

    // Apply LIMIT.
    if let Some(limit) = return_clause.limit {
        let limit = limit.max(0) as usize;
        result_rows.truncate(limit);
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns,
        rows: result_rows,
        stats,
    })
}

/// Recursively enumerate valid variable bindings.
///
/// For each unbound variable, collects adjacency iterators from all relations
/// that connect the variable to already-bound variables, then intersects them
/// via leapfrog_join to find valid bindings.
#[allow(clippy::too_many_arguments, clippy::only_used_in_recursion)]
fn enumerate_bindings(
    storage: &StorageEngine,
    adj_table_id: &TableId,
    plan: &WcoJoinPlan,
    variables: &[String],
    var_idx: usize,
    var_bindings: &mut HashMap<String, Vec<u8>>,
    result_rows: &mut Vec<Vec<serde_json::Value>>,
    return_clause: &ReturnClause,
    columns: &[String],
    config: &GraphEngineConfig,
    start: Instant,
    stats: &mut QueryStats,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<()> {
    if result_rows.len() >= config.max_result_rows {
        return Ok(());
    }
    check_timeout(start, config.query_timeout)?;

    // Base case: all variables are bound. Verify that all relations are satisfied
    // (some closing edges may connect two already-bound variables).
    if var_idx >= variables.len() {
        // Check that every relation is satisfied with current bindings.
        if verify_all_relations(storage, adj_table_id, plan, var_bindings, stats)? {
            // Build a result row from the bindings.
            let row = project_bindings(storage, plan, var_bindings, return_clause, stats, schema)?;
            result_rows.push(row);
        }
        return Ok(());
    }

    let current_var = &variables[var_idx];

    // Collect adjacency iterators for relations connecting current_var
    // to already-bound variables.
    let mut iterators = Vec::new();
    for rel in &plan.relations {
        if &rel.dst_var == current_var {
            if let Some(src_bytes) = var_bindings.get(&rel.src_var) {
                let src_key = DecoratedKey::new(PartitionKey::new(src_bytes.clone()));
                let it = AdjacencyIterator::from_partition(
                    storage,
                    adj_table_id,
                    &src_key,
                    rel.edge_label.as_deref(),
                );
                stats.edges_read += it.neighbors.len();
                iterators.push(it);
            }
        }
        // Also handle reverse direction: if current_var is a source and
        // the destination is already bound, we need to check the reverse
        // adjacency (IN direction). But since our adjacency index stores
        // both directions, we look up the dst's IN edges.
        if &rel.src_var == current_var {
            if let Some(dst_bytes) = var_bindings.get(&rel.dst_var) {
                // Look up IN-edges of the destination that point back.
                // The adjacency index for dst should have IN entries with
                // current_var's ID as neighbor.
                // For leapfrog, we need the set of source vertices that
                // have an edge TO dst. We read dst's IN-adjacency.
                let dst_key = DecoratedKey::new(PartitionKey::new(dst_bytes.clone()));
                let partition = storage.read(adj_table_id, &dst_key).ok().flatten();
                let mut neighbors = Vec::new();
                if let Some(p) = partition {
                    for row in &p.rows {
                        // Filter for IN direction (byte 0x01) and matching label.
                        if !row.clustering.is_empty() && row.clustering[0] == 0x01 {
                            if let Some(nid) =
                                extract_neighbor_id(&row.clustering, rel.edge_label.as_deref())
                            {
                                neighbors.push(nid);
                            }
                        }
                    }
                }
                neighbors.sort();
                neighbors.dedup();
                stats.edges_read += neighbors.len();
                iterators.push(AdjacencyIterator::from_sorted(neighbors));
            }
        }
    }

    if iterators.is_empty() {
        // No constraints on this variable from bound variables yet.
        // Fall back to scanning all candidates for this variable.
        if let Some(table) = plan.var_tables.get(current_var) {
            let table_id = TableId::new(&table.keyspace, &table.table);
            let candidates = storage.read_range(&table_id, None, None, config.max_result_rows)?;
            stats.vertices_read += candidates.len();

            for candidate in &candidates {
                if result_rows.len() >= config.max_result_rows {
                    break;
                }
                let key_bytes = candidate.key.key.as_bytes().to_vec();
                var_bindings.insert(current_var.clone(), key_bytes);
                enumerate_bindings(
                    storage,
                    adj_table_id,
                    plan,
                    variables,
                    var_idx + 1,
                    var_bindings,
                    result_rows,
                    return_clause,
                    columns,
                    config,
                    start,
                    stats,
                    schema,
                )?;
            }
            var_bindings.remove(current_var);
        }
        return Ok(());
    }

    // Leapfrog join: intersect all iterators for the current variable.
    let valid_bindings = leapfrog_join(&mut iterators, config.max_result_rows);

    for binding in &valid_bindings {
        if result_rows.len() >= config.max_result_rows {
            break;
        }
        var_bindings.insert(current_var.clone(), binding.clone());
        enumerate_bindings(
            storage,
            adj_table_id,
            plan,
            variables,
            var_idx + 1,
            var_bindings,
            result_rows,
            return_clause,
            columns,
            config,
            start,
            stats,
            schema,
        )?;
    }
    var_bindings.remove(current_var);

    Ok(())
}

/// Verify that all relations in the plan are satisfied by the current bindings.
///
/// This checks "closing" edges -- relations between two variables that were both
/// already bound before the other was enumerated (e.g., the `(c)->(a)` edge
/// in a triangle where `a` was the anchor and `c` was the last variable eliminated).
fn verify_all_relations(
    storage: &StorageEngine,
    adj_table_id: &TableId,
    plan: &WcoJoinPlan,
    var_bindings: &HashMap<String, Vec<u8>>,
    stats: &mut QueryStats,
) -> Result<bool> {
    for rel in &plan.relations {
        let src_bytes = match var_bindings.get(&rel.src_var) {
            Some(b) => b,
            None => return Ok(false),
        };
        let dst_bytes = match var_bindings.get(&rel.dst_var) {
            Some(b) => b,
            None => return Ok(false),
        };

        // Check that src has an OUT edge to dst with the given label.
        let src_key = DecoratedKey::new(PartitionKey::new(src_bytes.clone()));
        let partition = storage.read(adj_table_id, &src_key).ok().flatten();
        let found = if let Some(p) = partition {
            stats.edges_read += 1;
            p.rows.iter().any(|row| {
                if let Some(nid) = extract_neighbor_id(&row.clustering, rel.edge_label.as_deref()) {
                    nid == *dst_bytes
                } else {
                    false
                }
            })
        } else {
            false
        };

        if !found {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Project variable bindings into a result row according to the return clause.
fn project_bindings(
    storage: &StorageEngine,
    plan: &WcoJoinPlan,
    var_bindings: &HashMap<String, Vec<u8>>,
    return_clause: &ReturnClause,
    stats: &mut QueryStats,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<Vec<serde_json::Value>> {
    // Build JSON bindings for eval_expr by reading each variable's partition.
    let mut json_bindings: HashMap<String, serde_json::Value> = HashMap::new();

    for (var, key_bytes) in var_bindings {
        let hex_id = hex::encode(key_bytes);
        if let Some(table) = plan.var_tables.get(var) {
            let table_id = TableId::new(&table.keyspace, &table.table);
            let dk = DecoratedKey::new(PartitionKey::new(key_bytes.clone()));
            let partition = storage.read(&table_id, &dk)?;
            stats.vertices_read += 1;

            let col_names =
                super::expand::column_names_for_table(schema, &table_id.keyspace, &table_id.table);
            let row_json = if let Some(ref part) = partition {
                eval::partition_to_json(part, &hex_id, &col_names)
            } else {
                serde_json::Value::String(hex_id)
            };
            json_bindings.insert(var.clone(), row_json);
        } else {
            json_bindings.insert(var.clone(), serde_json::Value::String(hex_id));
        }
    }

    let row: Vec<serde_json::Value> = return_clause
        .items
        .iter()
        .map(|item| eval::eval_expr(&item.expr, &json_bindings).unwrap_or(serde_json::Value::Null))
        .collect();

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a clustering key for adjacency entries.
    #[allow(dead_code)]
    fn make_clustering(direction: u8, label: &str, neighbor_id: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(direction);
        let label_bytes = label.as_bytes();
        out.extend_from_slice(&(label_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(label_bytes);
        out.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
        out.extend_from_slice(neighbor_id);
        out
    }

    #[test]
    fn leapfrog_join_basic() {
        // Three sorted iterators with known intersection {3, 5}.
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2], vec![3], vec![5], vec![7]]),
            AdjacencyIterator::from_sorted(vec![vec![2], vec![3], vec![5], vec![6]]),
            AdjacencyIterator::from_sorted(vec![vec![3], vec![4], vec![5], vec![8]]),
        ];

        let results = leapfrog_join(&mut iters, 100);
        assert_eq!(results, vec![vec![3], vec![5]]);
    }

    #[test]
    fn leapfrog_join_empty_intersection() {
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2]]),
            AdjacencyIterator::from_sorted(vec![vec![3], vec![4]]),
            AdjacencyIterator::from_sorted(vec![vec![5], vec![6]]),
        ];

        let results = leapfrog_join(&mut iters, 100);
        assert!(results.is_empty());
    }

    #[test]
    fn leapfrog_join_single_iterator() {
        // Single iterator degenerates to returning all elements (up to max).
        let mut iters = vec![AdjacencyIterator::from_sorted(vec![
            vec![1],
            vec![2],
            vec![3],
        ])];

        let results = leapfrog_join(&mut iters, 100);
        assert_eq!(results, vec![vec![1], vec![2], vec![3]]);
    }

    #[test]
    fn leapfrog_join_respects_max_results() {
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2], vec![3], vec![4]]),
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2], vec![3], vec![4]]),
        ];

        let results = leapfrog_join(&mut iters, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec![vec![1], vec![2]]);
    }

    #[test]
    fn leapfrog_join_empty_iterators() {
        let mut iters: Vec<AdjacencyIterator> = vec![];
        let results = leapfrog_join(&mut iters, 100);
        assert!(results.is_empty());
    }

    #[test]
    fn leapfrog_join_one_empty_iterator() {
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2]]),
            AdjacencyIterator::from_sorted(vec![]),
        ];

        let results = leapfrog_join(&mut iters, 100);
        assert!(results.is_empty());
    }

    #[test]
    fn adjacency_iterator_seek() {
        let mut it =
            AdjacencyIterator::from_sorted(vec![vec![1], vec![3], vec![5], vec![7], vec![9]]);

        assert_eq!(it.current(), Some(vec![1].as_slice()));

        it.seek(&[4]);
        assert_eq!(it.current(), Some(vec![5].as_slice()));

        it.seek(&[5]);
        assert_eq!(it.current(), Some(vec![5].as_slice()));

        it.seek(&[10]);
        assert!(it.is_exhausted());
    }

    #[test]
    fn adjacency_iterator_sorted_dedup() {
        // Verify that from_sorted preserves sorted, deduped input.
        let input = vec![vec![1], vec![2], vec![3]];
        let it = AdjacencyIterator::from_sorted(input.clone());
        assert_eq!(it.neighbors, input);
        assert_eq!(it.pos, 0);
    }

    #[test]
    fn adjacency_iterator_next_and_exhaustion() {
        let mut it = AdjacencyIterator::from_sorted(vec![vec![1], vec![2]]);

        assert!(!it.is_exhausted());
        assert_eq!(it.current(), Some(vec![1].as_slice()));

        it.next();
        assert_eq!(it.current(), Some(vec![2].as_slice()));

        it.next();
        assert!(it.is_exhausted());
        assert_eq!(it.current(), None);

        // Calling next on exhausted iterator is safe.
        it.next();
        assert!(it.is_exhausted());
    }

    #[test]
    fn adjacency_iterator_reset() {
        let mut it = AdjacencyIterator::from_sorted(vec![vec![1], vec![2], vec![3]]);

        it.next();
        it.next();
        assert_eq!(it.current(), Some(vec![3].as_slice()));

        it.reset();
        assert_eq!(it.current(), Some(vec![1].as_slice()));
        assert_eq!(it.pos, 0);
    }
}
