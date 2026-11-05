//! Leapfrog triejoin: worst-case optimal (WCO) multi-way join.
//!
//! For cyclic graph patterns (e.g., triangle queries like
//! `MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)`),
//! leapfrog triejoin intersects sorted adjacency lists to find
//! variable bindings that satisfy all edges simultaneously.
//!
//! This avoids the intermediate blowup of nested-loop Expand on
//! multi-way patterns, achieving worst-case optimal time complexity.

use crate::executor::io::GraphWrite;

use std::collections::HashMap;
use std::time::Instant;

use futures::StreamExt as _;

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::VirtualTableRegistry;
use ferrosa_storage::TableId;

use crate::adjacency::schema::adjacency_keyspace_name;
use crate::error::{GraphError, Result};
use crate::executor::eval;
use crate::executor::expand::{
    build_columns, check_timeout, extract_neighbor_id, GraphEngineConfig,
};
use crate::executor::result::{GraphResult, QueryStats};
use crate::executor::spill::{OrderSemantics, RowAcc};
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
    ///
    /// A failed read is an error, not an empty neighbour list: treating it as
    /// "no neighbours" made the join return a silently short answer.
    async fn from_partition(
        write_path: &dyn GraphWrite,
        adj_table_id: &TableId,
        vertex_key: &DecoratedKey,
        edge_label: Option<&str>,
    ) -> Result<Self> {
        let partition = write_path.read(adj_table_id, vertex_key).await?;
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
        Ok(Self { neighbors, pos: 0 })
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

/// Advance a set of sorted iterators to their next common value.
///
/// Returns `None` once any iterator is exhausted, at which point the
/// intersection is complete.
///
/// **Streams; does not materialise.** This used to be `leapfrog_join`, which
/// collected the whole intersection into a `Vec<Vec<u8>>` and took a
/// `max_results` argument that every production call defeated with
/// `usize::MAX`. That combination is the worst of both: a cap that drops
/// ANSWERS silently when it is honoured, and an unbounded in-memory Vec when it
/// is not.
///
/// Neither is necessary. Leapfrog converges on one common value per round by
/// seeking every iterator to the current maximum, so the intersection is
/// naturally incremental — building a Vec was gratuitous. Yielding one value at
/// a time removes the cap and the allocation together, so there is nothing to
/// spill.
///
/// Work is bounded by the caller's `check_timeout`, which fails loud.
fn next_common_value(iterators: &mut [AdjacencyIterator]) -> Option<Vec<u8>> {
    if iterators.is_empty() || iterators.iter().any(|it| it.is_exhausted()) {
        return None;
    }

    loop {
        // Find the iterator with the maximum current value.
        let max_val = iterators
            .iter()
            .filter_map(|it| it.current())
            .max()
            .map(|v| v.to_vec())?;

        // Seek all iterators to >= max_val.
        for it in iterators.iter_mut() {
            it.seek(&max_val);
            if it.is_exhausted() {
                return None;
            }
        }

        // All at the same value means it is in every iterator.
        let all_equal = iterators
            .iter()
            .all(|it| it.current() == Some(max_val.as_slice()));

        if all_equal {
            // Advance past the match so the next call makes progress.
            for it in iterators.iter_mut() {
                it.next();
            }
            return Some(max_val);
        }
        // Otherwise the next round converges on the new maximum.
    }
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
pub async fn execute_wco_join(
    write_path: &dyn GraphWrite,
    keyspace: &str,
    plan: &WcoJoinPlan,
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<GraphResult> {
    // One implementation. This collects the streaming one for the callers that
    // still want a whole result, rather than keeping a second executor that
    // could drift from it.
    let (columns, rows, stats) = execute_wco_join_streaming(
        write_path,
        keyspace,
        plan,
        return_clause,
        config,
        start,
        virtual_tables,
        schema,
    )
    .await?;
    // `usize::MAX`, deliberately NOT `config.max_result_rows`: this bridge
    // truncates silently at whatever it is given, and a caller asking for a
    // whole result must get the whole result. The materialisation here is the
    // caller's choice; a cap would be a wrong answer.
    crate::executor::collect_to_graph_result(columns, rows, stats, usize::MAX).await
}

/// The streaming form: rows are produced on pull and never all held at once.
///
/// The accumulator already spilled to disk while accumulating, and then
/// `finish_rows()` loaded every row back into a `Vec` to build a `GraphResult`
/// — so the spill was undone at the last step and a large result still landed
/// in RAM. `finish_stream()` was sitting next to it, unused from here.
///
/// DISTINCT and LIMIT move onto the stream for the same reason: a `Vec::dedup`
/// after a full sort, and a `Vec::truncate`, both require the whole result to
/// exist first. `dedup_stream` and `take` do not.
#[allow(clippy::too_many_arguments)]
pub async fn execute_wco_join_streaming<'a>(
    write_path: &'a dyn GraphWrite,
    keyspace: &'a str,
    plan: &WcoJoinPlan,
    return_clause: &ReturnClause,
    config: &'a GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<(Vec<String>, crate::executor::RowStream<'static>, QueryStats)> {
    let _ = virtual_tables; // Reserved for future virtual table support.
    let mut stats = QueryStats::default();

    if plan.variables.is_empty() || plan.relations.is_empty() {
        stats.execution_ms = start.elapsed().as_millis() as u64;
        return Ok((
            build_columns(return_clause),
            crate::executor::stream::stream_from_rows(vec![]),
            stats,
        ));
    }

    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    // Get all candidate vertices for the first variable — STREAMED, not
    // materialized: `range_read` returned every partition of the table in one
    // `Vec` before the first candidate was examined (t_3f2f961a). Each pull
    // now yields one partition; only the candidate's key survives the
    // iteration.
    let first_var = &plan.variables[0];
    let first_table = plan.var_tables.get(first_var).ok_or_else(|| {
        GraphError::Validation(format!("no resolved table for variable '{first_var}'"))
    })?;
    let first_table_id = TableId::new(&first_table.keyspace, &first_table.table);

    let columns = build_columns(return_clause);

    // With an ORDER BY, result accumulation goes into the ORDER BY sink —
    // spill-backed whenever a backend is configured, with every order term a
    // hidden key — so an unbounded ORDER BY never holds the full result in
    // memory, projected terms or not. The silent `max_result_rows` truncation
    // that used to stop enumeration is REMOVED (result caps are bugs: a client
    // could not tell a partial answer from a complete one). Work stays bounded
    // by `check_timeout`, which every enumeration step already consults.
    let mut acc = RowAcc::new(
        &columns,
        return_clause,
        config,
        "graph_wco_order_by",
        OrderSemantics::ProjectedColumns,
    )?;

    let mut candidates = write_path.range_read_stream_all(&first_table_id, 0).await?;
    while let Some(candidate) = candidates.next().await {
        let candidate = candidate?;
        stats.vertices_read += 1;
        check_timeout(start, config.query_timeout)?;

        let first_key_bytes = candidate.key.key.as_bytes().to_vec();

        // Recursive variable elimination: bind first_var, then find valid
        // bindings for remaining variables.
        let mut var_bindings: HashMap<String, Vec<u8>> = HashMap::new();
        var_bindings.insert(first_var.clone(), first_key_bytes);

        Box::pin(enumerate_bindings(
            write_path,
            &adj_table_id,
            plan,
            &plan.variables,
            1, // Start from second variable.
            &mut var_bindings,
            &mut acc,
            return_clause,
            &columns,
            config,
            start,
            &mut stats,
            schema,
        ))
        .await?;
    }

    // Terminal ORDER BY, then DISTINCT, then LIMIT, all on the stream. The
    // sink already sorted while accumulating and hands back a stream that
    // reads its spilled runs on pull, hidden keys stripped.
    //
    // DISTINCT: the buffered form sorted the whole result by its debug
    // representation and then deduped, which needed every row present at once
    // — and incidentally returned RETURN DISTINCT in string-repr sorted order.
    // `dedup_stream` is first-seen order, matching the streaming paths that
    // already made that change deliberately (t_4ce82a3e). LIMIT: `take` stops
    // pulling where `Vec::truncate` had to build the whole result first.
    let rows = acc.into_stream(return_clause)?;

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok((columns, rows, stats))
}

/// Recursively enumerate valid variable bindings.
///
/// For each unbound variable, collects adjacency iterators from all relations
/// that connect the variable to already-bound variables, then intersects them
/// via leapfrog_join to find valid bindings.
#[allow(clippy::too_many_arguments, clippy::only_used_in_recursion)]
async fn enumerate_bindings(
    write_path: &dyn GraphWrite,
    adj_table_id: &TableId,
    plan: &WcoJoinPlan,
    variables: &[String],
    var_idx: usize,
    var_bindings: &mut HashMap<String, Vec<u8>>,
    acc: &mut RowAcc,
    return_clause: &ReturnClause,
    columns: &[String],
    config: &GraphEngineConfig,
    start: Instant,
    stats: &mut QueryStats,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<()> {
    check_timeout(start, config.query_timeout)?;

    // Base case: all variables are bound. Verify that all relations are satisfied
    // (some closing edges may connect two already-bound variables).
    if var_idx >= variables.len() {
        // Check that every relation is satisfied with current bindings.
        if verify_all_relations(write_path, adj_table_id, plan, var_bindings, stats).await? {
            // Build a result row from the bindings; the ORDER BY sink takes
            // the JSON bindings too, to evaluate the hidden sort keys.
            let (row, json_bindings) =
                project_bindings(write_path, plan, var_bindings, return_clause, stats, schema)
                    .await?;
            acc.push(row, &json_bindings)?;
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
                    write_path,
                    adj_table_id,
                    &src_key,
                    rel.edge_label.as_deref(),
                )
                .await?;
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
                let partition = write_path.read(adj_table_id, &dst_key).await?;
                let mut neighbors = Vec::new();
                if let Some(p) = partition {
                    for row in &p.rows {
                        // Standard composite: [u16 1][1B direction][...].
                        // Direction sits at offset 2; filter for IN (0x01).
                        if row.clustering.len() >= 3 && row.clustering[2] == 0x01 {
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
        // Fall back to scanning all candidates for this variable — streamed,
        // one partition per pull, instead of materializing the whole table
        // (t_3f2f961a).
        if let Some(table) = plan.var_tables.get(current_var) {
            let table_id = TableId::new(&table.keyspace, &table.table);
            let mut candidates = write_path.range_read_stream_all(&table_id, 0).await?;

            while let Some(candidate) = candidates.next().await {
                let candidate = candidate?;
                stats.vertices_read += 1;
                let key_bytes = candidate.key.key.as_bytes().to_vec();
                var_bindings.insert(current_var.clone(), key_bytes);
                Box::pin(enumerate_bindings(
                    write_path,
                    adj_table_id,
                    plan,
                    variables,
                    var_idx + 1,
                    var_bindings,
                    acc,
                    return_clause,
                    columns,
                    config,
                    start,
                    stats,
                    schema,
                ))
                .await?;
            }
            var_bindings.remove(current_var);
        }
        return Ok(());
    }

    // Leapfrog join: intersect all iterators for the current variable. The
    // intersection is bounded by the smallest adjacency list, which is already
    // resident — truncating it (`max_result_rows`, removed) only dropped valid
    // join results without saving memory.
    // Stream the intersection. Nothing is collected: each common value is
    // consumed and its subtree enumerated before the next is produced, so a
    // large intersection costs no memory here regardless of size.
    while let Some(binding) = next_common_value(&mut iterators) {
        check_timeout(start, config.query_timeout)?;
        var_bindings.insert(current_var.clone(), binding);
        Box::pin(enumerate_bindings(
            write_path,
            adj_table_id,
            plan,
            variables,
            var_idx + 1,
            var_bindings,
            acc,
            return_clause,
            columns,
            config,
            start,
            stats,
            schema,
        ))
        .await?;
    }
    var_bindings.remove(current_var);

    Ok(())
}

/// Verify that all relations in the plan are satisfied by the current bindings.
///
/// This checks "closing" edges -- relations between two variables that were both
/// already bound before the other was enumerated (e.g., the `(c)->(a)` edge
/// in a triangle where `a` was the anchor and `c` was the last variable eliminated).
async fn verify_all_relations(
    write_path: &dyn GraphWrite,
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
        // A failed read must fail the join: as "edge absent" it silently
        // dropped a valid match.
        let partition = write_path.read(adj_table_id, &src_key).await?;
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
/// Returns the JSON bindings the row was evaluated against alongside it.
async fn project_bindings(
    write_path: &dyn GraphWrite,
    plan: &WcoJoinPlan,
    var_bindings: &HashMap<String, Vec<u8>>,
    return_clause: &ReturnClause,
    stats: &mut QueryStats,
    schema: Option<&ferrosa_schema::Schema>,
) -> Result<(Vec<serde_json::Value>, HashMap<String, serde_json::Value>)> {
    // Build JSON bindings for eval_expr by reading each variable's partition.
    let mut json_bindings: HashMap<String, serde_json::Value> = HashMap::new();

    for (var, key_bytes) in var_bindings {
        let hex_id = hex::encode(key_bytes);
        if let Some(table) = plan.var_tables.get(var) {
            let table_id = TableId::new(&table.keyspace, &table.table);
            let dk = DecoratedKey::new(PartitionKey::new(key_bytes.clone()));
            let partition = write_path.read(&table_id, &dk).await?;
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

    Ok((row, json_bindings))
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

    /// Drain the streaming intersection for assertions.
    ///
    /// Test-only on purpose: production consumes `next_common_value` one value
    /// at a time and never holds the intersection in memory.
    fn drain_intersection(iterators: &mut [AdjacencyIterator]) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| next_common_value(iterators)).collect()
    }

    // --- storage errors must fail the join, not shorten it ----------------

    /// Delegates to a real `WritePath`, except that every point read of the
    /// adjacency table fails — the shape of a storage I/O error mid-join.
    struct FailingAdjacencyReads {
        inner: ferrosa_cluster::write_path::WritePath,
        adjacency: TableId,
    }

    #[async_trait::async_trait]
    impl GraphWrite for FailingAdjacencyReads {
        async fn read(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
        ) -> ferrosa_common::Result<Option<ferrosa_sstable::types::Partition>> {
            if table_id == &self.adjacency {
                return Err(ferrosa_common::Error::InvalidFormat(
                    "injected adjacency read failure".to_string(),
                ));
            }
            self.inner.read(table_id, key).await
        }
        async fn pk_read(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
            cl: ferrosa_cluster::consistency::ConsistencyLevel,
            strategy: &ferrosa_cluster::ring::strategy::ReplicationStrategy,
        ) -> ferrosa_common::Result<Option<ferrosa_sstable::types::Partition>> {
            GraphWrite::pk_read(&self.inner, table_id, key, cl, strategy).await
        }
        async fn range_read_stream_all(
            &self,
            table_id: &TableId,
            row_limit: usize,
        ) -> ferrosa_cluster::error::Result<ferrosa_cluster::write_path::PartitionResultStream>
        {
            GraphWrite::range_read_stream_all(&self.inner, table_id, row_limit).await
        }
        async fn count_range_with(
            &self,
            table_id: &TableId,
            cl: ferrosa_cluster::consistency::ConsistencyLevel,
            strategy: &ferrosa_cluster::ring::strategy::ReplicationStrategy,
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
        ) -> ferrosa_cluster::error::Result<ferrosa_cluster::write_path::PartitionResultStream>
        {
            GraphWrite::index_read_stream(&self.inner, table_id, index_name, index_key, after).await
        }
        async fn write(
            &self,
            table_id: &TableId,
            key: &DecoratedKey,
            row: ferrosa_sstable::types::Row,
            timestamp: i64,
            cl: ferrosa_cluster::consistency::ConsistencyLevel,
            strategy: &ferrosa_cluster::ring::strategy::ReplicationStrategy,
        ) -> ferrosa_common::Result<()> {
            GraphWrite::write(&self.inner, table_id, key, row, timestamp, cl, strategy).await
        }
        async fn write_batch(
            &self,
            mutations: Vec<ferrosa_storage::Mutation>,
            cl: ferrosa_cluster::consistency::ConsistencyLevel,
            rf: usize,
        ) -> ferrosa_common::Result<()> {
            GraphWrite::write_batch(&self.inner, mutations, cl, rf).await
        }
    }

    /// One vertex `A` in `test_ks.person_v`, behind a write path whose
    /// adjacency reads all fail.
    fn failing_adjacency_fixture() -> (tempfile::TempDir, FailingAdjacencyReads) {
        use ferrosa_common::schema::TableSchema;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(
            ferrosa_storage::StorageEngine::new(
                ferrosa_storage::StorageEngineConfig::test_config(dir.path()),
                None,
            )
            .unwrap(),
        );
        storage
            .register_table(TableSchema {
                keyspace: "test_ks".to_string(),
                table: "person_v".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: Default::default(),
            })
            .unwrap();
        storage
            .write(
                &TableId::new("test_ks", "person_v"),
                &DecoratedKey::new(PartitionKey::new(b"A".to_vec())),
                Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1),
                },
                1,
            )
            .unwrap();
        let adjacency = TableId::new(adjacency_keyspace_name("test_ks"), "adjacency");
        let wp = FailingAdjacencyReads {
            inner: ferrosa_cluster::write_path::WritePath::direct(storage),
            adjacency,
        };
        (dir, wp)
    }

    fn person() -> crate::planner::logical::ResolvedTable {
        crate::planner::logical::ResolvedTable {
            keyspace: "test_ks".to_string(),
            table: "person_v".to_string(),
            graph_type: "vertex".to_string(),
            label: "Person".to_string(),
        }
    }

    fn relation(src: &str, dst: &str) -> crate::planner::physical::JoinRelation {
        crate::planner::physical::JoinRelation {
            src_var: src.to_string(),
            dst_var: dst.to_string(),
            direction: crate::parser::Direction::Out,
            edge_label: Some("KNOWS".to_string()),
            edge_table: None,
        }
    }

    /// Each WCO adjacency read site used `.ok().flatten()`, so a storage error
    /// read as "this vertex has no neighbours" and the join returned a
    /// silently SHORT answer — `Ok` with fewer rows than exist. One plan per
    /// site, each shaped so that site performs the first adjacency read:
    /// - `a->b`: the OUT iterator for `b` (`AdjacencyIterator::from_partition`);
    /// - `b->a`: the IN-adjacency read for `b` in `enumerate_bindings`;
    /// - `a->a`: the closing-edge check (`verify_all_relations`).
    #[tokio::test]
    async fn a_failed_adjacency_read_fails_the_join_instead_of_shortening_it() {
        let cases: [(
            &str,
            Vec<String>,
            Vec<crate::planner::physical::JoinRelation>,
        ); 3] = [
            (
                "from_partition",
                vec!["a".to_string(), "b".to_string()],
                vec![relation("a", "b")],
            ),
            (
                "IN-adjacency read",
                vec!["a".to_string(), "b".to_string()],
                vec![relation("b", "a")],
            ),
            (
                "verify_all_relations",
                vec!["a".to_string()],
                vec![relation("a", "a")],
            ),
        ];
        for (site, variables, relations) in cases {
            let (_dir, wp) = failing_adjacency_fixture();
            let var_tables = variables.iter().map(|v| (v.clone(), person())).collect();
            let plan = WcoJoinPlan {
                variables,
                relations,
                var_tables,
            };
            let return_clause = ReturnClause {
                distinct: false,
                items: vec![crate::parser::ReturnItem {
                    expr: crate::parser::Expr::Var("a".to_string()),
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            };
            let result = execute_wco_join(
                &wp,
                "test_ks",
                &plan,
                &return_clause,
                &GraphEngineConfig::default(),
                Instant::now(),
                None,
                None,
            )
            .await;
            match result {
                Err(err) => assert!(
                    err.to_string().contains("injected adjacency read failure"),
                    "{site}: the storage error must surface, got {err}"
                ),
                Ok(ok) => panic!(
                    "{site}: a failed adjacency read must fail the join, but it \
                     returned Ok with {} rows",
                    ok.rows.len()
                ),
            }
        }
    }

    #[test]
    fn leapfrog_join_basic() {
        // Three sorted iterators with known intersection {3, 5}.
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2], vec![3], vec![5], vec![7]]),
            AdjacencyIterator::from_sorted(vec![vec![2], vec![3], vec![5], vec![6]]),
            AdjacencyIterator::from_sorted(vec![vec![3], vec![4], vec![5], vec![8]]),
        ];

        let results = drain_intersection(&mut iters);
        assert_eq!(results, vec![vec![3], vec![5]]);
    }

    #[test]
    fn leapfrog_join_empty_intersection() {
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2]]),
            AdjacencyIterator::from_sorted(vec![vec![3], vec![4]]),
            AdjacencyIterator::from_sorted(vec![vec![5], vec![6]]),
        ];

        let results = drain_intersection(&mut iters);
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

        let results = drain_intersection(&mut iters);
        assert_eq!(results, vec![vec![1], vec![2], vec![3]]);
    }

    /// The intersection is produced incrementally, not collected.
    ///
    /// Completeness alone would still pass if the implementation built the
    /// whole `Vec` first, which is what it used to do -- and a result set that
    /// must fit in RAM to be correct is a cap waiting to be reintroduced. This
    /// pins the shape: one value per call, with the iterators left positioned
    /// for the next, so a caller can consume an intersection larger than
    /// memory.
    #[test]
    fn the_intersection_is_produced_one_value_at_a_time() {
        let shared: Vec<Vec<u8>> = (0..500u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let mut iters = vec![
            AdjacencyIterator::from_sorted(shared.clone()),
            AdjacencyIterator::from_sorted(shared.clone()),
        ];

        // Take three values and stop. A collecting implementation would have
        // computed all 500 to hand back the first.
        let first = next_common_value(&mut iters).expect("first");
        let second = next_common_value(&mut iters).expect("second");
        let third = next_common_value(&mut iters).expect("third");
        assert_eq!(vec![first, second, third], shared[..3].to_vec());

        // Resuming continues from where it stopped rather than restarting.
        let rest: Vec<Vec<u8>> = std::iter::from_fn(|| next_common_value(&mut iters)).collect();
        assert_eq!(
            rest,
            shared[3..].to_vec(),
            "the iterators must stay positioned between calls; restarting would \
             duplicate answers and re-read the adjacency lists"
        );

        // Exhausted means exhausted, not wrapped.
        assert!(next_common_value(&mut iters).is_none());
    }

    /// The intersection is returned complete, however large it is.
    ///
    /// Replaces `leapfrog_join_respects_max_results`, which asserted the
    /// opposite: that the join stopped at a caller-supplied count. It passed
    /// while the production call site defeated the cap with `usize::MAX`, so
    /// the only thing it pinned was the ability to truncate a result set — the
    /// behaviour worth preventing, not preserving.
    ///
    /// 500 sits well past the old 100-row default, so a reintroduced cap fails
    /// here rather than in a query someone runs later.
    #[test]
    fn leapfrog_join_returns_the_complete_intersection() {
        let shared: Vec<Vec<u8>> = (0..500u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let mut iters = vec![
            AdjacencyIterator::from_sorted(shared.clone()),
            AdjacencyIterator::from_sorted(shared.clone()),
        ];

        let results = drain_intersection(&mut iters);

        assert_eq!(
            results.len(),
            shared.len(),
            "every value present in both iterators is an answer; dropping one \
             returns a wrong result, not a partial one"
        );
        assert_eq!(results, shared);
    }

    #[test]
    fn leapfrog_join_empty_iterators() {
        let mut iters: Vec<AdjacencyIterator> = vec![];
        let results = drain_intersection(&mut iters);
        assert!(results.is_empty());
    }

    #[test]
    fn leapfrog_join_one_empty_iterator() {
        let mut iters = vec![
            AdjacencyIterator::from_sorted(vec![vec![1], vec![2]]),
            AdjacencyIterator::from_sorted(vec![]),
        ];

        let results = drain_intersection(&mut iters);
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
