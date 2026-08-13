//! SPARQL query executor.
//!
//! Executes a [`QueryPlan`] against ferrosa's StorageEngine, producing
//! binding sets that are serialized into SPARQL results.
//!
//! # Streaming scans
//!
//! Triple-pattern scans STREAM. A scan pulls one partition at a time from
//! [`WritePath::range_read_stream_all`], decodes its rows, filters them, and
//! pushes each surviving triple straight into the binding loop. There is no
//! intermediate `Vec` of fetched triples, so the memory a scan costs is one
//! partition — not the table. This is what stops `SELECT * WHERE { ?s ?p ?o }`
//! (30 bytes of query) from materializing an entire triple store.
//!
//! # Bounds
//!
//! [`ExecutionLimits::max_rows`] is the executor's ONE row bound. It is checked
//! at the source (storage rows read by a scan) and on every operator's solution
//! buffer. Exceeding it is an ERROR, never a truncation: a clipped result
//! reported as complete is worse than a failure, because the caller cannot tell
//! the difference. `LIMIT` is pushed INTO the scan whenever it is safe to do so
//! — one triple pattern, no ORDER BY, DISTINCT, FILTER, or graph query form —
//! so a bounded query does bounded work rather than reading the whole table and
//! throwing most of it away.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sstable::types::{Partition, Row};
use futures::StreamExt;

use crate::error::SparqlError;
use crate::planner::{QueryPlan, TripleOp, TripleScope};
use crate::results::{Binding, SparqlJsonResults};
use crate::triple_store;

/// Default value for [`ExecutionLimits::max_rows`].
///
/// Sized so that the worst case an unconstrained query can buffer — one
/// solution row per storage row — stays in tens of megabytes rather than
/// exhausting the server. Deployments with larger stores should raise it
/// deliberately via `SparqlConfig::max_rows` and accept the memory cost.
pub const DEFAULT_MAX_ROWS: usize = 100_000;

/// The executor's resource bounds.
///
/// One number, enforced in two places, both of them real:
///
/// - **at the source** — the number of storage rows a single triple-pattern
///   scan may read, checked as rows are pulled from the partition stream;
/// - **at each operator** — the number of solutions a pattern may buffer,
///   checked as solutions are appended.
///
/// Crossing either is a [`SparqlError::Execution`]. Nothing is silently
/// truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    /// Maximum storage rows read per scan, and maximum solutions buffered per
    /// operator.
    pub max_rows: usize,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
        }
    }
}

/// Execute a query plan and return SPARQL JSON results.
pub async fn execute(
    plan: &QueryPlan,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<SparqlJsonResults, SparqlError> {
    let mut binding_sets = evaluate_triple_patterns(plan, write_path, limits).await?;

    // Apply FILTER expressions to binding sets.
    if !plan.filters.is_empty() {
        binding_sets.retain(|row| {
            plan.filters
                .iter()
                .all(|expr| crate::filter::eval_filter(expr, row))
        });
    }

    // BUG-S13 / URS-QEC-S04: apply ORDER BY (evaluates expressions per solution;
    // fails loud on unsupported forms).
    apply_order_by(&mut binding_sets, &plan.order_by)?;

    // BUG-S13: apply DISTINCT.
    if plan.distinct {
        apply_distinct(&mut binding_sets, &plan.projection);
    }

    // BUG-S5 fix: clamp start to len so slicing never panics.
    //
    // `offset` and `limit` come straight out of the query text and are
    // attacker-controlled `usize` values, so `start + limit` MUST saturate:
    // `OFFSET 1 LIMIT 18446744073709551615` panics in debug and wraps to 0 in
    // release (silently returning no rows) with an unchecked add. Do not rely
    // on the trailing `.min(len)` to mask the wrap — it only happens to work
    // because a materialized `Vec` has a `len()` to clamp against.
    let start = plan.offset.unwrap_or(0).min(binding_sets.len());
    let end = plan
        .limit
        .map(|l| start.saturating_add(l))
        .unwrap_or(binding_sets.len())
        .min(binding_sets.len());

    let mut results = SparqlJsonResults::new(plan.projection.clone());
    for row in &binding_sets[start..end] {
        let projected: HashMap<String, Binding> = plan
            .projection
            .iter()
            .filter_map(|var| row.get(var).map(|b| (var.clone(), b.clone())))
            .collect();
        results.add_row(projected);
    }

    Ok(results)
}

/// Evaluate a plan's WHERE pattern and return the raw solution bindings
/// (after FILTER, before projection/ORDER/DISTINCT/LIMIT).
///
/// Used by SPARQL UPDATE pattern-deletes (`DELETE WHERE`,
/// `DELETE/INSERT … WHERE`): the full, unprojected binding set is needed so
/// every matched solution can be substituted into the delete/insert templates.
pub async fn execute_bindings(
    plan: &QueryPlan,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let mut binding_sets = evaluate_triple_patterns(plan, write_path, limits).await?;

    if !plan.filters.is_empty() {
        binding_sets.retain(|row| {
            plan.filters
                .iter()
                .all(|expr| crate::filter::eval_filter(expr, row))
        });
    }

    Ok(binding_sets)
}

/// Evaluate all triple patterns via nested-loop join, returning binding sets.
///
/// SCOPE NOTE (unchanged by the streaming work): the join itself is still a
/// nested loop that rebuilds a materialized `binding_sets` per pattern, so for
/// a multi-pattern query the peak memory is set by the INTERMEDIATE solution
/// sets, not by the scan. Those intermediates are now *bounded* — appending
/// past [`ExecutionLimits::max_rows`] is a loud error — but they are not
/// pipelined. Pipelining the join is a separate redesign.
async fn evaluate_triple_patterns(
    plan: &QueryPlan,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let mut binding_sets: Vec<HashMap<String, Binding>> = vec![HashMap::new()];
    let budget = scan_budget(plan);

    for (tp, op) in &plan.ops {
        let new_bindings = match op {
            TripleOp::PropertyPath {
                scope,
                subject,
                path,
                object,
            } => {
                evaluate_path_op(
                    scope,
                    subject,
                    path,
                    object,
                    &binding_sets,
                    write_path,
                    limits,
                )
                .await?
            }
            _ => evaluate_standard_op(tp, op, &binding_sets, write_path, limits, budget).await?,
        };
        binding_sets = new_bindings;
    }

    Ok(binding_sets)
}

/// How many solutions a single-pattern scan must produce before the executor
/// can stop reading storage — the LIMIT pushdown budget.
///
/// `LIMIT` may only be pushed into the scan when every operator between the
/// scan and the LIMIT preserves both the row count and the row order:
///
/// - exactly one triple pattern — a join re-pairs solutions, so pattern *n*'s
///   rows are not the query's rows;
/// - no `ORDER BY` and no `DISTINCT` — both are blocking, and the top *n* of a
///   sorted or deduplicated sequence is not the first *n* rows read;
/// - no `FILTER` — a filter drops solutions after binding, so stopping at *n*
///   bound rows can return fewer than *n* results;
/// - no `CONSTRUCT`/`DESCRIBE`, whose graph result is derived from every
///   solution rather than from the projected window.
///
/// `None` means "read every matching row". That is still bounded by
/// [`ExecutionLimits::max_rows`] — it is unbounded LIMIT, not unbounded work.
fn scan_budget(plan: &QueryPlan) -> Option<usize> {
    if plan.ops.len() != 1
        || !plan.order_by.is_empty()
        || plan.distinct
        || !plan.filters.is_empty()
        || plan.graph_mode.is_some()
    {
        return None;
    }
    // OFFSET rows are consumed and discarded downstream, so the scan must
    // produce `offset + limit` solutions. Saturating: both come from the query
    // text and `usize::MAX` is a legal LIMIT.
    plan.limit
        .map(|l| plan.offset.unwrap_or(0).saturating_add(l))
}

/// Evaluate a property path op via BFS traversal.
async fn evaluate_path_op(
    scope: &TripleScope,
    subject: &spargebra::term::TermPattern,
    path: &spargebra::algebra::PropertyPathExpression,
    object: &spargebra::term::TermPattern,
    existing_bindings: &[HashMap<String, Binding>],
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let results = crate::property_path::evaluate_property_path(
        subject, path, object, scope, write_path, limits,
    )
    .await?;
    let path_bindings = crate::property_path::path_results_to_bindings(subject, object, &results);

    let mut new_bindings = Vec::new();
    for existing in existing_bindings {
        for pb in &path_bindings {
            if let Some(merged) = try_merge_bindings(existing, pb) {
                new_bindings.push(merged);
                check_solution_bound(new_bindings.len(), limits)?;
            }
        }
    }
    Ok(new_bindings)
}

/// Evaluate a standard (non-path) triple pattern op against a STREAMING scan.
///
/// The loop nesting is triple-major (`for each streamed triple { for each
/// existing binding }`) rather than the binding-major form it replaced. That
/// inversion is what lets the triple side stream: a triple is bound and dropped
/// before the next one is pulled, so no `Vec<FetchedTriple>` ever exists. For
/// the single-pattern case (`existing_bindings == [{}]`) the emitted order is
/// identical to the old code; for a join the solution order changes, which
/// SPARQL bag semantics do not constrain in the absence of `ORDER BY`.
async fn evaluate_standard_op(
    tp: &spargebra::term::TriplePattern,
    op: &TripleOp,
    existing_bindings: &[HashMap<String, Binding>],
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
    budget: Option<usize>,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let mut new_bindings: Vec<HashMap<String, Binding>> = Vec::new();
    for_each_triple(op, write_path, limits, |triple| {
        for existing in existing_bindings {
            let Some(row) = try_bind_triple(tp, &triple, existing) else {
                continue;
            };
            new_bindings.push(row);
            check_solution_bound(new_bindings.len(), limits)?;
            if budget.is_some_and(|needed| new_bindings.len() >= needed) {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    })
    .await?;
    Ok(new_bindings)
}

/// Fail loud when an operator's solution buffer crosses the row bound.
fn check_solution_bound(buffered: usize, limits: &ExecutionLimits) -> Result<(), SparqlError> {
    if buffered > limits.max_rows {
        return Err(SparqlError::Execution(format!(
            "SPARQL query buffered more than {} solutions. Refusing to return a \
             truncated result that would look complete — add a LIMIT, constrain the \
             pattern, or raise the engine's max_rows bound.",
            limits.max_rows
        )));
    }
    Ok(())
}

/// Merge two binding sets if compatible (no conflicting values).
fn try_merge_bindings(
    a: &HashMap<String, Binding>,
    b: &HashMap<String, Binding>,
) -> Option<HashMap<String, Binding>> {
    let mut merged = a.clone();
    for (key, val) in b {
        if !try_insert_binding(&mut merged, key, val) {
            return None;
        }
    }
    Some(merged)
}

/// Try to bind a fetched triple into an existing binding row.
/// Returns `None` if the triple does not match the pattern, or is incompatible
/// with the existing bindings.
///
/// BUG-S4 fix: checks both value AND binding_type for compatibility.
///
/// t_c3a2d3e4 fix: a CONSTANT term in the pattern is a match constraint, not
/// decoration. Every position is checked, not only the ones the access path
/// happened to push down — a `PredicateScan` is chosen on the predicate alone
/// and would otherwise return every object, and a `SubjectLookup` is chosen on
/// the subject alone. A blank node in a pattern is a non-selectable variable
/// per SPARQL 1.1 §4.1.4, so it constrains nothing.
fn try_bind_triple(
    tp: &spargebra::term::TriplePattern,
    triple: &FetchedTriple,
    existing: &HashMap<String, Binding>,
) -> Option<HashMap<String, Binding>> {
    let (s, p, o, obj_type, datatype, lang) = triple;
    let mut row = existing.clone();

    if !bind_or_check_subject(&tp.subject, s, &mut row)
        || !bind_or_check_predicate(&tp.predicate, p, &mut row)
        || !bind_or_check_object(&tp.object, o, obj_type, datatype, lang, &mut row)
    {
        return None;
    }
    Some(row)
}

/// Bind the subject variable, or check a subject constant.
///
/// BUG-S11 fix: detect blank nodes by the `_:` prefix instead of assuming URI.
fn bind_or_check_subject(
    pattern: &spargebra::term::TermPattern,
    subject: &str,
    row: &mut HashMap<String, Binding>,
) -> bool {
    use spargebra::term::TermPattern;
    match pattern {
        TermPattern::Variable(var) => {
            let binding = Binding {
                binding_type: if subject.starts_with("_:") {
                    "bnode"
                } else {
                    "uri"
                }
                .into(),
                value: subject.to_string(),
                datatype: None,
                lang: None,
            };
            try_insert_binding(row, var.as_str(), &binding)
        }
        TermPattern::NamedNode(n) => n.as_str() == subject,
        // A blank node in the PATTERN is a non-selectable variable and
        // constrains nothing; a quoted triple is rejected by the planner.
        _ => true,
    }
}

/// Bind the predicate variable, or check a predicate constant.
fn bind_or_check_predicate(
    pattern: &spargebra::term::NamedNodePattern,
    predicate: &str,
    row: &mut HashMap<String, Binding>,
) -> bool {
    use spargebra::term::NamedNodePattern;
    match pattern {
        NamedNodePattern::Variable(var) => {
            let binding = Binding {
                binding_type: "uri".into(),
                value: predicate.to_string(),
                datatype: None,
                lang: None,
            };
            try_insert_binding(row, var.as_str(), &binding)
        }
        NamedNodePattern::NamedNode(n) => n.as_str() == predicate,
    }
}

/// Bind the object variable, or check an object constant.
fn bind_or_check_object(
    pattern: &spargebra::term::TermPattern,
    object: &str,
    obj_type: &str,
    datatype: &Option<String>,
    lang: &Option<String>,
    row: &mut HashMap<String, Binding>,
) -> bool {
    use spargebra::term::TermPattern;
    match pattern {
        TermPattern::Variable(var) => {
            let binding = Binding {
                binding_type: obj_type.to_string(),
                value: object.to_string(),
                datatype: datatype.clone(),
                lang: lang.clone(),
            };
            try_insert_binding(row, var.as_str(), &binding)
        }
        // An IRI constant matches only a stored IRI object with the same value
        // — never a literal that happens to spell the same characters.
        TermPattern::NamedNode(n) => obj_type == "uri" && n.as_str() == object,
        // A literal constant is compared against the stored LEXICAL value.
        // `Literal::to_string()` is the quoted N-Triples serialization and can
        // never equal a stored object (t_c3a2d3e4).
        TermPattern::Literal(l) => obj_type == "literal" && l.value() == object,
        _ => true,
    }
}

/// Insert a binding into a row, or check compatibility if already present.
///
/// BUG-S4 fix: compares both `value` and `binding_type`.
/// Returns `false` if the existing binding is incompatible.
fn try_insert_binding(row: &mut HashMap<String, Binding>, name: &str, binding: &Binding) -> bool {
    if let Some(existing) = row.get(name) {
        existing.value == binding.value && existing.binding_type == binding.binding_type
    } else {
        row.insert(name.to_string(), binding.clone());
        true
    }
}

/// Sort binding sets by ORDER BY conditions (BUG-S13, URS-QEC-S04).
///
/// Each condition holds a full expression (a plain `?var` is the trivial
/// `Variable` expression). The expression is evaluated per solution and the
/// solutions are sorted by the result, numerically when both sides are numeric.
///
/// URS-QEC-X01 (fail loud): if any ORDER BY expression contains a sub-form this
/// engine cannot evaluate, return an error instead of silently sorting those
/// rows as equal (which would yield an arbitrary, undocumented order).
fn apply_order_by(
    binding_sets: &mut [HashMap<String, Binding>],
    order_by: &[crate::planner::OrderCondition],
) -> Result<(), SparqlError> {
    if order_by.is_empty() {
        return Ok(());
    }
    for cond in order_by {
        if let Some(what) = crate::filter::unsupported_expr(&cond.expression) {
            return Err(SparqlError::Plan(format!(
                "ORDER BY expression is not supported: {what}; \
                 cannot evaluate it per solution, refusing to return an \
                 arbitrarily-ordered result"
            )));
        }
    }
    binding_sets.sort_by(|a, b| {
        for cond in order_by {
            let ord = crate::filter::order_cmp(&cond.expression, a, b);
            let ord = if cond.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

/// Remove duplicate binding rows (BUG-S13).
fn apply_distinct(binding_sets: &mut Vec<HashMap<String, Binding>>, projection: &[String]) {
    let mut seen = std::collections::HashSet::new();
    binding_sets.retain(|row| {
        let key: Vec<(&str, &str, &str)> = projection
            .iter()
            .map(|var| {
                row.get(var)
                    .map(|b| (var.as_str(), b.value.as_str(), b.binding_type.as_str()))
                    .unwrap_or((var.as_str(), "", ""))
            })
            .collect();
        let key_str = format!("{key:?}");
        seen.insert(key_str)
    });
}

/// A fetched triple: (subject, predicate, object, object_type, datatype, language).
type FetchedTriple = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

/// Name of the secondary index on the object column.
const OBJECT_INDEX_NAME: &str = "rdf_triples_object_idx";

/// The graph a storage-backed op reads from. `None` for `PropertyPath`, which
/// is evaluated by [`evaluate_path_op`] and never reaches the scan.
fn op_scope(op: &TripleOp) -> Option<&TripleScope> {
    match op {
        // BUG-S2 fix: use the graph from the execution plan, not hardcoded "rdf".
        // The scope carries the keyspace (which table) and the graph (which
        // partition) separately; collapsing them into one value was the
        // bound-subject-returns-nothing bug.
        TripleOp::SubjectLookup { scope, .. }
        | TripleOp::PredicateScan { scope, .. }
        | TripleOp::ObjectScan { scope, .. }
        | TripleOp::FullScan { scope, .. } => Some(scope),
        TripleOp::PropertyPath { .. } => None,
    }
}

/// Human-readable access path, for bound-exceeded diagnostics.
fn describe_op(op: &TripleOp) -> String {
    match op {
        TripleOp::SubjectLookup { subject, .. } => format!("a subject lookup of <{subject}>"),
        TripleOp::PredicateScan { predicate, .. } => format!("a scan for predicate <{predicate}>"),
        TripleOp::ObjectScan { object, .. } => format!("a scan for object '{object}'"),
        TripleOp::FullScan { scope } => {
            format!("a full scan of graph '{}'", scope.graph)
        }
        TripleOp::PropertyPath { .. } => "a property path".to_string(),
    }
}

/// Per-scan state: the caller's sink plus the storage-row counter that enforces
/// [`ExecutionLimits::max_rows`] AT THE SOURCE — where the unbounded data
/// enters, not after it has already been materialized.
struct Scan<'a, F> {
    op: &'a TripleOp,
    limits: &'a ExecutionLimits,
    sink: F,
    rows_read: usize,
}

impl<F> Scan<'_, F>
where
    F: FnMut(FetchedTriple) -> Result<ControlFlow<()>, SparqlError>,
{
    /// Decode one partition and push its matching triples to the sink.
    ///
    /// Nothing accumulates here: a row is decoded, filtered, handed to the sink
    /// and dropped before the next one is touched.
    fn feed(&mut self, partition: &Partition) -> Result<ControlFlow<()>, SparqlError> {
        // BUG-S3 fix: decode composite partition key (graph, subject) properly.
        let subject = extract_subject_from_partition_key(partition.key.key.as_bytes());
        for row in &partition.rows {
            self.rows_read += 1;
            if self.rows_read > self.limits.max_rows {
                return Err(SparqlError::Execution(format!(
                    "SPARQL scan read more than {} storage rows while evaluating {}. \
                     Refusing to return a truncated result that would look complete — \
                     add a LIMIT, constrain the pattern, or raise the engine's max_rows \
                     bound.",
                    self.limits.max_rows,
                    describe_op(self.op),
                )));
            }
            let Some(triple) = decode_triple(row, &subject) else {
                continue;
            };
            if !triple_matches_op(self.op, &triple) {
                continue;
            }
            if (self.sink)(triple)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}

/// Drive `op`'s access path, handing every matching triple to `sink`.
///
/// This is the streaming source. A range scan pulls ONE PARTITION AT A TIME
/// from [`WritePath::range_read_stream_all`] — the same streaming primitive the
/// CQL path uses — instead of collecting the whole table into a `Vec` first.
/// `sink` returns [`ControlFlow::Break`] to stop the scan early, which is how
/// `LIMIT` pushdown terminates a scan without reading the rest of the table.
async fn for_each_triple<F>(
    op: &TripleOp,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
    sink: F,
) -> Result<(), SparqlError>
where
    F: FnMut(FetchedTriple) -> Result<ControlFlow<()>, SparqlError>,
{
    let Some(scope) = op_scope(op) else {
        return Ok(());
    };
    // The TABLE is named by the keyspace; the partition key is (GRAPH, subject).
    // These were the same value until this fix, which is why a point read
    // missed rows a scan of the same table found.
    let table_id = triple_store::triples_table_id(&scope.keyspace);
    let mut scan = Scan {
        op,
        limits,
        sink,
        rows_read: 0,
    };

    match op {
        TripleOp::SubjectLookup { subject, .. } => {
            // Point read: bounded by one partition's row count by construction.
            let key = triple_store::partition_key(&scope.graph, subject);
            if let Some(partition) = write_path.read(&table_id, &key).await? {
                // Exactly one partition: `Break` and `Continue` are equivalent
                // here because there is nothing left to feed either way.
                let _ = scan.feed(&partition)?;
            }
        }
        TripleOp::ObjectScan { object, .. } => {
            // Keyed index read first: bounded by the number of index matches.
            let index_key = ferrosa_index::IndexKey(object.as_bytes().to_vec());
            let indexed = write_path
                .index_read(&table_id, OBJECT_INDEX_NAME, &index_key)
                .await?;
            if indexed.is_empty() {
                tracing::warn!(
                    object,
                    "ObjectScan: no secondary index hit; falling back to a streaming \
                     full scan with filtering"
                );
                stream_scan(&table_id, write_path, &mut scan).await?;
            } else {
                for partition in &indexed {
                    if scan.feed(partition)?.is_break() {
                        break;
                    }
                }
            }
        }
        TripleOp::PredicateScan { .. } | TripleOp::FullScan { .. } => {
            stream_scan(&table_id, write_path, &mut scan).await?;
        }
        TripleOp::PropertyPath { .. } => {}
    }
    Ok(())
}

/// Consume a streaming range scan, feeding each partition to `scan` as it
/// arrives and stopping as soon as the sink is satisfied.
async fn stream_scan<F>(
    table_id: &ferrosa_storage::TableId,
    write_path: &Arc<WritePath>,
    scan: &mut Scan<'_, F>,
) -> Result<(), SparqlError>
where
    F: FnMut(FetchedTriple) -> Result<ControlFlow<()>, SparqlError>,
{
    // `row_limit = 0` means "do not truncate rows within a partition"; the
    // executor's own bound and LIMIT budget decide when to stop.
    let mut partitions = write_path.range_read_stream_all(table_id, 0).await?;
    while let Some(partition) = partitions.next().await {
        if scan.feed(&partition?)?.is_break() {
            break;
        }
    }
    Ok(())
}

/// BUG-S3 fix: Properly decode CQL composite partition key to extract the subject.
///
/// Composite key format: `[u16 len][bytes][0x00]` repeated for each component.
/// Component 0 = graph, component 1 = subject.
fn extract_subject_from_partition_key(key_bytes: &[u8]) -> String {
    match extract_composite_component(key_bytes, 1) {
        Some(s) => s,
        None => {
            tracing::warn!(
                key_len = key_bytes.len(),
                "failed to decode subject from composite partition key; \
                 falling back to raw bytes"
            );
            String::from_utf8_lossy(key_bytes).to_string()
        }
    }
}

/// Extract the Nth component from a CQL composite key.
///
/// Format: `[u16 len][bytes][0x00 separator]` per component.
fn extract_composite_component(data: &[u8], position: usize) -> Option<String> {
    let mut offset = 0;
    for i in 0..=position {
        if offset + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + len > data.len() {
            return None;
        }
        if i == position {
            return Some(String::from_utf8_lossy(&data[offset..offset + len]).to_string());
        }
        offset += len;
        // Skip end-of-component separator byte.
        if offset < data.len() && data[offset] == 0 {
            offset += 1;
        }
    }
    None
}

/// Decode one storage row into a triple, or `None` if it is not a live triple.
///
/// Returns `None` for a row whose deletion marker is non-live: that row is a
/// tombstone (or is masked by one in the merge) and MUST NOT surface as a
/// triple. Without this, a SPARQL delete would remain visible to a subsequent
/// SELECT (URS-QEC-D05).
fn decode_triple(row: &Row, subject: &str) -> Option<FetchedTriple> {
    if !row.deletion.is_live() {
        return None;
    }

    let predicate = extract_clustering_string(&row.clustering, 0);
    let object = extract_clustering_string(&row.clustering, 1);

    // BUG-S18 fix: validate object type against known values.
    let raw_obj_type =
        cell_string(row, triple_store::COL_OBJECT_TYPE).unwrap_or_else(|| "literal".to_string());
    let obj_type = match raw_obj_type.as_str() {
        "uri" | "literal" | "bnode" => raw_obj_type,
        other => {
            tracing::warn!(value = other, "invalid object type, defaulting to literal");
            "literal".into()
        }
    };

    Some((
        subject.to_string(),
        predicate,
        object,
        obj_type,
        cell_string(row, triple_store::COL_DATATYPE),
        cell_string(row, triple_store::COL_LANGUAGE),
    ))
}

/// Read a regular column's value out of a row as a UTF-8 string.
fn cell_string(row: &Row, column: u16) -> Option<String> {
    row.cells
        .iter()
        .find(|(idx, _)| *idx == column)
        .and_then(|(_, cell)| cell.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).to_string())
}

/// Access-path filter: does this triple satisfy the constraint the op was
/// chosen on?
///
/// This is the pushdown half of matching — it discards non-matching rows at the
/// scan, before they ever become solutions. It is deliberately NOT the whole
/// story: [`try_bind_triple`] enforces every constant in the triple pattern,
/// including the ones no access path was chosen on. Applying it per row rather
/// than to a collected `Vec` is what lets the scan stream.
///
/// BUG-S8 fix: ObjectScan filters by object value.
/// BUG-S9 fix: PredicateScan filters by predicate value.
fn triple_matches_op(op: &TripleOp, triple: &FetchedTriple) -> bool {
    let (_, p, o, _, _, _) = triple;
    match op {
        TripleOp::SubjectLookup {
            predicate_filter: Some(pred),
            ..
        } => p == pred,
        TripleOp::PredicateScan { predicate, .. } => p == predicate,
        TripleOp::ObjectScan { object, .. } => o == object,
        _ => true,
    }
}

/// Extract a string component from a CQL clustering key at the given position.
///
/// Clustering keys use the strict CQL composite encoding written by
/// `update::encode_triple_clustering`: `[u16 len][bytes]` per component with NO
/// separator byte (this is what `ferrosa-common::schema` validates). A trailing
/// separator must NOT be assumed — for short components the next component's
/// length prefix high byte is `0x00`, and skipping it would corrupt the read.
///
/// BUG-S10 fix: logs warnings on malformed/truncated keys instead of
/// returning empty strings silently.
fn extract_clustering_string(clustering: &[u8], position: usize) -> String {
    let mut offset = 0;
    for i in 0..=position {
        if offset + 2 > clustering.len() {
            tracing::warn!(
                position,
                clustering_len = clustering.len(),
                byte_offset = offset,
                "clustering key too short: cannot read length prefix for component"
            );
            return String::new();
        }
        let len = u16::from_be_bytes([clustering[offset], clustering[offset + 1]]) as usize;
        offset += 2;
        if i == position {
            if offset + len > clustering.len() {
                tracing::warn!(
                    position,
                    component_len = len,
                    remaining = clustering.len() - offset,
                    "clustering key truncated: component length exceeds remaining bytes"
                );
                return String::new();
            }
            return String::from_utf8_lossy(&clustering[offset..offset + len]).to_string();
        }
        offset += len;
    }
    tracing::warn!(
        position,
        clustering_len = clustering.len(),
        "clustering key: fell through component loop without finding position"
    );
    String::new()
}

/// Extract the subject string from a partition's composite key.
///
/// Public wrapper for use by [`crate::property_path`].
pub fn extract_subject_from_key(partition: &ferrosa_sstable::types::Partition) -> String {
    extract_subject_from_partition_key(partition.key.key.as_bytes())
}

/// Extract a clustering key component by position.
///
/// Public wrapper for use by [`crate::property_path`].
pub fn clustering_component(clustering: &[u8], position: usize) -> String {
    extract_clustering_string(clustering, position)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a CQL length-prefixed component: [u16 len][bytes][0x00].
    fn encode_component(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
        buf
    }

    // --- BUG-S3: composite partition key decoding ---

    #[test]
    fn extract_composite_component_first() {
        let mut key = encode_component("default");
        key.extend_from_slice(&encode_component("http://example.org/alice"));
        let graph = extract_composite_component(&key, 0);
        assert_eq!(graph.as_deref(), Some("default"));
    }

    #[test]
    fn extract_composite_component_second() {
        let mut key = encode_component("default");
        key.extend_from_slice(&encode_component("http://example.org/alice"));
        let subject = extract_composite_component(&key, 1);
        assert_eq!(subject.as_deref(), Some("http://example.org/alice"));
    }

    #[test]
    fn extract_composite_component_out_of_range() {
        let key = encode_component("default");
        assert!(extract_composite_component(&key, 1).is_none());
    }

    #[test]
    fn extract_subject_from_partition_key_decodes_second_component() {
        let mut key = encode_component("mygraph");
        key.extend_from_slice(&encode_component("http://example.org/bob"));
        let subject = extract_subject_from_partition_key(&key);
        assert_eq!(subject, "http://example.org/bob");
    }

    // --- BUG-S4: binding type compatibility ---

    #[test]
    fn try_insert_binding_compatible() {
        let mut row = HashMap::new();
        let b = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "s", &b));
        assert!(
            try_insert_binding(&mut row, "s", &b),
            "same binding is compatible"
        );
    }

    #[test]
    fn try_insert_binding_incompatible_value() {
        let mut row = HashMap::new();
        let b1 = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        let b2 = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/bob".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "s", &b1));
        assert!(!try_insert_binding(&mut row, "s", &b2));
    }

    #[test]
    fn try_insert_binding_incompatible_type() {
        let mut row = HashMap::new();
        let uri_binding = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        let literal_binding = Binding {
            binding_type: "literal".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "x", &uri_binding));
        assert!(
            !try_insert_binding(&mut row, "x", &literal_binding),
            "different binding_type must be incompatible even with same value"
        );
    }

    // --- BUG-S5: OFFSET/LIMIT clamping ---
    //
    // Covered END TO END by `tests/sparql_executor_invariants.rs`:
    // `offset_skips_exactly_k_solutions` and
    // `limit_and_offset_never_overflow_for_any_usize`.
    //
    // The unit test that used to live here (`offset_clamp_prevents_panic`)
    // re-implemented `start.min(len)` in its own body instead of calling
    // `execute`, so it asserted a property of the test, not of the executor —
    // it could not have detected the `start + limit` overflow that the
    // end-to-end invariant test caught immediately.

    // --- BUG-S13: ORDER BY ---

    #[test]
    fn apply_order_by_sorts_ascending() {
        let mut rows = vec![
            make_binding_row("name", "Charlie"),
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Bob"),
        ];
        let conditions = vec![crate::planner::OrderCondition {
            expression: var_expr("name"),
            ascending: true,
        }];
        apply_order_by(&mut rows, &conditions).unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r["name"].value.as_str()).collect();
        assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
    }

    #[test]
    fn apply_order_by_sorts_descending() {
        let mut rows = vec![
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Charlie"),
            make_binding_row("name", "Bob"),
        ];
        let conditions = vec![crate::planner::OrderCondition {
            expression: var_expr("name"),
            ascending: false,
        }];
        apply_order_by(&mut rows, &conditions).unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r["name"].value.as_str()).collect();
        assert_eq!(names, vec!["Charlie", "Bob", "Alice"]);
    }

    // --- URS-QEC-S04: ORDER BY on expressions ---

    fn make_numeric_row(
        a_var: &str,
        a_val: &str,
        b_var: &str,
        b_val: &str,
    ) -> HashMap<String, Binding> {
        let mut row = HashMap::new();
        for (var, val) in [(a_var, a_val), (b_var, b_val)] {
            row.insert(
                var.to_string(),
                Binding {
                    binding_type: "literal".into(),
                    value: val.into(),
                    datatype: Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                    lang: None,
                },
            );
        }
        row
    }

    fn var_expr(name: &str) -> spargebra::algebra::Expression {
        spargebra::algebra::Expression::Variable(spargebra::term::Variable::new_unchecked(name))
    }

    #[test]
    fn apply_order_by_arithmetic_expression_desc() {
        // ORDER BY (?a + ?b) DESC must sort by the evaluated sum, descending.
        // Rows: sums are 5 (2+3), 9 (4+5), 7 (1+6).
        let mut rows = vec![
            make_numeric_row("a", "2", "b", "3"), // 5
            make_numeric_row("a", "4", "b", "5"), // 9
            make_numeric_row("a", "1", "b", "6"), // 7
        ];
        let sum =
            spargebra::algebra::Expression::Add(Box::new(var_expr("a")), Box::new(var_expr("b")));
        let conditions = vec![crate::planner::OrderCondition {
            expression: sum,
            ascending: false,
        }];
        apply_order_by(&mut rows, &conditions).unwrap();
        let sums: Vec<i64> = rows
            .iter()
            .map(|r| r["a"].value.parse::<i64>().unwrap() + r["b"].value.parse::<i64>().unwrap())
            .collect();
        assert_eq!(
            sums,
            vec![9, 7, 5],
            "ORDER BY (?a + ?b) DESC must order by sum descending"
        );
    }

    // --- BUG-S13: DISTINCT ---

    #[test]
    fn apply_distinct_removes_duplicates() {
        let mut rows = vec![
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Bob"),
            make_binding_row("name", "Alice"),
        ];
        let projection = vec!["name".into()];
        apply_distinct(&mut rows, &projection);
        assert_eq!(rows.len(), 2);
    }

    // --- BUG-S10: clustering string extraction with logging ---

    #[test]
    fn extract_clustering_string_empty_on_short_input() {
        let result = extract_clustering_string(&[], 0);
        assert!(
            result.is_empty(),
            "empty clustering key should return empty string"
        );
    }

    /// Build a strict CQL clustering component: `[u16 len][bytes]` (no
    /// separator), matching `update::encode_triple_clustering`.
    fn encode_clustering_component(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf
    }

    #[test]
    fn extract_clustering_string_two_components() {
        // Strict separator-free encoding — the reader must NOT assume a
        // trailing 0x00 separator (it would corrupt the second component's
        // length prefix, whose high byte is 0x00 for short objects).
        let mut clustering = encode_clustering_component("http://xmlns.com/foaf/0.1/name");
        clustering.extend_from_slice(&encode_clustering_component("Alice"));
        assert_eq!(
            extract_clustering_string(&clustering, 0),
            "http://xmlns.com/foaf/0.1/name"
        );
        assert_eq!(extract_clustering_string(&clustering, 1), "Alice");
    }

    // --- BUG-S8/S9: scan filters ---

    #[test]
    fn predicate_scan_filters_by_predicate() {
        let op = TripleOp::PredicateScan {
            scope: TripleScope::new("default", "default"),
            predicate: "http://foaf/name".into(),
        };
        assert!(triple_matches_op(
            &op,
            &triple("s1", "http://foaf/name", "Alice", "literal")
        ));
        assert!(!triple_matches_op(
            &op,
            &triple("s1", "http://foaf/knows", "s2", "uri")
        ));
    }

    #[test]
    fn object_scan_filters_by_object() {
        let op = TripleOp::ObjectScan {
            scope: TripleScope::new("default", "default"),
            object: "Bob".into(),
        };
        assert!(!triple_matches_op(
            &op,
            &triple("s1", "http://foaf/name", "Alice", "literal")
        ));
        assert!(triple_matches_op(
            &op,
            &triple("s2", "http://foaf/name", "Bob", "literal")
        ));
    }

    #[test]
    fn subject_lookup_filters_by_predicate_filter() {
        let op = TripleOp::SubjectLookup {
            scope: TripleScope::new("default", "default"),
            subject: "s1".into(),
            predicate_filter: Some("http://foaf/name".into()),
        };
        assert!(triple_matches_op(
            &op,
            &triple("s1", "http://foaf/name", "Alice", "literal")
        ));
        assert!(!triple_matches_op(
            &op,
            &triple("s1", "http://foaf/age", "30", "literal")
        ));
    }

    #[test]
    fn full_scan_matches_every_triple() {
        let op = TripleOp::FullScan {
            scope: TripleScope::new("default", "default"),
        };
        assert!(triple_matches_op(
            &op,
            &triple("s1", "p1", "anything", "literal")
        ));
    }

    /// Tripwire: the SPARQL crate must never reach for a `Vec`-returning range
    /// read.
    ///
    /// `WritePath::range_read` / `range_read_with` collect EVERY partition of a
    /// table before returning. That is the API that let `SELECT * WHERE { ?s ?p
    /// ?o }` — 30 bytes of query — materialize an entire triple store and OOM
    /// the server. Scans go through `range_read_stream_all`, which yields one
    /// partition at a time and can be dropped early. This test fails if a
    /// materializing call is reintroduced.
    #[test]
    fn sparql_scans_never_call_a_materializing_range_read() {
        // Assembled by `concat!` so this test's own source does not match.
        let banned = [
            concat!(".range_", "read("),
            concat!(".range_", "read_with("),
        ];
        for (file, src) in [
            ("executor.rs", include_str!("executor.rs")),
            ("property_path.rs", include_str!("property_path.rs")),
            ("update.rs", include_str!("update.rs")),
            ("engine.rs", include_str!("engine.rs")),
        ] {
            for call in banned {
                assert!(
                    !src.contains(call),
                    "{file} calls the Vec-returning WritePath{call}) — use \
                     range_read_stream_all so the scan stays bounded and streams"
                );
            }
        }
    }

    #[test]
    fn object_index_name_is_correct() {
        assert_eq!(
            OBJECT_INDEX_NAME, "rdf_triples_object_idx",
            "index name must match the DDL"
        );
    }

    // --- try_merge_bindings ---

    #[test]
    fn merge_bindings_compatible() {
        let mut a = HashMap::new();
        a.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/alice".into(),
                datatype: None,
                lang: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "o".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/bob".into(),
                datatype: None,
                lang: None,
            },
        );
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_some(), "disjoint bindings must merge");
        let m = merged.unwrap();
        assert!(m.contains_key("s"));
        assert!(m.contains_key("o"));
    }

    #[test]
    fn merge_bindings_conflict() {
        let mut a = HashMap::new();
        a.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/alice".into(),
                datatype: None,
                lang: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/bob".into(),
                datatype: None,
                lang: None,
            },
        );
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_none(), "conflicting bindings must not merge");
    }

    // --- Helpers ---

    fn make_binding_row(var: &str, val: &str) -> HashMap<String, Binding> {
        let mut row = HashMap::new();
        row.insert(
            var.to_string(),
            Binding {
                binding_type: "literal".into(),
                value: val.into(),
                datatype: None,
                lang: None,
            },
        );
        row
    }

    fn triple(s: &str, p: &str, o: &str, otype: &str) -> FetchedTriple {
        (s.into(), p.into(), o.into(), otype.into(), None, None)
    }
}
