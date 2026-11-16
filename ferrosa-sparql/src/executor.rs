//! SPARQL query executor.
//!
//! Executes a [`QueryPlan`] against ferrosa's write path as a LAZY STREAM of
//! solutions. Nothing in the pipeline holds "every solution": the consumer
//! (the HTTP response body, an ASK, an UPDATE) pulls one solution at a time,
//! and each pull pulls just enough from the storage scan beneath it.
//!
//! # Streaming scans
//!
//! A triple-pattern scan pulls one partition at a time from
//! [`WritePath::range_read_stream_all`] (or a point read / index stream),
//! decodes its rows one by one and hands each surviving triple downstream. The
//! memory a scan costs is one partition.
//!
//! # Joins
//!
//! A pattern applied to the solutions before it is a BLOCK nested-loop join:
//! the outer stream is cut into blocks of [`JOIN_BLOCK_SOLUTIONS`], and for each
//! block the inner side (a scan, a property path, a sub-pattern or a COUNT) is
//! re-evaluated as a stream and matched against the block. The working set is
//! one block plus one inner item, whatever the sizes of the two sides.
//!
//! # Blocking operators spill
//!
//! ORDER BY sorts through ferrosa-storage's [`ExternalSorter`]; DISTINCT
//! de-duplicates through its [`SpillingDedup`](ferrosa_storage::spilling_dedup::SpillingDedup).
//! Both keep a bounded working set and spill the rest under the context's spill
//! root (see [`ExecutionContext`]).
//!
//! # No row bound
//!
//! There is no server-side row cap. A query returns every solution its pattern
//! matches, or an error; it never returns a truncated result that looks
//! complete. `LIMIT` is an optimization, not a bound: because every stage is
//! pulled, `LIMIT n` stops the scan after the `n`-th solution (plus `OFFSET`).
//! Correctness: legacy RDF identity tests and COUNT/JSON/protocol regressions.
//! Last revised: 2026-11-16
//! Last changed: Stream solutions end to end; spill ORDER BY and DISTINCT; drop the row cap.

use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_storage::external_sort::{ExternalSorter, SortedRows, SpillOrder};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

use crate::error::SparqlError;
use crate::planner::{OrderCondition, PlanStep, QueryPlan, TripleOp, TripleScope};
use crate::results::{Binding, SparqlJsonResults};
use crate::triple_store;

use crate::spill::{solution_key, spill_error, SpillSolution};
pub use crate::spill::{ExecutionContext, ExecutionStats, Solution};

/// A lazy stream of solutions. `'static` so it can outlive the request handler
/// and be written into the response body as it is produced.
pub type SolutionStream = BoxStream<'static, Result<Solution, SparqlError>>;

/// A lazy stream of decoded triples from one access path.
pub(crate) type TripleStream = BoxStream<'static, Result<FetchedTriple, SparqlError>>;

/// Outer solutions per block of a block nested-loop join: a working-set size,
/// not a result bound — every block is joined, however many there are.
pub const JOIN_BLOCK_SOLUTIONS: usize = 1024;

/// What every stage of one query shares.
pub(crate) struct Shared {
    pub(crate) plan: Arc<QueryPlan>,
    pub(crate) write_path: Arc<WritePath>,
    pub(crate) cx: ExecutionContext,
}

/// Execute a query plan and collect SPARQL JSON results.
///
/// This MATERIALIZES the result, so it is for tests and embedded callers that
/// want a value. The served path ([`crate::http`]) writes [`select_stream`]
/// into the response incrementally instead.
pub async fn execute(
    plan: &QueryPlan,
    write_path: &Arc<WritePath>,
    cx: &ExecutionContext,
) -> Result<SparqlJsonResults, SparqlError> {
    let plan = Arc::new(plan.clone());
    let projection = plan.projection.clone();
    let rows: Vec<Solution> = select_stream(plan, Arc::clone(write_path), cx.clone())?
        .try_collect()
        .await?;
    let mut results = SparqlJsonResults::new(projection);
    rows.into_iter().for_each(|row| results.add_row(row));
    Ok(results)
}

/// The full SELECT pipeline as a stream: WHERE → ORDER BY → projection →
/// DISTINCT → OFFSET/LIMIT.
///
/// Forms the executor cannot evaluate (an unsupported ORDER BY expression) are
/// refused HERE, before any row is produced, so a caller can still answer with
/// a clean error status.
pub fn select_stream(
    plan: Arc<QueryPlan>,
    write_path: Arc<WritePath>,
    cx: ExecutionContext,
) -> Result<SolutionStream, SparqlError> {
    validate_order_by(&plan.order_by)?;
    let shared = Arc::new(Shared {
        plan: Arc::clone(&plan),
        write_path,
        cx: cx.clone(),
    });
    let mut rows = where_stream(&shared);
    if !plan.order_by.is_empty() {
        rows = order_by_stream(rows, Arc::new(plan.order_by.clone()), cx.clone());
    }
    let projection: Arc<[String]> = plan.projection.clone().into();
    rows = rows.map_ok(move |row| project(row, &projection)).boxed();
    if plan.distinct {
        rows = distinct_stream(rows, plan.projection.clone(), &cx);
    }
    Ok(slice_stream(rows, plan.offset.unwrap_or(0), plan.limit))
}

/// Evaluate a plan's WHERE pattern as a stream of full (unprojected) solutions.
///
/// Used by SPARQL UPDATE pattern ops, which substitute every solution into
/// their templates.
pub fn where_solutions(
    plan: Arc<QueryPlan>,
    write_path: Arc<WritePath>,
    cx: ExecutionContext,
) -> SolutionStream {
    where_stream(&Arc::new(Shared {
        plan,
        write_path,
        cx,
    }))
}

fn where_stream(shared: &Arc<Shared>) -> SolutionStream {
    let seed: SolutionStream = stream::iter([Ok(Solution::new())]).boxed();
    let steps: Arc<[PlanStep]> = shared.plan.steps.clone().into();
    build_steps(&steps, seed, shared)
}

/// Compose `steps` over `input`. Synchronous: it only wires streams together;
/// nothing is read until the result is polled.
fn build_steps(steps: &[PlanStep], input: SolutionStream, shared: &Arc<Shared>) -> SolutionStream {
    let mut rows = input;
    for step in steps {
        rows = match step {
            PlanStep::Filter(index) => {
                let shared = Arc::clone(shared);
                let index = *index;
                rows.try_filter(move |row| {
                    ready(crate::filter::eval_filter(&shared.plan.filters[index], row))
                })
                .boxed()
            }
            PlanStep::Extend {
                variable,
                expression,
            } => {
                let variable: Arc<str> = variable.as_str().into();
                let expression = Arc::new(expression.clone());
                rows.and_then(move |mut row| {
                    let bound = crate::bind::evaluate(&expression, &row).map(|binding| {
                        if let Some(binding) = binding {
                            row.insert(variable.to_string(), binding);
                        }
                        row
                    });
                    ready(bound)
                })
                .boxed()
            }
            PlanStep::Join { left, right } => {
                // Neither operand may see the other's variables until the join,
                // so each is evaluated from an empty seed and joined.
                let rows = block_join(rows, sub_pattern(left.clone().into(), shared));
                block_join(rows, sub_pattern(right.clone().into(), shared))
            }
            PlanStep::Count {
                input,
                variables,
                aggregates,
            } => {
                let inner = count_inner(
                    input.clone().into(),
                    variables.clone().into(),
                    aggregates.clone().into(),
                    shared,
                );
                block_join(rows, inner)
            }
            PlanStep::Triple(index) => match &shared.plan.ops[*index].1 {
                TripleOp::PropertyPath {
                    scope,
                    subject,
                    path,
                    object,
                } => {
                    let inner = path_inner(
                        scope.clone(),
                        subject.clone(),
                        path.clone(),
                        object.clone(),
                        shared,
                    );
                    block_join(rows, inner)
                }
                _ => triple_join(rows, *index, shared),
            },
        };
    }
    rows
}

/// Re-evaluates an inner side for each outer block.
type InnerFactory = Arc<dyn Fn() -> SolutionStream + Send + Sync>;

fn sub_pattern(steps: Arc<[PlanStep]>, shared: &Arc<Shared>) -> InnerFactory {
    let shared = Arc::clone(shared);
    Arc::new(move || {
        let seed: SolutionStream = stream::iter([Ok(Solution::new())]).boxed();
        build_steps(&steps, seed, &shared)
    })
}

fn count_inner(
    input: Arc<[PlanStep]>,
    variables: Arc<[String]>,
    aggregates: Arc<[(String, crate::planner::CountAggregate)]>,
    shared: &Arc<Shared>,
) -> InnerFactory {
    let shared = Arc::clone(shared);
    Arc::new(move || {
        let seed: SolutionStream = stream::iter([Ok(Solution::new())]).boxed();
        let rows = build_steps(&input, seed, &shared);
        crate::aggregate::count_stream(
            rows,
            Arc::clone(&variables),
            Arc::clone(&aggregates),
            shared.cx.clone(),
        )
    })
}

fn path_inner(
    scope: TripleScope,
    subject: spargebra::term::TermPattern,
    path: spargebra::algebra::PropertyPathExpression,
    object: spargebra::term::TermPattern,
    shared: &Arc<Shared>,
) -> InnerFactory {
    let shared = Arc::clone(shared);
    let parts = Arc::new((scope, subject, path, object));
    Arc::new(move || {
        let (scope, subject, path, object) = &*parts;
        let pairs = crate::property_path::path_pairs(
            subject,
            path,
            object,
            scope,
            &shared.write_path,
            &shared.cx,
        );
        let ends = Arc::clone(&parts);
        pairs
            .try_filter_map(move |(s, o)| {
                ready(Ok(crate::property_path::pair_to_binding(
                    &ends.1, &ends.3, s, o,
                )))
            })
            .boxed()
    })
}

/// Block nested-loop join of `outer` with a re-evaluated inner stream.
fn block_join(outer: SolutionStream, inner: InnerFactory) -> SolutionStream {
    outer
        .try_chunks(JOIN_BLOCK_SOLUTIONS)
        .map_err(|e| e.1)
        .map_ok(move |block| {
            let block = Arc::new(block);
            inner()
                .map_ok(move |inner_row| {
                    let joined: Vec<Result<Solution, SparqlError>> = block
                        .iter()
                        .filter_map(|outer_row| try_merge_bindings(outer_row, &inner_row))
                        .map(Ok)
                        .collect();
                    stream::iter(joined)
                })
                .try_flatten()
        })
        .try_flatten()
        .boxed()
}

/// Join `outer` with triple pattern `index`: per outer block, one streaming
/// scan of the pattern's access path, each triple bound against the block.
///
/// The loop nesting is triple-major within a block (`for each streamed triple
/// { for each block solution }`), which is what lets the triple side stream.
/// SPARQL bag semantics do not constrain solution order without `ORDER BY`.
fn triple_join(outer: SolutionStream, index: usize, shared: &Arc<Shared>) -> SolutionStream {
    let shared = Arc::clone(shared);
    outer
        .try_chunks(JOIN_BLOCK_SOLUTIONS)
        .map_err(|e| e.1)
        .map_ok(move |block| {
            let (pattern, op) = &shared.plan.ops[index];
            let subject_index = Arc::new(bound_subject_index(pattern, &block));
            let block = Arc::new(block);
            let bind = Arc::clone(&shared);
            triple_stream(op, &shared.write_path, &shared.cx)
                .map_ok(move |triple| {
                    let pattern = &bind.plan.ops[index].0;
                    stream::iter(bind_against_block(
                        pattern,
                        &triple,
                        &block,
                        Option::as_ref(&*subject_index),
                    ))
                })
                .try_flatten()
        })
        .try_flatten()
        .boxed()
}

/// Every block solution `triple` extends, as ready-to-emit items.
fn bind_against_block(
    pattern: &spargebra::term::TriplePattern,
    triple: &FetchedTriple,
    block: &[Solution],
    subject_index: Option<&SubjectIndex>,
) -> Vec<Result<Solution, SparqlError>> {
    let bind = |existing: &Solution| try_bind_triple(pattern, triple, existing).map(Ok);
    match subject_index {
        // An unbound subject still needs the general compatibility join.
        None => block.iter().filter_map(bind).collect(),
        // Otherwise only visit the block rows whose subject hashes alike;
        // `try_bind_triple` re-checks the exact term.
        Some(index) => index
            .get(&subject_hash(subject_kind(&triple.0), &triple.0))
            .into_iter()
            .flatten()
            .filter_map(|&position| bind(&block[position]))
            .collect(),
    }
}

/// Block positions keyed by the hash of their bound subject term.
type SubjectIndex = HashMap<u64, Vec<usize>>;

fn subject_kind(subject: &str) -> &'static str {
    if subject.starts_with("_:") {
        "bnode"
    } else {
        "uri"
    }
}

fn subject_hash(kind: &str, value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    kind.hash(&mut hasher);
    value.hash(&mut hasher);
    hasher.finish()
}

/// Index the block by its subject variable's value, when every row binds it.
fn bound_subject_index(
    pattern: &spargebra::term::TriplePattern,
    rows: &[Solution],
) -> Option<SubjectIndex> {
    let spargebra::term::TermPattern::Variable(var) = &pattern.subject else {
        return None;
    };
    let mut index = HashMap::new();
    for (position, row) in rows.iter().enumerate() {
        let value = row.get(var.as_str())?;
        let key = subject_hash(value.kind(), value.scalar_value()?);
        index.entry(key).or_insert_with(Vec::new).push(position);
    }
    Some(index)
}

/// Move the projected bindings out of a solution.
fn project(mut row: Solution, projection: &[String]) -> Solution {
    projection
        .iter()
        .filter_map(|var| row.remove_entry(var))
        .collect()
}

/// `OFFSET`/`LIMIT` over a stream. Errors are never skipped or counted: an
/// error inside the OFFSET window still fails the query. Once `limit` solutions
/// have been emitted the upstream is not polled again, which is what stops the
/// scan (LIMIT pushdown by pull).
fn slice_stream(rows: SolutionStream, offset: usize, limit: Option<usize>) -> SolutionStream {
    let mut skipped = 0usize;
    let rows = rows.try_filter(move |_| {
        let keep = skipped >= offset;
        if !keep {
            skipped += 1;
        }
        ready(keep)
    });
    let Some(remaining) = limit else {
        return rows.boxed();
    };
    stream::unfold(
        (rows.boxed(), remaining),
        |(mut rows, remaining)| async move {
            if remaining == 0 {
                return None;
            }
            let item = rows.next().await?;
            let remaining = if item.is_ok() { remaining - 1 } else { 0 };
            Some((item, (rows, remaining)))
        },
    )
    .boxed()
}

/// DISTINCT on the (already projected) solution, first occurrence wins.
fn distinct_stream(
    rows: SolutionStream,
    projection: Vec<String>,
    cx: &ExecutionContext,
) -> SolutionStream {
    let mut seen = cx.dedup("sparql-distinct");
    rows.try_filter_map(move |row| {
        let fresh =
            solution_key(&row, &projection).and_then(|key| seen.insert(&key).map(|f| (f, row)));
        ready(fresh.map(|(fresh, row)| fresh.then_some(row)))
    })
    .boxed()
}

/// Refuse ORDER BY forms this engine cannot evaluate (URS-QEC-X01): sorting
/// those rows as equal would return an arbitrary order that looks deliberate.
fn validate_order_by(order_by: &[OrderCondition]) -> Result<(), SparqlError> {
    for cond in order_by {
        if let Some(what) = crate::filter::unsupported_expr(&cond.expression) {
            return Err(SparqlError::Plan(format!(
                "ORDER BY expression is not supported: {what}; \
                 cannot evaluate it per solution, refusing to return an \
                 arbitrarily-ordered result"
            )));
        }
    }
    Ok(())
}

/// ORDER BY conditions as an external-sort comparator.
#[derive(Clone)]
struct SolutionOrder(Arc<Vec<OrderCondition>>);

impl SpillOrder<SpillSolution> for SolutionOrder {
    fn compare(&self, a: &SpillSolution, b: &SpillSolution) -> std::cmp::Ordering {
        compare_solutions(&self.0, &a.0, &b.0)
    }
}

/// Sort solutions by ORDER BY conditions (BUG-S13, URS-QEC-S04).
///
/// Each condition holds a full expression (a plain `?var` is the trivial
/// `Variable` expression), evaluated per solution; numeric when both sides are
/// numeric.
fn compare_solutions(
    order_by: &[OrderCondition],
    a: &Solution,
    b: &Solution,
) -> std::cmp::Ordering {
    for cond in order_by {
        let ord = crate::filter::order_cmp(&cond.expression, a, b);
        let ord = if cond.ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// ORDER BY through ferrosa-storage's spilling external sorter.
///
/// Blocking by nature — no output row is known until every input row is seen —
/// but bounded: the sorter holds at most the spill threshold in memory and
/// merges sorted runs from disk with one row per run resident.
fn order_by_stream(
    rows: SolutionStream,
    order_by: Arc<Vec<OrderCondition>>,
    cx: ExecutionContext,
) -> SolutionStream {
    stream::once(async move {
        let dir = cx.reserve_dir("sparql-order-by")?;
        let mut sorter = ExternalSorter::new(
            dir.path(),
            SolutionOrder(order_by),
            cx.spill_threshold_bytes(),
        );
        rows.try_for_each(|row| {
            ready(
                sorter
                    .push(SpillSolution(row))
                    .map_err(|e| spill_error("ORDER BY", e)),
            )
        })
        .await?;
        if sorter.spilled() {
            cx.stats().note_sort_spill();
        }
        let sorted = sorter.finish().map_err(|e| spill_error("ORDER BY", e))?;
        Ok::<_, SparqlError>(stream::iter(Sorted {
            rows: sorted,
            _dir: dir,
        }))
    })
    .try_flatten()
    .boxed()
}

/// Sorted output that keeps its run directory alive until it is consumed.
struct Sorted {
    rows: SortedRows<SpillSolution, SolutionOrder>,
    _dir: ferrosa_storage::engine::TempSortTableReservation,
}

impl Iterator for Sorted {
    type Item = Result<Solution, SparqlError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.rows
            .next()
            .map(|row| row.map(|r| r.0).map_err(|e| spill_error("ORDER BY", e)))
    }
}

// ---------------------------------------------------------------------------
// Access paths
// ---------------------------------------------------------------------------

/// Stream every triple `op`'s access path yields, filtered to the op.
///
/// This is the streaming source. A range scan pulls ONE PARTITION AT A TIME
/// from [`WritePath::range_read_stream_all`]; dropping the stream (a satisfied
/// `LIMIT`, an ASK, a cancelled request) stops the scan.
pub(crate) fn triple_stream(
    op: &TripleOp,
    write_path: &Arc<WritePath>,
    cx: &ExecutionContext,
) -> TripleStream {
    let Some(scope) = op_scope(op) else {
        return stream::empty().boxed();
    };
    let graph_filter = scope
        .isolated_graph
        .then(|| Arc::<str>::from(scope.graph.as_str()));
    let op = Arc::new(op.clone());
    let stats = Arc::clone(cx.stats());
    let write_path = Arc::clone(write_path);
    stream::once(open_partitions(Arc::clone(&op), write_path))
        .try_flatten()
        .map_ok(move |partition| {
            stream::iter(PartitionTriples::new(
                partition,
                Arc::clone(&op),
                graph_filter.clone(),
                Arc::clone(&stats),
            ))
        })
        .try_flatten()
        .boxed()
}

type PartitionStream = BoxStream<'static, Result<Partition, SparqlError>>;

/// Open `op`'s partition source.
async fn open_partitions(
    op: Arc<TripleOp>,
    write_path: Arc<WritePath>,
) -> Result<PartitionStream, SparqlError> {
    let Some(scope) = op_scope(&op) else {
        return Ok(stream::empty().boxed());
    };
    // The TABLE is named by the keyspace; the partition key is (GRAPH, subject).
    let table_id = triple_store::triples_table_id(&scope.keyspace);
    match &*op {
        TripleOp::SubjectLookup { subject, .. } => {
            // Point read: one partition by construction.
            let key = triple_store::checked_partition_key(&scope.graph, subject)?;
            let partition = write_path.read(&table_id, &key).await?;
            Ok(stream::iter(partition.map(Ok)).boxed())
        }
        TripleOp::ObjectScan { object, .. } => {
            object_partitions(object, &table_id, &write_path).await
        }
        TripleOp::PredicateScan { .. } | TripleOp::FullScan { .. } => {
            scan_partitions(&table_id, &write_path).await
        }
        TripleOp::PropertyPath { .. } => Ok(stream::empty().boxed()),
    }
}

/// A streaming full scan of the table.
async fn scan_partitions(
    table_id: &ferrosa_storage::TableId,
    write_path: &Arc<WritePath>,
) -> Result<PartitionStream, SparqlError> {
    // `row_limit = 0` means "do not truncate rows within a partition".
    Ok(write_path
        .range_read_stream_all(table_id, 0)
        .await?
        .map_err(SparqlError::from)
        .boxed())
}

/// The object index is an optimisation over a scan that is always correct, so
/// it is read only where it can answer: this node must DECLARE it and it must
/// be CURRENT (a pending backfill holds a fraction of the table, and a
/// fraction reported as the whole answer is worse than a slow one).
async fn object_partitions(
    object: &str,
    table_id: &ferrosa_storage::TableId,
    write_path: &Arc<WritePath>,
) -> Result<PartitionStream, SparqlError> {
    if !write_path.declares_index_locally(table_id, OBJECT_INDEX_NAME) {
        tracing::warn!(
            object,
            index = OBJECT_INDEX_NAME,
            "ObjectScan: object index is not declared; streaming full scan \
             with filtering"
        );
        return scan_partitions(table_id, write_path).await;
    }
    if !write_path.index_is_current(table_id, OBJECT_INDEX_NAME) {
        tracing::warn!(
            object,
            index = OBJECT_INDEX_NAME,
            "ObjectScan: object index is declared but not yet current — its backfill \
             is still pending or has failed; streaming full scan with filtering"
        );
        return scan_partitions(table_id, write_path).await;
    }
    let index_key = ferrosa_index::IndexKey(object.as_bytes().to_vec());
    let mut indexed = write_path
        .index_read_stream(table_id, OBJECT_INDEX_NAME, &index_key, None)
        .await?
        .map_err(SparqlError::from)
        .peekable();
    if std::pin::Pin::new(&mut indexed).peek().await.is_none() {
        tracing::warn!(
            object,
            "ObjectScan: no secondary index hit; falling back to a streaming \
             full scan with filtering"
        );
        return scan_partitions(table_id, write_path).await;
    }
    Ok(indexed.boxed())
}

/// The triples of one partition, decoded one row at a time.
struct PartitionTriples {
    partition: Partition,
    subject: Option<String>,
    next_row: usize,
    op: Arc<TripleOp>,
    stats: Arc<ExecutionStats>,
    failed: Option<SparqlError>,
}

impl PartitionTriples {
    fn new(
        partition: Partition,
        op: Arc<TripleOp>,
        graph_filter: Option<Arc<str>>,
        stats: Arc<ExecutionStats>,
    ) -> Self {
        // BUG-S3 fix: decode composite partition key (graph, subject) properly.
        let (subject, failed) = match triple_store::decode_partition(partition.key.key.as_bytes()) {
            // A partition outside an isolated graph is skipped whole.
            Ok((graph, _)) if graph_filter.as_deref().is_some_and(|g| g != graph) => (None, None),
            Ok((_, subject)) => (Some(subject), None),
            Err(e) => (None, Some(e)),
        };
        Self {
            partition,
            subject,
            next_row: 0,
            op,
            stats,
            failed,
        }
    }
}

impl Iterator for PartitionTriples {
    type Item = Result<FetchedTriple, SparqlError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.failed.take() {
            return Some(Err(e));
        }
        let subject = self.subject.as_deref()?;
        while let Some(row) = self.partition.rows.get(self.next_row) {
            self.next_row += 1;
            self.stats.add_rows_read(1);
            let triple = match decode_triple(row, subject) {
                Ok(Some(triple)) => triple,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };
            if triple_matches_op(&self.op, &triple) {
                return Some(Ok(triple));
            }
        }
        None
    }
}

/// Merge two binding sets if compatible (no conflicting values).
fn try_merge_bindings(
    a: &HashMap<String, Binding>,
    b: &HashMap<String, Binding>,
) -> Option<HashMap<String, Binding>> {
    if b.iter()
        .any(|(key, value)| a.get(key).is_some_and(|old| old != value))
    {
        return None;
    }
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
    let (s, p, o) = triple;
    // Reject bound join-key mismatches using borrowed terms before cloning
    // the solution. Most pairs in a selective join do not match.
    if let spargebra::term::TermPattern::Variable(var) = &tp.subject {
        if existing.get(var.as_str()).is_some_and(|value| {
            let kind = if s.starts_with("_:") { "bnode" } else { "uri" };
            value.kind() != kind || value.scalar_value() != Some(s.as_str())
        }) {
            return None;
        }
    }
    if let spargebra::term::NamedNodePattern::Variable(var) = &tp.predicate {
        if existing
            .get(var.as_str())
            .is_some_and(|value| value.kind() != "uri" || value.scalar_value() != Some(p.as_str()))
        {
            return None;
        }
    }
    if let spargebra::term::TermPattern::Variable(var) = &tp.object {
        if existing.get(var.as_str()).is_some_and(|value| value != o) {
            return None;
        }
    }
    let mut row = existing.clone();

    if !bind_or_check_subject(&tp.subject, s, &mut row)
        || !bind_or_check_predicate(&tp.predicate, p, &mut row)
        || !bind_or_check_object(&tp.object, o, &mut row)
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
            let binding = if subject.starts_with("_:") {
                Binding::blank_node(subject)
            } else {
                Binding::uri(subject)
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
            let binding = Binding::uri(predicate);
            try_insert_binding(row, var.as_str(), &binding)
        }
        NamedNodePattern::NamedNode(n) => n.as_str() == predicate,
    }
}

/// Bind the object variable, or check an object constant.
fn bind_or_check_object(
    pattern: &spargebra::term::TermPattern,
    object: &Binding,
    row: &mut HashMap<String, Binding>,
) -> bool {
    use spargebra::term::TermPattern;
    match pattern {
        TermPattern::Variable(variable) => try_insert_binding(row, variable.as_str(), object),
        TermPattern::NamedNode(node) => {
            object.kind() == "uri" && object.scalar_value() == Some(node.as_str())
        }
        TermPattern::Literal(literal) => triple_store::canonical_term(
            "literal",
            literal.value(),
            Some(literal.datatype().as_str()),
            literal.language(),
        )
        .is_ok_and(|expected| expected == *object),
        TermPattern::BlankNode(_) => true,
        TermPattern::Triple(_) => false,
    }
}

/// Insert a binding into a row, or check compatibility if already present.
///
/// Compare the full RDF term: lexical value, term kind, datatype and language.
/// Returns `false` if the existing binding is incompatible.
fn try_insert_binding(row: &mut HashMap<String, Binding>, name: &str, binding: &Binding) -> bool {
    if let Some(existing) = row.get(name) {
        existing == binding
    } else {
        row.insert(name.to_string(), binding.clone());
        true
    }
}

/// A fetched triple: (subject, predicate, object, object_type, datatype, language).
type FetchedTriple = (String, String, Binding);

/// Name of the secondary index on the object column.
///
/// `pub(crate)` because registration declares it: the `ObjectScan` arm below
/// reads through this index and falls back to a scan when the node does not
/// declare it, so the declaration site and the read site must name one thing.
pub(crate) const OBJECT_INDEX_NAME: &str = "rdf_triples_object_idx";

/// The graph a storage-backed op reads from. `None` for `PropertyPath`, which
/// is evaluated by [`crate::property_path`] and never reaches the scan.
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
/// The v2 decoder validates identity and accounts for liveness newer than a
/// retained tombstone. Deleted rows are omitted; malformed live rows fail loud.
fn decode_triple(row: &Row, subject: &str) -> Result<Option<FetchedTriple>, SparqlError> {
    Ok(triple_store::decode_v2_row(row)?
        .map(|(predicate, term)| (subject.to_owned(), predicate, term)))
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
    let (_, p, o) = triple;
    match op {
        TripleOp::SubjectLookup {
            predicate_filter: Some(pred),
            ..
        } => p == pred,
        TripleOp::PredicateScan { predicate, .. } => p == predicate,
        TripleOp::ObjectScan { object, .. } => o.scalar_value() == Some(object.as_str()),
        _ => true,
    }
}

/// Extract a string component from a CQL clustering key at the given position.
///
/// Clustering keys use the strict CQL composite encoding written by
/// `triple_store::encode_clustering`: `[u16 len][bytes]` per component with NO
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
        let b = Binding::uri("http://example.org/alice");
        assert!(try_insert_binding(&mut row, "s", &b));
        assert!(
            try_insert_binding(&mut row, "s", &b),
            "same binding is compatible"
        );
    }

    #[test]
    fn try_insert_binding_incompatible_value() {
        let mut row = HashMap::new();
        let b1 = Binding::uri("http://example.org/alice");
        let b2 = Binding::uri("http://example.org/bob");
        assert!(try_insert_binding(&mut row, "s", &b1));
        assert!(!try_insert_binding(&mut row, "s", &b2));
    }

    #[test]
    fn try_insert_binding_incompatible_type() {
        let mut row = HashMap::new();
        let uri_binding = Binding::uri("http://example.org/alice");
        let literal_binding = Binding::literal("http://example.org/alice", None, None);
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
        let names: Vec<&str> = rows
            .iter()
            .map(|r| r["name"].scalar_value().unwrap())
            .collect();
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
        let names: Vec<&str> = rows
            .iter()
            .map(|r| r["name"].scalar_value().unwrap())
            .collect();
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
                Binding::literal(
                    val,
                    Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                    None,
                ),
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
            .map(|r| {
                r["a"].scalar_value().unwrap().parse::<i64>().unwrap()
                    + r["b"].scalar_value().unwrap().parse::<i64>().unwrap()
            })
            .collect();
        assert_eq!(
            sums,
            vec![9, 7, 5],
            "ORDER BY (?a + ?b) DESC must order by sum descending"
        );
    }

    // --- BUG-S13: DISTINCT ---

    #[test]
    fn distinct_stream_removes_duplicates() {
        let rows = vec![
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Bob"),
            make_binding_row("name", "Alice"),
        ];
        let dir = tempfile::tempdir().unwrap();
        let cx = ExecutionContext::new(dir.path(), 1 << 20);
        let input: SolutionStream = stream::iter(rows.into_iter().map(Ok)).boxed();
        let out: Vec<Solution> = futures::executor::block_on(
            distinct_stream(input, vec!["name".into()], &cx).try_collect(),
        )
        .unwrap();
        let names: Vec<&str> = out
            .iter()
            .map(|r| r["name"].scalar_value().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["Alice", "Bob"],
            "first occurrence wins, order kept"
        );
    }

    #[test]
    fn distinct_stream_past_its_resident_keys_spills_and_stays_exact() {
        let dir = tempfile::tempdir().unwrap();
        let cx = ExecutionContext::new(dir.path(), 1 << 20).with_dedup_resident_keys(4);
        let rows = (0..200).map(|i| Ok(make_binding_row("n", &(i % 50).to_string())));
        let input: SolutionStream = stream::iter(rows.collect::<Vec<_>>()).boxed();
        let out: Vec<Solution> = futures::executor::block_on(
            distinct_stream(input, vec!["n".into()], &cx).try_collect(),
        )
        .unwrap();
        assert_eq!(out.len(), 50);
        assert_eq!(cx.stats().dedup_spills(), 1, "the set must have spilled");
    }

    #[test]
    fn slice_stream_never_skips_an_error_inside_the_offset() {
        let input: SolutionStream = stream::iter(vec![
            Err(SparqlError::Execution("boom".into())),
            Ok(make_binding_row("n", "1")),
        ])
        .boxed();
        let out: Result<Vec<Solution>, _> =
            futures::executor::block_on(slice_stream(input, 1, Some(1)).try_collect());
        assert!(out.is_err(), "an error is never consumed by OFFSET");
    }

    fn apply_order_by(
        rows: &mut [Solution],
        conditions: &[crate::planner::OrderCondition],
    ) -> Result<(), SparqlError> {
        validate_order_by(conditions)?;
        rows.sort_by(|a, b| compare_solutions(conditions, a, b));
        Ok(())
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
    /// separator), matching `triple_store::encode_clustering`.
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
        a.insert("s".into(), Binding::uri("http://ex/alice"));
        let mut b = HashMap::new();
        b.insert("o".into(), Binding::uri("http://ex/bob"));
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_some(), "disjoint bindings must merge");
        let m = merged.unwrap();
        assert!(m.contains_key("s"));
        assert!(m.contains_key("o"));
    }

    #[test]
    fn merge_bindings_conflict() {
        let mut a = HashMap::new();
        a.insert("s".into(), Binding::uri("http://ex/alice"));
        let mut b = HashMap::new();
        b.insert("s".into(), Binding::uri("http://ex/bob"));
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_none(), "conflicting bindings must not merge");
    }

    // --- Helpers ---

    fn make_binding_row(var: &str, val: &str) -> HashMap<String, Binding> {
        let mut row = HashMap::new();
        row.insert(var.to_string(), Binding::literal(val, None, None));
        row
    }

    fn triple(s: &str, p: &str, o: &str, otype: &str) -> FetchedTriple {
        (
            s.into(),
            p.into(),
            match otype {
                "uri" => Binding::uri(o),
                "bnode" => Binding::blank_node(o),
                _ => Binding::literal(o, None, None),
            },
        )
    }
}
