//! Expand executor: traverses graph patterns via the adjacency index.
//!
//! Executes a `PhysicalPlan::Expand` by:
//! 1. Looking up the anchor vertex via `storage.read_range()` (or
//!    `VirtualTable::read()` if the source is a virtual table)
//! 2. For each hop, reading the adjacency index to find neighbors
//! 3. Building a `GraphResult` with columns from the return clause

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_schema::{Schema, VirtualTableRegistry};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::{Mutation, TableId};

use crate::adjacency::observer::derive_adjacency_mutations;
use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};
use crate::error::{GraphError, Result};
use crate::executor::aggregate::{create_accumulator, Accumulator};
use crate::executor::eval;
use crate::executor::result::{GraphResult, QueryStats};
use crate::executor::stream::{
    chain_streams, collect_to_graph_result, dedup_stream, stream_from_rows, RowStream,
};
use crate::parser::{
    CompareOp, Direction, Expr, Literal, PatternComprehensionHop, RemoveItem, ReturnClause,
    ReturnItem, SortDir, WithPipeline,
};
use crate::planner::physical::{AggregateProjection, Anchor, CreateOp, Hop, MergeOp, PhysicalPlan};

/// Maximum neighbor hydrations in flight at once within a single hop
/// (t_4ce82a3e). Bounds the concurrent storage reads a high-degree hub can
/// launch: fan-out is unbounded, in-flight work is not. Small enough that the
/// per-hop future set stays cheap, large enough to overlap read latency.
const HOP_HYDRATION_CONCURRENCY: usize = 16;

#[derive(Debug, Clone)]
struct ExpandState {
    current_key: DecoratedKey,
    bindings: HashMap<String, serde_json::Value>,
}

struct MergeWriteShape {
    key: DecoratedKey,
    clustering: Vec<u8>,
    create_cells: Vec<(u16, CellValue)>,
    used_schema_layout: bool,
}

struct MatchedTableRow {
    key: DecoratedKey,
    clustering: Vec<u8>,
    json: serde_json::Value,
}

fn graph_replication_strategy(
    schema: Option<&Schema>,
    keyspace: &str,
) -> Result<ReplicationStrategy> {
    let Some(schema) = schema else {
        return Ok(ReplicationStrategy::Simple {
            replication_factor: 1,
        });
    };

    let snap = schema.snapshot();
    let Some(keyspace_meta) = snap.keyspaces.get(keyspace) else {
        return Ok(ReplicationStrategy::Simple {
            replication_factor: 1,
        });
    };

    ReplicationStrategy::try_from(&keyspace_meta.replication).map_err(|err| {
        GraphError::Validation(format!(
            "invalid replication strategy for keyspace '{keyspace}': {err}"
        ))
    })
}

/// Configuration for the graph query engine (T4 DoS limits).
#[derive(Debug, Clone)]
pub struct GraphEngineConfig {
    /// Maximum query execution time.
    pub query_timeout: Duration,
    /// Maximum number of result rows.
    ///
    /// DEPRECATED — being removed (t_4ce82a3e). This used to SILENTLY TRUNCATE
    /// the result set (`break`, no error, no flag), so a client could not tell a
    /// partial answer from a complete one. ORDER BY now spills instead of
    /// capping (see `spill`); the remaining uses are on the not-yet-converted
    /// varpath/leapfrog paths.
    pub max_result_rows: usize,
    /// Backend for spilling an unbounded ORDER BY to disk. `None` = sort in
    /// memory (unit tests, embedded use). See
    /// [`crate::executor::spill::SpillReserver`].
    pub spill: Option<std::sync::Arc<dyn crate::executor::spill::SpillReserver>>,
    /// Bytes of rows to accumulate before spilling a sorted run.
    pub spill_threshold_bytes: u64,
    /// Maximum fan-out per hop (T4: limits adjacency reads).
    pub max_fan_out_per_hop: usize,
    /// Maximum number of groups in an aggregation (FMEA F7).
    pub max_groups: usize,
    /// Maximum number of values in a `collect()` accumulator (FMEA F6).
    pub max_collect_size: usize,
    /// Maximum number of vertices visited during variable-length path BFS
    /// (threat T13, FMEA F3: DoS protection for `[*]` patterns).
    pub max_var_path_visited: usize,
}

impl Default for GraphEngineConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_result_rows: 10_000,
            spill: None,
            // 64 MiB of accumulated rows before a run is spilled — large enough
            // that ordinary queries never touch disk, small enough that a huge
            // sort stays bounded.
            spill_threshold_bytes: 64 * 1024 * 1024,
            max_fan_out_per_hop: 10_000,
            max_groups: 100_000,
            max_collect_size: 10_000,
            max_var_path_visited: 100_000,
        }
    }
}

/// The columns, row stream, and stats produced by [`execute_streaming`].
type StreamingPlanResult<'a> = Result<(Vec<String>, RowStream<'a>, QueryStats)>;

/// Execute a physical plan against the storage engine.
///
/// If `virtual_tables` is provided, the executor checks the registry before
/// going to storage for anchor lookups. Virtual tables (e.g. in
/// `system_observability`) return rows directly from memory.
///
/// If `schema` is provided, column names from table metadata are used to
/// map cell indices to property names (e.g., `name`, `age`) so that Cypher
/// property lookups like `a.name` resolve correctly.
///
/// This is the buffered façade over [`execute_streaming`]: it drains the row
/// stream through [`collect_to_graph_result`]. There is one executor, not two —
/// this entry point adds only the materialization.
pub async fn execute(
    plan: PhysicalPlan,
    write_path: &WritePath,
    keyspace: &str,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    let (columns, rows_stream, stats) =
        execute_streaming(plan, write_path, keyspace, config, virtual_tables, schema).await?;
    // `usize::MAX`, deliberately NOT `config.max_result_rows`: `execute` has
    // never applied a global row cap. The caps live inside individual operators
    // (leapfrog, the virtual-table anchor, projection), and a UNION of capped
    // arms may legitimately return more than `max_result_rows` rows today.
    // Capping here would silently truncate results that currently come back in
    // full — a behavior change, and a silent one.
    collect_to_graph_result(columns, rows_stream, stats, usize::MAX).await
}

/// Streaming entry point: the projected column names, a pull-based row stream,
/// and the query's [`QueryStats`].
///
/// Additive scaffold (increment 2 of the streaming executor, t_4ce82a3e). Only
/// the provably-safe plan variants stream; everything else computes today's
/// buffered [`GraphResult`] and hands it back through [`stream_from_rows`].
/// Behavior is identical for every query either way — [`execute`] is this
/// function plus a `collect`.
///
/// # What actually streams
///
/// - [`PhysicalPlan::Subscribe`] — pure passthrough to the inner plan, so it
///   streams exactly as well as whatever it wraps.
/// - [`PhysicalPlan::Union`] with `all: true` — concatenation, via
///   [`chain_streams`]. A later arm is never polled until the earlier arms are
///   drained. The arms themselves are
///   still *executed* eagerly, because `stats` is returned up front and a
///   UNION's stats are the sum over its arms — so the end-to-end laziness is
///   real only when the arms are themselves converted variants.
/// - [`PhysicalPlan::ReturnOnly`] — at most one row, produced on pull, so
///   `LIMIT 0` never evaluates the projection.
///
/// # What deliberately does not
///
/// Everything else, including `UNION` *without* `ALL` (its `HashSet` dedup
/// spans the whole concatenation), `DeleteNodes` (it walks the matched rows
/// twice — validate every vertex, then tombstone — and streaming it would
/// partially delete), `Aggregate`, DISTINCT, ORDER BY, and
/// `ExpandVarLength`.
///
/// `WcoJoin` moved into the streaming set: its accumulator already spilled to
/// disk, and only the final `finish_rows()` was pulling the result back into
/// memory.
///
/// # Stats
///
/// `QueryStats` is returned alongside the stream rather than after it, so it is
/// computed exactly where the buffered path computes it today. That is why a
/// fallback variant is fully executed before this function returns.
pub async fn execute_streaming<'a>(
    plan: PhysicalPlan,
    write_path: &'a WritePath,
    keyspace: &'a str,
    config: &'a GraphEngineConfig,
    virtual_tables: Option<&'a VirtualTableRegistry>,
    schema: Option<&'a Schema>,
) -> StreamingPlanResult<'a> {
    execute_streaming_inner(plan, write_path, keyspace, config, virtual_tables, schema).await
}

/// Owned handles for one query execution — the same inputs
/// [`execute_streaming`] borrows, held by value.
///
/// # Why this exists
///
/// [`execute_streaming`] returns a `RowStream<'a>` that borrows its inputs. That
/// is exactly what we want inside the executor (no per-row clones), and exactly
/// what a *transport* cannot use: an HTTP response body outlives the handler
/// frame, so `axum::body::Body::from_stream` demands `Stream + Send + 'static`.
///
/// Two concrete borrows blocked it. `arc_swap::ArcSwap::load()` yields a `Guard`
/// that is a local of the calling frame, and `Schema::snapshot()` /
/// `Schema::virtual_tables()` borrow the `Arc<Schema>` the caller is holding.
/// Both have owned counterparts (`load_full()`, `virtual_tables_arc()`, an
/// `Arc<Schema>` clone), which is what this struct collects.
///
/// This is a wrapper, not a second executor: [`execute_streaming_owned`] calls
/// straight through to [`execute_streaming`]. There is one dispatch.
pub struct OwnedExecCtx {
    /// From `ArcSwap::load_full()`, not `load()` — an owned `Arc`, not a `Guard`.
    pub write_path: std::sync::Arc<WritePath>,
    /// Owned copy of the caller's `&str`.
    pub keyspace: String,
    /// Owned config; `GraphEngineConfig` is small and `Clone`.
    pub config: std::sync::Arc<GraphEngineConfig>,
    /// From `Schema::virtual_tables_arc()`, not `virtual_tables()`.
    pub virtual_tables: Option<std::sync::Arc<VirtualTableRegistry>>,
    /// An `Arc<Schema>` clone, not a `&Schema`.
    pub schema: Option<std::sync::Arc<Schema>>,
}

/// One item of the internal owned execution generator.
///
/// The generator has to deliver the columns and stats *through* the stream
/// (they are produced inside it, where the borrows of the owned context live),
/// so the first item is the head and every later item is a row.
/// [`execute_streaming_owned`] peels the head back off, which is why this type
/// never escapes the module.
enum OwnedStreamItem {
    Head(Vec<String>, QueryStats),
    Row(crate::executor::stream::RowVals),
}

/// [`execute_streaming`] with owned handles: the row stream is `'static`, so it
/// can be handed to a response body that outlives this frame.
///
/// # Error timing (this is a transport-visible contract)
///
/// A failure raised while *setting up* the execution — validation, a fallback
/// plan's whole buffered execution, the first storage read — comes back as
/// `Err` from this function, before the caller has written a byte. That is what
/// lets the HTTP layer still choose a status code.
///
/// A failure raised *after* the first row has been yielded can only appear as an
/// `Err` item on the stream. A transport must abort the body there; the status
/// line is already on the wire.
///
/// # Laziness
///
/// Unchanged from [`execute_streaming`]: whatever streams there streams here,
/// and whatever falls back to a buffered `GraphResult` there falls back here.
/// Wrapping does not convert an operator.
pub async fn execute_streaming_owned(
    plan: PhysicalPlan,
    ctx: OwnedExecCtx,
) -> Result<(Vec<String>, RowStream<'static>, QueryStats)> {
    use futures::StreamExt as _;

    let mut items = Box::pin(owned_stream_items(plan, ctx));

    // Drive the generator up to (and including) the head. This is what runs
    // setup, so a setup error is returned as `Err` rather than becoming the
    // stream's first item.
    let (columns, stats) = match items.next().await {
        Some(Ok(OwnedStreamItem::Head(columns, stats))) => (columns, stats),
        Some(Ok(OwnedStreamItem::Row(_))) => {
            return Err(GraphError::Internal(
                "owned execution stream yielded a row before its head".to_string(),
            ))
        }
        Some(Err(e)) => return Err(e),
        None => {
            return Err(GraphError::Internal(
                "owned execution stream ended before its head".to_string(),
            ))
        }
    };

    let rows: RowStream<'static> = Box::pin(items.map(|item| match item {
        Ok(OwnedStreamItem::Row(row)) => Ok(row),
        // Fail loud rather than drop: a second head would mean the generator
        // below was restructured and this peel is now wrong.
        Ok(OwnedStreamItem::Head(..)) => Err(GraphError::Internal(
            "owned execution stream yielded a second head".to_string(),
        )),
        Err(e) => Err(e),
    }));

    Ok((columns, rows, stats))
}

/// The owned execution generator.
///
/// `ctx` is *moved into* the async generator, so the `&ctx.write_path` /
/// `&ctx.keyspace` / `&ctx.config` borrows [`execute_streaming`] needs are
/// borrows of the generator's own locals — legal to hold across yield points,
/// and invisible from outside. That is what turns a `RowStream<'a>` into a
/// `'static` one without cloning per row, spawning a task, or duplicating the
/// executor.
fn owned_stream_items(
    plan: PhysicalPlan,
    ctx: OwnedExecCtx,
) -> impl futures::Stream<Item = Result<OwnedStreamItem>> + Send + 'static {
    use futures::StreamExt as _;

    async_stream::stream! {
        let started = execute_streaming(
            plan,
            &ctx.write_path,
            &ctx.keyspace,
            &ctx.config,
            ctx.virtual_tables.as_deref(),
            ctx.schema.as_deref(),
        )
        .await;

        let (columns, mut rows, stats) = match started {
            Ok(started) => started,
            Err(e) => {
                yield Err(e);
                return;
            }
        };

        yield Ok(OwnedStreamItem::Head(columns, stats));

        while let Some(row) = rows.next().await {
            match row {
                Ok(row) => yield Ok(OwnedStreamItem::Row(row)),
                Err(e) => {
                    // Surface and stop: continuing past an error would emit a
                    // short result that looks complete.
                    yield Err(e);
                    return;
                }
            }
        }
    }
}

/// Boxed body of [`execute_streaming`].
///
/// `Subscribe` and `Union` recurse into it, so the future has to be type-erased.
fn execute_streaming_inner<'a>(
    plan: PhysicalPlan,
    write_path: &'a WritePath,
    keyspace: &'a str,
    config: &'a GraphEngineConfig,
    virtual_tables: Option<&'a VirtualTableRegistry>,
    schema: Option<&'a Schema>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = StreamingPlanResult<'a>> + Send + 'a>> {
    Box::pin(async move {
        let start = Instant::now();

        match plan {
            // --- converted: streams for real ---------------------------------
            PhysicalPlan::Subscribe { inner, .. } => {
                // The initial snapshot IS the inner plan's result. Passthrough,
                // including the inner plan's own `start` for `execution_ms`,
                // exactly as the buffered path's recursive `execute` call did.
                execute_streaming_inner(
                    *inner,
                    write_path,
                    keyspace,
                    config,
                    virtual_tables,
                    schema,
                )
                .await
            }
            PhysicalPlan::Union { arms, all: true } => {
                let mut columns: Option<Vec<String>> = None;
                let mut stats = QueryStats::default();
                let mut arm_streams: Vec<RowStream<'a>> = Vec::with_capacity(arms.len());
                for arm in arms {
                    let (arm_columns, arm_stream, arm_stats) = execute_streaming_inner(
                        arm,
                        write_path,
                        keyspace,
                        config,
                        virtual_tables,
                        schema,
                    )
                    .await?;
                    if let Some(existing) = &columns {
                        if existing != &arm_columns {
                            return Err(GraphError::Validation(
                                "UNION arms must return identical columns".to_string(),
                            ));
                        }
                    } else {
                        columns = Some(arm_columns);
                    }
                    stats.vertices_read += arm_stats.vertices_read;
                    stats.edges_read += arm_stats.edges_read;
                    stats.vertices_written += arm_stats.vertices_written;
                    stats.vertices_deleted += arm_stats.vertices_deleted;
                    // `edges_deleted` is NOT summed here. That omission is
                    // carried over verbatim from the buffered UNION: fixing it
                    // would change what a `UNION` over DELETE arms reports, so
                    // it is out of scope for a behavior-preserving scaffold.
                    arm_streams.push(arm_stream);
                }
                stats.execution_ms = start.elapsed().as_millis() as u64;
                Ok((
                    columns.unwrap_or_default(),
                    chain_streams(arm_streams),
                    stats,
                ))
            }
            PhysicalPlan::ReturnOnly { return_clause } => {
                Ok(return_only_stream(return_clause, start))
            }
            PhysicalPlan::Expand {
                anchor,
                hops,
                optional_hops,
                post_filters,
                with_pipeline,
                return_clause,
            } => {
                execute_expand_streaming(
                    ExpandParts {
                        anchor,
                        hops,
                        optional_hops,
                        post_filters,
                        with_pipeline,
                        return_clause,
                    },
                    ExpandCtx {
                        write_path,
                        keyspace,
                        config,
                        virtual_tables,
                        schema,
                        start,
                    },
                )
                .await
            }

            // Worst-case-optimal join. Its accumulator already spilled while
            // accumulating; routing it here is what lets the spill survive to
            // the transport instead of being collected back into a Vec by the
            // fallback below.
            PhysicalPlan::WcoJoin {
                plan,
                return_clause,
            } => {
                super::leapfrog::execute_wco_join_streaming(
                    write_path,
                    keyspace,
                    &plan,
                    &return_clause,
                    config,
                    start,
                    virtual_tables,
                    schema,
                )
                .await
            }

            // --- everything else: today's buffered result, wrapped -----------
            other => {
                let result = execute_buffered(
                    other,
                    write_path,
                    keyspace,
                    config,
                    virtual_tables,
                    schema,
                    start,
                )
                .await?;
                Ok((result.columns, stream_from_rows(result.rows), result.stats))
            }
        }
    })
}

/// Remove duplicate rows, keeping the first occurrence and the existing order.
///
/// The previous form sorted by debug representation and then `dedup()`ed:
///
/// ```ignore
/// rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
/// rows.dedup();
/// ```
///
/// `Vec::dedup` only removes *consecutive* duplicates, so the sort was there to
/// make duplicates adjacent — but it ran **after** the ORDER BY sort and
/// therefore destroyed it. `RETURN DISTINCT x ORDER BY x DESC` came back in
/// debug-string ascending order, with no indication the requested order had
/// been discarded.
///
/// Deduplicating on a hash of the row keeps every duplicate out without
/// touching the order, so ORDER BY survives. First-seen order also matches what
/// the streaming paths already do (`dedup_stream`, t_4ce82a3e), so the two
/// forms of DISTINCT now agree instead of differing by which path a query took.
pub(super) fn dedup_preserving_order(rows: &mut Vec<super::stream::RowVals>) {
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| seen.insert(serde_json::to_string(row).unwrap_or_default()));
}

/// `RETURN <exprs>` with no MATCH: at most one row, built on pull.
///
/// The buffered form projected the row, ran `sort_rows`, then
/// `rows.truncate(limit)`. Streaming, the truncate is a `take`, so `LIMIT 0`
/// short-circuits before the projection is evaluated at all. `ORDER BY` is
/// dropped on purpose: sorting a one-row result is the identity.
fn return_only_stream<'a>(
    return_clause: ReturnClause,
    start: Instant,
) -> (Vec<String>, RowStream<'a>, QueryStats) {
    use futures::StreamExt as _;

    let columns = build_columns(&return_clause);
    let limit = return_clause
        .limit
        .map(|limit| limit.max(0) as usize)
        .unwrap_or(usize::MAX);
    let row = futures::stream::once(async move {
        let bindings = HashMap::new();
        Ok(return_clause
            .items
            .iter()
            .map(|item| eval::eval_expr(&item.expr, &bindings).unwrap_or(serde_json::Value::Null))
            .collect::<Vec<_>>())
    });
    let stats = QueryStats {
        vertices_read: 0,
        edges_read: 0,
        vertices_written: 0,
        vertices_deleted: 0,
        edges_deleted: 0,
        execution_ms: start.elapsed().as_millis() as u64,
    };
    (columns, Box::pin(row.take(limit)), stats)
}

/// The materializing executor: every plan variant that has not been converted
/// to streaming, unchanged.
///
/// `start` is threaded in from [`execute_streaming_inner`] so `execution_ms`
/// still measures from the same instant the buffered `execute` used to.
async fn execute_buffered<'a>(
    plan: PhysicalPlan,
    write_path: &'a WritePath,
    keyspace: &'a str,
    config: &'a GraphEngineConfig,
    virtual_tables: Option<&'a VirtualTableRegistry>,
    schema: Option<&'a Schema>,
    start: Instant,
) -> Result<GraphResult> {
    match plan {
        PhysicalPlan::Union { arms, all } => {
            // Concatenation is the streaming half; `all: false` adds a HashSet
            // dedup over the WHOLE concatenation, which has to see every row
            // before any row is final. Route the concatenation through the
            // streaming UNION ALL so there is one implementation of it, then
            // materialize and dedup.
            let (columns, rows_stream, mut stats) = execute_streaming(
                PhysicalPlan::Union { arms, all: true },
                write_path,
                keyspace,
                config,
                virtual_tables,
                schema,
            )
            .await?;
            let mut rows = crate::executor::stream::collect_rows(rows_stream).await?;
            if !all {
                let mut seen = std::collections::HashSet::new();
                rows.retain(|row| seen.insert(serde_json::to_string(row).unwrap_or_default()));
            }
            stats.execution_ms = start.elapsed().as_millis() as u64;
            Ok(GraphResult {
                columns,
                rows,
                stats,
            })
        }
        // Converted variants: `execute_streaming` intercepts these before it
        // ever delegates here. Present so this function stays total over
        // `PhysicalPlan`, and routed through the same code so there is no
        // second implementation to drift.
        PhysicalPlan::ReturnOnly { return_clause } => {
            let (columns, rows_stream, stats) = return_only_stream(return_clause, start);
            collect_to_graph_result(columns, rows_stream, stats, usize::MAX).await
        }
        PhysicalPlan::Subscribe { inner, .. } => {
            Box::pin(execute(
                *inner,
                write_path,
                keyspace,
                config,
                virtual_tables,
                schema,
            ))
            .await
        }
        PhysicalPlan::Unwind {
            expr,
            var,
            with_pipeline,
            return_clause,
        } => execute_unwind(&expr, &var, with_pipeline.as_ref(), &return_clause, start).await,
        PhysicalPlan::Expand {
            anchor,
            hops,
            optional_hops,
            post_filters,
            with_pipeline,
            return_clause,
        } => {
            execute_expand(
                ExpandPlan {
                    anchor: &anchor,
                    hops: &hops,
                    optional_hops: &optional_hops,
                    post_filters: &post_filters,
                    with_pipeline: with_pipeline.as_ref(),
                    return_clause: &return_clause,
                },
                ExpandCtx {
                    write_path,
                    keyspace,
                    config,
                    virtual_tables,
                    schema,
                    start,
                },
            )
            .await
        }
        PhysicalPlan::CreateNodes {
            creates,
            return_clause,
        } => {
            execute_create(
                write_path,
                &creates,
                return_clause.as_ref(),
                config,
                start,
                schema,
            )
            .await
        }
        PhysicalPlan::SetProperties {
            expand,
            assignments,
            variable_tables,
        } => {
            execute_set(
                write_path,
                *expand,
                keyspace,
                &assignments,
                &variable_tables,
                config,
                virtual_tables,
                start,
                schema,
            )
            .await
        }
        PhysicalPlan::RemoveProperties {
            expand,
            items,
            variable_tables,
        } => {
            execute_remove(
                write_path,
                *expand,
                keyspace,
                &items,
                &variable_tables,
                config,
                virtual_tables,
                start,
                schema,
            )
            .await
        }
        PhysicalPlan::DeleteNodes {
            expand,
            variables,
            detach,
            variable_tables,
        } => {
            // NOT streamable: this consumes the matched rows TWICE — validate
            // across every matched vertex first, then tombstone. A single-pass
            // stream would delete a prefix and then fail validation, i.e.
            // partial deletion.
            execute_delete(
                write_path,
                *expand,
                keyspace,
                &variables,
                detach,
                config,
                virtual_tables,
                start,
                schema,
                &variable_tables,
            )
            .await
        }
        PhysicalPlan::Aggregate {
            inner,
            group_keys,
            projections,
            return_clause,
        } => {
            execute_aggregate(
                write_path,
                keyspace,
                *inner,
                &group_keys,
                &projections,
                &return_clause,
                config,
                start,
                virtual_tables,
                schema,
            )
            .await
        }
        PhysicalPlan::ExpandVarLength {
            anchor,
            hop,
            min_hops,
            max_hops,
            return_clause,
        } => {
            super::varpath::execute_var_length(
                write_path,
                keyspace,
                &anchor,
                &hop,
                min_hops,
                max_hops,
                &return_clause,
                config,
                start,
                virtual_tables,
                schema,
            )
            .await
        }
        PhysicalPlan::WcoJoin {
            plan,
            return_clause,
        } => {
            super::leapfrog::execute_wco_join(
                write_path,
                keyspace,
                &plan,
                &return_clause,
                config,
                start,
                virtual_tables,
                schema,
            )
            .await
        }
        PhysicalPlan::MergeUpsert {
            merges,
            set_clause,
            return_clause,
        } => {
            execute_merge(
                write_path,
                &merges,
                &set_clause,
                return_clause.as_ref(),
                config,
                start,
                schema,
            )
            .await
        }
    }
}

fn graph_filter_passes<'a>(
    expr: &'a Expr,
    write_path: &'a WritePath,
    keyspace: &'a str,
    schema: Option<&'a Schema>,
    bindings: &'a HashMap<String, serde_json::Value>,
    current_var: &'a str,
    current_key: &'a DecoratedKey,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expr::And(left, right) => Ok(graph_filter_passes(
                left,
                write_path,
                keyspace,
                schema,
                bindings,
                current_var,
                current_key,
            )
            .await?
                && graph_filter_passes(
                    right,
                    write_path,
                    keyspace,
                    schema,
                    bindings,
                    current_var,
                    current_key,
                )
                .await?),
            Expr::Or(left, right) => Ok(graph_filter_passes(
                left,
                write_path,
                keyspace,
                schema,
                bindings,
                current_var,
                current_key,
            )
            .await?
                || graph_filter_passes(
                    right,
                    write_path,
                    keyspace,
                    schema,
                    bindings,
                    current_var,
                    current_key,
                )
                .await?),
            Expr::Not(inner) => Ok(!graph_filter_passes(
                inner,
                write_path,
                keyspace,
                schema,
                bindings,
                current_var,
                current_key,
            )
            .await?),
            Expr::PatternPredicate {
                start_var,
                hops,
                negated,
            } => {
                let exists = pattern_predicate_exists(
                    write_path,
                    keyspace,
                    schema,
                    bindings,
                    current_var,
                    current_key,
                    start_var,
                    hops,
                )
                .await?;
                Ok(if *negated { !exists } else { exists })
            }
            _ => eval::filter_passes(expr, bindings),
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn pattern_predicate_exists(
    write_path: &WritePath,
    keyspace: &str,
    schema: Option<&Schema>,
    bindings: &HashMap<String, serde_json::Value>,
    current_var: &str,
    current_key: &DecoratedKey,
    start_var: &str,
    hops: &[crate::parser::PatternPredicateHop],
) -> Result<bool> {
    if hops.is_empty() {
        return Ok(bindings.contains_key(start_var));
    }
    if start_var != current_var {
        return Err(GraphError::Validation(format!(
            "pattern predicate start variable `{start_var}` is not bound at this point"
        )));
    }

    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");
    let mut frontier = vec![current_key.clone()];

    for (hop_index, hop) in hops.iter().enumerate() {
        let mut next_frontier = Vec::new();
        for vertex_key in &frontier {
            let mut source_keys = vec![vertex_key.clone()];
            let hex_candidate = DecoratedKey::new(ferrosa_common::PartitionKey::new(
                hex::encode(vertex_key.key.as_bytes()).into_bytes(),
            ));
            source_keys.push(hex_candidate);
            if hop_index == 0 {
                if let Some(serde_json::Value::Object(obj)) = bindings.get(start_var) {
                    for (name, value) in obj {
                        if name == "id" || name.ends_with("_id") {
                            if let serde_json::Value::String(s) = value {
                                let candidate = DecoratedKey::new(
                                    ferrosa_common::PartitionKey::new(s.as_bytes().to_vec()),
                                );
                                if !source_keys
                                    .iter()
                                    .any(|key| key.key.as_bytes() == candidate.key.as_bytes())
                                {
                                    source_keys.push(candidate);
                                }
                            }
                        }
                    }
                }
            }
            // The adjacency and edge tables are keyed by source vertex for outgoing traversals.
            // Direction::Both can use that same outgoing index here; incoming-only predicates
            // need a reverse adjacency/edge index before they can be efficient.
            if matches!(hop.direction, Direction::In) {
                return Err(GraphError::Validation(
                    "incoming pattern predicates require a reverse adjacency index".into(),
                ));
            }

            let mut neighbor_ids = Vec::new();
            for source_key in &source_keys {
                if let Some(partition) = write_path.read(&adj_table_id, source_key).await? {
                    for row in &partition.rows {
                        if let Some(neighbor_id) = extract_neighbor_id_for_direction(
                            &row.clustering,
                            hop.rel_type.as_deref(),
                            Some(DIRECTION_OUT),
                        ) {
                            neighbor_ids.push(neighbor_id);
                        }
                    }
                }
            }

            // Indexed fallback: if derived adjacency rows are unavailable, read the
            // relationship table partition for this source vertex. This is still a
            // primary-key lookup, not a full edge-table scan.
            if neighbor_ids.is_empty() {
                if let (Some(schema_ref), Some(rel_type)) = (schema, hop.rel_type.as_ref()) {
                    if let Some(edge_meta) =
                        resolve_table_by_graph_label(schema_ref, keyspace, rel_type)
                    {
                        if edge_meta
                            .extensions
                            .get("graph.type")
                            .is_some_and(|graph_type| graph_type == "edge")
                        {
                            let edge_tid = TableId::new(&edge_meta.keyspace, &edge_meta.name);
                            let strategy = graph_replication_strategy(schema, &edge_meta.keyspace)?;
                            for source_key in &source_keys {
                                if let Some(edge_partition) = write_path
                                    .pk_read(
                                        &edge_tid,
                                        source_key,
                                        ConsistencyLevel::One,
                                        &strategy,
                                    )
                                    .await?
                                {
                                    for row in &edge_partition.rows {
                                        if let Some(components) = decode_clustering_components(
                                            &row.clustering,
                                            edge_meta.clustering_key.len(),
                                        ) {
                                            if let Some(dst) = components.first() {
                                                neighbor_ids.push(dst.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            for neighbor_id in neighbor_ids {
                let neighbor_key =
                    DecoratedKey::new(ferrosa_common::PartitionKey::new(neighbor_id.clone()));

                if let Some(target_label) = &hop.target_label {
                    let Some(schema_ref) = schema else {
                        return Err(GraphError::Validation(
                            "pattern predicate label filters require schema metadata".into(),
                        ));
                    };
                    let Some(target_meta) =
                        resolve_table_by_graph_label(schema_ref, keyspace, target_label)
                    else {
                        return Err(GraphError::Validation(format!(
                            "unknown graph label in pattern predicate: {target_label}"
                        )));
                    };
                    let target_tid = TableId::new(&target_meta.keyspace, &target_meta.name);
                    let matched = find_vertex_match(
                        write_path,
                        &target_tid,
                        &target_meta,
                        &HashMap::new(),
                        &neighbor_id,
                        hop.target_props.as_slice(),
                        "_pattern_predicate_target",
                        schema,
                    )
                    .await?;
                    if matched.is_none() {
                        continue;
                    }
                }

                next_frontier.push(neighbor_key);
            }
        }
        if next_frontier.is_empty() {
            return Ok(false);
        }
        next_frontier.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
        next_frontier.dedup_by(|a, b| a.key.as_bytes() == b.key.as_bytes());
        frontier = next_frontier;
    }

    Ok(!frontier.is_empty())
}

/// Whether an expression (or any sub-expression) is a pattern comprehension,
/// which requires graph-aware evaluation during projection.
fn expr_has_pattern_comprehension(expr: &Expr) -> bool {
    match expr {
        Expr::PatternComprehension { .. } => true,
        Expr::Function { args, .. } | Expr::List(args) => {
            args.iter().any(expr_has_pattern_comprehension)
        }
        Expr::Distinct(e) | Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => {
            expr_has_pattern_comprehension(e)
        }
        Expr::Comparison { left, right, .. }
        | Expr::Arithmetic { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::In {
            value: left,
            list: right,
        }
        | Expr::Index {
            target: left,
            index: right,
        } => expr_has_pattern_comprehension(left) || expr_has_pattern_comprehension(right),
        Expr::ListPredicate {
            list, predicate, ..
        } => expr_has_pattern_comprehension(list) || expr_has_pattern_comprehension(predicate),
        Expr::ListComprehension {
            list,
            filter,
            projection,
            ..
        } => {
            expr_has_pattern_comprehension(list)
                || filter
                    .as_deref()
                    .is_some_and(expr_has_pattern_comprehension)
                || projection
                    .as_deref()
                    .is_some_and(expr_has_pattern_comprehension)
        }
        Expr::Map(props) => props.iter().any(|(_, e)| expr_has_pattern_comprehension(e)),
        Expr::MapProjection { selectors, .. } => selectors.iter().any(|s| match s {
            crate::parser::MapProjectionSelector::Literal { value, .. } => {
                expr_has_pattern_comprehension(value)
            }
            _ => false,
        }),
        Expr::Slice { target, start, end } => {
            expr_has_pattern_comprehension(target)
                || start.as_deref().is_some_and(expr_has_pattern_comprehension)
                || end.as_deref().is_some_and(expr_has_pattern_comprehension)
        }
        Expr::Var(_)
        | Expr::Property { .. }
        | Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::PatternPredicate { .. } => false,
    }
}

/// Evaluate a RETURN/projection expression with graph context.
///
/// Pattern comprehensions need to traverse edges, so they cannot be handled by
/// the pure-binding [`eval::eval_expr`]. This function intercepts them (and any
/// nesting that contains them) and delegates everything else to `eval_expr`.
#[allow(clippy::too_many_arguments)]
fn graph_eval_expr<'a>(
    expr: &'a Expr,
    write_path: &'a WritePath,
    keyspace: &'a str,
    schema: Option<&'a Schema>,
    bindings: &'a HashMap<String, serde_json::Value>,
    current_var: &'a str,
    current_key: &'a DecoratedKey,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expr::PatternComprehension {
                start_var,
                hops,
                filter,
                projection,
            } => {
                eval_pattern_comprehension(
                    write_path,
                    keyspace,
                    schema,
                    bindings,
                    current_var,
                    current_key,
                    start_var,
                    hops,
                    filter.as_deref(),
                    projection,
                )
                .await
            }
            // No graph-aware sub-evaluation needed for other expression kinds.
            _ => eval::eval_expr(expr, bindings),
        }
    })
}

/// Evaluate a pattern comprehension `[ (start)-[:R]->(m) WHERE p | proj ]`.
///
/// Traverses a single hop from the already-bound `start_var`, binds each
/// matched target node to its variable, applies the optional `filter`, and
/// collects the `projection` per surviving match into a list.
///
/// Multi-hop comprehensions are not yet supported and fail loud rather than
/// returning a partial/wrong list.
#[allow(clippy::too_many_arguments)]
async fn eval_pattern_comprehension(
    write_path: &WritePath,
    keyspace: &str,
    schema: Option<&Schema>,
    bindings: &HashMap<String, serde_json::Value>,
    current_var: &str,
    current_key: &DecoratedKey,
    start_var: &str,
    hops: &[PatternComprehensionHop],
    filter: Option<&Expr>,
    projection: &Expr,
) -> Result<serde_json::Value> {
    if hops.len() != 1 {
        return Err(GraphError::Validation(
            "multi-hop pattern comprehensions are not yet supported".into(),
        ));
    }
    if start_var != current_var {
        return Err(GraphError::Validation(format!(
            "pattern comprehension start variable `{start_var}` is not bound at this point"
        )));
    }
    let hop = &hops[0];
    if matches!(hop.direction, Direction::In) {
        return Err(GraphError::Validation(
            "incoming pattern comprehensions require a reverse adjacency index".into(),
        ));
    }

    let neighbor_ids = outgoing_neighbor_ids(
        write_path,
        keyspace,
        schema,
        current_key,
        hop.rel_type.as_deref(),
    )
    .await?;

    let mut out = Vec::new();
    for neighbor_id in neighbor_ids {
        let target_json = resolve_target_node_json(
            write_path,
            keyspace,
            schema,
            &neighbor_id,
            hop.target_label.as_deref(),
            hop.target_props.as_slice(),
            hop.target_var.as_deref().unwrap_or("_comprehension_target"),
        )
        .await?;
        let Some(target_json) = target_json else {
            continue;
        };

        let mut scoped = bindings.clone();
        if let Some(target_var) = &hop.target_var {
            scoped.insert(target_var.clone(), target_json);
        }
        if let Some(pred) = filter {
            if !eval::filter_passes(pred, &scoped)? {
                continue;
            }
        }
        out.push(eval::eval_expr(projection, &scoped)?);
    }
    Ok(serde_json::Value::Array(out))
}

/// Collect outgoing neighbor vertex ids for a source vertex over an optional
/// relationship type, using the adjacency index with an edge-table fallback.
async fn outgoing_neighbor_ids(
    write_path: &WritePath,
    keyspace: &str,
    schema: Option<&Schema>,
    source_key: &DecoratedKey,
    rel_type: Option<&str>,
) -> Result<Vec<Vec<u8>>> {
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    let hex_candidate = DecoratedKey::new(PartitionKey::new(
        hex::encode(source_key.key.as_bytes()).into_bytes(),
    ));
    let source_keys = [source_key.clone(), hex_candidate];

    let mut neighbor_ids = Vec::new();
    for key in &source_keys {
        if let Some(partition) = write_path.read(&adj_table_id, key).await? {
            for row in &partition.rows {
                if let Some(neighbor_id) = extract_neighbor_id_for_direction(
                    &row.clustering,
                    rel_type,
                    Some(DIRECTION_OUT),
                ) {
                    neighbor_ids.push(neighbor_id);
                }
            }
        }
    }

    if neighbor_ids.is_empty() {
        if let (Some(schema_ref), Some(rel_type)) = (schema, rel_type) {
            if let Some(edge_meta) = resolve_table_by_graph_label(schema_ref, keyspace, rel_type) {
                if edge_meta
                    .extensions
                    .get("graph.type")
                    .is_some_and(|graph_type| graph_type == "edge")
                {
                    let edge_tid = TableId::new(&edge_meta.keyspace, &edge_meta.name);
                    let strategy = graph_replication_strategy(schema, &edge_meta.keyspace)?;
                    for key in &source_keys {
                        if let Some(edge_partition) = write_path
                            .pk_read(&edge_tid, key, ConsistencyLevel::One, &strategy)
                            .await?
                        {
                            for row in &edge_partition.rows {
                                if let Some(components) = decode_clustering_components(
                                    &row.clustering,
                                    edge_meta.clustering_key.len(),
                                ) {
                                    if let Some(dst) = components.first() {
                                        neighbor_ids.push(dst.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(neighbor_ids)
}

/// Load a neighbor vertex's properties as JSON, honoring an optional label and
/// inline property filter. Returns `None` when the label is missing or the
/// inline filter does not match.
async fn resolve_target_node_json(
    write_path: &WritePath,
    keyspace: &str,
    schema: Option<&Schema>,
    neighbor_id: &[u8],
    target_label: Option<&str>,
    target_props: &[(String, Expr)],
    var_name: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(target_label) = target_label else {
        // Unlabeled target: expose the neighbor id (hex) when we cannot resolve
        // a concrete row, so projections like `| id(m)` still work.
        return Ok(Some(serde_json::Value::String(hex::encode(neighbor_id))));
    };
    let Some(schema_ref) = schema else {
        return Err(GraphError::Validation(
            "pattern comprehension label filters require schema metadata".into(),
        ));
    };
    let Some(target_meta) = resolve_table_by_graph_label(schema_ref, keyspace, target_label) else {
        return Err(GraphError::Validation(format!(
            "unknown graph label in pattern comprehension: {target_label}"
        )));
    };
    let target_tid = TableId::new(&target_meta.keyspace, &target_meta.name);
    let matched = find_vertex_match(
        write_path,
        &target_tid,
        &target_meta,
        &HashMap::new(),
        neighbor_id,
        target_props,
        var_name,
        schema,
    )
    .await?;
    Ok(matched.map(|m| m.json))
}

async fn execute_unwind(
    expr: &Expr,
    var: &str,
    with_pipeline: Option<&WithPipeline>,
    return_clause: &ReturnClause,
    start: Instant,
) -> Result<GraphResult> {
    let value = eval::eval_expr(expr, &HashMap::new())?;
    let values = match value {
        serde_json::Value::Array(values) => values,
        serde_json::Value::Null => Vec::new(),
        other => {
            return Err(GraphError::Validation(format!(
                "UNWIND expects a list expression, got {other:?}"
            )))
        }
    };
    let mut states = Vec::new();
    for value in values {
        let mut bindings = HashMap::new();
        bindings.insert(var.to_string(), value);
        states.push(ExpandState {
            current_key: DecoratedKey::new(PartitionKey::new(Vec::new())),
            bindings,
        });
    }
    if let Some(with_pipeline) = with_pipeline {
        states = apply_with_pipeline(states, with_pipeline)?;
    }

    let columns = build_columns(return_clause);

    // ORDER BY must see every row before any row is final, so it stays a
    // pipeline-breaker over the (bounded) UNWIND list. Without it, projection
    // and LIMIT stream: rows are produced lazily and `take(limit)` stops pulling
    // — no full projection followed by a truncate (t_4ce82a3e increment 2).
    let rows = if return_clause.order_by.is_empty() {
        use futures::StreamExt as _;
        let limit = return_clause
            .limit
            .map(|l| l.max(0) as usize)
            .unwrap_or(usize::MAX);
        let projected = futures::stream::iter(states.iter()).map(|state| {
            Ok(return_clause
                .items
                .iter()
                .map(|item| {
                    eval::eval_expr(&item.expr, &state.bindings).unwrap_or(serde_json::Value::Null)
                })
                .collect::<Vec<_>>())
        });
        let stream: crate::executor::stream::RowStream<'_> = Box::pin(projected.take(limit));
        crate::executor::stream::collect_rows(stream).await?
    } else {
        let mut rows = Vec::new();
        for state in &states {
            rows.push(
                return_clause
                    .items
                    .iter()
                    .map(|item| {
                        eval::eval_expr(&item.expr, &state.bindings)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect::<Vec<_>>(),
            );
        }
        sort_projected_rows_by_bindings(&mut rows, &states, return_clause);
        if let Some(limit) = return_clause.limit {
            rows.truncate(limit.max(0) as usize);
        }
        rows
    };

    Ok(GraphResult {
        columns,
        rows,
        stats: QueryStats {
            vertices_read: 0,
            edges_read: 0,
            vertices_written: 0,
            vertices_deleted: 0,
            edges_deleted: 0,
            execution_ms: start.elapsed().as_millis() as u64,
        },
    })
}

/// The borrowed pieces of a `PhysicalPlan::Expand`.
#[derive(Clone, Copy)]
struct ExpandPlan<'p> {
    anchor: &'p Anchor,
    hops: &'p [Hop],
    optional_hops: &'p [Hop],
    post_filters: &'p [Expr],
    with_pipeline: Option<&'p crate::parser::WithPipeline>,
    return_clause: &'p ReturnClause,
}

/// The shared execution context an Expand runs against.
#[derive(Clone, Copy)]
struct ExpandCtx<'a> {
    write_path: &'a WritePath,
    keyspace: &'a str,
    config: &'a GraphEngineConfig,
    virtual_tables: Option<&'a VirtualTableRegistry>,
    schema: Option<&'a Schema>,
    start: Instant,
}

/// Outcome of an Expand's state-materialization phase.
enum ExpandStates {
    /// Virtual-table anchor: `execute_virtual_anchor` produced the whole result
    /// itself. Deliberately NOT converted to streaming — its handling of
    /// `ORDER BY`/`DISTINCT` is under separate investigation, and streaming it
    /// would lock in the current behavior.
    Virtual(GraphResult),
    /// The surviving states after hops, post-filters, OPTIONAL MATCH and the
    /// WITH pipeline, plus the stats accumulated getting there.
    Materialized(Vec<ExpandState>, QueryStats),
}

/// Execute a `PhysicalPlan::Expand` and buffer the whole result.
async fn execute_expand(plan: ExpandPlan<'_>, ctx: ExpandCtx<'_>) -> Result<GraphResult> {
    match expand_to_states(plan, ctx).await? {
        ExpandStates::Virtual(result) => Ok(result),
        ExpandStates::Materialized(states, stats) => {
            finish_expand_buffered(states, stats, plan, ctx).await
        }
    }
}

/// Owned pieces of a `PhysicalPlan::Expand`, for the streaming entry point.
///
/// Owned rather than borrowed because the projection stream outlives the
/// destructured plan: `execute_streaming` consumes the `PhysicalPlan` by value,
/// so nothing in the returned stream may borrow from it.
struct ExpandParts {
    anchor: Anchor,
    hops: Vec<Hop>,
    optional_hops: Vec<Hop>,
    post_filters: Vec<Expr>,
    with_pipeline: Option<crate::parser::WithPipeline>,
    return_clause: ReturnClause,
}

/// Execute a `PhysicalPlan::Expand`, streaming the projection when it is safe.
///
/// Phase A (which states survive) is shared verbatim with the buffered path and
/// is NOT lazy — the hop loop still materializes the frontier, and the
/// sequential-vs-concurrent hop split is untouched. What streams is phase B:
/// one `project_state` per pull, so a `LIMIT k` projects k states instead of
/// projecting everything and truncating.
///
/// Falls back to [`finish_expand_buffered`] when the query has an `ORDER BY`
/// (it must see every row before the first is final), and to
/// `execute_virtual_anchor`'s buffered result for a virtual-table anchor.
///
/// `DISTINCT` composes as [`dedup_stream`] on top of the projection — see that
/// function for the deliberate first-seen-vs-sorted order change.
async fn execute_expand_streaming<'a>(
    parts: ExpandParts,
    ctx: ExpandCtx<'a>,
) -> StreamingPlanResult<'a> {
    use futures::StreamExt as _;

    let plan = ExpandPlan {
        anchor: &parts.anchor,
        hops: &parts.hops,
        optional_hops: &parts.optional_hops,
        post_filters: &parts.post_filters,
        with_pipeline: parts.with_pipeline.as_ref(),
        return_clause: &parts.return_clause,
    };

    let (states, mut stats) = match expand_to_states(plan, ctx).await? {
        ExpandStates::Virtual(result) => {
            return Ok((result.columns, stream_from_rows(result.rows), result.stats));
        }
        ExpandStates::Materialized(states, stats) => (states, stats),
    };

    // ORDER BY (with or without DISTINCT) is a pipeline breaker: no row is
    // final until every row is projected. But "must see every row" does not
    // mean "must hold every row": when the spilling sorter applies, project
    // each state straight INTO it and stream the merged output — the full
    // result set is never resident (t_3f2f961a).
    //
    // DISTINCT used to be excluded here because the buffered tail re-sorted by
    // debug string after the ORDER BY sort, so streaming it would have changed
    // observable behaviour. That re-sort was a bug — it discarded the query's
    // ORDER BY — and now that the buffered tail dedups without reordering, both
    // forms are first-seen order and the exclusion has nothing left to protect.
    if !parts.return_clause.order_by.is_empty() {
        let columns = build_columns(&parts.return_clause);
        {
            if let Some(mut sink) = crate::executor::spill::SpillSortSink::try_new(
                &columns,
                &parts.return_clause,
                ctx.config,
                "graph_order_by_stream",
            )? {
                let proj = ExpandProjection {
                    needs_graph_projection: parts
                        .return_clause
                        .items
                        .iter()
                        .any(|item| expr_has_pattern_comprehension(&item.expr)),
                    anchor_var: parts.anchor.var.clone().unwrap_or_default(),
                    return_clause: parts.return_clause.clone(),
                    write_path: ctx.write_path,
                    keyspace: ctx.keyspace,
                    schema: ctx.schema,
                };
                project_states_into_sink(
                    &states,
                    &proj,
                    &mut sink,
                    ctx.start,
                    ctx.config.query_timeout,
                )
                .await?;
                stats.execution_ms = ctx.start.elapsed().as_millis() as u64;
                let mut sorted = sink.finish_stream()?;

                // DISTINCT on the sorted stream, in the same order the buffered
                // tail applies it: sort, then dedup, then limit.
                //
                // `dedup_stream` hashes every row it has emitted, so this is
                // bounded by the number of DISTINCT rows rather than by the
                // number of candidate rows — inherent to DISTINCT without a
                // spilling set, and still far below materialising everything.
                // Adjacent-only dedup would be O(1) but WRONG here: the stream
                // is sorted by the ORDER BY terms, which may be a subset of the
                // projected columns, so equal rows are not necessarily adjacent.
                if parts.return_clause.distinct {
                    sorted = crate::executor::stream::dedup_stream(sorted);
                }

                // LIMIT applies to the sorted, deduplicated output. Taking it
                // here is what lets the sink accept a LIMIT at all: the sort
                // still sees every candidate row, but only `limit` are pulled.
                if let Some(limit) = parts.return_clause.limit {
                    use futures::StreamExt as _;
                    sorted = Box::pin(sorted.take(limit.max(0) as usize));
                }
                return Ok((columns, sorted, stats));
            }
        }
        // Fallback: no spill backend, LIMIT present, an order term that is not
        // a projected column (needs the states' bindings), or DISTINCT.
        let result = finish_expand_buffered(states, stats, plan, ctx).await?;
        return Ok((result.columns, stream_from_rows(result.rows), result.stats));
    }

    let columns = build_columns(&parts.return_clause);
    let needs_graph_projection = parts
        .return_clause
        .items
        .iter()
        .any(|item| expr_has_pattern_comprehension(&item.expr));
    let distinct = parts.return_clause.distinct;
    let limit = parts.return_clause.limit.map(|limit| limit.max(0) as usize);
    // Stats are returned up front (see `execute_streaming`), so `execution_ms`
    // is measured here rather than after the last row. For a streamed Expand it
    // therefore EXCLUDES projection time, where the buffered path included it.
    // Fixing that needs the deferred stats handle, not a change here.
    stats.execution_ms = ctx.start.elapsed().as_millis() as u64;

    let ExpandParts {
        anchor,
        return_clause,
        ..
    } = parts;
    let mut rows = project_states_stream(
        states,
        ExpandProjection {
            return_clause,
            // Matches the buffered tail's `anchor.var.as_deref().unwrap_or("")`.
            anchor_var: anchor.var.unwrap_or_default(),
            needs_graph_projection,
            write_path: ctx.write_path,
            keyspace: ctx.keyspace,
            schema: ctx.schema,
        },
    );
    if distinct {
        rows = dedup_stream(rows);
    }
    if let Some(limit) = limit {
        // `rows.truncate(limit)` becomes `take(limit)`: the states past the
        // limit are never projected at all.
        rows = Box::pin(rows.take(limit));
    }
    Ok((columns, rows, stats))
}

/// Phase A of an Expand: anchor scan, hop traversal, post-filters, OPTIONAL
/// MATCH and the WITH pipeline — everything that decides WHICH states survive.
///
/// Shared verbatim by the buffered and streaming tails, so the two can never
/// disagree about what matched. Nothing here is lazy yet: the hop loop still
/// materializes the frontier (later increments), and `max_fan_out_per_hop`
/// still guards it exactly as before.
async fn expand_to_states(plan: ExpandPlan<'_>, ctx: ExpandCtx<'_>) -> Result<ExpandStates> {
    let ExpandPlan {
        anchor,
        hops,
        optional_hops,
        post_filters,
        with_pipeline,
        return_clause,
    } = plan;
    let ExpandCtx {
        write_path,
        keyspace,
        config,
        virtual_tables,
        schema,
        start,
    } = ctx;
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
        )
        .map(ExpandStates::Virtual);
    }

    let (mut current_states, traversal_hops): (Vec<ExpandState>, &[Hop]) = if let Some(states) =
        try_edge_anchored_initial_states(
            write_path,
            anchor,
            hops,
            optional_hops,
            schema,
            &mut stats,
        )
        .await?
    {
        check_timeout(start, config.query_timeout)?;
        (states, &hops[1..])
    } else {
        let anchor_meta = table_metadata_for(schema, &anchor.table.keyspace, &anchor.table.table);
        let anchor_partitions =
            read_anchor_partitions(write_path, anchor, anchor_meta.as_ref(), schema).await?;
        stats.vertices_read += anchor_partitions.len();
        check_timeout(start, config.query_timeout)?;

        // Resolve column names from schema for property mapping.
        let anchor_col_names =
            column_names_for_table(schema, &anchor.table.keyspace, &anchor.table.table);

        // Apply WHERE filters to anchor partitions using the expression evaluator.
        // Skip partitions that are fully tombstoned (no live cells in any row
        // and no live static row) — these represent deleted vertices whose
        // tombstones have not yet been purged by compaction.
        let anchor_var = anchor.var.as_deref().unwrap_or("_anon");
        let mut states: Vec<ExpandState> = Vec::with_capacity(anchor_partitions.len());
        for partition in &anchor_partitions {
            if is_partition_dead(partition) {
                continue;
            }

            if let Some(meta) = anchor_meta.as_ref() {
                for row in &partition.rows {
                    let row_json = row_to_json(meta, partition, row);
                    if !prop_map_passes(anchor_var, &anchor.props, &row_json)? {
                        continue;
                    }

                    let mut bindings = HashMap::new();
                    bindings.insert(anchor_var.to_string(), row_json);
                    let current_key = graph_vertex_lookup_key(meta, partition, row, &anchor.props)
                        .unwrap_or_else(|| partition.key.clone());

                    let mut passes = true;
                    for filter in &anchor.filters {
                        if !graph_filter_passes(
                            filter,
                            write_path,
                            keyspace,
                            schema,
                            &bindings,
                            anchor_var,
                            &current_key,
                        )
                        .await?
                        {
                            passes = false;
                            break;
                        }
                    }
                    if passes {
                        states.push(ExpandState {
                            current_key,
                            bindings,
                        });
                    }
                }
            } else {
                let hex_id = hex::encode(partition.key.key.as_bytes());
                let row_json = eval::partition_to_json(partition, &hex_id, &anchor_col_names);
                let mut bindings = HashMap::new();
                bindings.insert(anchor_var.to_string(), row_json);

                let mut passes = true;
                for filter in &anchor.filters {
                    if !graph_filter_passes(
                        filter,
                        write_path,
                        keyspace,
                        schema,
                        &bindings,
                        anchor_var,
                        &partition.key,
                    )
                    .await?
                    {
                        passes = false;
                        break;
                    }
                }
                if passes {
                    states.push(ExpandState {
                        current_key: partition.key.clone(),
                        bindings,
                    });
                }
            }
        }
        (states, hops)
    };

    // Step 2: For each hop, traverse adjacency index.
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    // t_0cc8d63e: LIMIT short-circuit. For a plain bounded expansion — no
    // ORDER BY / DISTINCT / WITH pipeline / post-MATCH WHERE / OPTIONAL MATCH
    // (each of which reorders rows or needs the full set before limiting) — the
    // first `limit` rows in expansion order are exactly the final answer. So we
    // may stop expanding (and hydrating) the LAST hop as soon as `limit` rows
    // accumulate, instead of hydrating the whole fan-out and truncating at the
    // end. Aggregates never reach this path (they use PhysicalPlan::Aggregate).
    let pushdown_cap: Option<usize> = return_clause.limit.and_then(|limit| {
        let safe = return_clause.order_by.is_empty()
            && !return_clause.distinct
            && with_pipeline.is_none()
            && post_filters.is_empty()
            && optional_hops.is_empty();
        (safe && limit > 0).then_some(limit as usize)
    });
    for hop in traversal_hops.iter() {
        check_timeout(start, config.query_timeout)?;
        // LIMIT short-circuit on EVERY hop, not just the last (t_4ce82a3e
        // increment 4).
        //
        // Why an INTERMEDIATE hop may also stop at `limit`: `pushdown_cap` is
        // only set when there is no ORDER BY / DISTINCT / WITH pipeline /
        // post-MATCH WHERE / OPTIONAL MATCH (see its definition above). With all
        // of those absent, nothing between the hop loop and the projection can
        // drop or reorder a state, and the projection emits EXACTLY ONE row per
        // surviving state. So `limit` states at any hop can already produce the
        // whole answer: expanding an extra state could only ever yield rows past
        // the limit, which are discarded. Each state's own fan-out still feeds
        // the next hop — we simply stop collecting NEW states once we hold
        // enough to satisfy the limit.
        //
        // (Without the guard conditions this would be wrong: a post-filter could
        // drop a state, an ORDER BY could promote a later one, and truncating
        // early would lose rows. That is exactly why the cap is `None` then.)
        let cap_this_hop = pushdown_cap;

        let mut next_states = Vec::new();
        'states: for state in &current_states {
            let vertex_key = &state.current_key;
            // Read adjacency entries for this vertex.
            let adj_partition = write_path.read(&adj_table_id, vertex_key).await?;
            if let Some(partition) = adj_partition {
                stats.edges_read += partition.rows.len();

                // If this hop has property filters and an edge table, read
                // the edge partition once so we can check edge properties.
                let edge_meta = hop
                    .edge_table
                    .as_ref()
                    .and_then(|et| table_metadata_for(schema, &et.keyspace, &et.table));
                let vertex_meta = hop
                    .vertex_table
                    .as_ref()
                    .and_then(|vt| table_metadata_for(schema, &vt.keyspace, &vt.table));

                let pairs: Vec<(&Row, Vec<u8>)> = partition
                    .rows
                    .iter()
                    .filter_map(|row| {
                        extract_neighbor_id_for_direction(
                            &row.clustering,
                            hop.edge_label.as_deref(),
                            expected_adjacency_direction(hop.direction),
                        )
                        .map(|nid| (row, nid))
                    })
                    .collect();

                // Hydrate this vertex's neighbors with BOUNDED CONCURRENCY
                // (t_4ce82a3e increment 3).
                //
                // Every input to `hydrate_hop_neighbor` is shared/immutable, so
                // neighbors of one vertex hydrate independently. The futures are
                // held in a `FuturesUnordered` that this loop drives INLINE —
                // which, unlike `buffer_unordered`/`tokio::spawn`, does NOT
                // require `Send + 'static`: the set polls the futures in place,
                // so they may keep borrowing `write_path`/`schema`/`hop`/`state`
                // /`row`. That is what dissolves the old "Send is not general
                // enough" wall WITHOUT the clone-per-neighbor restructure the
                // task forbids — the only owned input is `nid`, exactly as
                // before.
                //
                // In-flight is capped at HOP_HYDRATION_CONCURRENCY so a
                // high-degree hub cannot launch unbounded concurrent reads, and
                // `cap_this_hop` (LIMIT push-down) still stops the moment the
                // cap is met, keeping the bounded-query storage-touch guarantee
                // (http_limit_short_circuits_expansion_hydration).
                //
                // Ordering: results are drained in completion order, so a
                // `cap_this_hop` prefix could differ from the sequential order.
                // Capped hops therefore stay STRICTLY SEQUENTIAL (concurrency
                // would change which rows a LIMIT returns); only uncapped hops —
                // where the full fan-out is consumed and order is re-established
                // downstream — hydrate concurrently.
                let mut pairs_iter = pairs.into_iter();
                if cap_this_hop.is_some() {
                    for (row, nid) in pairs_iter {
                        if let Some(st) = hydrate_hop_neighbor(
                            write_path,
                            schema,
                            hop,
                            state,
                            vertex_key,
                            edge_meta.as_ref(),
                            vertex_meta.as_ref(),
                            row,
                            nid,
                        )
                        .await?
                        {
                            next_states.push(st);
                            if cap_this_hop.is_some_and(|cap| next_states.len() >= cap) {
                                break 'states;
                            }
                        }
                    }
                } else {
                    use futures::stream::{FuturesUnordered, StreamExt as _};
                    let mut in_flight = FuturesUnordered::new();
                    loop {
                        while in_flight.len() < HOP_HYDRATION_CONCURRENCY {
                            let Some((row, nid)) = pairs_iter.next() else {
                                break;
                            };
                            in_flight.push(hydrate_hop_neighbor(
                                write_path,
                                schema,
                                hop,
                                state,
                                vertex_key,
                                edge_meta.as_ref(),
                                vertex_meta.as_ref(),
                                row,
                                nid,
                            ));
                        }
                        let Some(hydrated) = in_flight.next().await else {
                            break;
                        };
                        if let Some(st) = hydrated? {
                            next_states.push(st);
                        }
                    }
                }

                // T4: fan-out limit per hop.
                if next_states.len() > config.max_fan_out_per_hop {
                    return Err(GraphError::ResourceLimit(format!(
                        "fan-out limit exceeded: {} neighbors (limit: {})",
                        next_states.len(),
                        config.max_fan_out_per_hop
                    )));
                }
            }
        }

        if let Some(cap) = cap_this_hop {
            next_states.truncate(cap);
        }
        stats.vertices_read += next_states.len();
        current_states = next_states;
    }

    // Apply WHERE predicates that reference hop-bound variables. The required
    // pattern is now fully matched, so every variable (anchor + each hop's
    // target) is bound in each state — these predicates would wrongly filter
    // everything out if evaluated earlier at the anchor scan.
    if !post_filters.is_empty() {
        let anchor_var = anchor.var.as_deref().unwrap_or("");
        let mut kept = Vec::with_capacity(current_states.len());
        for state in current_states {
            let mut passes = true;
            for filter in post_filters {
                if !graph_filter_passes(
                    filter,
                    write_path,
                    keyspace,
                    schema,
                    &state.bindings,
                    anchor_var,
                    &state.current_key,
                )
                .await?
                {
                    passes = false;
                    break;
                }
            }
            if passes {
                kept.push(state);
            }
        }
        current_states = kept;
    }

    if !optional_hops.is_empty() {
        current_states = execute_optional_hops(
            write_path,
            keyspace,
            current_states,
            optional_hops,
            config,
            start,
            schema,
            &mut stats,
        )
        .await?;
    }

    if let Some(with_pipeline) = with_pipeline {
        current_states = apply_with_pipeline(current_states, with_pipeline)?;
    }

    Ok(ExpandStates::Materialized(current_states, stats))
}

/// Phase B, buffered: project every surviving state, then `ORDER BY`,
/// `DISTINCT` and `LIMIT` over the materialized rows.
///
/// This is the pipeline-breaking tail. `execute_expand_streaming` keeps it
/// unchanged for any query with an `ORDER BY`, and replaces it with
/// `project_states_stream` (+ `dedup_stream` + `take`) otherwise.
async fn finish_expand_buffered(
    current_states: Vec<ExpandState>,
    mut stats: QueryStats,
    plan: ExpandPlan<'_>,
    ctx: ExpandCtx<'_>,
) -> Result<GraphResult> {
    let ExpandPlan {
        anchor,
        return_clause,
        ..
    } = plan;
    let ExpandCtx {
        write_path,
        keyspace,
        config,
        schema,
        start,
        ..
    } = ctx;
    // Step 3: Build result from return clause, projecting property values.
    let columns = build_columns(return_clause);

    // Pattern comprehensions in the projection need graph traversal keyed on
    // the anchor (start) variable. Only take the graph-aware path when a
    // comprehension is actually present, to avoid per-row overhead otherwise.
    let needs_graph_projection = return_clause
        .items
        .iter()
        .any(|item| expr_has_pattern_comprehension(&item.expr));
    let anchor_var = anchor.var.as_deref().unwrap_or("");

    // Project every surviving state. The old `max_result_rows` break here
    // SILENTLY TRUNCATED the result — no error, no flag, so a client could not
    // tell a partial answer from a complete one. Removed (t_4ce82a3e): an
    // unbounded ORDER BY now SPILLS instead of being capped, which is the
    // honest answer rather than a quiet wrong one.
    //
    // When the spilling sorter applies, each projected row goes straight INTO
    // it — the interim rows `Vec` and the per-state clones (only ever needed by
    // the bindings-based fallback sort) are skipped entirely, so projection
    // memory is bounded by the spill threshold rather than the result size
    // (t_3f2f961a). The final `Vec` below is this function's buffered
    // contract; the streaming entry point avoids even that.
    //
    // ORDER BY runs before projection-only values disappear: Cypher permits
    // ordering by expressions that are not part of RETURN, and exactly that
    // case is why the fallback sorts via the states' bindings — a spilled row
    // carries no binding context, so `SpillSortSink::try_new` refuses it.
    let sink = crate::executor::spill::SpillSortSink::try_new(
        &columns,
        return_clause,
        config,
        "graph_order_by",
    )?;
    let mut rows = Vec::new();
    match sink {
        Some(mut sink) => {
            let proj = ExpandProjection {
                return_clause: return_clause.clone(),
                anchor_var: anchor_var.to_string(),
                needs_graph_projection,
                write_path,
                keyspace,
                schema,
            };
            project_states_into_sink(
                &current_states,
                &proj,
                &mut sink,
                start,
                config.query_timeout,
            )
            .await?;
            rows = sink.finish_rows()?;
        }
        None => {
            let mut result_states = Vec::new();
            for state in &current_states {
                let row = project_state(
                    return_clause,
                    needs_graph_projection,
                    write_path,
                    keyspace,
                    schema,
                    anchor_var,
                    state,
                )
                .await?;
                result_states.push(state.clone());
                rows.push(row);
            }
            sort_projected_rows_by_bindings(&mut rows, &result_states, return_clause);
        }
    }

    // Apply DISTINCT.
    if return_clause.distinct {
        dedup_preserving_order(&mut rows);
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

/// Read the anchor table's candidate partitions: a direct primary-key lookup
/// when the anchor's properties pin the full key, else a range scan of the
/// table.
///
/// Extracted from `execute_expand` so the anchor read strategy is one named
/// decision the pull-based streaming `AnchorScan` (t_4ce82a3e,
/// specs/streaming-executor-design.md) can reuse. NOTE for that work: the
/// `range_read` arm is the unbounded materialization — the streaming operator
/// replaces it with a lazy scan, while the pk arm is already bounded.
async fn read_anchor_partitions(
    write_path: &WritePath,
    anchor: &Anchor,
    anchor_meta: Option<&ferrosa_schema::metadata::table::TableMetadata>,
    schema: Option<&Schema>,
) -> Result<Vec<Partition>> {
    let anchor_table_id = TableId::new(&anchor.table.keyspace, &anchor.table.table);
    if let Some(meta) = anchor_meta {
        if let Some((key, _clustering)) =
            build_direct_lookup_shape(meta, &HashMap::new(), &anchor.props, &HashMap::new())?
        {
            let strategy = graph_replication_strategy(schema, &anchor.table.keyspace)?;
            return Ok(write_path
                .pk_read(&anchor_table_id, &key, ConsistencyLevel::One, &strategy)
                .await?
                .into_iter()
                .collect());
        }
    }
    Ok(write_path.range_read(&anchor_table_id).await?)
}

/// Project one expand state's bindings into a result row per the RETURN items.
///
/// Extracted from the `execute_expand` projection loop so the pull-based
/// streaming `Project` operator (t_4ce82a3e, specs/streaming-executor-design.md)
/// reuses this exact per-state logic instead of re-deriving it — and so the
/// materialize loop no longer inlines it. `needs_graph_projection` selects the
/// graph-aware path (pattern comprehensions need traversal keyed on the anchor
/// var) vs. the cheap binding-only evaluator.
async fn project_state(
    return_clause: &ReturnClause,
    needs_graph_projection: bool,
    write_path: &WritePath,
    keyspace: &str,
    schema: Option<&Schema>,
    anchor_var: &str,
    state: &ExpandState,
) -> Result<Vec<serde_json::Value>> {
    let mut row = Vec::with_capacity(return_clause.items.len());
    for item in &return_clause.items {
        let value = if needs_graph_projection {
            graph_eval_expr(
                &item.expr,
                write_path,
                keyspace,
                schema,
                &state.bindings,
                anchor_var,
                &state.current_key,
            )
            .await?
        } else {
            eval::eval_expr(&item.expr, &state.bindings).unwrap_or(serde_json::Value::Null)
        };
        row.push(value);
    }
    Ok(row)
}

/// Everything the Expand projection needs once the surviving states are final.
///
/// Owned (`return_clause`, `anchor_var`) rather than borrowed, because the
/// projection stream outlives the destructured `PhysicalPlan` it came from.
struct ExpandProjection<'a> {
    return_clause: ReturnClause,
    anchor_var: String,
    needs_graph_projection: bool,
    write_path: &'a WritePath,
    keyspace: &'a str,
    schema: Option<&'a Schema>,
}

/// Project every surviving state sequentially, pushing each row straight into
/// the spilling ORDER BY sorter — the interim rows `Vec` never exists, so
/// projection memory is bounded by the spill threshold rather than the result
/// size (t_3f2f961a). Shared by the streaming and buffered ORDER BY tails so
/// they cannot drift.
///
/// Checks the query timeout once per state: projection can read storage (a
/// pattern comprehension traverses per row), so it is WORK and gets the same
/// bound as the hop loop. The buffered tail historically skipped that check —
/// a gap, not a behavior worth preserving.
async fn project_states_into_sink(
    states: &[ExpandState],
    proj: &ExpandProjection<'_>,
    sink: &mut crate::executor::spill::SpillSortSink,
    start: Instant,
    query_timeout: std::time::Duration,
) -> Result<()> {
    for state in states {
        check_timeout(start, query_timeout)?;
        let row = project_state(
            &proj.return_clause,
            proj.needs_graph_projection,
            proj.write_path,
            proj.keyspace,
            proj.schema,
            &proj.anchor_var,
            state,
        )
        .await?;
        sink.push(row)?;
    }
    Ok(())
}

/// Project the surviving expand states lazily: one [`project_state`] call per
/// pull, in expansion order.
///
/// This is the streaming replacement for the materializing projection loop. Two
/// things it does NOT do, and one thing it drops:
///
/// - It does not sort. `ORDER BY` must see every row before the first is final,
///   so an ordered Expand keeps the buffered tail.
/// - It does not dedup. `DISTINCT` composes on top via
///   [`dedup_stream`](crate::executor::stream::dedup_stream).
/// - It drops the buffered loop's `result_states.push(state.clone())` — that
///   per-row deep clone of every binding map existed only to let
///   `sort_projected_rows_by_bindings` order by non-projected expressions,
///   which is the `ORDER BY` path this stream never takes.
///
/// The states themselves are still materialized by the hop loop; making the
/// hops lazy is a later increment.
fn project_states_stream<'a>(
    states: Vec<ExpandState>,
    projection: ExpandProjection<'a>,
) -> RowStream<'a> {
    Box::pin(futures::stream::unfold(
        (states.into_iter(), projection, false),
        |(mut states, projection, failed)| async move {
            // Stop after a failure rather than projecting the rest: a caller
            // that kept pulling would see a partial result follow an error.
            if failed {
                return None;
            }
            let state = states.next()?;
            let row = project_state(
                &projection.return_clause,
                projection.needs_graph_projection,
                projection.write_path,
                projection.keyspace,
                projection.schema,
                &projection.anchor_var,
                &state,
            )
            .await;
            let failed = row.is_err();
            Some((row, (states, projection, failed)))
        },
    ))
}

fn sort_projected_rows_by_bindings(
    rows: &mut Vec<Vec<serde_json::Value>>,
    states: &[ExpandState],
    return_clause: &ReturnClause,
) {
    if return_clause.order_by.is_empty() || rows.len() != states.len() {
        if !return_clause.order_by.is_empty() {
            let columns = build_columns(return_clause);
            sort_rows(rows, &columns, &return_clause.order_by);
        }
        return;
    }

    let columns = build_columns(return_clause);
    let mut paired: Vec<(Vec<serde_json::Value>, &ExpandState)> =
        rows.drain(..).zip(states.iter()).collect();
    paired.sort_by(|(row_a, a), (row_b, b)| {
        for order in &return_clause.order_by {
            let av = projected_order_value(&order.expr, &columns, row_a, &a.bindings);
            let bv = projected_order_value(&order.expr, &columns, row_b, &b.bindings);
            let cmp = pipeline_value_cmp(&av, &bv);
            if cmp != std::cmp::Ordering::Equal {
                return match order.direction {
                    SortDir::Asc => cmp,
                    SortDir::Desc => cmp.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
    });
    rows.extend(paired.into_iter().map(|(row, _)| row));
}

fn projected_order_value(
    expr: &Expr,
    columns: &[String],
    row: &[serde_json::Value],
    bindings: &HashMap<String, serde_json::Value>,
) -> serde_json::Value {
    let value = eval::eval_expr(expr, bindings).unwrap_or(serde_json::Value::Null);
    if !value.is_null() {
        return value;
    }
    let column_name = expr_to_column_name(expr);
    columns
        .iter()
        .position(|col| col == &column_name)
        .and_then(|idx| row.get(idx).cloned())
        .unwrap_or(serde_json::Value::Null)
}

fn pipeline_value_cmp(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    match (a, b) {
        (serde_json::Value::Number(an), serde_json::Value::Number(bn)) => an
            .as_f64()
            .partial_cmp(&bn.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (serde_json::Value::String(as_), serde_json::Value::String(bs)) => as_.cmp(bs),
        (serde_json::Value::Bool(ab), serde_json::Value::Bool(bb)) => ab.cmp(bb),
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

fn apply_with_pipeline(
    states: Vec<ExpandState>,
    with_pipeline: &crate::parser::WithPipeline,
) -> Result<Vec<ExpandState>> {
    let mut projected = Vec::with_capacity(states.len());
    for state in states {
        let mut bindings = HashMap::new();
        for item in &with_pipeline.clause.items {
            let value =
                eval::eval_expr(&item.expr, &state.bindings).unwrap_or(serde_json::Value::Null);
            let name = item
                .alias
                .clone()
                .unwrap_or_else(|| expr_to_column_name(&item.expr));
            bindings.insert(name, value);
        }
        if let Some(where_clause) = &with_pipeline.where_clause {
            if !eval::filter_passes(where_clause, &bindings)? {
                continue;
            }
        }
        projected.push(ExpandState {
            current_key: state.current_key,
            bindings,
        });
    }

    if with_pipeline.clause.distinct {
        let mut seen = std::collections::HashSet::new();
        projected.retain(|state| {
            let key = serde_json::to_string(&state.bindings).unwrap_or_default();
            seen.insert(key)
        });
    }

    if !with_pipeline.clause.order_by.is_empty() {
        projected.sort_by(|a, b| {
            for order in &with_pipeline.clause.order_by {
                let av =
                    eval::eval_expr(&order.expr, &a.bindings).unwrap_or(serde_json::Value::Null);
                let bv =
                    eval::eval_expr(&order.expr, &b.bindings).unwrap_or(serde_json::Value::Null);
                let cmp = pipeline_value_cmp(&av, &bv);
                if cmp != std::cmp::Ordering::Equal {
                    return match order.direction {
                        SortDir::Asc => cmp,
                        SortDir::Desc => cmp.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(limit) = with_pipeline.clause.limit {
        projected.truncate(limit.max(0) as usize);
    }
    Ok(projected)
}

#[allow(clippy::too_many_arguments)]
async fn execute_optional_hops(
    write_path: &WritePath,
    keyspace: &str,
    states: Vec<ExpandState>,
    optional_hops: &[Hop],
    config: &GraphEngineConfig,
    start: Instant,
    schema: Option<&Schema>,
    stats: &mut QueryStats,
) -> Result<Vec<ExpandState>> {
    let optional_vars: Vec<String> = optional_hops
        .iter()
        .flat_map(|hop| hop.var.iter().chain(hop.rel_var.iter()))
        .cloned()
        .collect();
    let mut current_states = states;
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table_id = TableId::new(&adj_ks, "adjacency");

    for hop in optional_hops {
        check_timeout(start, config.query_timeout)?;
        let mut next_states = Vec::new();
        for state in &current_states {
            let before_len = next_states.len();
            let vertex_key = &state.current_key;
            if matches!(hop.direction, Direction::In) {
                return Err(GraphError::Validation(
                    "incoming OPTIONAL MATCH requires a reverse adjacency index".into(),
                ));
            }
            if let Some(partition) = write_path.read(&adj_table_id, vertex_key).await? {
                stats.edges_read += partition.rows.len();
                let edge_meta = hop
                    .edge_table
                    .as_ref()
                    .and_then(|et| table_metadata_for(schema, &et.keyspace, &et.table));
                let vertex_meta = hop
                    .vertex_table
                    .as_ref()
                    .and_then(|vt| table_metadata_for(schema, &vt.keyspace, &vt.table));
                for row in &partition.rows {
                    if !adjacency_row_matches_direction(&row.clustering, hop.direction) {
                        continue;
                    }
                    let Some(neighbor_id) = extract_neighbor_id_for_direction(
                        &row.clustering,
                        hop.edge_label.as_deref(),
                        expected_adjacency_direction(hop.direction),
                    ) else {
                        continue;
                    };
                    let edge_match = if !hop.prop_filters.is_empty() || hop.rel_var.is_some() {
                        if let (Some(et), Some(meta)) =
                            (hop.edge_table.as_ref(), edge_meta.as_ref())
                        {
                            let edge_tid = TableId::new(&et.keyspace, &et.table);
                            find_edge_match(
                                write_path,
                                &edge_tid,
                                meta,
                                &state.bindings,
                                &hop.prop_filters,
                                vertex_key.key.as_bytes(),
                                &neighbor_id,
                                schema,
                            )
                            .await?
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if !hop.prop_filters.is_empty()
                        && !edge_row_passes_filters(edge_match.as_ref(), &hop.prop_filters)
                    {
                        continue;
                    }
                    let mut target_bindings = HashMap::new();
                    if let Some(var_name) = &hop.var {
                        if let Some(value) = state.bindings.get(var_name) {
                            target_bindings.insert(var_name.clone(), value.clone());
                        }
                    }
                    if let Some(edge_match) = edge_match.as_ref() {
                        target_bindings.insert("_edge".to_string(), edge_match.json.clone());
                    }
                    let neighbor_json = if let (Some(vertex_tid), Some(meta)) = (
                        hop.vertex_table
                            .as_ref()
                            .map(|vt| TableId::new(&vt.keyspace, &vt.table)),
                        vertex_meta.as_ref(),
                    ) {
                        find_vertex_match(
                            write_path,
                            &vertex_tid,
                            meta,
                            &target_bindings,
                            &neighbor_id,
                            hop.target_props
                                .iter()
                                .filter(|(name, _)| name != "__where__")
                                .cloned()
                                .collect::<Vec<_>>()
                                .as_slice(),
                            hop.var.as_deref().unwrap_or("_optional_hop"),
                            schema,
                        )
                        .await?
                        .map(|matched| matched.json)
                    } else {
                        None
                    };
                    if hop.vertex_table.is_some() && neighbor_json.is_none() {
                        continue;
                    }
                    let mut bindings = state.bindings.clone();
                    if let Some(var_name) = &hop.var {
                        bindings.insert(
                            var_name.clone(),
                            neighbor_json.unwrap_or_else(|| {
                                serde_json::Value::String(hex::encode(&neighbor_id))
                            }),
                        );
                    }
                    let neighbor_key =
                        DecoratedKey::new(ferrosa_common::PartitionKey::new(neighbor_id.clone()));
                    if let Some(rel_var) = &hop.rel_var {
                        let edge_json = edge_binding_json(
                            row,
                            edge_match.as_ref(),
                            hop.edge_label.as_deref(),
                            vertex_key,
                            &neighbor_id,
                        );
                        bindings.insert(rel_var.clone(), edge_json);
                    }
                    next_states.push(ExpandState {
                        current_key: neighbor_key,
                        bindings,
                    });
                }
            }
            if next_states.len() == before_len {
                let mut bindings = state.bindings.clone();
                for var in &optional_vars {
                    bindings
                        .entry(var.clone())
                        .or_insert(serde_json::Value::Null);
                }
                next_states.push(ExpandState {
                    current_key: state.current_key.clone(),
                    bindings,
                });
            }
            if next_states.len() > config.max_fan_out_per_hop {
                return Err(GraphError::ResourceLimit(format!(
                    "fan-out limit exceeded: {} neighbors (limit: {})",
                    next_states.len(),
                    config.max_fan_out_per_hop
                )));
            }
        }
        stats.vertices_read += next_states.len();
        current_states = next_states;
    }
    Ok(current_states)
}

#[allow(clippy::too_many_arguments)]
async fn try_edge_anchored_initial_states(
    write_path: &WritePath,
    anchor: &Anchor,
    hops: &[Hop],
    optional_hops: &[Hop],
    schema: Option<&Schema>,
    stats: &mut QueryStats,
) -> Result<Option<Vec<ExpandState>>> {
    let Some(hop) = hops.first() else {
        return Ok(None);
    };
    let Some(edge_table) = hop.edge_table.as_ref() else {
        return Ok(None);
    };
    let Some(edge_meta) = table_metadata_for(schema, &edge_table.keyspace, &edge_table.table)
    else {
        return Ok(None);
    };
    let Some(source_col) = edge_meta.extensions.get("graph.source") else {
        return Ok(None);
    };
    let Some(target_col) = edge_meta.extensions.get("graph.target") else {
        return Ok(None);
    };

    // Keep this fast path deliberately narrow: it replaces the pathological
    // unanchored vertex-table scan for `MATCH (a)-[r {prop}]->(b)` with a
    // single relationship-table scan filtered by edge properties. Anchored
    // node patterns and anchor WHERE filters still use the general executor.
    if !anchor.props.is_empty()
        || !anchor.filters.is_empty()
        || hop.prop_filters.is_empty()
        || matches!(hop.direction, Direction::Both)
        || !optional_hops.is_empty()
    {
        return Ok(None);
    }

    let edge_tid = TableId::new(&edge_table.keyspace, &edge_table.table);
    let strategy = graph_replication_strategy(schema, &edge_table.keyspace)?;
    let edge_partitions: Vec<Partition> = if let Some((key, _clustering)) =
        build_direct_lookup_shape(
            &edge_meta,
            &HashMap::new(),
            &hop.prop_filters,
            &HashMap::new(),
        )? {
        write_path
            .pk_read(&edge_tid, &key, ConsistencyLevel::One, &strategy)
            .await?
            .into_iter()
            .collect()
    } else {
        write_path.range_read(&edge_tid).await?
    };
    let source_vertex_tid = TableId::new(&anchor.table.keyspace, &anchor.table.table);
    let source_vertex_meta =
        table_metadata_for(schema, &anchor.table.keyspace, &anchor.table.table);
    let target_vertex = hop.vertex_table.as_ref();
    let target_vertex_tid = target_vertex.map(|vt| TableId::new(&vt.keyspace, &vt.table));
    let target_vertex_meta =
        target_vertex.and_then(|vt| table_metadata_for(schema, &vt.keyspace, &vt.table));

    let mut states = Vec::new();
    for partition in edge_partitions {
        if is_partition_dead(&partition) {
            continue;
        }
        stats.edges_read += partition.rows.len();
        for row in &partition.rows {
            let edge_json = row_to_json(&edge_meta, &partition, row);
            let edge_match = MatchedTableRow {
                key: partition.key.clone(),
                clustering: row.clustering.clone(),
                json: edge_json.clone(),
            };
            if !edge_row_passes_filters(Some(&edge_match), &hop.prop_filters) {
                continue;
            }

            let Some(raw_source_id) = extract_column_bytes_from_row(
                &edge_meta,
                partition.key.key.as_bytes(),
                row,
                source_col,
            ) else {
                continue;
            };
            let Some(raw_target_id) = extract_column_bytes_from_row(
                &edge_meta,
                partition.key.key.as_bytes(),
                row,
                target_col,
            ) else {
                continue;
            };
            let (source_id, target_id) = match hop.direction {
                Direction::Out => (raw_source_id, raw_target_id),
                Direction::In => (raw_target_id, raw_source_id),
                Direction::Both => unreachable!("Both is rejected above"),
            };

            let mut edge_bindings = HashMap::new();
            edge_bindings.insert("_edge".to_string(), edge_json.clone());

            let source_json = if let Some(meta) = source_vertex_meta.as_ref() {
                find_vertex_match(
                    write_path,
                    &source_vertex_tid,
                    meta,
                    &edge_bindings,
                    &source_id,
                    anchor.props.as_slice(),
                    anchor.var.as_deref().unwrap_or("_anchor"),
                    schema,
                )
                .await?
                .map(|matched| matched.json)
            } else {
                None
            };
            if source_vertex_meta.is_some() && source_json.is_none() {
                continue;
            }

            let target_json = if let (Some(vertex_tid), Some(meta)) =
                (target_vertex_tid.as_ref(), target_vertex_meta.as_ref())
            {
                find_vertex_match(
                    write_path,
                    vertex_tid,
                    meta,
                    &edge_bindings,
                    &target_id,
                    hop.target_props.as_slice(),
                    hop.var.as_deref().unwrap_or("_hop"),
                    schema,
                )
                .await?
                .map(|matched| matched.json)
            } else {
                None
            };
            if target_vertex_meta.is_some() && target_json.is_none() {
                continue;
            }

            let mut bindings = HashMap::new();
            if let Some(anchor_var) = &anchor.var {
                bindings.insert(
                    anchor_var.clone(),
                    source_json
                        .unwrap_or_else(|| serde_json::Value::String(hex::encode(&source_id))),
                );
            }
            if let Some(target_var) = &hop.var {
                bindings.insert(
                    target_var.clone(),
                    target_json
                        .unwrap_or_else(|| serde_json::Value::String(hex::encode(&target_id))),
                );
            }
            if let Some(rel_var) = &hop.rel_var {
                bindings.insert(
                    rel_var.clone(),
                    edge_binding_json(
                        row,
                        Some(&edge_match),
                        hop.edge_label.as_deref(),
                        &partition.key,
                        &target_id,
                    ),
                );
            }

            states.push(ExpandState {
                current_key: DecoratedKey::new(PartitionKey::new(target_id)),
                bindings,
            });
        }
    }

    Ok(Some(states))
}

/// Check whether an edge row passes all property filters.
///
/// Looks up the edge row in the given edge partition by matching the
fn edge_row_passes_filters(
    edge_match: Option<&MatchedTableRow>,
    prop_filters: &[(String, Expr)],
) -> bool {
    let Some(edge_match) = edge_match else {
        return false;
    };
    prop_map_passes("r", prop_filters, &edge_match.json).unwrap_or(false)
}

fn edge_binding_json(
    adjacency_row: &Row,
    edge_match: Option<&MatchedTableRow>,
    edge_label: Option<&str>,
    src_key: &DecoratedKey,
    neighbor_id: &[u8],
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "_id".to_string(),
        serde_json::Value::String(format!(
            "{}:{}:{}",
            hex::encode(src_key.key.as_bytes()),
            edge_label.unwrap_or_default(),
            hex::encode(neighbor_id)
        )),
    );
    map.insert(
        "_src".to_string(),
        serde_json::Value::String(hex::encode(src_key.key.as_bytes())),
    );
    map.insert(
        "_dst".to_string(),
        serde_json::Value::String(hex::encode(neighbor_id)),
    );
    if let Some(label) = edge_label {
        map.insert(
            "_type".to_string(),
            serde_json::Value::String(label.to_string()),
        );
    }

    if let Some(edge_match) = edge_match {
        map.insert(
            "__ferrosa_key".to_string(),
            serde_json::Value::String(hex::encode(edge_match.key.key.as_bytes())),
        );
        map.insert(
            "__ferrosa_clustering".to_string(),
            serde_json::Value::String(hex::encode(&edge_match.clustering)),
        );
        if let serde_json::Value::Object(obj) = &edge_match.json {
            map.extend(obj.clone());
        }
    } else {
        for (col_idx, cell) in &adjacency_row.cells {
            let value = match &cell.value {
                Some(bytes) => match std::str::from_utf8(bytes) {
                    Ok(s) => serde_json::Value::String(s.to_string()),
                    Err(_) => serde_json::Value::String(hex::encode(bytes)),
                },
                None => serde_json::Value::Null,
            };
            map.insert(format!("col_{col_idx}"), value);
        }
    }

    serde_json::Value::Object(map)
}

pub(super) fn table_metadata_for(
    schema: Option<&Schema>,
    keyspace: &str,
    table: &str,
) -> Option<ferrosa_schema::metadata::table::TableMetadata> {
    let schema = schema?;
    let snap = schema.snapshot();
    snap.tables
        .get(&(keyspace.to_string(), table.to_string()))
        .cloned()
}

pub(super) fn row_to_json(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    partition: &ferrosa_sstable::types::Partition,
    row: &Row,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "_id".to_string(),
        serde_json::Value::String(hex::encode(partition.key.key.as_bytes())),
    );

    if let Some(components) =
        decode_partition_components(partition.key.key.as_bytes(), meta.partition_key.len())
    {
        for (idx, name) in meta.partition_key.iter().enumerate() {
            if let (Some(bytes), Some(column)) = (components.get(idx), meta.columns.get(name)) {
                map.insert(
                    name.clone(),
                    decode_bytes_to_json(&column.column_type, bytes.as_slice()),
                );
            }
        }
    }

    if let Some(components) =
        decode_clustering_components(&row.clustering, meta.clustering_key.len())
    {
        for (idx, (name, _)) in meta.clustering_key.iter().enumerate() {
            if let (Some(bytes), Some(column)) = (components.get(idx), meta.columns.get(name)) {
                map.insert(
                    name.clone(),
                    decode_bytes_to_json(&column.column_type, bytes.as_slice()),
                );
            }
        }
    }

    for column in storage_cell_columns(meta) {
        match column.kind {
            ferrosa_schema::metadata::column::ColumnKind::Regular
            | ferrosa_schema::metadata::column::ColumnKind::Static => {
                let Some(cell_idx) = meta.storage_column_index(&column.name) else {
                    continue;
                };
                if let Some((_, cell)) = row.cells.iter().find(|(idx, _)| *idx == cell_idx) {
                    let value = match &cell.value {
                        Some(bytes) => decode_bytes_to_json(&column.column_type, bytes),
                        None => serde_json::Value::Null,
                    };
                    map.insert(column.name.clone(), value);
                }
            }
            _ => {}
        }
    }

    serde_json::Value::Object(map)
}

fn decode_partition_components(key: &[u8], count: usize) -> Option<Vec<Vec<u8>>> {
    if count == 0 {
        return Some(vec![]);
    }
    if count == 1 {
        return Some(vec![key.to_vec()]);
    }

    let mut offset = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > key.len() {
            return None;
        }
        let len = u16::from_be_bytes([key[offset], key[offset + 1]]) as usize;
        offset += 2;
        if offset + len > key.len() {
            return None;
        }
        out.push(key[offset..offset + len].to_vec());
        offset += len;
        if offset >= key.len() {
            return None;
        }
        offset += 1;
    }
    Some(out)
}

fn decode_clustering_components(clustering: &[u8], count: usize) -> Option<Vec<Vec<u8>>> {
    if count == 0 {
        return Some(vec![]);
    }
    if count == 1 {
        return Some(vec![clustering.to_vec()]);
    }

    let mut offset = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > clustering.len() {
            return None;
        }
        let len = u16::from_be_bytes([clustering[offset], clustering[offset + 1]]) as usize;
        offset += 2;
        if offset + len > clustering.len() {
            return None;
        }
        out.push(clustering[offset..offset + len].to_vec());
        offset += len;
    }
    Some(out)
}

fn decode_bytes_to_json(column_type: &str, bytes: &[u8]) -> serde_json::Value {
    match column_type {
        "text" | "varchar" | "ascii" => match std::str::from_utf8(bytes) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::Value::String(hex::encode(bytes)),
        },
        "uuid" => uuid::Uuid::from_slice(bytes)
            .map(|uuid| serde_json::Value::String(uuid.to_string()))
            .unwrap_or_else(|_| serde_json::Value::String(hex::encode(bytes))),
        "int" if bytes.len() == 4 => {
            serde_json::Value::Number(i32::from_be_bytes(bytes.try_into().unwrap()).into())
        }
        "bigint" | "counter" | "timestamp" if bytes.len() == 8 => {
            let raw = i64::from_be_bytes(bytes.try_into().unwrap());
            if column_type == "timestamp" {
                let value = chrono::DateTime::from_timestamp_millis(raw)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| raw.to_string());
                serde_json::Value::String(value)
            } else {
                serde_json::Value::Number(raw.into())
            }
        }
        "float" if bytes.len() == 4 => {
            serde_json::Number::from_f64(f32::from_be_bytes(bytes.try_into().unwrap()) as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        "double" if bytes.len() == 8 => {
            serde_json::Number::from_f64(f64::from_be_bytes(bytes.try_into().unwrap()))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        "boolean" if bytes.len() == 1 => serde_json::Value::Bool(bytes[0] != 0),
        _ => match std::str::from_utf8(bytes) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::Value::String(hex::encode(bytes)),
        },
    }
}

fn prop_map_passes(
    var_name: &str,
    props: &[(String, Expr)],
    row_json: &serde_json::Value,
) -> Result<bool> {
    let mut bindings = HashMap::new();
    bindings.insert(var_name.to_string(), row_json.clone());
    for (name, expr) in props {
        let filter = Expr::Comparison {
            left: Box::new(Expr::Property {
                var: var_name.to_string(),
                name: name.clone(),
            }),
            op: CompareOp::Eq,
            right: Box::new(expr.clone()),
        };
        if !eval::filter_passes(&filter, &bindings)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn graph_vertex_lookup_key(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    partition: &ferrosa_sstable::types::Partition,
    row: &Row,
    props: &[(String, Expr)],
) -> Option<DecoratedKey> {
    if meta.partition_key.len() == 1 && meta.clustering_key.is_empty() {
        return Some(partition.key.clone());
    }

    if meta.clustering_key.len() == 1 {
        let bytes = decode_clustering_components(&row.clustering, 1)?
            .into_iter()
            .next()?;
        return Some(DecoratedKey::new(PartitionKey::new(bytes)));
    }

    let preferred_prop = props
        .iter()
        .find(|(name, _)| name == "id" || name.ends_with("_id"))
        .or_else(|| props.first())?;
    let column_type = meta.columns.get(&preferred_prop.0)?.column_type.as_str();
    let bytes = encode_expr_for_column_type(&preferred_prop.1, column_type).ok()?;
    Some(DecoratedKey::new(PartitionKey::new(bytes)))
}

pub(super) fn extract_column_bytes_from_row(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    partition_key: &[u8],
    row: &Row,
    column_name: &str,
) -> Option<Vec<u8>> {
    let column = meta.columns.get(column_name)?;
    match column.kind {
        ferrosa_schema::metadata::column::ColumnKind::PartitionKey => {
            let idx = meta
                .partition_key
                .iter()
                .position(|name| name == column_name)?;
            let components = decode_partition_components(partition_key, meta.partition_key.len())?;
            components.get(idx).cloned()
        }
        ferrosa_schema::metadata::column::ColumnKind::Clustering => {
            let idx = meta
                .clustering_key
                .iter()
                .position(|(name, _)| name == column_name)?;
            let components =
                decode_clustering_components(&row.clustering, meta.clustering_key.len())?;
            components.get(idx).cloned()
        }
        ferrosa_schema::metadata::column::ColumnKind::Regular
        | ferrosa_schema::metadata::column::ColumnKind::Static => {
            let cell_idx = meta.storage_column_index(column_name)?;
            row.cells
                .iter()
                .find(|(idx, _)| *idx == cell_idx)
                .and_then(|(_, cell)| cell.value.clone())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn find_edge_match(
    write_path: &WritePath,
    table_id: &TableId,
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    bindings: &HashMap<String, serde_json::Value>,
    prop_filters: &[(String, Expr)],
    source_id: &[u8],
    target_id: &[u8],
    schema: Option<&Schema>,
) -> Result<Option<MatchedTableRow>> {
    let Some(source_col) = meta.extensions.get("graph.source") else {
        return Ok(None);
    };
    let Some(target_col) = meta.extensions.get("graph.target") else {
        return Ok(None);
    };

    let mut direct_components = HashMap::new();
    direct_components.insert(source_col.clone(), source_id.to_vec());
    direct_components.insert(target_col.clone(), target_id.to_vec());

    if let Some((key, clustering)) =
        build_direct_lookup_shape(meta, bindings, prop_filters, &direct_components)?
    {
        let strategy = graph_replication_strategy(schema, &table_id.keyspace)?;
        if let Some(partition) = write_path
            .pk_read(table_id, &key, ConsistencyLevel::One, &strategy)
            .await?
        {
            if let Some(row) = partition
                .rows
                .iter()
                .find(|row| row.clustering == clustering)
            {
                return Ok(Some(MatchedTableRow {
                    key: partition.key.clone(),
                    clustering: row.clustering.clone(),
                    json: row_to_json(meta, &partition, row),
                }));
            }
        }
        return Ok(None);
    }

    for partition in write_path.range_read(table_id).await? {
        for row in &partition.rows {
            let row_source =
                extract_column_bytes_from_row(meta, partition.key.key.as_bytes(), row, source_col);
            let row_target =
                extract_column_bytes_from_row(meta, partition.key.key.as_bytes(), row, target_col);
            if row_source.as_deref() == Some(source_id) && row_target.as_deref() == Some(target_id)
            {
                return Ok(Some(MatchedTableRow {
                    key: partition.key.clone(),
                    clustering: row.clustering.clone(),
                    json: row_to_json(meta, &partition, row),
                }));
            }
        }
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
async fn find_vertex_match(
    write_path: &WritePath,
    table_id: &TableId,
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    bindings: &HashMap<String, serde_json::Value>,
    neighbor_id: &[u8],
    target_props: &[(String, Expr)],
    var_name: &str,
    schema: Option<&Schema>,
) -> Result<Option<MatchedTableRow>> {
    if let Some((key, clustering)) =
        build_neighbor_vertex_lookup_shape(meta, bindings, target_props, neighbor_id)?
    {
        let strategy = graph_replication_strategy(schema, &table_id.keyspace)?;
        if let Some(partition) = write_path
            .pk_read(table_id, &key, ConsistencyLevel::One, &strategy)
            .await?
        {
            if let Some(row) = partition
                .rows
                .iter()
                .find(|row| row.clustering == clustering)
            {
                let row_json = row_to_json(meta, &partition, row);
                if prop_map_passes(var_name, target_props, &row_json)?
                    && graph_vertex_lookup_key(meta, &partition, row, target_props)
                        .is_some_and(|vertex_key| vertex_key.key.as_bytes() == neighbor_id)
                {
                    return Ok(Some(MatchedTableRow {
                        key: partition.key.clone(),
                        clustering: row.clustering.clone(),
                        json: row_json,
                    }));
                }
            }
        }
    }

    for partition in write_path.range_read(table_id).await? {
        if is_partition_dead(&partition) {
            continue;
        }
        for row in &partition.rows {
            let row_json = row_to_json(meta, &partition, row);
            if !prop_map_passes(var_name, target_props, &row_json)? {
                continue;
            }
            let Some(vertex_key) = graph_vertex_lookup_key(meta, &partition, row, target_props)
            else {
                continue;
            };
            if vertex_key.key.as_bytes() == neighbor_id {
                return Ok(Some(MatchedTableRow {
                    key: partition.key.clone(),
                    clustering: row.clustering.clone(),
                    json: row_json,
                }));
            }
        }
    }

    Ok(None)
}

fn build_neighbor_vertex_lookup_shape(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    bindings: &HashMap<String, serde_json::Value>,
    target_props: &[(String, Expr)],
    neighbor_id: &[u8],
) -> Result<Option<(DecoratedKey, Vec<u8>)>> {
    let mut direct_components = HashMap::new();
    if let Some((column_name, _)) = meta
        .clustering_key
        .iter()
        .find(|(name, _)| name == "id" || name.ends_with("_id") || name.ends_with("_uuid"))
    {
        direct_components.insert(column_name.clone(), neighbor_id.to_vec());
    }

    build_direct_lookup_shape(meta, bindings, target_props, &direct_components)
}

fn build_direct_lookup_shape(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    bindings: &HashMap<String, serde_json::Value>,
    props: &[(String, Expr)],
    direct_components: &HashMap<String, Vec<u8>>,
) -> Result<Option<(DecoratedKey, Vec<u8>)>> {
    let mut partition_components = Vec::with_capacity(meta.partition_key.len());
    for column_name in &meta.partition_key {
        let Some(column) = meta.columns.get(column_name) else {
            return Ok(None);
        };
        let Some(bytes) = resolve_lookup_component(column, bindings, props, direct_components)?
        else {
            return Ok(None);
        };
        partition_components.push(bytes);
    }

    let mut clustering_components = Vec::with_capacity(meta.clustering_key.len());
    for (column_name, _) in &meta.clustering_key {
        let Some(column) = meta.columns.get(column_name) else {
            return Ok(None);
        };
        let Some(bytes) = resolve_lookup_component(column, bindings, props, direct_components)?
        else {
            return Ok(None);
        };
        clustering_components.push(bytes);
    }

    Ok(Some((
        DecoratedKey::new(PartitionKey::new(encode_partition_components(
            &partition_components,
        ))),
        encode_clustering_components(&clustering_components),
    )))
}

fn resolve_lookup_component(
    column: &ferrosa_schema::metadata::column::ColumnMetadata,
    bindings: &HashMap<String, serde_json::Value>,
    props: &[(String, Expr)],
    direct_components: &HashMap<String, Vec<u8>>,
) -> Result<Option<Vec<u8>>> {
    if let Some(bytes) = direct_components.get(&column.name) {
        return Ok(Some(bytes.clone()));
    }

    if let Some((_, expr)) = props.iter().find(|(name, _)| name == &column.name) {
        return encode_expr_for_column_type(expr, &column.column_type).map(Some);
    }

    for value in bindings.values() {
        let serde_json::Value::Object(map) = value else {
            continue;
        };
        let Some(field) = map.get(&column.name) else {
            continue;
        };
        if let Some(expr) = json_value_to_expr(field) {
            return encode_expr_for_column_type(&expr, &column.column_type).map(Some);
        }
    }

    Ok(None)
}

async fn find_table_row_by_props(
    write_path: &WritePath,
    table_id: &TableId,
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    props: &[(String, Expr)],
    var_name: &str,
) -> Result<Option<MatchedTableRow>> {
    if let Some((key, clustering)) =
        build_direct_lookup_shape(meta, &HashMap::new(), props, &HashMap::new())?
    {
        if let Some(partition) = write_path.read(table_id, &key).await? {
            if let Some(row) = partition
                .rows
                .iter()
                .find(|row| row.clustering == clustering)
            {
                let row_json = row_to_json(meta, &partition, row);
                if prop_map_passes(var_name, props, &row_json)? {
                    return Ok(Some(MatchedTableRow {
                        key: partition.key.clone(),
                        clustering: row.clustering.clone(),
                        json: row_json,
                    }));
                }
            }
        }

        // A complete primary-key shape is terminal: if that exact row is absent
        // or fails the property predicates, scanning the whole graph table cannot
        // produce the same identity and can overload live multi-tenant tables.
        return Ok(None);
    }

    for partition in write_path.range_read(table_id).await? {
        if is_partition_dead(&partition) {
            continue;
        }
        for row in &partition.rows {
            let row_json = row_to_json(meta, &partition, row);
            if !prop_map_passes(var_name, props, &row_json)? {
                continue;
            }
            return Ok(Some(MatchedTableRow {
                key: partition.key.clone(),
                clustering: row.clustering.clone(),
                json: row_json,
            }));
        }
    }
    Ok(None)
}

fn json_value_to_expr(value: &serde_json::Value) -> Option<Expr> {
    match value {
        serde_json::Value::String(s) => Some(Expr::Literal(Literal::String(s.clone()))),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|i| Expr::Literal(Literal::Integer(i)))
            .or_else(|| n.as_f64().map(|f| Expr::Literal(Literal::Float(f)))),
        serde_json::Value::Bool(b) => Some(Expr::Literal(Literal::Bool(*b))),
        serde_json::Value::Null => Some(Expr::Literal(Literal::Null)),
        _ => None,
    }
}

fn resolve_table_by_graph_label(
    schema: &Schema,
    keyspace: &str,
    label: &str,
) -> Option<ferrosa_schema::metadata::table::TableMetadata> {
    let snap = schema.snapshot();
    snap.tables.values().find_map(|meta| {
        (meta.keyspace == keyspace
            && meta
                .extensions
                .get("graph.label")
                .is_some_and(|graph_label| graph_label == label))
        .then(|| meta.clone())
    })
}

/// Label-agnostic expansion (t_8c506227): the MATCH pattern gave no edge label,
/// so recover the edge table this adjacency row came from. Each adjacency row
/// records its originating edge table in regular column 0 (`keyspace.table`).
/// Fails loud when that column is missing or names a table absent from the
/// schema rather than silently dropping the relationship's identity.
fn resolve_edge_from_adjacency_row(
    schema: Option<&Schema>,
    adjacency_row: &Row,
) -> Result<(
    crate::planner::ResolvedTable,
    ferrosa_schema::metadata::table::TableMetadata,
)> {
    let schema = schema.ok_or_else(|| {
        GraphError::Validation(
            "label-agnostic relationship expansion requires schema metadata".into(),
        )
    })?;
    let edge_table_str = adjacency_row
        .cells
        .iter()
        .find(|(idx, _)| *idx == 0)
        .and_then(|(_, cell)| cell.value.as_ref())
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or_else(|| {
            GraphError::Validation(
                "adjacency row is missing its edge_table column; cannot hydrate the \
                 relationship of a label-agnostic expansion"
                    .into(),
            )
        })?;
    let (edge_ks, edge_tbl) = edge_table_str.split_once('.').ok_or_else(|| {
        GraphError::Validation(format!(
            "adjacency edge_table '{edge_table_str}' is not in 'keyspace.table' form"
        ))
    })?;
    let edge_meta = table_metadata_for(Some(schema), edge_ks, edge_tbl).ok_or_else(|| {
        GraphError::Validation(format!(
            "edge table '{edge_table_str}' recorded in adjacency is not registered in the schema"
        ))
    })?;
    let edge_rt = crate::planner::ResolvedTable {
        keyspace: edge_ks.to_string(),
        table: edge_tbl.to_string(),
        graph_type: "edge".to_string(),
        label: edge_meta
            .extensions
            .get("graph.label")
            .cloned()
            .unwrap_or_default(),
    };
    Ok((edge_rt, edge_meta))
}

/// Resolve the OPPOSITE vertex table of an edge for a hop whose target node was
/// unlabeled (t_8c506227). The opposite endpoint is the edge's target vertex
/// for an outgoing traversal and its source vertex for an incoming one, named
/// by the edge's `graph.target_label` / `graph.source_label`. Fails loud when
/// the required endpoint label is absent or resolves to no vertex table, so a
/// mis-declared edge surfaces as a clear error rather than a null endpoint
/// (acceptance #4).
fn resolve_opposite_vertex(
    schema: Option<&Schema>,
    edge_meta: &ferrosa_schema::metadata::table::TableMetadata,
    neighbor_direction: u8,
) -> Result<(
    crate::planner::ResolvedTable,
    ferrosa_schema::metadata::table::TableMetadata,
)> {
    let schema = schema.ok_or_else(|| {
        GraphError::Validation(
            "label-agnostic relationship expansion requires schema metadata".into(),
        )
    })?;
    let edge_name = format!("{}.{}", edge_meta.keyspace, edge_meta.name);
    let (label_key, dir_word) = if neighbor_direction == DIRECTION_IN {
        ("graph.source_label", "incoming")
    } else {
        ("graph.target_label", "outgoing")
    };
    let vertex_label = edge_meta
        .extensions
        .get(label_key)
        .cloned()
        .ok_or_else(|| {
            GraphError::Validation(format!(
                "edge table '{edge_name}' lacks the '{label_key}' extension needed to hydrate the \
             {dir_word} endpoint of an unlabeled traversal; declare the edge's source/target \
             vertex labels"
            ))
        })?;
    let vertex_meta = resolve_table_by_graph_label(schema, &edge_meta.keyspace, &vertex_label)
        .ok_or_else(|| {
            GraphError::Validation(format!(
                "edge table '{edge_name}' declares {label_key}='{vertex_label}', but no vertex \
                 table with graph.label '{vertex_label}' exists in keyspace '{}'",
                edge_meta.keyspace
            ))
        })?;
    let vertex_rt = crate::planner::ResolvedTable {
        keyspace: vertex_meta.keyspace.clone(),
        table: vertex_meta.name.clone(),
        graph_type: "vertex".to_string(),
        label: vertex_label,
    };
    Ok((vertex_rt, vertex_meta))
}

/// Hydrate one hop neighbor: resolve the effective edge/vertex tables
/// (t_8c506227), match + filter the edge, hydrate the opposite vertex, and
/// build the resulting bindings. Returns `Ok(None)` when the candidate is
/// filtered out (edge property filter, or an unmatched target-node property).
/// All inputs are shared/immutable, so many neighbors can be hydrated
/// concurrently within a hop (t_0cc8d63e).
#[allow(clippy::too_many_arguments)]
async fn hydrate_hop_neighbor(
    write_path: &WritePath,
    schema: Option<&Schema>,
    hop: &Hop,
    state: &ExpandState,
    vertex_key: &DecoratedKey,
    edge_meta: Option<&ferrosa_schema::metadata::table::TableMetadata>,
    vertex_meta: Option<&ferrosa_schema::metadata::table::TableMetadata>,
    row: &Row,
    neighbor_id: Vec<u8>,
) -> Result<Option<ExpandState>> {
    let neighbor_direction = match hop.direction {
        Direction::In => DIRECTION_IN,
        Direction::Out => DIRECTION_OUT,
        Direction::Both => row.clustering.get(2).copied().unwrap_or(DIRECTION_OUT),
    };
    let need_edge =
        hop.rel_var.is_some() || !hop.prop_filters.is_empty() || hop.vertex_table.is_none();
    let resolved_edge = if hop.edge_table.is_none() && need_edge {
        Some(resolve_edge_from_adjacency_row(schema, row)?)
    } else {
        None
    };
    let eff_edge_table = hop
        .edge_table
        .as_ref()
        .or(resolved_edge.as_ref().map(|e| &e.0));
    let eff_edge_meta = edge_meta.or(resolved_edge.as_ref().map(|e| &e.1));
    let eff_edge_label = hop
        .edge_label
        .as_deref()
        .or(eff_edge_table.map(|e| e.label.as_str()));
    let resolved_vertex = if hop.vertex_table.is_none() {
        match eff_edge_meta {
            Some(em) => Some(resolve_opposite_vertex(schema, em, neighbor_direction)?),
            None => None,
        }
    } else {
        None
    };
    let eff_vertex_table = hop
        .vertex_table
        .as_ref()
        .or(resolved_vertex.as_ref().map(|v| &v.0));
    let eff_vertex_meta = vertex_meta.or(resolved_vertex.as_ref().map(|v| &v.1));

    let edge_match = if !hop.prop_filters.is_empty() || hop.rel_var.is_some() {
        if let (Some(et), Some(meta)) = (eff_edge_table, eff_edge_meta) {
            let edge_tid = TableId::new(&et.keyspace, &et.table);
            find_edge_match(
                write_path,
                &edge_tid,
                meta,
                &state.bindings,
                &hop.prop_filters,
                vertex_key.key.as_bytes(),
                &neighbor_id,
                schema,
            )
            .await?
        } else {
            None
        }
    } else {
        None
    };

    if !hop.prop_filters.is_empty()
        && !edge_row_passes_filters(edge_match.as_ref(), &hop.prop_filters)
    {
        return Ok(None);
    }

    let mut bindings = state.bindings.clone();
    let neighbor_key = DecoratedKey::new(ferrosa_common::PartitionKey::new(neighbor_id.clone()));

    let mut target_bindings = HashMap::new();
    if let Some(var_name) = &hop.var {
        if let Some(value) = state.bindings.get(var_name) {
            target_bindings.insert(var_name.clone(), value.clone());
        }
    }
    if let Some(edge_match) = edge_match.as_ref() {
        target_bindings.insert("_edge".to_string(), edge_match.json.clone());
    }

    let neighbor_json = if let (Some(vertex_tid), Some(meta)) = (
        eff_vertex_table.map(|vt| TableId::new(&vt.keyspace, &vt.table)),
        eff_vertex_meta,
    ) {
        find_vertex_match(
            write_path,
            &vertex_tid,
            meta,
            &target_bindings,
            &neighbor_id,
            hop.target_props.as_slice(),
            hop.var.as_deref().unwrap_or("_hop"),
            schema,
        )
        .await?
        .map(|matched| matched.json)
    } else {
        None
    };

    if !hop.target_props.is_empty() && neighbor_json.is_none() {
        return Ok(None);
    }

    if let Some(var_name) = &hop.var {
        bindings.insert(
            var_name.clone(),
            neighbor_json.unwrap_or_else(|| serde_json::Value::String(hex::encode(&neighbor_id))),
        );
    }

    if let Some(rel_var) = &hop.rel_var {
        let edge_json = edge_binding_json(
            row,
            edge_match.as_ref(),
            eff_edge_label,
            vertex_key,
            &neighbor_id,
        );
        bindings.insert(rel_var.clone(), edge_json);
    }

    Ok(Some(ExpandState {
        current_key: neighbor_key,
        bindings,
    }))
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

fn expr_to_column_bytes(
    expr: &Expr,
    column_type: &str,
) -> std::result::Result<Vec<u8>, GraphError> {
    let Expr::Literal(lit) = expr else {
        return Err(GraphError::Validation(format!(
            "cannot convert expression to bytes: {expr:?}"
        )));
    };
    let ty = column_type.to_ascii_lowercase();
    match (lit, ty.as_str()) {
        (Literal::String(s), _) => Ok(s.as_bytes().to_vec()),
        (Literal::Integer(i), "int" | "integer") => Ok((*i as i32).to_be_bytes().to_vec()),
        (Literal::Integer(i), _) => Ok(i.to_be_bytes().to_vec()),
        (Literal::Float(f), _) => Ok(f.to_be_bytes().to_vec()),
        (Literal::Bool(b), _) => Ok(vec![if *b { 1 } else { 0 }]),
        (Literal::Null, _) => Ok(vec![]),
    }
}

fn create_row_from_schema(
    op: &CreateOp,
    meta: Option<&ferrosa_schema::metadata::table::TableMetadata>,
    timestamp: i64,
) -> Result<(DecoratedKey, Row, String)> {
    let Some(meta) = meta else {
        let key_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        let hex_key = hex::encode(&key_bytes);
        let cells: Vec<(u16, CellValue)> = op
            .props
            .iter()
            .enumerate()
            .map(|(idx, (_name, expr))| {
                let bytes = expr_to_bytes(expr).unwrap_or_default();
                (idx as u16, CellValue::live(bytes, timestamp))
            })
            .collect();
        return Ok((
            DecoratedKey::new(PartitionKey::new(key_bytes)),
            Row {
                clustering: vec![],
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            },
            hex_key,
        ));
    };

    let prop = |name: &str| {
        op.props
            .iter()
            .find(|(prop_name, _)| prop_name == name)
            .map(|(_, expr)| expr)
    };

    let mut key_components = Vec::new();
    for pk_name in &meta.partition_key {
        let column = meta.columns.get(pk_name).ok_or_else(|| {
            GraphError::Validation(format!(
                "table {}.{} partition key column {pk_name} missing from metadata",
                meta.keyspace, meta.name
            ))
        })?;
        let key_bytes = if let Some(expr) = prop(pk_name) {
            expr_to_column_bytes(expr, &column.column_type)?
        } else {
            // Neo4j-style CREATE does not require applications to provide an
            // id property. For schema-backed Ferrosa vertex tables, synthesize
            // a primary key using the declared CQL type so the row can still be
            // materialized without treating the key as a regular property.
            match column.column_type.as_str() {
                "text" | "varchar" | "ascii" => uuid::Uuid::new_v4().to_string().into_bytes(),
                "uuid" => uuid::Uuid::new_v4().as_bytes().to_vec(),
                other => {
                    return Err(GraphError::Validation(format!(
                        "CREATE for label {} requires partition key property {pk_name} for unsupported generated key type {other}",
                        op.table.label
                    )))
                }
            }
        };
        key_components.push(key_bytes);
    }
    let key_bytes = if key_components.len() == 1 {
        key_components.remove(0)
    } else {
        encode_components(&key_components)
    };
    let hex_key = hex::encode(&key_bytes);

    let mut clustering_components = Vec::new();
    for (ck_name, _) in &meta.clustering_key {
        let column = meta.columns.get(ck_name).ok_or_else(|| {
            GraphError::Validation(format!(
                "table {}.{} clustering key column {ck_name} missing from metadata",
                meta.keyspace, meta.name
            ))
        })?;
        let expr = prop(ck_name).ok_or_else(|| {
            GraphError::Validation(format!(
                "CREATE for label {} requires clustering key property {ck_name}",
                op.table.label
            ))
        })?;
        clustering_components.push(expr_to_column_bytes(expr, &column.column_type)?);
    }
    let clustering = if clustering_components.is_empty() {
        vec![]
    } else if clustering_components.len() == 1 {
        clustering_components.remove(0)
    } else {
        encode_components(&clustering_components)
    };

    let property_exprs = op
        .props
        .iter()
        .map(|(name, expr)| (name.clone(), expr.clone()))
        .collect();
    let cells = encode_storage_ordered_cells(meta, &property_exprs, timestamp)?;

    Ok((
        DecoratedKey::new(PartitionKey::new(key_bytes)),
        Row {
            clustering,
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        },
        hex_key,
    ))
}

fn encode_components(components: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for component in components {
        out.extend_from_slice(&(component.len() as u16).to_be_bytes());
        out.extend_from_slice(component);
    }
    out
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
async fn execute_create(
    write_path: &WritePath,
    creates: &[CreateOp],
    return_clause: Option<&crate::parser::ReturnClause>,
    _config: &GraphEngineConfig,
    start: Instant,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();
    let timestamp = now_micros();

    // Accumulate (var_name, hex_key) for each written node so that a RETURN
    // clause can project the created vertex IDs back to the caller.
    let mut var_keys: Vec<(Option<String>, String)> = Vec::with_capacity(creates.len());

    for op in creates {
        let table_id = TableId::new(&op.table.keyspace, &op.table.table);
        let strategy = graph_replication_strategy(schema, &op.table.keyspace)?;

        let meta = schema.and_then(|schema| {
            table_metadata_for(Some(schema), &op.table.keyspace, &op.table.table)
        });
        let (key, row, hex_key) = create_row_from_schema(op, meta.as_ref(), timestamp)?;

        write_path
            .write(
                &table_id,
                &key,
                row.clone(),
                timestamp,
                graph_write_consistency(),
                &strategy,
            )
            .await?;
        write_explicit_adjacency_entries(write_path, &table_id, &key, &row, timestamp, schema)
            .await?;
        stats.vertices_written += 1;
        var_keys.push((op.var.clone(), hex_key));
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    // If a RETURN clause was provided, project the created node IDs.
    if let Some(rc) = return_clause {
        let columns = build_columns(rc);
        // Build a single row: for each RETURN item, look up the hex key
        // for the matching variable, or emit null if unresolved.
        let row: Vec<serde_json::Value> = rc
            .items
            .iter()
            .map(|item| {
                if let crate::parser::Expr::Var(var_name) = &item.expr {
                    var_keys
                        .iter()
                        .find(|(v, _)| v.as_deref() == Some(var_name.as_str()))
                        .map(|(_, hex)| serde_json::Value::String(hex.clone()))
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            })
            .collect();
        return Ok(GraphResult {
            columns,
            rows: vec![row],
            stats,
        });
    }

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "created {} vertices",
            stats.vertices_written
        ))]],
        stats,
    })
}

/// Derive a deterministic content-addressed partition key from match-property bytes.
///
/// Properties are sorted by key name to ensure two callers with the same logical
/// match properties always arrive at the same bytes regardless of insertion order.
/// This is the R1 mitigation: concurrent MERGE calls with identical match-props
/// will hash to the same partition key, so at-most-once write semantics hold
/// even without row-level locks (last-write-wins on the same key is idempotent).
///
/// Key layout: `blake3(key_name_0 || NUL || value_bytes_0 || NUL || key_name_1 || ...)`
/// sorted by `key_name`.
fn content_addressed_key(match_props: &[(String, Expr)]) -> Vec<u8> {
    let mut sorted: Vec<&(String, Expr)> = match_props.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = blake3::Hasher::new();
    for (name, expr) in &sorted {
        hasher.update(name.as_bytes());
        hasher.update(b"\x00");
        let val_bytes = expr_to_bytes(expr).unwrap_or_default();
        hasher.update(&val_bytes);
        hasher.update(b"\x00");
    }
    hasher.finalize().as_bytes().to_vec()
}

fn graph_write_consistency() -> ConsistencyLevel {
    ConsistencyLevel::Quorum
}

/// Execute a MERGE plan: match-or-create with content-addressed deterministic key.
///
/// For each MergeOp (in declaration order, preserving R2 binding dependency):
///   1. Derive the partition key via `content_addressed_key(match_props)`.
///   2. Attempt `write_path.read()` to check whether the row already exists.
///   3. If absent: call `write_path.write()` (same path as `execute_create`) so
///      that `AdjacencyIndexObserver` fires on the new row (R3 mitigation).
///   4. If present: skip create; proceed to SET phase.
///
/// After all merges, apply any trailing SET assignments.
async fn execute_merge(
    write_path: &WritePath,
    merges: &[MergeOp],
    set_clause: &[(String, String, Expr)],
    _return_clause: Option<&crate::parser::ReturnClause>,
    _config: &GraphEngineConfig,
    start: Instant,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    let mut stats = QueryStats::default();
    let timestamp = now_micros();
    let strategy = graph_replication_strategy(
        schema,
        merges
            .first()
            .map(|op| op.table.keyspace.as_str())
            .unwrap_or_default(),
    )?;

    // var_name -> (table_id, key, clustering) so that SET can update the same row
    // that MERGE created or matched, including edge rows.
    let mut var_keys: HashMap<String, (TableId, DecoratedKey, Vec<u8>, bool)> = HashMap::new();
    let mut var_rows: HashMap<String, serde_json::Value> = HashMap::new();

    for op in merges {
        // --- Precondition: table must be identified ---
        let table_id = TableId::new(&op.table.keyspace, &op.table.table);
        let op_set_props: Vec<(String, Expr)> = op
            .var
            .as_ref()
            .map(|var_name| {
                set_clause
                    .iter()
                    .filter(|(var, _, _)| var == var_name)
                    .map(|(_, property, expr)| (property.clone(), expr.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let mut shape = build_merge_write_shape(op, &op_set_props, schema, timestamp)?;
        if !shape.used_schema_layout {
            if let Some(schema_ref) = schema {
                if let Some(inferred) = infer_schema_aware_merge_shape(
                    write_path,
                    op,
                    &op_set_props,
                    schema_ref,
                    timestamp,
                    &var_rows,
                )
                .await?
                {
                    shape = inferred;
                } else if schema_merge_requires_hidden_key_resolution(op, &op_set_props, schema_ref)
                {
                    return Err(GraphError::Validation(format!(
                        "MERGE on '{}.{}' is missing required scoped key columns; match existing scoped vertices or set the missing key properties explicitly",
                        op.table.keyspace, op.table.table
                    )));
                }
            }
        }
        let key = shape.key.clone();
        let clustering = shape.clustering.clone();

        let skip_partition_read = merge_can_bind_schema_key_only_vertex_without_read(op, &shape);

        // Step 2: read-before-write — check for existing row unless this is a
        // schema-aware key-only vertex MERGE. Scoped graph vertex tables can have
        // very large partitions (for example agent_memory.entity_store is keyed
        // by (tenant_id, session_id) and clustered by entity_id); materializing
        // the whole partition just to bind one primary-key row can OOM the graph
        // service. A key-only MERGE has all storage-key bytes already in `shape`
        // and no regular cells to preserve from an existing row, so it can bind
        // from the deterministic key shape and write an idempotent liveness row
        // without a partition read.
        //
        // Treat `skip_partition_read` as "no read, but still WRITE the liveness
        // row idempotently". The previous code set `row_exists = true` here,
        // which skipped the create arm entirely — leaving the entity_store
        // partition with no row at all and breaking every subsequent
        // `MATCH (a:Entity {entity_id})` (smoke "graph readback: KNOWN ISSUE
        // rows=[]"). Idempotent LWW means re-writing the same key with the same
        // empty-cells liveness row at a fresh timestamp is safe — it's
        // exactly the same shape we'd write on the first MERGE — and the read
        // path then finds the row via the normal anchor scan + clustering
        // match.
        let existing = if skip_partition_read {
            None
        } else {
            write_path.read(&table_id, &key).await.map_err(|e| {
                tracing::error!(
                    file = file!(),
                    line = line!(),
                    error = %e,
                    table = %op.table.table,
                    "execute_merge: read failed"
                );
                e
            })?
        };
        let row_exists = if skip_partition_read {
            false
        } else {
            existing.as_ref().is_some_and(|partition| {
                partition
                    .rows
                    .iter()
                    .any(|row| row.clustering == clustering)
            })
        };
        let existing_row_json = schema.and_then(|schema_ref| {
            let meta = table_metadata_for(Some(schema_ref), &op.table.keyspace, &op.table.table)?;
            let partition = existing.as_ref()?;
            let row = partition
                .rows
                .iter()
                .find(|row| row.clustering == clustering)?;
            Some(row_to_json(&meta, partition, row))
        });

        if !row_exists {
            // Step 3: create arm — use write_path.write() to fire adjacency observer (R3).
            let row = Row {
                clustering: clustering.clone(),
                cells: shape.create_cells.clone(),
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            };

            write_path
                .write(
                    &table_id,
                    &key,
                    row.clone(),
                    timestamp,
                    graph_write_consistency(),
                    &strategy,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        file = file!(),
                        line = line!(),
                        error = %e,
                        table = %op.table.table,
                        "execute_merge: write failed on create arm"
                    );
                    e
                })?;
            write_explicit_adjacency_entries(write_path, &table_id, &key, &row, timestamp, schema)
                .await?;
            stats.vertices_written += 1;
        }
        // Existing row: no create needed, count as read.
        stats.vertices_read += 1;

        if let Some(var_name) = &op.var {
            var_keys.insert(
                var_name.clone(),
                (table_id, key, clustering, shape.used_schema_layout),
            );
            if let Some(row_json) = existing_row_json.or_else(|| {
                let meta = table_metadata_for(schema, &op.table.keyspace, &op.table.table)?;
                let row = Row {
                    clustering: shape.clustering.clone(),
                    cells: shape.create_cells.clone(),
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                };
                let partition = ferrosa_sstable::types::Partition {
                    key: shape.key.clone(),
                    deletion: DeletionTime::LIVE,
                    static_row: None,
                    rows: vec![row.clone()],
                };
                Some(row_to_json(&meta, &partition, &row))
            }) {
                var_rows.insert(var_name.clone(), row_json);
            }
        }
    }

    // Step 4: apply trailing SET assignments.
    for (var, property, val_expr) in set_clause {
        let Some((table_id, key, clustering, used_schema_layout)) = var_keys.get(var) else {
            return Err(GraphError::Validation(format!(
                "execute_merge: SET references variable '{}' which was not bound by any MERGE; \
                 check that the MERGE pattern declares a variable binding \
                 (file: {}, line: {})",
                var,
                file!(),
                line!()
            )));
        };

        let Some(column_idx) = regular_column_index_for_property(
            schema,
            table_id.keyspace.as_str(),
            table_id.table.as_str(),
            property,
        ) else {
            if *used_schema_layout
                && matches!(
                    column_kind_for_property(
                        schema,
                        table_id.keyspace.as_str(),
                        table_id.table.as_str(),
                        property,
                    ),
                    Some(
                        ferrosa_schema::metadata::column::ColumnKind::PartitionKey
                            | ferrosa_schema::metadata::column::ColumnKind::Clustering
                    )
                )
            {
                continue;
            }
            return Err(GraphError::Validation(format!(
                "execute_merge: SET references unknown property '{}' on table '{}.{}'",
                property, table_id.keyspace, table_id.table
            )));
        };
        let bytes = encode_property_value_for_table(
            schema,
            table_id.keyspace.as_str(),
            table_id.table.as_str(),
            property,
            val_expr,
        )?;

        let update_row = Row {
            clustering: clustering.clone(),
            cells: vec![(column_idx, CellValue::live(bytes, timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        };

        write_path
            .write(
                table_id,
                key,
                update_row,
                timestamp,
                graph_write_consistency(),
                &strategy,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    file = file!(),
                    line = line!(),
                    error = %e,
                    var = %var,
                    property = %property,
                    "execute_merge: SET write failed"
                );
                e
            })?;
        stats.vertices_written += 1;
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "merged {} vertices, {} properties updated",
            merges.len(),
            set_clause.len()
        ))]],
        stats,
    })
}

async fn write_explicit_adjacency_entries(
    write_path: &WritePath,
    table_id: &TableId,
    key: &DecoratedKey,
    row: &Row,
    timestamp: i64,
    schema: Option<&Schema>,
) -> Result<()> {
    let Some(schema) = schema else {
        return Ok(());
    };

    let mutation = Mutation::new(
        table_id.keyspace.clone(),
        table_id.table.clone(),
        key.clone(),
        vec![row.clone()],
        timestamp,
    );

    for derived in derive_adjacency_mutations(schema, table_id, &mutation) {
        let adj_table_id = TableId::new(&derived.keyspace, &derived.table);
        let strategy = graph_replication_strategy(Some(schema), &derived.keyspace)?;
        for derived_row in derived.rows {
            adjacency_write_with_retry(
                write_path,
                &adj_table_id,
                &derived.key,
                derived_row,
                derived.timestamp,
                &strategy,
                table_id,
            )
            .await?;
        }
    }

    Ok(())
}

/// Write a derived adjacency row, retrying on transient failures that arise
/// when the `system_graph_<ks>.adjacency` table was just created via Raft DDL
/// and a follower hasn't yet finished applying the CREATE TABLE on its own
/// state machine.
///
/// The window is narrow — openraft commits once a majority has replicated the
/// log entry, but each follower's `apply` to the local StorageEngine happens
/// in a separate task and can lag a few hundred ms behind on a busy CI
/// runner. When the coordinator immediately fans out the first derived
/// adjacency write at CL=QUORUM, lagging followers reject it with
/// `table not registered: system_graph_<ks>.adjacency` (logged as
/// `MutationForward write failed — not sending ACK`), the coordinator
/// returns `WriteTimeout(received=1, required=2)`, and the graph HTTP
/// request hangs until the client's read timeout.
///
/// Retrying with exponential backoff lets the followers catch up. The write
/// is content-addressed and idempotent (LWW with monotonic timestamps), so
/// duplicate forwards to replicas that already applied are safe.
async fn adjacency_write_with_retry(
    write_path: &WritePath,
    adj_table_id: &TableId,
    key: &DecoratedKey,
    row: Row,
    timestamp: i64,
    strategy: &ReplicationStrategy,
    edge_table_id: &TableId,
) -> Result<()> {
    // Total budget is ~3 seconds across 6 attempts: 100, 200, 400, 800,
    // 1600 ms. Picks up where DDL_AGREEMENT_APPLY_DRAIN (50 ms) stops.
    const BACKOFFS_MS: &[u64] = &[100, 200, 400, 800, 1600];
    let mut attempt = 0usize;
    let mut backoffs = BACKOFFS_MS.iter().copied();
    loop {
        match write_path
            .write(
                adj_table_id,
                key,
                row.clone(),
                timestamp,
                graph_write_consistency(),
                strategy,
            )
            .await
        {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(
                        adj_table = %adj_table_id,
                        attempt,
                        "adjacency write succeeded after retries (follower apply lag)"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                if !is_transient_replica_lag(&e) {
                    return Err(GraphError::Storage(ferrosa_common::Error::InvalidData(
                        format!(
                            "failed to write derived adjacency row for {}.{}: {e}",
                            edge_table_id.keyspace, edge_table_id.table
                        ),
                    )));
                }
                let Some(backoff_ms) = backoffs.next() else {
                    // Retry budget exhausted — surface the last transient error
                    // so the operator sees the actual cluster state.
                    return Err(GraphError::Storage(ferrosa_common::Error::InvalidData(
                        format!(
                            "failed to write derived adjacency row for {}.{} after \
                             {attempt} retries: {e}",
                            edge_table_id.keyspace, edge_table_id.table
                        ),
                    )));
                };
                tracing::warn!(
                    adj_table = %adj_table_id,
                    attempt = attempt + 1,
                    backoff_ms,
                    %e,
                    "adjacency write hit replica schema lag — retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                attempt += 1;
            }
        }
    }
}

/// Distinguish a transient replica-schema-lag write timeout (retryable) from
/// a real cluster failure (not retryable). The cluster coordinator surfaces
/// `WriteTimeout` as `Error::InvalidData("cluster: write timeout: CL=…,
/// received=N, required=M")` — that exact wrapping is the contract we match
/// against. Any other error (unavailable, validation, storage I/O) is final.
fn is_transient_replica_lag(e: &ferrosa_common::Error) -> bool {
    let msg = e.to_string();
    msg.contains("cluster: write timeout") || msg.contains("table not registered")
}

fn build_merge_write_shape(
    op: &MergeOp,
    set_props: &[(String, Expr)],
    schema: Option<&Schema>,
    timestamp: i64,
) -> Result<MergeWriteShape> {
    if let Some(shape) = build_schema_aware_merge_shape(op, set_props, schema, timestamp)? {
        return Ok(shape);
    }

    let key_bytes: Vec<u8> = if op.table.graph_type == "edge" {
        match &op.src_match_props {
            Some(src_props) => content_addressed_key(src_props),
            None => {
                tracing::error!(
                    file = file!(),
                    line = line!(),
                    table = %op.table.table,
                    graph_type = %op.table.graph_type,
                    "execute_merge: edge MergeOp has no src_match_props — \
                     cannot derive partition key for adjacency index. \
                     The planner did not thread the source node's props \
                     into this MergeOp. This is a planner bug."
                );
                return Err(crate::error::GraphError::Validation(format!(
                    "execute_merge: edge MergeOp for table '{}' has no \
                     src_match_props; cannot derive partition key \
                     (file: {}, line: {})",
                    op.table.table,
                    file!(),
                    line!()
                )));
            }
        }
    } else {
        content_addressed_key(&op.match_props)
    };

    let clustering: Vec<u8> = if op.table.graph_type == "edge" {
        match &op.dst_match_props {
            Some(dst_props) => content_addressed_key(dst_props),
            None => {
                tracing::error!(
                    file = file!(),
                    line = line!(),
                    table = %op.table.table,
                    graph_type = %op.table.graph_type,
                    "execute_merge: edge MergeOp has no dst_match_props — \
                     cannot derive clustering key for adjacency index. \
                     The planner did not thread the destination node's props \
                     into this MergeOp. This is a planner bug."
                );
                return Err(crate::error::GraphError::Validation(format!(
                    "execute_merge: edge MergeOp for table '{}' has no \
                     dst_match_props; cannot derive clustering key \
                     (file: {}, line: {})",
                    op.table.table,
                    file!(),
                    line!()
                )));
            }
        }
    } else {
        vec![]
    };

    let create_cells: Vec<(u16, CellValue)> = if let Some(schema) = schema {
        let snap = schema.snapshot();
        if let Some(meta) = snap
            .tables
            .get(&(op.table.keyspace.clone(), op.table.table.clone()))
        {
            let property_exprs: HashMap<String, Expr> = op
                .match_props
                .iter()
                .chain(op.create_props.iter())
                .chain(set_props.iter())
                .map(|(name, expr)| (name.clone(), expr.clone()))
                .collect();
            encode_storage_ordered_cells(meta, &property_exprs, timestamp)?
        } else {
            op.match_props
                .iter()
                .chain(op.create_props.iter())
                .enumerate()
                .map(|(idx, (_name, expr))| {
                    let bytes = expr_to_bytes(expr).unwrap_or_default();
                    (idx as u16, CellValue::live(bytes, timestamp))
                })
                .collect()
        }
    } else {
        op.match_props
            .iter()
            .chain(op.create_props.iter())
            .enumerate()
            .map(|(idx, (_name, expr))| {
                let bytes = expr_to_bytes(expr).unwrap_or_default();
                (idx as u16, CellValue::live(bytes, timestamp))
            })
            .collect()
    };

    Ok(MergeWriteShape {
        key: DecoratedKey::new(PartitionKey::new(key_bytes)),
        clustering,
        create_cells,
        used_schema_layout: false,
    })
}

fn merge_can_bind_schema_key_only_vertex_without_read(
    op: &MergeOp,
    shape: &MergeWriteShape,
) -> bool {
    op.table.graph_type == "vertex"
        && shape.used_schema_layout
        && shape.create_cells.is_empty()
        && !shape.clustering.is_empty()
}

fn build_schema_aware_merge_shape(
    op: &MergeOp,
    set_props: &[(String, Expr)],
    schema: Option<&Schema>,
    timestamp: i64,
) -> Result<Option<MergeWriteShape>> {
    let Some(schema) = schema else {
        return Ok(None);
    };
    let snap = schema.snapshot();
    let Some(meta) = snap
        .tables
        .get(&(op.table.keyspace.clone(), op.table.table.clone()))
    else {
        return Ok(None);
    };

    let mut property_exprs: HashMap<String, Expr> = HashMap::new();
    for (name, expr) in op.match_props.iter().chain(op.create_props.iter()) {
        property_exprs.insert(name.clone(), expr.clone());
    }
    for (name, expr) in set_props {
        property_exprs.insert(name.clone(), expr.clone());
    }

    if op.table.graph_type == "edge" {
        if let Some(source_col) = meta.extensions.get("graph.source") {
            if !property_exprs.contains_key(source_col) {
                if let Some(source_type) = meta
                    .columns
                    .get(source_col)
                    .map(|col| col.column_type.as_str())
                {
                    if let Some(expr) = resolve_endpoint_property_expr(
                        op.src_match_props.as_deref(),
                        source_col,
                        source_type,
                    ) {
                        property_exprs.insert(source_col.clone(), expr);
                    }
                }
            }
        }
        if let Some(target_col) = meta.extensions.get("graph.target") {
            if !property_exprs.contains_key(target_col) {
                if let Some(target_type) = meta
                    .columns
                    .get(target_col)
                    .map(|col| col.column_type.as_str())
                {
                    if let Some(expr) = resolve_endpoint_property_expr(
                        op.dst_match_props.as_deref(),
                        target_col,
                        target_type,
                    ) {
                        property_exprs.insert(target_col.clone(), expr);
                    }
                }
            }
        }
    }

    let mut partition_components = Vec::with_capacity(meta.partition_key.len());
    for pk_name in &meta.partition_key {
        let Some(expr) = property_exprs.get(pk_name) else {
            return Ok(None);
        };
        let Some(column) = meta.columns.get(pk_name) else {
            return Ok(None);
        };
        let Ok(bytes) = try_encode_expr_for_column_type(expr, &column.column_type) else {
            return Ok(None);
        };
        partition_components.push(bytes);
    }

    let mut clustering_components = Vec::with_capacity(meta.clustering_key.len());
    for (ck_name, _) in &meta.clustering_key {
        let Some(expr) = property_exprs.get(ck_name) else {
            return Ok(None);
        };
        let Some(column) = meta.columns.get(ck_name) else {
            return Ok(None);
        };
        let Ok(bytes) = try_encode_expr_for_column_type(expr, &column.column_type) else {
            return Ok(None);
        };
        clustering_components.push(bytes);
    }

    let partition_key = encode_partition_components(&partition_components);
    let clustering = encode_clustering_components(&clustering_components);
    let create_cells = encode_storage_ordered_cells(meta, &property_exprs, timestamp)?;

    Ok(Some(MergeWriteShape {
        key: DecoratedKey::new(PartitionKey::new(partition_key)),
        clustering,
        create_cells,
        used_schema_layout: true,
    }))
}

async fn infer_schema_aware_merge_shape(
    write_path: &WritePath,
    op: &MergeOp,
    set_props: &[(String, Expr)],
    schema: &Schema,
    timestamp: i64,
    bound_rows: &HashMap<String, serde_json::Value>,
) -> Result<Option<MergeWriteShape>> {
    let snap = schema.snapshot();
    let Some(meta) = snap
        .tables
        .get(&(op.table.keyspace.clone(), op.table.table.clone()))
        .cloned()
    else {
        return Ok(None);
    };

    let mut property_exprs: HashMap<String, Expr> = HashMap::new();
    for (name, expr) in op.match_props.iter().chain(op.create_props.iter()) {
        property_exprs.insert(name.clone(), expr.clone());
    }
    for (name, expr) in set_props {
        property_exprs.insert(name.clone(), expr.clone());
    }

    if op.table.graph_type == "edge" {
        if let Some(source_col) = meta.extensions.get("graph.source") {
            if !property_exprs.contains_key(source_col) {
                if let Some(source_type) = meta
                    .columns
                    .get(source_col)
                    .map(|col| col.column_type.as_str())
                {
                    if let Some(expr) = resolve_endpoint_property_expr(
                        op.src_match_props.as_deref(),
                        source_col,
                        source_type,
                    ) {
                        property_exprs.insert(source_col.clone(), expr);
                    }
                }
            }
        }
        if let Some(target_col) = meta.extensions.get("graph.target") {
            if !property_exprs.contains_key(target_col) {
                if let Some(target_type) = meta
                    .columns
                    .get(target_col)
                    .map(|col| col.column_type.as_str())
                {
                    if let Some(expr) = resolve_endpoint_property_expr(
                        op.dst_match_props.as_deref(),
                        target_col,
                        target_type,
                    ) {
                        property_exprs.insert(target_col.clone(), expr);
                    }
                }
            }
        }

        let mut source_json = if let Some(var_name) = op.src_var.as_ref() {
            bound_rows.get(var_name).cloned()
        } else {
            None
        };
        if source_json.is_none() {
            if let (Some(source_label), Some(source_props)) = (
                meta.extensions.get("graph.source_label"),
                op.src_match_props.as_deref(),
            ) {
                if let Some(source_meta) =
                    resolve_table_by_graph_label(schema, &op.table.keyspace, source_label)
                {
                    let source_tid = TableId::new(&source_meta.keyspace, &source_meta.name);
                    source_json = find_table_row_by_props(
                        write_path,
                        &source_tid,
                        &source_meta,
                        source_props,
                        "src",
                    )
                    .await
                    .map(|row| row.map(|row| row.json))?;
                }
            }
        }
        let mut target_json = if let Some(var_name) = op.dst_var.as_ref() {
            bound_rows.get(var_name).cloned()
        } else {
            None
        };
        if target_json.is_none() {
            if let (Some(target_label), Some(target_props)) = (
                meta.extensions.get("graph.target_label"),
                op.dst_match_props.as_deref(),
            ) {
                if let Some(target_meta) =
                    resolve_table_by_graph_label(schema, &op.table.keyspace, target_label)
                {
                    let target_tid = TableId::new(&target_meta.keyspace, &target_meta.name);
                    target_json = find_table_row_by_props(
                        write_path,
                        &target_tid,
                        &target_meta,
                        target_props,
                        "dst",
                    )
                    .await
                    .map(|row| row.map(|row| row.json))?;
                }
            }
        }

        for key_name in meta
            .partition_key
            .iter()
            .chain(meta.clustering_key.iter().map(|(name, _)| name))
        {
            if property_exprs.contains_key(key_name) {
                continue;
            }

            let source_value = source_json
                .as_ref()
                .and_then(|row| row.get(key_name))
                .and_then(json_value_to_expr);
            let target_value = target_json
                .as_ref()
                .and_then(|row| row.get(key_name))
                .and_then(json_value_to_expr);

            let inferred = match (source_value, target_value) {
                (Some(src), Some(dst)) if src == dst => Some(src),
                (Some(src), None) => Some(src),
                (None, Some(dst)) => Some(dst),
                _ => None,
            };
            if let Some(expr) = inferred {
                property_exprs.insert(key_name.clone(), expr);
            }
        }
    } else {
        let table_tid = TableId::new(&meta.keyspace, &meta.name);
        if let Some(existing) =
            find_table_row_by_props(write_path, &table_tid, &meta, &op.match_props, "n").await?
        {
            return Ok(Some(MergeWriteShape {
                key: existing.key,
                clustering: existing.clustering,
                create_cells: vec![],
                used_schema_layout: true,
            }));
        }
    }

    let mut partition_components = Vec::with_capacity(meta.partition_key.len());
    for pk_name in &meta.partition_key {
        let Some(expr) = property_exprs.get(pk_name) else {
            return Ok(None);
        };
        let Some(column) = meta.columns.get(pk_name) else {
            return Ok(None);
        };
        let Ok(bytes) = try_encode_expr_for_column_type(expr, &column.column_type) else {
            return Ok(None);
        };
        partition_components.push(bytes);
    }

    let mut clustering_components = Vec::with_capacity(meta.clustering_key.len());
    for (ck_name, _) in &meta.clustering_key {
        let Some(expr) = property_exprs.get(ck_name) else {
            return Ok(None);
        };
        let Some(column) = meta.columns.get(ck_name) else {
            return Ok(None);
        };
        let Ok(bytes) = try_encode_expr_for_column_type(expr, &column.column_type) else {
            return Ok(None);
        };
        clustering_components.push(bytes);
    }

    let partition_key = encode_partition_components(&partition_components);
    let clustering = encode_clustering_components(&clustering_components);
    let create_cells = encode_storage_ordered_cells(&meta, &property_exprs, timestamp)?;

    Ok(Some(MergeWriteShape {
        key: DecoratedKey::new(PartitionKey::new(partition_key)),
        clustering,
        create_cells,
        used_schema_layout: true,
    }))
}

fn encode_storage_ordered_cells(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
    property_exprs: &HashMap<String, Expr>,
    timestamp: i64,
) -> Result<Vec<(u16, CellValue)>> {
    let mut create_cells = Vec::new();
    for column in meta.columns.values() {
        match column.kind {
            ferrosa_schema::metadata::column::ColumnKind::Regular
            | ferrosa_schema::metadata::column::ColumnKind::Static => {
                if let Some(expr) = property_exprs.get(&column.name) {
                    let storage_idx = meta.storage_column_index(&column.name).ok_or_else(|| {
                        GraphError::Validation(format!(
                            "table '{}.{}' column '{}' has no storage column index",
                            meta.keyspace, meta.name, column.name
                        ))
                    })?;
                    let bytes = encode_expr_for_column_type(expr, &column.column_type)?;
                    create_cells.push((storage_idx, CellValue::live(bytes, timestamp)));
                }
            }
            _ => {}
        }
    }
    create_cells.sort_by_key(|(idx, _)| *idx);
    Ok(create_cells)
}

fn storage_cell_columns(
    meta: &ferrosa_schema::metadata::table::TableMetadata,
) -> Vec<&ferrosa_schema::metadata::column::ColumnMetadata> {
    let mut columns: Vec<_> = meta
        .columns
        .values()
        .filter(|column| {
            column.kind == ferrosa_schema::metadata::column::ColumnKind::Regular
                || column.kind == ferrosa_schema::metadata::column::ColumnKind::Static
        })
        .collect();
    columns.sort_by_key(|column| meta.storage_column_index(&column.name).unwrap_or(u16::MAX));
    columns
}

fn schema_merge_requires_hidden_key_resolution(
    op: &MergeOp,
    set_props: &[(String, Expr)],
    schema: &Schema,
) -> bool {
    let snap = schema.snapshot();
    let Some(meta) = snap
        .tables
        .get(&(op.table.keyspace.clone(), op.table.table.clone()))
    else {
        return false;
    };

    let mut property_names: std::collections::HashSet<String> = op
        .match_props
        .iter()
        .chain(op.create_props.iter())
        .map(|(name, _)| name.clone())
        .collect();
    property_names.extend(set_props.iter().map(|(name, _)| name.clone()));

    if op.table.graph_type == "edge" {
        if let Some(source_col) = meta.extensions.get("graph.source") {
            if let Some(source_type) = meta
                .columns
                .get(source_col)
                .map(|col| col.column_type.as_str())
            {
                if resolve_endpoint_property_expr(
                    op.src_match_props.as_deref(),
                    source_col,
                    source_type,
                )
                .is_some()
                {
                    property_names.insert(source_col.clone());
                }
            }
        }
        if let Some(target_col) = meta.extensions.get("graph.target") {
            if let Some(target_type) = meta
                .columns
                .get(target_col)
                .map(|col| col.column_type.as_str())
            {
                if resolve_endpoint_property_expr(
                    op.dst_match_props.as_deref(),
                    target_col,
                    target_type,
                )
                .is_some()
                {
                    property_names.insert(target_col.clone());
                }
            }
        }
    }

    let mut saw_present = false;
    let mut saw_missing = false;
    for name in meta
        .partition_key
        .iter()
        .chain(meta.clustering_key.iter().map(|(name, _)| name))
    {
        if property_names.contains(name) {
            saw_present = true;
        } else {
            saw_missing = true;
        }
    }

    saw_present && saw_missing
}

fn resolve_endpoint_property_expr(
    endpoint_props: Option<&[(String, Expr)]>,
    target_column: &str,
    target_column_type: &str,
) -> Option<Expr> {
    let props = endpoint_props?;

    if let Some((_, expr)) = props.iter().find(|(name, _)| name == target_column) {
        return Some(expr.clone());
    }

    let preferred_endpoint_ids: &[&str] = match target_column {
        "entity_a" | "entity_b" | "src_id" | "dst_id" => &["entity_id"],
        "source_fold_id" | "target_fold_id" | "fold_id" => &["fold_id"],
        "new_event_id" | "old_event_id" | "event_id" => &["event_id"],
        _ => &[],
    };
    for preferred in preferred_endpoint_ids {
        if let Some((_, expr)) = props.iter().find(|(name, expr)| {
            name == preferred && try_encode_expr_for_column_type(expr, target_column_type).is_ok()
        }) {
            return Some(expr.clone());
        }
    }

    if props.len() == 1 && try_encode_expr_for_column_type(&props[0].1, target_column_type).is_ok()
    {
        return Some(props[0].1.clone());
    }

    props
        .iter()
        .find(|(name, expr)| {
            name.ends_with("_id")
                && name.as_str() != "tenant_id"
                && name.as_str() != "session_id"
                && try_encode_expr_for_column_type(expr, target_column_type).is_ok()
        })
        .map(|(_, expr)| expr.clone())
}

fn encode_partition_components(components: &[Vec<u8>]) -> Vec<u8> {
    if components.len() == 1 {
        return components[0].clone();
    }

    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
        buf.push(0x00);
    }
    buf
}

fn encode_clustering_components(components: &[Vec<u8>]) -> Vec<u8> {
    if components.len() == 1 {
        return components[0].clone();
    }

    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
    }
    buf
}

fn encode_property_value_for_table(
    schema: Option<&Schema>,
    keyspace: &str,
    table: &str,
    property: &str,
    expr: &Expr,
) -> Result<Vec<u8>> {
    if let Some(column_type) = column_type_for_property(schema, keyspace, table, property) {
        return encode_expr_for_column_type(expr, &column_type);
    }

    expr_to_bytes(expr)
}

fn try_encode_expr_for_column_type(
    expr: &Expr,
    column_type: &str,
) -> std::result::Result<Vec<u8>, GraphError> {
    encode_expr_for_column_type(expr, column_type)
}

fn encode_expr_for_column_type(expr: &Expr, column_type: &str) -> Result<Vec<u8>> {
    let lower = column_type.to_ascii_lowercase();
    match expr {
        Expr::Literal(Literal::String(s)) if lower == "uuid" || lower.ends_with("uuidtype") => {
            let uuid = uuid::Uuid::parse_str(s).map_err(|e| {
                GraphError::Validation(format!("invalid UUID literal '{s}' for {column_type}: {e}"))
            })?;
            Ok(uuid.as_bytes().to_vec())
        }
        Expr::Literal(Literal::String(s))
            if lower == "text"
                || lower == "varchar"
                || lower.ends_with("utf8type")
                || lower.ends_with("asciitype") =>
        {
            Ok(s.as_bytes().to_vec())
        }
        Expr::Literal(Literal::String(s))
            if lower == "timestamp" || lower.ends_with("timestamptype") =>
        {
            let ms = chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|e| {
                    GraphError::Validation(format!("invalid timestamp literal '{s}': {e}"))
                })?
                .timestamp_millis();
            Ok(ms.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "timestamp" || lower.ends_with("timestamptype") =>
        {
            Ok(i.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Float(f)) if lower == "double" || lower.ends_with("doubletype") => {
            Ok(f.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "double" || lower.ends_with("doubletype") =>
        {
            Ok((*i as f64).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Float(f)) if lower == "float" || lower.ends_with("floattype") => {
            Ok((*f as f32).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i)) if lower == "float" || lower.ends_with("floattype") => {
            Ok((*i as f32).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i)) if lower == "int" || lower.ends_with("int32type") => {
            let value = i32::try_from(*i).map_err(|_| {
                GraphError::Validation(format!("integer literal {i} does not fit in int"))
            })?;
            Ok(value.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "bigint" || lower == "long" || lower.ends_with("longtype") =>
        {
            Ok(i.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Bool(b)) if lower == "boolean" || lower.ends_with("booleantype") => {
            Ok(vec![u8::from(*b)])
        }
        _ => expr_to_bytes(expr),
    }
}

/// Does this plan's projection READ STORAGE, rather than only the values
/// already bound by the match?
///
/// SET and REMOVE consume their inner expand as a stream, so its projection is
/// interleaved with their writes. That is only sound when the projection cannot
/// observe those writes. `project_state` touches storage exactly when a RETURN
/// item contains a pattern comprehension (`graph_eval_expr`); otherwise it
/// evaluates against `state.bindings`, which were captured before any write.
///
/// The planner synthesizes plain `Expr::Var` projections for SET/REMOVE
/// (`plan_set` / `plan_remove`), so this is false in practice. It exists so
/// that if that ever changes, the caller drains the projection BEFORE writing
/// instead of silently letting a comprehension read half-written state.
fn projection_reads_storage(plan: &PhysicalPlan) -> bool {
    let return_clause = match plan {
        PhysicalPlan::Expand { return_clause, .. } => return_clause,
        // Any other shape is not the plain expand SET/REMOVE plan for; be
        // conservative and make the caller materialize first.
        _ => return true,
    };
    return_clause
        .items
        .iter()
        .any(|item| expr_has_pattern_comprehension(&item.expr))
}

/// Execute a SET plan: run the expand to find matching vertices, then write
/// updated cells for each one.
#[allow(clippy::too_many_arguments)]
async fn execute_set(
    write_path: &WritePath,
    expand: PhysicalPlan,
    keyspace: &str,
    assignments: &[(String, String, Expr)],
    variable_tables: &HashMap<String, String>,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    start: Instant,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    // Execute the inner expand to find matching vertices.
    // Consume the inner expand as a STREAM: its projected rows are never
    // collected into a `Vec`, so a SET/REMOVE across a large MATCH no longer
    // buffers one row per matched vertex. Columns arrive before rows, which is
    // all the write loop below needs.
    //
    // Soundness of interleaving projection with our writes: the projection only
    // touches storage when it contains a pattern comprehension, and
    // `projection_reads_storage` makes us drain the stream first in that case,
    // so a projection can never observe a write this statement is making.
    use futures::StreamExt as _;

    let drain_first = projection_reads_storage(&expand);
    let (columns, rows_stream, expand_stats) = Box::pin(execute_streaming(
        expand,
        write_path,
        keyspace,
        config,
        virtual_tables,
        schema,
    ))
    .await?;
    let mut rows_stream = if drain_first {
        stream_from_rows(crate::executor::stream::collect_rows(rows_stream).await?)
    } else {
        rows_stream
    };
    let mut stats = QueryStats {
        vertices_read: expand_stats.vertices_read,
        edges_read: expand_stats.edges_read,
        ..Default::default()
    };

    let timestamp = now_micros();
    let strategy = graph_replication_strategy(schema, keyspace)?;

    // For each matched vertex, apply the assignments.
    // The expand result rows contain vertex IDs (hex-encoded).
    // We need to reconstruct the partition key from the hex ID.
    while let Some(row_values) = rows_stream.next().await {
        let row_values = row_values?;
        for (col_idx, col_name) in columns.iter().enumerate() {
            // Find assignments that target this variable.
            let matching_assignments: Vec<&(String, String, Expr)> = assignments
                .iter()
                .filter(|(var, _prop, _val)| col_name == var)
                .collect();

            if matching_assignments.is_empty() {
                continue;
            }

            // Get the vertex ID from the row. `RETURN n`/`Expr::Var` produces a
            // JSON object with `_id`; older/simple projections can still be a
            // bare hex string.
            let hex_id = match row_values.get(col_idx) {
                Some(serde_json::Value::String(s)) => Some(s.as_str()),
                Some(serde_json::Value::Object(map)) => map.get("_id").and_then(|v| v.as_str()),
                _ => None,
            };

            if let Some(hex_id) = hex_id {
                let table_name = variable_tables
                    .get(col_name)
                    .map(String::as_str)
                    .unwrap_or(col_name);
                let table_id = TableId::new(keyspace, table_name);
                let table_meta = table_metadata_for(schema, keyspace, table_name);
                let is_edge_table = table_meta.as_ref().is_some_and(|meta| {
                    meta.extensions
                        .get("graph.type")
                        .is_some_and(|graph_type| graph_type == "edge")
                });
                let key_hex = if is_edge_table {
                    row_values
                        .get(col_idx)
                        .and_then(|value| value.as_object())
                        .and_then(|map| map.get("__ferrosa_key"))
                        .and_then(|value| value.as_str())
                        .unwrap_or(hex_id)
                } else {
                    hex_id
                };
                let key_bytes = hex::decode(key_hex)
                    .map_err(|e| GraphError::Internal(format!("invalid hex storage key: {e}")))?;
                let key = DecoratedKey::new(PartitionKey::new(key_bytes));
                let clustering = if is_edge_table {
                    row_values
                        .get(col_idx)
                        .and_then(|value| value.as_object())
                        .and_then(|map| map.get("__ferrosa_clustering"))
                        .and_then(|value| value.as_str())
                        .map(hex::decode)
                        .transpose()
                        .map_err(|e| {
                            GraphError::Internal(format!("invalid hex storage clustering: {e}"))
                        })?
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };

                let mut cells = Vec::with_capacity(matching_assignments.len());
                for (_var, prop, val) in matching_assignments {
                    let Some(column_idx) =
                        regular_column_index_for_property(schema, keyspace, table_name, prop)
                    else {
                        if matches!(
                            column_kind_for_property(schema, keyspace, table_name, prop),
                            Some(
                                ferrosa_schema::metadata::column::ColumnKind::PartitionKey
                                    | ferrosa_schema::metadata::column::ColumnKind::Clustering
                            )
                        ) {
                            continue;
                        }
                        return Err(GraphError::Validation(format!(
                            "execute_set: SET references unknown property '{}' on table '{}.{}'",
                            prop, keyspace, table_name
                        )));
                    };
                    let bytes =
                        encode_property_value_for_table(schema, keyspace, table_name, prop, val)?;
                    cells.push((column_idx, CellValue::live(bytes, timestamp)));
                }

                if cells.is_empty() {
                    continue;
                }

                let update_row = Row {
                    clustering,
                    cells,
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::NONE,
                };

                write_path
                    .write(
                        &table_id,
                        &key,
                        update_row,
                        timestamp,
                        graph_write_consistency(),
                        &strategy,
                    )
                    .await?;
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

/// Execute a REMOVE plan: run the inner expand to find matching vertices, then
/// for each `REMOVE n.prop` write a cell-level tombstone so the property is
/// unset and invisible to subsequent reads (URS-QEC-D06 / T-QEC-D07).
///
/// Mirrors `execute_set`, but instead of writing a live cell it writes a
/// `CellValue::tombstone` at the property's column. `REMOVE n:Label` has no
/// representation in this label-as-table storage model, so it fails loud rather
/// than silently no-op (URS-QEC-X01).
#[allow(clippy::too_many_arguments)]
async fn execute_remove(
    write_path: &WritePath,
    expand: PhysicalPlan,
    keyspace: &str,
    items: &[RemoveItem],
    variable_tables: &HashMap<String, String>,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    start: Instant,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    // Fail loud on label removal: there is no storage representation for
    // dropping a label (label == table membership), so never fake success.
    for item in items {
        if let RemoveItem::Label { label, .. } = item {
            return Err(GraphError::Validation(format!(
                "execute_remove: REMOVE n:{label} (label removal) is not supported \
                 in the label-as-table storage model"
            )));
        }
    }

    // Consume the inner expand as a STREAM: its projected rows are never
    // collected into a `Vec`, so a SET/REMOVE across a large MATCH no longer
    // buffers one row per matched vertex. Columns arrive before rows, which is
    // all the write loop below needs.
    //
    // Soundness of interleaving projection with our writes: the projection only
    // touches storage when it contains a pattern comprehension, and
    // `projection_reads_storage` makes us drain the stream first in that case,
    // so a projection can never observe a write this statement is making.
    use futures::StreamExt as _;

    let drain_first = projection_reads_storage(&expand);
    let (columns, rows_stream, expand_stats) = Box::pin(execute_streaming(
        expand,
        write_path,
        keyspace,
        config,
        virtual_tables,
        schema,
    ))
    .await?;
    let mut rows_stream = if drain_first {
        stream_from_rows(crate::executor::stream::collect_rows(rows_stream).await?)
    } else {
        rows_stream
    };
    let mut stats = QueryStats {
        vertices_read: expand_stats.vertices_read,
        edges_read: expand_stats.edges_read,
        ..Default::default()
    };

    let timestamp = now_micros();
    let local_deletion_time = timestamp.div_euclid(1_000_000).clamp(0, i32::MAX as i64) as i32;
    let strategy = graph_replication_strategy(schema, keyspace)?;

    while let Some(row_values) = rows_stream.next().await {
        let row_values = row_values?;
        for (col_idx, col_name) in columns.iter().enumerate() {
            let matching_props: Vec<&str> = items
                .iter()
                .filter_map(|item| match item {
                    RemoveItem::Property { var, property } if var == col_name => {
                        Some(property.as_str())
                    }
                    _ => None,
                })
                .collect();
            if matching_props.is_empty() {
                continue;
            }

            let hex_id = match row_values.get(col_idx) {
                Some(serde_json::Value::String(s)) => Some(s.as_str()),
                Some(serde_json::Value::Object(map)) => map.get("_id").and_then(|v| v.as_str()),
                _ => None,
            };
            let Some(hex_id) = hex_id else { continue };

            let table_name = variable_tables
                .get(col_name)
                .map(String::as_str)
                .unwrap_or(col_name);
            let table_id = TableId::new(keyspace, table_name);
            let table_meta = table_metadata_for(schema, keyspace, table_name);
            let is_edge_table = table_meta.as_ref().is_some_and(|meta| {
                meta.extensions
                    .get("graph.type")
                    .is_some_and(|graph_type| graph_type == "edge")
            });
            let key_hex = if is_edge_table {
                row_values
                    .get(col_idx)
                    .and_then(|value| value.as_object())
                    .and_then(|map| map.get("__ferrosa_key"))
                    .and_then(|value| value.as_str())
                    .unwrap_or(hex_id)
            } else {
                hex_id
            };
            let key_bytes = hex::decode(key_hex)
                .map_err(|e| GraphError::Internal(format!("invalid hex storage key: {e}")))?;
            let key = DecoratedKey::new(PartitionKey::new(key_bytes));
            let clustering = if is_edge_table {
                row_values
                    .get(col_idx)
                    .and_then(|value| value.as_object())
                    .and_then(|map| map.get("__ferrosa_clustering"))
                    .and_then(|value| value.as_str())
                    .map(hex::decode)
                    .transpose()
                    .map_err(|e| {
                        GraphError::Internal(format!("invalid hex storage clustering: {e}"))
                    })?
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            let mut cells = Vec::with_capacity(matching_props.len());
            for prop in &matching_props {
                let Some(column_idx) =
                    regular_column_index_for_property(schema, keyspace, table_name, prop)
                else {
                    if matches!(
                        column_kind_for_property(schema, keyspace, table_name, prop),
                        Some(
                            ferrosa_schema::metadata::column::ColumnKind::PartitionKey
                                | ferrosa_schema::metadata::column::ColumnKind::Clustering
                        )
                    ) {
                        return Err(GraphError::Validation(format!(
                            "execute_remove: REMOVE cannot unset key column '{}' on table '{}.{}'",
                            prop, keyspace, table_name
                        )));
                    }
                    return Err(GraphError::Validation(format!(
                        "execute_remove: REMOVE references unknown property '{}' on table '{}.{}'",
                        prop, keyspace, table_name
                    )));
                };
                cells.push((
                    column_idx,
                    CellValue::tombstone(timestamp, local_deletion_time),
                ));
            }

            if cells.is_empty() {
                continue;
            }

            let update_row = Row {
                clustering,
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            };

            write_path
                .write(
                    &table_id,
                    &key,
                    update_row,
                    timestamp,
                    graph_write_consistency(),
                    &strategy,
                )
                .await?;
            stats.vertices_written += 1;
        }
    }

    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns: vec!["status".to_string()],
        rows: vec![vec![serde_json::Value::String(format!(
            "removed properties on {} vertices",
            stats.vertices_written
        ))]],
        stats,
    })
}

/// Execute a DELETE plan: run the expand to find matching vertices, then write
/// tombstones for each one.
#[allow(clippy::too_many_arguments)]
/// Build the standard composite adjacency clustering key
/// `[u16 1][1B direction][u16 label_len][label][u16 nid_len][neighbor_id]`,
/// matching `make_adjacency_mutation`'s wire format. Defined once so the
/// DETACH-DELETE cascade tombstones the exact key the observer wrote.
fn adjacency_clustering_bytes(direction: u8, edge_label: &str, neighbor_id: &[u8]) -> Vec<u8> {
    let mut clustering = Vec::new();
    clustering.extend_from_slice(&1u16.to_be_bytes());
    clustering.push(direction);
    clustering.extend_from_slice(&(edge_label.len() as u16).to_be_bytes());
    clustering.extend_from_slice(edge_label.as_bytes());
    clustering.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
    clustering.extend_from_slice(neighbor_id);
    clustering
}

/// Read the edge-label string out of an adjacency clustering key
/// (component 1 of the standard composite layout).
fn adjacency_edge_label(clustering: &[u8]) -> Option<String> {
    if clustering.len() < 5 {
        return None;
    }
    let dir_len = u16::from_be_bytes([clustering[0], clustering[1]]) as usize;
    let label_len_pos = 2 + dir_len;
    if label_len_pos + 2 > clustering.len() {
        return None;
    }
    let label_len =
        u16::from_be_bytes([clustering[label_len_pos], clustering[label_len_pos + 1]]) as usize;
    let label_start = label_len_pos + 2;
    if label_start + label_len > clustering.len() {
        return None;
    }
    std::str::from_utf8(&clustering[label_start..label_start + label_len])
        .ok()
        .map(str::to_string)
}

/// Build a pure-tombstone [`Mutation`] for one row in `keyspace.table`.
/// `clustering = None` deletes the whole partition; `Some` deletes one row.
/// Mirrors `StorageEngine`'s `BatchOp::Tombstone` lowering so the
/// DETACH-DELETE cascade and the engine batch path agree on representation.
fn tombstone_mutation(
    keyspace: &str,
    table: &str,
    key: &DecoratedKey,
    clustering: Option<Vec<u8>>,
    timestamp: i64,
) -> Mutation {
    let local_deletion_time = timestamp.div_euclid(1_000_000).clamp(0, u32::MAX as i64) as u32;
    let row = Row {
        clustering: clustering.unwrap_or_default(),
        cells: vec![],
        deletion: DeletionTime::new(timestamp, local_deletion_time),
        primary_key_liveness: LivenessInfo::NONE,
    };
    Mutation::new(
        keyspace.to_string(),
        table.to_string(),
        key.clone(),
        vec![row],
        timestamp,
    )
}

/// DETACH DELETE cascade for one vertex (URS-QEC-D01/D03/D07).
///
/// Enumerates ALL incident edges via the adjacency index (forward OUT +
/// backward IN), then collects tombstones for each edge row, its OWN
/// adjacency entry, the neighbor's mirror adjacency entry, and finally the
/// vertex row, into ONE batch applied atomically through the shared
/// `StorageEngine` multi-write primitive (`WritePath::write_batch` →
/// `write_atomic_batch`/`coordinate_logged_batch`). No new batch path is
/// invented; adjacency cleanup happens inside the delete, so a subsequent
/// scan finds zero references without reconciliation (URS-QEC-D03).
///
/// Returns the number of incident edges tombstoned.
async fn delete_with_adjacency(
    write_path: &WritePath,
    keyspace: &str,
    vertex_key: &DecoratedKey,
    vertex_table: &TableId,
    timestamp: i64,
    strategy: &ReplicationStrategy,
) -> Result<usize> {
    let vid = vertex_key.key.as_bytes().to_vec();
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table = TableId::new(&adj_ks, "adjacency");

    let mut mutations: Vec<Mutation> = Vec::new();
    let mut edges_deleted = 0usize;

    // Enumerate incident edges from the vertex's adjacency partition.
    if let Some(partition) = write_path.read(&adj_table, vertex_key).await? {
        for row in &partition.rows {
            if !row.deletion.is_live() {
                continue;
            }
            // Standard composite: direction byte sits at offset 2.
            if row.clustering.len() < 3 {
                continue;
            }
            let direction = row.clustering[2];
            let Some(neighbor_id) = extract_neighbor_id(&row.clustering, None) else {
                continue;
            };
            let Some(edge_label) = adjacency_edge_label(&row.clustering) else {
                continue;
            };
            // The edge table FQN is stored in the row's first cell value.
            let Some(edge_fqn) = row
                .cells
                .first()
                .and_then(|(_, cell)| cell.value.as_ref())
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
            else {
                continue;
            };
            let Some((edge_ks, edge_tbl)) = edge_fqn.split_once('.') else {
                continue;
            };

            // Reconstruct the edge's storage key: partition = source vertex,
            // clustering = target vertex.
            let (src_id, dst_id) = if direction == DIRECTION_OUT {
                (vid.clone(), neighbor_id.clone())
            } else {
                (neighbor_id.clone(), vid.clone())
            };
            let edge_src_key = DecoratedKey::new(PartitionKey::new(src_id));

            // (a) Tombstone the edge row itself.
            mutations.push(tombstone_mutation(
                edge_ks,
                edge_tbl,
                &edge_src_key,
                Some(dst_id),
                timestamp,
            ));

            // (b) Tombstone this vertex's adjacency entry (exact clustering).
            mutations.push(tombstone_mutation(
                &adj_ks,
                "adjacency",
                vertex_key,
                Some(row.clustering.clone()),
                timestamp,
            ));

            // (c) Tombstone the neighbor's mirror adjacency entry: flipped
            //     direction, neighbor = this vertex.
            let mirror_direction = if direction == DIRECTION_OUT {
                DIRECTION_IN
            } else {
                DIRECTION_OUT
            };
            let neighbor_key = DecoratedKey::new(PartitionKey::new(neighbor_id));
            let mirror_clustering = adjacency_clustering_bytes(mirror_direction, &edge_label, &vid);
            mutations.push(tombstone_mutation(
                &adj_ks,
                "adjacency",
                &neighbor_key,
                Some(mirror_clustering),
                timestamp,
            ));

            edges_deleted += 1;
        }
    }

    // Finally, tombstone the vertex row (whole-partition delete).
    mutations.push(tombstone_mutation(
        &vertex_table.keyspace,
        &vertex_table.table,
        vertex_key,
        None,
        timestamp,
    ));

    let rf = strategy.replication_factor();
    write_path
        .write_batch(mutations, graph_write_consistency(), rf)
        .await
        .map_err(GraphError::Storage)?;

    Ok(edges_deleted)
}

/// Probe the adjacency index for any **live** incident edge of `vertex_key`
/// (URS-QEC-D02). Returns `true` if the vertex still has at least one surviving
/// relationship in either direction. Read-only: performs no writes, so a plain
/// `DELETE n` can fail loud before tombstoning anything.
async fn vertex_has_incident_edges(
    write_path: &WritePath,
    keyspace: &str,
    vertex_key: &DecoratedKey,
) -> Result<bool> {
    let adj_ks = adjacency_keyspace_name(keyspace);
    let adj_table = TableId::new(&adj_ks, "adjacency");
    let Some(partition) = write_path.read(&adj_table, vertex_key).await? else {
        return Ok(false);
    };
    let has_live = partition.rows.iter().any(|row| {
        row.deletion.is_live()
            && row.clustering.len() >= 3
            && extract_neighbor_id(&row.clustering, None).is_some()
    });
    Ok(has_live)
}

/// Validate the plain-`DELETE n` constraint (URS-QEC-D02) for every matched
/// non-edge vertex BEFORE any tombstone is written: if any target node still
/// has surviving relationships, return a Neo4j-style constraint error so the
/// whole `DELETE` deletes nothing. Read-only.
async fn check_plain_delete_constraint(
    write_path: &WritePath,
    expand_result: &GraphResult,
    keyspace: &str,
    variables: &[String],
    schema: Option<&Schema>,
    variable_tables: &HashMap<String, String>,
) -> Result<()> {
    for row_values in &expand_result.rows {
        for (col_idx, col_name) in expand_result.columns.iter().enumerate() {
            if !variables.iter().any(|v| v == col_name) {
                continue;
            }
            let hex_id = match row_values.get(col_idx) {
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(serde_json::Value::Object(map)) => {
                    map.get("_id").and_then(|v| v.as_str()).map(String::from)
                }
                _ => None,
            };
            let Some(hex_id) = hex_id else { continue };
            let table_name = variable_tables
                .get(col_name)
                .map(String::as_str)
                .unwrap_or(col_name);
            // Edge tables have no adjacency partition keyed by the edge row;
            // the constraint only governs vertices.
            let is_edge_table = table_metadata_for(schema, keyspace, table_name)
                .as_ref()
                .is_some_and(|meta| {
                    meta.extensions
                        .get("graph.type")
                        .is_some_and(|graph_type| graph_type == "edge")
                });
            if is_edge_table {
                continue;
            }
            let key_bytes = hex::decode(&hex_id)
                .map_err(|e| GraphError::Internal(format!("invalid hex storage key: {e}")))?;
            let key = DecoratedKey::new(PartitionKey::new(key_bytes));
            if vertex_has_incident_edges(write_path, keyspace, &key).await? {
                return Err(GraphError::ConstraintViolation(format!(
                    "Cannot delete node {hex_id}, because it still has relationships. \
                     To delete this node, you must first delete its relationships, \
                     or use DETACH DELETE."
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_delete(
    write_path: &WritePath,
    expand: PhysicalPlan,
    keyspace: &str,
    variables: &[String],
    detach: bool,
    config: &GraphEngineConfig,
    virtual_tables: Option<&VirtualTableRegistry>,
    start: Instant,
    schema: Option<&Schema>,
    variable_tables: &HashMap<String, String>,
) -> Result<GraphResult> {
    let expand_result = Box::pin(execute(
        expand,
        write_path,
        keyspace,
        config,
        virtual_tables,
        schema,
    ))
    .await?;
    let mut stats = QueryStats::default();
    stats.vertices_read = expand_result.stats.vertices_read;
    stats.edges_read = expand_result.stats.edges_read;

    let timestamp = now_micros();
    let local_deletion_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    let strategy = graph_replication_strategy(schema, keyspace)?;

    // URS-QEC-D02: for a plain `DELETE n` (no DETACH), validate the
    // no-surviving-relationships constraint across ALL matched vertices BEFORE
    // writing any tombstone, so a failure deletes NOTHING even when several
    // vertices match. Probing is read-only.
    if !detach {
        check_plain_delete_constraint(
            write_path,
            &expand_result,
            keyspace,
            variables,
            schema,
            variable_tables,
        )
        .await?;
    }

    // For each matched vertex in the specified variables, write a tombstone.
    for row_values in &expand_result.rows {
        for (col_idx, col_name) in expand_result.columns.iter().enumerate() {
            if !variables.iter().any(|v| v == col_name) {
                continue;
            }

            // Extract the hex-encoded vertex ID from the expand result.
            // The expand returns either:
            //   - a JSON object with an `_id` field (Expr::Var bindings), or
            //   - a plain hex string (legacy / property projection).
            let hex_id = match row_values.get(col_idx) {
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(serde_json::Value::Object(map)) => {
                    map.get("_id").and_then(|v| v.as_str()).map(String::from)
                }
                _ => None,
            };

            if let Some(hex_id) = hex_id {
                // Use the resolved table name (e.g. "Person") rather than
                // the Cypher variable name (e.g. "n") so the tombstone is
                // written to the same table that the vertex lives in.
                let table_name = variable_tables
                    .get(col_name)
                    .map(String::as_str)
                    .unwrap_or(col_name);
                let table_id = TableId::new(keyspace, table_name);
                let table_meta = table_metadata_for(schema, keyspace, table_name);
                let is_edge_table = table_meta.as_ref().is_some_and(|meta| {
                    meta.extensions
                        .get("graph.type")
                        .is_some_and(|graph_type| graph_type == "edge")
                });
                let key_hex = if is_edge_table {
                    row_values
                        .get(col_idx)
                        .and_then(|value| value.as_object())
                        .and_then(|map| map.get("__ferrosa_key"))
                        .and_then(|value| value.as_str())
                        .unwrap_or(hex_id.as_str())
                } else {
                    hex_id.as_str()
                };
                let key_bytes = hex::decode(key_hex)
                    .map_err(|e| GraphError::Internal(format!("invalid hex storage key: {e}")))?;
                let key = DecoratedKey::new(PartitionKey::new(key_bytes));
                let clustering = if is_edge_table {
                    row_values
                        .get(col_idx)
                        .and_then(|value| value.as_object())
                        .and_then(|map| map.get("__ferrosa_clustering"))
                        .and_then(|value| value.as_str())
                        .map(hex::decode)
                        .transpose()
                        .map_err(|e| {
                            GraphError::Internal(format!("invalid hex storage clustering: {e}"))
                        })?
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };

                if detach && !is_edge_table {
                    // DETACH DELETE: cascade through the adjacency index,
                    // tombstoning every incident edge (both directions) and its
                    // adjacency entries together with the vertex, atomically
                    // (URS-QEC-D01/D03/D07).
                    let edges_deleted = delete_with_adjacency(
                        write_path, keyspace, &key, &table_id, timestamp, &strategy,
                    )
                    .await?;
                    stats.edges_deleted += edges_deleted;
                    stats.vertices_deleted += 1;
                    continue;
                }

                // Plain `DELETE n` constraint (URS-QEC-D02) was already
                // validated for every matched vertex by the pre-pass above, so
                // by here the write is guaranteed safe.

                // Write a row-level tombstone.
                let tombstone_row = Row {
                    clustering,
                    cells: vec![],
                    deletion: DeletionTime::new(timestamp, local_deletion_time),
                    primary_key_liveness: LivenessInfo::NONE,
                };

                write_path
                    .write(
                        &table_id,
                        &key,
                        tombstone_row,
                        timestamp,
                        graph_write_consistency(),
                        &strategy,
                    )
                    .await?;
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

/// Execute an Aggregate plan.
///
/// 1. Executes the inner plan to get all rows.
/// 2. Groups rows by group key values.
/// 3. For each group, creates accumulators, feeds values, builds output rows.
/// 4. Enforces max group count (FMEA F7).
#[allow(clippy::too_many_arguments)]
async fn execute_aggregate(
    write_path: &WritePath,
    keyspace: &str,
    inner: PhysicalPlan,
    group_keys: &[usize],
    projections: &[AggregateProjection],
    return_clause: &ReturnClause,
    config: &GraphEngineConfig,
    start: Instant,
    virtual_tables: Option<&VirtualTableRegistry>,
    schema: Option<&Schema>,
) -> Result<GraphResult> {
    // Step 1: Execute inner plan to get all rows.
    let inner_result = Box::pin(execute(
        inner,
        write_path,
        keyspace,
        config,
        virtual_tables,
        schema,
    ))
    .await?;
    check_timeout(start, config.query_timeout)?;

    let inner_columns = &inner_result.columns;
    let inner_rows = &inner_result.rows;

    // Step 2: Group rows by group key values.
    // Use a BTreeMap keyed by serialized group key for deterministic ordering.
    let mut groups: std::collections::BTreeMap<String, Vec<&Vec<serde_json::Value>>> =
        std::collections::BTreeMap::new();

    for row in inner_rows {
        // Build group key from the group_keys indices.
        let group_key_values: Vec<&serde_json::Value> = group_keys
            .iter()
            .map(|&idx| row.get(idx).unwrap_or(&serde_json::Value::Null))
            .collect();

        let group_key_str = serde_json::to_string(&group_key_values).unwrap_or_default();

        // Check group count limit (FMEA F7).
        if !groups.contains_key(&group_key_str) && groups.len() >= config.max_groups {
            return Err(GraphError::ResourceLimit(format!(
                "aggregation group count limit exceeded: {} (limit: {})",
                groups.len(),
                config.max_groups
            )));
        }

        groups.entry(group_key_str).or_default().push(row);
    }

    // If there are no group keys and no rows, produce a single group with empty rows
    // so aggregates like count(*) return 0 rather than no rows.
    if group_keys.is_empty() && groups.is_empty() {
        groups.insert(String::new(), Vec::new());
    }

    // Step 3: Build output rows.
    let columns = build_columns(return_clause);
    let mut result_rows = Vec::new();

    for group_rows in groups.values() {
        // Create accumulators for each aggregate projection.
        let mut accumulators: Vec<Option<Box<dyn Accumulator>>> = projections
            .iter()
            .map(|proj| match proj {
                AggregateProjection::GroupKey(_) => Ok(None),
                AggregateProjection::AggregateFunc { name, arg, .. } => {
                    let count_star = name == "count" && matches!(arg, Expr::Var(v) if v == "*");
                    create_accumulator(name, count_star, config.max_collect_size).map(Some)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let mut seen_distinct: Vec<std::collections::HashSet<String>> = projections
            .iter()
            .map(|_| std::collections::HashSet::new())
            .collect();

        // Feed rows into accumulators.
        for row in group_rows {
            for (proj_idx, proj) in projections.iter().enumerate() {
                if let AggregateProjection::AggregateFunc { arg, distinct, .. } = proj {
                    if let Some(ref mut acc) = accumulators[proj_idx] {
                        let value = eval_aggregate_arg(arg, row, inner_columns);
                        if *distinct {
                            let key = serde_json::to_string(&value).unwrap_or_default();
                            if !seen_distinct[proj_idx].insert(key) {
                                continue;
                            }
                        }
                        acc.accumulate(&value);
                    }
                }
            }
        }

        // Check collect size limit (FMEA F6).
        for acc in accumulators.iter().flatten() {
            if acc.name() == "collect" {
                let result = acc.finish();
                if let serde_json::Value::Array(arr) = &result {
                    if arr.len() >= config.max_collect_size {
                        return Err(GraphError::ResourceLimit(format!(
                            "collect() size limit exceeded: {} (limit: {})",
                            arr.len(),
                            config.max_collect_size,
                        )));
                    }
                }
            }
        }

        // Build the output row.
        let mut output_row = Vec::new();
        let first_row = group_rows.first();

        for (proj_idx, proj) in projections.iter().enumerate() {
            match proj {
                AggregateProjection::GroupKey(key_idx) => {
                    let col_idx = group_keys.get(*key_idx).copied().unwrap_or(0);
                    let value = first_row
                        .and_then(|r| r.get(col_idx))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    output_row.push(value);
                }
                AggregateProjection::AggregateFunc { .. } => {
                    let value = accumulators[proj_idx]
                        .as_ref()
                        .map(|a| a.finish())
                        .unwrap_or(serde_json::Value::Null);
                    output_row.push(value);
                }
            }
        }

        result_rows.push(output_row);
    }

    let mut stats = QueryStats::default();
    stats.vertices_read = inner_result.stats.vertices_read;
    stats.edges_read = inner_result.stats.edges_read;
    stats.execution_ms = start.elapsed().as_millis() as u64;

    Ok(GraphResult {
        columns,
        rows: result_rows,
        stats,
    })
}

/// Evaluate an aggregate function's argument expression against a row.
///
/// For `Var("*")` (count star), returns a non-null sentinel.
/// For `Property { var, name }`, looks up the column in the inner result.
/// For `Var(v)`, looks up the variable column.
fn eval_aggregate_arg(
    arg: &Expr,
    row: &[serde_json::Value],
    columns: &[String],
) -> serde_json::Value {
    match arg {
        Expr::Var(v) if v == "*" => serde_json::json!(1), // sentinel for count(*)
        Expr::Var(v) => {
            let col_idx = columns.iter().position(|c| c == v);
            col_idx
                .and_then(|idx| row.get(idx))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }
        Expr::Property { var, name } => {
            let col_name = format!("{var}.{name}");
            let col_idx = columns.iter().position(|c| c == &col_name);
            col_idx
                .and_then(|idx| row.get(idx))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }
        Expr::Distinct(inner) => eval_aggregate_arg(inner, row, columns),
        Expr::Function { name, args } => {
            // Nested function calls: for now just return the inner result column
            let col_name = format!("{}({})", name, if args.is_empty() { "*" } else { "?" });
            let col_idx = columns.iter().position(|c| c == &col_name);
            col_idx
                .and_then(|idx| row.get(idx))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }
        Expr::Literal(lit) => match lit {
            Literal::Integer(i) => serde_json::json!(i),
            Literal::Float(f) => serde_json::json!(f),
            Literal::String(s) => serde_json::json!(s),
            Literal::Bool(b) => serde_json::json!(b),
            Literal::Null => serde_json::Value::Null,
        },
        _ => serde_json::Value::Null,
    }
}

/// Sort rows by the specified ORDER BY columns.
pub fn sort_rows(
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
pub(super) fn compare_json_values(
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

    // Build a JSON object for each virtual row so eval_expr can project.
    let anchor_var = anchor.var.as_deref().unwrap_or("_anon");

    let mut rows = Vec::new();
    for vrow in &virtual_rows {
        if rows.len() >= config.max_result_rows {
            break;
        }

        // Construct a JSON object from virtual row cells, keyed by column name.
        let mut obj = serde_json::Map::new();
        for (idx, col_def) in vtable_columns.iter().enumerate() {
            if idx < vrow.cells.len() {
                obj.insert(col_def.name.clone(), cell_value_to_json(&vrow.cells[idx]));
            }
        }
        let row_json = serde_json::Value::Object(obj);
        let mut bindings = HashMap::new();
        bindings.insert(anchor_var.to_string(), row_json);

        let row: Vec<serde_json::Value> = return_clause
            .items
            .iter()
            .map(|item| eval::eval_expr(&item.expr, &bindings).unwrap_or(serde_json::Value::Null))
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
pub fn build_columns(return_clause: &ReturnClause) -> Vec<String> {
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
pub(super) fn expr_to_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Property { var, name } => format!("{var}.{name}"),
        Expr::Var(v) => v.clone(),
        Expr::Distinct(inner) => expr_to_column_name(inner),
        Expr::Function { name, args } => {
            let arg_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(expr_to_column_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{name}({arg_str})")
        }
        _ => "?".to_string(),
    }
}

/// Look up regular column names for a table from the schema.
///
/// Returns the names of regular (non-key, non-static) columns in the order
/// they appear in storage column order. Falls back to an empty vec when the
/// schema or table is unavailable.
pub fn column_names_for_table(schema: Option<&Schema>, keyspace: &str, table: &str) -> Vec<String> {
    let schema = match schema {
        Some(s) => s,
        None => return vec![],
    };
    let snap = schema.snapshot();
    snap.tables
        .get(&(keyspace.to_string(), table.to_string()))
        .map(|meta| {
            storage_cell_columns(meta)
                .into_iter()
                .map(|col| col.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn regular_column_index_for_property(
    schema: Option<&Schema>,
    keyspace: &str,
    table: &str,
    property: &str,
) -> Option<u16> {
    let schema = schema?;
    let snap = schema.snapshot();
    let meta = snap
        .tables
        .get(&(keyspace.to_string(), table.to_string()))?;

    meta.storage_column_index(property)
}

fn column_kind_for_property(
    schema: Option<&Schema>,
    keyspace: &str,
    table: &str,
    property: &str,
) -> Option<ferrosa_schema::metadata::column::ColumnKind> {
    let schema = schema?;
    let snap = schema.snapshot();
    let meta = snap
        .tables
        .get(&(keyspace.to_string(), table.to_string()))?;
    meta.columns.get(property).map(|col| col.kind)
}

fn column_type_for_property(
    schema: Option<&Schema>,
    keyspace: &str,
    table: &str,
    property: &str,
) -> Option<String> {
    let schema = schema?;
    let snap = schema.snapshot();
    let meta = snap
        .tables
        .get(&(keyspace.to_string(), table.to_string()))?;
    meta.columns
        .get(property)
        .map(|col| col.column_type.clone())
}

fn adjacency_direction(clustering: &[u8]) -> Option<u8> {
    if clustering.len() < 3 {
        return None;
    }
    let dir_len = u16::from_be_bytes([clustering[0], clustering[1]]) as usize;
    if dir_len != 1 || clustering.len() < 3 {
        return None;
    }
    Some(clustering[2])
}

fn adjacency_row_matches_direction(clustering: &[u8], direction: Direction) -> bool {
    match direction {
        Direction::Out => adjacency_direction(clustering) == Some(0),
        Direction::In => adjacency_direction(clustering) == Some(1),
        Direction::Both => true,
    }
}

/// Extract the neighbor ID from an adjacency row's clustering key.
///
/// Clustering format (standard composite, per SSTable writer's multi-column
/// composite parser): each component is `[u16 BE length][bytes]`.
///   0: [u16 1][1 byte direction]
///   1: [u16 label_len][edge_label bytes]
///   2: [u16 id_len][neighbor_id bytes]
///
/// If `expected_label` is Some, only returns the neighbor ID if the edge
/// label matches (case-insensitive).
pub fn extract_neighbor_id(clustering: &[u8], expected_label: Option<&str>) -> Option<Vec<u8>> {
    extract_neighbor_id_for_direction(clustering, expected_label, None)
}

fn label_matches_alternative(actual: &str, expected: &str) -> bool {
    expected
        .split('|')
        .any(|candidate| actual.eq_ignore_ascii_case(candidate.trim()))
}

fn expected_adjacency_direction(direction: Direction) -> Option<u8> {
    match direction {
        Direction::Out => Some(DIRECTION_OUT),
        Direction::In => Some(DIRECTION_IN),
        Direction::Both => None,
    }
}

fn extract_neighbor_id_for_direction(
    clustering: &[u8],
    expected_label: Option<&str>,
    expected_direction: Option<u8>,
) -> Option<Vec<u8>> {
    // Minimum: 2+1 (direction component) + 2 (label_len) + 2 (id_len) = 7
    if clustering.len() < 7 {
        return None;
    }

    // Component 0: direction. Skip [u16 len][1 byte].
    let dir_len = u16::from_be_bytes([clustering[0], clustering[1]]) as usize;
    if dir_len != 1 || 2 + dir_len > clustering.len() {
        return None;
    }
    let direction = clustering[2];
    if expected_direction.is_some_and(|expected| direction != expected) {
        return None;
    }
    let mut pos = 2 + dir_len;
    if pos + 2 > clustering.len() {
        return None;
    }

    // Component 1: edge_label.
    let label_len = u16::from_be_bytes([clustering[pos], clustering[pos + 1]]) as usize;
    pos += 2;
    if pos + label_len > clustering.len() {
        return None;
    }
    let label_bytes = &clustering[pos..pos + label_len];
    pos += label_len;

    if let Some(expected) = expected_label {
        let label_str = std::str::from_utf8(label_bytes).ok()?;
        if !label_matches_alternative(label_str, expected) {
            return None;
        }
    }

    // Component 2: neighbor_id.
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

/// Returns `true` if a partition has no live data — i.e., it is either
/// partition-level deleted or every row has been tombstoned (row-level
/// deletion with no surviving cells and no live static row). Such
/// partitions represent deleted vertices that have not yet been purged
/// by compaction and must be skipped during graph traversal.
fn is_partition_dead(partition: &ferrosa_sstable::types::Partition) -> bool {
    // Partition-level deletion with no surviving rows.
    if !partition.deletion.is_live() && partition.rows.is_empty() {
        return partition
            .static_row
            .as_ref()
            .is_none_or(|sr| sr.cells.is_empty());
    }

    // If there are no rows at all and no static row, the partition is empty
    // (but not necessarily deleted — could be a key-only entry). Treat as dead.
    if partition.rows.is_empty() && partition.static_row.is_none() {
        return true;
    }

    // Check whether every row is tombstoned: the row has a deletion marker
    // and no surviving cells.
    let all_rows_dead = partition
        .rows
        .iter()
        .all(|row| !row.deletion.is_live() && row.cells.is_empty());

    if !all_rows_dead {
        return false;
    }

    // If all rows are dead, also check the static row.
    partition
        .static_row
        .as_ref()
        .is_none_or(|sr| sr.cells.is_empty())
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

    /// `ORDER BY` with `DISTINCT` must reach the spilling sorter.
    ///
    /// DISTINCT was excluded from this path because the buffered tail re-sorted
    /// by debug string afterwards, so streaming would have changed the observed
    /// order. That re-sort was the bug (it discarded the ORDER BY); with it
    /// gone, excluding DISTINCT only forces the whole result into memory for no
    /// behavioural reason.
    ///
    /// Asserted on the source because the regression is a routing one: putting
    /// the `!distinct` guard back still returns correct rows, merely
    /// materialised, so no result-shaped assertion would catch it.
    #[test]
    fn distinct_is_not_excluded_from_the_streaming_order_by_path() {
        let src = include_str!("expand.rs");
        let body = src
            .split("ORDER BY (with or without DISTINCT) is a pipeline breaker")
            .nth(1)
            .expect("the streaming ORDER BY arm must exist");
        let arm = &body[..body
            .find("// Fallback:")
            .expect("the fallback comment must exist")];

        assert!(
            !arm.contains("if !parts.return_clause.distinct"),
            "DISTINCT must not be routed away from the spilling sorter: the \
             buffered tail no longer reorders, so there is nothing left to \
             preserve by materialising"
        );
        assert!(
            arm.contains("dedup_stream"),
            "the streaming arm must apply DISTINCT to the sorted stream"
        );
    }

    /// DISTINCT must not discard the ORDER BY the query asked for.
    ///
    /// The buffered tail sorted by debug representation and then `dedup()`ed,
    /// because `Vec::dedup` only removes consecutive duplicates. That sort ran
    /// AFTER the ORDER BY sort, so `RETURN DISTINCT x ORDER BY x DESC` came
    /// back in debug-string ascending order — the requested order silently
    /// replaced, with no error and nothing in the result to show it.
    #[test]
    fn distinct_dedups_without_reordering() {
        use serde_json::json;

        // Already in the order an `ORDER BY n DESC` would have produced, with
        // duplicates that are not adjacent.
        let mut rows: Vec<super::super::stream::RowVals> = vec![
            vec![json!(30)],
            vec![json!(20)],
            vec![json!(30)],
            vec![json!(10)],
            vec![json!(20)],
        ];

        super::dedup_preserving_order(&mut rows);

        let got: Vec<i64> = rows.iter().map(|r| r[0].as_i64().unwrap()).collect();
        assert_eq!(
            got,
            vec![30, 20, 10],
            "duplicates must go and the descending order must stay; sorting to \
             make duplicates adjacent throws away the ORDER BY"
        );
    }

    /// `WcoJoin` must reach the transport as a stream, not as a collected Vec.
    ///
    /// Its accumulator already spilled to disk while accumulating, and then
    /// `finish_rows()` pulled every row back into memory to build a
    /// `GraphResult` — so the spill was undone at the last step and a large
    /// result still landed in RAM. Routing the plan through the streaming
    /// dispatch is what keeps the spill alive to the consumer.
    ///
    /// This asserts the routing, which is the part that regresses silently: if
    /// `WcoJoin` falls back to `other =>` again it still returns correct rows,
    /// just materialised, and no result-shaped assertion would notice.
    #[test]
    fn wco_join_is_routed_to_the_streaming_dispatch_not_the_buffered_fallback() {
        let src = include_str!("expand.rs");
        let dispatch = src
            .split("fn execute_streaming_inner")
            .nth(1)
            .expect("execute_streaming_inner must exist");
        let dispatch = &dispatch[..dispatch
            .find("// --- everything else")
            .expect("the buffered fallback arm must exist")];

        assert!(
            dispatch.contains("PhysicalPlan::WcoJoin"),
            "WcoJoin must be handled BEFORE the buffered fallback; falling \
             through to `other =>` collects the spilled result back into a Vec"
        );
        assert!(
            dispatch.contains("execute_wco_join_streaming"),
            "the WcoJoin arm must call the streaming form, not the collecting \
             wrapper"
        );
    }
    use super::*;

    use crate::adjacency::observer::make_adjacency_mutation;
    use crate::adjacency::schema::adjacency_table_metadata;
    use indexmap::IndexMap;
    use std::sync::Arc;

    use ferrosa_common::schema::TableSchema;
    use ferrosa_common::CellValue;
    use ferrosa_common::DataType;
    use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};
    use ferrosa_schema::virtual_table::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
    };
    use std::collections::{HashMap, HashSet};

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

    #[test]
    fn endpoint_resolution_prefers_entity_id_over_scope_ids_for_entity_edges() {
        let tenant = Expr::Literal(Literal::String(
            "11111111-1111-1111-1111-111111111111".to_string(),
        ));
        let session = Expr::Literal(Literal::String(
            "22222222-2222-2222-2222-222222222222".to_string(),
        ));
        let entity = Expr::Literal(Literal::String(
            "33333333-3333-3333-3333-333333333333".to_string(),
        ));
        let endpoint_props = vec![
            ("tenant_id".to_string(), tenant),
            ("session_id".to_string(), session),
            ("entity_id".to_string(), entity.clone()),
        ];

        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "entity_a", "uuid"),
            Some(entity.clone())
        );
        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "src_id", "uuid"),
            Some(entity)
        );
    }

    #[test]
    fn endpoint_resolution_prefers_fold_and_event_aliases_over_scope_ids() {
        let tenant = Expr::Literal(Literal::String(
            "11111111-1111-1111-1111-111111111111".to_string(),
        ));
        let session = Expr::Literal(Literal::String(
            "22222222-2222-2222-2222-222222222222".to_string(),
        ));
        let entity = Expr::Literal(Literal::String(
            "33333333-3333-3333-3333-333333333333".to_string(),
        ));
        let fold = Expr::Literal(Literal::String(
            "44444444-4444-4444-4444-444444444444".to_string(),
        ));
        let event = Expr::Literal(Literal::String(
            "55555555-5555-5555-5555-555555555555".to_string(),
        ));
        let endpoint_props = vec![
            ("tenant_id".to_string(), tenant),
            ("session_id".to_string(), session),
            ("entity_id".to_string(), entity.clone()),
            ("fold_id".to_string(), fold.clone()),
            ("event_id".to_string(), event.clone()),
        ];

        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "source_fold_id", "uuid"),
            Some(fold.clone())
        );
        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "target_fold_id", "uuid"),
            Some(fold)
        );
        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "new_event_id", "uuid"),
            Some(event.clone())
        );
        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "old_event_id", "uuid"),
            Some(event)
        );
    }

    #[test]
    fn endpoint_resolution_rejects_scope_ids_and_invalid_generic_ids() {
        let tenant = Expr::Literal(Literal::String(
            "11111111-1111-1111-1111-111111111111".to_string(),
        ));
        let session = Expr::Literal(Literal::String(
            "22222222-2222-2222-2222-222222222222".to_string(),
        ));
        let invalid = Expr::Literal(Literal::String("not-a-uuid".to_string()));
        let endpoint_props = vec![
            ("tenant_id".to_string(), tenant),
            ("session_id".to_string(), session),
            ("foo_id".to_string(), invalid),
        ];

        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "src_id", "uuid"),
            None
        );
        assert_eq!(
            resolve_endpoint_property_expr(Some(&endpoint_props), "entity_a", "uuid"),
            None
        );
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
                props: vec![],
                filters: vec![],
            },
            hops: vec![],
            optional_hops: vec![],
            post_filters: vec![],
            with_pipeline: None,
            return_clause: ReturnClause {
                distinct: false,
                items,
                order_by: vec![],
                limit: None,
            },
        }
    }

    fn test_column(
        name: &str,
        kind: ColumnKind,
        position: i32,
        column_type: &str,
        clustering_order: ClusteringOrder,
    ) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position,
            column_type: column_type.to_string(),
            clustering_order,
            mask: None,
        }
    }

    fn scoped_entity_meta() -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert(
            "tenant_id".to_string(),
            test_column(
                "tenant_id",
                ColumnKind::PartitionKey,
                0,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "session_id".to_string(),
            test_column(
                "session_id",
                ColumnKind::PartitionKey,
                1,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "entity_id".to_string(),
            test_column(
                "entity_id",
                ColumnKind::Clustering,
                0,
                "uuid",
                ClusteringOrder::Asc,
            ),
        );
        columns.insert(
            "name".to_string(),
            test_column(
                "name",
                ColumnKind::Regular,
                0,
                "text",
                ClusteringOrder::None,
            ),
        );
        TableMetadata {
            keyspace: "agent_memory".to_string(),
            name: "entity_store".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
            clustering_key: vec![("entity_id".to_string(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags: HashSet::from([TableFlag::Compound]),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    fn scoped_typed_edge_meta() -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert(
            "tenant_id".to_string(),
            test_column(
                "tenant_id",
                ColumnKind::PartitionKey,
                0,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "session_id".to_string(),
            test_column(
                "session_id",
                ColumnKind::PartitionKey,
                1,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "src_id".to_string(),
            test_column(
                "src_id",
                ColumnKind::Clustering,
                0,
                "uuid",
                ClusteringOrder::Asc,
            ),
        );
        columns.insert(
            "edge_type".to_string(),
            test_column(
                "edge_type",
                ColumnKind::Clustering,
                1,
                "text",
                ClusteringOrder::Asc,
            ),
        );
        columns.insert(
            "dst_id".to_string(),
            test_column(
                "dst_id",
                ColumnKind::Clustering,
                2,
                "uuid",
                ClusteringOrder::Asc,
            ),
        );
        columns.insert(
            "weight".to_string(),
            test_column(
                "weight",
                ColumnKind::Regular,
                0,
                "double",
                ClusteringOrder::None,
            ),
        );
        TableMetadata {
            keyspace: "agent_memory".to_string(),
            name: "typed_edges".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
            clustering_key: vec![
                ("src_id".to_string(), ClusteringOrder::Asc),
                ("edge_type".to_string(), ClusteringOrder::Asc),
                ("dst_id".to_string(), ClusteringOrder::Asc),
            ],
            params: TableParams::default(),
            flags: HashSet::from([TableFlag::Compound]),
            extensions: HashMap::from([
                ("graph.source".to_string(), "src_id".to_string()),
                ("graph.target".to_string(), "dst_id".to_string()),
            ]),
            is_system: false,
        }
    }

    fn register_scoped_entity_storage_schema(storage: &ferrosa_storage::StorageEngine) {
        storage
            .register_table(TableSchema {
                keyspace: "agent_memory".to_string(),
                table: "entity_store".to_string(),
                key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: HashMap::new(),
            })
            .unwrap();
    }

    #[test]
    fn build_direct_lookup_shape_uses_scoped_bindings_for_vertex_rows() {
        let meta = scoped_entity_meta();
        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let entity_id = uuid::Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

        let mut bindings = HashMap::new();
        bindings.insert(
            "a".to_string(),
            serde_json::json!({
                "tenant_id": tenant_id.to_string(),
                "session_id": session_id.to_string()
            }),
        );

        let props = vec![(
            "entity_id".to_string(),
            Expr::Literal(Literal::String(entity_id.to_string())),
        )];

        let (key, clustering) =
            build_direct_lookup_shape(&meta, &bindings, &props, &HashMap::new())
                .unwrap()
                .expect("scoped bindings should produce a direct vertex lookup");

        assert_eq!(
            key.key.as_bytes(),
            encode_partition_components(&[
                tenant_id.as_bytes().to_vec(),
                session_id.as_bytes().to_vec(),
            ])
        );
        assert_eq!(clustering, entity_id.as_bytes().to_vec());
    }

    #[test]
    fn build_neighbor_vertex_lookup_shape_uses_edge_scope_and_neighbor_id() {
        let meta = scoped_entity_meta();
        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let entity_id = uuid::Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

        let mut bindings = HashMap::new();
        bindings.insert(
            "_edge".to_string(),
            serde_json::json!({
                "tenant_id": tenant_id.to_string(),
                "session_id": session_id.to_string()
            }),
        );

        let (key, clustering) =
            build_neighbor_vertex_lookup_shape(&meta, &bindings, &[], entity_id.as_bytes())
                .unwrap()
                .expect("edge scope plus neighbor id should produce a direct vertex lookup");

        assert_eq!(
            key.key.as_bytes(),
            encode_partition_components(&[
                tenant_id.as_bytes().to_vec(),
                session_id.as_bytes().to_vec(),
            ])
        );
        assert_eq!(clustering, entity_id.as_bytes().to_vec());
    }

    #[test]
    fn build_direct_lookup_shape_uses_scope_and_edge_components_for_edge_rows() {
        let meta = scoped_typed_edge_meta();
        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let src_id = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let dst_id = uuid::Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

        let mut bindings = HashMap::new();
        bindings.insert(
            "a".to_string(),
            serde_json::json!({
                "tenant_id": tenant_id.to_string(),
                "session_id": session_id.to_string()
            }),
        );

        let props = vec![(
            "edge_type".to_string(),
            Expr::Literal(Literal::String("related_to".to_string())),
        )];
        let direct = HashMap::from([
            ("src_id".to_string(), src_id.as_bytes().to_vec()),
            ("dst_id".to_string(), dst_id.as_bytes().to_vec()),
        ]);

        let (key, clustering) = build_direct_lookup_shape(&meta, &bindings, &props, &direct)
            .unwrap()
            .expect("scoped bindings should produce a direct edge lookup");

        assert_eq!(
            key.key.as_bytes(),
            encode_partition_components(&[
                tenant_id.as_bytes().to_vec(),
                session_id.as_bytes().to_vec(),
            ])
        );
        assert_eq!(
            clustering,
            encode_clustering_components(&[
                src_id.as_bytes().to_vec(),
                b"related_to".to_vec(),
                dst_id.as_bytes().to_vec(),
            ])
        );
    }

    #[tokio::test]
    async fn infer_schema_aware_merge_shape_uses_bound_vertex_rows_for_scoped_edge_merges() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        register_scoped_entity_storage_schema(&storage);

        let schema = Arc::new(
            Schema::new(ferrosa_schema::SchemaConfig {
                hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: ferrosa_schema::RateLimitConfig::default(),
                audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::DeploymentMode::Development,
            })
            .unwrap(),
        );
        let auth = ferrosa_schema::auth::role::AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        schema
            .create_keyspace(
                ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                    name: "agent_memory".to_string(),
                    durable_writes: true,
                    replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: HashMap::from([(
                            "replication_factor".to_string(),
                            "1".to_string(),
                        )]),
                    },
                },
                &auth,
            )
            .unwrap();
        schema.create_table(scoped_entity_meta(), &auth).unwrap();
        schema
            .create_table(scoped_typed_edge_meta(), &auth)
            .unwrap();

        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let src_id = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let dst_id = uuid::Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();
        let write_path = ferrosa_cluster::write_path::WritePath::direct(storage);

        let op = MergeOp {
            var: Some("r".to_string()),
            table: crate::planner::ResolvedTable {
                keyspace: "agent_memory".to_string(),
                table: "typed_edges".to_string(),
                label: "TYPED_EDGE".to_string(),
                graph_type: "edge".to_string(),
            },
            match_props: vec![(
                "edge_type".to_string(),
                Expr::Literal(Literal::String("related_to".to_string())),
            )],
            create_props: vec![],
            src_match_props: Some(vec![(
                "entity_id".to_string(),
                Expr::Literal(Literal::String(src_id.to_string())),
            )]),
            src_var: Some("a".to_string()),
            dst_match_props: Some(vec![(
                "entity_id".to_string(),
                Expr::Literal(Literal::String(dst_id.to_string())),
            )]),
            dst_var: Some("b".to_string()),
        };

        let bound_rows = HashMap::from([
            (
                "a".to_string(),
                serde_json::json!({
                    "tenant_id": tenant_id.to_string(),
                    "session_id": session_id.to_string(),
                    "entity_id": src_id.to_string(),
                }),
            ),
            (
                "b".to_string(),
                serde_json::json!({
                    "tenant_id": tenant_id.to_string(),
                    "session_id": session_id.to_string(),
                    "entity_id": dst_id.to_string(),
                }),
            ),
        ]);

        let shape = infer_schema_aware_merge_shape(
            &write_path,
            &op,
            &[("weight".to_string(), Expr::Literal(Literal::Float(1.0)))],
            &schema,
            1000,
            &bound_rows,
        )
        .await
        .unwrap()
        .expect("bound node rows should infer the scoped typed edge shape");

        let partition_key = encode_partition_components(&[
            tenant_id.as_bytes().to_vec(),
            session_id.as_bytes().to_vec(),
        ]);
        assert!(
            shape.key.key.as_bytes() == partition_key,
            "inferred edge partition key should reuse tenant/session from bound vertices"
        );
        assert_eq!(
            shape.clustering,
            encode_clustering_components(&[
                src_id.as_bytes().to_vec(),
                b"related_to".to_vec(),
                dst_id.as_bytes().to_vec(),
            ]),
        );
    }

    #[tokio::test]
    async fn execute_merge_writes_adjacency_rows_without_registered_observer() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = Arc::new(
            ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: ferrosa_schema::RateLimitConfig::default(),
                audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::DeploymentMode::Development,
            })
            .unwrap(),
        );
        let auth = ferrosa_schema::auth::role::AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        let user_ks = ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: "agent_memory".to_string(),
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };
        schema.create_keyspace(user_ks.clone(), &auth).unwrap();
        schema
            .create_keyspace(
                ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                    name: adjacency_keyspace_name("agent_memory"),
                    ..user_ks
                },
                &auth,
            )
            .unwrap();

        let mut entity_meta = scoped_entity_meta();
        entity_meta
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        entity_meta
            .extensions
            .insert("graph.label".to_string(), "Entity".to_string());
        let mut edge_meta = scoped_typed_edge_meta();
        edge_meta
            .extensions
            .insert("graph.type".to_string(), "edge".to_string());
        edge_meta
            .extensions
            .insert("graph.label".to_string(), "TYPED_EDGE".to_string());
        edge_meta
            .extensions
            .insert("graph.source_label".to_string(), "Entity".to_string());
        edge_meta
            .extensions
            .insert("graph.target_label".to_string(), "Entity".to_string());
        let adjacency_meta = adjacency_table_metadata("agent_memory");
        schema.create_table(entity_meta.clone(), &auth).unwrap();
        schema.create_table(edge_meta.clone(), &auth).unwrap();
        schema.create_table(adjacency_meta.clone(), &auth).unwrap();
        storage
            .register_table(entity_meta.to_storage_schema())
            .unwrap();
        storage
            .register_table(edge_meta.to_storage_schema())
            .unwrap();
        storage
            .register_table(adjacency_meta.to_storage_schema())
            .unwrap();

        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let src_id = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let dst_id = uuid::Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();

        let entity_tid = TableId::new("agent_memory", "entity_store");
        let partition_key = encode_partition_components(&[
            tenant_id.as_bytes().to_vec(),
            session_id.as_bytes().to_vec(),
        ]);
        let src_row = Row {
            clustering: encode_clustering_components(&[src_id.as_bytes().to_vec()]),
            cells: vec![(0, CellValue::live(b"src".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        let dst_row = Row {
            clustering: encode_clustering_components(&[dst_id.as_bytes().to_vec()]),
            cells: vec![(0, CellValue::live(b"dst".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        storage
            .write(
                &entity_tid,
                &DecoratedKey::new(PartitionKey::new(partition_key.clone())),
                src_row,
                1000,
            )
            .unwrap();
        storage
            .write(
                &entity_tid,
                &DecoratedKey::new(PartitionKey::new(partition_key)),
                dst_row,
                1000,
            )
            .unwrap();

        let merges = vec![
            MergeOp {
                var: Some("a".to_string()),
                table: crate::planner::ResolvedTable {
                    keyspace: "agent_memory".to_string(),
                    table: "entity_store".to_string(),
                    label: "Entity".to_string(),
                    graph_type: "vertex".to_string(),
                },
                match_props: vec![(
                    "entity_id".to_string(),
                    Expr::Literal(Literal::String(src_id.to_string())),
                )],
                create_props: vec![],
                src_match_props: None,
                src_var: None,
                dst_match_props: None,
                dst_var: None,
            },
            MergeOp {
                var: Some("b".to_string()),
                table: crate::planner::ResolvedTable {
                    keyspace: "agent_memory".to_string(),
                    table: "entity_store".to_string(),
                    label: "Entity".to_string(),
                    graph_type: "vertex".to_string(),
                },
                match_props: vec![(
                    "entity_id".to_string(),
                    Expr::Literal(Literal::String(dst_id.to_string())),
                )],
                create_props: vec![],
                src_match_props: None,
                src_var: None,
                dst_match_props: None,
                dst_var: None,
            },
            MergeOp {
                var: Some("r".to_string()),
                table: crate::planner::ResolvedTable {
                    keyspace: "agent_memory".to_string(),
                    table: "typed_edges".to_string(),
                    label: "TYPED_EDGE".to_string(),
                    graph_type: "edge".to_string(),
                },
                match_props: vec![(
                    "edge_type".to_string(),
                    Expr::Literal(Literal::String("implements".to_string())),
                )],
                create_props: vec![],
                src_match_props: Some(vec![(
                    "entity_id".to_string(),
                    Expr::Literal(Literal::String(src_id.to_string())),
                )]),
                src_var: Some("a".to_string()),
                dst_match_props: Some(vec![(
                    "entity_id".to_string(),
                    Expr::Literal(Literal::String(dst_id.to_string())),
                )]),
                dst_var: Some("b".to_string()),
            },
        ];

        let write_path = ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage));
        execute_merge(
            &write_path,
            &merges,
            &[(
                "r".to_string(),
                "weight".to_string(),
                Expr::Literal(Literal::Float(1.0)),
            )],
            None,
            &GraphEngineConfig::default(),
            Instant::now(),
            Some(&schema),
        )
        .await
        .unwrap();

        let adj_tid = TableId::new(adjacency_keyspace_name("agent_memory"), "adjacency");
        let src_adj = write_path
            .read(
                &adj_tid,
                &DecoratedKey::new(PartitionKey::new(src_id.as_bytes().to_vec())),
            )
            .await
            .unwrap()
            .expect("source adjacency partition should exist immediately after merge");
        let dst_adj = write_path
            .read(
                &adj_tid,
                &DecoratedKey::new(PartitionKey::new(dst_id.as_bytes().to_vec())),
            )
            .await
            .unwrap()
            .expect("target adjacency partition should exist immediately after merge");

        let expected_out = make_adjacency_mutation(
            &adjacency_keyspace_name("agent_memory"),
            src_id.as_bytes(),
            crate::adjacency::schema::DIRECTION_OUT,
            "TYPED_EDGE",
            dst_id.as_bytes(),
            "agent_memory.typed_edges",
            0,
        );
        let expected_in = make_adjacency_mutation(
            &adjacency_keyspace_name("agent_memory"),
            dst_id.as_bytes(),
            crate::adjacency::schema::DIRECTION_IN,
            "TYPED_EDGE",
            src_id.as_bytes(),
            "agent_memory.typed_edges",
            0,
        );

        assert!(
            src_adj
                .rows
                .iter()
                .any(|row| row.clustering == expected_out.rows[0].clustering),
            "source adjacency should contain OUT entry for merged edge"
        );
        assert!(
            dst_adj
                .rows
                .iter()
                .any(|row| row.clustering == expected_in.rows[0].clustering),
            "target adjacency should contain IN entry for merged edge"
        );
    }

    /// TDD regression for the 2026-05-17 smoke "graph readback: KNOWN
    /// ISSUE rows=[]" failure mode.
    ///
    /// `MERGE (a:Entity {tenant_id, session_id, entity_id})` on a
    /// scoped vertex table (PK = (tenant_id, session_id), CK =
    /// entity_id, no regular cells in the MERGE) was a NO-OP because
    /// `merge_can_bind_schema_key_only_vertex_without_read` returned
    /// true → `skip_partition_read = true` → `row_exists` was hard-
    /// coded to true → the create arm never ran → the entity row was
    /// never persisted to entity_store. The MERGE reported "merged 1
    /// vertices" but `vertices_written=0` and CQL readback returned
    /// nothing.
    ///
    /// Subsequent `MATCH (a:Entity {entity_id: ...})` then failed
    /// because the anchor scan over entity_store had no row to bind
    /// to — even though the typed_edges row + adjacency entries had
    /// been written successfully.
    ///
    /// This test asserts that a key-only vertex MERGE persists the
    /// liveness row to the scoped vertex table.
    #[tokio::test]
    async fn execute_merge_key_only_scoped_vertex_persists_liveness_row() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        let schema = Arc::new(
            ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: ferrosa_schema::RateLimitConfig::default(),
                audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::DeploymentMode::Development,
            })
            .unwrap(),
        );
        let auth = ferrosa_schema::auth::role::AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        let user_ks = ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: "agent_memory".to_string(),
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };
        schema.create_keyspace(user_ks.clone(), &auth).unwrap();
        schema
            .create_keyspace(
                ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                    name: adjacency_keyspace_name("agent_memory"),
                    ..user_ks
                },
                &auth,
            )
            .unwrap();

        let mut entity_meta = scoped_entity_meta();
        entity_meta
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        entity_meta
            .extensions
            .insert("graph.label".to_string(), "Entity".to_string());
        let adjacency_meta = adjacency_table_metadata("agent_memory");
        schema.create_table(entity_meta.clone(), &auth).unwrap();
        schema.create_table(adjacency_meta.clone(), &auth).unwrap();
        storage
            .register_table(entity_meta.to_storage_schema())
            .unwrap();
        storage
            .register_table(adjacency_meta.to_storage_schema())
            .unwrap();

        let tenant_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let session_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let entity_id = uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

        // Crucially: do NOT pre-write the entity row. The MERGE
        // should create it.
        let merges = vec![MergeOp {
            var: Some("a".to_string()),
            table: crate::planner::ResolvedTable {
                keyspace: "agent_memory".to_string(),
                table: "entity_store".to_string(),
                label: "Entity".to_string(),
                graph_type: "vertex".to_string(),
            },
            match_props: vec![
                (
                    "tenant_id".to_string(),
                    Expr::Literal(Literal::String(tenant_id.to_string())),
                ),
                (
                    "session_id".to_string(),
                    Expr::Literal(Literal::String(session_id.to_string())),
                ),
                (
                    "entity_id".to_string(),
                    Expr::Literal(Literal::String(entity_id.to_string())),
                ),
            ],
            create_props: vec![],
            src_match_props: None,
            src_var: None,
            dst_match_props: None,
            dst_var: None,
        }];

        let write_path = ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage));
        execute_merge(
            &write_path,
            &merges,
            &[],
            None,
            &GraphEngineConfig::default(),
            Instant::now(),
            Some(&schema),
        )
        .await
        .unwrap();

        // The MERGE must persist a row in entity_store: PK =
        // (tenant_id, session_id), CK = entity_id. A subsequent
        // MATCH over entity_store needs to find this row by
        // scanning the partition and matching the clustering key.
        let entity_tid = TableId::new("agent_memory", "entity_store");
        let partition_key = encode_partition_components(&[
            tenant_id.as_bytes().to_vec(),
            session_id.as_bytes().to_vec(),
        ]);
        let expected_clustering = encode_clustering_components(&[entity_id.as_bytes().to_vec()]);

        let partition = write_path
            .read(
                &entity_tid,
                &DecoratedKey::new(PartitionKey::new(partition_key)),
            )
            .await
            .unwrap()
            .expect(
                "MERGE on key-only scoped vertex must persist the liveness row \
                 — without this, MATCH on a freshly-merged entity_id finds nothing",
            );

        let row = partition
            .rows
            .iter()
            .find(|row| row.clustering == expected_clustering)
            .expect(
                "merged row must exist with the requested clustering key \
                 (entity_id) — the create arm did not run",
            );

        // Liveness row: no cell payloads required, just the
        // primary_key_liveness marker.
        assert!(
            row.primary_key_liveness.timestamp > 0,
            "merged liveness row must have a non-zero timestamp"
        );
    }

    #[tokio::test]
    async fn execute_virtual_table_anchor_returns_rows() {
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
        let wp = WritePath::direct(storage);
        let result = execute(
            plan,
            &wp,
            "system_observability",
            &config,
            Some(&registry),
            None,
        )
        .await
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

    #[tokio::test]
    async fn execute_virtual_table_missing_column_returns_null() {
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
        let wp = WritePath::direct(storage);
        let result = execute(
            plan,
            &wp,
            "system_observability",
            &config,
            Some(&registry),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            serde_json::Value::String("10.0.0.1".to_string())
        );
        assert_eq!(result.rows[0][1], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn execute_virtual_table_tombstone_returns_null() {
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
        let wp = WritePath::direct(storage);
        let result = execute(
            plan,
            &wp,
            "system_observability",
            &config,
            Some(&registry),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn execute_virtual_table_empty_returns_no_rows() {
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
        let wp = WritePath::direct(storage);
        let result = execute(
            plan,
            &wp,
            "system_observability",
            &config,
            Some(&registry),
            None,
        )
        .await
        .unwrap();

        assert!(result.rows.is_empty());
        assert_eq!(result.stats.vertices_read, 0);
    }

    #[tokio::test]
    async fn execute_non_virtual_table_falls_through_to_storage() {
        // When virtual tables registry exists but table is NOT registered,
        // execution should fall through to the normal storage path.
        let registry = VirtualTableRegistry::new();
        // Don't register any tables.

        let plan = virtual_anchor_plan("social", "person_v", &[("n", "name")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        // This should succeed (empty result from storage, not error).
        let wp = WritePath::direct(storage);
        let result = execute(plan, &wp, "social", &config, Some(&registry), None)
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn execute_without_virtual_registry_falls_through() {
        // When no virtual table registry is provided, execute normally.
        let plan = virtual_anchor_plan("social", "person_v", &[("n", "name")]);

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let wp = WritePath::direct(storage);
        let result = execute(plan, &wp, "social", &config, None, None)
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn execute_virtual_table_respects_max_rows() {
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

        let wp = WritePath::direct(storage);
        let result = execute(
            plan,
            &wp,
            "system_observability",
            &config,
            Some(&registry),
            None,
        )
        .await
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
    fn test_storage_engine(dir: &std::path::Path) -> Arc<ferrosa_storage::StorageEngine> {
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
        };

        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
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
        Arc::new(ferrosa_storage::StorageEngine::new(config, None).unwrap())
    }

    fn test_schema() -> Schema {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
            RateLimitConfig, SchemaConfig, TestAuditSink,
        };

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

    #[test]
    fn graph_replication_strategy_defaults_to_rf1_without_schema() {
        let strategy = graph_replication_strategy(None, "agent_memory").unwrap();
        match strategy {
            ReplicationStrategy::Simple { replication_factor } => {
                assert_eq!(replication_factor, 1);
            }
            other => panic!("expected SimpleStrategy fallback, got {other:?}"),
        }
    }

    #[test]
    fn graph_replication_strategy_uses_keyspace_replication() {
        use ferrosa_schema::metadata::keyspace::ReplicationParams;
        use ferrosa_schema::KeyspaceMetadata;

        let schema = test_schema();
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: "agent_memory".into(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: [("replication_factor".into(), "3".into())].into(),
                },
            })
            .unwrap();

        let strategy = graph_replication_strategy(Some(&schema), "agent_memory").unwrap();
        match strategy {
            ReplicationStrategy::Simple { replication_factor } => {
                assert_eq!(replication_factor, 3);
            }
            other => panic!("expected SimpleStrategy, got {other:?}"),
        }
    }

    #[test]
    fn extract_neighbor_id_basic() {
        // Build a clustering key: direction=0, label="KNOWS", neighbor_id=[1,2,3]
        let label = b"KNOWS";
        let neighbor = vec![1u8, 2, 3];
        let mut clustering = Vec::new();
        clustering.extend_from_slice(&1u16.to_be_bytes()); // direction component len
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
        clustering.extend_from_slice(&1u16.to_be_bytes());
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
        clustering.extend_from_slice(&1u16.to_be_bytes());
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
        clustering.extend_from_slice(&1u16.to_be_bytes());
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
        // [u16 1][1B direction=0][u16 0][u16 0]
        let clustering = vec![0u8, 1, 0, 0, 0, 0, 0];
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

    /// UNWIND's unsorted path now projects through a `RowStream` and applies
    /// LIMIT as `take(k)` (t_4ce82a3e increment 2). This drives the real
    /// executor fn to prove the conversion preserves behavior: LIMIT yields
    /// exactly k rows, in list order, and the un-limited query returns all.
    #[tokio::test]
    async fn unwind_streaming_limit_yields_exactly_k_rows_in_order() {
        use crate::parser::{Expr, Literal, ReturnClause, ReturnItem};

        let list = Expr::List(
            (1..=5)
                .map(|i| Expr::Literal(Literal::Integer(i * 10)))
                .collect(),
        );
        let items = vec![ReturnItem {
            expr: Expr::Var("x".to_string()),
            alias: None,
        }];

        let limited = ReturnClause {
            items: items.clone(),
            distinct: false,
            order_by: Vec::new(),
            limit: Some(2),
        };
        let res = execute_unwind(&list, "x", None, &limited, Instant::now())
            .await
            .expect("unwind with limit");
        assert_eq!(
            res.rows,
            vec![vec![serde_json::json!(10)], vec![serde_json::json!(20)]],
            "LIMIT 2 must yield the first two list elements, in order"
        );

        let unlimited = ReturnClause {
            items,
            distinct: false,
            order_by: Vec::new(),
            limit: None,
        };
        let res = execute_unwind(&list, "x", None, &unlimited, Instant::now())
            .await
            .expect("unwind without limit");
        assert_eq!(res.rows.len(), 5, "no LIMIT must yield every element");
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

    #[tokio::test]
    async fn execute_aggregate_count() {
        // Test end-to-end aggregation: count over a virtual table.
        let registry = VirtualTableRegistry::new();
        let vtable = Arc::new(TestVirtualTable {
            table_name: "person_v".to_string(),
            ks: "social".to_string(),
            cols: vec![VirtualColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![
                VirtualRow {
                    cells: vec![CellValue::live(b"Alice".to_vec(), 1000)],
                },
                VirtualRow {
                    cells: vec![CellValue::live(b"Bob".to_vec(), 1000)],
                },
                VirtualRow {
                    cells: vec![CellValue::live(b"Charlie".to_vec(), 1000)],
                },
            ],
        });
        registry.register(vtable);

        use crate::planner::logical::ResolvedTable;
        use crate::planner::physical::{AggregateProjection, Anchor};

        // The outer return clause has count(*) — no specific column arg.
        let outer_return_clause = ReturnClause {
            distinct: false,
            items: vec![crate::parser::ReturnItem {
                expr: Expr::Function {
                    name: "count".to_string(),
                    args: vec![],
                },
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        // The inner Expand uses Var("*") as a sentinel (the planner rewrites
        // count() with no args into Var("*")). We return the anchor variable
        // so each virtual table row produces one output row.
        let inner_return_clause = ReturnClause {
            distinct: false,
            items: vec![crate::parser::ReturnItem {
                expr: Expr::Var("*".to_string()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        let inner_expand = PhysicalPlan::Expand {
            anchor: Anchor {
                var: Some("n".to_string()),
                table: ResolvedTable {
                    keyspace: "social".to_string(),
                    table: "person_v".to_string(),
                    graph_type: "vertex".to_string(),
                    label: "Person".to_string(),
                },
                props: vec![],
                filters: vec![],
            },
            hops: vec![],
            optional_hops: vec![],
            post_filters: vec![],
            with_pipeline: None,
            return_clause: inner_return_clause,
        };

        let agg_plan = PhysicalPlan::Aggregate {
            inner: Box::new(inner_expand),
            group_keys: vec![],
            projections: vec![AggregateProjection::AggregateFunc {
                name: "count".to_string(),
                arg: Expr::Var("*".to_string()),
                distinct: false,
            }],
            return_clause: outer_return_clause,
        };

        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());

        let config = GraphEngineConfig::default();
        let wp = WritePath::direct(storage);
        let result = execute(agg_plan, &wp, "social", &config, Some(&registry), None)
            .await
            .unwrap();

        assert_eq!(result.columns, vec!["count(*)"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], serde_json::json!(3u64));
    }

    #[test]
    fn graph_write_consistency_is_quorum() {
        assert_eq!(graph_write_consistency(), ConsistencyLevel::Quorum);
    }

    /// Mirrors the ferrosa-memory `co_occurs_with` schema after migrations
    /// 003 (create), 010 (add strength + last_reinforced), and 031 (add
    /// first_seen). All regular columns have position=0 because both CQL
    /// CREATE TABLE and ALTER TABLE ADD pin them to 0 in the live registry
    /// (router.rs:4300, router.rs:4572).
    fn co_occurs_with_meta_after_alters() -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert(
            "entity_a".to_string(),
            test_column(
                "entity_a",
                ColumnKind::PartitionKey,
                0,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "entity_b".to_string(),
            test_column(
                "entity_b",
                ColumnKind::Clustering,
                0,
                "uuid",
                ClusteringOrder::Asc,
            ),
        );
        columns.insert(
            "session_id".to_string(),
            test_column(
                "session_id",
                ColumnKind::Regular,
                0,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "tenant_id".to_string(),
            test_column(
                "tenant_id",
                ColumnKind::Regular,
                0,
                "uuid",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "created_at".to_string(),
            test_column(
                "created_at",
                ColumnKind::Regular,
                0,
                "timestamp",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "strength".to_string(),
            test_column(
                "strength",
                ColumnKind::Regular,
                0,
                "float",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "last_reinforced".to_string(),
            test_column(
                "last_reinforced",
                ColumnKind::Regular,
                0,
                "timestamp",
                ClusteringOrder::None,
            ),
        );
        columns.insert(
            "first_seen".to_string(),
            test_column(
                "first_seen",
                ColumnKind::Regular,
                0,
                "timestamp",
                ClusteringOrder::None,
            ),
        );
        TableMetadata {
            keyspace: "agent_memory".to_string(),
            name: "co_occurs_with".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["entity_a".to_string()],
            clustering_key: vec![("entity_b".to_string(), ClusteringOrder::Asc)],
            params: TableParams::default(),
            flags: HashSet::from([TableFlag::Compound]),
            extensions: HashMap::from([
                ("graph.type".to_string(), "edge".to_string()),
                ("graph.label".to_string(), "CO_OCCURS_WITH".to_string()),
                ("graph.source".to_string(), "entity_a".to_string()),
                ("graph.target".to_string(), "entity_b".to_string()),
                ("graph.source_label".to_string(), "Entity".to_string()),
                ("graph.target_label".to_string(), "Entity".to_string()),
            ]),
            is_system: false,
        }
    }

    /// Repro for ferrosa-memory PR#4 cluster-int failure: when a partial SET
    /// (e.g. only `r.strength`) is encoded by `build_schema_aware_merge_shape`,
    /// every produced cell `(col_idx, bytes)` must be decodable by the
    /// receiver as `regular_columns[col_idx]` in the same TableMetadata's
    /// `to_storage_schema()`. Otherwise replicas reject the
    /// MutationForward with `expects N raw bytes but value provided M`.
    #[test]
    fn merge_write_shape_cell_indices_match_storage_schema_regular_columns() {
        use ferrosa_common::schema::{fixed_width_for_marshal_type, validate_cell_bytes};

        let meta = co_occurs_with_meta_after_alters();
        let storage_schema = meta.to_storage_schema();

        // Build a live Schema and register the keyspace + table so that
        // build_schema_aware_merge_shape can resolve it via Schema::snapshot.
        let schema = Schema::new(ferrosa_schema::SchemaConfig {
            hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
            password_policy: ferrosa_schema::PasswordPolicy::permissive(),
            auth_method: ferrosa_schema::AuthMethod::Password,
            rate_limit: ferrosa_schema::RateLimitConfig::default(),
            audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: ferrosa_schema::DeploymentMode::Development,
        })
        .unwrap();
        let auth = ferrosa_schema::auth::role::AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        schema
            .create_keyspace(
                ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                    name: meta.keyspace.clone(),
                    durable_writes: true,
                    replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: HashMap::from([(
                            "replication_factor".to_string(),
                            "1".to_string(),
                        )]),
                    },
                },
                &auth,
            )
            .unwrap();
        // CO_OCCURS_WITH's graph.source_label points at "Entity"; the
        // validator requires the vertex label to already exist.
        let mut entity_meta = scoped_entity_meta();
        entity_meta
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        entity_meta
            .extensions
            .insert("graph.label".to_string(), "Entity".to_string());
        schema.create_table(entity_meta, &auth).unwrap();
        schema.create_table(meta.clone(), &auth).unwrap();

        // Mirror the warm-up probe query:
        //   MERGE (a)-[r:CO_OCCURS_WITH {tenant_id, session_id}]->(b)
        //   SET r.strength = 0.5 RETURN r
        let tenant = Expr::Literal(Literal::String(
            "00000000-0000-0000-0000-000000000001".to_string(),
        ));
        let session = Expr::Literal(Literal::String(
            "11111111-1111-1111-1111-111111111111".to_string(),
        ));
        let entity_a = Expr::Literal(Literal::String(
            "22222222-2222-2222-2222-222222222222".to_string(),
        ));
        let entity_b = Expr::Literal(Literal::String(
            "33333333-3333-3333-3333-333333333333".to_string(),
        ));

        let op = MergeOp {
            var: Some("r".to_string()),
            table: crate::planner::ResolvedTable {
                keyspace: meta.keyspace.clone(),
                table: meta.name.clone(),
                label: "CO_OCCURS_WITH".to_string(),
                graph_type: "edge".to_string(),
            },
            match_props: vec![
                ("tenant_id".to_string(), tenant.clone()),
                ("session_id".to_string(), session.clone()),
            ],
            create_props: vec![],
            src_match_props: Some(vec![("entity_a".to_string(), entity_a)]),
            src_var: Some("a".to_string()),
            dst_match_props: Some(vec![("entity_b".to_string(), entity_b)]),
            dst_var: Some("b".to_string()),
        };

        let set_props = vec![("strength".to_string(), Expr::Literal(Literal::Float(0.5)))];

        let shape = build_schema_aware_merge_shape(&op, &set_props, Some(&schema), 1000)
            .expect("build_schema_aware_merge_shape ok")
            .expect("schema-aware shape produced");

        // For each cell the encoder emits, the receiver maps col_idx → name
        // and runs validate_cell_bytes against the column's marshal type.
        // If our cell_idx and storage_schema.regular_columns ordering drift,
        // this is where the live cluster sees:
        //   "TimestampType expects 8 raw bytes but value provided 4".
        let static_count = storage_schema.static_columns.len();
        for (col_idx, cell) in &shape.create_cells {
            let Some(bytes) = &cell.value else { continue };
            let idx = *col_idx as usize;
            assert!(
                idx >= static_count,
                "encoder used static-column slot {idx} but co_occurs_with has no statics"
            );
            let regular_idx = idx - static_count;
            let column = storage_schema
                .regular_columns
                .get(regular_idx)
                .unwrap_or_else(|| {
                    panic!(
                        "cell col_idx={col_idx} out of range for {} regular columns",
                        storage_schema.regular_columns.len()
                    )
                });
            // The receiver only knows column.type_name, so any drift between
            // encoder ordering (IndexMap iteration) and storage ordering
            // (position-stable-sort in to_storage_schema) surfaces as
            // bytes.len() vs the type's fixed width.
            if let Err(reason) = validate_cell_bytes(&column.type_name, bytes) {
                let expected = fixed_width_for_marshal_type(&column.type_name)
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "variable".to_string());
                panic!(
                    "encoder produced cell col_idx={col_idx} with {} bytes for \
                     receiver-side column \"{}\" ({}, expects {} bytes): {}",
                    bytes.len(),
                    column.name,
                    column.type_name,
                    expected,
                    reason
                );
            }
            // Defensive: the cell at col_idx must be the column the encoder
            // intended (strength, in this case). If the receiver decodes
            // col_idx 3 as first_seen but the encoder meant strength, the
            // bytes/type check above already catches it — but assert the
            // identity here for clarity.
            if column.name == "strength" {
                assert_eq!(
                    bytes.len(),
                    4,
                    "strength must round-trip as f32 (4 bytes), got {}",
                    bytes.len()
                );
            }
        }

        // The encoder must populate strength somewhere — otherwise the SET
        // silently dropped the property and the test is moot.
        let mut strength_idx = None;
        for (col_idx, _) in &shape.create_cells {
            let regular_idx = *col_idx as usize - static_count;
            if let Some(c) = storage_schema.regular_columns.get(regular_idx) {
                if c.name == "strength" {
                    strength_idx = Some(*col_idx);
                    break;
                }
            }
        }
        strength_idx.expect(
            "SET r.strength must produce a cell whose col_idx maps to the strength column \
             on the receiver's TableSchema",
        );
    }

    // ---------------------------------------------------------------------
    // Streaming entry point (`execute_streaming`)
    //
    // Two obligations, one per test group:
    //   (1) a converted variant genuinely streams — see
    //       `union_all_streams_without_pulling_later_arms`;
    //   (2) EVERY variant — converted or fallback — reproduces the buffered
    //       `GraphResult` exactly, which is what `assert_streaming_matches`
    //       asserts field by field (`execution_ms` excluded: it is a wall-clock
    //       measurement, not a result value).
    // ---------------------------------------------------------------------

    fn streaming_test_write_path() -> (tempfile::TempDir, WritePath) {
        let tmp = tempfile::tempdir().unwrap();
        let storage = test_storage_engine(tmp.path());
        (tmp, WritePath::direct(storage))
    }

    fn return_one_plan(items: &[(&str, i64)], limit: Option<i64>) -> PhysicalPlan {
        use crate::parser::{Expr, Literal, ReturnClause, ReturnItem};
        PhysicalPlan::ReturnOnly {
            return_clause: ReturnClause {
                distinct: false,
                items: items
                    .iter()
                    .map(|(alias, value)| ReturnItem {
                        expr: Expr::Literal(Literal::Integer(*value)),
                        alias: Some((*alias).to_string()),
                    })
                    .collect(),
                order_by: vec![],
                limit,
            },
        }
    }

    /// Run `plan` through both entry points and assert the streaming one is
    /// indistinguishable from today's buffered one.
    async fn assert_streaming_matches(
        // `PhysicalPlan` is not `Clone`, and the plan is consumed by value, so
        // the caller supplies a factory that builds it once per entry point.
        build_plan: impl Fn() -> PhysicalPlan,
        keyspace: &str,
        registry: Option<&VirtualTableRegistry>,
    ) -> GraphResult {
        let (_tmp, wp) = streaming_test_write_path();
        let config = GraphEngineConfig::default();

        let buffered = execute(build_plan(), &wp, keyspace, &config, registry, None)
            .await
            .unwrap();

        let (columns, rows_stream, stats) =
            execute_streaming(build_plan(), &wp, keyspace, &config, registry, None)
                .await
                .unwrap();
        let rows = crate::executor::stream::collect_rows(rows_stream)
            .await
            .unwrap();

        assert_eq!(columns, buffered.columns, "columns must be identical");
        assert_eq!(rows, buffered.rows, "rows must be identical, in order");
        assert_eq!(stats.vertices_read, buffered.stats.vertices_read);
        assert_eq!(stats.edges_read, buffered.stats.edges_read);
        assert_eq!(stats.vertices_written, buffered.stats.vertices_written);
        assert_eq!(stats.vertices_deleted, buffered.stats.vertices_deleted);
        assert_eq!(stats.edges_deleted, buffered.stats.edges_deleted);

        GraphResult {
            columns,
            rows,
            stats,
        }
    }

    // --- owned-handle entry point (`execute_streaming_owned`) -------------
    //
    // The HTTP body outlives the handler frame, so it can only be fed by a
    // `RowStream<'static>`. These pin the three properties that makes possible:
    // same rows as the borrowed entry point, a stream that genuinely outlives
    // the scope that built its handles, and setup errors that still surface as
    // `Err` (so the transport can still pick an HTTP status before writing).

    fn owned_test_ctx(wp: Arc<WritePath>, keyspace: &str) -> OwnedExecCtx {
        OwnedExecCtx {
            write_path: wp,
            keyspace: keyspace.to_string(),
            config: Arc::new(GraphEngineConfig::default()),
            virtual_tables: None,
            schema: None,
        }
    }

    #[tokio::test]
    async fn owned_streaming_matches_the_borrowed_entry_point() {
        let (_tmp, wp) = streaming_test_write_path();
        let wp = Arc::new(wp);
        let config = GraphEngineConfig::default();
        let build = || PhysicalPlan::Union {
            arms: vec![
                return_one_plan(&[("a", 1)], None),
                return_one_plan(&[("a", 2)], None),
            ],
            all: true,
        };

        let (borrowed_cols, borrowed_stream, _) =
            execute_streaming(build(), &wp, "social", &config, None, None)
                .await
                .unwrap();
        let borrowed_rows = crate::executor::stream::collect_rows(borrowed_stream)
            .await
            .unwrap();

        let (owned_cols, owned_stream, _) =
            execute_streaming_owned(build(), owned_test_ctx(Arc::clone(&wp), "social"))
                .await
                .unwrap();
        let owned_rows = crate::executor::stream::collect_rows(owned_stream)
            .await
            .unwrap();

        assert_eq!(owned_cols, borrowed_cols);
        assert_eq!(owned_rows, borrowed_rows);
    }

    /// The whole point of the owned entry point: the returned stream is
    /// `'static`, so it can be handed to a response body that outlives the
    /// function that built the handles.
    ///
    /// The `RowStream<'static>` annotation is the real assertion — it does not
    /// compile if the stream borrows the write path, the keyspace, the config,
    /// or the schema. Draining it after the builder frame has returned proves
    /// the handles are genuinely kept alive by the stream.
    #[tokio::test]
    async fn owned_row_stream_is_static_and_outlives_its_builder() {
        async fn build_stream(wp: Arc<WritePath>) -> (Vec<String>, RowStream<'static>, QueryStats) {
            // Everything borrowed by the executor is created and dropped inside
            // this frame; only the stream escapes.
            execute_streaming_owned(
                return_one_plan(&[("a", 42)], None),
                OwnedExecCtx {
                    write_path: wp,
                    keyspace: "social".to_string(),
                    config: Arc::new(GraphEngineConfig::default()),
                    virtual_tables: None,
                    schema: None,
                },
            )
            .await
            .unwrap()
        }

        let (_tmp, wp) = streaming_test_write_path();
        let (columns, stream, _stats) = build_stream(Arc::new(wp)).await;
        let stream: RowStream<'static> = stream;
        assert_eq!(columns, vec!["a".to_string()]);
        assert_eq!(
            crate::executor::stream::collect_rows(stream).await.unwrap(),
            vec![vec![serde_json::json!(42)]]
        );
    }

    /// A setup failure must still come back as `Err` from the owned entry
    /// point, NOT as an error item on the stream. The transport picks its HTTP
    /// status here, before the first byte of the body is written — once the
    /// body starts there is no status left to choose.
    #[tokio::test]
    async fn owned_streaming_setup_error_surfaces_before_the_stream() {
        let (_tmp, wp) = streaming_test_write_path();
        let plan = PhysicalPlan::Union {
            arms: vec![
                return_one_plan(&[("a", 1)], None),
                return_one_plan(&[("b", 2)], None),
            ],
            all: true,
        };
        // `RowStream` is not `Debug`, so the Ok arm cannot be unwrapped.
        let err = match execute_streaming_owned(plan, owned_test_ctx(Arc::new(wp), "social")).await
        {
            Ok(_) => panic!("owned streaming must reject arms with mismatched columns"),
            Err(err) => err,
        };
        assert!(
            matches!(err, GraphError::Validation(ref m) if m.contains("identical columns")),
            "got {err:?}"
        );
    }

    /// Laziness survives the owned wrapper: a downstream `take(k)` still stops
    /// the chain. If the wrapper collected internally this would be the only
    /// visible symptom — the rows would be right, but every arm would run.
    #[tokio::test]
    async fn owned_streaming_take_still_short_circuits() {
        use futures::StreamExt as _;

        let (_tmp, wp) = streaming_test_write_path();
        let plan = PhysicalPlan::Union {
            arms: (0..64)
                .map(|i| return_one_plan(&[("a", i)], None))
                .collect(),
            all: true,
        };
        let (_columns, rows_stream, _stats) =
            execute_streaming_owned(plan, owned_test_ctx(Arc::new(wp), "social"))
                .await
                .unwrap();
        let limited: RowStream<'static> = Box::pin(rows_stream.take(2));
        assert_eq!(
            crate::executor::stream::collect_rows(limited)
                .await
                .unwrap(),
            vec![vec![serde_json::json!(0)], vec![serde_json::json!(1)]]
        );
    }

    /// An error raised MID-stream (after rows have already been yielded) must
    /// still reach the consumer as an `Err` item — never be swallowed into a
    /// short-but-successful result.
    #[tokio::test]
    async fn owned_streaming_propagates_a_mid_stream_error() {
        use futures::StreamExt as _;

        let (_tmp, wp) = streaming_test_write_path();
        let (_columns, rows_stream, _stats) = execute_streaming_owned(
            return_one_plan(&[("a", 1)], None),
            owned_test_ctx(Arc::new(wp), "social"),
        )
        .await
        .unwrap();

        // Splice a failure in after the real row, the way a lazily-hydrated hop
        // surfaces a storage error partway through the projection.
        let spliced: RowStream<'static> = Box::pin(rows_stream.chain(stream_error_once()));
        let mut rows = Vec::new();
        let mut seen_err = None;
        let mut spliced = spliced;
        while let Some(item) = futures::StreamExt::next(&mut spliced).await {
            match item {
                Ok(row) => rows.push(row),
                Err(e) => {
                    seen_err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(rows, vec![vec![serde_json::json!(1)]]);
        assert!(
            matches!(seen_err, Some(GraphError::Internal(ref m)) if m == "mid-stream boom"),
            "the mid-stream error must reach the consumer"
        );
    }

    fn stream_error_once() -> RowStream<'static> {
        Box::pin(futures::stream::once(async {
            Err(GraphError::Internal("mid-stream boom".to_string()))
        }))
    }

    // --- converted: ReturnOnly -------------------------------------------

    #[tokio::test]
    async fn return_only_streaming_matches_buffered() {
        let result = assert_streaming_matches(
            || return_one_plan(&[("a", 1), ("b", 2)], None),
            "social",
            None,
        )
        .await;
        assert_eq!(result.columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            result.rows,
            vec![vec![serde_json::json!(1), serde_json::json!(2)]]
        );
    }

    /// `RETURN 1 LIMIT 0` truncated a materialized one-row Vec. Streaming, the
    /// `take(0)` means the projection is never even reached — and the result is
    /// still the same empty row set.
    #[tokio::test]
    async fn return_only_streaming_honors_limit_zero() {
        let result =
            assert_streaming_matches(|| return_one_plan(&[("a", 1)], Some(0)), "social", None)
                .await;
        assert!(result.rows.is_empty(), "LIMIT 0 yields no rows");
    }

    #[tokio::test]
    async fn return_only_streaming_honors_negative_limit() {
        // `limit.max(0)` in the buffered path: a negative LIMIT is a zero LIMIT.
        let result =
            assert_streaming_matches(|| return_one_plan(&[("a", 1)], Some(-5)), "social", None)
                .await;
        assert!(result.rows.is_empty());
    }

    // --- converted: Subscribe --------------------------------------------

    #[tokio::test]
    async fn subscribe_streaming_passes_through_to_inner() {
        let build = || PhysicalPlan::Subscribe {
            inner: Box::new(return_one_plan(&[("a", 7)], None)),
            interval: Duration::from_secs(1),
            delta: false,
            return_clause: crate::parser::ReturnClause {
                distinct: false,
                items: vec![],
                order_by: vec![],
                limit: None,
            },
        };
        let result = assert_streaming_matches(build, "social", None).await;
        assert_eq!(result.columns, vec!["a".to_string()]);
        assert_eq!(result.rows, vec![vec![serde_json::json!(7)]]);
    }

    // --- converted: Union { all: true } ----------------------------------

    #[tokio::test]
    async fn union_all_streaming_matches_buffered() {
        let build = || PhysicalPlan::Union {
            arms: vec![
                return_one_plan(&[("a", 1)], None),
                return_one_plan(&[("a", 2)], None),
                // A duplicate row: UNION ALL keeps it.
                return_one_plan(&[("a", 1)], None),
            ],
            all: true,
        };
        let result = assert_streaming_matches(build, "social", None).await;
        assert_eq!(
            result.rows,
            vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(1)]
            ],
            "UNION ALL is concatenation in arm order, duplicates kept"
        );
    }

    #[tokio::test]
    async fn union_all_streaming_with_no_arms_matches_buffered() {
        let build = || PhysicalPlan::Union {
            arms: vec![],
            all: true,
        };
        let result = assert_streaming_matches(build, "social", None).await;
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
    }

    /// Incrementality at the PLAN level: a downstream `take(k)` over a
    /// converted `UNION ALL` returns after the arm that supplies row `k` and
    /// drops the rest of the chain unpolled.
    ///
    /// The per-arm production counting lives in
    /// `stream::tests::chain_streams_does_not_pull_later_arms_early`, where the
    /// arms can be instrumented. Here the arms are `ReturnOnly` plans, whose
    /// projection has no observable side effect — so this test pins the
    /// row-level contract (which rows come back, and that the stream can be
    /// abandoned early without error), not the arm-execution schedule.
    #[tokio::test]
    async fn union_all_streams_without_pulling_later_arms() {
        let (_tmp, wp) = streaming_test_write_path();
        let config = GraphEngineConfig::default();
        let plan = PhysicalPlan::Union {
            arms: (0..64)
                .map(|i| return_one_plan(&[("a", i)], None))
                .collect(),
            all: true,
        };
        let (columns, rows_stream, _stats) =
            execute_streaming(plan, &wp, "social", &config, None, None)
                .await
                .unwrap();
        assert_eq!(columns, vec!["a".to_string()]);

        let limited: crate::executor::stream::RowStream<'_> = {
            use futures::StreamExt as _;
            Box::pin(rows_stream.take(2))
        };
        let rows = crate::executor::stream::collect_rows(limited)
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![serde_json::json!(0)], vec![serde_json::json!(1)]],
            "take(2) over UNION ALL yields the first two arms' rows and stops"
        );
    }

    #[tokio::test]
    async fn union_streaming_column_mismatch_still_errors() {
        let (_tmp, wp) = streaming_test_write_path();
        let config = GraphEngineConfig::default();
        let build = || PhysicalPlan::Union {
            arms: vec![
                return_one_plan(&[("a", 1)], None),
                return_one_plan(&[("b", 2)], None),
            ],
            all: true,
        };
        // `RowStream` is not `Debug`, so the Ok arm cannot be unwrapped/printed.
        let err = match execute_streaming(build(), &wp, "social", &config, None, None).await {
            Ok(_) => panic!("streaming UNION must reject arms with mismatched columns"),
            Err(err) => err,
        };
        assert!(
            matches!(err, GraphError::Validation(ref m) if m.contains("identical columns")),
            "streaming UNION must reject mismatched arms exactly as the buffered path does, got {err:?}"
        );
        // ...and the buffered entry point still rejects it too.
        assert!(execute(build(), &wp, "social", &config, None, None)
            .await
            .is_err());
    }

    // --- fallback: Union { all: false } ----------------------------------

    /// `UNION` (no ALL) dedups over the WHOLE concatenation, so it must NOT be
    /// converted. Proving the fallback still dedups is the regression guard.
    #[tokio::test]
    async fn union_distinct_falls_back_and_still_dedups() {
        let build = || PhysicalPlan::Union {
            arms: vec![
                return_one_plan(&[("a", 1)], None),
                return_one_plan(&[("a", 2)], None),
                return_one_plan(&[("a", 1)], None),
            ],
            all: false,
        };
        let result = assert_streaming_matches(build, "social", None).await;
        assert_eq!(
            result.rows,
            vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
            "UNION without ALL must still dedup across all arms"
        );
    }

    // --- fallback: Expand (virtual table + storage) -----------------------

    #[tokio::test]
    async fn expand_falls_back_with_identical_result() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(TestVirtualTable {
            table_name: "connections".to_string(),
            ks: "system_observability".to_string(),
            cols: vec![VirtualColumnDef {
                name: "peer_address".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![
                VirtualRow {
                    cells: vec![CellValue::live(b"10.0.0.1".to_vec(), 1000)],
                },
                VirtualRow {
                    cells: vec![CellValue::live(b"10.0.0.2".to_vec(), 1000)],
                },
            ],
        }));

        let build = || {
            virtual_anchor_plan(
                "system_observability",
                "connections",
                &[("n", "peer_address")],
            )
        };
        let result = assert_streaming_matches(build, "system_observability", Some(&registry)).await;
        assert_eq!(result.rows.len(), 2, "the fallback must still return rows");
    }

    #[tokio::test]
    async fn empty_storage_expand_falls_back_with_identical_result() {
        let build = || virtual_anchor_plan("social", "person_v", &[("n", "name")]);
        let result = assert_streaming_matches(build, "social", None).await;
        assert!(result.rows.is_empty());
    }

    // --- fallback: Unwind -------------------------------------------------

    #[tokio::test]
    async fn unwind_falls_back_with_identical_result() {
        use crate::parser::{Expr, Literal, ReturnClause, ReturnItem};
        let build = || PhysicalPlan::Unwind {
            expr: Expr::List(vec![
                Expr::Literal(Literal::Integer(3)),
                Expr::Literal(Literal::Integer(1)),
                Expr::Literal(Literal::Integer(2)),
            ]),
            var: "x".to_string(),
            with_pipeline: None,
            return_clause: ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Var("x".to_string()),
                    alias: Some("x".to_string()),
                }],
                order_by: vec![],
                limit: None,
            },
        };
        let result = assert_streaming_matches(build, "social", None).await;
        assert_eq!(
            result.rows,
            vec![
                vec![serde_json::json!(3)],
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)]
            ]
        );
    }

    // ---------------------------------------------------------------------
    // SET / REMOVE stream their INNER EXPAND (their own output is one summary
    // row, only knowable after the last write). `projection_reads_storage` is
    // the guard that makes interleaving the inner projection with those writes
    // sound.
    // ---------------------------------------------------------------------

    #[test]
    fn projection_reads_storage_is_false_for_the_planner_shape_set_and_remove_use() {
        // `plan_set` / `plan_remove` synthesize plain `Expr::Var` projections.
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        let plan = PhysicalPlan::Expand {
            anchor: Anchor {
                var: Some("n".to_string()),
                table: crate::planner::logical::ResolvedTable {
                    keyspace: "social".to_string(),
                    table: "person".to_string(),
                    graph_type: "vertex".to_string(),
                    label: "Person".to_string(),
                },
                props: vec![],
                filters: vec![],
            },
            hops: vec![],
            optional_hops: vec![],
            post_filters: vec![],
            with_pipeline: None,
            return_clause: ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Var("n".to_string()),
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
        };
        assert!(
            !projection_reads_storage(&plan),
            "a plain Expr::Var projection reads only the bindings, so SET/REMOVE may \
             stream the inner expand and interleave their writes with it"
        );
    }

    #[test]
    fn projection_reads_storage_is_true_for_a_non_expand_inner_plan() {
        // Anything that is not the plain expand SET/REMOVE plan for must be
        // materialized before writing — conservative by construction.
        assert!(projection_reads_storage(&return_one_plan(
            &[("a", 1)],
            None
        )));
    }

    // ---------------------------------------------------------------------
    // Streaming Expand projection (`project_states_stream`)
    //
    // The states are still materialized by the hop loop (increments 3-4); what
    // streams here is the PROJECTION — one `project_state` call per pull, so a
    // downstream `take(k)` projects exactly k states.
    // ---------------------------------------------------------------------

    fn projection_state(name: &str) -> ExpandState {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), serde_json::json!({ "name": name }));
        ExpandState {
            current_key: DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec())),
            bindings,
        }
    }

    fn name_return_clause(distinct: bool, limit: Option<i64>) -> crate::parser::ReturnClause {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        ReturnClause {
            distinct,
            items: vec![ReturnItem {
                expr: Expr::Property {
                    var: "n".to_string(),
                    name: "name".to_string(),
                },
                alias: None,
            }],
            order_by: vec![],
            limit,
        }
    }

    fn projection_ctx<'a>(
        write_path: &'a WritePath,
        return_clause: crate::parser::ReturnClause,
        needs_graph_projection: bool,
    ) -> ExpandProjection<'a> {
        ExpandProjection {
            return_clause,
            anchor_var: "n".to_string(),
            needs_graph_projection,
            write_path,
            keyspace: "social",
            schema: None,
        }
    }

    #[tokio::test]
    async fn project_states_stream_yields_one_row_per_state_in_order() {
        let (_tmp, wp) = streaming_test_write_path();
        let states = vec![
            projection_state("Cara"),
            projection_state("Alice"),
            projection_state("Bob"),
        ];
        let stream = project_states_stream(
            states,
            projection_ctx(&wp, name_return_clause(false, None), false),
        );
        assert_eq!(
            crate::executor::stream::collect_rows(stream).await.unwrap(),
            vec![
                vec![serde_json::json!("Cara")],
                vec![serde_json::json!("Alice")],
                vec![serde_json::json!("Bob")],
            ],
            "projection is 1:1 with the surviving states, in expansion order"
        );
    }

    /// The incrementality obligation for the Expand conversion: projection work
    /// happens ON PULL, not up front.
    ///
    /// Observed through an error. `project_state`'s graph-aware path propagates
    /// evaluation failures (`graph_eval_expr` uses `?`, unlike the binding-only
    /// path's `unwrap_or(Null)`), and `Expr::Parameter` is always an unbound-
    /// parameter error. So: draining the stream MUST fail (the projection ran),
    /// while `take(0)` MUST succeed (no state was ever projected). A
    /// materialize-then-return implementation cannot pass both halves.
    #[tokio::test]
    async fn project_states_stream_defers_projection_until_pulled() {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        let (_tmp, wp) = streaming_test_write_path();
        let erroring = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Parameter("never_bound".to_string()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };
        let states = || {
            vec![
                projection_state("Cara"),
                projection_state("Alice"),
                projection_state("Bob"),
            ]
        };

        let drained = crate::executor::stream::collect_rows(project_states_stream(
            states(),
            projection_ctx(&wp, erroring.clone(), true),
        ))
        .await;
        assert!(
            drained.is_err(),
            "draining must surface the projection error — otherwise the projection never ran \
             and this test proves nothing"
        );

        let untouched: crate::executor::stream::RowStream<'_> = {
            use futures::StreamExt as _;
            Box::pin(project_states_stream(states(), projection_ctx(&wp, erroring, true)).take(0))
        };
        assert_eq!(
            crate::executor::stream::collect_rows(untouched)
                .await
                .unwrap(),
            Vec::<Vec<serde_json::Value>>::new(),
            "take(0) must project NOTHING — no state may be projected before it is pulled"
        );
    }

    /// LIMIT over a streaming projection is `take(k)`: only k states are
    /// projected, proven by the same error observable.
    #[tokio::test]
    async fn project_states_stream_take_projects_only_the_pulled_prefix() {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        let (_tmp, wp) = streaming_test_write_path();
        // Item 1 always evaluates; a SECOND item is an unbound parameter, so
        // projecting ANY state fails. With `take(0)` nothing is projected.
        let clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Parameter("never_bound".to_string()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };
        let stream: crate::executor::stream::RowStream<'_> = {
            use futures::StreamExt as _;
            Box::pin(
                project_states_stream(
                    vec![projection_state("Cara"), projection_state("Alice")],
                    projection_ctx(&wp, clause, true),
                )
                .take(0),
            )
        };
        assert!(
            crate::executor::stream::collect_rows(stream).await.is_ok(),
            "a zero-row LIMIT must not project the first state"
        );
    }

    /// After an error the stream STOPS. Continuing would re-poll an operator
    /// that already failed and could report a partial result as complete.
    #[tokio::test]
    async fn project_states_stream_stops_after_a_projection_error() {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        use futures::StreamExt as _;
        let (_tmp, wp) = streaming_test_write_path();
        let clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Parameter("never_bound".to_string()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };
        let mut stream = project_states_stream(
            vec![projection_state("Cara"), projection_state("Alice")],
            projection_ctx(&wp, clause, true),
        );
        assert!(stream.next().await.expect("first item").is_err());
        assert!(
            stream.next().await.is_none(),
            "the stream must terminate after a projection error, not keep yielding"
        );
    }
}
