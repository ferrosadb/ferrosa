//! CQL query router — dispatches parsed statements to schema/storage.
//!
//! This is the central piece of the CQL layer: it takes a parsed `Statement`,
//! resolves keyspaces, checks permissions, and delegates to the appropriate
//! schema or storage operations.
//!
//! Security mitigations:
//! - **M8**: Every `route_*` function checks permissions via `Schema::check_permission`.
//! - **M12**: Batch size is capped at `MAX_BATCH_STATEMENTS` (500).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use futures::StreamExt;
use indexmap::IndexMap;
use sha2::{Digest, Sha256};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_cluster::{DdlPath, WritePath};
use ferrosa_common::DataType;
use ferrosa_index::IndexType;
use ferrosa_schema::{
    query_columns, query_keyspaces, query_local_with_view, query_peers_with_view,
    query_role_members, query_role_permissions, query_tables, AuthContext,
    ClusteringOrder as SchemaClusteringOrder, ColumnKind, ColumnMetadata, GrantEntry,
    IndexMetadata, KeyspaceMetadata, KeyspaceUpdates, NodeConfig, Permission, ReplicationParams,
    Resource, RoleMetadata, RoleUpdates, RowPredicate, Schema, TableMetadata, TableParams,
    TableUpdates, UserAggregateMetadata, UserFunctionMetadata, UserTypeMetadata,
    VirtualColumnUpdate, VirtualTableUpdate,
};
use ferrosa_storage::StorageEngine;
use ferrosa_storage::TableId;
use ferrosa_storage::FILTER_PREDICATE_OPTION_KEY;
use ferrosa_udf::UdfExecutor;

use crate::ast::*;
use crate::bridge;
use crate::error::CqlError;
use crate::observability::{CqlMetrics, CqlOpcode};
use crate::planner::{self, ScanPlan};
use crate::prepared::PreparedCache;
use crate::result;
use crate::topology::ClientTopologyPolicy;
use crate::types::{encode_value, CqlType, CqlValue};
use crate::virtual_tables::active_queries::QueryTracker;
use crate::virtual_tables::connections::ConnectionTracker;

/// Default maximum number of statements allowed in a BATCH (security
/// mitigation M12). Operators can tune via
/// `FERROSA_CQL_MAX_BATCH_STATEMENTS`; zero or unparseable values fall
/// back to the default. Raising this lets larger batched writes
/// through; lowering it tightens the M12 cap for environments that
/// have to absorb adversarial clients.
pub const DEFAULT_MAX_BATCH_STATEMENTS: usize = 500;

fn max_batch_statements() -> usize {
    std::env::var("FERROSA_CQL_MAX_BATCH_STATEMENTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_BATCH_STATEMENTS)
}

/// Default cooperative-yield cadence for long partition scans. Larger
/// values reduce yield overhead but increase tail latency on competing
/// tasks; smaller values do the opposite. Tune via
/// `FERROSA_CQL_SCAN_YIELD_EVERY_PARTITIONS`.
pub const DEFAULT_COOPERATIVE_SCAN_YIELD_EVERY_PARTITIONS: usize = 32;

fn cooperative_scan_yield_every_partitions() -> usize {
    std::env::var("FERROSA_CQL_SCAN_YIELD_EVERY_PARTITIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_COOPERATIVE_SCAN_YIELD_EVERY_PARTITIONS)
}
/// Warn once an arbitrary unbounded ORDER BY would scan at least this many
/// local SSTable bytes before sorting. The warning is a planner signal: this
/// query shape should use the spillable temp-sort table path instead of
/// silently building an unbounded in-memory sort buffer.
const ORDER_BY_TEMP_SORT_WARN_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderByExecutionPlan {
    /// ORDER BY is absent, bounded by a literal LIMIT, or follows clustering
    /// order inside a fully-specified partition.
    Inline,
    /// ORDER BY is arbitrary and unbounded; reserve a spillable temp-sort table
    /// and warn based on local SSTable bytes that would be scanned.
    SpillableTempTable { estimated_scan_bytes: u64 },
}

fn has_literal_limit(s: &SelectStatement) -> bool {
    s.limit.as_ref().and_then(Limit::as_literal).is_some()
}

fn has_full_partition_key_equality(s: &SelectStatement, table_meta: &TableMetadata) -> bool {
    table_meta.partition_key.iter().all(|pk| {
        s.where_clauses
            .iter()
            .any(|wc| !wc.token_fn && wc.column == *pk && wc.op == ComparisonOp::Eq)
    })
}

fn order_by_matches_clustering_order(s: &SelectStatement, table_meta: &TableMetadata) -> bool {
    if s.order_by.is_empty() || s.order_by.len() > table_meta.clustering_key.len() {
        return false;
    }
    s.order_by.iter().zip(table_meta.clustering_key.iter()).all(
        |((order_col, order_dir), (ck_col, ck_order))| {
            order_col == ck_col
                && matches!(
                    (order_dir, ck_order),
                    (OrderDirection::Asc, SchemaClusteringOrder::Asc)
                        | (OrderDirection::Desc, SchemaClusteringOrder::Desc)
                        | (OrderDirection::Asc, SchemaClusteringOrder::None)
                )
        },
    )
}

fn classify_order_by_execution(
    state: &SharedState,
    ks: &str,
    s: &SelectStatement,
    table_meta: &TableMetadata,
) -> OrderByExecutionPlan {
    if s.order_by.is_empty()
        || has_literal_limit(s)
        || (has_full_partition_key_equality(s, table_meta)
            && order_by_matches_clustering_order(s, table_meta))
    {
        return OrderByExecutionPlan::Inline;
    }

    OrderByExecutionPlan::SpillableTempTable {
        estimated_scan_bytes: state
            .engine
            .estimated_table_scan_bytes(ks, &s.table)
            .unwrap_or_default(),
    }
}

fn prepare_order_by_execution(
    state: &SharedState,
    ks: &str,
    s: &SelectStatement,
    table_meta: &TableMetadata,
) -> Result<Option<ferrosa_storage::TempSortTableReservation>, CqlError> {
    match classify_order_by_execution(state, ks, s, table_meta) {
        OrderByExecutionPlan::Inline => Ok(None),
        OrderByExecutionPlan::SpillableTempTable {
            estimated_scan_bytes,
        } => {
            let reservation = state
                .engine
                .reserve_order_by_temp_sort_table(ks, &s.table)
                .map_err(|e| {
                    CqlError::ServerError(format!("ORDER BY temp-sort setup failed: {e}"))
                })?;
            if estimated_scan_bytes >= ORDER_BY_TEMP_SORT_WARN_BYTES {
                tracing::warn!(
                    keyspace = ks,
                    table = %s.table,
                    order_by = ?s.order_by,
                    estimated_scan_bytes,
                    temp_sort_table = %reservation.path().display(),
                    "unbounded arbitrary ORDER BY will use a cancellable spillable temp-sort table; consider a materialized view keyed by this order or explicit LIMIT"
                );
            } else {
                tracing::info!(
                    keyspace = ks,
                    table = %s.table,
                    order_by = ?s.order_by,
                    estimated_scan_bytes,
                    temp_sort_table = %reservation.path().display(),
                    "unbounded arbitrary ORDER BY classified for cancellable spillable temp-sort table"
                );
            }
            Ok(Some(reservation))
        }
    }
}

/// ADR-020 projection fast path eligibility + ordinal computation.
///
/// Returns `Some(wanted)` if the SELECT statement is safe to route
/// through `WritePath::range_read_projected`, where `wanted` is the
/// storage-ordinal Vec of regular columns the projection asks for.
/// Returns `None` otherwise — the caller falls back to the legacy
/// `range_read` path.
///
/// Eligibility:
/// - All SELECT items are `SelectColumn::Column(_)` (no `*`, no
///   function calls — those need every cell).
/// - Static columns are not projected. The storage projection
///   target is `regular_columns` ordinal space; a SELECT that names
///   a static column needs a different path (static rows live in
///   `Partition::static_row`, separate from `Partition::rows[*].cells`).
///   We fall back to the legacy path in that case rather than silently
///   dropping static values.
///
/// An empty `wanted` Vec (e.g. `SELECT pk, ck FROM t` — projects
/// only PK/CK, no regular columns) is the BEST case: the SSTable
/// layer byte-skips every cell. PK comes from the partition key
/// bytes, CK from `Row.clustering` — both are already present in
/// every row regardless of cell projection.
///
/// Ordinal mapping mirrors `ferrosa_schema::convert` (Vec of regular
/// columns sorted by `ColumnMetadata.position`) so the indexes match
/// `SerializationHeader::regular_columns` on disk.
///
/// The caller must additionally confirm WHERE is empty — a predicate
/// on a non-projected regular column would silently evaluate against
/// the stripped (NULL) cell and produce wrong results.
fn vector_bits_from_term(term: &Term, target_type: &CqlType) -> Result<Vec<u32>, CqlError> {
    match bridge::term_to_cql_value(term, target_type)? {
        CqlValue::Vector(bits) => Ok(bits),
        other => Err(CqlError::Invalid(format!(
            "ANN query value must resolve to vector, got {other:?}"
        ))),
    }
}

fn squared_l2_distance(left: &[u32], right: &[u32]) -> Result<f32, CqlError> {
    if left.len() != right.len() {
        return Err(CqlError::Invalid(format!(
            "vector dimension mismatch: expected {}, got {}",
            left.len(),
            right.len()
        )));
    }

    Ok(left
        .iter()
        .zip(right)
        .map(|(a, b)| {
            let diff = f32::from_bits(*a) - f32::from_bits(*b);
            diff * diff
        })
        .sum())
}

fn apply_ann_of_ordering(
    rows: &mut Vec<Vec<Option<CqlValue>>>,
    ann_col: &str,
    ann_query: &Term,
    all_col_names: &[String],
    all_col_types: &[CqlType],
) -> Result<(), CqlError> {
    let col_idx = all_col_names
        .iter()
        .position(|name| name == ann_col)
        .ok_or_else(|| CqlError::Invalid(format!("unknown ANN OF column {ann_col}")))?;
    let target_type = all_col_types.get(col_idx).ok_or_else(|| {
        CqlError::Invalid(format!("missing type metadata for ANN OF column {ann_col}"))
    })?;
    let query_bits = vector_bits_from_term(ann_query, target_type)?;

    let mut keyed_rows = Vec::with_capacity(rows.len());
    for (ordinal, row) in rows.drain(..).enumerate() {
        let value = row
            .get(col_idx)
            .ok_or_else(|| CqlError::Invalid(format!("missing ANN OF column {ann_col} in row")))?;
        // A row whose ANN column is NULL or non-vector simply has no embedding
        // to score against, so it cannot participate in a similarity search.
        // Skip it rather than failing the whole query — one un-embedded row must
        // not poison ANN for every embedded row in the table.
        let Some(CqlValue::Vector(row_bits)) = value else {
            continue;
        };
        let distance = squared_l2_distance(row_bits, &query_bits)?;
        keyed_rows.push((distance, ordinal, row));
    }

    keyed_rows.sort_by(
        |(left_distance, left_ordinal, _), (right_distance, right_ordinal, _)| {
            left_distance
                .total_cmp(right_distance)
                .then_with(|| left_ordinal.cmp(right_ordinal))
        },
    );

    // Rebuild from the (filtered, ranked) rows — the result excludes any skipped
    // un-embedded rows, so the cardinality may shrink.
    *rows = keyed_rows.into_iter().map(|(_, _, row)| row).collect();
    Ok(())
}

/// Default `k` cap for `ANN OF` queries that omit `LIMIT`, bounding the index
/// consult and the row fetch.
const ANN_DEFAULT_K: usize = 100;

/// `ef_search` width used for router-level ANN index consults. The brute-force
/// memtable index ignores it; persisted HNSW sidecars use it as the search
/// beam width. Sized comfortably above typical `k`.
const ANN_EF_SEARCH: usize = 128;

/// Resolve the registered **vector** index name for `ann_column` on
/// `ks`.`table`, if one exists. Returns `None` when no vector index targets the
/// column, in which case the caller falls through to the unchanged full-scan
/// ANN ordering path.
fn vector_index_for_ann_column(
    snap: &ferrosa_schema::SchemaSnapshot,
    ks: &str,
    table: &str,
    ann_column: &str,
) -> Option<String> {
    snap.indexes
        .iter()
        .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == table)
        .find(|(_, meta)| {
            meta.index_type == ferrosa_index::IndexType::Vector
                && meta.target_columns.iter().any(|c| c == ann_column)
        })
        .map(|(_, meta)| meta.name.clone())
}

fn projection_storage_ordinals(
    select_columns: &[SelectColumn],
    table_meta: &TableMetadata,
) -> Option<Vec<u16>> {
    // All projected items must be simple column names.
    let names: Vec<&str> = select_columns
        .iter()
        .map(|c| match c {
            SelectColumn::Column(name) => Some(name.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    // Build the regular column list in Cassandra **name-sorted** order —
    // exactly what `ferrosa_schema::convert::to_storage_schema` emits into the
    // SSTable's `SerializationHeader.regular_columns` (and what
    // `storage_column_index` indexes against). Sorting by declared `position`
    // here is WRONG: for a table whose declared column order differs from
    // name order, the projected SSTable read would then decode the wrong
    // column and the requested column would come back NULL once the memtable
    // flushed (the scholarly-search embedding → ANN regression).
    let mut regulars: Vec<&ColumnMetadata> = table_meta
        .columns
        .values()
        .filter(|c| c.kind == ColumnKind::Regular)
        .collect();
    regulars.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));

    let mut wanted: Vec<u16> = Vec::new();
    for name in &names {
        if let Some(col) = table_meta
            .columns
            .iter()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
            .map(|(_, c)| c)
        {
            match col.kind {
                ColumnKind::Regular => {
                    if let Some(idx) = regulars
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                    {
                        wanted.push(idx as u16);
                    }
                }
                ColumnKind::PartitionKey | ColumnKind::Clustering => {
                    // Live in the partition-key bytes / clustering
                    // bytes, not cells. No-op for `wanted`; the row
                    // carries them regardless of cell projection.
                }
                ColumnKind::Static => {
                    // Static columns live in Partition::static_row,
                    // not in Partition::rows[*].cells. The current
                    // projected path doesn't yet handle static
                    // columns separately — bail out so the legacy
                    // path returns the full static row correctly.
                    return None;
                }
            }
        } else {
            // Unknown column name — bail out so the legacy path
            // returns the right error.
            return None;
        }
    }
    // An empty `wanted` is intentional and valid: SELECT pk, ck
    // (no regular columns) means "skip every cell". The SSTable
    // layer's `read_partition_projected` accepts `&[]` and returns
    // rows whose `cells` is empty — exactly what we want.
    Some(wanted)
}

fn projection_storage_ordinals_for_count_predicates(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
) -> Option<Vec<u16>> {
    let names: Vec<SelectColumn> = where_clauses
        .iter()
        .map(|wc| SelectColumn::Column(wc.column.clone()))
        .collect();
    projection_storage_ordinals(&names, table_meta)
}

fn projection_storage_ordinals_for_select_scan(
    select: &SelectStatement,
    table_meta: &TableMetadata,
) -> Option<Vec<u16>> {
    let mut needed = select.columns.clone();
    if let Some((ann_col, _)) = &select.ann_of {
        let already_needed = needed.iter().any(|column| match column {
            SelectColumn::Column(name) => name.eq_ignore_ascii_case(ann_col),
            SelectColumn::Star => true,
            SelectColumn::FunctionCall { .. } => false,
        });
        if !already_needed {
            needed.push(SelectColumn::Column(ann_col.clone()));
        }
    }
    projection_storage_ordinals(&needed, table_meta)
}

fn is_count_only_select(columns: &[SelectColumn]) -> bool {
    !columns.is_empty()
        && columns.iter().all(|c| {
            matches!(c, SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count"))
        })
}

fn safe_partition_key_filter_row_limit(
    s: &SelectStatement,
    table_meta: &TableMetadata,
    count_only_select: bool,
) -> Option<usize> {
    if count_only_select || !s.order_by.is_empty() {
        return None;
    }
    let limit = s.limit.as_ref()?.as_literal()? as usize;
    if limit == 0 || s.where_clauses.is_empty() {
        return None;
    }

    let partition_keys: std::collections::HashSet<&str> = table_meta
        .partition_key
        .iter()
        .map(String::as_str)
        .collect();

    // Safe only when every predicate is an equality on a partition-key
    // component. For matching partitions, all rows satisfy those predicates,
    // so returning at most LIMIT rows per partition cannot underfill due to a
    // row-level post-filter. Non-PK predicates must continue using uncapped
    // partition rows to avoid dropping the first matching row after the cap.
    s.where_clauses
        .iter()
        .all(|wc| {
            !wc.token_fn && wc.op == ComparisonOp::Eq && partition_keys.contains(wc.column.as_str())
        })
        .then_some(limit)
}

fn row_matches_select_predicates(
    row: &[Option<CqlValue>],
    s: &SelectStatement,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    table_meta: &TableMetadata,
    ks: &str,
    state: &SharedState,
) -> Result<bool, CqlError> {
    evaluate_where_predicates(
        row,
        &s.where_clauses,
        all_col_names,
        all_col_types,
        table_meta,
        ks,
        state,
    )
}

struct PartitionRowContext<'a> {
    all_col_names: &'a [String],
    all_col_types: &'a [CqlType],
    pk_indices: &'a [usize],
    ck_indices: &'a [usize],
    storage_to_table: &'a [usize],
}

struct SelectPredicateContext<'a> {
    statement: &'a SelectStatement,
    table_meta: &'a TableMetadata,
    keyspace: &'a str,
    state: &'a SharedState,
}

async fn count_rows_from_partitions(
    partitions: &[ferrosa_sstable::types::Partition],
    row_context: PartitionRowContext<'_>,
    predicate_context: SelectPredicateContext<'_>,
) -> Result<i64, CqlError> {
    let mut count = 0_i64;
    for (idx, partition) in partitions.iter().enumerate() {
        for row in bridge::partition_to_rows_with_storage_mapping(
            partition,
            row_context.all_col_names,
            row_context.all_col_types,
            row_context.pk_indices,
            row_context.ck_indices,
            row_context.storage_to_table,
        ) {
            if row_matches_select_predicates(
                &row,
                predicate_context.statement,
                row_context.all_col_names,
                row_context.all_col_types,
                predicate_context.table_meta,
                predicate_context.keyspace,
                predicate_context.state,
            )? {
                count += 1;
            }
        }
        if should_yield_during_partition_scan(idx + 1, cooperative_scan_yield_every_partitions()) {
            tokio::task::yield_now().await;
        }
    }
    Ok(count)
}

fn filter_rows_by_select_predicates(
    rows: &mut Vec<Vec<Option<CqlValue>>>,
    statement: &SelectStatement,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    table_meta: &TableMetadata,
    ks: &str,
    state: &SharedState,
) -> Result<(), CqlError> {
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        if row_matches_select_predicates(
            &row,
            statement,
            all_col_names,
            all_col_types,
            table_meta,
            ks,
            state,
        )? {
            kept.push(row);
        }
    }
    *rows = kept;
    Ok(())
}

async fn extend_rows_from_partitions(
    partitions: &[ferrosa_sstable::types::Partition],
    all_rows: &mut Vec<Vec<Option<CqlValue>>>,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    pk_indices: &[usize],
    ck_indices: &[usize],
    storage_to_table: &[usize],
) {
    for (idx, partition) in partitions.iter().enumerate() {
        let mut prows = bridge::partition_to_rows_with_storage_mapping(
            partition,
            all_col_names,
            all_col_types,
            pk_indices,
            ck_indices,
            storage_to_table,
        );
        all_rows.append(&mut prows);
        if should_yield_during_partition_scan(idx + 1, cooperative_scan_yield_every_partitions()) {
            tokio::task::yield_now().await;
        }
    }
}

async fn count_rows_from_partition_stream(
    mut stream: ferrosa_cluster::write_path::PartitionResultStream,
    row_context: PartitionRowContext<'_>,
    predicate_context: SelectPredicateContext<'_>,
) -> Result<i64, CqlError> {
    let mut count = 0_i64;
    let mut processed_partitions = 0usize;
    while let Some(partition) = stream.next().await {
        let partition = partition?;
        for row in bridge::partition_to_rows_with_storage_mapping(
            &partition,
            row_context.all_col_names,
            row_context.all_col_types,
            row_context.pk_indices,
            row_context.ck_indices,
            row_context.storage_to_table,
        ) {
            if row_matches_select_predicates(
                &row,
                predicate_context.statement,
                row_context.all_col_names,
                row_context.all_col_types,
                predicate_context.table_meta,
                predicate_context.keyspace,
                predicate_context.state,
            )? {
                count += 1;
            }
        }
        processed_partitions += 1;
        if should_yield_during_partition_scan(
            processed_partitions,
            cooperative_scan_yield_every_partitions(),
        ) {
            tokio::task::yield_now().await;
        }
    }
    Ok(count)
}

async fn extend_rows_from_partition_stream(
    mut stream: ferrosa_cluster::write_path::PartitionResultStream,
    all_rows: &mut Vec<Vec<Option<CqlValue>>>,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    pk_indices: &[usize],
    ck_indices: &[usize],
    storage_to_table: &[usize],
) -> Result<(), CqlError> {
    let mut processed_partitions = 0usize;
    while let Some(partition) = stream.next().await {
        let partition = partition?;
        let mut prows = bridge::partition_to_rows_with_storage_mapping(
            &partition,
            all_col_names,
            all_col_types,
            pk_indices,
            ck_indices,
            storage_to_table,
        );
        all_rows.append(&mut prows);
        processed_partitions += 1;
        if should_yield_during_partition_scan(
            processed_partitions,
            cooperative_scan_yield_every_partitions(),
        ) {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// Resume cursor decoded from the wire `paging_state` for a streaming scan.
///
/// `partition_key` / `clustering_key` are the raw serialized bytes of the last
/// row returned on the previous page. Resume re-enters the scan at the last
/// partition key (inclusive) and skips every row in that partition whose
/// clustering bytes are `<= clustering_key`, so an exactly-once continuation
/// holds whether the previous page ended on a partition boundary or mid-way
/// through a wide partition.
struct StreamResumeCursor {
    partition_key: Vec<u8>,
    clustering_key: Vec<u8>,
}

impl StreamResumeCursor {
    fn from_paging_state(paging_state: Option<&[u8]>) -> Result<Option<Self>, CqlError> {
        match paging_state {
            None => Ok(None),
            Some(bytes) => {
                let state = crate::paging::PagingState::decode(bytes)?;
                Ok(Some(Self {
                    partition_key: state.partition_key,
                    clustering_key: state.clustering_key,
                }))
            }
        }
    }
}

/// One bounded page collected from a partition stream.
struct StreamedPage {
    rows: Vec<Vec<Option<CqlValue>>>,
    /// Continuation token for the next page, `None` when the scan is exhausted.
    next_paging_state: Option<Vec<u8>>,
}

/// Collect at most `page_size` rows from a partition stream, encoding a
/// `PagingState` continuation when more rows remain.
///
/// This is the coordinator-side OOM bound for unbounded `SELECT *`-shaped
/// scans: the stream is fragmented (intra-partition streaming), so the
/// producer holds `O(num_sources + K)` rows resident, and this consumer never
/// retains more than `page_size` output rows. The previous behavior buffered
/// the entire table into `all_rows` before returning.
///
/// Correctness: partitions arrive in token order and rows in clustering order.
/// `resume` re-enters at the last partition key (inclusive) and drops rows
/// already emitted (`clustering <= resume.clustering_key` within that key), so
/// the union of all pages equals the whole scan with no gaps or duplicates,
/// including a wide partition that spans pages.
async fn collect_page_from_partition_stream(
    mut stream: ferrosa_cluster::write_path::PartitionResultStream,
    page_size: usize,
    resume: Option<StreamResumeCursor>,
    row_context: PartitionRowContext<'_>,
) -> Result<StreamedPage, CqlError> {
    debug_assert!(page_size > 0, "page_size must be positive");

    let mut rows: Vec<Vec<Option<CqlValue>>> = Vec::with_capacity(page_size);
    // Cursor bytes for the most recently accepted row.
    let mut last_pk: Vec<u8> = Vec::new();
    let mut last_ck: Vec<u8> = Vec::new();
    // Set once we have a full page and then observe one more row — that extra
    // row proves a continuation is needed.
    let mut more_rows_remain = false;
    let mut processed_partitions = 0usize;

    'outer: while let Some(partition) = stream.next().await {
        let partition = partition?;
        let pk_bytes = partition.key.key.as_bytes().to_vec();
        let paired = bridge::partition_to_rows_with_clustering(
            &partition,
            row_context.all_col_names,
            row_context.all_col_types,
            row_context.pk_indices,
            row_context.ck_indices,
            row_context.storage_to_table,
        );

        for (clustering, output_row) in paired {
            // Skip rows already returned on a previous page. Only applies to
            // the partition the cursor stopped in; once we pass it (or land in
            // a later partition), nothing is skipped.
            if let Some(ref cur) = resume {
                if pk_bytes == cur.partition_key && clustering <= cur.clustering_key {
                    continue;
                }
            }

            if rows.len() == page_size {
                // We already have a full page and just found another row — a
                // continuation is required. Stop without consuming further.
                more_rows_remain = true;
                break 'outer;
            }

            rows.push(output_row);
            last_pk = pk_bytes.clone();
            last_ck = clustering;
        }

        processed_partitions += 1;
        if should_yield_during_partition_scan(
            processed_partitions,
            cooperative_scan_yield_every_partitions(),
        ) {
            tokio::task::yield_now().await;
        }
    }

    let next_paging_state = if more_rows_remain {
        Some(
            crate::paging::PagingState {
                partition_key: last_pk,
                clustering_key: last_ck,
                remaining_in_partition: false,
            }
            .encode(),
        )
    } else {
        None
    };

    Ok(StreamedPage {
        rows,
        next_paging_state,
    })
}

fn storage_to_table_indices(table_meta: &TableMetadata) -> Vec<usize> {
    let mut pairs: Vec<(u16, usize)> = table_meta
        .columns
        .iter()
        .filter(|(_, col)| matches!(col.kind, ColumnKind::Regular | ColumnKind::Static))
        .filter_map(|(name, _)| {
            let storage_idx = table_meta.storage_column_index(name)?;
            let table_idx = table_meta.columns.get_index_of(name)?;
            Some((storage_idx, table_idx))
        })
        .collect();
    pairs.sort_by_key(|(storage_idx, _)| *storage_idx);
    pairs.into_iter().map(|(_, table_idx)| table_idx).collect()
}

fn should_yield_during_partition_scan(processed_partitions: usize, yield_every: usize) -> bool {
    yield_every > 0 && processed_partitions > 0 && processed_partitions.is_multiple_of(yield_every)
}

/// Extract the Unix-epoch millisecond timestamp from a Timeuuid.
///
/// Converts from 100-nanosecond intervals since 1582-10-15 (UUID epoch) to
/// milliseconds since 1970-01-01 (Unix epoch).
fn eval_to_timestamp(timeuuid: &CqlValue) -> Result<CqlValue, CqlError> {
    match timeuuid {
        CqlValue::Timeuuid(uuid) => {
            let bytes = uuid.as_bytes();
            let time_low = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64;
            let time_mid = u16::from_be_bytes([bytes[4], bytes[5]]) as u64;
            let time_hi = (u16::from_be_bytes([bytes[6], bytes[7]]) & 0x0FFF) as u64;
            let uuid_ts = time_low | (time_mid << 32) | (time_hi << 48);
            let millis = (uuid_ts - crate::bridge::UUID_EPOCH_OFFSET) / 10_000;
            Ok(CqlValue::Timestamp(millis as i64))
        }
        _ => Err(CqlError::Invalid(
            "toTimestamp requires a timeuuid argument".into(),
        )),
    }
}

// ── UDF/UDA query-time resolution types ──────────────────────────────────

/// A user-defined function (scalar or aggregate) resolved from the schema,
/// ready for query-time execution.
#[derive(Debug, Clone)]
struct ResolvedFunction {
    /// Keyspace and function name.
    func_name: String,
    /// The keyspace in which the function is defined.
    func_keyspace: String,
    /// Scalar or aggregate.
    kind: ResolvedFunctionKind,
    /// CQL return type of the function.
    return_type: CqlType,
    /// Whether the function should be called when any argument is NULL.
    called_on_null: bool,
    /// Indices into the full row column list for each argument that references
    /// a column (non-column literal args use `usize::MAX` as sentinel).
    arg_indices: Vec<usize>,
    /// The original `Term` arguments (needed for literal extraction).
    arg_terms: Vec<Term>,
    /// The inferred CQL types of each argument.
    arg_types: Vec<CqlType>,
    /// Display name (alias or default) for the result column.
    display_name: String,
}

/// Whether a resolved function is scalar or aggregate.
#[derive(Debug, Clone)]
enum ResolvedFunctionKind {
    Scalar,
    Aggregate { init_cond: Option<CqlValue> },
}

/// Shared state available to all request handlers.
pub struct SharedState {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub node_config: Arc<NodeConfig>,
    pub cluster_state: Arc<ArcSwap<ferrosa_cluster::ClusterStateHolder>>,
    pub write_path: Arc<ArcSwap<WritePath>>,
    pub ddl_path: Arc<ArcSwap<DdlPath>>,
    pub prepared_cache: Arc<PreparedCache>,
    pub connection_tracker: Arc<ConnectionTracker>,
    pub query_tracker: Arc<QueryTracker>,
    /// Records full-scan occurrences for `system_observability.full_scan_reasons`.
    pub full_scan_tracker: Arc<crate::virtual_tables::FullScanTracker>,
    /// Records secondary-index usage for `system_observability.index_usage`.
    pub index_usage_tracker: Arc<crate::virtual_tables::IndexUsageTracker>,
    /// WASM UDF executor for compiling and invoking user-defined functions.
    pub udf_executor: Arc<UdfExecutor>,
    /// Broadcast channel for CQL EVENT push notifications.
    pub event_sender: tokio::sync::broadcast::Sender<crate::event::CqlEvent>,
    /// Mode controller for checking CQL readiness (pair mode gating).
    pub mode_controller: Arc<ferrosa_cluster::ModeController>,
    /// CQL request metrics (per-opcode counters and error counter).
    pub cql_metrics: Arc<CqlMetrics>,
    /// Decides whether a given connection should see public or internal
    /// topology addresses in `system.local` / `system.peers_v2`.
    pub topology_policy: ClientTopologyPolicy,
    /// When `true`, permission failures are logged as warnings and allowed
    /// through instead of returning 0x2100 Unauthorized.  Set from
    /// `FERROSA_AUTH_WARN=true` to enable the soak observation period.
    pub auth_warn: bool,
    /// Peer connection manager for Accord coordinator fanout.
    ///
    /// `None` in standalone mode or when the cluster layer has not yet
    /// provided a `PeerManager` (e.g. in unit tests).  When `None`, LWT
    /// statements in cluster mode return a fail-loud `ServerError` instead
    /// of routing through Accord.
    pub peer_manager: Option<Arc<ferrosa_net::peer::PeerManager>>,
    /// Hybrid logical clock used to generate monotone transaction timestamps.
    ///
    /// `None` when `peer_manager` is `None`.
    pub accord_clock: Option<Arc<ferrosa_common::accord::HybridLogicalClock>>,
}

/// Per-request context: authentication, current keyspace, and consistency level.
pub struct RequestContext<'a> {
    pub auth: &'a AuthContext,
    pub current_keyspace: &'a Option<String>,
    /// Client-requested consistency level parsed from the CQL protocol frame.
    pub consistency: ConsistencyLevel,
    /// Serial consistency level for lightweight transactions (LWT).
    /// Parsed from the CQL QUERY/EXECUTE frame when flags bit 0x0010 is set.
    pub serial_consistency: Option<ConsistencyLevel>,
    /// Pagination parameters from the QUERY/EXECUTE frame.
    pub paging: crate::paging::PagingParams,
    /// Remote peer address (e.g. "10.0.0.1:54321"), used by the query tracker
    /// for the `client_address` column in `system_observability.active_queries`.
    pub client_address: String,
}

/// Raw (un-encoded) result of a user-table SELECT, used by delta subscriptions.
///
/// Carries the projected column metadata and row data so the subscription
/// delivery layer can compute a row-level diff against the previous delivery
/// without re-parsing encoded CQL RESULT frames.
///
/// The `paging_state` field carries the opaque continuation token when the
/// original query was paginated. Pass it through when encoding for normal
/// SELECT responses; ignore it in subscription/delta contexts.
pub struct SelectRawResult {
    pub column_names: Vec<String>,
    pub column_types: Vec<CqlType>,
    pub rows: Vec<Vec<Option<CqlValue>>>,
    pub keyspace: String,
    pub table: String,
    /// Opaque paging continuation token, `None` when no more pages.
    pub paging_state: Option<Vec<u8>>,
}

impl SelectRawResult {
    /// Encode this result into a CQL RESULT frame body (Rows kind = 0x0002).
    ///
    /// Includes the paging continuation token when present so normal SELECT
    /// responses are indistinguishable from the previous implementation.
    pub fn encode(self) -> BytesMut {
        result::encode_rows_paged(
            &self.column_names,
            &self.column_types,
            &self.keyspace,
            &self.table,
            &self.rows,
            self.paging_state.as_deref(),
        )
    }
}

/// Result of routing a statement.
pub enum RouteResult {
    /// A CQL RESULT frame body.
    Result(BytesMut),
    /// USE keyspace: returns the new keyspace name and a SetKeyspace frame body.
    SetKeyspace(String, BytesMut),
    /// Subscription accepted — connection should spawn a polling task.
    Subscribe {
        inner: Box<Statement>,
        interval: Option<std::time::Duration>,
        delta: bool,
    },
    /// Unsubscribe — cancel one or all subscriptions.
    Unsubscribe { stream_id: Option<u16> },
}

// ── Accord LWT dispatch ──────────────────────────────────────────────────

/// Materialize an LWT statement into the real partition key and the encoded
/// mutation that Accord must replicate and apply.
///
/// Returns `(decorated_key_bytes, mutation_bytes)` where:
/// - `decorated_key_bytes` is the partition-key token+bytes used by Accord for
///   conflict ordering (the transaction's "key").
/// - `mutation_bytes` is a self-describing commit-log [`Mutation`]
///   (keyspace/table, `DecoratedKey`, rows, timestamp) serialized with
///   `serialize_into`, replacing the former `b"lwt-placeholder-key"` stub that
///   carried nothing. The mutation is now threaded across the wire so the Apply
///   phase has the real bytes to persist.
///
///   NOTE: a replica only *persists* these bytes once it is constructed with an
///   [`EngineStorageApplier`](ferrosa_cluster::accord::apply). Production today
///   still builds `AccordStateMachine` with the default `NoopStorageApplier`
///   (see `controller/cluster.rs`), so on a live replica the decode-and-write
///   step is a no-op pending the inc6 startup-wiring increment — no client path
///   reaches `route_lwt_via_accord` yet, so this is not user-visible.
///
/// Reuses the same `materialize_insert`/`materialize_update`/`materialize_delete`
/// path as logged batches, so an Accord-routed INSERT/UPDATE/DELETE produces
/// byte-identical storage writes to the local path.
///
/// # Errors
///
/// Fails loud (`CqlError::Invalid`) for any statement that is not an
/// INSERT/UPDATE/DELETE — never silently fabricates a mutation.
fn build_lwt_mutation(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    stmt: &Statement,
) -> Result<(Vec<u8>, Vec<u8>), CqlError> {
    use ferrosa_storage::Mutation;

    let now_micros = || -> Result<i64, CqlError> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64)
    };

    let (table_id, key, row, ts) = match stmt {
        Statement::Insert(s) => materialize_insert(state, ctx, s, now_micros()?)?,
        Statement::Update(s) => materialize_update(state, ctx, s, now_micros()?)?,
        Statement::Delete(s) => materialize_delete(state, ctx, s, now_micros()?)?,
        _ => {
            return Err(CqlError::Invalid(
                "LWT via Accord supports only INSERT/UPDATE/DELETE statements".into(),
            ));
        }
    };

    let key_bytes = key.key.as_bytes().to_vec();

    let mutation = Mutation::new(
        table_id.keyspace.clone(),
        table_id.table.clone(),
        key,
        vec![row],
        ts,
    );
    let mut mutation_bytes = vec![0u8; mutation.serialized_size()];
    mutation.serialize_into(&mut mutation_bytes);

    Ok((key_bytes, mutation_bytes))
}

/// Decode the agreed row-at-`t` bytes (a serialized single-partition
/// [`Mutation`](ferrosa_storage::Mutation)) into a `column -> value` map for IF
/// evaluation, using the SAME positional decode the local read path uses
/// ([`bridge::partition_to_rows_with_storage_mapping`]).
///
/// Returns `Ok(None)` when there is no agreed row (row absent at `t`).
fn decode_agreed_row_to_map(
    schema: &Schema,
    ks: &str,
    table: &str,
    agreed_row: Option<&[u8]>,
) -> Result<Option<HashMap<String, Option<CqlValue>>>, CqlError> {
    use ferrosa_storage::Mutation;

    let bytes = match agreed_row {
        None => return Ok(None),
        Some(b) => b,
    };

    let mutation = Mutation::deserialize_from(bytes)
        .map_err(|e| CqlError::ServerError(format!("failed to decode LWT read-vote row: {e}")))?;

    let snap = schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), table.to_string()))
        .ok_or_else(|| CqlError::Invalid(format!("table {ks}.{table} not found")))?;

    let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
    let all_col_types: Vec<CqlType> = table_meta
        .columns
        .values()
        .map(|c| resolve_col_type(&c.column_type, ks, schema))
        .collect::<Result<Vec<_>, _>>()?;
    let pk_indices: Vec<usize> = table_meta
        .partition_key
        .iter()
        .filter_map(|name| table_meta.columns.get_index_of(name))
        .collect();
    let ck_indices: Vec<usize> = table_meta
        .clustering_key
        .iter()
        .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
        .collect();
    let storage_to_table = storage_to_table_indices(table_meta);

    let partition = ferrosa_sstable::types::Partition {
        key: mutation.key.clone(),
        deletion: ferrosa_sstable::types::DeletionTime::LIVE,
        static_row: None,
        rows: mutation.rows.clone(),
    };

    let rows = bridge::partition_to_rows_with_storage_mapping(
        &partition,
        &all_col_names,
        &all_col_types,
        &pk_indices,
        &ck_indices,
        &storage_to_table,
    );

    let row_values = match rows.into_iter().next() {
        None => return Ok(None),
        Some(r) => r,
    };

    let map: HashMap<String, Option<CqlValue>> =
        all_col_names.iter().cloned().zip(row_values).collect();
    Ok(Some(map))
}

/// Resolve the `(keyspace, table)` an LWT statement targets.
fn lwt_keyspace_table(
    ctx: &RequestContext<'_>,
    stmt: &Statement,
) -> Result<(String, String), CqlError> {
    let (ks_opt, table) = match stmt {
        Statement::Insert(s) => (&s.keyspace, &s.table),
        Statement::Update(s) => (&s.keyspace, &s.table),
        Statement::Delete(s) => (&s.keyspace, &s.table),
        _ => {
            return Err(CqlError::Invalid(
                "LWT via Accord supports only INSERT/UPDATE/DELETE".into(),
            ))
        }
    };
    let ks = resolve_keyspace(ks_opt, ctx.current_keyspace)?.to_string();
    Ok((ks, table.clone()))
}

/// Columns (name + type) to include in a `[applied]=false` LWT result set.
///
/// For a generic `IF`, these are the IF-condition columns. For `INSERT IF NOT
/// EXISTS` (and any path whose statement carries no explicit conditions), all
/// table columns are returned (the Cassandra contract returns the full
/// conflicting row). Returns an empty list when `lwt.applied` (no row needed).
fn lwt_condition_columns(
    state: &SharedState,
    ks: &str,
    table: &str,
    lwt: &crate::accord_router::LwtResult,
) -> Result<Vec<(String, CqlType)>, CqlError> {
    if lwt.applied {
        return Ok(Vec::new());
    }
    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), table.to_string()))
        .ok_or_else(|| CqlError::Invalid(format!("table {ks}.{table} not found")))?;

    let mut cols = Vec::new();
    for name in table_meta.columns.keys() {
        let col_meta = &table_meta.columns[name];
        let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
        cols.push((name.clone(), cql_type));
    }
    Ok(cols)
}

/// Route a LWT statement through the Accord consensus protocol.
///
/// Constructs an `AccordCoordinatorDriver`, runs the full PreAccept → Commit
/// protocol over real TCP, and returns a CQL `[applied]` result set.
///
/// The replica set is built dynamically from `PeerManager::live_peer_ids()`
/// plus the local node's `host_id`. This ensures newly joined peers are
/// included without requiring a restart.
///
/// # Errors
///
/// Returns `CqlError::ServerError` when:
/// - The replica set has fewer than 1 member (no peers connected).
/// - The Accord quorum cannot be reached (network failure / too few replicas).
async fn route_lwt_via_accord(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    stmt: &Statement,
    peers: Arc<ferrosa_net::peer::PeerManager>,
    clock: Arc<ferrosa_common::accord::HybridLogicalClock>,
) -> Result<RouteResult, CqlError> {
    use ferrosa_cluster::accord::AccordCoordinatorDriver;

    // Build the replica set from the live peer map plus this node itself.
    // Ordering is deterministic: local node first, then peers sorted by UUID.
    let host_id = state.node_config.host_id;
    let mut replica_ids: Vec<uuid::Uuid> = peers.live_peer_ids();
    // Include local node if not already in the list.
    if !replica_ids.contains(&host_id) {
        replica_ids.push(host_id);
    }
    replica_ids.sort_unstable();

    if replica_ids.is_empty() {
        return Err(CqlError::ServerError(
            "Accord replica set is empty — no peers connected for LWT consensus".into(),
        ));
    }

    // Derive a stable u64 node_id from the host UUID (first 8 bytes big-endian).
    let host_bytes = host_id.as_bytes();
    let node_id = u64::from_be_bytes(host_bytes[..8].try_into().expect("uuid has 16 bytes"));

    // Extract the REAL partition key + serialized mutation from the statement.
    // `key` is the Accord conflict-ordering key (partition-key bytes); `mutation`
    // is the encoded commit-log Mutation that each replica decodes and writes in
    // the Apply phase. This replaces the former `b"lwt-placeholder-key"` stub
    // that persisted nothing (the phantom-write bug).
    let (key, mutation) = build_lwt_mutation(state, ctx, stmt)?;

    // Resolve the target keyspace/table for the generic-IF read-vote so replicas
    // (and the coordinator's own local reader) can target the read-at-`t`.
    let (ks, table) = lwt_keyspace_table(ctx, stmt)?;

    // Classify the IF predicate. `NotExists` uses the replica existence path;
    // `Generic` reads the row at `t` so the coordinator evaluates `IF col=val`.
    use crate::accord_router::{classify_lwt, LwtPredicateKind};
    use ferrosa_cluster::accord::ReadPredicate;
    let predicate_kind = classify_lwt(stmt);
    let read_predicate = match predicate_kind {
        Some(LwtPredicateKind::Generic) => ReadPredicate::ReadRow {
            keyspace: ks.clone(),
            table: table.clone(),
        },
        // NotExists, or a non-LWT statement reaching here (defensive): keep the
        // existence semantics.
        _ => ReadPredicate::NotExists,
    };

    let mut driver = AccordCoordinatorDriver::new(
        node_id,
        replica_ids,
        peers,
        false, // not leaseholder (derive from TokenRing in future)
        &clock,
        key,
        mutation,
    )
    .with_read_predicate(read_predicate);

    // Give the coordinator a local applier so its OWN replica persists the
    // mutation it coordinates (its self-send Apply RPC is unreachable). Without
    // this the coordinator node silently lacks its own LWT writes. For the
    // generic path also wire a local reader so its read-at-`t` counts toward the
    // F+1 row agreement and matches the replicas that applied.
    {
        let applier = Arc::new(ferrosa_cluster::accord::EngineStorageApplier::new(
            state.engine.clone(),
        ));
        driver = driver.with_local_applier(applier);
    }
    if matches!(predicate_kind, Some(LwtPredicateKind::Generic)) {
        let reader = Arc::new(ferrosa_cluster::accord::EngineStorageReader::new(
            state.engine.clone(),
        ));
        driver = driver.with_local_reader(reader);

        // GATE THE WRITE on the IF condition. The coordinator evaluates this
        // closure against the F+1-agreed, linearizable row-at-`t` DURING the
        // read-vote phase and aborts (ConditionNotMet, no Apply) when it returns
        // false — so a failing `IF col=val` never persists its mutation. The
        // closure wraps the canonical eval_if_conditions (via
        // eval_lwt_for_statement), so there is no forked evaluator. We capture
        // owned clones (schema Arc, ks/table, the statement) because the gate is
        // a 'static closure called inside the driver.
        //
        // Fail-loud: a decode failure of our own read-vote bytes is a real
        // corruption signal. The gate cannot return an error, so it records the
        // failure into `gate_err`; we surface it after run_transaction instead of
        // silently treating it as "condition not met" (which would be a fake
        // failure). A genuine decode of the agreed bytes is deterministic and
        // identical across replicas, so the verdict is consistent.
        let schema = state.schema.clone();
        let gate_ks = ks.clone();
        let gate_table = table.clone();
        let gate_stmt = stmt.clone();
        let gate_err: Arc<std::sync::Mutex<Option<CqlError>>> =
            Arc::new(std::sync::Mutex::new(None));
        let gate_err_w = gate_err.clone();
        let gate = Box::new(move |row: Option<&[u8]>| -> bool {
            match decode_agreed_row_to_map(&schema, &gate_ks, &gate_table, row) {
                Ok(agreed) => {
                    crate::accord_router::eval_lwt_for_statement(&gate_stmt, agreed.as_ref())
                        .map(|r| r.applied)
                        // Not an LWT reaching the gate (defensive): never silently apply
                        // — treat as condition-not-met so the write does not persist.
                        .unwrap_or(false)
                }
                Err(e) => {
                    *gate_err_w.lock().expect("gate_err mutex poisoned") = Some(e);
                    // Refuse to apply on a decode error (fail closed); the real
                    // error is surfaced below.
                    false
                }
            }
        });
        driver = driver.with_condition_gate(gate);
        // Stash the error cell so the post-run match can surface a decode failure
        // rather than reporting a bogus [applied]=false.
        return finish_lwt_via_accord(state, &ks, &table, driver, Some(gate_err)).await;
    }

    finish_lwt_via_accord(state, &ks, &table, driver, None).await
}

/// Drive the Accord transaction to completion and map its result to a CQL LWT
/// result set.
///
/// For the generic-`IF` path the condition has ALREADY been evaluated inside the
/// driver (the [`AccordCoordinatorDriver::with_condition_gate`] closure), which
/// aborts with `ConditionNotMet` before the Apply phase when the condition is
/// false. So:
/// - `Ok(_)` ⇒ the condition held (or there was none) and the write applied ⇒
///   `[applied]=true`.
/// - `ConditionNotMet { current_row }` ⇒ the condition did not hold and NOTHING
///   was persisted ⇒ `[applied]=false` + the real current row.
///
/// `gate_err` carries any decode failure recorded inside the generic-IF gate so
/// it is surfaced as a server error rather than a fake `[applied]=false`.
async fn finish_lwt_via_accord(
    state: &SharedState,
    ks: &str,
    table: &str,
    mut driver: ferrosa_cluster::accord::AccordCoordinatorDriver,
    gate_err: Option<Arc<std::sync::Mutex<Option<CqlError>>>>,
) -> Result<RouteResult, CqlError> {
    use ferrosa_cluster::accord::AccordDriverError;

    let outcome = driver.run_transaction().await;

    // Surface any decode failure recorded by the generic-IF gate first — a
    // corrupt read-vote row must fail loud, never masquerade as [applied]=false.
    if let Some(cell) = &gate_err {
        if let Some(e) = cell.lock().expect("gate_err mutex poisoned").take() {
            return Err(e);
        }
    }

    match outcome {
        Ok(_) => {
            // The write applied: the read-vote either had no predicate (INSERT IF
            // NOT EXISTS reaching Ok means the row was absent) or the generic-IF
            // gate passed (the condition held). Either way [applied]=true.
            let lwt = crate::accord_router::LwtResult {
                applied: true,
                current_values: HashMap::new(),
            };
            let cond_cols = lwt_condition_columns(state, ks, table, &lwt)?;
            let result = crate::accord_router::encode_lwt_result(&lwt, ks, table, &cond_cols);
            Ok(RouteResult::Result(result))
        }
        Err(AccordDriverError::ConditionNotMet { current_row }) => {
            // The IF condition did NOT hold — and the mutation was NOT applied
            // (the driver aborted before the Apply phase). Return [applied]=false
            // with the real current row from the F+1-agreed read-vote.
            let agreed_bytes: Option<&[u8]> = if current_row.is_empty() {
                None
            } else {
                Some(current_row.as_slice())
            };
            let agreed = decode_agreed_row_to_map(&state.schema, ks, table, agreed_bytes)?;
            let lwt = crate::accord_router::LwtResult {
                applied: false,
                current_values: agreed.unwrap_or_default(),
            };
            let cond_cols = lwt_condition_columns(state, ks, table, &lwt)?;
            let result = crate::accord_router::encode_lwt_result(&lwt, ks, table, &cond_cols);
            Ok(RouteResult::Result(result))
        }
        Err(AccordDriverError::QuorumUnavailable) => Err(CqlError::ServerError(
            "Accord quorum unavailable for LWT transaction".into(),
        )),
        Err(AccordDriverError::ApplyQuorumUnavailable) => Err(CqlError::ServerError(
            "Accord apply quorum unavailable — LWT transaction may not be durable".into(),
        )),
        Err(AccordDriverError::Network(msg)) => Err(CqlError::ServerError(format!(
            "Accord network error: {msg}"
        ))),
        Err(AccordDriverError::Codec(msg)) => {
            Err(CqlError::ServerError(format!("Accord codec error: {msg}")))
        }
    }
}

// ── Main dispatch ────────────────────────────────────────────────────────

/// Route a parsed statement to the appropriate handler.
pub async fn route(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    stmt: Statement,
) -> Result<RouteResult, CqlError> {
    // Classify the statement opcode for metrics before matching.
    let opcode = match &stmt {
        Statement::Select(_) => CqlOpcode::Select,
        Statement::Insert(_) => CqlOpcode::Insert,
        Statement::Update(_) => CqlOpcode::Update,
        Statement::Delete(_) => CqlOpcode::Delete,
        Statement::Batch(_) => CqlOpcode::Batch,
        Statement::CreateKeyspace(_)
        | Statement::AlterKeyspace(_)
        | Statement::DropKeyspace(_)
        | Statement::CreateTable(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::CreateRole(_)
        | Statement::AlterRole(_)
        | Statement::DropRole(_)
        | Statement::CreateType { .. }
        | Statement::AlterType { .. }
        | Statement::DropType { .. }
        | Statement::CreateFunction { .. }
        | Statement::DropFunction { .. }
        | Statement::CreateAggregate { .. }
        | Statement::DropAggregate { .. }
        | Statement::Truncate(_)
        | Statement::Compact(_) => CqlOpcode::Ddl,
        _ => CqlOpcode::Other,
    };

    // Track the query for observability; the guard calls complete() on drop.
    // Keep this compact. Formatting the full substituted AST on every EXECUTE
    // allocates and copies bound values on the hot path, while active_queries
    // only needs a recognizable in-flight operation label.
    let query_desc = statement_query_label(&stmt, opcode);
    let keyspace = ctx.current_keyspace.as_deref().unwrap_or("");
    let _guard = state.query_tracker.begin_guarded(
        &query_desc,
        keyspace,
        &ctx.client_address,
        &ctx.auth.role,
    );

    // Check if this statement requires Accord consensus (LWT).
    // Determined by serial_consistency being set in the request context.
    {
        use crate::accord_router::{route_decision, RouteDecision, RoutingMode};
        let mode = match &**state.cluster_state.load() {
            ferrosa_cluster::ClusterStateHolder::Standalone => RoutingMode::Standalone,
            _ => RoutingMode::Cluster,
        };
        if route_decision(mode, &stmt, ctx.serial_consistency) == RouteDecision::Accord {
            // Route through the Accord consensus protocol when the coordinator
            // layer (PeerManager + replica list + HLC) has been wired in.
            // When those are absent (standalone tests, pair mode) fail loud per
            // the p0-03 policy so callers see a clear error rather than
            // silently falling through to a non-linearizable local path.
            match (&state.peer_manager, &state.accord_clock) {
                (Some(peers), Some(clock)) => {
                    return route_lwt_via_accord(state, ctx, &stmt, peers.clone(), clock.clone())
                        .await;
                }
                _ => {
                    return Err(CqlError::ServerError(
                        "LWT routing to Accord is not yet implemented at the CQL layer; \
                         see ferrosa_docs/specs/todo/p0-03b-accord-implementation-gap.md \
                         — coordinator driver (Gaps 1–3, 7) implemented on fix/p0-03b-accord-network"
                            .into(),
                    ));
                }
            }
        }
    }

    let result = match stmt {
        Statement::Select(s) => route_select(state, ctx, s).await.map(RouteResult::Result),
        Statement::Insert(i) => route_insert(state, ctx, i).await.map(RouteResult::Result),
        Statement::Update(u) => route_update(state, ctx, u).await.map(RouteResult::Result),
        Statement::Delete(d) => route_delete(state, ctx, d).await.map(RouteResult::Result),
        Statement::Batch(b) => route_batch(state, ctx, b).await.map(RouteResult::Result),
        Statement::CreateKeyspace(ck) => route_create_keyspace(state, ctx, ck)
            .await
            .map(RouteResult::Result),
        Statement::CreateTable(ct) => route_create_table(state, ctx, ct)
            .await
            .map(RouteResult::Result),
        Statement::AlterTable(at) => route_alter_table(state, ctx, at)
            .await
            .map(RouteResult::Result),
        Statement::DropTable(dt) => route_drop_table(state, ctx, dt)
            .await
            .map(RouteResult::Result),
        Statement::AlterKeyspace(ak) => route_alter_keyspace(state, ctx, ak)
            .await
            .map(RouteResult::Result),
        Statement::DropKeyspace(dk) => route_drop_keyspace(state, ctx, dk)
            .await
            .map(RouteResult::Result),
        Statement::CreateRole(cr) => route_create_role(state, ctx, cr)
            .await
            .map(RouteResult::Result),
        Statement::AlterRole(ar) => route_alter_role(state, ctx, ar)
            .await
            .map(RouteResult::Result),
        Statement::DropRole(dr) => route_drop_role(state, ctx, dr)
            .await
            .map(RouteResult::Result),
        Statement::ListRoles {
            of,
            no_recursive,
            users_alias,
        } => route_list_roles(state, ctx, of, no_recursive, users_alias)
            .await
            .map(RouteResult::Result),
        Statement::ListPermissions {
            permission,
            resource,
            of,
            no_recursive,
        } => route_list_permissions(state, ctx, permission, resource, of, no_recursive)
            .await
            .map(RouteResult::Result),
        Statement::Grant(g) => route_grant(state, ctx, g).await.map(RouteResult::Result),
        Statement::Revoke(r) => route_revoke(state, ctx, r).await.map(RouteResult::Result),
        Statement::GrantRole { role, member } => route_grant_role(state, ctx, role, member, true)
            .await
            .map(RouteResult::Result),
        Statement::RevokeRole { role, member } => route_grant_role(state, ctx, role, member, false)
            .await
            .map(RouteResult::Result),
        Statement::Use(u) => {
            validate_keyspace_exists(&state.schema, &u.keyspace)?;
            let body = result::encode_set_keyspace(&u.keyspace);
            Ok(RouteResult::SetKeyspace(u.keyspace, body))
        }
        Statement::Truncate(t) => route_truncate(state, ctx, t).await.map(RouteResult::Result),
        Statement::Compact(c) => route_compact(state, ctx, c).await.map(RouteResult::Result),
        Statement::CreateIndex(ci) => route_create_index(state, ctx, ci)
            .await
            .map(RouteResult::Result),
        Statement::DropIndex(di) => route_drop_index(state, ctx, di)
            .await
            .map(RouteResult::Result),
        Statement::Subscribe {
            inner,
            interval,
            delta,
        } => {
            // Validate: inner must be a Select
            match inner.as_ref() {
                Statement::Select(s) => {
                    let ks = s
                        .keyspace
                        .as_deref()
                        .or(ctx.current_keyspace.as_deref())
                        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;
                    state.schema.check_permission(
                        ctx.auth,
                        Permission::Select,
                        &Resource::Table(ks.to_string(), s.table.clone()),
                    )?;
                }
                _ => {
                    return Err(CqlError::Invalid(
                        "SUBSCRIBE requires a SELECT statement".into(),
                    ))
                }
            }
            Ok(RouteResult::Subscribe {
                inner,
                interval,
                delta,
            })
        }
        Statement::Unsubscribe { stream_id } => Ok(RouteResult::Unsubscribe { stream_id }),
        Statement::CreateType {
            keyspace,
            name,
            if_not_exists,
            fields,
        } => route_create_type(state, ctx, keyspace, name, if_not_exists, fields)
            .await
            .map(RouteResult::Result),
        Statement::AlterType {
            keyspace,
            name,
            alterations,
        } => route_alter_type(state, ctx, keyspace, name, alterations)
            .await
            .map(RouteResult::Result),
        Statement::DropType {
            keyspace,
            name,
            if_exists,
        } => route_drop_type(state, ctx, keyspace, name, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::CreateFunction {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            params,
            called_on_null,
            return_type,
            language,
            body,
        } => route_create_function(
            state,
            ctx,
            keyspace,
            name,
            or_replace,
            if_not_exists,
            params,
            called_on_null,
            return_type,
            language,
            body,
        )
        .await
        .map(RouteResult::Result),
        Statement::DropFunction {
            keyspace,
            name,
            arg_types,
            if_exists,
        } => route_drop_function(state, ctx, keyspace, name, arg_types, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::CreateAggregate {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            state_func,
            state_type,
            final_func,
            init_cond,
        } => route_create_aggregate(
            state,
            ctx,
            keyspace,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            state_func,
            state_type,
            final_func,
            init_cond,
        )
        .await
        .map(RouteResult::Result),
        Statement::DropAggregate {
            keyspace,
            name,
            arg_types,
            if_exists,
        } => route_drop_aggregate(state, ctx, keyspace, name, arg_types, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::Explain(s) => route_explain(state, ctx, *s).map(RouteResult::Result),
        // Accord transaction control statements are handled at the session/connection
        // level, not in the router. If they reach here, return a void result.
        Statement::BeginTransaction | Statement::Commit | Statement::Rollback => {
            Ok(RouteResult::Result(crate::result::encode_void()))
        }
    };

    // Track CQL metrics: increment per-opcode counter, and error counter on failure.
    state.cql_metrics.inc_request(opcode);
    if result.is_err() {
        state.cql_metrics.inc_error();
    }

    result
}

fn statement_query_label(stmt: &Statement, opcode: CqlOpcode) -> String {
    match stmt {
        Statement::Select(s) => {
            format!("SELECT {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table)
        }
        Statement::Insert(s) => {
            format!("INSERT {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table)
        }
        Statement::Update(s) => {
            format!("UPDATE {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table)
        }
        Statement::Delete(s) => {
            format!("DELETE {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table)
        }
        Statement::Batch(b) => format!("BATCH {}", b.statements.len()),
        Statement::CreateKeyspace(s) => format!("CREATE KEYSPACE {}", s.name),
        Statement::AlterKeyspace(s) => format!("ALTER KEYSPACE {}", s.name),
        Statement::DropKeyspace(s) => format!("DROP KEYSPACE {}", s.name),
        Statement::CreateTable(s) => format!(
            "CREATE TABLE {}.{}",
            s.keyspace.as_deref().unwrap_or(""),
            s.name
        ),
        Statement::AlterTable(s) => format!(
            "ALTER TABLE {}.{}",
            s.keyspace.as_deref().unwrap_or(""),
            s.table
        ),
        Statement::DropTable(s) => format!(
            "DROP TABLE {}.{}",
            s.keyspace.as_deref().unwrap_or(""),
            s.table
        ),
        _ => format!("{:?}", opcode),
    }
}

// ── SELECT ───────────────────────────────────────────────────────────────

async fn route_select(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: SelectStatement,
) -> Result<BytesMut, CqlError> {
    let ks = s
        .keyspace
        .as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;

    // Helper: filter system table columns to match the SELECT list.
    // If the SELECT list is `*`, return everything unchanged.
    //
    // Each `SelectColumn` is one of three shapes:
    //   - Star (`SELECT *`)              — return all columns unchanged.
    //   - Column(name)                   — project the named source column.
    //   - FunctionCall { name, args, .. } — materialise a value (zero-arg
    //     built-ins only: `now()`, `currenttimestamp()`, `uuid()`).
    //
    // Before this fix, the FunctionCall arm fell through silently and the
    // resulting projection had zero columns, which crashes both cdrs-tokio
    // and python cassandra-driver on `rows[0][0]`. See
    // specs/in-process/bug-cql-system-local-empty-columns.md.
    fn filter_system_columns(
        select_cols: &[SelectColumn],
        all_names: &[String],
        all_types: &[CqlType],
        all_rows: &[Vec<Option<CqlValue>>],
    ) -> (Vec<String>, Vec<CqlType>, Vec<Vec<Option<CqlValue>>>) {
        let is_star = select_cols.iter().any(|c| matches!(c, SelectColumn::Star));
        if is_star || select_cols.is_empty() {
            return (all_names.to_vec(), all_types.to_vec(), all_rows.to_vec());
        }

        // Each output column is either a (source-row index) projection or a
        // (per-row value generator) for builtin function calls. Encoding both
        // as `enum ProjOp` keeps the row-build loop simple.
        enum ProjOp<'a> {
            FromRow(usize),
            ConstFn(&'a str), // "now" | "currenttimestamp" | "uuid"
        }

        let mut names: Vec<String> = Vec::new();
        let mut types: Vec<CqlType> = Vec::new();
        let mut ops: Vec<ProjOp> = Vec::new();

        for sc in select_cols {
            match sc {
                SelectColumn::Column(name) => {
                    if let Some(pos) = all_names.iter().position(|n| n == name) {
                        names.push(all_names[pos].clone());
                        types.push(all_types[pos].clone());
                        ops.push(ProjOp::FromRow(pos));
                    }
                }
                SelectColumn::FunctionCall { name, alias, .. } => {
                    let fn_lower = name.to_lowercase();
                    let display = alias.clone().unwrap_or_else(|| fn_lower.clone());
                    match fn_lower.as_str() {
                        "now" => {
                            names.push(display);
                            types.push(CqlType::Timeuuid);
                            ops.push(ProjOp::ConstFn("now"));
                        }
                        "currenttimestamp" => {
                            names.push(display);
                            types.push(CqlType::Timestamp);
                            ops.push(ProjOp::ConstFn("currenttimestamp"));
                        }
                        "uuid" => {
                            names.push(display);
                            types.push(CqlType::Uuid);
                            ops.push(ProjOp::ConstFn("uuid"));
                        }
                        // Any other function call against system.* is not
                        // supported here. Skip silently to preserve prior
                        // behaviour for non-zero-arg or unrecognised names.
                        _ => {}
                    }
                }
                SelectColumn::Star => unreachable!("handled above"),
            }
        }

        let rows: Vec<Vec<Option<CqlValue>>> = all_rows
            .iter()
            .map(|row| {
                ops.iter()
                    .map(|op| match op {
                        ProjOp::FromRow(i) => row[*i].clone(),
                        ProjOp::ConstFn(fn_name) => match *fn_name {
                            "now" => Some(bridge::eval_now()),
                            "currenttimestamp" => {
                                Some(CqlValue::Timestamp(chrono::Utc::now().timestamp_millis()))
                            }
                            "uuid" => Some(CqlValue::Uuid(uuid::Uuid::new_v4())),
                            _ => None,
                        },
                    })
                    .collect()
            })
            .collect();

        (names, types, rows)
    }

    fn loopback_client_ip(client_address: &str) -> Option<IpAddr> {
        if let Ok(addr) = client_address.parse::<std::net::SocketAddr>() {
            return addr.ip().is_loopback().then_some(addr.ip());
        }
        if let Ok(ip) = client_address.parse::<IpAddr>() {
            return ip.is_loopback().then_some(ip);
        }
        None
    }

    fn harmonize_loopback_family(advertised: IpAddr, client_loopback_ip: Option<IpAddr>) -> IpAddr {
        match client_loopback_ip {
            Some(client_ip) if advertised.is_loopback() && client_ip.is_loopback() => client_ip,
            _ => advertised,
        }
    }

    // System table dispatch — no permission check needed for system tables.
    match (ks, s.table.as_str()) {
        ("system", "local") => {
            let local_addresses = [
                state.node_config.internal_rpc_address,
                state.node_config.listen_address,
                state.node_config.broadcast_address,
            ];
            let client_loopback_ip = loopback_client_ip(&ctx.client_address);
            let topology_view = state
                .topology_policy
                .topology_view_for_client_with_locals(&ctx.client_address, &local_addresses);
            let mut info = query_local_with_view(&state.schema, &state.node_config, topology_view);
            info.rpc_address = harmonize_loopback_family(info.rpc_address, client_loopback_ip);
            let col_names: Vec<String> = vec![
                "key",
                "cluster_name",
                "data_center",
                "rack",
                "host_id",
                "partitioner",
                "native_protocol_version",
                "cql_version",
                "release_version",
                "schema_version",
                "rpc_port",
                "listen_address",
                "broadcast_address",
                "rpc_address",
                "bootstrapped",
                "tokens",
            ]
            .into_iter()
            .map(String::from)
            .collect();
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Int,
                CqlType::Inet,
                CqlType::Inet,
                CqlType::Inet,
                CqlType::Varchar,
                CqlType::Set(Box::new(CqlType::Varchar)),
            ];
            let tokens_set: Vec<CqlValue> = info
                .tokens
                .iter()
                .map(|t| CqlValue::Text(t.clone()))
                .collect();
            let row = vec![
                Some(CqlValue::Text(info.key)),
                Some(CqlValue::Text(info.cluster_name)),
                Some(CqlValue::Text(info.data_center)),
                Some(CqlValue::Text(info.rack)),
                Some(CqlValue::Uuid(info.host_id)),
                Some(CqlValue::Text(info.partitioner)),
                Some(CqlValue::Text(info.native_protocol_version)),
                Some(CqlValue::Text(info.cql_version)),
                Some(CqlValue::Text(info.release_version)),
                Some(CqlValue::Uuid(info.schema_version)),
                Some(CqlValue::Int(info.rpc_port as i32)),
                Some(CqlValue::Inet(info.listen_address)),
                Some(CqlValue::Inet(info.broadcast_address)),
                Some(CqlValue::Inet(info.rpc_address)),
                Some(CqlValue::Text(info.bootstrapped)),
                Some(CqlValue::Set(tokens_set)),
            ];
            let (filt_names, filt_types, filt_rows) =
                filter_system_columns(&s.columns, &col_names, &col_types, &[row]);
            Ok(result::encode_rows(
                &filt_names,
                &filt_types,
                "system",
                "local",
                &filt_rows,
            ))
        }
        ("system", "peers" | "peers_v2") => {
            let local_addresses = [
                state.node_config.internal_rpc_address,
                state.node_config.listen_address,
                state.node_config.broadcast_address,
            ];
            let client_loopback_ip = loopback_client_ip(&ctx.client_address);
            let topology_view = state
                .topology_policy
                .topology_view_for_client_with_locals(&ctx.client_address, &local_addresses);
            let mut peers = query_peers_with_view(
                &state.schema,
                state.cluster_state.load().as_ref(),
                topology_view,
            );
            for peer in &mut peers {
                peer.native_address =
                    harmonize_loopback_family(peer.native_address, client_loopback_ip);
            }
            // p1-37: legacy `system.peers` exposes `rpc_address` (the
            // pre-peers_v2 column drivers like scylla 0.15 type-check on
            // metadata fetch). `peers_v2` replaces it with `native_address`.
            let is_v2 = s.table == "peers_v2";
            let (col_names, col_types): (Vec<String>, Vec<CqlType>) = if is_v2 {
                (
                    vec![
                        "peer",
                        "peer_port",
                        "data_center",
                        "rack",
                        "host_id",
                        "native_address",
                        "native_port",
                        "schema_version",
                        "release_version",
                        "tokens",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                    vec![
                        CqlType::Inet,
                        CqlType::Int,
                        CqlType::Varchar,
                        CqlType::Varchar,
                        CqlType::Uuid,
                        CqlType::Inet,
                        CqlType::Int,
                        CqlType::Uuid,
                        CqlType::Varchar,
                        CqlType::Set(Box::new(CqlType::Varchar)),
                    ],
                )
            } else {
                (
                    vec![
                        "peer",
                        "peer_port",
                        "data_center",
                        "rack",
                        "host_id",
                        "native_address",
                        "native_port",
                        "rpc_address",
                        "schema_version",
                        "release_version",
                        "tokens",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                    vec![
                        CqlType::Inet,
                        CqlType::Int,
                        CqlType::Varchar,
                        CqlType::Varchar,
                        CqlType::Uuid,
                        CqlType::Inet,
                        CqlType::Int,
                        CqlType::Inet,
                        CqlType::Uuid,
                        CqlType::Varchar,
                        CqlType::Set(Box::new(CqlType::Varchar)),
                    ],
                )
            };
            let rows: Vec<Vec<Option<CqlValue>>> = peers
                .iter()
                .map(|p| {
                    let tokens_set: Vec<CqlValue> =
                        p.tokens.iter().map(|t| CqlValue::Text(t.clone())).collect();
                    if is_v2 {
                        vec![
                            Some(CqlValue::Inet(p.peer)),
                            Some(CqlValue::Int(p.peer_port as i32)),
                            Some(CqlValue::Text(p.data_center.clone())),
                            Some(CqlValue::Text(p.rack.clone())),
                            Some(CqlValue::Uuid(p.host_id)),
                            Some(CqlValue::Inet(p.native_address)),
                            Some(CqlValue::Int(p.native_port as i32)),
                            Some(CqlValue::Uuid(p.schema_version)),
                            Some(CqlValue::Text(p.release_version.clone())),
                            Some(CqlValue::Set(tokens_set)),
                        ]
                    } else {
                        vec![
                            Some(CqlValue::Inet(p.peer)),
                            Some(CqlValue::Int(p.peer_port as i32)),
                            Some(CqlValue::Text(p.data_center.clone())),
                            Some(CqlValue::Text(p.rack.clone())),
                            Some(CqlValue::Uuid(p.host_id)),
                            Some(CqlValue::Inet(p.native_address)),
                            Some(CqlValue::Int(p.native_port as i32)),
                            Some(CqlValue::Inet(p.native_address)),
                            Some(CqlValue::Uuid(p.schema_version)),
                            Some(CqlValue::Text(p.release_version.clone())),
                            Some(CqlValue::Set(tokens_set)),
                        ]
                    }
                })
                .collect();
            let (filt_names, filt_types, filt_rows) =
                filter_system_columns(&s.columns, &col_names, &col_types, &rows);
            Ok(result::encode_rows(
                &filt_names,
                &filt_types,
                "system",
                s.table.as_str(),
                &filt_rows,
            ))
        }
        ("system_schema", "keyspaces") => {
            let snap = state.schema.snapshot();
            let ks_rows = query_keyspaces(&snap);
            let all_col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "durable_writes".into(),
                "replication".into(),
            ];
            let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Varchar));
            let all_col_types = vec![CqlType::Varchar, CqlType::Boolean, map_type];
            // Apply WHERE equality filters.
            let filtered: Vec<_> = ks_rows
                .iter()
                .filter(|k| {
                    s.where_clauses.iter().all(|wc| {
                        if wc.op != crate::ast::ComparisonOp::Eq {
                            return true;
                        }
                        let val = match &wc.value {
                            crate::ast::Term::StringLiteral(s) => s.as_str(),
                            _ => return true,
                        };
                        match wc.column.as_str() {
                            "keyspace_name" => k.keyspace_name == val,
                            _ => true,
                        }
                    })
                })
                .collect();
            let all_rows: Vec<Vec<Option<CqlValue>>> = filtered
                .iter()
                .map(|k| {
                    // Build the replication map as CqlValue::Map
                    let repl_map: Vec<(CqlValue, CqlValue)> = k
                        .replication
                        .iter()
                        .map(|(key, val)| {
                            (CqlValue::Text(key.clone()), CqlValue::Text(val.clone()))
                        })
                        .collect();
                    vec![
                        Some(CqlValue::Text(k.keyspace_name.clone())),
                        Some(CqlValue::Boolean(k.durable_writes)),
                        Some(CqlValue::Map(repl_map)),
                    ]
                })
                .collect();
            apply_system_select(
                &s.columns,
                &all_col_names,
                &all_col_types,
                &all_rows,
                "system_schema",
                "keyspaces",
            )
        }
        ("system_schema", "tables") => {
            let snap = state.schema.snapshot();
            let table_rows = query_tables(&snap);
            // Cassandra 5.0 `system_schema.tables` has 20 columns; ferrosa
            // currently advertises four: the three keys the existing schema
            // introspection paths use (`keyspace_name`, `table_name`, `id`)
            // plus `cdc` (boolean, default `false`). The DataStax Java
            // Driver 4.x `TableParser.parseRow` calls
            // `row.getBoolean("cdc")` unconditionally; without the column
            // present, `getBoolean` returns null and unboxing throws NPE
            // during the post-DDL schema refresh.  See
            // ferrosa-nosqlbench/docs/initial-gaps-found.md (Gap 7).
            // Boolean columns the DataStax driver reads unconditionally
            // during schema refresh:
            //   - cdc                 (driver default-ok via ifPresentAndNonNull)
            //   - allow_auto_snapshot (driver requires column present)
            //   - incremental_backups (driver requires column present)
            // Cassandra returns NULL for the latter two on most rows — we do
            // the same so the column metadata exists but values stay null.
            let col_names = vec![
                "keyspace_name".into(),
                "table_name".into(),
                "id".into(),
                "cdc".into(),
                "allow_auto_snapshot".into(),
                "incremental_backups".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Boolean,
                CqlType::Boolean,
                CqlType::Boolean,
            ];
            // Apply WHERE equality filters.
            let filtered: Vec<_> = table_rows
                .iter()
                .filter(|t| {
                    s.where_clauses.iter().all(|wc| {
                        if wc.op != crate::ast::ComparisonOp::Eq {
                            return true;
                        }
                        let val = match &wc.value {
                            crate::ast::Term::StringLiteral(s) => s.as_str(),
                            _ => return true,
                        };
                        match wc.column.as_str() {
                            "keyspace_name" => t.keyspace_name == val,
                            "table_name" => t.table_name == val,
                            _ => true,
                        }
                    })
                })
                .collect();
            let rows: Vec<Vec<Option<CqlValue>>> = filtered
                .iter()
                .map(|t| {
                    vec![
                        Some(CqlValue::Text(t.keyspace_name.clone())),
                        Some(CqlValue::Text(t.table_name.clone())),
                        Some(CqlValue::Uuid(t.id)),
                        Some(CqlValue::Boolean(t.cdc)),
                        // Cassandra returns NULL for these on most tables — match.
                        None,
                        None,
                    ]
                })
                .collect();
            // p1-37: honor the SELECT projection — scylla 0.15 issues
            // `SELECT keyspace_name, table_name FROM system_schema.tables`
            // and type-checks the result against a 2-tuple. Without
            // projection we'd return 3 cols and trip a column-count
            // mismatch.
            let (filt_names, filt_types, filt_rows) =
                filter_system_columns(&s.columns, &col_names, &col_types, &rows);
            Ok(result::encode_rows(
                &filt_names,
                &filt_types,
                "system_schema",
                "tables",
                &filt_rows,
            ))
        }
        ("system_schema", "columns") => {
            let snap = state.schema.snapshot();
            let col_rows = query_columns(&snap);
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "table_name".into(),
                "column_name".into(),
                "kind".into(),
                "position".into(),
                "type".into(),
                "clustering_order".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Int,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            // Apply WHERE equality filters (string columns only).
            let filtered: Vec<_> = col_rows
                .iter()
                .filter(|c| {
                    s.where_clauses.iter().all(|wc| {
                        if wc.op != crate::ast::ComparisonOp::Eq {
                            return true; // skip non-equality ops
                        }
                        let val = match &wc.value {
                            crate::ast::Term::StringLiteral(s) => s.as_str(),
                            _ => return true,
                        };
                        match wc.column.as_str() {
                            "keyspace_name" => c.keyspace_name == val,
                            "table_name" => c.table_name == val,
                            "column_name" => c.column_name == val,
                            "kind" => c.kind == val,
                            "clustering_order" => c.clustering_order == val,
                            _ => true,
                        }
                    })
                })
                .collect();
            let rows: Vec<Vec<Option<CqlValue>>> = filtered
                .iter()
                .map(|c| {
                    vec![
                        Some(CqlValue::Text(c.keyspace_name.clone())),
                        Some(CqlValue::Text(c.table_name.clone())),
                        Some(CqlValue::Text(c.column_name.clone())),
                        Some(CqlValue::Text(c.kind.clone())),
                        Some(CqlValue::Int(c.position)),
                        Some(CqlValue::Text(c.column_type.clone())),
                        Some(CqlValue::Text(c.clustering_order.clone())),
                    ]
                })
                .collect();
            apply_system_select(
                &s.columns,
                &col_names,
                &col_types,
                &rows,
                "system_schema",
                "columns",
            )
        }
        ("system_auth", "roles") => {
            let snap = state.schema.snapshot();

            // Apply the WHERE clause. `role` is the partition key on
            // system_auth.roles, but pre-fix the handler returned every
            // role unconditionally — `WHERE role = 'cassandra'` matched
            // every row. Filter eq predicates here.
            let mut sorted_roles: Vec<&ferrosa_schema::auth::role::RoleMetadata> = snap
                .roles
                .values()
                .filter(|r| {
                    s.where_clauses.iter().all(|wc| {
                        if wc.op != crate::ast::ComparisonOp::Eq {
                            return true;
                        }
                        let val = match &wc.value {
                            crate::ast::Term::StringLiteral(v) => v.as_str(),
                            _ => return true,
                        };
                        match wc.column.as_str() {
                            "role" => r.name == val,
                            _ => true,
                        }
                    })
                })
                .collect();
            sorted_roles.sort_by(|a, b| a.name.cmp(&b.name));

            // Project `salted_hash` as a column. Non-superuser callers
            // see a fixed redaction marker (the column exists but the
            // hash isn't disclosed); superusers see the real hash.
            // Pre-fix the column was omitted entirely, which (a) hid
            // the existence of any hash and (b) made the silent
            // hash-not-stored bug undetectable from a SELECT *.
            let col_names = vec![
                "role".into(),
                "is_superuser".into(),
                "can_login".into(),
                "salted_hash".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Boolean,
                CqlType::Boolean,
                CqlType::Varchar,
            ];
            let is_superuser_caller = ctx.auth.is_superuser;
            let rows: Vec<Vec<Option<CqlValue>>> = sorted_roles
                .iter()
                .map(|r| {
                    let salted_hash_cell = match (&r.salted_hash, is_superuser_caller) {
                        (Some(hash), true) => Some(CqlValue::Text(hash.clone())),
                        (Some(_), false) => Some(CqlValue::Text("[REDACTED]".to_string())),
                        (None, _) => None,
                    };
                    vec![
                        Some(CqlValue::Text(r.name.clone())),
                        Some(CqlValue::Boolean(r.is_superuser)),
                        Some(CqlValue::Boolean(r.can_login)),
                        salted_hash_cell,
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "roles",
                &rows,
            ))
        }
        ("system_auth", "role_members") => {
            let snap = state.schema.snapshot();
            let rows_data = query_role_members(&snap);
            let col_names = vec!["role".into(), "member".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar];
            let rows: Vec<Vec<Option<CqlValue>>> = rows_data
                .iter()
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.role.clone())),
                        Some(CqlValue::Text(r.member.clone())),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "role_members",
                &rows,
            ))
        }
        ("system_auth", "role_permissions") => {
            let snap = state.schema.snapshot();
            let rows_data = query_role_permissions(&snap);
            let col_names = vec!["role".into(), "resource".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar];
            let rows: Vec<Vec<Option<CqlValue>>> = rows_data
                .iter()
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.role.clone())),
                        Some(CqlValue::Text(r.resource.clone())),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "role_permissions",
                &rows,
            ))
        }
        ("system_schema", "types") => {
            // Dogfooding: serve from the persisted `system_schema.types` storage
            // table (not the in-memory Registry or the retired virtual table).
            // The Registry remains the live in-memory cache, rebuilt from these
            // same rows at boot via `replay_types_into_schema`.
            //
            // p1-37: field_names/field_types MUST be `frozen<list<text>>` per
            // Cassandra. scylla 0.15's metadata fetch type-checks these columns
            // and refuses to cache the schema if they're declared as `text` —
            // which then blocks every DDL through the driver.
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "type_name".into(),
                "field_names".into(),
                "field_types".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::List(Box::new(CqlType::Varchar)),
                CqlType::List(Box::new(CqlType::Varchar)),
            ];
            let stored = state
                .engine
                .read_persisted_types()
                .map_err(|e| CqlError::ServerError(format!("read system_schema.types: {e}")))?;
            let rows: Vec<Vec<Option<CqlValue>>> = stored
                .into_iter()
                .map(|udt| {
                    let field_names: Vec<CqlValue> = udt
                        .fields
                        .iter()
                        .map(|(n, _)| CqlValue::Text(n.clone()))
                        .collect();
                    let field_types: Vec<CqlValue> = udt
                        .fields
                        .iter()
                        .map(|(_, t)| CqlValue::Text(bridge::cql_type_display_name(t).to_string()))
                        .collect();
                    vec![
                        Some(CqlValue::Text(udt.keyspace_name.clone())),
                        Some(CqlValue::Text(udt.type_name.clone())),
                        Some(CqlValue::List(field_names)),
                        Some(CqlValue::List(field_types)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "types",
                &rows,
            ))
        }
        ("system_schema", "indexes") => {
            // Dogfooding step 4: serve from the persisted `system_schema.indexes`
            // storage table (not the in-memory Registry or the retired virtual
            // table). The Registry remains the live in-memory cache, rebuilt from
            // these same rows at boot.
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "table_name".into(),
                "index_name".into(),
                "kind".into(),
                "options".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            let stored = state
                .engine
                .read_persisted_indexes()
                .map_err(|e| CqlError::ServerError(format!("read system_schema.indexes: {e}")))?;
            let rows: Vec<Vec<Option<CqlValue>>> = stored
                .into_iter()
                .filter(|r| index_row_matches_where(r, &s.where_clauses))
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.keyspace_name)),
                        Some(CqlValue::Text(r.table_name)),
                        Some(CqlValue::Text(r.index_name)),
                        Some(CqlValue::Text(cassandra_index_kind(&r.kind).to_string())),
                        Some(CqlValue::Text(r.options)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "indexes",
                &rows,
            ))
        }
        // Cassandra 5.0 `system_virtual_schema` keyspace — describes the
        // virtual tables a node exposes.  DataStax Java Driver 4.x queries
        // all three of these during connection initialization; returning
        // ERROR(table not found) leaves the driver's host pool empty and
        // every subsequent query fails with "No node was available".
        // See ferrosa-nosqlbench/docs/initial-gaps-found.md (Gap 2).
        //
        // We currently return well-typed empty result sets — Cassandra's
        // driver tolerates an empty system_virtual_schema and proceeds to
        // schema refresh of user keyspaces.  When ferrosa grows a
        // first-class introspection surface for its virtual tables
        // (system_observability.*, etc.) we populate these rows.
        ("system_virtual_schema", "keyspaces") => {
            let col_names = vec!["keyspace_name".to_string()];
            let col_types = vec![CqlType::Varchar];
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_virtual_schema",
                "keyspaces",
                &[],
            ))
        }
        ("system_virtual_schema", "tables") => {
            let col_names = vec![
                "keyspace_name".to_string(),
                "table_name".to_string(),
                "comment".to_string(),
            ];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar, CqlType::Varchar];
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_virtual_schema",
                "tables",
                &[],
            ))
        }
        ("system_virtual_schema", "columns") => {
            let col_names = vec![
                "keyspace_name".to_string(),
                "table_name".to_string(),
                "column_name".to_string(),
                "clustering_order".to_string(),
                "column_name_bytes".to_string(),
                "kind".to_string(),
                "position".to_string(),
                "type".to_string(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Blob,
                CqlType::Varchar,
                CqlType::Int,
                CqlType::Varchar,
            ];
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_virtual_schema",
                "columns",
                &[],
            ))
        }
        // system_schema.views — empty (ferrosa does not implement
        // materialized views).  Two driver shapes must work:
        //   * DataStax Java 4.x / NoSQLBench `SELECT *` — needs the full
        //     Cassandra-5.0 column set so `ViewParser.getBoolean(...)`
        //     finds `cdc`, `include_all_columns`, `allow_auto_snapshot`,
        //     `incremental_backups` (NPE otherwise — Gap 7).
        //   * scylla 0.15 `SELECT keyspace_name, view_name, base_table_name`
        //     — driver tuple type-check rejects extra columns
        //     ("statement operates on 10 columns, but given rust types
        //     contains 3" — p1-37 regression after Gap 7).
        // Honor SELECT projection like aggregates/triggers does so both
        // drivers see the column count they expect; type the canonical
        // columns by name, fall back to text for unknown names.
        ("system_schema", "views") => {
            let canonical: [(&str, CqlType); 10] = [
                ("keyspace_name", CqlType::Varchar),
                ("view_name", CqlType::Varchar),
                ("base_table_id", CqlType::Uuid),
                ("base_table_name", CqlType::Varchar),
                ("cdc", CqlType::Boolean),
                ("include_all_columns", CqlType::Boolean),
                ("allow_auto_snapshot", CqlType::Boolean),
                ("incremental_backups", CqlType::Boolean),
                ("id", CqlType::Uuid),
                ("where_clause", CqlType::Varchar),
            ];
            let select_all = s.columns.is_empty()
                || s.columns
                    .iter()
                    .any(|c| matches!(c, crate::ast::SelectColumn::Star));
            let (col_names, col_types): (Vec<String>, Vec<CqlType>) = if select_all {
                canonical
                    .iter()
                    .map(|(n, t)| (String::from(*n), t.clone()))
                    .unzip()
            } else {
                s.columns
                    .iter()
                    .filter_map(|c| match c {
                        crate::ast::SelectColumn::Column(name) => Some(name.clone()),
                        _ => None,
                    })
                    .map(|name| {
                        let ty = canonical
                            .iter()
                            .find(|(n, _)| *n == name)
                            .map(|(_, t)| t.clone())
                            .unwrap_or(CqlType::Varchar);
                        (name, ty)
                    })
                    .unzip()
            };
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "views",
                &[],
            ))
        }
        // system_schema.functions — dogfooded: served from the persisted
        // `system_schema.functions` storage table (not the in-memory Registry).
        // The column shape mirrors Cassandra so the Java driver's
        // `FunctionParser` finds the boolean `called_on_null_input` and the
        // `argument_types`/`argument_names` `list<text>` columns it expects (see
        // gap 7 in ferrosa-nosqlbench/docs/initial-gaps-found.md). The Registry
        // remains the live in-memory cache, rebuilt from these same rows at boot
        // via `replay_functions_into_schema`.
        ("system_schema", "functions") => {
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "function_name".into(),
                "argument_types".into(),
                "argument_names".into(),
                "body".into(),
                "called_on_null_input".into(),
                "language".into(),
                "return_type".into(),
            ];
            let list_text = CqlType::List(Box::new(CqlType::Varchar));
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                list_text.clone(),
                list_text,
                CqlType::Varchar,
                CqlType::Boolean,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            let stored = state
                .engine
                .read_persisted_functions()
                .map_err(|e| CqlError::ServerError(format!("read system_schema.functions: {e}")))?;
            let rows: Vec<Vec<Option<CqlValue>>> = stored
                .into_iter()
                .map(|f| {
                    let arg_types: Vec<CqlValue> = f
                        .arg_types
                        .iter()
                        .map(|t| CqlValue::Text(bridge::cql_type_display_name(t).to_string()))
                        .collect();
                    let arg_names: Vec<CqlValue> =
                        f.arg_names.iter().cloned().map(CqlValue::Text).collect();
                    vec![
                        Some(CqlValue::Text(f.keyspace_name)),
                        Some(CqlValue::Text(f.function_name)),
                        Some(CqlValue::List(arg_types)),
                        Some(CqlValue::List(arg_names)),
                        Some(CqlValue::Text(f.body)),
                        Some(CqlValue::Boolean(f.called_on_null)),
                        Some(CqlValue::Text(f.language)),
                        Some(CqlValue::Text(
                            bridge::cql_type_display_name(&f.return_type).to_string(),
                        )),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "functions",
                &rows,
            ))
        }
        // cqlsh queries these system_schema tables during startup introspection.
        // Return empty results for tables we don't populate yet.
        ("system_schema", "aggregates" | "triggers") => {
            // p1-37: Stub these tables until they have first-class
            // implementations. Honor the SELECT projection so col_specs
            // match driver-requested shape — every column is advertised
            // as `text` in the empty result set, which is good enough
            // when there are no rows to type-check.
            let col_names: Vec<String> = if s.columns.is_empty()
                || s.columns
                    .iter()
                    .any(|c| matches!(c, crate::ast::SelectColumn::Star))
            {
                vec!["keyspace_name".into()]
            } else {
                s.columns
                    .iter()
                    .filter_map(|c| match c {
                        crate::ast::SelectColumn::Column(name) => Some(name.clone()),
                        _ => None,
                    })
                    .collect()
            };
            let col_types: Vec<CqlType> = col_names.iter().map(|_| CqlType::Varchar).collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                s.table.as_str(),
                &[],
            ))
        }
        _ => {
            // Virtual table: check registry before storage lookup.
            if let Some(vtable) = state.schema.virtual_tables().get(ks, &s.table) {
                return encode_virtual_rows_streaming(ks, &s.table, vtable.as_ref(), None);
            }

            // User table: permission check + bridge + storage
            let raw = route_select_user_table(state, ctx, ks, &s).await?;
            Ok(raw.encode())
        }
    }
}

fn next_prepared_select_term<'a>(
    template: &'a Term,
    bound_terms: &'a [Term],
    bind_idx: &mut usize,
) -> Option<&'a Term> {
    match template {
        Term::BindMarker(_) => {
            let term = bound_terms.get(*bind_idx)?;
            *bind_idx += 1;
            Some(term)
        }
        other if term_has_bind_marker(other) => None,
        other => Some(other),
    }
}

fn prepared_select_limit(
    limit: &Option<Limit>,
    bound_terms: &[Term],
    bind_idx: &mut usize,
) -> Option<Result<Option<i32>, CqlError>> {
    match limit {
        None => Some(Ok(None)),
        Some(Limit::Literal(n)) => Some(Ok(Some(*n))),
        Some(Limit::BindMarker | Limit::NamedBindMarker(_)) => {
            let term = bound_terms.get(*bind_idx)?;
            *bind_idx += 1;
            match term {
                Term::IntegerLiteral(n) => Some(
                    i32::try_from(*n)
                        .map(Some)
                        .map_err(|_| CqlError::Invalid(format!("LIMIT value {n} out of range"))),
                ),
                other => Some(Err(CqlError::Invalid(format!(
                    "LIMIT bind marker must be an integer, got {other:?}"
                )))),
            }
        }
    }
}

/// Execute the hot NoSQLBench-style prepared SELECT shape without cloning and
/// rewriting the AST: exact full partition key equality, optional literal or
/// top-level bound LIMIT, no secondary filters, no ORDER BY/ANN/aggregation.
pub async fn route_prepared_select_fast(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &SelectStatement,
    bound_terms: &[Term],
) -> Option<Result<RouteResult, CqlError>> {
    if s.distinct
        || s.allow_filtering
        || !s.order_by.is_empty()
        || s.ann_of.is_some()
        || s.where_clauses.is_empty()
    {
        return None;
    }
    if s.columns.iter().any(|col| match col {
        SelectColumn::FunctionCall { .. } => true,
        SelectColumn::Star | SelectColumn::Column(_) => false,
    }) {
        return None;
    }
    if s.where_clauses.iter().any(|wc| {
        wc.op != ComparisonOp::Eq || wc.token_fn || !prepared_insert_term_fast_supported(&wc.value)
    }) {
        return None;
    }

    let ks = match s.keyspace.as_deref().or(ctx.current_keyspace.as_deref()) {
        Some(ks) => ks,
        None => {
            return Some(Err(CqlError::Invalid("no keyspace specified".into())));
        }
    };
    if let Err(e) = validate_keyspace_exists(&state.schema, ks) {
        return Some(Err(e));
    }
    let snap = state.schema.snapshot();
    let table_meta = match snap.tables.get(&(ks.to_string(), s.table.clone())) {
        Some(table_meta) => table_meta,
        None => {
            return Some(Err(CqlError::Invalid(format!(
                "table {}.{} not found",
                ks, s.table
            ))));
        }
    };
    if s.where_clauses.len() != table_meta.partition_key.len() {
        return None;
    }
    if s.where_clauses.iter().any(|wc| {
        table_meta
            .columns
            .get(&wc.column)
            .is_none_or(|col| col.kind != ColumnKind::PartitionKey)
    }) {
        return None;
    }

    let opcode = CqlOpcode::Select;
    let query_desc = format!("SELECT {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table);
    let keyspace = ctx.current_keyspace.as_deref().unwrap_or("");
    let _guard = state.query_tracker.begin_guarded(
        &query_desc,
        keyspace,
        &ctx.client_address,
        &ctx.auth.role,
    );

    let result = async {
        state.schema.check_permission(
            ctx.auth,
            Permission::Select,
            &Resource::Table(ks.to_string(), s.table.clone()),
        )?;

        let mut bind_idx = 0usize;
        let mut pk_values_by_position: Vec<(i32, CqlValue)> =
            Vec::with_capacity(table_meta.partition_key.len());
        for wc in &s.where_clauses {
            let col_meta = table_meta
                .columns
                .get(&wc.column)
                .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", wc.column)))?;
            let term = next_prepared_select_term(&wc.value, bound_terms, &mut bind_idx)
                .ok_or_else(|| {
                    CqlError::Invalid("unsupported prepared SELECT bind shape".into())
                })?;
            let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
            pk_values_by_position.push((
                col_meta.position,
                bridge::term_to_cql_value(term, &cql_type)?,
            ));
        }
        let limit =
            prepared_select_limit(&s.limit, bound_terms, &mut bind_idx).ok_or_else(|| {
                CqlError::Invalid("missing prepared SELECT LIMIT bind value".into())
            })??;

        pk_values_by_position.sort_by_key(|(position, _)| *position);
        for (expected, (position, _)) in pk_values_by_position.iter().enumerate() {
            if *position as usize != expected {
                return Err(CqlError::Invalid(
                    "prepared SELECT fast path requires each partition key once".into(),
                ));
            }
        }
        let pk_values: Vec<CqlValue> = pk_values_by_position
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        let pk_types: Vec<CqlType> = table_meta
            .partition_key
            .iter()
            .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        let table_id = TableId::new(&table_meta.keyspace, &table_meta.name);
        let read_strategy = keyspace_strategy(&state.schema, ks);
        let row_limit = limit.unwrap_or(0).max(0) as usize;

        let partition = state
            .write_path
            .load()
            .pk_read_limited_rows(
                &table_id,
                &decorated_key,
                ctx.consistency,
                &read_strategy,
                row_limit,
            )
            .await?;

        let (col_names, col_types) = build_column_info(table_meta, &s.columns, ks, &state.schema)?;
        if matches!(s.columns.as_slice(), [SelectColumn::Star]) {
            let body =
                result::encode_rows_raw_with_writer(&col_names, &col_types, ks, &s.table, |emit| {
                    if let Some(partition) = partition.as_ref() {
                        let pk_indices: Vec<usize> = table_meta
                            .partition_key
                            .iter()
                            .filter_map(|name| table_meta.columns.get_index_of(name))
                            .collect();
                        let ck_indices: Vec<usize> = table_meta
                            .clustering_key
                            .iter()
                            .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
                            .collect();
                        let storage_to_table = storage_to_table_indices(table_meta);
                        bridge::write_partition_raw_rows_with_storage_mapping(
                            partition,
                            col_names.len(),
                            &pk_indices,
                            &ck_indices,
                            &storage_to_table,
                            emit,
                        );
                    }
                });
            return Ok(RouteResult::Result(body));
        }

        let rows = if let Some(partition) = partition {
            let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
            let all_col_types: Vec<CqlType> = table_meta
                .columns
                .values()
                .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
                .collect::<Result<Vec<_>, _>>()?;
            let pk_indices: Vec<usize> = table_meta
                .partition_key
                .iter()
                .filter_map(|name| table_meta.columns.get_index_of(name))
                .collect();
            let ck_indices: Vec<usize> = table_meta
                .clustering_key
                .iter()
                .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
                .collect();
            let storage_to_table = storage_to_table_indices(table_meta);
            let rows = bridge::partition_to_rows_with_storage_mapping(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
                &storage_to_table,
            );
            let selected = select_columns(&rows, &all_col_names, &col_names);
            if let Some(limit) = limit {
                selected.into_iter().take(limit.max(0) as usize).collect()
            } else {
                selected
            }
        } else {
            Vec::new()
        };

        Ok(RouteResult::Result(
            SelectRawResult {
                column_names: col_names,
                column_types: col_types,
                rows,
                keyspace: ks.to_string(),
                table: s.table.clone(),
                paging_state: None,
            }
            .encode(),
        ))
    }
    .await;

    state.cql_metrics.inc_request(opcode);
    if result.is_err() {
        state.cql_metrics.inc_error();
    }
    Some(result)
}

/// Column/row metadata threaded into the geospatial select path.
struct GeoRowContext<'a> {
    col_names: &'a [String],
    col_types: &'a [CqlType],
    all_col_names: &'a [String],
    all_col_types: &'a [CqlType],
    pk_indices: &'a [usize],
    ck_indices: &'a [usize],
    storage_to_table: &'a [usize],
}

/// Resolve the geo index name for `column` from the schema snapshot, matching by
/// keyspace, table, column, and `IndexType::Geo`.
fn resolve_geo_index_name(
    snap: &ferrosa_schema::SchemaSnapshot,
    ks: &str,
    table: &str,
    column: &str,
) -> Option<String> {
    snap.indexes
        .iter()
        .find(|((idx_ks, idx_tbl, _), meta)| {
            idx_ks == ks
                && idx_tbl == table
                && meta.index_type == IndexType::Geo
                && meta.target_columns.iter().any(|c| c == column)
        })
        .map(|(_, meta)| meta.name.clone())
}

/// Extract a `(lat, lon)` point from a row's geo column, which is stored as a
/// `CqlValue::Tuple([Double(lat), Double(lon)])`. Returns `None` if the column
/// is null or not a well-formed two-element double tuple — such a row cannot be
/// refined and is dropped rather than mis-located.
fn row_geo_point(row: &[Option<CqlValue>], col_idx: usize) -> Option<(f64, f64)> {
    match row.get(col_idx) {
        Some(Some(CqlValue::Tuple(elems))) if elems.len() == 2 => {
            let lat = match elems[0] {
                Some(CqlValue::Double(bits)) => f64::from_bits(bits),
                _ => return None,
            };
            let lon = match elems[1] {
                Some(CqlValue::Double(bits)) => f64::from_bits(bits),
                _ => return None,
            };
            Some((lat, lon))
        }
        _ => None,
    }
}

/// Convert geo cover ranges into the `(u64, u64)` pairs the storage layer wants.
fn cover_ranges_to_pairs(ranges: &[ferrosa_index::geo::CellRange]) -> Vec<(u64, u64)> {
    ranges.iter().map(|r| (r.start, r.end)).collect()
}

/// Encode a full row to a stable byte key for cross-ring deduplication during
/// k-NN. Each cell is length-prefixed (`-1` for null) so distinct rows produce
/// distinct keys. The projected row includes the primary key, so two different
/// base rows can never collide.
fn encode_row_identity(row: &[Option<CqlValue>]) -> Vec<u8> {
    let mut key = Vec::new();
    for cell in row {
        match cell {
            Some(v) => {
                let bytes = encode_value(v);
                key.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                key.extend_from_slice(&bytes);
            }
            None => key.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    key
}

/// Execute a geospatial SELECT: `GEO_NEAREST OF`, `GEO_WITHIN_RADIUS`, or
/// `GEO_WITHIN_BBOX`. Resolves the geo index, fetches candidate partitions by
/// covering cell ranges, refines with exact distance / containment, then
/// orders / bounds / projects the result. Records `index_usage` and is reported
/// by EXPLAIN via [`ScanPlan::GeoIndex`].
async fn route_geo_select(
    state: &SharedState,
    ks: &str,
    s: &SelectStatement,
    snap: &ferrosa_schema::SchemaSnapshot,
    table_meta: &ferrosa_schema::metadata::table::TableMetadata,
    table_id: &TableId,
    rowctx: GeoRowContext<'_>,
) -> Result<SelectRawResult, CqlError> {
    use ferrosa_index::geo;

    // Exactly one geo operation per query (parser guarantees GEO_NEAREST is the
    // sole ordering; we take the first geo predicate when present).
    let (geo_column, plan_kind) = if let Some(gn) = &s.geo_nearest {
        (gn.column.clone(), "GeoNearest")
    } else {
        let pred = &s.geo_predicates[0];
        let kind = match pred {
            GeoPredicate::WithinRadius { .. } => "GeoWithinRadius",
            GeoPredicate::WithinBbox { .. } => "GeoWithinBbox",
            GeoPredicate::WithinPolygon { .. } => "GeoWithinPolygon",
        };
        (pred.column().to_string(), kind)
    };

    let index_name = resolve_geo_index_name(snap, ks, &s.table, &geo_column).ok_or_else(|| {
        CqlError::Invalid(format!(
            "no geo index on {}.{}({geo_column}); create one with CREATE INDEX ... USING 'geo'",
            ks, s.table
        ))
    })?;

    // Observability: a geo index was consulted.
    state
        .index_usage_tracker
        .record(ks, &s.table, &index_name, plan_kind);

    // Index of the geo column within the full (storage-order) row, for refining.
    let geo_col_idx = rowctx
        .all_col_names
        .iter()
        .position(|n| n == &geo_column)
        .ok_or_else(|| CqlError::Invalid(format!("geo column {geo_column} not found")))?;

    // Fetch candidate partitions for a set of covering cell ranges, convert to
    // rows. Bounded by the storage layer's INDEX_RESULT_CAP (fail-loud).
    let fetch_rows = |ranges: Vec<(u64, u64)>| -> Result<Vec<Vec<Option<CqlValue>>>, CqlError> {
        let partitions = state
            .engine
            .read_by_index_cell_ranges(table_id, &index_name, &ranges)
            .map_err(|e| CqlError::Invalid(format!("geo index read failed: {e}")))?;
        let mut rows = Vec::new();
        for partition in &partitions {
            let mut prows = bridge::partition_to_rows_with_storage_mapping(
                partition,
                rowctx.all_col_names,
                rowctx.all_col_types,
                rowctx.pk_indices,
                rowctx.ck_indices,
                rowctx.storage_to_table,
            );
            rows.append(&mut prows);
        }
        Ok(rows)
    };

    // Build the final, refined row set for the specific geo operation.
    let mut result_rows: Vec<Vec<Option<CqlValue>>> = if let Some(gn) = &s.geo_nearest {
        // k-NN: expanding-ring search. The ring `fetch` returns the candidate
        // points keyed by an opaque ordinal into a side table of rows so we can
        // reassemble the chosen rows after ranking.
        let k = s
            .limit
            .as_ref()
            .and_then(|l| l.as_literal())
            .map(|n| n.max(0) as usize)
            .unwrap_or(usize::MAX);

        // Collect candidate rows across rings, then rank by haversine and take k.
        // The same partition is re-fetched as the search rings expand, so each
        // row must get a STABLE id across rings or `nearest_k`'s id-based dedup
        // cannot drop the repeats. We key each distinct row to a stable index in
        // `seen_rows` by its full encoded contents (the projected row includes
        // the primary key, so distinct rows never collide).
        let mut seen_rows: Vec<Vec<Option<CqlValue>>> = Vec::new();
        let mut row_ids: HashMap<Vec<u8>, usize> = HashMap::new();
        let candidates = geo::nearest_k(gn.lat, gn.lon, k, |ranges| {
            let pairs = cover_ranges_to_pairs(ranges);
            let rows = match fetch_rows(pairs) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut out = Vec::new();
            for row in rows {
                if let Some((lat, lon)) = row_geo_point(&row, geo_col_idx) {
                    let row_key = encode_row_identity(&row);
                    let id = *row_ids.entry(row_key).or_insert_with(|| {
                        let next = seen_rows.len();
                        seen_rows.push(row.clone());
                        next
                    });
                    out.push(geo::GeoCandidate { id, lat, lon });
                }
            }
            out
        });
        candidates
            .into_iter()
            .map(|c| seen_rows[c.id].clone())
            .collect()
    } else {
        match &s.geo_predicates[0] {
            GeoPredicate::WithinRadius {
                lat, lon, radius_m, ..
            } => {
                let ranges = cover_ranges_to_pairs(&geo::cover_radius(
                    *lat,
                    *lon,
                    *radius_m,
                    geo::DEFAULT_COVER_LEVEL,
                ));
                let rows = fetch_rows(ranges)?;
                // Refine: keep rows whose exact haversine distance is within the
                // radius (the cover is an over-approximation).
                rows.into_iter()
                    .filter(|row| {
                        row_geo_point(row, geo_col_idx)
                            .map(|(la, lo)| geo::within_radius(*lat, *lon, *radius_m, la, lo))
                            .unwrap_or(false)
                    })
                    .collect()
            }
            GeoPredicate::WithinBbox { sw, ne, .. } => {
                let ranges =
                    cover_ranges_to_pairs(&geo::cover_bbox(*sw, *ne, geo::DEFAULT_COVER_LEVEL));
                let rows = fetch_rows(ranges)?;
                // Refine: exact axis-aligned (antimeridian-aware) containment.
                rows.into_iter()
                    .filter(|row| {
                        row_geo_point(row, geo_col_idx)
                            .map(|(la, lo)| geo::within_bbox(sw.0, sw.1, ne.0, ne.1, la, lo))
                            .unwrap_or(false)
                    })
                    .collect()
            }
            GeoPredicate::WithinPolygon { vertices, .. } => {
                let polygon = geo::Polygon::new(vertices.clone());
                // A dateline-straddling polygon is not handled by planar ray
                // casting; reject it loudly rather than return wrong rows.
                if polygon.crosses_antimeridian() {
                    return Err(CqlError::Invalid(
                        "ST_WITHIN does not support polygons crossing the ±180° antimeridian"
                            .to_string(),
                    ));
                }
                // Cover the polygon's bounding box, then refine each candidate.
                // The cell cover is coarse, so the fetched rows over-approximate
                // the polygon's bbox. We bulk-load the candidate points into an
                // R-tree and query it with the polygon's exact bbox: that prunes
                // off-bbox candidates in O(log n) before running the expensive
                // point-in-polygon ray-cast only on the survivors. A degenerate
                // polygon has no bbox and matches nothing.
                match geo::polygon_bbox(&polygon) {
                    Some((sw, ne)) => {
                        let ranges = cover_ranges_to_pairs(&geo::cover_bbox(
                            sw,
                            ne,
                            geo::DEFAULT_COVER_LEVEL,
                        ));
                        let rows = fetch_rows(ranges)?;
                        // Each candidate carries its row index as the opaque id so
                        // the R-tree survivors map straight back to source rows.
                        let candidates: Vec<geo::GeoPoint<usize>> =
                            rows.iter()
                                .enumerate()
                                .filter_map(|(i, row)| {
                                    row_geo_point(row, geo_col_idx)
                                        .map(|(lat, lon)| geo::GeoPoint { id: i, lat, lon })
                                })
                                .collect();
                        let mut kept = geo::points_in_polygon_rtree(&candidates, &polygon);
                        // Stable, deterministic output order (R-tree traversal is
                        // not input order).
                        kept.sort_unstable();
                        kept.into_iter().map(|i| rows[i].clone()).collect()
                    }
                    None => Vec::new(),
                }
            }
        }
    };

    // Apply any remaining scalar WHERE predicates as a post-filter (the geo
    // predicate is handled above and not present in `where_clauses`).
    filter_rows_by_select_predicates(
        &mut result_rows,
        s,
        rowctx.all_col_names,
        rowctx.all_col_types,
        table_meta,
        ks,
        state,
    )?;

    // LIMIT for the non-nearest forms (k-NN already applied k above).
    if s.geo_nearest.is_none() {
        if let Some(limit) = s.limit.as_ref().and_then(|l| l.as_literal()) {
            result_rows.truncate(limit.max(0) as usize);
        }
    }

    // Project to the selected columns.
    let selected_rows = select_columns(&result_rows, rowctx.all_col_names, rowctx.col_names);

    Ok(SelectRawResult {
        column_names: rowctx.col_names.to_vec(),
        column_types: rowctx.col_types.to_vec(),
        rows: selected_rows,
        keyspace: ks.to_string(),
        table: s.table.clone(),
        paging_state: None,
    })
}

async fn route_select_user_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    ks: &str,
    s: &SelectStatement,
) -> Result<SelectRawResult, CqlError> {
    validate_keyspace_exists(&state.schema, ks)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Select,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;
    let table_strategy = keyspace_strategy(&state.schema, ks);

    // ALLOW FILTERING: permit full-table scans with post-filter when the
    // client explicitly opts in.  Log a warning so operators can identify
    // expensive queries, but do not reject.
    if s.allow_filtering && !where_has_udf_calls(&s.where_clauses) {
        tracing::warn!(
            keyspace = ks,
            table = %s.table,
            "executing query with ALLOW FILTERING — full table scan with post-filter"
        );
    }

    // If WHERE contains UDF calls but ALLOW FILTERING is not set, reject.
    if where_has_udf_calls(&s.where_clauses) && !s.allow_filtering {
        return Err(CqlError::Invalid(
            "queries with UDFs in WHERE predicates require ALLOW FILTERING".into(),
        ));
    }
    if where_has_udf_calls(&s.where_clauses) {
        tracing::warn!(
            keyspace = ks,
            table = %s.table,
            "executing query with UDF in WHERE — full scan with ALLOW FILTERING"
        );
    }

    // Build column info for result
    let (col_names, col_types) = build_column_info(table_meta, &s.columns, ks, &state.schema)?;

    let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
    let all_col_types: Vec<CqlType> = table_meta
        .columns
        .values()
        .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let pk_indices: Vec<usize> = table_meta
        .partition_key
        .iter()
        .map(|name| {
            table_meta.columns.get_index_of(name).ok_or_else(|| {
                CqlError::Invalid(format!("partition key column '{}' not found", name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ck_indices: Vec<usize> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| {
            table_meta.columns.get_index_of(name).ok_or_else(|| {
                CqlError::Invalid(format!("clustering key column '{}' not found", name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let storage_to_table = storage_to_table_indices(table_meta);
    let table_id = TableId::new(&table_meta.keyspace, &table_meta.name);

    // ── fts_match(): full-text index search ───────────────────────────────────
    //
    // Handles `WHERE col = fts_match('query')`.  We detect the function, run
    // the FTI search to obtain matching partition keys, fetch each partition,
    // and apply remaining WHERE predicates as a post-filter.
    if where_has_fts_match(&s.where_clauses) {
        let (fts_column, fts_query) = extract_fts_match(&s.where_clauses)
            .ok_or_else(|| CqlError::Invalid("fts_match: failed to extract column/query".into()))?;

        // Look up the full-text index name for the referenced column. This is
        // the same resolution EXPLAIN uses (`resolve_fulltext_index_name`), so
        // the reported `FullTextIndex` plan and the executed index agree. Falls
        // back to the column name for simple single-column FTI registration.
        let fti_index_name = resolve_fulltext_index_name(&snap, ks, &s.table, fts_column);
        let index_name = fti_index_name.as_str();

        // Observability: record that the full-text index was consulted.
        state
            .index_usage_tracker
            .record(ks, &s.table, index_name, "FullText");

        let matching_pks = state
            .engine
            .fulltext_search(&table_id, index_name, fts_query)
            .map_err(|e| CqlError::Invalid(format!("fts_match search failed: {e}")))?;

        // Fetch each matching partition and apply post-filter.
        // The raw_pk bytes are the PartitionKey bytes as stored in the FTI.
        // Reconstruct a DecoratedKey by wrapping them in PartitionKey directly.
        let mut fts_rows = Vec::new();
        for raw_pk in matching_pks {
            let decorated =
                ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(raw_pk));
            if let Some(partition) = state
                .write_path
                .load()
                .read(&table_id, &decorated)
                .await
                .map_err(|e| CqlError::ServerError(format!("{e}")))?
            {
                let mut prows = bridge::partition_to_rows_with_storage_mapping(
                    &partition,
                    &all_col_names,
                    &all_col_types,
                    &pk_indices,
                    &ck_indices,
                    &storage_to_table,
                );
                // Post-filter: apply remaining (non-fts_match) WHERE predicates.
                filter_rows_by_select_predicates(
                    &mut prows,
                    s,
                    &all_col_names,
                    &all_col_types,
                    table_meta,
                    ks,
                    state,
                )?;
                fts_rows.append(&mut prows);
            }
        }

        // Apply LIMIT if specified.
        let fts_rows: Vec<Vec<Option<CqlValue>>> =
            if let Some(limit) = s.limit.as_ref().and_then(|l| l.as_literal()) {
                fts_rows.into_iter().take(limit as usize).collect()
            } else {
                fts_rows
            };
        // Project to selected columns.
        let selected_rows = select_columns(&fts_rows, &all_col_names, &col_names);
        return Ok(SelectRawResult {
            column_names: col_names.to_vec(),
            column_types: col_types.to_vec(),
            rows: selected_rows,
            keyspace: ks.to_string(),
            table: s.table.clone(),
            paging_state: None,
        });
    }

    // ── Geospatial query surface (GEO_NEAREST / GEO_WITHIN_RADIUS / BBOX) ──────
    //
    // A geo predicate is a function over a geo-indexed column, not a scalar
    // comparison, so it is handled in its own index-backed branch: resolve the
    // geo index, ask it for covering cell ranges, fetch candidates, refine with
    // exact distance / containment, then sort/limit and project. EXPLAIN reports
    // the geo index and `index_usage` increments on the hit.
    if s.geo_nearest.is_some() || !s.geo_predicates.is_empty() {
        return route_geo_select(
            state,
            ks,
            s,
            &snap,
            table_meta,
            &table_id,
            GeoRowContext {
                col_names: &col_names,
                col_types: &col_types,
                all_col_names: &all_col_names,
                all_col_types: &all_col_types,
                pk_indices: &pk_indices,
                ck_indices: &ck_indices,
                storage_to_table: &storage_to_table,
            },
        )
        .await;
    }

    // Try PK-based lookup first; fall back to full scan with ALLOW FILTERING
    let pk_result = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    );

    let read_strategy = keyspace_strategy(&state.schema, ks);
    let count_only_select = is_count_only_select(&s.columns);

    // ADR-020 COUNT(*) fast path. `SELECT COUNT(*) FROM t` with no
    // WHERE clauses, no LIMIT, no GROUP BY, and no other projected
    // columns goes through `WritePath::count_range` which uses the
    // metadata-only k-way merger in ferrosa-storage. Cell payloads
    // are byte-skipped at every SSTable — typical 5-10× speedup
    // over the legacy `range_read → Vec → count_rows` path.
    //
    // We bail out to the legacy path for any WHERE clauses
    // (predicates may filter partitions), ORDER BY (changes the
    // visible row order), or LIMIT (caller wants bounded output).
    let no_where = s.where_clauses.is_empty();
    let no_order_by = s.order_by.is_empty();
    let no_limit = s.limit.is_none();
    if count_only_select && no_where && no_order_by && no_limit && pk_result.is_err() {
        let count = state.write_path.load().count_range(&table_id).await?;
        return Ok(SelectRawResult {
            column_names: col_names.to_vec(),
            column_types: col_types.to_vec(),
            rows: vec![vec![Some(CqlValue::Bigint(count as i64))]],
            keyspace: ks.to_string(),
            table: s.table.clone(),
            paging_state: None,
        });
    }

    // Set by the streaming full-scan page path when it has already applied
    // page bounds + the continuation cursor. `Some(state)` (which may itself be
    // `None` for the final page) means the generic `apply_pagination` tail must
    // be skipped — the rows are already exactly one bounded page.
    let mut streamed_paging_state: Option<Option<Vec<u8>>> = None;

    let rows = if let Ok(pk_values) = pk_result {
        // PK present — single partition lookup
        let pk_types: Vec<CqlType> = table_meta
            .partition_key
            .iter()
            .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        let row_limit =
            safe_partition_key_filter_row_limit(s, table_meta, count_only_select).unwrap_or(0);
        let exact_clustering = if clustering_key_equality_has_phonetic_index(
            &s.where_clauses,
            table_meta,
            &state.schema,
        ) {
            None
        } else {
            extract_clustering_key_values(&s.where_clauses, table_meta, ks, &state.schema)?
                .map(|values| bridge::build_clustering_key(&values))
        };
        let partition = if let Some(clustering) = exact_clustering {
            state
                .write_path
                .load()
                .pk_read_clustering_row(
                    &table_id,
                    &decorated_key,
                    &clustering,
                    ctx.consistency,
                    &read_strategy,
                )
                .await?
        } else {
            state
                .write_path
                .load()
                .pk_read_limited_rows(
                    &table_id,
                    &decorated_key,
                    ctx.consistency,
                    &read_strategy,
                    row_limit,
                )
                .await?
        };
        let mut pk_rows = match partition {
            Some(partition) => bridge::partition_to_rows_with_storage_mapping(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
                &storage_to_table,
            ),
            None => vec![],
        };
        // Apply clustering key and other non-PK WHERE predicates.
        filter_rows_by_select_predicates(
            &mut pk_rows,
            s,
            &all_col_names,
            &all_col_types,
            table_meta,
            ks,
            state,
        )?;
        pk_rows
    } else if let Some(in_rows) = try_pk_in_lookup(
        &s.where_clauses,
        table_meta,
        ks,
        &state.schema,
        &state.engine,
        &table_id,
        &all_col_names,
        &all_col_types,
        &pk_indices,
        &ck_indices,
        &storage_to_table,
    )? {
        // PK IN (...) — multi-partition lookup.
        // Apply clustering key and other non-PK WHERE predicates.
        let mut filtered = in_rows;
        filter_rows_by_select_predicates(
            &mut filtered,
            s,
            &all_col_names,
            &all_col_types,
            table_meta,
            ks,
            state,
        )?;
        filtered
    } else {
        // No PK — use the query planner to decide the access path.
        // A partial (Filtered) index is offered to the planner ONLY when the
        // query implies its predicate (see `query_implies_filter_predicate`);
        // otherwise it is withheld so the planner cannot unsoundly serve an
        // incomplete result from it.
        let usable_indexes: Vec<&IndexMetadata> = snap
            .indexes
            .iter()
            .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
            .map(|(_, meta)| meta)
            .filter(|meta| {
                filtered_index_is_usable(meta, &s.where_clauses, table_meta, ks, &state.schema)
            })
            .collect();
        let planner_indexes: Vec<(String, Vec<String>)> = usable_indexes
            .iter()
            .map(|meta| (meta.name.clone(), meta.target_columns.clone()))
            .collect();
        // Filter columns of usable partial indexes are covered by the index
        // itself, so a WHERE predicate on them must not require ALLOW FILTERING.
        let filtered_covered_columns = filtered_index_covered_columns(&usable_indexes, table_meta);

        let scan_plan = planner::plan_with_covered(
            &s.where_clauses,
            &table_meta.partition_key,
            &planner_indexes,
            &filtered_covered_columns,
        );

        // ── Vector ANN index consult ──────────────────────────────────────
        //
        // `ORDER BY col ANN OF [...] LIMIT k` with a vector index registered on
        // `col`: consult the index for the k nearest partitions instead of
        // full-scanning the table and post-filtering. The recovered rows feed
        // the SAME downstream pipeline below (apply_ann_of_ordering re-ranks the
        // k rows, then LIMIT + projection). When NO vector index targets the
        // column, `ann_index` is `None` and we fall through UNCHANGED to
        // `match scan_plan` (byte-identical fallback).
        let ann_index = s.ann_of.as_ref().and_then(|(ann_col, _)| {
            vector_index_for_ann_column(&snap, ks, &s.table, ann_col).map(|name| (name, ann_col))
        });
        if let Some((index_name, ann_col)) = ann_index {
            let ann_query = &s
                .ann_of
                .as_ref()
                .expect("ann_of present when ann_index resolved")
                .1;
            let col_idx = all_col_names
                .iter()
                .position(|name| name == ann_col)
                .ok_or_else(|| CqlError::Invalid(format!("unknown ANN OF column {ann_col}")))?;
            let target_type = all_col_types.get(col_idx).ok_or_else(|| {
                CqlError::Invalid(format!("missing type for ANN OF column {ann_col}"))
            })?;
            let query_bits = vector_bits_from_term(ann_query, target_type)?;
            let query: Vec<f32> = query_bits.iter().map(|b| f32::from_bits(*b)).collect();
            let k = s
                .limit
                .as_ref()
                .and_then(|l| l.as_literal())
                .map(|n| (n.max(0) as usize).min(ANN_DEFAULT_K))
                .filter(|n| *n > 0)
                .unwrap_or(ANN_DEFAULT_K);

            let partitions = state
                .engine
                .ann_search_partitions(&table_id, &index_name, &query, k, ANN_EF_SEARCH)
                .map_err(|e| CqlError::ServerError(format!("ANN index search failed: {e}")))?;

            // Observability parity with the other index types (Phase 3): an ANN
            // index consult is an index hit, not a full scan.
            state
                .index_usage_tracker
                .record(ks, &s.table, &index_name, "VectorAnn");

            tracing::debug!(
                keyspace = ks,
                table = %s.table,
                index = %index_name,
                k,
                returned = partitions.len(),
                "ANN OF served from vector index consult (no full scan)"
            );

            let mut ann_rows = Vec::new();
            extend_rows_from_partitions(
                &partitions,
                &mut ann_rows,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
                &storage_to_table,
            )
            .await;
            ann_rows
        } else {
            match scan_plan {
                ScanPlan::VectorAnn { .. } => {
                    // `planner::plan` never emits VectorAnn — the ANN index consult
                    // is handled by the early branch above. Reaching here means the
                    // planner contract changed without updating this dispatch; fail
                    // loud rather than silently degrade to a full scan.
                    return Err(CqlError::ServerError(
                        "internal: VectorAnn scan plan reached the scan dispatch; \
                     ANN index consult should have handled it"
                            .into(),
                    ));
                }
                ScanPlan::GeoIndex { .. } => {
                    // Geo queries are served by `route_geo_select` in an early
                    // branch before the planner runs, so `planner::plan` never
                    // returns this variant here. It exists only for EXPLAIN's
                    // plan rendering.
                    return Err(CqlError::ServerError(
                        "internal: geo plan reached the scalar scan dispatch".into(),
                    ));
                }
                ScanPlan::FullTextIndex { .. } => {
                    // Full-text queries (`WHERE col = fts_match(...)`) are served
                    // by the `where_has_fts_match` early branch above before the
                    // planner runs, so `planner::plan` never returns this variant
                    // here. It exists only for EXPLAIN's plan rendering.
                    return Err(CqlError::ServerError(
                        "internal: full-text plan reached the scalar scan dispatch".into(),
                    ));
                }
                ScanPlan::PartitionKeyLookup => {
                    // This can happen when extract_pk_values fails (e.g., bind
                    // values that can't be coerced to the PK column type) but
                    // the planner still sees Eq predicates on all PK columns.
                    // Fall through to a full scan rather than panicking.
                    let partitions = state
                        .write_path
                        .load()
                        .range_read_with(&table_id, ctx.consistency, &table_strategy)
                        .await?;
                    if count_only_select {
                        let count = count_rows_from_partitions(
                            &partitions,
                            PartitionRowContext {
                                all_col_names: &all_col_names,
                                all_col_types: &all_col_types,
                                pk_indices: &pk_indices,
                                ck_indices: &ck_indices,
                                storage_to_table: &storage_to_table,
                            },
                            SelectPredicateContext {
                                statement: s,
                                table_meta,
                                keyspace: ks,
                                state,
                            },
                        )
                        .await?;
                        return Ok(SelectRawResult {
                            column_names: col_names.to_vec(),
                            column_types: col_types.to_vec(),
                            rows: vec![vec![Some(CqlValue::Bigint(count))]],
                            keyspace: ks.to_string(),
                            table: s.table.clone(),
                            paging_state: None,
                        });
                    }
                    let mut all_rows = Vec::new();
                    extend_rows_from_partitions(
                        &partitions,
                        &mut all_rows,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                        &storage_to_table,
                    )
                    .await;
                    filter_rows_by_select_predicates(
                        &mut all_rows,
                        s,
                        &all_col_names,
                        &all_col_types,
                        table_meta,
                        ks,
                        state,
                    )?;
                    all_rows
                }

                ScanPlan::SingleIndex {
                    ref index_name,
                    ref index_column,
                }
                | ScanPlan::IndexScanWithFilter {
                    ref index_name,
                    ref index_column,
                    ..
                } => {
                    // IndexScanWithFilter means some WHERE columns are not covered
                    // by any index — require ALLOW FILTERING for these queries.
                    if matches!(scan_plan, ScanPlan::IndexScanWithFilter { .. })
                        && !s.allow_filtering
                    {
                        return Err(CqlError::Invalid(
                            "Cannot execute this query as it requires filtering on non-indexed \
                         columns. Use ALLOW FILTERING, create a secondary index on the \
                         filtered columns, or restructure your query to use partition keys."
                                .into(),
                        ));
                    }

                    // Observability: record that a secondary index was consulted.
                    // A partial (Filtered) index records the "Filtered" kind so
                    // its acceleration is distinguishable in index_usage; the
                    // planner only reached here for it when the query implied its
                    // predicate (see `filtered_index_is_usable`).
                    let is_filtered = snap
                        .indexes
                        .get(&(ks.to_string(), s.table.clone(), index_name.clone()))
                        .map(|m| m.index_type == IndexType::Filtered)
                        .unwrap_or(false);
                    let plan_kind = if is_filtered {
                        "Filtered"
                    } else if matches!(scan_plan, ScanPlan::IndexScanWithFilter { .. }) {
                        "IndexScanWithFilter"
                    } else {
                        "SingleIndex"
                    };
                    state
                        .index_usage_tracker
                        .record(ks, &s.table, index_name, plan_kind);

                    // Find the WHERE clause for the indexed column.
                    let index_wc = s
                        .where_clauses
                        .iter()
                        .find(|wc| wc.column == *index_column && wc.op == ComparisonOp::Eq)
                        .ok_or_else(|| {
                            CqlError::Invalid(
                                "planner selected index but no matching WHERE clause found".into(),
                            )
                        })?;

                    let index_key = term_to_index_key(
                        &index_wc.value,
                        index_column,
                        table_meta,
                        ks,
                        &state.schema,
                    )?;

                    // Scatter-gather index read: in cluster mode this fans out
                    // to all ring nodes so results include rows on every node.
                    let partitions = state
                        .write_path
                        .load()
                        .index_read(&table_id, index_name, &index_key)
                        .await?;

                    // Fallback: if the index read returns empty, the memtable index
                    // may not be wired yet (Sprint I-3). Fall back to full scan so
                    // queries still return correct results.
                    let partitions = if partitions.is_empty() {
                        state
                            .write_path
                            .load()
                            .range_read_with(&table_id, ctx.consistency, &table_strategy)
                            .await?
                    } else {
                        partitions
                    };

                    if count_only_select {
                        let count = count_rows_from_partitions(
                            &partitions,
                            PartitionRowContext {
                                all_col_names: &all_col_names,
                                all_col_types: &all_col_types,
                                pk_indices: &pk_indices,
                                ck_indices: &ck_indices,
                                storage_to_table: &storage_to_table,
                            },
                            SelectPredicateContext {
                                statement: s,
                                table_meta,
                                keyspace: ks,
                                state,
                            },
                        )
                        .await?;
                        return Ok(SelectRawResult {
                            column_names: col_names.to_vec(),
                            column_types: col_types.to_vec(),
                            rows: vec![vec![Some(CqlValue::Bigint(count))]],
                            keyspace: ks.to_string(),
                            table: s.table.clone(),
                            paging_state: None,
                        });
                    }

                    let mut all_rows = Vec::new();
                    extend_rows_from_partitions(
                        &partitions,
                        &mut all_rows,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                        &storage_to_table,
                    )
                    .await;

                    // Always apply post-filter as defensive measure.
                    // SingleIndex: redundant but safe; IndexScanWithFilter: necessary.
                    filter_rows_by_select_predicates(
                        &mut all_rows,
                        s,
                        &all_col_names,
                        &all_col_types,
                        table_meta,
                        ks,
                        state,
                    )?;

                    all_rows
                }

                ScanPlan::IndexIntersection { ref indexes } => {
                    // Observability: record that each intersected index was consulted.
                    for (index_name, _index_column) in indexes {
                        state.index_usage_tracker.record(
                            ks,
                            &s.table,
                            index_name,
                            "IndexIntersection",
                        );
                    }
                    // Consult ALL matched single-column indexes and intersect their
                    // result sets on partition-key identity, so we fetch only the
                    // partitions present in every index rather than the full result
                    // of indexes[0] alone. The post-filter below still enforces
                    // per-row predicate precision on clustered tables.
                    let partitions = read_index_intersection(
                        state,
                        &table_id,
                        indexes,
                        s,
                        table_meta,
                        ks,
                        ctx.consistency,
                        &table_strategy,
                    )
                    .await?;

                    if count_only_select {
                        let count = count_rows_from_partitions(
                            &partitions,
                            PartitionRowContext {
                                all_col_names: &all_col_names,
                                all_col_types: &all_col_types,
                                pk_indices: &pk_indices,
                                ck_indices: &ck_indices,
                                storage_to_table: &storage_to_table,
                            },
                            SelectPredicateContext {
                                statement: s,
                                table_meta,
                                keyspace: ks,
                                state,
                            },
                        )
                        .await?;
                        return Ok(SelectRawResult {
                            column_names: col_names.to_vec(),
                            column_types: col_types.to_vec(),
                            rows: vec![vec![Some(CqlValue::Bigint(count))]],
                            keyspace: ks.to_string(),
                            table: s.table.clone(),
                            paging_state: None,
                        });
                    }

                    let mut all_rows = Vec::new();
                    extend_rows_from_partitions(
                        &partitions,
                        &mut all_rows,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                        &storage_to_table,
                    )
                    .await;
                    filter_rows_by_select_predicates(
                        &mut all_rows,
                        s,
                        &all_col_names,
                        &all_col_types,
                        table_meta,
                        ks,
                        state,
                    )?;

                    all_rows
                }

                ScanPlan::FullScan => {
                    // Observability: record the predicate that triggered the full
                    // scan so operators can find queries that need an index.
                    {
                        let (pred_col, pred_op) = s
                            .where_clauses
                            .iter()
                            .find(|wc| !wc.token_fn)
                            .map(|wc| (wc.column.as_str(), comparison_op_str(&wc.op)))
                            .unwrap_or(("", ""));
                        state
                            .full_scan_tracker
                            .record(ks, &s.table, pred_col, pred_op);
                    }

                    // Check ALLOW FILTERING requirement.
                    let indexed_columns: Vec<String> = snap
                        .indexes
                        .iter()
                        .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
                        .flat_map(|(_, meta)| meta.target_columns.iter().cloned())
                        .collect();

                    // Exclude token() predicates — they are range scan hints,
                    // not column filters that require indexing.
                    let non_token_clauses: Vec<&WhereClause> =
                        s.where_clauses.iter().filter(|wc| !wc.token_fn).collect();
                    let all_where_columns_indexed = non_token_clauses
                        .iter()
                        .all(|wc| indexed_columns.iter().any(|ic| ic == &wc.column));

                    if !non_token_clauses.is_empty()
                        && !all_where_columns_indexed
                        && !s.allow_filtering
                    {
                        return Err(CqlError::Invalid(
                            "Cannot execute this query as it requires filtering on non-indexed \
                         columns. Use ALLOW FILTERING, create a secondary index on the \
                         filtered columns, or restructure your query to use partition keys."
                                .into(),
                        ));
                    }

                    // Coordinator-side OOM bound (P0): an unbounded full-table scan
                    // with no WHERE/ORDER BY/ANN/DISTINCT/LIMIT used to accumulate
                    // the ENTIRE result into `all_rows` before returning, OOM-killing
                    // the coordinator on large tables. Stream at most one bounded
                    // page instead and return a `PagingState` continuation. A
                    // default page applies when the client sends no `page_size`, so
                    // even an un-paged `SELECT *` cannot accumulate unbounded rows.
                    //
                    // Other shapes (predicates, ORDER BY, ANN, DISTINCT, LIMIT,
                    // COUNT) keep their existing materialize-bounded behavior, where
                    // the scan window is already bounded by LIMIT/sort/count needs.
                    // Exclude any function-call projection: aggregates
                    // (COUNT/AVG/MIN/MAX/SUM) and UDAs fold over the WHOLE result,
                    // so paging the scan would compute them over a single page.
                    // Scalar UDFs/toJson are per-row and safe, but excluding all
                    // function calls keeps the streaming gate to plain column/star
                    // projections — exactly the unbounded `SELECT *` OOM shape.
                    let has_function_projection = s
                        .columns
                        .iter()
                        .any(|c| matches!(c, SelectColumn::FunctionCall { .. }));
                    let unbounded_scan_shape = s.where_clauses.is_empty()
                        && s.order_by.is_empty()
                        && s.ann_of.is_none()
                        && !s.distinct
                        && s.limit.is_none()
                        && !count_only_select
                        && !has_function_projection;
                    if unbounded_scan_shape {
                        let page_size = ctx
                            .paging
                            .page_size
                            .and_then(|ps| (ps > 0).then_some(ps as usize))
                            .unwrap_or_else(crate::paging::default_scan_page_size);

                        let resume = StreamResumeCursor::from_paging_state(
                            ctx.paging.paging_state.as_deref(),
                        )?;
                        // Resume the scan at the last partition key (inclusive); the
                        // collector drops rows already emitted within that key.
                        let start_key = resume.as_ref().map(|cur| {
                            ferrosa_common::key::DecoratedKey::new(
                                ferrosa_common::key::PartitionKey::from(
                                    cur.partition_key.as_slice(),
                                ),
                            )
                        });

                        // The gate above guarantees `where_clauses.is_empty()`, so a
                        // column projection is always safe here (no predicate reads
                        // an unprojected cell).
                        let scan_projection =
                            projection_storage_ordinals_for_select_scan(s, table_meta);

                        let stream = if let Some(wanted) = scan_projection {
                            state
                                .write_path
                                .load()
                                .range_read_projected_stream_all_from(
                                    &table_id,
                                    wanted,
                                    start_key.as_ref(),
                                    ctx.consistency,
                                    &table_strategy,
                                )
                                .await?
                        } else {
                            state
                                .write_path
                                .load()
                                .range_read_stream_all_from(
                                    &table_id,
                                    start_key.as_ref(),
                                    ctx.consistency,
                                    &table_strategy,
                                )
                                .await?
                        };

                        let page = collect_page_from_partition_stream(
                            stream,
                            page_size,
                            resume,
                            PartitionRowContext {
                                all_col_names: &all_col_names,
                                all_col_types: &all_col_types,
                                pk_indices: &pk_indices,
                                ck_indices: &ck_indices,
                                storage_to_table: &storage_to_table,
                            },
                        )
                        .await?;

                        streamed_paging_state = Some(page.next_paging_state);
                        page.rows
                    } else {
                        // Use a bounded upstream partition cap for unordered,
                        // non-aggregate scans so first pages do not wait behind an
                        // unbounded table materialization. When ALLOW FILTERING has
                        // post-filter predicates, no upstream partition cap is safe:
                        // LIMIT 1 may need to inspect many non-matching partitions
                        // before finding the first matching row, and a fixed cap would
                        // silently drop later matches. In that shape, stream the full
                        // table and apply LIMIT/page semantics after filtering.
                        // Ordered and aggregate queries still materialize their scan
                        // window before sorting/counting.
                        let scan_bound = if s.order_by.is_empty()
                            && s.ann_of.is_none()
                            && !count_only_select
                        {
                            let has_post_filter =
                                s.allow_filtering && !non_token_clauses.is_empty();
                            if has_post_filter {
                                None
                            } else {
                                let page_size = ctx
                                    .paging
                                    .page_size
                                    .and_then(|ps| (ps > 0).then_some(ps as usize));
                                let limit_size = s
                                    .limit
                                    .as_ref()
                                    .and_then(|l| l.as_literal())
                                    .map(|n| n as usize);
                                let base = match (page_size, limit_size) {
                                    (Some(ps), Some(lim)) => Some(std::cmp::min(ps, lim)),
                                    (Some(ps), None) => Some(ps),
                                    (None, Some(lim)) => Some(lim),
                                    (None, None) => None,
                                };
                                let start = ctx
                                    .paging
                                    .paging_state
                                    .as_deref()
                                    .and_then(|bytes| {
                                        crate::paging::PagingState::decode(bytes).ok()
                                    })
                                    .and_then(|state| {
                                        (state.partition_key.len() == 8).then(|| {
                                            u64::from_be_bytes(
                                                state.partition_key.as_slice().try_into().unwrap(),
                                            ) as usize
                                        })
                                    })
                                    .unwrap_or(0);
                                base.map(|n| start.saturating_add(n).max(1))
                            }
                        } else {
                            None
                        };
                        let row_limit =
                            safe_partition_key_filter_row_limit(s, table_meta, count_only_select)
                                .unwrap_or(0);
                        // ADR-020 projection fast path. Route through
                        // range_read_projected whenever the query only needs a subset
                        // of regular cells, so the SSTable layer byte-skips bulky
                        // unneeded payloads. Big win on wide tables with bulky cells
                        // (e.g. entity_store's entity_embedding column).
                        //
                        // Non-count SELECT requires no WHERE because predicates over
                        // unprojected regular columns would evaluate against NULL.
                        let projection_wanted = if !count_only_select && s.where_clauses.is_empty()
                        {
                            projection_storage_ordinals_for_select_scan(s, table_meta)
                        } else {
                            None
                        };
                        // Count-only filtered scans project predicate columns only and
                        // fold over a partition stream, so COUNT(*) avoids decoding
                        // unrelated cells without first collecting partitions in a Vec.
                        let count_projection_wanted = if count_only_select {
                            projection_storage_ordinals_for_count_predicates(
                                &s.where_clauses,
                                table_meta,
                            )
                        } else {
                            None
                        };
                        let partitions = if let Some(wanted) = projection_wanted {
                            // Push partition-count cap down to the merger so
                            // `LIMIT N` stops the scan after N partitions
                            // rather than walking every SSTable.
                            Some(
                                state
                                    .write_path
                                    .load()
                                    .range_read_projected(&table_id, wanted, scan_bound)
                                    .await?,
                            )
                        } else if let Some(bound) = scan_bound {
                            Some(
                                state
                                    .write_path
                                    .load()
                                    .range_read_limited_rows(&table_id, bound, row_limit)
                                    .await?,
                            )
                        } else if row_limit > 0 {
                            Some(
                                state
                                    .write_path
                                    .load()
                                    .range_read_limited_rows(
                                        &table_id,
                                        ferrosa_cluster::write_path::DEFAULT_RANGE_READ_LIMIT,
                                        row_limit,
                                    )
                                    .await?,
                            )
                        } else {
                            None
                        };
                        if count_only_select {
                            let row_context = PartitionRowContext {
                                all_col_names: &all_col_names,
                                all_col_types: &all_col_types,
                                pk_indices: &pk_indices,
                                ck_indices: &ck_indices,
                                storage_to_table: &storage_to_table,
                            };
                            let predicate_context = SelectPredicateContext {
                                statement: s,
                                table_meta,
                                keyspace: ks,
                                state,
                            };
                            let count = if let Some(partitions) = partitions.as_ref() {
                                count_rows_from_partitions(
                                    partitions,
                                    row_context,
                                    predicate_context,
                                )
                                .await?
                            } else if let Some(wanted) = count_projection_wanted {
                                let stream = state
                                    .write_path
                                    .load()
                                    .range_read_projected_stream_all_with(
                                        &table_id,
                                        wanted,
                                        scan_bound,
                                        ctx.consistency,
                                        &table_strategy,
                                    )
                                    .await?;
                                count_rows_from_partition_stream(
                                    stream,
                                    row_context,
                                    predicate_context,
                                )
                                .await?
                            } else {
                                let stream = state
                                    .write_path
                                    .load()
                                    .range_read_stream_all_with(
                                        &table_id,
                                        row_limit,
                                        ctx.consistency,
                                        &table_strategy,
                                    )
                                    .await?;
                                count_rows_from_partition_stream(
                                    stream,
                                    row_context,
                                    predicate_context,
                                )
                                .await?
                            };
                            return Ok(SelectRawResult {
                                column_names: col_names.to_vec(),
                                column_types: col_types.to_vec(),
                                rows: vec![vec![Some(CqlValue::Bigint(count))]],
                                keyspace: ks.to_string(),
                                table: s.table.clone(),
                                paging_state: None,
                            });
                        }
                        let mut all_rows = Vec::new();
                        if let Some(partitions) = partitions.as_ref() {
                            extend_rows_from_partitions(
                                partitions,
                                &mut all_rows,
                                &all_col_names,
                                &all_col_types,
                                &pk_indices,
                                &ck_indices,
                                &storage_to_table,
                            )
                            .await;
                        } else {
                            let stream = state
                                .write_path
                                .load()
                                .range_read_stream_all_with(
                                    &table_id,
                                    row_limit,
                                    ctx.consistency,
                                    &table_strategy,
                                )
                                .await?;
                            extend_rows_from_partition_stream(
                                stream,
                                &mut all_rows,
                                &all_col_names,
                                &all_col_types,
                                &pk_indices,
                                &ck_indices,
                                &storage_to_table,
                            )
                            .await?;
                        }
                        filter_rows_by_select_predicates(
                            &mut all_rows,
                            s,
                            &all_col_names,
                            &all_col_types,
                            table_meta,
                            ks,
                            state,
                        )?;
                        all_rows
                    }
                }
            }
        }
    };

    // Classify arbitrary unbounded ORDER BY before sorting. The temp-sort
    // reservation is intentionally held until this SELECT returns; dropping it
    // on completion or cancellation cleans up the temporary table directory.
    let _order_by_temp_sort = prepare_order_by_execution(state, ks, s, table_meta)?;

    // Apply ORDER BY sorting (FRSA-BUG-004)
    let rows = if let Some((ann_col, ann_query)) = &s.ann_of {
        let mut sorted = rows;
        apply_ann_of_ordering(
            &mut sorted,
            ann_col,
            ann_query,
            &all_col_names,
            &all_col_types,
        )?;
        sorted
    } else if !s.order_by.is_empty() {
        let mut sorted = rows;
        // Resolve column indices for ORDER BY columns
        let order_specs: Vec<(usize, bool)> = s
            .order_by
            .iter()
            .filter_map(|(col_name, dir)| {
                let idx = all_col_names.iter().position(|n| n == col_name)?;
                let ascending = *dir == OrderDirection::Asc;
                Some((idx, ascending))
            })
            .collect();
        sorted.sort_by(|a, b| {
            for &(idx, ascending) in &order_specs {
                let cmp = match (&a[idx], &b[idx]) {
                    (Some(va), Some(vb)) => va.cmp(vb),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let cmp = if ascending { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
        sorted
    } else {
        rows
    };

    // Resolve UDFs/UDAs in SELECT columns.
    let mut resolved_funcs: Vec<(usize, ResolvedFunction)> = Vec::new();
    for (i, sc) in s.columns.iter().enumerate() {
        if let SelectColumn::FunctionCall {
            keyspace: func_ks,
            name,
            args,
            alias,
        } = sc
        {
            let fn_lower = name.to_lowercase();
            // Skip built-in functions — they are handled inline.
            if matches!(
                fn_lower.as_str(),
                "count"
                    | "avg"
                    | "min"
                    | "max"
                    | "sum"
                    | "writetime"
                    | "ttl"
                    | "uuid"
                    | "now"
                    | "totimestamp"
                    | "todate"
            ) {
                continue;
            }
            let resolved = resolve_select_function(
                ks,
                func_ks.as_deref(),
                name,
                args,
                alias.as_deref(),
                &all_col_names,
                &all_col_types,
                &state.schema,
            )?;
            resolved_funcs.push((i, resolved));
        }
    }

    // Check for aggregate functions (builtin COUNT/AVG/MIN/MAX/SUM or UDA).
    let has_builtin_agg = s.columns.iter().any(|c| {
        matches!(c, SelectColumn::FunctionCall { name, .. }
            if name.eq_ignore_ascii_case("count")
                || name.eq_ignore_ascii_case("avg")
                || name.eq_ignore_ascii_case("min")
                || name.eq_ignore_ascii_case("max")
                || name.eq_ignore_ascii_case("sum"))
    });
    let has_uda = resolved_funcs
        .iter()
        .any(|(_, f)| matches!(f.kind, ResolvedFunctionKind::Aggregate { .. }));
    let is_aggregate = has_builtin_agg || has_uda;

    if is_aggregate {
        // Build a single result row with aggregate values.
        let mut agg_row: Vec<Option<CqlValue>> = Vec::with_capacity(s.columns.len());
        for (col_idx, sc) in s.columns.iter().enumerate() {
            match sc {
                SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count") => {
                    agg_row.push(Some(CqlValue::Bigint(rows.len() as i64)));
                }
                SelectColumn::FunctionCall { name, args, .. }
                    if name.eq_ignore_ascii_case("avg")
                        || name.eq_ignore_ascii_case("min")
                        || name.eq_ignore_ascii_case("max")
                        || name.eq_ignore_ascii_case("sum") =>
                {
                    let fn_lower = name.to_lowercase();
                    // Resolve the target column from the first argument.
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                let col_type = &all_col_types[src_idx];
                                let result =
                                    compute_builtin_aggregate(&fn_lower, &rows, src_idx, col_type);
                                agg_row.push(result);
                            } else {
                                return Err(CqlError::Invalid(format!(
                                    "unknown column in {}(): {}",
                                    fn_lower, col_name
                                )));
                            }
                        } else {
                            agg_row.push(Some(CqlValue::Null));
                        }
                    } else {
                        agg_row.push(Some(CqlValue::Null));
                    }
                }
                SelectColumn::FunctionCall { .. } => {
                    // Check if this column is a resolved UDA.
                    if let Some((_, ref func)) = resolved_funcs.iter().find(|(i, _)| *i == col_idx)
                    {
                        if matches!(func.kind, ResolvedFunctionKind::Aggregate { .. }) {
                            let result = execute_uda(state, ks, func, &rows)?;
                            agg_row.push(Some(result));
                        } else {
                            // Scalar UDF in an aggregate query — evaluate on
                            // first row if available, otherwise NULL.
                            agg_row.push(None);
                        }
                    } else {
                        agg_row.push(None);
                    }
                }
                _ => {
                    agg_row.push(None);
                }
            }
        }
        let agg_rows = vec![agg_row];
        return Ok(SelectRawResult {
            column_names: col_names.to_vec(),
            column_types: col_types.to_vec(),
            rows: agg_rows,
            keyspace: ks.to_string(),
            table: s.table.clone(),
            paging_state: None,
        });
    }

    // Apply column selection if not Star
    let selected_rows = select_columns(&rows, &all_col_names, &col_names);

    // Evaluate scalar UDFs on each projected row.
    let selected_rows = if resolved_funcs.is_empty() {
        selected_rows
    } else {
        // We need the full rows to extract UDF args, then replace the UDF
        // columns in the projected output.
        let scalar_funcs: Vec<&ResolvedFunction> = resolved_funcs
            .iter()
            .filter(|(_, f)| matches!(f.kind, ResolvedFunctionKind::Scalar))
            .map(|(_, f)| f)
            .collect();

        if scalar_funcs.is_empty() {
            selected_rows
        } else {
            let mut result_rows = selected_rows;
            for (full_row, projected_row) in rows.iter().zip(result_rows.iter_mut()) {
                let udf_results = evaluate_row_udfs(state, full_row, &scalar_funcs)?;
                // Map UDF results back into the projected row. Each resolved
                // func knows its column index in the SELECT list.
                for ((col_idx, _), udf_val) in resolved_funcs.iter().zip(udf_results.iter()) {
                    // Find the position of col_idx in the projected columns.
                    if let Some(proj_pos) = col_names.iter().position(|n| {
                        if let Some((_, ref f)) = resolved_funcs.iter().find(|(i, _)| i == col_idx)
                        {
                            *n == f.display_name
                        } else {
                            false
                        }
                    }) {
                        projected_row[proj_pos] = udf_val.clone();
                    }
                }
            }
            result_rows
        }
    };

    // Apply toJson() built-in on projected columns.
    let selected_rows =
        apply_tojson_projections(&s.columns, &col_names, &all_col_names, &rows, selected_rows);

    let selected_rows = if s.distinct {
        let mut seen = std::collections::BTreeSet::new();
        selected_rows
            .into_iter()
            .filter(|row| seen.insert(row.clone()))
            .collect()
    } else {
        selected_rows
    };

    // Apply LIMIT
    let limit_val = s.limit.as_ref().and_then(|l| l.as_literal());
    let limited = if let Some(limit) = limit_val {
        &selected_rows[..std::cmp::min(selected_rows.len(), limit as usize)]
    } else {
        &selected_rows
    };

    // Apply pagination: page_size interacts with LIMIT.
    // If both page_size and LIMIT are set, the effective limit is min(page_size, limit).
    // Pagination operates on the already-limited result set.
    let effective_page_size = match (ctx.paging.page_size, limit_val) {
        (Some(ps), Some(lim)) => Some(std::cmp::min(ps, lim)),
        (Some(ps), None) => Some(ps),
        (None, _) => None,
    };

    // When the streaming full-scan page path already produced exactly one
    // bounded page and its cursor, skip the generic offset-based pagination:
    // the rows ARE the page and the continuation is the stream cursor.
    let (page_rows, next_paging_state): (Vec<Vec<Option<CqlValue>>>, Option<Vec<u8>>) =
        if let Some(stream_cursor) = streamed_paging_state {
            (limited.to_vec(), stream_cursor)
        } else {
            let paged = crate::paging::apply_pagination(
                limited.len(),
                effective_page_size,
                ctx.paging.paging_state.as_deref(),
            )?;
            (
                limited[paged.start..paged.end].to_vec(),
                paged.next_paging_state,
            )
        };

    // Return raw result; callers that need an encoded frame call .encode()
    // or result::encode_rows_paged directly. Delta subscriptions use the raw rows.
    Ok(SelectRawResult {
        column_names: col_names.to_vec(),
        column_types: col_types.to_vec(),
        rows: page_rows,
        keyspace: ks.to_string(),
        table: s.table.clone(),
        paging_state: next_paging_state,
    })
}

/// Execute a SELECT against a user table and return raw (un-encoded) rows.
///
/// Used by delta subscriptions to compute a row-level diff. Only supports
/// user tables; system table subscriptions are not expected to use delta mode.
pub async fn route_select_raw(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &SelectStatement,
) -> Result<SelectRawResult, CqlError> {
    let ks = s
        .keyspace
        .as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;
    route_select_user_table(state, ctx, ks, s).await
}

// ── Virtual table helpers ────────────────────────────────────────────────

/// Convert a `DataType` (from ferrosa-common) to the CQL protocol `CqlType`.
fn data_type_to_cql_type(dt: &DataType) -> CqlType {
    match dt {
        DataType::Text => CqlType::Varchar,
        DataType::Int => CqlType::Int,
        DataType::BigInt => CqlType::Bigint,
        DataType::Double => CqlType::Double,
        DataType::Boolean => CqlType::Boolean,
        DataType::Uuid => CqlType::Uuid,
        DataType::Timestamp => CqlType::Timestamp,
        DataType::Blob => CqlType::Blob,
        DataType::Duration => CqlType::Duration,
        // DataType is #[non_exhaustive]; treat unknown variants as blob.
        _ => CqlType::Blob,
    }
}

/// Convert a `CellValue` (raw bytes) to a typed `CqlValue` using the column's `DataType`.
///
/// Returns `None` (CQL null) for tombstones or cells with no value.
fn cell_to_cql_value(cell: &ferrosa_common::CellValue, dt: &DataType) -> Option<CqlValue> {
    let bytes = cell.value.as_ref()?;
    Some(match dt {
        DataType::Text => CqlValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        DataType::Int => {
            let arr: [u8; 4] = bytes.as_slice().try_into().unwrap_or([0; 4]);
            CqlValue::Int(i32::from_be_bytes(arr))
        }
        DataType::BigInt => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Bigint(i64::from_be_bytes(arr))
        }
        DataType::Double => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Double(u64::from_be_bytes(arr))
        }
        DataType::Boolean => CqlValue::Boolean(!bytes.is_empty() && bytes[0] != 0),
        DataType::Uuid => {
            let arr: [u8; 16] = bytes.as_slice().try_into().unwrap_or([0; 16]);
            CqlValue::Uuid(uuid::Uuid::from_bytes(arr))
        }
        DataType::Timestamp => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Timestamp(i64::from_be_bytes(arr))
        }
        DataType::Blob => CqlValue::Blob(bytes.clone()),
        // DataType is #[non_exhaustive]; treat unknown as blob.
        _ => CqlValue::Blob(bytes.clone()),
    })
}

/// Decode a `list<text>` virtual cell. The bytes were produced by
/// `VirtualColumnDef::encode_list_text` (4-byte BE count, then per
/// element 4-byte BE length + UTF-8 bytes).
fn decode_list_text_cell(cell: &ferrosa_common::CellValue) -> Option<CqlValue> {
    let bytes = cell.value.as_ref()?;
    if bytes.len() < 4 {
        return None;
    }
    let count = i32::from_be_bytes(bytes[0..4].try_into().ok()?);
    if count < 0 {
        return None;
    }
    let mut items: Vec<CqlValue> = Vec::with_capacity(count as usize);
    let mut off = 4usize;
    for _ in 0..count {
        if off + 4 > bytes.len() {
            return None;
        }
        let len = i32::from_be_bytes(bytes[off..off + 4].try_into().ok()?);
        off += 4;
        if len < 0 {
            items.push(CqlValue::Text(String::new()));
            continue;
        }
        let len = len as usize;
        if off + len > bytes.len() {
            return None;
        }
        let s = String::from_utf8_lossy(&bytes[off..off + len]).into_owned();
        off += len;
        items.push(CqlValue::Text(s));
    }
    Some(CqlValue::List(items))
}

fn encode_virtual_rows_streaming(
    keyspace: &str,
    table: &str,
    vtable: &dyn ferrosa_schema::VirtualTable,
    predicate: Option<&RowPredicate>,
) -> Result<BytesMut, CqlError> {
    let columns = vtable.columns();
    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let col_types: Vec<CqlType> = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| match vtable.wire_type_for(idx) {
            Some(ferrosa_schema::WireType::ListText) => CqlType::List(Box::new(CqlType::Varchar)),
            None => data_type_to_cql_type(&c.data_type),
        })
        .collect();

    Ok(result::encode_rows_with_writer(
        &col_names,
        &col_types,
        keyspace,
        table,
        |emit| {
            vtable.visit_rows(predicate, &mut |row| {
                let cql_row: Vec<Option<CqlValue>> = row
                    .cells
                    .iter()
                    .zip(columns.iter().enumerate())
                    .map(|(cell, (idx, col))| match vtable.wire_type_for(idx) {
                        Some(ferrosa_schema::WireType::ListText) => decode_list_text_cell(cell),
                        None => cell_to_cql_value(cell, &col.data_type),
                    })
                    .collect();
                emit(&cql_row);
            });
        },
    ))
}

/// Render a `ComparisonOp` as the operator string recorded in
/// `system_observability.full_scan_reasons`.
fn comparison_op_str(op: &ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Gt => ">",
        ComparisonOp::Le => "<=",
        ComparisonOp::Ge => ">=",
        ComparisonOp::In => "IN",
        ComparisonOp::Ne => "!=",
        ComparisonOp::Contains => "CONTAINS",
        ComparisonOp::ContainsKey => "CONTAINS KEY",
        ComparisonOp::SoundsLike => "SOUNDS LIKE",
        ComparisonOp::Like => "LIKE",
    }
}

// ── EXPLAIN ──────────────────────────────────────────────────────────────

fn route_explain(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: SelectStatement,
) -> Result<BytesMut, CqlError> {
    let ks = s
        .keyspace
        .as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    // EXPLAIN mirrors the SELECT planner exactly: a partial (Filtered) index is
    // offered only when the query implies its predicate, so EXPLAIN reports the
    // filtered index when it WOULD be used and FullScan when it would not.
    let planner_indexes: Vec<(String, Vec<String>)> = snap
        .indexes
        .iter()
        .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
        .filter(|(_, meta)| {
            filtered_index_is_usable(meta, &s.where_clauses, table_meta, ks, &state.schema)
        })
        .map(|(_, meta)| (meta.name.clone(), meta.target_columns.clone()))
        .collect();

    // Geospatial queries are served by the geo index in their own branch, not by
    // the generic planner, so report `GeoIndex` here rather than FullScan.
    // Full-text queries (`WHERE col = fts_match(...)`) are likewise served by the
    // FTI in an early branch, so report `FullTextIndex` rather than FullScan.
    let scan_plan = if let Some(geo_plan) = geo_explain_plan(&snap, ks, &s) {
        geo_plan
    } else if let Some(fts_plan) = fulltext_explain_plan(&snap, ks, &s) {
        fts_plan
    } else {
        planner::plan(
            &s.where_clauses,
            &table_meta.partition_key,
            &planner_indexes,
        )
    };

    // `ORDER BY col ANN OF [...]` on a vector-indexed column is served by the
    // index consult in `route_select_user_table`, not the WHERE-clause planner,
    // so EXPLAIN must report the vector index rather than the planner's
    // (FullScan) verdict for the empty WHERE clause.
    let scan_plan = match s.ann_of.as_ref() {
        Some((ann_col, _)) => match vector_index_for_ann_column(&snap, ks, &s.table, ann_col) {
            Some(index_name) => ScanPlan::VectorAnn {
                index_name,
                index_column: ann_col.clone(),
            },
            None => scan_plan,
        },
        None => scan_plan,
    };

    let plan_text = format!("{scan_plan}");

    let col_names = vec!["plan".to_string()];
    let col_types = vec![CqlType::Varchar];
    let rows = vec![vec![Some(CqlValue::Text(plan_text))]];
    Ok(result::encode_rows(
        &col_names, &col_types, ks, &s.table, &rows,
    ))
}

/// Build a [`ScanPlan::GeoIndex`] for EXPLAIN when `s` carries a geospatial
/// operation backed by a registered geo index. Returns `None` when there is no
/// geo operation (so the generic planner runs) or no matching geo index.
fn geo_explain_plan(
    snap: &ferrosa_schema::SchemaSnapshot,
    ks: &str,
    s: &SelectStatement,
) -> Option<ScanPlan> {
    let (column, op) = if let Some(gn) = &s.geo_nearest {
        (gn.column.clone(), "GeoNearest")
    } else if let Some(pred) = s.geo_predicates.first() {
        let op = match pred {
            GeoPredicate::WithinRadius { .. } => "GeoWithinRadius",
            GeoPredicate::WithinBbox { .. } => "GeoWithinBbox",
            GeoPredicate::WithinPolygon { .. } => "GeoWithinPolygon",
        };
        (pred.column().to_string(), op)
    } else {
        return None;
    };
    let index_name = resolve_geo_index_name(snap, ks, &s.table, &column)?;
    Some(ScanPlan::GeoIndex {
        index_name,
        index_column: column,
        op: op.to_string(),
    })
}

/// Build a [`ScanPlan::FullTextIndex`] for EXPLAIN when `s` carries an
/// `fts_match()` predicate. Returns `None` when there is no full-text predicate
/// (so the generic planner runs). The index-name resolution mirrors the
/// execution path in `route_select_user_table` exactly: prefer a registered
/// index whose target columns include the searched column, else fall back to
/// the column name itself (matching simple single-column FTI registration).
fn fulltext_explain_plan(
    snap: &ferrosa_schema::SchemaSnapshot,
    ks: &str,
    s: &SelectStatement,
) -> Option<ScanPlan> {
    if !where_has_fts_match(&s.where_clauses) {
        return None;
    }
    let (fts_column, _fts_query) = extract_fts_match(&s.where_clauses)?;
    let index_name = resolve_fulltext_index_name(snap, ks, &s.table, fts_column);
    Some(ScanPlan::FullTextIndex {
        index_name,
        index_column: fts_column.to_string(),
    })
}

/// Resolve the full-text index name serving `column` on `ks.table`. Prefers a
/// registered index whose target columns include `column`; otherwise falls back
/// to the column name itself. This is the single source of truth shared by the
/// EXPLAIN reporting path and the execution path so they never diverge.
fn resolve_fulltext_index_name(
    snap: &ferrosa_schema::SchemaSnapshot,
    ks: &str,
    table: &str,
    column: &str,
) -> String {
    snap.indexes
        .iter()
        .find(|((idx_ks, idx_tbl, _), meta)| {
            idx_ks == ks && idx_tbl == table && meta.target_columns.iter().any(|c| c == column)
        })
        .map(|((_, _, _), meta)| meta.name.clone())
        .unwrap_or_else(|| column.to_string())
}

// ── USING TIMESTAMP / TTL helpers (Gap 10) ───────────────────────────────
//
// `using_timestamp` and `using_ttl` are now `Option<Term>` so the prepare
// path can register bind markers (`USING TIMESTAMP ?`).  By the time the
// router executes, `substitute_in_statement` has replaced any
// `Term::BindMarker` with the bound literal.  These helpers extract the
// integer with a clear error if substitution was missed or the literal is
// out of range.

fn using_timestamp_as_i64(t: &Option<Term>) -> Result<Option<i64>, CqlError> {
    match t {
        None => Ok(None),
        Some(Term::IntegerLiteral(n)) => Ok(Some(*n)),
        Some(Term::BindMarker(_)) => Err(CqlError::Protocol(
            "USING TIMESTAMP bind marker was not substituted before execution".into(),
        )),
        Some(other) => Err(CqlError::Invalid(format!(
            "USING TIMESTAMP must be an integer literal or bind marker, got {other:?}"
        ))),
    }
}

fn using_ttl_as_i32(t: &Option<Term>) -> Result<Option<i32>, CqlError> {
    match t {
        None => Ok(None),
        Some(Term::IntegerLiteral(n)) => i32::try_from(*n)
            .map(Some)
            .map_err(|_| CqlError::Invalid(format!("USING TTL value {n} out of i32 range"))),
        Some(Term::BindMarker(_)) => Err(CqlError::Protocol(
            "USING TTL bind marker was not substituted before execution".into(),
        )),
        Some(other) => Err(CqlError::Invalid(format!(
            "USING TTL must be an integer literal or bind marker, got {other:?}"
        ))),
    }
}

// ── INSERT ───────────────────────────────────────────────────────────────

fn term_has_bind_marker(term: &Term) -> bool {
    match term {
        Term::BindMarker(_) => true,
        Term::InList(items)
        | Term::ListLiteral(items)
        | Term::SetLiteral(items)
        | Term::TupleLiteral(items) => items.iter().any(term_has_bind_marker),
        Term::MapLiteral(entries) => entries
            .iter()
            .any(|(key, value)| term_has_bind_marker(key) || term_has_bind_marker(value)),
        Term::FunctionCall { args, .. } => args.iter().any(term_has_bind_marker),
        Term::TemporalArithmetic { base, offset, .. } => {
            term_has_bind_marker(base) || term_has_bind_marker(offset)
        }
        _ => false,
    }
}

fn next_prepared_insert_term<'a>(
    template: &'a Term,
    bound_terms: &'a [Term],
    bind_idx: &mut usize,
) -> Option<&'a Term> {
    match template {
        Term::BindMarker(_) => {
            let term = bound_terms.get(*bind_idx)?;
            *bind_idx += 1;
            Some(term)
        }
        other if term_has_bind_marker(other) => None,
        other => Some(other),
    }
}

fn prepared_insert_term_fast_supported(term: &Term) -> bool {
    matches!(term, Term::BindMarker(_)) || !term_has_bind_marker(term)
}

fn using_timestamp_term_as_i64(t: Option<&Term>) -> Result<Option<i64>, CqlError> {
    match t {
        None => Ok(None),
        Some(Term::IntegerLiteral(n)) => Ok(Some(*n)),
        Some(Term::BindMarker(_)) => Err(CqlError::Protocol(
            "USING TIMESTAMP bind marker was not substituted before execution".into(),
        )),
        Some(other) => Err(CqlError::Invalid(format!(
            "USING TIMESTAMP must be an integer literal or bind marker, got {other:?}"
        ))),
    }
}

fn using_ttl_term_as_i32(t: Option<&Term>) -> Result<Option<i32>, CqlError> {
    match t {
        None => Ok(None),
        Some(Term::IntegerLiteral(n)) => i32::try_from(*n)
            .map(Some)
            .map_err(|_| CqlError::Invalid(format!("USING TTL value {n} out of i32 range"))),
        Some(Term::BindMarker(_)) => Err(CqlError::Protocol(
            "USING TTL bind marker was not substituted before execution".into(),
        )),
        Some(other) => Err(CqlError::Invalid(format!(
            "USING TTL must be an integer literal or bind marker, got {other:?}"
        ))),
    }
}

/// Execute a common prepared INSERT shape without cloning and rewriting the
/// full AST. This intentionally supports only top-level bind markers in VALUES
/// and USING TIMESTAMP/TTL; complex nested bind markers fall back to the
/// generic substitution path in the connection handler.
pub async fn route_prepared_insert_fast(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &InsertStatement,
    bound_terms: &[Term],
) -> Option<Result<RouteResult, CqlError>> {
    if s.if_not_exists || s.columns.len() != s.values.len() {
        return None;
    }
    if s.values
        .iter()
        .any(|term| !prepared_insert_term_fast_supported(term))
        || s.using_timestamp
            .as_ref()
            .is_some_and(|term| !prepared_insert_term_fast_supported(term))
        || s.using_ttl
            .as_ref()
            .is_some_and(|term| !prepared_insert_term_fast_supported(term))
    {
        return None;
    }

    let opcode = CqlOpcode::Insert;
    let query_desc = format!("INSERT {}.{}", s.keyspace.as_deref().unwrap_or(""), s.table);
    let keyspace = ctx.current_keyspace.as_deref().unwrap_or("");
    let _guard = state.query_tracker.begin_guarded(
        &query_desc,
        keyspace,
        &ctx.client_address,
        &ctx.auth.role,
    );

    let result = async {
        let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;
        validate_keyspace_exists(&state.schema, ks)?;

        state.schema.check_permission(
            ctx.auth,
            Permission::Modify,
            &Resource::Table(ks.to_string(), s.table.clone()),
        )?;

        let snap = state.schema.snapshot();
        let table_meta = snap
            .tables
            .get(&(ks.to_string(), s.table.clone()))
            .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

        let mut pk_vals: Vec<(i32, CqlValue)> = Vec::new();
        let mut ck_vals: Vec<(i32, CqlValue)> = Vec::new();
        let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
        let mut bind_idx = 0usize;

        for (i, col_name) in s.columns.iter().enumerate() {
            let term = next_prepared_insert_term(&s.values[i], bound_terms, &mut bind_idx)
                .ok_or_else(|| {
                    CqlError::Invalid("unsupported prepared INSERT bind shape".into())
                })?;
            let col_meta = table_meta
                .columns
                .get(col_name)
                .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
            let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
            let value = bridge::term_to_cql_value(term, &cql_type)?;

            match col_meta.kind {
                ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
                ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
                ColumnKind::Regular | ColumnKind::Static => {
                    let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
                        CqlError::Invalid(format!(
                            "column '{}' not found in storage schema",
                            col_name
                        ))
                    })?;
                    regular_cells.push((col_idx, value));
                }
            }
        }

        let timestamp_term = match &s.using_timestamp {
            Some(t) => Some(
                next_prepared_insert_term(t, bound_terms, &mut bind_idx).ok_or_else(|| {
                    CqlError::Invalid(
                        "unsupported prepared INSERT USING TIMESTAMP bind shape".into(),
                    )
                })?,
            ),
            None => None,
        };
        let ttl_term = match &s.using_ttl {
            Some(t) => Some(
                next_prepared_insert_term(t, bound_terms, &mut bind_idx).ok_or_else(|| {
                    CqlError::Invalid("unsupported prepared INSERT USING TTL bind shape".into())
                })?,
            ),
            None => None,
        };
        let timestamp = match using_timestamp_term_as_i64(timestamp_term)? {
            Some(ts) => ts,
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
                .as_micros() as i64,
        };

        pk_vals.sort_by_key(|(pos, _)| *pos);
        ck_vals.sort_by_key(|(pos, _)| *pos);
        let pk_values: Vec<CqlValue> = pk_vals.into_iter().map(|(_, v)| v).collect();
        let ck_values: Vec<CqlValue> = ck_vals.into_iter().map(|(_, v)| v).collect();
        let pk_types: Vec<CqlType> = table_meta
            .partition_key
            .iter()
            .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;

        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        let row = bridge::build_row(
            &regular_cells,
            &ck_values,
            timestamp,
            using_ttl_term_as_i32(ttl_term)?,
        );
        let table_id = TableId::new(ks, &s.table);
        let strategy = keyspace_strategy(&state.schema, ks);

        state
            .write_path
            .load()
            .write(
                &table_id,
                &decorated_key,
                row,
                timestamp,
                ctx.consistency,
                &strategy,
            )
            .await?;

        Ok(RouteResult::Result(result::encode_void()))
    }
    .await;

    state.cql_metrics.inc_request(opcode);
    if result.is_err() {
        state.cql_metrics.inc_error();
    }
    Some(result)
}

async fn route_insert(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: InsertStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;
    validate_keyspace_exists(&state.schema, ks)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    // Convert terms to CqlValues using target column types
    let mut pk_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut ck_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    let timestamp = match using_timestamp_as_i64(&s.using_timestamp)? {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    for (i, col_name) in s.columns.iter().enumerate() {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
        let value = bridge::term_to_cql_value(&s.values[i], &cql_type)?;

        match col_meta.kind {
            ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
            ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
            ColumnKind::Regular | ColumnKind::Static => {
                let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
                    CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
                })?;
                regular_cells.push((col_idx, value));
            }
        }
    }

    // Sort PK and CK by position
    pk_vals.sort_by_key(|(pos, _)| *pos);
    ck_vals.sort_by_key(|(pos, _)| *pos);
    let pk_values: Vec<CqlValue> = pk_vals.into_iter().map(|(_, v)| v).collect();
    let ck_values: Vec<CqlValue> = ck_vals.into_iter().map(|(_, v)| v).collect();
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(
        &regular_cells,
        &ck_values,
        timestamp,
        using_ttl_as_i32(&s.using_ttl)?,
    );
    let table_id = TableId::new(ks, &s.table);
    let strategy = keyspace_strategy(&state.schema, ks);

    // BUG-0016: IF NOT EXISTS — check whether the row already exists before writing.
    if s.if_not_exists {
        let existing_row = if let Some(partition) = state
            .write_path
            .load()
            .read(&table_id, &decorated_key)
            .await
            .map_err(|e| CqlError::ServerError(format!("{e}")))?
        {
            let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
            let all_col_types: Vec<CqlType> = table_meta
                .columns
                .values()
                .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
                .collect::<Result<Vec<_>, _>>()?;
            let pk_indices: Vec<usize> = table_meta
                .partition_key
                .iter()
                .filter_map(|name| table_meta.columns.get_index_of(name))
                .collect();
            let ck_indices: Vec<usize> = table_meta
                .clustering_key
                .iter()
                .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
                .collect();
            let storage_to_table = storage_to_table_indices(table_meta);
            let rows = bridge::partition_to_rows_with_storage_mapping(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
                &storage_to_table,
            );
            let matching = if ck_values.is_empty() {
                rows.into_iter().next()
            } else {
                rows.into_iter().find(|row| {
                    ck_indices
                        .iter()
                        .zip(ck_values.iter())
                        .all(|(&idx, ck_val)| row.get(idx).and_then(|v| v.as_ref()) == Some(ck_val))
                })
            };
            matching
        } else {
            None
        };

        if let Some(ref existing) = existing_row {
            // Row already exists — return [applied] = false with existing row data
            return Ok(encode_lwt_applied(
                false,
                ks,
                &s.table,
                table_meta,
                &state.schema,
                Some(existing),
            ));
        }
    }

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            &strategy,
        )
        .await?;

    if s.if_not_exists {
        // Insert was applied — return [applied] = true
        Ok(encode_lwt_applied(
            true,
            ks,
            &s.table,
            table_meta,
            &state.schema,
            None,
        ))
    } else {
        Ok(result::encode_void())
    }
}

fn route_update_virtual_table(
    ctx: &RequestContext<'_>,
    vtable: &dyn ferrosa_schema::VirtualTable,
    s: &UpdateStatement,
) -> Result<BytesMut, CqlError> {
    if !ctx.auth.is_superuser {
        return Err(CqlError::Unauthorized(
            "updating virtual control tables requires a superuser role".into(),
        ));
    }
    if s.if_exists
        || !s.if_conditions.is_empty()
        || s.using_timestamp.is_some()
        || s.using_ttl.is_some()
    {
        return Err(CqlError::Invalid(
            "virtual table updates do not support IF or USING clauses".into(),
        ));
    }

    let assignments = s
        .assignments
        .iter()
        .map(|assignment| match assignment {
            Assignment::Simple { column, value } => {
                let col = vtable
                    .columns()
                    .iter()
                    .find(|candidate| candidate.name == *column)
                    .ok_or_else(|| {
                        CqlError::Invalid(format!("unknown virtual column: {column}"))
                    })?;
                let cql_type = data_type_to_cql_type(&col.data_type);
                let cql_value = bridge::term_to_cql_value(value, &cql_type)?;
                Ok(VirtualColumnUpdate {
                    column: column.clone(),
                    value: ferrosa_common::CellValue::live(encode_value(&cql_value), 0),
                })
            }
            _ => Err(CqlError::Invalid(
                "virtual table updates only support simple assignments".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let filters = s
        .where_clauses
        .iter()
        .map(|clause| {
            let op = match clause.op {
                ComparisonOp::Eq => ferrosa_schema::PredicateOp::Eq,
                ComparisonOp::Gt => ferrosa_schema::PredicateOp::Gt,
                ComparisonOp::Lt => ferrosa_schema::PredicateOp::Lt,
                ComparisonOp::Ge => ferrosa_schema::PredicateOp::Gte,
                ComparisonOp::Le => ferrosa_schema::PredicateOp::Lte,
                _ => {
                    return Err(CqlError::Invalid(
                        "virtual table updates only support scalar WHERE comparisons".into(),
                    ));
                }
            };
            let col = vtable
                .columns()
                .iter()
                .find(|candidate| candidate.name == clause.column)
                .ok_or_else(|| {
                    CqlError::Invalid(format!("unknown virtual column: {}", clause.column))
                })?;
            let cql_type = data_type_to_cql_type(&col.data_type);
            let cql_value = bridge::term_to_cql_value(&clause.value, &cql_type)?;
            Ok(ferrosa_schema::ColumnFilter {
                column: clause.column.clone(),
                op,
                value: ferrosa_common::CellValue::live(encode_value(&cql_value), 0),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    vtable
        .apply_update(&VirtualTableUpdate {
            assignments,
            predicate: ferrosa_schema::RowPredicate { filters },
        })
        .map_err(CqlError::Invalid)?;

    Ok(result::encode_void())
}

/// Encode a lightweight-transaction `[applied]` result.
///
/// Cassandra semantics:
/// - **Applied** (`applied=true`): the result is the single `[applied]=true`
///   column. No table columns are appended — strictly-typed drivers
///   deserialize an applied conditional as `(bool,)`.
/// - **Not applied** (`applied=false`): `[applied]=false` plus the current
///   values of the conflicting row (`existing_row`), so the caller can see why
///   the condition failed.
fn encode_lwt_applied(
    applied: bool,
    keyspace: &str,
    table: &str,
    table_meta: &TableMetadata,
    schema: &Schema,
    existing_row: Option<&[Option<CqlValue>]>,
) -> BytesMut {
    if applied {
        return result::encode_rows(
            &["[applied]".to_string()],
            &[CqlType::Boolean],
            keyspace,
            table,
            &[vec![Some(CqlValue::Boolean(true))]],
        );
    }

    let mut col_names = vec!["[applied]".to_string()];
    let mut col_types = vec![CqlType::Boolean];
    let mut row: Vec<Option<CqlValue>> = vec![Some(CqlValue::Boolean(false))];

    for (i, (name, cm)) in table_meta.columns.iter().enumerate() {
        col_names.push(name.clone());
        let cql_type = resolve_col_type(&cm.column_type, keyspace, schema).unwrap_or(CqlType::Blob);
        col_types.push(cql_type);
        let val = existing_row.and_then(|r| r.get(i)).and_then(|v| v.clone());
        row.push(val);
    }

    result::encode_rows(&col_names, &col_types, keyspace, table, &[row])
}

// ── UPDATE ───────────────────────────────────────────────────────────────

async fn route_update(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: UpdateStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    if let Some(vtable) = state.schema.virtual_tables().get(ks, &s.table) {
        return route_update_virtual_table(ctx, vtable.as_ref(), &s);
    }

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = match using_timestamp_as_i64(&s.using_timestamp)? {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    // Extract PK and CK values from WHERE clauses
    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Extract CK values from WHERE
    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let table_id = TableId::new(ks, &s.table);

    // Check if any assignments require a read-modify-write (collection +/- or counter)
    let needs_read = s
        .assignments
        .iter()
        .any(|a| matches!(a, Assignment::Add { .. } | Assignment::Sub { .. }));

    // Lazily read existing row for collection Add/Sub merge
    let existing_row: Option<Vec<Option<CqlValue>>> = if needs_read {
        let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
        let all_col_types: Vec<CqlType> = table_meta
            .columns
            .values()
            .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let pk_indices: Vec<usize> = table_meta
            .partition_key
            .iter()
            .filter_map(|name| table_meta.columns.get_index_of(name))
            .collect();
        let ck_indices: Vec<usize> = table_meta
            .clustering_key
            .iter()
            .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
            .collect();
        let storage_to_table = storage_to_table_indices(table_meta);

        if let Some(partition) = state
            .write_path
            .load()
            .read(&table_id, &decorated_key)
            .await
            .map_err(|e| CqlError::ServerError(format!("{e}")))?
        {
            let rows = bridge::partition_to_rows_with_storage_mapping(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
                &storage_to_table,
            );
            // Find the row matching our CK values
            if ck_values.is_empty() {
                rows.into_iter().next()
            } else {
                rows.into_iter().find(|row| {
                    ck_indices
                        .iter()
                        .zip(ck_values.iter())
                        .all(|(&idx, ck_val)| row.get(idx).and_then(|v| v.as_ref()) == Some(ck_val))
                })
            }
        } else {
            None
        }
    } else {
        None
    };

    // Build cells from SET assignments
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for assignment in &s.assignments {
        let (col_name, value) = match assignment {
            Assignment::Simple { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(value, &cql_type)?;
                (column.as_str(), val)
            }
            Assignment::Add { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;

                // For counters: read-modify-write increment. For collections: append/merge.
                match &cql_type {
                    CqlType::Counter => {
                        let increment = bridge::term_to_cql_value(value, &CqlType::Bigint)?;
                        if let CqlValue::Bigint(n) = increment {
                            let col_table_idx = table_meta
                                .columns
                                .get_index_of(column)
                                .expect("column verified above");
                            let current = existing_row
                                .as_ref()
                                .and_then(|row| row.get(col_table_idx))
                                .and_then(|v| v.as_ref())
                                .and_then(|v| match v {
                                    CqlValue::Counter(c) => Some(*c),
                                    _ => None,
                                })
                                .unwrap_or(0);
                            (column.as_str(), CqlValue::Counter(current + n))
                        } else {
                            return Err(CqlError::Invalid(
                                "counter increment must be an integer".into(),
                            ));
                        }
                    }
                    _ => {
                        let new_val = bridge::term_to_cql_value(value, &cql_type)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_add(existing, &new_val);
                        (column.as_str(), merged)
                    }
                }
            }
            Assignment::Sub { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;

                match &cql_type {
                    CqlType::Counter => {
                        let decrement = bridge::term_to_cql_value(value, &CqlType::Bigint)?;
                        if let CqlValue::Bigint(n) = decrement {
                            let col_table_idx = table_meta
                                .columns
                                .get_index_of(column)
                                .expect("column verified above");
                            let current = existing_row
                                .as_ref()
                                .and_then(|row| row.get(col_table_idx))
                                .and_then(|v| v.as_ref())
                                .and_then(|v| match v {
                                    CqlValue::Counter(c) => Some(*c),
                                    _ => None,
                                })
                                .unwrap_or(0);
                            (column.as_str(), CqlValue::Counter(current - n))
                        } else {
                            return Err(CqlError::Invalid(
                                "counter decrement must be an integer".into(),
                            ));
                        }
                    }
                    CqlType::Map(key_type, _) => {
                        // Map subtraction: RHS is a set of keys to remove.
                        let set_of_keys = CqlType::Set(key_type.clone());
                        let keys_to_remove = bridge::term_to_cql_value(value, &set_of_keys)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_sub_map(existing, &keys_to_remove);
                        (column.as_str(), merged)
                    }
                    _ => {
                        // Set/list subtraction: remove matching elements.
                        let to_remove = bridge::term_to_cql_value(value, &cql_type)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_sub(existing, &to_remove);
                        (column.as_str(), merged)
                    }
                }
            }
            Assignment::Element {
                column,
                key: _,
                value,
            } => {
                // Map/list element set: coerce value to the element type, not the collection type
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let value_type = match &cql_type {
                    CqlType::Map(_, v) => (**v).clone(),
                    CqlType::List(v) => (**v).clone(),
                    _ => cql_type.clone(),
                };
                let val = bridge::term_to_cql_value(value, &value_type)?;
                (column.as_str(), val)
            }
        };
        let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
            CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
        })?;
        regular_cells.push((col_idx, value));
    }

    let row = bridge::build_row(
        &regular_cells,
        &ck_values,
        timestamp,
        using_ttl_as_i32(&s.using_ttl)?,
    );
    let strategy = keyspace_strategy(&state.schema, ks);

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            &strategy,
        )
        .await?;
    Ok(result::encode_void())
}

/// Merge a new collection value into an existing one (for `col = col + val`).
///
/// - List: appends new elements to the end of the existing list.
/// - Set: merges new elements into the existing set (deduplicating).
/// - Map: merges new key-value pairs, overwriting existing keys.
/// - Non-collection: returns the new value as-is.
fn collection_add(existing: Option<&CqlValue>, new_val: &CqlValue) -> CqlValue {
    match (existing, new_val) {
        (Some(CqlValue::List(old)), CqlValue::List(add)) => {
            let mut merged = old.clone();
            merged.extend(add.iter().cloned());
            CqlValue::List(merged)
        }
        (Some(CqlValue::Set(old)), CqlValue::Set(add)) => {
            let mut merged = old.clone();
            for item in add {
                if !merged.contains(item) {
                    merged.push(item.clone());
                }
            }
            CqlValue::Set(merged)
        }
        (Some(CqlValue::Map(old)), CqlValue::Map(add)) => {
            let mut merged = old.clone();
            for (k, v) in add {
                if let Some(entry) = merged.iter_mut().find(|(ek, _)| ek == k) {
                    entry.1 = v.clone();
                } else {
                    merged.push((k.clone(), v.clone()));
                }
            }
            CqlValue::Map(merged)
        }
        (None, _) => new_val.clone(),
        _ => new_val.clone(),
    }
}

/// Subtract elements from a collection (for `col = col - val` on list/set).
///
/// - List: removes all occurrences of each element in `to_remove`.
/// - Set: removes matching elements.
/// - Non-collection: returns the existing value or Null.
fn collection_sub(existing: Option<&CqlValue>, to_remove: &CqlValue) -> CqlValue {
    match (existing, to_remove) {
        (Some(CqlValue::List(old)), CqlValue::List(remove)) => {
            let filtered: Vec<CqlValue> = old
                .iter()
                .filter(|item| !remove.contains(item))
                .cloned()
                .collect();
            CqlValue::List(filtered)
        }
        (Some(CqlValue::Set(old)), CqlValue::Set(remove)) => {
            let filtered: Vec<CqlValue> = old
                .iter()
                .filter(|item| !remove.contains(item))
                .cloned()
                .collect();
            CqlValue::Set(filtered)
        }
        (Some(existing_val), _) => existing_val.clone(),
        (None, _) => CqlValue::Null,
    }
}

/// Remove map entries by key (for `col = col - {key_set}` on maps).
///
/// The `keys_to_remove` is a Set of keys. All map entries whose key appears
/// in the set are removed.
fn collection_sub_map(existing: Option<&CqlValue>, keys_to_remove: &CqlValue) -> CqlValue {
    match (existing, keys_to_remove) {
        (Some(CqlValue::Map(old)), CqlValue::Set(remove_keys)) => {
            let filtered: Vec<(CqlValue, CqlValue)> = old
                .iter()
                .filter(|(k, _)| !remove_keys.contains(k))
                .cloned()
                .collect();
            CqlValue::Map(filtered)
        }
        (Some(existing_val), _) => existing_val.clone(),
        (None, _) => CqlValue::Null,
    }
}

// ── DELETE ────────────────────────────────────────────────────────────────

async fn route_delete(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DeleteStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = match using_timestamp_as_i64(&s.using_timestamp)? {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    // Extract PK values from WHERE
    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Extract CK values from WHERE
    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    // Build delete column indices (empty = row-level delete).
    // MapElement targets are treated as whole-column deletes for now —
    // the storage layer tombstones the column, which is safe (if broader
    // than strictly necessary).
    let delete_columns: Vec<u16> = s
        .columns
        .iter()
        .map(|target| {
            let col_name = target.column_name();
            table_meta.storage_column_index(col_name).ok_or_else(|| {
                CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_delete_row(&delete_columns, &ck_values, timestamp);
    let table_id = TableId::new(ks, &s.table);
    let strategy = keyspace_strategy(&state.schema, ks);

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            &strategy,
        )
        .await?;
    Ok(result::encode_void())
}

// ── BATCH ────────────────────────────────────────────────────────────────

async fn route_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    // Security mitigation M12: batch size limit
    let max_batch = max_batch_statements();
    if b.statements.len() > max_batch {
        return Err(CqlError::Invalid(format!(
            "batch too large: {} statements (max {})",
            b.statements.len(),
            max_batch
        )));
    }

    match b.batch_type {
        BatchType::Logged => route_logged_batch(state, ctx, b).await,
        BatchType::Unlogged | BatchType::Counter => route_unlogged_batch(state, ctx, b).await,
    }
}

/// Route an UNLOGGED or COUNTER batch: dispatch each statement individually.
async fn route_unlogged_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    for stmt in b.statements {
        match stmt {
            Statement::Insert(s) => {
                route_insert(state, ctx, s).await?;
            }
            Statement::Update(s) => {
                route_update(state, ctx, s).await?;
            }
            Statement::Delete(s) => {
                route_delete(state, ctx, s).await?;
            }
            _ => {
                return Err(CqlError::Invalid(
                    "batch may only contain INSERT, UPDATE, or DELETE statements".into(),
                ));
            }
        }
    }
    Ok(result::encode_void())
}

/// Route a LOGGED batch. Behavior depends on the active write path:
///
/// - **Direct (single-node)**: Writes all mutations as a single atomic
///   commit log group via `StorageEngine::write_atomic_batch()`. No batchlog
///   needed -- the commit log provides crash recovery.
///
/// - **Cluster**: Delegates to `coordinate_logged_batch()` for the full
///   3-phase batchlog protocol.
async fn route_logged_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    use ferrosa_storage::Mutation;

    let batch_timestamp = using_timestamp_as_i64(&b.using_timestamp)?.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    });

    let mut mutations = Vec::with_capacity(b.statements.len());

    for stmt in &b.statements {
        match stmt {
            Statement::Insert(s) => {
                let (table_id, key, row, ts) = materialize_insert(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key,
                    vec![row],
                    ts,
                ));
            }
            Statement::Update(s) => {
                let (table_id, key, row, ts) = materialize_update(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key,
                    vec![row],
                    ts,
                ));
            }
            Statement::Delete(s) => {
                let (table_id, key, row, ts) = materialize_delete(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key,
                    vec![row],
                    ts,
                ));
            }
            _ => {
                return Err(CqlError::Invalid(
                    "batch may only contain INSERT, UPDATE, or DELETE statements".into(),
                ));
            }
        }
    }

    // Determine RF from first mutation's keyspace (batches typically target one keyspace).
    let rf = if let Some(first) = mutations.first() {
        keyspace_rf(&state.schema, &first.keyspace)
    } else {
        1
    };

    state
        .write_path
        .load()
        .write_batch(mutations, ctx.consistency, rf)
        .await
        .map_err(CqlError::from)?;

    Ok(result::encode_void())
}

/// Materialize an INSERT statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_insert(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &InsertStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = using_timestamp_as_i64(&s.using_timestamp)?.unwrap_or(batch_timestamp);

    let mut pk_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut ck_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();

    for (i, col_name) in s.columns.iter().enumerate() {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
        let value = bridge::term_to_cql_value(&s.values[i], &cql_type)?;

        match col_meta.kind {
            ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
            ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
            ColumnKind::Regular | ColumnKind::Static => {
                let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
                    CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
                })?;
                regular_cells.push((col_idx, value));
            }
        }
    }

    pk_vals.sort_by_key(|(pos, _)| *pos);
    ck_vals.sort_by_key(|(pos, _)| *pos);
    let pk_values: Vec<CqlValue> = pk_vals.into_iter().map(|(_, v)| v).collect();
    let ck_values: Vec<CqlValue> = ck_vals.into_iter().map(|(_, v)| v).collect();
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(
        &regular_cells,
        &ck_values,
        timestamp,
        using_ttl_as_i32(&s.using_ttl)?,
    );
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

/// Materialize an UPDATE statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_update(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &UpdateStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = using_timestamp_as_i64(&s.using_timestamp)?.unwrap_or(batch_timestamp);

    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for assignment in &s.assignments {
        let (col_name, value) = match assignment {
            Assignment::Simple { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(value, &cql_type)?;
                (column.as_str(), val)
            }
            _ => {
                // For complex assignments (Add, Sub, Element), fall back to simple value
                // extraction. Full handling deferred to route_update.
                continue;
            }
        };
        let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
            CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
        })?;
        regular_cells.push((col_idx, value));
    }

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(
        &regular_cells,
        &ck_values,
        timestamp,
        using_ttl_as_i32(&s.using_ttl)?,
    );
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

/// Materialize a DELETE statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_delete(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &DeleteStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = using_timestamp_as_i64(&s.using_timestamp)?.unwrap_or(batch_timestamp);

    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let delete_columns: Vec<u16> = s
        .columns
        .iter()
        .map(|target| {
            let col_name = target.column_name();
            table_meta.storage_column_index(col_name).ok_or_else(|| {
                CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_delete_row(&delete_columns, &ck_values, timestamp);
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

// ── DDL: Keyspace ────────────────────────────────────────────────────────

async fn route_create_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Create, &Resource::AllKeyspaces)?;

    // IF NOT EXISTS: silently succeed when keyspace already exists
    if s.if_not_exists && state.schema.snapshot().keyspaces.contains_key(&s.name) {
        return Ok(result::encode_schema_change(
            "CREATED",
            "KEYSPACE",
            &[&s.name],
        ));
    }

    let mut options = std::collections::HashMap::new();
    let mut strategy = String::new();
    for (k, v) in &s.replication {
        if k == "class" {
            strategy = v.clone();
        } else {
            options.insert(k.clone(), v.clone());
        }
    }

    // Validate NTS datacenter names against cluster nodes.
    if strategy.contains("NetworkTopology") {
        let local_dc = &state.node_config.data_center;
        for dc_name in options.keys() {
            if dc_name == local_dc {
                continue;
            }
            // Check if any peer reports this DC (best-effort — we may not
            // have full cluster state in standalone/pair mode).
            tracing::warn!(
                dc = dc_name,
                local_dc,
                keyspace = %s.name,
                "NTS datacenter '{}' does not match local node datacenter '{}'",
                dc_name,
                local_dc,
            );
        }
    }

    let ks_meta = KeyspaceMetadata {
        name: s.name.clone(),
        durable_writes: s.durable_writes.unwrap_or(true),
        replication: ReplicationParams { strategy, options },
    };

    // Validate the keyspace definition at the PROPOSER, before the DDL enters
    // the Raft log. The state-machine apply path inserts keyspaces and writes
    // system tables directly from the committed op and must not reject (that
    // would diverge replicas), so an invalid replication map — e.g. transient
    // replication ('3/1'), which strict CQL drivers cannot parse during schema
    // agreement — has to be rejected here, before replication, on every DdlPath.
    ferrosa_schema::validation::validate_keyspace(&ks_meta)?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.create_keyspace(ks_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateKeyspace(ks_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateKeyspace(ks_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "KEYSPACE",
        &[&s.name],
    ))
}

async fn route_alter_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Keyspace(s.name.clone()),
    )?;

    let replication = s.replication.map(|pairs| {
        let mut options = std::collections::HashMap::new();
        let mut strategy = String::new();
        for (k, v) in &pairs {
            if k == "class" {
                strategy = v.clone();
            } else {
                options.insert(k.clone(), v.clone());
            }
        }
        ReplicationParams { strategy, options }
    });

    let updates = KeyspaceUpdates {
        replication,
        durable_writes: s.durable_writes,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.alter_keyspace(&s.name, updates, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::AlterKeyspace {
                name: s.name.clone(),
                updates,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::AlterKeyspace {
                name: s.name.clone(),
                updates,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "KEYSPACE",
        &[&s.name],
    ))
}

async fn route_drop_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Drop,
        &Resource::Keyspace(s.name.clone()),
    )?;

    // IF EXISTS: silently succeed when keyspace doesn't exist
    if s.if_exists && !state.schema.snapshot().keyspaces.contains_key(&s.name) {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "KEYSPACE",
            &[&s.name],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Collect tables before drop so we can unregister from storage.
            let table_ids: Vec<_> = state
                .schema
                .snapshot()
                .tables
                .keys()
                .filter(|(ks, _)| ks == &s.name)
                .map(|(ks, tbl)| ferrosa_storage::TableId::new(ks, tbl))
                .collect();
            state.schema.drop_keyspace(&s.name, ctx.auth)?;
            for tid in &table_ids {
                let _ = state.engine.unregister_table(tid);
            }
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropKeyspace(s.name.clone());
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropKeyspace(s.name.clone());
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "KEYSPACE",
        &[&s.name],
    ))
}

// ── DDL: Table ───────────────────────────────────────────────────────────

async fn route_create_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Create,
        &Resource::Keyspace(ks.to_string()),
    )?;

    // IF NOT EXISTS: silently succeed when table already exists
    if s.if_not_exists
        && state
            .schema
            .snapshot()
            .tables
            .contains_key(&(ks.to_string(), s.name.clone()))
    {
        return Ok(result::encode_schema_change(
            "CREATED",
            "TABLE",
            &[ks, &s.name],
        ));
    }

    // Build column metadata
    let mut columns = IndexMap::new();
    let pk_set: HashSet<&str> = s.partition_key.iter().map(|s| s.as_str()).collect();
    let ck_map: std::collections::HashMap<&str, (i32, crate::ast::ClusteringOrder)> = s
        .clustering_key
        .iter()
        .enumerate()
        .map(|(i, (name, order))| (name.as_str(), (i as i32, *order)))
        .collect();

    let mut pk_position = 0i32;
    for (col_name, type_name) in &s.columns {
        let type_str = cql_type_name_to_string(type_name);
        let (kind, position, clustering_order) = if pk_set.contains(col_name.as_str()) {
            let pos = pk_position;
            pk_position += 1;
            (ColumnKind::PartitionKey, pos, SchemaClusteringOrder::None)
        } else if let Some((ck_pos, ck_order)) = ck_map.get(col_name.as_str()) {
            let schema_order = match ck_order {
                crate::ast::ClusteringOrder::Asc => SchemaClusteringOrder::Asc,
                crate::ast::ClusteringOrder::Desc => SchemaClusteringOrder::Desc,
            };
            (ColumnKind::Clustering, *ck_pos, schema_order)
        } else {
            (ColumnKind::Regular, 0, SchemaClusteringOrder::None)
        };

        columns.insert(
            col_name.clone(),
            ColumnMetadata {
                name: col_name.clone(),
                kind,
                position,
                column_type: type_str,
                clustering_order,
                mask: None,
            },
        );
    }

    let table_meta = TableMetadata {
        keyspace: ks.to_string(),
        name: s.name.clone(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: s.partition_key.clone(),
        clustering_key: s
            .clustering_key
            .iter()
            .map(|(name, order)| {
                let schema_order = match order {
                    crate::ast::ClusteringOrder::Asc => SchemaClusteringOrder::Asc,
                    crate::ast::ClusteringOrder::Desc => SchemaClusteringOrder::Desc,
                };
                (name.clone(), schema_order)
            })
            .collect(),
        params: parse_table_params(&s.table_options),
        flags: HashSet::new(),
        extensions: s
            .extensions
            .as_ref()
            .map(|pairs| pairs.iter().cloned().collect())
            .unwrap_or_default(),
        is_system: false,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Register with schema
            state.schema.create_table(table_meta.clone(), ctx.auth)?;

            // Register with storage engine
            let storage_schema = table_meta.to_storage_schema();
            state.engine.register_table(storage_schema)?;

            // Auto-create cascade tables if consolidation.cascade is enabled.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateTable(Box::new(table_meta.clone()));
            coordinator.coordinate_ddl(op).await?;

            // Auto-create cascade tables after primary table is created.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateTable(Box::new(table_meta.clone()));
            ddl.execute(op).await.map_err(CqlError::from)?;

            // Auto-create cascade tables after primary table is created.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "TABLE",
        &[ks, &s.name],
    ))
}

/// If the table has `consolidation.cascade = true` in its extensions,
/// auto-create the downstream cascade tables (e.g., sensor_5m, sensor_15m, sensor_1h).
fn create_cascade_tables_if_needed(
    state: &SharedState,
    source: &TableMetadata,
) -> Result<(), CqlError> {
    use ferrosa_storage::timeseries::config::{generate_cascade_chain, ConsolidationConfig};

    let config = match ConsolidationConfig::from_extensions(&source.extensions) {
        Some(Ok(c)) if c.cascade => c,
        Some(Err(e)) => {
            tracing::warn!(
                table = %source.name,
                "invalid consolidation extensions: {e}"
            );
            return Ok(());
        }
        _ => return Ok(()), // no consolidation or cascade disabled
    };

    let chain = generate_cascade_chain(&source.name, &config);
    if chain.is_empty() {
        return Ok(());
    }

    tracing::info!(
        table = %source.name,
        cascade_len = chain.len(),
        "auto-creating cascade tables"
    );

    for spec in &chain {
        // Build columns: same partition key + clustering key as source,
        // plus output columns (e.g., value_min, value_max, value_avg, value_stddev).
        let mut columns = IndexMap::new();

        // Copy partition key columns from source.
        for pk_name in &source.partition_key {
            if let Some(col) = source.columns.get(pk_name) {
                columns.insert(pk_name.clone(), col.clone());
            }
        }

        // Clustering key: `ts timestamp` (DESC).
        let ck_name = source
            .clustering_key
            .first()
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| "ts".to_string());

        if !columns.contains_key(&ck_name) {
            columns.insert(
                ck_name.clone(),
                ColumnMetadata {
                    name: ck_name.clone(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                    column_type: "timestamp".to_string(),
                    clustering_order: SchemaClusteringOrder::Desc,
                    mask: None,
                },
            );
        }

        // Add output columns (value_min, value_max, value_avg, etc.).
        for (i, col_name) in spec.output_columns.iter().enumerate() {
            columns.insert(
                col_name.clone(),
                ColumnMetadata {
                    name: col_name.clone(),
                    kind: ColumnKind::Regular,
                    position: i as i32,
                    column_type: "double".to_string(),
                    clustering_order: SchemaClusteringOrder::None,
                    mask: None,
                },
            );
        }

        // Build extensions for this tier's consolidation config.
        let mut extensions = HashMap::new();
        if let Some(ref target) = spec.target {
            extensions.insert("consolidation.target".to_string(), target.clone());
            extensions.insert(
                "consolidation.interval".to_string(),
                format_consolidation_interval(&spec.interval),
            );
            // Inherit functions and columns from source config.
            let fn_str: String = config
                .functions
                .iter()
                .filter_map(consolidation_fn_name)
                .collect::<Vec<_>>()
                .join(",");
            extensions.insert("consolidation.functions".to_string(), fn_str);
            extensions.insert(
                "consolidation.columns".to_string(),
                spec.output_columns
                    .iter()
                    .filter_map(|c| c.rsplit_once('_').map(|(base, _)| base.to_string()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        let cascade_table = TableMetadata {
            keyspace: source.keyspace.clone(),
            name: spec.table_name.clone(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: source.partition_key.clone(),
            clustering_key: vec![(ck_name.clone(), SchemaClusteringOrder::Desc)],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        };

        // Idempotent: silently succeeds if table already exists.
        state
            .schema
            .create_table_internal(cascade_table.clone())
            .map_err(|e| CqlError::ServerError(format!("cascade table creation failed: {e}")))?;

        let storage_schema = cascade_table.to_storage_schema();
        state.engine.register_table(storage_schema).map_err(|e| {
            CqlError::ServerError(format!(
                "cascade table storage registration failed for {}.{}: {e}",
                source.keyspace, spec.table_name
            ))
        })?;

        tracing::info!(
            cascade_table = %spec.table_name,
            "created cascade table"
        );
    }

    Ok(())
}

/// Format a Duration as a consolidation interval string (e.g., "5m", "15m", "1h").
fn format_consolidation_interval(d: &std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

/// Map a ConsolidationFn to its string name for extension serialization.
fn consolidation_fn_name(
    f: &ferrosa_storage::timeseries::consolidation::ConsolidationFn,
) -> Option<String> {
    f.extension_name()
}

async fn route_alter_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // RENAME and ALTER ... TYPE parse, but applying them to live data is not yet
    // implemented. Reject loudly rather than silently building empty updates and
    // reporting success — a no-op "success" would be worse than a clear error.
    if !s.rename_columns.is_empty() {
        return Err(CqlError::Invalid(
            "ALTER TABLE ... RENAME is not yet supported".to_string(),
        ));
    }
    if !s.alter_column_types.is_empty() {
        return Err(CqlError::Invalid(
            "ALTER TABLE ... ALTER <column> TYPE is not yet supported".to_string(),
        ));
    }

    let add_columns: Vec<ColumnMetadata> = s
        .add_columns
        .iter()
        .map(|(name, type_name)| ColumnMetadata {
            name: name.clone(),
            kind: ColumnKind::Regular,
            position: 0,
            column_type: cql_type_name_to_string(type_name),
            clustering_order: SchemaClusteringOrder::None,
            mask: None,
        })
        .collect();

    let extensions = s.extensions.map(|pairs| pairs.into_iter().collect());

    let updates = TableUpdates {
        params: None,
        add_columns,
        drop_columns: s.drop_columns.clone(),
        extensions,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.alter_table(ks, &s.table, updates, ctx.auth)?;
            // Propagate the post-ALTER column set to the storage engine,
            // mirroring what CreateTable's Direct arm does. Without this,
            // standalone/Direct nodes leave their TableSchema stuck at the
            // CREATE-TABLE column set; every subsequent write produces a
            // cell whose col_idx (computed from the live Schema metadata)
            // exceeds the storage TableSchema's num_columns. The flush
            // path's fail-loud assertion at writer.rs:496 then panics:
            //   "cell col_idx N is out of range (num_columns=M)"
            // and the row is silently dropped. The cluster-mode path
            // already calls engine.update_table_schema via the RaftOp
            // AlterTable apply (state_machine.rs:995) — the Direct
            // path was simply missing the equivalent local update.
            let snap = state.schema.snapshot();
            if let Some(tbl) = snap.tables.get(&(ks.to_string(), s.table.clone())) {
                let tid = ferrosa_storage::TableId::new(ks, &s.table);
                if let Err(e) = state
                    .engine
                    .update_table_schema(&tid, tbl.to_storage_schema())
                {
                    tracing::error!(
                        %e,
                        keyspace = %ks,
                        table = %s.table,
                        "Direct ALTER: engine.update_table_schema failed — future flushes may panic on stale column count"
                    );
                }
            }
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::AlterTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
                updates: Box::new(updates),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::AlterTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
                updates: Box::new(updates),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "TABLE",
        &[ks, &s.table],
    ))
}

async fn route_drop_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Drop,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // IF EXISTS: silently succeed when table doesn't exist
    if s.if_exists
        && !state
            .schema
            .snapshot()
            .tables
            .contains_key(&(ks.to_string(), s.table.clone()))
    {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "TABLE",
            &[ks, &s.table],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.drop_table(ks, &s.table, ctx.auth)?;
            let tid = ferrosa_storage::TableId::new(ks, &s.table);
            let _ = state.engine.unregister_table(&tid);
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "TABLE",
        &[ks, &s.table],
    ))
}

// ── DDL: Index ───────────────────────────────────────────────────────────

/// Parse a Filtered (partial) index's `filter_op` option string into a
/// [`ferrosa_index::FilterOp`]. Fails loud on any unrecognized operator.
fn parse_filter_op(op: &str) -> Result<ferrosa_index::FilterOp, CqlError> {
    use ferrosa_index::FilterOp;
    match op.trim() {
        "=" => Ok(FilterOp::Eq),
        "!=" => Ok(FilterOp::NotEq),
        "<" => Ok(FilterOp::Lt),
        ">" => Ok(FilterOp::Gt),
        "<=" => Ok(FilterOp::LtEq),
        ">=" => Ok(FilterOp::GtEq),
        other => Err(CqlError::Invalid(format!(
            "invalid filter_op '{other}' for filtered index (expected one of =, !=, <, >, <=, >=)"
        ))),
    }
}

/// Coerce a filtered-index `filter_value` (always a string in the WITH OPTIONS
/// map) into the [`Term`] shape the column's [`CqlType`] expects.
///
/// Text-like columns keep the value verbatim as a string literal. Numeric and
/// boolean columns parse the string into the matching literal so that, e.g.,
/// `filter_value':'21'` on an `int` column produces an integer literal rather
/// than failing the type check that rejects a string literal on an int column.
/// Parsing failures fail loud — a partial index with an unparseable predicate
/// value must be rejected at CREATE time, not silently registered.
fn filter_value_to_term(value: &str, cql_type: &CqlType) -> Result<Term, CqlError> {
    match cql_type {
        CqlType::Int
        | CqlType::Bigint
        | CqlType::Smallint
        | CqlType::Tinyint
        | CqlType::Varint
        | CqlType::Counter
        | CqlType::Timestamp => {
            let n = value.parse::<i64>().map_err(|_| {
                CqlError::Invalid(format!(
                    "filtered index filter_value '{value}' is not a valid integer for the \
                     filter column type"
                ))
            })?;
            Ok(Term::IntegerLiteral(n))
        }
        CqlType::Float | CqlType::Double | CqlType::Decimal => {
            let f = value.parse::<f64>().map_err(|_| {
                CqlError::Invalid(format!(
                    "filtered index filter_value '{value}' is not a valid number for the \
                     filter column type"
                ))
            })?;
            Ok(Term::FloatLiteral(f))
        }
        CqlType::Boolean => {
            let b = value.parse::<bool>().map_err(|_| {
                CqlError::Invalid(format!(
                    "filtered index filter_value '{value}' is not a valid boolean (true/false)"
                ))
            })?;
            Ok(Term::BoolLiteral(b))
        }
        // Text, inet, timestamps-as-strings, uuid, blob, dates, etc. all accept
        // a string literal and parse it in `term_to_cql_value`.
        _ => Ok(Term::StringLiteral(value.to_string())),
    }
}

/// Build one fully-encoded [`ferrosa_index::FilterClause`] from a
/// `(column, op, value)` triple, resolving the column's **storage** ordinal and
/// encoding the value to storage bytes. Every failure is loud — a partial index
/// with a broken clause would silently index the wrong rows.
fn build_filter_clause(
    state: &SharedState,
    ks: &str,
    table: &str,
    column: &str,
    op_str: &str,
    value_str: &str,
) -> Result<ferrosa_index::FilterClause, CqlError> {
    let op = parse_filter_op(op_str)?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), table.to_string()))
        .ok_or_else(|| CqlError::Invalid(format!("table {ks}.{table} not found")))?;

    // Storage ordinal of the filter column: the cell tag the build/memtable
    // paths compare the clause against.
    let column_position = table_meta.storage_column_index(column).ok_or_else(|| {
        CqlError::Invalid(format!(
            "filtered index filter column '{column}' is not a column of {ks}.{table}"
        ))
    })? as usize;

    let col_meta = table_meta
        .columns
        .get(column)
        .ok_or_else(|| CqlError::Invalid(format!("filter column '{column}' not found")))?;
    let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
    // The WITH OPTIONS map always carries the filter value as a string. Coerce
    // it to the term shape the column's type expects (e.g. `'21'` on an `int`
    // column becomes an integer literal) before encoding — otherwise a numeric
    // partial predicate would be rejected as a type mismatch at CREATE time.
    let term = filter_value_to_term(value_str, &cql_type)?;
    let cql_value = bridge::term_to_cql_value(&term, &cql_type)?;
    let value = crate::types::encode_value(&cql_value);

    Ok(ferrosa_index::FilterClause::new(column_position, op, value))
}

/// Build a fully-encoded [`ferrosa_index::FilterPredicate`] (a conjunction of
/// one or more clauses) from the WITH OPTIONS of a
/// `CREATE INDEX ... USING 'filtered'` statement.
///
/// Two accepted forms:
/// - **Single string** `{'filter': "col <op> lit [AND col <op> lit]..."}` — a
///   conjunction parsed from a restricted CQL boolean expression. This is the
///   multi-column form.
/// - **Legacy three-key** `{'filter_column': .., 'filter_op': .., 'filter_value':
///   ..}` — a single clause, preserved for backward compatibility.
///
/// Every missing/invalid option fails loud — a partial index with a broken
/// predicate would silently index the wrong rows, an unsound result.
fn build_filter_predicate_from_options(
    state: &SharedState,
    ks: &str,
    table: &str,
    options: &HashMap<String, String>,
) -> Result<ferrosa_index::FilterPredicate, CqlError> {
    // Preferred form: a single `'filter'` conjunction string.
    if let Some(filter_expr) = options.get("filter") {
        let parsed = parse_filter_conjunction(filter_expr)?;
        let clauses = parsed
            .into_iter()
            .map(|(col, op, val)| build_filter_clause(state, ks, table, &col, &op, &val))
            .collect::<Result<Vec<_>, _>>()?;
        if clauses.is_empty() {
            return Err(CqlError::Invalid(
                "filtered index 'filter' expression parsed to zero clauses".into(),
            ));
        }
        return Ok(ferrosa_index::FilterPredicate::conjunction(clauses));
    }

    // Legacy single-clause three-key form.
    let filter_column = options.get("filter_column").ok_or_else(|| {
        CqlError::Invalid(
            "filtered index requires WITH OPTIONS = {'filter': \"col <op> lit AND ...\"} or the \
             legacy {'filter_column': ..., 'filter_op': ..., 'filter_value': ...}; missing both"
                .into(),
        )
    })?;
    let filter_op_str = options.get("filter_op").ok_or_else(|| {
        CqlError::Invalid("filtered index WITH OPTIONS missing 'filter_op'".into())
    })?;
    let filter_value = options.get("filter_value").ok_or_else(|| {
        CqlError::Invalid("filtered index WITH OPTIONS missing 'filter_value'".into())
    })?;

    let clause = build_filter_clause(state, ks, table, filter_column, filter_op_str, filter_value)?;
    Ok(ferrosa_index::FilterPredicate::conjunction(vec![clause]))
}

/// Parse a restricted CQL boolean conjunction `col <op> literal [AND col <op>
/// literal]...` into `(column, op_str, value_str)` triples. The op tokens are
/// the same symbols [`parse_filter_op`] accepts (`=`,`!=`,`<`,`>`,`<=`,`>=`).
/// String literals may be single-quoted (`'eng'`); the quotes are stripped.
/// Any unparseable clause fails loud rather than being silently dropped.
fn parse_filter_conjunction(expr: &str) -> Result<Vec<(String, String, String)>, CqlError> {
    // Split on the word `AND` (case-insensitive), surrounded by whitespace.
    let mut clauses = Vec::new();
    for raw in split_on_and(expr) {
        let clause = raw.trim();
        if clause.is_empty() {
            continue;
        }
        let (column, op, value) = split_clause(clause)?;
        clauses.push((column, op, value));
    }
    if clauses.is_empty() {
        return Err(CqlError::Invalid(format!(
            "filtered index 'filter' expression '{expr}' has no clauses"
        )));
    }
    Ok(clauses)
}

/// Split a conjunction expression on the keyword `AND` (case-insensitive),
/// matched only as a whole whitespace-delimited token so column/value text
/// containing the substring "and" is not split.
fn split_on_and(expr: &str) -> Vec<String> {
    let tokens: Vec<&str> = expr.split_whitespace().collect();
    let mut out = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for tok in tokens {
        if tok.eq_ignore_ascii_case("and") {
            out.push(current.join(" "));
            current.clear();
        } else {
            current.push(tok);
        }
    }
    out.push(current.join(" "));
    out
}

/// Split a single clause `col <op> literal` into its three parts. The operator
/// is matched greedily (two-char ops `<=`/`>=`/`!=` before single-char) so
/// `age>=21` and `age >= 21` both parse. The literal has surrounding single
/// quotes stripped.
fn split_clause(clause: &str) -> Result<(String, String, String), CqlError> {
    // Two-char operators must be tried before single-char to avoid `>=` parsing
    // as `>`.
    const TWO_CHAR: &[&str] = &["<=", ">=", "!="];
    const ONE_CHAR: &[&str] = &["=", "<", ">"];

    let find_op = |ops: &[&str]| -> Option<(usize, usize)> {
        ops.iter()
            .filter_map(|op| clause.find(op).map(|idx| (idx, op.len())))
            .min_by_key(|(idx, _)| *idx)
    };

    let (op_idx, op_len) = find_op(TWO_CHAR)
        .or_else(|| find_op(ONE_CHAR))
        .ok_or_else(|| {
            CqlError::Invalid(format!(
            "filtered index clause '{clause}' has no comparison operator (expected =,!=,<,>,<=,>=)"
        ))
        })?;

    let column = clause[..op_idx].trim().to_string();
    let op = clause[op_idx..op_idx + op_len].to_string();
    let value_raw = clause[op_idx + op_len..].trim();
    if column.is_empty() || value_raw.is_empty() {
        return Err(CqlError::Invalid(format!(
            "filtered index clause '{clause}' is malformed (expected 'column <op> literal')"
        )));
    }
    // Strip a single pair of surrounding single quotes from string literals.
    let value = value_raw
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(value_raw)
        .to_string();
    Ok((column, op, value))
}

/// Soundness gate for partial (Filtered) indexes.
///
/// A partial index holds ONLY rows whose filter column satisfies its predicate,
/// so serving `WHERE indexed_col = v` from it is COMPLETE only when the query
/// also constrains the filter column such that every qualifying row is
/// guaranteed to be in the index. Using it otherwise drops rows that match
/// `indexed_col = v` but fall outside the partial predicate — a silent
/// correctness bug.
///
/// Sound rule: the query must carry a predicate on the index's filter column
/// whose value-set is a provable SUBSET of the index's retained set (see
/// [`ferrosa_index::query_constraint_implies_predicate`]). This covers both the
/// Eq-implies-Eq case and RANGE implication:
/// - index predicate `status = 'active'`, query `status = 'active'` → usable
/// - index predicate `age > 21`, query `age = 30` / `age > 25` / `age >= 22` → usable
/// - index predicate `age > 21`, query `age > 10` / `age >= 21` → withheld
///   (those queries admit rows the index excludes, so the result would be
///   incomplete)
///
/// A query with no predicate on the filter column, or one whose value-set is
/// not provably contained, does not qualify and the index is withheld (the
/// planner then falls back to FullScan / the usual unindexed-column error).
fn query_implies_filter_predicate(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
    predicate: &ferrosa_index::FilterPredicate,
) -> bool {
    // Conjunction soundness: the index retains rows where EVERY clause holds, so
    // the query may serve from it ONLY when each clause is provably implied by
    // some WHERE constraint on that clause's column. If even one clause is not
    // implied, withhold the index — serving it would silently drop rows.
    let clauses = predicate.clauses();
    if clauses.is_empty() {
        return false;
    }
    clauses
        .iter()
        .all(|clause| clause_implied_by_where(clause, where_clauses, table_meta, ks, schema))
}

/// Is a single filter `clause` provably implied by some WHERE constraint on the
/// clause's own column? Scans the WHERE clauses for a scalar comparison on the
/// clause column whose value-set is a provable subset of the clause's retained
/// set (per [`ferrosa_index::query_constraint_implies_predicate_clause`]).
fn clause_implied_by_where(
    clause: &ferrosa_index::FilterClause,
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> bool {
    for wc in where_clauses {
        if wc.token_fn {
            continue;
        }
        // Map the WHERE op to a filter op; only scalar comparisons can imply a
        // clause (IN/CONTAINS/LIKE/etc. are not handled and withhold).
        let Some(query_op) = comparison_to_filter_op(&wc.op) else {
            continue;
        };
        // Match the WHERE column to the clause's column by storage ordinal — the
        // same ordinal the clause was built with.
        let Some(storage_idx) = table_meta.storage_column_index(&wc.column) else {
            continue;
        };
        if storage_idx as usize != clause.column_position {
            continue;
        }
        // Encode the query's literal the same way the clause value was encoded,
        // then check subset containment in that byte space.
        let Ok(key) = term_to_index_key(&wc.value, &wc.column, table_meta, ks, schema) else {
            continue;
        };
        if ferrosa_index::query_constraint_implies_predicate_clause(query_op, &key.0, clause) {
            return true;
        }
    }
    false
}

/// Map a CQL [`ComparisonOp`] to the index [`ferrosa_index::FilterOp`] used by
/// partial-predicate implication. Only the scalar comparison operators have a
/// filter-op equivalent; multi-value / pattern operators (`IN`, `CONTAINS`,
/// `LIKE`, `SoundsLike`, …) return `None` so the planner withholds the partial
/// index rather than reasoning unsoundly about them.
fn comparison_to_filter_op(op: &ComparisonOp) -> Option<ferrosa_index::FilterOp> {
    use ferrosa_index::FilterOp;
    match op {
        ComparisonOp::Eq => Some(FilterOp::Eq),
        ComparisonOp::Ne => Some(FilterOp::NotEq),
        ComparisonOp::Lt => Some(FilterOp::Lt),
        ComparisonOp::Gt => Some(FilterOp::Gt),
        ComparisonOp::Le => Some(FilterOp::LtEq),
        ComparisonOp::Ge => Some(FilterOp::GtEq),
        ComparisonOp::In
        | ComparisonOp::Contains
        | ComparisonOp::ContainsKey
        | ComparisonOp::SoundsLike
        | ComparisonOp::Like => None,
    }
}

/// The set of filter-column NAMES covered by the given usable filtered
/// indexes. Maps each filtered index predicate's storage `column_position` back
/// to a column name so the planner can mark it covered. Non-filtered indexes
/// and predicates whose ordinal does not resolve are ignored.
fn filtered_index_covered_columns(
    usable_indexes: &[&IndexMetadata],
    table_meta: &TableMetadata,
) -> Vec<String> {
    let mut covered = Vec::new();
    for meta in usable_indexes {
        if meta.index_type != IndexType::Filtered {
            continue;
        }
        let Some(pred) = meta.filter_predicate.as_ref() else {
            continue;
        };
        // Mark EVERY conjunction clause's column covered: the partial index has
        // already enforced all of them, so the planner need not re-filter any.
        for clause in pred.clauses() {
            for (name, _) in table_meta.columns.iter() {
                if table_meta.storage_column_index(name).map(|i| i as usize)
                    == Some(clause.column_position)
                {
                    covered.push(name.clone());
                    break;
                }
            }
        }
    }
    covered
}

/// Whether an index may be offered to the planner for this query.
///
/// Non-partial indexes are always usable. A Filtered index is usable only when
/// the query implies its partial predicate (see
/// [`query_implies_filter_predicate`]); a Filtered index whose predicate
/// somehow failed to persist is treated as NOT usable (fail safe — never serve
/// from a partial index we can't prove is implied).
fn filtered_index_is_usable(
    meta: &IndexMetadata,
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> bool {
    if meta.index_type != IndexType::Filtered {
        return true;
    }
    match &meta.filter_predicate {
        Some(pred) => query_implies_filter_predicate(where_clauses, table_meta, ks, schema, pred),
        None => false,
    }
}

/// Resolve the USING string to an IndexType.
fn resolve_index_type(
    using: Option<&str>,
    _columns: &[String],
    _options: &HashMap<String, String>,
) -> Result<IndexType, CqlError> {
    match using {
        None | Some("btree") => Ok(IndexType::BTree),
        Some("hash") => Ok(IndexType::Hash),
        Some("composite") => Ok(IndexType::Composite),
        Some("phonetic") => Ok(IndexType::Phonetic),
        Some("filtered") => Ok(IndexType::Filtered),
        Some("vector") => Ok(IndexType::Vector),
        Some("fulltext") => Ok(IndexType::FullText),
        Some("geo") => Ok(IndexType::Geo),
        Some(other) => Err(CqlError::Invalid(format!("unknown index type: {other}"))),
    }
}

/// Resolve the `method` index option to a storage [`VectorIndexMethod`].
///
/// Absent or `'hnsw'` selects the full-precision HNSW sidecar (the default);
/// `'hvq'` selects the hybrid vector quantization (quantized IVF / C-SPANN)
/// artifact path. Any other value fails loudly rather than silently falling
/// back to HNSW.
fn resolve_vector_index_method(
    options: &HashMap<String, String>,
) -> Result<ferrosa_storage::VectorIndexMethod, CqlError> {
    use ferrosa_storage::VectorIndexMethod;
    match options.get("method").map(String::as_str) {
        None | Some("hnsw") => Ok(VectorIndexMethod::Hnsw),
        Some("hvq") => Ok(VectorIndexMethod::QuantizedIvf),
        Some(other) => Err(CqlError::Invalid(format!(
            "unknown vector index method '{other}' (expected 'hnsw' or 'hvq')"
        ))),
    }
}

fn vector_dimension_from_column_type(column_type: &str) -> Option<usize> {
    let lower = column_type.trim().to_ascii_lowercase();
    let inner = lower.strip_prefix("vector<")?.strip_suffix('>')?;
    inner
        .rsplit_once(',')
        .and_then(|(_, dim)| dim.trim().parse::<usize>().ok())
}

async fn route_create_index(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateIndexStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8) — ALTER on the table
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // Convert options Vec to HashMap
    let mut options_map: HashMap<String, String> = s.options.iter().cloned().collect();

    // Resolve index type
    let index_type = resolve_index_type(s.using.as_deref(), &s.columns, &options_map)?;

    // For vector indexes, resolve the artifact/search method up front so an
    // unknown `method` option rejects the whole statement before any DDL runs.
    let vector_method = if index_type == IndexType::Vector {
        Some(resolve_vector_index_method(&options_map)?)
    } else {
        None
    };

    // For a Filtered (partial) index, parse and validate the partial predicate
    // from WITH OPTIONS up front so a malformed spec rejects the statement
    // before any DDL runs (fail loud). The fully-encoded predicate is also
    // mirrored into `options` under the reserved `__filter_predicate` key so it
    // survives restart and the storage reload path can reconstruct it without
    // the CQL type system. Done before any DDL so persistence captures it.
    let filter_predicate = if index_type == IndexType::Filtered {
        let pred = build_filter_predicate_from_options(state, ks, &s.table, &options_map)?;
        let json = pred.to_option_string().map_err(|e| {
            CqlError::ServerError(format!("failed to serialize filter predicate: {e}"))
        })?;
        options_map.insert(FILTER_PREDICATE_OPTION_KEY.to_string(), json);
        Some(pred)
    } else {
        None
    };

    // Generate index name if not provided
    let index_name = s
        .name
        .unwrap_or_else(|| format!("{}_{}_idx", s.table, s.columns.join("_")));

    let index_meta = IndexMetadata {
        keyspace: ks.to_string(),
        table: s.table.clone(),
        name: index_name.clone(),
        index_type,
        target_columns: s.columns.clone(),
        filter_predicate: filter_predicate.clone(),
        options: options_map,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Dogfood: persist the system_schema.indexes row so the storage-backed
            // SELECT path returns it. The cluster/pair DDL paths persist this via
            // SystemTableWriter on the leader; standalone Direct mode writes it here.
            persist_index_row_direct(&state.engine, &index_meta);
            state.schema.create_index(index_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateIndex(index_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateIndex(index_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    // Notify the storage engine about the new index so future memtable writes
    // are indexed. We look up the column position from the post-DDL schema
    // snapshot. Only the first target column is wired (composite index support
    // is deferred). If the table is not yet registered with the engine (e.g.,
    // schema-only mode), a warning is logged but the operation does not fail.
    if let Some(target_col) = s.columns.first() {
        let snap = state.schema.snapshot();
        let table_id = TableId::new(ks, &s.table);
        let col_info = snap
            .tables
            .get(&(ks.to_string(), s.table.clone()))
            .and_then(|tbl| {
                // The memtable/SSTable cell position is the *storage* column
                // index (statics then regulars, each sorted by Cassandra's
                // column-name comparator), NOT the declaration order. The write
                // path indexes the cell at `storage_column_index`, so the index
                // must register that exact position or it would read the wrong
                // cell (e.g. on a multi-regular-column table the geo tuple and a
                // text column would swap, decoding to garbage).
                let storage_idx = tbl.storage_column_index(target_col)? as usize;
                let column_type = tbl.columns.get(target_col)?.column_type.clone();
                Some((storage_idx, column_type))
            });

        if let Some((pos, column_type)) = col_info {
            let wire_result = if index_type == IndexType::Vector {
                let dimension =
                    vector_dimension_from_column_type(&column_type).ok_or_else(|| {
                        CqlError::Invalid(format!(
                            "vector index target column '{}' has non-vector type '{}'",
                            target_col, column_type
                        ))
                    })?;
                let method = vector_method.expect("vector index resolves a method above");
                state.engine.add_vector_index_with_method(
                    &table_id,
                    &index_name,
                    pos,
                    dimension,
                    method,
                )
            } else if index_type == IndexType::FullText {
                // Full-text needs its own inverted-index sidecar (built on flush);
                // the generic add_index() only builds a BTree.
                state.engine.add_fulltext_index(&table_id, &index_name, pos)
            } else if index_type == IndexType::Filtered {
                // Partial index: thread the predicate into the engine so the
                // memtable index and the SSTable sidecar build both filter to
                // exactly the matching rows.
                state.engine.add_index_with_predicate(
                    &table_id,
                    &index_name,
                    pos,
                    index_type,
                    filter_predicate.clone(),
                )
            } else {
                state
                    .engine
                    .add_index(&table_id, &index_name, pos, index_type)
            };

            if let Err(e) = wire_result {
                // Log warning but don't fail — index is persisted in schema;
                // it will be populated once the table is registered (e.g., on restart).
                tracing::warn!(
                    %e,
                    index_name,
                    table = %format!("{ks}.{}", s.table),
                    "router: CREATE INDEX failed to wire index to storage engine"
                );
            }
        }
    }

    // CQL native protocol: INDEX schema changes use TABLE as the target
    // so cqlsh refreshes the table metadata (which includes indexes).
    Ok(result::encode_schema_change(
        "CREATED",
        "TABLE",
        &[ks, &s.table],
    ))
}

async fn route_drop_index(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropIndexStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Look up which table owns this index by scanning the schema snapshot
    let snap = state.schema.snapshot();
    let table_name = snap
        .indexes
        .keys()
        .find(|(k, _t, n)| k == ks && n == &s.name)
        .map(|(_, t, _)| t.clone());

    let table_name = match table_name {
        Some(t) => t,
        None => {
            if s.if_exists {
                return Ok(result::encode_schema_change(
                    "DROPPED",
                    "TABLE",
                    &[ks, &s.name],
                ));
            }
            return Err(CqlError::Invalid(format!(
                "index '{}' not found in keyspace '{}'",
                s.name, ks
            )));
        }
    };

    // Permission check (M8) — ALTER on the parent table
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), table_name.clone()),
    )?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state
                .schema
                .drop_index(ks, &table_name, &s.name, ctx.auth)?;
            // Tombstone the dogfooded system_schema.indexes row (cluster/pair
            // paths do this via SystemTableWriter; Direct mode does it here).
            tombstone_index_row_direct(&state.engine, ks, &table_name, &s.name);
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropIndex {
                keyspace: ks.to_string(),
                table: table_name.clone(),
                index: s.name.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropIndex {
                keyspace: ks.to_string(),
                table: table_name.clone(),
                index: s.name.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    // Return TABLE event so cqlsh refreshes table metadata (includes indexes).
    Ok(result::encode_schema_change(
        "DROPPED",
        "TABLE",
        &[ks, &table_name],
    ))
}

// ── DDL: Role ────────────────────────────────────────────────────────────

async fn route_create_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Create, &Resource::AllRoles)?;

    // Security (threat-model T9): ferrosa has no network authorizer, so reject
    // ACCESS TO/FROM clauses rather than silently accept an unenforced access
    // restriction (which would give a false sense of security). Fail loud.
    if !s.access.is_empty() {
        return Err(CqlError::Invalid(
            "ACCESS (network authorization) is not supported".into(),
        ));
    }

    let base_role = RoleMetadata {
        name: s.name.clone(),
        is_superuser: s.superuser.unwrap_or(false),
        can_login: s.login.unwrap_or(false),
        salted_hash: None,
        member_of: HashSet::new(),
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Direct path: `Schema::create_role` hashes the cleartext under the
            // write-lock; `create_role_hashed` validates and stores a supplied
            // HASHED PASSWORD verbatim. Both emit the audit event.
            if let Some(ref h) = s.hashed_password {
                state.schema.create_role_hashed(base_role, h, ctx.auth)?;
            } else {
                state
                    .schema
                    .create_role(base_role, s.password.as_deref(), ctx.auth)?;
            }
        }
        DdlPath::Pair(_) | DdlPath::Cluster { .. } => {
            // Pair/Cluster paths: serialise `DdlOperation::CreateRole(role)`
            // over the wire and apply via `create_role_internal(role)`,
            // which writes the role verbatim. We MUST hash the password
            // on the coordinator and embed the hash in the role —
            // otherwise the role persists with `salted_hash = None` and
            // login returns `Bad credentials` for an apparently-existing
            // role (the pre-fix bug).
            //
            // Hashing on the coordinator also keeps the cleartext
            // password off the wire and out of the Raft log.
            let mut role = base_role;
            if let Some(ref h) = s.hashed_password {
                // HASHED PASSWORD: validate on the coordinator, ship verbatim.
                state.schema.validate_password_hash(h)?;
                role.salted_hash = Some(h.clone());
            } else if let Some(ref pw) = s.password {
                state.schema.password_policy().validate(pw, &s.name)?;
                role.salted_hash = Some(state.schema.password_hasher().hash_password(pw)?);
            }
            match ddl {
                DdlPath::Pair(coordinator) => {
                    coordinator
                        .coordinate_ddl(DdlOperation::CreateRole(role))
                        .await?;
                }
                DdlPath::Cluster { .. } => {
                    ddl.execute(DdlOperation::CreateRole(role))
                        .await
                        .map_err(CqlError::from)?;
                }
                _ => unreachable!("matched in outer match"),
            }
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_alter_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Role(s.name.clone()))?;

    // Security (threat-model T9): reject unenforced ACCESS clauses. Fail loud.
    if !s.access.is_empty() {
        return Err(CqlError::Invalid(
            "ACCESS (network authorization) is not supported".into(),
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Direct path: `Schema::alter_role` hashes plaintext under the lock,
            // or validates+stores a HASHED PASSWORD verbatim; emits the audit event.
            let updates = RoleUpdates {
                is_superuser: s.superuser,
                can_login: s.login,
                password: s.password.clone(),
                hashed_password: s.hashed_password.clone(),
                member_of: None,
            };
            state.schema.alter_role(&s.name, updates, ctx.auth)?;
        }
        DdlPath::Pair(_) | DdlPath::Cluster { .. } => {
            // Pair/Cluster paths: `alter_role_internal` (registry.rs:481)
            // stores `updates.password` directly into `salted_hash`
            // without re-hashing — it expects a pre-hashed value as part
            // of the "replication carries the hash" contract. Pre-fix,
            // the router was sending cleartext into that field, so the
            // role's salted_hash became the literal cleartext password
            // (which then never validated against bcrypt). Hash on the
            // coordinator, then ship the hash via the DDL operation.
            let hashed_password = if let Some(ref pw) = s.password {
                state.schema.password_policy().validate(pw, &s.name)?;
                Some(state.schema.password_hasher().hash_password(pw)?)
            } else {
                None
            };
            // HASHED PASSWORD: validate on the coordinator before replicating.
            if let Some(ref h) = s.hashed_password {
                state.schema.validate_password_hash(h)?;
            }
            let updates = RoleUpdates {
                is_superuser: s.superuser,
                can_login: s.login,
                password: hashed_password,
                hashed_password: s.hashed_password.clone(),
                member_of: None,
            };
            match ddl {
                DdlPath::Pair(coordinator) => {
                    coordinator
                        .coordinate_ddl(DdlOperation::AlterRole {
                            name: s.name.clone(),
                            updates,
                        })
                        .await?;
                }
                DdlPath::Cluster { .. } => {
                    ddl.execute(DdlOperation::AlterRole {
                        name: s.name.clone(),
                        updates,
                    })
                    .await
                    .map_err(CqlError::from)?;
                }
                _ => unreachable!("matched in outer match"),
            }
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_drop_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Drop, &Resource::Role(s.name.clone()))?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.drop_role(&s.name, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropRole(s.name.clone());
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropRole(s.name.clone());
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

/// Handle `LIST ROLES [OF role] [NORECURSIVE]`. Mirrors Cassandra's
/// `system_auth.roles` virtual view: returns one row per known role
/// with `role`, `super`, `login`, and `member_of` columns. Cleartext
/// salted_hash is NEVER projected — the login path reads it directly
/// from the schema snapshot. (See SELECT * redaction for how reads
/// against `system_auth.roles` should expose a masked column instead.)
async fn route_list_roles(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    of: Option<String>,
    no_recursive: bool,
    _users_alias: bool,
) -> Result<BytesMut, CqlError> {
    state
        .schema
        .check_permission(ctx.auth, Permission::Describe, &Resource::AllRoles)?;

    let snap = state.schema.snapshot();

    // Build the visible role set. Without `OF`, list everything; with
    // `OF role`, walk that role's `member_of` graph (recursively unless
    // `NORECURSIVE` was specified).
    let mut visible: Vec<&ferrosa_schema::auth::role::RoleMetadata> = if let Some(ref start) = of {
        let mut acc: Vec<&ferrosa_schema::auth::role::RoleMetadata> = Vec::new();
        let mut stack: Vec<String> = vec![start.clone()];
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(name) = stack.pop() {
            if !seen.insert(name.clone()) {
                continue;
            }
            if let Some(role) = snap.roles.get(&name) {
                acc.push(role);
                if !no_recursive {
                    for parent in &role.member_of {
                        stack.push(parent.clone());
                    }
                }
            }
        }
        acc
    } else {
        snap.roles.values().collect()
    };
    visible.sort_by(|a, b| a.name.cmp(&b.name));

    let column_names = vec![
        "role".to_string(),
        "super".to_string(),
        "login".to_string(),
        "member_of".to_string(),
    ];
    let column_types = vec![
        CqlType::Varchar,
        CqlType::Boolean,
        CqlType::Boolean,
        CqlType::Set(Box::new(CqlType::Varchar)),
    ];

    let rows: Vec<Vec<Option<CqlValue>>> = visible
        .into_iter()
        .map(|r| {
            let mut members: Vec<String> = r.member_of.iter().cloned().collect();
            members.sort();
            vec![
                Some(CqlValue::Text(r.name.clone())),
                Some(CqlValue::Boolean(r.is_superuser)),
                Some(CqlValue::Boolean(r.can_login)),
                Some(CqlValue::Set(
                    members.into_iter().map(CqlValue::Text).collect(),
                )),
            ]
        })
        .collect();

    Ok(result::encode_rows(
        &column_names,
        &column_types,
        "system_auth",
        "roles",
        &rows,
    ))
}

/// Handle `LIST [ALL | <perm>] PERMISSIONS [ON <resource>] [OF <role>]
/// [NORECURSIVE]`. Mirrors Cassandra's `system_auth.role_permissions` view —
/// one row per (role, resource, permission). Gated on DESCRIBE over all roles
/// (the same check as LIST ROLES) so a caller cannot enumerate grants it has no
/// authority to see.
async fn route_list_permissions(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    permission: Option<String>,
    resource: Option<GrantResource>,
    of: Option<String>,
    no_recursive: bool,
) -> Result<BytesMut, CqlError> {
    state
        .schema
        .check_permission(ctx.auth, Permission::Describe, &Resource::AllRoles)?;

    let snap = state.schema.snapshot();

    // `OF <role>` restricts to that role and — unless NORECURSIVE — the roles it
    // is a member of, since a role inherits their permissions.
    let role_filter: Option<HashSet<String>> = of.as_ref().map(|start| {
        let mut set = HashSet::new();
        let mut stack = vec![start.clone()];
        while let Some(name) = stack.pop() {
            if !set.insert(name.clone()) {
                continue;
            }
            if !no_recursive {
                if let Some(role) = snap.roles.get(&name) {
                    for parent in &role.member_of {
                        stack.push(parent.clone());
                    }
                }
            }
        }
        set
    });

    // `ON <resource>` compares against the stored resource string form.
    let resource_filter: Option<String> = match resource {
        Some(r) => Some(ast_resource_to_schema(&r, ctx.current_keyspace)?.to_string()),
        None => None,
    };

    let mut perm_rows = ferrosa_schema::query_role_permissions(&snap);
    perm_rows.sort_by(|a, b| {
        a.role
            .cmp(&b.role)
            .then_with(|| a.resource.cmp(&b.resource))
    });

    let column_names = vec![
        "role".to_string(),
        "resource".to_string(),
        "permission".to_string(),
    ];
    let column_types = vec![CqlType::Varchar, CqlType::Varchar, CqlType::Varchar];

    let mut rows: Vec<Vec<Option<CqlValue>>> = Vec::new();
    for row in perm_rows {
        if let Some(ref roles) = role_filter {
            if !roles.contains(&row.role) {
                continue;
            }
        }
        if let Some(ref res) = resource_filter {
            if &row.resource != res {
                continue;
            }
        }
        let mut perms: Vec<String> = row.permissions.into_iter().collect();
        perms.sort();
        for perm in perms {
            if let Some(ref want) = permission {
                if &perm != want {
                    continue;
                }
            }
            rows.push(vec![
                Some(CqlValue::Text(row.role.clone())),
                Some(CqlValue::Text(row.resource.clone())),
                Some(CqlValue::Text(perm)),
            ]);
        }
    }

    Ok(result::encode_rows(
        &column_names,
        &column_types,
        "system_auth",
        "role_permissions",
        &rows,
    ))
}

// ── GRANT / REVOKE ───────────────────────────────────────────────────────

async fn route_grant(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: GrantStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Authorize,
        &ast_resource_to_schema(&s.resource, ctx.current_keyspace)?,
    )?;

    let resource = ast_resource_to_schema(&s.resource, ctx.current_keyspace)?;
    let perms = parse_permissions(&s.permissions)?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.grant(&s.role, &resource, perms, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::Grant(GrantEntry {
                role: s.role.clone(),
                resource,
                permissions: perms,
            });
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::Grant(GrantEntry {
                role: s.role.clone(),
                resource,
                permissions: perms,
            });
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_revoke(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: RevokeStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Authorize,
        &ast_resource_to_schema(&s.resource, ctx.current_keyspace)?,
    )?;

    let resource = ast_resource_to_schema(&s.resource, ctx.current_keyspace)?;
    let perms = parse_permissions(&s.permissions)?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.revoke(&s.role, &resource, perms, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            // DdlOperation::Revoke carries one permission at a time; emit one
            // operation per permission so each can be replicated atomically.
            for perm in perms {
                let op = DdlOperation::Revoke {
                    role: s.role.clone(),
                    resource: resource.clone(),
                    permission: perm,
                };
                coordinator.coordinate_ddl(op).await?;
            }
        }
        DdlPath::Cluster { .. } => {
            // Same as Pair: one Raft proposal per permission for atomic replication.
            for perm in perms {
                let op = DdlOperation::Revoke {
                    role: s.role.clone(),
                    resource: resource.clone(),
                    permission: perm,
                };
                ddl.execute(op).await.map_err(CqlError::from)?;
            }
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

/// `GRANT/REVOKE <role> TO/FROM <member>` — role membership (the member
/// inherits the role's permissions). Replicates via the additive
/// `DdlOperation::GrantRole` / `RevokeRole`, which mutate a single `member_of`
/// edge, so concurrent role grants never clobber one another and cross-grant
/// cycles are caught at apply.
async fn route_grant_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    role: String,
    member: String,
    grant: bool,
) -> Result<BytesMut, CqlError> {
    // Authorization: (re)granting a role requires AUTHORIZE on that role.
    // Superusers pass implicitly through check_permission.
    state.schema.check_permission(
        ctx.auth,
        Permission::Authorize,
        &Resource::Role(role.clone()),
    )?;

    // Both roles must exist — fail loud to the client rather than no-op at apply.
    {
        let snap = state.schema.snapshot();
        if !snap.roles.contains_key(&role) {
            return Err(CqlError::Invalid(format!("role not found: {role}")));
        }
        if !snap.roles.contains_key(&member) {
            return Err(CqlError::Invalid(format!("role not found: {member}")));
        }
    }

    let op = if grant {
        DdlOperation::GrantRole {
            member: member.clone(),
            granted_role: role.clone(),
        }
    } else {
        DdlOperation::RevokeRole {
            member: member.clone(),
            granted_role: role.clone(),
        }
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            if grant {
                state.schema.grant_role_internal(&member, &role)?;
            } else {
                state.schema.revoke_role_internal(&member, &role)?;
            }
        }
        DdlPath::Pair(coordinator) => {
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

// ── TRUNCATE ─────────────────────────────────────────────────────────────

async fn route_truncate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: TruncateStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // Truncate the table's data across all cluster nodes.
    let table_id = ferrosa_storage::TableId::new(ks, &s.table);
    state
        .write_path
        .load()
        .truncate(&table_id)
        .await
        .map_err(|e| CqlError::ServerError(format!("truncate failed: {e}")))?;

    Ok(result::encode_void())
}

async fn route_compact(
    _state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CompactStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;
    Err(CqlError::Invalid(format!(
        "COMPACT is not supported for {ks}.{}; manual SSTable compaction is not implemented",
        s.table
    )))
}

// ── Helper functions ─────────────────────────────────────────────────────

/// Resolve an explicit keyspace or fall back to the session's current keyspace.
/// Parse DDL `table_options` into `TableParams`, populating compaction,
/// compression, comment, and other recognized options from the stringified
/// map values. Unrecognized options are silently ignored.
fn parse_table_params(options: &[(String, String)]) -> TableParams {
    let mut params = TableParams::default();
    for (key, value) in options {
        match key.to_lowercase().as_str() {
            "compaction" => {
                params.compaction = parse_stringified_map(value);
            }
            "compression" => {
                params.compression = parse_stringified_map(value);
            }
            "comment" => {
                params.comment = value.clone();
            }
            "default_time_to_live" => {
                if let Ok(ttl) = value.parse::<i32>() {
                    params.default_time_to_live = ttl;
                }
            }
            "gc_grace_seconds" => {
                if let Ok(gc) = value.parse::<i32>() {
                    params.gc_grace_seconds = gc;
                }
            }
            "bloom_filter_fp_chance" => {
                if let Ok(fp) = value.parse::<f64>() {
                    params.bloom_filter_fp_chance = fp;
                }
            }
            _ => {} // Ignore unrecognized options
        }
    }
    params
}

/// Parse a stringified CQL map literal like `{'key': 'value', 'k2': 'v2'}`
/// back into a HashMap. Handles the format produced by `parse_option_value()`.
fn parse_stringified_map(s: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let trimmed = s.trim().trim_start_matches('{').trim_end_matches('}');
    if trimmed.is_empty() {
        return map;
    }
    for pair in trimmed.split(',') {
        let pair = pair.trim();
        if let Some((k, v)) = pair.split_once(':') {
            let k = k.trim().trim_matches('\'').trim_matches('"').to_string();
            let v = v.trim().trim_matches('\'').trim_matches('"').to_string();
            if !k.is_empty() {
                map.insert(k, v);
            }
        }
    }
    map
}

fn resolve_keyspace<'a>(
    explicit: &'a Option<String>,
    current: &'a Option<String>,
) -> Result<&'a str, CqlError> {
    explicit
        .as_deref()
        .or(current.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))
}

/// Check that a user keyspace exists in the local schema. Returns a clear
/// error immediately instead of timing out when the schema hasn't propagated
/// yet (e.g., during cluster formation).
fn validate_keyspace_exists(schema: &ferrosa_schema::Schema, ks: &str) -> Result<(), CqlError> {
    if ferrosa_schema::is_system_keyspace(ks) {
        return Ok(());
    }
    let snap = schema.snapshot();
    if snap.keyspaces.contains_key(ks) {
        Ok(())
    } else {
        Err(CqlError::Invalid(format!(
            "keyspace '{ks}' not found — schema may still be propagating. Retry in a few seconds."
        )))
    }
}

/// Look up the replication strategy for a keyspace from the schema.
///
/// Falls back to SimpleStrategy RF=1 if the keyspace is not found or parsing fails.
fn keyspace_strategy(
    schema: &Schema,
    ks: &str,
) -> ferrosa_cluster::ring::strategy::ReplicationStrategy {
    use ferrosa_cluster::ring::strategy::ReplicationStrategy;
    let snap = schema.snapshot();
    snap.keyspaces
        .get(ks)
        .and_then(|km| ReplicationStrategy::try_from(&km.replication).ok())
        .unwrap_or(ReplicationStrategy::Simple {
            replication_factor: 1,
        })
}

/// Look up the effective replication factor for a keyspace from the schema.
///
/// For SimpleStrategy, returns `replication_factor`.
/// For NetworkTopologyStrategy, returns the sum of all per-DC replication factors.
/// Falls back to 1 if the keyspace is not found or parsing fails.
fn keyspace_rf(schema: &Schema, ks: &str) -> usize {
    keyspace_strategy(schema, ks).replication_factor()
}

/// Build column names and types for a SELECT result.
fn build_column_info(
    table_meta: &TableMetadata,
    select_columns: &[SelectColumn],
    ks: &str,
    schema: &Schema,
) -> Result<(Vec<String>, Vec<CqlType>), CqlError> {
    // Check for Star
    let has_star = select_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Star));
    if has_star {
        // Return all columns
        let names: Vec<String> = table_meta.columns.keys().cloned().collect();
        let types: Vec<CqlType> = table_meta
            .columns
            .values()
            .map(|c| resolve_col_type(&c.column_type, ks, schema))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((names, types));
    }

    let mut names = Vec::new();
    let mut types = Vec::new();
    for sc in select_columns {
        match sc {
            SelectColumn::Star => unreachable!(),
            SelectColumn::Column(name) => {
                let col = table_meta
                    .columns
                    .get(name)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", name)))?;
                names.push(name.clone());
                types.push(resolve_col_type(&col.column_type, ks, schema)?);
            }
            SelectColumn::FunctionCall {
                keyspace: func_ks,
                name,
                alias,
                args,
            } => {
                let builtin_display_name = alias.clone().unwrap_or_else(|| name.to_lowercase());
                let fn_lower = name.to_lowercase();
                let (display_name, cql_type) = match fn_lower.as_str() {
                    // COUNT(*) column name must be "count" (not "system.count")
                    // to match Cassandra driver expectations. The alias takes
                    // precedence if provided.
                    "count" => {
                        let display = alias.clone().unwrap_or_else(|| "count".to_string());
                        (display, CqlType::Bigint)
                    }
                    "writetime" => (builtin_display_name, CqlType::Bigint),
                    "ttl" => (builtin_display_name, CqlType::Int),
                    "now" | "totimestamp" | "todate" | "currenttimestamp" => {
                        (builtin_display_name, CqlType::Timestamp)
                    }
                    "uuid" | "timeuuid" => (builtin_display_name, CqlType::Uuid),
                    "token" => (builtin_display_name, CqlType::Bigint),
                    "avg" | "min" | "max" | "sum" => {
                        // Resolve the argument column type.
                        let t = if let Some(arg) = args.first() {
                            if let Ok(col_name) = extract_column_name(arg) {
                                let col = table_meta.columns.get(&col_name).ok_or_else(|| {
                                    CqlError::Invalid(format!(
                                        "unknown column in {}(): {}",
                                        fn_lower, col_name
                                    ))
                                })?;
                                let arg_type = resolve_col_type(&col.column_type, ks, schema)?;
                                if fn_lower == "avg" {
                                    // avg always returns Double (matches Cassandra for
                                    // non-decimal types; close enough for now).
                                    match arg_type {
                                        CqlType::Float => CqlType::Float,
                                        _ => CqlType::Double,
                                    }
                                } else {
                                    // min, max, sum return the same type as the column.
                                    arg_type
                                }
                            } else {
                                CqlType::Double
                            }
                        } else {
                            CqlType::Double
                        };
                        (builtin_display_name, t)
                    }
                    _ => {
                        // Try to resolve as UDF/UDA — need table column info.
                        let all_col_names: Vec<String> =
                            table_meta.columns.keys().cloned().collect();
                        let all_col_types: Vec<CqlType> = table_meta
                            .columns
                            .values()
                            .map(|c| resolve_col_type(&c.column_type, ks, schema))
                            .collect::<Result<Vec<_>, _>>()?;
                        let resolved = resolve_select_function(
                            ks,
                            func_ks.as_deref(),
                            name,
                            args,
                            alias.as_deref(),
                            &all_col_names,
                            &all_col_types,
                            schema,
                        )?;
                        // Use the resolved display_name so it matches the
                        // name produced by resolve_select_function during
                        // UDF evaluation (ensures col_names alignment).
                        (resolved.display_name, resolved.return_type.clone())
                    }
                };
                names.push(display_name);
                types.push(cql_type);
            }
        }
    }
    Ok((names, types))
}

/// Extract partition key values from WHERE clauses, in PK column order.
fn extract_pk_values(
    where_clauses: &[WhereClause],
    pk_names: &[String],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> Result<Vec<CqlValue>, CqlError> {
    let mut values = Vec::with_capacity(pk_names.len());
    for pk_name in pk_names {
        let wc = where_clauses
            .iter()
            .find(|w| w.column == *pk_name && w.op == ComparisonOp::Eq)
            .ok_or_else(|| {
                CqlError::Invalid(format!(
                    "missing equality constraint on partition key column: {}",
                    pk_name
                ))
            })?;
        let col_meta = &table_meta.columns[pk_name];
        let cql_type = resolve_col_type(&col_meta.column_type, ks, schema)?;
        let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
        values.push(val);
    }
    Ok(values)
}

/// Extract clustering-key equality values in clustering-column order.
///
/// Returns `Ok(None)` when the statement is not an exact full primary-key
/// lookup. Type conversion errors still propagate because those predicates
/// are present and invalid.
fn extract_clustering_key_values(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> Result<Option<Vec<CqlValue>>, CqlError> {
    if table_meta.clustering_key.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(table_meta.clustering_key.len());
    for (ck_name, _) in &table_meta.clustering_key {
        let Some(wc) = where_clauses
            .iter()
            .find(|w| w.column == *ck_name && w.op == ComparisonOp::Eq)
        else {
            return Ok(None);
        };
        let col_meta = &table_meta.columns[ck_name];
        let cql_type = resolve_col_type(&col_meta.column_type, ks, schema)?;
        values.push(bridge::term_to_cql_value(&wc.value, &cql_type)?);
    }
    Ok(Some(values))
}

fn clustering_key_equality_has_phonetic_index(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    schema: &Schema,
) -> bool {
    let snap = schema.snapshot();
    table_meta.clustering_key.iter().any(|(ck_name, _)| {
        where_clauses
            .iter()
            .any(|wc| wc.column == *ck_name && wc.op == ComparisonOp::Eq)
            && snap.indexes.values().any(|idx| {
                idx.keyspace == table_meta.keyspace
                    && idx.table == table_meta.name
                    && idx.index_type == IndexType::Phonetic
                    && idx.target_columns.contains(ck_name)
            })
    })
}

/// Try to handle `WHERE partition_key IN (v1, v2, ...)` by performing a
/// separate PK lookup for each value in the IN list.
///
/// Returns `Ok(Some(rows))` if the WHERE clause contains an IN predicate on
/// all partition key columns (currently supports single-column PK with IN,
/// or multi-column PK where all but one column are `Eq` and one is `In`).
/// Returns `Ok(None)` if no IN predicate is found on PK columns, letting
/// the caller fall through to the planner.
#[allow(clippy::too_many_arguments)]
fn try_pk_in_lookup(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
    engine: &StorageEngine,
    table_id: &TableId,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    pk_indices: &[usize],
    ck_indices: &[usize],
    storage_to_table: &[usize],
) -> Result<Option<Vec<Vec<Option<CqlValue>>>>, CqlError> {
    let pk_names = &table_meta.partition_key;

    // Find which PK column (if any) has an IN predicate.
    let in_col_idx = pk_names.iter().position(|pk_name| {
        where_clauses
            .iter()
            .any(|w| w.column == *pk_name && w.op == ComparisonOp::In)
    });

    let in_col_idx = match in_col_idx {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // For the IN column, every other PK column must have an Eq predicate.
    for (i, pk_name) in pk_names.iter().enumerate() {
        if i == in_col_idx {
            continue;
        }
        let has_eq = where_clauses
            .iter()
            .any(|w| w.column == *pk_name && w.op == ComparisonOp::Eq);
        if !has_eq {
            return Ok(None);
        }
    }

    // Extract the IN list values.
    let in_wc = where_clauses
        .iter()
        .find(|w| w.column == pk_names[in_col_idx] && w.op == ComparisonOp::In)
        .expect("IN predicate verified above");

    let in_terms = match &in_wc.value {
        Term::InList(terms) => terms,
        _ => {
            return Err(CqlError::Invalid(
                "IN predicate value must be a list".into(),
            ));
        }
    };

    if in_terms.is_empty() {
        return Ok(Some(vec![]));
    }

    // Resolve PK column types.
    let pk_types: Vec<CqlType> = pk_names
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Pre-resolve the Eq values for non-IN PK columns.
    let mut eq_values: Vec<Option<CqlValue>> = vec![None; pk_names.len()];
    for (i, pk_name) in pk_names.iter().enumerate() {
        if i == in_col_idx {
            continue;
        }
        let wc = where_clauses
            .iter()
            .find(|w| w.column == *pk_name && w.op == ComparisonOp::Eq)
            .expect("Eq predicate verified above");
        eq_values[i] = Some(bridge::term_to_cql_value(&wc.value, &pk_types[i])?);
    }

    // Iterate over each value in the IN list, build PK, and read.
    let in_col_type = &pk_types[in_col_idx];
    let mut all_rows = Vec::new();

    for term in in_terms {
        let in_value = bridge::term_to_cql_value(term, in_col_type)?;

        // Assemble the full PK values vector in column order.
        let mut pk_values = Vec::with_capacity(pk_names.len());
        for (i, _) in pk_names.iter().enumerate() {
            if i == in_col_idx {
                pk_values.push(in_value.clone());
            } else {
                pk_values.push(eq_values[i].clone().expect("Eq value resolved above"));
            }
        }

        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        if let Some(partition) = engine.read(table_id, &decorated_key)? {
            let mut prows = bridge::partition_to_rows_with_storage_mapping(
                &partition,
                all_col_names,
                all_col_types,
                pk_indices,
                ck_indices,
                storage_to_table,
            );
            all_rows.append(&mut prows);
        }
    }

    Ok(Some(all_rows))
}

/// Convert a WHERE clause `Term` for a given column into an `IndexKey` for
/// secondary index lookup.
/// Read and intersect the result sets of every matched single-column index.
///
/// Each index is point-looked-up for its `Eq` predicate; the returned
/// partitions are intersected on partition-key identity, so the fetched set is
/// the partitions present in *every* index rather than the (larger) result of a
/// single index. Per-row precision on clustered tables is enforced by the
/// caller's post-filter. Falls back to a full scan only if every index read is
/// empty (the memtable-index-not-yet-wired fallback), preserving correctness.
#[allow(clippy::too_many_arguments)]
async fn read_index_intersection(
    state: &SharedState,
    table_id: &TableId,
    indexes: &[(String, String)],
    s: &SelectStatement,
    table_meta: &TableMetadata,
    ks: &str,
    consistency: ConsistencyLevel,
    table_strategy: &ferrosa_cluster::ring::strategy::ReplicationStrategy,
) -> Result<Vec<ferrosa_sstable::types::Partition>, CqlError> {
    let mut per_index: Vec<Vec<ferrosa_sstable::types::Partition>> =
        Vec::with_capacity(indexes.len());
    for (index_name, index_column) in indexes {
        let index_wc = s
            .where_clauses
            .iter()
            .find(|wc| wc.column == *index_column && wc.op == ComparisonOp::Eq)
            .ok_or_else(|| {
                CqlError::Invalid(
                    "planner selected index but no matching WHERE clause found".into(),
                )
            })?;
        let index_key =
            term_to_index_key(&index_wc.value, index_column, table_meta, ks, &state.schema)?;
        let partitions = state
            .write_path
            .load()
            .index_read(table_id, index_name, &index_key)
            .await?;
        per_index.push(partitions);
    }

    // Memtable-index-not-yet-wired fallback: if every index read came back
    // empty, fall back to a full scan so results stay correct.
    if per_index.iter().all(|p| p.is_empty()) {
        return Ok(state
            .write_path
            .load()
            .range_read_with(table_id, consistency, table_strategy)
            .await?);
    }

    Ok(intersect_partitions_by_key(per_index))
}

/// Intersect partition lists on partition-key bytes, keeping each surviving
/// partition once. A partition survives only if its key appears in every list.
fn intersect_partitions_by_key(
    mut per_index: Vec<Vec<ferrosa_sstable::types::Partition>>,
) -> Vec<ferrosa_sstable::types::Partition> {
    // Order lists smallest-first so the retained set starts as tight as
    // possible, then intersect each subsequent list's key set against it.
    per_index.sort_by_key(|p| p.len());
    let mut iter = per_index.into_iter();
    let mut result = match iter.next() {
        Some(first) => first,
        None => return Vec::new(),
    };
    for partitions in iter {
        let keys: std::collections::HashSet<Vec<u8>> = partitions
            .iter()
            .map(|p| p.key.key.as_bytes().to_vec())
            .collect();
        result.retain(|p| keys.contains(p.key.key.as_bytes()));
    }
    result
}

fn term_to_index_key(
    term: &Term,
    column: &str,
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> Result<ferrosa_index::IndexKey, CqlError> {
    let col_meta = table_meta
        .columns
        .get(column)
        .ok_or_else(|| CqlError::Invalid(format!("column '{column}' not found")))?;
    let cql_type = resolve_col_type(&col_meta.column_type, ks, schema)?;
    let cql_value = bridge::term_to_cql_value(term, &cql_type)?;
    let bytes = crate::types::encode_value(&cql_value);
    Ok(ferrosa_index::IndexKey(bytes))
}

fn evaluate_where_rhs_term(
    term: &Term,
    expected_type: &CqlType,
    row: &[Option<CqlValue>],
    all_col_names: &[String],
    all_col_types: &[CqlType],
    ks: &str,
    state: &SharedState,
) -> Result<CqlValue, CqlError> {
    if let Term::FunctionCall {
        keyspace,
        name,
        args,
    } = term
    {
        if !args.is_empty() && term_has_udf_call(term) {
            let func = resolve_select_function(
                ks,
                keyspace.as_deref(),
                name,
                args,
                None,
                all_col_names,
                all_col_types,
                &state.schema,
            )?;
            if !matches!(func.kind, ResolvedFunctionKind::Scalar) {
                return Err(CqlError::Invalid(format!(
                    "WHERE predicate RHS function {}.{} must be a scalar UDF",
                    func.func_keyspace, func.func_name
                )));
            }
            let mut results = evaluate_row_udfs(state, row, &[&func])?;
            return results
                .pop()
                .flatten()
                .ok_or_else(|| CqlError::Invalid("WHERE predicate RHS UDF returned NULL".into()));
        }
    }

    bridge::term_to_cql_value(term, expected_type)
}

/// Evaluate WHERE predicates against a row for ALLOW FILTERING post-filter.
fn evaluate_where_predicates(
    row: &[Option<CqlValue>],
    where_clauses: &[WhereClause],
    all_col_names: &[String],
    all_col_types: &[CqlType],
    table_meta: &TableMetadata,
    ks: &str,
    state: &SharedState,
) -> Result<bool, CqlError> {
    for wc in where_clauses {
        // Skip token() predicates — token range filtering is handled by
        // the scan bounds, not by post-filter row evaluation.
        if wc.token_fn {
            continue;
        }
        // Skip fts_match() predicates — full-text search is handled by
        // the FTI lookup path, not by post-filter row evaluation.
        // term_to_cql_value cannot convert FunctionCall terms, so leaving
        // these in causes every row to be silently rejected.
        if is_fts_match_term(&wc.value) {
            continue;
        }
        let col_idx = match all_col_names.iter().position(|n| n == &wc.column) {
            Some(i) => i,
            None => return Ok(false),
        };
        let col_meta = match table_meta.columns.get(&wc.column) {
            Some(m) => m,
            None => return Ok(false),
        };
        let cql_type = match resolve_col_type(&col_meta.column_type, ks, &state.schema) {
            Ok(t) => t,
            Err(_) => return Ok(false),
        };
        let actual = match &row[col_idx] {
            Some(v) => v,
            None => return Ok(false),
        };

        // IN requires special handling: the term is an InList, not a single
        // value, so we must check membership before the normal term_to_cql_value
        // path (which rejects InList terms).
        if wc.op == ComparisonOp::In {
            let in_terms = match &wc.value {
                Term::InList(terms) => terms,
                _ => return Ok(false),
            };
            let found = in_terms.iter().any(|t| {
                if let Ok(v) = bridge::term_to_cql_value(t, &cql_type) {
                    *actual == v
                } else {
                    false
                }
            });
            if !found {
                return Ok(false);
            }
            continue;
        }

        // CONTAINS / CONTAINS KEY require element/key type coercion, not
        // collection-level coercion, so they are handled before the normal
        // term_to_cql_value path (same pattern as IN above).
        if wc.op == ComparisonOp::Contains {
            let element_type = match &cql_type {
                CqlType::List(inner) | CqlType::Set(inner) => (**inner).clone(),
                CqlType::Map(_, val_type) => (**val_type).clone(),
                _ => return Ok(false),
            };
            let needle = match bridge::term_to_cql_value(&wc.value, &element_type) {
                Ok(v) => v,
                Err(_) => return Ok(false),
            };
            let found = match actual {
                CqlValue::List(items) | CqlValue::Set(items) => items.contains(&needle),
                CqlValue::Map(entries) => entries.iter().any(|(_, v)| *v == needle),
                _ => false,
            };
            if !found {
                return Ok(false);
            }
            continue;
        }
        if wc.op == ComparisonOp::ContainsKey {
            let key_type = match &cql_type {
                CqlType::Map(key_type, _) => (**key_type).clone(),
                _ => return Ok(false),
            };
            let needle = match bridge::term_to_cql_value(&wc.value, &key_type) {
                Ok(v) => v,
                Err(_) => return Ok(false),
            };
            let found = match actual {
                CqlValue::Map(entries) => entries.iter().any(|(k, _)| *k == needle),
                _ => false,
            };
            if !found {
                return Ok(false);
            }
            continue;
        }

        let expected = evaluate_where_rhs_term(
            &wc.value,
            &cql_type,
            row,
            all_col_names,
            all_col_types,
            ks,
            state,
        )?;

        // Phonetic index support: when the predicate is Eq on a column that
        // has a phonetic index, use case-insensitive comparison as a partial
        // phonetic match. Full Double Metaphone matching requires the SOUNDS
        // LIKE syntax (deferred).
        let has_phonetic_index = if wc.op == ComparisonOp::Eq {
            let snap = state.schema.snapshot();
            snap.indexes.values().any(|idx| {
                idx.keyspace == table_meta.keyspace
                    && idx.table == table_meta.name
                    && idx.index_type == IndexType::Phonetic
                    && idx.target_columns.contains(&wc.column)
            })
        } else {
            false
        };

        let matches = match wc.op {
            ComparisonOp::Eq if has_phonetic_index => {
                // Phonetic matching via simplified Soundex-like comparison.
                // Words that sound similar should match even if spelled differently
                // (e.g., "John Smith" matches "Jon Smyth").
                match (actual, &expected) {
                    (CqlValue::Text(a), CqlValue::Text(b)) => phonetic_match(a, b),
                    _ => *actual == expected,
                }
            }
            ComparisonOp::Eq => *actual == expected,
            ComparisonOp::Ne => *actual != expected,
            ComparisonOp::Gt => *actual > expected,
            ComparisonOp::Lt => *actual < expected,
            ComparisonOp::Ge => *actual >= expected,
            ComparisonOp::Le => *actual <= expected,
            ComparisonOp::SoundsLike => match (actual, &expected) {
                (CqlValue::Text(a), CqlValue::Text(b)) => phonetic_match(a, b),
                _ => false,
            },
            ComparisonOp::Like => match (actual, &expected) {
                (CqlValue::Text(a), CqlValue::Text(b)) => like_match(a, b),
                _ => false,
            },
            ComparisonOp::In => unreachable!("IN handled above"),
            ComparisonOp::Contains | ComparisonOp::ContainsKey => {
                unreachable!("CONTAINS/CONTAINS KEY handled above")
            }
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Cassandra `LIKE` pattern matching. `%` matches any (possibly empty) sequence
/// of characters; every other character — including `_` — matches literally,
/// which is Cassandra's SAI `LIKE` semantics. Matching is case-sensitive, the
/// default for an un-analyzed column.
fn like_match(text: &str, pattern: &str) -> bool {
    let segments: Vec<&str> = pattern.split('%').collect();
    // No wildcard: exact match.
    if segments.len() == 1 {
        return text == pattern;
    }
    let last = segments.len() - 1;
    let mut idx = 0usize;
    for (i, seg) in segments.iter().enumerate() {
        if seg.is_empty() {
            continue;
        }
        if i == 0 {
            // Leading literal must be a prefix.
            if !text[idx..].starts_with(seg) {
                return false;
            }
            idx += seg.len();
        } else if i == last {
            // Trailing literal must be a suffix of what remains.
            if !text[idx..].ends_with(seg) {
                return false;
            }
        } else {
            // Interior literal must occur, in order, at or after the cursor.
            match text[idx..].find(seg) {
                Some(pos) => idx += pos + seg.len(),
                None => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod like_match_tests {
    use super::like_match;

    #[test]
    fn prefix_suffix_contains_and_exact() {
        assert!(like_match("mark", "m%"));
        assert!(like_match("mark", "%k"));
        assert!(like_match("mark", "%ar%"));
        assert!(like_match("mark", "m%k"));
        assert!(like_match("mark", "mark"));
        assert!(like_match("anything", "%"));

        assert!(!like_match("mark", "M%")); // case-sensitive
        assert!(!like_match("mark", "%z"));
        assert!(!like_match("mark", "z%"));
        assert!(!like_match("mark", "mark2"));
        assert!(!like_match("ma", "m%k")); // suffix missing
    }
}

/// Apply column selection (with `toJson()` support) to system table query results.
///
/// System table handlers build the full set of columns, then this function
/// projects down to the columns requested in the `SELECT` list. If the
/// `SELECT` list contains `toJson(col)`, the column value is serialized to
/// a JSON string using [`bridge::cql_value_to_json`].
/// Persist a `system_schema.indexes` row for standalone (Direct) DDL.
///
/// The cluster/pair DDL paths dogfood this via `SystemTableWriter` on the Raft
/// leader; standalone mode has no such path, so the CQL router writes the row
/// directly. A failure here (e.g. the system table is not registered in a
/// schema-only deployment) is logged loudly but does not fail CREATE INDEX —
/// the index is still live in the Registry and is repersisted at next boot.
fn persist_index_row_direct(engine: &StorageEngine, index: &IndexMetadata) {
    let row = ferrosa_schema::system::persistence::index_to_rows(index);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let tid = TableId::new("system_schema", "indexes");
    if let Err(e) = engine.write(&tid, &row.key, row.row, ts) {
        tracing::warn!(
            %e,
            keyspace = %index.keyspace,
            table = %index.table,
            index = %index.name,
            "router: failed to persist system_schema.indexes row (Direct DDL)"
        );
    }
}

/// Tombstone a `system_schema.indexes` row for standalone (Direct) DROP INDEX.
///
/// Mirrors [`persist_index_row_direct`] on the drop side. Failures are logged
/// loudly but do not fail DROP INDEX — the Registry already dropped the index.
fn tombstone_index_row_direct(engine: &StorageEngine, keyspace: &str, table: &str, name: &str) {
    if let Err(e) = engine.write_index_tombstone(keyspace, table, name) {
        tracing::warn!(
            %e,
            keyspace,
            table,
            index = name,
            "router: failed to tombstone system_schema.indexes row (Direct DDL)"
        );
    }
}

/// Map a persisted `system_schema.indexes` kind string (`btree`, `hash`, …)
/// to the Cassandra-faithful kind value (`COMPOSITES`/`CUSTOM`) that CQL
/// drivers expect when introspecting `system_schema.indexes`.
fn cassandra_index_kind(stored_kind: &str) -> &'static str {
    match stored_kind {
        "btree" | "composite" => "COMPOSITES",
        _ => "CUSTOM",
    }
}

/// Apply `SELECT ... WHERE col = '...'` equality predicates to a persisted
/// `system_schema.indexes` row. Non-equality / non-string predicates and
/// unknown columns are ignored (treated as matching), mirroring the prior
/// best-effort virtual-table filter.
fn index_row_matches_where(
    row: &ferrosa_storage::PersistedIndexRow,
    where_clauses: &[crate::ast::WhereClause],
) -> bool {
    where_clauses.iter().all(|wc| {
        if wc.op != crate::ast::ComparisonOp::Eq {
            return true;
        }
        let val = match &wc.value {
            crate::ast::Term::StringLiteral(s) => s.as_str(),
            _ => return true,
        };
        match wc.column.as_str() {
            "keyspace_name" => row.keyspace_name == val,
            "table_name" => row.table_name == val,
            "index_name" => row.index_name == val,
            _ => true,
        }
    })
}

fn apply_system_select(
    select_columns_ast: &[SelectColumn],
    all_col_names: &[String],
    all_col_types: &[CqlType],
    all_rows: &[Vec<Option<CqlValue>>],
    keyspace: &str,
    table: &str,
) -> Result<BytesMut, CqlError> {
    // Star — return everything
    if select_columns_ast
        .iter()
        .any(|c| matches!(c, SelectColumn::Star))
    {
        return Ok(result::encode_rows(
            all_col_names,
            all_col_types,
            keyspace,
            table,
            all_rows,
        ));
    }

    // Build projected column names, types, and transform descriptors.
    let mut proj_names: Vec<String> = Vec::new();
    let mut proj_types: Vec<CqlType> = Vec::new();
    // Each entry: (source column index, apply_tojson)
    let mut proj_ops: Vec<(usize, bool)> = Vec::new();

    for sc in select_columns_ast {
        match sc {
            SelectColumn::Star => unreachable!(),
            SelectColumn::Column(name) => {
                let idx = all_col_names
                    .iter()
                    .position(|n| n == name)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", name)))?;
                proj_names.push(name.clone());
                proj_types.push(all_col_types[idx].clone());
                proj_ops.push((idx, false));
            }
            SelectColumn::FunctionCall {
                name, args, alias, ..
            } => {
                let fn_lower = name.to_lowercase();
                if fn_lower == "tojson" {
                    // toJson(column_ref) — single argument expected
                    if args.len() != 1 {
                        return Err(CqlError::Invalid(
                            "toJson() requires exactly one argument".into(),
                        ));
                    }
                    let col_name = extract_column_name(&args[0])?;
                    let idx = all_col_names
                        .iter()
                        .position(|n| *n == col_name)
                        .ok_or_else(|| {
                            CqlError::Invalid(format!("unknown column: {}", col_name))
                        })?;
                    let display = alias
                        .clone()
                        .unwrap_or_else(|| format!("system.tojson({})", col_name));
                    proj_names.push(display);
                    proj_types.push(CqlType::Varchar); // toJson returns text
                    proj_ops.push((idx, true));
                } else if fn_lower == "count" {
                    let display = alias.clone().unwrap_or_else(|| "count".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Bigint);
                    // COUNT is handled below as aggregate
                    proj_ops.push((usize::MAX, false));
                } else if fn_lower == "now" {
                    let display = alias.clone().unwrap_or_else(|| "system.now()".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Timeuuid);
                    // Sentinel: usize::MAX - 1 for now()
                    proj_ops.push((usize::MAX - 1, false));
                } else if fn_lower == "totimestamp" {
                    let display = alias
                        .clone()
                        .unwrap_or_else(|| "system.totimestamp(system.now())".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Timestamp);
                    // Sentinel: usize::MAX - 2 for toTimestamp(now())
                    proj_ops.push((usize::MAX - 2, false));
                } else {
                    return Err(CqlError::Invalid(format!(
                        "unsupported function in system table query: {}",
                        name
                    )));
                }
            }
        }
    }

    // Check for COUNT(*) aggregate
    let has_count = select_columns_ast.iter().any(|c| {
        matches!(c, SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count"))
    });

    if has_count {
        // Aggregate query -- return a single row
        let mut agg_row: Vec<Option<CqlValue>> = Vec::new();
        for (i, sc) in select_columns_ast.iter().enumerate() {
            if matches!(sc, SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count"))
            {
                agg_row.push(Some(CqlValue::Bigint(all_rows.len() as i64)));
            } else {
                let (src_idx, apply_tojson) = proj_ops[i];
                if src_idx == usize::MAX - 1 {
                    // now()
                    agg_row.push(Some(crate::bridge::eval_now()));
                } else if src_idx == usize::MAX - 2 {
                    // toTimestamp(now())
                    let timeuuid = crate::bridge::eval_now();
                    agg_row.push(eval_to_timestamp(&timeuuid).ok());
                } else if src_idx < all_col_names.len() {
                    let val = all_rows
                        .first()
                        .and_then(|r| r.get(src_idx))
                        .cloned()
                        .flatten();
                    if apply_tojson {
                        let json =
                            bridge::cql_value_to_json(val.as_ref().unwrap_or(&CqlValue::Null));
                        agg_row.push(Some(CqlValue::Text(json)));
                    } else {
                        agg_row.push(val);
                    }
                } else {
                    agg_row.push(None);
                }
            }
        }
        return Ok(result::encode_rows(
            &proj_names,
            &proj_types,
            keyspace,
            table,
            &[agg_row],
        ));
    }

    // Non-aggregate: project each row
    let projected: Vec<Vec<Option<CqlValue>>> = all_rows
        .iter()
        .map(|row| {
            proj_ops
                .iter()
                .map(|&(src_idx, apply_tojson)| {
                    if src_idx == usize::MAX - 1 {
                        // now()
                        Some(crate::bridge::eval_now())
                    } else if src_idx == usize::MAX - 2 {
                        // toTimestamp(now())
                        let timeuuid = crate::bridge::eval_now();
                        eval_to_timestamp(&timeuuid).ok()
                    } else {
                        let val = row.get(src_idx).cloned().flatten();
                        if apply_tojson {
                            let json =
                                bridge::cql_value_to_json(val.as_ref().unwrap_or(&CqlValue::Null));
                            Some(CqlValue::Text(json))
                        } else {
                            val
                        }
                    }
                })
                .collect()
        })
        .collect();

    Ok(result::encode_rows(
        &proj_names,
        &proj_types,
        keyspace,
        table,
        &projected,
    ))
}

/// Extract a column name from a `Term` that should reference a column.
///
/// In the `parse_term` codepath, bare identifiers in function arguments are
/// parsed as `Term::FunctionCall { name, args: [] }` (zero-arg function call).
pub(crate) fn extract_column_name(term: &Term) -> Result<String, CqlError> {
    match term {
        Term::FunctionCall { name, args, .. } if args.is_empty() => Ok(name.to_lowercase()),
        Term::StringLiteral(s) => Ok(s.clone()),
        _ => Err(CqlError::Invalid(format!(
            "expected column reference in toJson(), got {:?}",
            term
        ))),
    }
}

/// Convert a [`CqlValue`] to `f64` for built-in aggregate computation.
///
/// Supports all numeric CQL types. Returns `None` for non-numeric values so
/// that NULL / non-numeric cells are skipped during aggregation.
fn cql_value_to_f64(val: &CqlValue) -> Option<f64> {
    match val {
        CqlValue::Tinyint(v) => Some(f64::from(*v)),
        CqlValue::Smallint(v) => Some(f64::from(*v)),
        CqlValue::Int(v) => Some(f64::from(*v)),
        CqlValue::Bigint(v) => Some(*v as f64),
        CqlValue::Float(bits) => Some(f64::from(f32::from_bits(*bits))),
        CqlValue::Double(bits) => Some(f64::from_bits(*bits)),
        CqlValue::Counter(v) => Some(*v as f64),
        _ => None,
    }
}

/// Build the result [`CqlValue`] for a built-in aggregate (`avg`, `sum`,
/// `min`, `max`) given the source column type.
///
/// `avg` and `sum` on integer types still return `Double` for consistency
/// with Cassandra semantics (CQL `avg(int)` returns `int`, but returning
/// `Double` avoids truncation and matches driver expectations for
/// analytics queries).  If more precise Cassandra-compat semantics are
/// needed later, this can be refined per-type.
fn f64_to_cql_aggregate(val: f64, col_type: &CqlType) -> CqlValue {
    match col_type {
        CqlType::Tinyint => CqlValue::Tinyint(val as i8),
        CqlType::Smallint => CqlValue::Smallint(val as i16),
        CqlType::Int => CqlValue::Int(val as i32),
        CqlType::Bigint | CqlType::Counter => CqlValue::Bigint(val as i64),
        CqlType::Float => CqlValue::Float((val as f32).to_bits()),
        // Default to Double for any other numeric or unknown type.
        _ => CqlValue::Double(val.to_bits()),
    }
}

/// Compute a built-in aggregate (`avg`, `min`, `max`, `sum`) over a column.
///
/// `agg_name` must be one of `"avg"`, `"min"`, `"max"`, `"sum"` (lowercase).
/// The function iterates over `rows`, extracting the value at `col_idx`,
/// converts to `f64`, and applies the aggregate.  NULL cells are skipped.
fn compute_builtin_aggregate(
    agg_name: &str,
    rows: &[Vec<Option<CqlValue>>],
    col_idx: usize,
    col_type: &CqlType,
) -> Option<CqlValue> {
    let values: Vec<f64> = rows
        .iter()
        .filter_map(|row| row.get(col_idx).and_then(|cell| cell.as_ref()))
        .filter_map(cql_value_to_f64)
        .collect();

    if values.is_empty() {
        return Some(CqlValue::Null);
    }

    let result = match agg_name {
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "sum" => values.iter().sum::<f64>(),
        "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
        "max" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        _ => return None,
    };
    Some(f64_to_cql_aggregate(result, col_type))
}

/// Apply `toJson()` transformations to projected rows for user-table SELECT queries.
///
/// Scans the SELECT column list for `toJson(col)` calls. For each one, finds the
/// source column value from the full (unprojected) row and replaces the projected
/// cell with its JSON representation.
fn apply_tojson_projections(
    select_columns_ast: &[SelectColumn],
    proj_col_names: &[String],
    all_col_names: &[String],
    full_rows: &[Vec<Option<CqlValue>>],
    mut projected_rows: Vec<Vec<Option<CqlValue>>>,
) -> Vec<Vec<Option<CqlValue>>> {
    // Find toJson columns: (projected_index, source_column_index)
    let mut tojson_ops: Vec<(usize, usize)> = Vec::new();
    for (proj_idx, sc) in select_columns_ast.iter().enumerate() {
        if let SelectColumn::FunctionCall { name, args, .. } = sc {
            if name.eq_ignore_ascii_case("tojson") {
                if let Some(arg) = args.first() {
                    if let Ok(col_name) = extract_column_name(arg) {
                        if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name) {
                            if proj_idx < proj_col_names.len() {
                                tojson_ops.push((proj_idx, src_idx));
                            }
                        }
                    }
                }
            }
        }
    }

    if tojson_ops.is_empty() {
        return projected_rows;
    }

    for (full_row, proj_row) in full_rows.iter().zip(projected_rows.iter_mut()) {
        for &(proj_idx, src_idx) in &tojson_ops {
            let val = full_row
                .get(src_idx)
                .and_then(|v| v.as_ref())
                .unwrap_or(&CqlValue::Null);
            let json = bridge::cql_value_to_json(val);
            proj_row[proj_idx] = Some(CqlValue::Text(json));
        }
    }

    projected_rows
}

/// Apply `now()`, `toTimestamp()`, `writetime()`, and `TTL()` built-in functions
/// to projected rows for user-table SELECT queries.
///
/// `cell_meta` carries per-column metadata (timestamp, TTL) from storage,
/// parallel to `full_rows`. For `now()` and `toTimestamp()`, no source data
/// is needed. For `writetime()` and `TTL()`, the metadata is looked up by
/// the target column name.
///
/// Currently unused — will be wired into route_select when partition reads
/// integrate cell metadata via partition_to_rows_with_metadata.
#[allow(dead_code)]
fn apply_builtin_functions(
    select_columns_ast: &[SelectColumn],
    proj_col_names: &[String],
    all_col_names: &[String],
    cell_meta: &[Vec<bridge::CellMeta>],
    mut projected_rows: Vec<Vec<Option<CqlValue>>>,
) -> Vec<Vec<Option<CqlValue>>> {
    // Collect operations: (projected_index, kind)
    enum BuiltinOp {
        Now,
        ToTimestamp,
        Writetime(usize), // source column index in full row
        Ttl(usize),       // source column index in full row
    }
    let mut ops: Vec<(usize, BuiltinOp)> = Vec::new();

    for (proj_idx, sc) in select_columns_ast.iter().enumerate() {
        if let SelectColumn::FunctionCall { name, args, .. } = sc {
            let fn_lower = name.to_lowercase();
            match fn_lower.as_str() {
                "now" if proj_idx < proj_col_names.len() => {
                    ops.push((proj_idx, BuiltinOp::Now));
                }
                "totimestamp" if proj_idx < proj_col_names.len() => {
                    ops.push((proj_idx, BuiltinOp::ToTimestamp));
                }
                "writetime" => {
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                if proj_idx < proj_col_names.len() {
                                    ops.push((proj_idx, BuiltinOp::Writetime(src_idx)));
                                }
                            }
                        }
                    }
                }
                "ttl" => {
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                if proj_idx < proj_col_names.len() {
                                    ops.push((proj_idx, BuiltinOp::Ttl(src_idx)));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if ops.is_empty() {
        return projected_rows;
    }

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    for (row_idx, proj_row) in projected_rows.iter_mut().enumerate() {
        let meta = cell_meta.get(row_idx);
        for (proj_idx, op) in &ops {
            match op {
                BuiltinOp::Now => {
                    proj_row[*proj_idx] = Some(crate::bridge::eval_now());
                }
                BuiltinOp::ToTimestamp => {
                    // toTimestamp(now()) -- generate a timeuuid and convert
                    let timeuuid = crate::bridge::eval_now();
                    proj_row[*proj_idx] = eval_to_timestamp(&timeuuid).ok();
                }
                BuiltinOp::Writetime(src_idx) => {
                    if let Some(m) = meta.and_then(|m| m.get(*src_idx)) {
                        if m.timestamp != i64::MIN {
                            proj_row[*proj_idx] = Some(CqlValue::Bigint(m.timestamp));
                        } else {
                            proj_row[*proj_idx] = None;
                        }
                    } else {
                        proj_row[*proj_idx] = None;
                    }
                }
                BuiltinOp::Ttl(src_idx) => {
                    if let Some(m) = meta.and_then(|m| m.get(*src_idx)) {
                        if m.ttl > 0 {
                            // Remaining TTL = ttl - (now_secs - cell_timestamp_secs)
                            let cell_ts_secs = m.timestamp / 1_000_000;
                            let elapsed = now_ts - cell_ts_secs;
                            let remaining = (m.ttl as i64) - elapsed;
                            if remaining > 0 {
                                proj_row[*proj_idx] = Some(CqlValue::Int(remaining as i32));
                            } else {
                                proj_row[*proj_idx] = None; // expired
                            }
                        } else {
                            proj_row[*proj_idx] = None; // no TTL set
                        }
                    } else {
                        proj_row[*proj_idx] = None;
                    }
                }
            }
        }
    }

    projected_rows
}

/// Project rows to a selected column subset.
///
/// Columns that do not match any name in `all_names` (e.g. UDF display
/// names like `"ks.to_celsius"`) get a `None` placeholder so the
/// projected row has exactly `selected.len()` cells. The caller can
/// then overwrite those placeholders with UDF results.
fn select_columns(
    rows: &[Vec<Option<CqlValue>>],
    all_names: &[String],
    selected: &[String],
) -> Vec<Vec<Option<CqlValue>>> {
    // If selected == all_names, return as-is
    if all_names == selected {
        return rows.to_vec();
    }
    // Build index mapping: Some(idx) for real columns, None for function calls.
    let indices: Vec<Option<usize>> = selected
        .iter()
        .map(|name| all_names.iter().position(|n| n == name))
        .collect();
    rows.iter()
        .map(|row| {
            indices
                .iter()
                .map(|opt_i| match opt_i {
                    Some(i) => row[*i].clone(),
                    None => None,
                })
                .collect()
        })
        .collect()
}

/// Convert an AST `GrantResource` to a schema `Resource`.
fn ast_resource_to_schema(
    resource: &GrantResource,
    current_ks: &Option<String>,
) -> Result<Resource, CqlError> {
    match resource {
        GrantResource::AllKeyspaces => Ok(Resource::AllKeyspaces),
        GrantResource::Keyspace(ks) => Ok(Resource::Keyspace(ks.clone())),
        GrantResource::Table(opt_ks, table) => {
            let ks = opt_ks
                .as_deref()
                .or(current_ks.as_deref())
                .ok_or_else(|| CqlError::Invalid("no keyspace specified for table".into()))?;
            Ok(Resource::Table(ks.to_string(), table.clone()))
        }
        GrantResource::AllRoles => Ok(Resource::AllRoles),
        GrantResource::Role(name) => Ok(Resource::Role(name.clone())),
        GrantResource::Function {
            keyspace,
            name,
            arg_types,
        } => {
            let ks = keyspace
                .as_deref()
                .or(current_ks.as_deref())
                .ok_or_else(|| CqlError::Invalid("no keyspace specified for function".into()))?;
            let type_strings: Vec<String> = arg_types.iter().map(|t| format!("{t:?}")).collect();
            Ok(Resource::Function(
                ks.to_string(),
                name.clone(),
                type_strings,
            ))
        }
        GrantResource::AllFunctions { keyspace } => {
            let ks = keyspace
                .as_deref()
                .or(current_ks.as_deref())
                .ok_or_else(|| {
                    CqlError::Invalid("no keyspace specified for ALL FUNCTIONS".into())
                })?;
            Ok(Resource::AllFunctions(ks.to_string()))
        }
    }
}

/// Parse permission strings into a `HashSet<Permission>`.
fn parse_permissions(perm_strings: &[String]) -> Result<HashSet<Permission>, CqlError> {
    let mut perms = HashSet::new();
    for s in perm_strings {
        match s.to_uppercase().as_str() {
            "ALL" | "ALL PERMISSIONS" => {
                perms.insert(Permission::Create);
                perms.insert(Permission::Alter);
                perms.insert(Permission::Drop);
                perms.insert(Permission::Select);
                perms.insert(Permission::Modify);
                perms.insert(Permission::Authorize);
                perms.insert(Permission::Describe);
                perms.insert(Permission::Execute);
            }
            "CREATE" => {
                perms.insert(Permission::Create);
            }
            "ALTER" => {
                perms.insert(Permission::Alter);
            }
            "DROP" => {
                perms.insert(Permission::Drop);
            }
            "SELECT" => {
                perms.insert(Permission::Select);
            }
            "MODIFY" => {
                perms.insert(Permission::Modify);
            }
            "AUTHORIZE" => {
                perms.insert(Permission::Authorize);
            }
            "DESCRIBE" => {
                perms.insert(Permission::Describe);
            }
            "EXECUTE" => {
                perms.insert(Permission::Execute);
            }
            _ => {
                return Err(CqlError::Invalid(format!("unknown permission: {}", s)));
            }
        }
    }
    Ok(perms)
}

/// Resolve a column type string using schema context for UDT resolution.
///
/// Wrapper around `bridge::parse_cql_type_in_keyspace` that provides
/// the keyspace and schema needed to resolve user-defined type names.
fn resolve_col_type(col_type: &str, ks: &str, schema: &Schema) -> Result<CqlType, CqlError> {
    bridge::parse_cql_type_in_keyspace(col_type, ks, schema)
}

/// Convert an AST `CqlTypeName` to a column_type string for `ColumnMetadata`.
fn cql_type_name_to_string(type_name: &CqlTypeName) -> String {
    match type_name {
        CqlTypeName::Simple(s) => s.clone(),
        CqlTypeName::List(inner) => format!("list<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Set(inner) => format!("set<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Map(k, v) => format!(
            "map<{}, {}>",
            cql_type_name_to_string(k),
            cql_type_name_to_string(v)
        ),
        CqlTypeName::Tuple(types) => {
            let inner: Vec<String> = types.iter().map(cql_type_name_to_string).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlTypeName::Frozen(inner) => format!("frozen<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Vector(inner, dim) => {
            format!("vector<{}, {}>", cql_type_name_to_string(inner), dim)
        }
    }
}

// ── DDL: User-Defined Types ──────────────────────────────────────────────

async fn route_create_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    if_not_exists: bool,
    fields: Vec<(String, CqlTypeName)>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Create,
        &Resource::Keyspace(ks.clone()),
    )?;

    // IF NOT EXISTS check
    if if_not_exists && state.schema.get_type(&ks, &name).is_some() {
        return Ok(result::encode_schema_change(
            "CREATED",
            "TYPE",
            &[&ks, &name],
        ));
    }

    // Resolve field types using bridge::resolve_type_name
    let resolved_fields: Vec<(String, CqlType)> = fields
        .iter()
        .map(|(fname, ftype)| {
            let resolved = bridge::resolve_type_name(ftype, &ks, &state.schema)?;
            Ok((fname.clone(), resolved))
        })
        .collect::<Result<_, CqlError>>()?;

    let udt = UserTypeMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        fields: resolved_fields,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::CreateType(udt))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateType(udt);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateType(udt);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "TYPE",
        &[&ks, &name],
    ))
}

async fn route_alter_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    alterations: Vec<TypeAlteration>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    for alt in alterations {
        match alt {
            TypeAlteration::AddField {
                name: fname,
                field_type,
            } => {
                let resolved = bridge::resolve_type_name(&field_type, &ks, &state.schema)?;
                state
                    .schema
                    .alter_type_add_field(&ks, &name, &fname, resolved)
                    .map_err(|e| CqlError::Invalid(format!("alter type: {e}")))?;
            }
            TypeAlteration::RenameField { from, to } => {
                state
                    .schema
                    .alter_type_rename_field(&ks, &name, &from, &to)
                    .map_err(|e| CqlError::Invalid(format!("alter type: {e}")))?;
            }
            TypeAlteration::AlterField { .. } => {
                // ALTER TYPE ... ALTER field TYPE is deprecated in Cassandra; defer
            }
        }
    }

    // Re-persist the altered UDT to the dogfooded `system_schema.types` table so
    // the storage-served read path reflects the change after restart. ALTER TYPE
    // mutates the Registry in place (not via the DDL `SystemTableWriter`), so we
    // upsert the row here — same PK (keyspace) + clustering (type_name) as the
    // CREATE path, overwriting the previous field list.
    if let Some(updated) = state.schema.get_type(&ks, &name) {
        let row = ferrosa_schema::system::persistence::type_to_row(&updated);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let tid = TableId::new("system_schema", "types");
        state
            .engine
            .write(&tid, &row.key, row.row, ts)
            .map_err(|e| CqlError::ServerError(format!("persist altered type: {e}")))?;
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "TYPE",
        &[&ks, &name],
    ))
}

async fn route_drop_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Drop, &Resource::Keyspace(ks.clone()))?;

    // IF EXISTS check
    if if_exists && state.schema.get_type(&ks, &name).is_none() {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "TYPE",
            &[&ks, &name],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "TYPE",
        &[&ks, &name],
    ))
}

// ── UDF/UDA DDL routing ──────────────────────────────────────────────────

/// Decode a hex-encoded string to bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, CqlError> {
    // Strip optional 0x / 0X prefix.
    let hex = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex);
    if !hex.len().is_multiple_of(2) {
        return Err(CqlError::Invalid("hex body has odd length".into()));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| CqlError::Invalid(format!("invalid hex at offset {i}")))
        })
        .collect()
}

fn hex_encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode_bytes(&Sha256::digest(bytes))
}

/// Compile an inline AssemblyScript function body to a UDF component, returning
/// (component bytes, hex for replication). The compile runs on a blocking thread
/// (it drives the QuickJS/wasmtime toolchain synchronously).
#[cfg(feature = "asc-udf")]
async fn compile_assemblyscript_body(
    name: &str,
    body: &FunctionBodySource,
    arg_types: &[ferrosa_common::CqlType],
    return_type: &ferrosa_common::CqlType,
) -> Result<(Vec<u8>, String), CqlError> {
    let source = match body {
        FunctionBodySource::InlineHex(s) => s.clone(),
        _ => {
            return Err(CqlError::Invalid(
                "AssemblyScript UDFs require inline source: AS '<source>'".into(),
            ))
        }
    };
    let nm = name.to_string();
    let at = arg_types.to_vec();
    let rt = return_type.clone();
    let component = ferrosa_net::task_pool::TaskPool::current("cql-udf-compile")
        .spawn_blocking(move || {
            ferrosa_udf::component::compile_to_component(&nm, &source, &at, &rt)
        })
        .await
        .map_err(|e| CqlError::Invalid(format!("AssemblyScript compile task panicked: {e}")))?
        .map_err(|e| CqlError::Invalid(format!("{e}")))?;
    let stored = hex_encode_bytes(&component);
    Ok((component, stored))
}

#[cfg(not(feature = "asc-udf"))]
async fn compile_assemblyscript_body(
    _name: &str,
    _body: &FunctionBodySource,
    _arg_types: &[ferrosa_common::CqlType],
    _return_type: &ferrosa_common::CqlType,
) -> Result<(Vec<u8>, String), CqlError> {
    Err(CqlError::Invalid(
        "AssemblyScript UDF support was not compiled in (rebuild ferrosa with the \
         asc-udf feature)"
            .into(),
    ))
}

async fn load_function_body(body: &FunctionBodySource) -> Result<(Vec<u8>, String), CqlError> {
    match body {
        FunctionBodySource::InlineHex(hex) => Ok((hex_decode(hex)?, hex.clone())),
        FunctionBodySource::File(path) => {
            let bytes = fs::read(path).map_err(|e| {
                CqlError::Invalid(format!("failed to read WASM file '{path}': {e}"))
            })?;
            let stored_body = hex_encode_bytes(&bytes);
            Ok((bytes, stored_body))
        }
        FunctionBodySource::Url { url, sha256 } => {
            let response = reqwest::get(url)
                .await
                .map_err(|e| CqlError::Invalid(format!("failed to fetch WASM URL '{url}': {e}")))?;
            if !response.status().is_success() {
                return Err(CqlError::Invalid(format!(
                    "failed to fetch WASM URL '{url}': HTTP {}",
                    response.status()
                )));
            }
            let bytes = response
                .bytes()
                .await
                .map_err(|e| CqlError::Invalid(format!("failed to read WASM URL '{url}': {e}")))?;
            let bytes = bytes.to_vec();
            let actual = sha256_hex(&bytes);
            let expected = sha256
                .strip_prefix("0x")
                .or_else(|| sha256.strip_prefix("0X"))
                .unwrap_or(sha256)
                .to_ascii_lowercase();
            if actual != expected {
                return Err(CqlError::Invalid(format!(
                    "WASM URL SHA-256 mismatch for '{url}': expected {expected}, got {actual}"
                )));
            }
            Ok((bytes.clone(), hex_encode_bytes(&bytes)))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn route_create_function(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    or_replace: bool,
    if_not_exists: bool,
    params: Vec<(String, CqlTypeName)>,
    called_on_null: bool,
    return_type: CqlTypeName,
    language: String,
    body: FunctionBodySource,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // WASM (precompiled) and AssemblyScript (compiled inline) are supported.
    let is_assemblyscript = language.eq_ignore_ascii_case("assemblyscript");
    if !language.eq_ignore_ascii_case("wasm") && !is_assemblyscript {
        return Err(CqlError::Invalid(format!(
            "unsupported UDF language '{}': only 'wasm' and 'assemblyscript' are supported",
            language
        )));
    }
    if !ctx.auth.is_superuser {
        return Err(CqlError::Invalid(
            "CREATE FUNCTION requires a superuser role".into(),
        ));
    }

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve parameter types
    let (arg_names, arg_types): (Vec<String>, Vec<ferrosa_common::CqlType>) = params
        .iter()
        .map(|(pname, ptype)| {
            let resolved = bridge::resolve_type_name(ptype, &ks, &state.schema)?;
            Ok((pname.clone(), cql_type_to_common(&resolved)))
        })
        .collect::<Result<Vec<_>, CqlError>>()?
        .into_iter()
        .unzip();

    // Resolve return type
    let resolved_return = bridge::resolve_type_name(&return_type, &ks, &state.schema)?;
    let common_return = cql_type_to_common(&resolved_return);

    // Check for existing function
    let existing = state.schema.get_function(&ks, &name, &arg_types);
    let replacing_existing = existing.is_some() && or_replace;
    if existing.is_some() {
        if if_not_exists {
            let arg_type_names: Vec<String> = arg_types
                .iter()
                .map(bridge::cql_type_display_name)
                .collect();
            return Ok(result::encode_schema_change_with_args(
                "CREATED",
                "FUNCTION",
                &[&ks, &name],
                &arg_type_names,
            ));
        } else if or_replace {
            // Continue. The new component is loaded and compiled before schema
            // metadata is replaced, so invalid replacements leave the old
            // schema entry intact.
        } else {
            return Err(CqlError::Invalid(format!(
                "function {ks}.{name} already exists"
            )));
        }
    }

    // Build arg type name strings for the SCHEMA_CHANGE response (CQL protocol
    // requires a [string list] of argument type names for FUNCTION targets).
    let arg_type_names: Vec<String> = arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // Load WASM bytes (precompiled) or compile AssemblyScript source inline, only
    // after existence semantics are settled. Either way `wasm_bytes` is a
    // component the executor compiles; `stored_body` is its hex for replication.
    let (wasm_bytes, stored_body) = if is_assemblyscript {
        compile_assemblyscript_body(&name, &body, &arg_types, &common_return).await?
    } else {
        load_function_body(&body).await?
    };
    if ferrosa_udf::UdfExecutor::wasm_declares_streaming_aggregate_abi(&wasm_bytes) {
        state
            .udf_executor
            .compile_streaming_aggregate(&ks, &name, &arg_types, &wasm_bytes)
            .map_err(CqlError::from)?;
    } else {
        state
            .udf_executor
            .compile(&ks, &name, &arg_types, &wasm_bytes)
            .map_err(CqlError::from)?;
    }
    let drop_arg_types = arg_types.clone();

    let func_meta = UserFunctionMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        arg_names,
        arg_types,
        return_type: common_return,
        called_on_null,
        language: language.to_ascii_lowercase(),
        body: stored_body,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            if replacing_existing {
                ddl.execute(DdlOperation::DropFunction {
                    keyspace: ks.clone(),
                    name: name.clone(),
                    arg_types: drop_arg_types.clone(),
                })
                .await
                .map_err(CqlError::from)?;
            }
            ddl.execute(DdlOperation::CreateFunction(func_meta))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            if replacing_existing {
                coordinator
                    .coordinate_ddl(DdlOperation::DropFunction {
                        keyspace: ks.clone(),
                        name: name.clone(),
                        arg_types: drop_arg_types.clone(),
                    })
                    .await?;
            }
            let op = DdlOperation::CreateFunction(func_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            if replacing_existing {
                ddl.execute(DdlOperation::DropFunction {
                    keyspace: ks.clone(),
                    name: name.clone(),
                    arg_types: drop_arg_types.clone(),
                })
                .await
                .map_err(CqlError::from)?;
            }
            let op = DdlOperation::CreateFunction(func_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "CREATED",
        "FUNCTION",
        &[&ks, &name],
        &arg_type_names,
    ))
}

async fn route_drop_function(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    arg_type_names: Option<Vec<CqlTypeName>>,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve arg types to find the exact function
    let resolved_arg_types: Vec<ferrosa_common::CqlType> = match arg_type_names {
        Some(type_names) => type_names
            .iter()
            .map(|tn| {
                let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
                Ok(cql_type_to_common(&resolved))
            })
            .collect::<Result<_, CqlError>>()?,
        None => {
            // No arg types specified — find function by name only.
            // Search all functions in this keyspace with this name.
            let snap = state.schema.snapshot();
            let matching: Vec<Vec<ferrosa_common::CqlType>> = snap
                .functions
                .keys()
                .filter(|(fks, fname, _)| fks == &ks && fname == &name)
                .map(|(_, _, arg_types)| arg_types.clone())
                .collect();
            match matching.len() {
                0 => {
                    if if_exists {
                        return Ok(result::encode_schema_change_with_args(
                            "DROPPED",
                            "FUNCTION",
                            &[&ks, &name],
                            &[],
                        ));
                    }
                    return Err(CqlError::Invalid(format!("function {ks}.{name} not found")));
                }
                1 => {
                    // SAFETY: len() == 1 guarantees next() returns Some
                    matching.into_iter().next().ok_or_else(|| {
                        CqlError::ServerError(format!("function {ks}.{name} lookup inconsistency"))
                    })?
                }
                _ => {
                    return Err(CqlError::Invalid(format!(
                        "ambiguous function {ks}.{name}: multiple overloads exist, specify argument types"
                    )));
                }
            }
        }
    };

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let resolved_arg_type_names: Vec<String> = resolved_arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // IF EXISTS check
    if if_exists
        && state
            .schema
            .get_function(&ks, &name, &resolved_arg_types)
            .is_none()
    {
        return Ok(result::encode_schema_change_with_args(
            "DROPPED",
            "FUNCTION",
            &[&ks, &name],
            &resolved_arg_type_names,
        ));
    }

    // Invalidate cached compilation
    state
        .udf_executor
        .invalidate(&ks, &name, &resolved_arg_types);

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "DROPPED",
        "FUNCTION",
        &[&ks, &name],
        &resolved_arg_type_names,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn route_create_aggregate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    or_replace: bool,
    if_not_exists: bool,
    ast_arg_types: Vec<CqlTypeName>,
    state_func: String,
    state_type: CqlTypeName,
    final_func: Option<String>,
    init_cond: Option<Term>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve argument types
    let arg_types: Vec<ferrosa_common::CqlType> = ast_arg_types
        .iter()
        .map(|tn| {
            let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
            Ok(cql_type_to_common(&resolved))
        })
        .collect::<Result<_, CqlError>>()?;

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let agg_arg_type_names: Vec<String> = arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // Resolve state type
    let resolved_state_type = bridge::resolve_type_name(&state_type, &ks, &state.schema)?;
    let common_state_type = cql_type_to_common(&resolved_state_type);

    // Validate state function exists. The state function takes (state_type, arg_types...) as params.
    let mut sfunc_arg_types = vec![common_state_type.clone()];
    sfunc_arg_types.extend(arg_types.iter().cloned());
    if state
        .schema
        .get_function(&ks, &state_func, &sfunc_arg_types)
        .is_none()
    {
        return Err(CqlError::Invalid(format!(
            "state function {ks}.{state_func} not found with expected signature"
        )));
    }

    // Determine return type: final_func return type if present, otherwise state_type.
    let return_type = if let Some(ref ff_name) = final_func {
        // Final function takes a single arg of the state type
        let ff_args = vec![common_state_type.clone()];
        match state.schema.get_function(&ks, ff_name, &ff_args) {
            Some(ff_meta) => ff_meta.return_type.clone(),
            None => {
                return Err(CqlError::Invalid(format!(
                    "final function {ks}.{ff_name} not found with expected signature"
                )));
            }
        }
    } else {
        common_state_type.clone()
    };

    // Resolve init_cond if present
    let resolved_init_cond = if let Some(ref term) = init_cond {
        let cql_state_type = &resolved_state_type;
        Some(bridge::term_to_cql_value(term, cql_state_type)?)
    } else {
        None
    };

    // Convert the CqlValue (wire type) to common CqlValue
    let common_init_cond = resolved_init_cond.map(|v| cql_value_to_common(&v));

    // Check for existing aggregate
    let existing = state.schema.get_aggregate(&ks, &name, &arg_types);
    if existing.is_some() {
        if or_replace {
            // Will be replaced by the new creation
        } else if if_not_exists {
            return Ok(result::encode_schema_change_with_args(
                "CREATED",
                "AGGREGATE",
                &[&ks, &name],
                &agg_arg_type_names,
            ));
        } else {
            return Err(CqlError::Invalid(format!(
                "aggregate {ks}.{name} already exists"
            )));
        }
    }

    let agg_meta = UserAggregateMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        arg_types,
        state_func,
        state_type: common_state_type,
        final_func,
        init_cond: common_init_cond,
        return_type,
        wasm_body: None,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::CreateAggregate(agg_meta))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateAggregate(agg_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateAggregate(agg_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "CREATED",
        "AGGREGATE",
        &[&ks, &name],
        &agg_arg_type_names,
    ))
}

async fn route_drop_aggregate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    arg_type_names: Option<Vec<CqlTypeName>>,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve arg types to find the exact aggregate
    let resolved_arg_types: Vec<ferrosa_common::CqlType> = match arg_type_names {
        Some(type_names) => type_names
            .iter()
            .map(|tn| {
                let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
                Ok(cql_type_to_common(&resolved))
            })
            .collect::<Result<_, CqlError>>()?,
        None => {
            // No arg types specified — find aggregate by name only.
            let snap = state.schema.snapshot();
            let matching: Vec<Vec<ferrosa_common::CqlType>> = snap
                .aggregates
                .keys()
                .filter(|(a_ks, a_name, _)| a_ks == &ks && a_name == &name)
                .map(|(_, _, arg_types)| arg_types.clone())
                .collect();
            match matching.len() {
                0 => {
                    if if_exists {
                        return Ok(result::encode_schema_change_with_args(
                            "DROPPED",
                            "AGGREGATE",
                            &[&ks, &name],
                            &[],
                        ));
                    }
                    return Err(CqlError::Invalid(format!(
                        "aggregate {ks}.{name} not found"
                    )));
                }
                1 => {
                    // SAFETY: len() == 1 guarantees next() returns Some
                    matching.into_iter().next().ok_or_else(|| {
                        CqlError::ServerError(format!("aggregate {ks}.{name} lookup inconsistency"))
                    })?
                }
                _ => {
                    return Err(CqlError::Invalid(format!(
                        "ambiguous aggregate {ks}.{name}: multiple overloads exist, specify argument types"
                    )));
                }
            }
        }
    };

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let resolved_agg_arg_type_names: Vec<String> = resolved_arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // IF EXISTS check
    if if_exists
        && state
            .schema
            .get_aggregate(&ks, &name, &resolved_arg_types)
            .is_none()
    {
        return Ok(result::encode_schema_change_with_args(
            "DROPPED",
            "AGGREGATE",
            &[&ks, &name],
            &resolved_agg_arg_type_names,
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable | DdlPath::Forming { .. } => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "DROPPED",
        "AGGREGATE",
        &[&ks, &name],
        &resolved_agg_arg_type_names,
    ))
}

/// Convert a CQL wire type to ferrosa_common::CqlType.
///
/// In Ferrosa, `CqlType` from the CQL crate and from ferrosa_common are the
/// same type (`ferrosa_common::CqlType`), so this is an identity conversion.
/// However, the bridge layer might use a different repr in the future.
fn cql_type_to_common(cql_type: &CqlType) -> ferrosa_common::CqlType {
    cql_type.clone()
}

/// Convert a CQL wire value to ferrosa_common::CqlValue.
fn cql_value_to_common(val: &CqlValue) -> ferrosa_common::CqlValue {
    // CqlValue in ferrosa-cql IS ferrosa_common::CqlValue — same type, identity conversion.
    val.clone()
}

// ── UDF/UDA query-time resolution and execution ──────────────────────────

/// Infer the CQL type of a `Term` expression.
///
/// For column references (parsed as `Term::FunctionCall { name, args: [] }`),
/// the column name is looked up in `all_col_names` / `all_col_types` to return
/// the column's declared type.
fn infer_term_type(
    term: &Term,
    all_col_names: &[String],
    all_col_types: &[CqlType],
) -> Result<CqlType, CqlError> {
    match term {
        Term::IntegerLiteral(_) => Ok(CqlType::Int),
        Term::FloatLiteral(_) => Ok(CqlType::Double),
        Term::StringLiteral(_) => Ok(CqlType::Varchar),
        Term::BoolLiteral(_) => Ok(CqlType::Boolean),
        Term::UuidLiteral(_) => Ok(CqlType::Uuid),
        Term::BlobLiteral(_) => Ok(CqlType::Blob),
        Term::Null => Ok(CqlType::Varchar), // default for untyped null
        Term::FunctionCall { name, args, .. } if args.is_empty() => {
            // Zero-arg function call may actually be a column reference.
            if let Some(idx) = all_col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
            {
                Ok(all_col_types[idx].clone())
            } else {
                // Actual zero-arg function — treat return type as Varchar as a
                // default; the real type will be resolved during function lookup.
                Ok(CqlType::Varchar)
            }
        }
        _ => Err(CqlError::Invalid(format!(
            "cannot infer type for term: {:?}",
            term
        ))),
    }
}

/// Resolve a `SelectColumn::FunctionCall` into a `ResolvedFunction`.
///
/// Looks up the function in the schema (trying aggregates first, then scalar
/// UDFs) and builds the argument index mapping.
#[allow(clippy::too_many_arguments)]
fn resolve_select_function(
    ks: &str,
    func_keyspace: Option<&str>,
    func_name: &str,
    args: &[Term],
    alias: Option<&str>,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    schema: &Schema,
) -> Result<ResolvedFunction, CqlError> {
    let func_ks = func_keyspace.unwrap_or(ks);

    // Infer argument types.
    let arg_types: Vec<CqlType> = args
        .iter()
        .map(|a| infer_term_type(a, all_col_names, all_col_types))
        .collect::<Result<Vec<_>, _>>()?;

    // Build arg_indices: for each arg, if it is a column reference, record its
    // index in the full row; otherwise use usize::MAX as sentinel for literals.
    let arg_indices: Vec<usize> = args
        .iter()
        .map(|a| match a {
            Term::FunctionCall {
                name,
                args: inner_args,
                ..
            } if inner_args.is_empty() => all_col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
                .unwrap_or(usize::MAX),
            _ => usize::MAX,
        })
        .collect();

    let display_name = alias
        .map(|a| a.to_string())
        .unwrap_or_else(|| format!("{}.{}", func_ks, func_name));

    // Try aggregate first.
    if let Some(agg_meta) = schema.get_aggregate(func_ks, func_name, &arg_types) {
        return Ok(ResolvedFunction {
            func_name: func_name.to_string(),
            func_keyspace: func_ks.to_string(),
            kind: ResolvedFunctionKind::Aggregate {
                init_cond: agg_meta.init_cond.clone(),
            },
            return_type: agg_meta.return_type.clone(),
            called_on_null: false, // aggregates always accumulate
            arg_indices,
            arg_terms: args.to_vec(),
            arg_types,
            display_name,
        });
    }

    // Try scalar UDF.
    if let Some(func_meta) = schema.get_function(func_ks, func_name, &arg_types) {
        return Ok(ResolvedFunction {
            func_name: func_name.to_string(),
            func_keyspace: func_ks.to_string(),
            kind: ResolvedFunctionKind::Scalar,
            return_type: func_meta.return_type.clone(),
            called_on_null: func_meta.called_on_null,
            arg_indices,
            arg_terms: args.to_vec(),
            arg_types,
            display_name,
        });
    }

    Err(CqlError::Invalid(format!(
        "unknown function: {}.{}({})",
        func_ks,
        func_name,
        arg_types
            .iter()
            .map(bridge::cql_type_display_name)
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Extract the argument values for a UDF call from a full row.
///
/// For each arg: if it references a column (arg_indices[i] != usize::MAX),
/// extract the column value from the row; otherwise convert the literal Term.
fn extract_udf_args(
    row: &[Option<CqlValue>],
    func: &ResolvedFunction,
) -> Result<Vec<CqlValue>, CqlError> {
    let mut values = Vec::with_capacity(func.arg_indices.len());
    for (i, &idx) in func.arg_indices.iter().enumerate() {
        if idx != usize::MAX {
            // Column reference — use row value (or Null).
            let val = row
                .get(idx)
                .and_then(|v| v.clone())
                .unwrap_or(CqlValue::Null);
            values.push(val);
        } else {
            // Literal — convert the term using the inferred type.
            let val = bridge::term_to_cql_value(&func.arg_terms[i], &func.arg_types[i])?;
            values.push(val);
        }
    }
    Ok(values)
}

/// Evaluate scalar UDFs against a single row, returning the UDF results
/// in the order of `resolved_funcs`.
///
/// Each resolved function is evaluated using the WASM executor. If the function
/// is not `called_on_null` and any argument is NULL, the result is NULL without
/// invoking WASM.
fn evaluate_row_udfs(
    state: &SharedState,
    row: &[Option<CqlValue>],
    resolved_funcs: &[&ResolvedFunction],
) -> Result<Vec<Option<CqlValue>>, CqlError> {
    let mut results = Vec::with_capacity(resolved_funcs.len());
    for func in resolved_funcs {
        if !matches!(func.kind, ResolvedFunctionKind::Scalar) {
            // Aggregates are handled separately.
            results.push(None);
            continue;
        }
        let args = extract_udf_args(row, func)?;

        // Called-on-null short circuit: if any arg is Null and the function is
        // not marked CALLED ON NULL INPUT, return Null without invoking WASM.
        if !func.called_on_null && args.iter().any(|v| matches!(v, CqlValue::Null)) {
            results.push(None);
            continue;
        }

        let result = state.udf_executor.call(
            &func.func_keyspace,
            &func.func_name,
            args,
            &func.arg_types,
            &func.return_type,
        )?;
        results.push(Some(result));
    }
    Ok(results)
}

/// Execute a user-defined aggregate (UDA) over a set of rows.
///
/// Runs the state function per row and the optional final function once,
/// returning the aggregated result.
fn execute_uda(
    state: &SharedState,
    ks: &str,
    func: &ResolvedFunction,
    rows: &[Vec<Option<CqlValue>>],
) -> Result<CqlValue, CqlError> {
    let init_cond = match &func.kind {
        ResolvedFunctionKind::Aggregate { init_cond } => init_cond.clone(),
        _ => {
            return Err(CqlError::Invalid(
                "execute_uda called on non-aggregate function".into(),
            ));
        }
    };

    // Look up the aggregate metadata to get state_func, final_func, state_type.
    let agg_meta = state
        .schema
        .get_aggregate(&func.func_keyspace, &func.func_name, &func.arg_types)
        .ok_or_else(|| {
            CqlError::Invalid(format!(
                "aggregate not found: {}.{}",
                func.func_keyspace, func.func_name
            ))
        })?;

    // Initialize accumulator.
    let mut acc = init_cond.unwrap_or(CqlValue::Null);

    // Look up the state function metadata (needed for called_on_null).
    let state_func_arg_types: Vec<CqlType> = {
        let mut types = vec![agg_meta.state_type.clone()];
        types.extend(func.arg_types.iter().cloned());
        types
    };

    let state_func_meta =
        state
            .schema
            .get_function(ks, &agg_meta.state_func, &state_func_arg_types);
    let state_func_called_on_null = state_func_meta
        .as_ref()
        .map(|m| m.called_on_null)
        .unwrap_or(false);

    // Accumulate: call state_func(acc, row_args...) for each row.
    for row in rows {
        let row_args = extract_udf_args(row, func)?;

        // Skip rows with null args if state func is not called on null.
        if !state_func_called_on_null && row_args.iter().any(|v| matches!(v, CqlValue::Null)) {
            continue;
        }

        let mut call_args = vec![acc.clone()];
        call_args.extend(row_args);

        acc = state.udf_executor.call(
            ks,
            &agg_meta.state_func,
            call_args,
            &state_func_arg_types,
            &agg_meta.state_type,
        )?;
    }

    // Finalize: if there is a final function, call it on the accumulator.
    if let Some(ref final_func_name) = agg_meta.final_func {
        let final_arg_types = vec![agg_meta.state_type.clone()];
        acc = state.udf_executor.call(
            ks,
            final_func_name,
            vec![acc],
            &final_arg_types,
            &func.return_type,
        )?;
    }

    Ok(acc)
}

/// Check if any WHERE clause value contains a non-builtin function call,
/// indicating a UDF in WHERE that requires ALLOW FILTERING.
fn where_has_udf_calls(where_clauses: &[WhereClause]) -> bool {
    for wc in where_clauses {
        if term_has_udf_call(&wc.value) {
            return true;
        }
    }
    false
}

/// Check if any WHERE clause contains an `fts_match` function call.
///
/// `fts_match` is expressed as `WHERE col = fts_match('query string')` in the
/// parsed AST.  This function detects that pattern.
fn where_has_fts_match(where_clauses: &[WhereClause]) -> bool {
    where_clauses.iter().any(|wc| is_fts_match_term(&wc.value))
}

/// Returns true when `term` is `fts_match(query_string)`.
fn is_fts_match_term(term: &Term) -> bool {
    matches!(term, Term::FunctionCall { name, args, .. }
        if name.eq_ignore_ascii_case("fts_match") && !args.is_empty())
}

/// Extract the `(column_name, query_string)` pair from an `fts_match` WHERE clause.
///
/// Returns the first `fts_match` clause found, or `None` if none exists.
fn extract_fts_match(where_clauses: &[WhereClause]) -> Option<(&str, &str)> {
    for wc in where_clauses {
        if let Term::FunctionCall { name, args, .. } = &wc.value {
            if name.eq_ignore_ascii_case("fts_match") {
                if let Some(Term::StringLiteral(q)) = args.first() {
                    return Some((&wc.column, q.as_str()));
                }
            }
        }
    }
    None
}

/// Simple phonetic matching: compare two strings word-by-word using Soundex.
/// Returns true if all words have the same Soundex code.
/// Phonetic matching using the Double Metaphone encoder from ferrosa-index.
/// Compares word-by-word: "John Smith" matches "Jon Smyth" because each
/// word pair produces the same phonetic code.
fn phonetic_match(a: &str, b: &str) -> bool {
    let encoder = ferrosa_index::phonetic::PhoneticAlgorithm::DoubleMetaphone.encoder();
    let a_words: Vec<&str> = a.split_whitespace().collect();
    let b_words: Vec<&str> = b.split_whitespace().collect();
    if a_words.len() != b_words.len() {
        return false;
    }
    a_words
        .iter()
        .zip(b_words.iter())
        .all(|(wa, wb)| encoder.encode(wa) == encoder.encode(wb))
}

/// Check if a Term contains a non-builtin function call.
fn term_has_udf_call(term: &Term) -> bool {
    match term {
        Term::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            let is_builtin = matches!(
                lower.as_str(),
                "uuid"
                    | "now"
                    | "totimestamp"
                    | "todate"
                    | "count"
                    | "writetime"
                    | "ttl"
                    | "token"
                    | "fts_match"
            );
            if !is_builtin && !args.is_empty() {
                return true;
            }
            // Also check nested args.
            args.iter().any(term_has_udf_call)
        }
        Term::InList(items) | Term::ListLiteral(items) | Term::SetLiteral(items) => {
            items.iter().any(term_has_udf_call)
        }
        Term::TupleLiteral(items) => items.iter().any(term_has_udf_call),
        Term::MapLiteral(pairs) => pairs
            .iter()
            .any(|(k, v)| term_has_udf_call(k) || term_has_udf_call(v)),
        _ => false,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_tables::active_queries::ActiveQueriesTable;
    use crate::virtual_tables::connections::ConnectionsTable;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, SchemaConfig, TestAuditSink,
    };
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use tempfile::TempDir;

    fn setup() -> (SharedState, TempDir) {
        let dir = TempDir::new().unwrap();

        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        };
        let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
        let engine_config = StorageEngineConfig {
            commit_log,
            compaction,
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());

        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );

        let node_config = Arc::new(NodeConfig {
            cluster_name: "test".into(),
            data_center: "datacenter1".into(),
            rack: "rack1".into(),
            rpc_port: 9042,
            host_id: uuid::Uuid::new_v4(),
            listen_address: "127.0.0.1".parse().unwrap(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".parse().unwrap(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_port: 9042,
            tokens: vec![],
        });

        let udf_executor =
            Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());
        engine.set_time_series_wasm_aggregate_executor(Arc::new(
            crate::wasm_aggregate::UdfTimeSeriesAggregateExecutor::new(Arc::clone(&udf_executor)),
        ));

        let mode_controller =
            ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());
        let state = SharedState {
            engine: engine.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(
                ferrosa_cluster::ClusterStateHolder::Standalone,
            )),
            write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
            ddl_path: Arc::new(ArcSwap::from_pointee(DdlPath::Direct { schema, engine })),
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            full_scan_tracker: Arc::new(crate::virtual_tables::FullScanTracker::new()),
            index_usage_tracker: Arc::new(crate::virtual_tables::IndexUsageTracker::new()),
            udf_executor,
            event_sender: tokio::sync::broadcast::channel(64).0,
            mode_controller,
            cql_metrics: Arc::new(CqlMetrics::new()),
            topology_policy: ClientTopologyPolicy::default(),
            auth_warn: false,
            peer_manager: None,
            accord_clock: None,
        };
        (state, dir)
    }

    fn dev_auth() -> AuthContext {
        AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn test_ctx<'a>(auth: &'a AuthContext, ks: &'a Option<String>) -> RequestContext<'a> {
        RequestContext {
            auth,
            current_keyspace: ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        }
    }

    fn superuser_auth() -> ferrosa_schema::AuthContext {
        ferrosa_schema::AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn create_test_keyspace(
        schema: &Schema,
        name: &str,
        strategy: &str,
        opts: std::collections::HashMap<String, String>,
    ) {
        schema
            .create_keyspace(
                ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
                    name: name.to_string(),
                    durable_writes: true,
                    replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                        strategy: strategy.to_string(),
                        options: opts,
                    },
                },
                &superuser_auth(),
            )
            .unwrap();
    }

    #[test]
    fn parse_filter_conjunction_single_and_multi_clause() {
        // Single clause.
        let parsed = parse_filter_conjunction("age > 21").unwrap();
        assert_eq!(parsed, vec![("age".into(), ">".into(), "21".into())]);

        // Two clauses, mixed ops, quoted string literal stripped.
        let parsed = parse_filter_conjunction("age >= 21 AND dept = 'eng'").unwrap();
        assert_eq!(
            parsed,
            vec![
                ("age".into(), ">=".into(), "21".into()),
                ("dept".into(), "=".into(), "eng".into()),
            ]
        );

        // Case-insensitive AND, no spaces around the operator.
        let parsed = parse_filter_conjunction("a<=5 and b!='x'").unwrap();
        assert_eq!(
            parsed,
            vec![
                ("a".into(), "<=".into(), "5".into()),
                ("b".into(), "!=".into(), "x".into()),
            ]
        );
    }

    #[test]
    fn parse_filter_conjunction_rejects_malformed() {
        // No operator.
        assert!(parse_filter_conjunction("age 21").is_err());
        // Missing value.
        assert!(parse_filter_conjunction("age >").is_err());
        // Missing column.
        assert!(parse_filter_conjunction("> 21").is_err());
        // Empty expression.
        assert!(parse_filter_conjunction("   ").is_err());
    }

    #[test]
    fn parser_rejects_udf_on_where_lhs_with_clear_error() {
        let err =
            crate::parser::parse("SELECT * FROM ks.tbl WHERE is_hot(v) = true ALLOW FILTERING")
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("UDF calls on the left-hand side of WHERE predicates are not supported"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn where_rhs_udf_resolution_errors_instead_of_filtering_false() {
        let (state, _dir) = setup();
        let term = Term::FunctionCall {
            keyspace: None,
            name: "missing_fn".to_string(),
            args: vec![Term::IntegerLiteral(7)],
        };
        let err = evaluate_where_rhs_term(
            &term,
            &CqlType::Int,
            &[Some(CqlValue::Int(7))],
            &["v".to_string()],
            &[CqlType::Int],
            "ks",
            &state,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing_fn"), "unexpected error: {msg}");
    }

    #[test]
    fn keyspace_rf_returns_rf_for_simple_strategy() {
        let (state, _dir) = setup();
        let mut opts = std::collections::HashMap::new();
        opts.insert("replication_factor".to_string(), "3".to_string());
        create_test_keyspace(&state.schema, "test_ss", "SimpleStrategy", opts);
        assert_eq!(keyspace_rf(&state.schema, "test_ss"), 3);
    }

    #[test]
    fn keyspace_rf_returns_total_rf_for_nts() {
        let (state, _dir) = setup();
        let mut opts = std::collections::HashMap::new();
        opts.insert("datacenter1".to_string(), "3".to_string());
        create_test_keyspace(&state.schema, "test_nts", "NetworkTopologyStrategy", opts);
        assert_eq!(keyspace_rf(&state.schema, "test_nts"), 3);
    }

    #[test]
    fn keyspace_rf_returns_sum_for_multi_dc_nts() {
        let (state, _dir) = setup();
        let mut opts = std::collections::HashMap::new();
        opts.insert("datacenter1".to_string(), "3".to_string());
        opts.insert("datacenter2".to_string(), "2".to_string());
        create_test_keyspace(
            &state.schema,
            "test_multi_dc",
            "NetworkTopologyStrategy",
            opts,
        );
        assert_eq!(keyspace_rf(&state.schema, "test_multi_dc"), 5);
    }

    #[tokio::test]
    async fn prepared_insert_fast_path_executes_top_level_binds() {
        let (state, _dir) = setup();
        let current_keyspace = Some("fastins".to_string());
        let auth = dev_auth();
        let ctx = test_ctx(&auth, &current_keyspace);

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE fastins WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse("CREATE TABLE fastins.t (id int PRIMARY KEY, name text)").unwrap(),
        )
        .await
        .unwrap();

        let insert = match crate::parser::parse("INSERT INTO t (id, name) VALUES (?, ?)").unwrap() {
            Statement::Insert(insert) => insert,
            other => panic!("expected insert, got {other:?}"),
        };
        let bound_terms = vec![Term::IntegerLiteral(7), Term::StringLiteral("seven".into())];
        let executed_before = state.query_tracker.total_executed();

        let result = route_prepared_insert_fast(&state, &ctx, &insert, &bound_terms)
            .await
            .expect("simple top-level prepared INSERT should take fast path")
            .unwrap();

        match result {
            RouteResult::Result(body) => assert_eq!(&body[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected void result"),
        }
        assert_eq!(state.query_tracker.total_executed(), executed_before + 1);
    }

    #[tokio::test]
    async fn prepared_select_fast_path_reads_full_partition_key_with_limit() {
        let (state, _dir) = setup();
        let current_keyspace = Some("fastsel".to_string());
        let auth = dev_auth();
        let ctx = test_ctx(&auth, &current_keyspace);

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE fastsel WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse("CREATE TABLE fastsel.t (id int PRIMARY KEY, name text)").unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse("INSERT INTO fastsel.t (id, name) VALUES (7, 'seven')").unwrap(),
        )
        .await
        .unwrap();

        let select = match crate::parser::parse("SELECT * FROM t WHERE id = ? LIMIT 10").unwrap() {
            Statement::Select(select) => select,
            other => panic!("expected select, got {other:?}"),
        };
        let bound_terms = vec![Term::IntegerLiteral(7)];
        let executed_before = state.query_tracker.total_executed();

        let result = route_prepared_select_fast(&state, &ctx, &select, &bound_terms)
            .await
            .expect("full partition-key prepared SELECT should take fast path")
            .unwrap();

        match result {
            RouteResult::Result(body) => assert_eq!(&body[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected rows result"),
        }
        assert_eq!(state.query_tracker.total_executed(), executed_before + 1);
    }

    #[test]
    fn keyspace_rf_returns_1_when_keyspace_not_found() {
        let (state, _dir) = setup();
        assert_eq!(keyspace_rf(&state.schema, "nonexistent"), 1);
    }

    #[tokio::test]
    async fn create_keyspace_then_table_then_insert_then_select() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // CREATE KEYSPACE
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0005i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // CREATE TABLE
        let stmt =
            crate::parser::parse("CREATE TABLE ks.users (id int PRIMARY KEY, name text)").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0005i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // INSERT
        let stmt =
            crate::parser::parse("INSERT INTO ks.users (id, name) VALUES (1, 'alice')").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // SELECT
        let stmt = crate::parser::parse("SELECT * FROM ks.users WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    /// Transient replication ('<full>/<transient>') must be rejected at the
    /// proposer, before the DDL enters the Raft log — not at apply (which would
    /// diverge the state machine). Accepting it would persist an option string
    /// ('3/1') that strict CQL drivers cannot parse during schema agreement,
    /// poisoning every subsequent metadata fetch against the node.
    #[tokio::test]
    async fn create_keyspace_rejects_transient_replication() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE trans_ks WITH replication = \
             {'class': 'NetworkTopologyStrategy', 'datacenter1': '3/1'}",
        )
        .unwrap();
        let err = match route(&state, &ctx, stmt).await {
            Ok(_) => panic!("transient replication must be rejected"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("transient replication"),
            "expected transient-replication rejection, got: {msg}"
        );

        // The keyspace must NOT have been created.
        assert!(
            !state.schema.snapshot().keyspaces.contains_key("trans_ks"),
            "rejected keyspace must not exist in the schema"
        );
    }

    #[tokio::test]
    async fn cql_inserts_materialize_rrd_rollup_rows() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        for cql in [
            "CREATE KEYSPACE rrd WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE rrd.sensor_10s (sensor_id text, ts bigint, value_avg double, PRIMARY KEY (sensor_id, ts))",
            "CREATE TABLE rrd.sensor (sensor_id text, ts bigint, value double, PRIMARY KEY (sensor_id, ts)) WITH extensions = {'consolidation.interval': '10s', 'consolidation.functions': 'avg', 'consolidation.target': 'sensor_10s', 'consolidation.columns': 'value', 'consolidation.ring_capacity': '4'}",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap();
        }

        for ts in 0..=10 {
            let micros = ts * 1_000_000;
            let cql = format!(
                "INSERT INTO rrd.sensor (sensor_id, ts, value) VALUES ('s1', {micros}, {ts}.0)"
            );
            route(&state, &ctx, crate::parser::parse(&cql).unwrap())
                .await
                .unwrap();
        }

        assert_eq!(
            state
                .engine
                .process_pending_time_series_materializations(8)
                .unwrap(),
            1
        );

        let result = route(
            &state,
            &ctx,
            crate::parser::parse("SELECT value_avg FROM rrd.sensor_10s WHERE sensor_id = 's1'")
                .unwrap(),
        )
        .await
        .unwrap();
        let RouteResult::Result(body) = result else {
            panic!("expected Rows result");
        };
        let value = extract_first_double_value(&body);
        assert!(
            (value - 4.5).abs() < f64::EPSILON,
            "CQL DDL/DML should produce the same full-window rollup as storage-level materialization"
        );
    }

    #[tokio::test]
    async fn timeseries_rrd_example_executes_real_rollup_rows() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let mut current_keyspace: Option<String> = None;

        for cql in example_cql_statements(include_str!("../../examples/timeseries-rrd/schema.cql"))
            .into_iter()
            .chain(example_cql_statements(include_str!(
                "../../examples/timeseries-rrd/data.cql"
            )))
        {
            let ctx = RequestContext {
                auth: &auth,
                current_keyspace: &current_keyspace,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            };
            match route(&state, &ctx, crate::parser::parse(&cql).unwrap())
                .await
                .unwrap()
            {
                RouteResult::SetKeyspace(ks, _) => current_keyspace = Some(ks),
                RouteResult::Result(_) => {}
                _ => panic!("unexpected route result for example statement {cql:?}"),
            }
        }

        assert!(
            state
                .engine
                .process_pending_time_series_materializations(16)
                .unwrap()
                >= 1,
            "the runnable RRD example must enqueue at least one materialized 5-minute rollup"
        );

        let ctx = RequestContext {
            auth: &auth,
            current_keyspace: &Some("plant".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let full_scan = route(
            &state,
            &ctx,
            crate::parser::parse("SELECT vibration_mm_s_avg FROM sensor_readings_5m").unwrap(),
        )
        .await
        .unwrap();
        let RouteResult::Result(full_scan_body) = full_scan else {
            panic!("expected Rows result");
        };
        assert!(
            extract_row_count(&full_scan_body) >= 1,
            "example materialization should write at least one 5-minute target row"
        );

        let result = route(
            &state,
            &ctx,
            crate::parser::parse(
                "SELECT vibration_mm_s_avg \
                 FROM sensor_readings_5m \
                 WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let RouteResult::Result(body) = result else {
            panic!("expected Rows result");
        };
        let value = extract_first_double_value(&body);
        assert!(
            (value - 5.08).abs() < 0.000_000_1,
            "example first 5-minute rollup should match the documented Pump A average"
        );
    }

    #[tokio::test]
    async fn create_table_accepts_rrd_wasm_function_after_streaming_abi_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE rrd_wasm WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE TABLE rrd_wasm.sensor (sensor_id text, ts bigint, value double, PRIMARY KEY (sensor_id, ts)) WITH extensions = {'consolidation.interval': '10s', 'consolidation.functions': 'wasm:rrd_wasm.stddev', 'consolidation.target': 'sensor_10s', 'consolidation.columns': 'value'}",
            )
            .unwrap(),
        )
        .await
        .expect("RRD WASM function DDL should parse/register after streaming ABI exists");
        assert_eq!(state.engine.time_series_consolidator_count(), 1);
    }

    #[tokio::test]
    async fn feedback_outcomes_boolean_is_stored_as_single_byte_at_succeeded_column() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ctx = RequestContext {
            auth: &auth,
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let create_ks = crate::parser::parse(
            "CREATE KEYSPACE agent_memory WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, create_ks).await.unwrap();

        let create_table = crate::parser::parse(
            "CREATE TABLE agent_memory.feedback_outcomes (\
             tenant_id uuid, \
             created_at bigint, \
             query_id uuid, \
             session_id uuid, \
             program_type text, \
             query_embedding text, \
             task_complexity text, \
             succeeded boolean, \
             latency_ms int, \
             token_cost int, \
             guideline_version text, \
             PRIMARY KEY ((tenant_id), created_at, query_id))",
        )
        .unwrap();
        route(&state, &ctx, create_table).await.unwrap();

        let tenant_id = uuid::Uuid::from_bytes([0x11; 16]);
        let session_id = uuid::Uuid::from_bytes([0x22; 16]);
        let query_id = uuid::Uuid::from_bytes([0x33; 16]);
        // Mirrors ferrosa-memory's prepared `feedback_put`: it intentionally
        // omits nullable regular columns (`query_embedding`, `guideline_version`).
        // Storage cell indexes must still be schema indexes, not VALUES-list
        // indexes, or the final timestamp can land in `succeeded`'s BooleanType
        // slot during replica mutation forwarding/replay.
        let insert = crate::parser::parse(&format!(
            "INSERT INTO agent_memory.feedback_outcomes \
             (tenant_id, session_id, query_id, program_type, task_complexity, \
              succeeded, latency_ms, token_cost, created_at) \
             VALUES ({tenant_id}, {session_id}, {query_id}, \
                     'hybrid_search', 'linear', true, 42, 7, 1700000000000)"
        ))
        .unwrap();
        route(&state, &ctx, insert).await.unwrap();

        let table_id = ferrosa_storage::TableId::new("agent_memory", "feedback_outcomes");
        let decorated_key =
            bridge::build_decorated_key(&[CqlValue::Uuid(tenant_id)], &[CqlType::Uuid]).unwrap();
        let partition = state
            .engine
            .read(&table_id, &decorated_key)
            .unwrap()
            .expect("feedback_outcomes partition should exist");
        let row = partition.rows.first().expect("insert should write one row");
        let snap = state.schema.snapshot();
        let table_meta = snap
            .tables
            .get(&("agent_memory".to_string(), "feedback_outcomes".to_string()))
            .expect("feedback_outcomes schema exists");
        let succeeded_storage_idx = table_meta
            .storage_column_index("succeeded")
            .expect("succeeded has storage column index");
        let succeeded = row
            .cells
            .iter()
            .find(|(idx, _)| *idx == succeeded_storage_idx)
            .expect("succeeded should be present at its schema storage column index")
            .1
            .value
            .as_deref();

        assert_eq!(
            succeeded,
            Some(&[1u8][..]),
            "feedback_outcomes.succeeded must be stored as the 1-byte BooleanType representation, not an 8-byte integer/timestamp-adjacent value"
        );
    }

    #[tokio::test]
    async fn use_sets_default_keyspace() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        // Use a system keyspace — always exists, no need to create it.
        let stmt = crate::parser::parse("USE system").unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::SetKeyspace(ks, body) => {
                assert_eq!(ks, "system");
                assert_eq!(&body[0..4], &0x0003i32.to_be_bytes());
            }
            _ => panic!("expected SetKeyspace"),
        }
    }

    #[tokio::test]
    async fn select_system_local() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // Result kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
                // Column count is at offset 8 (after kind(4) + flags(4))
                let col_count = i32::from_be_bytes(b[8..12].try_into().unwrap());
                // 16 columns: key, cluster_name, data_center, rack, host_id,
                // partitioner, native_protocol_version, cql_version,
                // release_version, schema_version, rpc_port, listen_address,
                // broadcast_address, rpc_address, bootstrapped, tokens
                assert_eq!(col_count, 16);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_now_from_system_local_returns_one_timeuuid_column() {
        // Regression: `SELECT now() FROM system.local` previously returned a
        // row-set with one row and ZERO columns, because `filter_system_columns`
        // only handled `SelectColumn::Star` and `SelectColumn::Column(name)` —
        // any `FunctionCall` projection silently produced no columns.
        // Both cdrs-tokio and python cassandra-driver crash on the empty
        // column shape (see specs/in-process/bug-cql-system-local-empty-columns.md).
        //
        // Fix: filter_system_columns must materialise zero-arg builtin function
        // calls (`now()`, `currenttimestamp()`, `uuid()`) as a one-column
        // projection with the appropriate value + type.
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT now() FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes(), "kind=Rows");
                let col_count = i32::from_be_bytes(b[8..12].try_into().unwrap());
                assert_eq!(
                    col_count, 1,
                    "SELECT now() FROM system.local must produce exactly 1 column, \
                     got {col_count} (zero-column rows crash drivers)"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn internal_client_sees_internal_system_local_endpoint() {
        let (mut state, _dir) = setup();
        let mut node_config = (*state.node_config).clone();
        node_config.rpc_address = "127.0.0.1".parse().unwrap();
        node_config.rpc_port = 19042;
        node_config.internal_rpc_address = "10.89.1.48".parse().unwrap();
        node_config.internal_rpc_port = 9042;
        state.node_config = Arc::new(node_config);
        state.topology_policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "10.89.1.60:32123".into(),
        };
        let stmt = crate::parser::parse("SELECT rpc_address, rpc_port FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_column_names(b), vec!["rpc_address", "rpc_port"]);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "10.89.1.48".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 9042);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn client_that_looks_like_local_container_ip_still_gets_public_system_local_endpoint() {
        let (mut state, _dir) = setup();
        let mut node_config = (*state.node_config).clone();
        node_config.rpc_address = "127.0.0.1".parse().unwrap();
        node_config.rpc_port = 19042;
        node_config.internal_rpc_address = "10.89.1.48".parse().unwrap();
        node_config.internal_rpc_port = 9042;
        node_config.listen_address = "10.89.1.48".parse().unwrap();
        node_config.broadcast_address = "10.89.1.48".parse().unwrap();
        state.node_config = Arc::new(node_config);
        state.topology_policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "10.89.1.48:32123".into(),
        };
        let stmt = crate::parser::parse("SELECT rpc_address, rpc_port FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_column_names(b), vec!["rpc_address", "rpc_port"]);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 19042);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn ipv6_loopback_client_sees_ipv6_system_local_endpoint() {
        let (mut state, _dir) = setup();
        let mut node_config = (*state.node_config).clone();
        node_config.rpc_address = "127.0.0.1".parse().unwrap();
        node_config.rpc_port = 19042;
        state.node_config = Arc::new(node_config);

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "[::1]:32123".into(),
        };
        let stmt = crate::parser::parse("SELECT rpc_address, rpc_port FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_column_names(b), vec!["rpc_address", "rpc_port"]);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "::1".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 19042);
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression test: cqlsh expects `tokens` column (set<varchar>) in
    /// system.local. Without it, cqlsh prints "'local' not found in
    /// keyspace 'system'" during startup introspection.
    #[tokio::test]
    async fn select_system_local_includes_tokens_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let b = match &result {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result"),
        };

        // Decode column names from the Rows metadata.
        // Layout: [i32 kind][i32 flags][i32 col_count][string ks][string table]
        //         then per column: [string name][u16 type_id][type params]
        let col_count = i32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;

        // Skip past keyspace and table global_table_spec strings
        let mut off = 12;
        // keyspace string: [u16 len][bytes]
        let ks_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // table string: [u16 len][bytes]
        let tbl_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;

        // Read column names
        let mut col_names = Vec::new();
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let name = std::str::from_utf8(&b[off..off + name_len]).unwrap();
            col_names.push(name.to_string());
            off += name_len;
            // Skip type_id (u16) + possible type params
            let type_id = u16::from_be_bytes(b[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2, // List/Set: element type_id
                0x0021 => off += 4,          // Map: key + val type_ids
                _ => {}
            }
        }

        assert!(
            col_names.contains(&"tokens".to_string()),
            "system.local must include 'tokens' column, got: {col_names:?}"
        );
    }

    #[tokio::test]
    async fn no_keyspace_returns_invalid() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        assert!(route(&state, &ctx, stmt).await.is_err());
    }

    #[tokio::test]
    async fn batch_too_large_rejected() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        // Build a batch statement with > 500 entries programmatically
        let stmts: Vec<Statement> = (0..501)
            .map(|i| {
                Statement::Insert(InsertStatement {
                    keyspace: Some("ks".into()),
                    table: "t".into(),
                    columns: vec!["k".into()],
                    values: vec![Term::IntegerLiteral(i)],
                    if_not_exists: false,
                    using_timestamp: None,
                    using_ttl: None,
                })
            })
            .collect();
        let batch = Statement::Batch(BatchStatement {
            batch_type: BatchType::Unlogged,
            statements: stmts,
            using_timestamp: None,
        });
        assert!(route(&state, &ctx, batch).await.is_err());
    }

    #[tokio::test]
    async fn select_system_peers_empty() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn internal_client_sees_internal_system_peers_endpoint() {
        use ferrosa_cluster::raft::{NodeInfo, NodeState};
        use ferrosa_cluster::ring::TokenRing;

        let (mut state, _dir) = setup();
        state.topology_policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();

        let local_id = 1_u64;
        let peer_id = 2_u64;
        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.48:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19042".to_string()),
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.49:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        state
            .cluster_state
            .store(Arc::new(ferrosa_cluster::ClusterStateHolder::Cluster(
                ferrosa_cluster::RaftClusterState::new(
                    Arc::new(ArcSwap::from_pointee(ring)),
                    local_id,
                ),
            )));

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "10.89.1.60:32123".into(),
        };
        let stmt =
            crate::parser::parse("SELECT native_address, native_port FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "10.89.1.49".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 9042);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn client_that_looks_like_local_container_ip_still_gets_public_system_peers_endpoint() {
        use ferrosa_cluster::raft::{NodeInfo, NodeState};
        use ferrosa_cluster::ring::TokenRing;

        let (mut state, _dir) = setup();
        state.topology_policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();
        let mut node_config = (*state.node_config).clone();
        node_config.internal_rpc_address = "10.89.1.48".parse().unwrap();
        node_config.listen_address = "10.89.1.48".parse().unwrap();
        node_config.broadcast_address = "10.89.1.48".parse().unwrap();
        state.node_config = Arc::new(node_config);

        let local_id = 1_u64;
        let peer_id = 2_u64;
        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.48:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19042".to_string()),
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.49:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        state
            .cluster_state
            .store(Arc::new(ferrosa_cluster::ClusterStateHolder::Cluster(
                ferrosa_cluster::RaftClusterState::new(
                    Arc::new(ArcSwap::from_pointee(ring)),
                    local_id,
                ),
            )));

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "10.89.1.48:32123".into(),
        };
        let stmt =
            crate::parser::parse("SELECT native_address, native_port FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 19043);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn ipv6_loopback_client_sees_ipv6_system_peers_endpoints() {
        use ferrosa_cluster::raft::{NodeInfo, NodeState};
        use ferrosa_cluster::ring::TokenRing;

        let (state, _dir) = setup();

        let local_id = 1_u64;
        let peer_id = 2_u64;
        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.48:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19042".to_string()),
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: uuid::Uuid::new_v4(),
                addr: "10.89.1.49:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        state
            .cluster_state
            .store(Arc::new(ferrosa_cluster::ClusterStateHolder::Cluster(
                ferrosa_cluster::RaftClusterState::new(
                    Arc::new(ArcSwap::from_pointee(ring)),
                    local_id,
                ),
            )));

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: "[::1]:32123".into(),
        };
        let stmt =
            crate::parser::parse("SELECT native_address, native_port FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1);
                let row = extract_single_row_cells(b, 2);
                assert_eq!(
                    decode_inet_cell(row[0].as_deref().unwrap()),
                    "::1".parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(decode_int_cell(row[1].as_deref().unwrap()), 19043);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_system_schema_keyspaces() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system_schema.keyspaces").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    /// Step 4 (dogfood system_schema.indexes): `CREATE INDEX` then
    /// `SELECT * FROM system_schema.indexes` must return the index — served
    /// from the persisted `system_schema.indexes` storage table, not the
    /// retired virtual table or the in-memory Registry computation.
    #[tokio::test]
    async fn select_system_schema_indexes_reads_from_storage() {
        let (state, _dir) = setup();
        // Boot order parity: system_schema.* tables are registered with the
        // engine before any DDL runs (real boot does this at engine startup).
        state.engine.register_system_tables().unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE sysidx WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE sysidx.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE INDEX sysidx_email ON sysidx.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT * — the row must come from the stored table.
        let stmt = crate::parser::parse("SELECT * FROM system_schema.indexes").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "CREATE INDEX must persist exactly one system_schema.indexes row"
        );

        // WHERE filter on the clustering index_name must still match.
        let stmt = crate::parser::parse(
            "SELECT * FROM system_schema.indexes WHERE index_name = 'sysidx_email'",
        )
        .unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "WHERE index_name filter must match the persisted row"
        );
    }

    /// Proves the `system_schema.indexes` read path is backed by *storage*, not
    /// the in-memory Registry: a row written directly to the stored table (with
    /// no corresponding Registry entry) is still returned by the SELECT.
    #[tokio::test]
    async fn system_schema_indexes_served_from_storage_not_registry() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Write a system_schema.indexes row straight to storage, bypassing the
        // Registry entirely.
        let idx = ferrosa_schema::IndexMetadata {
            keyspace: "ghost_ks".to_string(),
            table: "ghost_tbl".to_string(),
            name: "ghost_idx".to_string(),
            index_type: ferrosa_index::IndexType::BTree,
            target_columns: vec!["col".to_string()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        };
        let row = ferrosa_schema::system::persistence::index_to_rows(&idx);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tid = TableId::new("system_schema", "indexes");
        state.engine.write(&tid, &row.key, row.row, ts).unwrap();

        // The Registry has no such index, yet the SELECT returns it.
        assert!(
            !state.schema.snapshot().indexes.contains_key(&(
                "ghost_ks".into(),
                "ghost_tbl".into(),
                "ghost_idx".into()
            )),
            "precondition: Registry must NOT contain the ghost index"
        );

        let stmt = crate::parser::parse(
            "SELECT * FROM system_schema.indexes WHERE keyspace_name = 'ghost_ks'",
        )
        .unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "row written only to storage must be returned by the SELECT"
        );
    }

    /// Dogfooding parity: `CREATE TYPE` persists a `system_schema.types` row
    /// and `SELECT * FROM system_schema.types` is served from that stored row
    /// (not the in-memory Registry or the retired virtual table).
    #[tokio::test]
    async fn select_system_schema_types_reads_from_storage() {
        let (state, _dir) = setup();
        // Boot order parity: system_schema.* tables registered before any DDL.
        state.engine.register_system_tables().unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE systypes WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TYPE systypes.address (street text, zip int)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT * — the row must come from the stored table.
        let stmt = crate::parser::parse("SELECT * FROM system_schema.types").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "CREATE TYPE must persist exactly one system_schema.types row"
        );
    }

    /// Proves the `system_schema.types` read path is backed by *storage*, not
    /// the in-memory Registry: a row written directly to the stored table (with
    /// no corresponding Registry entry) is still returned by the SELECT, and a
    /// DROP-TYPE tombstone removes it.
    #[tokio::test]
    async fn system_schema_types_served_from_storage_not_registry() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Write a system_schema.types row straight to storage, bypassing the
        // Registry entirely.
        let udt = ferrosa_schema::UserTypeMetadata {
            keyspace: "ghost_ks".to_string(),
            name: "ghost_type".to_string(),
            fields: vec![("f".to_string(), CqlType::Int)],
        };
        let row = ferrosa_schema::system::persistence::type_to_row(&udt);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tid = TableId::new("system_schema", "types");
        state.engine.write(&tid, &row.key, row.row, ts).unwrap();

        // The Registry has no such type, yet the SELECT returns it.
        assert!(
            !state
                .schema
                .snapshot()
                .types
                .contains_key(&("ghost_ks".into(), "ghost_type".into())),
            "precondition: Registry must NOT contain the ghost type"
        );

        let stmt = crate::parser::parse("SELECT * FROM system_schema.types").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "row written only to storage must be returned by the SELECT"
        );
    }

    /// Dogfooding parity: a `system_schema.functions` row written straight to
    /// storage (bypassing the Registry) is returned by
    /// `SELECT * FROM system_schema.functions`, proving the read path is
    /// storage-backed (replacing the old hardcoded-empty arm), and a
    /// DROP-FUNCTION tombstone removes it.
    #[tokio::test]
    async fn system_schema_functions_served_from_storage_not_registry() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Write a function row straight to storage, bypassing the Registry.
        let func = ferrosa_schema::UserFunctionMetadata {
            keyspace: "ghost_ks".to_string(),
            name: "ghost_fn".to_string(),
            arg_names: vec!["v".to_string()],
            arg_types: vec![CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: true,
            language: "wasm".to_string(),
            body: "ab".to_string(),
        };
        let row = ferrosa_schema::system::persistence::function_to_row(&func);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tid = TableId::new("system_schema", "functions");
        state.engine.write(&tid, &row.key, row.row, ts).unwrap();

        // The Registry has no such function, yet the SELECT returns it.
        assert!(
            !state.schema.snapshot().functions.contains_key(&(
                "ghost_ks".into(),
                "ghost_fn".into(),
                vec![CqlType::Int]
            )),
            "precondition: Registry must NOT contain the ghost function"
        );

        let stmt = crate::parser::parse("SELECT * FROM system_schema.functions").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            1,
            "row written only to storage must be returned by the SELECT"
        );

        // Tombstone the function (DROP FUNCTION) and confirm it disappears.
        let ts2 = ts + 1;
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"ghost_ks".to_vec(),
        ));
        let tombstone = ferrosa_sstable::types::Row {
            clustering: ferrosa_schema::system::persistence::function_clustering(
                "ghost_fn",
                &[CqlType::Int],
            ),
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::new(ts2, (ts2 / 1_000_000) as u32),
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::NONE,
        };
        state.engine.write(&tid, &key, tombstone, ts2).unwrap();

        let stmt = crate::parser::parse("SELECT * FROM system_schema.functions").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result");
        };
        assert_eq!(
            extract_row_count(&body),
            0,
            "dropped function must not appear after tombstone"
        );
    }

    /// Regression: PR#21 (Gap 7) hardcoded the 10-col Cassandra-5.0 shape
    /// for system_schema.views to satisfy the DataStax driver's `ViewParser`
    /// boolean lookups.  The scylla 0.15 driver issues
    /// `SELECT keyspace_name, view_name, base_table_name FROM
    /// system_schema.views` and tuple-type-checks the result; ferrosa was
    /// returning 10 columns and the driver rejected metadata with
    /// "statement operates on 10 columns, but given rust types contains 3".
    ///
    /// Post-fix the views arm honors SELECT projection (3 cols requested →
    /// 3 cols back) while still returning the full canonical 10-col shape
    /// for `SELECT *`.
    #[tokio::test]
    async fn select_system_schema_views_honors_projection() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // SELECT * → 10 columns (DataStax/NoSQLBench path).
        let stmt = crate::parser::parse("SELECT * FROM system_schema.views").unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result for SELECT *");
        };
        assert_eq!(&body[0..4], &0x0002i32.to_be_bytes());
        // [0..4]=kind, [4..8]=flags, [8..12]=col_count
        let col_count = i32::from_be_bytes(body[8..12].try_into().unwrap());
        assert_eq!(
            col_count, 10,
            "SELECT * must return all 10 canonical columns"
        );

        // SELECT specific cols → only those columns (scylla path).
        let stmt = crate::parser::parse(
            "SELECT keyspace_name, view_name, base_table_name FROM system_schema.views",
        )
        .unwrap();
        let RouteResult::Result(body) = route(&state, &ctx, stmt).await.unwrap() else {
            panic!("expected Rows result for SELECT-projected");
        };
        assert_eq!(&body[0..4], &0x0002i32.to_be_bytes());
        let col_count = i32::from_be_bytes(body[8..12].try_into().unwrap());
        assert_eq!(
            col_count, 3,
            "SELECT-projected must return only requested columns",
        );
    }

    /// Gap 2 (ferrosa-nosqlbench/docs/initial-gaps-found.md): the DataStax
    /// Java Driver 4.x queries all three system_virtual_schema tables
    /// during connection bring-up.  Pre-fix, ferrosa returned
    /// "table not found" and the driver gave up on every host; post-fix
    /// each query must succeed with an empty-but-typed result.
    #[tokio::test]
    async fn select_system_virtual_schema_keyspaces_returns_empty_ok() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for table in ["keyspaces", "tables", "columns"] {
            let q = format!("SELECT * FROM system_virtual_schema.{table}");
            let stmt = crate::parser::parse(&q).unwrap();
            let result = route(&state, &ctx, stmt)
                .await
                .unwrap_or_else(|e| panic!("system_virtual_schema.{table} returned error: {e:?}"));
            match &result {
                // Rows kind (0x0002) — schema-typed empty result.
                RouteResult::Result(b) => assert_eq!(
                    &b[0..4],
                    &0x0002i32.to_be_bytes(),
                    "system_virtual_schema.{table} should return Rows result",
                ),
                _ => panic!("expected RouteResult::Result for system_virtual_schema.{table}"),
            }
        }
    }

    #[test]
    fn cql_type_name_to_string_simple() {
        assert_eq!(
            cql_type_name_to_string(&CqlTypeName::Simple("text".into())),
            "text"
        );
    }

    #[test]
    fn cql_type_name_to_string_collection() {
        assert_eq!(
            cql_type_name_to_string(&CqlTypeName::Map(
                Box::new(CqlTypeName::Simple("text".into())),
                Box::new(CqlTypeName::Simple("int".into())),
            )),
            "map<text, int>"
        );
    }

    #[test]
    fn parse_permissions_all() {
        let perms = parse_permissions(&["ALL".into()]).unwrap();
        assert!(perms.contains(&Permission::Select));
        assert!(perms.contains(&Permission::Modify));
        assert!(perms.contains(&Permission::Create));
        assert!(perms.contains(&Permission::Drop));
        assert!(perms.contains(&Permission::Alter));
        assert!(perms.contains(&Permission::Authorize));
    }

    #[test]
    fn parse_permissions_single() {
        let perms = parse_permissions(&["SELECT".into()]).unwrap();
        assert_eq!(perms.len(), 1);
        assert!(perms.contains(&Permission::Select));
    }

    #[test]
    fn parse_permissions_unknown_rejected() {
        assert!(parse_permissions(&["BANANA".into()]).is_err());
    }

    #[tokio::test]
    async fn router_tracks_query_count() {
        let (state, _dir) = setup();
        assert_eq!(state.query_tracker.total_executed(), 0);
        // Execute a simple query
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let _ = route(&state, &ctx, stmt).await;
        assert_eq!(state.query_tracker.total_executed(), 1);
    }

    #[tokio::test]
    async fn update_rrd_runtime_settings_virtual_table_adjusts_budget() {
        let (state, _dir) = setup();
        let settings = Arc::new(ferrosa_storage::timeseries::TimeSeriesRuntimeSettings::new(
            Some(1024),
            100,
        ));
        state.schema.virtual_tables().register(Arc::new(
            crate::virtual_tables::RrdRuntimeSettingsTable::new(Arc::clone(&settings)),
        ));
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "UPDATE system_observability.rrd_runtime_settings \
             SET setting_value = 4096 \
             WHERE setting_name = 'ring_memory_budget_bytes'",
        )
        .unwrap();

        route(&state, &ctx, stmt).await.unwrap();

        assert_eq!(settings.ring_memory_budget_bytes(), Some(4096));
    }

    #[tokio::test]
    async fn update_rrd_runtime_settings_requires_superuser() {
        let (state, _dir) = setup();
        let settings = Arc::new(ferrosa_storage::timeseries::TimeSeriesRuntimeSettings::new(
            Some(1024),
            100,
        ));
        state.schema.virtual_tables().register(Arc::new(
            crate::virtual_tables::RrdRuntimeSettingsTable::new(settings),
        ));
        let auth = AuthContext {
            role: "operator".into(),
            is_superuser: false,
            must_change_password: false,
        };
        let ctx = RequestContext {
            auth: &auth,
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "UPDATE system_observability.rrd_runtime_settings \
             SET setting_value = 4096 \
             WHERE setting_name = 'ring_memory_budget_bytes'",
        )
        .unwrap();

        let err = match route(&state, &ctx, stmt).await {
            Ok(_) => panic!("non-superuser virtual setting update should fail"),
            Err(err) => err,
        };

        assert!(matches!(err, CqlError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn truncate_returns_void() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace and table first
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE ks.t (k int PRIMARY KEY)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("TRUNCATE ks.t").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn compact_returns_clear_unsupported_error() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse("COMPACT ks.t").unwrap();
        let err = match route(&state, &ctx, stmt).await {
            Ok(_) => panic!("COMPACT should return an unsupported error"),
            Err(err) => err,
        };

        assert!(
            matches!(err, CqlError::Invalid(ref message) if message.contains("COMPACT is not supported for ks.t")),
            "expected clear COMPACT unsupported error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn select_virtual_table_routes_through_registry() {
        use ferrosa_common::{CellValue, DataType};
        use ferrosa_schema::{
            RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
        };

        /// Stub virtual table that returns one row with two columns.
        struct StubVTable;

        impl VirtualTable for StubVTable {
            fn name(&self) -> &str {
                "test_vtable"
            }

            fn keyspace(&self) -> &str {
                "test_ks"
            }

            fn columns(&self) -> &[VirtualColumnDef] {
                // Use a leaked slice so we can return &[VirtualColumnDef]
                // from a trait method without self-referential lifetime issues.
                static COLS: std::sync::OnceLock<Vec<VirtualColumnDef>> =
                    std::sync::OnceLock::new();
                COLS.get_or_init(|| {
                    vec![
                        VirtualColumnDef {
                            name: "name".into(),
                            data_type: DataType::Text,
                        },
                        VirtualColumnDef {
                            name: "value".into(),
                            data_type: DataType::Int,
                        },
                    ]
                })
            }

            fn primary_key_columns(&self) -> &[usize] {
                &[0]
            }

            fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
                vec![VirtualRow {
                    cells: vec![
                        CellValue::live(b"hello".to_vec(), 0),
                        CellValue::live(42i32.to_be_bytes().to_vec(), 0),
                    ],
                }]
            }

            fn subscription_mode(&self) -> SubscriptionMode {
                SubscriptionMode::Pollable
            }
        }

        let (state, _dir) = setup();

        // Register the stub virtual table in the schema's registry.
        state.schema.virtual_tables().register(Arc::new(StubVTable));

        // Create the keyspace so the parser doesn't complain about qualified name.
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse("SELECT * FROM test_ks.test_vtable").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // Should be a Rows result (kind = 0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
                // The result should contain data (more than just the header)
                assert!(b.len() > 4);
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_virtual_table_uses_streaming_visit_rows() {
        use ferrosa_common::{CellValue, DataType};
        use ferrosa_schema::{
            RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
        };

        struct StreamingOnlyVTable;

        impl VirtualTable for StreamingOnlyVTable {
            fn name(&self) -> &str {
                "streaming_vtable"
            }

            fn keyspace(&self) -> &str {
                "test_ks"
            }

            fn columns(&self) -> &[VirtualColumnDef] {
                static COLS: std::sync::OnceLock<Vec<VirtualColumnDef>> =
                    std::sync::OnceLock::new();
                COLS.get_or_init(|| {
                    vec![VirtualColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                    }]
                })
            }

            fn primary_key_columns(&self) -> &[usize] {
                &[0]
            }

            fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
                panic!("virtual table route must not require Vec materialization")
            }

            fn visit_rows(&self, _: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
                visit(VirtualRow {
                    cells: vec![CellValue::live(b"streamed".to_vec(), 0)],
                });
            }

            fn subscription_mode(&self) -> SubscriptionMode {
                SubscriptionMode::Pollable
            }
        }

        let (state, _dir) = setup();
        state
            .schema
            .virtual_tables()
            .register(Arc::new(StreamingOnlyVTable));

        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse("SELECT * FROM test_ks.streaming_vtable").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    // ---------------------------------------------------------------
    // resolve_index_type tests
    // ---------------------------------------------------------------

    #[test]
    fn resolve_index_type_defaults_to_btree() {
        let result = resolve_index_type(None, &["col".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::BTree);
    }

    #[test]
    fn resolve_index_type_phonetic() {
        let result = resolve_index_type(Some("phonetic"), &["name".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Phonetic);
    }

    #[test]
    fn resolve_index_type_fulltext() {
        let result = resolve_index_type(Some("fulltext"), &["body".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::FullText);
    }

    #[test]
    fn resolve_index_type_unknown_errors() {
        let result = resolve_index_type(Some("nonexistent"), &["col".to_string()], &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn resolve_index_type_hash() {
        let result = resolve_index_type(Some("hash"), &["user_id".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Hash);
    }

    #[test]
    fn resolve_index_type_btree_explicit() {
        let result = resolve_index_type(Some("btree"), &["col".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::BTree);
    }

    #[test]
    fn resolve_index_type_composite() {
        let result = resolve_index_type(
            Some("composite"),
            &["a".to_string(), "b".to_string()],
            &HashMap::new(),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Composite);
    }

    #[tokio::test]
    async fn create_hash_index_stores_correct_type_in_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE hashks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create table
        let stmt =
            crate::parser::parse("CREATE TABLE hashks.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE INDEX USING 'hash'
        let stmt = crate::parser::parse(
            "CREATE INDEX idx_email_hash ON hashks.users (email) USING 'hash'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE INDEX USING 'hash' should succeed, got: {:?}",
            result.err()
        );

        // Verify the index is stored with IndexType::Hash
        let snap = state.schema.snapshot();
        let key = (
            "hashks".to_string(),
            "users".to_string(),
            "idx_email_hash".to_string(),
        );
        let idx = snap.indexes.get(&key);
        assert!(idx.is_some(), "index should be registered in schema");
        let idx = idx.unwrap();
        assert!(
            matches!(idx.index_type, IndexType::Hash),
            "expected IndexType::Hash, got {:?}",
            idx.index_type
        );
        assert_eq!(idx.target_columns, vec!["email"]);
    }

    #[tokio::test]
    async fn hash_index_eq_predicate_uses_single_index_plan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + table + hash index
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE hashplan WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE hashplan.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE INDEX idx_hp_email ON hashplan.users (email) USING 'hash'",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // INSERT a row
        let stmt = crate::parser::parse(
            "INSERT INTO hashplan.users (id, email) VALUES (1, 'alice@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT WHERE email = ? — hash index Eq lookup must succeed (not require ALLOW FILTERING)
        let stmt =
            crate::parser::parse("SELECT * FROM hashplan.users WHERE email = 'alice@example.com'")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "SELECT WHERE indexed_col = ? should succeed with hash index, got: {:?}",
            result.err()
        );
    }

    // ── UDT DDL routing tests ──────────────────────────────────────────

    #[tokio::test]
    async fn route_create_type_stores_in_schema() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace first
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE ks.address (street text, city text)
        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // SchemaChange kind = 0x0005
                assert_eq!(&b[0..4], &0x0005i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }

        // Verify type exists in schema
        let udt = state.schema.get_type("ks", "address");
        assert!(
            udt.is_some(),
            "type should exist in schema after CREATE TYPE"
        );
        let udt = udt.unwrap();
        assert_eq!(udt.fields.len(), 2);
        assert_eq!(udt.fields[0].0, "street");
        assert_eq!(udt.fields[1].0, "city");
    }

    #[tokio::test]
    async fn route_drop_type() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        assert!(state.schema.get_type("ks", "address").is_some());

        // DROP TYPE
        let stmt = crate::parser::parse("DROP TYPE ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0005i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }

        assert!(
            state.schema.get_type("ks", "address").is_none(),
            "type should be gone after DROP TYPE"
        );
    }

    #[tokio::test]
    async fn route_create_type_if_not_exists() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE IF NOT EXISTS with same name — should succeed without error
        let stmt =
            crate::parser::parse("CREATE TYPE IF NOT EXISTS ks.address (street text, city text)")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF NOT EXISTS should not error on duplicate"
        );
    }

    #[tokio::test]
    async fn route_drop_type_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP TYPE IF EXISTS on non-existent type — should succeed
        let stmt = crate::parser::parse("DROP TYPE IF EXISTS ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "IF EXISTS should not error on missing type");
    }

    #[tokio::test]
    async fn route_alter_type_add_field() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER TYPE ADD city text
        let stmt = crate::parser::parse("ALTER TYPE ks.address ADD city text").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "ALTER TYPE ADD should succeed");

        let udt = state.schema.get_type("ks", "address").unwrap();
        assert_eq!(udt.fields.len(), 2);
        assert_eq!(udt.fields[1].0, "city");
    }

    #[tokio::test]
    async fn route_alter_type_rename_field() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER TYPE RENAME street TO street_name
        let stmt =
            crate::parser::parse("ALTER TYPE ks.address RENAME street TO street_name").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "ALTER TYPE RENAME should succeed");

        let udt = state.schema.get_type("ks", "address").unwrap();
        assert_eq!(udt.fields[0].0, "street_name");
    }

    #[tokio::test]
    async fn route_create_type_uses_session_keyspace() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ks = Some("ks".to_string());
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace first (with explicit ks in statement)
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE without explicit keyspace — should use session keyspace
        let stmt = crate::parser::parse("CREATE TYPE address (street text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "should use session keyspace");

        let udt = state.schema.get_type("ks", "address");
        assert!(udt.is_some(), "type should be in session keyspace");
    }

    #[tokio::test]
    async fn route_create_type_no_keyspace_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // CREATE TYPE without keyspace and no session keyspace
        let stmt = crate::parser::parse("CREATE TYPE address (street text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_err(), "should error without keyspace");
    }

    #[tokio::test]
    async fn route_create_type_duplicate_without_if_not_exists() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE again without IF NOT EXISTS — should return an error
        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "duplicate CREATE TYPE without IF NOT EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_drop_type_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace but no type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP TYPE on non-existent type without IF EXISTS — should return an error
        let stmt = crate::parser::parse("DROP TYPE ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP TYPE on non-existent type without IF EXISTS should error"
        );
    }

    // ── UDF/UDA DDL routing tests ──────────────────────────────────────

    #[tokio::test]
    async fn route_create_function_rejects_non_wasm_language() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS '0061736d'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("unsupported UDF language"),
                    "error should mention unsupported language, got: {err_msg}"
                );
            }
            Ok(_) => panic!("non-wasm language should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_rejects_invalid_hex() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'ZZZZ'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("invalid hex"),
                    "error should mention invalid hex, got: {err_msg}"
                );
            }
            Ok(_) => panic!("invalid hex should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_rejects_invalid_wasm() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Valid hex but not valid WASM
        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'deadbeef'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("compilation failed"),
                    "error should mention compilation failure, got: {err_msg}"
                );
            }
            Ok(_) => panic!("invalid wasm should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_no_keyspace_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'deadbeef'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_err(), "should error without keyspace");
    }

    #[tokio::test]
    async fn route_drop_function_if_exists_nonexistent() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP FUNCTION IF EXISTS on non-existent function — should succeed
        let stmt = crate::parser::parse("DROP FUNCTION IF EXISTS ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF EXISTS should not error on missing function"
        );
    }

    #[tokio::test]
    async fn route_drop_function_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP FUNCTION without IF EXISTS on non-existent function — should error
        let stmt = crate::parser::parse("DROP FUNCTION ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP FUNCTION on non-existent function without IF EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_drop_aggregate_if_exists_nonexistent() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP AGGREGATE IF EXISTS on non-existent aggregate — should succeed
        let stmt = crate::parser::parse("DROP AGGREGATE IF EXISTS ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF EXISTS should not error on missing aggregate"
        );
    }

    #[tokio::test]
    async fn route_drop_aggregate_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP AGGREGATE without IF EXISTS on non-existent — should error
        let stmt = crate::parser::parse("DROP AGGREGATE ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP AGGREGATE on non-existent without IF EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_hex_decode_odd_length() {
        let result = super::hex_decode("abc");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn route_hex_decode_valid() {
        let result = super::hex_decode("deadbeef").unwrap();
        assert_eq!(result, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    // ── Helper: extract row count from a Rows result body ────────────

    /// Parse a Rows RESULT body and return the row count.
    /// Wire format: [i32 kind][i32 flags][i32 col_count][string ks][string table]
    ///              per-column: [string name][u16 type_id][type params]
    ///              [i32 row_count]
    fn extract_row_count(buf: &[u8]) -> i32 {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        // Skip keyspace string
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // Skip table string
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        // Skip column specs
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2, // List/Set: element type_id
                0x0021 => off += 4,          // Map: key + val type_ids
                0x0031 => {
                    // Tuple: [u16 n][type_id * n]
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        // row_count
        i32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
    }

    /// Decode a `SELECT id, name` Rows result into `(i32, String)` pairs in
    /// result order. Assumes the projection is exactly `(int, text)`.
    fn decode_id_name_rows(buf: &[u8]) -> Vec<(i32, String)> {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            // Column 0: id int (4 bytes)
            let id_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            assert_eq!(id_len, 4, "id column must be a 4-byte int");
            let id = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            // Column 1: name text (variable)
            let name_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            let name = if name_len < 0 {
                String::new()
            } else {
                let n = name_len as usize;
                let s = String::from_utf8_lossy(&buf[off..off + n]).into_owned();
                off += n;
                s
            };
            rows.push((id, name));
        }
        rows
    }

    fn extract_first_bigint_value(buf: &[u8]) -> i64 {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
        assert_eq!(row_count, 1, "expected exactly one aggregate row");
        off += 4;
        let value_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(value_len, 8, "expected bigint value length");
        i64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
    }

    fn extract_first_double_value(buf: &[u8]) -> f64 {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
        assert_eq!(row_count, 1, "expected exactly one aggregate row");
        off += 4;
        let value_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(value_len, 8, "expected double value length");
        f64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
    }

    fn example_cql_statements(script: &str) -> Vec<String> {
        script
            .lines()
            .map(|line| line.split_once("--").map_or(line, |(cql, _)| cql))
            .collect::<Vec<_>>()
            .join("\n")
            .split(';')
            .map(str::trim)
            .filter(|stmt| !stmt.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    /// Extract column names from a Rows result buffer.
    fn extract_column_names(buf: &[u8]) -> Vec<String> {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        // Skip keyspace string
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // Skip table string
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        // Read column names
        let mut names = Vec::new();
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let name = std::str::from_utf8(&buf[off..off + name_len])
                .unwrap()
                .to_string();
            names.push(name);
            off += name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        names
    }

    /// Extract the column count from a Rows result buffer.
    fn extract_column_count(buf: &[u8]) -> usize {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize
    }

    /// Extract the first row's raw cell payloads from a Rows result buffer.
    fn extract_single_row_cells(buf: &[u8], expected_cols: usize) -> Vec<Option<Vec<u8>>> {
        assert_eq!(extract_row_count(buf), 1, "expected exactly one row");

        let col_count = extract_column_count(buf);
        assert_eq!(col_count, expected_cols, "unexpected column count");

        let mut off = 12;
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }

        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        assert_eq!(row_count, 1, "expected exactly one row");
        off += 4;

        let mut cells = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            if len < 0 {
                cells.push(None);
                continue;
            }
            let len = len as usize;
            cells.push(Some(buf[off..off + len].to_vec()));
            off += len;
        }
        cells
    }

    fn decode_int_cell(cell: &[u8]) -> i32 {
        i32::from_be_bytes(cell.try_into().unwrap())
    }

    fn decode_inet_cell(cell: &[u8]) -> std::net::IpAddr {
        match cell.len() {
            4 => std::net::IpAddr::V4(std::net::Ipv4Addr::new(cell[0], cell[1], cell[2], cell[3])),
            16 => std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                <[u8; 16]>::try_from(cell).unwrap(),
            )),
            len => panic!("unexpected inet length: {len}"),
        }
    }

    // ── gocql / Temporal compatibility ──────────────────────────────────

    /// gocql schema agreement: SELECT schema_version FROM system.local WHERE key='local'
    /// Must return exactly 1 column (schema_version), not all 16.
    #[tokio::test]
    async fn gocql_select_schema_version_from_system_local() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("SELECT schema_version FROM system.local WHERE key = 'local'")
                .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                let col_names = extract_column_names(b);
                assert_eq!(
                    col_names,
                    vec!["schema_version"],
                    "gocql schema agreement expects exactly 1 column, got: {col_names:?}"
                );
                assert_eq!(extract_row_count(b), 1, "system.local must return 1 row");
            }
            _ => panic!("expected Result"),
        }
    }

    /// gocql host discovery: SELECT * FROM system.local WHERE key='local'
    /// Must return all columns and 1 row (WHERE clause must not break it).
    #[tokio::test]
    async fn gocql_select_star_from_system_local_where_key() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local WHERE key = 'local'").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                let col_count = extract_column_count(b);
                assert_eq!(col_count, 16, "SELECT * should return all 16 columns");
                assert_eq!(extract_row_count(b), 1);
            }
            _ => panic!("expected Result"),
        }
    }

    /// gocql peer discovery: SELECT * FROM system.peers_v2 (empty on single node)
    /// Must succeed with 0 rows, not error.
    #[tokio::test]
    async fn gocql_select_star_from_system_peers_v2() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.peers_v2").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                let col_names = extract_column_names(b);
                assert!(
                    col_names.contains(&"schema_version".to_string()),
                    "system.peers_v2 must include schema_version column"
                );
                assert_eq!(extract_row_count(b), 0, "single node has no peers");
            }
            _ => panic!("expected Result"),
        }
    }

    /// Selecting specific columns from system.local must filter the result.
    #[tokio::test]
    async fn system_local_column_filtering() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("SELECT cluster_name, release_version FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                let col_names = extract_column_names(b);
                assert_eq!(
                    col_names,
                    vec!["cluster_name", "release_version"],
                    "should return only requested columns"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Selecting specific columns from system.peers must filter the result.
    #[tokio::test]
    async fn system_peers_column_filtering() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("SELECT peer, host_id, schema_version FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                let col_names = extract_column_names(b);
                assert_eq!(
                    col_names,
                    vec!["peer", "host_id", "schema_version"],
                    "should return only requested columns"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Temporal schema migrations use CREATE TYPE for UDTs.
    #[tokio::test]
    async fn create_type_udt() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE temporal WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("temporal".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TYPE temporal.serialized_event_batch (encoding_type text, version int, data blob)",
        ).unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE TYPE must succeed, got: {:?}",
            result.err()
        );
    }

    /// Temporal schema migrations use ALTER TABLE ADD.
    #[tokio::test]
    async fn alter_table_add_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("CREATE TABLE ks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse("ALTER TABLE ks.t ADD new_col blob").unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "ALTER TABLE ADD must succeed, got: {:?}",
            result.err()
        );

        // Verify the column exists by inserting into it
        let stmt = crate::parser::parse(
            "INSERT INTO ks.t (k, v, new_col) VALUES (1, 'hello', 0xdeadbeef)",
        )
        .unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "INSERT into added column must succeed, got: {:?}",
            result.err()
        );
    }

    /// Direct repro for ferrosa-memory PR#4 cluster-int third-layer
    /// failure: `live_run_three_step_scenario` panicked the test-cluster
    /// node1 at flush time with
    ///   "SSTable writer: cell col_idx 3 is out of range (num_columns=3)"
    /// because `route_alter_table`'s Direct arm only updated the Schema
    /// registry — never `engine.update_table_schema`. The encoder used
    /// the post-ALTER column set (via Schema metadata) and produced a
    /// cell at the new column's index, but the storage TableSchema was
    /// still stuck at the CREATE-TABLE column count. The mismatch goes
    /// undetected on the write (memtable validation skips out-of-range
    /// indices) and only fails loud at flush time, dropping the row.
    ///
    /// Without the fix in `route_alter_table` this test panics at the
    /// `engine.flush` call. With the fix it succeeds.
    #[tokio::test]
    async fn alter_table_direct_propagates_schema_to_storage_for_flush() {
        let (state, dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("CREATE TABLE ks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse("ALTER TABLE ks.t ADD new_col text").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("INSERT INTO ks.t (k, v, new_col) VALUES (1, 'hello', 'world')")
                .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // The bug: storage TableSchema is stuck at CREATE-time num_columns=1
        // (just `v`). The INSERT for `new_col` writes a cell at col_idx=1.
        // memtable validation accepts it (silently skipped on out-of-range
        // index), but flush panics at writer.rs:496. With the fix in
        // route_alter_table, this flush completes cleanly.
        let table_id = ferrosa_storage::TableId::new("ks", "t");
        state
            .engine
            .flush(&table_id)
            .expect("flush after ALTER + INSERT must not panic; storage TableSchema must reflect post-ALTER column count");

        // Belt and suspenders: re-issue another ALTER + INSERT + flush so
        // we catch the regression even if some lazy refresh path
        // accidentally covered the first ALTER.
        let stmt = crate::parser::parse("ALTER TABLE ks.t ADD third_col text").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO ks.t (k, v, new_col, third_col) VALUES (2, 'h2', 'w2', 't2')",
        )
        .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        state
            .engine
            .flush(&table_id)
            .expect("flush after second ALTER + INSERT must not panic");

        // Keep `dir` alive for the duration of the test so tempdir
        // doesn't drop and unlink the SSTable directory underneath us.
        drop(dir);
    }

    /// Temporal schema migrations use ALTER TABLE DROP.
    #[tokio::test]
    async fn alter_table_drop_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("CREATE TABLE ks.t (k int PRIMARY KEY, v text, extra blob)")
                .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse("ALTER TABLE ks.t DROP extra").unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "ALTER TABLE DROP must succeed, got: {:?}",
            result.err()
        );
    }

    /// PREPARE for a SELECT must include result column metadata so gocql's
    /// Scan() knows how many columns to expect.
    #[tokio::test]
    async fn prepare_select_includes_result_columns() {
        use bytes::{BufMut, BytesMut};
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        // Create keyspace and table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("CREATE TABLE ks.t (k text PRIMARY KEY, v text, n int)").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // Now test PREPARE — build a PREPARE frame body
        let query = "SELECT v, n FROM ks.t WHERE k = ?";
        let query_bytes = query.as_bytes();
        let mut body = BytesMut::new();
        body.put_i32(query_bytes.len() as i32);
        body.put_slice(query_bytes);

        let result = crate::connection::handle_prepare(
            &mut None,
            &mut Some("ks".into()),
            &state,
            &body.freeze(),
        )
        .await;
        match result {
            crate::connection::HandleResult::Reply(opcode, body) => {
                // PREPARED = 0x04 Result opcode
                assert_eq!(opcode, crate::frame::Opcode::Result);
                // Result body: [4 kind][id...][bind_metadata...][result_metadata]
                // kind = 0x0004 (Prepared)
                assert_eq!(&body[0..4], &0x0004i32.to_be_bytes());

                // Skip past the prepared ID (short_bytes: [u16 len][bytes])
                let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                let mut off = 6 + id_len;

                // Bind metadata: [i32 flags][i32 columns_count]
                let _bind_flags = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;
                let bind_col_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;

                // Skip pk_count if global_tables_spec flag is set
                if _bind_flags & 0x0001 != 0 {
                    // pk_count
                    let pk_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                    off += 4;
                    // pk indexes
                    off += pk_count as usize * 2;
                    // global table spec: ks string + table string
                    let ks_len =
                        u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + ks_len;
                    let tbl_len =
                        u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + tbl_len;
                }
                // Skip bind column specs
                for _ in 0..bind_col_count {
                    if _bind_flags & 0x0001 == 0 {
                        // per-column ks+table
                        let ks_len =
                            u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                        off += 2 + ks_len;
                        let tbl_len =
                            u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                        off += 2 + tbl_len;
                    }
                    let name_len =
                        u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + name_len;
                    off += 2; // type_id
                }

                // Result metadata: [i32 flags][i32 columns_count]
                let _result_flags = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;
                let result_col_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());

                assert_eq!(
                    result_col_count, 2,
                    "PREPARE for SELECT v, n should report 2 result columns, got {result_col_count}"
                );
            }
            _ => panic!("expected Reply"),
        }
    }

    /// Temporal's CREATE TABLE uses WITH COMPACTION = { map }.
    /// Parser must handle map-valued table options.
    #[tokio::test]
    async fn create_table_with_compaction_option() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE temporal WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("temporal".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE temporal.t (k int PRIMARY KEY) WITH COMPACTION = { 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy' }",
        );
        assert!(
            stmt.is_ok(),
            "CREATE TABLE WITH COMPACTION = {{map}} must parse, got: {:?}",
            stmt.err()
        );
        let result = route(&state, &ctx_ks, stmt.unwrap()).await;
        assert!(
            result.is_ok(),
            "CREATE TABLE WITH COMPACTION must succeed, got: {:?}",
            result.err()
        );
    }

    /// WP-005: CREATE TABLE WITH compaction = {map} must persist params
    /// into TableMetadata so strategy_for_table() can read them.
    #[tokio::test]
    async fn create_table_with_compaction_persists_params() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ucs WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE ucs.t (id int PRIMARY KEY, v text) \
             WITH compaction = {'class': 'UnifiedCompactionStrategy', 'fan_factor': '4'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify the compaction params are persisted in the schema
        let snap = state.schema.snapshot();
        let table = snap
            .tables
            .get(&("ucs".to_string(), "t".to_string()))
            .expect("table should exist");
        assert!(
            !table.params.compaction.is_empty(),
            "compaction params should be populated from DDL, got empty HashMap"
        );
        assert_eq!(
            table.params.compaction.get("class").map(|s| s.as_str()),
            Some("UnifiedCompactionStrategy"),
            "compaction class should be 'UnifiedCompactionStrategy'"
        );
        assert_eq!(
            table
                .params
                .compaction
                .get("fan_factor")
                .map(|s| s.as_str()),
            Some("4"),
            "fan_factor should be '4'"
        );
    }

    /// Temporal uses frozen<UDT> in collection types.
    #[tokio::test]
    async fn create_table_with_frozen_udt_collection() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE temporal WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("temporal".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create the UDT first
        let stmt = crate::parser::parse(
            "CREATE TYPE temporal.serialized_event_batch (encoding_type text, version int, data blob)",
        ).unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // Create table with frozen<UDT> in a list
        let stmt = crate::parser::parse(
            "CREATE TABLE temporal.executions (k int PRIMARY KEY, events list<frozen<serialized_event_batch>>)",
        );
        assert!(
            stmt.is_ok(),
            "frozen<UDT> in list must parse, got: {:?}",
            stmt.err()
        );
    }

    /// Temporal uses CLUSTERING ORDER BY in table definition.
    #[tokio::test]
    async fn create_table_with_clustering_order_and_compaction() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE temporal WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let _ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("temporal".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE TABLE temporal.history_node (\
                tree_id uuid, branch_id uuid, node_id bigint, txn_id bigint, \
                data blob, data_encoding text, \
                PRIMARY KEY ((tree_id), branch_id, node_id, txn_id)) \
            WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC) \
            AND COMPACTION = { 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy' }",
        );
        assert!(
            stmt.is_ok(),
            "CLUSTERING ORDER + COMPACTION must parse, got: {:?}",
            stmt.err()
        );
    }

    /// FRSA-BUG-024: PREPARE must see columns added by ALTER TABLE ADD.
    /// Temporal's SaveClusterMetadata fails because PREPARE only reports
    /// original table columns, not those added by ALTER TABLE.
    #[tokio::test]
    async fn prepare_sees_alter_table_add_columns() {
        use bytes::{BufMut, BytesMut};

        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace and table with 2 columns
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("CREATE TABLE ks.cluster_metadata (k int PRIMARY KEY, data blob)")
                .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // ALTER TABLE ADD 2 new columns
        let stmt =
            crate::parser::parse("ALTER TABLE ks.cluster_metadata ADD encoding text").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();
        let stmt =
            crate::parser::parse("ALTER TABLE ks.cluster_metadata ADD version bigint").unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // PREPARE an INSERT with all 4 columns (including the 2 added ones)
        let query =
            "INSERT INTO ks.cluster_metadata (k, data, encoding, version) VALUES (?, ?, ?, ?)";
        let query_bytes = query.as_bytes();
        let mut body = BytesMut::new();
        body.put_i32(query_bytes.len() as i32);
        body.put_slice(query_bytes);

        let result = crate::connection::handle_prepare(
            &mut None,
            &mut Some("ks".into()),
            &state,
            &body.freeze(),
        )
        .await;
        match result {
            crate::connection::HandleResult::Reply(opcode, body) => {
                assert_eq!(opcode, crate::frame::Opcode::Result);
                // kind = 0x0004 (Prepared)
                assert_eq!(&body[0..4], &0x0004i32.to_be_bytes());
                // Skip past prepared ID
                let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                let mut off = 6 + id_len;
                // Bind metadata: [i32 flags][i32 columns_count]
                let _bind_flags = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;
                let bind_col_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());

                assert_eq!(
                    bind_col_count, 4,
                    "PREPARE should see 4 bind columns (k, data, encoding, version) after ALTER TABLE ADD, got {bind_col_count}"
                );
            }
            _ => panic!("expected Reply"),
        }
    }

    /// FRSA-BUG-024: PREPARE for UPDATE...IF condition must include the
    /// condition's bind marker in the bind columns count.
    /// Temporal's SaveClusterMetadata uses: UPDATE ... SET ... WHERE ... IF version = ?
    #[tokio::test]
    async fn prepare_update_if_condition_includes_bind_marker() {
        use bytes::{BufMut, BytesMut};

        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE ks.meta (p int, name text, data blob, version bigint, PRIMARY KEY (p, name))",
        ).unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // PREPARE: UPDATE with IF condition containing a bind marker
        let query =
            "UPDATE ks.meta SET data = ?, version = ? WHERE p = ? AND name = ? IF version = ?";
        let query_bytes = query.as_bytes();
        let mut body = BytesMut::new();
        body.put_i32(query_bytes.len() as i32);
        body.put_slice(query_bytes);

        let result = crate::connection::handle_prepare(
            &mut None,
            &mut Some("ks".into()),
            &state,
            &body.freeze(),
        )
        .await;
        match result {
            crate::connection::HandleResult::Reply(opcode, body) => {
                assert_eq!(opcode, crate::frame::Opcode::Result);
                assert_eq!(&body[0..4], &0x0004i32.to_be_bytes());
                let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                let mut off = 6 + id_len;
                let _bind_flags = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;
                let bind_col_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());

                // 5 bind markers: data=?, version=? (SET) + p=?, name=? (WHERE) + version=? (IF)
                assert_eq!(
                    bind_col_count, 5,
                    "PREPARE should report 5 bind columns for UPDATE...IF version=?, got {bind_col_count}"
                );
            }
            _ => panic!("expected Reply"),
        }
    }

    // ── FRSA-BUG-026: INSERT IF NOT EXISTS with map column ────────────

    /// Temporal's queue_metadata: INSERT IF NOT EXISTS with a map column
    /// must not error on the existence check read-back.
    #[tokio::test]
    async fn insert_if_not_exists_with_map_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE ks.queue_metadata (queue_type int PRIMARY KEY, cluster_ack_level map<text, bigint>, version bigint)",
        ).unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // First insert: should succeed (row doesn't exist)
        let stmt = crate::parser::parse(
            "INSERT INTO ks.queue_metadata (queue_type, cluster_ack_level, version) VALUES (1, {}, 0) IF NOT EXISTS",
        ).unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "first INSERT IF NOT EXISTS should succeed: {:?}",
            result.err()
        );

        // Second insert: should return [applied]=false (row exists)
        let stmt = crate::parser::parse(
            "INSERT INTO ks.queue_metadata (queue_type, cluster_ack_level, version) VALUES (1, {}, 0) IF NOT EXISTS",
        ).unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "second INSERT IF NOT EXISTS should succeed (return applied=false): {:?}",
            result.err()
        );
    }

    /// Cassandra LWT result shape: an *applied* conditional returns ONLY the
    /// `[applied]=true` column; a *not-applied* one returns `[applied]=false`
    /// plus the conflicting row's columns. Returning the full column set on
    /// apply breaks strictly-typed drivers (scylla-rust) that deserialize the
    /// applied result as `(bool,)`.
    #[tokio::test]
    async fn lwt_applied_returns_only_applied_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        route(
            &state,
            &ctx_ks,
            crate::parser::parse("CREATE TABLE ks.t (id int PRIMARY KEY, name text, email text)")
                .unwrap(),
        )
        .await
        .unwrap();

        // Applied → exactly one column: [applied].
        let applied = route(
            &state,
            &ctx_ks,
            crate::parser::parse("INSERT INTO ks.t (id, name) VALUES (1, 'a') IF NOT EXISTS")
                .unwrap(),
        )
        .await
        .unwrap();
        let buf = match applied {
            RouteResult::Result(b) => b,
            other => panic!("expected Result, got {:?}", std::mem::discriminant(&other)),
        };
        assert_eq!(
            extract_column_count(&buf),
            1,
            "applied LWT must return only the [applied] column"
        );

        // Not applied → [applied] plus the conflicting row's columns (> 1).
        let not_applied = route(
            &state,
            &ctx_ks,
            crate::parser::parse("INSERT INTO ks.t (id, name) VALUES (1, 'b') IF NOT EXISTS")
                .unwrap(),
        )
        .await
        .unwrap();
        let buf2 = match not_applied {
            RouteResult::Result(b) => b,
            other => panic!("expected Result, got {:?}", std::mem::discriminant(&other)),
        };
        assert!(
            extract_column_count(&buf2) > 1,
            "not-applied LWT must return the conflicting row columns"
        );
    }

    // ── FRSA-BUG-025: collection bind value decoding ──────────────────

    /// Empty map bind value must not error with "type mismatch: expected map".
    #[tokio::test]
    async fn insert_empty_map_literal() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE ks.qm (qt int PRIMARY KEY, ack map<text, bigint>, ver bigint)",
        )
        .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        // Empty map literal
        let stmt =
            crate::parser::parse("INSERT INTO ks.qm (qt, ack, ver) VALUES (1, {}, 0)").unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "empty map insert failed: {:?}",
            result.err()
        );
    }

    /// Non-empty map literal roundtrip.
    #[tokio::test]
    async fn insert_and_select_map_literal() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE ks.qm (qt int PRIMARY KEY, ack map<text, bigint>, ver bigint)",
        )
        .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("INSERT INTO ks.qm (qt, ack, ver) VALUES (1, {'dc1': 100}, 0)")
                .unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(result.is_ok(), "map insert failed: {:?}", result.err());

        let sel = crate::parser::parse("SELECT qt, ver FROM ks.qm WHERE qt = 1").unwrap();
        let result = route(&state, &ctx_ks, sel).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1);
            }
            _ => panic!("expected Result"),
        }
    }

    /// Set and list bind values must also decode correctly.
    #[tokio::test]
    async fn insert_set_and_list_literals() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE TABLE ks.coll (k int PRIMARY KEY, ids set<uuid>, events list<text>)",
        )
        .unwrap();
        route(&state, &ctx_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO ks.coll (k, ids, events) VALUES (1, {550e8400-e29b-41d4-a716-446655440000}, ['login', 'logout'])",
        )
        .unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(result.is_ok(), "set/list insert failed: {:?}", result.err());
    }

    // ── BUG-001: TRUNCATE must actually delete data ──────────────────

    #[tokio::test]
    async fn truncate_deletes_data() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup: create keyspace and table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE ks.t (id int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert data
        let stmt = crate::parser::parse("INSERT INTO ks.t (id, v) VALUES (1, 'hello')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("INSERT INTO ks.t (id, v) VALUES (2, 'world')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify data exists
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1, "should have 1 row before truncate")
            }
            _ => panic!("expected Result"),
        }

        // TRUNCATE
        let stmt = crate::parser::parse("TRUNCATE ks.t").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT after truncate should return 0 rows
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 0, "should have 0 rows after truncate")
            }
            _ => panic!("expected Result"),
        }

        // Full scan should also return 0 rows
        let stmt = crate::parser::parse("SELECT * FROM ks.t").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(
                extract_row_count(b),
                0,
                "full scan should return 0 rows after truncate"
            ),
            _ => panic!("expected Result"),
        }
    }

    // ── ALLOW FILTERING executes full scan with post-filter ────────
    //
    // ALLOW FILTERING performs a full table scan and filters rows in
    // memory.  Queries without ALLOW FILTERING on non-indexed columns
    // are still rejected.

    #[test]
    fn partition_scan_yield_policy_triggers_at_threshold() {
        assert!(!should_yield_during_partition_scan(0, 32));
        assert!(!should_yield_during_partition_scan(31, 32));
        assert!(should_yield_during_partition_scan(32, 32));
        assert!(should_yield_during_partition_scan(64, 32));
    }

    #[test]
    fn partition_scan_yield_policy_can_be_disabled() {
        assert!(!should_yield_during_partition_scan(32, 0));
    }

    #[tokio::test]
    async fn allow_filtering_executes_full_scan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE ks.t (id int PRIMARY KEY, score int)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("SELECT * FROM ks.t WHERE score > 25 ALLOW FILTERING").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALLOW FILTERING should execute full scan, got: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn allow_filtering_with_limit_executes() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE afl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE afl.t (id int PRIMARY KEY, flag int)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        for id in 0..10 {
            let flag = id % 2;
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO afl.t (id, flag) VALUES ({id}, {flag})"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        let stmt =
            crate::parser::parse("SELECT * FROM afl.t WHERE flag = 1 LIMIT 2 ALLOW FILTERING")
                .unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), route(&state, &ctx, stmt))
                .await
                .expect("ALLOW FILTERING with LIMIT should not wait for an unbounded scan");
        let result = result.expect("ALLOW FILTERING with LIMIT should execute");
        let row_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            row_count, 2,
            "ALLOW FILTERING LIMIT should return the bounded first page"
        );
    }

    // ── ALLOW FILTERING returns correct filtered rows ────────────────
    //
    // Regression test: ALLOW FILTERING must actually filter rows and
    // return only matching data, not just succeed without error.

    #[tokio::test]
    async fn allow_filtering_returns_correct_rows() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup: table with PK = id, non-indexed column = category
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE afr WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE afr.items (id int PRIMARY KEY, category text, score int)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert 5 rows: 3 with category='tech', 2 with category='art'
        for (id, cat, score) in [
            (1, "tech", 10),
            (2, "art", 20),
            (3, "tech", 30),
            (4, "art", 40),
            (5, "tech", 50),
        ] {
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO afr.items (id, category, score) VALUES ({id}, '{cat}', {score})"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // SELECT all rows (no filter) — should return 5
        let stmt = crate::parser::parse("SELECT * FROM afr.items").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let all_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(all_count, 5, "should have 5 total rows");

        // SELECT with ALLOW FILTERING on category='tech' — should return 3
        let stmt =
            crate::parser::parse("SELECT * FROM afr.items WHERE category = 'tech' ALLOW FILTERING")
                .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let tech_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            tech_count, 3,
            "ALLOW FILTERING on category='tech' should return 3 rows, got {tech_count}"
        );

        // SELECT with ALLOW FILTERING on score > 25 — should return 3
        let stmt = crate::parser::parse("SELECT * FROM afr.items WHERE score > 25 ALLOW FILTERING")
            .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let gt_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            gt_count, 3,
            "ALLOW FILTERING on score > 25 should return 3 rows, got {gt_count}"
        );

        // SELECT with ALLOW FILTERING combining two predicates — should return 2
        let stmt = crate::parser::parse(
            "SELECT * FROM afr.items WHERE category = 'tech' AND score > 25 ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let combined_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(combined_count, 2, "ALLOW FILTERING on category='tech' AND score > 25 should return 2 rows, got {combined_count}");
    }

    // ── ALLOW FILTERING with UUID composite PK + non-PK column ──────
    //
    // Regression test for ferrosa-memory compatibility: composite UUID
    // partition key queries with ALLOW FILTERING must correctly filter
    // on non-PK columns.

    #[tokio::test]
    async fn allow_filtering_uuid_composite_pk() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Schema mirrors ferrosa-memory entity_store
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE agent WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE agent.entity_store (
                tenant_id uuid,
                session_id uuid,
                entity_id uuid,
                entity_name text,
                entity_type text,
                confidence float,
                PRIMARY KEY ((tenant_id, session_id), entity_id)
            )",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let sid = "11111111-2222-3333-4444-555555555555";

        // Insert 3 entities in same partition, different types
        for (eid, name, etype) in [
            ("00000001-0000-0000-0000-000000000001", "Alice", "person"),
            ("00000002-0000-0000-0000-000000000002", "Rust", "concept"),
            ("00000003-0000-0000-0000-000000000003", "Bob", "person"),
        ] {
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO agent.entity_store \
                 (tenant_id, session_id, entity_id, entity_name, entity_type, confidence) \
                 VALUES ({tid}, {sid}, {eid}, '{name}', '{etype}', 0.9)"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // PK lookup — should return all 3
        let stmt = crate::parser::parse(&format!(
            "SELECT * FROM agent.entity_store WHERE tenant_id = {tid} AND session_id = {sid}"
        ))
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let pk_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            pk_count, 3,
            "PK lookup should return 3 rows, got {pk_count}"
        );

        // PK + filter on entity_type = 'person' — should return 2
        let stmt = crate::parser::parse(&format!(
            "SELECT * FROM agent.entity_store \
             WHERE tenant_id = {tid} AND session_id = {sid} AND entity_type = 'person' \
             ALLOW FILTERING"
        ))
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let filtered_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            filtered_count, 2,
            "ALLOW FILTERING on entity_type='person' should return 2 rows, got {filtered_count}"
        );

        let stmt = crate::parser::parse(&format!(
            "SELECT entity_id, entity_name FROM agent.entity_store \
             WHERE tenant_id = {tid} LIMIT 1 ALLOW FILTERING"
        ))
        .unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), route(&state, &ctx, stmt))
                .await
                .expect(
                    "tenant-only partition-key LIMIT should not wait for unbounded materialization",
                )
                .expect("tenant-only partition-key LIMIT should execute");
        let limited_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            limited_count, 1,
            "tenant-only partition-key LIMIT should return exactly one row, got {limited_count}"
        );
    }

    #[tokio::test]
    async fn allow_filtering_partial_composite_partition_key_scans_all_matching_partitions() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE edge_scan WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE TABLE edge_scan.typed_edges (
                    tenant_id uuid,
                    session_id uuid,
                    src_id uuid,
                    edge_type text,
                    dst_id uuid,
                    weight double,
                    PRIMARY KEY ((tenant_id, session_id), src_id, edge_type, dst_id)
                )",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let tenant_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let tenant_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        let matching_rows = ferrosa_cluster::write_path::DEFAULT_RANGE_READ_LIMIT + 2;
        for i in 0..matching_rows {
            let suffix = format!("{:012x}", i + 1);
            let session = format!("00000000-0000-0000-0000-{suffix}");
            let src = format!("10000000-0000-0000-0000-{suffix}");
            let dst = format!("20000000-0000-0000-0000-{suffix}");
            route(
                &state,
                &ctx,
                crate::parser::parse(&format!(
                    "INSERT INTO edge_scan.typed_edges \
                     (tenant_id, session_id, src_id, edge_type, dst_id, weight) \
                     VALUES ({tenant_a}, {session}, {src}, 'related_to', {dst}, 1.0)"
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        }
        route(
            &state,
            &ctx,
            crate::parser::parse(&format!(
                "INSERT INTO edge_scan.typed_edges \
                 (tenant_id, session_id, src_id, edge_type, dst_id, weight) \
                 VALUES ({tenant_b}, 00000000-0000-0000-0000-000000000001, \
                         30000000-0000-0000-0000-000000000001, 'related_to', \
                         40000000-0000-0000-0000-000000000001, 1.0)"
            ))
            .unwrap(),
        )
        .await
        .unwrap();

        let stmt = crate::parser::parse(&format!(
            "SELECT src_id, edge_type, dst_id FROM edge_scan.typed_edges \
             WHERE tenant_id = {tenant_a} ALLOW FILTERING"
        ))
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let row_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            row_count, matching_rows as i32,
            "ALLOW FILTERING on the first component of a composite partition key must scan every matching tenant/session partition"
        );
    }

    #[tokio::test]
    async fn select_distinct_tenant_id_returns_unique_partition_key_components() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE distinct_scan WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE TABLE distinct_scan.typed_edges (
                    tenant_id uuid,
                    session_id uuid,
                    src_id uuid,
                    edge_type text,
                    dst_id uuid,
                    weight double,
                    PRIMARY KEY ((tenant_id, session_id), src_id, edge_type, dst_id)
                )",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        for (tenant, session, src, dst) in [
            (
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "00000000-0000-0000-0000-000000000001",
                "10000000-0000-0000-0000-000000000001",
                "20000000-0000-0000-0000-000000000001",
            ),
            (
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "00000000-0000-0000-0000-000000000002",
                "10000000-0000-0000-0000-000000000002",
                "20000000-0000-0000-0000-000000000002",
            ),
            (
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "00000000-0000-0000-0000-000000000003",
                "10000000-0000-0000-0000-000000000003",
                "20000000-0000-0000-0000-000000000003",
            ),
        ] {
            route(
                &state,
                &ctx,
                crate::parser::parse(&format!(
                    "INSERT INTO distinct_scan.typed_edges \
                     (tenant_id, session_id, src_id, edge_type, dst_id, weight) \
                     VALUES ({tenant}, {session}, {src}, 'related_to', {dst}, 1.0)"
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        }

        let result = route(
            &state,
            &ctx,
            crate::parser::parse("SELECT DISTINCT tenant_id FROM distinct_scan.typed_edges")
                .unwrap(),
        )
        .await
        .unwrap();
        let row_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            row_count, 2,
            "SELECT DISTINCT tenant_id must deduplicate repeated tenant partition-key components"
        );
    }

    #[tokio::test]
    async fn composite_clustering_text_predicate_matches_exact_pk_lookup() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE agent WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE TABLE agent.typed_edges (
                    tenant_id uuid,
                    session_id uuid,
                    src_id uuid,
                    edge_type text,
                    dst_id uuid,
                    weight double,
                    PRIMARY KEY ((tenant_id, session_id), src_id, edge_type, dst_id)
                )",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let tid = "6792702e-2a9c-4465-ba65-ba100b5aaafa";
        let sid = "909e2671-aea0-534a-83bc-bb5efc544b0f";
        let src = "41753309-7297-454e-8f2d-c6546740cf2b";
        let dst = "f6ffe258-9194-470d-9811-5b3e23b33103";

        for edge_type in ["related_to", "afteradj_1776880700"] {
            route(
                &state,
                &ctx,
                crate::parser::parse(&format!(
                    "INSERT INTO agent.typed_edges \
                     (tenant_id, session_id, src_id, edge_type, dst_id, weight) \
                     VALUES ({tid}, {sid}, {src}, '{edge_type}', {dst}, 1.0)"
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        }

        let stmt = crate::parser::parse(&format!(
            "SELECT src_id, edge_type, dst_id, weight FROM agent.typed_edges \
             WHERE tenant_id = {tid} AND session_id = {sid} AND src_id = {src} \
             AND edge_type = 'afteradj_1776880700'"
        ))
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let row_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            row_count, 1,
            "exact PK lookup including text clustering column should return the matching row"
        );

        let stmt = crate::parser::parse(
            "SELECT src_id, edge_type, dst_id, weight FROM agent.typed_edges \
             WHERE edge_type = 'afteradj_1776880700' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let filtered_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(filtered_count, 1);
    }

    // ── ALLOW FILTERING rejection: exact Cassandra semantics ─────────
    //
    // Cassandra requires ALLOW FILTERING when ANY WHERE column is not
    // covered by the partition key or a secondary index.  The presence of
    // an index on *some* WHERE columns does not exempt the remaining
    // un-indexed columns from that requirement.
    //
    // The UAT test `single_allow_filtering_rejection` exposed a gap: when
    // a table has an index on `email` and the query is
    //   SELECT * WHERE email = 'x' AND name = 'Ada'   (no ALLOW FILTERING)
    // Ferrosa was incorrectly accepting the query because `has_matching_index`
    // was true for `email`, bypassing the check for the un-indexed `name`
    // column.  Cassandra rejects this with "Cannot execute this query as it
    // might involve data filtering…".

    /// Non-indexed column in WHERE without ALLOW FILTERING must be rejected
    /// even when another WHERE column IS indexed.  This was the gap caught by
    /// the UAT run of 2026-03-17.
    #[tokio::test]
    async fn allow_filtering_required_for_non_indexed_where_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup: table with an index on `email` but NOT on `name`
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create an index on `email`
        let stmt = crate::parser::parse("CREATE INDEX af_email_idx ON af.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert a row
        let stmt = crate::parser::parse(
            "INSERT INTO af.users (id, name, email) VALUES (1, 'Ada', 'ada@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query: email is indexed but name is NOT — without ALLOW FILTERING
        // this must be rejected because `name` requires a full-scan filter.
        let stmt = crate::parser::parse(
            "SELECT * FROM af.users WHERE email = 'ada@example.com' AND name = 'Ada'",
        )
        .unwrap();
        match route(&state, &ctx, stmt).await {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("non-indexed"),
                    "error should mention non-indexed columns, got: {msg}"
                );
            }
            Ok(_) => panic!(
                "WHERE on partially-indexed columns without ALLOW FILTERING should be rejected"
            ),
        }
    }

    /// With an index on some columns and ALLOW FILTERING, the query should
    /// succeed — the index narrows the scan, post-filter handles the rest.
    #[tokio::test]
    async fn allow_filtering_with_partial_index_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af2.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX af2_email_idx ON af2.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "SELECT * FROM af2.users WHERE email = 'ada@example.com' AND name = 'Ada' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALLOW FILTERING with partial index should succeed: {:?}",
            result.err()
        );
    }

    /// Querying on a fully-indexed column without ALLOW FILTERING must succeed.
    /// This is the baseline: index present, no extra unindexed filter columns.
    #[tokio::test]
    async fn indexed_column_where_without_allow_filtering_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af3.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX af3_email_idx ON af3.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO af3.users (id, name, email) VALUES (1, 'Ada', 'ada@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // All WHERE columns are indexed — no ALLOW FILTERING needed.
        let stmt = crate::parser::parse("SELECT * FROM af3.users WHERE email = 'ada@example.com'")
            .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "fully-indexed WHERE should not require ALLOW FILTERING: {:?}",
            result.err()
        );
    }

    // ── BUG-004: ORDER BY parsed but not executed ────────────────────

    #[tokio::test]
    async fn order_by_sorts_results() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup: table with composite key (pk, ck) and a value column
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert rows with same pk but different ck values
        for ck in [3, 1, 4, 1, 5] {
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO ks.t (pk, ck, v) VALUES (1, {ck}, 'val{ck}')"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // SELECT with ORDER BY ck ASC
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE pk = 1 ORDER BY ck ASC").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let rows_asc = match &result {
            RouteResult::Result(b) => extract_int_column_values(b, "ck"),
            _ => panic!("expected Result"),
        };
        let mut sorted = rows_asc.clone();
        sorted.sort();
        assert_eq!(
            rows_asc, sorted,
            "ORDER BY ck ASC should return sorted ascending: got {rows_asc:?}"
        );

        // SELECT with ORDER BY ck DESC
        let stmt =
            crate::parser::parse("SELECT * FROM ks.t WHERE pk = 1 ORDER BY ck DESC").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let rows_desc = match &result {
            RouteResult::Result(b) => extract_int_column_values(b, "ck"),
            _ => panic!("expected Result"),
        };
        let mut sorted_desc = rows_desc.clone();
        sorted_desc.sort_by(|a, b| b.cmp(a));
        assert_eq!(
            rows_desc, sorted_desc,
            "ORDER BY ck DESC should return sorted descending: got {rows_desc:?}"
        );
    }

    #[tokio::test]
    async fn arbitrary_unbounded_order_by_reserves_and_cleans_temp_sort_table() {
        let (state, dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE sortks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
        route(
            &state,
            &ctx,
            crate::parser::parse("CREATE TABLE sortks.t (pk int PRIMARY KEY, v int)").unwrap(),
        )
        .await
        .unwrap();
        for (pk, v) in [(1, 30), (2, 10), (3, 20)] {
            route(
                &state,
                &ctx,
                crate::parser::parse(&format!("INSERT INTO sortks.t (pk, v) VALUES ({pk}, {v})"))
                    .unwrap(),
            )
            .await
            .unwrap();
        }

        let stmt = crate::parser::parse("SELECT * FROM sortks.t ORDER BY v ASC").unwrap();
        let Statement::Select(ref select) = stmt else {
            panic!("expected SELECT");
        };
        let snap = state.schema.snapshot();
        let table_meta = snap
            .tables
            .get(&("sortks".to_string(), "t".to_string()))
            .unwrap();
        assert_eq!(
            classify_order_by_execution(&state, "sortks", select, table_meta),
            OrderByExecutionPlan::SpillableTempTable {
                estimated_scan_bytes: 0,
            }
        );

        route(&state, &ctx, stmt).await.unwrap();

        let tmp_root = dir.path().join("tmp_order_by_sort");
        if tmp_root.exists() {
            let leftovers = std::fs::read_dir(&tmp_root).unwrap().count();
            assert_eq!(leftovers, 0, "ORDER BY temp-sort table must be cleaned up");
        }
    }

    #[test]
    fn order_by_temp_sort_reservation_cleans_up_on_drop() {
        let (state, dir) = setup();
        let reservation = state
            .engine
            .reserve_order_by_temp_sort_table("ks", "t")
            .unwrap();
        let path = reservation.path().to_path_buf();
        assert!(path.exists(), "temp-sort table should be created");
        drop(reservation);
        assert!(
            !path.exists(),
            "dropping reservation must clean temp-sort table"
        );
        assert!(
            dir.path().join("tmp_order_by_sort").exists(),
            "cleanup removes the per-query temp table, not the shared temp root"
        );
    }

    /// Extract values for a named int column from a Rows result.
    fn extract_int_column_values(buf: &[u8], target_col: &str) -> Vec<i32> {
        assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes());
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        // Skip keyspace string
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // Skip table string
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        // Read column names and types to find the target column index
        let mut target_idx = None;
        let mut col_type_ids = Vec::new();
        for i in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let name = std::str::from_utf8(&buf[off..off + name_len]).unwrap();
            if name == target_col {
                target_idx = Some(i);
            }
            off += name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            col_type_ids.push(type_id);
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        let target_idx = target_idx.expect("target column not found in result");
        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        off += 4;

        let mut values = Vec::new();
        for _ in 0..row_count {
            for col in 0..col_count {
                let cell_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
                off += 4;
                if cell_len >= 0 {
                    let cell_bytes = &buf[off..off + cell_len as usize];
                    if col == target_idx {
                        let val = i32::from_be_bytes(cell_bytes.try_into().unwrap());
                        values.push(val);
                    }
                    off += cell_len as usize;
                }
                // cell_len == -1 means null; skip
            }
        }
        values
    }

    // ── Observability virtual table tests ────────────────────────────────

    /// Helper: register the standard observability virtual tables into the
    /// schema's shared registry so CQL SELECT queries can resolve them.
    fn setup_with_observability() -> (SharedState, TempDir) {
        let (state, dir) = setup();
        state
            .schema
            .virtual_tables()
            .register(Arc::new(ConnectionsTable::new(
                state.connection_tracker.clone(),
            )));
        state
            .schema
            .virtual_tables()
            .register(Arc::new(ActiveQueriesTable::new(
                state.query_tracker.clone(),
            )));
        (state, dir)
    }

    /// Regression: `SELECT * FROM system_observability.connections` must succeed
    /// when the virtual table is registered in the schema's shared registry.
    #[tokio::test]
    async fn select_system_observability_connections() {
        let (state, _dir) = setup_with_observability();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system_observability.connections").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                // Kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression: `SELECT * FROM system_observability.active_queries` must
    /// succeed when the virtual table is registered in the schema's shared registry.
    #[tokio::test]
    async fn select_system_observability_active_queries() {
        let (state, _dir) = setup_with_observability();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("SELECT * FROM system_observability.active_queries").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                // Kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression: `USE system_observability` followed by `SELECT * FROM connections`
    /// (unqualified, relying on current keyspace) must also succeed.
    #[tokio::test]
    async fn select_observability_with_use_keyspace() {
        let (state, _dir) = setup_with_observability();
        let dev = dev_auth();

        // USE system_observability — no validation, just sets current keyspace.
        let use_stmt = crate::parser::parse("USE system_observability").unwrap();
        let use_result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            use_stmt,
        )
        .await
        .unwrap();
        let new_ks = match use_result {
            RouteResult::SetKeyspace(ks, _) => ks,
            _ => panic!("expected SetKeyspace"),
        };
        assert_eq!(new_ks, "system_observability");

        // SELECT * FROM connections (unqualified, using current_keyspace)
        let sel_stmt = crate::parser::parse("SELECT * FROM connections").unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &Some(new_ks),
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            sel_stmt,
        )
        .await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    // =========================================================================
    // Task 3.5: CREATE INDEX wires through to StorageEngine indexed_columns
    // =========================================================================

    // ── UDF/UDA query-time wiring tests ──────────────────────────────────

    /// Helper: hex-encode a byte slice.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Minimal valid WASM component bytes (same as in ferrosa-udf tests).
    fn minimal_wasm_component() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, // \0asm
            0x0d, 0x00, // version 13
            0x01, 0x00, // layer = component
        ]
    }

    fn minimal_streaming_aggregate_component() -> Vec<u8> {
        let mut bytes = minimal_wasm_component();
        append_component_custom_section(
            &mut bytes,
            "ferrosa:streaming-aggregate:v1",
            b"contract=init/update/finalize;value=f64;state=bounded",
        );
        bytes
    }

    fn append_component_custom_section(bytes: &mut Vec<u8>, name: &str, payload: &[u8]) {
        let mut section = Vec::new();
        push_uleb128(&mut section, name.len() as u32);
        section.extend_from_slice(name.as_bytes());
        section.extend_from_slice(payload);

        bytes.push(0);
        push_uleb128(bytes, section.len() as u32);
        bytes.extend_from_slice(&section);
    }

    fn push_uleb128(out: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn route_create_function_valid_wasm_stores_in_schema() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE udf_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create function with valid WASM component bytes
        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION udf_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE FUNCTION with valid WASM should succeed, got: {:?}",
            result.err()
        );

        // Verify function is stored in schema
        let func = state
            .schema
            .get_function("udf_ks", "my_func", &[CqlType::Int]);
        assert!(
            func.is_some(),
            "function should be registered in schema after CREATE FUNCTION"
        );
        let func = func.unwrap();
        assert_eq!(func.name, "my_func");
        assert_eq!(func.arg_names, vec!["val"]);
        assert_eq!(func.arg_types, vec![CqlType::Int]);
        assert_eq!(func.return_type, CqlType::Int);
        assert!(func.called_on_null);
        assert_eq!(func.language, "wasm");
    }

    /// CREATE FUNCTION ... LANGUAGE assemblyscript compiles the inline source to a
    /// component, registers it, and the executor invokes it. Requires the asc
    /// bundle (FERROSA_ASC_BUNDLE) and the asc-udf feature.
    #[cfg(all(feature = "asc-udf", feature = "live-infra-tests"))]
    #[tokio::test]
    async fn route_create_function_assemblyscript_compiles_and_runs() {
        use ferrosa_common::CqlValue;
        if std::env::var_os("FERROSA_ASC_BUNDLE").is_none() {
            panic!(
                "FERROSA_ASC_BUNDLE is not set. Build the asc bundle and run:\n  \
                 ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs\n  \
                 FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs cargo test -p ferrosa-cql \
                 --features 'asc-udf live-infra-tests' route_create_function_assemblyscript"
            );
        }
        let (state, _dir) = setup();
        // CREATE FUNCTION now dogfoods a write to system_schema.functions, so the
        // table must be registered (mirrors the WASM create-function tests). This
        // test only runs under `--features asc-udf` + FERROSA_ASC_BUNDLE, so the
        // missing registration wasn't caught by a default `cargo test`.
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE udf_as WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let cql = "CREATE FUNCTION udf_as.addi(a int, b int) CALLED ON NULL INPUT \
                   RETURNS int LANGUAGE assemblyscript \
                   AS 'export function addi(a: i32, b: i32): i32 { return a + b; }'";
        let stmt = crate::parser::parse(cql).unwrap();
        route(&state, &ctx, stmt)
            .await
            .expect("CREATE FUNCTION LANGUAGE assemblyscript should succeed");

        let func = state
            .schema
            .get_function("udf_as", "addi", &[CqlType::Int, CqlType::Int])
            .expect("asc function registered in schema");
        assert_eq!(func.language, "assemblyscript");

        let out = state
            .udf_executor
            .call(
                "udf_as",
                "addi",
                vec![CqlValue::Int(2), CqlValue::Int(3)],
                &[CqlType::Int, CqlType::Int],
                &CqlType::Int,
            )
            .expect("invoke compiled asc UDF");
        assert_eq!(out, CqlValue::Int(5));
    }

    #[tokio::test]
    async fn route_create_function_with_streaming_marker_compiles_aggregate() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE udf_agg_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let hex_body = hex_encode(&minimal_streaming_aggregate_component());
        let cql = format!(
            "CREATE FUNCTION udf_agg_ks.stddev(val double) CALLED ON NULL INPUT RETURNS double LANGUAGE wasm AS '{hex_body}'"
        );
        route(&state, &ctx, crate::parser::parse(&cql).unwrap())
            .await
            .unwrap();

        assert_eq!(
            state
                .udf_executor
                .get_kind("udf_agg_ks", "stddev", &[CqlType::Double])
                .unwrap(),
            ferrosa_udf::FunctionKind::Aggregate
        );
    }

    #[tokio::test]
    async fn route_create_function_requires_superuser() {
        let (state, _dir) = setup();
        let admin_auth = dev_auth();
        let admin_ctx = RequestContext {
            auth: &admin_auth,
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE udf_admin_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &admin_ctx, stmt).await.unwrap();

        let user_auth = AuthContext {
            role: "sensor_app".into(),
            is_superuser: false,
            must_change_password: false,
        };
        let user_ctx = RequestContext {
            auth: &user_auth,
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION udf_admin_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let err = match route(&state, &user_ctx, stmt).await {
            Ok(_) => panic!("CREATE FUNCTION should require a superuser role"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("superuser"),
            "expected superuser-only error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn route_create_or_replace_function_replaces_schema_entry() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE replace_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION replace_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify function is in schema and WASM cache
        assert!(state
            .schema
            .get_function("replace_ks", "my_func", &[CqlType::Int])
            .is_some());

        let cql = format!(
            "CREATE OR REPLACE FUNCTION replace_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "OR REPLACE should drop and recreate the schema entry, got: {:?}",
            result.err()
        );

        assert!(
            state
                .schema
                .get_function("replace_ks", "my_func", &[CqlType::Int])
                .is_some(),
            "function should remain in schema after replacement"
        );
    }

    #[tokio::test]
    async fn route_create_function_from_url_verifies_sha256_and_stores_hex_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE url_udf_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let wasm = minimal_wasm_component();
        let digest = sha256_hex(&wasm);
        let body = wasm.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        });

        let cql = format!(
            "CREATE FUNCTION url_udf_ks.from_url(val int) CALLED ON NULL INPUT RETURNS int \
             LANGUAGE wasm AS URL 'http://{addr}/from-url.wasm' WITH SHA256 = '{digest}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let func = state
            .schema
            .get_function("url_udf_ks", "from_url", &[CqlType::Int])
            .expect("function should be stored");
        assert_eq!(func.body, hex_encode(&wasm));
    }

    #[tokio::test]
    async fn route_create_function_duplicate_without_replace_errors() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dup_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION dup_ks.dup_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Duplicate create without OR REPLACE or IF NOT EXISTS should error
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "duplicate CREATE FUNCTION without OR REPLACE should error"
        );
    }

    #[tokio::test]
    async fn route_create_function_if_not_exists_succeeds_on_duplicate() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ine_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION ine_ks.ine_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // IF NOT EXISTS should succeed without error
        let cql = format!(
            "CREATE FUNCTION IF NOT EXISTS ine_ks.ine_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF NOT EXISTS should not error on duplicate"
        );
    }

    #[tokio::test]
    async fn route_create_function_if_not_exists_duplicate_does_not_compile_body() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ine_order_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION ine_order_ks.ine_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE FUNCTION IF NOT EXISTS ine_order_ks.ine_func(val int) \
             CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'not valid hex'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "duplicate IF NOT EXISTS should return before decoding or compiling body"
        );
    }

    #[tokio::test]
    async fn route_drop_function_removes_from_schema() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace and function
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE drop_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION drop_ks.to_drop(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify function exists
        assert!(state
            .schema
            .get_function("drop_ks", "to_drop", &[CqlType::Int])
            .is_some());

        // DROP FUNCTION
        let stmt = crate::parser::parse("DROP FUNCTION drop_ks.to_drop").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP FUNCTION should succeed, got: {:?}",
            result.err()
        );

        // Verify function is gone from schema
        assert!(
            state
                .schema
                .get_function("drop_ks", "to_drop", &[CqlType::Int])
                .is_none(),
            "function should be removed from schema after DROP FUNCTION"
        );
    }

    #[tokio::test]
    async fn route_create_function_with_current_keyspace() {
        let (state, _dir) = setup();
        state.engine.register_system_tables().unwrap();

        // Create keyspace first (without current_keyspace set)
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cur_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Now use current_keyspace context for unqualified function name
        let cur_ks = Some("cur_ks".to_string());
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &cur_ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION cur_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE FUNCTION with current_keyspace should succeed, got: {:?}",
            result.err()
        );

        // Verify function is stored under the current keyspace
        let func = state
            .schema
            .get_function("cur_ks", "cur_func", &[CqlType::Int]);
        assert!(
            func.is_some(),
            "function should be registered under current keyspace"
        );
    }

    #[tokio::test]
    async fn route_create_aggregate_requires_state_function() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE agg_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Try to create aggregate without the state function existing
        let stmt = crate::parser::parse(
            "CREATE AGGREGATE agg_ks.my_agg(int) SFUNC nonexistent_sfunc STYPE int",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "CREATE AGGREGATE should fail when state function does not exist"
        );
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("state function"),
            "error should mention missing state function, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn route_hex_decode_roundtrip() {
        // Test that our hex_encode/hex_decode are consistent
        let original = vec![0x00, 0x61, 0x73, 0x6d, 0xff, 0x00];
        let encoded = hex_encode(&original);
        let decoded = super::hex_decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    /// Phonetic index: equality on clustering key column with phonetic index matches by
    /// Double Metaphone code, not exact string. PK lookup + post-filter path.
    #[tokio::test]
    async fn phonetic_index_equality_matches_by_metaphone_clustering_key() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE phon_ck WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE phon_ck.entities (tenant_id text, session_id text, entity_name text, data text, PRIMARY KEY ((tenant_id, session_id), entity_name))",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE INDEX phon_ck_idx ON phon_ck.entities (entity_name) USING 'phonetic'",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO phon_ck.entities (tenant_id, session_id, entity_name, data) VALUES ('t1', 's1', 'John Smith', 'some data')",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT with phonetically equivalent name — must match via post-filter
        let stmt = crate::parser::parse(
            "SELECT entity_name, data FROM phon_ck.entities WHERE tenant_id = 't1' AND session_id = 's1' AND entity_name = 'Jon Smyth'",
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "phonetic query should not error: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "phonetic index should match 'Jon Smyth' to 'John Smith', got {count} rows"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Full-text index end-to-end: `CREATE INDEX ... USING 'fulltext'` must be
    /// accepted and wired to the engine so that, after a flush builds the FTI
    /// sidecar, `fts_match()` returns the matching rows.
    #[tokio::test]
    async fn fulltext_index_fts_match_end_to_end() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for cql in [
            "CREATE KEYSPACE fts_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fts_ks.docs (id int PRIMARY KEY, body text)",
            "CREATE INDEX docs_fts ON fts_ks.docs (body) USING 'fulltext'",
            "INSERT INTO fts_ks.docs (id, body) VALUES (1, 'rust is a fast distributed database')",
            "INSERT INTO fts_ks.docs (id, body) VALUES (2, 'cassandra is a distributed database')",
            "INSERT INTO fts_ks.docs (id, body) VALUES (3, 'hello world')",
        ] {
            let stmt = crate::parser::parse(cql).unwrap();
            route(&state, &ctx, stmt).await.unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        // The FTI sidecar is built on flush; fts_match reads it.
        state
            .engine
            .flush(&ferrosa_storage::TableId::new("fts_ks", "docs"))
            .unwrap();

        let stmt = crate::parser::parse(
            "SELECT id FROM fts_ks.docs WHERE body = fts_match('distributed AND database')",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "fts_match query errored: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(count, 2, "docs 1 and 2 have both terms; got {count} rows");
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn fulltext_index_fts_match_reads_unflushed_memtable_row() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for cql in [
            "CREATE KEYSPACE fts_mem WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fts_mem.docs (id int PRIMARY KEY, body text)",
            "CREATE INDEX docs_fts ON fts_mem.docs (body) USING 'fulltext'",
            "INSERT INTO fts_mem.docs (id, body) VALUES (1, 'ferrosaftsfresh native fts probe body')",
        ] {
            let stmt = crate::parser::parse(cql).unwrap();
            route(&state, &ctx, stmt)
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        let stmt = crate::parser::parse(
            "SELECT id FROM fts_mem.docs WHERE body = fts_match('ferrosaftsfresh')",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "fts_match query errored: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "fts_match must return a row that is still only in the memtable"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Phonetic index: equality on regular column (not part of PK) matches
    /// by Double Metaphone. This exercises the index-based scan path.
    #[tokio::test]
    async fn phonetic_index_equality_matches_by_metaphone_regular_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE phon_reg WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // entity_name is a REGULAR column, entity_id is clustering key
        let stmt = crate::parser::parse(
            "CREATE TABLE phon_reg.entities (tenant_id text, session_id text, entity_id text, entity_name text, data text, PRIMARY KEY ((tenant_id, session_id), entity_id))",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE INDEX phon_reg_idx ON phon_reg.entities (entity_name) USING 'phonetic'",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO phon_reg.entities (tenant_id, session_id, entity_id, entity_name, data) VALUES ('t1', 's1', 'e1', 'John Smith', 'some data')",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query with phonetically equivalent name on regular column
        let stmt = crate::parser::parse(
            "SELECT entity_name, data FROM phon_reg.entities WHERE tenant_id = 't1' AND session_id = 's1' AND entity_name = 'Jon Smyth'",
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "phonetic query should not error: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "phonetic index should match 'Jon Smyth' to 'John Smith' on regular column, got {count} rows"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Phonetic index: query WITHOUT partition key — forces index-only scan path.
    #[tokio::test]
    async fn phonetic_index_equality_matches_without_pk() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE phon_nopk WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE phon_nopk.entities (id int PRIMARY KEY, entity_name text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE INDEX phon_nopk_idx ON phon_nopk.entities (entity_name) USING 'phonetic'",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO phon_nopk.entities (id, entity_name) VALUES (1, 'John Smith')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query by phonetic equivalent WITHOUT specifying PK — forces index path
        let stmt = crate::parser::parse(
            "SELECT id, entity_name FROM phon_nopk.entities WHERE entity_name = 'Jon Smyth'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "phonetic query without PK should not error: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "phonetic index lookup should match 'Jon Smyth' to 'John Smith', got {count} rows"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Phase 2 (per-type read dispatch): a phonetic `WHERE name = 'Jon'` without
    /// a PK predicate must take the secondary-index path (`SingleIndex`), and the
    /// index must point-look-up the phonetic sidecar/memtable by encoded code —
    /// returning the row even after a flush, where the memtable index is empty.
    /// This proves the index is consulted, not a full-scan + post-filter.
    #[tokio::test]
    async fn phonetic_select_takes_index_path_not_full_scan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        for cql in [
            "CREATE KEYSPACE phon_idx WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE phon_idx.people (id int PRIMARY KEY, name text)",
            "CREATE INDEX phon_name ON phon_idx.people (name) USING 'phonetic'",
            "INSERT INTO phon_idx.people (id, name) VALUES (1, 'John')",
        ] {
            let stmt = crate::parser::parse(cql).unwrap();
            route(&state, &ctx, stmt)
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // EXPLAIN must show the planner chose the index path, not a FullScan.
        let stmt =
            crate::parser::parse("EXPLAIN SELECT id FROM phon_idx.people WHERE name = 'Jon'")
                .unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let haystack = String::from_utf8_lossy(&b);
                assert!(
                    haystack.contains("SingleIndex"),
                    "phonetic query must plan SingleIndex, got: {haystack}"
                );
                assert!(
                    !haystack.contains("FullScan"),
                    "phonetic query must not plan FullScan, got: {haystack}"
                );
            }
            _ => panic!("expected Result from EXPLAIN"),
        }

        // Flush so the memtable index is empty: a correct answer here can only
        // come from the phonetic sidecar via the index path (a full scan would
        // be a fallback, but the planned path is SingleIndex per EXPLAIN above).
        state
            .engine
            .flush(&ferrosa_storage::TableId::new("phon_idx", "people"))
            .unwrap();

        let stmt =
            crate::parser::parse("SELECT id FROM phon_idx.people WHERE name = 'Jon'").unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "phonetic index path must match 'Jon' to flushed 'John', got {count} rows"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    /// Phase 2 (per-type read dispatch): a two-predicate `WHERE a = ? AND b = ?`
    /// where both columns are independently indexed must plan `IndexIntersection`
    /// and consult BOTH indexes, returning only rows present in both index
    /// result sets. The discriminating fixture has rows matching only `a`, only
    /// `b`, and both — a correct intersection returns exactly the both-match row.
    #[tokio::test]
    async fn index_intersection_consults_all_indexes() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        for cql in [
            "CREATE KEYSPACE isect WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE isect.people (id int PRIMARY KEY, city text, dept text)",
            "CREATE INDEX isect_city ON isect.people (city)",
            "CREATE INDEX isect_dept ON isect.people (dept)",
            // row 1: matches both city='NYC' AND dept='eng' — the only answer.
            "INSERT INTO isect.people (id, city, dept) VALUES (1, 'NYC', 'eng')",
            // row 2: matches city only.
            "INSERT INTO isect.people (id, city, dept) VALUES (2, 'NYC', 'sales')",
            // row 3: matches dept only.
            "INSERT INTO isect.people (id, city, dept) VALUES (3, 'LA', 'eng')",
        ] {
            let stmt = crate::parser::parse(cql).unwrap();
            route(&state, &ctx, stmt)
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // EXPLAIN must show IndexIntersection over both indexes.
        let stmt = crate::parser::parse(
            "EXPLAIN SELECT id FROM isect.people WHERE city = 'NYC' AND dept = 'eng'",
        )
        .unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let haystack = String::from_utf8_lossy(&b);
                assert!(
                    haystack.contains("IndexIntersection"),
                    "two indexed Eq predicates must plan IndexIntersection, got: {haystack}"
                );
                assert!(
                    haystack.contains("isect_city") && haystack.contains("isect_dept"),
                    "IndexIntersection must reference both indexes, got: {haystack}"
                );
            }
            _ => panic!("expected Result from EXPLAIN"),
        }

        let stmt =
            crate::parser::parse("SELECT id FROM isect.people WHERE city = 'NYC' AND dept = 'eng'")
                .unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "intersection of city='NYC' and dept='eng' must return exactly row 1, got {count}"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    // ── Phase 3: per-type index-usage observability ─────────────────────────
    //
    // Each of the following tests proves, for one index type, BOTH that the
    // query returns the correct rows AND that the index was observably hit:
    // the `system_observability.index_usage` counter increments and EXPLAIN
    // reports a non-`FullScan` plan. The vector test asserts correct ANN
    // ordering and that the vector index is registered (the router ANN path is
    // a brute-force sort pending Phase 2 vector read dispatch, so it honestly
    // does not claim an index_usage hit it cannot demonstrate).
    //
    // ── 2i acceleration coverage matrix ─────────────────────────────────────
    //
    // Every one of the 8 `IndexType`s is query-accelerated AND observable. For
    // each type the named test asserts ALL of: (a) correct rows, (b) `EXPLAIN`
    // reports the plan variant and NOT `FullScan` (via `assert_explain_plan`),
    // (c) the `index_usage` counter increments at execution (via
    // `assert_index_hit_and_count`).
    //
    // | IndexType | EXPLAIN plan variant | proving test |
    // |-----------|----------------------|--------------|
    // | BTree     | SingleIndex          | `btree_index_usage_observable_end_to_end` |
    // | Hash      | SingleIndex          | `hash_index_usage_observable_end_to_end` |
    // | Composite | SingleIndex          | `composite_index_usage_observable_end_to_end` |
    // | Phonetic  | SingleIndex          | `phonetic_index_usage_observable_end_to_end` |
    // | Filtered  | SingleIndex (partial)| `filtered_index_used_when_query_implies_predicate` |
    // | FullText  | FullTextIndex        | `fulltext_index_usage_observable_end_to_end` |
    // | Vector    | VectorAnn            | `ann_query_consults_vector_index_and_is_observable` |
    // | Geo       | GeoIndex             | `geo_st_within_polygon_filters_to_central_sf` (and other `geo_*`) |
    //
    // BTree/Hash/Composite/Phonetic all render `SingleIndex` because they are
    // scalar-`=` indexes the generic planner matches identically; the type
    // distinction lives in `ferrosa-index` build/read dispatch, exercised by
    // those crate-level tests. Filtered renders `SingleIndex` but is gated by
    // `filtered_index_is_usable` (predicate implication) — soundness is proven
    // by `filtered_index_withheld_when_query_does_not_imply_predicate`. FullText,
    // Vector, and Geo each take an early non-planner branch and report a
    // dedicated plan variant.

    /// Run a non-EXPLAIN query and assert the index_usage counter advanced by
    /// exactly the expected delta, returning the decoded row count.
    async fn assert_index_hit_and_count(
        state: &SharedState,
        ctx: &RequestContext<'_>,
        select_cql: &str,
        expected_delta: u64,
    ) -> i32 {
        let before = state.index_usage_tracker.total_index_hits();
        let stmt = crate::parser::parse(select_cql).unwrap();
        let row_count = match route(state, ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => extract_row_count(&b),
            _ => panic!("expected Result for: {select_cql}"),
        };
        let after = state.index_usage_tracker.total_index_hits();
        assert_eq!(
            after - before,
            expected_delta,
            "index_usage must advance by {expected_delta} for: {select_cql} (before={before}, after={after})"
        );
        row_count
    }

    /// Assert that EXPLAIN of `select_cql` reports a plan that is not a
    /// `FullScan` and contains `expected_plan`.
    async fn assert_explain_plan(
        state: &SharedState,
        ctx: &RequestContext<'_>,
        select_cql: &str,
        expected_plan: &str,
    ) {
        let stmt = crate::parser::parse(&format!("EXPLAIN {select_cql}")).unwrap();
        match route(state, ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let haystack = String::from_utf8_lossy(&b).into_owned();
                assert!(
                    haystack.contains(expected_plan),
                    "EXPLAIN of `{select_cql}` must contain `{expected_plan}`, got: {haystack}"
                );
                assert!(
                    !haystack.contains("FullScan"),
                    "EXPLAIN of `{select_cql}` must not be FullScan, got: {haystack}"
                );
            }
            _ => panic!("expected Result from EXPLAIN"),
        }
    }

    /// BTree (default) index: `WHERE col = ?` without a PK predicate plans
    /// `SingleIndex`, returns the matching row, and increments index_usage.
    #[tokio::test]
    async fn btree_index_usage_observable_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE bt WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE bt.users (id int PRIMARY KEY, email text)",
            "CREATE INDEX bt_email ON bt.users (email) USING 'btree'",
            "INSERT INTO bt.users (id, email) VALUES (1, 'a@x.com')",
            "INSERT INTO bt.users (id, email) VALUES (2, 'b@x.com')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let q = "SELECT id FROM bt.users WHERE email = 'a@x.com'";
        assert_explain_plan(&state, &ctx, q, "SingleIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "btree index lookup must return exactly 1 row");
    }

    /// Hash index: `WHERE col = ?` (POINT_LOOKUP) plans `SingleIndex`, returns
    /// the matching row, and increments index_usage.
    #[tokio::test]
    async fn hash_index_usage_observable_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE hsh WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE hsh.sessions (id int PRIMARY KEY, user_id text)",
            "CREATE INDEX hsh_user ON hsh.sessions (user_id) USING 'hash'",
            "INSERT INTO hsh.sessions (id, user_id) VALUES (1, 'u-100')",
            "INSERT INTO hsh.sessions (id, user_id) VALUES (2, 'u-200')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let q = "SELECT id FROM hsh.sessions WHERE user_id = 'u-100'";
        assert_explain_plan(&state, &ctx, q, "SingleIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "hash index lookup must return exactly 1 row");
    }

    /// Composite index: `WHERE col = ?` plans `SingleIndex`, returns the
    /// matching row, and increments index_usage.
    #[tokio::test]
    async fn composite_index_usage_observable_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE cmp WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE cmp.events (id int PRIMARY KEY, region text)",
            "CREATE INDEX cmp_region ON cmp.events (region) USING 'composite'",
            "INSERT INTO cmp.events (id, region) VALUES (1, 'us-east')",
            "INSERT INTO cmp.events (id, region) VALUES (2, 'eu-west')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let q = "SELECT id FROM cmp.events WHERE region = 'us-east'";
        assert_explain_plan(&state, &ctx, q, "SingleIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "composite index lookup must return exactly 1 row");
    }

    /// Phonetic index: `WHERE name = 'Jon'` matches 'John' via the phonetic
    /// index path, returns the row, and increments index_usage.
    #[tokio::test]
    async fn phonetic_index_usage_observable_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE phu WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE phu.people (id int PRIMARY KEY, name text)",
            "CREATE INDEX phu_name ON phu.people (name) USING 'phonetic'",
            "INSERT INTO phu.people (id, name) VALUES (1, 'John')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let q = "SELECT id FROM phu.people WHERE name = 'Jon'";
        assert_explain_plan(&state, &ctx, q, "SingleIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "phonetic index must match 'Jon' to 'John'");
    }

    /// Filtered (partial) index, happy path: index `name`, partial on
    /// `status = 'active'`. A query that implies the predicate
    /// (`WHERE name = v AND status = 'active'`) is served by the filtered
    /// index — EXPLAIN reports the index (not FullScan), index_usage records a
    /// "Filtered" hit, and ONLY the matching+active row is returned.
    #[tokio::test]
    async fn filtered_index_used_when_query_implies_predicate() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE flt WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE flt.users (id int PRIMARY KEY, name text, status text)",
            "CREATE INDEX flt_name_active ON flt.users (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'status','filter_op':'=','filter_value':'active'}",
            // alice active, alice inactive (same name, different status), bob active.
            "INSERT INTO flt.users (id, name, status) VALUES (1, 'alice', 'active')",
            "INSERT INTO flt.users (id, name, status) VALUES (2, 'alice', 'inactive')",
            "INSERT INTO flt.users (id, name, status) VALUES (3, 'bob', 'active')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        let q = "SELECT id FROM flt.users WHERE name = 'alice' AND status = 'active'";
        // EXPLAIN must report the filtered index (rendered as SingleIndex), not FullScan.
        assert_explain_plan(&state, &ctx, q, "flt_name_active").await;
        // The filtered index is consulted (index_usage advances by 1) and ONLY
        // the active alice row (id=1) is returned — not the inactive alice (id=2).
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(
            count, 1,
            "filtered index must return only the active 'alice' row"
        );
    }

    /// Filtered (partial) index, soundness: a query that does NOT imply the
    /// index's predicate (`WHERE name = v`, no `status`) must NOT be served from
    /// the partial index — that would silently drop the inactive 'alice'. EXPLAIN
    /// must not name the filtered index, no "Filtered" index hit is recorded, and
    /// the full-scan fallback returns BOTH alices (complete result).
    #[tokio::test]
    async fn filtered_index_withheld_when_query_does_not_imply_predicate() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE fls WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fls.users (id int PRIMARY KEY, name text, status text)",
            "CREATE INDEX fls_name_active ON fls.users (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'status','filter_op':'=','filter_value':'active'}",
            "INSERT INTO fls.users (id, name, status) VALUES (1, 'alice', 'active')",
            "INSERT INTO fls.users (id, name, status) VALUES (2, 'alice', 'inactive')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // No `status` predicate => the partial index does not imply the query.
        let q = "SELECT id FROM fls.users WHERE name = 'alice'";

        // EXPLAIN must NOT report the filtered index; it falls to FullScan.
        let stmt = crate::parser::parse(&format!("EXPLAIN {q}")).unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let haystack = String::from_utf8_lossy(&b).into_owned();
                assert!(
                    !haystack.contains("fls_name_active"),
                    "partial index must not be used without an implied predicate, got: {haystack}"
                );
                assert!(
                    haystack.contains("FullScan"),
                    "query that does not imply the predicate must fall to FullScan, got: {haystack}"
                );
            }
            _ => panic!("expected Result from EXPLAIN"),
        }

        // The full-scan fallback must return BOTH alices (active + inactive):
        // using the partial index here would silently drop the inactive row.
        // No "Filtered" index hit is recorded (delta 0).
        let count = assert_index_hit_and_count(&state, &ctx, q, 0).await;
        assert_eq!(
            count, 2,
            "without the implied predicate, both 'alice' rows must be returned (completeness)"
        );
    }

    /// Filtered (partial) index, RANGE-implication soundness (true positives):
    /// a partial index `age > 21` must serve queries whose value-set is a
    /// provable subset of `{age > 21}` — `age = 30`, `age > 25`, and `age >= 22`.
    /// Each is index-accelerated (EXPLAIN names the index, not FullScan) and
    /// returns exactly the implied rows. Built on an `int` column so the storage
    /// encoding is the positive big-endian range where byte-order == value-order.
    #[tokio::test]
    async fn filtered_index_used_when_query_implies_range_predicate() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE fra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fra.people (id int PRIMARY KEY, name text, age int)",
            // Partial index on `name`, retaining only rows with age > 21.
            "CREATE INDEX fra_name_adult ON fra.people (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'age','filter_op':'>','filter_value':'21'}",
            "INSERT INTO fra.people (id, name, age) VALUES (1, 'alice', 30)",
            "INSERT INTO fra.people (id, name, age) VALUES (2, 'alice', 18)",
            "INSERT INTO fra.people (id, name, age) VALUES (3, 'alice', 26)",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // age = 30 ⊆ {age > 21}: implied. Only the age=30 alice (id=1) qualifies.
        let q = "SELECT id FROM fra.people WHERE name = 'alice' AND age = 30";
        assert_explain_plan(&state, &ctx, q, "fra_name_adult").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(
            count, 1,
            "age=30 implies age>21 and selects only the 30 row"
        );

        // age > 25 ⊆ {age > 21}: implied. alice rows with age>25 are id=1 (30)
        // and id=3 (26); both are in the index, so the index serves them.
        let q = "SELECT id FROM fra.people WHERE name = 'alice' AND age > 25";
        assert_explain_plan(&state, &ctx, q, "fra_name_adult").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(
            count, 2,
            "age>25 implies age>21 and selects the 30 and 26 rows"
        );

        // age >= 22 ⊆ {age > 21}: implied (22 > 21). Same two rows.
        let q = "SELECT id FROM fra.people WHERE name = 'alice' AND age >= 22";
        assert_explain_plan(&state, &ctx, q, "fra_name_adult").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 2, "age>=22 implies age>21");
    }

    /// Filtered (partial) index, RANGE-implication soundness (withheld cases):
    /// a query whose filter-column value-set is NOT a provable subset of the
    /// index's retained set MUST fall to FullScan so no qualifying row is
    /// dropped. For a `age > 21` partial index, `age > 10` and `age >= 21` both
    /// admit rows the index excludes (e.g. age 21 itself, age 18), so the index
    /// is withheld and the complete result set is returned.
    #[tokio::test]
    async fn filtered_index_withheld_when_query_does_not_imply_range_predicate() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE frw WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE frw.people (id int PRIMARY KEY, name text, age int)",
            "CREATE INDEX frw_name_adult ON frw.people (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'age','filter_op':'>','filter_value':'21'}",
            // Three alices: age 30 (in index), age 21 (boundary, excluded),
            // age 18 (excluded).
            "INSERT INTO frw.people (id, name, age) VALUES (1, 'alice', 30)",
            "INSERT INTO frw.people (id, name, age) VALUES (2, 'alice', 21)",
            "INSERT INTO frw.people (id, name, age) VALUES (3, 'alice', 18)",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // Assert a query is withheld (the partial index is not named and the
        // plan is FullScan, proving soundness) and that the complete result set
        // is still returned. Because withholding leaves `age` genuinely
        // unindexed, the value-returning query must use ALLOW FILTERING — which
        // is exactly the safe fallback we want: a full scan with a precise
        // post-filter, never a silently incomplete index read.
        async fn assert_withheld_full(
            state: &SharedState,
            ctx: &RequestContext<'_>,
            q: &str,
            index_name: &str,
            expected_count: i32,
        ) {
            let stmt = crate::parser::parse(&format!("EXPLAIN {q}")).unwrap();
            match route(state, ctx, stmt).await.unwrap() {
                RouteResult::Result(b) => {
                    let haystack = String::from_utf8_lossy(&b).into_owned();
                    assert!(
                        !haystack.contains(index_name),
                        "partial index must be withheld for `{q}`, got: {haystack}"
                    );
                    assert!(
                        haystack.contains("FullScan"),
                        "`{q}` must fall to FullScan, got: {haystack}"
                    );
                }
                _ => panic!("expected Result from EXPLAIN"),
            }
            // No partial-index hit is recorded; the complete set is returned via
            // the full-scan + post-filter path.
            let count =
                assert_index_hit_and_count(state, ctx, &format!("{q} ALLOW FILTERING"), 0).await;
            assert_eq!(
                count, expected_count,
                "`{q}` must return the complete set ({expected_count} rows)"
            );
        }

        // age > 10 admits age=18 and age=21, neither in the {age>21} index.
        assert_withheld_full(
            &state,
            &ctx,
            "SELECT id FROM frw.people WHERE name = 'alice' AND age > 10",
            "frw_name_adult",
            3,
        )
        .await;

        // age >= 21 admits age=21 (the index excludes the boundary).
        assert_withheld_full(
            &state,
            &ctx,
            "SELECT id FROM frw.people WHERE name = 'alice' AND age >= 21",
            "frw_name_adult",
            2,
        )
        .await;
    }

    /// Filtered (partial) index with a MULTI-COLUMN conjunction predicate
    /// (`age > 21 AND dept = 'eng'`). The index retains rows where BOTH clauses
    /// hold. A query implying both clauses uses the index and returns exactly
    /// the matching rows; a query implying only ONE clause is withheld (serving
    /// it would drop rows) and, with ALLOW FILTERING, returns the complete set.
    #[tokio::test]
    async fn filtered_index_multi_column_conjunction_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE fmc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fmc.people (id int PRIMARY KEY, name text, age int, dept text)",
            // Partial index on `name`, retaining only rows with age > 21 AND
            // dept = 'eng' (the multi-column conjunction `filter` form).
            "CREATE INDEX fmc_eng_adults ON fmc.people (name) USING 'filtered' \
             WITH OPTIONS = {'filter': 'age > 21 AND dept = ''eng'''}",
            // id=1: age 30, eng  -> BOTH clauses hold  -> indexed.
            "INSERT INTO fmc.people (id, name, age, dept) VALUES (1, 'alice', 30, 'eng')",
            // id=2: age 18, eng  -> age fails           -> excluded.
            "INSERT INTO fmc.people (id, name, age, dept) VALUES (2, 'alice', 18, 'eng')",
            // id=3: age 40, sales-> dept fails          -> excluded.
            "INSERT INTO fmc.people (id, name, age, dept) VALUES (3, 'alice', 40, 'sales')",
            // id=4: age 26, eng  -> BOTH clauses hold   -> indexed.
            "INSERT INTO fmc.people (id, name, age, dept) VALUES (4, 'alice', 26, 'eng')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // Query implies BOTH clauses: age = 30 ⊆ {age>21} and dept = 'eng' ⊆
        // {dept='eng'}. The index is usable and returns exactly id=1.
        let q = "SELECT id FROM fmc.people WHERE name = 'alice' AND age = 30 AND dept = 'eng'";
        assert_explain_plan(&state, &ctx, q, "fmc_eng_adults").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "age=30 AND dept=eng implies both clauses, 1 row");

        // Query implies BOTH via a range on age: age > 25 ⊆ {age>21}, dept eng.
        // id=1 (30) and id=4 (26) are both in the index.
        let q = "SELECT id FROM fmc.people WHERE name = 'alice' AND age > 25 AND dept = 'eng'";
        assert_explain_plan(&state, &ctx, q, "fmc_eng_adults").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 2, "age>25 AND dept=eng implies both clauses, 2 rows");

        // Withheld: implies ONLY the age clause (dept is unconstrained). Using
        // the index would drop the genuinely-retained eng rows that this query
        // also wants via the dept-agnostic predicate; serving it is unsound.
        // The plan must be FullScan and, with ALLOW FILTERING, return the full
        // set of alices with age>25 (id=1 age30 eng, id=3 age40 sales, id=4
        // age26 eng).
        let q = "SELECT id FROM fmc.people WHERE name = 'alice' AND age > 25";
        {
            let stmt = crate::parser::parse(&format!("EXPLAIN {q}")).unwrap();
            match route(&state, &ctx, stmt).await.unwrap() {
                RouteResult::Result(b) => {
                    let haystack = String::from_utf8_lossy(&b).into_owned();
                    assert!(
                        !haystack.contains("fmc_eng_adults"),
                        "partial index must be withheld when only one clause is implied, got: {haystack}"
                    );
                    assert!(
                        haystack.contains("FullScan"),
                        "withheld query must fall to FullScan, got: {haystack}"
                    );
                }
                _ => panic!("expected Result from EXPLAIN"),
            }
            let count =
                assert_index_hit_and_count(&state, &ctx, &format!("{q} ALLOW FILTERING"), 0).await;
            assert_eq!(
                count, 3,
                "withheld query returns the complete set (3 alices with age>25)"
            );
        }

        // Withheld: implies ONLY the dept clause (age unconstrained).
        let q = "SELECT id FROM fmc.people WHERE name = 'alice' AND dept = 'eng'";
        {
            let stmt = crate::parser::parse(&format!("EXPLAIN {q}")).unwrap();
            match route(&state, &ctx, stmt).await.unwrap() {
                RouteResult::Result(b) => {
                    let haystack = String::from_utf8_lossy(&b).into_owned();
                    assert!(
                        !haystack.contains("fmc_eng_adults"),
                        "partial index must be withheld when only the dept clause is implied, got: {haystack}"
                    );
                }
                _ => panic!("expected Result from EXPLAIN"),
            }
            // All eng alices regardless of age: id=1 (30), id=2 (18), id=4 (26).
            let count =
                assert_index_hit_and_count(&state, &ctx, &format!("{q} ALLOW FILTERING"), 0).await;
            assert_eq!(
                count, 3,
                "withheld dept-only query returns all 3 eng alices"
            );
        }
    }

    /// CREATE INDEX ... USING 'filtered' with a missing/invalid predicate option
    /// must fail loud at CREATE time rather than registering an unfiltered index.
    #[tokio::test]
    async fn filtered_index_create_rejects_invalid_options() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE flv WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE flv.users (id int PRIMARY KEY, name text, status text)",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }

        // Run a CREATE that must fail and return the rendered error string.
        async fn expect_create_err(
            state: &SharedState,
            ctx: &RequestContext<'_>,
            cql: &str,
        ) -> String {
            match route(state, ctx, crate::parser::parse(cql).unwrap()).await {
                Ok(_) => panic!("expected CREATE INDEX to be rejected: {cql}"),
                Err(e) => format!("{e:?}"),
            }
        }

        // Missing filter options entirely.
        let err = expect_create_err(
            &state,
            &ctx,
            "CREATE INDEX flv_bad ON flv.users (name) USING 'filtered'",
        )
        .await;
        assert!(
            err.contains("filter_column"),
            "error should mention the missing filter_column option, got: {err}"
        );

        // Unknown filter_op.
        let err = expect_create_err(
            &state,
            &ctx,
            "CREATE INDEX flv_bad ON flv.users (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'status','filter_op':'~~','filter_value':'active'}",
        )
        .await;
        assert!(
            err.contains("filter_op"),
            "error should mention the invalid filter_op, got: {err}"
        );

        // filter_column not a real column.
        let err = expect_create_err(
            &state,
            &ctx,
            "CREATE INDEX flv_bad ON flv.users (name) USING 'filtered' \
             WITH OPTIONS = {'filter_column':'nope','filter_op':'=','filter_value':'active'}",
        )
        .await;
        assert!(
            err.contains("nope"),
            "error should mention the unknown filter_column, got: {err}"
        );
    }

    /// Full-text index: `WHERE body = fts_match(...)` consults the FTI, returns
    /// the matching rows, and increments index_usage.
    #[tokio::test]
    async fn fulltext_index_usage_observable_end_to_end() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE ftu WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE ftu.docs (id int PRIMARY KEY, body text)",
            "CREATE INDEX ftu_body ON ftu.docs (body) USING 'fulltext'",
            "INSERT INTO ftu.docs (id, body) VALUES (1, 'rust is a fast distributed database')",
            "INSERT INTO ftu.docs (id, body) VALUES (2, 'hello world')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        state
            .engine
            .flush(&ferrosa_storage::TableId::new("ftu", "docs"))
            .unwrap();
        let q = "SELECT id FROM ftu.docs WHERE body = fts_match('distributed AND database')";
        // EXPLAIN must report the full-text index, not a FullScan: the FTS path
        // is genuinely accelerated via `engine.fulltext_search`, and EXPLAIN now
        // reflects that (closing the prior FullText EXPLAIN gap).
        assert_explain_plan(&state, &ctx, q, "FullTextIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 1, "fts_match must return exactly the matching doc");
    }

    /// Set up the `geo.places` schema, the geo index, and the sample points
    /// from `examples/geospatial.cql`. Returns the state + ctx-owning auth.
    async fn setup_geo(state: &SharedState, ctx: &RequestContext<'_>) {
        for cql in [
            "CREATE KEYSPACE geo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE geo.places (id int PRIMARY KEY, name text, location frozen<tuple<double, double>>)",
            "CREATE INDEX places_location_geo ON geo.places (location) USING 'geo'",
            "INSERT INTO geo.places (id, name, location) VALUES (1, 'SF Ferry Building',   (37.7955, -122.3937))",
            "INSERT INTO geo.places (id, name, location) VALUES (2, 'SF Union Square',     (37.7880, -122.4074))",
            "INSERT INTO geo.places (id, name, location) VALUES (3, 'SF Golden Gate Park', (37.7694, -122.4862))",
            "INSERT INTO geo.places (id, name, location) VALUES (4, 'NYC Times Square',    (40.7580,  -73.9855))",
            "INSERT INTO geo.places (id, name, location) VALUES (5, 'Dateline East',       (0.0000,  179.9900))",
            "INSERT INTO geo.places (id, name, location) VALUES (6, 'Dateline West',       (0.0000, -179.9900))",
            "INSERT INTO geo.places (id, name, location) VALUES (7, 'Near North Pole',     (89.9000,    0.0000))",
        ] {
            route(state, ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
    }

    /// Run a SELECT and return its result rows as `(id, name)` pairs, in result
    /// order. Panics on any non-Result outcome.
    async fn run_geo_select(
        state: &SharedState,
        ctx: &RequestContext<'_>,
        cql: &str,
    ) -> Vec<(i32, String)> {
        let stmt = crate::parser::parse(cql).unwrap();
        let body = match route(state, ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result for `{cql}`"),
        };
        decode_id_name_rows(&body)
    }

    /// Geo `GEO_NEAREST OF` k-NN: the three SF places rank ahead of NYC.
    #[tokio::test]
    async fn geo_nearest_returns_k_nearest_in_order() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        let q = "SELECT id, name FROM geo.places \
                 ORDER BY location GEO_NEAREST OF (37.7880, -122.4074) LIMIT 3";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let rows = run_geo_select(&state, &ctx, q).await;
        assert_eq!(rows.len(), 3, "LIMIT 3 must bound the result");
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        // The three SF places (ids 1,2,3) are nearest to Union Square; NYC (4)
        // and the dateline/pole points are all farther.
        assert!(
            ids.contains(&1) && ids.contains(&2) && ids.contains(&3),
            "nearest 3 must be the SF cluster, got {ids:?}"
        );
        // Union Square itself is the closest (distance 0).
        assert_eq!(
            rows[0].0, 2,
            "Union Square is the query point and ranks first"
        );
    }

    /// Geo `GEO_WITHIN_RADIUS`: a 3km radius around the Ferry Building keeps the
    /// downtown SF places and drops Golden Gate Park + NYC.
    #[tokio::test]
    async fn geo_within_radius_filters_by_exact_distance() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        let q = "SELECT id, name FROM geo.places \
                 WHERE GEO_WITHIN_RADIUS(location, (37.7955, -122.3937), 3000)";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        let rows = run_geo_select(&state, &ctx, q).await;
        let mut ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2],
            "3km radius keeps Ferry Building + Union Square, drops GG Park & NYC"
        );
        assert_eq!(count, 2);
    }

    /// Geo `GEO_WITHIN_BBOX`: a central-SF box returns only the SF places.
    #[tokio::test]
    async fn geo_within_bbox_filters_to_central_sf() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        let q = "SELECT id, name FROM geo.places \
                 WHERE GEO_WITHIN_BBOX(location, (37.70, -122.52), (37.83, -122.35))";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let rows = run_geo_select(&state, &ctx, q).await;
        let mut ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "central-SF bbox returns all three SF places only"
        );
    }

    /// Geo `GEO_WITHIN_BBOX` across the antimeridian: SW lon 179, NE lon -179
    /// must return BOTH dateline points (ids 5 and 6).
    #[tokio::test]
    async fn geo_within_bbox_antimeridian_returns_both_dateline_points() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        let q = "SELECT id, name FROM geo.places \
                 WHERE GEO_WITHIN_BBOX(location, (-1.0, 179.0), (1.0, -179.0))";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let rows = run_geo_select(&state, &ctx, q).await;
        let mut ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![5, 6],
            "antimeridian bbox must return both dateline points, not zero"
        );
    }

    /// Geo `ST_WITHIN`: a polygon around central SF returns only the SF places
    /// (ids 1, 2, 3) and excludes NYC and the dateline points. The query must
    /// hit the geo index (EXPLAIN reports GeoIndex, not FullScan) and advance
    /// the index_usage counter.
    #[tokio::test]
    async fn geo_st_within_polygon_filters_to_central_sf() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        // A quadrilateral hugging central SF: contains the three SF points,
        // excludes NYC (≈ lon -74) and both dateline points (lon ≈ ±180).
        let q = "SELECT id, name FROM geo.places \
                 WHERE ST_WITHIN(location, ((37.70, -122.52), (37.83, -122.52), \
                 (37.83, -122.35), (37.70, -122.35)))";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        let rows = run_geo_select(&state, &ctx, q).await;
        let mut ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "central-SF polygon returns all three SF places only"
        );
        assert_eq!(count, 3);
    }

    /// Geo `ST_WITHIN` with a concave polygon: a point inside the bounding box
    /// but outside the actual polygon (in a notch) must be excluded. This proves
    /// the exact point-in-polygon refinement runs, not just the bbox cover.
    #[tokio::test]
    async fn geo_st_within_concave_polygon_excludes_notch_point() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        setup_geo(&state, &ctx).await;

        // Add two points: one squarely inside, one in the concave notch (inside
        // the bbox but outside the polygon).
        for cql in [
            "INSERT INTO geo.places (id, name, location) VALUES (10, 'Body',  (0.5, 2.0))",
            "INSERT INTO geo.places (id, name, location) VALUES (11, 'Notch', (1.5, 1.0))",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap();
        }

        // The concave "U" polygon from the geometry unit tests.
        let q = "SELECT id, name FROM geo.places \
                 WHERE ST_WITHIN(location, ((0.0, 0.0), (0.0, 4.0), (4.0, 4.0), \
                 (4.0, 0.0), (2.0, 0.0), (2.0, 3.0), (1.0, 3.0), (1.0, 0.0)))";
        assert_explain_plan(&state, &ctx, q, "GeoIndex").await;
        let rows = run_geo_select(&state, &ctx, q).await;
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![10],
            "body point in, notch point excluded by refine"
        );
    }

    /// Vector index: an `ORDER BY ... ANN OF` query returns the nearest rows in
    /// distance order, and the vector index is registered in storage. The
    /// router ANN path is brute-force pending Phase 2 vector read dispatch, so
    /// this asserts correctness + index registration rather than an
    /// index_usage hit it cannot honestly demonstrate.
    #[tokio::test]
    async fn vector_index_registered_and_ann_orders_correctly() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE vec WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE vec.items (id int PRIMARY KEY, embedding vector<float, 4>)",
            "CREATE INDEX vec_ann ON vec.items (embedding) USING 'vector'",
            "INSERT INTO vec.items (id, embedding) VALUES (1, [0.90, 0.10, 0.00, 0.00])",
            "INSERT INTO vec.items (id, embedding) VALUES (2, [0.00, 0.00, 0.90, 0.10])",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        // The vector index must be registered with the engine (HNSW default).
        assert_eq!(
            state
                .engine
                .vector_index_method(&ferrosa_storage::TableId::new("vec", "items"), "vec_ann")
                .unwrap(),
            ferrosa_storage::VectorIndexMethod::Hnsw,
            "vector index must be registered with the default HNSW method"
        );
        // ANN query returns the nearest row first.
        let stmt = crate::parser::parse(
            "SELECT id FROM vec.items ORDER BY embedding ANN OF [0.90, 0.10, 0.00, 0.00] LIMIT 1",
        )
        .unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(count, 1, "ANN OF LIMIT 1 must return exactly 1 row");
            }
            _ => panic!("expected Result"),
        }
    }

    /// `ORDER BY col ANN OF [...]` on a vector-indexed column must be served by
    /// the index consult, observable as a `VectorAnn` EXPLAIN plan AND an
    /// `index_usage` hit — never a silent full scan. Storage-layer ordering
    /// correctness (nearest-first by score) is covered by
    /// `ann_search_partitions_returns_nearest_rows_with_pk_in_score_order`.
    #[tokio::test]
    async fn ann_query_consults_vector_index_and_is_observable() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE annobs WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE annobs.papers (id int PRIMARY KEY, embedding vector<float, 3>)",
            "CREATE INDEX papers_ann ON annobs.papers (embedding) USING 'vector'",
            "INSERT INTO annobs.papers (id, embedding) VALUES (1, [1.0, 0.0, 0.0])",
            "INSERT INTO annobs.papers (id, embedding) VALUES (2, [0.9, 0.1, 0.0])",
            "INSERT INTO annobs.papers (id, embedding) VALUES (3, [0.7, 0.3, 0.0])",
            "INSERT INTO annobs.papers (id, embedding) VALUES (4, [0.5, 0.5, 0.0])",
            "INSERT INTO annobs.papers (id, embedding) VALUES (5, [0.0, 0.0, 1.0])",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let q = "SELECT id FROM annobs.papers ORDER BY embedding ANN OF [1.0, 0.0, 0.0] LIMIT 3";
        // EXPLAIN must report the vector index, not a full scan.
        assert_explain_plan(&state, &ctx, q, "VectorAnn").await;
        // Executing the query records exactly one index hit (the consult) and
        // returns k rows — proving the index path ran, not a full scan.
        let count = assert_index_hit_and_count(&state, &ctx, q, 1).await;
        assert_eq!(count, 3, "ANN OF LIMIT 3 must return exactly k=3 rows");
    }

    /// Full-scan observability: a `WHERE col = ?` with no index and ALLOW
    /// FILTERING must plan `FullScan`, record into the full_scan_tracker (so
    /// `system_observability.full_scan_reasons` surfaces it), and return the
    /// correct rows. This is the negative counterpart to the index-hit tests.
    #[tokio::test]
    async fn full_scan_recorded_when_no_index() {
        let (state, _dir) = setup();
        let auth = dev_auth();
        let ks = None;
        let ctx = test_ctx(&auth, &ks);
        for cql in [
            "CREATE KEYSPACE fsr WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE fsr.t (id int PRIMARY KEY, color text)",
            "INSERT INTO fsr.t (id, color) VALUES (1, 'red')",
            "INSERT INTO fsr.t (id, color) VALUES (2, 'blue')",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{cql}: {e:?}"));
        }
        let before = state.full_scan_tracker.total_full_scans();
        let stmt = crate::parser::parse("SELECT id FROM fsr.t WHERE color = 'red' ALLOW FILTERING")
            .unwrap();
        let count = match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => extract_row_count(&b),
            _ => panic!("expected Result"),
        };
        assert_eq!(count, 1, "full scan with post-filter must return 1 row");
        assert_eq!(
            state.full_scan_tracker.total_full_scans() - before,
            1,
            "an unindexed WHERE must record exactly one full scan"
        );
    }

    /// `CREATE INDEX` followed by `INSERT` then `SELECT WHERE indexed_col = ?`
    /// must return the inserted row via the memtable index.  This verifies the
    /// full wire-up: router calls engine.add_index(), the TableStore's
    /// indexed_columns is updated, and the next write is indexed.
    #[tokio::test]
    async fn create_index_wires_memtable_indexing_end_to_end() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace and table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE idx_wire WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE idx_wire.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create index on `email` — this must wire engine.add_index()
        let stmt =
            crate::parser::parse("CREATE INDEX wire_email_idx ON idx_wire.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert a row AFTER the index was created
        let stmt = crate::parser::parse(
            "INSERT INTO idx_wire.users (id, email) VALUES (42, 'bob@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query by indexed column — must find the row
        let stmt = crate::parser::parse(
            "SELECT id, email FROM idx_wire.users WHERE email = 'bob@example.com'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "SELECT on indexed column should succeed: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "should find exactly 1 row via memtable index, got {count}"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    // ── CQL built-in function tests ─────────────────────────────────────

    #[tokio::test]
    async fn cql_function_now() {
        // Test crate::bridge::eval_now() directly: should produce a v1 UUID.
        let timeuuid = crate::bridge::eval_now();
        match timeuuid {
            CqlValue::Timeuuid(uuid) => {
                let bytes = uuid.as_bytes();
                let version = (bytes[6] >> 4) & 0x0F;
                assert_eq!(version, 1, "now() should return a v1 UUID, got v{version}");
            }
            other => panic!("expected Timeuuid, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn cql_function_to_timestamp() {
        // Test crate::bridge::eval_now() + eval_to_timestamp() directly.
        let timeuuid = crate::bridge::eval_now();
        let ts = super::eval_to_timestamp(&timeuuid).expect("toTimestamp should succeed");

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        match ts {
            CqlValue::Timestamp(millis) => {
                let diff = (now_millis - millis).abs();
                assert!(
                    diff < 5000,
                    "toTimestamp(now()) should be within 5s of current time, diff={diff}ms"
                );
            }
            other => panic!("expected Timestamp, got {:?}", other),
        }
    }

    // ── BUG-024: PREPARE metadata must reflect ALTER TABLE changes ─────────

    /// BUG-024 Step 2: After ALTER TABLE ADD, PREPARE sees the new column.
    ///
    /// Table starts with (pk text, a int, b text). ALTER TABLE ADD c boolean.
    /// PREPARE of INSERT (pk, a, b, c) must report 4 bound columns, not 3.
    #[tokio::test]
    async fn prepare_after_alter_table_add_column() {
        use bytes::{BufMut, BytesMut};

        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE bug024 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("bug024".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // CREATE TABLE with 3 columns (pk, a, b)
        route(
            &state,
            &ctx_ks,
            crate::parser::parse("CREATE TABLE bug024.t (pk text PRIMARY KEY, a int, b text)")
                .unwrap(),
        )
        .await
        .unwrap();

        // ALTER TABLE ADD c boolean
        route(
            &state,
            &ctx_ks,
            crate::parser::parse("ALTER TABLE bug024.t ADD c boolean").unwrap(),
        )
        .await
        .unwrap();

        // PREPARE INSERT with all 4 columns (including the added one)
        let query = "INSERT INTO bug024.t (pk, a, b, c) VALUES (?, ?, ?, ?)";
        let query_bytes = query.as_bytes();
        let mut body = BytesMut::new();
        body.put_i32(query_bytes.len() as i32);
        body.put_slice(query_bytes);

        let result = crate::connection::handle_prepare(
            &mut None,
            &mut Some("bug024".into()),
            &state,
            &body.freeze(),
        )
        .await;

        match result {
            crate::connection::HandleResult::Reply(opcode, body) => {
                assert_eq!(opcode, crate::frame::Opcode::Result);
                assert_eq!(
                    &body[0..4],
                    &0x0004i32.to_be_bytes(),
                    "result kind must be Prepared"
                );
                let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                let mut off = 6 + id_len;
                let _bind_flags = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                off += 4;
                let bind_col_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
                assert_eq!(
                    bind_col_count, 4,
                    "PREPARE must see 4 bound columns after ALTER TABLE ADD c; got {bind_col_count}"
                );
            }
            _ => panic!("expected Reply from handle_prepare"),
        }
    }

    /// BUG-024 Step 4: After ALTER TABLE DROP, PREPARE does not include the
    /// dropped column in result metadata.
    ///
    /// Table (pk text, a int, b text, c boolean) → DROP c → PREPARE SELECT
    /// pk, a, b must report 3 result columns (not 4) and 1 bound column for
    /// the WHERE pk = ? predicate.
    ///
    /// Prepared result wire layout (CQL native protocol v4+):
    ///   [i32 kind=0x0004]
    ///   [u16 id_len=16][16 bytes id]
    ///   Bind metadata:
    ///     [i32 flags]       — 0x0001 = Global_tables_spec
    ///     [i32 col_count]
    ///     [i32 pk_count]    — number of pk_indexes that follow
    ///     pk_count × [i16 pk_index]
    ///     If flags & 0x0001: [u16 ks_len][ks][u16 tbl_len][tbl]
    ///     col_count × ([u16 name_len][name][u16 type_id][type_params...])
    ///   Result metadata:
    ///     [i32 flags]
    ///     [i32 col_count]
    ///     If flags & 0x0001: [u16 ks_len][ks][u16 tbl_len][tbl]
    ///     col_count × ([u16 name_len][name][u16 type_id][type_params...])
    #[tokio::test]
    async fn prepare_after_alter_table_drop_column() {
        use bytes::{BufMut, BytesMut};

        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        route(
            &state,
            &ctx,
            crate::parser::parse(
                "CREATE KEYSPACE bug024b WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("bug024b".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // CREATE TABLE with 4 columns including c
        route(
            &state,
            &ctx_ks,
            crate::parser::parse(
                "CREATE TABLE bug024b.t (pk text PRIMARY KEY, a int, b text, c boolean)",
            )
            .unwrap(),
        )
        .await
        .unwrap();

        // ALTER TABLE DROP c
        route(
            &state,
            &ctx_ks,
            crate::parser::parse("ALTER TABLE bug024b.t DROP c").unwrap(),
        )
        .await
        .unwrap();

        // PREPARE SELECT pk, a, b (does not reference dropped column c)
        let query = "SELECT pk, a, b FROM bug024b.t WHERE pk = ?";
        let query_bytes = query.as_bytes();
        let mut body = BytesMut::new();
        body.put_i32(query_bytes.len() as i32);
        body.put_slice(query_bytes);

        let result = crate::connection::handle_prepare(
            &mut None,
            &mut Some("bug024b".into()),
            &state,
            &body.freeze(),
        )
        .await;

        match result {
            crate::connection::HandleResult::Reply(opcode, body) => {
                assert_eq!(opcode, crate::frame::Opcode::Result);
                assert_eq!(
                    &body[0..4],
                    &0x0004i32.to_be_bytes(),
                    "result kind must be Prepared"
                );

                // kind(4) + id_len(2) + id(16) = 22 bytes before bind metadata
                let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                let mut pos = 6 + id_len; // skip kind + id_len + id

                // --- Bind metadata ---
                let bind_flags = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let bind_col_count = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                pos += 4;
                assert_eq!(
                    bind_col_count, 1,
                    "SELECT pk, a, b WHERE pk = ? must have 1 bound column; got {bind_col_count}"
                );

                // pk_count + pk_indexes (CQL protocol v4+ addition)
                let pk_count = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                pos += 4;
                pos += pk_count as usize * 2; // each pk_index is i16

                // global_tables_spec: ks + table strings
                if bind_flags & 0x0001 != 0 {
                    let ks_len =
                        u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + ks_len;
                    let tbl_len =
                        u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + tbl_len;
                }

                // skip each bound column spec: [u16 name_len][name][u16 type_id][type_params]
                for _ in 0..bind_col_count {
                    if bind_flags & 0x0001 == 0 {
                        // per-column ks + table when no global spec
                        let ks_len =
                            u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
                        pos += 2 + ks_len;
                        let tbl_len =
                            u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
                        pos += 2 + tbl_len;
                    }
                    let name_len =
                        u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + name_len;
                    let type_id = u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap());
                    pos += 2;
                    // advance past any type parameters
                    match type_id {
                        0x0020 | 0x0022 => pos += 2, // List or Set: 1 extra u16
                        0x0021 => pos += 4,          // Map: 2 extra u16s
                        _ => {}                      // simple types: nothing extra
                    }
                }

                // --- Result metadata ---
                let result_flags = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let result_col_count = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());

                // Result must NOT use No_metadata (0x0004) — SELECT always has columns
                assert_ne!(
                    result_flags & 0x0004,
                    0x0004,
                    "SELECT result metadata must not use No_metadata flag"
                );
                assert_eq!(
                    result_col_count, 3,
                    "SELECT pk, a, b must produce 3 result columns after DROP c; got {result_col_count}"
                );
            }
            _ => panic!("expected Reply from handle_prepare"),
        }
    }

    // ── FT-011: fts_match() via storage engine ───────────────────────────────

    /// Verifies `fts_match` basic wiring at the storage-engine level:
    /// inserts 3 rows with different text, writes an FTI sidecar, flushes,
    /// then calls `engine.fulltext_search` to find 2 rows containing a term.
    #[test]
    fn fts_match_simple_query() {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        use ferrosa_index::fulltext::builder::FullTextIndexBuilder;
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
            SyncStrategyConfig,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        };
        let engine_config = StorageEngineConfig {
            commit_log,
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        let engine = StorageEngine::new(engine_config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "fts_ks".to_string(),
            table: "articles".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();

        let table_id = ferrosa_storage::TableId::new("fts_ks", "articles");

        // Build FTI bytes for 3 docs.
        let doc_texts = [
            ("row1", "rust programming systems language"),
            ("row2", "go programming concurrency"),
            ("row3", "python scripting data"),
        ];

        let mut fti_builder = FullTextIndexBuilder::new();
        for (pk, text) in &doc_texts {
            // The partition key bytes used in the FTI must match what the engine
            // uses for the DecoratedKey.  Here we use the raw UTF-8 bytes of the
            // partition key string (Cassandra UTF8Type serialization for a single
            // text partition key).
            fti_builder.add_document(pk.as_bytes().to_vec(), text);
        }
        let fti_bytes = fti_builder.finish().unwrap();

        // Flush a single row to create the SSTable directory and discover the gen.
        let key1 = DecoratedKey::new(PartitionKey::new(b"row1".to_vec()));
        let row1 = ferrosa_sstable::Row {
            clustering: vec![],
            cells: vec![(
                0,
                ferrosa_common::CellValue::live(b"rust programming systems language".to_vec(), 1),
            )],
            deletion: ferrosa_sstable::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::LivenessInfo::with_timestamp(1),
        };
        engine.write(&table_id, &key1, row1, 1).unwrap();
        engine.flush(&table_id).unwrap();

        // Discover the generation number from the SSTable directory.
        let sstable_dir = dir.path().join("sstables").join(table_id.to_string());
        let gen: u64 = std::fs::read_dir(&sstable_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse().ok()
                } else {
                    None
                }
            })
            .next()
            .expect("expected at least one SSTable after flush");

        // Write the FTI sidecar to the table's SSTable directory.
        let fti_name = format!("{gen}-FTI-body.db");
        let fti_path = sstable_dir.join(&fti_name);
        std::fs::write(&fti_path, &fti_bytes).unwrap();

        // Search for "programming" — rows 1 and 2 contain it.
        let results = engine
            .fulltext_search(&table_id, "body", "programming")
            .unwrap();

        assert_eq!(
            results.len(),
            2,
            "fts_match for 'programming' must return 2 matching rows (row1, row2); got: {:?}",
            results
        );

        // Verify both pk1 and pk2 are present.
        let has_row1 = results.iter().any(|pk| pk == b"row1");
        let has_row2 = results.iter().any(|pk| pk == b"row2");
        assert!(
            has_row1,
            "row1 must be in fts_match results for 'programming'"
        );
        assert!(
            has_row2,
            "row2 must be in fts_match results for 'programming'"
        );

        // "scripting" only appears in row3 — verify single match.
        let results3 = engine
            .fulltext_search(&table_id, "body", "scripting")
            .unwrap();
        assert_eq!(
            results3.len(),
            1,
            "fts_match for 'scripting' must return exactly 1 row"
        );
        assert_eq!(results3[0], b"row3".to_vec());
    }

    // ── fts_match post-filter bug: FunctionCall in WHERE rejects all rows ──

    /// Regression test: evaluate_where_predicates must skip fts_match()
    /// WHERE clauses. term_to_cql_value cannot convert Term::FunctionCall
    /// → Err → return false → every row rejected → 0 results.
    #[tokio::test]
    async fn evaluate_where_predicates_skips_fts_match_clauses() {
        use crate::ast::{ComparisonOp, Term, WhereClause};
        use ferrosa_common::cql_type::CqlValue;

        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create table via router (sets up schema properly)
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE fts_ks WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE fts_ks.articles (id text PRIMARY KEY, body text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let snap = state.schema.snapshot();
        let table_meta = snap
            .tables
            .get(&("fts_ks".to_string(), "articles".to_string()))
            .unwrap();

        // A row: id='row1', body='rust programming language'
        let row: Vec<Option<CqlValue>> = vec![
            Some(CqlValue::Text("row1".to_string())),
            Some(CqlValue::Text("rust programming language".to_string())),
        ];
        let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
        let all_col_types: Vec<CqlType> = all_col_names
            .iter()
            .map(|name| {
                resolve_col_type(
                    &table_meta.columns[name].column_type,
                    "fts_ks",
                    &state.schema,
                )
                .unwrap()
            })
            .collect();

        // WHERE clause with fts_match: body = fts_match('programming')
        let fts_clause = WhereClause {
            column: "body".to_string(),
            op: ComparisonOp::Eq,
            value: Term::FunctionCall {
                keyspace: None,
                name: "fts_match".to_string(),
                args: vec![Term::StringLiteral("programming".to_string())],
            },
            token_fn: false,
        };

        // BUG: with only the fts_match clause, evaluate_where_predicates
        // should return true (skip the fts_match clause), but currently
        // returns false because term_to_cql_value can't convert FunctionCall.
        let result = evaluate_where_predicates(
            &row,
            std::slice::from_ref(&fts_clause),
            &all_col_names,
            &all_col_types,
            table_meta,
            "fts_ks",
            &state,
        )
        .unwrap();
        assert!(
            result,
            "evaluate_where_predicates must skip fts_match() clauses; \
             got false — FunctionCall term caused silent rejection"
        );

        // Also test: fts_match clause + a normal PK clause that matches
        let pk_clause = WhereClause {
            column: "id".to_string(),
            op: ComparisonOp::Eq,
            value: Term::StringLiteral("row1".to_string()),
            token_fn: false,
        };
        let result2 = evaluate_where_predicates(
            &row,
            &[fts_clause, pk_clause],
            &all_col_names,
            &all_col_types,
            table_meta,
            "fts_ks",
            &state,
        )
        .unwrap();
        assert!(
            result2,
            "evaluate_where_predicates with fts_match + matching PK clause \
             should return true"
        );
    }

    // ── ferrosa_bugs: COUNT(*) column name ──────────────────────────────

    /// COUNT(*) result column must be named "count" so drivers can access
    /// it by name (r_by_name("count")). Cassandra returns "count" for
    /// unaliased COUNT(*) queries.
    #[tokio::test]
    async fn count_star_column_named_count() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cnt WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE cnt.t (id int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert 3 rows
        for i in 1..=3 {
            let stmt =
                crate::parser::parse(&format!("INSERT INTO cnt.t (id, v) VALUES ({i}, 'val{i}')"))
                    .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        let stmt = crate::parser::parse("SELECT COUNT(*) FROM cnt.t").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let (names, count) = match &result {
            RouteResult::Result(b) => {
                let names = extract_column_names(b);
                let cnt = extract_row_count(b);
                (names, cnt)
            }
            _ => panic!("expected Result"),
        };

        assert_eq!(
            names,
            vec!["count"],
            "COUNT(*) column must be named 'count', got {names:?}"
        );
        assert_eq!(count, 1, "COUNT(*) should return exactly 1 row");
    }

    #[tokio::test]
    async fn count_star_allow_filtering_returns_filtered_count_without_result_rows() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cnt_filter WITH REPLICATION =              {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();
        let stmt =
            crate::parser::parse("CREATE TABLE cnt_filter.t (id int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        for (id, v) in [(1, "keep"), (2, "drop"), (3, "keep")] {
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO cnt_filter.t (id, v) VALUES ({id}, '{v}')"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        let stmt = crate::parser::parse(
            "SELECT COUNT(*) FROM cnt_filter.t WHERE v = 'keep' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let value = match &result {
            RouteResult::Result(b) => extract_first_bigint_value(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(value, 2, "COUNT(*) should aggregate matching rows only");
    }

    #[tokio::test]
    async fn count_star_key_filtered_matches_equivalent_streamed_key_scan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cnt_key_filter WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();
        let stmt = crate::parser::parse(
            "CREATE TABLE cnt_key_filter.entities (tenant_id uuid, session_id uuid, entity_id uuid, body text, PRIMARY KEY ((tenant_id, session_id), entity_id))",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let tenant_a = "00000000-0000-0000-0000-000000000001";
        let tenant_b = "00000000-0000-0000-0000-000000000002";
        for i in 0..11 {
            let tenant = if i < 7 { tenant_a } else { tenant_b };
            let session = format!("10000000-0000-0000-0000-{i:012}");
            let entity = format!("20000000-0000-0000-0000-{i:012}");
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO cnt_key_filter.entities \
                 (tenant_id, session_id, entity_id, body) \
                 VALUES ({tenant}, {session}, {entity}, 'row-{i}')"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        let count_stmt = crate::parser::parse(&format!(
            "SELECT COUNT(*) FROM cnt_key_filter.entities \
             WHERE tenant_id = {tenant_a} ALLOW FILTERING"
        ))
        .unwrap();
        let count_result = route(&state, &ctx, count_stmt).await.unwrap();
        let count_value = match &count_result {
            RouteResult::Result(b) => extract_first_bigint_value(b),
            _ => panic!("expected count result"),
        };

        let scan_stmt = crate::parser::parse(&format!(
            "SELECT tenant_id, session_id, entity_id FROM cnt_key_filter.entities \
             WHERE tenant_id = {tenant_a} ALLOW FILTERING"
        ))
        .unwrap();
        let scan_result = route(&state, &ctx, scan_stmt).await.unwrap();
        let streamed_rows = match &scan_result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected scan result"),
        };

        assert_eq!(
            count_value, streamed_rows as i64,
            "COUNT(*) over key-filtered broad scans must match the equivalent streamed key-column scan"
        );
        assert_eq!(count_value, 7);
    }

    // ── ferrosa_bugs: phonetic match ────────────────────────────────────

    /// With a phonetic index on a text column, WHERE col = 'Jon Smyth'
    /// should match rows containing 'John Smith' via Double Metaphone.
    #[tokio::test]
    async fn phonetic_index_matches_similar_sounding_names() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE phon WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE phon.people (id int PRIMARY KEY, name text)")
            .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create phonetic index
        let stmt =
            crate::parser::parse("CREATE INDEX phon_name ON phon.people (name) USING 'phonetic'")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert 'John Smith'
        let stmt =
            crate::parser::parse("INSERT INTO phon.people (id, name) VALUES (1, 'John Smith')")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query with phonetically similar 'Jon Smyth' — should match
        let stmt = crate::parser::parse(
            "SELECT * FROM phon.people WHERE id = 1 AND name = 'Jon Smyth' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            count, 1,
            "phonetic index should match 'Jon Smyth' to 'John Smith'"
        );
    }

    // ── ferrosa_bugs: secondary index returns all matching rows ─────────

    /// Secondary index queries must return ALL matching rows, not a
    /// partial subset. When the memtable index has data, those results
    /// should be correct and complete for in-memory data.
    #[tokio::test]
    async fn secondary_index_returns_all_matching_rows() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE sidx WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE sidx.items (id int PRIMARY KEY, category text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX sidx_cat ON sidx.items (category)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert 20 rows: 15 with category='a', 5 with category='b'
        for i in 1..=20 {
            let cat = if i <= 15 { "a" } else { "b" };
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO sidx.items (id, category) VALUES ({i}, '{cat}')"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // Full scan baseline
        let stmt = crate::parser::parse("SELECT * FROM sidx.items").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let total = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(total, 20, "should have 20 total rows");

        // Secondary index query for category='a' — must return 15
        let stmt = crate::parser::parse("SELECT * FROM sidx.items WHERE category = 'a'").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let idx_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            idx_count, 15,
            "secondary index query for category='a' should return 15 rows, got {idx_count}"
        );

        // Secondary index query for category='b' — must return 5
        let stmt = crate::parser::parse("SELECT * FROM sidx.items WHERE category = 'b'").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let b_count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            b_count, 5,
            "secondary index query for category='b' should return 5 rows, got {b_count}"
        );
    }

    // ── BUG-007: SOUNDS LIKE syntax parses ──────────────────────────────

    /// SELECT ... WHERE col SOUNDS LIKE 'value' must parse without error.
    #[test]
    fn sounds_like_syntax_parses() {
        let result = crate::parser::parse("SELECT * FROM ks.t WHERE name SOUNDS LIKE 'Smith'");
        assert!(
            result.is_ok(),
            "SOUNDS LIKE syntax should parse, got: {:?}",
            result.err()
        );
    }

    /// SOUNDS LIKE must execute correctly: find phonetically similar matches.
    #[tokio::test]
    async fn sounds_like_finds_phonetic_matches() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE sl WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE sl.t (id int PRIMARY KEY, name text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("INSERT INTO sl.t (id, name) VALUES (1, 'John Smith')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("INSERT INTO sl.t (id, name) VALUES (2, 'Jane Doe')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SOUNDS LIKE should find 'John Smith' when querying 'Jon Smyth'
        let stmt = crate::parser::parse(
            "SELECT * FROM sl.t WHERE name SOUNDS LIKE 'Jon Smyth' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            count, 1,
            "SOUNDS LIKE 'Jon Smyth' should match 'John Smith' (1 row), got {count}"
        );
    }

    // ── BUG-008: Logged batch atomicity ─────────────────────────────────

    /// A logged batch where one statement targets a non-existent table
    /// should not leave earlier statements committed.
    #[tokio::test]
    async fn logged_batch_rolls_back_on_partial_failure() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE bat WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE bat.t (id int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Logged batch: first stmt writes to existing table, second to
        // non-existent table — the batch should fail atomically.
        let stmt = crate::parser::parse(
            "BEGIN BATCH \
               INSERT INTO bat.t (id, v) VALUES (1, 'should-rollback'); \
               INSERT INTO bat.nonexistent (id) VALUES (1); \
             APPLY BATCH",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        // Batch should fail (second statement targets non-existent table)
        assert!(result.is_err(), "batch with bad table should fail");

        // The first statement's write must NOT be visible
        let stmt = crate::parser::parse("SELECT * FROM bat.t WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(
            count, 0,
            "logged batch rollback: first statement's write should not be visible \
             after batch failure, but found {count} rows"
        );
    }

    // ── GRANT/REVOKE on function resources ──────────────────────────

    /// GRANT EXECUTE ON FUNCTION must succeed (not return "not yet implemented").
    #[tokio::test]
    async fn grant_execute_on_function() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup keyspace + function
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE grf WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create a role to grant to
        let stmt =
            crate::parser::parse("CREATE ROLE fn_user WITH PASSWORD = 'pass' AND LOGIN = true")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE
        let stmt =
            crate::parser::parse("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE grf TO fn_user")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "GRANT EXECUTE ON ALL FUNCTIONS should succeed, got: {:?}",
            result.err()
        );

        // REVOKE should also work
        let stmt =
            crate::parser::parse("REVOKE EXECUTE ON ALL FUNCTIONS IN KEYSPACE grf FROM fn_user")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "REVOKE EXECUTE ON ALL FUNCTIONS should succeed, got: {:?}",
            result.err()
        );
    }

    // ── DROP TABLE routing tests ──────────────────────────────────────

    #[tokio::test]
    async fn drop_table_removes_from_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace + table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dtks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE dtks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify table exists
        let snap = state.schema.snapshot();
        assert!(snap.tables.contains_key(&("dtks".into(), "t".into())));

        // DROP TABLE
        let stmt = crate::parser::parse("DROP TABLE dtks.t").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP TABLE should succeed: {:?}",
            result.err()
        );
        match &result.unwrap() {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0005i32.to_be_bytes()),
            _ => panic!("expected Result (schema change)"),
        }

        // Table must be gone
        let snap = state.schema.snapshot();
        assert!(!snap.tables.contains_key(&("dtks".into(), "t".into())));
    }

    #[tokio::test]
    async fn drop_table_if_exists_nonexistent_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create keyspace only (no table)
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dtks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP TABLE IF EXISTS on nonexistent table should succeed silently
        let stmt = crate::parser::parse("DROP TABLE IF EXISTS dtks2.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP TABLE IF EXISTS should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn drop_table_nonexistent_without_if_exists_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dtks3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("DROP TABLE dtks3.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_err(), "DROP TABLE without IF EXISTS should fail");
    }

    // ── CREATE ROLE / ALTER ROLE / DROP ROLE routing tests ────────────

    #[tokio::test]
    async fn create_role_stores_in_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE ROLE test_role WITH PASSWORD = 'secret' AND LOGIN = true AND SUPERUSER = false", // pragma: allowlist secret
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE ROLE should succeed: {:?}",
            result.err()
        );
        match &result.unwrap() {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected Result (void)"),
        }

        // Verify role exists in schema
        let snap = state.schema.snapshot();
        assert!(
            snap.roles.contains_key("test_role"),
            "role should be in schema"
        );
    }

    /// Regression for the auth-defects bug: CREATE ROLE WITH PASSWORD
    /// returned success but never persisted the salted_hash, so login
    /// returned `Bad credentials` for an apparently-existing role. The
    /// fix hashes on the coordinator before sending through the DDL
    /// path, so this test pins that the role's `salted_hash` is set
    /// AND that re-hashing the cleartext does NOT match (verifying the
    /// stored value is actually a salted bcrypt hash, not the literal).
    #[tokio::test]
    async fn create_role_persists_password_hash() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE ROLE pwd_test WITH PASSWORD = 'mypassword123' AND LOGIN = true", // pragma: allowlist secret
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let snap = state.schema.snapshot();
        let role = snap
            .roles
            .get("pwd_test")
            .expect("role must exist after CREATE ROLE");
        let hash = role
            .salted_hash
            .as_deref()
            .expect("salted_hash must be persisted, not None — was the regression");
        assert!(!hash.is_empty(), "salted_hash must not be an empty string");
        assert_ne!(
            hash, "mypassword123",
            "salted_hash must be the hash, not the cleartext"
        );
        // bcrypt-format hashes start with `$2`. We don't lock the
        // exact format here so the hasher can be swapped in future.
        assert!(
            hash.starts_with("$2") || hash.len() >= 32,
            "salted_hash must look like a real hash, got: {hash}"
        );

        // Verify the hash actually validates the original cleartext —
        // the canonical "auth path works" check.
        assert!(
            ferrosa_schema::auth::password::PasswordHasher::verify_password_any(
                "mypassword123",
                hash
            )
            .unwrap(),
            "stored hash must verify against the original cleartext"
        );
    }

    /// Regression for the auth-defects bug: `CREATE USER` (deprecated
    /// alias for `CREATE ROLE WITH LOGIN = true`) was rejected by the
    /// parser, then once it parsed, suffered the same password-hash
    /// drop as CREATE ROLE.
    #[tokio::test]
    async fn create_user_persists_password_hash_and_login() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse("CREATE USER alice WITH PASSWORD = 'a-password-123'") // pragma: allowlist secret
            .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let snap = state.schema.snapshot();
        let role = snap.roles.get("alice").expect("USER must persist as role");
        assert!(role.can_login, "CREATE USER must default LOGIN = true");
        let hash = role
            .salted_hash
            .as_deref()
            .expect("CREATE USER must persist salted_hash");
        assert!(
            ferrosa_schema::auth::password::PasswordHasher::verify_password_any(
                "a-password-123",
                hash
            )
            .unwrap()
        );
    }

    /// Regression: `SELECT * FROM system_auth.roles WHERE role = 'X'`
    /// returned every row instead of just the matching one — the
    /// handler's WHERE clause was ignored. Test pins the eq filter.
    #[tokio::test]
    async fn select_system_auth_roles_filters_by_partition_key() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        for name in ["alpha", "beta", "gamma"] {
            let stmt = crate::parser::parse(&format!(
                "CREATE ROLE {name} WITH PASSWORD = 'p' AND LOGIN = true" // pragma: allowlist secret
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        let stmt =
            crate::parser::parse("SELECT * FROM system_auth.roles WHERE role = 'beta'").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let body = match result {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result"),
        };
        // The response must contain "beta" exactly once.
        let beta_hits = body
            .windows(b"beta".len())
            .filter(|w| *w == b"beta")
            .count();
        assert_eq!(
            beta_hits, 1,
            "WHERE role = 'beta' must filter to exactly the beta row"
        );
        // And must NOT contain alpha or gamma.
        assert!(
            !body.windows(b"alpha".len()).any(|w| w == b"alpha"),
            "alpha must be filtered out"
        );
        assert!(
            !body.windows(b"gamma".len()).any(|w| w == b"gamma"),
            "gamma must be filtered out"
        );
    }

    /// Regression: `SELECT *` against system_auth.roles silently
    /// dropped the salted_hash column. With #1 (CREATE ROLE
    /// password-not-stored) that omission masked the silent failure.
    /// The fix exposes the column with `[REDACTED]` for non-superusers
    /// and the real hash for superusers — the column existence is now
    /// observable.
    #[tokio::test]
    async fn select_system_auth_roles_redacts_salted_hash_for_nonsuperuser() {
        let (state, _dir) = setup();
        let admin_ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create a role with a password under the superuser context.
        let stmt = crate::parser::parse(
            "CREATE ROLE redact_test WITH PASSWORD = 'mypwd123' AND LOGIN = true", // pragma: allowlist secret
        )
        .unwrap();
        route(&state, &admin_ctx, stmt).await.unwrap();

        // Superuser SELECT *: response must contain the actual hash
        // bytes (or at least the bcrypt prefix `$2`) somewhere.
        let stmt =
            crate::parser::parse("SELECT * FROM system_auth.roles WHERE role = 'redact_test'")
                .unwrap();
        let body = match route(&state, &admin_ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result"),
        };
        assert!(
            body.windows(2).any(|w| w == b"$2"),
            "superuser SELECT must include the real bcrypt hash"
        );
        assert!(
            !body
                .windows(b"[REDACTED]".len())
                .any(|w| w == b"[REDACTED]"),
            "superuser SELECT must NOT redact"
        );

        // Non-superuser SELECT *: response must contain `[REDACTED]`
        // (column visible, hash hidden). The column name `salted_hash`
        // is also included in the metadata so the column exists in
        // the response schema.
        let nonsuper = ferrosa_schema::AuthContext {
            role: "nobody".into(),
            is_superuser: false,
            must_change_password: false,
        };
        let user_ctx = RequestContext {
            auth: &nonsuper,
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse("SELECT * FROM system_auth.roles WHERE role = 'redact_test'")
                .unwrap();
        let body = match route(&state, &user_ctx, stmt).await.unwrap() {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result"),
        };
        assert!(
            body.windows(b"[REDACTED]".len())
                .any(|w| w == b"[REDACTED]"),
            "non-superuser SELECT must include the [REDACTED] marker"
        );
        assert!(
            body.windows(b"salted_hash".len())
                .any(|w| w == b"salted_hash"),
            "salted_hash column metadata must be present"
        );
        assert!(
            !body.windows(2).any(|w| w == b"$2"),
            "non-superuser SELECT must NOT leak the bcrypt hash"
        );
    }

    /// Regression: `LIST ROLES` returned `SyntaxException: unexpected
    /// token Keyword(List)`. This test pins the new parser + router
    /// path returns a row per known role with the expected columns.
    #[tokio::test]
    async fn list_roles_returns_known_roles() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let create = crate::parser::parse(
            "CREATE ROLE list_test WITH PASSWORD = 'p' AND LOGIN = true AND SUPERUSER = false", // pragma: allowlist secret
        )
        .unwrap();
        route(&state, &ctx, create).await.unwrap();

        let stmt = crate::parser::parse("LIST ROLES").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "LIST ROLES should succeed (was SyntaxException): {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                // Result kind 0x0002 = Rows.
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
                // Body must contain the role name we just created.
                assert!(
                    b.windows(b"list_test".len()).any(|w| w == b"list_test"),
                    "LIST ROLES output must include the new role"
                );
            }
            _ => panic!("expected Result (rows) from LIST ROLES"),
        }
    }

    #[tokio::test]
    async fn create_role_with_access_clause_is_rejected() {
        // Threat T9: ferrosa has no network authorizer, so an ACCESS restriction
        // must be rejected (fail loud), never silently accepted-and-ignored.
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE ROLE netrole WITH PASSWORD = 'p' AND ACCESS TO DATACENTERS {'DC1'}", // pragma: allowlist secret
        )
        .unwrap();
        assert!(
            route(&state, &ctx, stmt).await.is_err(),
            "CREATE ROLE with ACCESS must be rejected, not silently accepted"
        );
        assert!(!state.schema.snapshot().roles.contains_key("netrole"));
    }

    #[tokio::test]
    async fn create_role_with_hashed_password_stores_hash_verbatim() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        // A valid bcrypt hash string (HASHED PASSWORD input).
        let hash = "$2a$10$JSJEMFm6GeaW9XxT5JIheuEtPvat6i7uKbnTcxX3c1wshIIsGyUtG";
        let cql = format!("CREATE ROLE hrole WITH HASHED PASSWORD = '{hash}' AND LOGIN = true");
        route(&state, &ctx, crate::parser::parse(&cql).unwrap())
            .await
            .unwrap();
        assert_eq!(
            state
                .schema
                .snapshot()
                .roles
                .get("hrole")
                .unwrap()
                .salted_hash
                .as_deref(),
            Some(hash),
            "HASHED PASSWORD must be stored verbatim"
        );
    }

    #[tokio::test]
    async fn alter_role_updates_login() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create role first
        let stmt =
            crate::parser::parse("CREATE ROLE ar_role WITH PASSWORD = 'pass' AND LOGIN = false")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER ROLE via direct AST (parser doesn't support ALTER ROLE yet)
        let stmt = Statement::AlterRole(AlterRoleStatement {
            name: "ar_role".into(),
            password: None,
            hashed_password: None,
            superuser: None,
            login: Some(true),
            options: Vec::new(),
            access: Vec::new(),
        });
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALTER ROLE should succeed: {:?}",
            result.err()
        );

        let snap = state.schema.snapshot();
        let role = snap.roles.get("ar_role").unwrap();
        assert!(role.can_login, "role should have login enabled after alter");
    }

    #[tokio::test]
    async fn drop_role_removes_from_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Create + drop role
        let stmt =
            crate::parser::parse("CREATE ROLE dr_role WITH PASSWORD = 'pass' AND LOGIN = true")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let snap = state.schema.snapshot();
        assert!(snap.roles.contains_key("dr_role"));

        let stmt = crate::parser::parse("DROP ROLE dr_role").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP ROLE should succeed: {:?}",
            result.err()
        );

        let snap = state.schema.snapshot();
        assert!(!snap.roles.contains_key("dr_role"), "role should be gone");
    }

    // ── DROP INDEX routing tests ──────────────────────────────────────

    #[tokio::test]
    async fn drop_index_removes_from_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup keyspace + table + index
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE diks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE diks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX idx_v ON diks.t (v)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify index exists
        let snap = state.schema.snapshot();
        assert!(
            snap.indexes
                .keys()
                .any(|(ks, _t, n)| ks == "diks" && n == "idx_v"),
            "index should exist in schema"
        );

        // DROP INDEX
        let stmt = crate::parser::parse("DROP INDEX diks.idx_v").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP INDEX should succeed: {:?}",
            result.err()
        );

        // Index must be gone
        let snap = state.schema.snapshot();
        assert!(
            !snap
                .indexes
                .keys()
                .any(|(ks, _t, n)| ks == "diks" && n == "idx_v"),
            "index should be removed from schema"
        );
    }

    #[tokio::test]
    async fn drop_index_if_exists_nonexistent_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE diks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("DROP INDEX IF EXISTS diks2.no_such_idx").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP INDEX IF EXISTS should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn drop_index_nonexistent_without_if_exists_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE diks3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("DROP INDEX diks3.nonexistent_idx").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP INDEX without IF EXISTS on missing index should fail"
        );
    }

    // ── GRANT / REVOKE on table resources ────────────────────────────

    #[tokio::test]
    async fn grant_select_on_table() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup keyspace + table + role
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE grks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE grks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE ROLE gr_user WITH PASSWORD = 'pass' AND LOGIN = true")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // GRANT SELECT ON TABLE
        let stmt = crate::parser::parse("GRANT SELECT ON grks.t TO gr_user").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "GRANT SELECT ON TABLE should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn revoke_select_on_table() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        // Setup keyspace + table + role + grant
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE rvks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE rvks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE ROLE rv_user WITH PASSWORD = 'pass' AND LOGIN = true")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("GRANT SELECT ON rvks.t TO rv_user").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // REVOKE SELECT ON TABLE
        let stmt = crate::parser::parse("REVOKE SELECT ON rvks.t FROM rv_user").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "REVOKE SELECT ON TABLE should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn grant_all_on_keyspace() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE gaks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE ROLE ga_user WITH PASSWORD = 'pass' AND LOGIN = true")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("GRANT ALL ON KEYSPACE gaks TO ga_user").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "GRANT ALL ON KEYSPACE should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn grant_and_revoke_role_membership() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for r in ["reporter", "alice"] {
            let stmt = crate::parser::parse(&format!(
                "CREATE ROLE {r} WITH PASSWORD = 'pw' AND LOGIN = true"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // GRANT reporter TO alice — alice becomes a member of reporter.
        let stmt = crate::parser::parse("GRANT reporter TO alice").unwrap();
        route(&state, &ctx, stmt).await.expect("grant role");
        assert!(state
            .schema
            .snapshot()
            .roles
            .get("alice")
            .unwrap()
            .member_of
            .contains("reporter"));

        // Granting a role to a nonexistent member fails loud.
        let stmt = crate::parser::parse("GRANT reporter TO ghost").unwrap();
        match route(&state, &ctx, stmt).await {
            Ok(_) => panic!("granting to a nonexistent role must fail"),
            Err(e) => assert!(format!("{e}").contains("ghost")),
        }

        // REVOKE removes the single edge.
        let stmt = crate::parser::parse("REVOKE reporter FROM alice").unwrap();
        route(&state, &ctx, stmt).await.expect("revoke role");
        assert!(!state
            .schema
            .snapshot()
            .roles
            .get("alice")
            .unwrap()
            .member_of
            .contains("reporter"));
    }

    #[tokio::test]
    async fn list_permissions_reflects_grants() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for q in [
            "CREATE KEYSPACE lpks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE lpks.t (k int PRIMARY KEY)",
            "CREATE ROLE reader WITH PASSWORD = 'pw' AND LOGIN = true",
            "GRANT SELECT ON lpks.t TO reader",
        ] {
            let stmt = crate::parser::parse(q).unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // LIST ALL PERMISSIONS returns a Rows result (kind 0x0002).
        let stmt = crate::parser::parse("LIST ALL PERMISSIONS OF reader").unwrap();
        let result = route(&state, &ctx, stmt).await.expect("list permissions");
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes(), "expected Rows result");
            }
            _ => panic!("expected a Rows Result from LIST PERMISSIONS"),
        }
    }

    // ── ALTER TABLE with extensions ──────────────────────────────────

    #[tokio::test]
    async fn alter_table_with_extensions() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ateks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE ateks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER TABLE with extensions
        let stmt = Statement::AlterTable(AlterTableStatement {
            keyspace: Some("ateks".into()),
            table: "t".into(),
            add_columns: vec![],
            drop_columns: vec![],
            rename_columns: vec![],
            alter_column_types: vec![],
            extensions: Some(vec![("vertex_label".into(), "Person".into())]),
            table_options: vec![],
        });
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALTER TABLE with extensions should succeed: {:?}",
            result.err()
        );
    }

    /// ALTER TABLE RENAME / ALTER TYPE parse but are not yet executable; they
    /// must be rejected loudly rather than silently succeeding as a no-op.
    #[tokio::test]
    async fn alter_table_rename_and_alter_type_rejected() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE atrk WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();
        let stmt = crate::parser::parse("CREATE TABLE atrk.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let rename = crate::parser::parse("ALTER TABLE atrk.t RENAME k TO k2").unwrap();
        match route(&state, &ctx, rename).await {
            Ok(_) => panic!("RENAME must be rejected, not silently succeed"),
            Err(e) => assert!(format!("{e}").contains("RENAME")),
        }

        let alter = crate::parser::parse("ALTER TABLE atrk.t ALTER v TYPE blob").unwrap();
        match route(&state, &ctx, alter).await {
            Ok(_) => panic!("ALTER TYPE must be rejected, not silently succeed"),
            Err(e) => assert!(format!("{e}").contains("TYPE")),
        }
    }

    // ── CREATE INDEX with auto-generated name ────────────────────────

    #[tokio::test]
    async fn create_index_auto_name() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cian WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE cian.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE INDEX without explicit name — should auto-generate name
        let stmt = crate::parser::parse("CREATE INDEX ON cian.t (v)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE INDEX (auto-name) should succeed: {:?}",
            result.err()
        );

        // Verify the auto-generated index name follows the pattern table_column_idx
        let snap = state.schema.snapshot();
        assert!(
            snap.indexes
                .keys()
                .any(|(ks, _t, n)| ks == "cian" && n == "t_v_idx"),
            "auto-generated index name should be 't_v_idx'"
        );
    }

    // ── resolve_index_type coverage for remaining variants ───────────

    #[test]
    fn resolve_index_type_filtered() {
        let result = resolve_index_type(Some("filtered"), &["col".into()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Filtered);
    }

    #[test]
    fn ann_of_ordering_sorts_candidate_rows_by_vector_distance() {
        let all_col_names = vec!["tenant".to_string(), "id".to_string(), "vec".to_string()];
        let all_col_types = vec![
            CqlType::Varchar,
            CqlType::Int,
            CqlType::Vector(Box::new(CqlType::Float), 3),
        ];
        let query = Term::BlobLiteral(
            [1.0_f32, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_be_bytes)
                .collect(),
        );
        let mut rows = vec![
            vec![
                Some(CqlValue::Text("a".to_string())),
                Some(CqlValue::Int(1)),
                Some(CqlValue::Vector(vec![
                    0.0f32.to_bits(),
                    1.0f32.to_bits(),
                    0.0f32.to_bits(),
                ])),
            ],
            vec![
                Some(CqlValue::Text("a".to_string())),
                Some(CqlValue::Int(2)),
                Some(CqlValue::Vector(vec![
                    1.0f32.to_bits(),
                    0.0f32.to_bits(),
                    0.0f32.to_bits(),
                ])),
            ],
        ];

        apply_ann_of_ordering(&mut rows, "vec", &query, &all_col_names, &all_col_types).unwrap();

        assert_eq!(rows[0][1], Some(CqlValue::Int(2)));
        assert_eq!(rows[1][1], Some(CqlValue::Int(1)));
    }

    #[test]
    fn ann_of_ordering_skips_null_vector_rows() {
        // A single un-embedded (NULL vector) row must NOT poison ANN for the
        // whole result set — the embedded rows are still ranked and returned.
        let all_col_names = vec!["tenant".to_string(), "id".to_string(), "vec".to_string()];
        let all_col_types = vec![
            CqlType::Varchar,
            CqlType::Int,
            CqlType::Vector(Box::new(CqlType::Float), 3),
        ];
        let query = Term::BlobLiteral(
            [1.0_f32, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_be_bytes)
                .collect(),
        );
        let mut rows = vec![
            // embedded, far from the query
            vec![
                Some(CqlValue::Text("a".to_string())),
                Some(CqlValue::Int(1)),
                Some(CqlValue::Vector(vec![
                    0.0f32.to_bits(),
                    1.0f32.to_bits(),
                    0.0f32.to_bits(),
                ])),
            ],
            // un-embedded: NULL vector — must be skipped, not error
            vec![
                Some(CqlValue::Text("a".to_string())),
                Some(CqlValue::Int(99)),
                None,
            ],
            // embedded, nearest to the query
            vec![
                Some(CqlValue::Text("a".to_string())),
                Some(CqlValue::Int(2)),
                Some(CqlValue::Vector(vec![
                    1.0f32.to_bits(),
                    0.0f32.to_bits(),
                    0.0f32.to_bits(),
                ])),
            ],
        ];

        apply_ann_of_ordering(&mut rows, "vec", &query, &all_col_names, &all_col_types)
            .expect("a NULL-vector row must be skipped, not fail the whole query");

        assert_eq!(
            rows.len(),
            2,
            "the un-embedded NULL-vector row must be excluded from ANN results"
        );
        assert_eq!(
            rows[0][1],
            Some(CqlValue::Int(2)),
            "nearest embedded row first"
        );
        assert_eq!(rows[1][1], Some(CqlValue::Int(1)));
        assert!(
            !rows.iter().any(|r| r[1] == Some(CqlValue::Int(99))),
            "the un-embedded row must not appear in the ranked results"
        );
    }

    #[test]
    fn resolve_index_type_vector() {
        let result = resolve_index_type(Some("vector"), &["embedding".into()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Vector);
    }

    #[tokio::test]
    async fn ann_prefix_create_vector_index_wires_scoped_storage_index() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        for cql in [
            "CREATE KEYSPACE annks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE annks.entities (tenant text, session text, id int, embedding vector<float, 3>, PRIMARY KEY ((tenant, session), id))",
            "CREATE INDEX entity_embedding_ann ON annks.entities (embedding) USING 'vector'",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap();
        }

        let vector_blob = |values: &[f32]| -> String {
            let bytes: Vec<u8> = values
                .iter()
                .flat_map(|value| value.to_be_bytes())
                .collect();
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        };
        let far = vector_blob(&[0.0, 1.0, 0.0]);
        let exact = vector_blob(&[1.0, 0.0, 0.0]);
        for cql in [
            format!("INSERT INTO annks.entities (tenant, session, id, embedding) VALUES ('tenant-a', 'session-1', 1, 0x{far})"),
            format!("INSERT INTO annks.entities (tenant, session, id, embedding) VALUES ('tenant-b', 'session-1', 2, 0x{exact})"),
        ] {
            route(&state, &ctx, crate::parser::parse(&cql).unwrap())
                .await
                .unwrap();
        }

        let table_id = TableId::new("annks", "entities");
        let pk_types = [CqlType::Varchar, CqlType::Varchar];
        let scope = bridge::build_decorated_key(
            &[
                CqlValue::Text("tenant-a".to_string()),
                CqlValue::Text("session-1".to_string()),
            ],
            &pk_types,
        )
        .unwrap();
        let scoped = state
            .engine
            .ann_search_in_partition_scope(
                &table_id,
                "entity_embedding_ann",
                scope.key.as_bytes(),
                &[1.0, 0.0, 0.0],
                1,
                20,
            )
            .unwrap();

        assert_eq!(scoped.len(), 1);
        assert!(
            scoped[0].score > 0.1,
            "CQL-created vector index must feed scoped storage ANN and exclude cross-prefix exact matches: {scoped:?}"
        );
    }

    // ── CREATE INDEX … USING 'vector' WITH OPTIONS={'method': …} ──────

    /// Set up a keyspace + table with a `vector<float, 3>` column, then run the
    /// supplied `CREATE INDEX` statement. Returns the shared state so the caller
    /// can inspect the storage engine. The temp dir is leaked into the tuple to
    /// keep it alive for the duration of the test.
    async fn create_vector_index(
        create_index_cql: &str,
    ) -> (SharedState, TempDir, Result<RouteResult, CqlError>) {
        let (state, dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        for cql in [
            "CREATE KEYSPACE vidx WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE vidx.docs (id int PRIMARY KEY, embedding vector<float, 3>)",
        ] {
            route(&state, &ctx, crate::parser::parse(cql).unwrap())
                .await
                .unwrap();
        }
        let result = route(
            &state,
            &ctx,
            crate::parser::parse(create_index_cql).unwrap(),
        )
        .await;
        (state, dir, result)
    }

    #[tokio::test]
    async fn create_vector_index_default_method_is_hnsw() {
        let (state, _dir, result) =
            create_vector_index("CREATE INDEX idx ON vidx.docs (embedding) USING 'vector'").await;
        result.expect("default vector index create must succeed");
        let method = state
            .engine
            .vector_index_method(&TableId::new("vidx", "docs"), "idx")
            .unwrap();
        assert_eq!(method, ferrosa_storage::VectorIndexMethod::Hnsw);
    }

    #[tokio::test]
    async fn create_vector_index_hvq_method_selects_quantized_storage() {
        let (state, _dir, result) = create_vector_index(
            "CREATE INDEX idx ON vidx.docs (embedding) USING 'vector' WITH OPTIONS = {'method': 'hvq'}",
        )
        .await;
        result.expect("hvq vector index create must succeed");
        let method = state
            .engine
            .vector_index_method(&TableId::new("vidx", "docs"), "idx")
            .unwrap();
        assert_eq!(
            method,
            ferrosa_storage::VectorIndexMethod::QuantizedIvf,
            "WITH OPTIONS={{'method':'hvq'}} must register the quantized IVF storage method"
        );
    }

    #[tokio::test]
    async fn create_vector_index_unknown_method_is_rejected() {
        let (_state, _dir, result) = create_vector_index(
            "CREATE INDEX idx ON vidx.docs (embedding) USING 'vector' WITH OPTIONS = {'method': 'bogus'}",
        )
        .await;
        assert!(
            matches!(result.err(), Some(CqlError::Invalid(_))),
            "an unknown vector index method must fail loudly"
        );
    }

    /// Drive the full `examples/vector-indexes` flow through `route()`: create
    /// an HVQ-method vector index, insert vectors as CQL list literals, and run
    /// an `ANN OF` query — the exact path the CI examples job exercises via
    /// cqlsh. Every statement must execute without error.
    #[tokio::test]
    async fn example_hvq_vector_index_flow_executes_end_to_end() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let run = |cql: String| {
            let state = &state;
            let ctx = &ctx;
            async move {
                route(state, ctx, crate::parser::parse(&cql).unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("statement failed: {cql}: {e:?}"))
            }
        };

        run("CREATE KEYSPACE semantic WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}".into()).await;
        run("CREATE TABLE semantic.articles (id int PRIMARY KEY, category text, embedding vector<float, 4>)".into()).await;
        run("CREATE INDEX articles_hvq_ann ON semantic.articles (embedding) USING 'vector' WITH OPTIONS = {'method': 'hvq'}".into()).await;

        // Storage registered the quantized method for this CQL-created index.
        assert_eq!(
            state
                .engine
                .vector_index_method(&TableId::new("semantic", "articles"), "articles_hvq_ann")
                .unwrap(),
            ferrosa_storage::VectorIndexMethod::QuantizedIvf,
        );

        for (id, cat, vec) in [
            (1, "science", "[0.90, 0.10, 0.00, 0.00]"),
            (2, "science", "[0.80, 0.20, 0.10, 0.00]"),
            (3, "history", "[0.00, 0.00, 0.90, 0.10]"),
            (4, "history", "[0.10, 0.00, 0.80, 0.20]"),
        ] {
            run(format!(
                "INSERT INTO semantic.articles (id, category, embedding) VALUES ({id}, '{cat}', {vec})"
            ))
            .await;
        }

        // ANN query against the HVQ index must execute and return a result set.
        run("SELECT id FROM semantic.articles ORDER BY embedding ANN OF [0.90, 0.10, 0.00, 0.00] LIMIT 2".into()).await;
    }

    /// Reproduction for the scholarly-search example failure: a 768-d
    /// `vector<float, 768>` column on a graph-vertex table, with enough rows
    /// (and a small flush threshold in `setup()`) that the memtable flushes to
    /// a local SSTable mid-insert. A subsequent full-scan `ANN OF` query must
    /// still see every row's embedding as a vector — i.e. the large vector cell
    /// must round-trip through the SSTable read path. Before the fix this failed
    /// with "ANN OF column embedding must contain vector values" because a
    /// flushed row read back without its vector cell.
    #[tokio::test]
    async fn ann_over_768d_vectors_survives_flush_end_to_end() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let run = |cql: String| {
            let state = &state;
            let ctx = &ctx;
            async move {
                route(state, ctx, crate::parser::parse(&cql).unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("statement failed: {cql}: {e:?}"))
            }
        };

        // Build a distinct, well-formed 768-d unit-ish vector literal per row.
        let vec_literal = |seed: usize| -> String {
            let mut parts = Vec::with_capacity(768);
            for i in 0..768 {
                // Deterministic small floats in [-0.1, 0.1], distinct per seed.
                let v = (((seed * 31 + i * 7) % 200) as f32 - 100.0) / 1000.0;
                parts.push(format!("{v:.6}"));
            }
            format!("[{}]", parts.join(", "))
        };

        run("CREATE KEYSPACE scholar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}".into()).await;
        run("CREATE TABLE scholar.paper (paper_id int PRIMARY KEY, title text, abstract text, year int, venue text, embedding vector<float, 768>) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Paper' }".into()).await;
        run("CREATE INDEX paper_ann ON scholar.paper (embedding) USING 'vector' WITH OPTIONS = {'method': 'hvq'}".into()).await;

        // 12 papers, each with a 3072-byte vector cell: total well over the
        // 4096-byte flush threshold, forcing flushes during the insert run.
        for id in 1..=12usize {
            run(format!(
                "INSERT INTO scholar.paper (paper_id, title, abstract, year, venue, embedding) \
                 VALUES ({id}, 'Paper {id}', 'abstract {id}', 2024, 'Venue', {})",
                vec_literal(id)
            ))
            .await;
        }

        // Full-scan ANN ordering must read every flushed row's vector back.
        let result = run(format!(
            "SELECT paper_id, title FROM scholar.paper ORDER BY embedding ANN OF {} LIMIT 5",
            vec_literal(3)
        ))
        .await;
        let RouteResult::Result(body) = result else {
            panic!("ANN OF query must return a RESULT frame");
        };
        let row_count = extract_row_count(&body);
        assert_eq!(
            row_count, 5,
            "ANN OF full scan must rank all flushed 768-d rows, got {row_count}"
        );
    }

    /// Reproduction for the scholarly-search example failure on the shared CI
    /// node: two graph-vertex tables in *different* keyspaces declare the same
    /// `graph.label`, but only one has a vector column. An `ANN OF` query
    /// against the vector table must stay scoped to its own keyspace.table and
    /// rank its own rows -- it must not resolve columns/rows through the shared
    /// vertex label and trip over the other table's (embedding-less) schema.
    /// Before the fix this failed with "ANN OF column embedding must contain
    /// vector values" once the second same-label table existed.
    #[tokio::test]
    async fn ann_of_is_isolated_from_same_graph_label_in_other_keyspace() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let run = |cql: String| {
            let state = &state;
            let ctx = &ctx;
            async move {
                route(state, ctx, crate::parser::parse(&cql).unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("statement failed: {cql}: {e:?}"))
            }
        };

        // Keyspace `scholar`: a Paper vertex WITH a vector embedding.
        run("CREATE KEYSPACE scholar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}".into()).await;
        run("CREATE TABLE scholar.paper (paper_id int PRIMARY KEY, title text, embedding vector<float, 4>) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Paper' }".into()).await;
        run("CREATE INDEX paper_ann ON scholar.paper (embedding) USING 'vector' WITH OPTIONS = {'method': 'hvq'}".into()).await;
        for (id, vec) in [
            (1, "[0.90, 0.10, 0.00, 0.00]"),
            (2, "[0.80, 0.20, 0.10, 0.00]"),
            (3, "[0.00, 0.00, 0.90, 0.10]"),
        ] {
            run(format!(
                "INSERT INTO scholar.paper (paper_id, title, embedding) VALUES ({id}, 'P{id}', {vec})"
            ))
            .await;
        }

        // Keyspace `knowledge`: a different Paper vertex with NO vector column,
        // sharing the same graph.label. Creating it must not corrupt the
        // schema/column resolution that scholar.paper's ANN query relies on.
        run("CREATE KEYSPACE knowledge WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}".into()).await;
        run("CREATE TABLE knowledge.paper (paper_id uuid PRIMARY KEY, title text) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Paper' }".into()).await;

        // ANN query against scholar.paper must still rank its own 3 rows.
        let result = run(
            "SELECT paper_id, title FROM scholar.paper ORDER BY embedding ANN OF [0.90, 0.10, 0.00, 0.00] LIMIT 5".into(),
        )
        .await;
        let RouteResult::Result(body) = result else {
            panic!("ANN OF query must return a RESULT frame");
        };
        assert_eq!(
            extract_row_count(&body),
            3,
            "ANN OF must stay scoped to scholar.paper despite the same graph.label in another keyspace"
        );
    }

    /// Regression for the scholarly-search ANN failure root cause: the
    /// projection fast path (`SELECT a, b FROM t` with no WHERE) must request
    /// the **SSTable storage ordinal** of each column — i.e. regular columns in
    /// Cassandra **name-sorted** order, exactly as `ferrosa_schema::convert::
    /// to_storage_schema` writes them into the SerializationHeader — NOT the
    /// table-definition `position` order. For a table whose declared order
    /// differs from name order, requesting the position index made the flushed
    /// SSTable projected read decode the wrong column, so the projected column
    /// came back NULL once the memtable flushed (e.g. a 768-d embedding → the
    /// ANN query then failed "must contain vector values").
    #[test]
    fn projection_storage_ordinals_use_name_sorted_storage_order_not_table_position() {
        let mk = |name: &str, kind: ColumnKind, position: i32, ty: &str| ColumnMetadata {
            name: name.to_string(),
            kind,
            position,
            column_type: ty.to_string(),
            clustering_order: SchemaClusteringOrder::None,
            mask: None,
        };
        // Declared order: id(pk), title(1), abstract(2), embedding(3).
        // Name-sorted regular storage order is abstract(0), embedding(1),
        // title(2) — so embedding's storage ordinal is 1, NOT its table
        // position index of 2.
        let mut columns: indexmap::IndexMap<String, ColumnMetadata> = indexmap::IndexMap::new();
        columns.insert("id".into(), mk("id", ColumnKind::PartitionKey, 0, "int"));
        columns.insert("title".into(), mk("title", ColumnKind::Regular, 1, "text"));
        columns.insert(
            "abstract".into(),
            mk("abstract", ColumnKind::Regular, 2, "text"),
        );
        columns.insert(
            "embedding".into(),
            mk("embedding", ColumnKind::Regular, 3, "vector<float, 4>"),
        );

        let table_meta = TableMetadata {
            keyspace: "ks".into(),
            name: "t".into(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["id".into()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };

        let wanted =
            projection_storage_ordinals(&[SelectColumn::Column("embedding".into())], &table_meta);
        assert_eq!(
            wanted,
            Some(vec![1]),
            "projection must use the SSTable name-sorted storage ordinal (embedding=1), \
             not the table-definition position (embedding=2)"
        );

        let stmt = crate::parser::parse(
            "SELECT id, title FROM ks.t ORDER BY embedding ANN OF [0.1, 0.2, 0.3, 0.4] LIMIT 5",
        )
        .unwrap();
        let Statement::Select(select) = stmt else {
            panic!("expected SELECT statement");
        };
        assert_eq!(
            projection_storage_ordinals_for_select_scan(&select, &table_meta),
            Some(vec![2, 1]),
            "ANN scans must fetch the ORDER BY vector column even when it is not projected"
        );
    }

    // ── parse_permissions coverage for remaining variants ────────────

    #[test]
    fn parse_permissions_modify() {
        let perms = parse_permissions(&["MODIFY".into()]).unwrap();
        assert_eq!(perms.len(), 1);
        assert!(perms.contains(&Permission::Modify));
    }

    #[test]
    fn parse_permissions_multiple() {
        let perms = parse_permissions(&["SELECT".into(), "MODIFY".into(), "ALTER".into()]).unwrap();
        assert_eq!(perms.len(), 3);
        assert!(perms.contains(&Permission::Select));
        assert!(perms.contains(&Permission::Modify));
        assert!(perms.contains(&Permission::Alter));
    }

    #[test]
    fn parse_permissions_describe() {
        let perms = parse_permissions(&["DESCRIBE".into()]).unwrap();
        assert!(perms.contains(&Permission::Describe));
    }

    #[test]
    fn parse_permissions_execute() {
        let perms = parse_permissions(&["EXECUTE".into()]).unwrap();
        assert!(perms.contains(&Permission::Execute));
    }

    #[test]
    fn parse_permissions_authorize() {
        let perms = parse_permissions(&["AUTHORIZE".into()]).unwrap();
        assert!(perms.contains(&Permission::Authorize));
    }

    #[test]
    fn parse_permissions_create() {
        let perms = parse_permissions(&["CREATE".into()]).unwrap();
        assert!(perms.contains(&Permission::Create));
    }

    #[test]
    fn parse_permissions_drop() {
        let perms = parse_permissions(&["DROP".into()]).unwrap();
        assert!(perms.contains(&Permission::Drop));
    }

    // ── ast_resource_to_schema coverage ──────────────────────────────

    #[test]
    fn ast_resource_all_keyspaces() {
        let r = ast_resource_to_schema(&GrantResource::AllKeyspaces, &None).unwrap();
        assert!(matches!(r, Resource::AllKeyspaces));
    }

    #[test]
    fn ast_resource_keyspace() {
        let r = ast_resource_to_schema(&GrantResource::Keyspace("ks".into()), &None).unwrap();
        assert!(matches!(r, Resource::Keyspace(ref k) if k == "ks"));
    }

    #[test]
    fn ast_resource_table_with_keyspace() {
        let r = ast_resource_to_schema(
            &GrantResource::Table(Some("ks".into()), "tbl".into()),
            &None,
        )
        .unwrap();
        assert!(matches!(r, Resource::Table(ref k, ref t) if k == "ks" && t == "tbl"));
    }

    #[test]
    fn ast_resource_table_uses_session_keyspace() {
        let r = ast_resource_to_schema(
            &GrantResource::Table(None, "tbl".into()),
            &Some("session_ks".into()),
        )
        .unwrap();
        assert!(matches!(r, Resource::Table(ref k, ref t) if k == "session_ks" && t == "tbl"));
    }

    #[test]
    fn ast_resource_table_no_keyspace_errors() {
        let r = ast_resource_to_schema(&GrantResource::Table(None, "tbl".into()), &None);
        assert!(r.is_err());
    }

    #[test]
    fn ast_resource_all_roles() {
        let r = ast_resource_to_schema(&GrantResource::AllRoles, &None).unwrap();
        assert!(matches!(r, Resource::AllRoles));
    }

    #[test]
    fn ast_resource_role() {
        let r = ast_resource_to_schema(&GrantResource::Role("admin".into()), &None).unwrap();
        assert!(matches!(r, Resource::Role(ref n) if n == "admin"));
    }

    #[test]
    fn ast_resource_function_with_keyspace() {
        let r = ast_resource_to_schema(
            &GrantResource::Function {
                keyspace: Some("ks".into()),
                name: "myfunc".into(),
                arg_types: vec![],
            },
            &None,
        )
        .unwrap();
        assert!(matches!(r, Resource::Function(ref k, ref n, _) if k == "ks" && n == "myfunc"));
    }

    #[test]
    fn ast_resource_function_no_keyspace_errors() {
        let r = ast_resource_to_schema(
            &GrantResource::Function {
                keyspace: None,
                name: "myfunc".into(),
                arg_types: vec![],
            },
            &None,
        );
        assert!(r.is_err());
    }

    #[test]
    fn ast_resource_all_functions_with_keyspace() {
        let r = ast_resource_to_schema(
            &GrantResource::AllFunctions {
                keyspace: Some("ks".into()),
            },
            &None,
        )
        .unwrap();
        assert!(matches!(r, Resource::AllFunctions(ref k) if k == "ks"));
    }

    #[test]
    fn ast_resource_all_functions_no_keyspace_errors() {
        let r = ast_resource_to_schema(&GrantResource::AllFunctions { keyspace: None }, &None);
        assert!(r.is_err());
    }

    // ── Truncate with data verification ──────────────────────────────

    #[tokio::test]
    async fn truncate_clears_inserted_rows() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE trks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE trks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert some rows
        let stmt = crate::parser::parse("INSERT INTO trks.t (k, v) VALUES (1, 'hello')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("INSERT INTO trks.t (k, v) VALUES (2, 'world')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Truncate
        let stmt = crate::parser::parse("TRUNCATE trks.t").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "TRUNCATE should succeed: {:?}",
            result.err()
        );

        // SELECT should return 0 rows
        let stmt = crate::parser::parse("SELECT * FROM trks.t WHERE k = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let count = match &result {
            RouteResult::Result(b) => extract_row_count(b),
            _ => panic!("expected Result"),
        };
        assert_eq!(count, 0, "table should be empty after TRUNCATE");
    }

    // ── CREATE INDEX with session keyspace ───────────────────────────

    #[tokio::test]
    async fn create_index_uses_session_keyspace() {
        let (state, _dir) = setup();
        let ctx_no_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE sks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx_no_ks, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE sks.t (k int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx_no_ks, stmt).await.unwrap();

        // Use session keyspace
        let ctx_ks = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("sks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse("CREATE INDEX idx_sk ON t (v)").unwrap();
        let result = route(&state, &ctx_ks, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE INDEX with session keyspace should succeed: {:?}",
            result.err()
        );
    }

    // ── Drop keyspace routing ────────────────────────────────────────

    #[tokio::test]
    async fn drop_keyspace_removes_from_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dkks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let snap = state.schema.snapshot();
        assert!(snap.keyspaces.contains_key("dkks"));

        let stmt = crate::parser::parse("DROP KEYSPACE dkks").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP KEYSPACE should succeed: {:?}",
            result.err()
        );

        let snap = state.schema.snapshot();
        assert!(!snap.keyspaces.contains_key("dkks"));
    }

    // ── ALTER KEYSPACE routing ───────────────────────────────────────

    #[tokio::test]
    async fn alter_keyspace_changes_durable_writes() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE akks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER KEYSPACE via direct AST (parser doesn't support ALTER KEYSPACE yet)
        let stmt = Statement::AlterKeyspace(AlterKeyspaceStatement {
            name: "akks".into(),
            replication: Some(vec![
                ("class".into(), "SimpleStrategy".into()),
                ("replication_factor".into(), "1".into()),
            ]),
            durable_writes: Some(false),
        });
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALTER KEYSPACE should succeed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn use_nonexistent_keyspace_returns_error() {
        let (state, _dir) = setup();
        let dev = dev_auth();
        let stmt = crate::parser::parse("USE totally_bogus_ks").unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await;
        match result {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("totally_bogus_ks"),
                    "error should name the keyspace, got: {err_msg}"
                );
            }
            Ok(_) => panic!("USE against nonexistent keyspace must return an error"),
        }
    }

    #[tokio::test]
    async fn system_schema_keyspaces_where_filters_rows() {
        let (state, _dir) = setup();
        let dev = dev_auth();

        // Query system_schema.keyspaces WHERE keyspace_name = 'nonexistent_ks_xyz'
        let stmt = crate::parser::parse(
            "SELECT keyspace_name FROM system_schema.keyspaces \
             WHERE keyspace_name = 'nonexistent_ks_xyz'",
        )
        .unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await
        .unwrap();

        match &result {
            RouteResult::Result(b) => {
                let row_count = extract_row_count(b);
                assert_eq!(
                    row_count, 0,
                    "WHERE keyspace_name='nonexistent_ks_xyz' should return 0 rows, got {row_count}"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn system_schema_tables_where_filters_rows() {
        let (state, _dir) = setup();
        let dev = dev_auth();

        let stmt = crate::parser::parse(
            "SELECT table_name FROM system_schema.tables \
             WHERE keyspace_name = 'nonexistent_ks_xyz'",
        )
        .unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await
        .unwrap();

        match &result {
            RouteResult::Result(b) => {
                let row_count = extract_row_count(b);
                assert_eq!(
                    row_count, 0,
                    "WHERE keyspace_name='nonexistent_ks_xyz' on tables should return 0 rows, got {row_count}"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_now_from_system_local_returns_timeuuid() {
        let (state, _dir) = setup();
        let dev = dev_auth();
        let stmt = crate::parser::parse("SELECT now() FROM system.local").unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await
        .unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes(), "must be a Rows result");
                let row_count = extract_row_count(b);
                assert_eq!(
                    row_count, 1,
                    "SELECT now() FROM system.local must return 1 row"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    // -----------------------------------------------------------------------
    // p0-03: LWT must return ServerError, not silently fall through to CL path
    // -----------------------------------------------------------------------

    /// Build a SharedState with ClusterStateHolder::Cluster so that
    /// route_decision() returns RouteDecision::Accord for LWT statements.
    fn setup_cluster_mode() -> (SharedState, TempDir) {
        let (state, dir) = setup();

        // Replace the Standalone cluster state with Cluster mode.
        // RaftClusterState with an empty ring is sufficient — the LWT guard
        // fires before any routing into the ring happens.
        let ring = Arc::new(ArcSwap::from_pointee(
            ferrosa_cluster::ring::TokenRing::new(),
        ));
        let raft_state = ferrosa_cluster::RaftClusterState::new(ring, 1);
        state
            .cluster_state
            .store(Arc::new(ferrosa_cluster::ClusterStateHolder::Cluster(
                raft_state,
            )));
        (state, dir)
    }

    /// LWT INSERT IF NOT EXISTS in cluster mode must return ServerError with
    /// the gap-spec reference, not silently fall through to the CL path.
    ///
    /// This test exercises the fix for p0-03 (Outcome B): the server returns
    /// an explicit error rather than pretending to honour linearizability with
    /// a process-local substitute.
    #[tokio::test]
    async fn lwt_insert_if_not_exists_cluster_mode_returns_server_error() {
        let (state, _dir) = setup_cluster_mode();
        let dev = dev_auth();

        let stmt =
            crate::parser::parse("INSERT INTO system.local (key) VALUES ('test') IF NOT EXISTS")
                .unwrap();

        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                // serial_consistency = Some triggers the LWT routing guard.
                serial_consistency: Some(ConsistencyLevel::Serial),
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await;

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("LWT in cluster mode must return an error, not silently succeed"),
        };

        match err {
            CqlError::ServerError(msg) => {
                assert!(
                    msg.contains("p0-03b-accord-implementation-gap.md"),
                    "error must reference the gap spec, got: {msg:?}"
                );
                assert!(
                    msg.contains("LWT routing to Accord is not yet implemented"),
                    "error must name the unimplemented feature, got: {msg:?}"
                );
            }
            other => panic!("expected CqlError::ServerError, got {:?}", other),
        }
    }

    /// LWT in standalone mode must NOT return an error — standalone bypasses
    /// Accord entirely and falls through to the local storage path normally.
    ///
    /// This confirms the routing guard is scoped to cluster mode only, so
    /// development and single-node deployments are unaffected.
    #[tokio::test]
    async fn lwt_insert_if_not_exists_standalone_mode_does_not_error() {
        let (state, _dir) = setup(); // Standalone mode
        let dev = dev_auth();

        // We just need to confirm no ServerError fires; the insert itself
        // may fail for unrelated reasons (table not found, etc.) — those
        // are not the p0-03 error.
        let stmt =
            crate::parser::parse("INSERT INTO system.local (key) VALUES ('test') IF NOT EXISTS")
                .unwrap();

        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: Some(ConsistencyLevel::Serial),
                paging: crate::paging::PagingParams::default(),
                client_address: String::new(),
            },
            stmt,
        )
        .await;

        // Must not be the p0-03 ServerError.
        if let Err(CqlError::ServerError(ref msg)) = result {
            assert!(
                !msg.contains("p0-03b-accord-implementation-gap"),
                "standalone mode must not trigger the LWT Accord gap error, got: {msg:?}"
            );
        }
        // Any other outcome (Ok or a different error) is acceptable.
    }

    #[test]
    fn broad_select_paths_must_not_materialize_full_range_before_paging() {
        let source = include_str!("router.rs");
        let broad_scan = source
            .split("let scan_bound =")
            .nth(1)
            .and_then(|rest| rest.split("let mut all_rows = Vec::new();").next())
            .expect("broad select scan block must be present");

        assert!(
            !broad_scan.contains(".range_read(&table_id).await?"),
            "broad SELECT scans must consume a partition stream directly instead of materializing range_read Vec<Partition>"
        );
        assert!(
            broad_scan.contains("range_read_stream_all"),
            "broad SELECT scans must use the stream boundary for unbounded scans"
        );
        assert!(
            broad_scan.contains("range_read_stream_all_with")
                && broad_scan.contains("ctx.consistency")
                && broad_scan.contains("&table_strategy"),
            "broad SELECT streams must propagate the request consistency and keyspace replication strategy"
        );
    }

    #[test]
    fn single_partition_limit_pushes_row_cap_into_point_read() {
        let source = include_str!("router.rs");
        let pk_lookup_blocks: Vec<&str> = source
            .split("let decorated_key = bridge::build_decorated_key")
            .skip(1)
            .filter_map(|rest| rest.split("} else if let Some(in_rows)").next())
            .collect();

        assert!(
            pk_lookup_blocks
                .iter()
                .any(|block| block.contains("safe_partition_key_filter_row_limit")),
            "single-partition SELECT must derive a safe row cap from CQL LIMIT before reading storage"
        );
        assert!(
            pk_lookup_blocks
                .iter()
                .any(|block| block.contains(".pk_read_limited_rows(")),
            "single-partition SELECT ... LIMIT must push the row cap into the point-read path instead of materializing the whole partition"
        );
    }

    #[test]
    fn full_primary_key_lookup_pushes_clustering_key_into_point_read() {
        let source = include_str!("router.rs");
        let pk_lookup_blocks: Vec<&str> = source
            .split("let decorated_key = bridge::build_decorated_key")
            .skip(1)
            .filter_map(|rest| rest.split("} else if let Some(in_rows)").next())
            .collect();

        assert!(
            pk_lookup_blocks
                .iter()
                .any(|block| block.contains("extract_clustering_key_values")),
            "single-partition SELECT with equality on every clustering column must detect a full primary-key lookup"
        );
        assert!(
            pk_lookup_blocks
                .iter()
                .any(|block| block.contains(".pk_read_clustering_row(")),
            "full primary-key SELECT must read the exact clustering row instead of scanning the whole partition"
        );
    }

    #[test]
    fn count_filtering_scans_project_only_predicate_columns() {
        let source = include_str!("router.rs");
        let broad_scan = source
            .split("let projection_wanted =")
            .nth(1)
            .and_then(|rest| rest.split("let partitions =").next())
            .expect("projection decision block must be present");
        let projected_scan = source
            .split("let partitions = if let Some(wanted) = projection_wanted")
            .nth(1)
            .and_then(|rest| rest.split("if count_only_select {").next())
            .expect("projected scan block must be present");

        assert!(
            broad_scan.contains("count_projection_wanted")
                && broad_scan.contains("projection_storage_ordinals_for_count_predicates"),
            "COUNT(*) filtered scans must push predicate-only projection into storage"
        );
        assert!(
            source.contains("range_read_projected_stream_all_with"),
            "COUNT(*) filtered scans must use the projected streaming range path when possible"
        );
        assert!(
            !projected_scan.contains("range_read(&table_id).await?"),
            "COUNT(*) filtered scans must not materialize full partitions"
        );
    }

    #[test]
    fn planner_fallback_range_reads_must_propagate_replication_strategy() {
        let source = include_str!("router.rs");
        let planner_fallback = source
            .split("match scan_plan {")
            .nth(1)
            .and_then(|rest| rest.split("ScanPlan::FullScan =>").next())
            .expect("planner fallback block must be present");

        assert!(
            !planner_fallback.contains(".range_read(&table_id).await?"),
            "planner fallback range reads must not use coordinator bootstrap RF/CL defaults"
        );
        assert!(
            planner_fallback.contains("range_read_with(&table_id, ctx.consistency, &table_strategy)"),
            "planner fallback range reads must carry request consistency and keyspace replication strategy"
        );
    }

    // ── Coordinator-side range-scan paging / OOM bound ────────────────────
    //
    // These tests pin the coordinator-side behavior that a full-table scan
    // streams at most one bounded page and returns a `next_paging_state`
    // continuation, rather than accumulating the entire result into
    // `all_rows`. The bound must hold even when the client sends no
    // `page_size` (a sane default applies), and paged traversal must equal
    // the whole-scan result with no skipped or duplicated rows — including
    // when a single wide partition spans multiple pages.

    async fn run_ddl(state: &SharedState, ctx: &RequestContext<'_>, cql: &str) {
        route(state, ctx, crate::parser::parse(cql).unwrap())
            .await
            .unwrap_or_else(|e| panic!("DDL failed [{cql}]: {e}"));
    }

    fn paging_ctx<'a>(
        auth: &'a AuthContext,
        ks: &'a Option<String>,
        page_size: Option<i32>,
        paging_state: Option<Vec<u8>>,
    ) -> RequestContext<'a> {
        RequestContext {
            auth,
            current_keyspace: ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams {
                page_size,
                paging_state,
            },
            client_address: String::new(),
        }
    }

    /// Walk every page of `select_cql` with the given `page_size` (None means
    /// the client sent no page_size at all) and return the concatenated rows
    /// in page order plus the number of pages observed.
    async fn collect_all_pages(
        state: &SharedState,
        auth: &AuthContext,
        ks: &Option<String>,
        select_cql: &str,
        page_size: Option<i32>,
    ) -> (Vec<Vec<Option<CqlValue>>>, usize) {
        let select = match crate::parser::parse(select_cql).unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
        let mut collected: Vec<Vec<Option<CqlValue>>> = Vec::new();
        let mut paging_state: Option<Vec<u8>> = None;
        let mut pages = 0usize;
        // Hard cap so a broken cursor can never loop forever in tests.
        for _ in 0..100_000 {
            let ctx = paging_ctx(auth, ks, page_size, paging_state.clone());
            let res = route_select_raw(state, &ctx, &select).await.unwrap();
            if let Some(ps) = page_size {
                if ps > 0 {
                    assert!(
                        res.rows.len() <= ps as usize,
                        "page returned {} rows, exceeds page_size {ps}",
                        res.rows.len()
                    );
                }
            }
            let empty_page = res.rows.is_empty();
            collected.extend(res.rows);
            if !empty_page {
                pages += 1;
            }
            match res.paging_state {
                Some(ns) => paging_state = Some(ns),
                None => return (collected, pages),
            }
        }
        panic!("paging did not terminate within 100k pages — cursor is not advancing");
    }

    /// Setup: keyspace + a narrow-partition table with `n` rows (id is the PK).
    async fn setup_wide_scan_table(n: i64) -> (SharedState, TempDir, AuthContext, Option<String>) {
        let (state, dir) = setup();
        let auth = dev_auth();
        let ks = Some("pageks".to_string());
        let ctx = paging_ctx(&auth, &ks, None, None);
        run_ddl(
            &state,
            &ctx,
            "CREATE KEYSPACE pageks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        )
        .await;
        run_ddl(
            &state,
            &ctx,
            "CREATE TABLE pageks.t (id int PRIMARY KEY, v int)",
        )
        .await;
        for i in 0..n {
            run_ddl(
                &state,
                &ctx,
                &format!("INSERT INTO pageks.t (id, v) VALUES ({i}, {})", i * 10),
            )
            .await;
        }
        (state, dir, auth, ks)
    }

    #[tokio::test]
    async fn range_scan_first_page_is_bounded_by_page_size() {
        // N >> page_size. First page must return exactly page_size rows and a
        // non-None continuation. Fail-before: the scan accumulated all N rows.
        let n = 5_000;
        let page_size = 200;
        let (state, _dir, auth, ks) = setup_wide_scan_table(n).await;

        let select = match crate::parser::parse("SELECT * FROM pageks.t").unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
        let ctx = paging_ctx(&auth, &ks, Some(page_size), None);
        let res = route_select_raw(&state, &ctx, &select).await.unwrap();

        assert_eq!(
            res.rows.len(),
            page_size as usize,
            "first page must return exactly page_size rows, not the whole table"
        );
        assert!(
            res.paging_state.is_some(),
            "a scan with more rows than page_size must return a continuation token"
        );
    }

    #[tokio::test]
    async fn range_scan_paged_traversal_equals_whole_scan_no_gaps_or_dupes() {
        // Paged traversal (page_size << N) must equal the unpaged scan exactly,
        // in order, with no gaps and no duplicates.
        let n = 3_000;
        let (state, _dir, auth, ks) = setup_wide_scan_table(n).await;

        // Whole scan in one shot (no paging).
        let (whole, _) =
            collect_all_pages(&state, &auth, &ks, "SELECT * FROM pageks.t", None).await;
        // Sort by id so we compare row sets independent of token order.
        let mut whole_sorted = whole.clone();
        whole_sorted.sort_by(|a, b| a[0].cmp(&b[0]));
        assert_eq!(
            whole_sorted.len(),
            n as usize,
            "whole scan must see every row"
        );

        let (paged, pages) =
            collect_all_pages(&state, &auth, &ks, "SELECT * FROM pageks.t", Some(137)).await;
        assert!(pages > 1, "expected multiple pages, got {pages}");

        let mut paged_sorted = paged.clone();
        paged_sorted.sort_by(|a, b| a[0].cmp(&b[0]));

        assert_eq!(
            paged_sorted.len(),
            whole_sorted.len(),
            "paged traversal must yield the same row count as the whole scan"
        );
        assert_eq!(
            paged_sorted, whole_sorted,
            "paged union must equal the whole scan byte-for-byte (no gaps, no dupes)"
        );

        // Explicit duplicate check on the partition key.
        let mut ids: Vec<_> = paged.iter().map(|r| r[0].clone()).collect();
        ids.sort();
        let unique = {
            let mut u = ids.clone();
            u.dedup();
            u.len()
        };
        assert_eq!(unique, ids.len(), "paged traversal emitted duplicate rows");
    }

    #[tokio::test]
    async fn range_scan_with_no_page_size_applies_default_bounded_page() {
        // A SELECT with NO client page_size must still return a bounded first
        // page plus a continuation rather than pulling the whole table.
        // Fail-before: with no page_size, the scan returned every row.
        let default = crate::paging::default_scan_page_size();
        let n = (default as i64) * 2 + 17; // safely exceeds one default page

        let (state, _dir, auth, ks) = setup_wide_scan_table(n).await;
        let select = match crate::parser::parse("SELECT * FROM pageks.t").unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
        let ctx = paging_ctx(&auth, &ks, None, None);
        let res = route_select_raw(&state, &ctx, &select).await.unwrap();

        assert!(
            res.rows.len() <= default,
            "no-page-size scan returned {} rows, exceeds default page {default}",
            res.rows.len()
        );
        assert!(
            res.paging_state.is_some(),
            "no-page-size scan over a large table must return a continuation token"
        );

        // And the full paged traversal (default page applied each call) still
        // returns every row exactly once.
        let (paged, _) =
            collect_all_pages(&state, &auth, &ks, "SELECT * FROM pageks.t", None).await;
        let mut ids: Vec<_> = paged.iter().map(|r| r[0].clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            n as usize,
            "default-paged traversal must still visit every row exactly once"
        );
    }

    #[tokio::test]
    async fn limit_query_unaffected_by_default_paging() {
        // LIMIT N must still cap the result and not be silently re-paged into
        // multiple pages when the client did not request paging.
        let n = 1_000;
        let (state, _dir, auth, ks) = setup_wide_scan_table(n).await;
        let select = match crate::parser::parse("SELECT * FROM pageks.t LIMIT 5").unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
        let ctx = paging_ctx(&auth, &ks, None, None);
        let res = route_select_raw(&state, &ctx, &select).await.unwrap();
        assert_eq!(res.rows.len(), 5, "LIMIT 5 must return exactly 5 rows");
        assert!(
            res.paging_state.is_none(),
            "a LIMIT smaller than the default page must not return a continuation"
        );
    }

    #[tokio::test]
    async fn point_read_unaffected_by_paging() {
        // PK-equality (point read) must behave unchanged with default paging.
        let n = 100;
        let (state, _dir, auth, ks) = setup_wide_scan_table(n).await;
        let select = match crate::parser::parse("SELECT * FROM pageks.t WHERE id = 42").unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
        let ctx = paging_ctx(&auth, &ks, None, None);
        let res = route_select_raw(&state, &ctx, &select).await.unwrap();
        assert_eq!(res.rows.len(), 1, "point read must return exactly one row");
        assert_eq!(res.rows[0][0], Some(CqlValue::Int(42)));
        assert!(
            res.paging_state.is_none(),
            "point read needs no continuation"
        );
    }

    // -----------------------------------------------------------------------
    // Cross-crate roundtrip: the router's `build_lwt_mutation` serialize_into
    // output is EXACTLY what the real `EngineStorageApplier` deserialize_from
    // can decode and persist — with real partition-key extraction and real
    // cell values from the INSERT, and the persisted cell timestamp honoring
    // the Accord-agreed `t` (not the coordinator's materialize-time wall clock).
    //
    // This guards two production-correctness contracts that no in-crate test
    // can cover, because the producer (`build_lwt_mutation`, ferrosa-cql) and
    // the consumer (`EngineStorageApplier`, ferrosa-cluster) live in different
    // crates:
    //   1. SERIALIZATION SKEW: if the router's `Mutation::serialize_into` and
    //      the applier's `Mutation::deserialize_from` ever drift apart, the
    //      decode fails (or yields a wrong key/row) and this test fails loud.
    //   2. LINEARIZABILITY: the router stamps cells with `SystemTime::now()`
    //      micros; under clock skew that wall-clock stamp can invert the
    //      Accord order. The applier MUST re-stamp to `t`, so the persisted
    //      cell timestamp equals the agreed `t`, not the wall clock.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn build_lwt_mutation_round_trips_through_engine_storage_applier() {
        use ferrosa_cluster::accord::apply::{ApplyMutation, EngineStorageApplier, StorageApplier};
        use ferrosa_common::accord::{Timestamp, TxnId};
        use ferrosa_storage::TableId;

        let (state, _dir) = setup();
        let auth = dev_auth();
        let no_ks: Option<String> = None;
        let ctx = test_ctx(&auth, &no_ks);

        // Real keyspace + table.
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE lwt_rt WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();
        let stmt =
            crate::parser::parse("CREATE TABLE lwt_rt.accts (id text PRIMARY KEY, balance text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Parse a real LWT INSERT. `build_lwt_mutation` ignores the IF clause
        // (that is the read-phase predicate); it materializes the write.
        let stmt = crate::parser::parse(
            "INSERT INTO lwt_rt.accts (id, balance) VALUES ('acct-1', '100') IF NOT EXISTS",
        )
        .unwrap();

        // PRODUCER: the router builds the (key, mutation) wire payload exactly
        // as the Accord coordinator would ship it in the Apply phase.
        let (key_bytes, mutation_bytes) =
            build_lwt_mutation(&state, &ctx, &stmt).expect("router must build the LWT mutation");

        // The conflict-ordering key must be the real partition-key bytes —
        // proving real key extraction (not the old `b"lwt-placeholder-key"`).
        assert_eq!(
            key_bytes,
            b"acct-1".to_vec(),
            "Accord key must be the real partition-key bytes, not a placeholder"
        );

        // CONSUMER: a real applier over the SAME engine decodes those exact
        // bytes and persists. The agreed `t` is deliberately a tiny value
        // (777), far below the wall-clock micros the router stamped cells with,
        // so a successful read at ts==777 proves the applier re-stamped to `t`.
        let agreed = Timestamp::synthetic(777);
        let applier = EngineStorageApplier::new(state.engine.clone());
        applier
            .apply(
                TxnId::new(1, agreed),
                ApplyMutation {
                    data: mutation_bytes,
                    t: agreed,
                    deps: vec![],
                },
            )
            .expect("applier must decode the router's bytes and persist (no skew)");

        // The row the router serialized must now be readable via the engine,
        // with the REAL cell value from the INSERT.
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(key_bytes));
        let partition = state
            .engine
            .read(&TableId::new("lwt_rt", "accts"), &key)
            .unwrap()
            .expect("the round-tripped row must be persisted and readable");
        let row = partition.rows.first().expect("one row");
        let (_, balance_cell) = row
            .cells
            .iter()
            .find(|(_, c)| c.value.as_deref() == Some(b"100".as_slice()))
            .expect("the real INSERT cell value must survive the roundtrip");

        // The persisted LWW cell timestamp must equal the Accord-agreed `t`
        // (777), NOT the router's materialize-time wall clock. If the applier
        // honored the wall clock, this would be ~1.7e15 micros and fail.
        assert_eq!(
            balance_cell.timestamp, 777,
            "persisted cell timestamp must be the Accord-agreed t, not the coordinator wall clock"
        );
    }
}
