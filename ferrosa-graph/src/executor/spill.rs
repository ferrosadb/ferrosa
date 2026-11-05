//! Module: Spill adapter — lets graph result rows use the storage engine's
//! bounded-memory external merge sort.
//! Correctness: Correct when an `OrderBySink` orders rows EXACTLY as the
//!   operator's in-memory ORDER BY always has (same key evaluation, same
//!   comparator, same ASC/DESC and Null handling), so spilling changes memory
//!   behavior and nothing else; and when the hidden sort-key columns it appends
//!   are stripped from every row before a consumer can see one.
//! Last revised: 2026-11-04
//! Last changed: Hidden sort keys. Every ORDER BY term is evaluated against the
//!   row's bindings at projection time and carried as a trailing hidden column,
//!   so a non-projected term no longer forces the whole result into an
//!   in-memory sort. `finish_rows` and `spill_order_by` are deleted: the merge
//!   is only ever consumed as a stream (fix/stream-graph-order-by).
//!
//! # Why
//!
//! `ORDER BY` without `LIMIT` is a pipeline-breaker: every row must be seen
//! before the first output row is known. The graph executor used to buffer the
//! whole result and silently truncate at `max_result_rows` — no error, no flag,
//! so a client could not distinguish a partial result from a complete one.
//!
//! ferrosa-cql already solved this with [`ferrosa_storage::ExternalSorter`]:
//! accumulate to a byte threshold, spill sorted runs to a temp directory, k-way
//! merge on finish, fail loud on any spill/merge I/O error — with the temp dir
//! held by a `TempSortTableReservation` whose `Drop` removes it, so a cancelled
//! query cleans up automatically. Owner decision (2026-07-25): the graph path
//! SPILLS rather than caps, and `max_result_rows` is then removed outright.
//!
//! # Hidden sort keys
//!
//! A spilled row is compared in isolation — read back from a run file, with no
//! binding context — so the sorter can only order by values IN the row. Cypher
//! permits `RETURN n.name ORDER BY n.age`, where the key is not projected. The
//! sink used to refuse such a query (`try_new` returned `Ok(None)`) and the
//! caller sorted the whole accumulated `Vec` in memory: a silent widening of
//! the memory bound.
//!
//! Now `OrderKeys::attach` evaluates every ORDER BY term against the row's
//! bindings when the row is produced — exactly the evaluation the operator's
//! in-memory sort performs — and appends the values as hidden trailing columns.
//! The comparator orders by those columns only; `OrderKeys::detach` strips
//! them on the way out, so the `columns` a client sees never change. The hidden
//! keys are counted in `GraphRow::retained_bytes`, so the spill threshold
//! stays honest about what is resident.
//!
//! # Never re-collected
//!
//! There is no `finish_rows`. A sink's only output is `OrderBySink::finish_stream`,
//! which reads the merge on pull. A buffered `GraphResult` caller may collect
//! that stream at its own API boundary, but no operator can collect it.
//!
//! # Why a graph-specific comparator
//!
//! The sorter is generic over `SpillOrder<T>` precisely so callers keep their
//! own ordering. Graph rows are `Vec<serde_json::Value>`, and the two in-memory
//! graph sorts have semantics `CqlValue`'s `Ord` does not share (see
//! [`OrderSemantics`]). Converting graph rows to `CqlValue` to reuse the CQL
//! comparator would silently reorder those cases; this adapter preserves them.
//!
//! # No spill backend
//!
//! `GraphEngineConfig::spill == None` (unit tests, embedded use) is the one
//! designed in-memory fallback: the same keyed rows, the same comparator,
//! sorted in a `Vec`. It is observable — the first occurrence logs at `warn`,
//! and [`in_memory_order_by_sorts`] counts every one — and it is never reached
//! when a backend is configured.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use ferrosa_storage::external_sort::{SpillOrder, SpillRow};

use super::stream::{RowStream, RowVals};
use crate::error::{GraphError, Result};
use crate::parser::ReturnClause;

/// Reserves a cancellable temp directory for a spilling ORDER BY sort.
///
/// Hung off `GraphEngineConfig` — which the executor already
/// threads everywhere — rather than plumbing a `StorageEngine` through every
/// recursive `execute` call. `None` means no spill backend is available (unit
/// tests, embedded use); ORDER BY then takes the observable in-memory fallback.
///
/// The returned reservation's `Drop` removes the directory, so an aborted or
/// cancelled query cleans up the same way a successful one does.
pub trait SpillReserver: Send + Sync + std::fmt::Debug {
    fn reserve(
        &self,
        label: &str,
    ) -> ferrosa_common::Result<ferrosa_storage::TempSortTableReservation>;
}

/// A graph result row that the external sorter can spill.
///
/// Newtype because both `Vec<serde_json::Value>` and the `SpillRow` trait are
/// foreign to this crate, so the impl needs a local type.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GraphRow(pub RowVals);

impl SpillRow for GraphRow {
    /// The row's `Vec` header and slot capacity plus [`json_value_heap_bytes`]
    /// of every value — hidden sort-key columns included, since they are
    /// resident too.
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.0.capacity() * std::mem::size_of::<serde_json::Value>()
            + self.0.iter().map(json_value_heap_bytes).sum::<usize>()
    }
}

/// Heap bytes a JSON value keeps alive beyond its fixed enum size: string and
/// array CAPACITY, and for an object each entry's key string, value slot, key
/// capacity and nested value. Map-node overhead is not counted.
fn json_value_heap_bytes(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::String(s) => s.capacity(),
        serde_json::Value::Array(items) => {
            items.capacity() * std::mem::size_of::<serde_json::Value>()
                + items.iter().map(json_value_heap_bytes).sum::<usize>()
        }
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, val)| {
                std::mem::size_of::<String>()
                    + std::mem::size_of::<serde_json::Value>()
                    + k.capacity()
                    + json_value_heap_bytes(val)
            })
            .sum::<usize>(),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
    }
}

/// Which in-memory ORDER BY a sort must reproduce exactly.
///
/// The executor has two, and they differ on mixed types, booleans and complex
/// values. Each operator keeps the one it has always used, so moving its sort
/// onto the spilling sorter (or onto hidden keys) changes nothing observable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSemantics {
    /// `sort_rows`: a term resolves to its projected column; compared with
    /// `compare_json_values` (Null first, mixed types and complex values
    /// Equal, booleans Equal). Used by var-length paths and WCO joins. A term
    /// that is not projected is evaluated against the row's bindings (it used
    /// to be skipped silently).
    ProjectedColumns,
    /// `sort_projected_rows_by_bindings`: a term is evaluated against the
    /// bindings first and falls back to its projected column
    /// (`projected_order_value`); compared with `pipeline_value_cmp`. Used by
    /// Expand.
    Bindings,
    /// No ORDER BY: the single hidden key is the row's arrival number, compared
    /// as a `u64`, so a spilled unordered result returns in production order.
    Arrival,
}

/// One resolved ORDER BY term: the row column index, and whether ascending.
///
/// Resolved to an INDEX up front (rather than carrying the expression) because
/// the sorter compares rows in isolation — including rows read back from a
/// spilled run, where no binding context exists. In the executor the index
/// always points at a hidden key column (see `OrderKeys`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphOrderTerm {
    pub column: usize,
    pub ascending: bool,
}

/// Graph ORDER BY comparator for the external sorter.
///
/// Applies terms left-to-right; the first unequal column decides.
#[derive(Debug, Clone)]
pub struct GraphRowOrder {
    terms: Vec<GraphOrderTerm>,
    semantics: OrderSemantics,
}

impl GraphRowOrder {
    /// An order with `sort_rows` semantics (`compare_json_values`).
    pub fn new(terms: Vec<GraphOrderTerm>) -> Self {
        Self::with_semantics(terms, OrderSemantics::ProjectedColumns)
    }

    pub fn with_semantics(terms: Vec<GraphOrderTerm>, semantics: OrderSemantics) -> Self {
        Self { terms, semantics }
    }

    /// Compare two rows under this order.
    pub fn compare_rows(&self, a: &RowVals, b: &RowVals) -> Ordering {
        const NULL: serde_json::Value = serde_json::Value::Null;
        for term in &self.terms {
            let cmp = match self.semantics {
                OrderSemantics::ProjectedColumns => {
                    super::expand::compare_json_values(a.get(term.column), b.get(term.column))
                }
                OrderSemantics::Bindings => super::expand::pipeline_value_cmp(
                    a.get(term.column).unwrap_or(&NULL),
                    b.get(term.column).unwrap_or(&NULL),
                ),
                OrderSemantics::Arrival => {
                    let arrival = |row: &RowVals| row.get(term.column).and_then(|v| v.as_u64());
                    arrival(a).cmp(&arrival(b))
                }
            };
            let cmp = if term.ascending { cmp } else { cmp.reverse() };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

impl SpillOrder<GraphRow> for GraphRowOrder {
    fn compare(&self, a: &GraphRow, b: &GraphRow) -> Ordering {
        self.compare_rows(&a.0, &b.0)
    }
}

/// The hidden sort-key layout for one query: `visible` projected columns,
/// followed by one key column per ORDER BY term — or, for a result with no
/// ORDER BY, a single arrival-number key, so an unordered result can spill
/// and still come back in exactly the order it was produced.
#[derive(Debug, Clone)]
pub(super) struct OrderKeys {
    order_by: Vec<crate::parser::OrderItem>,
    columns: Vec<String>,
    semantics: OrderSemantics,
    /// `Some(next)` in arrival-order mode: the number the next row is keyed by.
    next_arrival: Option<u64>,
}

impl OrderKeys {
    fn new(columns: &[String], return_clause: &ReturnClause, semantics: OrderSemantics) -> Self {
        Self {
            order_by: return_clause.order_by.clone(),
            columns: columns.to_vec(),
            semantics,
            next_arrival: None,
        }
    }

    /// Keys for a result with no ORDER BY: each row is keyed by its arrival
    /// number, so the merge reproduces production order.
    fn arrival_order(columns: &[String]) -> Self {
        Self {
            order_by: Vec::new(),
            columns: columns.to_vec(),
            semantics: OrderSemantics::Arrival,
            next_arrival: Some(0),
        }
    }

    fn visible(&self) -> usize {
        self.columns.len()
    }

    /// How many hidden key columns follow the visible ones.
    fn key_count(&self) -> usize {
        if self.next_arrival.is_some() {
            1
        } else {
            self.order_by.len()
        }
    }

    /// The comparator over the hidden key columns.
    fn row_order(&self) -> GraphRowOrder {
        let visible = self.visible();
        if self.next_arrival.is_some() {
            return GraphRowOrder::with_semantics(
                vec![GraphOrderTerm {
                    column: visible,
                    ascending: true,
                }],
                OrderSemantics::Arrival,
            );
        }
        let terms = self
            .order_by
            .iter()
            .enumerate()
            .map(|(i, item)| GraphOrderTerm {
                column: visible + i,
                ascending: matches!(item.direction, crate::parser::SortDir::Asc),
            })
            .collect();
        GraphRowOrder::with_semantics(terms, self.semantics)
    }

    /// Append the hidden keys to a projected row: its arrival number in
    /// arrival-order mode, otherwise one key per ORDER BY term evaluated
    /// against the bindings the row was projected from.
    pub(super) fn attach(
        &mut self,
        mut row: RowVals,
        bindings: &HashMap<String, serde_json::Value>,
    ) -> Result<RowVals> {
        let visible = self.visible();
        if row.len() != visible {
            return Err(GraphError::Internal(format!(
                "ORDER BY key attach: row has {} values, projection has {visible} columns",
                row.len()
            )));
        }
        if let Some(next) = self.next_arrival.as_mut() {
            row.push(serde_json::Value::from(*next));
            *next += 1;
            return Ok(row);
        }
        row.reserve(self.order_by.len());
        for item in &self.order_by {
            let key = match self.semantics {
                OrderSemantics::Bindings => super::expand::projected_order_value(
                    &item.expr,
                    &self.columns,
                    &row[..visible],
                    bindings,
                )?,
                OrderSemantics::ProjectedColumns => {
                    let name = super::expand::expr_to_column_name(&item.expr);
                    match self.columns.iter().position(|c| c == &name) {
                        Some(index) => row[index].clone(),
                        None => super::eval::eval_expr(&item.expr, bindings)?,
                    }
                }
                OrderSemantics::Arrival => {
                    return Err(GraphError::Internal(
                        "ORDER BY key attach: arrival semantics given ORDER BY terms".to_string(),
                    ))
                }
            };
            row.push(key);
        }
        Ok(row)
    }

    /// Strip the hidden keys, restoring exactly the projected row. Fails loud
    /// on a width mismatch: a row that is not `visible + keys` wide means the
    /// layout was violated, and truncating it anyway could hand a client a
    /// key value as if it were a projected one.
    pub(super) fn detach(&self, mut row: RowVals) -> Result<RowVals> {
        let visible = self.visible();
        let expected = visible + self.key_count();
        if row.len() != expected {
            return Err(GraphError::Internal(format!(
                "ORDER BY key detach: row has {} values, expected {expected}",
                row.len()
            )));
        }
        row.truncate(visible);
        Ok(row)
    }
}

/// A spill-backed ORDER BY accumulator.
///
/// Producers hand each row over AS IT IS PRODUCED, with the bindings it was
/// projected from; memory is bounded by `spill_threshold_bytes` regardless of
/// result size. [`SpillSortSink::try_new`] returns `Ok(None)` only when there
/// is no spill backend or no ORDER BY — never because of what a term is.
///
/// Fails loud on any spill/merge I/O error — a dropped run would silently lose
/// rows, which is strictly worse than the old cap.
pub(super) struct SpillSortSink {
    keys: OrderKeys,
    reservation: ferrosa_storage::TempSortTableReservation,
    sorter: ferrosa_storage::external_sort::ExternalSorter<GraphRow, GraphRowOrder>,
    rows_pushed: usize,
}

impl std::fmt::Debug for SpillSortSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillSortSink")
            .field("rows_pushed", &self.rows_pushed)
            .field("temp_dir", &self.reservation.path())
            .finish()
    }
}

impl SpillSortSink {
    /// Build a sink when a spill backend is configured and the query has an
    /// ORDER BY; `Ok(None)` otherwise.
    ///
    /// A LIMIT deliberately does NOT disqualify the sink. The sort has to
    /// consider every candidate row whatever the limit is, so the limit shrinks
    /// the OUTPUT, not the work; the caller takes `limit` from the sorted
    /// stream.
    pub(super) fn try_new(
        columns: &[String],
        return_clause: &ReturnClause,
        config: &super::expand::GraphEngineConfig,
        label: &str,
        semantics: OrderSemantics,
    ) -> Result<Option<Self>> {
        if return_clause.order_by.is_empty() {
            return Ok(None);
        }
        Self::with_keys(
            OrderKeys::new(columns, return_clause, semantics),
            config,
            label,
        )
    }

    /// A sink for a result with NO ORDER BY: rows spill keyed by arrival
    /// number, and the stream returns them in production order. `Ok(None)`
    /// only when no spill backend is configured.
    pub(super) fn try_new_in_arrival_order(
        columns: &[String],
        config: &super::expand::GraphEngineConfig,
        label: &str,
    ) -> Result<Option<Self>> {
        Self::with_keys(OrderKeys::arrival_order(columns), config, label)
    }

    fn with_keys(
        keys: OrderKeys,
        config: &super::expand::GraphEngineConfig,
        label: &str,
    ) -> Result<Option<Self>> {
        let Some(reserver) = config.spill.as_ref() else {
            return Ok(None);
        };
        let reservation = reserver
            .reserve(label)
            .map_err(|e| GraphError::Internal(format!("ORDER BY temp-sort setup failed: {e}")))?;
        let sorter = ferrosa_storage::external_sort::ExternalSorter::new(
            reservation.path(),
            keys.row_order(),
            config.spill_threshold_bytes,
        );
        Ok(Some(Self {
            keys,
            reservation,
            sorter,
            rows_pushed: 0,
        }))
    }

    /// Accept one produced row. Spills a sorted run when the buffer crosses
    /// the byte threshold; fails loud on I/O error.
    pub(super) fn push(
        &mut self,
        row: RowVals,
        bindings: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let keyed = self.keys.attach(row, bindings)?;
        self.rows_pushed += 1;
        self.sorter
            .push(GraphRow(keyed))
            .map_err(|e| GraphError::Internal(format!("ORDER BY spill: {e}")))
    }

    /// Finish the sort and stream the merged output WITHOUT re-materializing
    /// it: each pull reads the next row from the merge and strips its hidden
    /// keys. The temp-dir reservation is moved into the stream, so the spilled
    /// runs live exactly as long as a consumer can still pull from them and
    /// are removed when the stream drops — cancelled, exhausted, or abandoned
    /// alike.
    pub(super) fn finish_stream(self) -> Result<RowStream<'static>> {
        let spilled_to_disk = self.sorter.spilled();
        let runs = self.sorter.run_count();
        let rows_pushed = self.rows_pushed;
        let temp_dir = self.reservation.path().display().to_string();
        let sorted = self
            .sorter
            .finish()
            .map_err(|e| GraphError::Internal(format!("ORDER BY spill finish: {e}")))?;
        if spilled_to_disk {
            tracing::info!(
                rows = rows_pushed,
                runs,
                temp_sort_table = %temp_dir,
                "graph ORDER BY streaming from a cancellable temp-sort table"
            );
        }
        let reservation = self.reservation;
        let keys = self.keys;
        let iter = sorted.map(move |row| {
            // The closure owns `reservation`; referencing it keeps the temp
            // dir alive until the stream itself is dropped.
            let _keep_alive = &reservation;
            let row = row.map_err(|e| GraphError::Internal(format!("ORDER BY merge: {e}")))?;
            keys.detach(row.0)
        });
        Ok(Box::pin(futures::stream::iter(iter)))
    }
}

/// Every in-memory ORDER BY sort taken because no spill backend was configured.
static IN_MEMORY_ORDER_BY_SORTS: AtomicU64 = AtomicU64::new(0);

/// How many ORDER BY sorts have run in memory for lack of a spill backend
/// since process start. Zero in a server: the engine always configures one.
pub fn in_memory_order_by_sorts() -> u64 {
    IN_MEMORY_ORDER_BY_SORTS.load(AtomicOrdering::Relaxed)
}

/// The designed no-backend fallback: keyed rows sorted in a `Vec`, with the
/// same comparator the spilling sorter uses. Unbounded by construction — which
/// is why it is counted, warned about, and unreachable with a backend.
#[derive(Debug)]
pub(super) struct InMemorySortSink {
    keys: OrderKeys,
    rows: Vec<RowVals>,
}

impl InMemorySortSink {
    fn new(columns: &[String], return_clause: &ReturnClause, semantics: OrderSemantics) -> Self {
        let prior = IN_MEMORY_ORDER_BY_SORTS.fetch_add(1, AtomicOrdering::Relaxed);
        if prior == 0 {
            tracing::warn!(
                "graph ORDER BY is sorting in memory: no spill backend is configured \
                 (GraphEngineConfig::spill is None), so the result is not bounded by \
                 spill_threshold_bytes. Expected only in unit tests and embedded use; \
                 further occurrences are counted by in_memory_order_by_sorts()"
            );
        }
        Self {
            keys: OrderKeys::new(columns, return_clause, semantics),
            rows: Vec::new(),
        }
    }

    fn push(&mut self, row: RowVals, bindings: &HashMap<String, serde_json::Value>) -> Result<()> {
        self.rows.push(self.keys.attach(row, bindings)?);
        Ok(())
    }

    fn finish_stream(mut self) -> RowStream<'static> {
        let order = self.keys.row_order();
        // Stable, like the in-memory sorts this replaces: ties keep
        // production order.
        self.rows.sort_by(|a, b| order.compare_rows(a, b));
        let keys = self.keys;
        Box::pin(futures::stream::iter(
            self.rows.into_iter().map(move |row| keys.detach(row)),
        ))
    }
}

/// An ORDER BY accumulator: spill-backed whenever a backend is configured,
/// the observable in-memory fallback otherwise.
#[derive(Debug)]
pub(super) enum OrderBySink {
    Spill(SpillSortSink),
    InMemory(InMemorySortSink),
}

impl OrderBySink {
    /// `Ok(None)` only when the query has no ORDER BY.
    pub(super) fn new(
        columns: &[String],
        return_clause: &ReturnClause,
        config: &super::expand::GraphEngineConfig,
        label: &str,
        semantics: OrderSemantics,
    ) -> Result<Option<Self>> {
        if return_clause.order_by.is_empty() {
            return Ok(None);
        }
        if let Some(sink) =
            SpillSortSink::try_new(columns, return_clause, config, label, semantics)?
        {
            return Ok(Some(Self::Spill(sink)));
        }
        Ok(Some(Self::InMemory(InMemorySortSink::new(
            columns,
            return_clause,
            semantics,
        ))))
    }

    pub(super) fn push(
        &mut self,
        row: RowVals,
        bindings: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        match self {
            Self::Spill(sink) => sink.push(row, bindings),
            Self::InMemory(sink) => sink.push(row, bindings),
        }
    }

    /// The sorted rows, hidden keys stripped, as a stream.
    pub(super) fn finish_stream(self) -> Result<RowStream<'static>> {
        match self {
            Self::Spill(sink) => sink.finish_stream(),
            Self::InMemory(sink) => Ok(sink.finish_stream()),
        }
    }
}

/// DISTINCT then LIMIT over an already-ordered (or unordered) row stream — the
/// order every result tail applies them: sort, then dedup, then limit.
///
/// `dedup_stream` hashes every row it has emitted, so it is bounded by the
/// number of DISTINCT rows rather than by the number of candidate rows. It
/// must run AFTER the hidden keys are stripped (they are, by
/// [`OrderBySink::finish_stream`]): two rows that project identically but sort
/// by different keys are duplicates.
pub(super) fn distinct_then_limit<'a>(
    mut rows: RowStream<'a>,
    return_clause: &ReturnClause,
) -> RowStream<'a> {
    if return_clause.distinct {
        rows = super::stream::dedup_stream(rows);
    }
    if let Some(limit) = return_clause.limit {
        use futures::StreamExt as _;
        rows = Box::pin(rows.take(limit.max(0) as usize));
    }
    rows
}

/// Every unordered result buffered in memory because no spill backend was
/// configured.
static IN_MEMORY_UNORDERED_RESULTS: AtomicU64 = AtomicU64::new(0);

/// How many results with no ORDER BY were held in a `Vec` for lack of a spill
/// backend since process start. Zero in a server: the engine always configures
/// one.
pub fn in_memory_unordered_results() -> u64 {
    IN_MEMORY_UNORDERED_RESULTS.load(AtomicOrdering::Relaxed)
}

/// Where an operator's produced rows go.
///
/// With a spill backend, every result spills: an ORDER BY through its sort
/// keys, and an unordered result keyed by arrival number so it comes back in
/// production order. Without a backend (unit tests, embedded use) the designed
/// in-memory fallbacks apply, and each is counted and warned about once.
#[derive(Debug)]
pub(super) enum RowAcc {
    /// No ORDER BY and no spill backend: a counted in-memory buffer.
    InMemoryUnordered(Vec<RowVals>),
    /// Boxed: the sink carries the external sorter and its spill-pool charge,
    /// ten times the size of the in-memory buffer.
    Ordered(Box<OrderBySink>),
    /// No ORDER BY, spill backend configured: spills in arrival order.
    ArrivalOrder(Box<SpillSortSink>),
}

impl RowAcc {
    pub(super) fn new(
        columns: &[String],
        return_clause: &ReturnClause,
        config: &super::expand::GraphEngineConfig,
        label: &str,
        semantics: OrderSemantics,
    ) -> Result<Self> {
        if let Some(sink) = OrderBySink::new(columns, return_clause, config, label, semantics)? {
            return Ok(Self::Ordered(Box::new(sink)));
        }
        if let Some(sink) = SpillSortSink::try_new_in_arrival_order(columns, config, label)? {
            return Ok(Self::ArrivalOrder(Box::new(sink)));
        }
        let prior = IN_MEMORY_UNORDERED_RESULTS.fetch_add(1, AtomicOrdering::Relaxed);
        if prior == 0 {
            tracing::warn!(
                "graph result is buffered in memory: no spill backend is configured \
                 (GraphEngineConfig::spill is None), so it is not bounded by \
                 spill_threshold_bytes. Expected only in unit tests and embedded use; \
                 further occurrences are counted by in_memory_unordered_results()"
            );
        }
        Ok(Self::InMemoryUnordered(Vec::new()))
    }

    pub(super) fn push(
        &mut self,
        row: RowVals,
        bindings: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        match self {
            Self::InMemoryUnordered(rows) => {
                rows.push(row);
                Ok(())
            }
            Self::Ordered(sink) => sink.push(row, bindings),
            Self::ArrivalOrder(sink) => sink.push(row, bindings),
        }
    }

    /// The result tail as a stream: sorted (if ordered), then DISTINCT, then
    /// LIMIT.
    pub(super) fn into_stream(self, return_clause: &ReturnClause) -> Result<RowStream<'static>> {
        let rows = match self {
            Self::InMemoryUnordered(rows) => super::stream::stream_from_rows(rows),
            Self::Ordered(sink) => sink.finish_stream()?,
            Self::ArrivalOrder(sink) => sink.finish_stream()?,
        };
        Ok(distinct_then_limit(rows, return_clause))
    }
}

/// Test doubles for the spill backend, shared by every executor test that has
/// to prove a sort actually went to disk.
#[cfg(test)]
pub(crate) mod test_support {
    use super::SpillReserver;

    /// Reserves subdirectories of one tempdir and records each reservation
    /// path, so tests can assert that a sort spilled, how many run files it
    /// wrote, and that the directory was removed afterwards.
    #[derive(Debug)]
    pub(crate) struct TempDirReserver {
        root: std::path::PathBuf,
        reserved: std::sync::Mutex<Vec<std::path::PathBuf>>,
    }

    impl TempDirReserver {
        pub(crate) fn new(root: &std::path::Path) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                root: root.to_path_buf(),
                reserved: std::sync::Mutex::new(Vec::new()),
            })
        }

        pub(crate) fn reserved_paths(&self) -> Vec<std::path::PathBuf> {
            self.reserved.lock().unwrap().clone()
        }

        /// Run files currently on disk across every reservation that still
        /// exists. Read while a sorted stream is alive, this is the evidence
        /// that the sort spilled and is being merged from disk on pull.
        pub(crate) fn live_run_files(&self) -> usize {
            self.reserved_paths()
                .iter()
                .filter(|dir| dir.exists())
                .map(|dir| {
                    std::fs::read_dir(dir)
                        .unwrap_or_else(|e| panic!("read spill dir {}: {e}", dir.display()))
                        .count()
                })
                .sum()
        }
    }

    impl SpillReserver for TempDirReserver {
        fn reserve(
            &self,
            label: &str,
        ) -> ferrosa_common::Result<ferrosa_storage::TempSortTableReservation> {
            let path = self.root.join(format!("{label}_{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path)
                .map_err(|e| ferrosa_common::Error::InvalidFormat(e.to_string()))?;
            self.reserved.lock().unwrap().push(path.clone());
            Ok(ferrosa_storage::TempSortTableReservation::claim_dir(path))
        }
    }

    /// `base` with a spill backend whose sorter spills on EVERY push
    /// (threshold 1 byte), so tests exercise the run-file + merge path rather
    /// than the in-memory fast path.
    pub(crate) fn spilling_config(
        reserver: std::sync::Arc<TempDirReserver>,
        base: super::super::expand::GraphEngineConfig,
    ) -> super::super::expand::GraphEngineConfig {
        super::super::expand::GraphEngineConfig {
            spill: Some(reserver),
            spill_threshold_bytes: 1,
            ..base
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::TempDirReserver;
    use super::*;
    use ferrosa_storage::external_sort::ExternalSorter;
    use futures::StreamExt as _;

    fn row(vals: &[serde_json::Value]) -> GraphRow {
        GraphRow(vals.to_vec())
    }

    fn asc(column: usize) -> GraphRowOrder {
        GraphRowOrder::new(vec![GraphOrderTerm {
            column,
            ascending: true,
        }])
    }

    fn spilling_config(
        reserver: std::sync::Arc<TempDirReserver>,
    ) -> super::super::expand::GraphEngineConfig {
        super::test_support::spilling_config(reserver, Default::default())
    }

    fn no_bindings() -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }

    fn order_by_n_return_clause() -> ReturnClause {
        ReturnClause {
            order_by: vec![crate::parser::OrderItem {
                expr: crate::parser::Expr::Var("n".to_string()),
                direction: crate::parser::SortDir::Asc,
            }],
            ..return_n_clause()
        }
    }

    fn return_n_clause() -> ReturnClause {
        ReturnClause {
            items: vec![crate::parser::ReturnItem {
                expr: crate::parser::Expr::Var("n".to_string()),
                alias: None,
            }],
            distinct: false,
            order_by: vec![],
            limit: None,
        }
    }

    async fn drain(stream: RowStream<'static>) -> Vec<RowVals> {
        crate::executor::stream::collect_rows(stream).await.unwrap()
    }

    /// `ORDER BY n LIMIT k` yields the k smallest, sorted, without the whole
    /// result being resident.
    ///
    /// A LIMIT used to disqualify the sink outright, so this shape took the
    /// in-memory path and materialised every candidate row to return a handful.
    /// The sort still has to see every row -- that is inherent -- but only the
    /// rows the caller takes are ever pulled out of the merge.
    #[tokio::test]
    async fn order_by_with_a_limit_streams_the_smallest_rows_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver.clone());
        let mut clause = order_by_n_return_clause();
        clause.limit = Some(3);

        let mut acc = RowAcc::new(
            &["n".to_string()],
            &clause,
            &config,
            "test",
            OrderSemantics::ProjectedColumns,
        )
        .unwrap();
        assert!(
            matches!(&acc, RowAcc::Ordered(sink) if matches!(**sink, OrderBySink::Spill(_))),
            "a LIMIT must not disqualify the spilling sort"
        );

        // Pushed out of order, and more rows than the limit.
        for n in [9i64, 2, 7, 1, 8, 3] {
            acc.push(vec![serde_json::json!(n)], &no_bindings())
                .unwrap();
        }

        let taken: Vec<i64> = drain(acc.into_stream(&clause).unwrap())
            .await
            .iter()
            .map(|row| row[0].as_i64().unwrap())
            .collect();
        assert_eq!(
            taken,
            vec![1, 2, 3],
            "the limit must be applied to the SORTED stream, so it returns the \
             smallest rows rather than the first three pushed"
        );
    }

    /// Without an ORDER BY, the accumulator used to collect every row in a
    /// `Vec` before streaming (PR #651 review: the largest remaining memory
    /// hole). With a spill backend it now spills in arrival order and the
    /// stream returns exactly the production order.
    #[tokio::test]
    async fn an_unordered_result_spills_and_streams_in_arrival_order() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver.clone());
        let clause = return_n_clause();

        let mut acc = RowAcc::new(
            &["n".to_string()],
            &clause,
            &config,
            "test",
            OrderSemantics::ProjectedColumns,
        )
        .unwrap();
        let produced = [9i64, 2, 7, 1, 8, 3];
        for n in produced {
            acc.push(vec![serde_json::json!(n)], &no_bindings())
                .unwrap();
        }
        let stream = acc.into_stream(&clause).unwrap();
        assert!(
            reserver.live_run_files() > 0,
            "an unordered result must spill to disk, not sit in a Vec"
        );
        let got: Vec<i64> = drain(stream)
            .await
            .iter()
            .map(|row| {
                assert_eq!(row.len(), 1, "the arrival key must be stripped");
                row[0].as_i64().unwrap()
            })
            .collect();
        assert_eq!(got, produced.to_vec(), "arrival order is preserved");
    }

    #[tokio::test]
    async fn finish_stream_yields_sorted_rows_then_cleans_up_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver.clone());
        let clause = order_by_n_return_clause();

        let mut sink = SpillSortSink::try_new(
            &["n".to_string()],
            &clause,
            &config,
            "test",
            OrderSemantics::ProjectedColumns,
        )
        .unwrap()
        .expect("sink must apply");
        for n in [3i64, 1, 2] {
            sink.push(vec![serde_json::json!(n)], &no_bindings())
                .unwrap();
        }

        let reserved = reserver.reserved_paths();
        assert_eq!(reserved.len(), 1);
        let mut stream = sink.finish_stream().unwrap();
        assert!(
            reserved[0].exists(),
            "spilled runs must survive until the stream is dropped"
        );

        let mut got = Vec::new();
        while let Some(row) = stream.next().await {
            got.push(row.unwrap()[0].as_i64().unwrap());
        }
        assert_eq!(got, vec![1, 2, 3]);

        drop(stream);
        assert!(
            !reserved[0].exists(),
            "dropping the stream must remove the temp-sort directory"
        );
    }

    /// The sink refuses only two shapes: no ORDER BY, and no backend. A
    /// non-projected order term used to be a third, which sent the whole
    /// result to an in-memory sort; it is now carried as a hidden key.
    #[test]
    fn try_new_refuses_only_a_missing_order_by_or_backend() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver);
        let columns = vec!["n".to_string()];
        let semantics = OrderSemantics::ProjectedColumns;

        // No ORDER BY.
        assert!(
            SpillSortSink::try_new(&columns, &return_n_clause(), &config, "t", semantics)
                .unwrap()
                .is_none()
        );
        // LIMIT present: the sink still applies. `ORDER BY x LIMIT 10` is the
        // shape that most needs it -- the sort is over the whole result no
        // matter how small the limit.
        let mut with_limit = order_by_n_return_clause();
        with_limit.limit = Some(10);
        assert!(
            SpillSortSink::try_new(&columns, &with_limit, &config, "t", semantics)
                .unwrap()
                .is_some(),
            "a LIMIT bounds the OUTPUT, not the sort"
        );
        // Order term not projected: still spills, via a hidden key.
        assert!(
            SpillSortSink::try_new(
                &["other".to_string()],
                &order_by_n_return_clause(),
                &config,
                "t",
                semantics
            )
            .unwrap()
            .is_some(),
            "a non-projected order term must not force an in-memory sort"
        );
        // No backend.
        let no_backend = super::super::expand::GraphEngineConfig::default();
        assert!(SpillSortSink::try_new(
            &columns,
            &order_by_n_return_clause(),
            &no_backend,
            "t",
            semantics
        )
        .unwrap()
        .is_none());
    }

    /// With no backend the sink falls back to the in-memory sort — and that
    /// is counted, so it cannot happen unnoticed. With a backend it is never
    /// chosen.
    #[tokio::test]
    async fn the_in_memory_fallback_is_counted_and_only_used_without_a_backend() {
        let clause = order_by_n_return_clause();
        let columns = vec!["n".to_string()];

        let dir = tempfile::tempdir().unwrap();
        let with_backend = spilling_config(TempDirReserver::new(dir.path()));
        let sink = OrderBySink::new(
            &columns,
            &clause,
            &with_backend,
            "t",
            OrderSemantics::Bindings,
        )
        .unwrap()
        .expect("ORDER BY present");
        assert!(
            matches!(sink, OrderBySink::Spill(_)),
            "a configured backend must always be used"
        );

        let before = in_memory_order_by_sorts();
        let no_backend = super::super::expand::GraphEngineConfig::default();
        let mut sink = OrderBySink::new(
            &columns,
            &clause,
            &no_backend,
            "t",
            OrderSemantics::Bindings,
        )
        .unwrap()
        .expect("ORDER BY present");
        assert!(matches!(sink, OrderBySink::InMemory(_)));
        assert!(
            in_memory_order_by_sorts() > before,
            "the in-memory fallback must be counted"
        );
        for n in [2i64, 3, 1] {
            sink.push(vec![serde_json::json!(n)], &no_bindings())
                .unwrap();
        }
        assert_eq!(
            drain(sink.finish_stream().unwrap()).await,
            vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)]
            ]
        );
    }

    /// The hidden key is evaluated from the BINDINGS, travels through a real
    /// spill + merge, decides the order, and is stripped before the row is
    /// seen — for both semantics.
    #[tokio::test]
    async fn non_projected_keys_order_spilled_rows_and_are_stripped() {
        let clause = ReturnClause {
            items: vec![crate::parser::ReturnItem {
                expr: crate::parser::Expr::Property {
                    var: "n".to_string(),
                    name: "name".to_string(),
                },
                alias: None,
            }],
            distinct: false,
            order_by: vec![crate::parser::OrderItem {
                expr: crate::parser::Expr::Property {
                    var: "n".to_string(),
                    name: "age".to_string(),
                },
                direction: crate::parser::SortDir::Desc,
            }],
            limit: None,
        };
        let columns = vec!["n.name".to_string()];
        for semantics in [OrderSemantics::Bindings, OrderSemantics::ProjectedColumns] {
            let dir = tempfile::tempdir().unwrap();
            let reserver = TempDirReserver::new(dir.path());
            let config = spilling_config(reserver.clone());
            let mut sink = SpillSortSink::try_new(&columns, &clause, &config, "t", semantics)
                .unwrap()
                .expect("must spill");
            for (name, age) in [("b", 20), ("a", 40), ("c", 10)] {
                let bindings = HashMap::from([(
                    "n".to_string(),
                    serde_json::json!({"name": name, "age": age}),
                )]);
                sink.push(vec![serde_json::json!(name)], &bindings).unwrap();
            }
            assert!(sink.sorter.spilled(), "threshold 1 must force the spill");
            let rows = drain(sink.finish_stream().unwrap()).await;
            assert_eq!(
                rows,
                vec![
                    vec![serde_json::json!("a")],
                    vec![serde_json::json!("b")],
                    vec![serde_json::json!("c")]
                ],
                "{semantics:?}: ordered by the hidden age DESC, key stripped"
            );
        }
    }

    /// DISTINCT runs on the visible row, after the keys are stripped: two rows
    /// that project identically but sort by different keys are duplicates.
    #[tokio::test]
    async fn distinct_dedups_on_the_visible_row_not_the_hidden_keys() {
        let dir = tempfile::tempdir().unwrap();
        let config = spilling_config(TempDirReserver::new(dir.path()));
        let clause = ReturnClause {
            distinct: true,
            limit: Some(2),
            order_by: vec![crate::parser::OrderItem {
                expr: crate::parser::Expr::Var("k".to_string()),
                direction: crate::parser::SortDir::Asc,
            }],
            ..return_n_clause()
        };
        let mut acc = RowAcc::new(
            &["n".to_string()],
            &clause,
            &config,
            "t",
            OrderSemantics::Bindings,
        )
        .unwrap();
        for (n, k) in [("x", 3), ("y", 1), ("x", 2), ("z", 4)] {
            let bindings = HashMap::from([("k".to_string(), serde_json::json!(k))]);
            acc.push(vec![serde_json::json!(n)], &bindings).unwrap();
        }
        assert_eq!(
            drain(acc.into_stream(&clause).unwrap()).await,
            vec![vec![serde_json::json!("y")], vec![serde_json::json!("x")]],
            "sort by k (y1, x2, x3, z4) -> dedup (y, x, z) -> limit 2"
        );
    }

    #[test]
    fn detach_fails_loud_on_a_row_of_the_wrong_width() {
        let keys = OrderKeys::new(
            &["n".to_string()],
            &order_by_n_return_clause(),
            OrderSemantics::Bindings,
        );
        let err = keys.detach(vec![serde_json::json!(1)]).unwrap_err();
        assert!(matches!(err, GraphError::Internal(_)), "{err:?}");
    }

    /// The load-bearing property for `ProjectedColumns`: this comparator must
    /// order rows EXACTLY as the in-memory `sort_rows` does.
    #[test]
    fn matches_in_memory_sort_rows_ordering() {
        let mut rows: Vec<RowVals> = vec![
            vec![serde_json::json!(3)],
            vec![serde_json::json!(1)],
            vec![serde_json::Value::Null],
            vec![serde_json::json!(2)],
        ];
        let columns = vec!["n".to_string()];
        let order_by = vec![crate::parser::OrderItem {
            expr: crate::parser::Expr::Var("n".to_string()),
            direction: crate::parser::SortDir::Asc,
        }];

        let mut reference = rows.clone();
        super::super::expand::sort_rows(&mut reference, &columns, &order_by);

        rows.sort_by(|a, b| asc(0).compare_rows(a, b));

        assert_eq!(
            rows, reference,
            "spill comparator must match sort_rows exactly (incl. Null-sorts-first)"
        );
    }

    /// Graph semantics `CqlValue::Ord` does not share: mixed types compare Equal.
    /// If this ever changed, bridging to the CQL comparator would look safe and
    /// silently reorder results — which is why the sorter takes OUR comparator.
    #[test]
    fn mixed_types_compare_equal_like_the_in_memory_comparator() {
        let a = vec![serde_json::json!("text")];
        let b = vec![serde_json::json!(42)];
        assert_eq!(asc(0).compare_rows(&a, &b), Ordering::Equal);
    }

    #[test]
    fn descending_reverses_and_multi_term_breaks_ties() {
        let order = GraphRowOrder::new(vec![
            GraphOrderTerm {
                column: 0,
                ascending: true,
            },
            GraphOrderTerm {
                column: 1,
                ascending: false,
            },
        ]);
        let a = vec![serde_json::json!(1), serde_json::json!(5)];
        let b = vec![serde_json::json!(1), serde_json::json!(9)];
        // First term ties; second (descending) puts the LARGER value first.
        assert_eq!(order.compare_rows(&a, &b), Ordering::Greater);
    }

    /// End-to-end through the real sorter, forced down the spill+merge path
    /// (threshold 1 byte) rather than the in-memory fast path.
    #[test]
    fn spills_and_merges_graph_rows_in_graph_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut sorter: ExternalSorter<GraphRow, GraphRowOrder> =
            ExternalSorter::new(dir.path(), asc(0), 1);
        for n in [5i64, 1, 4, 2, 3] {
            sorter.push(row(&[serde_json::json!(n)])).unwrap();
        }
        assert!(sorter.spilled(), "threshold 1 must force the spill path");

        let sorted: Vec<GraphRow> = sorter
            .finish()
            .unwrap()
            .collect::<ferrosa_common::Result<_>>()
            .unwrap();
        let got: Vec<i64> = sorted.iter().map(|r| r.0[0].as_i64().unwrap()).collect();
        assert_eq!(got, vec![1, 2, 3, 4, 5]);
    }
}
