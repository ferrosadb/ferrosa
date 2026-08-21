//! Module: Spill adapter — lets graph result rows use the storage engine's
//! bounded-memory external merge sort.
//! Correctness: Correct when [`GraphRowOrder`] orders rows EXACTLY as the
//!   in-memory `sort_rows` does, so switching ORDER BY to the
//!   spilling sorter changes memory behavior and nothing else.
//! Last revised: 2026-07-25
//! Last changed: Wired into the executor — `execute_expand`'s unbounded ORDER BY
//!   now sorts through `spill_order_by`, and the `max_result_rows` truncation
//!   it replaced is gone (increment 5 of the streaming executor, t_4ce82a3e).
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
//! # Why a graph-specific comparator
//!
//! The sorter is generic over `SpillOrder<T>` precisely so callers keep their
//! own ordering. Graph rows are `Vec<serde_json::Value>` and
//! `compare_json_values` has semantics `CqlValue`'s `Ord` does
//! not share — mixed types (String vs Number) and complex values (objects,
//! arrays) compare **Equal**, and `Null` sorts before any present value.
//! Converting graph rows to `CqlValue` to reuse the CQL comparator would
//! silently reorder those cases; this adapter preserves them exactly.

use std::cmp::Ordering;

use ferrosa_storage::external_sort::{SpillOrder, SpillRow};

use super::stream::RowVals;

/// Reserves a cancellable temp directory for a spilling ORDER BY sort.
///
/// Hung off `GraphEngineConfig` — which the executor already
/// threads everywhere — rather than plumbing a `StorageEngine` through every
/// recursive `execute` call. `None` means no spill backend is available (unit
/// tests, embedded use); ORDER BY then sorts in memory exactly as before.
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
    /// Approximate footprint for spill-threshold accounting. Need not be exact —
    /// only monotonic in real memory use, so the buffer spills before the
    /// process is starved. Strings and arrays/objects carry their payload.
    fn estimated_bytes(&self) -> usize {
        self.0.iter().map(json_value_bytes).sum::<usize>()
            + self.0.len() * std::mem::size_of::<serde_json::Value>()
    }
}

/// Payload bytes of a JSON value beyond its fixed enum size.
fn json_value_bytes(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(items) => {
            items.iter().map(json_value_bytes).sum::<usize>()
                + items.len() * std::mem::size_of::<serde_json::Value>()
        }
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, val)| k.len() + json_value_bytes(val))
            .sum::<usize>(),
        _ => 0,
    }
}

/// One resolved ORDER BY term: the projected column index, and whether ascending.
///
/// Resolved to an INDEX up front (rather than carrying the expression) because
/// the sorter compares rows in isolation — including rows read back from a
/// spilled run, where no binding context exists. `sort_rows` resolves the same
/// way, skipping order terms whose column is not projected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphOrderTerm {
    pub column: usize,
    pub ascending: bool,
}

/// Graph ORDER BY comparator for the external sorter.
///
/// Applies terms left-to-right; the first unequal column decides. Mirrors
/// `sort_rows` exactly, including its use of `compare_json_values`.
#[derive(Debug, Clone)]
pub struct GraphRowOrder {
    terms: Vec<GraphOrderTerm>,
}

impl GraphRowOrder {
    pub fn new(terms: Vec<GraphOrderTerm>) -> Self {
        Self { terms }
    }

    /// Compare two rows under this order.
    pub fn compare_rows(&self, a: &RowVals, b: &RowVals) -> Ordering {
        for term in &self.terms {
            let cmp = super::expand::compare_json_values(a.get(term.column), b.get(term.column));
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

/// An incremental, spill-backed ORDER BY accumulator.
///
/// The original `spill_order_by` took a fully-materialized `Vec` of rows and
/// drained it into the external sorter — so the complete result still had to
/// fit in memory once before any spilling happened (owner finding on #308,
/// t_3f2f961a). This sink is the same sorter behind a push-per-row surface:
/// producers hand each row over AS IT IS PRODUCED, and memory is bounded by
/// `spill_threshold_bytes` regardless of result size.
///
/// Eligibility ([`SpillSortSink::try_new`] returns `Ok(None)`) matches the old
/// `spill_order_by` exactly:
/// - no spill backend configured (unit tests, embedded use);
/// - no ORDER BY, or a LIMIT is present (the bounded case does not need spill);
/// - an ORDER BY term is not a projected column, which the in-memory
///   `sort_projected_rows_by_bindings` handles via the states' bindings and this
///   path cannot (a spilled row carries no binding context).
///
/// Fails loud on any spill/merge I/O error — a dropped run would silently lose
/// rows, which is strictly worse than the old cap.
pub(super) struct SpillSortSink {
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
    /// Build a sink when the spilling ORDER BY sort can apply; `Ok(None`)
    /// otherwise (caller keeps its in-memory path, exactly as before).
    pub(super) fn try_new(
        columns: &[String],
        return_clause: &crate::parser::ReturnClause,
        config: &super::expand::GraphEngineConfig,
        label: &str,
    ) -> crate::error::Result<Option<Self>> {
        let Some(reserver) = config.spill.as_ref() else {
            return Ok(None);
        };
        if return_clause.order_by.is_empty() {
            return Ok(None);
        }
        // A LIMIT deliberately does NOT disqualify the sink.
        //
        // It used to: `|| return_clause.limit.is_some()` sent every
        // `ORDER BY ... LIMIT n` to the in-memory path. That is backwards. The
        // sort has to consider every candidate row whatever the limit is, so
        // the limit shrinks the OUTPUT, not the work — and bailing meant a
        // ten-row answer first materialised every candidate row, which is the
        // shape most likely to be large.
        //
        // The caller takes `limit` from the sorted stream, which yields the
        // same rows in the same order while the sink keeps the sort bounded.
        // Resolve every order term to a projected column index; bail to the
        // in-memory path if any term is not projected.
        let mut terms = Vec::with_capacity(return_clause.order_by.len());
        for item in &return_clause.order_by {
            let name = super::expand::expr_to_column_name(&item.expr);
            let Some(column) = columns.iter().position(|c| c == &name) else {
                return Ok(None);
            };
            terms.push(GraphOrderTerm {
                column,
                ascending: matches!(item.direction, crate::parser::SortDir::Asc),
            });
        }

        let reservation = reserver.reserve(label).map_err(|e| {
            crate::error::GraphError::Internal(format!("ORDER BY temp-sort setup failed: {e}"))
        })?;
        let sorter = ferrosa_storage::external_sort::ExternalSorter::new(
            reservation.path(),
            GraphRowOrder::new(terms),
            config.spill_threshold_bytes,
        );
        Ok(Some(Self {
            reservation,
            sorter,
            rows_pushed: 0,
        }))
    }

    /// Accept one produced row. Spills a sorted run when the buffer crosses
    /// the byte threshold; fails loud on I/O error.
    pub(super) fn push(&mut self, row: RowVals) -> crate::error::Result<()> {
        self.rows_pushed += 1;
        self.sorter
            .push(GraphRow(row))
            .map_err(|e| crate::error::GraphError::Internal(format!("ORDER BY spill: {e}")))
    }

    /// Finish the sort and materialize the merged output into a `Vec`, for
    /// callers bound to the buffered `GraphResult` contract.
    pub(super) fn finish_rows(self) -> crate::error::Result<Vec<RowVals>> {
        let rows_pushed = self.rows_pushed;
        let spilled_to_disk = self.sorter.spilled();
        let temp_dir = self.reservation.path().display().to_string();
        let sorted = self.sorter.finish().map_err(|e| {
            crate::error::GraphError::Internal(format!("ORDER BY spill finish: {e}"))
        })?;
        let mut rows = Vec::with_capacity(rows_pushed);
        for row in sorted {
            rows.push(
                row.map_err(|e| {
                    crate::error::GraphError::Internal(format!("ORDER BY merge: {e}"))
                })?
                .0,
            );
        }
        if spilled_to_disk {
            tracing::info!(
                rows = rows.len(),
                temp_sort_table = %temp_dir,
                "graph ORDER BY spilled to a cancellable temp-sort table"
            );
        }
        // `self.reservation` drops here — its Drop removes the temp directory,
        // so a cancelled or failed query cleans up exactly like a successful one.
        Ok(rows)
    }

    /// Finish the sort and stream the merged output WITHOUT re-materializing
    /// it: each pull reads the next row from the merge. The temp-dir
    /// reservation is moved into the stream, so the spilled runs live exactly
    /// as long as a consumer can still pull from them and are removed when the
    /// stream drops — cancelled, exhausted, or abandoned alike.
    pub(super) fn finish_stream(self) -> crate::error::Result<super::stream::RowStream<'static>> {
        let spilled_to_disk = self.sorter.spilled();
        let rows_pushed = self.rows_pushed;
        let temp_dir = self.reservation.path().display().to_string();
        let sorted = self.sorter.finish().map_err(|e| {
            crate::error::GraphError::Internal(format!("ORDER BY spill finish: {e}"))
        })?;
        if spilled_to_disk {
            tracing::info!(
                rows = rows_pushed,
                temp_sort_table = %temp_dir,
                "graph ORDER BY streaming from a cancellable temp-sort table"
            );
        }
        let reservation = self.reservation;
        let iter = sorted.map(move |row| {
            // The closure owns `reservation`; referencing it keeps the temp
            // dir alive until the stream itself is dropped.
            let _keep_alive = &reservation;
            row.map(|r| r.0)
                .map_err(|e| crate::error::GraphError::Internal(format!("ORDER BY merge: {e}")))
        });
        Ok(Box::pin(futures::stream::iter(iter)))
    }
}

/// Sort `rows` in place through the storage engine's bounded external merge
/// sort, spilling to a cancellable temp directory instead of holding the whole
/// sorted set in memory (t_4ce82a3e).
///
/// Returns `Ok(false)` — leaving `rows` untouched for the in-memory sort — when
/// the sink cannot apply (see [`SpillSortSink::try_new`]). Retained for callers
/// that already hold a materialized `Vec`; producers that can hand rows over
/// one at a time should push into a [`SpillSortSink`] instead and never build
/// the interim `Vec` at all (t_3f2f961a).
pub(super) async fn spill_order_by(
    rows: &mut Vec<Vec<serde_json::Value>>,
    columns: &[String],
    return_clause: &crate::parser::ReturnClause,
    config: &super::expand::GraphEngineConfig,
) -> crate::error::Result<bool> {
    let Some(mut sink) = SpillSortSink::try_new(columns, return_clause, config, "graph_order_by")?
    else {
        return Ok(false);
    };
    for row in rows.drain(..) {
        sink.push(row)?;
    }
    *rows = sink.finish_rows()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::external_sort::ExternalSorter;

    fn row(vals: &[serde_json::Value]) -> GraphRow {
        GraphRow(vals.to_vec())
    }

    fn asc(column: usize) -> GraphRowOrder {
        GraphRowOrder::new(vec![GraphOrderTerm {
            column,
            ascending: true,
        }])
    }

    /// Test double for the spill backend: reserves subdirectories of one
    /// tempdir and records each reservation path so tests can assert cleanup.
    #[derive(Debug)]
    struct TempDirReserver {
        root: std::path::PathBuf,
        reserved: std::sync::Mutex<Vec<std::path::PathBuf>>,
    }

    impl TempDirReserver {
        fn new(root: &std::path::Path) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                root: root.to_path_buf(),
                reserved: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn reserved_paths(&self) -> Vec<std::path::PathBuf> {
            self.reserved.lock().unwrap().clone()
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

    /// A config whose sink ALWAYS spills (threshold 1 byte), so tests exercise
    /// the run-file + merge path rather than the in-memory fast path.
    fn spilling_config(
        reserver: std::sync::Arc<TempDirReserver>,
    ) -> super::super::expand::GraphEngineConfig {
        super::super::expand::GraphEngineConfig {
            spill: Some(reserver),
            spill_threshold_bytes: 1,
            ..Default::default()
        }
    }

    fn order_by_n_return_clause() -> crate::parser::ReturnClause {
        crate::parser::ReturnClause {
            order_by: vec![crate::parser::OrderItem {
                expr: crate::parser::Expr::Var("n".to_string()),
                direction: crate::parser::SortDir::Asc,
            }],
            ..return_n_clause()
        }
    }

    fn return_n_clause() -> crate::parser::ReturnClause {
        crate::parser::ReturnClause {
            items: vec![crate::parser::ReturnItem {
                expr: crate::parser::Expr::Var("n".to_string()),
                alias: None,
            }],
            distinct: false,
            order_by: vec![],
            limit: None,
        }
    }

    /// The sink must produce exactly what the Vec-based `spill_order_by`
    /// produces — same rows, same order — because callers were converted from
    /// one to the other with no intended behavior change (t_3f2f961a).
    #[tokio::test]
    async fn sink_matches_the_vec_based_spill_order_by() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver);
        let columns = vec!["n".to_string()];
        let clause = order_by_n_return_clause();
        let source: Vec<RowVals> = [5i64, 1, 4, 2, 3]
            .iter()
            .map(|n| vec![serde_json::json!(n)])
            .collect();

        let mut vec_path = source.clone();
        assert!(
            spill_order_by(&mut vec_path, &columns, &clause, &config)
                .await
                .unwrap(),
            "spill path must apply"
        );

        let mut sink = SpillSortSink::try_new(&columns, &clause, &config, "test")
            .unwrap()
            .expect("sink must apply under the same conditions");
        for row in source {
            sink.push(row).unwrap();
        }
        assert_eq!(sink.finish_rows().unwrap(), vec_path);
    }

    /// `finish_stream` yields the merged rows in sorted order WITHOUT
    /// re-materializing, and the temp dir lives exactly as long as the stream.
    /// `ORDER BY n LIMIT k` yields the k smallest, sorted, without the whole
    /// result being resident.
    ///
    /// A LIMIT used to disqualify the sink outright, so this shape took the
    /// in-memory path and materialised every candidate row to return a handful.
    /// The sort still has to see every row -- that is inherent -- but only the
    /// rows the caller takes are ever pulled out of the merge.
    #[tokio::test]
    async fn order_by_with_a_limit_streams_the_smallest_rows_in_order() {
        use futures::StreamExt as _;

        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver.clone());
        let mut clause = order_by_n_return_clause();
        clause.limit = Some(3);

        let mut sink = SpillSortSink::try_new(&["n".to_string()], &clause, &config, "test")
            .unwrap()
            .expect("a LIMIT must not disqualify the spilling sort");

        // Pushed out of order, and more rows than the limit.
        for n in [9i64, 2, 7, 1, 8, 3] {
            sink.push(vec![serde_json::json!(n)]).unwrap();
        }

        let sorted = sink.finish_stream().unwrap();
        let taken: Vec<i64> = Box::pin(sorted.take(3))
            .map(|row| row.unwrap()[0].as_i64().unwrap())
            .collect()
            .await;

        assert_eq!(
            taken,
            vec![1, 2, 3],
            "the limit must be applied to the SORTED stream, so it returns the \
             smallest rows rather than the first three pushed"
        );
    }

    #[tokio::test]
    async fn finish_stream_yields_sorted_rows_then_cleans_up_on_drop() {
        use futures::StreamExt as _;

        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver.clone());
        let clause = order_by_n_return_clause();

        let mut sink = SpillSortSink::try_new(&["n".to_string()], &clause, &config, "test")
            .unwrap()
            .expect("sink must apply");
        for n in [3i64, 1, 2] {
            sink.push(vec![serde_json::json!(n)]).unwrap();
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

    /// Eligibility must match the old `spill_order_by` gates exactly.
    #[test]
    fn try_new_refuses_the_ineligible_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let reserver = TempDirReserver::new(dir.path());
        let config = spilling_config(reserver);
        let columns = vec!["n".to_string()];

        // No ORDER BY.
        assert!(
            SpillSortSink::try_new(&columns, &return_n_clause(), &config, "t")
                .unwrap()
                .is_none()
        );
        // LIMIT present: the sink still applies. `ORDER BY x LIMIT 10` is the
        // shape that most needs it -- the sort is over the whole result no
        // matter how small the limit, so bailing to the in-memory path made a
        // ten-row answer materialise every candidate row first. The caller
        // takes the limit from the sorted stream.
        let mut with_limit = order_by_n_return_clause();
        with_limit.limit = Some(10);
        assert!(
            SpillSortSink::try_new(&columns, &with_limit, &config, "t")
                .unwrap()
                .is_some(),
            "a LIMIT bounds the OUTPUT, not the sort; it must not force the \
             whole result set to be sorted in memory"
        );
        // Order term not projected (needs binding context downstream).
        assert!(SpillSortSink::try_new(
            &["other".to_string()],
            &order_by_n_return_clause(),
            &config,
            "t"
        )
        .unwrap()
        .is_none());
        // No backend.
        let no_backend = super::super::expand::GraphEngineConfig::default();
        assert!(
            SpillSortSink::try_new(&columns, &order_by_n_return_clause(), &no_backend, "t")
                .unwrap()
                .is_none()
        );
    }

    /// The load-bearing property: this comparator must order rows EXACTLY as the
    /// in-memory `sort_rows` does, so moving ORDER BY onto the spilling sorter
    /// changes memory behavior and nothing observable.
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
