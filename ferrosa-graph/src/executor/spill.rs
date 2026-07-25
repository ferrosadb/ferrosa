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

/// Sort `rows` in place through the storage engine's bounded external merge
/// sort, spilling to a cancellable temp directory instead of holding the whole
/// sorted set in memory (t_4ce82a3e).
///
/// Returns `Ok(false)` — leaving `rows` untouched for the in-memory sort — when
/// this cannot apply:
/// - no spill backend configured (unit tests, embedded use);
/// - no ORDER BY, or a LIMIT is present (the bounded case does not need spill);
/// - an ORDER BY term is not a projected column, which the in-memory
///   `sort_projected_rows_by_bindings` handles via the states' bindings and this
///   path cannot (a spilled row carries no binding context).
///
/// Fails loud on any spill/merge I/O error — a dropped run would silently lose
/// rows, which is strictly worse than the old cap.
pub(super) async fn spill_order_by(
    rows: &mut Vec<Vec<serde_json::Value>>,
    columns: &[String],
    return_clause: &crate::parser::ReturnClause,
    config: &super::expand::GraphEngineConfig,
) -> crate::error::Result<bool> {
    let Some(reserver) = config.spill.as_ref() else {
        return Ok(false);
    };
    if return_clause.order_by.is_empty() || return_clause.limit.is_some() {
        return Ok(false);
    }
    // Resolve every order term to a projected column index; bail to the
    // in-memory path if any term is not projected.
    let mut terms = Vec::with_capacity(return_clause.order_by.len());
    for item in &return_clause.order_by {
        let name = super::expand::expr_to_column_name(&item.expr);
        let Some(column) = columns.iter().position(|c| c == &name) else {
            return Ok(false);
        };
        terms.push(GraphOrderTerm {
            column,
            ascending: matches!(item.direction, crate::parser::SortDir::Asc),
        });
    }

    let reservation = reserver.reserve("graph_order_by").map_err(|e| {
        crate::error::GraphError::Internal(format!("ORDER BY temp-sort setup failed: {e}"))
    })?;
    let mut sorter: ferrosa_storage::external_sort::ExternalSorter<GraphRow, GraphRowOrder> =
        ferrosa_storage::external_sort::ExternalSorter::new(
            reservation.path(),
            GraphRowOrder::new(terms),
            config.spill_threshold_bytes,
        );
    for row in rows.drain(..) {
        sorter
            .push(GraphRow(row))
            .map_err(|e| crate::error::GraphError::Internal(format!("ORDER BY spill: {e}")))?;
    }
    let spilled_to_disk = sorter.spilled();
    let sorted = sorter
        .finish()
        .map_err(|e| crate::error::GraphError::Internal(format!("ORDER BY spill finish: {e}")))?;
    for row in sorted {
        rows.push(
            row.map_err(|e| crate::error::GraphError::Internal(format!("ORDER BY merge: {e}")))?
                .0,
        );
    }
    if spilled_to_disk {
        tracing::info!(
            rows = rows.len(),
            temp_sort_table = %reservation.path().display(),
            "graph ORDER BY spilled to a cancellable temp-sort table"
        );
    }
    // `reservation` drops here — its Drop removes the temp directory, so a
    // cancelled or failed query cleans up exactly like a successful one.
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
