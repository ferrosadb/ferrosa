//! Module: Spill adapter — lets graph result rows use the storage engine's
//! bounded-memory external merge sort.
//! Correctness: Correct when [`GraphRowOrder`] orders rows EXACTLY as the
//!   in-memory [`super::expand::sort_rows`] does, so switching ORDER BY to the
//!   spilling sorter changes memory behavior and nothing else.
//! Last revised: 2026-07-25
//! Last changed: New module — increment 5 of the streaming executor
//!   (t_4ce82a3e). Adapter only; the executor is not wired to it yet.
//!
//! # Why
//!
//! `ORDER BY` without `LIMIT` is a pipeline-breaker: every row must be seen
//! before the first output row is known. The graph executor currently buffers
//! the whole result and silently truncates at `max_result_rows` — a client
//! cannot distinguish a partial result from a complete one.
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
//! [`super::expand::compare_json_values`] has semantics `CqlValue`'s `Ord` does
//! not share — mixed types (String vs Number) and complex values (objects,
//! arrays) compare **Equal**, and `Null` sorts before any present value.
//! Converting graph rows to `CqlValue` to reuse the CQL comparator would
//! silently reorder those cases; this adapter preserves them exactly.

use std::cmp::Ordering;

use ferrosa_storage::external_sort::{SpillOrder, SpillRow};

use super::stream::RowVals;

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
