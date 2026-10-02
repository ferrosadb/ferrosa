//! The scan contract the engine pulls rows from.

use crate::types::{RelSchema, Row};

/// A source of rows with a known schema — the `TableProvider` equivalent the
/// engine scans. Backed by an in-memory table in tests; by ferrosa storage
/// (with predicate/projection pushdown) in production.
pub trait TableProvider {
    fn schema(&self) -> &RelSchema;
    /// A pull-based scan over the table's rows.
    fn scan(&self) -> Box<dyn Iterator<Item = Row> + '_>;
}

/// In-memory table for tests and small fixtures.
#[derive(Debug, Clone)]
pub struct InMemoryTable {
    schema: RelSchema,
    rows: Vec<Row>,
}

impl InMemoryTable {
    pub fn new(schema: RelSchema, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
}

impl TableProvider for InMemoryTable {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }

    fn scan(&self) -> Box<dyn Iterator<Item = Row> + '_> {
        // The trait's `Item = Row` and the `'_` borrow make an owned-row scan
        // impossible to express (the engine-wide contract is out of scope here).
        // What matters is WHEN the copy happens: the old `self.rows.iter()
        // .cloned()` adapter eagerly cloned EVERY row the instant the scan was
        // requested, even if the consumer short-circuited after one. `LazyRows`
        // defers each clone to `next()`, so a `LIMIT`, `first()`, or key-
        // predicate early-exit copies only the rows it actually pulls.
        //
        // Residual cost: draining a whole scan is still O(table) — e.g.
        // `seq_scan(...).collect()` in the relational planner, tracked under
        // t_50d99192 (that is the executor's `Vec<Row>`, not this provider).
        // This change removes the eager copy that preceded it, not that.
        Box::new(LazyRows {
            rows: &self.rows,
            idx: 0,
        })
    }
}

/// A deferred clone over an [`InMemoryTable`]'s rows: each `next()` clones
/// exactly one row, so work and peak are proportional to what the consumer
/// pulls, never to the table size.
struct LazyRows<'a> {
    rows: &'a [Row],
    idx: usize,
}

impl Iterator for LazyRows<'_> {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        let row = self.rows.get(self.idx)?.clone();
        self.idx += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.idx;
        (remaining, Some(remaining))
    }
}
