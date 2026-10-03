//! The scan contract the engine pulls rows from.

use std::sync::Arc;

use crate::types::{RelSchema, Row};

/// A source of rows with a known schema — the `TableProvider` equivalent the
/// engine scans. Backed by an in-memory table in tests; by ferrosa storage
/// (with predicate/projection pushdown) in production.
pub trait TableProvider {
    fn schema(&self) -> &RelSchema;
    /// A pull-based scan over the table's rows.
    ///
    /// The iterator owns what it reads and is `Send`: a query suspended between
    /// pulls (a PostgreSQL portal stopped by `max_rows`) keeps its scans
    /// without keeping a thread, and resumes them on whichever thread is free.
    fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send>;
}

/// In-memory table for tests and small fixtures.
#[derive(Debug, Clone)]
pub struct InMemoryTable {
    schema: RelSchema,
    /// The rows, behind a shared handle so a scan can own them without
    /// copying any.
    storage: Arc<[Row]>,
}

impl InMemoryTable {
    pub fn new(schema: RelSchema, rows: Vec<Row>) -> Self {
        Self {
            schema,
            storage: rows.into(),
        }
    }

    pub fn rows(&self) -> &[Row] {
        &self.storage
    }

    /// Another handle to the same rows: a reference-count increment, no row
    /// is copied.
    fn share(&self) -> Arc<[Row]> {
        Arc::clone(&self.storage)
    }
}

impl TableProvider for InMemoryTable {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }

    fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send> {
        // What matters is WHEN the copy happens: the old `self.rows.iter()
        // .cloned()` adapter eagerly cloned EVERY row the instant the scan was
        // requested, even if the consumer short-circuited after one. `LazyRows`
        // defers each clone to `next()`, so a `LIMIT`, `first()`, or key-
        // predicate early-exit copies only the rows it actually pulls. It shares
        // the rows through an `Arc` rather than borrowing them, so the scan
        // owns what it reads.
        //
        // Residual cost: draining a whole scan is still O(table) — e.g.
        // `seq_scan(...).collect()` in the relational planner, tracked under
        // t_50d99192 (that is the executor's `Vec<Row>`, not this provider).
        // This change removes the eager copy that preceded it, not that.
        Box::new(LazyRows {
            rows: self.share(),
            idx: 0,
        })
    }
}

/// A deferred clone over an [`InMemoryTable`]'s rows: each `next()` clones
/// exactly one row, so work and peak are proportional to what the consumer
/// pulls, never to the table size.
struct LazyRows {
    rows: Arc<[Row]>,
    idx: usize,
}

impl Iterator for LazyRows {
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
