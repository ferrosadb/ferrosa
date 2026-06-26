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
        Box::new(self.rows.iter().cloned())
    }
}
