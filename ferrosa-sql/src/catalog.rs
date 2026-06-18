//! Name resolution: a qualified `(schema, table)` name → a scannable provider.
//!
//! The binder uses a [`Catalog`] to turn table references in a query into scan
//! operators. Returning `None` is the binder's signal to **fail loud** ("no such
//! table") rather than scan an empty relation — the storage-backed catalog must
//! never paper over a missing table with an empty stream (see risk R15).

use std::collections::HashMap;
use std::sync::Arc;

use crate::provider::TableProvider;

/// A shareable, scannable table source.
pub type SharedTable = Arc<dyn TableProvider + Send + Sync>;

/// Resolves a qualified table name to a provider the engine can scan.
pub trait Catalog {
    /// Resolve `schema.table`. `None` means the table does not exist — the
    /// caller must error, not substitute an empty relation.
    fn resolve(&self, schema: &str, table: &str) -> Option<SharedTable>;
}

/// In-memory catalog for tests and fixtures.
#[derive(Default)]
pub struct MapCatalog {
    tables: HashMap<(String, String), SharedTable>,
}

impl MapCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style registration of a table under `schema.table`.
    pub fn with_table(
        mut self,
        schema: impl Into<String>,
        table: impl Into<String>,
        provider: SharedTable,
    ) -> Self {
        self.tables.insert((schema.into(), table.into()), provider);
        self
    }
}

impl Catalog for MapCatalog {
    fn resolve(&self, schema: &str, table: &str) -> Option<SharedTable> {
        self.tables
            .get(&(schema.to_string(), table.to_string()))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::InMemoryTable;
    use crate::types::{Column, ColumnType, RelSchema, Row, Value};

    fn one_row_table() -> SharedTable {
        Arc::new(InMemoryTable::new(
            RelSchema::new(vec![Column::new("id", ColumnType::Int)]),
            vec![Row::new(vec![Value::Int(1)])],
        ))
    }

    #[test]
    fn resolves_a_registered_table() {
        let cat = MapCatalog::new().with_table("ks", "t", one_row_table());
        let provider = cat.resolve("ks", "t").expect("table should resolve");
        assert_eq!(provider.scan().count(), 1);
        assert_eq!(provider.schema().width(), 1);
    }

    #[test]
    fn unknown_table_resolves_to_none() {
        let cat = MapCatalog::new().with_table("ks", "t", one_row_table());
        assert!(cat.resolve("ks", "missing").is_none()); // wrong table
        assert!(cat.resolve("other", "t").is_none()); // wrong schema
    }
}
