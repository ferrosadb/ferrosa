//! Table schema definitions shared across storage and schema crates.
//!
//! `TableSchema` describes a table's column structure: partition key type,
//! clustering columns, static columns, and regular columns. It does NOT
//! depend on ferrosa-sstable — conversion to `SerializationHeader` lives
//! in ferrosa-storage::flush to avoid circular dependencies.

use serde::{Deserialize, Serialize};

/// A single column definition within a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Cassandra type class name (e.g., `org.apache.cassandra.db.marshal.UTF8Type`).
    pub type_name: String,
}

/// Describes a table's column structure.
///
/// Column ordering: static columns first (by position in `static_columns`),
/// then regular columns (by position in `regular_columns`). This matches
/// Cassandra's internal column index assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub keyspace: String,
    pub table: String,
    /// Cassandra type class name for the partition key.
    pub key_type: String,
    pub clustering_columns: Vec<ColumnDefinition>,
    pub static_columns: Vec<ColumnDefinition>,
    /// Regular columns, ordered by column index.
    pub regular_columns: Vec<ColumnDefinition>,
}

impl TableSchema {
    /// Returns the type names of all clustering columns, in order.
    pub fn clustering_types(&self) -> Vec<String> {
        self.clustering_columns
            .iter()
            .map(|c| c.type_name.clone())
            .collect()
    }

    /// Look up a column's index by name.
    ///
    /// Static columns are indexed first (0..static_columns.len()),
    /// then regular columns (static_columns.len()..).
    pub fn column_index(&self, name: &str) -> Option<u16> {
        for (i, col) in self.static_columns.iter().enumerate() {
            if col.name == name {
                return Some(i as u16);
            }
        }
        let offset = self.static_columns.len();
        for (i, col) in self.regular_columns.iter().enumerate() {
            if col.name == name {
                return Some((offset + i) as u16);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_definition_stores_name_and_type() {
        let col = ColumnDefinition {
            name: "age".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        };
        assert_eq!(col.name, "age");
        assert_eq!(col.type_name, "org.apache.cassandra.db.marshal.Int32Type");
    }

    #[test]
    fn table_schema_construction() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "age".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
        };
        assert_eq!(schema.keyspace, "ks");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.regular_columns.len(), 2);
    }

    #[test]
    fn clustering_types_returns_type_names() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![
                ColumnDefinition {
                    name: "c1".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
                ColumnDefinition {
                    name: "c2".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            static_columns: vec![],
            regular_columns: vec![],
        };
        assert_eq!(
            schema.clustering_types(),
            vec![
                "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            ]
        );
    }

    #[test]
    fn column_index_finds_regular_columns() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![ColumnDefinition {
                name: "s1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "age".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
        };
        // Static columns are indexed first, then regular columns
        assert_eq!(schema.column_index("s1"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("age"), Some(2));
        assert_eq!(schema.column_index("nonexistent"), None);
    }

    #[test]
    fn column_index_no_static_columns() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "a".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "b".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
        };
        assert_eq!(schema.column_index("a"), Some(0));
        assert_eq!(schema.column_index("b"), Some(1));
    }
}
