//! Table schema definitions shared across storage and schema crates.
//!
//! `TableSchema` describes a table's column structure: partition key type,
//! clustering columns, static columns, and regular columns. It does NOT
//! depend on ferrosa-sstable — conversion to `SerializationHeader` lives
//! in ferrosa-storage::flush to avoid circular dependencies.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A single column definition within a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Cassandra type class name (e.g., `org.apache.cassandra.db.marshal.UTF8Type`).
    pub type_name: String,
}

/// NVMe-pinning configuration derived from table extensions.
///
/// When a table is created with `extensions = {'storage.pin': 'nvme'}`,
/// its SSTables are kept on local NVMe storage only — never uploaded to S3
/// and never evicted from `LocalCache`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PinConfig {
    /// True when the table's `storage.pin` extension is `"nvme"`.
    pub nvme: bool,
}

impl PinConfig {
    /// Returns true if the table is pinned to NVMe (must stay local, skip S3).
    pub fn is_pinned(&self) -> bool {
        self.nvme
    }

    /// Build a `PinConfig` from a raw extensions map (e.g. from `WITH extensions`).
    pub fn from_extensions(extensions: &HashMap<String, String>) -> Self {
        Self {
            nvme: extensions
                .get("storage.pin")
                .map(|v| v == "nvme")
                .unwrap_or(false),
        }
    }
}

/// Describes a table's column structure.
///
/// Column ordering: static columns first (by position in `static_columns`),
/// then regular columns (by position in `regular_columns`). This matches
/// Cassandra's internal column index assignment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub keyspace: String,
    pub table: String,
    /// Cassandra type class name for the partition key.
    pub key_type: String,
    pub clustering_columns: Vec<ColumnDefinition>,
    pub static_columns: Vec<ColumnDefinition>,
    /// Regular columns, ordered by column index.
    pub regular_columns: Vec<ColumnDefinition>,
    /// Optional table-level extension key/value pairs.
    ///
    /// Set via `WITH extensions = {'key': 'value'}` in CQL. Used to carry
    /// storage hints such as `storage.pin = nvme`.
    #[serde(default)]
    pub extensions: HashMap<String, String>,
}

impl TableSchema {
    /// Derives the `PinConfig` for this table from its extensions.
    pub fn pin_config(&self) -> PinConfig {
        PinConfig::from_extensions(&self.extensions)
    }

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
            extensions: Default::default(),
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
            extensions: Default::default(),
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
            extensions: Default::default(),
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
            extensions: Default::default(),
        };
        assert_eq!(schema.column_index("a"), Some(0));
        assert_eq!(schema.column_index("b"), Some(1));
    }
}
