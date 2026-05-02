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

/// Returns the fixed-width byte count for a Cassandra marshal type, or
/// `None` if the type is variable-width.
///
/// Used as a fail-loud guard against malformed cells reaching the SSTable
/// writer. The ferrosa-cql `now()` builtin previously returned an 8-byte
/// Timestamp for TimeUUID columns; the bad cell flowed through the write
/// path, the commit log, and the memtable, then wedged every flush
/// because the writer rejected the 8-byte payload for a 16-byte type. See
/// specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-from-now-
/// function.md.
///
/// Variable-width types (Inet — 4 or 16 bytes — text, blob, decimal,
/// varint, list/set/map/tuple/UDT) return `None` and are not validated
/// at this layer; their lengths are checked at decode time downstream.
pub fn fixed_width_for_marshal_type(type_name: &str) -> Option<usize> {
    match type_name {
        "org.apache.cassandra.db.marshal.TimeUUIDType"
        | "org.apache.cassandra.db.marshal.UUIDType"
        | "org.apache.cassandra.db.marshal.LexicalUUIDType" => Some(16),
        "org.apache.cassandra.db.marshal.Int32Type" => Some(4),
        "org.apache.cassandra.db.marshal.LongType"
        | "org.apache.cassandra.db.marshal.TimestampType"
        | "org.apache.cassandra.db.marshal.DateType"
        | "org.apache.cassandra.db.marshal.TimeType"
        | "org.apache.cassandra.db.marshal.CounterColumnType" => Some(8),
        "org.apache.cassandra.db.marshal.FloatType" => Some(4),
        "org.apache.cassandra.db.marshal.DoubleType" => Some(8),
        "org.apache.cassandra.db.marshal.BooleanType"
        | "org.apache.cassandra.db.marshal.ByteType" => Some(1),
        "org.apache.cassandra.db.marshal.ShortType" => Some(2),
        "org.apache.cassandra.db.marshal.SimpleDateType" => Some(4),
        _ => None,
    }
}

/// Validate that an encoded cell's byte length matches the column's
/// declared fixed-width type. Returns `Err(reason)` on mismatch with a
/// message including the expected and actual lengths.
///
/// Empty bytes are always allowed (NULL cell or tombstone tracking).
/// Variable-width types pass through unchecked.
pub fn validate_cell_bytes(type_name: &str, bytes: &[u8]) -> Result<(), String> {
    if bytes.is_empty() {
        return Ok(());
    }
    if let Some(expected) = fixed_width_for_marshal_type(type_name) {
        if bytes.len() != expected {
            return Err(format!(
                "{} expects {} raw bytes but value provided {}",
                type_name,
                expected,
                bytes.len()
            ));
        }
    }
    Ok(())
}

/// Validate that a row's `clustering` bytes match the shape demanded by
/// `clustering_columns`. Returns `Err(reason)` on mismatch.
///
/// Mirrors `ferrosa_sstable::writer::validate_clustering_shape` so the
/// fail-loud guard fires at the storage boundary (Memtable::put) AND the
/// flush-time quarantine path can route bad clustering bytes to the
/// JSONL file instead of letting the SSTable writer panic the flush.
///
/// Format (matches how `ferrosa_cql::bridge::encode_clustering`
/// produces and how `serialize_row` consumes the bytes):
/// - 0 clustering columns: any (including empty) is OK.
/// - 1 clustering column: row.clustering is RAW component bytes;
///   fixed-length types must match exactly.
/// - 2+ clustering columns: row.clustering is a u16-prefixed composite
///   `[u16 len][bytes]...` per component; each component is validated
///   against its declared type.
///
/// See specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-from-
/// now-function.md.
pub fn validate_clustering_shape(
    clustering_columns: &[ColumnDefinition],
    clustering: &[u8],
) -> Result<(), String> {
    let num_ck = clustering_columns.len();
    if num_ck == 0 {
        return Ok(());
    }
    if clustering.is_empty() {
        let expected_hint = if num_ck == 1 {
            match fixed_width_for_marshal_type(&clustering_columns[0].type_name) {
                Some(n) => format!(
                    " (schema expects {n} raw bytes for {})",
                    clustering_columns[0].type_name
                ),
                None => String::new(),
            }
        } else {
            String::new()
        };
        return Err(format!(
            "clustering bytes are empty but schema declares {num_ck} clustering column(s){expected_hint}"
        ));
    }
    if num_ck == 1 {
        let type_name = &clustering_columns[0].type_name;
        if let Some(fixed_len) = fixed_width_for_marshal_type(type_name) {
            if clustering.len() != fixed_len {
                return Err(format!(
                    "clustering column 0 ({type_name}) expects {fixed_len} raw bytes but row provided {got}",
                    got = clustering.len(),
                ));
            }
        }
        return Ok(());
    }
    // Multi-column composite: u16-prefixed.
    let mut pos = 0usize;
    for (col_idx, column) in clustering_columns.iter().enumerate() {
        if pos + 2 > clustering.len() {
            return Err(format!(
                "truncated composite clustering — column {col_idx} ({}) expected u16 length prefix at byte offset {pos} but buffer is only {total} bytes",
                column.type_name,
                total = clustering.len(),
            ));
        }
        let prefix = u16::from_be_bytes([clustering[pos], clustering[pos + 1]]) as usize;
        pos += 2;
        if pos + prefix > clustering.len() {
            return Err(format!(
                "truncated composite clustering — column {col_idx} ({}) length prefix claims {prefix} bytes but only {remaining} remain",
                column.type_name,
                remaining = clustering.len() - pos,
            ));
        }
        if let Some(fixed_len) = fixed_width_for_marshal_type(&column.type_name) {
            if prefix != fixed_len {
                return Err(format!(
                    "clustering column {col_idx} ({}) expects {fixed_len} bytes but row provided {prefix}",
                    column.type_name,
                ));
            }
        }
        pos += prefix;
    }
    if pos != clustering.len() {
        return Err(format!(
            "{trailing} trailing byte(s) after composite clustering columns",
            trailing = clustering.len() - pos,
        ));
    }
    Ok(())
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
    fn fixed_width_known_types() {
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.TimeUUIDType"),
            Some(16)
        );
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.UUIDType"),
            Some(16)
        );
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.Int32Type"),
            Some(4)
        );
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.LongType"),
            Some(8)
        );
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.BooleanType"),
            Some(1)
        );
    }

    #[test]
    fn fixed_width_variable_types_return_none() {
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.UTF8Type"),
            None
        );
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.BytesType"),
            None
        );
        // Inet is 4 (v4) or 16 (v6), so we don't pin it at this layer.
        assert_eq!(
            fixed_width_for_marshal_type("org.apache.cassandra.db.marshal.InetAddressType"),
            None
        );
    }

    /// Regression test for the memtable-flush wedge: an 8-byte cell
    /// (the millisecond Timestamp the buggy `now()` produced) bound
    /// to a TimeUUID column must be rejected by the validator.
    #[test]
    fn validate_rejects_8_byte_value_in_timeuuid_column() {
        let bad = vec![0u8; 8];
        let err = validate_cell_bytes("org.apache.cassandra.db.marshal.TimeUUIDType", &bad)
            .expect_err("8-byte cell in TimeUUID column must be rejected");
        assert!(
            err.contains("16"),
            "error message should cite 16 bytes: {err}"
        );
        assert!(
            err.contains("8"),
            "error message should cite actual 8 bytes: {err}"
        );
    }

    #[test]
    fn validate_accepts_correct_widths() {
        assert!(
            validate_cell_bytes("org.apache.cassandra.db.marshal.TimeUUIDType", &[0u8; 16]).is_ok()
        );
        assert!(
            validate_cell_bytes("org.apache.cassandra.db.marshal.Int32Type", &[0u8; 4]).is_ok()
        );
        assert!(validate_cell_bytes("org.apache.cassandra.db.marshal.LongType", &[0u8; 8]).is_ok());
    }

    #[test]
    fn validate_passes_through_variable_width_types() {
        // Anything goes through for UTF8/Bytes/etc.
        assert!(validate_cell_bytes(
            "org.apache.cassandra.db.marshal.UTF8Type",
            "hello world".as_bytes()
        )
        .is_ok());
        assert!(
            validate_cell_bytes("org.apache.cassandra.db.marshal.BytesType", &[0u8; 999]).is_ok()
        );
    }

    #[test]
    fn validate_empty_bytes_always_ok() {
        // Empty cells (null markers) should not trip the length check.
        assert!(validate_cell_bytes("org.apache.cassandra.db.marshal.TimeUUIDType", &[]).is_ok());
    }

    /// Production-observed wedge shape: the bug's malformed bytes are
    /// in `row.clustering` (8 bytes) on a TimeUUID-clustered table
    /// where the schema demands 16. The per-cell validator missed
    /// this; this test pins the clustering-validator's behaviour.
    #[test]
    fn validate_clustering_rejects_8_bytes_in_timeuuid_column() {
        let cols = vec![ColumnDefinition {
            name: "call_id".to_string(),
            type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
        }];
        let err = validate_clustering_shape(&cols, &[0u8; 8])
            .expect_err("8-byte clustering on TimeUUID column must be rejected");
        assert!(err.contains("16"), "error must cite 16 expected: {err}");
        assert!(err.contains("8"), "error must cite 8 actual: {err}");
    }

    #[test]
    fn validate_clustering_accepts_16_bytes_in_timeuuid_column() {
        let cols = vec![ColumnDefinition {
            name: "call_id".to_string(),
            type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
        }];
        assert!(validate_clustering_shape(&cols, &[0u8; 16]).is_ok());
    }

    #[test]
    fn validate_clustering_no_columns_accepts_anything() {
        assert!(validate_clustering_shape(&[], &[]).is_ok());
        assert!(validate_clustering_shape(&[], &[1, 2, 3]).is_ok());
    }

    #[test]
    fn validate_clustering_empty_with_columns_rejected() {
        let cols = vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }];
        let err = validate_clustering_shape(&cols, &[])
            .expect_err("empty clustering on schema with 1 clustering column must be rejected");
        assert!(
            err.contains("4"),
            "error should cite expected 4 bytes: {err}"
        );
    }

    #[test]
    fn validate_clustering_multi_column_composite_ok() {
        let cols = vec![
            ColumnDefinition {
                name: "a".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "b".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            },
        ];
        // [u16 len=3]"abc"[u16 len=4][int 1]
        let composite = [0u8, 3, b'a', b'b', b'c', 0u8, 4, 0u8, 0u8, 0u8, 1u8];
        assert!(validate_clustering_shape(&cols, &composite).is_ok());
    }

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
