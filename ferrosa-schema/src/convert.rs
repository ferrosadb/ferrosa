//! Storage bridge: CQL type conversion and TableMetadata → TableSchema.
//!
//! Maps CQL type names to Cassandra marshal type class names and converts
//! `TableMetadata` into `ferrosa_common::schema::TableSchema`.

use ferrosa_common::schema::{ColumnDefinition, TableSchema};

use crate::metadata::column::ColumnKind;
use crate::metadata::table::TableMetadata;

/// Convert a CQL type name to its Cassandra marshal type class name.
///
/// Handles scalar types, collection types (`set<T>`, `list<T>`, `map<K,V>`,
/// `frozen<T>`), and returns unknown types as-is for forward compatibility.
pub fn cql_to_marshal_type(cql_type: &str) -> String {
    let trimmed = cql_type.trim();

    // Handle collection/wrapper types
    if let Some(inner) = strip_wrapper(trimmed, "frozen") {
        let inner_marshal = cql_to_marshal_type(inner);
        return format!(
            "org.apache.cassandra.db.marshal.FrozenType({})",
            inner_marshal
        );
    }
    if let Some(inner) = strip_wrapper(trimmed, "set") {
        let inner_marshal = cql_to_marshal_type(inner);
        return format!("org.apache.cassandra.db.marshal.SetType({})", inner_marshal);
    }
    if let Some(inner) = strip_wrapper(trimmed, "list") {
        let inner_marshal = cql_to_marshal_type(inner);
        return format!(
            "org.apache.cassandra.db.marshal.ListType({})",
            inner_marshal
        );
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "map<") {
        if let Some(inner) = rest.strip_suffix('>') {
            if let Some((k, v)) = split_map_types(inner) {
                let k_marshal = cql_to_marshal_type(k);
                let v_marshal = cql_to_marshal_type(v);
                return format!(
                    "org.apache.cassandra.db.marshal.MapType({},{})",
                    k_marshal, v_marshal
                );
            }
        }
    }

    // Scalar types
    match trimmed {
        "text" | "varchar" => "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        "int" => "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        "bigint" => "org.apache.cassandra.db.marshal.LongType".to_string(),
        "boolean" => "org.apache.cassandra.db.marshal.BooleanType".to_string(),
        "float" => "org.apache.cassandra.db.marshal.FloatType".to_string(),
        "double" => "org.apache.cassandra.db.marshal.DoubleType".to_string(),
        "blob" => "org.apache.cassandra.db.marshal.BytesType".to_string(),
        "timestamp" => "org.apache.cassandra.db.marshal.TimestampType".to_string(),
        "uuid" => "org.apache.cassandra.db.marshal.UUIDType".to_string(),
        "timeuuid" => "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
        "inet" => "org.apache.cassandra.db.marshal.InetAddressType".to_string(),
        "counter" => "org.apache.cassandra.db.marshal.CounterColumnType".to_string(),
        "ascii" => "org.apache.cassandra.db.marshal.AsciiType".to_string(),
        "decimal" => "org.apache.cassandra.db.marshal.DecimalType".to_string(),
        "varint" => "org.apache.cassandra.db.marshal.IntegerType".to_string(),
        "smallint" => "org.apache.cassandra.db.marshal.ShortType".to_string(),
        "tinyint" => "org.apache.cassandra.db.marshal.ByteType".to_string(),
        "date" => "org.apache.cassandra.db.marshal.SimpleDateType".to_string(),
        "time" => "org.apache.cassandra.db.marshal.TimeType".to_string(),
        "duration" => "org.apache.cassandra.db.marshal.DurationType".to_string(),
        // Unknown types: return as-is for forward compatibility
        other => other.to_string(),
    }
}

/// Strip a wrapper type prefix like `set<...>` and return the inner type.
fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = strip_prefix_ci(s, &format!("{prefix}<"))?;
    rest.strip_suffix('>')
}

/// Case-insensitive prefix stripping.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Split `map<K, V>` inner types on the top-level comma.
///
/// Handles nested generics by tracking angle bracket depth.
fn split_map_types(s: &str) -> Option<(&str, &str)> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                let key = s[..i].trim();
                let val = s[i + 1..].trim();
                return Some((key, val));
            }
            _ => {}
        }
    }
    None
}

impl TableMetadata {
    /// Convert this table metadata to a `TableSchema` suitable for the storage layer.
    ///
    /// Maps partition key columns to a single `key_type` (or `CompositeType` for
    /// compound keys), keeps clustering columns in clustering-key position,
    /// and orders static/regular cells by Cassandra's column-name comparator.
    /// Compute the storage-layer column index for a given column name.
    ///
    /// Storage column indices are 0-based within `[static_columns... regular_columns...]`,
    /// matching the SSTable format. PK and CK columns are NOT included in this
    /// index space — they are encoded separately in the partition key and
    /// clustering key, respectively.
    ///
    /// Returns `None` if the column is not found, or is a PK/CK column.
    pub fn storage_column_index(&self, col_name: &str) -> Option<u16> {
        let col = self.columns.get(col_name)?;
        match col.kind {
            ColumnKind::PartitionKey | ColumnKind::Clustering => None,
            ColumnKind::Static => {
                // Static columns are indexed first and sorted by Cassandra's
                // column-name comparator.
                let mut static_cols: Vec<_> = self
                    .columns
                    .values()
                    .filter(|c| c.kind == ColumnKind::Static)
                    .collect();
                static_cols.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
                static_cols
                    .iter()
                    .position(|c| c.name == col_name)
                    .map(|p| p as u16)
            }
            ColumnKind::Regular => {
                // Regular columns follow static columns and are sorted by
                // Cassandra's column-name comparator.
                let num_statics = self
                    .columns
                    .values()
                    .filter(|c| c.kind == ColumnKind::Static)
                    .count();
                let mut regulars: Vec<_> = self
                    .columns
                    .values()
                    .filter(|c| c.kind == ColumnKind::Regular)
                    .collect();
                regulars.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
                regulars
                    .iter()
                    .position(|c| c.name == col_name)
                    .map(|p| (num_statics + p) as u16)
            }
        }
    }

    /// Convert this table metadata to a `TableSchema` suitable for the storage layer.
    ///
    /// Maps partition key columns to a single `key_type` (or `CompositeType` for
    /// compound keys), keeps clustering columns in clustering-key position,
    /// and orders static/regular cells by Cassandra's column-name comparator.
    pub fn to_storage_schema(&self) -> TableSchema {
        // Build key_type
        let key_type = if self.partition_key.len() == 1 {
            // Single partition key column
            self.columns
                .get(&self.partition_key[0])
                .map(|c| cql_to_marshal_type(&c.column_type))
                .unwrap_or_default()
        } else {
            // Composite partition key
            let inner: Vec<String> = self
                .partition_key
                .iter()
                .filter_map(|name| self.columns.get(name))
                .map(|c| cql_to_marshal_type(&c.column_type))
                .collect();
            format!(
                "org.apache.cassandra.db.marshal.CompositeType({})",
                inner.join(",")
            )
        };

        // Collect and sort clustering columns by position
        let mut clustering: Vec<_> = self
            .columns
            .values()
            .filter(|c| c.kind == ColumnKind::Clustering)
            .collect();
        clustering.sort_by_key(|c| c.position);
        let clustering_columns: Vec<ColumnDefinition> = clustering
            .iter()
            .map(|c| ColumnDefinition {
                name: c.name.clone(),
                type_name: cql_to_marshal_type(&c.column_type),
            })
            .collect();

        // Collect and sort static columns by Cassandra's column-name comparator.
        let mut static_cols: Vec<_> = self
            .columns
            .values()
            .filter(|c| c.kind == ColumnKind::Static)
            .collect();
        static_cols.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let static_columns: Vec<ColumnDefinition> = static_cols
            .iter()
            .map(|c| ColumnDefinition {
                name: c.name.clone(),
                type_name: cql_to_marshal_type(&c.column_type),
            })
            .collect();

        // Collect and sort regular columns by Cassandra's column-name comparator.
        let mut regulars: Vec<_> = self
            .columns
            .values()
            .filter(|c| c.kind == ColumnKind::Regular)
            .collect();
        regulars.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let regular_columns: Vec<ColumnDefinition> = regulars
            .iter()
            .map(|c| ColumnDefinition {
                name: c.name.clone(),
                type_name: cql_to_marshal_type(&c.column_type),
            })
            .collect();

        // Merge table options into extensions so the storage layer can
        // configure components without depending on schema metadata types.
        let mut extensions = self.extensions.clone();
        for (k, v) in &self.params.compaction {
            extensions.insert(format!("compaction.{k}"), v.clone());
        }
        for (k, v) in &self.params.compression {
            extensions.insert(format!("compression.{k}"), v.clone());
        }

        TableSchema {
            keyspace: self.keyspace.clone(),
            table: self.name.clone(),
            key_type,
            clustering_columns,
            static_columns,
            regular_columns,
            extensions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::column::{ClusteringOrder, ColumnMetadata};
    use crate::metadata::table::TableParams;
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn cql_text_to_utf8type() {
        assert_eq!(
            cql_to_marshal_type("text"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
    }

    #[test]
    fn cql_varchar_to_utf8type() {
        assert_eq!(
            cql_to_marshal_type("varchar"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
    }

    #[test]
    fn cql_int_to_int32type() {
        assert_eq!(
            cql_to_marshal_type("int"),
            "org.apache.cassandra.db.marshal.Int32Type"
        );
    }

    #[test]
    fn cql_bigint_to_longtype() {
        assert_eq!(
            cql_to_marshal_type("bigint"),
            "org.apache.cassandra.db.marshal.LongType"
        );
    }

    #[test]
    fn cql_boolean_to_booleantype() {
        assert_eq!(
            cql_to_marshal_type("boolean"),
            "org.apache.cassandra.db.marshal.BooleanType"
        );
    }

    #[test]
    fn cql_uuid_to_uuidtype() {
        assert_eq!(
            cql_to_marshal_type("uuid"),
            "org.apache.cassandra.db.marshal.UUIDType"
        );
    }

    #[test]
    fn cql_timestamp_to_timestamptype() {
        assert_eq!(
            cql_to_marshal_type("timestamp"),
            "org.apache.cassandra.db.marshal.TimestampType"
        );
    }

    #[test]
    fn cql_counter_to_countertype() {
        assert_eq!(
            cql_to_marshal_type("counter"),
            "org.apache.cassandra.db.marshal.CounterColumnType"
        );
    }

    #[test]
    fn cql_unknown_passthrough() {
        assert_eq!(cql_to_marshal_type("my_udt"), "my_udt");
    }

    #[test]
    fn cql_set_collection() {
        assert_eq!(
            cql_to_marshal_type("set<text>"),
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
        );
    }

    #[test]
    fn cql_list_collection() {
        assert_eq!(
            cql_to_marshal_type("list<int>"),
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
        );
    }

    #[test]
    fn cql_map_collection() {
        assert_eq!(
            cql_to_marshal_type("map<text, int>"),
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        );
    }

    #[test]
    fn cql_frozen_type() {
        assert_eq!(
            cql_to_marshal_type("frozen<set<text>>"),
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))"
        );
    }

    #[test]
    fn to_storage_schema_basic() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnMetadata {
                name: "name".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let table = TableMetadata {
            keyspace: "ks1".to_string(),
            name: "users".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };

        let schema = table.to_storage_schema();
        assert_eq!(schema.keyspace, "ks1");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.key_type, "org.apache.cassandra.db.marshal.UUIDType");
        assert_eq!(schema.regular_columns.len(), 1);
        assert_eq!(schema.regular_columns[0].name, "name");
        assert_eq!(
            schema.regular_columns[0].type_name,
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
        assert!(schema.clustering_columns.is_empty());
        assert!(schema.static_columns.is_empty());
    }

    #[test]
    fn to_storage_schema_copies_compression_params() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let mut params = TableParams::default();
        params.compression.insert(
            "class".to_string(),
            "org.apache.cassandra.io.compress.ZstdCompressor".to_string(),
        );
        params
            .compression
            .insert("compression_level".to_string(), "5".to_string());
        params
            .compression
            .insert("chunk_length_kb".to_string(), "32".to_string());

        let table = TableMetadata {
            keyspace: "ks1".to_string(),
            name: "compressed".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params,
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };

        let schema = table.to_storage_schema();
        assert_eq!(
            schema.extensions.get("compression.class"),
            Some(&"org.apache.cassandra.io.compress.ZstdCompressor".to_string())
        );
        assert_eq!(
            schema.extensions.get("compression.compression_level"),
            Some(&"5".to_string())
        );
        assert_eq!(
            schema.extensions.get("compression.chunk_length_kb"),
            Some(&"32".to_string())
        );
    }

    #[test]
    fn regular_and_static_storage_ordinals_follow_cassandra_column_name_order() {
        let mut columns = IndexMap::new();
        columns.insert(
            "pk".to_string(),
            ColumnMetadata {
                name: "pk".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "z_static".to_string(),
            ColumnMetadata {
                name: "z_static".to_string(),
                kind: ColumnKind::Static,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "a_static".to_string(),
            ColumnMetadata {
                name: "a_static".to_string(),
                kind: ColumnKind::Static,
                position: 1,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "v_text".to_string(),
            ColumnMetadata {
                name: "v_text".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "v_int".to_string(),
            ColumnMetadata {
                name: "v_int".to_string(),
                kind: ColumnKind::Regular,
                position: 1,
                column_type: "int".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "mixed_cells".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["pk".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };

        let schema = table.to_storage_schema();
        assert_eq!(schema.static_columns[0].name, "a_static");
        assert_eq!(schema.static_columns[1].name, "z_static");
        assert_eq!(schema.regular_columns[0].name, "v_int");
        assert_eq!(schema.regular_columns[1].name, "v_text");
        assert_eq!(table.storage_column_index("a_static"), Some(0));
        assert_eq!(table.storage_column_index("z_static"), Some(1));
        assert_eq!(table.storage_column_index("v_int"), Some(2));
        assert_eq!(table.storage_column_index("v_text"), Some(3));
    }

    #[test]
    fn to_storage_schema_composite_key() {
        let mut columns = IndexMap::new();
        columns.insert(
            "tenant".to_string(),
            ColumnMetadata {
                name: "tenant".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "user_id".to_string(),
            ColumnMetadata {
                name: "user_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 1,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "ts".to_string(),
            ColumnMetadata {
                name: "ts".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "timestamp".to_string(),
                clustering_order: ClusteringOrder::Desc,
                mask: None,
            },
        );

        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "events".to_string(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: vec!["tenant".to_string(), "user_id".to_string()],
            clustering_key: vec![("ts".to_string(), ClusteringOrder::Desc)],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };

        let schema = table.to_storage_schema();
        assert!(schema
            .key_type
            .starts_with("org.apache.cassandra.db.marshal.CompositeType("));
        assert!(schema.key_type.contains("UTF8Type"));
        assert!(schema.key_type.contains("UUIDType"));
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "ts");
        assert_eq!(
            schema.clustering_columns[0].type_name,
            "org.apache.cassandra.db.marshal.TimestampType"
        );
    }
}
