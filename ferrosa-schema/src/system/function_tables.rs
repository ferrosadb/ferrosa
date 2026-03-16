//! `system_schema.functions` virtual table.
//!
//! Provides a virtual table that exposes user-defined function metadata from
//! the schema snapshot. Compatible with Cassandra's `system_schema.functions`
//! table layout for CQL driver introspection.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_common::{CellValue, CqlType, DataType};

use crate::registry::SchemaSnapshot;
use crate::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Virtual table implementation for `system_schema.functions`.
///
/// Reads the current schema snapshot's functions map and materializes
/// rows on demand. The snapshot is shared via `Arc<ArcSwap<SchemaSnapshot>>`
/// for lock-free reads.
pub struct SystemSchemaFunctionsTable {
    snapshot: Arc<ArcSwap<SchemaSnapshot>>,
    columns: Vec<VirtualColumnDef>,
}

impl SystemSchemaFunctionsTable {
    /// Create a new `system_schema.functions` virtual table backed by the
    /// given snapshot handle.
    pub fn new(snapshot: Arc<ArcSwap<SchemaSnapshot>>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "function_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "argument_names".to_string(),
                data_type: DataType::Text, // list<text> serialized
            },
            VirtualColumnDef {
                name: "argument_types".to_string(),
                data_type: DataType::Text, // list<text> serialized
            },
            VirtualColumnDef {
                name: "return_type".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "called_on_null_input".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "language".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "body".to_string(),
                data_type: DataType::Text,
            },
        ];
        Self { snapshot, columns }
    }
}

/// Convert a `CqlType` to its CQL type string representation.
///
/// Produces lowercase CQL type names matching Cassandra's `system_schema.functions`
/// `argument_types` / `return_type` column format.
fn cql_type_to_string(ty: &CqlType) -> String {
    match ty {
        CqlType::Ascii => "ascii".to_string(),
        CqlType::Bigint => "bigint".to_string(),
        CqlType::Blob => "blob".to_string(),
        CqlType::Boolean => "boolean".to_string(),
        CqlType::Counter => "counter".to_string(),
        CqlType::Decimal => "decimal".to_string(),
        CqlType::Double => "double".to_string(),
        CqlType::Float => "float".to_string(),
        CqlType::Int => "int".to_string(),
        CqlType::Timestamp => "timestamp".to_string(),
        CqlType::Uuid => "uuid".to_string(),
        CqlType::Varchar => "text".to_string(),
        CqlType::Varint => "varint".to_string(),
        CqlType::Timeuuid => "timeuuid".to_string(),
        CqlType::Inet => "inet".to_string(),
        CqlType::Date => "date".to_string(),
        CqlType::Time => "time".to_string(),
        CqlType::Smallint => "smallint".to_string(),
        CqlType::Tinyint => "tinyint".to_string(),
        CqlType::Duration => "duration".to_string(),
        CqlType::List(inner) => format!("list<{}>", cql_type_to_string(inner)),
        CqlType::Set(inner) => format!("set<{}>", cql_type_to_string(inner)),
        CqlType::Map(k, v) => {
            format!("map<{}, {}>", cql_type_to_string(k), cql_type_to_string(v))
        }
        CqlType::Tuple(types) => {
            let inner: Vec<String> = types.iter().map(cql_type_to_string).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlType::Udt { keyspace, name, .. } => format!("{keyspace}.{name}"),
    }
}

/// Serialize a list of strings into a JSON array representation.
///
/// Produces `["a", "b", "c"]` format matching how Cassandra exposes
/// list<text> columns in system tables.
fn serialize_string_list(items: &[String]) -> String {
    let escaped: Vec<String> = items.iter().map(|s| format!("\"{}\"", s)).collect();
    format!("[{}]", escaped.join(", "))
}

impl VirtualTable for SystemSchemaFunctionsTable {
    fn name(&self) -> &str {
        "functions"
    }

    fn keyspace(&self) -> &str {
        "system_schema"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        // keyspace_name (0), function_name (1)
        &[0, 1]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let snap = self.snapshot.load_full();
        let mut rows = Vec::new();

        for ((_ks, _name, _atypes), func) in &snap.functions {
            let arg_names_str = serialize_string_list(&func.arg_names);
            let arg_types: Vec<String> = func.arg_types.iter().map(cql_type_to_string).collect();
            let arg_types_str = serialize_string_list(&arg_types);
            let return_type_str = cql_type_to_string(&func.return_type);
            let called_on_null = if func.called_on_null { "true" } else { "false" };

            rows.push(VirtualRow {
                cells: vec![
                    CellValue::live(func.keyspace.as_bytes().to_vec(), 0),
                    CellValue::live(func.name.as_bytes().to_vec(), 0),
                    CellValue::live(arg_names_str.into_bytes(), 0),
                    CellValue::live(arg_types_str.into_bytes(), 0),
                    CellValue::live(return_type_str.into_bytes(), 0),
                    CellValue::live(called_on_null.as_bytes().to_vec(), 0),
                    CellValue::live(func.language.as_bytes().to_vec(), 0),
                    CellValue::live(func.body.as_bytes().to_vec(), 0),
                ],
            });
        }

        rows
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::function::UserFunctionMetadata;
    use ferrosa_common::CqlType;

    fn empty_snapshot() -> Arc<ArcSwap<SchemaSnapshot>> {
        Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())))
    }

    fn snapshot_with_function(func: UserFunctionMetadata) -> Arc<ArcSwap<SchemaSnapshot>> {
        let mut snap = SchemaSnapshot::new();
        let key = (
            func.keyspace.clone(),
            func.name.clone(),
            func.arg_types.clone(),
        );
        snap.functions.insert(key, func);
        Arc::new(ArcSwap::new(Arc::new(snap)))
    }

    #[test]
    fn functions_table_columns() {
        let snap = empty_snapshot();
        let table = SystemSchemaFunctionsTable::new(snap);
        let cols = table.columns();
        assert_eq!(cols.len(), 8);
        assert_eq!(cols[0].name, "keyspace_name");
        assert_eq!(cols[1].name, "function_name");
        assert_eq!(cols[2].name, "argument_names");
        assert_eq!(cols[3].name, "argument_types");
        assert_eq!(cols[4].name, "return_type");
        assert_eq!(cols[5].name, "called_on_null_input");
        assert_eq!(cols[6].name, "language");
        assert_eq!(cols[7].name, "body");
    }

    #[test]
    fn functions_table_empty_snapshot() {
        let snap = empty_snapshot();
        let table = SystemSchemaFunctionsTable::new(snap);
        let rows = table.read(None);
        assert!(rows.is_empty());
    }

    #[test]
    fn functions_table_returns_rows() {
        let func = UserFunctionMetadata {
            keyspace: "ks1".to_string(),
            name: "double_val".to_string(),
            arg_names: vec!["val".to_string()],
            arg_types: vec![CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: false,
            language: "wasm".to_string(),
            body: "AGFzbQ==".to_string(),
        };
        let snap = snapshot_with_function(func);
        let table = SystemSchemaFunctionsTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.cells.len(), 8);
        // keyspace_name
        assert_eq!(row.cells[0].value.as_deref(), Some(b"ks1".as_slice()));
        // function_name
        assert_eq!(
            row.cells[1].value.as_deref(),
            Some(b"double_val".as_slice())
        );
        // argument_names — JSON list
        let arg_names = std::str::from_utf8(row.cells[2].value.as_deref().unwrap()).unwrap();
        assert_eq!(arg_names, r#"["val"]"#);
        // argument_types — JSON list
        let arg_types = std::str::from_utf8(row.cells[3].value.as_deref().unwrap()).unwrap();
        assert_eq!(arg_types, r#"["int"]"#);
        // return_type
        assert_eq!(row.cells[4].value.as_deref(), Some(b"int".as_slice()));
        // called_on_null_input
        assert_eq!(row.cells[5].value.as_deref(), Some(b"false".as_slice()));
        // language
        assert_eq!(row.cells[6].value.as_deref(), Some(b"wasm".as_slice()));
        // body
        assert_eq!(row.cells[7].value.as_deref(), Some(b"AGFzbQ==".as_slice()));
    }

    #[test]
    fn functions_table_metadata() {
        let snap = empty_snapshot();
        let table = SystemSchemaFunctionsTable::new(snap);
        assert_eq!(table.name(), "functions");
        assert_eq!(table.keyspace(), "system_schema");
        assert_eq!(table.primary_key_columns(), &[0, 1]);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::None));
    }

    #[test]
    fn functions_table_called_on_null_true() {
        let func = UserFunctionMetadata {
            keyspace: "ks1".to_string(),
            name: "null_safe".to_string(),
            arg_names: vec!["x".to_string()],
            arg_types: vec![CqlType::Varchar],
            return_type: CqlType::Varchar,
            called_on_null: true,
            language: "wasm".to_string(),
            body: "body".to_string(),
        };
        let snap = snapshot_with_function(func);
        let table = SystemSchemaFunctionsTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows[0].cells[5].value.as_deref(), Some(b"true".as_slice()));
    }

    #[test]
    fn functions_table_multiple_args() {
        let func = UserFunctionMetadata {
            keyspace: "ks1".to_string(),
            name: "add".to_string(),
            arg_names: vec!["a".to_string(), "b".to_string()],
            arg_types: vec![CqlType::Int, CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: false,
            language: "wasm".to_string(),
            body: "body".to_string(),
        };
        let snap = snapshot_with_function(func);
        let table = SystemSchemaFunctionsTable::new(snap);
        let rows = table.read(None);
        let arg_names = std::str::from_utf8(rows[0].cells[2].value.as_deref().unwrap()).unwrap();
        assert_eq!(arg_names, r#"["a", "b"]"#);
        let arg_types = std::str::from_utf8(rows[0].cells[3].value.as_deref().unwrap()).unwrap();
        assert_eq!(arg_types, r#"["int", "int"]"#);
    }
}
