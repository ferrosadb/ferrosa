//! `system_schema.types` virtual table.
//!
//! Provides a virtual table that exposes user-defined type metadata from
//! the schema snapshot. Compatible with Cassandra's `system_schema.types`
//! table layout for CQL driver introspection.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_common::{CellValue, CqlType, DataType};

use crate::registry::SchemaSnapshot;
use crate::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Virtual table implementation for `system_schema.types`.
///
/// Reads the current schema snapshot's types map and materializes
/// rows on demand. The snapshot is shared via `Arc<ArcSwap<SchemaSnapshot>>`
/// for lock-free reads.
pub struct SystemSchemaTypesTable {
    snapshot: Arc<ArcSwap<SchemaSnapshot>>,
    columns: Vec<VirtualColumnDef>,
}

impl SystemSchemaTypesTable {
    /// Create a new `system_schema.types` virtual table backed by the
    /// given snapshot handle.
    pub fn new(snapshot: Arc<ArcSwap<SchemaSnapshot>>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "type_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "field_names".to_string(),
                data_type: DataType::Text, // list<text> serialized
            },
            VirtualColumnDef {
                name: "field_types".to_string(),
                data_type: DataType::Text, // list<text> serialized
            },
        ];
        Self { snapshot, columns }
    }
}

/// Convert a `CqlType` to its CQL type string representation.
///
/// Produces lowercase CQL type names matching Cassandra's `system_schema.types`
/// `field_types` column format (e.g. `"text"`, `"int"`, `"list<text>"`).
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

impl VirtualTable for SystemSchemaTypesTable {
    fn name(&self) -> &str {
        "types"
    }

    fn keyspace(&self) -> &str {
        "system_schema"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        // keyspace_name (0), type_name (1)
        &[0, 1]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let snap = self.snapshot.load_full();
        let mut rows = Vec::new();

        for ((_ks, _name), udt) in &snap.types {
            let field_names: Vec<String> = udt.fields.iter().map(|(n, _)| n.clone()).collect();
            let field_types: Vec<String> = udt
                .fields
                .iter()
                .map(|(_, t)| cql_type_to_string(t))
                .collect();

            let field_names_str = serialize_string_list(&field_names);
            let field_types_str = serialize_string_list(&field_types);

            rows.push(VirtualRow {
                cells: vec![
                    CellValue::live(udt.keyspace.as_bytes().to_vec(), 0),
                    CellValue::live(udt.name.as_bytes().to_vec(), 0),
                    CellValue::live(field_names_str.into_bytes(), 0),
                    CellValue::live(field_types_str.into_bytes(), 0),
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
    use crate::metadata::user_type::UserTypeMetadata;
    use ferrosa_common::CqlType;

    fn empty_snapshot() -> Arc<ArcSwap<SchemaSnapshot>> {
        Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())))
    }

    fn snapshot_with_type(udt: UserTypeMetadata) -> Arc<ArcSwap<SchemaSnapshot>> {
        let mut snap = SchemaSnapshot::new();
        let key = (udt.keyspace.clone(), udt.name.clone());
        snap.types.insert(key, udt);
        Arc::new(ArcSwap::new(Arc::new(snap)))
    }

    #[test]
    fn types_table_metadata() {
        let snap = empty_snapshot();
        let table = SystemSchemaTypesTable::new(snap);
        assert_eq!(table.name(), "types");
        assert_eq!(table.keyspace(), "system_schema");
        assert_eq!(table.primary_key_columns(), &[0, 1]);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::None));
    }

    #[test]
    fn types_table_columns() {
        let snap = empty_snapshot();
        let table = SystemSchemaTypesTable::new(snap);
        let cols = table.columns();
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0].name, "keyspace_name");
        assert_eq!(cols[1].name, "type_name");
        assert_eq!(cols[2].name, "field_names");
        assert_eq!(cols[3].name, "field_types");
    }

    #[test]
    fn types_virtual_table_empty_when_no_types() {
        let snap = empty_snapshot();
        let table = SystemSchemaTypesTable::new(snap);
        let rows = table.read(None);
        assert!(rows.is_empty());
    }

    #[test]
    fn types_virtual_table_lists_udts() {
        let udt = UserTypeMetadata {
            keyspace: "ks1".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        let snap = snapshot_with_type(udt);
        let table = SystemSchemaTypesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.cells.len(), 4);
        // keyspace_name
        assert_eq!(row.cells[0].value.as_deref(), Some(b"ks1".as_slice()));
        // type_name
        assert_eq!(row.cells[1].value.as_deref(), Some(b"address".as_slice()));
        // field_names — JSON list
        let field_names = std::str::from_utf8(row.cells[2].value.as_deref().unwrap()).unwrap();
        assert_eq!(field_names, r#"["street", "city", "zip"]"#);
        // field_types — JSON list
        let field_types = std::str::from_utf8(row.cells[3].value.as_deref().unwrap()).unwrap();
        assert_eq!(field_types, r#"["text", "text", "int"]"#);
    }

    #[test]
    fn types_virtual_table_multiple_types() {
        let mut snap = SchemaSnapshot::new();
        snap.types.insert(
            ("ks1".to_string(), "address".to_string()),
            UserTypeMetadata {
                keyspace: "ks1".to_string(),
                name: "address".to_string(),
                fields: vec![("street".to_string(), CqlType::Varchar)],
            },
        );
        snap.types.insert(
            ("ks1".to_string(), "phone".to_string()),
            UserTypeMetadata {
                keyspace: "ks1".to_string(),
                name: "phone".to_string(),
                fields: vec![
                    ("country_code".to_string(), CqlType::Int),
                    ("number".to_string(), CqlType::Varchar),
                ],
            },
        );
        let handle = Arc::new(ArcSwap::new(Arc::new(snap)));
        let table = SystemSchemaTypesTable::new(handle);
        let rows = table.read(None);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn types_virtual_table_collection_field_types() {
        let udt = UserTypeMetadata {
            keyspace: "ks1".to_string(),
            name: "complex".to_string(),
            fields: vec![
                (
                    "tags".to_string(),
                    CqlType::List(Box::new(CqlType::Varchar)),
                ),
                (
                    "attrs".to_string(),
                    CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
                ),
                ("ids".to_string(), CqlType::Set(Box::new(CqlType::Uuid))),
            ],
        };
        let snap = snapshot_with_type(udt);
        let table = SystemSchemaTypesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let field_types = std::str::from_utf8(rows[0].cells[3].value.as_deref().unwrap()).unwrap();
        assert_eq!(
            field_types,
            r#"["list<text>", "map<text, int>", "set<uuid>"]"#
        );
    }

    #[test]
    fn cql_type_to_string_scalars() {
        assert_eq!(cql_type_to_string(&CqlType::Ascii), "ascii");
        assert_eq!(cql_type_to_string(&CqlType::Bigint), "bigint");
        assert_eq!(cql_type_to_string(&CqlType::Blob), "blob");
        assert_eq!(cql_type_to_string(&CqlType::Boolean), "boolean");
        assert_eq!(cql_type_to_string(&CqlType::Counter), "counter");
        assert_eq!(cql_type_to_string(&CqlType::Decimal), "decimal");
        assert_eq!(cql_type_to_string(&CqlType::Double), "double");
        assert_eq!(cql_type_to_string(&CqlType::Float), "float");
        assert_eq!(cql_type_to_string(&CqlType::Int), "int");
        assert_eq!(cql_type_to_string(&CqlType::Timestamp), "timestamp");
        assert_eq!(cql_type_to_string(&CqlType::Uuid), "uuid");
        assert_eq!(cql_type_to_string(&CqlType::Varchar), "text");
        assert_eq!(cql_type_to_string(&CqlType::Varint), "varint");
        assert_eq!(cql_type_to_string(&CqlType::Timeuuid), "timeuuid");
        assert_eq!(cql_type_to_string(&CqlType::Inet), "inet");
        assert_eq!(cql_type_to_string(&CqlType::Date), "date");
        assert_eq!(cql_type_to_string(&CqlType::Time), "time");
        assert_eq!(cql_type_to_string(&CqlType::Smallint), "smallint");
        assert_eq!(cql_type_to_string(&CqlType::Tinyint), "tinyint");
        assert_eq!(cql_type_to_string(&CqlType::Duration), "duration");
    }

    #[test]
    fn cql_type_to_string_collections() {
        assert_eq!(
            cql_type_to_string(&CqlType::List(Box::new(CqlType::Int))),
            "list<int>"
        );
        assert_eq!(
            cql_type_to_string(&CqlType::Set(Box::new(CqlType::Varchar))),
            "set<text>"
        );
        assert_eq!(
            cql_type_to_string(&CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::Int)
            )),
            "map<text, int>"
        );
    }

    #[test]
    fn cql_type_to_string_tuple() {
        assert_eq!(
            cql_type_to_string(&CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])),
            "tuple<int, text>"
        );
    }

    #[test]
    fn cql_type_to_string_udt_reference() {
        let udt_ref = CqlType::Udt {
            keyspace: "ks1".to_string(),
            name: "address".to_string(),
            fields: vec![],
        };
        assert_eq!(cql_type_to_string(&udt_ref), "ks1.address");
    }
}
