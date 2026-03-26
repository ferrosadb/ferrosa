//! `system_schema.aggregates` virtual table.
//!
//! Provides a virtual table that exposes user-defined aggregate metadata from
//! the schema snapshot. Compatible with Cassandra's `system_schema.aggregates`
//! table layout for CQL driver introspection.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_common::{CellValue, CqlType, CqlValue, DataType};

use crate::registry::SchemaSnapshot;
use crate::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Virtual table implementation for `system_schema.aggregates`.
///
/// Reads the current schema snapshot's aggregates map and materializes
/// rows on demand. The snapshot is shared via `Arc<ArcSwap<SchemaSnapshot>>`
/// for lock-free reads.
pub struct SystemSchemaAggregatesTable {
    snapshot: Arc<ArcSwap<SchemaSnapshot>>,
    columns: Vec<VirtualColumnDef>,
}

impl SystemSchemaAggregatesTable {
    /// Create a new `system_schema.aggregates` virtual table backed by the
    /// given snapshot handle.
    pub fn new(snapshot: Arc<ArcSwap<SchemaSnapshot>>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "aggregate_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "argument_types".to_string(),
                data_type: DataType::Text, // list<text> serialized
            },
            VirtualColumnDef {
                name: "state_func".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "state_type".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "final_func".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "initcond".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "return_type".to_string(),
                data_type: DataType::Text,
            },
        ];
        Self { snapshot, columns }
    }
}

/// Convert a `CqlType` to its CQL type string representation.
///
/// Produces lowercase CQL type names matching Cassandra's `system_schema.aggregates`
/// column format.
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
        CqlType::Vector(elem, dim) => {
            format!("vector<{}, {}>", cql_type_to_string(elem), dim)
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

/// Convert a `CqlValue` to its CQL literal string representation.
///
/// Used for the `initcond` column in `system_schema.aggregates`.
fn cql_value_to_literal(val: &CqlValue) -> String {
    match val {
        CqlValue::Null => "null".to_string(),
        CqlValue::Int(v) => v.to_string(),
        CqlValue::Bigint(v) => v.to_string(),
        CqlValue::Smallint(v) => v.to_string(),
        CqlValue::Tinyint(v) => v.to_string(),
        CqlValue::Float(bits) => format!("{}", f32::from_bits(*bits)),
        CqlValue::Double(bits) => format!("{}", f64::from_bits(*bits)),
        CqlValue::Boolean(v) => v.to_string(),
        CqlValue::Text(v) | CqlValue::Ascii(v) => v.clone(),
        CqlValue::Tuple(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| match f {
                    Some(v) => cql_value_to_literal(v),
                    None => "null".to_string(),
                })
                .collect();
            format!("({})", inner.join(", "))
        }
        _ => format!("{val:?}"),
    }
}

impl VirtualTable for SystemSchemaAggregatesTable {
    fn name(&self) -> &str {
        "aggregates"
    }

    fn keyspace(&self) -> &str {
        "system_schema"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        // keyspace_name (0), aggregate_name (1)
        &[0, 1]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let snap = self.snapshot.load_full();
        let mut rows = Vec::new();

        for ((_ks, _name, _atypes), agg) in &snap.aggregates {
            let arg_types: Vec<String> = agg.arg_types.iter().map(cql_type_to_string).collect();
            let arg_types_str = serialize_string_list(&arg_types);
            let state_type_str = cql_type_to_string(&agg.state_type);
            let final_func_str = agg.final_func.as_deref().unwrap_or("");
            let init_cond_str = agg
                .init_cond
                .as_ref()
                .map(cql_value_to_literal)
                .unwrap_or_default();
            let return_type_str = cql_type_to_string(&agg.return_type);

            rows.push(VirtualRow {
                cells: vec![
                    CellValue::live(agg.keyspace.as_bytes().to_vec(), 0),
                    CellValue::live(agg.name.as_bytes().to_vec(), 0),
                    CellValue::live(arg_types_str.into_bytes(), 0),
                    CellValue::live(agg.state_func.as_bytes().to_vec(), 0),
                    CellValue::live(state_type_str.into_bytes(), 0),
                    CellValue::live(final_func_str.as_bytes().to_vec(), 0),
                    CellValue::live(init_cond_str.as_bytes().to_vec(), 0),
                    CellValue::live(return_type_str.into_bytes(), 0),
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
    use crate::metadata::aggregate::UserAggregateMetadata;
    use ferrosa_common::CqlType;
    use ferrosa_common::CqlValue;

    fn empty_snapshot() -> Arc<ArcSwap<SchemaSnapshot>> {
        Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())))
    }

    fn snapshot_with_aggregate(agg: UserAggregateMetadata) -> Arc<ArcSwap<SchemaSnapshot>> {
        let mut snap = SchemaSnapshot::new();
        let key = (
            agg.keyspace.clone(),
            agg.name.clone(),
            agg.arg_types.clone(),
        );
        snap.aggregates.insert(key, agg);
        Arc::new(ArcSwap::new(Arc::new(snap)))
    }

    #[test]
    fn aggregates_table_columns() {
        let snap = empty_snapshot();
        let table = SystemSchemaAggregatesTable::new(snap);
        let cols = table.columns();
        assert_eq!(cols.len(), 8);
        assert_eq!(cols[0].name, "keyspace_name");
        assert_eq!(cols[1].name, "aggregate_name");
        assert_eq!(cols[2].name, "argument_types");
        assert_eq!(cols[3].name, "state_func");
        assert_eq!(cols[4].name, "state_type");
        assert_eq!(cols[5].name, "final_func");
        assert_eq!(cols[6].name, "initcond");
        assert_eq!(cols[7].name, "return_type");
    }

    #[test]
    fn aggregates_table_empty_snapshot() {
        let snap = empty_snapshot();
        let table = SystemSchemaAggregatesTable::new(snap);
        let rows = table.read(None);
        assert!(rows.is_empty());
    }

    #[test]
    fn aggregates_table_returns_rows() {
        let agg = UserAggregateMetadata {
            keyspace: "ks1".to_string(),
            name: "avg_int".to_string(),
            arg_types: vec![CqlType::Int],
            state_func: "avg_state".to_string(),
            state_type: CqlType::Tuple(vec![CqlType::Bigint, CqlType::Int]),
            final_func: Some("avg_final".to_string()),
            init_cond: Some(CqlValue::Tuple(vec![
                Some(CqlValue::Bigint(0)),
                Some(CqlValue::Int(0)),
            ])),
            return_type: CqlType::Double,
            wasm_body: None,
        };
        let snap = snapshot_with_aggregate(agg);
        let table = SystemSchemaAggregatesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.cells.len(), 8);
        // keyspace_name
        assert_eq!(row.cells[0].value.as_deref(), Some(b"ks1".as_slice()));
        // aggregate_name
        assert_eq!(row.cells[1].value.as_deref(), Some(b"avg_int".as_slice()));
        // argument_types — JSON list
        let arg_types = std::str::from_utf8(row.cells[2].value.as_deref().unwrap()).unwrap();
        assert_eq!(arg_types, r#"["int"]"#);
        // state_func
        assert_eq!(row.cells[3].value.as_deref(), Some(b"avg_state".as_slice()));
        // state_type
        let state_type = std::str::from_utf8(row.cells[4].value.as_deref().unwrap()).unwrap();
        assert_eq!(state_type, "tuple<bigint, int>");
        // final_func
        assert_eq!(row.cells[5].value.as_deref(), Some(b"avg_final".as_slice()));
        // initcond
        assert_eq!(row.cells[6].value.as_deref(), Some(b"(0, 0)".as_slice()));
        // return_type
        assert_eq!(row.cells[7].value.as_deref(), Some(b"double".as_slice()));
    }

    #[test]
    fn aggregates_table_without_final_func() {
        let agg = UserAggregateMetadata {
            keyspace: "ks1".to_string(),
            name: "sum_int".to_string(),
            arg_types: vec![CqlType::Int],
            state_func: "sum_state".to_string(),
            state_type: CqlType::Int,
            final_func: None,
            init_cond: Some(CqlValue::Int(0)),
            return_type: CqlType::Int,
            wasm_body: None,
        };
        let snap = snapshot_with_aggregate(agg);
        let table = SystemSchemaAggregatesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        // final_func should be empty
        assert_eq!(row.cells[5].value.as_deref(), Some(b"".as_slice()));
        // initcond should be "0"
        assert_eq!(row.cells[6].value.as_deref(), Some(b"0".as_slice()));
    }

    #[test]
    fn aggregates_table_metadata() {
        let snap = empty_snapshot();
        let table = SystemSchemaAggregatesTable::new(snap);
        assert_eq!(table.name(), "aggregates");
        assert_eq!(table.keyspace(), "system_schema");
        assert_eq!(table.primary_key_columns(), &[0, 1]);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::None));
    }

    #[test]
    fn aggregates_table_without_init_cond() {
        let agg = UserAggregateMetadata {
            keyspace: "ks1".to_string(),
            name: "count_all".to_string(),
            arg_types: vec![CqlType::Int],
            state_func: "count_state".to_string(),
            state_type: CqlType::Bigint,
            final_func: None,
            init_cond: None,
            return_type: CqlType::Bigint,
            wasm_body: None,
        };
        let snap = snapshot_with_aggregate(agg);
        let table = SystemSchemaAggregatesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        // final_func should be empty
        assert_eq!(row.cells[5].value.as_deref(), Some(b"".as_slice()));
        // initcond should be empty
        assert_eq!(row.cells[6].value.as_deref(), Some(b"".as_slice()));
    }
}
