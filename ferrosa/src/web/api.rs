use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use ferrosa_common::DataType;
use ferrosa_schema::VirtualTableRegistry;
use serde_json::{json, Value};

type AppState = Arc<VirtualTableRegistry>;

pub fn routes(registry: Arc<VirtualTableRegistry>) -> Router {
    Router::new()
        .route("/connections", get(get_connections))
        .route("/storage_stats", get(get_storage_stats))
        .route("/active_queries", get(get_active_queries))
        .route("/tables", get(list_tables))
        .with_state(registry)
}

async fn get_connections(State(registry): State<AppState>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "connections"))
}

async fn get_storage_stats(State(registry): State<AppState>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "storage_stats"))
}

async fn get_active_queries(State(registry): State<AppState>) -> Json<Value> {
    Json(virtual_table_to_json(&registry, "active_queries"))
}

async fn list_tables(State(registry): State<AppState>) -> Json<Value> {
    let tables = registry.list("system_observability");
    let names: Vec<&str> = tables.iter().map(|t| t.name()).collect();
    Json(json!(names))
}

fn virtual_table_to_json(registry: &VirtualTableRegistry, table_name: &str) -> Value {
    let table = match registry.get("system_observability", table_name) {
        Some(t) => t,
        None => return json!([]),
    };

    let columns = table.columns();
    let rows = table.read(None);

    let json_rows: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(cell) = row.cells.get(i) {
                    if let Some(bytes) = cell.value.as_deref() {
                        let val = match col.data_type {
                            DataType::Text => {
                                Value::String(String::from_utf8_lossy(bytes).to_string())
                            }
                            DataType::Int => {
                                if bytes.len() >= 4 {
                                    Value::Number(
                                        i32::from_be_bytes(
                                            bytes[..4].try_into().unwrap_or_default(),
                                        )
                                        .into(),
                                    )
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::BigInt | DataType::Timestamp => {
                                if bytes.len() >= 8 {
                                    Value::Number(
                                        i64::from_be_bytes(
                                            bytes[..8].try_into().unwrap_or_default(),
                                        )
                                        .into(),
                                    )
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::Double => {
                                if bytes.len() >= 8 {
                                    let f = f64::from_be_bytes(
                                        bytes[..8].try_into().unwrap_or_default(),
                                    );
                                    serde_json::Number::from_f64(f)
                                        .map(Value::Number)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            }
                            DataType::Boolean => Value::Bool(!bytes.is_empty() && bytes[0] != 0),
                            _ => Value::String("<binary>".to_string()),
                        };
                        obj.insert(col.name.clone(), val);
                    } else {
                        obj.insert(col.name.clone(), Value::Null);
                    }
                }
            }
            Value::Object(obj)
        })
        .collect();

    json!(json_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::CellValue;
    use ferrosa_schema::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
    };

    struct StubTable {
        cols: Vec<VirtualColumnDef>,
        rows: Vec<VirtualRow>,
    }

    impl VirtualTable for StubTable {
        fn name(&self) -> &str {
            "test_table"
        }
        fn keyspace(&self) -> &str {
            "system_observability"
        }
        fn columns(&self) -> &[VirtualColumnDef] {
            &self.cols
        }
        fn primary_key_columns(&self) -> &[usize] {
            &[0]
        }
        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
            self.rows.clone()
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn virtual_table_to_json_empty() {
        let registry = VirtualTableRegistry::new();
        let result = virtual_table_to_json(&registry, "nonexistent");
        assert_eq!(result, json!([]));
    }

    #[test]
    fn virtual_table_to_json_text_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "host".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(b"127.0.0.1".to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["host"], "127.0.0.1");
    }

    #[test]
    fn virtual_table_to_json_int_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "count".to_string(),
                data_type: DataType::Int,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(42i32.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["count"], 42);
    }

    #[test]
    fn virtual_table_to_json_bigint_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "total".to_string(),
                data_type: DataType::BigInt,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(1_000_000i64.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["total"], 1_000_000);
    }

    #[test]
    fn virtual_table_to_json_double_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "ratio".to_string(),
                data_type: DataType::Double,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(1.5f64.to_be_bytes().to_vec(), 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert!((rows[0]["ratio"].as_f64().unwrap() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn virtual_table_to_json_boolean_column() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(vec![1], 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["active"], true);
    }

    #[test]
    fn virtual_table_to_json_null_value() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "host".to_string(),
                data_type: DataType::Text,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::tombstone(1, 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["host"], Value::Null);
    }

    #[test]
    fn virtual_table_to_json_blob_shows_binary() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![VirtualColumnDef {
                name: "data".to_string(),
                data_type: DataType::Blob,
            }],
            rows: vec![VirtualRow {
                cells: vec![CellValue::live(vec![0xDE, 0xAD], 1)],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["data"], "<binary>");
    }

    #[test]
    fn virtual_table_to_json_multiple_columns() {
        let registry = VirtualTableRegistry::new();
        let table = StubTable {
            cols: vec![
                VirtualColumnDef {
                    name: "host".to_string(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "port".to_string(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"localhost".to_vec(), 1),
                    CellValue::live(9042i32.to_be_bytes().to_vec(), 1),
                ],
            }],
        };
        registry.register(Arc::new(table));

        let result = virtual_table_to_json(&registry, "test_table");
        let rows = result.as_array().unwrap();
        assert_eq!(rows[0]["host"], "localhost");
        assert_eq!(rows[0]["port"], 9042);
    }
}
