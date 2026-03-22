//! `system_observability.consolidation_status` virtual table.
//!
//! Exposes the status of time-series consolidation configurations.
//! Each row represents one table with active consolidation extensions.

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::{
    RowPredicate, Schema, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};
use std::sync::Arc;

/// Virtual table exposing consolidation status for tables with RRD extensions.
pub struct ConsolidationStatusTable {
    schema: Arc<Schema>,
    columns: Vec<VirtualColumnDef>,
}

impl ConsolidationStatusTable {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            columns: make_columns(),
        }
    }
}

fn make_columns() -> Vec<VirtualColumnDef> {
    vec![
        VirtualColumnDef {
            name: "keyspace_name".to_string(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "table_name".to_string(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "interval".to_string(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "target_table".to_string(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "functions".to_string(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "status".to_string(),
            data_type: DataType::Text,
        },
    ]
}

impl VirtualTable for ConsolidationStatusTable {
    fn name(&self) -> &str {
        "consolidation_status"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1] // keyspace_name, table_name
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let snap = self.schema.snapshot();
        let mut rows = Vec::new();

        for ((ks, tbl), meta) in &snap.tables {
            // Check if table has consolidation extensions.
            let has_consolidation = meta
                .extensions
                .keys()
                .any(|k| k.starts_with("consolidation."));
            if !has_consolidation {
                continue;
            }

            let interval = meta
                .extensions
                .get("consolidation.interval")
                .cloned()
                .unwrap_or_default();
            let target = meta
                .extensions
                .get("consolidation.target")
                .cloned()
                .unwrap_or_default();
            let functions = meta
                .extensions
                .get("consolidation.functions")
                .cloned()
                .unwrap_or_default();

            rows.push(VirtualRow {
                cells: vec![
                    CellValue::live(ks.as_bytes().to_vec(), 0),
                    CellValue::live(tbl.as_bytes().to_vec(), 0),
                    CellValue::live(interval.as_bytes().to_vec(), 0),
                    CellValue::live(target.as_bytes().to_vec(), 0),
                    CellValue::live(functions.as_bytes().to_vec(), 0),
                    CellValue::live("active".as_bytes().to_vec(), 0),
                ],
            });
        }

        rows
    }
}
