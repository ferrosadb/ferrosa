//! Runtime control table for RRD/time-series materialization settings.

use std::sync::Arc;

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::{
    PredicateOp, RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualColumnUpdate, VirtualRow,
    VirtualTable, VirtualTableUpdate,
};
use ferrosa_storage::timeseries::TimeSeriesRuntimeSettings;

pub struct RrdRuntimeSettingsTable {
    settings: Arc<TimeSeriesRuntimeSettings>,
    columns: Vec<VirtualColumnDef>,
}

impl RrdRuntimeSettingsTable {
    pub fn new(settings: Arc<TimeSeriesRuntimeSettings>) -> Self {
        Self {
            settings,
            columns: vec![
                col("setting_name", DataType::Text),
                col("setting_value", DataType::BigInt),
                col("source", DataType::Text),
            ],
        }
    }

    fn update_setting(
        &self,
        setting_name: &str,
        assignment: &VirtualColumnUpdate,
    ) -> Result<(), String> {
        if assignment.column != "setting_value" {
            return Err("rrd_runtime_settings only allows setting_value updates".to_string());
        }
        let value = decode_bigint(&assignment.value)?;
        if value < 0 {
            return Err("setting_value must be non-negative".to_string());
        }

        match setting_name {
            "ring_memory_budget_bytes" => self
                .settings
                .set_ring_memory_budget_bytes(Some(value as usize)),
            "ring_thrash_warn_evictions" => {
                self.settings.set_ring_thrash_warn_evictions(value as u64)
            }
            other => return Err(format!("unknown RRD runtime setting: {other}")),
        }
        Ok(())
    }
}

impl VirtualTable for RrdRuntimeSettingsTable {
    fn name(&self) -> &str {
        "rrd_runtime_settings"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        vec![
            row(
                "ring_memory_budget_bytes",
                self.settings.ring_memory_budget_bytes().unwrap_or(0) as i64,
                "runtime",
            ),
            row(
                "ring_thrash_warn_evictions",
                self.settings.ring_thrash_warn_evictions() as i64,
                "runtime",
            ),
        ]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }

    fn apply_update(&self, update: &VirtualTableUpdate) -> Result<(), String> {
        let setting_name = extract_setting_name(&update.predicate)?;
        if update.assignments.len() != 1 {
            return Err("rrd_runtime_settings updates exactly one setting_value".to_string());
        }
        self.update_setting(&setting_name, &update.assignments[0])
    }
}

fn extract_setting_name(predicate: &RowPredicate) -> Result<String, String> {
    for filter in &predicate.filters {
        if filter.column == "setting_name" && filter.op == PredicateOp::Eq {
            return decode_text(&filter.value);
        }
    }
    Err("rrd_runtime_settings requires WHERE setting_name = ...".to_string())
}

fn row(setting_name: &str, setting_value: i64, source: &str) -> VirtualRow {
    VirtualRow {
        cells: vec![text(setting_name), bigint(setting_value), text(source)],
    }
}

fn col(name: &str, data_type: DataType) -> VirtualColumnDef {
    VirtualColumnDef {
        name: name.to_string(),
        data_type,
    }
}

fn text(value: &str) -> CellValue {
    CellValue::live(value.as_bytes().to_vec(), 0)
}

fn bigint(value: i64) -> CellValue {
    CellValue::live(value.to_be_bytes().to_vec(), 0)
}

fn decode_text(cell: &CellValue) -> Result<String, String> {
    let bytes = cell.value.as_ref().ok_or("setting_name must not be null")?;
    String::from_utf8(bytes.clone()).map_err(|_| "setting_name must be UTF-8 text".to_string())
}

fn decode_bigint(cell: &CellValue) -> Result<i64, String> {
    let bytes = cell
        .value
        .as_ref()
        .ok_or("setting_value must not be null")?;
    let arr: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "setting_value must be a bigint".to_string())?;
    Ok(i64::from_be_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::{ColumnFilter, PredicateOp, VirtualTable};

    #[test]
    fn rrd_runtime_settings_updates_ring_budget() {
        let settings = Arc::new(TimeSeriesRuntimeSettings::new(Some(1024), 100));
        let table = RrdRuntimeSettingsTable::new(Arc::clone(&settings));

        table
            .apply_update(&VirtualTableUpdate {
                assignments: vec![VirtualColumnUpdate {
                    column: "setting_value".to_string(),
                    value: bigint(4096),
                }],
                predicate: RowPredicate {
                    filters: vec![ColumnFilter {
                        column: "setting_name".to_string(),
                        op: PredicateOp::Eq,
                        value: text("ring_memory_budget_bytes"),
                    }],
                },
            })
            .unwrap();

        assert_eq!(settings.ring_memory_budget_bytes(), Some(4096));
    }

    #[test]
    fn rrd_runtime_settings_rejects_unknown_setting() {
        let settings = Arc::new(TimeSeriesRuntimeSettings::new(Some(1024), 100));
        let table = RrdRuntimeSettingsTable::new(settings);

        let err = table
            .apply_update(&VirtualTableUpdate {
                assignments: vec![VirtualColumnUpdate {
                    column: "setting_value".to_string(),
                    value: bigint(4096),
                }],
                predicate: RowPredicate {
                    filters: vec![ColumnFilter {
                        column: "setting_name".to_string(),
                        op: PredicateOp::Eq,
                        value: text("unknown"),
                    }],
                },
            })
            .unwrap_err();

        assert!(err.contains("unknown"));
    }
}
