//! Materialization observability virtual tables.

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::{RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable};
use std::sync::{Arc, RwLock};

use ferrosa_storage::StorageEngine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationQueueSnapshot {
    pub keyspace_name: String,
    pub table_name: String,
    pub target_table: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub task_type: String,
    pub enqueued_at_ms: i64,
    pub oldest_task_age_ms: i64,
    pub queue_depth: i64,
    pub retry_count: i64,
    pub last_error: Option<String>,
    pub max_delay_ms: i64,
    pub alerting: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationStatusSnapshot {
    pub keyspace_name: String,
    pub table_name: String,
    pub target_table: String,
    pub status: String,
    pub pending_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub stale_drops_total: i64,
    pub last_materialized_window_end_ms: Option<i64>,
    pub last_error: Option<String>,
}

pub trait MaterializationObservabilityProvider: Send + Sync {
    fn visit_queue_snapshots(&self, visit: &mut dyn FnMut(&MaterializationQueueSnapshot));

    fn visit_status_snapshots(&self, visit: &mut dyn FnMut(&MaterializationStatusSnapshot));
}

/// Storage-backed materialization provider used by live system_observability tables.
pub struct StorageMaterializationProvider {
    storage: Arc<StorageEngine>,
}

impl StorageMaterializationProvider {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

impl MaterializationObservabilityProvider for StorageMaterializationProvider {
    fn visit_queue_snapshots(&self, visit: &mut dyn FnMut(&MaterializationQueueSnapshot)) {
        self.storage
            .visit_time_series_materialization_queues(&mut |snapshot| {
                let mapped = MaterializationQueueSnapshot {
                    keyspace_name: snapshot.source_table.keyspace().to_string(),
                    table_name: snapshot.source_table.table().to_string(),
                    target_table: snapshot.target_table.table().to_string(),
                    window_start_ms: micros_to_millis(snapshot.window_start_ts),
                    window_end_ms: micros_to_millis(snapshot.window_end_ts),
                    task_type: snapshot.task_type,
                    enqueued_at_ms: snapshot.enqueued_at_ms,
                    oldest_task_age_ms: snapshot.oldest_task_age_ms,
                    queue_depth: snapshot.queue_depth,
                    retry_count: snapshot.retry_count,
                    last_error: snapshot.last_error,
                    max_delay_ms: snapshot.max_delay_ms,
                    alerting: snapshot.alerting,
                };
                visit(&mapped);
            });
    }

    fn visit_status_snapshots(&self, visit: &mut dyn FnMut(&MaterializationStatusSnapshot)) {
        self.storage
            .visit_time_series_materialization_statuses(&mut |snapshot| {
                let mapped = MaterializationStatusSnapshot {
                    keyspace_name: snapshot.source_table.keyspace().to_string(),
                    table_name: snapshot.source_table.table().to_string(),
                    target_table: snapshot.target_table.table().to_string(),
                    status: snapshot.status,
                    pending_tasks: snapshot.pending_tasks,
                    completed_tasks: snapshot.completed_tasks,
                    failed_tasks: snapshot.failed_tasks,
                    stale_drops_total: snapshot.stale_drops_total,
                    last_materialized_window_end_ms: snapshot
                        .last_materialized_window_end_ms
                        .map(micros_to_millis),
                    last_error: snapshot.last_error,
                };
                visit(&mapped);
            });
    }
}

fn micros_to_millis(value: i64) -> i64 {
    value / 1_000
}

#[derive(Default)]
pub struct InMemoryMaterializationProvider {
    queues: RwLock<Vec<MaterializationQueueSnapshot>>,
    statuses: RwLock<Vec<MaterializationStatusSnapshot>>,
}

impl InMemoryMaterializationProvider {
    pub fn set_queue(&self, snapshot: MaterializationQueueSnapshot) {
        self.queues
            .write()
            .expect("materialization queue lock poisoned")
            .push(snapshot);
    }

    pub fn set_status(&self, snapshot: MaterializationStatusSnapshot) {
        self.statuses
            .write()
            .expect("materialization status lock poisoned")
            .push(snapshot);
    }
}

impl MaterializationObservabilityProvider for InMemoryMaterializationProvider {
    fn visit_queue_snapshots(&self, visit: &mut dyn FnMut(&MaterializationQueueSnapshot)) {
        let guard = self
            .queues
            .read()
            .expect("materialization queue lock poisoned");
        for snapshot in guard.iter() {
            visit(snapshot);
        }
    }

    fn visit_status_snapshots(&self, visit: &mut dyn FnMut(&MaterializationStatusSnapshot)) {
        let guard = self
            .statuses
            .read()
            .expect("materialization status lock poisoned");
        for snapshot in guard.iter() {
            visit(snapshot);
        }
    }
}

pub struct MaterializationQueuesTable {
    provider: Arc<dyn MaterializationObservabilityProvider>,
    columns: Vec<VirtualColumnDef>,
}

impl MaterializationQueuesTable {
    pub fn new(provider: Arc<dyn MaterializationObservabilityProvider>) -> Self {
        Self {
            provider,
            columns: queue_columns(),
        }
    }
}

impl VirtualTable for MaterializationQueuesTable {
    fn name(&self) -> &str {
        "materialization_queues"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1, 2, 3, 5]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let mut rows = Vec::new();
        self.visit_rows(None, &mut |row| rows.push(row));
        rows
    }

    fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
        self.provider.visit_queue_snapshots(&mut |snapshot| {
            visit(queue_virtual_row(snapshot));
        });
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

pub struct MaterializationStatusTable {
    provider: Arc<dyn MaterializationObservabilityProvider>,
    columns: Vec<VirtualColumnDef>,
}

impl MaterializationStatusTable {
    pub fn new(provider: Arc<dyn MaterializationObservabilityProvider>) -> Self {
        Self {
            provider,
            columns: status_columns(),
        }
    }
}

impl VirtualTable for MaterializationStatusTable {
    fn name(&self) -> &str {
        "materialization_status"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1, 2]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let mut rows = Vec::new();
        self.visit_rows(None, &mut |row| rows.push(row));
        rows
    }

    fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
        self.provider.visit_status_snapshots(&mut |snapshot| {
            visit(status_virtual_row(snapshot));
        });
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

fn queue_columns() -> Vec<VirtualColumnDef> {
    vec![
        col("keyspace_name", DataType::Text),
        col("table_name", DataType::Text),
        col("target_table", DataType::Text),
        col("window_start_ms", DataType::BigInt),
        col("window_end_ms", DataType::BigInt),
        col("task_type", DataType::Text),
        col("enqueued_at_ms", DataType::BigInt),
        col("oldest_task_age_ms", DataType::BigInt),
        col("queue_depth", DataType::BigInt),
        col("retry_count", DataType::BigInt),
        col("last_error", DataType::Text),
        col("max_delay_ms", DataType::BigInt),
        col("alerting", DataType::Boolean),
    ]
}

fn status_columns() -> Vec<VirtualColumnDef> {
    vec![
        col("keyspace_name", DataType::Text),
        col("table_name", DataType::Text),
        col("target_table", DataType::Text),
        col("status", DataType::Text),
        col("pending_tasks", DataType::BigInt),
        col("completed_tasks", DataType::BigInt),
        col("failed_tasks", DataType::BigInt),
        col("stale_drops_total", DataType::BigInt),
        col("last_materialized_window_end_ms", DataType::BigInt),
        col("last_error", DataType::Text),
    ]
}

fn queue_virtual_row(snapshot: &MaterializationQueueSnapshot) -> VirtualRow {
    VirtualRow {
        cells: vec![
            text(&snapshot.keyspace_name),
            text(&snapshot.table_name),
            text(&snapshot.target_table),
            bigint(snapshot.window_start_ms),
            bigint(snapshot.window_end_ms),
            text(&snapshot.task_type),
            bigint(snapshot.enqueued_at_ms),
            bigint(snapshot.oldest_task_age_ms),
            bigint(snapshot.queue_depth),
            bigint(snapshot.retry_count),
            optional_text(snapshot.last_error.as_deref()),
            bigint(snapshot.max_delay_ms),
            boolean(snapshot.alerting),
        ],
    }
}

fn status_virtual_row(snapshot: &MaterializationStatusSnapshot) -> VirtualRow {
    VirtualRow {
        cells: vec![
            text(&snapshot.keyspace_name),
            text(&snapshot.table_name),
            text(&snapshot.target_table),
            text(&snapshot.status),
            bigint(snapshot.pending_tasks),
            bigint(snapshot.completed_tasks),
            bigint(snapshot.failed_tasks),
            bigint(snapshot.stale_drops_total),
            optional_bigint(snapshot.last_materialized_window_end_ms),
            optional_text(snapshot.last_error.as_deref()),
        ],
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

fn optional_bigint(value: Option<i64>) -> CellValue {
    value
        .map(bigint)
        .unwrap_or_else(|| CellValue::tombstone(0, 0))
}

fn optional_text(value: Option<&str>) -> CellValue {
    value
        .map(text)
        .unwrap_or_else(|| CellValue::tombstone(0, 0))
}

fn boolean(value: bool) -> CellValue {
    CellValue::live(vec![u8::from(value)], 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::DataType;
    use ferrosa_schema::{SubscriptionMode, VirtualTable};
    use std::sync::Arc;

    fn text_cell(row: &ferrosa_schema::VirtualRow, idx: usize) -> String {
        String::from_utf8(row.cells[idx].value.clone().expect("text cell")).unwrap()
    }

    fn bigint_cell(row: &ferrosa_schema::VirtualRow, idx: usize) -> i64 {
        i64::from_be_bytes(
            row.cells[idx]
                .value
                .clone()
                .expect("bigint cell")
                .try_into()
                .unwrap(),
        )
    }

    fn bool_cell(row: &ferrosa_schema::VirtualRow, idx: usize) -> bool {
        row.cells[idx].value.as_deref() == Some(&[1])
    }

    #[test]
    fn materialization_queues_has_stable_schema_and_rows() {
        let provider = Arc::new(InMemoryMaterializationProvider::default());
        provider.set_queue(MaterializationQueueSnapshot {
            keyspace_name: "plant".to_string(),
            table_name: "sensor_readings_raw".to_string(),
            target_table: "sensor_readings_5m".to_string(),
            window_start_ms: 1_774_032_000_000,
            window_end_ms: 1_774_032_300_000,
            task_type: "window_close".to_string(),
            enqueued_at_ms: 1_774_032_301_000,
            oldest_task_age_ms: 15_000,
            queue_depth: 3,
            retry_count: 2,
            last_error: Some("wasm timeout".to_string()),
            max_delay_ms: 10_000,
            alerting: true,
        });

        let table = MaterializationQueuesTable::new(provider);
        let names: Vec<&str> = table.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "keyspace_name",
                "table_name",
                "target_table",
                "window_start_ms",
                "window_end_ms",
                "task_type",
                "enqueued_at_ms",
                "oldest_task_age_ms",
                "queue_depth",
                "retry_count",
                "last_error",
                "max_delay_ms",
                "alerting",
            ]
        );
        assert_eq!(table.columns()[8].data_type, DataType::BigInt);
        assert_eq!(table.columns()[12].data_type, DataType::Boolean);
        assert_eq!(table.primary_key_columns(), &[0, 1, 2, 3, 5]);
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));

        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells.len(), table.columns().len());
        assert_eq!(text_cell(&rows[0], 0), "plant");
        assert_eq!(text_cell(&rows[0], 2), "sensor_readings_5m");
        assert_eq!(bigint_cell(&rows[0], 8), 3);
        assert_eq!(bigint_cell(&rows[0], 9), 2);
        assert_eq!(text_cell(&rows[0], 10), "wasm timeout");
        assert!(bool_cell(&rows[0], 12));
    }

    #[test]
    fn materialization_status_has_stable_schema_and_rows() {
        let provider = Arc::new(InMemoryMaterializationProvider::default());
        provider.set_status(MaterializationStatusSnapshot {
            keyspace_name: "plant".to_string(),
            table_name: "sensor_readings_raw".to_string(),
            target_table: "sensor_readings_5m".to_string(),
            status: "degraded".to_string(),
            pending_tasks: 3,
            completed_tasks: 42,
            failed_tasks: 1,
            stale_drops_total: 4,
            last_materialized_window_end_ms: Some(1_774_032_300_000),
            last_error: Some("wasm timeout".to_string()),
        });

        let table = MaterializationStatusTable::new(provider);
        let names: Vec<&str> = table.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "keyspace_name",
                "table_name",
                "target_table",
                "status",
                "pending_tasks",
                "completed_tasks",
                "failed_tasks",
                "stale_drops_total",
                "last_materialized_window_end_ms",
                "last_error",
            ]
        );
        assert_eq!(table.primary_key_columns(), &[0, 1, 2]);
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));

        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(text_cell(&rows[0], 3), "degraded");
        assert_eq!(bigint_cell(&rows[0], 4), 3);
        assert_eq!(bigint_cell(&rows[0], 5), 42);
        assert_eq!(bigint_cell(&rows[0], 8), 1_774_032_300_000);
        assert_eq!(text_cell(&rows[0], 9), "wasm timeout");
    }
}
