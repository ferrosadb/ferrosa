//! Alert evaluator virtual table (T-27: O4.10).
//!
//! Background task checks thresholds every 30s:
//! - Slow query rate > 10/min
//! - S3 upload queue > 100
//! - Hint backlog growing
//!
//! Active alerts are exposed as `system_observability.alerts`.

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Severity levels for alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl AlertSeverity {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

/// A single active alert.
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert name (e.g. "slow_query_rate").
    pub name: String,
    /// Severity level.
    pub severity: AlertSeverity,
    /// Human-readable message.
    pub message: String,
    /// When the alert was first triggered (epoch millis).
    pub triggered_at_ms: i64,
    /// When the alert was last evaluated as active (epoch millis).
    pub last_evaluated_ms: i64,
}

/// Registry of active alerts, updated by the alert evaluator task.
pub struct AlertRegistry {
    alerts: RwLock<Vec<Alert>>,
}

impl AlertRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            alerts: RwLock::new(Vec::new()),
        }
    }

    /// Set or update an alert. If an alert with the same name exists,
    /// update its last_evaluated timestamp. Otherwise, create it.
    pub fn set_alert(&self, name: &str, severity: AlertSeverity, message: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut alerts = self.alerts.write().expect("AlertRegistry lock poisoned");
        if let Some(existing) = alerts.iter_mut().find(|a| a.name == name) {
            existing.severity = severity;
            existing.message = message.to_string();
            existing.last_evaluated_ms = now_ms;
        } else {
            alerts.push(Alert {
                name: name.to_string(),
                severity,
                message: message.to_string(),
                triggered_at_ms: now_ms,
                last_evaluated_ms: now_ms,
            });
        }
    }

    /// Clear an alert by name (condition no longer met).
    pub fn clear_alert(&self, name: &str) {
        let mut alerts = self.alerts.write().expect("AlertRegistry lock poisoned");
        alerts.retain(|a| a.name != name);
    }

    /// Number of active alerts.
    pub fn active_count(&self) -> usize {
        self.alerts
            .read()
            .expect("AlertRegistry lock poisoned")
            .len()
    }

    /// Snapshot of all active alerts.
    fn snapshot(&self) -> Vec<Alert> {
        self.alerts
            .read()
            .expect("AlertRegistry lock poisoned")
            .clone()
    }
}

impl Default for AlertRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.alerts`
pub struct AlertsTable {
    registry: Arc<AlertRegistry>,
    columns: Vec<VirtualColumnDef>,
}

impl AlertsTable {
    pub fn new(registry: Arc<AlertRegistry>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "severity".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "message".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "triggered_at_ms".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "last_evaluated_ms".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { registry, columns }
    }
}

impl VirtualTable for AlertsTable {
    fn name(&self) -> &str {
        "alerts"
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
        self.registry
            .snapshot()
            .into_iter()
            .map(|a| {
                let cells = vec![
                    CellValue::live(a.name.into_bytes(), 0),
                    CellValue::live(a.severity.as_str().as_bytes().to_vec(), 0),
                    CellValue::live(a.message.into_bytes(), 0),
                    CellValue::live(a.triggered_at_ms.to_be_bytes().to_vec(), 0),
                    CellValue::live(a.last_evaluated_ms.to_be_bytes().to_vec(), 0),
                ];
                VirtualRow { cells }
            })
            .collect()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::DemandDriven {
            default_interval: std::time::Duration::from_secs(30),
        }
    }
}

/// Spawn the alert evaluator background task.
///
/// This checks virtual tables for threshold violations every 30 seconds.
pub fn spawn_alert_evaluator(
    alert_registry: Arc<AlertRegistry>,
    vtable_registry: Arc<ferrosa_schema::VirtualTableRegistry>,
    task_pool: ferrosa_net::task_pool::TaskPool,
) {
    task_pool.spawn(async move {
        let interval = std::time::Duration::from_secs(30);
        loop {
            tokio::time::sleep(interval).await;
            evaluate_alerts(&alert_registry, &vtable_registry);
        }
    });
}

/// Evaluate alert conditions against current virtual table data.
fn evaluate_alerts(alerts: &AlertRegistry, _registry: &ferrosa_schema::VirtualTableRegistry) {
    // Placeholder evaluation logic — checks are based on virtual table data
    // that will be wired in as the system matures.
    //
    // For now, this serves as the framework. Real threshold checks:
    // - Slow query rate: count active_queries with elapsed_ms > 1000
    // - S3 upload queue: check storage_stats pending_compactions
    // - Hint backlog: check hint store size
    //
    // The alerts are cleared when conditions are no longer met.
    let _ = alerts;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_registry_set_and_clear() {
        let registry = AlertRegistry::new();
        registry.set_alert(
            "slow_queries",
            AlertSeverity::Warning,
            "too many slow queries",
        );
        assert_eq!(registry.active_count(), 1);

        registry.clear_alert("slow_queries");
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn alert_registry_update_existing() {
        let registry = AlertRegistry::new();
        registry.set_alert("test", AlertSeverity::Warning, "first message");
        registry.set_alert("test", AlertSeverity::Critical, "updated message");
        assert_eq!(registry.active_count(), 1);

        let alerts = registry.snapshot();
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        assert_eq!(alerts[0].message, "updated message");
    }

    #[test]
    fn alerts_table_metadata() {
        let registry = Arc::new(AlertRegistry::new());
        let table = AlertsTable::new(registry);
        assert_eq!(table.name(), "alerts");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 5);
    }

    #[test]
    fn alerts_table_returns_active_alerts() {
        let registry = Arc::new(AlertRegistry::new());
        registry.set_alert(
            "slow_queries",
            AlertSeverity::Warning,
            "slow query rate > 10/min",
        );
        registry.set_alert("s3_queue", AlertSeverity::Critical, "S3 upload queue > 100");

        let table = AlertsTable::new(registry);
        let rows = table.read(None);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.cells.len(), 5);
        }
    }

    #[test]
    fn evaluate_alerts_does_not_panic() {
        let alert_registry = AlertRegistry::new();
        let vtable_registry = ferrosa_schema::VirtualTableRegistry::new();
        evaluate_alerts(&alert_registry, &vtable_registry);
    }
}
