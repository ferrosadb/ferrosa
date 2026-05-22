//! Prometheus text exposition format exporter.
//!
//! Converts virtual table data into Prometheus metric lines. The
//! `/metrics` endpoint (served by `ferrosa-net`) calls [`render_metrics`]
//! on each scrape to produce a complete metrics response.
//!
//! Convention: metric names follow `ferrosa_<table>_<column>`. Text
//! columns become labels; numeric columns (Int, BigInt, Double) become
//! metric values.

use ferrosa_common::DataType;
use ferrosa_schema::VirtualTableRegistry;

/// Format a single Prometheus metric line.
///
/// # Examples
///
/// ```
/// use ferrosa_cql::prometheus::format_metric;
///
/// let line = format_metric("ferrosa_connections_active", &[("state", "ready")], 42.0);
/// assert_eq!(line, "ferrosa_connections_active{state=\"ready\"} 42\n");
/// ```
pub fn format_metric(name: &str, labels: &[(&str, &str)], value: f64) -> String {
    if labels.is_empty() {
        format!("{name} {value}\n")
    } else {
        let label_str: String = labels
            .iter()
            .map(|(k, v)| format!("{k}=\"{v}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!("{name}{{{label_str}}} {value}\n")
    }
}

/// Render all virtual tables in `system_observability` as Prometheus
/// text exposition format.
///
/// For each row in each virtual table:
/// - Text columns become labels on the metric line.
/// - Numeric columns (Int, BigInt, Double) become separate metric
///   values, each emitting one line named `ferrosa_<table>_<column>`.
///
/// This is called on every Prometheus scrape — it reads from virtual
/// tables which return cached snapshots and never block.
pub fn render_metrics(registry: &VirtualTableRegistry) -> String {
    let mut output = String::new();

    // Baseline process metrics — always emitted so /metrics is never empty.
    output
        .push_str("# HELP ferrosa_up Whether the Ferrosa node is up (always 1 when reachable).\n");
    output.push_str("# TYPE ferrosa_up gauge\n");
    output.push_str(&format_metric("ferrosa_up", &[], 1.0));

    // Process memory metrics — critical for diagnosing memory leaks.
    // On Linux (containers), read from /proc/self/status.
    if let Some((rss, vsize)) = read_process_memory() {
        output.push_str(
            "# HELP ferrosa_process_resident_memory_bytes Resident set size (RSS) in bytes.\n",
        );
        output.push_str("# TYPE ferrosa_process_resident_memory_bytes gauge\n");
        output.push_str(&format_metric(
            "ferrosa_process_resident_memory_bytes",
            &[],
            rss as f64,
        ));
        output.push_str(
            "# HELP ferrosa_process_virtual_memory_bytes Virtual memory size in bytes.\n",
        );
        output.push_str("# TYPE ferrosa_process_virtual_memory_bytes gauge\n");
        output.push_str(&format_metric(
            "ferrosa_process_virtual_memory_bytes",
            &[],
            vsize as f64,
        ));
    }

    // Get all tables from system_observability keyspace
    let tables = registry.list("system_observability");

    output.push_str("# HELP ferrosa_virtual_tables_registered Number of registered observability virtual tables.\n");
    output.push_str("# TYPE ferrosa_virtual_tables_registered gauge\n");
    output.push_str(&format_metric(
        "ferrosa_virtual_tables_registered",
        &[],
        tables.len() as f64,
    ));

    for table in &tables {
        let table_name = table.name();
        let columns = table.columns();
        let rows = table.read(None);

        // For each row, extract numeric columns as metrics
        // Use text columns as labels
        for row in &rows {
            let mut labels = Vec::new();

            // Collect text columns as labels
            for (i, col) in columns.iter().enumerate() {
                if col.data_type == DataType::Text {
                    if let Some(bytes) = row.cells.get(i).and_then(|c| c.value.as_deref()) {
                        if let Ok(s) = std::str::from_utf8(bytes) {
                            labels.push((col.name.as_str(), s));
                        }
                    }
                }
            }

            // Emit numeric columns as metrics
            for (i, col) in columns.iter().enumerate() {
                if col.data_type.is_numeric() {
                    if let Some(bytes) = row.cells.get(i).and_then(|c| c.value.as_deref()) {
                        let value = match col.data_type {
                            DataType::Int => {
                                if bytes.len() >= 4 {
                                    i32::from_be_bytes(bytes[..4].try_into().unwrap_or_default())
                                        as f64
                                } else {
                                    continue;
                                }
                            }
                            DataType::BigInt => {
                                if bytes.len() >= 8 {
                                    i64::from_be_bytes(bytes[..8].try_into().unwrap_or_default())
                                        as f64
                                } else {
                                    continue;
                                }
                            }
                            DataType::Double => {
                                if bytes.len() >= 8 {
                                    f64::from_be_bytes(bytes[..8].try_into().unwrap_or_default())
                                } else {
                                    continue;
                                }
                            }
                            _ => continue,
                        };

                        let metric_name = format!("ferrosa_{table_name}_{}", col.name);
                        let label_refs: Vec<(&str, &str)> =
                            labels.iter().map(|(k, v)| (*k, *v)).collect();
                        output.push_str(&format_metric(&metric_name, &label_refs, value));
                    }
                }
            }
        }
    }

    output
}

/// Read the current process's RSS and virtual memory size.
///
/// Returns `(rss_bytes, vsize_bytes)`. On Linux reads `/proc/self/status`;
/// on macOS reads `/proc/{pid}/status`-equivalent via `ps`. Returns `None`
/// on unsupported platforms or if the read fails.
fn read_process_memory() -> Option<(u64, u64)> {
    // Linux: /proc/self/status has VmRSS and VmSize in kB.
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let mut rss_kb = 0u64;
        let mut vsize_kb = 0u64;
        for line in status.lines() {
            if let Some(val) = line.strip_prefix("VmRSS:") {
                rss_kb = val.trim().trim_end_matches(" kB").trim().parse().ok()?;
            } else if let Some(val) = line.strip_prefix("VmSize:") {
                vsize_kb = val.trim().trim_end_matches(" kB").trim().parse().ok()?;
            }
        }
        return Some((rss_kb * 1024, vsize_kb * 1024));
    }

    // macOS fallback: use `ps` to read RSS (no libc dependency).
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=,vsz=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts.len() >= 2 {
        let rss_kb: u64 = parts[0].parse().ok()?;
        let vsz_kb: u64 = parts[1].parse().ok()?;
        return Some((rss_kb * 1024, vsz_kb * 1024));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::CellValue;
    use ferrosa_schema::{
        RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
        VirtualTableRegistry,
    };
    use std::sync::Arc;

    struct StubMetricsTable {
        name: &'static str,
        columns: Vec<VirtualColumnDef>,
        rows: Vec<VirtualRow>,
    }

    impl VirtualTable for StubMetricsTable {
        fn name(&self) -> &str {
            self.name
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
        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
            self.rows.clone()
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn format_gauge_metric() {
        let line = format_metric("ferrosa_connections_active", &[("state", "ready")], 42.0);
        assert_eq!(line, "ferrosa_connections_active{state=\"ready\"} 42\n");
    }

    #[test]
    fn format_metric_no_labels() {
        let line = format_metric("ferrosa_total", &[], 100.0);
        assert_eq!(line, "ferrosa_total 100\n");
    }

    #[test]
    fn format_metric_multiple_labels() {
        let line = format_metric(
            "ferrosa_requests",
            &[("host", "node1"), ("dc", "us-east-1")],
            99.0,
        );
        assert_eq!(
            line,
            "ferrosa_requests{host=\"node1\",dc=\"us-east-1\"} 99\n"
        );
    }

    #[test]
    fn render_metrics_from_virtual_table() {
        let registry = VirtualTableRegistry::new();

        let table = StubMetricsTable {
            name: "test_metrics",
            columns: vec![
                VirtualColumnDef {
                    name: "host".to_owned(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "count".to_owned(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"node1".to_vec(), 0),
                    CellValue::live(42i32.to_be_bytes().to_vec(), 0),
                ],
            }],
        };

        registry.register(Arc::new(table));
        let output = render_metrics(&registry);
        assert!(output.contains("ferrosa_test_metrics_count"));
        assert!(output.contains("host=\"node1\""));
        assert!(output.contains("42"));
    }

    #[test]
    fn render_metrics_empty_registry_has_baseline() {
        let registry = VirtualTableRegistry::new();
        let output = render_metrics(&registry);
        // Even with no virtual tables, baseline metrics are always emitted.
        assert!(output.contains("ferrosa_up"));
        assert!(output.contains("ferrosa_virtual_tables_registered"));
    }

    #[test]
    fn render_metrics_exposes_fd_budget_and_emfile() {
        // Operators need these three families visible on every scrape
        // so the macOS-launchd EMFILE failure mode (see spec
        // p0-emfile-launchd-startup) shows up in dashboards.
        let registry = VirtualTableRegistry::new();
        let output = render_metrics(&registry);
        assert!(output.contains("ferrosa_emfile_total"));
        assert!(output.contains("ferrosa_fd_budget_soft"));
        assert!(output.contains("ferrosa_fd_budget_hard"));
    }

    #[test]
    fn render_metrics_includes_process_memory() {
        let registry = VirtualTableRegistry::new();
        let output = render_metrics(&registry);
        assert!(
            output.contains("ferrosa_process_resident_memory_bytes"),
            "metrics must include RSS for memory leak diagnosis"
        );
        assert!(
            output.contains("ferrosa_process_virtual_memory_bytes"),
            "metrics must include vsize for memory leak diagnosis"
        );
    }

    #[test]
    fn render_metrics_bigint_column() {
        let registry = VirtualTableRegistry::new();

        let table = StubMetricsTable {
            name: "storage",
            columns: vec![
                VirtualColumnDef {
                    name: "keyspace".to_owned(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "bytes_used".to_owned(),
                    data_type: DataType::BigInt,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"my_ks".to_vec(), 0),
                    CellValue::live(1_000_000i64.to_be_bytes().to_vec(), 0),
                ],
            }],
        };

        registry.register(Arc::new(table));
        let output = render_metrics(&registry);
        assert!(output.contains("ferrosa_storage_bytes_used"));
        assert!(output.contains("keyspace=\"my_ks\""));
        assert!(output.contains("1000000"));
    }

    #[test]
    fn render_metrics_double_column() {
        let registry = VirtualTableRegistry::new();

        let table = StubMetricsTable {
            name: "host_metrics",
            columns: vec![
                VirtualColumnDef {
                    name: "host".to_owned(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "cpu_percent".to_owned(),
                    data_type: DataType::Double,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"node1".to_vec(), 0),
                    CellValue::live(75.5f64.to_be_bytes().to_vec(), 0),
                ],
            }],
        };

        registry.register(Arc::new(table));
        let output = render_metrics(&registry);
        assert!(output.contains("ferrosa_host_metrics_cpu_percent"));
        assert!(output.contains("host=\"node1\""));
        assert!(output.contains("75.5"));
    }

    #[test]
    fn render_metrics_skips_non_observability_keyspace() {
        let registry = VirtualTableRegistry::new();

        struct OtherKeyspaceTable;
        impl VirtualTable for OtherKeyspaceTable {
            fn name(&self) -> &str {
                "other"
            }
            fn keyspace(&self) -> &str {
                "system"
            }
            fn columns(&self) -> &[VirtualColumnDef] {
                &[]
            }
            fn primary_key_columns(&self) -> &[usize] {
                &[]
            }
            fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
                vec![]
            }
            fn subscription_mode(&self) -> SubscriptionMode {
                SubscriptionMode::Pollable
            }
        }

        registry.register(Arc::new(OtherKeyspaceTable));
        let output = render_metrics(&registry);
        // Baseline metrics are always present, but no table-specific metrics
        // should appear from a non-observability keyspace table.
        assert!(output.contains("ferrosa_up"));
        assert!(!output.contains("ferrosa_other_"));
    }

    #[test]
    fn render_metrics_skips_tombstoned_cells() {
        let registry = VirtualTableRegistry::new();

        let table = StubMetricsTable {
            name: "test",
            columns: vec![
                VirtualColumnDef {
                    name: "label".to_owned(),
                    data_type: DataType::Text,
                },
                VirtualColumnDef {
                    name: "value".to_owned(),
                    data_type: DataType::Int,
                },
            ],
            rows: vec![VirtualRow {
                cells: vec![
                    CellValue::live(b"node1".to_vec(), 0),
                    // Tombstone: no value
                    CellValue::tombstone(0, 0),
                ],
            }],
        };

        registry.register(Arc::new(table));
        let output = render_metrics(&registry);
        // Tombstoned numeric cell should be skipped
        assert!(!output.contains("ferrosa_test_value"));
    }
}
