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
use std::sync::atomic::{AtomicU64, Ordering};

static EMFILE_TOTAL: AtomicU64 = AtomicU64::new(0);

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
            .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
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

    // File descriptor pressure metrics — always emitted so the
    // macOS-launchd low-NOFILE failure mode is visible even before a
    // table snapshot is available.
    let (fd_soft, fd_hard) = read_fd_budget().unwrap_or((0, 0));
    output.push_str(
        "# HELP ferrosa_fd_budget_soft Soft process file descriptor limit (RLIMIT_NOFILE).\n",
    );
    output.push_str("# TYPE ferrosa_fd_budget_soft gauge\n");
    output.push_str(&format_metric(
        "ferrosa_fd_budget_soft",
        &[],
        fd_soft as f64,
    ));
    output.push_str(
        "# HELP ferrosa_fd_budget_hard Hard process file descriptor limit (RLIMIT_NOFILE).\n",
    );
    output.push_str("# TYPE ferrosa_fd_budget_hard gauge\n");
    output.push_str(&format_metric(
        "ferrosa_fd_budget_hard",
        &[],
        fd_hard as f64,
    ));
    output.push_str("# HELP ferrosa_emfile_total Total observed EMFILE open-file-limit errors.\n");
    output.push_str("# TYPE ferrosa_emfile_total counter\n");
    output.push_str(&format_metric(
        "ferrosa_emfile_total",
        &[],
        emfile_total() as f64,
    ));

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

    if let Some(cpu_seconds) = read_process_cpu_seconds() {
        output.push_str(
            "# HELP ferrosa_process_cpu_seconds_total Total user and system CPU time spent by this process.\n",
        );
        output.push_str("# TYPE ferrosa_process_cpu_seconds_total counter\n");
        output.push_str(&format_metric(
            "ferrosa_process_cpu_seconds_total",
            &[],
            cpu_seconds,
        ));
    }

    if let Some(io) = read_process_io() {
        output.push_str("# HELP ferrosa_process_io_read_bytes_total Bytes read from storage by this process, from /proc/self/io where available.\n");
        output.push_str("# TYPE ferrosa_process_io_read_bytes_total counter\n");
        output.push_str(&format_metric(
            "ferrosa_process_io_read_bytes_total",
            &[],
            io.read_bytes as f64,
        ));
        output.push_str("# HELP ferrosa_process_io_write_bytes_total Bytes written to storage by this process, from /proc/self/io where available.\n");
        output.push_str("# TYPE ferrosa_process_io_write_bytes_total counter\n");
        output.push_str(&format_metric(
            "ferrosa_process_io_write_bytes_total",
            &[],
            io.write_bytes as f64,
        ));
        output.push_str(
            "# HELP ferrosa_process_io_read_syscalls_total Read syscalls issued by this process.\n",
        );
        output.push_str("# TYPE ferrosa_process_io_read_syscalls_total counter\n");
        output.push_str(&format_metric(
            "ferrosa_process_io_read_syscalls_total",
            &[],
            io.read_syscalls as f64,
        ));
        output.push_str("# HELP ferrosa_process_io_write_syscalls_total Write syscalls issued by this process.\n");
        output.push_str("# TYPE ferrosa_process_io_write_syscalls_total counter\n");
        output.push_str(&format_metric(
            "ferrosa_process_io_write_syscalls_total",
            &[],
            io.write_syscalls as f64,
        ));
    }

    let net = read_host_network_counters();
    if !net.is_empty() {
        output.push_str("# HELP ferrosa_host_network_receive_bytes_total Host network receive bytes by interface, from /proc/net/dev where available.\n");
        output.push_str("# TYPE ferrosa_host_network_receive_bytes_total counter\n");
        output.push_str("# HELP ferrosa_host_network_transmit_bytes_total Host network transmit bytes by interface, from /proc/net/dev where available.\n");
        output.push_str("# TYPE ferrosa_host_network_transmit_bytes_total counter\n");
        for iface in net {
            output.push_str(&format_metric(
                "ferrosa_host_network_receive_bytes_total",
                &[("interface", &iface.name)],
                iface.receive_bytes as f64,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_network_transmit_bytes_total",
                &[("interface", &iface.name)],
                iface.transmit_bytes as f64,
            ));
        }
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

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

/// Record an EMFILE ("too many open files") failure for metrics.
pub fn record_emfile_error() {
    EMFILE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn emfile_total() -> u64 {
    EMFILE_TOTAL.load(Ordering::Relaxed)
}

#[cfg(unix)]
fn read_fd_budget() -> Option<(u64, u64)> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: getrlimit initializes `limit` when it returns 0.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if rc == 0 {
        // SAFETY: guarded by the successful getrlimit return code.
        let limit = unsafe { limit.assume_init() };
        Some((limit.rlim_cur, limit.rlim_max))
    } else {
        None
    }
}

#[cfg(not(unix))]
fn read_fd_budget() -> Option<(u64, u64)> {
    None
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

fn read_process_cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    read_process_cpu_seconds_from_stat(&stat)
}

fn read_process_cpu_seconds_from_stat(stat: &str) -> Option<f64> {
    let end_comm = stat.rfind(") ")?;
    let fields: Vec<&str> = stat[end_comm + 2..].split_whitespace().collect();
    let utime_ticks: u64 = fields.get(11)?.parse().ok()?;
    let stime_ticks: u64 = fields.get(12)?.parse().ok()?;
    // SAFETY: sysconf does not dereference pointers and `_SC_CLK_TCK` has no
    // side effects; a non-positive return is treated as unavailable.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_second <= 0 {
        return None;
    }
    Some((utime_ticks + stime_ticks) as f64 / ticks_per_second as f64)
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessIo {
    read_bytes: u64,
    write_bytes: u64,
    read_syscalls: u64,
    write_syscalls: u64,
}

fn read_process_io() -> Option<ProcessIo> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    Some(read_process_io_from_proc(&text))
}

fn read_process_io_from_proc(text: &str) -> ProcessIo {
    let mut io = ProcessIo::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().unwrap_or(0);
        match key {
            "read_bytes" => io.read_bytes = value,
            "write_bytes" => io.write_bytes = value,
            "syscr" => io.read_syscalls = value,
            "syscw" => io.write_syscalls = value,
            _ => {}
        }
    }
    io
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NetworkCounters {
    name: String,
    receive_bytes: u64,
    transmit_bytes: u64,
}

fn read_host_network_counters() -> Vec<NetworkCounters> {
    std::fs::read_to_string("/proc/net/dev")
        .ok()
        .map(|text| read_host_network_counters_from_proc(&text))
        .unwrap_or_default()
}

fn read_host_network_counters_from_proc(text: &str) -> Vec<NetworkCounters> {
    text.lines()
        .filter_map(|line| {
            let (name, data) = line.split_once(':')?;
            let name = name.trim();
            if name.is_empty() || name == "lo" {
                return None;
            }
            let fields: Vec<&str> = data.split_whitespace().collect();
            Some(NetworkCounters {
                name: name.to_string(),
                receive_bytes: fields.first()?.parse().ok()?,
                transmit_bytes: fields.get(8)?.parse().ok()?,
            })
        })
        .collect()
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
    fn record_emfile_error_increments_counter() {
        let before = emfile_total();
        record_emfile_error();

        let registry = VirtualTableRegistry::new();
        let output = render_metrics(&registry);

        assert!(output.contains(&format!("ferrosa_emfile_total {}", before + 1)));
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
    fn format_metric_escapes_label_values() {
        let line = format_metric("ferrosa_test_metric", &[("path", "a\\b\"c\nnext")], 1.0);
        assert_eq!(line, "ferrosa_test_metric{path=\"a\\\\b\\\"c\\nnext\"} 1\n");
    }

    #[test]
    fn process_cpu_stat_parser_handles_command_with_spaces() {
        let stat = "123 (ferrosa worker) S 1 2 3 4 5 6 7 8 9 10 11 12 300 500 16 17 18";
        let seconds = read_process_cpu_seconds_from_stat(stat).unwrap();
        assert!(seconds > 0.0);
    }

    #[test]
    fn process_io_parser_extracts_read_write_counters() {
        let io = read_process_io_from_proc(
            "rchar: 10\nwchar: 20\nsyscr: 3\nsyscw: 4\nread_bytes: 4096\nwrite_bytes: 8192\n",
        );
        assert_eq!(io.read_bytes, 4096);
        assert_eq!(io.write_bytes, 8192);
        assert_eq!(io.read_syscalls, 3);
        assert_eq!(io.write_syscalls, 4);
    }

    #[test]
    fn network_parser_skips_loopback_and_extracts_bytes() {
        let counters = read_host_network_counters_from_proc(
            "Inter-| Receive | Transmit\n\
             face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n\
                lo: 1 0 0 0 0 0 0 0 2 0 0 0 0 0 0 0\n\
              eth0: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0\n",
        );
        assert_eq!(
            counters,
            vec![NetworkCounters {
                name: "eth0".to_string(),
                receive_bytes: 100,
                transmit_bytes: 200,
            }]
        );
    }

    #[test]
    fn render_metrics_includes_cpu_io_and_network_when_available() {
        let registry = VirtualTableRegistry::new();
        let output = render_metrics(&registry);
        if std::path::Path::new("/proc/self/stat").exists() {
            assert!(output.contains("ferrosa_process_cpu_seconds_total"));
        }
        if std::path::Path::new("/proc/self/io").exists() {
            assert!(output.contains("ferrosa_process_io_read_bytes_total"));
        }
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
