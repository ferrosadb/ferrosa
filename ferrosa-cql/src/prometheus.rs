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
    output.push_str(&ferrosa_net::metrics::render_prometheus());
    output.push_str(&ferrosa_cluster::coordinator::metrics::render_prometheus());
    output.push_str(&ferrosa_storage::commitlog::render_prometheus());
    output.push_str(&ferrosa_storage::metrics::render_prometheus());

    // Process memory metrics — critical for diagnosing memory leaks.
    // On Linux (containers), read from /proc/self/status.
    if let Some(memory) = read_process_memory() {
        output.push_str(
            "# HELP ferrosa_process_resident_memory_bytes Resident set size (RSS) in bytes.\n",
        );
        output.push_str("# TYPE ferrosa_process_resident_memory_bytes gauge\n");
        output.push_str(&format_metric(
            "ferrosa_process_resident_memory_bytes",
            &[],
            memory.rss_bytes as f64,
        ));
        output.push_str(
            "# HELP ferrosa_process_virtual_memory_bytes Virtual memory size in bytes.\n",
        );
        output.push_str("# TYPE ferrosa_process_virtual_memory_bytes gauge\n");
        output.push_str(&format_metric(
            "ferrosa_process_virtual_memory_bytes",
            &[],
            memory.vsize_bytes as f64,
        ));
        output.push_str("# HELP ferrosa_process_memory_bytes Process memory from /proc/self/status by category.\n");
        output.push_str("# TYPE ferrosa_process_memory_bytes gauge\n");
        for (kind, bytes) in memory.as_labeled_bytes() {
            output.push_str(&format_metric(
                "ferrosa_process_memory_bytes",
                &[("kind", kind)],
                bytes as f64,
            ));
        }
    }

    if let Some(smaps) = read_process_smaps_rollup() {
        output.push_str("# HELP ferrosa_process_smaps_rollup_bytes Process memory accounting from /proc/self/smaps_rollup by category.\n");
        output.push_str("# TYPE ferrosa_process_smaps_rollup_bytes gauge\n");
        for (kind, bytes) in smaps.as_labeled_bytes() {
            output.push_str(&format_metric(
                "ferrosa_process_smaps_rollup_bytes",
                &[("kind", kind)],
                bytes as f64,
            ));
        }
    }

    if let Some(cgroup) = read_cgroup_memory() {
        output.push_str(
            "# HELP ferrosa_cgroup_memory_bytes Cgroup memory usage and limit in bytes.\n",
        );
        output.push_str("# TYPE ferrosa_cgroup_memory_bytes gauge\n");
        for (kind, bytes) in cgroup.as_labeled_bytes() {
            output.push_str(&format_metric(
                "ferrosa_cgroup_memory_bytes",
                &[("kind", kind)],
                bytes as f64,
            ));
        }
        output.push_str(
            "# HELP ferrosa_cgroup_memory_events_total Cgroup memory pressure and OOM events.\n",
        );
        output.push_str("# TYPE ferrosa_cgroup_memory_events_total counter\n");
        for (event, count) in &cgroup.events {
            output.push_str(&format_metric(
                "ferrosa_cgroup_memory_events_total",
                &[("event", event)],
                *count as f64,
            ));
        }
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

    let block_devices = read_block_device_counters();
    if !block_devices.is_empty() {
        output.push_str("# HELP ferrosa_host_block_device_io_total Block device IO operations from /proc/diskstats.\n");
        output.push_str("# TYPE ferrosa_host_block_device_io_total counter\n");
        output.push_str("# HELP ferrosa_host_block_device_io_bytes_total Block device IO bytes from /proc/diskstats, assuming 512-byte sectors.\n");
        output.push_str("# TYPE ferrosa_host_block_device_io_bytes_total counter\n");
        output.push_str("# HELP ferrosa_host_block_device_io_seconds_total Block device IO time from /proc/diskstats.\n");
        output.push_str("# TYPE ferrosa_host_block_device_io_seconds_total counter\n");
        output.push_str("# HELP ferrosa_host_block_device_in_flight Current in-flight IOs from /proc/diskstats.\n");
        output.push_str("# TYPE ferrosa_host_block_device_in_flight gauge\n");
        for device in block_devices {
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_total",
                &[("device", &device.name), ("direction", "read")],
                device.reads_completed as f64,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_total",
                &[("device", &device.name), ("direction", "write")],
                device.writes_completed as f64,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_bytes_total",
                &[("device", &device.name), ("direction", "read")],
                device.read_bytes as f64,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_bytes_total",
                &[("device", &device.name), ("direction", "write")],
                device.write_bytes as f64,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_seconds_total",
                &[("device", &device.name), ("direction", "read")],
                device.read_seconds,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_io_seconds_total",
                &[("device", &device.name), ("direction", "write")],
                device.write_seconds,
            ));
            output.push_str(&format_metric(
                "ferrosa_host_block_device_in_flight",
                &[("device", &device.name)],
                device.in_flight as f64,
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
#[derive(Debug, Clone, Copy, Default)]
struct ProcessMemory {
    rss_bytes: u64,
    vsize_bytes: u64,
    rss_hwm_bytes: u64,
    data_bytes: u64,
    stack_bytes: u64,
    locked_bytes: u64,
    pinned_bytes: u64,
    swap_bytes: u64,
}

impl ProcessMemory {
    fn as_labeled_bytes(&self) -> [(&'static str, u64); 8] {
        [
            ("rss", self.rss_bytes),
            ("vsize", self.vsize_bytes),
            ("rss_hwm", self.rss_hwm_bytes),
            ("data", self.data_bytes),
            ("stack", self.stack_bytes),
            ("locked", self.locked_bytes),
            ("pinned", self.pinned_bytes),
            ("swap", self.swap_bytes),
        ]
    }
}

fn read_process_memory() -> Option<ProcessMemory> {
    // Linux: /proc/self/status has VmRSS and VmSize in kB.
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let mut memory = ProcessMemory::default();
        for line in status.lines() {
            if let Some(value) = parse_proc_status_bytes(line, "VmRSS:") {
                memory.rss_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmSize:") {
                memory.vsize_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmHWM:") {
                memory.rss_hwm_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmData:") {
                memory.data_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmStk:") {
                memory.stack_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmLck:") {
                memory.locked_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmPin:") {
                memory.pinned_bytes = value;
            } else if let Some(value) = parse_proc_status_bytes(line, "VmSwap:") {
                memory.swap_bytes = value;
            }
        }
        return Some(memory);
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
        return Some(ProcessMemory {
            rss_bytes: rss_kb * 1024,
            vsize_bytes: vsz_kb * 1024,
            ..ProcessMemory::default()
        });
    }

    None
}

fn parse_proc_status_bytes(line: &str, key: &str) -> Option<u64> {
    line.strip_prefix(key)?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kb| kb * 1024)
}

#[derive(Debug, Clone, Copy, Default)]
struct SmapsRollup {
    rss_bytes: u64,
    pss_bytes: u64,
    shared_clean_bytes: u64,
    shared_dirty_bytes: u64,
    private_clean_bytes: u64,
    private_dirty_bytes: u64,
    referenced_bytes: u64,
    anonymous_bytes: u64,
    lazy_free_bytes: u64,
    anon_huge_pages_bytes: u64,
    swap_bytes: u64,
}

impl SmapsRollup {
    fn as_labeled_bytes(&self) -> [(&'static str, u64); 11] {
        [
            ("rss", self.rss_bytes),
            ("pss", self.pss_bytes),
            ("shared_clean", self.shared_clean_bytes),
            ("shared_dirty", self.shared_dirty_bytes),
            ("private_clean", self.private_clean_bytes),
            ("private_dirty", self.private_dirty_bytes),
            ("referenced", self.referenced_bytes),
            ("anonymous", self.anonymous_bytes),
            ("lazy_free", self.lazy_free_bytes),
            ("anon_huge_pages", self.anon_huge_pages_bytes),
            ("swap", self.swap_bytes),
        ]
    }
}

fn read_process_smaps_rollup() -> Option<SmapsRollup> {
    let text = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    Some(read_process_smaps_rollup_from_proc(&text))
}

fn read_process_smaps_rollup_from_proc(text: &str) -> SmapsRollup {
    let mut rollup = SmapsRollup::default();
    for line in text.lines() {
        if let Some(value) = parse_proc_status_bytes(line, "Rss:") {
            rollup.rss_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Pss:") {
            rollup.pss_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Shared_Clean:") {
            rollup.shared_clean_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Shared_Dirty:") {
            rollup.shared_dirty_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Private_Clean:") {
            rollup.private_clean_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Private_Dirty:") {
            rollup.private_dirty_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Referenced:") {
            rollup.referenced_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Anonymous:") {
            rollup.anonymous_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "LazyFree:") {
            rollup.lazy_free_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "AnonHugePages:") {
            rollup.anon_huge_pages_bytes = value;
        } else if let Some(value) = parse_proc_status_bytes(line, "Swap:") {
            rollup.swap_bytes = value;
        }
    }
    rollup
}

#[derive(Debug, Clone, Default)]
struct CgroupMemory {
    current_bytes: u64,
    max_bytes: u64,
    events: Vec<(String, u64)>,
}

impl CgroupMemory {
    fn as_labeled_bytes(&self) -> [(&'static str, u64); 2] {
        [("current", self.current_bytes), ("max", self.max_bytes)]
    }
}

fn read_cgroup_memory() -> Option<CgroupMemory> {
    let current = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    let max = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|text| parse_cgroup_memory_max(text.trim()));
    let events = std::fs::read_to_string("/sys/fs/cgroup/memory.events")
        .ok()
        .map(|text| read_cgroup_memory_events_from_proc(&text))
        .unwrap_or_default();
    Some(CgroupMemory {
        current_bytes: current,
        max_bytes: max.unwrap_or(0),
        events,
    })
}

fn parse_cgroup_memory_max(value: &str) -> Option<u64> {
    if value == "max" {
        Some(0)
    } else {
        value.parse().ok()
    }
}

fn read_cgroup_memory_events_from_proc(text: &str) -> Vec<(String, u64)> {
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let name = parts.next()?;
            let value = parts.next()?.parse::<u64>().ok()?;
            Some((name.to_string(), value))
        })
        .collect()
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

#[derive(Debug, Clone, PartialEq)]
struct BlockDeviceCounters {
    name: String,
    reads_completed: u64,
    read_bytes: u64,
    read_seconds: f64,
    writes_completed: u64,
    write_bytes: u64,
    write_seconds: f64,
    in_flight: u64,
}

fn read_block_device_counters() -> Vec<BlockDeviceCounters> {
    std::fs::read_to_string("/proc/diskstats")
        .ok()
        .map(|text| read_block_device_counters_from_proc(&text))
        .unwrap_or_default()
}

fn read_block_device_counters_from_proc(text: &str) -> Vec<BlockDeviceCounters> {
    text.lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 14 {
                return None;
            }
            let name = fields[2];
            if name.starts_with("loop") || name.starts_with("ram") {
                return None;
            }
            let reads_completed = fields[3].parse().ok()?;
            let sectors_read: u64 = fields[5].parse().ok()?;
            let read_ms: u64 = fields[6].parse().ok()?;
            let writes_completed = fields[7].parse().ok()?;
            let sectors_written: u64 = fields[9].parse().ok()?;
            let write_ms: u64 = fields[10].parse().ok()?;
            let in_flight = fields[11].parse().ok()?;
            Some(BlockDeviceCounters {
                name: name.to_string(),
                reads_completed,
                read_bytes: sectors_read.saturating_mul(512),
                read_seconds: read_ms as f64 / 1000.0,
                writes_completed,
                write_bytes: sectors_written.saturating_mul(512),
                write_seconds: write_ms as f64 / 1000.0,
                in_flight,
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
        assert!(
            output.contains("ferrosa_process_memory_bytes"),
            "metrics must include category-level process memory"
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
    fn process_status_memory_parser_extracts_kb_values() {
        assert_eq!(
            parse_proc_status_bytes("VmRSS:\t 123 kB", "VmRSS:"),
            Some(123 * 1024)
        );
        assert_eq!(
            parse_proc_status_bytes("VmData:\t456 kB", "VmData:"),
            Some(456 * 1024)
        );
        assert_eq!(parse_proc_status_bytes("VmRSS:\t 123 kB", "VmData:"), None);
    }

    #[test]
    fn smaps_rollup_parser_extracts_memory_categories() {
        let rollup = read_process_smaps_rollup_from_proc(
            "Rss:                100 kB\n\
             Pss:                 90 kB\n\
             Shared_Clean:        10 kB\n\
             Shared_Dirty:         2 kB\n\
             Private_Clean:        3 kB\n\
             Private_Dirty:       80 kB\n\
             Referenced:          95 kB\n\
             Anonymous:           70 kB\n\
             LazyFree:             1 kB\n\
             AnonHugePages:        0 kB\n\
             Swap:                 4 kB\n",
        );
        assert_eq!(rollup.rss_bytes, 100 * 1024);
        assert_eq!(rollup.pss_bytes, 90 * 1024);
        assert_eq!(rollup.private_dirty_bytes, 80 * 1024);
        assert_eq!(rollup.anonymous_bytes, 70 * 1024);
        assert_eq!(rollup.swap_bytes, 4 * 1024);
    }

    #[test]
    fn cgroup_memory_events_parser_extracts_counts() {
        let events =
            read_cgroup_memory_events_from_proc("low 0\nhigh 2\nmax 3\noom 4\noom_kill 5\n");
        assert_eq!(
            events,
            vec![
                ("low".to_string(), 0),
                ("high".to_string(), 2),
                ("max".to_string(), 3),
                ("oom".to_string(), 4),
                ("oom_kill".to_string(), 5),
            ]
        );
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
    fn diskstats_parser_extracts_block_device_counters() {
        let counters = read_block_device_counters_from_proc(
            "   7       0 loop0 1 0 2 3 4 0 5 6 0 8 9 0 0 0 0 0 0\n\
             254       0 vda 10 0 20 30 40 0 50 60 2 80 90 0 0 0 0 0 0\n",
        );
        assert_eq!(
            counters,
            vec![BlockDeviceCounters {
                name: "vda".to_string(),
                reads_completed: 10,
                read_bytes: 20 * 512,
                read_seconds: 0.030,
                writes_completed: 40,
                write_bytes: 50 * 512,
                write_seconds: 0.060,
                in_flight: 2,
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
        if std::path::Path::new("/proc/diskstats").exists() {
            assert!(output.contains("ferrosa_host_block_device_io_total"));
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
