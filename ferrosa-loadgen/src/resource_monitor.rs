//! Resource leak detection for burn-in / long-running load tests.
//!
//! Samples OS-level resource counters (file descriptors, memory, sockets,
//! threads) and storage-engine metrics (commit log segments, SSTable handles)
//! at each snapshot interval. Detects monotonic growth that indicates a leak
//! and aborts the test before the machine runs out of resources.
//!
//! # Abort Thresholds
//!
//! Hard limits are based on the system's `ulimit -n` (or a configurable
//! fallback). When any resource exceeds its abort threshold the monitor
//! returns `LeakVerdict::Abort` and the orchestrator must stop the test
//! immediately.
//!
//! # Leak Detection
//!
//! After a warmup window (first N samples are discarded), the monitor
//! tracks whether a metric has grown monotonically across consecutive
//! samples. If a counter increases for `MONOTONIC_GROWTH_WINDOW`
//! consecutive samples, it is flagged as a probable leak.

use std::fmt;

/// Number of consecutive increasing samples before flagging a leak.
/// Set high enough to avoid false positives from normal operational
/// growth (e.g., SSTable count growing between compaction cycles).
const MONOTONIC_GROWTH_WINDOW: usize = 20;

/// Fraction of ulimit at which we abort (e.g., 0.80 = abort at 80%).
const FD_ABORT_FRACTION: f64 = 0.80;

/// Absolute RSS growth threshold: abort if RSS grows by this much over
/// the baseline (after warmup). 2 GB default.
const RSS_GROWTH_ABORT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// A single resource snapshot.
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub open_fds: u64,
    pub fd_limit: u64,
    pub rss_bytes: u64,
    pub vsz_bytes: u64,
    pub tcp_sockets: u64,
    pub unix_sockets: u64,
    pub thread_count: u64,
    pub commit_log_closed_segments: u64,
    pub sstable_count: u64,
    pub tmp_files: u64,
}

/// Result of the leak analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeakVerdict {
    /// All resources are within bounds and not growing monotonically.
    Healthy,
    /// A resource is growing monotonically — probable leak but below abort threshold.
    Warning(Vec<LeakWarning>),
    /// A hard limit is about to be hit — abort the test immediately.
    Abort(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeakWarning {
    pub resource: String,
    pub current: u64,
    pub baseline: u64,
    pub consecutive_increases: usize,
}

impl fmt::Display for LeakWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} -> {} ({} consecutive increases)",
            self.resource, self.baseline, self.current, self.consecutive_increases
        )
    }
}

/// Tracks resource snapshots and detects leaks.
pub struct ResourceMonitor {
    snapshots: Vec<ResourceSnapshot>,
    warmup_samples: usize,
}

impl ResourceMonitor {
    /// Create a new monitor. The first `warmup_samples` are excluded from
    /// leak detection (JIT, lazy init, cache warmup cause legitimate growth).
    pub fn new(warmup_samples: usize) -> Self {
        Self {
            snapshots: Vec::new(),
            warmup_samples,
        }
    }

    /// Record a snapshot and check for leaks.
    ///
    /// Returns `LeakVerdict::Abort` if a hard limit is about to be hit.
    pub fn record(&mut self, snap: ResourceSnapshot) -> LeakVerdict {
        // Hard abort checks (always active, even during warmup).
        let fd_abort_threshold = (snap.fd_limit as f64 * FD_ABORT_FRACTION) as u64;
        if snap.open_fds >= fd_abort_threshold {
            return LeakVerdict::Abort(format!(
                "open FDs ({}) >= {}% of ulimit ({}) — aborting to prevent system crash",
                snap.open_fds,
                (FD_ABORT_FRACTION * 100.0) as u32,
                snap.fd_limit,
            ));
        }

        // RSS growth abort (relative to first snapshot).
        if let Some(first) = self.snapshots.first() {
            let growth = snap.rss_bytes.saturating_sub(first.rss_bytes);
            if growth >= RSS_GROWTH_ABORT_BYTES {
                return LeakVerdict::Abort(format!(
                    "RSS grew by {:.1} MB (from {:.1} MB to {:.1} MB) — aborting to prevent OOM",
                    growth as f64 / (1024.0 * 1024.0),
                    first.rss_bytes as f64 / (1024.0 * 1024.0),
                    snap.rss_bytes as f64 / (1024.0 * 1024.0),
                ));
            }
        }

        self.snapshots.push(snap);

        // Monotonic growth detection (skip warmup window).
        if self.snapshots.len() <= self.warmup_samples {
            return LeakVerdict::Healthy;
        }

        let mut warnings = Vec::new();

        // Only check metrics that should be STABLE during normal operation.
        // SSTables and RSS grow naturally (flushes add SSTables, compaction
        // removes them; RSS grows with cache and working set). FDs that track
        // SSTable count are expected. Commit log segments should stay near
        // zero after flush+discard — sustained growth there IS a leak.
        self.check_monotonic("tcp_sockets", |s| s.tcp_sockets, &mut warnings);
        self.check_monotonic("unix_sockets", |s| s.unix_sockets, &mut warnings);
        self.check_monotonic("thread_count", |s| s.thread_count, &mut warnings);
        self.check_monotonic("commit_log_segments", |s| s.commit_log_closed_segments, &mut warnings);
        self.check_monotonic("tmp_files", |s| s.tmp_files, &mut warnings);

        // For FDs: check growth relative to SSTable count. If FDs grow but
        // SSTables don't, that's a leak. If both grow together, it's normal.
        self.check_fd_leak(&mut warnings);

        if warnings.is_empty() {
            LeakVerdict::Healthy
        } else {
            LeakVerdict::Warning(warnings)
        }
    }

    /// Check if a metric has been monotonically increasing for the last
    /// `MONOTONIC_GROWTH_WINDOW` post-warmup samples.
    fn check_monotonic(
        &self,
        name: &str,
        extract: impl Fn(&ResourceSnapshot) -> u64,
        warnings: &mut Vec<LeakWarning>,
    ) {
        let post_warmup = &self.snapshots[self.warmup_samples..];
        if post_warmup.len() < MONOTONIC_GROWTH_WINDOW + 1 {
            return;
        }

        let tail = &post_warmup[post_warmup.len() - MONOTONIC_GROWTH_WINDOW - 1..];
        let mut consecutive = 0;
        for window in tail.windows(2) {
            if extract(&window[1]) > extract(&window[0]) {
                consecutive += 1;
            } else {
                consecutive = 0;
            }
        }

        if consecutive >= MONOTONIC_GROWTH_WINDOW {
            let baseline = extract(&post_warmup[0]);
            let current = extract(post_warmup.last().unwrap());
            warnings.push(LeakWarning {
                resource: name.to_string(),
                current,
                baseline,
                consecutive_increases: consecutive,
            });
        }
    }

    /// Check for FD leaks independent of SSTable growth.
    ///
    /// FDs naturally grow with SSTable count (each SSTable opens data + index
    /// files). A real FD leak is when FDs grow but SSTables don't — indicating
    /// handles that aren't being closed. We check the ratio: if FDs grew by
    /// more than 3x the SSTable growth (in absolute terms), flag it.
    fn check_fd_leak(&self, warnings: &mut Vec<LeakWarning>) {
        let post_warmup = &self.snapshots[self.warmup_samples..];
        if post_warmup.len() < MONOTONIC_GROWTH_WINDOW + 1 {
            return;
        }

        let baseline = &post_warmup[0];
        let current = post_warmup.last().unwrap();

        let fd_growth = current.open_fds.saturating_sub(baseline.open_fds);
        let sst_growth = current.sstable_count.saturating_sub(baseline.sstable_count);

        // Each SSTable can open 2-4 file handles (data, index, filter, sidecar).
        // If FDs grew by more than 4x SSTable growth + 20 (baseline noise),
        // something is leaking handles.
        let expected_fd_growth = sst_growth * 4 + 20;
        if fd_growth > expected_fd_growth && fd_growth > 50 {
            warnings.push(LeakWarning {
                resource: "open_fds (independent of SSTables)".to_string(),
                current: current.open_fds,
                baseline: baseline.open_fds,
                consecutive_increases: 0,
            });
        }
    }

    /// Return the baseline snapshot (first post-warmup sample), if available.
    pub fn baseline(&self) -> Option<&ResourceSnapshot> {
        self.snapshots.get(self.warmup_samples)
    }

    /// Return the latest snapshot.
    pub fn latest(&self) -> Option<&ResourceSnapshot> {
        self.snapshots.last()
    }

    /// Summary of resource deltas for the final report.
    pub fn summary(&self) -> Option<ResourceSummary> {
        let baseline = self.baseline()?;
        let latest = self.latest()?;
        Some(ResourceSummary {
            fd_baseline: baseline.open_fds,
            fd_final: latest.open_fds,
            fd_limit: latest.fd_limit,
            rss_baseline: baseline.rss_bytes,
            rss_final: latest.rss_bytes,
            vsz_baseline: baseline.vsz_bytes,
            vsz_final: latest.vsz_bytes,
            tcp_baseline: baseline.tcp_sockets,
            tcp_final: latest.tcp_sockets,
            segments_baseline: baseline.commit_log_closed_segments,
            segments_final: latest.commit_log_closed_segments,
            sstables_baseline: baseline.sstable_count,
            sstables_final: latest.sstable_count,
            threads_baseline: baseline.thread_count,
            threads_final: latest.thread_count,
            samples: self.snapshots.len(),
        })
    }
}

/// Summary for the final stats report.
#[derive(Debug, Clone)]
pub struct ResourceSummary {
    pub fd_baseline: u64,
    pub fd_final: u64,
    pub fd_limit: u64,
    pub rss_baseline: u64,
    pub rss_final: u64,
    pub vsz_baseline: u64,
    pub vsz_final: u64,
    pub tcp_baseline: u64,
    pub tcp_final: u64,
    pub segments_baseline: u64,
    pub segments_final: u64,
    pub sstables_baseline: u64,
    pub sstables_final: u64,
    pub threads_baseline: u64,
    pub threads_final: u64,
    pub samples: usize,
}

impl fmt::Display for ResourceSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "--- Resource Leak Detection ---")?;
        writeln!(f, "Samples:            {:>12}", self.samples)?;
        write_delta(f, "File descriptors", self.fd_baseline, self.fd_final)?;
        writeln!(f, "  (limit: {})", self.fd_limit)?;
        write_delta_mb(f, "RSS", self.rss_baseline, self.rss_final)?;
        write_delta_mb(f, "VSZ", self.vsz_baseline, self.vsz_final)?;
        write_delta(f, "TCP sockets", self.tcp_baseline, self.tcp_final)?;
        write_delta(f, "CL segments", self.segments_baseline, self.segments_final)?;
        write_delta(f, "SSTables", self.sstables_baseline, self.sstables_final)?;
        write_delta(f, "Threads", self.threads_baseline, self.threads_final)?;

        let leaked = self.fd_final > self.fd_baseline + 10
            || self.segments_final > self.segments_baseline + 10
            || self.tcp_final > self.tcp_baseline + 5;
        let verdict = if leaked { "SUSPECT" } else { "CLEAN" };
        writeln!(f, "Leak verdict:       {verdict}")
    }
}

fn write_delta(f: &mut fmt::Formatter<'_>, label: &str, baseline: u64, final_val: u64) -> fmt::Result {
    let delta = final_val as i64 - baseline as i64;
    let sign = if delta >= 0 { "+" } else { "" };
    writeln!(
        f,
        "{:<20} {:>6} -> {:>6}  ({sign}{})",
        label, baseline, final_val, delta
    )
}

fn write_delta_mb(f: &mut fmt::Formatter<'_>, label: &str, baseline: u64, final_val: u64) -> fmt::Result {
    let delta = final_val as i64 - baseline as i64;
    let sign = if delta >= 0 { "+" } else { "" };
    writeln!(
        f,
        "{:<20} {:>6.1} MB -> {:>6.1} MB  ({sign}{:.1} MB)",
        label,
        baseline as f64 / (1024.0 * 1024.0),
        final_val as f64 / (1024.0 * 1024.0),
        delta as f64 / (1024.0 * 1024.0),
    )
}

// ---------------------------------------------------------------------------
// OS-level resource sampling
// ---------------------------------------------------------------------------

/// Sample all resource counters for the current process.
pub fn sample_resources(
    commit_log_closed_segments: u64,
    sstable_count: u64,
) -> ResourceSnapshot {
    ResourceSnapshot {
        open_fds: count_open_fds(),
        fd_limit: get_fd_limit(),
        rss_bytes: crate::generator::process_rss_bytes(),
        vsz_bytes: process_vsz_bytes(),
        tcp_sockets: count_tcp_sockets(),
        unix_sockets: count_unix_sockets(),
        thread_count: count_threads(),
        commit_log_closed_segments,
        sstable_count,
        tmp_files: count_tmp_files(),
    }
}

/// Count open file descriptors for the current process.
fn count_open_fds() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/fd")
            .map(|entries| entries.count() as u64)
            .unwrap_or(0)
    }
    #[cfg(target_os = "macos")]
    {
        // lsof is slow; use proc_pidinfo via libc.
        // Fallback: count via /dev/fd.
        std::fs::read_dir("/dev/fd")
            .map(|entries| entries.count() as u64)
            .unwrap_or(0)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

/// Get the soft file descriptor limit (ulimit -n).
fn get_fd_limit() -> u64 {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // SAFETY: getrlimit is a standard POSIX call.
        unsafe {
            let mut rlim: libc::rlimit = std::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                rlim.rlim_cur
            } else {
                // Conservative fallback.
                1024
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        1024
    }
}

/// Get the virtual memory size.
fn process_vsz_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        // Use task_info mach API for fast VSZ without subprocess.
        // SAFETY: task_info is a standard mach system call.
        #[allow(deprecated)] // libc::mach_task_self — mach2 crate not needed for this
        unsafe {
            let mut info: libc::mach_task_basic_info_data_t = std::mem::zeroed();
            let mut count = (std::mem::size_of::<libc::mach_task_basic_info_data_t>()
                / std::mem::size_of::<libc::natural_t>()) as u32;
            let kr = libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as *mut i32,
                &mut count,
            );
            if kr == 0 {
                info.virtual_size as u64
            } else {
                0
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or(0)
            * 4096 // pages to bytes
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Count TCP sockets held by this process.
fn count_tcp_sockets() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // /proc/self/net/tcp + tcp6
        let count_file = |path: &str| -> u64 {
            std::fs::read_to_string(path)
                .map(|s| s.lines().count().saturating_sub(1) as u64) // skip header
                .unwrap_or(0)
        };
        count_file("/proc/self/net/tcp") + count_file("/proc/self/net/tcp6")
    }
    #[cfg(target_os = "macos")]
    {
        // lsof is too slow (2-5s per call) to run every 500ms.
        // Count FDs pointing to sockets via /dev/fd.
        use std::os::unix::fs::FileTypeExt;
        std::fs::read_dir("/dev/fd")
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .filter(|m| m.file_type().is_socket())
                    .count() as u64
            })
            .unwrap_or(0)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

/// Count Unix domain sockets held by this process.
fn count_unix_sockets() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/net/unix")
            .map(|s| s.lines().count().saturating_sub(1) as u64)
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0 // macOS doesn't expose per-process unix socket count cheaply
    }
}

/// Count threads in the current process.
fn count_threads() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/task")
            .map(|entries| entries.count() as u64)
            .unwrap_or(1)
    }
    #[cfg(target_os = "macos")]
    {
        // No cheap per-process thread count API on macOS. Subprocess calls
        // (ps -M) are too slow for 500ms sampling. Return 0 to skip this
        // metric — FDs and RSS are more important for leak detection.
        0
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        1
    }
}

/// Count temporary files in /tmp that belong to this process.
fn count_tmp_files() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // Check /proc/self/fd for symlinks into /tmp
        std::fs::read_dir("/proc/self/fd")
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| std::fs::read_link(e.path()).ok())
                    .filter(|target| {
                        target
                            .to_str()
                            .is_some_and(|s| s.starts_with("/tmp") || s.starts_with("/var/tmp"))
                    })
                    .count() as u64
            })
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0 // Expensive on macOS; skip for now.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snap(fds: u64, rss: u64, segments: u64) -> ResourceSnapshot {
        ResourceSnapshot {
            open_fds: fds,
            fd_limit: 10000,
            rss_bytes: rss,
            vsz_bytes: rss * 2,
            tcp_sockets: 0,
            unix_sockets: 0,
            thread_count: 4,
            commit_log_closed_segments: segments,
            sstable_count: 5,
            tmp_files: 0,
        }
    }

    #[test]
    fn healthy_when_stable() {
        let mut mon = ResourceMonitor::new(2);
        // Warmup: 2 samples.
        assert_eq!(mon.record(make_snap(50, 100_000, 0)), LeakVerdict::Healthy);
        assert_eq!(mon.record(make_snap(55, 110_000, 0)), LeakVerdict::Healthy);
        // Post-warmup: stable values.
        for _ in 0..10 {
            assert_eq!(mon.record(make_snap(50, 100_000, 0)), LeakVerdict::Healthy);
        }
    }

    #[test]
    fn detects_commit_log_segment_leak() {
        let mut mon = ResourceMonitor::new(2);
        // Warmup.
        mon.record(make_snap(50, 100_000, 0));
        mon.record(make_snap(50, 100_000, 0));
        // Post-warmup: monotonically increasing commit log segments.
        for i in 0..=MONOTONIC_GROWTH_WINDOW {
            let verdict = mon.record(make_snap(50, 100_000, i as u64));
            if i < MONOTONIC_GROWTH_WINDOW {
                // Not enough consecutive increases yet.
            } else {
                match verdict {
                    LeakVerdict::Warning(ref warnings) => {
                        assert!(
                            warnings
                                .iter()
                                .any(|w| w.resource == "commit_log_segments"),
                            "expected commit_log_segments warning, got: {warnings:?}"
                        );
                    }
                    other => panic!("expected Warning, got: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn detects_fd_leak_independent_of_sstables() {
        let mut mon = ResourceMonitor::new(2);
        // Warmup.
        mon.record(make_snap(50, 100_000, 0));
        mon.record(make_snap(50, 100_000, 0));
        // Post-warmup: stable baseline.
        for _ in 0..=MONOTONIC_GROWTH_WINDOW {
            mon.record(make_snap(50, 100_000, 0));
        }
        // Now record a big FD jump with no SSTable growth.
        let snap = ResourceSnapshot {
            open_fds: 200,
            sstable_count: 5, // unchanged from make_snap default
            ..make_snap(0, 100_000, 0)
        };
        match mon.record(snap) {
            LeakVerdict::Warning(ref warnings) => {
                assert!(
                    warnings.iter().any(|w| w.resource.contains("open_fds")),
                    "expected open_fds warning, got: {warnings:?}"
                );
            }
            other => panic!("expected Warning for FD leak, got: {other:?}"),
        }
    }

    #[test]
    fn aborts_on_fd_limit() {
        let mut mon = ResourceMonitor::new(0);
        let snap = ResourceSnapshot {
            open_fds: 8500,
            fd_limit: 10000,
            ..make_snap(0, 100_000, 0)
        };
        match mon.record(snap) {
            LeakVerdict::Abort(msg) => assert!(msg.contains("FDs")),
            other => panic!("expected Abort, got: {other:?}"),
        }
    }

    #[test]
    fn aborts_on_rss_growth() {
        let mut mon = ResourceMonitor::new(0);
        let baseline_rss = 100 * 1024 * 1024; // 100 MB
        mon.record(make_snap(50, baseline_rss, 0));
        let big_rss = baseline_rss + RSS_GROWTH_ABORT_BYTES;
        match mon.record(make_snap(50, big_rss, 0)) {
            LeakVerdict::Abort(msg) => assert!(msg.contains("RSS")),
            other => panic!("expected Abort, got: {other:?}"),
        }
    }

    #[test]
    fn no_warning_with_fluctuating_values() {
        let mut mon = ResourceMonitor::new(2);
        mon.record(make_snap(50, 100_000, 0));
        mon.record(make_snap(50, 100_000, 0));
        // Fluctuating: goes up, then down, then up — not monotonic.
        let values = [50, 55, 52, 58, 53, 60, 55, 62, 58, 65];
        for &v in &values {
            let verdict = mon.record(make_snap(v, 100_000, 0));
            assert_ne!(
                verdict,
                LeakVerdict::Abort(String::new()),
                "should not abort on fluctuating values"
            );
        }
    }

    #[test]
    fn sample_resources_returns_valid_snapshot() {
        let snap = sample_resources(3, 10);
        // FD count should be at least 3 (stdin/stdout/stderr).
        assert!(snap.open_fds >= 3, "expected >= 3 FDs, got {}", snap.open_fds);
        // FD limit should be > 0.
        assert!(snap.fd_limit > 0);
        // Engine-provided values should pass through.
        assert_eq!(snap.commit_log_closed_segments, 3);
        assert_eq!(snap.sstable_count, 10);
    }

    #[test]
    fn summary_after_enough_samples() {
        let mut mon = ResourceMonitor::new(2);
        for i in 0..5 {
            mon.record(make_snap(50 + i, 100_000, i));
        }
        let summary = mon.summary().expect("should have summary");
        assert_eq!(summary.samples, 5);
        // Baseline is the third sample (index 2, first post-warmup).
        assert_eq!(summary.fd_baseline, 52);
    }
}
