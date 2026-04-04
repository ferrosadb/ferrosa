//! Load test statistics collection and reporting.
//!
//! Uses HDR histograms for accurate percentile latency tracking and
//! atomic counters for throughput.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::resource_monitor::ResourceSummary;

/// Latency percentiles extracted from an HDR histogram.
#[derive(Debug, Clone)]
pub struct LatencyPercentiles {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p100_us: u64,
    pub mean_us: f64,
    pub count: u64,
}

impl fmt::Display for LatencyPercentiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p50={:.1}ms  p95={:.1}ms  p99={:.1}ms  p100={:.1}ms  mean={:.1}ms  n={}",
            self.p50_us as f64 / 1000.0,
            self.p95_us as f64 / 1000.0,
            self.p99_us as f64 / 1000.0,
            self.p100_us as f64 / 1000.0,
            self.mean_us / 1000.0,
            self.count,
        )
    }
}

/// Periodic snapshot of test statistics.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub elapsed: Duration,
    pub writes: u64,
    pub reads: u64,
    pub writes_per_sec: f64,
    pub reads_per_sec: f64,
    pub memtable_bytes: u64,
    pub sstable_count: u64,
    pub bytes_written: u64,
    pub s3_uploads: u64,
    pub bytes_reclaimed: u64,
    pub rss_bytes: u64,
    pub deletes: u64,
    pub updates: u64,
}

/// Final statistics from a load test run.
#[derive(Debug, Clone)]
pub struct LoadStats {
    pub profile_name: String,
    pub total_writes: u64,
    pub total_reads: u64,
    pub write_errors: u64,
    pub read_errors: u64,
    pub data_mismatches: u64,
    pub missing_keys: u64,
    pub elapsed: Duration,
    pub writes_per_sec: f64,
    pub reads_per_sec: f64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub compaction_tasks_completed: u64,
    pub s3_uploads: u64,
    pub s3_deletes: u64,
    pub bytes_reclaimed: u64,
    pub sstable_count_final: u64,
    pub keys_verified: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
    pub rss_start_bytes: u64,
    pub rss_end_bytes: u64,
    pub rss_peak_bytes: u64,
    pub write_latency: LatencyPercentiles,
    pub read_latency: LatencyPercentiles,
    pub snapshots: Vec<StatsSnapshot>,
    /// Sampled error messages with counts, for diagnosing write failures.
    pub error_breakdown: Vec<(String, u64)>,
    /// Resource leak detection summary (FDs, sockets, memory, segments).
    pub resource_summary: Option<ResourceSummary>,
    /// If the test was aborted due to a resource limit, the reason.
    pub abort_reason: Option<String>,
}

impl fmt::Display for LoadStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== UCS Load Test: {} ===", self.profile_name)?;
        writeln!(f, "Duration:           {:.1}s", self.elapsed.as_secs_f64())?;
        writeln!(f)?;

        writeln!(f, "--- Throughput ---")?;
        writeln!(
            f,
            "Total writes:       {:>12}  ({:.0} writes/s)",
            self.total_writes, self.writes_per_sec
        )?;
        writeln!(
            f,
            "Total reads:        {:>12}  ({:.0} reads/s)",
            self.total_reads, self.reads_per_sec
        )?;
        writeln!(
            f,
            "Bytes written:      {:>12}  ({:.1} MB)",
            self.bytes_written,
            self.bytes_written as f64 / (1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "Bytes read:         {:>12}  ({:.1} MB)",
            self.bytes_read,
            self.bytes_read as f64 / (1024.0 * 1024.0)
        )?;
        let secs = self.elapsed.as_secs_f64().max(0.001);
        writeln!(
            f,
            "Write throughput:   {:>12}  ({:.1} MB/s)",
            "",
            self.bytes_written as f64 / secs / (1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "Read throughput:    {:>12}  ({:.1} MB/s)",
            "",
            self.bytes_read as f64 / secs / (1024.0 * 1024.0)
        )?;
        writeln!(f, "Total updates:      {:>12}", self.total_updates)?;
        writeln!(f, "Total deletes:      {:>12}", self.total_deletes)?;
        writeln!(f, "Write errors:       {:>12}", self.write_errors)?;
        writeln!(f, "Read errors:        {:>12}", self.read_errors)?;
        if !self.error_breakdown.is_empty() {
            writeln!(f)?;
            writeln!(f, "--- Error Breakdown ---")?;
            for (msg, count) in &self.error_breakdown {
                writeln!(f, "  [{count:>6}x] {msg}")?;
            }
        }
        writeln!(f)?;

        writeln!(f, "--- Latency ---")?;
        writeln!(f, "Write: {}", self.write_latency)?;
        writeln!(f, "Read:  {}", self.read_latency)?;
        writeln!(f)?;

        writeln!(f, "--- Storage ---")?;
        writeln!(f, "Final SSTables:     {:>12}", self.sstable_count_final)?;
        writeln!(
            f,
            "Compaction tasks:   {:>12}",
            self.compaction_tasks_completed
        )?;
        writeln!(f, "S3 uploads:         {:>12}", self.s3_uploads)?;
        writeln!(f, "S3 deletes:         {:>12}", self.s3_deletes)?;
        writeln!(
            f,
            "Bytes reclaimed:    {:>12}  ({:.1} MB)",
            self.bytes_reclaimed,
            self.bytes_reclaimed as f64 / (1024.0 * 1024.0)
        )?;
        writeln!(f)?;

        writeln!(f, "--- Memory (RSS) ---")?;
        writeln!(
            f,
            "Start:              {:>12}  ({:.1} MB)",
            self.rss_start_bytes,
            self.rss_start_bytes as f64 / (1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "End:                {:>12}  ({:.1} MB)",
            self.rss_end_bytes,
            self.rss_end_bytes as f64 / (1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "Peak:               {:>12}  ({:.1} MB)",
            self.rss_peak_bytes,
            self.rss_peak_bytes as f64 / (1024.0 * 1024.0)
        )?;
        let growth = self.rss_end_bytes.saturating_sub(self.rss_start_bytes);
        writeln!(
            f,
            "Growth:             {:>12}  ({:.1} MB)",
            growth,
            growth as f64 / (1024.0 * 1024.0)
        )?;
        writeln!(f)?;

        writeln!(f, "--- Integrity ---")?;
        writeln!(f, "Data mismatches:    {:>12}", self.data_mismatches)?;
        writeln!(f, "Missing keys:       {:>12}", self.missing_keys)?;
        let integrity_ok = self.missing_keys == 0 && self.data_mismatches == 0;
        let verdict = if integrity_ok { "PASS" } else { "FAIL" };
        writeln!(
            f,
            "Verdict:            {} ({} keys verified)",
            verdict, self.keys_verified
        )?;

        // Resource leak detection summary.
        if let Some(ref summary) = self.resource_summary {
            writeln!(f)?;
            write!(f, "{summary}")?;
        }

        // Abort reason (if test was stopped early).
        if let Some(ref reason) = self.abort_reason {
            writeln!(f)?;
            writeln!(f, "--- ABORTED ---")?;
            writeln!(f, "Reason: {reason}")?;
        }

        if !self.snapshots.is_empty() {
            writeln!(f)?;
            writeln!(f, "--- Timeline ---")?;
            writeln!(
                f,
                "{:>8} {:>10} {:>10} {:>10} {:>10} {:>8} {:>10} {:>8}",
                "time", "writes", "reads", "w/s", "r/s", "sst", "s3_up", "RSS_MB"
            )?;
            for snap in &self.snapshots {
                writeln!(
                    f,
                    "{:>7.0}s {:>10} {:>10} {:>10.0} {:>10.0} {:>8} {:>10} {:>8.1}",
                    snap.elapsed.as_secs_f64(),
                    snap.writes,
                    snap.reads,
                    snap.writes_per_sec,
                    snap.reads_per_sec,
                    snap.sstable_count,
                    snap.s3_uploads,
                    snap.rss_bytes as f64 / (1024.0 * 1024.0),
                )?;
            }
        }

        Ok(())
    }
}

/// Thread-safe latency histogram (wraps hdrhistogram behind a mutex).
pub struct LatencyHistogram {
    inner: parking_lot::Mutex<hdrhistogram::Histogram<u64>>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // Track latencies from 1us to 60s with 3 significant digits.
        let hist = hdrhistogram::Histogram::new_with_bounds(1, 60_000_000, 3)
            .expect("valid histogram bounds");
        Self {
            inner: parking_lot::Mutex::new(hist),
        }
    }

    /// Record a latency in microseconds.
    pub fn record(&self, us: u64) {
        let mut h = self.inner.lock();
        let _ = h.record(us);
    }

    /// Record a Duration.
    pub fn record_duration(&self, d: Duration) {
        self.record(d.as_micros() as u64);
    }

    /// Extract percentiles.
    pub fn percentiles(&self) -> LatencyPercentiles {
        let h = self.inner.lock();
        LatencyPercentiles {
            p50_us: h.value_at_quantile(0.50),
            p95_us: h.value_at_quantile(0.95),
            p99_us: h.value_at_quantile(0.99),
            p100_us: h.max(),
            mean_us: h.mean(),
            count: h.len(),
        }
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum number of distinct error messages to sample.
const MAX_ERROR_SAMPLES: usize = 10;

/// Context for finalizing a load test run into a [`LoadStats`] report.
pub struct FinalizeContext<'a> {
    pub profile_name: &'a str,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub compaction_tasks: u64,
    pub s3_uploads: u64,
    pub s3_deletes: u64,
    pub bytes_reclaimed: u64,
    pub sstable_count_final: u64,
    pub missing_keys: u64,
    pub data_mismatches: u64,
    pub keys_verified: u64,
    pub resource_summary: Option<ResourceSummary>,
    pub abort_reason: Option<String>,
}

/// Collects statistics during a load test run.
pub struct StatsCollector {
    pub(crate) start: Instant,
    pub(crate) writes: AtomicU64,
    pub(crate) reads: AtomicU64,
    pub(crate) updates: AtomicU64,
    pub(crate) deletes: AtomicU64,
    pub(crate) write_errors: AtomicU64,
    pub(crate) read_errors: AtomicU64,
    pub(crate) rss_start: u64,
    pub(crate) rss_peak: AtomicU64,
    pub(crate) write_hist: LatencyHistogram,
    pub(crate) read_hist: LatencyHistogram,
    pub(crate) snapshots: parking_lot::Mutex<Vec<StatsSnapshot>>,
    /// Sampled error messages (deduplicated, capped at MAX_ERROR_SAMPLES).
    pub(crate) error_samples: parking_lot::Mutex<Vec<(String, u64)>>,
}

impl StatsCollector {
    pub fn new() -> Self {
        let rss_start = crate::generator::process_rss_bytes();
        Self {
            start: Instant::now(),
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
            rss_start,
            rss_peak: AtomicU64::new(rss_start),
            write_hist: LatencyHistogram::new(),
            read_hist: LatencyHistogram::new(),
            snapshots: parking_lot::Mutex::new(Vec::new()),
            error_samples: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn record_update(&self, latency: Duration) {
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_hist.record_duration(latency);
    }

    pub fn record_delete(&self, latency: Duration) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_hist.record_duration(latency);
    }

    /// Record a successful write with its latency.
    pub fn record_write(&self, latency: Duration) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_hist.record_duration(latency);
    }

    /// Record a successful read with its latency.
    pub fn record_read(&self, latency: Duration) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_hist.record_duration(latency);
    }

    pub fn record_write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_read_error(&self) {
        self.read_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error message sample for the final report.
    /// Deduplicates by message prefix and caps at MAX_ERROR_SAMPLES.
    pub fn record_error_sample(&self, err: ferrosa_common::Error) {
        let msg = err.to_string();
        // Truncate to first 120 chars for dedup key.
        let key: String = msg.chars().take(120).collect();
        let mut samples = self.error_samples.lock();
        if let Some(entry) = samples.iter_mut().find(|(k, _)| *k == key) {
            entry.1 += 1;
        } else if samples.len() < MAX_ERROR_SAMPLES {
            samples.push((key, 1));
        }
    }

    /// Take a periodic snapshot (includes engine metrics and RSS).
    pub fn take_snapshot(
        &self,
        memtable_bytes: u64,
        sstable_count: u64,
        bytes_written: u64,
        s3_uploads: u64,
        bytes_reclaimed: u64,
    ) {
        let elapsed = self.start.elapsed();
        let secs = elapsed.as_secs_f64().max(0.001);
        let writes = self.writes.load(Ordering::Relaxed);
        let reads = self.reads.load(Ordering::Relaxed);
        let rss = crate::generator::process_rss_bytes();

        // Update peak RSS.
        self.rss_peak.fetch_max(rss, Ordering::Relaxed);

        let snap = StatsSnapshot {
            elapsed,
            writes,
            reads,
            writes_per_sec: writes as f64 / secs,
            reads_per_sec: reads as f64 / secs,
            memtable_bytes,
            sstable_count,
            bytes_written,
            s3_uploads,
            bytes_reclaimed,
            rss_bytes: rss,
            deletes: self.deletes.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
        };
        self.snapshots.lock().push(snap);
    }

    /// Finalize and produce the complete stats report.
    pub fn finalize(self, ctx: FinalizeContext<'_>) -> LoadStats {
        let FinalizeContext {
            profile_name,
            bytes_written,
            bytes_read,
            compaction_tasks,
            s3_uploads,
            s3_deletes,
            bytes_reclaimed,
            sstable_count_final,
            missing_keys,
            data_mismatches,
            keys_verified,
            resource_summary,
            abort_reason,
        } = ctx;
        let elapsed = self.start.elapsed();
        let secs = elapsed.as_secs_f64().max(0.001);
        let total_writes = self.writes.load(Ordering::Relaxed);
        let total_reads = self.reads.load(Ordering::Relaxed);

        let rss_end = crate::generator::process_rss_bytes();
        let rss_peak = self.rss_peak.load(Ordering::Relaxed).max(rss_end);

        LoadStats {
            profile_name: profile_name.to_string(),
            total_writes,
            total_reads,
            write_errors: self.write_errors.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            data_mismatches,
            missing_keys,
            elapsed,
            writes_per_sec: total_writes as f64 / secs,
            reads_per_sec: total_reads as f64 / secs,
            bytes_written,
            bytes_read,
            compaction_tasks_completed: compaction_tasks,
            s3_uploads,
            s3_deletes,
            bytes_reclaimed,
            sstable_count_final,
            keys_verified,
            total_updates: self.updates.load(Ordering::Relaxed),
            total_deletes: self.deletes.load(Ordering::Relaxed),
            rss_start_bytes: self.rss_start,
            rss_end_bytes: rss_end,
            rss_peak_bytes: rss_peak,
            write_latency: self.write_hist.percentiles(),
            read_latency: self.read_hist.percentiles(),
            snapshots: self.snapshots.into_inner(),
            error_breakdown: self.error_samples.into_inner(),
            resource_summary,
            abort_reason,
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(name: &str) -> FinalizeContext<'_> {
        FinalizeContext {
            profile_name: name,
            bytes_written: 0,
            bytes_read: 0,
            compaction_tasks: 0,
            s3_uploads: 0,
            s3_deletes: 0,
            bytes_reclaimed: 0,
            sstable_count_final: 0,
            missing_keys: 0,
            data_mismatches: 0,
            keys_verified: 0,
            resource_summary: None,
            abort_reason: None,
        }
    }

    #[test]
    fn stats_collector_tracks_writes_and_reads() {
        let sc = StatsCollector::new();
        for _ in 0..100 {
            sc.record_write(Duration::from_micros(50));
        }
        for _ in 0..50 {
            sc.record_read(Duration::from_micros(100));
        }
        let stats = sc.finalize(test_ctx("test"));
        assert_eq!(stats.total_writes, 100);
        assert_eq!(stats.total_reads, 50);
    }

    #[test]
    fn stats_collector_tracks_errors() {
        let sc = StatsCollector::new();
        sc.record_write_error();
        sc.record_write_error();
        sc.record_read_error();
        let stats = sc.finalize(test_ctx("test"));
        assert_eq!(stats.write_errors, 2);
        assert_eq!(stats.read_errors, 1);
    }

    #[test]
    fn latency_histogram_percentiles() {
        let hist = LatencyHistogram::new();
        // Record 100 values from 1us to 100us.
        for i in 1..=100 {
            hist.record(i);
        }
        let p = hist.percentiles();
        assert_eq!(p.count, 100);
        assert!(p.p50_us >= 49 && p.p50_us <= 51);
        assert!(p.p95_us >= 94 && p.p95_us <= 96);
        assert!(p.p99_us >= 98 && p.p99_us <= 100);
        assert_eq!(p.p100_us, 100);
    }

    #[test]
    fn latency_histogram_records_duration() {
        let hist = LatencyHistogram::new();
        hist.record_duration(Duration::from_millis(5));
        let p = hist.percentiles();
        assert_eq!(p.count, 1);
        assert!(p.p50_us >= 4900 && p.p50_us <= 5100);
    }

    #[test]
    fn write_latency_tracked_in_finalize() {
        let sc = StatsCollector::new();
        sc.record_write(Duration::from_micros(100));
        sc.record_write(Duration::from_micros(200));
        sc.record_write(Duration::from_micros(5000));
        let stats = sc.finalize(test_ctx("test"));
        assert_eq!(stats.write_latency.count, 3);
        // HDR histogram quantizes values — allow 1% tolerance.
        assert!(
            stats.write_latency.p100_us >= 4950 && stats.write_latency.p100_us <= 5050,
            "p100 should be ~5000us, got {}",
            stats.write_latency.p100_us
        );
        assert!(stats.write_latency.p50_us <= 250);
    }

    #[test]
    fn stats_display_includes_latency() {
        let sc = StatsCollector::new();
        sc.record_write(Duration::from_millis(1));
        sc.record_read(Duration::from_millis(2));
        let stats = sc.finalize(FinalizeContext {
            bytes_written: 1000,
            sstable_count_final: 1,
            keys_verified: 100,
            ..test_ctx("test")
        });
        let output = format!("{stats}");
        assert!(output.contains("Latency"));
        assert!(output.contains("p50="));
        assert!(output.contains("p99="));
        assert!(output.contains("PASS"));
    }

    #[test]
    fn stats_snapshot_includes_s3_metrics() {
        let sc = StatsCollector::new();
        sc.take_snapshot(1024, 3, 5000, 2, 1000);
        let stats = sc.finalize(test_ctx("test"));
        assert_eq!(stats.snapshots.len(), 1);
        assert_eq!(stats.snapshots[0].s3_uploads, 2);
        assert_eq!(stats.snapshots[0].bytes_reclaimed, 1000);
    }

    #[test]
    fn stats_display_includes_bytes_read() {
        let sc = StatsCollector::new();
        let stats = sc.finalize(FinalizeContext {
            bytes_read: 5000,
            ..test_ctx("bytes_read_test")
        });
        let output = format!("{stats}");
        assert!(
            output.contains("Bytes read:"),
            "output should contain 'Bytes read:' line, got:\n{output}"
        );
        assert!(
            output.contains("5000"),
            "output should contain '5000', got:\n{output}"
        );
    }

    #[test]
    fn stats_display_includes_throughput_mbs() {
        let sc = StatsCollector::new();
        let stats = sc.finalize(FinalizeContext {
            bytes_written: 10 * 1024 * 1024,
            bytes_read: 20 * 1024 * 1024,
            ..test_ctx("throughput_test")
        });
        let output = format!("{stats}");
        assert!(
            output.contains("Write throughput:"),
            "output should contain 'Write throughput:', got:\n{output}"
        );
        assert!(
            output.contains("Read throughput:"),
            "output should contain 'Read throughput:', got:\n{output}"
        );
        assert!(
            output.contains("MB/s"),
            "output should contain 'MB/s', got:\n{output}"
        );
    }

    #[test]
    fn stats_display_zero_bytes_read() {
        let sc = StatsCollector::new();
        let stats = sc.finalize(FinalizeContext {
            bytes_read: 0,
            ..test_ctx("zero_bytes_read_test")
        });
        let output = format!("{stats}");
        assert!(
            output.contains("Bytes read:"),
            "output should contain 'Bytes read:' line, got:\n{output}"
        );
        // The "Bytes read:" line should show 0 bytes and 0.0 MB.
        // Find the specific line to verify the value is 0.
        let bytes_read_line = output
            .lines()
            .find(|l| l.contains("Bytes read:"))
            .expect("should have a 'Bytes read:' line");
        assert!(
            bytes_read_line.contains("0"),
            "Bytes read line should contain '0', got: {bytes_read_line}"
        );
    }
}
