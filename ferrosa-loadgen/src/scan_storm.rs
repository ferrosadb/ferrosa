//! Module: Drive a concurrent full-table `ALLOW FILTERING` scan storm against a
//!   running Ferrosa cluster.
//! Correctness: Correct when every worker issues the exact full-table filtering
//!   scan, workers are spread round-robin across nodes, and the run reports real
//!   scan/error/connect counts (never a faked success).
//! Last revised: 2026-07-21
//! Last changed: New module — the B0 scan-starvation regression (t_88223ad0 /
//!   T0.6) needs the viz-class workload (full-table `ALLOW FILTERING` fan-out)
//!   that the ratio-based load profiles cannot produce (they are all
//!   primary-key point ops).
//!
//! # Why this exists
//!
//! The `viz` consolidation query that starved the raft leader on 2026-07-17 was
//! a full-table `ALLOW FILTERING` scan over the entity/edge tables. None of the
//! [`LoadProfile`](crate::profile::LoadProfile) mixes can reproduce it — they
//! only issue `SELECT val FROM data WHERE pk = ?` point reads. This module fires
//! the real scan shape concurrently so the regression can prove the bounded
//! scheduler pool keeps the raft heartbeat un-starved.
//!
//! The filter predicate deliberately matches (almost) nothing
//! (`WHERE val = 0x<sentinel>`): the coordinator must still read every row of
//! every SSTable to decide no row matches, so the scan does maximal storage work
//! while returning a near-empty result set — pure scan pressure, minimal
//! result-serialization noise.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cluster::connect_authed;

/// How a single scan-storm run is parameterized.
#[derive(Debug, Clone)]
pub struct ScanStormConfig {
    /// Keyspace to scan (the loadgen seed keyspace).
    pub keyspace: String,
    /// Table to scan.
    pub table: String,
    /// Non-primary-key column the `ALLOW FILTERING` predicate filters on.
    pub filter_col: String,
    /// Hex bytes of the sentinel blob the predicate compares against (chosen to
    /// match no seeded row, forcing a full scan).
    pub filter_hex: String,
    /// Number of concurrent scan workers.
    pub concurrency: usize,
    /// How long to sustain the storm.
    pub duration: Duration,
}

impl ScanStormConfig {
    /// Defaults matching the loadgen seed schema (`load_test.data(pk,ck,val)`).
    /// `val` is the blob column, so filtering on it is unindexed and forces the
    /// `ALLOW FILTERING` full-table scan.
    pub fn for_seed_table(concurrency: usize, duration: Duration) -> Self {
        Self {
            keyspace: "load_test".to_string(),
            table: "data".to_string(),
            filter_col: "val".to_string(),
            filter_hex: "deadbeefdeadbeef".to_string(),
            concurrency: resolve_concurrency(concurrency),
            duration,
        }
    }
}

/// Outcome of a scan-storm run. Reported verbatim — a run where every worker
/// failed to connect reports `scans_completed == 0`, never a faked success.
#[derive(Debug, Clone, Default)]
pub struct ScanStormStats {
    /// Total scans that returned a result (across all workers).
    pub scans_completed: u64,
    /// Scans that returned an error (e.g. coordinator unavailable mid-step-down).
    pub scan_errors: u64,
    /// Workers that could not (re)establish a session.
    pub connect_failures: u64,
    /// Number of workers launched.
    pub workers: usize,
    /// Wall-clock the storm actually ran.
    pub elapsed: Duration,
}

impl std::fmt::Display for ScanStormStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "scan-storm: {} workers, {} scans, {} scan-errors, {} connect-failures in {:.1}s",
            self.workers,
            self.scans_completed,
            self.scan_errors,
            self.connect_failures,
            self.elapsed.as_secs_f64(),
        )
    }
}

/// Build the full-table `ALLOW FILTERING` scan statement. Pure so it can be
/// pinned by a unit test without a live cluster.
pub fn scan_statement(cfg: &ScanStormConfig) -> String {
    format!(
        "SELECT pk, ck, val FROM {}.{} WHERE {} = 0x{} ALLOW FILTERING",
        cfg.keyspace, cfg.table, cfg.filter_col, cfg.filter_hex
    )
}

/// Round-robin node assignment for worker `task_idx`. Pure.
pub fn node_for_task(task_idx: usize, node_count: usize) -> usize {
    task_idx % node_count.max(1)
}

/// Clamp a requested worker count to at least one. Pure.
pub fn resolve_concurrency(requested: usize) -> usize {
    requested.max(1)
}

/// Run a scan storm, blocking until the duration elapses. Builds its own
/// multi-threaded runtime (the CLI entry point is synchronous), mirroring
/// [`crate::cluster::run_cluster_load_test`].
pub fn run_scan_storm_blocking(nodes: &[SocketAddr], cfg: &ScanStormConfig) -> ScanStormStats {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime");
    rt.block_on(run_scan_storm(nodes, cfg))
}

/// Connect to `addr`, retrying up to `attempts` times with a short backoff so a
/// mid-step-down connection drop does not permanently kill a worker.
async fn connect_retry(
    addr: SocketAddr,
    attempts: usize,
) -> Option<ferrosa_cql::client::CqlClient> {
    for n in 1..=attempts {
        match connect_authed(addr).await {
            Ok(client) => return Some(client),
            Err(_) if n < attempts => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(_) => return None,
        }
    }
    None
}

async fn run_scan_storm(nodes: &[SocketAddr], cfg: &ScanStormConfig) -> ScanStormStats {
    assert!(!nodes.is_empty(), "scan storm needs at least one node");

    let stmt = Arc::new(scan_statement(cfg));
    eprintln!(
        "scan-storm: {} workers over {} nodes for {}s\n  {}",
        cfg.concurrency,
        nodes.len(),
        cfg.duration.as_secs(),
        stmt
    );

    let start = Instant::now();
    let deadline = start + cfg.duration;
    let scans = Arc::new(AtomicU64::new(0));
    let scan_errors = Arc::new(AtomicU64::new(0));
    let connect_failures = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(cfg.concurrency);
    for task_idx in 0..cfg.concurrency {
        let addr = nodes[node_for_task(task_idx, nodes.len())];
        let stmt = stmt.clone();
        let scans = scans.clone();
        let scan_errors = scan_errors.clone();
        let connect_failures = connect_failures.clone();
        handles.push(tokio::spawn(async move {
            let mut client = match connect_retry(addr, 30).await {
                Some(c) => c,
                None => {
                    connect_failures.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            while Instant::now() < deadline {
                match client.query_quorum(stmt.as_str()).await {
                    Ok(_) => {
                        scans.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        // A step-down can drop the coordinator connection; count
                        // the error and try to re-establish so the storm keeps
                        // pressure on across the very event we aim to observe.
                        scan_errors.fetch_add(1, Ordering::Relaxed);
                        match connect_retry(addr, 5).await {
                            Some(c) => client = c,
                            None => {
                                connect_failures.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    ScanStormStats {
        scans_completed: scans.load(Ordering::Relaxed),
        scan_errors: scan_errors.load(Ordering::Relaxed),
        connect_failures: connect_failures.load(Ordering::Relaxed),
        workers: cfg.concurrency,
        elapsed: start.elapsed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_statement_builds_full_table_allow_filtering() {
        let cfg = ScanStormConfig::for_seed_table(8, Duration::from_secs(30));
        assert_eq!(
            scan_statement(&cfg),
            "SELECT pk, ck, val FROM load_test.data WHERE val = 0xdeadbeefdeadbeef ALLOW FILTERING"
        );
    }

    #[test]
    fn node_for_task_round_robins_across_nodes() {
        assert_eq!(node_for_task(0, 3), 0);
        assert_eq!(node_for_task(1, 3), 1);
        assert_eq!(node_for_task(2, 3), 2);
        assert_eq!(node_for_task(3, 3), 0);
        assert_eq!(node_for_task(7, 3), 1);
    }

    #[test]
    fn node_for_task_never_divides_by_zero() {
        // Defensive: a zero node_count must not panic (callers assert non-empty,
        // but the pure helper stays total).
        assert_eq!(node_for_task(5, 0), 0);
    }

    #[test]
    fn resolve_concurrency_clamps_to_at_least_one() {
        assert_eq!(resolve_concurrency(0), 1);
        assert_eq!(resolve_concurrency(1), 1);
        assert_eq!(resolve_concurrency(64), 64);
    }
}
