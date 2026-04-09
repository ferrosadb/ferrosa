//! CQL-based cluster load test client.
//!
//! Connects to a running Ferrosa cluster via CQL, creates a test
//! keyspace + table, and runs the load workload over the network.
//! The cluster handles compaction, S3 sync, and all server-side
//! concerns — the loadgen is a pure CQL client.
//!
//! Server-side metrics (S3 uploads, SSTables, compaction) are polled
//! from the web API (/api/storage) on the first node.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_cql::client::CqlClient;
use rand::SeedableRng;

use crate::ground_truth::GroundTruth;
use crate::profile::LoadProfile;
use crate::resource_monitor::{self, LeakVerdict, ResourceMonitor, ResourceSnapshot};
use crate::stats::{FinalizeContext, LoadStats, StatsCollector};
use crate::tui::{TuiDashboard, TuiFrame};

/// Parse a comma-separated list of node addresses.
///
/// Each entry is `host:port` for CQL. The web API port is inferred as
/// CQL port + 5048 (9042 → 9090, 9043 → 9091, etc.) unless overridden.
pub fn parse_nodes(node_str: &str) -> Vec<SocketAddr> {
    node_str
        .split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            trimmed.parse::<SocketAddr>().ok().or_else(|| {
                eprintln!("warning: ignoring invalid node address '{trimmed}'");
                None
            })
        })
        .collect()
}

/// Infer web API port from CQL port (9042→9090, 9043→9091, etc.)
fn web_port(cql_addr: &SocketAddr) -> u16 {
    let cql_port = cql_addr.port();
    // Standard mapping: CQL 9042+N → Web 9090+N
    9090 + cql_port.saturating_sub(9042)
}

/// Set up the test keyspace and table on the cluster.
/// Truncates any existing data so back-to-back runs start clean.
async fn setup_schema(client: &mut CqlClient, rf: usize) -> Result<(), String> {
    client
        .query(&format!(
            "CREATE KEYSPACE IF NOT EXISTS load_test \
             WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {rf}}}"
        ))
        .await
        .map_err(|e| format!("CREATE KEYSPACE failed: {e}"))?;

    client
        .query("USE load_test")
        .await
        .map_err(|e| format!("USE failed: {e}"))?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS data ( \
                pk text, \
                ck int, \
                val blob, \
                PRIMARY KEY (pk, ck) \
             )",
        )
        .await
        .map_err(|e| format!("CREATE TABLE failed: {e}"))?;

    // Truncate stale data from previous runs so integrity checks
    // only verify keys written in this session.
    client
        .query("TRUNCATE data")
        .await
        .map_err(|e| format!("TRUNCATE failed: {e}"))?;

    Ok(())
}

/// Server-side metrics from the web API.
#[derive(Debug, Clone, Default)]
struct ServerMetrics {
    sstable_count: u64,
    s3_object_count: u64,
    s3_bytes: u64,
    memtable_bytes: u64,
    pending_compactions: u64,
}

/// Poll /api/storage on a node's web API.
async fn poll_server_metrics(http: &reqwest::Client, web_url: &str) -> ServerMetrics {
    let url = format!("{web_url}/api/storage");
    let resp = match http.get(&url).timeout(Duration::from_secs(2)).send().await {
        Ok(r) => r,
        Err(_) => return ServerMetrics::default(),
    };
    let tables: Vec<serde_json::Value> = match resp.json().await {
        Ok(v) => v,
        Err(_) => return ServerMetrics::default(),
    };

    let mut m = ServerMetrics::default();
    for t in &tables {
        m.sstable_count += t["sstable_count"].as_u64().unwrap_or(0);
        m.s3_object_count += t["s3_object_count"].as_u64().unwrap_or(0);
        m.s3_bytes += t["s3_bytes"].as_u64().unwrap_or(0);
        m.memtable_bytes += t["memtable_size_bytes"].as_u64().unwrap_or(0);
        m.pending_compactions += t["pending_compactions"].as_u64().unwrap_or(0);
    }
    m
}

/// Run a CQL-based cluster load test.
pub fn run_cluster_load_test(
    nodes: &[SocketAddr],
    profile: &LoadProfile,
    enable_tui: bool,
) -> LoadStats {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime");

    rt.block_on(run_cluster_load_test_async(nodes, profile, enable_tui))
}

async fn run_cluster_load_test_async(
    nodes: &[SocketAddr],
    profile: &LoadProfile,
    enable_tui: bool,
) -> LoadStats {
    assert!(!nodes.is_empty(), "need at least one node");

    let rf = nodes.len().min(3); // RF = min(nodes, 3)
    eprintln!("Cluster nodes: {nodes:?} (RF={rf})");

    // Set up schema via the first node. Raft DDL may not be ready immediately
    // after cluster boot (leader election in progress), so retry with backoff.
    eprintln!("Setting up schema (waiting for Raft DDL readiness)...");
    let mut schema_ok = false;
    for attempt in 1..=30 {
        match CqlClient::connect(nodes[0]).await {
            Ok(mut client) => match setup_schema(&mut client, rf).await {
                Ok(()) => {
                    eprintln!("  Schema ready on attempt {attempt}");
                    schema_ok = true;
                    break;
                }
                Err(e) => {
                    if attempt <= 3 || attempt % 5 == 0 {
                        eprintln!("  Attempt {attempt}: {e}");
                    }
                }
            },
            Err(e) => {
                if attempt <= 3 {
                    eprintln!("  Attempt {attempt}: connect failed: {e}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !schema_ok {
        eprintln!("WARNING: Schema setup failed after 30 attempts — proceeding anyway");
    }

    // Verify schema propagated to all nodes.
    for node in nodes {
        for _ in 0..10 {
            if let Ok(mut c) = CqlClient::connect(*node).await {
                if c.query("USE load_test").await.is_ok()
                    && c.query("SELECT pk FROM data LIMIT 1").await.is_ok()
                {
                    eprintln!("  {node}: schema confirmed");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    let http = reqwest::Client::new();
    let web_url = format!("http://{}:{}", nodes[0].ip(), web_port(&nodes[0]));

    let stats = Arc::new(StatsCollector::new());
    let stop = Arc::new(AtomicBool::new(false));
    let bytes_written = Arc::new(AtomicU64::new(0));
    let bytes_read = Arc::new(AtomicU64::new(0));
    let ground_truth = Arc::new(GroundTruth::new());
    let mut resource_mon = ResourceMonitor::new(4);
    let mut abort_reason: Option<String> = None;
    let mut throughput_history: Vec<u64> = Vec::new();
    #[allow(unused_assignments)]
    let mut leak_warnings: usize = 0;
    #[allow(unused_assignments)]
    let mut last_resource_snap: Option<ResourceSnapshot> = None;

    let mut dashboard = if enable_tui {
        TuiDashboard::init().ok()
    } else {
        None
    };

    let total_workers = profile.num_writers + profile.num_readers;

    // Spawn worker tasks — round-robin across nodes, each with its own CQL connection.
    let mut handles = Vec::new();
    for worker_id in 0..total_workers {
        let is_reader = worker_id >= profile.num_writers;
        let node = nodes[worker_id % nodes.len()];
        let key_space = profile.key_space_size;
        let value_range = profile.value_size_range;
        let sc = Arc::clone(&stats);
        let st = Arc::clone(&stop);
        let bw = Arc::clone(&bytes_written);
        let br = Arc::clone(&bytes_read);
        let gt = Arc::clone(&ground_truth);

        handles.push(tokio::spawn(async move {
            let mut client = match CqlClient::connect(node).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("worker {worker_id} failed to connect to {node}: {e}");
                    return;
                }
            };

            if let Err(e) = client.query("USE load_test").await {
                eprintln!("worker {worker_id} USE failed: {e}");
                return;
            }

            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut local_ts = (worker_id as i64) * 1_000_000_000;

            while !st.load(Ordering::Relaxed) {
                let key_idx = rand::Rng::gen_range(&mut rng, 0..key_space);
                let key_str = crate::generator::make_key_string(key_idx);

                if is_reader {
                    let cql = format!("SELECT val FROM data WHERE pk = '{key_str}' AND ck = 1");
                    let t0 = Instant::now();
                    match client.query_quorum(&cql).await {
                        Ok(result) => {
                            sc.record_read(t0.elapsed());
                            if let Some(Some(v)) = result
                                .rows
                                .first()
                                .map(|r| r.columns.first().and_then(|c| c.as_ref()))
                            {
                                br.fetch_add(v.len() as u64, Ordering::Relaxed);
                            }
                        }
                        Err(_) => sc.record_read_error(),
                    }
                } else {
                    let val_len =
                        crate::generator::choose_value_len(&mut rng, value_range.0, value_range.1);
                    let value = crate::generator::make_random_value(&mut rng, val_len);
                    local_ts += 1;
                    let val_hex = hex::encode(&value);
                    let cql = format!(
                        "INSERT INTO data (pk, ck, val) VALUES ('{key_str}', 1, 0x{val_hex}) \
                         USING TIMESTAMP {local_ts}"
                    );
                    let t0 = Instant::now();
                    match client.query_quorum(&cql).await {
                        Ok(_) => {
                            sc.record_write(t0.elapsed());
                            bw.fetch_add(val_len as u64, Ordering::Relaxed);
                            gt.record_write(&key_str, &value, local_ts);
                        }
                        Err(e) => {
                            sc.record_write_error();
                            sc.record_error_sample(ferrosa_common::Error::InvalidData(
                                e.to_string(),
                            ));
                        }
                    }
                }
            }
        }));
    }

    // Main loop: poll server metrics, resource monitoring, TUI.
    let start = Instant::now();
    let mut prev_writes: u64 = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if start.elapsed() >= profile.duration {
            break;
        }

        if let Some(ref mut d) = dashboard {
            if d.poll_quit() {
                abort_reason = Some("user pressed q".to_string());
                break;
            }
        }

        let bw = bytes_written.load(Ordering::Relaxed);

        // Poll server-side metrics from the web API.
        let server = poll_server_metrics(&http, &web_url).await;

        stats.take_snapshot(
            server.memtable_bytes,
            server.sstable_count,
            bw,
            server.s3_object_count,
            0,
        );

        let current_writes = stats.writes.load(Ordering::Relaxed);
        let delta_writes = current_writes.saturating_sub(prev_writes);
        let wps = delta_writes * 2;
        throughput_history.push(wps);
        if throughput_history.len() > 120 {
            throughput_history.remove(0);
        }
        prev_writes = current_writes;

        let snap = resource_monitor::sample_resources(0, server.sstable_count);
        last_resource_snap = Some(snap.clone());
        match resource_mon.record(snap) {
            LeakVerdict::Abort(reason) => {
                eprintln!("[ABORT] {reason}");
                abort_reason = Some(reason);
                break;
            }
            LeakVerdict::Warning(warnings) => {
                leak_warnings = warnings.len();
                if dashboard.is_none() {
                    for w in &warnings {
                        eprintln!("[WARN] {w}");
                    }
                }
            }
            LeakVerdict::Healthy => {
                leak_warnings = 0;
            }
        }

        if let Some(ref mut d) = dashboard {
            let elapsed = start.elapsed();
            let secs = elapsed.as_secs_f64().max(0.001);
            let total_w = stats.writes.load(Ordering::Relaxed);
            let total_r = stats.reads.load(Ordering::Relaxed);

            let frame = TuiFrame {
                profile_name: format!("{} (cluster: {} nodes)", profile.name, nodes.len()),
                elapsed_secs: secs,
                duration_secs: profile.duration.as_secs_f64(),
                total_writes: total_w,
                total_reads: total_r,
                total_updates: stats.updates.load(Ordering::Relaxed),
                total_deletes: stats.deletes.load(Ordering::Relaxed),
                write_errors: stats.write_errors.load(Ordering::Relaxed),
                read_errors: stats.read_errors.load(Ordering::Relaxed),
                writes_per_sec: total_w as f64 / secs,
                reads_per_sec: total_r as f64 / secs,
                write_latency: stats.write_hist.percentiles(),
                read_latency: stats.read_hist.percentiles(),
                memtable_bytes: server.memtable_bytes,
                sstable_count: server.sstable_count,
                bytes_written: bw,
                s3_uploads: server.s3_object_count,
                bytes_reclaimed: server.s3_bytes,
                resources: last_resource_snap.clone(),
                throughput_history: throughput_history.clone(),
                abort_reason: abort_reason.clone(),
                leak_warnings,
            };
            let _ = d.render(&frame);
        }

        if bw >= profile.target_data_size_bytes {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        let _ = handle.await;
    }

    if let Some(ref mut d) = dashboard {
        d.restore();
    }
    drop(dashboard);

    let resource_summary = resource_mon.summary();
    let bw_final = bytes_written.load(Ordering::Relaxed);

    // Final server metrics.
    let final_server = rt_poll_server(&http, &web_url).await;

    // CQL-based integrity verification: sample keys from ground truth and
    // read them back via CQL to verify values match.
    let integrity = verify_via_cql(nodes, &ground_truth, 1000).await;

    let stats = Arc::try_unwrap(stats)
        .unwrap_or_else(|_| panic!("worker tasks still hold Arc<StatsCollector>"));
    let br_final = bytes_read.load(Ordering::Relaxed);
    stats.finalize(FinalizeContext {
        profile_name: &format!("{} (cluster: {} nodes)", profile.name, nodes.len()),
        bytes_written: bw_final,
        bytes_read: br_final,
        compaction_tasks: 0,
        s3_uploads: final_server.s3_object_count,
        s3_deletes: 0,
        bytes_reclaimed: final_server.s3_bytes,
        sstable_count_final: final_server.sstable_count,
        missing_keys: integrity.missing,
        data_mismatches: integrity.mismatched,
        keys_verified: integrity.checked,
        resource_summary,
        abort_reason,
    })
}

/// Poll server metrics (used at end of test for final report).
async fn rt_poll_server(http: &reqwest::Client, web_url: &str) -> ServerMetrics {
    poll_server_metrics(http, web_url).await
}

struct IntegrityResult {
    checked: u64,
    missing: u64,
    mismatched: u64,
}

/// Verify data integrity by reading a sample of keys back via CQL and
/// comparing against ground truth.  Runs after all writers have stopped,
/// so there are no concurrent mutations — the snapshot is stable.
async fn verify_via_cql(
    nodes: &[SocketAddr],
    ground_truth: &GroundTruth,
    max_sample: usize,
) -> IntegrityResult {
    let snapshot = ground_truth.snapshot();
    if snapshot.is_empty() {
        return IntegrityResult {
            checked: 0,
            missing: 0,
            mismatched: 0,
        };
    }

    // Connect to the first node for verification reads.
    let mut client = match CqlClient::connect(nodes[0]).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("integrity: connect failed: {e}");
            return IntegrityResult {
                checked: 0,
                missing: 0,
                mismatched: 0,
            };
        }
    };
    if let Err(e) = client.query("USE load_test").await {
        eprintln!("integrity: USE failed: {e}");
        return IntegrityResult {
            checked: 0,
            missing: 0,
            mismatched: 0,
        };
    }

    // Deterministic stride-based sampling.
    let total = snapshot.len();
    let step = if total <= max_sample {
        1
    } else {
        total / max_sample
    };
    let mut checked = 0u64;
    let mut missing = 0u64;
    let mut mismatched = 0u64;

    let keys: Vec<_> = snapshot.keys().collect();
    for (i, key) in keys.iter().enumerate() {
        if i % step != 0 {
            continue;
        }
        if checked >= max_sample as u64 {
            break;
        }
        checked += 1;

        let (expected_val, _ts, is_deleted) = &snapshot[*key];
        let cql = format!("SELECT val FROM data WHERE pk = '{key}' AND ck = 1");
        match client.query_quorum(&cql).await {
            Ok(result) => {
                if *is_deleted {
                    // Deleted key — either no rows or a row is acceptable.
                    continue;
                }
                if result.rows.is_empty() {
                    missing += 1;
                    if missing <= 5 {
                        eprintln!("integrity: missing key '{key}'");
                    }
                } else if let Some(got) = result.rows[0].columns.first().and_then(|c| c.as_deref())
                {
                    if got != expected_val.as_slice() {
                        mismatched += 1;
                        if mismatched <= 5 {
                            eprintln!(
                                "integrity: mismatch key '{key}': expected {} bytes (ts={}), got {} bytes",
                                expected_val.len(),
                                _ts,
                                got.len()
                            );
                            // Diagnostic: read from each node with WRITETIME to compare
                            let ts_cql = format!(
                                "SELECT val, writetime(val) FROM data WHERE pk = '{key}' AND ck = 1"
                            );
                            for node in nodes {
                                if let Ok(mut nc) = CqlClient::connect(*node).await {
                                    let _ = nc.query("USE load_test").await;
                                    if let Ok(nr) = nc.query(&ts_cql).await {
                                        if let Some(row) = nr.rows.first() {
                                            let val_len = row
                                                .columns
                                                .first()
                                                .and_then(|c| c.as_ref())
                                                .map(|v| v.len());
                                            let wt = row
                                                .columns
                                                .get(1)
                                                .and_then(|c| c.as_ref())
                                                .and_then(|v| {
                                                    if v.len() == 8 {
                                                        Some(i64::from_be_bytes(
                                                            v[..8].try_into().unwrap(),
                                                        ))
                                                    } else {
                                                        None
                                                    }
                                                });
                                            eprintln!(
                                                "  {node}: {} bytes, writetime={}",
                                                val_len.map_or("NULL".into(), |l| l.to_string()),
                                                wt.map_or("NULL".into(), |t| t.to_string())
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    mismatched += 1;
                    if mismatched <= 5 {
                        eprintln!("integrity: key '{key}' returned NULL value");
                    }
                }
            }
            Err(e) => {
                mismatched += 1;
                if mismatched <= 5 {
                    eprintln!("integrity: read error for '{key}': {e}");
                }
            }
        }
    }

    eprintln!("Integrity: {checked} keys verified, {missing} missing, {mismatched} mismatches");
    IntegrityResult {
        checked,
        missing,
        mismatched,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nodes_single() {
        let nodes = parse_nodes("127.0.0.1:9042");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], "127.0.0.1:9042".parse().unwrap());
    }

    #[test]
    fn parse_nodes_multiple() {
        let nodes = parse_nodes("127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044");
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn parse_nodes_with_spaces() {
        let nodes = parse_nodes("127.0.0.1:9042 , 127.0.0.1:9043");
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn web_port_mapping() {
        let addr: SocketAddr = "127.0.0.1:9042".parse().unwrap();
        assert_eq!(web_port(&addr), 9090);
        let addr2: SocketAddr = "127.0.0.1:9043".parse().unwrap();
        assert_eq!(web_port(&addr2), 9091);
    }
}
