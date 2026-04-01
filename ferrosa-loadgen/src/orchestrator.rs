//! Load test orchestrator — spawns writers, readers, stats, integrity tasks.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use ferrosa_storage::commitlog::TableId;
use ferrosa_storage::engine::StorageEngine;

use crate::generator::{choose_op, choose_value_len, make_key_string, make_random_value, OpType};
use crate::ground_truth::GroundTruth;
use crate::integrity::IntegrityVerifier;
use crate::profile::LoadProfile;
use crate::resource_monitor::{self, LeakVerdict, ResourceMonitor};
use crate::stats::{LoadStats, StatsCollector};
use crate::tui::{TuiDashboard, TuiFrame};

/// Create the table schema used by load tests.
pub fn load_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "load_test".to_string(),
        table: "data".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn make_tombstone_row(timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![],
        deletion: DeletionTime::new(timestamp, timestamp as u32),
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

/// Run a load test with text output (no TUI).
pub fn run_load_test(profile: &LoadProfile, engine: &StorageEngine) -> LoadStats {
    run_load_test_inner(profile, engine, false)
}

/// Run a load test with a live TUI dashboard.
pub fn run_load_test_with_tui(profile: &LoadProfile, engine: &StorageEngine) -> LoadStats {
    run_load_test_inner(profile, engine, true)
}

fn run_load_test_inner(
    profile: &LoadProfile,
    engine: &StorageEngine,
    enable_tui: bool,
) -> LoadStats {
    let table_id = TableId::new("load_test", "data");
    let ground_truth = GroundTruth::new();
    let stats = StatsCollector::new();
    let stop = AtomicBool::new(false);
    let bytes_written = AtomicU64::new(0);
    let mut resource_mon = ResourceMonitor::new(4);
    let mut abort_reason: Option<String> = None;
    let mut throughput_history: Vec<u64> = Vec::new();
    let mut leak_warnings: usize = 0;
    let mut last_resource_snap: Option<resource_monitor::ResourceSnapshot> = None;

    // Initialize TUI if requested.
    let mut dashboard = if enable_tui {
        match TuiDashboard::init() {
            Ok(d) => Some(d),
            Err(e) => {
                eprintln!("Failed to initialize TUI: {e}. Falling back to text output.");
                None
            }
        }
    } else {
        None
    };

    // Register table (ignore if already registered from previous test).
    let _ = engine.register_table(load_test_schema());

    let total_workers = profile.num_writers + profile.num_readers;

    std::thread::scope(|s| {
        for worker_id in 0..total_workers {
            let tid = &table_id;
            let gt = &ground_truth;
            let sc = &stats;
            let st = &stop;
            let bw = &bytes_written;
            let key_space = profile.key_space_size;
            let value_range = profile.value_size_range;
            let read_ratio = profile.read_ratio;
            let update_ratio = profile.update_ratio;
            let delete_ratio = profile.delete_ratio;

            s.spawn(move || {
                let mut rng = rand::thread_rng();
                let mut local_ts = (worker_id as i64) * 1_000_000_000;

                while !st.load(Ordering::Relaxed) {
                    let op = choose_op(&mut rng, read_ratio, update_ratio, delete_ratio);
                    let key_idx = rand::Rng::gen_range(&mut rng, 0..key_space);
                    let key_str = make_key_string(key_idx);

                    match op {
                        OpType::Write | OpType::Update => {
                            let val_len = choose_value_len(&mut rng, value_range.0, value_range.1);
                            let value = make_random_value(&mut rng, val_len);
                            local_ts += 1;
                            let dk = make_key(&key_str);
                            let t0 = Instant::now();
                            match engine.write(tid, &dk, make_row(&value, local_ts), local_ts) {
                                Ok(()) => {
                                    let latency = t0.elapsed();
                                    gt.record_write(&key_str, &value, local_ts);
                                    if matches!(op, OpType::Update) {
                                        sc.record_update(latency);
                                    } else {
                                        sc.record_write(latency);
                                    }
                                    bw.fetch_add(val_len as u64, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    sc.record_write_error();
                                    sc.record_error_sample(e);
                                }
                            }
                        }
                        OpType::Delete => {
                            local_ts += 1;
                            let dk = make_key(&key_str);
                            let t0 = Instant::now();
                            match engine.write(tid, &dk, make_tombstone_row(local_ts), local_ts) {
                                Ok(()) => {
                                    let latency = t0.elapsed();
                                    gt.record_delete(&key_str, local_ts);
                                    sc.record_delete(latency);
                                }
                                Err(e) => {
                                    sc.record_write_error();
                                    sc.record_error_sample(e);
                                }
                            }
                        }
                        OpType::Read => {
                            let dk = make_key(&key_str);
                            let t0 = Instant::now();
                            match engine.read(tid, &dk) {
                                Ok(result) => {
                                    let latency = t0.elapsed();
                                    let got = result
                                        .and_then(|p| p.rows.into_iter().next())
                                        .and_then(|r| r.cells.into_iter().next())
                                        .and_then(|(_, c)| c.value);
                                    gt.record_read(&key_str, got.as_deref());
                                    sc.record_read(latency);
                                }
                                Err(_) => sc.record_read_error(),
                            }
                        }
                    }
                }
            });
        }

        // Main thread: periodic flushes, compaction, stats, resource checks,
        // and TUI rendering. Compaction polling is async, so we need a
        // tokio runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create tokio runtime for compaction polling");
        let start = Instant::now();
        let mut prev_writes: u64 = 0;

        while start.elapsed() < profile.duration {
            std::thread::sleep(Duration::from_millis(500));

            // Check for user quit (TUI mode).
            if let Some(ref mut d) = dashboard {
                if d.poll_quit() {
                    abort_reason = Some("user pressed q".to_string());
                    break;
                }
            }

            let _ = engine.flush(&table_id);
            let _ = engine.discard_completed_commit_log_segments();

            // Poll compaction: collect completed tasks, submit new ones.
            // Without this, SSTables accumulate unboundedly and RSS grows
            // until the test is aborted.
            rt.block_on(engine.poll_compactions());

            let memtable_bytes = engine.memtable_size(&table_id) as u64;
            let sst_count = engine.sstable_count(&table_id) as u64;
            let bw = bytes_written.load(Ordering::Relaxed);
            let s3_up = engine
                .compaction_metrics
                .s3_uploads_total
                .load(Ordering::Relaxed) as u64;
            let reclaimed = engine
                .compaction_metrics
                .input_bytes_reclaimed
                .load(Ordering::Relaxed) as u64;

            stats.take_snapshot(memtable_bytes, sst_count, bw, s3_up, reclaimed);

            // Track throughput history for sparkline (writes/sec over last sample).
            let current_writes = stats.writes.load(Ordering::Relaxed);
            let delta_writes = current_writes.saturating_sub(prev_writes);
            // We sample every ~500ms, so approximate writes/sec.
            let wps = delta_writes * 2;
            throughput_history.push(wps);
            // Keep last 120 samples (60 seconds of history).
            if throughput_history.len() > 120 {
                throughput_history.remove(0);
            }
            prev_writes = current_writes;

            // Sample OS resources and check for leaks.
            let cl_segments = engine.commit_log_closed_segment_count() as u64;
            let snap = resource_monitor::sample_resources(cl_segments, sst_count);
            last_resource_snap = Some(snap.clone());
            match resource_mon.record(snap) {
                LeakVerdict::Abort(reason) => {
                    if dashboard.is_none() {
                        eprintln!("[ABORT] Resource limit exceeded: {reason}");
                    }
                    abort_reason = Some(reason);
                    break;
                }
                LeakVerdict::Warning(warnings) => {
                    leak_warnings = warnings.len();
                    if dashboard.is_none() {
                        for w in &warnings {
                            eprintln!("[WARN] Probable resource leak: {w}");
                        }
                    }
                }
                LeakVerdict::Healthy => {
                    leak_warnings = 0;
                }
            }

            // Render TUI frame.
            if let Some(ref mut d) = dashboard {
                let elapsed = start.elapsed();
                let secs = elapsed.as_secs_f64().max(0.001);
                let total_w = stats.writes.load(Ordering::Relaxed);
                let total_r = stats.reads.load(Ordering::Relaxed);

                let frame = TuiFrame {
                    profile_name: profile.name.clone(),
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
                    memtable_bytes,
                    sstable_count: sst_count,
                    bytes_written: bw,
                    s3_uploads: s3_up,
                    bytes_reclaimed: reclaimed,
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
    });

    // Restore terminal before printing final report.
    if let Some(ref mut d) = dashboard {
        d.restore();
    }
    drop(dashboard);

    // Final flush.
    let _ = engine.flush(&table_id);

    // Final integrity check.
    let report = IntegrityVerifier::verify_all(engine, &table_id, &ground_truth);

    let (_, _, _, _, _, total_bytes) = ground_truth.stats();
    let sstable_count = engine.sstable_count(&table_id) as u64;
    let s3_uploads = engine
        .compaction_metrics
        .s3_uploads_total
        .load(Ordering::Relaxed) as u64;
    let s3_deletes = engine
        .compaction_metrics
        .s3_deletes_total
        .load(Ordering::Relaxed) as u64;
    let bytes_reclaimed = engine
        .compaction_metrics
        .input_bytes_reclaimed
        .load(Ordering::Relaxed) as u64;

    let resource_summary = resource_mon.summary();

    stats.finalize(
        &profile.name,
        total_bytes,
        0,
        s3_uploads,
        s3_deletes,
        bytes_reclaimed,
        sstable_count,
        report.missing_keys.len() as u64,
        report.mismatched_keys.len() as u64,
        report.keys_checked,
        resource_summary,
        abort_reason,
    )
}
