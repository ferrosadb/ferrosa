//! Bulk write stability tests.
//!
//! These tests verify that sustained high-throughput writes do not degrade
//! cluster stability. The primary concern is that rapid CQL inserts can
//! saturate the Data lane (64-slot channel), causing backpressure that
//! starves the tokio runtime and delays Raft heartbeat processing on
//! follower nodes — triggering an election storm.
//!
//! See: specs/bugs/bulk-write-raft-starvation.md

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::pair::node::PairNode;
use ferrosa_cluster::pair::PairRole;
use ferrosa_net::config::NetConfig;
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, Mutation, TableId};

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, PartitionKey, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

// ---------------------------------------------------------------------------
// Helpers (duplicated from integration.rs — a shared test-util crate would
// eliminate this, but keeping it self-contained for now).
// ---------------------------------------------------------------------------

fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

fn register_test_table(storage: &StorageEngine) {
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    };
    storage.register_table(schema).unwrap();
}

fn make_mutation(key_id: u64, timestamp: i64) -> Mutation {
    let key_bytes = key_id.to_be_bytes().to_vec();
    Mutation {
        mutation_id: {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&key_id.to_be_bytes());
            id[8..].copy_from_slice(&timestamp.to_be_bytes());
            id
        },
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        key: DecoratedKey {
            token: Token(key_id as i64),
            key: PartitionKey::new(key_bytes),
        },
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"value".to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }],
        timestamp,
    }
}

/// Set up a 2-node PairNode cluster and return both nodes + storages.
async fn setup_pair() -> (PairNode, PairNode, Arc<StorageEngine>, Arc<StorageEngine>) {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    // Leak tempdirs so they outlive the test — they're cleaned up at process exit.
    let dir1 = Box::leak(Box::new(dir1));
    let dir2 = Box::leak(Box::new(dir2));

    let id_primary = Uuid::from_bytes([0xFF; 16]);
    let id_secondary = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    register_test_table(&storage1);
    register_test_table(&storage2);

    // Start secondary first.
    let net2 = Arc::new(test_net_config());
    let node2 = PairNode::new(
        config.clone(),
        net2,
        id_secondary,
        id_primary,
        "127.0.0.1:19999".parse().unwrap(),
        storage2.clone(),
    );
    let addr2 = node2.start().await.unwrap();

    // Start primary pointing to secondary.
    let net1 = Arc::new(test_net_config());
    let node1 = PairNode::new(
        config,
        net1,
        id_primary,
        id_secondary,
        addr2,
        storage1.clone(),
    );
    let addr1 = node1.start().await.unwrap();

    // Connect secondary → primary.
    node2.connect_to_peer(addr1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(node1.role(), PairRole::Primary);
    assert_eq!(node2.role(), PairRole::Secondary);

    (node1, node2, storage1, storage2)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Sustained sequential writes: measures throughput and tail latency.
///
/// This replicates the exact pattern that killed the cluster in production:
/// a restore script firing 2000+ individual INSERT statements sequentially
/// through a single CQL connection.
///
/// Expected: all writes succeed, p99 latency stays bounded.
/// Observed bug: after ~500-1000 writes the Data lane (64 slots) saturates,
/// write latencies spike, and on a 3-node cluster the Raft heartbeat is
/// starved — causing an election storm.
#[tokio::test]
async fn sequential_bulk_writes_complete_without_timeout() {
    let (node1, _node2, _storage1, storage2) = setup_pair().await;

    let num_writes = 2000u64;
    let mut latencies = Vec::with_capacity(num_writes as usize);
    let mut errors = 0u64;

    let start = Instant::now();
    for i in 0..num_writes {
        let mutation = make_mutation(i, (i + 1) as i64 * 1000);
        let t0 = Instant::now();
        match node1.coordinator().coordinate_write(&mutation).await {
            Ok(()) => latencies.push(t0.elapsed()),
            Err(e) => {
                errors += 1;
                if errors == 1 {
                    eprintln!("first error at write {i}: {e}");
                }
            }
        }
    }
    let elapsed = start.elapsed();

    // Sort latencies for percentile calculation.
    latencies.sort();
    let p50 = latencies
        .get(latencies.len() / 2)
        .copied()
        .unwrap_or_default();
    let p99 = latencies
        .get(latencies.len() * 99 / 100)
        .copied()
        .unwrap_or_default();
    let p999 = latencies
        .get(latencies.len() * 999 / 1000)
        .copied()
        .unwrap_or_default();

    let throughput = num_writes as f64 / elapsed.as_secs_f64();

    eprintln!("--- sequential bulk write results ---");
    eprintln!("  writes:     {num_writes} ({errors} errors)");
    eprintln!("  elapsed:    {elapsed:?}");
    eprintln!("  throughput: {throughput:.0} writes/s");
    eprintln!("  p50:        {p50:?}");
    eprintln!("  p99:        {p99:?}");
    eprintln!("  p99.9:      {p999:?}");

    // Assertions: zero errors, reasonable throughput.
    assert_eq!(errors, 0, "{errors} writes failed out of {num_writes}");

    // p99 should be under 100ms for a local 2-node pair.
    // If the lane is saturated, this will blow up to seconds.
    assert!(
        p99 < Duration::from_millis(100),
        "p99 latency {p99:?} exceeds 100ms — lane backpressure likely"
    );

    // Verify replication: data should be on both nodes.
    let table_id = TableId::new("test_ks", "test_tbl");
    let spot_check_key = make_mutation(500, 501_000).key;
    let result = storage2.read(&table_id, &spot_check_key).unwrap();
    assert!(result.is_some(), "write not replicated to secondary");
}

/// Concurrent burst writes: fires N writes in parallel to stress the lane.
///
/// With LANE_CHANNEL_CAPACITY=64, once we exceed 64 concurrent in-flight
/// writes per peer, callers block on `reserve().await`. This test measures
/// how throughput and latency degrade as concurrency increases past the
/// lane capacity.
#[tokio::test]
async fn concurrent_burst_writes_measure_lane_saturation() {
    let (node1, _node2, _storage1, _storage2) = setup_pair().await;

    // Test at increasing concurrency levels.
    let concurrency_levels = [16, 64, 128, 256];
    let writes_per_level = 500u64;

    eprintln!("--- concurrent burst write results ---");
    eprintln!(
        "{:>12} {:>12} {:>12} {:>12} {:>8}",
        "concurrency", "writes/s", "p50", "p99", "errors"
    );

    for concurrency in concurrency_levels {
        let coordinator = node1.coordinator().clone();
        let errors = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicU64::new(0));

        // Use a semaphore to control concurrency.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let start = Instant::now();
        let mut handles = Vec::new();

        for i in 0..writes_per_level {
            let coordinator = coordinator.clone();
            let sem = semaphore.clone();
            let errors = errors.clone();
            let completed = completed.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let mutation =
                    make_mutation(i + concurrency as u64 * 10_000, (i + 1) as i64 * 1000);
                let t0 = Instant::now();
                match coordinator.coordinate_write(&mutation).await {
                    Ok(()) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                        Some(t0.elapsed())
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            }));
        }

        let mut latencies = Vec::new();
        for h in handles {
            if let Ok(Some(d)) = h.await {
                latencies.push(d);
            }
        }
        let elapsed = start.elapsed();

        latencies.sort();
        let p50 = latencies
            .get(latencies.len() / 2)
            .copied()
            .unwrap_or_default();
        let p99 = latencies
            .get(latencies.len() * 99 / 100)
            .copied()
            .unwrap_or_default();
        let err_count = errors.load(Ordering::Relaxed);
        let throughput = writes_per_level as f64 / elapsed.as_secs_f64();

        eprintln!("{concurrency:>12} {throughput:>12.0} {p50:>12.1?} {p99:>12.1?} {err_count:>8}");

        // At concurrency <= 64 (lane capacity), errors should be zero.
        if concurrency <= 64 {
            assert_eq!(
                err_count, 0,
                "concurrency={concurrency}: {err_count} errors (lane should not be saturated)"
            );
        }
    }
}

/// Mixed workload: sustained writes with periodic "heartbeat-like" probes.
///
/// Simulates what happens when Raft heartbeats compete with bulk data writes
/// for tokio runtime scheduling. If the runtime is saturated by storage
/// writes, the probe latency will spike — mirroring how Raft election
/// timeouts get missed in production.
#[tokio::test]
async fn bulk_writes_do_not_starve_probe_latency() {
    let (node1, _node2, _storage1, _storage2) = setup_pair().await;

    let write_count = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn a background writer that fires writes as fast as possible.
    let coordinator = node1.coordinator().clone();
    let wc = write_count.clone();
    let st = stop.clone();
    let writer = tokio::spawn(async move {
        let mut i = 0u64;
        while !st.load(Ordering::Relaxed) {
            let mutation = make_mutation(i, (i + 1) as i64 * 1000);
            let _ = coordinator.coordinate_write(&mutation).await;
            wc.fetch_add(1, Ordering::Relaxed);
            i += 1;
        }
    });

    // Give the writer time to saturate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now measure probe latency: how long does a single write take when
    // the system is under sustained load?
    let mut probe_latencies = Vec::new();
    for probe_id in 0..50u64 {
        let mutation = make_mutation(1_000_000 + probe_id, 999_999_000);
        let t0 = Instant::now();
        let _ = node1.coordinator().coordinate_write(&mutation).await;
        probe_latencies.push(t0.elapsed());
        // Space probes 100ms apart (like Raft heartbeats at 100ms interval).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    stop.store(true, Ordering::Relaxed);
    let _ = writer.await;

    let total_writes = write_count.load(Ordering::Relaxed);

    probe_latencies.sort();
    let p50 = probe_latencies[probe_latencies.len() / 2];
    let p99 = probe_latencies[probe_latencies.len() * 99 / 100];
    let max = *probe_latencies.last().unwrap();

    eprintln!("--- probe latency under sustained write load ---");
    eprintln!("  background writes: {total_writes}");
    eprintln!("  probe p50:  {p50:?}");
    eprintln!("  probe p99:  {p99:?}");
    eprintln!("  probe max:  {max:?}");

    // If the max probe latency exceeds the Raft election timeout (1000ms),
    // an election storm WILL happen in a real 3-node cluster.
    assert!(
        max < Duration::from_millis(1000),
        "probe max latency {max:?} exceeds Raft election timeout (1s) — \
         sustained writes would cause election storm in 3-node cluster"
    );

    // Stricter: probe p99 should stay under the heartbeat interval (300ms).
    // If it doesn't, heartbeats will be delayed and followers will suspect
    // the leader is dead.
    assert!(
        p99 < Duration::from_millis(300),
        "probe p99 {p99:?} exceeds Raft heartbeat interval (300ms) — \
         followers would miss heartbeats under this write load"
    );
}
