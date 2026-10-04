//! A range-scan producer never waits on its thread for its consumer.
//!
//! The consumer of a range scan often needs a blocking thread itself to make
//! room: the PG executor pulls rows with `blocking_recv` on `spawn_blocking`.
//! A producer that waits for room on its own blocking thread therefore holds
//! the very resource its consumer needs. With a few such producers the whole
//! bounded blocking pool sits in those waits, every wait runs to its timeout,
//! and every scan behind them crawls: on ferrosa PR #502 CI (4 vCPU) another
//! session's `SELECT` made no progress in 20 s beside 8 slow PG readers
//! (`pg_stalled_reader_holds_no_thread`), and locally it took 9 s against
//! 0.1 s alone, with all four blocking threads in the producers' grace waits.
//!
//! The invariant: a producer never waits on its thread. When the channel is
//! full it pauses at once, returning its slot AND its thread; the async
//! supervisor waits for room. So while consumers are behind, no scheduler
//! carrier thread is busy (`ferrosa_sched::scan_carrier_threads_busy`).
//!
//! Its own binary: both counters are process-wide.

use std::path::Path;
use std::time::Duration;

use futures::StreamExt;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

const PARTITIONS: usize = 2_000;

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_secs(1),
            },
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

fn seeded_engine(dir: &Path) -> (StorageEngine, TableId) {
    let engine = StorageEngine::new(engine_config(dir), None).unwrap();
    engine
        .register_table(TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        })
        .unwrap();
    let table_id = TableId::new("ks", "t");
    for i in 0..PARTITIONS {
        let key = DecoratedKey::new(PartitionKey::new(format!("pk-{i:06}").into_bytes()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(format!("v-{i}").into_bytes(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &key, row, 1000).unwrap();
    }
    (engine, table_id)
}

#[test]
fn consumers_that_fall_behind_hold_no_producer_thread() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let (engine, table_id) = seeded_engine(dir.path());

        // Consumers that stopped reading: more of them than carrier threads.
        // Each scan fills its channel and pauses; none may keep a thread.
        let stalled_count = 2 * ferrosa_sched::scan_carrier_threads_capacity();
        let releases_before = ferrosa_sched::scan_releases_total();
        let mut stalled = Vec::with_capacity(stalled_count);
        for _ in 0..stalled_count {
            let mut stream = engine.range_iter_fragmented(&table_id, None, None);
            let first = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .expect("a scan was never admitted beside stalled ones");
            assert!(matches!(first, Some(Ok(_))), "each scan yields a partition");
            stalled.push(stream);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while ferrosa_sched::scan_releases_total() - releases_before < stalled_count as u64 {
            assert!(
                std::time::Instant::now() < deadline,
                "only {} of {stalled_count} stalled scans paused",
                ferrosa_sched::scan_releases_total() - releases_before
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            ferrosa_sched::scan_carrier_threads_busy(),
            0,
            "{stalled_count} scans whose consumers stopped reading still occupy carrier \
             threads: a producer is waiting on its thread"
        );

        // A consumer slower than the producer, but never stalled, still gets
        // every partition, through pauses and resumes.
        let resumes_before = ferrosa_storage::range_scan_resumes_total();
        let mut stream = engine.range_iter(&table_id, None, None);
        let mut delivered = 0usize;
        let drained = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(item) = stream.next().await {
                item.expect("the scan delivers partitions");
                delivered += 1;
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        })
        .await;
        assert!(drained.is_ok(), "the slow consumer's scan never finished");
        assert_eq!(delivered, PARTITIONS, "every partition is delivered");
        assert!(
            ferrosa_storage::range_scan_resumes_total() > resumes_before,
            "premise: the slow consumer must really fall behind the producer"
        );

        // The stalled scans are intact once their consumers read again.
        for (i, stream) in stalled.into_iter().enumerate() {
            let rest = tokio::time::timeout(Duration::from_secs(30), stream.collect::<Vec<_>>())
                .await
                .unwrap_or_else(|_| panic!("stalled scan {i} never resumed"));
            assert_eq!(
                rest.len(),
                PARTITIONS - 1,
                "stalled scan {i} delivers the rest"
            );
        }
        engine.shutdown().unwrap();
    });
}
