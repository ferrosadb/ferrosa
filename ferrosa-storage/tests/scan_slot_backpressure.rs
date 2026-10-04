//! A range-scan producer must not hold its scheduler-pool slot while it is
//! parked on consumer backpressure.
//!
//! Every `range_iter*` producer runs inside a `ferrosa_sched` pool slot (there
//! are `cores - 1` of them) and hands partitions to its consumer through a
//! 4-item channel. When the consumer stops pulling, the producer parks in the
//! channel send. If it parks HOLDING its slot, the slot is lost to everyone
//! else for as long as the consumer is idle — and the consumer may itself be
//! waiting on another scan that needs a slot. That is a hold-and-wait deadlock.
//!
//! The live shape (ferrosa PR #499 CI, `quorum_scan_reads_from_a_live_replica_
//! when_the_first_is_down`): a coordinator opens two cluster scans and drains
//! them one at a time. The undrained scan's local producer parks holding a
//! slot; the drained scan's N-way merge needs a local fragment AND a remote
//! window continuation, each needing a slot, while its own local producer parks
//! (holding another) waiting for the merge. On a 4-vCPU runner (3 slots) the
//! merge starved for 30 s: "a fragment-merge source produced no fragment
//! within 30000ms".
//!
//! This test is host-independent: it parks exactly `capacity` scans (whatever
//! the host's slot count is) and then requires one more scan to complete.
//!
//! Its own binary on purpose: the scheduler pool is process-global, so a
//! concurrent test's scans would change how many slots are free.

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

/// More partitions than the producer's channel buffer (4) plus the one item
/// each parked consumer takes, so every parked producer is genuinely blocked
/// in its send rather than finished.
const PARTITIONS: usize = 64;

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: u64::MAX / 2,
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
        let key = DecoratedKey::new(PartitionKey::new(format!("pk-{i:04}").into_bytes()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(format!("v-{i}").into_bytes(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id, &key, row, 1000).unwrap();
    }
    engine.flush(&table_id).unwrap();
    (engine, table_id)
}

/// `capacity` scans whose consumers have stopped pulling must not starve the
/// next scan: it must still be admitted and run to completion.
#[test]
fn parked_scan_consumers_do_not_starve_a_new_scan() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let (engine, table_id) = seeded_engine(dir.path());
        let capacity = ferrosa_sched::global_pool().capacity();
        assert!(capacity >= 1, "the scan pool always has a slot");

        // Park `capacity` scans: pull ONE partition from each (so its producer
        // has been admitted and is running), then stop. Each producer fills its
        // channel and blocks in the send — the shape of a consumer that is busy
        // elsewhere (a second cluster scan the coordinator drains later).
        let mut parked = Vec::with_capacity(capacity);
        for i in 0..capacity {
            let mut stream = engine.range_iter_fragmented(&table_id, None, None);
            let first = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "parked scan {i} of {capacity} was never admitted: a slot was \
                         still held by an earlier parked scan"
                    )
                });
            assert!(
                matches!(first, Some(Ok(_))),
                "parked scan {i} must yield a partition"
            );
            parked.push(stream);
        }

        // Every slot is now owned by a scan whose consumer is idle. A fresh scan
        // must still be admitted and complete.
        let fresh = engine.range_iter_fragmented(&table_id, None, None);
        let drained = tokio::time::timeout(Duration::from_secs(30), fresh.collect::<Vec<_>>())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "a new scan starved for 30s behind {capacity} scans parked on consumer \
                     backpressure: their producers hold scheduler slots while blocked in \
                     the channel send"
                )
            });
        assert_eq!(drained.len(), PARTITIONS, "the fresh scan must be complete");
        assert!(
            drained.iter().all(|p| p.is_ok()),
            "the fresh scan must not fail"
        );

        // The parked scans are still intact once their consumers resume.
        for (i, stream) in parked.into_iter().enumerate() {
            let rest = tokio::time::timeout(Duration::from_secs(30), stream.collect::<Vec<_>>())
                .await
                .unwrap_or_else(|_| panic!("parked scan {i} never resumed"));
            assert_eq!(
                rest.len(),
                PARTITIONS - 1,
                "parked scan {i} must deliver every remaining partition after resuming"
            );
        }
        engine.shutdown().unwrap();
    });
}
