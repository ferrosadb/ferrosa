//! Property tests for StorageEngine invariants.

use std::path::Path;
use std::time::Duration;

use proptest::prelude::*;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "prop".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
    }
}

fn test_engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.to_path_buf(),
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

// Property: every write to the engine is subsequently readable.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn all_writes_readable(
        keys in prop::collection::vec("[a-z]{1,8}", 1..=20),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = TableId::new("ks", "prop");

        // Write all keys.
        for (i, key_str) in keys.iter().enumerate() {
            let key = make_key(key_str);
            let ts = i as i64 + 1;
            engine
                .write(&tid, &key, make_row(key_str.as_bytes(), ts), ts)
                .unwrap();
        }

        // All should be readable.
        for key_str in &keys {
            let key = make_key(key_str);
            let result = engine.read(&tid, &key).unwrap();
            prop_assert!(result.is_some(), "key '{}' should be readable", key_str);
        }

        engine.shutdown().unwrap();
    }
}

// Property: writes survive flush — no data loss.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn writes_survive_flush(
        keys in prop::collection::vec("[a-z]{1,6}", 1..=15),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = TableId::new("ks", "prop");

        // Write all keys.
        for (i, key_str) in keys.iter().enumerate() {
            let key = make_key(key_str);
            let ts = i as i64 + 1;
            engine
                .write(&tid, &key, make_row(key_str.as_bytes(), ts), ts)
                .unwrap();
        }

        // Flush.
        engine.flush(&tid).unwrap();

        // All still readable from SSTable.
        for key_str in &keys {
            let key = make_key(key_str);
            let result = engine.read(&tid, &key).unwrap();
            prop_assert!(result.is_some(), "key '{}' should survive flush", key_str);
        }

        engine.shutdown().unwrap();
    }
}

// Property: last-write-wins — overwriting a key always returns the latest value.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn last_write_wins(
        num_overwrites in 2..=5_usize,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = TableId::new("ks", "prop");
        let key = make_key("overwritten");

        let mut last_value = Vec::new();
        for i in 0..num_overwrites {
            let val = format!("value_{i}");
            let ts = (i as i64 + 1) * 1000;
            engine
                .write(&tid, &key, make_row(val.as_bytes(), ts), ts)
                .unwrap();
            last_value = val.into_bytes();

            // Optionally flush mid-stream.
            if i % 2 == 0 {
                engine.flush(&tid).unwrap();
            }
        }

        let result = engine.read(&tid, &key).unwrap().unwrap();
        let read_value = result.rows[0].cells[0].1.value.as_deref().unwrap();
        prop_assert_eq!(read_value, last_value.as_slice());

        engine.shutdown().unwrap();
    }
}
