//! Shared test fixtures for self-heal unit tests.
//!
//! Builds a *real* [`StorageEngine`] with N healthy SSTable generations on
//! disk via the normal write/flush path, then exposes helpers to corrupt one
//! generation the same way the engine's own corruption tests do (truncate
//! `Data.db`). Reusing the production write path guarantees the detector and
//! quarantine action are exercised against genuine SSTables, not hand-rolled
//! files that could drift from the real format.

#![cfg(test)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use tempfile::TempDir;

use crate::engine::{StorageEngine, StorageEngineConfig};
use crate::TableId;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
        extensions: Default::default(),
    }
}

fn make_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

/// Build an engine, write+flush `n` generations, return the engine, its temp
/// data dir (kept alive by the caller) and the table's SSTable directory.
pub fn table_dir_with_n_generations(n: u64) -> (Arc<StorageEngine>, TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    let tid = TableId::new("test_ks", "test_table");
    engine.register_table(test_schema()).unwrap();

    for i in 0..n {
        let key = DecoratedKey::new(PartitionKey::new(format!("pk{i}").into_bytes()));
        let ts = 1000 + i as i64;
        engine
            .write(&tid, &key, make_row(format!("v{i}").as_bytes(), ts), ts)
            .unwrap();
        engine.flush(&tid).unwrap();
    }

    let table_dir = engine.table_sstable_dir(&tid);
    (Arc::new(engine), dir, table_dir)
}

/// Like [`table_dir_with_n_generations`] but returns only the engine, keeping
/// the temp data dir alive for the process lifetime (test-only leak — the OS
/// reclaims it at exit). Convenient for controller tests that drive the engine
/// directly and don't need to hold the `TempDir`.
pub fn table_dir_with_n_generations_engine(n: u64) -> Arc<StorageEngine> {
    let (engine, dir, _table_dir) = table_dir_with_n_generations(n);
    // Intentionally keep the temp dir from being deleted while the test runs.
    std::mem::forget(dir);
    engine
}

/// Truncate the `Data.db` of the highest generation in `table_dir` so it
/// opens but fails full iteration — the warn-mode corruption signature.
/// Returns the corrupted generation number.
pub fn corrupt_one_generation(table_dir: &Path) -> u64 {
    let gens = StorageEngine::list_generations_in_dir(table_dir);
    let gen = *gens.first().expect("at least one generation present");
    let data_file = StorageEngine::generation_component_path_for_test(table_dir, gen, "Data.db")
        .expect("Data.db present");
    let original_len = std::fs::metadata(&data_file).unwrap().len();
    assert!(original_len > 8, "need bytes to truncate");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&data_file)
        .unwrap()
        .set_len(original_len / 2)
        .unwrap();
    gen
}
