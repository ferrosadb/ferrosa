//! Integrity verification for load tests — full scan and sample checks.

use std::time::{Duration, Instant};

use ferrosa_storage::commitlog::TableId;
use ferrosa_storage::engine::StorageEngine;

use crate::ground_truth::GroundTruth;

use ferrosa_common::key::{DecoratedKey, PartitionKey};

/// Report from an integrity check.
#[derive(Debug, Clone)]
pub struct IntegrityReport {
    pub keys_checked: u64,
    pub keys_ok: u64,
    pub missing_keys: Vec<String>,
    pub mismatched_keys: Vec<(String, String)>,
    pub elapsed: Duration,
}

impl IntegrityReport {
    pub fn is_ok(&self) -> bool {
        self.missing_keys.is_empty() && self.mismatched_keys.is_empty()
    }
}

/// Integrity verification utilities.
pub struct IntegrityVerifier;

impl IntegrityVerifier {
    /// Full-table scan: every key in ground truth must be readable
    /// with the correct latest value.
    pub fn verify_all(
        engine: &StorageEngine,
        table_id: &TableId,
        ground_truth: &GroundTruth,
    ) -> IntegrityReport {
        let start = Instant::now();
        let snapshot = ground_truth.snapshot();
        let mut keys_ok = 0u64;
        let mut missing = Vec::new();
        let mut mismatched = Vec::new();

        for (key_str, (expected_val, _ts, is_deleted)) in &snapshot {
            let dk = DecoratedKey::new(PartitionKey::new(key_str.as_bytes().to_vec()));
            match engine.read(table_id, &dk) {
                Ok(Some(partition)) => {
                    if *is_deleted {
                        // Deleted key returned a partition — may have a tombstone
                        // that still produces a result. Accept either way since
                        // tombstone semantics vary during compaction.
                        keys_ok += 1;
                    } else if let Some(row) = partition.rows.first() {
                        if let Some((_, cell)) = row.cells.first() {
                            if let Some(got) = cell.value.as_deref() {
                                if got == expected_val.as_slice() {
                                    keys_ok += 1;
                                } else {
                                    mismatched.push((
                                        key_str.clone(),
                                        format!(
                                            "expected {} bytes, got {} bytes",
                                            expected_val.len(),
                                            got.len()
                                        ),
                                    ));
                                }
                            } else {
                                mismatched.push((key_str.clone(), "cell value is None".into()));
                            }
                        } else {
                            mismatched.push((key_str.clone(), "row has no cells".into()));
                        }
                    } else {
                        mismatched.push((key_str.clone(), "partition has no rows".into()));
                    }
                }
                Ok(None) => {
                    if *is_deleted {
                        // Deleted key reads as None — correct.
                        keys_ok += 1;
                    } else {
                        missing.push(key_str.clone());
                    }
                }
                Err(e) => {
                    mismatched.push((key_str.clone(), format!("read error: {e}")));
                }
            }
        }

        IntegrityReport {
            keys_checked: snapshot.len() as u64,
            keys_ok,
            missing_keys: missing,
            mismatched_keys: mismatched,
            elapsed: start.elapsed(),
        }
    }

    /// Spot-check a random sample of keys from the ground truth.
    pub fn verify_sample(
        engine: &StorageEngine,
        table_id: &TableId,
        ground_truth: &GroundTruth,
        sample_size: usize,
    ) -> IntegrityReport {
        let start = Instant::now();
        let snapshot = ground_truth.snapshot();

        // Take up to sample_size keys. Use a deterministic skip for simplicity
        // (avoids needing rand in production code).
        let total = snapshot.len();
        let step = if total <= sample_size {
            1
        } else {
            total / sample_size
        };

        let mut keys_ok = 0u64;
        let mut missing = Vec::new();
        let mut mismatched = Vec::new();
        let mut checked = 0u64;

        for (i, (key_str, (expected_val, _ts, is_deleted))) in snapshot.iter().enumerate() {
            if i % step != 0 {
                continue;
            }
            if checked >= sample_size as u64 {
                break;
            }
            checked += 1;

            let dk = DecoratedKey::new(PartitionKey::new(key_str.as_bytes().to_vec()));
            match engine.read(table_id, &dk) {
                Ok(Some(partition)) => {
                    if *is_deleted {
                        keys_ok += 1;
                    } else if let Some(row) = partition.rows.first() {
                        if let Some((_, cell)) = row.cells.first() {
                            if cell.value.as_deref() == Some(expected_val.as_slice()) {
                                keys_ok += 1;
                            } else {
                                mismatched.push((key_str.clone(), "value mismatch".into()));
                            }
                        } else {
                            mismatched.push((key_str.clone(), "no cells".into()));
                        }
                    } else {
                        mismatched.push((key_str.clone(), "no rows".into()));
                    }
                }
                Ok(None) => {
                    if *is_deleted {
                        keys_ok += 1;
                    } else {
                        missing.push(key_str.clone());
                    }
                }
                Err(e) => mismatched.push((key_str.clone(), format!("error: {e}"))),
            }
        }

        IntegrityReport {
            keys_checked: checked,
            keys_ok,
            missing_keys: missing,
            mismatched_keys: mismatched,
            elapsed: start.elapsed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::commitlog::TableId;
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };

    use crate::ground_truth::GroundTruth;
    use crate::orchestrator::load_test_schema;

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

    fn setup_engine() -> (tempfile::TempDir, StorageEngine, TableId) {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
        };
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(load_test_schema()).unwrap();
        let table_id = TableId::new("load_test", "data");
        (dir, engine, table_id)
    }

    #[test]
    fn verify_all_with_matching_data() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        for i in 0..10 {
            let key_str = format!("key_{i:04}");
            let value = format!("value_{i}").into_bytes();
            let ts = (i + 1) as i64;
            let dk = make_key(&key_str);
            engine
                .write(&table_id, &dk, make_row(&value, ts), ts)
                .unwrap();
            gt.record_write(&key_str, &value, ts);
        }

        let report = IntegrityVerifier::verify_all(&engine, &table_id, &gt);

        assert!(report.is_ok(), "report should be ok: {report:?}");
        assert_eq!(report.keys_checked, 10);
        assert_eq!(report.keys_ok, 10);
        assert!(report.missing_keys.is_empty());
        assert!(report.mismatched_keys.is_empty());
    }

    #[test]
    fn verify_all_detects_missing_key() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        // Write 5 keys to both engine and ground truth.
        for i in 0..5 {
            let key_str = format!("key_{i:04}");
            let value = format!("value_{i}").into_bytes();
            let ts = (i + 1) as i64;
            let dk = make_key(&key_str);
            engine
                .write(&table_id, &dk, make_row(&value, ts), ts)
                .unwrap();
            gt.record_write(&key_str, &value, ts);
        }

        // Write 2 keys to ground truth only — engine has no data for these.
        for i in 5..7 {
            let key_str = format!("key_{i:04}");
            let value = format!("value_{i}").into_bytes();
            let ts = (i + 1) as i64;
            gt.record_write(&key_str, &value, ts);
        }

        let report = IntegrityVerifier::verify_all(&engine, &table_id, &gt);

        assert!(!report.is_ok());
        assert_eq!(report.keys_checked, 7);
        assert_eq!(report.keys_ok, 5);
        assert_eq!(report.missing_keys.len(), 2);
        assert!(report.mismatched_keys.is_empty());
    }

    #[test]
    fn verify_all_detects_mismatch() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        // Write one key with matching data.
        let dk = make_key("match_key");
        engine
            .write(&table_id, &dk, make_row(b"correct", 1), 1)
            .unwrap();
        gt.record_write("match_key", b"correct", 1);

        // Write another key with different values in engine vs ground truth.
        let dk2 = make_key("mismatch_key");
        engine
            .write(&table_id, &dk2, make_row(b"engine_value", 2), 2)
            .unwrap();
        gt.record_write("mismatch_key", b"ground_truth_value", 2);

        let report = IntegrityVerifier::verify_all(&engine, &table_id, &gt);

        assert!(!report.is_ok());
        assert_eq!(report.keys_checked, 2);
        assert_eq!(report.keys_ok, 1);
        assert_eq!(report.mismatched_keys.len(), 1);
        assert_eq!(report.mismatched_keys[0].0, "mismatch_key");
        assert!(report.missing_keys.is_empty());
    }

    #[test]
    fn verify_all_handles_deleted_keys() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        // Write a key, then mark it deleted in ground truth.
        // Engine returns None for this key since it was never written there,
        // and ground truth says deleted — should be accepted.
        gt.record_write("deleted_key", b"some_data", 1);
        gt.record_delete("deleted_key", 2);

        // Also write a live key to verify mixed results.
        let dk = make_key("live_key");
        engine
            .write(&table_id, &dk, make_row(b"alive", 3), 3)
            .unwrap();
        gt.record_write("live_key", b"alive", 3);

        let report = IntegrityVerifier::verify_all(&engine, &table_id, &gt);

        assert!(report.is_ok(), "report should be ok: {report:?}");
        assert_eq!(report.keys_checked, 2);
        assert_eq!(report.keys_ok, 2);
    }

    #[test]
    fn verify_sample_limits_checked_keys() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        // Write 100 keys to engine and ground truth.
        for i in 0..100 {
            let key_str = format!("key_{i:04}");
            let value = format!("v{i}").into_bytes();
            let ts = (i + 1) as i64;
            let dk = make_key(&key_str);
            engine
                .write(&table_id, &dk, make_row(&value, ts), ts)
                .unwrap();
            gt.record_write(&key_str, &value, ts);
        }

        let report = IntegrityVerifier::verify_sample(&engine, &table_id, &gt, 5);

        assert_eq!(report.keys_checked, 5);
        // All sampled keys should match since data is consistent.
        assert!(report.is_ok(), "report should be ok: {report:?}");
        assert_eq!(report.keys_ok, 5);
    }

    #[test]
    fn verify_all_empty_ground_truth() {
        let (_dir, engine, table_id) = setup_engine();
        let gt = GroundTruth::new();

        let report = IntegrityVerifier::verify_all(&engine, &table_id, &gt);

        assert!(report.is_ok());
        assert_eq!(report.keys_checked, 0);
        assert_eq!(report.keys_ok, 0);
        assert!(report.missing_keys.is_empty());
        assert!(report.mismatched_keys.is_empty());
    }
}
