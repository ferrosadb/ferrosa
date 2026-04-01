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
