//! Change Data Capture: tails commit log segments from a checkpoint.
//!
//! [`CdcReader`] reads durable segment files, filters by table, and yields
//! mutations with their commit log positions. Consumers checkpoint their
//! progress so they can resume after restart.
//!
//! # Design
//!
//! CDC reads from on-disk segment files (post-fsync), not from in-memory
//! buffers. This means CDC visibility lags behind writes by at most the
//! sync strategy interval (e.g., 10ms for Periodic). This is a deliberate
//! tradeoff: reading committed data simplifies the concurrency model and
//! guarantees at-least-once delivery of durable mutations.

use std::path::Path;

use super::config::CommitLogPosition;

#[allow(dead_code)]
const CDC_CHECKPOINT_FILENAME: &str = "cdc_checkpoint.json";

/// Persists the CDC reader's position so it can resume after restart.
#[allow(dead_code)]
pub(crate) struct CdcCheckpoint;

impl CdcCheckpoint {
    /// Loads the CDC checkpoint. Returns `None` if no checkpoint exists.
    #[allow(dead_code)]
    pub fn load(dir: &Path) -> ferrosa_common::Result<Option<CommitLogPosition>> {
        let path = dir.join(CDC_CHECKPOINT_FILENAME);
        match std::fs::read(&path) {
            Ok(data) => {
                let pos: CdcPosition = serde_json::from_slice(&data).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "cdc checkpoint not valid JSON: {e}"
                    ))
                })?;
                Ok(Some(CommitLogPosition {
                    segment_id: pos.segment_id,
                    offset: pos.offset,
                }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ferrosa_common::Error::Io(e)),
        }
    }

    /// Saves the CDC checkpoint atomically (tmp + rename).
    #[allow(dead_code)]
    pub fn save(dir: &Path, position: &CommitLogPosition) -> ferrosa_common::Result<()> {
        let tmp = dir.join("cdc_checkpoint.json.tmp");
        let final_path = dir.join(CDC_CHECKPOINT_FILENAME);
        let data = serde_json::to_vec_pretty(&CdcPosition {
            segment_id: position.segment_id,
            offset: position.offset,
        })
        .map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to serialize cdc checkpoint: {e}"))
        })?;
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &final_path)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(serde::Serialize, serde::Deserialize)]
struct CdcPosition {
    segment_id: u64,
    offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::config::CommitLogPosition;
    // Used by reader_yields_mutations_from_segment (commented out until CdcReader is implemented).
    #[allow(unused_imports)]
    use crate::commitlog::{CommitLog, CommitLogConfig};

    #[test]
    fn checkpoint_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let pos = CommitLogPosition {
            segment_id: 5,
            offset: 1024,
        };
        CdcCheckpoint::save(dir.path(), &pos).unwrap();
        let loaded = CdcCheckpoint::load(dir.path()).unwrap();
        assert_eq!(loaded, Some(pos));
    }

    #[test]
    fn checkpoint_load_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = CdcCheckpoint::load(dir.path()).unwrap();
        assert_eq!(loaded, None);
    }

    // TODO: Uncomment when CdcReader is implemented (Chunk 1, Task 3).
    // #[test]
    // fn reader_yields_mutations_from_segment() {
    //     let dir = tempfile::tempdir().unwrap();
    //     let config = CommitLogConfig::test_config(dir.path());
    //     let cl = CommitLog::new(config).unwrap();
    //
    //     let m1 = test_mutation("ks", "t1");
    //     let m2 = test_mutation("ks", "t2");
    //     cl.append(&m1).unwrap();
    //     cl.append(&m2).unwrap();
    //     cl.shutdown().unwrap();
    //
    //     let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
    //     let mut results = Vec::new();
    //     while let Some((mutation, _pos)) = reader.next_mutation().unwrap() {
    //         results.push(mutation);
    //     }
    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].table, "t1");
    //     assert_eq!(results[1].table, "t2");
    // }

    #[allow(dead_code)]
    fn test_mutation(ks: &str, table: &str) -> super::super::mutation::Mutation {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        super::super::mutation::Mutation {
            keyspace: ks.to_string(),
            table: table.to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 1000,
        }
    }
}
