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
}
