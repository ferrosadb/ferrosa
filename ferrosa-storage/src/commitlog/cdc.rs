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

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::config::{CommitLogPosition, TableId};
use super::mutation::Mutation;
use super::reader::SegmentReader;

const CDC_CHECKPOINT_FILENAME: &str = "cdc_checkpoint.json";

/// Persists the CDC reader's position so it can resume after restart.
pub(crate) struct CdcCheckpoint;

impl CdcCheckpoint {
    /// Loads the CDC checkpoint. Returns `None` if no checkpoint exists.
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

#[derive(serde::Serialize, serde::Deserialize)]
struct CdcPosition {
    segment_id: u64,
    offset: u64,
}

/// Tails commit log segments and yields mutations.
///
/// The reader scans segment files in order (by segment ID), starting from
/// the last checkpointed position. It optionally filters by table ID.
///
/// # Usage
///
/// ```ignore
/// let mut reader = CdcReader::new(log_dir, checkpoint_dir, None)?;
/// while let Some((mutation, pos)) = reader.next_mutation()? {
///     process(mutation);
///     reader.save_checkpoint()?; // persist progress
/// }
/// ```
pub struct CdcReader {
    log_dir: PathBuf,
    checkpoint_dir: PathBuf,
    table_filter: Option<HashSet<TableId>>,
    position: CommitLogPosition,
    /// Buffered mutations from the current segment, with positions.
    buffer: Vec<(CommitLogPosition, Mutation)>,
    /// Index into buffer for the next mutation to yield.
    buffer_cursor: usize,
    /// Segment files we haven't processed yet, sorted by segment ID.
    pending_segments: Vec<(u64, PathBuf)>,
    /// Whether we've scanned for segment files.
    scanned: bool,
}

impl CdcReader {
    /// Creates a new CDC reader.
    ///
    /// If `table_filter` is `Some`, only mutations for those tables are yielded.
    /// If `None`, all mutations are yielded.
    ///
    /// Starts from the last saved checkpoint, or from the beginning if none.
    pub fn new(
        log_dir: &Path,
        checkpoint_dir: &Path,
        table_filter: Option<HashSet<TableId>>,
    ) -> ferrosa_common::Result<Self> {
        let position = CdcCheckpoint::load(checkpoint_dir)?.unwrap_or(CommitLogPosition {
            segment_id: 0,
            offset: 0,
        });

        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            checkpoint_dir: checkpoint_dir.to_path_buf(),
            table_filter,
            position,
            buffer: Vec::new(),
            buffer_cursor: 0,
            pending_segments: Vec::new(),
            scanned: false,
        })
    }

    /// Returns the next mutation, or `None` if no more are available.
    ///
    /// Scans segment files lazily on first call. Returns mutations in
    /// commit log order (segment ID, then offset within segment).
    pub fn next_mutation(
        &mut self,
    ) -> ferrosa_common::Result<Option<(Mutation, CommitLogPosition)>> {
        loop {
            // Yield from buffer if available.
            if self.buffer_cursor < self.buffer.len() {
                let (pos, mutation) = self.buffer[self.buffer_cursor].clone();
                self.buffer_cursor += 1;
                self.position = pos;
                return Ok(Some((mutation, pos)));
            }

            // Buffer exhausted -- load next segment.
            if !self.scanned {
                self.scan_segments()?;
                self.scanned = true;
            }

            if self.pending_segments.is_empty() {
                return Ok(None);
            }

            let (_seg_id, path) = self.pending_segments.remove(0);
            self.load_segment(&path)?;
        }
    }

    /// Saves the current position as a checkpoint.
    pub fn save_checkpoint(&self) -> ferrosa_common::Result<()> {
        CdcCheckpoint::save(&self.checkpoint_dir, &self.position)
    }

    /// Returns the current reader position.
    pub fn position(&self) -> CommitLogPosition {
        self.position
    }

    /// Scans log_dir for segment files, sorted by ID, filtered to those
    /// after our checkpoint position.
    fn scan_segments(&mut self) -> ferrosa_common::Result<()> {
        let mut segments = Vec::new();
        if self.log_dir.exists() {
            for entry in std::fs::read_dir(&self.log_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id) = super::parse_segment_id(name) {
                        if id >= self.position.segment_id {
                            segments.push((id, path));
                        }
                    }
                }
            }
        }
        segments.sort_by_key(|(id, _)| *id);
        self.pending_segments = segments;
        Ok(())
    }

    /// Loads a segment file into the buffer, filtering by table and position.
    fn load_segment(&mut self, path: &Path) -> ferrosa_common::Result<()> {
        let mut reader = SegmentReader::open(path)?;
        let entries = reader.read_all()?;

        self.buffer.clear();
        self.buffer_cursor = 0;

        for (pos, mutation) in entries {
            // Skip entries at or before our checkpoint position.
            if pos <= self.position {
                continue;
            }
            // Apply table filter.
            if let Some(ref filter) = self.table_filter {
                let table_id = TableId::new(&mutation.keyspace, &mutation.table);
                if !filter.contains(&table_id) {
                    continue;
                }
            }
            self.buffer.push((pos, mutation));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::config::CommitLogPosition;
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

    #[test]
    fn reader_yields_mutations_from_segment() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let m1 = test_mutation("ks", "t1");
        let m2 = test_mutation("ks", "t2");
        cl.append(&m1).unwrap();
        cl.append(&m2).unwrap();
        cl.shutdown().unwrap();

        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        let mut results = Vec::new();
        while let Some((mutation, _pos)) = reader.next_mutation().unwrap() {
            results.push(mutation);
        }
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].table, "t1");
        assert_eq!(results[1].table, "t2");
    }

    #[test]
    fn reader_filters_by_table() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        cl.append(&test_mutation("ks", "wanted")).unwrap();
        cl.append(&test_mutation("ks", "ignored")).unwrap();
        cl.append(&test_mutation("ks", "wanted")).unwrap();
        cl.shutdown().unwrap();

        let filter = HashSet::from([TableId::new("ks", "wanted")]);
        let mut reader = CdcReader::new(dir.path(), dir.path(), Some(filter)).unwrap();

        let mut results = Vec::new();
        while let Some((mutation, _)) = reader.next_mutation().unwrap() {
            results.push(mutation);
        }
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|m| m.table == "wanted"));
    }

    #[test]
    fn reader_resumes_from_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        cl.append(&test_mutation("ks", "t1")).unwrap();
        let mid_pos = cl.append(&test_mutation("ks", "t2")).unwrap();
        cl.append(&test_mutation("ks", "t3")).unwrap();
        cl.shutdown().unwrap();

        // Save checkpoint at mid_pos.
        CdcCheckpoint::save(dir.path(), &mid_pos).unwrap();

        // New reader should start after mid_pos.
        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        let mut results = Vec::new();
        while let Some((mutation, _)) = reader.next_mutation().unwrap() {
            results.push(mutation);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "t3");
    }

    #[test]
    fn reader_follows_across_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Small segments to force rotation.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        // Append enough mutations to span multiple segments.
        let mut expected_count = 0;
        for i in 0..10 {
            if cl.append(&test_mutation("ks", &format!("t{i}"))).is_ok() {
                expected_count += 1;
            }
        }
        cl.shutdown().unwrap();

        // Verify multiple segment files exist.
        let segment_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
            })
            .count();
        assert!(
            segment_count >= 2,
            "need multiple segments, got {segment_count}"
        );

        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        let mut results = Vec::new();
        while let Some((mutation, _)) = reader.next_mutation().unwrap() {
            results.push(mutation);
        }
        assert_eq!(results.len(), expected_count);
    }

    fn test_mutation(ks: &str, table: &str) -> Mutation {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Mutation {
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
