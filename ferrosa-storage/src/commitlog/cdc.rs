//! Module: Provide bounded durable CDC replay from explicit cursors or checkpoints.
//! Correctness: Correct when pages are strict-after, count-and-byte bounded, gaps
//! fail typed, and active/rotated segments are consumed without feed materialization.
//! Last revised: 2026-08-29
//! Last changed: Added durable client cursors, bounded pages, and incremental tailing.
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
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use super::config::{CommitLogPosition, TableId};
use super::mutation::Mutation;
use super::reader::{SegmentEntryRead, SegmentReader};

const CDC_CHECKPOINT_FILENAME: &str = "cdc_checkpoint.json";
const MAX_CDC_CHECKPOINT_BYTES: usize = 4096;
pub const MAX_DURABLE_CDC_PAGE_MUTATIONS: usize = 128;
pub const MAX_DURABLE_CDC_PAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CdcPageLimit(usize);

impl CdcPageLimit {
    pub fn new(value: usize) -> Result<Self, CdcReplayError> {
        if value == 0 || value > MAX_DURABLE_CDC_PAGE_MUTATIONS {
            return Err(CdcReplayError::InvalidPageLimit {
                requested: value,
                maximum: MAX_DURABLE_CDC_PAGE_MUTATIONS,
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

/// One bounded durable page. Callers must consume and discard `entries`
/// before requesting the next page.
pub struct DurableCdcPage {
    pub entries: Vec<(Mutation, CommitLogPosition)>,
    pub high_water: CommitLogPosition,
}

/// Fail-loud durable replay errors exposed to client-facing CDC adapters.
#[derive(Debug)]
pub enum CdcReplayError {
    Storage(ferrosa_common::Error),
    CursorExpired {
        requested_segment: u64,
        oldest_retained_segment: Option<u64>,
    },
    InvalidCursor {
        requested_segment: u64,
        newest_retained_segment: Option<u64>,
    },
    InvalidPageLimit {
        requested: usize,
        maximum: usize,
    },
    EventTooLarge {
        bytes: usize,
        maximum: usize,
    },
}

impl std::fmt::Display for CdcReplayError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "durable CDC storage error: {error}"),
            Self::CursorExpired {
                requested_segment,
                oldest_retained_segment,
            } => write!(
                formatter,
                "durable CDC cursor segment {requested_segment} is no longer retained; oldest retained segment is {oldest_retained_segment:?}"
            ),
            Self::InvalidCursor {
                requested_segment,
                newest_retained_segment,
            } => write!(
                formatter,
                "durable CDC cursor segment {requested_segment} is ahead of retained data; newest retained segment is {newest_retained_segment:?}"
            ),
            Self::InvalidPageLimit { requested, maximum } => write!(
                formatter,
                "durable CDC page limit {requested} is outside 1..={maximum}"
            ),
            Self::EventTooLarge { bytes, maximum } => write!(
                formatter,
                "durable CDC event is {bytes} bytes, exceeding the {maximum}-byte page budget"
            ),
        }
    }
}

impl std::error::Error for CdcReplayError {}

impl From<ferrosa_common::Error> for CdcReplayError {
    fn from(error: ferrosa_common::Error) -> Self {
        Self::Storage(error)
    }
}

/// Persists the CDC reader's position so it can resume after restart.
pub(crate) struct CdcCheckpoint;

impl CdcCheckpoint {
    /// Loads the CDC checkpoint. Returns `None` if no checkpoint exists.
    pub fn load(dir: &Path) -> ferrosa_common::Result<Option<CommitLogPosition>> {
        let path = dir.join(CDC_CHECKPOINT_FILENAME);
        match File::open(&path) {
            Ok(mut file) => {
                let mut data = [0_u8; MAX_CDC_CHECKPOINT_BYTES + 1];
                let mut bytes_read = 0usize;
                while bytes_read < data.len() {
                    let count = file.read(&mut data[bytes_read..])?;
                    if count == 0 {
                        break;
                    }
                    bytes_read += count;
                }
                if bytes_read > MAX_CDC_CHECKPOINT_BYTES {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "cdc checkpoint exceeds {MAX_CDC_CHECKPOINT_BYTES} bytes"
                    )));
                }
                let pos: CdcPosition =
                    serde_json::from_slice(&data[..bytes_read]).map_err(|e| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "cdc checkpoint not valid JSON: {e}"
                        ))
                    })?;
                Ok(Some(CommitLogPosition {
                    segment_id: pos.segment_id,
                    offset: pos.offset,
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ferrosa_common::Error::Io(error)),
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
/// ```text
/// let mut reader = CdcReader::new(log_dir, checkpoint_dir, None)?;
/// while let Some((mutation, pos)) = reader.next_mutation()? {
///     process(mutation);
///     reader.save_checkpoint()?; // persist progress
/// }
/// ```
pub struct CdcReader {
    log_dir: PathBuf,
    checkpoint_dir: Option<PathBuf>,
    table_filter: Option<HashSet<TableId>>,
    position: CommitLogPosition,
    /// Incremental reader for the current segment. It owns no segment-sized
    /// buffer and yields one mutation payload at a time.
    current_segment: Option<SegmentReader>,
    /// True after the current active segment reached its durable tail. The next
    /// call refreshes that same reader unless a newer segment has appeared.
    current_segment_caught_up: bool,
    /// One already-decoded entry that did not fit in the previous byte-bounded
    /// page. Keeping at most one entry prevents cumulative page materialization.
    pending_entry: Option<(Mutation, CommitLogPosition)>,
    /// Lowest segment ID eligible for the next allocation-free directory scan.
    /// The reader rescans after catch-up so newly-created segments become visible.
    next_segment_floor: u64,
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
            checkpoint_dir: Some(checkpoint_dir.to_path_buf()),
            table_filter,
            position,
            current_segment: None,
            current_segment_caught_up: false,
            pending_entry: None,
            next_segment_floor: position.segment_id,
        })
    }

    /// Creates a reader positioned strictly after a client-provided durable
    /// cursor. This reader never writes the server-side singleton checkpoint;
    /// cursor ownership remains with the authenticated client.
    pub fn from_position(
        log_dir: &Path,
        position: CommitLogPosition,
        table_filter: Option<HashSet<TableId>>,
    ) -> Result<Self, CdcReplayError> {
        let reader = Self {
            log_dir: log_dir.to_path_buf(),
            checkpoint_dir: None,
            table_filter,
            position,
            current_segment: None,
            current_segment_caught_up: false,
            pending_entry: None,
            next_segment_floor: position.segment_id,
        };
        reader.validate_explicit_position()?;
        Ok(reader)
    }

    /// Returns the next mutation, or `None` if no more are available.
    ///
    /// Scans segment files lazily on first call. Returns mutations in
    /// commit log order (segment ID, then offset within segment).
    pub fn next_mutation(
        &mut self,
    ) -> ferrosa_common::Result<Option<(Mutation, CommitLogPosition)>> {
        match self.next_mutation_with_payload_limit(usize::MAX) {
            Ok(entry) => Ok(entry),
            Err(CdcReplayError::Storage(error)) => Err(error),
            Err(error) => Err(ferrosa_common::Error::InvalidData(error.to_string())),
        }
    }

    fn next_mutation_with_payload_limit(
        &mut self,
        maximum_payload_bytes: usize,
    ) -> Result<Option<(Mutation, CommitLogPosition)>, CdcReplayError> {
        if let Some((mutation, position)) = self.pending_entry.take() {
            self.position = position;
            return Ok(Some((mutation, position)));
        }
        if self.current_segment_caught_up {
            if let Some(reader) = self.current_segment.as_mut() {
                reader.refresh()?;
            }
            self.current_segment_caught_up = false;
        }
        loop {
            if let Some(reader) = self.current_segment.as_mut() {
                while let Some(entry) = reader.next_entry_bounded(maximum_payload_bytes)? {
                    let (pos, mutation) = match entry {
                        SegmentEntryRead::Mutation(pos, mutation) => (pos, mutation),
                        SegmentEntryRead::PayloadTooLarge { position: _, bytes } => {
                            return Err(CdcReplayError::EventTooLarge {
                                bytes,
                                maximum: maximum_payload_bytes,
                            });
                        }
                    };
                    if pos <= self.position {
                        continue;
                    }
                    if let Some(ref filter) = self.table_filter {
                        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
                        if !filter.contains(&table_id) {
                            self.position = pos;
                            continue;
                        }
                    }
                    self.position = pos;
                    return Ok(Some((mutation, pos)));
                }
                let current_id = reader.segment_id();
                if let Some((_segment_id, path)) =
                    self.find_segment_at_or_after(current_id.saturating_add(1))?
                {
                    self.current_segment = Some(SegmentReader::open(&path)?);
                    continue;
                }
                self.current_segment_caught_up = true;
                return Ok(None);
            }

            // Current segment exhausted: scan without collecting the directory.
            // If caught up, the next call scans again and can discover a segment
            // created after this call returned `None`.
            let Some((_segment_id, path)) =
                self.find_segment_at_or_after(self.next_segment_floor)?
            else {
                return Ok(None);
            };
            self.current_segment = Some(SegmentReader::open(&path)?);
        }
    }

    /// Saves the current position as a checkpoint.
    pub fn save_checkpoint(&self) -> ferrosa_common::Result<()> {
        let checkpoint_dir = self.checkpoint_dir.as_ref().ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(
                "client-positioned CDC readers cannot write the shared checkpoint".into(),
            )
        })?;
        CdcCheckpoint::save(checkpoint_dir, &self.position)
    }

    /// Returns the current reader position.
    pub fn position(&self) -> CommitLogPosition {
        self.position
    }

    /// Read at most `limit` mutations. The vector is hard-bounded by both
    /// [`MAX_DURABLE_CDC_PAGE_MUTATIONS`] and [`MAX_DURABLE_CDC_PAGE_BYTES`]
    /// and is discarded between requests.
    pub fn read_page(&mut self, limit: CdcPageLimit) -> Result<DurableCdcPage, CdcReplayError> {
        let mut entries = Vec::with_capacity(limit.get());
        let mut resident_bytes = 0usize;
        let mut high_water = self.position;
        while entries.len() < limit.get() {
            let Some((mutation, position)) =
                self.next_mutation_with_payload_limit(MAX_DURABLE_CDC_PAGE_BYTES)?
            else {
                // The reader may have skipped filtered mutations after the last
                // returned entry. Advancing across them avoids rescanning them
                // on the next client request.
                high_water = self.position;
                break;
            };
            let mutation_bytes = mutation.serialized_size();
            if mutation_bytes > MAX_DURABLE_CDC_PAGE_BYTES {
                self.pending_entry = Some((mutation, position));
                self.position = high_water;
                return Err(CdcReplayError::EventTooLarge {
                    bytes: mutation_bytes,
                    maximum: MAX_DURABLE_CDC_PAGE_BYTES,
                });
            }
            if !entries.is_empty()
                && resident_bytes.saturating_add(mutation_bytes) > MAX_DURABLE_CDC_PAGE_BYTES
            {
                self.pending_entry = Some((mutation, position));
                self.position = high_water;
                break;
            }
            resident_bytes += mutation_bytes;
            high_water = position;
            entries.push((mutation, position));
        }
        Ok(DurableCdcPage {
            entries,
            high_water,
        })
    }

    /// Finds only the next eligible segment. Resident segment-index memory is
    /// constant regardless of retention depth.
    fn find_segment_at_or_after(
        &self,
        segment_floor: u64,
    ) -> ferrosa_common::Result<Option<(u64, PathBuf)>> {
        let mut next: Option<(u64, PathBuf)> = None;
        if self.log_dir.exists() {
            for entry in std::fs::read_dir(&self.log_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id) = super::parse_segment_id(name) {
                        if id >= segment_floor
                            && next.as_ref().is_none_or(|(next_id, _)| id < *next_id)
                        {
                            next = Some((id, path));
                        }
                    }
                }
            }
        }
        Ok(next)
    }

    fn validate_explicit_position(&self) -> Result<(), CdcReplayError> {
        if self.position.segment_id == 0 {
            return if self.position.offset == 0 {
                Ok(())
            } else {
                Err(CdcReplayError::InvalidCursor {
                    requested_segment: 0,
                    newest_retained_segment: None,
                })
            };
        }
        let mut oldest: Option<u64> = None;
        let mut newest: Option<u64> = None;
        let mut requested_path = None;
        if self.log_dir.exists() {
            for entry in std::fs::read_dir(&self.log_dir).map_err(ferrosa_common::Error::from)? {
                let path = entry.map_err(ferrosa_common::Error::from)?.path();
                let Some(id) = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(super::parse_segment_id)
                else {
                    continue;
                };
                oldest = Some(oldest.map_or(id, |value| value.min(id)));
                newest = Some(newest.map_or(id, |value| value.max(id)));
                if id == self.position.segment_id {
                    requested_path = Some(path);
                }
            }
        }
        if let Some(path) = requested_path {
            let file_len = std::fs::metadata(&path)
                .map_err(ferrosa_common::Error::from)?
                .len();
            if self.position.offset < file_len {
                let mut reader = SegmentReader::open(&path)?;
                while let Some(entry) = reader.next_entry_bounded(MAX_DURABLE_CDC_PAGE_BYTES)? {
                    let position = match entry {
                        SegmentEntryRead::Mutation(position, _mutation) => position,
                        SegmentEntryRead::PayloadTooLarge { bytes, .. } => {
                            return Err(CdcReplayError::EventTooLarge {
                                bytes,
                                maximum: MAX_DURABLE_CDC_PAGE_BYTES,
                            });
                        }
                    };
                    if position.offset == self.position.offset {
                        return Ok(());
                    }
                    if position.offset > self.position.offset {
                        break;
                    }
                }
            }
            return Err(CdcReplayError::InvalidCursor {
                requested_segment: self.position.segment_id,
                newest_retained_segment: newest,
            });
        }

        if oldest.is_some_and(|id| id > self.position.segment_id)
            || newest.is_some_and(|id| id > self.position.segment_id)
        {
            Err(CdcReplayError::CursorExpired {
                requested_segment: self.position.segment_id,
                oldest_retained_segment: oldest,
            })
        } else {
            Err(CdcReplayError::InvalidCursor {
                requested_segment: self.position.segment_id,
                newest_retained_segment: newest,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::config::CommitLogPosition;
    use crate::commitlog::{CommitLog, CommitLogConfig};
    use ferrosa_common::CellValue;

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
    fn durable_cdc_checkpoint_rejects_oversized_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CDC_CHECKPOINT_FILENAME),
            [b'x'; MAX_CDC_CHECKPOINT_BYTES + 1],
        )
        .unwrap();

        let error = CdcCheckpoint::load(dir.path()).unwrap_err();
        assert!(matches!(error, ferrosa_common::Error::InvalidFormat(_)));
        assert!(error.to_string().contains("exceeds 4096 bytes"));
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
    fn durable_cdc_reader_resumes_strictly_after_explicit_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        cl.append(&test_mutation("ks", "before")).unwrap();
        let cursor = cl.append(&test_mutation("ks", "acknowledged")).unwrap();
        cl.append(&test_mutation("ks", "after")).unwrap();
        cl.shutdown().unwrap();

        let mut reader = CdcReader::from_position(dir.path(), cursor, None).unwrap();
        let (mutation, position) = reader.next_mutation().unwrap().unwrap();
        assert_eq!(mutation.table, "after");
        assert!(position > cursor);
        assert!(reader.next_mutation().unwrap().is_none());
    }

    #[test]
    fn durable_cdc_reader_rejects_cursor_that_is_not_an_entry_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let cl = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        let valid_position = cl.append(&test_mutation("ks", "one")).unwrap();
        cl.shutdown().unwrap();

        let error = CdcReader::from_position(
            dir.path(),
            CommitLogPosition {
                segment_id: valid_position.segment_id,
                offset: valid_position.offset + 1,
            },
            None,
        )
        .err()
        .expect("cursor inside an entry must fail before replay");
        assert!(matches!(error, CdcReplayError::InvalidCursor { .. }));
    }

    #[test]
    fn durable_cdc_reader_returns_typed_gap_when_cursor_segment_was_recycled() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();
        let cursor = cl.append(&test_mutation("ks", "expired")).unwrap();
        cl.force_rotate().unwrap();
        cl.append(&test_mutation("ks", "retained")).unwrap();
        cl.shutdown().unwrap();

        let expired_segment = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(super::super::parse_segment_id)
                    == Some(cursor.segment_id)
            })
            .unwrap();
        std::fs::remove_file(expired_segment).unwrap();

        assert!(matches!(
            CdcReader::from_position(dir.path(), cursor, None),
            Err(CdcReplayError::CursorExpired { .. })
        ));
    }

    #[test]
    fn durable_cdc_page_is_bounded_and_its_watermark_resumes_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();
        cl.append(&test_mutation("ks", "one")).unwrap();
        cl.append(&test_mutation("ks", "two")).unwrap();
        cl.append(&test_mutation("ks", "three")).unwrap();
        cl.shutdown().unwrap();

        let mut reader = CdcReader::from_position(
            dir.path(),
            CommitLogPosition {
                segment_id: 0,
                offset: 0,
            },
            None,
        )
        .unwrap();
        let page = reader.read_page(CdcPageLimit::new(2).unwrap()).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].0.table, "one");
        assert_eq!(page.entries[1].0.table, "two");

        let mut resumed = CdcReader::from_position(dir.path(), page.high_water, None).unwrap();
        let next = resumed.read_page(CdcPageLimit::new(2).unwrap()).unwrap();
        assert_eq!(next.entries.len(), 1);
        assert_eq!(next.entries[0].0.table, "three");
    }

    #[test]
    fn durable_cdc_page_never_exceeds_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 4 * 1024 * 1024,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();
        for table in ["one", "two", "three"] {
            let mut mutation = test_mutation("ks", table);
            mutation.rows[0].cells[0].1 = CellValue::live(vec![0x5a; 600 * 1024], 1000);
            cl.append(&mutation).unwrap();
        }
        cl.shutdown().unwrap();

        let mut reader = CdcReader::from_position(
            dir.path(),
            CommitLogPosition {
                segment_id: 0,
                offset: 0,
            },
            None,
        )
        .unwrap();
        let page = reader.read_page(CdcPageLimit::new(3).unwrap()).unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0.table, "one");
        let resident_bytes: usize = page
            .entries
            .iter()
            .map(|(mutation, _)| mutation.serialized_size())
            .sum();
        assert!(resident_bytes <= MAX_DURABLE_CDC_PAGE_BYTES);

        let next = reader.read_page(CdcPageLimit::new(3).unwrap()).unwrap();
        assert_eq!(next.entries.len(), 1);
        assert_eq!(next.entries[0].0.table, "two");

        let mut resumed = CdcReader::from_position(dir.path(), page.high_water, None).unwrap();
        let replayed = resumed.read_page(CdcPageLimit::new(3).unwrap()).unwrap();
        assert_eq!(replayed.entries.len(), 1);
        assert_eq!(replayed.entries[0].0.table, "two");
    }

    #[test]
    fn durable_cdc_reader_discovers_segments_after_catching_up() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        assert!(reader.next_mutation().unwrap().is_none());

        let cl = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        cl.append(&test_mutation("ks", "created_later")).unwrap();
        cl.shutdown().unwrap();

        let (mutation, _) = reader.next_mutation().unwrap().unwrap();
        assert_eq!(mutation.table, "created_later");
    }

    #[test]
    fn durable_cdc_reader_discovers_later_flush_in_same_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let cl = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        cl.append(&test_mutation("ks", "first")).unwrap();
        cl.force_sync().unwrap();

        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        assert_eq!(reader.next_mutation().unwrap().unwrap().0.table, "first");
        assert!(reader.next_mutation().unwrap().is_none());

        cl.append(&test_mutation("ks", "second_same_segment"))
            .unwrap();
        cl.force_sync().unwrap();
        assert_eq!(
            reader.next_mutation().unwrap().unwrap().0.table,
            "second_same_segment"
        );
        cl.shutdown().unwrap();
    }

    #[test]
    fn durable_cdc_reader_drains_late_entry_before_rotated_successor() {
        let dir = tempfile::tempdir().unwrap();
        let cl = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        cl.append(&test_mutation("ks", "first")).unwrap();
        cl.force_sync().unwrap();

        let mut reader = CdcReader::new(dir.path(), dir.path(), None).unwrap();
        assert_eq!(reader.next_mutation().unwrap().unwrap().0.table, "first");
        assert!(reader.next_mutation().unwrap().is_none());

        cl.append(&test_mutation("ks", "late_in_old_segment"))
            .unwrap();
        cl.force_rotate().unwrap();
        cl.append(&test_mutation("ks", "first_in_new_segment"))
            .unwrap();
        cl.force_sync().unwrap();

        assert_eq!(
            reader.next_mutation().unwrap().unwrap().0.table,
            "late_in_old_segment"
        );
        assert_eq!(
            reader.next_mutation().unwrap().unwrap().0.table,
            "first_in_new_segment"
        );
        assert!(reader.next_mutation().unwrap().is_none());
        cl.shutdown().unwrap();
    }

    #[test]
    fn durable_cdc_page_fails_loud_for_event_larger_than_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 4 * 1024 * 1024,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();
        let mut mutation = test_mutation("ks", "oversized");
        mutation.rows[0].cells[0].1 =
            CellValue::live(vec![0x5a; MAX_DURABLE_CDC_PAGE_BYTES + 1], 1000);
        cl.append(&mutation).unwrap();
        cl.shutdown().unwrap();

        let mut reader = CdcReader::from_position(
            dir.path(),
            CommitLogPosition {
                segment_id: 0,
                offset: 0,
            },
            None,
        )
        .unwrap();
        let error = reader
            .read_page(CdcPageLimit::new(1).unwrap())
            .err()
            .expect("oversized event must fail instead of being truncated or skipped");
        assert!(matches!(
            error,
            CdcReplayError::EventTooLarge {
                maximum: MAX_DURABLE_CDC_PAGE_BYTES,
                ..
            }
        ));
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
            mutation_id: [0x11u8; 16],
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
