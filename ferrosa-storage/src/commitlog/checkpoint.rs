//! Checkpoint file: tracks the last flushed [`CommitLogPosition`] per table.
//!
//! The checkpoint is a JSON file written atomically (temp file + rename) so
//! a crash during a write never leaves a partially-written checkpoint on disk.
//!
//! # File format
//!
//! ```json
//! {
//!   "format_version": 1,
//!   "flushed_positions": {
//!     "ks1.table1": { "segment_id": 42, "offset": 8192 }
//!   },
//!   "timestamp": "2026-03-11T12:00:00Z"
//! }
//! ```
//!
//! Keys in `flushed_positions` use `TableId`'s `Display` impl (`"keyspace.table"`).

// Used by CommitLog (Task 9); suppress dead-code warnings until that module exists.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::config::{CommitLogPosition, TableId};

const CHECKPOINT_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_FILENAME: &str = "commitlog_checkpoint.json";

// ---------------------------------------------------------------------------
// On-disk representation
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointFile {
    format_version: u32,
    flushed_positions: HashMap<String, PositionEntry>,
    timestamp: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PositionEntry {
    segment_id: u64,
    offset: u64,
}

// ---------------------------------------------------------------------------
// Public type
// ---------------------------------------------------------------------------

/// Reads and writes the commit log checkpoint file.
///
/// All operations are associated functions so callers don't need to
/// instantiate anything — the checkpoint file itself is the state.
pub struct CommitLogCheckpoint;

impl CommitLogCheckpoint {
    /// Loads the checkpoint file from `dir`.
    ///
    /// Returns an empty map if the file does not exist. Returns an error if
    /// the file exists but cannot be parsed or has an unsupported
    /// `format_version`.
    pub fn load(dir: &Path) -> ferrosa_common::Result<HashMap<TableId, CommitLogPosition>> {
        let path = dir.join(CHECKPOINT_FILENAME);

        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Ok(HashMap::new());
            }
            Err(e) => return Err(ferrosa_common::Error::Io(e)),
        };

        let file: CheckpointFile = serde_json::from_slice(&data).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("checkpoint file is not valid JSON: {e}"))
        })?;

        if file.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(ferrosa_common::Error::UnsupportedVersion(format!(
                "checkpoint format_version {} is not supported (expected {})",
                file.format_version, CHECKPOINT_FORMAT_VERSION
            )));
        }

        let mut positions = HashMap::with_capacity(file.flushed_positions.len());
        for (key, entry) in file.flushed_positions {
            let table_id = parse_table_id(&key)?;
            let pos = CommitLogPosition {
                segment_id: entry.segment_id,
                offset: entry.offset,
            };
            positions.insert(table_id, pos);
        }

        Ok(positions)
    }

    /// Saves `positions` to the checkpoint file in `dir`.
    ///
    /// The write is atomic: data is first written to a `.tmp` file, then
    /// renamed over the final file so a crash never leaves a partial write.
    pub fn save(
        dir: &Path,
        positions: &HashMap<TableId, CommitLogPosition>,
    ) -> ferrosa_common::Result<()> {
        let tmp_path = checkpoint_tmp_path(dir);
        let final_path = dir.join(CHECKPOINT_FILENAME);

        let flushed_positions: HashMap<String, PositionEntry> = positions
            .iter()
            .map(|(table_id, pos)| {
                (
                    table_id.to_string(),
                    PositionEntry {
                        segment_id: pos.segment_id,
                        offset: pos.offset,
                    },
                )
            })
            .collect();

        let file = CheckpointFile {
            format_version: CHECKPOINT_FORMAT_VERSION,
            flushed_positions,
            timestamp: current_timestamp(),
        };

        let json = serde_json::to_vec_pretty(&file).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to serialize checkpoint: {e}"))
        })?;

        if let Err(e) = publish_atomically(&tmp_path, &final_path, &json) {
            // Our own temp file, and only ours: the path carries this process's
            // id and a counter no other writer uses.
            let _ = fs::remove_file(&tmp_path);
            return Err(e.into());
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Where a checkpoint write stages its bytes before publishing them.
///
/// Unique per write. One shared `.tmp` path let two writers land in the same
/// bytes: the shorter document was renamed into place while the longer one was
/// still writing past its end, so what got published was a complete JSON
/// document with a tail after it — and a node refuses to start on that. It took
/// out two of three nodes on one machine on 2026-09-11. The same sharing also
/// made writers steal each other's file, so `rename` failed with ENOENT; 119 of
/// 200 concurrent saves failed that way in the test below.
///
/// In `dir`, never a system temp directory: `rename` is atomic only within a
/// filesystem, and across one it degrades to copy-then-delete.
pub(super) fn staging_path_for(dir: &Path, final_name: &str) -> PathBuf {
    static NEXT_WRITE: AtomicU64 = AtomicU64::new(0);
    let n = NEXT_WRITE.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{final_name}.tmp.{}.{n}", std::process::id()))
}

/// This module's own checkpoint file, staged.
fn checkpoint_tmp_path(dir: &Path) -> PathBuf {
    staging_path_for(dir, CHECKPOINT_FILENAME)
}

/// Write `bytes` and publish them at `final_path`, atomically.
///
/// fsync before the rename, or the rename can be durable while the bytes it
/// points at are not — a crash then publishes a file of zeros. fsync the
/// directory after, or the rename itself can be lost. Neither was done before.
pub(super) fn publish_atomically(
    tmp_path: &Path,
    final_path: &Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    {
        let mut file = fs::File::create(tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, final_path)?;
    if let Some(parent) = final_path.parent() {
        // Best effort, and said out loud: the data is already published and
        // readable; only its survival of a power cut is at stake, and failing
        // the save here would make a durability hint look like a lost write.
        if let Err(e) = fs::File::open(parent).and_then(|d| d.sync_all()) {
            tracing::warn!(
                directory = %parent.display(),
                error = %e,
                "checkpoint published but the directory was not fsynced; \
                 a power cut could lose the rename"
            );
        }
    }
    Ok(())
}

/// Parses a `"keyspace.table"` string into a [`TableId`].
///
/// Returns `Error::InvalidFormat` if the string does not contain exactly one
/// `.` separator.
fn parse_table_id(s: &str) -> ferrosa_common::Result<TableId> {
    match s.splitn(2, '.').collect::<Vec<_>>()[..] {
        [ks, tbl] => Ok(TableId::new(ks, tbl)),
        _ => Err(ferrosa_common::Error::InvalidFormat(format!(
            "checkpoint key {s:?} is not in \"keyspace.table\" format"
        ))),
    }
}

/// Returns a simple ISO 8601–style timestamp string.
///
/// Uses seconds since the Unix epoch for portability; exact format is not
/// load-bearing (the field is informational only).
fn current_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Format as a minimal UTC timestamp: "YYYY-MM-DDTHH:MM:SSZ".
    // We compute this without external crates by hand-rolling the conversion.
    let (year, month, day, hour, min, sec) = secs_to_datetime(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Converts seconds since Unix epoch to (year, month, day, hour, min, sec).
fn secs_to_datetime(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let sec = secs % 60;
    let min = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;
    let days = secs / 86400;

    // Gregorian calendar calculation (valid for dates >= 1970-01-01).
    let mut year = 1970u64;
    let mut remaining = days;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }

    let leap = is_leap(year);
    let month_days: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1u64;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    let day = remaining + 1;
    (year, month, day, hour, min, sec)
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// `count` distinct tables, so two writers can produce documents of very
    /// different lengths.
    fn positions_of(count: usize) -> HashMap<TableId, CommitLogPosition> {
        (0..count)
            .map(|i| {
                (
                    TableId::new("ks", format!("table_{i:05}")),
                    CommitLogPosition {
                        segment_id: i as u64,
                        offset: (i * 64) as u64,
                    },
                )
            })
            .collect()
    }

    fn make_positions() -> HashMap<TableId, CommitLogPosition> {
        let mut map = HashMap::new();
        map.insert(
            TableId::new("ks1", "table1"),
            CommitLogPosition {
                segment_id: 42,
                offset: 8192,
            },
        );
        map.insert(
            TableId::new("ks2", "orders"),
            CommitLogPosition {
                segment_id: 7,
                offset: 1024,
            },
        );
        map
    }

    #[test]
    fn write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let positions = make_positions();

        CommitLogCheckpoint::save(dir.path(), &positions).unwrap();
        let loaded = CommitLogCheckpoint::load(dir.path()).unwrap();

        assert_eq!(loaded.len(), positions.len());
        for (table_id, expected) in &positions {
            let got = loaded.get(table_id).expect("table_id should be present");
            assert_eq!(got.segment_id, expected.segment_id);
            assert_eq!(got.offset, expected.offset);
        }
    }

    #[test]
    fn load_nonexistent_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = CommitLogCheckpoint::load(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn atomic_update() {
        let dir = tempfile::tempdir().unwrap();
        let positions = make_positions();

        CommitLogCheckpoint::save(dir.path(), &positions).unwrap();

        // The final checkpoint file must exist at the expected path.
        let checkpoint_path = dir.path().join(CHECKPOINT_FILENAME);
        assert!(
            checkpoint_path.exists(),
            "checkpoint file should exist at {checkpoint_path:?}"
        );

        // The temp file must NOT linger after a successful save. Named by
        // pattern now rather than by one constant, because each write stages
        // under a path of its own.
        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary file should not remain after save: {leftovers:?}"
        );
    }

    /// THE BUG, reproduced. Two writers sharing one fixed temp path interleave
    /// in it: the shorter one renames its file into place while the longer one
    /// is still writing past its end, and what gets published is a complete
    /// JSON document with a tail of the other writer's bytes after it.
    ///
    /// Found on Ben's machine 2026-09-11, where it took out two of three nodes
    /// at once. Both refused to start on
    /// `InvalidFormat("checkpoint file is not valid JSON: trailing characters
    /// at line 1438 column 2")`; node1's file was 28817 bytes with a valid
    /// 28816-byte document in it, node3's 28662 with a valid 28660.
    ///
    /// The assertion is on the published bytes, not on `load()`: serde stops at
    /// the end of a value, so a reader that only calls `from_slice` would call
    /// a torn file healthy. Trailing bytes are the defect.
    #[test]
    fn concurrent_saves_never_publish_a_file_with_a_tail() {
        let dir = tempfile::tempdir().unwrap();

        // Sizes that differ by a lot, so one writer's document ends well before
        // the other's and a tail is detectable rather than a coin flip.
        let small = positions_of(2);
        let large = positions_of(4_000);

        // Collected rather than unwrapped: with one shared temp path the
        // writers also steal each other's file, so `rename` fails with ENOENT.
        // Both that and a torn file are the same defect, and the report should
        // say which one happened.
        let failures = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                for positions in [&small, &large] {
                    scope.spawn(|| {
                        for _ in 0..25 {
                            if let Err(e) = CommitLogCheckpoint::save(dir.path(), positions) {
                                failures.lock().unwrap().push(e.to_string());
                            }
                        }
                    });
                }
            }
        });

        let failures = failures.into_inner().unwrap();
        assert!(
            failures.is_empty(),
            "{} of 200 concurrent saves failed, first: {}",
            failures.len(),
            failures.first().unwrap(),
        );

        let published = fs::read(dir.path().join(CHECKPOINT_FILENAME)).unwrap();
        // from_slice refuses trailing characters, which is the same judgement
        // the node makes at startup and the same message it printed.
        let parsed = serde_json::from_slice::<CheckpointFile>(&published);
        assert!(
            parsed.is_ok(),
            "published a checkpoint no node can read ({} bytes): {}",
            published.len(),
            parsed.unwrap_err(),
        );
    }

    /// The mechanism. Every in-flight write needs a path of its own; a shared
    /// one is what let two writers land in the same bytes. Same directory,
    /// because a rename is only atomic within a filesystem.
    #[test]
    fn each_write_gets_a_temp_path_of_its_own() {
        let dir = std::path::Path::new("/data/ferrosa/commitlog");

        let first = checkpoint_tmp_path(dir);
        let second = checkpoint_tmp_path(dir);

        assert_ne!(first, second, "two writes must not share a temp path");
        assert_eq!(
            first.parent(),
            Some(dir),
            "the temp file must be beside the checkpoint"
        );
        assert_eq!(second.parent(), Some(dir));
        for path in [&first, &second] {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            assert!(name.starts_with(CHECKPOINT_FILENAME), "{name}");
            assert!(
                name.contains(".tmp."),
                "a temp file must be recognisable as one: {name}"
            );
        }
    }

    /// A writer that died mid-save leaves its temp file behind. It must not be
    /// mistaken for the checkpoint, and it must not stop the next save.
    #[test]
    fn a_temp_file_left_by_a_dead_writer_is_not_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = dir
            .path()
            .join(format!("{CHECKPOINT_FILENAME}.tmp.99999.7"));
        fs::write(&orphan, b"{ half a document").unwrap();

        CommitLogCheckpoint::save(dir.path(), &make_positions()).unwrap();

        let loaded = CommitLogCheckpoint::load(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2, "the orphan must not affect what loads");
        assert!(
            orphan.exists(),
            "another writer's temp file is not ours to delete"
        );
    }

    /// What a save leaves behind is one file. Its own temp file is gone,
    /// whatever it was named.
    #[test]
    fn a_save_leaves_no_temp_file_of_its_own() {
        let dir = tempfile::tempdir().unwrap();

        CommitLogCheckpoint::save(dir.path(), &make_positions()).unwrap();

        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files remained: {leftovers:?}");
    }

    #[test]
    fn format_version_check() {
        let dir = tempfile::tempdir().unwrap();

        // Manually write a checkpoint with an unsupported format_version.
        let bad = serde_json::json!({
            "format_version": 99,
            "flushed_positions": {},
            "timestamp": "2026-01-01T00:00:00Z"
        });
        let path = dir.path().join(CHECKPOINT_FILENAME);
        fs::write(&path, serde_json::to_vec(&bad).unwrap()).unwrap();

        let result = CommitLogCheckpoint::load(dir.path());
        assert!(
            result.is_err(),
            "loading a version-99 checkpoint should fail"
        );
        assert!(matches!(
            result.unwrap_err(),
            ferrosa_common::Error::UnsupportedVersion(_)
        ));
    }
}
