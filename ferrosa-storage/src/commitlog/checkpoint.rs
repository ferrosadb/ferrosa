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
use std::io::ErrorKind;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::config::{CommitLogPosition, TableId};

const CHECKPOINT_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_FILENAME: &str = "commitlog_checkpoint.json";
const CHECKPOINT_TMP_FILENAME: &str = "commitlog_checkpoint.json.tmp";

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
        let tmp_path = dir.join(CHECKPOINT_TMP_FILENAME);
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

        fs::write(&tmp_path, &json)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

        // The temp file must NOT linger after a successful save.
        let tmp_path = dir.path().join(CHECKPOINT_TMP_FILENAME);
        assert!(
            !tmp_path.exists(),
            "temporary file should not remain after save"
        );
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
