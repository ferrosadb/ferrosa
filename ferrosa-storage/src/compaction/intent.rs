//! Durable replacement record for a single compaction, a.k.a. the "intent".
//!
//! `compaction-cancel-safety.md` C2/C3 (forge `t_fca66994`): after #449 started
//! purging tombstones once `gc_grace_seconds` elapses, nothing recorded which
//! output SSTable replaces which input SSTables. `evict_local_input_sstable_files`
//! retires inputs one at a time with no atomicity across the set, so a crash
//! between two input retirements can delete the input that held a purged
//! tombstone while the input holding the row it shadowed survives -- that row
//! is resurrected at restart.
//!
//! This module is the durable record, written and fsynced *before* promotion
//! (the commit point), advanced through `Promoting -> Swapped -> Retired` as
//! `StorageEngine::poll_compactions` finishes each step, and deleted once the
//! generation it describes needs no further recovery. `StorageEngine` (engine.rs)
//! owns the commit protocol and startup reconciliation that use it; this module
//! is deliberately pure I/O + (de)serialization with no logging, so every
//! caller decides for itself how loudly to report what it finds.
//!
//! # Format
//!
//! One JSON file per in-flight compaction at
//! `sstables/<table>/.compaction-<output_gen>.intent`, where `<output_gen>` is
//! the compaction's pre-promotion staged output id (unique per task, already
//! allocated by the compaction executor before the merge runs -- reused here
//! rather than inventing a second task-id scheme).
//!
//! # Relationship to `upload::PendingUploadsLog`
//!
//! The two records answer different questions and neither replaces the other.
//! `PendingUploadsLog` (a single per-node append-only file) tracks whether a
//! *local* SSTable has been durably uploaded to S3 and the manifest updated --
//! it is written only after promote/swap/retire already happened, and only
//! when S3 is configured. This record is the source of truth for whether
//! promote/swap/retire *itself* completed safely; it exists and is enforced
//! regardless of whether S3 is configured at all. `poll_compactions` writes
//! both, in order: this record first (commit), then, on the S3 path, the
//! pending-upload log entry (Step 1 of the S3 dance). They are not redundant:
//! deleting this record does not touch the pending-upload log, and vice versa.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const INTENT_PREFIX: &str = ".compaction-";
const INTENT_SUFFIX: &str = ".intent";

/// Where a compaction's commit protocol currently stands. Recorded phases
/// only ever move forward (`Promoting` -> `Swapped` -> `Retired`); a phase
/// going backward would mean two writers raced on the same intent file,
/// which never happens because only the task that created it advances it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionIntentPhase {
    /// The record is written and fsynced; promotion has not yet run, or ran
    /// but the process died before the phase was advanced. Roll back if the
    /// output is missing; roll forward if it is present and its digest
    /// matches.
    Promoting,
    /// The output is promoted and swapped into the live view. Retirement of
    /// the inputs has not yet been confirmed complete.
    Swapped,
    /// Every input listed in this record has been retired (or was already
    /// gone). The local half of the invariant already holds; only S3
    /// convergence (driven by the separate pending-upload log) may still be
    /// outstanding.
    Retired,
}

/// The durable replacement record for one compaction task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionIntentRecord {
    /// The compaction's pre-promotion staged output id. Doubles as this
    /// record's identity (see the filename it is written under).
    pub task_id: String,
    /// The output SSTable's generation id *after* promotion. Recorded here
    /// (rather than re-derived) because `promote_compaction_output` is free
    /// to pick a different generation number than the staged id, to avoid
    /// colliding with a flush that ran concurrently.
    pub output_gen: String,
    /// The output's `Digest.crc32` value, read from the staged output before
    /// promotion. Startup reconciliation recomputes this from the promoted
    /// generation's `Digest.crc32` and compares.
    pub output_digest: u32,
    /// Generation ids of every input SSTable this compaction claims to
    /// supersede.
    pub inputs: Vec<String>,
    pub phase: CompactionIntentPhase,
}

impl CompactionIntentRecord {
    /// The path a record for `task_id` lives at under `table_dir`.
    pub fn path(table_dir: &Path, task_id: &str) -> PathBuf {
        table_dir.join(format!("{INTENT_PREFIX}{task_id}{INTENT_SUFFIX}"))
    }

    fn tmp_path(table_dir: &Path, task_id: &str) -> PathBuf {
        table_dir.join(format!("{INTENT_PREFIX}{task_id}{INTENT_SUFFIX}.tmp"))
    }

    /// Whether `file_name` (no directory component) names an intent record,
    /// as opposed to its own `.tmp` staging name or unrelated debris.
    pub fn is_record_name(file_name: &str) -> bool {
        file_name.starts_with(INTENT_PREFIX) && file_name.ends_with(INTENT_SUFFIX)
    }

    /// Write this record to `table_dir`: serialize to a `.tmp` sibling,
    /// fsync the file, rename into place, then fsync `table_dir` so the
    /// rename's directory entry is durable. The caller may treat the record
    /// as committed only once this returns `Ok`.
    pub fn write(&self, table_dir: &Path) -> std::io::Result<()> {
        let tmp = Self::tmp_path(table_dir, &self.task_id);
        let json = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&json)?;
            file.flush()?;
            file.sync_all()?;
        }
        let path = Self::path(table_dir, &self.task_id);
        std::fs::rename(&tmp, &path)?;
        #[cfg(test)]
        crate::flush::fsync_probe::note_rename(&path);
        // `fsync_dir` records its own dir-fsync event under `#[cfg(test)]`.
        crate::flush::FileFlushTarget::fsync_dir(table_dir)?;
        Ok(())
    }

    /// Delete this record for `task_id` under `table_dir` and fsync the
    /// directory. A record that is already gone is not an error -- deletion
    /// is idempotent, since a crash between the unlink and its directory
    /// fsync can otherwise replay this step.
    pub fn delete(table_dir: &Path, task_id: &str) -> std::io::Result<()> {
        let path = Self::path(table_dir, task_id);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                crate::flush::FileFlushTarget::fsync_dir(table_dir)?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Read and parse the record at `path`. `Ok(None)` only for a missing
    /// file; a present-but-unparseable file is `Err` so the caller decides
    /// how to report a corrupt record rather than silently skipping it.
    pub fn read_at(path: &Path) -> std::io::Result<Option<Self>> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(std::io::Error::other),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(phase: CompactionIntentPhase) -> CompactionIntentRecord {
        CompactionIntentRecord {
            task_id: "42".to_string(),
            output_gen: "43".to_string(),
            output_digest: 0xDEAD_BEEF,
            inputs: vec!["10".to_string(), "11".to_string()],
            phase,
        }
    }

    #[test]
    fn write_read_roundtrip_preserves_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let rec = record(CompactionIntentPhase::Promoting);
        rec.write(dir.path()).unwrap();

        let path = CompactionIntentRecord::path(dir.path(), &rec.task_id);
        let read_back = CompactionIntentRecord::read_at(&path).unwrap().unwrap();
        assert_eq!(read_back, rec);
    }

    #[test]
    fn write_leaves_no_tmp_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        record(CompactionIntentPhase::Promoting)
            .write(dir.path())
            .unwrap();
        assert!(!CompactionIntentRecord::tmp_path(dir.path(), "42").exists());
    }

    #[test]
    fn rewriting_advances_the_phase_in_place() {
        let dir = tempfile::tempdir().unwrap();
        record(CompactionIntentPhase::Promoting)
            .write(dir.path())
            .unwrap();
        record(CompactionIntentPhase::Swapped)
            .write(dir.path())
            .unwrap();
        record(CompactionIntentPhase::Retired)
            .write(dir.path())
            .unwrap();

        let path = CompactionIntentRecord::path(dir.path(), "42");
        let read_back = CompactionIntentRecord::read_at(&path).unwrap().unwrap();
        assert_eq!(read_back.phase, CompactionIntentPhase::Retired);
    }

    #[test]
    fn read_at_missing_file_is_ok_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = CompactionIntentRecord::path(dir.path(), "nope");
        assert_eq!(CompactionIntentRecord::read_at(&path).unwrap(), None);
    }

    #[test]
    fn read_at_corrupt_file_is_err_not_silently_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = CompactionIntentRecord::path(dir.path(), "42");
        std::fs::write(&path, b"not json").unwrap();
        assert!(CompactionIntentRecord::read_at(&path).is_err());
    }

    #[test]
    fn delete_is_idempotent_on_an_already_missing_record() {
        let dir = tempfile::tempdir().unwrap();
        CompactionIntentRecord::delete(dir.path(), "never-existed").unwrap();
    }

    #[test]
    fn delete_removes_the_record_file() {
        let dir = tempfile::tempdir().unwrap();
        let rec = record(CompactionIntentPhase::Retired);
        rec.write(dir.path()).unwrap();
        let path = CompactionIntentRecord::path(dir.path(), &rec.task_id);
        assert!(path.exists());
        CompactionIntentRecord::delete(dir.path(), &rec.task_id).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn is_record_name_matches_only_the_intent_suffix() {
        assert!(CompactionIntentRecord::is_record_name(
            ".compaction-42.intent"
        ));
        assert!(!CompactionIntentRecord::is_record_name(
            ".compaction-42.intent.tmp"
        ));
        assert!(!CompactionIntentRecord::is_record_name("42-Data.db"));
        assert!(!CompactionIntentRecord::is_record_name(".promote-42"));
    }
}
