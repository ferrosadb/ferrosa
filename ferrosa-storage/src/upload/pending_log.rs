//! Durable ledger of in-progress SSTable uploads.
//!
//! An entry is written (and fsynced) before compaction retires its inputs and
//! removed after S3 confirms the upload and the manifest records it. On
//! restart, the remaining entries identify SSTables that are not yet durably
//! stored in S3 — the startup replay path re-submits their uploads — and the
//! eviction path refuses to delete any SSTable an entry still names.
//!
//! # Layout
//!
//! One file per pending upload under `{data_dir}/pending-uploads/`, named
//! `{table_id}~{sstable_id}.pending` with both parts percent-encoded, holding
//! the record as JSON. Adding an entry writes a uniquely named temp file,
//! fsyncs it, renames it into place and fsyncs the directory; removing one
//! unlinks it and fsyncs the directory. No mutation reads or rewrites another
//! entry, so concurrent writers need no lock and cannot corrupt each other.
//!
//! # Why not one log file
//!
//! Up to 0.29 the ledger was one append-only file whose removals rewrote it in
//! place (`truncate` then write every surviving line). Compaction finalizers
//! and startup replay tasks mutate the ledger concurrently, and that rewrite
//! raced all of them (t_c14245f9):
//!
//! - an add appended between a remove's read and its truncate was erased, so
//!   the eviction guard no longer protected a not-yet-durable SSTable;
//! - two overlapping rewrites, each writing from its own file offset, left a
//!   hole of NUL bytes that became the next record's table name
//!   (`table="\0\0…agent_memory.typed_edges"` on all three dev nodes);
//! - the rewrite kept only two fields, so every OTHER entry lost its
//!   compaction context and would replay with full-ring manifest bounds.
//!
//! # The legacy log
//!
//! [`migrate_legacy_log`] converts a `{data_dir}/pending-uploads.log` once,
//! at engine open, before anything else can touch the ledger. A NUL-prefixed
//! line is salvaged under the name after the hole (the bytes after the hole
//! are a complete line another writer appended) and its raw bytes kept in
//! quarantine; any other line that does not parse is quarantined, never
//! replayed. The migrated log is kept, renamed, as evidence.
//!
//! A binary older than this change reads only the legacy log, so after a
//! downgrade it sees no pending uploads: replay does not re-upload, and
//! eviction relies on the manifest alone (an SSTable is evicted only once the
//! manifest lists it, which happens after its upload is confirmed).

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::manifest::ManifestEntry;

/// Directory, under the data directory, that holds one file per pending upload.
pub const LEDGER_DIR: &str = "pending-uploads";
/// The single-file ledger written up to 0.29; see [`migrate_legacy_log`].
pub const LEGACY_LOG_FILE: &str = "pending-uploads.log";
/// Directory, under [`LEDGER_DIR`], for records that can never be replayed.
pub const QUARANTINE_DIR: &str = "quarantine";
const ENTRY_SUFFIX: &str = ".pending";
/// An entry is a few hundred bytes of JSON; anything larger is not one.
const MAX_ENTRY_BYTES: u64 = 1 << 20;

/// Distinguishes the temp files of concurrent writers of the same entry.
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCompactionUpload {
    #[serde(default)]
    pub intent_task_id: Option<String>,
    pub remove_input_ids: Vec<String>,
    pub output: ManifestEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingUploadRecord {
    pub table_id: String,
    pub sstable_id: String,
    #[serde(default)]
    pub compaction: Option<PendingCompactionUpload>,
}

/// What [`PendingUploadsLog::quarantine`] stores: the record and why it can
/// never be replayed.
#[derive(Debug, Serialize, Deserialize)]
struct QuarantinedRecord {
    record: PendingUploadRecord,
    reason: String,
    quarantined_at_unix_secs: u64,
}

/// The SSTable uploads that have been written locally but not yet confirmed
/// by S3. See the module documentation for the layout.
pub struct PendingUploadsLog {
    dir: PathBuf,
}

impl PendingUploadsLog {
    /// Open (or create) the ledger of the data directory `data_dir`.
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join(LEDGER_DIR);
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// Where records that can never be replayed are kept.
    pub fn quarantine_dir(&self) -> PathBuf {
        self.dir.join(QUARANTINE_DIR)
    }

    /// Record a pending upload with no compaction context, durably.
    pub fn add_entry(&self, table_id: &str, sstable_id: &str) -> std::io::Result<()> {
        let start = Instant::now();
        self.write_entry(&PendingUploadRecord {
            table_id: table_id.to_string(),
            sstable_id: sstable_id.to_string(),
            compaction: None,
        })?;
        crate::metrics::observe_upload_phase(
            crate::metrics::UploadPhase::PendingLogAdd,
            start.elapsed(),
        );
        Ok(())
    }

    /// Record a pending upload of a compaction output, durably.
    ///
    /// The manifest context lets crash recovery finish the same
    /// upload-confirm → manifest-update → ledger-remove sequence that
    /// `poll_compactions` would have completed before the crash.
    pub fn add_compaction_entry(
        &self,
        table_id: &str,
        sstable_id: &str,
        compaction: PendingCompactionUpload,
    ) -> std::io::Result<()> {
        let start = Instant::now();
        self.write_entry(&PendingUploadRecord {
            table_id: table_id.to_string(),
            sstable_id: sstable_id.to_string(),
            compaction: Some(compaction),
        })?;
        crate::metrics::observe_upload_phase(
            crate::metrics::UploadPhase::PendingLogCompactionAdd,
            start.elapsed(),
        );
        Ok(())
    }

    /// Remove the entry for `(table_id, sstable_id)`, durably. Removing an
    /// entry that is not there succeeds: removal is idempotent.
    pub fn remove_entry(&self, table_id: &str, sstable_id: &str) -> std::io::Result<()> {
        let start = Instant::now();
        match std::fs::remove_file(self.entry_path(table_id, sstable_id)) {
            Ok(()) => sync_dir(&self.dir)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        crate::metrics::observe_upload_phase(
            crate::metrics::UploadPhase::PendingLogRemove,
            start.elapsed(),
        );
        Ok(())
    }

    /// Move `record` out of the live ledger into quarantine, with `reason`.
    ///
    /// For a record that can never be replayed: it stops being retried on
    /// every startup and stops protecting anything from eviction, and the
    /// quarantine keeps it, and why, for an operator.
    pub fn quarantine(&self, record: &PendingUploadRecord, reason: &str) -> std::io::Result<()> {
        let quarantined = QuarantinedRecord {
            record: record.clone(),
            reason: reason.to_string(),
            quarantined_at_unix_secs: unix_secs(),
        };
        let bytes = serde_json::to_vec_pretty(&quarantined).map_err(std::io::Error::other)?;
        let name = format!(
            "{}.{}",
            entry_file_name(&record.table_id, &record.sstable_id),
            quarantined.quarantined_at_unix_secs
        );
        write_durably(&self.quarantine_dir(), &name, &bytes)?;
        self.remove_entry(&record.table_id, &record.sstable_id)
    }

    /// Return all pending `(table_id, sstable_id)` pairs.
    pub fn pending_entries(&self) -> std::io::Result<Vec<(String, String)>> {
        Ok(self
            .pending_records()?
            .into_iter()
            .map(|record| (record.table_id, record.sstable_id))
            .collect())
    }

    /// Return every pending upload record, ordered by table then SSTable.
    ///
    /// Entries are published by rename, so a crash cannot leave a torn one.
    /// An entry that still does not parse, or whose content names a different
    /// SSTable than its file name, is corrupt: it is logged at ERROR, moved to
    /// quarantine and left out — never replayed as garbage.
    pub fn pending_records(&self) -> std::io::Result<Vec<PendingUploadRecord>> {
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                self.quarantine_corrupt_file(&entry.path(), "file name is not UTF-8");
                continue;
            };
            if name.starts_with('.') || !name.ends_with(ENTRY_SUFFIX) {
                continue;
            }
            match read_entry(&entry.path(), name) {
                Ok(Some(record)) => records.push(record),
                // Removed by a concurrent writer between the listing and the read.
                Ok(None) => {}
                Err(EntryError::Io(e)) => return Err(e),
                Err(EntryError::Corrupt(reason)) => {
                    self.quarantine_corrupt_file(&entry.path(), &reason)
                }
            }
        }
        records.sort_by(|a, b| {
            (a.table_id.as_str(), a.sstable_id.as_str())
                .cmp(&(b.table_id.as_str(), b.sstable_id.as_str()))
        });
        Ok(records)
    }

    fn entry_path(&self, table_id: &str, sstable_id: &str) -> PathBuf {
        self.dir.join(entry_file_name(table_id, sstable_id))
    }

    fn write_entry(&self, record: &PendingUploadRecord) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(record).map_err(std::io::Error::other)?;
        write_durably(
            &self.dir,
            &entry_file_name(&record.table_id, &record.sstable_id),
            &bytes,
        )
    }

    /// Move a corrupt entry file into quarantine, loudly. Failing to move it
    /// is logged too: the entry is still left out of the records, and the next
    /// read reports it again.
    fn quarantine_corrupt_file(&self, path: &Path, reason: &str) {
        crate::metrics::inc_pending_upload_ledger_corrupt_entries();
        let target = self.quarantine_dir().join(format!(
            "{}.corrupt.{}",
            path.file_name().map_or_else(
                || "unnamed".into(),
                |name| name.to_string_lossy().into_owned()
            ),
            unix_secs()
        ));
        let moved = std::fs::create_dir_all(self.quarantine_dir())
            .and_then(|()| std::fs::rename(path, &target))
            .and_then(|()| sync_dir(&self.dir));
        match moved {
            Ok(()) => tracing::error!(
                path = %path.display(),
                quarantined_to = %target.display(),
                reason,
                "pending upload ledger: corrupt entry quarantined; it will not be replayed"
            ),
            // A concurrent reader quarantined it first.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::error!(
                %e,
                path = %path.display(),
                reason,
                "pending upload ledger: corrupt entry left out of replay but could not be quarantined"
            ),
        }
    }
}

enum EntryError {
    Io(std::io::Error),
    Corrupt(String),
}

/// Read and validate one entry file. `Ok(None)` when it no longer exists.
fn read_entry(path: &Path, name: &str) -> Result<Option<PendingUploadRecord>, EntryError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(EntryError::Io(e)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_ENTRY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(EntryError::Io)?;
    if bytes.len() as u64 > MAX_ENTRY_BYTES {
        return Err(EntryError::Corrupt(format!(
            "entry is larger than {MAX_ENTRY_BYTES} bytes"
        )));
    }
    let record: PendingUploadRecord = serde_json::from_slice(&bytes)
        .map_err(|e| EntryError::Corrupt(format!("entry does not parse: {e}")))?;
    let expected = entry_file_name(&record.table_id, &record.sstable_id);
    if expected != name {
        return Err(EntryError::Corrupt(format!(
            "entry names {}/{} but is filed as {name}",
            record.table_id, record.sstable_id
        )));
    }
    Ok(Some(record))
}

/// `{table}~{sstable}.pending`, each part percent-encoded so it is one path
/// component and `~` separates them unambiguously.
fn entry_file_name(table_id: &str, sstable_id: &str) -> String {
    format!(
        "{}~{}{ENTRY_SUFFIX}",
        encode_component(table_id),
        encode_component(sstable_id)
    )
}

fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Write `bytes` to `dir/name` so that a crash leaves either the old file or
/// the complete new one: unique temp file, fsync, rename, fsync the directory.
fn write_durably(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, dir.join(name))
    })();
    if let Err(e) = written {
        if let Err(cleanup) = std::fs::remove_file(&tmp) {
            if cleanup.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(%cleanup, path = %tmp.display(), "pending upload ledger: could not remove a failed write's temp file");
            }
        }
        return Err(e);
    }
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// What [`migrate_legacy_log`] did with a legacy ledger.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LegacyMigration {
    /// Records carried into the ledger directory as they were.
    pub migrated: usize,
    /// NUL-prefixed records carried over under the name after the hole.
    pub salvaged: usize,
    /// Lines that could not be parsed and were quarantined, not carried over.
    pub quarantined: usize,
}

/// Convert `{data_dir}/pending-uploads.log` into the ledger directory, once.
///
/// Must run before anything else in the process mutates the ledger — the
/// engine calls it from its constructors, before it exists to be used.
/// Re-running after a crash part-way is safe: an entry is written by rename,
/// so writing it twice leaves one copy, and the log is renamed away last.
pub fn migrate_legacy_log(data_dir: &Path) -> std::io::Result<LegacyMigration> {
    let legacy = data_dir.join(LEGACY_LOG_FILE);
    let bytes = match File::open(&legacy) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(MAX_LEGACY_LOG_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_LEGACY_LOG_BYTES {
                return Err(std::io::Error::other(format!(
                    "legacy pending upload log {} is larger than {MAX_LEGACY_LOG_BYTES} bytes; \
                     refusing to migrate a file that is not a ledger",
                    legacy.display()
                )));
            }
            bytes
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(LegacyMigration::default()),
        Err(e) => return Err(e),
    };
    let ledger = PendingUploadsLog::open(data_dir)?;
    let mut outcome = LegacyMigration::default();
    let mut quarantined_lines: Vec<u8> = Vec::new();
    for raw in bytes.split(|&b| b == b'\n') {
        if raw.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match parse_legacy_line(raw) {
            LegacyLine::Record(record) => {
                ledger.write_entry(&record)?;
                outcome.migrated += 1;
            }
            LegacyLine::Salvaged { record, why } => {
                tracing::error!(
                    table = record.table_id,
                    sstable = record.sstable_id,
                    why,
                    "pending upload ledger: damaged legacy record kept pending so eviction still \
                     protects its SSTable; raw line quarantined (t_c14245f9)"
                );
                crate::metrics::inc_pending_upload_ledger_corrupt_entries();
                ledger.write_entry(&record)?;
                quarantined_lines.extend_from_slice(raw);
                quarantined_lines.push(b'\n');
                outcome.salvaged += 1;
            }
            LegacyLine::Corrupt(reason) => {
                tracing::error!(
                    reason,
                    line = %String::from_utf8_lossy(raw).escape_debug(),
                    "pending upload ledger: corrupt legacy line quarantined; it will not be replayed"
                );
                crate::metrics::inc_pending_upload_ledger_corrupt_entries();
                quarantined_lines.extend_from_slice(raw);
                quarantined_lines.push(b'\n');
                outcome.quarantined += 1;
            }
        }
    }
    let stamp = unix_secs();
    if !quarantined_lines.is_empty() {
        write_durably(
            &ledger.quarantine_dir(),
            &format!("{LEGACY_LOG_FILE}.{stamp}.lines"),
            &quarantined_lines,
        )?;
    }
    std::fs::rename(
        &legacy,
        data_dir.join(format!("{LEGACY_LOG_FILE}.migrated-{stamp}")),
    )?;
    sync_dir(data_dir)?;
    tracing::info!(
        migrated = outcome.migrated,
        salvaged = outcome.salvaged,
        quarantined = outcome.quarantined,
        "pending upload ledger: migrated the legacy single-file log to one entry per upload"
    );
    Ok(outcome)
}

/// A legacy log is one short line per pending upload; the largest live one
/// was 5 KB. Anything near this is not a ledger.
const MAX_LEGACY_LOG_BYTES: u64 = 64 << 20;

enum LegacyLine {
    Record(PendingUploadRecord),
    /// Damaged, but the record it carries is intact enough to keep pending.
    Salvaged {
        record: PendingUploadRecord,
        why: String,
    },
    Corrupt(String),
}

/// Parse one legacy line: `{table}\t{sstable}[\t{compaction json}]`, each
/// field percent-encoded by the old writer.
fn parse_legacy_line(raw: &[u8]) -> LegacyLine {
    let nul_bytes = raw.iter().take_while(|&&b| b == 0).count();
    let body = &raw[nul_bytes..];
    if body.contains(&0) {
        return LegacyLine::Corrupt("NUL bytes inside the record".to_string());
    }
    let Ok(line) = std::str::from_utf8(body) else {
        return LegacyLine::Corrupt("not UTF-8".to_string());
    };
    let mut parts = line.trim().splitn(3, '\t');
    let (Some(table), Some(sstable)) = (parts.next(), parts.next()) else {
        return LegacyLine::Corrupt("fewer than two tab-separated fields".to_string());
    };
    let (table_id, sstable_id) = (url_decode(table), url_decode(sstable));
    if table_id.is_empty() || sstable_id.is_empty() {
        return LegacyLine::Corrupt("empty table or SSTable id".to_string());
    }
    let mut damage = Vec::new();
    if nul_bytes > 0 {
        damage.push(format!(
            "{nul_bytes} NUL bytes before the record (a hole left by overlapping in-place rewrites)"
        ));
    }
    // An unreadable compaction context still names an SSTable whose upload is
    // unconfirmed: keep it pending without the context rather than drop it.
    let compaction = match parts.next().map(url_decode) {
        None => None,
        Some(payload) => match serde_json::from_str(&payload) {
            Ok(compaction) => Some(compaction),
            Err(e) => {
                damage.push(format!(
                    "compaction context does not parse ({e}); kept without it"
                ));
                None
            }
        },
    };
    let record = PendingUploadRecord {
        table_id,
        sstable_id,
        compaction,
    };
    if damage.is_empty() {
        LegacyLine::Record(record)
    } else {
        LegacyLine::Salvaged {
            record,
            why: damage.join("; "),
        }
    }
}

/// Decode the small subset of percent-encoding the legacy writer produced.
fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 3 <= bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn pending_uploads_log_add_remove_roundtrip() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();

        // Start empty.
        assert!(log.pending_entries().unwrap().is_empty());

        // Add an entry and verify it is present.
        log.add_entry("ks.t", "abc123").unwrap();
        let entries = log.pending_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], ("ks.t".to_string(), "abc123".to_string()));

        // Remove it and verify the log is empty again.
        log.remove_entry("ks.t", "abc123").unwrap();
        assert!(log.pending_entries().unwrap().is_empty());
    }

    #[test]
    fn pending_uploads_log_multiple_entries() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();

        log.add_entry("ks.t", "sst1").unwrap();
        log.add_entry("ks.t", "sst2").unwrap();
        log.add_entry("ks.t", "sst3").unwrap();

        // Remove the middle entry; the other two should survive.
        log.remove_entry("ks.t", "sst2").unwrap();

        let entries = log.pending_entries().unwrap();
        assert_eq!(entries.len(), 2);
        let ids: Vec<&str> = entries.iter().map(|(_, sid)| sid.as_str()).collect();
        assert!(ids.contains(&"sst1"));
        assert!(ids.contains(&"sst3"));
        assert!(!ids.contains(&"sst2"));
    }

    #[test]
    fn pending_uploads_log_remove_entry_requires_table_id() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();

        log.add_entry("ks.table_a", "1").unwrap();
        log.add_entry("ks.table_b", "1").unwrap();

        log.remove_entry("ks.table_a", "1").unwrap();

        assert_eq!(
            log.pending_entries().unwrap(),
            vec![("ks.table_b".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn pending_uploads_log_roundtrips_compaction_context() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();

        let compaction = PendingCompactionUpload {
            intent_task_id: Some("task-3".to_string()),
            remove_input_ids: vec!["1".to_string(), "2".to_string()],
            output: ManifestEntry {
                id: "3".to_string(),
                size: 42,
                min_token: -10,
                max_token: 10,
                min_timestamp: 100,
                max_timestamp: 200,
            },
        };
        log.add_compaction_entry("ks.table", "3", compaction.clone())
            .unwrap();

        let records = log.pending_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].table_id, "ks.table");
        assert_eq!(records[0].sstable_id, "3");
        assert_eq!(records[0].compaction.as_ref(), Some(&compaction));

        log.remove_entry("ks.table", "3").unwrap();
        assert!(log.pending_records().unwrap().is_empty());
    }

    #[test]
    fn pending_uploads_log_persists_across_open() {
        let dir = tempdir().unwrap();

        // First handle: write an entry.
        {
            let log = PendingUploadsLog::open(dir.path()).unwrap();
            log.add_entry("ks.t", "crash-sst").unwrap();
        }

        // Second handle (simulating restart): entry must still be there.
        {
            let log = PendingUploadsLog::open(dir.path()).unwrap();
            let entries = log.pending_entries().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].1, "crash-sst");
        }
    }

    fn compaction_context(output: &str) -> PendingCompactionUpload {
        PendingCompactionUpload {
            intent_task_id: Some(format!("task-{output}")),
            remove_input_ids: vec![format!("{output}-in")],
            output: ManifestEntry {
                id: output.to_string(),
                size: 7,
                min_token: -1,
                max_token: 1,
                min_timestamp: 1,
                max_timestamp: 2,
            },
        }
    }

    /// Removing one entry must not touch another entry's recovery context: a
    /// compaction entry stripped to two fields replays as "no compaction
    /// context" and lands in the manifest with full-ring bounds and no input
    /// cleanup (t_c14245f9 — every live ledger had lost every payload).
    #[test]
    fn remove_entry_keeps_the_compaction_context_of_other_entries() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        log.add_compaction_entry("ks.t", "10", compaction_context("10"))
            .unwrap();
        log.add_compaction_entry("ks.t", "11", compaction_context("11"))
            .unwrap();

        log.remove_entry("ks.t", "10").unwrap();

        let records = log.pending_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sstable_id, "11");
        assert_eq!(
            records[0].compaction.as_ref(),
            Some(&compaction_context("11")),
            "the surviving entry lost its compaction context"
        );
    }

    /// Ledger mutations come from concurrent compaction finalizers and replay
    /// tasks. A remove that rewrote the file in place raced every other writer:
    /// an add landing between its read and its truncate vanished (the #235
    /// eviction guard then no longer protected that SSTable), and two
    /// overlapping rewrites left a hole of NUL bytes that became the next
    /// record's table name (`table="\0\0…agent_memory.typed_edges"` on all
    /// three dev nodes, t_c14245f9).
    #[test]
    fn concurrent_adds_and_removes_never_lose_an_entry_or_write_nul_bytes() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 40;
        let dir = tempdir().unwrap();
        let log = std::sync::Arc::new(PendingUploadsLog::open(dir.path()).unwrap());

        let workers: Vec<_> = (0..THREADS)
            .map(|thread| {
                let log = std::sync::Arc::clone(&log);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let table = format!("ks.table_{thread}");
                        let removed = format!("{i}-removed");
                        log.add_compaction_entry(&table, &removed, compaction_context(&removed))
                            .unwrap();
                        log.add_entry(&table, &format!("{i}-kept")).unwrap();
                        log.remove_entry(&table, &removed).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }

        let mut kept: Vec<(String, String)> = log.pending_entries().unwrap();
        kept.sort();
        let mut expected: Vec<(String, String)> = (0..THREADS)
            .flat_map(|thread| {
                (0..PER_THREAD).map(move |i| (format!("ks.table_{thread}"), format!("{i}-kept")))
            })
            .collect();
        expected.sort();
        assert_eq!(
            kept, expected,
            "a concurrent ledger mutation lost or resurrected an entry"
        );
        for (table, _) in &kept {
            assert!(
                !table.contains('\0'),
                "NUL bytes reached a table name: {table:?}"
            );
        }
    }

    /// A ledger entry that does not parse is reported and moved to
    /// quarantine, never returned for replay — and never fatal to the read.
    #[test]
    fn a_corrupt_entry_file_is_quarantined_not_replayed() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        log.add_entry("ks.t", "1").unwrap();
        let corrupt = dir
            .path()
            .join(LEDGER_DIR)
            .join(entry_file_name("ks.t", "2"));
        std::fs::write(&corrupt, b"\0\0\0 not json").unwrap();
        let misfiled = dir
            .path()
            .join(LEDGER_DIR)
            .join(entry_file_name("ks.t", "3"));
        std::fs::write(
            &misfiled,
            serde_json::to_vec(&PendingUploadRecord {
                table_id: "ks.other".to_string(),
                sstable_id: "9".to_string(),
                compaction: None,
            })
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            log.pending_entries().unwrap(),
            vec![("ks.t".to_string(), "1".to_string())]
        );
        assert!(
            !corrupt.exists() && !misfiled.exists(),
            "corrupt entries left live"
        );
        assert_eq!(std::fs::read_dir(log.quarantine_dir()).unwrap().count(), 2);
    }

    /// `quarantine` takes a record out of the live ledger and keeps it, with
    /// its reason, where an operator can find it.
    #[test]
    fn quarantine_moves_a_record_out_of_the_live_ledger_with_its_reason() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        log.add_compaction_entry("ks.t", "5", compaction_context("5"))
            .unwrap();
        let record = log.pending_records().unwrap().remove(0);

        log.quarantine(&record, "files are gone").unwrap();

        assert!(log.pending_records().unwrap().is_empty());
        let kept: Vec<_> = std::fs::read_dir(log.quarantine_dir())
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect();
        assert_eq!(kept.len(), 1);
        let kept: QuarantinedRecord = serde_json::from_str(&kept[0]).unwrap();
        assert_eq!(kept.record, record);
        assert_eq!(kept.reason, "files are gone");
    }

    /// The live dev ledgers, reproduced: plain records, a compaction record, a
    /// NUL-prefixed record, and garbage. Migration carries the good ones over
    /// with their context, salvages the NUL-prefixed one under its real name,
    /// quarantines the garbage, and keeps the old log renamed as evidence.
    #[test]
    fn legacy_log_migration_salvages_nul_holes_and_quarantines_garbage() {
        let dir = tempdir().unwrap();
        let payload = serde_json::to_string(&compaction_context("12")).unwrap();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(b"ks.t\t10\n");
        legacy.extend_from_slice(format!("ks.t\t12\t{payload}\n").as_bytes());
        legacy.extend_from_slice(&[0u8; 98]);
        legacy.extend_from_slice(b"agent_memory.entity_store\t1779774830521969\n");
        legacy.extend_from_slice(b"no-tab-here\n");
        legacy.extend_from_slice(b"ks.t\t\0inner\n\n");
        std::fs::write(dir.path().join(LEGACY_LOG_FILE), &legacy).unwrap();

        let outcome = migrate_legacy_log(dir.path()).unwrap();

        assert_eq!(
            outcome,
            LegacyMigration {
                migrated: 2,
                salvaged: 1,
                quarantined: 2,
            }
        );
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        let records = log.pending_records().unwrap();
        let keys: Vec<_> = records
            .iter()
            .map(|r| (r.table_id.as_str(), r.sstable_id.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("agent_memory.entity_store", "1779774830521969"),
                ("ks.t", "10"),
                ("ks.t", "12"),
            ]
        );
        assert_eq!(records[2].compaction, Some(compaction_context("12")));
        assert!(!dir.path().join(LEGACY_LOG_FILE).exists());
        let evidence: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("pending-uploads.log.migrated-"))
            .collect();
        assert_eq!(evidence.len(), 1, "the migrated log is kept as evidence");
        let quarantined: Vec<u8> = std::fs::read_dir(log.quarantine_dir())
            .unwrap()
            .map(|e| std::fs::read(e.unwrap().path()).unwrap())
            .next()
            .unwrap();
        assert_eq!(
            quarantined
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .count(),
            3,
            "the NUL-prefixed line and both garbage lines are kept raw"
        );

        assert_eq!(
            migrate_legacy_log(dir.path()).unwrap(),
            LegacyMigration::default(),
            "a migrated data directory has nothing left to migrate"
        );
    }
}
