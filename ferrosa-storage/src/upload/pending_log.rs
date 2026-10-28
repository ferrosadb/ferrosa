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
//! ```text
//! {data_dir}/pending-uploads/
//!     {enc(table_id)}/{enc(sstable_id)}.pending     one JSON record per upload
//!     quarantine/{YYYY-MM-DD}/…                     records that can never replay
//!     quarantine/archive/{YYYY-MM-DD}.jsonl         days past the retention bound
//! ```
//!
//! Both id parts are percent-encoded so each is one path component. Adding an
//! entry makes sure its table directory exists — creating it fsyncs the table
//! directory, and every add then fsyncs the ledger directory, because a writer
//! that finds the table directory already there cannot tell whether its
//! creator's fsync has finished — then writes a uniquely named temp file,
//! fsyncs it, renames it into place and fsyncs the table directory. Removing
//! one unlinks it and fsyncs the table directory. No mutation reads or
//! rewrites another entry, so concurrent writers need no lock and cannot
//! corrupt each other.
//!
//! **An emptied table directory stays.** Removing it would race a concurrent
//! add: the add can find the directory, then lose it to the `rmdir` before its
//! temp file is created. The number of table directories is bounded by the
//! number of tables, and [`migrate_ledger`] removes empty ones at engine open,
//! when nothing else can be writing.
//!
//! **The eviction guard costs one `stat` per candidate**
//! ([`PendingUploadsLog::contains`]): it never lists, opens or parses the
//! ledger, so a ledger grown by an S3 outage does not slow the cycle that has
//! to free disk. Only startup replay walks the tree.
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
//! # Migration
//!
//! [`migrate_ledger`] runs once at engine open, before anything else can touch
//! the ledger. It converts a legacy `{data_dir}/pending-uploads.log` straight
//! into the tree: a NUL-prefixed line is salvaged under the name after the
//! hole (the bytes after the hole are a complete line another writer
//! appended) and its raw bytes kept in quarantine; any other line that does
//! not parse is quarantined, never replayed; the old log is kept, renamed, as
//! evidence. It also converts the flat `{table}~{sstable}.pending` layout an
//! unmerged revision of this change wrote, and is idempotent.
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

/// Directory, under the data directory, that holds the ledger tree.
pub const LEDGER_DIR: &str = "pending-uploads";
/// The single-file ledger written up to 0.29; see [`migrate_ledger`].
pub const LEGACY_LOG_FILE: &str = "pending-uploads.log";
/// Directory, under [`LEDGER_DIR`], for records that can never be replayed.
pub const QUARANTINE_DIR: &str = "quarantine";
/// Directory, under [`QUARANTINE_DIR`], for days past the retention bound.
pub const QUARANTINE_ARCHIVE_DIR: &str = "archive";
/// Quarantined entries kept unarchived; see [`QuarantineRetention`].
pub const QUARANTINE_MAX_ENTRIES_ENV: &str = "FERROSA_PENDING_UPLOAD_QUARANTINE_MAX_ENTRIES";
/// Age in days past which a quarantine day is archived.
pub const QUARANTINE_MAX_AGE_DAYS_ENV: &str = "FERROSA_PENDING_UPLOAD_QUARANTINE_MAX_AGE_DAYS";
const DEFAULT_QUARANTINE_MAX_ENTRIES: usize = 1_000;
const DEFAULT_QUARANTINE_MAX_AGE_DAYS: u64 = 30;
const ENTRY_SUFFIX: &str = ".pending";
/// An entry is a few hundred bytes of JSON; anything larger is not one.
const MAX_ENTRY_BYTES: u64 = 1 << 20;
/// A legacy log is one short line per pending upload; the largest live one
/// was 5 KB. Anything near this is not a ledger.
const MAX_LEGACY_LOG_BYTES: u64 = 64 << 20;
const SECS_PER_DAY: u64 = 86_400;

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

/// How much quarantine is kept as individual files before whole days are
/// archived. Defaults: 1,000 entries and 30 days; override with
/// `FERROSA_PENDING_UPLOAD_QUARANTINE_MAX_ENTRIES` and
/// `FERROSA_PENDING_UPLOAD_QUARANTINE_MAX_AGE_DAYS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRetention {
    /// Entries kept across all unarchived days; the oldest days go first.
    pub max_entries: usize,
    /// Days older than this are archived regardless of count.
    pub max_age_days: u64,
}

impl Default for QuarantineRetention {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_QUARANTINE_MAX_ENTRIES,
            max_age_days: DEFAULT_QUARANTINE_MAX_AGE_DAYS,
        }
    }
}

impl QuarantineRetention {
    /// The bounds from the environment. A value that is not a positive
    /// integer is reported at WARN and the default applies.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            max_entries: env_bound(QUARANTINE_MAX_ENTRIES_ENV, defaults.max_entries),
            max_age_days: env_bound(QUARANTINE_MAX_AGE_DAYS_ENV, defaults.max_age_days),
        }
    }
}

fn env_bound<T: std::str::FromStr + PartialOrd + Default + Copy + std::fmt::Display>(
    name: &str,
    default: T,
) -> T {
    match std::env::var(name) {
        Err(_) => default,
        Ok(raw) => match raw.trim().parse::<T>() {
            Ok(value) if value > T::default() => value,
            _ => {
                tracing::warn!(
                    variable = name,
                    value = raw,
                    %default,
                    "pending upload quarantine: bound is not a positive integer; using the default"
                );
                default
            }
        },
    }
}

/// What [`PendingUploadsLog::prune_quarantine`] archived.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct QuarantinePrune {
    /// The days archived, oldest first.
    pub archived_days: Vec<String>,
    /// Entries those days held.
    pub archived_entries: usize,
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

    /// Whether an entry for `(table_id, sstable_id)` is pending: one `stat`,
    /// no read. An error other than "not found" is returned, so a caller
    /// deciding whether an SSTable may be deleted can fail closed.
    pub fn contains(&self, table_id: &str, sstable_id: &str) -> std::io::Result<bool> {
        match std::fs::symlink_metadata(self.entry_path(table_id, sstable_id)) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
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
    /// entry that is not there succeeds: removal is idempotent. The table
    /// directory stays, even when this empties it (see the module docs).
    pub fn remove_entry(&self, table_id: &str, sstable_id: &str) -> std::io::Result<()> {
        let start = Instant::now();
        let path = self.entry_path(table_id, sstable_id);
        match std::fs::remove_file(&path) {
            Ok(()) => sync_dir(path.parent().expect("an entry path has a table directory"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        crate::metrics::observe_upload_phase(
            crate::metrics::UploadPhase::PendingLogRemove,
            start.elapsed(),
        );
        Ok(())
    }

    /// Move `record` out of the live ledger into today's quarantine, with
    /// `reason`.
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
            "{}~{}.{}.json",
            encode_component(&record.table_id),
            encode_component(&record.sstable_id),
            quarantined.quarantined_at_unix_secs
        );
        let day_dir = self.quarantine_day_dir(quarantined.quarantined_at_unix_secs)?;
        write_durably(&day_dir, &name, &bytes)?;
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
    /// This walks the whole tree; it is for startup replay, never for a
    /// per-cycle check (use [`Self::contains`]). Entries are published by
    /// rename, so a crash cannot leave a torn one. An entry that still does
    /// not parse, or that is filed under a different table or SSTable than it
    /// names, is corrupt: it is logged at ERROR, moved to quarantine and left
    /// out — never replayed as garbage.
    pub fn pending_records(&self) -> std::io::Result<Vec<PendingUploadRecord>> {
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == QUARANTINE_DIR || name.to_string_lossy().starts_with('.') {
                continue;
            }
            if !entry.file_type()?.is_dir() {
                tracing::warn!(
                    path = %entry.path().display(),
                    "pending upload ledger: file outside any table directory is not an entry; \
                     migration at engine open converts the flat layout"
                );
                continue;
            }
            self.collect_table_records(&entry.path(), &name.to_string_lossy(), &mut records)?;
        }
        records.sort_by(|a, b| {
            (a.table_id.as_str(), a.sstable_id.as_str())
                .cmp(&(b.table_id.as_str(), b.sstable_id.as_str()))
        });
        Ok(records)
    }

    fn collect_table_records(
        &self,
        table_dir: &Path,
        table_dir_name: &str,
        records: &mut Vec<PendingUploadRecord>,
    ) -> std::io::Result<()> {
        let entries = match std::fs::read_dir(table_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || !name.ends_with(ENTRY_SUFFIX) {
                continue;
            }
            match read_entry(&entry.path()) {
                Ok(Some(record)) => {
                    let filed_as = (table_dir_name, name.as_str());
                    let names = (
                        encode_component(&record.table_id),
                        entry_file_name(&record.sstable_id),
                    );
                    if filed_as == (names.0.as_str(), names.1.as_str()) {
                        records.push(record);
                    } else {
                        self.quarantine_corrupt_file(
                            &entry.path(),
                            &format!(
                                "entry names {}/{} but is filed as {table_dir_name}/{name}",
                                record.table_id, record.sstable_id
                            ),
                        );
                    }
                }
                // Removed by a concurrent writer between the listing and the read.
                Ok(None) => {}
                Err(EntryError::Io(e)) => return Err(e),
                Err(EntryError::Corrupt(reason)) => {
                    self.quarantine_corrupt_file(&entry.path(), &reason)
                }
            }
        }
        Ok(())
    }

    /// Archive whole quarantine days past `retention`, oldest first: every
    /// day older than `max_age_days`, then the oldest days while more than
    /// `max_entries` entries remain. Each archived day's entries are appended
    /// to `quarantine/archive/{day}.jsonl` before its files are removed, so
    /// nothing is dropped. Reaching the bound is logged at ERROR and counted
    /// in `pending_upload_quarantine_archived_total`; each archived day is
    /// logged with the entries it held. `now_unix_secs` is a parameter so a
    /// test can choose the day.
    pub fn prune_quarantine(
        &self,
        retention: &QuarantineRetention,
        now_unix_secs: u64,
    ) -> std::io::Result<QuarantinePrune> {
        let days = self.quarantine_days()?;
        let cutoff = utc_date(
            now_unix_secs.saturating_sub(retention.max_age_days.saturating_mul(SECS_PER_DAY)),
        );
        let mut remaining: usize = days.iter().map(|(_, count)| count).sum();
        let mut outcome = QuarantinePrune::default();
        for (day, count) in days {
            let too_old = day < cutoff;
            if !too_old && remaining <= retention.max_entries {
                break;
            }
            self.archive_quarantine_day(&day)?;
            remaining -= count;
            outcome.archived_entries += count;
            outcome.archived_days.push(day);
        }
        if !outcome.archived_days.is_empty() {
            crate::metrics::add_pending_upload_quarantine_archived(outcome.archived_entries as u64);
            tracing::error!(
                archived_days = ?outcome.archived_days,
                archived_entries = outcome.archived_entries,
                remaining_entries = remaining,
                max_entries = retention.max_entries,
                max_age_days = retention.max_age_days,
                archive = %self.quarantine_dir().join(QUARANTINE_ARCHIVE_DIR).display(),
                "pending upload quarantine reached its retention bound; archived whole days, \
                 nothing dropped. Quarantined uploads mean lost or untracked SSTables: inspect them"
            );
        }
        Ok(outcome)
    }

    /// Unarchived quarantine days with their entry counts, oldest first.
    fn quarantine_days(&self) -> std::io::Result<Vec<(String, usize)>> {
        let mut days = Vec::new();
        let listing = match std::fs::read_dir(self.quarantine_dir()) {
            Ok(listing) => listing,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(days),
            Err(e) => return Err(e),
        };
        for entry in listing {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !entry.file_type()?.is_dir() || !is_utc_date(&name) {
                continue;
            }
            let count = std::fs::read_dir(entry.path())?.count();
            days.push((name, count));
        }
        days.sort();
        Ok(days)
    }

    /// Append every entry of quarantine day `day` to its archive file, fsync
    /// it, then remove the day's files and directory.
    fn archive_quarantine_day(&self, day: &str) -> std::io::Result<()> {
        let day_dir = self.quarantine_dir().join(day);
        let mut files: Vec<PathBuf> = std::fs::read_dir(&day_dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<_>>()?;
        files.sort();
        let archive_dir = ensure_subdir(&self.quarantine_dir(), QUARANTINE_ARCHIVE_DIR)?;
        let archive_path = archive_dir.join(format!("{day}.jsonl"));
        let mut archive = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&archive_path)?;
        let mut names = Vec::with_capacity(files.len());
        for path in &files {
            let mut content = Vec::new();
            File::open(path)?
                .take(MAX_ENTRY_BYTES)
                .read_to_end(&mut content)?;
            let name = path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
            let line = serde_json::json!({
                "file": name,
                "content": String::from_utf8_lossy(&content),
            });
            archive.write_all(line.to_string().as_bytes())?;
            archive.write_all(b"\n")?;
            names.push(name);
        }
        archive.sync_all()?;
        sync_dir(&archive_dir)?;
        for path in &files {
            std::fs::remove_file(path)?;
        }
        std::fs::remove_dir(&day_dir)?;
        sync_dir(&self.quarantine_dir())?;
        tracing::info!(
            day,
            archive = %archive_path.display(),
            entries = ?names,
            "pending upload quarantine: archived a day and removed its files"
        );
        Ok(())
    }

    fn entry_path(&self, table_id: &str, sstable_id: &str) -> PathBuf {
        self.dir
            .join(encode_component(table_id))
            .join(entry_file_name(sstable_id))
    }

    /// `quarantine/{YYYY-MM-DD}` for `unix_secs`, created durably.
    fn quarantine_day_dir(&self, unix_secs: u64) -> std::io::Result<PathBuf> {
        let quarantine = ensure_subdir(&self.dir, QUARANTINE_DIR)?;
        ensure_subdir(&quarantine, &utc_date(unix_secs))
    }

    /// Write an entry where [`Self::write_entry`] would, without any fsync,
    /// so a test can lay down thousands of entries quickly.
    #[cfg(test)]
    pub(crate) fn write_entry_unsynced_for_test(&self, table_id: &str, sstable_id: &str) {
        let path = self.entry_path(table_id, sstable_id);
        std::fs::create_dir_all(path.parent().expect("entry has a parent")).unwrap();
        let record = PendingUploadRecord {
            table_id: table_id.to_string(),
            sstable_id: sstable_id.to_string(),
            compaction: None,
        };
        std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    }

    fn write_entry(&self, record: &PendingUploadRecord) -> std::io::Result<()> {
        let table_dir_name = encode_component(&record.table_id);
        if table_dir_name == QUARANTINE_DIR {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "table id {:?} collides with the ledger's quarantine directory",
                    record.table_id
                ),
            ));
        }
        let bytes = serde_json::to_vec(record).map_err(std::io::Error::other)?;
        let table_dir = ensure_subdir(&self.dir, &table_dir_name)?;
        write_durably(&table_dir, &entry_file_name(&record.sstable_id), &bytes)
    }

    /// Move a corrupt entry file into today's quarantine, loudly. Failing to
    /// move it is logged too: the entry is still left out of the records, and
    /// the next read reports it again.
    fn quarantine_corrupt_file(&self, path: &Path, reason: &str) {
        crate::metrics::inc_pending_upload_ledger_corrupt_entries();
        let component = |p: Option<&std::ffi::OsStr>| {
            p.map_or_else(|| "unnamed".into(), |n| n.to_string_lossy().into_owned())
        };
        let source_dir = path.parent().unwrap_or(&self.dir).to_path_buf();
        let name = format!(
            "{}~{}.corrupt.{}",
            component(source_dir.file_name()),
            component(path.file_name()),
            unix_secs()
        );
        let moved = self.quarantine_day_dir(unix_secs()).and_then(|day_dir| {
            let target = day_dir.join(&name);
            std::fs::rename(path, &target)?;
            sync_dir(&source_dir)?;
            Ok(target)
        });
        match moved {
            Ok(target) => tracing::error!(
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

#[cfg(test)]
thread_local! {
    /// Entry files this thread has opened, so a test can prove a code path
    /// reads none (the eviction guard must cost O(1) per candidate).
    static ENTRY_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Entry files the calling thread has opened through [`read_entry`].
#[cfg(test)]
pub(crate) fn entry_reads_on_this_thread() -> u64 {
    ENTRY_READS.with(std::cell::Cell::get)
}

/// Read and parse one entry file. `Ok(None)` when it no longer exists. The
/// caller checks that it is filed where it says it belongs.
fn read_entry(path: &Path) -> Result<Option<PendingUploadRecord>, EntryError> {
    #[cfg(test)]
    ENTRY_READS.with(|reads| reads.set(reads.get() + 1));
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
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| EntryError::Corrupt(format!("entry does not parse: {e}")))
}

/// `{sstable}.pending`, percent-encoded.
fn entry_file_name(sstable_id: &str) -> String {
    format!("{}{ENTRY_SUFFIX}", encode_component(sstable_id))
}

/// The name the flat layout gave an entry: `{table}~{sstable}.pending`.
fn flat_entry_file_name(table_id: &str, sstable_id: &str) -> String {
    format!(
        "{}~{}{ENTRY_SUFFIX}",
        encode_component(table_id),
        encode_component(sstable_id)
    )
}

/// Percent-encode everything but `[A-Za-z0-9._-]`, and a leading `.`, so the
/// result is one path component that is never `.`, `..` or a hidden name
/// (the walk skips hidden names: they are temp files).
fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, b) in s.bytes().enumerate() {
        let plain = b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-') || (b == b'.' && i > 0);
        if plain {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Make sure `parent/name` exists and its directory entry is durable.
///
/// Creating it fsyncs the new directory, and `parent` is fsynced every time:
/// a caller that finds the directory already there cannot tell whether its
/// creator's fsync of `parent` has finished, and returning before it has
/// would let a crash lose the entry about to be written inside.
fn ensure_subdir(parent: &Path, name: &str) -> std::io::Result<PathBuf> {
    let dir = parent.join(name);
    match std::fs::create_dir(&dir) {
        Ok(()) => sync_dir(&dir)?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    sync_dir(parent)?;
    Ok(dir)
}

/// Write `bytes` to `dir/name` so that a crash leaves either the old file or
/// the complete new one: unique temp file, fsync, rename, fsync the directory.
/// `dir` must exist.
fn write_durably(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
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

/// Seconds since the Unix epoch; 0 if the clock is before it.
pub(crate) fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// `YYYY-MM-DD` (UTC) of `unix_secs`.
fn utc_date(unix_secs: u64) -> String {
    let (year, month, day) = crate::commitlog::archiver::days_to_ymd(unix_secs / SECS_PER_DAY);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Whether `name` has the `YYYY-MM-DD` shape a quarantine day directory has.
fn is_utc_date(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
}

/// What [`migrate_ledger`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LedgerMigration {
    /// Legacy-log records carried into the tree as they were.
    pub migrated: usize,
    /// Damaged legacy records (NUL-prefixed, unreadable context) kept pending.
    pub salvaged: usize,
    /// Legacy lines that could not be parsed and were quarantined.
    pub quarantined: usize,
    /// Flat-layout entries moved into the tree.
    pub flat_migrated: usize,
    /// Flat-layout entries that did not parse and were quarantined.
    pub flat_quarantined: usize,
    /// Quarantine files outside any day directory moved into today's.
    pub quarantine_files_dated: usize,
    /// Empty table directories removed.
    pub empty_table_dirs_removed: usize,
}

/// Bring `data_dir`'s ledger to the tree layout: convert a legacy log, then
/// any flat-layout entries and undated quarantine files, then remove empty
/// table directories.
///
/// Must run before anything else in the process mutates the ledger — the
/// engine calls it from its constructors, before it exists to be used — which
/// is also what makes removing empty table directories safe here. Re-running
/// after a crash part-way is safe: an entry is written by rename, so writing
/// it twice leaves one copy, and each source is removed only after its copy
/// is durable.
pub fn migrate_ledger(data_dir: &Path) -> std::io::Result<LedgerMigration> {
    let mut outcome = LedgerMigration::default();
    migrate_legacy_log(data_dir, &mut outcome)?;
    if !data_dir.join(LEDGER_DIR).is_dir() {
        return Ok(outcome);
    }
    let ledger = PendingUploadsLog::open(data_dir)?;
    migrate_flat_entries(&ledger, &mut outcome)?;
    date_flat_quarantine_files(&ledger, &mut outcome)?;
    remove_empty_table_dirs(&ledger, &mut outcome)?;
    if outcome != LedgerMigration::default() {
        tracing::info!(
            ?outcome,
            "pending upload ledger: migrated to the tree layout"
        );
    }
    Ok(outcome)
}

/// Convert `{data_dir}/pending-uploads.log` straight into the tree.
fn migrate_legacy_log(data_dir: &Path, outcome: &mut LedgerMigration) -> std::io::Result<()> {
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
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let ledger = PendingUploadsLog::open(data_dir)?;
    let mut quarantined_lines: Vec<u8> = Vec::new();
    for raw in bytes.split(|&b| b == b'\n') {
        if raw.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let line = parse_legacy_line(raw);
        let damaged = !matches!(line, LegacyLine::Record(_));
        if let Some(record) = migrate_legacy_line(line, raw, outcome) {
            ledger.write_entry(&record)?;
        }
        if damaged {
            quarantined_lines.extend_from_slice(raw);
            quarantined_lines.push(b'\n');
        }
    }
    let stamp = unix_secs();
    if !quarantined_lines.is_empty() {
        write_durably(
            &ledger.quarantine_day_dir(stamp)?,
            &format!("{LEGACY_LOG_FILE}.{stamp}.lines"),
            &quarantined_lines,
        )?;
    }
    std::fs::rename(
        &legacy,
        data_dir.join(format!("{LEGACY_LOG_FILE}.migrated-{stamp}")),
    )?;
    sync_dir(data_dir)
}

/// Report one parsed legacy line and return the record to keep pending, if
/// any.
fn migrate_legacy_line(
    line: LegacyLine,
    raw: &[u8],
    outcome: &mut LedgerMigration,
) -> Option<PendingUploadRecord> {
    match line {
        LegacyLine::Record(record) => {
            outcome.migrated += 1;
            Some(record)
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
            outcome.salvaged += 1;
            Some(record)
        }
        LegacyLine::Corrupt(reason) => {
            tracing::error!(
                reason,
                line = %String::from_utf8_lossy(raw).escape_debug(),
                "pending upload ledger: corrupt legacy line quarantined; it will not be replayed"
            );
            crate::metrics::inc_pending_upload_ledger_corrupt_entries();
            outcome.quarantined += 1;
            None
        }
    }
}

/// Move flat-layout `{table}~{sstable}.pending` files at the ledger root into
/// the tree; quarantine any that do not parse or are misnamed.
fn migrate_flat_entries(
    ledger: &PendingUploadsLog,
    outcome: &mut LedgerMigration,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(&ledger.dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type()?.is_file() || name.starts_with('.') || !name.ends_with(ENTRY_SUFFIX) {
            continue;
        }
        match read_entry(&entry.path()) {
            Ok(Some(record))
                if flat_entry_file_name(&record.table_id, &record.sstable_id) == name =>
            {
                ledger.write_entry(&record)?;
                std::fs::remove_file(entry.path())?;
                outcome.flat_migrated += 1;
            }
            Ok(Some(_)) => {
                ledger.quarantine_corrupt_file(&entry.path(), "flat entry names another SSTable");
                outcome.flat_quarantined += 1;
            }
            Ok(None) => {}
            Err(EntryError::Io(e)) => return Err(e),
            Err(EntryError::Corrupt(reason)) => {
                ledger.quarantine_corrupt_file(&entry.path(), &reason);
                outcome.flat_quarantined += 1;
            }
        }
    }
    sync_dir(&ledger.dir)
}

/// Move quarantine files that sit outside any day directory (the flat layout
/// kept them there) into today's day directory.
fn date_flat_quarantine_files(
    ledger: &PendingUploadsLog,
    outcome: &mut LedgerMigration,
) -> std::io::Result<()> {
    let quarantine = ledger.quarantine_dir();
    let listing = match std::fs::read_dir(&quarantine) {
        Ok(listing) => listing,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut undated = Vec::new();
    for entry in listing {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            undated.push(entry);
        }
    }
    if undated.is_empty() {
        return Ok(());
    }
    let day_dir = ledger.quarantine_day_dir(unix_secs())?;
    for entry in undated {
        std::fs::rename(entry.path(), day_dir.join(entry.file_name()))?;
        outcome.quarantine_files_dated += 1;
    }
    sync_dir(&day_dir)?;
    sync_dir(&quarantine)
}

/// Remove table directories holding no entries. Safe only while nothing else
/// can add to the ledger, which is why only [`migrate_ledger`] calls it.
fn remove_empty_table_dirs(
    ledger: &PendingUploadsLog,
    outcome: &mut LedgerMigration,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(&ledger.dir)? {
        let entry = entry?;
        if entry.file_name() == QUARANTINE_DIR || !entry.file_type()?.is_dir() {
            continue;
        }
        if std::fs::read_dir(entry.path())?.next().is_some() {
            continue;
        }
        std::fs::remove_dir(entry.path())?;
        outcome.empty_table_dirs_removed += 1;
    }
    if outcome.empty_table_dirs_removed > 0 {
        sync_dir(&ledger.dir)?;
    }
    Ok(())
}

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

    /// The tree layout: one directory per table, one file per SSTable. An
    /// entry survives a reopen (a restart), a removal takes only its own
    /// file, and the emptied table directory stays for the next add.
    #[test]
    fn tree_layout_round_trips_through_add_remove_and_reopen() {
        let dir = tempdir().unwrap();
        {
            let log = PendingUploadsLog::open(dir.path()).unwrap();
            log.add_compaction_entry("ks.t", "5", compaction_context("5"))
                .unwrap();
            log.add_entry("ks.t", "6").unwrap();
            log.add_entry("ks.u", "7").unwrap();
        }
        let tree = dir.path().join(LEDGER_DIR);
        assert!(tree.join("ks.t").join("5.pending").is_file());
        assert!(tree.join("ks.t").join("6.pending").is_file());
        assert!(tree.join("ks.u").join("7.pending").is_file());

        let log = PendingUploadsLog::open(dir.path()).unwrap();
        assert!(log.contains("ks.t", "5").unwrap());
        assert!(!log.contains("ks.t", "8").unwrap());
        log.remove_entry("ks.u", "7").unwrap();
        assert!(!log.contains("ks.u", "7").unwrap());
        assert!(
            tree.join("ks.u").is_dir(),
            "an emptied table dir stays in place"
        );

        let reopened = PendingUploadsLog::open(dir.path()).unwrap();
        let records = reopened.pending_records().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (r.table_id.as_str(), r.sstable_id.as_str()))
                .collect::<Vec<_>>(),
            vec![("ks.t", "5"), ("ks.t", "6")]
        );
        assert_eq!(records[0].compaction, Some(compaction_context("5")));
    }

    /// Many writers creating the same new table directory at once, adding
    /// and removing entries in it: every add that returned must be visible,
    /// and no removal may take another writer's entry.
    #[test]
    fn concurrent_adds_and_removes_in_one_new_table_dir_all_survive() {
        const THREADS: usize = 16;
        const PER_THREAD: usize = 25;
        let dir = tempdir().unwrap();
        let log = std::sync::Arc::new(PendingUploadsLog::open(dir.path()).unwrap());
        let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
        let workers: Vec<_> = (0..THREADS)
            .map(|thread| {
                let log = std::sync::Arc::clone(&log);
                let start = std::sync::Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for i in 0..PER_THREAD {
                        log.add_entry("ks.shared", &format!("{thread}-{i}-kept"))
                            .unwrap();
                        let removed = format!("{thread}-{i}-removed");
                        log.add_entry("ks.shared", &removed).unwrap();
                        log.remove_entry("ks.shared", &removed).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let mut kept = log.pending_entries().unwrap();
        kept.sort();
        let mut expected: Vec<(String, String)> = (0..THREADS)
            .flat_map(|t| {
                (0..PER_THREAD).map(move |i| ("ks.shared".to_string(), format!("{t}-{i}-kept")))
            })
            .collect();
        expected.sort();
        assert_eq!(kept, expected);
    }

    /// Quarantine is kept per day and bounded. Past the age or count bound a
    /// day is archived into one file, loudly, never dropped.
    #[test]
    fn quarantine_prune_archives_days_beyond_the_age_and_count_bounds() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        let day = 86_400;
        let now = 20_000 * day; // 2024-10-04
        for (age_days, files) in [(40u64, 2usize), (3, 3), (2, 4), (0, 1)] {
            let day_dir = log.quarantine_dir().join(utc_date(now - age_days * day));
            std::fs::create_dir_all(&day_dir).unwrap();
            for i in 0..files {
                std::fs::write(
                    day_dir.join(format!("entry-{i}.json")),
                    format!("{{\"n\":{i}}}"),
                )
                .unwrap();
            }
        }

        let outcome = log
            .prune_quarantine(
                &QuarantineRetention {
                    max_entries: 6,
                    max_age_days: 30,
                },
                now,
            )
            .unwrap();

        // The 40-day-old day goes on age; then 3+4+1 = 8 > 6 sends the oldest
        // remaining day (3 files) too, leaving 5.
        assert_eq!(
            outcome,
            QuarantinePrune {
                archived_days: vec![utc_date(now - 40 * day), utc_date(now - 3 * day)],
                archived_entries: 5,
            }
        );
        let remaining: usize = std::fs::read_dir(log.quarantine_dir())
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_name() != QUARANTINE_ARCHIVE_DIR)
            .map(|e| std::fs::read_dir(e.path()).unwrap().count())
            .sum();
        assert_eq!(remaining, 5);
        let archived = std::fs::read_to_string(
            log.quarantine_dir()
                .join(QUARANTINE_ARCHIVE_DIR)
                .join(format!("{}.jsonl", utc_date(now - 40 * day))),
        )
        .unwrap();
        assert_eq!(
            archived.lines().count(),
            2,
            "an archived day keeps every entry"
        );

        assert_eq!(
            log.prune_quarantine(
                &QuarantineRetention {
                    max_entries: 6,
                    max_age_days: 30,
                },
                now,
            )
            .unwrap(),
            QuarantinePrune::default(),
            "a pruned quarantine is within its bounds"
        );
    }

    /// The flat layout (one directory, `{table}~{sstable}.pending`) existed
    /// only on an unmerged branch; any data directory that ran it converts to
    /// the tree, and converting twice changes nothing.
    #[test]
    fn flat_layout_migrates_to_the_tree_idempotently() {
        let dir = tempdir().unwrap();
        let flat = dir.path().join(LEDGER_DIR);
        std::fs::create_dir_all(flat.join(QUARANTINE_DIR)).unwrap();
        for (table, sstable, compaction) in [
            ("ks.t", "10", Some(compaction_context("10"))),
            ("ks.u", "11", None),
        ] {
            let record = PendingUploadRecord {
                table_id: table.to_string(),
                sstable_id: sstable.to_string(),
                compaction,
            };
            std::fs::write(
                flat.join(format!("{table}~{sstable}.pending")),
                serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
        }
        std::fs::write(flat.join("ks.t~12.pending"), b"garbage").unwrap();
        std::fs::write(flat.join(QUARANTINE_DIR).join("old.json"), b"{}").unwrap();

        let first = migrate_ledger(dir.path()).unwrap();
        assert_eq!(first.flat_migrated, 2);
        assert_eq!(first.flat_quarantined, 1);
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        assert!(flat.join("ks.t").join("10.pending").is_file());
        assert!(flat.join("ks.u").join("11.pending").is_file());
        assert!(!flat.join("ks.t~10.pending").exists());
        assert_eq!(
            log.pending_records().unwrap()[0].compaction,
            Some(compaction_context("10"))
        );
        assert!(
            !flat.join(QUARANTINE_DIR).join("old.json").exists(),
            "flat quarantine files move under a day directory"
        );

        assert_eq!(
            migrate_ledger(dir.path()).unwrap(),
            LedgerMigration::default()
        );
        assert_eq!(log.pending_entries().unwrap().len(), 2);
    }

    /// A ledger entry that does not parse is reported and moved to
    /// quarantine, never returned for replay — and never fatal to the read.
    #[test]
    fn a_corrupt_entry_file_is_quarantined_not_replayed() {
        let dir = tempdir().unwrap();
        let log = PendingUploadsLog::open(dir.path()).unwrap();
        log.add_entry("ks.t", "1").unwrap();
        let table_dir = dir.path().join(LEDGER_DIR).join("ks.t");
        let corrupt = table_dir.join(entry_file_name("2"));
        std::fs::write(&corrupt, b"\0\0\0 not json").unwrap();
        let misfiled = table_dir.join(entry_file_name("3"));
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
        assert_eq!(quarantined_files(&log).len(), 2);
    }

    /// Every file under the quarantine's day directories, oldest day first.
    fn quarantined_files(log: &PendingUploadsLog) -> Vec<PathBuf> {
        let mut days: Vec<PathBuf> = std::fs::read_dir(log.quarantine_dir())
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_name() != QUARANTINE_ARCHIVE_DIR)
            .map(|e| e.path())
            .collect();
        days.sort();
        for day in &days {
            assert!(
                is_utc_date(&day.file_name().unwrap().to_string_lossy()),
                "quarantine holds something outside a day directory: {}",
                day.display()
            );
        }
        let mut files: Vec<PathBuf> = days
            .iter()
            .flat_map(|day| std::fs::read_dir(day).unwrap().map(|e| e.unwrap().path()))
            .collect();
        files.sort();
        files
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
        let kept: Vec<_> = quarantined_files(&log)
            .iter()
            .map(|path| std::fs::read_to_string(path).unwrap())
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

        let outcome = migrate_ledger(dir.path()).unwrap();

        assert_eq!(
            outcome,
            LedgerMigration {
                migrated: 2,
                salvaged: 1,
                quarantined: 2,
                ..LedgerMigration::default()
            }
        );
        let tree = dir.path().join(LEDGER_DIR);
        assert!(
            tree.join("ks.t").join("12.pending").is_file(),
            "the legacy log migrates straight into the tree"
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
        let quarantined = quarantined_files(&log);
        assert_eq!(quarantined.len(), 1);
        let quarantined = std::fs::read(&quarantined[0]).unwrap();
        assert_eq!(
            quarantined
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .count(),
            3,
            "the NUL-prefixed line and both garbage lines are kept raw"
        );

        assert_eq!(
            migrate_ledger(dir.path()).unwrap(),
            LedgerMigration::default(),
            "a migrated data directory has nothing left to migrate"
        );
    }
}
