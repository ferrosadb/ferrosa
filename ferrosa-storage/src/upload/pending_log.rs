//! Append-only ledger of in-progress SSTable uploads.
//!
//! Written (and fsynced) before flush completes; entries removed after S3
//! confirms the upload.  On restart, any remaining entries identify SSTables
//! that were flushed but not yet durably stored in S3 — the startup replay
//! path re-submits their uploads before the manifest is updated.
//!
//! # Format
//!
//! One entry per line: `{table_id}\t{sstable_id}\n`.  Both fields are
//! URL-encoded to ensure that tabs and newlines in identifiers do not corrupt
//! the file (in practice, table/SSTable IDs never contain these characters,
//! but encoding is cheap insurance).
//!
//! # Durability contract
//!
//! `add_entry` fsyncs the file before returning, so a crash that occurs after
//! `add_entry` returns will find the entry on the next startup.  Without the
//! fsync the crash window we are trying to close would merely shift earlier
//! instead of being eliminated.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::manifest::ManifestEntry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCompactionUpload {
    pub remove_input_ids: Vec<String>,
    pub output: ManifestEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUploadRecord {
    pub table_id: String,
    pub sstable_id: String,
    pub compaction: Option<PendingCompactionUpload>,
}

/// Append-only ledger tracking SSTable uploads that have been flushed to disk
/// but not yet confirmed by S3.
pub struct PendingUploadsLog {
    path: PathBuf,
}

impl PendingUploadsLog {
    /// Open (or create) the pending-uploads log at the given path.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        // Touch the file so it exists from the start.
        if !path.exists() {
            File::create(path)?;
        }
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    /// Append a pending-upload entry and fsync before returning.
    ///
    /// The fsync ensures the entry survives a crash that occurs immediately
    /// after this call returns — a fundamental requirement for closing the
    /// manifest-before-S3 crash window.
    pub fn add_entry(&self, table_id: &str, sstable_id: &str) -> std::io::Result<()> {
        let encoded = format!("{}\t{}\n", url_encode(table_id), url_encode(sstable_id),);
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(encoded.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Append a pending-upload entry for a compaction output.
    ///
    /// The extra manifest context lets crash recovery finish the same
    /// upload-confirm → manifest-update → log-remove sequence that
    /// `poll_compactions` would have completed before the crash.
    pub fn add_compaction_entry(
        &self,
        table_id: &str,
        sstable_id: &str,
        compaction: PendingCompactionUpload,
    ) -> std::io::Result<()> {
        let payload = serde_json::to_string(&compaction).map_err(std::io::Error::other)?;
        let encoded = format!(
            "{}\t{}\t{}\n",
            url_encode(table_id),
            url_encode(sstable_id),
            url_encode(&payload),
        );
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(encoded.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Remove the entry for the given `(table_id, sstable_id)` by rewriting the file.
    ///
    /// This is O(n) in the number of pending entries, which is acceptable:
    /// the list is short (at most a few hundred entries) and this is called
    /// only after S3 confirms an upload.
    pub fn remove_entry(&self, table_id: &str, sstable_id: &str) -> std::io::Result<()> {
        let entries = self.pending_entries()?;
        let encoded_table_id = url_encode(table_id);
        let encoded_id = url_encode(sstable_id);
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        for (tid, sid) in &entries {
            if url_encode(tid) != encoded_table_id || url_encode(sid) != encoded_id {
                let line = format!("{}\t{}\n", url_encode(tid), url_encode(sid));
                file.write_all(line.as_bytes())?;
            }
        }
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Return all pending `(table_id, sstable_id)` pairs.
    ///
    /// Lines that do not parse cleanly are silently skipped — a partially
    /// written line from a crash cannot be replayed and is harmless to ignore
    /// (the SSTable will simply not be in S3; a subsequent repair cycle or
    /// re-flush will handle it).
    pub fn pending_entries(&self) -> std::io::Result<Vec<(String, String)>> {
        Ok(self
            .pending_records()?
            .into_iter()
            .map(|record| (record.table_id, record.sstable_id))
            .collect())
    }

    /// Return all pending upload records, including optional compaction context.
    ///
    /// The original two-field log format remains supported. Malformed JSON in
    /// the optional third field degrades to a plain upload record so recovery
    /// still retries the SSTable upload instead of dropping the entry.
    pub fn pending_records(&self) -> std::io::Result<Vec<PendingUploadRecord>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut result = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(3, '\t');
            if let (Some(tid_enc), Some(sid_enc)) = (parts.next(), parts.next()) {
                let compaction = parts
                    .next()
                    .map(url_decode)
                    .and_then(|payload| serde_json::from_str(&payload).ok());
                result.push(PendingUploadRecord {
                    table_id: url_decode(tid_enc),
                    sstable_id: url_decode(sid_enc),
                    compaction,
                });
            }
            // Lines without a tab are skipped (corrupt/partial write).
        }
        Ok(result)
    }
}

/// Percent-encode the characters that would corrupt the log format (`\t`, `\n`,
/// `\r`, `%`).  All other bytes pass through unchanged.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'\t' => out.push_str("%09"),
            b'\n' => out.push_str("%0A"),
            b'\r' => out.push_str("%0D"),
            b'%' => out.push_str("%25"),
            _ => out.push(b as char),
        }
    }
    out
}

/// Decode the small subset of percent-encoding produced by `url_encode`.
fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
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
        let log_path = dir.path().join("pending-uploads.log");
        let log = PendingUploadsLog::open(&log_path).unwrap();

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
        let log_path = dir.path().join("pending-uploads.log");
        let log = PendingUploadsLog::open(&log_path).unwrap();

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
        let log_path = dir.path().join("pending-uploads.log");
        let log = PendingUploadsLog::open(&log_path).unwrap();

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
        let log_path = dir.path().join("pending-uploads.log");
        let log = PendingUploadsLog::open(&log_path).unwrap();

        let compaction = PendingCompactionUpload {
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
        let log_path = dir.path().join("pending-uploads.log");

        // First handle: write an entry.
        {
            let log = PendingUploadsLog::open(&log_path).unwrap();
            log.add_entry("ks.t", "crash-sst").unwrap();
        }

        // Second handle (simulating restart): entry must still be there.
        {
            let log = PendingUploadsLog::open(&log_path).unwrap();
            let entries = log.pending_entries().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].1, "crash-sst");
        }
    }

    #[test]
    fn url_encode_decode_roundtrip() {
        let cases = ["plain", "tab\there", "new\nline", "percent%here", ""];
        for s in cases {
            assert_eq!(url_decode(&url_encode(s)), s, "roundtrip failed for {s:?}");
        }
    }
}
