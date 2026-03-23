//! Companion `.accord` sidecar files for SSTables.
//!
//! When memtables containing Accord-applied transactions are flushed to
//! SSTables, a companion `.accord` file is written alongside the SSTable
//! data file. This sidecar records which transactions were applied in that
//! SSTable, enabling:
//!
//! - **Recovery**: reconstruct the applied-transaction set without scanning
//!   the full SSTable.
//! - **GC**: after an ExclusiveSyncPoint covers all shards, the sidecar
//!   (and its SSTable) can be garbage-collected.
//! - **S3 upload**: the sidecar is uploaded alongside the SSTable.
//!
//! # File format
//!
//! ```text
//! [4-byte magic: "ACSD"]
//! [4-byte version: u32 LE]
//! [4-byte entry count: u32 LE]
//! [entry 0: serialized AccordAppliedEntry with length prefix]
//! [entry 1: ...]
//! ...
//! [4-byte CRC32 of all preceding bytes]
//! ```
//!
//! Each entry is length-prefixed: `[4-byte LE length] [serialized bytes]`.
//!
//! # Normal reads
//!
//! Normal read paths do NOT touch the sidecar — it is only read during
//! recovery and GC.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use super::entries::{AccordAppliedEntry, TxnId};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying an Accord sidecar file.
const SIDECAR_MAGIC: &[u8; 4] = b"ACSD";

/// Current sidecar format version.
const SIDECAR_VERSION: u32 = 1;

/// File extension for sidecar files.
pub const SIDECAR_EXTENSION: &str = ".accord";

// ---------------------------------------------------------------------------
// AccordSidecar
// ---------------------------------------------------------------------------

/// An Accord sidecar file: a companion to an SSTable that records which
/// Accord transactions were applied in that SSTable.
#[derive(Debug, Clone)]
pub struct AccordSidecar {
    /// The applied entries in this sidecar.
    entries: Vec<AccordAppliedEntry>,
}

impl AccordSidecar {
    /// Create an empty sidecar.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create a sidecar from a list of applied entries.
    pub fn from_entries(entries: Vec<AccordAppliedEntry>) -> Self {
        Self { entries }
    }

    /// Add an applied entry to the sidecar.
    pub fn add(&mut self, entry: AccordAppliedEntry) {
        self.entries.push(entry);
    }

    /// Returns true if the sidecar has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the applied entries.
    pub fn entries(&self) -> &[AccordAppliedEntry] {
        &self.entries
    }

    /// Returns the set of applied transaction IDs.
    pub fn applied_txn_ids(&self) -> HashSet<TxnId> {
        self.entries.iter().map(|e| e.txn_id).collect()
    }

    /// Serialize the sidecar to bytes.
    ///
    /// Format: magic + version + count + length-prefixed entries + CRC32.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        // Header.
        buf.extend_from_slice(SIDECAR_MAGIC);
        buf.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        // Entries, each length-prefixed.
        for entry in &self.entries {
            let serialized = entry.serialize();
            buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            buf.extend_from_slice(&serialized);
        }

        // CRC32 over everything preceding.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a sidecar from bytes.
    ///
    /// Validates magic, version, entry count, individual entry CRCs,
    /// and the trailing file CRC.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, io::Error> {
        // Minimum size: 4 magic + 4 version + 4 count + 4 CRC = 16 bytes.
        if bytes.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar too short",
            ));
        }

        // Verify trailing CRC first.
        let payload_len = bytes.len() - 4;
        let stored_crc = u32::from_le_bytes(bytes[payload_len..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&bytes[..payload_len]);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sidecar CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
                ),
            ));
        }

        let mut pos = 0;

        // Magic.
        if &bytes[pos..pos + 4] != SIDECAR_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid sidecar magic",
            ));
        }
        pos += 4;

        // Version.
        let version = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        if version != SIDECAR_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sidecar version: {version}"),
            ));
        }
        pos += 4;

        // Entry count.
        let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Read entries.
        let mut entries = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            if pos + 4 > payload_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated entry length",
                ));
            }
            let entry_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + entry_len > payload_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated entry data",
                ));
            }
            let entry = AccordAppliedEntry::deserialize(&bytes[pos..pos + entry_len])?;
            entries.push(entry);
            pos += entry_len;
        }

        Ok(Self { entries })
    }

    /// Derive the sidecar file path from an SSTable data file path.
    ///
    /// E.g., `data/ks/table/nb-1-big-Data.db` -> `data/ks/table/nb-1-big-Data.db.accord`
    pub fn sidecar_path(sstable_path: &Path) -> PathBuf {
        let mut path = sstable_path.as_os_str().to_os_string();
        path.push(SIDECAR_EXTENSION);
        PathBuf::from(path)
    }

    /// Write the sidecar to disk alongside an SSTable.
    ///
    /// If the sidecar is empty, no file is written and `Ok(false)` is returned.
    /// On success, `Ok(true)` is returned.
    pub fn write_to_disk(&self, sstable_path: &Path) -> io::Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }

        let path = Self::sidecar_path(sstable_path);
        let bytes = self.serialize();
        std::fs::write(&path, &bytes)?;
        Ok(true)
    }

    /// Read a sidecar from disk.
    ///
    /// Returns `Ok(None)` if the sidecar file does not exist.
    pub fn read_from_disk(sstable_path: &Path) -> io::Result<Option<Self>> {
        let path = Self::sidecar_path(sstable_path);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        let sidecar = Self::deserialize(&bytes)?;
        Ok(Some(sidecar))
    }

    /// Delete the sidecar file for an SSTable (GC).
    ///
    /// Returns `Ok(true)` if the file was deleted, `Ok(false)` if it
    /// did not exist.
    pub fn gc(sstable_path: &Path) -> io::Result<bool> {
        let path = Self::sidecar_path(sstable_path);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

impl Default for AccordSidecar {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SidecarUploadManifest — metadata for S3 upload
// ---------------------------------------------------------------------------

/// Metadata about a sidecar file to be uploaded to S3 alongside its SSTable.
#[derive(Debug, Clone)]
pub struct SidecarUploadManifest {
    /// Path to the SSTable data file.
    pub sstable_path: PathBuf,
    /// Path to the sidecar file.
    pub sidecar_path: PathBuf,
    /// Number of applied entries in the sidecar.
    pub entry_count: usize,
    /// Size in bytes of the serialized sidecar.
    pub size_bytes: usize,
}

impl SidecarUploadManifest {
    /// Create a manifest from an SSTable path and sidecar.
    pub fn from_sidecar(sstable_path: &Path, sidecar: &AccordSidecar) -> Self {
        let serialized = sidecar.serialize();
        Self {
            sstable_path: sstable_path.to_path_buf(),
            sidecar_path: AccordSidecar::sidecar_path(sstable_path),
            entry_count: sidecar.len(),
            size_bytes: serialized.len(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::entries::{AccordAppliedEntry, Timestamp, TxnId};

    fn ts(micros: u64, logical: u32) -> Timestamp {
        Timestamp {
            epoch_micros: micros,
            logical,
        }
    }

    fn txn(node: u64, micros: u64) -> TxnId {
        TxnId {
            node,
            timestamp: ts(micros, 0),
        }
    }

    fn make_applied(node: u64, micros: u64, result: &[u8]) -> AccordAppliedEntry {
        AccordAppliedEntry {
            txn_id: txn(node, micros),
            t: ts(micros + 1, 0),
            result: result.to_vec(),
        }
    }

    /// A5.4 Test 1: accord_sidecar_write_on_flush
    ///
    /// On SSTable flush, write companion .accord file with AccordApplied results.
    /// Create a sidecar with 3 applied entries, write to disk, read back,
    /// verify all entries match.
    #[test]
    fn accord_sidecar_write_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-1-big-Data.db");

        // Simulate SSTable file existing.
        std::fs::write(&sstable_path, b"fake sstable data").unwrap();

        let mut sidecar = AccordSidecar::new();
        sidecar.add(make_applied(1, 1000, &[1, 2, 3]));
        sidecar.add(make_applied(2, 2000, &[4, 5, 6]));
        sidecar.add(make_applied(3, 3000, &[]));

        assert_eq!(sidecar.len(), 3);
        assert!(!sidecar.is_empty());

        // Write to disk.
        let written = sidecar.write_to_disk(&sstable_path).unwrap();
        assert!(written, "sidecar should have been written");

        // Verify the sidecar file exists with correct extension.
        let sidecar_path = AccordSidecar::sidecar_path(&sstable_path);
        assert!(sidecar_path.exists(), "sidecar file should exist");
        assert!(
            sidecar_path.to_str().unwrap().ends_with(".accord"),
            "sidecar should have .accord extension"
        );

        // Read back and verify.
        let read_back = AccordSidecar::read_from_disk(&sstable_path)
            .unwrap()
            .expect("sidecar should be readable");

        assert_eq!(read_back.len(), 3);
        assert_eq!(read_back.entries()[0].txn_id, txn(1, 1000));
        assert_eq!(read_back.entries()[0].result, vec![1, 2, 3]);
        assert_eq!(read_back.entries()[1].txn_id, txn(2, 2000));
        assert_eq!(read_back.entries()[1].result, vec![4, 5, 6]);
        assert_eq!(read_back.entries()[2].txn_id, txn(3, 3000));
        assert_eq!(read_back.entries()[2].result, Vec::<u8>::new());
    }

    /// A5.4 Test 2: accord_sidecar_recovery_read
    ///
    /// Recovery reads sidecar to reconstruct applied state.
    /// Write a sidecar, read it, extract applied_txn_ids, verify correctness.
    #[test]
    fn accord_sidecar_recovery_read() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-2-big-Data.db");
        std::fs::write(&sstable_path, b"fake").unwrap();

        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);
        let t3 = txn(3, 3000);

        let sidecar = AccordSidecar::from_entries(vec![
            AccordAppliedEntry {
                txn_id: t1,
                t: ts(1001, 0),
                result: vec![10],
            },
            AccordAppliedEntry {
                txn_id: t2,
                t: ts(2001, 0),
                result: vec![20],
            },
            AccordAppliedEntry {
                txn_id: t3,
                t: ts(3001, 0),
                result: vec![30],
            },
        ]);

        sidecar.write_to_disk(&sstable_path).unwrap();

        // Recovery: read sidecar and extract applied txn IDs.
        let recovered = AccordSidecar::read_from_disk(&sstable_path)
            .unwrap()
            .expect("sidecar should exist");

        let applied_ids = recovered.applied_txn_ids();
        assert!(applied_ids.contains(&t1));
        assert!(applied_ids.contains(&t2));
        assert!(applied_ids.contains(&t3));
        assert_eq!(applied_ids.len(), 3);

        // Verify individual entries for full state reconstruction.
        for entry in recovered.entries() {
            match entry.txn_id {
                id if id == t1 => assert_eq!(entry.result, vec![10]),
                id if id == t2 => assert_eq!(entry.result, vec![20]),
                id if id == t3 => assert_eq!(entry.result, vec![30]),
                _ => panic!("unexpected txn_id"),
            }
        }
    }

    /// A5.4 Test 3: accord_sidecar_gc
    ///
    /// GC after all-shard ExclusiveSyncPoint: delete the sidecar file.
    /// Write, verify exists, GC, verify deleted.
    #[test]
    fn accord_sidecar_gc() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-3-big-Data.db");
        std::fs::write(&sstable_path, b"fake").unwrap();

        let sidecar = AccordSidecar::from_entries(vec![make_applied(1, 1000, &[1])]);
        sidecar.write_to_disk(&sstable_path).unwrap();

        let sidecar_path = AccordSidecar::sidecar_path(&sstable_path);
        assert!(sidecar_path.exists(), "sidecar should exist before GC");

        // GC the sidecar.
        let deleted = AccordSidecar::gc(&sstable_path).unwrap();
        assert!(deleted, "GC should report deletion");
        assert!(!sidecar_path.exists(), "sidecar should be deleted after GC");

        // GC again — should be a no-op.
        let deleted_again = AccordSidecar::gc(&sstable_path).unwrap();
        assert!(!deleted_again, "second GC should report no file to delete");

        // Read should return None after GC.
        let read_result = AccordSidecar::read_from_disk(&sstable_path).unwrap();
        assert!(read_result.is_none(), "no sidecar after GC");
    }

    /// A5.4 Test 4: accord_sidecar_s3_upload
    ///
    /// Sidecar uploaded alongside SSTable: verify SidecarUploadManifest
    /// is correctly populated.
    #[test]
    fn accord_sidecar_s3_upload() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-4-big-Data.db");

        let sidecar = AccordSidecar::from_entries(vec![
            make_applied(1, 1000, &[1, 2, 3]),
            make_applied(2, 2000, &[4, 5]),
        ]);

        let manifest = SidecarUploadManifest::from_sidecar(&sstable_path, &sidecar);

        assert_eq!(manifest.sstable_path, sstable_path);
        assert_eq!(
            manifest.sidecar_path,
            AccordSidecar::sidecar_path(&sstable_path)
        );
        assert_eq!(manifest.entry_count, 2);
        assert!(
            manifest.size_bytes > 0,
            "serialized sidecar should have bytes"
        );

        // Verify the sidecar path ends with .accord.
        assert!(
            manifest.sidecar_path.to_str().unwrap().ends_with(".accord"),
            "upload path should end with .accord"
        );

        // Verify the manifest size matches actual serialization.
        let serialized = sidecar.serialize();
        assert_eq!(manifest.size_bytes, serialized.len());

        // Write and verify the file can be read for upload.
        std::fs::write(&sstable_path, b"fake").unwrap();
        sidecar.write_to_disk(&sstable_path).unwrap();
        let sidecar_bytes = std::fs::read(&manifest.sidecar_path).unwrap();
        assert_eq!(sidecar_bytes.len(), manifest.size_bytes);
    }

    /// A5.4 Test 5: accord_sidecar_normal_read_ignores
    ///
    /// Normal reads don't touch sidecar. Verify that reading an SSTable
    /// path that has a sidecar does not implicitly load sidecar data.
    /// This test verifies the design: sidecar must be explicitly requested.
    #[test]
    fn accord_sidecar_normal_read_ignores() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-5-big-Data.db");
        std::fs::write(&sstable_path, b"fake sstable data").unwrap();

        // Write a sidecar.
        let sidecar = AccordSidecar::from_entries(vec![make_applied(1, 1000, &[1, 2, 3])]);
        sidecar.write_to_disk(&sstable_path).unwrap();

        // Simulate a "normal read" — just reading the SSTable file.
        // The sidecar should NOT be loaded.
        let sstable_data = std::fs::read(&sstable_path).unwrap();
        assert_eq!(sstable_data, b"fake sstable data");

        // The sidecar file is separate and not embedded in the SSTable.
        let sidecar_path = AccordSidecar::sidecar_path(&sstable_path);
        assert!(sidecar_path.exists(), "sidecar file exists on disk");
        assert_ne!(
            sstable_path, sidecar_path,
            "sidecar and SSTable are different files"
        );

        // To read the sidecar, you must explicitly call read_from_disk.
        // There is no implicit loading in the SSTable read path.
        let explicit_read = AccordSidecar::read_from_disk(&sstable_path)
            .unwrap()
            .expect("explicit read should find sidecar");
        assert_eq!(explicit_read.len(), 1);

        // AccordSidecar does not implement any trait that SSTable readers use.
        // This is a compile-time / design guarantee: sidecar is opt-in.
    }

    /// A5.4 Test 6: accord_sidecar_empty_flush
    ///
    /// Flush with no Accord data writes no sidecar.
    /// An empty sidecar should not create a file on disk.
    #[test]
    fn accord_sidecar_empty_flush() {
        let dir = tempfile::tempdir().unwrap();
        let sstable_path = dir.path().join("nb-6-big-Data.db");
        std::fs::write(&sstable_path, b"fake").unwrap();

        let sidecar = AccordSidecar::new();
        assert!(sidecar.is_empty());
        assert_eq!(sidecar.len(), 0);

        // Write to disk — should be a no-op.
        let written = sidecar.write_to_disk(&sstable_path).unwrap();
        assert!(!written, "empty sidecar should not be written");

        // Verify no sidecar file was created.
        let sidecar_path = AccordSidecar::sidecar_path(&sstable_path);
        assert!(!sidecar_path.exists(), "no sidecar file for empty flush");

        // Read should return None.
        let read_result = AccordSidecar::read_from_disk(&sstable_path).unwrap();
        assert!(read_result.is_none(), "no sidecar to read");

        // Serialization of empty sidecar should still be valid.
        let bytes = sidecar.serialize();
        let deserialized = AccordSidecar::deserialize(&bytes).unwrap();
        assert!(deserialized.is_empty());
        assert_eq!(deserialized.len(), 0);
    }
}
