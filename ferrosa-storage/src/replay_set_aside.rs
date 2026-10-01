//! Durable set-aside of commit-log mutations that startup replay cannot bind
//! to a table schema.
//!
//! Replay normally streams mutations into registered tables. When no schema is
//! available yet (both `schema.json` and `storage-schema.json` missing or
//! empty), mutations are buffered in memory up to
//! `max_pending_replay_mutations_without_schema`. Past that bound the engine
//! used to refuse to open, which bricked a local-only node whose log had grown
//! past the bound (t_2db96eb9).
//!
//! The overflow is instead written here, fsynced before the commit-log segment
//! that carried it is deleted, so no acknowledged mutation is lost: it stays on
//! disk under `<data_dir>/commitlog-unreplayed/`, is counted, and is named in
//! the startup log. Nothing reads it back automatically; an operator (or a
//! later recovery tool) re-ingests it once the schema is restored.
//!
//! # File format
//!
//! A sequence of frames: `len:u32 BE | crc32(payload):u32 BE | payload`, where
//! the payload is a serialized [`Mutation`]. A torn tail frame (crash while
//! setting aside) is reported by [`read_set_aside_file`] rather than skipped.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::{Error, Result};

use crate::commitlog::mutation::Mutation;

/// Directory under the data dir that holds set-aside files.
pub const SET_ASIDE_DIR: &str = "commitlog-unreplayed";

/// Extension of a set-aside file.
const SET_ASIDE_EXT: &str = "unreplayed";

/// Largest frame accepted on read; guards against a corrupt length prefix.
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

static SET_ASIDE_MUTATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-wide count of mutations set aside because no schema was available
/// to replay them. Non-zero means data is on disk but NOT in any memtable.
pub fn replay_set_aside_mutations_total() -> u64 {
    SET_ASIDE_MUTATIONS_TOTAL.load(Ordering::Relaxed)
}

/// What a startup replay set aside. Kept on the engine so status endpoints and
/// tests can report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySetAsideReport {
    /// The file the mutations were written to.
    pub path: PathBuf,
    /// Total mutations set aside.
    pub mutations: u64,
    /// Mutations per `keyspace.table` id.
    pub tables: BTreeMap<String, u64>,
}

/// Appends mutations to one set-aside file for the duration of a replay.
pub struct ReplaySetAside {
    dir: PathBuf,
    file: Option<(File, PathBuf)>,
    mutations: u64,
    tables: BTreeMap<String, u64>,
    dirty: bool,
}

impl ReplaySetAside {
    /// Creates a set-aside that will write under `<data_dir>/commitlog-unreplayed/`.
    /// No file is created until the first [`append`](Self::append).
    pub fn new(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.join(SET_ASIDE_DIR),
            file: None,
            mutations: 0,
            tables: BTreeMap::new(),
            dirty: false,
        }
    }

    /// Number of mutations set aside so far.
    pub fn count(&self) -> u64 {
        self.mutations
    }

    fn open_file(&mut self) -> Result<&mut File> {
        if self.file.is_none() {
            std::fs::create_dir_all(&self.dir)?;
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis());
            let path = self
                .dir
                .join(format!("{stamp}-{}.{SET_ASIDE_EXT}", std::process::id()));
            let file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)?;
            self.file = Some((file, path));
        }
        match self.file.as_mut() {
            Some((file, _)) => Ok(file),
            None => Err(Error::InvalidData(
                "replay set-aside file missing after open".into(),
            )),
        }
    }

    /// Writes one mutation as a frame. Durable only after [`sync`](Self::sync).
    pub fn append(&mut self, mutation: &Mutation) -> Result<()> {
        let mut payload = vec![0u8; mutation.serialized_size()];
        mutation.serialize_into(&mut payload);
        let len = u32::try_from(payload.len()).map_err(|_| {
            Error::InvalidData(format!(
                "mutation of {} bytes is too large to set aside",
                payload.len()
            ))
        })?;
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);
        self.open_file()?.write_all(&frame)?;
        self.mutations += 1;
        self.dirty = true;
        *self
            .tables
            .entry(format!("{}.{}", mutation.keyspace, mutation.table))
            .or_insert(0) += 1;
        SET_ASIDE_MUTATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Makes everything appended so far durable (file and directory entry).
    /// Must complete before the commit-log segment that carried the mutations
    /// is deleted.
    pub fn sync(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let Some((file, _)) = self.file.as_ref() else {
            return Err(Error::InvalidData(
                "replay set-aside is dirty but has no file".into(),
            ));
        };
        file.sync_all()?;
        File::open(&self.dir)?.sync_all()?;
        self.dirty = false;
        Ok(())
    }

    /// Consumes the set-aside, returning what it holds, or `None` if nothing
    /// was set aside.
    pub fn into_report(self) -> Option<ReplaySetAsideReport> {
        let (_, path) = self.file?;
        Some(ReplaySetAsideReport {
            path,
            mutations: self.mutations,
            tables: self.tables,
        })
    }
}

/// Reads every mutation from a set-aside file. A torn or corrupt frame is an
/// error naming the byte offset, never a silent truncation.
pub fn read_set_aside_file(path: &Path) -> Result<Vec<Mutation>> {
    let bytes = std::fs::read(path)?;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let header = bytes.get(pos..pos + 8).ok_or_else(|| {
            Error::InvalidData(format!(
                "set-aside file {} has a torn frame header at byte {pos}",
                path.display()
            ))
        })?;
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        if len > MAX_FRAME_BYTES {
            return Err(Error::InvalidData(format!(
                "set-aside file {} has an implausible frame length {len} at byte {pos}",
                path.display()
            )));
        }
        let payload = bytes.get(pos + 8..pos + 8 + len).ok_or_else(|| {
            Error::InvalidData(format!(
                "set-aside file {} has a torn frame body at byte {pos}",
                path.display()
            ))
        })?;
        if crc32fast::hash(payload) != crc {
            return Err(Error::InvalidData(format!(
                "set-aside file {} has a checksum mismatch at byte {pos}",
                path.display()
            )));
        }
        let mutation = Mutation::deserialize_from(payload).map_err(|e| {
            Error::InvalidData(format!(
                "set-aside file {} frame at byte {pos} does not decode: {e}",
                path.display()
            ))
        })?;
        out.push(mutation);
        pos += 8 + len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{DecoratedKey, PartitionKey};

    fn mutation(table: &str, key: &str) -> Mutation {
        let key = DecoratedKey::new(PartitionKey::new(key.as_bytes().to_vec()));
        Mutation::new("ks".into(), table.into(), key, vec![], 7)
    }

    #[test]
    fn round_trips_and_counts_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut aside = ReplaySetAside::new(dir.path());
        aside.append(&mutation("a", "k1")).unwrap();
        aside.append(&mutation("a", "k2")).unwrap();
        aside.append(&mutation("b", "k3")).unwrap();
        aside.sync().unwrap();
        let report = aside.into_report().expect("three mutations set aside");
        assert_eq!(report.mutations, 3);
        assert_eq!(report.tables.get("ks.a"), Some(&2));
        assert_eq!(report.tables.get("ks.b"), Some(&1));
        let back = read_set_aside_file(&report.path).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back[2].table, "b");
    }

    #[test]
    fn nothing_appended_creates_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let aside = ReplaySetAside::new(dir.path());
        assert!(aside.into_report().is_none());
        assert!(!dir.path().join(SET_ASIDE_DIR).exists());
    }

    #[test]
    fn torn_tail_is_an_error_not_a_silent_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let mut aside = ReplaySetAside::new(dir.path());
        aside.append(&mutation("a", "k1")).unwrap();
        aside.sync().unwrap();
        let path = aside.into_report().unwrap().path;
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, bytes).unwrap();
        let err = read_set_aside_file(&path).unwrap_err().to_string();
        assert!(err.contains("torn frame"), "got: {err}");
    }
}
