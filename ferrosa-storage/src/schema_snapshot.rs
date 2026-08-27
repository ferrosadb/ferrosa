//! Crash-safe local persistence for the authoritative schema registry snapshot.
//!
//! `schema.json` has exactly one format and one write implementation. Callers
//! may request a persist, but this store owns locking, serialization, retained
//! generations, verification, and atomic publication.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use ferrosa_common::{Error, Result};
use ferrosa_schema::SchemaSnapshot;
use fs2::FileExt;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const SNAPSHOT_FILE: &str = "schema.json";
const LOCK_FILE: &str = ".schema.json.lock";
const STAGE_PREFIX: &str = ".schema.json.staging-";
const GENERATION_PREFIX: &str = "schema.json.generation-";
const FORMAT: &str = "ferrosa-schema-snapshot";
const FORMAT_VERSION: u32 = 1;
const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const RETAINED_GENERATIONS: usize = 3;

struct BoundedWriter<W> {
    inner: W,
    written: u64,
    max_bytes: u64,
}

impl<W> BoundedWriter<W> {
    fn new(inner: W, max_bytes: u64) -> Self {
        Self {
            inner,
            written: 0,
            max_bytes,
        }
    }
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next = self.written.saturating_add(buffer.len() as u64);
        if next > self.max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "serialized JSON exceeds the {} byte durability bound",
                    self.max_bytes
                ),
            ));
        }
        let count = self.inner.write(buffer)?;
        self.written = self.written.saturating_add(count as u64);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotDocument {
    format: String,
    format_version: u32,
    snapshot: SchemaSnapshot,
}

#[derive(Serialize)]
struct SnapshotDocumentRef<'a> {
    format: &'static str,
    format_version: u32,
    snapshot: &'a SchemaSnapshot,
}

impl<'a> SnapshotDocumentRef<'a> {
    fn new(snapshot: &'a SchemaSnapshot) -> Self {
        Self {
            format: FORMAT,
            format_version: FORMAT_VERSION,
            snapshot,
        }
    }
}

impl SnapshotDocument {
    fn into_snapshot(self) -> Result<SchemaSnapshot> {
        if self.format != FORMAT {
            return Err(Error::InvalidFormat(format!(
                "unsupported schema snapshot format {:?}; expected {:?}",
                self.format, FORMAT
            )));
        }
        if self.format_version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(format!(
                "schema snapshot format version {}; supported version is {}",
                self.format_version, FORMAT_VERSION
            )));
        }
        Ok(self.snapshot)
    }
}

/// The only component allowed to publish the registry-owned `schema.json`.
#[derive(Debug, Clone)]
pub struct SchemaSnapshotStore {
    data_dir: PathBuf,
    max_bytes: u64,
}

impl SchemaSnapshotStore {
    /// Open the store rooted at a node's data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }

    /// Load the authoritative snapshot without buffering the JSON file.
    ///
    /// A legacy storage-only array or malformed/unknown document is moved to
    /// an evidence-preserving quarantine path and returned as an error. It is
    /// never treated as an empty schema.
    pub fn load(&self) -> Result<Option<SchemaSnapshot>> {
        if !self.data_dir.exists() {
            return Ok(None);
        }
        let lock = self.lock()?;
        self.cleanup_staging()?;
        let result = self.load_live_unlocked();
        FileExt::unlock(&lock)?;
        result
    }

    /// Persist a snapshot using stage, flush, fsync, parse verification,
    /// retained generation, atomic rename, and directory fsync.
    pub fn persist(&self, snapshot: &SchemaSnapshot) -> Result<()> {
        self.persist_inner(snapshot, None)
    }

    fn persist_inner(
        &self,
        snapshot: &SchemaSnapshot,
        #[cfg_attr(not(test), allow(unused_variables))] crash: Option<CrashPoint>,
    ) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        let lock = self.lock()?;
        self.cleanup_staging()?;

        // Never replace unreadable evidence. `load_live_unlocked` quarantines
        // it and returns a loud error before a stage is created.
        if self.live_path().exists() {
            self.load_live_unlocked()?;
        }

        let stage = self
            .data_dir
            .join(format!("{STAGE_PREFIX}{}", Uuid::new_v4()));
        let stage_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stage)?;
        crash_at(crash, CrashPoint::StageCreated)?;

        {
            let mut writer = BufWriter::new(BoundedWriter::new(stage_file, self.max_bytes));
            serde_json::to_writer_pretty(&mut writer, &SnapshotDocumentRef::new(snapshot))
                .map_err(json_error("serialize staged schema snapshot"))?;
            writer.flush()?;
            writer.get_ref().inner.sync_all()?;
        }
        crash_at(crash, CrashPoint::StageSynced)?;

        let verified = self.read_snapshot(&stage)?;
        if verified.version != snapshot.version {
            return Err(Error::InvalidData(format!(
                "staged schema snapshot version mismatch: wrote {}, read {}",
                snapshot.version, verified.version
            )));
        }
        crash_at(crash, CrashPoint::Verified)?;

        self.rotate_generations()?;
        std::fs::rename(&stage, self.live_path())?;
        crash_at(crash, CrashPoint::Renamed)?;

        self.sync_directory()?;
        crash_at(crash, CrashPoint::DirectorySynced)?;
        FileExt::unlock(&lock)?;
        Ok(())
    }

    fn load_live_unlocked(&self) -> Result<Option<SchemaSnapshot>> {
        let path = self.live_path();
        if !path.exists() {
            return Ok(None);
        }
        match self.read_snapshot(&path) {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(error) => {
                let quarantined = self.quarantine_live()?;
                Err(Error::InvalidFormat(format!(
                    "refusing to start with unreadable schema snapshot {}; preserved at {}: {}",
                    path.display(),
                    quarantined.display(),
                    error
                )))
            }
        }
    }

    fn read_snapshot(&self, path: &Path) -> Result<SchemaSnapshot> {
        let metadata = std::fs::metadata(path)?;
        if metadata.len() > self.max_bytes {
            return Err(Error::InvalidFormat(format!(
                "schema snapshot is {} bytes; maximum is {}",
                metadata.len(),
                self.max_bytes
            )));
        }

        let first = first_non_whitespace(path, self.max_bytes)?;
        if first == Some(b'[') {
            return Err(Error::InvalidFormat(
                "legacy storage TableSchema array cannot restore the schema registry".to_owned(),
            ));
        }
        if first != Some(b'{') {
            return Err(Error::InvalidFormat(
                "schema snapshot must be a JSON object".to_owned(),
            ));
        }

        let document_result = deserialize_bounded::<SnapshotDocument>(path, self.max_bytes);
        match document_result {
            Ok(document) => document.into_snapshot(),
            Err(document_error) => {
                // Compatibility with the pre-discriminator registry object.
                deserialize_bounded::<SchemaSnapshot>(path, self.max_bytes).map_err(|legacy_error| {
                    Error::InvalidFormat(format!(
                        "document parse failed: {document_error}; legacy object parse failed: {legacy_error}"
                    ))
                })
            }
        }
    }

    fn rotate_generations(&self) -> Result<()> {
        let live = self.live_path();
        if !live.exists() {
            return Ok(());
        }
        for slot in (2..=RETAINED_GENERATIONS).rev() {
            let source = self.generation_path(slot - 1);
            if source.exists() {
                std::fs::rename(source, self.generation_path(slot))?;
            }
        }
        std::fs::hard_link(live, self.generation_path(1))?;
        Ok(())
    }

    fn cleanup_staging(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(STAGE_PREFIX) {
                let file_type = entry.file_type()?;
                if file_type.is_file() {
                    std::fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
    }

    fn quarantine_live(&self) -> Result<PathBuf> {
        let quarantined = self
            .data_dir
            .join(format!("schema.json.unparseable-{}", Uuid::new_v4()));
        std::fs::rename(self.live_path(), &quarantined)?;
        self.sync_directory()?;
        Ok(quarantined)
    }

    fn lock(&self) -> Result<File> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.data_dir.join(LOCK_FILE))?;
        lock.lock_exclusive()?;
        Ok(lock)
    }

    fn sync_directory(&self) -> Result<()> {
        File::open(&self.data_dir)?.sync_all()?;
        Ok(())
    }

    fn live_path(&self) -> PathBuf {
        self.data_dir.join(SNAPSHOT_FILE)
    }

    fn generation_path(&self, slot: usize) -> PathBuf {
        self.data_dir.join(format!("{GENERATION_PREFIX}{slot}"))
    }
}

/// Atomically persist a storage-private JSON document without constructing an
/// intermediate byte buffer. The filename is crate-controlled, not user input.
pub(crate) fn persist_bounded_json<T: Serialize>(
    data_dir: &Path,
    filename: &str,
    value: &T,
) -> Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let lock_path = data_dir.join(format!(".{filename}.lock"));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock.lock_exclusive()?;

    let stage_prefix = format!(".{filename}.staging-");
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(&stage_prefix)
            && entry.file_type()?.is_file()
        {
            std::fs::remove_file(entry.path())?;
        }
    }

    let stage = data_dir.join(format!("{stage_prefix}{}", Uuid::new_v4()));
    let stage_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stage)?;
    {
        let mut writer = BufWriter::new(BoundedWriter::new(stage_file, DEFAULT_MAX_BYTES));
        serde_json::to_writer_pretty(&mut writer, value)
            .map_err(json_error("serialize storage schema"))?;
        writer.flush()?;
        writer.get_ref().inner.sync_all()?;
    }

    let _: IgnoredAny = deserialize_bounded(&stage, DEFAULT_MAX_BYTES)?;
    std::fs::rename(&stage, data_dir.join(filename))?;
    File::open(data_dir)?.sync_all()?;
    FileExt::unlock(&lock)?;
    Ok(())
}

/// Load a storage-private JSON document under the same hard byte bound used by
/// authoritative snapshots.
pub(crate) fn load_bounded_json<T: for<'de> Deserialize<'de>>(
    data_dir: &Path,
    filename: &str,
) -> Result<Option<T>> {
    let path = data_dir.join(filename);
    if !path.exists() {
        return Ok(None);
    }
    let metadata = std::fs::metadata(&path)?;
    if metadata.len() > DEFAULT_MAX_BYTES {
        return Err(Error::InvalidFormat(format!(
            "{} is {} bytes; maximum is {}",
            path.display(),
            metadata.len(),
            DEFAULT_MAX_BYTES
        )));
    }
    deserialize_bounded(&path, DEFAULT_MAX_BYTES).map(Some)
}

fn deserialize_bounded<T: for<'de> Deserialize<'de>>(path: &Path, max_bytes: u64) -> Result<T> {
    let file = File::open(path)?;
    let reader = BufReader::new(file).take(max_bytes.saturating_add(1));
    serde_json::from_reader(reader).map_err(json_error("deserialize schema snapshot"))
}

fn first_non_whitespace(path: &Path, max_bytes: u64) -> Result<Option<u8>> {
    let file = File::open(path)?;
    for byte in BufReader::new(file)
        .take(max_bytes.saturating_add(1))
        .bytes()
    {
        let byte = byte?;
        if !byte.is_ascii_whitespace() {
            return Ok(Some(byte));
        }
    }
    Ok(None)
}

fn json_error(context: &'static str) -> impl FnOnce(serde_json::Error) -> Error {
    move |error| Error::InvalidFormat(format!("{context}: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrashPoint {
    StageCreated,
    StageSynced,
    Verified,
    Renamed,
    DirectorySynced,
}

fn crash_at(requested: Option<CrashPoint>, current: CrashPoint) -> Result<()> {
    if requested == Some(current) {
        return Err(Error::InvalidData(format!(
            "simulated crash at {current:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn legacy_object_loads_and_next_persist_adds_discriminator() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaSnapshotStore::new(dir.path());
        let snapshot = SchemaSnapshot::new();
        serde_json::to_writer_pretty(
            File::create(dir.path().join(SNAPSHOT_FILE)).unwrap(),
            &snapshot,
        )
        .unwrap();

        assert_eq!(store.load().unwrap().unwrap().version, snapshot.version);
        store.persist(&snapshot).unwrap();

        let document: SnapshotDocument =
            deserialize_bounded(&dir.path().join(SNAPSHOT_FILE), DEFAULT_MAX_BYTES).unwrap();
        assert_eq!(document.format, FORMAT);
        assert_eq!(document.format_version, FORMAT_VERSION);
    }

    #[test]
    fn legacy_array_is_quarantined_and_fails_loud() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaSnapshotStore::new(dir.path());
        std::fs::write(dir.path().join(SNAPSHOT_FILE), b"[]").unwrap();

        let error = store.load().unwrap_err().to_string();

        assert!(error.contains("refusing to start"));
        assert!(!dir.path().join(SNAPSHOT_FILE).exists());
        assert_eq!(quarantine_count(dir.path()), 1);
    }

    #[test]
    fn corrupt_input_is_preserved_and_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaSnapshotStore::new(dir.path());
        std::fs::write(dir.path().join(SNAPSHOT_FILE), b"{broken").unwrap();

        assert!(store.persist(&SchemaSnapshot::new()).is_err());

        assert!(!dir.path().join(SNAPSHOT_FILE).exists());
        assert_eq!(quarantine_count(dir.path()), 1);
    }

    #[test]
    fn retained_generation_contains_previous_verified_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaSnapshotStore::new(dir.path());
        let first = SchemaSnapshot::new();
        let second = SchemaSnapshot::new();
        store.persist(&first).unwrap();
        store.persist(&second).unwrap();

        let retained = store.read_snapshot(&store.generation_path(1)).unwrap();
        assert_eq!(retained.version, first.version);
        assert_eq!(store.load().unwrap().unwrap().version, second.version);
    }

    #[test]
    fn retained_generation_count_is_fixed() {
        let dir = tempfile::tempdir().unwrap();
        let store = SchemaSnapshotStore::new(dir.path());
        for _ in 0..8 {
            store.persist(&SchemaSnapshot::new()).unwrap();
        }

        let generations = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(GENERATION_PREFIX)
            })
            .count();
        assert_eq!(generations, RETAINED_GENERATIONS);
    }

    #[test]
    fn unknown_discriminator_is_quarantined_and_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let document = SnapshotDocument {
            format: "someone-elses-schema".to_owned(),
            format_version: FORMAT_VERSION,
            snapshot: SchemaSnapshot::new(),
        };
        serde_json::to_writer_pretty(
            File::create(dir.path().join(SNAPSHOT_FILE)).unwrap(),
            &document,
        )
        .unwrap();

        let error = SchemaSnapshotStore::new(dir.path())
            .load()
            .unwrap_err()
            .to_string();

        assert!(error.contains("unsupported schema snapshot format"));
        assert_eq!(quarantine_count(dir.path()), 1);
    }

    #[test]
    fn startup_removes_only_inert_staging_files() {
        let dir = tempfile::tempdir().unwrap();
        let stage_file = dir.path().join(format!("{STAGE_PREFIX}dead-writer"));
        let unrelated = dir.path().join("operator-note.txt");
        std::fs::write(&stage_file, b"partial").unwrap();
        std::fs::write(&unrelated, b"keep").unwrap();

        let loaded = SchemaSnapshotStore::new(dir.path()).load().unwrap();

        assert!(loaded.is_none());
        assert!(!stage_file.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn every_crash_boundary_keeps_a_verified_snapshot_loadable() {
        for point in [
            CrashPoint::StageCreated,
            CrashPoint::StageSynced,
            CrashPoint::Verified,
            CrashPoint::Renamed,
            CrashPoint::DirectorySynced,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = SchemaSnapshotStore::new(dir.path());
            let old = SchemaSnapshot::new();
            let new = SchemaSnapshot::new();
            store.persist(&old).unwrap();

            assert!(store.persist_inner(&new, Some(point)).is_err());

            let loaded = store.load().unwrap().unwrap();
            let expected = match point {
                CrashPoint::StageCreated | CrashPoint::StageSynced | CrashPoint::Verified => {
                    old.version
                }
                CrashPoint::Renamed | CrashPoint::DirectorySynced => new.version,
            };
            assert_eq!(loaded.version, expected, "crash point: {point:?}");
            assert_eq!(staging_count(dir.path()), 0, "crash point: {point:?}");
        }
    }

    #[test]
    fn concurrent_writers_publish_one_complete_discriminated_document() {
        let dir = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let first = SchemaSnapshot::new();
        let second = SchemaSnapshot::new();
        let accepted_versions = [first.version, second.version];

        let first_store = SchemaSnapshotStore::new(dir.path());
        let first_barrier = Arc::clone(&barrier);
        let first_thread = std::thread::spawn(move || {
            first_barrier.wait();
            for _ in 0..16 {
                first_store.persist(&first).unwrap();
            }
        });

        let second_store = SchemaSnapshotStore::new(dir.path());
        let second_barrier = Arc::clone(&barrier);
        let second_thread = std::thread::spawn(move || {
            second_barrier.wait();
            for _ in 0..16 {
                second_store.persist(&second).unwrap();
            }
        });

        barrier.wait();
        first_thread.join().unwrap();
        second_thread.join().unwrap();

        let store = SchemaSnapshotStore::new(dir.path());
        let loaded = store.load().unwrap().unwrap();
        assert!(accepted_versions.contains(&loaded.version));
        assert_eq!(staging_count(dir.path()), 0);
        let document: SnapshotDocument =
            deserialize_bounded(&dir.path().join(SNAPSHOT_FILE), DEFAULT_MAX_BYTES).unwrap();
        assert_eq!(document.format, FORMAT);
        assert_eq!(document.format_version, FORMAT_VERSION);
    }

    #[test]
    fn oversized_snapshot_is_rejected_before_deserialization_and_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(SNAPSHOT_FILE);
        File::create(&path)
            .unwrap()
            .set_len(DEFAULT_MAX_BYTES + 1)
            .unwrap();

        let error = SchemaSnapshotStore::new(dir.path())
            .load()
            .unwrap_err()
            .to_string();

        assert!(error.contains("maximum"));
        assert!(!path.exists());
        assert_eq!(quarantine_count(dir.path()), 1);
    }

    #[test]
    fn bounded_writer_refuses_output_past_its_fixed_limit() {
        let mut writer = BoundedWriter::new(Vec::new(), 4);
        writer.write_all(b"1234").unwrap();
        let error = writer.write_all(b"5").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(writer.inner, b"1234");
    }

    fn quarantine_count(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("schema.json.unparseable-")
            })
            .count()
    }

    fn staging_count(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(STAGE_PREFIX)
            })
            .count()
    }
}
