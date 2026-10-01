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
//! the startup log, and reported by `/readyz` while it holds anything.
//!
//! # Re-ingest
//!
//! The engine re-ingests a file ([`reingest_file`]) at construction for every
//! table whose schema is registered, and again each time a table is registered
//! afterwards, so the next boot that has a schema heals itself. Frames stream
//! through one at a time. A frame leaves the file only after the tables it
//! touched are flushed to SSTables; frames for tables still unknown are
//! copied to `<file>.partial`, which atomically replaces the file. A crash at
//! any point leaves the original whole, and re-applying a frame rewrites
//! identical cells, so a repeat is harmless. `ferrosa-ctl commitlog set-aside`
//! inspects and applies a file offline.
//!
//! # File format
//!
//! A sequence of frames: `len:u32 BE | crc32(payload):u32 BE | payload`, where
//! the payload is a serialized [`Mutation`]. A torn tail frame (crash while
//! setting aside) is reported by [`read_set_aside_file`] rather than skipped.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::{Error, Result};

use crate::commitlog::mutation::Mutation;

/// Directory under the data dir that holds set-aside files.
pub const SET_ASIDE_DIR: &str = "commitlog-unreplayed";

/// Extension of a set-aside file.
const SET_ASIDE_EXT: &str = "unreplayed";

/// `len:u32 | crc:u32`.
const FRAME_HEADER_BYTES: u64 = 8;

/// Largest frame accepted on read; guards against a corrupt length prefix.
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

static SET_ASIDE_MUTATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-wide count of mutations set aside because no schema was available
/// to replay them. Non-zero means data is on disk but NOT in any memtable.
pub fn replay_set_aside_mutations_total() -> u64 {
    SET_ASIDE_MUTATIONS_TOTAL.load(Ordering::Relaxed)
}

static SET_ASIDE_STALE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-wide count of set-aside frames withheld from a table because they
/// predate its current incarnation (they sit in the quarantine directory).
pub fn replay_set_aside_stale_frames_total() -> u64 {
    SET_ASIDE_STALE_TOTAL.load(Ordering::Relaxed)
}

/// Counts one frame quarantined as stale.
pub fn count_stale_frame() {
    SET_ASIDE_STALE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

static SET_ASIDE_PENDING: AtomicU64 = AtomicU64::new(0);

/// Process-wide count of set-aside mutations not yet re-ingested. Unlike the
/// `_total` counter this falls as re-ingest succeeds; non-zero means rows are
/// on disk but invisible to reads.
pub fn replay_set_aside_pending_mutations() -> u64 {
    SET_ASIDE_PENDING.load(Ordering::Relaxed)
}

/// Records the current pending count (set by the engine after every change).
pub fn set_replay_set_aside_pending_mutations(n: u64) {
    SET_ASIDE_PENDING.store(n, Ordering::Relaxed);
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
    let mut reader = SetAsideReader::open(path)?;
    let mut out = Vec::new();
    while let Some(frame) = reader.next_frame()? {
        out.push(frame.mutation);
    }
    Ok(out)
}

/// One frame read back from a set-aside file.
#[derive(Debug)]
pub struct SetAsideFrame {
    /// Byte offset of the frame header in the file.
    pub offset: u64,
    /// The whole frame (header and payload), for copying into a rewritten file.
    pub raw: Vec<u8>,
    /// The decoded mutation.
    pub mutation: Mutation,
}

/// Streams frames out of a set-aside file one at a time, so a file of any size
/// is read with one frame resident.
pub struct SetAsideReader {
    path: PathBuf,
    file: BufReader<File>,
    pos: u64,
    len: u64,
}

impl SetAsideReader {
    /// Opens `path` for streaming.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Self {
            path: path.to_path_buf(),
            file: BufReader::new(file),
            pos: 0,
            len,
        })
    }

    fn corrupt(&self, what: &str, pos: u64) -> Error {
        Error::InvalidData(format!(
            "set-aside file {} has {what} at byte {pos}",
            self.path.display()
        ))
    }

    /// Next frame, `None` at a clean end of file, or an error naming the byte
    /// offset of a torn or corrupt frame. A torn tail is never skipped.
    pub fn next_frame(&mut self) -> Result<Option<SetAsideFrame>> {
        let pos = self.pos;
        if pos == self.len {
            return Ok(None);
        }
        let remaining = self.len - pos;
        if remaining < FRAME_HEADER_BYTES {
            return Err(self.corrupt("a torn frame header", pos));
        }
        let mut header = [0u8; FRAME_HEADER_BYTES as usize];
        self.file.read_exact(&mut header)?;
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        if len > MAX_FRAME_BYTES {
            return Err(self.corrupt(&format!("an implausible frame length {len}"), pos));
        }
        if len as u64 > remaining - FRAME_HEADER_BYTES {
            return Err(self.corrupt("a torn frame body", pos));
        }
        let mut raw = vec![0u8; FRAME_HEADER_BYTES as usize + len];
        raw[..FRAME_HEADER_BYTES as usize].copy_from_slice(&header);
        self.file
            .read_exact(&mut raw[FRAME_HEADER_BYTES as usize..])?;
        let payload = &raw[FRAME_HEADER_BYTES as usize..];
        if crc32fast::hash(payload) != crc {
            return Err(self.corrupt("a checksum mismatch", pos));
        }
        let mutation = Mutation::deserialize_from(payload).map_err(|e| {
            Error::InvalidData(format!(
                "set-aside file {} frame at byte {pos} does not decode: {e}",
                self.path.display()
            ))
        })?;
        self.pos += raw.len() as u64;
        Ok(Some(SetAsideFrame {
            offset: pos,
            raw,
            mutation,
        }))
    }
}

/// What one set-aside file holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetAsideFileStatus {
    pub path: PathBuf,
    pub mutations: u64,
    pub tables: BTreeMap<String, u64>,
    /// Why the file could not be read to its end, if it could not.
    pub error: Option<String>,
}

/// Everything set aside and not yet re-ingested.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetAsideStatus {
    pub files: Vec<SetAsideFileStatus>,
}

impl SetAsideStatus {
    /// True when no set-aside file exists, so nothing is invisible to reads.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Mutations still waiting to be re-ingested, across all readable files.
    pub fn mutations(&self) -> u64 {
        self.files.iter().map(|f| f.mutations).sum()
    }

    /// Per `keyspace.table` counts across all files.
    pub fn tables(&self) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        for file in &self.files {
            for (table, n) in &file.tables {
                *out.entry(table.clone()).or_insert(0) += n;
            }
        }
        out
    }

    /// Files that could not be read to their end.
    pub fn unreadable(&self) -> Vec<&SetAsideFileStatus> {
        self.files.iter().filter(|f| f.error.is_some()).collect()
    }
}

/// The engine's record of what is set aside.
///
/// This is the one place the engine learns about set-aside files, and it is
/// the enforcement point for adoption: `StorageEngine` holds one as a field, it
/// has no `Default`, and [`adopt`](Self::adopt) is its only constructor. A
/// constructor of `StorageEngine` cannot build the struct without scanning the
/// data directory, so a future constructor that forgets set-aside files does
/// not compile, rather than quietly serving empty tables (the failure mode of
/// PR #468's evicted-SSTable restore, which was wired into `new` but not `open`).
#[derive(Debug)]
pub struct SetAsideLedger {
    status: parking_lot::Mutex<SetAsideStatus>,
}

impl SetAsideLedger {
    /// Scans `data_dir`, reports what is set aside (ERROR per unreadable file,
    /// ERROR for the pending total), and publishes the pending-mutations gauge.
    pub fn adopt(data_dir: &Path) -> Result<Self> {
        let status = scan_set_aside_dir(data_dir)?;
        for file in status.unreadable() {
            tracing::error!(
                path = %file.path.display(),
                error = file.error.as_deref().unwrap_or_default(),
                "set-aside file cannot be read to its end; it is kept untouched and its \
                 mutations are NOT re-ingested. Inspect it with \
                 `ferrosa-ctl commitlog set-aside`"
            );
        }
        if !status.is_empty() {
            tracing::error!(
                files = status.files.len(),
                mutations = status.mutations(),
                tables = ?status.tables(),
                "mutations are set aside on disk and invisible to reads until their table \
                 schema is registered; /readyz reports this node as not ready"
            );
        }
        set_replay_set_aside_pending_mutations(status.mutations());
        Ok(Self {
            status: parking_lot::Mutex::new(status),
        })
    }

    /// The current status, locked for the caller.
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, SetAsideStatus> {
        self.status.lock()
    }

    /// Replaces the status and the pending gauge together.
    pub fn publish(&self, status: SetAsideStatus) {
        set_replay_set_aside_pending_mutations(status.mutations());
        *self.status.lock() = status;
    }
}

/// Counts what a file holds by streaming it. A torn or corrupt frame is
/// recorded in `error`; the frames before it are still counted.
pub fn summarize_set_aside_file(path: &Path) -> SetAsideFileStatus {
    let mut status = SetAsideFileStatus {
        path: path.to_path_buf(),
        ..SetAsideFileStatus::default()
    };
    let mut reader = match SetAsideReader::open(path) {
        Ok(reader) => reader,
        Err(e) => {
            status.error = Some(e.to_string());
            return status;
        }
    };
    loop {
        match reader.next_frame() {
            Ok(Some(frame)) => {
                status.mutations += 1;
                *status.tables.entry(table_key(&frame.mutation)).or_insert(0) += 1;
            }
            Ok(None) => return status,
            Err(e) => {
                status.error = Some(e.to_string());
                return status;
            }
        }
    }
}

fn table_key(mutation: &Mutation) -> String {
    format!("{}.{}", mutation.keyspace, mutation.table)
}

/// Every `*.unreplayed` file under `data_dir`, oldest name first.
pub fn list_set_aside_files(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let dir = data_dir.join(SET_ASIDE_DIR);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == SET_ASIDE_EXT) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Lists and summarizes every set-aside file under `data_dir`.
pub fn scan_set_aside_dir(data_dir: &Path) -> Result<SetAsideStatus> {
    let files = list_set_aside_files(data_dir)?
        .iter()
        .map(|path| summarize_set_aside_file(path))
        .collect();
    Ok(SetAsideStatus { files })
}

/// What a [`SetAsideSink`] decided for one frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFate {
    /// Applied to a table; the frame leaves the file once the table is durable.
    Applied,
    /// Stays in the file (its schema is still unknown, or the run is scoped to
    /// another table).
    Keep,
    /// Belongs to a previous incarnation of its table. It is NOT applied; it
    /// is copied to the quarantine file and leaves the set-aside file only once
    /// that copy is durable. Never dropped, never applied.
    Stale,
}

/// Directory under the data dir that holds frames withheld as stale.
pub const STALE_QUARANTINE_DIR: &str = "commitlog-quarantine";

/// Where the stale frames of the set-aside file at `path` are quarantined:
/// `<data_dir>/commitlog-quarantine/<file name>.stale`.
pub fn stale_quarantine_path(path: &Path) -> Result<PathBuf> {
    let data_dir = path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::InvalidData(format!("{} has no data dir", path.display())))?;
    let name = path
        .file_name()
        .ok_or_else(|| Error::InvalidData(format!("{} has no file name", path.display())))?;
    let mut file = name.to_os_string();
    file.push(".stale");
    Ok(data_dir.join(STALE_QUARANTINE_DIR).join(file))
}

/// Receives frames during [`reingest_file`].
pub trait SetAsideSink {
    /// Decides one mutation's fate.
    fn apply(&mut self, index: usize, mutation: &Mutation) -> Result<FrameFate>;
    /// Makes everything applied so far durable.
    fn make_durable(&mut self) -> Result<()>;
}

/// Result of [`reingest_file`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileOutcome {
    pub applied: u64,
    pub kept: u64,
    pub kept_tables: BTreeMap<String, u64>,
    /// Frames of a previous table incarnation, moved to the quarantine file.
    pub stale: u64,
    pub stale_tables: BTreeMap<String, u64>,
    pub removed: bool,
}

impl FileOutcome {
    /// Frames that left the file, applied or quarantined.
    pub fn departed(&self) -> u64 {
        self.applied + self.stale
    }
}

/// Applied frames between `make_durable` calls, so a huge file does not pile
/// unflushed rows into memtables.
const DURABLE_EVERY: u64 = 50_000;

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Streams `path` into `sink`, then drops the applied frames from the file.
///
/// Crash safety: the file is never modified until `sink.make_durable()` has
/// succeeded for everything applied. Frames the sink declines are copied to
/// `<file>.partial`, which replaces the original by atomic rename (or the
/// original is removed when nothing was declined). A crash or error at any
/// earlier point leaves the original whole, so the next run re-applies the same
/// frames; that is safe because re-applying a mutation rewrites identical cells
/// with identical timestamps. An unreadable frame stops the run with an error
/// and keeps the file.
pub fn reingest_file(path: &Path, sink: &mut dyn SetAsideSink) -> Result<FileOutcome> {
    let mut reader = SetAsideReader::open(path)?;
    let mut run = ReingestRun {
        partial: path.with_extension("partial"),
        kept_writer: None,
        stale_writer: None,
        outcome: FileOutcome::default(),
    };
    let mut since_durable = 0u64;
    let mut index = 0usize;
    while let Some(frame) = reader.next_frame()? {
        match sink.apply(index, &frame.mutation)? {
            FrameFate::Applied => {
                run.outcome.applied += 1;
                since_durable += 1;
                if since_durable >= DURABLE_EVERY {
                    sink.make_durable()?;
                    since_durable = 0;
                }
            }
            FrameFate::Keep => run.keep(&frame)?,
            FrameFate::Stale => run.quarantine(path, &frame)?,
        }
        index += 1;
    }
    finish_file(path, run, sink)
}

/// The two side files a re-ingest run may write, and its tally.
struct ReingestRun {
    partial: PathBuf,
    kept_writer: Option<BufWriter<File>>,
    stale_writer: Option<(BufWriter<File>, PathBuf)>,
    outcome: FileOutcome,
}

impl ReingestRun {
    fn keep(&mut self, frame: &SetAsideFrame) -> Result<()> {
        if self.kept_writer.is_none() {
            self.kept_writer = Some(BufWriter::new(File::create(&self.partial)?));
        }
        if let Some(writer) = self.kept_writer.as_mut() {
            writer.write_all(&frame.raw)?;
        }
        self.outcome.kept += 1;
        *self
            .outcome
            .kept_tables
            .entry(table_key(&frame.mutation))
            .or_insert(0) += 1;
        Ok(())
    }

    /// Appends the frame to the quarantine file. Appending (not truncating)
    /// keeps frames an earlier run already moved there; a rerun after a crash
    /// may repeat a frame, which is harmless for a file an operator reads.
    fn quarantine(&mut self, source: &Path, frame: &SetAsideFrame) -> Result<()> {
        if self.stale_writer.is_none() {
            let target = stale_quarantine_path(source)?;
            if let Some(dir) = target.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(&target)?;
            self.stale_writer = Some((BufWriter::new(file), target));
        }
        if let Some((writer, _)) = self.stale_writer.as_mut() {
            writer.write_all(&frame.raw)?;
        }
        self.outcome.stale += 1;
        *self
            .outcome
            .stale_tables
            .entry(table_key(&frame.mutation))
            .or_insert(0) += 1;
        Ok(())
    }

    /// Flushes and fsyncs the quarantine file and its directory. Must complete
    /// before any stale frame leaves the set-aside file.
    fn make_quarantine_durable(&mut self) -> Result<()> {
        if let Some((writer, target)) = self.stale_writer.take() {
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            if let Some(dir) = target.parent() {
                sync_dir(dir)?;
            }
        }
        Ok(())
    }
}

fn finish_file(
    path: &Path,
    mut run: ReingestRun,
    sink: &mut dyn SetAsideSink,
) -> Result<FileOutcome> {
    if run.outcome.departed() == 0 {
        // Nothing changed; the original stays byte for byte.
        if run.kept_writer.take().is_some() {
            std::fs::remove_file(&run.partial)?;
        }
        return Ok(run.outcome);
    }
    sink.make_durable()?;
    run.make_quarantine_durable()?;
    let dir = path.parent().ok_or_else(|| {
        Error::InvalidData(format!("set-aside file {} has no parent", path.display()))
    })?;
    match run.kept_writer.take() {
        None => {
            std::fs::remove_file(path)?;
            sync_dir(dir)?;
            run.outcome.removed = true;
        }
        Some(writer) => {
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            std::fs::rename(&run.partial, path)?;
            sync_dir(dir)?;
        }
    }
    Ok(run.outcome)
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

    fn aside_file(dir: &Path, muts: &[Mutation]) -> PathBuf {
        let mut aside = ReplaySetAside::new(dir);
        for m in muts {
            aside.append(m).unwrap();
        }
        aside.sync().unwrap();
        aside.into_report().unwrap().path
    }

    /// Applies only the tables in `known`; can be told to fail at a frame.
    struct RecordingSink {
        known: Vec<String>,
        stale: Vec<String>,
        durable_calls: usize,
        fail_at: Option<usize>,
    }

    impl RecordingSink {
        fn new(known: &[&str]) -> Self {
            Self {
                known: known.iter().map(|s| (*s).to_string()).collect(),
                stale: Vec::new(),
                durable_calls: 0,
                fail_at: None,
            }
        }
    }

    impl SetAsideSink for RecordingSink {
        fn apply(&mut self, index: usize, m: &Mutation) -> Result<FrameFate> {
            if self.fail_at == Some(index) {
                return Err(Error::InvalidData("injected".into()));
            }
            if self.stale.contains(&m.table) {
                return Ok(FrameFate::Stale);
            }
            Ok(if self.known.contains(&m.table) {
                FrameFate::Applied
            } else {
                FrameFate::Keep
            })
        }
        fn make_durable(&mut self) -> Result<()> {
            self.durable_calls += 1;
            Ok(())
        }
    }

    #[test]
    fn reader_streams_frames_with_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1"), mutation("b", "k2")]);
        let mut reader = SetAsideReader::open(&path).unwrap();
        let first = reader.next_frame().unwrap().unwrap();
        assert_eq!(first.offset, 0);
        assert_eq!(first.mutation.table, "a");
        let second = reader.next_frame().unwrap().unwrap();
        assert_eq!(second.offset, first.raw.len() as u64);
        assert!(reader.next_frame().unwrap().is_none());
    }

    #[test]
    fn reader_reports_a_torn_tail_as_an_error_after_the_good_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1"), mutation("a", "k2")]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, bytes).unwrap();
        let mut reader = SetAsideReader::open(&path).unwrap();
        assert!(reader.next_frame().unwrap().is_some());
        let err = reader.next_frame().unwrap_err().to_string();
        assert!(err.contains("torn frame"), "got: {err}");
    }

    #[test]
    fn reader_reports_a_flipped_byte_as_a_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1")]);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let err = SetAsideReader::open(&path)
            .unwrap()
            .next_frame()
            .unwrap_err()
            .to_string();
        assert!(err.contains("checksum mismatch"), "got: {err}");
    }

    #[test]
    fn scan_counts_files_and_tables_and_flags_a_torn_file() {
        let dir = tempfile::tempdir().unwrap();
        let good = aside_file(dir.path(), &[mutation("a", "k1"), mutation("b", "k2")]);
        // A second file needs a distinct name: the stamp has millisecond grain.
        let torn = dir.path().join(SET_ASIDE_DIR).join("0-torn.unreplayed");
        let mut bytes = std::fs::read(&good).unwrap();
        bytes.truncate(bytes.len() - 2);
        std::fs::write(&torn, bytes).unwrap();
        let status = scan_set_aside_dir(dir.path()).unwrap();
        assert_eq!(status.files.len(), 2);
        assert!(!status.is_empty());
        let torn_status = status.files.iter().find(|f| f.path == torn).unwrap();
        assert!(torn_status.error.as_deref().unwrap().contains("torn frame"));
        let good_status = status.files.iter().find(|f| f.path == good).unwrap();
        assert_eq!(good_status.mutations, 2);
        assert!(good_status.error.is_none());
    }

    #[test]
    fn scan_of_a_dir_with_no_set_aside_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let status = scan_set_aside_dir(dir.path()).unwrap();
        assert!(status.is_empty());
        assert_eq!(status.mutations(), 0);
    }

    #[test]
    fn reingest_removes_a_fully_applied_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1"), mutation("a", "k2")]);
        let mut sink = RecordingSink::new(&["a"]);
        let outcome = reingest_file(&path, &mut sink).unwrap();
        assert_eq!(outcome.applied, 2);
        assert!(outcome.removed);
        assert!(
            !path.exists(),
            "the file is gone once everything is applied"
        );
        assert!(
            sink.durable_calls >= 1,
            "durable before the file is removed"
        );
    }

    #[test]
    fn reingest_keeps_only_the_frames_whose_schema_is_still_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(
            dir.path(),
            &[
                mutation("a", "k1"),
                mutation("b", "k2"),
                mutation("a", "k3"),
            ],
        );
        let mut sink = RecordingSink::new(&["a"]);
        let outcome = reingest_file(&path, &mut sink).unwrap();
        assert_eq!(outcome.applied, 2);
        assert_eq!(outcome.kept, 1);
        assert_eq!(outcome.kept_tables.get("ks.b"), Some(&1));
        assert!(!outcome.removed);
        let left = read_set_aside_file(&path).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].table, "b");
    }

    #[test]
    fn reingest_with_nothing_known_leaves_the_file_byte_identical() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1")]);
        let before = std::fs::read(&path).unwrap();
        let outcome = reingest_file(&path, &mut RecordingSink::new(&[])).unwrap();
        assert_eq!(outcome.applied, 0);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn a_failure_mid_file_leaves_the_file_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(
            dir.path(),
            &[
                mutation("a", "k1"),
                mutation("a", "k2"),
                mutation("a", "k3"),
            ],
        );
        let before = std::fs::read(&path).unwrap();
        let mut sink = RecordingSink::new(&["a"]);
        sink.fail_at = Some(2);
        let err = reingest_file(&path, &mut sink).unwrap_err().to_string();
        assert!(err.contains("injected"), "got: {err}");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "no frame is dropped until its mutation is durable"
        );
    }

    #[test]
    fn a_torn_frame_stops_reingest_and_keeps_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("a", "k1"), mutation("a", "k2")]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, &bytes).unwrap();
        let err = reingest_file(&path, &mut RecordingSink::new(&["a"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("torn frame"), "got: {err}");
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn stale_frames_move_to_quarantine_and_leave_the_file_only_after_it_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(
            dir.path(),
            &[
                mutation("a", "k1"),
                mutation("old", "k2"),
                mutation("a", "k3"),
                mutation("old", "k4"),
            ],
        );
        let mut sink = RecordingSink::new(&["a"]);
        sink.stale.push("old".into());
        let outcome = reingest_file(&path, &mut sink).unwrap();
        assert_eq!(outcome.applied, 2);
        assert_eq!(outcome.stale, 2);
        assert_eq!(outcome.stale_tables.get("ks.old"), Some(&2));
        assert!(outcome.removed, "every frame left: applied or quarantined");
        assert!(!path.exists());

        let quarantine = stale_quarantine_path(&path).unwrap();
        let held = read_set_aside_file(&quarantine).unwrap();
        assert_eq!(held.len(), 2, "stale frames are kept, never dropped");
        assert!(held.iter().all(|m| m.table == "old"));
    }

    #[test]
    fn a_file_of_only_stale_frames_is_quarantined_even_with_nothing_applied() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("old", "k1")]);
        let mut sink = RecordingSink::new(&[]);
        sink.stale.push("old".into());
        let outcome = reingest_file(&path, &mut sink).unwrap();
        assert_eq!((outcome.applied, outcome.stale), (0, 1));
        assert!(!path.exists());
        assert_eq!(
            read_set_aside_file(&stale_quarantine_path(&path).unwrap())
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn quarantined_frames_accumulate_across_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = aside_file(dir.path(), &[mutation("old", "k1")]);
        let quarantine = stale_quarantine_path(&path).unwrap();
        let mut sink = RecordingSink::new(&[]);
        sink.stale.push("old".into());
        reingest_file(&path, &mut sink).unwrap();
        // A later file under the same name: the quarantine path follows the name.
        let second = aside_file(dir.path(), &[mutation("old", "k2")]);
        if second != path {
            std::fs::rename(&second, &path).unwrap();
        }
        reingest_file(&path, &mut sink).unwrap();
        assert_eq!(read_set_aside_file(&quarantine).unwrap().len(), 2);
    }
}
