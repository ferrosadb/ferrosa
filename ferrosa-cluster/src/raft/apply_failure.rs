//! Durable refusal after a failed Raft physical/projection apply.
//! Correctness: a recorded failure (including a partial record) blocks an older startup.
//! Last revised: 2026-11-05
//! Last changed: persist failure context outside the storage write-admission path.

use std::{fs::{File, OpenOptions}, io::{self, Write}, path::{Path, PathBuf}};
use openraft::LogId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FailureRecord {
    version: u32,
    pub(crate) previous_applied: Option<LogId<u64>>,
    pub(crate) failed_from: LogId<u64>,
    pub(crate) failed_through: LogId<u64>,
    pub(crate) reason: String,
}

impl FailureRecord {
    pub(crate) fn new(previous_applied: Option<LogId<u64>>, failed_from: LogId<u64>, failed_through: LogId<u64>, reason: String) -> Self {
        Self {version: 1, previous_applied, failed_from, failed_through, reason}
    }
}

pub(crate) fn marker_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path.with_extension("apply-failure.json")
}

/// Close startup durably before attempting the reason payload. A failed payload
/// write leaves an empty/partial marker, which `check` also refuses. This runs
/// only on failure; successful applies perform no additional fsync.
pub(crate) fn persist(snapshot: &Path, record: &FailureRecord) -> io::Result<()> {
    persist_using(snapshot, record, |file, bytes| file.write_all(bytes))
}

fn persist_using(snapshot: &Path, record: &FailureRecord, write_payload: impl FnOnce(&mut File, &[u8]) -> io::Result<()>) -> io::Result<()> {
    let path = marker_path(snapshot);
    let parent = path.parent().ok_or_else(|| io::Error::other("failure marker has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let mut options = OpenOptions::new(); options.write(true).create_new(true);
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if !std::fs::symlink_metadata(&path)?.file_type().is_file() {
                return Err(io::Error::other("non-regular Raft failure marker"));
            }
            // Never erase an unresolved or damaged refusal record.
            if check(snapshot, record.previous_applied).is_err() { return Ok(()); }
            OpenOptions::new().write(true).truncate(true).open(&path)?
        }
        Err(error) => return Err(error),
    };
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    let bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
    write_payload(&mut file, &bytes)?;
    file.sync_all()
}

pub(crate) fn check(snapshot: &Path, recovered: Option<LogId<u64>>) -> io::Result<()> {
    let path = marker_path(snapshot);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() { return Err(io::Error::other("non-regular Raft failure marker")); }
    let bytes = std::fs::read(&path)?;
    let record: FailureRecord = serde_json::from_slice(&bytes).map_err(|error| io::Error::other(format!("Raft apply failure marker is incomplete or unreadable: {error}")))?;
    if record.version != 1 || record.failed_from.index > record.failed_through.index {
        return Err(io::Error::other("invalid Raft apply failure record"));
    }
    if recovered.is_some_and(|applied| applied.index >= record.failed_through.index) { return Ok(()); }
    Err(io::Error::other(format!("Raft startup refused: apply entries {}..={} failed after {:?}: {}", record.failed_from.index, record.failed_through.index, record.previous_applied, record.reason)))
}

/// Keep panic recording inside the owned blocking operation and before its
/// physical lease drops; cancellation of the async waiter cannot discard it.
pub(crate) struct UnwindGuard {
    pub(crate) snapshot: Option<PathBuf>,
    pub(crate) previous: Option<LogId<u64>>,
    pub(crate) first: LogId<u64>,
    pub(crate) last: LogId<u64>,
}
impl Drop for UnwindGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(path) = &self.snapshot {
                let record = FailureRecord::new(self.previous, self.first, self.last, "Raft physical apply job panicked".into());
                if let Err(error) = persist(path, &record) { tracing::error!(%error, "could not persist Raft panic refusal"); }
            }
        }
    }
}
