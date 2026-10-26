//! One process per data directory (P0, t_34e20fa6).
//!
//! On 2026-10-25 two launchd jobs ran one `ferrosa.toml` against one data
//! directory. Each start of the duplicate replayed the commit log, which
//! unlinks every segment it replays, including the live node's ACTIVE one; the
//! live node kept appending and fsyncing into an inode with no name, so writes
//! it had acknowledged and read back were gone after the next reboot. The
//! duplicate also removed the live node's compaction staging directory.
//!
//! Every directory a node owns is locked with an exclusive `flock` on
//! `<dir>/ferrosa.lock` BEFORE anything opens, cleans, replays or creates a
//! file under it. The lock lives in the kernel, held by an open descriptor:
//! it goes away when the process does, however it dies, so a lock file left
//! behind by a dead node never blocks the next start. The file's contents are
//! only a diagnostic — the holder's pid, so a refused start can name it.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// The lock file each locked directory carries.
pub const LOCK_FILE_NAME: &str = "ferrosa.lock";

/// Exit code of a node refused because another process holds a directory.
/// `EX_TEMPFAIL`: the condition clears when the other process exits.
pub const EXIT_DATA_DIR_LOCKED: i32 = 75;

/// The most of a held lock file read back to name its holder.
const HOLDER_PID_MAX_BYTES: usize = 64;

/// An exclusive lock on one directory, held while this value is alive.
#[derive(Debug)]
pub struct DirLock {
    /// The open descriptor IS the lock; dropping it releases it.
    _file: File,
    dir: PathBuf,
}

impl DirLock {
    /// The directory this lock covers.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Why a directory could not be locked.
#[derive(Debug)]
pub enum LockError {
    /// Another process holds the lock.
    Held {
        dir: PathBuf,
        /// What the lock file says, trimmed; `None` when it is empty, which
        /// happens only while the holder is between taking the lock and
        /// recording its pid.
        holder_pid: Option<String>,
    },
    /// The lock file could not be opened, locked, read or written.
    Io {
        dir: PathBuf,
        op: &'static str,
        source: std::io::Error,
    },
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held { dir, holder_pid } => {
                let holder = match holder_pid {
                    Some(pid) => format!("pid {pid}"),
                    None => "a process that has not yet recorded its pid".to_string(),
                };
                write!(
                    f,
                    "data directory {} is in use by another ferrosa process ({holder}); \
                     two processes on one data directory lose acknowledged writes, so this \
                     one is exiting without touching it. Stop the other process (check for a \
                     duplicate launchd job or systemd unit on the same config) and start again",
                    dir.display()
                )
            }
            Self::Io { dir, op, source } => write!(
                f,
                "cannot {op} the lock file {}: {source}",
                dir.join(LOCK_FILE_NAME).display()
            ),
        }
    }
}

impl std::error::Error for LockError {}

/// Take the exclusive lock on `dir`, which must exist.
///
/// Opens (creating if absent, never truncating) `dir/ferrosa.lock` and takes
/// a non-blocking exclusive lock on it. On success the file is rewritten to
/// hold this process's pid. When another process holds the lock, nothing is
/// written: the holder's pid is read back and reported.
pub fn lock_dir(dir: &Path) -> Result<DirLock, LockError> {
    let io = |op: &'static str| {
        let dir = dir.to_path_buf();
        move |source| LockError::Io { dir, op, source }
    };
    let path = dir.join(LOCK_FILE_NAME);
    // No truncate: the holder's pid must still be readable if we lose.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(io("open"))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            // A pid is a few bytes; read no more than that from a file anyone
            // could have filled. Lossy, so stray bytes cannot fail the report.
            let mut raw = Vec::with_capacity(HOLDER_PID_MAX_BYTES);
            (&mut file)
                .take(HOLDER_PID_MAX_BYTES as u64)
                .read_to_end(&mut raw)
                .map_err(io("read"))?;
            let recorded = String::from_utf8_lossy(&raw);
            let recorded = recorded.trim();
            return Err(LockError::Held {
                dir: dir.to_path_buf(),
                holder_pid: (!recorded.is_empty()).then(|| recorded.to_string()),
            });
        }
        Err(TryLockError::Error(source)) => return Err(io("lock")(source)),
    }
    file.set_len(0).map_err(io("truncate"))?;
    file.seek(SeekFrom::Start(0)).map_err(io("seek"))?;
    writeln!(file, "{}", std::process::id()).map_err(io("write"))?;
    file.sync_all().map_err(io("sync"))?;
    Ok(DirLock {
        _file: file,
        dir: dir.to_path_buf(),
    })
}

/// Lock `data_dir` and every extra root that is not already inside it.
///
/// `data_dir` is locked first, so a refused process stops before it creates
/// any other directory. Each extra root is created if absent (the node
/// creates it at startup anyway), then compared by canonical path: a root
/// under `data_dir`, or one listed twice, is covered by a lock already taken.
pub fn lock_node_dirs(data_dir: &Path, extra_roots: &[PathBuf]) -> Result<Vec<DirLock>, LockError> {
    let canonical = |dir: &Path| {
        std::fs::canonicalize(dir).map_err(|source| LockError::Io {
            dir: dir.to_path_buf(),
            op: "resolve the directory of",
            source,
        })
    };
    let mut locks = vec![lock_dir(data_dir)?];
    let mut covered = vec![canonical(data_dir)?];
    for root in extra_roots {
        std::fs::create_dir_all(root).map_err(|source| LockError::Io {
            dir: root.clone(),
            op: "create the directory of",
            source,
        })?;
        let resolved = canonical(root)?;
        if covered.iter().any(|held| resolved.starts_with(held)) {
            continue;
        }
        locks.push(lock_dir(root)?);
        covered.push(resolved);
    }
    debug_assert_eq!(locks.len(), covered.len());
    Ok(locks)
}

/// The node's locks, held until the process exits.
///
/// Not a local in `run()`: `run` returns before `lifecycle` writes the last
/// line and exits, and the leaked upload runtime can still be writing then.
/// The kernel releases the locks when the process is gone, never before.
static HELD: std::sync::OnceLock<Vec<DirLock>> = std::sync::OnceLock::new();

/// Keep `locks` for the rest of the process. Refuses a second call: a node
/// locks its directories once.
pub fn hold_for_process_lifetime(locks: Vec<DirLock>) -> Result<(), String> {
    HELD.set(locks).map_err(|rejected| {
        format!(
            "data directory locks were already held; refusing to replace them with {:?}",
            rejected.iter().map(DirLock::dir).collect::<Vec<_>>()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded_pid(dir: &Path) -> String {
        std::fs::read_to_string(dir.join(LOCK_FILE_NAME))
            .expect("read lock file")
            .trim()
            .to_string()
    }

    #[test]
    fn the_holder_records_its_pid() {
        let dir = tempfile::tempdir().expect("temp dir");
        let lock = lock_dir(dir.path()).expect("first lock");
        assert_eq!(lock.dir(), dir.path());
        assert_eq!(recorded_pid(dir.path()), std::process::id().to_string());
    }

    /// flock conflicts between separate open file descriptions, so a second
    /// open in this same process stands in for a second process.
    #[test]
    fn a_second_lock_is_refused_names_the_holder_and_touches_nothing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let staging = dir.path().join("compaction");
        std::fs::create_dir(&staging).expect("staging dir");
        let sentinel = dir.path().join("sentinel");
        std::fs::write(&sentinel, b"keep").expect("sentinel");
        let _held = lock_dir(dir.path()).expect("first lock");

        let refused = lock_dir(dir.path()).expect_err("second lock must be refused");
        match &refused {
            LockError::Held { dir: d, holder_pid } => {
                assert_eq!(d, dir.path());
                assert_eq!(
                    holder_pid.as_deref(),
                    Some(std::process::id().to_string().as_str())
                );
            }
            other => panic!("expected Held, got {other:?}"),
        }
        let message = refused.to_string();
        assert!(
            message.contains(&dir.path().display().to_string()),
            "{message}"
        );
        assert!(
            message.contains(&format!("pid {}", std::process::id())),
            "{message}"
        );
        assert!(staging.is_dir(), "staging dir survives");
        assert_eq!(std::fs::read(&sentinel).expect("sentinel"), b"keep");
        assert_eq!(
            recorded_pid(dir.path()),
            std::process::id().to_string(),
            "the refused attempt must not overwrite the holder's pid"
        );
    }

    #[test]
    fn a_released_lock_can_be_taken_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        drop(lock_dir(dir.path()).expect("first lock"));
        lock_dir(dir.path()).expect("the lock is free once its holder is gone");
    }

    /// A lock file left by a dead process (a pid that no longer runs, a
    /// longer leftover than the new pid) does not block the start, and is
    /// rewritten to exactly the new holder's pid.
    #[test]
    fn a_stale_lock_file_with_no_live_holder_does_not_block() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            dir.path().join(LOCK_FILE_NAME),
            "4294967295\nleftover bytes from a dead node\n",
        )
        .expect("stale lock file");
        let _lock = lock_dir(dir.path()).expect("a stale file is not a lock");
        assert_eq!(recorded_pid(dir.path()), std::process::id().to_string());
    }

    #[test]
    fn a_missing_dir_is_an_error_not_a_lock() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("absent");
        match lock_dir(&missing) {
            Err(LockError::Io { op: "open", .. }) => {}
            other => panic!("expected an open error, got {other:?}"),
        }
    }

    #[test]
    fn roots_inside_the_data_dir_share_its_lock() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        std::fs::create_dir(&data).expect("data dir");
        let locks = lock_node_dirs(&data, &[data.join("raft"), data.join("hints")])
            .expect("lock node dirs");
        assert_eq!(locks.len(), 1);
        assert!(
            !data.join("raft").join(LOCK_FILE_NAME).exists(),
            "a nested root needs no lock file of its own"
        );
    }

    #[test]
    fn a_separately_configured_root_is_locked_too() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        let raft = dir.path().join("raft-elsewhere");
        std::fs::create_dir(&data).expect("data dir");
        let _held = lock_node_dirs(&data, &[raft.clone(), raft.clone()]).expect("first");
        assert!(raft.join(LOCK_FILE_NAME).is_file());

        // A second node with its own data dir but the SAME raft dir is refused
        // on the raft dir.
        let other_data = dir.path().join("other-data");
        std::fs::create_dir(&other_data).expect("other data dir");
        match lock_node_dirs(&other_data, std::slice::from_ref(&raft)) {
            Err(LockError::Held { dir: d, .. }) => assert_eq!(d, raft),
            other => panic!("expected the raft dir to be held, got {other:?}"),
        }
    }

    #[test]
    fn a_second_node_on_the_same_data_dir_creates_no_other_root() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        std::fs::create_dir(&data).expect("data dir");
        let _held = lock_node_dirs(&data, &[]).expect("first");
        let hints = dir.path().join("hints-elsewhere");
        assert!(matches!(
            lock_node_dirs(&data, std::slice::from_ref(&hints)),
            Err(LockError::Held { .. })
        ));
        assert!(!hints.exists(), "refused before creating any other root");
    }
}
