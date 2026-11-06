//! Durable record of which keyspaces this node has completely healed.
//!
//! The deploy heal ([`ReconcileMode::Rewrite`](super::reconcile::ReconcileMode))
//! repairs damage that builds before 330a0c29 left in the adjacency index. No
//! build since makes that damage, so one complete heal per node is enough.
//! Held only in memory, the heal reran on every restart, and every adjacency
//! query on the keyspace waited for it: about five minutes of CL=ALL rewrites
//! on the memory cluster (2026-10-19), for an index that was already whole.
//!
//! The record lives in the storage data dir, beside the storage engine's own
//! ledgers, so it is exactly as durable as the data it describes: a node that
//! loses its data dir loses the record and heals again. It is written only after
//! a heal completes. Each entry carries the [`HEAL_VERSION`] that wrote it; a
//! build that needs every node to heal again raises the version.
//!
//! A record that cannot be read is not trusted: the caller heals and logs why.
//! Healing an index that is already whole is slow but correct; skipping a heal
//! the index needs is not.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

use ferrosa_common::{Error, Result};

/// File under the storage data dir that holds the record.
pub const HEAL_RECORD_FILE: &str = "graph-adjacency-heals.json";

/// The heal this build performs. A recorded heal of an older version does not
/// count: raise this when a fix needs every node's index healed again.
pub const HEAL_VERSION: u32 = 1;

pub type WriterLock = Arc<parking_lot::Mutex<()>>;

/// One writer lock per data dir that has a record write in flight. Weak, so
/// an entry lives only while a writer holds its lock; dead entries are pruned
/// on every lookup, which bounds the map by the concurrent writers.
static WRITERS: LazyLock<parking_lot::Mutex<HashMap<PathBuf, Weak<parking_lot::Mutex<()>>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// The lock that serializes this data dir's record read-modify-write.
///
/// Per data dir, not process-wide: the record of one dir never waits on
/// another's fsyncs. It was one global lock, so every engine in a process
/// (each test's, each with its own dir) queued its record behind all the
/// others, and the first-use heal gate waits for the record. The key is the
/// canonical path, so two spellings of one dir share a lock.
fn writer_lock(data_dir: &Path) -> Result<WriterLock> {
    std::fs::create_dir_all(data_dir)?;
    let key = std::fs::canonicalize(data_dir)?;
    let mut writers = WRITERS.lock();
    writers.retain(|_, writer| writer.strong_count() > 0);
    if let Some(live) = writers.get(&key).and_then(Weak::upgrade) {
        return Ok(live);
    }
    let lock = Arc::new(parking_lot::Mutex::new(()));
    writers.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

/// Data keyspace to the version of the last heal that completed for it.
type HealRecord = BTreeMap<String, u32>;

fn load(data_dir: &Path) -> Result<HealRecord> {
    let path = data_dir.join(HEAL_RECORD_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HealRecord::new()),
        Err(e) => return Err(e.into()),
    };
    serde_json::from_slice(&bytes).map_err(|e| {
        Error::InvalidData(format!(
            "{} does not parse, so it cannot say which adjacency heals completed: {e}",
            path.display()
        ))
    })
}

/// The writer lock of `data_dir`'s record, for a test that must hold a
/// record write in flight (see `writer_lock`).
#[doc(hidden)]
pub fn record_writer_lock_for_test(data_dir: &Path) -> Result<WriterLock> {
    writer_lock(data_dir)
}

/// Whether this node completed a heal of `keyspace` at the current
/// [`HEAL_VERSION`].
pub fn healed(data_dir: &Path, keyspace: &str) -> Result<bool> {
    Ok(load(data_dir)?.get(keyspace) == Some(&HEAL_VERSION))
}

/// Durably records that a heal of `keyspace` completed at [`HEAL_VERSION`].
/// Call only after the heal reported no errors.
pub fn record_healed(data_dir: &Path, keyspace: &str) -> Result<()> {
    let serial = writer_lock(data_dir)?;
    let _serial = serial.lock();
    let mut record = load(data_dir)?;
    if record.insert(keyspace.to_string(), HEAL_VERSION) != Some(HEAL_VERSION) {
        ferrosa_storage::schema_snapshot::persist_bounded_json(
            data_dir,
            HEAL_RECORD_FILE,
            &record,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_healed_before_anything_is_recorded() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!healed(dir.path(), "ks").unwrap());
    }

    #[test]
    fn a_recorded_heal_survives_a_reload_and_is_per_keyspace() {
        let dir = tempfile::tempdir().unwrap();
        record_healed(dir.path(), "ks").unwrap();
        record_healed(dir.path(), "ks").unwrap();
        assert!(healed(dir.path(), "ks").unwrap());
        assert!(!healed(dir.path(), "other").unwrap());
    }

    #[test]
    fn a_heal_recorded_by_an_older_version_does_not_count() {
        let dir = tempfile::tempdir().unwrap();
        let older = HealRecord::from([("ks".to_string(), HEAL_VERSION - 1)]);
        std::fs::write(
            dir.path().join(HEAL_RECORD_FILE),
            serde_json::to_vec(&older).unwrap(),
        )
        .unwrap();
        assert!(!healed(dir.path(), "ks").unwrap());
        record_healed(dir.path(), "ks").unwrap();
        assert!(healed(dir.path(), "ks").unwrap());
    }

    /// Recording a heal is serialized per data dir, never process-wide. With
    /// one global lock every engine in a process queued its record (two
    /// fsyncs) behind every other engine's, whatever its data dir: measured
    /// p50 312 ms of lock WAIT against 51 ms held, across the HTTP suite's
    /// engines. The first-use heal gate waits for the record, so the wait
    /// alone pushed heals past the 1 s budget and queries were refused.
    #[test]
    fn a_record_in_one_data_dir_does_not_wait_for_another() {
        let busy = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let busy_lock = writer_lock(busy.path()).unwrap();
        let _held = busy_lock.lock();
        let (done, recorded) = std::sync::mpsc::channel();
        let other_dir = other.path().to_path_buf();
        std::thread::spawn(move || {
            let outcome = record_healed(&other_dir, "ks").map_err(|e| e.to_string());
            // The receiver outlives this thread unless the test already
            // failed on its timeout; a failed send then changes nothing.
            if done.send(outcome).is_err() {
                eprintln!("record finished after the test gave up waiting");
            }
        });
        let outcome = recorded
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a record in another data dir waited on this one's writer lock");
        outcome.unwrap();
        assert!(healed(other.path(), "ks").unwrap());
    }

    #[test]
    fn one_data_dir_shares_one_writer_lock() {
        let dir = tempfile::tempdir().unwrap();
        let a = writer_lock(dir.path()).unwrap();
        let b = writer_lock(&dir.path().join(".")).unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "two spellings of one data dir must serialize on one lock"
        );
    }

    #[test]
    fn a_corrupt_record_is_an_error_and_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(HEAL_RECORD_FILE), b"{not json").unwrap();
        let err = healed(dir.path(), "ks").unwrap_err().to_string();
        assert!(err.contains("does not parse"), "got: {err}");
        record_healed(dir.path(), "ks").expect_err("must not overwrite what it cannot read");
    }
}
