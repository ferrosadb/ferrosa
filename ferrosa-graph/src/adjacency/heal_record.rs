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

use std::collections::BTreeMap;
use std::path::Path;

use ferrosa_common::{Error, Result};

/// File under the storage data dir that holds the record.
pub const HEAL_RECORD_FILE: &str = "graph-adjacency-heals.json";

/// The heal this build performs. A recorded heal of an older version does not
/// count: raise this when a fix needs every node's index healed again.
pub const HEAL_VERSION: u32 = 1;

static WRITE_SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

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

/// Whether this node completed a heal of `keyspace` at the current
/// [`HEAL_VERSION`].
pub fn healed(data_dir: &Path, keyspace: &str) -> Result<bool> {
    Ok(load(data_dir)?.get(keyspace) == Some(&HEAL_VERSION))
}

/// Durably records that a heal of `keyspace` completed at [`HEAL_VERSION`].
/// Call only after the heal reported no errors.
pub fn record_healed(data_dir: &Path, keyspace: &str) -> Result<()> {
    let _serial = WRITE_SERIAL.lock();
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

    #[test]
    fn a_corrupt_record_is_an_error_and_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(HEAL_RECORD_FILE), b"{not json").unwrap();
        let err = healed(dir.path(), "ks").unwrap_err().to_string();
        assert!(err.contains("does not parse"), "got: {err}");
        record_healed(dir.path(), "ks").expect_err("must not overwrite what it cannot read");
    }
}
