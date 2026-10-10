//! Durable record of when each table name was last dropped.
//!
//! The engine has no stable identifier for a table incarnation: `TableSchema`
//! and `Mutation` identify a table by `keyspace.table` alone (the registry's
//! `TableMetadata::id` UUID never reaches storage). Without one, a set-aside
//! frame written for a table that was later dropped is indistinguishable from a
//! frame for a new table of the same name, and re-ingesting it resurrects the
//! old table's rows.
//!
//! This ledger is the narrowest sound substitute. `unregister_table` records
//! the wall-clock time of each drop here before it removes anything. A
//! set-aside file's name carries the time it was created, which is an upper
//! bound on when every frame in it was written (frames reach a set-aside file
//! only after they were committed). A file created at or before a table's drop
//! therefore holds only frames from before that drop: they belong to a previous
//! incarnation and must not be applied to whatever now holds the name.
//!
//! What it cannot decide: a file created AFTER the drop may still hold frames
//! from before it (the commit log outlives a drop until its segment retires).
//! Telling those apart needs the incarnation stamped into the commit-log
//! mutation at write time, which is a wire-format change tracked separately.
//! Tables dropped before this ledger existed have no entry and are never
//! treated as stale, matching the behaviour before the ledger.
//!
//! The ledger keeps one entry per table name (the latest drop), so it is
//! bounded by the number of distinct names ever dropped.

use std::collections::BTreeMap;
use std::path::Path;

use ferrosa_common::{Error, Result};

/// File under the data dir that holds the ledger.
pub const DROPPED_TABLES_FILE: &str = "dropped-tables.json";

/// File under the data dir that lists table names whose DROP/TRUNCATE could not
/// remove their SSTable directory.
///
/// A destructive table operation reports success only after its data files are
/// gone (invariant: no read path may return a dropped row). When the removal
/// fails — the live incident: an aborted bulk load left an entry under
/// `public.pgbench_accounts` that `remove_dir_all` could not delete — the drop
/// must not silently succeed, and the survivors must never be loaded again.
///
/// This set is the durable half of that contract. A name is added **before** the
/// removal is attempted and removed **after** it succeeds, so:
/// - a removal that fails leaves the name recorded: the drop fails loud AND the
///   next `build_table_state` for that name sweeps the directory before loading,
///   so a same-name CREATE can never reload the dropped rows;
/// - a removal that succeeds clears the name, so a legitimately re-created table
///   is untouched.
///
/// Everything in such a directory at registration time was written before the
/// drop (registration precedes any write), so the whole directory is orphaned
/// debris and is removed, not merely hidden.
pub const PENDING_SWEEPS_FILE: &str = "pending-table-sweeps.json";

static WRITE_SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// `keyspace.table` to the wall-clock milliseconds of its latest drop.
pub type DropLedger = BTreeMap<String, u64>;

/// Table names whose SSTable directory must be swept before it is next loaded.
pub type PendingSweeps = std::collections::BTreeSet<String>;

/// Key of a table in the ledger.
pub fn ledger_key(keyspace: &str, table: &str) -> String {
    format!("{keyspace}.{table}")
}

/// Reads the pending-sweep set. A missing file is an empty set; a file that does
/// not parse is an error, because treating it as empty would let orphaned rows be
/// loaded again.
pub fn load_pending_sweeps(data_dir: &Path) -> Result<PendingSweeps> {
    let path = data_dir.join(PENDING_SWEEPS_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(PendingSweeps::new()),
        Err(e) => return Err(e.into()),
    };
    serde_json::from_slice(&bytes).map_err(|e| {
        Error::InvalidData(format!(
            "{} does not parse; refusing to guess whether a dropped table's orphans may load: {e}",
            path.display()
        ))
    })
}

/// Whether `keyspace.table` has a pending sweep (a drop that could not remove
/// its SSTables).
pub fn is_pending_sweep(data_dir: &Path, keyspace: &str, table: &str) -> Result<bool> {
    Ok(load_pending_sweeps(data_dir)?.contains(&ledger_key(keyspace, table)))
}

/// Durably records that `keyspace.table`'s SSTable directory must be swept before
/// it is loaded again. Written before the removal is attempted.
pub fn mark_pending_sweep(data_dir: &Path, keyspace: &str, table: &str) -> Result<()> {
    let _serial = WRITE_SERIAL.lock();
    let mut sweeps = load_pending_sweeps(data_dir)?;
    if sweeps.insert(ledger_key(keyspace, table)) {
        crate::schema_snapshot::persist_bounded_json(data_dir, PENDING_SWEEPS_FILE, &sweeps)?;
    }
    Ok(())
}

/// Clears the pending sweep for `keyspace.table` once its directory is gone.
pub fn clear_pending_sweep(data_dir: &Path, keyspace: &str, table: &str) -> Result<()> {
    let _serial = WRITE_SERIAL.lock();
    let mut sweeps = load_pending_sweeps(data_dir)?;
    if sweeps.remove(&ledger_key(keyspace, table)) {
        crate::schema_snapshot::persist_bounded_json(data_dir, PENDING_SWEEPS_FILE, &sweeps)?;
    }
    Ok(())
}

/// Wall-clock milliseconds since the epoch. A clock before 1970 is an error,
/// not a zero: a zero here would mark every file as older than the drop.
pub fn now_millis() -> Result<u64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::InvalidData(format!("system clock is before 1970: {e}")))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| Error::InvalidData("system clock does not fit in u64 milliseconds".into()))
}

/// Reads the ledger. A missing file is an empty ledger; a file that does not
/// parse is an error, because treating it as empty would let stale frames apply.
pub fn load(data_dir: &Path) -> Result<DropLedger> {
    let path = data_dir.join(DROPPED_TABLES_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(DropLedger::new()),
        Err(e) => return Err(e.into()),
    };
    serde_json::from_slice(&bytes).map_err(|e| {
        Error::InvalidData(format!(
            "{} does not parse; refusing to guess which set-aside frames are stale: {e}",
            path.display()
        ))
    })
}

/// Durably records that `keyspace.table` was dropped at `at_millis`.
pub fn record_drop(data_dir: &Path, keyspace: &str, table: &str, at_millis: u64) -> Result<()> {
    let _serial = WRITE_SERIAL.lock();
    let mut ledger = load(data_dir)?;
    let entry = ledger.entry(ledger_key(keyspace, table)).or_insert(0);
    *entry = (*entry).max(at_millis);
    crate::schema_snapshot::persist_bounded_json(data_dir, DROPPED_TABLES_FILE, &ledger)
}

/// The creation time, in milliseconds, encoded in a set-aside file name
/// (`<millis>-<pid>.unreplayed`), or `None` when the name carries none.
pub fn set_aside_file_created_millis(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.split_once('-')?.0.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_ledger_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn a_recorded_drop_survives_a_reload_and_keeps_the_latest_time() {
        let dir = tempfile::tempdir().unwrap();
        record_drop(dir.path(), "ks", "t", 500).unwrap();
        record_drop(dir.path(), "ks", "t", 300).unwrap();
        record_drop(dir.path(), "ks", "u", 900).unwrap();
        let ledger = load(dir.path()).unwrap();
        assert_eq!(ledger.get("ks.t"), Some(&500), "the latest drop wins");
        assert_eq!(ledger.get("ks.u"), Some(&900));
    }

    #[test]
    fn a_corrupt_ledger_is_an_error_not_an_empty_ledger() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(DROPPED_TABLES_FILE), b"{not json").unwrap();
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("does not parse"), "got: {err}");
        record_drop(dir.path(), "ks", "t", 1).expect_err("must not overwrite what it cannot read");
    }

    #[test]
    fn the_creation_time_comes_from_the_file_name() {
        let p = Path::new("/d/commitlog-unreplayed/1759287456789-4242.unreplayed");
        assert_eq!(set_aside_file_created_millis(p), Some(1_759_287_456_789));
        assert_eq!(
            set_aside_file_created_millis(Path::new("/d/odd.unreplayed")),
            None
        );
    }
}
