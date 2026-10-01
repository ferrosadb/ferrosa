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

static WRITE_SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// `keyspace.table` to the wall-clock milliseconds of its latest drop.
pub type DropLedger = BTreeMap<String, u64>;

/// Key of a table in the ledger.
pub fn ledger_key(keyspace: &str, table: &str) -> String {
    format!("{keyspace}.{table}")
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
