//! Monotone recovery evidence, independent of table-directory lifecycle cleanup.
//! One small document per table name; never stores rejected row payload/history.
use std::fs::File;
use std::path::{Path, PathBuf};

use ferrosa_common::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::commitlog::config::TableId;

const DIRECTORY: &str = "recovery-incomplete";
const MAX_STATUS_BYTES: u64 = 256;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IncompleteRecovery {
    version: u8,
    incomplete: bool,
}

fn location(data_dir: &Path, table: &TableId) -> (PathBuf, String) {
    // Encode each name independently: neither path separators nor ambiguous
    // dots in quoted identifiers can alias another table's evidence.
    let encode = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    (
        data_dir.join(DIRECTORY),
        format!(
            "{}-{}.json",
            encode(table.keyspace()),
            encode(table.table())
        ),
    )
}

pub(super) fn incomplete(data_dir: &Path, table: &TableId) -> Result<bool> {
    let unknown = unknown_incomplete(data_dir)?;
    let table = read_status(data_dir, &location(data_dir, table).1)?;
    Ok(unknown || table)
}

pub(super) fn unknown_incomplete(data_dir: &Path) -> Result<bool> {
    read_status(data_dir, "unknown-source.json")
}

fn read_status(data_dir: &Path, filename: &str) -> Result<bool> {
    let dir = data_dir.join(DIRECTORY);
    let path = dir.join(filename);
    // A missing status directory is clean; an unreadable/dangling directory
    // is evidence we cannot inspect, not an absent status document.
    match std::fs::symlink_metadata(&dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
        Ok(_) => {
            if !std::fs::metadata(&dir)?.is_dir() {
                return Err(Error::InvalidFormat(format!(
                    "recovery status directory is not a directory: {}",
                    dir.display()
                )));
            }
        }
    }
    // Unlike Path::exists, errors and dangling symlinks must not look clean.
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    if std::fs::metadata(&path)?.len() > MAX_STATUS_BYTES {
        return Err(Error::InvalidFormat(format!(
            "recovery status too large: {}",
            path.display()
        )));
    }
    let status: IncompleteRecovery = crate::schema_snapshot::load_bounded_json(&dir, filename)?
        .ok_or_else(|| {
            Error::InvalidFormat(format!("recovery status disappeared: {}", path.display()))
        })?;
    if status.version != 1 || !status.incomplete {
        return Err(Error::InvalidFormat(format!(
            "invalid incomplete recovery status: {}",
            path.display()
        )));
    }
    Ok(true)
}

pub(super) fn record(data_dir: &Path, table: &TableId) -> Result<()> {
    // Validate existing evidence before replacing it; never hide corruption.
    incomplete(data_dir, table)?;
    publish(data_dir, &location(data_dir, table).1)
}

/// One fixed-size unknown-source latch blocks every table, including tables
/// registered later. No rejected payload or unbounded frame history is stored.
pub(super) fn record_unknown(data_dir: &Path) -> Result<()> {
    unknown_incomplete(data_dir)?;
    publish(data_dir, "unknown-source.json")
}

fn publish(data_dir: &Path, filename: &str) -> Result<()> {
    let dir = data_dir.join(DIRECTORY);
    crate::schema_snapshot::persist_bounded_json(
        &dir,
        filename,
        &IncompleteRecovery {
            version: 1,
            incomplete: true,
        },
    )?;
    // persist_bounded_json syncs the containing directory. Also make a newly
    // created recovery-incomplete directory durable in the data directory.
    File::open(data_dir)?.sync_all()?;
    Ok(())
}

/// Constructors that do not replay cannot certify pre-existing WAL artifacts.
/// This deliberately detects files only; it does not decode or recover the WAL.
pub(super) fn wal_artifacts(log_dir: &Path) -> Result<bool> {
    for entry in std::fs::read_dir(log_dir)? {
        if entry?.path().extension().is_some_and(|ext| ext == "log") {
            return Ok(true);
        }
    }
    Ok(false)
}
