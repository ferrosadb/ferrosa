//! Inspect and replay set-aside commit-log mutations —
//! `ferrosa-ctl commitlog set-aside ...`.
//!
//! When startup replay finds no table schema for a mutation and its in-memory
//! buffer is full, the engine writes the mutation to
//! `<data_dir>/commitlog-unreplayed/*.unreplayed`. Those rows are durable but
//! invisible to reads, and `/readyz` reports the node as not ready until they
//! are applied. A node re-ingests them by itself once the table's schema is
//! registered; this command is the operator's view and the offline route.
//!
//! Like `ferrosa-ctl sstable`, it takes **no network connection** and works on
//! a data directory. It is a dry run unless `--apply` is given, and `--apply`
//! needs the node STOPPED: it opens the data dir's local schema, applies each
//! frame whose table is known, flushes those tables to SSTables, and rewrites
//! each file without the applied frames (removing it when nothing is left).
//! Frames for tables whose schema is still unknown stay in the file. The node
//! commit log is not touched: the engine runs over a scratch commit log.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use tabled::builder::Builder;
use tabled::settings::Style;

use ferrosa_storage::replay_set_aside::{scan_set_aside_dir, SetAsideStatus};
use ferrosa_storage::StorageEngine;

/// Error type shared with `main`'s unified result.
type CtlError = Box<dyn std::error::Error + Send + Sync>;

/// What one set-aside file holds.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FileReport {
    pub path: PathBuf,
    pub mutations: u64,
    pub tables: BTreeMap<String, u64>,
    /// Why the file could not be read to its end, if it could not.
    pub error: Option<String>,
}

/// Everything set aside under one data dir.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SetAsideReport {
    pub data_dir: PathBuf,
    pub files: Vec<FileReport>,
    pub mutations: u64,
    pub tables: BTreeMap<String, u64>,
}

impl SetAsideReport {
    fn from_status(data_dir: &Path, status: &SetAsideStatus) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            files: status
                .files
                .iter()
                .map(|f| FileReport {
                    path: f.path.clone(),
                    mutations: f.mutations,
                    tables: f.tables.clone(),
                    error: f.error.clone(),
                })
                .collect(),
            mutations: status.mutations(),
            tables: status.tables(),
        }
    }
}

/// What `--apply` did.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ApplyReport {
    pub before: SetAsideReport,
    pub after: SetAsideReport,
    pub applied: u64,
}

/// Dry run: counts what is set aside under `data_dir`. Pure read.
pub fn set_aside_report(data_dir: &Path) -> Result<SetAsideReport, CtlError> {
    if !data_dir.is_dir() {
        return Err(format!("not a directory: {}", data_dir.display()).into());
    }
    let status = scan_set_aside_dir(data_dir)?;
    Ok(SetAsideReport::from_status(data_dir, &status))
}

/// Applies every set-aside frame whose table schema is known locally. The node
/// must be stopped.
pub fn set_aside_apply(data_dir: &Path) -> Result<ApplyReport, CtlError> {
    let before = set_aside_report(data_dir)?;
    let scratch = tempfile::tempdir()?;
    let after_status = StorageEngine::reingest_set_aside_offline(data_dir, scratch.path())?;
    let after = SetAsideReport::from_status(data_dir, &after_status);
    let applied = before.mutations.saturating_sub(after.mutations);
    Ok(ApplyReport {
        before,
        after,
        applied,
    })
}

/// `ferrosa-ctl commitlog set-aside` entry point.
pub fn run_set_aside(data_dir: &Path, apply: bool, json: bool) -> Result<(), CtlError> {
    if apply {
        let report = set_aside_apply(data_dir)?;
        if json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            print_report(&report.before, "before");
            println!(
                "applied {} mutation(s); {} still set aside",
                report.applied, report.after.mutations
            );
            print_report(&report.after, "after");
        }
        if report.after.mutations > 0 || report.after.files.iter().any(|f| f.error.is_some()) {
            return Err(format!(
                "{} mutation(s) remain set aside; their table schema is not known locally, \
                 or a file is unreadable",
                report.after.mutations
            )
            .into());
        }
        return Ok(());
    }
    let report = set_aside_report(data_dir)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report, "set aside");
        if report.mutations > 0 {
            println!("dry run: pass --apply, with the node STOPPED, to replay them");
        }
    }
    Ok(())
}

fn print_report(report: &SetAsideReport, label: &str) {
    let mut builder = Builder::default();
    builder.push_record(["file", "mutations", "tables", "error"]);
    for f in &report.files {
        let tables = f
            .tables
            .iter()
            .map(|(t, n)| format!("{t}={n}"))
            .collect::<Vec<_>>()
            .join(", ");
        builder.push_record([
            f.path.display().to_string(),
            f.mutations.to_string(),
            tables,
            f.error.clone().unwrap_or_default(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    println!(
        "{}: {label}: {} file(s), {} mutation(s)",
        report.data_dir.display(),
        report.files.len(),
        report.mutations
    );
    for (table, n) in &report.tables {
        println!("  {table}: {n}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::replay_set_aside::ReplaySetAside;
    use ferrosa_storage::{Mutation, StorageEngineConfig, TableId};

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ctl_ks".to_string(),
            table: "ctl_t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn key(k: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(k.as_bytes().to_vec()))
    }

    fn row(ts: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"v".to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn set_aside(data_dir: &Path, table: &str, keys: &[&str]) -> PathBuf {
        let mut aside = ReplaySetAside::new(data_dir);
        for (i, k) in keys.iter().enumerate() {
            let ts = 10 + i as i64;
            aside
                .append(&Mutation::new(
                    "ctl_ks".into(),
                    table.into(),
                    key(k),
                    vec![row(ts)],
                    ts,
                ))
                .unwrap();
        }
        aside.sync().unwrap();
        aside.into_report().unwrap().path
    }

    /// A data dir whose local schema knows `ctl_ks.ctl_t`, as a node that has
    /// flushed once would have.
    fn data_dir_with_schema() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        engine.register_table(schema()).unwrap();
        engine
            .write(&TableId::new("ctl_ks", "ctl_t"), &key("anchor"), row(1), 1)
            .unwrap();
        engine.flush(&TableId::new("ctl_ks", "ctl_t")).unwrap();
        engine.shutdown().unwrap();
        dir
    }

    #[test]
    fn dry_run_reports_counts_and_changes_nothing() {
        let dir = data_dir_with_schema();
        let path = set_aside(dir.path(), "ctl_t", &["a", "b", "c"]);
        let before = std::fs::read(&path).unwrap();

        let report = set_aside_report(dir.path()).unwrap();

        assert_eq!(report.mutations, 3);
        assert_eq!(report.files.len(), 1);
        assert_eq!(report.tables.get("ctl_ks.ctl_t"), Some(&3));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "dry run is a pure read"
        );
    }

    #[test]
    fn dry_run_of_a_dir_with_nothing_set_aside_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let report = set_aside_report(dir.path()).unwrap();
        assert_eq!(report.mutations, 0);
        assert!(report.files.is_empty());
    }

    #[test]
    fn apply_replays_known_tables_and_removes_the_file() {
        let dir = data_dir_with_schema();
        let path = set_aside(dir.path(), "ctl_t", &["a", "b", "c"]);

        let report = set_aside_apply(dir.path()).unwrap();

        assert_eq!(report.before.mutations, 3);
        assert_eq!(report.applied, 3);
        assert_eq!(report.after.mutations, 0);
        assert!(!path.exists(), "the applied file is gone");

        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        for k in ["a", "b", "c"] {
            assert!(
                engine
                    .read(&TableId::new("ctl_ks", "ctl_t"), &key(k))
                    .unwrap()
                    .is_some(),
                "{k} must be readable after --apply"
            );
        }
    }

    #[test]
    fn apply_leaves_frames_for_unknown_tables() {
        let dir = data_dir_with_schema();
        let path = set_aside(dir.path(), "no_such_table", &["x", "y"]);

        let report = set_aside_apply(dir.path()).unwrap();

        assert_eq!(report.applied, 0);
        assert_eq!(report.after.mutations, 2);
        assert!(path.exists(), "unknown-schema mutations stay set aside");
    }
}
