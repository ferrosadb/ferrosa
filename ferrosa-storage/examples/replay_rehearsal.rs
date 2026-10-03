//! Restart rehearsal: an operator tool, not a test. It opens a COPY of a real
//! node's data directory, replays the commit log, and flushes every table, so
//! you know a commit log replays and flushes on this build before restarting
//! the node on it. It was written for the 2026-10-03 incident (t_e0445d4b):
//! node2's log held whole-value collection cells beside element cells, the
//! shape that killed its `storage-flush`.
//!
//! ```text
//! cargo run --release -p ferrosa-storage --example replay_rehearsal -- <copied-data-dir>
//! ```
//!
//! The directory needs `commitlog/` and the `*.json` schema files; SSTables are
//! not needed. NEVER point it at a live node's directory, because replay and
//! flush write into it.
//!
//! The in-suite coverage of the same behaviour is
//! `replayed_mixed_collection_shapes_flush_and_read_back`. This used to be a
//! `live-infra-tests` test, but CI's `--all-features` run enables that feature
//! with no data directory to point at, so it failed every run.

use ferrosa_storage::{CommitLogConfig, StorageEngine, StorageEngineConfig};

fn main() {
    let dir = std::env::args_os().nth(1).unwrap_or_else(|| {
        eprintln!(
            "usage: replay_rehearsal <COPY of a node data dir> \
             (commitlog/ plus schema.json, storage-schema.json, dropped-tables.json)"
        );
        std::process::exit(2);
    });
    let dir = std::path::PathBuf::from(dir);
    assert!(
        dir.join("commitlog").is_dir(),
        "{} has no commitlog/",
        dir.display()
    );

    let mut config = StorageEngineConfig::test_config(&dir);
    config.commit_log = CommitLogConfig {
        log_dir: dir.join("commitlog"),
        checkpoint_dir: dir.join("commitlog"),
        ..CommitLogConfig::default()
    };
    // Production thresholds: replay must not flush mid-way at 4 KB.
    config.flush_threshold_bytes = 256 * 1024 * 1024;
    config.flush_max_age_secs = 3600;

    let (engine, pending) = StorageEngine::open(config, None).expect("open copied data dir");
    let mutations = pending.len();
    let rows: usize = pending.iter().map(|m| m.rows.len()).sum();
    let cells: usize = pending
        .iter()
        .flat_map(|m| m.rows.iter())
        .map(|r| r.cells.len())
        .sum();
    engine.replay_mutations(pending).expect("replay");
    engine
        .flush_all()
        .expect("every table must flush after replay on this build");

    // How much legacy whole-value collection data the log held, per table:
    // every expansion is counted in the Prometheus text.
    let expansions: Vec<String> = ferrosa_storage::metrics::render_prometheus()
        .lines()
        .filter(|l| l.starts_with("ferrosa_storage_collection_blob_expansions_total{"))
        .map(str::to_string)
        .collect();
    let tables_with_sstables = std::fs::read_dir(dir.join("sstables"))
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    std::fs::read_dir(e.path()).is_ok_and(|mut files| {
                        files.any(|f| {
                            f.is_ok_and(|f| f.file_name().to_string_lossy().ends_with("-Data.db"))
                        })
                    })
                })
                .count()
        })
        .expect("read the copy's sstables dir");
    let report = format!(
        "handed back for deferred replay (open() replays registered tables itself): mutations={mutations} rows={rows} cells={cells}\n\
         tables flushed to SSTables={tables_with_sstables}\n\
         collection blob expansions: {}\n",
        if expansions.is_empty() {
            "none".to_string()
        } else {
            expansions.join("; ")
        }
    );
    eprintln!("whole-value collection cells expanded:\n{report}");
    // Also left beside the copy, since a passing test's stderr is not kept.
    std::fs::write(dir.join("collection-blob-expansions.txt"), &report)
        .expect("write the expansion report into the copied data dir");
}
