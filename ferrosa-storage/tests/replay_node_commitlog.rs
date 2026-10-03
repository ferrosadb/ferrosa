//! Restart rehearsal against a COPY of a real node's data directory: open it,
//! replay its commit log, then flush every table. Proves that a commit log
//! holding whole-value collection cells beside element cells (the shape that
//! killed node2's `storage-flush` on 2026-10-03, t_e0445d4b) replays and
//! flushes on this build before the node itself is restarted on it.
//!
//! Opt in with `--features live-infra-tests` and point
//! `FERROSA_TEST_REPLAY_DATA_DIR` at a copy holding `commitlog/` and the
//! `*.json` schema files (SSTables are not needed). Never point it at a live
//! node's directory: replay and flush write into it.
#![cfg(feature = "live-infra-tests")]

use ferrosa_storage::{CommitLogConfig, StorageEngine, StorageEngineConfig};

#[test]
fn copied_node_commitlog_replays_and_flushes() {
    let dir = std::env::var("FERROSA_TEST_REPLAY_DATA_DIR").unwrap_or_else(|_| {
        panic!(
            "set FERROSA_TEST_REPLAY_DATA_DIR to a COPY of a node data dir \
             (commitlog/ plus schema.json, storage-schema.json, dropped-tables.json)"
        )
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
