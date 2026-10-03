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
    eprintln!("replaying {} pending mutations", pending.len());
    engine.replay_mutations(pending).expect("replay");
    engine
        .flush_all()
        .expect("every table must flush after replay on this build");
}
