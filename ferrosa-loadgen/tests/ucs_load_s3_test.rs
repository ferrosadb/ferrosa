//! UCS load tests against a live cluster with S3 (RustFS) backend.
//!
//! Requires:
//!   FERROSA_TEST_CONTAINERS=1
//!   docker-compose.compaction-test.yml running (podman compose)
//!
//! The compaction-test compose runs on 2xxxx ports:
//!   - RustFS S3: 29000
//!   - Node CQL:  29042, 29043, 29044
//!
//! Run with --nocapture to see the full stats report:
//!   FERROSA_TEST_CONTAINERS=1 \
//!   cargo test -p ferrosa-loadgen --test ucs_load_s3_test -- --nocapture

use std::path::Path;
use std::time::Duration;

use ferrosa_loadgen::orchestrator::run_load_test;
use ferrosa_loadgen::profile::LoadProfile;
use ferrosa_storage::upload::ObjectStoreConfig;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};

fn s3_config(dir: &Path, profile: &LoadProfile) -> StorageEngineConfig {
    let s3_endpoint = std::env::var("FERROSA_COMPACTION_TEST_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:29000".into());

    let object_store = ObjectStoreConfig {
        endpoint: s3_endpoint,
        bucket: "ferrosa-compaction-test".into(),
        region: "us-east-1".into(),
        access_key_id: Some("rustfsadmin".into()),
        secret_access_key: Some("rustfsadmin".into()),
        allow_http: true,
        prefix: format!("test-{}", uuid::Uuid::new_v4()),
        upload_queue_depth: 8,
    };

    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: Some(object_store),
        local_cache_max_bytes: profile.local_cache_max_bytes,
        flush_threshold_bytes: profile.flush_threshold_bytes,
        flush_max_age_secs: 30,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
    }
}

#[test]
fn ucs_load_s3_write_heavy() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set — run:\n  \
             podman compose -f tests/docker-compose.compaction-test.yml up -d"
        );
    }

    let mut profile = LoadProfile::write_heavy();
    profile.duration = Duration::from_secs(60);
    profile.target_data_size_bytes = 50 * 1024 * 1024; // 50 MB
    profile.local_cache_max_bytes = 2 * 1024 * 1024; // 2 MB cache
    profile.flush_threshold_bytes = 32 * 1024;
    profile.key_space_size = 10_000;
    profile.num_writers = 6;
    profile.num_readers = 1;

    let dir = tempfile::tempdir().unwrap();
    let config = s3_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss through S3 pipeline");
    assert_eq!(
        stats.data_mismatches, 0,
        "no corruption through S3 pipeline"
    );
    assert!(stats.total_writes > 0, "must have written data");
    assert!(
        stats.bytes_written > profile.local_cache_max_bytes,
        "data written ({}) must exceed cache ({}) to force S3 reads",
        stats.bytes_written,
        profile.local_cache_max_bytes
    );
    println!("{stats}");

    engine.shutdown().unwrap();
}

#[test]
fn ucs_load_s3_balanced() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set — run:\n  \
             podman compose -f tests/docker-compose.compaction-test.yml up -d"
        );
    }

    let mut profile = LoadProfile::balanced();
    profile.duration = Duration::from_secs(60);
    profile.target_data_size_bytes = 30 * 1024 * 1024;
    profile.local_cache_max_bytes = 2 * 1024 * 1024;
    profile.flush_threshold_bytes = 64 * 1024;
    profile.key_space_size = 5000;
    profile.num_writers = 4;
    profile.num_readers = 4;

    let dir = tempfile::tempdir().unwrap();
    let config = s3_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(stats.total_writes > 0);
    assert!(stats.total_reads > 0);
    println!("{stats}");

    engine.shutdown().unwrap();
}

#[test]
fn ucs_load_s3_read_heavy() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set — run:\n  \
             podman compose -f tests/docker-compose.compaction-test.yml up -d"
        );
    }

    let mut profile = LoadProfile::read_heavy();
    profile.duration = Duration::from_secs(60);
    profile.target_data_size_bytes = 20 * 1024 * 1024;
    profile.local_cache_max_bytes = 1024 * 1024; // 1 MB
    profile.flush_threshold_bytes = 32 * 1024;
    profile.key_space_size = 3000;
    profile.num_writers = 2;
    profile.num_readers = 8;

    let dir = tempfile::tempdir().unwrap();
    let config = s3_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(
        stats.total_reads > stats.total_writes,
        "read-heavy should do more reads"
    );
    println!("{stats}");

    engine.shutdown().unwrap();
}
