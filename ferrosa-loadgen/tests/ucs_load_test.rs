//! UCS end-to-end load tests.
//!
//! Three workload profiles exercise the full write → flush → compact pipeline
//! with property-based random load. Each verifies zero data loss and zero
//! corruption via ground truth comparison.
//!
//! Run with --nocapture to see the full stats report:
//!   cargo test -p ferrosa-loadgen --test ucs_load_test -- --nocapture

use std::path::Path;
use std::time::Duration;

use proptest::prelude::*;

use ferrosa_loadgen::orchestrator::run_load_test;
use ferrosa_loadgen::profile::LoadProfile;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};

fn test_engine_config(dir: &Path, profile: &LoadProfile) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 32 * 1024 * 1024, // 32 MB — must exceed largest mutation
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: profile.local_cache_max_bytes,
        flush_threshold_bytes: profile.flush_threshold_bytes,
        flush_max_age_secs: 30,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
    }
}

// ---------------------------------------------------------------------------
// Three profile tests — 30s each to generate real compaction pressure.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "30s loadgen test; flaky under workspace parallel load. Run explicitly: cargo test -p ferrosa-loadgen --test ucs_load_test -- --ignored"]
fn ucs_load_read_heavy() {
    let mut profile = LoadProfile::read_heavy();
    profile.duration = Duration::from_secs(30);
    profile.target_data_size_bytes = 20 * 1024 * 1024; // 20 MB
    profile.key_space_size = 5000;
    profile.num_writers = 2;
    profile.num_readers = 6;
    profile.flush_threshold_bytes = 64 * 1024; // 64 KB

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(stats.total_writes > 0, "must have written data");
    assert!(stats.total_reads > 0, "must have read data");
    assert!(
        stats.write_latency.count > 0,
        "write latency must be tracked"
    );
    assert!(stats.read_latency.count > 0, "read latency must be tracked");
    println!("{stats}");

    engine.shutdown().unwrap();
}

#[test]
#[ignore = "30s loadgen test; flaky under workspace parallel load. Run explicitly: cargo test -p ferrosa-loadgen --test ucs_load_test -- --ignored"]
fn ucs_load_balanced() {
    let mut profile = LoadProfile::balanced();
    profile.duration = Duration::from_secs(30);
    profile.target_data_size_bytes = 20 * 1024 * 1024;
    profile.key_space_size = 5000;
    profile.num_writers = 4;
    profile.num_readers = 4;
    profile.flush_threshold_bytes = 64 * 1024;

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path(), &profile);
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
#[ignore = "30s loadgen test; flaky under workspace parallel load. Run explicitly: cargo test -p ferrosa-loadgen --test ucs_load_test -- --ignored"]
fn ucs_load_write_heavy() {
    let mut profile = LoadProfile::write_heavy();
    profile.duration = Duration::from_secs(30);
    profile.target_data_size_bytes = 30 * 1024 * 1024; // 30 MB
    profile.key_space_size = 10_000;
    profile.num_writers = 6;
    profile.num_readers = 1;
    profile.flush_threshold_bytes = 32 * 1024; // 32 KB — max flush pressure

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(stats.total_writes > 0);
    println!("{stats}");

    engine.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Compaction correctness under load
// ---------------------------------------------------------------------------

#[test]
fn ucs_compaction_reduces_sstable_count() {
    let mut profile = LoadProfile::write_heavy();
    profile.duration = Duration::from_secs(15);
    profile.fan_factor = 2; // aggressive compaction
    profile.flush_threshold_bytes = 16 * 1024; // 16 KB
    profile.target_data_size_bytes = 10 * 1024 * 1024;
    profile.key_space_size = 2000;
    profile.num_writers = 4;
    profile.num_readers = 0;
    profile.read_ratio = 0.0;
    profile.write_ratio = 1.0;

    let dir = tempfile::tempdir().unwrap();
    let config = test_engine_config(dir.path(), &profile);
    let engine = StorageEngine::new(config, None).unwrap();

    let stats = run_load_test(&profile, &engine);

    assert_eq!(stats.missing_keys, 0, "no data loss after compaction");
    assert_eq!(stats.data_mismatches, 0, "no corruption after compaction");
    assert!(
        stats.total_writes > 100,
        "must have written enough to trigger compaction"
    );
    println!("{stats}");

    engine.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Proptest-driven fuzz profiles
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3))]

    #[test]
    fn ucs_load_random_profile(
        read_pct in 0.0f64..=1.0,
        key_space in 100usize..=5000,
        val_min in 64usize..=256,
        fan_factor in 2u32..=8,
    ) {
        let profile = LoadProfile {
            name: "fuzz".into(),
            read_ratio: read_pct,
            write_ratio: 1.0 - read_pct,
            update_ratio: 0.3,
            delete_ratio: 0.1,
            key_space_size: key_space,
            value_size_range: (val_min, val_min + 256),
            num_writers: 2,
            num_readers: 2,
            duration: Duration::from_secs(5),
            flush_threshold_bytes: 64 * 1024,
            local_cache_max_bytes: 5 * 1024 * 1024,
            target_data_size_bytes: 2 * 1024 * 1024,
            fan_factor,
        };

        let dir = tempfile::tempdir().unwrap();
        let config = test_engine_config(dir.path(), &profile);
        let engine = StorageEngine::new(config, None).unwrap();

        let stats = run_load_test(&profile, &engine);

        prop_assert_eq!(stats.missing_keys, 0, "no data loss (seed-reproducible)");
        prop_assert_eq!(stats.data_mismatches, 0, "no corruption (seed-reproducible)");

        engine.shutdown().unwrap();
    }
}
