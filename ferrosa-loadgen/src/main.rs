//! `ferrosa-loadgen` — load test runner for Ferrosa.
//!
//! Two modes:
//!
//!   # In-process mode (storage engine directly, no network):
//!   ferrosa-loadgen --profile balanced --duration 300 --tui
//!
//!   # Cluster mode (CQL client against a running cluster):
//!   ferrosa-loadgen --node 127.0.0.1:9042 --profile balanced --duration 300 --tui

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use ferrosa_loadgen::cluster::run_cluster_load_test;
use ferrosa_loadgen::orchestrator::{run_load_test, run_load_test_with_tui};
use ferrosa_loadgen::profile::LoadProfile;
use ferrosa_storage::upload::ObjectStoreConfig;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};

/// Ferrosa load test generator.
///
/// Without --node: runs an in-process storage engine (no network).
/// With --node: connects to a running cluster via CQL.
#[derive(Debug, Parser)]
#[command(name = "ferrosa-loadgen", version)]
struct Args {
    /// Load profile: read_heavy, balanced, write_heavy, delete_update_heavy, compaction_stress
    #[arg(short, long, default_value = "balanced")]
    profile: String,

    /// Test duration in seconds (overrides profile default).
    #[arg(short, long)]
    duration: Option<u64>,

    /// Connect to a running Ferrosa cluster via CQL.
    /// Comma-separated list of node addresses (e.g., 127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044).
    /// Workers are distributed round-robin across all nodes.
    #[arg(long)]
    node: Option<String>,

    /// Data directory for in-process mode.
    #[arg(long, default_value = "/tmp/ferrosa-loadgen")]
    data_dir: PathBuf,

    /// S3-compatible endpoint (in-process mode only).
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 bucket name.
    #[arg(long, default_value = "ferrosa-compaction-test")]
    s3_bucket: String,

    /// S3 access key.
    #[arg(long, default_value = "rustfsadmin")]
    s3_access_key: String,

    /// S3 secret key.
    #[arg(long, default_value = "rustfsadmin")]
    s3_secret_key: String,

    /// Local cache max bytes (overrides profile).
    #[arg(long)]
    cache_max_bytes: Option<u64>,

    /// Show a live TUI dashboard during the test.
    #[arg(long)]
    tui: bool,

    /// Remove the data directory after the test completes (in-process mode only).
    #[arg(long)]
    cleanup: bool,

    /// List available profiles and exit.
    #[arg(long)]
    list_profiles: bool,
}

fn main() {
    let args = Args::parse();

    if args.list_profiles {
        println!("Available profiles:");
        println!("  read_heavy           90% read / 10% write");
        println!("  balanced             50/50 read/write with 30% updates, 10% deletes");
        println!("  write_heavy          10% read / 90% write with 30% updates, 10% deletes");
        println!(
            "  delete_update_heavy  10% read / 70% update / 20% delete — max tombstone pressure"
        );
        println!("  compaction_stress    Long-running compaction stress with W=2");
        return;
    }

    let mut profile = match args.profile.as_str() {
        "read_heavy" => LoadProfile::read_heavy(),
        "balanced" => LoadProfile::balanced(),
        "write_heavy" => LoadProfile::write_heavy(),
        "delete_update_heavy" => LoadProfile::delete_update_heavy(),
        "compaction_stress" => LoadProfile::compaction_stress(),
        other => {
            eprintln!("Unknown profile: {other}. Use --list-profiles to see options.");
            std::process::exit(1);
        }
    };

    if let Some(dur) = args.duration {
        profile.duration = Duration::from_secs(dur);
    }
    if let Some(cache) = args.cache_max_bytes {
        profile.local_cache_max_bytes = cache;
    }

    // ── Cluster mode ──────────────────────────────────────────────────
    if let Some(ref node_str) = args.node {
        let nodes = ferrosa_loadgen::cluster::parse_nodes(node_str);
        if nodes.is_empty() {
            eprintln!("No valid node addresses in '{node_str}'");
            std::process::exit(1);
        }

        if !args.tui {
            println!("Starting cluster load test: {}", profile.name);
            println!("  Nodes: {nodes:?}");
            println!(
                "  Duration: {}s, Writers: {}, Readers: {}",
                profile.duration.as_secs(),
                profile.num_writers,
                profile.num_readers,
            );
            println!();
        }

        let stats = run_cluster_load_test(&nodes, &profile, args.tui);
        println!("{stats}");

        if stats.abort_reason.is_some() {
            std::process::exit(2);
        }
        return;
    }

    // ── In-process mode ───────────────────────────────────────────────
    std::fs::create_dir_all(&args.data_dir).expect("create data dir");

    let object_store = args.s3_endpoint.map(|endpoint| ObjectStoreConfig {
        endpoint,
        bucket: args.s3_bucket,
        region: "us-east-1".into(),
        access_key_id: Some(args.s3_access_key),
        secret_access_key: Some(args.s3_secret_key),
        allow_http: true,
        prefix: format!("loadgen-{}", uuid::Uuid::new_v4()),
        upload_queue_depth: 16,
    });

    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 32 * 1024 * 1024,
            max_segment_age: Duration::from_secs(300),
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_millis(10),
            },
            log_dir: args.data_dir.join("commitlog"),
            checkpoint_dir: args.data_dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(args.data_dir.join("compaction")),
        object_store,
        local_cache_max_bytes: profile.local_cache_max_bytes,
        flush_threshold_bytes: profile.flush_threshold_bytes,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 30,
        data_dir: args.data_dir.clone(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    };

    if !args.tui {
        println!("Starting in-process load test: {}", profile.name);
        println!(
            "  Duration: {}s, Keys: {}, Writers: {}, Readers: {}",
            profile.duration.as_secs(),
            profile.key_space_size,
            profile.num_writers,
            profile.num_readers,
        );
        if config.object_store.is_some() {
            println!("  S3: enabled");
        } else {
            println!("  S3: disabled (local only)");
        }
        println!();
    }

    let engine = StorageEngine::new(config, None).expect("create storage engine");
    let stats = if args.tui {
        run_load_test_with_tui(&profile, &engine)
    } else {
        run_load_test(&profile, &engine)
    };

    println!("{stats}");

    engine.shutdown().expect("shutdown engine");

    if args.cleanup {
        if let Err(e) = std::fs::remove_dir_all(&args.data_dir) {
            eprintln!("cleanup failed: {e}");
        } else {
            println!("Cleaned up {}", args.data_dir.display());
        }
    }

    if stats.missing_keys > 0 || stats.data_mismatches > 0 {
        std::process::exit(1);
    }
    if stats.abort_reason.is_some() {
        std::process::exit(2);
    }
}
