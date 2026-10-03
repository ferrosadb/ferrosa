//! A dropped `ModeController` must free itself, its storage engine and the
//! engine's threads.
//!
//! The controller owns its `PeerManager` and the peer manager held the
//! controller as its event listener through a strong `Arc`, so neither was
//! ever freed. Each leaked controller kept a whole storage engine alive with
//! its worker threads. The co-located-layout properties in
//! `controller/tests.rs` build one per case; at the nightly fuzz job's
//! `PROPTEST_CASES=5000` that exhausted the runner's memory (2026-10-02, "Out
//! of memory") or wedged it until the job was cancelled (10-01, 10-03).
//!
//! This is its own test binary, with one test, so the process thread count is
//! not moved by other tests running concurrently.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::controller::ModeController;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_schema::{
    AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
    PasswordPolicy, RateLimitConfig, Schema, SchemaConfig,
};
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig};
use uuid::Uuid;

const CONTROLLERS: usize = 12;

fn storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

fn schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: SchemaDeploymentMode::Development,
        })
        .unwrap(),
    )
}

/// Threads in this process, read from the OS. Panics rather than guess.
fn thread_count() -> usize {
    if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
        return tasks.count();
    }
    // macOS: one header line plus one line per thread.
    let out = std::process::Command::new("ps")
        .args(["-M", "-p", &std::process::id().to_string()])
        .output()
        .expect("neither /proc/self/task nor `ps -M` is available to count threads");
    String::from_utf8_lossy(&out.stdout).lines().count() - 1
}

/// Build a controller wired to its peer manager exactly as `main` does, then
/// drop every handle. Returns weak references to what must now be freed.
fn build_and_drop(
    dir: &std::path::Path,
    local_id: Uuid,
) -> (Weak<ModeController>, Weak<StorageEngine>) {
    let net_config = Arc::new(NetConfig::default());
    let engine = storage(dir);
    let weak_engine = Arc::downgrade(&engine);
    let (controller, handles) = ModeController::new(
        Arc::new(ClusterConfig::default()),
        net_config.clone(),
        local_id,
        engine,
        schema(),
        Arc::new(HandlerRegistry::new()),
    );
    let pm = Arc::new(PeerManager::with_weak_listener(
        net_config,
        local_id,
        controller.as_peer_listener(),
    ));
    controller.set_peer_manager(pm);
    let weak_controller = Arc::downgrade(&controller);
    drop(handles);
    drop(controller);
    (weak_controller, weak_engine)
}

#[test]
fn dropped_controllers_free_their_storage_engines_and_threads() {
    let dirs: Vec<_> = (0..CONTROLLERS)
        .map(|_| tempfile::tempdir().unwrap())
        .collect();
    // Warm up once so lazily created process-wide pools are in the baseline.
    let _ = build_and_drop(dirs[0].path(), Uuid::new_v4());
    let baseline = thread_count();

    let dropped: Vec<_> = dirs[1..]
        .iter()
        .map(|d| build_and_drop(d.path(), Uuid::new_v4()))
        .collect();

    // Engine workers exit asynchronously once released; bound the wait.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut now = thread_count();
    while now > baseline && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        now = thread_count();
    }

    let live_controllers = dropped
        .iter()
        .filter(|(c, _)| c.upgrade().is_some())
        .count();
    let live_engines = dropped
        .iter()
        .filter(|(_, e)| e.upgrade().is_some())
        .count();
    assert_eq!(
        live_controllers,
        0,
        "{live_controllers} of {} dropped controllers are still alive: something still \
         holds a strong reference (the peer manager's listener did)",
        dropped.len()
    );
    assert_eq!(
        live_engines, 0,
        "{live_engines} storage engines outlived their dropped controllers"
    );
    assert!(
        now <= baseline,
        "thread count did not return to baseline after dropping {} controllers: \
         baseline {baseline}, now {now} ({} leaked per controller)",
        dropped.len(),
        (now - baseline) as f64 / dropped.len() as f64
    );
}
