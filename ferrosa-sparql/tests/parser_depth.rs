//! SPARQL text is untrusted, and `spargebra` recurses once per nesting level
//! with no limit: 100,000 nested `(` overflowed a 2 MiB thread stack and
//! aborted the process. Both engine entry points must refuse such a query as a
//! parse error, on a runtime whose threads are tokio-worker sized, and the
//! limit itself must still parse there.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine};
use ferrosa_sparql::error::SparqlError;
use ferrosa_sparql::nesting::MAX_NESTING;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

const DEPTH: usize = 100_000;
const WORKER_STACK: usize = 2 * 1024 * 1024;

fn engine(dir: &TempDir) -> SparqlEngine {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    SparqlEngine::new(
        storage,
        write_path,
        SparqlConfig {
            default_graph: "default".to_string(),
            ..Default::default()
        },
    )
}

/// Run `f` on a current-thread runtime inside a thread with a worker-sized
/// stack, as the HTTP handlers run.
fn on_worker_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(f)
        .expect("spawn")
        .join()
        .expect("the parsing thread must not panic")
}

fn filter_query(depth: usize) -> String {
    // The WHERE group and FILTER account for two levels.
    format!(
        "SELECT * WHERE {{ ?s ?p ?o FILTER({}1{}) }}",
        "(".repeat(depth),
        ")".repeat(depth)
    )
}

fn nested_update(depth: usize) -> String {
    // As for the query: the WHERE group and FILTER are two levels.
    format!(
        "DELETE {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o FILTER({}1{}) }}",
        "(".repeat(depth),
        ")".repeat(depth)
    )
}

#[test]
fn a_deeply_nested_query_is_refused_not_overflowed() {
    let outcome = on_worker_stack(|| {
        let dir = TempDir::new().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(engine(&dir).execute(&filter_query(DEPTH), "default"))
            .map(drop)
    });
    assert!(matches!(outcome, Err(SparqlError::Parse(_))), "{outcome:?}");
}

#[test]
fn a_deeply_nested_update_is_refused_not_overflowed() {
    let outcome = on_worker_stack(|| {
        let dir = TempDir::new().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(engine(&dir).execute_update(&nested_update(DEPTH), "default"))
            .map(drop)
    });
    assert!(matches!(outcome, Err(SparqlError::Parse(_))), "{outcome:?}");
}

/// The limit is safe: the deepest accepted text parses on a worker stack.
#[test]
fn the_deepest_accepted_query_parses_on_a_worker_stack() {
    let query = filter_query(MAX_NESTING - 2);
    assert!(ferrosa_sparql::nesting::check_nesting(&query).is_ok());
    let parsed = on_worker_stack(move || {
        spargebra::SparqlParser::new()
            .parse_query(&query)
            .map(drop)
            .map_err(|e| e.to_string())
    });
    assert_eq!(parsed, Ok(()));
    let update = nested_update(MAX_NESTING - 2);
    assert!(ferrosa_sparql::nesting::check_nesting(&update).is_ok());
    let parsed = on_worker_stack(move || {
        spargebra::SparqlParser::new()
            .parse_update(&update)
            .map(drop)
            .map_err(|e| e.to_string())
    });
    assert_eq!(parsed, Ok(()));
}
