// Outage 2026-10-10 hardening: startup registration progress and stalls (F5,
// D6) and index backfill over evicted generations after `open` (F6, D7).
//
// Included into `engine::tests` next to `engine_evicted_ranged_tests.rs`, so
// the engine test helpers (`test_schema`, `make_key`, `CountingStore`,
// `CapturedLogs`, `collect_index_results`, ...) are in scope.

/// Engine config over `dir` whose uploaded-SSTable cache is one byte (every
/// sync evicts what it uploaded) and whose compaction never merges the
/// fixture's generations.
fn outage_evicting_config(dir: &std::path::Path, prefix: &str) -> StorageEngineConfig {
    let mut config = StorageEngineConfig::test_config(dir);
    config.local_cache_max_bytes = 1;
    config.compaction.min_threshold = 100_000;
    config.object_store = Some(crate::upload::ObjectStoreConfig {
        prefix: prefix.to_string(),
        ..crate::upload::ObjectStoreConfig::test_config()
    });
    config
}

/// `generations` flushed, uploaded and evicted SSTables of `test_schema`, one
/// row (`k{i}` = "shared") each, over `store`. Returns the evicted gens.
fn evicted_generations_fixture(
    runtime: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    generations: usize,
) -> Vec<String> {
    let engine = StorageEngine::new_with_upload_store(
        outage_evicting_config(dir, prefix),
        Arc::clone(store),
        prefix.to_string(),
        runtime.handle(),
    )
    .unwrap();
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    for i in 0..generations {
        engine
            .write(&tid, &make_key(&format!("k{i:04}")), make_row(b"shared", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
    }
    assert!(runtime.block_on(engine.sync_sstables_to_s3()).unwrap() >= generations);
    engine.shutdown().unwrap();
    let evicted: Vec<String> = StorageEngine::evicted_generations(&dir.join("sstables"))
        .into_values()
        .flatten()
        .collect();
    assert_eq!(evicted.len(), generations, "every generation was evicted");
    evicted
}

/// Run startup registration of `dir`'s evicted generations on a
/// current-thread runtime with every log line captured.
fn register_capturing(
    dir: &std::path::Path,
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    watch: RegistrationWatch,
    hang_guard: std::time::Duration,
) -> (Option<ferrosa_common::Result<usize>>, String) {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            tokio::time::timeout(
                hang_guard,
                StorageEngine::register_evicted_sstables_remote_with(
                    dir,
                    Some((store, prefix.to_string())),
                    watch,
                ),
            )
            .await
            .ok()
        })
    });
    (outcome, logs.text())
}

/// T-F5-4 (2026-10-10): node1 spent minutes registering evicted generations
/// with nothing in the log, and two operators restarted it as hung. 200
/// evicted generations (registration measured at about 8 s here, dominated by
/// the per-component fsyncs, so it spans many 50 ms reporting intervals) must
/// produce progress lines that
/// carry done/total, bytes and rate, with `done` advancing, and a final
/// summary that names every generation.
#[test]
fn startup_registration_reports_progress() {
    const GENERATIONS: usize = 200;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let counting = Arc::new(CountingStore::default());
    let store: Arc<dyn object_store::ObjectStore> = counting.clone();
    evicted_generations_fixture(&runtime, dir.path(), &store, "t-f5-4", GENERATIONS);
    counting.reset_counters();

    let (outcome, text) = register_capturing(
        dir.path(),
        store,
        "t-f5-4",
        RegistrationWatch {
            progress_every: std::time::Duration::from_millis(50),
            stall_after: std::time::Duration::from_secs(30),
        },
        std::time::Duration::from_secs(60),
    );

    let registered = outcome
        .expect("registration finished")
        .expect("registration succeeded");
    assert_eq!(registered, GENERATIONS);
    let progress: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("registering evicted SSTables: progress"))
        .collect();
    assert!(progress.len() >= 2, "expected several progress lines:\n{text}");
    let mut last_done = 0usize;
    for line in &progress {
        assert!(line.contains(&format!("total={GENERATIONS}")), "{line}");
        assert!(line.contains("bytes="), "{line}");
        assert!(line.contains("rate_per_sec="), "{line}");
        assert!(line.contains("elapsed_ms="), "{line}");
        assert!(line.contains("failures=0"), "{line}");
        let done: usize = line
            .split("done=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("no done= value in {line}"));
        assert!(done >= last_done && done <= GENERATIONS, "{line}");
        last_done = done;
    }
    let summary = text
        .lines()
        .find(|l| l.contains("evicted SSTables registered as remote-backed"))
        .unwrap_or_else(|| panic!("no summary line:\n{text}"));
    assert!(
        summary.contains(&format!("registered={GENERATIONS}")),
        "{summary}"
    );
}

/// Wraps a store; every GET (and HEAD) of an SSTable component never answers.
/// The manifest still loads.
#[derive(Debug)]
struct NeverAnsweringComponents(Arc<dyn object_store::ObjectStore>);

impl std::fmt::Display for NeverAnsweringComponents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NeverAnsweringComponents")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for NeverAnsweringComponents {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.0.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref().ends_with(".db") {
            futures::future::pending::<()>().await;
        }
        self.0.get_opts(location, options).await
    }
    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.0.delete(location).await
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.0.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(prefix).await
    }
    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.0.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.0.copy_if_not_exists(from, to).await
    }
}

/// T-F5-5: a store that never answers must make registration fail with a
/// stall error naming the requests in flight, the elapsed time and the last
/// progress, instead of hanging startup forever.
#[test]
fn stalled_startup_registration_fails_loudly() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let memory: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    evicted_generations_fixture(&runtime, dir.path(), &memory, "t-f5-5", 3);
    let stalling: Arc<dyn object_store::ObjectStore> =
        Arc::new(NeverAnsweringComponents(memory));

    let (outcome, text) = register_capturing(
        dir.path(),
        stalling,
        "t-f5-5",
        RegistrationWatch {
            progress_every: std::time::Duration::from_millis(50),
            stall_after: std::time::Duration::from_millis(500),
        },
        std::time::Duration::from_secs(10),
    );

    let err = match outcome {
        None => panic!("registration hung against a store that never answers:\n{text}"),
        Some(Ok(n)) => panic!("registration claimed {n} generations from a silent store"),
        Some(Err(e)) => e.to_string(),
    };
    assert!(err.contains("stalled"), "{err}");
    assert!(err.contains("3 request(s) in flight"), "{err}");
    assert!(err.contains("0/3 generations"), "{err}");
    assert!(err.contains("elapsed"), "{err}");
    assert!(err.contains("last progress"), "{err}");
}

// ---- F6: backfill over evicted generations after `open` -------------------

/// Wait (bounded) until `index_name` is current or failed; return its state.
/// The bound is a hang guard: a backfill over a few tiny SSTables takes
/// milliseconds.
fn wait_for_index_settled(
    engine: &StorageEngine,
    tid: &TableId,
    index_name: &str,
) -> crate::index::IndexState {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let state = engine
            .index_tracker()
            .get_state(tid.keyspace(), tid.table(), index_name)
            .expect("index is tracked");
        let settled = matches!(
            state.status,
            crate::index::IndexStatus::Current | crate::index::IndexStatus::Failed { .. }
        ) && (state.pending_sstables.is_empty()
            || matches!(state.status, crate::index::IndexStatus::Failed { .. }));
        if settled || std::time::Instant::now() >= deadline {
            return state;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// A node directory as the 2026-10-10 restart found it: one generation
/// flushed, uploaded and evicted (`k0`..`k4`), and two rows (`k5`, `k6`) only
/// in commit-log segments. The engine is dropped, not shut down, so the
/// segments survive. Returns the evicted generation.
fn evicted_generation_with_commit_log(
    runtime: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> String {
    let engine = StorageEngine::new_with_upload_store(
        outage_evicting_config(dir, prefix),
        Arc::clone(store),
        prefix.to_string(),
        runtime.handle(),
    )
    .unwrap();
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    for i in 0..5 {
        engine
            .write(&tid, &make_key(&format!("k{i}")), make_row(b"shared", 1000), 1000)
            .unwrap();
    }
    engine.flush(&tid).unwrap();
    assert!(runtime.block_on(engine.sync_sstables_to_s3()).unwrap() >= 1);
    assert!(
        StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid)).is_empty(),
        "precondition: the flushed generation is evicted"
    );
    for key in ["k5", "k6"] {
        engine
            .write(&tid, &make_key(key), make_row(b"shared", 1000), 1000)
            .unwrap();
    }
    drop(engine);
    let evicted: Vec<String> = StorageEngine::evicted_generations(&dir.join("sstables"))
        .into_values()
        .flatten()
        .collect();
    assert_eq!(evicted.len(), 1);
    evicted[0].clone()
}

/// Reopen `dir` through [`StorageEngine::open`] against `store`, the
/// constructor a node with commit-log segments starts through.
fn try_reopen_engine(
    runtime: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> ferrosa_common::Result<StorageEngine> {
    let (engine, _pending) = StorageEngine::open_with_upload_store(
        outage_evicting_config(dir, prefix),
        Arc::clone(store),
        runtime.handle(),
    )?;
    Ok(engine)
}

fn partition_keys_and_values(partitions: &[Partition]) -> std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> {
    partitions
        .iter()
        .map(|p| {
            (
                p.key.key.as_bytes().to_vec(),
                p.rows[0].cells[0].1.value.as_ref().expect("live cell").to_vec(),
            )
        })
        .collect()
}

/// T-F6-1 (2026-10-10): after the restart, several secondary indexes stayed
/// failed with `open data: No such file or directory`: the backfill opened
/// `Data.db` of an evicted generation directly. Reopened through `open` with
/// commit-log segments and an evicted generation, a backfill must make the
/// index current, the rows read through the index must equal the base rows,
/// and the evicted `Data.db` must be read by ranged reads (not downloaded),
/// as queries read it.
#[test]
fn backfill_after_open_builds_index_from_evicted_generation() {
    use ferrosa_index::IndexKey;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _context = runtime.enter();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gen = evicted_generation_with_commit_log(&runtime, dir.path(), &store, "t-f6-1");
    let engine = try_reopen_engine(&runtime, dir.path(), &store, "t-f6-1").unwrap();
    let tid = table_id();
    assert!(
        engine.read(&tid, &make_key("k5")).unwrap().is_some(),
        "the commit log replays the unflushed rows"
    );

    engine
        .add_index(&tid, "val_idx", 0, ferrosa_index::IndexType::BTree)
        .unwrap();
    let state = wait_for_index_settled(&engine, &tid, "val_idx");

    assert!(
        matches!(state.status, crate::index::IndexStatus::Current),
        "the backfill must make the index current: {state:?}"
    );
    let through_index =
        collect_index_results(&engine, &tid, "val_idx", &IndexKey(b"shared".to_vec())).unwrap();
    let base = engine.read_range(&tid, None, None, 100).unwrap();
    assert_eq!(base.len(), 7, "k0..k4 evicted plus k5, k6 replayed");
    assert_eq!(
        partition_keys_and_values(&through_index),
        partition_keys_and_values(&base),
        "rows through the index equal the base rows"
    );
    let table_dir = engine.table_sstable_dir(&tid);
    assert!(
        !table_dir.join(format!("{gen}-Data.db")).exists(),
        "the backfill reads the evicted Data.db by ranged reads, as queries do"
    );
    assert!(StorageEngine::evicted_marker_path(&table_dir, &gen).exists());
    engine.shutdown().unwrap();
}

/// T-F6-2: when the evicted generation's object is gone from the store, the
/// backfill must record a distinct, typed "evicted generation unavailable"
/// failure, not the generic `open data: No such file or directory`.
#[test]
fn backfill_of_deleted_object_reports_evicted_unavailable() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _context = runtime.enter();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gen = evicted_generation_with_commit_log(&runtime, dir.path(), &store, "t-f6-2");
    let engine = try_reopen_engine(&runtime, dir.path(), &store, "t-f6-2").unwrap();
    let tid = table_id();
    let key = crate::upload::manager::sstable_object_key(
        "t-f6-2",
        &crate::upload::manager::hex_prefix_for(&gen),
        &tid.to_string(),
        &gen,
        "Data.db",
    );
    runtime.block_on(store.delete(&key)).unwrap();

    engine
        .add_index(&tid, "val_idx", 0, ferrosa_index::IndexType::BTree)
        .unwrap();
    let state = wait_for_index_settled(&engine, &tid, "val_idx");

    let crate::index::IndexStatus::Failed { error, kind, .. } = &state.status else {
        panic!("a backfill over a deleted object must fail: {state:?}");
    };
    assert_eq!(
        *kind,
        crate::index::BuildFailureKind::EvictedUnavailable,
        "the tracker records the typed kind: {error}"
    );
    assert!(
        error.contains("evicted generation") && error.contains("unavailable"),
        "the failure must say the evicted generation is unavailable: {error}"
    );
    assert!(error.contains(&gen), "{error}");
    assert!(
        !error.contains("No such file or directory"),
        "not the generic I/O error: {error}"
    );
    engine.shutdown().unwrap();
}

/// T-F6-3: `StorageEngine::new` installs the whole-file rehydration hook;
/// `open` did not, so a whole-file read of an evicted component after `open`
/// failed instead of rehydrating from the store.
#[test]
fn open_installs_the_rehydration_hook() {
    use object_store::ObjectStore;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _context = runtime.enter();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let gen = evicted_generation_with_commit_log(&runtime, dir.path(), &store, "t-f6-3");
    let engine = try_reopen_engine(&runtime, dir.path(), &store, "t-f6-3").unwrap();
    let tid = table_id();
    let local = engine.table_sstable_dir(&tid).join(format!("{gen}-Data.db"));
    assert!(!local.exists(), "precondition: Data.db is evicted");

    let restored = ferrosa_sstable::io::rehydrate_file(&local).unwrap();

    assert!(restored, "a whole-file read after open must rehydrate");
    let key = crate::upload::manager::sstable_object_key(
        "t-f6-3",
        &crate::upload::manager::hex_prefix_for(&gen),
        &tid.to_string(),
        &gen,
        "Data.db",
    );
    let object = runtime
        .block_on(async { store.get(&key).await.unwrap().bytes().await })
        .unwrap();
    assert_eq!(std::fs::read(&local).unwrap(), object.to_vec());
    engine.shutdown().unwrap();
}

/// Coordinator follow-up to T-F6 (2026-10-10 node1): started through `open`
/// with nothing evicted, the engine evicted `agent_memory.schema_version` at
/// runtime, and a plain single-partition SELECT then failed with
/// `outcome=NotOwned` / `missing required Data.db`: only `new` (or boot-time
/// registration of already-evicted generations) registered the evicted-read
/// store. A generation evicted after an `open` start must stay readable by an
/// ordinary query (values asserted) and buildable by an index backfill.
#[test]
fn open_engine_reads_and_backfills_a_generation_evicted_at_runtime() {
    use ferrosa_index::IndexKey;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _context = runtime.enter();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let engine = try_reopen_engine(&runtime, dir.path(), &store, "t-f6-runtime").unwrap();
    assert!(
        StorageEngine::evicted_generations(&dir.path().join("sstables")).is_empty(),
        "precondition: nothing is evicted at startup"
    );
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    for i in 0..5 {
        engine
            .write(
                &tid,
                &make_key(&format!("k{i}")),
                make_row(format!("v{i}").as_bytes(), 1000),
                1000,
            )
            .unwrap();
    }
    engine.flush(&tid).unwrap();
    assert!(runtime.block_on(engine.sync_sstables_to_s3()).unwrap() >= 1);
    let table_dir = engine.table_sstable_dir(&tid);
    let gen = StorageEngine::evicted_generations(&dir.path().join("sstables"))
        .into_values()
        .flatten()
        .next()
        .expect("the sync evicted the generation at runtime");
    assert!(!table_dir.join(format!("{gen}-Data.db")).exists());
    // Drop the pooled reader and fd so the next read reopens the evicted
    // generation, as a read long after the eviction does.
    engine.evict_pooled_reader_for_test(&tid, gen.parse().unwrap());
    ferrosa_sstable::io::evict_global_fd_for_test(table_dir.join(format!("{gen}-Data.db")));

    for i in 0..5 {
        let partition = engine
            .read(&tid, &make_key(&format!("k{i}")))
            .unwrap_or_else(|e| panic!("k{i}: a plain read of the evicted generation failed: {e}"))
            .unwrap_or_else(|| panic!("k{i} vanished"));
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(format!("v{i}").as_bytes()),
            "k{i} reads back its value"
        );
    }

    engine
        .add_index(&tid, "val_idx", 0, ferrosa_index::IndexType::BTree)
        .unwrap();
    let state = wait_for_index_settled(&engine, &tid, "val_idx");
    assert!(
        matches!(state.status, crate::index::IndexStatus::Current),
        "the backfill over the runtime-evicted generation must complete: {state:?}"
    );
    let through_index =
        collect_index_results(&engine, &tid, "val_idx", &IndexKey(b"v3".to_vec())).unwrap();
    assert_eq!(
        partition_keys_and_values(&through_index),
        partition_keys_and_values(&[engine.read(&tid, &make_key("k3")).unwrap().unwrap()]),
        "the index finds k3 by its value"
    );
    engine.shutdown().unwrap();
}
