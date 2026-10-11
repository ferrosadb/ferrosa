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
