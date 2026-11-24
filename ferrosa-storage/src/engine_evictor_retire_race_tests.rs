// ST-114: the free-space evictor must not mark a generation compaction retired.
//
// Included into `engine::tests`, so the engine test helpers (`table_id`,
// `manifest_entry`, `STORE_PREFIX`, `store_holding`) are in scope.

/// An in-memory store whose first HEAD of one generation's object retires that
/// generation, the way a compaction finishing during the evictor's store check
/// does. The retired generation's objects stay in the store until GC, so every
/// HEAD still succeeds.
#[derive(Debug)]
struct RetireOnHeadStore {
    inner: object_store::memory::InMemory,
    table_dir: std::path::PathBuf,
    gen: String,
    fired: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for RetireOnHeadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetireOnHeadStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for RetireOnHeadStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn head(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<object_store::ObjectMeta> {
        use std::sync::atomic::Ordering::SeqCst;
        let ours = location
            .filename()
            .is_some_and(|name| name.starts_with(&format!("{}-", self.gen)));
        if ours && !self.fired.swap(true, SeqCst) {
            assert!(crate::compaction::retire::retire(&self.table_dir, &self.gen));
        }
        self.inner.head(location).await
    }
    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Production, 2026-11-16/17 (ferrosa 0.36.0): compaction retired a generation
/// after the evictor had picked it from its pass-start manifest snapshot. The
/// store check skips components that are no longer on disk, so it passed
/// vacuously; the evictor then wrote a `<gen>.evicted` marker with no
/// artifacts for a generation that no longer existed, and the next start
/// refused ("evicted SSTable generation(s) are neither local nor resolvable
/// from the object store ... not in the manifest").
#[tokio::test]
async fn the_evictor_does_not_mark_a_generation_compaction_retired_mid_pass() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.local_cache_max_bytes = 1; // the generation is over the cap
    let engine = StorageEngine::new(config, None).unwrap();
    let table_id = table_id().to_string();
    let table_dir = dir.path().join("sstables").join(&table_id);
    std::fs::create_dir_all(&table_dir).unwrap();
    std::fs::write(table_dir.join("1-Data.db"), vec![1u8; 80]).unwrap();
    std::fs::write(table_dir.join("1-Filter.db"), vec![1u8; 8]).unwrap();
    let mut manifest = crate::manifest::Manifest::new();
    manifest.add_sstable(&table_id, manifest_entry("1", 80));
    let store = Arc::new(RetireOnHeadStore {
        inner: store_holding(dir.path(), &manifest).await,
        table_dir: table_dir.clone(),
        gen: "1".to_string(),
        fired: std::sync::atomic::AtomicBool::new(false),
    });

    let evicted = engine
        .enforce_uploaded_sstable_cache_limit(&manifest, store.as_ref(), STORE_PREFIX)
        .await
        .unwrap();

    assert!(
        store.fired.load(std::sync::atomic::Ordering::SeqCst),
        "the store check must have run, and retired the generation during it"
    );
    assert_eq!(evicted, 0, "a retired generation is not an eviction");
    assert!(
        !StorageEngine::evicted_marker_path(&table_dir, "1").exists(),
        "no eviction marker for a generation compaction retired"
    );

    // Compaction's manifest no longer lists the input. The next start must
    // find nothing to refuse over.
    let after = crate::manifest::Manifest::new();
    after
        .save_unconditional(store.as_ref(), STORE_PREFIX)
        .await
        .unwrap();
    let store: Arc<dyn object_store::ObjectStore> = store;
    let registered = StorageEngine::register_evicted_sstables_remote_in(
        dir.path(),
        Some((store, STORE_PREFIX.to_string())),
    )
    .await
    .expect("startup must not refuse over a generation compaction retired");
    assert_eq!(registered, 0);
}

/// The same race with the retirement still in flight when the evictor reaches
/// the generation: the evictor takes the generation's guard, so it waits for
/// the sweep to finish instead of racing it, then sees the generation gone.
#[tokio::test]
async fn the_evictor_waits_for_an_in_flight_retirement_then_skips() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.local_cache_max_bytes = 1;
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    let table_id = table_id().to_string();
    let table_dir = dir.path().join("sstables").join(&table_id);
    std::fs::create_dir_all(&table_dir).unwrap();
    std::fs::write(table_dir.join("1-Data.db"), vec![1u8; 80]).unwrap();
    let mut manifest = crate::manifest::Manifest::new();
    manifest.add_sstable(&table_id, manifest_entry("1", 80));
    let store = store_holding(dir.path(), &manifest).await;

    // A retirement holds the guard; it finishes (removing Data.db and marking
    // the slot retired) only once released.
    let slot = crate::generation_guard::slot(&table_dir, "1");
    let mut in_flight = slot.lock();
    let pass = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                engine
                    .enforce_uploaded_sstable_cache_limit(&manifest, &store, STORE_PREFIX)
                    .await
                    .unwrap()
            })
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(
        !StorageEngine::evicted_marker_path(&table_dir, "1").exists(),
        "no marker may be written while a retirement of the generation holds its guard"
    );
    std::fs::remove_file(table_dir.join("1-Data.db")).unwrap();
    in_flight.mark_retired();
    drop(in_flight);

    assert_eq!(pass.join().unwrap(), 0);
    assert!(!StorageEngine::evicted_marker_path(&table_dir, "1").exists());
}
