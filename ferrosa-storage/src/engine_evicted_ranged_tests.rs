// ST-51: queries over an evicted SSTable read only the bytes they need.
//
// Included into `engine::tests` (see `engine_wiring_tests.rs`), so the engine
// test helpers (`evicting_s3_engine`, `make_key`, `table_id`, ...) are in scope.

/// Wraps an in-memory store and counts what the engine fetches from it, so a
/// test can assert on bytes moved rather than on timing.
#[derive(Debug, Default)]
struct CountingStore {
    inner: object_store::memory::InMemory,
    /// Bytes returned by GETs of `*-Data.db` objects (ranged or whole).
    data_db_bytes: std::sync::atomic::AtomicU64,
    /// Number of GETs of `*-Data.db` objects.
    data_db_gets: std::sync::atomic::AtomicU64,
    /// Bytes returned by GETs of every other object.
    other_bytes: std::sync::atomic::AtomicU64,
    /// Added latency per GET, so concurrent readers overlap.
    get_delay_ms: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CountingStore {
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
        use std::sync::atomic::Ordering::SeqCst;
        let head_only = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head_only {
            let bytes = (result.range.end - result.range.start) as u64;
            if location.as_ref().ends_with("-Data.db") {
                self.data_db_bytes.fetch_add(bytes, SeqCst);
                self.data_db_gets.fetch_add(1, SeqCst);
            } else {
                self.other_bytes.fetch_add(bytes, SeqCst);
            }
            let delay = self.get_delay_ms.load(SeqCst);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }
        Ok(result)
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

/// 32 KiB of incompressible bytes, deterministic per `seed`, so `Data.db` is
/// about as large as the rows written.
fn incompressible_value(seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..32 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

const RANGED_ROWS: usize = 400;

/// A flushed, uploaded and evicted SSTable of `RANGED_ROWS` x 32 KiB rows
/// (`Data.db` ~ 13 MB), over a counting store. `compression` is the table's
/// `compression.class` ("lz4" or "none"). Pooled reader and fd dropped, so the
/// next read reopens the evicted generation.
struct BigEvicted {
    engine: StorageEngine,
    store: Arc<CountingStore>,
    table_dir: std::path::PathBuf,
    gen: String,
    prefix: String,
    data_dir: std::path::PathBuf,
}

async fn big_evicted_sstable(dir: &std::path::Path, prefix: &str, compression: &str) -> BigEvicted {
    let counting = Arc::new(CountingStore::default());
    let store: Arc<dyn object_store::ObjectStore> = counting.clone();
    // `evicting_s3_engine` with a commit-log segment big enough for 32 KiB rows.
    let mut config = StorageEngineConfig::test_config(dir);
    config.local_cache_max_bytes = 1;
    config.commit_log.segment_size = 8 * 1024 * 1024;
    config.object_store = Some(crate::upload::ObjectStoreConfig {
        prefix: prefix.to_string(),
        ..crate::upload::ObjectStoreConfig::test_config()
    });
    let engine = StorageEngine::new_with_upload_store(
        config,
        Arc::clone(&store),
        prefix.to_string(),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    StorageEngine::install_s3_file_read_rehydration_hook(
        dir.to_path_buf(),
        prefix.to_string(),
        Arc::clone(&store),
    );
    let mut schema = test_schema();
    schema
        .extensions
        .insert("compression.class".to_owned(), compression.to_owned());
    engine.register_table(schema).unwrap();
    let tid = table_id();
    for i in 0..RANGED_ROWS {
        engine
            .write(
                &tid,
                &make_key(&format!("k{i:04}")),
                make_row(&incompressible_value(i as u64), 1000),
                1000,
            )
            .unwrap();
    }
    engine.flush(&tid).unwrap();
    assert!(engine.sync_sstables_to_s3().await.unwrap() >= 1);
    let table_dir = engine.table_sstable_dir(&tid);
    let evicted = StorageEngine::evicted_generations(&dir.join("sstables"));
    let gen = evicted
        .values()
        .next()
        .and_then(|gens| gens.iter().next())
        .expect("the sync recorded an eviction")
        .clone();
    assert!(StorageEngine::list_generations_in_dir(&table_dir).is_empty());
    engine.evict_pooled_reader_for_test(&tid, gen.parse().unwrap());
    ferrosa_sstable::io::evict_global_fd_for_test(table_dir.join(format!("{gen}-Data.db")));
    BigEvicted {
        engine,
        store: counting,
        table_dir,
        gen,
        prefix: prefix.to_string(),
        data_dir: dir.to_path_buf(),
    }
}

impl BigEvicted {
    /// The size of the generation's `Data.db` object in the store.
    async fn data_db_object_len(&self) -> u64 {
        use object_store::ObjectStore;
        let hex = crate::upload::manager::hex_prefix_for(&self.gen);
        let key = crate::upload::manager::sstable_object_key(
            &self.prefix,
            &hex,
            &table_id().to_string(),
            &self.gen,
            "Data.db",
        );
        self.store.head(&key).await.unwrap().size as u64
    }

    fn expect_row(&self, i: usize) {
        let partition = self
            .engine
            .read(&table_id(), &make_key(&format!("k{i:04}")))
            .unwrap_or_else(|e| panic!("row {i} read failed: {e}"))
            .unwrap_or_else(|| panic!("row {i} of the evicted SSTable is missing"));
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(incompressible_value(i as u64).as_slice()),
            "row {i} must read back byte for byte"
        );
    }
}

/// ST-51 / the 2026-09-30 incident: a point read of one key waited for the
/// whole 479 MB SSTable to download. It must fetch a few pages instead, and
/// must not leave `Data.db` on local disk.
#[tokio::test(flavor = "multi_thread")]
async fn a_point_read_of_an_evicted_sstable_fetches_only_a_few_pages() {
    for compression in ["lz4", "none"] {
        let dir = tempfile::tempdir().unwrap();
        let big = big_evicted_sstable(dir.path(), "test-ranged-point", compression).await;
        let data_len = big.data_db_object_len().await;
        assert!(data_len > 8 * 1024 * 1024, "precondition: Data.db is large");
        let before = big
            .store
            .data_db_bytes
            .load(std::sync::atomic::Ordering::SeqCst);

        big.expect_row(123);

        let fetched = big
            .store
            .data_db_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
            - before;
        println!("[{compression}] point read fetched {fetched} Data.db bytes of {data_len}");
        assert!(
            fetched <= 3 * 1024 * 1024,
            "[{compression}] a point read fetched {fetched} Data.db bytes of {data_len}; \
             it must fetch only the pages it needs"
        );
        assert!(
            !big.table_dir
                .join(format!("{}-Data.db", big.gen))
                .exists(),
            "[{compression}] a ranged read must not materialise Data.db locally"
        );
        assert!(
            StorageEngine::evicted_marker_path(&big.table_dir, &big.gen).exists(),
            "[{compression}] the generation is still evicted, so its marker stays"
        );
        big.engine.shutdown().unwrap();
    }
}

impl BigEvicted {
    /// Object-store key of one of the generation's components.
    fn component_key(&self, component: &str) -> object_store::path::Path {
        let hex = crate::upload::manager::hex_prefix_for(&self.gen);
        crate::upload::manager::sstable_object_key(
            &self.prefix,
            &hex,
            &table_id().to_string(),
            &self.gen,
            component,
        )
    }

    /// Flip one bit of the stored `Data.db` object at `offset`.
    async fn corrupt_data_db_at(&self, offset: usize) {
        use object_store::ObjectStore;
        let key = self.component_key("Data.db");
        let mut bytes = self
            .store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        bytes[offset] ^= 0x01;
        self.store
            .put(&key, object_store::PutPayload::from(bytes))
            .await
            .unwrap();
    }

    fn local_data_db(&self) -> std::path::PathBuf {
        self.table_dir.join(format!("{}-Data.db", self.gen))
    }
}

/// Every row value in `partitions`, to compare with what was written.
fn partition_values(partitions: &[Partition]) -> std::collections::BTreeSet<Vec<u8>> {
    partitions
        .iter()
        .map(|p| p.rows[0].cells[0].1.value.clone().expect("live cell"))
        .collect()
}

fn all_written_values() -> std::collections::BTreeSet<Vec<u8>> {
    (0..RANGED_ROWS as u64).map(incompressible_value).collect()
}

/// Range, token-range and walking reads of a ranged-mode SSTable return every
/// row, byte for byte, and still do not materialise `Data.db`.
#[tokio::test(flavor = "multi_thread")]
async fn scans_over_a_ranged_sstable_return_every_row() {
    for compression in ["lz4", "none"] {
        let dir = tempfile::tempdir().unwrap();
        let big = big_evicted_sstable(dir.path(), "test-ranged-scan", compression).await;
        let tid = table_id();

        let by_key = big
            .engine
            .read_range(&tid, None, None, RANGED_ROWS + 10)
            .unwrap();
        assert_eq!(by_key.len(), RANGED_ROWS, "[{compression}] read_range");
        assert_eq!(partition_values(&by_key), all_written_values());

        let by_token = big
            .engine
            .read_token_range(&tid, i64::MIN, i64::MAX, RANGED_ROWS + 10)
            .unwrap();
        assert_eq!(
            by_token.len(),
            RANGED_ROWS,
            "[{compression}] read_token_range"
        );
        assert_eq!(partition_values(&by_token), all_written_values());

        let mut walked = 0usize;
        big.engine
            .walk_token_range(&tid, i64::MIN, i64::MAX, |_| {
                walked += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(walked, RANGED_ROWS, "[{compression}] walk_token_range");

        assert!(
            !big.local_data_db().exists(),
            "[{compression}] scans must not materialise Data.db"
        );
        big.engine.shutdown().unwrap();
    }
}

/// A flipped byte in the stored object must surface as an error from the
/// checksum the reader already verifies, never as a wrong row. Compressed
/// tables fail the chunk CRC; uncompressed ones fail `CRC.db`.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupted_ranged_byte_is_an_error_never_a_wrong_row() {
    for compression in ["lz4", "none"] {
        let dir = tempfile::tempdir().unwrap();
        let big = big_evicted_sstable(dir.path(), "test-ranged-corrupt", compression).await;
        let len = big.data_db_object_len().await as usize;
        big.corrupt_data_db_at(len / 2).await;

        // Every row up to the first failure must be right. The first read that
        // touches the corrupted page fails and the read path then quarantines
        // the SSTable (existing, designed behaviour), so reads stop at the
        // first error: what a quarantined table serves afterwards is not the
        // ranged reader's concern.
        let mut failed_at = None;
        for i in 0..RANGED_ROWS {
            match big.engine.read(&table_id(), &make_key(&format!("k{i:04}"))) {
                Err(_) => {
                    failed_at = Some(i);
                    break;
                }
                Ok(Some(partition)) => assert_eq!(
                    partition.rows[0].cells[0].1.value.as_deref(),
                    Some(incompressible_value(i as u64).as_slice()),
                    "[{compression}] row {i} came back wrong instead of failing"
                ),
                Ok(None) => panic!("[{compression}] row {i} silently vanished before any error"),
            }
        }
        assert!(
            failed_at.is_some(),
            "[{compression}] reading every row of a corrupted object must fail at least once"
        );
        big.engine.shutdown().unwrap();
    }
}

/// ST-41 on the ranged path: once the `Data.db` object is gone, a page that
/// was never fetched is an error. A key read must not answer `None` and a
/// range read must not return fewer rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_ranged_read_fails_loud_when_the_data_object_disappears() {
    use object_store::ObjectStore;
    let dir = tempfile::tempdir().unwrap();
    let big = big_evicted_sstable(dir.path(), "test-ranged-gone", "lz4").await;
    big.expect_row(0); // open in ranged mode; caches only the first pages
    big.store
        .delete(&big.component_key("Data.db"))
        .await
        .unwrap();

    let far = big
        .engine
        .read(&table_id(), &make_key(&format!("k{:04}", RANGED_ROWS - 1)));
    assert!(far.is_err(), "a key read must fail, got {far:?}");
    big.engine
        .read_range(&table_id(), None, None, RANGED_ROWS + 10)
        .expect_err("a range read must fail, not return fewer rows");
    big.engine.shutdown().unwrap();
}

/// Compaction reads whole files, so it still rehydrates its evicted inputs in
/// full, and the rows survive it.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_over_a_ranged_input_rehydrates_it_and_keeps_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let big = big_evicted_sstable(dir.path(), "test-ranged-compact", "lz4").await;
    big.expect_row(7); // ranged mode first
    assert!(!big.local_data_db().exists());
    let tid = table_id();
    big.engine
        .write(&tid, &make_key("zz-late"), make_row(b"late", 2000), 2000)
        .unwrap();
    big.engine.flush(&tid).unwrap();

    big.engine.force_compact_all();

    let rows = big
        .engine
        .read_range(&tid, None, None, RANGED_ROWS + 10)
        .unwrap();
    assert_eq!(rows.len(), RANGED_ROWS + 1, "every row survives compaction");
    big.expect_row(300);
    big.engine.shutdown().unwrap();
}

/// A restart after ranged reads still sees the eviction marker and restores
/// the generation: ranged mode leaves the marker semantics untouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_after_ranged_reads_still_restores_the_marked_generation() {
    let dir = tempfile::tempdir().unwrap();
    let big = big_evicted_sstable(dir.path(), "test-ranged-restart", "lz4").await;
    big.expect_row(11);
    let counting = Arc::clone(&big.store);
    let prefix = big.prefix.clone();
    let data_dir = big.data_dir.clone();
    let marker = StorageEngine::evicted_marker_path(&big.table_dir, &big.gen);
    assert!(marker.exists(), "ranged mode keeps the marker");
    big.engine.shutdown().unwrap();
    drop(big);

    let store: Arc<dyn object_store::ObjectStore> = counting;
    let engine = evicting_s3_engine(&data_dir, &store, &prefix);
    engine.register_table(test_schema()).unwrap();
    let partitions = engine
        .read_range(&table_id(), None, None, RANGED_ROWS + 10)
        .unwrap();
    assert_eq!(partitions.len(), RANGED_ROWS);
    assert_eq!(partition_values(&partitions), all_written_values());
    engine.shutdown().unwrap();
}

// ---- Startup registers evicted generations remote-backed (t_6a2847c8) ----

impl CountingStore {
    fn reset_counters(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.data_db_bytes.store(0, SeqCst);
        self.data_db_gets.store(0, SeqCst);
        self.other_bytes.store(0, SeqCst);
    }
}

/// [`evicting_s3_engine_with_hot_window`] with an explicit cache limit and
/// a fallible constructor.
fn try_restart_engine(
    dir: &std::path::Path,
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    hot_window_secs: u64,
    cache_max_bytes: u64,
) -> ferrosa_common::Result<StorageEngine> {
    let mut config = StorageEngineConfig::test_config(dir);
    config.local_cache_max_bytes = cache_max_bytes;
    config.cache_hot_window_secs = hot_window_secs;
    config.object_store = Some(crate::upload::ObjectStoreConfig {
        prefix: prefix.to_string(),
        ..crate::upload::ObjectStoreConfig::test_config()
    });
    StorageEngine::new_with_upload_store(
        config,
        Arc::clone(store),
        prefix.to_string(),
        &tokio::runtime::Handle::current(),
    )
}

/// The 2026-09-30 incident: a restart downloaded ~345 evicted SSTables before
/// serving, refilling the disk the evictor had just freed. Startup must now
/// fetch index components only, register every generation remote-backed, and
/// still serve every row.
#[tokio::test(flavor = "multi_thread")]
async fn startup_registers_evicted_generations_remote_backed_without_downloading_data() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = tempfile::tempdir().unwrap();
    let big = big_evicted_sstable(dir.path(), "test-startup-remote", "lz4").await;
    let counting = Arc::clone(&big.store);
    let (table_dir, gen) = (big.table_dir.clone(), big.gen.clone());
    let data_len = big.data_db_object_len().await;
    big.engine.shutdown().unwrap();
    drop(big);
    counting.reset_counters();

    let store: Arc<dyn object_store::ObjectStore> = counting.clone();
    let engine = try_restart_engine(dir.path(), &store, "test-startup-remote", 900, 1).unwrap();

    assert_eq!(
        counting.data_db_bytes.load(SeqCst),
        0,
        "startup must not download Data.db ({data_len} bytes)"
    );
    assert_eq!(counting.data_db_gets.load(SeqCst), 0);
    assert!(
        counting.other_bytes.load(SeqCst) < 1024 * 1024,
        "startup fetched {} non-Data bytes; only index components may be fetched",
        counting.other_bytes.load(SeqCst)
    );
    engine.register_table(test_schema()).unwrap();
    let rows = engine
        .read_range(&table_id(), None, None, RANGED_ROWS + 10)
        .unwrap();
    assert_eq!(rows.len(), RANGED_ROWS, "every row is served remote-backed");
    assert_eq!(partition_values(&rows), all_written_values());
    assert!(
        counting.data_db_bytes.load(SeqCst) > 0,
        "the rows came from ranged reads of the object store"
    );
    assert!(
        !table_dir.join(format!("{gen}-Data.db")).exists(),
        "Data.db must still be remote"
    );
    assert!(
        StorageEngine::evicted_marker_path(&table_dir, &gen).exists(),
        "the marker keeps its meaning"
    );
    engine.shutdown().unwrap();
}

/// A marked generation that is neither local nor resolvable remotely must stop
/// startup with an error, not be dropped from its table (the 2026-09-29
/// data-loss shape).
#[tokio::test(flavor = "multi_thread")]
async fn startup_fails_loud_when_an_evicted_generation_is_gone_from_the_store() {
    use object_store::ObjectStore;
    let dir = tempfile::tempdir().unwrap();
    let big = big_evicted_sstable(dir.path(), "test-startup-gone", "lz4").await;
    let counting = Arc::clone(&big.store);
    counting.delete(&big.component_key("Data.db")).await.unwrap();
    big.engine.shutdown().unwrap();
    drop(big);

    let store: Arc<dyn ObjectStore> = counting;
    let outcome = try_restart_engine(dir.path(), &store, "test-startup-gone", 900, 1);

    let err = match outcome {
        Ok(_) => panic!("startup must fail when an evicted generation cannot be resolved"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("evicted"),
        "the error must name the evicted generation: {err}"
    );
}

/// Two evicted tables after a restart; the hot one (read within the window)
/// is fully restored in the background, the cold one stays remote.
async fn restart_with_two_evicted_tables(
    dir: &std::path::Path,
    prefix: &str,
    cache_max_bytes: u64,
) -> (StorageEngine, std::path::PathBuf, std::path::PathBuf) {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    {
        let engine = evicting_s3_engine_with_hot_window(dir, &store, prefix, 0);
        engine.register_table(test_schema()).unwrap();
        engine.register_table(test_schema_2()).unwrap();
        for tid in [table_id(), table_id_2()] {
            engine
                .write(&tid, &make_key("k"), make_row(b"v", 1000), 1000)
                .unwrap();
            engine.flush(&tid).unwrap();
        }
        assert!(engine.sync_sstables_to_s3().await.unwrap() >= 2);
        engine.shutdown().unwrap();
    }
    let engine = try_restart_engine(dir, &store, prefix, 900, cache_max_bytes).unwrap();
    engine.register_table(test_schema()).unwrap();
    engine.register_table(test_schema_2()).unwrap();
    let hot = engine.table_sstable_dir(&table_id());
    let cold = engine.table_sstable_dir(&table_id_2());
    (engine, hot, cold)
}

fn has_local_data(table_dir: &std::path::Path) -> bool {
    !StorageEngine::list_generations_in_dir(table_dir).is_empty()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hot_table_is_restored_in_the_background_and_a_cold_one_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, hot, cold) =
        restart_with_two_evicted_tables(dir.path(), "test-hot-restore", u64::MAX / 4).await;
    engine.set_disk_free_cache_for_test(u64::MAX / 4);
    assert!(!has_local_data(&hot) && !has_local_data(&cold), "precondition");
    engine.read(&table_id(), &make_key("k")).unwrap().expect("hot row");

    let restored = engine.restore_hot_evicted_sstables().await.unwrap();

    assert_eq!(restored, 1, "exactly the hot table's generation is restored");
    assert!(has_local_data(&hot), "the hot table is local again");
    assert!(
        StorageEngine::evicted_generations(&dir.path().join("sstables"))
            .keys()
            .all(|t| *t != table_id().to_string()),
        "the restored generation's marker is cleared"
    );
    assert!(!has_local_data(&cold), "the cold table stays remote-backed");
    assert!(engine.read(&table_id_2(), &make_key("k")).unwrap().is_some());
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn background_restore_stops_when_free_space_nears_the_eviction_target() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, hot, _cold) =
        restart_with_two_evicted_tables(dir.path(), "test-hot-restore-low", u64::MAX / 4).await;
    engine.set_disk_free_cache_for_test(0);
    engine.read(&table_id(), &make_key("k")).unwrap().expect("hot row");

    let restored = engine.restore_hot_evicted_sstables().await.unwrap();

    assert_eq!(restored, 0, "no free space, no restore");
    assert!(!has_local_data(&hot), "the hot table stays remote-backed");
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn background_restore_respects_the_uploaded_cache_limit() {
    let dir = tempfile::tempdir().unwrap();
    // A one-byte cache limit: restoring would immediately re-trigger eviction.
    let (engine, hot, _cold) = restart_with_two_evicted_tables(dir.path(), "test-hot-cache", 1).await;
    engine.set_disk_free_cache_for_test(u64::MAX / 4);
    engine.read(&table_id(), &make_key("k")).unwrap().expect("hot row");

    let restored = engine.restore_hot_evicted_sstables().await.unwrap();

    assert_eq!(restored, 0);
    assert!(!has_local_data(&hot));
    engine.shutdown().unwrap();
}
