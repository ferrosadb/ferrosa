//! Unit tests for [`super`]: paging, coalescing, cache bound, shared store.

use super::*;
use std::sync::atomic::AtomicU64;

const PAGE: u64 = 4096;

/// An in-memory store that counts non-HEAD GETs and can delay them, so
/// concurrent readers overlap.
#[derive(Debug, Default)]
struct CountingStore {
    inner: object_store::memory::InMemory,
    gets: AtomicU64,
    get_delay_ms: AtomicU64,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let head_only = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head_only {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let delay = self.get_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }
        Ok(result)
    }
    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    counting: Arc<CountingStore>,
    shared: Arc<dyn ObjectStore>,
    reads: Arc<EvictedReadStore>,
    path: PathBuf,
    key: ObjectPath,
    content: Vec<u8>,
}

/// A `pages`-page `Data.db` object in an in-memory store, with ranged reads
/// registered for a fresh data directory.
fn fixture(pages: usize, cache_pages: u64) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let counting = Arc::new(CountingStore::default());
    let shared: Arc<dyn ObjectStore> = counting.clone();
    let content: Vec<u8> = (0..pages as u64 * PAGE).map(|i| (i % 251) as u8).collect();
    let hex = crate::upload::manager::hex_prefix_for("7");
    let key = crate::upload::manager::sstable_object_key("pfx", &hex, "tbl", "7", "Data.db");
    futures::executor::block_on(shared.put(&key, object_store::PutPayload::from(content.clone())))
        .unwrap();
    let reads = register_with_config(
        data_dir.clone(),
        "pfx".to_string(),
        Arc::clone(&shared),
        EvictedReadConfig {
            page_bytes: PAGE,
            cache_bytes: cache_pages * PAGE,
        },
    );
    Fixture {
        path: data_dir.join("sstables").join("tbl").join("7-Data.db"),
        _dir: dir,
        counting,
        shared,
        reads,
        key,
        content,
    }
}

impl Fixture {
    fn read(&self, offset: u64, len: usize) -> Vec<u8> {
        self.reads
            .read_range(&self.path, offset, len)
            .unwrap()
            .expect("the object exists")
    }

    fn expect(&self, offset: u64, len: usize) {
        let o = offset as usize;
        assert_eq!(self.read(offset, len), self.content[o..o + len]);
    }
}

#[test]
fn adjacent_misses_are_fetched_with_one_ranged_get() {
    let f = fixture(10, 100);
    f.expect(2 * PAGE + 10, 3 * PAGE as usize); // pages 2..=5, 4 pages
    assert_eq!(f.reads.gets(), 1, "four adjacent pages, one request");
    f.expect(3 * PAGE, PAGE as usize);
    assert_eq!(f.reads.gets(), 1, "a cached page costs no request");
    f.expect(4 * PAGE, 4 * PAGE as usize); // 4,5 cached; 6,7 missing
    assert_eq!(f.reads.gets(), 2, "only the missing run is fetched");
}

#[test]
fn cached_pages_in_the_middle_split_the_runs() {
    let f = fixture(10, 100);
    f.expect(PAGE, 1); // page 1
    f.expect(3 * PAGE, 1); // page 3
    assert_eq!(f.reads.gets(), 2);
    f.expect(0, 5 * PAGE as usize); // 0, 2, 4 missing; 1, 3 cached
    assert_eq!(f.reads.gets(), 5, "three separate single-page gaps");
}

#[test]
fn concurrent_readers_of_one_page_cause_one_fetch() {
    let f = fixture(10, 100);
    f.counting.get_delay_ms.store(50, Ordering::SeqCst);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                f.expect(5 * PAGE + 100, 200);
            });
        }
    });
    assert_eq!(f.reads.gets(), 1, "eight readers, one fetch");
}

#[test]
fn concurrent_overlapping_runs_neither_deadlock_nor_fetch_a_page_twice() {
    let f = fixture(12, 100);
    f.counting.get_delay_ms.store(20, Ordering::SeqCst);
    std::thread::scope(|scope| {
        for start in [0u64, 2, 4, 6] {
            let f = &f;
            scope.spawn(move || f.expect(start * PAGE, 6 * PAGE as usize));
        }
    });
    assert_eq!(
        f.reads.page_bytes_fetched.load(Ordering::Relaxed),
        12 * PAGE,
        "twelve pages exist and none may be fetched twice"
    );
}

#[test]
fn the_page_cache_never_exceeds_its_byte_bound() {
    let f = fixture(10, 3);
    for page in 0..10u64 {
        f.expect(page * PAGE, PAGE as usize);
        assert!(
            f.reads.cached_bytes() <= 3 * PAGE,
            "cache holds {} bytes after page {page}",
            f.reads.cached_bytes()
        );
    }
    // Ten pages went through a three-page cache: it is full, and pages 0..7
    // were evicted, so re-reading page 0 must hit the store again.
    assert_eq!(f.reads.cached_bytes(), 3 * PAGE, "cache is full, not empty");
    let gets = f.reads.gets();
    f.expect(0, PAGE as usize);
    assert_eq!(f.reads.gets(), gets + 1, "page 0 was evicted and refetched");
    // One read wider than the cache still returns every byte.
    f.expect(0, 10 * PAGE as usize);
    assert!(f.reads.cached_bytes() <= 3 * PAGE);
}

#[test]
fn reads_reuse_the_one_shared_store() {
    let f = fixture(10, 100);
    assert!(Arc::ptr_eq(&f.reads.store, &f.shared));
    let count = Arc::strong_count(&f.shared);
    for page in 0..10u64 {
        f.expect(page * PAGE, 10);
    }
    f.reads.object_len(&f.path).unwrap();
    assert!(Arc::ptr_eq(&f.reads.store, &f.shared));
    assert_eq!(
        Arc::strong_count(&f.shared),
        count,
        "reads must not retain or construct store handles"
    );
}

#[test]
fn a_vanished_object_is_an_error_not_a_short_read() {
    let f = fixture(10, 100);
    f.expect(0, 10); // sizes the object and caches page 0
    futures::executor::block_on(f.shared.delete(&f.key)).unwrap();
    f.reads
        .read_range(&f.path, 9 * PAGE, 10)
        .expect_err("an uncached page of a deleted object must fail");
    f.expect(0, 10); // the cached page still serves
}

#[test]
fn a_truncated_object_is_an_error_not_a_short_read() {
    let f = fixture(10, 100);
    f.expect(0, 10);
    let short = f.content[..4 * PAGE as usize].to_vec();
    futures::executor::block_on(f.shared.put(&f.key, object_store::PutPayload::from(short)))
        .unwrap();
    f.reads
        .read_range(&f.path, 9 * PAGE, 10)
        .expect_err("a ranged read past the end of a truncated object must fail");
}

#[test]
fn compressed_pages_follow_chunk_boundaries() {
    // Chunks at 0, 1000, 3000, 9000, 9500 of a 12000-byte object, pages of at
    // least 4096 bytes, each starting on a chunk offset.
    let starts = chunk_aligned_page_starts(&[0, 1000, 3000, 9000, 9500], 4096);
    assert_eq!(starts, vec![0, 9000]);
    let info = ObjectInfo {
        size: 12000,
        page_starts: Some(starts),
    };
    assert_eq!(info.page_range(8999, 4096), (0, 9000));
    assert_eq!(info.page_range(9000, 4096), (9000, 12000));
}

fn put_component(f: &Fixture, component: &str, bytes: &[u8]) {
    let hex = crate::upload::manager::hex_prefix_for("7");
    let key = crate::upload::manager::sstable_object_key("pfx", &hex, "tbl", "7", component);
    futures::executor::block_on(
        f.shared
            .put(&key, object_store::PutPayload::from(bytes.to_vec())),
    )
    .unwrap();
}

#[test]
fn opening_fetches_the_index_components_and_never_data_db() {
    let f = fixture(10, 100);
    put_component(&f, "Partitions.db", b"partitions");
    put_component(&f, "Rows.db", b"rows");
    put_component(&f, "Filter.db", b"filter");
    let dir = f.path.parent().unwrap();

    let outcome = f.reads.fetch_query_components(dir, "7").unwrap();

    assert_eq!(outcome, QueryFetch::Ready);
    assert_eq!(
        std::fs::read(dir.join("7-Partitions.db")).unwrap(),
        b"partitions"
    );
    assert_eq!(std::fs::read(dir.join("7-Filter.db")).unwrap(), b"filter");
    assert!(!dir.join("7-Data.db").exists(), "Data.db stays remote");
    assert_eq!(f.reads.index_bytes_fetched(), 20);
    assert_eq!(f.reads.gets(), 0, "no Data.db page was fetched to open");
}

#[test]
fn a_generation_missing_a_required_component_cannot_be_opened() {
    let f = fixture(10, 100);
    put_component(&f, "Partitions.db", b"partitions");
    put_component(&f, "Rows.db", b"rows"); // no Filter.db
    let dir = f.path.parent().unwrap();

    let outcome = f.reads.fetch_query_components(dir, "7").unwrap();

    assert_eq!(outcome, QueryFetch::Missing);
}
