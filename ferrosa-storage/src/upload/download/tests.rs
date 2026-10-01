use super::*;
use async_trait::async_trait;
use object_store::{
    ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i % 251) as u8 ^ (i / 251 % 7) as u8)
        .collect()
}

fn cfg(part_bytes: u64, part_concurrency: usize) -> DownloadConfig {
    DownloadConfig {
        part_bytes,
        part_concurrency,
        restore_concurrency: 2,
        retry_backoff: Duration::from_millis(1),
    }
}

fn key() -> ObjectPath {
    ObjectPath::from("p/ab/ks.t/7/7-Data.db")
}

/// An in-memory store whose GETs can be scripted: per-start failures,
/// recorded requests, in-flight tracking, size lies and mid-download
/// replacement of the object.
struct ScriptedStore {
    inner: object_store::memory::InMemory,
    /// Start offset of each GET (`None` = whole-object GET), in arrival order.
    requests: Mutex<Vec<Option<usize>>>,
    /// start -> (failures left, answer 429 instead of a generic error)
    failures: Mutex<HashMap<usize, (u32, bool)>>,
    delay: Duration,
    inflight: AtomicUsize,
    /// Highest in-flight GET count ever seen.
    max_inflight: AtomicUsize,
    /// Highest in-flight count seen by a request that began after a failure.
    max_inflight_after_failure: AtomicUsize,
    failed_once: AtomicBool,
    /// After the first GET, replace the object with this (same size, new etag).
    swap_after_first: Mutex<Option<Bytes>>,
    /// Serve whole-object GETs with half the bytes while claiming the full size.
    truncate_whole_get: AtomicBool,
}

impl ScriptedStore {
    fn new() -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            requests: Mutex::new(Vec::new()),
            failures: Mutex::new(HashMap::new()),
            delay: Duration::ZERO,
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            max_inflight_after_failure: AtomicUsize::new(0),
            failed_once: AtomicBool::new(false),
            swap_after_first: Mutex::new(None),
            truncate_whole_get: AtomicBool::new(false),
        }
    }

    async fn with_object(body: &[u8]) -> Self {
        let store = Self::new();
        store
            .inner
            .put(&key(), PutPayload::from(body.to_vec()))
            .await
            .unwrap();
        store
    }

    fn fail_start(&self, start: usize, times: u32, rate_limited: bool) {
        self.failures
            .lock()
            .unwrap()
            .insert(start, (times, rate_limited));
    }

    fn requests_for(&self, start: Option<usize>) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|s| **s == start)
            .count()
    }

    fn total_requests(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl fmt::Debug for ScriptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScriptedStore")
    }
}

impl fmt::Display for ScriptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScriptedStore")
    }
}

struct InflightGuard<'a>(&'a AtomicUsize);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for ScriptedStore {
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let start = match &options.range {
            Some(GetRange::Bounded(r)) => Some(r.start),
            _ => None,
        };
        self.requests.lock().unwrap().push(start);
        let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let _guard = InflightGuard(&self.inflight);
        self.max_inflight.fetch_max(now, Ordering::SeqCst);
        if self.failed_once.load(Ordering::SeqCst) {
            self.max_inflight_after_failure
                .fetch_max(now, Ordering::SeqCst);
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if let Some(start) = start {
            let scripted = {
                let mut failures = self.failures.lock().unwrap();
                match failures.get_mut(&start) {
                    Some((left, rate_limited)) if *left > 0 => {
                        *left -= 1;
                        Some(*rate_limited)
                    }
                    _ => None,
                }
            };
            if let Some(rate_limited) = scripted {
                self.failed_once.store(true, Ordering::SeqCst);
                let source = if rate_limited {
                    "Error performing GET: HTTP status client error (429 Too Many Requests)"
                } else {
                    "scripted failure"
                };
                return Err(object_store::Error::Generic {
                    store: "Scripted",
                    source: source.into(),
                });
            }
        }
        let mut result = self.inner.get_opts(location, options).await?;
        if start.is_none() {
            let replacement = self.swap_after_first.lock().unwrap().take();
            if let Some(replacement) = replacement {
                self.inner
                    .put(location, PutPayload::from(replacement))
                    .await?;
            }
            if self.truncate_whole_get.load(Ordering::SeqCst) {
                let all = result.bytes().await?;
                let half = all.slice(..all.len() / 2);
                let meta = self.inner.head(location).await?;
                result = object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(
                        futures::stream::once(async move { Ok(half) }).boxed(),
                    ),
                    range: 0..meta.size,
                    meta,
                    attributes: Default::default(),
                };
            }
        }
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
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

fn leftovers(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

// ---- configuration -------------------------------------------------------

#[test]
fn download_tunables_default_when_unset_or_empty() {
    let defaults = DownloadConfig::parse(None, Some(""), Some("  ")).unwrap();
    assert_eq!(defaults.part_bytes, 16 * 1024 * 1024);
    assert_eq!(defaults.part_concurrency, 4);
    assert_eq!(defaults.restore_concurrency, 4);
}

#[test]
fn download_tunables_parse_positive_integers() {
    let c = DownloadConfig::parse(Some("1048576"), Some("8"), Some("2")).unwrap();
    assert_eq!(
        (c.part_bytes, c.part_concurrency, c.restore_concurrency),
        (1_048_576, 8, 2)
    );
}

#[test]
fn an_invalid_download_tunable_is_rejected_naming_its_variable() {
    for bad in ["0", "-1", "abc", "1.5"] {
        for (parsed, name) in [
            (
                DownloadConfig::parse(Some(bad), None, None),
                "FERROSA_S3_DOWNLOAD_PART_BYTES",
            ),
            (
                DownloadConfig::parse(None, Some(bad), None),
                "FERROSA_S3_DOWNLOAD_PART_CONCURRENCY",
            ),
            (
                DownloadConfig::parse(None, None, Some(bad)),
                "FERROSA_RESTORE_CONCURRENCY",
            ),
        ] {
            let err = parsed.unwrap_err().to_string();
            assert!(err.contains(name), "{bad:?}: {err}");
        }
    }
}

#[test]
fn parts_cover_the_object_exactly() {
    assert_eq!(part_ranges(10, 4), vec![0..4, 4..8, 8..10]);
    assert_eq!(part_ranges(8, 4), vec![0..4, 4..8]);
    assert_eq!(part_ranges(3, 4), vec![0..3]);
    assert!(part_ranges(0, 4).is_empty());
}

// ---- correctness ---------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_large_object_downloads_as_ranged_parts_byte_identical() {
    let body = pattern(5 * 1024 + 123);
    let store = ScriptedStore::with_object(&body).await;
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let written = download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(written, Some(body.len() as u64));
    assert_eq!(std::fs::read(&local).unwrap(), body);
    assert_eq!(leftovers(dir.path()), vec!["7-Data.db".to_string()]);
    // One whole GET reveals the size and serves part 0; parts 1..=5 are ranged.
    assert_eq!(store.requests_for(None), 1);
    for start in [1024, 2048, 3072, 4096, 5120] {
        assert_eq!(store.requests_for(Some(start)), 1, "part at {start}");
    }
    assert_eq!(store.requests_for(Some(0)), 0, "part 0 rides the first GET");
}

#[tokio::test(flavor = "multi_thread")]
async fn parts_download_concurrently_up_to_the_configured_limit() {
    let body = pattern(7 * 1024);
    let mut store = ScriptedStore::with_object(&body).await;
    store.delay = Duration::from_millis(40);
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(std::fs::read(&local).unwrap(), body);
    // Six ranged parts at concurrency 3: a sequential fallback peaks at 1, an
    // unbounded fan-out peaks at 6.
    assert_eq!(store.max_inflight.load(Ordering::SeqCst), 3);
    assert_eq!(
        store.total_requests(),
        7,
        "one whole GET + six ranged parts"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_object_is_one_get() {
    let body = pattern(700);
    let store = ScriptedStore::with_object(&body).await;
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let written = download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(written, Some(700));
    assert_eq!(std::fs::read(&local).unwrap(), body);
    assert_eq!(store.total_requests(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_exactly_one_part_long_is_one_get() {
    let body = pattern(1024);
    let store = ScriptedStore::with_object(&body).await;
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(store.total_requests(), 1);
    assert_eq!(std::fs::read(&local).unwrap(), body);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_object_is_none_and_leaves_no_file() {
    let store = ScriptedStore::new();
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let written = download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(written, None);
    assert!(leftovers(dir.path()).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_part_is_retried_alone() {
    let body = pattern(5 * 1024);
    let store = ScriptedStore::with_object(&body).await;
    store.fail_start(2048, 2, false);
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .expect("a transient part failure must be retried");

    assert_eq!(std::fs::read(&local).unwrap(), body);
    assert_eq!(
        store.requests_for(Some(2048)),
        3,
        "failed part: 2 failures + 1 success"
    );
    for start in [1024, 3072, 4096] {
        assert_eq!(
            store.requests_for(Some(start)),
            1,
            "part at {start} must not be refetched"
        );
    }
    assert_eq!(store.requests_for(None), 1, "the object is not restarted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_that_never_succeeds_fails_loud_and_leaves_nothing() {
    let body = pattern(4 * 1024);
    let store = ScriptedStore::with_object(&body).await;
    store.fail_start(2048, u32::MAX, false);
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let err = download_component(&store, &key(), &local, &cfg(1024, 2))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("2048"), "the error names the part: {err}");
    assert!(
        leftovers(dir.path()).is_empty(),
        "no truncated file may remain: {:?}",
        leftovers(dir.path())
    );
    assert_eq!(store.requests_for(Some(2048)) as u32, MAX_ATTEMPTS);
    assert_eq!(
        store.requests_for(None),
        1,
        "exhausted parts are not retried as a whole object"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_whole_get_fails_loud_and_leaves_nothing() {
    let body = pattern(700);
    let store = ScriptedStore::with_object(&body).await;
    store.truncate_whole_get.store(true, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let err = download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("350"),
        "the error reports the bytes that arrived: {err}"
    );
    assert!(leftovers(dir.path()).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_replaced_mid_download_fails_loud() {
    let body = pattern(4 * 1024);
    let store = ScriptedStore::with_object(&body).await;
    *store.swap_after_first.lock().unwrap() = Some(Bytes::from(
        pattern(4 * 1024).into_iter().rev().collect::<Vec<u8>>(),
    ));
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    let err = download_component(&store, &key(), &local, &cfg(1024, 2))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("changed"), "{err}");
    assert!(leftovers(dir.path()).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rate_limited_part_shrinks_that_objects_concurrency() {
    let body = pattern(10 * 1024);
    let mut store = ScriptedStore::with_object(&body).await;
    store.delay = Duration::from_millis(30);
    store.fail_start(1024, 1, true);
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("7-Data.db");

    download_component(&store, &key(), &local, &cfg(1024, 3))
        .await
        .unwrap();

    assert_eq!(std::fs::read(&local).unwrap(), body);
    let after = store.max_inflight_after_failure.load(Ordering::SeqCst);
    assert!(
        after <= 2,
        "after a 429 at concurrency 3 no request may start with more than 2 in flight, saw {after}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_legacy_whole_object_path_and_the_new_one_write_the_same_bytes() {
    let body = pattern(3 * 1024 + 5);
    let store = ScriptedStore::with_object(&body).await;
    let dir = tempfile::tempdir().unwrap();
    let new = dir.path().join("new");
    let old = dir.path().join("old");

    download_component(&store, &key(), &new, &cfg(1024, 4))
        .await
        .unwrap();
    legacy_download(&store, &key(), &old).await.unwrap();

    assert_eq!(std::fs::read(new).unwrap(), std::fs::read(old).unwrap());
}

/// The path this module replaced: one GET streamed into an unbuffered
/// `tokio::fs::File`, one `write_all` per network chunk. Kept to benchmark
/// against and to prove the new path writes identical bytes.
async fn legacy_download(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    local: &Path,
) -> Result<u64, String> {
    let result = store.get(path).await.map_err(|e| e.to_string())?;
    let mut stream = result.into_stream();
    let mut file = tokio::fs::File::create(local)
        .await
        .map_err(|e| e.to_string())?;
    let mut bytes = 0u64;
    while let Some(chunk) = stream.try_next().await.map_err(|e| e.to_string())? {
        bytes += chunk.len() as u64;
        file.write_all(&chunk).await.map_err(|e| e.to_string())?;
    }
    file.sync_data().await.map_err(|e| e.to_string())?;
    Ok(bytes)
}

// ---- connection reuse ----------------------------------------------------

/// Minimal keep-alive HTTP/1.1 object server that counts accepted TCP
/// connections. It answers `GET` with the whole body or a `Range` slice.
async fn spawn_counting_server(body: Bytes) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.expect("accept");
            counter.fetch_add(1, Ordering::SeqCst);
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        match sock.read(&mut chunk).await {
                            Ok(0) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            // The client closed or reset the connection: the
                            // end of this connection, not a server failure.
                            Err(_) => return,
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
                    buf.drain(..head_end);
                    let range = head
                        .lines()
                        .find_map(|l| l.strip_prefix("range: bytes="))
                        .map(|r| {
                            let (a, b) = r.trim().split_once('-').expect("bounded range");
                            (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap())
                        });
                    let (status, slice, content_range) = match range {
                        Some((a, b)) => {
                            let b = b.min(body.len() - 1);
                            (
                                "206 Partial Content",
                                body.slice(a..=b),
                                format!("Content-Range: bytes {a}-{b}/{}\r\n", body.len()),
                            )
                        }
                        None => ("200 OK", body.clone(), String::new()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{content_range}ETag: \"e1\"\r\n\
                         Last-Modified: Thu, 01 Oct 2026 00:00:00 GMT\r\nAccept-Ranges: bytes\r\n\r\n",
                        slice.len()
                    );
                    use tokio::io::AsyncWriteExt;
                    sock.write_all(response.as_bytes())
                        .await
                        .expect("write head");
                    sock.write_all(&slice).await.expect("write body");
                }
            });
        }
    });
    (addr, accepted)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_downloads_reuse_pooled_connections() {
    const DOWNLOADS: usize = 20;
    let body = Bytes::from(pattern(1024 * 1024));
    let (addr, accepted) = spawn_counting_server(body.clone()).await;
    let config = crate::upload::config::ObjectStoreConfig {
        endpoint: format!("http://{addr}"),
        ..crate::upload::config::ObjectStoreConfig::test_config()
    };
    // One client for every download: the pool only helps if it is shared.
    let store = config.build_object_store().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg(256 * 1024, 4);

    for i in 0..DOWNLOADS {
        let out = dir.path().join(format!("o{i}"));
        let n = download_component(store.as_ref(), &key(), &out, &cfg)
            .await
            .unwrap();
        assert_eq!(n, Some(body.len() as u64));
        assert!(
            std::fs::read(&out).unwrap() == body.as_ref(),
            "download {i} differs"
        );
    }

    let conns = accepted.load(Ordering::SeqCst);
    // Each download issues 4 requests, 80 in all; a pool that works needs
    // roughly the per-object concurrency, not one connection per request.
    assert!(
        conns <= cfg.part_concurrency + 4,
        "{conns} connections accepted for {DOWNLOADS} downloads: the pool is not reusing them"
    );
}

// ---- benchmark -----------------------------------------------------------

#[cfg(feature = "slow-tests")]
mod slow {
    //! Throughput benchmark of component downloads: the replaced path against
    //! the ranged-parts path, over a store that serves 16-64 KiB chunks like a
    //! network response, with and without R2-like shaping (per-stream and
    //! link-wide byte rates measured on this host on 2026-10-01: one stream
    //! about 61 MB/s, eight about 90 MB/s). Numbers go to stdout; run with
    //! `--nocapture`.
    use super::*;

    const OBJECT_BYTES: usize = 256 * 1024 * 1024;

    /// Pacing shared by every stream of one store.
    struct Link {
        stream_bytes_per_sec: Option<f64>,
        link_bytes_per_sec: Option<f64>,
        link_next: Mutex<tokio::time::Instant>,
    }

    impl Link {
        /// When the link has carried `n` more bytes, if the link is shaped.
        fn slot(&self, n: usize) -> Option<tokio::time::Instant> {
            let rate = self.link_bytes_per_sec?;
            let mut next = self.link_next.lock().unwrap();
            let start = (*next).max(tokio::time::Instant::now());
            *next = start + Duration::from_secs_f64(n as f64 / rate);
            Some(*next)
        }
    }

    /// Serves an in-memory object in `chunk` sized pieces, optionally paced
    /// per stream and across all streams.
    struct ShapedStore {
        inner: object_store::memory::InMemory,
        object: Bytes,
        chunk: usize,
        link: Arc<Link>,
    }

    impl ShapedStore {
        async fn new(
            object: Bytes,
            chunk: usize,
            stream_mb: Option<f64>,
            link_mb: Option<f64>,
        ) -> Self {
            let inner = object_store::memory::InMemory::new();
            inner
                .put(&key(), PutPayload::from(object.clone()))
                .await
                .unwrap();
            Self {
                inner,
                object,
                chunk,
                link: Arc::new(Link {
                    stream_bytes_per_sec: stream_mb.map(|m| m * 1e6),
                    link_bytes_per_sec: link_mb.map(|m| m * 1e6),
                    link_next: Mutex::new(tokio::time::Instant::now()),
                }),
            }
        }
    }

    impl fmt::Debug for ShapedStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ShapedStore")
        }
    }
    impl fmt::Display for ShapedStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ShapedStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ShapedStore {
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let mut result = self.inner.get_opts(location, options).await?;
            let object = self.object.clone();
            let chunk = self.chunk;
            let link = Arc::clone(&self.link);
            let end = result.range.end;
            let stream = futures::stream::unfold(
                (result.range.start, tokio::time::Instant::now()),
                move |(pos, deadline)| {
                    let object = object.clone();
                    let link = Arc::clone(&link);
                    async move {
                        if pos >= end {
                            return None;
                        }
                        let n = chunk.min(end - pos);
                        let mut deadline = deadline;
                        if let Some(rate) = link.stream_bytes_per_sec {
                            deadline += Duration::from_secs_f64(n as f64 / rate);
                        }
                        let target = link.slot(n).map_or(deadline, |slot| slot.max(deadline));
                        if link.stream_bytes_per_sec.is_some() || link.link_bytes_per_sec.is_some()
                        {
                            tokio::time::sleep_until(target).await;
                        }
                        Some((Ok(object.slice(pos..pos + n)), (pos + n, deadline)))
                    }
                },
            )
            .boxed();
            result.payload = object_store::GetResultPayload::Stream(stream);
            Ok(result)
        }

        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
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

    async fn timed<F: std::future::Future<Output = u64>>(label: &str, f: F) -> f64 {
        let started = Instant::now();
        let bytes = f.await;
        let secs = started.elapsed().as_secs_f64();
        let rate = bytes as f64 / 1e6 / secs;
        println!("BENCH {label:<46} {secs:>7.2}s {rate:>8.1} MB/s");
        rate
    }

    async fn run(label: &str, store: &ShapedStore) -> (f64, f64) {
        // Isolate the write path from the parallelism: the same single GET,
        // but through the 1 MiB buffered writer instead of per-chunk writes.
        let buffered_dir = tempfile::tempdir().unwrap();
        let single = DownloadConfig {
            part_bytes: u64::MAX,
            ..DownloadConfig::default()
        };
        timed(
            &format!("{label} / single GET, 1MiB buffered write"),
            async {
                download_component(store, &key(), &buffered_dir.path().join("b"), &single)
                    .await
                    .unwrap()
                    .unwrap()
            },
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let path = key();
        let legacy_file = dir.path().join("legacy");
        let new_file = dir.path().join("new");
        let legacy = timed(&format!("{label} / legacy single stream"), async {
            legacy_download(store, &path, &legacy_file).await.unwrap()
        })
        .await;
        let parts = timed(&format!("{label} / 16MiB x4 ranged parts"), async {
            download_component(store, &path, &new_file, &DownloadConfig::default())
                .await
                .unwrap()
                .unwrap()
        })
        .await;
        assert_eq!(
            std::fs::metadata(&legacy_file).unwrap().len(),
            std::fs::metadata(&new_file).unwrap().len()
        );
        assert!(
            std::fs::read(&legacy_file).unwrap() == std::fs::read(&new_file).unwrap(),
            "both paths must write identical bytes"
        );
        (legacy, parts)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bench_unshaped_local_write_path() {
        let object = Bytes::from(pattern(OBJECT_BYTES));
        let store = ShapedStore::new(object, 16 * 1024, None, None).await;
        let (legacy, parts) = run("unshaped 16KiB chunks", &store).await;
        println!("BENCH unshaped speedup {:.2}x", parts / legacy);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bench_r2_shaped_61mb_stream_90mb_link() {
        let object = Bytes::from(pattern(OBJECT_BYTES));
        let store = ShapedStore::new(object, 64 * 1024, Some(61.0), Some(90.0)).await;
        let (legacy, parts) = run("shaped 61MB/s stream 90MB/s link", &store).await;
        println!("BENCH shaped speedup {:.2}x", parts / legacy);
        assert!(
            parts > legacy * 1.2,
            "parallel parts must beat one stream when the link outruns a stream: {parts:.1} vs {legacy:.1} MB/s"
        );
    }
}
