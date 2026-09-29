//! Client-side request throttling for the object store.
//!
//! Cloudflare R2 answers bursts with `429 Too Many Requests`, and
//! `object_store` 0.11 retries only 5xx responses, so every 429 failed its
//! request outright. Recovery (restoring evicted SSTables, read-through
//! rehydration) issues exactly those bursts.
//!
//! [`ThrottledStore`] wraps the one object store every path shares (uploads,
//! compaction uploads, deletes, rehydration, restore): a [`RequestPacer`]
//! spaces requests to a configured rate, and a request answered 429 is
//! retried with bounded exponential backoff. Concurrency is capped separately
//! by `object_store::limit::LimitStore` (see `ObjectStoreConfig`).

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};
use tokio::time::Instant;

/// Spaces requests evenly at no more than `per_second`, with no burst.
///
/// Each caller reserves the next free slot and sleeps until it; slots are
/// `1 / per_second` apart, so any window of one second admits at most
/// `per_second` requests however many callers race.
#[derive(Debug)]
pub struct RequestPacer {
    interval: Duration,
    next_slot: tokio::sync::Mutex<Instant>,
}

impl RequestPacer {
    /// A pacer admitting at most `per_second` requests per second.
    pub fn new(per_second: u32) -> Self {
        assert!(per_second > 0, "a request rate must be positive");
        Self {
            interval: Duration::from_secs(1) / per_second,
            next_slot: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    /// Wait for this request's slot.
    pub async fn acquire(&self) {
        let slot = {
            let mut next = self.next_slot.lock().await;
            let slot = (*next).max(Instant::now());
            *next = slot + self.interval;
            slot
        };
        tokio::time::sleep_until(slot).await;
    }
}

/// How a request answered `429 Too Many Requests` is retried.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitRetry {
    /// Total attempts, the first included. The last 429 is returned.
    pub max_attempts: u32,
    /// Backoff before the first retry; doubles per retry up to `max_backoff`.
    pub base_backoff: Duration,
    /// Upper bound on one backoff.
    pub max_backoff: Duration,
}

impl Default for RateLimitRetry {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Whether an object-store error is the server rate-limiting us.
///
/// `object_store` 0.11 does not expose the HTTP status of a failed request
/// as a type, only in its message ("HTTP status client error (429 Too Many
/// Requests)"), so this matches on that text.
pub fn is_rate_limited(err: &object_store::Error) -> bool {
    let text = err.to_string();
    text.contains("429") && text.contains("Too Many Requests")
}

/// An [`ObjectStore`] that paces requests and retries rate-limited ones.
pub struct ThrottledStore {
    inner: Arc<dyn ObjectStore>,
    pacer: Option<RequestPacer>,
    retry: RateLimitRetry,
    /// Set while requests are being rate-limited, so the log reports the
    /// start and end of an episode rather than every 429.
    throttled: AtomicBool,
}

impl ThrottledStore {
    /// Wrap `inner`, pacing to `requests_per_second` when set.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        requests_per_second: Option<u32>,
        retry: RateLimitRetry,
    ) -> Self {
        Self {
            inner,
            pacer: requests_per_second.map(RequestPacer::new),
            retry,
            throttled: AtomicBool::new(false),
        }
    }

    async fn pace(&self) {
        if let Some(pacer) = &self.pacer {
            pacer.acquire().await;
        }
    }

    /// Run one request, pacing each attempt and retrying a 429.
    async fn request<T, F, Fut>(
        &self,
        op: &'static str,
        location: &Path,
        mut attempt: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut backoff = self.retry.base_backoff;
        let mut tries = 0u32;
        loop {
            self.pace().await;
            tries += 1;
            match attempt().await {
                Err(e) if is_rate_limited(&e) && tries < self.retry.max_attempts => {
                    if !self.throttled.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            op,
                            %location,
                            error = %e,
                            "object store is rate-limiting requests (429); backing off and retrying. \
                             Lower FERROSA_S3_MAX_REQUESTS_PER_SECOND / FERROSA_S3_MAX_CONCURRENT_REQUESTS if this persists"
                        );
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                }
                Err(e) if is_rate_limited(&e) => {
                    tracing::error!(
                        op,
                        %location,
                        attempts = tries,
                        error = %e,
                        "object store request still rate-limited (429) after every retry; failing it"
                    );
                    return Err(e);
                }
                result => {
                    if self.throttled.swap(false, Ordering::Relaxed) {
                        tracing::info!(
                            op,
                            "object store rate-limiting cleared; requests succeeding again"
                        );
                    }
                    return result;
                }
            }
        }
    }
}

impl fmt::Debug for ThrottledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThrottledStore")
            .field("pacer", &self.pacer)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ThrottledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ThrottledStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ThrottledStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.request("put", location, || {
            self.inner.put_opts(location, payload.clone(), opts.clone())
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.request("put_multipart", location, || {
            self.inner.put_multipart_opts(location, opts.clone())
        })
        .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.request("get", location, || {
            self.inner.get_opts(location, options.clone())
        })
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.request("get_range", location, || {
            self.inner.get_range(location, range.clone())
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.request("get_ranges", location, || {
            self.inner.get_ranges(location, ranges)
        })
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.request("head", location, || self.inner.head(location))
            .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.request("delete", location, || self.inner.delete(location))
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // A listing is a paged stream; pacing covers only its first page.
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let location = prefix.cloned().unwrap_or_default();
        self.request("list", &location, || self.inner.list_with_delimiter(prefix))
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.request("copy", from, || self.inner.copy(from, to))
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.request("copy_if_not_exists", from, || {
            self.inner.copy_if_not_exists(from, to)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_pacer_admits_no_more_than_its_rate() {
        let pacer = std::sync::Arc::new(RequestPacer::new(100));
        let start = Instant::now();

        let tasks: Vec<_> = (0..21)
            .map(|_| {
                let pacer = std::sync::Arc::clone(&pacer);
                tokio::spawn(async move { pacer.acquire().await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        // 21 requests at 100/s: the first goes at t=0, the 21st at t=200ms.
        assert!(
            start.elapsed() >= Duration::from_millis(200),
            "21 requests at 100/s must take at least 200ms, took {:?}",
            start.elapsed()
        );
    }

    /// Answers the first `rate_limited` GETs the way R2 does under load,
    /// then serves from memory.
    #[derive(Debug)]
    struct RateLimitingStore {
        inner: object_store::memory::InMemory,
        rate_limited: AtomicU32,
    }

    impl fmt::Display for RateLimitingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RateLimitingStore")
        }
    }

    fn too_many_requests() -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: "Error performing GET: HTTP status client error (429 Too Many Requests)".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for RateLimitingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            let still_limited = self
                .rate_limited
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if still_limited {
                return Err(too_many_requests());
            }
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn fast_retry(max_attempts: u32) -> RateLimitRetry {
        RateLimitRetry {
            max_attempts,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        }
    }

    async fn store_rate_limiting(times: u32) -> (Arc<dyn ObjectStore>, Path) {
        let inner = RateLimitingStore {
            inner: object_store::memory::InMemory::new(),
            rate_limited: AtomicU32::new(0),
        };
        let path = Path::from("prefix/sstables/1-Data.db");
        inner
            .put(&path, PutPayload::from_static(b"rows"))
            .await
            .unwrap();
        inner.rate_limited.store(times, Ordering::SeqCst);
        (Arc::new(inner), path)
    }

    /// Restoring evicted SSTables from R2 failed on the first 429, because
    /// object_store does not retry client errors. A rate-limited read must
    /// be retried until the bucket lets it through.
    #[tokio::test]
    async fn a_rate_limited_read_is_retried_until_it_succeeds() {
        let (inner, path) = store_rate_limiting(2).await;
        let store = ThrottledStore::new(inner, None, fast_retry(5));

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();

        assert_eq!(bytes.as_ref(), b"rows");
    }

    /// Retrying is bounded: a bucket that never stops rate-limiting surfaces
    /// the 429 instead of hanging recovery forever.
    #[tokio::test]
    async fn a_read_rate_limited_past_its_attempts_fails_with_the_429() {
        let (inner, path) = store_rate_limiting(u32::MAX).await;
        let store = ThrottledStore::new(inner, None, fast_retry(3));

        let err = store.get(&path).await.unwrap_err();

        assert!(is_rate_limited(&err), "the 429 is surfaced: {err}");
    }
}
