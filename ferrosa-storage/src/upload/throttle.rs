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
//! retried with bounded exponential backoff.
//!
//! It also caps requests in flight (`FERROSA_S3_MAX_CONCURRENT_REQUESTS`) with
//! its own permits rather than `object_store::limit::LimitStore`, for two
//! reasons from the 2026-10-10 outage:
//! - the time a request spends waiting for the pacer and a permit is
//!   measured, and a wait over [`DEFAULT_WAIT_WARN_AFTER`] is reported (edges
//!   only: the start of a waiting streak and its recovery with a count), so
//!   a queue behind the client-side limits no longer reads as a hang;
//! - a GET body holds its permit only while it keeps sending: a body that
//!   yields nothing for `FERROSA_S3_BODY_IDLE_TIMEOUT_SECS` fails with a
//!   retryable error and releases the permit, instead of holding it for the
//!   900 s request timeout. `get_range` and `get_ranges` go through `get_opts`
//!   (the trait defaults), so every body consumer gets the same bound.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// A wait for the pacer and a permit longer than this is reported.
pub const DEFAULT_WAIT_WARN_AFTER: Duration = Duration::from_secs(5);

/// The client-side limits a [`ThrottledStore`] enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThrottleLimits {
    /// `FERROSA_S3_MAX_REQUESTS_PER_SECOND`; `None` is unpaced.
    pub requests_per_second: Option<u32>,
    /// `FERROSA_S3_MAX_CONCURRENT_REQUESTS` (or its default).
    pub max_in_flight: usize,
    /// `FERROSA_S3_BODY_IDLE_TIMEOUT_SECS`: longest silence a GET body may keep.
    pub body_idle_timeout: Duration,
    /// Waits for the pacer and a permit longer than this are reported.
    pub wait_warn_after: Duration,
}

impl ThrottleLimits {
    fn rate_text(&self) -> String {
        self.requests_per_second
            .map_or_else(|| "unset".to_owned(), |rps| rps.to_string())
    }
}

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

/// Count of 429 responses seen by any [`ThrottledStore`] in this process.
static RATE_LIMIT_EVENTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many 429 responses this process has seen. A download compares the
/// value before and after a request to learn that the store pushed back,
/// because the store layer retries a 429 itself and the caller sees success.
/// The signal is process-wide, so it may shrink an object's concurrency for
/// pressure another request caused; that errs toward backing off.
pub fn rate_limit_events() -> u64 {
    RATE_LIMIT_EVENTS.load(Ordering::Relaxed)
}

/// An [`ObjectStore`] that caps requests in flight, paces them, retries
/// rate-limited ones, and bounds how long a GET body may stay silent.
pub struct ThrottledStore {
    inner: Arc<dyn ObjectStore>,
    pacer: Option<RequestPacer>,
    retry: RateLimitRetry,
    limits: ThrottleLimits,
    /// One permit per request in flight; a GET body keeps its permit.
    permits: Arc<Semaphore>,
    /// Set while requests are being rate-limited, so the log reports the
    /// start and end of an episode rather than every 429.
    throttled: AtomicBool,
    /// Set while requests wait longer than `limits.wait_warn_after` for the
    /// pacer and a permit; the log reports the streak's start and end.
    waiting: AtomicBool,
    /// Slow waits in the current streak, reported when it ends.
    slow_waits: AtomicU64,
}

impl ThrottledStore {
    /// Wrap `inner` with `limits`.
    pub fn new(inner: Arc<dyn ObjectStore>, limits: ThrottleLimits, retry: RateLimitRetry) -> Self {
        assert!(
            limits.max_in_flight > 0,
            "the in-flight limit must be positive"
        );
        assert!(
            !limits.body_idle_timeout.is_zero(),
            "the body idle timeout must be positive"
        );
        Self {
            inner,
            pacer: limits.requests_per_second.map(RequestPacer::new),
            retry,
            limits,
            permits: Arc::new(Semaphore::new(limits.max_in_flight)),
            throttled: AtomicBool::new(false),
            waiting: AtomicBool::new(false),
            slow_waits: AtomicU64::new(0),
        }
    }

    /// Wait for the pacer, then for a permit, and report a long wait.
    async fn admit(&self, op: &'static str) -> OwnedSemaphorePermit {
        let started = Instant::now();
        if let Some(pacer) = &self.pacer {
            pacer.acquire().await;
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the request semaphore is never closed");
        self.note_wait(op, started.elapsed());
        permit
    }

    /// Report the edges of a streak of waits longer than the threshold: the
    /// first slow wait, and the first prompt admission after it, with the
    /// count. One line per request would flood the log in exactly the
    /// conditions it describes.
    fn note_wait(&self, op: &'static str, waited: Duration) {
        if waited > self.limits.wait_warn_after {
            self.slow_waits.fetch_add(1, Ordering::Relaxed);
            if !self.waiting.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    op,
                    waited_ms = waited.as_millis() as u64,
                    "object-store requests waited {waited:?} for the client-side limits \
                     (FERROSA_S3_MAX_CONCURRENT_REQUESTS={}, FERROSA_S3_MAX_REQUESTS_PER_SECOND={}); \
                     the limits, not the store, are the bottleneck",
                    self.limits.max_in_flight,
                    self.limits.rate_text(),
                );
            }
        } else if self.waiting.swap(false, Ordering::Relaxed) {
            let slow_waits = self.slow_waits.swap(0, Ordering::Relaxed);
            tracing::info!(
                op,
                slow_waits,
                "object-store requests are no longer waiting for the client-side limits"
            );
        }
    }

    /// Run one request, pacing each attempt and retrying a 429. The permit
    /// that admitted the successful attempt is returned with its result.
    async fn request<T, F, Fut>(
        &self,
        op: &'static str,
        location: &Path,
        mut attempt: F,
    ) -> Result<(T, OwnedSemaphorePermit)>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut backoff = self.retry.base_backoff;
        let mut tries = 0u32;
        loop {
            let permit = self.admit(op).await;
            tries += 1;
            match attempt().await {
                Err(e) if is_rate_limited(&e) && tries < self.retry.max_attempts => {
                    drop(permit);
                    RATE_LIMIT_EVENTS.fetch_add(1, Ordering::Relaxed);
                    super::stats::record_retry(op);
                    if !self.throttled.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            op,
                            %location,
                            error = %e,
                            "object store is rate-limiting requests (429); backing off and retrying. \
                             If 429s persist, cap the request rate below the bucket's limit with \
                             FERROSA_S3_MAX_REQUESTS_PER_SECOND. Slow responses (latency) are not \
                             a rate problem: do not lower the limits for them"
                        );
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                }
                Err(e) if is_rate_limited(&e) => {
                    RATE_LIMIT_EVENTS.fetch_add(1, Ordering::Relaxed);
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
                    return result.map(|value| (value, permit));
                }
            }
        }
    }

    /// Run one request whose result holds no body: its permit is released.
    async fn call<T, F, Fut>(&self, op: &'static str, location: &Path, attempt: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let (value, _permit) = self.request(op, location, attempt).await?;
        Ok(value)
    }
}

/// The error a GET body fails with when it stays silent past the idle timeout.
fn idle_timeout_error(location: &Path, idle: Duration) -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: format!(
            "response body of {location} sent nothing for {idle:?} \
             (FERROSA_S3_BODY_IDLE_TIMEOUT_SECS); abandoning it so the request can be retried"
        )
        .into(),
    }
}

/// `stream`, holding `permit` until it ends, failing (and releasing the permit)
/// when it yields nothing for `idle`.
fn idle_bounded(
    stream: BoxStream<'static, Result<Bytes>>,
    permit: OwnedSemaphorePermit,
    idle: Duration,
    location: Path,
) -> BoxStream<'static, Result<Bytes>> {
    futures::stream::unfold(Some((stream, permit)), move |state| {
        let location = location.clone();
        async move {
            let (mut stream, permit) = state?;
            match tokio::time::timeout(idle, stream.next()).await {
                Ok(Some(item)) => Some((item, Some((stream, permit)))),
                Ok(None) => None,
                Err(_) => {
                    drop(stream);
                    drop(permit);
                    super::stats::record_retry("get_body_idle");
                    tracing::warn!(
                        %location,
                        idle_secs = idle.as_secs_f64(),
                        "object-store response body stalled past FERROSA_S3_BODY_IDLE_TIMEOUT_SECS; \
                         failing it as retryable and releasing its request slot"
                    );
                    Some((Err(idle_timeout_error(&location, idle)), None))
                }
            }
        }
    })
    .boxed()
}

/// A multipart upload whose part, complete and abort requests each take one
/// of the store's permits, as `object_store::limit::LimitUpload` does.
#[derive(Debug)]
struct LimitedUpload {
    upload: Box<dyn MultipartUpload>,
    permits: Arc<Semaphore>,
}

#[async_trait]
impl MultipartUpload for LimitedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = self.upload.put_part(data);
        let permits = Arc::clone(&self.permits);
        Box::pin(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .expect("the request semaphore is never closed");
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let _permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the request semaphore is never closed");
        self.upload.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the request semaphore is never closed");
        self.upload.abort().await
    }
}

impl fmt::Debug for ThrottledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThrottledStore")
            .field("pacer", &self.pacer)
            .field("limits", &self.limits)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ThrottledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ThrottledStore(max_in_flight={}, {})",
            self.limits.max_in_flight, self.inner
        )
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
        self.call("put", location, || {
            self.inner.put_opts(location, payload.clone(), opts.clone())
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self
            .call("put_multipart", location, || {
                self.inner.put_multipart_opts(location, opts.clone())
            })
            .await?;
        Ok(Box::new(LimitedUpload {
            upload,
            permits: Arc::clone(&self.permits),
        }))
    }

    /// `get`, `get_range` and `get_ranges` are the trait defaults, which all
    /// come here, so every body is idle-bounded and holds its permit.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (result, permit) = self
            .request("get", location, || {
                self.inner.get_opts(location, options.clone())
            })
            .await?;
        let payload = match result.payload {
            GetResultPayload::Stream(stream) => GetResultPayload::Stream(idle_bounded(
                stream,
                permit,
                self.limits.body_idle_timeout,
                location.clone(),
            )),
            file @ GetResultPayload::File(..) => file,
        };
        Ok(GetResult { payload, ..result })
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.call("head", location, || self.inner.head(location))
            .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.call("delete", location, || self.inner.delete(location))
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // A listing is a paged stream; it holds one permit while it runs and
        // pacing covers only its first page.
        let prefix = prefix.cloned();
        self.admit("list")
            .map(move |permit| {
                self.inner
                    .list(prefix.as_ref())
                    .map(move |item| {
                        let _held = &permit;
                        item
                    })
                    .boxed()
            })
            .into_stream()
            .flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let location = prefix.cloned().unwrap_or_default();
        self.call("list", &location, || self.inner.list_with_delimiter(prefix))
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.call("copy", from, || self.inner.copy(from, to)).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.call("copy_if_not_exists", from, || {
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

    /// Unpaced limits with `max_in_flight` permits and production timeouts.
    fn limits(max_in_flight: usize) -> ThrottleLimits {
        ThrottleLimits {
            requests_per_second: None,
            max_in_flight,
            body_idle_timeout: Duration::from_secs(30),
            wait_warn_after: DEFAULT_WAIT_WARN_AFTER,
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
        let store = ThrottledStore::new(inner, limits(64), fast_retry(5));

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();

        assert_eq!(bytes.as_ref(), b"rows");
    }

    /// T-F5-3 (2026-10-10): requests queued for minutes behind four permits
    /// and nothing said so, so the stall read as a hang. With one request in
    /// flight and the permit held by a gated request for 300 ms, the next
    /// request waits longer than the (test) threshold of 100 ms and the wait
    /// is logged, naming both limits; a following request that does not wait
    /// reports the recovery with the count of slow waits.
    #[tokio::test]
    async fn a_long_permit_wait_is_logged() {
        let logs = crate::upload::config::tests::CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(logs.subscriber());
        let path = Path::from("prefix/sstables/1-Index.db");
        let inner = object_store::memory::InMemory::new();
        inner
            .put(&path, PutPayload::from_static(b"rows"))
            .await
            .unwrap();
        let store = Arc::new(ThrottledStore::new(
            Arc::new(inner),
            ThrottleLimits {
                wait_warn_after: Duration::from_millis(100),
                ..limits(1)
            },
            fast_retry(3),
        ));

        // The gated request: its unread body holds the only permit.
        let held = store.get(&path).await.unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(held);
        });
        let waited = store.get(&path).await.unwrap().bytes().await.unwrap();
        release.await.unwrap();
        assert_eq!(waited.as_ref(), b"rows");
        let prompt = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(prompt.as_ref(), b"rows");

        let text = logs.text();
        let warning = text
            .lines()
            .find(|l| l.contains("WARN") && l.contains("waited"))
            .unwrap_or_else(|| panic!("no permit-wait warning was logged:\n{text}"));
        assert!(
            warning.contains("FERROSA_S3_MAX_CONCURRENT_REQUESTS=1")
                && warning.contains("FERROSA_S3_MAX_REQUESTS_PER_SECOND=unset"),
            "{warning}"
        );
        let recovered = text
            .lines()
            .find(|l| l.contains("INFO") && l.contains("no longer waiting"))
            .unwrap_or_else(|| panic!("no recovery line was logged:\n{text}"));
        assert!(recovered.contains("slow_waits=1"), "{recovered}");
    }

    /// 2026-10-10: the 429 warning told operators to lower both limits "if this
    /// persists", and they lowered in-flight to 4 when the real cause was
    /// latency. The warning names the rate cap as the remedy for a 429 and
    /// must not advise lowering the in-flight limit.
    #[tokio::test]
    async fn the_429_warning_does_not_advise_lowering_the_in_flight_limit() {
        let logs = crate::upload::config::tests::CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(logs.subscriber());
        let (inner, path) = store_rate_limiting(1).await;
        let store = ThrottledStore::new(inner, limits(64), fast_retry(5));

        store.get(&path).await.unwrap().bytes().await.unwrap();

        let text = logs.text();
        let warning = text
            .lines()
            .find(|l| l.contains("WARN") && l.contains("429"))
            .unwrap_or_else(|| panic!("no 429 warning:\n{text}"));
        assert!(
            warning.contains("FERROSA_S3_MAX_REQUESTS_PER_SECOND"),
            "{warning}"
        );
        assert!(
            !warning.contains("FERROSA_S3_MAX_CONCURRENT_REQUESTS"),
            "a 429 is a rate problem; lowering in-flight does not fix it: {warning}"
        );
        assert!(warning.contains("latency"), "{warning}");
    }

    /// Retrying is bounded: a bucket that never stops rate-limiting surfaces
    /// the 429 instead of hanging recovery forever.
    #[tokio::test]
    async fn a_read_rate_limited_past_its_attempts_fails_with_the_429() {
        let (inner, path) = store_rate_limiting(u32::MAX).await;
        let store = ThrottledStore::new(inner, limits(64), fast_retry(3));

        let err = store.get(&path).await.unwrap_err();

        assert!(is_rate_limited(&err), "the 429 is surfaced: {err}");
    }
}
