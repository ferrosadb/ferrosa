//! Client-side request throttling for the object store.
//!
//! Cloudflare R2 answers bursts with `429 Too Many Requests`. Recovery
//! (restoring evicted SSTables, read-through rehydration) issues exactly
//! those bursts.
//!
//! [`ThrottledStore`] wraps the one object store every path shares (uploads,
//! compaction uploads, deletes, rehydration, restore): a [`RequestPacer`]
//! spaces requests to a configured rate, and a request answered 429 is
//! retried with bounded exponential backoff.
//!
//! The 429 retry lives here and nowhere else. `object_store` 0.11 never
//! retried a 429; 0.13 and later retry it inside the HTTP client, unpaced,
//! where this layer cannot count it. [`RateLimitSurfacing`] is installed as
//! the S3 client's HTTP connector and turns every 429 response into a
//! non-retryable error, so each one reaches [`ThrottledStore`]: it is paced,
//! counted in [`rate_limit_events`] (which download concurrency backs off
//! on), logged, and bounded by [`RateLimitRetry`] as before.
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
//!   (`ObjectStoreExt` and the trait default), so every body consumer gets
//!   the same bound.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
    ReqwestConnector,
};
use object_store::path::Path;
use object_store::{
    ClientOptions, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result, UploadPart,
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
/// `object_store` does not expose the HTTP status of a failed request as a
/// public type (its `RetryError` is crate-private), only in its message, so
/// this matches on that text. [`RateLimitSurfacing`] writes the same text.
pub fn is_rate_limited(err: &object_store::Error) -> bool {
    let text = err.to_string();
    text.contains("429") && text.contains("Too Many Requests")
}

/// The message [`RateLimitSurfacing`] gives a 429 response. It carries the
/// text [`is_rate_limited`] matches.
const RATE_LIMITED_RESPONSE: &str = "HTTP status client error (429 Too Many Requests); \
     surfaced to ferrosa's ThrottledStore, which paces and retries it";

/// An [`HttpConnector`] whose clients return every `429 Too Many Requests`
/// response as a non-retryable [`HttpError`] instead of a response.
///
/// `object_store` 0.13+ retries a 429 inside its HTTP client, below every
/// [`ObjectStore`] wrapper: unpaced by [`RequestPacer`], invisible to
/// [`rate_limit_events`] and the 429 log, and stacked under this layer's own
/// [`RateLimitRetry`]. An [`HttpErrorKind::Unknown`] error is never retried
/// by the client (`client/retry.rs`), so the 429 reaches [`ThrottledStore`]
/// exactly as it did on 0.11, which never retried one. Every other status,
/// 5xx retries included, passes through untouched.
#[derive(Debug, Default)]
pub struct RateLimitSurfacing {
    inner: ReqwestConnector,
}

impl HttpConnector for RateLimitSurfacing {
    fn connect(&self, options: &ClientOptions) -> Result<HttpClient> {
        let client = self.inner.connect(options)?;
        Ok(HttpClient::new(SurfaceRateLimits(client)))
    }
}

/// The [`HttpService`] [`RateLimitSurfacing`] hands out.
#[derive(Debug)]
struct SurfaceRateLimits<S>(S);

/// Something that can execute one HTTP request: an [`HttpClient`], or a
/// scripted service in tests.
#[async_trait]
trait Execute: fmt::Debug + Send + Sync + 'static {
    async fn execute(&self, req: HttpRequest) -> std::result::Result<HttpResponse, HttpError>;
}

#[async_trait]
impl Execute for HttpClient {
    async fn execute(&self, req: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        HttpClient::execute(self, req).await
    }
}

#[async_trait]
impl<S: Execute> HttpService for SurfaceRateLimits<S> {
    async fn call(&self, req: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let response = self.0.execute(req).await?;
        if response.status().as_u16() == 429 {
            return Err(HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other(RATE_LIMITED_RESPONSE),
            ));
        }
        Ok(response)
    }
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
///
/// The state sits behind an [`Arc`] because `list` and `delete_stream` return
/// `'static` streams: each one owns a handle to the limits it is paced by.
pub struct ThrottledStore {
    shared: Arc<Throttle>,
}

/// The limits and accounting every request of one [`ThrottledStore`] shares.
struct Throttle {
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
            shared: Arc::new(Throttle {
                inner,
                pacer: limits.requests_per_second.map(RequestPacer::new),
                retry,
                limits,
                permits: Arc::new(Semaphore::new(limits.max_in_flight)),
                throttled: AtomicBool::new(false),
                waiting: AtomicBool::new(false),
                slow_waits: AtomicU64::new(0),
            }),
        }
    }
}

impl Throttle {
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
    // Fuse before handing the stream out. `futures::stream::unfold` PANICS if it
    // is polled after returning `Poll::Ready(None)`, and its consumers do poll
    // once past the end: `object_store::util::collect_bytes` — which backs
    // `GetResult::bytes()`, the way every component download is read — awaits
    // `stream.next()` a second time unconditionally to detect a single-chunk
    // body. On a ZERO-LENGTH body the first `next()` is already `None`, so that
    // second poll lands on a finished unfold and aborts the process.
    //
    // The trigger is therefore any empty object, and the writer emits exactly
    // that: a zero-byte `Rows.db` for every table whose partitions need no row
    // index (most of them). Downloading one during startup registration of
    // evicted generations killed node2 on every restart, 0.35 s in, and killed
    // node1 on a worker thread via the repair path (2026-10-14). Fusing latches
    // completion, so a finished stream keeps reporting "no more data" — which is
    // what every caller is entitled to assume — instead of aborting the node.
    .fuse()
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
            .field("pacer", &self.shared.pacer)
            .field("limits", &self.shared.limits)
            .field("retry", &self.shared.retry)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ThrottledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ThrottledStore(max_in_flight={}, {})",
            self.shared.limits.max_in_flight, self.shared.inner
        )
    }
}

/// Every method is implemented (`missing_trait_methods` is denied), so a
/// method added to the trait cannot slip past the limits through a default.
/// `get_ranges` and `rename_opts` restate the trait defaults on top of this
/// store's own `get_opts`, `copy_opts` and `delete_stream`, so each request
/// they issue is paced, permitted and idle-bounded.
#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ThrottledStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let t = &self.shared;
        t.call("put", location, || {
            t.inner.put_opts(location, payload.clone(), opts.clone())
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let t = &self.shared;
        let upload = t
            .call("put_multipart", location, || {
                t.inner.put_multipart_opts(location, opts.clone())
            })
            .await?;
        Ok(Box::new(LimitedUpload {
            upload,
            permits: Arc::clone(&t.permits),
        }))
    }

    /// `get`, `get_range` and `head` (`ObjectStoreExt`) and `get_ranges` (the
    /// trait default) all come here. A body is idle-bounded and holds its
    /// permit; a HEAD has no body, so its permit is released at once.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let t = &self.shared;
        if options.head {
            return t
                .call("head", location, || {
                    t.inner.get_opts(location, options.clone())
                })
                .await;
        }
        let (result, permit) = t
            .request("get", location, || {
                t.inner.get_opts(location, options.clone())
            })
            .await?;
        let payload = match result.payload {
            GetResultPayload::Stream(stream) => GetResultPayload::Stream(idle_bounded(
                stream,
                permit,
                t.limits.body_idle_timeout,
                location.clone(),
            )),
            file @ GetResultPayload::File(..) => file,
        };
        Ok(GetResult { payload, ..result })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<Bytes>> {
        object_store::coalesce_ranges(
            ranges,
            |range| self.get_range(location, range),
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    /// Each location is deleted by its own request, paced, holding a permit
    /// and retried on 429 like any other: `ObjectStoreExt::delete` comes here
    /// with a stream of one. Results keep the input order.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let t = Arc::clone(&self.shared);
        let in_flight = t.limits.max_in_flight;
        locations
            .map(move |location| {
                let t = Arc::clone(&t);
                async move {
                    let location = location?;
                    t.call("delete", &location, || t.inner.delete(&location))
                        .await?;
                    Ok(location)
                }
            })
            .buffered(in_flight)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        // A listing is a paged stream; it holds one permit while it runs and
        // pacing covers only its first page.
        let t = Arc::clone(&self.shared);
        let prefix = prefix.cloned();
        async move {
            let permit = t.admit("list").await;
            t.inner
                .list(prefix.as_ref())
                .map(move |item| {
                    let _held = &permit;
                    item
                })
                .boxed()
        }
        .into_stream()
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let t = Arc::clone(&self.shared);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        async move {
            let permit = t.admit("list").await;
            t.inner
                .list_with_offset(prefix.as_ref(), &offset)
                .map(move |item| {
                    let _held = &permit;
                    item
                })
                .boxed()
        }
        .into_stream()
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let t = &self.shared;
        let location = prefix.cloned().unwrap_or_default();
        t.call("list", &location, || t.inner.list_with_delimiter(prefix))
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let t = &self.shared;
        let op = match options.mode {
            object_store::CopyMode::Overwrite => "copy",
            object_store::CopyMode::Create => "copy_if_not_exists",
        };
        t.call(op, from, || t.inner.copy_opts(from, to, options.clone()))
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::RenameOptions,
    ) -> Result<()> {
        // Copy then delete, as the trait default does, through this store so
        // both requests take the limits.
        let mode = match options.target_mode {
            object_store::RenameTargetMode::Overwrite => object_store::CopyMode::Overwrite,
            object_store::RenameTargetMode::Create => object_store::CopyMode::Create,
        };
        let copy = CopyOptions {
            mode,
            extensions: options.extensions,
        };
        self.copy_opts(from, to, copy).await?;
        self.delete(from).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicUsize};

    /// A consumer that polls a finished stream once more must be told it is
    /// finished.
    ///
    /// `futures::stream::unfold` PANICS when polled after it returned
    /// `Poll::Ready(None)`, and `idle_bounded` handed out a bare `unfold`. That
    /// panic aborts the whole process, and `object_store`'s
    /// `GetResult::bytes()` drains this stream and polls once past the end — so
    /// any component fetch during startup registration could kill the node.
    /// Observed: node2, 2026-10-14, aborting every restart 0.35 s into
    /// `register_evicted_sstables_remote` with `Unfold must not be polled after
    /// it returned `Poll::Ready(None)``.
    ///
    /// Polling a completed stream is not an error: the caller is entitled to be
    /// told there is no more data.
    #[tokio::test]
    async fn a_finished_idle_bounded_stream_reports_completion_instead_of_aborting() {
        let stream = futures::stream::empty::<Result<Bytes>>().boxed();
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("a test semaphore admits its one permit");
        let mut bounded = idle_bounded(stream, permit, Duration::from_secs(1), Path::from("t"));

        assert!(
            bounded.next().await.is_none(),
            "an empty body ends on its first poll"
        );
        assert!(
            bounded.next().await.is_none(),
            "polling a finished stream must report completion, never abort the process"
        );
    }

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

    /// Answers the first `rate_limited` GETs (and the first `deletes.limited`
    /// deletes) the way R2 does under load, then serves from memory.
    #[derive(Debug)]
    struct RateLimitingStore {
        inner: Arc<object_store::memory::InMemory>,
        rate_limited: AtomicU32,
        deletes: Arc<DeleteScript>,
    }

    /// What the deletes reaching a [`RateLimitingStore`] see and do.
    #[derive(Debug, Default)]
    struct DeleteScript {
        /// Deletes still to answer 429.
        limited: AtomicU32,
        /// Delete requests that reached the store, 429s included.
        issued: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        /// How long a delete that is not rate-limited takes.
        delay: Duration,
    }

    impl RateLimitingStore {
        fn new(deletes: DeleteScript) -> Self {
            Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                rate_limited: AtomicU32::new(0),
                deletes: Arc::new(deletes),
            }
        }
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
            opts: PutMultipartOptions,
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
        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            let script = Arc::clone(&self.deletes);
            let inner = Arc::clone(&self.inner);
            locations
                .then(move |location| {
                    let script = Arc::clone(&script);
                    let inner = Arc::clone(&inner);
                    async move {
                        let location = location?;
                        script.issued.fetch_add(1, Ordering::SeqCst);
                        let limited = script
                            .limited
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                            .is_ok();
                        if limited {
                            return Err(too_many_requests());
                        }
                        let now = script.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        script.max_in_flight.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(script.delay).await;
                        let result = inner.delete(&location).await;
                        script.in_flight.fetch_sub(1, Ordering::SeqCst);
                        result.map(|()| location)
                    }
                })
                .boxed()
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
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
        let inner = RateLimitingStore::new(DeleteScript::default());
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

    /// `object_store` 0.13 turned `delete` into `delete_stream`, which a
    /// wrapper must implement itself. Each delete in the stream is its own
    /// request: rate-limited ones are retried like any other, and the 429 is
    /// counted for the download back-off.
    #[tokio::test]
    async fn a_rate_limited_delete_is_retried_until_it_succeeds() {
        let inner = Arc::new(RateLimitingStore::new(DeleteScript {
            limited: AtomicU32::new(2),
            ..DeleteScript::default()
        }));
        let path = Path::from("prefix/sstables/1-Data.db");
        inner
            .put(&path, PutPayload::from_static(b"rows"))
            .await
            .unwrap();
        let store = ThrottledStore::new(inner.clone(), limits(64), fast_retry(5));
        let events_before = rate_limit_events();

        store.delete(&path).await.unwrap();

        assert_eq!(inner.deletes.issued.load(Ordering::SeqCst), 3);
        assert!(
            matches!(
                inner.inner.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "the delete took effect"
        );
        assert!(rate_limit_events() >= events_before + 2);
    }

    /// Every delete of a multi-location `delete_stream` takes one of the
    /// store's shared permits, so deletes count against the same in-flight cap
    /// as everything else: with one of two permits held by an unread GET body,
    /// at most one delete runs at a time.
    #[tokio::test]
    async fn deletes_share_the_in_flight_cap_with_other_requests() {
        let inner = Arc::new(RateLimitingStore::new(DeleteScript {
            delay: Duration::from_millis(20),
            ..DeleteScript::default()
        }));
        let held_path = Path::from("prefix/sstables/held-Data.db");
        inner
            .put(&held_path, PutPayload::from_static(b"rows"))
            .await
            .unwrap();
        let paths: Vec<Path> = (0..6)
            .map(|i| Path::from(format!("prefix/sstables/{i}-Data.db")))
            .collect();
        for path in &paths {
            inner
                .put(path, PutPayload::from_static(b"rows"))
                .await
                .unwrap();
        }
        let store = ThrottledStore::new(inner.clone(), limits(2), fast_retry(3));
        let held = store.get(&held_path).await.unwrap();

        let deleted: Vec<Path> = store
            .delete_stream(futures::stream::iter(paths.clone().into_iter().map(Ok)).boxed())
            .map(|r| r.unwrap())
            .collect()
            .await;
        drop(held);

        assert_eq!(deleted, paths, "every location, in input order");
        let max = inner.deletes.max_in_flight.load(Ordering::SeqCst);
        assert_eq!(
            max, 1,
            "one permit of two is held by a GET body, so deletes run one at a time"
        );
    }

    /// Deletes are paced like every other request: 11 deletes at 50/s take
    /// at least 200 ms.
    #[tokio::test]
    async fn deletes_are_paced() {
        let inner = Arc::new(RateLimitingStore::new(DeleteScript::default()));
        let store = ThrottledStore::new(
            inner.clone(),
            ThrottleLimits {
                requests_per_second: Some(50),
                ..limits(64)
            },
            fast_retry(3),
        );
        let paths = (0..11).map(|i| Ok(Path::from(format!("p/{i}"))));
        let start = Instant::now();

        let results: Vec<Result<Path>> = store
            .delete_stream(futures::stream::iter(paths).boxed())
            .collect()
            .await;

        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert!(
            start.elapsed() >= Duration::from_millis(200),
            "11 deletes at 50/s must take at least 200ms, took {:?}",
            start.elapsed()
        );
    }

    /// `list` returns a `'static` stream since 0.13, so the listing must own
    /// its permit rather than borrow the store: it holds the permit while it
    /// is alive, keeps working after the store handle is dropped, and releases
    /// the permit when dropped.
    #[tokio::test]
    async fn a_listing_owns_its_permit_until_it_is_dropped() {
        let inner = object_store::memory::InMemory::new();
        for i in 0..3 {
            inner
                .put(&Path::from(format!("p/{i}")), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        let store = ThrottledStore::new(Arc::new(inner), limits(1), fast_retry(3));
        let probe = Path::from("p/0");

        let mut listing = store.list(Some(&Path::from("p")));
        let first = listing.next().await.expect("one item").unwrap();
        assert!(first.location.as_ref().starts_with("p/"));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), store.head(&probe))
                .await
                .is_err(),
            "a live listing holds the only permit"
        );

        let other = ThrottledStore {
            shared: Arc::clone(&store.shared),
        };
        drop(store);
        let rest: Vec<_> = listing.by_ref().collect().await;
        assert_eq!(rest.len(), 2, "the listing outlives the store handle");
        drop(listing);

        tokio::time::timeout(Duration::from_secs(5), other.head(&probe))
            .await
            .expect("a dropped listing releases its permit")
            .unwrap();
    }

    /// `head` is `get_opts` with `head: true` since 0.13. It must not hold a
    /// permit past the response (it has no body to read), and a 429 on it is
    /// retried as a head.
    #[tokio::test]
    async fn a_head_releases_its_permit_and_is_retried_on_429() {
        let (inner, path) = store_rate_limiting(1).await;
        let store = ThrottledStore::new(inner, limits(1), fast_retry(3));

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 4);
        tokio::time::timeout(Duration::from_secs(5), store.head(&path))
            .await
            .expect("the first head released the only permit")
            .unwrap();
    }

    /// An HTTP server that answers the first `limited` requests with
    /// `429 Too Many Requests` and every later one with a 4-byte object,
    /// counting the requests it receives.
    async fn rate_limiting_server(limited: usize) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&hits);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        let n = counted.fetch_add(1, Ordering::SeqCst);
                        let response: &[u8] = if n < limited {
                            b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n"
                        } else {
                            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"e\"\r\n\
                              Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\nrows"
                        };
                        if sock.write_all(response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), hits)
    }

    /// The production S3 client, pointed at `endpoint`, under a fast-retry
    /// throttle: the same builder `build_object_store` uses.
    fn production_s3_against(endpoint: &str, retry: RateLimitRetry) -> ThrottledStore {
        let cfg = super::super::config::ObjectStoreConfig {
            endpoint: endpoint.to_owned(),
            ..super::super::config::ObjectStoreConfig::test_config()
        };
        let s3 = cfg.s3_builder().unwrap().build().unwrap();
        ThrottledStore::new(Arc::new(s3), limits(4), retry)
    }

    /// object_store 0.13+ retries a 429 inside its HTTP client, below this
    /// layer: unpaced, uncounted, and stacked under this layer's own retry
    /// (10 x 10 attempts). The production client must hand every 429 up, so
    /// each one is exactly one request this layer paced and counted.
    #[tokio::test]
    async fn the_s3_client_hands_every_429_to_the_throttle() {
        let logs = crate::upload::config::tests::CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(logs.subscriber());
        let (endpoint, hits) = rate_limiting_server(2).await;
        let store = production_s3_against(&endpoint, fast_retry(5));

        let bytes = store
            .get(&Path::from("k"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"rows");
        assert_eq!(hits.load(Ordering::SeqCst), 3, "two 429s and the success");
        // The server count is the same whichever layer retried; what differs
        // is whether this layer saw the 429s. Without RateLimitSurfacing the
        // client absorbs both, and this store neither logs nor counts them.
        let text = logs.text();
        assert!(
            text.lines()
                .any(|l| l.contains("WARN") && l.contains("rate-limiting requests (429)")),
            "the 429 reached the throttle:\n{text}"
        );
    }

    /// A bucket that never stops answering 429 fails after exactly this
    /// layer's attempts, with the 429 recognisable to [`is_rate_limited`].
    #[tokio::test]
    async fn an_endless_429_from_the_s3_client_fails_after_the_throttle_attempts() {
        let (endpoint, hits) = rate_limiting_server(usize::MAX).await;
        let store = production_s3_against(&endpoint, fast_retry(3));

        let err = store.head(&Path::from("k")).await.unwrap_err();

        assert!(is_rate_limited(&err), "{err}");
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    /// Only a 429 is surfaced; any other response passes through unchanged,
    /// so the client keeps retrying 5xx itself.
    #[tokio::test]
    async fn only_a_429_response_is_turned_into_an_error() {
        #[derive(Debug)]
        struct Answer(u16);
        #[async_trait]
        impl Execute for Answer {
            async fn execute(
                &self,
                _req: HttpRequest,
            ) -> std::result::Result<HttpResponse, HttpError> {
                let mut response =
                    HttpResponse::new(object_store::client::HttpResponseBody::from(Bytes::new()));
                *response.status_mut() = self.0.try_into().unwrap();
                Ok(response)
            }
        }
        let request = || HttpRequest::new(object_store::client::HttpRequestBody::empty());

        let err = SurfaceRateLimits(Answer(429))
            .call(request())
            .await
            .unwrap_err();
        assert_eq!(
            err.kind(),
            HttpErrorKind::Unknown,
            "never retried by the client"
        );
        assert!(err.to_string().contains("429 Too Many Requests"), "{err}");
        for status in [200u16, 404, 500, 503] {
            let response = SurfaceRateLimits(Answer(status))
                .call(request())
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
        }
    }
}
