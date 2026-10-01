//! Object-store component downloads.
//!
//! One SSTable component is one object, and a `Data.db` can be hundreds of
//! megabytes. A single GET stream tops out at the per-connection rate of the
//! store (about 61 MB/s against R2) while several ranged GETs of the same
//! object reach the link rate (about 90 MB/s), so a large object is fetched
//! as parallel ranged parts written with positional writes into a
//! preallocated temp file. A small object is one GET with a buffered write.
//!
//! Failure rules:
//! - the temp file is renamed to its final name only after its length is
//!   verified and it is synced, so a truncated file never carries the final
//!   name; every failure removes the temp file;
//! - a failed part is retried alone (bounded attempts, exponential backoff);
//!   the parts that already landed are kept;
//! - a 429 seen during a part (after the store layer's own retries) halves
//!   nothing silently: it shrinks that object's concurrency by one slot and
//!   logs the first such edge per object;
//! - a length that does not match the object's size is an error, never a
//!   short file.
//!
//! Tunables (rejected loudly when invalid, like the other `FERROSA_S3_*`):
//! `FERROSA_S3_DOWNLOAD_PART_BYTES`, `FERROSA_S3_DOWNLOAD_PART_CONCURRENCY`,
//! `FERROSA_RESTORE_CONCURRENCY`.

use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore};
use tokio::io::AsyncWriteExt;

use super::config::parse_request_limit;
use super::stats;

/// Default size of one ranged part (and the size at or below which an
/// object is fetched with a single GET).
pub const DEFAULT_PART_BYTES: u64 = 16 * 1024 * 1024;
/// Default number of ranged parts in flight for one object.
pub const DEFAULT_PART_CONCURRENCY: usize = 4;
/// Default number of SSTable generations restored at once.
pub const DEFAULT_RESTORE_CONCURRENCY: usize = 4;

/// Attempts per object (small path) and per part (ranged path).
const MAX_ATTEMPTS: u32 = 5;
const MAX_BACKOFF: Duration = Duration::from_secs(10);
/// Buffer for the single-GET path and for part 0 riding the first request.
const WRITE_BUFFER_BYTES: usize = 1024 * 1024;

/// Download tunables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadConfig {
    /// Ranged part size; objects of at most this size use one GET.
    pub part_bytes: u64,
    /// Parts in flight per object.
    pub part_concurrency: usize,
    /// Generations restored concurrently by startup restore.
    pub restore_concurrency: usize,
    /// First retry delay; doubles per retry up to 10 s.
    pub retry_backoff: Duration,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            part_bytes: DEFAULT_PART_BYTES,
            part_concurrency: DEFAULT_PART_CONCURRENCY,
            restore_concurrency: DEFAULT_RESTORE_CONCURRENCY,
            retry_backoff: Duration::from_millis(500),
        }
    }
}

impl DownloadConfig {
    /// Parse the three optional variables. Unset or empty means the default;
    /// anything else must be a positive integer, and a bad value is rejected
    /// naming its variable.
    pub fn parse(
        part_bytes: Option<&str>,
        part_concurrency: Option<&str>,
        restore_concurrency: Option<&str>,
    ) -> ferrosa_common::Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            part_bytes: parse_request_limit::<u64>("FERROSA_S3_DOWNLOAD_PART_BYTES", part_bytes)?
                .unwrap_or(defaults.part_bytes),
            part_concurrency: parse_request_limit::<usize>(
                "FERROSA_S3_DOWNLOAD_PART_CONCURRENCY",
                part_concurrency,
            )?
            .unwrap_or(defaults.part_concurrency),
            restore_concurrency: parse_request_limit::<usize>(
                "FERROSA_RESTORE_CONCURRENCY",
                restore_concurrency,
            )?
            .unwrap_or(defaults.restore_concurrency),
            retry_backoff: defaults.retry_backoff,
        })
    }

    /// Read the three variables from the process environment.
    pub fn from_env() -> ferrosa_common::Result<Self> {
        Self::parse(
            std::env::var("FERROSA_S3_DOWNLOAD_PART_BYTES")
                .ok()
                .as_deref(),
            std::env::var("FERROSA_S3_DOWNLOAD_PART_CONCURRENCY")
                .ok()
                .as_deref(),
            std::env::var("FERROSA_RESTORE_CONCURRENCY").ok().as_deref(),
        )
    }
}

static CONFIG: OnceLock<DownloadConfig> = OnceLock::new();

/// The process-wide download config, read from the environment on first use.
/// An invalid variable is an error on every call until it is fixed; it is
/// never replaced by a default.
pub fn config() -> ferrosa_common::Result<DownloadConfig> {
    if let Some(config) = CONFIG.get() {
        return Ok(*config);
    }
    let parsed = DownloadConfig::from_env()?;
    Ok(*CONFIG.get_or_init(|| parsed))
}

fn invalid(msg: String) -> ferrosa_common::Error {
    ferrosa_common::Error::InvalidFormat(msg)
}

/// How a failed attempt may be handled by the caller.
enum Fail {
    /// Worth another attempt (transient network or body failure).
    Retry(String),
    /// Retrying cannot help (parts already exhausted their attempts, local
    /// IO failure, object vanished mid-download).
    Fatal(String),
}

impl Fail {
    fn into_error(self) -> ferrosa_common::Error {
        match self {
            Fail::Retry(m) | Fail::Fatal(m) => invalid(m),
        }
    }
}

/// What a finished download did, for stats.
struct Downloaded {
    object_bytes: u64,
    parts: u32,
    ranged: bool,
    part_retries: u32,
}

/// Download `s3_path` to `local_path`. `Ok(None)` means the object does not
/// exist. On any failure nothing is left at `local_path` or its temp name.
pub async fn download_component(
    store: &dyn ObjectStore,
    s3_path: &ObjectPath,
    local_path: &Path,
    cfg: &DownloadConfig,
) -> ferrosa_common::Result<Option<u64>> {
    assert!(cfg.part_bytes > 0 && cfg.part_concurrency > 0);
    let tmp = temp_path(local_path);
    let started = Instant::now();
    match download_with_retries(store, s3_path, &tmp, cfg).await {
        Ok(Some(done)) => {
            if let Err(e) = tokio::fs::rename(&tmp, local_path).await {
                remove_temp(&tmp).await;
                return Err(invalid(format!(
                    "failed to publish downloaded component {}: {e}",
                    local_path.display()
                )));
            }
            stats::record_download(&stats::DownloadRecord {
                path: s3_path,
                object_bytes: done.object_bytes,
                elapsed: started.elapsed(),
                parts: done.parts,
                ranged: done.ranged,
                part_retries: done.part_retries,
            });
            Ok(Some(done.object_bytes))
        }
        Ok(None) => {
            remove_temp(&tmp).await;
            Ok(None)
        }
        Err(fail) => {
            remove_temp(&tmp).await;
            Err(fail.into_error())
        }
    }
}

fn temp_path(local_path: &Path) -> PathBuf {
    let mut name = local_path.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

/// Remove a temp file; one that is already absent is the normal case.
async fn remove_temp(tmp: &Path) {
    match tokio::fs::remove_file(tmp).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            file = %tmp.display(),
            error = %e,
            "could not remove a download temp file after a failed download"
        ),
    }
}

async fn download_with_retries(
    store: &dyn ObjectStore,
    s3_path: &ObjectPath,
    tmp: &Path,
    cfg: &DownloadConfig,
) -> Result<Option<Downloaded>, Fail> {
    let mut backoff = cfg.retry_backoff;
    let mut attempt = 1;
    loop {
        match download_once(store, s3_path, tmp, cfg).await {
            Err(Fail::Retry(msg)) if attempt < MAX_ATTEMPTS => {
                tracing::warn!(
                    path = %s3_path,
                    attempt,
                    error = %msg,
                    "SSTable component download failed; retrying the object"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                attempt += 1;
            }
            result => return result,
        }
    }
}

async fn download_once(
    store: &dyn ObjectStore,
    s3_path: &ObjectPath,
    tmp: &Path,
    cfg: &DownloadConfig,
) -> Result<Option<Downloaded>, Fail> {
    let Some(result) = first_response(store, s3_path, cfg).await? else {
        return Ok(None);
    };
    let size = result.meta.size as u64;
    let etag = result.meta.e_tag.clone();
    let stream = result.into_stream();
    if size <= cfg.part_bytes {
        let written = write_whole(stream, tmp, s3_path).await?;
        verify_len(written, size, s3_path)?;
        return Ok(Some(Downloaded {
            object_bytes: size,
            parts: 1,
            ranged: false,
            part_retries: 0,
        }));
    }
    download_ranged(store, s3_path, tmp, cfg, size, etag, stream).await
}

/// The request that reveals the object's size. It asks for the first part
/// only (`0..part_bytes`, which a store clamps to the object's length), so its
/// body is read to the end and the connection goes back to the pool. An
/// unbounded GET here would have its body abandoned after part 0 on a large
/// object, which closes the connection: one new TLS connection per download.
///
/// A store that rejects the range (an empty object has no byte 0) is asked
/// again with a plain GET, which is always a whole-object read.
async fn first_response(
    store: &dyn ObjectStore,
    s3_path: &ObjectPath,
    cfg: &DownloadConfig,
) -> Result<Option<object_store::GetResult>, Fail> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..cfg.part_bytes as usize)),
        ..GetOptions::default()
    };
    match store.get_opts(s3_path, options).await {
        Ok(result) => Ok(Some(result)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(first) => {
            tracing::debug!(path = %s3_path, error = %first, "ranged first request refused; using a plain GET");
            match store.get(s3_path).await {
                Ok(result) => Ok(Some(result)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(Fail::Retry(format!(
                    "S3 download failed for {s3_path}: {e} (ranged attempt: {first})"
                ))),
            }
        }
    }
}

fn verify_len(written: u64, expected: u64, s3_path: &ObjectPath) -> Result<(), Fail> {
    if written == expected {
        return Ok(());
    }
    Err(Fail::Retry(format!(
        "SSTable component {s3_path} is {expected} bytes in the object store but {written} bytes arrived"
    )))
}

/// Single-GET path: stream to a buffered writer, then sync.
async fn write_whole(
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
    tmp: &Path,
    s3_path: &ObjectPath,
) -> Result<u64, Fail> {
    let local = |what: &str, e: std::io::Error| {
        Fail::Fatal(format!("failed to {what} {}: {e}", tmp.display()))
    };
    let file = tokio::fs::File::create(tmp)
        .await
        .map_err(|e| local("create SSTable download temp file", e))?;
    let mut writer = tokio::io::BufWriter::with_capacity(WRITE_BUFFER_BYTES, file);
    let mut written = 0u64;
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| Fail::Retry(format!("failed to stream SSTable component {s3_path}: {e}")))?
    {
        written += chunk.len() as u64;
        writer
            .write_all(&chunk)
            .await
            .map_err(|e| local("write SSTable download temp file", e))?;
    }
    writer
        .flush()
        .await
        .map_err(|e| local("flush SSTable download temp file", e))?;
    writer
        .get_mut()
        .sync_data()
        .await
        .map_err(|e| local("sync SSTable download temp file", e))?;
    Ok(written)
}

/// Parts of `size` bytes in `part_bytes` pieces; the last may be short.
pub fn part_ranges(size: u64, part_bytes: u64) -> Vec<Range<u64>> {
    assert!(part_bytes > 0, "a part must hold at least one byte");
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < size {
        let end = (start + part_bytes).min(size);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

/// Per-object concurrency gate that shrinks when the store pushes back.
struct PartGate {
    permits: tokio::sync::Semaphore,
    limit: AtomicUsize,
    reduced: AtomicBool,
}

impl PartGate {
    fn new(concurrency: usize) -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(concurrency),
            limit: AtomicUsize::new(concurrency),
            reduced: AtomicBool::new(false),
        }
    }

    /// The store pushed back while `permit` was held: keep that slot out of
    /// circulation (never below one) and report the first such edge.
    fn shrink(&self, permit: tokio::sync::SemaphorePermit<'_>, path: &ObjectPath) {
        let before = self.limit.load(Ordering::Relaxed);
        if before <= 1 {
            return;
        }
        self.limit.store(before - 1, Ordering::Relaxed);
        permit.forget();
        if !self.reduced.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                path = %path,
                from = before,
                to = before - 1,
                "object store is rate-limiting ranged downloads of this object (429); \
                 reducing its part concurrency"
            );
        }
    }
}

struct PartContext<'a> {
    store: &'a dyn ObjectStore,
    path: &'a ObjectPath,
    etag: Option<String>,
    file: Arc<std::fs::File>,
    cfg: DownloadConfig,
    gate: PartGate,
    retries: AtomicU32,
}

async fn download_ranged(
    store: &dyn ObjectStore,
    s3_path: &ObjectPath,
    tmp: &Path,
    cfg: &DownloadConfig,
    size: u64,
    etag: Option<String>,
    first_stream: BoxStream<'static, object_store::Result<Bytes>>,
) -> Result<Option<Downloaded>, Fail> {
    let file = preallocate(tmp, size).await?;
    let ranges = part_ranges(size, cfg.part_bytes);
    let ctx = PartContext {
        store,
        path: s3_path,
        etag,
        file: Arc::clone(&file),
        cfg: *cfg,
        gate: PartGate::new(cfg.part_concurrency),
        retries: AtomicU32::new(0),
    };
    let ctx = &ctx;
    let mut first_stream = Some(first_stream);
    let mut parts = Vec::with_capacity(ranges.len());
    for (index, range) in ranges.iter().cloned().enumerate() {
        let stream = if index == 0 {
            first_stream.take()
        } else {
            None
        };
        parts.push(async move {
            match stream {
                Some(stream) => part_from_stream(ctx, stream, range).await,
                None => fetch_part(ctx, range).await,
            }
        });
    }
    let count = parts.len() as u32;
    let mut inflight = futures::stream::iter(parts).buffer_unordered(count as usize);
    let mut total = 0u64;
    while let Some(len) = inflight.next().await {
        total += len?;
    }
    drop(inflight);
    verify_len(total, size, s3_path)?;
    let synced = Arc::clone(&file);
    run_blocking(move || synced.sync_data())
        .await
        .map_err(|e| Fail::Fatal(format!("failed to sync {}: {e}", tmp.display())))?;
    Ok(Some(Downloaded {
        object_bytes: size,
        parts: count,
        ranged: true,
        part_retries: ctx.retries.load(Ordering::Relaxed),
    }))
}

async fn preallocate(tmp: &Path, size: u64) -> Result<Arc<std::fs::File>, Fail> {
    let path = tmp.to_owned();
    run_blocking(move || {
        let file = std::fs::File::create(&path)?;
        file.set_len(size)?;
        Ok(file)
    })
    .await
    .map(Arc::new)
    .map_err(|e| {
        Fail::Fatal(format!(
            "failed to create SSTable download temp file {}: {e}",
            tmp.display()
        ))
    })
}

/// Run blocking file IO off the async workers. A panic in `f` surfaces as an
/// error rather than being swallowed.
async fn run_blocking<T, F>(f: F) -> std::io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| std::io::Error::other(format!("blocking file task failed: {e}")))?
}

async fn write_at(file: &Arc<std::fs::File>, offset: u64, data: Bytes) -> Result<(), Fail> {
    let file = Arc::clone(file);
    run_blocking(move || file.write_all_at(&data, offset))
        .await
        .map_err(|e| Fail::Fatal(format!("failed to write SSTable download at {offset}: {e}")))
}

/// Part 0 rides the request that revealed the object's size. Any failure
/// falls back to an ordinary part fetch, so it is retried like the rest.
async fn part_from_stream(
    ctx: &PartContext<'_>,
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
    range: Range<u64>,
) -> Result<u64, Fail> {
    let want = range.end - range.start;
    let mut offset = range.start;
    let mut pending: Vec<u8> = Vec::with_capacity(WRITE_BUFFER_BYTES);
    let mut taken = 0u64;
    let streamed: Result<(), String> = async {
        while taken < want {
            let Some(chunk) = stream.try_next().await.map_err(|e| e.to_string())? else {
                return Err(format!("stream ended after {taken} of {want} bytes"));
            };
            let take = (chunk.len() as u64).min(want - taken) as usize;
            pending.extend_from_slice(&chunk[..take]);
            taken += take as u64;
            if pending.len() >= WRITE_BUFFER_BYTES || taken == want {
                let data = Bytes::from(std::mem::take(&mut pending));
                let len = data.len() as u64;
                write_at(&ctx.file, offset, data)
                    .await
                    .map_err(|f| f.into_error().to_string())?;
                offset += len;
            }
        }
        // Read the body to its end so the connection is returned to the pool.
        if let Some(extra) = stream.try_next().await.map_err(|e| e.to_string())? {
            return Err(format!(
                "response is longer than its part: {} more bytes",
                extra.len()
            ));
        }
        Ok(())
    }
    .await;
    drop(stream);
    match streamed {
        Ok(()) => Ok(want),
        Err(msg) => {
            ctx.retries.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                path = %ctx.path,
                error = %msg,
                "first part of a ranged download failed on the initial request; refetching it"
            );
            fetch_part(ctx, range).await
        }
    }
}

/// Fetch and write one part, retrying that part alone.
async fn fetch_part(ctx: &PartContext<'_>, range: Range<u64>) -> Result<u64, Fail> {
    let mut backoff = ctx.cfg.retry_backoff;
    let mut attempt = 1;
    loop {
        let permit = ctx
            .gate
            .permits
            .acquire()
            .await
            .map_err(|e| Fail::Fatal(format!("download gate closed: {e}")))?;
        let events_before = super::throttle::rate_limit_events();
        let fetched = get_part(ctx, &range).await;
        let pressured = super::throttle::rate_limit_events() > events_before
            || matches!(&fetched, Err(PartError::RateLimited(_)));
        if pressured {
            ctx.gate.shrink(permit, ctx.path);
        } else {
            drop(permit);
        }
        match fetched {
            Ok(data) => {
                write_at(&ctx.file, range.start, data).await?;
                return Ok(range.end - range.start);
            }
            Err(PartError::Gone(msg)) => return Err(Fail::Fatal(msg)),
            Err(PartError::RateLimited(msg) | PartError::Other(msg)) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(Fail::Fatal(format!(
                        "part {}..{} of {} failed after {attempt} attempts: {msg}",
                        range.start, range.end, ctx.path
                    )));
                }
                ctx.retries.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    path = %ctx.path,
                    start = range.start,
                    end = range.end,
                    attempt,
                    error = %msg,
                    "ranged download part failed; retrying that part"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                attempt += 1;
            }
        }
    }
}

enum PartError {
    /// The object no longer exists or changed under us.
    Gone(String),
    RateLimited(String),
    Other(String),
}

async fn get_part(ctx: &PartContext<'_>, range: &Range<u64>) -> Result<Bytes, PartError> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(range.start as usize..range.end as usize)),
        if_match: ctx.etag.clone(),
        ..GetOptions::default()
    };
    let result = match ctx.store.get_opts(ctx.path, options).await {
        Ok(result) => result,
        Err(
            e @ (object_store::Error::NotFound { .. } | object_store::Error::Precondition { .. }),
        ) => {
            return Err(PartError::Gone(format!(
                "{} vanished or changed during a ranged download: {e}",
                ctx.path
            )))
        }
        Err(e) if super::throttle::is_rate_limited(&e) => {
            return Err(PartError::RateLimited(e.to_string()))
        }
        Err(e) => return Err(PartError::Other(e.to_string())),
    };
    let data = result
        .bytes()
        .await
        .map_err(|e| PartError::Other(format!("part body failed: {e}")))?;
    let want = range.end - range.start;
    if data.len() as u64 != want {
        return Err(PartError::Other(format!(
            "part {}..{} returned {} bytes, expected {want}",
            range.start,
            range.end,
            data.len()
        )));
    }
    Ok(data)
}

#[cfg(test)]
mod tests;
