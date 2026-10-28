//! Object store configuration from environment variables.

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

/// Object store configuration.
///
/// By default this describes an S3-compatible backend whose settings are read
/// from `FERROSA_S3_*` environment variables, following 12-factor app
/// principles. When [`local_path`](Self::local_path) is `Some`, the engine
/// instead builds a durable local `file://` backend
/// ([`object_store::local::LocalFileSystem`]) rooted at that path — intended
/// for single-node deployments that want durable storage without S3. The S3
/// fields are ignored in that mode.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// When set, use a local `file://` backend rooted at this directory instead
    /// of S3. Single-node durability: flushed SSTables get a durable copy on
    /// local disk and eviction is disabled (disk is the durable store).
    ///
    /// The local backend does **not** support conditional PUT (CAS); the
    /// startup CAS probe detects this and manifest saves fall back to
    /// unconditional PUT. This is safe because a single node is the only
    /// manifest writer.
    pub local_path: Option<std::path::PathBuf>,
    /// S3-compatible endpoint URL (e.g., `https://s3.amazonaws.com`).
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Access key ID (optional — falls back to instance profile).
    pub access_key_id: Option<String>,
    /// Secret access key (optional).
    pub secret_access_key: Option<String>,
    /// Send every request unsigned (`FERROSA_S3_SKIP_SIGNATURE=true`; default
    /// `false`). For hosts with no credentials at all, whose access is granted
    /// by the bucket policy (for example to a VPC endpoint and the host's
    /// address) rather than by an identity. Never combined with access keys.
    pub skip_signature: bool,
    /// Allow non-TLS connections (for MinIO local dev).
    pub allow_http: bool,
    /// Key prefix for multi-tenant separation.
    pub prefix: String,
    /// Bounded upload queue depth (backpressure control).
    pub upload_queue_depth: usize,
    /// Number of concurrent upload workers.
    pub upload_workers: usize,
    /// Number of concurrent compaction-output upload workers.
    pub compaction_upload_workers: usize,
    /// Bounded compaction-output upload queue depth.
    pub compaction_upload_queue_depth: usize,
    /// Number of concurrent delete workers.
    pub delete_workers: usize,
    /// Client-side cap on object-store requests per second
    /// (`FERROSA_S3_MAX_REQUESTS_PER_SECOND`); `None` is unpaced. Keeps
    /// recovery from R2 under the bucket's rate limit.
    pub max_requests_per_second: Option<u32>,
    /// Client-side cap on object-store requests in flight
    /// (`FERROSA_S3_MAX_CONCURRENT_REQUESTS`); `None` is uncapped.
    pub max_concurrent_requests: Option<usize>,
    /// Per-request timeout, covering the whole response body
    /// (`FERROSA_S3_REQUEST_TIMEOUT_SECS`, default
    /// [`DEFAULT_S3_REQUEST_TIMEOUT_SECS`]).
    pub request_timeout: std::time::Duration,
    /// Longest a GET response body may send nothing before it fails as
    /// retryable and releases its request slot
    /// (`FERROSA_S3_BODY_IDLE_TIMEOUT_SECS`, default
    /// [`DEFAULT_S3_BODY_IDLE_TIMEOUT_SECS`]).
    pub body_idle_timeout: std::time::Duration,
}

impl ObjectStoreConfig {
    /// Reads configuration from environment variables.
    ///
    /// If `FERROSA_LOCAL_STORE_PATH` is set, returns a **local `file://`
    /// backend** configuration rooted at that path; the `FERROSA_S3_*`
    /// variables are ignored. Otherwise reads the S3 configuration, where
    /// `FERROSA_S3_ENDPOINT` and `FERROSA_S3_BUCKET` are required and the rest
    /// have defaults (region, allow_http, prefix, queue/worker counts).
    pub fn from_env() -> ferrosa_common::Result<Self> {
        // Shared worker/queue tuning is honored in both backends.
        let prefix = std::env::var("FERROSA_S3_PREFIX").unwrap_or_default();
        let upload_queue_depth = Self::queue_depth_from_env();
        let upload_workers = Self::workers_from_env("FERROSA_S3_UPLOAD_WORKERS", 8);
        let compaction_upload_workers =
            Self::workers_from_env("FERROSA_S3_COMPACTION_UPLOAD_WORKERS", 4);
        let compaction_upload_queue_depth =
            std::env::var("FERROSA_S3_COMPACTION_UPLOAD_QUEUE_DEPTH")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&v| v > 0)
                .unwrap_or(upload_queue_depth);
        let delete_workers = Self::workers_from_env("FERROSA_S3_DELETE_WORKERS", 2);
        let max_requests_per_second = parse_request_limit(
            "FERROSA_S3_MAX_REQUESTS_PER_SECOND",
            std::env::var("FERROSA_S3_MAX_REQUESTS_PER_SECOND")
                .ok()
                .as_deref(),
        )?;
        let max_concurrent_requests = parse_request_limit(
            "FERROSA_S3_MAX_CONCURRENT_REQUESTS",
            std::env::var("FERROSA_S3_MAX_CONCURRENT_REQUESTS")
                .ok()
                .as_deref(),
        )?;
        let request_timeout = parse_request_timeout(
            std::env::var("FERROSA_S3_REQUEST_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        )?;
        let body_idle_timeout = parse_body_idle_timeout(
            std::env::var("FERROSA_S3_BODY_IDLE_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        )?;
        // Validate the download tunables and the stats switch up front so a
        // typo fails startup instead of the first restore. Both are read once
        // per process; `build_object_store` and the download path use them.
        super::download::config()?;
        super::pool::config()?;
        super::stats::enabled_from_env()?;

        // Local file:// backend takes precedence when its path is set.
        if let Ok(local_path) = std::env::var("FERROSA_LOCAL_STORE_PATH") {
            if !local_path.trim().is_empty() {
                return Ok(Self {
                    local_path: Some(std::path::PathBuf::from(local_path)),
                    endpoint: String::new(),
                    bucket: String::new(),
                    region: String::new(),
                    access_key_id: None,
                    secret_access_key: None,
                    skip_signature: false,
                    allow_http: false,
                    prefix,
                    upload_queue_depth,
                    upload_workers,
                    compaction_upload_workers,
                    compaction_upload_queue_depth,
                    delete_workers,
                    max_requests_per_second,
                    max_concurrent_requests,
                    request_timeout,
                    body_idle_timeout,
                });
            }
        }

        let endpoint = std::env::var("FERROSA_S3_ENDPOINT").map_err(|_| {
            ferrosa_common::Error::InvalidFormat(
                "FERROSA_S3_ENDPOINT environment variable is required".into(),
            )
        })?;

        let bucket = std::env::var("FERROSA_S3_BUCKET").map_err(|_| {
            ferrosa_common::Error::InvalidFormat(
                "FERROSA_S3_BUCKET environment variable is required".into(),
            )
        })?;

        let region = std::env::var("FERROSA_S3_REGION").unwrap_or_else(|_| "us-east-1".into());

        let access_key_id = std::env::var("FERROSA_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = std::env::var("FERROSA_S3_SECRET_ACCESS_KEY").ok();
        let skip_signature = parse_skip_signature(
            std::env::var("FERROSA_S3_SKIP_SIGNATURE").ok().as_deref(),
            access_key_id.is_some() || secret_access_key.is_some(),
        )?;

        let allow_http = std::env::var("FERROSA_S3_ALLOW_HTTP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(false);

        Ok(Self {
            local_path: None,
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            skip_signature,
            allow_http,
            prefix,
            upload_queue_depth,
            upload_workers,
            compaction_upload_workers,
            compaction_upload_queue_depth,
            delete_workers,
            max_requests_per_second,
            max_concurrent_requests,
            request_timeout,
            body_idle_timeout,
        })
    }

    /// HTTP client options for the S3 client. The timeout spans the whole
    /// response, body included, so it is what bounds a large download.
    ///
    /// `allow_http` lives here too: installing client options replaces the
    /// builder's own, so an `allow_http` set on the builder alone is lost.
    ///
    /// This is the one place connection-pool settings live, so every path
    /// that shares the client (uploads, deletes, restore, rehydrate, ranged
    /// reads) inherits them. See [`Self::pool_max_idle_per_host`].
    pub fn client_options(&self) -> ferrosa_common::Result<object_store::ClientOptions> {
        let pool = super::pool::config()?;
        Ok(object_store::ClientOptions::new()
            .with_allow_http(self.allow_http)
            .with_timeout(self.request_timeout)
            .with_connect_timeout(pool.connect_timeout)
            .with_pool_max_idle_per_host(self.pool_max_idle_per_host()?)
            .with_pool_idle_timeout(pool.idle_timeout))
    }

    /// Cap on concurrent object-store requests: `FERROSA_S3_MAX_CONCURRENT_REQUESTS`
    /// when set, else [`super::pool::DEFAULT_MAX_IN_FLIGHT`], which is sized for
    /// a WAN link (bandwidth x round-trip time), not for this machine's cores.
    pub fn effective_max_in_flight(&self) -> usize {
        self.max_concurrent_requests
            .unwrap_or(super::pool::DEFAULT_MAX_IN_FLIGHT)
    }

    /// Idle connections kept per host: `FERROSA_S3_POOL_MAX_IDLE_PER_HOST`
    /// when set, else the in-flight limit. Never smaller than the in-flight
    /// limit, because a pool below peak in-flight reconnects on every burst.
    pub fn pool_max_idle_per_host(&self) -> ferrosa_common::Result<usize> {
        super::pool::config()?.max_idle_per_host(self.effective_max_in_flight())
    }

    /// Whether this configuration targets the local `file://` backend.
    pub fn is_local(&self) -> bool {
        self.local_path.is_some()
    }

    fn queue_depth_from_env() -> usize {
        std::env::var("FERROSA_S3_UPLOAD_QUEUE_DEPTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16)
    }

    fn workers_from_env(var: &str, default: usize) -> usize {
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(default)
    }

    /// Builds an `ObjectStore` instance from this configuration.
    ///
    /// When [`local_path`](Self::local_path) is set, builds a durable local
    /// `file://` backend rooted there (creating the directory if missing).
    /// Otherwise builds the S3-compatible client.
    pub fn build_object_store(&self) -> ferrosa_common::Result<Box<dyn ObjectStore>> {
        if let Some(ref path) = self.local_path {
            std::fs::create_dir_all(path).map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to create local object store dir {}: {e}",
                    path.display()
                ))
            })?;
            let store = LocalFileSystem::new_with_prefix(path).map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to build local file:// object store at {}: {e}",
                    path.display()
                ))
            })?;
            return Ok(Box::new(store));
        }

        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(self.client_options()?);

        if let Some(ref key_id) = self.access_key_id {
            builder = builder.with_access_key_id(key_id);
        }
        if let Some(ref secret) = self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if self.skip_signature {
            parse_skip_signature(
                Some("true"),
                self.access_key_id.is_some() || self.secret_access_key.is_some(),
            )?;
            tracing::warn!(
                bucket = %self.bucket,
                "object store requests are UNSIGNED (FERROSA_S3_SKIP_SIGNATURE=true): access rests on the bucket policy, not on a credential"
            );
            builder = builder.with_skip_signature(true);
        }

        let store = builder.build().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to build S3 client: {e}"))
        })?;

        // Stats sit innermost so they see every real request (each 429 retry
        // as its own) and latency without client-side pacing. Off unless
        // FERROSA_S3_STATS enabled the process-wide stats.
        let store: std::sync::Arc<dyn ObjectStore> = match super::stats::global() {
            Some(stats) => std::sync::Arc::new(super::stats::StatsStore::new(
                std::sync::Arc::new(store),
                std::sync::Arc::clone(stats),
            )),
            None => std::sync::Arc::new(store),
        };
        // Every path shares this one store, so the caps here bound uploads,
        // deletes, rehydration and restore together. 429 retry is always on:
        // object_store does not retry client errors.
        let max_in_flight = self.effective_max_in_flight();
        let pool_max_idle = self.pool_max_idle_per_host()?;
        let pool = super::pool::config()?;
        super::stats::record_pool_settings(max_in_flight, pool_max_idle);
        tracing::info!(
            max_in_flight,
            pool_max_idle_per_host = pool_max_idle,
            pool_idle_timeout_secs = pool.idle_timeout.as_secs(),
            connect_timeout_secs = pool.connect_timeout.as_secs(),
            max_requests_per_second = ?self.max_requests_per_second,
            body_idle_timeout_secs = self.body_idle_timeout.as_secs(),
            "object store client built"
        );
        for warning in validate_limits(
            max_in_flight,
            self.max_requests_per_second,
            STARTUP_FETCH_CONCURRENCY,
            &self.endpoint,
        ) {
            tracing::warn!("object store limits: {warning}");
        }
        Ok(Box::new(super::throttle::ThrottledStore::new(
            store,
            super::throttle::ThrottleLimits {
                requests_per_second: self.max_requests_per_second,
                max_in_flight,
                body_idle_timeout: self.body_idle_timeout,
                wait_warn_after: super::throttle::DEFAULT_WAIT_WARN_AFTER,
            },
            super::throttle::RateLimitRetry::default(),
        )))
    }

    /// Creates a test config pointing to an in-memory store.
    #[cfg(test)]
    pub fn test_config() -> Self {
        Self {
            local_path: None,
            endpoint: "http://localhost:9000".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: Some("minioadmin".into()),
            secret_access_key: Some("minioadmin".into()),
            skip_signature: false,
            allow_http: true,
            prefix: String::new(),
            upload_queue_depth: 16,
            upload_workers: 8,
            compaction_upload_workers: 4,
            compaction_upload_queue_depth: 16,
            delete_workers: 2,
            max_requests_per_second: None,
            max_concurrent_requests: None,
            request_timeout: std::time::Duration::from_secs(DEFAULT_S3_REQUEST_TIMEOUT_SECS),
            body_idle_timeout: std::time::Duration::from_secs(DEFAULT_S3_BODY_IDLE_TIMEOUT_SECS),
        }
    }
}

/// Generations startup registration of evicted SSTables fetches at once.
pub const STARTUP_FETCH_CONCURRENCY: usize = 16;

/// Round trip assumed when checking that a rate cap can keep the in-flight
/// limit busy: a GET to R2 measured 150-300 ms on 2026-10-10.
const ASSUMED_ROUND_TRIP_MS: usize = 300;

/// Fewest requests in flight that make sense against a remote endpoint: at
/// a 300 ms round trip, fewer cannot keep startup or a restore moving.
const MIN_REMOTE_IN_FLIGHT: usize = 8;

/// Check the client-side limits against each other and the endpoint. Each
/// returned string is a warning naming the variables involved; an empty
/// result means the limits are coherent. They are warnings, not errors: an
/// operator may set a low cap on purpose during an R2 429 storm.
///
/// - a rate cap `rps` keeps at most `rps x 0.3 s` requests in flight, so a
///   cap below `in_flight / 0.3` makes the in-flight limit unreachable;
/// - startup fetches `startup_fetch` generations at once, so an in-flight
///   limit below it queues startup behind the limit;
/// - fewer than 8 in flight against a remote endpoint starves every path.
pub fn validate_limits(
    in_flight: usize,
    requests_per_second: Option<u32>,
    startup_fetch: usize,
    endpoint: &str,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(rps) = requests_per_second {
        let sustained = rps as usize * ASSUMED_ROUND_TRIP_MS / 1000;
        if sustained < in_flight {
            warnings.push(format!(
                "FERROSA_S3_MAX_REQUESTS_PER_SECOND={rps} keeps at most {sustained} requests in \
                 flight at a {ASSUMED_ROUND_TRIP_MS} ms round trip, below \
                 FERROSA_S3_MAX_CONCURRENT_REQUESTS={in_flight}: the rate cap, not the in-flight \
                 limit, bounds throughput. Unset the rate cap unless the bucket answers 429"
            ));
        }
    }
    if in_flight < startup_fetch {
        warnings.push(format!(
            "FERROSA_S3_MAX_CONCURRENT_REQUESTS={in_flight} is below the startup fetch \
             concurrency {startup_fetch}: startup registration of evicted SSTables queues \
             behind the limit"
        ));
    }
    if in_flight < MIN_REMOTE_IN_FLIGHT && is_remote_endpoint(endpoint) {
        warnings.push(format!(
            "FERROSA_S3_MAX_CONCURRENT_REQUESTS={in_flight} is below {MIN_REMOTE_IN_FLIGHT} for \
             the remote endpoint {endpoint}: at a {ASSUMED_ROUND_TRIP_MS} ms round trip it \
             cannot keep startup or a restore moving"
        ));
    }
    warnings
}

/// Whether `endpoint` names a host other than this machine.
fn is_remote_endpoint(endpoint: &str) -> bool {
    let without_scheme = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    let authority = without_scheme.split('/').next().unwrap_or_default();
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => authority.split(':').next().unwrap_or_default(),
    };
    !(host.eq_ignore_ascii_case("localhost") || host == "::1" || host.starts_with("127."))
}

/// The object the startup check writes and deletes: `<prefix>/.ferrosa/connectivity-check`,
/// or `.ferrosa/connectivity-check` with no prefix.
pub fn connectivity_probe_path(prefix: &str) -> object_store::path::Path {
    if prefix.is_empty() {
        object_store::path::Path::from(".ferrosa/connectivity-check")
    } else {
        object_store::path::Path::from(format!("{prefix}/.ferrosa/connectivity-check"))
    }
}

/// Validate S3 bucket connectivity and write permissions at startup.
///
/// Performs a list + put + delete cycle to confirm the bucket is accessible
/// and writable. Returns warnings (non-fatal) or an error (fatal).
///
/// Everything happens under `prefix` (`FERROSA_S3_PREFIX`), the only part of the
/// bucket the node writes: a deployment that grants each node its own prefix, and
/// nothing at the bucket root, must pass. Checking the root instead failed every
/// such node, fatally when `FERROSA_S3_REQUIRED` is set.
pub async fn validate_s3_bucket(
    store: &dyn ObjectStore,
    prefix: &str,
) -> ferrosa_common::Result<Vec<String>> {
    let mut warnings = Vec::new();

    // Check connectivity — list the node's own prefix (the bucket root when none)
    let list_prefix = (!prefix.is_empty()).then(|| object_store::path::Path::from(prefix));
    store
        .list_with_delimiter(list_prefix.as_ref())
        .await
        .map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "S3 bucket not accessible under prefix {prefix:?}: {e}"
            ))
        })?;

    // Check write permission — write a test object
    let test_path = connectivity_probe_path(prefix);
    let test_data = bytes::Bytes::from_static(b"ok");
    store.put(&test_path, test_data.into()).await.map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!("S3 write permission check failed: {e}"))
    })?;

    // Clean up test object (best effort)
    if let Err(e) = store.delete(&test_path).await {
        warnings.push(format!(
            "S3 delete permission check failed (non-fatal): {e}"
        ));
    }

    Ok(warnings)
}

/// Parse `FERROSA_S3_REQUIRED`. Absent or empty means not required. A value that is
/// neither a recognised true nor false is an error: a safety flag that silently
/// reads as "off" on a typo would hide exactly the failure it exists to surface.
pub fn parse_s3_required(value: Option<&str>) -> Result<bool, String> {
    let Some(raw) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(false);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(format!(
            "FERROSA_S3_REQUIRED={raw:?} is not a boolean (use true or false)"
        )),
    }
}

/// Whether S3 storage is required (`FERROSA_S3_REQUIRED`).
pub fn s3_required_from_env() -> Result<bool, String> {
    parse_s3_required(std::env::var("FERROSA_S3_REQUIRED").ok().as_deref())
}

/// Decide the object store from the environment result.
///
/// Not required: a missing/invalid S3 configuration means local-only storage, as it
/// always did. The caller must log that; it is no longer silent. Required: a missing
/// or invalid configuration, or a local `file://` backend, is an error.
pub fn resolve_object_store(
    required: bool,
    from_env: ferrosa_common::Result<ObjectStoreConfig>,
) -> ferrosa_common::Result<Option<ObjectStoreConfig>> {
    match (required, from_env) {
        (false, Ok(config)) => Ok(Some(config)),
        (false, Err(reason)) => {
            tracing::warn!(
                %reason,
                "object store is not configured: running with LOCAL-ONLY storage \
                 (no S3 durability). Set FERROSA_S3_REQUIRED=true to make this an error."
            );
            Ok(None)
        }
        (true, Err(reason)) => Err(ferrosa_common::Error::InvalidFormat(format!(
            "FERROSA_S3_REQUIRED is set but the object store is not configured: {reason}"
        ))),
        (true, Ok(config)) if config.is_local() => Err(ferrosa_common::Error::InvalidFormat(
            "FERROSA_S3_REQUIRED is set but FERROSA_LOCAL_STORE_PATH selects a local \
             file:// backend; unset one of them"
                .into(),
        )),
        (true, Ok(config)) => Ok(Some(config)),
    }
}

/// Check bucket access at startup. Required: an access failure is an error naming
/// `FERROSA_S3_REQUIRED`. Not required: it is logged at WARN and returned as a
/// warning, never swallowed.
pub async fn enforce_bucket_access(
    store: &dyn ObjectStore,
    prefix: &str,
    required: bool,
) -> ferrosa_common::Result<Vec<String>> {
    match validate_s3_bucket(store, prefix).await {
        Ok(warnings) => Ok(warnings),
        Err(reason) if required => Err(ferrosa_common::Error::InvalidFormat(format!(
            "S3 access failed and FERROSA_S3_REQUIRED is set: {reason}"
        ))),
        Err(reason) => {
            tracing::warn!(
                %reason,
                "S3 access check failed; continuing because FERROSA_S3_REQUIRED is not set. \
                 SSTable uploads will fail until this is fixed."
            );
            Ok(vec![reason.to_string()])
        }
    }
}

/// Parse an optional positive request limit from environment variable `name`.
///
/// Unset or empty means no limit. Anything else must be a positive integer:
/// a typo silently meaning "unthrottled" is the setting that gets the bucket
/// rate-limited during recovery, so it is rejected, naming the variable.
pub fn parse_request_limit<T>(name: &str, value: Option<&str>) -> ferrosa_common::Result<Option<T>>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    let Some(raw) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    match raw.parse::<T>() {
        Ok(limit) if limit > T::default() => Ok(Some(limit)),
        _ => Err(ferrosa_common::Error::InvalidFormat(format!(
            "{name} must be a positive integer, got {raw:?}"
        ))),
    }
}

/// Default per-request timeout for object-store calls, in seconds.
///
/// It covers the whole response body, so it must fit the largest SSTable
/// component at the slowest link we expect: a 525 MB `Data.db` at under
/// 1 MB/s. object_store's own default is 30 s, which cut off every large
/// restore on 2026-09-30.
pub const DEFAULT_S3_REQUEST_TIMEOUT_SECS: u64 = 900;

/// Parse `FERROSA_S3_REQUEST_TIMEOUT_SECS`. Unset or empty means the default;
/// anything else must be a positive whole number of seconds, and a bad value
/// is rejected naming the variable rather than quietly using the default.
pub fn parse_request_timeout(value: Option<&str>) -> ferrosa_common::Result<std::time::Duration> {
    let secs = parse_request_limit::<u64>("FERROSA_S3_REQUEST_TIMEOUT_SECS", value)?
        .unwrap_or(DEFAULT_S3_REQUEST_TIMEOUT_SECS);
    Ok(std::time::Duration::from_secs(secs))
}

/// Default for `FERROSA_S3_BODY_IDLE_TIMEOUT_SECS`. A healthy body sends
/// something every few hundred milliseconds even at 1 MB/s; 30 s of silence is
/// a stalled connection, which on 2026-10-10 otherwise held its request slot
/// for the whole 900 s request timeout.
pub const DEFAULT_S3_BODY_IDLE_TIMEOUT_SECS: u64 = 30;

/// Parse `FERROSA_S3_BODY_IDLE_TIMEOUT_SECS`. Unset or empty means the
/// default; anything else must be a positive whole number of seconds, and a
/// bad value is rejected naming the variable.
pub fn parse_body_idle_timeout(value: Option<&str>) -> ferrosa_common::Result<std::time::Duration> {
    let secs = parse_request_limit::<u64>("FERROSA_S3_BODY_IDLE_TIMEOUT_SECS", value)?
        .unwrap_or(DEFAULT_S3_BODY_IDLE_TIMEOUT_SECS);
    Ok(std::time::Duration::from_secs(secs))
}

/// Parse `FERROSA_S3_SKIP_SIGNATURE`. Unset or empty is `false`; otherwise it
/// must be exactly `true` or `false`, and `true` is refused when access keys
/// are also configured, since a request cannot be both signed and unsigned.
pub fn parse_skip_signature(value: Option<&str>, has_keys: bool) -> ferrosa_common::Result<bool> {
    let skip = match value.map(str::trim) {
        None | Some("") | Some("false") => false,
        Some("true") => true,
        Some(other) => {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "FERROSA_S3_SKIP_SIGNATURE must be true or false, got {other:?}"
            )))
        }
    };
    if skip && has_keys {
        return Err(ferrosa_common::Error::InvalidFormat(
            "FERROSA_S3_SKIP_SIGNATURE=true cannot be combined with FERROSA_S3_ACCESS_KEY_ID or FERROSA_S3_SECRET_ACCESS_KEY".into(),
        ));
    }
    Ok(skip)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn skip_signature_is_off_unless_exactly_true() {
        assert!(!parse_skip_signature(None, false).unwrap());
        assert!(!parse_skip_signature(Some(""), false).unwrap());
        assert!(!parse_skip_signature(Some("false"), true).unwrap());
        assert!(parse_skip_signature(Some("true"), false).unwrap());
        assert!(parse_skip_signature(Some("yes"), false).is_err());
        assert!(parse_skip_signature(Some("1"), false).is_err());
    }

    #[test]
    fn skip_signature_refuses_access_keys() {
        let err = parse_skip_signature(Some("true"), true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be combined"), "{err}");
    }

    #[tokio::test]
    async fn an_unsigned_client_sends_no_authorization_header() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let n = sock.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
            let _ = sock.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
            req
        });
        let cfg = ObjectStoreConfig {
            endpoint: format!("http://{addr}"),
            bucket: "b".into(),
            allow_http: true,
            access_key_id: None,
            secret_access_key: None,
            skip_signature: true,
            ..ObjectStoreConfig::test_config()
        };
        let store = cfg.build_object_store().unwrap();
        let _ = store.head(&object_store::path::Path::from("k")).await;
        let req = server.join().unwrap();
        assert!(req.starts_with("head /b/k"), "{req}");
        assert!(
            !req.contains("authorization:"),
            "an unsigned request carried a signature: {req}"
        );
    }

    #[test]
    fn the_pool_is_sized_from_in_flight_and_ignores_workers_and_cores() {
        let cfg = ObjectStoreConfig::test_config();
        assert_eq!(
            cfg.effective_max_in_flight(),
            super::super::pool::DEFAULT_MAX_IN_FLIGHT
        );
        assert_eq!(
            cfg.pool_max_idle_per_host().unwrap(),
            cfg.effective_max_in_flight()
        );

        let busy = ObjectStoreConfig {
            upload_workers: 500,
            delete_workers: 500,
            ..ObjectStoreConfig::test_config()
        };
        assert_eq!(
            busy.pool_max_idle_per_host().unwrap(),
            cfg.pool_max_idle_per_host().unwrap(),
            "worker counts must not move the pool"
        );

        let wide = ObjectStoreConfig {
            max_concurrent_requests: Some(300),
            ..ObjectStoreConfig::test_config()
        };
        assert_eq!(wide.effective_max_in_flight(), 300);
        assert_eq!(wide.pool_max_idle_per_host().unwrap(), 300);
    }

    #[test]
    fn s3_required_parsing_is_strict() {
        assert_eq!(parse_s3_required(None), Ok(false));
        assert_eq!(parse_s3_required(Some("")), Ok(false));
        assert_eq!(parse_s3_required(Some("  ")), Ok(false));
        for on in ["1", "true", "TRUE", "True", "on", "yes"] {
            assert_eq!(parse_s3_required(Some(on)), Ok(true), "{on:?}");
        }
        for off in ["0", "false", "FALSE", "off", "no"] {
            assert_eq!(parse_s3_required(Some(off)), Ok(false), "{off:?}");
        }
        for bad in ["treu", "2", "enabled", "s3"] {
            assert!(
                parse_s3_required(Some(bad)).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    fn missing_config() -> ferrosa_common::Result<ObjectStoreConfig> {
        Err(ferrosa_common::Error::InvalidFormat(
            "FERROSA_S3_ENDPOINT environment variable is required".into(),
        ))
    }

    #[test]
    fn a_missing_s3_config_is_local_only_unless_s3_is_required() {
        assert!(resolve_object_store(false, missing_config())
            .unwrap()
            .is_none());
        let err = resolve_object_store(true, missing_config())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("FERROSA_S3_REQUIRED"),
            "names the switch: {err}"
        );
        assert!(
            err.contains("FERROSA_S3_ENDPOINT"),
            "keeps the cause: {err}"
        );
    }

    #[test]
    fn a_configured_s3_store_is_used_whether_or_not_it_is_required() {
        for required in [false, true] {
            let cfg = resolve_object_store(required, Ok(ObjectStoreConfig::test_config()))
                .unwrap()
                .expect("configured store");
            assert_eq!(cfg.bucket, "test-bucket");
        }
    }

    #[test]
    fn a_local_backend_does_not_satisfy_required_s3() {
        let mut local = ObjectStoreConfig::test_config();
        local.local_path = Some(std::path::PathBuf::from("/var/lib/ferrosa/store"));
        assert!(resolve_object_store(false, Ok(local.clone()))
            .unwrap()
            .is_some());
        let err = resolve_object_store(true, Ok(local))
            .unwrap_err()
            .to_string();
        assert!(err.contains("FERROSA_LOCAL_STORE_PATH"), "{err}");
    }

    /// An S3 endpoint nothing listens on, with retries off so the failure is
    /// immediate and deterministic.
    fn unreachable_s3() -> object_store::aws::AmazonS3 {
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint("http://127.0.0.1:1")
            .with_bucket_name("b")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_access_key_id("k")
            .with_secret_access_key("s")
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                retry_timeout: std::time::Duration::from_secs(1),
                ..Default::default()
            })
            .build()
            .expect("s3 client builds without touching the network")
    }

    #[tokio::test]
    async fn required_s3_fails_startup_when_the_bucket_is_unreachable() {
        let err = enforce_bucket_access(&unreachable_s3(), "", true)
            .await
            .expect_err("required S3 must not start against an unreachable bucket")
            .to_string();
        assert!(err.contains("FERROSA_S3_REQUIRED"), "{err}");
        assert!(err.to_lowercase().contains("s3"), "{err}");
    }

    #[tokio::test]
    async fn optional_s3_reports_an_unreachable_bucket_instead_of_hiding_it() {
        let warnings = enforce_bucket_access(&unreachable_s3(), "", false)
            .await
            .expect("not required: startup continues");
        assert_eq!(warnings.len(), 1, "the failure is returned, not swallowed");
    }

    #[tokio::test]
    async fn a_reachable_bucket_passes_whether_or_not_it_is_required() {
        for required in [false, true] {
            let store = object_store::memory::InMemory::new();
            assert!(enforce_bucket_access(&store, "", required)
                .await
                .unwrap()
                .is_empty());
        }
    }

    /// A store that, like a bucket policy granting one node its own prefix, refuses
    /// every path outside `scope/`.
    #[derive(Debug)]
    struct ScopedStore {
        inner: object_store::memory::InMemory,
        scope: String,
    }

    impl std::fmt::Display for ScopedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ScopedStore({})", self.scope)
        }
    }

    impl ScopedStore {
        fn new(scope: &str) -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                scope: scope.to_string(),
            }
        }

        fn check(&self, path: &object_store::path::Path) -> object_store::Result<()> {
            let p = path.as_ref();
            if p == self.scope || p.starts_with(&format!("{}/", self.scope)) {
                Ok(())
            } else {
                Err(object_store::Error::PermissionDenied {
                    path: p.to_string(),
                    source: format!("outside the granted prefix {}", self.scope).into(),
                })
            }
        }

        fn check_listing(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<()> {
            match prefix {
                Some(p) => self.check(p),
                None => self.check(&object_store::path::Path::from("")),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ScopedStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.check(location)?;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.check(location)?;
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.check(location)?;
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.check(location)?;
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            use futures::StreamExt;
            match self.check_listing(prefix) {
                Ok(()) => self.inner.list(prefix),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.check_listing(prefix)?;
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.check(from)?;
            self.check(to)?;
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.check(from)?;
            self.check(to)?;
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn the_access_check_stays_inside_the_configured_prefix() {
        // A node granted only database/database-1/* (the enterprise bucket policy) must
        // pass the required check: it lists and probes under its own prefix, not at the
        // bucket root.
        let store = ScopedStore::new("database/database-1");
        let warnings = enforce_bucket_access(&store, "database/database-1", true)
            .await
            .expect("a node with its own prefix granted passes the required check");
        assert!(warnings.is_empty(), "{warnings:?}");
        let left = store
            .inner
            .list_with_delimiter(None)
            .await
            .expect("list the backing store");
        assert!(
            left.objects.is_empty() && left.common_prefixes.is_empty(),
            "the probe is cleaned up: {left:?}"
        );
    }

    #[tokio::test]
    async fn the_access_check_still_fails_when_the_prefix_is_not_granted() {
        let store = ScopedStore::new("database/database-2");
        let err = enforce_bucket_access(&store, "database/database-1", true)
            .await
            .expect_err("a prefix the node may not write is still refused")
            .to_string();
        assert!(err.contains("FERROSA_S3_REQUIRED"), "{err}");
    }

    #[test]
    fn the_probe_path_is_under_the_prefix_and_at_the_root_without_one() {
        assert_eq!(
            connectivity_probe_path("database/database-1").as_ref(),
            "database/database-1/.ferrosa/connectivity-check"
        );
        assert_eq!(
            connectivity_probe_path("").as_ref(),
            ".ferrosa/connectivity-check"
        );
    }

    #[test]
    fn a_request_limit_is_off_when_unset_and_positive_when_set() {
        assert_eq!(parse_request_limit::<u32>("X", None).unwrap(), None);
        assert_eq!(parse_request_limit::<u32>("X", Some("")).unwrap(), None);
        assert_eq!(
            parse_request_limit::<u32>("X", Some("25")).unwrap(),
            Some(25)
        );
    }

    /// A typo in a throttle must not silently mean "unthrottled" — that is
    /// exactly the setting that gets the bucket rate-limited during recovery.
    #[test]
    fn an_invalid_request_limit_is_rejected_naming_the_variable() {
        for bad in ["0", "-1", "fast", "2.5"] {
            let err = parse_request_limit::<u32>("FERROSA_S3_MAX_REQUESTS_PER_SECOND", Some(bad))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("FERROSA_S3_MAX_REQUESTS_PER_SECOND"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn a_configured_s3_store_is_throttled_and_concurrency_capped() {
        let mut cfg = ObjectStoreConfig::test_config();
        cfg.max_requests_per_second = Some(20);
        cfg.max_concurrent_requests = Some(4);
        let store = cfg.build_object_store().unwrap().to_string();
        assert!(store.starts_with("ThrottledStore("), "{store}");
        assert!(store.contains("max_in_flight=4"), "{store}");
    }

    /// An HTTP server that answers every request with a 10-byte body sent one
    /// byte every `gap`, so the whole response takes about `10 * gap`.
    async fn slow_body_server(gap: std::time::Duration) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    if sock.read(&mut buf).await.is_err() {
                        return;
                    }
                    let head = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"e\"\r\n\
                                Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\n";
                    if sock.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    for b in b"0123456789" {
                        tokio::time::sleep(gap).await;
                        if sock.write_all(&[*b]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    fn s3_against(endpoint: &str, timeout_secs: u64) -> object_store::aws::AmazonS3 {
        let mut cfg = ObjectStoreConfig::test_config();
        cfg.request_timeout = std::time::Duration::from_secs(timeout_secs);
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name("b")
            .with_region("us-east-1")
            .with_access_key_id("k")
            .with_secret_access_key("s")
            // allow_http comes only from client_options(), as in production.
            .with_client_options(cfg.client_options().unwrap())
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                retry_timeout: std::time::Duration::from_secs(1),
                ..Default::default()
            })
            .build()
            .unwrap()
    }

    /// 2026-09-30: restoring a 479 MB evicted Data.db from R2 failed four
    /// times with "error decoding response body" and the node exited. The
    /// client had no options, so object_store's 30 s default request timeout
    /// — which covers the body — cut off every download that took longer.
    /// The timeout must be the configured one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_download_slower_than_the_request_timeout_fails_and_a_longer_timeout_completes_it() {
        use object_store::ObjectStore;
        let endpoint = slow_body_server(std::time::Duration::from_millis(200)).await;
        let path = object_store::path::Path::from("big-Data.db");

        let short = s3_against(&endpoint, 1);
        let cut_off = match short.get(&path).await {
            Ok(r) => r.bytes().await.map(|_| ()),
            Err(e) => Err(e),
        };
        // It must fail on the body timeout, not on anything earlier (a bad
        // scheme or refused connection would pass a bare is_err()).
        let err = format!(
            "{:?}",
            cut_off.expect_err("a 2 s body must not fit a 1 s timeout")
        );
        // reqwest Decode -> Body -> TimedOut: the "error decoding response
        // body" that ended the live restore.
        assert!(
            err.contains("Decode") && err.contains("TimedOut"),
            "expected a body timeout, got {err}"
        );

        let long = s3_against(&endpoint, 10);
        let body = long.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"0123456789");
    }

    /// An HTTP object server for the 10-byte object `0123456789` that honours
    /// `Range` (206 with `Content-Range`). The first `stalls` responses send
    /// their headers and one body byte, then nothing more while holding the
    /// connection open: the shape of the 2026-10-10 stalled R2 GETs. Later
    /// responses are sent whole.
    async fn stalling_body_server(stalls: usize) -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let answered = std::sync::Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let answered = std::sync::Arc::clone(&answered);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = match sock.read(&mut buf).await {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    let head = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
                    let ranged = head.contains("range: bytes=");
                    let (status, content_range) = if ranged {
                        ("206 Partial Content", "Content-Range: bytes 0-9/10\r\n")
                    } else {
                        ("200 OK", "")
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: 10\r\n{content_range}ETag: \"e\"\r\n\
                         Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\n"
                    );
                    if sock.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                    if answered.fetch_add(1, Ordering::SeqCst) < stalls {
                        if sock.write_all(b"0").await.is_err() {
                            return;
                        }
                        // Hold the connection open and send nothing more.
                        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                        return;
                    }
                    let _ = sock.write_all(b"0123456789").await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// A store built the production way against `endpoint`, with ONE request
    /// in flight, so a stalled body that kept its permit would block every
    /// later request.
    fn one_permit_store_against(endpoint: &str) -> Box<dyn ObjectStore> {
        let cfg = ObjectStoreConfig {
            endpoint: endpoint.to_string(),
            max_concurrent_requests: Some(1),
            body_idle_timeout: std::time::Duration::from_secs(1),
            ..ObjectStoreConfig::test_config()
        };
        cfg.build_object_store().unwrap()
    }

    /// T-F5-1 (2026-10-10): stalled GET bodies held every object-store permit
    /// for the 900 s request timeout, so startup sat idle with open
    /// connections. A body that yields nothing for the idle timeout must fail
    /// as retryable, release its permit, and the retry must complete the
    /// download. Bound: idle timeout 1 s plus a 1 ms retry backoff; measured
    /// at about 1.0 s, asserted under 3 s.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_get_whose_body_stalls_fails_on_the_idle_timeout() {
        let endpoint = stalling_body_server(1).await;
        let store = one_permit_store_against(&endpoint);
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("1-Data.db");
        let dl = super::super::download::DownloadConfig {
            retry_backoff: std::time::Duration::from_millis(1),
            ..super::super::download::DownloadConfig::default()
        };
        let started = std::time::Instant::now();

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(6),
            super::super::download::download_component(
                store.as_ref(),
                &object_store::path::Path::from("p/1-Data.db"),
                &out,
                &dl,
            ),
        )
        .await
        .expect("a stalled body must fail on the idle timeout, not hold the download for 900 s");

        assert_eq!(outcome.unwrap(), Some(10), "the retry downloads the object");
        assert_eq!(std::fs::read(&out).unwrap(), b"0123456789");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "took {:?}",
            started.elapsed()
        );
    }

    /// T-F5-1, the `.bytes()` consumers (evicted-read small components, ranged
    /// parts, `get_range`): a body that stalls fails with an error naming the
    /// idle-timeout variable instead of hanging.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_body_read_whole_or_by_range_fails_naming_the_idle_timeout() {
        let endpoint = stalling_body_server(usize::MAX).await;
        let store = one_permit_store_against(&endpoint);
        let path = object_store::path::Path::from("p/1-Index.db");

        for how in ["get().bytes()", "get_range"] {
            let started = std::time::Instant::now();
            let read = async {
                match how {
                    "get_range" => store.get_range(&path, 0..10).await,
                    _ => match store.get(&path).await {
                        Ok(r) => r.bytes().await,
                        Err(e) => Err(e),
                    },
                }
            };
            let err = tokio::time::timeout(std::time::Duration::from_secs(6), read)
                .await
                .unwrap_or_else(|_| panic!("{how}: a stalled body hung past 6 s"))
                .expect_err("a stalled body must fail");
            assert!(
                err.to_string()
                    .contains("FERROSA_S3_BODY_IDLE_TIMEOUT_SECS"),
                "{how}: {err}"
            );
            assert!(
                started.elapsed() < std::time::Duration::from_secs(3),
                "{how}: took {:?}",
                started.elapsed()
            );
        }
    }

    /// Captures formatted tracing output so a test can assert on log lines.
    #[derive(Clone, Default)]
    pub(crate) struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl CapturedLogs {
        pub(crate) fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log buffer")).into_owned()
        }

        pub(crate) fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
            tracing_subscriber::fmt()
                .with_writer(self.clone())
                .with_max_level(tracing::Level::INFO)
                .with_ansi(false)
                .finish()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Build `cfg`'s store and return the WARN lines it logged.
    fn warnings_building(cfg: &ObjectStoreConfig) -> Vec<String> {
        let logs = CapturedLogs::default();
        tracing::subscriber::with_default(logs.subscriber(), || {
            cfg.build_object_store().unwrap();
        });
        logs.text()
            .lines()
            .filter(|l| l.contains("WARN"))
            .map(str::to_owned)
            .collect()
    }

    /// T-F5-2: the nodes ran `rps=20` with 64 requests in flight. 20 requests
    /// a second at a 300 ms round trip keep only 6 in flight, so the in-flight
    /// limit was unreachable and nothing said so. Building the store must warn,
    /// naming both variables and their values.
    #[test]
    fn misconfigured_limits_warn_naming_both_variables() {
        let paced = ObjectStoreConfig {
            max_requests_per_second: Some(20),
            max_concurrent_requests: Some(64),
            ..ObjectStoreConfig::test_config()
        };
        let warned = warnings_building(&paced).join("\n");
        assert!(
            warned.contains("FERROSA_S3_MAX_REQUESTS_PER_SECOND=20")
                && warned.contains("FERROSA_S3_MAX_CONCURRENT_REQUESTS=64"),
            "{warned}"
        );

        // Startup fetches 16 generations at once; 8 in flight starves it.
        let narrow = ObjectStoreConfig {
            max_concurrent_requests: Some(8),
            ..ObjectStoreConfig::test_config()
        };
        let warned = warnings_building(&narrow).join("\n");
        assert!(
            warned.contains("FERROSA_S3_MAX_CONCURRENT_REQUESTS=8")
                && warned.contains("startup fetch concurrency 16"),
            "{warned}"
        );

        // The incident's in-flight 4 against a remote endpoint.
        let remote_four = ObjectStoreConfig {
            endpoint: "https://account.r2.cloudflarestorage.com".into(),
            max_concurrent_requests: Some(4),
            ..ObjectStoreConfig::test_config()
        };
        let warned = warnings_building(&remote_four).join("\n");
        assert!(
            warned.contains("FERROSA_S3_MAX_CONCURRENT_REQUESTS=4") && warned.contains("below 8"),
            "{warned}"
        );

        // The defaults are sane: no limit warning at all.
        let defaults = warnings_building(&ObjectStoreConfig::test_config());
        assert!(defaults.is_empty(), "{defaults:?}");
    }

    /// T-F5-6: the nodes ran a build that predated the 64 in-flight default.
    /// Unset limits mean 64 in flight, no pacing, and a 30 s body idle timeout.
    #[test]
    fn default_limits_are_64_in_flight_and_unpaced() {
        let cfg = ObjectStoreConfig::test_config();
        assert_eq!(cfg.max_concurrent_requests, None);
        assert_eq!(cfg.effective_max_in_flight(), 64);
        assert_eq!(
            parse_request_limit::<u32>("FERROSA_S3_MAX_REQUESTS_PER_SECOND", None).unwrap(),
            None
        );
        let built = format!("{:?}", cfg.build_object_store().unwrap());
        assert!(built.contains("pacer: None"), "{built}");
        assert!(built.contains("max_in_flight: 64"), "{built}");
        assert_eq!(
            parse_body_idle_timeout(None).unwrap(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(cfg.body_idle_timeout, std::time::Duration::from_secs(30));
        assert!(built.contains("body_idle_timeout: 30s"), "{built}");
    }

    #[test]
    fn an_invalid_body_idle_timeout_is_rejected_naming_the_variable() {
        for bad in ["0", "-1", "soon", "1.5"] {
            let err = parse_body_idle_timeout(Some(bad)).unwrap_err().to_string();
            assert!(
                err.contains("FERROSA_S3_BODY_IDLE_TIMEOUT_SECS"),
                "{bad}: {err}"
            );
        }
        assert_eq!(
            parse_body_idle_timeout(Some("5")).unwrap(),
            std::time::Duration::from_secs(5)
        );
    }

    #[test]
    fn the_request_timeout_defaults_long_enough_for_large_sstables() {
        assert_eq!(
            parse_request_timeout(None).unwrap(),
            std::time::Duration::from_secs(DEFAULT_S3_REQUEST_TIMEOUT_SECS)
        );
        const { assert!(DEFAULT_S3_REQUEST_TIMEOUT_SECS >= 600) };
        assert_eq!(
            parse_request_timeout(Some("120")).unwrap(),
            std::time::Duration::from_secs(120)
        );
    }

    #[test]
    fn an_invalid_request_timeout_is_rejected_naming_the_variable() {
        for bad in ["0", "-5", "soon", "1.5"] {
            let err = parse_request_timeout(Some(bad)).unwrap_err().to_string();
            assert!(
                err.contains("FERROSA_S3_REQUEST_TIMEOUT_SECS"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = ObjectStoreConfig::test_config();
        assert_eq!(config.region, "us-east-1");
        assert!(config.allow_http);
        assert_eq!(config.upload_queue_depth, 16);
        assert_eq!(config.upload_workers, 8);
        assert_eq!(config.compaction_upload_workers, 4);
        assert_eq!(config.compaction_upload_queue_depth, 16);
        assert_eq!(config.delete_workers, 2);
    }

    #[tokio::test]
    async fn validate_s3_bucket_succeeds_with_in_memory_store() {
        let store = object_store::memory::InMemory::new();
        let warnings = validate_s3_bucket(&store, "").await.unwrap();
        assert!(warnings.is_empty());
    }

    #[tokio::test]
    async fn local_object_store_round_trips() {
        use object_store::path::Path as ObjectPath;

        let dir = tempfile::tempdir().unwrap();
        let store_root = dir.path().join("does-not-exist-yet");
        let config = ObjectStoreConfig {
            local_path: Some(store_root.clone()),
            ..ObjectStoreConfig::test_config()
        };
        assert!(config.is_local());

        // build_object_store creates the missing directory and returns a
        // LocalFileSystem rooted there.
        let store = config.build_object_store().unwrap();
        assert!(store_root.is_dir(), "build must create the store dir");

        let path = ObjectPath::from("sub/dir/blob.bin");
        let payload = bytes::Bytes::from_static(b"durable-local-bytes");
        store.put(&path, payload.clone().into()).await.unwrap();

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, payload);

        // The blob is a real file on disk under the store root.
        assert!(store_root.join("sub/dir/blob.bin").is_file());
    }
}
