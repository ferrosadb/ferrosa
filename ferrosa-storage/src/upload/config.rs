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
        })
    }

    /// HTTP client options for the S3 client. The timeout spans the whole
    /// response, body included, so it is what bounds a large download.
    ///
    /// `allow_http` lives here too: installing client options replaces the
    /// builder's own, so an `allow_http` set on the builder alone is lost.
    pub fn client_options(&self) -> object_store::ClientOptions {
        object_store::ClientOptions::new()
            .with_allow_http(self.allow_http)
            .with_timeout(self.request_timeout)
            .with_connect_timeout(std::time::Duration::from_secs(10))
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
            .with_client_options(self.client_options());

        if let Some(ref key_id) = self.access_key_id {
            builder = builder.with_access_key_id(key_id);
        }
        if let Some(ref secret) = self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }

        let store = builder.build().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to build S3 client: {e}"))
        })?;

        // Every path shares this one store, so the caps here bound uploads,
        // deletes, rehydration and restore together. 429 retry is always on:
        // object_store does not retry client errors.
        let store: std::sync::Arc<dyn ObjectStore> = match self.max_concurrent_requests {
            Some(max) => std::sync::Arc::new(object_store::limit::LimitStore::new(store, max)),
            None => std::sync::Arc::new(store),
        };
        if self.max_requests_per_second.is_some() || self.max_concurrent_requests.is_some() {
            tracing::info!(
                max_requests_per_second = ?self.max_requests_per_second,
                max_concurrent_requests = ?self.max_concurrent_requests,
                "object store requests are throttled"
            );
        }
        Ok(Box::new(super::throttle::ThrottledStore::new(
            store,
            self.max_requests_per_second,
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
        }
    }
}

/// Validate S3 bucket connectivity and write permissions at startup.
///
/// Performs a list + put + delete cycle to confirm the bucket is accessible
/// and writable. Returns warnings (non-fatal) or an error (fatal).
pub async fn validate_s3_bucket(store: &dyn ObjectStore) -> ferrosa_common::Result<Vec<String>> {
    let mut warnings = Vec::new();

    // Check connectivity — list root with a short prefix
    store.list_with_delimiter(None).await.map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!("S3 bucket not accessible: {e}"))
    })?;

    // Check write permission — write a test object
    let test_path = object_store::path::Path::from(".ferrosa/connectivity-check");
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
    required: bool,
) -> ferrosa_common::Result<Vec<String>> {
    match validate_s3_bucket(store).await {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let err = enforce_bucket_access(&unreachable_s3(), true)
            .await
            .expect_err("required S3 must not start against an unreachable bucket")
            .to_string();
        assert!(err.contains("FERROSA_S3_REQUIRED"), "{err}");
        assert!(err.to_lowercase().contains("s3"), "{err}");
    }

    #[tokio::test]
    async fn optional_s3_reports_an_unreachable_bucket_instead_of_hiding_it() {
        let warnings = enforce_bucket_access(&unreachable_s3(), false)
            .await
            .expect("not required: startup continues");
        assert_eq!(warnings.len(), 1, "the failure is returned, not swallowed");
    }

    #[tokio::test]
    async fn a_reachable_bucket_passes_whether_or_not_it_is_required() {
        for required in [false, true] {
            let store = object_store::memory::InMemory::new();
            assert!(enforce_bucket_access(&store, required)
                .await
                .unwrap()
                .is_empty());
        }
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
        assert!(store.contains("LimitStore(4"), "{store}");
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
            .with_client_options(cfg.client_options())
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
        let warnings = validate_s3_bucket(&store).await.unwrap();
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
