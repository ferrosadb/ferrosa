//! Remote index build backend and engine backend configuration.
//!
//! [`RemoteBackend`] implements [`IndexBuildBackend`] by delegating index
//! builds to an external `ferrosa-index-builder` process over HTTP. It
//! includes per-endpoint circuit breakers and a [`LocalBackend`] fallback.
//!
//! [`IndexBackendConfig`] is the engine-level enum that controls which
//! backend the [`super::IndexBuildScheduler`] uses (or disables it entirely).

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::scheduler::{IndexBuildBackend, IndexBuildJob, IndexBuildResult, LocalBackend};

// ── Engine backend configuration ────────────────────────────────────────────

/// Controls how the engine handles secondary index builds.
#[derive(Debug, Clone, Default)]
pub enum IndexBackendConfig {
    /// Build indexes in-process using [`LocalBackend`] (default).
    #[default]
    Local,
    /// Delegate to external `ferrosa-index-builder` instances via HTTP.
    /// Falls back to [`LocalBackend`] when all endpoints are unhealthy.
    Remote {
        /// One or more builder HTTP endpoints (e.g. `http://builder:8090`).
        endpoints: Vec<String>,
        /// Per-request timeout.
        timeout: Duration,
        /// Max retries per endpoint for transient failures.
        max_retries: u32,
        /// Consecutive failures before tripping the circuit breaker.
        circuit_breaker_threshold: u32,
        /// How long to wait before a half-open probe after tripping.
        circuit_breaker_recovery: Duration,
    },
    /// Disable index building entirely. An external `ferrosa-index-builder`
    /// running in pull mode handles all index construction.
    Off,
}

impl IndexBackendConfig {
    /// Parse from environment variables.
    ///
    /// - `FERROSA_INDEX_BACKEND`: `local` (default), `remote`, or `off`
    /// - `FERROSA_INDEX_SIDECAR_ENDPOINTS`: comma-separated URLs
    /// - `FERROSA_INDEX_SIDECAR_TIMEOUT_MS`: per-request timeout (default 30000)
    /// - `FERROSA_INDEX_SIDECAR_MAX_RETRIES`: retries per endpoint (default 2)
    /// - `FERROSA_INDEX_CB_THRESHOLD`: circuit breaker threshold (default 5)
    /// - `FERROSA_INDEX_CB_RECOVERY_MS`: circuit breaker recovery (default 60000)
    pub fn from_env() -> Self {
        let backend = std::env::var("FERROSA_INDEX_BACKEND")
            .unwrap_or_else(|_| "local".into())
            .to_lowercase();

        match backend.as_str() {
            "off" | "disabled" => Self::Off,
            "remote" => {
                let endpoints: Vec<String> = std::env::var("FERROSA_INDEX_SIDECAR_ENDPOINTS")
                    .unwrap_or_default()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if endpoints.is_empty() {
                    tracing::warn!(
                        "FERROSA_INDEX_BACKEND=remote but no FERROSA_INDEX_SIDECAR_ENDPOINTS set, \
                         falling back to local"
                    );
                    return Self::Local;
                }

                let timeout_ms: u64 = std::env::var("FERROSA_INDEX_SIDECAR_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30_000);

                let max_retries: u32 = std::env::var("FERROSA_INDEX_SIDECAR_MAX_RETRIES")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2);

                let cb_threshold: u32 = std::env::var("FERROSA_INDEX_CB_THRESHOLD")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5);

                let cb_recovery_ms: u64 = std::env::var("FERROSA_INDEX_CB_RECOVERY_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60_000);

                Self::Remote {
                    endpoints,
                    timeout: Duration::from_millis(timeout_ms),
                    max_retries,
                    circuit_breaker_threshold: cb_threshold,
                    circuit_breaker_recovery: Duration::from_millis(cb_recovery_ms),
                }
            }
            _ => Self::Local,
        }
    }
}

// ── S3 path resolution ─────────────────────��─────────────────────────���──────

/// Resolves SSTable IDs to S3 prefixes using the same layout as
/// [`UploadManager`](crate::upload::UploadManager).
#[derive(Debug, Clone)]
pub struct S3PathResolver {
    pub bucket: String,
    pub endpoint: String,
    pub prefix: String,
}

impl S3PathResolver {
    /// Build from the existing `ObjectStoreConfig` if available.
    pub fn from_object_store_config(config: &crate::upload::ObjectStoreConfig) -> Self {
        Self {
            bucket: config.bucket.clone(),
            endpoint: config.endpoint.clone(),
            prefix: config.prefix.clone(),
        }
    }

    /// Resolve the S3 prefix for the given SSTable.
    ///
    /// Format: `{prefix}/{hex_prefix}/{table_id}/{sstable_id}`
    /// where `hex_prefix` = first 2 hex chars of hash(sstable_id),
    /// matching [`crate::upload::manager::hex_prefix_for`].
    pub fn resolve(&self, table_id: &str, sstable_id: &str) -> String {
        let hex = crate::upload::hex_prefix_for(sstable_id);
        if self.prefix.is_empty() {
            format!("{hex}/{table_id}/{sstable_id}")
        } else {
            format!("{}/{hex}/{table_id}/{sstable_id}", self.prefix)
        }
    }
}

// ── Circuit breaker ─────────────────────────────────────────────────────────

/// Per-endpoint health state: closed (healthy), open (tripped), half-open (probing).
#[derive(Debug)]
struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    failure_threshold: u32,
    tripped_at: Mutex<Option<Instant>>,
    recovery_timeout: Duration,
}

impl CircuitBreaker {
    fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            failure_threshold,
            tripped_at: Mutex::new(None),
            recovery_timeout,
        }
    }

    /// Returns `true` if the endpoint should receive requests.
    fn is_available(&self) -> bool {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures < self.failure_threshold {
            return true; // closed
        }
        // Check if recovery timeout has elapsed (half-open).
        let guard = self.tripped_at.lock();
        if let Some(tripped) = *guard {
            tripped.elapsed() >= self.recovery_timeout
        } else {
            false
        }
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        *self.tripped_at.lock() = None;
    }

    fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        if prev + 1 >= self.failure_threshold {
            let mut guard = self.tripped_at.lock();
            if guard.is_none() {
                *guard = Some(Instant::now());
            }
        }
    }

    /// 0 = closed, 1 = open, 2 = half-open.
    #[allow(dead_code)]
    fn state(&self) -> u8 {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures < self.failure_threshold {
            return 0; // closed
        }
        let guard = self.tripped_at.lock();
        if let Some(tripped) = *guard {
            if tripped.elapsed() >= self.recovery_timeout {
                2 // half-open
            } else {
                1 // open
            }
        } else {
            1 // open
        }
    }
}

// ── Remote backend ──────────────────────────────────────────────────────────

/// Index build backend that delegates to external `ferrosa-index-builder`
/// instances over HTTP.
///
/// Uses a round-robin endpoint selection with per-endpoint circuit breakers.
/// Falls back to [`LocalBackend`] when all endpoints are unhealthy.
pub struct RemoteBackend {
    endpoints: Vec<String>,
    s3_resolver: S3PathResolver,
    timeout: Duration,
    max_retries: u32,
    circuit_breakers: Vec<CircuitBreaker>,
    local_fallback: LocalBackend,
    /// Round-robin counter for endpoint selection.
    next_endpoint: AtomicU32,
}

impl RemoteBackend {
    /// Create a new `RemoteBackend`.
    pub fn new(
        endpoints: Vec<String>,
        s3_resolver: S3PathResolver,
        timeout: Duration,
        max_retries: u32,
        cb_threshold: u32,
        cb_recovery: Duration,
        local_fallback: LocalBackend,
    ) -> Self {
        let circuit_breakers = endpoints
            .iter()
            .map(|_| CircuitBreaker::new(cb_threshold, cb_recovery))
            .collect();

        Self {
            endpoints,
            s3_resolver,
            timeout,
            max_retries,
            circuit_breakers,
            local_fallback,
            next_endpoint: AtomicU32::new(0),
        }
    }

    /// Send a build request to the given endpoint. Returns the parsed response
    /// or an error string.
    fn send_request(
        &self,
        endpoint_idx: usize,
        job: &IndexBuildJob,
    ) -> Result<BuildResponse, String> {
        let endpoint = &self.endpoints[endpoint_idx];
        let url = format!("{endpoint}/internal/index/build");

        let table_id = format!("{}.{}", job.table.0, job.table.1);
        let s3_prefix = self.s3_resolver.resolve(&table_id, &job.sstable_id);

        let body = serde_json::json!({
            "sstable_id": job.sstable_id,
            "index_name": job.index_name,
            "index_type": format!("{:?}", job.index_type).to_lowercase(),
            "s3_endpoint": self.s3_resolver.endpoint,
            "s3_bucket": self.s3_resolver.bucket,
            "s3_prefix": s3_prefix,
            "table": [&job.table.0, &job.table.1],
            "column_position": job.column_position,
            "priority": match job.priority {
                super::scheduler::BuildPriority::High => "high",
                super::scheduler::BuildPriority::Normal => "normal",
                super::scheduler::BuildPriority::Initial => "initial",
            },
        });

        let config = ureq::config::Config::builder()
            .timeout_global(Some(self.timeout))
            .build();
        let agent = config.new_agent();

        let response = agent
            .post(&url)
            .header("Content-Type", "application/json")
            .send_json(&body)
            .map_err(|e| format!("HTTP request to {endpoint} failed: {e}"))?;

        let status = response.status();
        if status.as_u16() >= 400 {
            return Err(format!("HTTP {status} from {endpoint}"));
        }

        let body_str = response
            .into_body()
            .read_to_string()
            .map_err(|e| format!("failed to read response from {endpoint}: {e}"))?;

        let parsed: BuildResponse = serde_json::from_str(&body_str)
            .map_err(|e| format!("failed to parse response from {endpoint}: {e}"))?;

        Ok(parsed)
    }

    /// Try sending to one endpoint with retries. Returns Ok on success,
    /// Err on exhausting retries (transient) or permanent failure.
    fn try_endpoint(
        &self,
        endpoint_idx: usize,
        job: &IndexBuildJob,
    ) -> Result<IndexBuildResult, BuildError> {
        for attempt in 0..=self.max_retries {
            match self.send_request(endpoint_idx, job) {
                Ok(resp) => {
                    self.circuit_breakers[endpoint_idx].record_success();
                    if resp.status == "completed" {
                        return Ok(IndexBuildResult {
                            sstable_id: job.sstable_id.clone(),
                            sidecar_entries: std::collections::HashMap::new(),
                            build_duration: Duration::from_millis(resp.elapsed_ms.unwrap_or(0)),
                            sidecar_written_to_s3: true,
                        });
                    }
                    // Application-level failure — permanent, don't retry.
                    let msg = resp.error.unwrap_or_else(|| "unknown error".into());
                    return Err(BuildError::Permanent(msg));
                }
                Err(e) => {
                    if attempt == self.max_retries {
                        self.circuit_breakers[endpoint_idx].record_failure();
                        return Err(BuildError::Transient(e));
                    }
                    // Brief pause before retry.
                    std::thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                }
            }
        }
        unreachable!()
    }
}

impl IndexBuildBackend for RemoteBackend {
    fn build(&self, job: &IndexBuildJob) -> Result<IndexBuildResult, String> {
        // Try each available endpoint.
        let n = self.endpoints.len();
        let start = self.next_endpoint.fetch_add(1, Ordering::Relaxed) as usize;
        let mut last_error = String::new();

        for i in 0..n {
            let idx = (start + i) % n;
            if !self.circuit_breakers[idx].is_available() {
                continue;
            }
            match self.try_endpoint(idx, job) {
                Ok(result) => return Ok(result),
                Err(BuildError::Permanent(e)) => return Err(e),
                Err(BuildError::Transient(e)) => {
                    last_error = e;
                    continue;
                }
            }
        }

        // All endpoints exhausted or tripped — fall back to local.
        tracing::warn!(
            sstable_id = %job.sstable_id,
            index_name = %job.index_name,
            last_error = %last_error,
            "all remote index builder endpoints unhealthy, using local fallback"
        );
        self.local_fallback.build(job)
    }
}

// ── HTTP response types ───────────────────────���─────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct BuildResponse {
    status: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    elapsed_ms: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    sidecar_s3_path: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    entries_built: Option<u64>,
}

enum BuildError {
    /// Transient HTTP/network failure — try next endpoint.
    Transient(String),
    /// Application-level failure — do not retry.
    Permanent(String),
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_backend_config_default_is_local() {
        let config = IndexBackendConfig::default();
        assert!(matches!(config, IndexBackendConfig::Local));
    }

    #[test]
    fn circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(60));
        assert!(cb.is_available());
        assert_eq!(cb.state(), 0);
    }

    #[test]
    fn circuit_breaker_trips_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(60));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_available()); // 2 < 3
        cb.record_failure();
        assert!(!cb.is_available()); // 3 >= 3, tripped
        assert_eq!(cb.state(), 1); // open
    }

    #[test]
    fn circuit_breaker_resets_on_success() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(60));
        cb.record_failure();
        cb.record_success();
        assert!(cb.is_available());
        assert_eq!(cb.state(), 0);
    }

    #[test]
    fn circuit_breaker_half_open_after_recovery() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(10));
        cb.record_failure();
        assert!(!cb.is_available()); // open
        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.is_available()); // half-open
        assert_eq!(cb.state(), 2);
    }

    #[test]
    fn s3_path_resolver_format() {
        let resolver = S3PathResolver {
            bucket: "test-bucket".into(),
            endpoint: "http://localhost:9000".into(),
            prefix: "prod".into(),
        };
        let path = resolver.resolve("ks.users", "gen-42");
        // Should contain hex prefix, table_id, sstable_id.
        assert!(path.starts_with("prod/"));
        assert!(path.contains("ks.users"));
        assert!(path.ends_with("gen-42"));
    }

    #[test]
    fn s3_path_resolver_empty_prefix() {
        let resolver = S3PathResolver {
            bucket: "test-bucket".into(),
            endpoint: "http://localhost:9000".into(),
            prefix: String::new(),
        };
        let path = resolver.resolve("ks.users", "gen-42");
        // No leading prefix — starts with hex.
        assert!(!path.starts_with('/'));
        assert!(path.contains("ks.users"));
        assert!(path.ends_with("gen-42"));
    }
}
