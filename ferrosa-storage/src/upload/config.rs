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
        })
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
            .with_conditional_put(S3ConditionalPut::ETagMatch);

        if let Some(ref key_id) = self.access_key_id {
            builder = builder.with_access_key_id(key_id);
        }
        if let Some(ref secret) = self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }

        let store = builder.build().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to build S3 client: {e}"))
        })?;

        Ok(Box::new(store))
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

#[cfg(test)]
mod tests {
    use super::*;

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
