//! Object store configuration from environment variables.

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

/// S3-compatible object store configuration.
///
/// All settings are read from `FERROSA_S3_*` environment variables,
/// following 12-factor app principles.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
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
}

impl ObjectStoreConfig {
    /// Reads configuration from `FERROSA_S3_*` environment variables.
    ///
    /// Required: `FERROSA_S3_ENDPOINT`, `FERROSA_S3_BUCKET`.
    /// Optional with defaults: region, allow_http, prefix, queue_depth.
    pub fn from_env() -> ferrosa_common::Result<Self> {
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

        let prefix = std::env::var("FERROSA_S3_PREFIX").unwrap_or_default();

        let upload_queue_depth = std::env::var("FERROSA_S3_UPLOAD_QUEUE_DEPTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16);

        Ok(Self {
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            allow_http,
            prefix,
            upload_queue_depth,
        })
    }

    /// Builds an `ObjectStore` instance from this configuration.
    pub fn build_object_store(&self) -> ferrosa_common::Result<Box<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http);

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
            endpoint: "http://localhost:9000".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: Some("minioadmin".into()),
            secret_access_key: Some("minioadmin".into()),
            allow_http: true,
            prefix: String::new(),
            upload_queue_depth: 16,
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
    }

    #[tokio::test]
    async fn validate_s3_bucket_succeeds_with_in_memory_store() {
        let store = object_store::memory::InMemory::new();
        let warnings = validate_s3_bucket(&store).await.unwrap();
        assert!(warnings.is_empty());
    }
}
