//! Bounded worker pool for index building.
//!
//! Workers download SSTable components from S3 to a temp directory,
//! run [`LocalBackend::build()`] on them, write the sidecar file back
//! to S3, and clean up the temp files.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use ferrosa_index::IndexType;
use ferrosa_storage::index::sidecar::SidecarWriter;
use ferrosa_storage::index::{
    BuildPriority, ClusteringComponentRef, IndexBuildBackend, IndexBuildJob, LocalBackend,
    PartitionKeyComponentRef,
};

/// Request sent to the worker pool from the HTTP handler.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BuildRequest {
    /// Engine-issued job id. Names the job's local scratch directory; the
    /// builder never lets the caller pick a path. Validated by
    /// [`BuildRequest::validate`].
    pub job_id: String,
    pub sstable_id: String,
    pub index_name: String,
    pub index_type: String,
    #[serde(default)]
    pub artifact_kind: Option<String>,
    #[serde(default)]
    pub direct_upload: bool,
    /// Informational only. The builder uses its own configured object store.
    #[serde(default)]
    pub s3_endpoint: String,
    /// Informational only. The builder uses its own configured object store.
    #[serde(default)]
    pub s3_bucket: String,
    pub table: (String, String),
    pub column_position: usize,
    /// Clustering-key component metadata for indexes on CLUSTERING columns.
    /// Omitted for regular/static-column indexes.
    #[serde(default)]
    pub clustering_source: Option<ClusteringComponentRef>,
    /// Partition-key component metadata for indexes on PARTITION-KEY columns.
    /// Omitted for every other index. Without this the remote builder would
    /// look for the value in a cell that does not exist and build an empty
    /// index while reporting success.
    #[serde(default)]
    pub partition_key_source: Option<PartitionKeyComponentRef>,
    pub priority: String,
    /// Partial-index predicate. `Some` only for a `filtered` index build: the
    /// fully-encoded [`ferrosa_index::FilterPredicate`] (value bytes already in
    /// storage encoding) the builder applies at build time so the remote sidecar
    /// contains exactly the matching rows — never an unfiltered sidecar. Omitted
    /// (deserializes to `None`) for every other index type.
    #[serde(default)]
    pub filter_predicate: Option<ferrosa_index::FilterPredicate>,
}

/// Response returned from a completed build.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar_s3_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries_built: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_manifest_entry: Option<ferrosa_storage::index::ArtifactManifestEntry>,
}

impl BuildResponse {
    pub fn completed_quantized(
        entry: ferrosa_storage::index::ArtifactManifestEntry,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            status: "completed".into(),
            error: None,
            sidecar_s3_path: None,
            entries_built: None,
            elapsed_ms: Some(elapsed_ms),
            artifact_manifest_entry: Some(entry),
        }
    }

    fn failed(error: impl Into<String>, elapsed_ms: Option<u64>) -> Self {
        Self {
            status: "failed".into(),
            error: Some(error.into()),
            sidecar_s3_path: None,
            entries_built: None,
            elapsed_ms,
            artifact_manifest_entry: None,
        }
    }
}

/// Longest accepted identifier (`job_id`, `sstable_id`, `index_name`, keyspace, table).
const MAX_COMPONENT_LEN: usize = 128;

/// Refuse any identifier that could escape its directory or key prefix.
///
/// Accepts only `[A-Za-z0-9._-]`, 1..=128 bytes, with no `..` substring and not
/// starting with `.`. This rules out path separators, dot segments, NUL, and
/// anything else that could steer an S3 key or local path (JB-T3).
pub fn validate_path_component(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_COMPONENT_LEN {
        return Err(format!(
            "invalid {field}: length must be 1..={MAX_COMPONENT_LEN}"
        ));
    }
    if value.starts_with('.') || value.contains("..") {
        return Err(format!("invalid {field}: dot segments are not allowed"));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "invalid {field}: only [A-Za-z0-9._-] characters are allowed"
        ));
    }
    Ok(())
}

/// Derive the S3 prefix of an SSTable from validated components and the
/// builder's configured base prefix, using the engine's layout
/// `{base}/{hex}/{keyspace.table}/{sstable_id}`. Never takes a caller prefix.
pub fn derive_s3_prefix(base: &str, table: &(String, String), sstable_id: &str) -> String {
    let hex = ferrosa_storage::upload::hex_prefix_for(sstable_id);
    let base = base.trim_matches('/');
    if base.is_empty() {
        format!("{hex}/{}.{}/{sstable_id}", table.0, table.1)
    } else {
        format!("{base}/{hex}/{}.{}/{sstable_id}", table.0, table.1)
    }
}

/// Bounded worker pool that processes index build jobs.
pub struct WorkerPool {
    object_store: Arc<dyn ObjectStore>,
    /// Configured base key prefix; the only prefix source (never the caller).
    s3_base_prefix: String,
    temp_dir: PathBuf,
    max_temp_bytes: u64,
    temp_bytes_used: AtomicU64,
    jobs_completed: AtomicUsize,
    jobs_failed: AtomicUsize,
    /// Semaphore limiting concurrent builds.
    semaphore: tokio::sync::Semaphore,
}

impl WorkerPool {
    /// Create a new worker pool.
    pub fn new(
        max_workers: usize,
        object_store: Arc<dyn ObjectStore>,
        max_temp_bytes: u64,
    ) -> Self {
        let temp_dir = std::env::temp_dir().join("ferrosa-index-builder");
        std::fs::create_dir_all(&temp_dir).expect("failed to create temp dir");

        Self {
            object_store,
            s3_base_prefix: String::new(),
            temp_dir,
            max_temp_bytes,
            temp_bytes_used: AtomicU64::new(0),
            jobs_completed: AtomicUsize::new(0),
            jobs_failed: AtomicUsize::new(0),
            semaphore: tokio::sync::Semaphore::new(max_workers),
        }
    }

    /// Set the configured base S3 key prefix (must match the engine's prefix).
    #[must_use]
    pub fn with_s3_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.s3_base_prefix = prefix.into();
        self
    }

    /// Number of active workers.
    pub fn active_workers(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Total completed jobs.
    pub fn jobs_completed(&self) -> usize {
        self.jobs_completed.load(Ordering::Relaxed)
    }

    /// Total failed jobs.
    pub fn jobs_failed(&self) -> usize {
        self.jobs_failed.load(Ordering::Relaxed)
    }

    /// Execute a build request. Acquires a semaphore permit, downloads
    /// SSTable components, builds the index, uploads the sidecar, and
    /// cleans up.
    pub async fn execute(&self, req: BuildRequest) -> BuildResponse {
        if let Err(e) = req.validate() {
            self.jobs_failed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, "rejected build request with invalid identifiers");
            return BuildResponse::failed(e, None);
        }
        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                return BuildResponse::failed("worker pool shut down", None);
            }
        };

        let start = Instant::now();

        if req.is_quantized_direct_upload() {
            return BuildResponse::failed(
                "quantized .qvec direct-upload is not implemented: builder must stream/upload final .qvec, validate object size and sha256, and return artifact_manifest_entry before engine can publish it",
                Some(start.elapsed().as_millis() as u64),
            );
        }

        match self.do_build(&req).await {
            Ok((s3_path, entries)) => {
                self.jobs_completed.fetch_add(1, Ordering::Relaxed);
                BuildResponse {
                    status: "completed".into(),
                    error: None,
                    sidecar_s3_path: Some(s3_path),
                    entries_built: Some(entries),
                    elapsed_ms: Some(start.elapsed().as_millis() as u64),
                    artifact_manifest_entry: None,
                }
            }
            Err(e) => {
                self.jobs_failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    sstable_id = %req.sstable_id,
                    index_name = %req.index_name,
                    error = %e,
                    "index build failed"
                );
                BuildResponse::failed(e, Some(start.elapsed().as_millis() as u64))
            }
        }
    }

    /// Internal: download, build, upload, cleanup.
    async fn do_build(&self, req: &BuildRequest) -> Result<(String, u64), String> {
        // Create a per-job temp directory.
        // Every path is derived from validated identifiers and configuration.
        let s3_prefix = derive_s3_prefix(&self.s3_base_prefix, &req.table, &req.sstable_id);
        let job_dir = self.temp_dir.join(&req.job_id);
        tokio::fs::create_dir_all(&job_dir)
            .await
            .map_err(|e| format!("create temp dir: {e}"))?;

        // Download SSTable components from S3.
        let components = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "CompressionInfo.db",
        ];

        let mut downloaded_bytes: u64 = 0;
        for component in &components {
            let s3_path = ObjectPath::from(format!("{s3_prefix}/{component}"));
            let local_path = job_dir.join(format!("{}-{component}", req.sstable_id));

            match self.object_store.get(&s3_path).await {
                Ok(result) => {
                    let data = result
                        .bytes()
                        .await
                        .map_err(|e| format!("read {component}: {e}"))?;
                    downloaded_bytes += data.len() as u64;

                    // Check temp disk budget.
                    let current = self
                        .temp_bytes_used
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                    if current + data.len() as u64 > self.max_temp_bytes {
                        self.temp_bytes_used
                            .fetch_sub(data.len() as u64 + downloaded_bytes, Ordering::Relaxed);
                        cleanup_dir(&job_dir).await;
                        return Err("temp disk budget exceeded".into());
                    }

                    tokio::fs::write(&local_path, &data)
                        .await
                        .map_err(|e| format!("write {component}: {e}"))?;
                }
                Err(object_store::Error::NotFound { .. }) => {
                    // CompressionInfo.db is optional.
                    if *component != "CompressionInfo.db" {
                        cleanup_dir(&job_dir).await;
                        self.temp_bytes_used
                            .fetch_sub(downloaded_bytes, Ordering::Relaxed);
                        return Err(format!("{component} not found at {s3_path}"));
                    }
                }
                Err(e) => {
                    cleanup_dir(&job_dir).await;
                    self.temp_bytes_used
                        .fetch_sub(downloaded_bytes, Ordering::Relaxed);
                    return Err(format!("download {component}: {e}"));
                }
            }
        }

        // Build the index using LocalBackend (blocking — run on a thread).
        let job = build_job(req)?;

        let job_dir_clone = job_dir.clone();
        let build_result = tokio::task::spawn_blocking(move || {
            let backend = LocalBackend::new(job_dir_clone);
            backend.build(&job)
        })
        .await
        .map_err(|e| format!("spawn_blocking: {e}"))?
        .map_err(|e| format!("build: {e}"))?;

        // Write sidecar file locally, then upload to S3.
        let mut total_entries: u64 = 0;
        let mut sidecar_s3_path = String::new();

        for (index_name, entries) in &build_result.sidecar_entries {
            validate_path_component("sidecar index name", index_name)?;
            total_entries += entries.len() as u64;
            let sidecar_filename = format!("{}-{index_name}.sidecar", req.sstable_id);
            let local_sidecar = job_dir.join(&sidecar_filename);

            SidecarWriter::write(&local_sidecar, entries)
                .map_err(|e| format!("write sidecar: {e}"))?;

            let sidecar_bytes = tokio::fs::read(&local_sidecar)
                .await
                .map_err(|e| format!("read sidecar: {e}"))?;

            let s3_sidecar_path = ObjectPath::from(format!("{s3_prefix}/{sidecar_filename}"));
            sidecar_s3_path = s3_sidecar_path.to_string();

            self.object_store
                .put(&s3_sidecar_path, Bytes::from(sidecar_bytes).into())
                .await
                .map_err(|e| format!("upload sidecar: {e}"))?;
        }

        // Cleanup temp files.
        self.temp_bytes_used
            .fetch_sub(downloaded_bytes, Ordering::Relaxed);
        cleanup_dir(&job_dir).await;

        Ok((sidecar_s3_path, total_entries))
    }
}

/// Best-effort removal of a job's scratch directory. A failure is logged, never
/// silent: leaked scratch space eventually exhausts the temp budget.
async fn cleanup_dir(dir: &std::path::Path) {
    if let Err(e) = tokio::fs::remove_dir_all(dir).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(dir = %dir.display(), error = %e, "failed to remove job scratch dir");
        }
    }
}

impl BuildRequest {
    /// Reject any request whose identifiers could steer a path or S3 key.
    pub fn validate(&self) -> Result<(), String> {
        validate_path_component("job_id", &self.job_id)?;
        validate_path_component("sstable_id", &self.sstable_id)?;
        validate_path_component("index_name", &self.index_name)?;
        validate_path_component("keyspace", &self.table.0)?;
        validate_path_component("table", &self.table.1)?;
        Ok(())
    }

    fn is_quantized_direct_upload(&self) -> bool {
        self.direct_upload
            && self
                .artifact_kind
                .as_deref()
                .is_some_and(|kind| kind == "hvq_qvec")
    }
}

/// Build an [`IndexBuildJob`] from a [`BuildRequest`], carrying the optional
/// clustering-key source and partial predicate so remote builds match the local
/// scheduler semantics. This is the single place the wire request is mapped to
/// a job; extracted so protocol plumbing is unit-testable without spinning up
/// the HTTP path.
fn build_job(req: &BuildRequest) -> Result<IndexBuildJob, String> {
    let index_type = parse_index_type(&req.index_type)?;
    Ok(IndexBuildJob {
        sstable_id: req.sstable_id.clone(),
        index_name: req.index_name.clone(),
        index_type,
        table: (req.table.0.clone(), req.table.1.clone()),
        priority: parse_priority(&req.priority),
        enqueued_at: Instant::now(),
        column_position: req.column_position,
        clustering_source: req.clustering_source,
        partition_key_source: req.partition_key_source,
        filter_predicate: req.filter_predicate.clone(),
    })
}

fn parse_index_type(s: &str) -> Result<IndexType, String> {
    match s.to_lowercase().as_str() {
        "btree" => Ok(IndexType::BTree),
        "hash" => Ok(IndexType::Hash),
        "composite" => Ok(IndexType::Composite),
        "phonetic" => Ok(IndexType::Phonetic),
        "filtered" => Ok(IndexType::Filtered),
        "vector" => Ok(IndexType::Vector),
        "fulltext" => Ok(IndexType::FullText),
        "geo" => Ok(IndexType::Geo),
        other => Err(format!("unknown index type: {other}")),
    }
}

fn parse_priority(s: &str) -> BuildPriority {
    match s.to_lowercase().as_str() {
        "high" => BuildPriority::High,
        "initial" => BuildPriority::Initial,
        _ => BuildPriority::Normal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_index_types() {
        assert!(matches!(parse_index_type("btree"), Ok(IndexType::BTree)));
        assert!(matches!(parse_index_type("HASH"), Ok(IndexType::Hash)));
        assert!(matches!(
            parse_index_type("FullText"),
            Ok(IndexType::FullText)
        ));
        assert!(matches!(parse_index_type("GEO"), Ok(IndexType::Geo)));
        // Filtered must be recognized now that the remote builder carries the
        // partial predicate and filters at build time.
        assert!(matches!(
            parse_index_type("filtered"),
            Ok(IndexType::Filtered)
        ));
        assert!(parse_index_type("unknown").is_err());
    }

    /// A filtered build request deserializes the wire `filter_predicate` and,
    /// via `build_job`, threads it into the `IndexBuildJob` so `LocalBackend`
    /// filters rows at build time. Without this, the remote builder would write
    /// an UNFILTERED sidecar — a silent correctness bug.
    #[test]
    fn build_request_threads_filter_predicate_into_job() {
        use ferrosa_index::{FilterOp, FilterPredicate};

        let predicate = FilterPredicate::single(1, FilterOp::Gt, vec![0, 0, 0, 21]);
        // Construct the request the way the engine sends it (JSON), to prove the
        // wire field deserializes.
        let json = serde_json::json!({
            "job_id": "job-1",
            "sstable_id": "gen-7",
            "index_name": "name_adult_idx",
            "index_type": "filtered",
            "s3_endpoint": "memory://",
            "s3_bucket": "b",
            "table": ["ks", "tbl"],
            "column_position": 0,
            "priority": "normal",
            "filter_predicate": predicate,
        });
        let req: BuildRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.filter_predicate.as_ref(), Some(&predicate));

        let job = build_job(&req).unwrap();
        assert!(matches!(job.index_type, IndexType::Filtered));
        assert_eq!(job.filter_predicate.as_ref(), Some(&predicate));
        assert_eq!(job.column_position, 0);
    }

    /// A non-filtered request omits `filter_predicate`; the job carries `None`.
    #[test]
    fn build_request_without_predicate_yields_none() {
        let json = serde_json::json!({
            "job_id": "job-1",
            "sstable_id": "gen-1",
            "index_name": "email_idx",
            "index_type": "btree",
            "s3_endpoint": "memory://",
            "s3_bucket": "b",
            "table": ["ks", "tbl"],
            "column_position": 0,
            "priority": "normal",
        });
        let req: BuildRequest = serde_json::from_value(json).unwrap();
        assert!(req.filter_predicate.is_none());
        let job = build_job(&req).unwrap();
        assert!(job.filter_predicate.is_none());
    }

    #[test]
    fn build_request_threads_clustering_source_into_job() {
        let json = serde_json::json!({
            "job_id": "job-1",
            "sstable_id": "gen-9",
            "index_name": "ck_idx",
            "index_type": "btree",
            "s3_endpoint": "memory://",
            "s3_bucket": "b",
            "table": ["ks", "tbl"],
            "column_position": 1,
            "clustering_source": {
                "component": 1,
                "total": 3
            },
            "priority": "initial",
        });
        let req: BuildRequest = serde_json::from_value(json).unwrap();
        let job = build_job(&req).unwrap();

        assert_eq!(
            job.clustering_source,
            Some(ClusteringComponentRef {
                component: 1,
                total: 3
            })
        );
        assert_eq!(job.column_position, 1);
        assert!(matches!(job.priority, BuildPriority::Initial));
    }

    /// The partition-key source must survive the wire, for the same reason the
    /// clustering one must: a remote worker that receives the job without it
    /// looks for the value in a cell that does not exist, finds nothing, and
    /// returns a successfully-built EMPTY index. Nothing errors, and the index
    /// then answers every query with no rows.
    #[test]
    fn build_request_threads_partition_key_source_into_job() {
        let json = serde_json::json!({
            "job_id": "job-1",
            "sstable_id": "gen-9",
            "index_name": "idx_by_tenant",
            "index_type": "btree",
            "s3_endpoint": "memory://",
            "s3_bucket": "b",
            "table": ["ks", "tbl"],
            "column_position": 0,
            "partition_key_source": {
                "component": 0,
                "total": 2
            },
            "priority": "initial",
        });
        let req: BuildRequest = serde_json::from_value(json).unwrap();
        let job = build_job(&req).unwrap();

        assert_eq!(
            job.partition_key_source,
            Some(PartitionKeyComponentRef {
                component: 0,
                total: 2
            }),
            "a partition-key job that arrives without its source builds an empty \
             index and reports success"
        );
        assert!(
            job.clustering_source.is_none(),
            "a column cannot be in both halves of the primary key"
        );
    }

    #[test]
    fn parse_priorities() {
        assert!(matches!(parse_priority("high"), BuildPriority::High));
        assert!(matches!(parse_priority("normal"), BuildPriority::Normal));
        assert!(matches!(parse_priority("initial"), BuildPriority::Initial));
        assert!(matches!(parse_priority("whatever"), BuildPriority::Normal));
    }

    #[tokio::test]
    async fn quantized_remote_builder_fails_closed_without_direct_upload_support() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let pool = WorkerPool::new(1, store, 1024 * 1024);
        let response = pool
            .execute(BuildRequest {
                job_id: "job-1".into(),
                sstable_id: "gen-42".into(),
                index_name: "idx_embedding".into(),
                index_type: "vector".into(),
                artifact_kind: Some("hvq_qvec".into()),
                direct_upload: true,
                s3_endpoint: "memory://".into(),
                s3_bucket: "bucket".into(),
                table: ("ks".into(), "tbl".into()),
                column_position: 0,
                clustering_source: None,
                partition_key_source: None,
                priority: "normal".into(),
                filter_predicate: None,
            })
            .await;

        assert_eq!(response.status, "failed");
        assert!(response
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("quantized .qvec direct-upload is not implemented"));
        assert!(response.artifact_manifest_entry.is_none());
    }

    #[test]
    fn quantized_remote_builder_response_carries_validated_qvec_manifest() {
        let entry = ferrosa_storage::index::ArtifactManifestEntry {
            artifact_kind: "hvq_qvec".into(),
            table_id: "ks.tbl".into(),
            index_name: "idx_embedding".into(),
            generation: 42,
            build_id: 9,
            object_key: "prod/42/ks.tbl/gen-42/idx_embedding/q4.qvec".into(),
            size_bytes: 4096,
            sha256_hex: "abc123".into(),
            page_count: 12,
        };

        let response = BuildResponse::completed_quantized(entry.clone(), 37);
        assert_eq!(response.status, "completed");
        assert_eq!(response.artifact_manifest_entry, Some(entry));
        assert_eq!(response.sidecar_s3_path, None);
        assert_eq!(response.elapsed_ms, Some(37));
    }

    fn request(sstable_id: &str, index_name: &str) -> BuildRequest {
        BuildRequest {
            job_id: "job-1".into(),
            sstable_id: sstable_id.into(),
            index_name: index_name.into(),
            index_type: "btree".into(),
            artifact_kind: None,
            direct_upload: false,
            s3_endpoint: String::new(),
            s3_bucket: String::new(),
            table: ("ks".into(), "tbl".into()),
            column_position: 0,
            clustering_source: None,
            partition_key_source: None,
            priority: "normal".into(),
            filter_predicate: None,
        }
    }

    /// JB-T3: identifiers with separators or dot segments are refused, and the
    /// S3 prefix comes from configuration plus validated ids, never the caller.
    #[tokio::test]
    async fn index_builder_rejects_caller_supplied_paths() {
        for bad in [
            "../etc/passwd",
            "..",
            ".",
            "a/b",
            "a\\b",
            "/abs",
            "gen..1",
            "",
            "gen\0",
            ".hidden",
        ] {
            assert!(
                request(bad, "idx").validate().is_err(),
                "sstable_id {bad:?}"
            );
            assert!(
                request("gen-1", bad).validate().is_err(),
                "index_name {bad:?}"
            );
            let mut r = request("gen-1", "idx");
            r.job_id = bad.into();
            assert!(r.validate().is_err(), "job_id {bad:?}");
            let mut r = request("gen-1", "idx");
            r.table = ("ks".into(), bad.into());
            assert!(r.validate().is_err(), "table {bad:?}");
        }
        assert!(request("gen-42", "idx_email").validate().is_ok());

        // A caller-supplied s3_prefix is not even a field: it is ignored.
        let json = serde_json::json!({
            "job_id": "job-1", "sstable_id": "gen-1", "index_name": "i",
            "index_type": "btree", "s3_prefix": "victim-tenant/secrets",
            "table": ["ks", "tbl"], "column_position": 0, "priority": "normal",
        });
        let req: BuildRequest = serde_json::from_value(json).unwrap();
        let prefix = derive_s3_prefix("base", &req.table, &req.sstable_id);
        assert!(prefix.starts_with("base/"), "{prefix}");
        assert!(prefix.ends_with("/ks.tbl/gen-1"), "{prefix}");
        assert!(!prefix.contains("victim"));

        // A refused job fails without touching the store.
        let store = Arc::new(object_store::memory::InMemory::new());
        let pool = WorkerPool::new(1, store.clone(), 1024);
        let resp = pool.execute(request("../../x", "idx")).await;
        assert_eq!(resp.status, "failed");
        assert_eq!(pool.jobs_failed(), 1);
        assert!(store_is_empty(&store).await);
    }

    async fn store_is_empty(store: &object_store::memory::InMemory) -> bool {
        let l = store.list_with_delimiter(None).await.unwrap();
        l.objects.is_empty() && l.common_prefixes.is_empty()
    }
}
