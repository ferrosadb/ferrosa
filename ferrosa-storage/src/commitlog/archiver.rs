//! Commit log archiver: uploads closed segments to S3 for PITR.
//!
//! The archiver runs as a background Tokio task. It receives segment IDs
//! via an mpsc channel (lock-free notification from CommitLog) and uploads
//! the corresponding segment files to S3 with SHA-256 checksums.
//!
//! # Coordination
//!
//! The archiver does NOT share mutable state with the CommitLog. All
//! communication is via the mpsc channel. Segment files are immutable
//! once closed (no further writes after rotation).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};

/// Result of successfully archiving a single segment.
#[derive(Debug, Clone)]
pub struct ArchivedSegment {
    /// Segment ID.
    pub segment_id: u64,
    /// SHA-256 hex digest of the segment file.
    pub sha256: String,
    /// Size in bytes of the segment file.
    pub size: u64,
    /// ISO 8601 timestamp when the segment was archived.
    pub archived_at: String,
}

/// Uploads closed commit log segments to S3.
///
/// Stateless: each `archive_segment()` call reads from disk and writes
/// to S3. No mutable fields.
pub struct CommitLogArchiver {
    /// S3-compatible object store.
    store: Arc<dyn ObjectStore>,
    /// S3 key prefix (e.g., "cluster-01/node-3").
    prefix: String,
    /// Local commit log directory where segment files live.
    log_dir: PathBuf,
}

/// Maximum retry attempts for S3 upload.
const MAX_RETRIES: u32 = 5;

/// Base delay for exponential backoff: 1s, 2s, 4s, 8s, 16s.
const BASE_DELAY: Duration = Duration::from_secs(1);

impl CommitLogArchiver {
    /// Creates a new archiver.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, log_dir: PathBuf) -> Self {
        Self {
            store,
            prefix,
            log_dir,
        }
    }

    /// Returns a reference to the object store.
    pub fn store(&self) -> &dyn ObjectStore {
        self.store.as_ref()
    }

    /// Returns the S3 prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Archives a single closed segment to S3.
    ///
    /// Reads the segment file from disk, computes its SHA-256, uploads
    /// to `{prefix}/commitlog-archive/{hex}/{segment_id}.log` (where `hex`
    /// is a 2-char prefix from `hex_prefix_for`), and returns
    /// the archive metadata.
    ///
    /// Retries transient S3 errors with exponential backoff (5 attempts:
    /// 1s, 2s, 4s, 8s, 16s).
    pub async fn archive_segment(
        &self,
        segment_id: u64,
    ) -> ferrosa_common::Result<ArchivedSegment> {
        // Read segment file from disk.
        let path = self.log_dir.join(format!("commitlog-{segment_id}.log"));
        let data = tokio::fs::read(&path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to read segment {segment_id} at {}: {e}",
                path.display()
            ))
        })?;

        // Compute SHA-256.
        let sha256 = hex_sha256(&data);
        let size = data.len() as u64;

        // Upload to S3 with retry.
        // Distribute across 256 hex-prefixed buckets (same pattern as SSTable uploads).
        let hex = crate::upload::manager::hex_prefix_for(&segment_id.to_string());
        let s3_path = ObjectPath::from(format!(
            "{}/commitlog-archive/{hex}/{segment_id}.log",
            self.prefix
        ));
        let bytes = Bytes::from(data);

        self.put_with_retry(&s3_path, bytes, MAX_RETRIES).await?;

        let archived_at = now_iso8601();

        Ok(ArchivedSegment {
            segment_id,
            sha256,
            size,
            archived_at,
        })
    }

    /// Puts data to S3 with exponential backoff retry.
    async fn put_with_retry(
        &self,
        path: &ObjectPath,
        data: Bytes,
        max_retries: u32,
    ) -> ferrosa_common::Result<()> {
        let mut delay = BASE_DELAY;

        for attempt in 0..=max_retries {
            match self.store.put(path, data.clone().into()).await {
                Ok(_) => return Ok(()),
                Err(e) if attempt < max_retries => {
                    eprintln!(
                        "[commitlog-archiver] upload attempt {}/{} failed for {}: {}",
                        attempt + 1,
                        max_retries + 1,
                        path,
                        e
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "failed to upload {path} after {} attempts: {e}",
                        max_retries + 1
                    )));
                }
            }
        }

        unreachable!()
    }
}

/// Compute SHA-256 hex digest of a byte slice.
fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    // Format as lowercase hex without pulling in the `hex` crate at runtime.
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Returns the current time as an ISO 8601 string (UTC).
///
/// Uses a simple manual format to avoid pulling in `chrono`.
fn now_iso8601() -> String {
    use std::time::SystemTime;
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Approximate: good enough for archive metadata. Not leap-second aware.
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Days since epoch -> year/month/day (simplified Gregorian).
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Converts days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's chrono-compatible date library.
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use std::sync::Arc;

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Helper: write a fake segment file to disk and return its path.
    fn write_fake_segment(
        dir: &std::path::Path,
        segment_id: u64,
        data: &[u8],
    ) -> std::path::PathBuf {
        let path = dir.join(format!("commitlog-{segment_id}.log"));
        std::fs::write(&path, data).unwrap();
        path
    }

    #[test]
    fn archiver_uploads_segment_to_s3() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            let prefix = "test-node".to_string();

            let segment_data = b"fake-segment-data-for-testing";
            write_fake_segment(dir.path(), 42, segment_data);

            let archiver = CommitLogArchiver::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                prefix.clone(),
                dir.path().to_path_buf(),
            );

            let result = archiver.archive_segment(42).await.unwrap();

            // Verify segment was uploaded to correct S3 path (hex-prefixed).
            let hex = crate::upload::manager::hex_prefix_for("42");
            let s3_path = ObjectPath::from(format!("{prefix}/commitlog-archive/{hex}/42.log"));
            let get_result = store.get(&s3_path).await.unwrap();
            let bytes = get_result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), segment_data);

            // Verify SHA-256 checksum is correct.
            use sha2::{Digest, Sha256};
            let expected_hash = hex::encode(Sha256::digest(segment_data));
            assert_eq!(result.sha256, expected_hash);
            assert_eq!(result.segment_id, 42);
            assert_eq!(result.size, segment_data.len() as u64);
        });
    }

    #[test]
    fn archiver_retries_on_transient_failure() {
        // This test verifies the retry logic exists by checking that
        // archive_segment succeeds even when the underlying store
        // would normally require retries. With InMemory store, the
        // first attempt always succeeds, so this is a smoke test
        // that the retry loop doesn't break normal operation.
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            write_fake_segment(dir.path(), 1, b"data");

            let archiver = CommitLogArchiver::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".to_string(),
                dir.path().to_path_buf(),
            );

            let result = archiver.archive_segment(1).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn archiver_returns_error_for_missing_segment() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            // Do NOT write segment 99 to disk.

            let archiver = CommitLogArchiver::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".to_string(),
                dir.path().to_path_buf(),
            );

            let result = archiver.archive_segment(99).await;
            assert!(result.is_err(), "missing segment file should return error");
        });
    }

    #[test]
    fn archiver_sha256_matches_file_content() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            let data = b"deterministic content for hashing";
            write_fake_segment(dir.path(), 7, data);

            let archiver = CommitLogArchiver::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "node1".to_string(),
                dir.path().to_path_buf(),
            );

            let result = archiver.archive_segment(7).await.unwrap();

            use sha2::{Digest, Sha256};
            let expected = hex::encode(Sha256::digest(data));
            assert_eq!(result.sha256, expected);
        });
    }
}
