//! Index build pipeline and staleness tracking.
//!
//! Manages per-index state tracking and background index build scheduling.
//! The [`IndexStateTracker`] tracks which SSTables have been indexed and which
//! are pending, while the [`IndexBuildScheduler`] processes build jobs on
//! background worker threads following the same channel-based pattern as
//! `CompactionExecutor`.

pub mod remote_backend;
pub mod scheduler;
pub mod sidecar;
pub mod tracker;
pub mod virtual_table;

#[cfg(test)]
mod quantized_compaction_tests {
    use crate::index::artifact_manifest::{
        ArtifactManifestEntry, ArtifactPublishCandidate, QuantizedArtifactCacheKey,
    };

    #[test]
    fn quantized_compaction_cache_key_includes_generation_build_and_checksum() {
        let original = QuantizedArtifactCacheKey::new("ks.tbl", "idx_embedding", 41, 7, "old");
        let replacement_same_generation =
            QuantizedArtifactCacheKey::new("ks.tbl", "idx_embedding", 41, 8, "old");
        let replacement_same_build =
            QuantizedArtifactCacheKey::new("ks.tbl", "idx_embedding", 41, 7, "new");

        assert_ne!(original, replacement_same_generation);
        assert_ne!(original, replacement_same_build);
    }

    #[test]
    fn quantized_compaction_publish_requires_validated_upload_metadata() {
        let candidate = ArtifactPublishCandidate::new_qvec(
            "ks.tbl",
            "idx_embedding",
            42,
            9,
            "prod/42/ks.tbl/gen-42/idx_embedding/q4.qvec",
            4096,
            "abc123",
            12,
        );

        let err = candidate
            .publish_after_upload(2048, "abc123")
            .expect_err("partial upload must not produce a manifest entry");
        assert!(err.contains("size mismatch"));

        let entry = candidate
            .publish_after_upload(4096, "abc123")
            .expect("validated upload publishes manifest metadata");
        assert_eq!(entry.artifact_kind, "hvq_qvec");
        assert_eq!(
            entry.object_key,
            "prod/42/ks.tbl/gen-42/idx_embedding/q4.qvec"
        );
        assert_eq!(entry.size_bytes, 4096);
    }

    #[test]
    fn quantized_compaction_rejects_non_qvec_manifest_metadata() {
        let entry = ArtifactManifestEntry {
            artifact_kind: "hvq_qvec".into(),
            table_id: "ks.tbl".into(),
            index_name: "idx_embedding".into(),
            generation: 42,
            build_id: 9,
            object_key: "prod/42/ks.tbl/gen-42/idx_embedding/q4.tmp".into(),
            size_bytes: 4096,
            sha256_hex: "abc123".into(),
            page_count: 12,
        };

        let err = entry
            .validate_qvec()
            .expect_err("only finalized .qvec objects are publishable");
        assert!(err.contains(".qvec"));
    }
}

pub mod artifact_manifest;

pub use artifact_manifest::{
    ArtifactManifestEntry, ArtifactPublishCandidate, QuantizedArtifactCacheKey,
};
pub use remote_backend::{IndexBackendConfig, RemoteBackend, S3PathResolver};
pub use scheduler::{
    BuildPriority, ClusteringComponentRef, IndexBuildBackend, IndexBuildJob, IndexBuildResult,
    IndexBuildScheduler, LocalBackend,
};
pub use tracker::{IndexState, IndexStateTracker, IndexStatus};
