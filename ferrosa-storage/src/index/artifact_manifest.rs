//! Index artifact manifest metadata for object-backed quantized vector artifacts.
//!
//! Module: Validate `.qvec` artifact metadata before compaction or remote builders publish it.
//! Correctness: Correct when cache keys include generation/build/checksum identity and manifests are emitted only after size/checksum-validated uploads.
//! Last revised: 2026-05-29
//! Last changed: Added HVQ `.qvec` publish and cache-key seams for compaction replacement safety.

/// Manifest entry for a storage-visible index artifact.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactManifestEntry {
    /// Artifact family. HVQ uses `hvq_qvec`.
    pub artifact_kind: String,
    /// Fully-qualified table id, normally `keyspace.table`.
    pub table_id: String,
    /// Logical index name owning this artifact.
    pub index_name: String,
    /// SSTable/storage generation this artifact indexes.
    pub generation: u64,
    /// Monotonic build id within the generation.
    pub build_id: u64,
    /// Final object-store key. Temporary/staging keys are not publishable.
    pub object_key: String,
    /// Validated object size.
    pub size_bytes: u64,
    /// Validated SHA-256 hex digest.
    pub sha256_hex: String,
    /// Number of page-table entries in the `.qvec` container.
    pub page_count: u32,
}

impl ArtifactManifestEntry {
    /// Validate that the entry is safe to expose to readers as an HVQ `.qvec` artifact.
    pub fn validate_qvec(&self) -> Result<(), String> {
        if self.artifact_kind != "hvq_qvec" {
            return Err(format!(
                "unsupported artifact kind for .qvec manifest: {}",
                self.artifact_kind
            ));
        }
        if !self.object_key.ends_with(".qvec") {
            return Err(format!(
                "quantized artifact object key must end with .qvec: {}",
                self.object_key
            ));
        }
        if self.size_bytes == 0 {
            return Err("quantized artifact size must be non-zero".into());
        }
        if self.sha256_hex.is_empty() {
            return Err("quantized artifact sha256 must be present".into());
        }
        if self.page_count == 0 {
            return Err("quantized artifact page_count must be non-zero".into());
        }
        Ok(())
    }

    /// Reader cache identity. Includes the fields that distinguish replacement builds.
    pub fn cache_key(&self) -> Result<QuantizedArtifactCacheKey, String> {
        self.validate_qvec()?;
        Ok(QuantizedArtifactCacheKey::new(
            &self.table_id,
            &self.index_name,
            self.generation,
            self.build_id,
            &self.sha256_hex,
        ))
    }
}

/// Pre-publication metadata for an artifact the writer has staged/uploaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPublishCandidate {
    entry: ArtifactManifestEntry,
}

impl ArtifactPublishCandidate {
    /// Construct a candidate for a final `.qvec` object.
    #[allow(clippy::too_many_arguments)]
    pub fn new_qvec(
        table_id: impl Into<String>,
        index_name: impl Into<String>,
        generation: u64,
        build_id: u64,
        object_key: impl Into<String>,
        size_bytes: u64,
        sha256_hex: impl Into<String>,
        page_count: u32,
    ) -> Self {
        Self {
            entry: ArtifactManifestEntry {
                artifact_kind: "hvq_qvec".into(),
                table_id: table_id.into(),
                index_name: index_name.into(),
                generation,
                build_id,
                object_key: object_key.into(),
                size_bytes,
                sha256_hex: sha256_hex.into(),
                page_count,
            },
        }
    }

    /// Publish only after object-store upload metadata matches the candidate.
    pub fn publish_after_upload(
        &self,
        uploaded_size_bytes: u64,
        uploaded_sha256_hex: &str,
    ) -> Result<ArtifactManifestEntry, String> {
        self.entry.validate_qvec()?;
        if uploaded_size_bytes != self.entry.size_bytes {
            return Err(format!(
                "quantized artifact size mismatch: expected {}, uploaded {}",
                self.entry.size_bytes, uploaded_size_bytes
            ));
        }
        if uploaded_sha256_hex != self.entry.sha256_hex {
            return Err("quantized artifact sha256 mismatch".into());
        }
        Ok(self.entry.clone())
    }
}

/// Cache identity for `.qvec` pages.
///
/// This deliberately includes generation, build id, and checksum so compaction
/// replacement artifacts cannot alias stale cached pages from an older build.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuantizedArtifactCacheKey {
    pub table_id: String,
    pub index_name: String,
    pub generation: u64,
    pub build_id: u64,
    pub sha256_hex: String,
}

impl QuantizedArtifactCacheKey {
    pub fn new(
        table_id: impl Into<String>,
        index_name: impl Into<String>,
        generation: u64,
        build_id: u64,
        sha256_hex: impl Into<String>,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            index_name: index_name.into(),
            generation,
            build_id,
            sha256_hex: sha256_hex.into(),
        }
    }
}
