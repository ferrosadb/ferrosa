//! Quantized IVFFlat builder for page-addressable `.qvec` artifacts.
//!
//! Module: Build deterministic IVFFlat centroids and tiered list pages from full `f32` vectors.
//! Correctness: Correct when a stable corpus emits stable manifest rows, list headers, row refs, quantized tiers, and optional F32 rerank pages without storage/CQL dependencies.
//! Last revised: 2026-05-29
//! Last changed: Implemented deterministic centroid assignment and tiered list-page emission for the HVQ builder.

use crate::{IndexError, IndexResult};

/// Quantization tiers emitted for IVFFlat list pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuantizedTier {
    /// Unsigned 8-bit scalar quantized vectors.
    Q8,
    /// Unsigned 4-bit scalar quantized vectors.
    Q4,
}

/// Stable row identity carried inside quantized vector pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuantizedRowRef {
    /// SSTable generation for persisted rows; `None` for not-yet-persisted inputs.
    pub generation: Option<u64>,
    /// Byte offset of the row within its source.
    pub offset: u64,
}

impl QuantizedRowRef {
    /// Construct a row ref for a persisted SSTable row.
    pub fn sstable(generation: u64, offset: u64) -> Self {
        Self {
            generation: Some(generation),
            offset,
        }
    }
}

/// Builder configuration for deterministic quantized IVFFlat artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantizedIvfBuilderConfig {
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Number of IVF lists/centroids to build.
    pub list_count: usize,
    /// Maximum rows packed into one logical page per tier.
    pub page_row_limit: usize,
    /// Quantized tiers to emit for every non-empty list.
    pub tiers: Vec<QuantizedTier>,
    /// Whether to emit exact `f32` rerank pages.
    pub include_f32_rerank: bool,
}

/// Kind of logical page emitted by the IVFFlat builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuantizedIvfPageKind {
    /// Centroid table page.
    Centroids,
    /// Per-list header page.
    ListHeader,
    /// Per-list row-reference page.
    RowRefs,
    /// Per-list Q8 codes page.
    Q8,
    /// Per-list Q4 codes page.
    Q4,
    /// Per-list exact f32 vectors for survivor reranking.
    F32Rerank,
}

/// Manifest entry for one logical page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantizedIvfPageManifest {
    /// Stable page id in artifact order.
    pub page_id: u32,
    /// IVF list id for list-local pages; `None` for global pages.
    pub list_id: Option<u32>,
    /// Page kind/tier.
    pub kind: QuantizedIvfPageKind,
    /// Rows represented by this page.
    pub row_count: u32,
    /// Byte length of the encoded page payload.
    pub len: u32,
    /// CRC32 checksum over the page payload.
    pub crc32: u32,
}

/// Manifest for a built quantized IVFFlat artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedIvfManifest {
    /// Format version for this in-memory artifact contract.
    pub format_version: u16,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Number of rows in the corpus.
    pub row_count: u32,
    /// Centroids emitted by the builder.
    pub centroids: Vec<Vec<f32>>,
    /// Requested quantized tiers.
    pub tiers: Vec<QuantizedTier>,
    /// Whether F32 rerank pages were emitted.
    pub include_f32_rerank: bool,
    /// Stable page table.
    pub pages: Vec<QuantizedIvfPageManifest>,
}

/// Encoded page payload plus manifest metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantizedIvfPage {
    /// Page-table metadata.
    pub manifest: QuantizedIvfPageManifest,
    /// Deterministic binary payload.
    pub bytes: Vec<u8>,
}

/// Built quantized IVFFlat artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedIvfArtifact {
    /// Artifact manifest.
    pub manifest: QuantizedIvfManifest,
    /// Encoded pages in manifest order.
    pub pages: Vec<QuantizedIvfPage>,
}

/// Deterministic builder for quantized IVFFlat list pages.
#[derive(Debug, Clone)]
pub struct QuantizedIvfBuilder {
    config: QuantizedIvfBuilderConfig,
    rows: Vec<(QuantizedRowRef, Vec<f32>)>,
}

#[derive(Debug, Clone)]
struct AssignedVector {
    row_ref: QuantizedRowRef,
    vector: Vec<f32>,
}

impl QuantizedIvfBuilder {
    /// Create an empty builder.
    pub fn new(config: QuantizedIvfBuilderConfig) -> IndexResult<Self> {
        if config.dimensions == 0 {
            return Err(IndexError::Format(
                "dimensions must be greater than zero".to_string(),
            ));
        }
        if config.list_count == 0 {
            return Err(IndexError::Format(
                "list_count must be greater than zero".to_string(),
            ));
        }
        if config.page_row_limit == 0 {
            return Err(IndexError::Format(
                "page_row_limit must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            config,
            rows: Vec::new(),
        })
    }

    /// Add one full vector and its stable row reference.
    pub fn add_vector(&mut self, row_ref: QuantizedRowRef, vector: &[f32]) -> IndexResult<()> {
        if vector.len() != self.config.dimensions {
            return Err(IndexError::DimensionMismatch {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }
        self.rows.push((row_ref, vector.to_vec()));
        Ok(())
    }

    /// Finish the artifact.
    pub fn finish(self) -> IndexResult<QuantizedIvfArtifact> {
        if self.rows.is_empty() {
            return Err(IndexError::Format(
                "quantized IVFFlat builder requires at least one vector".to_string(),
            ));
        }
        if self.rows.len() > u32::MAX as usize {
            return Err(IndexError::Format("row count exceeds u32".to_string()));
        }

        let mut rows = self.rows;
        rows.sort_by_key(|(row_ref, _)| *row_ref);

        let centroids = build_centroids(&rows, self.config.list_count, self.config.dimensions);
        let lists = assign_lists(&rows, &centroids);
        let mut pages = Vec::new();

        push_page(
            &mut pages,
            None,
            QuantizedIvfPageKind::Centroids,
            centroids.len(),
            encode_centroids(&centroids),
        )?;

        for (list_id, list_rows) in lists.iter().enumerate() {
            if list_rows.is_empty() {
                continue;
            }
            push_page(
                &mut pages,
                Some(list_id),
                QuantizedIvfPageKind::ListHeader,
                list_rows.len(),
                encode_list_header(list_id, list_rows.len()),
            )?;
            push_page(
                &mut pages,
                Some(list_id),
                QuantizedIvfPageKind::RowRefs,
                list_rows.len(),
                encode_row_refs(list_rows),
            )?;
            for tier in &self.config.tiers {
                let (kind, bytes) = match tier {
                    QuantizedTier::Q8 => (QuantizedIvfPageKind::Q8, encode_q8(list_rows)),
                    QuantizedTier::Q4 => (QuantizedIvfPageKind::Q4, encode_q4(list_rows)),
                };
                push_page(&mut pages, Some(list_id), kind, list_rows.len(), bytes)?;
            }
            if self.config.include_f32_rerank {
                push_page(
                    &mut pages,
                    Some(list_id),
                    QuantizedIvfPageKind::F32Rerank,
                    list_rows.len(),
                    encode_f32(list_rows),
                )?;
            }
        }

        let manifest_pages = pages.iter().map(|page| page.manifest.clone()).collect();
        Ok(QuantizedIvfArtifact {
            manifest: QuantizedIvfManifest {
                format_version: 1,
                dimensions: self.config.dimensions,
                row_count: rows.len() as u32,
                centroids,
                tiers: self.config.tiers,
                include_f32_rerank: self.config.include_f32_rerank,
                pages: manifest_pages,
            },
            pages,
        })
    }
}

fn build_centroids(
    rows: &[(QuantizedRowRef, Vec<f32>)],
    list_count: usize,
    dimensions: usize,
) -> Vec<Vec<f32>> {
    let centroid_count = list_count.min(rows.len());
    let mut sorted_vectors: Vec<_> = rows.iter().map(|(_, vector)| vector.clone()).collect();
    sorted_vectors.sort_by(|left, right| lexicographic_f32_cmp(left, right));

    let mut centroids = (0..centroid_count)
        .map(|idx| {
            let source_idx = if centroid_count == 1 {
                0
            } else {
                idx * (sorted_vectors.len() - 1) / (centroid_count - 1)
            };
            sorted_vectors[source_idx].clone()
        })
        .collect::<Vec<_>>();

    for _ in 0..8 {
        let mut sums = vec![vec![0.0; dimensions]; centroid_count];
        let mut counts = vec![0_usize; centroid_count];
        for (_, vector) in rows {
            let list_id = nearest_centroid(vector, &centroids);
            counts[list_id] += 1;
            for (dim, value) in vector.iter().enumerate() {
                sums[list_id][dim] += value;
            }
        }
        for (list_id, centroid) in centroids.iter_mut().enumerate() {
            if counts[list_id] == 0 {
                continue;
            }
            for (dim, value) in centroid.iter_mut().enumerate() {
                *value = sums[list_id][dim] / counts[list_id] as f32;
            }
        }
    }

    centroids
}

fn assign_lists(
    rows: &[(QuantizedRowRef, Vec<f32>)],
    centroids: &[Vec<f32>],
) -> Vec<Vec<AssignedVector>> {
    let mut lists = vec![Vec::new(); centroids.len()];
    for (row_ref, vector) in rows {
        let list_id = nearest_centroid(vector, centroids);
        lists[list_id].push(AssignedVector {
            row_ref: *row_ref,
            vector: vector.clone(),
        });
    }
    for list in &mut lists {
        list.sort_by_key(|row| row.row_ref);
    }
    lists
}

fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    centroids
        .iter()
        .enumerate()
        .min_by(|(left_id, left), (right_id, right)| {
            squared_l2(vector, left)
                .total_cmp(&squared_l2(vector, right))
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(idx, _)| idx)
        .expect("centroid list is non-empty")
}

fn squared_l2(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn lexicographic_f32_cmp(left: &[f32], right: &[f32]) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = left.total_cmp(right);
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn push_page(
    pages: &mut Vec<QuantizedIvfPage>,
    list_id: Option<usize>,
    kind: QuantizedIvfPageKind,
    row_count: usize,
    bytes: Vec<u8>,
) -> IndexResult<()> {
    let page_id = u32::try_from(pages.len())
        .map_err(|_| IndexError::Format("page count exceeds u32".to_string()))?;
    let list_id = list_id
        .map(|value| {
            u32::try_from(value).map_err(|_| IndexError::Format("list id exceeds u32".to_string()))
        })
        .transpose()?;
    let row_count = u32::try_from(row_count)
        .map_err(|_| IndexError::Format("page row count exceeds u32".to_string()))?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| IndexError::Format("page length exceeds u32".to_string()))?;
    let manifest = QuantizedIvfPageManifest {
        page_id,
        list_id,
        kind,
        row_count,
        len,
        crc32: crc32fast::hash(&bytes),
    };
    pages.push(QuantizedIvfPage { manifest, bytes });
    Ok(())
}

fn encode_centroids(centroids: &[Vec<f32>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for centroid in centroids {
        for value in centroid {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    bytes
}

fn encode_list_header(list_id: usize, row_count: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(list_id as u32).to_le_bytes());
    bytes.extend_from_slice(&(row_count as u32).to_le_bytes());
    bytes
}

fn encode_row_refs(rows: &[AssignedVector]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for row in rows {
        bytes.extend_from_slice(&row.row_ref.generation.unwrap_or(u64::MAX).to_le_bytes());
        bytes.extend_from_slice(&row.row_ref.offset.to_le_bytes());
    }
    bytes
}

fn encode_q8(rows: &[AssignedVector]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let bounds = per_dimension_bounds(rows);
    for row in rows {
        for (dim, value) in row.vector.iter().enumerate() {
            bytes.push(quantize_to_bucket(*value, bounds[dim], 255) as u8);
        }
    }
    bytes
}

fn encode_q4(rows: &[AssignedVector]) -> Vec<u8> {
    let bounds = per_dimension_bounds(rows);
    let mut nibbles = Vec::new();
    for row in rows {
        for (dim, value) in row.vector.iter().enumerate() {
            nibbles.push(quantize_to_bucket(*value, bounds[dim], 15) as u8);
        }
    }

    let mut bytes = Vec::new();
    for chunk in nibbles.chunks(2) {
        let low = chunk[0] & 0x0f;
        let high = chunk.get(1).copied().unwrap_or(0) & 0x0f;
        bytes.push(low | (high << 4));
    }
    bytes
}

fn encode_f32(rows: &[AssignedVector]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for row in rows {
        for value in &row.vector {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    bytes
}

fn per_dimension_bounds(rows: &[AssignedVector]) -> Vec<(f32, f32)> {
    let dimensions = rows
        .first()
        .map(|row| row.vector.len())
        .expect("non-empty list pages only");
    let mut bounds = vec![(f32::INFINITY, f32::NEG_INFINITY); dimensions];
    for row in rows {
        for (dim, value) in row.vector.iter().enumerate() {
            bounds[dim].0 = bounds[dim].0.min(*value);
            bounds[dim].1 = bounds[dim].1.max(*value);
        }
    }
    bounds
}

fn quantize_to_bucket(value: f32, (min, max): (f32, f32), max_bucket: u32) -> u32 {
    if min == max {
        return 0;
    }
    (((value - min) / (max - min)) * max_bucket as f32).round() as u32
}

#[cfg(test)]
mod quantized_ivf_builder_tests {
    use super::*;

    fn deterministic_builder() -> QuantizedIvfBuilder {
        QuantizedIvfBuilder::new(QuantizedIvfBuilderConfig {
            dimensions: 2,
            list_count: 2,
            page_row_limit: 16,
            tiers: vec![QuantizedTier::Q8, QuantizedTier::Q4],
            include_f32_rerank: true,
        })
        .expect("valid builder config")
    }

    #[test]
    fn quantized_ivf_builder_emits_stable_manifest_page_table_and_tier_pages() {
        let mut builder = deterministic_builder();
        for (row_ref, vector) in [
            (QuantizedRowRef::sstable(7, 0), vec![0.0, 0.0]),
            (QuantizedRowRef::sstable(7, 16), vec![0.0, 1.0]),
            (QuantizedRowRef::sstable(8, 0), vec![10.0, 10.0]),
            (QuantizedRowRef::sstable(8, 16), vec![10.0, 11.0]),
        ] {
            builder
                .add_vector(row_ref, &vector)
                .expect("deterministic corpus vectors match config dimensions");
        }

        let artifact = builder
            .finish()
            .expect("deterministic corpus should build quantized IVFFlat artifact");

        assert_eq!(artifact.manifest.format_version, 1);
        assert_eq!(artifact.manifest.dimensions, 2);
        assert_eq!(artifact.manifest.row_count, 4);
        assert_eq!(
            artifact.manifest.tiers,
            vec![QuantizedTier::Q8, QuantizedTier::Q4]
        );
        assert!(artifact.manifest.include_f32_rerank);
        assert_eq!(
            artifact.manifest.centroids,
            vec![vec![0.0, 0.5], vec![10.0, 10.5]]
        );
        assert_eq!(artifact.manifest.pages.len(), artifact.pages.len());

        let page_kinds: Vec<_> = artifact
            .manifest
            .pages
            .iter()
            .map(|page| (page.page_id, page.list_id, page.kind, page.row_count))
            .collect();
        assert_eq!(
            page_kinds,
            vec![
                (0, None, QuantizedIvfPageKind::Centroids, 2),
                (1, Some(0), QuantizedIvfPageKind::ListHeader, 2),
                (2, Some(0), QuantizedIvfPageKind::RowRefs, 2),
                (3, Some(0), QuantizedIvfPageKind::Q8, 2),
                (4, Some(0), QuantizedIvfPageKind::Q4, 2),
                (5, Some(0), QuantizedIvfPageKind::F32Rerank, 2),
                (6, Some(1), QuantizedIvfPageKind::ListHeader, 2),
                (7, Some(1), QuantizedIvfPageKind::RowRefs, 2),
                (8, Some(1), QuantizedIvfPageKind::Q8, 2),
                (9, Some(1), QuantizedIvfPageKind::Q4, 2),
                (10, Some(1), QuantizedIvfPageKind::F32Rerank, 2),
            ]
        );

        assert_eq!(artifact.pages[1].bytes, vec![0, 0, 0, 0, 2, 0, 0, 0]);
        assert_eq!(artifact.pages[2].bytes.len(), 32);
        assert_eq!(artifact.pages[3].bytes, vec![0, 0, 0, 255]);
        assert_eq!(artifact.pages[4].bytes, vec![0x00, 0xf0]);
        assert_eq!(artifact.pages[5].bytes.len(), 16);
        assert!(artifact
            .manifest
            .pages
            .iter()
            .all(|entry| entry.len > 0 && entry.crc32 != 0));
    }
}
