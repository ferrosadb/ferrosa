//! Module: Quantized IVFFlat staged artifact reader.
//! Correctness: Correct when queries route through centroid pages under a bounded page-read budget, deserialize artifact pages fail loud, and exact survivor rerank matches f32 scoring.
//! Last revised: 2026-05-29
//! Last changed: Added staged reader implementation with page-budget enforcement and exact survivor rerank.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::vector::{distance, RowPosition};
use crate::{DistanceMetric, IndexError};

const MANIFEST_FILE: &str = "quantized_ivf_manifest.json";

/// Build-time shape for a quantized IVFFlat artifact.
#[derive(Debug, Clone, Copy)]
pub struct QuantizedIvfConfig {
    /// Number of centroid lists to route through.
    pub lists: usize,
    /// Exact metric used both for routing and survivor rerank.
    pub metric: DistanceMetric,
    /// Maximum entries per staged page.
    pub page_size: usize,
}

/// Query-time controls for a staged quantized IVFFlat read.
#[derive(Debug, Clone, Copy)]
pub struct QuantizedIvfSearchOptions {
    /// Number of hits to return after exact rerank.
    pub k: usize,
    /// Number of nearest centroid lists to probe.
    pub probes: usize,
    /// Hard cap on staged page reads. The reader errors before exceeding it.
    pub max_page_reads: usize,
}

/// One reranked hit from a quantized IVFFlat search.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedIvfHit {
    pub position: RowPosition,
    pub score: f32,
}

/// Search output including the budget accounting the planner depends on.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedIvfSearchResult {
    pub hits: Vec<QuantizedIvfHit>,
    pub page_reads: usize,
    pub max_page_reads: usize,
}

/// Durable manifest for the staged IVFFlat artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizedIvfManifest {
    pub dimensions: usize,
    pub metric: DistanceMetric,
    pub centroids: Vec<Vec<f32>>,
    pub lists: Vec<QuantizedIvfListManifest>,
}

/// Per-list manifest data. Pages are small independently readable units.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizedIvfListManifest {
    pub centroid_id: usize,
    pub pages: Vec<QuantizedIvfPageRef>,
}

/// A logical page reference relative to the artifact directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizedIvfPageRef {
    pub object_key: String,
    pub entry_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuantizedIvfPage {
    centroid_id: usize,
    entries: Vec<QuantizedIvfPageEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuantizedIvfPageEntry {
    position: RowPosition,
    /// Coarse tier used for cheap pruning in later packet integration. It is
    /// persisted now so page format forces staged quantized reads, but final
    /// result ordering never trusts it.
    q8: Vec<i8>,
    /// Full precision vector retained for exact survivor rerank.
    vector: Vec<f32>,
}

/// Accumulates vectors and writes a deterministic staged IVFFlat artifact.
pub struct QuantizedIvfBuilder {
    dir: PathBuf,
    config: QuantizedIvfConfig,
    vectors: Vec<(RowPosition, Vec<f32>)>,
}

impl QuantizedIvfBuilder {
    pub fn new(dir: &Path, config: QuantizedIvfConfig) -> Self {
        Self {
            dir: dir.to_path_buf(),
            config,
            vectors: Vec::new(),
        }
    }

    pub fn add_vector(&mut self, position: RowPosition, vector: &[f32]) -> Result<(), IndexError> {
        if let Some((_, first)) = self.vectors.first() {
            if vector.len() != first.len() {
                return Err(IndexError::DimensionMismatch {
                    expected: first.len(),
                    got: vector.len(),
                });
            }
        }
        self.vectors.push((position, vector.to_vec()));
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), IndexError> {
        if self.config.lists == 0 {
            return Err(IndexError::Format(
                "quantized IVFFlat requires at least one list".to_string(),
            ));
        }
        if self.config.page_size == 0 {
            return Err(IndexError::Format(
                "quantized IVFFlat requires page_size > 0".to_string(),
            ));
        }

        std::fs::create_dir_all(&self.dir)?;
        let dimensions = self.vectors.first().map_or(0, |(_, vector)| vector.len());
        let (centroids, assignments) =
            deterministic_centroids(&self.vectors, self.config.lists, self.config.metric);
        let mut grouped: Vec<Vec<QuantizedIvfPageEntry>> = vec![Vec::new(); centroids.len()];
        for ((position, vector), centroid_id) in self.vectors.iter().zip(assignments) {
            grouped[centroid_id].push(QuantizedIvfPageEntry {
                position: *position,
                q8: quantize_q8(vector),
                vector: vector.clone(),
            });
        }

        for entries in &mut grouped {
            entries.sort_by_key(|entry| entry.position.offset);
        }

        let mut lists = Vec::with_capacity(grouped.len());
        for (centroid_id, entries) in grouped.into_iter().enumerate() {
            let mut pages = Vec::new();
            for (page_idx, chunk) in entries.chunks(self.config.page_size).enumerate() {
                let object_key =
                    format!("quantized_ivf/list_{centroid_id:04}_page_{page_idx:04}.json");
                let page = QuantizedIvfPage {
                    centroid_id,
                    entries: chunk.to_vec(),
                };
                let page_path = self.dir.join(&object_key);
                if let Some(parent) = page_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let bytes = serde_json::to_vec(&page).map_err(|err| {
                    IndexError::Format(format!("serialize quantized IVF page: {err}"))
                })?;
                std::fs::write(&page_path, bytes)?;
                pages.push(QuantizedIvfPageRef {
                    object_key,
                    entry_count: page.entries.len(),
                });
            }
            lists.push(QuantizedIvfListManifest { centroid_id, pages });
        }

        let manifest = QuantizedIvfManifest {
            dimensions,
            metric: self.config.metric,
            centroids,
            lists,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|err| {
            IndexError::Format(format!("serialize quantized IVF manifest: {err}"))
        })?;
        std::fs::write(self.dir.join(MANIFEST_FILE), manifest_bytes)?;
        Ok(())
    }
}

/// Staged page reader for quantized IVFFlat artifacts.
pub struct QuantizedIvfReader {
    dir: PathBuf,
    manifest: QuantizedIvfManifest,
}

impl QuantizedIvfReader {
    pub fn open(dir: &Path) -> Result<Self, IndexError> {
        let manifest_path = dir.join(MANIFEST_FILE);
        let bytes = std::fs::read(&manifest_path)?;
        let manifest: QuantizedIvfManifest = serde_json::from_slice(&bytes).map_err(|err| {
            IndexError::Corrupt(format!(
                "quantized IVF manifest {} is corrupt: {err}",
                manifest_path.display()
            ))
        })?;
        validate_manifest(&manifest)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            manifest,
        })
    }

    pub fn manifest(&self) -> &QuantizedIvfManifest {
        &self.manifest
    }

    pub fn search(
        &self,
        query: &[f32],
        options: QuantizedIvfSearchOptions,
    ) -> Result<QuantizedIvfSearchResult, IndexError> {
        if options.k == 0 || self.manifest.centroids.is_empty() {
            return Ok(QuantizedIvfSearchResult {
                hits: Vec::new(),
                page_reads: 0,
                max_page_reads: options.max_page_reads,
            });
        }
        if query.len() != self.manifest.dimensions {
            return Err(IndexError::DimensionMismatch {
                expected: self.manifest.dimensions,
                got: query.len(),
            });
        }

        let probes = options.probes.max(1).min(self.manifest.centroids.len());
        let routed_lists = self.route_lists(query, probes);
        let required_pages: usize = routed_lists
            .iter()
            .map(|&list_id| self.manifest.lists[list_id].pages.len())
            .sum();
        if required_pages > options.max_page_reads {
            return Err(IndexError::Unsupported(format!(
                "page read budget exceeded: need {required_pages} pages for {probes} probes, budget is {}",
                options.max_page_reads
            )));
        }

        let mut page_reads = 0;
        let mut survivors = Vec::new();
        for list_id in routed_lists {
            let list = &self.manifest.lists[list_id];
            for page_ref in &list.pages {
                let page = self.read_page(page_ref)?;
                page_reads += 1;
                if page.centroid_id != list.centroid_id {
                    return Err(IndexError::Corrupt(format!(
                        "page {} belongs to centroid {}, expected {}",
                        page_ref.object_key, page.centroid_id, list.centroid_id
                    )));
                }
                for entry in page.entries {
                    let exact_score = distance(&self.manifest.metric, query, &entry.vector);
                    survivors.push(QuantizedIvfHit {
                        position: entry.position,
                        score: exact_score,
                    });
                }
            }
        }

        survivors.sort_by(compare_hits);
        survivors.truncate(options.k);
        Ok(QuantizedIvfSearchResult {
            hits: survivors,
            page_reads,
            max_page_reads: options.max_page_reads,
        })
    }

    fn route_lists(&self, query: &[f32], probes: usize) -> Vec<usize> {
        let mut scored: Vec<(f32, usize)> = self
            .manifest
            .centroids
            .iter()
            .enumerate()
            .map(|(idx, centroid)| (distance(&self.manifest.metric, query, centroid), idx))
            .collect();
        scored.sort_by(|a, b| compare_score_then_id(a.0, a.1, b.0, b.1));
        scored
            .into_iter()
            .take(probes)
            .map(|(_, idx)| idx)
            .collect()
    }

    fn read_page(&self, page_ref: &QuantizedIvfPageRef) -> Result<QuantizedIvfPage, IndexError> {
        let path = self.dir.join(&page_ref.object_key);
        let bytes = std::fs::read(&path)?;
        let page: QuantizedIvfPage = serde_json::from_slice(&bytes).map_err(|err| {
            IndexError::Corrupt(format!(
                "quantized IVF page {} is corrupt: {err}",
                path.display()
            ))
        })?;
        if page.entries.len() != page_ref.entry_count {
            return Err(IndexError::Corrupt(format!(
                "quantized IVF page {} entry count mismatch: manifest {}, page {}",
                path.display(),
                page_ref.entry_count,
                page.entries.len()
            )));
        }
        Ok(page)
    }
}

fn validate_manifest(manifest: &QuantizedIvfManifest) -> Result<(), IndexError> {
    if manifest.centroids.len() != manifest.lists.len() {
        return Err(IndexError::Corrupt(format!(
            "quantized IVF manifest has {} centroids but {} lists",
            manifest.centroids.len(),
            manifest.lists.len()
        )));
    }
    for centroid in &manifest.centroids {
        if centroid.len() != manifest.dimensions {
            return Err(IndexError::Corrupt(
                "quantized IVF centroid dimension mismatch".to_string(),
            ));
        }
    }
    for (idx, list) in manifest.lists.iter().enumerate() {
        if list.centroid_id != idx {
            return Err(IndexError::Corrupt(format!(
                "quantized IVF list id mismatch: expected {idx}, got {}",
                list.centroid_id
            )));
        }
    }
    Ok(())
}

fn deterministic_centroids(
    vectors: &[(RowPosition, Vec<f32>)],
    requested_lists: usize,
    metric: DistanceMetric,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    if vectors.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let list_count = requested_lists.min(vectors.len());
    let mut sorted = vectors.to_vec();
    sorted.sort_by_key(|(position, _)| position.offset);
    let mut centroids: Vec<Vec<f32>> = sorted
        .iter()
        .take(list_count)
        .map(|(_, vector)| vector.clone())
        .collect();
    let mut assignments = vec![0; vectors.len()];

    for _ in 0..16 {
        let mut changed = false;
        for (idx, (_, vector)) in vectors.iter().enumerate() {
            let nearest = nearest_centroid(vector, &centroids, metric);
            if assignments[idx] != nearest {
                assignments[idx] = nearest;
                changed = true;
            }
        }

        let dimensions = vectors[0].1.len();
        let mut sums = vec![vec![0.0f32; dimensions]; list_count];
        let mut counts = vec![0usize; list_count];
        for ((_, vector), &assignment) in vectors.iter().zip(&assignments) {
            counts[assignment] += 1;
            for (dim, value) in vector.iter().enumerate() {
                sums[assignment][dim] += *value;
            }
        }
        for centroid_id in 0..list_count {
            if counts[centroid_id] > 0 {
                for value in &mut sums[centroid_id] {
                    *value /= counts[centroid_id] as f32;
                }
                centroids[centroid_id] = sums[centroid_id].clone();
            }
        }

        if !changed {
            break;
        }
    }

    (centroids, assignments)
}

fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>], metric: DistanceMetric) -> usize {
    centroids
        .iter()
        .enumerate()
        .map(|(idx, centroid)| (distance(&metric, vector, centroid), idx))
        .min_by(|a, b| compare_score_then_id(a.0, a.1, b.0, b.1))
        .map(|(_, idx)| idx)
        .unwrap_or(0)
}

fn quantize_q8(vector: &[f32]) -> Vec<i8> {
    vector
        .iter()
        .map(|value| (value.clamp(-1.0, 1.0) * 127.0).round() as i8)
        .collect()
}

fn compare_hits(a: &QuantizedIvfHit, b: &QuantizedIvfHit) -> Ordering {
    compare_score_then_id(
        a.score,
        a.position.offset as usize,
        b.score,
        b.position.offset as usize,
    )
}

fn compare_score_then_id(a_score: f32, a_id: usize, b_score: f32, b_id: usize) -> Ordering {
    a_score
        .partial_cmp(&b_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a_id.cmp(&b_id))
}

#[cfg(test)]
mod quantized_ivf_reader_tests {
    use super::*;

    fn row(offset: u64) -> RowPosition {
        RowPosition::new(offset)
    }

    #[test]
    fn quantized_ivf_reader_returns_stable_top_k_with_page_budget_and_exact_rerank() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = QuantizedIvfBuilder::new(
            dir.path(),
            QuantizedIvfConfig {
                lists: 2,
                metric: DistanceMetric::L2,
                page_size: 3,
            },
        );

        builder.add_vector(row(30), &[10.4, 0.0]).unwrap();
        builder.add_vector(row(10), &[10.0, 0.0]).unwrap();
        builder.add_vector(row(20), &[10.2, 0.0]).unwrap();
        builder.add_vector(row(40), &[0.0, 10.0]).unwrap();
        builder.add_vector(row(50), &[0.0, 10.4]).unwrap();
        builder.finish().unwrap();

        let reader = QuantizedIvfReader::open(dir.path()).unwrap();
        let result = reader
            .search(
                &[10.05, 0.0],
                QuantizedIvfSearchOptions {
                    k: 3,
                    probes: 1,
                    max_page_reads: 1,
                },
            )
            .unwrap();

        let offsets: Vec<u64> = result.hits.iter().map(|hit| hit.position.offset).collect();
        assert_eq!(offsets, vec![10, 20, 30]);
        assert_eq!(result.page_reads, 1);
        assert!(result.page_reads <= result.max_page_reads);
    }

    #[test]
    fn quantized_ivf_reader_fails_loudly_when_budget_cannot_read_requested_probe_pages() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = QuantizedIvfBuilder::new(
            dir.path(),
            QuantizedIvfConfig {
                lists: 2,
                metric: DistanceMetric::L2,
                page_size: 2,
            },
        );
        builder.add_vector(row(1), &[0.0, 0.0]).unwrap();
        builder.add_vector(row(2), &[10.0, 10.0]).unwrap();
        builder.finish().unwrap();

        let reader = QuantizedIvfReader::open(dir.path()).unwrap();
        let err = reader
            .search(
                &[1.0, 1.0],
                QuantizedIvfSearchOptions {
                    k: 2,
                    probes: 2,
                    max_page_reads: 1,
                },
            )
            .unwrap_err();

        assert!(
            matches!(err, IndexError::Unsupported(message) if message.contains("page read budget"))
        );
    }

    #[test]
    fn quantized_ivf_reader_missing_or_corrupt_pages_fail_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = QuantizedIvfBuilder::new(
            dir.path(),
            QuantizedIvfConfig {
                lists: 1,
                metric: DistanceMetric::L2,
                page_size: 2,
            },
        );
        builder.add_vector(row(1), &[1.0, 0.0]).unwrap();
        builder.finish().unwrap();

        let reader = QuantizedIvfReader::open(dir.path()).unwrap();
        let first_page = reader.manifest().lists[0].pages[0].object_key.clone();
        std::fs::remove_file(dir.path().join(&first_page)).unwrap();
        let missing = reader
            .search(
                &[1.0, 0.0],
                QuantizedIvfSearchOptions {
                    k: 1,
                    probes: 1,
                    max_page_reads: 1,
                },
            )
            .unwrap_err();
        assert!(matches!(missing, IndexError::Io(_)));

        std::fs::write(dir.path().join(&first_page), b"not-json").unwrap();
        let corrupt = reader
            .search(
                &[1.0, 0.0],
                QuantizedIvfSearchOptions {
                    k: 1,
                    probes: 1,
                    max_page_reads: 1,
                },
            )
            .unwrap_err();
        assert!(matches!(corrupt, IndexError::Corrupt(message) if message.contains("page")));
    }
}
