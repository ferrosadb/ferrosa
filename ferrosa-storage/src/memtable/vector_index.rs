//! Mutable vector index for memtable-level ANN search.
//!
//! Wraps an HNSW graph behind `RwLock` for concurrent read/write access.
//! Unlike the scalar MemtableIndex (persistent/functional), HNSW mutation
//! is inherently in-place, so we use locking instead of path-copying.

use parking_lot::RwLock;

use ferrosa_index::vector::{distance, IndexResult, RowPosition};
use ferrosa_index::DistanceMetric;

/// An ANN result paired with the partition-key scope captured at insert time
/// (`None` when the entry was inserted without a scope).
pub type ScopedIndexResult = (IndexResult, Option<Vec<u8>>);

/// Mutable in-memory vector index backed by a brute-force linear scan.
///
/// Thread safety: `RwLock` -- writers take exclusive lock for insert,
/// readers take shared lock for search. This simple implementation stores
/// all vectors and scans them at query time. It is suitable for the memtable
/// path where the number of vectors is bounded by memtable flush size.
pub struct VectorMemtableIndex {
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
    inner: RwLock<VectorMemtableInner>,
}

struct VectorMemtableInner {
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
    scopes: Vec<Option<Vec<u8>>>,
}

impl VectorMemtableInner {
    fn new() -> Self {
        Self {
            vectors: Vec::new(),
            positions: Vec::new(),
            scopes: Vec::new(),
        }
    }
}

impl VectorMemtableIndex {
    /// Create a new empty vector memtable index.
    ///
    /// `m` and `ef_construction` are HNSW parameters kept for API
    /// compatibility with the full HNSW implementation; this implementation
    /// performs a brute-force linear scan.
    pub fn new(metric: DistanceMetric, m: usize, ef_construction: usize) -> Self {
        Self {
            metric,
            m,
            ef_construction,
            inner: RwLock::new(VectorMemtableInner::new()),
        }
    }

    /// Insert a vector and its associated row position.
    pub fn insert(&self, position: RowPosition, vector: Vec<f32>) {
        self.insert_with_scope(position, vector, None);
    }

    /// Insert a vector and its associated row position plus optional prefix scope.
    pub fn insert_with_scope(
        &self,
        position: RowPosition,
        vector: Vec<f32>,
        scope: Option<Vec<u8>>,
    ) {
        let mut inner = self.inner.write();
        inner.positions.push(position);
        inner.vectors.push(vector);
        inner.scopes.push(scope);
    }

    /// Search for the `k` nearest vectors to `query`.
    ///
    /// `ef_search` is accepted for API compatibility but ignored in this
    /// brute-force implementation.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        _ef_search: usize,
    ) -> Result<Vec<IndexResult>, ferrosa_index::IndexError> {
        let inner = self.inner.read();

        if inner.vectors.is_empty() {
            return Ok(Vec::new());
        }

        // Brute-force: compute distance to every vector
        let mut scored: Vec<(f32, RowPosition)> = inner
            .vectors
            .iter()
            .zip(inner.positions.iter())
            .map(|(vec, pos)| (distance(&self.metric, query, vec), *pos))
            .collect();

        // Sort ascending by distance (lowest = closest)
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        let results = scored
            .into_iter()
            .map(|(score, position)| IndexResult { position, score })
            .collect();

        Ok(results)
    }

    /// Search for nearest vectors restricted to a specific prefix scope.
    pub fn search_with_scope(
        &self,
        query: &[f32],
        k: usize,
        _ef_search: usize,
        scope: &[u8],
    ) -> Result<Vec<IndexResult>, ferrosa_index::IndexError> {
        let inner = self.inner.read();

        if inner.vectors.is_empty() {
            return Ok(Vec::new());
        }

        let mut scored: Vec<(f32, RowPosition)> = inner
            .vectors
            .iter()
            .zip(inner.positions.iter())
            .zip(inner.scopes.iter())
            .filter_map(|((vec, pos), entry_scope)| {
                entry_scope
                    .as_deref()
                    .filter(|entry_scope| *entry_scope == scope)
                    .map(|_| (distance(&self.metric, query, vec), *pos))
            })
            .collect();

        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        Ok(scored
            .into_iter()
            .map(|(score, position)| IndexResult { position, score })
            .collect())
    }

    /// Search for the `k` nearest vectors to `query`, returning each result
    /// paired with the partition-key scope captured at insert time.
    ///
    /// The scope is the serialized partition key bytes recorded via
    /// `insert_with_scope`. It is `None` for entries inserted without a scope.
    /// The router-level index-consult path uses the scope to recover the base
    /// table row, since the vector `RowPosition::offset` is only a sequential
    /// placeholder in the memtable and cannot address a row on its own.
    ///
    /// `ef_search` is accepted for API compatibility but ignored in this
    /// brute-force implementation.
    pub fn search_with_scopes(
        &self,
        query: &[f32],
        k: usize,
        _ef_search: usize,
    ) -> Result<Vec<ScopedIndexResult>, ferrosa_index::IndexError> {
        let inner = self.inner.read();

        if inner.vectors.is_empty() {
            return Ok(Vec::new());
        }

        let mut scored: Vec<(f32, RowPosition, Option<Vec<u8>>)> = inner
            .vectors
            .iter()
            .zip(inner.positions.iter())
            .zip(inner.scopes.iter())
            .map(|((vec, pos), scope)| (distance(&self.metric, query, vec), *pos, scope.clone()))
            .collect();

        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        Ok(scored
            .into_iter()
            .map(|(score, position, scope)| (IndexResult { position, score }, scope))
            .collect())
    }

    /// Returns the number of vectors stored in this index.
    pub fn len(&self) -> usize {
        self.inner.read().vectors.len()
    }

    /// Returns `true` if no vectors have been inserted.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain all vectors from the index, returning them for use during flush.
    ///
    /// After calling this, the index is empty and ready to receive new writes.
    pub fn drain(&self) -> Vec<(RowPosition, Vec<f32>)> {
        let mut inner = self.inner.write();
        let positions = std::mem::take(&mut inner.positions);
        let vectors = std::mem::take(&mut inner.vectors);
        let _ = std::mem::take(&mut inner.scopes);
        positions.into_iter().zip(vectors).collect()
    }

    /// Drain all vectors from the index with their optional prefix scopes.
    pub fn drain_with_scopes(&self) -> Vec<(Option<Vec<u8>>, RowPosition, Vec<f32>)> {
        let mut inner = self.inner.write();
        let positions = std::mem::take(&mut inner.positions);
        let vectors = std::mem::take(&mut inner.vectors);
        let scopes = std::mem::take(&mut inner.scopes);
        scopes
            .into_iter()
            .zip(positions)
            .zip(vectors)
            .map(|((scope, position), vector)| (scope, position, vector))
            .collect()
    }

    /// HNSW `m` parameter (max connections per node per layer).
    pub fn m(&self) -> usize {
        self.m
    }

    /// HNSW `ef_construction` parameter (construction-time search width).
    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    /// Distance metric used by this index.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_index::vector::RowPosition;
    use ferrosa_index::DistanceMetric;

    #[test]
    fn insert_and_search_l2() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert(RowPosition::new(0), vec![1.0, 0.0, 0.0]);
        index.insert(RowPosition::new(100), vec![0.0, 1.0, 0.0]);
        index.insert(RowPosition::new(200), vec![0.9, 0.1, 0.0]);

        let results = index.search(&[1.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(results.len(), 2);
        // Closest should be the exact match at offset 0
        assert_eq!(results[0].position.offset, 0);
        assert!(results[0].score < 1e-6);
    }

    #[test]
    fn search_empty_index() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        let results = index.search(&[1.0, 0.0], 5, 50).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_cosine_metric() {
        let index = VectorMemtableIndex::new(DistanceMetric::Cosine, 16, 200);
        index.insert(RowPosition::new(0), vec![1.0, 0.0]);
        index.insert(RowPosition::new(100), vec![0.0, 1.0]);
        index.insert(RowPosition::new(200), vec![0.7, 0.7]);

        let results = index.search(&[1.0, 0.0], 1, 50).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].position.offset, 0);
    }

    #[test]
    fn concurrent_insert_and_search() {
        use std::sync::Arc;
        use std::thread;

        let index = Arc::new(VectorMemtableIndex::new(DistanceMetric::L2, 16, 200));
        let num_threads = 4;
        let inserts_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let idx = Arc::clone(&index);
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        let offset = (t * inserts_per_thread + i) as u64 * 100;
                        let v = vec![t as f32, i as f32, 0.0];
                        idx.insert(RowPosition::new(offset), v);
                    }
                })
            })
            .collect();

        // Concurrent reader
        let reader_idx = Arc::clone(&index);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                let _ = reader_idx.search(&[0.0, 0.0, 0.0], 5, 50);
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        reader.join().unwrap();

        // After all inserts, search should return results
        let results = index.search(&[0.0, 0.0, 0.0], 5, 50).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn entry_count_tracks_inserts() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        assert_eq!(index.len(), 0);
        index.insert(RowPosition::new(0), vec![1.0, 0.0]);
        index.insert(RowPosition::new(100), vec![0.0, 1.0]);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn drain_clears_index() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert(RowPosition::new(0), vec![1.0, 0.0]);
        index.insert(RowPosition::new(100), vec![0.0, 1.0]);
        assert_eq!(index.len(), 2);

        let drained = index.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn drain_returns_all_entries() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert(RowPosition::new(0), vec![1.0, 0.0]);
        index.insert(RowPosition::new(100), vec![0.5, 0.5]);

        let drained = index.drain();
        let offsets: Vec<u64> = drained.iter().map(|(pos, _)| pos.offset).collect();
        assert!(offsets.contains(&0));
        assert!(offsets.contains(&100));
    }

    #[test]
    fn search_returns_at_most_k() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        for i in 0..10u64 {
            index.insert(RowPosition::new(i * 100), vec![i as f32, 0.0]);
        }
        let results = index.search(&[0.0, 0.0], 3, 50).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn search_with_scopes_returns_partition_scope_in_distance_order() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert_with_scope(
            RowPosition::new(0),
            vec![10.0, 0.0],
            Some(b"pk_far".to_vec()),
        );
        index.insert_with_scope(
            RowPosition::new(1),
            vec![1.0, 0.0],
            Some(b"pk_near".to_vec()),
        );
        index.insert_with_scope(
            RowPosition::new(2),
            vec![5.0, 0.0],
            Some(b"pk_mid".to_vec()),
        );

        let results = index.search_with_scopes(&[0.0, 0.0], 3, 50).unwrap();
        assert_eq!(results.len(), 3);
        // Nearest first: pk_near, pk_mid, pk_far.
        assert_eq!(results[0].1.as_deref(), Some(b"pk_near".as_ref()));
        assert_eq!(results[1].1.as_deref(), Some(b"pk_mid".as_ref()));
        assert_eq!(results[2].1.as_deref(), Some(b"pk_far".as_ref()));
        // Scores are non-decreasing.
        for w in results.windows(2) {
            assert!(w[0].0.score <= w[1].0.score);
        }
    }

    #[test]
    fn search_with_scopes_truncates_to_k() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        for i in 0..10u64 {
            index.insert_with_scope(
                RowPosition::new(i),
                vec![i as f32, 0.0],
                Some(format!("pk{i}").into_bytes()),
            );
        }
        let results = index.search_with_scopes(&[0.0, 0.0], 3, 50).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn search_with_scopes_carries_none_for_unscoped_inserts() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert(RowPosition::new(0), vec![1.0, 0.0]);
        let results = index.search_with_scopes(&[1.0, 0.0], 1, 50).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, None);
    }

    #[test]
    fn search_results_sorted_by_distance() {
        let index = VectorMemtableIndex::new(DistanceMetric::L2, 16, 200);
        index.insert(RowPosition::new(0), vec![10.0, 0.0]);
        index.insert(RowPosition::new(100), vec![1.0, 0.0]);
        index.insert(RowPosition::new(200), vec![5.0, 0.0]);

        let results = index.search(&[0.0, 0.0], 3, 50).unwrap();
        assert_eq!(results.len(), 3);
        // Scores must be non-decreasing
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score);
        }
        // Closest is [1.0, 0.0] at offset 100
        assert_eq!(results[0].position.offset, 100);
    }
}
