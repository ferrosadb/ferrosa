//! Bounded top-k selection for full-text hits (t_ee98faa0 layer 2).
//!
//! A `LIMIT k` full-text query must never materialize its full match set to
//! rank it — [`TopK`] keeps the k best-scoring (score, doc-key) pairs in a
//! min-heap so per-search memory is O(k), independent of how many documents
//! match. `k` is always QUERY-derived (the statement's `LIMIT`), never a
//! server-side cap.
//!
//! Invariant: callers push each doc key at most once per search (posting
//! lists hold one posting per document; map-based callers push from a
//! deduplicated map). Pushing partial scores for the same key twice would
//! rank the key by whichever partial survived.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::reader::FtsHit;

/// One retained candidate. Ordered by score using `f64::total_cmp` for a
/// total order; on score ties the SMALLER key ranks higher. This matches the
/// deterministic sorted-partition-key order the CQL `fts_match` arm iterates
/// in, so a pushed-down `LIMIT k` selects the same rows the arm's early-exit
/// loop would have kept.
struct Entry {
    score: f64,
    key: Vec<u8>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher score wins; on ties the SMALLER key is the better candidate.
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.key.cmp(&self.key))
    }
}

/// Bounded top-k accumulator: retains at most `k` entries, evicting the
/// current worst when a better candidate arrives. O(k) memory.
pub struct TopK {
    k: usize,
    heap: BinaryHeap<Reverse<Entry>>,
}

impl TopK {
    /// A new accumulator retaining the `k` best-scoring keys.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k.min(4096).saturating_add(1)),
        }
    }

    /// Offer an owned key. The key is dropped immediately when it cannot beat
    /// the current worst retained entry.
    pub fn push_owned(&mut self, key: Vec<u8>, score: f64) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(Reverse(Entry { score, key }));
            return;
        }
        let candidate = Entry { score, key };
        if let Some(worst) = self.heap.peek() {
            if candidate > worst.0 {
                self.heap.pop();
                self.heap.push(Reverse(candidate));
            }
        }
    }

    /// Offer a borrowed key; it is cloned ONLY when it will actually be
    /// retained, so rejected candidates cost no allocation.
    pub fn push(&mut self, key: &[u8], score: f64) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(Reverse(Entry {
                score,
                key: key.to_vec(),
            }));
            return;
        }
        if let Some(worst) = self.heap.peek() {
            let beats = match score.total_cmp(&worst.0.score) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => key < worst.0.key.as_slice(),
            };
            if beats {
                self.heap.pop();
                self.heap.push(Reverse(Entry {
                    score,
                    key: key.to_vec(),
                }));
            }
        }
    }

    /// The retained hits, best score first (key-ascending on ties).
    pub fn into_hits(self) -> Vec<FtsHit> {
        let mut entries: Vec<Entry> = self.heap.into_iter().map(|r| r.0).collect();
        entries.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.key.cmp(&b.key)));
        entries
            .into_iter()
            .map(|e| FtsHit {
                partition_key: e.key,
                score: e.score,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_k_best_scores() {
        let mut topk = TopK::new(3);
        for i in 0..100u32 {
            topk.push(format!("k{i:03}").as_bytes(), f64::from(i));
        }
        let hits = topk.into_hits();
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].score, 99.0);
        assert_eq!(hits[1].score, 98.0);
        assert_eq!(hits[2].score, 97.0);
    }

    #[test]
    fn fewer_candidates_than_k_returns_all() {
        let mut topk = TopK::new(10);
        topk.push(b"a", 1.0);
        topk.push_owned(b"b".to_vec(), 2.0);
        let hits = topk.into_hits();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].partition_key, b"b");
    }

    #[test]
    fn k_zero_retains_nothing() {
        let mut topk = TopK::new(0);
        topk.push(b"a", 1.0);
        assert!(topk.into_hits().is_empty());
    }

    #[test]
    fn ties_break_deterministically_by_key() {
        let mut a = TopK::new(2);
        let mut b = TopK::new(2);
        for key in [b"k1", b"k2", b"k3"] {
            a.push(key.as_slice(), 5.0);
        }
        for key in [b"k3", b"k1", b"k2"] {
            b.push(key.as_slice(), 5.0);
        }
        let (ha, hb) = (a.into_hits(), b.into_hits());
        assert_eq!(
            ha.iter().map(|h| &h.partition_key).collect::<Vec<_>>(),
            hb.iter().map(|h| &h.partition_key).collect::<Vec<_>>(),
            "tie-breaking must not depend on push order"
        );
    }
}
