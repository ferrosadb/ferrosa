//! Bounded selection of scored, owned ANN candidates without payload copies.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct ScoredRow<T> {
    distance: f64,
    ordinal: usize,
    row: T,
}

impl<T> PartialEq for ScoredRow<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for ScoredRow<T> {}

impl<T> PartialOrd for ScoredRow<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ScoredRow<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

/// Retains the nearest `k` rows, breaking distance ties by encounter ordinal.
pub(crate) struct AnnTopK<T> {
    k: usize,
    rows: BinaryHeap<ScoredRow<T>>,
}

impl<T> AnnTopK<T> {
    /// Allocates the selector storage once. Callers must cap `k` to input size.
    pub(crate) fn new(k: usize) -> Self {
        Self {
            k,
            rows: BinaryHeap::with_capacity(k),
        }
    }

    /// Moves a candidate into the selector or drops it without growing storage.
    pub(crate) fn offer(&mut self, distance: f64, ordinal: usize, row: T) {
        let candidate = ScoredRow {
            distance,
            ordinal,
            row,
        };
        if self.len() < self.k {
            self.rows.push(candidate);
        } else if let Some(mut worst) = self.rows.peek_mut() {
            if candidate < *worst {
                *worst = candidate;
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    /// Sorts in the heap's allocation and moves rows out in ascending order.
    pub(crate) fn into_sorted_rows(self) -> impl Iterator<Item = T> {
        self.rows
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.row)
    }
}
