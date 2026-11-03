//! Module: the durability book — what a run attempted, acknowledged, left indeterminate and
//! recovered, kept as multisets, and the verdict computed from it.
//! Correctness: correct when every derived set equals its Lean definition in
//! `specs/lean4/Ferrosa/Testing/Histories.lean` (`lost`, `unexpected`, `fabricated`,
//! `recoveredIndeterminate`, `anomalous`, `verdictFor`); each witness there has a test here of the
//! same name.
//! Last revised: 2026-10-31
//! Last changed: New module (packet JE-4, decision D7 of `specs/jepsen-evidence`).
//!
//! A pass/fail bit cannot say "a user silently lost data"; only the multiset arithmetic can. `lost`
//! and `fabricated` are separate multisets and are never summed, because a run can lose one value
//! and fabricate another, and one count would hide the loss behind the fabrication.
//!
//! * `lost = acknowledged − recovered`
//! * `unexpected = recovered − acknowledged`
//! * `fabricated = unexpected − indeterminate` — only indeterminate writes excuse a recovered value.
//!   A write that failed definitively and was read back is a fabrication: the database said it did
//!   not happen.
//! * `recovered_indeterminate = unexpected − fabricated` — reported, never convicting.
//!
//! Multisets are counts (`value → multiplicity`). The book's size is bounded by the attempted writes
//! plus the values the final read returned; values are unique per attempt in practice, but the
//! arithmetic is multiset-correct so a duplicate is counted rather than collapsed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::profile::{Assurance, ConsistencyProfile, NotTested};

/// A multiset of written values, stored as multiplicities. Zero counts are never stored, so two
/// multisets with the same elements compare equal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Multiset {
    counts: BTreeMap<i64, u64>,
}

impl Multiset {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one copy of `v`.
    pub fn add(&mut self, v: i64) {
        *self.counts.entry(v).or_insert(0) += 1;
    }

    /// How many copies of `v` the multiset holds (Lean `cnt`).
    pub fn count(&self, v: i64) -> u64 {
        self.counts.get(&v).copied().unwrap_or(0)
    }

    /// Total number of elements, counting multiplicity.
    pub fn len(&self) -> u64 {
        self.counts.values().sum()
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Distinct values with their multiplicities, in ascending value order.
    pub fn iter(&self) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.counts.iter().map(|(v, n)| (*v, *n))
    }

    /// Multiset subtraction (Lean `msSub`): remove one copy of each element of `other`. A value
    /// with more copies in `other` than in `self` does not go negative.
    pub fn sub(&self, other: &Multiset) -> Multiset {
        let counts = self
            .counts
            .iter()
            .filter_map(|(v, n)| {
                let left = n.saturating_sub(other.count(*v));
                (left > 0).then_some((*v, left))
            })
            .collect();
        Multiset { counts }
    }

    /// The elements, one entry per copy, ascending. For reports and tests over small sets.
    pub fn to_vec(&self) -> Vec<i64> {
        let mut out = Vec::with_capacity(usize::try_from(self.len()).unwrap_or(0));
        for (v, n) in self.iter() {
            for _ in 0..n {
                out.push(v);
            }
        }
        out
    }
}

impl FromIterator<i64> for Multiset {
    fn from_iter<I: IntoIterator<Item = i64>>(iter: I) -> Self {
        let mut m = Multiset::new();
        for v in iter {
            m.add(v);
        }
        m
    }
}

/// What a run recorded (Lean `Book`). A write that failed definitively is in `attempted` and in
/// neither `acknowledged` nor `indeterminate`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    /// Every value whose write was invoked.
    pub attempted: Multiset,
    /// Values whose write returned success.
    pub acknowledged: Multiset,
    /// Values whose write timed out or failed after being sent: the client cannot know whether
    /// it applied.
    pub indeterminate: Multiset,
    /// Values the final read returned.
    pub recovered: Multiset,
}

impl Book {
    /// Acknowledged but not observed: the write was promised and the read did not return it.
    pub fn lost(&self) -> Multiset {
        self.acknowledged.sub(&self.recovered)
    }

    /// Observed but never acknowledged.
    pub fn unexpected(&self) -> Multiset {
        self.recovered.sub(&self.acknowledged)
    }

    /// Observed, never acknowledged, and not explained by any write whose outcome was unknown.
    pub fn fabricated(&self) -> Multiset {
        self.unexpected().sub(&self.indeterminate)
    }

    /// The part of `unexpected` an indeterminate write explains. Reported, never convicting.
    pub fn recovered_indeterminate(&self) -> Multiset {
        self.unexpected().sub(&self.fabricated())
    }

    /// A loss with nothing unexpected read: the shape of the failure the product forbids.
    pub fn silent_loss(&self) -> bool {
        !self.lost().is_empty() && self.unexpected().is_empty()
    }

    /// Something lost or something fabricated. A recovered indeterminate write is not an anomaly.
    pub fn anomalous(&self) -> bool {
        !self.lost().is_empty() || !self.fabricated().is_empty()
    }

    /// **The verdict is a function of `(book, profile)`** (Lean `verdictFor`). The profile decides
    /// whether an anomaly is evidence; the book decides whether there is one. A run that
    /// acknowledged nothing cannot lose anything, so a clean one is not tested rather than
    /// confirmed (D9); a fabrication still refutes it, because that needs no acknowledged write.
    /// `lost` and `fabricated` are tested separately and never summed.
    pub fn verdict(&self, profile: &ConsistencyProfile) -> Assurance {
        match profile.classify(!self.anomalous()) {
            Assurance::Confirmed if self.acknowledged.is_empty() => {
                Assurance::NotTested(NotTested::NoAcknowledgedWrites)
            }
            decided => decided,
        }
    }

    /// The counts and a bounded sample of each derived set, for the run's report.
    pub fn summary(&self) -> BookSummary {
        let lost = self.lost();
        let fabricated = self.fabricated();
        let recovered_indeterminate = self.recovered_indeterminate();
        BookSummary {
            attempted: self.attempted.len(),
            acknowledged: self.acknowledged.len(),
            indeterminate: self.indeterminate.len(),
            recovered: self.recovered.len(),
            lost: lost.len(),
            fabricated: fabricated.len(),
            recovered_indeterminate: recovered_indeterminate.len(),
            lost_sample: sample(&lost),
            fabricated_sample: sample(&fabricated),
            recovered_indeterminate_sample: sample(&recovered_indeterminate),
        }
    }
}

/// How many values of each derived set a summary names. The counts are always exact.
pub const SUMMARY_SAMPLE: usize = 32;

fn sample(m: &Multiset) -> Vec<i64> {
    m.iter()
        .flat_map(|(v, n)| std::iter::repeat_n(v, usize::try_from(n).unwrap_or(usize::MAX)))
        .take(SUMMARY_SAMPLE)
        .collect()
}

/// A book reduced to what a report carries: exact counts, and at most [`SUMMARY_SAMPLE`] values of
/// each derived set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSummary {
    pub attempted: u64,
    pub acknowledged: u64,
    pub indeterminate: u64,
    pub recovered: u64,
    pub lost: u64,
    pub fabricated: u64,
    pub recovered_indeterminate: u64,
    pub lost_sample: Vec<i64>,
    pub fabricated_sample: Vec<i64>,
    pub recovered_indeterminate_sample: Vec<i64>,
}

impl BookSummary {
    /// The anomalies, each named separately, or `None` when there are none. `lost` and
    /// `fabricated` are reported side by side, never as one error count.
    pub fn anomaly_message(&self) -> Option<String> {
        let mut parts = Vec::new();
        if self.lost > 0 {
            parts.push(format!(
                "lost {} acknowledged value(s) (first {}: {:?})",
                self.lost,
                self.lost_sample.len(),
                self.lost_sample
            ));
        }
        if self.fabricated > 0 {
            parts.push(format!(
                "fabricated {} value(s) no write could have produced (first {}: {:?})",
                self.fabricated,
                self.fabricated_sample.len(),
                self.fabricated_sample
            ));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("; "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::profile::Level;

    fn ms(values: &[i64]) -> Multiset {
        values.iter().copied().collect()
    }

    /// Lean `⟨attempts, acknowledged, recovered, indeterminate⟩`, in that field order.
    fn book(attempts: &[i64], acked: &[i64], recovered: &[i64], indeterminate: &[i64]) -> Book {
        Book {
            attempted: ms(attempts),
            acknowledged: ms(acked),
            indeterminate: ms(indeterminate),
            recovered: ms(recovered),
        }
    }

    fn quorum() -> ConsistencyProfile {
        ConsistencyProfile::new(3, Level::Quorum, Level::Quorum, false)
    }

    #[test]
    fn a_loss_and_a_fabrication_do_not_cancel() {
        let b = book(&[1, 2], &[1, 2], &[1, 3], &[]);
        assert_eq!(b.lost().to_vec(), vec![2]);
        assert_eq!(b.fabricated().to_vec(), vec![3]);
        // The counts are equal and opposite; one error count would read as "nothing wrong".
        assert_eq!(b.recovered.len(), b.acknowledged.len());
        assert_eq!(b.verdict(&quorum()), Assurance::Refuted);
    }

    #[test]
    fn a_timed_out_write_that_landed_is_not_fabricated() {
        let b = book(&[1], &[], &[1], &[1]);
        assert!(b.fabricated().is_empty());
        assert_eq!(b.recovered_indeterminate().to_vec(), vec![1]);
        assert_eq!(b.unexpected().to_vec(), vec![1]);
    }

    #[test]
    fn a_definite_failure_that_landed_is_fabricated() {
        let b = book(&[1], &[], &[1], &[]);
        assert_eq!(b.fabricated().to_vec(), vec![1]);
        assert!(b.anomalous());
    }

    #[test]
    fn an_indeterminate_write_never_hides_a_loss() {
        let b = book(&[1, 2], &[1], &[2], &[2]);
        assert_eq!(b.lost().to_vec(), vec![1]);
        assert!(b.fabricated().is_empty());
        assert_eq!(b.verdict(&quorum()), Assurance::Refuted);
    }

    #[test]
    fn a_landed_timeout_under_quorum_is_confirmed() {
        let b = book(&[1, 2], &[1], &[1, 2], &[2]);
        assert_eq!(b.verdict(&quorum()), Assurance::Confirmed);
        assert_eq!(b.recovered_indeterminate().to_vec(), vec![2]);
    }

    #[test]
    fn a_run_without_a_final_read_loses_every_acknowledged_write() {
        let b = book(&[1, 2], &[1, 2], &[], &[]);
        assert_eq!(b.lost().to_vec(), vec![1, 2]);
        assert!(b.silent_loss());
    }

    #[test]
    fn a_write_free_generator_cannot_test_durability() {
        // Whatever was read, and whatever was indeterminate, nothing acknowledged means nothing lost…
        for (recovered, indeterminate) in [(&[][..], &[][..]), (&[4, 5][..], &[5][..])] {
            let b = book(&[], &[], recovered, indeterminate);
            assert!(b.lost().is_empty(), "{b:?}");
        }
        // …so the run is not tested, never confirmed (D9).
        assert_eq!(
            book(&[], &[], &[], &[]).verdict(&quorum()),
            Assurance::NotTested(NotTested::NoAcknowledgedWrites)
        );
    }

    /// D9 excuses only the absence of a loss: a run that acknowledged nothing but read back a value
    /// that failed definitively has still fabricated it, and that refutes (Lean `verdictFor` has no
    /// write-count clause; `anomalous` alone decides).
    #[test]
    fn a_fabrication_without_acknowledged_writes_is_still_refuted() {
        let b = book(&[1], &[], &[1], &[]);
        assert_eq!(b.verdict(&quorum()), Assurance::Refuted);
    }

    /// Multiplicity is respected: two acknowledged copies and one recovered leaves one lost copy,
    /// and a subtrahend with more copies than the minuend does not go negative.
    #[test]
    fn subtraction_counts_duplicates() {
        assert_eq!(ms(&[7, 7, 8]).sub(&ms(&[7])).to_vec(), vec![7, 8]);
        assert_eq!(ms(&[7]).sub(&ms(&[7, 7, 7])).to_vec(), Vec::<i64>::new());
        assert_eq!(ms(&[7, 7]).sub(&ms(&[7])), ms(&[7]));
        let b = book(&[7, 7], &[7, 7], &[7], &[]);
        assert_eq!(b.lost().to_vec(), vec![7]);
    }

    /// The profile is read first: a clean book under ONE/ONE is not confirmed.
    #[test]
    fn a_clean_book_under_one_one_is_not_confirmed() {
        let b = book(&[1], &[1], &[1], &[]);
        let one = ConsistencyProfile::new(3, Level::One, Level::One, false);
        assert_eq!(
            b.verdict(&one),
            Assurance::NotTested(NotTested::ReadsDoNotOverlapWrites)
        );
        assert_eq!(b.verdict(&quorum()), Assurance::Confirmed);
    }

    #[test]
    fn the_summary_names_loss_and_fabrication_separately() {
        let s = book(&[1, 2], &[1, 2], &[1, 3], &[]).summary();
        assert_eq!((s.lost, s.fabricated), (1, 1));
        let msg = s.anomaly_message().expect("anomalous");
        assert!(msg.contains("lost 1"), "{msg}");
        assert!(msg.contains("fabricated 1"), "{msg}");
        assert_eq!(
            book(&[1], &[1], &[1], &[]).summary().anomaly_message(),
            None
        );
    }
}
