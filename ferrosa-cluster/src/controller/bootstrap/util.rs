//! Shared helpers for bootstrap phases.
//!
//! The DeliverInvites, EstablishPools, BootstrapStream, and Promote
//! phases all share a "missing-set" pattern: compute
//! `expected − observed` and turn a non-empty result into a phase
//! error.  This module factors that pattern out.

use std::collections::BTreeSet;
use std::fmt::Debug;

use super::phase::{BootstrapError, BootstrapPhase};

/// Compute the set of `expected` keys that are absent from `observed`,
/// returning `None` when nothing is missing.
///
/// Returned in `BTreeSet` order so log lines and error messages are
/// deterministic across runs.
pub fn missing<T: Copy + Ord>(expected: &BTreeSet<T>, observed: &BTreeSet<T>) -> Option<Vec<T>> {
    let diff: Vec<T> = expected.difference(observed).copied().collect();
    if diff.is_empty() {
        None
    } else {
        Some(diff)
    }
}

/// Convert a `Vec<T>` of missing items into a phase error labelled
/// with `kind` (e.g. "peer", "owner").
pub fn missing_error<T: Debug>(
    phase: BootstrapPhase,
    kind: &str,
    missing: Vec<T>,
) -> BootstrapError {
    BootstrapError::phase(
        phase,
        format!("{n} {kind}(s) missing: {missing:?}", n = missing.len()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_returns_none_when_observed_covers_expected() {
        let expected: BTreeSet<u64> = [1, 2, 3].into_iter().collect();
        let observed: BTreeSet<u64> = [1, 2, 3].into_iter().collect();
        assert!(missing(&expected, &observed).is_none());
    }

    #[test]
    fn missing_returns_diff_in_sorted_order() {
        let expected: BTreeSet<u64> = [1, 2, 3, 4].into_iter().collect();
        let observed: BTreeSet<u64> = [2, 4].into_iter().collect();
        let m = missing(&expected, &observed).unwrap();
        assert_eq!(m, vec![1, 3]);
    }

    #[test]
    fn missing_error_attaches_phase_label() {
        let m: Vec<u64> = vec![5, 6];
        let err = missing_error(BootstrapPhase::EstablishPools, "peer", m);
        assert_eq!(err.name(), BootstrapPhase::EstablishPools);
        let msg = format!("{err}");
        assert!(msg.contains("peer"), "{msg}");
    }
}
