//! Whether a scatter-gather full-text result actually covers the ring.
//!
//! `coordinate_fulltext_search` fans a query out to every node and unions the
//! matching doc keys. Until 2026-09-21 it failed the WHOLE search if ANY node
//! failed to answer:
//!
//! ```text
//! coordinate_fulltext_search: replica failure makes the result incomplete
//!   failed_nodes=1 keys_received=4
//! ```
//!
//! The fail-loud instinct was right — a silently partial match set is the worst
//! outcome — but the test was wrong. A node failing does not imply the result
//! is short. With RF=3 on three nodes every node holds every token range, so
//! two responders already cover the ring and the union is complete. The check
//! counted responders instead of asking what they covered.
//!
//! The cost of that was not one failed query. `fulltext_guard` reacts to a
//! failed search by disabling EVERY lexical search leg for a 30-60s backoff, so
//! one transient replica hiccup became a sustained lexical-search outage and
//! `hybrid_search` silently degraded to vector-only.
//!
//! So ask coverage, not counts: the union is complete exactly when every token
//! range has at least one responding owner. Below that it is genuinely short
//! and still fails loudly, naming the ranges nobody answered for.

use std::collections::HashSet;

/// A token range's identity, as the ring reports it.
///
/// Opaque here on purpose: coverage is a set-cover question and does not care
/// how ranges are numbered.
pub type RangeId = u64;

/// A cluster node's identity.
pub type NodeId = u64;

/// What the responding nodes actually covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulltextCoverage {
    /// Every token range had at least one owner respond. The union is complete
    /// and may be served, however many nodes failed.
    Complete,

    /// At least one token range had no owner respond, so the union is missing
    /// whatever those ranges hold. Fail loudly and name them.
    Incomplete { uncovered: Vec<RangeId> },
}

impl FulltextCoverage {
    /// Whether the result may be served.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Decide whether the nodes that answered cover every token range.
///
/// `range_owners` is every token range paired with the nodes that own a replica
/// of it. `responded` is the nodes that returned keys.
///
/// A range is covered when any one of its owners responded — replicas of a
/// range hold the same rows, so one answer for a range is the whole range.
pub fn classify_fulltext_coverage(
    range_owners: &[(RangeId, Vec<NodeId>)],
    responded: &HashSet<NodeId>,
) -> FulltextCoverage {
    let uncovered: Vec<RangeId> = range_owners
        .iter()
        .filter(|(_, owners)| !owners.iter().any(|owner| responded.contains(owner)))
        .map(|(range, _)| *range)
        .collect();

    if uncovered.is_empty() {
        FulltextCoverage::Complete
    } else {
        FulltextCoverage::Incomplete { uncovered }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn responded(ids: &[NodeId]) -> HashSet<NodeId> {
        ids.iter().copied().collect()
    }

    /// Three ranges, RF=3 on three nodes: every node owns everything.
    fn rf3_on_three_nodes() -> Vec<(RangeId, Vec<NodeId>)> {
        vec![(1, vec![1, 2, 3]), (2, vec![1, 2, 3]), (3, vec![1, 2, 3])]
    }

    /// The ordinary case.
    #[test]
    fn every_node_responding_is_complete() {
        let coverage = classify_fulltext_coverage(&rf3_on_three_nodes(), &responded(&[1, 2, 3]));
        assert_eq!(coverage, FulltextCoverage::Complete);
        assert!(coverage.is_complete());
    }

    /// The regression, and the exact shape observed on the live cluster:
    /// `failed_nodes=1 keys_received=4` with RF=3 on three nodes. Node 1 did
    /// not answer, but nodes 2 and 3 each own every range, so the union is
    /// already the whole ring. The old count-based check failed this search and
    /// tripped `fulltext_guard`, disabling every lexical leg for a backoff.
    #[test]
    fn a_failed_node_whose_ranges_are_covered_elsewhere_is_complete() {
        let coverage = classify_fulltext_coverage(&rf3_on_three_nodes(), &responded(&[2, 3]));
        assert_eq!(
            coverage,
            FulltextCoverage::Complete,
            "two RF=3 replicas cover every range; the union is not short"
        );
    }

    /// Even a single responder covers an RF=3 three-node ring.
    #[test]
    fn one_responder_covers_a_fully_replicated_ring() {
        assert_eq!(
            classify_fulltext_coverage(&rf3_on_three_nodes(), &responded(&[3])),
            FulltextCoverage::Complete
        );
    }

    /// The case the fail-loud check exists for: a range whose only owner is
    /// silent. Serving here would hand back a short match set the caller
    /// believes is whole.
    #[test]
    fn a_range_with_no_responding_owner_is_incomplete() {
        let owners = vec![(1, vec![1, 2]), (2, vec![2, 3]), (3, vec![4])];
        assert_eq!(
            classify_fulltext_coverage(&owners, &responded(&[1, 2, 3])),
            FulltextCoverage::Incomplete { uncovered: vec![3] },
            "range 3's only owner (node 4) did not answer"
        );
    }

    /// Every owner of a range failing is still incomplete, not merely degraded.
    #[test]
    fn a_range_whose_every_owner_failed_is_incomplete() {
        let owners = vec![(1, vec![1, 2]), (2, vec![3, 4])];
        assert_eq!(
            classify_fulltext_coverage(&owners, &responded(&[1, 2])),
            FulltextCoverage::Incomplete { uncovered: vec![2] }
        );
    }

    /// Nobody answered: every range is uncovered, and the error should say so
    /// rather than reporting an empty result set.
    #[test]
    fn no_responders_leaves_every_range_uncovered() {
        assert_eq!(
            classify_fulltext_coverage(&rf3_on_three_nodes(), &responded(&[])),
            FulltextCoverage::Incomplete {
                uncovered: vec![1, 2, 3]
            }
        );
    }

    /// Every uncovered range is named, not just the first — an operator
    /// chasing this needs the whole set.
    #[test]
    fn all_uncovered_ranges_are_reported() {
        let owners = vec![(10, vec![1]), (20, vec![2]), (30, vec![3]), (40, vec![1])];
        assert_eq!(
            classify_fulltext_coverage(&owners, &responded(&[2])),
            FulltextCoverage::Incomplete {
                uncovered: vec![10, 30, 40]
            }
        );
    }

    /// A responder that owns nothing cannot cover anything. Guards against
    /// "somebody answered, so we're fine".
    #[test]
    fn a_responder_that_owns_no_range_covers_nothing() {
        let owners = vec![(1, vec![1]), (2, vec![2])];
        assert_eq!(
            classify_fulltext_coverage(&owners, &responded(&[99])),
            FulltextCoverage::Incomplete {
                uncovered: vec![1, 2]
            }
        );
    }

    /// Degenerate ring: nothing to cover, so nothing is missing. An empty
    /// result here is the true answer, not a hidden failure.
    #[test]
    fn an_empty_ring_is_trivially_complete() {
        assert_eq!(
            classify_fulltext_coverage(&[], &responded(&[])),
            FulltextCoverage::Complete
        );
    }

    /// The property the type exists to hold: completeness depends ONLY on
    /// coverage, never on how many nodes failed. Any non-empty subset of a
    /// fully-replicated ring is complete.
    #[test]
    fn completeness_depends_on_coverage_not_on_failure_count() {
        let owners = rf3_on_three_nodes();
        for subset in [
            vec![1],
            vec![2],
            vec![3],
            vec![1, 2],
            vec![1, 3],
            vec![2, 3],
            vec![1, 2, 3],
        ] {
            let failed = 3 - subset.len();
            assert_eq!(
                classify_fulltext_coverage(&owners, &responded(&subset)),
                FulltextCoverage::Complete,
                "{subset:?} covers the ring, so {failed} failed node(s) must not fail the search"
            );
        }
    }
}
