//! Making a replica-side full-text search say what it did and how long it took.
//!
//! A `fts_match` that misses its deadline currently leaves NO trace on the
//! replica. The coordinator reports `net: timeout: Bulk lane timeout` and the
//! replica logs nothing at all, so "the handler was slow" and "the request was
//! delayed in transport" are indistinguishable from the outside. That is what
//! kept the live outage unexplained through several wrong hypotheses.
//!
//! Two decisions live here, both pure so they can be tested without a cluster:
//!
//! 1. [`classify_search_duration`] — how loudly to report one completed search.
//!    A line per request would bury the one that mattered, so ordinary searches
//!    stay quiet and only the slow ones speak (`skills/rules/safety.md`:
//!    report the edges, not the events).
//!
//! 2. [`plan_for_query`] — whether the query will STREAM postings off the
//!    sidecar with a bounded working set, or READ THE WHOLE SIDECAR into memory
//!    and deserialize it.
//!
//! The second is the one that matters for diagnosis. `fulltext_search` streams
//! only `FtsQuery::Term`; every other shape reads the whole file. On the live
//! cluster `idx_entity_context_snippet_fts` is 1,097 MB across 49 sidecars, so a
//! plain multi-word query — which parses to `MultiTerm`, not `Term` — reads and
//! deserializes that much per replica, per query. Logging the plan alongside the
//! duration turns "it was slow" into "it was slow BECAUSE it materialized", and
//! once the streaming fix lands the same line proves the plan flipped.

use std::time::Duration;

use ferrosa_index::fulltext::query::FtsQuery;

/// How loudly one completed search should be reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchReport {
    /// Ordinary. Debug only — a line per search would bury the slow ones.
    Quiet,
    /// Slow enough to be worth a warning, but the caller is probably still
    /// waiting.
    Slow,
    /// Past the caller's deadline: by the time this finished, the coordinator
    /// had already given up and the work was wasted. Errors, because a replica
    /// that silently burns a minute per query is the failure being chased.
    TooLate,
}

/// Classify one search's elapsed time.
///
/// `deadline` is the caller's timeout (the Bulk lane's, for a remote request).
/// A search that ran past it produced an answer nobody was waiting for.
pub fn classify_search_duration(
    elapsed: Duration,
    slow_after: Duration,
    deadline: Duration,
) -> SearchReport {
    // A misconfigured pair (deadline below the slow threshold) must not make a
    // very slow search look quiet, so check the harsher condition first.
    if elapsed >= deadline {
        SearchReport::TooLate
    } else if elapsed >= slow_after {
        SearchReport::Slow
    } else {
        SearchReport::Quiet
    }
}

/// How a query will be executed against one sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FulltextPlan {
    /// Postings are streamed off the file with a bounded working set. The
    /// sidecar is never read whole.
    StreamedPostings,
    /// The entire sidecar is read into memory and deserialized before the query
    /// runs. Cost is the size of the index, not the size of the answer.
    ReadWholeSidecar,
}

impl FulltextPlan {
    /// Whether this plan's cost scales with the INDEX rather than the result.
    pub fn reads_whole_index(self) -> bool {
        matches!(self, Self::ReadWholeSidecar)
    }
}

/// Which plan `fulltext_search` will take for `query`.
///
/// Mirrors the branch in `StorageEngine::fulltext_search`: only a bare
/// single-term query streams today. This exists so the replica can SAY which
/// path it took, rather than leaving an operator to infer it from the query
/// text.
pub fn plan_for_query(query: &FtsQuery) -> FulltextPlan {
    match query {
        FtsQuery::Term(_) => FulltextPlan::StreamedPostings,
        _ => FulltextPlan::ReadWholeSidecar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOW: Duration = Duration::from_secs(1);
    const DEADLINE: Duration = Duration::from_secs(60);

    // ---- classify_search_duration ----------------------------------------

    #[test]
    fn a_fast_search_stays_quiet() {
        assert_eq!(
            classify_search_duration(Duration::from_millis(12), SLOW, DEADLINE),
            SearchReport::Quiet
        );
    }

    #[test]
    fn a_search_past_the_slow_threshold_warns() {
        assert_eq!(
            classify_search_duration(Duration::from_secs(5), SLOW, DEADLINE),
            SearchReport::Slow
        );
    }

    /// The live shape: the replica finished, but only after the coordinator's
    /// 60s Bulk-lane timeout had already fired. The answer went nowhere.
    #[test]
    fn a_search_past_the_callers_deadline_is_too_late() {
        assert_eq!(
            classify_search_duration(Duration::from_secs(61), SLOW, DEADLINE),
            SearchReport::TooLate
        );
    }

    /// Thresholds are inclusive, so a search landing exactly on one does not
    /// fall into the quieter bucket and disappear.
    #[test]
    fn the_thresholds_are_inclusive() {
        assert_eq!(
            classify_search_duration(SLOW, SLOW, DEADLINE),
            SearchReport::Slow
        );
        assert_eq!(
            classify_search_duration(DEADLINE, SLOW, DEADLINE),
            SearchReport::TooLate
        );
    }

    /// A misconfigured pair must not let a very slow search report as quiet.
    /// The harsher verdict wins.
    #[test]
    fn a_deadline_below_the_slow_threshold_still_reports_too_late() {
        let deadline = Duration::from_millis(100);
        let slow_after = Duration::from_secs(10);
        assert_eq!(
            classify_search_duration(Duration::from_secs(30), slow_after, deadline),
            SearchReport::TooLate
        );
    }

    /// Zero elapsed is quiet, not an edge that fires every time.
    #[test]
    fn a_zero_duration_search_is_quiet() {
        assert_eq!(
            classify_search_duration(Duration::ZERO, SLOW, DEADLINE),
            SearchReport::Quiet
        );
    }

    /// Monotonic: a slower search is never reported more quietly than a faster
    /// one. Guards against a future threshold change inverting the buckets.
    #[test]
    fn a_slower_search_is_never_reported_more_quietly() {
        let severity = |r| match r {
            SearchReport::Quiet => 0,
            SearchReport::Slow => 1,
            SearchReport::TooLate => 2,
        };
        let mut previous = 0;
        for ms in [0u64, 500, 999, 1_000, 5_000, 59_999, 60_000, 120_000] {
            let got = severity(classify_search_duration(
                Duration::from_millis(ms),
                SLOW,
                DEADLINE,
            ));
            assert!(
                got >= previous,
                "{ms}ms reported more quietly than the step before it"
            );
            previous = got;
        }
    }

    // ---- plan_for_query --------------------------------------------------

    /// The one shape that streams today.
    #[test]
    fn a_single_term_query_streams_postings() {
        let plan = plan_for_query(&FtsQuery::Term("prevote".into()));
        assert_eq!(plan, FulltextPlan::StreamedPostings);
        assert!(!plan.reads_whole_index());
    }

    /// The live shape. A plain multi-word query parses to `MultiTerm`, which
    /// does NOT stream — it reads the whole sidecar. On the live cluster that
    /// is 1,097 MB across 49 files for `idx_entity_context_snippet_fts`, per
    /// replica, per query.
    #[test]
    fn a_plain_multi_word_query_reads_the_whole_sidecar() {
        let plan = plan_for_query(&FtsQuery::MultiTerm(vec![
            "prevote".into(),
            "transport".into(),
        ]));
        assert_eq!(
            plan,
            FulltextPlan::ReadWholeSidecar,
            "MultiTerm is the shape ferrosa-memory sends and it does not stream"
        );
        assert!(plan.reads_whole_index());
    }

    /// Every remaining shape also materializes. Pinned individually so that
    /// when one of them is moved onto the streaming path, this test fails and
    /// has to be updated deliberately rather than drifting.
    #[test]
    fn every_non_single_term_shape_reads_the_whole_sidecar() {
        let shapes = [
            FtsQuery::Phrase(vec!["bulk".into(), "lane".into()]),
            FtsQuery::MultiTerm(vec!["a".into(), "b".into()]),
            FtsQuery::Prefix("prev".into()),
            FtsQuery::And(
                Box::new(FtsQuery::Term("a".into())),
                Box::new(FtsQuery::Term("b".into())),
            ),
            FtsQuery::Or(
                Box::new(FtsQuery::Term("a".into())),
                Box::new(FtsQuery::Term("b".into())),
            ),
            FtsQuery::Not(Box::new(FtsQuery::Term("a".into()))),
        ];
        for shape in shapes {
            assert_eq!(
                plan_for_query(&shape),
                FulltextPlan::ReadWholeSidecar,
                "{shape:?} should be reported as materializing until it is moved to streaming"
            );
        }
    }
}
