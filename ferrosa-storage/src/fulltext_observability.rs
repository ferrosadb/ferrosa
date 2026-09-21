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
//! The second is the one that matters for diagnosis, and it is not merely
//! descriptive: `fulltext_search` MATCHES on this plan to choose its path, so
//! the logged plan is by construction the plan that ran.
//!
//! It also records how much is still materializing. Single terms and plain
//! multi-word conjunctions now stream their postings; phrase, prefix, explicit
//! AND/OR and NOT still read the whole sidecar. On the live cluster
//! `idx_entity_context_snippet_fts` is 1,097 MB across 49 sidecars, so a shape
//! still on `ReadWholeSidecar` reads and deserializes that much per replica per
//! query — which is what blew the coordinator's 60s deadline while the node sat
//! at 0% CPU, blocked on I/O. Logging the plan beside the duration turns "it was
//! slow" into "it was slow BECAUSE it materialized".

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
///
/// This is the SINGLE source of truth for that decision: `fulltext_search`
/// matches on this value to choose its path, and the replica handler logs the
/// same value. An earlier version mirrored the engine's `match` here for
/// logging, which drifted silently the moment the engine changed — the log then
/// reported `ReadWholeSidecar` for a query that had just been moved onto
/// streaming, and the test pinning the mirror still passed. Carrying the terms
/// makes the plan executable, so a mirror cannot exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulltextPlan {
    /// One term: stream its postings with a bounded top-k working set.
    StreamSingleTerm(String),
    /// A conjunction: stream each term's postings and intersect. The limit is
    /// applied to the intersection, never per term.
    StreamConjunction(Vec<String>),
    /// No streaming path yet: the entire sidecar is read into memory and
    /// deserialized before the query runs. Cost is the size of the index, not
    /// the size of the answer.
    ReadWholeSidecar,
}

impl FulltextPlan {
    /// Whether this plan's cost scales with the INDEX rather than the result.
    pub fn reads_whole_index(&self) -> bool {
        matches!(self, Self::ReadWholeSidecar)
    }

    /// A short, stable label for logs — the variant without its payload, so a
    /// log line does not carry query text.
    pub fn label(&self) -> &'static str {
        match self {
            Self::StreamSingleTerm(_) => "StreamSingleTerm",
            Self::StreamConjunction(_) => "StreamConjunction",
            Self::ReadWholeSidecar => "ReadWholeSidecar",
        }
    }
}

/// Which plan `fulltext_search` will take for `query`.
pub fn plan_for_query(query: &FtsQuery) -> FulltextPlan {
    match query {
        FtsQuery::Term(term) => FulltextPlan::StreamSingleTerm(term.clone()),
        FtsQuery::MultiTerm(terms) => FulltextPlan::StreamConjunction(terms.clone()),
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

    /// A single term streams its postings.
    #[test]
    fn a_single_term_query_streams_postings() {
        let plan = plan_for_query(&FtsQuery::Term("prevote".into()));
        assert_eq!(plan, FulltextPlan::StreamSingleTerm("prevote".into()));
        assert!(!plan.reads_whole_index());
        assert_eq!(plan.label(), "StreamSingleTerm");
    }

    /// The live shape. A plain multi-word query parses to `MultiTerm`, and it
    /// now streams each term and intersects instead of reading the whole
    /// sidecar — which on the live cluster was ~1.1 GB per replica per query.
    #[test]
    fn a_plain_multi_word_query_streams_a_conjunction() {
        let plan = plan_for_query(&FtsQuery::MultiTerm(vec![
            "prevote".into(),
            "transport".into(),
        ]));
        assert_eq!(
            plan,
            FulltextPlan::StreamConjunction(vec!["prevote".into(), "transport".into()]),
            "MultiTerm is the shape a search client sends and it must stream"
        );
        assert!(!plan.reads_whole_index());
    }

    /// The plan carries the terms, so it can be EXECUTED rather than merely
    /// described. That is what stops a logging-only mirror of the engine's
    /// branch drifting out of step with the engine.
    #[test]
    fn a_streaming_plan_carries_the_terms_it_will_scan() {
        match plan_for_query(&FtsQuery::MultiTerm(vec!["a".into(), "b".into()])) {
            FulltextPlan::StreamConjunction(terms) => assert_eq!(terms, vec!["a", "b"]),
            other => panic!("expected StreamConjunction, got {other:?}"),
        }
    }

    /// Shapes with no streaming path yet. Pinned individually so moving one
    /// onto streaming has to update this test deliberately rather than drift.
    #[test]
    fn shapes_without_a_streaming_path_read_the_whole_sidecar() {
        let shapes = [
            FtsQuery::Phrase(vec!["bulk".into(), "lane".into()]),
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
            let plan = plan_for_query(&shape);
            assert_eq!(
                plan,
                FulltextPlan::ReadWholeSidecar,
                "{shape:?} still materializes; update this deliberately when it streams"
            );
            assert!(plan.reads_whole_index());
        }
    }

    /// The label never carries query text into a log line.
    #[test]
    fn the_log_label_carries_no_query_text() {
        let plan = plan_for_query(&FtsQuery::MultiTerm(vec!["secret-token".into()]));
        assert!(!plan.label().contains("secret"));
    }
}
