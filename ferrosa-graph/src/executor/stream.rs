//! Module: Pull-based (Volcano) row streaming for the graph executor.
//! Correctness: Correct when a `RowStream` yields exactly the rows the
//!   equivalent materializing path would, in the same order, and when
//!   `collect_to_graph_result` reproduces the buffered `GraphResult` byte-for-
//!   byte — so an operator can be converted to streaming without any observable
//!   behavior change.
//! Last revised: 2026-07-25
//! Last changed: Added `chain_streams` — the `UNION ALL` concatenation operator
//!   used by `expand::execute_streaming` (t_4ce82a3e,
//!   specs/streaming-executor-design.md increment 2). The row stream type and
//!   the collect bridges landed in increment 1.
//!
//! # Why
//!
//! The executor materializes the full result set (and full per-hop fan-out)
//! into `Vec`s and then truncates for ORDER BY / DISTINCT / LIMIT — the OOM
//! risk for unbounded / high-degree-hub queries. The replacement is a pull-based
//! model where each operator is a lazy stream: `Limit(k)` becomes `take(k)`, so
//! dropping the downstream stream stops all upstream work (the LIMIT
//! short-circuit) for every operator, not just the last hop.
//!
//! # Migration contract
//!
//! Operators are converted one at a time. Each converted operator produces a
//! [`RowStream`]; callers that still need a buffered result use
//! [`collect_to_graph_result`], which is behavior-identical to the old path.
//! That keeps every increment green against the existing integration suite.

use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

use super::result::{GraphResult, QueryStats};
use crate::error::Result;

/// One projected result row: the RETURN items' values, in column order.
pub type RowVals = Vec<serde_json::Value>;

/// A pull-based row source.
///
/// `next()` yields the next row, `None` at end of stream. Dropping the stream
/// stops upstream work — this is what makes `Limit` a true short-circuit rather
/// than a post-hoc truncation.
///
/// Boxed + `Send` so operators compose into a tree across await points on the
/// multi-threaded runtime. The `'a` lifetime lets an operator BORROW its inputs
/// (the read context, the hop, the current bindings) instead of cloning them per
/// row — the property that avoids both the per-neighbor data clones and the
/// `'static` requirement that blocked concurrent hydration (see the design's
/// §3: inline-driven `FuturesUnordered`, not `buffer_unordered`).
pub type RowStream<'a> = Pin<Box<dyn Stream<Item = Result<RowVals>> + Send + 'a>>;

/// Drain a [`RowStream`] into the buffered [`GraphResult`] the current callers
/// expect.
///
/// This is the bridge that keeps each streaming increment behavior-preserving:
/// a converted operator streams, and any caller not yet converted collects here.
/// It is also the honest boundary — a `collect` is a materialization, so every
/// remaining use marks a caller still to convert (the last of them disappears in
/// the transport increment, where rows go straight to HTTP/Bolt).
///
/// `max_rows` mirrors the executor's `max_result_rows` cap: collection stops
/// once that many rows are buffered, exactly like the materializing loop's
/// `if rows.len() >= config.max_result_rows { break }`.
pub async fn collect_to_graph_result(
    columns: Vec<String>,
    mut rows_stream: RowStream<'_>,
    stats: QueryStats,
    max_rows: usize,
) -> Result<GraphResult> {
    let mut rows = Vec::new();
    while let Some(row) = rows_stream.next().await {
        if rows.len() >= max_rows {
            break;
        }
        rows.push(row?);
    }
    Ok(GraphResult {
        columns,
        rows,
        stats,
    })
}

/// Drain a [`RowStream`] into its rows.
///
/// The un-wrapped half of [`collect_to_graph_result`], for callers that build
/// the `GraphResult` themselves. The stream's own `take`/`Limit` is what bounds
/// this — there is no separate cap here, so a caller relying on a row cap must
/// express it in the stream (e.g. `.take(limit)`), which is the point: the limit
/// stops upstream production instead of truncating a materialized Vec.
pub async fn collect_rows(mut rows_stream: RowStream<'_>) -> Result<Vec<RowVals>> {
    let mut rows = Vec::new();
    while let Some(row) = rows_stream.next().await {
        rows.push(row?);
    }
    Ok(rows)
}

/// Build a [`RowStream`] from an already-materialized set of rows.
///
/// The adapter used while converting callers: a not-yet-streaming operator can
/// still hand its output to a streaming consumer. Every use is a materialization
/// that a later increment removes.
pub fn stream_from_rows<'a>(rows: Vec<RowVals>) -> RowStream<'a> {
    Box::pin(futures::stream::iter(rows.into_iter().map(Ok)))
}

/// Concatenate row streams end to end, in order.
///
/// This is `UNION ALL`: arm order is result order, duplicates are kept. Unlike
/// the materializing form (`rows.extend(arm_rows)` per arm) it never builds the
/// concatenated `Vec`, and it never polls a later arm until every earlier arm is
/// drained — so a downstream `take(k)` stops at whichever arm supplies row `k`.
///
/// `UNION` *without* `ALL` is deliberately NOT this function: its `HashSet`
/// dedup spans the whole concatenation, which is a pipeline breaker.
pub fn chain_streams<'a>(streams: Vec<RowStream<'a>>) -> RowStream<'a> {
    streams.into_iter().fold(
        Box::pin(futures::stream::empty()) as RowStream<'a>,
        |acc, next| Box::pin(acc.chain(next)) as RowStream<'a>,
    )
}

/// Streaming `DISTINCT`: emit each row the first time it is seen.
///
/// # Behavior change (deliberate, owner-approved — t_4ce82a3e)
///
/// The buffered form was `rows.sort_by(|a, b| format!("{a:?}").cmp(..))` then
/// `dedup()`, so `RETURN DISTINCT` with no `ORDER BY` returned rows in **string-
/// repr sorted order**. This returns them in **first-seen (expansion) order**.
/// The SET of rows is identical; the ORDER is not. Callers that need a
/// particular order must say `ORDER BY`.
///
/// # Memory
///
/// The `HashSet` of seen rows is **unbounded** — it grows with the number of
/// DISTINCT rows, exactly as the buffered `Vec` did. This is a latency and
/// ordering change, NOT a memory fix: a high-cardinality `DISTINCT` can still
/// exhaust memory. Bounding it (spill or approximate dedup) is separate work.
pub fn dedup_stream<'a>(rows: RowStream<'a>) -> RowStream<'a> {
    let mut seen = std::collections::HashSet::new();
    Box::pin(rows.filter(move |item| {
        // `Err` items always pass through: dropping them would turn a failed
        // query into a silently short successful one.
        let keep = match item {
            Ok(row) => seen.insert(serde_json::to_string(row).unwrap_or_default()),
            Err(_) => true,
        };
        futures::future::ready(keep)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(n: i64) -> RowVals {
        vec![serde_json::json!(n)]
    }

    fn text_row(s: &str) -> RowVals {
        vec![serde_json::json!(s)]
    }

    /// DISTINCT is FIRST-SEEN order, not sorted order.
    ///
    /// This pins the deliberate behavior change (owner-approved, t_4ce82a3e):
    /// the buffered form was `rows.sort_by(|a, b| format!("{a:?}").cmp(..))`
    /// followed by `dedup()`, so a `RETURN DISTINCT` with no `ORDER BY` came
    /// back in string-repr sorted order. Streaming dedup emits each row the
    /// first time it is seen, which is expansion order.
    #[tokio::test]
    async fn dedup_stream_emits_first_seen_order_not_sorted_order() {
        let source = stream_from_rows(vec![
            text_row("Cara"),
            text_row("Alice"),
            text_row("Cara"),
            text_row("Bob"),
            text_row("Alice"),
        ]);
        let deduped = collect_rows(dedup_stream(source)).await.unwrap();

        assert_eq!(
            deduped,
            vec![text_row("Cara"), text_row("Alice"), text_row("Bob")],
            "DISTINCT must emit rows in first-seen order"
        );
        // Spelled out so the change cannot be reverted by accident: the OLD
        // buffered behavior returned this same set sorted by string repr.
        let old_sorted_behavior = vec![text_row("Alice"), text_row("Bob"), text_row("Cara")];
        assert_ne!(
            deduped, old_sorted_behavior,
            "first-seen order must be observably different from the old sorted order"
        );
    }

    #[tokio::test]
    async fn dedup_stream_compares_whole_rows_not_first_column() {
        let source = stream_from_rows(vec![
            vec![serde_json::json!(1), serde_json::json!("a")],
            vec![serde_json::json!(1), serde_json::json!("b")],
            vec![serde_json::json!(1), serde_json::json!("a")],
        ]);
        assert_eq!(
            collect_rows(dedup_stream(source)).await.unwrap(),
            vec![
                vec![serde_json::json!(1), serde_json::json!("a")],
                vec![serde_json::json!(1), serde_json::json!("b")],
            ],
            "DISTINCT dedups whole projected rows"
        );
    }

    /// Dedup must not swallow an upstream failure — a filtered stream that
    /// dropped `Err` items would turn a failed query into a short success.
    #[tokio::test]
    async fn dedup_stream_propagates_upstream_errors() {
        let source: RowStream<'_> = Box::pin(futures::stream::iter(vec![
            Ok(row(1)),
            Err(crate::error::GraphError::Internal("boom".to_string())),
        ]));
        let err = collect_rows(dedup_stream(source)).await.unwrap_err();
        assert!(matches!(err, crate::error::GraphError::Internal(ref m) if m == "boom"));
    }

    /// Dedup is a streaming stage, not a pipeline breaker: it emits row 1
    /// without pulling the rest of an unbounded upstream.
    #[tokio::test]
    async fn dedup_stream_does_not_drain_upstream_before_emitting() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let produced = Arc::new(AtomicUsize::new(0));
        let counter = produced.clone();
        let source: RowStream<'_> = Box::pin(futures::stream::repeat_with(move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            Ok(row(n as i64))
        }));

        let limited: RowStream<'_> = Box::pin(dedup_stream(source).take(2));
        assert_eq!(
            collect_rows(limited).await.unwrap(),
            vec![row(0), row(1)],
            "distinct rows pass straight through"
        );
        assert_eq!(
            produced.load(Ordering::SeqCst),
            2,
            "dedup must not read past the rows the downstream pulled"
        );
    }

    #[tokio::test]
    async fn chain_streams_concatenates_in_arm_order() {
        // `UNION ALL` is concatenation: arm order is result order.
        let chained = chain_streams(vec![
            stream_from_rows(vec![row(1), row(2)]),
            stream_from_rows(vec![]),
            stream_from_rows(vec![row(3)]),
        ]);
        assert_eq!(
            collect_rows(chained).await.unwrap(),
            vec![row(1), row(2), row(3)]
        );
    }

    #[tokio::test]
    async fn chain_streams_of_nothing_is_empty() {
        assert!(collect_rows(chain_streams(vec![]))
            .await
            .unwrap()
            .is_empty());
    }

    /// The incrementality property for the converted `UNION ALL`: pulling `k`
    /// rows pulls them from the earliest arms only — a later arm is never
    /// polled. Same shape as `limit_short_circuits_upstream_production`: assert
    /// on how many items each source actually produced.
    #[tokio::test]
    async fn chain_streams_does_not_pull_later_arms_early() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));

        let c1 = first.clone();
        // An "infinite" first arm: a materializing UNION could never terminate.
        let arm1: RowStream<'_> = Box::pin(futures::stream::repeat_with(move || {
            c1.fetch_add(1, Ordering::SeqCst);
            Ok(row(1))
        }));
        let c2 = second.clone();
        let arm2: RowStream<'_> = Box::pin(futures::stream::repeat_with(move || {
            c2.fetch_add(1, Ordering::SeqCst);
            Ok(row(2))
        }));

        let limited: RowStream<'_> = Box::pin(chain_streams(vec![arm1, arm2]).take(3));
        assert_eq!(collect_rows(limited).await.unwrap(), vec![row(1); 3]);

        assert_eq!(
            first.load(Ordering::SeqCst),
            3,
            "the first arm must produce only the rows actually pulled"
        );
        assert_eq!(
            second.load(Ordering::SeqCst),
            0,
            "a later UNION ALL arm must not be polled before the earlier arms are drained"
        );
    }

    #[tokio::test]
    async fn collect_reproduces_the_buffered_result() {
        let rows = vec![row(1), row(2), row(3)];
        let result = collect_to_graph_result(
            vec!["n".to_string()],
            stream_from_rows(rows.clone()),
            QueryStats::default(),
            usize::MAX,
        )
        .await
        .unwrap();
        assert_eq!(result.columns, vec!["n".to_string()]);
        assert_eq!(result.rows, rows, "collect must preserve rows and order");
    }

    #[tokio::test]
    async fn collect_honors_the_max_rows_cap() {
        // Mirrors the materializing loop's `max_result_rows` break.
        let result = collect_to_graph_result(
            vec!["n".to_string()],
            stream_from_rows(vec![row(1), row(2), row(3), row(4)]),
            QueryStats::default(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(result.rows, vec![row(1), row(2)]);
    }

    /// The property the whole refactor rests on: `take(k)` stops the upstream
    /// stream — no work happens past the limit. Asserted by counting how many
    /// items the source actually produced.
    #[tokio::test]
    async fn limit_short_circuits_upstream_production() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let produced = Arc::new(AtomicUsize::new(0));
        let counter = produced.clone();
        // An "infinite" upstream: if LIMIT did not short-circuit, this would
        // never terminate (the materialize-then-truncate model cannot do this).
        let source = futures::stream::repeat_with(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(row(7))
        });
        let limited: RowStream<'_> = Box::pin(source.take(3));

        let result = collect_to_graph_result(
            vec!["n".to_string()],
            limited,
            QueryStats::default(),
            usize::MAX,
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 3, "LIMIT yields exactly k rows");
        assert_eq!(
            produced.load(Ordering::SeqCst),
            3,
            "upstream must produce only the k rows pulled — the short-circuit"
        );
    }
}
