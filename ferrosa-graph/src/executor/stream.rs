//! Module: Pull-based (Volcano) row streaming for the graph executor.
//! Correctness: Correct when a `RowStream` yields exactly the rows the
//!   equivalent materializing path would, in the same order, and when
//!   `collect_to_graph_result` reproduces the buffered `GraphResult` byte-for-
//!   byte — so an operator can be converted to streaming without any observable
//!   behavior change.
//! Last revised: 2026-07-25
//! Last changed: New module — increment 1 of the streaming executor
//!   (t_4ce82a3e, specs/streaming-executor-design.md). Scaffold only: the row
//!   stream type + the collect bridge. No operator is streaming yet.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(n: i64) -> RowVals {
        vec![serde_json::json!(n)]
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
