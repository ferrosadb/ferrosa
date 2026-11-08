//! Execute a compiled [`Query`] as a `'static` row stream.
//!
//! Pipeline: scan(FROM) → nested-loop join per JOIN → WHERE → project (or
//! pre-project → hash aggregate → HAVING → project) → DISTINCT → sort → strip
//! sort-only columns → LIMIT/OFFSET. Every stage streams except the blocking
//! ones (join's right side, aggregate, DISTINCT, sort), which spill past the
//! context's threshold, so memory stays bounded however large the input.
//!
//! **Rows move.** A row is lent to subqueries by moving it into an `Arc<Frame>`
//! and moving it back out once the expression is done (`Arc::try_unwrap`); no
//! row is copied for that. A join copies the left row's values once per match,
//! because each match is a distinct output row.
//!
//! **Errors are fenced.** The streams carry [`SpillError`], the type the
//! spilling operators already propagate. An expression error is parked in the
//! query's [`Fence`] and a placeholder `SpillError` flows downstream, through any
//! sort or aggregate, to the cursor, which reports the parked [`ExecError`]
//! (with its SQLSTATE) instead of the placeholder.

use std::sync::{Arc, Mutex};

use super::eval::{eval, internal, truth, Env};
use super::{CExpr, Mode, Query, Source, SourceTable};
use crate::ast::JoinKind;
use crate::exec::{
    dedup, fallible, hash_aggregate, limit_offset, seq_scan, sort, AggregateError, TryRowStream,
};
use crate::plan::ExecError;
use crate::spill::{ReplayBuffer, ReplayIter, SeqRow, SpillCtx, SpillError};
use crate::types::{Row, Value};

/// A row lent to the subqueries of the expression evaluating over it.
#[derive(Debug)]
pub(crate) struct Frame {
    pub(crate) row: Row,
    pub(crate) parent: Option<Arc<Frame>>,
}

/// Per-statement runtime state shared by every stage (and every subquery).
#[derive(Debug)]
pub(crate) struct Rt {
    pub(crate) params: Arc<[Value]>,
    pub(crate) ctx: SpillCtx,
    pub(crate) fence: Arc<Fence>,
    /// `now()`: the statement's start, one value for the whole statement.
    pub(crate) now_micros: i64,
    /// The value of each uncorrelated subquery node, computed once. Keyed by the
    /// node's address in the compiled query, which the statement holds alive;
    /// bounded by the number of subquery nodes in the statement.
    pub(crate) memo: Mutex<std::collections::HashMap<usize, Value>>,
}

/// Where an expression error waits while its placeholder flows downstream.
#[derive(Debug, Default)]
pub(crate) struct Fence {
    slot: Mutex<Option<ExecError>>,
}

impl Fence {
    /// Park `err` (the first one wins) and return the placeholder to stream.
    pub(crate) fn park(&self, err: ExecError) -> SpillError {
        let message = err.to_string();
        let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
        slot.get_or_insert(err);
        SpillError::new("expression", message)
    }

    /// The error a stream failure stands for: the parked expression error if
    /// there is one, else the spill failure itself.
    pub(crate) fn lift(&self, err: SpillError) -> ExecError {
        let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
        slot.take().unwrap_or(ExecError::Spill(err))
    }
}

/// Open the top-level statement.
pub(crate) fn open_top(
    query: &Arc<Query>,
    params: &[Value],
    ctx: &SpillCtx,
) -> Result<(TryRowStream<'static>, Arc<Fence>), ExecError> {
    if params.len() < query.params_needed {
        return Err(ExecError::MissingParameter(query.params_needed));
    }
    let fence = Arc::new(Fence::default());
    let rt = Arc::new(Rt {
        params: params.into(),
        ctx: ctx.clone(),
        fence: Arc::clone(&fence),
        now_micros: now_micros(),
        memo: Mutex::new(std::collections::HashMap::new()),
    });
    let rows = open(query, &rt, None)?;
    Ok((rows, fence))
}

fn now_micros() -> i64 {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
}

/// Run a subquery for the row `env` is evaluating, collecting at most `cap` rows.
pub(crate) fn subquery_rows(
    q: &Arc<Query>,
    env: &Env<'_>,
    cap: usize,
) -> Result<Vec<Row>, ExecError> {
    // An uncorrelated subquery reads no outer row, so it is lent none.
    let outer = if q.correlated {
        let frame = env
            .frame
            .ok_or_else(|| internal("a subquery ran without its outer row".into()))?;
        Some(Arc::clone(frame))
    } else {
        None
    };
    let rows = open(q, env.rt, outer)?;
    let mut out = Vec::new();
    for row in rows.take(cap) {
        out.push(row.map_err(|e| env.rt.fence.lift(e))?);
    }
    Ok(out)
}

/// Build `q`'s row stream. `outer` is the enclosing query's current row.
fn open(
    q: &Arc<Query>,
    rt: &Arc<Rt>,
    outer: Option<Arc<Frame>>,
) -> Result<TryRowStream<'static>, ExecError> {
    let mut rows = scan(&q.sources[0], rt)?;
    for j in 1..q.sources.len() {
        rows = nested_loop(rows, q, j, rt, outer.clone())?;
    }
    if q.filter.is_some() {
        rows = keep(rows, q, rt, outer.clone(), Pred::Where);
    }
    match &q.mode {
        Mode::Plain { .. } => rows = project(rows, q, rt, outer, Proj::Plain),
        Mode::Aggregate {
            groups,
            aggs,
            having,
            ..
        } => {
            let pre = project(rows, q, rt, outer.clone(), Proj::Pre);
            let group_cols: Vec<usize> = (0..*groups).collect();
            rows = hash_aggregate(pre, &group_cols, aggs, &rt.ctx).map_err(|e| match e {
                AggregateError::Spill(s) => rt.fence.lift(s),
                AggregateError::OutOfRange(m) => ExecError::AggregateOutOfRange(m),
            })?;
            if having.is_some() {
                rows = keep(rows, q, rt, outer.clone(), Pred::Having);
            }
            rows = project(rows, q, rt, outer, Proj::Post);
        }
    }
    if q.distinct {
        rows = dedup(rows, &rt.ctx).map_err(|e| rt.fence.lift(e))?;
    }
    if !q.order.is_empty() {
        rows = sort(rows, &q.order, &rt.ctx).map_err(|e| rt.fence.lift(e))?;
        if q.hidden > 0 {
            let width = q.columns.len();
            rows = Box::new(rows.map(move |r| {
                r.map(|mut row| {
                    row.0.truncate(width);
                    row
                })
            }));
        }
    }
    let offset = match q.offset_param {
        Some(n) => bound_param(rt, n, "OFFSET", "2201X")?,
        None => q.offset,
    }
    .map_or(0, |n| usize::try_from(n).unwrap_or(usize::MAX));
    let limit = match q.limit_param {
        Some(n) => bound_param(rt, n, "LIMIT", "2201W")?,
        None => q.limit,
    }
    .map(|n| usize::try_from(n).unwrap_or(usize::MAX));
    Ok(limit_offset(rows, offset, limit))
}

/// Resolve a `LIMIT $n` / `OFFSET $n` value (`Limit.lean` `resolve`): NULL is no
/// bound, a negative count is an error, never clamped.
fn bound_param(
    rt: &Rt,
    n: usize,
    clause: &str,
    negative_state: &'static str,
) -> Result<Option<u64>, ExecError> {
    match rt.params.get(n - 1) {
        None => Err(ExecError::MissingParameter(n)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int(v)) => u64::try_from(*v)
            .map(Some)
            .map_err(|_| ExecError::eval(negative_state, format!("{clause} must not be negative"))),
        Some(other) => Err(ExecError::eval(
            "22023",
            format!("{clause} must be an integer, not {other:?}"),
        )),
    }
}

fn scan(source: &Source, rt: &Arc<Rt>) -> Result<TryRowStream<'static>, ExecError> {
    match &source.table {
        SourceTable::Table(t) => Ok(fallible(seq_scan(&**t))),
        SourceTable::Cte(q) => open(q, rt, None),
        SourceTable::OneRow => Ok(Box::new(std::iter::once(Ok(Row(Vec::new()))))),
    }
}

/// Evaluate `f` over `row`, lending the row to subqueries when the query needs
/// it, and hand the row back (moved, never copied) with the result.
fn with_row<T>(
    q: &Query,
    row: Row,
    rt: &Arc<Rt>,
    outer: Option<&Arc<Frame>>,
    f: impl FnOnce(&Env<'_>) -> Result<T, ExecError>,
) -> Result<(Row, T), ExecError> {
    if !q.needs_frame {
        let t = f(&Env {
            row: &row,
            frame: None,
            outer,
            rt,
        })?;
        return Ok((row, t));
    }
    let frame = Arc::new(Frame {
        row,
        parent: outer.cloned(),
    });
    let t = f(&Env {
        row: &frame.row,
        frame: Some(&frame),
        outer,
        rt,
    })?;
    // Every subquery stream holding the frame was drained and dropped inside `f`.
    let frame = Arc::try_unwrap(frame)
        .map_err(|_| internal("an outer row outlived its subqueries".into()))?;
    Ok((frame.row, t))
}

#[derive(Clone, Copy)]
enum Pred {
    Where,
    Having,
}

fn predicate(q: &Query, which: Pred) -> Option<&CExpr> {
    match (which, &q.mode) {
        (Pred::Where, _) => q.filter.as_ref(),
        (Pred::Having, Mode::Aggregate { having, .. }) => having.as_ref(),
        (Pred::Having, Mode::Plain { .. }) => None,
    }
}

/// Keep the rows whose condition is TRUE; UNKNOWN and FALSE rows are dropped
/// (`filter_keeps_only_true`).
fn keep(
    rows: TryRowStream<'static>,
    q: &Arc<Query>,
    rt: &Arc<Rt>,
    outer: Option<Arc<Frame>>,
    which: Pred,
) -> TryRowStream<'static> {
    let (q, rt) = (Arc::clone(q), Arc::clone(rt));
    Box::new(rows.filter_map(move |r| {
        let row = match r {
            Ok(row) => row,
            Err(e) => return Some(Err(e)),
        };
        let Some(cond) = predicate(&q, which) else {
            return Some(Ok(row));
        };
        match with_row(&q, row, &rt, outer.as_ref(), |env| truth(&eval(cond, env)?)) {
            Ok((row, Some(true))) => Some(Ok(row)),
            Ok(_) => None,
            Err(e) => Some(Err(rt.fence.park(e))),
        }
    }))
}

#[derive(Clone, Copy)]
enum Proj {
    Plain,
    Pre,
    Post,
}

fn projection(q: &Query, which: Proj) -> &[CExpr] {
    match (which, &q.mode) {
        (Proj::Plain, Mode::Plain { project }) => project,
        (Proj::Pre, Mode::Aggregate { pre, .. }) => pre,
        (Proj::Post, Mode::Aggregate { project, .. }) => project,
        _ => &[],
    }
}

fn project(
    rows: TryRowStream<'static>,
    q: &Arc<Query>,
    rt: &Arc<Rt>,
    outer: Option<Arc<Frame>>,
    which: Proj,
) -> TryRowStream<'static> {
    let (q, rt) = (Arc::clone(q), Arc::clone(rt));
    Box::new(rows.map(move |r| {
        let row = r?;
        let exprs = projection(&q, which);
        let evaluated = with_row(&q, row, &rt, outer.as_ref(), |env| {
            let mut out = Vec::with_capacity(exprs.len());
            for e in exprs {
                out.push(eval(e, env)?);
            }
            Ok(out)
        });
        match evaluated {
            Ok((_, out)) => Ok(Row(out)),
            Err(e) => Err(rt.fence.park(e)),
        }
    }))
}

/// Join source `j` onto the rows so far: its rows are buffered once (spilling
/// past the threshold) and replayed against each left row in order, so the
/// output keeps the left side's order. A LEFT join emits an unmatched left row
/// padded with NULLs.
fn nested_loop(
    left: TryRowStream<'static>,
    q: &Arc<Query>,
    j: usize,
    rt: &Arc<Rt>,
    outer: Option<Arc<Frame>>,
) -> Result<TryRowStream<'static>, ExecError> {
    const LABEL: &str = "nested_loop_join";
    let reservation = rt.ctx.reserve(LABEL).map_err(ExecError::Spill)?;
    let mut right = ReplayBuffer::new(reservation.path(), &rt.ctx, LABEL);
    for (seq, row) in (0u64..).zip(scan(&q.sources[j], rt)?) {
        let row = row.map_err(|e| rt.fence.lift(e))?;
        right
            .push(SeqRow::new(seq, row))
            .map_err(ExecError::Spill)?;
    }
    Ok(Box::new(NestedLoop {
        left,
        right,
        _reservation: reservation,
        q: Arc::clone(q),
        j,
        rt: Arc::clone(rt),
        outer,
        current: None,
    }))
}

struct NestedLoop {
    left: TryRowStream<'static>,
    right: ReplayBuffer,
    /// Owns the directory the buffer may spill into; removed on drop.
    _reservation: ferrosa_storage::TempSortTableReservation,
    q: Arc<Query>,
    j: usize,
    rt: Arc<Rt>,
    outer: Option<Arc<Frame>>,
    /// The left row being joined, its pass over the right side, and whether it
    /// has matched yet.
    current: Option<(Row, ReplayIter, bool)>,
}

impl NestedLoop {
    fn source(&self) -> &Source {
        &self.q.sources[self.j]
    }

    /// Start the next left row's pass, or report the end / an error.
    fn advance_left(&mut self) -> Option<Result<(), SpillError>> {
        match self.left.next()? {
            Err(e) => Some(Err(e)),
            Ok(row) => match self.right.replay() {
                Ok(pass) => {
                    self.current = Some((row, pass, false));
                    Some(Ok(()))
                }
                Err(e) => Some(Err(e)),
            },
        }
    }

    /// The left row's pass ended: a LEFT join pads an unmatched row.
    fn finish_left(&mut self) -> Option<Row> {
        let (row, _, matched) = self.current.take()?;
        if matched || self.source().kind != JoinKind::Left {
            return None;
        }
        let mut values = row.0;
        values.resize(values.len() + self.source().width, Value::Null);
        Some(Row(values))
    }
}

impl Iterator for NestedLoop {
    type Item = Result<Row, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                if let Err(e) = self.advance_left()? {
                    return Some(Err(e));
                }
            }
            let (left, pass, _) = self.current.as_mut().expect("a left row is open");
            let right = match pass.next() {
                None => match self.finish_left() {
                    Some(padded) => return Some(Ok(padded)),
                    None => continue,
                },
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(item)) => item.row,
            };
            let mut values = Vec::with_capacity(left.0.len() + right.0.len());
            values.extend(left.0.iter().cloned());
            values.extend(right.0);
            let on = self.q.sources[self.j].on.as_ref();
            let verdict = match on {
                None => Ok((Row(values), Some(true))),
                Some(on) => with_row(&self.q, Row(values), &self.rt, self.outer.as_ref(), |env| {
                    truth(&eval(on, env)?)
                }),
            };
            match verdict {
                Ok((row, Some(true))) => {
                    if let Some((_, _, matched)) = self.current.as_mut() {
                        *matched = true;
                    }
                    return Some(Ok(row));
                }
                Ok(_) => {}
                Err(e) => return Some(Err(self.rt.fence.park(e))),
            }
        }
    }
}
