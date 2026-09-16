//! Physical operators (Volcano-style, iterator-based).
//!
//! `seq_scan`, `filter` and `project` are *streaming*: they transform one row at
//! a time and hold nothing. `sort`, `hash_aggregate`, `hash_join` and `dedup`
//! are **blocking** — none can emit its first output row before it has consumed
//! its whole input — so pipelining is unavailable to them and they instead
//! SPILL through [`crate::spill`], which reuses the storage engine's bounded
//! external merge sort (forge t_50d99192). NULL semantics follow SQL: a
//! comparison with NULL is UNKNOWN (row excluded), and NULL join keys never
//! match.
//!
//! # What each blocking operator does now
//!
//! | Operator | Was | Is |
//! |---|---|---|
//! | `sort` | `Vec::sort` over the whole input | spilling external merge sort |
//! | `hash_aggregate` | group table up to input size | sort-based groups, one accumulator set resident |
//! | `hash_join` | whole right side in a `HashMap` **and** every output row accumulated | sort-merge join, one key group replayable from disk |
//! | `dedup` | `HashSet` + `Vec` proportional to input | sort-based adjacent dedup |
//!
//! Each sort-based operator restores the in-memory operator's OUTPUT order with
//! a second sort on the arrival tag, so first-seen group order, DISTINCT
//! first-occurrence order and the join's left-input order are all unchanged.
//! Switching to spill changes memory behavior and nothing observable.
//!
//! # Fail loud
//!
//! Every spill/merge I/O error propagates as a [`SpillError`]. A dropped run
//! would silently lose rows, which is strictly worse than the materialization it
//! replaced, so nothing here ever converts an I/O failure into a short result.

use std::cmp::Ordering;

use crate::provider::TableProvider;
use crate::spill::{
    canonical_cmp, Lookahead, ReplayBuffer, SeqRow, SpillCtx, SpillError, SpillSort, SqlOrder,
};
use crate::types::{Row, Value};

/// Sort direction for an `ORDER BY` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// One column of a (possibly multi-key) sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub col: usize,
    pub dir: SortDir,
}

/// Compare two values for `ORDER BY`, honoring Postgres NULL placement:
/// ASC ⇒ NULLS LAST, DESC ⇒ NULLS FIRST. Non-null comparison uses
/// [`Value::sql_cmp`]; a `None` result (type mismatch / UNKNOWN) is treated as
/// `Equal` so the sort stays total and stable.
///
/// `pub(crate)` so [`crate::spill::SqlOrder`] orders spilled rows with the exact
/// comparator the in-memory sort used — that identity is what makes spilling an
/// invisible change.
pub(crate) fn order_cmp(a: &Value, b: &Value, dir: SortDir) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        // ASC ⇒ NULLs sort after non-nulls; DESC ⇒ NULLs sort before.
        (true, false) => match dir {
            SortDir::Asc => Ordering::Greater,
            SortDir::Desc => Ordering::Less,
        },
        (false, true) => match dir {
            SortDir::Asc => Ordering::Less,
            SortDir::Desc => Ordering::Greater,
        },
        (false, false) => {
            let base = a.sql_cmp(b).unwrap_or(Ordering::Equal);
            match dir {
                SortDir::Asc => base,
                SortDir::Desc => base.reverse(),
            }
        }
    }
}

/// A pull-based stream of rows flowing between the streaming operators.
pub type RowStream<'a> = Box<dyn Iterator<Item = Row> + 'a>;

/// A pull-based stream of rows that can fail.
///
/// Blocking operators read and produce this: once an operator can spill, every
/// pull can hit a spill/merge I/O error, and that error has to reach the caller
/// rather than ending the stream early.
pub type TryRowStream<'a> = Box<dyn Iterator<Item = Result<Row, SpillError>> + 'a>;

/// Lift an infallible stream (a scan, filter or projection) into a
/// [`TryRowStream`] so a blocking operator can consume it.
pub fn fallible<'a>(input: RowStream<'a>) -> TryRowStream<'a> {
    Box::new(input.map(Ok))
}

/// Multi-key sort, spilling to disk past the context's byte threshold.
///
/// The sort is stable: [`SqlOrder::Sql`] breaks ties on arrival order, which is
/// what preserves the guarantee `Vec::sort_by` gave for free once the rows are
/// split across run files.
pub fn sort(
    input: TryRowStream<'_>,
    keys: &[SortKey],
    ctx: &SpillCtx,
) -> Result<TryRowStream<'static>, SpillError> {
    let mut sorter = SpillSort::new(ctx, SqlOrder::Sql(keys.to_vec()), "sort")?;
    for row in input {
        sorter.push(row?)?;
    }
    Ok(Box::new(sorter.finish()?.map(|r| r.map(|s| s.row))))
}

/// Apply `OFFSET`/`LIMIT` to a row stream: `offset` rows are skipped; at most
/// `limit` (when `Some`) are then yielded.
///
/// Both are applied lazily, so a `LIMIT` never forces the rest of the stream to
/// be read — and neither bounds what the query *can* return, only what the
/// client asked for. An error is never skipped: `offset` counts rows, so a spill
/// failure inside the skipped prefix still surfaces.
pub fn limit_offset<'a>(
    input: TryRowStream<'a>,
    offset: usize,
    limit: Option<usize>,
) -> TryRowStream<'a> {
    let mut skipped = 0usize;
    let skipped_stream = input.filter_map(move |r| match r {
        Err(e) => Some(Err(e)),
        Ok(_) if skipped < offset => {
            skipped += 1;
            None
        }
        Ok(row) => Some(Ok(row)),
    });
    match limit {
        Some(n) => Box::new(skipped_stream.take(n)),
        None => Box::new(skipped_stream),
    }
}

/// A supported aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// One accumulator, paired with its `(func, arg-column)` definition.
///
/// SUM/AVG track an integer running sum (`int_sum`) and a float running sum
/// (`float_sum`) plus `saw_float`: an Int column sums into `int_sum` and yields
/// `Int`, a Float column sums into `float_sum` and yields `Float`, and a mixed
/// column promotes to `Float`. `numeric_count` is the count of non-NULL numeric
/// values feeding SUM/AVG; AVG divides the total by it.
struct Accumulator {
    count: i64,
    int_sum: i64,
    float_sum: f64,
    saw_float: bool,
    numeric_count: i64,
    extreme: Option<Value>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            count: 0,
            int_sum: 0,
            float_sum: 0.0,
            saw_float: false,
            numeric_count: 0,
            extreme: None,
        }
    }

    /// Accumulate one non-NULL numeric value into the SUM/AVG running totals.
    /// Non-numeric values are skipped (the count is unaffected).
    fn add_numeric(&mut self, v: &Value) {
        match v {
            Value::Int(n) => {
                self.int_sum += *n;
                self.float_sum += *n as f64;
                self.numeric_count += 1;
            }
            Value::Float(f) => {
                self.float_sum += f.0;
                self.saw_float = true;
                self.numeric_count += 1;
            }
            _ => {}
        }
    }

    fn update(&mut self, func: AggFunc, arg: Option<usize>, row: &Row) {
        match func {
            AggFunc::Count => match arg {
                // COUNT(*) counts every row; COUNT(col) only non-NULL.
                None => self.count += 1,
                Some(c) => {
                    if !row.0[c].is_null() {
                        self.count += 1;
                    }
                }
            },
            AggFunc::Sum | AggFunc::Avg => {
                if let Some(c) = arg {
                    self.add_numeric(&row.0[c]);
                }
            }
            AggFunc::Min | AggFunc::Max => {
                if let Some(c) = arg {
                    let v = &row.0[c];
                    if v.is_null() {
                        return;
                    }
                    let take = match &self.extreme {
                        None => true,
                        Some(cur) => {
                            let ord = v.sql_cmp(cur).unwrap_or(Ordering::Equal);
                            match func {
                                AggFunc::Min => ord == Ordering::Less,
                                AggFunc::Max => ord == Ordering::Greater,
                                _ => unreachable!(),
                            }
                        }
                    };
                    if take {
                        self.extreme = Some(v.clone());
                    }
                }
            }
        }
    }

    fn finish(&self, func: AggFunc) -> Value {
        match func {
            AggFunc::Count => Value::Int(self.count),
            // Postgres: SUM over no non-null rows is NULL. A Float column (or any
            // float seen) yields Float; a pure-Int column yields Int.
            AggFunc::Sum => {
                if self.numeric_count == 0 {
                    Value::Null
                } else if self.saw_float {
                    Value::float(self.float_sum)
                } else {
                    Value::Int(self.int_sum)
                }
            }
            // AVG always yields Float (or NULL over no non-null numeric rows).
            AggFunc::Avg => {
                if self.numeric_count == 0 {
                    Value::Null
                } else {
                    Value::float(self.float_sum / self.numeric_count as f64)
                }
            }
            AggFunc::Min | AggFunc::Max => self.extreme.clone().unwrap_or(Value::Null),
        }
    }
}

/// Build one output row `[group-key values…, agg values…]` from a finished group.
fn finish_group(key: Vec<Value>, accs: &[Accumulator], aggs: &[(AggFunc, Option<usize>)]) -> Row {
    let mut values = key;
    for (acc, (func, _)) in accs.iter().zip(aggs.iter()) {
        values.push(acc.finish(*func));
    }
    Row(values)
}

/// Group rows by `group_cols` and compute each aggregate, spilling past the
/// context's byte threshold. Output row layout is `[group-key values…, agg
/// values…]`.
///
/// # How it stays bounded
///
/// The group table was the problem: one entry per distinct GROUP BY key, so a
/// high-cardinality grouping held state proportional to the whole input. This
/// sorts the input by group key instead and walks it, so **exactly one
/// accumulator set is resident** however many groups there are. The sort spills;
/// the walk does not accumulate.
///
/// Grouping sorts under [`canonical_cmp`], NOT the SQL comparator: `sql_cmp`
/// calls mismatched types UNKNOWN, and folding that to `Equal` would merge
/// `Int(1)` with `Text("1")` into one group. NULL is a distinct group key
/// (unlike join keys), which the canonical order gives directly.
///
/// First-seen group order is preserved by tagging each group with the arrival
/// position of its first row and sorting the finished groups on it.
///
/// With no `group_cols` the whole input is one group; over an empty input that
/// single group still emits exactly one row (COUNT=0, SUM/MIN/MAX=NULL). With
/// `group_cols` and empty input, zero rows are emitted.
pub fn hash_aggregate(
    input: TryRowStream<'_>,
    group_cols: &[usize],
    aggs: &[(AggFunc, Option<usize>)],
    ctx: &SpillCtx,
) -> Result<TryRowStream<'static>, SpillError> {
    const LABEL: &str = "hash_aggregate";

    let mut by_key = SpillSort::new(ctx, SqlOrder::Canonical(Some(group_cols.to_vec())), LABEL)?;
    for row in input {
        by_key.push(row?)?;
    }
    let rows_in = by_key.pushed();
    let sorted = by_key.finish()?;

    // Walk the key-ordered rows, closing each group when the key changes. Only
    // the current group's key and accumulators are resident.
    let mut by_seq = SpillSort::new(ctx, SqlOrder::Seq, LABEL)?;
    let mut current: Option<(Vec<Value>, u64, Vec<Accumulator>)> = None;
    for item in sorted {
        let item = item?;
        let key: Vec<Value> = group_cols.iter().map(|&c| item.row.0[c].clone()).collect();
        let is_same = current.as_ref().is_some_and(|(k, _, _)| *k == key);
        if !is_same {
            if let Some((k, seq, accs)) = current.take() {
                by_seq.push_tagged(SeqRow::new(seq, finish_group(k, &accs, aggs)))?;
            }
            current = Some((
                key,
                item.seq,
                aggs.iter().map(|_| Accumulator::new()).collect(),
            ));
        }
        let (_, _, accs) = current.as_mut().expect("group open");
        for (acc, (func, arg)) in accs.iter_mut().zip(aggs.iter()) {
            acc.update(*func, *arg, &item.row);
        }
    }
    if let Some((k, seq, accs)) = current.take() {
        by_seq.push_tagged(SeqRow::new(seq, finish_group(k, &accs, aggs)))?;
    }

    // Ungrouped aggregate over an empty input: synthesize one all-empty group.
    if group_cols.is_empty() && rows_in == 0 {
        let accs: Vec<Accumulator> = aggs.iter().map(|_| Accumulator::new()).collect();
        by_seq.push_tagged(SeqRow::new(0, finish_group(Vec::new(), &accs, aggs)))?;
    }

    Ok(Box::new(by_seq.finish()?.map(|r| r.map(|s| s.row))))
}

/// Deduplicate rows, preserving first-occurrence order, spilling past the
/// context's byte threshold.
///
/// The `HashSet` of seen rows plus the output `Vec` were both proportional to
/// the input. Sorting under [`canonical_cmp`] puts equal rows adjacent — and
/// because that order breaks ties on arrival position, the FIRST row of each
/// equal run is the first occurrence — so dedup becomes an adjacent comparison
/// against one remembered row. A second sort on the arrival tag restores
/// first-occurrence order.
pub fn dedup(input: TryRowStream<'_>, ctx: &SpillCtx) -> Result<TryRowStream<'static>, SpillError> {
    const LABEL: &str = "dedup";

    let mut by_row = SpillSort::new(ctx, SqlOrder::Canonical(None), LABEL)?;
    for row in input {
        by_row.push(row?)?;
    }
    let sorted = by_row.finish()?;

    // One row is held back rather than copied: `pending` IS the row that will be
    // emitted, so the comparison that decides whether the next row is a
    // duplicate costs no clone.
    let mut by_seq = SpillSort::new(ctx, SqlOrder::Seq, LABEL)?;
    let mut pending: Option<SeqRow> = None;
    for item in sorted {
        let item = item?;
        let duplicate = pending.as_ref().is_some_and(|p| p.row == item.row);
        if !duplicate {
            if let Some(previous) = pending.take() {
                by_seq.push_tagged(previous)?;
            }
            pending = Some(item);
        }
    }
    if let Some(previous) = pending.take() {
        by_seq.push_tagged(previous)?;
    }
    Ok(Box::new(by_seq.finish()?.map(|r| r.map(|s| s.row))))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A single-column comparison predicate (`row[col] <op> value`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    pub col: usize,
    pub op: CmpOp,
    pub value: Value,
}

impl Predicate {
    /// Evaluate against a row. SQL three-valued logic: UNKNOWN → `false`.
    pub fn eval(&self, row: &Row) -> bool {
        match row.0[self.col].sql_cmp(&self.value) {
            None => false, // UNKNOWN (NULL or type mismatch) → excluded
            Some(ord) => match self.op {
                CmpOp::Eq => ord == Ordering::Equal,
                CmpOp::Ne => ord != Ordering::Equal,
                CmpOp::Lt => ord == Ordering::Less,
                CmpOp::Le => ord != Ordering::Greater,
                CmpOp::Gt => ord == Ordering::Greater,
                CmpOp::Ge => ord != Ordering::Less,
            },
        }
    }
}

/// Scan all rows of a table.
pub fn seq_scan(table: &dyn TableProvider) -> RowStream<'_> {
    table.scan()
}

/// Keep rows for which `pred` holds.
pub fn filter<'a>(input: RowStream<'a>, pred: Predicate) -> RowStream<'a> {
    Box::new(input.filter(move |row| pred.eval(row)))
}

/// Project the given columns (in order) out of each row.
pub fn project<'a>(input: RowStream<'a>, cols: Vec<usize>) -> RowStream<'a> {
    Box::new(input.map(move |row| Row(cols.iter().map(|&i| row.0[i].clone()).collect())))
}

/// Keep rows for which `pred` holds, over a fallible stream.
pub fn try_filter<'a>(input: TryRowStream<'a>, pred: Predicate) -> TryRowStream<'a> {
    Box::new(input.filter(move |row| match row {
        Err(_) => true, // never swallow a spill failure
        Ok(row) => pred.eval(row),
    }))
}

/// Project the given columns (in order) out of each row of a fallible stream.
pub fn try_project<'a>(input: TryRowStream<'a>, cols: Vec<usize>) -> TryRowStream<'a> {
    Box::new(input.map(move |row| row.map(|r| Row(cols.iter().map(|&i| r.0[i].clone()).collect()))))
}

/// Inner equi-join: emit `left ++ right` for every pair where
/// `left[left_key] == right[right_key]`. NULL keys never match.
///
/// # How it stays bounded
///
/// This was the worst site in the crate: the whole right stream went into a
/// `HashMap`, and every output row was accumulated, so a skewed key made peak
/// memory `O(left x right)` — quadratic in the input for a single hot key.
///
/// It is now a **sort-merge join**. Both sides are sorted by join key through
/// the spilling sorter (NULL keys dropped up front, since they can never match),
/// then merged. For a matching key the right-hand group goes into a
/// [`ReplayBuffer`] — resident while small, written to a run file inside the
/// query's reservation once it crosses the threshold — and is replayed once per
/// left row of that key. Nothing holds a whole side, and nothing accumulates the
/// output: it is pushed straight into the ordering sort and streamed out.
///
/// Output order is unchanged. The merge visits keys in canonical order, so the
/// pairs are tagged `(left arrival, right arrival)` and a final sort on that tag
/// reproduces exactly the left-input order the `HashMap` build/probe emitted.
pub fn hash_join(
    left: TryRowStream<'_>,
    right: TryRowStream<'_>,
    left_key: usize,
    right_key: usize,
    ctx: &SpillCtx,
) -> Result<TryRowStream<'static>, SpillError> {
    const LABEL: &str = "hash_join";

    // Sort both sides by join key. A NULL key never matches, so those rows are
    // dropped here rather than being sorted and merged for nothing.
    let mut left_sorted = SpillSort::new(ctx, SqlOrder::Canonical(Some(vec![left_key])), LABEL)?;
    for row in left {
        let row = row?;
        if !row.0[left_key].is_null() {
            left_sorted.push(row)?;
        }
    }
    let mut right_sorted = SpillSort::new(ctx, SqlOrder::Canonical(Some(vec![right_key])), LABEL)?;
    for row in right {
        let row = row?;
        if !row.0[right_key].is_null() {
            right_sorted.push(row)?;
        }
    }

    let mut lefts = Lookahead::new(left_sorted.finish()?);
    let mut rights = Lookahead::new(right_sorted.finish()?);

    // One reservation for the per-key group buffer; its Drop removes the
    // directory when this function returns, on every exit path.
    let group_dir = ctx.reserve(LABEL)?;
    let mut group = ReplayBuffer::new(group_dir.path(), ctx, LABEL);
    let mut out = SpillSort::new(ctx, SqlOrder::Seq, LABEL)?;

    // Ends as soon as either side is exhausted: no further pair can exist.
    while let (Some(lk), Some(rk)) = (lefts.peek_key(left_key)?, rights.peek_key(right_key)?) {
        match canonical_cmp(&lk, &rk) {
            Ordering::Less => {
                lefts.next_row()?;
            }
            Ordering::Greater => {
                rights.next_row()?;
            }
            Ordering::Equal => {
                // Gather this key's right-hand group (replayable, spills itself).
                group.clear()?;
                while let Some(k) = rights.peek_key(right_key)? {
                    if canonical_cmp(&k, &rk) != Ordering::Equal {
                        break;
                    }
                    let row = rights.next_row()?.expect("peeked a row");
                    group.push(row)?;
                }
                // Emit the cross product for every left row of the same key.
                while let Some(k) = lefts.peek_key(left_key)? {
                    if canonical_cmp(&k, &rk) != Ordering::Equal {
                        break;
                    }
                    let left_row = lefts.next_row()?.expect("peeked a row");
                    for item in group.replay()? {
                        let right_row = item?;
                        // The left row is reused across the whole group so it is
                        // copied per pair; the right row is owned by this
                        // iteration and moves into the joined row.
                        let mut values = left_row.row.0.clone();
                        values.extend(right_row.row.0);
                        out.push_tagged(SeqRow {
                            seq: left_row.seq,
                            seq2: right_row.seq,
                            row: Row(values),
                        })?;
                    }
                }
            }
        }
    }
    // Free the last group's run file before the output is streamed.
    group.clear()?;

    Ok(Box::new(out.finish()?.map(|r| r.map(|s| s.row))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::InMemoryTable;
    use crate::spill::DirReserver;
    use crate::types::{Column, ColumnType, RelSchema, Row, Value};
    use std::sync::Arc;

    // ---------------------------------------------------------------------
    // Vec-shaped adapters over the streaming operators.
    //
    // These shadow the real `sort` / `hash_aggregate` / `hash_join` / `dedup`
    // for the semantic tests below, which assert WHAT the operators compute and
    // are unchanged by the move to spilling. The threshold is large, so these
    // exercise the in-memory fast path; the spill+merge path and the
    // bounded-peak invariant are covered by `tests/spill_operators.rs`.
    // ---------------------------------------------------------------------

    /// A context whose threshold keeps the operator in memory.
    fn buffered_ctx(dir: &tempfile::TempDir) -> SpillCtx {
        SpillCtx::new(Arc::new(DirReserver::new(dir.path())), 1 << 30)
    }

    fn drain(s: TryRowStream<'_>) -> Vec<Row> {
        s.collect::<Result<Vec<Row>, _>>().expect("no spill error")
    }

    fn from_vec(rows: Vec<Row>) -> TryRowStream<'static> {
        Box::new(rows.into_iter().map(Ok))
    }

    fn sort(rows: Vec<Row>, keys: &[SortKey]) -> Vec<Row> {
        let dir = tempfile::tempdir().unwrap();
        drain(super::sort(from_vec(rows), keys, &buffered_ctx(&dir)).unwrap())
    }

    fn limit_offset(rows: Vec<Row>, offset: usize, limit: Option<usize>) -> Vec<Row> {
        drain(super::limit_offset(from_vec(rows), offset, limit))
    }

    fn hash_aggregate(
        rows: Vec<Row>,
        group_cols: &[usize],
        aggs: &[(AggFunc, Option<usize>)],
    ) -> Vec<Row> {
        let dir = tempfile::tempdir().unwrap();
        drain(super::hash_aggregate(from_vec(rows), group_cols, aggs, &buffered_ctx(&dir)).unwrap())
    }

    fn hash_join(
        left: RowStream<'_>,
        right: RowStream<'_>,
        left_key: usize,
        right_key: usize,
    ) -> Vec<Row> {
        let dir = tempfile::tempdir().unwrap();
        drain(
            super::hash_join(
                fallible(left),
                fallible(right),
                left_key,
                right_key,
                &buffered_ctx(&dir),
            )
            .unwrap(),
        )
    }

    fn dedup(rows: Vec<Row>) -> Vec<Row> {
        let dir = tempfile::tempdir().unwrap();
        drain(super::dedup(from_vec(rows), &buffered_ctx(&dir)).unwrap())
    }

    fn users() -> InMemoryTable {
        InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("id", ColumnType::Int),
                Column::new("name", ColumnType::Text),
            ]),
            vec![
                Row::new(vec![Value::Int(1), Value::Text("alice".into())]),
                Row::new(vec![Value::Int(2), Value::Text("bob".into())]),
                Row::new(vec![Value::Int(3), Value::Text("carol".into())]),
            ],
        )
    }

    fn orders() -> InMemoryTable {
        InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("oid", ColumnType::Int),
                Column::new("uid", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Int(10), Value::Int(1)]),
                Row::new(vec![Value::Int(11), Value::Int(1)]),
                Row::new(vec![Value::Int(12), Value::Int(2)]),
                Row::new(vec![Value::Int(13), Value::Null]), // null FK: must not join
            ],
        )
    }

    #[test]
    fn seq_scan_yields_all_rows() {
        let rows: Vec<Row> = seq_scan(&users()).collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(1), &Value::Text("alice".into()));
    }

    #[test]
    fn filter_eq_selects_matching_rows() {
        let t = users();
        let out: Vec<Row> = filter(
            seq_scan(&t),
            Predicate {
                col: 0,
                op: CmpOp::Eq,
                value: Value::Int(2),
            },
        )
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get(1), &Value::Text("bob".into()));
    }

    #[test]
    fn filter_gt_on_ints() {
        let t = users();
        let out: Vec<Row> = filter(
            seq_scan(&t),
            Predicate {
                col: 0,
                op: CmpOp::Gt,
                value: Value::Int(1),
            },
        )
        .collect();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn filter_null_comparison_is_unknown_and_excluded() {
        let t = InMemoryTable::new(
            RelSchema::new(vec![Column::new("x", ColumnType::Int)]),
            vec![Row::new(vec![Value::Null]), Row::new(vec![Value::Int(5)])],
        );
        // NULL = NULL is UNKNOWN, not true → excluded
        let out: Vec<Row> = filter(
            seq_scan(&t),
            Predicate {
                col: 0,
                op: CmpOp::Eq,
                value: Value::Null,
            },
        )
        .collect();
        assert!(out.is_empty());
    }

    #[test]
    fn project_picks_columns_in_order() {
        let t = users();
        let out: Vec<Row> = project(seq_scan(&t), vec![1]).collect();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], Row::new(vec![Value::Text("alice".into())]));
    }

    #[test]
    fn hash_join_inner_equi_join() {
        let (u, o) = (users(), orders());
        let joined = hash_join(seq_scan(&u), seq_scan(&o), 0, 1);
        // user1 → 2 orders, user2 → 1, user3 → none, null FK dropped ⇒ 3 rows
        assert_eq!(joined.len(), 3);
        // joined row layout = [id, name, oid, uid]
        assert_eq!(
            joined
                .iter()
                .filter(|r| r.get(1) == &Value::Text("alice".into()))
                .count(),
            2
        );
        assert_eq!(
            joined
                .iter()
                .filter(|r| r.get(1) == &Value::Text("bob".into()))
                .count(),
            1
        );
    }

    fn r(vals: Vec<Value>) -> Row {
        Row::new(vals)
    }

    #[test]
    fn sort_single_key_asc_and_desc() {
        let rows = vec![
            r(vec![Value::Int(3)]),
            r(vec![Value::Int(1)]),
            r(vec![Value::Int(2)]),
        ];
        let asc = sort(
            rows.clone(),
            &[SortKey {
                col: 0,
                dir: SortDir::Asc,
            }],
        );
        assert_eq!(
            asc.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(2), Value::Int(3)]
        );
        let desc = sort(
            rows,
            &[SortKey {
                col: 0,
                dir: SortDir::Desc,
            }],
        );
        assert_eq!(
            desc.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(3), Value::Int(2), Value::Int(1)]
        );
    }

    #[test]
    fn sort_is_stable_and_multi_key() {
        // Sort by col0 asc, then col1 asc; equal col0 keeps insertion order.
        let rows = vec![
            r(vec![Value::Int(1), Value::Text("b".into())]),
            r(vec![Value::Int(1), Value::Text("a".into())]),
            r(vec![Value::Int(2), Value::Text("z".into())]),
            r(vec![Value::Int(1), Value::Text("a".into())]), // stable tie with row[1]
        ];
        let out = sort(
            rows,
            &[
                SortKey {
                    col: 0,
                    dir: SortDir::Asc,
                },
                SortKey {
                    col: 1,
                    dir: SortDir::Asc,
                },
            ],
        );
        let got: Vec<(Value, Value)> = out
            .iter()
            .map(|r| (r.get(0).clone(), r.get(1).clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                (Value::Int(1), Value::Text("a".into())),
                (Value::Int(1), Value::Text("a".into())),
                (Value::Int(1), Value::Text("b".into())),
                (Value::Int(2), Value::Text("z".into())),
            ]
        );
    }

    #[test]
    fn sort_null_placement_follows_postgres() {
        let rows = vec![
            r(vec![Value::Int(2)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(1)]),
        ];
        // ASC ⇒ NULLS LAST
        let asc = sort(
            rows.clone(),
            &[SortKey {
                col: 0,
                dir: SortDir::Asc,
            }],
        );
        assert_eq!(
            asc.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(2), Value::Null]
        );
        // DESC ⇒ NULLS FIRST
        let desc = sort(
            rows,
            &[SortKey {
                col: 0,
                dir: SortDir::Desc,
            }],
        );
        assert_eq!(
            desc.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Null, Value::Int(2), Value::Int(1)]
        );
    }

    #[test]
    fn limit_offset_slices() {
        let rows: Vec<Row> = (0..5).map(|i| r(vec![Value::Int(i)])).collect();
        assert_eq!(limit_offset(rows.clone(), 0, Some(2)).len(), 2);
        assert_eq!(limit_offset(rows.clone(), 3, None).len(), 2);
        assert_eq!(
            limit_offset(rows.clone(), 1, Some(2))[0],
            r(vec![Value::Int(1)])
        );
        assert_eq!(limit_offset(rows.clone(), 10, Some(2)).len(), 0);
        assert_eq!(limit_offset(rows, 0, None).len(), 5);
    }

    #[test]
    fn aggregate_ungrouped_count_sum_with_nulls() {
        // col0 = group-irrelevant value, col1 = nullable int
        let rows = vec![
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(3)]),
        ];
        // COUNT(*), COUNT(col0), SUM(col0)
        let out = hash_aggregate(
            rows,
            &[],
            &[
                (AggFunc::Count, None),
                (AggFunc::Count, Some(0)),
                (AggFunc::Sum, Some(0)),
            ],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, vec![Value::Int(3), Value::Int(2), Value::Int(4)]);
    }

    #[test]
    fn aggregate_min_max() {
        let rows = vec![
            r(vec![Value::Int(5)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(2)]),
            r(vec![Value::Int(9)]),
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[(AggFunc::Min, Some(0)), (AggFunc::Max, Some(0))],
        );
        assert_eq!(out[0].0, vec![Value::Int(2), Value::Int(9)]);
    }

    #[test]
    fn aggregate_grouped_count_and_sum() {
        // col0 = region (group), col1 = amount
        let rows = vec![
            r(vec![Value::Text("east".into()), Value::Int(10)]),
            r(vec![Value::Text("west".into()), Value::Int(5)]),
            r(vec![Value::Text("east".into()), Value::Int(20)]),
        ];
        let out = hash_aggregate(
            rows,
            &[0],
            &[(AggFunc::Count, None), (AggFunc::Sum, Some(1))],
        );
        // Group order is first-seen: east, west.
        assert_eq!(
            out[0].0,
            vec![Value::Text("east".into()), Value::Int(2), Value::Int(30)]
        );
        assert_eq!(
            out[1].0,
            vec![Value::Text("west".into()), Value::Int(1), Value::Int(5)]
        );
    }

    #[test]
    fn aggregate_empty_input_ungrouped_yields_one_row() {
        let out = hash_aggregate(
            vec![],
            &[],
            &[
                (AggFunc::Count, None),
                (AggFunc::Sum, Some(0)),
                (AggFunc::Min, Some(0)),
                (AggFunc::Max, Some(0)),
            ],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].0,
            vec![Value::Int(0), Value::Null, Value::Null, Value::Null]
        );
    }

    #[test]
    fn aggregate_empty_input_grouped_yields_zero_rows() {
        let out = hash_aggregate(vec![], &[0], &[(AggFunc::Count, None)]);
        assert!(out.is_empty());
    }

    #[test]
    fn aggregate_null_is_a_distinct_group_key() {
        let rows = vec![
            r(vec![Value::Null]),
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
        ];
        let out = hash_aggregate(rows, &[0], &[(AggFunc::Count, None)]);
        // Two groups: NULL (count 2) and 1 (count 1).
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, vec![Value::Null, Value::Int(2)]);
        assert_eq!(out[1].0, vec![Value::Int(1), Value::Int(1)]);
    }

    #[test]
    fn aggregate_avg_ungrouped_integer_column_gives_fractional() {
        // AVG over an Int column: (1 + 2) / 2 = 1.5, a fractional result.
        let rows = vec![
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(2)]),
        ];
        let out = hash_aggregate(rows, &[], &[(AggFunc::Avg, Some(0))]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, vec![Value::float(1.5)]);
    }

    #[test]
    fn aggregate_avg_grouped() {
        // col0 = region (group), col1 = amount
        let rows = vec![
            r(vec![Value::Text("east".into()), Value::Int(10)]),
            r(vec![Value::Text("west".into()), Value::Int(5)]),
            r(vec![Value::Text("east".into()), Value::Int(20)]),
        ];
        let out = hash_aggregate(rows, &[0], &[(AggFunc::Avg, Some(1))]);
        // east: (10+20)/2 = 15.0; west: 5/1 = 5.0
        assert_eq!(
            out[0].0,
            vec![Value::Text("east".into()), Value::float(15.0)]
        );
        assert_eq!(
            out[1].0,
            vec![Value::Text("west".into()), Value::float(5.0)]
        );
    }

    #[test]
    fn aggregate_avg_no_rows_is_null() {
        let rows = vec![r(vec![Value::Null])];
        let out = hash_aggregate(rows, &[], &[(AggFunc::Avg, Some(0))]);
        assert_eq!(out[0].0, vec![Value::Null]);
    }

    #[test]
    fn aggregate_sum_over_float_column_yields_float() {
        let rows = vec![
            r(vec![Value::float(1.5)]),
            r(vec![Value::Null]),
            r(vec![Value::float(2.25)]),
        ];
        let out = hash_aggregate(rows, &[], &[(AggFunc::Sum, Some(0))]);
        assert_eq!(out[0].0, vec![Value::float(3.75)]);
    }

    #[test]
    fn aggregate_sum_over_int_column_stays_int() {
        let rows = vec![r(vec![Value::Int(2)]), r(vec![Value::Int(3)])];
        let out = hash_aggregate(rows, &[], &[(AggFunc::Sum, Some(0))]);
        assert_eq!(out[0].0, vec![Value::Int(5)]);
    }

    #[test]
    fn aggregate_min_max_over_floats() {
        let rows = vec![
            r(vec![Value::float(5.5)]),
            r(vec![Value::Null]),
            r(vec![Value::float(2.25)]),
            r(vec![Value::float(9.0)]),
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[(AggFunc::Min, Some(0)), (AggFunc::Max, Some(0))],
        );
        assert_eq!(out[0].0, vec![Value::float(2.25), Value::float(9.0)]);
    }

    #[test]
    fn dedup_keeps_first_occurrence_order() {
        let rows = vec![
            r(vec![Value::Int(2)]),
            r(vec![Value::Int(1)]),
            r(vec![Value::Int(2)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
        ];
        let out = dedup(rows);
        assert_eq!(
            out,
            vec![
                r(vec![Value::Int(2)]),
                r(vec![Value::Int(1)]),
                r(vec![Value::Null]),
            ],
            "DISTINCT keeps the first occurrence of each row, in input order, \
             and NULL is a value like any other here"
        );
    }

    #[test]
    fn dedup_distinguishes_rows_whose_values_differ_only_by_type() {
        // `sql_cmp` calls Int(1) vs Text("1") UNKNOWN; structural equality calls
        // them different rows, and DISTINCT must agree with structural equality.
        let rows = vec![
            r(vec![Value::Int(1)]),
            r(vec![Value::Text("1".into())]),
            r(vec![Value::Int(1)]),
        ];
        assert_eq!(dedup(rows).len(), 2);
    }

    #[test]
    fn hash_join_null_keys_never_match() {
        let left = InMemoryTable::new(
            RelSchema::new(vec![Column::new("k", ColumnType::Int)]),
            vec![Row::new(vec![Value::Null])],
        );
        let right = InMemoryTable::new(
            RelSchema::new(vec![Column::new("k", ColumnType::Int)]),
            vec![Row::new(vec![Value::Null])],
        );
        assert!(hash_join(seq_scan(&left), seq_scan(&right), 0, 0).is_empty());
    }

    #[test]
    fn m1_first_join_query_shape() {
        // SELECT u.name, o.oid
        //   FROM users u JOIN orders o ON u.id = o.uid
        //  WHERE u.id = 1
        let (u, o) = (users(), orders());
        let filtered = filter(
            seq_scan(&u),
            Predicate {
                col: 0,
                op: CmpOp::Eq,
                value: Value::Int(1),
            },
        );
        let joined = hash_join(filtered, seq_scan(&o), 0, 1);
        // project u.name (idx 1) and o.oid (idx 2 = users width 2 + orders col 0)
        let projected: Vec<Row> = project(Box::new(joined.into_iter()), vec![1, 2]).collect();
        assert_eq!(projected.len(), 2); // user1 has two orders
        for r in &projected {
            assert_eq!(r.get(0), &Value::Text("alice".into()));
        }
        let oids: Vec<&Value> = projected.iter().map(|r| r.get(1)).collect();
        assert!(oids.contains(&&Value::Int(10)) && oids.contains(&&Value::Int(11)));
    }
}
