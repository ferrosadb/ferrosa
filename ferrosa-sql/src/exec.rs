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

use num_bigint::{BigInt, Sign};

use crate::provider::TableProvider;
use crate::spill::{
    canonical_cmp, Lookahead, ReplayBuffer, SeqRow, SpillCtx, SpillError, SpillSort, SqlOrder,
};
use crate::types::{ColumnType, Row, Value};

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
///
/// `Send`, so a suspended query can be held between pulls and resumed on
/// whichever thread is free (see [`crate::RowCursor`]).
pub type RowStream<'a> = Box<dyn Iterator<Item = Row> + Send + 'a>;

/// A pull-based stream of rows that can fail.
///
/// Blocking operators read and produce this: once an operator can spill, every
/// pull can hit a spill/merge I/O error, and that error has to reach the caller
/// rather than ending the stream early.
pub type TryRowStream<'a> = Box<dyn Iterator<Item = Result<Row, SpillError>> + Send + 'a>;

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

/// One aggregate request to [`hash_aggregate`]: the function, its argument
/// column (global index into each input row; `None` = `COUNT(*)`), and the
/// argument column's DECLARED engine type.
///
/// The declared type is load-bearing, not decorative. PostgreSQL types
/// `sum(int2|int4)` as `int8` but `sum(int8|numeric)` as `numeric`, and `avg`
/// of any integer or numeric as `numeric`; the plan derives the OUTPUT column
/// from exactly that rule (`plan::aggregate_column`). The accumulator must
/// return a `Value` whose type matches the derived column (invariant I-OID), so
/// it is TOLD the argument type rather than guessing it from the values it sees
/// — the engine stores both `int4` and `int8` columns as `Value::Int`, so those
/// two are indistinguishable from the values alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggSpec {
    pub func: AggFunc,
    pub arg: Option<usize>,
    /// The argument's declared type. `None` (only for `COUNT(*)`, or a caller
    /// with no schema, such as a direct unit test) is treated as the
    /// integer/numeric domain.
    pub arg_type: Option<ColumnType>,
}

impl AggSpec {
    /// A spec whose argument's declared type is not needed (`COUNT(*)`) or
    /// unknown (a schema-less caller). SUM/AVG fall back to the int8/numeric
    /// domain.
    pub fn new(func: AggFunc, arg: Option<usize>) -> Self {
        Self {
            func,
            arg,
            arg_type: None,
        }
    }

    /// A spec whose argument's declared type is known (the plan path, which
    /// derives SUM/AVG result types from it).
    pub fn typed(func: AggFunc, arg: Option<usize>, arg_type: ColumnType) -> Self {
        Self {
            func,
            arg,
            arg_type: Some(arg_type),
        }
    }
}

/// What an accumulator computes, fixed from its [`AggFunc`] and the argument's
/// declared type. This mirrors PostgreSQL's aggregate result-type rules so the
/// accumulator's `Value` always matches the plan's derived column (I-OID).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccKind {
    /// `COUNT(*)` / `COUNT(col)` -> int8.
    Count,
    /// `sum(int2|int4)` -> int8: exact, refusing (not wrapping) an out-of-int8
    /// total.
    SumInt8,
    /// `sum(int8|numeric)` -> numeric: exact and unbounded.
    SumNumeric,
    /// `sum(float4|float8)` -> float8.
    SumFloat,
    /// `avg(int2|int4|int8|numeric)` -> numeric: exact, no float rounding.
    AvgNumeric,
    /// `avg(float4|float8)` -> float8.
    AvgFloat,
    /// `MIN`/`MAX` -> the argument's own type.
    Extreme,
}

impl AccKind {
    fn of(func: AggFunc, arg_type: Option<ColumnType>) -> AccKind {
        match func {
            AggFunc::Count => AccKind::Count,
            AggFunc::Min | AggFunc::Max => AccKind::Extreme,
            AggFunc::Sum => match arg_type {
                Some(ColumnType::Float) => AccKind::SumFloat,
                Some(ColumnType::BigInt) | Some(ColumnType::Numeric) => AccKind::SumNumeric,
                // int2/int4, and the schema-less fallback, get sum's int8 result.
                _ => AccKind::SumInt8,
            },
            AggFunc::Avg => match arg_type {
                Some(ColumnType::Float) => AccKind::AvgFloat,
                _ => AccKind::AvgNumeric,
            },
        }
    }
}

/// Fractional digits PostgreSQL's `avg` of an integer/numeric result carries.
/// The division is rounded half-away-from-zero to this scale, exactly as
/// PostgreSQL does; `Value::numeric` then normalizes (strips trailing zeros)
/// without changing the value.
const AVG_DECIMAL_SCALE: i32 = 16;

/// One streaming accumulator.
///
/// Its state is a FIXED set of fields however many rows it consumes (invariant
/// I-SPACE): a row count, a `num_bigint::BigInt` running total, one `i32`
/// scale, one `f64`, and an optional extreme value. No `Vec`, nothing
/// proportional to the input. The `BigInt` grows only with the MAGNITUDE of the
/// exact total (`O(log sum)` bits — the same growth PostgreSQL's exact `numeric`
/// sum has), never with the row count, and an integer total that cannot fit its
/// declared result type fails loud instead of wrapping (invariant I-EXACT).
struct Accumulator {
    kind: AccKind,
    /// COUNT(*)/COUNT(col) result.
    count: i64,
    /// Exact running total over the integer and decimal domains.
    total: BigInt,
    /// The decimal scale of `total` (always 0 for an integer-domain SUM/AVG).
    total_scale: i32,
    /// Running total over the float domain.
    float_sum: f64,
    /// Non-NULL values that fed SUM/AVG; also the AVG divisor.
    numeric_count: i64,
    /// MIN/MAX running extreme.
    extreme: Option<Value>,
}

impl Accumulator {
    fn new(kind: AccKind) -> Self {
        Self {
            kind,
            count: 0,
            total: BigInt::from(0),
            total_scale: 0,
            float_sum: 0.0,
            numeric_count: 0,
            extreme: None,
        }
    }

    /// Accumulate one value into the exact integer/decimal running total.
    /// Non-numeric values are ignored (the plan guarantees the argument's type).
    fn add_exact(&mut self, v: &Value) {
        match v {
            Value::Int(n) => {
                self.total += BigInt::from(*n);
                self.numeric_count += 1;
            }
            Value::Numeric { unscaled, scale } => {
                self.add_decimal(unscaled, *scale);
                self.numeric_count += 1;
            }
            _ => {}
        }
    }

    /// Add `unscaled * 10^(-scale)` into `total`, aligning scales with pure
    /// integer math (no float can enter an integer/numeric total).
    fn add_decimal(&mut self, unscaled: &BigInt, scale: i32) {
        if scale > self.total_scale {
            self.total *= pow10(scale - self.total_scale);
            self.total_scale = scale;
        }
        let addend = unscaled * pow10(self.total_scale - scale);
        self.total += addend;
    }

    fn update(&mut self, spec: &AggSpec, row: &Row) {
        match self.kind {
            AccKind::Count => match spec.arg {
                // COUNT(*) counts every row; COUNT(col) only non-NULL.
                None => self.count += 1,
                Some(c) => {
                    if !row.0[c].is_null() {
                        self.count += 1;
                    }
                }
            },
            AccKind::SumInt8 | AccKind::SumNumeric | AccKind::AvgNumeric => {
                if let Some(c) = spec.arg {
                    self.add_exact(&row.0[c]);
                }
            }
            AccKind::SumFloat | AccKind::AvgFloat => {
                if let Some(c) = spec.arg {
                    match &row.0[c] {
                        Value::Float(f) => {
                            self.float_sum += f.0;
                            self.numeric_count += 1;
                        }
                        // A float-declared column always stores `Float`, but a
                        // directly-driven caller may hand an `Int`: promote it
                        // rather than drop it.
                        Value::Int(n) => {
                            self.float_sum += *n as f64;
                            self.numeric_count += 1;
                        }
                        _ => {}
                    }
                }
            }
            AccKind::Extreme => {
                if let Some(c) = spec.arg {
                    let v = &row.0[c];
                    if v.is_null() {
                        return;
                    }
                    let take = match &self.extreme {
                        None => true,
                        Some(cur) => {
                            let ord = v.sql_cmp(cur).unwrap_or(Ordering::Equal);
                            match spec.func {
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

    fn finish(&self) -> Result<Value, AggregateError> {
        match self.kind {
            AccKind::Count => Ok(Value::Int(self.count)),
            // PostgreSQL: SUM over no non-null rows is NULL.
            AccKind::SumInt8 => {
                if self.numeric_count == 0 {
                    return Ok(Value::Null);
                }
                i64::try_from(&self.total).map(Value::Int).map_err(|_| {
                    AggregateError::OutOfRange(format!(
                        "bigint out of range: sum of {} row(s) does not fit int8",
                        self.numeric_count
                    ))
                })
            }
            AccKind::SumNumeric => {
                if self.numeric_count == 0 {
                    return Ok(Value::Null);
                }
                Ok(Value::numeric(self.total.clone(), self.total_scale))
            }
            AccKind::SumFloat => {
                if self.numeric_count == 0 {
                    return Ok(Value::Null);
                }
                Ok(Value::float(self.float_sum))
            }
            AccKind::AvgNumeric => {
                if self.numeric_count == 0 {
                    return Ok(Value::Null);
                }
                Ok(avg_numeric(
                    &self.total,
                    self.total_scale,
                    self.numeric_count,
                ))
            }
            AccKind::AvgFloat => {
                if self.numeric_count == 0 {
                    return Ok(Value::Null);
                }
                Ok(Value::float(self.float_sum / self.numeric_count as f64))
            }
            AccKind::Extreme => Ok(self.extreme.clone().unwrap_or(Value::Null)),
        }
    }
}

/// `10^power` as a `BigInt`; a non-positive power is 1.
fn pow10(power: i32) -> BigInt {
    if power <= 0 {
        BigInt::from(1)
    } else {
        BigInt::from(10).pow(power as u32)
    }
}

/// The exact average of `total * 10^(-total_scale)` over `count` values, as the
/// `numeric` PostgreSQL's `avg` of an integer/numeric produces: pure integer
/// math rounded half-away-from-zero to [`AVG_DECIMAL_SCALE`] fractional digits,
/// so no float rounding can enter (invariant I-EXACT).
fn avg_numeric(total: &BigInt, total_scale: i32, count: i64) -> Value {
    // avg = total * 10^(-total_scale) / count. Expressed at AVG_DECIMAL_SCALE
    // fractional digits, unscaled = round(total * 10^(shift) / count).
    let shift = AVG_DECIMAL_SCALE - total_scale;
    let (num, den) = if shift >= 0 {
        (total * pow10(shift), BigInt::from(count))
    } else {
        (total.clone(), BigInt::from(count) * pow10(-shift))
    };
    Value::numeric(round_div_half_away(&num, &den), AVG_DECIMAL_SCALE)
}

/// `round(num / den)` half-away-from-zero, exact integer arithmetic. `den` is
/// non-zero; `num` may be negative.
fn round_div_half_away(num: &BigInt, den: &BigInt) -> BigInt {
    let q = num / den;
    let r = num % den;
    if r.sign() == Sign::NoSign {
        return q;
    }
    // Round away from zero when |r| >= |den|/2, i.e. 2|r| >= |den|. Magnitudes
    // carry no sign, so no `abs` (and no `num-traits` dependency) is needed.
    let mag_r = r.magnitude();
    let twice_r = mag_r + mag_r;
    if twice_r.cmp(den.magnitude()) != Ordering::Less {
        if num.sign() == den.sign() {
            q + 1
        } else {
            q - 1
        }
    } else {
        q
    }
}

/// The error of the aggregate operator: a spill failure while reading or
/// writing the bounded external sort, OR an exact-sum range failure. A range
/// failure is a DATA condition (SQLSTATE 22003), not an I/O failure, so it is
/// kept distinct from a spill error rather than folded into one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateError {
    Spill(SpillError),
    /// An exact integer total does not fit the type PostgreSQL gives the SUM
    /// (`bigint out of range`, 22003). Never wrapped.
    OutOfRange(String),
}

impl From<SpillError> for AggregateError {
    fn from(e: SpillError) -> Self {
        AggregateError::Spill(e)
    }
}

impl std::fmt::Display for AggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateError::Spill(e) => write!(f, "{e}"),
            AggregateError::OutOfRange(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for AggregateError {}

/// Build one output row `[group-key values…, agg values…]` from a finished group.
fn finish_group(key: Vec<Value>, accs: &[Accumulator]) -> Result<Row, AggregateError> {
    let mut values = key;
    for acc in accs {
        values.push(acc.finish()?);
    }
    Ok(Row(values))
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
    aggs: &[AggSpec],
    ctx: &SpillCtx,
) -> Result<TryRowStream<'static>, AggregateError> {
    const LABEL: &str = "hash_aggregate";

    let mut by_key = SpillSort::new(ctx, SqlOrder::Canonical(Some(group_cols.to_vec())), LABEL)?;
    for row in input {
        by_key.push(row?)?;
    }
    let rows_in = by_key.pushed();
    let sorted = by_key.finish()?;

    // The accumulator kind is fixed per aggregate, from its function and the
    // argument's declared type. Built once so allocation is not per-row.
    let fresh_accs = || -> Vec<Accumulator> {
        aggs.iter()
            .map(|a| Accumulator::new(AccKind::of(a.func, a.arg_type)))
            .collect()
    };

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
                by_seq.push_tagged(SeqRow::new(seq, finish_group(k, &accs)?))?;
            }
            current = Some((key, item.seq, fresh_accs()));
        }
        let (_, _, accs) = current.as_mut().expect("group open");
        for (acc, spec) in accs.iter_mut().zip(aggs.iter()) {
            acc.update(spec, &item.row);
        }
    }
    if let Some((k, seq, accs)) = current.take() {
        by_seq.push_tagged(SeqRow::new(seq, finish_group(k, &accs)?))?;
    }

    // Ungrouped aggregate over an empty input: synthesize one all-empty group.
    if group_cols.is_empty() && rows_in == 0 {
        by_seq.push_tagged(SeqRow::new(0, finish_group(Vec::new(), &fresh_accs())?))?;
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

/// Scan all rows of a table. The stream owns what it reads, so it outlives the
/// borrow of `table`.
pub fn seq_scan(table: &dyn TableProvider) -> RowStream<'static> {
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
    use num_bigint::BigInt;
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

    fn hash_aggregate(rows: Vec<Row>, group_cols: &[usize], aggs: &[AggSpec]) -> Vec<Row> {
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
                AggSpec::new(AggFunc::Count, None),
                AggSpec::new(AggFunc::Count, Some(0)),
                AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Int),
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
            &[
                AggSpec::new(AggFunc::Min, Some(0)),
                AggSpec::new(AggFunc::Max, Some(0)),
            ],
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
            &[
                AggSpec::new(AggFunc::Count, None),
                AggSpec::typed(AggFunc::Sum, Some(1), ColumnType::Int),
            ],
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
                AggSpec::new(AggFunc::Count, None),
                AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Int),
                AggSpec::new(AggFunc::Min, Some(0)),
                AggSpec::new(AggFunc::Max, Some(0)),
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
        let out = hash_aggregate(vec![], &[0], &[AggSpec::new(AggFunc::Count, None)]);
        assert!(out.is_empty());
    }

    #[test]
    fn aggregate_null_is_a_distinct_group_key() {
        let rows = vec![
            r(vec![Value::Null]),
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
        ];
        let out = hash_aggregate(rows, &[0], &[AggSpec::new(AggFunc::Count, None)]);
        // Two groups: NULL (count 2) and 1 (count 1).
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, vec![Value::Null, Value::Int(2)]);
        assert_eq!(out[1].0, vec![Value::Int(1), Value::Int(1)]);
    }

    #[test]
    fn aggregate_avg_ungrouped_integer_column_is_exact_numeric() {
        // AVG over an Int column: (1 + 2) / 2 = 1.5. PostgreSQL types avg(int4)
        // as `numeric` and computes it exactly. STRENGTHENED from the previous
        // `Value::float(1.5)`: the accumulator must yield the exact numeric the
        // plan already advertises (I-EXACT, I-OID).
        let rows = vec![
            r(vec![Value::Int(1)]),
            r(vec![Value::Null]),
            r(vec![Value::Int(2)]),
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Avg, Some(0), ColumnType::Int)],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, vec![Value::numeric(BigInt::from(15), 1)]);
    }

    #[test]
    fn aggregate_avg_grouped() {
        // col0 = region (group), col1 = amount
        let rows = vec![
            r(vec![Value::Text("east".into()), Value::Int(10)]),
            r(vec![Value::Text("west".into()), Value::Int(5)]),
            r(vec![Value::Text("east".into()), Value::Int(20)]),
        ];
        let out = hash_aggregate(
            rows,
            &[0],
            &[AggSpec::typed(AggFunc::Avg, Some(1), ColumnType::Int)],
        );
        // east: (10+20)/2 = 15; west: 5/1 = 5, exact numeric (I-EXACT).
        assert_eq!(
            out[0].0,
            vec![
                Value::Text("east".into()),
                Value::numeric(BigInt::from(15), 0)
            ]
        );
        assert_eq!(
            out[1].0,
            vec![
                Value::Text("west".into()),
                Value::numeric(BigInt::from(5), 0)
            ]
        );
    }

    #[test]
    fn aggregate_avg_no_rows_is_null() {
        let rows = vec![r(vec![Value::Null])];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Avg, Some(0), ColumnType::Int)],
        );
        assert_eq!(out[0].0, vec![Value::Null]);
    }

    #[test]
    fn aggregate_sum_over_float_column_yields_float() {
        let rows = vec![
            r(vec![Value::float(1.5)]),
            r(vec![Value::Null]),
            r(vec![Value::float(2.25)]),
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Float)],
        );
        assert_eq!(out[0].0, vec![Value::float(3.75)]);
    }

    #[test]
    fn aggregate_sum_over_int_column_stays_int() {
        let rows = vec![r(vec![Value::Int(2)]), r(vec![Value::Int(3)])];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Int)],
        );
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
            &[
                AggSpec::new(AggFunc::Min, Some(0)),
                AggSpec::new(AggFunc::Max, Some(0)),
            ],
        );
        assert_eq!(out[0].0, vec![Value::float(2.25), Value::float(9.0)]);
    }

    // ── I-EXACT: exact integer/numeric arithmetic ──────────────────────────

    /// `sum(int8)` is exact. Two int8 values whose total overflows the OLD i64
    /// accumulator must come back as the exact decimal, not a wrapped number —
    /// which is exactly why PostgreSQL types `sum(int8)` as `numeric`.
    #[test]
    fn sum_over_bigint_is_exact_across_i64_overflow() {
        let rows = vec![
            r(vec![Value::Int(i64::MAX)]),
            r(vec![Value::Int(i64::MAX)]),
            r(vec![Value::Int(1)]),
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::BigInt)],
        );
        // 2 * i64::MAX + 1 = 18446744073709551615, exactly.
        let want = BigInt::from(i64::MAX) * BigInt::from(2) + BigInt::from(1);
        assert_eq!(out[0].0, vec![Value::numeric(want, 0)]);
        // The wrapped i64 the old accumulator produced is WRONG by construction:
        // i64::MAX + i64::MAX + 1 == -1. Pin that we did NOT return it.
        assert_ne!(out[0].0, vec![Value::Int(-1)]);
    }

    /// `sum(numeric)` stays exact and preserves the decimal scale.
    #[test]
    fn sum_over_numeric_is_exact_and_scaled() {
        let rows = vec![
            r(vec![Value::numeric(BigInt::from(15), 1)]), // 1.5
            r(vec![Value::numeric(BigInt::from(25), 2)]), // 0.25
        ];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Numeric)],
        );
        // 1.75 == unscaled 175, scale 2.
        assert_eq!(out[0].0, vec![Value::numeric(BigInt::from(175), 2)]);
    }

    /// `avg(int)` is exact even where an f64 cannot represent the answer:
    /// `(2^53 + (2^53 + 2)) / 2 == 2^53 + 1`, which is NOT an f64.
    #[test]
    fn avg_over_int_is_exact_where_float_would_round() {
        let a = 1i64 << 53; // 9007199254740992
        let rows = vec![r(vec![Value::Int(a)]), r(vec![Value::Int(a + 2)])];
        let out = hash_aggregate(
            rows,
            &[],
            &[AggSpec::typed(AggFunc::Avg, Some(0), ColumnType::Int)],
        );
        assert_eq!(out[0].0, vec![Value::numeric(BigInt::from(a + 1), 0)]);
    }

    /// A `sum(int4)` whose exact total does not fit int8 is REFUSED, never
    /// wrapped (I-EXACT: exact or fail loud). The positive control — a sum that
    /// fits — succeeds, proving the refusal check can both fire and pass.
    #[test]
    fn sum_over_int_that_overflows_int8_is_refused_not_wrapped() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = buffered_ctx(&dir);
        let over = super::hash_aggregate(
            from_vec(vec![
                r(vec![Value::Int(i64::MAX)]),
                r(vec![Value::Int(i64::MAX)]),
            ]),
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Int)],
            &ctx,
        );
        let err = over
            .err()
            .expect("an int8-overflowing sum(int4) must fail loud, not wrap");
        assert!(
            matches!(err, AggregateError::OutOfRange(_)),
            "expected OutOfRange, got {err:?}"
        );

        // Positive control: the same check passes for a total that fits int8.
        let ok = hash_aggregate(
            vec![r(vec![Value::Int(1)]), r(vec![Value::Int(2)])],
            &[],
            &[AggSpec::typed(AggFunc::Sum, Some(0), ColumnType::Int)],
        );
        assert_eq!(ok[0].0, vec![Value::Int(3)]);
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
