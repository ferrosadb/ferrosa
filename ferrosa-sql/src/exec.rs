//! Physical operators (Volcano-style, iterator-based).
//!
//! First slice: `seq_scan`, `filter`, `project`, and an inner equi-`hash_join` —
//! the operators behind the M1 first-JOIN. NULL semantics follow SQL: a
//! comparison with NULL is UNKNOWN (row excluded), and NULL join keys never
//! match. The hash join materializes its build side for now; spill-to-disk past
//! a memory threshold is a bounded follow-up (Power-of-10 rule 3).

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::provider::TableProvider;
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
fn order_cmp(a: &Value, b: &Value, dir: SortDir) -> Ordering {
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

/// Stable multi-key sort over materialized rows.
pub fn sort(mut rows: Vec<Row>, keys: &[SortKey]) -> Vec<Row> {
    rows.sort_by(|l, r| {
        for k in keys {
            let ord = order_cmp(&l.0[k.col], &r.0[k.col], k.dir);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    rows
}

/// Apply `OFFSET`/`LIMIT` to materialized rows. `offset` rows are skipped; at
/// most `limit` (when `Some`) are then kept.
pub fn limit_offset(rows: Vec<Row>, offset: usize, limit: Option<usize>) -> Vec<Row> {
    let mut it = rows.into_iter().skip(offset);
    match limit {
        Some(n) => it.by_ref().take(n).collect(),
        None => it.collect(),
    }
}

/// A supported aggregate function. `AVG` is deliberately absent — [`Value`] has
/// no float variant — and is rejected at parse time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
}

/// One accumulator, paired with its `(func, arg-column)` definition.
struct Accumulator {
    count: i64,
    sum: i64,
    has_sum: bool,
    extreme: Option<Value>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0,
            has_sum: false,
            extreme: None,
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
            AggFunc::Sum => {
                if let Some(c) = arg {
                    if let Value::Int(n) = &row.0[c] {
                        self.sum += *n;
                        self.has_sum = true;
                    }
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
            // Postgres: SUM over no non-null rows is NULL.
            AggFunc::Sum => {
                if self.has_sum {
                    Value::Int(self.sum)
                } else {
                    Value::Null
                }
            }
            AggFunc::Min | AggFunc::Max => self.extreme.clone().unwrap_or(Value::Null),
        }
    }
}

/// Group rows by `group_cols` (a `Vec<Value>` key — NULL is a distinct group,
/// unlike join keys) and compute each aggregate. Output row layout is
/// `[group-key values…, agg values…]`.
///
/// With no `group_cols` the whole input is one group; over an empty input that
/// single group still emits exactly one row (COUNT=0, SUM/MIN/MAX=NULL). With
/// `group_cols` and empty input, zero rows are emitted.
pub fn hash_aggregate(
    rows: Vec<Row>,
    group_cols: &[usize],
    aggs: &[(AggFunc, Option<usize>)],
) -> Vec<Row> {
    // Preserve first-seen group order for deterministic output.
    let mut order: Vec<Vec<Value>> = Vec::new();
    let mut groups: HashMap<Vec<Value>, Vec<Accumulator>> = HashMap::new();

    for row in &rows {
        let key: Vec<Value> = group_cols.iter().map(|&c| row.0[c].clone()).collect();
        let accs = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            aggs.iter().map(|_| Accumulator::new()).collect()
        });
        for (acc, (func, arg)) in accs.iter_mut().zip(aggs.iter()) {
            acc.update(*func, *arg, row);
        }
    }

    // Ungrouped aggregate over an empty input: synthesize one all-empty group.
    if group_cols.is_empty() && order.is_empty() {
        let accs: Vec<Accumulator> = aggs.iter().map(|_| Accumulator::new()).collect();
        let values: Vec<Value> = aggs
            .iter()
            .zip(accs.iter())
            .map(|((func, _), acc)| acc.finish(*func))
            .collect();
        return vec![Row(values)];
    }

    order
        .into_iter()
        .map(|key| {
            let accs = &groups[&key];
            let mut values = key.clone();
            for (acc, (func, _)) in accs.iter().zip(aggs.iter()) {
                values.push(acc.finish(*func));
            }
            Row(values)
        })
        .collect()
}

/// A pull-based stream of rows flowing between operators.
pub type RowStream<'a> = Box<dyn Iterator<Item = Row> + 'a>;

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

/// Inner equi-join: emit `left ++ right` for every pair where
/// `left[left_key] == right[right_key]`. NULL keys never match.
pub fn hash_join(left: RowStream, right: RowStream, left_key: usize, right_key: usize) -> Vec<Row> {
    // Build phase: hash the right side by its key, skipping NULL keys.
    let mut build: HashMap<Value, Vec<Row>> = HashMap::new();
    for row in right {
        let key = row.0[right_key].clone();
        if key.is_null() {
            continue;
        }
        build.entry(key).or_default().push(row);
    }

    // Probe phase: for each left row, emit a concatenated row per match.
    let mut out = Vec::new();
    for left_row in left {
        let key = &left_row.0[left_key];
        if key.is_null() {
            continue;
        }
        if let Some(matches) = build.get(key) {
            for right_row in matches {
                let mut values = left_row.0.clone();
                values.extend(right_row.0.iter().cloned());
                out.push(Row(values));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::InMemoryTable;
    use crate::types::{Column, ColumnType, RelSchema, Row, Value};

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
