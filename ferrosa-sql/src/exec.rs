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
