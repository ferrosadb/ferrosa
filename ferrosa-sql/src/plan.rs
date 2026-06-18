//! Bind + execute an M1 `SelectStmt` against a [`Catalog`].
//!
//! Pipeline: scan(from) [→ hash_join(scan(join))] [→ filter] → project. Column
//! references resolve through a scope of `(binding_name, RelSchema, base_offset)`
//! entries; an unqualified name that matches more than one table is rejected
//! (fail loud), and an unknown table/column errors rather than returning wrong
//! or empty results.

use std::fmt;

use crate::ast::{AggArg, ColumnRef, Projection, SelectItem, SelectStmt, TableRef};
use crate::catalog::Catalog;
use crate::exec::{
    hash_aggregate, hash_join, limit_offset, seq_scan, sort, AggFunc, Predicate, SortKey,
};
use crate::types::{Column, ColumnType, RelSchema, Row};

/// The result of executing a query: output column metadata + materialized rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    NoSuchTable {
        schema: String,
        table: String,
    },
    NoSuchColumn(String),
    AmbiguousColumn(String),
    UnknownQualifier(String),
    /// A non-aggregated column in the SELECT list is absent from `GROUP BY`.
    NotGrouped(String),
    /// An `ORDER BY` ordinal is out of range of the output columns.
    InvalidOrderBy(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::NoSuchTable { schema, table } => {
                write!(f, "relation \"{schema}.{table}\" does not exist")
            }
            ExecError::NoSuchColumn(c) => write!(f, "column \"{c}\" does not exist"),
            ExecError::AmbiguousColumn(c) => write!(f, "column reference \"{c}\" is ambiguous"),
            ExecError::UnknownQualifier(q) => {
                write!(f, "missing FROM-clause entry for table \"{q}\"")
            }
            ExecError::NotGrouped(c) => write!(
                f,
                "column \"{c}\" must appear in the GROUP BY clause or be used in an aggregate function"
            ),
            ExecError::InvalidOrderBy(c) => {
                write!(f, "ORDER BY position \"{c}\" is not in select list")
            }
        }
    }
}

impl std::error::Error for ExecError {}

/// Execute a parsed statement against `catalog`; bare table names resolve under
/// `default_schema`.
pub fn execute(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<QueryResult, ExecError> {
    let from_provider = resolve_table(catalog, &stmt.from, default_schema)?;
    let from_schema = from_provider.schema().clone();
    let from_binding = stmt.from.binding_name().to_string();

    let mut scope = vec![Bound {
        binding: from_binding.clone(),
        schema: from_schema.clone(),
        base: 0,
    }];
    let combined_schema: RelSchema;
    let base_rows: Vec<Row>;

    if let Some(join) = &stmt.join {
        let join_provider = resolve_table(catalog, &join.table, default_schema)?;
        let join_schema = join_provider.schema().clone();
        let join_binding = join.table.binding_name().to_string();

        // Resolve `ON a = b` to (from-local, join-local) key indices, either order.
        let (left_key, right_key) = resolve_join_keys(
            &from_binding,
            &from_schema,
            &join_binding,
            &join_schema,
            &join.left,
            &join.right,
        )?;

        base_rows = hash_join(
            seq_scan(&*from_provider),
            seq_scan(&*join_provider),
            left_key,
            right_key,
        );

        scope.push(Bound {
            binding: join_binding,
            schema: join_schema.clone(),
            base: from_schema.width(),
        });
        let mut cols = from_schema.columns.clone();
        cols.extend(join_schema.columns);
        combined_schema = RelSchema::new(cols);
    } else {
        base_rows = seq_scan(&*from_provider).collect();
        combined_schema = from_schema;
    }

    // WHERE
    let filtered: Vec<Row> = if let Some(f) = &stmt.filter {
        let idx = resolve_column(&scope, &f.column)?;
        let pred = Predicate {
            col: idx,
            op: f.op,
            value: f.value.clone(),
        };
        base_rows.into_iter().filter(|r| pred.eval(r)).collect()
    } else {
        base_rows
    };

    // Aggregate mode iff GROUP BY is present or any select item is an aggregate.
    let is_aggregate = !stmt.group_by.is_empty()
        || matches!(&stmt.projection, Projection::Items(items)
            if items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. })));

    let (columns, rows) = if is_aggregate {
        plan_aggregate(stmt, &scope, &combined_schema, filtered)?
    } else {
        plan_simple(stmt, &scope, &combined_schema, filtered)?
    };

    // LIMIT / OFFSET apply to the final output rows.
    let offset = stmt.offset.unwrap_or(0) as usize;
    let limit = stmt.limit.map(|n| n as usize);
    let rows = limit_offset(rows, offset, limit);

    Ok(QueryResult { columns, rows })
}

/// Non-aggregate path: ORDER BY (resolved against the scope) then projection.
fn plan_simple(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
    rows: Vec<Row>,
) -> Result<(Vec<Column>, Vec<Row>), ExecError> {
    // ORDER BY resolves against the input scope, so it may name a non-selected
    // column. Apply it to the joined/filtered rows before projecting.
    let rows = if stmt.order_by.is_empty() {
        rows
    } else {
        let mut keys = Vec::with_capacity(stmt.order_by.len());
        for item in &stmt.order_by {
            let col = resolve_column(scope, &item.column)?;
            keys.push(SortKey { col, dir: item.dir });
        }
        sort(rows, &keys)
    };

    let (columns, indices): (Vec<Column>, Vec<usize>) = match &stmt.projection {
        Projection::Star => (
            combined_schema.columns.clone(),
            (0..combined_schema.width()).collect(),
        ),
        Projection::Items(items) => {
            let mut columns = Vec::with_capacity(items.len());
            let mut indices = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    SelectItem::Column(cr) => {
                        let gi = resolve_column(scope, cr)?;
                        indices.push(gi);
                        columns.push(combined_schema.columns[gi].clone());
                    }
                    // Unreachable: an aggregate item forces aggregate mode.
                    SelectItem::Aggregate { .. } => unreachable!("aggregate in simple plan"),
                }
            }
            (columns, indices)
        }
    };

    let out = rows
        .into_iter()
        .map(|r| Row(indices.iter().map(|&i| r.0[i].clone()).collect()))
        .collect();
    Ok((columns, out))
}

/// Aggregate path: resolve group/agg columns, validate, run `hash_aggregate`,
/// reorder into SELECT-list order, then ORDER BY against the output.
fn plan_aggregate(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
    rows: Vec<Row>,
) -> Result<(Vec<Column>, Vec<Row>), ExecError> {
    // Resolve GROUP BY columns to global indices.
    let mut group_cols = Vec::with_capacity(stmt.group_by.len());
    for cr in &stmt.group_by {
        group_cols.push(resolve_column(scope, cr)?);
    }

    let items = match &stmt.projection {
        // `SELECT *` with GROUP BY is not meaningfully supported here; treat
        // every grouped column as the projection is out of scope — reject.
        Projection::Star => return Err(ExecError::NotGrouped("*".into())),
        Projection::Items(items) => items,
    };

    // Build the aggregate definitions and validate plain columns are grouped.
    let mut aggs: Vec<(AggFunc, Option<usize>)> = Vec::new();
    // For each SELECT item, record where its value lives in the hash_aggregate
    // output layout `[group_cols..., aggs...]`: Group(group_index) or Agg(agg_index).
    enum Slot {
        Group(usize),
        Agg(usize),
    }
    let mut slots: Vec<Slot> = Vec::with_capacity(items.len());
    // Output column metadata, parallel to SELECT-list order.
    let mut out_columns: Vec<Column> = Vec::with_capacity(items.len());

    for item in items {
        match item {
            SelectItem::Column(cr) => {
                let gi = resolve_column(scope, cr)?;
                // Must be one of the GROUP BY columns (by global index).
                let pos = group_cols
                    .iter()
                    .position(|&g| g == gi)
                    .ok_or_else(|| ExecError::NotGrouped(cr.qualified_name()))?;
                slots.push(Slot::Group(pos));
                out_columns.push(combined_schema.columns[gi].clone());
            }
            SelectItem::Aggregate { func, arg } => {
                let arg_col = match arg {
                    AggArg::Star => None,
                    AggArg::Column(cr) => Some(resolve_column(scope, cr)?),
                };
                let agg_index = aggs.len();
                aggs.push((*func, arg_col));
                slots.push(Slot::Agg(agg_index));
                out_columns.push(aggregate_column(*func, arg_col, combined_schema));
            }
        }
    }

    let agg_rows = hash_aggregate(rows, &group_cols, &aggs);

    // hash_aggregate output layout is [group values..., agg values...]; project
    // into SELECT-list order via the recorded slots.
    let group_len = group_cols.len();
    let reordered: Vec<Row> = agg_rows
        .into_iter()
        .map(|r| {
            let values = slots
                .iter()
                .map(|slot| match slot {
                    Slot::Group(i) => r.0[*i].clone(),
                    Slot::Agg(i) => r.0[group_len + *i].clone(),
                })
                .collect();
            Row(values)
        })
        .collect();

    // ORDER BY in aggregate mode resolves against the OUTPUT columns: by name or
    // by 1-based ordinal.
    let out_rows = if stmt.order_by.is_empty() {
        reordered
    } else {
        let mut keys = Vec::with_capacity(stmt.order_by.len());
        for item in &stmt.order_by {
            let col = resolve_output_position(&item.column, &out_columns)?;
            keys.push(SortKey { col, dir: item.dir });
        }
        sort(reordered, &keys)
    };

    Ok((out_columns, out_rows))
}

/// The output column for an aggregate: name `count`/`sum`/`min`/`max`/`avg`.
/// COUNT is always `Int`; AVG is always `Float`; SUM and MIN/MAX take the
/// argument column's type.
fn aggregate_column(func: AggFunc, arg_col: Option<usize>, schema: &RelSchema) -> Column {
    let (name, ty) = match func {
        AggFunc::Count => ("count", ColumnType::Int),
        AggFunc::Sum => ("sum", arg_ty(arg_col, schema)),
        AggFunc::Avg => ("avg", ColumnType::Float),
        AggFunc::Min => ("min", arg_ty(arg_col, schema)),
        AggFunc::Max => ("max", arg_ty(arg_col, schema)),
    };
    Column::new(name, ty)
}

fn arg_ty(arg_col: Option<usize>, schema: &RelSchema) -> ColumnType {
    match arg_col {
        Some(c) => schema.columns[c].ty,
        None => ColumnType::Int,
    }
}

/// Resolve an ORDER BY key against the aggregate output columns: a bare integer
/// name is a 1-based ordinal; otherwise match by column name.
fn resolve_output_position(col: &ColumnRef, out_columns: &[Column]) -> Result<usize, ExecError> {
    if col.qualifier.is_none() {
        if let Ok(ordinal) = col.name.parse::<usize>() {
            if ordinal == 0 || ordinal > out_columns.len() {
                return Err(ExecError::InvalidOrderBy(col.name.clone()));
            }
            return Ok(ordinal - 1);
        }
    }
    out_columns
        .iter()
        .position(|c| c.name == col.name)
        .ok_or_else(|| ExecError::NoSuchColumn(col.name.clone()))
}

/// One bound relation in the FROM/JOIN scope.
struct Bound {
    binding: String,
    schema: RelSchema,
    base: usize,
}

fn resolve_table(
    catalog: &dyn Catalog,
    table: &TableRef,
    default_schema: &str,
) -> Result<crate::catalog::SharedTable, ExecError> {
    let schema = table.schema.as_deref().unwrap_or(default_schema);
    catalog
        .resolve(schema, &table.table)
        .ok_or_else(|| ExecError::NoSuchTable {
            schema: schema.to_string(),
            table: table.table.clone(),
        })
}

/// Local column index within one table, honoring an optional qualifier.
fn local_index(binding: &str, schema: &RelSchema, col: &ColumnRef) -> Option<usize> {
    if let Some(q) = &col.qualifier {
        if q != binding {
            return None;
        }
    }
    schema.index_of(&col.name)
}

/// Resolve an `ON left = right` condition into `(from_local, join_local)` key
/// indices, accepting either column order.
fn resolve_join_keys(
    from_binding: &str,
    from_schema: &RelSchema,
    join_binding: &str,
    join_schema: &RelSchema,
    left: &ColumnRef,
    right: &ColumnRef,
) -> Result<(usize, usize), ExecError> {
    if let (Some(l), Some(r)) = (
        local_index(from_binding, from_schema, left),
        local_index(join_binding, join_schema, right),
    ) {
        return Ok((l, r));
    }
    if let (Some(r), Some(l)) = (
        local_index(from_binding, from_schema, right),
        local_index(join_binding, join_schema, left),
    ) {
        return Ok((r, l));
    }
    Err(ExecError::NoSuchColumn(format!(
        "{} = {}",
        left.name, right.name
    )))
}

/// Resolve a column reference to a global index into the joined row.
fn resolve_column(scope: &[Bound], col: &ColumnRef) -> Result<usize, ExecError> {
    let mut found: Option<usize> = None;
    for b in scope {
        if let Some(q) = &col.qualifier {
            if q != &b.binding {
                continue;
            }
        }
        if let Some(local) = b.schema.index_of(&col.name) {
            let global = b.base + local;
            if found.is_some() {
                return Err(ExecError::AmbiguousColumn(col.name.clone()));
            }
            found = Some(global);
        }
    }
    // A qualifier that matched no table at all is a distinct error.
    if found.is_none() {
        if let Some(q) = &col.qualifier {
            if !scope.iter().any(|b| &b.binding == q) {
                return Err(ExecError::UnknownQualifier(q.clone()));
            }
        }
    }
    found.ok_or_else(|| ExecError::NoSuchColumn(col.name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MapCatalog;
    use crate::parser::parse;
    use crate::provider::InMemoryTable;
    use crate::types::{Column, ColumnType, RelSchema, Row, Value};
    use std::sync::Arc;

    fn catalog() -> MapCatalog {
        let users = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("id", ColumnType::Int),
                Column::new("name", ColumnType::Text),
            ]),
            vec![
                Row::new(vec![Value::Int(1), Value::Text("alice".into())]),
                Row::new(vec![Value::Int(2), Value::Text("bob".into())]),
            ],
        ));
        let orders = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("oid", ColumnType::Int),
                Column::new("uid", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Int(10), Value::Int(1)]),
                Row::new(vec![Value::Int(11), Value::Int(1)]),
                Row::new(vec![Value::Int(12), Value::Int(2)]),
            ],
        ));
        MapCatalog::new()
            .with_table("public", "users", users)
            .with_table("public", "orders", orders)
    }

    fn run(sql: &str) -> QueryResult {
        execute(&parse(sql).unwrap(), &catalog(), "public").unwrap()
    }

    #[test]
    fn select_star_returns_all_columns_and_rows() {
        let r = run("SELECT * FROM users");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn projection_selects_named_columns() {
        let r = run("SELECT name FROM users");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "name");
        assert_eq!(r.rows[0], Row::new(vec![Value::Text("alice".into())]));
    }

    #[test]
    fn filter_restricts_rows() {
        let r = run("SELECT name FROM users WHERE id = 2");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Text("bob".into())])]);
    }

    #[test]
    fn the_m1_join_returns_correct_rows() {
        let r =
            run("SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["name", "oid"]
        );
        assert_eq!(r.rows.len(), 2); // alice has two orders
        for row in &r.rows {
            assert_eq!(row.get(0), &Value::Text("alice".into()));
        }
        let oids: Vec<&Value> = r.rows.iter().map(|row| row.get(1)).collect();
        assert!(oids.contains(&&Value::Int(10)) && oids.contains(&&Value::Int(11)));
    }

    #[test]
    fn unknown_table_fails_loud() {
        let err = execute(&parse("SELECT * FROM nope").unwrap(), &catalog(), "public").unwrap_err();
        assert!(matches!(err, ExecError::NoSuchTable { .. }));
    }

    #[test]
    fn unknown_column_fails_loud() {
        let err = execute(
            &parse("SELECT zzz FROM users").unwrap(),
            &catalog(),
            "public",
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NoSuchColumn(_)));
    }

    fn sales_catalog() -> MapCatalog {
        // region (text), amount (nullable int)
        let sales = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("region", ColumnType::Text),
                Column::new("amount", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Text("east".into()), Value::Int(10)]),
                Row::new(vec![Value::Text("west".into()), Value::Int(5)]),
                Row::new(vec![Value::Text("east".into()), Value::Int(20)]),
                Row::new(vec![Value::Text("west".into()), Value::Null]),
            ],
        ));
        MapCatalog::new().with_table("public", "sales", sales)
    }

    fn run_sales(sql: &str) -> QueryResult {
        execute(&parse(sql).unwrap(), &sales_catalog(), "public").unwrap()
    }

    #[test]
    fn group_by_with_count_and_sum() {
        let r = run_sales("SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| (c.name.as_str(), c.ty))
                .collect::<Vec<_>>(),
            [
                ("region", ColumnType::Text),
                ("count", ColumnType::Int),
                ("sum", ColumnType::Int),
            ]
        );
        // east: count 2, sum 30; west: count 2, sum 5 (one NULL ignored).
        assert_eq!(
            r.rows[0].0,
            vec![Value::Text("east".into()), Value::Int(2), Value::Int(30)]
        );
        assert_eq!(
            r.rows[1].0,
            vec![Value::Text("west".into()), Value::Int(2), Value::Int(5)]
        );
    }

    #[test]
    fn avg_ungrouped_gives_fractional_float() {
        // AVG over the int `amount` column: (10 + 5 + 20) / 3 = 11.666...
        let r = run_sales("SELECT AVG(amount) FROM sales");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "avg");
        assert_eq!(r.columns[0].ty, ColumnType::Float);
        let Value::Float(of) = &r.rows[0].0[0] else {
            panic!("expected float avg");
        };
        assert!((of.0 - 35.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn avg_grouped_output_types_and_values() {
        let r = run_sales("SELECT region, AVG(amount) FROM sales GROUP BY region");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| (c.name.as_str(), c.ty))
                .collect::<Vec<_>>(),
            [("region", ColumnType::Text), ("avg", ColumnType::Float)]
        );
        // east: (10+20)/2 = 15.0; west: 5/1 = 5.0 (NULL ignored).
        assert_eq!(
            r.rows[0].0,
            vec![Value::Text("east".into()), Value::float(15.0)]
        );
        assert_eq!(
            r.rows[1].0,
            vec![Value::Text("west".into()), Value::float(5.0)]
        );
    }

    #[test]
    fn avg_unknown_arg_column_fails_loud() {
        // AVG's argument must resolve like any other aggregate argument.
        let err = execute(
            &parse("SELECT AVG(nope) FROM sales").unwrap(),
            &sales_catalog(),
            "public",
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NoSuchColumn(_)));
    }

    #[test]
    fn ungrouped_count_star_over_table() {
        let r = run_sales("SELECT COUNT(*) FROM sales");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "count");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Int(4)])]);
    }

    #[test]
    fn min_max_aggregate() {
        let r = run_sales("SELECT MIN(amount), MAX(amount) FROM sales");
        assert_eq!(r.columns[0].name, "min");
        assert_eq!(r.columns[1].name, "max");
        assert_eq!(r.columns[0].ty, ColumnType::Int);
        assert_eq!(r.rows[0].0, vec![Value::Int(5), Value::Int(20)]);
    }

    #[test]
    fn order_by_asc_and_desc_with_null() {
        // ASC ⇒ NULLS LAST
        let r = run_sales("SELECT amount FROM sales ORDER BY amount ASC");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(5), Value::Int(10), Value::Int(20), Value::Null]
        );
        // DESC ⇒ NULLS FIRST
        let r = run_sales("SELECT amount FROM sales ORDER BY amount DESC");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Null, Value::Int(20), Value::Int(10), Value::Int(5)]
        );
    }

    #[test]
    fn order_by_non_selected_column() {
        // Order by amount but only select region.
        let r = run_sales("SELECT region FROM sales ORDER BY amount ASC");
        assert_eq!(r.columns.len(), 1);
        // First by amount asc: 5(west),10(east),20(east),NULL(west)
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![
                Value::Text("west".into()),
                Value::Text("east".into()),
                Value::Text("east".into()),
                Value::Text("west".into()),
            ]
        );
    }

    #[test]
    fn limit_and_offset() {
        let r = run_sales("SELECT amount FROM sales ORDER BY amount ASC LIMIT 2 OFFSET 1");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(10), Value::Int(20)]
        );
    }

    #[test]
    fn non_grouped_column_fails_loud() {
        let err = execute(
            &parse("SELECT region, amount FROM sales GROUP BY region").unwrap(),
            &sales_catalog(),
            "public",
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NotGrouped(_)));
    }

    #[test]
    fn order_by_ordinal_against_output() {
        let r = run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 2 DESC");
        // both groups have count 2; stable, so order preserved (east, west).
        // Make it discriminating: order by region desc via ordinal 1.
        let r2 = run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 1 DESC");
        assert_eq!(r2.rows[0].0[0], Value::Text("west".into()));
        assert_eq!(r2.rows[1].0[0], Value::Text("east".into()));
        // ordinal-2 sort still yields both rows.
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn order_by_ordinal_out_of_range_fails_loud() {
        let err = execute(
            &parse("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 5").unwrap(),
            &sales_catalog(),
            "public",
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::InvalidOrderBy(_)));
    }

    #[test]
    fn aggregate_with_where_and_join() {
        // users(id,name) JOIN orders(oid,uid); count orders per user, filtered.
        let r = execute(
            &parse(
                "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.uid \
                 WHERE u.id = 1 GROUP BY u.name",
            )
            .unwrap(),
            &catalog(),
            "public",
        )
        .unwrap();
        assert_eq!(r.columns[0].name, "name");
        assert_eq!(r.columns[1].name, "count");
        assert_eq!(
            r.rows,
            vec![Row::new(vec![Value::Text("alice".into()), Value::Int(2)])]
        );
    }

    #[test]
    fn ungrouped_aggregate_over_empty_filter_yields_one_row() {
        let r = run_sales("SELECT COUNT(*), SUM(amount) FROM sales WHERE amount > 1000");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].0, vec![Value::Int(0), Value::Null]);
    }

    #[test]
    fn ambiguous_unqualified_column_fails_loud() {
        // both users and orders would need a shared column; craft one:
        let shared = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![Column::new("x", ColumnType::Int)]),
            vec![Row::new(vec![Value::Int(1)])],
        ));
        let cat = MapCatalog::new()
            .with_table("public", "a", shared.clone())
            .with_table("public", "b", shared);
        let err = execute(
            &parse("SELECT x FROM a JOIN b ON a.x = b.x").unwrap(),
            &cat,
            "public",
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::AmbiguousColumn(_)));
    }
}
