//! Bind + execute an M1 `SelectStmt` against a [`Catalog`].
//!
//! Pipeline: scan(from) [→ hash_join(scan(join))] [→ filter] → project. Column
//! references resolve through a scope of `(binding_name, RelSchema, base_offset)`
//! entries; an unqualified name that matches more than one table is rejected
//! (fail loud), and an unknown table/column errors rather than returning wrong
//! or empty results.

use std::fmt;

use crate::ast::{ColumnRef, Projection, SelectStmt, TableRef};
use crate::catalog::Catalog;
use crate::exec::{hash_join, seq_scan, Predicate};
use crate::types::{Column, RelSchema, Row};

/// The result of executing a query: output column metadata + materialized rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    NoSuchTable { schema: String, table: String },
    NoSuchColumn(String),
    AmbiguousColumn(String),
    UnknownQualifier(String),
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

    // SELECT list
    let (columns, indices): (Vec<Column>, Vec<usize>) = match &stmt.projection {
        Projection::Star => (
            combined_schema.columns.clone(),
            (0..combined_schema.width()).collect(),
        ),
        Projection::Columns(refs) => {
            let mut columns = Vec::with_capacity(refs.len());
            let mut indices = Vec::with_capacity(refs.len());
            for cr in refs {
                let gi = resolve_column(&scope, cr)?;
                indices.push(gi);
                columns.push(combined_schema.columns[gi].clone());
            }
            (columns, indices)
        }
    };

    let rows = filtered
        .into_iter()
        .map(|r| Row(indices.iter().map(|&i| r.0[i].clone()).collect()))
        .collect();

    Ok(QueryResult { columns, rows })
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
