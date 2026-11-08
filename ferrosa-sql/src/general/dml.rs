//! The read half of a general `UPDATE` / `DELETE` (lane W2).
//!
//! A [`DmlStmt`] compiles to two stacked queries:
//!
//! * **Stage A** scans the target joined with the `FROM` list (cross joins, the
//!   `WHERE` constrains them) and projects the whole joined row followed by the
//!   new value of every `SET` column, each evaluated over the old row and
//!   assignment-cast to its column's type.
//! * **Stage B** reads stage A's rows. Its scope re-creates stage A's bindings
//!   over the same slots, except that an assigned target column is routed to
//!   its `SET` value. So `RETURNING` compiled there reads the new row
//!   (`Dml.lean`, `returning_sees_new_row`). It projects
//!   `[storage key…, SET values…, RETURNING values…, old target row…]`.
//!
//! With a `FROM` list one target row can match several times. Stage B then sorts
//! on the key (a stable, spilling sort) and keeps the first row of each run
//! (`firstPerRun_strict`, `firstPerRun_first`). So each target row is written
//! once, with its first match in arrival order. PostgreSQL also applies a single
//! match; which one it applies is unspecified, and ours is deterministic.
//!
//! The front end writes each row by key (`read_then_write_eq_declarative`).

use std::sync::Arc;

use super::{
    has_subquery, item_expr, Binding, CExpr, Compiler, Ctes, Ctx, Mode, Query, Scope, Source,
    SourceTable, Ty,
};
use crate::ast::{ColumnRef, DmlAction, DmlStmt, JoinKind, Projection, ScalarExpr, TableRef};
use crate::catalog::Catalog;
use crate::exec::{SortDir, SortKey};
use crate::plan::{DmlLayout, DmlSpec, ExecError};
use crate::types::{Column, ColumnType, RelSchema};

/// Compile `stmt`'s read for `spec`.
pub(crate) fn compile_dml(
    stmt: &DmlStmt,
    spec: &DmlSpec,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<(Arc<Query>, DmlLayout), ExecError> {
    let compiler = Compiler::new(catalog, default_schema);
    let (mut q, layout) = compiler.dml(stmt, spec)?;
    q.params_needed = compiler.max_param.get();
    Ok((Arc::new(q), layout))
}

/// Infer each `$n`'s type in `stmt` (a `SET col = $n` takes `col`'s type).
pub(crate) fn infer_dml_params(
    stmt: &DmlStmt,
    spec: &DmlSpec,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<Vec<ColumnType>, ExecError> {
    let compiler = Compiler::new(catalog, default_schema);
    compiler.dml(stmt, spec)?;
    Ok(compiler.inferred_params())
}

/// Stage A, with what stage B needs to know about it.
struct StageA {
    query: Query,
    /// `(binding name, schema, base)` of the target (first) and each `FROM` item.
    bindings: Vec<(String, RelSchema, usize)>,
    /// The joined row's width; `SET` value `k` is slot `joined + k`.
    joined: usize,
    /// For each `SET`, the target column it assigns (local index) and the column.
    sets: Vec<(usize, Column)>,
}

impl Compiler<'_> {
    fn dml(&self, d: &DmlStmt, spec: &DmlSpec) -> Result<(Query, DmlLayout), ExecError> {
        if spec.key_columns.is_empty() {
            return Err(ExecError::Unsupported(
                "a DML read with no storage key".into(),
            ));
        }
        let mut ctes: Vec<(String, Arc<Query>)> = Vec::with_capacity(d.ctes.len());
        for cte in &d.ctes {
            let q = self.query(&cte.query, None, &ctes)?;
            ctes.push((cte.name.clone(), Arc::new(q)));
        }
        // Both stages are the statement's own query level: a subquery inside
        // them that reads a target column is correlated to it.
        self.refs.borrow_mut().push(usize::MAX);
        let compiled = self
            .dml_stage_a(d, spec, &ctes)
            .and_then(|a| self.dml_stage_b(d, spec, &ctes, a));
        let popped = self.refs.borrow_mut().pop();
        debug_assert!(popped.is_some(), "pushed above");
        compiled
    }

    /// The target must be a table: a CTE (or anything else) cannot be written.
    fn dml_target(
        &self,
        target: &TableRef,
        ctes: &Ctes,
    ) -> Result<(SourceTable, RelSchema), ExecError> {
        match self.source(target, ctes)? {
            (SourceTable::Cte(_), _) => Err(ExecError::Unsupported(format!(
                "UPDATE/DELETE of the CTE \"{}\"",
                target.table
            ))),
            table => Ok(table),
        }
    }

    fn dml_stage_a(&self, d: &DmlStmt, spec: &DmlSpec, ctes: &Ctes) -> Result<StageA, ExecError> {
        let mut scope = Scope {
            bindings: Vec::new(),
            parent: None,
        };
        let mut sources = Vec::with_capacity(1 + d.from.len());
        let (table, schema) = self.dml_target(&d.target, ctes)?;
        sources.push(cross_source(table, &schema));
        self.bind(&mut scope, &d.target, schema)?;
        for item in &d.from {
            let (table, schema) = self.source(item, ctes)?;
            sources.push(cross_source(table, &schema));
            self.bind(&mut scope, item, schema)?;
        }
        let mut needs_frame = d.filter.as_ref().is_some_and(super::condition_has_subquery);
        let filter = match &d.filter {
            Some(f) => Some(self.condition(f, &scope, ctes, &mut Ctx::Row { clause: "WHERE" })?),
            None => None,
        };
        let joined: usize = sources.iter().map(|s| s.width).sum();
        let mut project: Vec<CExpr> = (0..joined)
            .map(|idx| CExpr::Col { depth: 0, idx })
            .collect();
        let mut columns: Vec<Column> = scope
            .bindings
            .iter()
            .flat_map(|b| b.schema.columns.iter().cloned())
            .collect();
        let sets = self.dml_sets(d, spec, &scope, ctes)?;
        let mut set_columns = Vec::with_capacity(sets.len());
        for (local, column, expr, subquery) in sets {
            needs_frame |= subquery;
            project.push(expr);
            columns.push(column.clone());
            set_columns.push((local, column));
        }
        let bindings = scope
            .bindings
            .into_iter()
            .map(|b| (b.name, b.schema, b.base))
            .collect();
        Ok(StageA {
            query: plain_query(sources, filter, project, columns, needs_frame),
            bindings,
            joined,
            sets: set_columns,
        })
    }

    /// Compile each `SET col = expr` over the old row, assignment-cast to `col`.
    fn dml_sets(
        &self,
        d: &DmlStmt,
        spec: &DmlSpec,
        scope: &Scope<'_>,
        ctes: &Ctes,
    ) -> Result<Vec<(usize, Column, CExpr, bool)>, ExecError> {
        let DmlAction::Update(assignments) = &d.action else {
            return Ok(Vec::new());
        };
        let target = &scope.bindings[0].schema;
        let mut out: Vec<(usize, Column, CExpr, bool)> = Vec::with_capacity(assignments.len());
        for (name, expr) in assignments {
            let local = target
                .index_of(name)
                .ok_or_else(|| ExecError::NoSuchColumn(name.clone()))?;
            if spec.key_columns.iter().any(|k| k == name) {
                return Err(ExecError::Unsupported(format!(
                    "UPDATE of the storage key column \"{name}\""
                )));
            }
            if out.iter().any(|(l, ..)| *l == local) {
                return Err(ExecError::eval(
                    "42601",
                    format!("multiple assignments to same column \"{name}\""),
                ));
            }
            let ty = target.columns[local].ty;
            let (c, et) = self.expr(expr, scope, ctes, &mut Ctx::Row { clause: "SET" })?;
            self.record(&c, Some(ty));
            out.push((
                local,
                Column::new(name.clone(), ty),
                assign(c, et, ty),
                has_subquery(expr),
            ));
        }
        Ok(out)
    }

    fn dml_stage_b(
        &self,
        d: &DmlStmt,
        spec: &DmlSpec,
        ctes: &Ctes,
        a: StageA,
    ) -> Result<(Query, DmlLayout), ExecError> {
        let StageA {
            query,
            bindings,
            joined,
            sets,
        } = a;
        let target_schema = bindings[0].1.clone();
        let scope = stage_b_scope(bindings, joined, &sets);
        let mut project = Vec::new();
        let mut layout = DmlLayout::default();
        for name in &spec.key_columns {
            let local = target_schema
                .index_of(name)
                .ok_or_else(|| ExecError::NoSuchColumn(name.clone()))?;
            project.push(CExpr::Col {
                depth: 0,
                idx: local,
            });
            layout.keys.push(target_schema.columns[local].clone());
        }
        for (k, (_, column)) in sets.iter().enumerate() {
            project.push(CExpr::Col {
                depth: 0,
                idx: joined + k,
            });
            layout.sets.push(column.clone());
        }
        let returning = self.dml_returning(d, &scope, ctes)?;
        let needs_frame = returning.iter().any(|(_, _, sub)| *sub);
        for (expr, column, _) in returning {
            project.push(expr);
            layout.returning.push(column);
        }
        if spec.old_row {
            for (local, column) in target_schema.columns.iter().enumerate() {
                project.push(CExpr::Col {
                    depth: 0,
                    idx: local,
                });
                layout.old.push(column.clone());
            }
        }
        let width = joined + sets.len();
        let source = Source {
            kind: JoinKind::Inner,
            table: SourceTable::Cte(Arc::new(query)),
            on: None,
            width,
        };
        let columns = layout.all_columns();
        let mut q = plain_query(vec![source], None, project, columns, needs_frame);
        if !d.from.is_empty() {
            let keys = layout.keys.len();
            q.order = (0..keys)
                .map(|col| SortKey {
                    col,
                    dir: SortDir::Asc,
                })
                .collect();
            q.first_per_key = keys;
        }
        Ok((q, layout))
    }

    /// `RETURNING` compiled over stage B's scope: expression, output column, and
    /// whether it holds a subquery.
    fn dml_returning(
        &self,
        d: &DmlStmt,
        scope: &Scope<'_>,
        ctes: &Ctes,
    ) -> Result<Vec<(CExpr, Column, bool)>, ExecError> {
        let items: Vec<(ScalarExpr, String)> = match &d.returning {
            None => return Ok(Vec::new()),
            Some(Projection::Star) => scope
                .bindings
                .iter()
                .flat_map(|b| {
                    b.schema.columns.iter().map(move |c| {
                        let col = ColumnRef {
                            qualifier: Some(b.name.clone()),
                            name: c.name.clone(),
                        };
                        (ScalarExpr::Column(col), c.name.clone())
                    })
                })
                .collect(),
            Some(Projection::Items(items)) => items.iter().map(item_expr).collect(),
        };
        let mut out = Vec::with_capacity(items.len());
        for (expr, name) in &items {
            let (c, ty) = self.expr(
                expr,
                scope,
                ctes,
                &mut Ctx::Row {
                    clause: "RETURNING",
                },
            )?;
            out.push((
                c,
                Column::new(name.clone(), ty.unwrap_or(ColumnType::Text)),
                has_subquery(expr),
            ));
        }
        Ok(out)
    }
}

/// A `FROM` item joined with no condition of its own (the `WHERE` constrains it).
fn cross_source(table: SourceTable, schema: &RelSchema) -> Source {
    Source {
        kind: JoinKind::Inner,
        width: schema.width(),
        table,
        on: None,
    }
}

/// Stage A's bindings over the same slots, with each assigned target column
/// routed to its `SET` value at `joined + k`.
fn stage_b_scope<'p>(
    bindings: Vec<(String, RelSchema, usize)>,
    joined: usize,
    sets: &[(usize, Column)],
) -> Scope<'p> {
    let bindings = bindings
        .into_iter()
        .enumerate()
        .map(|(i, (name, schema, base))| {
            let remap = if i == 0 {
                (0..schema.width())
                    .map(|local| {
                        sets.iter()
                            .position(|(l, _)| *l == local)
                            .map(|k| joined + k)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            Binding {
                name,
                schema,
                base,
                remap,
            }
        })
        .collect();
    Scope {
        bindings,
        parent: None,
    }
}

/// PostgreSQL's assignment conversion: a value of another type is cast to the
/// column's (an untyped NULL or parameter already takes the column's type). The
/// cast range-checks and fails loudly on input it cannot convert.
fn assign(c: CExpr, ty_of: Ty, column: ColumnType) -> CExpr {
    match ty_of {
        Some(t) if t != column => CExpr::Cast(Box::new(c), column),
        _ => c,
    }
}

fn plain_query(
    sources: Vec<Source>,
    filter: Option<CExpr>,
    project: Vec<CExpr>,
    columns: Vec<Column>,
    needs_frame: bool,
) -> Query {
    Query {
        sources,
        filter,
        mode: Mode::Plain { project },
        columns,
        hidden: 0,
        order: Vec::new(),
        distinct: false,
        limit: None,
        offset: None,
        limit_param: None,
        offset_param: None,
        needs_frame,
        params_needed: 0,
        correlated: false,
        first_per_key: 0,
    }
}
