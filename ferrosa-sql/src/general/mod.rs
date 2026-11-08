//! The general planner: statements the legacy plan cannot carry.
//!
//! A statement goes here when [`SelectStmt::is_general`] says so: a projected
//! expression, a join on an arbitrary condition (or a LEFT join, or more than one
//! join), a CTE, or a `WHERE`/`HAVING` holding a node the legacy `Expr::Compare`
//! cannot express. The semantics are the ones `specs/lean4/Ferrosa/Sql/Expr.lean`
//! pins: three-valued logic, `WHERE` keeping only TRUE rows, `= ANY` and `NOT IN`
//! over NULLs, `coalesce`.
//!
//! The statement is first *compiled* ([`compile`]): every name resolves against
//! the catalog once, every column reference becomes a `(depth, index)` slot (depth
//! 0 is the query's own row, depth `d` the `d`-th enclosing query's row, which is
//! how a correlated subquery reads its outer row), and every expression gets its
//! static type, so `describe` and execution agree on the output columns.
//!
//! The compiled [`Query`] owns its tables (`Arc`) and its subqueries (`Arc`), so
//! the row stream [`run::open`] builds is `'static` and a suspended portal can
//! hold it. Rows move through the pipeline; nothing is copied except where a row
//! is genuinely duplicated (a join emitting one left row against several right
//! rows).

mod eval;
pub(crate) mod run;

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use crate::ast::{
    AggArg, BinOp, ColumnRef, Expr, JoinKind, Operand, Projection, ScalarExpr, SelectItem,
    SelectStmt, TableRef, Term, TypeName, UnaryOp,
};
use crate::catalog::{Catalog, SharedTable};
use crate::exec::{AggFunc, AggSpec, CmpOp, SortKey};
use crate::plan::ExecError;
use crate::types::{ArrayElem, Column, ColumnType, RelSchema, Value};

pub(crate) use run::Fence;

/// A compiled `SELECT`.
#[derive(Debug)]
pub(crate) struct Query {
    /// `FROM` first, then each join in order.
    sources: Vec<Source>,
    filter: Option<CExpr>,
    mode: Mode,
    /// The visible output columns.
    columns: Vec<Column>,
    /// Trailing projected values that exist only to sort on; stripped after the sort.
    hidden: usize,
    order: Vec<SortKey>,
    distinct: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    /// `LIMIT $n` / `OFFSET $n`, resolved when the query opens (`Limit.lean`).
    limit_param: Option<usize>,
    offset_param: Option<usize>,
    /// Whether any expression evaluated over this query's rows holds a subquery,
    /// so each row must be lent to it as an outer frame.
    needs_frame: bool,
    /// The highest `$n` the statement references (top level only).
    params_needed: usize,
}

#[derive(Debug)]
struct Source {
    kind: JoinKind,
    table: SourceTable,
    /// The join condition, over the combined row of every source up to this one.
    on: Option<CExpr>,
    width: usize,
}

enum SourceTable {
    Table(SharedTable),
    Cte(Arc<Query>),
}

impl std::fmt::Debug for SourceTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceTable::Table(t) => f.debug_tuple("Table").field(t.schema()).finish(),
            SourceTable::Cte(q) => f.debug_tuple("Cte").field(q).finish(),
        }
    }
}

#[derive(Debug)]
enum Mode {
    /// One output row per source row: `project` over the source row.
    Plain { project: Vec<CExpr> },
    /// `pre` (group keys, then aggregate arguments) over the source row feeds the
    /// hash aggregate, whose rows are `[group keys…, aggregate results…]`;
    /// `having` and `project` run over those.
    Aggregate {
        pre: Vec<CExpr>,
        groups: usize,
        aggs: Vec<AggSpec>,
        having: Option<CExpr>,
        project: Vec<CExpr>,
    },
}

/// A compiled scalar expression.
#[derive(Debug)]
pub(crate) enum CExpr {
    Col {
        depth: usize,
        idx: usize,
    },
    Lit(Value),
    /// `$n`, 1-based.
    Param(usize),
    Cast(Box<CExpr>, ColumnType),
    Neg(Box<CExpr>, ColumnType),
    Not(Box<CExpr>),
    /// Arithmetic and `||`; `ty` is the result type (range checks use it).
    Arith {
        op: BinOp,
        left: Box<CExpr>,
        right: Box<CExpr>,
        ty: ColumnType,
    },
    Cmp(CmpOp, Box<CExpr>, Box<CExpr>),
    And(Box<CExpr>, Box<CExpr>),
    Or(Box<CExpr>, Box<CExpr>),
    IsNull(Box<CExpr>, bool),
    InList {
        expr: Box<CExpr>,
        list: Vec<CExpr>,
        negated: bool,
    },
    Between {
        expr: Box<CExpr>,
        low: Box<CExpr>,
        high: Box<CExpr>,
        negated: bool,
    },
    Any {
        left: Box<CExpr>,
        op: CmpOp,
        right: Box<CExpr>,
    },
    Func(Func, Vec<CExpr>),
    Case {
        operand: Option<Box<CExpr>>,
        whens: Vec<(CExpr, CExpr)>,
        otherwise: Option<Box<CExpr>>,
    },
    Exists(Arc<Query>, bool),
    Subquery(Arc<Query>),
    ArraySubquery(Arc<Query>, ArrayElem),
    Array(Vec<CExpr>, ArrayElem),
    /// `x AT TIME ZONE 'UTC'` either way round: the instant's microseconds are
    /// unchanged, only the static type flips between timestamp and timestamptz.
    AtUtc(Box<CExpr>),
}

/// The functions the engine evaluates. Anything else is refused by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Func {
    Coalesce,
    NullIf,
    CharLength,
    Lower,
    Upper,
    Mod,
    Abs,
    Greatest,
    Least,
    Now,
}

/// Plan-time type of an expression. `None` is an untyped NULL or parameter,
/// which takes its type from context.
type Ty = Option<ColumnType>;

/// Compile `stmt` against `catalog`.
pub(crate) fn compile(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<Arc<Query>, ExecError> {
    let compiler = Compiler {
        catalog,
        default_schema,
        params: RefCell::new(Vec::new()),
        max_param: Cell::new(0),
    };
    let mut q = compiler.query(stmt, None, &[])?;
    q.params_needed = compiler.max_param.get();
    Ok(Arc::new(q))
}

/// The output columns of a compiled query.
pub(crate) fn columns(query: &Query) -> &[Column] {
    &query.columns
}

/// Infer each `$n`'s type from what it is compared, combined or cast with.
pub(crate) fn infer_params(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<Vec<ColumnType>, ExecError> {
    let compiler = Compiler {
        catalog,
        default_schema,
        params: RefCell::new(Vec::new()),
        max_param: Cell::new(0),
    };
    compiler.query(stmt, None, &[])?;
    Ok(compiler
        .params
        .into_inner()
        .into_iter()
        .map(|t| t.unwrap_or(ColumnType::Text))
        .collect())
}

struct Compiler<'c> {
    catalog: &'c dyn Catalog,
    default_schema: &'c str,
    /// Each `$n`'s type, as the first typed context it meets decides it.
    params: RefCell<Vec<Option<ColumnType>>>,
    max_param: Cell<usize>,
}

/// One relation visible to column references.
struct Binding {
    name: String,
    schema: RelSchema,
    base: usize,
}

/// The names a query's expressions resolve against, and the enclosing query's.
struct Scope<'p> {
    bindings: Vec<Binding>,
    parent: Option<&'p Scope<'p>>,
}

/// How an expression's column references and aggregates resolve.
enum Ctx<'a> {
    /// Over the source row; aggregates refused (`WHERE`, `ON`, plain projection).
    Row { clause: &'static str },
    /// Over the aggregate's output row.
    Post(&'a mut AggPlan),
}

/// What an aggregate query collects while its post-aggregate expressions compile.
struct AggPlan {
    /// Group keys as source-row slots (`GROUP BY` names columns only).
    groups: Vec<usize>,
    group_types: Vec<ColumnType>,
    /// Interned aggregates: function, argument slot in the source row, its type.
    aggs: Vec<(AggFunc, Option<usize>, Option<ColumnType>)>,
}

impl AggPlan {
    fn slot_of_group(&self, idx: usize) -> Option<usize> {
        self.groups.iter().position(|&g| g == idx)
    }

    fn intern(&mut self, func: AggFunc, arg: Option<usize>, ty: Option<ColumnType>) -> usize {
        let at = self
            .aggs
            .iter()
            .position(|(f, a, _)| *f == func && *a == arg)
            .unwrap_or_else(|| {
                self.aggs.push((func, arg, ty));
                self.aggs.len() - 1
            });
        self.groups.len() + at
    }
}

/// CTEs visible to a query: name and compiled query.
type Ctes = [(String, Arc<Query>)];

impl Compiler<'_> {
    fn query(
        &self,
        stmt: &SelectStmt,
        parent: Option<&Scope<'_>>,
        outer_ctes: &Ctes,
    ) -> Result<Query, ExecError> {
        if stmt.join.is_some() && !stmt.joins.is_empty() {
            return Err(ExecError::Unsupported("mixing join forms".into()));
        }
        let mut ctes: Vec<(String, Arc<Query>)> = outer_ctes.to_vec();
        for cte in &stmt.ctes {
            let q = self.query(&cte.query, None, &ctes)?;
            ctes.push((cte.name.clone(), Arc::new(q)));
        }

        let mut scope = Scope {
            bindings: Vec::new(),
            parent,
        };
        let mut sources = Vec::with_capacity(1 + stmt.joins.len());
        let mut needs_frame = false;
        let (table, schema) = self.source(&stmt.from, &ctes)?;
        self.bind(&mut scope, &stmt.from, schema)?;
        sources.push(Source {
            kind: JoinKind::Inner,
            width: scope.bindings[0].schema.width(),
            table,
            on: None,
        });
        for join in self.joins_of(stmt) {
            let (table, schema) = self.source(&join.table, &ctes)?;
            let width = schema.width();
            self.bind(&mut scope, &join.table, schema)?;
            let (on, _) = self.expr(&join.on, &scope, &ctes, &mut Ctx::Row { clause: "JOIN" })?;
            needs_frame |= has_subquery(&join.on);
            sources.push(Source {
                kind: join.kind,
                table,
                on: Some(on),
                width,
            });
        }

        for n in stmt.limit_param.iter().chain(stmt.offset_param.iter()) {
            self.max_param.set(self.max_param.get().max(*n));
            self.record(&CExpr::Param(*n), Some(ColumnType::BigInt));
        }
        let filter = match &stmt.filter {
            Some(f) => {
                needs_frame |= condition_has_subquery(f);
                Some(self.condition(f, &scope, &ctes, &mut Ctx::Row { clause: "WHERE" })?)
            }
            None => None,
        };

        let items = self.items(stmt, &scope);
        needs_frame |= items.iter().any(|(e, _)| has_subquery(e));
        let aggregate = !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || items.iter().any(|(e, _)| has_aggregate(e));
        let mut q = Query {
            sources,
            filter,
            mode: Mode::Plain {
                project: Vec::new(),
            },
            columns: Vec::new(),
            hidden: 0,
            order: Vec::new(),
            distinct: stmt.distinct,
            limit: stmt.limit,
            offset: stmt.offset,
            limit_param: stmt.limit_param,
            offset_param: stmt.offset_param,
            needs_frame,
            params_needed: 0,
        };
        if aggregate {
            self.aggregate(stmt, &scope, &ctes, items, &mut q)?;
        } else {
            self.plain(stmt, &scope, &ctes, items, &mut q)?;
        }
        Ok(q)
    }

    /// The general joins, or the legacy equi-join restated as one.
    fn joins_of(&self, stmt: &SelectStmt) -> Vec<crate::ast::JoinClause> {
        match &stmt.join {
            Some(j) => vec![crate::ast::JoinClause {
                kind: JoinKind::Inner,
                table: j.table.clone(),
                on: ScalarExpr::Binary {
                    op: BinOp::Cmp(CmpOp::Eq),
                    left: Box::new(ScalarExpr::Column(j.left.clone())),
                    right: Box::new(ScalarExpr::Column(j.right.clone())),
                },
            }],
            None => stmt.joins.clone(),
        }
    }

    fn source(&self, table: &TableRef, ctes: &Ctes) -> Result<(SourceTable, RelSchema), ExecError> {
        if table.schema.is_none() {
            if let Some((_, q)) = ctes.iter().rev().find(|(n, _)| *n == table.table) {
                let schema = RelSchema::new(q.columns.clone());
                return Ok((SourceTable::Cte(Arc::clone(q)), schema));
            }
        }
        let schema_name = table.schema.as_deref().unwrap_or(self.default_schema);
        let provider = self
            .catalog
            .resolve(schema_name, &table.table)
            .ok_or_else(|| ExecError::NoSuchTable {
                schema: schema_name.to_string(),
                table: table.table.clone(),
            })?;
        let schema = provider.schema().clone();
        Ok((SourceTable::Table(provider), schema))
    }

    fn bind(
        &self,
        scope: &mut Scope<'_>,
        table: &TableRef,
        schema: RelSchema,
    ) -> Result<(), ExecError> {
        let name = table.binding_name().to_string();
        if scope.bindings.iter().any(|b| b.name == name) {
            return Err(ExecError::eval(
                "42712",
                format!("table name \"{name}\" specified more than once"),
            ));
        }
        let base = scope
            .bindings
            .last()
            .map_or(0, |b| b.base + b.schema.width());
        scope.bindings.push(Binding { name, schema, base });
        Ok(())
    }

    /// The SELECT list as expressions with output names (`*` expands).
    fn items(&self, stmt: &SelectStmt, scope: &Scope<'_>) -> Vec<(ScalarExpr, String)> {
        match &stmt.projection {
            Projection::Star => scope
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
            Projection::Items(items) => items.iter().map(item_expr).collect(),
        }
    }

    fn plain(
        &self,
        stmt: &SelectStmt,
        scope: &Scope<'_>,
        ctes: &Ctes,
        items: Vec<(ScalarExpr, String)>,
        q: &mut Query,
    ) -> Result<(), ExecError> {
        let mut project = Vec::with_capacity(items.len());
        let mut ctx = Ctx::Row { clause: "SELECT" };
        for (expr, name) in &items {
            let (c, ty) = self.expr(expr, scope, ctes, &mut ctx)?;
            project.push(c);
            q.columns
                .push(Column::new(name.clone(), ty.unwrap_or(ColumnType::Text)));
        }
        for item in &stmt.order_by {
            let col = match self.output_slot(&item.column, q)? {
                Some(i) => i,
                None => {
                    self.refuse_hidden_under_distinct(q, &item.column)?;
                    let (c, _) = self.column(&item.column, scope)?;
                    project.push(c);
                    q.hidden += 1;
                    project.len() - 1
                }
            };
            q.order.push(SortKey { col, dir: item.dir });
        }
        q.mode = Mode::Plain { project };
        Ok(())
    }

    fn aggregate(
        &self,
        stmt: &SelectStmt,
        scope: &Scope<'_>,
        ctes: &Ctes,
        items: Vec<(ScalarExpr, String)>,
        q: &mut Query,
    ) -> Result<(), ExecError> {
        let mut plan = AggPlan {
            groups: Vec::with_capacity(stmt.group_by.len()),
            group_types: Vec::with_capacity(stmt.group_by.len()),
            aggs: Vec::new(),
        };
        for g in &stmt.group_by {
            let (idx, ty) = local_column(scope, g)?;
            plan.groups.push(idx);
            plan.group_types.push(ty);
        }
        let mut project = Vec::with_capacity(items.len());
        for (expr, name) in &items {
            let (c, ty) = self.expr(expr, scope, ctes, &mut Ctx::Post(&mut plan))?;
            project.push(c);
            q.columns
                .push(Column::new(name.clone(), ty.unwrap_or(ColumnType::Text)));
        }
        let having = match &stmt.having {
            Some(h) => {
                q.needs_frame |= condition_has_subquery(h);
                Some(self.condition(h, scope, ctes, &mut Ctx::Post(&mut plan))?)
            }
            None => None,
        };
        for item in &stmt.order_by {
            let col = match self.output_slot(&item.column, q)? {
                Some(i) => i,
                None => {
                    self.refuse_hidden_under_distinct(q, &item.column)?;
                    let (c, _) = self.expr(
                        &ScalarExpr::Column(item.column.clone()),
                        scope,
                        ctes,
                        &mut Ctx::Post(&mut plan),
                    )?;
                    project.push(c);
                    q.hidden += 1;
                    project.len() - 1
                }
            };
            q.order.push(SortKey { col, dir: item.dir });
        }
        let groups = plan.groups.len();
        let mut pre: Vec<CExpr> = plan
            .groups
            .iter()
            .map(|&idx| CExpr::Col { depth: 0, idx })
            .collect();
        let mut aggs = Vec::with_capacity(plan.aggs.len());
        for (func, arg, ty) in plan.aggs {
            let slot = arg.map(|idx| {
                pre.push(CExpr::Col { depth: 0, idx });
                pre.len() - 1
            });
            aggs.push(AggSpec {
                func,
                arg: slot,
                arg_type: ty,
            });
        }
        q.mode = Mode::Aggregate {
            pre,
            groups,
            aggs,
            having,
            project,
        };
        Ok(())
    }

    fn refuse_hidden_under_distinct(&self, q: &Query, col: &ColumnRef) -> Result<(), ExecError> {
        if q.distinct {
            return Err(ExecError::eval(
                "42P10",
                format!(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list: {}",
                    col.qualified_name()
                ),
            ));
        }
        Ok(())
    }

    /// An `ORDER BY` key naming an output column (by name or ordinal).
    fn output_slot(&self, col: &ColumnRef, q: &Query) -> Result<Option<usize>, ExecError> {
        if col.qualifier.is_some() {
            return Ok(None);
        }
        if let Ok(n) = col.name.parse::<usize>() {
            return if (1..=q.columns.len()).contains(&n) {
                Ok(Some(n - 1))
            } else {
                Err(ExecError::InvalidOrderBy(col.name.clone()))
            };
        }
        let mut hits = q
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.name == col.name);
        match (hits.next(), hits.next()) {
            (Some((i, _)), None) => Ok(Some(i)),
            (Some(_), Some(_)) => Err(ExecError::AmbiguousColumn(col.name.clone())),
            _ => Ok(None),
        }
    }

    /// A legacy-shaped condition (`Compare`/`And`/`Or`/`Not`/`Scalar`).
    fn condition(
        &self,
        expr: &Expr,
        scope: &Scope<'_>,
        ctes: &Ctes,
        ctx: &mut Ctx<'_>,
    ) -> Result<CExpr, ExecError> {
        Ok(match expr {
            Expr::And(a, b) => CExpr::And(
                Box::new(self.condition(a, scope, ctes, ctx)?),
                Box::new(self.condition(b, scope, ctes, ctx)?),
            ),
            Expr::Or(a, b) => CExpr::Or(
                Box::new(self.condition(a, scope, ctes, ctx)?),
                Box::new(self.condition(b, scope, ctes, ctx)?),
            ),
            Expr::Not(a) => CExpr::Not(Box::new(self.condition(a, scope, ctes, ctx)?)),
            Expr::Scalar(e) => self.expr(e, scope, ctes, ctx)?.0,
            Expr::Compare { left, op, value } => {
                let left = match left {
                    Operand::Column(c) => ScalarExpr::Column(c.clone()),
                    Operand::Aggregate { func, arg } => ScalarExpr::Aggregate {
                        func: *func,
                        arg: arg.clone(),
                    },
                };
                let (l, lt) = self.expr(&left, scope, ctes, ctx)?;
                let r = term(value)?;
                if let CExpr::Param(n) = r {
                    self.max_param.set(self.max_param.get().max(n));
                }
                self.record(&r, lt);
                CExpr::Cmp(*op, Box::new(l), Box::new(r))
            }
        })
    }

    /// Compile one expression, returning it with its type. The parser bounds the
    /// tree's depth; the stack grows on demand so debug-build frames cannot
    /// overflow a worker stack within that bound.
    fn expr(
        &self,
        expr: &ScalarExpr,
        scope: &Scope<'_>,
        ctes: &Ctes,
        ctx: &mut Ctx<'_>,
    ) -> Result<(CExpr, Ty), ExecError> {
        stacker::maybe_grow(64 * 1024, 1024 * 1024, || {
            self.expr_node(expr, scope, ctes, ctx)
        })
    }

    fn expr_node(
        &self,
        expr: &ScalarExpr,
        scope: &Scope<'_>,
        ctes: &Ctes,
        ctx: &mut Ctx<'_>,
    ) -> Result<(CExpr, Ty), ExecError> {
        let sub = |e: &ScalarExpr, ctx: &mut Ctx<'_>| self.expr(e, scope, ctes, ctx);
        Ok(match expr {
            ScalarExpr::Column(col) => self.column_in(col, scope, ctx)?,
            ScalarExpr::Literal(v) => (CExpr::Lit(v.clone()), literal_type(v)),
            ScalarExpr::Param(n) => {
                self.max_param.set(self.max_param.get().max(*n));
                (CExpr::Param(*n), None)
            }
            ScalarExpr::Cast { expr, ty } => {
                let target = cast_type(ty)?;
                let (inner, _) = sub(expr, ctx)?;
                self.record(&inner, Some(target));
                (CExpr::Cast(Box::new(inner), target), Some(target))
            }
            ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => {
                let (inner, ty) = sub(expr, ctx)?;
                let ty = ty.unwrap_or(ColumnType::Int);
                (CExpr::Neg(Box::new(inner), ty), Some(ty))
            }
            ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr,
            } => (
                CExpr::Not(Box::new(sub(expr, ctx)?.0)),
                Some(ColumnType::Bool),
            ),
            ScalarExpr::Binary { op, left, right } => {
                let (l, lt) = sub(left, ctx)?;
                let (r, rt) = sub(right, ctx)?;
                if !matches!(op, BinOp::And | BinOp::Or | BinOp::Concat) {
                    self.record(&l, rt);
                    self.record(&r, lt);
                }
                binary(*op, l, lt, r, rt)?
            }
            ScalarExpr::IsNull { expr, negated } => (
                CExpr::IsNull(Box::new(sub(expr, ctx)?.0), *negated),
                Some(ColumnType::Bool),
            ),
            ScalarExpr::InList {
                expr,
                list,
                negated,
            } => {
                let (e, et) = sub(expr, ctx)?;
                let mut items = Vec::with_capacity(list.len());
                for x in list {
                    let (c, ct) = sub(x, ctx)?;
                    self.record(&c, et);
                    self.record(&e, ct);
                    items.push(c);
                }
                let list = items;
                (
                    CExpr::InList {
                        expr: Box::new(e),
                        list,
                        negated: *negated,
                    },
                    Some(ColumnType::Bool),
                )
            }
            ScalarExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let (e, et) = sub(expr, ctx)?;
                let (lo, lot) = sub(low, ctx)?;
                let (hi, hit) = sub(high, ctx)?;
                self.record(&lo, et);
                self.record(&hi, et);
                self.record(&e, lot.or(hit));
                (
                    CExpr::Between {
                        expr: Box::new(e),
                        low: Box::new(lo),
                        high: Box::new(hi),
                        negated: *negated,
                    },
                    Some(ColumnType::Bool),
                )
            }
            ScalarExpr::Any { left, op, right } => {
                let (l, lt) = sub(left, ctx)?;
                let (r, rt) = sub(right, ctx)?;
                if let Some(t) = lt {
                    self.record(&r, array_elem(t).ok().map(ColumnType::Array));
                }
                if let Some(ColumnType::Array(e)) = rt {
                    self.record(&l, Some(elem_type(e)));
                }
                (
                    CExpr::Any {
                        left: Box::new(l),
                        op: *op,
                        right: Box::new(r),
                    },
                    Some(ColumnType::Bool),
                )
            }
            ScalarExpr::Func { name, args } => {
                let mut compiled = Vec::with_capacity(args.len());
                let mut types = Vec::with_capacity(args.len());
                for a in args {
                    let (c, t) = sub(a, ctx)?;
                    compiled.push(c);
                    types.push(t);
                }
                let (func, ty) = function(name, &types)?;
                (CExpr::Func(func, compiled), ty)
            }
            ScalarExpr::Aggregate { func, arg } => self.aggregate_ref(*func, arg, scope, ctx)?,
            ScalarExpr::Case {
                operand,
                whens,
                otherwise,
            } => {
                let operand = match operand {
                    Some(o) => Some(Box::new(sub(o, ctx)?.0)),
                    None => None,
                };
                let mut ty: Ty = None;
                let mut arms = Vec::with_capacity(whens.len());
                for (w, t) in whens {
                    let (w, _) = sub(w, ctx)?;
                    let (t, tt) = sub(t, ctx)?;
                    ty = ty.or(tt);
                    arms.push((w, t));
                }
                let otherwise = match otherwise {
                    Some(o) => {
                        let (o, ot) = sub(o, ctx)?;
                        ty = ty.or(ot);
                        Some(Box::new(o))
                    }
                    None => None,
                };
                (
                    CExpr::Case {
                        operand,
                        whens: arms,
                        otherwise,
                    },
                    ty,
                )
            }
            ScalarExpr::Exists { query, negated } => {
                let q = self.query(query, Some(scope), ctes)?;
                (CExpr::Exists(Arc::new(q), *negated), Some(ColumnType::Bool))
            }
            ScalarExpr::Subquery(query) => {
                let q = self.query(query, Some(scope), ctes)?;
                let ty = single_column(&q, "subquery")?;
                (CExpr::Subquery(Arc::new(q)), Some(ty))
            }
            ScalarExpr::ArraySubquery(query) => {
                let q = self.query(query, Some(scope), ctes)?;
                let elem = array_elem(single_column(&q, "ARRAY subquery")?)?;
                (
                    CExpr::ArraySubquery(Arc::new(q), elem),
                    Some(ColumnType::Array(elem)),
                )
            }
            ScalarExpr::Array(items) => {
                let mut compiled = Vec::with_capacity(items.len());
                let mut ty: Ty = None;
                for i in items {
                    let (c, t) = sub(i, ctx)?;
                    ty = ty.or(t);
                    compiled.push(c);
                }
                let elem = array_elem(ty.unwrap_or(ColumnType::Text))?;
                (CExpr::Array(compiled, elem), Some(ColumnType::Array(elem)))
            }
            ScalarExpr::AtTimeZone { expr, zone } => {
                require_utc(zone)?;
                let (inner, ty) = sub(expr, ctx)?;
                match ty {
                    Some(ColumnType::Timestamp) => {
                        (CExpr::AtUtc(Box::new(inner)), Some(ColumnType::TimestampTz))
                    }
                    Some(ColumnType::TimestampTz) => {
                        (CExpr::AtUtc(Box::new(inner)), Some(ColumnType::Timestamp))
                    }
                    other => {
                        return Err(ExecError::eval(
                            "42883",
                            format!("AT TIME ZONE needs a timestamp, not {other:?}"),
                        ))
                    }
                }
            }
        })
    }

    fn aggregate_ref(
        &self,
        func: AggFunc,
        arg: &AggArg,
        scope: &Scope<'_>,
        ctx: &mut Ctx<'_>,
    ) -> Result<(CExpr, Ty), ExecError> {
        let Ctx::Post(plan) = ctx else {
            let clause = match ctx {
                Ctx::Row { clause } => *clause,
                Ctx::Post(_) => unreachable!(),
            };
            return Err(ExecError::AggregateInWhere(format!("{func:?} in {clause}")));
        };
        let (arg, arg_ty) = match arg {
            AggArg::Star => (None, None),
            AggArg::Column(c) => {
                let (idx, ty) = local_column(scope, c)?;
                (Some(idx), Some(ty))
            }
        };
        let slot = plan.intern(func, arg, arg_ty);
        let ty = aggregate_type(func, arg_ty);
        Ok((
            CExpr::Col {
                depth: 0,
                idx: slot,
            },
            Some(ty),
        ))
    }

    /// A column reference under `ctx`: a source slot, or (after aggregation) a
    /// group-key slot. A reference to an enclosing query reads its row either way.
    fn column_in(
        &self,
        col: &ColumnRef,
        scope: &Scope<'_>,
        ctx: &mut Ctx<'_>,
    ) -> Result<(CExpr, Ty), ExecError> {
        let (c, ty) = self.column(col, scope)?;
        match (c, ctx) {
            (CExpr::Col { depth: 0, idx }, Ctx::Post(plan)) => {
                let slot = plan
                    .slot_of_group(idx)
                    .ok_or_else(|| ExecError::NotGrouped(col.qualified_name()))?;
                Ok((
                    CExpr::Col {
                        depth: 0,
                        idx: slot,
                    },
                    Some(plan.group_types[slot]),
                ))
            }
            (c, _) => Ok((c, ty)),
        }
    }

    /// When `expr` is a bare `$n`, note `ty` as its type (first context wins).
    fn record(&self, expr: &CExpr, ty: Ty) {
        let (CExpr::Param(n), Some(ty)) = (expr, ty) else {
            return;
        };
        let mut params = self.params.borrow_mut();
        if params.len() < *n {
            params.resize(*n, None);
        }
        params[n - 1].get_or_insert(ty);
    }

    /// Resolve a column through the scope chain.
    fn column(&self, col: &ColumnRef, scope: &Scope<'_>) -> Result<(CExpr, Ty), ExecError> {
        let mut depth = 0;
        let mut at = Some(scope);
        let mut first_err = None;
        while let Some(s) = at {
            match local_column(s, col) {
                Ok((idx, ty)) => return Ok((CExpr::Col { depth, idx }, Some(ty))),
                Err(e @ ExecError::AmbiguousColumn(_)) => return Err(e),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
            at = s.parent;
            depth += 1;
        }
        Err(first_err.expect("a scope chain has at least one scope"))
    }
}

/// Resolve a column in one scope only.
fn local_column(scope: &Scope<'_>, col: &ColumnRef) -> Result<(usize, ColumnType), ExecError> {
    let mut found = None;
    for b in &scope.bindings {
        if col.qualifier.as_ref().is_some_and(|q| *q != b.name) {
            continue;
        }
        if let Some(local) = b.schema.index_of(&col.name) {
            if found.is_some() {
                return Err(ExecError::AmbiguousColumn(col.qualified_name()));
            }
            found = Some((b.base + local, b.schema.columns[local].ty));
        }
    }
    if found.is_none() {
        if let Some(q) = &col.qualifier {
            if !scope.bindings.iter().any(|b| b.name == *q) {
                return Err(ExecError::UnknownQualifier(q.clone()));
            }
        }
    }
    found.ok_or_else(|| ExecError::NoSuchColumn(col.qualified_name()))
}

fn item_expr(item: &SelectItem) -> (ScalarExpr, String) {
    match item {
        SelectItem::Column { col, alias } => (
            ScalarExpr::Column(col.clone()),
            alias.clone().unwrap_or_else(|| col.name.clone()),
        ),
        SelectItem::Aggregate { func, arg, alias } => (
            ScalarExpr::Aggregate {
                func: *func,
                arg: arg.clone(),
            },
            alias
                .clone()
                .unwrap_or_else(|| aggregate_name(*func).to_string()),
        ),
        SelectItem::Expr { expr, alias } => (
            expr.clone(),
            alias.clone().unwrap_or_else(|| default_name(expr)),
        ),
    }
}

/// PostgreSQL's default output name for an unaliased expression.
fn default_name(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Column(c) => c.name.clone(),
        ScalarExpr::Func { name, .. } => name.rsplit('.').next().unwrap_or(name).to_string(),
        ScalarExpr::Aggregate { func, .. } => aggregate_name(*func).to_string(),
        ScalarExpr::Cast { expr, ty } => match &**expr {
            ScalarExpr::Column(_) | ScalarExpr::Func { .. } => default_name(expr),
            _ => ty.name.clone(),
        },
        ScalarExpr::Case { .. } => "case".into(),
        ScalarExpr::Exists { .. } => "exists".into(),
        ScalarExpr::Array(_) | ScalarExpr::ArraySubquery(_) => "array".into(),
        _ => "?column?".into(),
    }
}

fn aggregate_name(func: AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Avg => "avg",
    }
}

/// PostgreSQL's aggregate result types (the legacy planner's rule).
fn aggregate_type(func: AggFunc, arg: Option<ColumnType>) -> ColumnType {
    let arg = arg.unwrap_or(ColumnType::BigInt);
    match func {
        AggFunc::Count => ColumnType::BigInt,
        AggFunc::Sum => match arg {
            ColumnType::Int => ColumnType::BigInt,
            ColumnType::Float => ColumnType::Float,
            _ => ColumnType::Numeric,
        },
        AggFunc::Avg => match arg {
            ColumnType::Float => ColumnType::Float,
            _ => ColumnType::Numeric,
        },
        AggFunc::Min | AggFunc::Max => arg,
    }
}

fn term(t: &Term) -> Result<CExpr, ExecError> {
    match t {
        Term::Literal(v) => Ok(CExpr::Lit(v.clone())),
        Term::Param(n) => Ok(CExpr::Param(*n)),
        Term::Cast { ty, .. } => Err(ExecError::UnresolvedCast(*ty)),
    }
}

fn literal_type(v: &Value) -> Ty {
    match v {
        Value::Null => None,
        Value::Int(n) if i32::try_from(*n).is_ok() => Some(ColumnType::Int),
        Value::Int(_) => Some(ColumnType::BigInt),
        Value::Text(_) => Some(ColumnType::Text),
        Value::Bool(_) => Some(ColumnType::Bool),
        Value::Float(_) => Some(ColumnType::Float),
        Value::Numeric { .. } => Some(ColumnType::Numeric),
        Value::Uuid(_) => Some(ColumnType::Uuid),
        Value::Timestamp(_) => Some(ColumnType::Timestamp),
        Value::Date(_) => Some(ColumnType::Date),
        Value::Time(_) => Some(ColumnType::Time),
        Value::Array(a) => Some(ColumnType::Array(a.elem)),
        _ => None,
    }
}

/// Whether the engine implements a cast to `ty` (`::regclass` is the front end's).
pub fn cast_type_supported(ty: &TypeName) -> bool {
    let regclass = !ty.array
        && ty.name.eq_ignore_ascii_case("regclass")
        && ty
            .schema
            .as_deref()
            .is_none_or(|s| s.eq_ignore_ascii_case("pg_catalog"));
    regclass || cast_type(ty).is_ok()
}

/// The type a `::name` cast produces. A name outside this set is refused.
fn cast_type(ty: &TypeName) -> Result<ColumnType, ExecError> {
    if let Some(schema) = &ty.schema {
        if !schema.eq_ignore_ascii_case("pg_catalog") {
            return Err(ExecError::Unsupported(format!(
                "cast to {schema}.{}",
                ty.name
            )));
        }
    }
    let base = scalar_type(&ty.name)
        .ok_or_else(|| ExecError::Unsupported(format!("cast to {}", ty.name)))?;
    if ty.array {
        Ok(ColumnType::Array(array_elem(base)?))
    } else {
        Ok(base)
    }
}

fn scalar_type(name: &str) -> Option<ColumnType> {
    Some(match name.to_ascii_lowercase().as_str() {
        "int" | "int4" | "integer" | "int2" | "smallint" => ColumnType::Int,
        "int8" | "bigint" => ColumnType::BigInt,
        "oid" => ColumnType::Oid,
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => {
            ColumnType::Text
        }
        "bool" | "boolean" => ColumnType::Bool,
        "float8" | "float4" | "double precision" | "real" | "float" => ColumnType::Float,
        "numeric" | "decimal" => ColumnType::Numeric,
        "uuid" => ColumnType::Uuid,
        "timestamp" | "timestamp without time zone" => ColumnType::Timestamp,
        "timestamptz" | "timestamp with time zone" => ColumnType::TimestampTz,
        "date" => ColumnType::Date,
        "jsonb" => ColumnType::Jsonb,
        "json" => ColumnType::Json,
        _ => return None,
    })
}

/// The element type of an array of `ty`; an array of anything else is refused.
fn array_elem(ty: ColumnType) -> Result<ArrayElem, ExecError> {
    Ok(match ty {
        ColumnType::Int => ArrayElem::Int,
        ColumnType::BigInt => ArrayElem::BigInt,
        ColumnType::Oid => ArrayElem::Oid,
        ColumnType::Text => ArrayElem::Text,
        ColumnType::Bool => ArrayElem::Bool,
        ColumnType::Float => ArrayElem::Float,
        ColumnType::Uuid => ArrayElem::Uuid,
        ColumnType::Timestamp | ColumnType::TimestampTz => ArrayElem::Timestamp,
        ColumnType::Numeric => ArrayElem::Numeric,
        ColumnType::Jsonb => ArrayElem::Jsonb,
        other => return Err(ExecError::Unsupported(format!("an array of {other:?}"))),
    })
}

/// The scalar type of an array's elements.
fn elem_type(elem: ArrayElem) -> ColumnType {
    match elem {
        ArrayElem::Int => ColumnType::Int,
        ArrayElem::BigInt => ColumnType::BigInt,
        ArrayElem::Oid => ColumnType::Oid,
        ArrayElem::Text => ColumnType::Text,
        ArrayElem::Bool => ColumnType::Bool,
        ArrayElem::Float => ColumnType::Float,
        ArrayElem::Uuid => ColumnType::Uuid,
        ArrayElem::Timestamp => ColumnType::Timestamp,
        ArrayElem::Numeric => ColumnType::Numeric,
        ArrayElem::Jsonb => ColumnType::Jsonb,
    }
}

fn single_column(q: &Query, what: &str) -> Result<ColumnType, ExecError> {
    match q.columns.as_slice() {
        [c] => Ok(c.ty),
        _ => Err(ExecError::eval(
            "42601",
            format!("{what} must return only one column"),
        )),
    }
}

fn require_utc(zone: &ScalarExpr) -> Result<(), ExecError> {
    match zone {
        ScalarExpr::Literal(Value::Text(z)) if is_utc(z.as_str()) => Ok(()),
        _ => Err(ExecError::Unsupported(
            "AT TIME ZONE other than a literal 'UTC'".into(),
        )),
    }
}

pub(crate) fn is_utc(zone: &str) -> bool {
    matches!(
        zone.to_ascii_uppercase().as_str(),
        "UTC" | "ETC/UTC" | "Z" | "ZULU" | "GMT"
    )
}

/// Combine two typed operands.
fn binary(op: BinOp, l: CExpr, lt: Ty, r: CExpr, rt: Ty) -> Result<(CExpr, Ty), ExecError> {
    let (l, r) = (Box::new(l), Box::new(r));
    Ok(match op {
        BinOp::And => (CExpr::And(l, r), Some(ColumnType::Bool)),
        BinOp::Or => (CExpr::Or(l, r), Some(ColumnType::Bool)),
        BinOp::Cmp(c) => (CExpr::Cmp(c, l, r), Some(ColumnType::Bool)),
        BinOp::Concat => (
            CExpr::Arith {
                op,
                left: l,
                right: r,
                ty: ColumnType::Text,
            },
            Some(ColumnType::Text),
        ),
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let ty = numeric_result(lt, rt)?;
            (
                CExpr::Arith {
                    op,
                    left: l,
                    right: r,
                    ty,
                },
                Some(ty),
            )
        }
    })
}

/// PostgreSQL's numeric promotion: int4 < int8 < numeric < float8.
fn numeric_result(l: Ty, r: Ty) -> Result<ColumnType, ExecError> {
    fn rank(t: ColumnType) -> Option<u8> {
        match t {
            ColumnType::Int => Some(0),
            ColumnType::BigInt => Some(1),
            ColumnType::Numeric => Some(2),
            ColumnType::Float => Some(3),
            _ => None,
        }
    }
    let pick = |t: Ty| -> Result<Option<u8>, ExecError> {
        match t {
            None => Ok(None),
            Some(t) => rank(t).map(Some).ok_or_else(|| {
                ExecError::eval("42883", format!("operator does not exist for type {t:?}"))
            }),
        }
    };
    let rank = pick(l)?.max(pick(r)?).unwrap_or(0);
    Ok([
        ColumnType::Int,
        ColumnType::BigInt,
        ColumnType::Numeric,
        ColumnType::Float,
    ][rank as usize])
}

/// Resolve a function by name, with its result type.
fn function(name: &str, args: &[Ty]) -> Result<(Func, Ty), ExecError> {
    let bare = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let arity = |want: std::ops::RangeInclusive<usize>| -> Result<(), ExecError> {
        if want.contains(&args.len()) {
            Ok(())
        } else {
            Err(ExecError::eval(
                "42883",
                format!("function {bare} does not take {} arguments", args.len()),
            ))
        }
    };
    let first_typed = || args.iter().flatten().next().copied();
    Ok(match bare {
        "coalesce" => {
            arity(1..=usize::MAX)?;
            (Func::Coalesce, first_typed())
        }
        "nullif" => {
            arity(2..=2)?;
            (Func::NullIf, args[0].or(args[1]))
        }
        "char_length" | "character_length" | "length" => {
            arity(1..=1)?;
            (Func::CharLength, Some(ColumnType::Int))
        }
        "lower" => {
            arity(1..=1)?;
            (Func::Lower, Some(ColumnType::Text))
        }
        "upper" => {
            arity(1..=1)?;
            (Func::Upper, Some(ColumnType::Text))
        }
        "mod" => {
            arity(2..=2)?;
            (Func::Mod, Some(numeric_result(args[0], args[1])?))
        }
        "abs" => {
            arity(1..=1)?;
            (Func::Abs, args[0])
        }
        "greatest" => {
            arity(1..=usize::MAX)?;
            (Func::Greatest, first_typed())
        }
        "least" => {
            arity(1..=usize::MAX)?;
            (Func::Least, first_typed())
        }
        "now" | "transaction_timestamp" | "statement_timestamp" | "current_timestamp" => {
            arity(0..=0)?;
            (Func::Now, Some(ColumnType::TimestampTz))
        }
        _ => {
            return Err(ExecError::eval(
                "42883",
                format!("function {name} is not supported by the SQL engine"),
            ))
        }
    })
}

fn has_aggregate(e: &ScalarExpr) -> bool {
    match e {
        ScalarExpr::Aggregate { .. } => true,
        _ => {
            let mut found = false;
            e.for_each_child(|c| found = found || has_aggregate(c));
            found
        }
    }
}

fn has_subquery(e: &ScalarExpr) -> bool {
    match e {
        ScalarExpr::Exists { .. } | ScalarExpr::Subquery(_) | ScalarExpr::ArraySubquery(_) => true,
        _ => {
            let mut found = false;
            e.for_each_child(|c| found = found || has_subquery(c));
            found
        }
    }
}

fn condition_has_subquery(e: &Expr) -> bool {
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => condition_has_subquery(a) || condition_has_subquery(b),
        Expr::Not(a) => condition_has_subquery(a),
        Expr::Scalar(s) => has_subquery(s),
        Expr::Compare { .. } => false,
    }
}
