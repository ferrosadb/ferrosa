//! Logical AST for the SQL subset: `SELECT [DISTINCT] <list|*> FROM t [alias]
//! [INNER JOIN t2 [alias] ON a.x = b.y] [WHERE <bool-expr>]
//! [GROUP BY ...] [HAVING <bool-expr>] [ORDER BY ... [ASC|DESC]]
//! [LIMIT n] [OFFSET m]`.

use crate::exec::{AggFunc, CmpOp, SortDir};
use crate::types::Value;

/// A parsed top-level SQL statement — the unit a Postgres-wire client sends.
///
/// `parse_statement` returns this; the legacy `parse` returns just the
/// [`SelectStmt`] for table queries (kept for callers that only do table
/// scans). Transaction-control and session statements are *parsed* here; the
/// front-end gives them their real semantics (transactions route through
/// Accord — they are never silently no-op'd).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// A table query: `SELECT ... FROM ...`. Boxed — `SelectStmt` is far larger
    /// than the other variants.
    Select(Box<SelectStmt>),
    /// A no-`FROM` expression query (`SELECT 1`, `SELECT version()`,
    /// `SELECT $1`) — yields exactly one row.
    SelectExprs(Vec<ScalarItem>),
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT` / `END`.
    Commit,
    /// `ROLLBACK` / `ABORT`.
    Rollback,
    /// `SET <name> [=|TO] <value>`.
    Set { name: String, value: String },
    /// `RESET <name>` (`RESET ALL` carries name `ALL`).
    Reset { name: String },
}

/// One projected scalar in a no-`FROM` SELECT, with an optional `AS` alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarItem {
    pub value: ScalarValue,
    pub alias: Option<String>,
}

/// A scalar value in a no-`FROM` SELECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    /// An inline literal (`1`, `'x'`, `TRUE`, `NULL`).
    Literal(Value),
    /// A bare (zero-arg) function call (`version()`, `current_database()`, …),
    /// carrying the uppercased function name. The front-end evaluates it (it
    /// owns the session context the function needs).
    Func(String),
    /// A `$N` parameter placeholder (extended-query path).
    Param(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
    pub distinct: bool,
    pub projection: Projection,
    pub from: TableRef,
    pub join: Option<Join>,
    /// `WHERE` boolean expression (operands must be plain columns).
    pub filter: Option<Expr>,
    pub group_by: Vec<ColumnRef>,
    /// `HAVING` boolean expression (operands may be columns or aggregates).
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// The right-hand side of a comparison: either an inline literal or a bound
/// parameter placeholder (`$N`, 1-based). Parameters are substituted with a
/// concrete [`Value`] at execute time (the prepared/extended-query path);
/// the simple-query path uses only literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Literal(Value),
    /// A `$N` placeholder, carrying the 1-based parameter index `N`.
    Param(usize),
}

/// A boolean WHERE/HAVING expression: comparisons combined with AND/OR/NOT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    /// A single comparison `<operand> <op> <term>`, where the term is a literal
    /// or a `$N` parameter placeholder.
    Compare {
        left: Operand,
        op: CmpOp,
        value: Term,
    },
}

/// The left-hand side of a comparison: a column reference or an aggregate call.
/// Aggregates are only legal in `HAVING`; an aggregate operand in `WHERE` is a
/// fail-loud error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    Column(ColumnRef),
    Aggregate { func: AggFunc, arg: AggArg },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// `SELECT *`
    Star,
    /// `SELECT a, COUNT(*), b.c, ...`
    Items(Vec<SelectItem>),
}

/// One entry in a non-star SELECT list: a plain column or an aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Column(ColumnRef),
    Aggregate { func: AggFunc, arg: AggArg },
}

/// The argument to an aggregate: `COUNT(*)` vs `FUNC(col)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggArg {
    Star,
    Column(ColumnRef),
}

/// One `ORDER BY` key. The column may also be an output name or ordinal in
/// aggregate mode; that resolution happens in the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderItem {
    pub column: ColumnRef,
    pub dir: SortDir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

impl TableRef {
    /// The name a column qualifier must match: the alias if present, else the table.
    pub fn binding_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    /// Optional `table`/`alias` qualifier (`u` in `u.name`).
    pub qualifier: Option<String>,
    pub name: String,
}

impl ColumnRef {
    /// Render as `qualifier.name` (or just `name`) for diagnostics.
    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{q}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// An inner equi-join: `JOIN <table> ON <left> = <right>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub table: TableRef,
    pub left: ColumnRef,
    pub right: ColumnRef,
}
