//! Logical AST for the SQL subset: `SELECT <list|*> FROM t [alias]
//! [INNER JOIN t2 [alias] ON a.x = b.y] [WHERE a.x <op> <literal>]
//! [GROUP BY ...] [ORDER BY ... [ASC|DESC]] [LIMIT n] [OFFSET m]`.

use crate::exec::{AggFunc, CmpOp, SortDir};
use crate::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
    pub projection: Projection,
    pub from: TableRef,
    pub join: Option<Join>,
    pub filter: Option<Filter>,
    pub group_by: Vec<ColumnRef>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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

/// A single-column comparison against a literal: `WHERE <column> <op> <value>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub column: ColumnRef,
    pub op: CmpOp,
    pub value: Value,
}
