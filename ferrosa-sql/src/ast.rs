//! Logical AST for the M1 SQL subset: `SELECT <list|*> FROM t [alias]
//! [INNER JOIN t2 [alias] ON a.x = b.y] [WHERE a.x <op> <literal>]`.

use crate::exec::CmpOp;
use crate::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
    pub projection: Projection,
    pub from: TableRef,
    pub join: Option<Join>,
    pub filter: Option<Filter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// `SELECT *`
    Star,
    /// `SELECT a, b.c, ...`
    Columns(Vec<ColumnRef>),
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
