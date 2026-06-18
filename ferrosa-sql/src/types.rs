//! Core value / row / schema types for the relational engine.

use std::cmp::Ordering;

/// A scalar column value. First-slice subset; widens toward the full Postgres
/// type system. `Eq`/`Hash` make values usable as join keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// SQL three-valued comparison: `None` (UNKNOWN) when either side is NULL or
    /// the types differ; callers treat UNKNOWN as "no match".
    pub fn sql_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// The schema of a relation (an ordered list of columns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelSchema {
    pub columns: Vec<Column>,
}

impl RelSchema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn width(&self) -> usize {
        self.columns.len()
    }
}

/// A row: positional values matching a [`RelSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Row(values)
    }

    pub fn get(&self, i: usize) -> &Value {
        &self.0[i]
    }
}
