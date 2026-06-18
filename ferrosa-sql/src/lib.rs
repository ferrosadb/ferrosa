//! Bespoke relational query engine for Ferrosa (decision D3).
//!
//! No DataFusion / Arrow: this crate owns the value/row model, a `TableProvider`
//! scan contract, and the physical operators (scan, filter, project, hash join)
//! that the Postgres front-end's queries lower onto. The first slice covers
//! single-table scans and an inner equi-join — the operators behind the M1
//! first-JOIN milestone — built TDD against an in-memory provider. The
//! storage-backed provider and the SQL parser/planner land on top.

pub mod ast;
pub mod catalog;
pub mod exec;
pub mod parser;
pub mod plan;
pub mod provider;
pub mod types;

pub use ast::SelectStmt;
pub use ast::{AggArg, Expr, Operand, OrderItem, Projection, SelectItem, Term};
pub use catalog::{Catalog, MapCatalog, SharedTable};
pub use exec::{
    filter, hash_aggregate, hash_join, limit_offset, project, seq_scan, sort, AggFunc, CmpOp,
    Predicate, RowStream, SortDir, SortKey,
};
pub use parser::{parse, ParseError};
pub use plan::{describe, execute, infer_param_types, ExecError, QueryResult};
pub use provider::{InMemoryTable, TableProvider};
pub use types::{Column, ColumnType, RelSchema, Row, Value};
