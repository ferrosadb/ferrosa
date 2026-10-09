//! Logical AST for the SQL subset: `SELECT [DISTINCT] <list|*> FROM t [alias]
//! [INNER JOIN t2 [alias] ON a.x = b.y] [WHERE <bool-expr>]
//! [GROUP BY ...] [HAVING <bool-expr>] [ORDER BY ... [ASC|DESC]]
//! [LIMIT n] [OFFSET m]`, plus the no-`FROM` expression select
//! `SELECT <scalar> [, <scalar>]*` (see [`ScalarItem`]).
//!
//! The `SELECT` list itself is deliberately NOT an expression grammar. It holds
//! columns and aggregates (a table query) or [`ScalarValue`]s (an expression
//! query), and the ONE operator either will accept is `||` string concatenation.
//! Every other select-list expression form is refused by name at parse time.

use crate::exec::{AggFunc, CmpOp, SortDir};
use crate::types::Value;

/// PostgreSQL transaction isolation level requested by `BEGIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

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
    /// `SELECT $1`, `SELECT 'a' || 'b'`) — yields exactly one row.
    SelectExprs(Vec<ScalarItem>),
    /// `BEGIN` / `START TRANSACTION`, preserving an explicitly requested
    /// isolation level. `None` means the session default.
    Begin { isolation: Option<IsolationLevel> },
    /// `COMMIT` / `END`.
    Commit,
    /// `ROLLBACK` / `ABORT`.
    Rollback,
    /// `SET <name> [=|TO] <value>`.
    Set { name: String, value: String },
    /// `RESET <name>` (`RESET ALL` carries name `ALL`).
    Reset { name: String },
    /// `INSERT INTO t (cols) VALUES (...)`. Boxed for size parity with `Select`.
    Insert(Box<InsertStmt>),
    /// `UPDATE t SET ... WHERE ...`. Boxed for size parity with `Select`.
    Update(Box<UpdateStmt>),
    /// `DELETE FROM t WHERE ...`. Boxed for size parity with `Select`.
    Delete(Box<DeleteStmt>),
    /// `CREATE TABLE [IF NOT EXISTS] ...` (D10 Ecto-migration subset). Parse
    /// only: execution and schema creation are later packets. Boxed for size
    /// parity with `Select`.
    CreateTable(Box<CreateTableStmt>),
    /// `DROP TABLE [IF EXISTS] a [, b, ...]`. pgbench's initializer and any
    /// client-side schema reset drop several tables in one statement; each
    /// named table is dropped by the front-end. Boxed for size parity.
    DropTable(Box<DropTableStatement>),
    /// `ALTER TABLE t <operation>` (see [`AlterOperation`]).
    AlterTable(Box<AlterTableStmt>),
    /// `COPY t [(cols)] FROM STDIN [(options)]`.
    CopyFromStdin(Box<CopyFromStdinStmt>),
}

/// `DROP TABLE [IF EXISTS] a [, b, ...]`. `tables` is non-empty; every entry
/// names a table (optionally schema-qualified) to drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub tables: Vec<TableRef>,
}

/// `COPY <table> [(<cols>)] FROM STDIN [[WITH] (<options>)]`.
///
/// The payload does not appear here: it arrives afterwards, as `CopyData` frames, which is why
/// this is a statement the connection loop must drive rather than one that can be answered in a
/// single step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFromStdinStmt {
    pub table: TableRef,
    /// The columns named, or `None` meaning "the table's columns, in order".
    pub columns: Option<Vec<String>>,
    pub format: CopyFormatKind,
    /// `DELIMITER`, defaulted per format when absent.
    pub delimiter: Option<char>,
    /// `NULL`, defaulted per format when absent.
    pub null: Option<String>,
    /// `HEADER` — csv only.
    pub header: bool,
}

/// The `FORMAT` of a `COPY` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormatKind {
    /// PostgreSQL's default.
    Text,
    Csv,
}

/// `ALTER TABLE <table> <operation>`.
///
/// Only the operations ferrosa can actually apply are accepted. Every other `ALTER TABLE` form
/// is refused *by name* rather than parsed loosely: a change we do not implement must not look
/// like it took effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStmt {
    pub table: TableRef,
    pub operation: AlterOperation,
}

/// What an `ALTER TABLE` changes.
///
/// The set is exactly what ferrosa's schema layer can express — the operations of
/// `ferrosa_schema::TableUpdates` — plus the declared key, which is recorded in the table's
/// extensions. Forms that would need machinery ferrosa does not have (renaming, changing a
/// column's type, constraints other than a key) are refused, not approximated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterOperation {
    /// `ADD [CONSTRAINT <name>] PRIMARY KEY (<cols>)`.
    ///
    /// The **PostgreSQL** primary key, which is not necessarily the storage partition key: a
    /// table created without one has a synthetic `_sys_ck_` storage key and no PostgreSQL key at
    /// all until this runs. See `ferrosa-postgres`'s `pg_key`.
    AddPrimaryKey(Vec<String>),
    /// `ADD [COLUMN] <name> <type> [NOT NULL]`.
    AddColumn(ColumnDef),
    /// `DROP [COLUMN] <name>`.
    DropColumn(String),
}

/// `CREATE TABLE [IF NOT EXISTS] [public.]name (col type [NOT NULL]..., PRIMARY KEY (...))`.
///
/// `primary_key` is non-empty and every entry names a column in `columns`; the
/// first entry is the partition key and the rest are clustering columns (D10).
/// Key columns have `not_null` and `primary_key` set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStmt {
    pub if_not_exists: bool,
    pub name: TableRef,
    pub columns: Vec<ColumnDef>,
    /// The declared primary key, in key order. **Empty means the statement declared
    /// none** — PostgreSQL allows that, and the Postgres front-end supplies a synthetic
    /// key (see `ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN`) rather than the parser
    /// refusing the statement.
    pub primary_key: Vec<String>,
}

/// One column of a `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: PgType,
    pub not_null: bool,
    /// True when the column is part of the table's primary key.
    pub primary_key: bool,
}

/// A PostgreSQL column type accepted by `CREATE TABLE`, as written (aliases
/// normalized). Mapping to a storage type is the next packet's job, except
/// [`PgType::storage`], which encodes D11: `json` is stored as `jsonb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Boolean,
    Text,
    /// `varchar[(n)]` / `character varying[(n)]`.
    Varchar(Option<u32>),
    Bytea,
    Uuid,
    /// `timestamp [(p)] [without time zone]`.
    Timestamp,
    /// `timestamptz` / `timestamp [(p)] with time zone`.
    TimestampTz,
    Date,
    Time,
    Inet,
    Jsonb,
    /// `json`. Stored as `jsonb` (D11); see [`PgType::storage`].
    Json,
}

impl PgType {
    /// The type actually stored: `json` collapses to `jsonb` (D11), all else is
    /// itself.
    pub fn storage(self) -> PgType {
        match self {
            PgType::Json => PgType::Jsonb,
            PgType::SmallInt
            | PgType::Integer
            | PgType::BigInt
            | PgType::Real
            | PgType::DoublePrecision
            | PgType::Numeric { .. }
            | PgType::Boolean
            | PgType::Text
            | PgType::Varchar(_)
            | PgType::Bytea
            | PgType::Uuid
            | PgType::Timestamp
            | PgType::TimestampTz
            | PgType::Date
            | PgType::Time
            | PgType::Inet
            | PgType::Jsonb => self,
        }
    }
}

/// A DDL clause that is out of scope for the PG subset (D10). Each is refused
/// by name, never dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedClause {
    /// `FOREIGN KEY (...)` or a column `REFERENCES`.
    ForeignKey,
    /// `CHECK (...)`, table or column level.
    Check,
    /// `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` (implicit sequences).
    Serial,
    /// `DEFAULT <expr>`.
    DefaultExpr,
    /// A table schema other than the mapped one (`public`).
    ForeignSchema,
    /// `UNIQUE` constraints (not in the D10 subset).
    Unique,
}

impl UnsupportedClause {
    /// The clause as a user would write it, for error messages.
    pub fn name(self) -> &'static str {
        match self {
            UnsupportedClause::ForeignKey => "FOREIGN KEY",
            UnsupportedClause::Check => "CHECK",
            UnsupportedClause::Serial => "SERIAL",
            UnsupportedClause::DefaultExpr => "DEFAULT",
            UnsupportedClause::ForeignSchema => "schema other than public",
            UnsupportedClause::Unique => "UNIQUE",
        }
    }
}

/// `INSERT INTO [schema.]table (col, ...) VALUES (val, ...) [RETURNING ...]`.
/// Single-row, with literal or `$N` values (one per named column). The optional
/// `returning` clause names the columns to echo back (see [`Returning`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStmt {
    pub table: TableRef,
    pub columns: Vec<String>,
    /// One entry per `VALUES (…)` tuple; every inner vec has one value per
    /// column in `columns` (checked at parse time). A single-row INSERT is a
    /// one-element `rows`.
    pub rows: Vec<Vec<ScalarValue>>,
    pub returning: Option<Returning>,
}

/// `UPDATE [schema.]table SET col = val, ... WHERE col = val [AND ...]
/// [RETURNING ...]`. The `WHERE` is restricted to equality on key columns
/// (Cassandra-style upsert: the full primary key identifies the row);
/// `assignments` are the SET cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStmt {
    pub table: TableRef,
    pub assignments: Vec<(String, ScalarValue)>,
    pub where_eq: Vec<(String, ScalarValue)>,
    pub returning: Option<Returning>,
}

/// `DELETE FROM [schema.]table WHERE col = val [AND ...] [RETURNING ...]`.
/// Row-level delete; the equality `WHERE` supplies the full primary key
/// identifying the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStmt {
    pub table: TableRef,
    pub where_eq: Vec<(String, ScalarValue)>,
    pub returning: Option<Returning>,
}

/// A `RETURNING` clause: either `RETURNING *` (all of the table's columns, in
/// schema order — resolved at execute time) or an explicit list of column names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Returning {
    /// `RETURNING *` — every column of the target table.
    Star,
    /// `RETURNING col, col, ...` — the named columns, in the order written.
    Columns(Vec<String>),
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
    /// `<operand> || <operand>` — the SQL string-concatenation operator.
    ///
    /// Boxed to keep [`ScalarValue`] small (it is one variant of [`Statement`]'s
    /// largest arm). Each operand is itself a [`ScalarValue`], so `a || b || c`
    /// nests left-associatively. Concatenation is a **select-list** operator
    /// only: the WHERE/DML grammars never produce this variant, and `||` over a
    /// `FROM` relation is refused by name at parse time.
    Concat {
        left: Box<ScalarValue>,
        right: Box<ScalarValue>,
    },
}

impl ScalarValue {
    /// Whether this value references a `$N` parameter, at any depth.
    ///
    /// The extended-protocol `Parse` path refuses expression selects that carry
    /// a parameter (no column to infer its type from), so the check has to walk
    /// a concatenation rather than only looking at the top level.
    pub fn references_param(&self) -> bool {
        match self {
            ScalarValue::Param(_) => true,
            ScalarValue::Concat { left, right } => {
                left.references_param() || right.references_param()
            }
            ScalarValue::Literal(_) | ScalarValue::Func(_) => false,
        }
    }
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
