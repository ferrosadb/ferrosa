//! CQL abstract syntax tree types.
//!
//! These types represent parsed CQL statements. The parser produces
//! `Statement` values; the router dispatches them to schema/storage.

use std::time::Duration;
use uuid::Uuid;

/// Top-level parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Batch(BatchStatement),
    CreateKeyspace(CreateKeyspaceStatement),
    AlterKeyspace(AlterKeyspaceStatement),
    DropKeyspace(DropKeyspaceStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    CreateRole(CreateRoleStatement),
    AlterRole(AlterRoleStatement),
    DropRole(DropRoleStatement),
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    Use(UseStatement),
    Truncate(TruncateStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    Subscribe {
        inner: Box<Statement>,
        interval: Option<Duration>,
        delta: bool,
    },
    Unsubscribe {
        stream_id: Option<u16>,
    },
}

/// A value expression in DML statements.
///
/// Uses parser-level literal types (not CqlValue) because the parser
/// doesn't know the target column type at parse time. The bridge's
/// `term_to_cql_value()` handles type coercion using the target column's CqlType.
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    UuidLiteral(Uuid),
    BlobLiteral(Vec<u8>),
    BoolLiteral(bool),
    Null,
    BindMarker(Option<String>),
    InList(Vec<Term>),
    ListLiteral(Vec<Term>),
    MapLiteral(Vec<(Term, Term)>),
    SetLiteral(Vec<Term>),
    TupleLiteral(Vec<Term>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Lt,
    Gt,
    Le,
    Ge,
    In,
    Ne,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub op: ComparisonOp,
    pub value: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    Star,
    Column(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<SelectColumn>,
    pub where_clauses: Vec<WhereClause>,
    pub order_by: Vec<(String, OrderDirection)>,
    pub limit: Option<i32>,
    pub allow_filtering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Term>,
    pub if_not_exists: bool,
    pub using_timestamp: Option<i64>,
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub assignments: Vec<(String, Term)>,
    pub where_clauses: Vec<WhereClause>,
    pub if_exists: bool,
    pub using_timestamp: Option<i64>,
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub where_clauses: Vec<WhereClause>,
    pub if_exists: bool,
    pub using_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchType {
    Logged,
    Unlogged,
    Counter,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchStatement {
    pub batch_type: BatchType,
    pub statements: Vec<Statement>,
    pub using_timestamp: Option<i64>,
}

/// CQL type name as written in CREATE TABLE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlTypeName {
    Simple(String),
    List(Box<CqlTypeName>),
    Set(Box<CqlTypeName>),
    Map(Box<CqlTypeName>, Box<CqlTypeName>),
    Tuple(Vec<CqlTypeName>),
    Frozen(Box<CqlTypeName>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusteringOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub keyspace: Option<String>,
    pub name: String,
    pub columns: Vec<(String, CqlTypeName)>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<(String, ClusteringOrder)>,
    pub if_not_exists: bool,
    pub table_options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub add_columns: Vec<(String, CqlTypeName)>,
    pub drop_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub name: Option<String>,
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub using: Option<String>,
    pub filter: Option<String>,
    pub options: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStatement {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateKeyspaceStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub replication: Vec<(String, String)>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterKeyspaceStatement {
    pub name: String,
    pub replication: Option<Vec<(String, String)>>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropKeyspaceStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseStatement {
    pub keyspace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateStatement {
    pub keyspace: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoleStatement {
    pub name: String,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRoleStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantResource {
    AllKeyspaces,
    Keyspace(String),
    Table(Option<String>, String),
    AllRoles,
    Role(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantStatement {
    pub permissions: Vec<String>,
    pub resource: GrantResource,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeStatement {
    pub permissions: Vec<String>,
    pub resource: GrantResource,
    pub role: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_subscribe_statement() {
        let inner = Statement::Select(SelectStatement {
            keyspace: None,
            table: "users".to_string(),
            columns: vec![SelectColumn::Star],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
        });
        let stmt = Statement::Subscribe {
            inner: Box::new(inner),
            interval: None,
            delta: false,
        };
        assert!(matches!(stmt, Statement::Subscribe { .. }));
    }

    #[test]
    fn construct_unsubscribe_statement() {
        let stmt = Statement::Unsubscribe {
            stream_id: Some(42),
        };
        assert!(matches!(
            stmt,
            Statement::Unsubscribe {
                stream_id: Some(42)
            }
        ));

        let stmt_all = Statement::Unsubscribe { stream_id: None };
        assert!(matches!(
            stmt_all,
            Statement::Unsubscribe { stream_id: None }
        ));
    }
}
