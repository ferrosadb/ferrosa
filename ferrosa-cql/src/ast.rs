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
    CreateType {
        keyspace: Option<String>,
        name: String,
        if_not_exists: bool,
        fields: Vec<(String, CqlTypeName)>,
    },
    AlterType {
        keyspace: Option<String>,
        name: String,
        alterations: Vec<TypeAlteration>,
    },
    DropType {
        keyspace: Option<String>,
        name: String,
        if_exists: bool,
    },
    CreateFunction {
        keyspace: Option<String>,
        name: String,
        or_replace: bool,
        if_not_exists: bool,
        params: Vec<(String, CqlTypeName)>,
        called_on_null: bool,
        return_type: CqlTypeName,
        language: String,
        body: String,
    },
    DropFunction {
        keyspace: Option<String>,
        name: String,
        arg_types: Option<Vec<CqlTypeName>>,
        if_exists: bool,
    },
    CreateAggregate {
        keyspace: Option<String>,
        name: String,
        or_replace: bool,
        if_not_exists: bool,
        arg_types: Vec<CqlTypeName>,
        state_func: String,
        state_type: CqlTypeName,
        final_func: Option<String>,
        init_cond: Option<Term>,
    },
    DropAggregate {
        keyspace: Option<String>,
        name: String,
        arg_types: Option<Vec<CqlTypeName>>,
        if_exists: bool,
    },
    Subscribe {
        inner: Box<Statement>,
        interval: Option<Duration>,
        delta: bool,
    },
    Unsubscribe {
        stream_id: Option<u16>,
    },
    Explain(Box<SelectStatement>),
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
    FunctionCall {
        keyspace: Option<String>,
        name: String,
        args: Vec<Term>,
    },
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
    Contains,
    ContainsKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub op: ComparisonOp,
    pub value: Term,
    /// When true, this clause represents `token(column) op token(value)`
    /// and should be evaluated as a token-range predicate.
    pub token_fn: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    Star,
    Column(String),
    FunctionCall {
        keyspace: Option<String>,
        name: String,
        args: Vec<Term>,
        alias: Option<String>,
    },
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

/// An assignment in an UPDATE SET clause.
#[derive(Debug, Clone, PartialEq)]
pub enum Assignment {
    /// Simple: `col = term`
    Simple { column: String, value: Term },
    /// Addition: `col = col + term` (counter increment or collection append)
    Add { column: String, value: Term },
    /// Subtraction: `col = col - term` (counter decrement or collection subtract)
    Sub { column: String, value: Term },
    /// Map/list element: `col[key] = term`
    Element {
        column: String,
        key: Term,
        value: Term,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clauses: Vec<WhereClause>,
    pub if_exists: bool,
    pub using_timestamp: Option<i64>,
    pub using_ttl: Option<i32>,
}

/// A column target in a DELETE statement — either a whole column or a map element.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteTarget {
    /// Delete entire column: `DELETE col FROM ...`
    Column(String),
    /// Delete a single map element: `DELETE col['key'] FROM ...`
    MapElement { column: String, key: Term },
}

impl DeleteTarget {
    /// Returns the column name regardless of variant.
    pub fn column_name(&self) -> &str {
        match self {
            DeleteTarget::Column(name) => name,
            DeleteTarget::MapElement { column, .. } => column,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<DeleteTarget>,
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
    /// Vector type with element type and fixed dimension: `vector<float, 3>`.
    Vector(Box<CqlTypeName>, usize),
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
    /// Table extensions (e.g. graph metadata: `WITH extensions = {'vertex_label': 'Person'}`).
    pub extensions: Option<Vec<(String, String)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub add_columns: Vec<(String, CqlTypeName)>,
    pub drop_columns: Vec<String>,
    /// Table extensions (e.g. graph metadata: `WITH extensions = {'vertex_label': 'Person'}`).
    pub extensions: Option<Vec<(String, String)>>,
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

/// Alteration operations for `ALTER TYPE` statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeAlteration {
    AddField {
        name: String,
        field_type: CqlTypeName,
    },
    RenameField {
        from: String,
        to: String,
    },
    AlterField {
        name: String,
        new_type: CqlTypeName,
    },
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
    Function {
        keyspace: Option<String>,
        name: String,
        arg_types: Vec<CqlTypeName>,
    },
    AllFunctions {
        keyspace: Option<String>,
    },
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

    #[test]
    fn construct_create_type_statement() {
        let stmt = Statement::CreateType {
            keyspace: Some("ks".to_string()),
            name: "address".to_string(),
            if_not_exists: true,
            fields: vec![
                (
                    "street".to_string(),
                    CqlTypeName::Simple("text".to_string()),
                ),
                ("zip".to_string(), CqlTypeName::Simple("int".to_string())),
            ],
        };
        match &stmt {
            Statement::CreateType {
                keyspace,
                name,
                if_not_exists,
                fields,
            } => {
                assert_eq!(keyspace.as_deref(), Some("ks"));
                assert_eq!(name, "address");
                assert!(if_not_exists);
                assert_eq!(fields.len(), 2);
            }
            other => panic!("expected CreateType, got {:?}", other),
        }
    }

    #[test]
    fn construct_alter_type_statement() {
        let stmt = Statement::AlterType {
            keyspace: None,
            name: "address".to_string(),
            alterations: vec![
                TypeAlteration::AddField {
                    name: "city".to_string(),
                    field_type: CqlTypeName::Simple("text".to_string()),
                },
                TypeAlteration::RenameField {
                    from: "zip".to_string(),
                    to: "postal_code".to_string(),
                },
                TypeAlteration::AlterField {
                    name: "street".to_string(),
                    new_type: CqlTypeName::Simple("varchar".to_string()),
                },
            ],
        };
        match &stmt {
            Statement::AlterType { alterations, .. } => {
                assert_eq!(alterations.len(), 3);
                assert!(
                    matches!(&alterations[0], TypeAlteration::AddField { name, .. } if name == "city")
                );
                assert!(
                    matches!(&alterations[1], TypeAlteration::RenameField { from, to } if from == "zip" && to == "postal_code")
                );
                assert!(
                    matches!(&alterations[2], TypeAlteration::AlterField { name, .. } if name == "street")
                );
            }
            other => panic!("expected AlterType, got {:?}", other),
        }
    }

    #[test]
    fn construct_drop_type_statement() {
        let stmt = Statement::DropType {
            keyspace: Some("ks".to_string()),
            name: "address".to_string(),
            if_exists: true,
        };
        assert!(matches!(
            stmt,
            Statement::DropType {
                keyspace: Some(_),
                name: _,
                if_exists: true,
            }
        ));
    }
}
