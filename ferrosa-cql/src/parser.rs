//! Recursive-descent CQL parser.
//!
//! One function per grammar rule. Produces AST `Statement` values from
//! the token stream produced by `Lexer`.
//!
//! Security mitigations:
//! - **M2**: Nesting depth capped at `MAX_NESTING_DEPTH` (32).
//! - **M4**: No `unwrap()` on user-derived data — all fallible paths return `Result`.
//! - **M6**: Collection element count capped at `MAX_COLLECTION_ELEMENTS` (65,536).

use std::time::Duration;

use crate::ast::*;
use crate::error::CqlError;
use crate::lexer::{Keyword, Lexer, TokenKind};

/// Maximum nesting depth for collection/tuple literals and parameterized types.
/// Security mitigation M2.
const MAX_NESTING_DEPTH: usize = 32;

/// Return type for ORDER BY parsing: standard ordering clauses plus an
/// optional ANN OF (Approximate Nearest Neighbor) clause.
type OrderByResult = (Vec<(String, OrderDirection)>, Option<(String, Term)>);

/// Maximum number of elements in a collection literal.
/// Security mitigation M6.
const MAX_COLLECTION_ELEMENTS: usize = 65_536;

/// Options parsed from a CREATE/ALTER ROLE `WITH …` clause. Shared by the
/// CREATE and ALTER role parsers (identical grammar).
#[derive(Default)]
struct RoleOptionSet {
    password: Option<String>,
    hashed_password: Option<String>,
    superuser: Option<bool>,
    login: Option<bool>,
    options: Vec<(String, String)>,
    access: Vec<RoleAccess>,
}

/// Parse a CQL statement from the given input string.
pub fn parse(input: &str) -> Result<Statement, CqlError> {
    let span = tracing::info_span!("cql.parse", cql.statement_type = tracing::field::Empty,);
    let _entered = span.enter();
    let lexer = Lexer::new(input)?;
    let mut parser = Parser::new(lexer);
    let stmt = parser.parse_statement()?;
    // Consume optional trailing semicolon
    parser.lexer.eat(&TokenKind::Semicolon)?;
    // Verify we consumed everything
    let tok = parser.lexer.peek()?;
    if tok.kind != TokenKind::Eof {
        return Err(CqlError::SyntaxError(format!(
            "unexpected token {:?} after statement at position {}",
            tok.kind, tok.pos
        )));
    }
    // Record the statement type now that parsing succeeded.
    span.record("cql.statement_type", statement_type_name(&stmt));
    Ok(stmt)
}

/// Returns a short name for the statement variant (e.g. "SELECT", "INSERT").
fn statement_type_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Select(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Batch(_) => "BATCH",
        Statement::CreateKeyspace(_) => "CREATE_KEYSPACE",
        Statement::AlterKeyspace(_) => "ALTER_KEYSPACE",
        Statement::DropKeyspace(_) => "DROP_KEYSPACE",
        Statement::CreateTable(_) => "CREATE_TABLE",
        Statement::AlterTable(_) => "ALTER_TABLE",
        Statement::DropTable(_) => "DROP_TABLE",
        Statement::CreateRole(_) => "CREATE_ROLE",
        Statement::AlterRole(_) => "ALTER_ROLE",
        Statement::DropRole(_) => "DROP_ROLE",
        Statement::Grant(_) => "GRANT",
        Statement::Revoke(_) => "REVOKE",
        Statement::Use(_) => "USE",
        Statement::Truncate(_) => "TRUNCATE",
        Statement::Compact(_) => "COMPACT",
        Statement::CreateIndex(_) => "CREATE_INDEX",
        Statement::DropIndex(_) => "DROP_INDEX",
        _ => "OTHER",
    }
}

/// Parser state wrapping a [`Lexer`] and tracking nesting depth.
struct Parser<'input> {
    lexer: Lexer<'input>,
    /// Current nesting depth (incremented for collection/tuple/type parsing).
    depth: usize,
}

impl<'input> Parser<'input> {
    fn new(lexer: Lexer<'input>) -> Self {
        Self { lexer, depth: 0 }
    }

    /// Increment nesting depth, returning an error if the limit is exceeded.
    fn enter_nesting(&mut self) -> Result<(), CqlError> {
        self.depth += 1;
        if self.depth > MAX_NESTING_DEPTH {
            Err(CqlError::SyntaxError(
                "maximum nesting depth exceeded".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Decrement nesting depth.
    fn exit_nesting(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    // ---------------------------------------------------------------
    // Statement dispatch
    // ---------------------------------------------------------------

    fn parse_statement(&mut self) -> Result<Statement, CqlError> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Select) => self.parse_select().map(Statement::Select),
            TokenKind::Keyword(Keyword::Insert) => self.parse_insert().map(Statement::Insert),
            TokenKind::Keyword(Keyword::Update) => self.parse_update().map(Statement::Update),
            TokenKind::Keyword(Keyword::Delete) => self.parse_delete().map(Statement::Delete),
            TokenKind::Keyword(Keyword::Create) => self.parse_create(),
            TokenKind::Keyword(Keyword::Alter) => self.parse_alter(),
            TokenKind::Keyword(Keyword::Drop) => self.parse_drop(),
            TokenKind::Keyword(Keyword::Use) => self.parse_use().map(Statement::Use),
            TokenKind::Keyword(Keyword::Begin) => self.parse_begin(),
            TokenKind::Keyword(Keyword::Commit) => self.parse_commit(),
            TokenKind::Keyword(Keyword::Rollback) => self.parse_rollback(),
            TokenKind::Keyword(Keyword::Truncate) => self.parse_truncate().map(Statement::Truncate),
            TokenKind::Keyword(Keyword::Compact) => self.parse_compact().map(Statement::Compact),
            TokenKind::Keyword(Keyword::Grant) => self.parse_grant(),
            TokenKind::Keyword(Keyword::Revoke) => self.parse_revoke(),
            TokenKind::Keyword(Keyword::Subscribe) => self.parse_subscribe(),
            TokenKind::Keyword(Keyword::Unsubscribe) => self.parse_unsubscribe(),
            TokenKind::Keyword(Keyword::List) => self.parse_list(),
            TokenKind::Keyword(Keyword::Explain) => self.parse_explain(),
            TokenKind::Eof => Err(CqlError::SyntaxError("empty query".to_string())),
            _ => Err(CqlError::SyntaxError(format!(
                "unexpected token {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    // ---------------------------------------------------------------
    // SELECT
    // ---------------------------------------------------------------

    fn parse_select(&mut self) -> Result<SelectStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Select))?;

        let distinct = self.lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;

        // Columns: * or comma-separated identifiers
        let columns = self.parse_select_columns()?;

        self.lexer.expect(&TokenKind::Keyword(Keyword::From))?;
        let (keyspace, table) = self.parse_table_ref()?;

        // Optional WHERE
        let where_clauses = if self.lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
            self.parse_where_clauses()?
        } else {
            vec![]
        };

        // Optional ORDER BY (including ANN OF for vector similarity search)
        let (order_by, ann_of) = if self.lexer.eat(&TokenKind::Keyword(Keyword::Order))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::By))?;
            self.parse_order_by_with_ann()?
        } else {
            (vec![], None)
        };

        // Optional LIMIT
        let limit = if self.lexer.eat(&TokenKind::Keyword(Keyword::Limit))? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::IntegerLiteral(n) => {
                    let n = i32::try_from(n).map_err(|_| {
                        CqlError::SyntaxError(format!("LIMIT value out of range: {n}"))
                    })?;
                    Some(crate::ast::Limit::Literal(n))
                }
                TokenKind::QuestionMark => Some(crate::ast::Limit::BindMarker),
                TokenKind::NamedBind(name) => Some(crate::ast::Limit::NamedBindMarker(name)),
                _ => {
                    return Err(CqlError::SyntaxError(format!(
                        "expected integer or bind marker after LIMIT, got {:?}",
                        tok.kind
                    )))
                }
            }
        } else {
            None
        };

        // Optional ALLOW FILTERING
        let allow_filtering = if self.lexer.eat(&TokenKind::Keyword(Keyword::Allow))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::Filtering))?;
            true
        } else {
            false
        };

        Ok(SelectStatement {
            keyspace,
            table,
            columns,
            distinct,
            where_clauses,
            order_by,
            limit,
            allow_filtering,
            ann_of,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, CqlError> {
        if self.lexer.eat(&TokenKind::Star)? {
            return Ok(vec![SelectColumn::Star]);
        }

        let mut cols = vec![];
        loop {
            // Check if this is a function call: ident(...)
            let name = self.parse_ident()?;
            if self.lexer.eat(&TokenKind::LParen)? {
                // Function call: COUNT(*), WRITETIME(col), TTL(col), etc.
                let mut args = vec![];
                if self.lexer.eat(&TokenKind::Star)? {
                    // COUNT(*) — represent * as a special term
                    args.push(Term::StringLiteral("*".to_string()));
                } else if !matches!(self.lexer.peek()?.kind, TokenKind::RParen) {
                    args.push(self.parse_term()?);
                    while self.lexer.eat(&TokenKind::Comma)? {
                        args.push(self.parse_term()?);
                    }
                }
                self.lexer.expect(&TokenKind::RParen)?;

                // Optional AS alias
                let alias = if self.lexer.eat(&TokenKind::Keyword(Keyword::As))? {
                    Some(self.parse_ident()?)
                } else {
                    None
                };

                cols.push(SelectColumn::FunctionCall {
                    keyspace: None,
                    name,
                    args,
                    alias,
                });
            } else if self.lexer.eat(&TokenKind::Keyword(Keyword::As))? {
                // Column with alias: col AS alias
                let alias = self.parse_ident()?;
                cols.push(SelectColumn::FunctionCall {
                    keyspace: None,
                    name,
                    args: vec![],
                    alias: Some(alias),
                });
            } else {
                cols.push(SelectColumn::Column(name));
            }

            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        Ok(cols)
    }

    /// Parse ORDER BY clause, handling both standard `col ASC|DESC` and
    /// ANN (Approximate Nearest Neighbor) syntax: `col ANN OF <term>`.
    fn parse_order_by_with_ann(&mut self) -> Result<OrderByResult, CqlError> {
        let mut items = vec![];
        let mut ann_of = None;
        loop {
            let col = self.parse_ident()?;

            // Check for ANN OF <term> syntax (vector similarity ordering).
            // ANN is not a keyword — it arrives as Ident("ann") after lowercasing
            // by parse_ident, or as a raw Ident token we peek at directly.
            let peek = self.lexer.peek()?;
            let is_ann = matches!(&peek.kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case("ann"));
            if is_ann {
                // Consume "ANN"
                self.lexer.next_token()?;
                // Expect "OF"
                self.lexer.expect(&TokenKind::Keyword(Keyword::Of))?;
                // Parse the vector term (bind marker, list literal, etc.)
                let term = self.parse_term()?;
                ann_of = Some((col, term));
                // ANN OF is always the sole ordering clause — break out.
                break;
            }

            let dir = if self.lexer.eat(&TokenKind::Keyword(Keyword::Desc))? {
                OrderDirection::Desc
            } else {
                // ASC is default; also accept explicit ASC keyword
                self.lexer.eat(&TokenKind::Keyword(Keyword::Asc))?;
                OrderDirection::Asc
            };
            items.push((col, dir));
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        Ok((items, ann_of))
    }

    // ---------------------------------------------------------------
    // INSERT
    // ---------------------------------------------------------------

    fn parse_insert(&mut self) -> Result<InsertStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Insert))?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::Into))?;

        let (keyspace, table) = self.parse_table_ref()?;

        // (column, column, ...)
        self.lexer.expect(&TokenKind::LParen)?;
        let columns = self.parse_ident_list()?;
        self.lexer.expect(&TokenKind::RParen)?;

        self.lexer.expect(&TokenKind::Keyword(Keyword::Values))?;

        // (term, term, ...)
        self.lexer.expect(&TokenKind::LParen)?;
        let values = self.parse_term_list()?;
        self.lexer.expect(&TokenKind::RParen)?;

        // Optional IF NOT EXISTS
        let if_not_exists = self.parse_if_not_exists()?;

        // Optional USING
        let (using_timestamp, using_ttl) = self.parse_using_clause()?;

        Ok(InsertStatement {
            keyspace,
            table,
            columns,
            values,
            if_not_exists,
            using_timestamp,
            using_ttl,
        })
    }

    // ---------------------------------------------------------------
    // UPDATE
    // ---------------------------------------------------------------

    fn parse_update(&mut self) -> Result<UpdateStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Update))?;
        let (keyspace, table) = self.parse_table_ref()?;

        // Optional USING before SET
        let (mut using_timestamp, mut using_ttl) = (None, None);
        if self.lexer.eat(&TokenKind::Keyword(Keyword::Using))? {
            let (ts, ttl) = self.parse_using_clause_body()?;
            using_timestamp = ts;
            using_ttl = ttl;
        }

        self.lexer.expect(&TokenKind::Keyword(Keyword::Set))?;
        let assignments = self.parse_assignments()?;

        self.lexer.expect(&TokenKind::Keyword(Keyword::Where))?;
        let where_clauses = self.parse_where_clauses()?;

        // Optional IF EXISTS / IF <conditions>
        let (if_exists, if_conditions) = self.parse_if_clause()?;

        // Optional USING after WHERE (some syntaxes allow it here too)
        if using_timestamp.is_none() && using_ttl.is_none() {
            let (ts, ttl) = self.parse_using_clause()?;
            using_timestamp = ts;
            using_ttl = ttl;
        }

        Ok(UpdateStatement {
            keyspace,
            table,
            assignments,
            where_clauses,
            if_exists,
            if_conditions,
            using_timestamp,
            using_ttl,
        })
    }

    fn parse_assignments(&mut self) -> Result<Vec<Assignment>, CqlError> {
        let mut assignments = vec![];
        loop {
            let col = self.parse_ident()?;

            // Check for map/list element: col[key] = value
            if self.lexer.eat(&TokenKind::LBracket)? {
                let key = self.parse_term()?;
                self.lexer.expect(&TokenKind::RBracket)?;
                self.lexer.expect(&TokenKind::Eq)?;
                let value = self.parse_term()?;
                assignments.push(Assignment::Element {
                    column: col,
                    key,
                    value,
                });
            } else {
                self.lexer.expect(&TokenKind::Eq)?;
                let value = self.parse_term()?;

                // Check for: col = col + term  or  col = col - term
                if self.lexer.eat(&TokenKind::Plus)? {
                    let rhs = self.parse_term()?;
                    assignments.push(Assignment::Add {
                        column: col,
                        value: rhs,
                    });
                } else if self.lexer.eat(&TokenKind::Minus)? {
                    let rhs = self.parse_term()?;
                    assignments.push(Assignment::Sub {
                        column: col,
                        value: rhs,
                    });
                } else {
                    assignments.push(Assignment::Simple { column: col, value });
                }
            }

            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        Ok(assignments)
    }

    // ---------------------------------------------------------------
    // DELETE
    // ---------------------------------------------------------------

    fn parse_delete(&mut self) -> Result<DeleteStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Delete))?;

        // Optional column list (if next token is not FROM)
        let columns = {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::Keyword(Keyword::From) {
                vec![]
            } else {
                self.parse_delete_columns()?
            }
        };

        self.lexer.expect(&TokenKind::Keyword(Keyword::From))?;
        let (keyspace, table) = self.parse_table_ref()?;

        // Optional USING TIMESTAMP
        let (using_timestamp, _) = self.parse_using_clause()?;

        self.lexer.expect(&TokenKind::Keyword(Keyword::Where))?;
        let where_clauses = self.parse_where_clauses()?;

        let (if_exists, if_conditions) = self.parse_if_clause()?;

        Ok(DeleteStatement {
            keyspace,
            table,
            columns,
            where_clauses,
            if_exists,
            if_conditions,
            using_timestamp,
        })
    }

    fn parse_delete_columns(&mut self) -> Result<Vec<DeleteTarget>, CqlError> {
        let mut cols = vec![self.parse_delete_target()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            cols.push(self.parse_delete_target()?);
        }
        Ok(cols)
    }

    /// Parse a single delete target: either `col` or `col[key]`.
    fn parse_delete_target(&mut self) -> Result<DeleteTarget, CqlError> {
        let col = self.parse_ident()?;
        if self.lexer.eat(&TokenKind::LBracket)? {
            let key = self.parse_term()?;
            self.lexer.expect(&TokenKind::RBracket)?;
            Ok(DeleteTarget::MapElement { column: col, key })
        } else {
            Ok(DeleteTarget::Column(col))
        }
    }

    // ---------------------------------------------------------------
    // BEGIN — disambiguate BATCH vs TRANSACTION
    // ---------------------------------------------------------------

    fn parse_begin(&mut self) -> Result<Statement, CqlError> {
        // Consume BEGIN, then peek to decide: TRANSACTION or BATCH
        self.lexer.expect(&TokenKind::Keyword(Keyword::Begin))?;

        if self.lexer.eat(&TokenKind::Keyword(Keyword::Transaction))? {
            return Ok(Statement::BeginTransaction);
        }

        // Otherwise it's a BATCH statement — delegate to parse_batch_body
        // (BEGIN already consumed)
        self.parse_batch_body().map(Statement::Batch)
    }

    fn parse_commit(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Commit))?;
        Ok(Statement::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Rollback))?;
        Ok(Statement::Rollback)
    }

    // ---------------------------------------------------------------
    // BATCH
    // ---------------------------------------------------------------

    /// Parse the body of a BATCH statement (after BEGIN has already been consumed).
    fn parse_batch_body(&mut self) -> Result<BatchStatement, CqlError> {
        // Optional batch type
        let batch_type = if self.lexer.eat(&TokenKind::Keyword(Keyword::Unlogged))? {
            BatchType::Unlogged
        } else if self.lexer.eat(&TokenKind::Keyword(Keyword::Counter))? {
            BatchType::Counter
        } else {
            // LOGGED is default; also accept explicit LOGGED keyword
            self.lexer.eat(&TokenKind::Keyword(Keyword::Logged))?;
            BatchType::Logged
        };

        self.lexer.expect(&TokenKind::Keyword(Keyword::Batch))?;

        // Optional USING TIMESTAMP
        let (using_timestamp, _) = self.parse_using_clause()?;

        // Statements (only INSERT/UPDATE/DELETE)
        let mut statements = vec![];
        loop {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::Keyword(Keyword::Apply) {
                break;
            }
            let stmt = self.parse_batch_inner_statement()?;
            statements.push(stmt);
            // Consume optional semicolons between statements
            while self.lexer.eat(&TokenKind::Semicolon)? {}
        }

        self.lexer.expect(&TokenKind::Keyword(Keyword::Apply))?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::Batch))?;

        Ok(BatchStatement {
            batch_type,
            statements,
            using_timestamp,
        })
    }

    fn parse_batch_inner_statement(&mut self) -> Result<Statement, CqlError> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Insert) => self.parse_insert().map(Statement::Insert),
            TokenKind::Keyword(Keyword::Update) => self.parse_update().map(Statement::Update),
            TokenKind::Keyword(Keyword::Delete) => self.parse_delete().map(Statement::Delete),
            _ => Err(CqlError::SyntaxError(format!(
                "only INSERT, UPDATE, DELETE allowed in BATCH, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    // ---------------------------------------------------------------
    // CREATE
    // ---------------------------------------------------------------

    fn parse_create(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Create))?;
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Table) => {
                self.parse_create_table().map(Statement::CreateTable)
            }
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.parse_create_keyspace().map(Statement::CreateKeyspace)
            }
            TokenKind::Keyword(Keyword::Role) => {
                self.parse_create_role().map(Statement::CreateRole)
            }
            // CREATE USER is a deprecated alias for
            // `CREATE ROLE WITH LOGIN = true`.  Same syntax body
            // as CREATE ROLE; we just default `login` to true.
            TokenKind::Keyword(Keyword::User) => {
                self.parse_create_user_as_role().map(Statement::CreateRole)
            }
            TokenKind::Keyword(Keyword::Index) => {
                self.parse_create_index().map(Statement::CreateIndex)
            }
            TokenKind::Keyword(Keyword::Type) => self.parse_create_type(),
            TokenKind::Keyword(Keyword::Function) => self.parse_create_function(false),
            TokenKind::Keyword(Keyword::Aggregate) => self.parse_create_aggregate(false),
            TokenKind::Keyword(Keyword::Or) => {
                // CREATE OR REPLACE FUNCTION/AGGREGATE
                self.lexer.next_token()?; // consume OR
                self.lexer.expect(&TokenKind::Keyword(Keyword::Replace))?;
                let tok = self.lexer.peek()?;
                match &tok.kind {
                    TokenKind::Keyword(Keyword::Function) => self.parse_create_function(true),
                    TokenKind::Keyword(Keyword::Aggregate) => self.parse_create_aggregate(true),
                    _ => Err(CqlError::SyntaxError(format!(
                        "expected FUNCTION or AGGREGATE after CREATE OR REPLACE, got {:?} at position {}",
                        tok.kind, tok.pos
                    ))),
                }
            }
            _ => Err(CqlError::SyntaxError(format!(
                "CREATE {:?} not yet supported at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Table))?;

        let if_not_exists = self.parse_if_not_exists()?;
        let (keyspace, name) = self.parse_table_ref()?;

        self.lexer.expect(&TokenKind::LParen)?;

        // Column definitions and PRIMARY KEY
        let mut columns: Vec<(String, CqlTypeName)> = vec![];
        let mut partition_key: Vec<String> = vec![];
        let mut clustering_key: Vec<String> = vec![];

        loop {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::RParen {
                break;
            }

            // Check for PRIMARY KEY inline
            if tok.kind == TokenKind::Keyword(Keyword::Primary) {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::Key))?;
                self.lexer.expect(&TokenKind::LParen)?;

                let tok = self.lexer.peek()?;
                if tok.kind == TokenKind::LParen {
                    // Composite partition key: ((pk1, pk2), ck1, ck2)
                    self.lexer.next_token()?;
                    partition_key = self.parse_ident_list()?;
                    self.lexer.expect(&TokenKind::RParen)?;
                    // Clustering columns
                    while self.lexer.eat(&TokenKind::Comma)? {
                        clustering_key.push(self.parse_ident()?);
                    }
                } else {
                    // Simple or compound key: (pk) or (pk, ck1, ck2)
                    let first = self.parse_ident()?;
                    if self.lexer.eat(&TokenKind::Comma)? {
                        // pk, ck1, ck2...
                        partition_key.push(first);
                        loop {
                            clustering_key.push(self.parse_ident()?);
                            if !self.lexer.eat(&TokenKind::Comma)? {
                                break;
                            }
                        }
                    } else {
                        partition_key.push(first);
                    }
                }

                self.lexer.expect(&TokenKind::RParen)?;
            } else {
                // Column definition: name type [STATIC]
                let col_name = self.parse_ident()?;
                let col_type = self.parse_cql_type_name()?;
                // Consume optional STATIC keyword (we store it as part of the column definition)
                self.lexer.eat(&TokenKind::Keyword(Keyword::Static))?;
                // Check for PRIMARY KEY after column def
                if self.lexer.eat(&TokenKind::Keyword(Keyword::Primary))? {
                    self.lexer.expect(&TokenKind::Keyword(Keyword::Key))?;
                    partition_key.push(col_name.clone());
                }
                columns.push((col_name, col_type));
            }

            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }

        self.lexer.expect(&TokenKind::RParen)?;

        // WITH options
        let mut table_options: Vec<(String, String)> = vec![];
        let mut clustering_order: Vec<(String, ClusteringOrder)> = vec![];
        let mut extensions: Option<Vec<(String, String)>> = None;

        if self.lexer.eat(&TokenKind::Keyword(Keyword::With))? {
            loop {
                if self.lexer.eat(&TokenKind::Keyword(Keyword::Clustering))? {
                    // CLUSTERING ORDER BY (col DESC, ...)
                    self.lexer.expect(&TokenKind::Keyword(Keyword::Order))?;
                    self.lexer.expect(&TokenKind::Keyword(Keyword::By))?;
                    self.lexer.expect(&TokenKind::LParen)?;
                    loop {
                        let col = self.parse_ident()?;
                        let order = if self.lexer.eat(&TokenKind::Keyword(Keyword::Desc))? {
                            ClusteringOrder::Desc
                        } else {
                            self.lexer.eat(&TokenKind::Keyword(Keyword::Asc))?;
                            ClusteringOrder::Asc
                        };
                        clustering_order.push((col, order));
                        if !self.lexer.eat(&TokenKind::Comma)? {
                            break;
                        }
                    }
                    self.lexer.expect(&TokenKind::RParen)?;
                } else if self.lexer.eat(&TokenKind::Keyword(Keyword::Compact))? {
                    // COMPACT STORAGE — accept but ignore
                    self.lexer.expect(&TokenKind::Keyword(Keyword::Storage))?;
                    table_options.push(("compact_storage".to_string(), "true".to_string()));
                } else {
                    // Generic option: name = value
                    let opt_name = self.parse_ident()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    if opt_name == "extensions" {
                        // extensions takes a map literal: {'key': 'value', ...}
                        extensions = Some(self.parse_string_map()?);
                    } else {
                        let opt_val = self.parse_option_value()?;
                        table_options.push((opt_name, opt_val));
                    }
                }
                if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                    break;
                }
            }
        }

        // Merge clustering order into clustering_key
        let final_clustering_key: Vec<(String, ClusteringOrder)> = if clustering_order.is_empty() {
            clustering_key
                .into_iter()
                .map(|c| (c, ClusteringOrder::Asc))
                .collect()
        } else {
            // Use the explicit clustering order; must match clustering_key names
            clustering_order
        };

        Ok(CreateTableStatement {
            keyspace,
            name,
            columns,
            partition_key,
            clustering_key: final_clustering_key,
            if_not_exists,
            table_options,
            extensions,
        })
    }

    fn parse_option_value(&mut self) -> Result<String, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::StringLiteral(s) => Ok(s),
            TokenKind::IntegerLiteral(n) => Ok(n.to_string()),
            TokenKind::FloatLiteral(f) => Ok(f.to_string()),
            TokenKind::Keyword(Keyword::True) => Ok("true".to_string()),
            TokenKind::Keyword(Keyword::False) => Ok("false".to_string()),
            TokenKind::LBrace => {
                // Map literal: { 'key': 'value', ... }
                // Consume and stringify — these are table property maps
                // (COMPACTION, COMPRESSION, etc.) that ferrosa stores but
                // doesn't interpret.
                let pairs = self.parse_string_map_inner()?;
                let inner: Vec<String> = pairs
                    .iter()
                    .map(|(k, v)| format!("'{}': '{}'", k, v))
                    .collect();
                Ok(format!("{{{}}}", inner.join(", ")))
            }
            _ => Err(CqlError::SyntaxError(format!(
                "expected option value, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn parse_create_keyspace(&mut self) -> Result<CreateKeyspaceStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Keyspace))?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_ident()?;

        self.lexer.expect(&TokenKind::Keyword(Keyword::With))?;
        self.lexer
            .expect(&TokenKind::Keyword(Keyword::Replication))?;
        self.lexer.expect(&TokenKind::Eq)?;

        // Parse map literal for replication
        let replication = self.parse_string_map()?;

        // Optional AND DURABLE_WRITES = bool
        let durable_writes = if self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
            self.lexer
                .expect(&TokenKind::Keyword(Keyword::DurableWrites))?;
            self.lexer.expect(&TokenKind::Eq)?;
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::Keyword(Keyword::True) => Some(true),
                TokenKind::Keyword(Keyword::False) => Some(false),
                _ => {
                    return Err(CqlError::SyntaxError(format!(
                        "expected true or false for DURABLE_WRITES, got {:?} at position {}",
                        tok.kind, tok.pos
                    )))
                }
            }
        } else {
            None
        };

        Ok(CreateKeyspaceStatement {
            name,
            if_not_exists,
            replication,
            durable_writes,
        })
    }

    /// Parse a map literal of string keys and string/integer values: {'key': 'value', 'key2': 1, ...}
    fn parse_string_map(&mut self) -> Result<Vec<(String, String)>, CqlError> {
        self.lexer.expect(&TokenKind::LBrace)?;
        self.parse_string_map_inner()
    }

    /// Parse the inside of a `{ 'k': 'v', ... }` map literal after the
    /// opening brace has already been consumed.
    fn parse_string_map_inner(&mut self) -> Result<Vec<(String, String)>, CqlError> {
        let mut entries = vec![];

        if self.lexer.eat(&TokenKind::RBrace)? {
            return Ok(entries);
        }

        loop {
            let key = self.expect_string_literal()?;
            self.lexer.expect(&TokenKind::Colon)?;
            // Accept both string literals and integer literals as values
            // (Cassandra allows e.g. 'replication_factor': 1)
            let tok = self.lexer.peek()?;
            let value = match tok.kind {
                TokenKind::IntegerLiteral(_) => {
                    let t = self.lexer.next_token()?;
                    if let TokenKind::IntegerLiteral(n) = t.kind {
                        n.to_string()
                    } else {
                        unreachable!()
                    }
                }
                _ => self.expect_string_literal()?,
            };
            entries.push((key, value));
            if entries.len() > MAX_COLLECTION_ELEMENTS {
                return Err(CqlError::SyntaxError("collection too large".to_string()));
            }
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }

        self.lexer.expect(&TokenKind::RBrace)?;
        Ok(entries)
    }

    fn expect_string_literal(&mut self) -> Result<String, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::StringLiteral(s) => Ok(s),
            _ => Err(CqlError::SyntaxError(format!(
                "expected string literal, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    /// Parse `CREATE USER <name> WITH PASSWORD = '...' [SUPERUSER]` —
    /// the legacy alias for `CREATE ROLE ... WITH LOGIN = true`.
    /// Cassandra's grammar is permissive about ordering; we accept a
    /// trailing `SUPERUSER`/`NOSUPERUSER` keyword as a shorthand for
    /// `WITH SUPERUSER = true/false`.
    /// Parsed `WITH option (AND option)*` set for CREATE/ALTER ROLE.
    fn parse_role_option_set(&mut self) -> Result<RoleOptionSet, CqlError> {
        let mut o = RoleOptionSet::default();
        if self.lexer.eat(&TokenKind::Keyword(Keyword::With))? {
            self.parse_with_role_options(&mut o)?;
        }
        // Legacy USER syntax allows a trailing bare SUPERUSER / NOSUPERUSER flag
        // (no `WITH`, no `=`), e.g. `ALTER USER bob SUPERUSER` or
        // `CREATE USER ops WITH PASSWORD 'p' NOSUPERUSER`. Setting the flag is
        // still authorized downstream exactly as `SUPERUSER = <bool>` is.
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Superuser) => {
                self.lexer.next_token()?;
                o.superuser = Some(true);
            }
            TokenKind::Keyword(Keyword::Nosuperuser) => {
                self.lexer.next_token()?;
                o.superuser = Some(false);
            }
            _ => {}
        }
        Ok(o)
    }

    /// Parse the `WITH <option> [AND <option> ...]` role-option list (the caller
    /// has already consumed `WITH`).
    fn parse_with_role_options(&mut self, o: &mut RoleOptionSet) -> Result<(), CqlError> {
        loop {
            let tok = self.lexer.peek()?;
            match &tok.kind {
                TokenKind::Keyword(Keyword::Password) => {
                    self.lexer.next_token()?;
                    // `=` is optional: ROLE syntax uses `PASSWORD = 'x'`, the
                    // legacy USER syntax uses `PASSWORD 'x'`.
                    self.lexer.eat(&TokenKind::Eq)?;
                    o.password = Some(self.expect_string_literal()?);
                }
                TokenKind::Keyword(Keyword::Superuser) => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    o.superuser = Some(self.parse_bool()?);
                }
                TokenKind::Keyword(Keyword::Login) => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    o.login = Some(self.parse_bool()?);
                }
                TokenKind::Keyword(Keyword::Options) => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    // parse_string_map enforces MAX_COLLECTION_ELEMENTS (T8/DoS).
                    o.options = self.parse_string_map()?;
                }
                // `HASHED` and `ACCESS` are not reserved keywords — they lex as
                // identifiers. Match case-insensitively.
                TokenKind::Ident(s) if s.eq_ignore_ascii_case("hashed") => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Keyword(Keyword::Password))?;
                    // `=` is optional (see PASSWORD above).
                    self.lexer.eat(&TokenKind::Eq)?;
                    o.hashed_password = Some(self.expect_string_literal()?);
                }
                TokenKind::Ident(s) if s.eq_ignore_ascii_case("access") => {
                    self.lexer.next_token()?;
                    o.access.push(self.parse_role_access()?);
                }
                _ => {
                    return Err(CqlError::SyntaxError(format!(
                        "unexpected token {:?} in role options at position {}",
                        tok.kind, tok.pos
                    )))
                }
            }
            if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                break;
            }
        }
        Ok(())
    }

    /// Parse one `ACCESS TO/FROM …` clause (the `ACCESS` token is already consumed).
    fn parse_role_access(&mut self) -> Result<RoleAccess, CqlError> {
        let tok = self.lexer.next_token()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::To) => {
                if self.lexer.eat(&TokenKind::Keyword(Keyword::All))? {
                    self.expect_ident_ci("DATACENTERS")?;
                    Ok(RoleAccess::ToAllDatacenters)
                } else {
                    self.expect_ident_ci("DATACENTERS")?;
                    Ok(RoleAccess::ToDatacenters(self.parse_string_set()?))
                }
            }
            TokenKind::Keyword(Keyword::From) => {
                if self.lexer.eat(&TokenKind::Keyword(Keyword::All))? {
                    self.expect_ident_ci("CIDRS")?;
                    Ok(RoleAccess::FromAllCidrs)
                } else {
                    self.expect_ident_ci("CIDRS")?;
                    Ok(RoleAccess::FromCidrs(self.parse_string_set()?))
                }
            }
            other => Err(CqlError::SyntaxError(format!(
                "expected TO or FROM after ACCESS, got {:?} at position {}",
                other, tok.pos
            ))),
        }
    }

    /// Expect a (non-keyword) identifier matching `name`, case-insensitively.
    fn expect_ident_ci(&mut self, name: &str) -> Result<(), CqlError> {
        let tok = self.lexer.next_token()?;
        match &tok.kind {
            TokenKind::Ident(s) if s.eq_ignore_ascii_case(name) => Ok(()),
            other => Err(CqlError::SyntaxError(format!(
                "expected {name}, got {:?} at position {}",
                other, tok.pos
            ))),
        }
    }

    /// Parse `{ 'a', 'b', ... }` — a set of string literals (bounded).
    fn parse_string_set(&mut self) -> Result<Vec<String>, CqlError> {
        self.lexer.expect(&TokenKind::LBrace)?;
        let mut items = Vec::new();
        if self.lexer.eat(&TokenKind::RBrace)? {
            return Ok(items);
        }
        loop {
            items.push(self.expect_string_literal()?);
            if items.len() > MAX_COLLECTION_ELEMENTS {
                return Err(CqlError::SyntaxError("collection too large".to_string()));
            }
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        self.lexer.expect(&TokenKind::RBrace)?;
        Ok(items)
    }

    fn parse_create_user_as_role(&mut self) -> Result<CreateRoleStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::User))?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_ident()?;

        let mut password = None;
        let mut hashed_password = None;
        let mut superuser = None;

        if self.lexer.eat(&TokenKind::Keyword(Keyword::With))? {
            // Legacy USER grammar: `WITH [HASHED] PASSWORD '...'` (no `=`; we
            // also tolerate `=` for forgiving compatibility).
            let tok = self.lexer.peek()?;
            if matches!(&tok.kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case("hashed")) {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::Password))?;
                let _ = self.lexer.eat(&TokenKind::Eq)?;
                hashed_password = Some(self.expect_string_literal()?);
            } else if matches!(&tok.kind, TokenKind::Keyword(Keyword::Password)) {
                self.lexer.next_token()?;
                let _ = self.lexer.eat(&TokenKind::Eq)?;
                password = Some(self.expect_string_literal()?);
            }
        }

        // Trailing SUPERUSER / NOSUPERUSER shorthand.
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Superuser) => {
                self.lexer.next_token()?;
                superuser = Some(true);
            }
            TokenKind::Keyword(Keyword::Nosuperuser) => {
                self.lexer.next_token()?;
                superuser = Some(false);
            }
            _ => {}
        }

        Ok(CreateRoleStatement {
            name,
            if_not_exists,
            password,
            hashed_password,
            superuser,
            // CREATE USER is the alias that defaults LOGIN = true.
            login: Some(true),
            options: Vec::new(),
            access: Vec::new(),
        })
    }

    fn parse_create_role(&mut self) -> Result<CreateRoleStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Role))?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_ident()?;
        let opts = self.parse_role_option_set()?;

        Ok(CreateRoleStatement {
            name,
            if_not_exists,
            password: opts.password,
            hashed_password: opts.hashed_password,
            superuser: opts.superuser,
            login: opts.login,
            options: opts.options,
            access: opts.access,
        })
    }

    // ---------------------------------------------------------------
    // ALTER
    // ---------------------------------------------------------------

    /// Parse `LIST ROLES [OF role] [NORECURSIVE]` and the `LIST USERS`
    /// alias. Cassandra also has `LIST PERMISSIONS`, `LIST ALL
    /// PERMISSIONS` — those are deferred (return SyntaxError) until
    /// the permissions reporting pipeline catches up.
    fn parse_list(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::List))?;
        let tok = self.lexer.peek()?;

        // LIST ALL PERMISSIONS ...
        if matches!(tok.kind, TokenKind::Keyword(Keyword::All)) {
            self.lexer.next_token()?;
            self.expect_permissions_keyword()?;
            return self.parse_list_permissions_tail(None);
        }

        // LIST ROLES / USERS — matched as case-insensitive identifiers rather
        // than reserved keywords so user tables named `users` keep lexing as
        // `Ident("users")` elsewhere.
        if let TokenKind::Ident(name) = &tok.kind {
            if name.eq_ignore_ascii_case("roles") || name.eq_ignore_ascii_case("users") {
                let users_alias = name.eq_ignore_ascii_case("users");
                self.lexer.next_token()?;
                let of = if self.lexer.eat(&TokenKind::Keyword(Keyword::Of))? {
                    Some(self.parse_ident()?)
                } else {
                    None
                };
                let no_recursive = self.eat_norecursive()?;
                return Ok(Statement::ListRoles {
                    of,
                    no_recursive,
                    users_alias,
                });
            }
        }

        // Otherwise: LIST <permission> PERMISSIONS ... (e.g. LIST SELECT PERMISSIONS).
        let permission = self.parse_ident()?.to_uppercase();
        self.expect_permissions_keyword()?;
        self.parse_list_permissions_tail(Some(permission))
    }

    /// Expect the `PERMISSIONS` keyword.
    fn expect_permissions_keyword(&mut self) -> Result<(), CqlError> {
        self.lexer
            .expect(&TokenKind::Keyword(Keyword::Permissions))?;
        Ok(())
    }

    /// Cassandra spells the recursion suppressor `NORECURSIVE` as one token; we
    /// accept the bare ident form.
    fn eat_norecursive(&mut self) -> Result<bool, CqlError> {
        let tok = self.lexer.peek()?;
        if let TokenKind::Ident(name) = &tok.kind {
            if name.eq_ignore_ascii_case("norecursive") {
                self.lexer.next_token()?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Parse the tail of a `LIST … PERMISSIONS` statement: optional
    /// `ON <resource>`, `OF <role>`, and `NORECURSIVE`.
    fn parse_list_permissions_tail(
        &mut self,
        permission: Option<String>,
    ) -> Result<Statement, CqlError> {
        let resource = if self.lexer.eat(&TokenKind::Keyword(Keyword::On))? {
            Some(self.parse_resource()?)
        } else {
            None
        };
        let of = if self.lexer.eat(&TokenKind::Keyword(Keyword::Of))? {
            Some(self.parse_ident()?)
        } else {
            None
        };
        let no_recursive = self.eat_norecursive()?;
        Ok(Statement::ListPermissions {
            permission,
            resource,
            of,
            no_recursive,
        })
    }

    fn parse_alter(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Alter))?;
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Table) => {
                self.parse_alter_table().map(Statement::AlterTable)
            }
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.parse_alter_keyspace().map(Statement::AlterKeyspace)
            }
            TokenKind::Keyword(Keyword::Type) => self.parse_alter_type(),
            // ALTER ROLE / ALTER USER share the same options as CREATE.
            // Both forms accepted — `USER` is the deprecated alias.
            TokenKind::Keyword(Keyword::Role) | TokenKind::Keyword(Keyword::User) => {
                self.parse_alter_role().map(Statement::AlterRole)
            }
            _ => Err(CqlError::SyntaxError(format!(
                "ALTER {:?} not yet supported at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn parse_alter_role(&mut self) -> Result<AlterRoleStatement, CqlError> {
        // Caller has already peeked Keyword::Role or Keyword::User; consume it.
        let kw = self.lexer.next_token()?;
        debug_assert!(matches!(
            &kw.kind,
            TokenKind::Keyword(Keyword::Role) | TokenKind::Keyword(Keyword::User)
        ));

        let name = self.parse_ident()?;
        let opts = self.parse_role_option_set()?;

        Ok(AlterRoleStatement {
            name,
            password: opts.password,
            hashed_password: opts.hashed_password,
            superuser: opts.superuser,
            login: opts.login,
            options: opts.options,
            access: opts.access,
        })
    }

    fn parse_alter_keyspace(&mut self) -> Result<AlterKeyspaceStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Keyspace))?;
        let name = self.parse_ident()?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::With))?;

        let mut replication = None;
        let mut durable_writes = None;
        loop {
            let tok = self.lexer.peek()?;
            match &tok.kind {
                TokenKind::Keyword(Keyword::Replication) => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    replication = Some(self.parse_string_map()?);
                }
                TokenKind::Keyword(Keyword::DurableWrites) => {
                    self.lexer.next_token()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    let t = self.lexer.next_token()?;
                    durable_writes = match t.kind {
                        TokenKind::Keyword(Keyword::True) => Some(true),
                        TokenKind::Keyword(Keyword::False) => Some(false),
                        _ => {
                            return Err(CqlError::SyntaxError(format!(
                            "expected true or false for DURABLE_WRITES, got {:?} at position {}",
                            t.kind, t.pos
                        )))
                        }
                    };
                }
                _ => {
                    let kind = tok.kind.clone();
                    let pos = tok.pos;
                    return Err(CqlError::SyntaxError(format!(
                        "expected REPLICATION or DURABLE_WRITES after ALTER KEYSPACE ... WITH, \
                         got {:?} at position {}",
                        kind, pos
                    )));
                }
            }
            if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                break;
            }
        }

        if replication.is_none() && durable_writes.is_none() {
            return Err(CqlError::SyntaxError(
                "ALTER KEYSPACE requires REPLICATION or DURABLE_WRITES".to_string(),
            ));
        }

        Ok(AlterKeyspaceStatement {
            name,
            replication,
            durable_writes,
        })
    }

    fn parse_alter_table(&mut self) -> Result<AlterTableStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Table))?;
        let (keyspace, table) = self.parse_table_ref()?;

        let mut add_columns = vec![];
        let mut drop_columns = vec![];
        let mut rename_columns: Vec<(String, String)> = vec![];
        let mut alter_column_types = vec![];
        let mut extensions = None;
        let mut table_options = vec![];

        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Add) => {
                self.lexer.next_token()?;
                let col_name = self.parse_ident()?;
                let col_type = self.parse_cql_type_name()?;
                add_columns.push((col_name, col_type));
            }
            TokenKind::Keyword(Keyword::Drop) => {
                self.lexer.next_token()?;
                let col_name = self.parse_ident()?;
                drop_columns.push(col_name);
            }
            TokenKind::Keyword(Keyword::Rename) => {
                // RENAME <from> TO <to> [AND <from> TO <to> ...]
                self.lexer.next_token()?;
                loop {
                    let from = self.parse_ident()?;
                    self.lexer.expect(&TokenKind::Keyword(Keyword::To))?;
                    let to = self.parse_ident()?;
                    rename_columns.push((from, to));
                    if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                        break;
                    }
                }
            }
            TokenKind::Keyword(Keyword::Alter) => {
                // ALTER <column> TYPE <new_type>
                self.lexer.next_token()?;
                let col_name = self.parse_ident()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::Type))?;
                let new_type = self.parse_cql_type_name()?;
                alter_column_types.push((col_name, new_type));
            }
            TokenKind::Keyword(Keyword::With) => {
                self.lexer.next_token()?;
                loop {
                    let prop_name = self.parse_ident()?;
                    self.lexer.expect(&TokenKind::Eq)?;
                    if prop_name == "extensions" {
                        extensions = Some(self.parse_string_map()?);
                    } else {
                        let val = self.parse_option_value()?;
                        table_options.push((prop_name, val));
                    }
                    if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                        break;
                    }
                }
            }
            _ => {
                let kind = tok.kind.clone();
                let pos = tok.pos;
                return Err(CqlError::SyntaxError(format!(
                    "expected ADD, DROP, ALTER, RENAME, or WITH after ALTER TABLE, \
                     got {:?} at position {}",
                    kind, pos
                )));
            }
        }

        Ok(AlterTableStatement {
            keyspace,
            table,
            add_columns,
            drop_columns,
            rename_columns,
            alter_column_types,
            extensions,
            table_options,
        })
    }

    // ---------------------------------------------------------------
    // DROP
    // ---------------------------------------------------------------

    fn parse_drop(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Drop))?;
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Table) => self.parse_drop_table().map(Statement::DropTable),
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.parse_drop_keyspace().map(Statement::DropKeyspace)
            }
            TokenKind::Keyword(Keyword::Index) => self.parse_drop_index().map(Statement::DropIndex),
            TokenKind::Keyword(Keyword::Type) => self.parse_drop_type(),
            TokenKind::Keyword(Keyword::Function) => self.parse_drop_function(),
            TokenKind::Keyword(Keyword::Aggregate) => self.parse_drop_aggregate(),
            TokenKind::Keyword(Keyword::Role) => self.parse_drop_role().map(Statement::DropRole),
            // Bare identifier after DROP: treat as DROP TABLE (Cassandra shorthand).
            // e.g., "DROP cycling.race_winners" → DROP TABLE cycling.race_winners
            TokenKind::Ident(_) => self.parse_drop_table().map(Statement::DropTable),
            _ => Err(CqlError::SyntaxError(format!(
                "DROP {:?} not yet supported at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn parse_drop_keyspace(&mut self) -> Result<DropKeyspaceStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Keyspace))?;
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_ident()?;

        Ok(DropKeyspaceStatement { name, if_exists })
    }

    fn parse_drop_role(&mut self) -> Result<DropRoleStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Role))?;
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_ident()?;

        Ok(DropRoleStatement { name, if_exists })
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStatement, CqlError> {
        // TABLE keyword is optional (Cassandra accepts "DROP ks.tbl" shorthand)
        let _ = self.lexer.eat(&TokenKind::Keyword(Keyword::Table))?;
        let if_exists = self.parse_if_exists()?;
        let (keyspace, table) = self.parse_table_ref()?;

        Ok(DropTableStatement {
            keyspace,
            table,
            if_exists,
        })
    }

    // ---------------------------------------------------------------
    // CREATE/ALTER/DROP TYPE (UDT)
    // ---------------------------------------------------------------

    fn parse_create_type(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Type))?;

        let if_not_exists = self.parse_if_not_exists()?;
        let (keyspace, name) = self.parse_table_ref()?;

        self.lexer.expect(&TokenKind::LParen)?;

        let mut fields: Vec<(String, CqlTypeName)> = vec![];
        loop {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::RParen {
                break;
            }
            let field_name = self.parse_ident()?;
            let field_type = self.parse_cql_type_name()?;
            fields.push((field_name, field_type));
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }

        self.lexer.expect(&TokenKind::RParen)?;

        Ok(Statement::CreateType {
            keyspace,
            name,
            if_not_exists,
            fields,
        })
    }

    fn parse_alter_type(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Type))?;

        let (keyspace, name) = self.parse_table_ref()?;

        let mut alterations = vec![];
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Add) => {
                self.lexer.next_token()?;
                let field_name = self.parse_ident()?;
                let field_type = self.parse_cql_type_name()?;
                alterations.push(TypeAlteration::AddField {
                    name: field_name,
                    field_type,
                });
            }
            TokenKind::Keyword(Keyword::Rename) => {
                self.lexer.next_token()?;
                let from = self.parse_ident()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::To))?;
                let to = self.parse_ident()?;
                alterations.push(TypeAlteration::RenameField { from, to });
            }
            _ => {
                return Err(CqlError::SyntaxError(format!(
                    "expected ADD or RENAME after ALTER TYPE, got {:?} at position {}",
                    tok.kind, tok.pos
                )))
            }
        }

        Ok(Statement::AlterType {
            keyspace,
            name,
            alterations,
        })
    }

    fn parse_drop_type(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Type))?;
        let if_exists = self.parse_if_exists()?;
        let (keyspace, name) = self.parse_table_ref()?;

        Ok(Statement::DropType {
            keyspace,
            name,
            if_exists,
        })
    }

    // ---------------------------------------------------------------
    // CREATE/DROP FUNCTION and AGGREGATE (UDF/UDA)
    // ---------------------------------------------------------------

    fn parse_create_function(&mut self, or_replace: bool) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Function))?;

        let if_not_exists = self.parse_if_not_exists()?;
        if or_replace && if_not_exists {
            return Err(CqlError::SyntaxError(
                "cannot combine OR REPLACE with IF NOT EXISTS".to_string(),
            ));
        }

        let (keyspace, name) = self.parse_table_ref()?;

        // Parameter list: ( param_name type [, param_name type ]* )
        self.lexer.expect(&TokenKind::LParen)?;
        let mut params: Vec<(String, CqlTypeName)> = vec![];
        loop {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::RParen {
                break;
            }
            let param_name = self.parse_ident()?;
            let param_type = self.parse_cql_type_name()?;
            params.push((param_name, param_type));
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        self.lexer.expect(&TokenKind::RParen)?;

        // Null-input behavior:
        //   CALLED ON NULL INPUT
        //   RETURNS NULL ON NULL INPUT
        let called_on_null = if self.lexer.eat(&TokenKind::Keyword(Keyword::Called))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Input))?;
            true
        } else {
            self.lexer.expect(&TokenKind::Keyword(Keyword::Returns))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Input))?;
            false
        };

        // RETURNS return_type
        self.lexer.expect(&TokenKind::Keyword(Keyword::Returns))?;
        let return_type = self.parse_cql_type_name()?;

        // LANGUAGE language_name
        self.lexer.expect(&TokenKind::Keyword(Keyword::Language))?;
        let language = self.parse_ident()?;

        // AS 'body' | AS FILE 'path' | AS URL 'url' WITH SHA256 'hex'
        self.lexer.expect(&TokenKind::Keyword(Keyword::As))?;
        let body = if self.eat_ident_ci("file")? {
            FunctionBodySource::File(self.expect_string_literal()?)
        } else if self.eat_ident_ci("url")? {
            let url = self.expect_string_literal()?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::With))?;
            if !self.eat_ident_ci("sha256")? {
                let tok = self.lexer.peek()?;
                return Err(CqlError::SyntaxError(format!(
                    "expected SHA256 after WITH, got {:?} at position {}",
                    tok.kind, tok.pos
                )));
            }
            self.lexer.eat(&TokenKind::Eq)?;
            FunctionBodySource::Url {
                url,
                sha256: self.expect_string_literal()?,
            }
        } else {
            FunctionBodySource::InlineHex(self.expect_string_literal()?)
        };

        Ok(Statement::CreateFunction {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            params,
            called_on_null,
            return_type,
            language,
            body,
        })
    }

    fn parse_drop_function(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Function))?;
        let if_exists = self.parse_if_exists()?;
        let (keyspace, name) = self.parse_table_ref()?;

        // Optional argument type list: ( type [, type ]* )
        let arg_types = if self.lexer.eat(&TokenKind::LParen)? {
            let mut types = vec![];
            loop {
                let tok = self.lexer.peek()?;
                if tok.kind == TokenKind::RParen {
                    break;
                }
                types.push(self.parse_cql_type_name()?);
                if !self.lexer.eat(&TokenKind::Comma)? {
                    break;
                }
            }
            self.lexer.expect(&TokenKind::RParen)?;
            Some(types)
        } else {
            None
        };

        Ok(Statement::DropFunction {
            keyspace,
            name,
            arg_types,
            if_exists,
        })
    }

    fn parse_create_aggregate(&mut self, or_replace: bool) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Aggregate))?;

        let if_not_exists = self.parse_if_not_exists()?;
        if or_replace && if_not_exists {
            return Err(CqlError::SyntaxError(
                "cannot combine OR REPLACE with IF NOT EXISTS".to_string(),
            ));
        }

        let (keyspace, name) = self.parse_table_ref()?;

        // Argument type list: ( type [, type ]* )
        self.lexer.expect(&TokenKind::LParen)?;
        let mut arg_types = vec![];
        loop {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::RParen {
                break;
            }
            arg_types.push(self.parse_cql_type_name()?);
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        self.lexer.expect(&TokenKind::RParen)?;

        // SFUNC sfunc_name
        self.lexer.expect(&TokenKind::Keyword(Keyword::Sfunc))?;
        let state_func = self.parse_ident()?;

        // STYPE state_type
        self.lexer.expect(&TokenKind::Keyword(Keyword::Stype))?;
        let state_type = self.parse_cql_type_name()?;

        // Optional: FINALFUNC finalfunc_name
        let final_func = if self.lexer.eat(&TokenKind::Keyword(Keyword::Finalfunc))? {
            Some(self.parse_ident()?)
        } else {
            None
        };

        // Optional: INITCOND init_value
        let init_cond = if self.lexer.eat(&TokenKind::Keyword(Keyword::Initcond))? {
            Some(self.parse_term()?)
        } else {
            None
        };

        Ok(Statement::CreateAggregate {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            state_func,
            state_type,
            final_func,
            init_cond,
        })
    }

    fn parse_drop_aggregate(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Aggregate))?;
        let if_exists = self.parse_if_exists()?;
        let (keyspace, name) = self.parse_table_ref()?;

        // Optional argument type list: ( type [, type ]* )
        let arg_types = if self.lexer.eat(&TokenKind::LParen)? {
            let mut types = vec![];
            loop {
                let tok = self.lexer.peek()?;
                if tok.kind == TokenKind::RParen {
                    break;
                }
                types.push(self.parse_cql_type_name()?);
                if !self.lexer.eat(&TokenKind::Comma)? {
                    break;
                }
            }
            self.lexer.expect(&TokenKind::RParen)?;
            Some(types)
        } else {
            None
        };

        Ok(Statement::DropAggregate {
            keyspace,
            name,
            arg_types,
            if_exists,
        })
    }

    // ---------------------------------------------------------------
    // CREATE INDEX / DROP INDEX
    // ---------------------------------------------------------------

    fn parse_create_index(&mut self) -> Result<CreateIndexStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Index))?;

        let if_not_exists = self.parse_if_not_exists()?;

        // Optional index name — peek to see if next token is ON (no name) or an identifier (name)
        let name = {
            let tok = self.lexer.peek()?;
            if tok.kind == TokenKind::Keyword(Keyword::On) {
                None
            } else {
                Some(self.parse_ident()?)
            }
        };

        // ON
        self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;

        // Optional keyspace.table
        let (keyspace, table) = self.parse_table_ref()?;

        // (column, column, ...)
        self.lexer.expect(&TokenKind::LParen)?;
        let columns = self.parse_ident_list()?;
        self.lexer.expect(&TokenKind::RParen)?;

        // Optional USING 'type'
        let using = if self.lexer.eat(&TokenKind::Keyword(Keyword::Using))? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::StringLiteral(s) => Some(s),
                _ => {
                    return Err(CqlError::SyntaxError(format!(
                        "expected string literal after USING, got {:?} at position {}",
                        tok.kind, tok.pos
                    )))
                }
            }
        } else {
            None
        };

        // Optional WITH OPTIONS = { map }
        let options = if self.lexer.eat(&TokenKind::Keyword(Keyword::With))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::Options))?;
            self.lexer.expect(&TokenKind::Eq)?;
            self.parse_string_map()?
        } else {
            vec![]
        };

        Ok(CreateIndexStatement {
            name,
            keyspace,
            table,
            columns,
            using,
            filter: None,
            options,
            if_not_exists,
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Index))?;
        let if_exists = self.parse_if_exists()?;

        // Optional keyspace.index_name
        let first = self.parse_ident()?;
        if self.lexer.eat(&TokenKind::Dot)? {
            let second = self.parse_ident()?;
            Ok(DropIndexStatement {
                keyspace: Some(first),
                name: second,
                if_exists,
            })
        } else {
            Ok(DropIndexStatement {
                keyspace: None,
                name: first,
                if_exists,
            })
        }
    }

    // ---------------------------------------------------------------
    // USE
    // ---------------------------------------------------------------

    fn parse_use(&mut self) -> Result<UseStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Use))?;
        let keyspace = self.parse_ident()?;
        Ok(UseStatement { keyspace })
    }

    // ---------------------------------------------------------------
    // TRUNCATE
    // ---------------------------------------------------------------

    fn parse_truncate(&mut self) -> Result<TruncateStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Truncate))?;
        // Optional TABLE keyword
        self.lexer.eat(&TokenKind::Keyword(Keyword::Table))?;
        let (keyspace, table) = self.parse_table_ref()?;
        Ok(TruncateStatement { keyspace, table })
    }

    // ---------------------------------------------------------------
    // COMPACT
    // ---------------------------------------------------------------

    fn parse_compact(&mut self) -> Result<CompactStatement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Compact))?;
        self.lexer.eat(&TokenKind::Keyword(Keyword::Table))?;
        let (keyspace, table) = self.parse_table_ref()?;
        Ok(CompactStatement { keyspace, table })
    }

    // ---------------------------------------------------------------
    // GRANT / REVOKE
    // ---------------------------------------------------------------

    fn parse_grant(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Grant))?;
        // `GRANT ALL ...` is always a permission grant.
        if matches!(self.lexer.peek()?.kind, TokenKind::Keyword(Keyword::All)) {
            let permissions = self.parse_permissions()?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
            let resource = self.parse_resource()?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::To))?;
            let role = self.parse_ident()?;
            return Ok(Statement::Grant(GrantStatement {
                permissions,
                resource,
                role,
            }));
        }

        let first = self.parse_ident()?;
        // `GRANT <role> TO <member>` — role membership (no ON / resource).
        if self.lexer.eat(&TokenKind::Keyword(Keyword::To))? {
            let member = self.parse_ident()?;
            return Ok(Statement::GrantRole {
                role: first,
                member,
            });
        }

        // Otherwise `first` is the first permission of a permission grant.
        let mut permissions = vec![first.to_uppercase()];
        while self.lexer.eat(&TokenKind::Comma)? {
            permissions.push(self.parse_ident()?.to_uppercase());
        }
        self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
        let resource = self.parse_resource()?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::To))?;
        let role = self.parse_ident()?;
        Ok(Statement::Grant(GrantStatement {
            permissions,
            resource,
            role,
        }))
    }

    fn parse_revoke(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Revoke))?;
        if matches!(self.lexer.peek()?.kind, TokenKind::Keyword(Keyword::All)) {
            let permissions = self.parse_permissions()?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
            let resource = self.parse_resource()?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::From))?;
            let role = self.parse_ident()?;
            return Ok(Statement::Revoke(RevokeStatement {
                permissions,
                resource,
                role,
            }));
        }

        let first = self.parse_ident()?;
        // `REVOKE <role> FROM <member>` — role membership.
        if self.lexer.eat(&TokenKind::Keyword(Keyword::From))? {
            let member = self.parse_ident()?;
            return Ok(Statement::RevokeRole {
                role: first,
                member,
            });
        }

        let mut permissions = vec![first.to_uppercase()];
        while self.lexer.eat(&TokenKind::Comma)? {
            permissions.push(self.parse_ident()?.to_uppercase());
        }
        self.lexer.expect(&TokenKind::Keyword(Keyword::On))?;
        let resource = self.parse_resource()?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::From))?;
        let role = self.parse_ident()?;
        Ok(Statement::Revoke(RevokeStatement {
            permissions,
            resource,
            role,
        }))
    }

    fn parse_permissions(&mut self) -> Result<Vec<String>, CqlError> {
        // ALL [PERMISSIONS] or a comma-separated permission list
        // (SELECT, MODIFY, UNMASK, SELECT_MASKED, ...).
        if self.lexer.eat(&TokenKind::Keyword(Keyword::All))? {
            self.lexer.eat(&TokenKind::Keyword(Keyword::Permissions))?;
            Ok(vec!["ALL".to_string()])
        } else {
            let mut perms = vec![self.parse_ident()?.to_uppercase()];
            while self.lexer.eat(&TokenKind::Comma)? {
                perms.push(self.parse_ident()?.to_uppercase());
            }
            Ok(perms)
        }
    }

    fn parse_resource(&mut self) -> Result<GrantResource, CqlError> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::All) => {
                self.lexer.next_token()?;
                let tok2 = self.lexer.peek()?;
                match &tok2.kind {
                    TokenKind::Keyword(Keyword::Keyspace) => {
                        // Not a real keyword combo in CQL, but ALL KEYSPACES
                        self.lexer.next_token()?;
                        // Handle plural
                        Ok(GrantResource::AllKeyspaces)
                    }
                    TokenKind::Keyword(Keyword::Role) => {
                        self.lexer.next_token()?;
                        Ok(GrantResource::AllRoles)
                    }
                    _ => {
                        // "ALL KEYSPACES" — the word "keyspaces" is an ident, not a keyword
                        let ident = self.parse_ident()?;
                        if ident.eq_ignore_ascii_case("keyspaces") {
                            Ok(GrantResource::AllKeyspaces)
                        } else if ident.eq_ignore_ascii_case("roles") {
                            Ok(GrantResource::AllRoles)
                        } else if ident.eq_ignore_ascii_case("functions") {
                            // ALL FUNCTIONS IN KEYSPACE ks
                            self.lexer.expect(&TokenKind::Keyword(Keyword::In))?;
                            self.lexer.expect(&TokenKind::Keyword(Keyword::Keyspace))?;
                            let ks = self.parse_ident()?;
                            Ok(GrantResource::AllFunctions { keyspace: Some(ks) })
                        } else {
                            Err(CqlError::SyntaxError(format!(
                                "expected KEYSPACES, ROLES, or FUNCTIONS after ALL, got '{}'",
                                ident
                            )))
                        }
                    }
                }
            }
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.lexer.next_token()?;
                let ks = self.parse_ident()?;
                Ok(GrantResource::Keyspace(ks))
            }
            TokenKind::Keyword(Keyword::Table) => {
                // Explicit `TABLE [ks.]table` resource form. Resolves to the
                // same Table resource as the bare `[ks.]table` form below —
                // the keyword does not widen the grant's scope.
                self.lexer.next_token()?;
                let (ks, table) = self.parse_table_ref()?;
                Ok(GrantResource::Table(ks, table))
            }
            TokenKind::Keyword(Keyword::Role) => {
                self.lexer.next_token()?;
                let role = self.parse_ident()?;
                Ok(GrantResource::Role(role))
            }
            TokenKind::Keyword(Keyword::Function) => {
                self.lexer.next_token()?;
                // FUNCTION [ks.]name(arg_type, ...)
                let (ks, name) = self.parse_table_ref()?;
                // Parse argument types in parentheses
                let arg_types = if self.lexer.eat(&TokenKind::LParen)? {
                    if self.lexer.eat(&TokenKind::RParen)? {
                        vec![]
                    } else {
                        let mut types = vec![self.parse_cql_type_name()?];
                        while self.lexer.eat(&TokenKind::Comma)? {
                            types.push(self.parse_cql_type_name()?);
                        }
                        self.lexer.expect(&TokenKind::RParen)?;
                        types
                    }
                } else {
                    vec![]
                };
                Ok(GrantResource::Function {
                    keyspace: ks,
                    name,
                    arg_types,
                })
            }
            _ => {
                // Table resource: [ks.]table
                let (ks, table) = self.parse_table_ref()?;
                Ok(GrantResource::Table(ks, table))
            }
        }
    }

    // ---------------------------------------------------------------
    // SUBSCRIBE / UNSUBSCRIBE
    // ---------------------------------------------------------------

    fn parse_subscribe(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Subscribe))?;

        // The inner statement must be a SELECT
        let tok = self.lexer.peek()?;
        if !matches!(tok.kind, TokenKind::Keyword(Keyword::Select)) {
            return Err(CqlError::SyntaxError(
                "SUBSCRIBE requires a SELECT statement".to_string(),
            ));
        }
        let inner = self.parse_select().map(Statement::Select)?;

        // Optional EVERY <duration>
        let interval = if self.lexer.eat(&TokenKind::Keyword(Keyword::Every))? {
            let dur = self.parse_duration()?;
            if dur < Duration::from_millis(500) {
                return Err(CqlError::SyntaxError(format!(
                    "SUBSCRIBE interval must be at least 500ms, got {}ms",
                    dur.as_millis()
                )));
            }
            Some(dur)
        } else {
            None
        };

        // Optional DELTA
        let delta = self.lexer.eat(&TokenKind::Keyword(Keyword::Delta))?;

        Ok(Statement::Subscribe {
            inner: Box::new(inner),
            interval,
            delta,
        })
    }

    fn parse_unsubscribe(&mut self) -> Result<Statement, CqlError> {
        self.lexer
            .expect(&TokenKind::Keyword(Keyword::Unsubscribe))?;

        // Optional integer stream_id
        let tok = self.lexer.peek()?;
        let stream_id = if let TokenKind::IntegerLiteral(n) = tok.kind {
            self.lexer.next_token()?;
            let id = u16::try_from(n)
                .map_err(|_| CqlError::SyntaxError(format!("stream_id out of u16 range: {n}")))?;
            Some(id)
        } else {
            None
        };

        Ok(Statement::Unsubscribe { stream_id })
    }

    fn parse_explain(&mut self) -> Result<Statement, CqlError> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Explain))?;

        // EXPLAIN requires a SELECT statement
        let tok = self.lexer.peek()?;
        if !matches!(tok.kind, TokenKind::Keyword(Keyword::Select)) {
            return Err(CqlError::SyntaxError(
                "EXPLAIN requires a SELECT statement".to_string(),
            ));
        }
        let select = self.parse_select()?;
        Ok(Statement::Explain(Box::new(select)))
    }

    /// Parse a duration string like `5s`, `500ms`, `1m`.
    ///
    /// The lexer produces these as a single `Ident` token (e.g. `"5s"`)
    /// because a digit run followed by letters is read as one alphanumeric word.
    fn parse_duration(&mut self) -> Result<Duration, CqlError> {
        let tok = self.lexer.next_token()?;
        let text = match &tok.kind {
            TokenKind::Ident(s) => s.to_string(),
            TokenKind::StringLiteral(s) => s.to_string(),
            _ => {
                return Err(CqlError::SyntaxError(format!(
                    "expected duration (e.g. 5s, 500ms, 1m), got {:?} at position {}",
                    tok.kind, tok.pos
                )))
            }
        };

        // Split into numeric prefix and unit suffix
        let split_pos = text.find(|c: char| !c.is_ascii_digit()).ok_or_else(|| {
            CqlError::SyntaxError(format!(
                "expected duration with unit suffix (s, ms, m), got {:?}",
                text
            ))
        })?;

        let (num_str, unit) = text.split_at(split_pos);
        let value: u64 = num_str.parse().map_err(|_| {
            CqlError::SyntaxError(format!("invalid duration number: {:?}", num_str))
        })?;

        match unit {
            "s" => Ok(Duration::from_secs(value)),
            "ms" => Ok(Duration::from_millis(value)),
            "m" => Ok(Duration::from_secs(value * 60)),
            _ => Err(CqlError::SyntaxError(format!(
                "unknown duration unit {:?}, expected s, ms, or m",
                unit
            ))),
        }
    }

    // ---------------------------------------------------------------
    // WHERE clauses
    // ---------------------------------------------------------------

    fn parse_where_clauses(&mut self) -> Result<Vec<WhereClause>, CqlError> {
        let mut clauses = vec![];
        loop {
            let column = self.parse_ident()?;

            // Check for token(column) pattern: `token` followed by `(`.
            let has_lparen = self.lexer.eat(&TokenKind::LParen)?;
            let is_token_fn = column.eq_ignore_ascii_case("token") && has_lparen;
            let actual_column = if is_token_fn {
                let col = self.parse_ident()?;
                self.lexer.expect(&TokenKind::RParen)?;
                col
            } else if has_lparen {
                return Err(CqlError::SyntaxError(
                    "UDF calls on the left-hand side of WHERE predicates are not supported; place scalar UDF calls on the right-hand side with ALLOW FILTERING"
                        .to_string(),
                ));
            } else {
                column
            };

            let op = self.parse_comparison_op()?;
            let value = if op == ComparisonOp::In {
                // IN (term, term, ...)
                self.lexer.expect(&TokenKind::LParen)?;
                let terms = self.parse_term_list()?;
                self.lexer.expect(&TokenKind::RParen)?;
                Term::InList(terms)
            } else {
                self.parse_term()?
            };
            clauses.push(WhereClause {
                column: actual_column,
                op,
                value,
                token_fn: is_token_fn,
            });
            if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                break;
            }
        }
        Ok(clauses)
    }

    fn parse_comparison_op(&mut self) -> Result<ComparisonOp, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Eq => Ok(ComparisonOp::Eq),
            TokenKind::Lt => Ok(ComparisonOp::Lt),
            TokenKind::Gt => Ok(ComparisonOp::Gt),
            TokenKind::LtEq => Ok(ComparisonOp::Le),
            TokenKind::GtEq => Ok(ComparisonOp::Ge),
            TokenKind::NotEq => Ok(ComparisonOp::Ne),
            TokenKind::Keyword(Keyword::In) => Ok(ComparisonOp::In),
            TokenKind::Keyword(Keyword::Contains) => {
                // Check for CONTAINS KEY
                if self.lexer.eat(&TokenKind::Keyword(Keyword::Key))? {
                    Ok(ComparisonOp::ContainsKey)
                } else {
                    Ok(ComparisonOp::Contains)
                }
            }
            TokenKind::Keyword(Keyword::Sounds) => {
                // SOUNDS LIKE — phonetic comparison operator
                self.lexer.expect(&TokenKind::Keyword(Keyword::Like))?;
                Ok(ComparisonOp::SoundsLike)
            }
            TokenKind::Keyword(Keyword::Like) => Ok(ComparisonOp::Like),
            _ => Err(CqlError::SyntaxError(format!(
                "expected comparison operator, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    // ---------------------------------------------------------------
    // Term parsing (with nesting depth tracking — M2)
    // ---------------------------------------------------------------

    fn parse_term(&mut self) -> Result<Term, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::StringLiteral(s) => Ok(Term::StringLiteral(s)),
            TokenKind::IntegerLiteral(n) => Ok(Term::IntegerLiteral(n)),
            TokenKind::FloatLiteral(f) => Ok(Term::FloatLiteral(f)),
            TokenKind::UuidLiteral(u) => Ok(Term::UuidLiteral(u)),
            TokenKind::BlobLiteral(b) => Ok(Term::BlobLiteral(b)),
            TokenKind::Keyword(Keyword::True) => Ok(Term::BoolLiteral(true)),
            TokenKind::Keyword(Keyword::False) => Ok(Term::BoolLiteral(false)),
            TokenKind::Keyword(Keyword::Null) => Ok(Term::Null),
            TokenKind::QuestionMark => Ok(Term::BindMarker(None)),
            TokenKind::NamedBind(name) => Ok(Term::BindMarker(Some(name))),
            TokenKind::Minus => {
                // Negative number
                let next = self.lexer.next_token()?;
                match next.kind {
                    TokenKind::IntegerLiteral(n) => {
                        let neg = n.checked_neg().ok_or_else(|| {
                            CqlError::SyntaxError(format!("integer overflow negating {n}"))
                        })?;
                        Ok(Term::IntegerLiteral(neg))
                    }
                    TokenKind::FloatLiteral(f) => Ok(Term::FloatLiteral(-f)),
                    _ => Err(CqlError::SyntaxError(format!(
                        "expected number after '-', got {:?} at position {}",
                        next.kind, next.pos
                    ))),
                }
            }
            TokenKind::LBracket => {
                // List literal: [a, b, c]
                self.enter_nesting()?;
                let result = self.parse_list_literal();
                self.exit_nesting();
                result
            }
            TokenKind::LBrace => {
                // Map or set literal: {k:v, ...} or {a, b, ...}
                self.enter_nesting()?;
                let result = self.parse_map_or_set_literal();
                self.exit_nesting();
                result
            }
            TokenKind::LParen => {
                // Tuple literal: (a, b, ...)
                self.enter_nesting()?;
                let result = self.parse_tuple_literal();
                self.exit_nesting();
                result
            }
            // Identifiers and function calls
            TokenKind::Ident(name) => {
                let name = name.to_string();
                if self.lexer.eat(&TokenKind::LParen)? {
                    // Function call: name(args...)
                    self.enter_nesting()?;
                    let mut args = vec![];
                    if self.lexer.peek()?.kind != TokenKind::RParen {
                        args.push(self.parse_term()?);
                        while self.lexer.eat(&TokenKind::Comma)? {
                            args.push(self.parse_term()?);
                        }
                    }
                    self.lexer.expect(&TokenKind::RParen)?;
                    self.exit_nesting();
                    Ok(Term::FunctionCall {
                        keyspace: None,
                        name,
                        args,
                    })
                } else {
                    // Bare identifier in term position (column reference)
                    Ok(Term::FunctionCall {
                        keyspace: None,
                        name,
                        args: vec![],
                    })
                }
            }
            // Keywords that can be used as function names (uuid, now, toDate, etc.)
            TokenKind::Keyword(kw) => {
                let name = Self::keyword_as_ident(kw);
                if self.lexer.eat(&TokenKind::LParen)? {
                    self.enter_nesting()?;
                    let mut args = vec![];
                    if self.lexer.peek()?.kind != TokenKind::RParen {
                        args.push(self.parse_term()?);
                        while self.lexer.eat(&TokenKind::Comma)? {
                            args.push(self.parse_term()?);
                        }
                    }
                    self.lexer.expect(&TokenKind::RParen)?;
                    self.exit_nesting();
                    Ok(Term::FunctionCall {
                        keyspace: None,
                        name,
                        args,
                    })
                } else {
                    // Keyword used as bare identifier — this is likely a syntax error
                    // in most contexts, but we let the caller decide.
                    Ok(Term::FunctionCall {
                        keyspace: None,
                        name,
                        args: vec![],
                    })
                }
            }
            _ => Err(CqlError::SyntaxError(format!(
                "expected term, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn parse_list_literal(&mut self) -> Result<Term, CqlError> {
        let mut elements = vec![];
        if self.lexer.eat(&TokenKind::RBracket)? {
            return Ok(Term::ListLiteral(elements));
        }
        loop {
            if elements.len() >= MAX_COLLECTION_ELEMENTS {
                return Err(CqlError::SyntaxError("collection too large".to_string()));
            }
            elements.push(self.parse_term()?);
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        self.lexer.expect(&TokenKind::RBracket)?;
        Ok(Term::ListLiteral(elements))
    }

    fn parse_map_or_set_literal(&mut self) -> Result<Term, CqlError> {
        // Empty braces → empty map (we choose map; empty set and empty map look the same)
        if self.lexer.eat(&TokenKind::RBrace)? {
            return Ok(Term::MapLiteral(vec![]));
        }

        // Parse first term, then disambiguate
        let first = self.parse_term()?;

        if self.lexer.eat(&TokenKind::Colon)? {
            // Map literal
            let first_val = self.parse_term()?;
            let mut entries = vec![(first, first_val)];
            while self.lexer.eat(&TokenKind::Comma)? {
                if entries.len() >= MAX_COLLECTION_ELEMENTS {
                    return Err(CqlError::SyntaxError("collection too large".to_string()));
                }
                let k = self.parse_term()?;
                self.lexer.expect(&TokenKind::Colon)?;
                let v = self.parse_term()?;
                entries.push((k, v));
            }
            self.lexer.expect(&TokenKind::RBrace)?;
            Ok(Term::MapLiteral(entries))
        } else {
            // Set literal
            let mut elements = vec![first];
            while self.lexer.eat(&TokenKind::Comma)? {
                if elements.len() >= MAX_COLLECTION_ELEMENTS {
                    return Err(CqlError::SyntaxError("collection too large".to_string()));
                }
                elements.push(self.parse_term()?);
            }
            self.lexer.expect(&TokenKind::RBrace)?;
            Ok(Term::SetLiteral(elements))
        }
    }

    fn parse_tuple_literal(&mut self) -> Result<Term, CqlError> {
        let mut elements = vec![];
        if self.lexer.eat(&TokenKind::RParen)? {
            return Ok(Term::TupleLiteral(elements));
        }
        loop {
            if elements.len() >= MAX_COLLECTION_ELEMENTS {
                return Err(CqlError::SyntaxError("collection too large".to_string()));
            }
            elements.push(self.parse_term()?);
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        self.lexer.expect(&TokenKind::RParen)?;
        Ok(Term::TupleLiteral(elements))
    }

    // ---------------------------------------------------------------
    // CQL type name parsing (with nesting depth — M2)
    // ---------------------------------------------------------------

    fn parse_cql_type_name(&mut self) -> Result<CqlTypeName, CqlError> {
        let tok = self.lexer.next_token()?;
        let type_name = match &tok.kind {
            TokenKind::Keyword(kw) => Self::keyword_to_type_string(kw),
            TokenKind::Ident(s) => Some(s.to_lowercase()),
            _ => None,
        };

        let type_name = type_name.ok_or_else(|| {
            CqlError::SyntaxError(format!(
                "expected type name, got {:?} at position {}",
                tok.kind, tok.pos
            ))
        })?;

        match type_name.as_str() {
            "list" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let inner = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::List(Box::new(inner)))
            }
            "set" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let inner = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::Set(Box::new(inner)))
            }
            "map" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let key_type = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Comma)?;
                let val_type = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::Map(Box::new(key_type), Box::new(val_type)))
            }
            "tuple" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let mut types = vec![self.parse_cql_type_name()?];
                while self.lexer.eat(&TokenKind::Comma)? {
                    types.push(self.parse_cql_type_name()?);
                }
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::Tuple(types))
            }
            "frozen" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let inner = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::Frozen(Box::new(inner)))
            }
            "vector" => {
                self.enter_nesting()?;
                self.lexer.expect(&TokenKind::Lt)?;
                let elem_type = self.parse_cql_type_name()?;
                self.lexer.expect(&TokenKind::Comma)?;
                // Dimension is an integer literal inside the angle brackets
                let dim_tok = self.lexer.next_token()?;
                let dimension = match &dim_tok.kind {
                    TokenKind::IntegerLiteral(n) => {
                        if *n <= 0 {
                            return Err(CqlError::SyntaxError(
                                "vector dimension must be a positive integer".into(),
                            ));
                        }
                        *n as usize
                    }
                    _ => {
                        return Err(CqlError::SyntaxError(format!(
                            "expected integer dimension for vector type, got {:?}",
                            dim_tok.kind
                        )));
                    }
                };
                self.lexer.expect(&TokenKind::Gt)?;
                self.exit_nesting();
                Ok(CqlTypeName::Vector(Box::new(elem_type), dimension))
            }
            _ => Ok(CqlTypeName::Simple(type_name)),
        }
    }

    /// Map keywords to their type name strings for CQL column types.
    fn keyword_to_type_string(kw: &Keyword) -> Option<String> {
        let s = match kw {
            Keyword::Int => "int",
            Keyword::Bigint => "bigint",
            Keyword::Text => "text",
            Keyword::Varchar => "varchar",
            Keyword::Blob => "blob",
            Keyword::Boolean => "boolean",
            Keyword::Float => "float",
            Keyword::Double => "double",
            Keyword::Uuid => "uuid",
            Keyword::Timeuuid => "timeuuid",
            Keyword::Inet => "inet",
            Keyword::Varint => "varint",
            Keyword::Decimal => "decimal",
            Keyword::Date => "date",
            Keyword::Time => "time",
            Keyword::Timestamp => "timestamp",
            Keyword::Smallint => "smallint",
            Keyword::Tinyint => "tinyint",
            Keyword::Ascii => "ascii",
            Keyword::Counter => "counter",
            Keyword::List => "list",
            Keyword::Set => "set",
            Keyword::Map => "map",
            Keyword::Tuple => "tuple",
            Keyword::Frozen => "frozen",
            _ => return None,
        };
        Some(s.to_string())
    }

    // ---------------------------------------------------------------
    // USING clause
    // ---------------------------------------------------------------

    fn parse_using_clause(&mut self) -> Result<(Option<Term>, Option<Term>), CqlError> {
        if self.lexer.eat(&TokenKind::Keyword(Keyword::Using))? {
            self.parse_using_clause_body()
        } else {
            Ok((None, None))
        }
    }

    /// Parse the body of a `USING` clause. Each value is captured as a
    /// `Term` (literal or bind marker) so the prepare path can register
    /// `USING TIMESTAMP ?` / `USING TTL ?` placeholders correctly
    /// (Gap 10 in `ferrosa-nosqlbench/docs/initial-gaps-found.md`).
    fn parse_using_clause_body(&mut self) -> Result<(Option<Term>, Option<Term>), CqlError> {
        let mut timestamp: Option<Term> = None;
        let mut ttl: Option<Term> = None;
        loop {
            let tok = self.lexer.peek()?;
            match &tok.kind {
                TokenKind::Keyword(Keyword::Timestamp) => {
                    self.lexer.next_token()?;
                    let val_tok = self.lexer.next_token()?;
                    match val_tok.kind {
                        TokenKind::IntegerLiteral(n) => timestamp = Some(Term::IntegerLiteral(n)),
                        TokenKind::QuestionMark => timestamp = Some(Term::BindMarker(None)),
                        TokenKind::NamedBind(name) => {
                            timestamp = Some(Term::BindMarker(Some(name.to_string())))
                        }
                        _ => {
                            return Err(CqlError::SyntaxError(format!(
                                "expected integer or bind marker after TIMESTAMP, \
                                 got {:?} at position {}",
                                val_tok.kind, val_tok.pos
                            )))
                        }
                    }
                }
                TokenKind::Keyword(Keyword::Ttl) => {
                    self.lexer.next_token()?;
                    let val_tok = self.lexer.next_token()?;
                    match val_tok.kind {
                        TokenKind::IntegerLiteral(n) => ttl = Some(Term::IntegerLiteral(n)),
                        TokenKind::QuestionMark => ttl = Some(Term::BindMarker(None)),
                        TokenKind::NamedBind(name) => {
                            ttl = Some(Term::BindMarker(Some(name.to_string())))
                        }
                        _ => {
                            return Err(CqlError::SyntaxError(format!(
                                "expected integer or bind marker after TTL, \
                                 got {:?} at position {}",
                                val_tok.kind, val_tok.pos
                            )))
                        }
                    }
                }
                _ => {
                    return Err(CqlError::SyntaxError(format!(
                        "expected TIMESTAMP or TTL in USING clause, got {:?} at position {}",
                        tok.kind, tok.pos
                    )))
                }
            }
            if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                break;
            }
        }
        Ok((timestamp, ttl))
    }

    // ---------------------------------------------------------------
    // Shared helpers
    // ---------------------------------------------------------------

    /// Parse a table reference: `ident` or `ident.ident` → (keyspace, table).
    fn parse_table_ref(&mut self) -> Result<(Option<String>, String), CqlError> {
        let first = self.parse_ident()?;
        if self.lexer.eat(&TokenKind::Dot)? {
            let second = self.parse_ident()?;
            Ok((Some(first), second))
        } else {
            Ok((None, first))
        }
    }

    /// Parse an identifier from the next token.
    ///
    /// Accepts unquoted identifiers (lowercased), quoted identifiers
    /// (case-preserved), and keywords used as identifiers.
    fn parse_ident(&mut self) -> Result<String, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(s) => Ok(s.to_lowercase()),
            TokenKind::QuotedIdent(s) => Ok(s), // preserve case for quoted
            TokenKind::Keyword(kw) => Ok(Self::keyword_as_ident(kw)),
            _ => Err(CqlError::SyntaxError(format!(
                "expected identifier, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    fn eat_ident_ci(&mut self, expected: &str) -> Result<bool, CqlError> {
        let tok = self.lexer.peek()?;
        let matches = match &tok.kind {
            TokenKind::Ident(s) => s.eq_ignore_ascii_case(expected),
            TokenKind::QuotedIdent(s) => s.eq_ignore_ascii_case(expected),
            TokenKind::Keyword(kw) => Self::keyword_as_ident(*kw).eq_ignore_ascii_case(expected),
            _ => false,
        };
        if matches {
            self.lexer.next_token()?;
        }
        Ok(matches)
    }

    /// Convert a keyword to its lowercase string form for use as an identifier.
    fn keyword_as_ident(kw: Keyword) -> String {
        match kw {
            Keyword::Select => "select",
            Keyword::Insert => "insert",
            Keyword::Update => "update",
            Keyword::Delete => "delete",
            Keyword::Create => "create",
            Keyword::Alter => "alter",
            Keyword::Drop => "drop",
            Keyword::From => "from",
            Keyword::Where => "where",
            Keyword::And => "and",
            Keyword::Or => "or",
            Keyword::In => "in",
            Keyword::Set => "set",
            Keyword::Into => "into",
            Keyword::Values => "values",
            Keyword::If => "if",
            Keyword::Exists => "exists",
            Keyword::Not => "not",
            Keyword::Primary => "primary",
            Keyword::Key => "key",
            Keyword::Table => "table",
            Keyword::Keyspace => "keyspace",
            Keyword::Role => "role",
            Keyword::Grant => "grant",
            Keyword::Revoke => "revoke",
            Keyword::On => "on",
            Keyword::To => "to",
            Keyword::Of => "of",
            Keyword::Use => "use",
            Keyword::Batch => "batch",
            Keyword::Begin => "begin",
            Keyword::Apply => "apply",
            Keyword::Unlogged => "unlogged",
            Keyword::Counter => "counter",
            Keyword::Logged => "logged",
            Keyword::Truncate => "truncate",
            Keyword::Order => "order",
            Keyword::By => "by",
            Keyword::Asc => "asc",
            Keyword::Desc => "desc",
            Keyword::Limit => "limit",
            Keyword::Allow => "allow",
            Keyword::Filtering => "filtering",
            Keyword::With => "with",
            Keyword::Replication => "replication",
            Keyword::DurableWrites => "durable_writes",
            Keyword::Password => "password", // pragma: allowlist secret
            Keyword::Superuser => "superuser",
            Keyword::Login => "login",
            Keyword::Nosuperuser => "nosuperuser",
            Keyword::Nologin => "nologin",
            Keyword::True => "true",
            Keyword::False => "false",
            Keyword::Null => "null",
            Keyword::Using => "using",
            Keyword::Timestamp => "timestamp",
            Keyword::Ttl => "ttl",
            Keyword::Int => "int",
            Keyword::Bigint => "bigint",
            Keyword::Text => "text",
            Keyword::Varchar => "varchar",
            Keyword::Blob => "blob",
            Keyword::Boolean => "boolean",
            Keyword::Float => "float",
            Keyword::Double => "double",
            Keyword::Uuid => "uuid",
            Keyword::Timeuuid => "timeuuid",
            Keyword::Inet => "inet",
            Keyword::Varint => "varint",
            Keyword::Decimal => "decimal",
            Keyword::Date => "date",
            Keyword::Time => "time",
            Keyword::Smallint => "smallint",
            Keyword::Tinyint => "tinyint",
            Keyword::Ascii => "ascii",
            Keyword::List => "list",
            Keyword::Map => "map",
            Keyword::Tuple => "tuple",
            Keyword::Frozen => "frozen",
            Keyword::Static => "static",
            Keyword::Clustering => "clustering",
            Keyword::Compact => "compact",
            Keyword::Storage => "storage",
            Keyword::Token => "token",
            Keyword::Writetime => "writetime",
            Keyword::All => "all",
            Keyword::Permissions => "permissions",
            Keyword::Index => "index",
            Keyword::Options => "options",
            Keyword::Subscribe => "subscribe",
            Keyword::Unsubscribe => "unsubscribe",
            Keyword::Every => "every",
            Keyword::Delta => "delta",
            Keyword::Type => "type",
            Keyword::Rename => "rename",
            Keyword::Add => "add",
            Keyword::Function => "function",
            Keyword::Returns => "returns",
            Keyword::Language => "language",
            Keyword::Called => "called",
            Keyword::Input => "input",
            Keyword::Replace => "replace",
            Keyword::Aggregate => "aggregate",
            Keyword::Sfunc => "sfunc",
            Keyword::Stype => "stype",
            Keyword::Finalfunc => "finalfunc",
            Keyword::Initcond => "initcond",
            Keyword::As => "as",
            Keyword::Contains => "contains",
            Keyword::Explain => "explain",
            Keyword::Distinct => "distinct",
            Keyword::Transaction => "transaction",
            Keyword::Commit => "commit",
            Keyword::Rollback => "rollback",
            Keyword::Sounds => "sounds",
            Keyword::Like => "like",
            Keyword::User => "user",
        }
        .to_string()
    }

    /// Parse a comma-separated list of identifiers.
    fn parse_ident_list(&mut self) -> Result<Vec<String>, CqlError> {
        let mut idents = vec![self.parse_ident()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            idents.push(self.parse_ident()?);
        }
        Ok(idents)
    }

    /// Parse a comma-separated list of terms.
    fn parse_term_list(&mut self) -> Result<Vec<Term>, CqlError> {
        let mut terms = vec![];
        loop {
            if terms.len() >= MAX_COLLECTION_ELEMENTS {
                return Err(CqlError::SyntaxError("collection too large".to_string()));
            }
            terms.push(self.parse_term()?);
            if !self.lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
        Ok(terms)
    }

    /// Parse IF NOT EXISTS, returning true if present.
    fn parse_if_not_exists(&mut self) -> Result<bool, CqlError> {
        if self.lexer.eat(&TokenKind::Keyword(Keyword::If))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::Not))?;
            self.lexer.expect(&TokenKind::Keyword(Keyword::Exists))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Parse IF EXISTS, returning true if present.
    ///
    /// Used by DDL statements (DROP TABLE, etc.) that only support IF EXISTS.
    fn parse_if_exists(&mut self) -> Result<bool, CqlError> {
        if self.lexer.eat(&TokenKind::Keyword(Keyword::If))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::Exists))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Parse an IF clause on UPDATE/DELETE: either `IF EXISTS` or
    /// `IF col op value [AND col op value ...]`.
    ///
    /// Returns `(if_exists, if_conditions)`.
    fn parse_if_clause(&mut self) -> Result<(bool, Vec<IfCondition>), CqlError> {
        if !self.lexer.eat(&TokenKind::Keyword(Keyword::If))? {
            return Ok((false, vec![]));
        }
        if self.lexer.eat(&TokenKind::Keyword(Keyword::Exists))? {
            return Ok((true, vec![]));
        }
        let conditions = self.parse_if_conditions()?;
        Ok((false, conditions))
    }

    /// Parse one or more IF conditions: `col op value [AND col op value ...]`.
    fn parse_if_conditions(&mut self) -> Result<Vec<IfCondition>, CqlError> {
        let mut conditions = Vec::new();
        loop {
            let column = self.parse_ident()?;
            let operator = self.parse_if_operator()?;
            let value = if operator == IfOperator::In {
                self.lexer.expect(&TokenKind::LParen)?;
                let terms = self.parse_term_list()?;
                self.lexer.expect(&TokenKind::RParen)?;
                Term::InList(terms)
            } else {
                self.parse_term()?
            };
            conditions.push(IfCondition {
                column,
                operator,
                value,
            });
            if !self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
                break;
            }
        }
        Ok(conditions)
    }

    /// Parse a comparison operator in an IF condition.
    fn parse_if_operator(&mut self) -> Result<IfOperator, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Eq => Ok(IfOperator::Eq),
            TokenKind::NotEq => Ok(IfOperator::NotEq),
            TokenKind::Lt => Ok(IfOperator::Lt),
            TokenKind::Gt => Ok(IfOperator::Gt),
            TokenKind::LtEq => Ok(IfOperator::LtEq),
            TokenKind::GtEq => Ok(IfOperator::GtEq),
            TokenKind::Keyword(Keyword::In) => Ok(IfOperator::In),
            _ => Err(CqlError::SyntaxError(format!(
                "expected IF condition operator, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }

    /// Parse a boolean value (true/false).
    fn parse_bool(&mut self) -> Result<bool, CqlError> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Keyword(Keyword::True) => Ok(true),
            TokenKind::Keyword(Keyword::False) => Ok(false),
            _ => Err(CqlError::SyntaxError(format!(
                "expected true or false, got {:?} at position {}",
                tok.kind, tok.pos
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // DML tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_select_star() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns, vec![SelectColumn::Star]);
                assert_eq!(s.table, "users");
                assert!(s.keyspace.is_none());
                assert!(s.where_clauses.is_empty());
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_select_with_keyspace() {
        let stmt = parse("SELECT id, name FROM ks.users WHERE id = 42").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(!s.distinct);
                assert_eq!(
                    s.columns,
                    vec![
                        SelectColumn::Column("id".into()),
                        SelectColumn::Column("name".into()),
                    ]
                );
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.table, "users");
                assert_eq!(s.where_clauses.len(), 1);
                assert_eq!(s.where_clauses[0].column, "id");
                assert_eq!(s.where_clauses[0].op, ComparisonOp::Eq);
                assert_eq!(s.where_clauses[0].value, Term::IntegerLiteral(42));
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_select_where_like() {
        let stmt =
            parse("SELECT * FROM cycling.cyclist_name WHERE firstname LIKE 'm%' ALLOW FILTERING")
                .unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clauses.len(), 1);
                assert_eq!(s.where_clauses[0].column, "firstname");
                assert_eq!(s.where_clauses[0].op, ComparisonOp::Like);
                assert_eq!(s.where_clauses[0].value, Term::StringLiteral("m%".into()));
                assert!(s.allow_filtering);
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_select_with_order_limit() {
        let stmt = parse("SELECT * FROM events WHERE pk = 1 ORDER BY ts DESC LIMIT 10").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by, vec![("ts".into(), OrderDirection::Desc)]);
                assert_eq!(s.limit, Some(crate::ast::Limit::Literal(10)));
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_select_with_parameterized_limit() {
        // Positional bind marker
        let stmt = parse("SELECT * FROM t WHERE pk = 1 LIMIT ?").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.limit, Some(crate::ast::Limit::BindMarker));
            }
            other => panic!("expected Select, got {:?}", other),
        }

        // Named bind marker
        let stmt = parse("SELECT * FROM t WHERE pk = 1 LIMIT :page_size").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(
                    s.limit,
                    Some(crate::ast::Limit::NamedBindMarker("page_size".into()))
                );
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert() {
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'alice')").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(
                    s.values,
                    vec![Term::IntegerLiteral(1), Term::StringLiteral("alice".into()),]
                );
                assert!(!s.if_not_exists);
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_if_not_exists() {
        let stmt = parse("INSERT INTO t (k) VALUES (1) IF NOT EXISTS").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert!(s.if_not_exists);
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_using_timestamp_ttl() {
        let stmt = parse("INSERT INTO t (k, v) VALUES (1, 'x') USING TIMESTAMP 12345 AND TTL 3600")
            .unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(s.using_timestamp, Some(Term::IntegerLiteral(12345)));
                assert_eq!(s.using_ttl, Some(Term::IntegerLiteral(3600)));
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_update() {
        let stmt = parse("UPDATE users SET name = 'bob' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(
                    s.assignments,
                    vec![Assignment::Simple {
                        column: "name".into(),
                        value: Term::StringLiteral("bob".into()),
                    }]
                );
                assert_eq!(s.where_clauses.len(), 1);
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_collection_add() {
        let stmt = parse("UPDATE t SET tags = tags + {'new_tag'} WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.table, "t");
                assert_eq!(s.assignments.len(), 1);
                match &s.assignments[0] {
                    Assignment::Add { column, value } => {
                        assert_eq!(column, "tags");
                        assert!(
                            matches!(value, Term::SetLiteral(elems) if elems.len() == 1),
                            "expected SetLiteral, got {:?}",
                            value
                        );
                    }
                    other => panic!("expected Add assignment, got {:?}", other),
                }
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_collection_sub() {
        let stmt = parse("UPDATE t SET items = items - ['old'] WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.assignments.len(), 1);
                match &s.assignments[0] {
                    Assignment::Sub { column, value } => {
                        assert_eq!(column, "items");
                        assert!(
                            matches!(value, Term::ListLiteral(elems) if elems.len() == 1),
                            "expected ListLiteral, got {:?}",
                            value
                        );
                    }
                    other => panic!("expected Sub assignment, got {:?}", other),
                }
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert_eq!(s.table, "users");
                assert!(s.columns.is_empty());
                assert_eq!(s.where_clauses.len(), 1);
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete_columns() {
        let stmt = parse("DELETE name, email FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert_eq!(
                    s.columns,
                    vec![
                        DeleteTarget::Column("name".into()),
                        DeleteTarget::Column("email".into()),
                    ]
                );
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete_map_element() {
        let stmt = parse("DELETE social_links['twitter'] FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert_eq!(
                    s.columns,
                    vec![DeleteTarget::MapElement {
                        column: "social_links".into(),
                        key: Term::StringLiteral("twitter".into()),
                    }]
                );
                assert_eq!(s.table, "users");
                assert_eq!(s.where_clauses.len(), 1);
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn parse_batch() {
        let stmt = parse(
            "BEGIN BATCH INSERT INTO t (k) VALUES (1); INSERT INTO t (k) VALUES (2); APPLY BATCH",
        )
        .unwrap();
        match stmt {
            Statement::Batch(s) => {
                assert_eq!(s.batch_type, BatchType::Logged);
                assert_eq!(s.statements.len(), 2);
                assert!(matches!(s.statements[0], Statement::Insert(_)));
                assert!(matches!(s.statements[1], Statement::Insert(_)));
            }
            other => panic!("expected Batch, got {:?}", other),
        }
    }

    #[test]
    fn parse_bind_markers() {
        let stmt = parse("INSERT INTO t (k, v) VALUES (?, :val)").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values,
                    vec![Term::BindMarker(None), Term::BindMarker(Some("val".into())),]
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_collection_literals() {
        let stmt = parse("INSERT INTO t (k, tags) VALUES (1, ['a', 'b'])").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values,
                    vec![
                        Term::IntegerLiteral(1),
                        Term::ListLiteral(vec![
                            Term::StringLiteral("a".into()),
                            Term::StringLiteral("b".into()),
                        ]),
                    ]
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_scientific_float_values_in_list_literal() {
        let stmt = parse(
            "INSERT INTO entity_embeddings (tenant_id, entity_id, embedding) \
             VALUES (11111111-1111-1111-1111-111111111111, \
                     22222222-2222-2222-2222-222222222222, \
                     [1.23456789e-05, 9.87654321e-01])",
        )
        .unwrap();

        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values[2],
                    Term::ListLiteral(vec![
                        Term::FloatLiteral(1.23456789e-05),
                        Term::FloatLiteral(9.87654321e-01),
                    ])
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // DDL tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_alter_keyspace_replication() {
        let stmt = parse(
            "ALTER KEYSPACE excelsior WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor' : '4'}",
        )
        .unwrap();
        match stmt {
            Statement::AlterKeyspace(s) => {
                assert_eq!(s.name, "excelsior");
                assert_eq!(
                    s.replication,
                    Some(vec![
                        ("class".into(), "SimpleStrategy".into()),
                        ("replication_factor".into(), "4".into()),
                    ])
                );
                assert_eq!(s.durable_writes, None);
            }
            other => panic!("expected AlterKeyspace, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_keyspace_durable_writes_only() {
        let stmt = parse("ALTER KEYSPACE ks WITH DURABLE_WRITES = false").unwrap();
        match stmt {
            Statement::AlterKeyspace(s) => {
                assert_eq!(s.name, "ks");
                assert_eq!(s.replication, None);
                assert_eq!(s.durable_writes, Some(false));
            }
            other => panic!("expected AlterKeyspace, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_keyspace() {
        let stmt = parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        match stmt {
            Statement::CreateKeyspace(s) => {
                assert_eq!(s.name, "ks");
                assert!(!s.if_not_exists);
                assert_eq!(
                    s.replication,
                    vec![
                        ("class".into(), "SimpleStrategy".into()),
                        ("replication_factor".into(), "1".into()),
                    ]
                );
            }
            other => panic!("expected CreateKeyspace, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table() {
        let stmt =
            parse("CREATE TABLE ks.users (id uuid, name text, age int, PRIMARY KEY (id))").unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.name, "users");
                assert_eq!(s.columns.len(), 3);
                assert_eq!(s.partition_key, vec!["id".to_string()]);
                assert!(s.clustering_key.is_empty());
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table_composite_key() {
        let stmt = parse(
            "CREATE TABLE t (pk1 text, pk2 int, ck timestamp, v text, PRIMARY KEY ((pk1, pk2), ck)) WITH CLUSTERING ORDER BY (ck DESC)",
        )
        .unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(s.partition_key, vec!["pk1".to_string(), "pk2".to_string()]);
                assert_eq!(
                    s.clustering_key,
                    vec![("ck".to_string(), ClusteringOrder::Desc)]
                );
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_add() {
        let stmt = parse("ALTER TABLE users ADD email text").unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(
                    s.add_columns,
                    vec![("email".into(), CqlTypeName::Simple("text".into()))]
                );
                assert!(s.drop_columns.is_empty());
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_rename() {
        let stmt = parse("ALTER TABLE cycling.cyclist_name RENAME id TO cyclist_id").unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(s.table, "cyclist_name");
                assert_eq!(s.rename_columns, vec![("id".into(), "cyclist_id".into())]);
                assert!(s.alter_column_types.is_empty());
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_rename_multiple() {
        let stmt = parse("ALTER TABLE t RENAME a TO b AND c TO d").unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(
                    s.rename_columns,
                    vec![("a".into(), "b".into()), ("c".into(), "d".into())]
                );
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_alter_type() {
        let stmt =
            parse("ALTER TABLE cycling.cyclist_alt_stats ALTER favorite_color TYPE text").unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(
                    s.alter_column_types,
                    vec![("favorite_color".into(), CqlTypeName::Simple("text".into()))]
                );
                assert!(s.rename_columns.is_empty());
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_with_extensions() {
        let stmt =
            parse("ALTER TABLE ks.tbl WITH extensions = {'vertex_label': 'Person'}").unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.table, "tbl");
                assert!(s.add_columns.is_empty());
                assert!(s.drop_columns.is_empty());
                let ext = s.extensions.expect("extensions should be Some");
                assert_eq!(ext.len(), 1);
                assert_eq!(ext[0], ("vertex_label".into(), "Person".into()));
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_table_with_extensions_multi() {
        let stmt = parse(
            "ALTER TABLE social.users WITH extensions = {'graph.type': 'vertex', 'graph.label': 'Person'}"
        ).unwrap();
        match stmt {
            Statement::AlterTable(s) => {
                assert_eq!(s.keyspace, Some("social".into()));
                assert_eq!(s.table, "users");
                let ext = s.extensions.expect("extensions should be Some");
                assert_eq!(ext.len(), 2);
                assert_eq!(ext[0], ("graph.type".into(), "vertex".into()));
                assert_eq!(ext[1], ("graph.label".into(), "Person".into()));
            }
            other => panic!("expected AlterTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table_with_extensions_map() {
        let stmt = parse(
            "CREATE TABLE ks.t (id uuid PRIMARY KEY) WITH extensions = {'graph.type': 'vertex', 'graph.label': 'Person'}"
        ).unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.name, "t");
                let ext = s.extensions.expect("extensions should be Some");
                assert_eq!(ext.len(), 2);
                assert_eq!(ext[0], ("graph.type".into(), "vertex".into()));
                assert_eq!(ext[1], ("graph.label".into(), "Person".into()));
                // table_options should not contain extensions
                assert!(s.table_options.is_empty());
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table_with_simple_option_still_works() {
        let stmt =
            parse("CREATE TABLE t (id int PRIMARY KEY, v text) WITH comment = 'hello'").unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(s.name, "t");
                assert_eq!(s.table_options, vec![("comment".into(), "hello".into())]);
                assert!(s.extensions.is_none());
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table_with_extensions_and_other_options() {
        let stmt = parse(
            "CREATE TABLE ks.t (id uuid PRIMARY KEY) WITH gc_grace_seconds = 86400 AND extensions = {'graph.type': 'vertex'} AND comment = 'test'"
        ).unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                let ext = s.extensions.expect("extensions should be Some");
                assert_eq!(ext.len(), 1);
                assert_eq!(ext[0], ("graph.type".into(), "vertex".into()));
                assert_eq!(s.table_options.len(), 2);
                assert_eq!(
                    s.table_options[0],
                    ("gc_grace_seconds".into(), "86400".into())
                );
                assert_eq!(s.table_options[1], ("comment".into(), "test".into()));
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS ks.users").unwrap();
        match stmt {
            Statement::DropTable(s) => {
                assert!(s.if_exists);
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.table, "users");
            }
            other => panic!("expected DropTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_drop_keyspace() {
        let stmt = parse("DROP KEYSPACE my_ks").unwrap();
        match stmt {
            Statement::DropKeyspace(s) => {
                assert_eq!(s.name, "my_ks");
                assert!(!s.if_exists);
            }
            other => panic!("expected DropKeyspace, got {:?}", other),
        }
    }

    #[test]
    fn parse_drop_keyspace_if_exists() {
        let stmt = parse("DROP KEYSPACE IF EXISTS my_ks").unwrap();
        match stmt {
            Statement::DropKeyspace(s) => {
                assert_eq!(s.name, "my_ks");
                assert!(s.if_exists);
            }
            other => panic!("expected DropKeyspace, got {:?}", other),
        }
    }

    #[test]
    fn parse_drop_role() {
        let stmt = parse("DROP ROLE admin").unwrap();
        match stmt {
            Statement::DropRole(s) => {
                assert_eq!(s.name, "admin");
                assert!(!s.if_exists);
            }
            other => panic!("expected DropRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_drop_role_if_exists() {
        let stmt = parse("DROP ROLE IF EXISTS reader").unwrap();
        match stmt {
            Statement::DropRole(s) => {
                assert_eq!(s.name, "reader");
                assert!(s.if_exists);
            }
            other => panic!("expected DropRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_use() {
        let stmt = parse("USE my_keyspace").unwrap();
        match stmt {
            Statement::Use(s) => {
                assert_eq!(s.keyspace, "my_keyspace");
            }
            other => panic!("expected Use, got {:?}", other),
        }
    }

    #[test]
    fn parse_truncate() {
        let stmt = parse("TRUNCATE users").unwrap();
        match stmt {
            Statement::Truncate(s) => {
                assert_eq!(s.table, "users");
                assert!(s.keyspace.is_none());
            }
            other => panic!("expected Truncate, got {:?}", other),
        }
    }

    #[test]
    fn parse_compact_table_statement() {
        let stmt = parse("COMPACT agent_memory.entity_store").unwrap();
        match stmt {
            Statement::Compact(s) => {
                assert_eq!(s.keyspace.as_deref(), Some("agent_memory"));
                assert_eq!(s.table, "entity_store");
            }
            other => panic!("expected Compact, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_role_with_hashed_password() {
        // HASHED PASSWORD stores a pre-computed hash; password stays None.
        let stmt = parse(
            "CREATE ROLE alice WITH HASHED PASSWORD = '$2a$10$abcdefghijklmnopqrstuv' AND LOGIN = true", // pragma: allowlist secret
        )
        .unwrap();
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "alice");
                assert_eq!(s.password, None);
                assert_eq!(
                    s.hashed_password.as_deref(),
                    Some("$2a$10$abcdefghijklmnopqrstuv")
                );
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_role_with_options_and_access() {
        let stmt = parse(
            "CREATE ROLE carlos WITH OPTIONS = { 'opt1' : 'v1', 'opt2' : 99 } \
             AND ACCESS TO DATACENTERS {'DC1', 'DC3'}",
        )
        .unwrap();
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.options.len(), 2);
                assert_eq!(s.options[0], ("opt1".into(), "v1".into()));
                assert_eq!(s.options[1], ("opt2".into(), "99".into()));
                assert_eq!(
                    s.access,
                    vec![RoleAccess::ToDatacenters(vec!["DC1".into(), "DC3".into()])]
                );
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_role_with_access_from_all_cidrs() {
        let stmt = parse(
            "CREATE ROLE hob WITH LOGIN = true AND PASSWORD = 'p' AND ACCESS FROM ALL CIDRS", // pragma: allowlist secret
        )
        .unwrap();
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.access, vec![RoleAccess::FromAllCidrs]);
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_role_with_hashed_password() {
        let stmt = parse(
            "ALTER ROLE bob WITH HASHED PASSWORD = '$argon2id$v=19$m=1' AND SUPERUSER = false",
        ) // pragma: allowlist secret
        .unwrap();
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.hashed_password.as_deref(), Some("$argon2id$v=19$m=1"));
                assert_eq!(s.password, None);
                assert_eq!(s.superuser, Some(false));
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_user_with_hashed_password() {
        // Legacy USER grammar: no `=`, trailing NOSUPERUSER.
        let stmt = parse(
            "CREATE USER bob WITH HASHED PASSWORD '$2a$10$abcdefghijklmnopqrstuv' NOSUPERUSER",
        ) // pragma: allowlist secret
        .unwrap();
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(
                    s.hashed_password.as_deref(),
                    Some("$2a$10$abcdefghijklmnopqrstuv")
                );
                assert_eq!(s.password, None);
                assert_eq!(s.superuser, Some(false));
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_role() {
        let stmt = parse(
            "CREATE ROLE admin WITH PASSWORD = 'secret' AND SUPERUSER = true AND LOGIN = true", // pragma: allowlist secret
        )
        .unwrap();
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "admin");
                assert_eq!(s.password, Some("secret".into()));
                assert_eq!(s.superuser, Some(true));
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    /// Regression: `LIST ROLES` was rejected with
    /// `SyntaxException: unexpected token Keyword(List) at position 0`.
    #[test]
    fn parse_list_roles_bare() {
        let stmt = parse("LIST ROLES").unwrap();
        match stmt {
            Statement::ListRoles {
                of,
                no_recursive,
                users_alias,
            } => {
                assert_eq!(of, None);
                assert!(!no_recursive);
                assert!(!users_alias);
            }
            other => panic!("expected ListRoles, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_roles_of_role() {
        let stmt = parse("LIST ROLES OF admin").unwrap();
        match stmt {
            Statement::ListRoles {
                of,
                no_recursive,
                users_alias,
            } => {
                assert_eq!(of.as_deref(), Some("admin"));
                assert!(!no_recursive);
                assert!(!users_alias);
            }
            other => panic!("expected ListRoles, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_roles_norecursive() {
        let stmt = parse("LIST ROLES OF admin NORECURSIVE").unwrap();
        match stmt {
            Statement::ListRoles {
                of, no_recursive, ..
            } => {
                assert_eq!(of.as_deref(), Some("admin"));
                assert!(no_recursive);
            }
            other => panic!("expected ListRoles, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_users_alias() {
        let stmt = parse("LIST USERS").unwrap();
        match stmt {
            Statement::ListRoles { users_alias, .. } => {
                assert!(users_alias, "USERS variant must be flagged for the alias");
            }
            other => panic!("expected ListRoles, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_all_permissions() {
        let stmt = parse("LIST ALL PERMISSIONS").unwrap();
        match stmt {
            Statement::ListPermissions {
                permission,
                resource,
                of,
                ..
            } => {
                assert_eq!(permission, None);
                assert!(resource.is_none());
                assert_eq!(of, None);
            }
            other => panic!("expected ListPermissions, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_specific_permission_of_role() {
        let stmt = parse("LIST SELECT PERMISSIONS OF carlos").unwrap();
        match stmt {
            Statement::ListPermissions { permission, of, .. } => {
                assert_eq!(permission.as_deref(), Some("SELECT"));
                assert_eq!(of.as_deref(), Some("carlos"));
            }
            other => panic!("expected ListPermissions, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_all_permissions_on_resource_of_role() {
        let stmt = parse("LIST ALL PERMISSIONS ON keyspace1.table1 OF bob").unwrap();
        match stmt {
            Statement::ListPermissions {
                permission,
                resource,
                of,
                ..
            } => {
                assert_eq!(permission, None);
                assert_eq!(
                    resource,
                    Some(GrantResource::Table(
                        Some("keyspace1".into()),
                        "table1".into()
                    ))
                );
                assert_eq!(of.as_deref(), Some("bob"));
            }
            other => panic!("expected ListPermissions, got {:?}", other),
        }
    }

    /// `CREATE USER` is a deprecated alias for
    /// `CREATE ROLE WITH LOGIN = true`. Reported as failing alongside
    /// CREATE ROLE in the auth-defects bug list.
    #[test]
    fn parse_create_user_with_password() {
        let stmt = parse("CREATE USER alice WITH PASSWORD = 'secret'").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "alice");
                assert_eq!(s.password, Some("secret".into()));
                assert_eq!(s.login, Some(true), "USER implies LOGIN = true");
                assert_eq!(s.superuser, None);
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_user_superuser() {
        let stmt = parse("CREATE USER bob WITH PASSWORD = 'p' SUPERUSER").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "bob");
                assert_eq!(s.superuser, Some(true));
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_user_if_not_exists() {
        let stmt = parse("CREATE USER IF NOT EXISTS carol WITH PASSWORD = 'p'").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "carol");
                assert!(s.if_not_exists);
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    /// Regression: `ALTER ROLE x WITH PASSWORD = '...'` was rejected
    /// with `SyntaxException: ALTER Keyword(Role) not yet supported`,
    /// leaving roles whose password never persisted (see CREATE ROLE
    /// password-not-stored bug) with no recovery path. Test reported
    /// by the auth-defects bug list.
    #[test]
    fn parse_alter_role_with_password() {
        let stmt = parse("ALTER ROLE admin WITH PASSWORD = 'newsecret'").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "admin");
                assert_eq!(s.password, Some("newsecret".into()));
                assert_eq!(s.superuser, None);
                assert_eq!(s.login, None);
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_user_with_password_no_equals() {
        // Legacy USER syntax omits the '=' after PASSWORD.
        let stmt = parse("ALTER USER alice WITH PASSWORD 'PASSWORD_A'").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "alice");
                assert_eq!(s.password, Some("PASSWORD_A".into()));
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_user_with_hashed_password_no_equals() {
        let stmt =
            parse("ALTER USER alice WITH HASHED PASSWORD '$2a$10$abcdefghijklmnopqrstuv'").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "alice");
                assert_eq!(
                    s.hashed_password,
                    Some("$2a$10$abcdefghijklmnopqrstuv".into())
                );
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_user_bare_superuser() {
        // Legacy USER syntax: no WITH, trailing bare SUPERUSER flag.
        let stmt = parse("ALTER USER bob SUPERUSER").unwrap();
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "bob");
                assert_eq!(s.superuser, Some(true));
                assert_eq!(s.password, None);
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_user_trailing_nosuperuser() {
        let stmt = parse("CREATE USER ops WITH PASSWORD 'p' NOSUPERUSER").unwrap(); // pragma: allowlist secret
        match stmt {
            Statement::CreateRole(s) => {
                assert_eq!(s.name, "ops");
                assert_eq!(s.password, Some("p".into()));
                assert_eq!(s.superuser, Some(false));
            }
            other => panic!("expected CreateRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_role_with_login() {
        let stmt = parse("ALTER ROLE admin WITH LOGIN = false").unwrap();
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "admin");
                assert_eq!(s.login, Some(false));
                assert_eq!(s.password, None);
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_alter_role_combined_options() {
        let stmt = parse(
            "ALTER ROLE admin WITH PASSWORD = 'p' AND SUPERUSER = true AND LOGIN = true", // pragma: allowlist secret
        )
        .unwrap();
        match stmt {
            Statement::AlterRole(s) => {
                assert_eq!(s.name, "admin");
                assert_eq!(s.password, Some("p".into()));
                assert_eq!(s.superuser, Some(true));
                assert_eq!(s.login, Some(true));
            }
            other => panic!("expected AlterRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_grant() {
        let stmt = parse("GRANT SELECT ON ks.users TO reader").unwrap();
        match stmt {
            Statement::Grant(s) => {
                assert_eq!(s.permissions, vec!["SELECT".to_string()]);
                assert_eq!(
                    s.resource,
                    GrantResource::Table(Some("ks".into()), "users".into())
                );
                assert_eq!(s.role, "reader");
            }
            other => panic!("expected Grant, got {:?}", other),
        }
    }

    #[test]
    fn parse_grant_on_table_keyword() {
        // Cassandra accepts the explicit `ON TABLE <name>` resource form.
        let stmt = parse("GRANT SELECT ON TABLE patients TO privileged").unwrap();
        match stmt {
            Statement::Grant(s) => {
                assert_eq!(s.permissions, vec!["SELECT".to_string()]);
                assert_eq!(s.resource, GrantResource::Table(None, "patients".into()));
                assert_eq!(s.role, "privileged");
            }
            other => panic!("expected Grant, got {:?}", other),
        }
    }

    #[test]
    fn parse_grant_multiple_permissions() {
        let stmt = parse("GRANT SELECT, MODIFY ON TABLE patients TO trusted_user").unwrap();
        match stmt {
            Statement::Grant(s) => {
                assert_eq!(
                    s.permissions,
                    vec!["SELECT".to_string(), "MODIFY".to_string()]
                );
                assert_eq!(s.resource, GrantResource::Table(None, "patients".into()));
                assert_eq!(s.role, "trusted_user");
            }
            other => panic!("expected Grant, got {:?}", other),
        }
    }

    #[test]
    fn parse_grant_role_to_member() {
        let stmt = parse("GRANT report_writer TO alice").unwrap();
        match stmt {
            Statement::GrantRole { role, member } => {
                assert_eq!(role, "report_writer");
                assert_eq!(member, "alice");
            }
            other => panic!("expected GrantRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_revoke_role_from_member() {
        let stmt = parse("REVOKE role_a FROM role_b").unwrap();
        match stmt {
            Statement::RevokeRole { role, member } => {
                assert_eq!(role, "role_a");
                assert_eq!(member, "role_b");
            }
            other => panic!("expected RevokeRole, got {:?}", other),
        }
    }

    #[test]
    fn parse_revoke_on_table_keyword() {
        let stmt = parse("REVOKE UNMASK ON TABLE patients FROM privileged").unwrap();
        match stmt {
            Statement::Revoke(s) => {
                assert_eq!(s.permissions, vec!["UNMASK".to_string()]);
                assert_eq!(s.resource, GrantResource::Table(None, "patients".into()));
                assert_eq!(s.role, "privileged");
            }
            other => panic!("expected Revoke, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Error tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_unsupported_returns_error() {
        // CREATE MATERIALIZED VIEW is not yet supported
        let err = parse("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap_err();
        match err {
            CqlError::SyntaxError(msg) => assert!(
                msg.contains("not yet supported"),
                "expected 'not yet supported' in error, got: {}",
                msg
            ),
            other => panic!("expected SyntaxError, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // CREATE INDEX / DROP INDEX tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_create_btree_index() {
        let stmt = parse("CREATE INDEX idx_email ON users (email) USING 'btree'").unwrap();
        match stmt {
            Statement::CreateIndex(s) => {
                assert_eq!(s.name, Some("idx_email".into()));
                assert_eq!(s.table, "users");
                assert_eq!(s.columns, vec!["email"]);
                assert_eq!(s.using, Some("btree".into()));
                assert!(!s.if_not_exists);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_index_default_type() {
        let stmt = parse("CREATE INDEX idx_email ON users (email)").unwrap();
        match stmt {
            Statement::CreateIndex(s) => {
                assert_eq!(s.using, None);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_vector_index_with_options() {
        let stmt = parse(
            "CREATE INDEX idx_embed ON docs (embedding) USING 'vector' WITH OPTIONS = {'method': 'hnsw', 'metric': 'cosine', 'dimensions': '768'}",
        )
        .unwrap();
        match stmt {
            Statement::CreateIndex(s) => {
                assert_eq!(s.using, Some("vector".into()));
                assert_eq!(s.options.len(), 3);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_composite_index() {
        let stmt =
            parse("CREATE INDEX idx_name ON users (last_name, first_name) USING 'composite'")
                .unwrap();
        match stmt {
            Statement::CreateIndex(s) => {
                assert_eq!(s.columns, vec!["last_name", "first_name"]);
                assert_eq!(s.using, Some("composite".into()));
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_index_if_not_exists() {
        let stmt = parse("CREATE INDEX IF NOT EXISTS idx ON t (c) USING 'hash'").unwrap();
        match stmt {
            Statement::CreateIndex(s) => assert!(s.if_not_exists),
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse("DROP INDEX idx_email").unwrap();
        match stmt {
            Statement::DropIndex(s) => {
                assert_eq!(s.name, "idx_email");
                assert!(!s.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn parse_drop_index_if_exists() {
        let stmt = parse("DROP INDEX IF EXISTS ks.idx_email").unwrap();
        match stmt {
            Statement::DropIndex(s) => {
                assert_eq!(s.keyspace, Some("ks".into()));
                assert_eq!(s.name, "idx_email");
                assert!(s.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn parse_syntax_error() {
        let err = parse("SELECTT * FROM t").unwrap_err();
        assert!(matches!(err, CqlError::SyntaxError(_)));

        let err = parse("").unwrap_err();
        assert!(matches!(err, CqlError::SyntaxError(_)));
    }

    #[test]
    fn parse_nesting_depth_exceeded() {
        // Build a deeply nested type: list<list<list<...>>>  (33 levels)
        let mut input = "CREATE TABLE t (v ".to_string();
        for _ in 0..33 {
            input.push_str("list<");
        }
        input.push_str("int");
        for _ in 0..33 {
            input.push('>');
        }
        input.push_str(", PRIMARY KEY (v))");

        let err = parse(&input).unwrap_err();
        match err {
            CqlError::SyntaxError(msg) => assert!(
                msg.contains("nesting depth"),
                "expected 'nesting depth' in error, got: {}",
                msg
            ),
            other => panic!("expected SyntaxError, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Additional edge case tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_select_allow_filtering() {
        let stmt = parse("SELECT * FROM t WHERE x = 1 ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Select(s) => assert!(s.allow_filtering),
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_token_range_where() {
        let stmt = parse("SELECT * FROM t WHERE token(id) > token(3)").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clauses.len(), 1);
                let wc = &s.where_clauses[0];
                assert!(wc.token_fn, "WHERE clause should be marked as token_fn");
                assert_eq!(wc.column, "id");
                assert_eq!(wc.op, ComparisonOp::Gt);
                // RHS is token(3) parsed as FunctionCall
                match &wc.value {
                    Term::FunctionCall { name, args, .. } => {
                        assert_eq!(name, "token");
                        assert_eq!(args.len(), 1);
                    }
                    other => panic!("expected FunctionCall, got {:?}", other),
                }
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_map_literal() {
        let stmt = parse("INSERT INTO t (k, m) VALUES (1, {'a': 'b', 'c': 'd'})").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values[1],
                    Term::MapLiteral(vec![
                        (
                            Term::StringLiteral("a".into()),
                            Term::StringLiteral("b".into()),
                        ),
                        (
                            Term::StringLiteral("c".into()),
                            Term::StringLiteral("d".into()),
                        ),
                    ])
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_set_literal() {
        let stmt = parse("INSERT INTO t (k, s) VALUES (1, {'a', 'b'})").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values[1],
                    Term::SetLiteral(vec![
                        Term::StringLiteral("a".into()),
                        Term::StringLiteral("b".into()),
                    ])
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_tuple_literal() {
        let stmt = parse("INSERT INTO t (k, tp) VALUES (1, (10, 'hello'))").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(
                    s.values[1],
                    Term::TupleLiteral(vec![
                        Term::IntegerLiteral(10),
                        Term::StringLiteral("hello".into()),
                    ])
                );
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_negative_numbers() {
        let stmt = parse("INSERT INTO t (k, v) VALUES (-42, -1.5)").unwrap();
        match stmt {
            Statement::Insert(s) => {
                assert_eq!(s.values[0], Term::IntegerLiteral(-42));
                assert_eq!(s.values[1], Term::FloatLiteral(-1.5));
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn parse_where_in() {
        let stmt = parse("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clauses[0].op, ComparisonOp::In);
                assert_eq!(
                    s.where_clauses[0].value,
                    Term::InList(vec![
                        Term::IntegerLiteral(1),
                        Term::IntegerLiteral(2),
                        Term::IntegerLiteral(3),
                    ])
                );
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_select_distinct() {
        let stmt = parse("SELECT DISTINCT group_id, group_name FROM static_cols").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(s.distinct, "SELECT DISTINCT must be preserved in the AST");
                assert_eq!(
                    s.columns,
                    vec![
                        SelectColumn::Column("group_id".into()),
                        SelectColumn::Column("group_name".into()),
                    ]
                );
                assert_eq!(s.table, "static_cols");
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_trailing_semicolon() {
        let stmt = parse("SELECT * FROM t;").unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn parse_keyword_as_column_name() {
        // CQL allows keywords like 'key', 'token', 'type' as identifiers
        let stmt = parse("SELECT key, token FROM t WHERE key = 1").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(
                    s.columns,
                    vec![
                        SelectColumn::Column("key".into()),
                        SelectColumn::Column("token".into()),
                    ]
                );
                assert_eq!(s.where_clauses[0].column, "key");
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_table_frozen_collection() {
        let stmt = parse("CREATE TABLE t (k int, v frozen<map<text, list<int>>>, PRIMARY KEY (k))")
            .unwrap();
        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(
                    s.columns[1].1,
                    CqlTypeName::Frozen(Box::new(CqlTypeName::Map(
                        Box::new(CqlTypeName::Simple("text".into())),
                        Box::new(CqlTypeName::List(Box::new(CqlTypeName::Simple(
                            "int".into()
                        )))),
                    )))
                );
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_using_timestamp() {
        let stmt =
            parse("UPDATE t USING TIMESTAMP 1234 AND TTL 60 SET v = 'x' WHERE k = 1").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.using_timestamp, Some(Term::IntegerLiteral(1234)));
                assert_eq!(s.using_ttl, Some(Term::IntegerLiteral(60)));
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_unlogged_batch() {
        let stmt = parse("BEGIN UNLOGGED BATCH INSERT INTO t (k) VALUES (1); APPLY BATCH").unwrap();
        match stmt {
            Statement::Batch(s) => {
                assert_eq!(s.batch_type, BatchType::Unlogged);
            }
            other => panic!("expected Batch, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_keyspace_if_not_exists() {
        let stmt = parse(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND DURABLE_WRITES = false",
        )
        .unwrap();
        match stmt {
            Statement::CreateKeyspace(s) => {
                assert!(s.if_not_exists);
                assert_eq!(s.durable_writes, Some(false));
            }
            other => panic!("expected CreateKeyspace, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // SUBSCRIBE / UNSUBSCRIBE tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_subscribe_select() {
        let stmt = parse("SUBSCRIBE SELECT * FROM users WHERE active = true").unwrap();
        match stmt {
            Statement::Subscribe {
                inner,
                interval,
                delta,
            } => {
                assert!(interval.is_none());
                assert!(!delta);
                assert!(matches!(*inner, Statement::Select { .. }));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_subscribe_with_every() {
        let stmt = parse("SUBSCRIBE SELECT * FROM t EVERY 5s").unwrap();
        match stmt {
            Statement::Subscribe { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(5)));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_subscribe_with_delta() {
        let stmt = parse("SUBSCRIBE SELECT * FROM t DELTA").unwrap();
        match stmt {
            Statement::Subscribe { delta, .. } => assert!(delta),
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_subscribe_every_and_delta() {
        let stmt = parse("SUBSCRIBE SELECT * FROM t EVERY 1s DELTA").unwrap();
        match stmt {
            Statement::Subscribe {
                interval, delta, ..
            } => {
                assert_eq!(interval, Some(Duration::from_secs(1)));
                assert!(delta);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_unsubscribe_all() {
        let stmt = parse("UNSUBSCRIBE").unwrap();
        assert!(matches!(stmt, Statement::Unsubscribe { stream_id: None }));
    }

    #[test]
    fn parse_subscribe_rejects_non_select() {
        assert!(parse("SUBSCRIBE INSERT INTO t (a) VALUES (1)").is_err());
    }

    #[test]
    fn parse_subscribe_enforces_min_interval() {
        assert!(parse("SUBSCRIBE SELECT * FROM t EVERY 100ms").is_err());
    }

    // ---------------------------------------------------------------
    // CREATE/ALTER/DROP TYPE tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_create_type_basic() {
        let stmt = parse("CREATE TYPE ks.address (street text, city text, zip int)").unwrap();
        match stmt {
            Statement::CreateType {
                keyspace,
                name,
                fields,
                if_not_exists,
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "address");
                assert_eq!(fields.len(), 3);
                assert!(!if_not_exists);
            }
            _ => panic!("expected CreateType"),
        }
    }

    #[test]
    fn parse_create_type_if_not_exists() {
        let stmt = parse("CREATE TYPE IF NOT EXISTS ks.address (street text)").unwrap();
        match stmt {
            Statement::CreateType { if_not_exists, .. } => assert!(if_not_exists),
            _ => panic!("expected CreateType"),
        }
    }

    #[test]
    fn parse_create_type_no_keyspace() {
        let stmt = parse("CREATE TYPE address (street text, city text)").unwrap();
        match stmt {
            Statement::CreateType { keyspace, name, .. } => {
                assert_eq!(keyspace, None);
                assert_eq!(name, "address");
            }
            _ => panic!("expected CreateType"),
        }
    }

    #[test]
    fn parse_create_type_frozen_field() {
        let stmt = parse("CREATE TYPE ks.contact (addr frozen<text>, phone text)").unwrap();
        match stmt {
            Statement::CreateType { fields, .. } => {
                assert_eq!(fields.len(), 2);
                match &fields[0].1 {
                    CqlTypeName::Frozen(_) => {}
                    _ => panic!("expected Frozen type for first field"),
                }
            }
            _ => panic!("expected CreateType"),
        }
    }

    #[test]
    fn parse_alter_type_add() {
        let stmt = parse("ALTER TYPE ks.address ADD country text").unwrap();
        match stmt {
            Statement::AlterType {
                keyspace,
                name,
                alterations,
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "address");
                assert_eq!(alterations.len(), 1);
                match &alterations[0] {
                    TypeAlteration::AddField { name, .. } => assert_eq!(name, "country"),
                    _ => panic!("expected AddField"),
                }
            }
            _ => panic!("expected AlterType"),
        }
    }

    #[test]
    fn parse_alter_type_rename() {
        let stmt = parse("ALTER TYPE ks.address RENAME street TO street_name").unwrap();
        match stmt {
            Statement::AlterType { alterations, .. } => match &alterations[0] {
                TypeAlteration::RenameField { from, to } => {
                    assert_eq!(from, "street");
                    assert_eq!(to, "street_name");
                }
                _ => panic!("expected RenameField"),
            },
            _ => panic!("expected AlterType"),
        }
    }

    #[test]
    fn parse_drop_type() {
        let stmt = parse("DROP TYPE ks.address").unwrap();
        match stmt {
            Statement::DropType {
                keyspace,
                name,
                if_exists,
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "address");
                assert!(!if_exists);
            }
            _ => panic!("expected DropType"),
        }
    }

    #[test]
    fn parse_drop_type_if_exists() {
        let stmt = parse("DROP TYPE IF EXISTS ks.address").unwrap();
        match stmt {
            Statement::DropType { if_exists, .. } => assert!(if_exists),
            _ => panic!("expected DropType"),
        }
    }

    // ---------------------------------------------------------------
    // CREATE/DROP FUNCTION and AGGREGATE tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_create_function() {
        let stmt = parse(
            "CREATE FUNCTION ks.double_it (val int) \
             CALLED ON NULL INPUT \
             RETURNS int LANGUAGE wasm AS 'deadbeef'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction {
                keyspace,
                name,
                params,
                called_on_null,
                language,
                body,
                or_replace,
                if_not_exists,
                ..
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "double_it");
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].0, "val");
                assert!(called_on_null);
                assert_eq!(language, "wasm");
                assert_eq!(body, FunctionBodySource::InlineHex("deadbeef".into()));
                assert!(!or_replace);
                assert!(!if_not_exists);
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    #[test]
    fn parse_create_or_replace_function() {
        let stmt = parse(
            "CREATE OR REPLACE FUNCTION ks.f (a int) \
             RETURNS NULL ON NULL INPUT \
             RETURNS text LANGUAGE wasm AS 'cafe'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction {
                or_replace,
                called_on_null,
                ..
            } => {
                assert!(or_replace);
                assert!(!called_on_null);
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    #[test]
    fn parse_create_function_if_not_exists() {
        let stmt = parse(
            "CREATE FUNCTION IF NOT EXISTS ks.f (a int) \
             CALLED ON NULL INPUT \
             RETURNS int LANGUAGE wasm AS 'body'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    #[test]
    fn parse_create_function_as_file() {
        let stmt = parse(
            "CREATE FUNCTION ks.f (a int) \
             CALLED ON NULL INPUT \
             RETURNS int LANGUAGE wasm AS FILE '/tmp/f.wasm'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction { body, .. } => {
                assert_eq!(body, FunctionBodySource::File("/tmp/f.wasm".into()));
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    #[test]
    fn parse_create_function_as_url_with_sha256() {
        let stmt = parse(
            "CREATE FUNCTION ks.f (a int) \
             CALLED ON NULL INPUT \
             RETURNS int LANGUAGE wasm AS URL 'https://example.invalid/f.wasm' \
             WITH SHA256 = '0123456789abcdef'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction { body, .. } => {
                assert_eq!(
                    body,
                    FunctionBodySource::Url {
                        url: "https://example.invalid/f.wasm".into(),
                        sha256: "0123456789abcdef".into(),
                    }
                );
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    #[test]
    fn parse_create_function_or_replace_and_if_not_exists_fails() {
        let result = parse(
            "CREATE OR REPLACE FUNCTION IF NOT EXISTS ks.f (a int) \
             CALLED ON NULL INPUT \
             RETURNS int LANGUAGE wasm AS 'body'",
        );
        assert!(result.is_err());
    }

    #[test]
    fn parse_drop_function_with_arg_types() {
        let stmt = parse("DROP FUNCTION IF EXISTS ks.double_it (int)").unwrap();
        match stmt {
            Statement::DropFunction {
                name,
                arg_types,
                if_exists,
                keyspace,
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "double_it");
                assert!(if_exists);
                assert!(arg_types.is_some());
                assert_eq!(arg_types.unwrap().len(), 1);
            }
            _ => panic!("expected DropFunction"),
        }
    }

    #[test]
    fn parse_drop_function_without_arg_types() {
        let stmt = parse("DROP FUNCTION ks.double_it").unwrap();
        match stmt {
            Statement::DropFunction { arg_types, .. } => {
                assert!(arg_types.is_none());
            }
            _ => panic!("expected DropFunction"),
        }
    }

    #[test]
    fn parse_create_aggregate() {
        let stmt = parse(
            "CREATE AGGREGATE ks.my_avg (int) \
             SFUNC avg_state STYPE bigint \
             FINALFUNC avg_final INITCOND 0",
        )
        .unwrap();
        match stmt {
            Statement::CreateAggregate {
                keyspace,
                name,
                state_func,
                final_func,
                init_cond,
                arg_types,
                ..
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "my_avg");
                assert_eq!(state_func, "avg_state");
                assert_eq!(final_func, Some("avg_final".to_string()));
                assert!(init_cond.is_some());
                assert_eq!(arg_types.len(), 1);
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn parse_create_aggregate_minimal() {
        let stmt = parse(
            "CREATE AGGREGATE ks.my_sum (int) \
             SFUNC sum_state STYPE bigint",
        )
        .unwrap();
        match stmt {
            Statement::CreateAggregate {
                final_func,
                init_cond,
                ..
            } => {
                assert_eq!(final_func, None);
                assert_eq!(init_cond, None);
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn parse_create_or_replace_aggregate() {
        let stmt = parse(
            "CREATE OR REPLACE AGGREGATE ks.my_avg (int) \
             SFUNC avg_state STYPE bigint",
        )
        .unwrap();
        match stmt {
            Statement::CreateAggregate { or_replace, .. } => {
                assert!(or_replace);
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn parse_drop_aggregate() {
        let stmt = parse("DROP AGGREGATE IF EXISTS ks.my_avg (int)").unwrap();
        match stmt {
            Statement::DropAggregate {
                name,
                if_exists,
                keyspace,
                arg_types,
            } => {
                assert_eq!(keyspace, Some("ks".to_string()));
                assert_eq!(name, "my_avg");
                assert!(if_exists);
                assert!(arg_types.is_some());
            }
            _ => panic!("expected DropAggregate"),
        }
    }

    #[test]
    fn parse_drop_aggregate_without_arg_types() {
        let stmt = parse("DROP AGGREGATE ks.my_avg").unwrap();
        match stmt {
            Statement::DropAggregate { arg_types, .. } => {
                assert!(arg_types.is_none());
            }
            _ => panic!("expected DropAggregate"),
        }
    }

    #[test]
    fn parse_create_function_no_params() {
        let stmt = parse(
            "CREATE FUNCTION ks.get_time () \
             CALLED ON NULL INPUT \
             RETURNS timestamp LANGUAGE wasm AS 'aabb'",
        )
        .unwrap();
        match stmt {
            Statement::CreateFunction { params, .. } => {
                assert_eq!(params.len(), 0);
            }
            _ => panic!("expected CreateFunction"),
        }
    }

    // ---------------------------------------------------------------
    // EXPLAIN tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_explain_select() {
        let stmt = parse("EXPLAIN SELECT * FROM ks.users WHERE email = 'alice'").unwrap();
        match stmt {
            Statement::Explain(s) => {
                assert_eq!(s.keyspace.as_deref(), Some("ks"));
                assert_eq!(s.table, "users");
                assert_eq!(s.where_clauses.len(), 1);
            }
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn parse_explain_select_with_allow_filtering() {
        let stmt = parse("EXPLAIN SELECT * FROM t WHERE x = 1 ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Explain(s) => assert!(s.allow_filtering),
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn parse_explain_rejects_non_select() {
        assert!(parse("EXPLAIN INSERT INTO t (a) VALUES (1)").is_err());
    }

    #[test]
    fn parse_tojson_with_alias() {
        let stmt = parse(
            "SELECT keyspace_name, toJson(replication) AS replication FROM system_schema.keyspaces",
        )
        .unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.keyspace, Some("system_schema".into()));
                assert_eq!(s.table, "keyspaces");
                assert_eq!(s.columns.len(), 2);
                assert_eq!(s.columns[0], SelectColumn::Column("keyspace_name".into()));
                match &s.columns[1] {
                    SelectColumn::FunctionCall {
                        name, args, alias, ..
                    } => {
                        assert_eq!(name, "tojson");
                        assert_eq!(args.len(), 1);
                        // replication is a keyword, parsed as Term::FunctionCall with empty args
                        match &args[0] {
                            Term::FunctionCall { name, args, .. } => {
                                assert_eq!(name, "replication");
                                assert!(args.is_empty());
                            }
                            other => panic!("expected FunctionCall(replication), got {:?}", other),
                        }
                        assert_eq!(alias, &Some("replication".into()));
                    }
                    other => panic!("expected FunctionCall, got {:?}", other),
                }
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn parse_tojson_without_alias() {
        let stmt = parse("SELECT toJson(name) FROM users").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                match &s.columns[0] {
                    SelectColumn::FunctionCall { name, alias, .. } => {
                        assert_eq!(name, "tojson");
                        assert_eq!(alias, &None);
                    }
                    other => panic!("expected FunctionCall, got {:?}", other),
                }
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // IF condition tests (LWT conditional UPDATE/DELETE)
    // ---------------------------------------------------------------

    #[test]
    fn parse_update_if_single_condition() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF v = 0").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert!(!s.if_exists);
                assert_eq!(s.if_conditions.len(), 1);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[0].value, Term::IntegerLiteral(0));
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_if_multi_condition() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF v = 0 AND status = 'active'").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert!(!s.if_exists);
                assert_eq!(s.if_conditions.len(), 2);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[0].value, Term::IntegerLiteral(0));
                assert_eq!(s.if_conditions[1].column, "status");
                assert_eq!(s.if_conditions[1].operator, IfOperator::Eq);
                assert_eq!(
                    s.if_conditions[1].value,
                    Term::StringLiteral("active".into())
                );
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_if_not_eq() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF v != 0").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.if_conditions.len(), 1);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::NotEq);
                assert_eq!(s.if_conditions[0].value, Term::IntegerLiteral(0));
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_if_comparison_operators() {
        let cases = [
            ("IF v < 10", IfOperator::Lt),
            ("IF v > 10", IfOperator::Gt),
            ("IF v <= 10", IfOperator::LtEq),
            ("IF v >= 10", IfOperator::GtEq),
        ];
        for (clause, expected_op) in &cases {
            let sql = format!("UPDATE t SET v = 1 WHERE id = 1 {}", clause);
            let stmt = parse(&sql).unwrap();
            match stmt {
                Statement::Update(s) => {
                    assert_eq!(s.if_conditions.len(), 1, "clause: {}", clause);
                    assert_eq!(
                        s.if_conditions[0].operator, *expected_op,
                        "clause: {}",
                        clause
                    );
                    assert_eq!(
                        s.if_conditions[0].value,
                        Term::IntegerLiteral(10),
                        "clause: {}",
                        clause
                    );
                }
                other => panic!("expected Update for {}, got {:?}", clause, other),
            }
        }
    }

    #[test]
    fn parse_update_if_in() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF status IN ('a', 'b')").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.if_conditions.len(), 1);
                assert_eq!(s.if_conditions[0].column, "status");
                assert_eq!(s.if_conditions[0].operator, IfOperator::In);
                assert_eq!(
                    s.if_conditions[0].value,
                    Term::InList(vec![
                        Term::StringLiteral("a".into()),
                        Term::StringLiteral("b".into()),
                    ])
                );
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_if_exists_unchanged() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF EXISTS").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert!(s.if_exists);
                assert!(s.if_conditions.is_empty());
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_update_if_bind_marker() {
        let stmt = parse("UPDATE t SET v = 1 WHERE id = 1 IF v = ?").unwrap();
        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.if_conditions.len(), 1);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[0].value, Term::BindMarker(None));
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete_if_condition() {
        let stmt = parse("DELETE FROM t WHERE id = 1 IF v = 'old'").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert!(!s.if_exists);
                assert_eq!(s.if_conditions.len(), 1);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[0].value, Term::StringLiteral("old".into()));
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete_if_multi_condition() {
        let stmt = parse("DELETE FROM t WHERE id = 1 IF v = 'old' AND status = 1").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert!(!s.if_exists);
                assert_eq!(s.if_conditions.len(), 2);
                assert_eq!(s.if_conditions[0].column, "v");
                assert_eq!(s.if_conditions[0].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[0].value, Term::StringLiteral("old".into()));
                assert_eq!(s.if_conditions[1].column, "status");
                assert_eq!(s.if_conditions[1].operator, IfOperator::Eq);
                assert_eq!(s.if_conditions[1].value, Term::IntegerLiteral(1));
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn parse_delete_if_exists_unchanged() {
        let stmt = parse("DELETE FROM t WHERE id = 1 IF EXISTS").unwrap();
        match stmt {
            Statement::Delete(s) => {
                assert!(s.if_exists);
                assert!(s.if_conditions.is_empty());
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Accord transaction tests
    // ---------------------------------------------------------------

    #[test]
    fn parse_begin_commit_rollback() {
        // BEGIN TRANSACTION
        let stmt = parse("BEGIN TRANSACTION").unwrap();
        assert!(
            matches!(stmt, Statement::BeginTransaction),
            "expected BeginTransaction, got {:?}",
            stmt
        );

        // COMMIT
        let stmt = parse("COMMIT").unwrap();
        assert!(
            matches!(stmt, Statement::Commit),
            "expected Commit, got {:?}",
            stmt
        );

        // ROLLBACK
        let stmt = parse("ROLLBACK").unwrap();
        assert!(
            matches!(stmt, Statement::Rollback),
            "expected Rollback, got {:?}",
            stmt
        );
    }

    #[test]
    fn parse_begin_transaction_case_insensitive() {
        let stmt = parse("begin transaction").unwrap();
        assert!(matches!(stmt, Statement::BeginTransaction));

        let stmt = parse("Begin Transaction").unwrap();
        assert!(matches!(stmt, Statement::BeginTransaction));
    }

    #[test]
    fn parse_nested_transaction_rejected() {
        use crate::session::TransactionState;
        let mut txn_state = TransactionState::new();

        // First BEGIN is accepted.
        let stmt = parse("BEGIN TRANSACTION").unwrap();
        txn_state.validate_and_transition(&stmt).unwrap();

        // Second BEGIN inside the transaction must be rejected.
        let stmt = parse("BEGIN TRANSACTION").unwrap();
        let err = txn_state.validate_and_transition(&stmt);
        assert!(err.is_err(), "nested BEGIN TRANSACTION should be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("nested"),
            "error should mention 'nested', got: {msg}"
        );
    }

    #[test]
    fn parse_ddl_in_transaction_rejected() {
        use crate::session::TransactionState;
        let mut txn_state = TransactionState::new();

        // Start a transaction.
        let begin = parse("BEGIN TRANSACTION").unwrap();
        txn_state.validate_and_transition(&begin).unwrap();

        // DDL inside transaction must be rejected.
        let ddl = parse("CREATE TABLE ks.t (k int PRIMARY KEY)").unwrap();
        let err = txn_state.validate_and_transition(&ddl);
        assert!(err.is_err(), "DDL inside transaction should be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("DDL"),
            "error should mention 'DDL', got: {msg}"
        );
    }

    #[test]
    fn parse_empty_transaction() {
        use crate::session::TransactionState;
        let mut txn_state = TransactionState::new();

        // BEGIN immediately followed by COMMIT is legal.
        let begin = parse("BEGIN TRANSACTION").unwrap();
        txn_state.validate_and_transition(&begin).unwrap();

        let commit = parse("COMMIT").unwrap();
        txn_state.validate_and_transition(&commit).unwrap();

        // After commit, we're back to no-transaction state.
        assert!(!txn_state.in_transaction());
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parser_never_panics(input in "\\PC{0,200}") {
            let _ = super::parse(&input);
        }
    }
}

#[cfg(test)]
mod tracing_tests {
    use super::*;

    #[test]
    fn parse_creates_cql_parse_span_with_statement_type() {
        // Verify that parse() successfully produces a statement and that
        // statement_type_name returns the expected label.
        let stmt = parse("SELECT * FROM ks.t").unwrap();
        assert_eq!(statement_type_name(&stmt), "SELECT");

        let stmt = parse("INSERT INTO ks.t (a) VALUES (1)").unwrap();
        assert_eq!(statement_type_name(&stmt), "INSERT");

        let stmt = parse("DELETE FROM ks.t WHERE a = 1").unwrap();
        assert_eq!(statement_type_name(&stmt), "DELETE");

        let stmt = parse("UPDATE ks.t SET b = 2 WHERE a = 1").unwrap();
        assert_eq!(statement_type_name(&stmt), "UPDATE");
    }
}
