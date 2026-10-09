//! Module: `CREATE TABLE` parsing for the Ecto-migration DDL subset (D10, T-130).
//! Correctness: Correct when every statement `ecto_sql` emits for `create table`
//! parses to the expected AST, and every out-of-scope clause fails with a typed
//! `ParseError::UnsupportedClause` naming that clause (never silently dropped).
//! Last revised: 2026-09-28
//! Last changed: New module; parse layer only, no execution or schema creation.
//!
//! This is a child module of `parser` so it shares the private lexer/`Parser`.
//! Every loop here iterates over the token vector once; there is no recursion.

use super::{ParseError, Parser, Tok};
use crate::ast::{
    AlterOperation, AlterTableStmt, ColumnDef, CopyFormatKind, CopyFromStdinStmt, CreateTableStmt,
    DropTableStatement, PgType, Statement, TableRef, UnsupportedClause,
};

/// The one schema a table may be qualified with. Other schemas are refused
/// (`UnsupportedClause::ForeignSchema`) until schema-to-keyspace mapping exists.
const MAPPED_SCHEMA: &str = "public";

impl Parser {
    /// `CREATE TABLE [IF NOT EXISTS] name ( elem [, elem]* )`. The leading
    /// `CREATE` token has not been consumed yet.
    pub(super) fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("CREATE")?;
        self.expect_ident_kw("TABLE")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_create_table_name()?;
        self.expect(&Tok::LParen, "(")?;
        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut table_pk: Option<Vec<String>> = None;
        loop {
            self.parse_table_element(&mut columns, &mut table_pk)?;
            match self.next() {
                Some(Tok::Comma) => {}
                Some(Tok::RParen) => break,
                Some(t) => {
                    return Err(ParseError::Unexpected {
                        expected: ", or )",
                        found: format!("{t:?}"),
                    })
                }
                None => return Err(ParseError::UnexpectedEnd),
            }
        }
        self.expect_end()?;
        finish_create_table(if_not_exists, name, columns, table_pk)
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, ParseError> {
        if !self.peek_is_kw("IF") {
            return Ok(false);
        }
        self.next();
        self.expect(&Tok::Not, "NOT")?;
        self.expect_ident_kw("EXISTS")?;
        Ok(true)
    }

    fn parse_create_table_name(&mut self) -> Result<TableRef, ParseError> {
        let table = self.parse_qualified_table()?;
        match &table.schema {
            Some(s) if !s.eq_ignore_ascii_case(MAPPED_SCHEMA) => Err(
                ParseError::UnsupportedClause(UnsupportedClause::ForeignSchema),
            ),
            _ => Ok(table),
        }
    }

    /// One element of the table body: a table constraint or a column definition.
    fn parse_table_element(
        &mut self,
        columns: &mut Vec<ColumnDef>,
        table_pk: &mut Option<Vec<String>>,
    ) -> Result<(), ParseError> {
        if self.peek_is_kw("CONSTRAINT") {
            self.next();
            self.ident()?; // constraint name; the constraint kind follows
            return self.parse_table_constraint(table_pk);
        }
        if let Some(Tok::Ident(w)) = self.peek() {
            if matches!(
                w.to_ascii_uppercase().as_str(),
                "PRIMARY" | "FOREIGN" | "CHECK" | "UNIQUE"
            ) {
                return self.parse_table_constraint(table_pk);
            }
        }
        let col = self.parse_column_def()?;
        columns.push(col);
        Ok(())
    }

    fn parse_table_constraint(
        &mut self,
        table_pk: &mut Option<Vec<String>>,
    ) -> Result<(), ParseError> {
        let word = self.ident()?.to_ascii_uppercase();
        match word.as_str() {
            "PRIMARY" => {
                self.expect_ident_kw("KEY")?;
                if table_pk.is_some() {
                    return Err(ParseError::MultiplePrimaryKeys);
                }
                *table_pk = Some(self.parse_paren_ident_list()?);
                Ok(())
            }
            "FOREIGN" => Err(ParseError::UnsupportedClause(UnsupportedClause::ForeignKey)),
            "CHECK" => Err(ParseError::UnsupportedClause(UnsupportedClause::Check)),
            "UNIQUE" => Err(ParseError::UnsupportedClause(UnsupportedClause::Unique)),
            _ => Err(ParseError::Unexpected {
                expected: "PRIMARY KEY, FOREIGN KEY, CHECK or UNIQUE",
                found: word,
            }),
        }
    }

    /// `COPY <name> [(<cols>)] FROM STDIN [[WITH] (<options>)]`.
    ///
    /// Only the `FROM STDIN` direction is parsed: `COPY ... TO` writes a payload the client reads,
    /// which is a different exchange and is refused by name. Options are the modern parenthesised
    /// form; an unrecognised option is refused rather than ignored, because a client that asked for
    /// csv and silently got text would store garbage.
    pub(super) fn parse_copy(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("COPY")?;
        let table = self.parse_qualified_table()?;

        // The column list is optional; `(a, b)` restricts and orders the payload's fields.
        let columns = if matches!(self.peek(), Some(Tok::LParen)) {
            Some(self.parse_paren_ident_list()?)
        } else {
            None
        };

        // `COPY ... TO` writes a payload the client reads — the opposite exchange — and is refused
        // by name rather than reported as a stray token.
        if self.peek_is_kw("TO") {
            return Err(ParseError::UnsupportedAlter(
                "COPY ... TO is not supported; only COPY ... FROM STDIN".into(),
            ));
        }
        // `FROM` is a KEYWORD token here, not an identifier, so it is matched directly.
        if !matches!(self.peek(), Some(Tok::From)) {
            return Err(ParseError::Unexpected {
                expected: "FROM",
                found: format!("{:?}", self.peek()),
            });
        }
        self.next(); // FROM
        if !self.peek_is_kw("STDIN") {
            return Err(ParseError::UnsupportedAlter(
                "only COPY ... FROM STDIN is supported".into(),
            ));
        }
        self.next(); // STDIN

        let mut stmt = CopyFromStdinStmt {
            table,
            columns,
            format: CopyFormatKind::Text,
            delimiter: None,
            null: None,
            header: false,
        };
        // `WITH` is optional, as is the parenthesised list entirely.
        if self.peek_is_kw("WITH") {
            self.next();
        }
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.parse_copy_options(&mut stmt)?;
        } else if self.peek().is_some() {
            return Err(ParseError::UnsupportedAlter(
                "only the parenthesised COPY option list is supported".into(),
            ));
        }
        Ok(Statement::CopyFromStdin(Box::new(stmt)))
    }

    /// `(key [value] [, ...])`, applied to `stmt`.
    fn parse_copy_options(&mut self, stmt: &mut CopyFromStdinStmt) -> Result<(), ParseError> {
        self.expect(&Tok::LParen, "(")?;
        loop {
            let key = self.ident()?.to_ascii_uppercase();
            match key.as_str() {
                "FORMAT" => {
                    let value = self.ident()?.to_ascii_uppercase();
                    stmt.format = match value.as_str() {
                        "TEXT" => CopyFormatKind::Text,
                        "CSV" => CopyFormatKind::Csv,
                        other => {
                            return Err(ParseError::UnsupportedAlter(format!(
                                "COPY format `{other}` is not supported"
                            )))
                        }
                    };
                }
                "DELIMITER" => stmt.delimiter = Some(self.copy_option_char()?),
                "NULL" => stmt.null = Some(self.copy_option_string()?),
                "HEADER" => stmt.header = true,
                other => {
                    return Err(ParseError::UnsupportedAlter(format!(
                        "COPY option `{other}` is not supported"
                    )))
                }
            }
            match self.peek() {
                Some(Tok::Comma) => {
                    self.next();
                }
                Some(Tok::RParen) => {
                    self.next();
                    return Ok(());
                }
                _ => {
                    return Err(ParseError::Unexpected {
                        expected: ", or )",
                        found: format!("{:?}", self.peek()),
                    })
                }
            }
        }
    }

    /// A single-character COPY option value, written as a string literal (`DELIMITER ','`).
    fn copy_option_char(&mut self) -> Result<char, ParseError> {
        let text = self.copy_option_string()?;
        let mut chars = text.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => Ok(c),
            _ => Err(ParseError::UnsupportedAlter(format!(
                "DELIMITER must be a single character, got {text:?}"
            ))),
        }
    }

    /// A string-literal COPY option value.
    fn copy_option_string(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(s),
            // An identifier is accepted too, so `DELIMITER |`-style unquoted values read.
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(ParseError::Unexpected {
                expected: "a string literal",
                found: format!("{other:?}"),
            }),
        }
    }

    /// `ALTER TABLE <name> <operation>`.
    ///
    /// Only the operations ferrosa can apply are accepted; anything else is refused by name.
    pub(super) fn parse_alter_table(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("ALTER")?;
        if !self.peek_is_kw("TABLE") {
            return Err(ParseError::UnsupportedAlter(
                "only ALTER TABLE is supported".into(),
            ));
        }
        self.next(); // TABLE
                     // `ONLY` restricts the change to the named table and not its descendants. ferrosa has
                     // no inheritance, so accepting it is exact rather than approximate.
        if self.peek_is_kw("ONLY") {
            self.next();
        }
        let table = self.parse_qualified_table()?;

        let operation = if self.peek_is_kw("ADD") {
            self.next(); // ADD
            self.parse_alter_add()?
        } else if self.peek_is_kw("DROP") {
            self.next(); // DROP
                         // `DROP COLUMN` and `DROP` are the same thing here; the keyword is optional in
                         // Postgres. `DROP CONSTRAINT` is not implemented, and is caught by the ident check.
            if self.peek_is_kw("COLUMN") {
                self.next();
            } else if !matches!(self.peek(), Some(Tok::Ident(_))) {
                return Err(ParseError::UnsupportedAlter(
                    "only DROP COLUMN is supported".into(),
                ));
            }
            AlterOperation::DropColumn(self.ident()?)
        } else if self.peek_is_kw("RENAME") {
            return Err(ParseError::UnsupportedAlter(
                "RENAME is not supported: ferrosa has no rename path".into(),
            ));
        } else if self.peek_is_kw("ALTER") {
            return Err(ParseError::UnsupportedAlter(
                "changing a column's type or default is not supported".into(),
            ));
        } else {
            return Err(ParseError::UnsupportedAlter(
                "expected ADD, DROP, RENAME or ALTER COLUMN".into(),
            ));
        };

        Ok(Statement::AlterTable(Box::new(AlterTableStmt {
            table,
            operation,
        })))
    }

    /// The clause after `ALTER TABLE t ADD`.
    fn parse_alter_add(&mut self) -> Result<AlterOperation, ParseError> {
        // `ADD CONSTRAINT <name> PRIMARY KEY (...)`. The name is accepted and ignored: Postgres
        // does not require it to mean anything, and the key is described by its columns.
        if self.peek_is_kw("CONSTRAINT") {
            self.next();
            self.ident()?; // the constraint name
            return self.parse_add_primary_key();
        }
        if self.peek_is_kw("PRIMARY") {
            return self.parse_add_primary_key();
        }
        // `ADD [COLUMN] <name> <type>`. Postgres allows the keyword to be omitted, which is what
        // pgbench-style DDL and most ORMs emit.
        if self.peek_is_kw("COLUMN") {
            self.next();
        } else if !matches!(self.peek(), Some(Tok::Ident(_))) {
            return Err(ParseError::UnsupportedAlter(
                "only ADD COLUMN and ADD PRIMARY KEY are supported".into(),
            ));
        }
        let def = self.parse_column_def()?;
        // A column-level PRIMARY KEY would be a second way to say `ADD PRIMARY KEY`, and the two
        // would have to agree about order and about the storage key. Refuse it rather than pick.
        if def.primary_key {
            return Err(ParseError::UnsupportedAlter(
                "a column-level PRIMARY KEY in ADD COLUMN is not supported: use ADD PRIMARY KEY"
                    .into(),
            ));
        }
        Ok(AlterOperation::AddColumn(def))
    }

    fn parse_add_primary_key(&mut self) -> Result<AlterOperation, ParseError> {
        self.expect_ident_kw("PRIMARY")?;
        self.expect_ident_kw("KEY")?;
        Ok(AlterOperation::AddPrimaryKey(
            self.parse_paren_ident_list()?,
        ))
    }

    /// `( ident [, ident]* )` — the column list of a table-level PRIMARY KEY.
    fn parse_paren_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Tok::LParen, "(")?;
        let mut names = vec![self.ident()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            names.push(self.ident()?);
        }
        self.expect(&Tok::RParen, ")")?;
        Ok(names)
    }

    /// `name type [column-constraint]*`. A column-level `PRIMARY KEY` is recorded
    /// on the returned def (`primary_key`) and folded into the table key later.
    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.ident()?;
        let ty = self.parse_pg_type()?;
        let mut def = ColumnDef {
            name,
            ty,
            not_null: false,
            primary_key: false,
        };
        while let Some(tok) = self.peek() {
            if matches!(tok, Tok::Comma | Tok::RParen) {
                break;
            }
            self.parse_column_constraint(&mut def)?;
        }
        Ok(def)
    }

    fn parse_column_constraint(&mut self, def: &mut ColumnDef) -> Result<(), ParseError> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            self.expect_ident_kw("NULL")?;
            def.not_null = true;
            return Ok(());
        }
        let word = self.ident()?.to_ascii_uppercase();
        match word.as_str() {
            "NULL" => Ok(()),
            "PRIMARY" => {
                self.expect_ident_kw("KEY")?;
                def.primary_key = true;
                Ok(())
            }
            "DEFAULT" => Err(ParseError::UnsupportedClause(
                UnsupportedClause::DefaultExpr,
            )),
            "REFERENCES" => Err(ParseError::UnsupportedClause(UnsupportedClause::ForeignKey)),
            "CHECK" => Err(ParseError::UnsupportedClause(UnsupportedClause::Check)),
            "UNIQUE" => Err(ParseError::UnsupportedClause(UnsupportedClause::Unique)),
            _ => Err(ParseError::Unexpected {
                expected: "column constraint",
                found: word,
            }),
        }
    }

    fn peek_is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(w)) if w.eq_ignore_ascii_case(kw))
    }

    /// `DROP TABLE [IF EXISTS] name [, name]*`. The leading `DROP` token has not
    /// been consumed yet. pgbench's initializer drops all four of its tables in
    /// one statement, so the multi-table list is the common case, not the exotic
    /// one. Each name is kept verbatim (with any schema qualifier); the executor
    /// resolves and drops them one at a time.
    pub(super) fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("DROP")?;
        if !self.peek_is_kw("TABLE") {
            return Err(ParseError::Unexpected {
                expected: "TABLE",
                found: match self.peek() {
                    Some(t) => format!("{t:?}"),
                    None => "end of statement".into(),
                },
            });
        }
        self.next(); // TABLE
        let if_exists = self.parse_if_exists_kw()?;

        let mut tables = Vec::new();
        loop {
            tables.push(self.parse_qualified_table()?);
            match self.peek() {
                Some(Tok::Comma) => {
                    self.next();
                }
                _ => break,
            }
        }
        self.expect_end()?;
        Ok(Statement::DropTable(Box::new(DropTableStatement {
            if_exists,
            tables,
        })))
    }

    /// `IF EXISTS`. Only valid immediately before a name; a bare `IF` is an error.
    fn parse_if_exists_kw(&mut self) -> Result<bool, ParseError> {
        if !self.peek_is_kw("IF") {
            return Ok(false);
        }
        self.next();
        self.expect_ident_kw("EXISTS")?;
        Ok(true)
    }

    /// `( n )` after a type name, or nothing. Returns the modifiers in order.
    fn parse_type_modifiers(&mut self) -> Result<Vec<u32>, ParseError> {
        if !matches!(self.peek(), Some(Tok::LParen)) {
            return Ok(Vec::new());
        }
        self.next();
        let mut mods = vec![self.parse_type_modifier()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            mods.push(self.parse_type_modifier()?);
        }
        self.expect(&Tok::RParen, ")")?;
        Ok(mods)
    }

    fn parse_type_modifier(&mut self) -> Result<u32, ParseError> {
        match self.next() {
            Some(Tok::Int(n)) => u32::try_from(n).map_err(|_| ParseError::BadToken(n.to_string())),
            Some(t) => Err(ParseError::Unexpected {
                expected: "type modifier",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    /// Parse a PG column type name (with optional modifiers and time-zone suffix).
    fn parse_pg_type(&mut self) -> Result<PgType, ParseError> {
        let first = self.ident()?.to_ascii_lowercase();
        match first.as_str() {
            "double" => {
                self.expect_ident_kw("PRECISION")?;
                Ok(PgType::DoublePrecision)
            }
            "character" | "char" => {
                // `character varying[(n)]` is varchar. Bare `character[(n)]` /
                // `char[(n)]` is PostgreSQL's fixed-length, blank-padded `bpchar`;
                // ferrosa stores it as `varchar(n)` — UTF-8 text with NO blank
                // padding. A deliberate, documented deviation: pgbench's
                // `filler char(84)` is the motivating case and never reads it back.
                if self.peek_is_kw("VARYING") {
                    self.next();
                }
                Ok(PgType::Varchar(self.optional_length()?))
            }
            "timestamp" | "time" => self.parse_temporal(&first),
            _ => self.parse_simple_type(&first),
        }
    }

    fn optional_length(&mut self) -> Result<Option<u32>, ParseError> {
        let mods = self.parse_type_modifiers()?;
        match mods.as_slice() {
            [] => Ok(None),
            [n] => Ok(Some(*n)),
            _ => Err(ParseError::BadToken("type takes one modifier".into())),
        }
    }

    /// `timestamp|time [(p)] [WITH|WITHOUT TIME ZONE]`. `time with time zone`
    /// is refused as an unknown type: there is no storage type for it.
    fn parse_temporal(&mut self, base: &str) -> Result<PgType, ParseError> {
        self.optional_length()?; // precision: accepted, storage is microseconds
        let with_tz = if self.peek_is_kw("WITH") {
            self.next();
            true
        } else if self.peek_is_kw("WITHOUT") {
            self.next();
            false
        } else {
            return Ok(temporal_type(base, false));
        };
        self.expect_ident_kw("TIME")?;
        self.expect_ident_kw("ZONE")?;
        if base == "time" && with_tz {
            return Err(ParseError::UnknownType("time with time zone".into()));
        }
        Ok(temporal_type(base, with_tz))
    }

    fn parse_simple_type(&mut self, name: &str) -> Result<PgType, ParseError> {
        match name {
            "smallint" | "int2" => Ok(PgType::SmallInt),
            "integer" | "int" | "int4" => Ok(PgType::Integer),
            "bigint" | "int8" => Ok(PgType::BigInt),
            "real" | "float4" => Ok(PgType::Real),
            "float8" => Ok(PgType::DoublePrecision),
            "boolean" | "bool" => Ok(PgType::Boolean),
            "text" => Ok(PgType::Text),
            "varchar" => Ok(PgType::Varchar(self.optional_length()?)),
            "bytea" => Ok(PgType::Bytea),
            "uuid" => Ok(PgType::Uuid),
            "date" => Ok(PgType::Date),
            "inet" => Ok(PgType::Inet),
            "timestamptz" => Ok(PgType::TimestampTz),
            "jsonb" => Ok(PgType::Jsonb),
            "json" => Ok(PgType::Json),
            "numeric" | "decimal" => self.parse_numeric(),
            "serial" | "serial2" | "serial4" | "serial8" | "smallserial" | "bigserial" => {
                Err(ParseError::UnsupportedClause(UnsupportedClause::Serial))
            }
            other => Err(ParseError::UnknownType(other.to_string())),
        }
    }

    fn parse_numeric(&mut self) -> Result<PgType, ParseError> {
        let mods = self.parse_type_modifiers()?;
        match mods.as_slice() {
            [] => Ok(PgType::Numeric {
                precision: None,
                scale: None,
            }),
            [p] => Ok(PgType::Numeric {
                precision: Some(*p),
                scale: None,
            }),
            [p, s] => Ok(PgType::Numeric {
                precision: Some(*p),
                scale: Some(*s),
            }),
            _ => Err(ParseError::BadToken(
                "numeric takes at most two modifiers".into(),
            )),
        }
    }
}

fn temporal_type(base: &str, with_tz: bool) -> PgType {
    match (base, with_tz) {
        ("time", _) => PgType::Time,
        (_, true) => PgType::TimestampTz,
        (_, false) => PgType::Timestamp,
    }
}

/// Merge column-level and table-level primary keys, validate, and build the AST.
fn finish_create_table(
    if_not_exists: bool,
    name: TableRef,
    mut columns: Vec<ColumnDef>,
    table_pk: Option<Vec<String>>,
) -> Result<Statement, ParseError> {
    if columns.is_empty() {
        return Err(ParseError::Unexpected {
            expected: "at least one column",
            found: ")".into(),
        });
    }
    check_unique_names(&columns)?;
    let inline: Vec<String> = columns
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name.clone())
        .collect();
    let primary_key = match (inline.len(), table_pk) {
        (0, Some(cols)) => cols,
        // PostgreSQL allows a table with no PRIMARY KEY. The parser reports that
        // faithfully as an empty key rather than refusing; supplying a synthetic key
        // (and rejecting the reserved `_sys_` prefix) is the Postgres front-end's job,
        // where `is_reserved_column_name` lives.
        (0, None) => Vec::new(),
        (1, None) => inline,
        _ => return Err(ParseError::MultiplePrimaryKeys),
    };
    for key in &primary_key {
        match columns.iter_mut().find(|c| &c.name == key) {
            Some(col) => {
                col.primary_key = true;
                col.not_null = true; // a key column can never be NULL
            }
            None => return Err(ParseError::UnknownPrimaryKeyColumn(key.clone())),
        }
    }
    Ok(Statement::CreateTable(Box::new(CreateTableStmt {
        if_not_exists,
        name,
        columns,
        primary_key,
    })))
}

fn check_unique_names(columns: &[ColumnDef]) -> Result<(), ParseError> {
    for (i, col) in columns.iter().enumerate() {
        if columns[..i].iter().any(|c| c.name == col.name) {
            return Err(ParseError::DuplicateColumn(col.name.clone()));
        }
    }
    Ok(())
}
