//! Module: DDL/DML-adjacent parsing — `CREATE TABLE` for the Ecto-migration subset
//! (D10, T-130) and `COPY ... FROM STDIN` with its option list.
//! Correctness: Correct when every statement `ecto_sql` emits for `create table`
//! parses to the expected AST, and every out-of-scope clause fails with a typed
//! `ParseError::UnsupportedClause` naming that clause (never silently dropped).
//! Last revised: 2026-10-09
//! Last changed: `COPY ... FROM STDIN` options — `FREEZE [ON|OFF]` accepted-and-recorded
//! (no LSM analogue), every other option refused by name as `ParseError::UnsupportedCopy`.
//!
//! This is a child module of `parser` so it shares the private lexer/`Parser`.
//! Every loop here iterates over the token vector once; there is no recursion.

use super::{ParseError, Parser, Tok};
use crate::ast::{
    AlterOperation, AlterTableStmt, AnalyzeStmt, ColumnDef, CopyFormatKind, CopyFromStdinStmt,
    CreateTableStmt, DropTableStatement, ForeignKeyConstraint, PgType, Statement, TableRef,
    TruncateStatement, UnsupportedClause, VacuumStmt,
};

/// The one schema a table may be qualified with. Other schemas are refused
/// (`UnsupportedClause::ForeignSchema`) until schema-to-keyspace mapping exists.
const MAPPED_SCHEMA: &str = "public";

/// The `CREATE TABLE ... WITH (key = value)` storage parameters ferrosa **records
/// but does not apply**. Each is a pure physical-layout or background-maintenance
/// hint with no effect on query results:
///
/// - `fillfactor` — the heap page-fill target. It only changes how full Postgres
///   packs heap pages; it can never change what a query returns. pgbench's own
///   schema sets `with (fillfactor=100)`, so refusing it would keep `pgbench -i`
///   broken.
/// - `autovacuum_enabled` — Postgres's own autovacuum toggle, again layout-only.
///
/// ferrosa is an LSM/SSTable store: it has no heap pages and no autovacuum, so it
/// has no equivalent to honour either. Accepting them is safe precisely because
/// they are hints, and the AST keeps each one (`CreateTableStmt::storage_parameters`)
/// so the acceptance is visible rather than a silent swallow. Every other option
/// name is refused (`ParseError::UnsupportedStorageParameter`), so a client that
/// asked for real behaviour (e.g. `WITH (oids=false)`) learns immediately.
const ACCEPTED_STORAGE_PARAMETERS: &[&str] = &["fillfactor", "autovacuum_enabled"];

/// The `COPY ... FROM STDIN` options ferrosa **records but does not apply**.
///
/// - `FREEZE [ON | OFF]` — tells PostgreSQL to mark the rows this `COPY` loads as frozen
///   (all-visible, needing no vacuum). That is a heap-page concept and ferrosa is an
///   LSM/SSTable store: it has no heap pages and therefore no frozen-row concept, so there is
///   nothing to set. The option is accepted because it can never change what a query returns,
///   and the parsed value is kept on the statement (`CopyFromStdinStmt::freeze`) so the
///   acceptance is visible rather than a silent swallow. `pgbench -i` sends
///   `with (freeze on)` for every ordinary table on PostgreSQL v14+, so refusing it keeps
///   `pgbench -i` broken.
///
/// Every other COPY option name is refused by name (`ParseError::UnsupportedCopy`, whose
/// message names the option), so a client that asked for real behaviour learns immediately
/// rather than getting a COPY that does not do what it asked for.
pub(super) const RECORDED_COPY_OPTIONS: &[&str] = &["FREEZE"];

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
        let mut foreign_keys: Vec<ForeignKeyConstraint> = Vec::new();
        loop {
            self.parse_table_element(&mut columns, &mut table_pk, &mut foreign_keys)?;
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
        let storage_parameters = self.parse_optional_storage_parameters()?;
        self.expect_end()?;
        finish_create_table(
            if_not_exists,
            name,
            columns,
            table_pk,
            foreign_keys,
            storage_parameters,
        )
    }

    /// `WITH ( key = value [, ...] )`, an optional clause after the table body.
    ///
    /// The accepted names are [`ACCEPTED_STORAGE_PARAMETERS`]: PostgreSQL physical-layout
    /// hints ferrosa **records but does not apply**. Every other name is refused by name
    /// (`ParseError::UnsupportedStorageParameter`) rather than parsed and discarded — a
    /// client that asked for real behaviour must not get silent success. A `WITH` not
    /// followed by `(` (including PostgreSQL's `WITHOUT OIDS`) is not this clause and is
    /// left to `expect_end` to reject loudly.
    fn parse_optional_storage_parameters(&mut self) -> Result<Vec<(String, String)>, ParseError> {
        if !self.peek_is_kw("WITH") {
            return Ok(Vec::new());
        }
        self.next(); // WITH
        self.expect(&Tok::LParen, "(")?;
        let mut params = Vec::new();
        loop {
            // The key is an identifier, matched case-insensitively and stored lowercased.
            let key = self.ident()?.to_ascii_lowercase();
            if !ACCEPTED_STORAGE_PARAMETERS.contains(&key.as_str()) {
                return Err(ParseError::UnsupportedStorageParameter(key));
            }
            self.expect(&Tok::Eq, "=")?;
            params.push((key, self.parse_storage_parameter_value()?));
            match self.peek() {
                Some(Tok::Comma) => {
                    self.next();
                }
                Some(Tok::RParen) => {
                    self.next();
                    return Ok(params);
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

    /// A storage-parameter value: a number, a quoted string, or a bare identifier
    /// (`false`, `on`).
    fn parse_storage_parameter_value(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Tok::Int(n)) => Ok(n.to_string()),
            Some(Tok::Str(s) | Tok::Ident(s)) => Ok(s),
            other => Err(ParseError::Unexpected {
                expected: "a storage parameter value",
                found: format!("{other:?}"),
            }),
        }
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
        foreign_keys: &mut Vec<ForeignKeyConstraint>,
    ) -> Result<(), ParseError> {
        if self.peek_is_kw("CONSTRAINT") {
            self.next();
            let name = self.ident()?; // constraint name; the constraint kind follows
            return self.parse_table_constraint(table_pk, Some(name), foreign_keys);
        }
        if let Some(Tok::Ident(w)) = self.peek() {
            if matches!(
                w.to_ascii_uppercase().as_str(),
                "PRIMARY" | "FOREIGN" | "CHECK" | "UNIQUE"
            ) {
                return self.parse_table_constraint(table_pk, None, foreign_keys);
            }
        }
        let (col, reference) = self.parse_column_def()?;
        // A column-level `REFERENCES` is a foreign-key constraint on that column. Fold it
        // into the table's constraint list.
        if let Some(reference) = reference {
            foreign_keys.push(reference);
        }
        columns.push(col);
        Ok(())
    }

    fn parse_table_constraint(
        &mut self,
        table_pk: &mut Option<Vec<String>>,
        name: Option<String>,
        foreign_keys: &mut Vec<ForeignKeyConstraint>,
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
            "FOREIGN" => {
                self.expect_ident_kw("KEY")?;
                let columns = self.parse_paren_ident_list()?;
                let fk = self.parse_foreign_key_tail(name, columns)?;
                foreign_keys.push(fk);
                Ok(())
            }
            "CHECK" => Err(ParseError::UnsupportedClause(UnsupportedClause::Check)),
            "UNIQUE" => Err(ParseError::UnsupportedClause(UnsupportedClause::Unique)),
            _ => Err(ParseError::Unexpected {
                expected: "PRIMARY KEY, FOREIGN KEY, CHECK or UNIQUE",
                found: word,
            }),
        }
    }

    /// The tail of a foreign-key clause after `FOREIGN KEY`: `(<cols>) REFERENCES <parent>
    /// [(<pcols>)]`, plus the referential-action / deferrability tail (which must be empty
    /// or an accepted `NO ACTION`/`RESTRICT`).
    fn parse_foreign_key_tail(
        &mut self,
        name: Option<String>,
        columns: Vec<String>,
    ) -> Result<ForeignKeyConstraint, ParseError> {
        self.expect_ident_kw("REFERENCES")?;
        let parent = self.parse_qualified_table()?;
        let parent_columns = if matches!(self.peek(), Some(Tok::LParen)) {
            Some(self.parse_paren_ident_list()?)
        } else {
            None
        };
        self.parse_referential_tail()?;
        Ok(ForeignKeyConstraint {
            name,
            columns,
            parent,
            parent_columns,
        })
    }

    /// The referential-action / deferrability tail of a foreign-key clause.
    ///
    /// ferrosa implements PostgreSQL's default **NO ACTION** (and the equivalent
    /// immediate **RESTRICT**): a child write is refused when the parent is absent, and a
    /// parent delete is refused while referencing children remain. `ON DELETE`/`ON UPDATE`
    /// `CASCADE`, `SET NULL` and `SET DEFAULT`, `MATCH` forms other than the default
    /// `SIMPLE`, and `DEFERRABLE`/`INITIALLY` are all refused **by name** — a client that
    /// asked for cascading deletes must never receive a plain NO ACTION constraint in its
    /// place. `NOT VALID` is refused for the same reason: skipping validation of existing
    /// rows would make the constraint a lie.
    fn parse_referential_tail(&mut self) -> Result<(), ParseError> {
        loop {
            match self.peek() {
                Some(Tok::On) => {
                    self.next();
                    let event = self.ident()?.to_ascii_uppercase();
                    if event != "DELETE" && event != "UPDATE" {
                        return Err(ParseError::Unexpected {
                            expected: "DELETE or UPDATE",
                            found: event,
                        });
                    }
                    let action = self.ident()?.to_ascii_uppercase();
                    match action.as_str() {
                        // The default. `NO ACTION` and `RESTRICT` are the same here: both
                        // refuse the write that would leave a dangling reference.
                        "NO" => {
                            self.expect_ident_kw("ACTION")?;
                        }
                        "RESTRICT" => {}
                        // `SET NULL` / `SET DEFAULT` are two words; consume the second so the
                        // refusal NAMES the whole action the client wrote, never just `SET`.
                        "SET" => {
                            let fill = self.ident()?.to_ascii_uppercase();
                            return Err(ParseError::UnsupportedAlter(format!(
                                "ON {event} SET {fill} is not supported: ferrosa implements \
                                 NO ACTION (refuse) only, never CASCADE, SET NULL or SET DEFAULT"
                            )));
                        }
                        other => {
                            return Err(ParseError::UnsupportedAlter(format!(
                                "ON {event} {other} is not supported: ferrosa implements \
                                 NO ACTION (refuse) only, never CASCADE, SET NULL or SET DEFAULT"
                            )))
                        }
                    }
                }
                Some(Tok::Not) => {
                    // `NOT DEFERRABLE` (the default) is a no-op; `NOT VALID` is not.
                    self.next();
                    if self.peek_is_kw("VALID") {
                        return Err(ParseError::UnsupportedAlter(
                            "NOT VALID is not supported: ferrosa validates the constraint \
                             immediately rather than accepting one it does not check"
                                .into(),
                        ));
                    }
                    self.expect_ident_kw("DEFERRABLE")?;
                }
                Some(Tok::Ident(w)) => {
                    match w.to_ascii_uppercase().as_str() {
                        "MATCH" => return Err(ParseError::UnsupportedAlter(
                            "MATCH FULL/PARTIAL is not supported (only the default MATCH SIMPLE)"
                                .into(),
                        )),
                        "DEFERRABLE" => {
                            return Err(ParseError::UnsupportedAlter(
                                "DEFERRABLE foreign keys are not supported: checks run immediately"
                                    .into(),
                            ))
                        }
                        "INITIALLY" => {
                            return Err(ParseError::UnsupportedAlter(
                                "INITIALLY DEFERRED/IMMEDIATE is not supported: checks run \
                             immediately"
                                    .into(),
                            ))
                        }
                        _ => return Ok(()),
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    /// `COPY <name> [(<cols>)] FROM STDIN [[WITH] (<options>)]`.
    ///
    /// Only the `FROM STDIN` direction is parsed: `COPY ... TO` writes a payload the client reads,
    /// which is a different exchange and is refused by name. Options are the modern parenthesised
    /// form; an unrecognised option is refused by name (`ParseError::UnsupportedCopy`, whose
    /// message names the option) rather than ignored, because a client that asked for csv and
    /// silently got text would store garbage. The one option with no analogue here — `FREEZE`,
    /// a heap-page concept an LSM cannot honour — is accepted and recorded instead
    /// (see [`RECORDED_COPY_OPTIONS`] and `CopyFromStdinStmt::freeze`).
    ///
    /// A COPY refusal is a COPY refusal: it is reported as `UnsupportedCopy`, never as the
    /// `ALTER TABLE` form that [`ParseError::UnsupportedAlter`] means.
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
            return Err(ParseError::UnsupportedCopy(
                "only COPY ... FROM STDIN is supported, not COPY ... TO".into(),
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
            return Err(ParseError::UnsupportedCopy(
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
            freeze: None,
        };
        // `WITH` is optional, as is the parenthesised list entirely.
        if self.peek_is_kw("WITH") {
            self.next();
        }
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.parse_copy_options(&mut stmt)?;
        } else if self.peek().is_some() {
            return Err(ParseError::UnsupportedCopy(
                "only the parenthesised COPY option list is supported".into(),
            ));
        }
        Ok(Statement::CopyFromStdin(Box::new(stmt)))
    }

    /// `(key [value] [, ...])`, applied to `stmt`.
    ///
    /// Each option is named in the match below: `FORMAT`/`DELIMITER`/`NULL`/`HEADER` are applied,
    /// and `FREEZE` (see [`RECORDED_COPY_OPTIONS`]) is recorded as a no-op. Anything else is
    /// refused by name (`ParseError::UnsupportedCopy`) — the set is never widened to "accept
    /// anything", so an option ferrosa cannot honour fails loudly.
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
                            return Err(ParseError::UnsupportedCopy(format!(
                                "COPY option `FORMAT` value `{other}` is not supported"
                            )))
                        }
                    };
                }
                "DELIMITER" => stmt.delimiter = Some(self.copy_option_char()?),
                "NULL" => stmt.null = Some(self.copy_option_string()?),
                "HEADER" => stmt.header = true,
                // Accept-and-record (never apply): a heap-page concept this LSM cannot honour.
                // `pgbench -i` writes `with (freeze on)`; see `RECORDED_COPY_OPTIONS`.
                "FREEZE" => {
                    let value = self.copy_option_word()?;
                    stmt.freeze = Some(match value.as_str() {
                        "ON" => true,
                        "OFF" => false,
                        other => {
                            return Err(ParseError::UnsupportedCopy(format!(
                                "COPY option `FREEZE` value `{other}` is not supported"
                            )))
                        }
                    });
                    debug_assert!(RECORDED_COPY_OPTIONS.contains(&"FREEZE"));
                }
                other => {
                    return Err(ParseError::UnsupportedCopy(format!(
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
            _ => Err(ParseError::UnsupportedCopy(format!(
                "COPY option `DELIMITER` must be a single character, got {text:?}"
            ))),
        }
    }

    /// A bare-word COPY option value (`FREEZE ON` / `FREEZE OFF`), upper-cased for matching.
    ///
    /// `ON` is a keyword token (`JOIN ... ON`), so `ident()` alone would reject `freeze on`;
    /// `OFF` is an ordinary identifier.
    fn copy_option_word(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Tok::Ident(s) | Tok::QuotedIdent(s)) => Ok(s.to_ascii_uppercase()),
            Some(Tok::On) => Ok("ON".to_string()),
            other => Err(ParseError::Unexpected {
                expected: "an option value",
                found: format!("{other:?}"),
            }),
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
        // `ADD CONSTRAINT <name> <kind>`. The name is meaningful for a FOREIGN KEY (it names
        // the constraint and its index); PostgreSQL does not require it for a PRIMARY KEY.
        if self.peek_is_kw("CONSTRAINT") {
            self.next();
            let name = self.ident()?; // the constraint name
            if self.peek_is_kw("FOREIGN") {
                return self.parse_add_foreign_key(Some(name));
            }
            return self.parse_add_primary_key();
        }
        if self.peek_is_kw("FOREIGN") {
            return self.parse_add_foreign_key(None);
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
                "only ADD COLUMN, ADD PRIMARY KEY and ADD FOREIGN KEY are supported".into(),
            ));
        }
        let (def, reference) = self.parse_column_def()?;
        // A column-level PRIMARY KEY would be a second way to say `ADD PRIMARY KEY`, and the two
        // would have to agree about order and about the storage key. Refuse it rather than pick.
        if def.primary_key {
            return Err(ParseError::UnsupportedAlter(
                "a column-level PRIMARY KEY in ADD COLUMN is not supported: use ADD PRIMARY KEY"
                    .into(),
            ));
        }
        // A column-level REFERENCES in ADD COLUMN is a foreign-key constraint; it is parsed but
        // the ALTER layer applies one operation, so it is refused by name rather than dropped.
        if reference.is_some() {
            return Err(ParseError::UnsupportedAlter(
                "a column-level REFERENCES in ADD COLUMN is not supported: \
                 use ADD CONSTRAINT <name> FOREIGN KEY (<col>) REFERENCES <parent>"
                    .into(),
            ));
        }
        Ok(AlterOperation::AddColumn(def))
    }

    /// `ADD [CONSTRAINT <name>] FOREIGN KEY (<cols>) REFERENCES <parent> [(<pcols>)]`.
    fn parse_add_foreign_key(
        &mut self,
        name: Option<String>,
    ) -> Result<AlterOperation, ParseError> {
        self.expect_ident_kw("FOREIGN")?;
        self.expect_ident_kw("KEY")?;
        let columns = self.parse_paren_ident_list()?;
        let fk = self.parse_foreign_key_tail(name, columns)?;
        Ok(AlterOperation::AddForeignKey(fk))
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
    /// on the returned def (`primary_key`) and folded into the table key later. A
    /// column-level `REFERENCES` is returned separately as a foreign-key constraint
    /// (it names this column as the referencing side).
    fn parse_column_def(
        &mut self,
    ) -> Result<(ColumnDef, Option<ForeignKeyConstraint>), ParseError> {
        let name = self.ident()?;
        let ty = self.parse_pg_type()?;
        let mut def = ColumnDef {
            name,
            ty,
            not_null: false,
            primary_key: false,
        };
        let mut reference = None;
        while let Some(tok) = self.peek() {
            if matches!(tok, Tok::Comma | Tok::RParen) {
                break;
            }
            self.parse_column_constraint(&mut def, &mut reference)?;
        }
        Ok((def, reference))
    }

    fn parse_column_constraint(
        &mut self,
        def: &mut ColumnDef,
        reference: &mut Option<ForeignKeyConstraint>,
    ) -> Result<(), ParseError> {
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
            "REFERENCES" => {
                if reference.is_some() {
                    return Err(ParseError::UnsupportedAlter(
                        "a column may carry at most one REFERENCES clause".into(),
                    ));
                }
                let parent = self.parse_qualified_table()?;
                let parent_columns = if matches!(self.peek(), Some(Tok::LParen)) {
                    Some(self.parse_paren_ident_list()?)
                } else {
                    None
                };
                self.parse_referential_tail()?;
                *reference = Some(ForeignKeyConstraint {
                    name: None,
                    columns: vec![def.name.clone()],
                    parent,
                    parent_columns,
                });
                Ok(())
            }
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

    /// `TRUNCATE [TABLE] name [, name]*`. The leading `TRUNCATE` token has not
    /// been consumed yet.
    ///
    /// `TABLE` is an optional noise word in PostgreSQL; both `TRUNCATE t` and
    /// `TRUNCATE TABLE t` are accepted. Each name is kept verbatim (with any
    /// schema qualifier); the executor resolves and truncates them one at a
    /// time through the replicated write path. `TRUNCATE ... CASCADE` /
    /// `RESTART IDENTITY` are **not** parsed: they would silently mean nothing
    /// here (ferrosa has no foreign keys and no sequences), so a stray token
    /// after the table list is a fail-loud `ParseError::Unexpected`.
    pub(super) fn parse_truncate(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("TRUNCATE")?;
        if self.peek_is_kw("TABLE") {
            self.next();
        }
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
        Ok(Statement::Truncate(Box::new(TruncateStatement { tables })))
    }

    /// `VACUUM [FULL] [ANALYZE|ANALYSE] [name]`. The leading `VACUUM` token has
    /// not been consumed yet.
    ///
    /// `FULL` and `ANALYZE` are legacy noise words in PostgreSQL's grammar, so
    /// they are accepted in either order without parentheses; a parenthesised
    /// option list (`VACUUM (VERBOSE)`) is not parsed — a stray token after the
    /// modifiers is a fail-loud `ParseError::Unexpected`. At most one table name
    /// is accepted. All forms parse; the front-end answers each as a successful
    /// no-op regardless of what was asked for.
    pub(super) fn parse_vacuum(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("VACUUM")?;
        let mut full = false;
        if self.peek_is_kw("FULL") {
            self.next();
            full = true;
        }
        let analyze = self.parse_analyze_kw()?;
        // `FULL` may also follow the `ANALYZE` noise word (`VACUUM ANALYZE FULL t`).
        if !full && self.peek_is_kw("FULL") {
            self.next();
            full = true;
        }
        let table = if self.peek().is_some() {
            Some(self.parse_qualified_table()?)
        } else {
            None
        };
        self.expect_end()?;
        Ok(Statement::Vacuum(VacuumStmt {
            full,
            analyze,
            table,
        }))
    }

    /// `ANALYZE [name]` / `ANALYSE [name]`. The leading keyword has not been
    /// consumed yet (either spelling is accepted). At most one table name is
    /// accepted; the front-end answers it as a successful no-op.
    pub(super) fn parse_analyze(&mut self) -> Result<Statement, ParseError> {
        // The dispatcher matched `ANALYZE`/`ANALYSE`; consume it verbatim.
        self.next();
        let table = if self.peek().is_some() {
            Some(self.parse_qualified_table()?)
        } else {
            None
        };
        self.expect_end()?;
        Ok(Statement::Analyze(AnalyzeStmt { table }))
    }

    /// Consume an optional `ANALYZE`/`ANALYSE` noise word; report whether it was
    /// present.
    fn parse_analyze_kw(&mut self) -> Result<bool, ParseError> {
        if self.peek_is_kw("ANALYZE") || self.peek_is_kw("ANALYSE") {
            self.next();
            Ok(true)
        } else {
            Ok(false)
        }
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
    foreign_keys: Vec<ForeignKeyConstraint>,
    storage_parameters: Vec<(String, String)>,
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
        foreign_keys,
        storage_parameters,
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
