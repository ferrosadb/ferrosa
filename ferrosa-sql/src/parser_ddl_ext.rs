//! Module: the DDL and utility statements Oban's stock migrations add to the subset
//! (`specs/todo/postgres-frontend/oban-compat.md`, workstream W3): `LOCK TABLE`, and the
//! statements added after it in this file.
//! Correctness: every accepted form parses to an AST the front end gives real semantics;
//! every form outside the subset is a typed `ParseError` naming it, never dropped.
//! Last revised: 2026-11-08
//!
//! A child module of `parser`, so it shares the private lexer and `Parser`. No recursion.

use super::{ParseError, Parser, Tok};
use crate::ast::{
    AlterOperation, AlterTypeStmt, CommentStmt, CreateTypeStmt, LockMode, LockStmt, Statement,
};

impl Parser {
    /// The word after the statement's first keyword, upper-cased, without consuming it.
    fn second_word(&self) -> Option<String> {
        match self.toks.get(self.pos + 1) {
            Some(Tok::Ident(w)) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// `CREATE TYPE …` or `CREATE TABLE …` (the latter in `parser_ddl`).
    pub(super) fn parse_create_any(&mut self) -> Result<Statement, ParseError> {
        match self.second_word().as_deref() {
            Some("TYPE") => self.parse_create_type(),
            Some("SEQUENCE") => Err(ParseError::Unsupported(
                "CREATE SEQUENCE (there are no sequences; bigserial columns get unique \
                 server-generated ids, D15)"
                    .into(),
            )),
            _ => self.parse_create(),
        }
    }

    /// `ALTER TYPE …` or `ALTER TABLE …` (the latter in `parser_ddl`).
    pub(super) fn parse_alter_any(&mut self) -> Result<Statement, ParseError> {
        match self.second_word().as_deref() {
            Some("TYPE") => self.parse_alter_type(),
            _ => self.parse_alter_table(),
        }
    }

    /// `CREATE TYPE [schema.]name AS ENUM ( 'label' [, ...] )`. Composite, range and base
    /// types are refused by name.
    fn parse_create_type(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("CREATE")?;
        self.expect_ident_kw("TYPE")?;
        let name = self.parse_qualified_table()?;
        self.expect(&Tok::As, "AS")?;
        if !self.peek_is_kw("ENUM") {
            return Err(ParseError::Unsupported(
                "CREATE TYPE other than AS ENUM".into(),
            ));
        }
        self.next(); // ENUM
        self.expect(&Tok::LParen, "(")?;
        let mut labels = Vec::new();
        if !matches!(self.peek(), Some(Tok::RParen)) {
            labels.push(self.string_literal()?);
            while matches!(self.peek(), Some(Tok::Comma)) {
                self.next();
                labels.push(self.string_literal()?);
            }
        }
        self.expect(&Tok::RParen, ")")?;
        self.expect_end()?;
        Ok(Statement::CreateType(Box::new(CreateTypeStmt {
            name,
            labels,
        })))
    }

    /// `ALTER TYPE [schema.]name ADD VALUE [IF NOT EXISTS] 'label'`.
    fn parse_alter_type(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("ALTER")?;
        self.expect_ident_kw("TYPE")?;
        let name = self.parse_qualified_table()?;
        if !self.peek_is_kw("ADD") {
            return Err(ParseError::Unsupported(
                "ALTER TYPE other than ADD VALUE".into(),
            ));
        }
        self.next(); // ADD
        self.expect_ident_kw("VALUE")?;
        let if_not_exists = if self.peek_is_kw("IF") {
            self.next();
            self.expect(&Tok::Not, "NOT")?;
            self.expect_ident_kw("EXISTS")?;
            true
        } else {
            false
        };
        let value = self.string_literal()?;
        if self.peek_is_kw("BEFORE") || self.peek_is_kw("AFTER") {
            return Err(ParseError::Unsupported(
                "ALTER TYPE ... ADD VALUE BEFORE/AFTER (labels are appended)".into(),
            ));
        }
        self.expect_end()?;
        Ok(Statement::AlterType(Box::new(AlterTypeStmt {
            name,
            value,
            if_not_exists,
        })))
    }

    /// A `'single quoted'` string literal.
    fn string_literal(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(s),
            other => Err(ParseError::Unexpected {
                expected: "a string literal",
                found: format!("{other:?}"),
            }),
        }
    }

    /// `LOCK [TABLE] [ONLY] name [, ...] [IN <mode> MODE]`. `NOWAIT` is refused: the
    /// front end's lock waits, and a `NOWAIT` that waited would be a lie.
    pub(super) fn parse_lock(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("LOCK")?;
        if self.peek_is_kw("TABLE") {
            self.next();
        }
        if self.peek_is_kw("ONLY") {
            self.next(); // no inheritance in ferrosa, so ONLY is exact
        }
        let mut tables = vec![self.parse_qualified_table()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            tables.push(self.parse_qualified_table()?);
        }
        let mode = if self.peek_is_kw("IN") {
            self.next();
            self.parse_lock_mode()?
        } else {
            LockMode::AccessExclusive
        };
        if self.peek_is_kw("NOWAIT") {
            return Err(ParseError::Unsupported(
                "LOCK ... NOWAIT (the lock waits; it cannot fail fast)".into(),
            ));
        }
        self.expect_end()?;
        Ok(Statement::Lock(LockStmt { tables, mode }))
    }

    /// `COMMENT ON TABLE name IS 'text' | NULL`. Only tables carry comments here; any other
    /// object kind is refused by name.
    pub(super) fn parse_comment(&mut self) -> Result<Statement, ParseError> {
        self.expect_ident_kw("COMMENT")?;
        self.expect(&Tok::On, "ON")?;
        let kind = self.ident()?.to_ascii_uppercase();
        if kind != "TABLE" {
            return Err(ParseError::Unsupported(format!(
                "COMMENT ON {kind} (only COMMENT ON TABLE)"
            )));
        }
        let table = self.parse_qualified_table()?;
        self.expect_ident_kw("IS")?;
        let comment = match self.next() {
            Some(Tok::Str(text)) => Some(text),
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("NULL") => None,
            other => {
                return Err(ParseError::Unexpected {
                    expected: "a string literal or NULL",
                    found: format!("{other:?}"),
                })
            }
        };
        self.expect_end()?;
        Ok(Statement::Comment(Box::new(CommentStmt { table, comment })))
    }

    /// `<mode> MODE`, the words after `IN`.
    fn parse_lock_mode(&mut self) -> Result<LockMode, ParseError> {
        let mut words: Vec<String> = Vec::with_capacity(3);
        // At most three words precede MODE ("SHARE UPDATE EXCLUSIVE").
        for _ in 0..4 {
            if self.peek_is_kw("MODE") {
                break;
            }
            words.push(self.ident()?.to_ascii_uppercase());
        }
        self.expect_ident_kw("MODE")?;
        let mode = match words.join(" ").as_str() {
            "ACCESS SHARE" => LockMode::AccessShare,
            "ROW SHARE" => LockMode::RowShare,
            "ROW EXCLUSIVE" => LockMode::RowExclusive,
            "SHARE UPDATE EXCLUSIVE" => LockMode::ShareUpdateExclusive,
            "SHARE" => LockMode::Share,
            "SHARE ROW EXCLUSIVE" => LockMode::ShareRowExclusive,
            "EXCLUSIVE" => LockMode::Exclusive,
            "ACCESS EXCLUSIVE" => LockMode::AccessExclusive,
            other => {
                return Err(ParseError::Unexpected {
                    expected: "a lock mode",
                    found: other.to_string(),
                })
            }
        };
        Ok(mode)
    }
}

/// Words that end a column `DEFAULT` expression at nesting depth 0: the column constraints
/// that may follow it.
const DEFAULT_TERMINATORS: &[&str] = &[
    "NULL",
    "PRIMARY",
    "CHECK",
    "UNIQUE",
    "REFERENCES",
    "CONSTRAINT",
    "DEFAULT",
    "COLLATE",
];

impl Parser {
    /// `DEFAULT <expr>` after the `DEFAULT` keyword: the expression ends at a depth-0 `,` or
    /// `)` or at the next column constraint. It is parsed with the general scalar grammar
    /// (so a malformed default is refused here) and returned as canonical SQL text.
    pub(super) fn parse_default_expr(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        while let Some(tok) = self.toks.get(self.pos) {
            let ends = depth == 0
                && match tok {
                    Tok::Comma | Tok::RParen | Tok::Not => true,
                    Tok::Ident(w) => DEFAULT_TERMINATORS
                        .iter()
                        .any(|k| w.eq_ignore_ascii_case(k)),
                    _ => false,
                };
            if ends {
                break;
            }
            match tok {
                Tok::LParen | Tok::LBracket => depth += 1,
                Tok::RParen | Tok::RBracket => depth = depth.saturating_sub(1),
                _ => {}
            }
            self.pos += 1;
        }
        self.parse_expression_slice(start, self.pos, "a DEFAULT expression")
    }

    /// Parse the tokens `start..end` as one scalar expression and render them. A sub-parser
    /// owns its tokens, so the slice is copied once; the statement's tokens are untouched.
    fn parse_expression_slice(
        &self,
        start: usize,
        end: usize,
        what: &'static str,
    ) -> Result<String, ParseError> {
        let slice = &self.toks[start..end];
        if slice.is_empty() {
            return Err(ParseError::Unexpected {
                expected: what,
                found: "nothing".into(),
            });
        }
        let mut sub = Parser::new(slice.to_vec());
        sub.parse_scalar_expr()?;
        sub.expect_end()?;
        Ok(render_tokens(slice))
    }

    /// `CHECK ( <expr> ) [NOT VALID]` after `ADD [CONSTRAINT name]`.
    pub(super) fn parse_add_check(
        &mut self,
        name: Option<String>,
    ) -> Result<AlterOperation, ParseError> {
        self.expect_ident_kw("CHECK")?;
        self.expect(&Tok::LParen, "(")?;
        let start = self.pos;
        let mut depth = 1usize;
        while depth > 0 {
            match self.next() {
                Some(Tok::LParen) => depth += 1,
                Some(Tok::RParen) => depth -= 1,
                Some(_) => {}
                None => return Err(ParseError::UnexpectedEnd),
            }
        }
        let sql = self.parse_expression_slice(start, self.pos - 1, "a CHECK expression")?;
        let not_valid = if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            self.expect_ident_kw("VALID")?;
            true
        } else {
            false
        };
        Ok(AlterOperation::AddCheck {
            name,
            sql,
            not_valid,
        })
    }

    /// `CONSTRAINT [IF EXISTS] name` after `DROP`.
    pub(super) fn parse_drop_constraint(&mut self) -> Result<AlterOperation, ParseError> {
        self.expect_ident_kw("CONSTRAINT")?;
        let if_exists = if self.peek_is_kw("IF") {
            self.next();
            self.expect_ident_kw("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.ident()?;
        if self.peek_is_kw("CASCADE") || self.peek_is_kw("RESTRICT") {
            return Err(ParseError::UnsupportedAlter(
                "DROP CONSTRAINT ... CASCADE/RESTRICT".into(),
            ));
        }
        Ok(AlterOperation::DropConstraint { name, if_exists })
    }

    /// `[COLUMN] c …` after `ALTER`: `[SET DATA] TYPE t`, `SET DEFAULT e`, `DROP DEFAULT`,
    /// `SET NOT NULL`, `DROP NOT NULL`. Anything else is refused by name.
    pub(super) fn parse_alter_column(&mut self) -> Result<AlterOperation, ParseError> {
        if self.peek_is_kw("COLUMN") {
            self.next();
        }
        let column = self.ident()?;
        if self.peek_is_kw("TYPE") {
            self.next();
            return self.parse_alter_column_type(column);
        }
        let verb = self.ident()?.to_ascii_uppercase();
        match (verb.as_str(), self.peek()) {
            ("SET", Some(Tok::Ident(w))) if w.eq_ignore_ascii_case("DATA") => {
                self.next();
                self.expect_ident_kw("TYPE")?;
                self.parse_alter_column_type(column)
            }
            ("SET", Some(Tok::Ident(w))) if w.eq_ignore_ascii_case("DEFAULT") => {
                self.next();
                let sql = self.parse_default_expr()?;
                Ok(AlterOperation::SetDefault { column, sql })
            }
            ("DROP", Some(Tok::Ident(w))) if w.eq_ignore_ascii_case("DEFAULT") => {
                self.next();
                Ok(AlterOperation::DropDefault { column })
            }
            ("SET", Some(Tok::Not)) => {
                self.next();
                self.expect_ident_kw("NULL")?;
                Ok(AlterOperation::SetNotNull { column })
            }
            ("DROP", Some(Tok::Not)) => {
                self.next();
                self.expect_ident_kw("NULL")?;
                Ok(AlterOperation::DropNotNull { column })
            }
            _ => Err(ParseError::UnsupportedAlter(format!(
                "ALTER COLUMN ... {verb} (only TYPE, SET/DROP DEFAULT and SET/DROP NOT NULL)"
            ))),
        }
    }

    fn parse_alter_column_type(&mut self, column: String) -> Result<AlterOperation, ParseError> {
        let (ty, user_type) = self.parse_column_type()?;
        if self.peek_is_kw("USING") || self.peek_is_kw("COLLATE") {
            return Err(ParseError::UnsupportedAlter(
                "ALTER COLUMN ... TYPE ... USING/COLLATE".into(),
            ));
        }
        Ok(AlterOperation::AlterColumnType {
            column,
            ty,
            user_type,
        })
    }

    /// `UNLOGGED` / `LOGGED` after `ALTER TABLE t SET`.
    pub(super) fn parse_alter_set(&mut self) -> Result<AlterOperation, ParseError> {
        match self.ident()?.to_ascii_uppercase().as_str() {
            "UNLOGGED" => Ok(AlterOperation::SetLogged(false)),
            "LOGGED" => Ok(AlterOperation::SetLogged(true)),
            other => Err(ParseError::UnsupportedAlter(format!(
                "ALTER TABLE ... SET {other} (only SET LOGGED / SET UNLOGGED)"
            ))),
        }
    }
}

/// Render tokens back to canonical SQL: keywords upper-cased, identifiers and literals
/// re-quoted, a space between words, none inside calls, subscripts, casts or before commas.
pub(super) fn render_tokens(toks: &[Tok]) -> String {
    let mut out = String::new();
    let mut prev: Option<&Tok> = None;
    for tok in toks {
        let glue = matches!(
            tok,
            Tok::Comma | Tok::RParen | Tok::RBracket | Tok::Dot | Tok::Cast
        ) || matches!(
            prev,
            Some(Tok::LParen | Tok::LBracket | Tok::Dot | Tok::Cast)
        ) || (matches!(tok, Tok::LParen | Tok::LBracket)
            && matches!(
                prev,
                Some(Tok::Ident(_) | Tok::QuotedIdent(_) | Tok::RBracket)
            ));
        if prev.is_some() && !glue {
            out.push(' ');
        }
        out.push_str(&token_text(tok));
        prev = Some(tok);
    }
    out
}

fn token_text(tok: &Tok) -> String {
    let keyword = match tok {
        Tok::Select => "SELECT",
        Tok::From => "FROM",
        Tok::Join => "JOIN",
        Tok::Inner => "INNER",
        Tok::On => "ON",
        Tok::Where => "WHERE",
        Tok::As => "AS",
        Tok::Group => "GROUP",
        Tok::By => "BY",
        Tok::Having => "HAVING",
        Tok::Order => "ORDER",
        Tok::Asc => "ASC",
        Tok::Desc => "DESC",
        Tok::Limit => "LIMIT",
        Tok::Offset => "OFFSET",
        Tok::And => "AND",
        Tok::Or => "OR",
        Tok::Not => "NOT",
        Tok::Distinct => "DISTINCT",
        Tok::Star => "*",
        Tok::Comma => ",",
        Tok::Dot => ".",
        Tok::LParen => "(",
        Tok::RParen => ")",
        Tok::Concat => "||",
        Tok::Cast => "::",
        Tok::Eq => "=",
        Tok::Ne => "<>",
        Tok::Lt => "<",
        Tok::Le => "<=",
        Tok::Gt => ">",
        Tok::Ge => ">=",
        Tok::Plus => "+",
        Tok::Minus => "-",
        Tok::Slash => "/",
        Tok::Percent => "%",
        Tok::LBracket => "[",
        Tok::RBracket => "]",
        Tok::Ident(w) => return w.clone(), // the rendered text needs its own copy
        Tok::QuotedIdent(w) => return format!("\"{}\"", w.replace('"', "\"\"")),
        Tok::Str(s) => return format!("'{}'", s.replace('\'', "''")),
        Tok::Int(n) => return n.to_string(),
        Tok::Float(f) => return f.to_string(),
        Tok::Param(n) => return format!("${n}"),
        Tok::Other(c) => return c.to_string(),
    };
    keyword.to_string()
}

#[cfg(test)]
mod tests {
    use crate::ast::{AlterOperation, LockMode, Statement};
    use crate::parser::{parse_statement, ParseError};

    #[test]
    fn ecto_migration_lock_parses() {
        let Ok(Statement::Lock(lock)) =
            parse_statement("LOCK TABLE \"schema_migrations\" IN SHARE UPDATE EXCLUSIVE MODE")
        else {
            panic!("LOCK must parse");
        };
        assert_eq!(lock.tables.len(), 1);
        assert_eq!(lock.tables[0].table, "schema_migrations");
        assert_eq!(lock.mode, LockMode::ShareUpdateExclusive);
    }

    #[test]
    fn lock_defaults_to_access_exclusive_and_takes_a_list() {
        let Ok(Statement::Lock(lock)) = parse_statement("LOCK public.a, b") else {
            panic!("LOCK must parse");
        };
        assert_eq!(lock.mode, LockMode::AccessExclusive);
        assert_eq!(lock.tables.len(), 2);
        assert_eq!(lock.tables[0].schema.as_deref(), Some("public"));
    }

    #[test]
    fn comment_on_table_parses_text_and_null() {
        let Ok(Statement::Comment(c)) =
            parse_statement("COMMENT ON TABLE \"public\".oban_jobs IS '12'")
        else {
            panic!("COMMENT must parse");
        };
        assert_eq!(c.table.schema.as_deref(), Some("public"));
        assert_eq!(c.table.table, "oban_jobs");
        assert_eq!(c.comment.as_deref(), Some("12"));
        let Ok(Statement::Comment(c)) = parse_statement("COMMENT ON TABLE t IS NULL") else {
            panic!("COMMENT IS NULL must parse");
        };
        assert_eq!(c.comment, None);
        assert!(matches!(
            parse_statement("COMMENT ON COLUMN t.c IS 'x'"),
            Err(ParseError::Unsupported(form)) if form.contains("COLUMN")
        ));
    }

    #[test]
    fn oban_enum_ddl_parses() {
        let Ok(Statement::CreateType(t)) = parse_statement(
            "CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', 'suspended' )",
        ) else {
            panic!("CREATE TYPE must parse");
        };
        assert_eq!(t.name.table, "oban_job_state");
        assert_eq!(t.labels, vec!["available", "suspended"]);
        let Ok(Statement::AlterType(a)) = parse_statement(
            "ALTER TYPE \"public\".oban_job_state ADD VALUE IF NOT EXISTS 'cancelled'",
        ) else {
            panic!("ALTER TYPE must parse");
        };
        assert!(a.if_not_exists);
        assert_eq!(a.value, "cancelled");
        assert!(matches!(
            parse_statement("CREATE TYPE c AS (a int)"),
            Err(ParseError::Unsupported(_))
        ));
        assert!(matches!(
            parse_statement("ALTER TYPE t ADD VALUE 'x' BEFORE 'y'"),
            Err(ParseError::Unsupported(_))
        ));
    }

    #[test]
    fn a_column_typed_by_a_user_type_records_the_name() {
        let Ok(Statement::CreateTable(t)) = parse_statement(
            "CREATE TABLE j (id bigint PRIMARY KEY, \"state\" \"public\".oban_job_state NOT NULL)",
        ) else {
            panic!("CREATE TABLE must parse");
        };
        let state = &t.columns[1];
        let user_type = state.user_type.as_ref().expect("user type recorded");
        assert_eq!(user_type.schema.as_deref(), Some("public"));
        assert_eq!(user_type.table, "oban_job_state");
        assert!(state.not_null);
        // A built-in refusal is not reinterpreted as a user type.
        assert!(parse_statement("CREATE TABLE s (id serial PRIMARY KEY)").is_err());
    }

    #[test]
    fn defaults_are_parsed_and_rendered_canonically() {
        let Ok(Statement::CreateTable(t)) = parse_statement(
            "CREATE TABLE j (id bigint PRIMARY KEY, \"state\" text DEFAULT 'available' NOT NULL, \
             e jsonb DEFAULT ARRAY[]::jsonb[] NOT NULL, \
             at timestamp DEFAULT timezone( 'UTC' , now()) NOT NULL, n int DEFAULT 0)",
        ) else {
            panic!("CREATE TABLE with defaults must parse");
        };
        let default = |i: usize| t.columns[i].default.as_deref();
        assert_eq!(default(1), Some("'available'"));
        assert!(t.columns[1].not_null);
        assert_eq!(default(2), Some("ARRAY[]::jsonb[]"));
        assert_eq!(default(3), Some("timezone('UTC', now())"));
        assert_eq!(default(4), Some("0"));
        assert!(
            parse_statement("CREATE TABLE b (id int PRIMARY KEY, n int DEFAULT (1 +)").is_err()
        );
    }

    #[test]
    fn oban_alter_actions_parse() {
        let Ok(Statement::AlterTable(a)) = parse_statement(
            "ALTER TABLE \"public\".\"oban_jobs\" ALTER COLUMN \"priority\" TYPE integer, \
             ALTER COLUMN \"priority\" SET DEFAULT 0, ALTER COLUMN \"priority\" SET NOT NULL",
        ) else {
            panic!("multi-action ALTER must parse");
        };
        assert!(matches!(
            a.operation,
            AlterOperation::AlterColumnType { .. }
        ));
        assert_eq!(a.rest.len(), 2);
        let Ok(Statement::AlterTable(a)) = parse_statement(
            "ALTER TABLE t ADD CONSTRAINT \"non_negative_priority\" CHECK (priority >= 0) NOT VALID",
        ) else {
            panic!("ADD CHECK must parse");
        };
        assert_eq!(
            a.operation,
            AlterOperation::AddCheck {
                name: Some("non_negative_priority".into()),
                sql: "priority >= 0".into(),
                not_valid: true,
            }
        );
        let Ok(Statement::AlterTable(a)) = parse_statement(
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS \"a\" timestamp, DROP CONSTRAINT x, SET UNLOGGED",
        ) else {
            panic!("ADD IF NOT EXISTS / DROP CONSTRAINT / SET UNLOGGED must parse");
        };
        assert!(matches!(
            a.operation,
            AlterOperation::AddColumnIfNotExists(_)
        ));
        assert_eq!(a.rest[1], AlterOperation::SetLogged(false));
    }

    #[test]
    fn lock_nowait_is_refused_by_name() {
        assert!(matches!(
            parse_statement("LOCK TABLE t IN EXCLUSIVE MODE NOWAIT"),
            Err(ParseError::Unsupported(form)) if form.contains("NOWAIT")
        ));
        assert!(matches!(
            parse_statement("LOCK TABLE t IN BOGUS MODE"),
            Err(ParseError::Unexpected { .. })
        ));
    }
}
