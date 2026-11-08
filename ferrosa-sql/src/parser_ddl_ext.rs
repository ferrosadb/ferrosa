//! Module: the DDL and utility statements Oban's stock migrations add to the subset
//! (`specs/todo/postgres-frontend/oban-compat.md`, workstream W3): `LOCK TABLE`, and the
//! statements added after it in this file.
//! Correctness: every accepted form parses to an AST the front end gives real semantics;
//! every form outside the subset is a typed `ParseError` naming it, never dropped.
//! Last revised: 2026-11-08
//!
//! A child module of `parser`, so it shares the private lexer and `Parser`. No recursion.

use super::{ParseError, Parser, Tok};
use crate::ast::{AlterTypeStmt, CommentStmt, CreateTypeStmt, LockMode, LockStmt, Statement};

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

#[cfg(test)]
mod tests {
    use crate::ast::{LockMode, Statement};
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
