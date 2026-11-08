//! Module: the DDL and utility statements Oban's stock migrations add to the subset
//! (`specs/todo/postgres-frontend/oban-compat.md`, workstream W3): `LOCK TABLE`, and the
//! statements added after it in this file.
//! Correctness: every accepted form parses to an AST the front end gives real semantics;
//! every form outside the subset is a typed `ParseError` naming it, never dropped.
//! Last revised: 2026-11-08
//!
//! A child module of `parser`, so it shares the private lexer and `Parser`. No recursion.

use super::{ParseError, Parser, Tok};
use crate::ast::{LockMode, LockStmt, Statement};

impl Parser {
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
