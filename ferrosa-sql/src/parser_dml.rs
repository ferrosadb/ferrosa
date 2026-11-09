//! General `UPDATE` / `DELETE` and row-locking clauses (lane W2).
//!
//! A statement the keyed legacy grammar (`Parser::parse_update`,
//! `Parser::parse_delete`) carries completely, with no `RETURNING`, keeps the
//! legacy [`Statement::Update`] / [`Statement::Delete`]: pgbench's statements
//! run exactly as before. Everything else becomes a [`DmlStmt`]. The two agree on
//! what they share (`specs/lean4/Ferrosa/Sql/Dml.lean`, `legacy_where_agrees`,
//! `legacy_set_agrees`), so the choice changes the path a statement takes, not
//! its meaning.
//!
//! A form the general grammar does not implement is refused by name
//! ([`ParseError::UnsupportedDml`], SQLSTATE 0A000), never parsed loosely.

use crate::ast::{
    Cte, DmlAction, DmlStmt, Expr, LockStrength, LockWait, Projection, RowLock, ScalarExpr,
    SelectItem, Statement, TableRef,
};
use crate::parser::{ParseError, Parser, Tok};

impl Parser {
    /// `UPDATE …` at the cursor (after any `WITH`).
    pub(crate) fn parse_update_statement(
        &mut self,
        ctes: Vec<Cte>,
    ) -> Result<Statement, ParseError> {
        if ctes.is_empty() {
            if let Some(stmt) = self.try_legacy(Self::parse_update) {
                return Ok(stmt);
            }
        }
        self.parse_general_update(ctes)
    }

    /// `DELETE …` at the cursor (after any `WITH`).
    pub(crate) fn parse_delete_statement(
        &mut self,
        ctes: Vec<Cte>,
    ) -> Result<Statement, ParseError> {
        if ctes.is_empty() {
            if let Some(stmt) = self.try_legacy(Self::parse_delete) {
                return Ok(stmt);
            }
        }
        self.parse_general_delete(ctes)
    }

    /// Run a legacy DML parser; keep its statement only when it consumed the whole
    /// input and has no `RETURNING` (the legacy executors refuse that). Otherwise
    /// rewind, so the general grammar parses from the same token.
    fn try_legacy(
        &mut self,
        legacy: fn(&mut Self) -> Result<Statement, ParseError>,
    ) -> Option<Statement> {
        let (pos, depth) = (self.pos, self.depth);
        let keep = match legacy(self) {
            Ok(Statement::Update(u)) if u.returning.is_none() => Some(Statement::Update(u)),
            Ok(Statement::Delete(d)) if d.returning.is_none() => Some(Statement::Delete(d)),
            _ => None,
        };
        if keep.is_none() {
            self.pos = pos;
            self.depth = depth;
        }
        keep
    }

    fn parse_general_update(&mut self, ctes: Vec<Cte>) -> Result<Statement, ParseError> {
        self.expect_word("UPDATE")?;
        let target = self.parse_dml_target()?;
        self.expect_word("SET")?;
        let mut sets = vec![self.parse_dml_assignment()?];
        while self.eat(&Tok::Comma) {
            sets.push(self.parse_dml_assignment()?);
        }
        let from = if self.eat(&Tok::From) {
            self.parse_dml_from()?
        } else {
            Vec::new()
        };
        self.finish_dml(ctes, target, DmlAction::Update(sets), from)
    }

    fn parse_general_delete(&mut self, ctes: Vec<Cte>) -> Result<Statement, ParseError> {
        self.expect_word("DELETE")?;
        self.expect(&Tok::From, "FROM")?;
        let target = self.parse_dml_target()?;
        let from = if self.peek_word("USING") {
            self.next();
            self.parse_dml_from()?
        } else {
            Vec::new()
        };
        self.finish_dml(ctes, target, DmlAction::Delete, from)
    }

    /// `[WHERE …] [RETURNING …]` and the end of the statement.
    fn finish_dml(
        &mut self,
        ctes: Vec<Cte>,
        target: TableRef,
        action: DmlAction,
        from: Vec<TableRef>,
    ) -> Result<Statement, ParseError> {
        let filter = self.parse_dml_where()?;
        let returning = self.parse_dml_returning()?;
        self.expect_end()?;
        Ok(Statement::Dml(Box::new(DmlStmt {
            ctes,
            target,
            action,
            from,
            filter,
            returning,
        })))
    }

    /// The written table, with an optional alias. `ONLY` (inheritance) is refused:
    /// there is no inheritance to exclude, and accepting it silently would claim
    /// a meaning that does not exist here.
    fn parse_dml_target(&mut self) -> Result<TableRef, ParseError> {
        if self.peek_word("ONLY") {
            return Err(ParseError::UnsupportedDml("ONLY"));
        }
        self.parse_table_ref()
    }

    /// `col = <expr>`.
    fn parse_dml_assignment(&mut self) -> Result<(String, ScalarExpr), ParseError> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            return Err(ParseError::UnsupportedDml("SET (column list)"));
        }
        let column = self.ident()?;
        self.expect(&Tok::Eq, "=")?;
        if self.peek_word("DEFAULT") {
            return Err(ParseError::UnsupportedDml("SET column = DEFAULT"));
        }
        Ok((column, self.parse_top_scalar_expr()?))
    }

    /// `FROM` / `USING`: a comma list of relations. A `JOIN` inside it is refused.
    fn parse_dml_from(&mut self) -> Result<Vec<TableRef>, ParseError> {
        let mut from = Vec::new();
        loop {
            from.push(self.parse_table_ref()?);
            let joins = matches!(self.peek(), Some(Tok::Join | Tok::Inner))
                || ["LEFT", "RIGHT", "FULL", "CROSS", "NATURAL"]
                    .iter()
                    .any(|w| self.peek_word(w));
            if joins {
                return Err(ParseError::UnsupportedDml(
                    "JOIN in UPDATE … FROM / DELETE … USING",
                ));
            }
            if !self.eat(&Tok::Comma) {
                return Ok(from);
            }
        }
    }

    fn parse_dml_where(&mut self) -> Result<Option<Expr>, ParseError> {
        if !self.eat(&Tok::Where) {
            return Ok(None);
        }
        if self.peek_word("CURRENT") && self.peek_word_at(1, "OF") {
            return Err(ParseError::UnsupportedDml("WHERE CURRENT OF"));
        }
        Ok(Some(self.parse_condition()?))
    }

    /// `RETURNING * | item [, item]*`, each item an expression with an optional alias.
    fn parse_dml_returning(&mut self) -> Result<Option<Projection>, ParseError> {
        if !self.peek_word("RETURNING") {
            return Ok(None);
        }
        self.next();
        if matches!(self.peek(), Some(Tok::Star)) && self.toks.get(self.pos + 1).is_none() {
            self.next();
            return Ok(Some(Projection::Star));
        }
        let mut items: Vec<SelectItem> = vec![self.parse_general_select_item()?];
        while self.eat(&Tok::Comma) {
            items.push(self.parse_general_select_item()?);
        }
        Ok(Some(Projection::Items(items)))
    }

    /// `FOR UPDATE | FOR NO KEY UPDATE | FOR SHARE | FOR KEY SHARE
    /// [OF t [, …]] [NOWAIT | SKIP LOCKED]`. Recorded, takes no locks (D14).
    pub(crate) fn parse_row_lock(&mut self) -> Result<RowLock, ParseError> {
        self.expect_word("FOR")?;
        let strength = self.parse_lock_strength()?;
        let mut of = Vec::new();
        if self.peek_word("OF") {
            self.next();
            loop {
                of.push(self.ident()?);
                if !self.eat(&Tok::Comma) {
                    break;
                }
            }
        }
        let wait = if self.peek_word("NOWAIT") {
            self.next();
            LockWait::NoWait
        } else if self.peek_word("SKIP") {
            self.next();
            self.expect_word("LOCKED")?;
            LockWait::SkipLocked
        } else {
            LockWait::Wait
        };
        Ok(RowLock { strength, of, wait })
    }

    fn parse_lock_strength(&mut self) -> Result<LockStrength, ParseError> {
        if self.peek_word("UPDATE") {
            self.next();
            return Ok(LockStrength::Update);
        }
        if self.peek_word("SHARE") {
            self.next();
            return Ok(LockStrength::Share);
        }
        if self.peek_word("NO") {
            self.next();
            self.expect_word("KEY")?;
            self.expect_word("UPDATE")?;
            return Ok(LockStrength::NoKeyUpdate);
        }
        self.expect_word("KEY")?;
        self.expect_word("SHARE")?;
        Ok(LockStrength::KeyShare)
    }
}
