//! General scalar expressions: the grammar behind [`ScalarExpr`], and the
//! lowering that keeps legacy shapes on their fast paths.
//!
//! Precedence, loosest first (PostgreSQL's): `OR`, `AND`, `NOT`, comparison /
//! `IS [NOT] NULL` / `[NOT] IN` / `[NOT] BETWEEN` / `op ANY(...)`, `||`, `+ -`,
//! `* / %`, unary `-`, `::` cast and `AT TIME ZONE`, then primaries.
//!
//! **Lowering.** A condition the legacy AST can represent (`column <op> literal |
//! $n [::cast]`, combined with AND/OR/NOT) is lowered back to [`Expr::Compare`]
//! and friends, so point reads, Accord read sets and every other fast path that
//! pattern-matches `Compare` see exactly what they saw before. Only conditions
//! outside those shapes become [`Expr::Scalar`]. `Ferrosa/Sql/Expr.lean`'s
//! `legacy_compare_agrees` is why this is sound: both evaluate the same.

use crate::ast::{
    AggArg, BinOp, CastTarget, ColumnRef, Expr, Operand, ScalarExpr, SelectItem, Term, TypeName,
    UnaryOp,
};
use crate::exec::CmpOp;
use crate::parser::{aggregate_func, is_aggregate_name, ParseError, Parser, Tok};
use crate::types::Value;

impl Parser {
    /// Parse a scalar expression nested inside another (a parenthesis, an
    /// argument, a CASE arm): one level deeper.
    pub(crate) fn parse_scalar_expr(&mut self) -> Result<ScalarExpr, ParseError> {
        self.nested(Self::scalar_or)
    }

    /// Parse a clause-level expression (`WHERE`, `ON`, a select item). It starts at
    /// the statement's own level, so the full [`crate::MAX_EXPR_DEPTH`] is
    /// available to its nesting.
    pub(crate) fn parse_top_scalar_expr(&mut self) -> Result<ScalarExpr, ParseError> {
        self.scalar_or()
    }

    /// Parse a condition and lower it to the legacy shapes where it fits.
    pub(crate) fn parse_condition(&mut self) -> Result<Expr, ParseError> {
        Ok(lower_condition(self.parse_top_scalar_expr()?))
    }

    /// A select-list item: legacy `Column`/`Aggregate` where it is one, else `Expr`.
    pub(crate) fn parse_general_select_item(&mut self) -> Result<SelectItem, ParseError> {
        let expr = self.parse_top_scalar_expr()?;
        let alias = self.parse_optional_alias()?;
        Ok(match expr {
            ScalarExpr::Column(col) => SelectItem::Column { col, alias },
            ScalarExpr::Aggregate { func, arg } => SelectItem::Aggregate { func, arg, alias },
            expr => SelectItem::Expr { expr, alias },
        })
    }

    fn scalar_or(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut left = self.scalar_and()?;
        let base = self.depth;
        while matches!(self.peek(), Some(Tok::Or)) {
            self.deepen()?;
            self.next();
            let right = self.scalar_and()?;
            left = binary(BinOp::Or, left, right);
        }
        self.depth = base;
        Ok(left)
    }

    fn scalar_and(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut left = self.scalar_not()?;
        let base = self.depth;
        while matches!(self.peek(), Some(Tok::And)) {
            self.deepen()?;
            self.next();
            let right = self.scalar_not()?;
            left = binary(BinOp::And, left, right);
        }
        self.depth = base;
        Ok(left)
    }

    fn scalar_not(&mut self) -> Result<ScalarExpr, ParseError> {
        if matches!(self.peek(), Some(Tok::Not)) && !self.peek_word_at(1, "EXISTS") {
            self.next();
            let expr = self.nested(Self::scalar_not)?;
            return Ok(ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        self.scalar_predicate()
    }

    /// Comparison, `IS [NOT] NULL`, `[NOT] IN (...)`, `[NOT] BETWEEN`, `op ANY(...)`.
    fn scalar_predicate(&mut self) -> Result<ScalarExpr, ParseError> {
        let left = self.scalar_concat()?;
        if let Some(op) = self.peek_cmp_op() {
            self.next();
            if self.peek_word("ANY") || self.peek_word("SOME") {
                self.next();
                self.expect(&Tok::LParen, "(")?;
                let right = self.parse_scalar_expr()?;
                self.expect(&Tok::RParen, ")")?;
                return Ok(ScalarExpr::Any {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                });
            }
            let right = self.scalar_concat()?;
            return Ok(binary(BinOp::Cmp(op), left, right));
        }
        if self.peek_word("IS") {
            self.next();
            let negated = self.eat(&Tok::Not);
            self.expect_word("NULL")?;
            return Ok(ScalarExpr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }
        let negated = matches!(self.peek(), Some(Tok::Not))
            && (self.peek_word_at(1, "IN") || self.peek_word_at(1, "BETWEEN"));
        if negated {
            self.next();
        }
        if self.peek_word("IN") {
            self.next();
            self.expect(&Tok::LParen, "(")?;
            let list = self.scalar_list(&Tok::RParen)?;
            return Ok(ScalarExpr::InList {
                expr: Box::new(left),
                list,
                negated,
            });
        }
        if self.peek_word("BETWEEN") {
            self.next();
            let low = self.scalar_concat()?;
            self.expect(&Tok::And, "AND")?;
            let high = self.scalar_concat()?;
            return Ok(ScalarExpr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            });
        }
        Ok(left)
    }

    fn scalar_concat(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut left = self.scalar_additive()?;
        let base = self.depth;
        while matches!(self.peek(), Some(Tok::Concat)) {
            self.deepen()?;
            self.next();
            let right = self.scalar_additive()?;
            left = binary(BinOp::Concat, left, right);
        }
        self.depth = base;
        Ok(left)
    }

    fn scalar_additive(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut left = self.scalar_multiplicative()?;
        let base = self.depth;
        loop {
            let op = match self.peek() {
                Some(Tok::Plus) => BinOp::Add,
                Some(Tok::Minus) => BinOp::Sub,
                // `a -1`: the lexer folds a minus before a digit into the
                // literal; after an operand it is a subtraction.
                Some(Tok::Int(n)) if *n < 0 => {
                    let n = *n;
                    self.deepen()?;
                    self.next();
                    left = binary(BinOp::Sub, left, ScalarExpr::Literal(Value::Int(-n)));
                    continue;
                }
                _ => {
                    self.depth = base;
                    return Ok(left);
                }
            };
            self.deepen()?;
            self.next();
            let right = self.scalar_multiplicative()?;
            left = binary(op, left, right);
        }
    }

    fn scalar_multiplicative(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut left = self.scalar_unary()?;
        let base = self.depth;
        loop {
            let op = match self.peek() {
                Some(Tok::Star) => BinOp::Mul,
                Some(Tok::Slash) => BinOp::Div,
                Some(Tok::Percent) => BinOp::Mod,
                _ => {
                    self.depth = base;
                    return Ok(left);
                }
            };
            self.deepen()?;
            self.next();
            let right = self.scalar_unary()?;
            left = binary(op, left, right);
        }
    }

    fn scalar_unary(&mut self) -> Result<ScalarExpr, ParseError> {
        if matches!(self.peek(), Some(Tok::Minus)) {
            self.next();
            let expr = self.nested(Self::scalar_unary)?;
            return Ok(ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        self.scalar_postfix()
    }

    /// `::type` casts and `AT TIME ZONE zone`, left-associative.
    fn scalar_postfix(&mut self) -> Result<ScalarExpr, ParseError> {
        let mut expr = self.scalar_primary()?;
        loop {
            if matches!(self.peek(), Some(Tok::Cast)) {
                self.next();
                let ty = self.parse_type_name()?;
                expr = ScalarExpr::Cast {
                    expr: Box::new(expr),
                    ty,
                };
            } else if self.peek_word("AT") && self.peek_word_at(1, "TIME") {
                self.next();
                self.next();
                self.expect_word("ZONE")?;
                let zone = self.scalar_primary()?;
                expr = ScalarExpr::AtTimeZone {
                    expr: Box::new(expr),
                    zone: Box::new(zone),
                };
            } else {
                return Ok(expr);
            }
        }
    }

    fn scalar_primary(&mut self) -> Result<ScalarExpr, ParseError> {
        match self.peek() {
            Some(Tok::Param(n)) => {
                let n = *n;
                self.next();
                Ok(ScalarExpr::Param(n))
            }
            Some(Tok::LParen) => self.scalar_parenthesised(),
            Some(Tok::Not) if self.peek_word_at(1, "EXISTS") => {
                self.next();
                self.next();
                Ok(ScalarExpr::Exists {
                    query: Box::new(self.parenthesised_select()?),
                    negated: true,
                })
            }
            Some(Tok::Int(_) | Tok::Float(_) | Tok::Str(_)) => {
                Ok(ScalarExpr::Literal(self.parse_value()?))
            }
            Some(Tok::Ident(_) | Tok::QuotedIdent(_)) => self.scalar_word(),
            Some(t) => Err(ParseError::Unexpected {
                expected: "expression",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    /// `( expr )` or `( SELECT ... )`.
    fn scalar_parenthesised(&mut self) -> Result<ScalarExpr, ParseError> {
        if matches!(self.toks.get(self.pos + 1), Some(Tok::Select)) {
            return Ok(ScalarExpr::Subquery(Box::new(self.parenthesised_select()?)));
        }
        self.next();
        let inner = self.parse_scalar_expr()?;
        self.expect(&Tok::RParen, ")")?;
        Ok(inner)
    }

    pub(crate) fn parenthesised_select(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        self.expect(&Tok::LParen, "(")?;
        let stmt = self.nested(Self::parse_select_stmt)?;
        self.expect(&Tok::RParen, ")")?;
        Ok(stmt)
    }

    /// A primary that starts with a word: keyword forms, a typed literal, a
    /// function or aggregate call, or a (possibly qualified) column.
    fn scalar_word(&mut self) -> Result<ScalarExpr, ParseError> {
        if let Some(Tok::Ident(word)) = self.peek() {
            let upper = word.to_ascii_uppercase();
            match upper.as_str() {
                "TRUE" | "FALSE" | "NULL" => return Ok(ScalarExpr::Literal(self.parse_value()?)),
                "CASE" => return self.scalar_case(),
                "EXISTS" if matches!(self.toks.get(self.pos + 1), Some(Tok::LParen)) => {
                    self.next();
                    return Ok(ScalarExpr::Exists {
                        query: Box::new(self.parenthesised_select()?),
                        negated: false,
                    });
                }
                "ARRAY" => return self.scalar_array(),
                "CAST" if matches!(self.toks.get(self.pos + 1), Some(Tok::LParen)) => {
                    return self.scalar_cast_call()
                }
                _ => {}
            }
            if crate::parser::typed_literal_kind(word).is_some()
                && matches!(self.toks.get(self.pos + 1), Some(Tok::Str(_)))
            {
                return Ok(ScalarExpr::Literal(self.parse_value()?));
            }
        }
        let first = self.ident()?;
        let qualified = if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            Some(self.ident()?)
        } else {
            None
        };
        if matches!(self.peek(), Some(Tok::LParen)) {
            let name = match &qualified {
                Some(second) => format!(
                    "{}.{}",
                    first.to_ascii_lowercase(),
                    second.to_ascii_lowercase()
                ),
                None => first.to_ascii_lowercase(),
            };
            return self.scalar_call(name);
        }
        Ok(ScalarExpr::Column(match qualified {
            Some(name) => ColumnRef {
                qualifier: Some(first),
                name,
            },
            None => ColumnRef {
                qualifier: None,
                name: first,
            },
        }))
    }

    /// `name(args)` after the name; aggregates become [`ScalarExpr::Aggregate`].
    fn scalar_call(&mut self, name: String) -> Result<ScalarExpr, ParseError> {
        self.expect(&Tok::LParen, "(")?;
        if !name.contains('.') && is_aggregate_name(&name.to_ascii_uppercase()) {
            let func = aggregate_func(&name.to_ascii_uppercase())
                .ok_or_else(|| ParseError::UnsupportedAggregate(name.clone()))?;
            let arg = if self.eat(&Tok::Star) {
                AggArg::Star
            } else {
                AggArg::Column(self.parse_column_ref()?)
            };
            self.expect(&Tok::RParen, ")")?;
            return Ok(ScalarExpr::Aggregate { func, arg });
        }
        let args = self.scalar_list(&Tok::RParen)?;
        Ok(ScalarExpr::Func { name, args })
    }

    /// Comma-separated expressions up to and including `close`.
    fn scalar_list(&mut self, close: &Tok) -> Result<Vec<ScalarExpr>, ParseError> {
        let mut items = Vec::new();
        if self.eat(close) {
            return Ok(items);
        }
        loop {
            items.push(self.parse_scalar_expr()?);
            if self.eat(&Tok::Comma) {
                continue;
            }
            self.expect(close, "closing bracket")?;
            return Ok(items);
        }
    }

    fn scalar_case(&mut self) -> Result<ScalarExpr, ParseError> {
        self.next(); // CASE
        let operand = if self.peek_word("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_scalar_expr()?))
        };
        let mut whens = Vec::new();
        while self.peek_word("WHEN") {
            self.next();
            let when = self.parse_scalar_expr()?;
            self.expect_word("THEN")?;
            let then = self.parse_scalar_expr()?;
            whens.push((when, then));
        }
        if whens.is_empty() {
            return Err(ParseError::Unexpected {
                expected: "WHEN",
                found: format!("{:?}", self.peek()),
            });
        }
        let otherwise = if self.peek_word("ELSE") {
            self.next();
            Some(Box::new(self.parse_scalar_expr()?))
        } else {
            None
        };
        self.expect_word("END")?;
        Ok(ScalarExpr::Case {
            operand,
            whens,
            otherwise,
        })
    }

    /// `ARRAY[a, b]` or `ARRAY(SELECT ...)`.
    fn scalar_array(&mut self) -> Result<ScalarExpr, ParseError> {
        self.next(); // ARRAY
        match self.peek() {
            Some(Tok::LBracket) => {
                self.next();
                Ok(ScalarExpr::Array(self.scalar_list(&Tok::RBracket)?))
            }
            Some(Tok::LParen) => Ok(ScalarExpr::ArraySubquery(Box::new(
                self.parenthesised_select()?,
            ))),
            other => Err(ParseError::Unexpected {
                expected: "[ or ( after ARRAY",
                found: format!("{other:?}"),
            }),
        }
    }

    /// `CAST(expr AS type)`.
    fn scalar_cast_call(&mut self) -> Result<ScalarExpr, ParseError> {
        self.next(); // CAST
        self.expect(&Tok::LParen, "(")?;
        let expr = self.parse_scalar_expr()?;
        self.expect(&Tok::As, "AS")?;
        let ty = self.parse_type_name()?;
        self.expect(&Tok::RParen, ")")?;
        Ok(ScalarExpr::Cast {
            expr: Box::new(expr),
            ty,
        })
    }

    /// A type name: `[schema.]name` (two-word `double precision`, `timestamp
    /// with[out] time zone`, `character varying`), an optional `(n[, m])`
    /// modifier (accepted and recorded nowhere: PostgreSQL ignores it for casts
    /// to unbounded types, and the binder refuses the bounded ones it cannot
    /// honour), and an optional `[]`.
    pub(crate) fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let first = self.ident()?;
        let (schema, mut name) = if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            (Some(first), self.ident()?)
        } else {
            (None, first)
        };
        name = name.to_ascii_lowercase();
        if name == "double" && self.peek_word("PRECISION") {
            self.next();
            name = "double precision".into();
        } else if name == "character" && self.peek_word("VARYING") {
            self.next();
            name = "varchar".into();
        } else if matches!(name.as_str(), "timestamp" | "time")
            && (self.peek_word("WITH") || self.peek_word("WITHOUT"))
        {
            let with = self.peek_word("WITH");
            self.next();
            self.expect_word("TIME")?;
            self.expect_word("ZONE")?;
            if with {
                name = format!("{name}tz");
            }
        }
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.next();
            while !matches!(self.peek(), Some(Tok::RParen) | None) {
                self.next();
            }
            self.expect(&Tok::RParen, ")")?;
        }
        let array = if matches!(self.peek(), Some(Tok::LBracket)) {
            self.next();
            self.expect(&Tok::RBracket, "]")?;
            true
        } else {
            false
        };
        Ok(TypeName {
            schema,
            name,
            array,
        })
    }

    // -- small token helpers ------------------------------------------------------

    pub(crate) fn peek_word(&self, word: &str) -> bool {
        self.peek_word_at(0, word)
    }

    pub(crate) fn peek_word_at(&self, offset: usize, word: &str) -> bool {
        matches!(self.toks.get(self.pos + offset), Some(Tok::Ident(w)) if w.eq_ignore_ascii_case(word))
    }

    pub(crate) fn expect_word(&mut self, word: &'static str) -> Result<(), ParseError> {
        if self.peek_word(word) {
            self.next();
            Ok(())
        } else {
            Err(ParseError::Unexpected {
                expected: word,
                found: format!("{:?}", self.peek()),
            })
        }
    }

    pub(crate) fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == Some(tok) {
            self.next();
            true
        } else {
            false
        }
    }

    fn peek_cmp_op(&self) -> Option<CmpOp> {
        match self.peek()? {
            Tok::Eq => Some(CmpOp::Eq),
            Tok::Ne => Some(CmpOp::Ne),
            Tok::Lt => Some(CmpOp::Lt),
            Tok::Le => Some(CmpOp::Le),
            Tok::Gt => Some(CmpOp::Gt),
            Tok::Ge => Some(CmpOp::Ge),
            _ => None,
        }
    }
}

fn binary(op: BinOp, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// -- lowering -----------------------------------------------------------------------

/// Lower a condition to the legacy [`Expr`] shapes where it fits; anything else is
/// [`Expr::Scalar`]. Moves the expression; nothing is copied.
pub(crate) fn lower_condition(expr: ScalarExpr) -> Expr {
    match expr {
        ScalarExpr::Binary {
            op: BinOp::And,
            left,
            right,
        } => Expr::And(
            Box::new(lower_condition(*left)),
            Box::new(lower_condition(*right)),
        ),
        ScalarExpr::Binary {
            op: BinOp::Or,
            left,
            right,
        } => Expr::Or(
            Box::new(lower_condition(*left)),
            Box::new(lower_condition(*right)),
        ),
        ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Expr::Not(Box::new(lower_condition(*expr))),
        ScalarExpr::Binary {
            op: BinOp::Cmp(op),
            left,
            right,
        } => lower_compare(op, *left, *right),
        other => Expr::Scalar(Box::new(other)),
    }
}

fn lower_compare(op: CmpOp, left: ScalarExpr, right: ScalarExpr) -> Expr {
    let operand = match left {
        ScalarExpr::Column(col) => Ok(Operand::Column(col)),
        ScalarExpr::Aggregate { func, arg } => Ok(Operand::Aggregate { func, arg }),
        other => Err(other),
    };
    match (operand, legacy_term(right)) {
        (Ok(left), Ok(value)) => Expr::Compare { left, op, value },
        (left, right) => {
            let left = match left {
                Ok(Operand::Column(col)) => ScalarExpr::Column(col),
                Ok(Operand::Aggregate { func, arg }) => ScalarExpr::Aggregate { func, arg },
                Err(expr) => expr,
            };
            let right = match right {
                Ok(term) => term_to_scalar(term),
                Err(expr) => expr,
            };
            Expr::Scalar(Box::new(binary(BinOp::Cmp(op), left, right)))
        }
    }
}

/// A right-hand side the legacy [`Term`] can carry: a literal, a `$n`, or a
/// chain of `::regclass` casts of those. Gives the expression back otherwise.
fn legacy_term(expr: ScalarExpr) -> Result<Term, ScalarExpr> {
    match expr {
        ScalarExpr::Literal(v) => Ok(Term::Literal(v)),
        ScalarExpr::Param(n) => Ok(Term::Param(n)),
        ScalarExpr::Cast { expr, ty } if is_regclass(&ty) => match legacy_term(*expr) {
            Ok(term) => Ok(Term::Cast {
                value: Box::new(term),
                ty: CastTarget::Regclass,
            }),
            Err(inner) => Err(ScalarExpr::Cast {
                expr: Box::new(inner),
                ty,
            }),
        },
        other => Err(other),
    }
}

/// The inverse of [`legacy_term`]: rebuilds the expression a term was lowered from,
/// for a comparison whose other side did not fit the legacy shape.
fn term_to_scalar(term: Term) -> ScalarExpr {
    match term {
        Term::Literal(v) => ScalarExpr::Literal(v),
        Term::Param(n) => ScalarExpr::Param(n),
        Term::Cast { value, ty } => ScalarExpr::Cast {
            expr: Box::new(term_to_scalar(*value)),
            ty: TypeName {
                schema: None,
                name: ty.name().to_string(),
                array: false,
            },
        },
    }
}

fn is_regclass(ty: &TypeName) -> bool {
    !ty.array
        && ty.name == "regclass"
        && ty
            .schema
            .as_deref()
            .is_none_or(|s| s.eq_ignore_ascii_case("pg_catalog"))
}
