//! Hand-written lexer + recursive-descent parser for the M1 SQL subset.

use std::fmt;

use crate::ast::{
    AggArg, ColumnRef, DeleteStmt, Expr, InsertStmt, Join, Operand, OrderItem, Projection,
    Returning, ScalarItem, ScalarValue, SelectItem, SelectStmt, Statement, TableRef, Term,
    UpdateStmt,
};
use crate::exec::{AggFunc, CmpOp, SortDir};
use crate::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Found a token where something else was expected.
    Unexpected {
        expected: &'static str,
        found: String,
    },
    /// Ran out of input mid-statement.
    UnexpectedEnd,
    /// A lexing error (bad character, unterminated string).
    BadToken(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Unexpected { expected, found } => {
                write!(f, "expected {expected}, found `{found}`")
            }
            ParseError::UnexpectedEnd => write!(f, "unexpected end of statement"),
            ParseError::BadToken(t) => write!(f, "bad token: {t}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a single `SELECT` statement of the M1 subset.
pub fn parse(sql: &str) -> Result<SelectStmt, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let toks = lex(trimmed)?;
    let mut p = Parser { toks, pos: 0 };

    p.expect(&Tok::Select, "SELECT")?;
    let distinct = if matches!(p.peek(), Some(Tok::Distinct)) {
        p.next();
        true
    } else {
        false
    };
    let projection = p.parse_projection()?;
    p.expect(&Tok::From, "FROM")?;
    let from = p.parse_table_ref()?;
    let join = if matches!(p.peek(), Some(Tok::Join | Tok::Inner)) {
        Some(p.parse_join()?)
    } else {
        None
    };
    let filter = if matches!(p.peek(), Some(Tok::Where)) {
        p.next();
        Some(p.parse_expr()?)
    } else {
        None
    };
    let group_by = if matches!(p.peek(), Some(Tok::Group)) {
        p.next();
        p.expect(&Tok::By, "BY")?;
        p.parse_column_list()?
    } else {
        Vec::new()
    };
    let having = if matches!(p.peek(), Some(Tok::Having)) {
        p.next();
        Some(p.parse_expr()?)
    } else {
        None
    };
    let order_by = if matches!(p.peek(), Some(Tok::Order)) {
        p.next();
        p.expect(&Tok::By, "BY")?;
        p.parse_order_by()?
    } else {
        Vec::new()
    };
    // LIMIT and OFFSET in either order, both optional.
    let mut limit = None;
    let mut offset = None;
    loop {
        match p.peek() {
            Some(Tok::Limit) if limit.is_none() => {
                p.next();
                limit = Some(p.parse_u64()?);
            }
            Some(Tok::Offset) if offset.is_none() => {
                p.next();
                offset = Some(p.parse_u64()?);
            }
            _ => break,
        }
    }
    if let Some(t) = p.peek() {
        return Err(ParseError::Unexpected {
            expected: "end of statement",
            found: format!("{t:?}"),
        });
    }
    Ok(SelectStmt {
        distinct,
        projection,
        from,
        join,
        filter,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

/// Parse a top-level statement off the Postgres wire: a table `SELECT`, a
/// no-`FROM` expression `SELECT` (`SELECT 1`, `SELECT version()`), transaction
/// control (`BEGIN`/`COMMIT`/`ROLLBACK`), or a session statement (`SET`/`RESET`).
///
/// Transaction-control and session statements are recognized here so the
/// front-end can give them real semantics — they are not silently accepted.
pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let toks = lex(trimmed)?;
    let mut p = Parser { toks, pos: 0 };
    match p.peek() {
        Some(Tok::Select) => {
            if p.is_expr_select() {
                let items = p.parse_scalar_select()?;
                p.expect_end()?;
                Ok(Statement::SelectExprs(items))
            } else {
                // Table query: reuse the full SELECT grammar on the same SQL.
                Ok(Statement::Select(Box::new(parse(trimmed)?)))
            }
        }
        Some(Tok::Ident(w)) => match w.to_ascii_uppercase().as_str() {
            // Transaction control: `BEGIN [TRANSACTION|WORK]`, `START TRANSACTION`,
            // `COMMIT|END`, `ROLLBACK|ABORT`. Trailing modifier words are ignored.
            "BEGIN" | "START" => Ok(Statement::Begin),
            "COMMIT" | "END" => Ok(Statement::Commit),
            "ROLLBACK" | "ABORT" => Ok(Statement::Rollback),
            "SET" => p.parse_set(),
            "RESET" => p.parse_reset(),
            "INSERT" => p.parse_insert(),
            "UPDATE" => p.parse_update(),
            "DELETE" => p.parse_delete(),
            other => Err(ParseError::Unexpected {
                expected: "a statement",
                found: other.to_string(),
            }),
        },
        Some(t) => Err(ParseError::Unexpected {
            expected: "a statement",
            found: format!("{t:?}"),
        }),
        None => Err(ParseError::UnexpectedEnd),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Select,
    From,
    Join,
    Inner,
    On,
    Where,
    As,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Limit,
    Offset,
    And,
    Or,
    Not,
    Distinct,
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    /// A `$N` parameter placeholder, carrying the 1-based index `N`.
    Param(usize),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn lex(sql: &str) -> Result<Vec<Tok>, ParseError> {
    let chars: Vec<char> = sql.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '*' => {
                toks.push(Tok::Star);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '.' => {
                toks.push(Tok::Dot);
                i += 1;
            }
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            '=' => {
                toks.push(Tok::Eq);
                i += 1;
            }
            '<' => {
                if chars.get(i + 1) == Some(&'=') {
                    toks.push(Tok::Le);
                    i += 2;
                } else if chars.get(i + 1) == Some(&'>') {
                    toks.push(Tok::Ne);
                    i += 2;
                } else {
                    toks.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if chars.get(i + 1) == Some(&'=') {
                    toks.push(Tok::Ge);
                    i += 2;
                } else {
                    toks.push(Tok::Gt);
                    i += 1;
                }
            }
            '!' if chars.get(i + 1) == Some(&'=') => {
                toks.push(Tok::Ne);
                i += 2;
            }
            '$' if chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                // A `$N` bound-parameter placeholder: `$` followed by one or
                // more digits, yielding the 1-based parameter index N.
                i += 1; // consume '$'
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let digits: String = chars[start..i].iter().collect();
                let n = digits
                    .parse::<usize>()
                    .map_err(|_| ParseError::BadToken(format!("${digits}")))?;
                toks.push(Tok::Param(n));
            }
            '\'' => {
                let mut s = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err(ParseError::BadToken("unterminated string".into())),
                        Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                            s.push('\''); // doubled quote escape
                            i += 2;
                        }
                        Some('\'') => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            s.push(ch);
                            i += 1;
                        }
                    }
                }
                toks.push(Tok::Str(s));
            }
            c if c.is_ascii_digit()
                || (c == '-' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit())) =>
            {
                let start = i;
                if chars[i] == '-' {
                    i += 1;
                }
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // A single '.' makes this a float literal (e.g. `1.5`, `10.`,
                // `-0.25`). Consume the dot and any trailing fractional digits.
                let is_float = i < chars.len() && chars[i] == '.';
                if is_float {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                let text: String = chars[start..i].iter().collect();
                if is_float {
                    let f = text
                        .parse::<f64>()
                        .map_err(|_| ParseError::BadToken(text))?;
                    toks.push(Tok::Float(f));
                } else {
                    let n = text
                        .parse::<i64>()
                        .map_err(|_| ParseError::BadToken(text))?;
                    toks.push(Tok::Int(n));
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                toks.push(match word.to_ascii_uppercase().as_str() {
                    "SELECT" => Tok::Select,
                    "FROM" => Tok::From,
                    "JOIN" => Tok::Join,
                    "INNER" => Tok::Inner,
                    "ON" => Tok::On,
                    "WHERE" => Tok::Where,
                    "AS" => Tok::As,
                    "GROUP" => Tok::Group,
                    "BY" => Tok::By,
                    "HAVING" => Tok::Having,
                    "ORDER" => Tok::Order,
                    "ASC" => Tok::Asc,
                    "DESC" => Tok::Desc,
                    "LIMIT" => Tok::Limit,
                    "OFFSET" => Tok::Offset,
                    "AND" => Tok::And,
                    "OR" => Tok::Or,
                    "NOT" => Tok::Not,
                    "DISTINCT" => Tok::Distinct,
                    _ => Tok::Ident(word),
                });
            }
            other => return Err(ParseError::BadToken(other.to_string())),
        }
    }
    Ok(toks)
}

/// Whether an identifier names an aggregate function (used to decide whether a
/// `FUNC(` is an aggregate call).
fn is_aggregate_name(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "MIN" | "MAX" | "AVG"
    )
}

/// Map an aggregate identifier to its [`AggFunc`].
fn aggregate_func(word: &str) -> Option<AggFunc> {
    match word.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        "AVG" => Some(AggFunc::Avg),
        _ => None,
    }
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, want: &Tok, name: &'static str) -> Result<(), ParseError> {
        match self.next() {
            Some(ref t) if t == want => Ok(()),
            Some(t) => Err(ParseError::Unexpected {
                expected: name,
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    fn ident(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            Some(t) => Err(ParseError::Unexpected {
                expected: "identifier",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    fn parse_projection(&mut self) -> Result<Projection, ParseError> {
        // A single bare `*` is `SELECT *`.
        if matches!(self.peek(), Some(Tok::Star))
            && matches!(self.toks.get(self.pos + 1), Some(Tok::From))
        {
            self.next();
            return Ok(Projection::Star);
        }
        let mut items = vec![self.parse_select_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_select_item()?);
        }
        Ok(Projection::Items(items))
    }

    /// After a leading `SELECT`, decide whether this is a no-`FROM` expression
    /// select (`SELECT 1`, `SELECT version()`, `SELECT $1`, `SELECT TRUE`) vs a
    /// table select. `self.pos` is at the `SELECT` token.
    fn is_expr_select(&self) -> bool {
        match self.toks.get(self.pos + 1) {
            Some(Tok::Int(_) | Tok::Float(_) | Tok::Str(_) | Tok::Param(_)) => true,
            Some(Tok::Ident(w)) => {
                let up = w.to_ascii_uppercase();
                matches!(up.as_str(), "TRUE" | "FALSE" | "NULL")
                    || (!is_aggregate_name(w)
                        && matches!(self.toks.get(self.pos + 2), Some(Tok::LParen)))
            }
            _ => false,
        }
    }

    /// Parse `SELECT <scalar> [, <scalar>]*` with no `FROM`. Assumes `self.pos`
    /// is at the `SELECT` token.
    fn parse_scalar_select(&mut self) -> Result<Vec<ScalarItem>, ParseError> {
        self.expect(&Tok::Select, "SELECT")?;
        let mut items = vec![self.parse_scalar_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_scalar_item()?);
        }
        Ok(items)
    }

    /// One scalar select item: a `$N` param, a zero-arg function call, or a
    /// literal — with an optional `AS`/bare alias.
    fn parse_scalar_item(&mut self) -> Result<ScalarItem, ParseError> {
        let value = if let Some(Tok::Param(n)) = self.peek() {
            let n = *n;
            self.next();
            ScalarValue::Param(n)
        } else if matches!(self.peek(), Some(Tok::Ident(_)))
            && matches!(self.toks.get(self.pos + 1), Some(Tok::LParen))
        {
            let Some(Tok::Ident(w)) = self.next() else {
                unreachable!("peeked an Ident above");
            };
            self.expect(&Tok::LParen, "(")?;
            self.expect(&Tok::RParen, ")")?;
            ScalarValue::Func(w.to_ascii_uppercase())
        } else {
            ScalarValue::Literal(self.parse_value()?)
        };
        let alias = self.parse_optional_alias()?;
        Ok(ScalarItem { value, alias })
    }

    /// An optional output alias: `AS name` or a bare `name`.
    fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if matches!(self.peek(), Some(Tok::As | Tok::Ident(_))) {
            if matches!(self.peek(), Some(Tok::As)) {
                self.next();
            }
            Ok(Some(self.ident()?))
        } else {
            Ok(None)
        }
    }

    /// `SET <name> [=|TO] <value>`. Assumes `self.pos` is at the `SET` ident.
    fn parse_set(&mut self) -> Result<Statement, ParseError> {
        self.next(); // SET
        let name = self.ident()?;
        match self.peek() {
            Some(Tok::Eq) => {
                self.next();
            }
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("TO") => {
                self.next();
            }
            _ => {}
        }
        let value = match self.next() {
            Some(Tok::Str(s)) => s,
            Some(Tok::Int(n)) => n.to_string(),
            Some(Tok::Ident(w)) => w,
            Some(t) => {
                return Err(ParseError::Unexpected {
                    expected: "SET value",
                    found: format!("{t:?}"),
                })
            }
            None => return Err(ParseError::UnexpectedEnd),
        };
        Ok(Statement::Set { name, value })
    }

    /// `RESET <name>` (`RESET ALL` → name `ALL`). Assumes `self.pos` is at `RESET`.
    fn parse_reset(&mut self) -> Result<Statement, ParseError> {
        self.next(); // RESET
        let name = self.ident()?;
        Ok(Statement::Reset { name })
    }

    /// Error unless all tokens are consumed (statement fully parsed).
    fn expect_end(&self) -> Result<(), ParseError> {
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(ParseError::Unexpected {
                expected: "end of statement",
                found: format!("{t:?}"),
            }),
        }
    }

    /// Consume an identifier that case-insensitively equals `kw` (for the
    /// keyword-like idents the lexer leaves as `Ident`: `INTO`, `VALUES`).
    fn expect_ident_kw(&mut self, kw: &'static str) -> Result<(), ParseError> {
        match self.next() {
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case(kw) => Ok(()),
            Some(t) => Err(ParseError::Unexpected {
                expected: kw,
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    /// A scalar value in a `VALUES` list: a `$N` parameter or a literal.
    fn parse_scalar_value(&mut self) -> Result<ScalarValue, ParseError> {
        if let Some(Tok::Param(n)) = self.peek() {
            let n = *n;
            self.next();
            Ok(ScalarValue::Param(n))
        } else {
            Ok(ScalarValue::Literal(self.parse_value()?))
        }
    }

    /// `INSERT INTO [schema.]table (col, ...) VALUES (val, ...)`. Assumes
    /// `self.pos` is at the `INSERT` ident. Single-row; values are literals or
    /// `$N` parameters, one per named column.
    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.next(); // INSERT
        self.expect_ident_kw("INTO")?;
        let table = self.parse_table_ref()?;

        self.expect(&Tok::LParen, "(")?;
        let mut columns = vec![self.ident()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            columns.push(self.ident()?);
        }
        self.expect(&Tok::RParen, ")")?;

        self.expect_ident_kw("VALUES")?;
        self.expect(&Tok::LParen, "(")?;
        let mut values = vec![self.parse_scalar_value()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            values.push(self.parse_scalar_value()?);
        }
        self.expect(&Tok::RParen, ")")?;

        // ON CONFLICT is recognized to fail loud (out of scope), never silently
        // ignored — a client that relies on upsert semantics must learn it.
        self.reject_on_conflict()?;
        let returning = self.parse_returning()?;
        self.expect_end()?;

        if columns.len() != values.len() {
            return Err(ParseError::Unexpected {
                expected: "matching column and value counts",
                found: format!("{} columns, {} values", columns.len(), values.len()),
            });
        }
        Ok(Statement::Insert(Box::new(InsertStmt {
            table,
            columns,
            values,
            returning,
        })))
    }

    /// `UPDATE [schema.]table SET col = val, ... WHERE col = val [AND ...]`.
    /// Assumes `self.pos` is at the `UPDATE` ident. `WHERE` is equality-only
    /// (the key columns identify the row, Cassandra-style upsert).
    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.next(); // UPDATE
        let table = self.parse_qualified_table()?;

        self.expect_ident_kw("SET")?;
        let mut assignments = vec![self.parse_assignment()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            assignments.push(self.parse_assignment()?);
        }

        self.expect(&Tok::Where, "WHERE")?;
        let mut where_eq = vec![self.parse_assignment()?];
        while matches!(self.peek(), Some(Tok::And)) {
            self.next();
            where_eq.push(self.parse_assignment()?);
        }
        let returning = self.parse_returning()?;
        self.expect_end()?;

        Ok(Statement::Update(Box::new(UpdateStmt {
            table,
            assignments,
            where_eq,
            returning,
        })))
    }

    /// `DELETE FROM [schema.]table WHERE col = val [AND ...]`. Assumes `self.pos`
    /// is at the `DELETE` ident. `WHERE` is equality-only on key columns.
    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.next(); // DELETE
        self.expect(&Tok::From, "FROM")?;
        let table = self.parse_qualified_table()?;

        self.expect(&Tok::Where, "WHERE")?;
        let mut where_eq = vec![self.parse_assignment()?];
        while matches!(self.peek(), Some(Tok::And)) {
            self.next();
            where_eq.push(self.parse_assignment()?);
        }
        let returning = self.parse_returning()?;
        self.expect_end()?;

        Ok(Statement::Delete(Box::new(DeleteStmt {
            table,
            where_eq,
            returning,
        })))
    }

    /// Parse an optional `RETURNING * | col, col, ...` clause. Returns `None`
    /// when the next token is not the `RETURNING` keyword (it is lexed as a bare
    /// identifier). `RETURNING *` yields [`Returning::Star`].
    fn parse_returning(&mut self) -> Result<Option<Returning>, ParseError> {
        match self.peek() {
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("RETURNING") => {
                self.next(); // RETURNING
            }
            _ => return Ok(None),
        }
        if matches!(self.peek(), Some(Tok::Star)) {
            self.next();
            return Ok(Some(Returning::Star));
        }
        let mut cols = vec![self.ident()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            cols.push(self.ident()?);
        }
        Ok(Some(Returning::Columns(cols)))
    }

    /// Fail loud on an `ON CONFLICT` clause: upsert/`DO` semantics are out of
    /// scope, and silently dropping the clause would change the statement's
    /// meaning. A bare `ON` not followed by `CONFLICT` is left for the caller's
    /// `expect_end`/`parse_returning` to reject in context.
    fn reject_on_conflict(&mut self) -> Result<(), ParseError> {
        if matches!(self.peek(), Some(Tok::On))
            && matches!(self.toks.get(self.pos + 1), Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("CONFLICT"))
        {
            return Err(ParseError::Unexpected {
                expected: "no ON CONFLICT (not yet supported)",
                found: "ON CONFLICT".to_string(),
            });
        }
        Ok(())
    }

    /// Parse `[schema.]table` with NO trailing alias — for DML targets, where a
    /// following ident (`SET`, `WHERE`) is a keyword, not an alias.
    fn parse_qualified_table(&mut self) -> Result<TableRef, ParseError> {
        let first = self.ident()?;
        let (schema, table) = if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            (Some(first), self.ident()?)
        } else {
            (None, first)
        };
        Ok(TableRef {
            schema,
            table,
            alias: None,
        })
    }

    /// A `col = value` pair (used by `UPDATE`'s SET list and equality WHERE).
    fn parse_assignment(&mut self) -> Result<(String, ScalarValue), ParseError> {
        let col = self.ident()?;
        self.expect(&Tok::Eq, "=")?;
        let value = self.parse_scalar_value()?;
        Ok((col, value))
    }

    /// A single SELECT-list entry: an aggregate `FUNC(...)` if an identifier
    /// whose uppercase name is a known aggregate is immediately followed by
    /// `(`, otherwise a plain column reference.
    fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        if let Some(Tok::Ident(word)) = self.peek() {
            if is_aggregate_name(word) && matches!(self.toks.get(self.pos + 1), Some(Tok::LParen)) {
                let raw = word.clone();
                self.next(); // function name
                self.next(); // (
                let func = aggregate_func(&raw).ok_or(ParseError::Unexpected {
                    expected: "supported aggregate",
                    found: raw,
                })?;
                let arg = if matches!(self.peek(), Some(Tok::Star)) {
                    self.next();
                    AggArg::Star
                } else {
                    AggArg::Column(self.parse_column_ref()?)
                };
                self.expect(&Tok::RParen, ")")?;
                return Ok(SelectItem::Aggregate { func, arg });
            }
        }
        Ok(SelectItem::Column(self.parse_column_ref()?))
    }

    fn parse_column_list(&mut self) -> Result<Vec<ColumnRef>, ParseError> {
        let mut cols = vec![self.parse_column_ref()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            cols.push(self.parse_column_ref()?);
        }
        Ok(cols)
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderItem>, ParseError> {
        let mut items = vec![self.parse_order_item()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            items.push(self.parse_order_item()?);
        }
        Ok(items)
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, ParseError> {
        // An integer is an output-ordinal (aggregate mode); carry it as the
        // column name so the planner can interpret it.
        let column = if let Some(Tok::Int(n)) = self.peek() {
            let n = *n;
            self.next();
            ColumnRef {
                qualifier: None,
                name: n.to_string(),
            }
        } else {
            self.parse_column_ref()?
        };
        let dir = match self.peek() {
            Some(Tok::Asc) => {
                self.next();
                SortDir::Asc
            }
            Some(Tok::Desc) => {
                self.next();
                SortDir::Desc
            }
            _ => SortDir::Asc,
        };
        Ok(OrderItem { column, dir })
    }

    fn parse_u64(&mut self) -> Result<u64, ParseError> {
        match self.next() {
            Some(Tok::Int(n)) if n >= 0 => Ok(n as u64),
            Some(t) => Err(ParseError::Unexpected {
                expected: "non-negative integer",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    fn parse_column_ref(&mut self) -> Result<ColumnRef, ParseError> {
        let first = self.ident()?;
        if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            let name = self.ident()?;
            Ok(ColumnRef {
                qualifier: Some(first),
                name,
            })
        } else {
            Ok(ColumnRef {
                qualifier: None,
                name: first,
            })
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let first = self.ident()?;
        let (schema, table) = if matches!(self.peek(), Some(Tok::Dot)) {
            self.next();
            (Some(first), self.ident()?)
        } else {
            (None, first)
        };
        let alias = if matches!(self.peek(), Some(Tok::As)) {
            self.next();
            Some(self.ident()?)
        } else if matches!(self.peek(), Some(Tok::Ident(_))) {
            Some(self.ident()?)
        } else {
            None
        };
        Ok(TableRef {
            schema,
            table,
            alias,
        })
    }

    fn parse_join(&mut self) -> Result<Join, ParseError> {
        if matches!(self.peek(), Some(Tok::Inner)) {
            self.next();
        }
        self.expect(&Tok::Join, "JOIN")?;
        let table = self.parse_table_ref()?;
        self.expect(&Tok::On, "ON")?;
        let left = self.parse_column_ref()?;
        self.expect(&Tok::Eq, "=")?;
        let right = self.parse_column_ref()?;
        Ok(Join { table, left, right })
    }

    /// Boolean expression grammar with precedence
    /// OR (lowest) < AND < NOT < primary.
    ///
    /// ```text
    /// or_expr   := and_expr ( OR and_expr )*
    /// and_expr  := not_expr ( AND not_expr )*
    /// not_expr  := NOT not_expr | primary
    /// primary   := '(' or_expr ')' | comparison
    /// ```
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Tok::Or)) {
            self.next();
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Some(Tok::And)) {
            self.next();
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            let inner = self.parse_not()?;
            Ok(Expr::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.next();
            let inner = self.parse_or()?;
            self.expect(&Tok::RParen, ")")?;
            Ok(inner)
        } else {
            self.parse_comparison()
        }
    }

    /// `comparison := operand op value`, where operand is a column ref or an
    /// aggregate call (`COUNT(*)` / `FUNC(col)`).
    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_operand()?;
        let op = self.parse_cmp_op()?;
        let value = self.parse_term()?;
        Ok(Expr::Compare { left, op, value })
    }

    /// The right-hand side of a comparison: a `$N` parameter placeholder
    /// ([`Term::Param`]) or a literal value ([`Term::Literal`]).
    fn parse_term(&mut self) -> Result<Term, ParseError> {
        if let Some(Tok::Param(n)) = self.peek() {
            let n = *n;
            self.next();
            return Ok(Term::Param(n));
        }
        Ok(Term::Literal(self.parse_value()?))
    }

    /// An operand in a comparison: an aggregate `FUNC(...)` if an identifier
    /// whose uppercase name is a known aggregate is immediately followed by `(`,
    /// otherwise a plain column reference. (Mirrors `parse_select_item`.)
    fn parse_operand(&mut self) -> Result<Operand, ParseError> {
        if let Some(Tok::Ident(word)) = self.peek() {
            if is_aggregate_name(word) && matches!(self.toks.get(self.pos + 1), Some(Tok::LParen)) {
                let raw = word.clone();
                self.next(); // function name
                self.next(); // (
                let func = aggregate_func(&raw).ok_or(ParseError::Unexpected {
                    expected: "supported aggregate",
                    found: raw,
                })?;
                let arg = if matches!(self.peek(), Some(Tok::Star)) {
                    self.next();
                    AggArg::Star
                } else {
                    AggArg::Column(self.parse_column_ref()?)
                };
                self.expect(&Tok::RParen, ")")?;
                return Ok(Operand::Aggregate { func, arg });
            }
        }
        Ok(Operand::Column(self.parse_column_ref()?))
    }

    fn parse_cmp_op(&mut self) -> Result<CmpOp, ParseError> {
        match self.next() {
            Some(Tok::Eq) => Ok(CmpOp::Eq),
            Some(Tok::Ne) => Ok(CmpOp::Ne),
            Some(Tok::Lt) => Ok(CmpOp::Lt),
            Some(Tok::Le) => Ok(CmpOp::Le),
            Some(Tok::Gt) => Ok(CmpOp::Gt),
            Some(Tok::Ge) => Ok(CmpOp::Ge),
            Some(t) => Err(ParseError::Unexpected {
                expected: "comparison operator",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }

    fn parse_value(&mut self) -> Result<Value, ParseError> {
        // Typed literal: a type-keyword identifier immediately followed by a
        // string literal — `TIMESTAMP '2024-01-15 10:30:00'`, `DATE '2024-01-15'`,
        // `TIME '10:30:00'`, `INET '10.0.0.1'`, `NUMERIC '123.45'` (also `DECIMAL`).
        // This is how a typed value reaches a WHERE comparison against a
        // timestamp/date/time/inet/numeric column. We parse the string body into
        // the engine's value repr here so `sql_cmp` compares like-with-like.
        if let Some(Tok::Ident(w)) = self.peek() {
            if let Some(kind) = typed_literal_kind(w) {
                if let Some(Tok::Str(_)) = self.toks.get(self.pos + 1) {
                    self.next(); // type keyword
                    let Some(Tok::Str(body)) = self.next() else {
                        unreachable!("peeked a Str above");
                    };
                    return parse_typed_literal(kind, &body);
                }
            }
        }
        match self.next() {
            Some(Tok::Int(n)) => Ok(Value::Int(n)),
            Some(Tok::Float(f)) => Ok(Value::float(f)),
            Some(Tok::Str(s)) => Ok(Value::Text(s)),
            Some(Tok::Ident(w)) => match w.to_ascii_uppercase().as_str() {
                "TRUE" => Ok(Value::Bool(true)),
                "FALSE" => Ok(Value::Bool(false)),
                "NULL" => Ok(Value::Null),
                _ => Err(ParseError::Unexpected {
                    expected: "literal",
                    found: w,
                }),
            },
            Some(t) => Err(ParseError::Unexpected {
                expected: "literal",
                found: format!("{t:?}"),
            }),
            None => Err(ParseError::UnexpectedEnd),
        }
    }
}

/// The kind of typed literal a leading keyword introduces (`TIMESTAMP '...'` etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedLiteral {
    Timestamp,
    Date,
    Time,
    Inet,
    Numeric,
}

/// Map an identifier to the typed-literal kind it introduces, if any
/// (case-insensitive). `DECIMAL` is an alias for `NUMERIC`.
fn typed_literal_kind(word: &str) -> Option<TypedLiteral> {
    match word.to_ascii_uppercase().as_str() {
        "TIMESTAMP" => Some(TypedLiteral::Timestamp),
        "DATE" => Some(TypedLiteral::Date),
        "TIME" => Some(TypedLiteral::Time),
        "INET" => Some(TypedLiteral::Inet),
        "NUMERIC" | "DECIMAL" => Some(TypedLiteral::Numeric),
        _ => None,
    }
}

/// Parse the string body of a typed literal into the engine's [`Value`] repr.
/// A malformed body is a parse error (fail loud — never silently a `Text`).
fn parse_typed_literal(kind: TypedLiteral, body: &str) -> Result<Value, ParseError> {
    let bad = || ParseError::Unexpected {
        expected: "valid typed literal body",
        found: body.to_string(),
    };
    match kind {
        TypedLiteral::Timestamp => {
            let s = body.trim();
            let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .map_err(|_| bad())?;
            Ok(Value::Timestamp(naive.and_utc().timestamp_micros()))
        }
        TypedLiteral::Date => {
            let date =
                chrono::NaiveDate::parse_from_str(body.trim(), "%Y-%m-%d").map_err(|_| bad())?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch valid");
            let days = (date - epoch).num_days();
            Ok(Value::Date(i32::try_from(days).map_err(|_| bad())?))
        }
        TypedLiteral::Time => {
            let s = body.trim();
            let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                .map_err(|_| bad())?;
            let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("midnight valid");
            let micros = (t - midnight).num_microseconds().ok_or_else(bad)?;
            Ok(Value::Time(micros))
        }
        TypedLiteral::Inet => body
            .trim()
            .parse::<std::net::IpAddr>()
            .map(Value::Inet)
            .map_err(|_| bad()),
        TypedLiteral::Numeric => parse_numeric_literal(body.trim()),
    }
}

/// Parse a plain decimal string (`[+-]ddd[.ddd]`, no exponent) into a normalized
/// [`Value::Numeric`]. A malformed body is a parse error.
fn parse_numeric_literal(s: &str) -> Result<Value, ParseError> {
    use num_bigint::BigInt;
    let bad = || ParseError::Unexpected {
        expected: "decimal numeric literal",
        found: s.to_string(),
    };
    if s.is_empty() {
        return Err(bad());
    }
    let (sign, rest) = match s.strip_prefix('-') {
        Some(r) => (-1i8, r),
        None => (1i8, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
        || (int_part.is_empty() && frac_part.is_empty())
    {
        return Err(bad());
    }
    let digits = format!("{int_part}{frac_part}");
    let magnitude = digits.parse::<BigInt>().map_err(|_| bad())?;
    let unscaled = if sign < 0 { -magnitude } else { magnitude };
    let scale = i32::try_from(frac_part.len()).map_err(|_| bad())?;
    Ok(Value::numeric(unscaled, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_select_star_from_table() {
        let stmt = parse("SELECT * FROM users").unwrap();
        assert_eq!(stmt.projection, Projection::Star);
        assert_eq!(stmt.from.table, "users");
        assert!(stmt.from.schema.is_none() && stmt.join.is_none() && stmt.filter.is_none());
    }

    #[test]
    fn parses_schema_qualified_table_and_alias() {
        let stmt = parse("SELECT id FROM ks.users u").unwrap();
        assert_eq!(stmt.from.schema.as_deref(), Some("ks"));
        assert_eq!(stmt.from.table, "users");
        assert_eq!(stmt.from.alias.as_deref(), Some("u"));
        match stmt.projection {
            Projection::Items(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    SelectItem::Column(c) => {
                        assert_eq!(c.name, "id");
                        assert!(c.qualifier.is_none());
                    }
                    other => panic!("expected column, got {other:?}"),
                }
            }
            other => panic!("expected items, got {other:?}"),
        }
    }

    #[test]
    fn parses_qualified_projection_columns() {
        let stmt = parse("SELECT u.name, o.oid FROM users u").unwrap();
        match stmt.projection {
            Projection::Items(items) => {
                assert_eq!(
                    items[0],
                    SelectItem::Column(ColumnRef {
                        qualifier: Some("u".into()),
                        name: "name".into()
                    })
                );
                assert_eq!(
                    items[1],
                    SelectItem::Column(ColumnRef {
                        qualifier: Some("o".into()),
                        name: "oid".into()
                    })
                );
            }
            other => panic!("expected items, got {other:?}"),
        }
    }

    #[test]
    fn parses_inner_join_on_equality() {
        let stmt = parse("SELECT u.name FROM users u JOIN orders o ON u.id = o.uid").unwrap();
        let join = stmt.join.expect("join");
        assert_eq!(join.table.table, "orders");
        assert_eq!(join.table.alias.as_deref(), Some("o"));
        assert_eq!(
            join.left,
            ColumnRef {
                qualifier: Some("u".into()),
                name: "id".into()
            }
        );
        assert_eq!(
            join.right,
            ColumnRef {
                qualifier: Some("o".into()),
                name: "uid".into()
            }
        );
    }

    /// Unwrap a single `Compare` from a filter `Expr` for assertion ergonomics.
    fn as_compare(expr: &Expr) -> (&Operand, CmpOp, &Term) {
        match expr {
            Expr::Compare { left, op, value } => (left, *op, value),
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    /// Unwrap the literal value of a comparison's term, panicking on a param.
    fn compare_literal(expr: &Expr) -> &Value {
        match as_compare(expr).2 {
            Term::Literal(v) => v,
            other => panic!("expected literal term, got {other:?}"),
        }
    }

    fn compare_column(expr: &Expr) -> &ColumnRef {
        match as_compare(expr).0 {
            Operand::Column(c) => c,
            other => panic!("expected column operand, got {other:?}"),
        }
    }

    #[test]
    fn parses_where_with_int_and_string_literals() {
        let int_stmt = parse("SELECT * FROM users u WHERE u.id = 1").unwrap();
        let f = int_stmt.filter.unwrap();
        assert_eq!(
            *compare_column(&f),
            ColumnRef {
                qualifier: Some("u".into()),
                name: "id".into()
            }
        );
        let (_, op, _) = as_compare(&f);
        assert_eq!(op, CmpOp::Eq);
        assert_eq!(*compare_literal(&f), Value::Int(1));

        let str_stmt = parse("SELECT * FROM users WHERE name = 'alice'").unwrap();
        let f = str_stmt.filter.unwrap();
        assert_eq!(*compare_literal(&f), Value::Text("alice".into()));
    }

    #[test]
    fn parses_typed_literals_in_where() {
        use num_bigint::BigInt;
        // TIMESTAMP literal.
        let ts =
            parse("SELECT id FROM events WHERE at >= TIMESTAMP '2024-01-15 10:30:00'").unwrap();
        match compare_literal(&ts.filter.unwrap()) {
            Value::Timestamp(_) => {}
            other => panic!("expected Timestamp literal, got {other:?}"),
        }
        // DATE literal resolves to the right day count (1970-01-02 == day 1).
        let d = parse("SELECT id FROM events WHERE on_day < DATE '1970-01-02'").unwrap();
        assert_eq!(*compare_literal(&d.filter.unwrap()), Value::Date(1));
        // TIME literal (1 second past midnight).
        let t = parse("SELECT id FROM events WHERE at_time > TIME '00:00:01'").unwrap();
        assert_eq!(*compare_literal(&t.filter.unwrap()), Value::Time(1_000_000));
        // INET literal.
        let inet = parse("SELECT id FROM events WHERE src = INET '10.0.0.1'").unwrap();
        assert_eq!(
            *compare_literal(&inet.filter.unwrap()),
            Value::Inet("10.0.0.1".parse().unwrap())
        );
        // NUMERIC literal (and DECIMAL alias).
        let n = parse("SELECT id FROM events WHERE amt > NUMERIC '1.5'").unwrap();
        assert_eq!(
            *compare_literal(&n.filter.unwrap()),
            Value::numeric(BigInt::from(15), 1)
        );
        let dec = parse("SELECT id FROM events WHERE amt > DECIMAL '2'").unwrap();
        assert_eq!(
            *compare_literal(&dec.filter.unwrap()),
            Value::numeric(BigInt::from(2), 0)
        );
    }

    #[test]
    fn malformed_typed_literal_is_a_parse_error() {
        assert!(parse("SELECT id FROM events WHERE at = TIMESTAMP 'not-a-time'").is_err());
        assert!(parse("SELECT id FROM events WHERE amt = NUMERIC '1.2.3'").is_err());
    }

    #[test]
    fn parses_parameter_placeholder_in_where() {
        let stmt = parse("SELECT * FROM users u WHERE u.id = $1").unwrap();
        let f = stmt.filter.unwrap();
        let (operand, op, term) = as_compare(&f);
        match operand {
            Operand::Column(c) => assert_eq!(c.name, "id"),
            other => panic!("expected column operand, got {other:?}"),
        }
        assert_eq!(op, CmpOp::Eq);
        assert_eq!(*term, Term::Param(1));
    }

    #[test]
    fn parses_multi_digit_and_distinct_parameter_indices() {
        let stmt = parse("SELECT * FROM t WHERE a = $1 AND b = $12").unwrap();
        match stmt.filter.unwrap() {
            Expr::And(l, r) => {
                assert_eq!(*as_compare(&l).2, Term::Param(1));
                assert_eq!(*as_compare(&r).2, Term::Param(12));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn parameter_in_having_term() {
        let stmt =
            parse("SELECT region, COUNT(*) FROM t GROUP BY region HAVING COUNT(*) > $1").unwrap();
        let having = stmt.having.expect("having");
        assert_eq!(*as_compare(&having).2, Term::Param(1));
    }

    #[test]
    fn parses_full_m1_query() {
        let stmt =
            parse("SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1")
                .unwrap();
        assert!(matches!(stmt.projection, Projection::Items(_)));
        assert!(stmt.join.is_some());
        assert!(stmt.filter.is_some());
    }

    #[test]
    fn parses_aggregates_in_select_list() {
        let stmt = parse("SELECT region, COUNT(*), SUM(amount) FROM sales").unwrap();
        match stmt.projection {
            Projection::Items(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(
                    items[0],
                    SelectItem::Column(ColumnRef {
                        qualifier: None,
                        name: "region".into()
                    })
                );
                assert_eq!(
                    items[1],
                    SelectItem::Aggregate {
                        func: AggFunc::Count,
                        arg: AggArg::Star
                    }
                );
                assert_eq!(
                    items[2],
                    SelectItem::Aggregate {
                        func: AggFunc::Sum,
                        arg: AggArg::Column(ColumnRef {
                            qualifier: None,
                            name: "amount".into()
                        })
                    }
                );
            }
            other => panic!("expected items, got {other:?}"),
        }
    }

    #[test]
    fn column_named_like_aggregate_not_followed_by_paren_stays_column() {
        let stmt = parse("SELECT count FROM t").unwrap();
        match stmt.projection {
            Projection::Items(items) => assert_eq!(
                items[0],
                SelectItem::Column(ColumnRef {
                    qualifier: None,
                    name: "count".into()
                })
            ),
            other => panic!("expected items, got {other:?}"),
        }
    }

    #[test]
    fn parses_avg_aggregate() {
        let stmt = parse("SELECT AVG(amount) FROM t").unwrap();
        match stmt.projection {
            Projection::Items(items) => assert_eq!(
                items[0],
                SelectItem::Aggregate {
                    func: AggFunc::Avg,
                    arg: AggArg::Column(ColumnRef {
                        qualifier: None,
                        name: "amount".into()
                    })
                }
            ),
            other => panic!("expected items, got {other:?}"),
        }
    }

    #[test]
    fn parses_float_literal_in_where() {
        let stmt = parse("SELECT * FROM t WHERE x = 1.5").unwrap();
        let f = stmt.filter.unwrap();
        assert_eq!(*compare_literal(&f), Value::float(1.5));
    }

    #[test]
    fn parses_float_literal_variants() {
        // Negative fractional.
        let a = parse("SELECT * FROM t WHERE x = -0.25").unwrap();
        assert_eq!(*compare_literal(&a.filter.unwrap()), Value::float(-0.25));
        // Trailing dot with no fractional digits.
        let b = parse("SELECT * FROM t WHERE x = 10.").unwrap();
        assert_eq!(*compare_literal(&b.filter.unwrap()), Value::float(10.0));
        // A plain integer (no dot) stays an Int.
        let c = parse("SELECT * FROM t WHERE x = 10").unwrap();
        assert_eq!(*compare_literal(&c.filter.unwrap()), Value::Int(10));
    }

    #[test]
    fn float_in_comparison_predicate() {
        let stmt = parse("SELECT * FROM t WHERE score > 3.5").unwrap();
        let f = stmt.filter.unwrap();
        let (_, op, _) = as_compare(&f);
        assert_eq!(op, CmpOp::Gt);
        assert_eq!(*compare_literal(&f), Value::float(3.5));
    }

    #[test]
    fn parses_where_and() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 AND b = 2").unwrap();
        match stmt.filter.unwrap() {
            Expr::And(l, r) => {
                assert_eq!(compare_column(&l).name, "a");
                assert_eq!(compare_column(&r).name, "b");
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn parses_where_or() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 OR b = 2").unwrap();
        assert!(matches!(stmt.filter.unwrap(), Expr::Or(_, _)));
    }

    #[test]
    fn parses_where_not() {
        let stmt = parse("SELECT * FROM t WHERE NOT a = 1").unwrap();
        match stmt.filter.unwrap() {
            Expr::Not(inner) => assert_eq!(compare_column(&inner).name, "a"),
            other => panic!("expected Not, got {other:?}"),
        }
    }

    #[test]
    fn and_binds_tighter_than_or() {
        // `a = 1 OR b = 2 AND c = 3` parses as `a OR (b AND c)`.
        let stmt = parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        match stmt.filter.unwrap() {
            Expr::Or(l, r) => {
                assert_eq!(compare_column(&l).name, "a");
                assert!(matches!(*r, Expr::And(_, _)));
            }
            other => panic!("expected Or at top, got {other:?}"),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        // `(a = 1 OR b = 2) AND c = 3` parses as `(a OR b) AND c`.
        let stmt = parse("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3").unwrap();
        match stmt.filter.unwrap() {
            Expr::And(l, r) => {
                assert!(matches!(*l, Expr::Or(_, _)));
                assert_eq!(compare_column(&r).name, "c");
            }
            other => panic!("expected And at top, got {other:?}"),
        }
    }

    #[test]
    fn parses_distinct() {
        let stmt = parse("SELECT DISTINCT region FROM t").unwrap();
        assert!(stmt.distinct);
        let plain = parse("SELECT region FROM t").unwrap();
        assert!(!plain.distinct);
    }

    #[test]
    fn parses_having_with_aggregate_operand() {
        let stmt =
            parse("SELECT region, COUNT(*) FROM t GROUP BY region HAVING COUNT(*) > 1").unwrap();
        let having = stmt.having.expect("having");
        match having {
            Expr::Compare { left, op, value } => {
                assert_eq!(
                    left,
                    Operand::Aggregate {
                        func: AggFunc::Count,
                        arg: AggArg::Star
                    }
                );
                assert_eq!(op, CmpOp::Gt);
                assert_eq!(value, Term::Literal(Value::Int(1)));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn having_after_group_by_before_order_by() {
        let stmt = parse(
            "SELECT region, SUM(amount) FROM t GROUP BY region \
             HAVING SUM(amount) > 100 ORDER BY 2 DESC",
        )
        .unwrap();
        assert!(stmt.having.is_some());
        assert_eq!(stmt.order_by.len(), 1);
    }

    #[test]
    fn parses_group_by() {
        let stmt = parse("SELECT region, COUNT(*) FROM sales GROUP BY region").unwrap();
        assert_eq!(stmt.group_by.len(), 1);
        assert_eq!(stmt.group_by[0].name, "region");
    }

    #[test]
    fn parses_order_by_default_and_explicit_dirs() {
        let stmt = parse("SELECT id FROM t ORDER BY id").unwrap();
        assert_eq!(stmt.order_by.len(), 1);
        assert_eq!(stmt.order_by[0].dir, SortDir::Asc);

        let stmt = parse("SELECT id FROM t ORDER BY a ASC, b DESC").unwrap();
        assert_eq!(stmt.order_by.len(), 2);
        assert_eq!(stmt.order_by[0].dir, SortDir::Asc);
        assert_eq!(stmt.order_by[1].dir, SortDir::Desc);
    }

    #[test]
    fn parses_order_by_ordinal() {
        let stmt = parse("SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY 2 DESC").unwrap();
        assert_eq!(stmt.order_by[0].column.name, "2");
        assert_eq!(stmt.order_by[0].dir, SortDir::Desc);
    }

    #[test]
    fn parses_limit_and_offset_either_order() {
        let a = parse("SELECT id FROM t LIMIT 5 OFFSET 2").unwrap();
        assert_eq!(a.limit, Some(5));
        assert_eq!(a.offset, Some(2));

        let b = parse("SELECT id FROM t OFFSET 3 LIMIT 1").unwrap();
        assert_eq!(b.limit, Some(1));
        assert_eq!(b.offset, Some(3));

        let c = parse("SELECT id FROM t LIMIT 4").unwrap();
        assert_eq!(c.limit, Some(4));
        assert_eq!(c.offset, None);
    }

    #[test]
    fn parses_combined_full_query() {
        let stmt = parse(
            "SELECT region, COUNT(*) FROM sales WHERE amount > 0 \
             GROUP BY region ORDER BY 2 DESC LIMIT 10 OFFSET 1",
        )
        .unwrap();
        assert!(matches!(stmt.projection, Projection::Items(_)));
        assert!(stmt.filter.is_some());
        assert_eq!(stmt.group_by.len(), 1);
        assert_eq!(stmt.order_by.len(), 1);
        assert_eq!(stmt.limit, Some(10));
        assert_eq!(stmt.offset, Some(1));
    }

    #[test]
    fn keywords_are_case_insensitive() {
        let stmt = parse("select * from users").unwrap();
        assert_eq!(stmt.from.table, "users");
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("DELETE FROM users").is_err());
        assert!(parse("SELECT").is_err());
        assert!(parse("SELECT * FROM").is_err());
    }

    // ---- parse_statement: the Postgres-wire entry point ----

    #[test]
    fn parse_statement_routes_table_select() {
        assert!(matches!(
            parse_statement("SELECT id FROM users"),
            Ok(Statement::Select(_))
        ));
        assert!(matches!(
            parse_statement("SELECT * FROM users WHERE id = 1"),
            Ok(Statement::Select(_))
        ));
        assert!(matches!(
            parse_statement("SELECT COUNT(*) FROM users"),
            Ok(Statement::Select(_))
        ));
    }

    #[test]
    fn parse_statement_transaction_control() {
        assert_eq!(parse_statement("BEGIN").unwrap(), Statement::Begin);
        assert_eq!(
            parse_statement("begin transaction").unwrap(),
            Statement::Begin
        );
        assert_eq!(
            parse_statement("START TRANSACTION").unwrap(),
            Statement::Begin
        );
        assert_eq!(parse_statement("COMMIT").unwrap(), Statement::Commit);
        assert_eq!(parse_statement("END").unwrap(), Statement::Commit);
        assert_eq!(parse_statement("ROLLBACK").unwrap(), Statement::Rollback);
        assert_eq!(parse_statement("ABORT").unwrap(), Statement::Rollback);
    }

    #[test]
    fn parse_statement_literal_select() {
        assert_eq!(
            parse_statement("SELECT 1").unwrap(),
            Statement::SelectExprs(vec![ScalarItem {
                value: ScalarValue::Literal(Value::Int(1)),
                alias: None,
            }])
        );
        assert_eq!(
            parse_statement("SELECT 'hello'").unwrap(),
            Statement::SelectExprs(vec![ScalarItem {
                value: ScalarValue::Literal(Value::Text("hello".into())),
                alias: None,
            }])
        );
    }

    #[test]
    fn parse_statement_function_and_alias_select() {
        assert_eq!(
            parse_statement("SELECT version()").unwrap(),
            Statement::SelectExprs(vec![ScalarItem {
                value: ScalarValue::Func("VERSION".into()),
                alias: None,
            }])
        );
        assert_eq!(
            parse_statement("SELECT current_database() AS db, 1 AS one").unwrap(),
            Statement::SelectExprs(vec![
                ScalarItem {
                    value: ScalarValue::Func("CURRENT_DATABASE".into()),
                    alias: Some("db".into()),
                },
                ScalarItem {
                    value: ScalarValue::Literal(Value::Int(1)),
                    alias: Some("one".into()),
                },
            ])
        );
    }

    #[test]
    fn parse_statement_param_select() {
        assert_eq!(
            parse_statement("SELECT $1").unwrap(),
            Statement::SelectExprs(vec![ScalarItem {
                value: ScalarValue::Param(1),
                alias: None,
            }])
        );
    }

    #[test]
    fn parse_statement_insert() {
        let stmt = parse_statement("INSERT INTO demo.t (id, v) VALUES (1, 'x')").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table.schema.as_deref(), Some("demo"));
                assert_eq!(ins.table.table, "t");
                assert_eq!(ins.columns, vec!["id".to_string(), "v".to_string()]);
                assert_eq!(
                    ins.values,
                    vec![
                        ScalarValue::Literal(Value::Int(1)),
                        ScalarValue::Literal(Value::Text("x".into())),
                    ]
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_with_params_and_unqualified_table() {
        let stmt = parse_statement("INSERT INTO t (a, b) VALUES ($1, $2)").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table.schema, None);
                assert_eq!(ins.table.table, "t");
                assert_eq!(
                    ins.values,
                    vec![ScalarValue::Param(1), ScalarValue::Param(2)]
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_rejects_arity_mismatch() {
        assert!(parse_statement("INSERT INTO t (a, b) VALUES (1)").is_err());
    }

    #[test]
    fn parse_statement_update() {
        let stmt =
            parse_statement("UPDATE demo.t SET v = 'x', n = 3 WHERE id = 1 AND ck = 2").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.table.schema.as_deref(), Some("demo"));
                assert_eq!(u.table.table, "t");
                assert_eq!(
                    u.assignments,
                    vec![
                        (
                            "v".to_string(),
                            ScalarValue::Literal(Value::Text("x".into()))
                        ),
                        ("n".to_string(), ScalarValue::Literal(Value::Int(3))),
                    ]
                );
                assert_eq!(
                    u.where_eq,
                    vec![
                        ("id".to_string(), ScalarValue::Literal(Value::Int(1))),
                        ("ck".to_string(), ScalarValue::Literal(Value::Int(2))),
                    ]
                );
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_update_with_params() {
        let stmt = parse_statement("UPDATE t SET v = $1 WHERE id = $2").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(
                    u.assignments,
                    vec![("v".to_string(), ScalarValue::Param(1))]
                );
                assert_eq!(u.where_eq, vec![("id".to_string(), ScalarValue::Param(2))]);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_delete() {
        let stmt = parse_statement("DELETE FROM demo.t WHERE id = 1 AND ck = 2").unwrap();
        match stmt {
            Statement::Delete(d) => {
                assert_eq!(d.table.schema.as_deref(), Some("demo"));
                assert_eq!(d.table.table, "t");
                assert_eq!(
                    d.where_eq,
                    vec![
                        ("id".to_string(), ScalarValue::Literal(Value::Int(1))),
                        ("ck".to_string(), ScalarValue::Literal(Value::Int(2))),
                    ]
                );
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_returning_columns() {
        let stmt =
            parse_statement("INSERT INTO t (id, v) VALUES ($1, $2) RETURNING id, v").unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(
                    ins.values,
                    vec![ScalarValue::Param(1), ScalarValue::Param(2)]
                );
                assert_eq!(
                    ins.returning,
                    Some(Returning::Columns(vec!["id".to_string(), "v".to_string()]))
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_returning_star() {
        let stmt = parse_statement("INSERT INTO t (id) VALUES (1) RETURNING *").unwrap();
        match stmt {
            Statement::Insert(ins) => assert_eq!(ins.returning, Some(Returning::Star)),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_no_returning_is_none() {
        let stmt = parse_statement("INSERT INTO t (id) VALUES (1)").unwrap();
        match stmt {
            Statement::Insert(ins) => assert_eq!(ins.returning, None),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_update_delete_returning() {
        let upd = parse_statement("UPDATE t SET v = $1 WHERE id = $2 RETURNING id").unwrap();
        match upd {
            Statement::Update(u) => {
                assert_eq!(
                    u.assignments,
                    vec![("v".to_string(), ScalarValue::Param(1))]
                );
                assert_eq!(u.where_eq, vec![("id".to_string(), ScalarValue::Param(2))]);
                assert_eq!(
                    u.returning,
                    Some(Returning::Columns(vec!["id".to_string()]))
                );
            }
            other => panic!("expected Update, got {other:?}"),
        }
        let del = parse_statement("DELETE FROM t WHERE id = $1 RETURNING *").unwrap();
        match del {
            Statement::Delete(d) => {
                assert_eq!(d.where_eq, vec![("id".to_string(), ScalarValue::Param(1))]);
                assert_eq!(d.returning, Some(Returning::Star));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn parse_statement_insert_on_conflict_is_clear_error() {
        let err = parse_statement("INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING")
            .expect_err("ON CONFLICT must be rejected, not silently ignored");
        // The error message names ON CONFLICT so the boundary is explicit.
        assert!(
            err.to_string().contains("ON CONFLICT"),
            "error should mention ON CONFLICT, got: {err}"
        );
    }

    #[test]
    fn parse_statement_set_and_reset() {
        assert_eq!(
            parse_statement("SET client_encoding = 'UTF8'").unwrap(),
            Statement::Set {
                name: "client_encoding".into(),
                value: "UTF8".into(),
            }
        );
        assert_eq!(
            parse_statement("SET search_path TO public").unwrap(),
            Statement::Set {
                name: "search_path".into(),
                value: "public".into(),
            }
        );
        assert_eq!(
            parse_statement("RESET ALL").unwrap(),
            Statement::Reset { name: "ALL".into() }
        );
    }
}
