//! Hand-written lexer + recursive-descent parser for the M1 SQL subset.

use std::fmt;

use crate::ast::{ColumnRef, Filter, Join, Projection, SelectStmt, TableRef};
use crate::exec::CmpOp;
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
        Some(p.parse_filter()?)
    } else {
        None
    };
    if let Some(t) = p.peek() {
        return Err(ParseError::Unexpected {
            expected: "end of statement",
            found: format!("{t:?}"),
        });
    }
    Ok(SelectStmt {
        projection,
        from,
        join,
        filter,
    })
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
    Ident(String),
    Int(i64),
    Str(String),
    Star,
    Comma,
    Dot,
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
                let text: String = chars[start..i].iter().collect();
                let n = text
                    .parse::<i64>()
                    .map_err(|_| ParseError::BadToken(text))?;
                toks.push(Tok::Int(n));
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
                    _ => Tok::Ident(word),
                });
            }
            other => return Err(ParseError::BadToken(other.to_string())),
        }
    }
    Ok(toks)
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
        if matches!(self.peek(), Some(Tok::Star)) {
            self.next();
            return Ok(Projection::Star);
        }
        let mut cols = vec![self.parse_column_ref()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            cols.push(self.parse_column_ref()?);
        }
        Ok(Projection::Columns(cols))
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

    fn parse_filter(&mut self) -> Result<Filter, ParseError> {
        let column = self.parse_column_ref()?;
        let op = self.parse_cmp_op()?;
        let value = self.parse_value()?;
        Ok(Filter { column, op, value })
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
        match self.next() {
            Some(Tok::Int(n)) => Ok(Value::Int(n)),
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
            Projection::Columns(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].name, "id");
                assert!(cols[0].qualifier.is_none());
            }
            other => panic!("expected columns, got {other:?}"),
        }
    }

    #[test]
    fn parses_qualified_projection_columns() {
        let stmt = parse("SELECT u.name, o.oid FROM users u").unwrap();
        match stmt.projection {
            Projection::Columns(cols) => {
                assert_eq!(
                    cols[0],
                    ColumnRef {
                        qualifier: Some("u".into()),
                        name: "name".into()
                    }
                );
                assert_eq!(
                    cols[1],
                    ColumnRef {
                        qualifier: Some("o".into()),
                        name: "oid".into()
                    }
                );
            }
            other => panic!("expected columns, got {other:?}"),
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

    #[test]
    fn parses_where_with_int_and_string_literals() {
        let int_stmt = parse("SELECT * FROM users u WHERE u.id = 1").unwrap();
        let f = int_stmt.filter.unwrap();
        assert_eq!(
            f.column,
            ColumnRef {
                qualifier: Some("u".into()),
                name: "id".into()
            }
        );
        assert_eq!(f.op, CmpOp::Eq);
        assert_eq!(f.value, Value::Int(1));

        let str_stmt = parse("SELECT * FROM users WHERE name = 'alice'").unwrap();
        assert_eq!(str_stmt.filter.unwrap().value, Value::Text("alice".into()));
    }

    #[test]
    fn parses_full_m1_query() {
        let stmt =
            parse("SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1")
                .unwrap();
        assert!(matches!(stmt.projection, Projection::Columns(_)));
        assert!(stmt.join.is_some());
        assert!(stmt.filter.is_some());
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
}
