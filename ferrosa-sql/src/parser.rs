//! Hand-written lexer + recursive-descent parser for the M1 SQL subset.

use std::fmt;

use crate::ast::{
    AggArg, ColumnRef, Filter, Join, OrderItem, Projection, SelectItem, SelectStmt, TableRef,
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
    let group_by = if matches!(p.peek(), Some(Tok::Group)) {
        p.next();
        p.expect(&Tok::By, "BY")?;
        p.parse_column_list()?
    } else {
        Vec::new()
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
        projection,
        from,
        join,
        filter,
        group_by,
        order_by,
        limit,
        offset,
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
    Group,
    By,
    Order,
    Asc,
    Desc,
    Limit,
    Offset,
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
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
                    "ORDER" => Tok::Order,
                    "ASC" => Tok::Asc,
                    "DESC" => Tok::Desc,
                    "LIMIT" => Tok::Limit,
                    "OFFSET" => Tok::Offset,
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
        assert_eq!(stmt.filter.unwrap().value, Value::float(1.5));
    }

    #[test]
    fn parses_float_literal_variants() {
        // Negative fractional.
        let a = parse("SELECT * FROM t WHERE x = -0.25").unwrap();
        assert_eq!(a.filter.unwrap().value, Value::float(-0.25));
        // Trailing dot with no fractional digits.
        let b = parse("SELECT * FROM t WHERE x = 10.").unwrap();
        assert_eq!(b.filter.unwrap().value, Value::float(10.0));
        // A plain integer (no dot) stays an Int.
        let c = parse("SELECT * FROM t WHERE x = 10").unwrap();
        assert_eq!(c.filter.unwrap().value, Value::Int(10));
    }

    #[test]
    fn float_in_comparison_predicate() {
        let stmt = parse("SELECT * FROM t WHERE score > 3.5").unwrap();
        let f = stmt.filter.unwrap();
        assert_eq!(f.op, CmpOp::Gt);
        assert_eq!(f.value, Value::float(3.5));
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
}
