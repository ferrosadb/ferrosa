//! Zero-allocation Cypher lexer.
//!
//! Tokenizes a Cypher query string, yielding `Token<'input>` values that
//! borrow directly from the source. Keywords are recognized via a `phf`
//! perfect-hash map at compile time.

use phf::phf_map;

use crate::parser::error::{ParseError, ParseResult, Span};
use crate::parser::token::{Keyword, Token, TokenKind};

/// Compile-time keyword map. Case-insensitive lookup is done by
/// uppercasing the candidate before lookup.
static KEYWORDS: phf::Map<&'static str, Keyword> = phf_map! {
    "MATCH" => Keyword::Match,
    "RETURN" => Keyword::Return,
    "WHERE" => Keyword::Where,
    "CREATE" => Keyword::Create,
    "DELETE" => Keyword::Delete,
    "DETACH" => Keyword::Detach,
    "SET" => Keyword::Set,
    "ORDER" => Keyword::Order,
    "BY" => Keyword::By,
    "LIMIT" => Keyword::Limit,
    "AND" => Keyword::And,
    "OR" => Keyword::Or,
    "NOT" => Keyword::Not,
    "AS" => Keyword::As,
    "ASC" => Keyword::Asc,
    "DESC" => Keyword::Desc,
    "DISTINCT" => Keyword::Distinct,
    "TRUE" => Keyword::True,
    "FALSE" => Keyword::False,
    "NULL" => Keyword::Null,
    "IS" => Keyword::Is,
    "SUBSCRIBE" => Keyword::Subscribe,
    "UNSUBSCRIBE" => Keyword::Unsubscribe,
    "EVERY" => Keyword::Every,
    "DELTA" => Keyword::Delta,
};

/// Check if two token kinds match. For keywords, this compares the
/// specific keyword variant. For other token kinds, uses discriminant.
fn kind_matches(actual: &TokenKind<'_>, expected: &TokenKind<'_>) -> bool {
    match (actual, expected) {
        (TokenKind::Keyword(a), TokenKind::Keyword(b)) => a == b,
        _ => std::mem::discriminant(actual) == std::mem::discriminant(expected),
    }
}

/// Zero-allocation lexer for Cypher queries.
///
/// Yields `Token<'input>` values that borrow from the source string.
/// Maintains a byte offset cursor and supports peek/next.
pub struct Lexer<'input> {
    pub(crate) input: &'input str,
    bytes: &'input [u8],
    pos: usize,
    /// Peeked token, if any.
    peeked: Option<Token<'input>>,
}

impl<'input> Lexer<'input> {
    /// Create a new lexer over the given input.
    pub fn new(input: &'input str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            peeked: None,
        }
    }

    /// Peek at the next token without consuming it.
    pub fn peek(&mut self) -> ParseResult<&Token<'input>> {
        if self.peeked.is_none() {
            self.peeked = Some(self.advance()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    /// Consume and return the next token.
    pub fn next_token(&mut self) -> ParseResult<Token<'input>> {
        if let Some(tok) = self.peeked.take() {
            return Ok(tok);
        }
        self.advance()
    }

    /// Consume the next token and assert its kind matches.
    pub fn expect(&mut self, expected: &TokenKind<'_>) -> ParseResult<Token<'input>> {
        let tok = self.next_token()?;
        if kind_matches(&tok.kind, expected) {
            Ok(tok)
        } else {
            Err(ParseError::new(
                format!("expected {:?}, got {:?}", expected, tok.kind),
                tok.span,
            ))
        }
    }

    /// Consume the next token if it matches the given kind. Returns true
    /// if consumed.
    pub fn eat(&mut self, expected: &TokenKind<'_>) -> ParseResult<bool> {
        let tok = self.peek()?;
        if kind_matches(&tok.kind, expected) {
            self.next_token()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Current byte offset.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Skip whitespace and comments.
    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' {
                self.pos += 1;
            } else if b == b'/'
                && self.pos + 1 < self.bytes.len()
                && self.bytes[self.pos + 1] == b'/'
            {
                // Line comment: skip to end of line.
                self.pos += 2;
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Advance the cursor and produce the next token.
    fn advance(&mut self) -> ParseResult<Token<'input>> {
        self.skip_whitespace();

        if self.pos >= self.bytes.len() {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span {
                    start: self.pos,
                    end: self.pos,
                },
            });
        }

        let start = self.pos;
        let b = self.bytes[self.pos];

        match b {
            // Single-character tokens.
            b'(' => self.single(TokenKind::LParen, start),
            b')' => self.single(TokenKind::RParen, start),
            b'[' => self.single(TokenKind::LBracket, start),
            b'{' => self.single(TokenKind::LBrace, start),
            b'}' => self.single(TokenKind::RBrace, start),
            b':' => self.single(TokenKind::Colon, start),
            b'.' => self.single(TokenKind::Dot, start),
            b',' => self.single(TokenKind::Comma, start),
            b'+' => self.single(TokenKind::Plus, start),
            b'*' => self.single(TokenKind::Star, start),
            b'/' => self.single(TokenKind::Slash, start),

            // = or potential compound.
            b'=' => self.single(TokenKind::Eq, start),

            // < or <= or <> or <-
            b'<' => {
                self.pos += 1;
                if self.pos < self.bytes.len() {
                    match self.bytes[self.pos] {
                        b'=' => {
                            self.pos += 1;
                            Ok(Token {
                                kind: TokenKind::LtEq,
                                span: Span {
                                    start,
                                    end: self.pos,
                                },
                            })
                        }
                        b'>' => {
                            self.pos += 1;
                            Ok(Token {
                                kind: TokenKind::Neq,
                                span: Span {
                                    start,
                                    end: self.pos,
                                },
                            })
                        }
                        b'-' => {
                            self.pos += 1;
                            Ok(Token {
                                kind: TokenKind::ArrowLeft,
                                span: Span {
                                    start,
                                    end: self.pos,
                                },
                            })
                        }
                        _ => Ok(Token {
                            kind: TokenKind::Lt,
                            span: Span {
                                start,
                                end: self.pos,
                            },
                        }),
                    }
                } else {
                    Ok(Token {
                        kind: TokenKind::Lt,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    })
                }
            }

            // > or >=
            b'>' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'=' {
                    self.pos += 1;
                    Ok(Token {
                        kind: TokenKind::GtEq,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Gt,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    })
                }
            }

            // - can be: minus, -[ (dash-bracket), -> (arrow-right)
            b'-' => {
                self.pos += 1;
                if self.pos < self.bytes.len() {
                    match self.bytes[self.pos] {
                        b'[' => {
                            self.pos += 1;
                            Ok(Token {
                                kind: TokenKind::DashBracket,
                                span: Span {
                                    start,
                                    end: self.pos,
                                },
                            })
                        }
                        b'>' => {
                            self.pos += 1;
                            Ok(Token {
                                kind: TokenKind::ArrowRight,
                                span: Span {
                                    start,
                                    end: self.pos,
                                },
                            })
                        }
                        _ => Ok(Token {
                            kind: TokenKind::Minus,
                            span: Span {
                                start,
                                end: self.pos,
                            },
                        }),
                    }
                } else {
                    Ok(Token {
                        kind: TokenKind::Minus,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    })
                }
            }

            // ] or ]-> or ]-
            b']' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'-' {
                    self.pos += 1;
                    if self.pos < self.bytes.len() && self.bytes[self.pos] == b'>' {
                        self.pos += 1;
                        Ok(Token {
                            kind: TokenKind::BracketArrow,
                            span: Span {
                                start,
                                end: self.pos,
                            },
                        })
                    } else {
                        Ok(Token {
                            kind: TokenKind::BracketDash,
                            span: Span {
                                start,
                                end: self.pos,
                            },
                        })
                    }
                } else {
                    Ok(Token {
                        kind: TokenKind::RBracket,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    })
                }
            }

            // String literal: 'text' with '' escape, or "text" with "" escape.
            b'\'' => self.lex_string(start, b'\''),
            b'"' => self.lex_string(start, b'"'),

            // Number: integer or float.
            b'0'..=b'9' => self.lex_number(start),

            // Identifier or keyword.
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.lex_ident_or_keyword(start),

            other => Err(ParseError::new(
                format!("unexpected character: '{}'", other as char),
                Span {
                    start,
                    end: start + 1,
                },
            )),
        }
    }

    fn single(&mut self, kind: TokenKind<'input>, start: usize) -> ParseResult<Token<'input>> {
        self.pos += 1;
        Ok(Token {
            kind,
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn lex_string(&mut self, start: usize, quote: u8) -> ParseResult<Token<'input>> {
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        loop {
            if self.pos >= self.bytes.len() {
                return Err(ParseError::new(
                    "unterminated string literal",
                    Span {
                        start,
                        end: self.pos,
                    },
                ));
            }
            let b = self.bytes[self.pos];
            if b == quote {
                self.pos += 1;
                // Check for escaped quote ('' or "").
                if self.pos < self.bytes.len() && self.bytes[self.pos] == quote {
                    s.push(quote as char);
                    self.pos += 1;
                } else {
                    break;
                }
            } else {
                s.push(b as char);
                self.pos += 1;
            }
        }
        Ok(Token {
            kind: TokenKind::StringLit(s),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn lex_number(&mut self, start: usize) -> ParseResult<Token<'input>> {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        // Check for decimal point.
        if self.pos < self.bytes.len()
            && self.bytes[self.pos] == b'.'
            && self.pos + 1 < self.bytes.len()
            && self.bytes[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1; // skip dot
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];
            let value: f64 = text.parse().map_err(|_| {
                ParseError::new(
                    format!("invalid float literal: {}", text),
                    Span {
                        start,
                        end: self.pos,
                    },
                )
            })?;
            Ok(Token {
                kind: TokenKind::Float(value),
                span: Span {
                    start,
                    end: self.pos,
                },
            })
        } else {
            let text = &self.input[start..self.pos];
            let value: i64 = text.parse().map_err(|_| {
                ParseError::new(
                    format!("invalid integer literal: {}", text),
                    Span {
                        start,
                        end: self.pos,
                    },
                )
            })?;
            Ok(Token {
                kind: TokenKind::Integer(value),
                span: Span {
                    start,
                    end: self.pos,
                },
            })
        }
    }

    fn lex_ident_or_keyword(&mut self, start: usize) -> ParseResult<Token<'input>> {
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b.is_ascii_alphanumeric() || b == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let text = &self.input[start..self.pos];
        // Case-insensitive keyword lookup.
        let upper = text.to_ascii_uppercase();
        if let Some(&kw) = KEYWORDS.get(upper.as_str()) {
            Ok(Token {
                kind: TokenKind::Keyword(kw),
                span: Span {
                    start,
                    end: self.pos,
                },
            })
        } else {
            Ok(Token {
                kind: TokenKind::Ident(text),
                span: Span {
                    start,
                    end: self.pos,
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_all(input: &str) -> Vec<TokenKind<'_>> {
        let mut lexer = Lexer::new(input);
        let mut tokens = vec![];
        loop {
            let tok = lexer.next_token().unwrap();
            if tok.kind == TokenKind::Eof {
                break;
            }
            tokens.push(tok.kind);
        }
        tokens
    }

    #[test]
    fn lex_keywords_case_insensitive() {
        let tokens = lex_all("MATCH match Match");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Match),
                TokenKind::Keyword(Keyword::Match),
                TokenKind::Keyword(Keyword::Match),
            ]
        );
    }

    #[test]
    fn lex_identifiers() {
        let tokens = lex_all("foo bar_baz x1");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Ident("foo"),
                TokenKind::Ident("bar_baz"),
                TokenKind::Ident("x1"),
            ]
        );
    }

    #[test]
    fn lex_string_literal() {
        let tokens = lex_all("'hello' 'it''s'");
        assert_eq!(
            tokens,
            vec![
                TokenKind::StringLit("hello".into()),
                TokenKind::StringLit("it's".into()),
            ]
        );
    }

    #[test]
    fn lex_numbers() {
        let tokens = lex_all("42 3.25");
        assert_eq!(
            tokens,
            vec![TokenKind::Integer(42), TokenKind::Float(3.25),]
        );
    }

    #[test]
    fn lex_operators_and_punctuation() {
        let tokens = lex_all("( ) { } [ : . , = <> < > <= >=");
        assert_eq!(
            tokens,
            vec![
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::LBrace,
                TokenKind::RBrace,
                TokenKind::LBracket,
                TokenKind::Colon,
                TokenKind::Dot,
                TokenKind::Comma,
                TokenKind::Eq,
                TokenKind::Neq,
                TokenKind::Lt,
                TokenKind::Gt,
                TokenKind::LtEq,
                TokenKind::GtEq,
            ]
        );
    }

    #[test]
    fn lex_relationship_arrows() {
        let tokens = lex_all("-[ ]-> <- ]-");
        assert_eq!(
            tokens,
            vec![
                TokenKind::DashBracket,
                TokenKind::BracketArrow,
                TokenKind::ArrowLeft,
                TokenKind::BracketDash,
            ]
        );
    }

    #[test]
    fn lex_node_edge_pattern() {
        // (a)-[:KNOWS]->(b)
        let tokens = lex_all("(a)-[:KNOWS]->(b)");
        assert_eq!(
            tokens,
            vec![
                TokenKind::LParen,
                TokenKind::Ident("a"),
                TokenKind::RParen,
                TokenKind::DashBracket,
                TokenKind::Colon,
                TokenKind::Ident("KNOWS"),
                TokenKind::BracketArrow,
                TokenKind::LParen,
                TokenKind::Ident("b"),
                TokenKind::RParen,
            ]
        );
    }

    #[test]
    fn lex_skips_line_comments() {
        let tokens = lex_all("MATCH // comment\nRETURN");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Match),
                TokenKind::Keyword(Keyword::Return),
            ]
        );
    }

    #[test]
    fn lex_unterminated_string_error() {
        let mut lexer = Lexer::new("'oops");
        let err = lexer.next_token().unwrap_err();
        assert!(err.message.contains("unterminated"));
    }
}
