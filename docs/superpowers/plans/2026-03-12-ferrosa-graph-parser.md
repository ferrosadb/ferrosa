# ferrosa-graph Cypher Parser Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a hand-rolled recursive-descent Cypher parser for ferrosa-graph that transforms Cypher query text into a typed AST, with zero runtime allocation in the lexer and property-based testing for safety.

**Architecture:** Same architecture as ferrosa-cql's parser — `phf` compile-time keyword map, zero-copy `Token<'input>` borrowing from source, one function per grammar rule, LL(2) with no backtracking. The parser is a standalone module within the `ferrosa-graph` crate with no dependencies on ferrosa-storage or ferrosa-schema.

**Tech Stack:** Rust, `phf` (compile-time perfect hashing for keywords), `proptest` (dev, property-based testing)

**Design Spec:** `docs/superpowers/specs/2026-03-12-ferrosa-graph-design.md`

**CQL Parser Reference:** `docs/superpowers/specs/2026-03-12-ferrosa-cql-design.md` (same patterns)

**Worktree:** `.worktrees/graph-parser/` on branch `feature/graph-parser`

---

## File Structure

```
ferrosa-graph/
  Cargo.toml                    # phf, phf_macros; dev: proptest
  src/
    lib.rs                      # pub mod parser; (other modules added in later phases)
    parser/
      mod.rs                    # pub use re-exports, parse() entry point
      token.rs                  # Token enum, Span, TokenKind
      lexer.rs                  # Lexer struct, zero-alloc tokenizer, phf keyword map
      ast.rs                    # Statement, Pattern, Expr, ReturnClause, etc.
      parser.rs                 # Recursive descent: one fn per grammar rule
      error.rs                  # ParseError with span and message
```

## Cypher Subset Grammar (Phase 1)

```
statement       = match_stmt | create_stmt | delete_stmt | set_stmt
match_stmt      = MATCH pattern_list [WHERE expr] return_clause
create_stmt     = CREATE pattern_list
delete_stmt     = [MATCH pattern_list [WHERE expr]] (DELETE | DETACH DELETE) var_list
set_stmt        = MATCH pattern_list [WHERE expr] SET assignment_list

pattern_list    = pattern (',' pattern)*
pattern         = node_pattern (rel_pattern node_pattern)*
node_pattern    = '(' [var] [':' label] [prop_map] ')'
rel_pattern     = '-[' [var] [':' rel_type] [prop_map] ']->'
                | '<-[' [var] [':' rel_type] [prop_map] ']-'
                | '-[' [var] [':' rel_type] [prop_map] ']-'
prop_map        = '{' (ident ':' expr (',' ident ':' expr)*)? '}'

return_clause   = RETURN [DISTINCT] return_items [order_clause] [limit_clause]
return_items    = return_item (',' return_item)*
return_item     = expr [AS ident]
order_clause    = ORDER BY order_item (',' order_item)*
order_item      = expr [ASC | DESC]
limit_clause    = LIMIT integer

expr            = or_expr
or_expr         = and_expr (OR and_expr)*
and_expr        = not_expr (AND not_expr)*
not_expr        = [NOT] comparison
comparison      = addition ((= | <> | < | > | <= | >=) addition)?
addition        = multiplication ((+ | -) multiplication)*
multiplication  = unary ((* | /) unary)*
unary           = [- | NOT] primary
primary         = literal | property_access | var | function_call | '(' expr ')'
property_access = var '.' ident
function_call   = ident '(' [expr (',' expr)*] ')'
literal         = string | integer | float | TRUE | FALSE | NULL

assignment_list = assignment (',' assignment)*
assignment      = property_access '=' expr
var_list        = var (',' var)*
```

---

## Chunk 1: Crate Scaffold and AST Types

### Task 1: Create ferrosa-graph crate scaffold

**Files:**

- Create: `ferrosa-graph/Cargo.toml`
- Create: `ferrosa-graph/src/lib.rs`
- Create: `ferrosa-graph/src/parser/mod.rs`
- Create: `ferrosa-graph/src/parser/error.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "ferrosa-graph"
version = "0.1.0"
edition = "2021"
description = "Graph query engine for ferrosa — Cypher/GQL endpoint"

[dependencies]
phf = { version = "0.11", features = ["macros"] }

[dev-dependencies]
proptest = "1"
```

- [ ] **Step 2: Create src/lib.rs**

```rust
//! # ferrosa-graph
//!
//! Graph query engine for ferrosa. Provides a Cypher/GQL query endpoint
//! alongside CQL, with data stored in normal CQL tables and accessed
//! via a system-managed adjacency index.
//!
//! ## Modules
//!
//! - [`parser`] — Cypher lexer, parser, and AST types.

pub mod parser;
```

- [ ] **Step 3: Create src/parser/mod.rs**

```rust
//! Cypher query parser.
//!
//! Hand-rolled recursive-descent parser for an openCypher subset.
//! Zero-alloc lexer with `phf` keyword lookup, LL(2) grammar, one
//! function per production rule.

mod error;

pub use error::{ParseError, ParseResult};
```

- [ ] **Step 4: Create src/parser/error.rs**

```rust
//! Parse error types with source location.

use std::fmt;

/// Byte offset span in the source query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// A parse error with location and context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "parse error at byte {}: {}",
            self.span.start, self.message
        )
    }
}

impl std::error::Error for ParseError {}

impl ParseError {
    pub fn new(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

/// Result type for parser operations.
pub type ParseResult<T> = Result<T, ParseError>;
```

- [ ] **Step 5: Add ferrosa-graph to workspace Cargo.toml**

Add `"ferrosa-graph"` to the `members` list in the workspace `Cargo.toml`.

- [ ] **Step 6: Build to verify**

Run: `cargo build -p ferrosa-graph`
Expected: Compiles with no errors.

- [ ] **Step 7: Commit**

```bash
git add ferrosa-graph/ Cargo.toml
git commit -m "feat(graph): scaffold ferrosa-graph crate with parser module"
```

---

### Task 2: AST types

**Files:**

- Create: `ferrosa-graph/src/parser/ast.rs`
- Modify: `ferrosa-graph/src/parser/mod.rs`

- [ ] **Step 1: Create src/parser/ast.rs with all AST types**

```rust
//! Cypher AST types.
//!
//! Represents the parsed structure of Cypher queries. Each variant maps
//! to a production rule in the grammar. The AST is produced by the parser
//! and consumed by the planner (in a later phase).

/// Direction of a relationship in a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `(a)-[:REL]->(b)` — left to right.
    Out,
    /// `(a)<-[:REL]-(b)` — right to left.
    In,
    /// `(a)-[:REL]-(b)` — either direction.
    Both,
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,        // =
    Neq,       // <>
    Lt,        // <
    Gt,        // >
    LtEq,      // <=
    GtEq,      // >=
}

/// Arithmetic operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A literal value in Cypher.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// An expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Variable reference: `n`
    Var(String),
    /// Property access: `n.name`
    Property { var: String, name: String },
    /// Literal value.
    Literal(Literal),
    /// Function call: `count(n)`, `id(n)`
    Function { name: String, args: Vec<Expr> },
    /// Comparison: `a.age > 30`
    Comparison {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    /// Arithmetic: `a.age + 1`
    Arithmetic {
        left: Box<Expr>,
        op: ArithOp,
        right: Box<Expr>,
    },
    /// Boolean AND.
    And(Box<Expr>, Box<Expr>),
    /// Boolean OR.
    Or(Box<Expr>, Box<Expr>),
    /// Boolean NOT.
    Not(Box<Expr>),
    /// `expr IS NULL`
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`
    IsNotNull(Box<Expr>),
}

/// A property map in a node or edge pattern: `{name: 'Alice', age: 30}`.
pub type PropMap = Vec<(String, Expr)>;

/// A graph pattern element.
#[derive(Debug, Clone, PartialEq)]
pub enum Pattern {
    /// Node: `(n:Person {name: 'Alice'})`
    Node {
        var: Option<String>,
        label: Option<String>,
        props: PropMap,
    },
    /// Relationship: `-[:KNOWS {since: 2020}]->`
    ///
    /// Only appears inside `Pattern::Path` — never stands alone.
    /// The path provides the node context (src/dst).
    Rel {
        var: Option<String>,
        rel_type: Option<String>,
        direction: Direction,
        props: PropMap,
    },
    /// A path is an alternating sequence of nodes and relationships:
    /// `(a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)`
    /// Stored as: [Node, Rel, Node, Rel, Node]
    Path(Vec<Pattern>),
}

/// A single item in a RETURN clause: `a.name AS alias`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// A single item in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub direction: SortDir,
}

/// The RETURN clause with optional DISTINCT, ORDER BY, and LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
}

/// A property assignment in a SET clause: `n.name = 'Bob'`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub var: String,
    pub property: String,
    pub value: Expr,
}

/// Top-level Cypher statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `MATCH pattern [WHERE expr] RETURN ...`
    Match {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        return_clause: ReturnClause,
    },
    /// `CREATE pattern, ...`
    Create { patterns: Vec<Pattern> },
    /// `MATCH pattern [WHERE expr] SET assignments`
    Set {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    /// `MATCH pattern [WHERE expr] [DETACH] DELETE vars`
    Delete {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        detach: bool,
        variables: Vec<String>,
    },
}
```

- [ ] **Step 2: Add ast module to parser/mod.rs**

Add `pub mod ast;` to the module and re-export: `pub use ast::*;`

- [ ] **Step 3: Build to verify types compile**

Run: `cargo build -p ferrosa-graph`
Expected: Compiles with no errors.

- [ ] **Step 4: Add a unit test that constructs AST nodes**

Add to bottom of `ast.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_match_statement() {
        let stmt = Statement::Match {
            pattern: vec![Pattern::Path(vec![
                Pattern::Node {
                    var: Some("a".into()),
                    label: Some("Person".into()),
                    props: vec![("name".into(), Expr::Literal(Literal::String("Alice".into())))],
                },
                Pattern::Rel {
                    var: None,
                    rel_type: Some("KNOWS".into()),
                    direction: Direction::Out,
                    props: vec![],
                },
                Pattern::Node {
                    var: Some("b".into()),
                    label: None,
                    props: vec![],
                },
            ])],
            where_clause: None,
            return_clause: ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Property {
                        var: "b".into(),
                        name: "name".into(),
                    },
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
        };
        // Verify it's a Match variant.
        assert!(matches!(stmt, Statement::Match { .. }));
    }

    #[test]
    fn construct_create_statement() {
        let stmt = Statement::Create {
            patterns: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![
                    ("name".into(), Expr::Literal(Literal::String("Alice".into()))),
                    ("age".into(), Expr::Literal(Literal::Integer(30))),
                ],
            }],
        };
        assert!(matches!(stmt, Statement::Create { .. }));
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-graph`
Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add ferrosa-graph/src/parser/ast.rs ferrosa-graph/src/parser/mod.rs
git commit -m "feat(graph): add Cypher AST types for Phase 1 grammar"
```

---

## Chunk 2: Lexer

### Task 3: Token types and basic lexer

**Files:**

- Create: `ferrosa-graph/src/parser/token.rs`
- Create: `ferrosa-graph/src/parser/lexer.rs`
- Modify: `ferrosa-graph/src/parser/mod.rs`

- [ ] **Step 1: Create src/parser/token.rs**

```rust
//! Token types for the Cypher lexer.

use crate::parser::error::Span;

/// A keyword recognized by the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Match,
    Return,
    Where,
    Create,
    Delete,
    Detach,
    Set,
    Order,
    By,
    Limit,
    And,
    Or,
    Not,
    As,
    Asc,
    Desc,
    Distinct,
    True,
    False,
    Null,
    Is,
}

/// A token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind<'input> {
    /// A recognized keyword.
    Keyword(Keyword),
    /// An identifier (variable or label name).
    Ident(&'input str),
    /// A single-quoted string literal (unescaped content).
    StringLit(String),
    /// An integer literal.
    Integer(i64),
    /// A floating-point literal.
    Float(f64),

    // Punctuation
    LParen,       // (
    RParen,       // )
    LBracket,     // [
    RBracket,     // ]
    LBrace,       // {
    RBrace,       // }
    Colon,        // :
    Dot,          // .
    Comma,        // ,
    Eq,           // =
    Neq,          // <>
    Lt,           // <
    Gt,           // >
    LtEq,        // <=
    GtEq,        // >=
    Plus,         // +
    Minus,        // -
    Star,         // *
    Slash,        // /

    // Relationship arrows
    ArrowRight,   // ->
    ArrowLeft,    // <-
    DashBracket,  // -[
    BracketDash,  // ]-
    BracketArrow, // ]->

    /// End of input.
    Eof,
}

/// A token with its source span.
#[derive(Debug, Clone, PartialEq)]
pub struct Token<'input> {
    pub kind: TokenKind<'input>,
    pub span: Span,
}
```

- [ ] **Step 2: Create src/parser/lexer.rs with the Lexer struct**

```rust
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
};

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
        if std::mem::discriminant(&tok.kind) == std::mem::discriminant(expected) {
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
        if std::mem::discriminant(&tok.kind) == std::mem::discriminant(expected) {
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
            } else if b == b'/' && self.pos + 1 < self.bytes.len() && self.bytes[self.pos + 1] == b'/' {
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
                            Ok(Token { kind: TokenKind::LtEq, span: Span { start, end: self.pos } })
                        }
                        b'>' => {
                            self.pos += 1;
                            Ok(Token { kind: TokenKind::Neq, span: Span { start, end: self.pos } })
                        }
                        b'-' => {
                            self.pos += 1;
                            Ok(Token { kind: TokenKind::ArrowLeft, span: Span { start, end: self.pos } })
                        }
                        _ => Ok(Token { kind: TokenKind::Lt, span: Span { start, end: self.pos } }),
                    }
                } else {
                    Ok(Token { kind: TokenKind::Lt, span: Span { start, end: self.pos } })
                }
            }

            // > or >=
            b'>' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'=' {
                    self.pos += 1;
                    Ok(Token { kind: TokenKind::GtEq, span: Span { start, end: self.pos } })
                } else {
                    Ok(Token { kind: TokenKind::Gt, span: Span { start, end: self.pos } })
                }
            }

            // - can be: minus, -[ (dash-bracket), -> (arrow-right)
            b'-' => {
                self.pos += 1;
                if self.pos < self.bytes.len() {
                    match self.bytes[self.pos] {
                        b'[' => {
                            self.pos += 1;
                            Ok(Token { kind: TokenKind::DashBracket, span: Span { start, end: self.pos } })
                        }
                        b'>' => {
                            self.pos += 1;
                            Ok(Token { kind: TokenKind::ArrowRight, span: Span { start, end: self.pos } })
                        }
                        _ => Ok(Token { kind: TokenKind::Minus, span: Span { start, end: self.pos } }),
                    }
                } else {
                    Ok(Token { kind: TokenKind::Minus, span: Span { start, end: self.pos } })
                }
            }

            // ] or ]-> or ]-
            b']' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'-' {
                    self.pos += 1;
                    if self.pos < self.bytes.len() && self.bytes[self.pos] == b'>' {
                        self.pos += 1;
                        Ok(Token { kind: TokenKind::BracketArrow, span: Span { start, end: self.pos } })
                    } else {
                        Ok(Token { kind: TokenKind::BracketDash, span: Span { start, end: self.pos } })
                    }
                } else {
                    Ok(Token { kind: TokenKind::RBracket, span: Span { start, end: self.pos } })
                }
            }

            // String literal: 'text' with '' escape.
            b'\'' => self.lex_string(start),

            // Number: integer or float.
            b'0'..=b'9' => self.lex_number(start),

            // Identifier or keyword.
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.lex_ident_or_keyword(start),

            other => Err(ParseError::new(
                format!("unexpected character: '{}'", other as char),
                Span { start, end: start + 1 },
            )),
        }
    }

    fn single(&mut self, kind: TokenKind<'input>, start: usize) -> ParseResult<Token<'input>> {
        self.pos += 1;
        Ok(Token { kind, span: Span { start, end: self.pos } })
    }

    fn lex_string(&mut self, start: usize) -> ParseResult<Token<'input>> {
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        loop {
            if self.pos >= self.bytes.len() {
                return Err(ParseError::new(
                    "unterminated string literal",
                    Span { start, end: self.pos },
                ));
            }
            let b = self.bytes[self.pos];
            if b == b'\'' {
                self.pos += 1;
                // Check for escaped quote ('').
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'\'' {
                    s.push('\'');
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
            span: Span { start, end: self.pos },
        })
    }

    fn lex_number(&mut self, start: usize) -> ParseResult<Token<'input>> {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        // Check for decimal point.
        if self.pos < self.bytes.len() && self.bytes[self.pos] == b'.'
            && self.pos + 1 < self.bytes.len() && self.bytes[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1; // skip dot
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];
            let value: f64 = text.parse().map_err(|_| {
                ParseError::new(
                    format!("invalid float literal: {}", text),
                    Span { start, end: self.pos },
                )
            })?;
            Ok(Token {
                kind: TokenKind::Float(value),
                span: Span { start, end: self.pos },
            })
        } else {
            let text = &self.input[start..self.pos];
            let value: i64 = text.parse().map_err(|_| {
                ParseError::new(
                    format!("invalid integer literal: {}", text),
                    Span { start, end: self.pos },
                )
            })?;
            Ok(Token {
                kind: TokenKind::Integer(value),
                span: Span { start, end: self.pos },
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
                span: Span { start, end: self.pos },
            })
        } else {
            Ok(Token {
                kind: TokenKind::Ident(text),
                span: Span { start, end: self.pos },
            })
        }
    }
}
```

- [ ] **Step 3: Add modules to parser/mod.rs**

Add `pub mod token;` and `pub mod lexer;` to `mod.rs`. Add re-exports:

```rust
pub use lexer::Lexer;
pub use token::{Keyword, Token, TokenKind};
```

- [ ] **Step 4: Build to verify**

Run: `cargo build -p ferrosa-graph`
Expected: Compiles with no errors.

- [ ] **Step 5: Write lexer unit tests**

Add to bottom of `lexer.rs`:

```rust
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
        assert_eq!(tokens, vec![
            TokenKind::Keyword(Keyword::Match),
            TokenKind::Keyword(Keyword::Match),
            TokenKind::Keyword(Keyword::Match),
        ]);
    }

    #[test]
    fn lex_identifiers() {
        let tokens = lex_all("foo bar_baz x1");
        assert_eq!(tokens, vec![
            TokenKind::Ident("foo"),
            TokenKind::Ident("bar_baz"),
            TokenKind::Ident("x1"),
        ]);
    }

    #[test]
    fn lex_string_literal() {
        let tokens = lex_all("'hello' 'it''s'");
        assert_eq!(tokens, vec![
            TokenKind::StringLit("hello".into()),
            TokenKind::StringLit("it's".into()),
        ]);
    }

    #[test]
    fn lex_numbers() {
        let tokens = lex_all("42 3.14");
        assert_eq!(tokens, vec![
            TokenKind::Integer(42),
            TokenKind::Float(3.14),
        ]);
    }

    #[test]
    fn lex_operators_and_punctuation() {
        let tokens = lex_all("( ) { } [ : . , = <> < > <= >=");
        assert_eq!(tokens, vec![
            TokenKind::LParen, TokenKind::RParen,
            TokenKind::LBrace, TokenKind::RBrace,
            TokenKind::LBracket, TokenKind::Colon,
            TokenKind::Dot, TokenKind::Comma,
            TokenKind::Eq, TokenKind::Neq,
            TokenKind::Lt, TokenKind::Gt,
            TokenKind::LtEq, TokenKind::GtEq,
        ]);
    }

    #[test]
    fn lex_relationship_arrows() {
        let tokens = lex_all("-[ ]-> <- ]-");
        assert_eq!(tokens, vec![
            TokenKind::DashBracket,
            TokenKind::BracketArrow,
            TokenKind::ArrowLeft,
            TokenKind::BracketDash,
        ]);
    }

    #[test]
    fn lex_node_edge_pattern() {
        // (a)-[:KNOWS]->(b)
        let tokens = lex_all("(a)-[:KNOWS]->(b)");
        assert_eq!(tokens, vec![
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
        ]);
    }

    #[test]
    fn lex_skips_line_comments() {
        let tokens = lex_all("MATCH // comment\nRETURN");
        assert_eq!(tokens, vec![
            TokenKind::Keyword(Keyword::Match),
            TokenKind::Keyword(Keyword::Return),
        ]);
    }

    #[test]
    fn lex_unterminated_string_error() {
        let mut lexer = Lexer::new("'oops");
        let err = lexer.next_token().unwrap_err();
        assert!(err.message.contains("unterminated"));
    }
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ferrosa-graph`
Expected: All tests pass (AST tests from Task 2 + lexer tests).

- [ ] **Step 7: Commit**

```bash
git add ferrosa-graph/src/parser/token.rs ferrosa-graph/src/parser/lexer.rs ferrosa-graph/src/parser/mod.rs
git commit -m "feat(graph): add Cypher lexer with phf keyword map and zero-copy tokens"
```

---

## Chunk 3: Parser — Patterns and Expressions

### Task 4: Parser — node patterns and property maps

**Files:**

- Create: `ferrosa-graph/src/parser/parser.rs`
- Modify: `ferrosa-graph/src/parser/mod.rs`

- [ ] **Step 1: Write failing test for node pattern parsing**

Add to a `tests` module at the bottom of `parser.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;

    #[test]
    fn parse_empty_node() {
        let node = parse_node_pattern(&mut Lexer::new("()")).unwrap();
        assert_eq!(node, Pattern::Node { var: None, label: None, props: vec![] });
    }

    #[test]
    fn parse_node_with_var() {
        let node = parse_node_pattern(&mut Lexer::new("(n)")).unwrap();
        assert_eq!(node, Pattern::Node { var: Some("n".into()), label: None, props: vec![] });
    }

    #[test]
    fn parse_node_with_label() {
        let node = parse_node_pattern(&mut Lexer::new("(n:Person)")).unwrap();
        assert_eq!(node, Pattern::Node {
            var: Some("n".into()),
            label: Some("Person".into()),
            props: vec![],
        });
    }

    #[test]
    fn parse_node_with_props() {
        let node = parse_node_pattern(&mut Lexer::new("(n:Person {name: 'Alice', age: 30})")).unwrap();
        assert_eq!(node, Pattern::Node {
            var: Some("n".into()),
            label: Some("Person".into()),
            props: vec![
                ("name".into(), Expr::Literal(Literal::String("Alice".into()))),
                ("age".into(), Expr::Literal(Literal::Integer(30))),
            ],
        });
    }

    #[test]
    fn parse_node_label_only() {
        let node = parse_node_pattern(&mut Lexer::new("(:Person)")).unwrap();
        assert_eq!(node, Pattern::Node { var: None, label: Some("Person".into()), props: vec![] });
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-graph`
Expected: FAIL — `parse_node_pattern` not defined.

- [ ] **Step 3: Implement parser.rs with node pattern and property map parsing**

```rust
//! Recursive-descent Cypher parser.
//!
//! One function per grammar rule. LL(2) — at most two-token lookahead.
//! Produces an AST from the token stream.

use crate::parser::ast::*;
use crate::parser::error::{ParseError, ParseResult, Span};
use crate::parser::lexer::Lexer;
use crate::parser::token::{Keyword, TokenKind};

/// Parse a complete Cypher statement from source text.
pub fn parse(input: &str) -> ParseResult<Statement> {
    let mut lexer = Lexer::new(input);
    let stmt = parse_statement(&mut lexer)?;
    // Ensure we consumed all input.
    let tok = lexer.next_token()?;
    if tok.kind != TokenKind::Eof {
        return Err(ParseError::new(
            format!("unexpected token after statement: {:?}", tok.kind),
            tok.span,
        ));
    }
    Ok(stmt)
}

fn parse_statement(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Keyword(Keyword::Match) => parse_match(lexer),
        TokenKind::Keyword(Keyword::Create) => parse_create(lexer),
        _ => Err(ParseError::new(
            format!("expected MATCH or CREATE, got {:?}", tok.kind),
            tok.span,
        )),
    }
}

fn parse_match(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    lexer.expect(&TokenKind::Keyword(Keyword::Match))?;
    let pattern = parse_pattern_list(lexer)?;

    // Optional WHERE.
    let where_clause = if lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
        Some(parse_expr(lexer)?)
    } else {
        None
    };

    // Check what follows: RETURN, SET, DELETE, DETACH DELETE.
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Keyword(Keyword::Return) => {
            let return_clause = parse_return_clause(lexer)?;
            Ok(Statement::Match { pattern, where_clause, return_clause })
        }
        TokenKind::Keyword(Keyword::Set) => {
            lexer.next_token()?;
            let assignments = parse_assignment_list(lexer)?;
            Ok(Statement::Set { pattern, where_clause, assignments })
        }
        TokenKind::Keyword(Keyword::Delete) => {
            lexer.next_token()?;
            let variables = parse_var_list(lexer)?;
            Ok(Statement::Delete { pattern, where_clause, detach: false, variables })
        }
        TokenKind::Keyword(Keyword::Detach) => {
            lexer.next_token()?;
            lexer.expect(&TokenKind::Keyword(Keyword::Delete))?;
            let variables = parse_var_list(lexer)?;
            Ok(Statement::Delete { pattern, where_clause, detach: true, variables })
        }
        _ => Err(ParseError::new(
            format!("expected RETURN, SET, DELETE, or DETACH DELETE after MATCH, got {:?}", tok.kind),
            tok.span,
        )),
    }
}

fn parse_create(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    lexer.expect(&TokenKind::Keyword(Keyword::Create))?;
    let patterns = parse_pattern_list(lexer)?;
    Ok(Statement::Create { patterns })
}

// --- Pattern parsing ---

fn parse_pattern_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<Pattern>> {
    let mut patterns = vec![parse_pattern(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        patterns.push(parse_pattern(lexer)?);
    }
    Ok(patterns)
}

fn parse_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    let first = parse_node_pattern(lexer)?;
    let mut elements = vec![first];

    // Check for relationship continuation: -[ or <- or ->
    loop {
        let tok = lexer.peek()?;
        match &tok.kind {
            TokenKind::DashBracket | TokenKind::ArrowLeft | TokenKind::Minus => {
                let rel = parse_rel_pattern(lexer)?;
                elements.push(rel);
                let node = parse_node_pattern(lexer)?;
                elements.push(node);
            }
            _ => break,
        }
    }

    if elements.len() == 1 {
        Ok(elements.into_iter().next().unwrap())
    } else {
        Ok(Pattern::Path(elements))
    }
}

pub(crate) fn parse_node_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    lexer.expect(&TokenKind::LParen)?;

    let mut var = None;
    let mut label = None;
    let mut props = vec![];

    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::RParen => {
            // Empty node: ()
        }
        TokenKind::Colon => {
            // Label only: (:Person)
            label = Some(parse_label(lexer)?);
            if lexer.peek()?.kind == TokenKind::LBrace {
                props = parse_prop_map(lexer)?;
            }
        }
        TokenKind::Ident(_) => {
            // Variable, possibly followed by label and props.
            let name_tok = lexer.next_token()?;
            if let TokenKind::Ident(name) = name_tok.kind {
                var = Some(name.to_string());
            }
            if lexer.peek()?.kind == TokenKind::Colon {
                label = Some(parse_label(lexer)?);
            }
            if lexer.peek()?.kind == TokenKind::LBrace {
                props = parse_prop_map(lexer)?;
            }
        }
        TokenKind::LBrace => {
            props = parse_prop_map(lexer)?;
        }
        _ => {
            return Err(ParseError::new(
                format!("expected variable, label, or ')' in node pattern, got {:?}", tok.kind),
                tok.span,
            ));
        }
    }

    lexer.expect(&TokenKind::RParen)?;

    Ok(Pattern::Node { var, label, props })
}

fn parse_rel_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    let tok = lexer.peek()?;
    match &tok.kind {
        // -[:TYPE]-> or -[:TYPE]- or -[var:TYPE {props}]->
        TokenKind::DashBracket => {
            lexer.next_token()?;
            let (var, rel_type, props) = parse_rel_detail(lexer)?;

            // Expect ]-> or ]- or ]
            let close = lexer.peek()?;
            let direction = match &close.kind {
                TokenKind::BracketArrow => {
                    lexer.next_token()?;
                    Direction::Out
                }
                TokenKind::BracketDash => {
                    lexer.next_token()?;
                    Direction::Both
                }
                TokenKind::RBracket => {
                    lexer.next_token()?;
                    // Check for trailing ->
                    if lexer.eat(&TokenKind::ArrowRight)? {
                        Direction::Out
                    } else if lexer.eat(&TokenKind::Minus)? {
                        Direction::Both
                    } else {
                        Direction::Both
                    }
                }
                _ => {
                    return Err(ParseError::new(
                        format!("expected ]->, ]-, or ] in relationship, got {:?}", close.kind),
                        close.span,
                    ));
                }
            };

            Ok(Pattern::Rel { var, rel_type, direction, props })
        }
        // <-[:TYPE]- or <-[var:TYPE]-
        TokenKind::ArrowLeft => {
            lexer.next_token()?;
            lexer.expect(&TokenKind::LBracket)?;
            let (var, rel_type, props) = parse_rel_detail(lexer)?;
            // Expect ]- or ]
            let close = lexer.peek()?;
            match &close.kind {
                TokenKind::BracketDash => { lexer.next_token()?; }
                TokenKind::RBracket => {
                    lexer.next_token()?;
                    lexer.expect(&TokenKind::Minus)?;
                }
                _ => {
                    return Err(ParseError::new(
                        format!("expected ]- in incoming relationship, got {:?}", close.kind),
                        close.span,
                    ));
                }
            }
            Ok(Pattern::Rel { var, rel_type, direction: Direction::In, props })
        }
        // Bare - used in undirected: (a)-(b) — treated as Both
        TokenKind::Minus => {
            lexer.next_token()?;
            Ok(Pattern::Rel {
                var: None,
                rel_type: None,
                direction: Direction::Both,
                props: vec![],
            })
        }
        _ => Err(ParseError::new(
            format!("expected -[, <-, or - in relationship pattern, got {:?}", tok.kind),
            tok.span,
        )),
    }
}

/// Parse the inside of `[ ... ]` in a relationship: `var:TYPE {props}`
fn parse_rel_detail(lexer: &mut Lexer<'_>) -> ParseResult<(Option<String>, Option<String>, PropMap)> {
    let mut var = None;
    let mut rel_type = None;
    let mut props = vec![];

    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Colon => {
            rel_type = Some(parse_label(lexer)?);
        }
        TokenKind::Ident(_) => {
            let name_tok = lexer.next_token()?;
            if let TokenKind::Ident(name) = name_tok.kind {
                var = Some(name.to_string());
            }
            if lexer.peek()?.kind == TokenKind::Colon {
                rel_type = Some(parse_label(lexer)?);
            }
        }
        _ => {
            // Empty brackets: -[]-
        }
    }

    if lexer.peek()?.kind == TokenKind::LBrace {
        props = parse_prop_map(lexer)?;
    }

    Ok((var, rel_type, props))
}

/// Parse `:Label` — consume colon and return the label name.
fn parse_label(lexer: &mut Lexer<'_>) -> ParseResult<String> {
    lexer.expect(&TokenKind::Colon)?;
    let tok = lexer.next_token()?;
    match tok.kind {
        TokenKind::Ident(name) => Ok(name.to_string()),
        // Keywords can be labels too (e.g., :Order, :Set).
        TokenKind::Keyword(_) => {
            let text = &lexer.input[tok.span.start..tok.span.end];
            Ok(text.to_string())
        }
        _ => Err(ParseError::new(
            format!("expected label name after ':', got {:?}", tok.kind),
            tok.span,
        )),
    }
}

/// Parse `{key: value, ...}`.
fn parse_prop_map(lexer: &mut Lexer<'_>) -> ParseResult<PropMap> {
    lexer.expect(&TokenKind::LBrace)?;
    let mut props = vec![];

    if lexer.peek()?.kind != TokenKind::RBrace {
        loop {
            let key_tok = lexer.next_token()?;
            let key = match key_tok.kind {
                TokenKind::Ident(name) => name.to_string(),
                _ => {
                    return Err(ParseError::new(
                        format!("expected property name, got {:?}", key_tok.kind),
                        key_tok.span,
                    ));
                }
            };
            lexer.expect(&TokenKind::Colon)?;
            let value = parse_expr(lexer)?;
            props.push((key, value));

            if !lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
    }

    lexer.expect(&TokenKind::RBrace)?;
    Ok(props)
}

// --- Expression parsing (precedence climbing) ---

pub(crate) fn parse_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    parse_or_expr(lexer)
}

fn parse_or_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_and_expr(lexer)?;
    while lexer.eat(&TokenKind::Keyword(Keyword::Or))? {
        let right = parse_and_expr(lexer)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_not_expr(lexer)?;
    while lexer.eat(&TokenKind::Keyword(Keyword::And))? {
        let right = parse_not_expr(lexer)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    if lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
        let inner = parse_not_expr(lexer)?;
        Ok(Expr::Not(Box::new(inner)))
    } else {
        parse_comparison(lexer)
    }
}

fn parse_comparison(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let left = parse_addition(lexer)?;

    // Check for IS NULL / IS NOT NULL.
    if lexer.eat(&TokenKind::Keyword(Keyword::Is))? {
        if lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
            lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            return Ok(Expr::IsNotNull(Box::new(left)));
        } else {
            lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            return Ok(Expr::IsNull(Box::new(left)));
        }
    }

    let tok = lexer.peek()?;
    let op = match &tok.kind {
        TokenKind::Eq => Some(CompareOp::Eq),
        TokenKind::Neq => Some(CompareOp::Neq),
        TokenKind::Lt => Some(CompareOp::Lt),
        TokenKind::Gt => Some(CompareOp::Gt),
        TokenKind::LtEq => Some(CompareOp::LtEq),
        TokenKind::GtEq => Some(CompareOp::GtEq),
        _ => None,
    };

    if let Some(op) = op {
        lexer.next_token()?;
        let right = parse_addition(lexer)?;
        Ok(Expr::Comparison {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_addition(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_multiplication(lexer)?;
    loop {
        let tok = lexer.peek()?;
        let op = match &tok.kind {
            TokenKind::Plus => Some(ArithOp::Add),
            TokenKind::Minus => Some(ArithOp::Sub),
            _ => None,
        };
        if let Some(op) = op {
            lexer.next_token()?;
            let right = parse_multiplication(lexer)?;
            left = Expr::Arithmetic {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

fn parse_multiplication(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_unary(lexer)?;
    loop {
        let tok = lexer.peek()?;
        let op = match &tok.kind {
            TokenKind::Star => Some(ArithOp::Mul),
            TokenKind::Slash => Some(ArithOp::Div),
            _ => None,
        };
        if let Some(op) = op {
            lexer.next_token()?;
            let right = parse_unary(lexer)?;
            left = Expr::Arithmetic {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

fn parse_unary(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    if lexer.eat(&TokenKind::Minus)? {
        let inner = parse_unary(lexer)?;
        // Negate: wrap as 0 - inner.
        Ok(Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Integer(0))),
            op: ArithOp::Sub,
            right: Box::new(inner),
        })
    } else {
        parse_primary(lexer)
    }
}

fn parse_primary(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Integer(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::Integer(v) = tok.kind {
                Ok(Expr::Literal(Literal::Integer(v)))
            } else {
                unreachable!()
            }
        }
        TokenKind::Float(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::Float(v) = tok.kind {
                Ok(Expr::Literal(Literal::Float(v)))
            } else {
                unreachable!()
            }
        }
        TokenKind::StringLit(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::StringLit(s) = tok.kind {
                Ok(Expr::Literal(Literal::String(s)))
            } else {
                unreachable!()
            }
        }
        TokenKind::Keyword(Keyword::True) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Bool(true)))
        }
        TokenKind::Keyword(Keyword::False) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Bool(false)))
        }
        TokenKind::Keyword(Keyword::Null) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Null))
        }
        TokenKind::LParen => {
            lexer.next_token()?;
            let expr = parse_expr(lexer)?;
            lexer.expect(&TokenKind::RParen)?;
            Ok(expr)
        }
        TokenKind::Ident(_) => {
            let tok = lexer.next_token()?;
            let name = if let TokenKind::Ident(s) = tok.kind {
                s.to_string()
            } else {
                unreachable!()
            };

            // Check for property access (var.prop) or function call (fn(...)).
            let next = lexer.peek()?;
            match &next.kind {
                TokenKind::Dot => {
                    lexer.next_token()?;
                    let prop_tok = lexer.next_token()?;
                    let prop = match prop_tok.kind {
                        TokenKind::Ident(p) => p.to_string(),
                        _ => {
                            return Err(ParseError::new(
                                format!("expected property name after '.', got {:?}", prop_tok.kind),
                                prop_tok.span,
                            ));
                        }
                    };
                    Ok(Expr::Property { var: name, name: prop })
                }
                TokenKind::LParen => {
                    lexer.next_token()?;
                    let mut args = vec![];
                    if lexer.peek()?.kind != TokenKind::RParen {
                        loop {
                            args.push(parse_expr(lexer)?);
                            if !lexer.eat(&TokenKind::Comma)? {
                                break;
                            }
                        }
                    }
                    lexer.expect(&TokenKind::RParen)?;
                    Ok(Expr::Function { name, args })
                }
                _ => Ok(Expr::Var(name)),
            }
        }
        _ => Err(ParseError::new(
            format!("expected expression, got {:?}", tok.kind),
            tok.span,
        )),
    }
}

// --- RETURN clause ---

fn parse_return_clause(lexer: &mut Lexer<'_>) -> ParseResult<ReturnClause> {
    lexer.expect(&TokenKind::Keyword(Keyword::Return))?;

    let distinct = lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;

    let mut items = vec![parse_return_item(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        items.push(parse_return_item(lexer)?);
    }

    let order_by = if lexer.eat(&TokenKind::Keyword(Keyword::Order))? {
        lexer.expect(&TokenKind::Keyword(Keyword::By))?;
        let mut orders = vec![parse_order_item(lexer)?];
        while lexer.eat(&TokenKind::Comma)? {
            orders.push(parse_order_item(lexer)?);
        }
        orders
    } else {
        vec![]
    };

    let limit = if lexer.eat(&TokenKind::Keyword(Keyword::Limit))? {
        let tok = lexer.next_token()?;
        match tok.kind {
            TokenKind::Integer(v) => Some(v),
            _ => {
                return Err(ParseError::new(
                    format!("expected integer after LIMIT, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        }
    } else {
        None
    };

    Ok(ReturnClause { distinct, items, order_by, limit })
}

fn parse_return_item(lexer: &mut Lexer<'_>) -> ParseResult<ReturnItem> {
    let expr = parse_expr(lexer)?;
    let alias = if lexer.eat(&TokenKind::Keyword(Keyword::As))? {
        let tok = lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(name) => Some(name.to_string()),
            _ => {
                return Err(ParseError::new(
                    format!("expected alias name after AS, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        }
    } else {
        None
    };
    Ok(ReturnItem { expr, alias })
}

fn parse_order_item(lexer: &mut Lexer<'_>) -> ParseResult<OrderItem> {
    let expr = parse_expr(lexer)?;
    let direction = if lexer.eat(&TokenKind::Keyword(Keyword::Desc))? {
        SortDir::Desc
    } else {
        lexer.eat(&TokenKind::Keyword(Keyword::Asc))?;
        SortDir::Asc
    };
    Ok(OrderItem { expr, direction })
}

// --- SET and DELETE helpers ---

fn parse_assignment_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<Assignment>> {
    let mut assignments = vec![parse_assignment(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        assignments.push(parse_assignment(lexer)?);
    }
    Ok(assignments)
}

fn parse_assignment(lexer: &mut Lexer<'_>) -> ParseResult<Assignment> {
    let tok = lexer.next_token()?;
    let var = match tok.kind {
        TokenKind::Ident(name) => name.to_string(),
        _ => {
            return Err(ParseError::new(
                format!("expected variable name in SET, got {:?}", tok.kind),
                tok.span,
            ));
        }
    };
    lexer.expect(&TokenKind::Dot)?;
    let prop_tok = lexer.next_token()?;
    let property = match prop_tok.kind {
        TokenKind::Ident(name) => name.to_string(),
        _ => {
            return Err(ParseError::new(
                format!("expected property name after '.', got {:?}", prop_tok.kind),
                prop_tok.span,
            ));
        }
    };
    lexer.expect(&TokenKind::Eq)?;
    let value = parse_expr(lexer)?;
    Ok(Assignment { var, property, value })
}

fn parse_var_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<String>> {
    let mut vars = vec![];
    let tok = lexer.next_token()?;
    match tok.kind {
        TokenKind::Ident(name) => vars.push(name.to_string()),
        _ => {
            return Err(ParseError::new(
                format!("expected variable name in DELETE, got {:?}", tok.kind),
                tok.span,
            ));
        }
    }
    while lexer.eat(&TokenKind::Comma)? {
        let tok = lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(name) => vars.push(name.to_string()),
            _ => {
                return Err(ParseError::new(
                    format!("expected variable name in DELETE, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        }
    }
    Ok(vars)
}

#[cfg(test)]
mod tests {
    // Tests follow in subsequent steps.
}
```

- [ ] **Step 4: Add parser module to mod.rs**

Add `mod parser;` (private — the `parse()` function is re-exported from `mod.rs`) and update mod.rs:

```rust
pub mod ast;
mod error;
pub mod lexer;
mod parser;
pub mod token;

pub use error::{ParseError, ParseResult, Span};
pub use lexer::Lexer;
pub use parser::parse;
pub use token::{Keyword, Token, TokenKind};
```

- [ ] **Step 5: Build to verify**

Run: `cargo build -p ferrosa-graph`
Expected: Compiles with no errors (the `input` field needs to be `pub(crate)` in `Lexer` — fix if needed).

- [ ] **Step 6: Add comprehensive parser tests**

Replace the `#[cfg(test)]` block at the bottom of `parser.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;

    // --- Node patterns ---

    #[test]
    fn parse_empty_node() {
        let mut lexer = Lexer::new("()");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(node, Pattern::Node { var: None, label: None, props: vec![] });
    }

    #[test]
    fn parse_node_with_var() {
        let mut lexer = Lexer::new("(n)");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(node, Pattern::Node { var: Some("n".into()), label: None, props: vec![] });
    }

    #[test]
    fn parse_node_with_label() {
        let mut lexer = Lexer::new("(n:Person)");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(node, Pattern::Node {
            var: Some("n".into()),
            label: Some("Person".into()),
            props: vec![],
        });
    }

    #[test]
    fn parse_node_with_props() {
        let mut lexer = Lexer::new("(n:Person {name: 'Alice', age: 30})");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(node, Pattern::Node {
            var: Some("n".into()),
            label: Some("Person".into()),
            props: vec![
                ("name".into(), Expr::Literal(Literal::String("Alice".into()))),
                ("age".into(), Expr::Literal(Literal::Integer(30))),
            ],
        });
    }

    // --- Relationship patterns ---

    #[test]
    fn parse_outgoing_rel() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 1);
            if let Pattern::Path(elements) = &pattern[0] {
                assert_eq!(elements.len(), 3); // Node, Rel, Node
                assert!(matches!(&elements[1], Pattern::Rel { direction: Direction::Out, .. }));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_incoming_rel() {
        let stmt = parse("MATCH (a)<-[:KNOWS]-(b) RETURN a").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elements) = &pattern[0] {
                assert!(matches!(&elements[1], Pattern::Rel { direction: Direction::In, .. }));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- Full MATCH statements ---

    #[test]
    fn parse_match_with_where() {
        let stmt = parse("MATCH (a:Person) WHERE a.age > 30 RETURN a.name").unwrap();
        if let Statement::Match { where_clause, .. } = stmt {
            assert!(where_clause.is_some());
            let wc = where_clause.unwrap();
            assert!(matches!(wc, Expr::Comparison { op: CompareOp::Gt, .. }));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_with_order_limit() {
        let stmt = parse("MATCH (a:Person) RETURN a.name ORDER BY a.age DESC LIMIT 10").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.order_by.len(), 1);
            assert_eq!(return_clause.order_by[0].direction, SortDir::Desc);
            assert_eq!(return_clause.limit, Some(10));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_return_distinct() {
        let stmt = parse("MATCH (a) RETURN DISTINCT a.name").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert!(return_clause.distinct);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_return_alias() {
        let stmt = parse("MATCH (a) RETURN a.name AS person_name").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.items[0].alias, Some("person_name".into()));
        } else {
            panic!("expected Match");
        }
    }

    // --- CREATE ---

    #[test]
    fn parse_create_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(matches!(stmt, Statement::Create { .. }));
    }

    #[test]
    fn parse_create_edge() {
        let stmt = parse("CREATE (a)-[:KNOWS {since: 2020}]->(b)").unwrap();
        if let Statement::Create { patterns } = stmt {
            assert!(matches!(&patterns[0], Pattern::Path(_)));
        } else {
            panic!("expected Create");
        }
    }

    // --- SET ---

    #[test]
    fn parse_set() {
        let stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31").unwrap();
        if let Statement::Set { assignments, .. } = stmt {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].var, "n");
            assert_eq!(assignments[0].property, "age");
        } else {
            panic!("expected Set");
        }
    }

    // --- DELETE ---

    #[test]
    fn parse_delete() {
        let stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' DELETE n").unwrap();
        if let Statement::Delete { detach, variables, .. } = stmt {
            assert!(!detach);
            assert_eq!(variables, vec!["n"]);
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn parse_detach_delete() {
        let stmt = parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        if let Statement::Delete { detach, .. } = stmt {
            assert!(detach);
        } else {
            panic!("expected Delete");
        }
    }

    // --- Expressions ---

    #[test]
    fn parse_boolean_logic() {
        let stmt = parse("MATCH (a) WHERE a.x = 1 AND a.y = 2 OR a.z = 3 RETURN a").unwrap();
        if let Statement::Match { where_clause: Some(expr), .. } = stmt {
            // OR has lower precedence, so top-level is Or.
            assert!(matches!(expr, Expr::Or(_, _)));
        } else {
            panic!("expected Match with where");
        }
    }

    #[test]
    fn parse_function_call() {
        let stmt = parse("MATCH (a) RETURN count(a)").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert!(matches!(&return_clause.items[0].expr, Expr::Function { name, .. } if name == "count"));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_is_null() {
        let stmt = parse("MATCH (a) WHERE a.name IS NULL RETURN a").unwrap();
        if let Statement::Match { where_clause: Some(expr), .. } = stmt {
            assert!(matches!(expr, Expr::IsNull(_)));
        } else {
            panic!("expected Match with IS NULL");
        }
    }

    #[test]
    fn parse_is_not_null() {
        let stmt = parse("MATCH (a) WHERE a.name IS NOT NULL RETURN a").unwrap();
        if let Statement::Match { where_clause: Some(expr), .. } = stmt {
            assert!(matches!(expr, Expr::IsNotNull(_)));
        } else {
            panic!("expected Match with IS NOT NULL");
        }
    }

    // --- Multi-hop path ---

    #[test]
    fn parse_multi_hop_path() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN c.name").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                assert_eq!(elems.len(), 5); // Node, Rel, Node, Rel, Node
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- Error cases ---

    #[test]
    fn parse_error_missing_return() {
        let err = parse("MATCH (a)").unwrap_err();
        assert!(err.message.contains("expected"));
    }

    #[test]
    fn parse_error_unclosed_node() {
        let err = parse("MATCH (a RETURN a").unwrap_err();
        assert!(err.message.contains("expected"));
    }

    // --- Additional coverage (from review) ---

    #[test]
    fn parse_multiple_patterns() {
        let stmt = parse("MATCH (a:Person), (b:Company) RETURN a, b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 2);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_multiple_set_assignments() {
        let stmt = parse("MATCH (n) SET n.x = 1, n.y = 2").unwrap();
        if let Statement::Set { assignments, .. } = stmt {
            assert_eq!(assignments.len(), 2);
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn parse_multiple_delete_vars() {
        let stmt = parse("MATCH (a)-[r]->(b) DELETE a, r, b").unwrap();
        if let Statement::Delete { variables, .. } = stmt {
            assert_eq!(variables, vec!["a", "r", "b"]);
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn parse_undirected_relationship() {
        let stmt = parse("MATCH (a)-[:KNOWS]-(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                assert!(matches!(&elems[1], Pattern::Rel { direction: Direction::Both, .. }));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_named_relationship() {
        let stmt = parse("MATCH (a)-[r:KNOWS]->(b) RETURN r").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                if let Pattern::Rel { var, rel_type, .. } = &elems[1] {
                    assert_eq!(var, &Some("r".into()));
                    assert_eq!(rel_type, &Some("KNOWS".into()));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_incoming_relationship_full() {
        // <-[:KNOWS]- requires standalone [ after <-
        let stmt = parse("MATCH (a)<-[:KNOWS]-(b) RETURN a").unwrap();
        assert!(matches!(stmt, Statement::Match { .. }));
    }

    #[test]
    fn parse_empty_prop_map() {
        let stmt = parse("CREATE (n:Person {})").unwrap();
        if let Statement::Create { patterns } = stmt {
            if let Pattern::Node { props, .. } = &patterns[0] {
                assert!(props.is_empty());
            } else {
                panic!("expected Node");
            }
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_multiple_return_items() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name, a.age").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.items.len(), 3);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_nested_parens_in_expr() {
        let stmt = parse("MATCH (a) WHERE (a.x + 1) * 2 > 10 RETURN a").unwrap();
        assert!(matches!(stmt, Statement::Match { where_clause: Some(_), .. }));
    }

    #[test]
    fn parse_float_in_where() {
        let stmt = parse("MATCH (a) WHERE a.score > 3.14 RETURN a").unwrap();
        assert!(matches!(stmt, Statement::Match { where_clause: Some(_), .. }));
    }
}
```

- [ ] **Step 7: Run all tests**

Run: `cargo test -p ferrosa-graph`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add ferrosa-graph/src/parser/
git commit -m "feat(graph): add recursive-descent Cypher parser with expression evaluation"
```

---

## Chunk 4: Property Tests and Clippy

### Task 5: Property-based fuzz tests for parser safety

**Files:**

- Create: `ferrosa-graph/tests/parser_proptest.rs`

- [ ] **Step 1: Write proptest that arbitrary strings never panic**

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn parser_never_panics(input in "\\PC{0,200}") {
        // Parser should return Ok or Err, never panic.
        let _ = ferrosa_graph::parser::parse(&input);
    }
}
```

- [ ] **Step 2: Run proptest**

Run: `cargo test -p ferrosa-graph --test parser_proptest`
Expected: Pass (may find edge cases — fix any panics discovered).

- [ ] **Step 3: Write proptest that valid tokens never panic the lexer**

Add to the same file:

```rust
proptest! {
    #[test]
    fn lexer_never_panics(input in "\\PC{0,200}") {
        let mut lexer = ferrosa_graph::parser::Lexer::new(&input);
        loop {
            match lexer.next_token() {
                Ok(tok) => {
                    if tok.kind == ferrosa_graph::parser::TokenKind::Eof {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
}
```

- [ ] **Step 4: Run all tests**

Run: `cargo test -p ferrosa-graph`
Expected: All tests pass.

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p ferrosa-graph --all-targets`
Expected: No warnings. Fix any that appear.

- [ ] **Step 6: Run fmt check**

Run: `cargo fmt -p ferrosa-graph --check`
Expected: No formatting issues. Run `cargo fmt -p ferrosa-graph` if needed.

- [ ] **Step 7: Commit**

```bash
git add ferrosa-graph/tests/ ferrosa-graph/src/
git commit -m "test(graph): add property tests for parser and lexer safety"
```

---

## Execution Notes

**Working directory:** All commands run from `.worktrees/graph-parser/`.

**Build tool:** Use `mcp__skilltools__cargo` (not raw cargo via Bash) per project conventions.

**No dependencies on other ferrosa crates:** The parser is standalone. `ferrosa-common`, `ferrosa-schema`, and `ferrosa-storage` are not needed until the planner/executor phases.

**The `Lexer.input` field:** The parser module needs access to `lexer.input` for the `parse_label` function (to extract keyword text). Make the field `pub(crate)` in the Lexer struct.

**Known limitations (Phase 1 parser):**

- No backtick-quoted identifiers (`` `reserved` `` syntax). Keywords-as-labels are handled via `parse_label`.
- No `RETURN *` (wildcard return). Deferred to later phase.
- String literals are ASCII-only in Phase 1. Multi-byte UTF-8 within `'...'` may produce incorrect output.
- No `OPTIONAL MATCH`, `UNION`, `WITH`, `UNWIND`, variable-length paths (`[:KNOWS*1..3]`), or aggregations.

**After completion:** The ferrosa-graph crate exists with a working Cypher parser. The next implementation plan (after CQL merge) would cover Phase 0 hooks + Phase 1 planner/executor/HTTP/adjacency.
