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
    Remove,
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
    Subscribe,
    Unsubscribe,
    Every,
    Delta,
    Merge,
    Optional,
    With,
    Union,
    All,
    Unwind,
    Exists,
    In,
    Call,
    Foreach,
    Load,
    Csv,
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
    /// Query parameter: `$name`.
    Parameter(&'input str),

    // Punctuation
    LParen,   // (
    RParen,   // )
    LBracket, // [
    RBracket, // ]
    LBrace,   // {
    RBrace,   // }
    Colon,    // :
    Dot,      // .
    Comma,    // ,
    Eq,       // =
    Neq,      // <>
    Lt,       // <
    Gt,       // >
    LtEq,     // <=
    GtEq,     // >=
    Plus,     // +
    Minus,    // -
    Star,     // *
    Slash,    // /
    Pipe,     // |

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
