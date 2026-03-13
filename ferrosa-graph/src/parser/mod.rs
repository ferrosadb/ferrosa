//! Cypher query parser.
//!
//! Hand-rolled recursive-descent parser for an openCypher subset.
//! Zero-alloc lexer with `phf` keyword lookup, LL(2) grammar, one
//! function per production rule.

pub mod ast;
mod error;
pub mod lexer;
pub mod token;

pub use ast::*;
pub use error::{ParseError, ParseResult, Span};
pub use lexer::Lexer;
pub use token::{Keyword, Token, TokenKind};
