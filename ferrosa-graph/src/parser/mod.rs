//! Cypher query parser.
//!
//! Hand-rolled recursive-descent parser for an openCypher subset.
//! Zero-alloc lexer with `phf` keyword lookup, LL(2) grammar, one
//! function per production rule.

mod error;

pub use error::{ParseError, ParseResult};
