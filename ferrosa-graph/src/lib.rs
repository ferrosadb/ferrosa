//! # ferrosa-graph
//!
//! Graph query engine for ferrosa. Provides a Cypher/GQL query endpoint
//! alongside CQL, with data stored in normal CQL tables and accessed
//! via a system-managed adjacency index.
//!
//! ## Modules
//!
//! - [`parser`] — Cypher lexer, parser, and AST types.

pub mod adjacency;
pub mod error;
pub mod parser;
