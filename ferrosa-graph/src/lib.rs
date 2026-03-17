//! # ferrosa-graph
//!
//! Graph query engine for ferrosa. Provides a Cypher/GQL query endpoint
//! alongside CQL, with data stored in normal CQL tables and accessed
//! via a system-managed adjacency index.
//!
//! ## Modules
//!
//! - [`parser`] — Cypher lexer, parser, and AST types.
//! - [`error`] — Graph engine error types.
//! - [`adjacency`] — Adjacency index schema and observer.
//! - [`planner`] — Logical and physical query planners.
//! - [`executor`] — Query execution engine.
//! - [`engine`] — GraphEngine composition type.
//! - [`http`] — HTTP/JSON endpoint.
//! - [`bolt`] — Bolt v5 wire protocol for Neo4j driver compatibility.

pub mod adjacency;
pub mod bolt;
pub mod engine;
pub mod error;
pub mod executor;
pub mod http;
pub mod parser;
pub mod planner;
