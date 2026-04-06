//! SPARQL 1.1 Query and Update endpoint for Ferrosa.
//!
//! Parses SPARQL via the `spargebra` crate, translates algebra trees to
//! storage reads against ferrosa's CQL-backed triple store, and serializes
//! results via `sparesults`.

pub mod engine;
pub mod error;
pub mod executor;
pub mod http;
pub mod namespace;
pub mod planner;
pub mod results;
pub mod triple_store;
