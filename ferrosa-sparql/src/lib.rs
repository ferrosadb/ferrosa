//! SPARQL 1.1 Query and Update endpoint for Ferrosa.
//!
//! Parses SPARQL via the `spargebra` crate, translates algebra trees to
//! storage reads against ferrosa's CQL-backed triple store, and serializes
//! results via `sparesults`.

pub mod engine;
pub mod error;
pub mod executor;
pub mod filter;
pub mod http;
pub mod namespace;
pub mod planner;
pub mod property_path;
pub mod rdf_star;
pub mod results;
pub mod triple_store;
pub mod update;
