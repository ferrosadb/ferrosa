//! Adjacency index: per-keyspace system table for fast graph traversals.

pub mod observer;
pub mod reconcile;
pub mod schema;

pub use schema::{adjacency_keyspace_name, adjacency_table_metadata};
