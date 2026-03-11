//! Shared types for the Ferrosa distributed database.
//!
//! This crate provides the low-level types used across all Ferrosa crates:
//! Token, PartitionKey, DecoratedKey, CellValue, and error types.
//!
//! CQL-level type definitions (text, int, collections, UDTs) live in
//! `ferrosa-cql`, not here. Crates below `ferrosa-cql` in the dependency
//! graph work with raw bytes and cell values, not CQL-typed values.

pub mod cell;
pub mod key;
pub mod murmur3;
pub mod token;

pub use cell::{CellValue, Timestamp, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
pub use key::{DecoratedKey, PartitionKey};
pub use token::Token;
