//! Shared types for the Ferrosa distributed database.
//!
//! This crate provides the low-level types used across all Ferrosa crates:
//! Token, PartitionKey, DecoratedKey, CellValue, DataType, and error types.
//!
//! Scalar CQL type descriptors ([`DataType`]) live here because they are
//! needed by column definitions across multiple crates below `ferrosa-cql`.
//! Complex CQL types (collections, UDTs) and query execution live in
//! `ferrosa-cql`.

pub mod cell;
pub mod data_type;
pub mod error;
pub mod key;
pub mod murmur3;
pub mod schema;
pub mod token;

#[cfg(feature = "test-generators")]
pub mod test_generators;

pub use cell::{CellValue, Timestamp, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
pub use data_type::DataType;
pub use error::{Error, Result};
pub use key::{DecoratedKey, PartitionKey};
pub use schema::{ColumnDefinition, TableSchema};
pub use token::Token;
