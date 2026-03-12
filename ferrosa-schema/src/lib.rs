//! Schema management for the Ferrosa distributed database.
//!
//! This crate is the authority for keyspaces, tables, columns, and roles.
//! Every mutating operation requires an `AuthContext` (ADR-006).
//! Every mutation emits an audit event (ADR-008).

pub mod error;
pub mod metadata;

pub use error::{Result, SchemaError};
pub use metadata::{
    ClusteringOrder, ColumnKind, ColumnMask, ColumnMetadata, KeyspaceMetadata, ReplicationParams,
};
