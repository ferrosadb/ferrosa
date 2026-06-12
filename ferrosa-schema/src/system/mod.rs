//! System keyspace query functions.
//!
//! Provides types and functions that mirror `system.local`, `system.peers_v2`,
//! `system_schema.*`, and `system_auth.*` virtual tables.

pub mod aggregate_tables;
pub mod auth_tables;
pub mod local;
pub mod observability;
pub mod peers;
pub mod persistence;
pub mod schema_tables;

/// Cassandra-compatible release version reported by `system.local` and
/// `system.peers`. This must be consistent across all code paths.
pub const RELEASE_VERSION: &str = "5.1.0-ferrosa";

/// Which address family to expose through `system.local` / `system.peers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyView {
    Public,
    Internal,
}

pub use aggregate_tables::SystemSchemaAggregatesTable;
pub use auth_tables::{RoleMemberRow, RolePermissionRow, RoleRow};
pub use persistence::all_system_table_schemas;
pub use schema_tables::{ColumnRow, KeyspaceRow, TableRow};
