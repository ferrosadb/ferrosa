//! System keyspace query functions.
//!
//! Provides types and functions that mirror `system.local`, `system.peers_v2`,
//! `system_schema.*`, and `system_auth.*` virtual tables.

pub mod auth_tables;
pub mod index_tables;
pub mod local;
pub mod peers;
pub mod schema_tables;

pub use auth_tables::{RoleMemberRow, RolePermissionRow, RoleRow};
pub use index_tables::SystemSchemaIndexesTable;
pub use schema_tables::{ColumnRow, KeyspaceRow, TableRow};
