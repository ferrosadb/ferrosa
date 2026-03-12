//! System keyspace query functions.
//!
//! Provides types and functions that mirror `system.local`, `system.peers_v2`,
//! `system_schema.*`, and `system_auth.*` virtual tables.

pub mod auth_tables;
pub mod local;
pub mod peers;
pub mod schema_tables;
