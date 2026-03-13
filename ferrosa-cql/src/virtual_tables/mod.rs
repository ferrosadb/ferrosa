//! Virtual table implementations for `ferrosa-cql`.
//!
//! Each submodule provides a concrete [`VirtualTable`] implementation backed
//! by live runtime state. Tables are registered in the `VirtualTableRegistry`
//! during server startup so they are visible to CQL `SELECT` queries.

pub mod connections;

pub use connections::{ConnectionInfo, ConnectionTracker, ConnectionsTable};
