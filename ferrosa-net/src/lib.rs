//! Internode transport, RPC service, and failure detection for Ferrosa.
//!
//! ferrosa-net owns the wire protocol, connection pool, and peer lifecycle.
//! It is a standalone transport library with no dependency on ferrosa-common.
//! ferrosa-cluster registers message handlers and reacts to peer events.

pub mod codec;
pub mod config;
pub mod error;
pub mod handshake;
pub mod message;
