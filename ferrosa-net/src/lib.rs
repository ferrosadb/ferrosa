//! Internode transport, RPC service, and failure detection for Ferrosa.
//!
//! ferrosa-net owns the wire protocol, connection pool, and peer lifecycle.
//! It is a standalone transport library with no dependency on ferrosa-common.
//! ferrosa-cluster registers message handlers and reacts to peer events.

pub mod accord_messages;
pub mod codec;
pub mod config;
pub mod discovery;
pub mod error;
pub mod handshake;
pub mod message;
pub mod peer;
pub mod pool;
pub mod reconnect;
pub mod rpc;
pub mod skew;
pub mod tls;
