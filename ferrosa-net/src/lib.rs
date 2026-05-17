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
pub mod idle_timeout;
pub mod lane_actor;
pub mod message;
pub mod peer;
pub mod pool;
pub mod protocol;
pub mod reconnect;
pub mod rpc;
pub mod skew;
pub mod stream_router;
pub mod tls;
