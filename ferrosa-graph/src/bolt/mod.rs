//! Bolt v5 wire protocol implementation for Neo4j driver compatibility.
//!
//! This module implements the Bolt protocol used by Neo4j drivers to
//! communicate with graph databases. It supports:
//!
//! - **PackStream** binary serialization ([`codec`])
//! - **Chunked framing** for message transport ([`codec`])
//! - **Bolt message types** for client/server communication ([`message`])
//! - **Version negotiation** handshake ([`handshake`])
//! - **Server** connection handler ([`server`])

pub mod codec;
pub mod handshake;
pub mod message;
pub mod server;
