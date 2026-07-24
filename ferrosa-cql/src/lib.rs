//! CQL native protocol v5 for Ferrosa.
//!
//! This crate implements the binary framing, type system, parser, query
//! routing, and TCP server for CQL protocol v5 — the client-facing
//! interface to the Ferrosa database.
//!
//! # Architecture
//!
//! Each TCP connection gets its own Tokio task. The task owns a
//! `Framed<TcpStream, CqlCodec>` for zero-copy frame encoding/decoding,
//! an `AuthContext` after authentication, and a reference to the shared
//! `ArcSwap<SchemaSnapshot>` for lock-free schema reads.
//!
//! All hot paths are lock-free: schema lookups use `ArcSwap::load()`,
//! prepared statement cache uses `moka` (W-TinyLFU), and storage
//! access goes through `Arc<StorageEngine>`.

pub mod accord_router;
pub mod ast;
pub mod auth;
pub mod bridge;
pub mod client;
pub mod collection_cells;
pub mod connection;
pub mod duration;
pub mod error;
pub mod event;
pub mod frame;
pub mod lexer;
pub mod observability;
pub mod paging;
pub mod param_cache;
pub mod parser;
pub mod planner;
pub mod prepared;
pub mod prometheus;
pub mod result;
pub mod router;
pub mod server;
pub mod session;
pub mod subscribe;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
pub mod topology;
pub mod transaction_keys;
pub mod transaction_limits;
pub mod txn_registry;
pub mod types;
pub mod virtual_tables;
pub mod wasm_aggregate;
