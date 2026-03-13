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

pub mod auth;
pub mod error;
pub mod frame;
pub mod types;
