//! Standalone index builder for Ferrosa.
//!
//! Offloads secondary index construction from the main Ferrosa engine.
//! Reads SSTable components from S3, builds sidecar index files using
//! [`ferrosa_storage::index::LocalBackend`], and writes results back to S3.
//!
//! ## Modes
//!
//! - **Push mode** (default): HTTP server receives build requests from the
//!   engine's `RemoteBackend`.
//! - **Pull mode**: Watches the engine's manifest endpoint for unindexed
//!   SSTables and builds them independently.

pub mod pull;
pub mod server;
pub mod worker;
