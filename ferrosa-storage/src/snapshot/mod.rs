//! Point-in-time snapshot management.
//!
//! Snapshots freeze the manifest + schema + commit log position into S3
//! without duplicating SSTable data. The snapshot manifest references the
//! same SSTable paths as the live manifest.

pub mod metadata;
