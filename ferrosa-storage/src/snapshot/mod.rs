//! PITR snapshot management: named, S3-backed snapshots of the manifest.
//!
//! Each snapshot captures:
//! - [`metadata::SnapshotMetadata`] — identity, timing, commit-log position
//! - A copy of the live `Manifest` at snapshot time
//! - A copy of the schema at snapshot time
//!
//! SSTables are **not** copied; the snapshot manifest references the same
//! S3 object paths as the live manifest.

pub mod manager;
pub mod metadata;

pub use manager::SnapshotManager;
pub use metadata::{validate_snapshot_name, SnapshotMetadata, METADATA_FORMAT_VERSION};
