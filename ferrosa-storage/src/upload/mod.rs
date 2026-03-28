//! S3-compatible upload manager using the `object_store` crate.
//!
//! Uploads SSTable component files to S3-compatible storage (AWS S3, MinIO,
//! Cloudflare R2, Ceph). Configuration is 12-factor: all settings from
//! environment variables.

pub mod config;
pub mod manager;
pub mod pending_log;

pub use config::{validate_s3_bucket, ObjectStoreConfig};
pub use manager::{UploadManager, UploadTask};
pub use pending_log::PendingUploadsLog;
