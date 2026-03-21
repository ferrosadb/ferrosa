//! Configuration for the commit log.
//!
//! [`CommitLogConfig`] collects all tunables: segment size, rotation age,
//! sync strategy, and directory paths. [`SyncStrategyConfig`] selects
//! which [`SyncStrategy`](super::sync::SyncStrategy) to instantiate.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Position in the commit log: segment ID + byte offset.
///
/// Ordered first by segment_id, then by offset. Used to track how
/// far each table has been flushed so old segments can be deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommitLogPosition {
    pub segment_id: u64,
    pub offset: u64,
}

/// Identifies a table for flush tracking.
///
/// Two tables are considered the same if both keyspace and table name match.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId {
    pub keyspace: String,
    pub table: String,
}

impl TableId {
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }

    /// Returns the keyspace name.
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Returns the table name.
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.keyspace, self.table)
    }
}

/// Sync strategy selection.
///
/// | Strategy | Throughput | Latency | Durability Window |
/// |----------|-----------|---------|-------------------|
/// | Periodic | Highest | Lowest | Up to sync_interval |
/// | Batch | Lowest | Highest | Zero |
/// | Group | Good | Bounded | Up to max_wait |
#[derive(Debug, Clone)]
pub enum SyncStrategyConfig {
    /// Fsync on a timer. Best throughput, small durability window.
    Periodic {
        /// Interval between fsyncs (default 10ms).
        sync_interval: Duration,
    },
    /// Fsync per write. Zero data loss, highest latency.
    Batch,
    /// Fsync batches of writes. Bounded latency, good throughput.
    Group {
        /// Max time to wait before fsyncing a batch (default 1ms).
        max_wait: Duration,
    },
}

impl Default for SyncStrategyConfig {
    fn default() -> Self {
        SyncStrategyConfig::Periodic {
            sync_interval: Duration::from_millis(10),
        }
    }
}

/// Default archive poll interval: 5 seconds.
pub const DEFAULT_ARCHIVE_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Default archive retention: 7 days.
pub const DEFAULT_ARCHIVE_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// Configuration for commit log archiving to S3.
///
/// When enabled, closed commit log segments are uploaded to S3 for
/// point-in-time recovery. Disabled by default.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// Whether archiving is enabled.
    pub enabled: bool,
    /// How often the archiver polls for new closed segments.
    pub poll_interval: Duration,
    /// How long archived segments are retained in S3.
    pub retention: Duration,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval: DEFAULT_ARCHIVE_POLL_INTERVAL,
            retention: DEFAULT_ARCHIVE_RETENTION,
        }
    }
}

impl ArchiveConfig {
    /// Reads archive configuration from `FERROSA_ARCHIVE_*` environment variables.
    ///
    /// - `FERROSA_ARCHIVE_ENABLED` — `true` to enable (default: `false`)
    /// - `FERROSA_ARCHIVE_POLL_INTERVAL_SECS` — seconds (default: 5)
    /// - `FERROSA_ARCHIVE_RETENTION_DAYS` — days (default: 7)
    pub fn from_env() -> Self {
        let enabled = std::env::var("FERROSA_ARCHIVE_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(false);

        let poll_interval = std::env::var("FERROSA_ARCHIVE_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_ARCHIVE_POLL_INTERVAL);

        let retention = std::env::var("FERROSA_ARCHIVE_RETENTION_DAYS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|days| Duration::from_secs(days * 24 * 3600))
            .unwrap_or(DEFAULT_ARCHIVE_RETENTION);

        Self {
            enabled,
            poll_interval,
            retention,
        }
    }
}

/// Default segment size: 32 MB.
pub const DEFAULT_SEGMENT_SIZE: usize = 32 * 1024 * 1024;

/// Default max segment age before rotation: 5 minutes.
pub const DEFAULT_MAX_SEGMENT_AGE: Duration = Duration::from_secs(300);

/// Commit log configuration.
///
/// All sizes are configurable. Defaults are suitable for general workloads:
/// - 32 MB segments with 5-minute max age
/// - Periodic sync every 10ms (best throughput, up to 10ms data loss on crash)
#[derive(Debug, Clone)]
pub struct CommitLogConfig {
    /// Segment size in bytes (default 32 MB).
    pub segment_size: usize,
    /// Maximum segment age before rotation (default 5 minutes).
    pub max_segment_age: Duration,
    /// Sync strategy selection.
    pub sync_strategy: SyncStrategyConfig,
    /// Directory for commit log segment files.
    pub log_dir: PathBuf,
    /// Directory for checkpoint file (may be same as log_dir).
    pub checkpoint_dir: PathBuf,
    /// Optional commit log archiving configuration.
    pub archive: Option<ArchiveConfig>,
}

impl CommitLogConfig {
    /// Create a config for testing with small segments and a temp directory.
    #[cfg(test)]
    pub fn test_config(dir: &std::path::Path) -> Self {
        Self {
            segment_size: 4096, // 4 KB for fast rotation in tests
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch, // immediate fsync for deterministic tests
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
        }
    }
}

impl Default for CommitLogConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_segment_age: DEFAULT_MAX_SEGMENT_AGE,
            sync_strategy: SyncStrategyConfig::default(),
            log_dir: PathBuf::from("/var/lib/ferrosa/commitlog"),
            checkpoint_dir: PathBuf::from("/var/lib/ferrosa/commitlog"),
            archive: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = CommitLogConfig::default();
        assert_eq!(config.segment_size, 32 * 1024 * 1024);
        assert_eq!(config.max_segment_age, Duration::from_secs(300));
        assert!(matches!(
            config.sync_strategy,
            SyncStrategyConfig::Periodic { sync_interval }
            if sync_interval == Duration::from_millis(10)
        ));
    }

    #[test]
    fn commit_log_position_ordering() {
        let a = CommitLogPosition {
            segment_id: 1,
            offset: 100,
        };
        let b = CommitLogPosition {
            segment_id: 1,
            offset: 200,
        };
        let c = CommitLogPosition {
            segment_id: 2,
            offset: 50,
        };
        assert!(a < b);
        assert!(b < c); // segment_id takes precedence
    }

    #[test]
    fn table_id_equality() {
        let a = TableId::new("ks1", "users");
        let b = TableId::new("ks1", "users");
        let c = TableId::new("ks1", "orders");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn table_id_display() {
        let id = TableId::new("my_ks", "my_table");
        assert_eq!(format!("{id}"), "my_ks.my_table");
    }

    #[test]
    fn sync_strategy_default_is_periodic() {
        let strategy = SyncStrategyConfig::default();
        assert!(matches!(strategy, SyncStrategyConfig::Periodic { .. }));
    }

    #[test]
    fn archive_config_defaults() {
        let config = ArchiveConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.retention, Duration::from_secs(7 * 24 * 3600));
    }

    #[test]
    fn commit_log_config_archive_none_by_default() {
        let config = CommitLogConfig::default();
        assert!(config.archive.is_none());
    }

    #[test]
    fn archive_config_from_env_defaults() {
        // No env vars set — should return default (disabled).
        // Clear any stale env to be safe.
        std::env::remove_var("FERROSA_ARCHIVE_ENABLED");
        std::env::remove_var("FERROSA_ARCHIVE_POLL_INTERVAL_SECS");
        std::env::remove_var("FERROSA_ARCHIVE_RETENTION_DAYS");
        let config = ArchiveConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.retention, Duration::from_secs(7 * 24 * 3600));
    }
}
