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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
}
