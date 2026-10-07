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
/// | Strategy | Throughput | Latency | Acked-but-unsynced window |
/// |----------|-----------|---------|---------------------------|
/// | Periodic | Highest | Lowest | `max_delay` healthy; at most `sync_stall_deadline` |
/// | Batch | Lowest | Highest | Zero |
/// | Group | Good | Bounded | Zero (the writer waits, then fails) |
///
/// Production runs Periodic (the default). See `sync` for how the bound is
/// enforced: writes are refused, not acknowledged, once sync falls behind.
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

/// Which system call makes a commit-log segment durable.
///
/// This only matters on macOS. There, `fsync(2)` hands data to the drive but
/// does not flush the drive's volatile cache, so it survives a process crash
/// and a kernel panic (the controller keeps power and drains its cache) but
/// not a power loss. On Linux `fdatasync(2)` already flushes the device cache,
/// so every mode runs `fdatasync` there: a mode never makes Linux weaker.
///
/// | Mode | macOS call | Process crash | Kernel panic | Power loss |
/// |------|------------|---------------|--------------|------------|
/// | `Full` (default) | `fcntl(F_FULLFSYNC)` | survives | survives | survives |
/// | `Barrier` | `fcntl(F_BARRIERFSYNC)` | survives | survives | may lose the tail since the last full flush; never reorders it |
/// | `Fsync` | `fsync(2)` | survives | survives | may lose or reorder recent writes |
///
/// Note that Rust's `File::sync_all` and `File::sync_data` are both
/// `F_FULLFSYNC` on Apple targets, so `Fsync` has to call `libc::fsync`
/// itself. Selected at startup by `FERROSA_COMMITLOG_SYNC_MODE`, logged then,
/// and exported as `ferrosa_commitlog_sync_mode`; never a build-time choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitLogSyncMode {
    /// Flush the drive cache on every sync. Power-loss durable.
    #[default]
    Full,
    /// fsync plus a drive write barrier: ordered, not power-loss durable.
    Barrier,
    /// Plain fsync: neither ordered nor durable across power loss.
    Fsync,
}

impl CommitLogSyncMode {
    /// The environment variable that selects the mode.
    pub const ENV: &'static str = "FERROSA_COMMITLOG_SYNC_MODE";

    const ALL: [Self; 3] = [Self::Full, Self::Barrier, Self::Fsync];

    /// The name accepted by [`parse`](Self::parse).
    pub fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Barrier => "barrier",
            Self::Fsync => "fsync",
        }
    }

    /// Parses a mode name, ignoring case and surrounding whitespace.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let wanted = raw.trim();
        Self::ALL
            .into_iter()
            .find(|mode| mode.name().eq_ignore_ascii_case(wanted))
            .ok_or_else(|| {
                format!(
                    "unknown commit-log sync mode {raw:?}; expected one of full, barrier, fsync"
                )
            })
    }

    /// Reads [`ENV`](Self::ENV). Unset means `Full`. A set but unknown value
    /// is an error, not the default: a durability setting that was asked for
    /// and silently not applied is the failure this type exists to prevent.
    pub fn from_env() -> Result<Self, String> {
        match std::env::var(Self::ENV) {
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(e) => Err(format!("{}: {e}", Self::ENV)),
            Ok(raw) => Self::parse(&raw).map_err(|e| format!("{}: {e}", Self::ENV)),
        }
    }

    /// Whether an acknowledged, synced write survives sudden power loss. Off
    /// Apple targets every mode is `fdatasync(2)`, which flushes the device cache.
    pub fn survives_power_loss(self) -> bool {
        !cfg!(target_vendor = "apple") || matches!(self, Self::Full)
    }

    /// One line, for the startup log, saying what this mode guarantees on
    /// this platform. It never says "panic": the install smoke fails any
    /// startup log that does (see `no_durability_line_mentions_a_panic`).
    #[cfg(target_vendor = "apple")]
    pub fn durability(self) -> &'static str {
        match self {
            Self::Full => {
                "F_FULLFSYNC: synced writes survive process crash, OS crash and power loss"
            }
            Self::Barrier => {
                "F_BARRIERFSYNC: synced writes survive process crash and OS crash; on power loss \
                 the tail since the drive last flushed its cache can be lost, in order"
            }
            Self::Fsync => {
                "fsync(2): synced writes survive process crash and OS crash; on power loss \
                 recent writes can be lost or reordered"
            }
        }
    }

    /// One line, for the startup log, saying what this mode guarantees on
    /// this platform. It never says "panic": the install smoke fails any
    /// startup log that does (see `no_durability_line_mentions_a_panic`).
    #[cfg(not(target_vendor = "apple"))]
    pub fn durability(self) -> &'static str {
        "fdatasync(2): synced writes survive process crash, OS crash and power loss \
         (the mode chooses the sync call only on Apple targets)"
    }
}

/// Adaptive commit-log sync batching tunables.
///
/// The sync strategy opens a batch when the first dirty write arrives. Under
/// low load it flushes after `max_delay`; under high load it can flush earlier
/// once accumulated WAL bytes reach `target_bytes`.
#[derive(Debug, Clone)]
pub struct CommitLogBatchConfig {
    /// Flush as soon as pending WAL bytes reach this target.
    pub target_bytes: u64,
    /// Maximum time to hold a dirty batch open.
    pub max_delay: Duration,
    /// How long the oldest unsynced write may wait for an fsync before the
    /// commit log refuses new writes (Periodic) or fails the waiting writer
    /// (Group). This bounds the ack-before-fsync window; see `sync`.
    pub sync_stall_deadline: Duration,
    /// The system call each sync issues. See [`CommitLogSyncMode`].
    pub sync_mode: CommitLogSyncMode,
}

impl CommitLogBatchConfig {
    pub const DEFAULT_TARGET_BYTES: u64 = 64 * 1024;

    /// 200 periodic sync intervals. A healthy fsync takes milliseconds, even
    /// `F_FULLFSYNC` under load; two seconds without one means the sync thread
    /// is dead, wedged, or the disk is failing.
    pub const DEFAULT_SYNC_STALL_DEADLINE: Duration = Duration::from_secs(2);

    pub fn with_max_delay(max_delay: Duration) -> Self {
        Self {
            target_bytes: Self::DEFAULT_TARGET_BYTES,
            max_delay,
            sync_stall_deadline: Self::DEFAULT_SYNC_STALL_DEADLINE,
            sync_mode: CommitLogSyncMode::default(),
        }
    }

    /// Read `FERROSA_COMMITLOG_BATCH_TARGET_BYTES`,
    /// `FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS` and
    /// `FERROSA_COMMITLOG_SYNC_STALL_DEADLINE_MS`. A set but unusable value
    /// is reported and replaced by the default; it used to be dropped silently.
    pub fn from_env(default: Self) -> Self {
        let target_bytes = env_u64("FERROSA_COMMITLOG_BATCH_TARGET_BYTES")
            .filter(|v| *v > 0)
            .unwrap_or(default.target_bytes);
        let max_delay = env_u64("FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS")
            .map(Duration::from_micros)
            .unwrap_or(default.max_delay);
        let sync_stall_deadline = env_u64("FERROSA_COMMITLOG_SYNC_STALL_DEADLINE_MS")
            .filter(|v| *v > 0)
            .map(Duration::from_millis)
            .unwrap_or(default.sync_stall_deadline);
        // `sync_mode` is not read here: an unusable mode must stop startup
        // rather than fall back, so it comes from `CommitLogSyncMode::from_env`.
        Self {
            target_bytes,
            max_delay,
            sync_stall_deadline,
            sync_mode: default.sync_mode,
        }
    }
}

/// Parse `key` as a `u64`; `None` when unset or unparseable (the latter logged).
fn env_u64(key: &str) -> Option<u64> {
    let raw = std::env::var(key).ok()?;
    match raw.trim().parse::<u64>() {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::error!(key, value = %raw, %e, "unparseable commit-log setting; using the default");
            None
        }
    }
}

impl Default for CommitLogBatchConfig {
    fn default() -> Self {
        Self::with_max_delay(Duration::from_millis(10))
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
/// - Periodic sync every 10ms (best throughput). An acknowledged write is
///   fsynced within ~10ms while sync is healthy; when it is not, writes are
///   refused once the oldest unsynced write is `sync_stall_deadline` (2s) old,
///   so a crash loses at most that window of acknowledged writes.
/// - 64 KiB commit-log sync batches under sustained write load
#[derive(Debug, Clone)]
pub struct CommitLogConfig {
    /// Segment size in bytes (default 32 MB).
    pub segment_size: usize,
    /// Maximum segment age before rotation (default 5 minutes).
    pub max_segment_age: Duration,
    /// Sync strategy selection.
    pub sync_strategy: SyncStrategyConfig,
    /// Adaptive batching controls for periodic/group sync.
    pub batch: CommitLogBatchConfig,
    /// Directory for commit log segment files.
    pub log_dir: PathBuf,
    /// Directory for checkpoint file (may be same as log_dir).
    pub checkpoint_dir: PathBuf,
    /// Optional commit log archiving configuration.
    pub archive: Option<ArchiveConfig>,
}

impl CommitLogConfig {
    /// Create a config for testing with small segments and a temp directory.
    ///
    /// Not restricted to `#[cfg(test)]` so that integration-test helpers in
    /// sibling crates (e.g. `ferrosa-cql/tests/`) can construct a full
    /// `StorageEngineConfig::test_config` without duplicating every field.
    pub fn test_config(dir: &std::path::Path) -> Self {
        Self {
            segment_size: 4096, // 4 KB for fast rotation in tests
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch, // immediate fsync for deterministic tests
            batch: CommitLogBatchConfig::default(),
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
            batch: CommitLogBatchConfig::default(),
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
        assert_eq!(
            config.batch.target_bytes,
            CommitLogBatchConfig::DEFAULT_TARGET_BYTES
        );
        assert_eq!(config.batch.max_delay, Duration::from_millis(10));
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
    fn batch_config_default_targets_64k() {
        let batch = CommitLogBatchConfig::default();
        assert_eq!(batch.target_bytes, 64 * 1024);
        assert_eq!(batch.max_delay, Duration::from_millis(10));
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
    fn every_sync_mode_name_parses_and_round_trips() {
        for mode in [
            CommitLogSyncMode::Full,
            CommitLogSyncMode::Barrier,
            CommitLogSyncMode::Fsync,
        ] {
            assert_eq!(CommitLogSyncMode::parse(mode.name()), Ok(mode));
        }
        assert_eq!(
            CommitLogSyncMode::parse(" Barrier "),
            Ok(CommitLogSyncMode::Barrier)
        );
    }

    #[test]
    fn an_unknown_sync_mode_is_refused_naming_the_accepted_values() {
        let err = CommitLogSyncMode::parse("fullfsync").unwrap_err();
        assert!(err.contains("fullfsync"), "{err}");
        for accepted in ["full", "barrier", "fsync"] {
            assert!(err.contains(accepted), "{err}");
        }
    }

    #[test]
    fn the_default_sync_mode_is_the_power_loss_durable_one() {
        assert_eq!(CommitLogSyncMode::default(), CommitLogSyncMode::Full);
        assert_eq!(
            CommitLogBatchConfig::default().sync_mode,
            CommitLogSyncMode::Full
        );
        assert!(CommitLogSyncMode::Full.survives_power_loss());
    }

    /// The install smoke fails a startup log that mentions a panic
    /// (`grep -qiE "panic"`, tests/install_smoke.sh), and this line is logged
    /// at every start: "kernel panic" in it failed every install-smoke job on
    /// ferrosa#526.
    #[test]
    fn no_durability_line_mentions_a_panic() {
        for mode in CommitLogSyncMode::ALL {
            assert!(
                !mode.durability().to_ascii_lowercase().contains("panic"),
                "{mode:?}: {}",
                mode.durability()
            );
        }
    }

    /// Off Apple targets every mode runs fdatasync(2) (`segment::sync_file`), which flushes the device cache,
    /// so every mode survives power loss and the startup line says so rather
    /// than naming an Apple call this platform does not have.
    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn off_apple_every_mode_is_fdatasync_and_survives_power_loss() {
        for mode in CommitLogSyncMode::ALL {
            assert!(mode.survives_power_loss(), "{mode:?}");
            assert!(mode.durability().starts_with("fdatasync(2)"), "{mode:?}");
        }
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn weaker_sync_modes_say_they_do_not_survive_power_loss() {
        for mode in [CommitLogSyncMode::Barrier, CommitLogSyncMode::Fsync] {
            assert!(!mode.survives_power_loss(), "{mode:?}");
            assert!(
                mode.durability().contains("power loss"),
                "{mode:?}: {}",
                mode.durability()
            );
        }
    }

    #[test]
    #[serial_test::serial(env)]
    fn sync_mode_from_env_defaults_when_unset_and_refuses_garbage() {
        unsafe { std::env::remove_var("FERROSA_COMMITLOG_SYNC_MODE") };
        assert_eq!(CommitLogSyncMode::from_env(), Ok(CommitLogSyncMode::Full));

        unsafe { std::env::set_var("FERROSA_COMMITLOG_SYNC_MODE", "fsync") };
        assert_eq!(CommitLogSyncMode::from_env(), Ok(CommitLogSyncMode::Fsync));

        unsafe { std::env::set_var("FERROSA_COMMITLOG_SYNC_MODE", "fast") };
        let err = CommitLogSyncMode::from_env().unwrap_err();
        assert!(err.contains("FERROSA_COMMITLOG_SYNC_MODE"), "{err}");

        unsafe { std::env::remove_var("FERROSA_COMMITLOG_SYNC_MODE") };
    }

    #[test]
    #[serial_test::serial(env)]
    fn archive_config_from_env_defaults() {
        // No env vars set — should return default (disabled).
        // Clear any stale env to be safe.
        unsafe {
            std::env::remove_var("FERROSA_ARCHIVE_ENABLED");
            std::env::remove_var("FERROSA_ARCHIVE_POLL_INTERVAL_SECS");
            std::env::remove_var("FERROSA_ARCHIVE_RETENTION_DAYS");
        }
        let config = ArchiveConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.retention, Duration::from_secs(7 * 24 * 3600));
    }
}
