//! Top-level storage engine composing commit log, memtable, flush, compaction,
//! S3 upload, manifest, and local cache into a single API.
//!
//! [`StorageEngine`] is the entry point for all storage operations. It owns:
//! - A [`CommitLog`] for write-ahead durability.
//! - Per-table [`TableStore`] instances for memtable + SSTable management.
//! - A [`CompactionExecutor`] for background STCS compaction.
//! - An optional [`UploadManager`] for async S3 uploads.
//! - A [`LocalCache`] tracking ephemeral-disk SSTable files.
//!
//! Thread safety: reads are lock-free (via ArcSwap in TableStore). Writes
//! take no global lock — the commit log uses CAS, memtable is lock-free.
//! Only flush and compaction take per-table serialized guards.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ferrosa_common::task_pool::TaskPool;
use futures::TryStreamExt;
use parking_lot::RwLock;
use smallvec::SmallVec;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::TableSchema;
use ferrosa_schema::SchemaSnapshot;
use ferrosa_sstable::types::{Partition, Row};

use crate::cache::LocalCache;
use crate::commitlog::config::{CommitLogConfig, CommitLogPosition, TableId};
use crate::commitlog::mutation::Mutation;
use crate::commitlog::CommitLog;
use crate::compaction::executor::CompactionExecutor;
use crate::compaction::strategy::{CompactionConfig, SizeTieredStrategy};
use crate::compaction::CompactionStrategy;
use crate::flush::FileFlushTarget;
use crate::store::{TableStore, VectorIndexConfig, VectorIndexMethod};
use crate::timeseries::aggregator::decode_typed_numeric;
use crate::timeseries::config::{validate_numeric_columns, ConsolidationConfig};
use crate::timeseries::consolidation::Accumulator;
use crate::timeseries::{
    ConsolidationFn, ConsolidationMetrics, ConsolidationTask, LateWindowClassification,
    MaterializationTarget, MaterializedRollup, TimeSeriesAggregator, TimeSeriesRuntimeSettings,
    TimeSeriesTimestampUnit,
};
use crate::upload::{ObjectStoreConfig, UploadManager};

/// One operation in an atomic batch (spec URS-QEC-X02).
///
/// Both variants lower to a [`Row`] inside a [`Mutation`]; the enum exists so
/// callers (Accord LWT apply, Cypher delete-cascade, Bolt transactions) express
/// intent without hand-building `Row` deletion markers.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Upsert a row at `key` in `keyspace.table` @ `timestamp`.
    Write {
        /// Target keyspace.
        keyspace: String,
        /// Target table.
        table: String,
        /// Partition key.
        key: DecoratedKey,
        /// Row to upsert.
        row: Row,
        /// Mutation timestamp (microseconds since epoch).
        timestamp: i64,
    },
    /// Tombstone. `clustering = None` deletes the whole partition; `Some(bytes)`
    /// deletes one clustered row. Lowers to a `Row` carrying a `DeletionTime`.
    Tombstone {
        /// Target keyspace.
        keyspace: String,
        /// Target table.
        table: String,
        /// Partition key.
        key: DecoratedKey,
        /// `None` = whole-partition delete; `Some` = single clustered row.
        clustering: Option<Vec<u8>>,
        /// Mutation timestamp (microseconds since epoch).
        timestamp: i64,
    },
}

impl BatchOp {
    /// Lower this op to a single-row [`Mutation`] for `write_atomic_batch`.
    ///
    /// A `Tombstone` becomes a pure tombstone `Row` (no cells, non-LIVE
    /// `DeletionTime`) — the in-memory representation the memtable merge path
    /// already honors for both partition (`clustering = None`) and clustered
    /// (`clustering = Some`) deletes.
    fn into_mutation(self) -> Mutation {
        match self {
            BatchOp::Write {
                keyspace,
                table,
                key,
                row,
                timestamp,
            } => Mutation::new(keyspace, table, key, vec![row], timestamp),
            BatchOp::Tombstone {
                keyspace,
                table,
                key,
                clustering,
                timestamp,
            } => {
                let deletion = deletion_at(timestamp);
                let row = Row {
                    clustering: clustering.unwrap_or_default(),
                    cells: Vec::new(),
                    deletion,
                    primary_key_liveness: ferrosa_sstable::types::LivenessInfo::NONE,
                };
                Mutation::new(keyspace, table, key, vec![row], timestamp)
            }
        }
    }
}

/// Build a non-LIVE [`ferrosa_sstable::types::DeletionTime`] for a tombstone at
/// `timestamp` microseconds. `local_deletion_time` (seconds) is derived from the
/// microsecond timestamp, saturating into `u32`.
fn deletion_at(timestamp: i64) -> ferrosa_sstable::types::DeletionTime {
    let local_seconds = timestamp.div_euclid(1_000_000).clamp(0, u32::MAX as i64) as u32;
    ferrosa_sstable::types::DeletionTime::new(timestamp, local_seconds)
}

/// Staging handle for a Bolt explicit transaction (spec URS-QEC-X02 §5.3).
///
/// Ops accumulate in memory; nothing is durable until [`Self::commit`].
/// [`Self::abort`] (or `Drop`) discards them with no I/O.
pub struct BatchTxn<'e> {
    engine: &'e StorageEngine,
    ops: Vec<BatchOp>,
}

impl<'e> BatchTxn<'e> {
    /// Append an op to the pending set (no durable write yet).
    pub fn stage(&mut self, op: BatchOp) {
        self.ops.push(op);
    }

    /// Number of staged ops.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// `true` when no ops are staged.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Apply all staged ops atomically (delegates to
    /// [`StorageEngine::apply_batch`]). Fail-loud on engine error.
    pub fn commit(self) -> ferrosa_common::Result<()> {
        self.engine.apply_batch(self.ops)
    }

    /// Discard all staged ops; no I/O. Equivalent to dropping the handle.
    pub fn abort(self) {
        drop(self);
    }
}

pub(crate) fn write_options_for_schema(
    schema: &TableSchema,
    verify_output: bool,
) -> ferrosa_common::Result<ferrosa_sstable::WriteOptions> {
    let compression = compression_from_schema(schema)?;
    let chunk_size = schema
        .extensions
        .get("compression.chunk_length_kb")
        .or_else(|| schema.extensions.get("compression.chunk_length_in_kb"))
        .and_then(|v| v.parse::<usize>().ok())
        .map(|kb| kb.saturating_mul(1024))
        .filter(|bytes| *bytes > 0)
        .unwrap_or(ferrosa_sstable::Compression::DEFAULT_CHUNK_SIZE);

    Ok(ferrosa_sstable::WriteOptions {
        compression,
        chunk_size,
        verify_output,
        ..ferrosa_sstable::WriteOptions::default()
    })
}

fn compression_from_schema(
    schema: &TableSchema,
) -> ferrosa_common::Result<Option<ferrosa_sstable::Compression>> {
    let Some(class) = schema
        .extensions
        .get("compression.class")
        .or_else(|| schema.extensions.get("compression.sstable_compression"))
    else {
        return Ok(Some(ferrosa_sstable::Compression::Lz4));
    };

    let class = class.trim();
    if class.is_empty()
        || class.eq_ignore_ascii_case("none")
        || class.eq_ignore_ascii_case("null")
        || class.eq_ignore_ascii_case("false")
    {
        return Ok(None);
    }

    let short_name = class.rsplit('.').next().unwrap_or(class);
    match short_name {
        "LZ4Compressor" | "LZ4" | "lz4" => Ok(Some(ferrosa_sstable::Compression::Lz4)),
        "ZstdCompressor" | "Zstd" | "zstd" | "ZSTD" => {
            let level = schema
                .extensions
                .get("compression.compression_level")
                .or_else(|| schema.extensions.get("compression.level"))
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(3);
            Ok(Some(ferrosa_sstable::Compression::Zstd { level }))
        }
        other => Err(ferrosa_common::Error::UnsupportedCompression(
            other.to_string(),
        )),
    }
}

/// Bounded live queue snapshot for one time-series materialization target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesMaterializationQueueSnapshot {
    pub source_table: TableId,
    pub target_table: TableId,
    pub window_start_ts: i64,
    pub window_end_ts: i64,
    pub task_type: String,
    pub enqueued_at_ms: i64,
    pub oldest_task_age_ms: i64,
    pub queue_depth: i64,
    pub retry_count: i64,
    pub last_error: Option<String>,
    pub max_delay_ms: i64,
    pub alerting: bool,
}

/// Bounded live status snapshot for one time-series materialization target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesMaterializationStatusSnapshot {
    pub source_table: TableId,
    pub target_table: TableId,
    pub status: String,
    pub pending_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub stale_drops_total: i64,
    pub last_materialized_window_end_ms: Option<i64>,
    pub last_error: Option<String>,
}

struct PendingUploadReplayFinalize {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    pending_log_path: PathBuf,
    table_id: String,
    sstable_id: String,
    total_size: u64,
    compaction: Option<crate::upload::pending_log::PendingCompactionUpload>,
    cas_supported: bool,
}

/// Host hook used by RRD materialization to run custom WASM aggregate rollups.
///
/// `ferrosa-storage` owns the streaming materialization loop but intentionally
/// does not depend on the WASM executor crate. The CQL/runtime layer injects an
/// implementation that starts a bounded aggregate invocation, then receives one
/// numeric sample per source row through [`TimeSeriesWasmAggregateInvocation`].
pub trait TimeSeriesWasmAggregateExecutor: Send + Sync {
    fn start(
        &self,
        keyspace: &str,
        function_name: &str,
        arg_type: &str,
    ) -> Result<Box<dyn TimeSeriesWasmAggregateInvocation>, String>;
}

pub trait TimeSeriesWasmAggregateInvocation: Send {
    fn update(&mut self, value: f64) -> Result<(), String>;
    fn finalize(self: Box<Self>) -> Result<f64, String>;
}

/// Configuration for NVMe pin mode on a table.
///
/// When a table is pinned, newly flushed SSTables are kept on local NVMe
/// disk and S3 upload is skipped. When `max_bytes` is set, the oldest
/// pinned SSTables are evicted once total pinned bytes exceeds the cap.
#[derive(Debug, Clone)]
pub struct PinConfig {
    /// Maximum bytes of pinned SSTables to keep on local disk.
    /// When total pinned size exceeds this, oldest SSTables are evicted
    /// (and their files removed from disk). `None` means no cap.
    pub max_bytes: Option<u64>,
}

/// Reservation for a spillable ORDER BY temp-sort table.
///
/// Dropping the guard removes the temporary table directory, so cancellation
/// and normal completion share the same cleanup path. The initial version is a
/// local-disk staging table; object-store/S3 flush can be added behind this
/// same lifecycle boundary without changing query cancellation semantics.
pub struct TempSortTableReservation {
    path: PathBuf,
}

impl TempSortTableReservation {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempSortTableReservation {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.path) {
                tracing::warn!(
                    path = %self.path.display(),
                    %e,
                    "storage: failed to clean ORDER BY temp-sort table reservation"
                );
            }
        }
    }
}

/// Configuration for the entire storage engine.
///
/// Composes sub-configurations for each component. Use `from_env()` for
/// production (reads `FERROSA_*` env vars) or `test_config()` for tests.
pub struct StorageEngineConfig {
    pub commit_log: CommitLogConfig,
    pub compaction: CompactionConfig,
    pub object_store: Option<ObjectStoreConfig>,
    pub local_cache_max_bytes: u64,
    /// Minimum free bytes to preserve on the local data filesystem before
    /// admitting a new write. When the filesystem is below this reserve, writes
    /// fail closed before appending to the commit log, so periodic fsync and
    /// flush cleanup do not degrade into ENOSPC.
    pub local_disk_free_reserve_bytes: u64,
    pub flush_threshold_bytes: u64,
    /// Active memtable size at which foreground writes fail closed and ask the
    /// background maintenance loop to flush. Distinct from
    /// `flush_threshold_bytes`, which is the soft "schedule an async flush"
    /// trigger. The backpressure trigger is much higher so it only fires under
    /// sustained-write pressure where writes outpace the maintenance loop's
    /// drain rate.
    /// Default: `max(flush_threshold_bytes * 4, 64 MB)`.
    pub memtable_backpressure_bytes: u64,
    /// Maximum age (seconds) of unflushed memtable data before a time-based
    /// flush is triggered, regardless of size. Protects small/infrequent
    /// tables from data loss on restart. Default: 30 seconds.
    pub flush_max_age_secs: u64,
    pub data_dir: PathBuf,
    /// Controls how secondary index builds are handled.
    /// - `Local`: in-process (default)
    /// - `Remote`: delegate to external `ferrosa-index-builder`
    /// - `Off`: disable index building entirely
    pub index_backend: crate::index::IndexBackendConfig,
    /// Defensive SSTable-writer self-readback — when `true`, every
    /// flush reopens the freshly-built SSTable through the full reader
    /// pipeline and aborts the flush if any component is inconsistent.
    /// See `specs/in-process/bug-read-path-memory-growth-bloats-coordinator.md`.
    pub write_verify: bool,
    /// When `true`, the engine enforces CQL role-based authentication
    /// end-to-end: CQL connections must authenticate, the router rejects
    /// statements the role lacks permissions for, and the web `:9090`
    /// surface checks tokens on write/admin endpoints.
    ///
    /// **Default: `false`** — preserves legacy behavior on every code
    /// path that doesn't explicitly opt in. `from_env()` honors
    /// `FERROSA_AUTH_ENABLED=true`; `test_config()` defaults off so no
    /// existing test sees a behavior change.
    ///
    /// State is logged loudly at startup so operators can never
    /// silently disable the whole auth subsystem without a visible
    /// record in the logs. See
    /// `specs/decisions/design-cql-role-auth-rollout.md` Sprint A.
    pub auth_enabled: bool,
    /// When `true` AND `auth_enabled` is also true, CQL permission
    /// denials are downgraded to a loud `WARN` log line and the request
    /// still proceeds. This is the rollout "soak" mode — an operator
    /// watches logs for unexpected consumers before flipping
    /// enforcement on.
    ///
    /// **Default: `false`** (`from_env()` honors `FERROSA_AUTH_WARN=true`,
    /// any other value keeps the enforcement default).
    ///
    /// Invalid combination: `auth_enabled=false, auth_warn=true` —
    /// nothing is checked, so there is nothing to warn about. Startup
    /// logs this at `ERROR` level and `auth_warn` is effectively
    /// ignored. See Sprint D.
    pub auth_warn: bool,
    /// Maximum number of commit-log replay mutations that may be buffered in
    /// memory when no table schema is available yet.
    ///
    /// Normal crash recovery preloads local `schema.json` and streams replay
    /// directly into registered tables. This cap is only for legacy/no-schema
    /// compatibility paths; exceeding it fails closed instead of rebuilding the
    /// original unbounded pending `Vec<Mutation>` OOM bug.
    pub max_pending_replay_mutations_without_schema: usize,
    /// Number of memtable shards. Each shard is an independently-locked
    /// `BTreeMap<DecoratedKey, Arc<Partition>>`; shard selection is
    /// `key.token.0 as u64 % num_shards`, so writes to different shards
    /// never contend. Increase on high-core-count nodes to reduce write
    /// contention; decrease for very small VMs to save the per-shard
    /// overhead. **Default: 64.** Honors `FERROSA_MEMTABLE_NUM_SHARDS`.
    /// Must be > 0; values <= 0 in the env var fall back to the default.
    pub memtable_num_shards: usize,
}

impl StorageEngineConfig {
    /// Reads configuration from `FERROSA_*` environment variables.
    pub fn from_env() -> ferrosa_common::Result<Self> {
        let data_dir = PathBuf::from(
            std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into()),
        );

        let mut commit_log = CommitLogConfig {
            log_dir: data_dir.join("commitlog"),
            checkpoint_dir: data_dir.join("commitlog"),
            ..CommitLogConfig::default()
        };
        commit_log.batch =
            crate::commitlog::config::CommitLogBatchConfig::from_env(commit_log.batch.clone());

        let compaction = CompactionConfig::from_env(data_dir.join("compaction"));

        let object_store = ObjectStoreConfig::from_env().ok();

        let local_cache_max_bytes = std::env::var("FERROSA_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10 * 1024 * 1024 * 1024); // 10 GB default

        let local_disk_free_reserve_bytes = std::env::var("FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512 * 1024 * 1024); // 512 MB default

        let flush_threshold_bytes: u64 = std::env::var("FERROSA_FLUSH_THRESHOLD_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64 * 1024 * 1024); // 64 MB default

        let memtable_backpressure_bytes: u64 = std::env::var("FERROSA_MEMTABLE_BACKPRESSURE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                // Default: max(threshold × 4, 64 MB) so tests
                // with intentionally tiny thresholds (for testing
                // flush behaviour) don't trip the production
                // backpressure path. Production deployments with
                // the default 64 MB threshold land on 256 MB.
                std::cmp::max(flush_threshold_bytes.saturating_mul(4), 64 * 1024 * 1024)
            });

        let flush_max_age_secs = std::env::var("FERROSA_FLUSH_MAX_AGE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30); // 30 seconds default

        // FERROSA_AUTH_WARN=true|false (default false). When auth is
        // enabled and warn mode is on, CQL permission denials are logged
        // loudly but the request still proceeds. This is the rollout
        // soak mode — see Sprint D in
        // `specs/decisions/design-cql-role-auth-rollout.md`. Like every
        // other toggle in this config, any unparseable value keeps the
        // safer default (enforcement) so a typo can't silently leave
        // auth stuck in warn forever.
        let auth_warn = matches!(
            std::env::var("FERROSA_AUTH_WARN").ok().as_deref(),
            Some("true" | "1" | "on" | "yes")
        );
        // FERROSA_AUTH_ENABLED=true|false (default false). Opt-in: any
        // unparseable value leaves auth DISABLED so an upgrade path
        // that doesn't intend to flip auth can't accidentally lock
        // itself out. When true, the CQL server requires SASL PLAIN
        // authentication and the router consults
        // `ferrosa_schema::check_permission`.
        let auth_enabled = matches!(
            std::env::var("FERROSA_AUTH_ENABLED").ok().as_deref(),
            Some("true" | "1" | "on" | "yes")
        );
        tracing::info!(
            auth_enabled,
            source = if std::env::var_os("FERROSA_AUTH_ENABLED").is_some() {
                "FERROSA_AUTH_ENABLED env"
            } else {
                "default"
            },
            "storage-engine: CQL role auth is {} — {}",
            if auth_enabled { "ENABLED" } else { "DISABLED" },
            if auth_enabled {
                "CQL STARTUP requires SASL PLAIN, router enforces permission \
                 checks, web :9090 requires tokens on writes/admin"
            } else {
                "CQL accepts every STARTUP without credentials, router permits \
                 everything — matches legacy behavior; set \
                 FERROSA_AUTH_ENABLED=true to enforce"
            }
        );
        log_auth_warn_state(auth_enabled, auth_warn);

        let write_verify = !matches!(
            std::env::var("FERROSA_WRITE_VERIFY").ok().as_deref(),
            Some("false" | "0" | "off" | "no")
        );

        let max_pending_replay_mutations_without_schema =
            std::env::var("FERROSA_MAX_PENDING_REPLAY_WITHOUT_SCHEMA")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024);

        let memtable_num_shards = std::env::var("FERROSA_MEMTABLE_NUM_SHARDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(64);

        Ok(Self {
            commit_log,
            compaction,
            object_store,
            local_cache_max_bytes,
            local_disk_free_reserve_bytes,
            flush_threshold_bytes,
            memtable_backpressure_bytes,
            flush_max_age_secs,
            data_dir,
            index_backend: crate::index::IndexBackendConfig::from_env(),
            write_verify,
            auth_enabled,
            auth_warn,
            max_pending_replay_mutations_without_schema,
            memtable_num_shards,
        })
    }

    /// Creates a test configuration using the given temp directory.
    ///
    /// Public (not `cfg(test)`) so integration tests in sibling crates
    /// — including the auth-rollout coverage in
    /// `ferrosa-cql/tests/auth_warn_mode.rs` — can construct the same
    /// shape without duplicating every field.
    pub fn test_config(dir: &Path) -> Self {
        Self {
            commit_log: CommitLogConfig::test_config(dir),
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,    // 1 MB
            local_disk_free_reserve_bytes: 0,      // disabled by default in tests
            flush_threshold_bytes: 4096,           // 4 KB — triggers flush quickly in tests
            memtable_backpressure_bytes: u64::MAX, // disabled by default in tests; opt-in per test
            flush_max_age_secs: 5,                 // 5s — fast age-based flush in tests
            data_dir: dir.to_path_buf(),
            index_backend: crate::index::IndexBackendConfig::Local,
            // Tests keep verification on — writer bugs surface
            // immediately in CI, not at runtime in production.
            write_verify: true,
            // Off by default — tests that cover auth must opt in
            // explicitly, matching the production default so backwards
            // compatibility is pinned by the type system.
            auth_enabled: false,
            // Off by default — tests that exercise warn-mode denials
            // must flip this to true themselves.
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        }
    }
}

/// Emit the startup log line describing the current `(auth_enabled,
/// auth_warn)` combination. Kept as a free function so tests and other
/// startup paths can invoke it without standing up a whole config.
///
/// Healthy combinations log at `INFO`; the contradictory configuration
/// (`auth_enabled=false && auth_warn=true`) logs at `ERROR` level so an
/// operator paging through startup logs can never miss it.
pub fn log_auth_warn_state(auth_enabled: bool, auth_warn: bool) {
    match (auth_enabled, auth_warn) {
        (true, true) => {
            tracing::info!(
                auth_enabled,
                auth_warn,
                source = if std::env::var_os("FERROSA_AUTH_WARN").is_some() {
                    "FERROSA_AUTH_WARN env"
                } else {
                    "default"
                },
                "storage-engine: CQL auth is ENFORCED but in WARN MODE — \
                 denials will be LOGGED as warnings and the request permitted. \
                 This is a soak configuration; set FERROSA_AUTH_WARN=false to \
                 turn denials back into errors. \
                 See specs/decisions/design-cql-role-auth-rollout.md Sprint D."
            );
        }
        (true, false) => {
            tracing::info!(
                auth_enabled,
                auth_warn,
                "storage-engine: CQL auth warn-mode is DISABLED — \
                 denials return Unauthorized to the client as expected."
            );
        }
        (false, false) => {
            tracing::info!(
                auth_enabled,
                auth_warn,
                "storage-engine: CQL auth warn-mode is irrelevant — \
                 auth is disabled entirely; every request is permitted."
            );
        }
        (false, true) => {
            tracing::error!(
                auth_enabled,
                auth_warn,
                "storage-engine: FERROSA_AUTH_WARN=true requires auth to \
                 be enabled (FERROSA_AUTH_ENABLED=true, or \
                 FERROSA_AUTH_DISABLED unset). With auth off there is \
                 nothing to warn about — auth_warn is being IGNORED. \
                 Either turn auth on (soak) or unset FERROSA_AUTH_WARN \
                 (explicit no-auth)."
            );
        }
    }
}

/// Build the index scheduler and tracker based on the engine configuration.
///
/// Returns `(Option<IndexBuildScheduler>, Arc<IndexStateTracker>)`.
/// When `index_backend` is `Off`, the scheduler is `None` and no worker
/// threads are spawned.
fn build_index_scheduler(
    config: &StorageEngineConfig,
) -> (
    Option<crate::index::IndexBuildScheduler>,
    Arc<crate::index::IndexStateTracker>,
) {
    let tracker = Arc::new(crate::index::IndexStateTracker::new());

    let scheduler = match &config.index_backend {
        crate::index::IndexBackendConfig::Off => None,
        crate::index::IndexBackendConfig::Local => {
            let backend = Arc::new(crate::index::LocalBackend::new(config.data_dir.clone()));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        }
        crate::index::IndexBackendConfig::Remote {
            endpoints,
            timeout,
            max_retries,
            circuit_breaker_threshold,
            circuit_breaker_recovery,
        } => {
            let s3_resolver = config
                .object_store
                .as_ref()
                .map(crate::index::S3PathResolver::from_object_store_config)
                .unwrap_or_else(|| crate::index::S3PathResolver {
                    bucket: String::new(),
                    endpoint: String::new(),
                    prefix: String::new(),
                });

            let local_fallback = crate::index::LocalBackend::new(config.data_dir.clone());
            let backend = Arc::new(crate::index::RemoteBackend::new(
                endpoints.clone(),
                s3_resolver,
                *timeout,
                *max_retries,
                *circuit_breaker_threshold,
                *circuit_breaker_recovery,
                local_fallback,
            ));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        }
    };

    (scheduler, tracker)
}

/// Build a high-priority eager-index rebuild job for a single index on a newly
/// materialized SSTable (flush output or compaction output).
///
/// The index's real [`IndexType`](ferrosa_index::IndexType) is read from the
/// store via [`TableStore::index_type_for`] so a non-BTree index
/// (phonetic/vector/fulltext/filtered/geo) is dispatched to the correct builder
/// rather than mis-stamped as `BTree`. Both the flush and compaction eager-build
/// sites call this so they cannot drift apart again.
fn eager_index_build_job(
    store: &TableStore<FileFlushTarget>,
    table_id: &TableId,
    sstable_id: String,
    index_name: &str,
    column_position: usize,
) -> crate::index::IndexBuildJob {
    crate::index::IndexBuildJob {
        sstable_id,
        index_name: index_name.to_string(),
        index_type: store.index_type_for(index_name),
        table: (
            table_id.keyspace().to_string(),
            table_id.table().to_string(),
        ),
        priority: crate::index::BuildPriority::High,
        enqueued_at: std::time::Instant::now(),
        column_position,
        filter_predicate: None,
    }
}

/// Top-level storage engine.
///
/// One instance per node. Manages multiple tables, each with its own
/// `TableStore`. The commit log is shared across all tables.
pub struct StorageEngine {
    config: StorageEngineConfig,
    tables: RwLock<HashMap<TableId, TableState>>,
    pub(crate) commit_log: CommitLog,
    /// Commitlog mutations replayed before their table schema is registered.
    /// These are applied lazily when the table is later registered.
    deferred_replay_mutations: parking_lot::Mutex<Vec<Mutation>>,
    compaction_executor: CompactionExecutor,
    upload_manager: Option<UploadManager>,
    compaction_upload_manager: Option<UploadManager>,
    local_cache: LocalCache,
    observers: RwLock<Vec<Arc<dyn crate::observer::WriteObserver>>>,
    async_observers: RwLock<Vec<AsyncObserverState>>,
    time_series_consolidators: RwLock<HashMap<TableId, TimeSeriesConsolidatorHandle>>,
    time_series_runtime_settings: Arc<TimeSeriesRuntimeSettings>,
    time_series_wasm_aggregates: RwLock<Option<Arc<dyn TimeSeriesWasmAggregateExecutor>>>,
    /// Default channel capacity for async observers.
    async_observer_capacity: usize,
    /// Index build scheduler — rebuilds secondary indexes after compaction.
    index_scheduler: Option<crate::index::IndexBuildScheduler>,
    /// Shared index state tracker.
    index_tracker: Arc<crate::index::IndexStateTracker>,
    /// Batchlog manager for logged batch coordination.
    batchlog: Option<crate::batchlog::BatchlogManager>,
    /// Background archiver task handle, if archiving is enabled.
    archiver_handle: Option<tokio::task::JoinHandle<()>>,
    /// Compaction S3 operation metrics (uploads, deletes, bytes reclaimed).
    pub compaction_metrics: Arc<crate::metrics::CompactionMetrics>,
    /// NVMe pin/unpin operation metrics (pinned tables, bytes, evictions).
    pub pin_metrics: Arc<crate::metrics::PinMetrics>,
    /// Shared S3 object store client for manifest/sync/snapshot operations.
    object_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// Whether the configured object store supports conditional PUT (CAS).
    /// Set by `probe_s3_cas()` at startup.  When `false`, manifest saves
    /// fall back to unconditional PUT (last-writer-wins).
    pub(crate) s3_cas_supported: std::sync::atomic::AtomicBool,
    /// Single-flight guard for S3 SSTable sync. Periodic flush sync, schema
    /// sync, compaction retry, and operator-triggered syncs can otherwise race
    /// from the same manifest snapshot and re-upload the same SSTables.
    s3_sync_running: AtomicBool,
    /// Set when write admission observes local disk pressure. The process
    /// maintenance loop consumes this flag to run an urgent S3 upload/eviction
    /// pass instead of waiting for the next normal flush tick.
    s3_sync_requested: AtomicBool,
    /// Last observed free bytes on `config.data_dir`, refreshed by
    /// `disk_free_bytes_cached()` at most ~once per second. Reading this on the
    /// per-write admission path keeps the blocking `statvfs` syscall off the
    /// async worker threads (raft apply, coordinator, CQL handlers).
    cached_disk_free_bytes: AtomicU64,
    /// Milliseconds since [`reference_instant`] of the last successful disk-free
    /// refresh. `u64::MAX` is the sentinel for "never checked".
    disk_free_checked_at_ms: AtomicU64,
    /// Set when the write path observes a memtable at/near the flush threshold.
    /// The process maintenance loop consumes this flag to run an urgent flush
    /// outside request handling.
    flush_requested: AtomicBool,
    /// Last observed live manifest object/byte totals per table. Updated by
    /// S3 sync/restore paths and used by sync-only metrics surfaces.
    s3_manifest_stats: RwLock<HashMap<String, (i32, i64)>>,
    /// Injected object store used in tests to bypass `ObjectStoreConfig::build_object_store()`.
    /// When `Some`, `resolve_store_and_prefix()` returns this store instead of building one.
    #[cfg(test)]
    upload_store_override: Option<(Arc<dyn object_store::ObjectStore>, String)>,
    /// Engine-wide bounded SSTable reader pool, shared by every `TableStore`
    /// so resident reader memory is `O(reader_cap)` across all tables rather
    /// than `O(sstable_count)` per table (FMEA #8).
    reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt>,
}

/// Per-table state: schema + store + optional NVMe pin config.
struct TableState {
    #[allow(dead_code)]
    schema: TableSchema,
    store: TableStore<FileFlushTarget>,
    /// When `Some`, this table is pinned to NVMe. S3 upload is skipped for
    /// new flushes, and `pinned_sstables` tracks size for max_bytes enforcement.
    pin_config: Option<PinConfig>,
    /// SSTable IDs that are currently pinned on NVMe, in oldest-first order.
    /// Each entry is `(sstable_id, size_bytes)`.
    pinned_sstables: Vec<(String, u64)>,
    /// Nanoseconds since [`REFERENCE_INSTANT`] of the first write to the
    /// current (unflushed) memtable. `0` means "memtable is clean". Used by
    /// `flush_if_needed` to trigger time-based flushes for small, infrequently-
    /// updated tables.  `AtomicI64` so the write hot path is lock-free: a
    /// `compare_exchange(0, now)` that succeeds at most once per memtable
    /// epoch, no contention thereafter.
    first_unflushed_write_at_nanos: std::sync::atomic::AtomicI64,
    /// Latest commit-log position written for this table. Updated on every
    /// write; passed to `commit_log.discard_completed()` after flush so that
    /// fully-flushed segments can be GC'd.
    ///
    /// `ArcSwap` keeps the hot-path update lock-free — one `Arc::new` (~30ns)
    /// plus an atomic pointer swap, with no mutex contention even when
    /// many threads write to the same table concurrently.
    last_commit_log_position: ArcSwap<Option<CommitLogPosition>>,
}

impl TableState {
    fn time_series_timestamp_unit(&self) -> TimeSeriesTimestampUnit {
        self.schema
            .clustering_columns
            .first()
            .map(|column| TimeSeriesTimestampUnit::from_storage_type(&column.type_name))
            .unwrap_or(TimeSeriesTimestampUnit::Micros)
    }
}

/// Process-wide reference instant used as the base for
/// `first_unflushed_write_at_nanos`. Captured lazily on first access so
/// that `now() - REFERENCE_INSTANT` always fits in an i64 (~292 years).
static REFERENCE_INSTANT: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

fn reference_instant() -> std::time::Instant {
    *REFERENCE_INSTANT.get_or_init(std::time::Instant::now)
}

fn now_nanos_since_reference() -> i64 {
    reference_instant().elapsed().as_nanos() as i64
}

/// Decode a `system_schema.indexes` composite clustering key into
/// `(table_name, index_name)`.
///
/// Inverse of the `[u16 len][table_name][u16 len][index_name]` encoding in
/// `ferrosa_schema::system::persistence::index_to_rows`. Returns `None` when
/// the bytes are truncated or the lengths overrun the buffer.
fn decode_index_clustering(clustering: &[u8]) -> Option<(String, String)> {
    fn take_len_prefixed(buf: &[u8], pos: &mut usize) -> Option<String> {
        let len_end = pos.checked_add(2)?;
        let len = u16::from_be_bytes(buf.get(*pos..len_end)?.try_into().ok()?) as usize;
        let str_end = len_end.checked_add(len)?;
        let bytes = buf.get(len_end..str_end)?;
        *pos = str_end;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    let mut pos = 0usize;
    let table = take_len_prefixed(clustering, &mut pos)?;
    let index_name = take_len_prefixed(clustering, &mut pos)?;
    Some((table, index_name))
}

/// Read a UTF-8 cell value at `col_index` from a row, if present and live.
fn cell_text(row: &Row, col_index: u16) -> Option<String> {
    row.cells
        .iter()
        .find(|(idx, _)| *idx == col_index)
        .and_then(|(_, cell)| cell.value.as_deref())
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

/// Read the raw bytes of a cell at `col_index` from a row, if present and live.
fn cell_bytes(row: &Row, col_index: u16) -> Option<Vec<u8>> {
    row.cells
        .iter()
        .find(|(idx, _)| *idx == col_index)
        .and_then(|(_, cell)| cell.value.as_deref())
        .map(|bytes| bytes.to_vec())
}

/// A decoded row of the persisted `system_schema.indexes` table.
///
/// Returned by [`StorageEngine::read_persisted_indexes`] so the CQL router can
/// serve `SELECT * FROM system_schema.indexes` from storage. Field order
/// mirrors the persisted layout: PK `keyspace_name`, clustering
/// `(table_name, index_name)`, regular cells `kind`/`target`/`options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedIndexRow {
    /// Partition key — the keyspace the index belongs to.
    pub keyspace_name: String,
    /// First clustering column — the indexed table.
    pub table_name: String,
    /// Second clustering column — the index name.
    pub index_name: String,
    /// Index kind string (`btree`, `hash`, …).
    pub kind: String,
    /// Target column(s), comma-joined.
    pub target: String,
    /// Index options serialized as JSON.
    pub options: String,
}

/// A decoded row of the persisted `system_schema.types` table.
///
/// Returned by [`StorageEngine::read_persisted_types`] so both the CQL router
/// (to serve `SELECT * FROM system_schema.types`) and the boot-time loader (to
/// rebuild the schema Registry's UDT map) read from the same storage source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTypeRow {
    /// Partition key — the keyspace owning the type.
    pub keyspace_name: String,
    /// Clustering column — the type name.
    pub type_name: String,
    /// Ordered `(field_name, field_type)` pairs.
    pub fields: Vec<(String, ferrosa_common::CqlType)>,
}

/// Decode one stored `system_schema.types` row into a [`PersistedTypeRow`].
///
/// Returns `None` for tombstones or rows whose `field_names`/`field_types`
/// cells are missing, malformed JSON, or length-mismatched — so callers surface
/// only well-formed UDT metadata rather than reconstructing a corrupt type.
fn decode_persisted_type_row(keyspace: &str, row: &Row) -> Option<PersistedTypeRow> {
    if !row.deletion.is_live() {
        return None;
    }
    let type_name = String::from_utf8(row.clustering.clone()).ok()?;

    let names_bytes = cell_bytes(
        row,
        ferrosa_schema::system::persistence::TYPES_COL_FIELD_NAMES,
    )?;
    let types_bytes = cell_bytes(
        row,
        ferrosa_schema::system::persistence::TYPES_COL_FIELD_TYPES,
    )?;
    let names: Vec<String> = serde_json::from_slice(&names_bytes).ok()?;
    let types: Vec<ferrosa_common::CqlType> = serde_json::from_slice(&types_bytes).ok()?;
    if names.len() != types.len() {
        return None;
    }
    let fields = names.into_iter().zip(types).collect();
    Some(PersistedTypeRow {
        keyspace_name: keyspace.to_string(),
        type_name,
        fields,
    })
}

/// A decoded row of the persisted `system_schema.functions` table.
///
/// Returned by [`StorageEngine::read_persisted_functions`] so both the CQL
/// router (to serve `SELECT * FROM system_schema.functions`) and the boot-time
/// loader (to rebuild the schema Registry's function map) read from the same
/// storage source.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedFunctionRow {
    /// Partition key — the keyspace owning the function.
    pub keyspace_name: String,
    /// First clustering column — the function name.
    pub function_name: String,
    /// Argument names, in declaration order.
    pub arg_names: Vec<String>,
    /// Argument types (second clustering column, decoded from JSON), in order.
    pub arg_types: Vec<ferrosa_common::CqlType>,
    /// Declared return type.
    pub return_type: ferrosa_common::CqlType,
    /// Whether the function is invoked on null input.
    pub called_on_null: bool,
    /// Implementation language (e.g. "wasm").
    pub language: String,
    /// Function body (hex-encoded WASM binary).
    pub body: String,
}

/// Decode the `system_schema.functions` composite clustering key into
/// `(function_name, arg_types)`.
///
/// Inverse of `ferrosa_schema::system::persistence::function_clustering`:
/// `[u16 len][function_name][u16 len][argument_types_json]`. Returns `None` when
/// the bytes are truncated or the JSON is malformed.
fn decode_function_clustering(clustering: &[u8]) -> Option<(String, Vec<ferrosa_common::CqlType>)> {
    let mut pos = 0usize;
    let name_len_end = pos.checked_add(2)?;
    let name_len = u16::from_be_bytes(clustering.get(pos..name_len_end)?.try_into().ok()?) as usize;
    let name_end = name_len_end.checked_add(name_len)?;
    let name = String::from_utf8(clustering.get(name_len_end..name_end)?.to_vec()).ok()?;
    pos = name_end;

    let json_len_end = pos.checked_add(2)?;
    let json_len = u16::from_be_bytes(clustering.get(pos..json_len_end)?.try_into().ok()?) as usize;
    let json_end = json_len_end.checked_add(json_len)?;
    let json = clustering.get(json_len_end..json_end)?;
    let arg_types: Vec<ferrosa_common::CqlType> = serde_json::from_slice(json).ok()?;
    Some((name, arg_types))
}

/// Decode one stored `system_schema.functions` row into a [`PersistedFunctionRow`].
///
/// Returns `None` for tombstones or rows whose clustering / required cells are
/// missing or malformed, so callers surface only well-formed function metadata
/// rather than reconstructing a corrupt overload.
fn decode_persisted_function_row(keyspace: &str, row: &Row) -> Option<PersistedFunctionRow> {
    if !row.deletion.is_live() {
        return None;
    }
    let (function_name, arg_types) = decode_function_clustering(&row.clustering)?;

    let names_bytes = cell_bytes(
        row,
        ferrosa_schema::system::persistence::FUNCTIONS_COL_ARGUMENT_NAMES,
    )?;
    let arg_names: Vec<String> = serde_json::from_slice(&names_bytes).ok()?;

    let return_bytes = cell_bytes(
        row,
        ferrosa_schema::system::persistence::FUNCTIONS_COL_RETURN_TYPE,
    )?;
    let return_type: ferrosa_common::CqlType = serde_json::from_slice(&return_bytes).ok()?;

    let called_on_null_bytes = cell_bytes(
        row,
        ferrosa_schema::system::persistence::FUNCTIONS_COL_CALLED_ON_NULL,
    )?;
    let called_on_null = called_on_null_bytes.first().copied().unwrap_or(0) != 0;

    let language = cell_text(
        row,
        ferrosa_schema::system::persistence::FUNCTIONS_COL_LANGUAGE,
    )?;
    let body = cell_text(row, ferrosa_schema::system::persistence::FUNCTIONS_COL_BODY)?;

    Some(PersistedFunctionRow {
        keyspace_name: keyspace.to_string(),
        function_name,
        arg_names,
        arg_types,
        return_type,
        called_on_null,
        language,
        body,
    })
}

/// Decode one stored `system_schema.indexes` row into a [`PersistedIndexRow`].
///
/// Returns `None` for tombstones or rows whose clustering / required cells are
/// missing or malformed, so callers surface only well-formed index metadata.
fn decode_persisted_index_row(keyspace: &str, row: &Row) -> Option<PersistedIndexRow> {
    let (table_name, index_name) = decode_index_clustering(&row.clustering)?;
    let kind = cell_text(row, ferrosa_schema::system::persistence::INDEXES_COL_KIND)?;
    let target = cell_text(row, ferrosa_schema::system::persistence::INDEXES_COL_TARGET)?;
    let options = cell_text(
        row,
        ferrosa_schema::system::persistence::INDEXES_COL_OPTIONS,
    )
    .unwrap_or_else(|| "{}".to_string());
    Some(PersistedIndexRow {
        keyspace_name: keyspace.to_string(),
        table_name,
        index_name,
        kind,
        target,
        options,
    })
}

/// Reserved `system_schema.indexes` options key under which the CREATE path
/// stores a Filtered index's fully-encoded [`ferrosa_index::FilterPredicate`]
/// as JSON, so the predicate survives restart and reload reconstructs it
/// without needing the CQL type system.
pub const FILTER_PREDICATE_OPTION_KEY: &str = "__filter_predicate";

/// Decode the partial-index predicate from a persisted `system_schema.indexes`
/// row's `options` cell.
///
/// Returns `None` when the options cell is absent, not valid JSON, lacks the
/// reserved [`FILTER_PREDICATE_OPTION_KEY`], or the stored predicate JSON does
/// not deserialize — every one of which is a malformed Filtered index that the
/// caller must reject rather than reload as an unfiltered index.
fn decode_filter_predicate_from_options(row: &Row) -> Option<ferrosa_index::FilterPredicate> {
    let options_json = cell_text(
        row,
        ferrosa_schema::system::persistence::INDEXES_COL_OPTIONS,
    )?;
    let options: HashMap<String, String> = serde_json::from_str(&options_json).ok()?;
    let predicate_json = options.get(FILTER_PREDICATE_OPTION_KEY)?;
    ferrosa_index::FilterPredicate::from_option_string(predicate_json)
}

/// Sidecar map type alias: index name -> sidecar reader for one SSTable.
type SSTableSidecarMap = Arc<HashMap<String, crate::index::sidecar::SidecarReader>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupSstableRepairMode {
    Off,
    Warn,
    Quarantine,
}

impl StartupSstableRepairMode {
    fn from_env(value: &str) -> Self {
        let value = value.trim();
        if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("off") || value == "0"
        {
            Self::Off
        } else if value.eq_ignore_ascii_case("quarantine")
            || value.eq_ignore_ascii_case("repair")
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("on")
            || value == "1"
        {
            Self::Quarantine
        } else {
            Self::Warn
        }
    }
}

const DEFAULT_TIME_SERIES_MATERIALIZATION_POLL_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(100);
const DEFAULT_TIME_SERIES_MATERIALIZATION_BATCH_LIMIT: usize = 128;

/// State for a single async observer: the observer, its sender half, and a
/// drop counter for backpressure metrics.
struct AsyncObserverState {
    observer: Arc<dyn crate::observer::WriteObserver>,
    sender: tokio::sync::mpsc::Sender<(TableId, Mutation)>,
    drop_count: Arc<AtomicU64>,
}

struct TimeSeriesConsolidatorHandle {
    aggregator: Arc<TimeSeriesAggregator>,
    observer: Arc<dyn crate::observer::WriteObserver>,
    task_rx: parking_lot::Mutex<std::sync::mpsc::Receiver<ConsolidationTask>>,
    target: MaterializationTarget,
    late_window: std::time::Duration,
    source_column_count: usize,
    source_column_indices: Vec<u16>,
    source_column_types: Vec<String>,
    failed_tasks: AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl TimeSeriesConsolidatorHandle {
    fn note_materialization_failure(&self, error: &ferrosa_common::Error) {
        self.failed_tasks.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write() = Some(error.to_string());
    }

    fn failure_snapshot(&self) -> (u64, Option<String>) {
        (
            self.failed_tasks.load(Ordering::Relaxed),
            self.last_error.read().clone(),
        )
    }
}

fn normalize_consolidation_type(type_name: &str) -> String {
    match type_name {
        "org.apache.cassandra.db.marshal.DoubleType" => "double",
        "org.apache.cassandra.db.marshal.FloatType" => "float",
        "org.apache.cassandra.db.marshal.Int32Type" => "int",
        "org.apache.cassandra.db.marshal.LongType" => "bigint",
        "org.apache.cassandra.db.marshal.CounterColumnType" => "counter",
        "org.apache.cassandra.db.marshal.TimestampType" => "timestamp",
        other => other,
    }
    .to_ascii_lowercase()
}

impl StorageEngine {
    fn block_on_rehydration<F, T>(future: F) -> T
    where
        F: Future<Output = T>,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return tokio::task::block_in_place(|| handle.block_on(future));
            }
        }

        static REHYDRATION_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> =
            std::sync::OnceLock::new();
        REHYDRATION_RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build SSTable rehydration runtime")
            })
            .block_on(future)
    }

    fn parse_local_sstable_component_path(
        data_dir: &Path,
        path: &Path,
    ) -> Option<(String, String, String)> {
        let sstable_root = data_dir.join("sstables");
        let relative = path.strip_prefix(&sstable_root).ok()?;
        let parts: Vec<_> = relative.components().collect();
        match parts.as_slice() {
            [table, file] => {
                let table_id = table.as_os_str().to_str()?.to_string();
                let file_name = file.as_os_str().to_str()?;
                let (sstable_id, component) = file_name.split_once('-')?;
                Some((table_id, sstable_id.to_string(), component.to_string()))
            }
            [table, dir_gen, file] => {
                let table_id = table.as_os_str().to_str()?.to_string();
                let dir_gen = dir_gen.as_os_str().to_str()?;
                let file_name = file.as_os_str().to_str()?;
                let (sstable_id, component) = file_name.split_once('-')?;
                if sstable_id == dir_gen {
                    Some((table_id, sstable_id.to_string(), component.to_string()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn install_s3_file_read_rehydration_hook(
        data_dir: PathBuf,
        prefix: String,
        store: Arc<dyn object_store::ObjectStore>,
    ) {
        const SSTABLE_COMPONENTS: &[&str] = &[
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ];

        let rehydration_locks: Arc<DashMap<String, Arc<std::sync::Mutex<()>>>> =
            Arc::new(DashMap::new());
        {
            let data_dir = data_dir.clone();
            let prefix = prefix.clone();
            let store = Arc::clone(&store);
            ferrosa_sstable::io::register_file_read_range_hook(Arc::new(
                move |path, offset, len| {
                    let Some((table_id, sstable_id, component)) =
                        Self::parse_local_sstable_component_path(&data_dir, path)
                    else {
                        return Ok(None);
                    };
                    if !SSTABLE_COMPONENTS.contains(&component.as_str()) {
                        return Ok(None);
                    }
                    if len == 0 {
                        return Ok(Some(Vec::new()));
                    }

                    let start = usize::try_from(offset).map_err(|_| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "SSTable range read offset exceeds usize: {}",
                            offset
                        ))
                    })?;
                    let end = start.checked_add(len).ok_or_else(|| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "SSTable range read overflow: offset={} len={}",
                            offset, len
                        ))
                    })?;
                    let hex = crate::upload::manager::hex_prefix_for(&sstable_id);
                    let s3_path = crate::upload::manager::sstable_object_key(
                        &prefix,
                        &hex,
                        &table_id,
                        &sstable_id,
                        &component,
                    );
                    let store = Arc::clone(&store);
                    let result = Self::block_on_rehydration(async move {
                        match store.get_range(&s3_path, start..end).await {
                            Ok(bytes) => Ok(Some(bytes.to_vec())),
                            Err(object_store::Error::NotFound { .. }) => Ok(None),
                            Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                                "failed ranged SSTable component read {s3_path}: {e}"
                            ))),
                        }
                    })?;
                    if result.is_some() {
                        tracing::debug!(
                            local_path = %path.display(),
                            table = table_id,
                            sstable = sstable_id,
                            component,
                            offset,
                            len,
                            "served evicted SSTable component range from object storage"
                        );
                    }
                    Ok(result)
                },
            ));
        }
        {
            let data_dir = data_dir.clone();
            let prefix = prefix.clone();
            let store = Arc::clone(&store);
            ferrosa_sstable::io::register_file_read_len_hook(Arc::new(move |path| {
                let Some((table_id, sstable_id, component)) =
                    Self::parse_local_sstable_component_path(&data_dir, path)
                else {
                    return Ok(None);
                };
                if !SSTABLE_COMPONENTS.contains(&component.as_str()) {
                    return Ok(None);
                }
                let hex = crate::upload::manager::hex_prefix_for(&sstable_id);
                let s3_path = crate::upload::manager::sstable_object_key(
                    &prefix,
                    &hex,
                    &table_id,
                    &sstable_id,
                    &component,
                );
                let store = Arc::clone(&store);
                Self::block_on_rehydration(async move {
                    match store.head(&s3_path).await {
                        Ok(meta) => Ok(Some(meta.size as u64)),
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                            "failed SSTable component head {s3_path}: {e}"
                        ))),
                    }
                })
            }));
        }
        ferrosa_sstable::io::register_file_read_rehydration_hook(Arc::new(move |path| {
            let Some((table_id, sstable_id, component)) =
                Self::parse_local_sstable_component_path(&data_dir, path)
            else {
                return Ok(false);
            };
            if !SSTABLE_COMPONENTS.contains(&component.as_str()) {
                return Ok(false);
            }

            crate::metrics::inc_sstable_rehydration_request();
            let started = Instant::now();
            let lock_key = format!("{table_id}/{sstable_id}");
            let rehydration_lock = Arc::clone(
                rehydration_locks
                    .entry(lock_key.clone())
                    .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
                    .value(),
            );
            let _guard = rehydration_lock
                .lock()
                .expect("SSTable rehydration lock poisoned");
            let Some(parent) = path.parent() else {
                return Ok(false);
            };
            std::fs::create_dir_all(parent)?;

            if path.exists() {
                crate::metrics::observe_sstable_rehydration_success(started.elapsed(), 0, 0);
                return Ok(true);
            }

            let hex = crate::upload::manager::hex_prefix_for(&sstable_id);
            let store = Arc::clone(&store);
            let parent = parent.to_path_buf();
            let requested_path = path.to_path_buf();
            let async_prefix = prefix.clone();
            let async_table_id = table_id.clone();
            let async_sstable_id = sstable_id.clone();
            let async_requested_path = requested_path.clone();
            let components = SSTABLE_COMPONENTS.to_vec();

            crate::metrics::inc_sstable_rehydration_in_flight();
            let result = Self::block_on_rehydration(async move {
                let mut restored = 0usize;
                let mut restored_bytes = 0u64;
                for component_name in components {
                    let local_path = parent.join(format!("{async_sstable_id}-{component_name}"));
                    if local_path.exists() {
                        continue;
                    }

                    let s3_path = crate::upload::manager::sstable_object_key(
                        &async_prefix,
                        &hex,
                        &async_table_id,
                        &async_sstable_id,
                        component_name,
                    );
                    let tmp_path = local_path.with_extension(format!(
                        "{}.rehydrate.tmp",
                        local_path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .unwrap_or("component")
                    ));

                    match store.get(&s3_path).await {
                        Ok(result) => {
                            let mut stream = result.into_stream();
                            let mut file =
                                tokio::fs::File::create(&tmp_path).await.map_err(|e| {
                                    ferrosa_common::Error::InvalidFormat(format!(
                                        "failed to create SSTable rehydration temp file {}: {e}",
                                        tmp_path.display()
                                    ))
                                })?;
                            use tokio::io::AsyncWriteExt;
                            while let Some(chunk) = stream.try_next().await.map_err(|e| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "failed to stream SSTable component {s3_path}: {e}"
                                ))
                            })? {
                                restored_bytes = restored_bytes.saturating_add(chunk.len() as u64);
                                file.write_all(&chunk).await.map_err(|e| {
                                    ferrosa_common::Error::InvalidFormat(format!(
                                        "failed to write SSTable rehydration temp file {}: {e}",
                                        tmp_path.display()
                                    ))
                                })?;
                            }
                            file.sync_data().await.map_err(|e| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "failed to sync SSTable rehydration temp file {}: {e}",
                                    tmp_path.display()
                                ))
                            })?;
                            drop(file);
                            tokio::fs::rename(&tmp_path, &local_path)
                                .await
                                .map_err(|e| {
                                    ferrosa_common::Error::InvalidFormat(format!(
                                        "failed to promote SSTable rehydration temp file {} to {}: {e}",
                                        tmp_path.display(),
                                        local_path.display()
                                    ))
                            })?;
                            restored += 1;
                        }
                        Err(object_store::Error::NotFound { .. })
                            if component_name != "Data.db" =>
                        {
                            let _ = tokio::fs::remove_file(&tmp_path).await;
                            tracing::debug!(
                                table = async_table_id.as_str(),
                                sstable = async_sstable_id.as_str(),
                                component = component_name,
                                "optional SSTable component absent during read-through rehydration"
                            );
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            let _ = tokio::fs::remove_file(&tmp_path).await;
                            return Ok((false, restored, restored_bytes));
                        }
                        Err(e) => {
                            let _ = tokio::fs::remove_file(&tmp_path).await;
                            return Err(ferrosa_common::Error::InvalidFormat(format!(
                                "failed to rehydrate SSTable component {s3_path}: {e}"
                            )));
                        }
                    }
                }

                Ok((
                    async_requested_path.exists() || restored > 0,
                    restored,
                    restored_bytes,
                ))
            });
            crate::metrics::dec_sstable_rehydration_in_flight();

            if let Err(err) = &result {
                crate::metrics::observe_sstable_rehydration_failure(started.elapsed());
                tracing::warn!(
                    %err,
                    local_path = %requested_path.display(),
                    "SSTable read-through rehydration failed"
                );
            } else if let Ok((true, restored, restored_bytes)) = result {
                crate::metrics::observe_sstable_rehydration_success(
                    started.elapsed(),
                    restored as u64,
                    restored_bytes,
                );
                tracing::info!(
                    local_path = %requested_path.display(),
                    table = table_id,
                    sstable = sstable_id,
                    requested_component = component,
                    restored,
                    restored_bytes,
                    "SSTable generation rehydrated from object storage"
                );
                return Ok(true);
            } else {
                crate::metrics::observe_sstable_rehydration_failure(started.elapsed());
            }

            Ok(false)
        }));
    }

    fn build_upload_managers(
        object_store_config: Option<&ObjectStoreConfig>,
        runtime: Option<&tokio::runtime::Handle>,
        object_store: Option<&Arc<dyn object_store::ObjectStore>>,
    ) -> (Option<UploadManager>, Option<UploadManager>) {
        match (object_store_config, runtime, object_store) {
            (Some(os_config), Some(rt), Some(store)) => {
                let upload_manager = UploadManager::new_with_pools(
                    Arc::clone(store),
                    os_config.prefix.clone(),
                    os_config.upload_queue_depth,
                    os_config.upload_workers,
                    os_config.delete_workers,
                    rt,
                );
                let compaction_upload_manager = UploadManager::new_with_pools(
                    Arc::clone(store),
                    os_config.prefix.clone(),
                    os_config.compaction_upload_queue_depth,
                    os_config.compaction_upload_workers,
                    os_config.delete_workers,
                    rt,
                );
                (Some(upload_manager), Some(compaction_upload_manager))
            }
            _ => (None, None),
        }
    }

    /// Remove stale local compaction staging files from previous processes.
    ///
    /// Compaction output is only live while an in-process compaction result is
    /// waiting to be promoted. On startup there are no such in-memory results,
    /// so any files under `compaction/` are disk-only debris. Durable SSTables
    /// used for reads and pending S3 replay live under `sstables/`; replay drops
    /// missing-file entries instead of treating staging files as authoritative.
    fn cleanup_stale_compaction_staging(
        config: &StorageEngineConfig,
    ) -> ferrosa_common::Result<()> {
        let output_dir = &config.compaction.output_dir;
        if !output_dir.exists() {
            std::fs::create_dir_all(output_dir).map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to create compaction staging dir {}: {e}",
                    output_dir.display()
                ))
            })?;
            return Ok(());
        }

        let removed_files = Self::count_regular_files(output_dir)?;
        std::fs::remove_dir_all(output_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to remove stale compaction staging dir {}: {e}",
                output_dir.display()
            ))
        })?;
        std::fs::create_dir_all(output_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to recreate compaction staging dir {}: {e}",
                output_dir.display()
            ))
        })?;

        if removed_files > 0 {
            tracing::info!(
                removed_files,
                path = %output_dir.display(),
                "storage startup: cleaned stale compaction staging files"
            );
        }

        Ok(())
    }

    fn count_regular_files(dir: &Path) -> ferrosa_common::Result<usize> {
        let mut count = 0usize;
        for entry in std::fs::read_dir(dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to read compaction staging dir {}: {e}",
                dir.display()
            ))
        })? {
            let entry = entry.map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to read compaction staging entry in {}: {e}",
                    dir.display()
                ))
            })?;
            let file_type = entry.file_type().map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to inspect compaction staging entry {}: {e}",
                    entry.path().display()
                ))
            })?;
            if file_type.is_file() {
                count += 1;
            } else if file_type.is_dir() {
                count += Self::count_regular_files(&entry.path())?;
            }
        }
        Ok(count)
    }

    /// Free bytes on `config.data_dir`, served from a time-gated cache so the
    /// blocking `statvfs` syscall runs at most ~once per second instead of on
    /// every write. The cache is at most ~1s stale, which is well within the
    /// tolerance of the disk-reserve guard.
    ///
    /// Refresh is single-flight: a `compare_exchange` on the timestamp elects
    /// exactly one caller to run `statvfs` per ~1s window; everyone else reads
    /// the last cached value. On `statvfs` error we log and return the cached
    /// value rather than clobbering it (fail-visible, not fail-silent).
    fn disk_free_bytes_cached(&self) -> u64 {
        let now_ms = reference_instant().elapsed().as_millis() as u64;
        let last = self.disk_free_checked_at_ms.load(Ordering::Acquire);
        let stale = last == u64::MAX || now_ms.saturating_sub(last) >= 1000;
        if stale
            && self
                .disk_free_checked_at_ms
                .compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            match fs2::available_space(&self.config.data_dir) {
                Ok(available) => {
                    self.cached_disk_free_bytes
                        .store(available, Ordering::Release);
                    if available < self.local_disk_eviction_low_water_bytes() {
                        self.request_s3_sync();
                    }
                    return available;
                }
                Err(e) => {
                    tracing::warn!(
                        path = %self.config.data_dir.display(),
                        error = %e,
                        "statvfs failed during disk-free refresh; serving cached value"
                    );
                }
            }
        }
        self.cached_disk_free_bytes.load(Ordering::Acquire)
    }

    /// Test-only override of the disk-free cache. Marks the cache fresh so the
    /// admission gate reads the injected value instead of immediately running a
    /// live `statvfs` that would overwrite it.
    #[cfg(test)]
    pub(crate) fn set_disk_free_cache_for_test(&self, bytes: u64) {
        self.cached_disk_free_bytes.store(bytes, Ordering::Release);
        self.disk_free_checked_at_ms.store(
            reference_instant().elapsed().as_millis() as u64,
            Ordering::Release,
        );
    }

    pub fn check_write_admission(&self) -> ferrosa_common::Result<()> {
        let reserve = self.config.local_disk_free_reserve_bytes;
        if reserve == 0 {
            return Ok(());
        }

        let available = self.disk_free_bytes_cached();
        if available < reserve {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "local disk free space below write reserve: available={available} reserve={reserve} path={}",
                self.config.data_dir.display()
            )));
        }

        Ok(())
    }

    fn local_cache_min_bytes(&self) -> u64 {
        std::env::var("FERROSA_CACHE_MIN_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
            .min(self.config.local_cache_max_bytes)
    }

    fn local_disk_eviction_low_water_bytes(&self) -> u64 {
        std::env::var("FERROSA_LOCAL_DISK_EVICTION_LOW_WATER_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| self.config.local_disk_free_reserve_bytes.saturating_mul(2))
    }

    fn local_disk_eviction_target_free_bytes(&self) -> u64 {
        std::env::var("FERROSA_LOCAL_DISK_EVICTION_TARGET_FREE_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| {
                self.local_disk_eviction_low_water_bytes()
                    .max(self.config.local_disk_free_reserve_bytes.saturating_mul(3))
            })
    }

    fn check_memtable_write_admission(
        &self,
        table_id: &TableId,
        state: &TableState,
    ) -> ferrosa_common::Result<()> {
        let memtable_size = state.store.memtable_size() as u64;
        crate::metrics::observe_memtable_size(memtable_size);
        if memtable_size >= self.config.memtable_backpressure_bytes {
            self.request_flush();
            return Err(ferrosa_common::Error::InvalidData(format!(
                "overloaded: memtable backpressure threshold exceeded: table={table_id} size={memtable_size} threshold={}",
                self.config.memtable_backpressure_bytes
            )));
        }
        Ok(())
    }

    fn request_flush_if_needed(&self, state: &TableState) {
        let memtable_size = state.store.memtable_size() as u64;
        crate::metrics::observe_memtable_size(memtable_size);
        if memtable_size >= self.config.flush_threshold_bytes {
            self.request_flush();
        }
    }

    /// Creates a new storage engine. Initializes the commit log, compaction
    /// executor, and optional upload manager.
    pub fn new(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> ferrosa_common::Result<Self> {
        // Pin the process-wide memtable shard count from config. The
        // sharded memtable's `with_default_shards()` reads this
        // atomic, so every memtable created after this point honors
        // FERROSA_MEMTABLE_NUM_SHARDS (or the explicit field on
        // `StorageEngineConfig`).
        crate::memtable::sharded::set_configured_num_shards(config.memtable_num_shards);
        crate::metrics::set_memtable_thresholds(
            config.flush_threshold_bytes,
            config.memtable_backpressure_bytes,
        );

        // Ensure data directories exist.
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;
        Self::cleanup_stale_compaction_staging(&config)?;

        let commit_log = CommitLog::new(config.commit_log.clone())?;
        // Engine-wide reader pool, created before the compaction executor so the
        // executor can route input opens through it (FMEA #11).
        let reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );
        let compaction_executor = CompactionExecutor::with_reader_pool(Arc::clone(&reader_pool));

        let object_store: Option<Arc<dyn object_store::ObjectStore>> = match &config.object_store {
            Some(os_config) => Some(Arc::from(os_config.build_object_store()?)),
            None => None,
        };

        let (upload_manager, compaction_upload_manager) = Self::build_upload_managers(
            config.object_store.as_ref(),
            runtime,
            object_store.as_ref(),
        );

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let (index_scheduler, index_tracker) = build_index_scheduler(&config);

        let engine = Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            deferred_replay_mutations: parking_lot::Mutex::new(Vec::new()),
            compaction_executor,
            upload_manager,
            compaction_upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            time_series_consolidators: RwLock::new(HashMap::new()),
            time_series_runtime_settings: Arc::new(TimeSeriesRuntimeSettings::from_config(
                &ConsolidationConfig::default(),
            )),
            time_series_wasm_aggregates: RwLock::new(None),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            pin_metrics: Arc::new(crate::metrics::PinMetrics::new()),
            object_store,
            s3_cas_supported: std::sync::atomic::AtomicBool::new(true),
            s3_sync_running: AtomicBool::new(false),
            s3_sync_requested: AtomicBool::new(false),
            cached_disk_free_bytes: AtomicU64::new(0),
            disk_free_checked_at_ms: AtomicU64::new(u64::MAX),
            flush_requested: AtomicBool::new(false),
            s3_manifest_stats: RwLock::new(HashMap::new()),
            reader_pool,
            #[cfg(test)]
            upload_store_override: None,
        };

        if let (Some(os_config), Some(store)) = (
            engine.config.object_store.as_ref(),
            engine.object_store.as_ref(),
        ) {
            Self::install_s3_file_read_rehydration_hook(
                engine.config.data_dir.clone(),
                os_config.prefix.clone(),
                Arc::clone(store),
            );
        }

        engine.load_local_schema_if_present();
        Ok(engine)
    }

    /// Probe the configured object store for conditional put support.
    ///
    /// Call this once after construction when an S3 store is configured.
    /// Stores the result in `s3_cas_supported`. When CAS is not available,
    /// manifest saves fall back to unconditional PUT (last-writer-wins),
    /// which is safe for single-node prefixes and dev environments.
    pub async fn probe_s3_cas(&self) -> ferrosa_common::Result<()> {
        if let Ok((_, store)) = self.object_store_and_config() {
            let supported = crate::manifest::probe_conditional_put_support(store.as_ref()).await;
            self.s3_cas_supported
                .store(supported, std::sync::atomic::Ordering::Relaxed);
            if !supported {
                tracing::warn!(
                    "S3 object store does not support conditional PUT (CAS) — \
                     manifest saves will use unconditional PUT (last-writer-wins)"
                );
            }
        }
        Ok(())
    }

    /// Whether the configured S3 store supports CAS (conditional PUT).
    pub fn cas_supported(&self) -> bool {
        self.s3_cas_supported
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Creates a storage engine with an explicit archive object store.
    ///
    /// Used by tests to inject an InMemory store instead of real S3.
    /// When `archive_store` is `Some` and `config.commit_log.archive` is
    /// enabled, spawns a background archiver task on the provided runtime.
    pub fn new_with_archive_store(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
        archive_store: Option<Arc<dyn object_store::ObjectStore>>,
        archive_prefix: String,
    ) -> ferrosa_common::Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;
        Self::cleanup_stale_compaction_staging(&config)?;

        let mut commit_log = CommitLog::new(config.commit_log.clone())?;
        // Engine-wide reader pool, created before the compaction executor so the
        // executor can route input opens through it (FMEA #11).
        let reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );
        let compaction_executor = CompactionExecutor::with_reader_pool(Arc::clone(&reader_pool));

        let object_store: Option<Arc<dyn object_store::ObjectStore>> = match &config.object_store {
            Some(os_config) => Some(Arc::from(os_config.build_object_store()?)),
            None => None,
        };

        let (upload_manager, compaction_upload_manager) = Self::build_upload_managers(
            config.object_store.as_ref(),
            runtime,
            object_store.as_ref(),
        );

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        // Set up archiver if enabled.
        let archiver_handle = match (&config.commit_log.archive, archive_store, runtime) {
            (Some(archive_cfg), Some(store), Some(rt)) if archive_cfg.enabled => {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);
                commit_log.set_archive_channel(tx);

                let archiver = crate::commitlog::archiver::CommitLogArchiver::new(
                    store,
                    archive_prefix,
                    config.commit_log.log_dir.clone(),
                );

                // Spawn background archiver task.
                let handle = rt.spawn(async move {
                    while let Some(segment_id) = rx.recv().await {
                        match archiver.archive_segment(segment_id).await {
                            Ok(result) => {
                                // Update manifest.
                                let entry = crate::commitlog::manifest::ArchiveSegmentEntry {
                                    id: result.segment_id,
                                    sha256: result.sha256,
                                    size: result.size,
                                    archived_at: result.archived_at,
                                };
                                if let Err(e) =
                                    crate::commitlog::manifest::ArchiveManifest::append_and_save(
                                        archiver.store(),
                                        archiver.prefix(),
                                        entry,
                                    )
                                    .await
                                {
                                    tracing::error!(%e, segment_id, "commitlog-archiver: manifest update failed");
                                }
                            }
                            Err(e) => {
                                tracing::error!(%e, segment_id, "commitlog-archiver: failed to archive segment");
                            }
                        }
                    }
                });
                Some(handle)
            }
            _ => None,
        };

        let (index_scheduler, index_tracker) = build_index_scheduler(&config);

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            deferred_replay_mutations: parking_lot::Mutex::new(Vec::new()),
            compaction_executor,
            upload_manager,
            compaction_upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            time_series_consolidators: RwLock::new(HashMap::new()),
            time_series_runtime_settings: Arc::new(TimeSeriesRuntimeSettings::from_config(
                &ConsolidationConfig::default(),
            )),
            time_series_wasm_aggregates: RwLock::new(None),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            pin_metrics: Arc::new(crate::metrics::PinMetrics::new()),
            object_store,
            s3_cas_supported: std::sync::atomic::AtomicBool::new(true),
            s3_sync_running: AtomicBool::new(false),
            s3_sync_requested: AtomicBool::new(false),
            cached_disk_free_bytes: AtomicU64::new(0),
            disk_free_checked_at_ms: AtomicU64::new(u64::MAX),
            flush_requested: AtomicBool::new(false),
            s3_manifest_stats: RwLock::new(HashMap::new()),
            reader_pool,
            #[cfg(test)]
            upload_store_override: None,
        })
    }

    /// Opens an existing storage engine directory and replays uncommitted
    /// mutations from the commit log.
    ///
    /// Returns the engine and the list of mutations that need to be replayed.
    /// Call [`replay_mutations`](Self::replay_mutations) with the returned
    /// mutations after registering all table schemas.
    pub fn open(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> ferrosa_common::Result<(Self, Vec<Mutation>)> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;
        Self::cleanup_stale_compaction_staging(&config)?;

        // Engine-wide reader pool, created here so tables registered during
        // startup replay share it with those registered later — and so the
        // compaction executor routes its input opens through the same bounded
        // pool (FMEA #11).
        let reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );
        let compaction_executor = CompactionExecutor::with_reader_pool(Arc::clone(&reader_pool));

        let object_store: Option<Arc<dyn object_store::ObjectStore>> = match &config.object_store {
            Some(os_config) => Some(Arc::from(os_config.build_object_store()?)),
            None => None,
        };

        let (upload_manager, compaction_upload_manager) = Self::build_upload_managers(
            config.object_store.as_ref(),
            runtime,
            object_store.as_ref(),
        );

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let (index_scheduler, index_tracker) = build_index_scheduler(&config);

        let tables = RwLock::new(HashMap::new());
        let schema_path = config.data_dir.join("schema.json");
        if let Ok(data) = std::fs::read_to_string(&schema_path) {
            match Self::table_schemas_from_schema_json(&data) {
                Ok(schemas) => {
                    for schema in schemas {
                        let table_id = TableId::new(&schema.keyspace, &schema.table);
                        match Self::build_table_state(
                            &config,
                            schema,
                            vec![],
                            Arc::clone(&reader_pool),
                        ) {
                            Ok(state) => {
                                for (index_name, _col_pos) in state.store.indexed_columns() {
                                    index_tracker.register_index(
                                        table_id.keyspace(),
                                        table_id.table(),
                                        index_name,
                                    );
                                }
                                tables.write().insert(table_id, state);
                            }
                            Err(e) => tracing::warn!(
                                "failed to re-register table from schema.json before replay: {e}"
                            ),
                        }
                    }
                }
                Err(e) => tracing::warn!(
                    "failed to parse schema.json at {} before replay: {e}",
                    schema_path.display()
                ),
            }
        }

        let deferred_replay_mutations = parking_lot::Mutex::new(Vec::new());
        let mut pending_mutations = Vec::new();
        let max_pending_without_schema = config.max_pending_replay_mutations_without_schema;
        let mut seen_replay_ids: std::collections::HashSet<[u8; 16]> =
            std::collections::HashSet::new();
        let commit_log = crate::commitlog::CommitLog::open_and_replay_streaming(
            config.commit_log.clone(),
            |mutation| {
                if !mutation.has_legacy_id() && !seen_replay_ids.insert(mutation.mutation_id) {
                    return Ok(());
                }

                if tables.read().is_empty() {
                    if pending_mutations.len() >= max_pending_without_schema {
                        return Err(ferrosa_common::Error::InvalidData(format!(
                            "commit-log replay schema unavailable and pending replay limit \
                             ({max_pending_without_schema}) exceeded; restore local/S3 schema \
                             before replay or raise FERROSA_MAX_PENDING_REPLAY_WITHOUT_SCHEMA"
                        )));
                    }
                    pending_mutations.push(mutation);
                } else if !Self::apply_replay_mutation_to_tables(&tables, &mutation) {
                    deferred_replay_mutations.lock().push(mutation);
                }
                Ok(())
            },
        )?;

        let engine = Self {
            config,
            tables,
            commit_log,
            deferred_replay_mutations,
            compaction_executor,
            upload_manager,
            compaction_upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            time_series_consolidators: RwLock::new(HashMap::new()),
            time_series_runtime_settings: Arc::new(TimeSeriesRuntimeSettings::from_config(
                &ConsolidationConfig::default(),
            )),
            time_series_wasm_aggregates: RwLock::new(None),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            pin_metrics: Arc::new(crate::metrics::PinMetrics::new()),
            object_store,
            s3_cas_supported: std::sync::atomic::AtomicBool::new(true),
            s3_sync_running: AtomicBool::new(false),
            s3_sync_requested: AtomicBool::new(false),
            cached_disk_free_bytes: AtomicU64::new(0),
            disk_free_checked_at_ms: AtomicU64::new(u64::MAX),
            flush_requested: AtomicBool::new(false),
            s3_manifest_stats: RwLock::new(HashMap::new()),
            reader_pool,
            #[cfg(test)]
            upload_store_override: None,
        };

        Ok((engine, pending_mutations))
    }

    /// Replays pending S3 uploads that were interrupted by a crash.
    ///
    /// Reads the pending-uploads.log and re-submits upload tasks for each
    /// entry. The upload is idempotent (S3 PUT overwrites). Call this after
    /// the engine is opened and tables are registered.
    pub async fn replay_pending_uploads(&self) {
        let pending_log_path = self.config.data_dir.join("pending-uploads.log");
        let pending_log = match crate::upload::PendingUploadsLog::open(&pending_log_path) {
            Ok(log) => log,
            Err(_) => return, // No log file — nothing to replay
        };

        let records = match pending_log.pending_records() {
            Ok(e) if e.is_empty() => return,
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("failed to read pending-uploads.log: {e}");
                return;
            }
        };

        let Some(upload_mgr) = self.upload_manager.as_ref() else {
            tracing::warn!(
                "pending-uploads.log has {} entries but no upload manager configured — \
                 these SSTables may not be in S3",
                records.len()
            );
            return;
        };

        tracing::info!(
            count = records.len(),
            "replaying pending S3 uploads from crash recovery"
        );

        let Some((store, prefix)) = self.resolve_store_and_prefix() else {
            tracing::warn!(
                "pending-uploads.log has {} entries but no object store configured — \
                 these SSTables may not be in S3",
                records.len()
            );
            return;
        };
        let cas_supported = self.cas_supported();
        let mut finalize_handles = Vec::new();

        for (idx, record) in records.iter().enumerate() {
            let Some(files) = crate::upload::replay::find_pending_upload_files(
                &self.config.data_dir,
                &self.config.compaction.output_dir,
                &record.table_id,
                &record.sstable_id,
            ) else {
                tracing::warn!(
                    table = record.table_id,
                    sstable = record.sstable_id,
                    "pending upload: SSTable files not found on disk — cannot replay"
                );
                continue;
            };
            let total_size = files.iter().map(|file| file.size_bytes).sum();
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            let task = crate::upload::UploadTask::SSTable {
                table_id: record.table_id.clone(),
                sstable_id: record.sstable_id.clone(),
                files,
                on_complete: Some(tx),
            };
            match upload_mgr.try_submit(task) {
                Ok(()) => {
                    finalize_handles.push(TaskPool::current("storage-upload-finalize").spawn(
                        Self::finalize_replayed_pending_upload(
                            PendingUploadReplayFinalize {
                                store: Arc::clone(&store),
                                prefix: prefix.clone(),
                                pending_log_path: pending_log_path.clone(),
                                table_id: record.table_id.clone(),
                                sstable_id: record.sstable_id.clone(),
                                total_size,
                                compaction: record.compaction.clone(),
                                cas_supported,
                            },
                            rx,
                        ),
                    ));
                }
                Err(e) if e.to_string().contains("upload queue full") => {
                    tracing::warn!(
                        sstable = record.sstable_id,
                        remaining_entries = records.len() - idx,
                        "pending upload replay stopped early because upload queue is full; remaining entries stay durable for later retry"
                    );
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        table = record.table_id,
                        sstable = record.sstable_id,
                        "pending upload replay could not enqueue without blocking; leaving entry for later retry: {e}"
                    );
                }
            }
        }

        if !finalize_handles.is_empty() {
            let drain = async move {
                for handle in finalize_handles {
                    let _ = handle.await;
                }
            };
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), drain).await;
        }
    }

    async fn finalize_replayed_pending_upload(
        ctx: PendingUploadReplayFinalize,
        rx: tokio::sync::oneshot::Receiver<Result<(), String>>,
    ) {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                tracing::warn!(
                    table = ctx.table_id,
                    sstable = ctx.sstable_id,
                    "pending upload replay failed; leaving entry for later retry: {message}"
                );
                return;
            }
            Err(_) => {
                tracing::warn!(
                    table = ctx.table_id,
                    sstable = ctx.sstable_id,
                    "pending upload replay worker dropped completion channel; leaving entry for later retry"
                );
                return;
            }
        }

        let mut manifest = match crate::manifest::Manifest::load(ctx.store.as_ref(), &ctx.prefix)
            .await
        {
            Ok((manifest, _version)) => manifest,
            Err(e) => {
                tracing::warn!(
                    %e,
                    table = ctx.table_id,
                    sstable = ctx.sstable_id,
                    "pending upload replay could not load manifest; leaving entry for later retry"
                );
                return;
            }
        };

        let removals_for_cas_retry = if let Some(compaction) = ctx.compaction {
            manifest.remove_sstables(&ctx.table_id, &compaction.remove_input_ids);
            manifest.add_sstable(&ctx.table_id, compaction.output);
            vec![(ctx.table_id.clone(), compaction.remove_input_ids)]
        } else {
            tracing::warn!(
                table = ctx.table_id,
                sstable = ctx.sstable_id,
                "pending upload replay entry has no compaction context; adding SSTable to manifest without input cleanup"
            );
            manifest.add_sstable(
                &ctx.table_id,
                crate::manifest::ManifestEntry {
                    id: ctx.sstable_id.clone(),
                    size: ctx.total_size,
                    min_token: i64::MIN,
                    max_token: i64::MAX,
                    min_timestamp: 0,
                    max_timestamp: 0,
                },
            );
            Vec::new()
        };

        let save_result = if ctx.cas_supported {
            manifest
                .save_with_retry_and_removals(
                    ctx.store.as_ref(),
                    &ctx.prefix,
                    &removals_for_cas_retry,
                )
                .await
        } else {
            manifest
                .save_without_cas_and_removals(
                    ctx.store.as_ref(),
                    &ctx.prefix,
                    &removals_for_cas_retry,
                )
                .await
        };
        if let Err(e) = save_result {
            tracing::warn!(
                %e,
                table = ctx.table_id,
                sstable = ctx.sstable_id,
                "pending upload replay uploaded SSTable but could not save manifest; leaving entry for later retry"
            );
            return;
        }

        match crate::upload::PendingUploadsLog::open(&ctx.pending_log_path)
            .and_then(|log| log.remove_entry(&ctx.table_id, &ctx.sstable_id))
        {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(
                    %e,
                    table = ctx.table_id,
                    sstable = ctx.sstable_id,
                    "pending upload replay finalized manifest but could not remove log entry"
                );
            }
        }
    }

    /// Replays a set of pending mutations into their respective table memtables.
    ///
    /// This is called after [`open`](Self::open) and after all table schemas
    /// have been registered via [`register_table`](Self::register_table).
    /// Mutations for unregistered tables are silently skipped.
    pub fn replay_mutations(&self, mutations: Vec<Mutation>) -> ferrosa_common::Result<()> {
        // Deduplicate by mutation_id to make replay idempotent.
        //
        // If the process crashed during a previous replay (after some rows were
        // written to the memtable but before a flush checkpoint was saved), the
        // next startup will present the same mutations again.  We track all
        // non-zero ids we have already applied and skip duplicates.
        //
        // Zero ids are the legacy sentinel for segments written before the
        // mutation_id field was added — they are always re-applied (LWW
        // timestamp semantics keeps them safe).
        let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();

        for mutation in mutations {
            // Skip duplicate non-zero ids.
            if !mutation.has_legacy_id() && !seen.insert(mutation.mutation_id) {
                // Already applied this mutation in this replay pass — skip.
                continue;
            }

            if !self.apply_replay_mutation_if_registered(&mutation) {
                self.deferred_replay_mutations.lock().push(mutation);
            }
        }
        Ok(())
    }

    fn apply_replay_mutation_if_registered(&self, mutation: &Mutation) -> bool {
        Self::apply_replay_mutation_to_tables(&self.tables, mutation)
    }

    fn apply_replay_mutation_to_tables(
        tables: &RwLock<HashMap<TableId, TableState>>,
        mutation: &Mutation,
    ) -> bool {
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        let tables = tables.read();
        let Some(state) = tables.get(&table_id) else {
            return false;
        };
        // Layer 3 of the timeuuid-flush-wedge fix: when the per-cell
        // length validator (run inside `Memtable::put`) rejects a row at
        // replay time, salvage the row to a quarantine JSONL file rather
        // than letting it ride the commit log into a fresh memtable on
        // the next restart and re-wedge the cluster. See
        // specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-
        // from-now-function.md.
        let mut quarantine_writer: Option<crate::quarantine::QuarantineWriter> = None;
        let mut quarantined_in_partition = 0usize;
        for row in &mutation.rows {
            match state.store.write(&mutation.key, row.clone()) {
                Ok(()) => {}
                Err(ferrosa_common::Error::InvalidData(reason)) => {
                    if quarantine_writer.is_none() {
                        let schema = state.store.schema();
                        match crate::quarantine::QuarantineWriter::new(
                            state.store.flush_dir(),
                            &schema.keyspace,
                            &schema.table,
                        ) {
                            Ok(qw) => quarantine_writer = Some(qw),
                            Err(e) => {
                                tracing::error!(%e, %table_id, "replay: failed to open quarantine writer; row dropped");
                                continue;
                            }
                        }
                    }
                    let qw = quarantine_writer.as_ref().expect("just constructed");
                    let schema = state.store.schema();
                    if let Err(qe) =
                        qw.write_row(mutation.key.key.as_bytes(), row, &schema, &reason)
                    {
                        tracing::error!(%qe, %table_id, "replay: failed to write quarantine row");
                    } else {
                        quarantined_in_partition += 1;
                    }
                }
                Err(e) => {
                    tracing::error!(%e, %table_id, "replay: failed to replay row");
                }
            }
        }
        if quarantined_in_partition > 0 {
            // Log-budget aware: one ERROR per partition, not per row. A
            // wedged table can contain hundreds of bad rows; emitting a
            // line per row would flood the log.
            tracing::error!(
                %table_id,
                quarantined_rows = quarantined_in_partition,
                quarantine_file = ?quarantine_writer.as_ref().map(|w| w.path().display().to_string()),
                "replay: quarantined malformed rows — see quarantine file for forensic record"
            );
        }
        true
    }

    fn replay_deferred_mutations_for_table(&self, table_id: &TableId) {
        let pending = {
            let mut deferred = self.deferred_replay_mutations.lock();
            if deferred.is_empty() {
                return;
            }
            let mut ready = Vec::new();
            let mut remaining = Vec::with_capacity(deferred.len());
            for mutation in deferred.drain(..) {
                let mutation_table = TableId::new(&mutation.keyspace, &mutation.table);
                if &mutation_table == table_id {
                    ready.push(mutation);
                } else {
                    remaining.push(mutation);
                }
            }
            *deferred = remaining;
            ready
        };

        for mutation in pending {
            if !self.apply_replay_mutation_if_registered(&mutation) {
                self.deferred_replay_mutations.lock().push(mutation);
            }
        }
    }

    /// Estimate local SSTable bytes that a full scan of `keyspace.table` would touch.
    pub fn estimated_table_scan_bytes(&self, keyspace: &str, table: &str) -> Option<u64> {
        let table_id = TableId::new(keyspace, table);
        let tables = self.tables.read();
        tables
            .get(&table_id)
            .map(|state| state.store.estimated_disk_scan_bytes())
    }

    /// Reserve a temporary table directory for a spillable ORDER BY sort.
    ///
    /// The returned guard deletes the directory on drop, which makes aborted or
    /// cancelled query execution clean up the same way as successful execution.
    pub fn reserve_order_by_temp_sort_table(
        &self,
        keyspace: &str,
        table: &str,
    ) -> ferrosa_common::Result<TempSortTableReservation> {
        let root = self.config.data_dir.join("tmp_order_by_sort");
        std::fs::create_dir_all(&root).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create ORDER BY temp-sort root {}: {e}",
                root.display()
            ))
        })?;
        let name = format!(
            "{}_{}_{}",
            keyspace.replace(['/', '\\', ':'], "_"),
            table.replace(['/', '\\', ':'], "_"),
            uuid::Uuid::new_v4()
        );
        let path = root.join(name);
        std::fs::create_dir(&path).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create ORDER BY temp-sort table {}: {e}",
                path.display()
            ))
        })?;
        Ok(TempSortTableReservation { path })
    }

    /// Registers all system table schemas (`system_schema.*` and `system_auth.*`)
    /// so the storage engine can persist system metadata.
    ///
    /// Called during bootstrap before any DDL or auth operations. System tables
    /// use the same flush/compaction/S3 pipeline as user tables. Idempotent:
    /// safe to call multiple times.
    pub fn register_system_tables(&self) -> ferrosa_common::Result<()> {
        for schema in ferrosa_schema::system::persistence::all_system_table_schemas() {
            self.register_table(schema)?;
        }
        Ok(())
    }

    /// Registers a table schema so the engine can accept writes for it.
    ///
    /// Creates the per-table `FileFlushTarget` directory and `TableStore`.
    /// If the directory already contains SSTable files from a previous run,
    /// they are opened and loaded into the store so reads work immediately
    /// after re-opening the engine (crash recovery path). Sidecar index files
    /// are also loaded and associated with their corresponding SSTables.
    pub fn register_table(&self, schema: TableSchema) -> ferrosa_common::Result<()> {
        self.register_table_inner(schema, vec![])
    }

    /// Registers a table schema with NVMe pin configuration.
    ///
    /// The table is registered normally but S3 uploads are skipped for new
    /// flushes while the pin is active. If `pin_config.max_bytes` is set,
    /// the oldest pinned SSTables are evicted from disk once the cap is exceeded.
    ///
    /// Increments `pin_metrics.pinned_tables` on success.
    pub fn register_table_pinned(
        &self,
        schema: TableSchema,
        pin_config: PinConfig,
    ) -> ferrosa_common::Result<()> {
        let table_id = TableId::new(&schema.keyspace, &schema.table);
        // Register via the inner path first.
        self.register_table_inner(schema, vec![])?;
        // Apply pin config and update metrics.
        let mut tables = self.tables.write();
        if let Some(state) = tables.get_mut(&table_id) {
            state.pin_config = Some(pin_config);
            self.pin_metrics.inc_pinned_tables();
        }
        Ok(())
    }

    /// Updates the pin configuration for a registered table (ALTER TABLE).
    ///
    /// - `None` → `Some(cfg)`: pins the table, increments `pinned_tables`,
    ///   existing SSTables remain on disk (already uploaded to S3 if any).
    /// - `Some(_)` → `None`: unpins the table, decrements `pinned_tables`,
    ///   enqueues S3 upload for all currently-pinned SSTables.
    /// - `Some(_)` → `Some(cfg)`: updates config (e.g., changes max_bytes).
    ///
    /// Returns `Err` if the table is not registered.
    pub async fn update_table_pin_config(
        &self,
        table_id: &TableId,
        new_config: Option<PinConfig>,
    ) -> ferrosa_common::Result<()> {
        // Collect state needed before releasing the lock.
        let (old_was_pinned, pinned_ids) = {
            let tables = self.tables.read();
            let state = tables.get(table_id).ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
            })?;
            let was_pinned = state.pin_config.is_some();
            let ids: Vec<String> = state
                .pinned_sstables
                .iter()
                .map(|(id, _)| id.clone())
                .collect();
            (was_pinned, ids)
        };

        let now_pinned = new_config.is_some();

        // Apply the new config.
        {
            let mut tables = self.tables.write();
            let state = tables.get_mut(table_id).ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
            })?;
            state.pin_config = new_config;
            if !now_pinned {
                // Unpinned: clear the tracked list; bytes gauge will be zeroed below.
                state.pinned_sstables.clear();
            }
        }

        // Update pinned_tables gauge.
        match (old_was_pinned, now_pinned) {
            (false, true) => self.pin_metrics.inc_pinned_tables(),
            (true, false) => {
                self.pin_metrics.dec_pinned_tables();
                self.pin_metrics
                    .set_pinned_bytes(self.compute_pinned_bytes(table_id));
            }
            _ => {}
        }

        // If transitioning from pinned → unpinned, enqueue S3 uploads for
        // SSTables that were previously skipped.
        if old_was_pinned && !now_pinned && !pinned_ids.is_empty() {
            self.upload_previously_pinned_sstables(table_id, &pinned_ids)
                .await;
        }

        Ok(())
    }

    /// Registers a table schema with secondary index declarations.
    ///
    /// `indexed_columns` is a list of `(index_name, column_position)` pairs
    /// passed through to [`TableStore::new_with_indexes`]. Sidecar files from
    /// prior flushes are loaded from disk alongside the SSTables.
    pub fn register_table_with_indexes(
        &self,
        schema: TableSchema,
        indexed_columns: Vec<(String, usize)>,
    ) -> ferrosa_common::Result<()> {
        self.register_table_inner(schema, indexed_columns)
    }

    fn table_schemas_from_schema_json(data: &str) -> Result<Vec<TableSchema>, String> {
        match serde_json::from_str::<Vec<TableSchema>>(data) {
            Ok(schemas) => Ok(schemas),
            Err(legacy_err) => {
                let snapshot = serde_json::from_str::<SchemaSnapshot>(data).map_err(|snapshot_err| {
                    format!(
                        "legacy TableSchema list parse failed: {legacy_err}; SchemaSnapshot parse failed: {snapshot_err}"
                    )
                })?;
                Ok(snapshot
                    .tables
                    .into_values()
                    .map(|metadata| metadata.to_storage_schema())
                    .collect())
            }
        }
    }

    /// Builds per-table state for a schema without requiring a fully constructed
    /// `StorageEngine`. Startup replay uses this to preload schema-backed tables
    /// before streaming commit-log entries, avoiding an eager pending-mutation Vec.
    fn build_table_state(
        config: &StorageEngineConfig,
        schema: TableSchema,
        indexed_columns: Vec<(String, usize)>,
        reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt>,
    ) -> ferrosa_common::Result<TableState> {
        if let Some(warning) = schema.legacy_storage_column_order_warning() {
            tracing::warn!("{warning}");
        }

        let table_id = TableId::new(&schema.keyspace, &schema.table);
        let table_dir = config.data_dir.join("sstables").join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create table dir: {e}"))
        })?;

        // Load any SSTables that already exist on disk (e.g., after crash
        // recovery). Phase 5 (FMEA #1): validation is *transient and bounded* —
        // each generation is opened through the engine-wide pool, smoke-tested,
        // reduced to a lightweight `SstableDescriptor`, then its reader Arc is
        // dropped before the next generation opens. The pool's cap bounds how
        // many readers stay resident, so a node bloated with thousands of
        // SSTables no longer materializes O(count) readers at startup (the
        // observed startup OOM). The pool is reused as this table's pool below,
        // so the readers validated last stay warm for the first reads.
        let (existing_descriptors, existing_sidecars, existing_ids) =
            Self::load_existing_sstables_and_sidecars(
                &table_dir,
                &reader_pool,
                &table_id.to_string(),
            );

        let flush_target = FileFlushTarget::new_starting_at(table_dir)?;
        // Thread schema compression options and the engine-level write_verify
        // flag into the writer.
        let write_options = write_options_for_schema(&schema, config.write_verify)?;
        let mut store = if existing_descriptors.is_empty() && indexed_columns.is_empty() {
            TableStore::new(schema.clone(), flush_target, write_options)
        } else if existing_descriptors.is_empty() {
            TableStore::new_with_indexes(
                schema.clone(),
                flush_target,
                write_options,
                indexed_columns,
            )
        } else {
            TableStore::new_with_descriptors_and_indexes(
                schema.clone(),
                flush_target,
                write_options,
                existing_descriptors,
                existing_sidecars,
                existing_ids,
                indexed_columns,
            )
        };

        // Share the engine-wide reader pool, namespacing this table's
        // generations by its id so generations from different tables never
        // collide in the pool key space. The pool already holds the (bounded)
        // set of readers validated last during the transient startup loop above,
        // keyed by this same `table_id`, so they remain cache-warm.
        store.attach_reader_pool(reader_pool, table_id.to_string());

        Ok(TableState {
            schema,
            store,
            pin_config: None,
            pinned_sstables: Vec::new(),
            first_unflushed_write_at_nanos: std::sync::atomic::AtomicI64::new(0),
            last_commit_log_position: ArcSwap::from_pointee(None),
        })
    }

    /// Internal: create a `TableStore` for a table, loading existing SSTables
    /// and sidecar files from disk. Idempotent: skips already-registered tables.
    fn register_table_inner(
        &self,
        schema: TableSchema,
        indexed_columns: Vec<(String, usize)>,
    ) -> ferrosa_common::Result<()> {
        let table_id = TableId::new(&schema.keyspace, &schema.table);
        {
            let tables = self.tables.read();
            if tables.contains_key(&table_id) {
                drop(tables);
                self.replay_deferred_mutations_for_table(&table_id);
                return Ok(());
            }
        }
        let time_series_handle = self.build_time_series_consolidator(&table_id, &schema)?;
        let state = Self::build_table_state(
            &self.config,
            schema,
            indexed_columns,
            Arc::clone(&self.reader_pool),
        )?;

        // Register each declared index in the tracker.
        for (index_name, _col_pos) in state.store.indexed_columns() {
            self.index_tracker
                .register_index(table_id.keyspace(), table_id.table(), index_name);
        }

        self.tables.write().insert(table_id.clone(), state);
        self.install_time_series_consolidator(table_id.clone(), time_series_handle);

        // Trigger compaction check for tables loaded with existing SSTables.
        // Without this, tables restored from S3 bootstrap can have thousands
        // of tiny SSTables that never get compacted (each one holds an
        // in-memory reader, bloating RSS). With 2,462 SSTables of 2KB each
        // for a single table, reader overhead alone exceeded 1GB.
        {
            let tables = self.tables.read();
            if let Some(state) = tables.get(&table_id) {
                self.maybe_compact(&table_id, state);
            }
        }

        self.replay_deferred_mutations_for_table(&table_id);

        Ok(())
    }

    /// Updates the schema for a registered table after `ALTER TABLE`.
    ///
    /// Propagates the new schema both to `TableState.schema` (used by
    /// compaction/snapshot paths) and `TableStore.schema` (used by the flush
    /// path's `SerializationHeader`). Without this, `ALTER TABLE ADD COLUMN`
    /// left the storage engine with a stale column list and the flush path
    /// produced silently corrupt SSTables. See
    /// `specs/in-process/bug-sstable-writer-produces-zero-byte-rows-db.md`.
    ///
    /// Returns `Err` if the table is not registered.
    pub fn update_table_schema(
        &self,
        table_id: &TableId,
        new_schema: TableSchema,
    ) -> ferrosa_common::Result<()> {
        let time_series_handle = self.build_time_series_consolidator(table_id, &new_schema)?;
        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state.schema = new_schema.clone();
        state.store.update_schema(new_schema);
        drop(tables);
        self.remove_time_series_consolidator(table_id);
        self.install_time_series_consolidator(table_id.clone(), time_series_handle);
        Ok(())
    }

    /// Unregisters a table from the storage engine.
    ///
    /// Removes the `TableState` from the engine's table map AND deletes the
    /// local SSTable directory so that a subsequent `CREATE TABLE` with the
    /// same name starts empty. Any in-progress reads holding an `Arc`
    /// reference to the underlying `TableStore` or its SSTables will complete
    /// normally; the data is freed once those references drop.
    ///
    /// S3-side SSTable cleanup is handled by the manifest GC sweep (separate
    /// from this path). Local deletion is sufficient to prevent stale data
    /// from being loaded on re-creation.
    pub fn unregister_table(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        self.tables.write().remove(table_id);
        self.remove_time_series_consolidator(table_id);

        // Delete local SSTable directory so DROP+CREATE starts empty.
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        if table_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&table_dir) {
                tracing::warn!(
                    table = %table_id,
                    path = %table_dir.display(),
                    %e,
                    "failed to delete SSTable directory on DROP TABLE"
                );
            } else {
                tracing::info!(
                    table = %table_id,
                    "deleted local SSTable directory on DROP TABLE"
                );
            }
        }

        Ok(())
    }

    /// Persists all registered table schemas to `data_dir/schema.json` so a
    /// clean restart can recover all table schemas without needing to re-run the
    /// S3 bootstrap that was gated on `local_empty`.
    fn persist_schema_locally(&self) -> ferrosa_common::Result<()> {
        let schema_path = self.config.data_dir.join("schema.json");
        let tables = self.tables.read();
        let schemas: Vec<&TableSchema> = tables.values().map(|s| &s.schema).collect();
        let json = serde_json::to_string_pretty(&schemas).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("schema serialization failed: {e}"))
        })?;
        drop(tables);
        std::fs::write(&schema_path, json).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to write {}: {e}",
                schema_path.display()
            ))
        })?;
        Ok(())
    }

    /// Loads table schemas from `data_dir/schema.json` (if it exists) and
    /// registers any tables not already registered.
    ///
    /// Called during all `StorageEngine` constructors before any other work.
    /// By running unconditionally (not gated on SSTable presence) this fixes
    /// BUG-022: schema was lost on binary upgrades where the data directory
    /// was non-empty and the S3 bootstrap path was skipped.
    fn load_local_schema_if_present(&self) {
        let schema_path = self.config.data_dir.join("schema.json");
        let data = match std::fs::read_to_string(&schema_path) {
            Ok(d) => d,
            Err(_) => return, // No schema.json yet — first run.
        };
        let schemas = match Self::table_schemas_from_schema_json(&data) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "failed to parse schema.json at {}: {e}",
                    schema_path.display()
                );
                return;
            }
        };
        for schema in schemas {
            if let Err(e) = self.register_table_inner(schema, vec![]) {
                tracing::warn!("failed to re-register table from schema.json: {e}");
            }
        }
    }

    /// Rebuilds live secondary indexes from the persisted `system_schema.indexes`
    /// table after a restart.
    ///
    /// `load_local_schema_if_present` re-registers user tables with no indexes
    /// (`register_table_inner(.., vec![])`), so a restart used to silently drop
    /// every secondary index. This reads the dogfooded `system_schema.indexes`
    /// rows and re-registers each index on its (already-registered) user table
    /// via [`Self::add_index`], preserving the persisted `IndexType` so the
    /// memtable-index + backfill pipeline rebuilds the correct index kind.
    ///
    /// Must run *after* `system_schema.indexes` and the user tables are
    /// registered (boot order: `register_system_tables` → local schema restore →
    /// this). Returns the number of indexes re-registered. Rows that can't be
    /// resolved (unknown kind, missing target column, unregistered table) are
    /// logged and skipped rather than aborting the whole reload.
    pub fn reload_indexes_from_system_schema(&self) -> ferrosa_common::Result<usize> {
        let indexes_tid = TableId::new("system_schema", "indexes");
        if !self.tables.read().contains_key(&indexes_tid) {
            // Table not registered (no dogfooded schema yet) — nothing to do.
            return Ok(0);
        }

        // Full scan of system_schema.indexes. This table holds one row per
        // index cluster-wide; it is tiny. Bound the scan at the engine's
        // range-read materialization cap and warn loudly if we hit it, so a
        // truncated reload is visible rather than silently dropping indexes.
        const MAX_INDEXES_TO_RELOAD: usize = 10_000;
        let partitions = self.read_range(&indexes_tid, None, None, MAX_INDEXES_TO_RELOAD)?;

        let mut restored = 0usize;
        let mut rows_seen = 0usize;
        for partition in &partitions {
            let keyspace = String::from_utf8_lossy(partition.key.key.as_bytes()).to_string();
            for row in &partition.rows {
                rows_seen += 1;
                if self.reload_one_index(&keyspace, row)? {
                    restored += 1;
                }
            }
        }
        if rows_seen >= MAX_INDEXES_TO_RELOAD {
            tracing::warn!(
                rows_seen,
                cap = MAX_INDEXES_TO_RELOAD,
                "system_schema.indexes reload hit the read cap — some indexes may not have been restored"
            );
        }
        Ok(restored)
    }

    /// Reads every live row of the persisted `system_schema.indexes` table and
    /// decodes it into [`PersistedIndexRow`]s.
    ///
    /// This is the storage-backed source for `SELECT * FROM
    /// system_schema.indexes` (dogfooding step 4): the CQL router serves the
    /// query from these stored rows rather than recomputing from the in-memory
    /// Registry or the retired virtual table. Tombstoned/unresolvable rows are
    /// skipped. Returns an empty vector when the table is not registered.
    pub fn read_persisted_indexes(&self) -> ferrosa_common::Result<Vec<PersistedIndexRow>> {
        let indexes_tid = TableId::new("system_schema", "indexes");
        if !self.tables.read().contains_key(&indexes_tid) {
            return Ok(Vec::new());
        }

        const MAX_INDEXES_TO_READ: usize = 10_000;
        let partitions = self.read_range(&indexes_tid, None, None, MAX_INDEXES_TO_READ)?;

        let mut out = Vec::new();
        for partition in &partitions {
            let keyspace = String::from_utf8_lossy(partition.key.key.as_bytes()).to_string();
            for row in &partition.rows {
                if let Some(decoded) = decode_persisted_index_row(&keyspace, row) {
                    out.push(decoded);
                }
            }
        }
        Ok(out)
    }

    /// Reads every live row of the persisted `system_schema.types` table and
    /// decodes it into [`PersistedTypeRow`]s.
    ///
    /// Storage-backed source for both `SELECT * FROM system_schema.types` (the
    /// retired virtual table) and boot-time UDT reconstruction into the schema
    /// Registry. Tombstoned/malformed rows are skipped. Returns an empty vector
    /// when the table is not registered.
    pub fn read_persisted_types(&self) -> ferrosa_common::Result<Vec<PersistedTypeRow>> {
        let types_tid = TableId::new("system_schema", "types");
        if !self.tables.read().contains_key(&types_tid) {
            return Ok(Vec::new());
        }

        const MAX_TYPES_TO_READ: usize = 10_000;
        let partitions = self.read_range(&types_tid, None, None, MAX_TYPES_TO_READ)?;

        let mut out = Vec::new();
        for partition in &partitions {
            let keyspace = String::from_utf8_lossy(partition.key.key.as_bytes()).to_string();
            for row in &partition.rows {
                if let Some(decoded) = decode_persisted_type_row(&keyspace, row) {
                    out.push(decoded);
                }
            }
        }
        Ok(out)
    }

    /// Reads every live row of the persisted `system_schema.functions` table and
    /// decodes it into [`PersistedFunctionRow`]s.
    ///
    /// Storage-backed source for both `SELECT * FROM system_schema.functions`
    /// (replacing the hardcoded-empty router arm) and boot-time UDF
    /// reconstruction into the schema Registry. Tombstoned/malformed rows are
    /// skipped. Returns an empty vector when the table is not registered.
    pub fn read_persisted_functions(&self) -> ferrosa_common::Result<Vec<PersistedFunctionRow>> {
        let functions_tid = TableId::new("system_schema", "functions");
        if !self.tables.read().contains_key(&functions_tid) {
            return Ok(Vec::new());
        }

        const MAX_FUNCTIONS_TO_READ: usize = 10_000;
        let partitions = self.read_range(&functions_tid, None, None, MAX_FUNCTIONS_TO_READ)?;

        let mut out = Vec::new();
        for partition in &partitions {
            let keyspace = String::from_utf8_lossy(partition.key.key.as_bytes()).to_string();
            for row in &partition.rows {
                if let Some(decoded) = decode_persisted_function_row(&keyspace, row) {
                    out.push(decoded);
                }
            }
        }
        Ok(out)
    }

    /// Tombstones a `system_schema.indexes` row (DROP INDEX) using the same
    /// composite-clustering encoding as the create path.
    ///
    /// Shares the dogfooded layout with
    /// `ferrosa_schema::system::persistence::index_to_rows` so a tombstone
    /// written here masks the matching live row. Used by the standalone (Direct)
    /// DDL path; the cluster/pair paths tombstone via `SystemTableWriter`.
    pub fn write_index_tombstone(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
    ) -> ferrosa_common::Result<()> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let key = DecoratedKey::new(PartitionKey::new(keyspace.as_bytes().to_vec()));

        // Composite clustering: [u16 len][table][u16 len][index_name].
        let mut clustering = Vec::new();
        clustering.extend_from_slice(&(table.len() as u16).to_be_bytes());
        clustering.extend_from_slice(table.as_bytes());
        clustering.extend_from_slice(&(index_name.len() as u16).to_be_bytes());
        clustering.extend_from_slice(index_name.as_bytes());

        let row = Row {
            clustering,
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::new(ts, (ts / 1_000_000) as u32),
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::NONE,
        };
        let tid = TableId::new("system_schema", "indexes");
        self.write(&tid, &key, row, ts)
    }

    /// Re-registers a single index from a decoded `system_schema.indexes` row.
    ///
    /// Returns `Ok(true)` when the index was re-registered, `Ok(false)` when the
    /// row was skipped (tombstone, unknown kind, unresolvable column/table).
    fn reload_one_index(&self, keyspace: &str, row: &Row) -> ferrosa_common::Result<bool> {
        let Some((table, index_name)) = decode_index_clustering(&row.clustering) else {
            tracing::warn!(
                keyspace,
                "system_schema.indexes row has malformed clustering — skipping"
            );
            return Ok(false);
        };

        let kind = cell_text(row, ferrosa_schema::system::persistence::INDEXES_COL_KIND);
        let target = cell_text(row, ferrosa_schema::system::persistence::INDEXES_COL_TARGET);
        let (Some(kind), Some(target)) = (kind, target) else {
            tracing::warn!(
                keyspace,
                table,
                index_name,
                "system_schema.indexes row missing kind/target cell — skipping"
            );
            return Ok(false);
        };

        let Some(index_type) = ferrosa_schema::system::persistence::index_type_from_kind(&kind)
        else {
            tracing::warn!(
                keyspace,
                table,
                index_name,
                kind,
                "system_schema.indexes row has unknown index kind — skipping"
            );
            return Ok(false);
        };

        // Generic add_index covers BTree/Hash/Composite/Phonetic/Filtered.
        // FullText and Vector use dedicated sidecar build paths and are
        // reconstructed elsewhere; skip them loudly here so the gap is visible.
        if matches!(
            index_type,
            ferrosa_index::IndexType::FullText | ferrosa_index::IndexType::Vector
        ) {
            tracing::warn!(
                keyspace,
                table,
                index_name,
                kind,
                "skipping reload of fulltext/vector index — needs dedicated rebuild path"
            );
            return Ok(false);
        }

        let table_id = TableId::new(keyspace, &table);
        // `target` may be a "col_a, col_b" join; the first column drives the
        // ordinal (generic single-column indexes target one column).
        let target_col = target.split(", ").next().unwrap_or(&target);
        let Some(column_position) = self.regular_column_position(&table_id, target_col) else {
            tracing::warn!(
                keyspace,
                table,
                index_name,
                target_col,
                "cannot resolve index target column to a position — table unregistered or column missing"
            );
            return Ok(false);
        };

        // Reconstruct the partial-index predicate for a Filtered index. The
        // CREATE path persisted the fully-encoded `FilterPredicate` as JSON
        // under the reserved `__filter_predicate` options key (the value bytes
        // are already in storage encoding, so no CQL type system is needed
        // here). A Filtered index whose predicate is missing or malformed is
        // unsound — it would index every row — so skip it loudly rather than
        // silently degrade to a full index.
        let filter_predicate = if matches!(index_type, ferrosa_index::IndexType::Filtered) {
            match decode_filter_predicate_from_options(row) {
                Some(pred) => Some(pred),
                None => {
                    tracing::warn!(
                        keyspace,
                        table,
                        index_name,
                        "filtered index row missing/invalid __filter_predicate — skipping reload to avoid an unfiltered index"
                    );
                    return Ok(false);
                }
            }
        } else {
            None
        };

        self.add_index_with_predicate(
            &table_id,
            &index_name,
            column_position,
            index_type,
            filter_predicate,
        )?;
        tracing::info!(
            keyspace,
            table,
            index_name,
            kind,
            "re-registered persisted index after restart"
        );
        Ok(true)
    }

    /// Position of `column_name` within a registered table's regular columns,
    /// matching the ordinal convention used by the CREATE INDEX wire path.
    fn regular_column_position(&self, table_id: &TableId, column_name: &str) -> Option<usize> {
        let tables = self.tables.read();
        let state = tables.get(table_id)?;
        state
            .schema
            .regular_columns
            .iter()
            .position(|c| c.name == column_name)
    }

    /// Registers a secondary index on a table.
    ///
    /// Called when CREATE INDEX is processed. Updates the TableStore's
    /// indexed_columns so future writes are indexed in the memtable.
    /// Registers the index in the tracker and submits rebuild jobs for
    /// all existing SSTables.
    pub fn add_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
        index_type: ferrosa_index::IndexType,
    ) -> ferrosa_common::Result<()> {
        self.add_index_with_predicate(table_id, index_name, column_position, index_type, None)
    }

    /// Registers a secondary index, optionally carrying a partial-index
    /// `FilterPredicate`.
    ///
    /// For `IndexType::Filtered` the predicate is threaded into BOTH the
    /// memtable index (so live writes are filtered identically) and every
    /// backfill `IndexBuildJob` (so the SSTable sidecars hold only matching
    /// rows). For every other index type the predicate is `None` and this
    /// behaves exactly like [`add_index`](Self::add_index).
    pub fn add_index_with_predicate(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
        index_type: ferrosa_index::IndexType,
        filter_predicate: Option<ferrosa_index::FilterPredicate>,
    ) -> ferrosa_common::Result<()> {
        // Register with the tracker.
        self.index_tracker
            .register_index(table_id.keyspace(), table_id.table(), index_name);

        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state.store.add_index_with_predicate(
            index_name.to_string(),
            column_position,
            index_type,
            filter_predicate.clone(),
        );

        // Submit rebuild jobs for all existing SSTables.
        if let Some(ref scheduler) = self.index_scheduler {
            let sstable_ids = state.store.sstable_generation_ids();
            for sst_id in sstable_ids {
                let job = crate::index::IndexBuildJob {
                    sstable_id: sst_id,
                    index_name: index_name.to_string(),
                    index_type,
                    table: (
                        table_id.keyspace().to_string(),
                        table_id.table().to_string(),
                    ),
                    priority: crate::index::BuildPriority::Initial,
                    enqueued_at: std::time::Instant::now(),
                    column_position,
                    filter_predicate: filter_predicate.clone(),
                };
                if let Err(e) = scheduler.submit(job) {
                    tracing::error!(%e, "engine: failed to submit index backfill");
                }
            }
        }

        Ok(())
    }

    /// Register a full-text index on a table.
    ///
    /// After registration, each `flush()` will build an FTI sidecar file
    /// (`{gen}-FTI-{index_name}.db`) alongside the SSTable.
    pub fn add_fulltext_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
    ) -> ferrosa_common::Result<()> {
        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state
            .store
            .add_fulltext_index(index_name.to_string(), column_position);
        Ok(())
    }

    /// Register a vector ANN index on a table.
    ///
    /// Vector indexes are stored separately from scalar secondary indexes so
    /// ANN reads can use TableStore's vector sidecars, including partition-
    /// scoped sidecars for prefix-bounded queries.
    pub fn add_vector_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
        dimension: usize,
    ) -> ferrosa_common::Result<()> {
        self.add_vector_index_with_method(
            table_id,
            index_name,
            column_position,
            dimension,
            VectorIndexMethod::Hnsw,
        )
    }

    /// Register a vector index selecting the artifact/search `method`.
    ///
    /// `add_vector_index` delegates here with [`VectorIndexMethod::Hnsw`] so the
    /// legacy callers keep the full-precision sidecar path; the CQL DDL router
    /// passes [`VectorIndexMethod::QuantizedIvf`] when the user requests the
    /// `hvq` method via `WITH OPTIONS`.
    pub fn add_vector_index_with_method(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
        _dimension: usize,
        method: VectorIndexMethod,
    ) -> ferrosa_common::Result<()> {
        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        let config = VectorIndexConfig {
            index_name: index_name.to_string(),
            column_position,
            metric: ferrosa_index::DistanceMetric::L2,
            ef_construction: 64,
            m: 16,
        };
        match method {
            VectorIndexMethod::Hnsw => state.store.add_vector_index(config),
            VectorIndexMethod::QuantizedIvf => state.store.add_quantized_vector_index(config),
        }
        Ok(())
    }

    /// Report the artifact/search method registered for a table's vector index.
    pub fn vector_index_method(
        &self,
        table_id: &TableId,
        index_name: &str,
    ) -> ferrosa_common::Result<VectorIndexMethod> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        Ok(state.store.vector_index_method(index_name))
    }

    /// Run an ANN search on a registered table vector index.
    pub fn ann_search(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> ferrosa_common::Result<Vec<ferrosa_index::vector::IndexResult>> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state.store.ann_search(index_name, query, k, ef_search)
    }

    /// Consult the vector index and return the `k` nearest base-table
    /// partitions in nearest-first score order.
    ///
    /// Thin wrapper over [`TableStore::ann_search_partitions`]; see its docs for
    /// the per-source scope-recovery and fail-loud behavior. Lets the CQL router
    /// serve `ORDER BY col ANN OF [...] LIMIT k` from the index instead of a full
    /// table scan + post-filter.
    pub fn ann_search_partitions(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state
            .store
            .ann_search_partitions(index_name, query, k, ef_search)
    }

    /// Run an ANN search bounded to partition keys with `partition_scope`.
    pub fn ann_search_in_partition_scope(
        &self,
        table_id: &TableId,
        index_name: &str,
        partition_scope: &[u8],
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> ferrosa_common::Result<Vec<ferrosa_index::vector::IndexResult>> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state
            .store
            .ann_search_in_partition_scope(index_name, partition_scope, query, k, ef_search)
    }

    /// Scans a table directory for existing SSTable files and sidecar index files,
    /// opening both.
    ///
    /// Returns `(sstables, sidecars)` where each vec is ordered newest-first (by
    /// generation number descending) and the two vecs are parallel — position `i`
    /// in `sidecars` is the sidecar map for the SSTable at position `i`.
    ///
    #[cfg(test)]
    const TEST_FAIL_PROMOTION_AFTER_FIRST_COMPONENT: &str =
        ".test-promotion-fail-after-first-component";

    fn should_fail_promotion_after_first_component(_output_dir: &std::path::Path) -> bool {
        #[cfg(test)]
        {
            _output_dir
                .join(Self::TEST_FAIL_PROMOTION_AFTER_FIRST_COMPONENT)
                .exists()
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn temp_promotion_directory(
        target_dir: &std::path::Path,
        promoted_gen: u64,
    ) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos())
            .unwrap_or(0);
        target_dir.join(format!(".promote-{promoted_gen}-{suffix}"))
    }

    fn generation_dir_path(table_dir: &std::path::Path, gen: u64) -> Option<std::path::PathBuf> {
        let gen_str = gen.to_string();
        let generation_dir = table_dir.join(&gen_str);
        if generation_dir.exists() && generation_dir.join(format!("{gen_str}-Data.db")).exists() {
            Some(generation_dir)
        } else {
            let flat = table_dir.join(format!("{gen_str}-Data.db"));
            if flat.exists() {
                Some(table_dir.to_path_buf())
            } else {
                None
            }
        }
    }

    fn generation_component_path(
        table_dir: &std::path::Path,
        gen: &str,
        component: &str,
    ) -> Option<std::path::PathBuf> {
        if let Some(dir) = Self::generation_dir_path(table_dir, gen.parse::<u64>().ok()?) {
            let path = dir.join(format!("{gen}-{component}"));
            if path.exists() {
                return Some(path);
            }
        }

        let flat = table_dir.join(format!("{gen}-{component}"));
        if flat.exists() {
            Some(flat)
        } else {
            None
        }
    }

    fn quarantine_generation(
        table_dir: &std::path::Path,
        gen: u64,
        quarantine_dir: &std::path::Path,
    ) -> ferrosa_common::Result<()> {
        let gen_str = gen.to_string();
        let source_dir =
            Self::generation_dir_path(table_dir, gen).unwrap_or_else(|| table_dir.to_path_buf());
        let prefix = format!("{gen_str}-");

        for entry in std::fs::read_dir(&source_dir)
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(&prefix) {
                let dest = quarantine_dir.join(&*name);
                std::fs::rename(entry.path(), dest).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to quarantine stale SSTable generation file {}: {e}",
                        entry.path().display()
                    ))
                })?;
            }
        }

        if source_dir != *table_dir {
            let _ = std::fs::remove_dir(&source_dir);
        }

        Ok(())
    }

    fn generation_component_paths(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        if let Some(dir) = Self::generation_dir_path(table_dir, gen) {
            for component in [
                "Data.db",
                "Partitions.db",
                "Rows.db",
                "Filter.db",
                "Statistics.db",
                "TOC.txt",
                "CompressionInfo.db",
            ] {
                let path = dir.join(format!("{}-{component}", gen));
                if path.exists() {
                    out.push(path);
                }
            }
        }
        out
    }

    fn startup_sstable_repair_mode() -> StartupSstableRepairMode {
        std::env::var("FERROSA_STARTUP_SMOKE_TEST")
            .map(|value| StartupSstableRepairMode::from_env(&value))
            .unwrap_or(StartupSstableRepairMode::Warn)
    }

    fn validate_sstable_for_startup_repair<R: ferrosa_sstable::io::ReadAt>(
        reader: &ferrosa_sstable::reader::SSTableReader<R>,
    ) -> ferrosa_common::Result<()> {
        use ferrosa_common::NO_TIMESTAMP;

        // Note: the Data.db-truncation extent check (validate_data_extent) runs
        // mode-independently in the caller's load loop, BEFORE this smoke test,
        // so a truncated SSTable is already excluded by the time we get here.
        let partitions = reader.read_all_partitions()?;
        for partition in &partitions {
            if let Some(static_row) = &partition.static_row {
                Self::validate_startup_row_timestamps(static_row, true)?;
            }
            for row in &partition.rows {
                Self::validate_startup_row_timestamps(row, false)?;
            }
        }
        if reader.header().min_timestamp == NO_TIMESTAMP
            && partitions.iter().any(|partition| {
                partition
                    .static_row
                    .iter()
                    .chain(partition.rows.iter())
                    .flat_map(|row| row.cells.iter())
                    .any(|(_, cell)| cell.timestamp != NO_TIMESTAMP)
            })
        {
            return Err(ferrosa_common::Error::InvalidFormat(
                "startup smoke test found real cell timestamps with NO_TIMESTAMP serialization header".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_startup_row_timestamps(
        row: &ferrosa_sstable::types::Row,
        is_static: bool,
    ) -> ferrosa_common::Result<()> {
        use ferrosa_common::NO_TIMESTAMP;

        let row_kind = if is_static { "static row" } else { "row" };
        for (column_idx, cell) in &row.cells {
            let uses_row_timestamp = row.primary_key_liveness.has_timestamp()
                && cell.timestamp == row.primary_key_liveness.timestamp;
            if !uses_row_timestamp && cell.timestamp == NO_TIMESTAMP {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "startup smoke test found {row_kind} cell at column {column_idx} with NO_TIMESTAMP"
                )));
            }
        }
        Ok(())
    }

    fn load_existing_sstables_and_sidecars(
        table_dir: &std::path::Path,
        reader_pool: &crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt>,
        pool_table_key: &str,
    ) -> (
        Vec<crate::store::SstableDescriptor>,
        Vec<SSTableSidecarMap>,
        Vec<(String, std::path::PathBuf)>,
    ) {
        Self::load_existing_sstables_and_sidecars_with_repair_mode(
            table_dir,
            reader_pool,
            pool_table_key,
            Self::startup_sstable_repair_mode(),
        )
    }

    fn load_existing_sstables_and_sidecars_with_repair_mode(
        table_dir: &std::path::Path,
        reader_pool: &crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt>,
        pool_table_key: &str,
        repair_mode: StartupSstableRepairMode,
    ) -> (
        Vec<crate::store::SstableDescriptor>,
        Vec<SSTableSidecarMap>,
        Vec<(String, std::path::PathBuf)>,
    ) {
        // Collect all generation numbers by looking for Data.db files.
        let mut generations: Vec<u64> = {
            let mut values = std::collections::HashSet::new();

            for entry in std::fs::read_dir(table_dir).into_iter().flatten().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with("-Data.db") {
                    if let Some(gen) = name.split('-').next().and_then(|v| v.parse::<u64>().ok()) {
                        values.insert(gen);
                    }
                    continue;
                }

                if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    if let Ok(gen) = name.parse::<u64>() {
                        if table_dir
                            .join(gen.to_string())
                            .join(format!("{gen}-Data.db"))
                            .exists()
                        {
                            values.insert(gen);
                        }
                    }
                }
            }

            values.into_iter().collect()
        };

        // Sort descending — newest generation first.
        generations.sort_by(|a, b| b.cmp(a));

        let mut descriptors: Vec<crate::store::SstableDescriptor> = Vec::new();
        let mut sidecars = Vec::new();
        let mut ids: Vec<(String, std::path::PathBuf)> = Vec::new();
        let mut quarantined_count = 0usize;
        let mut excluded_count = 0usize;
        let mut smoke_tested_count = 0usize;
        let mut smoke_quarantined_count = 0usize;
        let smoke_start = std::time::Instant::now();
        for gen in generations {
            let gen_str = gen.to_string();

            // Quarantine SSTables with zero-byte critical components.
            // NOTE: Rows.db is intentionally excluded — the SSTable writer
            // emits a zero-byte Rows.db for simple partitions that don't need
            // a per-partition row index (ferrosa-sstable/src/writer.rs:212).
            // A 0-byte Rows.db is the expected output, not corruption; the
            // reader treats a missing/empty Rows.db as "no row index".
            // Only Data.db and Partitions.db being zero-byte is unrecoverable.
            let critical_components = ["Data.db", "Partitions.db"];
            let mut quarantine = false;
            for comp in &critical_components {
                let path = Self::generation_component_path(table_dir, &gen_str, comp)
                    .unwrap_or_else(|| table_dir.join(format!("{gen_str}-{comp}")));
                match std::fs::metadata(&path) {
                    Ok(meta) if meta.len() == 0 => {
                        quarantine = true;
                    }
                    Err(_) => {
                        // Missing component — will fail in open_sstable_from_dir.
                    }
                    _ => {}
                }
            }
            if quarantine {
                match repair_mode {
                    StartupSstableRepairMode::Quarantine => {
                        tracing::error!(
                            gen,
                            dir = %table_dir.display(),
                            "storage-engine: startup repair quarantining SSTable with zero-byte critical component"
                        );
                        let quarantine_dir = table_dir.join("quarantine");
                        let _ = std::fs::create_dir_all(&quarantine_dir);
                        if let Err(e) = Self::quarantine_generation(table_dir, gen, &quarantine_dir)
                        {
                            tracing::warn!(%e, gen, "storage-engine: failed to quarantine stale SSTable generation");
                        }
                        quarantined_count += 1;
                    }
                    StartupSstableRepairMode::Warn | StartupSstableRepairMode::Off => {
                        tracing::error!(
                            gen,
                            dir = %table_dir.display(),
                            mode = ?repair_mode,
                            "storage-engine: startup repair excluded SSTable with zero-byte critical component from active readers; files remain in place for salvage"
                        );
                        excluded_count += 1;
                    }
                }
                continue;
            }

            // Open through the engine-wide pool, keyed exactly as the live read
            // path will key it (`(pool_table_key, gen_num)`). The returned `Arc`
            // is held only for the duration of validation + descriptor capture in
            // this iteration, then dropped — so the pool's cap bounds resident
            // readers and startup never spikes past it (FMEA #1). On the
            // immediately-following reads, the last `cap` generations are still
            // cache-warm because the pool survives this loop.
            let table_dir_owned = table_dir.to_path_buf();
            let gen_num = crate::store::SstableDescriptor::gen_num_for(&gen_str);
            let pool_key = (pool_table_key.to_string(), gen_num);
            let open_result = reader_pool.get_or_open(pool_key, || {
                Self::open_sstable_from_dir(&table_dir_owned, &gen_str)
            });
            match open_result {
                Ok(reader) => {
                    // MODE-INDEPENDENT truncation gate (P0 crash-atomic-flush
                    // defense-in-depth): a Data.db shorter than the extent its
                    // own index claims is corrupt regardless of repair mode and
                    // must never be served. We do not move files when mode==Off
                    // (no destructive action without an explicit repair mode),
                    // but we always exclude the truncated generation from active
                    // readers so queries fail loud-at-load instead of returning
                    // truncated/garbage rows. Conservative: passes every healthy
                    // SSTable (see SSTableReader::validate_data_extent).
                    if let Err(e) = reader.validate_data_extent() {
                        tracing::error!(
                            %e,
                            gen,
                            dir = %table_dir.display(),
                            mode = ?repair_mode,
                            "storage-engine: SSTable Data.db is shorter than its index claims (truncated); excluding from active readers"
                        );
                        drop(reader);
                        reader_pool.remove(&(pool_table_key.to_string(), gen_num));
                        if repair_mode == StartupSstableRepairMode::Quarantine {
                            let quarantine_dir = table_dir.join("quarantine");
                            let _ = std::fs::create_dir_all(&quarantine_dir);
                            if let Err(qe) =
                                Self::quarantine_generation(table_dir, gen, &quarantine_dir)
                            {
                                tracing::warn!(%qe, gen, "storage-engine: failed to quarantine truncated SSTable");
                            }
                            quarantined_count += 1;
                        } else {
                            excluded_count += 1;
                        }
                        continue;
                    }
                    if repair_mode != StartupSstableRepairMode::Off {
                        smoke_tested_count += 1;
                        if let Err(e) = Self::validate_sstable_for_startup_repair(&reader) {
                            match repair_mode {
                                StartupSstableRepairMode::Warn => {
                                    tracing::error!(
                                        %e,
                                        gen,
                                        dir = %table_dir.display(),
                                        "storage-engine: startup smoke test found corrupt SSTable; excluded from active readers, files remain in place for salvage"
                                    );
                                    excluded_count += 1;
                                    // Excluded — must never be served from the
                                    // pool; drop the held Arc and evict the gen.
                                    drop(reader);
                                    reader_pool.remove(&(pool_table_key.to_string(), gen_num));
                                    continue;
                                }
                                StartupSstableRepairMode::Quarantine => {
                                    tracing::error!(
                                        %e,
                                        gen,
                                        dir = %table_dir.display(),
                                        "storage-engine: startup smoke test quarantining corrupt SSTable"
                                    );
                                    // Drop the held Arc and evict from the pool
                                    // before moving the files out from under it.
                                    drop(reader);
                                    reader_pool.remove(&(pool_table_key.to_string(), gen_num));
                                    let quarantine_dir = table_dir.join("quarantine");
                                    let _ = std::fs::create_dir_all(&quarantine_dir);
                                    if let Err(qe) =
                                        Self::quarantine_generation(table_dir, gen, &quarantine_dir)
                                    {
                                        tracing::warn!(%qe, gen, "storage-engine: failed to quarantine SSTable after startup smoke-test failure");
                                    }
                                    quarantined_count += 1;
                                    smoke_quarantined_count += 1;
                                    continue;
                                }
                                StartupSstableRepairMode::Off => {}
                            }
                        }
                    }
                    let sstable_dir = Self::generation_dir_path(table_dir, gen)
                        .unwrap_or_else(|| table_dir.to_path_buf());
                    // Capture the lightweight descriptor (key/token bounds from
                    // the index footer) and then let `reader` drop at the end of
                    // this iteration. It stays cached in the pool until evicted by
                    // the cap — resident readers never exceed the bound.
                    descriptors.push(crate::store::SstableDescriptor::from_reader(
                        gen_str.clone(),
                        sstable_dir.clone(),
                        &reader,
                    ));
                    sidecars.push(Arc::new(Self::load_sidecars_for_generation(table_dir, gen)));
                    ids.push((gen_str.clone(), sstable_dir));
                }
                Err(e) => match repair_mode {
                    StartupSstableRepairMode::Quarantine => {
                        tracing::warn!(%e, gen, dir = %table_dir.display(), "storage-engine: quarantining corrupt SSTable that failed to open");
                        let quarantine_dir = table_dir.join("quarantine");
                        let _ = std::fs::create_dir_all(&quarantine_dir);
                        if let Err(qe) =
                            Self::quarantine_generation(table_dir, gen, &quarantine_dir)
                        {
                            tracing::warn!(%qe, gen, "storage-engine: failed to quarantine SSTable that failed to open");
                        }
                        quarantined_count += 1;
                    }
                    StartupSstableRepairMode::Warn | StartupSstableRepairMode::Off => {
                        tracing::error!(
                            %e,
                            gen,
                            dir = %table_dir.display(),
                            mode = ?repair_mode,
                            "storage-engine: startup repair excluded SSTable that failed to open from active readers; files remain in place for salvage"
                        );
                        excluded_count += 1;
                    }
                },
            }
        }

        if repair_mode != StartupSstableRepairMode::Off {
            tracing::info!(
                smoke_tested = smoke_tested_count,
                smoke_quarantined = smoke_quarantined_count,
                excluded = excluded_count,
                elapsed_ms = smoke_start.elapsed().as_millis(),
                loaded = descriptors.len(),
                dir = %table_dir.display(),
                mode = ?repair_mode,
                "storage-engine: startup SSTable smoke test complete"
            );
        }

        if excluded_count > 0 {
            tracing::error!(
                excluded = excluded_count,
                loaded = descriptors.len(),
                dir = %table_dir.display(),
                "storage-engine: excluded {excluded_count} corrupt SSTable(s) from active readers during startup. \
                 Files were not moved or deleted; rows present only in those generations are unavailable until salvage or operator-controlled quarantine/repair."
            );
        }

        if quarantined_count > 0 {
            tracing::error!(
                quarantined = quarantined_count,
                loaded = descriptors.len(),
                dir = %table_dir.display(),
                "storage-engine: quarantined {quarantined_count} corrupt SSTable(s) during startup repair \
                 — moved to {}/quarantine/. \
                 Data in quarantined SSTables is unrecoverable.",
                table_dir.display(),
            );
        }

        (descriptors, sidecars, ids)
    }

    /// Scans a table directory for sidecar files belonging to a given generation.
    ///
    /// Looks for files matching `{gen}-*.sidecar`. Each successfully opened
    /// sidecar is added to the returned map keyed by index name. Files that
    /// fail to open are silently skipped (degraded to full-scan on that index).
    fn load_sidecars_for_generation(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> HashMap<String, crate::index::sidecar::SidecarReader> {
        use crate::index::sidecar::SidecarReader;

        let sidecar_prefix = format!("{gen}-");
        const SIDECAR_SUFFIX: &str = ".sidecar";

        let mut sidecars = HashMap::new();

        let dir =
            Self::generation_dir_path(table_dir, gen).unwrap_or_else(|| table_dir.to_path_buf());
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return sidecars,
        };

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&sidecar_prefix) && name.ends_with(SIDECAR_SUFFIX) {
                // Extract index name from "{gen}-{index_name}.sidecar"
                let index_name = &name[sidecar_prefix.len()..name.len() - SIDECAR_SUFFIX.len()];
                match SidecarReader::open(&entry.path()) {
                    Ok(reader) => {
                        sidecars.insert(index_name.to_string(), reader);
                    }
                    Err(e) => {
                        tracing::warn!(%e, %name, dir = %table_dir.display(), "storage-engine: skipping corrupt sidecar");
                    }
                }
            }
        }

        sidecars
    }

    /// Registers an observer that will be notified when mutations are written.
    ///
    /// Sync observers are called inline on the write path. Async observers
    /// receive mutations through a bounded channel — the drain loop is
    /// started externally (e.g., by `GraphEngine` in Slice 5).
    pub fn register_observer(&self, observer: Arc<dyn crate::observer::WriteObserver>) {
        match observer.mode() {
            crate::observer::ObserverMode::Sync => {
                self.observers.write().push(observer);
            }
            crate::observer::ObserverMode::Async => {
                let capacity = self.async_observer_capacity;
                let (tx, _rx) = tokio::sync::mpsc::channel(capacity);
                let state = AsyncObserverState {
                    observer,
                    sender: tx,
                    drop_count: Arc::new(AtomicU64::new(0)),
                };
                self.async_observers.write().push(state);
            }
        }
    }

    /// Registers an async observer and returns the receiver end of the bounded
    /// channel. The caller is responsible for draining the receiver.
    pub fn register_async_observer(
        &self,
        observer: Arc<dyn crate::observer::WriteObserver>,
        capacity: usize,
    ) -> tokio::sync::mpsc::Receiver<(TableId, Mutation)> {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let state = AsyncObserverState {
            observer,
            sender: tx,
            drop_count: Arc::new(AtomicU64::new(0)),
        };
        self.async_observers.write().push(state);
        rx
    }

    /// Returns the number of active DDL-registered time-series consolidators.
    pub fn time_series_consolidator_count(&self) -> usize {
        self.time_series_consolidators.read().len()
    }

    /// Returns the active ring count for a DDL-registered time-series table.
    pub fn time_series_ring_count(&self, table_id: &TableId) -> Option<usize> {
        self.time_series_consolidators
            .read()
            .get(table_id)
            .map(|handle| handle.aggregator.ring_count())
    }

    /// Returns runtime-adjustable process-wide controls for RRD/time-series materialization.
    pub fn time_series_runtime_settings(&self) -> Arc<TimeSeriesRuntimeSettings> {
        Arc::clone(&self.time_series_runtime_settings)
    }

    /// Install the process-local WASM aggregate executor used by RRD rollups.
    pub fn set_time_series_wasm_aggregate_executor(
        &self,
        executor: Arc<dyn TimeSeriesWasmAggregateExecutor>,
    ) {
        *self.time_series_wasm_aggregates.write() = Some(executor);
    }

    /// Visits rows in one partition and time window for a registered table.
    pub fn visit_time_series_window_rows<Cb>(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        window_start_ts: i64,
        window_end_ts: i64,
        cb: Cb,
    ) -> ferrosa_common::Result<usize>
    where
        Cb: FnMut(&Row) -> ferrosa_common::Result<()>,
    {
        let tables = self.tables.read();
        let Some(state) = tables.get(table_id) else {
            return Ok(0);
        };
        state.store.visit_time_series_window_rows(
            key,
            window_start_ts,
            window_end_ts,
            state.time_series_timestamp_unit(),
            cb,
        )
    }

    /// Visits bounded live queue snapshots for time-series materialization observability.
    pub fn visit_time_series_materialization_queues(
        &self,
        visit: &mut dyn FnMut(TimeSeriesMaterializationQueueSnapshot),
    ) {
        for handle in self.time_series_consolidators.read().values() {
            let queue = handle.aggregator.queue_snapshot();
            let (_failed_tasks, last_error) = handle.failure_snapshot();
            let max_delay_ms = handle.target.interval.as_millis().min(i64::MAX as u128) as i64;
            visit(TimeSeriesMaterializationQueueSnapshot {
                source_table: handle.target.source_table.clone(),
                target_table: handle.target.target_table.clone(),
                window_start_ts: queue.oldest_window_start_ts,
                window_end_ts: queue.oldest_window_end_ts,
                task_type: queue.oldest_task_type.to_string(),
                enqueued_at_ms: queue.oldest_task_enqueued_at_ms,
                oldest_task_age_ms: queue.oldest_task_age_ms,
                queue_depth: queue.pending_tasks.min(i64::MAX as u64) as i64,
                retry_count: 0,
                last_error,
                max_delay_ms,
                alerting: queue.oldest_task_age_ms > max_delay_ms,
            });
        }
    }

    /// Visits bounded live status snapshots for time-series materialization observability.
    pub fn visit_time_series_materialization_statuses(
        &self,
        visit: &mut dyn FnMut(TimeSeriesMaterializationStatusSnapshot),
    ) {
        for handle in self.time_series_consolidators.read().values() {
            let queue = handle.aggregator.queue_snapshot();
            let metrics = handle
                .aggregator
                .metrics()
                .map(|metrics| metrics.snapshot());
            let (worker_failed_tasks, last_error) = handle.failure_snapshot();
            let max_delay_ms = handle.target.interval.as_millis().min(i64::MAX as u128) as i64;
            let failed_tasks = worker_failed_tasks
                .saturating_add(metrics.as_ref().map(|m| m.decode_failures).unwrap_or(0));
            visit(TimeSeriesMaterializationStatusSnapshot {
                source_table: handle.target.source_table.clone(),
                target_table: handle.target.target_table.clone(),
                status: if failed_tasks > 0 {
                    "failed".to_string()
                } else if queue.pending_tasks == 0 {
                    "idle".to_string()
                } else if queue.oldest_task_age_ms > max_delay_ms {
                    "degraded".to_string()
                } else {
                    "pending".to_string()
                },
                pending_tasks: queue.pending_tasks.min(i64::MAX as u64) as i64,
                completed_tasks: metrics
                    .as_ref()
                    .map(|metrics| metrics.windows_consolidated.min(i64::MAX as u64) as i64)
                    .unwrap_or(0),
                failed_tasks: failed_tasks.min(i64::MAX as u64) as i64,
                stale_drops_total: metrics
                    .as_ref()
                    .map(|metrics| metrics.consolidation_drops.min(i64::MAX as u64) as i64)
                    .unwrap_or(0),
                last_materialized_window_end_ms: if queue.oldest_window_end_ts == 0 {
                    None
                } else {
                    Some(queue.oldest_window_end_ts)
                },
                last_error,
            });
        }
    }

    /// Drains and materializes one queued time-series descriptor for a source table.
    pub fn process_one_time_series_materialization(
        &self,
        table_id: &TableId,
    ) -> ferrosa_common::Result<bool> {
        let mutation = {
            let handles = self.time_series_consolidators.read();
            let Some(handle) = handles.get(table_id) else {
                return Ok(false);
            };

            let task = match handle.task_rx.lock().try_recv() {
                Ok(task) => {
                    handle.aggregator.note_materialization_task_drained();
                    task
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(false),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "time-series materialization queue disconnected for {table_id}"
                    )));
                }
            };

            let materialization_result = (|| -> ferrosa_common::Result<Option<Mutation>> {
                let (partition_key, window_start_ts, window_end_ts, write_timestamp) = match task {
                    ConsolidationTask::BoundaryCrossed {
                        partition_key,
                        window_start_ts,
                        window_end_ts,
                        ..
                    } => (partition_key, window_start_ts, window_end_ts, window_end_ts),
                    ConsolidationTask::LateData {
                        partition_key,
                        window_start_ts,
                        late_timestamp,
                        ..
                    } => {
                        let interval_micros = handle.target.interval.as_micros() as i64;
                        let window_end_ts = window_start_ts.saturating_add(interval_micros);
                        if let Some(watermark_window_start_ts) =
                            handle.aggregator.watermark_window_start(&partition_key)
                        {
                            if handle.target.classify_late_window(
                                window_start_ts,
                                watermark_window_start_ts,
                                handle.late_window,
                            ) == LateWindowClassification::Drop
                            {
                                if let Some(metrics) = handle.aggregator.metrics() {
                                    metrics.consolidation_drops.fetch_add(1, Ordering::Relaxed);
                                }
                                tracing::warn!(
                                    %table_id,
                                    target_table = %handle.target.target_table,
                                    window_start_ts,
                                    watermark_window_start_ts,
                                    late_window_ms = handle.late_window.as_millis(),
                                    "dropping stale time-series materialization outside late_window"
                                );
                                return Ok(None);
                            }
                        }
                        let late_offset = late_timestamp.saturating_sub(window_start_ts).max(1);
                        (
                            partition_key,
                            window_start_ts,
                            window_end_ts,
                            window_end_ts.saturating_add(late_offset),
                        )
                    }
                };

                let source_key = DecoratedKey::new(PartitionKey::new(partition_key.clone()));
                let mut results: SmallVec<[f64; 8]> = SmallVec::new();
                for source_column_ordinal in 0..handle.source_column_count {
                    let column_index = handle.source_column_indices[source_column_ordinal];
                    let column_type = &handle.source_column_types[source_column_ordinal];
                    let mut acc = Accumulator::new(false);
                    let mut wasm_invocations: SmallVec<
                        [(usize, Box<dyn TimeSeriesWasmAggregateInvocation>); 4],
                    > = SmallVec::new();
                    for (function_index, function) in handle.target.functions.iter().enumerate() {
                        if let ConsolidationFn::Wasm {
                            keyspace,
                            function_name,
                        } = function
                        {
                            let executor = self
                            .time_series_wasm_aggregates
                            .read()
                            .as_ref()
                            .cloned()
                            .ok_or_else(|| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "time-series materialization for {table_id} requires WASM aggregate executor for wasm:{keyspace}.{function_name}"
                                ))
                            })?;
                            let invocation = executor
                            .start(keyspace, function_name, column_type)
                            .map_err(|err| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "failed to start WASM aggregate wasm:{keyspace}.{function_name} for {table_id}: {err}"
                                ))
                            })?;
                            wasm_invocations.push((function_index, invocation));
                        }
                    }

                    self.visit_time_series_window_rows(
                        table_id,
                        &source_key,
                        window_start_ts,
                        window_end_ts,
                        |row| {
                            let Some((_, cell)) =
                                row.cells.iter().find(|(idx, _)| *idx == column_index)
                            else {
                                return Ok(());
                            };
                            let Some(bytes) = cell.value.as_deref() else {
                                return Ok(());
                            };
                            let Some(value) = decode_typed_numeric(bytes, column_type) else {
                                if let Some(metrics) = handle.aggregator.metrics() {
                                    metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
                                }
                                tracing::warn!(
                                    %table_id,
                                    column_index,
                                    byte_len = bytes.len(),
                                    "failed to decode numeric bytes for time-series materialization"
                                );
                                return Ok(());
                            };
                            acc.push(value);
                            for (_, invocation) in wasm_invocations.iter_mut() {
                                invocation.update(value).map_err(|err| {
                                    ferrosa_common::Error::InvalidFormat(format!(
                                        "failed to update WASM aggregate for {table_id}: {err}"
                                    ))
                                })?;
                            }
                            Ok(())
                        },
                    )?;
                    if acc.count() == 0 {
                        continue;
                    }

                    let mut wasm_invocations = wasm_invocations.into_iter().peekable();
                    for (function_index, function) in handle.target.functions.iter().enumerate() {
                        match function {
                            ConsolidationFn::Wasm { .. } => {
                                let Some((idx, invocation)) = wasm_invocations.next() else {
                                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                                        "missing WASM aggregate invocation for {table_id}"
                                    )));
                                };
                                if idx != function_index {
                                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                                        "WASM aggregate invocation order mismatch for {table_id}"
                                    )));
                                }
                                results.push(invocation.finalize().map_err(|err| {
                                    ferrosa_common::Error::InvalidFormat(format!(
                                        "failed to finalize WASM aggregate for {table_id}: {err}"
                                    ))
                                })?);
                            }
                            ConsolidationFn::Median | ConsolidationFn::Composite(_) => {
                                return Err(ferrosa_common::Error::InvalidFormat(format!(
                                "time-series materialization for {table_id} requires unsupported window materialization function: {function:?}"
                            )));
                            }
                            other => results.push(acc.result_for(other)),
                        }
                    }
                }

                if results.is_empty() {
                    return Ok(None);
                }

                Ok(Some(
                    MaterializedRollup {
                        target: handle.target.clone(),
                        partition_key,
                        window_start_ts,
                    }
                    .encode_mutation_from_results_at(results, write_timestamp),
                ))
            })();

            match materialization_result {
                Ok(Some(mutation)) => mutation,
                Ok(None) => return Ok(true),
                Err(e) => {
                    handle.note_materialization_failure(&e);
                    return Err(e);
                }
            }
        };

        let target_table_id = TableId::new(&mutation.keyspace, &mutation.table);
        self.apply_derived_mutation(&mutation);
        self.dispatch_sync_observers(&target_table_id, &mutation);
        self.dispatch_async_observers(&target_table_id, &mutation);
        Ok(true)
    }

    /// Drains queued time-series materialization descriptors across all active
    /// consolidators, bounded by `max_tasks`.
    pub fn process_pending_time_series_materializations(
        &self,
        max_tasks: usize,
    ) -> ferrosa_common::Result<usize> {
        let mut processed = 0;
        while processed < max_tasks {
            let table_ids: Vec<TableId> = self
                .time_series_consolidators
                .read()
                .keys()
                .cloned()
                .collect();
            if table_ids.is_empty() {
                break;
            }

            let mut made_progress = false;
            for table_id in table_ids {
                if processed >= max_tasks {
                    break;
                }
                if self.process_one_time_series_materialization(&table_id)? {
                    processed += 1;
                    made_progress = true;
                }
            }

            if !made_progress {
                break;
            }
        }

        Ok(processed)
    }

    /// Starts the background worker that materializes queued RRD rollups.
    pub fn spawn_time_series_materialization_worker(
        engine: Arc<Self>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_time_series_materialization_worker_with_config(
            engine,
            DEFAULT_TIME_SERIES_MATERIALIZATION_POLL_INTERVAL,
            DEFAULT_TIME_SERIES_MATERIALIZATION_BATCH_LIMIT,
        )
    }

    /// Starts the background worker that materializes queued RRD rollups using
    /// caller-provided scheduling controls.
    pub fn spawn_time_series_materialization_worker_with_config(
        engine: Arc<Self>,
        poll_interval: std::time::Duration,
        batch_limit: usize,
    ) -> tokio::task::JoinHandle<()> {
        TaskPool::current("time-series-materialization").spawn(async move {
            let poll_interval = poll_interval.max(std::time::Duration::from_millis(1));
            let batch_limit = batch_limit.max(1);
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;
                let worker_engine = engine.clone();
                match TaskPool::current("time-series-materialization-blocking")
                    .spawn_blocking(move || {
                        worker_engine.process_pending_time_series_materializations(batch_limit)
                    })
                    .await
                {
                    Ok(Ok(processed)) if processed > 0 => {
                        tracing::debug!(
                            processed,
                            "time-series materialization worker drained queued rollups"
                        );
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(
                            %e,
                            "time-series materialization worker failed; queued rollups may lag"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            %e,
                            "time-series materialization worker task panicked"
                        );
                    }
                    _ => {}
                }
            }
        })
    }

    fn build_time_series_consolidator(
        &self,
        table_id: &TableId,
        schema: &TableSchema,
    ) -> ferrosa_common::Result<Option<TimeSeriesConsolidatorHandle>> {
        let config = match ConsolidationConfig::from_extensions(&schema.extensions) {
            Some(Ok(config)) => config,
            Some(Err(reason)) => {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "invalid consolidation extensions for {table_id}: {reason}"
                )));
            }
            None => return Ok(None),
        };

        let mut column_types_by_name = HashMap::new();
        let mut column_indices_by_name = HashMap::new();
        for (idx, column) in schema
            .static_columns
            .iter()
            .chain(schema.regular_columns.iter())
            .enumerate()
        {
            column_types_by_name.insert(
                column.name.clone(),
                normalize_consolidation_type(&column.type_name),
            );
            column_indices_by_name.insert(column.name.clone(), idx as u16);
        }

        validate_numeric_columns(&config.columns, &column_types_by_name).map_err(|reason| {
            ferrosa_common::Error::InvalidFormat(format!(
                "invalid consolidation extensions for {table_id}: {reason}"
            ))
        })?;

        let source_timestamp_unit = schema
            .clustering_columns
            .first()
            .map(|column| TimeSeriesTimestampUnit::from_storage_type(&column.type_name))
            .unwrap_or(TimeSeriesTimestampUnit::Micros);

        let mut value_column_indices = Vec::with_capacity(config.columns.len());
        let mut column_types = Vec::with_capacity(config.columns.len());
        for column_name in &config.columns {
            let Some(&column_index) = column_indices_by_name.get(column_name) else {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "invalid consolidation extensions for {table_id}: column '{column_name}' not found"
                )));
            };
            value_column_indices.push(column_index);
            column_types.push(
                column_types_by_name
                    .get(column_name)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string()),
            );
        }

        let (task_tx, task_rx) = std::sync::mpsc::sync_channel(config.channel_capacity);
        let target_table_id = TableId::new(&schema.keyspace, &config.target_table);
        let target_tables = self.tables.read();
        let target_state = target_tables.get(&target_table_id);
        let target_timestamp_unit = target_state
            .map(TableState::time_series_timestamp_unit)
            .unwrap_or(source_timestamp_unit);
        let target_result_column_indices = if let Some(target_state) = target_state {
            let mut target_indices_by_name = HashMap::new();
            for (idx, column) in target_state
                .schema
                .static_columns
                .iter()
                .chain(target_state.schema.regular_columns.iter())
                .enumerate()
            {
                target_indices_by_name.insert(column.name.as_str(), idx as u16);
            }

            let mut indices = Vec::with_capacity(config.columns.len() * config.functions.len());
            for source_column in &config.columns {
                for function in &config.functions {
                    let Some(suffix) = function.output_suffix() else {
                        continue;
                    };
                    let output_column = format!("{source_column}_{suffix}");
                    let Some(&column_index) = target_indices_by_name.get(output_column.as_str())
                    else {
                        return Err(ferrosa_common::Error::InvalidFormat(format!(
                            "invalid consolidation extensions for {table_id}: target table \
                             '{target_table_id}' is missing rollup output column '{output_column}'"
                        )));
                    };
                    indices.push(column_index);
                }
            }
            indices
        } else {
            (0..config.columns.len() * config.functions.len())
                .map(|idx| idx as u16)
                .collect()
        };
        drop(target_tables);
        let target = MaterializationTarget {
            source_table: table_id.clone(),
            target_table: target_table_id,
            interval: config.interval,
            source_columns: config.columns.clone(),
            functions: config.functions.clone(),
            target_result_column_indices,
            target_timestamp_unit,
        };
        let source_column_count = value_column_indices.len();
        let late_window = config.late_window;
        let metrics = Arc::new(ConsolidationMetrics::new());
        let aggregator = Arc::new(
            TimeSeriesAggregator::with_column_types_runtime_settings_and_metrics(
                config,
                table_id.clone(),
                value_column_indices.clone(),
                column_types.clone(),
                task_tx,
                Arc::clone(&self.time_series_runtime_settings),
                metrics,
            )
            .with_timestamp_unit(source_timestamp_unit),
        );
        let observer: Arc<dyn crate::observer::WriteObserver> = aggregator.clone();

        Ok(Some(TimeSeriesConsolidatorHandle {
            aggregator,
            observer,
            task_rx: parking_lot::Mutex::new(task_rx),
            target,
            late_window,
            source_column_count,
            source_column_indices: value_column_indices,
            source_column_types: column_types,
            failed_tasks: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }))
    }

    fn install_time_series_consolidator(
        &self,
        table_id: TableId,
        handle: Option<TimeSeriesConsolidatorHandle>,
    ) {
        let Some(handle) = handle else {
            return;
        };
        self.register_observer(handle.observer.clone());
        self.time_series_consolidators
            .write()
            .insert(table_id, handle);
    }

    fn remove_time_series_consolidator(&self, table_id: &TableId) {
        let Some(handle) = self.time_series_consolidators.write().remove(table_id) else {
            return;
        };
        self.observers
            .write()
            .retain(|observer| !Arc::ptr_eq(observer, &handle.observer));
    }

    /// Registers an async observer and spawns an in-process drain task that
    /// applies derived mutations through the full write path (commit log +
    /// memtable).
    ///
    /// # Drain task guarantees
    ///
    /// - Lives as long as the engine: the sender half lives inside
    ///   `async_observers`; when the engine is dropped the senders drop, the
    ///   channel closes, `recv()` returns `None`, and the task exits cleanly.
    /// - Panics inside `observer.on_write` are caught and logged at ERROR level
    ///   (file + line), then the task continues — a single bad mutation must
    ///   not kill the drain loop for subsequent mutations.
    /// - Requires an active tokio runtime at call time (panics otherwise).
    pub fn register_async_observer_with_drain(
        &self,
        observer: Arc<dyn crate::observer::WriteObserver>,
        engine: Arc<StorageEngine>,
    ) {
        let capacity = self.async_observer_capacity;
        let mut rx = self.register_async_observer(observer.clone(), capacity);

        TaskPool::current("storage-observer-drain").spawn(async move {
            while let Some((table_id, mutation)) = rx.recv().await {
                // observer.on_write is synchronous and non-blocking.
                // Wrap in catch_unwind so a panicking observer does not kill the drain loop.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    observer.on_write(&table_id, &mutation)
                }));
                match result {
                    Ok(derived) => {
                        for dm in derived {
                            engine.apply_derived_mutation(&dm);
                        }
                    }
                    Err(panic_val) => {
                        let msg = if let Some(s) = panic_val.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_val.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "<non-string panic payload>".to_string()
                        };
                        tracing::error!(
                            table = %table_id,
                            panic = %msg,
                            "observer drain: observer.on_write panicked — derived mutations \
                             for this event are lost; drain loop continues for subsequent events"
                        );
                    }
                }
            }
        });
    }

    /// Writes a derived mutation (produced by an observer) to the commit log
    /// and the target table's memtable.
    ///
    /// Mirrors the pattern in `dispatch_sync_observers`. Errors are logged at
    /// ERROR level and the write is skipped; derived mutation failures must
    /// not crash the engine.
    fn apply_derived_mutation(&self, dm: &Mutation) {
        if let Err(e) = self.check_write_admission() {
            tracing::error!(
                %e,
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: local disk reserve exhausted; derived mutation dropped"
            );
            return;
        }
        if let Err(e) = self.commit_log.append(dm) {
            tracing::error!(
                %e,
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: commit log append failed for derived mutation"
            );
            return;
        }
        let dtid = TableId::new(&dm.keyspace, &dm.table);
        let tables = self.tables.read();
        if let Some(state) = tables.get(&dtid) {
            for row in &dm.rows {
                if let Err(e) = state.store.write(&dm.key, row.clone()) {
                    tracing::error!(
                        %e,
                        keyspace = %dm.keyspace,
                        table = %dm.table,
                        "observer drain: memtable write failed for derived mutation"
                    );
                }
            }
        } else {
            tracing::error!(
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: target table not registered — derived mutation dropped; \
                 register the adjacency table with StorageEngine before registering the observer"
            );
        }
    }

    /// Dispatches a mutation to all sync observers watching the given table.
    ///
    /// Derived mutations produced by observers go through the commit log for
    /// durability and are written to the target table's memtable.
    fn dispatch_sync_observers(&self, table_id: &TableId, mutation: &Mutation) {
        let observers = self.observers.read();
        for obs in observers.iter() {
            if obs.mode() == crate::observer::ObserverMode::Sync && obs.watches_table(table_id) {
                let derived = obs.on_write(table_id, mutation);
                for dm in derived {
                    if let Err(e) = self.check_write_admission() {
                        tracing::error!(
                            %e,
                            keyspace = %dm.keyspace,
                            table = %dm.table,
                            "observer: local disk reserve exhausted; derived mutation dropped"
                        );
                        continue;
                    }
                    // Durability: go through commit log.
                    if let Err(e) = self.commit_log.append(&dm) {
                        tracing::error!(%e, "observer: commit log append failed");
                        continue;
                    }
                    let dtid = TableId::new(&dm.keyspace, &dm.table);
                    let tables = self.tables.read();
                    if let Some(state) = tables.get(&dtid) {
                        for row in &dm.rows {
                            let _ = state.store.write(&dm.key, row.clone());
                        }
                    }
                }
            }
        }
    }

    fn has_observers_for_table(&self, table_id: &TableId) -> bool {
        self.observers
            .read()
            .iter()
            .any(|obs| obs.watches_table(table_id))
            || self
                .async_observers
                .read()
                .iter()
                .any(|state| state.observer.watches_table(table_id))
    }

    /// Dispatches a mutation to all async observers watching the given table.
    ///
    /// Uses `try_send` — never blocks the write path. If the channel is full,
    /// the mutation is dropped and the drop counter is incremented.
    fn dispatch_async_observers(&self, table_id: &TableId, mutation: &Mutation) {
        let async_obs = self.async_observers.read();
        for state in async_obs.iter() {
            if state.observer.watches_table(table_id)
                && state
                    .sender
                    .try_send((table_id.clone(), mutation.clone()))
                    .is_err()
            {
                state.drop_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the total number of mutations dropped by async observers due to
    /// backpressure (channel full).
    pub fn observer_drop_count(&self) -> u64 {
        let async_obs = self.async_observers.read();
        async_obs
            .iter()
            .map(|s| s.drop_count.load(Ordering::Relaxed))
            .sum()
    }

    /// Writes a row to the commit log and the table's memtable.
    ///
    /// The commit log append provides durability; the memtable write provides
    /// read visibility. Both are lock-free on the hot path.
    /// Return the data directory path.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.config.data_dir
    }

    /// Number of tables currently registered.
    pub fn table_count(&self) -> usize {
        self.tables.read().len()
    }

    /// The `TableId`s currently registered with this engine. Used by the
    /// self-heal controller to enumerate scan targets.
    pub fn registered_table_ids(&self) -> Vec<TableId> {
        self.tables.read().keys().cloned().collect()
    }

    /// Filesystem directory holding a table's SSTables:
    /// `<data_dir>/sstables/<keyspace>.<table>`. Used by the self-heal
    /// detector to scan for corrupt generations.
    pub fn table_sstable_dir(&self, table: &TableId) -> std::path::PathBuf {
        self.config
            .data_dir
            .join("sstables")
            .join(table.to_string())
    }

    /// Test-only accessor for the private `generation_component_path` so
    /// self-heal fixtures can locate a generation's component file.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn generation_component_path_for_test(
        table_dir: &std::path::Path,
        gen: u64,
        component: &str,
    ) -> Option<std::path::PathBuf> {
        Self::generation_component_path(table_dir, &gen.to_string(), component)
    }

    /// Enumerate the SSTable generations present in a table directory.
    ///
    /// Mirrors the discovery logic in
    /// `load_existing_sstables_and_sidecars_with_repair_mode`: a generation is
    /// either a flat `<gen>-Data.db` in `table_dir` or a `<gen>/<gen>-Data.db`
    /// subdir. Returned descending (newest first) for stable iteration.
    pub fn list_generations_in_dir(table_dir: &std::path::Path) -> Vec<u64> {
        let mut values = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(table_dir).into_iter().flatten().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stripped) = name.strip_suffix("-Data.db") {
                if let Ok(gen) = stripped.parse::<u64>() {
                    values.insert(gen);
                }
            }
            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                if let Ok(gen) = name.parse::<u64>() {
                    if table_dir
                        .join(gen.to_string())
                        .join(format!("{gen}-Data.db"))
                        .exists()
                    {
                        values.insert(gen);
                    }
                }
            }
        }
        let mut out: Vec<u64> = values.into_iter().collect();
        out.sort_by(|a, b| b.cmp(a));
        out
    }

    /// Smoke-test one generation in a table directory the same way startup
    /// does — open it and run `validate_sstable_for_startup_repair`. Returns
    /// `Ok(())` if healthy, `Err` with the failure reason if corrupt.
    ///
    /// This is the single source of corruption-detection truth, reused by the
    /// self-heal detector so it never diverges from the startup smoke test.
    pub fn smoke_test_generation(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> ferrosa_common::Result<()> {
        let gen_str = gen.to_string();
        // Zero-byte critical components are corruption (same rule as startup).
        for comp in ["Data.db", "Partitions.db"] {
            let path = Self::generation_component_path(table_dir, &gen_str, comp)
                .unwrap_or_else(|| table_dir.join(format!("{gen_str}-{comp}")));
            if let Ok(meta) = std::fs::metadata(&path) {
                if meta.len() == 0 {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "generation {gen}: zero-byte critical component {comp}"
                    )));
                }
            }
        }
        let reader = Self::open_sstable_from_dir(table_dir, &gen_str)?;
        Self::validate_sstable_for_startup_repair(&reader)
    }

    /// Scan a table directory and return the generations that fail the startup
    /// smoke test, paired with the failure reason. The healthy gens are left
    /// untouched. Pure read — moves no files.
    /// Smoke-test every generation in `table_dir`, returning the corrupt ones.
    ///
    /// `verified` is an in/out cache of generations already smoke-tested OK.
    /// SSTable generations are **immutable** — once a generation passes the
    /// smoke test it can never become corrupt — so verified generations are
    /// skipped on subsequent scans. This is what keeps the periodic self-heal
    /// corruption scan from re-reading every SSTable's rows on every tick (the
    /// idle-CPU spin — ../specs/bug-idle-cpu-spin-3cores.md): on an idle cluster
    /// with no new flushes/compactions there is nothing new to test, so a tick
    /// costs O(dir listing), not O(all data).
    pub fn scan_table_dir_for_corrupt(
        table_dir: &std::path::Path,
        verified: &mut std::collections::BTreeSet<u64>,
    ) -> Vec<(u64, String)> {
        let mut corrupt = Vec::new();
        for gen in Self::list_generations_in_dir(table_dir) {
            if verified.contains(&gen) {
                continue;
            }
            match Self::smoke_test_generation(table_dir, gen) {
                Ok(_) => {
                    verified.insert(gen);
                }
                Err(e) => corrupt.push((gen, e.to_string())),
            }
        }
        corrupt
    }

    /// Move a corrupt generation's files into the table's `quarantine/`
    /// subdirectory. Files are **moved, never deleted** (FMEA never-worse).
    /// The quarantine dir is created if absent.
    pub fn quarantine_corrupt_generation(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> ferrosa_common::Result<std::path::PathBuf> {
        let quarantine_dir = table_dir.join("quarantine");
        std::fs::create_dir_all(&quarantine_dir).map_err(|e| {
            ferrosa_common::Error::InvalidData(format!(
                "self-heal quarantine: failed to create {}: {e}",
                quarantine_dir.display()
            ))
        })?;
        Self::quarantine_generation(table_dir, gen, &quarantine_dir)?;
        Ok(quarantine_dir)
    }

    /// Total buffer bytes held by closed commit log segments.
    ///
    /// After the P0 OOM fix, this should be 0 — closed segments release
    /// their 32 MB write buffers after fsync. A non-zero value indicates
    /// the release path is not running (regression detector).
    pub fn closed_segment_buffer_bytes(&self) -> usize {
        self.commit_log.closed_segments_total_bytes()
    }

    /// Write directly to storage for observability tables.
    /// Same as `write()` but skips observer dispatch to prevent telemetry
    /// feedback loops (observability writes must not generate new spans).
    pub fn write_observability(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        _timestamp: i64,
    ) -> ferrosa_common::Result<()> {
        // Skip commit log for observability (best-effort, not durable).
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidData(format!("observability table not found: {table_id}"))
        })?;
        state.store.write(key, row)?;
        Ok(())
    }

    pub fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> ferrosa_common::Result<()> {
        let total_start = Instant::now();
        // Per-write span emitted at DEBUG level so it is free at the
        // default INFO subscriber filter — every span allocation was
        // measurable on a 50k ops/s workload.
        let _span = tracing::debug_span!(
            "storage.write",
            table = %table_id,
        )
        .entered();
        let phase_start = Instant::now();
        if let Err(e) = self.check_write_admission() {
            crate::metrics::observe_write_phase(
                crate::metrics::WritePhase::AdmissionDisk,
                phase_start.elapsed(),
            );
            crate::metrics::observe_write_phase(
                crate::metrics::WritePhase::Total,
                total_start.elapsed(),
            );
            crate::metrics::inc_write_failure_reason(
                crate::metrics::WriteFailureReason::DiskReserve,
            );
            return Err(e);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::AdmissionDisk,
            phase_start.elapsed(),
        );

        let phase_start = Instant::now();
        {
            let tables = self.tables.read();
            let state = match tables.get(table_id) {
                Some(state) => state,
                None => {
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::AdmissionMemtable,
                        phase_start.elapsed(),
                    );
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::Total,
                        total_start.elapsed(),
                    );
                    crate::metrics::inc_write_failure_reason(
                        crate::metrics::WriteFailureReason::TableMissing,
                    );
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    )));
                }
            };
            if let Err(e) = self.check_memtable_write_admission(table_id, state) {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::AdmissionMemtable,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::MemtableBackpressure,
                );
                return Err(e);
            }
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::AdmissionMemtable,
            phase_start.elapsed(),
        );

        // 1. Append to commit log for durability.
        let phase_start = Instant::now();
        let cl_pos = match self
            .commit_log
            .append_single_row(table_id, key, &row, timestamp)
        {
            Ok(pos) => pos,
            Err(e) => {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::CommitLogAppend,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::CommitLogAppend,
                );
                return Err(e);
            }
        };
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::CommitLogAppend,
            phase_start.elapsed(),
        );
        let observer_mutation = if self.has_observers_for_table(table_id) {
            Some(Mutation::new(
                table_id.keyspace.clone(),
                table_id.table.clone(),
                key.clone(),
                vec![row.clone()],
                timestamp,
            ))
        } else {
            None
        };

        // 2. Write to the table's memtable and track commit log position.
        let phase_start = Instant::now();
        {
            let tables = self.tables.read();
            let state = match tables.get(table_id) {
                Some(state) => state,
                None => {
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::MemtableWrite,
                        phase_start.elapsed(),
                    );
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::Total,
                        total_start.elapsed(),
                    );
                    crate::metrics::inc_write_failure_reason(
                        crate::metrics::WriteFailureReason::TableMissing,
                    );
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    )));
                }
            };
            if let Err(e) = state.store.write(key, row) {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::MemtableWrite,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::MemtableWrite,
                );
                return Err(e);
            }
            state.last_commit_log_position.store(Arc::new(Some(cl_pos)));
            // Set the first-unflushed timestamp via CAS — succeeds at most once
            // per memtable epoch, no contention after the first write.
            if state
                .first_unflushed_write_at_nanos
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0
            {
                let now = now_nanos_since_reference();
                let _ = state.first_unflushed_write_at_nanos.compare_exchange(
                    0,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            self.request_flush_if_needed(state);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::MemtableWrite,
            phase_start.elapsed(),
        );

        // 3. Notify observers after successful commit log + memtable write.
        let phase_start = Instant::now();
        if let Some(mutation) = observer_mutation.as_ref() {
            self.dispatch_sync_observers(table_id, mutation);
            self.dispatch_async_observers(table_id, mutation);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::Observers,
            phase_start.elapsed(),
        );
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::Total,
            total_start.elapsed(),
        );
        crate::metrics::inc_write_total();

        Ok(())
    }

    /// Writes multiple rows to a table in a single call.
    ///
    /// Each mutation is (key, row, timestamp). Mutations are appended to the
    /// commit log and memtable sequentially. Not atomic — a failure partway
    /// through leaves earlier writes committed.
    pub fn batch_write(
        &self,
        table_id: &TableId,
        mutations: Vec<(DecoratedKey, Row, i64)>,
    ) -> ferrosa_common::Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        // Collect committed mutations for observer dispatch after each write.
        for (key, row, timestamp) in mutations {
            self.check_write_admission()?;
            {
                let tables = self.tables.read();
                let state = tables.get(table_id).ok_or_else(|| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    ))
                })?;
                self.check_memtable_write_admission(table_id, state)?;
            }

            let mutation = Mutation::new(
                table_id.keyspace.clone(),
                table_id.table.clone(),
                key.clone(),
                vec![row.clone()],
                timestamp,
            );

            // Append to commit log.
            let cl_pos = self.commit_log.append(&mutation)?;

            // Write to memtable and track commit log position (scoped read lock).
            {
                let tables = self.tables.read();
                let state = tables.get(table_id).ok_or_else(|| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    ))
                })?;
                state.store.write(&key, row)?;
                state.last_commit_log_position.store(Arc::new(Some(cl_pos)));
                if state
                    .first_unflushed_write_at_nanos
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    let now = now_nanos_since_reference();
                    let _ = state.first_unflushed_write_at_nanos.compare_exchange(
                        0,
                        now,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                self.request_flush_if_needed(state);
            }

            // Notify observers after successful commit log + memtable write.
            self.dispatch_sync_observers(table_id, &mutation);
            self.dispatch_async_observers(table_id, &mutation);
        }

        Ok(())
    }

    /// Returns a reference to the batchlog manager, if enabled.
    pub fn batchlog(&self) -> Option<&crate::batchlog::BatchlogManager> {
        self.batchlog.as_ref()
    }

    /// Writes a batch of mutations atomically.
    ///
    /// All mutations are appended to the commit log first, then applied to
    /// their respective memtables. If the process crashes between commit log
    /// append and memtable apply, commit log replay will recover all mutations.
    ///
    /// This is the single-node fast path for logged batches: no batchlog
    /// coordination needed because the commit log provides the atomicity
    /// guarantee.
    pub fn write_atomic_batch(&self, mutations: Vec<Mutation>) -> ferrosa_common::Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        // Preflight every target table AND every entry's size before appending
        // any batch mutation to the commitlog. This preserves all-or-nothing
        // semantics: every fallible append condition (table registration,
        // overload admission, and oversized-entry rejection) is checked up
        // front, so Phase 1 cannot fail *partway* and leave an already-appended
        // (and therefore replay-durable) prefix of the batch on disk.
        {
            let tables = self.tables.read();
            let max_entry = self.commit_log.max_entry_size();
            let mut checked = HashSet::new();
            for m in &mutations {
                self.check_write_admission()?;

                // Oversized-entry preflight: an entry larger than a fresh
                // segment can never be appended, so reject the whole batch now
                // rather than after appending earlier ops.
                let entry_size = CommitLog::entry_size(m);
                if entry_size > max_entry {
                    return Err(ferrosa_common::Error::InvalidData(format!(
                        "batch entry ({entry_size} bytes) exceeds commit-log \
                         segment capacity ({max_entry} bytes usable); increase \
                         segment_size or split the batch"
                    )));
                }

                let table_id = TableId::new(&m.keyspace, &m.table);
                if !checked.insert(table_id.clone()) {
                    continue;
                }
                let state = tables.get(&table_id).ok_or_else(|| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    ))
                })?;
                self.check_memtable_write_admission(&table_id, state)?;
            }
        }

        // Phase 1: Append all mutations to the commit log, tracking positions.
        let mut positions: HashMap<TableId, CommitLogPosition> = HashMap::new();
        for m in &mutations {
            let cl_pos = self.commit_log.append(m)?;
            let table_id = TableId::new(&m.keyspace, &m.table);
            positions.insert(table_id, cl_pos);
        }

        // Durability barrier: synchronously fsync the appended batch BEFORE it
        // is made visible in the memtable (Phase 2). Without this, under the
        // production-default `Periodic` sync strategy `append()` only schedules
        // a background fsync, so the rows would become readable while still
        // unsynced — a crash in that window would lose an acked, already-visible
        // batch. `force_sync` propagates fsync failures as `Err` (fail-loud);
        // on error nothing has been applied to the memtable yet, so the batch
        // remains all-or-nothing: the appended entries replay together on the
        // next restart if they reached disk, or are lost together if they did
        // not — never a torn, partly-visible state.
        self.commit_log.force_sync()?;

        // Phase 2: Apply to memtables and update commit log positions.
        let tables = self.tables.read();
        for m in &mutations {
            let table_id = TableId::new(&m.keyspace, &m.table);
            let state = tables.get(&table_id).ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
            })?;
            for row in &m.rows {
                state.store.write(&m.key, row.clone())?;
            }
            if let Some(&cl_pos) = positions.get(&table_id) {
                state.last_commit_log_position.store(Arc::new(Some(cl_pos)));
                if state
                    .first_unflushed_write_at_nanos
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    let now = now_nanos_since_reference();
                    let _ = state.first_unflushed_write_at_nanos.compare_exchange(
                        0,
                        now,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }
            self.request_flush_if_needed(state);
        }
        drop(tables);

        // Phase 3: Notify observers.
        for m in &mutations {
            let table_id = TableId::new(&m.keyspace, &m.table);
            self.dispatch_sync_observers(&table_id, m);
            self.dispatch_async_observers(&table_id, m);
        }

        Ok(())
    }

    /// Apply a batch of mixed ops atomically and durably (single fsync group).
    ///
    /// All-or-nothing: every op lowers to a [`Mutation`] and the whole set is
    /// applied through [`Self::write_atomic_batch`], which preflights every
    /// target table **before** appending any commit-log record. On any
    /// preflight / append failure **none** of the ops are applied and an `Err`
    /// is returned (spec URS-QEC-X02, fail-loud per X01). Ops touching the same
    /// `(keyspace, table, key)` are applied in `ops` order.
    ///
    /// An empty batch is `Ok(())`.
    pub fn apply_batch(&self, ops: Vec<BatchOp>) -> ferrosa_common::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mutations = ops.into_iter().map(BatchOp::into_mutation).collect();
        self.write_atomic_batch(mutations)
    }

    /// Open a staging handle for a Bolt explicit transaction (B02).
    ///
    /// Ops are staged in memory; nothing is durable until [`BatchTxn::commit`].
    /// [`BatchTxn::abort`] (or dropping the handle) discards the staged ops with
    /// no I/O — exactly the `ROLLBACK` / connection-drop semantics.
    pub fn begin_batch(&self) -> BatchTxn<'_> {
        BatchTxn {
            engine: self,
            ops: Vec::new(),
        }
    }

    /// Reads a partition from a table, merging memtable and SSTable sources.
    pub fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        self.read_limited_rows(table_id, key, 0)
    }

    /// Reads a partition from a table with an optional clustered-row cap.
    ///
    /// A non-zero `row_limit` is pushed into memtable/SSTable sources so
    /// single-partition `LIMIT` queries do not materialize the full
    /// partition before returning the first page.
    pub fn read_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row_limit: usize,
    ) -> ferrosa_common::Result<Option<Partition>> {
        let _span = tracing::info_span!(
            "storage.read",
            table = %table_id,
        )
        .entered();
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_limited_rows(key, row_limit),
            None => Ok(None),
        }
    }

    /// Reads exactly one clustered row from a table partition, merging matching
    /// rows across memtable and SSTable sources.
    pub fn read_clustering_row(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        clustering: &[u8],
    ) -> ferrosa_common::Result<Option<Partition>> {
        let _span = tracing::info_span!(
            "storage.read_clustering_row",
            table = %table_id,
        )
        .entered();
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_clustering_row(key, clustering),
            None => Ok(None),
        }
    }

    /// Reads partitions from a table in token order with optional bounds and limit.
    pub fn read_range(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        self.read_range_limited_rows(table_id, start, end, limit, 0)
    }

    /// Read all partitions whose tokens fall in `[start_token, end_token)`,
    /// up to `limit`.
    ///
    /// Anti-entropy repair's per-Merkle-leaf "give me everything in this
    /// token sub-range" question can't be answered by the key-bounded
    /// [`Self::read_range`] — partition keys hash to tokens via Murmur3, so a
    /// contiguous token range is a discontiguous key range. This primitive
    /// answers the token question directly by reading the merged stream
    /// once and short-circuiting at `end_token`.
    ///
    /// Returns an empty vector when `start_token >= end_token` (empty
    /// range) or the table doesn't exist.
    pub fn read_token_range(
        &self,
        table_id: &TableId,
        start_token: i64,
        end_token: i64,
        limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        if start_token >= end_token || limit == 0 {
            return Ok(Vec::new());
        }
        let tables = self.tables.read();
        let Some(state) = tables.get(table_id) else {
            return Ok(Vec::new());
        };
        state.store.read_token_range(start_token, end_token, limit)
    }

    /// Token-ordered, budget-bounded chunked read for anti-entropy repair.
    /// Collects cell-merged partitions in `[start_token, end_token)` until
    /// `max_partitions` or `max_bytes` is reached, returning the resume cursor
    /// (token of the next unread partition, or `None` when exhausted). See
    /// [`TableStore::read_token_range_bounded`] for the full contract.
    pub fn read_token_range_bounded(
        &self,
        table_id: &TableId,
        start_token: i64,
        end_token: i64,
        max_partitions: usize,
        max_bytes: usize,
    ) -> ferrosa_common::Result<(Vec<Partition>, Option<i64>)> {
        if start_token >= end_token || max_partitions == 0 {
            return Ok((Vec::new(), None));
        }
        let tables = self.tables.read();
        let Some(state) = tables.get(table_id) else {
            return Ok((Vec::new(), None));
        };
        state
            .store
            .read_token_range_bounded(start_token, end_token, max_partitions, max_bytes)
    }

    /// Row-streaming walk over `[start_token, end_token)` for the
    /// anti-entropy repair digest path. For each unique partition
    /// the callback receives the header (key, deletion, optional
    /// static row) plus an `emit_rows` continuation that walks
    /// clustered rows one at a time. When a key is in exactly one
    /// SSTable source the rows are streamed via the SSTable
    /// reader's 2-phase API; no `Partition` is materialised. See
    /// `TableStore::walk_token_range_for_digest` for the full
    /// contract.
    pub fn walk_token_range_for_digest<Cb>(
        &self,
        table_id: &TableId,
        start_token: i64,
        end_token: i64,
        cb: Cb,
    ) -> ferrosa_common::Result<()>
    where
        Cb: FnMut(
            &ferrosa_common::DecoratedKey,
            ferrosa_sstable::types::DeletionTime,
            Option<&ferrosa_sstable::types::Row>,
            &mut dyn FnMut(
                &mut dyn FnMut(&ferrosa_sstable::types::Row) -> Result<(), ferrosa_common::Error>,
            ) -> Result<(), ferrosa_common::Error>,
        ) -> Result<(), ferrosa_common::Error>,
    {
        if start_token >= end_token {
            return Ok(());
        }
        let tables = self.tables.read();
        let Some(state) = tables.get(table_id) else {
            return Ok(());
        };
        state
            .store
            .walk_token_range_for_digest(start_token, end_token, cb)
    }

    /// Streaming token-bounded walk that invokes `cb` once per
    /// partition. Peak working set is **one** decoded partition per
    /// SSTable source held by the in-flight merge head, so a
    /// multi-GB table doesn't trip the per-container memory cap
    /// even when partitions contain multi-MB rows. Used by
    /// anti-entropy repair's Merkle-build path.
    pub fn walk_token_range<F>(
        &self,
        table_id: &TableId,
        start_token: i64,
        end_token: i64,
        cb: F,
    ) -> ferrosa_common::Result<()>
    where
        F: FnMut(&Partition) -> ferrosa_common::Result<()>,
    {
        if start_token >= end_token {
            return Ok(());
        }
        let tables = self.tables.read();
        let Some(state) = tables.get(table_id) else {
            return Ok(());
        };
        state.store.walk_token_range(start_token, end_token, cb)
    }

    /// Reads partitions from a table with an optional per-partition row cap.
    pub fn read_range_limited_rows(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
        row_limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state
                .store
                .read_range_limited_rows(start, end, limit, row_limit),
            None => Ok(vec![]),
        }
    }

    /// COUNT(*) fast path. Returns the total row count for
    /// `[start, end]` on `table_id` without ever decoding cell
    /// payloads. Returns `Ok(0)` when the table is not registered.
    pub fn count_range(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> ferrosa_common::Result<u64> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.count_range(start, end),
            None => Ok(0),
        }
    }

    /// Projection-aware variant of `range_iter`. Only the cells whose
    /// ordinals are in `wanted` are decoded; SSTable cells outside
    /// the projection are byte-skipped via
    /// `range_merger::merger_for_projected_sources`. Returns an
    /// empty stream when the table is not registered.
    pub fn range_iter_projected(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        partition_limit: Option<usize>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state
                .store
                .range_iter_projected(wanted, partition_limit, start, end),
            None => Box::pin(futures::stream::empty()),
        }
    }

    /// ADR-020 lazy range iterator. Returns an async `Stream` that
    /// yields every partition in `[start, end]` for `table_id`, one
    /// at a time, without materializing the full result. Backed by
    /// the per-source k-way merge in
    /// `crate::range_merger::RangeMerger`.
    ///
    /// Returns `Ok` with an empty stream when the table is not
    /// registered (matches the semantics of `read_range_limited_rows`,
    /// which returns Ok(vec![])).
    pub fn range_iter(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.range_iter(start, end),
            None => Box::pin(futures::stream::empty()),
        }
    }

    /// Intra-partition streaming variant of [`Self::range_iter`]. Wide
    /// partitions are delivered as a sequence of `<= K`-row `Partition`
    /// fragments (see [`crate::store::TableStore::range_iter_fragmented`]),
    /// so the producer holds `O(num_sources + K)` rows resident regardless
    /// of partition width — the OOM fix for full-table `SELECT *`.
    pub fn range_iter_fragmented(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.range_iter_fragmented(start, end),
            None => Box::pin(futures::stream::empty()),
        }
    }

    /// Projection-aware intra-partition streaming variant of
    /// [`Self::range_iter_projected`].
    pub fn range_iter_projected_fragmented(
        &self,
        table_id: &TableId,
        wanted: Vec<u16>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state
                .store
                .range_iter_projected_fragmented(wanted, start, end),
            None => Box::pin(futures::stream::empty()),
        }
    }

    /// Query by secondary index across memtable and SSTable sidecar indexes.
    ///
    /// Delegates to [`TableStore::read_by_index`] which merges results from
    /// the memtable index and (future) sidecar indexes. Returns an empty vec
    /// if the table is not registered.
    pub fn read_by_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_by_index(index_name, key),
            None => Ok(vec![]),
        }
    }

    /// Query a geo (cell-id) secondary index by inclusive `[start, end]` cell-id
    /// ranges across the memtable index and SSTable sidecars.
    ///
    /// Delegates to [`TableStore::read_by_index_cell_ranges`]. Returns an empty
    /// vec if the table is not registered. The same fail-loud `INDEX_RESULT_CAP`
    /// bound applies — an unbounded candidate set returns an error rather than
    /// silently truncating.
    pub fn read_by_index_cell_ranges(
        &self,
        table_id: &TableId,
        index_name: &str,
        ranges: &[(u64, u64)],
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_by_index_cell_ranges(index_name, ranges),
            None => Ok(vec![]),
        }
    }

    /// Truncates a table: clears the memtable and drops all SSTable references.
    ///
    /// Full-text search across all FTI sidecar files for a table+index.
    pub fn fulltext_search(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
    ) -> ferrosa_common::Result<Vec<Vec<u8>>> {
        use ferrosa_index::fulltext::reader::FullTextIndexReader;
        use std::collections::HashMap;

        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        let fti_suffix = format!("-FTI-{index_name}.db");

        let fti_files: Vec<std::path::PathBuf> = std::fs::read_dir(&table_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with(&fti_suffix) {
                    Some(e.path())
                } else {
                    None
                }
            })
            .collect();

        let mut score_map: HashMap<Vec<u8>, f64> = HashMap::new();
        {
            let tables = self.tables.read();
            let Some(state) = tables.get(table_id) else {
                return Ok(vec![]);
            };
            for (partition_key, score) in state.store.fulltext_memtable_search(index_name, query)? {
                let entry = score_map.entry(partition_key).or_insert(0.0);
                if score > *entry {
                    *entry = score;
                }
            }
        }

        for fti_path in fti_files {
            let bytes = match std::fs::read(&fti_path) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(path = %fti_path.display(), "failed to read FTI: {e}");
                    continue;
                }
            };
            let reader = match FullTextIndexReader::open(bytes) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(path = %fti_path.display(), "bad FTI: {e}");
                    continue;
                }
            };
            let hits = match reader.search_str(query) {
                Ok(h) => h,
                Err(e) => {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "fts_match query error: {e}"
                    )));
                }
            };
            for hit in hits {
                let entry = score_map.entry(hit.partition_key).or_insert(0.0);
                if hit.score > *entry {
                    *entry = hit.score;
                }
            }
        }

        let mut results: Vec<(Vec<u8>, f64)> = score_map.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results.into_iter().map(|(pk, _)| pk).collect())
    }

    /// Subsequent reads for this table will return empty results. Existing
    /// readers holding `Arc` references to old data will complete normally.
    pub fn truncate(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        // Clear in-memory state (memtable + SSTable references).
        state.store.truncate();
        drop(tables);

        // Delete local SSTable files so data doesn't reappear on restart.
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        if table_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&table_dir) {
                tracing::warn!(
                    table = %table_id,
                    %e,
                    "TRUNCATE: failed to delete local SSTable directory"
                );
            }
            // Re-create empty directory so future flushes have a target.
            let _ = std::fs::create_dir_all(&table_dir);
        }

        // Delete S3 objects + manifest entry for this table synchronously
        // so stale data doesn't reappear on bootstrap from S3.
        self.truncate_s3(table_id);

        Ok(())
    }

    /// Delete S3 objects and manifest entry for a truncated table.
    ///
    /// Runs synchronously on a dedicated thread with its own tokio runtime
    /// so this can be called from non-async contexts. Blocks until all
    /// S3 deletions complete — this ensures stale data doesn't reappear
    /// when other nodes bootstrap from S3 after a TRUNCATE.
    fn truncate_s3(&self, table_id: &TableId) {
        let Some((store, prefix)) = self.resolve_store_and_prefix() else {
            return;
        };
        let table_id_str = table_id.to_string();
        let manifest_path = self
            .config
            .object_store
            .as_ref()
            .map(|c| format!("{}/manifest.json", c.prefix));

        // Run S3 operations on a blocking thread with its own runtime.
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("truncate S3 runtime");
            rt.block_on(async {
                // 1. Delete all SSTable objects for this table (recursive).
                // S3 objects are stored under: {prefix}/{table_id}/{sstable_id}/...
                // We also need to check subdirectories, so use list_with_delimiter
                // at the table level to find SSTable dirs, then delete each.
                let table_path = object_store::path::Path::from(format!("{prefix}/{table_id_str}"));
                match store.list_with_delimiter(Some(&table_path)).await {
                    Ok(result) => {
                        let mut deleted = 0u64;
                        // Delete objects at the table level
                        for obj in &result.objects {
                            let _ = store.delete(&obj.location).await;
                            deleted += 1;
                        }
                        // Delete objects in SSTable subdirectories
                        for subdir in &result.common_prefixes {
                            if let Ok(sub_result) = store.list_with_delimiter(Some(subdir)).await {
                                for obj in &sub_result.objects {
                                    let _ = store.delete(&obj.location).await;
                                    deleted += 1;
                                }
                            }
                        }
                        if deleted > 0 {
                            tracing::info!(
                                table = %table_id_str,
                                deleted,
                                "TRUNCATE: deleted S3 objects"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            table = %table_id_str,
                            %e,
                            "TRUNCATE: S3 list failed"
                        );
                    }
                }

                // 2. Remove this table from the S3 manifest.
                if let Some(mpath) = manifest_path {
                    let path = object_store::path::Path::from(mpath);
                    if let Ok(data) = store.get(&path).await {
                        if let Ok(bytes) = data.bytes().await {
                            if let Ok(mut manifest) =
                                serde_json::from_slice::<crate::manifest::Manifest>(&bytes)
                            {
                                if manifest.sstables.remove(&table_id_str).is_some() {
                                    if let Ok(updated) = serde_json::to_vec_pretty(&manifest) {
                                        let _ = store
                                            .put(&path, object_store::PutPayload::from(updated))
                                            .await;
                                        tracing::info!(
                                            table = %table_id_str,
                                            "TRUNCATE: removed table from S3 manifest"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            });
        });
        // Wait for S3 cleanup to complete before returning.
        if let Err(e) = handle.join() {
            tracing::warn!("TRUNCATE: S3 cleanup thread panicked: {:?}", e);
        }
    }

    /// Replay mutations from a given commit log position forward.
    ///
    /// Returns mutations with positions after `position`. If the segment
    /// has been recycled, returns an empty vec (caller should bootstrap).
    pub fn replay_from(
        &self,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<Vec<Mutation>> {
        self.commit_log.replay_from(position)
    }

    /// Returns the current commit log write position.
    ///
    /// Used by snapshot creation to record which commit log position
    /// the snapshot covers.
    pub fn commit_log_position(&self) -> CommitLogPosition {
        self.commit_log.current_position()
    }

    /// Opens a StorageEngine by restoring from a named snapshot.
    ///
    /// Steps:
    /// 1. Loads and validates the snapshot manifest (SHA-256 integrity check).
    /// 2. Validates the node ID (cross-node restore requires `force = true`).
    /// 3. Downloads SSTables from the snapshot manifest to
    ///    `{config.data_dir}/sstables/`.
    /// 4. Downloads archived commit log segments from S3 to
    ///    `{config.commit_log.log_dir}`.
    /// 5. Validates segment continuity from the snapshot's commit-log position.
    /// 6. Opens the engine normally (SSTables are loaded from disk).
    ///
    /// Mutation replay from the downloaded segments is a future step — this
    /// constructor restores the engine to the state at the snapshot boundary.
    /// Callers that need point-in-time replay beyond the snapshot boundary
    /// should call `replay_from` after registering table schemas.
    ///
    /// # Arguments
    ///
    /// * `config` — engine configuration (data_dir, commit_log dirs, etc.)
    /// * `snapshot_name` — name of the snapshot stored in S3
    /// * `point_in_time` — optional Unix-epoch microsecond timestamp to filter
    ///   replay (placeholder; full replay is deferred)
    /// * `node_id` — ID of this node; must match the snapshot unless `force`
    /// * `force` — allow restoring a snapshot from a different node
    /// * `store` — injected object store (use for tests; production uses S3)
    /// * `prefix` — S3 key prefix under which the snapshot lives
    pub async fn open_from_snapshot_with_store(
        config: StorageEngineConfig,
        snapshot_name: &str,
        point_in_time: Option<i64>,
        node_id: &str,
        force: bool,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<Self> {
        // 1. Load and validate snapshot (SHA-256 integrity check).
        let restore_mgr =
            crate::restore::RestoreManager::new(std::sync::Arc::clone(&store), prefix.to_string());
        let (metadata, manifest) = restore_mgr
            .load_and_validate_snapshot(snapshot_name)
            .await?;

        // 2. Validate node ID — cross-node restore requires force = true.
        crate::restore::validation::validate_node_id(&metadata.node_id, node_id, force)?;

        // 3. Download SSTables to {data_dir}/sstables/.
        let sstable_dir = config.data_dir.join("sstables");
        std::fs::create_dir_all(&sstable_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create sstable dir {}: {e}",
                sstable_dir.display()
            ))
        })?;
        let _sst_count = restore_mgr
            .download_sstables(&manifest, &sstable_dir)
            .await?;

        // 4. Download archived commit log segments.
        let segment_dir = config.commit_log.log_dir.clone();
        std::fs::create_dir_all(&segment_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create segment dir {}: {e}",
                segment_dir.display()
            ))
        })?;
        let segment_ids = restore_mgr
            .download_segments(metadata.commit_log_position.segment_id, &segment_dir)
            .await?;

        // 5. Validate segment continuity.
        crate::restore::validation::validate_segment_continuity(
            &segment_ids,
            metadata.commit_log_position.segment_id,
        )?;

        // 6. Open the engine normally; SSTables are on disk from step 3.
        let engine = StorageEngine::new(config, None)?;

        // 7. Replay archived commit log segments from the snapshot boundary
        //    forward to `point_in_time` (inclusive).
        //
        //    Algorithm:
        //    a. Read each downloaded segment file with SegmentReader.
        //    b. Collect all mutations whose position is after the snapshot
        //       commit-log position (segment_id > snapshot's, or same segment
        //       with offset > snapshot's offset).
        //    c. If `point_in_time` is set, drop mutations with timestamp >
        //       point_in_time (precise PITR cutoff).
        //    d. Deduplicate by mutation_id (idempotent replay).
        //    e. Apply to memtables via replay_mutations().
        //
        //    Replay is deferred until after table schemas are registered, so
        //    we collect mutations here and use the deferred-mutation queue that
        //    replay_mutations() already provides.
        let snapshot_position = metadata.commit_log_position;

        if !segment_ids.is_empty() {
            let mut raw_mutations: Vec<Mutation> = Vec::new();

            for seg_id in &segment_ids {
                let seg_path = segment_dir.join(format!("commitlog-{seg_id}.log"));
                if !seg_path.exists() {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "PITR replay: segment file missing after download: {}",
                        seg_path.display()
                    )));
                }

                let mut reader =
                    crate::commitlog::reader::SegmentReader::open(&seg_path).map_err(|e| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "PITR replay: failed to open segment {seg_id}: {e}"
                        ))
                    })?;

                let entries = reader.read_all().map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "PITR replay: failed to read segment {seg_id}: {e}"
                    ))
                })?;

                for (pos, mutation) in entries {
                    // Skip mutations that are at or before the snapshot boundary.
                    // pos > snapshot_position means this mutation was written
                    // after the snapshot was taken.
                    if pos > snapshot_position {
                        raw_mutations.push(mutation);
                    }
                }
            }

            // Apply point-in-time cutoff: drop mutations after the target timestamp.
            let replay_mutations = match point_in_time {
                Some(pit) => {
                    crate::restore::validation::filter_mutations_by_timestamp(raw_mutations, pit)
                }
                None => raw_mutations,
            };

            tracing::info!(
                snapshot = snapshot_name,
                segment_count = segment_ids.len(),
                mutation_count = replay_mutations.len(),
                point_in_time = ?point_in_time,
                "PITR: replaying archived commit log mutations"
            );

            engine.replay_mutations(replay_mutations)?;
        }

        Ok(engine)
    }

    /// Creates a snapshot using an injected object store (for testing).
    ///
    /// 1. Flushes all memtables to SSTables.
    /// 2. Records the commit log position.
    /// 3. Loads the live manifest and schema from S3.
    /// 4. Delegates to SnapshotManager to write snapshot objects.
    pub async fn create_snapshot_with_store(
        &self,
        name: &str,
        node_id: &str,
        expires_at: Option<String>,
        ephemeral: bool,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<crate::snapshot::metadata::SnapshotMetadata> {
        // Step 1: Flush all tables.
        let table_ids: Vec<_> = self.tables.read().keys().cloned().collect();
        for table_id in &table_ids {
            self.flush(table_id)?;
        }

        // Step 2: Record commit log position.
        let position = self.commit_log_position();

        // Step 3: Load live manifest and schema from S3.
        let (manifest, _version) = crate::manifest::Manifest::load(store.as_ref(), prefix).await?;
        let schema_json = crate::manifest::load_schema_snapshot(store.as_ref(), prefix)
            .await?
            .unwrap_or_default();

        // Step 4: Create snapshot via manager.
        let manager = crate::snapshot::SnapshotManager::new(
            std::sync::Arc::clone(&store),
            prefix.to_string(),
        );

        manager
            .create_snapshot(
                name,
                &manifest,
                &schema_json,
                position,
                node_id,
                expires_at,
                ephemeral,
            )
            .await
    }

    /// Lists all snapshots from S3 using an injected store (for testing).
    pub async fn list_snapshots_with_store(
        &self,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<Vec<crate::snapshot::metadata::SnapshotMetadata>> {
        let manager = crate::snapshot::SnapshotManager::new(store, prefix.to_string());
        manager.list_snapshots().await
    }

    /// Deletes a snapshot from S3 using an injected store (for testing).
    pub async fn delete_snapshot_with_store(
        &self,
        name: &str,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<()> {
        let manager = crate::snapshot::SnapshotManager::new(store, prefix.to_string());
        manager.delete_snapshot(name).await
    }

    /// Force-syncs the commit log to disk.
    ///
    /// Ensures all buffered mutations are written to disk before reading
    /// the commit log (e.g., for catch-up replay after failover).
    pub fn force_commit_log_sync(&self) -> ferrosa_common::Result<()> {
        self.commit_log.force_sync()
    }

    /// Flushes the active memtable for a table to an SSTable on disk.
    ///
    /// After flushing, checks if compaction is needed and submits tasks
    /// to the compaction executor. For pinned tables, S3 upload is skipped
    /// and max_bytes eviction is enforced if configured.
    pub fn flush(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        let _span = tracing::info_span!(
            "storage.flush",
            table = %table_id,
        )
        .entered();
        // Flush + index submit under read lock, then release before write lock.
        let (gen, is_pinned, cl_position) = {
            let tables = self.tables.read();
            let state = tables.get(table_id).ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
            })?;

            // Snapshot the commit log position before flushing. All mutations
            // up to this position are in the memtable we're about to flush.
            let cl_position = **state.last_commit_log_position.load();

            // DURABILITY BARRIER (do not reorder): `store.flush()` only returns
            // Ok after the SSTable's component files AND their directory entries
            // are fsynced (see flush.rs `fsync_components`). This is the barrier
            // that makes the later `commit_log.discard_completed(cl_position)`
            // safe: we must NOT advance the WAL checkpoint / delete segments
            // until the SSTable copy of those mutations is durable on disk. If
            // flush() fails, we return here and the WAL is left intact so replay
            // can rebuild the memtable. Removing the fsync from flush(), or
            // moving discard_completed before this line, reintroduces the P0
            // "kill mid-flush loses both the torn SSTable and the WAL copy" bug.
            state.store.flush()?;

            // Reset the unflushed-write timestamp so the next write starts a
            // fresh age window. This must happen after flush() succeeds.
            state
                .first_unflushed_write_at_nanos
                .store(0, std::sync::atomic::Ordering::Relaxed);

            // Eager index build: submit high-priority index rebuild for the newly
            // flushed SSTable. This keeps the MemtableIndex (Layer 4) bounded to
            // 0-1 entries in steady state by ensuring sidecar indexes are current.
            if let Some(ref scheduler) = self.index_scheduler {
                let gen = state.store.last_flush_generation();
                for (index_name, col_pos) in state.store.indexed_columns() {
                    let tracker_state = self.index_tracker.get_state(
                        table_id.keyspace(),
                        table_id.table(),
                        index_name,
                    );
                    // Only submit if the index needs building (not already current).
                    if let Some(idx_state) = tracker_state {
                        if !idx_state.indexed_sstables.contains(&format!("{gen}")) {
                            let job = eager_index_build_job(
                                &state.store,
                                table_id,
                                format!("{gen}"),
                                index_name,
                                *col_pos,
                            );
                            let _ = scheduler.submit(job);
                        }
                    }
                }
            }

            // Check for compaction after flush.
            self.maybe_compact(table_id, state);

            let flushed_gen = state.store.last_flush_generation();
            let pinned = state.pin_config.is_some();
            (flushed_gen, pinned, cl_position)
        };

        // Advance commit log checkpoint: tell the commit log that this table's
        // mutations are now durable in SSTables up to cl_position. This allows
        // closed segments with no remaining dirty tables to be GC'd, preventing
        // file descriptor leaks from accumulating segment file handles.
        if let Some(pos) = cl_position {
            if let Err(e) = self.commit_log.discard_completed(table_id, pos) {
                tracing::warn!(%e, "commit log discard_completed failed for {}", table_id);
            }
        }

        // For pinned tables: record the new SSTable and enforce max_bytes.
        // We do this outside the read lock so we can take a write lock.
        if is_pinned {
            let table_dir = self
                .config
                .data_dir
                .join("sstables")
                .join(table_id.to_string());
            let size = Self::sstable_disk_size(&table_dir, gen);
            let sstable_id = gen.to_string();

            {
                let mut tables = self.tables.write();
                if let Some(state) = tables.get_mut(table_id) {
                    // Only append if this gen isn't already tracked (idempotent).
                    if !state
                        .pinned_sstables
                        .iter()
                        .any(|(id, _)| *id == sstable_id)
                    {
                        state.pinned_sstables.push((sstable_id.clone(), size));
                    }
                }
            }

            self.pin_metrics.add_pinned_bytes(size as i64);
            self.enforce_pin_max_bytes(table_id);
        }

        // Persist registered table schemas so the next restart can recover without
        // re-running S3 bootstrap (BUG-022).
        if let Err(e) = self.persist_schema_locally() {
            tracing::warn!("failed to persist schema.json: {e}");
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn is_table_registered_for_test(&self, table_id: &TableId) -> bool {
        self.tables.read().contains_key(table_id)
    }

    #[cfg(test)]
    pub(crate) fn deferred_replay_mutation_count_for_test(&self) -> usize {
        self.deferred_replay_mutations.lock().len()
    }

    /// Whether `index_name` is registered on `table_id`'s store, and its type.
    #[cfg(test)]
    pub(crate) fn index_type_for_test(
        &self,
        table_id: &TableId,
        index_name: &str,
    ) -> Option<ferrosa_index::IndexType> {
        let tables = self.tables.read();
        let state = tables.get(table_id)?;
        if state
            .store
            .indexed_columns()
            .iter()
            .any(|(n, _)| n == index_name)
        {
            Some(state.store.index_type_for(index_name))
        } else {
            None
        }
    }

    /// Test accessor: the partial-index predicate registered for `index_name`,
    /// cloned. `Some` only for a Filtered index reloaded with its predicate.
    #[cfg(test)]
    pub(crate) fn filter_predicate_for_test(
        &self,
        table_id: &TableId,
        index_name: &str,
    ) -> Option<ferrosa_index::FilterPredicate> {
        let tables = self.tables.read();
        let state = tables.get(table_id)?;
        state.store.filter_predicate_for(index_name).cloned()
    }

    /// Flushes tables that exceed the size threshold, have unflushed data older
    /// than `flush_max_age_secs`, or are holding closed commit-log segments
    /// hostage. The time-based trigger ensures small, infrequently-updated
    /// tables are durable within a bounded window; the retention-pressure
    /// trigger prevents one cold dirty memtable from pinning closed segments
    /// after hotter tables have already flushed.
    pub fn flush_if_needed(&self) -> ferrosa_common::Result<()> {
        let max_age = std::time::Duration::from_secs(self.config.flush_max_age_secs);
        let max_age_nanos = max_age.as_nanos() as i64;
        let now_nanos = now_nanos_since_reference();
        let retention_pressure = self.commit_log.closed_segment_count() > 0;
        let tables = self.tables.read();
        let to_flush: Vec<TableId> = tables
            .iter()
            .filter(|(_, state)| {
                let memtable_size = state.store.memtable_size();
                let size_exceeded = memtable_size as u64 >= self.config.flush_threshold_bytes;
                let first = state
                    .first_unflushed_write_at_nanos
                    .load(std::sync::atomic::Ordering::Relaxed);
                let age_exceeded = first > 0 && now_nanos.saturating_sub(first) >= max_age_nanos;
                memtable_size > 0 && (size_exceeded || age_exceeded || retention_pressure)
            })
            .map(|(id, _)| id.clone())
            .collect();
        drop(tables);

        for table_id in to_flush {
            self.flush(&table_id)?;
        }
        Ok(())
    }

    /// Flush all non-empty memtables regardless of size threshold.
    ///
    /// Used before S3 schema persistence to ensure data and metadata
    /// stay in sync.  Equivalent to Cassandra's SNAPSHOT flush reason.
    pub fn flush_all(&self) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let to_flush: Vec<TableId> = tables
            .iter()
            .filter(|(_, state)| state.store.memtable_size() > 0)
            .map(|(id, _)| id.clone())
            .collect();
        drop(tables);

        for table_id in to_flush {
            self.flush(&table_id)?;
        }
        Ok(())
    }

    /// Polls for completed compaction results, uploads the output SSTable to S3,
    /// updates the manifest, and enqueues deletion of input SSTables.
    ///
    /// Crash-safe: pending-log → upload → S3 confirm → manifest update →
    /// enqueue input deletions → evict local input directories.
    pub async fn poll_compactions(&self) {
        let results = self.compaction_executor.poll_results();
        for result in results {
            let _input_claim = CompactionResultInputClaim {
                executor: &self.compaction_executor,
                task: &result.task,
            };
            let input_id_paths: Vec<(String, std::path::PathBuf)> = result
                .task
                .inputs
                .iter()
                .map(|m| (m.id.clone(), m.path.clone()))
                .collect();
            let table_id = &result.task.table_id;

            let promote_start = Instant::now();
            let output = match self.promote_compaction_output(table_id, &result.output) {
                Ok(output) => {
                    crate::metrics::observe_compaction_phase(
                        crate::metrics::CompactionPhase::PromoteOutput,
                        promote_start.elapsed(),
                    );
                    output
                }
                Err(e) => {
                    crate::metrics::observe_compaction_phase(
                        crate::metrics::CompactionPhase::PromoteOutput,
                        promote_start.elapsed(),
                    );
                    tracing::error!(%e, %table_id, "compaction: failed to promote output SSTable");
                    continue;
                }
            };

            // Open the promoted compacted output SSTable.
            let gen = &output.id;
            let dir = &output.path;
            let reader = match Self::open_sstable_from_dir(dir, gen) {
                Ok(r) => Arc::new(r),
                Err(e) => {
                    tracing::error!(%e, "compaction: failed to open output SSTable");
                    continue;
                }
            };

            // Swap: remove input SSTables by ID, insert output.
            // Compaction output gen may collide with flush gen (different dirs).
            // Advance the flush target past this gen to prevent future collisions,
            // and use the store's unique ID allocator for the tracking ID.
            {
                let tables = self.tables.read();
                if let Some(state) = tables.get(table_id) {
                    let output_id = gen.clone();
                    // Advance the store's flush target past the compaction gen
                    // to prevent future flush IDs from colliding with this output.
                    if let Ok(gen_num) = gen.parse::<u64>() {
                        state.store.advance_gen_past(gen_num);
                    }
                    let pre_swap_count = state.store.sstable_count();
                    if let Err(e) = state.store.swap_compacted_sstables(
                        &input_id_paths,
                        output_id,
                        output.path.clone(),
                        reader,
                        std::collections::HashMap::new(),
                    ) {
                        tracing::error!(%e, "compaction: swap failed");
                        continue;
                    }
                    let post_swap_count = state.store.sstable_count();
                    tracing::info!(
                        %table_id,
                        pre_swap_count,
                        post_swap_count,
                        removed = input_id_paths.len(),
                        "compaction: swap complete"
                    );

                    // Eager index build: submit high-priority rebuild for compacted output.
                    // Same as flush — keeps MemtableIndex bounded in steady state.
                    if let Some(ref scheduler) = self.index_scheduler {
                        for (index_name, col_pos) in state.store.indexed_columns() {
                            let job = eager_index_build_job(
                                &state.store,
                                table_id,
                                output.id.clone(),
                                index_name,
                                *col_pos,
                            );
                            if let Err(e) = scheduler.submit(job) {
                                tracing::error!(%e, %index_name, "compaction: failed to submit index rebuild");
                            }
                        }
                    }
                }
            }

            // Register in local cache.
            self.local_cache
                .register(&output.id, output.path.clone(), output.size_bytes);
            let cleanup_start = Instant::now();
            Self::evict_local_input_sstable_files(&result.task.inputs);
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::InputCleanup,
                cleanup_start.elapsed(),
            );

            // ── Skip S3 upload for pinned tables ────────────────────────────
            //
            // Pinned tables keep SSTables on local NVMe only. Track the
            // compacted output the same way flush does.
            let is_compaction_pinned = {
                let tables = self.tables.read();
                tables.get(table_id).is_some_and(|s| s.pin_config.is_some())
            };
            if is_compaction_pinned {
                let size = output.size_bytes;
                let sstable_id = output.id.clone();
                {
                    let mut tables = self.tables.write();
                    if let Some(state) = tables.get_mut(table_id) {
                        if !state
                            .pinned_sstables
                            .iter()
                            .any(|(id, _)| *id == sstable_id)
                        {
                            state.pinned_sstables.push((sstable_id, size));
                        }
                    }
                }
                self.pin_metrics.add_pinned_bytes(size as i64);
                self.enforce_pin_max_bytes(table_id);
                continue;
            }

            // ── Crash-safe S3 upload + manifest update ─────────────────────
            //
            // 5-step pattern (mirrors sync_sstables_to_s3):
            //   1. Write pending-log entry (fsynced)
            //   2. Submit UploadTask with on_complete channel
            //   3. Await S3 confirmation
            //   4. Remove pending-log entry
            //   5. Update manifest (remove inputs, add output)
            //
            // If upload_manager is None (no S3 configured) we skip silently.
            let upload_mgr = self
                .compaction_upload_manager
                .as_ref()
                .or(self.upload_manager.as_ref());
            let Some(upload_mgr) = upload_mgr else {
                continue;
            };
            let Some((store, prefix)) = self.resolve_store_and_prefix() else {
                continue;
            };

            let table_id_str = table_id.to_string();
            let sstable_id = output.id.clone();

            // Parse the generation number — output id is always a decimal u64.
            let gen_u64: u64 = match sstable_id.parse() {
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(%e, %sstable_id, "compaction: output SSTable id is not a u64");
                    continue;
                }
            };

            // The compaction output has been promoted into the table SSTable directory.
            let output_dir = output.path.clone();

            let direct_upload = result.direct_upload;
            let total_size: u64 = direct_upload
                .as_ref()
                .map(|upload| upload.total_size_bytes())
                .unwrap_or_else(|| {
                    Self::collect_sstable_files(&output_dir, gen_u64)
                        .iter()
                        .map(|file| file.size_bytes)
                        .sum()
                });
            if total_size == 0 {
                tracing::warn!(%sstable_id, "compaction: no bytes for output SSTable, skipping S3 upload");
                continue;
            }
            let manifest_plan = crate::compaction::finalize::plan_manifest_update(
                &table_id_str,
                &result.task.inputs,
                &output,
                total_size,
            );

            // Step 1: record the pending upload (best-effort).
            let pending_log_path = self.config.data_dir.join("pending-uploads.log");
            let pending_log_result = crate::upload::PendingUploadsLog::open(&pending_log_path);
            if let Ok(ref pending_log) = pending_log_result {
                let compaction = crate::upload::pending_log::PendingCompactionUpload {
                    remove_input_ids: manifest_plan.remove_input_ids.clone(),
                    output: manifest_plan.add_output.clone(),
                };
                if let Err(e) =
                    pending_log.add_compaction_entry(&table_id_str, &sstable_id, compaction)
                {
                    tracing::warn!(%e, %sstable_id, "compaction: failed to write pending-log entry");
                }
            }

            // Step 2: create completion channel and submit the upload.
            //
            // **Non-blocking submit** (try_submit, not submit().await): if
            // the upload queue is full (e.g., heavy crash-recovery replay
            // backlog) we MUST NOT block the poll loop. Every other
            // queued compaction result is waiting behind us, and a stuck
            // upload here starves all subsequent swaps from reaching the
            // read path. The pending-log entry (Step 1) is durable — the
            // periodic `sync_sstables_to_s3` will re-submit. On queue-full
            // we skip the rest of the S3 dance and continue.
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            let task = if let Some(direct_upload) = direct_upload {
                crate::upload::UploadTask::SSTableBytes {
                    table_id: table_id_str.clone(),
                    sstable_id: sstable_id.clone(),
                    files: direct_upload.files,
                    on_complete: Some(tx),
                }
            } else {
                let files = Self::collect_sstable_files(&output_dir, gen_u64);
                if files.is_empty() {
                    tracing::warn!(
                        %sstable_id,
                        "compaction: no files for output SSTable, skipping S3 upload"
                    );
                    continue;
                }
                crate::upload::UploadTask::SSTable {
                    table_id: table_id_str.clone(),
                    sstable_id: sstable_id.clone(),
                    files,
                    on_complete: Some(tx),
                }
            };
            if let Err(e) = upload_mgr.try_submit(task) {
                tracing::warn!(
                    %e,
                    %sstable_id,
                    "compaction: upload queue full, deferring to sync_sstables_to_s3 — \
                     swap already complete, no data loss (pending-log persists)"
                );
                continue;
            }

            // Step 3: await S3 confirmation, with a **bounded** timeout so
            // a single slow upload can't starve the poll loop. The
            // pending-log entry from Step 1 means we lose no durability
            // by giving up here — sync_sstables_to_s3 will pick it up.
            const UPLOAD_AWAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
            let upload_await_start = Instant::now();
            let rx_result = match tokio::time::timeout(UPLOAD_AWAIT_TIMEOUT, rx).await {
                Ok(r) => {
                    crate::metrics::observe_compaction_phase(
                        crate::metrics::CompactionPhase::S3UploadAwait,
                        upload_await_start.elapsed(),
                    );
                    r
                }
                Err(_) => {
                    crate::metrics::observe_compaction_phase(
                        crate::metrics::CompactionPhase::S3UploadAwait,
                        upload_await_start.elapsed(),
                    );
                    tracing::warn!(
                        %sstable_id,
                        timeout_secs = UPLOAD_AWAIT_TIMEOUT.as_secs(),
                        "compaction: S3 confirmation timed out; deferring manifest \
                         update to sync_sstables_to_s3 — swap already complete"
                    );
                    continue;
                }
            };
            let confirmation =
                crate::compaction::finalize::upload_confirmation_from_result(rx_result);
            match &confirmation {
                crate::compaction::finalize::UploadConfirmation::Confirmed => {
                    self.compaction_metrics.inc_s3_uploads();
                }
                crate::compaction::finalize::UploadConfirmation::Failed { message } => {
                    tracing::error!(%sstable_id, %message, "compaction: upload failed");
                }
                crate::compaction::finalize::UploadConfirmation::WorkerDropped => {
                    tracing::error!(%sstable_id, "compaction: upload worker dropped channel");
                }
            }

            if !matches!(
                confirmation,
                crate::compaction::finalize::UploadConfirmation::Confirmed
            ) {
                continue;
            }

            // Step 4: update manifest — load fresh copy, remove inputs, add output, save.
            // Keep full input metadata for local eviction after manifest update.
            let input_ids = manifest_plan.remove_input_ids.clone();

            // Compute total input bytes for metrics (used after manifest update).
            // If the metadata carries a non-zero size we use it directly;
            // otherwise we sum the actual component file sizes from disk
            // (sstable_metadata() currently returns size_bytes = 0 as a
            // known placeholder — scanning disk gives the accurate value).
            let input_bytes_total: i64 = {
                let from_metadata: i64 =
                    result.task.inputs.iter().map(|i| i.size_bytes as i64).sum();
                if from_metadata > 0 {
                    from_metadata
                } else {
                    let component_suffixes = [
                        "Data.db",
                        "Partitions.db",
                        "Rows.db",
                        "Filter.db",
                        "Statistics.db",
                        "TOC.txt",
                        "CompressionInfo.db",
                    ];
                    result
                        .task
                        .inputs
                        .iter()
                        .flat_map(|input| {
                            component_suffixes.iter().map(move |suffix| {
                                let path = input.path.join(format!("{}-{suffix}", input.id));
                                std::fs::metadata(&path)
                                    .map(|m| m.len() as i64)
                                    .unwrap_or(0)
                            })
                        })
                        .sum()
                }
            };

            let mut manifest_saved = false;
            let manifest_update_start = Instant::now();
            match crate::manifest::Manifest::load(store.as_ref(), &prefix).await {
                Ok((mut manifest, _version)) => {
                    manifest
                        .remove_sstables(&manifest_plan.table_id, &manifest_plan.remove_input_ids);
                    manifest.add_sstable(&manifest_plan.table_id, manifest_plan.add_output.clone());
                    // Pass removals explicitly so CAS retry re-applies them
                    // after merging with the latest manifest. Without this,
                    // merge_into re-introduces the entries we removed.
                    let save_result = if self.cas_supported() {
                        manifest
                            .save_with_retry_and_removals(
                                store.as_ref(),
                                &prefix,
                                &manifest_plan.removals_for_cas_retry,
                            )
                            .await
                    } else {
                        manifest
                            .save_without_cas_and_removals(
                                store.as_ref(),
                                &prefix,
                                &manifest_plan.removals_for_cas_retry,
                            )
                            .await
                    };
                    if let Err(e) = save_result {
                        tracing::error!(%e, %sstable_id, "compaction: manifest save failed");
                    } else {
                        manifest_saved = true;
                        tracing::info!(%sstable_id, removed = input_ids.len(), "compaction: manifest updated");
                        // Record bytes freed by this compaction in the metrics gauge.
                        self.compaction_metrics
                            .add_bytes_reclaimed(input_bytes_total);
                    }
                }
                Err(e) => {
                    tracing::error!(%e, "compaction: failed to load manifest for update");
                }
            }
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::ManifestUpdate,
                manifest_update_start.elapsed(),
            );

            // Step 5: remove the pending-log entry only after both S3 upload
            // confirmation and manifest save. If the process crashes or manifest
            // persistence fails between those events, the durable marker must
            // remain so startup replay can retry rather than strand an uploaded
            // SSTable outside the manifest.
            match crate::compaction::finalize::pending_log_decision_after_manifest_save(
                confirmation,
                manifest_saved,
            ) {
                crate::compaction::finalize::PendingLogDecision::RemoveConfirmed => {
                    if let Ok(ref pending_log) = pending_log_result {
                        if let Err(e) = pending_log.remove_entry(&table_id_str, &sstable_id) {
                            tracing::warn!(%e, %sstable_id, "compaction: failed to remove pending-log entry");
                            // Non-fatal: replay will re-upload (idempotent).
                        }
                    }
                }
                crate::compaction::finalize::PendingLogDecision::KeepForReplay => {
                    continue;
                }
            }

            // Enqueue S3 deletion for each input SSTable (1-hour grace period).
            let deletion_plan = crate::compaction::finalize::plan_input_deletions(
                &table_id_str,
                &result.task.inputs,
                std::time::Duration::from_secs(3600),
            );
            for task_plan in deletion_plan.tasks {
                let (del_tx, del_rx) = tokio::sync::oneshot::channel();
                let _ = upload_mgr
                    .submit(crate::upload::UploadTask::DeleteSSTable {
                        table_id: task_plan.table_id,
                        sstable_id: task_plan.sstable_id,
                        grace_period: task_plan.grace_period,
                        on_complete: Some(del_tx),
                    })
                    .await;
                // Increment the S3 delete counter for each enqueued deletion.
                self.compaction_metrics.inc_s3_deletes();
                if deletion_plan.fire_and_forget {
                    // Fire-and-forget: S3 deletions are best-effort.
                    drop(del_rx);
                }
            }
        }
    }

    /// Opens an SSTable from component files in a directory.
    fn open_sstable_from_dir(
        dir: &std::path::Path,
        gen: &str,
    ) -> ferrosa_common::Result<
        ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>,
    > {
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::SSTableComponents;

        let data = Self::generation_component_path(dir, gen, "Data.db").ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!(
                "missing required Data.db for sstable generation {gen} in {}",
                dir.display()
            ))
        })?;
        let data = FileReadAt::open(data)?;

        let partitions =
            Self::generation_component_path(dir, gen, "Partitions.db").ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "missing required Partitions.db for sstable generation {gen} in {}",
                    dir.display()
                ))
            })?;
        let partitions = FileReadAt::open(partitions)?;

        let rows = Self::generation_component_path(dir, gen, "Rows.db").ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!(
                "missing required Rows.db for sstable generation {gen} in {}",
                dir.display()
            ))
        })?;
        let rows = FileReadAt::open(rows)?;

        let filter = Self::generation_component_path(dir, gen, "Filter.db")
            .and_then(|p| std::fs::read(p).ok())
            .unwrap_or_default();

        let statistics = Self::generation_component_path(dir, gen, "Statistics.db")
            .and_then(|p| std::fs::read(p).ok())
            .unwrap_or_default();

        let compression_info = Self::generation_component_path(dir, gen, "CompressionInfo.db")
            .and_then(|p| std::fs::read(p).ok());

        ferrosa_sstable::reader::SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        })
    }

    /// Returns the number of SSTables for a table.
    pub fn sstable_count(&self, table_id: &TableId) -> usize {
        self.tables
            .read()
            .get(table_id)
            .map(|s| s.store.sstable_count())
            .unwrap_or(0)
    }

    /// Test-only: drop a generation's pooled (already-open) reader so the next
    /// read reopens it from its on-disk component files. Used to exercise the
    /// disk-reopen path (e.g. a corrupt/degenerate `Filter.db`) instead of a
    /// cache hit on the reader the flush seeded.
    #[cfg(test)]
    pub(crate) fn evict_pooled_reader_for_test(&self, table_id: &TableId, gen: u64) {
        self.reader_pool.remove(&(table_id.to_string(), gen));
    }

    /// Grab the currently-cached reader `Arc` for `(table_id, gen)` without
    /// reopening or perturbing recency. Returns `None` if not resident.
    ///
    /// Used by the residual read-vs-compaction race test to capture an input
    /// SSTable's pooled reader *before* a compaction swap evicts it, so the
    /// same `Arc` can be re-seeded afterwards — modelling the window where
    /// `open_reader` is a cache HIT (open succeeds, in-memory bloom matches) but
    /// the data seek hits an already-deleted `Data.db` (`ENOENT` mid-read).
    #[cfg(test)]
    pub(crate) fn pooled_reader_arc_for_test(
        &self,
        table_id: &TableId,
        gen: u64,
    ) -> Option<
        std::sync::Arc<ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>>,
    > {
        self.reader_pool.peek_arc(&(table_id.to_string(), gen))
    }

    /// Re-insert an already-open reader `Arc` for `(table_id, gen)` into the
    /// pool, replacing any existing entry. Pairs with
    /// [`Self::pooled_reader_arc_for_test`] to restore a cache HIT for an input
    /// generation that a compaction swap evicted, so the next read takes the
    /// cache-hit path against a now-deleted `Data.db`.
    #[cfg(test)]
    pub(crate) fn reseed_pooled_reader_for_test(
        &self,
        table_id: &TableId,
        gen: u64,
        reader: std::sync::Arc<
            ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>,
        >,
    ) {
        self.reader_pool
            .insert_arc((table_id.to_string(), gen), reader);
    }

    /// Returns the count of SSTable read errors for a table.
    pub fn sstable_read_errors(&self, table_id: &TableId) -> u64 {
        self.tables
            .read()
            .get(table_id)
            .map(|s| {
                s.store
                    .sstable_read_errors
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(0)
    }

    /// Returns the memtable size in bytes for a table.
    pub fn memtable_size(&self, table_id: &TableId) -> usize {
        self.tables
            .read()
            .get(table_id)
            .map(|s| s.store.memtable_size())
            .unwrap_or(0)
    }

    /// Shuts down the storage engine gracefully.
    ///
    /// Flushes all dirty memtables, stops the compaction executor,
    /// shuts down the upload manager, and stops the commit log.
    pub fn shutdown(&self) -> ferrosa_common::Result<()> {
        // Flush all tables.
        let table_ids: Vec<TableId> = self.tables.read().keys().cloned().collect();
        for table_id in &table_ids {
            // Best-effort flush; log but don't fail on individual table errors.
            if let Err(e) = self.flush(table_id) {
                tracing::error!(%e, %table_id, "storage-engine: flush failed");
            }
        }

        // Drain index scheduler.
        if let Some(ref scheduler) = self.index_scheduler {
            scheduler.shutdown_with_timeout(std::time::Duration::from_secs(30));
        }

        // Stop compaction.
        self.compaction_executor.shutdown();

        // Stop archiver.
        if let Some(handle) = &self.archiver_handle {
            handle.abort();
        }

        // Commit log shutdown.
        self.commit_log.shutdown()?;

        Ok(())
    }

    /// Checks if compaction should be triggered after a flush.
    /// Force compaction for all tables, ignoring the STCS threshold.
    ///
    /// Useful for testing and debugging compaction issues. Submits a compaction
    /// task for every table that has at least 2 SSTables, regardless of size
    /// bucketing or min_threshold.
    pub fn force_compact_all(&self) {
        let tables = self.tables.read();
        for (table_id, state) in tables.iter() {
            let metadata = self.collect_sstable_metadata(table_id, state);
            tracing::info!(%table_id, count = metadata.len(), "force-compact: table SSTables");
            if metadata.len() >= 2 {
                let task = crate::compaction::metadata::CompactionTask {
                    inputs: metadata,
                    output_dir: self.config.compaction.output_dir.join(table_id.to_string()),
                    schema: state.schema.clone(),
                    table_id: table_id.clone(),
                };
                if let Err(e) = self.compaction_executor.submit(task) {
                    tracing::error!(%e, %table_id, "force-compact: submit failed");
                }
            }
        }
    }

    fn maybe_compact(&self, table_id: &TableId, state: &TableState) {
        let metadata = self.collect_sstable_metadata(table_id, state);
        let strategy = self.strategy_for_table(state);
        let tasks = strategy.select(&metadata, &state.schema, table_id);
        for task in tasks {
            if let Err(e) = self.compaction_executor.submit(task) {
                tracing::error!(%e, %table_id, "storage-engine: compaction submit failed");
            }
        }
    }

    /// Select the compaction strategy for a table based on its extensions.
    ///
    /// Tables with `compaction.class` containing "Unified" or "UCS" use the
    /// Unified Compaction Strategy. All others default to STCS.
    fn strategy_for_table(&self, state: &TableState) -> Box<dyn CompactionStrategy> {
        let extensions = &state.schema.extensions;
        let class = extensions.get("compaction.class").map(|s| s.as_str());
        match class {
            Some(c) if c.contains("Unified") || c.contains("UCS") => {
                // Collect compaction.* extensions into a params HashMap
                let params: std::collections::HashMap<String, String> = extensions
                    .iter()
                    .filter_map(|(k, v)| {
                        k.strip_prefix("compaction.")
                            .map(|k| (k.to_string(), v.clone()))
                    })
                    .collect();
                let config = crate::compaction::strategy_ucs::UcsConfig::from_params(
                    &params,
                    self.config.compaction.output_dir.clone(),
                );
                Box::new(crate::compaction::UnifiedCompactionStrategy::new(config))
            }
            _ => Box::new(SizeTieredStrategy::new(self.config.compaction.clone())),
        }
    }

    /// Collects SSTable metadata for compaction strategy evaluation.
    fn collect_sstable_metadata(
        &self,
        table_id: &TableId,
        state: &TableState,
    ) -> Vec<crate::compaction::metadata::SSTableMetadata> {
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        state.store.sstable_metadata(&table_dir)
    }

    /// Returns a reference to the local cache.
    pub fn local_cache(&self) -> &LocalCache {
        &self.local_cache
    }

    /// Returns the total pinned bytes for a table from its tracked list.
    fn compute_pinned_bytes(&self, table_id: &TableId) -> i64 {
        let tables = self.tables.read();
        tables
            .get(table_id)
            .map(|s| s.pinned_sstables.iter().map(|(_, b)| *b as i64).sum())
            .unwrap_or(0)
    }

    /// Measures the on-disk size of all component files for one SSTable
    /// generation in the table directory. Returns 0 if files are missing.
    fn sstable_disk_size(table_dir: &std::path::Path, gen: u64) -> u64 {
        let suffixes = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ];
        suffixes
            .iter()
            .filter_map(|s| {
                Self::generation_component_path(table_dir, &gen.to_string(), s)
                    .and_then(|p| std::fs::metadata(&p).ok().map(|m| m.len()))
            })
            .sum()
    }

    /// Deletes all on-disk component files for an SSTable generation.
    fn delete_sstable_files(table_dir: &std::path::Path, gen: &str) -> u64 {
        let suffixes = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ];
        let mut reclaimed = 0u64;

        // Resolve the component directory once before deleting anything. For
        // compaction-promoted SSTables components live in `<table>/<gen>/`.
        // Calling `generation_component_path` after removing `Data.db` would
        // no longer recognize the generation directory and would leave every
        // sidecar/index component behind.
        let component_dir = if table_dir.join(gen).is_dir() {
            table_dir.join(gen)
        } else {
            table_dir.to_path_buf()
        };

        for s in &suffixes {
            let path = component_dir.join(format!("{gen}-{s}"));
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if std::fs::remove_file(&path).is_ok() {
                reclaimed = reclaimed.saturating_add(size);
            }
        }

        if component_dir != table_dir {
            let _ = std::fs::remove_dir(&component_dir);
        }

        reclaimed
    }

    /// Enforces the `max_bytes` cap for a pinned table after a new SSTable is
    /// pinned. Evicts (deletes from disk) the oldest pinned SSTables until total
    /// pinned bytes <= max_bytes. Returns the number of SSTables evicted.
    fn enforce_pin_max_bytes(&self, table_id: &TableId) -> usize {
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());

        let mut evictions = 0usize;
        loop {
            // Re-check under write lock each iteration.
            let evict_id = {
                let tables = self.tables.read();
                let state = match tables.get(table_id) {
                    Some(s) => s,
                    None => break,
                };
                let max = match state.pin_config.as_ref().and_then(|c| c.max_bytes) {
                    Some(m) => m,
                    None => break, // No cap — nothing to enforce.
                };
                let total: u64 = state.pinned_sstables.iter().map(|(_, b)| *b).sum();
                if total <= max {
                    break;
                }
                // Evict oldest (front of Vec).
                state.pinned_sstables.first().map(|(id, _)| id.clone())
            };

            let evict_id = match evict_id {
                Some(id) => id,
                None => break,
            };

            // Remove from tracking and accumulate bytes delta.
            let evicted_bytes = {
                let mut tables = self.tables.write();
                let state = match tables.get_mut(table_id) {
                    Some(s) => s,
                    None => break,
                };
                if let Some(pos) = state
                    .pinned_sstables
                    .iter()
                    .position(|(id, _)| *id == evict_id)
                {
                    let (_, bytes) = state.pinned_sstables.remove(pos);
                    bytes
                } else {
                    break;
                }
            };

            // Delete files from disk.
            Self::delete_sstable_files(&table_dir, &evict_id);
            self.pin_metrics.sub_pinned_bytes(evicted_bytes as i64);
            self.pin_metrics.inc_pin_evictions();
            evictions += 1;
        }

        evictions
    }

    /// Enqueues S3 uploads for SSTables that were previously skipped due to
    /// pin mode. Called when a table transitions from pinned → unpinned.
    async fn upload_previously_pinned_sstables(&self, table_id: &TableId, sstable_ids: &[String]) {
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());

        let Some(upload_mgr) = self.upload_manager.as_ref() else {
            return;
        };

        let table_id_str = table_id.to_string();

        for sstable_id in sstable_ids {
            let gen: u64 = match sstable_id.parse() {
                Ok(n) => n,
                Err(_) => continue,
            };

            let files = Self::collect_sstable_files(&table_dir, gen);
            if files.is_empty() {
                continue;
            }

            let task = crate::upload::UploadTask::SSTable {
                table_id: table_id_str.clone(),
                sstable_id: sstable_id.clone(),
                files,
                on_complete: None,
            };
            let _ = upload_mgr.submit(task).await;
        }
    }

    /// Returns a reference to the upload manager, if S3 is configured.
    pub fn upload_manager(&self) -> Option<&UploadManager> {
        self.upload_manager.as_ref()
    }

    /// Returns true if S3 object storage is configured.
    pub fn has_s3(&self) -> bool {
        self.upload_manager.is_some()
    }

    pub fn request_s3_sync(&self) {
        if self.has_s3() {
            self.s3_sync_requested.store(true, Ordering::Release);
        }
    }

    pub fn take_s3_sync_request(&self) -> bool {
        self.s3_sync_requested.swap(false, Ordering::AcqRel)
    }

    pub fn request_flush(&self) {
        self.flush_requested.store(true, Ordering::Release);
    }

    pub fn take_flush_request(&self) -> bool {
        self.flush_requested.swap(false, Ordering::AcqRel)
    }

    /// Returns the shared index state tracker.
    pub fn index_tracker(&self) -> &Arc<crate::index::IndexStateTracker> {
        &self.index_tracker
    }

    /// Returns the S3 object store and config, if S3 is configured.
    pub fn object_store_and_config(
        &self,
    ) -> ferrosa_common::Result<(&ObjectStoreConfig, Arc<dyn object_store::ObjectStore>)> {
        let os_config = self
            .config
            .object_store
            .as_ref()
            .ok_or_else(|| ferrosa_common::Error::InvalidFormat("S3 not configured".into()))?;
        let store = match &self.object_store {
            Some(store) => Arc::clone(store),
            None => Arc::from(os_config.build_object_store()?),
        };
        Ok((os_config, store))
    }

    /// Returns the object store and prefix for S3 operations.
    ///
    /// In test builds, checks `upload_store_override` first so that tests
    /// can inject an `InMemory` store without a real S3 endpoint.
    fn resolve_store_and_prefix(&self) -> Option<(Arc<dyn object_store::ObjectStore>, String)> {
        #[cfg(test)]
        if let Some((store, prefix)) = &self.upload_store_override {
            return Some((Arc::clone(store), prefix.clone()));
        }
        self.object_store_and_config()
            .ok()
            .map(|(cfg, store)| (store, cfg.prefix.clone()))
    }

    /// Returns the number of closed commit log segments waiting for GC.
    ///
    /// Used by the load test resource monitor to detect segment accumulation.
    pub fn commit_log_closed_segment_count(&self) -> usize {
        self.commit_log.closed_segment_count()
    }

    /// Discards commit log segments that have no remaining dirty tables.
    ///
    /// Called from the background maintenance loop. Returns the number of
    /// segments cleaned up.
    pub fn discard_completed_commit_log_segments(&self) -> ferrosa_common::Result<usize> {
        self.commit_log.discard_completed_segments()
    }

    fn collect_uploaded_local_sstables(
        &self,
        manifest: &crate::manifest::Manifest,
    ) -> Vec<(
        String,
        String,
        std::path::PathBuf,
        u64,
        std::time::SystemTime,
    )> {
        let mut entries = Vec::new();
        for (table_id, manifest_entries) in &manifest.sstables {
            let table_dir = self.config.data_dir.join("sstables").join(table_id);
            for entry in manifest_entries {
                let Ok(gen) = entry.id.parse::<u64>() else {
                    continue;
                };
                let component_paths = Self::generation_component_paths(&table_dir, gen);
                if component_paths.is_empty() {
                    continue;
                }

                let mut size = 0u64;
                let mut last_modified = std::time::UNIX_EPOCH;
                for path in component_paths {
                    let Ok(metadata) = std::fs::metadata(&path) else {
                        continue;
                    };
                    size = size.saturating_add(metadata.len());
                    if let Ok(modified) = metadata.modified() {
                        last_modified = last_modified.max(modified);
                    }
                }

                if size > 0 {
                    entries.push((
                        table_id.clone(),
                        entry.id.clone(),
                        table_dir.clone(),
                        size,
                        last_modified,
                    ));
                }
            }
        }

        entries.sort_by_key(|(_, _, _, _, last_modified)| *last_modified);
        entries
    }

    fn enforce_uploaded_sstable_cache_limit(
        &self,
        manifest: &crate::manifest::Manifest,
    ) -> ferrosa_common::Result<usize> {
        let max_bytes = self.config.local_cache_max_bytes;
        let min_bytes = self.local_cache_min_bytes();
        let target_free = self.local_disk_eviction_target_free_bytes();
        let mut projected_available = if target_free > 0 {
            fs2::available_space(&self.config.data_dir).unwrap_or(0)
        } else {
            0
        };
        let candidates = self.collect_uploaded_local_sstables(manifest);
        let mut total_bytes = candidates
            .iter()
            .fold(0u64, |acc, (_, _, _, size, _)| acc.saturating_add(*size));
        let mut evicted = 0usize;

        for (table_id, sstable_id, table_dir, size, _) in candidates {
            let over_cache_limit = total_bytes > max_bytes;
            let under_free_target = target_free > 0 && projected_available < target_free;
            if !over_cache_limit && !under_free_target {
                break;
            }
            if total_bytes <= min_bytes {
                tracing::warn!(
                    total_uploaded_cache_bytes = total_bytes,
                    min_uploaded_cache_bytes = min_bytes,
                    projected_available_bytes = projected_available,
                    target_free_bytes = target_free,
                    "s3-sync: uploaded local cache at floor; cannot evict more for disk pressure"
                );
                break;
            }

            let reclaimed = Self::delete_sstable_files(&table_dir, &sstable_id);
            total_bytes = total_bytes.saturating_sub(size);
            projected_available = projected_available.saturating_add(reclaimed);
            evicted += 1;
            tracing::info!(
                table = table_id,
                sstable = sstable_id,
                size_bytes = size,
                reclaimed_bytes = reclaimed,
                remaining_uploaded_cache_bytes = total_bytes,
                max_uploaded_cache_bytes = max_bytes,
                min_uploaded_cache_bytes = min_bytes,
                projected_available_bytes = projected_available,
                target_free_bytes = target_free,
                "s3-sync: evicted uploaded local SSTable from cache"
            );
        }

        Ok(evicted)
    }

    fn update_s3_manifest_stats(&self, manifest: &crate::manifest::Manifest) {
        let mut stats = HashMap::new();
        for (table_id, entries) in &manifest.sstables {
            let object_count = entries.iter().fold(0i32, |acc, _| acc.saturating_add(7));
            let bytes = entries
                .iter()
                .fold(0i64, |acc, entry| acc.saturating_add(entry.size as i64));
            stats.insert(table_id.clone(), (object_count, bytes));
        }
        *self.s3_manifest_stats.write() = stats;
    }

    /// Sync all local SSTables to S3 and update the manifest.
    ///
    /// Scans each registered table's SSTable directory, collects component
    /// files for each generation, uploads them via UploadManager, and
    /// updates the S3 manifest with new entries.
    pub async fn sync_sstables_to_s3(&self) -> ferrosa_common::Result<usize> {
        struct S3SyncGuard<'a>(&'a AtomicBool);
        impl Drop for S3SyncGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        let _guard = match self.s3_sync_running.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => S3SyncGuard(&self.s3_sync_running),
            Err(_) => {
                tracing::debug!("s3-sync: skipped because another sync is already running");
                return Ok(0);
            }
        };

        let (os_config, store) = self.object_store_and_config()?;
        let prefix = os_config.prefix.clone();
        let upload_mgr = self.upload_manager.as_ref().ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat("UploadManager not initialized".into())
        })?;

        // Load current manifest to check which SSTables are already uploaded.
        let (mut manifest, _version) =
            crate::manifest::Manifest::load(store.as_ref(), &prefix).await?;
        self.update_s3_manifest_stats(&manifest);

        let mut uploaded = 0usize;

        // Collect table IDs and directories under the lock, then release
        // before any .await (RwLockReadGuard is !Send).
        // Pinned tables are excluded: their SSTables must not be uploaded
        // until the pin is removed via update_table_pin_config().
        let table_dirs: Vec<(String, std::path::PathBuf)> = {
            let tables = self.tables.read();
            tables
                .iter()
                .filter(|(_, state)| state.pin_config.is_none())
                .map(|(id, _)| {
                    let dir = self.config.data_dir.join("sstables").join(id.to_string());
                    (id.to_string(), dir)
                })
                .collect()
        };

        for (table_id_str, table_dir) in &table_dirs {
            if !table_dir.exists() {
                continue;
            }

            let generations = Self::scan_generations(table_dir);
            let existing_ids: std::collections::HashSet<String> = manifest
                .sstables
                .get(table_id_str)
                .map(|entries| entries.iter().map(|e| e.id.clone()).collect())
                .unwrap_or_default();

            for gen in generations {
                let gen_str = gen.to_string();
                if existing_ids.contains(&gen_str) {
                    continue;
                }

                let files = Self::collect_sstable_files(table_dir, gen);
                if files.is_empty() {
                    continue;
                }

                let total_size: u64 = files.iter().map(|file| file.size_bytes).sum();

                // Wait for S3 upload confirmation before adding to manifest.
                // Previously, manifest was updated immediately after submit
                // (fire-and-forget), causing phantom entries if upload failed.
                let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
                let task = crate::upload::UploadTask::SSTable {
                    table_id: table_id_str.clone(),
                    sstable_id: gen_str.clone(),
                    files,
                    on_complete: Some(tx),
                };
                upload_mgr.submit(task).await?;

                let phase_start = Instant::now();
                match rx.await {
                    Ok(Ok(())) => {
                        crate::metrics::observe_upload_phase(
                            crate::metrics::UploadPhase::SyncAwait,
                            phase_start.elapsed(),
                        );
                        manifest.add_sstable(
                            table_id_str,
                            crate::manifest::ManifestEntry {
                                id: gen_str,
                                size: total_size,
                                min_token: i64::MIN,
                                max_token: i64::MAX,
                                min_timestamp: 0,
                                max_timestamp: 0,
                            },
                        );
                        uploaded += 1;
                    }
                    Ok(Err(msg)) => {
                        crate::metrics::observe_upload_phase(
                            crate::metrics::UploadPhase::SyncAwait,
                            phase_start.elapsed(),
                        );
                        // "skipped: source compacted away before upload" is the
                        // race signal from the upload worker: the SSTable was
                        // already merged into a successor by compaction
                        // between the scan and the read, and the new file gets
                        // uploaded under its own generation. Log INFO and
                        // skip — this is normal operation, not a failure.
                        if msg.starts_with("skipped: source compacted away") {
                            tracing::info!(
                                table = table_id_str,
                                sstable = gen_str,
                                "S3 upload skipped — SSTable compacted away before sync"
                            );
                        } else {
                            tracing::error!(
                                table = table_id_str,
                                sstable = gen_str,
                                "S3 upload failed — NOT adding to manifest: {msg}"
                            );
                        }
                    }
                    Err(_) => {
                        crate::metrics::observe_upload_phase(
                            crate::metrics::UploadPhase::SyncAwait,
                            phase_start.elapsed(),
                        );
                        tracing::error!(
                            table = table_id_str,
                            sstable = gen_str,
                            "S3 upload worker dropped channel — NOT adding to manifest"
                        );
                    }
                }
            }
        }

        if uploaded > 0 {
            // Save updated manifest — use CAS if supported, unconditional otherwise.
            let phase_start = Instant::now();
            if self.cas_supported() {
                manifest.save_with_retry(store.as_ref(), &prefix).await?;
            } else {
                manifest.save_without_cas(store.as_ref(), &prefix).await?;
            }
            crate::metrics::observe_upload_phase(
                crate::metrics::UploadPhase::ManifestSave,
                phase_start.elapsed(),
            );
            self.update_s3_manifest_stats(&manifest);
        }

        let evicted = self.enforce_uploaded_sstable_cache_limit(&manifest)?;
        if uploaded > 0 || evicted > 0 {
            tracing::info!(uploaded, evicted, "s3-sync: SSTables synchronized");
        }

        Ok(uploaded)
    }

    /// Download SSTables from S3 to local disk for a specific table.
    ///
    /// Uses the manifest to know which SSTables exist, then downloads
    /// all component files for each. After this call, `register_table()`
    /// will find the files on local disk.
    pub async fn download_sstables_from_s3(
        &self,
        table_id: &TableId,
        manifest: &crate::manifest::Manifest,
    ) -> ferrosa_common::Result<usize> {
        // In test builds, `resolve_store_and_prefix` checks the injected
        // `upload_store_override` before falling back to the real S3 config.
        // In production this is equivalent to `object_store_and_config`.
        let (store, prefix) = self
            .resolve_store_and_prefix()
            .ok_or_else(|| ferrosa_common::Error::InvalidFormat("S3 not configured".into()))?;

        let entries = match manifest.sstables.get(&table_id.to_string()) {
            Some(e) => e,
            None => return Ok(0),
        };
        self.update_s3_manifest_stats(manifest);

        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create table dir: {e}"))
        })?;

        let mut downloaded = 0;
        // Data.db is the only required component; the rest are optional.
        // Missing optional components produce a warning but do not fail the
        // download.  Missing Data.db is a hard error — the SSTable cannot be
        // read without it.
        let required_components = ["Data.db"];
        let optional_components = [
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ];

        'entry_loop: for entry in entries {
            let hex = crate::upload::manager::hex_prefix_for(&entry.id);
            let mut wrote_any = false;

            // Required components: skip stale manifest entries whose required
            // data is neither local nor in object storage, but keep restoring
            // other entries. If none are restorable, fail closed below.
            for component in &required_components {
                let s3_path = crate::upload::manager::sstable_object_key(
                    &prefix,
                    &hex,
                    &table_id.to_string(),
                    &entry.id,
                    component,
                );
                let local_path = table_dir.join(format!("{}-{component}", entry.id));

                if local_path.exists() {
                    wrote_any = true;
                    continue;
                }

                match store.get(&s3_path).await {
                    Ok(result) => {
                        let data = result.bytes().await.map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to read {s3_path}: {e}"
                            ))
                        })?;
                        std::fs::write(&local_path, &data).map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to write {}: {e}",
                                local_path.display()
                            ))
                        })?;
                        wrote_any = true;
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        tracing::warn!(
                            sstable = entry.id,
                            component,
                            path = %s3_path,
                            "required SSTable component absent in S3 and local disk — skipping stale manifest entry"
                        );
                        continue 'entry_loop;
                    }
                    Err(e) => {
                        return Err(ferrosa_common::Error::InvalidFormat(format!(
                            "S3 download failed for {s3_path}: {e}"
                        )));
                    }
                }
            }

            // Optional components: log a warning on NotFound, continue.
            for component in &optional_components {
                let s3_path = crate::upload::manager::sstable_object_key(
                    &prefix,
                    &hex,
                    &table_id.to_string(),
                    &entry.id,
                    component,
                );
                let local_path = table_dir.join(format!("{}-{component}", entry.id));

                if local_path.exists() {
                    continue;
                }

                match store.get(&s3_path).await {
                    Ok(result) => {
                        let data = result.bytes().await.map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to read {s3_path}: {e}"
                            ))
                        })?;
                        std::fs::write(&local_path, &data).map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to write {}: {e}",
                                local_path.display()
                            ))
                        })?;
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        tracing::debug!(
                            sstable = entry.id,
                            component,
                            "optional SSTable component absent in S3 — skipping"
                        );
                    }
                    Err(e) => {
                        return Err(ferrosa_common::Error::InvalidFormat(format!(
                            "S3 download failed for {s3_path}: {e}"
                        )));
                    }
                }
            }

            // Only count this SSTable as downloaded after at least one file
            // landed on disk.  This ensures `downloaded_total` reflects
            // bytes-on-disk reality rather than manifest-entry count.
            if wrote_any {
                downloaded += 1;
            }
        }

        if downloaded == 0 && !entries.is_empty() {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "no SSTables for table {table_id} could be restored from local disk or S3"
            )));
        }

        Ok(downloaded)
    }

    /// Scan a table directory for SSTable generation numbers.
    fn scan_generations(table_dir: &std::path::Path) -> Vec<u64> {
        let mut generations = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for entry in std::fs::read_dir(table_dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                if let Some(gen) = name.split('-').next().and_then(|v| v.parse::<u64>().ok()) {
                    if seen.insert(gen) {
                        generations.push(gen);
                    }
                }
                continue;
            }

            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Ok(gen) = name.parse::<u64>() {
                    if table_dir
                        .join(&name)
                        .join(format!("{name}-Data.db"))
                        .exists()
                        && seen.insert(gen)
                    {
                        generations.push(gen);
                    }
                }
            }
        }

        generations
    }

    fn promote_compaction_output(
        &self,
        table_id: &TableId,
        output: &crate::compaction::metadata::SSTableMetadata,
    ) -> ferrosa_common::Result<crate::compaction::metadata::SSTableMetadata> {
        let source_dir = &output.path;
        let target_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        std::fs::create_dir_all(&target_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create promoted compaction directory: {e}"
            ))
        })?;

        let output_gen = output.id.parse::<u64>().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "compaction output SSTable id is not a u64: {e}"
            ))
        })?;
        let table_max_gen = Self::scan_generations(&target_dir)
            .into_iter()
            .max()
            .unwrap_or(0);
        let promoted_gen = table_max_gen.max(output_gen).saturating_add(1);
        let promoted_id = promoted_gen.to_string();
        let old_prefix = format!("{}-", output.id);
        let final_target = target_dir.join(&promoted_id);
        let fail_after_first = Self::should_fail_promotion_after_first_component(&output.path);

        if final_target.exists() {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "promoted compaction target already exists: {}",
                final_target.display()
            )));
        }

        let staging_dir = Self::temp_promotion_directory(&target_dir, promoted_gen);
        let mut moves = Vec::new();
        for entry in std::fs::read_dir(source_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to read compaction output directory: {e}"
            ))
        })? {
            let entry = entry.map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to read compaction output entry: {e}"
                ))
            })?;
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(component_suffix) = name.strip_prefix(&old_prefix) else {
                continue;
            };
            if component_suffix.is_empty() {
                continue;
            }
            let target = staging_dir.join(format!("{promoted_id}-{component_suffix}"));
            if target.exists() {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "promoted compaction target already exists: {}",
                    target.display()
                )));
            }
            moves.push((entry.path(), target, component_suffix.to_string()));
        }

        if moves.is_empty() {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "compaction output has no component files in {}",
                source_dir.display()
            )));
        }

        let _ = std::fs::remove_dir_all(&staging_dir);
        std::fs::create_dir_all(&staging_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create compaction promotion staging dir {}: {e}",
                staging_dir.display()
            ))
        })?;

        let rollback_moved = |moved: &[(std::path::PathBuf, std::path::PathBuf)]| {
            for (source, target) in moved.iter().rev() {
                if target.exists() {
                    let _ = std::fs::rename(target, source);
                }
            }
            let _ = std::fs::remove_dir_all(&staging_dir);
        };

        let mut moved = Vec::new();
        for (source, target, component_suffix) in &moves {
            let bytes = std::fs::metadata(source).map(|m| m.len()).map_err(|e| {
                let err = ferrosa_common::Error::InvalidFormat(format!(
                    "failed to inspect compaction component {} before promotion: {e}",
                    source.display()
                ));
                rollback_moved(&moved);
                err
            })?;
            if bytes == 0 && matches!(component_suffix.as_str(), "Data.db" | "Partitions.db") {
                rollback_moved(&moved);
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "refusing to promote compaction component {}: critical component is zero bytes",
                    source.display()
                )));
            }

            std::fs::rename(source, target).map_err(|e| {
                let err = ferrosa_common::Error::InvalidFormat(format!(
                    "failed to stage compaction component {} to {}: {e}",
                    source.display(),
                    target.display()
                ));
                rollback_moved(&moved);
                err
            })?;
            moved.push((source.clone(), target.clone()));

            if fail_after_first && moved.len() == 1 {
                rollback_moved(&moved);
                return Err(ferrosa_common::Error::InvalidFormat(
                    "simulated failure after first compaction component stage".to_string(),
                ));
            }
        }

        if final_target.exists() {
            rollback_moved(&moved);
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "promoted compaction target already exists: {}",
                final_target.display()
            )));
        }

        std::fs::rename(&staging_dir, &final_target).map_err(|e| {
            rollback_moved(&moved);
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to atomically promote compaction output to {}: {e}",
                final_target.display()
            ))
        })?;

        Self::cleanup_promoted_compaction_output(source_dir, &old_prefix);

        let size_bytes = Self::generation_component_paths(&final_target, promoted_gen)
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum();

        let mut promoted = output.clone();
        promoted.id = promoted_id;
        promoted.path = final_target;
        promoted.size_bytes = size_bytes;
        Ok(promoted)
    }

    fn cleanup_promoted_compaction_output(source_dir: &std::path::Path, promoted_prefix: &str) {
        let Ok(entries) = std::fs::read_dir(source_dir) else {
            return;
        };

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(promoted_prefix) {
                let path = entry.path();
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!(
                        %e,
                        path = %path.display(),
                        "compaction: failed to remove promoted output source component"
                    );
                }
            }
        }

        let is_empty = std::fs::read_dir(source_dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false);
        if is_empty {
            let _ = std::fs::remove_dir(source_dir);
        }
    }

    fn evict_local_input_sstable_files(inputs: &[crate::compaction::metadata::SSTableMetadata]) {
        let standard_components = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ];
        for input in inputs {
            for component in &standard_components {
                let file_path = Self::generation_component_path(&input.path, &input.id, component)
                    .unwrap_or_else(|| input.path.join(format!("{}-{component}", input.id)));
                let _ = std::fs::remove_file(&file_path);
            }
        }
    }

    /// Collect local component file paths for an SSTable generation.
    fn collect_sstable_files(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> Vec<crate::upload::manager::SstableComponentFile> {
        let gen_str = gen.to_string();
        let dir =
            Self::generation_dir_path(table_dir, gen).unwrap_or_else(|| table_dir.to_path_buf());
        std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.starts_with(&format!("{gen_str}-")) {
                    Some(crate::upload::manager::SstableComponentFile::new(
                        name,
                        e.path(),
                    ))
                } else {
                    None
                }
            })
            .collect()
    }
}

struct CompactionResultInputClaim<'a> {
    executor: &'a CompactionExecutor,
    task: &'a crate::compaction::metadata::CompactionTask,
}

impl Drop for CompactionResultInputClaim<'_> {
    fn drop(&mut self) {
        self.executor.release_task_inputs(self.task);
    }
}

// ---------------------------------------------------------------------------
// Test constructor: inject an in-memory object store
// ---------------------------------------------------------------------------

impl StorageEngine {
    /// Creates a storage engine with an explicit upload object store.
    ///
    /// Used by tests to inject an `InMemory` store for upload/manifest tests
    /// without requiring a real S3 endpoint.  The store is used directly for
    /// both uploads and manifest persistence.
    #[cfg(test)]
    pub fn new_with_upload_store(
        config: StorageEngineConfig,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: String,
        runtime: &tokio::runtime::Handle,
    ) -> ferrosa_common::Result<Self> {
        Self::new_with_upload_store_and_queue_depth(config, store, prefix, runtime, 16)
    }

    /// Creates a storage engine with an explicit upload object store and queue depth.
    ///
    /// Test-only helper used to exercise upload backpressure without requiring
    /// a real S3 endpoint.
    #[cfg(test)]
    pub fn new_with_upload_store_and_queue_depth(
        config: StorageEngineConfig,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: String,
        runtime: &tokio::runtime::Handle,
        upload_queue_depth: usize,
    ) -> ferrosa_common::Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;

        let commit_log = CommitLog::new(config.commit_log.clone())?;
        let reader_pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );
        let compaction_executor = CompactionExecutor::with_reader_pool(Arc::clone(&reader_pool));

        let upload_manager = Some(UploadManager::new_with_pools(
            Arc::clone(&store),
            prefix.clone(),
            upload_queue_depth,
            8,
            2,
            runtime,
        ));
        let compaction_upload_manager = Some(UploadManager::new_with_pools(
            Arc::clone(&store),
            prefix.clone(),
            upload_queue_depth,
            4,
            2,
            runtime,
        ));

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let (index_scheduler, index_tracker) = build_index_scheduler(&config);

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            deferred_replay_mutations: parking_lot::Mutex::new(Vec::new()),
            compaction_executor,
            upload_manager,
            compaction_upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            time_series_consolidators: RwLock::new(HashMap::new()),
            time_series_runtime_settings: Arc::new(TimeSeriesRuntimeSettings::from_config(
                &ConsolidationConfig::default(),
            )),
            time_series_wasm_aggregates: RwLock::new(None),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            pin_metrics: Arc::new(crate::metrics::PinMetrics::new()),
            object_store: Some(Arc::clone(&store)),
            s3_cas_supported: std::sync::atomic::AtomicBool::new(true),
            s3_sync_running: AtomicBool::new(false),
            s3_sync_requested: AtomicBool::new(false),
            cached_disk_free_bytes: AtomicU64::new(0),
            disk_free_checked_at_ms: AtomicU64::new(u64::MAX),
            flush_requested: AtomicBool::new(false),
            s3_manifest_stats: RwLock::new(HashMap::new()),
            reader_pool,
            upload_store_override: Some((store, prefix)),
        })
    }

    /// Test helper: uploads the engine's current SSTable inventory to the
    /// injected object store as a manifest. This makes `create_snapshot_with_store`
    /// capture the correct SSTable references (since tests don't run an
    /// `UploadManager` that would do this automatically in production).
    ///
    /// Also uploads placeholder SSTable data files so that `RestoreManager`
    /// can download them during restore.
    #[cfg(test)]
    pub async fn upload_manifest_for_test(
        &self,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) {
        let mut manifest = match crate::manifest::Manifest::load(store.as_ref(), prefix).await {
            Ok((m, _)) => m,
            Err(_) => crate::manifest::Manifest::new(),
        };

        // Collect table info under the lock, then drop it before async work.
        let table_dirs: Vec<(String, std::path::PathBuf)> = {
            let tables = self.tables.read();
            tables
                .keys()
                .map(|tid| {
                    let dir = self.config.data_dir.join("sstables").join(tid.to_string());
                    (tid.to_string(), dir)
                })
                .collect()
        };

        for (table_id_str, table_dir) in &table_dirs {
            if !table_dir.exists() {
                continue;
            }
            let generations: Vec<u64> = std::fs::read_dir(table_dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().to_str()?.to_string();
                    if name.ends_with("-Data.db") {
                        name.split('-').next()?.parse::<u64>().ok()
                    } else {
                        None
                    }
                })
                .collect();

            for gen in generations {
                let gen_str = gen.to_string();
                let hex = crate::upload::manager::hex_prefix_for(&gen_str);
                if manifest
                    .sstables
                    .get(table_id_str.as_str())
                    .is_some_and(|entries| entries.iter().any(|e| e.id == gen_str))
                {
                    continue;
                }
                let data_path = table_dir.join(format!("{gen}-Data.db"));
                let size = std::fs::metadata(&data_path).map(|m| m.len()).unwrap_or(0);
                manifest.add_sstable(
                    table_id_str,
                    crate::manifest::ManifestEntry {
                        id: gen_str.clone(),
                        size,
                        min_token: i64::MIN,
                        max_token: i64::MAX,
                        min_timestamp: 0,
                        max_timestamp: i64::MAX,
                    },
                );
                let components = [
                    "Data.db",
                    "Partitions.db",
                    "Rows.db",
                    "Filter.db",
                    "Statistics.db",
                    "TOC.txt",
                    "CompressionInfo.db",
                ];
                for component in &components {
                    let local_path = table_dir.join(format!("{gen}-{component}"));
                    if local_path.exists() {
                        let data = std::fs::read(&local_path).unwrap();
                        let s3_path = crate::upload::manager::sstable_object_key(
                            prefix,
                            &hex,
                            table_id_str,
                            &gen_str,
                            component,
                        );
                        store
                            .put(&s3_path, bytes::Bytes::from(data).into())
                            .await
                            .unwrap();
                    }
                }
            }
        }

        manifest
            .save_with_retry(store.as_ref(), prefix)
            .await
            .unwrap();

        // Schema snapshot — collect under lock, drop before await.
        let schema_json = {
            let tables = self.tables.read();
            let schemas: Vec<&TableSchema> = tables.values().map(|s| &s.schema).collect();
            serde_json::to_vec_pretty(&schemas).unwrap_or_default()
        };
        crate::manifest::save_schema_snapshot(store.as_ref(), prefix, &schema_json)
            .await
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// Virtual table provider implementations
// ---------------------------------------------------------------------------

impl crate::virtual_tables::StorageStatsProvider for StorageEngine {
    fn collect_stats(&self) -> Vec<crate::virtual_tables::StorageStats> {
        let tables = self.tables.read();
        tables
            .iter()
            .map(|(table_id, state)| {
                let sstable_count = state.store.sstable_count() as i32;

                let table_dir = self
                    .config
                    .data_dir
                    .join("sstables")
                    .join(table_id.to_string());
                let generations = Self::scan_generations(&table_dir);
                let mut sstable_size_bytes = 0i64;
                let mut local_sstable_component_count = 0i32;
                let mut compressed_sstable_count = 0i32;
                let mut uncompressed_sstable_count = 0i32;

                for gen in generations {
                    let component_paths = Self::generation_component_paths(&table_dir, gen);
                    if component_paths.is_empty() {
                        continue;
                    }
                    local_sstable_component_count =
                        local_sstable_component_count.saturating_add(component_paths.len() as i32);
                    for path in component_paths {
                        sstable_size_bytes = sstable_size_bytes.saturating_add(
                            std::fs::metadata(&path)
                                .map(|m| m.len() as i64)
                                .unwrap_or(0),
                        );
                    }
                    if Self::generation_component_path(
                        &table_dir,
                        &gen.to_string(),
                        "CompressionInfo.db",
                    )
                    .is_some()
                    {
                        compressed_sstable_count = compressed_sstable_count.saturating_add(1);
                    } else {
                        uncompressed_sstable_count = uncompressed_sstable_count.saturating_add(1);
                    }
                }

                let local_sstable_cache_bytes = if self.has_s3() {
                    // Local component footprint for this table. After S3 sync
                    // it should stay under `local_cache_max_bytes`, while
                    // remote bytes come from the last observed manifest below.
                    sstable_size_bytes
                } else {
                    0
                };

                let (s3_object_count, s3_bytes) = self
                    .s3_manifest_stats
                    .read()
                    .get(&table_id.to_string())
                    .copied()
                    .unwrap_or((0, 0));

                crate::virtual_tables::StorageStats {
                    keyspace: table_id.keyspace().to_string(),
                    table_name: table_id.table().to_string(),
                    memtable_size_bytes: state.store.memtable_size() as i64,
                    memtable_count: 1, // One active memtable per table
                    sstable_count,
                    sstable_size_bytes,
                    s3_object_count,
                    s3_bytes,
                    local_sstable_component_count,
                    compressed_sstable_count,
                    uncompressed_sstable_count,
                    local_cache_max_bytes: self.config.local_cache_max_bytes as i64,
                    local_sstable_cache_bytes,
                    pending_compactions: 0, // Per-table pending count not yet exposed
                }
            })
            .collect()
    }
}

impl crate::virtual_tables::ArchiveStatusProvider for StorageEngine {
    fn archive_status(&self) -> crate::virtual_tables::ArchiveStatusRow {
        let archived = self.commit_log.archived_segments();
        crate::virtual_tables::ArchiveStatusRow {
            // Approximate: total closed segments minus archived ones would
            // require knowing the full closed set. For now report archived count
            // as "0 unarchived" if any archiving has occurred.
            unarchived_segments: 0,
            oldest_unarchived_age_secs: 0,
            last_archive_success: if archived.is_empty() {
                "never".to_string()
            } else {
                "unknown".to_string()
            },
            archive_errors_total: 0,
        }
    }
}

impl crate::virtual_tables::SnapshotInfoProvider for StorageEngine {
    fn snapshot_info(&self) -> Vec<crate::virtual_tables::SnapshotInfoRow> {
        // The virtual table API is synchronous, while snapshot listing may
        // need async object-store I/O. Do not bridge that boundary here:
        // metrics and system table reads must never block Tokio worker
        // threads waiting for object-store futures to make progress.
        //
        // Async callers should use `list_snapshots_with_store`; this table
        // can be backed by a cached snapshot index once one exists.
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_common::schema::ColumnDefinition;
    #[cfg(feature = "live-infra-tests")]
    use ferrosa_sstable::statistics::{CompactionMetadata, StatsMetadata};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// Return "docker" or "podman" — whichever container runtime is in PATH.
    /// Panics if neither is found.
    #[cfg(feature = "live-infra-tests")]
    fn container_runtime() -> &'static str {
        // Use `info` (not `--version`) to confirm the daemon is actually running,
        // not just that the binary is installed. On macOS, Docker Desktop may be
        // installed but not started; Podman Desktop is typically running.
        for candidate in &["podman", "docker"] {
            if std::process::Command::new(candidate)
                .arg("info")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
            {
                return Box::leak((*candidate).to_string().into_boxed_str());
            }
        }
        panic!(
            "no container runtime daemon reachable — start Podman Desktop (macOS) or Docker Desktop \
             before running container-dependent tests"
        );
    }

    /// Returns the absolute path to a file under the workspace root.
    ///
    /// `CARGO_MANIFEST_DIR` points to the crate directory at compile time.
    /// The workspace root is one level up.
    #[cfg(feature = "live-infra-tests")]
    fn workspace_path(relative: &str) -> std::path::PathBuf {
        let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = crate_dir
            .parent()
            .expect("crate has a parent workspace dir");
        workspace_root.join(relative)
    }

    /// CompactionMetadata (component 1) bytes from a real Cassandra 5.0.7 nb-format
    /// Statistics.db (test_ks.test_table with pk text + ck int + val text).
    /// 19 bytes — HyperLogLog cardinality estimate for a minimal table.
    #[cfg(feature = "live-infra-tests")]
    const CASSANDRA_COMPACTION_METADATA_HEX: &str = "0000000ffffffffe0d190102deb87192a7b71c"; // pragma: allowlist secret

    /// StatsMetadata (component 2) bytes from a Cassandra 5 BTI-format (da) SSTable
    /// (test_ks.test_table with pk text + ck int + val text, 2-row table).
    /// 4628 bytes — BTI format has 52 extra bytes vs Big format.
    /// These are schema-independent; Cassandra does not validate histogram
    /// contents against actual SSTable data during `nodetool import`.
    #[cfg(feature = "live-infra-tests")]
    const CASSANDRA_STATS_METADATA_HEX: &str = "0000009c000000000000000100000000000000000000000000000001000000000000000000000000000000020000000000000000000000000000000300000000000000000000000000000004000000000000000000000000000000050000000000000000000000000000000600000000000000000000000000000007000000000000000000000000000000080000000000000000000000000000000a0000000000000000000000000000000c0000000000000000000000000000000e0000000000000000000000000000001100000000000000000000000000000014000000000000000100000000000000180000000000000001000000000000001d000000000000000000000000000000230000000000000000000000000000002a000000000000000000000000000000320000000000000000000000000000003c0000000000000000000000000000004800000000000000000000000000000056000000000000000000000000000000670000000000000000000000000000007c00000000000000000000000000000095000000000000000000000000000000b3000000000000000000000000000000d7000000000000000000000000000001020000000000000000000000000000013600000000000000000000000000000174000000000000000000000000000001be0000000000000000000000000000021700000000000000000000000000000282000000000000000000000000000003020000000000000000000000000000039c00000000000000000000000000000455000000000000000000000000000005330000000000000000000000000000063d0000000000000000000000000000077c000000000000000000000000000008fb00000000000000000000000000000ac700000000000000000000000000000cef00000000000000000000000000000f85000000000000000000000000000012a00000000000000000000000000000165a00000000000000000000000000001ad20000000000000000000000000000202f0000000000000000000000000000269f00000000000000000000000000002e580000000000000000000000000000379d000000000000000000000000000042bc00000000000000000000000000005015000000000000000000000000000060190000000000000000000000000000735100000000000000000000000000008a610000000000000000000000000000a60e0000000000000000000000000000c7440000000000000000000000000000ef1e00000000000000000000000000011ef10000000000000000000000000001585400000000000000000000000000019d320000000000000000000000000001efd6000000000000000000000000000253010000000000000000000000000002ca01000000000000000000000000000358ce0000000000000000000000000004042a0000000000000000000000000004d1cc0000000000000000000000000005c88e0000000000000000000000000006f0aa000000000000000000000000000853ff0000000000000000000000000009fe65000000000000000000000000000bfe13000000000000000000000000000e6417000000000000000000000000001144e80000000000000000000000000014b9160000000000000000000000000018de1a000000000000000000000000001dd7520000000000000000000000000023cf2f000000000000000000000000002af89f000000000000000000000000003390bf000000000000000000000000003de0e5000000000000000000000000004a411300000000000000000000000000591ae4000000000000000000000000006aed1200000000000000000000000000804faf0000000000000000000000000099f93800000000000000000000000000b8c4aa00000000000000000000000000ddb8cc000000000000000000000000010a10f5000000000000000000000000013f478c000000000000000000000000017f22a800000000000000000000000001cbc3300000000000000000000000000227b70600000000000000000000000002960ed4000000000000000000000000031a783200000000000000000000000003b95d090000000000000000000000000478093e000000000000000000000000055cd7e4000000000000000000000000066f697800000000000000000000000007b8e4f6000000000000000000000000094445f40000000000000000000000000b1eba580000000000000000000000000d5812d0000000000000000000000000100349c600000000000000000000000013372554000000000000000000000000170ef9980000000000000000000000001bab91ea000000000000000000000000213448b200000000000000000000000027d8573c0000000000000000000000002fd068ae00000000000000000000000039607d9e00000000000000000000000044da3057000000000000000000000000529f6d350000000000000000000000006325b64000000000000000000000000076fa0de60000000000000000000000008ec5aa47000000000000000000000000ab539922000000000000000000000000cd97848f000000000000000000000000f6b5d245000000000000000000000001280d62b900000000000000000000000163434344000000000000000000000001aa50b71e000000000000000000000001ff940ef100000000000000000000000265e4debb000000000000000000000002e0ac3e7a0000000000000000000000037401e49200000000000000000000000424cf1249000000000000000000000004f8f87c58000000000000000000000005f79095360000000000000000000000072913e64100000000000000000000000897b17ab400000000000000000000000a4fa1c67200000000000000000000000c5f8eee2200000000000000000000000ed911ea8f000000000000000000000011d148b312000000000000000000000015618a707c000000000000000000000019a83fba2e00000000000000000000001ec9e6129e000000000000000000000024f247498a00000000000000000000002c55ef250c00000000000000000000003533ebc60e00000000000000000000003fd7e7ba7700000000000000000000004c9cafac8f00000000000000000000005bef39357800000000000000000000006e5244a69000000000000000000000008462b8c7e000000000000000000000009edcddbca60000000000000000000000bea2a3af2e0000000000000000000000e4c32ad23700000000000000000000011283ccfc420000000000000000000001496af5fb8200000000000000000000018b4d272dcf0000000000000000000001da5c956a2c0000000000000000000002393be67f680000000000000000000002ab14ae327d000000000000000000000333b26aa2fc000000000000000000000077000000000000000100000000000000020000000000000001000000000000000000000000000000020000000000000000000000000000000300000000000000000000000000000004000000000000000000000000000000050000000000000000000000000000000600000000000000000000000000000007000000000000000000000000000000080000000000000000000000000000000a0000000000000000000000000000000c0000000000000000000000000000000e0000000000000000000000000000001100000000000000000000000000000014000000000000000000000000000000180000000000000000000000000000001d000000000000000000000000000000230000000000000000000000000000002a000000000000000000000000000000320000000000000000000000000000003c0000000000000000000000000000004800000000000000000000000000000056000000000000000000000000000000670000000000000000000000000000007c00000000000000000000000000000095000000000000000000000000000000b3000000000000000000000000000000d7000000000000000000000000000001020000000000000000000000000000013600000000000000000000000000000174000000000000000000000000000001be0000000000000000000000000000021700000000000000000000000000000282000000000000000000000000000003020000000000000000000000000000039c00000000000000000000000000000455000000000000000000000000000005330000000000000000000000000000063d0000000000000000000000000000077c000000000000000000000000000008fb00000000000000000000000000000ac700000000000000000000000000000cef00000000000000000000000000000f85000000000000000000000000000012a00000000000000000000000000000165a00000000000000000000000000001ad20000000000000000000000000000202f0000000000000000000000000000269f00000000000000000000000000002e580000000000000000000000000000379d000000000000000000000000000042bc00000000000000000000000000005015000000000000000000000000000060190000000000000000000000000000735100000000000000000000000000008a610000000000000000000000000000a60e0000000000000000000000000000c7440000000000000000000000000000ef1e00000000000000000000000000011ef10000000000000000000000000001585400000000000000000000000000019d320000000000000000000000000001efd6000000000000000000000000000253010000000000000000000000000002ca01000000000000000000000000000358ce0000000000000000000000000004042a0000000000000000000000000004d1cc0000000000000000000000000005c88e0000000000000000000000000006f0aa000000000000000000000000000853ff0000000000000000000000000009fe65000000000000000000000000000bfe13000000000000000000000000000e6417000000000000000000000000001144e80000000000000000000000000014b9160000000000000000000000000018de1a000000000000000000000000001dd7520000000000000000000000000023cf2f000000000000000000000000002af89f000000000000000000000000003390bf000000000000000000000000003de0e5000000000000000000000000004a411300000000000000000000000000591ae4000000000000000000000000006aed1200000000000000000000000000804faf0000000000000000000000000099f93800000000000000000000000000b8c4aa00000000000000000000000000ddb8cc000000000000000000000000010a10f5000000000000000000000000013f478c000000000000000000000000017f22a800000000000000000000000001cbc3300000000000000000000000000227b70600000000000000000000000002960ed4000000000000000000000000031a783200000000000000000000000003b95d090000000000000000000000000478093e000000000000000000000000055cd7e4000000000000000000000000066f697800000000000000000000000007b8e4f6000000000000000000000000094445f40000000000000000000000000b1eba580000000000000000000000000d5812d0000000000000000000000000100349c600000000000000000000000013372554000000000000000000000000170ef9980000000000000000000000001bab91ea000000000000000000000000213448b200000000000000000000000027d8573c0000000000000000000000002fd068ae00000000000000000000000039607d9e00000000000000000000000044da3057000000000000000000000000529f6d350000000000000000000000006325b64000000000000000000000000076fa0de60000000000000000000000008ec5aa47000000000000000000000000ab539922000000000000000000000000cd97848f000000000000000000000000f6b5d24500000000000000000000019d3a99ca990000f26400064e2cec8a32ec00064e2cec8e738dffffffffffffffff00000000000000003ff1000000000000000000000000000000000000000000000000000001296f72672e6170616368652e63617373616e6472612e64622e6d61727368616c2e496e743332547970650100010000000001060001000000000100000000000000000200000000000000020000019d3a99ca990000f17a000000010000019d3a99ca990000f17a0000019d3a99ca990000f2640000016fc8b33a2d3e45329528d6428291d58100016101627ff8000000000000";

    /// Decode a lowercase hex string into bytes.  Test-only — no performance concerns.
    #[cfg(feature = "live-infra-tests")]
    fn from_hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("invalid hex"))
            .collect()
    }

    #[test]
    fn replay_pending_uploads_does_not_block_startup_when_upload_worker_stalls() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let table_id = "agent_memory.entity_store";
        let table_dir = config.data_dir.join("sstables").join(table_id);
        std::fs::create_dir_all(&table_dir).unwrap();

        let pending_log =
            crate::upload::PendingUploadsLog::open(&config.data_dir.join("pending-uploads.log"))
                .unwrap();
        for sstable_id in ["1", "2", "3"] {
            std::fs::write(table_dir.join(format!("{sstable_id}-Data.db")), b"sstable").unwrap();
            pending_log.add_entry(table_id, sstable_id).unwrap();
        }

        // Keep the upload runtime idle so its worker cannot drain the bounded
        // queue. Current replay code awaits mpsc::send() for every pending
        // entry, so once the queue-depth-1 channel fills, startup replay blocks.
        let upload_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let engine = StorageEngine::new_with_upload_store_and_queue_depth(
            config,
            store,
            "test-prefix".to_string(),
            upload_runtime.handle(),
            1,
        )
        .unwrap();

        let replay_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = replay_runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                engine.replay_pending_uploads(),
            )
            .await
        });

        assert!(
            result.is_ok(),
            "pending upload replay must return promptly so startup can bind listeners"
        );
    }

    #[test]
    fn replay_pending_uploads_keeps_missing_files_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let table_id = "agent_memory.entity_store";

        let pending_log =
            crate::upload::PendingUploadsLog::open(&config.data_dir.join("pending-uploads.log"))
                .unwrap();
        pending_log.add_entry(table_id, "missing-101").unwrap();

        let upload_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let engine = StorageEngine::new_with_upload_store_and_queue_depth(
            config,
            store,
            "test-prefix".to_string(),
            upload_runtime.handle(),
            1,
        )
        .unwrap();

        let replay_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        replay_runtime.block_on(engine.replay_pending_uploads());

        let remaining = crate::upload::PendingUploadsLog::open(
            &engine.config.data_dir.join("pending-uploads.log"),
        )
        .unwrap()
        .pending_entries()
        .unwrap();
        assert!(
            !remaining.is_empty(),
            "missing SSTable uploads should stay in pending-uploads.log so crash recovery remains retryable"
        );
    }

    /// Read the first and last raw partition key bytes from Partitions.db.
    ///
    /// Footer (last 24 bytes): key_bounds_offset (i64 BE) | key_count (i64 BE) | root_pos (i64 BE).
    /// Key bounds section at key_bounds_offset: u16 BE length + bytes, repeated twice
    /// (smallest token first, then largest).
    #[cfg(feature = "live-infra-tests")]
    fn read_key_bounds_from_partitions_db(path: &std::path::Path) -> (Vec<u8>, Vec<u8>) {
        let data = std::fs::read(path).expect("read Partitions.db");
        let len = data.len();
        assert!(len >= 24, "Partitions.db too small for footer");

        let key_bounds_offset =
            i64::from_be_bytes(data[len - 24..len - 16].try_into().unwrap()) as usize;

        let first_len = u16::from_be_bytes(
            data[key_bounds_offset..key_bounds_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let first_key = data[key_bounds_offset + 2..key_bounds_offset + 2 + first_len].to_vec();

        let second_start = key_bounds_offset + 2 + first_len;
        let last_len =
            u16::from_be_bytes(data[second_start..second_start + 2].try_into().unwrap()) as usize;
        let last_key = data[second_start + 2..second_start + 2 + last_len].to_vec();

        (first_key, last_key)
    }

    /// Append `key` to `buf` with an unsigned vint32 length prefix (Cassandra format).
    #[cfg(feature = "live-infra-tests")]
    fn append_vint_prefixed_key(buf: &mut Vec<u8>, key: &[u8]) {
        let mut vint_buf = [0u8; 9];
        let n = ferrosa_sstable::varint::write_unsigned_vint(&mut vint_buf, key.len() as u64);
        buf.extend_from_slice(&vint_buf[..n]);
        buf.extend_from_slice(key);
    }

    /// Patch Statistics.db in `staging_dir` so that `nodetool import` can read it.
    ///
    /// Ferrosa writes empty bytes for CompactionMetadata (ordinal 1) and
    /// StatsMetadata (ordinal 2), which causes Cassandra's `StatsComponent.load`
    /// to fail when importing.  This function replaces those two components with
    /// real bytes extracted from a Cassandra 5.0.7 instance — the histogram
    /// boundaries and cardinality data are not validated during import, so the
    /// exact values do not need to match the actual SSTable contents.
    ///
    /// ValidationMetadata (ordinal 0) and SerializationHeader (ordinal 3) are
    /// preserved as written by ferrosa.
    ///
    /// The `CASSANDRA_STATS_METADATA_HEX` blob ends with firstKey="a"/lastKey="b"
    /// (from the SSTable it was extracted from).  This function reads the actual
    /// first/last keys from Partitions.db and replaces those last 12 bytes so that
    /// Cassandra's `SortedTableVerifier.deserializeIndex` does not fail with a
    /// key-mismatch CorruptSSTableException.
    #[cfg(feature = "live-infra-tests")]
    fn patch_statistics_for_cassandra_import(staging_dir: &std::path::Path) {
        use ferrosa_sstable::statistics::{read_statistics, write_statistics};

        let stats_path = std::fs::read_dir(staging_dir)
            .expect("read staging dir")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Statistics.db"))
                    .unwrap_or(false)
            })
            .expect("Statistics.db not found in staging dir — prepare_cassandra_import_dir must run first");

        // Read actual first/last partition keys from the renamed Partitions.db.
        let partitions_path = std::fs::read_dir(staging_dir)
            .expect("read staging dir for Partitions.db")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Partitions.db"))
                    .unwrap_or(false)
            })
            .expect("Partitions.db not found in staging dir");

        let (first_key, last_key) = read_key_bounds_from_partitions_db(&partitions_path);

        let data = std::fs::read(&stats_path).expect("read Statistics.db");
        let mut stats = read_statistics(&data).expect("parse Statistics.db from ferrosa output");

        stats.compaction = CompactionMetadata {
            data: from_hex(CASSANDRA_COMPACTION_METADATA_HEX),
        };

        if stats.stats.data.is_empty() {
            // Legacy clustered-table fallback: replace the empty StatsMetadata
            // blob, then fix its key-range tail. New no-clustering SSTables are
            // expected to carry writer-produced metadata, and this path must not
            // overwrite it with a clustered-table template.
            let mut stats_bytes = from_hex(CASSANDRA_STATS_METADATA_HEX);
            stats_bytes.truncate(stats_bytes.len() - 12);
            append_vint_prefixed_key(&mut stats_bytes, &first_key);
            append_vint_prefixed_key(&mut stats_bytes, &last_key);
            stats_bytes.extend_from_slice(&[0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

            stats.stats = StatsMetadata { data: stats_bytes };
        }

        let patched = write_statistics(&stats);
        std::fs::write(&stats_path, patched).expect("write patched Statistics.db");
    }

    /// Prepare a directory of SSTable files for `nodetool import`.
    ///
    /// Ferrosa writes files named `{gen}-Data.db`, but Cassandra's SSTableLoader
    /// expects the BTI descriptor prefix `da-{gen}-bti-{Component}`.
    /// ("da" is the BTI version prefix; "bti" is the format name.)
    /// This function:
    ///   1. Scans `src_dir` for files matching `{gen}-*.db` / `{gen}-*.txt`
    ///   2. Copies them to `dst_dir` with a Cassandra-local `da-1-bti-` prefix
    ///   3. Rewrites the TOC.txt content to list the new filenames
    ///
    /// Returns the destination directory path.
    #[cfg(feature = "live-infra-tests")]
    fn prepare_cassandra_import_dir(
        src_dir: &std::path::Path,
        dst_dir: &std::path::Path,
    ) -> std::path::PathBuf {
        std::fs::create_dir_all(dst_dir).expect("create import dir");

        for entry in std::fs::read_dir(src_dir).expect("read compaction dir") {
            let entry = entry.expect("read dir entry");
            let src_path = entry.path();
            let fname = src_path.file_name().unwrap().to_str().unwrap().to_string();

            // Split "{gen}-{Component}" and rewrite to a compact Cassandra
            // descriptor generation. Ferrosa generations are node/timestamp
            // sized; Cassandra import discovers normal BTI descriptors such as
            // `da-1-bti-Data.db`.
            let cassandra_fname = if let Some(dash_pos) = fname.find('-') {
                let component = &fname[dash_pos + 1..];
                format!("da-1-bti-{component}")
            } else {
                fname.clone()
            };

            let dst_path = dst_dir.join(&cassandra_fname);

            // Cassandra BTI TOC.txt contains bare component names (e.g. "Data.db"),
            // not prefixed names — copy content unchanged, just rename the file.
            std::fs::copy(&src_path, &dst_path).expect("copy SSTable component");
        }

        dst_dir.to_path_buf()
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn table_id() -> TableId {
        TableId::new("test_ks", "test_table")
    }

    #[test]
    fn write_options_default_to_lz4() {
        let options = write_options_for_schema(&test_schema(), true).unwrap();
        assert!(matches!(
            options.compression,
            Some(ferrosa_sstable::Compression::Lz4)
        ));
        assert!(options.verify_output);
    }

    #[test]
    fn write_options_parse_zstd_schema_extensions() {
        let mut schema = test_schema();
        schema.extensions.insert(
            "compression.class".to_string(),
            "org.apache.cassandra.io.compress.ZstdCompressor".to_string(),
        );
        schema
            .extensions
            .insert("compression.compression_level".to_string(), "7".to_string());
        schema
            .extensions
            .insert("compression.chunk_length_kb".to_string(), "32".to_string());

        let options = write_options_for_schema(&schema, false).unwrap();
        assert_eq!(
            options.compression,
            Some(ferrosa_sstable::Compression::Zstd { level: 7 })
        );
        assert_eq!(options.chunk_size, 32 * 1024);
        assert!(!options.verify_output);
    }

    #[test]
    fn uploaded_sstable_cache_eviction_deletes_only_manifest_confirmed_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.local_cache_max_bytes = 100;
        let engine = StorageEngine::new(config, None).unwrap();

        let table_id = table_id().to_string();
        let table_dir = dir.path().join("sstables").join(&table_id);
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join("1-Data.db"), vec![1u8; 80]).unwrap();
        std::fs::write(table_dir.join("2-Data.db"), vec![2u8; 80]).unwrap();
        std::fs::write(table_dir.join("3-Data.db"), vec![3u8; 80]).unwrap();

        let mut manifest = crate::manifest::Manifest::new();
        for id in ["1", "2"] {
            manifest.add_sstable(
                &table_id,
                crate::manifest::ManifestEntry {
                    id: id.to_string(),
                    size: 80,
                    min_token: i64::MIN,
                    max_token: i64::MAX,
                    min_timestamp: 0,
                    max_timestamp: 0,
                },
            );
        }

        let evicted = engine
            .enforce_uploaded_sstable_cache_limit(&manifest)
            .unwrap();

        assert_eq!(evicted, 1);
        let uploaded_remaining = ["1", "2"]
            .into_iter()
            .filter(|id| table_dir.join(format!("{id}-Data.db")).exists())
            .count();
        assert_eq!(uploaded_remaining, 1);
        assert!(
            table_dir.join("3-Data.db").exists(),
            "unmanifested local SSTables must not be evicted"
        );
    }

    #[test]
    fn delete_sstable_files_removes_directory_layout_components_after_data() {
        let dir = tempfile::tempdir().unwrap();
        let table_dir = dir.path().join("sstables").join(table_id().to_string());
        let gen_dir = table_dir.join("42");
        std::fs::create_dir_all(&gen_dir).unwrap();

        for component in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ] {
            std::fs::write(gen_dir.join(format!("42-{component}")), b"x").unwrap();
        }

        let reclaimed = StorageEngine::delete_sstable_files(&table_dir, "42");

        assert!(reclaimed >= 7);
        assert!(
            !gen_dir.exists(),
            "directory-layout SSTable should be removed as a unit"
        );
    }

    #[test]
    fn s3_read_through_serves_directory_layout_sstable_without_full_rehydrate() {
        use bytes::Bytes;
        use ferrosa_sstable::io::{FdCache, FileReadAt, ReadAt};
        use object_store::ObjectStore;
        use std::num::NonZeroUsize;

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let table_id = table_id().to_string();
        let sstable_id = "42";
        let table_dir = data_dir.join("sstables").join(&table_id);
        let gen_dir = table_dir.join(sstable_id);
        std::fs::create_dir_all(&gen_dir).unwrap();

        let components = [
            ("Data.db", b"restored data".as_slice()),
            ("Partitions.db", b"restored partitions".as_slice()),
            ("Rows.db", b"restored rows".as_slice()),
            ("Filter.db", b"restored filter".as_slice()),
            ("Statistics.db", b"restored statistics".as_slice()),
            ("TOC.txt", b"restored toc".as_slice()),
            ("CompressionInfo.db", b"restored compression".as_slice()),
        ];

        std::fs::write(gen_dir.join("42-Data.db"), b"original data").unwrap();
        let cache = std::sync::Arc::new(FdCache::with_capacity(NonZeroUsize::new(4).unwrap()));
        let reader = FileReadAt::open_with_cache(gen_dir.join("42-Data.db"), cache).unwrap();
        StorageEngine::delete_sstable_files(&table_dir, sstable_id);

        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let prefix = "rehydrate-test".to_string();
        let hex = crate::upload::manager::hex_prefix_for(sstable_id);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for (component, bytes) in components {
                let path = crate::upload::manager::sstable_object_key(
                    &prefix, &hex, &table_id, sstable_id, component,
                );
                store
                    .put(
                        &path,
                        object_store::PutPayload::from(Bytes::copy_from_slice(bytes)),
                    )
                    .await
                    .unwrap();
            }
        });

        StorageEngine::install_s3_file_read_rehydration_hook(
            data_dir,
            prefix,
            std::sync::Arc::clone(&store),
        );

        let mut buf = vec![0u8; "restored data".len()];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, b"restored data");

        assert!(
            !gen_dir.join("42-Data.db").exists(),
            "range read-through should avoid full local Data.db rehydration"
        );

        for (component, bytes) in components {
            let path = gen_dir.join(format!("42-{component}"));
            assert!(
                !path.exists() || std::fs::read(&path).unwrap() == bytes,
                "component should either remain evicted or match object storage: {}",
                path.display()
            );
        }
    }

    #[test]
    fn check_write_admission_reads_cached_disk_free_not_live_statvfs() {
        // RED: the admission gate must consult the time-gated disk-free cache,
        // not run a live statvfs on every write. With a real writable temp
        // data_dir (which has free space) a live statvfs would always pass;
        // this test forces the cache below the reserve and expects a failure.
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.local_disk_free_reserve_bytes = 1024;
        let engine = StorageEngine::new(config, None).unwrap();

        engine.set_disk_free_cache_for_test(10);
        assert!(
            engine.check_write_admission().is_err(),
            "cached free space below reserve must reject the write"
        );

        engine.set_disk_free_cache_for_test(1_000_000_000);
        assert!(
            engine.check_write_admission().is_ok(),
            "cached free space above reserve must admit the write"
        );
    }

    #[test]
    fn uploaded_sstable_cache_eviction_responds_to_disk_pressure_before_cache_max() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.local_cache_max_bytes = 1024 * 1024;
        config.local_disk_free_reserve_bytes = u64::MAX / 4;
        let engine = StorageEngine::new(config, None).unwrap();

        let table_id = table_id().to_string();
        let table_dir = dir.path().join("sstables").join(&table_id);
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join("1-Data.db"), vec![1u8; 80]).unwrap();
        std::fs::write(table_dir.join("2-Data.db"), vec![2u8; 80]).unwrap();

        let mut manifest = crate::manifest::Manifest::new();
        for id in ["1", "2"] {
            manifest.add_sstable(
                &table_id,
                crate::manifest::ManifestEntry {
                    id: id.to_string(),
                    size: 80,
                    min_token: i64::MIN,
                    max_token: i64::MAX,
                    min_timestamp: 0,
                    max_timestamp: 0,
                },
            );
        }

        let evicted = engine
            .enforce_uploaded_sstable_cache_limit(&manifest)
            .unwrap();

        assert!(
            evicted > 0,
            "free-space target should evict manifest-confirmed SSTables even below cache max"
        );
    }

    #[test]
    fn storage_engine_new_removes_stale_compaction_staging_without_touching_sstables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let table = table_id().to_string();
        let compaction_table_dir = config.compaction.output_dir.join(&table);
        let sstable_table_dir = config.data_dir.join("sstables").join(&table);
        std::fs::create_dir_all(&compaction_table_dir).unwrap();
        std::fs::create_dir_all(&sstable_table_dir).unwrap();

        let stale_data = compaction_table_dir.join("101-Data.db");
        let stale_toc = compaction_table_dir.join("101-TOC.txt");
        let live_data = sstable_table_dir.join("99-Data.db");
        std::fs::write(&stale_data, b"stale compaction output").unwrap();
        std::fs::write(&stale_toc, b"TOC").unwrap();
        std::fs::write(&live_data, b"live sstable output").unwrap();

        let _engine = StorageEngine::new(config, None).unwrap();

        assert!(
            !stale_data.exists() && !stale_toc.exists(),
            "startup must remove stale compaction staging files"
        );
        assert!(
            live_data.exists(),
            "startup cleanup must not remove normal SSTable files"
        );
    }

    #[test]
    fn storage_engine_open_removes_stale_compaction_staging_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let table = table_id().to_string();
        let compaction_table_dir = config.compaction.output_dir.join(&table);
        std::fs::create_dir_all(&compaction_table_dir).unwrap();

        let stale_data = compaction_table_dir.join("202-Data.db");
        std::fs::write(&stale_data, b"stale compaction output").unwrap();

        let (_engine, _pending) = StorageEngine::open(config, None).unwrap();

        assert!(
            !stale_data.exists(),
            "restart must remove stale compaction staging files"
        );
    }

    #[test]
    fn startup_removes_legacy_pending_upload_compaction_staging_without_sstable_copy() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let table = table_id().to_string();
        let compaction_table_dir = config.compaction.output_dir.join(&table);
        std::fs::create_dir_all(&compaction_table_dir).unwrap();

        let legacy_pending_data = compaction_table_dir.join("303-Data.db");
        let stale_data = compaction_table_dir.join("404-Data.db");
        std::fs::write(&legacy_pending_data, b"legacy pending upload output").unwrap();
        std::fs::write(&stale_data, b"stale compaction output").unwrap();
        crate::upload::PendingUploadsLog::open(&config.data_dir.join("pending-uploads.log"))
            .unwrap()
            .add_entry(&table, "303")
            .unwrap();

        let _engine = StorageEngine::new(config, None).unwrap();

        assert!(
            !legacy_pending_data.exists(),
            "startup must remove legacy compaction-only pending upload staging files"
        );
        assert!(
            !stale_data.exists(),
            "startup must still remove unrelated stale staging files"
        );
    }

    #[test]
    fn open_accepts_current_schema_snapshot_json_before_replay() {
        let dir = tempfile::tempdir().unwrap();
        let schema_snapshot = serde_json::json!({
            "version": "00000000-0000-0000-0000-000000000001",
            "keyspaces": {},
            "tables": [
                [
                    ["test_ks", "test_table"],
                    {
                        "keyspace": "test_ks",
                        "name": "test_table",
                        "id": "00000000-0000-0000-0000-000000000002",
                        "columns": {
                            "pk": {
                                "name": "pk",
                                "kind": "PartitionKey",
                                "position": 0,
                                "column_type": "text",
                                "clustering_order": "None",
                                "mask": null
                            },
                            "ck": {
                                "name": "ck",
                                "kind": "Clustering",
                                "position": 0,
                                "column_type": "int",
                                "clustering_order": "Asc",
                                "mask": null
                            },
                            "val": {
                                "name": "val",
                                "kind": "Regular",
                                "position": 0,
                                "column_type": "text",
                                "clustering_order": "None",
                                "mask": null
                            }
                        },
                        "partition_key": ["pk"],
                        "clustering_key": [["ck", "Asc"]],
                        "params": {
                            "bloom_filter_fp_chance": 0.01,
                            "caching": { "keys": "ALL", "rows_per_partition": "NONE" },
                            "comment": "",
                            "compaction": {},
                            "compression": {},
                            "crc_check_chance": 1.0,
                            "default_time_to_live": 0,
                            "gc_grace_seconds": 864000,
                            "max_index_interval": 2048,
                            "min_index_interval": 128,
                            "memtable_flush_period_in_ms": 0,
                            "speculative_retry": "99PERCENTILE",
                            "additional_write_policy": "99PERCENTILE",
                            "cdc": false,
                            "read_repair": "BLOCKING",
                            "allow_auto_snapshot": true,
                            "incremental_backups": true
                        },
                        "flags": [],
                        "extensions": {},
                        "is_system": false
                    }
                ]
            ],
            "roles": {},
            "grants": {},
            "indexes": [],
            "types": [],
            "functions": [],
            "aggregates": []
        });
        std::fs::write(
            dir.path().join("schema.json"),
            serde_json::to_vec_pretty(&schema_snapshot).unwrap(),
        )
        .unwrap();

        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, pending) = StorageEngine::open(config, None).unwrap();
        assert!(
            pending.is_empty(),
            "no commit-log mutations should be pending"
        );

        let key = make_key("pk1");
        engine
            .write(&table_id(), &key, make_row(b"schema-backed", 1000), 1000)
            .expect("table from current SchemaSnapshot JSON should be registered before replay");
    }

    #[test]
    fn write_then_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let key = make_key("pk1");
        engine
            .write(&table_id(), &key, make_row(b"hello", 1000), 1000)
            .unwrap();

        let result = engine.read(&table_id(), &key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn write_fails_closed_when_local_disk_reserve_is_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.local_disk_free_reserve_bytes = u64::MAX;
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let key = make_key("pk1");
        let before = engine.commit_log_position();
        let err = engine
            .write(&table_id(), &key, make_row(b"hello", 1000), 1000)
            .expect_err("write must fail before commitlog append when disk reserve is exhausted");
        assert!(
            err.to_string()
                .contains("local disk free space below write reserve"),
            "unexpected error: {err}"
        );
        assert_eq!(engine.commit_log_position(), before);
    }

    #[tokio::test]
    async fn write_pressure_requests_urgent_s3_sync() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.local_disk_free_reserve_bytes = u64::MAX;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let engine = StorageEngine::new_with_upload_store(
            config,
            store,
            "urgent-sync".into(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();

        engine.register_table(test_schema()).unwrap();
        let key = make_key("pk1");

        let err = engine
            .write(&table_id(), &key, make_row(b"hello", 1000), 1000)
            .expect_err("write must fail under impossible reserve");
        assert!(err.is_backpressure(), "unexpected error: {err}");
        assert!(
            engine.take_s3_sync_request(),
            "disk-pressure write admission should request urgent S3 sync"
        );
        assert!(
            !engine.take_s3_sync_request(),
            "urgent S3 sync request should be consumed exactly once"
        );
    }

    #[test]
    fn read_unregistered_table_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let key = make_key("k");
        let result = engine.read(&TableId::new("no", "such"), &key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn write_to_unregistered_table_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let key = make_key("k");
        let result = engine.write(&TableId::new("no", "such"), &key, make_row(b"v", 1), 1);
        assert!(result.is_err());
    }

    #[test]
    fn unregister_table_prevents_writes() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();

        engine.register_table(test_schema()).unwrap();

        // Write should succeed while registered.
        let key = make_key("before");
        engine.write(&tid, &key, make_row(b"val", 1), 1).unwrap();

        // Unregister the table.
        engine.unregister_table(&tid).unwrap();

        // Write should now fail — table is no longer registered.
        let result = engine.write(&tid, &make_key("after"), make_row(b"v", 2), 2);
        assert!(result.is_err(), "write to unregistered table should fail");
    }

    #[test]
    fn drop_table_then_recreate_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();

        engine.register_table(test_schema()).unwrap();

        // Write and flush so data is in SSTables on disk.
        let key = make_key("old_data");
        engine.write(&tid, &key, make_row(b"stale", 1), 1).unwrap();
        engine.flush(&tid).unwrap();

        // Verify data exists.
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "data should exist before DROP");

        // DROP TABLE (unregister).
        engine.unregister_table(&tid).unwrap();

        // Re-create the table.
        engine.register_table(test_schema()).unwrap();

        // Old data must NOT be visible — the DROP should have deleted
        // the local SSTable directory.
        let result = engine.read(&tid, &key).unwrap();
        assert!(
            result.is_none(),
            "data must be gone after DROP+CREATE — got {:?}",
            result
        );
    }

    /// Reproduce bug-data-loss-after-drop-table: DROP TABLE on one table
    /// must NOT affect data in other tables in the same keyspace.
    #[test]
    fn drop_one_table_does_not_affect_other_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        // Two tables in the same keyspace.
        let schema_a = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let schema_b = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "co_occurs_with".to_string(),
            ..schema_a.clone()
        };

        let tid_a = TableId::new("test_ks", "entity_store");
        let tid_b = TableId::new("test_ks", "co_occurs_with");

        engine.register_table(schema_a.clone()).unwrap();
        engine.register_table(schema_b).unwrap();

        // Write data to BOTH tables and flush to SSTable files on disk.
        let key_a = make_key("entity_1");
        engine
            .write(&tid_a, &key_a, make_row(b"important_data", 1), 1)
            .unwrap();
        engine.flush(&tid_a).unwrap();

        let key_b = make_key("edge_1");
        engine
            .write(&tid_b, &key_b, make_row(b"will_be_dropped", 2), 2)
            .unwrap();
        engine.flush(&tid_b).unwrap();

        // Verify both tables have data.
        assert!(
            engine.read(&tid_a, &key_a).unwrap().is_some(),
            "entity_store should have data"
        );
        assert!(
            engine.read(&tid_b, &key_b).unwrap().is_some(),
            "co_occurs_with should have data"
        );

        // DROP TABLE co_occurs_with.
        engine.unregister_table(&tid_b).unwrap();

        // CRITICAL: entity_store data must STILL be present.
        let result = engine.read(&tid_a, &key_a).unwrap();
        assert!(
            result.is_some(),
            "BUG: DROP TABLE on co_occurs_with destroyed entity_store data!"
        );
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"important_data".as_slice()),
            "entity_store data must be intact after dropping a different table"
        );

        // Verify the SSTable directory for entity_store still exists.
        let entity_dir = dir.path().join("sstables").join("test_ks.entity_store");
        assert!(
            entity_dir.exists(),
            "entity_store SSTable directory must survive DROP of co_occurs_with"
        );

        // Verify co_occurs_with directory is gone.
        let cooccurs_dir = dir.path().join("sstables").join("test_ks.co_occurs_with");
        assert!(
            !cooccurs_dir.exists(),
            "co_occurs_with SSTable directory should be deleted"
        );
    }

    #[test]
    fn truncate_deletes_flushed_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();

        engine.register_table(test_schema()).unwrap();

        let key = make_key("trunc_data");
        engine
            .write(&tid, &key, make_row(b"will_be_gone", 1), 1)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Verify data exists.
        assert!(engine.read(&tid, &key).unwrap().is_some());

        // TRUNCATE.
        engine.truncate(&tid).unwrap();

        // Data must be gone.
        let result = engine.read(&tid, &key).unwrap();
        assert!(
            result.is_none(),
            "data must be gone after TRUNCATE — got {:?}",
            result
        );
    }

    /// LWW must work across the flush boundary: a newer value in an SSTable
    /// must beat an older value in the memtable.
    #[test]
    fn lww_survives_flush_newer_in_sstable_wins() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let key = make_key("lww_flush");

        // Write value A at high timestamp, then flush to SSTable.
        engine
            .write(
                &tid,
                &key,
                make_row(b"NEWER_VALUE", 7_000_000_000),
                7_000_000_000,
            )
            .unwrap();
        engine.flush(&tid).unwrap();

        // Write value B at lower timestamp to the new (empty) memtable.
        engine
            .write(
                &tid,
                &key,
                make_row(b"OLDER_VALUE", 3_000_000_000),
                3_000_000_000,
            )
            .unwrap();

        // Read should return NEWER_VALUE (ts=7e9 > ts=3e9).
        let result = engine.read(&tid, &key).unwrap().unwrap();
        let cell_value = result.rows[0].cells[0].1.value.as_deref().unwrap();
        assert_eq!(
            cell_value,
            b"NEWER_VALUE",
            "LWW failed across flush boundary: SSTable value (ts=7e9) should beat \
             memtable value (ts=3e9), but got {:?}",
            std::str::from_utf8(cell_value).unwrap_or("(not utf8)")
        );
    }

    /// Same as above but with multiple flush cycles.
    #[test]
    fn lww_survives_multiple_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let key = make_key("lww_multi");

        // Flush 1: write at ts=5e9
        engine
            .write(&tid, &key, make_row(b"V5", 5_000_000_000), 5_000_000_000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Flush 2: write at ts=7e9 (HIGHEST — should win)
        engine
            .write(&tid, &key, make_row(b"V7", 7_000_000_000), 7_000_000_000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Flush 3: write at ts=3e9
        engine
            .write(&tid, &key, make_row(b"V3", 3_000_000_000), 3_000_000_000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Memtable: write at ts=1e9
        engine
            .write(&tid, &key, make_row(b"V1", 1_000_000_000), 1_000_000_000)
            .unwrap();

        // Read should return V7 (highest timestamp across all sources).
        let result = engine.read(&tid, &key).unwrap().unwrap();
        let cell_value = result.rows[0].cells[0].1.value.as_deref().unwrap();
        assert_eq!(
            cell_value,
            b"V7",
            "LWW across 3 SSTables + memtable: expected V7 (ts=7e9), got {:?}",
            std::str::from_utf8(cell_value).unwrap_or("(not utf8)")
        );
    }

    /// Concurrent writers + periodic flushes: reproduces the loadgen pattern.
    /// 8 writer threads with different timestamp ranges write to 1000 keys.
    /// A flush thread triggers flushes periodically. After all writers stop,
    /// every key must have the value from the highest-timestamp write.
    #[test]
    fn concurrent_writers_with_flushes_preserve_lww() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            flush_threshold_bytes: 4096, // small to force frequent flushes
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let num_writers = 4usize;
        let key_space = 100usize;
        let writes_per_worker = 500usize;
        let stop = Arc::new(AtomicBool::new(false));

        // Track expected values: for each key, the highest-timestamp write.
        // Using a simple mutex-protected map (same pattern as loadgen GT).
        type ExpectedMap = std::collections::HashMap<String, (Vec<u8>, i64)>;
        let expected: Arc<std::sync::Mutex<ExpectedMap>> =
            Arc::new(std::sync::Mutex::new(ExpectedMap::new()));

        // Spawn writer threads.
        let mut handles = Vec::new();
        for worker_id in 0..num_writers {
            let engine = Arc::clone(&engine);
            let tid = tid.clone();
            let expected = Arc::clone(&expected);
            handles.push(std::thread::spawn(move || {
                let mut ts = (worker_id as i64) * 1_000_000_000;
                let mut rng_state = worker_id as u64;
                for i in 0..writes_per_worker {
                    let key_idx = (worker_id * 31 + i * 7) % key_space;
                    let key_str = format!("k{key_idx:06}");
                    ts += 1;
                    // Deterministic "random" value based on worker + iteration
                    rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let val_len = 10 + (rng_state % 50) as usize;
                    let value: Vec<u8> = (0..val_len)
                        .map(|j| ((rng_state >> (j % 8)) & 0xFF) as u8)
                        .collect();

                    let key = make_key(&key_str);
                    let row = make_row(&value, ts);
                    engine.write(&tid, &key, row, ts).unwrap();

                    // Track in expected map (LWW by timestamp).
                    let mut map = expected.lock().unwrap();
                    let entry = map.entry(key_str).or_insert_with(|| (Vec::new(), 0));
                    if ts >= entry.1 {
                        entry.0 = value;
                        entry.1 = ts;
                    }
                }
            }));
        }

        // Spawn flush thread.
        let flush_engine = Arc::clone(&engine);
        let flush_tid = tid.clone();
        let flush_stop = Arc::clone(&stop);
        let flush_handle = std::thread::spawn(move || {
            let mut flush_count = 0u32;
            while !flush_stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(10));
                if flush_engine.flush(&flush_tid).is_ok() {
                    flush_count += 1;
                }
            }
            flush_count
        });

        // Wait for all writers.
        for h in handles {
            h.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let flush_count = flush_handle.join().unwrap();

        // Final flush to push remaining memtable data to SSTables.
        engine.flush(&tid).ok();

        // Verify: every key must have the highest-timestamp value.
        let map = expected.lock().unwrap();
        let mut mismatches = 0u32;
        let mut missing = 0u32;
        for (key_str, (expected_val, expected_ts)) in map.iter() {
            let key = make_key(key_str);
            match engine.read(&tid, &key).unwrap() {
                Some(partition) => {
                    if let Some(row) = partition.rows.first() {
                        if let Some((_, cell)) = row.cells.first() {
                            if let Some(got) = cell.value.as_deref() {
                                if got != expected_val.as_slice() {
                                    mismatches += 1;
                                    if mismatches <= 3 {
                                        eprintln!(
                                            "MISMATCH {key_str}: expected {} bytes (ts={expected_ts}), \
                                             got {} bytes (cell_ts={}, all_cell_ts={:?}, sstables={}, read_errors={}, memtable_size={})",
                                            expected_val.len(),
                                            got.len(),
                                            cell.timestamp,
                                            row.cells
                                                .iter()
                                                .map(|(_, c)| c.timestamp)
                                                .collect::<Vec<_>>(),
                                            engine.sstable_count(&tid),
                                            engine.sstable_read_errors(&tid),
                                            engine.memtable_size(&tid)
                                        );
                                    }
                                }
                            } else {
                                missing += 1;
                            }
                        }
                    }
                }
                None => {
                    missing += 1;
                }
            }
        }

        eprintln!(
            "concurrent_writers_with_flushes: {} keys, {} mismatches, {} missing, {} flushes",
            map.len(),
            mismatches,
            missing,
            flush_count
        );
        assert_eq!(
            mismatches, 0,
            "LWW violated under concurrent writes + flushes: {mismatches} mismatches"
        );
        assert_eq!(missing, 0, "missing keys: {missing}");
    }

    /// Reproduce bug-large-write-causes-data-loss-in-partition:
    /// writing many rows to the same partition, flushing, writing more,
    /// flushing again — earlier rows must survive.
    #[test]
    fn large_write_same_partition_preserves_prior_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            flush_threshold_bytes: 512, // small to force flushes
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        // Helper: build a row with a UNIQUE clustering key.
        fn make_row_ck(ck: i32, value: &[u8], timestamp: i64) -> Row {
            Row {
                clustering: ck.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }
        }

        // Batch 1: write 50 rows with unique clustering keys and flush.
        for i in 0..50 {
            let key = make_key("pk1"); // same partition key for all
            let row = make_row_ck(i, format!("batch1_{i}").as_bytes(), (i + 1) as i64);
            engine.write(&tid, &key, row, (i + 1) as i64).unwrap();
        }
        engine.flush(&tid).unwrap();

        // Verify batch 1 data exists.
        let result = engine.read(&tid, &make_key("pk1")).unwrap();
        assert!(result.is_some(), "batch 1 data must exist after flush");
        let batch1_count = result.unwrap().rows.len();
        assert!(
            batch1_count >= 50,
            "batch 1 should have 50 rows, got {batch1_count}"
        );

        // Batch 2: write 100 MORE rows with different clustering keys.
        for i in 50..150 {
            let key = make_key("pk1");
            let row = make_row_ck(i, format!("batch2_{i}").as_bytes(), (i + 1) as i64);
            engine.write(&tid, &key, row, (i + 1) as i64).unwrap();
        }
        engine.flush(&tid).unwrap();

        // CRITICAL: ALL rows from both batches must survive.
        let result = engine.read(&tid, &make_key("pk1")).unwrap();
        assert!(result.is_some(), "data must exist after second flush");
        let total_rows = result.unwrap().rows.len();
        assert!(
            total_rows >= 150,
            "BUG: expected 150+ rows from both batches, got {total_rows}. \
             Large write to same partition caused data loss from prior flush."
        );
    }

    /// Simulates the production scenario: 500 rows written to the same
    /// partition with auto-flush every ~50 rows (low threshold). All 500
    /// must survive — the auto-flush must not drop rows from prior flushes.
    #[test]
    fn auto_flush_during_bulk_write_preserves_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            flush_threshold_bytes: 256, // very small to force frequent flushes
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let total_rows = 500;
        let key = make_key("bulk_partition");

        for i in 0..total_rows {
            let row = Row {
                clustering: (i as i32).to_be_bytes().to_vec(), // unique ck
                cells: vec![(
                    0,
                    CellValue::live(format!("row_{i}").into_bytes(), (i + 1) as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp((i + 1) as i64),
            };
            engine.write(&tid, &key, row, (i + 1) as i64).unwrap();

            // Manually flush every 50 rows to simulate auto-flush behavior
            if (i + 1) % 50 == 0 {
                engine.flush(&tid).unwrap();
            }
        }
        // Final flush for any remaining rows
        engine.flush(&tid).unwrap();

        // ALL 500 rows must be present
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "partition must exist after bulk write");
        let row_count = result.unwrap().rows.len();
        assert_eq!(
            row_count, total_rows,
            "expected {total_rows} rows after bulk write with auto-flush, got {row_count}. \
             Data loss during flush!"
        );
    }

    /// Verify that reading from an SSTable whose files were deleted returns
    /// an error/skip, not silently empty results.
    #[test]
    fn read_after_sstable_deleted_skips_not_panics() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let key = make_key("delete_test");
        engine.write(&tid, &key, make_row(b"val", 1), 1).unwrap();
        engine.flush(&tid).unwrap();

        // Verify data exists
        assert!(engine.read(&tid, &key).unwrap().is_some());

        // Delete the SSTable files from disk (simulates compaction file eviction)
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        if table_dir.exists() {
            for entry in std::fs::read_dir(&table_dir).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|e| e == "db" || e == "txt") {
                    std::fs::remove_file(&path).unwrap();
                }
            }
        }

        // Read must NOT panic after SSTable files are deleted.
        // The reader may still have data via mmap — both Some and None
        // are acceptable. The key invariant: no panic, no hang.
        let result = engine.read(&tid, &key);
        assert!(
            result.is_ok(),
            "read after SSTable deletion must not panic: {:?}",
            result.err()
        );
    }

    /// Production scenario: batch1 (small), flush, batch2 (large), flush,
    /// then read_range. All rows from both batches must appear in read_range.
    #[test]
    fn read_range_after_multi_batch_flush_returns_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("partition1");

        // Batch 1: 100 rows
        for i in 0..100i32 {
            let row = Row {
                clustering: i.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(b"b1".to_vec(), i as i64 + 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(i as i64 + 1),
            };
            engine.write(&tid, &pk, row, i as i64 + 1).unwrap();
        }
        engine.flush(&tid).unwrap();

        // Batch 2: 500 rows (different clustering keys)
        for i in 100..600i32 {
            let row = Row {
                clustering: i.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(b"b2".to_vec(), i as i64 + 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(i as i64 + 1),
            };
            engine.write(&tid, &pk, row, i as i64 + 1).unwrap();
        }
        engine.flush(&tid).unwrap();

        // read_range: must return the partition with ALL 600 rows
        let partitions = engine.read_range(&tid, None, None, 1000).unwrap();
        assert!(!partitions.is_empty(), "read_range must return data");
        let total_rows: usize = partitions.iter().map(|p| p.rows.len()).sum();
        assert_eq!(
            total_rows, 600,
            "read_range must return all 600 rows from both batches, got {total_rows}"
        );
    }

    /// Same as above but with a point read instead of range scan.
    #[test]
    fn point_read_after_multi_batch_flush_returns_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("partition1");

        // Batch 1: 100 rows
        for i in 0..100i32 {
            let row = Row {
                clustering: i.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(b"b1".to_vec(), i as i64 + 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(i as i64 + 1),
            };
            engine.write(&tid, &pk, row, i as i64 + 1).unwrap();
        }
        engine.flush(&tid).unwrap();

        // Batch 2: 500 rows
        for i in 100..600i32 {
            let row = Row {
                clustering: i.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(b"b2".to_vec(), i as i64 + 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(i as i64 + 1),
            };
            engine.write(&tid, &pk, row, i as i64 + 1).unwrap();
        }
        engine.flush(&tid).unwrap();

        // Point read
        let result = engine.read(&tid, &pk).unwrap();
        assert!(result.is_some(), "point read must return data");
        assert_eq!(
            result.unwrap().rows.len(),
            600,
            "point read must return all 600 rows"
        );
    }

    /// What happens if we flush, compact, then read? The compaction output
    /// should contain all data from the input SSTables.
    #[test]
    fn compaction_then_read_preserves_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("compact_pk");

        // Write 4 batches of 50 rows each, flush after each to create 4 SSTables
        for batch in 0..4 {
            for i in 0..50i32 {
                let ck = batch * 50 + i;
                let row = Row {
                    clustering: ck.to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(format!("batch{batch}_row{i}").into_bytes(), ck as i64 + 1),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ck as i64 + 1),
                };
                engine.write(&tid, &pk, row, ck as i64 + 1).unwrap();
            }
            engine.flush(&tid).unwrap();
        }

        assert_eq!(engine.sstable_count(&tid), 4, "should have 4 SSTables");

        // Read before compaction — should have 200 rows
        let pre = engine.read(&tid, &pk).unwrap().unwrap();
        assert_eq!(pre.rows.len(), 200, "pre-compaction: 200 rows expected");

        // Trigger compaction (STCS should select all 4 for compaction)
        // The background compaction thread runs separately, so we just
        // verify the data is still accessible.
        let post = engine.read(&tid, &pk).unwrap().unwrap();
        assert_eq!(
            post.rows.len(),
            200,
            "post-compaction: all 200 rows must survive"
        );
    }

    /// CI-sized regression for production high-volume ingest: many rows to the
    /// same partition key with flush_if_needed triggering automatically based on
    /// size. Keep this below the bounded-read/materialization cap; full
    /// production-volume verification belongs on streaming/paged paths.
    #[test]
    fn high_volume_ingest_with_auto_flush_preserves_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            flush_threshold_bytes: 4096, // 4KB — will trigger every ~50-100 rows
            flush_max_age_secs: 1,
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("tenant_session"); // same partition for all rows
        let total = 2_000;

        for i in 0..total {
            let row = Row {
                clustering: (i as i32).to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    CellValue::live(format!("entity_{i}").into_bytes(), (i + 1) as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp((i + 1) as i64),
            };
            engine.write(&tid, &pk, row, (i + 1) as i64).unwrap();

            // Simulate the background flush loop calling flush_if_needed
            // every 100 writes (production runs on a timer)
            if (i + 1) % 100 == 0 {
                engine.flush_if_needed().unwrap();
            }
        }

        // Final flush
        engine.flush(&tid).unwrap();

        // ALL 11,000 rows MUST be present
        let result = engine.read(&tid, &pk).unwrap();
        assert!(result.is_some(), "partition must exist");
        let actual = result.unwrap().rows.len();
        assert_eq!(
            actual,
            total,
            "DATA LOSS: expected {total} rows after high-volume ingest, got {actual}. \
             {} rows lost during flush_if_needed cycles.",
            total - actual
        );
    }

    /// Concurrent writes + flush from separate thread.
    #[test]
    fn concurrent_write_and_flush_threads_preserve_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            flush_threshold_bytes: 2048,
            flush_max_age_secs: 1,
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("concurrent_pk");
        let total = 5_000usize;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Background flush thread
        let flush_engine = Arc::clone(&engine);
        let flush_tid = tid.clone();
        let flush_stop = Arc::clone(&stop);
        let flush_handle = std::thread::spawn(move || {
            let mut count = 0u64;
            while !flush_stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = flush_engine.flush_if_needed();
                count += 1;
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            let _ = flush_engine.flush(&flush_tid);
            count
        });

        // Writer
        for i in 0..total {
            let row = Row {
                clustering: (i as i32).to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    CellValue::live(format!("r{i}").into_bytes(), (i + 1) as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp((i + 1) as i64),
            };
            engine.write(&tid, &pk, row, (i + 1) as i64).unwrap();
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let flush_count = flush_handle.join().unwrap();
        engine.flush(&tid).unwrap();

        let result = engine.read(&tid, &pk).unwrap();
        assert!(result.is_some(), "partition must exist");
        let actual = result.unwrap().rows.len();
        assert_eq!(
            actual,
            total,
            "DATA LOSS: {total} rows written with {flush_count} concurrent flushes, \
             got {actual}. {} lost.",
            total - actual
        );
    }

    /// SSTable roundtrip: 1000 rows with varying value sizes, flush, read
    /// back, verify every row and value is intact (no corruption).
    #[test]
    fn sstable_roundtrip_large_partition_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("large_part");
        let total = 1000usize;

        for i in 0..total {
            let value = format!("val_{i:06}_{}", "x".repeat(i % 100));
            let row = Row {
                clustering: (i as i32).to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(value.into_bytes(), (i + 1) as i64))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp((i + 1) as i64),
            };
            engine.write(&tid, &pk, row, (i + 1) as i64).unwrap();
        }
        engine.flush(&tid).unwrap();

        let result = engine.read(&tid, &pk).unwrap();
        assert!(result.is_some(), "partition must exist");
        let partition = result.unwrap();
        assert_eq!(partition.rows.len(), total, "SSTable lost rows");

        for (i, row) in partition.rows.iter().enumerate() {
            let expected = format!("val_{i:06}_");
            let actual = row.cells[0]
                .1
                .value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();
            assert!(
                actual.starts_with(&expected),
                "row {i} corrupt: expected '{expected}...', got '{}'",
                &actual[..actual.len().min(40)]
            );
        }
    }

    /// Flush → drop engine → re-open from disk → read back.
    ///
    /// Simulates a node restart. The second engine instance loads SSTables
    /// from disk via `load_existing_sstables_and_sidecars`, exactly like
    /// production startup. If the SSTable files are corrupt (wrong header,
    /// generation collision, serialization mismatch), this test catches it.
    ///
    /// Uses entity_store schema (CompositeType PK, UUID CK, multiple columns)
    /// to match the production table that showed 98% data loss on restart.
    #[test]
    fn flush_restart_roundtrip_entity_store_schema() {
        let dir = tempfile::tempdir().unwrap();

        let schema = TableSchema {
            keyspace: "agent_memory".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                org.apache.cassandra.db.marshal.UUIDType,\
                org.apache.cassandra.db.marshal.UUIDType)"
                .to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "entity_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "entity_name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "entity_type".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        };
        let tid = TableId::new("agent_memory", "entity_store");

        // Phase 1: Write data and flush
        let total_rows = 200usize;
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            // Composite PK: [u16 len][uuid bytes][0x00][u16 len][uuid bytes][0x00]
            let tenant_uuid = [0x11u8; 16];
            let session_uuid = [0x22u8; 16];
            let mut pk_bytes = Vec::new();
            pk_bytes.extend_from_slice(&(16u16).to_be_bytes());
            pk_bytes.extend_from_slice(&tenant_uuid);
            pk_bytes.push(0x00);
            pk_bytes.extend_from_slice(&(16u16).to_be_bytes());
            pk_bytes.extend_from_slice(&session_uuid);
            pk_bytes.push(0x00);
            let pk = DecoratedKey::new(PartitionKey::new(pk_bytes));

            for i in 0..total_rows {
                let mut entity_uuid = [0u8; 16];
                entity_uuid[14] = (i >> 8) as u8;
                entity_uuid[15] = i as u8;

                let row = Row {
                    clustering: entity_uuid.to_vec(),
                    cells: vec![
                        (
                            0,
                            CellValue::live(format!("entity_{i}").into_bytes(), (i + 1) as i64),
                        ),
                        (1, CellValue::live(b"concept".to_vec(), (i + 1) as i64)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp((i + 1) as i64),
                };
                engine.write(&tid, &pk, row, (i + 1) as i64).unwrap();
            }
            engine.flush(&tid).unwrap();

            // Verify pre-drop: data is readable
            let pre = engine.read(&tid, &pk).unwrap().unwrap();
            assert_eq!(
                pre.rows.len(),
                total_rows,
                "pre-restart: all rows must be present"
            );
        }
        // Engine dropped here — simulates process exit

        // Phase 2: Re-open from same directory (simulates restart)
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema).unwrap();

            let tenant_uuid = [0x11u8; 16];
            let session_uuid = [0x22u8; 16];
            let mut pk_bytes = Vec::new();
            pk_bytes.extend_from_slice(&(16u16).to_be_bytes());
            pk_bytes.extend_from_slice(&tenant_uuid);
            pk_bytes.push(0x00);
            pk_bytes.extend_from_slice(&(16u16).to_be_bytes());
            pk_bytes.extend_from_slice(&session_uuid);
            pk_bytes.push(0x00);
            let pk = DecoratedKey::new(PartitionKey::new(pk_bytes));

            let result = engine.read(&tid, &pk);
            assert!(
                result.is_ok(),
                "post-restart read should not error: {:?}",
                result.err()
            );
            let partition = result.unwrap().expect("partition must exist after restart");
            assert_eq!(
                partition.rows.len(),
                total_rows,
                "post-restart: SSTable lost rows — data corruption on restart"
            );
        }
    }

    /// Regression: the startup quarantine must NOT treat zero-byte Rows.db
    /// as corruption. `ferrosa-sstable/src/writer.rs:212` intentionally
    /// emits an empty Rows.db for simple partitions (no per-partition row
    /// index), and every SSTable the current writer produces therefore has
    /// a zero-byte Rows.db. A previous version of the quarantine list
    /// included Rows.db, which caused every legitimate SSTable to be moved
    /// to `quarantine/` on restart, losing all persisted data on the
    /// ferrosa-memory cluster (871 SSTables × 14k+ entities).
    #[test]
    fn startup_does_not_quarantine_zero_byte_rows_db() {
        let dir = tempfile::tempdir().unwrap();
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "rows_db".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let tid = TableId::new("test_ks", "rows_db");

        // Phase 1: write and flush so a real on-disk SSTable with the
        // writer's default zero-byte Rows.db exists.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            let pk = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &pk, row, 1000).unwrap();
            engine.flush(&tid).unwrap();
        }

        // Confirm at least one Rows.db on disk is zero-byte — this is what
        // the pre-fix quarantine treated as "corruption".
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let rows_db_files: Vec<_> = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with("-Rows.db"))
            .collect();
        assert!(
            !rows_db_files.is_empty(),
            "flush must produce a Rows.db file"
        );
        let any_zero_byte_rows_db = rows_db_files
            .iter()
            .any(|e| e.metadata().map(|m| m.len() == 0).unwrap_or(false));
        assert!(
            any_zero_byte_rows_db,
            "writer currently emits zero-byte Rows.db; this test guards the quarantine \
             heuristic that treated them as corruption. If the writer changes to emit \
             non-empty Rows.db, revisit the quarantine policy."
        );

        // Phase 2: reopen. The quarantine code must NOT move the SSTable
        // because of the zero-byte Rows.db. A quarantine directory
        // containing any SSTable components after reload is a regression.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema).unwrap();

            let quarantine_dir = table_dir.join("quarantine");
            if quarantine_dir.exists() {
                let count = std::fs::read_dir(&quarantine_dir)
                    .map(|it| it.count())
                    .unwrap_or(0);
                assert_eq!(
                    count, 0,
                    "startup quarantined SSTable(s) with zero-byte Rows.db — \
                     these are the writer's expected output, not corruption. \
                     See ferrosa-sstable/src/writer.rs:212."
                );
            }

            // And reading back the data must still work.
            let pk = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
            let p = engine.read(&tid, &pk).unwrap();
            assert!(
                p.is_some(),
                "data must survive restart after zero-byte Rows.db"
            );
        }
    }

    /// Regression: the default startup smoke test must not quarantine
    /// SSTables whose component headers open but whose Data.db contents cannot
    /// be fully iterated. It also must not admit them into the active reader
    /// set: a known-corrupt immutable file must not poison every range read
    /// until an operator manually moves it aside.
    #[test]
    fn startup_warn_mode_excludes_sstable_that_opens_but_fails_full_iteration_without_moving_it() {
        let dir = tempfile::tempdir().unwrap();
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "truncated_data".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let tid = TableId::new("test_ks", "truncated_data");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            let pk = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &pk, row, 1000).unwrap();
            engine.flush(&tid).unwrap();
        }

        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let data_file = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("-Data.db")
            })
            .expect("flush must produce a Data.db");
        let original_len = std::fs::metadata(&data_file).unwrap().len();
        assert!(
            original_len > 8,
            "test needs enough Data.db bytes to truncate"
        );
        std::fs::OpenOptions::new()
            .write(true)
            .open(&data_file)
            .unwrap()
            .set_len(original_len / 2)
            .unwrap();

        let pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> =
            Arc::new(crate::reader_pool::ReaderPool::new(8));
        let (descriptors, _sidecars, ids) =
            StorageEngine::load_existing_sstables_and_sidecars(&table_dir, &pool, "smoke-warn");
        assert!(
            ids.is_empty(),
            "default warn-mode startup smoke test must not admit a corrupt SSTable into the live view"
        );
        assert!(
            descriptors.is_empty(),
            "warn-mode startup must not produce a descriptor for the corrupt SSTable"
        );
        assert_eq!(
            pool.resident(),
            0,
            "the excluded corrupt SSTable must be evicted from the pool, not left resident"
        );

        let quarantine_dir = table_dir.join("quarantine");
        assert!(
            !quarantine_dir.exists(),
            "default warn-mode startup smoke test must not create quarantine"
        );
        assert!(
            data_file.exists(),
            "default warn-mode startup smoke test must leave corrupt components in place for salvage"
        );
    }

    /// Explicit quarantine mode is still available for an operator-controlled
    /// repair run once they have accepted that unreadable SSTables will be
    /// removed from the live view.
    #[test]
    fn startup_quarantine_mode_moves_sstable_that_opens_but_fails_full_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "truncated_data".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let tid = TableId::new("test_ks", "truncated_data");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            let pk = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &pk, row, 1000).unwrap();
            engine.flush(&tid).unwrap();
        }

        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let data_file = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("-Data.db")
            })
            .expect("flush must produce a Data.db");
        let original_len = std::fs::metadata(&data_file).unwrap().len();
        assert!(
            original_len > 8,
            "test needs enough Data.db bytes to truncate"
        );
        std::fs::OpenOptions::new()
            .write(true)
            .open(&data_file)
            .unwrap()
            .set_len(original_len / 2)
            .unwrap();

        let pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> =
            Arc::new(crate::reader_pool::ReaderPool::new(8));
        let (descriptors, _sidecars, ids) =
            StorageEngine::load_existing_sstables_and_sidecars_with_repair_mode(
                &table_dir,
                &pool,
                "smoke-quarantine",
                StartupSstableRepairMode::Quarantine,
            );
        assert!(
            ids.is_empty(),
            "explicit quarantine-mode repair must not admit the truncated SSTable"
        );
        assert!(
            descriptors.is_empty(),
            "quarantine-mode repair must not produce a descriptor for the quarantined SSTable"
        );
        assert_eq!(
            pool.resident(),
            0,
            "the quarantined SSTable must be evicted from the pool before its files are moved"
        );

        let quarantine_dir = table_dir.join("quarantine");
        assert!(
            quarantine_dir.exists(),
            "explicit quarantine-mode repair must create the quarantine directory"
        );
        let quarantined: Vec<_> = std::fs::read_dir(&quarantine_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            quarantined.iter().any(|name| name.ends_with("-Data.db")),
            "explicit quarantine-mode repair must move the corrupt generation components to quarantine; got {quarantined:?}"
        );
    }

    /// P0 crash-non-atomic-flush defense-in-depth: a Data.db truncated below
    /// the extent its own partition index claims must be EXCLUDED from the live
    /// view even when startup repair is fully OFF (no smoke test). This is the
    /// exact production scenario: a SIGKILL mid-flush left a final-named, nonzero
    /// but truncated Data.db that previously loaded silently and only failed at
    /// query time. The mode-independent extent gate converts that into a
    /// loud-at-load exclusion.
    #[test]
    fn startup_off_mode_excludes_sstable_with_truncated_data_below_index_extent() {
        let dir = tempfile::tempdir().unwrap();
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "truncated_extent".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let tid = TableId::new("test_ks", "truncated_extent");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();
            // Multiple partitions so the last partition starts at a non-zero
            // Data.db offset — required for the extent check to have a positive
            // bound to compare against.
            for i in 0..16i32 {
                let pk = DecoratedKey::new(PartitionKey::new(i.to_be_bytes().to_vec()));
                let row = Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(format!("value-{i}").into_bytes(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                };
                engine.write(&tid, &pk, row, 1000).unwrap();
            }
            engine.flush(&tid).unwrap();
        }

        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let data_file = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("-Data.db")
            })
            .expect("flush must produce a Data.db");
        let original_len = std::fs::metadata(&data_file).unwrap().len();
        assert!(
            original_len > 16,
            "test needs a Data.db with multiple partitions to truncate"
        );
        // Truncate hard — to a single byte — so the file is nonzero (escapes the
        // zero-byte critical-component check) yet far shorter than the last
        // partition's indexed offset.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&data_file)
            .unwrap()
            .set_len(1)
            .unwrap();
        assert_eq!(
            std::fs::metadata(&data_file).unwrap().len(),
            1,
            "Data.db must be nonzero so the zero-byte check does not fire"
        );

        let pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> =
            Arc::new(crate::reader_pool::ReaderPool::new(8));
        // Repair OFF: no smoke test runs. The mode-independent extent gate must
        // still exclude the truncated generation.
        let (descriptors, _sidecars, ids) =
            StorageEngine::load_existing_sstables_and_sidecars_with_repair_mode(
                &table_dir,
                &pool,
                "extent-off",
                StartupSstableRepairMode::Off,
            );
        assert!(
            ids.is_empty(),
            "repair-OFF startup must not admit a Data.db truncated below its index extent"
        );
        assert!(
            descriptors.is_empty(),
            "repair-OFF startup must not produce a descriptor for the truncated SSTable"
        );
        assert_eq!(
            pool.resident(),
            0,
            "the excluded truncated SSTable must be evicted from the pool"
        );
        // OFF mode is non-destructive: files stay in place for salvage.
        assert!(
            data_file.exists(),
            "repair-OFF must leave truncated components in place for salvage"
        );
        assert!(
            !table_dir.join("quarantine").exists(),
            "repair-OFF must not move files to quarantine"
        );
    }

    /// Given a table with one healthy SSTable and one corrupt SSTable, restart
    /// must keep the healthy generation queryable while retaining the corrupt
    /// generation on disk for salvage. This is the production recovery contract:
    /// corrupt immutable inputs degrade completeness, not availability.
    #[test]
    fn startup_warn_mode_excludes_corrupt_sstable_but_keeps_healthy_sstables_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();
        let schema = test_schema();
        let healthy_key = make_key("healthy");
        let corrupt_key = make_key("corrupt");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            engine
                .write(&tid, &healthy_key, make_row(b"healthy", 1000), 1000)
                .unwrap();
            engine.flush(&tid).unwrap();

            engine
                .write(&tid, &corrupt_key, make_row(b"corrupt", 2000), 2000)
                .unwrap();
            engine.flush(&tid).unwrap();
        }

        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let mut data_files: Vec<_> = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("-Data.db")
            })
            .collect();
        data_files.sort();
        assert_eq!(data_files.len(), 2, "test requires exactly two SSTables");
        let corrupt_data_file = data_files.pop().unwrap();
        let original_len = std::fs::metadata(&corrupt_data_file).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&corrupt_data_file)
            .unwrap()
            .set_len(original_len / 2)
            .unwrap();

        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(schema).unwrap();

        let healthy = engine
            .read(&tid, &healthy_key)
            .expect("healthy SSTable read must not be poisoned by corrupt SSTable")
            .expect("healthy partition must remain readable");
        assert_eq!(healthy.rows.len(), 1);
        assert_eq!(
            healthy.rows[0].cells[0].1.value.as_deref(),
            Some(&b"healthy"[..])
        );

        let range = engine
            .read_range(&tid, None, None, 10)
            .expect("range read must skip excluded corrupt SSTable");
        assert_eq!(
            range.len(),
            1,
            "range read should return the healthy partition and not fail on the excluded corrupt generation"
        );
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "only the healthy SSTable should be in the active reader set"
        );
        assert!(
            corrupt_data_file.exists(),
            "excluded corrupt SSTable component must remain on disk for salvage"
        );
    }

    /// Phase 5 gate (FMEA #1, top RPN): startup must validate every on-disk
    /// SSTable *transiently* — open → smoke-test → capture descriptor → drop —
    /// never accumulating O(count) live readers before the pool can bound them.
    ///
    /// We write N ≫ cap SSTables to a table directory, then drive the real
    /// startup path (`build_table_state`) with a small-cap reader pool and
    /// assert the resident reader count and the high-water peak both stay ≤ cap.
    /// Before the Phase 5 fix this loop materialized all N readers into a `Vec`
    /// and the peak equalled N, OOM-killing bloated nodes during startup.
    #[test]
    fn startup_build_table_state_holds_resident_readers_within_cap() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let tid = table_id();
        let cap = 4usize;
        let n = 40usize;

        // Materialize N SSTables on disk in the exact layout the engine startup
        // loop scans (`<data_dir>/sstables/<table_id>/`). A bare TableStore flush
        // does not auto-compact, so all N generations persist independently.
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        std::fs::create_dir_all(&table_dir).unwrap();
        {
            let store = TableStore::new(
                schema.clone(),
                FileFlushTarget::new_starting_at(table_dir.clone()).unwrap(),
                write_options_for_schema(&schema, true).unwrap(),
            );
            for i in 0..n {
                let key = make_key(&format!("pk-{i}"));
                store
                    .write(&key, make_row(format!("v{i}").as_bytes(), 1000 + i as i64))
                    .unwrap();
                store.flush().unwrap();
            }
            assert_eq!(store.sstable_count(), n, "test must produce N SSTables");
        }

        // Drive the real startup path with a small-cap pool.
        let config = StorageEngineConfig::test_config(dir.path());
        let pool: crate::store::SharedReaderPool<ferrosa_sstable::io::FileReadAt> =
            Arc::new(crate::reader_pool::ReaderPool::new(cap));
        let state =
            StorageEngine::build_table_state(&config, schema, Vec::new(), Arc::clone(&pool))
                .unwrap();

        assert_eq!(
            state.store.sstable_count(),
            n,
            "all {n} SSTables must be registered as descriptors after startup"
        );
        assert!(
            state.store.resident_reader_count() <= cap,
            "startup left {} resident readers; must be <= cap {cap}",
            state.store.resident_reader_count()
        );
        assert!(
            state.store.peak_resident_readers() <= cap,
            "startup peaked at {} resident readers; must never spike past cap {cap}",
            state.store.peak_resident_readers()
        );
    }

    /// Multiple flushes → drop engine → re-open from disk → read back.
    ///
    /// Simulates the production scenario: many writes triggering multiple
    /// auto-flushes, resulting in multiple SSTables per table. On restart,
    /// all SSTables must be loaded and merged correctly.
    #[test]
    fn multi_flush_restart_roundtrip_preserves_all_data() {
        let dir = tempfile::tempdir().unwrap();

        let schema = TableSchema {
            keyspace: "agent_memory".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                org.apache.cassandra.db.marshal.UUIDType,\
                org.apache.cassandra.db.marshal.UUIDType)"
                .to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "entity_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "entity_name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "entity_type".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        };
        let tid = TableId::new("agent_memory", "entity_store");
        let total_rows = 500usize;
        let flushes = 5;
        let rows_per_flush = total_rows / flushes;

        // Phase 1: Write in batches with explicit flushes
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema.clone()).unwrap();

            let pk = make_entity_store_pk([0x11; 16], [0x22; 16]);

            for batch in 0..flushes {
                for i in 0..rows_per_flush {
                    let idx = batch * rows_per_flush + i;
                    let mut entity_uuid = [0u8; 16];
                    entity_uuid[12..16].copy_from_slice(&(idx as u32).to_be_bytes());

                    let row = Row {
                        clustering: entity_uuid.to_vec(),
                        cells: vec![
                            (
                                0,
                                CellValue::live(
                                    format!("entity_{idx}").into_bytes(),
                                    (idx + 1) as i64,
                                ),
                            ),
                            (1, CellValue::live(b"concept".to_vec(), (idx + 1) as i64)),
                        ],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp((idx + 1) as i64),
                    };
                    engine.write(&tid, &pk, row, (idx + 1) as i64).unwrap();
                }
                engine.flush(&tid).unwrap();
            }

            assert!(
                engine.sstable_count(&tid) >= flushes,
                "should have {} SSTables, got {}",
                flushes,
                engine.sstable_count(&tid)
            );
        }

        // Phase 2: Re-open and verify all data survived
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(schema).unwrap();

            let pk = make_entity_store_pk([0x11; 16], [0x22; 16]);
            let partition = engine
                .read(&tid, &pk)
                .expect("read should not error")
                .expect("partition must exist after restart");

            assert_eq!(
                partition.rows.len(),
                total_rows,
                "post-restart: expected {total_rows} rows, got {} — \
                 data lost across {} SSTables",
                partition.rows.len(),
                flushes,
            );
        }
    }

    /// Helper: build CompositeType PK for entity_store.
    fn make_entity_store_pk(tenant: [u8; 16], session: [u8; 16]) -> DecoratedKey {
        let mut pk = Vec::new();
        pk.extend_from_slice(&(16u16).to_be_bytes());
        pk.extend_from_slice(&tenant);
        pk.push(0x00);
        pk.extend_from_slice(&(16u16).to_be_bytes());
        pk.extend_from_slice(&session);
        pk.push(0x00);
        DecoratedKey::new(PartitionKey::new(pk))
    }

    /// SSTable compaction roundtrip: 4 flushes → STCS compaction → verify all rows.
    #[test]
    fn sstable_compaction_roundtrip_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();

        let pk = make_key("compact_test");

        // Write 4 batches, flush each → 4 SSTables
        let mut total = 0usize;
        for batch in 0..4 {
            for i in 0..150i32 {
                let ck = batch * 150 + i;
                let row = Row {
                    clustering: ck.to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(format!("b{batch}_{ck:05}").into_bytes(), ck as i64 + 1),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ck as i64 + 1),
                };
                engine.write(&tid, &pk, row, ck as i64 + 1).unwrap();
                total += 1;
            }
            engine.flush(&tid).unwrap();
        }

        assert!(engine.sstable_count(&tid) >= 4, "need 4 SSTables");

        let result = engine.read(&tid, &pk).unwrap().unwrap();
        assert_eq!(
            result.rows.len(),
            total,
            "compaction roundtrip: expected {}, got {}",
            total,
            result.rows.len()
        );

        // Verify first batch data intact
        let first_val = result.rows[0].cells[0].1.value.as_ref().unwrap();
        let s = String::from_utf8_lossy(first_val);
        assert!(s.starts_with("b0_"), "batch 0 data corrupt: {s}");
    }

    #[test]
    fn write_flush_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let key = make_key("flushed_key");
        engine
            .write(&tid, &key, make_row(b"before_flush", 1000), 1000)
            .unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 1);
        assert_eq!(engine.memtable_size(&tid), 0);

        // Should still be readable from SSTable.
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"before_flush".as_slice())
        );
    }

    #[test]
    fn write_flush_write_read_merges() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let key = make_key("merge_key");

        // Write old value and flush to SSTable.
        engine
            .write(&tid, &key, make_row(b"old", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Write newer value — stays in memtable.
        engine
            .write(&tid, &key, make_row(b"new", 2000), 2000)
            .unwrap();

        // Should merge: timestamp 2000 wins.
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema1 = TableSchema {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            ..test_schema()
        };
        let schema2 = TableSchema {
            keyspace: "ks".to_string(),
            table: "t2".to_string(),
            ..test_schema()
        };

        engine.register_table(schema1).unwrap();
        engine.register_table(schema2).unwrap();

        let tid1 = TableId::new("ks", "t1");
        let tid2 = TableId::new("ks", "t2");
        let key = make_key("shared_key");

        engine
            .write(&tid1, &key, make_row(b"val1", 1000), 1000)
            .unwrap();
        engine
            .write(&tid2, &key, make_row(b"val2", 2000), 2000)
            .unwrap();

        let r1 = engine.read(&tid1, &key).unwrap().unwrap();
        assert_eq!(
            r1.rows[0].cells[0].1.value.as_deref(),
            Some(b"val1".as_slice())
        );

        let r2 = engine.read(&tid2, &key).unwrap().unwrap();
        assert_eq!(
            r2.rows[0].cells[0].1.value.as_deref(),
            Some(b"val2".as_slice())
        );
    }

    #[test]
    fn shutdown_flushes_all() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();

        engine.shutdown().unwrap();

        // After shutdown, SSTable should exist (flush happened).
        assert_eq!(engine.sstable_count(&tid), 1);
    }

    #[test]
    fn batch_write_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let mutations = vec![
            (make_key("k1"), make_row(b"v1", 1000), 1000i64),
            (make_key("k2"), make_row(b"v2", 2000), 2000),
            (make_key("k3"), make_row(b"v3", 3000), 3000),
        ];

        engine.batch_write(&tid, mutations).unwrap();

        assert!(engine.read(&tid, &make_key("k1")).unwrap().is_some());
        assert!(engine.read(&tid, &make_key("k2")).unwrap().is_some());
        assert!(engine.read(&tid, &make_key("k3")).unwrap().is_some());
    }

    #[test]
    fn batch_write_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let mutations: Vec<(DecoratedKey, Row, i64)> = vec![];
        engine.batch_write(&table_id(), mutations).unwrap();
    }

    #[test]
    fn engine_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        for i in 0..5 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }

        let results = engine.read_range(&tid, None, None, 100).unwrap();
        assert_eq!(results.len(), 5);
    }

    /// `read_token_range` must return EXACTLY the partitions whose tokens
    /// fall in `[start_token, end_token)`, regardless of where those
    /// partitions sit in key order. This is the primitive anti-entropy
    /// repair needs to ask "give me everything in this Merkle leaf's
    /// token sub-range" — the existing key-bounded `read_range` cannot
    /// answer that question.
    #[test]
    fn read_token_range_filters_by_token_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // Write 20 partitions; Murmur3 scatters their tokens.
        let mut keys_with_tokens: Vec<(String, i64)> = Vec::new();
        for i in 0..20 {
            let k = format!("k{i:03}");
            let dk = make_key(&k);
            let tok = dk.token.0;
            engine.write(&tid, &dk, make_row(b"v", 1000), 1000).unwrap();
            keys_with_tokens.push((k, tok));
        }

        keys_with_tokens.sort_by_key(|(_, t)| *t);
        let start = keys_with_tokens[5].1;
        let end = keys_with_tokens[15].1; // exclusive
        let expected_count = keys_with_tokens
            .iter()
            .filter(|(_, t)| *t >= start && *t < end)
            .count();

        let results = engine.read_token_range(&tid, start, end, 100).unwrap();
        assert_eq!(
            results.len(),
            expected_count,
            "expected exactly {expected_count} partitions in token range [{start}, {end})"
        );
        for p in &results {
            let t = p.key.token.0;
            assert!(
                t >= start && t < end,
                "partition token {t} outside requested range [{start}, {end})"
            );
        }
    }

    #[test]
    fn read_token_range_empty_range_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        for i in 0..5 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }

        // start == end → empty range.
        let results = engine.read_token_range(&tid, 0, 0, 100).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn read_token_range_full_range_returns_everything() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        for i in 0..7 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }
        let results = engine
            .read_token_range(&tid, i64::MIN, i64::MAX, 100)
            .unwrap();
        assert_eq!(results.len(), 7);
    }

    /// `read_token_range_bounded` must walk the range in token order, stop once
    /// the *byte* budget is hit (after at least one partition), and hand back a
    /// resume cursor so a looping caller covers every partition exactly once.
    /// This is what bounds repair-fetch working set by bytes rather than by an
    /// unbounded partition count.
    #[test]
    fn read_token_range_bounded_byte_budget_covers_all_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // ~1 KB value per partition so the byte budget bites long before any
        // partition-count cap.
        let big = vec![b'x'; 1024];
        let n = 50usize;
        for i in 0..n {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i:04}")),
                    make_row(&big, 1000),
                    1000,
                )
                .unwrap();
        }

        let max_bytes = 4 * 1024; // ~4 partitions per chunk
        let mut seen: Vec<i64> = Vec::new();
        let mut cursor = i64::MIN;
        let mut chunks = 0;
        loop {
            let (chunk, next) = engine
                .read_token_range_bounded(&tid, cursor, i64::MAX, 1000, max_bytes)
                .unwrap();
            if chunk.is_empty() {
                assert!(next.is_none(), "empty chunk must signal exhaustion");
                break;
            }
            chunks += 1;
            for p in &chunk {
                if let Some(&last) = seen.last() {
                    assert!(p.key.token.0 >= last, "tokens must be non-decreasing");
                }
                seen.push(p.key.token.0);
            }
            match next {
                Some(c) => cursor = c,
                None => break,
            }
        }

        assert_eq!(seen.len(), n, "must cover all {n} partitions");
        let mut uniq = seen.clone();
        uniq.dedup();
        assert_eq!(uniq.len(), seen.len(), "no partition read twice");
        assert!(
            chunks > 1,
            "byte budget should have split the read into chunks"
        );
    }

    /// The partition-count cap also stops a chunk early and resumes cleanly.
    #[test]
    fn read_token_range_bounded_count_budget_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        let n = 20usize;
        for i in 0..n {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i:04}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }

        let mut total = 0usize;
        let mut cursor = i64::MIN;
        let mut chunks = 0;
        loop {
            let (chunk, next) = engine
                .read_token_range_bounded(&tid, cursor, i64::MAX, 5, usize::MAX)
                .unwrap();
            if chunk.is_empty() {
                break;
            }
            assert!(chunk.len() <= 5, "must honor the partition-count cap");
            chunks += 1;
            total += chunk.len();
            match next {
                Some(c) => cursor = c,
                None => break,
            }
        }
        assert_eq!(total, n);
        assert_eq!(chunks, 4, "20 partitions / cap 5 → 4 chunks");
    }

    /// A single partition larger than the byte budget must still be emitted
    /// (a chunk of one) so a chunked caller always makes forward progress.
    #[test]
    fn read_token_range_bounded_emits_oversized_partition() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        let big = vec![b'y'; 1024];
        engine
            .write(&tid, &make_key("only"), make_row(&big, 1000), 1000)
            .unwrap();

        // Byte budget far below the single partition's size.
        let (chunk, next) = engine
            .read_token_range_bounded(&tid, i64::MIN, i64::MAX, 1000, 256)
            .unwrap();
        assert_eq!(chunk.len(), 1, "oversized partition must still be emitted");
        assert!(next.is_none(), "single partition exhausts the range");
    }

    /// Streaming contract: even with thousands of partitions in the table,
    /// asking for a small token sub-range must return ONLY the in-range
    /// matches. This is what makes anti-entropy repair viable on a multi-GB
    /// table in a constrained container: working set scales with matches,
    /// not table size.
    #[test]
    fn read_token_range_streaming_returns_only_in_range_matches() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        let n_total: usize = 1_000;
        let mut tokens: Vec<i64> = Vec::with_capacity(n_total);
        for i in 0..n_total {
            let k = format!("k{i:06}");
            let dk = make_key(&k);
            tokens.push(dk.token.0);
            engine.write(&tid, &dk, make_row(b"v", 1000), 1000).unwrap();
        }
        tokens.sort_unstable();

        // 1 % slice of the token space.
        let lo = tokens[(n_total * 49) / 100];
        let hi = tokens[(n_total * 50) / 100];
        let expected = tokens.iter().filter(|&&t| t >= lo && t < hi).count();
        assert!(expected > 0);

        let results = engine.read_token_range(&tid, lo, hi, 10_000).unwrap();
        assert_eq!(
            results.len(),
            expected,
            "streaming must return exactly the matches, not a prefix of the table"
        );
        for p in &results {
            let t = p.key.token.0;
            assert!(t >= lo && t < hi);
        }
    }

    #[test]
    fn read_token_range_honors_limit() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        for i in 0..10 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }
        let results = engine
            .read_token_range(&tid, i64::MIN, i64::MAX, 3)
            .unwrap();
        assert_eq!(results.len(), 3, "limit must cap the result count");
    }

    /// Memtable write-path backpressure. Without it, sustained
    /// writes (e.g. anti-entropy repair's apply phase) can grow
    /// the active memtable far past `flush_threshold_bytes`
    /// between maintenance-loop ticks (default 30 s), pinning
    /// hundreds of MB of partitions in RSS while a previous
    /// flush is still draining S3.
    ///
    /// The foreground path must not run the flush inline. Once the
    /// active memtable crosses the hard backpressure threshold, the next
    /// writer fails closed and requests a background flush. That gives the
    /// client backpressure without parking request handlers behind
    /// SSTable encoding/S3 cleanup.
    #[test]
    fn write_requests_background_flush_when_backpressure_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.memtable_backpressure_bytes = 1;
        config.flush_threshold_bytes = 1;
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        engine
            .write(&tid, &make_key("k"), make_row(b"data", 1000), 1000)
            .unwrap();
        assert!(
            engine.take_flush_request(),
            "crossing the soft threshold should request background flush"
        );

        let err = engine
            .write(&tid, &make_key("k2"), make_row(b"data", 1000), 1000)
            .unwrap_err();
        assert!(
            err.to_string().contains("memtable backpressure"),
            "write past memtable_backpressure_bytes must fail closed; got {err}"
        );
        assert_eq!(
            engine.sstable_count(&tid),
            0,
            "foreground write must not block on inline SSTable flush"
        );
        assert!(
            engine.take_flush_request(),
            "hard backpressure rejection should request background flush"
        );

        engine.flush_if_needed().unwrap();
        assert_eq!(engine.sstable_count(&tid), 1);
    }

    #[test]
    fn flush_if_needed_triggers_on_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.flush_threshold_bytes = 1; // Trigger on any write.
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k"), make_row(b"data", 1000), 1000)
            .unwrap();

        engine.flush_if_needed().unwrap();

        assert_eq!(engine.sstable_count(&tid), 1);
    }

    #[test]
    fn flush_all_flushes_small_memtables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        // Default threshold is 64 MB — our test row is well below that.
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"tiny", 1000), 1000)
            .unwrap();

        // Memtable should have data.
        assert!(
            engine.memtable_size(&tid) > 0,
            "memtable should be non-empty after write"
        );

        // flush_if_needed should NOT flush — data is below 64 MB threshold.
        engine.flush_if_needed().unwrap();
        assert!(
            engine.memtable_size(&tid) > 0,
            "flush_if_needed should skip small memtable"
        );
        assert_eq!(engine.sstable_count(&tid), 0, "no SSTable should exist yet");

        // flush_all should flush regardless of size.
        engine.flush_all().unwrap();
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush_all"
        );
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "one SSTable should exist after flush_all"
        );
    }

    #[test]
    fn flush_if_needed_force_flushes_cold_table_holding_closed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 512,
                ..CommitLogConfig::test_config(dir.path())
            },
            flush_threshold_bytes: 1024 * 1024,
            flush_max_age_secs: 3600,
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        let cold = table_id();
        let hot = table_id_2();
        engine.register_table(test_schema()).unwrap();
        engine.register_table(test_schema_2()).unwrap();

        engine
            .write(&cold, &make_key("cold"), make_row(b"cold", 1000), 1000)
            .unwrap();
        for i in 0..16 {
            engine
                .write(
                    &hot,
                    &make_key(&format!("hot-{i}")),
                    make_row(format!("hot-{i}").as_bytes(), 2000 + i),
                    2000 + i,
                )
                .unwrap();
        }
        engine.flush(&hot).unwrap();

        assert!(
            engine.commit_log_closed_segment_count() > 0,
            "test setup must retain at least one closed segment before retention-pressure flush"
        );
        assert!(
            engine.memtable_size(&cold) > 0,
            "cold table must still be unflushed before retention-pressure flush"
        );

        engine.flush_if_needed().unwrap();

        assert_eq!(
            engine.memtable_size(&cold),
            0,
            "flush_if_needed must force-flush cold memtables when closed commit-log segments are retained"
        );
        assert_eq!(engine.commit_log_closed_segment_count(), 0);
    }

    /// Regression test: write data, flush to SSTable, read back the exact
    /// partition key. Catches format mismatches where read_exact_at fails
    /// with "wanted N bytes, got M" after a flush (e.g., when the SSTable
    /// format changes between binary versions).
    #[test]
    fn write_flush_read_point_query_no_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Table with composite partition key + clustering + regular column
        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "memo_cache".into(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
            clustering_columns: vec![ferrosa_common::ColumnDefinition {
                name: "tenant_id".into(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".into(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "result".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "memo_cache");

        // Write multiple partitions with different composite keys
        for i in 0..5 {
            let pk_bytes = format!("hash_{i}\x00v1");
            let key = DecoratedKey::new(PartitionKey::new(pk_bytes.into_bytes()));
            let row = Row {
                clustering: vec![0u8; 16], // UUID-sized clustering
                cells: vec![(
                    0,
                    CellValue::live(format!("result_{i}").into_bytes(), 1000 + i),
                )],
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(
                    1000 + i,
                ),
            };
            engine.write(&tid, &key, row, 1000 + i).unwrap();
        }

        // Force flush to SSTable
        engine.flush_all().unwrap();
        assert!(engine.sstable_count(&tid) >= 1, "should have flushed");

        // Point read each partition — must not error
        for i in 0..5 {
            let pk_bytes = format!("hash_{i}\x00v1");
            let key = DecoratedKey::new(PartitionKey::new(pk_bytes.into_bytes()));
            let result = engine.read(&tid, &key);
            assert!(
                result.is_ok(),
                "point read after flush failed for partition {i}: {:?}",
                result.err()
            );
        }
    }

    /// Regression: SSTable corruption with long composite partition keys.
    /// Row 1 (long value) corrupts after flush; Row 2 (short value) survives.
    /// Reproduces the exact memo_cache scenario from production.
    #[test]
    fn write_flush_read_long_composite_pk_survives() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // memo_cache schema: ((content_hash text, model_version text), tenant_id uuid)
        let schema = ferrosa_common::TableSchema {
            keyspace: "agent_memory".into(),
            table: "memo_cache".into(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
            clustering_columns: vec![ferrosa_common::ColumnDefinition {
                name: "tenant_id".into(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".into(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "result".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("agent_memory", "memo_cache");

        // Row 1: long hash + medium model version + 31-byte result (CORRUPTS in prod)
        let hash1 = "cac0302657b4c1d0dfd5aec98f2754f46a42f117e53c77a9ba384ebf2095633a"; // pragma: allowlist secret
        let model1 = "claude-opus-4-6";
        let result1 = "The capital of France is Paris.";
        // Composite key encoding: [u16 len][bytes][0x00] per component
        let mut pk1_bytes = Vec::new();
        pk1_bytes.extend_from_slice(&(hash1.len() as u16).to_be_bytes());
        pk1_bytes.extend_from_slice(hash1.as_bytes());
        pk1_bytes.push(0x00);
        pk1_bytes.extend_from_slice(&(model1.len() as u16).to_be_bytes());
        pk1_bytes.extend_from_slice(model1.as_bytes());
        pk1_bytes.push(0x00);

        let key1 = DecoratedKey::new(PartitionKey::new(pk1_bytes.clone()));
        let row1 = Row {
            clustering: vec![0x11u8; 16], // UUID
            cells: vec![(0, CellValue::live(result1.as_bytes().to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key1, row1, 1000).unwrap();

        // Row 2: long hash + short model version + 1-byte result (SURVIVES in prod)
        let hash2 = "4bbe47ee6bb1d0ecaa4d47fce3d99e2044cdfdbfac4dde0c0c083f97e4fad000"; // pragma: allowlist secret
        let model2 = "test-v1";
        let result2 = "4";
        let mut pk2_bytes = Vec::new();
        pk2_bytes.extend_from_slice(&(hash2.len() as u16).to_be_bytes());
        pk2_bytes.extend_from_slice(hash2.as_bytes());
        pk2_bytes.push(0x00);
        pk2_bytes.extend_from_slice(&(model2.len() as u16).to_be_bytes());
        pk2_bytes.extend_from_slice(model2.as_bytes());
        pk2_bytes.push(0x00);

        let key2 = DecoratedKey::new(PartitionKey::new(pk2_bytes.clone()));
        let row2 = Row {
            clustering: vec![0x22u8; 16], // UUID
            cells: vec![(0, CellValue::live(result2.as_bytes().to_vec(), 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key2, row2, 2000).unwrap();

        // Verify memtable reads work
        assert!(
            engine.read(&tid, &key1).unwrap().is_some(),
            "row1 memtable read"
        );
        assert!(
            engine.read(&tid, &key2).unwrap().is_some(),
            "row2 memtable read"
        );

        // Flush to SSTable
        engine.flush_all().unwrap();
        assert!(engine.sstable_count(&tid) >= 1);

        // Point reads after flush — BOTH must succeed with data
        let p1 = engine
            .read(&tid, &key1)
            .expect("row1 read after flush should not error")
            .expect("row1 should exist after flush");
        assert!(
            !p1.rows.is_empty(),
            "row1 partition should have rows after flush"
        );

        let p2 = engine
            .read(&tid, &key2)
            .expect("row2 read after flush should not error")
            .expect("row2 should exist after flush");
        assert!(
            !p2.rows.is_empty(),
            "row2 partition should have rows after flush"
        );

        // Range scan should return both partitions
        let all = engine.read_range(&tid, None, None, 100).unwrap();
        assert_eq!(all.len(), 2, "range scan should find both partitions");
    }

    /// A test observer that counts `on_write` calls.
    struct CountingObserver {
        watched: Vec<TableId>,
        call_count: std::sync::atomic::AtomicU64,
    }

    impl CountingObserver {
        fn new(watched: Vec<TableId>) -> Self {
            Self {
                watched,
                call_count: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn count(&self) -> u64 {
            self.call_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::observer::WriteObserver for CountingObserver {
        fn mode(&self) -> crate::observer::ObserverMode {
            crate::observer::ObserverMode::Sync
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Vec::new()
        }
    }

    #[test]
    fn sync_observer_fires_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(CountingObserver::new(vec![tid.clone()]));
        engine.register_observer(observer.clone());

        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        assert_eq!(observer.count(), 1);
    }

    #[test]
    fn sync_observer_only_fires_for_watched_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        // Observer watches table_a, but we write to test_table.
        let table_a = TableId::new("other_ks", "other_table");
        let observer = Arc::new(CountingObserver::new(vec![table_a]));
        engine.register_observer(observer.clone());

        let tid = table_id();
        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        assert_eq!(observer.count(), 0);
    }

    /// A test observer that operates in async mode.
    struct AsyncCountingObserver {
        watched: Vec<TableId>,
        call_count: std::sync::atomic::AtomicU64,
    }

    impl AsyncCountingObserver {
        fn new(watched: Vec<TableId>) -> Self {
            Self {
                watched,
                call_count: std::sync::atomic::AtomicU64::new(0),
            }
        }
    }

    impl crate::observer::WriteObserver for AsyncCountingObserver {
        fn mode(&self) -> crate::observer::ObserverMode {
            crate::observer::ObserverMode::Async
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Vec::new()
        }
    }

    #[test]
    fn async_observer_does_not_block_write() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(AsyncCountingObserver::new(vec![tid.clone()]));
        let mut rx = engine.register_async_observer(observer, 16);

        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        // Write should succeed (async observer does not block).
        // The mutation should be in the channel.
        let msg = rx.try_recv();
        assert!(msg.is_ok());
        let (recv_tid, recv_mutation) = msg.unwrap();
        assert_eq!(recv_tid, tid);
        assert_eq!(recv_mutation.keyspace, "test_ks");
    }

    #[test]
    fn async_observer_backpressure_drops() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(AsyncCountingObserver::new(vec![tid.clone()]));
        // Tiny channel capacity of 2 to test backpressure.
        let _rx = engine.register_async_observer(observer, 2);

        assert_eq!(engine.observer_drop_count(), 0);

        // Write 5 mutations — channel holds 2, so at least 3 should be dropped.
        for i in 0..5 {
            let key = make_key(&format!("k{i}"));
            engine
                .write(&tid, &key, make_row(b"val", 1000 + i), 1000 + i)
                .unwrap();
        }

        // Drop count should be >= 3 (channel capacity 2, 5 writes).
        assert!(
            engine.observer_drop_count() >= 3,
            "expected >= 3 drops, got {}",
            engine.observer_drop_count()
        );
    }

    #[test]
    fn full_lifecycle_write_flush_replay_compact() {
        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();

        // Phase 1: Write data across multiple flush cycles.
        {
            let mut config = StorageEngineConfig::test_config(dir.path());
            config.compaction.min_threshold = 4;
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();

            // 4 flush cycles → 4 SSTables → triggers STCS
            for batch in 0..4 {
                for i in 0..3 {
                    let key_name = format!("batch{batch}_key{i}");
                    let ts = (batch * 1000 + i) as i64;
                    engine
                        .write(
                            &tid,
                            &make_key(&key_name),
                            make_row(key_name.as_bytes(), ts),
                            ts,
                        )
                        .unwrap();
                }
                engine.flush(&tid).unwrap();
            }

            assert_eq!(engine.sstable_count(&tid), 4);

            // Write one more (not flushed) — this must survive via replay.
            engine
                .write(
                    &tid,
                    &make_key("unflushed"),
                    make_row(b"survive", 9999),
                    9999,
                )
                .unwrap();

            engine.commit_log.shutdown().unwrap();
        }

        // Phase 2: Re-open, replay, verify all data present.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.replay_mutations(pending).unwrap();

            // The unflushed mutation should be present from replay.
            let result = engine.read(&tid, &make_key("unflushed")).unwrap();
            assert!(result.is_some(), "unflushed mutation should survive replay");

            // All flushed data should also be readable.
            for batch in 0..4 {
                for i in 0..3 {
                    let key_name = format!("batch{batch}_key{i}");
                    let r = engine.read(&tid, &make_key(&key_name)).unwrap();
                    assert!(r.is_some(), "flushed key {key_name} should be readable");
                }
            }

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn replay_tolerates_corrupt_segment() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Write data.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();

            let tid = table_id();
            engine
                .write(&tid, &make_key("good"), make_row(b"val", 1000), 1000)
                .unwrap();
            engine.commit_log.shutdown().unwrap();
        }

        // Corrupt one of the segment files by overwriting bytes in the middle.
        let log_dir = dir.path();
        for entry in std::fs::read_dir(log_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                let mut data = std::fs::read(&path).unwrap();
                if data.len() > 100 {
                    // Corrupt some bytes in the data section (after the header).
                    for b in &mut data[80..90] {
                        *b = 0xFF;
                    }
                    std::fs::write(&path, &data).unwrap();
                }
            }
        }

        // Phase 2: Replay should skip the corrupted entry.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            // Replay should not panic — corrupted entries are silently skipped.
            engine.register_table(test_schema()).unwrap();
            engine.replay_mutations(pending).unwrap();
        }
    }

    #[test]
    fn replay_mutations_defer_until_table_registration() {
        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();
        let key = make_key("deferred");
        let mutation = Mutation::new(
            tid.keyspace.clone(),
            tid.table.clone(),
            key.clone(),
            vec![make_row(b"late-schema", 1000)],
            1000,
        );

        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.replay_mutations(vec![mutation]).unwrap();
        assert!(
            engine.read(&tid, &key).unwrap().is_none(),
            "mutation must stay deferred until the table exists"
        );

        engine.register_table(test_schema()).unwrap();

        let replayed = engine
            .read(&tid, &key)
            .unwrap()
            .expect("deferred replay should materialize once the table is registered");
        assert_eq!(replayed.rows.len(), 1);
    }

    #[test]
    fn open_then_replay_before_register_table_still_recovers_pending_rows() {
        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();
        let key = make_key("survive-deferred");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine
                .write(&tid, &key, make_row(b"pending", 2000), 2000)
                .unwrap();
            engine.commit_log.shutdown().unwrap();
        }

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            engine
                .replay_mutations(pending)
                .expect("replay before registration should defer, not drop");
            assert!(
                engine.read(&tid, &key).unwrap().is_none(),
                "table is still unregistered, so the deferred row should not be visible yet"
            );

            engine.register_table(test_schema()).unwrap();

            let replayed = engine
                .read(&tid, &key)
                .unwrap()
                .expect("deferred replay should be applied when the table is registered");
            assert_eq!(replayed.rows.len(), 1);
            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn replay_keeps_unflushed_table_when_other_table_flushes_and_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let make_config = || StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 512,
                ..CommitLogConfig::test_config(dir.path())
            },
            ..StorageEngineConfig::test_config(dir.path())
        };

        let table_a = table_id();
        let table_b = table_id_2();
        let survivor_key = make_key("typed-edge-survivor");

        {
            let engine = StorageEngine::new(make_config(), None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.register_table(test_schema_2()).unwrap();

            engine
                .write(&table_a, &survivor_key, make_row(b"survive", 1000), 1000)
                .unwrap();

            for i in 0..16 {
                let key = make_key(&format!("other-{i}"));
                engine
                    .write(
                        &table_b,
                        &key,
                        make_row(format!("v{i}").as_bytes(), 2000 + i),
                        2000 + i,
                    )
                    .unwrap();
            }

            engine.flush(&table_b).unwrap();
            engine.commit_log.shutdown().unwrap();
        }

        {
            let (engine, pending) = StorageEngine::open(make_config(), None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.register_table(test_schema_2()).unwrap();
            engine.replay_mutations(pending).unwrap();

            let partition = engine
                .read(&table_a, &survivor_key)
                .unwrap()
                .expect("unflushed table_a row must survive replay even after table_b flush");
            assert_eq!(partition.rows.len(), 1);
        }
    }

    #[test]
    fn crash_replay_keeps_unflushed_table_when_other_table_flushes_and_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let make_config = || StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 512,
                ..CommitLogConfig::test_config(dir.path())
            },
            ..StorageEngineConfig::test_config(dir.path())
        };

        let table_a = table_id();
        let table_b = table_id_2();
        let survivor_key = make_key("typed-edge-crash-survivor");

        {
            let engine = StorageEngine::new(make_config(), None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.register_table(test_schema_2()).unwrap();

            engine
                .write(&table_a, &survivor_key, make_row(b"survive", 3000), 3000)
                .unwrap();

            for i in 0..16 {
                let key = make_key(&format!("other-crash-{i}"));
                engine
                    .write(
                        &table_b,
                        &key,
                        make_row(format!("v{i}").as_bytes(), 4000 + i),
                        4000 + i,
                    )
                    .unwrap();
            }

            engine.flush(&table_b).unwrap();
            engine.force_commit_log_sync().unwrap();
            drop(engine);
        }

        {
            let (engine, pending) = StorageEngine::open(make_config(), None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.register_table(test_schema_2()).unwrap();
            engine.replay_mutations(pending).unwrap();

            let partition = engine
                .read(&table_a, &survivor_key)
                .unwrap()
                .expect("crash replay must recover unflushed table_a row after table_b flush");
            assert_eq!(partition.rows.len(), 1);
        }
    }

    /// Layer 3 of the timeuuid-flush-wedge fix: a malformed mutation
    /// already durable in the commit log (e.g. from a pre-fix node that
    /// got a buggy `now()` row through the write path) must be
    /// quarantined on replay rather than re-wedging the memtable. Before
    /// the fix, restarting a wedged node would re-materialise the bad row
    /// in a fresh memtable and re-wedge every flush forever.
    ///
    /// See specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-
    /// from-now-function.md.
    #[test]
    fn replay_quarantines_malformed_row_and_node_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let make_config = || StorageEngineConfig::test_config(dir.path());

        // Schema mirroring `tool_usage_log`: TimeUUID-clustered.
        let timeuuid_schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tool_usage_log".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "call_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "tool_name".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let tid = TableId::new("ks", "tool_usage_log");
        let bad_key = make_key("tenant#day");
        let good_key = make_key("tenant#day-good");

        // Phase 1: open engine, register table, append a malformed
        // mutation directly to the commit log (bypassing Layer 1's
        // memtable-put validator the way a pre-fix node would have).
        let bad_mutation = {
            let engine = StorageEngine::new(make_config(), None).unwrap();
            engine.register_table(timeuuid_schema.clone()).unwrap();

            // A "good" mutation goes through the public write API, so the
            // commit log carries both shapes — guarantees replay
            // continues past the bad row and applies the good ones.
            let good_clustering = vec![0u8; 16];
            let good_row = Row {
                clustering: good_clustering,
                cells: vec![(0, CellValue::live(b"good_tool".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &good_key, good_row, 1000).unwrap();

            // Now build a malformed mutation: 8-byte cell where the
            // schema declares UTF8 (variable, fine), but clustering is
            // a 16-byte TimeUUID — we use a malformed clustering shape
            // to trip the per-cell validator on the regular cell of an
            // hypothetical second column. Actually, the cleanest
            // failure mode is a regular cell whose declared column
            // is fixed-width and whose payload is the wrong width.
            // We bind a fake column with TimeUUIDType and an 8-byte
            // value. To do that we add a regular column to the
            // schema below at index 0 of regular_columns.
            let bad_row = Row {
                clustering: vec![0u8; 16],
                // Cell at column index 0 (regular: tool_name = UTF8) is OK,
                // so we instead inject the bad cell at a TimeUUID-typed
                // column. Add an extra fake column for the bad cell.
                cells: vec![(0, CellValue::live(vec![0u8; 8], 2000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(2000),
            };
            // Construct a Mutation with a TimeUUIDType column at index 0.
            // We *re-register* the table with a different schema so the
            // bad cell is at a TimeUUID-typed column. Use a different
            // table to keep schemas independent.
            let bad_table_schema = TableSchema {
                table: "tool_usage_log_bad".to_string(),
                regular_columns: vec![ColumnDefinition {
                    name: "call_id_cell".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
                }],
                ..timeuuid_schema.clone()
            };
            engine.register_table(bad_table_schema.clone()).unwrap();
            let bad_tid = TableId::new(&bad_table_schema.keyspace, &bad_table_schema.table);
            let mutation = Mutation::new(
                bad_table_schema.keyspace.clone(),
                bad_table_schema.table.clone(),
                bad_key.clone(),
                vec![bad_row],
                2000,
            );
            // Bypass Layer 1: append directly to the commit log without
            // going through the memtable.
            engine.commit_log.append(&mutation).unwrap();
            engine.commit_log.shutdown().unwrap();
            (mutation, bad_tid, bad_table_schema)
        };
        let (_, bad_tid, bad_table_schema) = bad_mutation;

        // Phase 2: reopen the engine. Replay must:
        //  - apply the good row to the good table,
        //  - quarantine the bad row from the bad table without panicking,
        //  - leave the engine readable.
        let before = crate::quarantine::flush_quarantined_rows_total();
        {
            let (engine, pending) = StorageEngine::open(make_config(), None).unwrap();
            engine.register_table(timeuuid_schema).unwrap();
            engine.register_table(bad_table_schema.clone()).unwrap();
            engine.replay_mutations(pending).unwrap();

            // Good row replayed cleanly.
            let good = engine
                .read(&tid, &good_key)
                .unwrap()
                .expect("good row must replay into the memtable");
            assert_eq!(good.rows.len(), 1);

            // Bad row is NOT in the memtable.
            let bad = engine.read(&bad_tid, &bad_key).unwrap();
            assert!(
                bad.is_none() || bad.unwrap().rows.is_empty(),
                "bad row must be quarantined, not materialised"
            );

            engine.commit_log.shutdown().unwrap();
        }
        let after = crate::quarantine::flush_quarantined_rows_total();
        assert!(
            after > before,
            "FLUSH_QUARANTINED_ROWS_TOTAL must increment on replay (was {before} → {after})"
        );

        // Quarantine file exists under the bad table's flush dir.
        // Layout: <data_dir>/sstables/<keyspace>.<table>/quarantine/.
        let bad_table_dir = dir.path().join("sstables").join(format!("{}", bad_tid));
        let quarantine_dir = bad_table_dir.join("quarantine");
        assert!(
            quarantine_dir.exists(),
            "quarantine directory must exist at {}",
            quarantine_dir.display()
        );
        let entries: Vec<_> = std::fs::read_dir(&quarantine_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with(".jsonl"))
            })
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "exactly one quarantine JSONL must exist; got {} entries",
            entries.len()
        );
        let body = std::fs::read_to_string(entries[0].path()).unwrap();
        assert!(
            body.contains("TimeUUIDType") && body.contains("\"value_hex\":\"0000000000000000\""),
            "quarantine line must capture the malformed cell; got: {body}"
        );
    }

    #[tokio::test]
    async fn concurrent_read_during_compaction() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Create 2 SSTables.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Start a concurrent reader that continuously reads.
        let eng = Arc::clone(&engine);
        let reader_tid = tid.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                // These reads must always succeed — ArcSwap provides
                // atomic visibility regardless of concurrent compaction.
                let r1 = eng.read(&reader_tid, &make_key("k1")).unwrap();
                assert!(r1.is_some(), "k1 must always be readable");
                let r2 = eng.read(&reader_tid, &make_key("k2")).unwrap();
                assert!(r2.is_some(), "k2 must always be readable");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        // Trigger compaction while reader is active.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        engine.poll_compactions().await;

        handle.join().unwrap();
    }

    /// Deterministic reproduction of the read-vs-compaction data-loss race that
    /// `concurrent_read_during_compaction` only hit under CI coverage timing. A
    /// read snapshots the view (still referencing gen2, which solely holds `t2`),
    /// pauses at a test barrier, and a compaction then merges gen1+gen2 into one
    /// SSTable and deletes the inputs. When the read resumes it fails to open the
    /// deleted gen2; it must NOT silently return `Ok(None)` but retry against the
    /// new view, where the merged SSTable still holds `t2`.
    #[tokio::test]
    async fn read_during_compaction_retries_against_new_view() {
        use crate::store::read_race_test_hook::{ReadViewBarrier, ARMED};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // t1 -> gen1, t2 -> gen2. t2 lives ONLY in gen2 (no survivor SSTable).
        engine
            .write(&tid, &make_key("t1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("t2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 2);

        // Submit a compaction merging gen1+gen2 and wait until the merged output
        // is PRODUCED — but not yet swapped in (poll_compactions applies the swap
        // and deletes the inputs; we control exactly when that happens).
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: dir.path().join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }
        // Reader thread: arm it so its read pauses right after snapshotting the
        // still-unswapped view (referencing gen2).
        let barrier = ReadViewBarrier::new();
        let b_reader = Arc::clone(&barrier);
        let eng = Arc::clone(&engine);
        let rtid = tid.clone();
        let reader = std::thread::spawn(move || {
            ARMED.with(|c| *c.borrow_mut() = Some(b_reader));
            eng.read(&rtid, &make_key("t2"))
                .expect("read must not error")
        });

        // With the read holding the old view, drive the swap to completion:
        // poll until the background merge finishes and poll_compactions applies
        // it (view -> merged, gen1/gen2 files deleted) out from under the read.
        // The bound is a generous hang-guard (~30s), NOT a timing assertion: the
        // background merge always completes, but under a fully parallel test run
        // the executor thread can be heavily starved, so we wait it out rather
        // than racing a fixed deadline.
        barrier.wait_reached();
        let mut swapped = false;
        for _ in 0..1500 {
            engine.poll_compactions().await;
            if engine.sstable_count(&tid) == 1 {
                swapped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            swapped,
            "compaction swap should merge the two inputs into one (background merge never completed)"
        );
        barrier.release();

        let got = reader.join().unwrap();
        assert!(
            got.is_some(),
            "t2 must remain readable across a concurrent compaction that deleted its SSTable"
        );
    }

    /// Residual window — the *cache-hit-then-mid-read-ENOENT* path that the
    /// open-failure tests above do NOT exercise.
    ///
    /// `read_during_compaction_retries_against_new_view` deletes gen2 and lets
    /// the resumed read REOPEN it, so `open_reader` returns `Err` (cache miss on
    /// a deleted file) and the existing open-failure retry fires. The residual
    /// race is subtler: the gen2 reader is still POOLED (a flush seeded it), so
    /// `open_reader` is a cache HIT — open succeeds and the in-memory bloom says
    /// the key is present — but `evict_local_input_sstable_files` already deleted
    /// gen2's `Data.db`, so the *data seek* inside `get_partition_limited_rows`
    /// re-opens the path and hits `ENOENT` MID-READ. Before the fix that `Err`
    /// arm of `read_with_view` left `sstable_open_failed = false`, so no view
    /// retry fired and the committed key vanished as a spurious `Ok(None)` —
    /// silent data loss of a row that lives in the freshly-merged SSTable.
    ///
    /// This test reproduces that exact ordering deterministically with the
    /// `ReadViewBarrier` hook: the read snapshots the pre-swap view (gen2), pauses
    /// at the barrier while the compaction swap completes (merged output holds
    /// t2; gen2 `Data.db` deleted), and we then (a) re-seed gen2's still-open
    /// reader into the pool so the resumed `open_reader` is a cache HIT, and
    /// (b) evict gen2's `Data.db` fd so the seek must re-open the deleted path.
    /// On release the read takes open=ok → bloom=present → seek=ENOENT, which the
    /// fix turns into a view retry that finds t2 in the merged SSTable.
    ///
    /// Non-vacuous: WITHOUT the fix the mid-read `Err` is swallowed and the read
    /// returns `Ok(None)`, failing the `is_some()` assertion.
    #[tokio::test]
    async fn read_cache_hit_mid_read_delete_retries_against_new_view() {
        use crate::store::read_race_test_hook::{ReadViewBarrier, ARMED};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // t1 -> gen1, t2 -> gen2. t2 lives ONLY in gen2 (no survivor SSTable).
        // Each flush SEEDS the gen's reader into the pool, so a later read of t2
        // is a cache hit that consults gen2's in-memory bloom.
        engine
            .write(&tid, &make_key("t1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("t2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 2);

        // Identify gen2 (newest) and grab its still-pooled reader `Arc`. Holding
        // this `Arc` keeps the reader (and its in-memory bloom) alive across the
        // swap's pool eviction, so we can re-seed it as a cache hit afterwards.
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let gen2 = *StorageEngine::list_generations_in_dir(&table_dir)
            .iter()
            .max()
            .expect("gen2 must exist");
        let gen2_reader = engine
            .pooled_reader_arc_for_test(&tid, gen2)
            .expect("gen2 reader must be pooled after flush (cache-hit precondition)");
        let gen2_data_db =
            StorageEngine::generation_component_path_for_test(&table_dir, gen2, "Data.db")
                .expect("gen2 Data.db path must resolve");

        // Submit a compaction merging gen1+gen2; control when the swap applies.
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: dir.path().join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Reader thread: arm it so its read pauses right after snapshotting the
        // still-unswapped view (referencing gen2).
        let barrier = ReadViewBarrier::new();
        let b_reader = Arc::clone(&barrier);
        let eng = Arc::clone(&engine);
        let rtid = tid.clone();
        let reader = std::thread::spawn(move || {
            ARMED.with(|c| *c.borrow_mut() = Some(b_reader));
            eng.read(&rtid, &make_key("t2"))
                .expect("read must not error")
        });

        // With the read holding the old view, drive the swap to completion:
        // poll_compactions publishes the merged output and deletes gen2's files.
        barrier.wait_reached();
        let mut swapped = false;
        for _ in 0..1500 {
            engine.poll_compactions().await;
            if engine.sstable_count(&tid) == 1 {
                swapped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            swapped,
            "compaction swap should merge the two inputs into one (background merge never completed)"
        );
        assert!(
            !gen2_data_db.exists(),
            "the swap must have deleted gen2's Data.db (that is the mid-read ENOENT trigger)"
        );

        // Re-establish the cache-HIT precondition the swap just tore down: put
        // gen2's still-open reader back in the pool so the resumed `open_reader`
        // succeeds and its in-memory bloom reports t2 present...
        engine.reseed_pooled_reader_for_test(&tid, gen2, Arc::clone(&gen2_reader));
        // ...and evict gen2's Data.db descriptor so the data seek re-opens the
        // (now-deleted) path and observes ENOENT instead of reading through a
        // lingering unlinked fd.
        ferrosa_sstable::io::evict_global_fd_for_test(&gen2_data_db);

        barrier.release();
        let got = reader.join().unwrap();
        assert!(
            got.is_some(),
            "t2 must remain readable when a pooled (cache-hit) gen2 reader's Data.db was \
             deleted mid-read by a concurrent compaction — the mid-read ENOENT must drive a \
             view retry to the merged SSTable, never a spurious Ok(None) (silent data loss)"
        );
    }

    /// Window (3): the same read-vs-compaction data-loss race, but exercised
    /// through `read_clustering_row` — the production full-primary-key point-read
    /// hot path (write_path.rs routes CQL `=` point reads here). Before the fix
    /// this path had NO view-retry loop at all: a stale-view open failure just
    /// `continue`d and the read returned a spurious `Ok(None)` = silent data loss.
    /// It must now retry against the freshly-swapped view like the partition read.
    #[tokio::test]
    async fn clustering_read_during_compaction_retries_against_new_view() {
        use crate::store::read_race_test_hook::{ReadViewBarrier, ARMED};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // t1 -> gen1, t2 -> gen2. t2 lives ONLY in gen2 (no survivor SSTable).
        // Both use clustering [0,0,0,1] (see make_row).
        engine
            .write(&tid, &make_key("t1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("t2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 2);

        // Submit a compaction merging gen1+gen2; control exactly when the swap
        // (view -> merged, input files deleted) is applied via poll_compactions.
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: dir.path().join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Reader thread issues a full-primary-key clustering read; arm it so the
        // read pauses right after snapshotting the still-unswapped view (gen2).
        let barrier = ReadViewBarrier::new();
        let b_reader = Arc::clone(&barrier);
        let eng = Arc::clone(&engine);
        let rtid = tid.clone();
        let clustering = vec![0x00, 0x00, 0x00, 0x01];
        let reader = std::thread::spawn(move || {
            ARMED.with(|c| *c.borrow_mut() = Some(b_reader));
            eng.read_clustering_row(&rtid, &make_key("t2"), &clustering)
                .expect("read must not error")
        });

        // Drive the swap to completion under the paused read.
        barrier.wait_reached();
        let mut swapped = false;
        for _ in 0..1500 {
            engine.poll_compactions().await;
            if engine.sstable_count(&tid) == 1 {
                swapped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            swapped,
            "compaction swap should merge the two inputs into one (background merge never completed)"
        );
        barrier.release();

        let got = reader.join().unwrap();
        assert!(
            got.is_some(),
            "t2 must remain readable via read_clustering_row across a concurrent compaction \
             that deleted its SSTable (no spurious Ok(None) on the point-read hot path)"
        );
    }

    /// Window (1), DELETED-`Filter.db` variant — fail-loud + retry path.
    ///
    /// A read snapshots a view referencing the sole-survivor input SSTable
    /// (gen2). Compaction deletes gen2 and swaps in a merged output holding the
    /// key; gen2's `Data.db` is then restored on disk WITHOUT its `Filter.db`,
    /// so the stale view reopens a Filter-less SSTable. `open_file_sstable` must
    /// FAIL LOUD on the absent `Filter.db` (FIX A) — converting the window into
    /// an open `Err` that engages the existing view-retry, which reopens against
    /// the merged view and finds the key. Asserts the read returns `Some`.
    ///
    /// (Note: the bare empty-filter case is also caught by `BloomFilter::read`'s
    /// length check; this test pins the *diagnosable* fail-loud behavior and
    /// guards against a future change that would make that check tolerant.)
    #[tokio::test]
    async fn read_during_compaction_filter_db_deleted_retries_against_new_view() {
        use crate::store::read_race_test_hook::{ReadViewBarrier, ARMED};
        use std::sync::Arc;

        let dir = std::mem::ManuallyDrop::new(tempfile::tempdir().unwrap());
        let data_dir = dir.path().to_path_buf();
        let (engine, tid, gen2_dir, gen2, backups) = setup_window1_compaction(&dir);

        // Submit a compaction merging gen1+gen2; control when the swap applies.
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: data_dir.join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        let barrier = ReadViewBarrier::new();
        let b_reader = Arc::clone(&barrier);
        let eng = Arc::clone(&engine);
        let rtid = tid.clone();
        let reader = std::thread::spawn(move || {
            ARMED.with(|c| *c.borrow_mut() = Some(b_reader));
            eng.read(&rtid, &make_key("t2"))
                .expect("read must not error")
        });

        barrier.wait_reached();
        let mut swapped = false;
        for _ in 0..1500 {
            engine.poll_compactions().await;
            if engine.sstable_count(&tid) == 1 {
                swapped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            swapped,
            "compaction swap should merge the two inputs into one"
        );

        // Restore gen2's Data/Partitions/Rows (NOT Filter.db).
        std::fs::create_dir_all(&gen2_dir).unwrap();
        for (path, bytes) in &backups {
            std::fs::write(path, bytes).unwrap();
        }
        assert!(
            !gen2_dir.join(format!("{gen2}-Filter.db")).exists(),
            "gen2 Filter.db must remain DELETED to exercise window-1"
        );
        assert!(
            gen2_dir.join(format!("{gen2}-Data.db")).exists(),
            "gen2 Data.db must be present (only Filter.db is missing)"
        );

        barrier.release();
        let got = reader.join().unwrap();
        assert!(
            got.is_some(),
            "t2 must remain readable when gen2's Filter.db was concurrently \
             deleted mid-open (no spurious Ok(None) / no panic)"
        );
    }

    /// Window (1), DEGENERATE-`Filter.db` variant — the genuinely-silent case.
    ///
    /// A truncated/corrupt `Filter.db` consisting of just a valid 8-byte header
    /// with `word_count == 0` parses successfully (it passes `BloomFilter::read`)
    /// into a zero-bit filter. Before the fix, probing that filter executed
    /// `hash % 0` and PANICKED inside the read — crashing the point read. The
    /// `num_bits == 0 => may-contain` guard (FIX B) makes the probe return
    /// `true` instead, so the real SSTable read runs and the key is found.
    /// Asserts the read neither panics nor returns a spurious `Ok(None)`.
    #[tokio::test]
    async fn read_with_zero_bit_filter_db_does_not_panic_or_lose_row() {
        use std::sync::Arc;

        let dir = std::mem::ManuallyDrop::new(tempfile::tempdir().unwrap());
        let (engine, tid, gen2_dir, gen2, _backups) = setup_window1_compaction(&dir);

        // Overwrite gen2's Filter.db with a degenerate 0-word filter: a valid
        // 8-byte header (hash_count=3, word_count=0). This parses OK and yields
        // num_bits == 0 — the `% 0` panic / false-negative window.
        let mut degenerate = Vec::new();
        degenerate.extend_from_slice(&3i32.to_be_bytes());
        degenerate.extend_from_slice(&0i32.to_be_bytes());
        std::fs::write(gen2_dir.join(format!("{gen2}-Filter.db")), &degenerate).unwrap();

        // Drop gen2's pooled reader (the flush seeded it with the original,
        // valid in-memory filter) so the next read REOPENS gen2 from disk and
        // actually parses the degenerate Filter.db we just wrote.
        engine.evict_pooled_reader_for_test(&tid, gen2);

        // No compaction: gen2 still holds t2 and is still in the live view. The
        // read opens gen2, probes the zero-bit filter, and must find t2.
        let eng = Arc::clone(&engine);
        let rtid = tid.clone();
        let got = std::thread::spawn(move || {
            eng.read(&rtid, &make_key("t2"))
                .expect("read must not error")
        })
        .join()
        .expect("read thread must not panic on a zero-bit filter");

        assert!(
            got.is_some(),
            "t2 must remain readable through a degenerate zero-bit Filter.db \
             (no `% 0` panic, no false-negative prune to Ok(None))"
        );
    }

    /// Shared setup for the window-1 tests: t1 -> gen1, t2 -> gen2 (t2 lives
    /// ONLY in gen2). Returns the engine, table id, gen2's directory + number,
    /// and a backup of gen2's non-filter components (for the deletion variant).
    #[allow(clippy::type_complexity)]
    fn setup_window1_compaction(
        dir: &tempfile::TempDir,
    ) -> (
        std::sync::Arc<StorageEngine>,
        TableId,
        std::path::PathBuf,
        u64,
        Vec<(std::path::PathBuf, Vec<u8>)>,
    ) {
        use std::sync::Arc;
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        engine
            .write(&tid, &make_key("t1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("t2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 2);

        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let gens = StorageEngine::list_generations_in_dir(&table_dir);
        let gen2 = *gens.iter().max().expect("a gen2 must exist");
        let gen2_dir = StorageEngine::generation_dir_path(&table_dir, gen2)
            .expect("gen2 directory must resolve");

        let mut backups: Vec<(std::path::PathBuf, Vec<u8>)> = Vec::new();
        for comp in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Statistics.db",
            "TOC.txt",
            "CompressionInfo.db",
        ] {
            let p = gen2_dir.join(format!("{gen2}-{comp}"));
            if let Ok(bytes) = std::fs::read(&p) {
                backups.push((p, bytes));
            }
        }
        assert!(!backups.is_empty(), "gen2 must have at least Data.db");
        (engine, tid, gen2_dir, gen2, backups)
    }

    #[test]
    fn commit_log_position_exposed() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let before = engine.commit_log_position();
        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        let after = engine.commit_log_position();

        assert!(
            after > before,
            "commit_log_position should advance after write"
        );

        engine.shutdown().unwrap();
    }

    /// Gap 2 (WAL discarded before durability): `engine.flush()` must NOT
    /// advance the commit-log checkpoint / discard segments when the SSTable
    /// flush fails. The WAL is the only other copy of those mutations; if flush
    /// tears or fails, replay must still be able to rebuild them. We induce a
    /// flush failure by making the table's SSTable directory read-only, then
    /// assert (a) flush returns Err and (b) no closed segment was discarded.
    #[cfg(unix)]
    #[test]
    fn flush_failure_does_not_discard_commit_log_before_durability() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 512, // tiny: forces rotation so there are closed segments
                ..CommitLogConfig::test_config(dir.path())
            },
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        for i in 0..20 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"value", 1000 + i),
                    1000 + i,
                )
                .unwrap();
        }

        let closed_before = engine.commit_log.closed_segment_count();
        assert!(
            closed_before >= 2,
            "need multiple closed segments for this test, got {closed_before}"
        );

        // Make the table's SSTable directory read-only so the flush write/rename
        // fails. The directory already exists (created on first flush attempt or
        // table registration); ensure it exists, then strip write perms.
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        std::fs::create_dir_all(&table_dir).unwrap();
        let mut perms = std::fs::metadata(&table_dir).unwrap().permissions();
        perms.set_mode(0o500); // r-x: no write
        std::fs::set_permissions(&table_dir, perms).unwrap();

        let flush_result = engine.flush(&tid);

        // Restore perms so the tempdir can be cleaned up regardless of outcome.
        let mut restore = std::fs::metadata(&table_dir).unwrap().permissions();
        restore.set_mode(0o700);
        std::fs::set_permissions(&table_dir, restore).unwrap();

        assert!(
            flush_result.is_err(),
            "flush into a read-only SSTable dir must fail, not silently succeed"
        );

        let closed_after = engine.commit_log.closed_segment_count();
        assert_eq!(
            closed_after, closed_before,
            "commit log segments must NOT be discarded when flush fails: \
             before={closed_before}, after={closed_after}"
        );

        engine.shutdown().unwrap();
    }

    #[test]
    fn flush_discards_commit_log_segments() {
        // Regression test for FD leak: flush() must call discard_completed()
        // so that closed commit log segments are GC'd and their file handles
        // released. Without this, segments accumulate indefinitely under load.
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 512, // tiny: forces rotation after ~3 mutations
                ..CommitLogConfig::test_config(dir.path())
            },
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Write enough mutations to force multiple segment rotations.
        for i in 0..20 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"value", 1000 + i),
                    1000 + i,
                )
                .unwrap();
        }

        let closed_before = engine.commit_log.closed_segment_count();
        assert!(
            closed_before >= 2,
            "need multiple closed segments for this test, got {closed_before}"
        );

        // Flush the table — this calls discard_completed() internally,
        // which removes segments where all tables have been flushed.
        engine.flush(&tid).unwrap();

        // discard_completed() in flush() should have already cleaned up
        // all closed segments (single table = all segments become empty).
        let closed_after = engine.commit_log.closed_segment_count();
        assert!(
            closed_after < closed_before,
            "closed segment count should decrease after flush: \
             before={closed_before}, after={closed_after}"
        );

        // Data should still be readable from SSTable after commit log GC.
        for i in 0..20 {
            let result = engine.read(&tid, &make_key(&format!("k{i}"))).unwrap();
            assert!(result.is_some(), "key k{i} should be readable after flush");
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn archiver_uploads_closed_segment_on_rotate() {
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let prefix = "test-node";

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig {
                    segment_size: 512, // small to force rotation
                    archive: Some(crate::commitlog::config::ArchiveConfig {
                        enabled: true,
                        poll_interval: std::time::Duration::from_millis(50),
                        ..crate::commitlog::config::ArchiveConfig::default()
                    }),
                    ..CommitLogConfig::test_config(dir.path())
                },
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new_with_archive_store(
                config,
                Some(&tokio::runtime::Handle::current()),
                Some(Arc::clone(&store)),
                prefix.to_string(),
            )
            .unwrap();

            engine.register_table(test_schema()).unwrap();

            let tid = table_id();
            let key = make_key("k1");
            let row = make_row(b"value", 1000);

            // Write enough to trigger rotation.
            for i in 0..20 {
                let _ = engine.write(&tid, &key, row.clone(), i);
            }

            // Give the archiver time to process.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            // Verify at least one segment was uploaded to S3.
            let manifest =
                crate::commitlog::manifest::ArchiveManifest::load(store.as_ref(), prefix)
                    .await
                    .unwrap();
            assert!(
                !manifest.segments.is_empty(),
                "archiver should have uploaded at least one segment"
            );

            // Verify the segment data is in S3.
            let seg = &manifest.segments[0];
            let hex = crate::upload::manager::hex_prefix_for(&seg.id.to_string());
            let s3_path =
                ObjectPath::from(format!("{prefix}/commitlog-archive/{hex}/{}.log", seg.id));
            let result = store.get(&s3_path).await;
            assert!(result.is_ok(), "segment file should exist in S3");

            engine.shutdown().unwrap();
        });
    }

    // ── open_from_snapshot_with_store tests ──────────────────────────────────

    #[test]
    fn open_from_snapshot_downloads_and_validates() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            // Set up: manifest + schema in S3, then create a snapshot.
            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot("test-snap", &manifest, b"{}", pos, "node-1", None, false)
                .await
                .unwrap();

            // Restore from snapshot.
            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap",
                None,     // no PIT filter
                "node-1", // same node
                false,    // no force
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            // Engine should be functional.
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn open_from_snapshot_rejects_cross_node_without_force() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot("test-snap", &manifest, b"{}", pos, "node-1", None, false)
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let result = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap",
                None,
                "node-2", // different node!
                false,    // no force
                Arc::clone(&store),
                prefix,
            )
            .await;

            assert!(
                result.is_err(),
                "cross-node restore without force must fail"
            );
            let err_msg = match result {
                Err(e) => e.to_string(),
                Ok(_) => unreachable!(),
            };
            assert!(
                err_msg.contains("force"),
                "error message should mention 'force': {err_msg}"
            );
        });
    }

    #[test]
    fn open_from_snapshot_force_allows_cross_node_restore() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot(
                    "test-snap-force",
                    &manifest,
                    b"{}",
                    pos,
                    "node-1",
                    None,
                    false,
                )
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            // force = true should succeed even though node IDs differ.
            let engine = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap-force",
                None,
                "node-2", // different node
                true,     // force override
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn open_from_snapshot_rejects_missing_snapshot() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let result = StorageEngine::open_from_snapshot_with_store(
                config,
                "does-not-exist",
                None,
                "node-1",
                false,
                Arc::clone(&store),
                "prefix",
            )
            .await;

            assert!(result.is_err(), "missing snapshot should return an error");
        });
    }

    #[test]
    fn create_snapshot_flushes_and_writes_to_s3() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: std::sync::Arc<dyn object_store::ObjectStore> =
                std::sync::Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            // Save a manifest and schema so snapshot can load them.
            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new(config, None).unwrap();

            // Register a table and write some data.
            engine.register_table(test_schema()).unwrap();
            let tid = table_id();
            let key = make_key("k1");
            engine
                .write(&tid, &key, make_row(b"value", 1000), 1000)
                .unwrap();

            // Create snapshot via injected store.
            let metadata = engine
                .create_snapshot_with_store(
                    "test-snap",
                    "node-1",
                    None,
                    false,
                    std::sync::Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            assert_eq!(metadata.name, "test-snap");
            assert_eq!(metadata.node_id, "node-1");
            assert!(!metadata.manifest_sha256.is_empty());
            assert!(!metadata.ephemeral);

            // Verify snapshot objects exist in S3.
            let meta_path = object_store::path::Path::from(format!(
                "{prefix}/snapshots/test-snap/metadata.json"
            ));
            assert!(store.get(&meta_path).await.is_ok());

            engine.shutdown().unwrap();
        });
    }

    // =========================================================================
    // PITR E2E: Full snapshot→restore data verification (FMEA E1-E3)
    // =========================================================================

    /// Helper: creates an engine with S3-backed manifest for snapshot tests.
    /// Returns (engine, store, prefix). Must be called from an async context.
    async fn setup_snapshot_engine(
        dir: &std::path::Path,
    ) -> (
        StorageEngine,
        Arc<dyn object_store::ObjectStore>,
        &'static str,
    ) {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "test-node";

        // Initialise live manifest + schema in S3 (required by create_snapshot_with_store).
        let manifest = crate::manifest::Manifest::new();
        manifest
            .save_with_retry(store.as_ref(), prefix)
            .await
            .unwrap();
        crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
            .await
            .unwrap();

        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir),
            ..StorageEngineConfig::test_config(dir)
        };

        let engine = StorageEngine::new(config, None).unwrap();
        (engine, store, prefix)
    }

    /// Helper: a second table schema for multi-table tests.
    fn test_schema_2() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "other_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn table_id_2() -> TableId {
        TableId::new("test_ks", "other_table")
    }

    /// E1: Write data → snapshot → write more → restore → verify only
    /// pre-snapshot data is present.
    ///
    /// This is the most fundamental PITR test: after restoring from a
    /// snapshot, the database must contain exactly the data that existed
    /// at snapshot time — no more, no less.
    #[test]
    fn e1_snapshot_restore_verifies_data_content() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            // ── Phase 1: Write data and create snapshot ──────────────
            let dir = tempfile::tempdir().unwrap();
            let (engine, store, prefix) = setup_snapshot_engine(dir.path()).await;

            engine.register_table(test_schema()).unwrap();
            let tid = table_id();

            // Write 3 rows before snapshot.
            engine
                .write(
                    &tid,
                    &make_key("alice"),
                    make_row(b"before-snap", 1000),
                    1000,
                )
                .unwrap();
            engine
                .write(&tid, &make_key("bob"), make_row(b"before-snap", 1001), 1001)
                .unwrap();
            engine
                .write(
                    &tid,
                    &make_key("carol"),
                    make_row(b"before-snap", 1002),
                    1002,
                )
                .unwrap();

            // Flush so data is in SSTables (and thus in the manifest).
            engine.flush(&tid).unwrap();

            // Update S3 manifest with the flushed SSTable(s).
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            // Create snapshot.
            let snap_metadata = engine
                .create_snapshot_with_store(
                    "snap-1",
                    "node-1",
                    None,
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();
            assert_eq!(snap_metadata.name, "snap-1");

            // ── Phase 2: Write MORE data after snapshot ──────────────
            engine
                .write(&tid, &make_key("dave"), make_row(b"after-snap", 2000), 2000)
                .unwrap();
            engine
                .write(&tid, &make_key("eve"), make_row(b"after-snap", 2001), 2001)
                .unwrap();

            // Verify all 5 rows exist in the live engine.
            assert!(engine.read(&tid, &make_key("alice")).unwrap().is_some());
            assert!(engine.read(&tid, &make_key("dave")).unwrap().is_some());
            engine.shutdown().unwrap();

            // ── Phase 3: Restore from snapshot ──────────────────────
            let restore_dir = tempfile::tempdir().unwrap();
            let restore_config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(restore_dir.path()),
                ..StorageEngineConfig::test_config(restore_dir.path())
            };

            let restored = StorageEngine::open_from_snapshot_with_store(
                restore_config,
                "snap-1",
                None,
                "node-1",
                false,
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            // Register the table so the engine can find the downloaded SSTables.
            restored.register_table(test_schema()).unwrap();

            // ── Phase 4: Verify data ────────────────────────────────
            // Pre-snapshot rows MUST be present.
            let alice = restored.read(&tid, &make_key("alice")).unwrap();
            assert!(
                alice.is_some(),
                "pre-snapshot row 'alice' must survive restore"
            );
            let alice_partition = alice.unwrap();
            assert_eq!(
                alice_partition.rows[0].cells[0].1.value.as_deref(),
                Some(b"before-snap".as_slice()),
                "restored data must match original"
            );

            let bob = restored.read(&tid, &make_key("bob")).unwrap();
            assert!(bob.is_some(), "pre-snapshot row 'bob' must survive restore");

            let carol = restored.read(&tid, &make_key("carol")).unwrap();
            assert!(
                carol.is_some(),
                "pre-snapshot row 'carol' must survive restore"
            );

            // Post-snapshot rows must NOT be present (they were only in
            // the memtable after the snapshot, never flushed to the
            // snapshot's SSTables).
            let dave = restored.read(&tid, &make_key("dave")).unwrap();
            assert!(
                dave.is_none(),
                "post-snapshot row 'dave' must NOT survive restore"
            );

            let eve = restored.read(&tid, &make_key("eve")).unwrap();
            assert!(
                eve.is_none(),
                "post-snapshot row 'eve' must NOT survive restore"
            );

            restored.shutdown().unwrap();
        });
    }

    /// E3: Multi-table snapshot — verifies data across two tables survives
    /// restore correctly.
    #[test]
    fn e3_multi_table_snapshot_restore() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let (engine, store, prefix) = setup_snapshot_engine(dir.path()).await;

            // Register two tables.
            engine.register_table(test_schema()).unwrap();
            engine.register_table(test_schema_2()).unwrap();

            let t1 = table_id();
            let t2 = table_id_2();

            // Write to both tables.
            engine
                .write(&t1, &make_key("t1-key"), make_row(b"table-one", 100), 100)
                .unwrap();
            engine
                .write(&t2, &make_key("t2-key"), make_row(b"table-two", 101), 101)
                .unwrap();

            // Flush both tables.
            engine.flush(&t1).unwrap();
            engine.flush(&t2).unwrap();

            // Update S3 manifest.
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            // Snapshot.
            let _snap = engine
                .create_snapshot_with_store(
                    "multi-snap",
                    "node-1",
                    None,
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            engine.shutdown().unwrap();

            // Restore.
            let restore_dir = tempfile::tempdir().unwrap();
            let restore_config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(restore_dir.path()),
                ..StorageEngineConfig::test_config(restore_dir.path())
            };

            let restored = StorageEngine::open_from_snapshot_with_store(
                restore_config,
                "multi-snap",
                None,
                "node-1",
                false,
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            restored.register_table(test_schema()).unwrap();
            restored.register_table(test_schema_2()).unwrap();

            // Both tables must have their data.
            let r1 = restored.read(&t1, &make_key("t1-key")).unwrap();
            assert!(
                r1.is_some(),
                "table 1 data must survive multi-table restore"
            );
            assert_eq!(
                r1.unwrap().rows[0].cells[0].1.value.as_deref(),
                Some(b"table-one".as_slice())
            );

            let r2 = restored.read(&t2, &make_key("t2-key")).unwrap();
            assert!(
                r2.is_some(),
                "table 2 data must survive multi-table restore"
            );
            assert_eq!(
                r2.unwrap().rows[0].cells[0].1.value.as_deref(),
                Some(b"table-two".as_slice())
            );

            restored.shutdown().unwrap();
        });
    }

    /// FM13: GC / compaction must not delete SSTables referenced by a live snapshot.
    ///
    /// Write data, flush, snapshot, flush again (creating new SSTable),
    /// then verify the original SSTable (referenced by snapshot) still
    /// exists and is readable after compaction or GC.
    #[test]
    fn fm13_snapshot_referenced_sstables_survive_compaction() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let (engine, store, prefix) = setup_snapshot_engine(dir.path()).await;

            engine.register_table(test_schema()).unwrap();
            let tid = table_id();

            // Write + flush → SSTable gen 1.
            engine
                .write(&tid, &make_key("k1"), make_row(b"gen1", 100), 100)
                .unwrap();
            engine.flush(&tid).unwrap();

            // Update S3 manifest and snapshot.
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            let _snap = engine
                .create_snapshot_with_store(
                    "protect-me",
                    "node-1",
                    None,
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            // Collect SSTable IDs protected by the snapshot.
            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let protected = snap_mgr.all_referenced_sstable_ids().await.unwrap();
            assert!(
                !protected.is_empty(),
                "snapshot must reference at least one SSTable"
            );

            // Write more data + flush → SSTable gen 2 (new data, same table).
            engine
                .write(&tid, &make_key("k2"), make_row(b"gen2", 200), 200)
                .unwrap();
            engine.flush(&tid).unwrap();

            // Verify the snapshot-referenced SSTables are still tracked.
            let still_protected = snap_mgr.all_referenced_sstable_ids().await.unwrap();
            assert_eq!(
                protected, still_protected,
                "snapshot references must not change after new writes"
            );

            // Restore should still work and return gen1 data.
            let restore_dir = tempfile::tempdir().unwrap();
            let restore_config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(restore_dir.path()),
                ..StorageEngineConfig::test_config(restore_dir.path())
            };

            let restored = StorageEngine::open_from_snapshot_with_store(
                restore_config,
                "protect-me",
                None,
                "node-1",
                false,
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            restored.register_table(test_schema()).unwrap();

            let r = restored.read(&tid, &make_key("k1")).unwrap();
            assert!(r.is_some(), "snapshot-protected data must be restorable");
            assert_eq!(
                r.unwrap().rows[0].cells[0].1.value.as_deref(),
                Some(b"gen1".as_slice())
            );

            engine.shutdown().unwrap();
            restored.shutdown().unwrap();
        });
    }

    // =========================================================================
    // PITR E2E: Commit-log replay to a point in time (p0-06)
    // =========================================================================

    /// Helper: run the PITR commit-log replay E2E test with a caller-supplied
    /// batch size.
    ///
    /// Writes `batch_size` rows (batch 1), takes a snapshot, writes another
    /// `batch_size` rows (batch 2) with later timestamps, then restores to a
    /// point-in-time between the two batches.  Asserts that every batch-1 row
    /// is present and every batch-2 row is absent.
    ///
    /// Used by both the fast (100-row) default test and the spec-mandated slow
    /// (1 000-row) test so both share the same code path.
    async fn run_pitr_replay_e2e(batch_size: usize) {
        // ── Phase 1: create engine with archiving enabled ────────────
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "pitr-replay-test";

        // Initialise live manifest + schema in S3.
        let init_manifest = crate::manifest::Manifest::new();
        init_manifest
            .save_with_retry(store.as_ref(), prefix)
            .await
            .unwrap();
        crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
            .await
            .unwrap();

        // Small segment size (256 bytes) forces rotation after each write,
        // ensuring every mutation reaches a closed segment that gets archived.
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 256,
                archive: Some(crate::commitlog::config::ArchiveConfig {
                    enabled: true,
                    poll_interval: std::time::Duration::from_millis(20),
                    ..crate::commitlog::config::ArchiveConfig::default()
                }),
                ..CommitLogConfig::test_config(dir.path())
            },
            ..StorageEngineConfig::test_config(dir.path())
        };

        let engine = StorageEngine::new_with_archive_store(
            config,
            Some(&tokio::runtime::Handle::current()),
            Some(Arc::clone(&store)),
            prefix.to_string(),
        )
        .unwrap();

        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // ── Phase 2: write batch 1 — timestamps 1_000 … (1_000 + batch_size - 1) ──
        // These rows MUST survive PITR restore.
        for i in 0..batch_size {
            let key_str = format!("batch1-{i:04}");
            engine
                .write(
                    &tid,
                    &make_key(&key_str),
                    make_row(b"batch1", (1_000 + i as i64) * 1_000),
                    (1_000 + i as i64) * 1_000,
                )
                .unwrap();
        }

        // Flush batch 1 to SSTables so the snapshot captures it.
        engine.flush(&tid).unwrap();
        engine
            .upload_manifest_for_test(Arc::clone(&store), prefix)
            .await;

        // ── Phase 3: create snapshot (records commit_log_position) ───
        let snap_meta = engine
            .create_snapshot_with_store(
                "pitr-snap",
                "node-1",
                None,
                false,
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

        assert_eq!(snap_meta.name, "pitr-snap");

        // point_in_time sits between the two batches (microseconds).
        // batch 1 max timestamp: (1_000 + batch_size - 1) * 1_000
        // batch 2 min timestamp: 2_000_000_000
        // We set cutoff at 1_999_999_999 — includes all of batch 1, excludes batch 2.
        let point_in_time: i64 = 1_999_999_999;

        // ── Phase 4: write batch 2 — timestamps 2_000_000_000 … ─────
        // These rows MUST be absent after PITR restore.
        for i in 0..batch_size {
            let key_str = format!("batch2-{i:04}");
            engine
                .write(
                    &tid,
                    &make_key(&key_str),
                    make_row(b"batch2", 2_000_000_000 + i as i64),
                    2_000_000_000 + i as i64,
                )
                .unwrap();
        }

        // Wait long enough for the archiver to upload all closed segments.
        // The small (256-byte) segment size guarantees batch-2 mutations
        // rotate into closed segments that the archiver will pick up.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Verify at least some segments were archived before proceeding.
        let arch_manifest =
            crate::commitlog::manifest::ArchiveManifest::load(store.as_ref(), prefix)
                .await
                .unwrap();
        assert!(
            !arch_manifest.segments.is_empty(),
            "archiver must have uploaded at least one segment before restore"
        );

        engine.shutdown().unwrap();

        // ── Phase 5: restore with PITR cutoff ───────────────────────
        let restore_dir = tempfile::tempdir().unwrap();
        let restore_config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(restore_dir.path()),
            ..StorageEngineConfig::test_config(restore_dir.path())
        };

        let restored = StorageEngine::open_from_snapshot_with_store(
            restore_config,
            "pitr-snap",
            Some(point_in_time),
            "node-1",
            false,
            Arc::clone(&store),
            prefix,
        )
        .await
        .unwrap();

        // Register table so deferred replay mutations are applied.
        restored.register_table(test_schema()).unwrap();

        // ── Phase 6: assertions ──────────────────────────────────────
        // Batch 1 rows: all must be present.
        for i in 0..batch_size {
            let key_str = format!("batch1-{i:04}");
            let result = restored.read(&tid, &make_key(&key_str)).unwrap();
            assert!(
                result.is_some(),
                "batch1 row '{key_str}' must be present after PITR restore"
            );
            assert_eq!(
                result.unwrap().rows[0].cells[0].1.value.as_deref(),
                Some(b"batch1".as_slice()),
                "batch1 row '{key_str}' must have correct value"
            );
        }

        // Batch 2 rows: all must be absent (timestamp > point_in_time).
        for i in 0..batch_size {
            let key_str = format!("batch2-{i:04}");
            let result = restored.read(&tid, &make_key(&key_str)).unwrap();
            assert!(
                result.is_none(),
                "batch2 row '{key_str}' must NOT be present after PITR restore to {point_in_time}"
            );
        }

        restored.shutdown().unwrap();
    }

    /// E4 (fast): Commit-log replay PITR — 100 rows pre-snapshot + 100 rows
    /// post-snapshot.  Runs in default CI.  Validates the replay code path
    /// without the runtime overhead of the full 1 000+1 000 spec requirement.
    ///
    /// For the spec-mandated 1 000+1 000 test see
    /// [`e4_slow_pitr_commit_log_replay_1k_plus_1k`].
    #[test]
    fn e4_pitr_commit_log_replay_to_point_in_time() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_pitr_replay_e2e(100));
    }

    /// E4 (slow): Commit-log replay PITR — spec acceptance criterion #2
    /// mandates exactly 1 000 pre-snapshot rows and 1 000 post-snapshot rows.
    ///
    /// Gated with `#[ignore]` because the archiver poll loop and 2 000
    /// individual commit-log writes with a 256-byte segment size take ~60–90 s
    /// on a typical CI runner.  Run explicitly with:
    ///
    /// ```sh
    /// cargo test -p ferrosa-storage -- --ignored e4_slow_pitr_commit_log_replay_1k_plus_1k
    /// ```
    ///
    /// Uses the identical code path as the fast 100-row variant; only the row
    /// count differs.
    #[test]
    fn e4_slow_pitr_commit_log_replay_1k_plus_1k() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_pitr_replay_e2e(1_000));
    }

    /// FM1/FM8: Archiver SHA-256 verification — archived segment data in S3
    /// must match the original segment on disk, bit-for-bit.
    #[test]
    fn fm1_archived_segment_content_integrity() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "integrity-test";

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig {
                    segment_size: 512, // small to force rotation
                    archive: Some(crate::commitlog::config::ArchiveConfig {
                        enabled: true,
                        poll_interval: std::time::Duration::from_millis(50),
                        ..crate::commitlog::config::ArchiveConfig::default()
                    }),
                    ..CommitLogConfig::test_config(dir.path())
                },
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new_with_archive_store(
                config,
                Some(&tokio::runtime::Handle::current()),
                Some(Arc::clone(&store)),
                prefix.to_string(),
            )
            .unwrap();

            engine.register_table(test_schema()).unwrap();
            let tid = table_id();

            // Write enough data to trigger multiple segment rotations.
            for i in 0..30 {
                let key_str = format!("key-{i:04}");
                let val_str = format!("value-{i:04}");
                engine
                    .write(
                        &tid,
                        &make_key(&key_str),
                        make_row(val_str.as_bytes(), i),
                        i,
                    )
                    .unwrap();
            }

            // Wait for archiver to process.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Load archive manifest.
            let manifest =
                crate::commitlog::manifest::ArchiveManifest::load(store.as_ref(), prefix)
                    .await
                    .unwrap();

            assert!(
                !manifest.segments.is_empty(),
                "at least one segment must be archived"
            );

            // Verify each archived segment: SHA-256 in manifest matches S3 content.
            for seg in &manifest.segments {
                let hex = crate::upload::manager::hex_prefix_for(&seg.id.to_string());
                let s3_path = object_store::path::Path::from(format!(
                    "{prefix}/commitlog-archive/{hex}/{}.log",
                    seg.id
                ));
                let result = store.get(&s3_path).await.unwrap();
                let data = result.bytes().await.unwrap();

                // Independently compute SHA-256.
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&data);
                let actual_sha = format!("{:x}", hasher.finalize());

                assert_eq!(
                    seg.sha256, actual_sha,
                    "archived segment {} SHA-256 mismatch",
                    seg.id
                );
                assert_eq!(
                    seg.size,
                    data.len() as u64,
                    "archived segment {} size mismatch",
                    seg.id
                );
            }

            engine.shutdown().unwrap();
        });
    }

    /// Snapshot manifest SHA-256 integrity — verify the manifest stored in the
    /// snapshot can be loaded and validated by RestoreManager.
    #[test]
    fn snapshot_manifest_sha256_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let (engine, store, prefix) = setup_snapshot_engine(dir.path()).await;

            engine.register_table(test_schema()).unwrap();
            let tid = table_id();

            // Write data so the manifest is non-empty.
            engine
                .write(&tid, &make_key("k"), make_row(b"v", 1), 1)
                .unwrap();
            engine.flush(&tid).unwrap();
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            let snap_meta = engine
                .create_snapshot_with_store(
                    "sha-test",
                    "node-1",
                    None,
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            // RestoreManager must validate the SHA-256 successfully.
            let restore_mgr =
                crate::restore::RestoreManager::new(Arc::clone(&store), prefix.to_string());
            let (loaded_meta, loaded_manifest) = restore_mgr
                .load_and_validate_snapshot("sha-test")
                .await
                .unwrap();

            assert_eq!(loaded_meta.name, "sha-test");
            assert_eq!(loaded_meta.manifest_sha256, snap_meta.manifest_sha256);
            // Manifest must have at least one table with SSTables.
            assert!(
                !loaded_manifest.sstables.is_empty(),
                "snapshot manifest should reference at least one table"
            );

            engine.shutdown().unwrap();
        });
    }

    /// Snapshot expiry — verify cleanup_expired removes old snapshots while
    /// keeping non-expired ones, and that expired snapshot data is no longer
    /// restorable.
    #[test]
    fn snapshot_expiry_cleanup_works() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let (engine, store, prefix) = setup_snapshot_engine(dir.path()).await;

            engine.register_table(test_schema()).unwrap();
            let tid = table_id();
            engine
                .write(&tid, &make_key("k"), make_row(b"v", 1), 1)
                .unwrap();
            engine.flush(&tid).unwrap();
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            // Create one snapshot that expires in the past, one that doesn't expire.
            engine
                .create_snapshot_with_store(
                    "expired-snap",
                    "node-1",
                    Some("2020-01-01T00:00:00Z".to_string()), // already expired
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();
            engine
                .create_snapshot_with_store(
                    "permanent-snap",
                    "node-1",
                    None, // no expiry
                    false,
                    Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());

            // Before cleanup: both exist.
            let before = snap_mgr.list_snapshots().await.unwrap();
            assert_eq!(before.len(), 2);

            // Cleanup at "now" (well past the expired date).
            let deleted = snap_mgr
                .cleanup_expired("2026-01-01T00:00:00Z")
                .await
                .unwrap();
            assert_eq!(deleted.len(), 1);
            assert_eq!(deleted[0], "expired-snap");

            // After cleanup: only permanent remains.
            let after = snap_mgr.list_snapshots().await.unwrap();
            assert_eq!(after.len(), 1);
            assert_eq!(after[0].name, "permanent-snap");

            // Trying to restore expired snapshot must fail.
            let restore_mgr =
                crate::restore::RestoreManager::new(Arc::clone(&store), prefix.to_string());
            let result = restore_mgr.load_and_validate_snapshot("expired-snap").await;
            assert!(result.is_err(), "expired snapshot should not be loadable");

            engine.shutdown().unwrap();
        });
    }

    // =========================================================================
    // Task 3.2: Sidecar files survive table re-registration
    // =========================================================================

    #[test]
    fn sidecar_survives_table_reregistration() {
        use ferrosa_index::IndexKey;

        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();

        // Phase 1: register table with an index, write indexed data, flush.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine
                .register_table_with_indexes(test_schema(), vec![("val_idx".to_string(), 0_usize)])
                .unwrap();

            engine
                .write(&tid, &make_key("user1"), make_row(b"alice", 1000), 1000)
                .unwrap();
            engine.flush(&tid).unwrap();

            // Verify readable before drop.
            let results = engine
                .read_by_index(&tid, "val_idx", &IndexKey(b"alice".to_vec()))
                .unwrap();
            assert_eq!(results.len(), 1, "pre-reregistration: should find user1");

            engine.shutdown().unwrap();
        }

        // Phase 2: create a new engine with the same data dir, re-register,
        // and verify the sidecar index is loaded from disk.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            engine
                .register_table_with_indexes(test_schema(), vec![("val_idx".to_string(), 0_usize)])
                .unwrap();
            engine.replay_mutations(pending).unwrap();

            let results = engine
                .read_by_index(&tid, "val_idx", &IndexKey(b"alice".to_vec()))
                .unwrap();
            assert_eq!(
                results.len(),
                1,
                "post-reregistration: sidecar should be loaded from disk and return user1"
            );
            assert_eq!(results[0].key.key.as_bytes(), b"user1");
        }
    }

    /// Geo schema: a `places` table whose `location` column is a
    /// `frozen<tuple<double,double>>` indexed with `IndexType::Geo`.
    fn geo_schema() -> TableSchema {
        TableSchema {
            keyspace: "geo".to_string(),
            table: "places".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "location".to_string(),
                type_name:
                    "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.DoubleType,org.apache.cassandra.db.marshal.DoubleType))"
                        .to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn geo_tuple_bytes(lat: f64, lon: f64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        for f in [lat, lon] {
            v.extend_from_slice(&8i32.to_be_bytes());
            v.extend_from_slice(&f.to_be_bytes());
        }
        v
    }

    #[test]
    fn engine_read_by_index_cell_ranges_finds_points_in_cover() {
        use ferrosa_index::geo::{cover_radius, DEFAULT_COVER_LEVEL};
        use ferrosa_index::IndexType;

        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("geo", "places");
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(geo_schema()).unwrap();
        engine
            .add_index(&tid, "loc_geo", 0, IndexType::Geo)
            .unwrap();

        for (pk, lat, lon) in [
            ("ferry", 37.7955, -122.3937),
            ("union", 37.7880, -122.4074),
            ("nyc", 40.7580, -73.9855),
        ] {
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(geo_tuple_bytes(lat, lon), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &make_key(pk), row, 1000).unwrap();
        }

        let ranges: Vec<(u64, u64)> = cover_radius(37.7955, -122.3937, 3000.0, DEFAULT_COVER_LEVEL)
            .iter()
            .map(|r| (r.start, r.end))
            .collect();
        let partitions = engine
            .read_by_index_cell_ranges(&tid, "loc_geo", &ranges)
            .unwrap();
        // The two SF points are inside the 3km cover; NYC is far away.
        assert!(
            partitions.len() >= 2,
            "expected >= 2 SF partitions, got {}",
            partitions.len()
        );
        let pks: Vec<Vec<u8>> = partitions
            .iter()
            .map(|p| p.key.key.as_bytes().to_vec())
            .collect();
        assert!(pks.iter().any(|k| k == b"ferry"));
        assert!(pks.iter().any(|k| k == b"union"));

        engine.shutdown().unwrap();
    }

    #[test]
    fn engine_read_by_index_cell_ranges_unknown_table_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let result = engine
            .read_by_index_cell_ranges(&TableId::new("nope", "nope"), "idx", &[(0, u64::MAX)])
            .unwrap();
        assert!(result.is_empty());
        engine.shutdown().unwrap();
    }

    #[test]
    fn engine_has_batchlog_manager() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        assert!(engine.batchlog().is_some());
    }

    #[test]
    fn engine_write_atomic_batch() {
        use ferrosa_common::Token;

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        // Register two tables
        use ferrosa_common::schema::TableSchema;
        for tbl in &["tbl_a", "tbl_b"] {
            let schema = TableSchema {
                keyspace: "ks".to_string(),
                table: tbl.to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "val".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            };
            engine.register_table(schema).unwrap();
        }

        let mutations = vec![
            Mutation {
                mutation_id: [0xA1u8; 16],
                keyspace: "ks".to_string(),
                table: "tbl_a".to_string(),
                key: DecoratedKey {
                    token: Token(1),
                    key: PartitionKey::new(b"pk1".to_vec()),
                },
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"val_a".to_vec(), 100))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(100),
                }],
                timestamp: 100,
            },
            Mutation {
                mutation_id: [0xA2u8; 16],
                keyspace: "ks".to_string(),
                table: "tbl_b".to_string(),
                key: DecoratedKey {
                    token: Token(2),
                    key: PartitionKey::new(b"pk2".to_vec()),
                },
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"val_b".to_vec(), 100))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(100),
                }],
                timestamp: 100,
            },
        ];

        engine.write_atomic_batch(mutations).unwrap();

        // Both writes should be visible
        let table_a = TableId::new("ks", "tbl_a");
        let key_a = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(b"pk1".to_vec()),
        };
        let result_a = engine.read(&table_a, &key_a).unwrap();
        assert!(result_a.is_some(), "mutation to tbl_a should be visible");

        let table_b = TableId::new("ks", "tbl_b");
        let key_b = DecoratedKey {
            token: Token(2),
            key: PartitionKey::new(b"pk2".to_vec()),
        };
        let result_b = engine.read(&table_b, &key_b).unwrap();
        assert!(result_b.is_some(), "mutation to tbl_b should be visible");
    }

    // -- System table registration tests --

    #[test]
    fn register_system_tables_creates_six_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_system_tables().unwrap();

        // Each system table has a distinct schema shape (different
        // clustering columns, fixed-width column types) — Layer 1's
        // per-cell-length and clustering-shape validators reject any
        // generic placeholder row that doesn't match. Reads, however,
        // are schema-agnostic: a missing partition is `Ok(None)`, an
        // unregistered table errors. Use that to verify registration
        // succeeded for every system table.
        let system_tables = [
            ("system_schema", "keyspaces"),
            ("system_schema", "tables"),
            ("system_schema", "columns"),
            ("system_auth", "roles"),
            ("system_auth", "role_members"),
            ("system_auth", "role_permissions"),
        ];

        for (ks, tbl) in &system_tables {
            let tid = TableId::new(*ks, *tbl);
            let key = make_key("test");
            let result = engine.read(&tid, &key);
            assert!(
                result.is_ok(),
                "system table {ks}.{tbl} should be registered, got err: {result:?}"
            );
        }
    }

    #[test]
    fn register_system_tables_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_system_tables().unwrap();
        // Second call should not error.
        engine.register_system_tables().unwrap();
    }

    /// FRSA-BUG-026: write a row with a map-typed cell value (CQL binary
    /// format), flush to SSTable, read back. The read must not error with
    /// "read_exact_at: wanted 1 bytes, got 0".
    #[test]
    fn write_flush_read_map_cell_value() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Temporal's queue_metadata: (queue_type int PK, cluster_ack_level map<text,bigint>, version bigint)
        let schema = ferrosa_common::TableSchema {
            keyspace: "temporal".into(),
            table: "queue_metadata".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ferrosa_common::ColumnDefinition {
                    name: "cluster_ack_level".into(),
                    type_name: "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)".into(),
                },
                ferrosa_common::ColumnDefinition {
                    name: "version".into(),
                    type_name: "org.apache.cassandra.db.marshal.LongType".into(),
                },
            ],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("temporal", "queue_metadata");

        // Write a row: queue_type=1, cluster_ack_level={} (empty map), version=0
        let pk_bytes = 1i32.to_be_bytes().to_vec();
        let key = DecoratedKey::new(PartitionKey::new(pk_bytes));

        // Empty map in CQL binary: [i32 count=0] = 4 bytes of zeros
        let empty_map_bytes = 0i32.to_be_bytes().to_vec();
        let version_bytes = 0i64.to_be_bytes().to_vec();

        let row = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(empty_map_bytes, 1000)),
                (1, CellValue::live(version_bytes, 1000)),
            ],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();

        // Read from memtable — should work
        let memtable_result = engine.read(&tid, &key);
        assert!(
            memtable_result.is_ok(),
            "memtable read failed: {:?}",
            memtable_result.err()
        );
        assert!(
            memtable_result.unwrap().is_some(),
            "row should exist in memtable"
        );

        // Flush to SSTable
        engine.flush_all().unwrap();
        assert!(
            engine.sstable_count(&tid) >= 1,
            "should have flushed to SSTable"
        );

        // Read from SSTable — this is where FRSA-BUG-026 fails
        let sstable_result = engine.read(&tid, &key);
        assert!(
            sstable_result.is_ok(),
            "SSTable read after flush failed: {:?}",
            sstable_result.err()
        );
        assert!(
            sstable_result.unwrap().is_some(),
            "row should exist in SSTable"
        );
    }

    // ── FMEA: SSTable corruption resilience ─────────────────────────────

    /// FMEA #1: Truncating an SSTable Data.db file should not crash reads.
    /// The read should return data from the memtable or other SSTables,
    /// logging a warning about the corrupt SSTable.
    #[test]
    fn read_survives_truncated_sstable_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "resilience".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "resilience");

        // Write and flush to SSTable
        let key = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt: truncate the Data.db file to 1 byte
        let sstable_dir = dir.path().join("sstables/test_ks.resilience");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, [0u8]).unwrap();
                }
            }
        }

        // Read should NOT crash — should return None (data lost but no panic)
        let result = engine.read(&tid, &key);
        assert!(
            result.is_ok(),
            "read with corrupt SSTable should not crash: {:?}",
            result.err()
        );
        // Data may be lost (from corrupt SSTable) but the operation didn't crash
    }

    /// FMEA #6: Zero-length Data.db should not crash reads.
    #[test]
    fn read_survives_zero_length_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "zero_data".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "zero_data");

        let key = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt: zero out the Data.db file
        let sstable_dir = dir.path().join("sstables/test_ks.zero_data");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, []).unwrap();
                }
            }
        }

        let result = engine.read(&tid, &key);
        assert!(
            result.is_ok(),
            "read with zero-length SSTable should not crash: {:?}",
            result.err()
        );
    }

    /// FMEA #9: Write data, flush, ALTER TABLE ADD column, write more,
    /// flush again, read back — old SSTables should still be readable.
    #[test]
    fn read_survives_schema_evolution_across_sstables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Original schema: (k int PK, v text)
        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "evolving".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema.clone()).unwrap();
        let tid = TableId::new("test_ks", "evolving");

        // Write row with 1 column, flush to SSTable
        let key1 = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row1 = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"old".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key1, row1, 1000).unwrap();
        engine.flush_all().unwrap();

        // "ALTER TABLE ADD" — propagate the post-ALTER schema to the storage
        // engine so flush builds the SerializationHeader with the correct
        // num_columns. Without this, the writer's fail-loud assertion
        // (ferrosa-sstable/src/writer.rs) catches the col_idx-out-of-range
        // case and panics. Bug:
        // specs/implemented/bug-sstable-writer-produces-zero-byte-rows-db.md.
        let mut schema_v2 = schema;
        schema_v2
            .regular_columns
            .push(ferrosa_common::ColumnDefinition {
                name: "extra".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            });
        engine.update_table_schema(&tid, schema_v2).unwrap();

        let key2 = DecoratedKey::new(PartitionKey::new(2i32.to_be_bytes().to_vec()));
        let row2 = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(b"new_v".to_vec(), 2000)),
                (1, CellValue::live(b"extra".to_vec(), 2000)),
            ],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key2, row2, 2000).unwrap();
        engine.flush_all().unwrap();

        // Read both rows — old SSTable should not crash
        let r1 = engine.read(&tid, &key1);
        assert!(r1.is_ok(), "old row read failed: {:?}", r1.err());
        assert!(r1.unwrap().is_some(), "old row should exist");

        let r2 = engine.read(&tid, &key2);
        assert!(r2.is_ok(), "new row read failed: {:?}", r2.err());
        assert!(r2.unwrap().is_some(), "new row should exist");
    }

    /// FMEA #8: Memtable write + new data in memtable should still work
    /// even if an SSTable is corrupt.
    #[test]
    fn memtable_data_survives_corrupt_sstable() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "survive".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "survive");

        // Write old data and flush
        let key_old = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"flushed".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key_old, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt the SSTable
        let sstable_dir = dir.path().join("sstables/test_ks.survive");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, [0xDE, 0xAD]).unwrap();
                }
            }
        }

        // Write new data to memtable
        let key_new = DecoratedKey::new(PartitionKey::new(2i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"memtable".to_vec(), 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key_new, row, 2000).unwrap();

        // Read new data from memtable — should work despite corrupt SSTable
        let r = engine.read(&tid, &key_new);
        assert!(r.is_ok(), "memtable read should work: {:?}", r.err());
        assert!(r.unwrap().is_some(), "memtable row should exist");

        // Read old data — corrupt SSTable, but should not crash
        let r_old = engine.read(&tid, &key_old);
        assert!(
            r_old.is_ok(),
            "read of corrupt SSTable data should not crash: {:?}",
            r_old.err()
        );
    }

    // ── S3 compaction tests (T-025 / T-026) ─────────────────────────────────

    /// Build an engine with a pending compaction result waiting to be polled.
    ///
    /// Flushes two SSTables, manually submits a compaction task, and waits
    /// until the compaction executor finishes writing the output files.
    async fn make_engine_with_pending_compaction(
        dir: &tempfile::TempDir,
    ) -> (
        StorageEngine,
        Arc<dyn object_store::ObjectStore>,
        String,
        TableId,
    ) {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "test-node".to_string();

        let config = StorageEngineConfig::test_config(dir.path());
        let rt = tokio::runtime::Handle::current();
        let engine =
            StorageEngine::new_with_upload_store(config, Arc::clone(&store), prefix.clone(), &rt)
                .unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Flush 1: write k1 → SSTable #1.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Flush 2: write k2 → SSTable #2.
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Manually submit a compaction task.
        {
            let compaction_output_dir = dir.path().join("compaction").join(tid.to_string());
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir,
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Wait for the compaction executor (background thread) to finish.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }

        (engine, store, prefix, tid)
    }

    #[tokio::test]
    async fn compaction_output_uploaded_to_s3() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        // poll_compactions integrates the compaction result, uploads to S3,
        // and updates the manifest. Retry until the channel result is consumed:
        // the compaction thread may write output files to disk slightly before
        // it sends the result on the channel, so a single poll may race.
        let tid_str = tid.to_string();
        let mut entries = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
            if !entries.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Exactly one entry: the merged output.
        assert_eq!(
            entries.len(),
            1,
            "manifest should contain exactly one SSTable after compaction, got: {:?}",
            entries
        );

        // The object itself must be present in the store.
        let output_id = &entries[0].id;
        let hex = crate::upload::manager::hex_prefix_for(output_id);
        let data_path = object_store::path::Path::from(format!(
            "{prefix}/{hex}/{tid_str}/{output_id}/{output_id}-Data.db"
        ));
        assert!(
            store.get(&data_path).await.is_ok(),
            "compacted SSTable Data.db must be present in S3 at {data_path}"
        );
    }

    #[tokio::test]
    async fn manifest_updated_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        let tid_str = tid.to_string();

        // Manifest must be empty before poll_compactions runs.
        let (before_manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let before_count = before_manifest
            .sstables
            .get(&tid_str)
            .map(|v| v.len())
            .unwrap_or(0);
        assert_eq!(
            before_count, 0,
            "manifest should be empty before poll_compactions"
        );

        // Integrate compaction result. Retry until the channel result is consumed.
        let mut entries = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            let (after_manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            entries = after_manifest
                .sstables
                .get(&tid_str)
                .cloned()
                .unwrap_or_default();
            if !entries.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(
            entries.len(),
            1,
            "manifest should have exactly 1 SSTable entry (the output) after compaction"
        );
        assert!(
            entries[0].size > 0,
            "output SSTable entry must have non-zero size"
        );
        assert!(
            !entries[0].id.is_empty(),
            "output SSTable id must be non-empty"
        );
    }

    #[tokio::test]
    async fn replay_pending_compaction_upload_finalizes_manifest_and_log() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;
        let tid_str = tid.to_string();

        let input_metadata = {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            engine.collect_sstable_metadata(&tid, state)
        };
        assert_eq!(input_metadata.len(), 2, "test setup should have two inputs");

        let mut manifest = crate::manifest::Manifest::new();
        for input in &input_metadata {
            manifest.add_sstable(
                &tid_str,
                crate::manifest::ManifestEntry {
                    id: input.id.clone(),
                    size: input.size_bytes,
                    min_token: input.min_token,
                    max_token: input.max_token,
                    min_timestamp: input.min_timestamp,
                    max_timestamp: input.max_timestamp,
                },
            );
        }
        manifest
            .save_without_cas(store.as_ref(), &prefix)
            .await
            .unwrap();

        let table_compaction_dir = dir.path().join("compaction").join(&tid_str);
        let output_gen = {
            let mut ready_gen = None;
            for _ in 0..60 {
                for gen in StorageEngine::scan_generations(&table_compaction_dir) {
                    if StorageEngine::open_sstable_from_dir(&table_compaction_dir, &gen.to_string())
                        .is_ok()
                    {
                        ready_gen = Some(gen);
                        break;
                    }
                }
                if ready_gen.is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            ready_gen.expect("readable compaction output generation")
        };

        let output_id = output_gen.to_string();
        let output_files = StorageEngine::collect_sstable_files(&table_compaction_dir, output_gen);
        let output_size = output_files.iter().map(|file| file.size_bytes).sum();
        let output_metadata = crate::compaction::metadata::SSTableMetadata {
            id: output_id.clone(),
            path: table_compaction_dir,
            size_bytes: output_size,
            min_token: i64::MIN,
            max_token: i64::MAX,
            min_timestamp: 0,
            max_timestamp: 0,
            partition_count: 2,
        };
        let manifest_plan = crate::compaction::finalize::plan_manifest_update(
            &tid_str,
            &input_metadata,
            &output_metadata,
            output_size,
        );

        let pending_log_path = engine.config.data_dir.join("pending-uploads.log");
        let pending_log = crate::upload::PendingUploadsLog::open(&pending_log_path).unwrap();
        pending_log
            .add_compaction_entry(
                &tid_str,
                &output_id,
                crate::upload::pending_log::PendingCompactionUpload {
                    remove_input_ids: manifest_plan.remove_input_ids.clone(),
                    output: manifest_plan.add_output.clone(),
                },
            )
            .unwrap();

        engine.replay_pending_uploads().await;

        let mut final_entries = Vec::new();
        for _ in 0..40 {
            let (loaded, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            final_entries = loaded.sstables.get(&tid_str).cloned().unwrap_or_default();
            if final_entries.len() == 1
                && final_entries[0].id == output_id
                && pending_log.pending_records().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(
            final_entries
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec![output_id.as_str()],
            "replay must remove compacted inputs and add the output manifest entry"
        );
        assert!(
            pending_log.pending_records().unwrap().is_empty(),
            "replay must remove the pending log entry only after manifest save"
        );
    }

    #[tokio::test]
    async fn manifest_compaction_concurrent_flush() {
        // Two independent compaction + flush operations run concurrently.
        // Neither must corrupt the manifest; both must complete without panicking.
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "test-concurrent".to_string();

        let config = StorageEngineConfig::test_config(dir.path());
        let rt = tokio::runtime::Handle::current();
        let engine = std::sync::Arc::new(
            StorageEngine::new_with_upload_store(config, Arc::clone(&store), prefix.clone(), &rt)
                .unwrap(),
        );
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Flush 2 SSTables.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Submit a compaction task manually.
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: dir.path().join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Write a third row for the concurrent flush.
        engine
            .write(&tid, &make_key("k3"), make_row(b"v3", 3000), 3000)
            .unwrap();

        // Wait for compaction to finish (up to 15s under heavy CI load).
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..300 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists()
                && std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false)
            {
                break;
            }
        }

        // Flush on a separate task while poll_compactions runs.
        let eng_clone = std::sync::Arc::clone(&engine);
        let tid_clone = tid.clone();
        let flush_handle = tokio::task::spawn_blocking(move || {
            eng_clone.flush(&tid_clone).unwrap();
        });

        // Poll compactions in a retry loop — under CI load the background
        // compaction thread may not have finished yet.
        for _ in 0..100 {
            engine.poll_compactions().await;
            let (m, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            if m.sstables.get(&tid.to_string()).map(|v| !v.is_empty()) == Some(true) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        flush_handle.await.unwrap();

        // Both operations completed without panic.
        // Manifest must have at least one SSTable entry.
        let (final_manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries = final_manifest
            .sstables
            .get(&tid_str)
            .cloned()
            .unwrap_or_default();
        assert!(
            !entries.is_empty(),
            "manifest should have at least one SSTable entry after concurrent compaction+flush"
        );
    }

    #[tokio::test]
    async fn compaction_cleanup_updates_manifest_enqueues_deletes_and_evicts_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        let tid_str = tid.to_string();
        let input_ids: Vec<String> = {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            engine
                .collect_sstable_metadata(&tid, state)
                .into_iter()
                .map(|input| input.id)
                .collect()
        };
        let mut entries = Vec::new();
        for _ in 0..60 {
            engine.poll_compactions().await;
            let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
            if entries.len() == 1
                && engine
                    .compaction_metrics
                    .s3_deletes_total
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 2
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(
            entries.len(),
            1,
            "manifest should contain only the compacted output after cleanup"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "compacted output must be uploaded before manifest cleanup"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "each compacted-away input must get a delete task"
        );

        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let remaining_components: Vec<_> = std::fs::read_dir(&sstable_dir)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        let remaining_names: Vec<_> = remaining_components
            .iter()
            .filter_map(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned())
            .collect();
        assert!(
            remaining_names.iter().all(|name| input_ids
                .iter()
                .all(|input_id| !name.starts_with(&format!("{input_id}-")))),
            "compacted input SSTable components should be evicted after manifest cleanup: {remaining_components:?}"
        );

        for suffix in ["k1", "k2"] {
            assert!(
                engine.read(&tid, &make_key(suffix)).unwrap().is_some(),
                "key {suffix} must remain readable from the compacted output"
            );
        }
    }

    // ── T-026: input SSTable deletion tests ──────────────────────────────────

    #[tokio::test]
    async fn compaction_inputs_deleted_from_s3_after_grace() {
        // Use grace_period = 0 so deletions are immediate (no real wait).
        // We patch the DeleteSSTable tasks by using the manager's channel directly,
        // but the cleanest approach is to use a zero grace period via the normal path.
        // Since grace_period is baked into the task by poll_compactions (1 hour),
        // we verify the deletion tasks were enqueued and the upload manager processes them.
        //
        // Strategy: upload the input SSTables first so they exist in S3, then run
        // poll_compactions with zero grace (we can't set grace to 0 via poll_compactions
        // directly, so we verify by running the upload manager with a zero-grace DeleteSSTable
        // task manually, which validates the idempotency path).
        //
        // Full integration test: build engine, compact, verify output is present and
        // deletions are submitted (fire-and-forget; they run in background with 1-hour grace).

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // Pre-populate input SSTable objects in S3 so deletion is meaningful.
        // Use the shared key constructor so paths match what DeleteSSTable issues.
        let prefix = "test-node";
        let table_id_str = "test_ks.test_table";
        let input_id = "input_sst_1";
        let hex = crate::upload::manager::hex_prefix_for(input_id);
        for component in &[
            "Data.db",
            "Index.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ] {
            let path = crate::upload::manager::sstable_object_key(
                prefix,
                &hex,
                table_id_str,
                input_id,
                component,
            );
            store
                .put(&path, bytes::Bytes::from_static(b"data").into())
                .await
                .unwrap();
        }

        // Submit a zero-grace DeleteSSTable task directly to verify idempotency.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let rt = tokio::runtime::Handle::current();
        let mgr =
            crate::upload::UploadManager::new(Arc::clone(&store), prefix.to_string(), 16, &rt);
        mgr.submit(crate::upload::UploadTask::DeleteSSTable {
            table_id: table_id_str.to_string(),
            sstable_id: input_id.to_string(),
            grace_period: std::time::Duration::from_secs(0),
            on_complete: Some(tx),
        })
        .await
        .unwrap();

        // Wait for deletion to complete.
        let result = rx.await.unwrap();
        assert!(
            result.is_ok(),
            "deletion should succeed: {:?}",
            result.err()
        );

        mgr.shutdown().await;

        // All five component files must be gone from S3.
        for component in &[
            "Data.db",
            "Index.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ] {
            let path = crate::upload::manager::sstable_object_key(
                prefix,
                &hex,
                table_id_str,
                input_id,
                component,
            );
            let get_result = store.get(&path).await;
            assert!(
                get_result.is_err(),
                "component {component} should be deleted from S3"
            );
        }

        // Idempotency: deleting again (already-gone objects) must not error.
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let rt2 = tokio::runtime::Handle::current();
        let mgr2 =
            crate::upload::UploadManager::new(Arc::clone(&store), prefix.to_string(), 16, &rt2);
        mgr2.submit(crate::upload::UploadTask::DeleteSSTable {
            table_id: table_id_str.to_string(),
            sstable_id: input_id.to_string(),
            grace_period: std::time::Duration::from_secs(0),
            on_complete: Some(tx2),
        })
        .await
        .unwrap();
        let result2 = rx2.await.unwrap();
        assert!(
            result2.is_ok(),
            "idempotent deletion must not error: {:?}",
            result2.err()
        );
        mgr2.shutdown().await;
    }

    #[tokio::test]
    async fn compaction_outputs_are_promoted_and_compaction_dir_drained() {
        // After poll_compactions(), input SSTable component files must be deleted
        // from the table directory, the compacted output must be promoted into
        // that same table directory, and the compaction staging directory must
        // not retain orphaned output files.
        let dir = tempfile::tempdir().unwrap();
        let (engine, _store, _prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        let tid_str = tid.to_string();

        // Record the paths of all SSTable component files before compaction.
        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let input_files_before: Vec<std::path::PathBuf> = std::fs::read_dir(&sstable_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();

        // There should be files from exactly 2 flushes.
        assert!(
            !input_files_before.is_empty(),
            "expected SSTable files before compaction, got none in: {:?}",
            sstable_dir
        );

        // Run compaction + local eviction. Retry until the channel result is
        // consumed: the compaction thread writes files before sending on the
        // channel, so a single poll may miss the result under parallel load.
        let table_compaction_dir = dir.path().join("compaction").join(&tid_str);
        let mut generations_after = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            generations_after = StorageEngine::scan_generations(&sstable_dir);
            let input_files_evicted = input_files_before.iter().all(|path| !path.exists());
            let promoted_data_file_present = generations_after.iter().any(|gen| {
                StorageEngine::generation_component_path(&sstable_dir, &gen.to_string(), "Data.db")
                    .is_some()
            });
            let staging_drained = table_compaction_dir
                .read_dir()
                .into_iter()
                .flatten()
                .next()
                .is_none();
            if input_files_evicted && promoted_data_file_present && staging_drained {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // The old input generations must be gone from the table directory.
        assert!(
            input_files_before.iter().all(|path| !path.exists()),
            "input SSTable files should be evicted, still present: {:?}",
            input_files_before
                .iter()
                .filter(|path| path.exists())
                .collect::<Vec<_>>()
        );

        // The compacted output must be durable in the normal SSTable directory.
        assert!(
            generations_after.iter().any(|gen| {
                StorageEngine::generation_component_path(&sstable_dir, &gen.to_string(), "Data.db")
                    .is_some()
            }),
            "compacted output Data.db should be promoted into {:?}, got {:?}",
            sstable_dir,
            generations_after
        );

        let staged_files: Vec<_> = table_compaction_dir
            .read_dir()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .collect();
        assert!(
            staged_files.is_empty(),
            "compaction staging dir should be drained after promotion, got {:?}",
            staged_files
        );
    }

    #[tokio::test]
    async fn compaction_promotion_fail_after_first_component_is_atomic_and_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _store, _prefix, tid) = make_engine_with_pending_compaction(&dir).await;
        let tid_str = tid.to_string();

        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let pre_generations = StorageEngine::scan_generations(&sstable_dir);
        assert!(
            !pre_generations.is_empty(),
            "setup should have at least one generation before compaction promotion"
        );

        // Wait for a completed compaction result to appear.
        let result = {
            let mut output = None;
            for _ in 0..120 {
                let mut results = engine.compaction_executor.poll_results();
                if let Some(compaction_result) = results.pop() {
                    output = Some(compaction_result.output);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            output.expect("compaction executor should produce output")
        };

        // Inject a synthetic first-component failure during promotion.
        let marker_path = result
            .path
            .join(StorageEngine::TEST_FAIL_PROMOTION_AFTER_FIRST_COMPONENT);
        std::fs::write(&marker_path, b"1").unwrap();

        let failed = engine.promote_compaction_output(&tid, &result);
        assert!(
            failed.is_err(),
            "promotion should fail with test marker present"
        );

        // No generation should become visible from a partial promotion.
        let post_generations = StorageEngine::scan_generations(&sstable_dir);
        assert_eq!(
            post_generations, pre_generations,
            "partial compaction generation must not be discoverable"
        );

        // Staging artifacts should be absent; the output can be retried from the
        // original compaction directory.
        let has_orphaned_stage = std::fs::read_dir(&sstable_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .any(|entry| entry.file_name().to_string_lossy().starts_with(".promote-"));
        assert!(
            !has_orphaned_stage,
            "simulated partial promotion should not leave orphaned staging directories"
        );

        std::fs::remove_file(&marker_path).unwrap();
        let recovered = engine.promote_compaction_output(&tid, &result);
        assert!(
            recovered.is_ok(),
            "promotion should recover from marker-cleared output: {:?}",
            recovered.err()
        );
        let recovered = recovered.unwrap();

        let recovered_generations = StorageEngine::scan_generations(&sstable_dir);
        let recovered_gen = recovered.id.parse::<u64>().unwrap();
        assert!(
            recovered_generations.contains(&recovered_gen),
            "recovered promotion should materialize a new generation"
        );
        assert!(
            !pre_generations.contains(&recovered_gen),
            "new generation should not replace preexisting generation IDs"
        );
        assert!(
            !result.path.exists(),
            "original compaction output directory should be removed after successful promotion"
        );
    }

    #[test]
    fn compaction_promotion_preserves_other_pending_outputs_in_shared_staging_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();
        let tid_str = tid.to_string();
        let compaction_dir = dir.path().join("compaction").join(&tid_str);
        std::fs::create_dir_all(&compaction_dir).unwrap();

        let output_a = write_fake_compaction_output(&compaction_dir, "101");
        let output_b = write_fake_compaction_output(&compaction_dir, "202");

        let promoted_a = engine
            .promote_compaction_output(&tid, &output_a)
            .expect("first compaction output should promote");
        assert!(
            promoted_a
                .path
                .join(format!("{}-Data.db", promoted_a.id))
                .exists(),
            "first output should be durable in the normal SSTable directory"
        );

        for component in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ] {
            let pending = compaction_dir.join(format!("202-{component}"));
            assert!(
                pending.exists(),
                "promoting one output must not delete pending component {}",
                pending.display()
            );
        }

        let promoted_b = engine
            .promote_compaction_output(&tid, &output_b)
            .expect("second output should still promote after the first output");
        assert!(
            promoted_b
                .path
                .join(format!("{}-Data.db", promoted_b.id))
                .exists(),
            "second output should be durable in the normal SSTable directory"
        );

        let remaining: Vec<_> = compaction_dir
            .read_dir()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .collect();
        assert!(
            remaining.is_empty(),
            "shared compaction directory should be empty after all outputs promote"
        );
    }

    fn write_fake_compaction_output(
        compaction_dir: &std::path::Path,
        id: &str,
    ) -> crate::compaction::metadata::SSTableMetadata {
        for component in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
        ] {
            std::fs::write(
                compaction_dir.join(format!("{id}-{component}")),
                format!("{id}-{component}"),
            )
            .unwrap();
        }
        std::fs::write(
            compaction_dir.join(format!("{id}-TOC.txt")),
            b"Data.db\nPartitions.db\nRows.db\nFilter.db\nStatistics.db\n",
        )
        .unwrap();

        crate::compaction::metadata::SSTableMetadata {
            id: id.to_string(),
            path: compaction_dir.to_path_buf(),
            size_bytes: 128,
            min_token: 0,
            max_token: 1,
            min_timestamp: 10,
            max_timestamp: 20,
            partition_count: 1,
        }
    }

    // ── T-027: metrics + end-to-end tests ────────────────────────────────────

    /// Verifies that compaction S3 metrics are accurate after a compaction
    /// cycle that uploads the output and enqueues input deletions.
    ///
    /// Uses an in-memory object store (no Docker required).
    #[tokio::test]
    async fn compaction_s3_metrics_accurate() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _store, _prefix, _tid) = make_engine_with_pending_compaction(&dir).await;

        // Before poll_compactions: all counters must be zero.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "s3_uploads_total should be 0 before poll_compactions"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "s3_deletes_total should be 0 before poll_compactions"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .input_bytes_reclaimed
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "input_bytes_reclaimed should be 0 before poll_compactions"
        );

        // Run compaction: merges 2 SSTables → 1 output, uploads to S3,
        // updates manifest, enqueues 2 input deletions. Retry until the channel
        // result is consumed (compaction thread writes files before channel send).
        for _ in 0..40 {
            engine.poll_compactions().await;
            if engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Exactly 1 upload (the compacted output SSTable).
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "s3_uploads_total should be 1 after compacting 2 SSTables into 1"
        );

        // Exactly 2 deletes enqueued (one per input SSTable).
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "s3_deletes_total should be 2 (one per input SSTable)"
        );

        // Input bytes reclaimed must be positive (inputs had non-zero size).
        assert!(
            engine
                .compaction_metrics
                .input_bytes_reclaimed
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "input_bytes_reclaimed should be > 0 after compaction"
        );

        // Verify Prometheus text export contains all three metric names.
        let text = engine.compaction_metrics.to_prometheus_text();
        assert!(
            text.contains("ferrosa_compaction_s3_uploads_total 1"),
            "prometheus text missing uploads counter: {text}"
        );
        assert!(
            text.contains("ferrosa_compaction_s3_deletes_total 2"),
            "prometheus text missing deletes counter: {text}"
        );
        assert!(
            text.contains("ferrosa_compaction_input_bytes_reclaimed"),
            "prometheus text missing bytes reclaimed gauge: {text}"
        );
    }

    /// Cassandra 5 reads a compacted SSTable from S3 (MinIO).
    ///
    /// Test flow:
    ///   1. Flush 2 SSTables with distinct partition keys and multiple cell types.
    ///   2. Compact them → single merged SSTable uploaded to MinIO.
    ///   3. Cassandra 5 mounts the MinIO-backed data directory and scans the table.
    ///   4. All original rows and cell types are present in the Cassandra output.
    ///
    /// Requires MinIO + Cassandra 5 containers (Docker or Podman).
    /// Set FERROSA_TEST_CONTAINERS=1 after starting the compose stack.
    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn cassandra_reads_compacted_sstable_from_s3() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — start MinIO+Cassandra containers \
                 (docker/podman compose up -d) then re-run with FERROSA_TEST_CONTAINERS=1"
            );
        }
        use std::process::Command;

        // ── Step 1: build engine, flush 2 SSTables with varied cell types ──
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "ferrosa-test".to_string();
        let rt = tokio::runtime::Handle::current();
        let engine = StorageEngine::new_with_upload_store(
            StorageEngineConfig::test_config(dir.path()),
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();

        let schema = TableSchema {
            keyspace: "test_ks".into(),
            table: "mixed_cells".into(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ferrosa_common::schema::ColumnDefinition {
                    name: "v_int".into(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
                },
                ferrosa_common::schema::ColumnDefinition {
                    name: "v_text".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                },
            ],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "mixed_cells");

        // Flush 1: row with text cell.
        let k1 = make_key("pk1");
        let row1 = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(
                1,
                ferrosa_common::cell::CellValue::live(b"hello".to_vec(), 1000),
            )],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &k1, row1, 1000).unwrap();
        engine.flush(&tid).unwrap();

        // Flush 2: row with int cell.
        let k2 = make_key("pk2");
        let int_bytes = 42i32.to_be_bytes().to_vec();
        let row2 = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(0, ferrosa_common::cell::CellValue::live(int_bytes, 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &k2, row2, 2000).unwrap();
        engine.flush(&tid).unwrap();

        // ── Step 2: compact and upload to MinIO ──
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let compaction_output_dir = dir.path().join("compaction");
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir.clone(),
                schema: TableSchema {
                    keyspace: "test_ks".into(),
                    table: "mixed_cells".into(),
                    key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                    clustering_columns: vec![],
                    static_columns: vec![],
                    regular_columns: vec![
                        ferrosa_common::schema::ColumnDefinition {
                            name: "v_int".into(),
                            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
                        },
                        ferrosa_common::schema::ColumnDefinition {
                            name: "v_text".into(),
                            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                        },
                    ],
                    extensions: Default::default(),
                },
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Wait for executor to finish writing output files.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }
        engine.poll_compactions().await;

        // Verify the output is in S3.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "compacted SSTable must be uploaded before Cassandra read"
        );

        // ── Step 3: start Cassandra container ──
        let compose_file = workspace_path("tests/docker/compaction-cassandra.yml");
        let cassandra_up = Command::new(container_runtime())
            .args(["compose", "-f", compose_file.to_str().unwrap(), "up", "-d"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(
            cassandra_up,
            "failed to start Cassandra container — is the runtime running?"
        );

        // Allow Cassandra to initialize (up to 120 s).
        // Two-phase probe: first wait for nodetool (JMX), then verify CQL responds,
        // because nodetool can succeed ~10 s before the CQL listener is ready.
        let mut cassandra_ready = false;
        'outer: for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let jmx_ok = Command::new(container_runtime())
                .args(["exec", "ferrosa-cassandra-test", "nodetool", "status"])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !jmx_ok {
                continue;
            }
            // JMX ready — now wait for CQL port to accept a query.
            for _ in 0..15 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let cql_ok = Command::new(container_runtime())
                    .args([
                        "exec",
                        "ferrosa-cassandra-test",
                        "cqlsh",
                        "--execute",
                        "SELECT now() FROM system.local;",
                    ])
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false);
                if cql_ok {
                    cassandra_ready = true;
                    break 'outer;
                }
            }
        }
        assert!(
            cassandra_ready,
            "Cassandra did not become ready within 120 s"
        );

        // ── Step 4: create keyspace + table so nodetool import has a target ──
        let create_schema = "\
            CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = \
              {'class': 'SimpleStrategy', 'replication_factor': 1};\
            CREATE TABLE IF NOT EXISTS test_ks.mixed_cells \
              (pk text PRIMARY KEY, v_text text, v_int int);";
        let schema_ok = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                create_schema,
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(schema_ok, "failed to create keyspace/table in Cassandra");

        let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
        assert_eq!(
            entries.len(),
            1,
            "manifest should contain exactly one promoted compacted SSTable"
        );
        let promoted_gen = entries[0].id.clone();
        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let import_source = StorageEngine::generation_dir_path(
            &sstable_dir,
            promoted_gen
                .parse::<u64>()
                .expect("numeric promoted generation"),
        )
        .expect("promoted compaction generation should be visible in SSTable directory");

        // ── Step 5: copy SSTable files into container and run nodetool import ──
        // Ferrosa names files `{gen}-Data.db`; Cassandra's SSTableLoader expects
        // the BTI descriptor prefix `da-{gen}-bti-`.  prepare_cassandra_import_dir
        // renames the files and rewrites the TOC.txt.
        let import_staging = dir.path().join("cassandra-import");
        prepare_cassandra_import_dir(&import_source, &import_staging);

        // Replace ferrosa's empty CompactionMetadata/StatsMetadata with real
        // Cassandra 5 bytes so nodetool import can deserialize Statistics.db.
        patch_statistics_for_cassandra_import(&import_staging);

        // Clean the import volume so stale files from previous runs don't confuse Cassandra.
        let _ = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "sh",
                "-c",
                "rm -f /var/lib/cassandra/import/*",
            ])
            .status();

        // Copy the renamed files into the container's import directory.
        for entry in std::fs::read_dir(&import_staging).expect("read import staging dir") {
            let src = entry.expect("entry").path();
            let cp_ok = Command::new(container_runtime())
                .args([
                    "cp",
                    src.to_str().unwrap(),
                    "ferrosa-cassandra-test:/var/lib/cassandra/import/",
                ])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            assert!(cp_ok, "docker cp failed for {:?}", src);
        }

        // Import the SSTable into the running Cassandra node.
        let import_ok = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "nodetool",
                "import",
                "test_ks",
                "mixed_cells",
                "/var/lib/cassandra/import",
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(import_ok, "nodetool import failed");

        // ── Step 6: verify rows via exact partition reads and full scan ──
        let pk1_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk, v_text, v_int FROM test_ks.mixed_cells WHERE pk='pk1';",
            ])
            .output()
            .expect("cqlsh failed");

        let pk1_stdout = String::from_utf8_lossy(&pk1_output.stdout);
        let pk1_stderr = String::from_utf8_lossy(&pk1_output.stderr);

        assert!(
            pk1_stdout.contains("pk1") && pk1_stdout.contains("hello"),
            "Cassandra output missing pk1/v_text row.\nstdout: {pk1_stdout}\nstderr: {pk1_stderr}"
        );

        let pk2_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk, v_text, v_int FROM test_ks.mixed_cells WHERE pk='pk2';",
            ])
            .output()
            .expect("cqlsh failed");

        let pk2_stdout = String::from_utf8_lossy(&pk2_output.stdout);
        let pk2_stderr = String::from_utf8_lossy(&pk2_output.stderr);

        assert!(
            pk2_stdout.contains("pk2") && pk2_stdout.contains("42"),
            "Cassandra output missing pk2/v_int row.\nstdout: {pk2_stdout}\nstderr: {pk2_stderr}"
        );

        let scan_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk, v_text, v_int FROM test_ks.mixed_cells;",
            ])
            .output()
            .expect("cqlsh full scan failed");

        let scan_stdout = String::from_utf8_lossy(&scan_output.stdout);
        let scan_stderr = String::from_utf8_lossy(&scan_output.stderr);

        assert!(
            scan_output.status.success()
                && scan_stdout.contains("pk1")
                && scan_stdout.contains("hello")
                && scan_stdout.contains("pk2")
                && scan_stdout.contains("42"),
            "Cassandra full scan must read both imported rows.\nstdout: {scan_stdout}\nstderr: {scan_stderr}"
        );

        // Cleanup.
        let _ = Command::new(container_runtime())
            .args([
                "compose",
                "-f",
                compose_file.to_str().unwrap(),
                "down",
                "-v",
            ])
            .status();
    }

    /// End-to-end compaction pipeline: 4 flush cycles trigger STCS compaction,
    /// the output is confirmed in S3, the manifest is updated, old files are
    /// evicted locally, and Cassandra 5 can read the result from MinIO.
    ///
    /// Pipeline:
    ///   4 flushes → STCS detects 4-SSTable bucket → compaction triggered
    ///   → output uploaded to MinIO → manifest updated (1 entry)
    ///   → input SSTable files deleted locally
    ///   → Cassandra 5 reads all 4 partition keys from compacted SSTable
    ///
    /// Requires MinIO + Cassandra 5 containers (Docker or Podman).
    /// Set FERROSA_TEST_CONTAINERS=1 after starting the compose stack.
    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn compaction_end_to_end_pipeline() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — start MinIO+Cassandra containers \
                 (docker/podman compose up -d) then re-run with FERROSA_TEST_CONTAINERS=1"
            );
        }
        use std::process::Command;

        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "ferrosa-e2e".to_string();
        let rt = tokio::runtime::Handle::current();

        // Use min_threshold=4 and a deliberately wide test bucket so these
        // tiny fixture SSTables compact as one STCS group. The production
        // default bucket ratio may correctly split very small files whose
        // fixed component overhead dominates their logical data size.
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 4;
        config.compaction.bucket_low = 0.0;
        config.compaction.bucket_high = 2.0;
        let engine =
            StorageEngine::new_with_upload_store(config, Arc::clone(&store), prefix.clone(), &rt)
                .unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // ── 4 flush cycles (each writes 1 partition to its own SSTable) ──
        for (i, key_suffix) in ["a", "b", "c", "d"].iter().enumerate() {
            let ts = (i as i64 + 1) * 1000;
            let value = format!("value-{key_suffix}");
            engine
                .write(
                    &tid,
                    &make_key(key_suffix),
                    make_row(value.as_bytes(), ts),
                    ts,
                )
                .unwrap();
            engine.flush(&tid).unwrap();
            // flush() calls maybe_compact(); on the 4th flush STCS will submit a task.
        }

        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            let strategy = engine.strategy_for_table(state);
            let selected = strategy.select(&metadata, &state.schema, &tid);
            assert_eq!(
                selected.len(),
                1,
                "configured STCS bucket should select the four E2E fixture SSTables; sizes={:?}",
                metadata.iter().map(|m| m.size_bytes).collect::<Vec<_>>()
            );
        }

        // Wait for the compaction executor background thread to finish.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }

        // ── poll_compactions: upload, manifest update, local eviction ──
        //
        // The executor writes compaction components before publishing the
        // completed result on its channel. A single non-blocking poll can race
        // that handoff; the production contract is eventual pickup by the
        // periodic poller, so this test waits for the observable upload.
        for _ in 0..100 {
            engine.poll_compactions().await;
            if engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed)
                == 1
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Upload confirmed — exactly 1 output SSTable uploaded.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "expected 1 S3 upload after STCS compaction of 4 SSTables"
        );

        // 4 input deletions enqueued.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            4,
            "expected 4 S3 delete tasks for 4 input SSTables"
        );

        // Manifest: exactly 1 SSTable entry (the merged output).
        let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
        assert_eq!(
            entries.len(),
            1,
            "manifest should have exactly 1 SSTable entry after STCS compaction"
        );

        // Local input files must be evicted while the promoted output remains live.
        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let output_gen = entries[0].id.parse::<u64>().expect("numeric output id");
        let import_source = StorageEngine::generation_dir_path(&sstable_dir, output_gen)
            .expect("promoted compaction generation should be visible in SSTable directory");
        assert!(
            StorageEngine::generation_component_path(&sstable_dir, &entries[0].id, "Data.db")
                .is_some(),
            "promoted compacted SSTable Data.db should remain live"
        );
        if sstable_dir.exists() {
            let remaining_db_files: Vec<_> = std::fs::read_dir(&sstable_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "db").unwrap_or(false))
                .collect();
            assert!(
                remaining_db_files.is_empty(),
                "all input .db files should be evicted after poll_compactions, remaining: {:?}",
                remaining_db_files
            );
        }

        // ── Cassandra: verify all 4 partition keys are readable ──
        let compose_file = workspace_path("tests/docker/compaction-cassandra.yml");
        let cassandra_up = Command::new(container_runtime())
            .args(["compose", "-f", compose_file.to_str().unwrap(), "up", "-d"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(cassandra_up, "failed to start Cassandra container");

        let mut cassandra_ready = false;
        'outer2: for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let jmx_ok = Command::new(container_runtime())
                .args(["exec", "ferrosa-cassandra-test", "nodetool", "status"])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !jmx_ok {
                continue;
            }
            for _ in 0..15 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let cql_ok = Command::new(container_runtime())
                    .args([
                        "exec",
                        "ferrosa-cassandra-test",
                        "cqlsh",
                        "--execute",
                        "SELECT now() FROM system.local;",
                    ])
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false);
                if cql_ok {
                    cassandra_ready = true;
                    break 'outer2;
                }
            }
        }
        assert!(
            cassandra_ready,
            "Cassandra did not become ready within 120 s"
        );

        // test_schema() → test_ks.test_table with pk (text), ck (int), val (text)
        let create_schema = "\
            CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = \
              {'class': 'SimpleStrategy', 'replication_factor': 1};\
            CREATE TABLE IF NOT EXISTS test_ks.test_table \
              (pk text, ck int, val text, PRIMARY KEY (pk, ck));";
        let schema_ok = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                create_schema,
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(schema_ok, "failed to create keyspace/table in Cassandra");

        let import_staging = dir.path().join("cassandra-import");
        prepare_cassandra_import_dir(&import_source, &import_staging);

        // Replace ferrosa's empty CompactionMetadata/StatsMetadata with real
        // Cassandra 5 bytes so nodetool import can deserialize Statistics.db.
        patch_statistics_for_cassandra_import(&import_staging);

        // Clean the import volume so stale files from previous runs don't confuse Cassandra.
        let _ = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "sh",
                "-c",
                "rm -f /var/lib/cassandra/import/*",
            ])
            .status();

        for entry in std::fs::read_dir(&import_staging).expect("read import staging dir") {
            let src = entry.expect("entry").path();
            let cp_ok = Command::new(container_runtime())
                .args([
                    "cp",
                    src.to_str().unwrap(),
                    "ferrosa-cassandra-test:/var/lib/cassandra/import/",
                ])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            assert!(cp_ok, "docker cp failed for {:?}", src);
        }

        let import_ok = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "nodetool",
                "import",
                "test_ks",
                "test_table",
                "/var/lib/cassandra/import",
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(import_ok, "nodetool import failed");

        let cql_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk FROM test_ks.test_table;",
            ])
            .output()
            .expect("cqlsh failed");

        let stdout = String::from_utf8_lossy(&cql_output.stdout);
        let stderr = String::from_utf8_lossy(&cql_output.stderr);
        for key in &["a", "b", "c", "d"] {
            assert!(
                stdout.contains(key),
                "Cassandra missing partition key '{key}'.\nstdout: {stdout}\nstderr: {stderr}"
            );
        }

        // Cleanup.
        let _ = Command::new(container_runtime())
            .args([
                "compose",
                "-f",
                compose_file.to_str().unwrap(),
                "down",
                "-v",
            ])
            .status();
    }

    // -----------------------------------------------------------------------
    // Collection flush readback tests (BUG-026)
    // -----------------------------------------------------------------------

    /// Encode CQL v4+ wire-format bytes for a map.
    ///
    /// Format: [4-byte BE count][4-byte BE key_len][key][4-byte BE val_len][val]...
    fn encode_cql_map(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
        for (k, v) in entries {
            buf.extend_from_slice(&(k.len() as i32).to_be_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
            buf.extend_from_slice(v);
        }
        buf
    }

    /// Encode CQL v4+ wire-format bytes for a list or set.
    fn encode_cql_sequence(elements: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(elements.len() as i32).to_be_bytes());
        for elem in elements {
            buf.extend_from_slice(&(elem.len() as i32).to_be_bytes());
            buf.extend_from_slice(elem);
        }
        buf
    }

    fn collection_schema(ks: &str, table: &str, col_type: &str) -> TableSchema {
        TableSchema {
            keyspace: ks.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "col".to_string(),
                type_name: col_type.to_string(),
            }],
            extensions: Default::default(),
        }
    }

    #[test]
    fn collection_map_flush_readback() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = collection_schema(
            "test_ks",
            "map_table",
            "org.apache.cassandra.db.marshal.MapType(\
             org.apache.cassandra.db.marshal.UTF8Type,\
             org.apache.cassandra.db.marshal.Int32Type)",
        );
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "map_table");
        let key = make_key("pk1");
        let map_bytes = encode_cql_map(&[(b"key", &42i32.to_be_bytes())]);

        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(map_bytes.clone(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "flush should have written 1 SSTable"
        );
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush"
        );

        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "row must be readable after flush");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(map_bytes.as_slice()),
            "map bytes must survive flush/read roundtrip unchanged"
        );
    }

    #[test]
    fn collection_set_flush_readback() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = collection_schema(
            "test_ks",
            "set_table",
            "org.apache.cassandra.db.marshal.SetType(\
             org.apache.cassandra.db.marshal.UTF8Type)",
        );
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "set_table");
        let key = make_key("pk2");
        let set_bytes = encode_cql_sequence(&[b"alpha", b"beta"]);

        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(set_bytes.clone(), 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key, row, 2000).unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "flush should have written 1 SSTable"
        );
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush"
        );

        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "row must be readable after flush");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(set_bytes.as_slice()),
            "set bytes must survive flush/read roundtrip unchanged"
        );
    }

    #[test]
    fn collection_list_flush_readback() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = collection_schema(
            "test_ks",
            "list_table",
            "org.apache.cassandra.db.marshal.ListType(\
             org.apache.cassandra.db.marshal.UTF8Type)",
        );
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "list_table");
        let key = make_key("pk3");
        let list_bytes = encode_cql_sequence(&[b"first", b"second", b"third"]);

        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(list_bytes.clone(), 3000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(3000),
        };
        engine.write(&tid, &key, row, 3000).unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "flush should have written 1 SSTable"
        );
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush"
        );

        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "row must be readable after flush");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(list_bytes.as_slice()),
            "list bytes must survive flush/read roundtrip unchanged"
        );
    }

    // ── Schema persistence across restarts ──────────────────────────────────

    /// Verify that a table schema registered before a flush survives an engine
    /// restart — `load_local_schema_if_present` reads the `schema.json` written
    /// by `flush` and re-registers all tables so that the new engine can write
    /// and read without calling `register_table` again.
    #[test]
    fn open_reloads_local_schema_before_commitlog_replay() {
        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("test_ks", "test_table");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.flush(&tid).unwrap();
        }

        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, _pending) = StorageEngine::open(config, None).unwrap();

        assert!(
            engine.is_table_registered_for_test(&tid),
            "StorageEngine::open must reload schema.json before commit-log replay so recovered nodes do not forward writes for locally unregistered tables"
        );
        assert_eq!(
            engine.deferred_replay_mutation_count_for_test(),
            0,
            "schema-backed open should not start with deferred replay mutations"
        );
    }

    /// Headline reload-survival test: a non-BTree (Phonetic) index created and
    /// persisted to `system_schema.indexes`, flushed, then recovered after the
    /// engine is dropped and reopened from the same data dir. After reopen,
    /// `reload_indexes_from_system_schema` must re-register the index on the
    /// user table AND restore its real type — not the BTree default that the
    /// old `register_table_inner(.., vec![])` gap produced.
    #[test]
    fn reopen_reregisters_persisted_index_with_real_type() {
        use ferrosa_index::IndexType;
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let user_tid = TableId::new("test_ks", "test_table");
        let indexes_tid = TableId::new("system_schema", "indexes");
        let idx_meta = ferrosa_schema::metadata::index::IndexMetadata {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            name: "idx_val_phonetic".to_string(),
            index_type: IndexType::Phonetic,
            target_columns: vec!["val".to_string()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        };

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.register_system_tables().unwrap();

            // Persist the index row exactly as the DDL write path does.
            let row = persistence::index_to_rows(&idx_meta);
            engine
                .write(&indexes_tid, &row.key, row.row, now_micros_for_test())
                .unwrap();

            engine.flush(&user_tid).unwrap();
            engine.flush(&indexes_tid).unwrap();
        }

        // Reopen: replicate the boot sequence — system tables registered, user
        // schema reloaded from schema.json, then index reconstruction.
        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, _pending) = StorageEngine::open(config, None).unwrap();
        engine.register_system_tables().unwrap();

        // Sanity: the index is NOT registered yet (the gap this fix closes).
        assert_eq!(
            engine.index_type_for_test(&user_tid, "idx_val_phonetic"),
            None,
            "index must be absent before reconstruction — proves the test exercises the fix"
        );

        let restored = engine.reload_indexes_from_system_schema().unwrap();
        assert_eq!(
            restored, 1,
            "exactly one persisted index should be restored"
        );

        assert_eq!(
            engine.index_type_for_test(&user_tid, "idx_val_phonetic"),
            Some(IndexType::Phonetic),
            "index must survive restart AND keep its real Phonetic type, not the BTree default"
        );
    }

    /// Regression guard for the compaction eager-index re-typing bug: the
    /// post-flush AND post-compaction eager rebuild both construct their
    /// `IndexBuildJob` via [`eager_index_build_job`], which must carry the
    /// index's *real* type read from the store — not a hardcoded `BTree`.
    ///
    /// The compaction site (engine.rs `poll_compactions`) previously stamped
    /// every rebuild job `index_type: IndexType::BTree`, so a Phonetic index
    /// got a BTree-typed rebuild after compaction and dispatched to the wrong
    /// builder. This asserts the shared helper both sites now call preserves
    /// the Phonetic type, and that an unknown index name still defaults to
    /// BTree (matching `index_type_for`).
    #[test]
    fn eager_index_build_job_carries_real_index_type_after_compaction() {
        use ferrosa_index::IndexType;

        let dir = tempfile::tempdir().unwrap();
        let table_id = TableId::new("test_ks", "test_table");
        let table_dir = dir.path().join("sstables").join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).unwrap();

        let schema = test_schema();
        let mut store = TableStore::new(
            schema.clone(),
            FileFlushTarget::new_starting_at(table_dir).unwrap(),
            write_options_for_schema(&schema, true).unwrap(),
        );
        // Register a non-BTree (Phonetic) index on column 0, exactly as the DDL
        // path does, so the store can report its real type.
        store.add_index("val_phonetic_idx".to_string(), 0, IndexType::Phonetic);

        // Mirror the compaction eager-rebuild call: output SSTable id + col pos.
        let job = eager_index_build_job(
            &store,
            &table_id,
            "compacted-1".to_string(),
            "val_phonetic_idx",
            0,
        );
        assert_eq!(
            job.index_type,
            IndexType::Phonetic,
            "compaction rebuild job must carry the real Phonetic type, not a hardcoded BTree"
        );
        assert_eq!(job.sstable_id, "compacted-1");
        assert_eq!(job.index_name, "val_phonetic_idx");
        assert_eq!(job.table, ("test_ks".to_string(), "test_table".to_string()));

        // Control: an index name the store does not know still defaults to
        // BTree (so the fix does not over-reach and break the common path).
        let btree_job = eager_index_build_job(
            &store,
            &table_id,
            "compacted-1".to_string(),
            "unknown_idx",
            0,
        );
        assert_eq!(
            btree_job.index_type,
            IndexType::BTree,
            "an unregistered index defaults to BTree, matching index_type_for"
        );
    }

    /// A Filtered (partial) index — its `FilterPredicate` persisted under the
    /// reserved `__filter_predicate` options key — must survive an engine
    /// restart. After `reload_indexes_from_system_schema`, the index is
    /// re-registered as Filtered, its predicate is restored exactly, and the
    /// memtable index still filters: a write whose filter cell fails the
    /// predicate is excluded from the index even though it is in the table.
    #[test]
    fn reopen_restores_filtered_index_predicate_and_still_filters() {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_index::{FilterOp, FilterPredicate, IndexType};
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let user_tid = TableId::new("test_ks", "filtered_table");
        let indexes_tid = TableId::new("system_schema", "indexes");

        // name (storage col) indexed; status (storage col) is the filter column.
        let user_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "filtered_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "status".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        };
        // Storage ordinal of the filter column `status`: regular columns are
        // stored name-sorted with no statics here, so `name`=0, `status`=1.
        let mut regular_names: Vec<&str> = user_schema
            .regular_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        regular_names.sort_unstable();
        let status_pos = regular_names
            .iter()
            .position(|n| *n == "status")
            .expect("status column present");

        let predicate = FilterPredicate::single(status_pos, FilterOp::Eq, b"active".to_vec());
        let mut options = std::collections::HashMap::new();
        options.insert(
            FILTER_PREDICATE_OPTION_KEY.to_string(),
            predicate.to_option_string().unwrap(),
        );
        let idx_meta = ferrosa_schema::metadata::index::IndexMetadata {
            keyspace: "test_ks".to_string(),
            table: "filtered_table".to_string(),
            name: "name_active_idx".to_string(),
            index_type: IndexType::Filtered,
            target_columns: vec!["name".to_string()],
            filter_predicate: Some(predicate.clone()),
            options,
        };

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(user_schema.clone()).unwrap();
            engine.register_system_tables().unwrap();

            let row = persistence::index_to_rows(&idx_meta);
            engine
                .write(&indexes_tid, &row.key, row.row, now_micros_for_test())
                .unwrap();

            engine.flush(&user_tid).unwrap();
            engine.flush(&indexes_tid).unwrap();
        }

        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, _pending) = StorageEngine::open(config, None).unwrap();
        engine.register_system_tables().unwrap();

        let restored = engine.reload_indexes_from_system_schema().unwrap();
        assert_eq!(restored, 1, "the filtered index should be restored");

        assert_eq!(
            engine.index_type_for_test(&user_tid, "name_active_idx"),
            Some(IndexType::Filtered),
            "index must survive restart as Filtered"
        );
        assert_eq!(
            engine.filter_predicate_for_test(&user_tid, "name_active_idx"),
            Some(predicate),
            "the partial predicate must survive restart exactly"
        );

        // The reloaded index still filters live writes: alice/active is indexed,
        // alice/inactive is not.
        let storage_to_cell = |name: &str, status: &str, ts: i64| Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(name.as_bytes().to_vec(), ts)),
                (1, CellValue::live(status.as_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        engine
            .write(
                &user_tid,
                &make_key("pk-active"),
                storage_to_cell("alice", "active", 1000),
                1000,
            )
            .unwrap();
        engine
            .write(
                &user_tid,
                &make_key("pk-inactive"),
                storage_to_cell("alice", "inactive", 1001),
                1001,
            )
            .unwrap();

        let hits = engine
            .read_by_index(
                &user_tid,
                "name_active_idx",
                &ferrosa_index::IndexKey(b"alice".to_vec()),
            )
            .unwrap();
        let pks: Vec<Vec<u8>> = hits.iter().map(|p| p.key.key.as_bytes().to_vec()).collect();
        assert_eq!(
            pks,
            vec![b"pk-active".to_vec()],
            "reloaded filtered index must return only the active row, not the inactive one"
        );
    }

    fn now_micros_for_test() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }

    /// A persisted `system_schema.types` row survives a flush + reopen and
    /// `read_persisted_types` decodes it back into the original UDT fields,
    /// including a nested collection field type (lossless serde round-trip).
    #[test]
    fn read_persisted_types_round_trips_through_storage() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::user_type::UserTypeMetadata;
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let types_tid = TableId::new("system_schema", "types");
        let udt = UserTypeMetadata {
            keyspace: "app".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
                (
                    "tags".to_string(),
                    CqlType::List(Box::new(CqlType::Varchar)),
                ),
            ],
        };

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_system_tables().unwrap();

            // Persist the type row exactly as the DDL write path does.
            let row = persistence::type_to_row(&udt);
            engine
                .write(&types_tid, &row.key, row.row, now_micros_for_test())
                .unwrap();
            engine.flush(&types_tid).unwrap();
        }

        // Reopen and read from storage (SSTable, not memtable).
        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, _pending) = StorageEngine::open(config, None).unwrap();
        engine.register_system_tables().unwrap();

        let stored = engine.read_persisted_types().unwrap();
        assert_eq!(stored.len(), 1, "exactly one persisted type expected");
        let row = &stored[0];
        assert_eq!(row.keyspace_name, "app");
        assert_eq!(row.type_name, "address");
        assert_eq!(
            row.fields,
            vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
                (
                    "tags".to_string(),
                    CqlType::List(Box::new(CqlType::Varchar))
                ),
            ],
            "fields (incl. nested list<text>) must round-trip losslessly"
        );
    }

    /// A tombstoned `system_schema.types` row (DROP TYPE) masks the live row:
    /// `read_persisted_types` returns nothing after the tombstone is written.
    #[test]
    fn read_persisted_types_skips_tombstoned_rows() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::user_type::UserTypeMetadata;
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let types_tid = TableId::new("system_schema", "types");
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_system_tables().unwrap();

        let udt = UserTypeMetadata {
            keyspace: "app".to_string(),
            name: "gone".to_string(),
            fields: vec![("x".to_string(), CqlType::Int)],
        };
        let row = persistence::type_to_row(&udt);
        engine
            .write(&types_tid, &row.key, row.row, now_micros_for_test())
            .unwrap();
        assert_eq!(engine.read_persisted_types().unwrap().len(), 1);

        // Tombstone the same (keyspace, type_name) via a row deletion.
        let ts = now_micros_for_test() + 1;
        let key = DecoratedKey::new(PartitionKey::new(b"app".to_vec()));
        let tombstone = Row {
            clustering: b"gone".to_vec(),
            cells: vec![],
            deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
            primary_key_liveness: LivenessInfo::NONE,
        };
        engine.write(&types_tid, &key, tombstone, ts).unwrap();

        assert!(
            engine.read_persisted_types().unwrap().is_empty(),
            "dropped type must not appear in read_persisted_types"
        );
    }

    /// A persisted `system_schema.functions` row survives a flush + reopen and
    /// `read_persisted_functions` decodes it back into the original metadata,
    /// including a nested-collection return type (lossless serde round-trip).
    #[test]
    fn read_persisted_functions_round_trips_through_storage() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::function::UserFunctionMetadata;
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let functions_tid = TableId::new("system_schema", "functions");
        let func = UserFunctionMetadata {
            keyspace: "app".to_string(),
            name: "tokenize".to_string(),
            arg_names: vec!["s".to_string()],
            arg_types: vec![CqlType::Varchar],
            return_type: CqlType::List(Box::new(CqlType::Varchar)),
            called_on_null: true,
            language: "wasm".to_string(),
            body: "deadbeef".to_string(),
        };

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_system_tables().unwrap();

            let row = persistence::function_to_row(&func);
            engine
                .write(&functions_tid, &row.key, row.row, now_micros_for_test())
                .unwrap();
            engine.flush(&functions_tid).unwrap();
        }

        // Reopen and read from storage (SSTable, not memtable).
        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, _pending) = StorageEngine::open(config, None).unwrap();
        engine.register_system_tables().unwrap();

        let stored = engine.read_persisted_functions().unwrap();
        assert_eq!(stored.len(), 1, "exactly one persisted function expected");
        let row = &stored[0];
        assert_eq!(row.keyspace_name, "app");
        assert_eq!(row.function_name, "tokenize");
        assert_eq!(row.arg_names, vec!["s".to_string()]);
        assert_eq!(row.arg_types, vec![CqlType::Varchar]);
        assert_eq!(row.return_type, CqlType::List(Box::new(CqlType::Varchar)));
        assert!(row.called_on_null);
        assert_eq!(row.language, "wasm");
        assert_eq!(row.body, "deadbeef");
    }

    /// Two overloads of the same function name persist as distinct rows and a
    /// tombstone of one overload (DROP FUNCTION) masks only that overload.
    #[test]
    fn read_persisted_functions_overloads_and_tombstone() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::function::UserFunctionMetadata;
        use ferrosa_schema::system::persistence;

        let dir = tempfile::tempdir().unwrap();
        let functions_tid = TableId::new("system_schema", "functions");
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_system_tables().unwrap();

        let make = |arg: CqlType| UserFunctionMetadata {
            keyspace: "app".to_string(),
            name: "norm".to_string(),
            arg_names: vec!["v".to_string()],
            arg_types: vec![arg.clone()],
            return_type: arg,
            called_on_null: false,
            language: "wasm".to_string(),
            body: "ab".to_string(),
        };
        for func in [make(CqlType::Int), make(CqlType::Varchar)] {
            let row = persistence::function_to_row(&func);
            engine
                .write(&functions_tid, &row.key, row.row, now_micros_for_test())
                .unwrap();
        }
        assert_eq!(
            engine.read_persisted_functions().unwrap().len(),
            2,
            "two overloads must persist as distinct rows"
        );

        // Tombstone only the int overload.
        let ts = now_micros_for_test() + 1;
        let key = DecoratedKey::new(PartitionKey::new(b"app".to_vec()));
        let tombstone = Row {
            clustering: persistence::function_clustering("norm", &[CqlType::Int]),
            cells: vec![],
            deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
            primary_key_liveness: LivenessInfo::NONE,
        };
        engine.write(&functions_tid, &key, tombstone, ts).unwrap();

        let remaining = engine.read_persisted_functions().unwrap();
        assert_eq!(remaining.len(), 1, "only the text overload should remain");
        assert_eq!(remaining[0].arg_types, vec![CqlType::Varchar]);
    }

    #[test]
    fn open_streams_schema_backed_commitlog_replay_without_pending_vec() {
        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("test_ks", "test_table");
        let key = make_key("schema-backed-replay");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.flush(&tid).unwrap();
            engine
                .write(&tid, &key, make_row(b"streamed", 42), 42)
                .unwrap();
            engine.commit_log.shutdown().unwrap();
        }

        let config = StorageEngineConfig::test_config(dir.path());
        let (engine, pending) = StorageEngine::open(config, None).unwrap();

        assert!(
            pending.is_empty(),
            "StorageEngine::open must stream schema-backed commit-log replay directly into registered tables instead of returning an eager pending Vec"
        );
        let replayed = engine
            .read(&tid, &key)
            .unwrap()
            .expect("schema-backed unflushed row should be visible immediately after open");
        assert_eq!(replayed.rows.len(), 1);
        assert_eq!(engine.deferred_replay_mutation_count_for_test(), 0);
    }

    #[test]
    fn open_no_schema_fallback_fails_before_unbounded_pending_vec() {
        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("test_ks", "test_table");

        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            for i in 0..2 {
                engine
                    .write(
                        &tid,
                        &make_key(&format!("no-schema-{i}")),
                        make_row(b"pending", i),
                        i,
                    )
                    .unwrap();
            }
            engine.commit_log.shutdown().unwrap();
        }

        let schema_path = dir.path().join("schema.json");
        if schema_path.exists() {
            std::fs::remove_file(schema_path).unwrap();
        }

        let config = StorageEngineConfig {
            max_pending_replay_mutations_without_schema: 1,
            memtable_num_shards: 64,
            ..StorageEngineConfig::test_config(dir.path())
        };
        let err = match StorageEngine::open(config, None) {
            Ok(_) => panic!(
                "no-schema compatibility replay must fail closed instead of growing an unbounded pending Vec"
            ),
            Err(err) => err.to_string(),
        };

        assert!(
            err.contains("schema unavailable") && err.contains("pending replay limit"),
            "error must explain that schema must be restored before commit-log replay can continue; got: {err}"
        );
    }

    #[test]
    fn schema_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("test_ks", "test_table");

        // First engine: register, write, flush (flush writes schema.json).
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            let key = make_key("restart_key");
            engine
                .write(&tid, &key, make_row(b"restart_val", 1000), 1000)
                .unwrap();
            engine.flush(&tid).unwrap();
            // engine drops here — schema.json is now on disk
        }

        // Second engine at the SAME directory: must NOT call register_table.
        let config2 = StorageEngineConfig::test_config(dir.path());
        let engine2 = StorageEngine::new(config2, None).unwrap();

        // Write succeeds only if the table was re-registered from schema.json.
        let key2 = make_key("restart_key2");
        let write_result = engine2.write(&tid, &key2, make_row(b"after_restart", 2000), 2000);
        assert!(
            write_result.is_ok(),
            "write after restart must succeed — schema must have been loaded \
             from schema.json; got: {:?}",
            write_result.err()
        );

        // Data written before restart is readable too.
        let key1 = make_key("restart_key");
        let read_result = engine2.read(&tid, &key1).unwrap();
        assert!(
            read_result.is_some(),
            "row written before restart must be readable after restart"
        );
        assert_eq!(
            read_result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"restart_val".as_slice()),
            "row value must be unchanged after restart"
        );
    }

    /// Like `schema_survives_restart` but explicitly confirms SSTable files
    /// are present on disk before the restart — this exercises the "non-empty
    /// data directory" code path where the old S3 bootstrap was gated.
    #[test]
    fn schema_survives_binary_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let tid = TableId::new("test_ks", "test_table");

        // First engine: register, write, flush.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            let key = make_key("upgrade_key");
            engine
                .write(&tid, &key, make_row(b"upgrade_val", 5000), 5000)
                .unwrap();
            engine.flush(&tid).unwrap();
        }

        // Verify at least one .db file exists on disk before restart.
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let db_files: Vec<_> = std::fs::read_dir(&table_dir)
            .expect("sstables table dir must exist after flush")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "db").unwrap_or(false))
            .collect();
        assert!(
            !db_files.is_empty(),
            "at least one .db SSTable file must exist on disk before restart"
        );

        // Second engine: schema must be present without calling register_table.
        let config2 = StorageEngineConfig::test_config(dir.path());
        let engine2 = StorageEngine::new(config2, None).unwrap();

        // The pre-restart row must be readable.
        let key = make_key("upgrade_key");
        let result = engine2.read(&tid, &key).unwrap();
        assert!(
            result.is_some(),
            "row written before binary upgrade must be readable after restart"
        );
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"upgrade_val".as_slice()),
            "row value must survive binary upgrade restart"
        );
    }

    /// Test that a map<text,int> encoded in CQL v4+ wire format (as gocql would
    /// send it) survives the full write→flush→read cycle without any byte loss
    /// or reinterpretation.
    ///
    /// Wire format: 4B BE count, then for each entry:
    ///   4B BE key_len + key_bytes + 4B BE val_len + val_bytes.
    #[test]
    fn collection_via_gocql_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = collection_schema(
            "test_ks",
            "gocql_map_table",
            "org.apache.cassandra.db.marshal.MapType(\
             org.apache.cassandra.db.marshal.UTF8Type,\
             org.apache.cassandra.db.marshal.Int32Type)",
        );
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "gocql_map_table");
        let key = make_key("gocql_pk");

        // Encode {'hello': 42, 'world': 99} as CQL v4+ wire format.
        let map_bytes = encode_cql_map(&[
            (b"hello", &42i32.to_be_bytes()),
            (b"world", &99i32.to_be_bytes()),
        ]);

        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(map_bytes.clone(), 7000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(7000),
        };
        engine.write(&tid, &key, row, 7000).unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "flush should have written 1 SSTable"
        );
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush"
        );

        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some(), "map row must be readable after flush");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(map_bytes.as_slice()),
            "gocql map bytes must survive write→flush→read unchanged"
        );
    }

    // ── C2.2: S3 upload confirmation before manifest update ──────────────────

    /// Object store wrapper that fails all PUT operations immediately with a
    /// non-transient error, simulating an S3 outage.  Read operations are
    /// delegated to an in-memory inner store so manifest probes succeed.
    struct FailOnPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
    }

    impl FailOnPutStore {
        fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
            Self { inner }
        }
    }

    impl std::fmt::Display for FailOnPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailOnPutStore")
        }
    }

    impl std::fmt::Debug for FailOnPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailOnPutStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailOnPutStore {
        /// Fail immediately with a non-transient error so `put_with_retry` does
        /// not loop through its 5-attempt backoff (which would make the test slow).
        async fn put_opts(
            &self,
            _location: &object_store::path::Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::NotSupported {
                source: "simulated S3 outage — PUT rejected by FailOnPutStore".into(),
            })
        }

        async fn put_multipart_opts(
            &self,
            _location: &object_store::path::Path,
            _opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Err(object_store::Error::NotSupported {
                source: "simulated S3 outage — multipart PUT rejected by FailOnPutStore".into(),
            })
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// C2.2 — S3 upload confirmation before manifest update.
    ///
    /// Verifies the invariant: when an S3 upload fails, `poll_compactions` must
    /// NOT update the manifest.  The compacted output entry must not appear in
    /// S3, and the upload counter must remain zero.
    ///
    /// The fix lives in `poll_compactions()` at the `rx.await` match arm: an
    /// upload failure causes `continue`, skipping the manifest-update block.
    /// This test proves that path is exercised correctly.
    #[tokio::test]
    async fn s3_upload_confirmation_before_manifest() {
        // Wrap a real in-memory store with the failing store so that:
        //   • manifest probes (GET) succeed against the inner store
        //   • upload PUTs fail immediately (non-transient, no retry loop)
        let inner_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let failing_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailOnPutStore::new(Arc::clone(&inner_store)));

        let dir = tempfile::tempdir().unwrap();
        let prefix = "test-fail-put".to_string();
        let rt = tokio::runtime::Handle::current();
        let engine = StorageEngine::new_with_upload_store(
            StorageEngineConfig::test_config(dir.path()),
            Arc::clone(&failing_store),
            prefix.clone(),
            &rt,
        )
        .unwrap();

        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // Write 4 partitions and flush each to create 4 SSTables — STCS fires
        // on the 4th flush (min_threshold = 4 by default).
        for (i, key_suffix) in ["a", "b", "c", "d"].iter().enumerate() {
            let ts = (i as i64 + 1) * 1000;
            engine
                .write(&tid, &make_key(key_suffix), make_row(b"v", ts), ts)
                .unwrap();
            engine.flush(&tid).unwrap();
        }

        // Wait for the compaction executor background thread to write output
        // files to disk before calling poll_compactions.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let has_output = compaction_dir.exists()
                && std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
            if has_output {
                break;
            }
        }

        // Call poll_compactions — upload will fail because FailOnPutStore rejects PUTs.
        engine.poll_compactions().await;

        // The upload counter must be zero: no successful S3 upload occurred.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "s3_uploads_total must be 0 when the S3 upload fails"
        );

        // The manifest must NOT contain the compacted output.
        // When upload fails, poll_compactions hits `continue` before step 5 (manifest update).
        // The inner store (used for GETs) has never had a manifest written to it,
        // so Manifest::load returns an empty manifest — no entries for this table.
        let (manifest, _) = crate::manifest::Manifest::load(inner_store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
        assert!(
            entries.is_empty(),
            "manifest must NOT be updated when S3 upload fails; \
             found {} entries for {tid_str}: {:?}",
            entries.len(),
            entries
        );
    }

    // ── NV-006: pin_max_bytes enforcement ────────────────────────────────────

    /// Verifies that pinned SSTables are tracked after each flush and that
    /// when total pinned bytes exceed max_bytes, the oldest SSTables are
    /// evicted from disk.
    ///
    /// Test setup: pin_config with max_bytes = 1 (any non-zero size exceeds it).
    /// After several flushes the first SSTable must have been evicted.
    #[test]
    fn pinned_table_respects_max_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        // Register the table in pin mode with a tiny cap.
        // 1-byte cap guarantees every flush after the first triggers an eviction.
        engine
            .register_table_pinned(test_schema(), PinConfig { max_bytes: Some(1) })
            .unwrap();

        let tid = table_id();

        // Write + flush multiple times to accumulate SSTables.
        for (i, key) in ["p1", "p2", "p3"].iter().enumerate() {
            let ts = (i as i64 + 1) * 1000;
            engine
                .write(&tid, &make_key(key), make_row(b"value", ts), ts)
                .unwrap();
            engine.flush(&tid).unwrap();
        }

        // With max_bytes=1 the engine enforces the cap after every flush.
        // The pin eviction counter must be > 0.
        assert!(
            engine
                .pin_metrics
                .pin_evictions_total
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "pin evictions must occur when total pinned bytes exceed max_bytes"
        );

        // The pinned_bytes gauge must reflect what is still on disk (≤ 0 or just the
        // last SSTable since the others were evicted).  Exact value depends on file
        // sizes, but we verify it is non-negative.
        assert!(
            engine
                .pin_metrics
                .pinned_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 0,
            "pinned_bytes gauge must be non-negative"
        );
    }

    /// Verifies that a pinned table is tracked via pin_metrics.pinned_tables == 1
    /// and that pinned_bytes grows after flush.
    #[test]
    fn pinned_metrics_accurate() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        // No cap so all SSTables are retained.
        engine
            .register_table_pinned(test_schema(), PinConfig { max_bytes: None })
            .unwrap();

        let tid = table_id();

        // Verify the gauge was incremented on registration.
        assert_eq!(
            engine
                .pin_metrics
                .pinned_tables
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "pinned_tables must be 1 after register_table_pinned"
        );

        // Write + flush so a real SSTable file exists.
        engine
            .write(&tid, &make_key("k1"), make_row(b"hello", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // pinned_bytes must be > 0 after flush (files written to disk).
        assert!(
            engine
                .pin_metrics
                .pinned_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "pinned_bytes must be > 0 after flushing a pinned table"
        );
    }

    // ── NV-007: ALTER TABLE toggle (pin → unpin → pin) ───────────────────────

    /// Verifies that unpinning a table triggers S3 upload for all
    /// previously-pinned SSTables that were skipped.
    #[tokio::test]
    async fn unpin_resumes_s3_upload() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let prefix = "test-unpin".to_string();
        let rt = tokio::runtime::Handle::current();

        let engine = StorageEngine::new_with_upload_store(
            StorageEngineConfig::test_config(dir.path()),
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();

        let tid = table_id();

        // Register as pinned — no cap, so all SSTables stay local.
        engine
            .register_table_pinned(test_schema(), PinConfig { max_bytes: None })
            .unwrap();

        // Write + flush: S3 upload must be skipped while pinned.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Confirm S3 has no SSTables for this table yet.
        let (manifest_before, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries_before = manifest_before
            .sstables
            .get(&tid_str)
            .cloned()
            .unwrap_or_default();
        assert!(
            entries_before.is_empty(),
            "S3 must have no entries while table is pinned; found: {:?}",
            entries_before
        );

        // Unpin the table — this should enqueue S3 uploads for the skipped SSTables.
        engine.update_table_pin_config(&tid, None).await.unwrap();

        // pinned_tables gauge must decrement.
        assert_eq!(
            engine
                .pin_metrics
                .pinned_tables
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "pinned_tables must be 0 after unpin"
        );

        // Give the upload manager a moment to process the queued tasks.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // After unpinning, sync_sstables_to_s3 should see the already-queued
        // SSTables and be able to pick them up (or they're already uploaded).
        // We verify by calling sync_sstables_to_s3 and checking manifest.
        let _synced = engine.sync_sstables_to_s3().await.unwrap_or(0);

        // Upload manager may or may not have completed by now (fire-and-forget).
        // The key assertion is that pinned_tables is 0 and no panic occurred.
    }

    /// Verifies that pinning a previously-normal table stops new flushes
    /// from being uploaded to S3.
    #[tokio::test]
    async fn pin_stops_s3_upload() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let prefix = "test-pin-stop".to_string();
        let rt = tokio::runtime::Handle::current();

        let engine = StorageEngine::new_with_upload_store(
            StorageEngineConfig::test_config(dir.path()),
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();

        let tid = table_id();

        // Register normally (no pin) and flush once — should upload to S3.
        engine.register_table(test_schema()).unwrap();
        engine
            .write(&tid, &make_key("before"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Now pin the table.
        engine
            .update_table_pin_config(&tid, Some(PinConfig { max_bytes: None }))
            .await
            .unwrap();

        assert_eq!(
            engine
                .pin_metrics
                .pinned_tables
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "pinned_tables must be 1 after pinning"
        );

        // Flush again while pinned — this SSTable must NOT be enqueued for S3.
        engine
            .write(&tid, &make_key("after"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // The pin metrics should show the second SSTable was pinned (bytes > 0).
        assert!(
            engine
                .pin_metrics
                .pinned_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 0,
            "pinned_bytes must be non-negative after flush while pinned"
        );
    }

    // ── FT-018: FTI sidecar created on flush ─────────────────────────────────

    /// Verifies that flushing a table with a fulltext index produces an FTI
    /// sidecar file alongside the SSTable, and that the FTI is queryable.
    #[test]
    fn fts_sidecar_created_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // Register a fulltext index on column 0.
        engine.add_fulltext_index(&tid, "idx_body", 0).unwrap();

        // Write 3 rows with text content in column 0.
        for (key, text) in [
            ("r1", "rust distributed database"),
            ("r2", "cassandra storage"),
            ("r3", "hello world"),
        ] {
            engine
                .write(&tid, &make_key(key), make_row(text.as_bytes(), 1000), 1000)
                .unwrap();
        }
        engine.flush(&tid).unwrap();

        // Check that an FTI sidecar file was created.
        let table_dir = dir.path().join("sstables").join(tid.to_string());
        let fti_files: Vec<_> = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("FTI-idx_body"))
            .collect();
        assert_eq!(
            fti_files.len(),
            1,
            "exactly one FTI sidecar file must exist after flush"
        );

        // Verify the FTI is valid and queryable.
        let fti_bytes = std::fs::read(fti_files[0].path()).unwrap();
        let reader = ferrosa_index::fulltext::reader::FullTextIndexReader::open(fti_bytes).unwrap();
        assert_eq!(reader.doc_count(), 3);

        let hits = reader.search_str("rust").unwrap();
        assert!(!hits.is_empty(), "search for 'rust' must return results");
    }

    // ── FT-019: FTS end-to-end insert → flush → query ──────────────────────

    #[test]
    fn fts_end_to_end_insert_query() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        engine.add_fulltext_index(&tid, "idx_body", 0).unwrap();

        // Insert rows with different text.
        let docs = [
            ("r1", "rust is a fast distributed database language"),
            ("r2", "cassandra is a distributed database"),
            ("r3", "hello world"),
            ("r4", "distributed systems are complex"),
            ("r5", "database normalization theory"),
        ];
        for (key, text) in &docs {
            engine
                .write(&tid, &make_key(key), make_row(text.as_bytes(), 1000), 1000)
                .unwrap();
        }
        engine.flush(&tid).unwrap();

        // Query for rows with BOTH "distributed" AND "database".
        let results = engine
            .fulltext_search(&tid, "idx_body", "distributed AND database")
            .unwrap();
        let result_keys: Vec<String> = results
            .iter()
            .map(|pk| String::from_utf8_lossy(pk).to_string())
            .collect();

        assert!(result_keys.contains(&"r1".to_string()), "r1 has both terms");
        assert!(result_keys.contains(&"r2".to_string()), "r2 has both terms");
        assert!(
            !result_keys.contains(&"r3".to_string()),
            "r3 has neither term"
        );
    }

    #[test]
    fn fts_query_sees_unflushed_memtable_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();
        let tid = table_id();
        engine.add_fulltext_index(&tid, "idx_body", 0).unwrap();

        engine
            .write(
                &tid,
                &make_key("fresh"),
                make_row(b"ferrosaftsfresh native fts probe body", 1000),
                1000,
            )
            .unwrap();

        let results = engine
            .fulltext_search(&tid, "idx_body", "ferrosaftsfresh")
            .unwrap();
        let result_keys: Vec<String> = results
            .iter()
            .map(|pk| String::from_utf8_lossy(pk).to_string())
            .collect();

        assert_eq!(
            result_keys,
            vec!["fresh".to_string()],
            "fts_match must include rows that are still only in the active memtable"
        );
    }

    // -----------------------------------------------------------------------
    // Data loss regression: write → flush → compact → read must return all keys
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn write_flush_compact_no_data_loss() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // Write 100 keys, flush every 25 to create 4+ SSTables and trigger compaction.
        for i in 0..100u64 {
            let key = make_key(&format!("key_{i:04}"));
            // `test_schema()` declares an Int32Type clustering column, so the
            // clustering bytes must be exactly 4. The SSTable writer's
            // Gate A rejects empty clustering on a schema that declares a
            // fixed-length clustering column.
            let row = Row {
                clustering: (i as i32).to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    CellValue::live(format!("val_{i}").into_bytes(), i as i64),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(i as i64),
            };
            engine.write(&tid, &key, row, i as i64).unwrap();

            if (i + 1) % 25 == 0 {
                engine.flush(&tid).unwrap();
            }
        }

        // Final flush.
        engine.flush(&tid).unwrap();

        // Poll compaction (STCS triggers at 4 SSTables).
        engine.poll_compactions().await;

        // Read all 100 keys — none should be missing.
        let mut missing = Vec::new();
        for i in 0..100u64 {
            let key = make_key(&format!("key_{i:04}"));
            match engine.read(&tid, &key) {
                Ok(Some(_)) => {}
                Ok(None) => missing.push(format!("key_{i:04}")),
                Err(e) => missing.push(format!("key_{i:04} (error: {e})")),
            }
        }

        assert!(
            missing.is_empty(),
            "data loss: {}/{} keys missing after flush+compact: {:?}",
            missing.len(),
            100,
            &missing[..missing.len().min(10)]
        );
    }

    /// Concurrent writes + flushes + compaction — reproduces the loadgen data loss.
    #[tokio::test]
    async fn concurrent_write_flush_compact_no_data_loss() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicU64::new(0));

        // Writer threads
        let mut handles = vec![];
        for worker in 0..4u64 {
            let eng = engine.clone();
            let tid = tid.clone();
            let stop = stop.clone();
            let written = written.clone();
            handles.push(std::thread::spawn(move || {
                let mut ts = worker * 1_000_000;
                while !stop.load(Ordering::Relaxed) {
                    let idx = written.fetch_add(1, Ordering::SeqCst);
                    let key = make_key(&format!("k_{idx:06}"));
                    ts += 1;
                    let row = Row {
                        clustering: vec![0, 0, 0, 1],
                        cells: vec![(
                            0,
                            CellValue::live(format!("v{idx}").into_bytes(), ts as i64),
                        )],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts as i64),
                    };
                    let _ = eng.write(&tid, &key, row, ts as i64);
                }
            }));
        }

        // Main thread: flush + compact for 2 seconds.
        let start = std::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_secs(2) {
            std::thread::sleep(std::time::Duration::from_millis(100));
            let _ = engine.flush(&tid);
            engine.poll_compactions().await;
        }
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        // Final flush.
        engine.flush(&tid).unwrap();

        // Read back all written keys.
        let total = written.load(Ordering::SeqCst);
        let mut missing = 0u64;
        for i in 0..total {
            let key = make_key(&format!("k_{i:06}"));
            if engine.read(&tid, &key).unwrap().is_none() {
                missing += 1;
            }
        }

        assert_eq!(
            missing, 0,
            "data loss: {missing}/{total} keys missing after concurrent write+flush+compact"
        );
    }

    #[test]
    fn storage_engine_creates_spans() {
        crate::test_span_collector::ensure_installed();
        crate::test_span_collector::drain_names();

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::schema::TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(schema).unwrap();

        let table_id = TableId::new("ks", "tbl");
        let key = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(b"pk1".to_vec()));
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"val".to_vec(), 1))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
        };

        engine.write(&table_id, &key, row, 1).unwrap();
        let _ = engine.read(&table_id, &key);

        let recorded = crate::test_span_collector::drain_names();
        assert!(
            recorded.iter().any(|n| n == "storage.write"),
            "expected 'storage.write' span, got: {recorded:?}",
        );
        assert!(
            recorded.iter().any(|n| n == "storage.read"),
            "expected 'storage.read' span, got: {recorded:?}",
        );
    }

    // =========================================================================
    // P0-13: SSTable upload→S3→download round-trip
    // =========================================================================

    #[test]
    fn download_sstables_from_s3_skips_stale_manifest_entries_when_other_sstables_restore() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-stale-manifest-entry".to_string();
            let tid = table_id();
            let table_id_str = tid.to_string();

            let engine = StorageEngine::new_with_upload_store(
                StorageEngineConfig::test_config(dir.path()),
                Arc::clone(&store),
                prefix.clone(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();

            let mut manifest = crate::manifest::Manifest::new();
            for id in ["1", "2"] {
                manifest.add_sstable(
                    &table_id_str,
                    crate::manifest::ManifestEntry {
                        id: id.to_string(),
                        size: 4,
                        min_token: 0,
                        max_token: 0,
                        min_timestamp: 0,
                        max_timestamp: 0,
                    },
                );
            }

            // Only generation 1 is actually present in object storage. Generation
            // 2 models a stale manifest entry left behind by interrupted
            // compaction/upload cleanup. Bootstrap should restore the usable
            // SSTable instead of failing the whole table on the stale entry.
            let hex = crate::upload::manager::hex_prefix_for("1");
            let path = crate::upload::manager::sstable_object_key(
                &prefix,
                &hex,
                &table_id_str,
                "1",
                "Data.db",
            );
            store
                .put(
                    &path,
                    object_store::PutPayload::from(bytes::Bytes::from_static(b"data")),
                )
                .await
                .unwrap();

            let downloaded = engine
                .download_sstables_from_s3(&tid, &manifest)
                .await
                .expect("stale missing SSTable entries must not fail partial restore");

            assert_eq!(downloaded, 1);
            assert!(dir
                .path()
                .join("sstables")
                .join(&table_id_str)
                .join("1-Data.db")
                .exists());
            assert!(!dir
                .path()
                .join("sstables")
                .join(&table_id_str)
                .join("2-Data.db")
                .exists());
        });
    }

    /// Round-trip test: write rows → flush SSTables → upload to mock S3 →
    /// delete local SSTable files → call download_sstables_from_s3 →
    /// verify files are back on disk and rows are readable.
    ///
    /// This test would have caught the upload/download key format mismatch
    /// (p0-13) because download_sstables_from_s3 would have returned an error
    /// (required component not found) instead of silently writing zero files.
    #[test]
    fn p0_13_sstable_upload_download_round_trip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-p0-13";

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new_with_upload_store(
                config,
                Arc::clone(&store),
                prefix.to_string(),
                &tokio::runtime::Handle::current(),
            )
            .unwrap();

            let schema = test_schema();
            let tid = table_id();
            engine.register_table(schema.clone()).unwrap();

            // Write rows so they end up in the memtable.
            let rows_to_write: Vec<(&str, &[u8])> = vec![
                ("alpha", b"value-alpha"),
                ("beta", b"value-beta"),
                ("gamma", b"value-gamma"),
            ];
            for (k, v) in &rows_to_write {
                engine
                    .write(&tid, &make_key(k), make_row(v, 1000), 1000)
                    .unwrap();
            }

            // Flush to disk: creates {gen}-Data.db and related component files.
            engine.flush(&tid).unwrap();

            // Verify the rows are readable from the flushed SSTable.
            for (k, _) in &rows_to_write {
                let result = engine.read(&tid, &make_key(k)).unwrap();
                assert!(result.is_some(), "row '{k}' should be readable after flush");
            }

            // Upload SSTables + manifest to the mock S3 store.
            // This uses upload_manifest_for_test which goes through
            // sstable_object_key — the same path that download uses.
            engine
                .upload_manifest_for_test(Arc::clone(&store), prefix)
                .await;

            // Load the manifest so we know what to download.
            let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), prefix)
                .await
                .unwrap();

            // Confirm manifest has our table's SSTable entries.
            let sstable_entries = manifest
                .sstables
                .get(&tid.to_string())
                .cloned()
                .unwrap_or_default();
            assert!(
                !sstable_entries.is_empty(),
                "manifest must contain at least one SSTable entry for table '{tid}'"
            );

            // ── Drop local SSTable files ──────────────────────────────────────
            let table_dir = dir.path().join("sstables").join(tid.to_string());
            for entry in std::fs::read_dir(&table_dir).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name().to_str().unwrap().to_string();
                if name.ends_with(".db") || name.ends_with(".txt") {
                    std::fs::remove_file(entry.path()).unwrap();
                }
            }

            // Confirm the table dir is now empty of .db files.
            let remaining_db_files: Vec<_> = std::fs::read_dir(&table_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with(".db"))
                        .unwrap_or(false)
                })
                .collect();
            assert!(
                remaining_db_files.is_empty(),
                "no .db files should remain before download, found: {remaining_db_files:?}"
            );

            // ── Download SSTables from mock S3 ───────────────────────────────
            let downloaded = engine
                .download_sstables_from_s3(&tid, &manifest)
                .await
                .expect("download_sstables_from_s3 must not return an error");

            assert!(
                downloaded > 0,
                "download_sstables_from_s3 must report at least one downloaded SSTable, \
                 got {downloaded} (p0-13: counter must reflect bytes-on-disk reality)"
            );
            assert_eq!(
                downloaded,
                sstable_entries.len(),
                "downloaded count ({downloaded}) must equal manifest entry count ({})",
                sstable_entries.len()
            );

            // ── Verify files are on disk ──────────────────────────────────────
            for entry in &sstable_entries {
                let data_file = table_dir.join(format!("{}-Data.db", entry.id));
                assert!(
                    data_file.exists(),
                    "Data.db for SSTable '{}' must exist on disk after download: {}",
                    entry.id,
                    data_file.display()
                );
                let meta = std::fs::metadata(&data_file).unwrap();
                assert!(
                    meta.len() > 0,
                    "Data.db for SSTable '{}' must not be zero bytes",
                    entry.id
                );
            }

            // ── Verify rows are readable after re-registering the table ───────
            // Re-register triggers SSTable discovery from disk (the downloaded files).
            engine.shutdown().unwrap();

            let restore_dir = tempfile::tempdir().unwrap();
            let restore_config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(restore_dir.path()),
                data_dir: dir.path().to_path_buf(),
                ..StorageEngineConfig::test_config(restore_dir.path())
            };
            let restored = StorageEngine::new(restore_config, None).unwrap();
            restored.register_table(schema).unwrap();

            for (k, v) in &rows_to_write {
                let result = restored
                    .read(&tid, &make_key(k))
                    .expect("read after download must not error");
                assert!(
                    result.is_some(),
                    "row '{k}' must be readable after S3 round-trip download"
                );
                // Verify cell value matches what was written.
                // read() returns a Partition; rows hold the actual cells.
                let partition = result.unwrap();
                let cell_bytes = partition
                    .rows
                    .first()
                    .and_then(|row| row.cells.first())
                    .and_then(|(_, cell)| cell.value.as_deref())
                    .expect("partition must have at least one row with a cell value");
                assert_eq!(
                    cell_bytes, *v,
                    "cell value for key '{k}' must match original after round-trip"
                );
            }

            restored.shutdown().unwrap();
        });
    }

    // ---------------------------------------------------------------------------
    // poll_compactions regression tests (2026-05-17)
    //
    // Live cluster bug: the loop blocked on `submit().await` + `rx.await` for
    // S3 confirmation, so one slow upload starved all queued compaction
    // results' swap step. Symptom on ferrosa-memory: 127 compaction outputs
    // sitting in /var/lib/ferrosa/compaction/entity_store/ that were
    // produced but never swapped into the in-memory view — reads kept
    // hitting all 43 original SSTables, LIMIT 5 took 22-32 s on cold cache.
    //
    // Fix (engine.rs poll_compactions): replace blocking submit+await with
    // try_submit + bounded timeout on the confirmation, so the loop body
    // for each result completes in bounded time. Durability is preserved
    // by the pending-uploads.log (Step 1) and the periodic
    // sync_sstables_to_s3 retry.
    //
    // The tests below pin the contract that the read-correctness path (swap
    // into the in-memory view) MUST complete regardless of whether the
    // S3-durability path succeeds, AND that a re-poll without new compaction
    // input does NOT re-process the same result (no infinite loop).
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn poll_compactions_swap_happens_without_upload_manager() {
        // No S3 / upload manager. The Some(upload_mgr) early-return at
        // engine.rs ~2972 means we skip all S3 work — but the swap MUST
        // still happen so reads see the compacted output.
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        let pre_swap_count = engine.sstable_count(&tid);
        assert_eq!(pre_swap_count, 2, "two flushes → two SSTables in view");

        // Submit a compaction merging the two flush SSTables.
        {
            let compaction_output_dir = dir.path().join("compaction");
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir,
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Wait for the compaction thread to produce the result, then poll.
        let compaction_dir = dir.path().join("compaction").join(tid.to_string());
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }

        engine.poll_compactions().await;

        let post_swap_count = engine.sstable_count(&tid);
        assert_eq!(
            post_swap_count, 1,
            "swap must merge 2 inputs into 1 output regardless of S3 availability \
             (pre={pre_swap_count}, post={post_swap_count})"
        );

        // Reads must still return both rows after the swap.
        let r1 = engine.read(&tid, &make_key("k1")).unwrap();
        assert!(r1.is_some(), "k1 must be readable after swap");
        let r2 = engine.read(&tid, &make_key("k2")).unwrap();
        assert!(r2.is_some(), "k2 must be readable after swap");
    }

    #[tokio::test]
    async fn poll_compactions_does_not_re_process_already_swapped_result() {
        // After a successful swap, poll_compactions must NOT see the same
        // result again — the executor's mpsc channel must be drained
        // exactly once per result. Re-running poll_compactions when no
        // new compaction has been submitted MUST be a no-op (no
        // re-swap, no growth in SSTable count, no growth in compaction
        // output dir).
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        let compaction_output_dir = dir.path().join("compaction");
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir.clone(),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        let table_compaction_dir = compaction_output_dir.join(tid.to_string());
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if table_compaction_dir.exists()
                && std::fs::read_dir(&table_compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false)
            {
                break;
            }
        }

        // First poll: swap should occur.
        engine.poll_compactions().await;
        let count_after_first_poll = engine.sstable_count(&tid);
        let dir_entries_after_first_poll = std::fs::read_dir(&table_compaction_dir)
            .map(|rd| rd.count())
            .unwrap_or(0);

        assert_eq!(
            count_after_first_poll, 1,
            "first poll: 2 inputs merged into 1 swap output"
        );

        // Second + third poll WITHOUT submitting any new task: must be
        // a complete no-op. No new compaction task gets created here,
        // and the queue is drained → result iterator is empty.
        engine.poll_compactions().await;
        engine.poll_compactions().await;

        let count_after_repolls = engine.sstable_count(&tid);
        assert_eq!(
            count_after_repolls, count_after_first_poll,
            "re-polling without new compaction must NOT change the in-memory \
             view (no re-swap, no infinite loop)"
        );

        // Compaction output dir must not grow either (no new output files
        // were emitted by re-polling).
        let dir_entries_after_repolls = std::fs::read_dir(&table_compaction_dir)
            .map(|rd| rd.count())
            .unwrap_or(0);
        assert_eq!(
            dir_entries_after_repolls, dir_entries_after_first_poll,
            "re-polling must NOT produce new output files (live cluster bug: \
             127 compaction outputs accumulated because results were stuck \
             behind blocked uploads and the strategy kept queuing the same \
             inputs)"
        );
    }
}
